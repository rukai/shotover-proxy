#[cfg(feature = "cassandra-cpp-driver-tests")]
use crate::helpers::cassandra::{
    assert_query_result, run_query, CassandraDriver::Datastax, CassandraError, CassandraErrorCode,
    ResultValue,
};
use crate::helpers::cassandra::{CassandraConnection, CassandraDriver, CassandraDriver::CdrsTokio};
use crate::helpers::ShotoverManager;
#[cfg(feature = "cassandra-cpp-driver-tests")]
use cassandra_cpp::{Error, ErrorKind};
use cassandra_protocol::frame::events::{StatusChange, StatusChangeType};
use cdrs_tokio::frame::events::{
    SchemaChange, SchemaChangeOptions, SchemaChangeTarget, SchemaChangeType, ServerEvent,
};
use docker_api::Docker;
#[cfg(feature = "cassandra-cpp-driver-tests")]
use futures::future::{join_all, try_join_all};
use metrics_util::debugging::DebuggingRecorder;
use rstest::rstest;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;
use tokio::time::{sleep, timeout, Duration};

mod batch_statements;
mod cache;
#[cfg(feature = "cassandra-cpp-driver-tests")]
mod cluster;
#[cfg(feature = "cassandra-cpp-driver-tests")]
mod cluster_multi_rack;
#[cfg(feature = "cassandra-cpp-driver-tests")]
mod cluster_single_rack_v3;
#[cfg(feature = "cassandra-cpp-driver-tests")]
mod cluster_single_rack_v4;
mod collections;
mod functions;
mod keyspace;
mod native_types;
mod prepared_statements;
#[cfg(feature = "cassandra-cpp-driver-tests")]
#[cfg(feature = "alpha-transforms")]
mod protect;
mod table;
mod udt;

#[rstest]
#[case(CdrsTokio)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_passthrough(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yml");

    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-passthrough/topology.yaml");

    let connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;

    keyspace::test(&connection).await;
    table::test(&connection).await;
    udt::test(&connection).await;
    native_types::test(&connection).await;
    collections::test(&connection, driver).await;
    functions::test(&connection).await;
    prepared_statements::test(&connection).await;
    batch_statements::test(&connection).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_source_tls_and_single_tls(#[case] driver: CassandraDriver) {
    test_helpers::cert::generate_cassandra_test_certs();
    let _compose = DockerCompose::new("example-configs/cassandra-tls/docker-compose.yml");

    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-tls/topology.yaml");

    let ca_cert = "example-configs/cassandra-tls/certs/localhost_CA.crt";

    {
        // Run a quick test straight to Cassandra to check our assumptions that Shotover and Cassandra TLS are behaving exactly the same
        let direct_connection = CassandraConnection::new_tls("127.0.0.1", 9042, ca_cert, driver);
        assert_query_result(
            &direct_connection,
            "SELECT bootstrapped FROM system.local",
            &[&[ResultValue::Varchar("COMPLETED".into())]],
        )
        .await;
    }

    let connection = CassandraConnection::new_tls("127.0.0.1", 9043, ca_cert, driver);

    keyspace::test(&connection).await;
    table::test(&connection).await;
    udt::test(&connection).await;
    native_types::test(&connection).await;
    collections::test(&connection, driver).await;
    functions::test(&connection).await;
    prepared_statements::test(&connection).await;
    batch_statements::test(&connection).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cluster_single_rack_v3(#[case] driver: CassandraDriver) {
    let _compose =
        DockerCompose::new("example-configs/cassandra-cluster/docker-compose-cassandra-v3.yml");

    {
        let _shotover_manager = ShotoverManager::from_topology_file(
            "example-configs/cassandra-cluster/topology-dummy-peers-v3.yaml",
        );

        let mut connection1 = CassandraConnection::new("127.0.0.1", 9042, driver).await;
        connection1
            .enable_schema_awaiter("172.16.1.2:9042", None)
            .await;
        keyspace::test(&connection1).await;
        table::test(&connection1).await;
        udt::test(&connection1).await;
        native_types::test(&connection1).await;
        collections::test(&connection1, driver).await;
        functions::test(&connection1).await;
        prepared_statements::test(&connection1).await;
        batch_statements::test(&connection1).await;
        cluster_single_rack_v3::test_dummy_peers(&connection1).await;

        //Check for bugs in cross connection state
        let mut connection2 = CassandraConnection::new("127.0.0.1", 9042, driver).await;
        connection2
            .enable_schema_awaiter("172.16.1.2:9042", None)
            .await;
        native_types::test(&connection2).await;
    }

    cluster_single_rack_v3::test_topology_task(None).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cluster_single_rack_v4(#[case] driver: CassandraDriver) {
    let _compose =
        DockerCompose::new("example-configs/cassandra-cluster/docker-compose-cassandra-v4.yml");

    {
        let _shotover_manager = ShotoverManager::from_topology_file(
            "example-configs/cassandra-cluster/topology-v4.yaml",
        );

        let mut connection1 = CassandraConnection::new("127.0.0.1", 9042, driver).await;
        connection1
            .enable_schema_awaiter("172.16.1.2:9044", None)
            .await;

        keyspace::test(&connection1).await;
        table::test(&connection1).await;
        udt::test(&connection1).await;
        native_types::test(&connection1).await;
        collections::test(&connection1, driver).await;
        functions::test(&connection1).await;
        prepared_statements::test(&connection1).await;
        batch_statements::test(&connection1).await;
        cluster_single_rack_v4::test(&connection1).await;

        //Check for bugs in cross connection state
        let mut connection2 = CassandraConnection::new("127.0.0.1", 9042, driver).await;
        connection2
            .enable_schema_awaiter("172.16.1.2:9044", None)
            .await;
        native_types::test(&connection2).await;
    }

    {
        let _shotover_manager = ShotoverManager::from_topology_file(
            "example-configs/cassandra-cluster/topology-dummy-peers-v4.yaml",
        );

        let mut connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;
        connection
            .enable_schema_awaiter("172.16.1.2:9044", None)
            .await;
        cluster_single_rack_v4::test_dummy_peers(&connection).await;
    }

    cluster_single_rack_v4::test_topology_task(None, Some(9044)).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cluster_multi_rack(#[case] driver: CassandraDriver) {
    let _compose =
        DockerCompose::new("example-configs/cassandra-cluster-multi-rack/docker-compose.yml");

    {
        let _shotover_manager_rack1 = ShotoverManager::from_topology_file_without_observability(
            "example-configs/cassandra-cluster-multi-rack/topology_rack1.yaml",
        );
        let _shotover_manager_rack2 = ShotoverManager::from_topology_file_without_observability(
            "example-configs/cassandra-cluster-multi-rack/topology_rack2.yaml",
        );
        let _shotover_manager_rack3 = ShotoverManager::from_topology_file_without_observability(
            "example-configs/cassandra-cluster-multi-rack/topology_rack3.yaml",
        );

        let mut connection1 = CassandraConnection::new("127.0.0.1", 9042, driver).await;
        connection1
            .enable_schema_awaiter("172.16.1.2:9042", None)
            .await;
        keyspace::test(&connection1).await;
        table::test(&connection1).await;
        udt::test(&connection1).await;
        native_types::test(&connection1).await;
        collections::test(&connection1, driver).await;
        functions::test(&connection1).await;
        prepared_statements::test(&connection1).await;
        batch_statements::test(&connection1).await;
        cluster_multi_rack::test(&connection1).await;

        //Check for bugs in cross connection state
        let mut connection2 = CassandraConnection::new("127.0.0.1", 9042, driver).await;
        connection2
            .enable_schema_awaiter("172.16.1.2:9042", None)
            .await;
        native_types::test(&connection2).await;
    }

    cluster_multi_rack::test_topology_task(None).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_source_tls_and_cluster_tls(#[case] driver: CassandraDriver) {
    test_helpers::cert::generate_cassandra_test_certs();
    let ca_cert = "example-configs/cassandra-tls/certs/localhost_CA.crt";

    let _compose = DockerCompose::new("example-configs/cassandra-cluster-tls/docker-compose.yml");
    {
        let _shotover_manager = ShotoverManager::from_topology_file(
            "example-configs/cassandra-cluster-tls/topology.yaml",
        );

        {
            // Run a quick test straight to Cassandra to check our assumptions that Shotover and Cassandra TLS are behaving exactly the same
            let direct_connection =
                CassandraConnection::new_tls("172.16.1.2", 9042, ca_cert, driver);
            assert_query_result(
                &direct_connection,
                "SELECT bootstrapped FROM system.local",
                &[&[ResultValue::Varchar("COMPLETED".into())]],
            )
            .await;
        }

        let mut connection = CassandraConnection::new_tls("127.0.0.1", 9042, ca_cert, driver);
        connection
            .enable_schema_awaiter("172.16.1.2:9042", Some(ca_cert))
            .await;

        let mut connection = CassandraConnection::new_tls("127.0.0.1", 9042, ca_cert, driver);
        connection
            .enable_schema_awaiter("172.16.1.2:9042", Some(ca_cert))
            .await;

        keyspace::test(&connection).await;
        table::test(&connection).await;
        udt::test(&connection).await;
        native_types::test(&connection).await;
        functions::test(&connection).await;
        collections::test(&connection, driver).await;
        prepared_statements::test(&connection).await;
        batch_statements::test(&connection).await;
        cluster_single_rack_v4::test(&connection).await;
    }

    cluster_single_rack_v4::test_topology_task(Some(ca_cert), None).await;
}

#[rstest]
#[case(CdrsTokio)]
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_redis_cache(#[case] driver: CassandraDriver) {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();
    let _compose = DockerCompose::new("example-configs/cassandra-redis-cache/docker-compose.yml");

    let shotover_manager = ShotoverManager::from_topology_file_without_observability(
        "example-configs/cassandra-redis-cache/topology.yaml",
    );

    let mut redis_connection = shotover_manager.redis_connection(6379);
    let connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;

    keyspace::test(&connection).await;
    table::test(&connection).await;
    udt::test(&connection).await;
    functions::test(&connection).await;
    prepared_statements::test(&connection).await;
    batch_statements::test(&connection).await;
    cache::test(&connection, &mut redis_connection, &snapshotter).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[cfg(feature = "alpha-transforms")]
#[rstest]
// #[case(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_protect_transform_local(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-protect-local/docker-compose.yml");

    let _shotover_manager = ShotoverManager::from_topology_file(
        "example-configs/cassandra-protect-local/topology.yaml",
    );

    let shotover_connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;
    let direct_connection = CassandraConnection::new("127.0.0.1", 9043, driver).await;

    keyspace::test(&shotover_connection).await;
    table::test(&shotover_connection).await;
    udt::test(&shotover_connection).await;
    native_types::test(&shotover_connection).await;
    collections::test(&shotover_connection, driver).await;
    functions::test(&shotover_connection).await;
    batch_statements::test(&shotover_connection).await;
    protect::test(&shotover_connection, &direct_connection).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[cfg(feature = "alpha-transforms")]
#[rstest]
//#[case(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_protect_transform_aws(#[case] driver: CassandraDriver) {
    let _compose = DockerCompose::new("example-configs/cassandra-protect-aws/docker-compose.yml");
    let _compose_aws = DockerCompose::new_moto();

    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-protect-aws/topology.yaml");

    let shotover_connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;
    let direct_connection = CassandraConnection::new("127.0.0.1", 9043, driver).await;

    keyspace::test(&shotover_connection).await;
    table::test(&shotover_connection).await;
    udt::test(&shotover_connection).await;
    native_types::test(&shotover_connection).await;
    collections::test(&shotover_connection, driver).await;
    functions::test(&shotover_connection).await;
    batch_statements::test(&shotover_connection).await;
    protect::test(&shotover_connection, &direct_connection).await;
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_peers_rewrite_cassandra4(#[case] driver: CassandraDriver) {
    let _docker_compose = DockerCompose::new(
        "tests/test-configs/cassandra-peers-rewrite/docker-compose-4.0-cassandra.yaml",
    );

    let _shotover_manager = ShotoverManager::from_topology_file(
        "tests/test-configs/cassandra-peers-rewrite/topology.yaml",
    );

    let normal_connection = CassandraConnection::new("127.0.0.1", 9043, driver).await;
    let rewrite_port_connection = CassandraConnection::new("127.0.0.1", 9044, driver).await;

    // run some basic tests to confirm it works as normal
    table::test(&normal_connection).await;

    {
        assert_query_result(
            &normal_connection,
            "SELECT data_center, native_port, rack FROM system.peers_v2;",
            &[&[
                ResultValue::Varchar("dc1".into()),
                ResultValue::Int(9042),
                ResultValue::Varchar("West".into()),
            ]],
        )
        .await;
        assert_query_result(
            &normal_connection,
            "SELECT native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9042)]],
        )
        .await;

        assert_query_result(
            &normal_connection,
            "SELECT native_port as foo FROM system.peers_v2;",
            &[&[ResultValue::Int(9042)]],
        )
        .await;
    }

    {
        assert_query_result(
            &rewrite_port_connection,
            "SELECT data_center, native_port, rack FROM system.peers_v2;",
            &[&[
                ResultValue::Varchar("dc1".into()),
                ResultValue::Int(9044),
                ResultValue::Varchar("West".into()),
            ]],
        )
        .await;

        assert_query_result(
            &rewrite_port_connection,
            "SELECT native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9044)]],
        )
        .await;

        assert_query_result(
            &rewrite_port_connection,
            "SELECT native_port as foo FROM system.peers_v2;",
            &[&[ResultValue::Int(9044)]],
        )
        .await;

        assert_query_result(
            &rewrite_port_connection,
            "SELECT native_port, native_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9044), ResultValue::Int(9044)]],
        )
        .await;

        assert_query_result(
            &rewrite_port_connection,
            "SELECT native_port, native_port as some_port FROM system.peers_v2;",
            &[&[ResultValue::Int(9044), ResultValue::Int(9044)]],
        )
        .await;

        let result = rewrite_port_connection
            .execute("SELECT * FROM system.peers_v2;")
            .await;
        assert_eq!(result[0][5], ResultValue::Int(9044));
    }
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_peers_rewrite_cassandra3(#[case] driver: CassandraDriver) {
    let _docker_compose = DockerCompose::new(
        "tests/test-configs/cassandra-peers-rewrite/docker-compose-3.11-cassandra.yaml",
    );

    let _shotover_manager = ShotoverManager::from_topology_file(
        "tests/test-configs/cassandra-peers-rewrite/topology.yaml",
    );

    let connection = CassandraConnection::new("127.0.0.1", 9044, driver).await;
    // run some basic tests to confirm it works as normal
    table::test(&connection).await;

    // Assert that the error cassandra gives because system.peers_v2 does not exist on cassandra v3
    // is passed through shotover unchanged.
    let statement = "SELECT data_center, native_port, rack FROM system.peers_v2;";
    let result = connection.execute_expect_err(statement);
    assert_eq!(
        result,
        CassandraError {
            code: CassandraErrorCode::InvalidQuery,
            message: "unconfigured table peers_v2".into()
        }
    );
}

#[cfg(feature = "cassandra-cpp-driver-tests")]
#[rstest]
//#[case(CdrsTokio)] // TODO
#[cfg_attr(feature = "cassandra-cpp-driver-tests", case(Datastax))]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cassandra_request_throttling(#[case] driver: CassandraDriver) {
    let _docker_compose =
        DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yml");

    let _shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/cassandra-request-throttling.yaml");

    let connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;
    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window and not trigger the rate limiter with client's startup reqeusts
    let connection_2 = CassandraConnection::new("127.0.0.1", 9042, driver).await;
    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window again

    let statement = "SELECT * FROM system.peers";

    // these should all be let through the request throttling
    {
        let mut futures = vec![];
        for _ in 0..25 {
            futures.push(connection.execute_async(statement));
            futures.push(connection_2.execute_async(statement));
        }
        try_join_all(futures).await.unwrap();
    }

    // sleep to reset the window
    std::thread::sleep(std::time::Duration::from_secs(1));

    // only around half of these should be let through the request throttling
    {
        let mut futures = vec![];
        for _ in 0..50 {
            futures.push(connection.execute_async(statement));
            futures.push(connection_2.execute_async(statement));
        }
        let mut results = join_all(futures).await;
        results.retain(|result| match result {
            Ok(_) => true,
            Err(Error(
                ErrorKind::CassErrorResult(cassandra_cpp::CassErrorCode::SERVER_OVERLOADED, ..),
                _,
            )) => false,
            Err(e) => panic!(
                "wrong error returned, got {:?}, expected SERVER_OVERLOADED",
                e
            ),
        });

        let len = results.len();
        assert!(50 < len && len <= 60, "got {len}");
    }

    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window

    // setup keyspace and table for the batch statement tests
    {
        run_query(&connection, "CREATE KEYSPACE test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
        run_query(&connection, "CREATE TABLE test_keyspace.my_table (id int PRIMARY KEY, lastname text, firstname text);").await;
    }

    // this batch set should be allowed through
    {
        let mut queries: Vec<String> = vec![];
        for i in 0..25 {
            queries.push(format!("INSERT INTO test_keyspace.my_table (id, lastname, firstname) VALUES ({}, 'text', 'text')", i));
        }
        connection.execute_batch(queries);
    }

    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window

    // this batch set should not be allowed through
    {
        let mut queries: Vec<String> = vec![];
        for i in 0..60 {
            queries.push(format!("INSERT INTO test_keyspace.my_table (id, lastname, firstname) VALUES ({}, 'text', 'text')", i));
        }
        let result = connection.execute_batch_expect_err(queries);
        assert_eq!(
            result,
            CassandraError {
                code: CassandraErrorCode::ServerOverloaded,
                message: "Server overloaded".into()
            }
        );
    }

    std::thread::sleep(std::time::Duration::from_secs(1)); // sleep to reset the window

    batch_statements::test(&connection).await;
}

#[rstest]
#[case(CdrsTokio)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_events_keyspace(#[case] driver: CassandraDriver) {
    let _docker_compose =
        DockerCompose::new("example-configs/cassandra-passthrough/docker-compose.yml");

    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-passthrough/topology.yaml");

    let connection = CassandraConnection::new("127.0.0.1", 9042, driver).await;

    let mut event_recv = connection.as_cdrs().create_event_receiver();

    sleep(Duration::from_secs(10)).await; // let the driver finish connecting to the cluster and registering for the events

    let create_ks = "CREATE KEYSPACE IF NOT EXISTS test_events_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    connection.execute(create_ks).await;

    let event = timeout(Duration::from_secs(10), event_recv.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        event,
        ServerEvent::SchemaChange(SchemaChange {
            change_type: SchemaChangeType::Created,
            target: SchemaChangeTarget::Keyspace,
            options: SchemaChangeOptions::Keyspace("test_events_ks".to_string())
        })
    );
}

// TODO: could integrate with test_cluster_single_rack_v4 by taking ownership of the DockerCompose
#[rstest]
#[case(CdrsTokio)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cluster_events_filtering(#[case] driver: CassandraDriver) {
    let _compose =
        DockerCompose::new("example-configs/cassandra-cluster/docker-compose-cassandra-v4.yml");
    let _shotover_manager =
        ShotoverManager::from_topology_file("example-configs/cassandra-cluster/topology-v4.yaml");

    let session_direct = CassandraConnection::new("172.16.1.2", 9042, driver).await;
    let mut event_recv_direct = session_direct.as_cdrs().create_event_receiver();

    let session_shotover = CassandraConnection::new("127.0.0.1", 9042, driver).await;
    let mut event_recv_shotover = session_shotover.as_cdrs().create_event_receiver();

    // let the driver finish connecting to the cluster and registering for the events
    sleep(Duration::from_secs(10)).await;

    // stop one of the containers to trigger a status change event
    let docker = Docker::new("unix:///var/run/docker.sock").unwrap();
    let containers = docker.containers();
    let mut found = false;
    for container in containers.list(&Default::default()).await.unwrap() {
        let names = container.names.unwrap();
        // session_direct is connecting to instance one.
        // So make sure to instead kill instance two.
        if names.len() == 1 && names[0] == "/cassandra-cluster-cassandra-two-1" {
            found = true;
            let container = containers.get(container.id.unwrap());
            container.stop(None).await.unwrap();
        }
    }
    assert!(found, "container was not found with expected name");

    // The direct connection should allow all events to pass through
    tracing::error!("TODO: Everything works fine up to here");
    let event = timeout(Duration::from_secs(120), event_recv_direct.recv())
        .await
        .unwrap()
        .unwrap();
    tracing::error!(
        "TODO: But we never reach this line, I think cdrs-tokio's event handling is broken."
    );

    assert!(
        matches!(
            event,
            ServerEvent::StatusChange(StatusChange {
                change_type: StatusChangeType::Down,
                ..
            })
        ),
        "expected status change down but was {:?}",
        event
    );

    // we have already received an event directly from the cassandra instance so its reasonable to
    // expect shotover to have processed that event within 10 seconds if it was ever going to
    timeout(Duration::from_secs(10), event_recv_shotover.recv())
        .await
        .expect_err("CassandraSinkClustser must filter out this event");
}
