mod test_cases;

use crate::shotover_process;
use rstest::rstest;
use std::time::Duration;
use test_helpers::connection::kafka::{KafkaConnectionBuilder, KafkaDriver};
use test_helpers::docker_compose::docker_compose;

#[rstest]
#[cfg_attr(feature = "rdkafka-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_standard(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::standard_test_suite(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[rstest]
#[cfg_attr(feature = "rdkafka-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_tls(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-tls/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough-tls/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::standard_test_suite(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[rstest]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_tls(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-tls/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/cluster-tls/topology.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192")
        .use_tls("tests/test-configs/kafka/tls/certs/kafka.keystore.jks");
    test_cases::standard_test_suite(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[cfg_attr(feature = "rdkafka-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_encode(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough/topology-encode.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::standard_test_suite(connection_builder).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
#[cfg_attr(feature = "rdkafka-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_sasl_plain(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-sasl/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/passthrough-sasl/topology.yaml")
        .start()
        .await;

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_plain("user", "password");
    test_cases::standard_test_suite(connection_builder).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[cfg(feature = "alpha-transforms")]
#[rstest]
#[cfg_attr(feature = "rdkafka-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_sasl_plain_encode(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-sasl/docker-compose.yaml");
    let shotover =
        shotover_process("tests/test-configs/kafka/passthrough-sasl/topology-encode.yaml")
            .start()
            .await;

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_plain("user", "password");
    test_cases::standard_test_suite(connection_builder).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[rstest]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn passthrough_sasl_scram(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose =
        docker_compose("tests/test-configs/kafka/passthrough-sasl-scram/docker-compose.yaml");
    let shotover =
        shotover_process("tests/test-configs/kafka/passthrough-sasl-scram/topology.yaml")
            .start()
            .await;

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_scram("user", "password");
    test_cases::standard_test_suite(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

/// This test demonstrates that kafka's SASL SCRAM implementation does not perform channel binding.
/// https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism#Channel_binding
/// This is shown because we have the client performing SCRAM over a plaintext connection while the broker is receiving SCRAM over a TLS connection.
#[rstest]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn single_sasl_scram_plaintext_source_tls_sink(#[case] driver: KafkaDriver) {
    test_helpers::cert::generate_kafka_test_certs();

    let _docker_compose = docker_compose(
        "tests/test-configs/kafka/single-sasl-scram-plaintext-source-tls-sink/docker-compose.yaml",
    );
    let shotover = shotover_process(
        "tests/test-configs/kafka/single-sasl-scram-plaintext-source-tls-sink/topology.yaml",
    )
    .start()
    .await;

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_scram("user", "password");
    test_cases::standard_test_suite(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[rstest]
#[cfg_attr(feature = "rdkafka-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_1_rack_single_shotover(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-1-rack/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/kafka/cluster-1-rack/topology-single.yaml")
        .start()
        .await;

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::standard_test_suite(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[cfg(feature = "rdkafka-driver-tests")] // temporarily needed to avoid a warning
#[rstest]
#[cfg_attr(feature = "rdkafka-driver-tests", case::cpp(KafkaDriver::Cpp))]
//#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_1_rack_multi_shotover(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-1-rack/docker-compose.yaml");
    let mut shotovers = vec![];
    for i in 1..4 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster-1-rack/topology{i}.yaml"
            ))
            .with_config(&format!(
                "tests/test-configs/shotover-config/config{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .start()
            .await,
        );
    }

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::standard_test_suite(connection_builder).await;

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[cfg(feature = "rdkafka-driver-tests")] // temporarily needed to avoid a warning
#[rstest]
#[cfg_attr(feature = "rdkafka-driver-tests", case::cpp(KafkaDriver::Cpp))]
//#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_2_racks_multi_shotover(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-2-racks/docker-compose.yaml");

    // One shotover instance per rack
    let mut shotovers = vec![];
    for i in 1..3 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster-2-racks/topology-rack{i}.yaml"
            ))
            .with_config(&format!(
                "tests/test-configs/shotover-config/config{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .start()
            .await,
        );
    }

    let connection_builder = KafkaConnectionBuilder::new(driver, "127.0.0.1:9192");
    test_cases::standard_test_suite(connection_builder).await;

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}

#[rstest]
#[cfg_attr(feature = "rdkafka-driver-tests", case::cpp(KafkaDriver::Cpp))]
#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_sasl_plain_single_shotover(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-sasl-plain-single/docker-compose.yaml");

    let shotover =
        shotover_process("tests/test-configs/kafka/cluster-sasl-plain-single/topology-single.yaml")
            .start()
            .await;

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_plain("user", "password");
    test_cases::standard_test_suite(connection_builder).await;

    tokio::time::timeout(
        Duration::from_secs(10),
        shotover.shutdown_and_then_consume_events(&[]),
    )
    .await
    .expect("Shotover did not shutdown within 10s");
}

#[cfg(feature = "rdkafka-driver-tests")] // temporarily needed to avoid a warning
#[rstest]
#[cfg_attr(feature = "rdkafka-driver-tests", case::cpp(KafkaDriver::Cpp))]
//#[case::java(KafkaDriver::Java)]
#[tokio::test(flavor = "multi_thread")] // multi_thread is needed since java driver will block when consuming, causing shotover logs to not appear
async fn cluster_sasl_plain_multi_shotover(#[case] driver: KafkaDriver) {
    let _docker_compose =
        docker_compose("tests/test-configs/kafka/cluster-sasl-plain/docker-compose.yaml");
    let mut shotovers = vec![];
    for i in 1..4 {
        shotovers.push(
            shotover_process(&format!(
                "tests/test-configs/kafka/cluster-sasl-plain/topology{i}.yaml"
            ))
            .with_config(&format!(
                "tests/test-configs/shotover-config/config{i}.yaml"
            ))
            .with_log_name(&format!("shotover{i}"))
            .start()
            .await,
        );
    }

    let connection_builder =
        KafkaConnectionBuilder::new(driver, "127.0.0.1:9192").use_sasl_plain("user", "password");
    test_cases::standard_test_suite(connection_builder).await;

    for shotover in shotovers {
        tokio::time::timeout(
            Duration::from_secs(10),
            shotover.shutdown_and_then_consume_events(&[]),
        )
        .await
        .expect("Shotover did not shutdown within 10s");
    }
}
