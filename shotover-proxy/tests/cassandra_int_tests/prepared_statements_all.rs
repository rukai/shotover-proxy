use crate::helpers::cassandra::{assert_query_result, run_query, CassandraConnection, ResultValue};

fn values() -> Vec<ResultValue> {
    vec![
        ResultValue::Int(1),
        ResultValue::Ascii("foo".to_owned()),
        ResultValue::BigInt(7834298),
        ResultValue::Blob(vec![1, 2, 3]),
        ResultValue::Boolean(true),
        ResultValue::Decimal(vec![0, 0, 0, 1, 10]),
        ResultValue::Double(1.2.into()),
        ResultValue::Float(3.4.into()),
        ResultValue::Timestamp(1111),
        ResultValue::Uuid("b684e6d6-8172-4d4d-a7e4-c87b780cba8f".parse().unwrap()),
        ResultValue::Inet("127.0.0.20".parse().unwrap()),
        ResultValue::Date(3),
        ResultValue::Time(5),
        ResultValue::SmallInt(1),
        ResultValue::TinyInt(2),
    ]
}

async fn insert(connection: &CassandraConnection) {
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    let datastax = matches!(connection, CassandraConnection::Datastax { .. });
    #[cfg(not(feature = "cassandra-cpp-driver-tests"))]
    let datastax = false;

    if datastax {
        // workaround cassandra-cpp not yet supporting binding decimal values
        let prepared = connection
                .prepare("INSERT INTO test_prepare_statements_all.test (id, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13) VALUES (?, ?, ?, ?, ?, 1.0, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
                .await;

        assert_eq!(
            connection
                .execute_prepared(
                    &prepared,
                    &[
                        ResultValue::Int(1),
                        ResultValue::Ascii("foo".to_owned()),
                        ResultValue::BigInt(7834298),
                        ResultValue::Blob(vec![1, 2, 3]),
                        ResultValue::Boolean(true),
                        // ResultValue::Decimal - Decimal handled in the query string
                        ResultValue::Double(1.2.into()),
                        ResultValue::Float(3.4.into()),
                        ResultValue::Timestamp(1111),
                        ResultValue::Uuid("b684e6d6-8172-4d4d-a7e4-c87b780cba8f".parse().unwrap()),
                        ResultValue::Inet("127.0.0.20".parse().unwrap()),
                        ResultValue::Date(3),
                        ResultValue::Time(5),
                        ResultValue::SmallInt(1),
                        ResultValue::TinyInt(2),
                    ]
                )
                .await,
            Ok(Vec::<Vec<_>>::new())
        );
    } else {
        let prepared = connection
                .prepare("INSERT INTO test_prepare_statements_all.test (id, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
                .await;
        assert_eq!(
            connection.execute_prepared(&prepared, &values()).await,
            Ok(Vec::<Vec<_>>::new())
        );
    }
}

async fn select(connection: &CassandraConnection) {
    if let CassandraConnection::CdrsTokio { .. } = connection {
        // workaround cdrs-tokio having broken encoding for bytes
        assert_query_result(
            connection,
            "SELECT id, v0, v1, v3, v5, v6, v7, v8, v9, v10, v11, v12, v13 FROM test_prepare_statements_all.test WHERE id = 1",
            &[&[
                ResultValue::Int(1),
                ResultValue::Ascii("foo".to_owned()),
                ResultValue::BigInt(7834298),
                //ResultValue::Blob - Not queried
                ResultValue::Boolean(true),
                // ResultValue::Decimal - Not queried
                ResultValue::Double(1.2.into()),
                ResultValue::Float(3.4.into()),
                ResultValue::Timestamp(1111),
                ResultValue::Uuid("b684e6d6-8172-4d4d-a7e4-c87b780cba8f".parse().unwrap()),
                ResultValue::Inet("127.0.0.20".parse().unwrap()),
                ResultValue::Date(3),
                ResultValue::Time(5),
                ResultValue::SmallInt(1),
                ResultValue::TinyInt(2),
            ]]
        )
        .await;
    } else {
        assert_query_result(
            connection,
            "SELECT id, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13 FROM test_prepare_statements_all.test WHERE id = 1",
            &[&values()],
        )
        .await;
    }
}

pub async fn test(connection: &CassandraConnection) {
    run_query(connection, "CREATE KEYSPACE test_prepare_statements_all WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    run_query(
        connection,
        "CREATE TABLE test_prepare_statements_all.test (id int PRIMARY KEY, v0 ascii, v1 bigint, v2 blob, v3 boolean, v4 decimal, v5 double, v6 float, v7 timestamp, v8 uuid, v9 inet, v10 date, v11 time, v12 smallint, v13 tinyint);",
    )
    .await;

    insert(connection).await;
    select(connection).await;
}
