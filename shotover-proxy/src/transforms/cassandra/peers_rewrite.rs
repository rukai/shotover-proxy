use crate::frame::{CassandraOperation, CassandraResult, Frame};
use crate::message::{IntSize, Message, MessageValue};
use crate::{
    error::ChainResponse,
    transforms::{Transform, Transforms, Wrapper},
};
use anyhow::Result;
use async_trait::async_trait;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::FQName;
use cql3_parser::select::SelectElement;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraPeersRewriteConfig {
    pub port: u32,
}

impl CassandraPeersRewriteConfig {
    pub async fn get_transform(&self) -> Result<Transforms> {
        Ok(Transforms::CassandraPeersRewrite(
            CassandraPeersRewrite::new(self.port),
        ))
    }
}

#[derive(Clone)]
pub struct CassandraPeersRewrite {
    port: u32,
    peer_table: FQName,
}

impl CassandraPeersRewrite {
    pub fn new(port: u32) -> Self {
        CassandraPeersRewrite {
            port,
            peer_table: FQName::new("system", "peers_v2"),
        }
    }
}

#[async_trait]
impl Transform for CassandraPeersRewrite {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        // Find the indices of queries to system.peers & system.peers_v2
        // we need to know which columns in which CQL queries in which messages have system peers
        let column_names: HashMap<usize, Vec<String>> = message_wrapper
            .messages
            .iter_mut()
            .enumerate()
            .filter_map(|(i, m)| {
                let sys_peers = extract_native_port_column(&self.peer_table, m);
                if sys_peers.is_empty() {
                    None
                } else {
                    Some((i, sys_peers))
                }
            })
            .collect();

        let mut response = message_wrapper.call_next_transform().await?;

        for (idx, name_list) in column_names {
            rewrite_port(&mut response[idx], &name_list, self.port);
        }

        Ok(response)
    }
}

/// determine if the message contains a SELECT from `system.peers_v2` that includes the `native_port` column
/// return a list of column names (or their alias) for each `native_port`.
fn extract_native_port_column(peer_table: &FQName, message: &mut Message) -> Vec<String> {
    let mut result: Vec<String> = vec![];
    if let Some(Frame::Cassandra(cassandra)) = message.frame() {
        if let CassandraOperation::Query { query, .. } = &cassandra.operation {
            for cql_statement in &query.statements {
                let statement = &cql_statement.statement;
                if let CassandraStatement::Select(select) = &statement {
                    if peer_table == &select.table_name {
                        for select_element in &select.columns {
                            match select_element {
                                SelectElement::Column(col_name) => {
                                    if col_name.name == "native_port" {
                                        result.push(col_name.alias_or_name().to_string());
                                    }
                                }
                                SelectElement::Star => result.push("native_port".to_string()),
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }
    result
}

/// Rewrite the `native_port` field in the results from a query to `system.peers_v2` table
/// Only Cassandra queries to the `system.peers` table found via the `is_system_peers` function should be passed to this
fn rewrite_port(message: &mut Message, column_names: &[String], new_port: u32) {
    if let Some(Frame::Cassandra(frame)) = message.frame() {
        if let CassandraOperation::Result(CassandraResult::Rows { value, metadata }) =
            &mut frame.operation
        {
            let port_column_index: Vec<usize> = metadata
                .col_specs
                .iter()
                .enumerate()
                .filter_map(|(idx, col)| {
                    if column_names.contains(&col.name) {
                        Some(idx)
                    } else {
                        None
                    }
                })
                .collect();

            if let MessageValue::Rows(rows) = value {
                for row in rows.iter_mut() {
                    for idx in &port_column_index {
                        row[*idx] = MessageValue::Integer(new_port as i64, IntSize::I32);
                    }
                }
                message.invalidate_cache();
            }
        } else {
            panic!(
                "Expected CassandraOperation::Result(CassandraResult::Rows), got {:?}",
                frame
            );
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frame::{CassandraFrame, CQL};
    use crate::transforms::cassandra::peers_rewrite::CassandraResult::Rows;
    use cassandra_protocol::frame::frame_result::ColType;
    use cassandra_protocol::{
        consistency::Consistency,
        frame::{
            frame_result::{
                ColSpec,
                ColType::{Inet, Int},
                ColTypeOption, RowsMetadata, RowsMetadataFlags, TableSpec,
            },
            Version,
        },
        query::QueryParams,
    };

    fn create_query_message(query: &str) -> Message {
        let original = Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: CQL::parse_from_string(query),
                params: QueryParams {
                    consistency: Consistency::One,
                    with_names: false,
                    values: None,
                    page_size: Some(5000),
                    paging_state: None,
                    serial_consistency: None,
                    timestamp: Some(1643855761086585),
                },
            },
        });

        Message::from_frame(original)
    }

    fn create_response_message(col_specs: &[ColSpec], rows: Vec<Vec<MessageValue>>) -> Message {
        let original = Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Result(Rows {
                value: MessageValue::Rows(rows),
                metadata: RowsMetadata {
                    flags: RowsMetadataFlags::GLOBAL_TABLE_SPACE,
                    columns_count: 1,
                    paging_state: None,
                    global_table_spec: Some(TableSpec {
                        ks_name: "system".into(),
                        table_name: "peers_v2".into(),
                    }),
                    col_specs: col_specs.to_owned(),
                },
            }),
        });

        Message::from_frame(original)
    }

    #[test]
    fn test_is_system_peers_v2() {
        let peer_table = FQName::new("system", "peers_v2");
        let v = extract_native_port_column(
            &peer_table,
            &mut create_query_message("SELECT * FROM system.peers_v2;"),
        );
        assert_eq!(vec!("native_port".to_string()), v);

        let v = extract_native_port_column(
            &peer_table,
            &mut create_query_message("SELECT * FROM not_system.peers_v2;"),
        );
        assert!(v.is_empty());

        let v = extract_native_port_column(
            &peer_table,
            &mut create_query_message("SELECT native_port as foo from system.peers_v2"),
        );
        assert_eq!(vec!("foo".to_string()), v);

        let v = extract_native_port_column(
            &peer_table,
            &mut create_query_message(
                "SELECT native_port as foo, native_port from system.peers_v2",
            ),
        );
        assert_eq!(vec!["foo".to_string(), "native_port".to_string()], v);
    }

    #[test]
    fn test_simple_rewrite_port() {
        //Test rewrites `native_port` column when included

        let col_spec = vec![ColSpec {
            table_spec: None,
            name: "native_port".into(),
            col_type: ColTypeOption {
                id: Int,
                value: None,
            },
        }];
        let mut message = create_response_message(
            &col_spec,
            vec![
                vec![MessageValue::Integer(9042, IntSize::I32)],
                vec![MessageValue::Integer(9042, IntSize::I32)],
            ],
        );

        rewrite_port(&mut message, &["native_port".to_string()], 9043);

        let expected = create_response_message(
            &col_spec,
            vec![
                vec![MessageValue::Integer(9043, IntSize::I32)],
                vec![MessageValue::Integer(9043, IntSize::I32)],
            ],
        );

        assert_eq!(message, expected);
    }

    #[test]
    fn test_simple_rewrite_port_no_match() {
        let col_spec = vec![ColSpec {
            table_spec: None,
            name: "peer".into(),
            col_type: ColTypeOption {
                id: Inet,
                value: None,
            },
        }];

        let mut original = create_response_message(
            &col_spec,
            vec![
                vec![MessageValue::Inet("127.0.0.1".parse().unwrap())],
                vec![MessageValue::Inet("10.123.56.1".parse().unwrap())],
            ],
        );

        let expected = create_response_message(
            &col_spec,
            vec![
                vec![MessageValue::Inet("127.0.0.1".parse().unwrap())],
                vec![MessageValue::Inet("10.123.56.1".parse().unwrap())],
            ],
        );

        rewrite_port(
            &mut original,
            &["native_port".to_string(), "alias_port".to_string()],
            9043,
        );

        assert_eq!(original, expected);
    }

    #[test]
    fn test_alias_rewrite_port() {
        let col_spec = vec![
            ColSpec {
                table_spec: None,
                name: "native_port".into(),
                col_type: ColTypeOption {
                    id: Int,
                    value: None,
                },
            },
            ColSpec {
                table_spec: None,
                name: "some_text".into(),
                col_type: ColTypeOption {
                    id: ColType::Varchar,
                    value: None,
                },
            },
            ColSpec {
                table_spec: None,
                name: "alias_port".into(),
                col_type: ColTypeOption {
                    id: Int,
                    value: None,
                },
            },
        ];

        let mut original = create_response_message(
            &col_spec,
            vec![
                vec![
                    MessageValue::Integer(9042, IntSize::I32),
                    MessageValue::Strings("Hello".into()),
                    MessageValue::Integer(9042, IntSize::I32),
                ],
                vec![
                    MessageValue::Integer(9042, IntSize::I32),
                    MessageValue::Strings("World".into()),
                    MessageValue::Integer(9042, IntSize::I32),
                ],
            ],
        );

        let expected = create_response_message(
            &col_spec,
            vec![
                vec![
                    MessageValue::Integer(9043, IntSize::I32),
                    MessageValue::Strings("Hello".into()),
                    MessageValue::Integer(9043, IntSize::I32),
                ],
                vec![
                    MessageValue::Integer(9043, IntSize::I32),
                    MessageValue::Strings("World".into()),
                    MessageValue::Integer(9043, IntSize::I32),
                ],
            ],
        );

        rewrite_port(
            &mut original,
            &["native_port".to_string(), "alias_port".to_string()],
            9043,
        );

        assert_eq!(original, expected);
    }
}
