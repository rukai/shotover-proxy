use super::node::ConnectionFactory;
use super::node_pool::NodePool;
use super::ShotoverNode;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{Message, Messages};
use crate::{
    frame::{
        cassandra::{parse_statement_single, Tracing},
        value::{GenericValue, IntSize},
    },
    message::MessageId,
};
use anyhow::{anyhow, Result};
use cassandra_protocol::frame::message_result::BodyResResultPrepared;
use cassandra_protocol::frame::Version;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{
    FQNameRef, Identifier, IdentifierRef, Operand, RelationElement, RelationOperator,
};
use cql3_parser::select::{Select, SelectElement};
use futures::future::try_join_all;
use itertools::Itertools;
use std::collections::HashMap;
use std::fmt::Write;
use std::net::{IpAddr, Ipv4Addr};
use uuid::Uuid;
use version_compare::Cmp;

const LOCAL_TABLE: FQNameRef = FQNameRef {
    keyspace: Some(IdentifierRef::Quoted("system")),
    name: IdentifierRef::Quoted("local"),
};
const PEERS_TABLE: FQNameRef = FQNameRef {
    keyspace: Some(IdentifierRef::Quoted("system")),
    name: IdentifierRef::Quoted("peers"),
};
const PEERS_V2_TABLE: FQNameRef = FQNameRef {
    keyspace: Some(IdentifierRef::Quoted("system")),
    name: IdentifierRef::Quoted("peers_v2"),
};

/// The MessageRewriter rewrites requests, takes note of what it has rewritten and then uses those notes to rewrite the responses when they come back.
/// This looks something like:
/// ```compile_fail
/// fn transform_logic(&mut self, requests: Vec<Message>) {
///     let responses_to_rewrite = self.rewriter.rewrite_requests(requests, ..)?;
///
///     let responses = send_receive(requests)?;
///
///     self.rewriter.rewrite_responses(responses, responses_to_rewrite, ..)?;
///
///     Ok(responses)
/// }
/// ```
#[derive(Clone)]
pub struct MessageRewriter {
    pub shotover_peers: Vec<ShotoverNode>,
    pub local_shotover_node: ShotoverNode,
    pub to_rewrite: Vec<TableToRewrite>,
    pub prepare_requests_to_destination_nodes: HashMap<MessageId, Uuid>,
}

impl MessageRewriter {
    /// Insert any extra requests into requests.
    /// A Vec<TableToRewrite> is returned which keeps track of the requests added
    /// so that rewrite_responses can restore the responses received into the responses expected by the client
    pub async fn rewrite_requests(
        &mut self,
        messages: &mut Vec<Message>,
        connection_factory: &ConnectionFactory,
        pool: &mut NodePool,
        version: Version,
    ) -> Result<()> {
        for (i, message) in messages.iter_mut().enumerate() {
            if let Some(rewrite) = self.get_rewrite_table(i, message) {
                self.to_rewrite.push(rewrite);
            }
        }

        // Insert the extra messages required by table rewrites.
        for table_to_rewrite in &mut self.to_rewrite {
            match &table_to_rewrite.ty {
                RewriteTableTy::Local => {
                    let query = "SELECT rack, data_center, schema_version, tokens, release_version FROM system.peers";
                    let message = create_query(messages, query, version)?;
                    table_to_rewrite
                        .collected_messages
                        .push(MessageOrId::Id(message.id()));
                    messages.push(message);
                }
                RewriteTableTy::Peers => {
                    let query = "SELECT rack, data_center, schema_version, tokens, release_version FROM system.peers";
                    let message = create_query(messages, query, version)?;
                    table_to_rewrite
                        .collected_messages
                        .push(MessageOrId::Id(message.id()));
                    messages.push(message);

                    let query = "SELECT rack, data_center, schema_version, tokens, release_version FROM system.local";
                    let message = create_query(messages, query, version)?;
                    table_to_rewrite
                        .collected_messages
                        .push(MessageOrId::Id(message.id()));
                    messages.push(message);
                }
                RewriteTableTy::Prepare { clone_index } => {
                    let mut first = true;
                    for node in pool.nodes().iter() {
                        if node.is_up && node.rack == self.local_shotover_node.rack {
                            if first {
                                let message_id = messages[*clone_index].id();
                                self.prepare_requests_to_destination_nodes
                                    .insert(message_id, node.host_id);
                            } else {
                                let message = messages[*clone_index].clone_with_new_id();
                                self.prepare_requests_to_destination_nodes
                                    .insert(message.id(), node.host_id);
                                table_to_rewrite
                                    .collected_messages
                                    .push(MessageOrId::Id(message.id()));
                                messages.push(message);
                            }
                            first = false;
                        }
                    }

                    // This is purely an optimization: To avoid opening these connections sequentially later on, we open them concurrently now.
                    try_join_all(
                        pool.nodes_mut()
                            .iter_mut()
                            .filter(|x| {
                                self.prepare_requests_to_destination_nodes
                                    .values()
                                    .any(|y| y == &x.host_id)
                            })
                            .map(|node| node.get_connection(connection_factory)),
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    /// Returns any information required to correctly rewrite the response.
    /// Will also perform minor modifications to the query required for the rewrite.
    fn get_rewrite_table(
        &self,
        request_index: usize,
        request: &mut Message,
    ) -> Option<TableToRewrite> {
        let request_id = request.id();
        if let Some(Frame::Cassandra(cassandra)) = request.frame() {
            // No need to handle Batch as selects can only occur on Query
            match &mut cassandra.operation {
                CassandraOperation::Query { query, .. } => {
                    if let CassandraStatement::Select(select) = query.as_mut() {
                        let ty = if LOCAL_TABLE == select.table_name {
                            RewriteTableTy::Local
                        } else if PEERS_TABLE == select.table_name
                            || PEERS_V2_TABLE == select.table_name
                        {
                            RewriteTableTy::Peers
                        } else {
                            return None;
                        };

                        let warnings = if has_no_where_clause(&ty, select) {
                            vec![]
                        } else {
                            select.where_clause.clear();
                            vec![format!(
                            "WHERE clause on the query was ignored. Shotover does not support WHERE clauses on queries against {}",
                            select.table_name
                        )]
                        };

                        return Some(TableToRewrite {
                            collected_messages: vec![MessageOrId::Id(request_id)],
                            ty,
                            warnings,
                            selects: select.columns.clone(),
                        });
                    }
                }
                CassandraOperation::Prepare(_) => {
                    return Some(TableToRewrite {
                        collected_messages: vec![MessageOrId::Id(request_id)],
                        ty: RewriteTableTy::Prepare {
                            clone_index: request_index,
                        },
                        warnings: vec![],
                        selects: vec![],
                    });
                }
                _ => {}
            }
        }
        None
    }

    pub fn get_destination_for_prepare(&mut self, message: &Message) -> Uuid {
        if let Some(node) = self
            .prepare_requests_to_destination_nodes
            .remove(&message.id())
        {
            return node;
        }

        unreachable!()
    }

    /// Rewrite responses using the `Vec<TableToRewrite>` returned by rewrite_requests.
    /// All extra responses are combined back into the amount of responses expected by the client.
    pub fn rewrite_responses(&mut self, responses: &mut Vec<Message>) -> Result<()> {
        for mut table_to_rewrite in std::mem::take(&mut self.to_rewrite) {
            if !self.rewrite_response(&mut table_to_rewrite, responses)? {
                self.to_rewrite.push(table_to_rewrite);
            }
        }
        Ok(())
    }

    /// Returns true when the TableToRewrite is completed and should be destroyed.
    fn rewrite_response(
        &self,
        table: &mut TableToRewrite,
        responses: &mut Vec<Message>,
    ) -> Result<bool> {
        fn get_warnings(message: &mut Message) -> Vec<String> {
            if let Some(Frame::Cassandra(frame)) = message.frame() {
                frame.warnings.clone()
            } else {
                vec![]
            }
        }

        for message_or_id in table.collected_messages.iter_mut() {
            let MessageOrId::Id(id) = message_or_id else {
                break;
            };
            if let Some(i) = responses.iter().position(|x| x.request_id() == Some(*id)) {
                *message_or_id = MessageOrId::Message(responses.swap_remove(i));
            }
        }

        if table
            .collected_messages
            .iter()
            .all(|x| matches!(x, MessageOrId::Message(_)))
        {
            // We have all the messages we need now.
            // Consume the TableToRewrite and return the resulting message to the client.

            let mut collected_messages: Vec<Message> = table
                .collected_messages
                .drain(..)
                .map(|x| match x {
                    MessageOrId::Message(m) => m,
                    MessageOrId::Id(_) => unreachable!(),
                })
                .collect();

            match &table.ty {
                RewriteTableTy::Local => {
                    let mut peers_response = collected_messages.pop().unwrap();
                    let mut local_response = collected_messages.pop().unwrap();
                    // Include warnings from every message that gets combined into the final message + any extra warnings noted in the TableToRewrite
                    let mut warnings = table.warnings.clone();
                    warnings.extend(get_warnings(&mut peers_response));
                    warnings.extend(get_warnings(&mut local_response));

                    self.rewrite_table_local(table, &mut local_response, peers_response, warnings)?;
                    local_response.invalidate_cache();
                    responses.push(local_response);
                }
                RewriteTableTy::Peers => {
                    let mut peers_response = collected_messages.pop().unwrap();
                    let mut local_response = collected_messages.pop().unwrap();
                    let mut client_peers_response = collected_messages.pop().unwrap();
                    // Include warnings from every message that gets combined into the final message + any extra warnings noted in the TableToRewrite
                    let mut warnings = table.warnings.clone();
                    warnings.extend(get_warnings(&mut peers_response));
                    warnings.extend(get_warnings(&mut local_response));
                    warnings.extend(get_warnings(&mut client_peers_response));

                    let mut nodes = match parse_system_nodes(peers_response) {
                        Ok(x) => x,
                        Err(MessageParseError::CassandraError(err)) => {
                            client_peers_response = client_peers_response
                                        .to_error_response(format!("{:?}", err.context("Shotover failed to generate system.peers, an error occured when an internal system.peers query was run.")))
                                        .expect("This must succeed because it is a cassandra message and we already know it to be parseable");
                            responses.push(client_peers_response);
                            return Ok(true);
                        }
                        Err(MessageParseError::ParseFailure(err)) => return Err(err),
                    };
                    match parse_system_nodes(local_response) {
                        Ok(x) => nodes.extend(x),
                        Err(MessageParseError::CassandraError(err)) => {
                            client_peers_response = client_peers_response
                                        .to_error_response(format!("{:?}", err.context("Shotover failed to generate system.peers, an error occured when an internal system.local query was run.")))
                                        .expect("This must succeed because it is a cassandra message and we already know it to be parseable");
                            responses.push(client_peers_response);
                            return Ok(true);
                        }
                        Err(MessageParseError::ParseFailure(err)) => return Err(err),
                    };

                    self.rewrite_table_peers(table, &mut client_peers_response, nodes, warnings)?;
                    client_peers_response.invalidate_cache();
                    responses.push(client_peers_response);
                }
                RewriteTableTy::Prepare { .. } => {
                    let mut warnings: Vec<String> = collected_messages
                        .iter_mut()
                        .flat_map(get_warnings)
                        .collect();

                    {
                        let prepared_results: Vec<&mut Box<BodyResResultPrepared>> = collected_messages
                            .iter_mut()
                            .filter_map(|message| match message.frame() {
                                Some(Frame::Cassandra(CassandraFrame {
                                    operation:
                                        CassandraOperation::Result(CassandraResult::Prepared(prepared)),
                                    ..
                                })) => Some(prepared),
                                other => {
                                    tracing::error!("Response to Prepare query was not a Prepared, was instead: {other:?}");
                                    warnings.push(format!("Shotover: Response to Prepare query was not a Prepared, was instead: {other:?}"));
                                    None
                                }
                            })
                            .collect();
                        if !prepared_results.windows(2).all(|w| w[0] == w[1]) {
                            let err_str =
                                prepared_results
                                    .iter()
                                    .fold(String::new(), |mut output, b| {
                                        let _ = write!(output, "\n{b:?}");
                                        output
                                    });

                            tracing::error!(
                            "Nodes did not return the same response to PREPARE statement {err_str}"
                        );
                            warnings.push(format!(
                            "Shotover: Nodes did not return the same response to PREPARE statement {err_str}"
                        ));
                        }
                    }

                    // If there is a succesful response, use that as our response by moving it to the beginning
                    for (i, response) in collected_messages.iter_mut().enumerate() {
                        if let Some(Frame::Cassandra(CassandraFrame {
                            operation: CassandraOperation::Result(CassandraResult::Prepared(_)),
                            ..
                        })) = response.frame()
                        {
                            collected_messages.swap(0, i);
                            break;
                        }
                    }

                    // Finalize our response by setting its warnings list to all the warnings we have accumulated
                    if let Some(Frame::Cassandra(frame)) = collected_messages[0].frame() {
                        frame.warnings = warnings;
                    }
                    responses.push(collected_messages.remove(0));
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn rewrite_table_peers(
        &self,
        table: &TableToRewrite,
        peers_response: &mut Message,
        nodes: Vec<NodeInfo>,
        warnings: Vec<String>,
    ) -> Result<()> {
        let mut data_center_alias = "data_center";
        let mut rack_alias = "rack";
        let mut host_id_alias = "host_id";
        let mut native_address_alias = "native_address";
        let mut native_port_alias = "native_port";
        let mut preferred_ip_alias = "preferred_ip";
        let mut preferred_port_alias = "preferred_port";
        let mut rpc_address_alias = "rpc_address";
        let mut peer_alias = "peer";
        let mut peer_port_alias = "peer_port";
        let mut release_version_alias = "release_version";
        let mut tokens_alias = "tokens";
        let mut schema_version_alias = "schema_version";
        for select in &table.selects {
            if let SelectElement::Column(column) = select {
                if let Some(alias) = &column.alias {
                    let alias = match alias {
                        Identifier::Unquoted(alias) => alias,
                        Identifier::Quoted(alias) => alias,
                    };
                    if column.name == IdentifierRef::Quoted("data_center") {
                        data_center_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("rack") {
                        rack_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("host_id") {
                        host_id_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("native_address") {
                        native_address_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("native_port") {
                        native_port_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("preferred_ip") {
                        preferred_ip_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("preferred_port") {
                        preferred_port_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("rpc_address") {
                        rpc_address_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("peer") {
                        peer_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("peer_port") {
                        peer_port_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("release_version") {
                        release_version_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("tokens") {
                        tokens_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("schema_version") {
                        schema_version_alias = alias;
                    }
                }
            }
        }

        if let Some(Frame::Cassandra(frame)) = peers_response.frame() {
            frame.warnings = warnings;
            if let CassandraOperation::Result(CassandraResult::Rows { rows, metadata }) =
                &mut frame.operation
            {
                *rows = self
                    .shotover_peers
                    .iter()
                    .map(|shotover_peer| {
                        let mut release_version = "".to_string();
                        let mut schema_version = None;
                        let mut tokens = vec![];
                        for node in &nodes {
                            if node.data_center == shotover_peer.data_center
                                && node.rack == shotover_peer.rack
                            {
                                if release_version.is_empty() {
                                    release_version = node.release_version.clone();
                                }
                                if let Ok(Cmp::Lt) = version_compare::compare(
                                    &node.release_version,
                                    &release_version,
                                ) {
                                    release_version = node.release_version.clone();
                                }

                                match &mut schema_version {
                                    Some(schema_version) => {
                                        if &node.schema_version != schema_version {
                                            *schema_version = Uuid::new_v4();
                                        }
                                    }
                                    None => schema_version = Some(node.schema_version),
                                }
                                tokens.extend(node.tokens.iter().cloned());
                            }
                        }
                        tokens.sort();

                        metadata
                            .col_specs
                            .iter()
                            .map(|colspec| {
                                if colspec.name == data_center_alias {
                                    GenericValue::Varchar(shotover_peer.data_center.clone())
                                } else if colspec.name == rack_alias {
                                    GenericValue::Varchar(shotover_peer.rack.clone())
                                } else if colspec.name == host_id_alias {
                                    GenericValue::Uuid(shotover_peer.host_id)
                                } else if colspec.name == preferred_ip_alias
                                    || colspec.name == preferred_port_alias
                                {
                                    GenericValue::Null
                                } else if colspec.name == native_address_alias {
                                    GenericValue::Inet(shotover_peer.address.ip())
                                } else if colspec.name == native_port_alias {
                                    GenericValue::Integer(
                                        shotover_peer.address.port() as i64,
                                        IntSize::I32,
                                    )
                                } else if colspec.name == peer_alias
                                    || colspec.name == rpc_address_alias
                                {
                                    GenericValue::Inet(shotover_peer.address.ip())
                                } else if colspec.name == peer_port_alias {
                                    GenericValue::Integer(7000, IntSize::I32)
                                } else if colspec.name == release_version_alias {
                                    GenericValue::Varchar(release_version.clone())
                                } else if colspec.name == tokens_alias {
                                    GenericValue::List(tokens.clone())
                                } else if colspec.name == schema_version_alias {
                                    GenericValue::Uuid(schema_version.unwrap_or_else(Uuid::new_v4))
                                } else {
                                    tracing::warn!(
                                        "Unknown column name in system.peers/system.peers_v2: {}",
                                        colspec.name
                                    );
                                    GenericValue::Null
                                }
                            })
                            .collect()
                    })
                    .collect();
            }
            Ok(())
        } else {
            Err(anyhow!(
                "Failed to parse system.local response {:?}",
                peers_response
            ))
        }
    }

    fn rewrite_table_local(
        &self,
        table: &TableToRewrite,
        local_response: &mut Message,
        peers_response: Message,
        warnings: Vec<String>,
    ) -> Result<()> {
        let mut peers = match parse_system_nodes(peers_response) {
            Ok(x) => x,
            Err(MessageParseError::CassandraError(err)) => {
                *local_response = local_response.to_error_response(format!("{:?}", err.context("Shotover failed to generate system.local, an error occured when an internal system.peers query was run.")))
                    .expect("This must succeed because it is a cassandra message and we already know it to be parseable");
                return Ok(());
            }
            Err(MessageParseError::ParseFailure(err)) => return Err(err),
        };
        peers.retain(|node| {
            node.data_center == self.local_shotover_node.data_center
                && node.rack == self.local_shotover_node.rack
        });

        let mut release_version_alias = "release_version";
        let mut tokens_alias = "tokens";
        let mut schema_version_alias = "schema_version";
        let mut broadcast_address_alias = "broadcast_address";
        let mut listen_address_alias = "listen_address";
        let mut host_id_alias = "host_id";
        let mut rpc_address_alias = "rpc_address";
        let mut rpc_port_alias = "rpc_port";
        for select in &table.selects {
            if let SelectElement::Column(column) = select {
                if let Some(alias) = &column.alias {
                    let alias = match alias {
                        Identifier::Unquoted(alias) => alias,
                        Identifier::Quoted(alias) => alias,
                    };
                    if column.name == IdentifierRef::Quoted("release_version") {
                        release_version_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("tokens") {
                        tokens_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("schema_version") {
                        schema_version_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("broadcast_address") {
                        broadcast_address_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("listen_address") {
                        listen_address_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("host_id") {
                        host_id_alias = alias;
                    } else if column.name == IdentifierRef::Quoted("rpc_address") {
                        rpc_address_alias = alias
                    } else if column.name == IdentifierRef::Quoted("rpc_port") {
                        rpc_port_alias = alias
                    }
                }
            }
        }

        if let Some(Frame::Cassandra(frame)) = local_response.frame() {
            if let CassandraOperation::Result(CassandraResult::Rows { rows, metadata }) =
                &mut frame.operation
            {
                frame.warnings = warnings;
                // The local_response message is guaranteed to come from a node that is in our configured data_center/rack.
                // That means we can leave fields like rack and data_center alone and get exactly what we want.
                for row in rows {
                    for (col, col_meta) in row.iter_mut().zip(metadata.col_specs.iter()) {
                        if col_meta.name == release_version_alias {
                            if let GenericValue::Varchar(release_version) = col {
                                for peer in &peers {
                                    if let Ok(Cmp::Lt) = version_compare::compare(
                                        &peer.release_version,
                                        &release_version,
                                    ) {
                                        *release_version = peer.release_version.clone();
                                    }
                                }
                            }
                        } else if col_meta.name == tokens_alias {
                            if let GenericValue::List(tokens) = col {
                                for peer in &peers {
                                    tokens.extend(peer.tokens.iter().cloned());
                                }
                                tokens.sort();
                            }
                        } else if col_meta.name == schema_version_alias {
                            if let GenericValue::Uuid(schema_version) = col {
                                for peer in &peers {
                                    if schema_version != &peer.schema_version {
                                        *schema_version = Uuid::new_v4();
                                        break;
                                    }
                                }
                            }
                        } else if col_meta.name == broadcast_address_alias
                            || col_meta.name == listen_address_alias
                        {
                            if let GenericValue::Inet(address) = col {
                                *address = self.local_shotover_node.address.ip();
                            }
                        } else if col_meta.name == host_id_alias {
                            if let GenericValue::Uuid(host_id) = col {
                                *host_id = self.local_shotover_node.host_id;
                            }
                        } else if col_meta.name == rpc_address_alias {
                            if let GenericValue::Inet(address) = col {
                                if address != &IpAddr::V4(Ipv4Addr::UNSPECIFIED) {
                                    *address = self.local_shotover_node.address.ip()
                                }
                            }
                        } else if col_meta.name == rpc_port_alias {
                            if let GenericValue::Integer(rpc_port, _) = col {
                                *rpc_port = self.local_shotover_node.address.port() as i64;
                            }
                        }
                    }
                }
            }
            Ok(())
        } else {
            Err(anyhow!(
                "Failed to parse system.local response {:?}",
                local_response
            ))
        }
    }
}

fn create_query(messages: &Messages, query: &str, version: Version) -> Result<Message> {
    let stream_id = get_unused_stream_id(messages)?;
    Ok(Message::from_frame(Frame::Cassandra(CassandraFrame {
        version,
        stream_id,
        tracing: Tracing::Request(false),
        warnings: vec![],
        operation: CassandraOperation::Query {
            query: Box::new(parse_statement_single(query)),
            params: Box::default(),
        },
    })))
}

fn get_unused_stream_id(messages: &Messages) -> Result<i16> {
    // start at an unusual number to hopefully avoid looping many times when we receive stream ids that look like [0, 1, 2, ..]
    // We can quite happily give up 358 stream ids as that still allows for shotover message batches containing 2 ** 16 - 358 = 65178 messages
    for i in 358..i16::MAX {
        if !messages
            .iter()
            .filter_map(|message| message.stream_id())
            .contains(&i)
        {
            return Ok(i);
        }
    }
    Err(anyhow!("Ran out of stream ids"))
}

fn has_no_where_clause(ty: &RewriteTableTy, select: &Select) -> bool {
    select.where_clause.is_empty()
        // Most drivers do `FROM system.local WHERE key = 'local'` when determining the topology.
        // I'm not sure why they do that it seems to have no affect as there is only ever one row and its key is always 'local'.
        // Maybe it was a workaround for an old version of cassandra that got copied around?
        // To keep warning noise down we consider it as having no where clause.
        || (ty == &RewriteTableTy::Local
            && select.where_clause
                == [RelationElement {
                    obj: Operand::Column(Identifier::Quoted("key".to_owned())),
                    oper: RelationOperator::Equal,
                    value: Operand::Const("'local'".to_owned()),
                }])
}

#[derive(Clone, Debug)]
pub struct TableToRewrite {
    pub collected_messages: Vec<MessageOrId>,
    pub ty: RewriteTableTy,
    selects: Vec<SelectElement>,
    warnings: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum MessageOrId {
    Message(Message),
    Id(MessageId),
}

#[derive(PartialEq, Clone, Debug)]
pub enum RewriteTableTy {
    Local,
    Peers,
    // We need to know which nodes we are sending to early on so that we can create the appropriate number of messages early and keep our rewrite indexes intact
    Prepare { clone_index: usize },
}

struct NodeInfo {
    tokens: Vec<GenericValue>,
    schema_version: Uuid,
    release_version: String,
    rack: String,
    data_center: String,
}

enum MessageParseError {
    /// The db returned a message that shotover failed to parse or process, this indicates a bug in shotover or the db
    ParseFailure(anyhow::Error),
    /// The db returned an error message, this indicates a user error e.g. the user does not have sufficient permissions
    CassandraError(anyhow::Error),
}

fn parse_system_nodes(mut response: Message) -> Result<Vec<NodeInfo>, MessageParseError> {
    if let Some(Frame::Cassandra(frame)) = response.frame() {
        match &mut frame.operation {
            CassandraOperation::Result(CassandraResult::Rows { rows, .. }) => rows
                .iter_mut()
                .map(|row| {
                    if row.len() != 5 {
                        return Err(MessageParseError::ParseFailure(anyhow!(
                            "expected 5 columns but was {}",
                            row.len()
                        )));
                    }

                    let release_version = match row.pop() {
                        Some(GenericValue::Varchar(value)) => value,
                        other => {
                            return Err(MessageParseError::ParseFailure(anyhow!(
                                "Expected release_version to be a Varchar but was {other:?}"
                            )))
                        }
                    };

                    let tokens = match row.pop() {
                        Some(GenericValue::List(value)) => value,
                        other => {
                            return Err(MessageParseError::ParseFailure(anyhow!(
                                "Expected tokens to be a list but was {other:?}"
                            )))
                        }
                    };

                    let schema_version = match row.pop() {
                        Some(GenericValue::Uuid(value)) => value,
                        other => {
                            return Err(MessageParseError::ParseFailure(anyhow!(
                                "Expected schema_version to be a Uuid but was {other:?}"
                            )))
                        }
                    };

                    let data_center = match row.pop() {
                        Some(GenericValue::Varchar(value)) => value,
                        other => {
                            return Err(MessageParseError::ParseFailure(anyhow!(
                                "Expected data_center to be a Varchar but was {other:?}"
                            )))
                        }
                    };

                    let rack = match row.pop() {
                        Some(GenericValue::Varchar(value)) => value,
                        other => {
                            return Err(MessageParseError::ParseFailure(anyhow!(
                                "Expected rack to be a Varchar but was {other:?}"
                            )))
                        }
                    };

                    Ok(NodeInfo {
                        tokens,
                        schema_version,
                        release_version,
                        data_center,
                        rack,
                    })
                })
                .collect(),
            CassandraOperation::Error(error) => Err(MessageParseError::CassandraError(anyhow!(
                "system.local returned error: {error:?}",
            ))),
            operation => Err(MessageParseError::ParseFailure(anyhow!(
                "system.local returned unexpected cassandra operation: {operation:?}",
            ))),
        }
    } else {
        Err(MessageParseError::ParseFailure(anyhow!(
            "Failed to parse system.local response {:?}",
            response
        )))
    }
}
