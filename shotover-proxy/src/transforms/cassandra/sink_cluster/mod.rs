use crate::codec::cassandra::CassandraCodec;
use crate::error::ChainResponse;
use crate::frame::cassandra::parse_statement_single;
use crate::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use crate::message::{Message, MessageValue, Messages};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::cassandra::connection::CassandraConnection;
use crate::transforms::util::Response;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cassandra_protocol::frame::Version;
use cassandra_protocol::query::QueryParams;
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::{FQName, Identifier};
use cql3_parser::select::SelectElement;
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use metrics::{register_counter, Counter};
use node::CassandraNode;
use rand::prelude::*;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;
use version_compare::Cmp;

mod node;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraSinkClusterConfig {
    /// contact points must be within the specified data_center and rack.
    /// If this is not followed, shotover's invariants will still be upheld but shotover will communicate with a
    /// node outside of the specified data_center and rack.
    pub first_contact_points: Vec<String>,
    pub data_center: String,
    pub rack: String,
    pub host_id: Uuid,
    pub tls: Option<TlsConnectorConfig>,
    pub read_timeout: Option<u64>,
}

impl CassandraSinkClusterConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Transforms::CassandraSinkCluster(CassandraSinkCluster::new(
            self.first_contact_points.clone(),
            chain_name,
            self.data_center.clone(),
            self.rack.clone(),
            self.host_id,
            tls,
            self.read_timeout,
        )))
    }
}

pub struct CassandraSinkCluster {
    contact_points: Vec<String>,
    init_handshake_connection: Option<CassandraConnection>,
    init_handshake: Vec<Message>,
    init_handshake_address: Option<SocketAddr>,
    init_handshake_complete: bool,
    init_handshake_use_received: bool,
    chain_name: String,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    read_timeout: Option<Duration>,
    local_table: FQName,
    peer_table: FQName,
    data_center: String,
    rack: String,
    host_id: Uuid,
    /// A local clone of topology_task_nodes
    /// Internally stores connections to the nodes
    local_nodes: Vec<CassandraNode>,
    /// Only written to by the topology task
    /// Transform instances should never write to this.
    topology_task_nodes: Arc<RwLock<Vec<CassandraNode>>>,
    rng: SmallRng,
    task_handshake_tx: mpsc::Sender<TaskHandshake>,
}

impl Clone for CassandraSinkCluster {
    fn clone(&self) -> Self {
        CassandraSinkCluster {
            contact_points: self.contact_points.clone(),
            init_handshake_connection: None,
            init_handshake: vec![],
            init_handshake_address: None,
            init_handshake_complete: false,
            init_handshake_use_received: false,
            chain_name: self.chain_name.clone(),
            tls: self.tls.clone(),
            failed_requests: self.failed_requests.clone(),
            pushed_messages_tx: None,
            read_timeout: self.read_timeout,
            local_table: self.local_table.clone(),
            peer_table: self.peer_table.clone(),
            data_center: self.data_center.clone(),
            rack: self.rack.clone(),
            host_id: self.host_id,
            local_nodes: vec![],
            topology_task_nodes: self.topology_task_nodes.clone(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            task_handshake_tx: self.task_handshake_tx.clone(),
        }
    }
}

impl CassandraSinkCluster {
    pub fn new(
        contact_points: Vec<String>,
        chain_name: String,
        data_center: String,
        rack: String,
        host_id: Uuid,
        tls: Option<TlsConnector>,
        timeout: Option<u64>,
    ) -> CassandraSinkCluster {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "CassandraSinkCluster");
        let receive_timeout = timeout.map(Duration::from_secs);

        let nodes_shared = Arc::new(RwLock::new(vec![]));

        let (task_handshake_tx, task_handshake_rx) = mpsc::channel(1);
        create_topology_task(
            tls.clone(),
            nodes_shared.clone(),
            task_handshake_rx,
            data_center.clone(),
        );

        CassandraSinkCluster {
            contact_points,
            init_handshake_connection: None,
            init_handshake: vec![],
            init_handshake_address: None,
            init_handshake_complete: false,
            init_handshake_use_received: false,
            chain_name,
            failed_requests,
            tls,
            pushed_messages_tx: None,
            read_timeout: receive_timeout,
            local_table: FQName::new("system", "local"),
            peer_table: FQName::new("system", "peers"),
            data_center,
            rack,
            host_id,
            local_nodes: vec![],
            topology_task_nodes: nodes_shared,
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            task_handshake_tx,
        }
    }
}

impl CassandraSinkCluster {
    async fn send_message(
        &mut self,
        mut messages: Messages,
        local_addr: SocketAddr,
    ) -> ChainResponse {
        // Attempt to populate nodes list if we still dont have one yet
        if self.local_nodes.is_empty() {
            let nodes_shared = self.topology_task_nodes.read().await;
            self.local_nodes = nodes_shared.clone();
        }

        let tables_to_rewrite: Vec<TableToRewrite> = messages
            .iter_mut()
            .enumerate()
            .filter_map(|(i, m)| self.get_rewrite_table(m, i))
            .collect();

        for table_to_rewrite in tables_to_rewrite.iter().rev() {
            let mut stream_id = 0;
            let mut restart = true;
            while restart {
                restart = false;
                for message in &mut messages {
                    if let Some(Frame::Cassandra(frame)) = message.frame() {
                        if stream_id == frame.stream_id {
                            match stream_id.checked_add(1) {
                                Some(new_stream_id) => stream_id = new_stream_id,
                                None => return Err(anyhow!("Ran out of stream_ids")),
                            }
                            restart = true;
                        }
                    }
                }
            }

            if let RewriteTableTy::Local = table_to_rewrite.ty {
                messages.insert(
                    table_to_rewrite.index+1,
                    Message::from_frame(Frame::Cassandra(CassandraFrame {
                        version: table_to_rewrite.version,
                        stream_id,
                        tracing_id: None,
                        warnings: vec![],
                        operation: CassandraOperation::Query {
                            query: Box::new(parse_statement_single(
                                "SELECT rack, data_center, schema_version, tokens, release_version FROM system.peers",
                            )),
                            params: Box::new(QueryParams::default()),
                        },
                    })),
                );
            }
        }

        // Create the initial connection.
        // Messages will be sent through this connection until we have extracted the handshake.
        if self.init_handshake_connection.is_none() {
            let random_point = if let Some(random_point) = self.local_nodes.choose(&mut self.rng) {
                SocketAddr::new(random_point.address, 9042)
            } else {
                tokio::net::lookup_host(self.contact_points.choose(&mut self.rng).unwrap())
                    .await?
                    .next()
                    .unwrap()
            };
            self.init_handshake_connection = Some(
                CassandraConnection::new(
                    random_point,
                    CassandraCodec::new(),
                    self.tls.clone(),
                    self.pushed_messages_tx.clone(),
                )
                .await?,
            );
            self.init_handshake_address = Some(random_point);
        }

        if !self.init_handshake_complete {
            for message in &mut messages {
                // Filter operation types so we are only left with messages relevant to the handshake.
                // Due to shotover pipelining we could receive non-handshake messages while !self.init_handshake_complete.
                // Despite being used by the client in a handshake, CassandraOperation::Options is not included
                // because it doesnt dont alter the state of the server and so it isnt needed.
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Startup(_) | CassandraOperation::AuthResponse(_),
                    ..
                })) = message.frame()
                {
                    self.init_handshake.push(message.clone());
                }
            }
        }

        let mut responses_future = FuturesOrdered::new();
        let mut responses_future_use = FuturesOrdered::new();
        let mut use_future_index_to_node_index = vec![];
        for mut message in messages {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            if self.local_nodes.is_empty()
                || !self.init_handshake_complete
                // system.local and system.peers must be routed to the same node otherwise the system.local node will be amongst the system.peers nodes and a node will be missing
                // DDL statements and system.local must be routed through the same connection, so that schema_version changes appear immediately in system.local
                || is_ddl_statement(&mut message)
                || self.is_system_local_or_peers(&mut message)
            {
                self.init_handshake_connection.as_mut().unwrap()
            } else if is_use_statement(&mut message) {
                // If we have already received a USE statement then pop it off the handshakes list to avoid infinite growth
                if self.init_handshake_use_received {
                    self.init_handshake.pop();
                }
                self.init_handshake_use_received = true;

                // Adding the USE statement to the handshake ensures that any new connection
                // created will have the correct keyspace setup.
                self.init_handshake.push(message.clone());

                // Send the USE statement to all open connections to ensure they are all in sync
                for (node_index, node) in self.local_nodes.iter().enumerate() {
                    if let Some(outbound) = &node.outbound {
                        let (return_chan_tx, return_chan_rx) = oneshot::channel();
                        outbound.send(message.clone(), return_chan_tx)?;
                        responses_future_use.push_back(return_chan_rx);
                        use_future_index_to_node_index.push(node_index);
                    }
                }

                // Send the USE statement to the handshake connection and use the response as shotovers response
                self.init_handshake_connection.as_mut().unwrap()
            } else {
                // We have a full nodes list and handshake, so we can do proper routing now.
                let random_node = self.local_nodes.choose_mut(&mut self.rng).unwrap();
                random_node
                    .get_connection(&self.init_handshake, &self.tls, &self.pushed_messages_tx)
                    .await?
            }
            .send(message, return_chan_tx)?;

            responses_future.push_back(return_chan_rx)
        }

        let mut responses =
            super::connection::receive(self.read_timeout, &self.failed_requests, responses_future)
                .await?;

        // When the server indicates that it is ready for normal operation via Ready or AuthSuccess,
        // we have succesfully collected an entire handshake so we mark the handshake as complete.
        if !self.init_handshake_complete {
            for response in &mut responses {
                if let Some(Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Ready(_) | CassandraOperation::AuthSuccess(_),
                    ..
                })) = response.frame()
                {
                    // Only send a handshake if the task really needs it
                    // i.e. when the channel of size 1 is empty
                    if let Ok(permit) = self.task_handshake_tx.try_reserve() {
                        permit.send(TaskHandshake {
                            handshake: self.init_handshake.clone(),
                            address: self.init_handshake_address.unwrap(),
                        })
                    }
                    self.init_handshake_complete = true;
                    break;
                }
            }
        }

        for node_index in use_future_index_to_node_index {
            let response = responses_future_use
                .next()
                .await
                .map(|x| x.map_err(|e| anyhow!(e)));
            // If any errors occurred close the connection as we can no
            // longer make any guarantees about the current state of the connection
            if !is_use_statement_successful(response) {
                self.local_nodes[node_index].outbound = None;
            }
        }

        for table_to_rewrite in tables_to_rewrite {
            self.rewrite_table(table_to_rewrite, local_addr, &mut responses)
                .await?;
        }

        Ok(responses)
    }

    fn get_rewrite_table(&self, request: &mut Message, index: usize) -> Option<TableToRewrite> {
        if let Some(Frame::Cassandra(cassandra)) = request.frame() {
            // No need to handle Batch as selects can only occur on Query
            if let CassandraOperation::Query { query, .. } = &cassandra.operation {
                if let CassandraStatement::Select(select) = query.as_ref() {
                    let ty = if self.local_table == select.table_name {
                        RewriteTableTy::Local
                    } else if self.peer_table == select.table_name {
                        RewriteTableTy::Peers
                    } else {
                        return None;
                    };

                    return Some(TableToRewrite {
                        index,
                        ty,
                        version: cassandra.version,
                        selects: select.columns.clone(),
                    });
                }
            }
        }
        None
    }

    async fn rewrite_table(
        &mut self,
        table: TableToRewrite,
        local_addr: SocketAddr,
        responses: &mut Vec<Message>,
    ) -> Result<()> {
        match table.ty {
            RewriteTableTy::Local => {
                if table.index + 1 < responses.len() {
                    let peers_response = responses.remove(table.index + 1);
                    if let Some(local_response) = responses.get_mut(table.index) {
                        self.rewrite_table_local(table, local_addr, local_response, peers_response)
                            .await?;
                        local_response.invalidate_cache();
                    }
                }
            }
            RewriteTableTy::Peers => {
                if let Some(peers_response) = responses.get_mut(table.index) {
                    self.rewrite_table_peers(peers_response).await?;
                    peers_response.invalidate_cache();
                }
            }
        }

        Ok(())
    }

    async fn rewrite_table_peers(&mut self, peers_response: &mut Message) -> Result<()> {
        // TODO: generate rows for shotover peers
        //       the current implementation will at least direct all traffic through shotover
        if let Some(Frame::Cassandra(frame)) = peers_response.frame() {
            if let CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                ..
            }) = &mut frame.operation
            {
                rows.clear();
            }
            Ok(())
        } else {
            Err(anyhow!(
                "Failed to parse system.local response {:?}",
                peers_response
            ))
        }
    }

    async fn rewrite_table_local(
        &mut self,
        table: TableToRewrite,
        local_address: SocketAddr,
        local_response: &mut Message,
        peers_response: Message,
    ) -> Result<()> {
        let peers = parse_system_peers(peers_response, &self.data_center, &self.rack)?;

        let mut release_version_alias = "release_version";
        let mut tokens_alias = "tokens";
        let mut schema_version_alias = "schema_version";
        let mut broadcast_address_alias = "broadcast_address";
        let mut listen_address_alias = "listen_address";
        let mut host_id_alias = "host_id";
        let mut rack_alias = "rack";
        let mut data_center_alias = "data_center_alias";
        for select in &table.selects {
            if let SelectElement::Column(column) = select {
                if let Some(alias) = &column.alias {
                    let alias = match alias {
                        Identifier::Unquoted(alias) => alias,
                        Identifier::Quoted(alias) => alias,
                    };
                    if column.name == Identifier::Unquoted("release_version".to_string()) {
                        release_version_alias = alias;
                    } else if column.name == Identifier::Unquoted("tokens".to_string()) {
                        tokens_alias = alias;
                    } else if column.name == Identifier::Unquoted("schema_version".to_string()) {
                        schema_version_alias = alias;
                    } else if column.name == Identifier::Unquoted("broadcast_address".to_string()) {
                        broadcast_address_alias = alias;
                    } else if column.name == Identifier::Unquoted("listen_address".to_string()) {
                        listen_address_alias = alias;
                    } else if column.name == Identifier::Unquoted("host_id".to_string()) {
                        host_id_alias = alias;
                    } else if column.name == Identifier::Unquoted("rack".to_string()) {
                        rack_alias = alias;
                    } else if column.name == Identifier::Unquoted("data_center".to_string()) {
                        data_center_alias = alias;
                    }
                }
            }
        }

        if let Some(Frame::Cassandra(frame)) = local_response.frame() {
            if let CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                metadata,
            }) = &mut frame.operation
            {
                for row in rows {
                    for (col, col_meta) in row.iter_mut().zip(metadata.col_specs.iter()) {
                        if col_meta.name == release_version_alias {
                            if let MessageValue::Varchar(release_version) = col {
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
                            if let MessageValue::List(tokens) = col {
                                for peer in &peers {
                                    tokens.extend(peer.tokens.iter().cloned());
                                }
                                tokens.sort();
                            }
                        } else if col_meta.name == schema_version_alias {
                            if let MessageValue::Uuid(schema_version) = col {
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
                            if let MessageValue::Inet(address) = col {
                                *address = local_address.ip();
                            }
                        } else if col_meta.name == host_id_alias {
                            if let MessageValue::Uuid(host_id) = col {
                                *host_id = self.host_id;
                            }
                        } else if col_meta.name == rack_alias {
                            if let MessageValue::Varchar(rack) = col {
                                if rack != &self.rack {
                                    *rack = self.rack.clone();
                                    tracing::warn!("A contact point node is not in the configured rack, this node will receive traffic from outside of its rack");
                                }
                            }
                        } else if col_meta.name == data_center_alias {
                            if let MessageValue::Varchar(data_center) = col {
                                if data_center != &self.data_center {
                                    *data_center = self.data_center.clone();
                                    tracing::warn!("A contact point node is not in the configured data_center, this node will receive traffic from outside of its data_center");
                                }
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

    // TODO: handle use statement state
    fn is_system_local_or_peers(&self, request: &mut Message) -> bool {
        if let Some(Frame::Cassandra(frame)) = request.frame() {
            if let CassandraOperation::Query { query, .. } = &mut frame.operation {
                if let CassandraStatement::Select(select) = query.as_ref() {
                    return self.local_table == select.table_name
                        || self.peer_table == select.table_name;
                }
            }
        }
        false
    }
}

struct TableToRewrite {
    index: usize,
    ty: RewriteTableTy,
    version: Version,
    selects: Vec<SelectElement>,
}

enum RewriteTableTy {
    Local,
    Peers,
}

pub fn create_topology_task(
    tls: Option<TlsConnector>,
    nodes: Arc<RwLock<Vec<CassandraNode>>>,
    mut handshake_rx: mpsc::Receiver<TaskHandshake>,
    data_center: String,
) {
    tokio::spawn(async move {
        while let Some(handshake) = handshake_rx.recv().await {
            let mut attempts = 0;
            while let Err(err) = topology_task_process(&tls, &nodes, &handshake, &data_center).await
            {
                tracing::error!("topology task failed, retrying, error was: {err:?}");
                attempts += 1;
                if attempts > 3 {
                    // 3 attempts have failed, lets try a new handshake
                    break;
                }
            }

            // Sleep for an hour.
            // TODO: This is a crude way to ensure we dont overload the transforms with too many topology changes.
            // This will be replaced with:
            // * the task subscribes to events
            // * the transforms request a reload when they hit connection errors
            tokio::time::sleep(std::time::Duration::from_secs(60 * 60)).await;
        }
    });
}

async fn topology_task_process(
    tls: &Option<TlsConnector>,
    nodes: &Arc<RwLock<Vec<CassandraNode>>>,
    handshake: &TaskHandshake,
    data_center: &str,
) -> Result<()> {
    let outbound =
        node::new_connection(&handshake.address, &handshake.handshake, tls, &None).await?;

    let (peers_tx, peers_rx) = oneshot::channel();
    outbound.send(
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single(
                    "SELECT peer, rack, data_center, tokens FROM system.peers",
                )),
                params: Box::new(QueryParams::default()),
            },
        })),
        peers_tx,
    )?;

    let (local_tx, local_rx) = oneshot::channel();
    outbound.send(
        Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 1,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single(
                    "SELECT listen_address, rack, data_center, tokens FROM system.local",
                )),
                params: Box::new(QueryParams::default()),
            },
        })),
        local_tx,
    )?;

    let (new_nodes, more_nodes) = tokio::join!(
        async { system_peers_into_nodes(peers_rx.await?.response?, data_center) },
        async { system_peers_into_nodes(local_rx.await?.response?, data_center) }
    );
    let mut new_nodes = new_nodes?;
    new_nodes.extend(more_nodes?);

    let mut write_lock = nodes.write().await;
    let expensive_drop = std::mem::replace(&mut *write_lock, new_nodes);

    // Make sure to drop write_lock before the expensive_drop which will have to perform many deallocations.
    std::mem::drop(write_lock);
    std::mem::drop(expensive_drop);

    Ok(())
}

fn system_peers_into_nodes(
    mut response: Message,
    config_data_center: &str,
) -> Result<Vec<CassandraNode>> {
    if let Some(Frame::Cassandra(frame)) = response.frame() {
        match &mut frame.operation {
            CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                ..
            }) => rows
                .iter_mut()
                .filter(|row| {
                    if let Some(MessageValue::Varchar(data_center)) = row.get(2) {
                        data_center == config_data_center
                    } else {
                        false
                    }
                })
                .map(|row| {
                    if row.len() != 4 {
                        return Err(anyhow!("expected 4 columns but was {}", row.len()));
                    }

                    let tokens = if let Some(MessageValue::List(list)) = row.pop() {
                        list.into_iter()
                            .map::<Result<String>, _>(|x| match x {
                                MessageValue::Varchar(a) => Ok(a),
                                _ => Err(anyhow!("tokens value not a varchar")),
                            })
                            .collect::<Result<Vec<String>>>()?
                    } else {
                        return Err(anyhow!("tokens not a list"));
                    };
                    let _data_center = row.pop();
                    let rack = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("rack not a varchar"));
                    };
                    let address = if let Some(MessageValue::Inet(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("address not an inet"));
                    };

                    Ok(CassandraNode {
                        address,
                        _rack: rack,
                        _tokens: tokens,
                        outbound: None,
                    })
                })
                .collect(),
            operation => Err(anyhow!(
                "system.peers returned unexpected cassandra operation: {:?}",
                operation
            )),
        }
    } else {
        Err(anyhow!(
            "Failed to parse system.peers response {:?}",
            response
        ))
    }
}

fn is_use_statement(request: &mut Message) -> bool {
    if let Some(Frame::Cassandra(frame)) = request.frame() {
        if let CassandraOperation::Query { query, .. } = &mut frame.operation {
            if let CassandraStatement::Use(_) = query.as_ref() {
                return true;
            }
        }
    }
    false
}

fn is_ddl_statement(request: &mut Message) -> bool {
    if let Some(Frame::Cassandra(frame)) = request.frame() {
        if let CassandraOperation::Query { query, .. } = &mut frame.operation {
            if let CassandraStatement::CreateAggregate(_)
            | CassandraStatement::CreateFunction(_)
            | CassandraStatement::CreateIndex(_)
            | CassandraStatement::CreateKeyspace(_)
            | CassandraStatement::CreateMaterializedView(_)
            | CassandraStatement::CreateRole(_)
            | CassandraStatement::CreateTable(_)
            | CassandraStatement::CreateTrigger(_)
            | CassandraStatement::CreateType(_)
            | CassandraStatement::CreateUser(_)
            | CassandraStatement::AlterKeyspace(_)
            | CassandraStatement::AlterMaterializedView(_)
            | CassandraStatement::AlterRole(_)
            | CassandraStatement::AlterTable(_)
            | CassandraStatement::AlterType(_)
            | CassandraStatement::AlterUser(_) = query.as_ref()
            {
                return true;
            }
        }
    }
    false
}

fn is_use_statement_successful(response: Option<Result<Response>>) -> bool {
    if let Some(Ok(Response {
        response: Ok(mut response),
        ..
    })) = response
    {
        if let Some(Frame::Cassandra(CassandraFrame {
            operation: CassandraOperation::Result(CassandraResult::SetKeyspace(_)),
            ..
        })) = response.frame()
        {
            return true;
        }
    }
    false
}

struct SystemPeer {
    tokens: Vec<MessageValue>,
    schema_version: Uuid,
    release_version: String,
}

fn parse_system_peers(
    mut response: Message,
    config_data_center: &str,
    config_rack: &str,
) -> Result<Vec<SystemPeer>> {
    if let Some(Frame::Cassandra(frame)) = response.frame() {
        match &mut frame.operation {
            CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(rows),
                ..
            }) => rows
                .iter_mut()
                .filter(|row| {
                    if let (
                        Some(MessageValue::Varchar(data_center)),
                        Some(MessageValue::Varchar(rack)),
                    ) = (row.get(1), row.get(0))
                    {
                        data_center == config_data_center && rack == config_rack
                    } else {
                        false
                    }
                })
                .map(|row| {
                    if row.len() != 5 {
                        return Err(anyhow!("expected 5 columns but was {}", row.len()));
                    }

                    let release_version = if let Some(MessageValue::Varchar(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("release_version not a list"));
                    };

                    let tokens = if let Some(MessageValue::List(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("tokens not a list"));
                    };

                    let schema_version = if let Some(MessageValue::Uuid(value)) = row.pop() {
                        value
                    } else {
                        return Err(anyhow!("schema_version not a uuid"));
                    };

                    let _data_center = row.pop();
                    let _rack = row.pop();

                    Ok(SystemPeer {
                        tokens,
                        schema_version,
                        release_version,
                    })
                })
                .collect(),
            operation => Err(anyhow!(
                "system.local returned unexpected cassandra operation: {:?}",
                operation
            )),
        }
    } else {
        Err(anyhow!(
            "Failed to parse system.local response {:?}",
            response
        ))
    }
}

#[async_trait]
impl Transform for CassandraSinkCluster {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.send_message(message_wrapper.messages, message_wrapper.local_addr)
            .await
    }

    fn is_terminating(&self) -> bool {
        true
    }

    fn add_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}

#[derive(Debug)]
pub struct TaskHandshake {
    pub handshake: Vec<Message>,
    pub address: SocketAddr,
}
