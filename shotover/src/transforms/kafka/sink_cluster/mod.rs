use crate::frame::kafka::{KafkaFrame, RequestBody, ResponseBody};
use crate::frame::{Frame, MessageType};
use crate::message::{Message, MessageIdMap, Messages};
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::{
    ChainState, DownChainProtocol, Transform, TransformBuilder, TransformContextBuilder,
    UpChainProtocol,
};
use crate::transforms::{TransformConfig, TransformContextConfig};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use connections::{Connections, Destination};
use dashmap::DashMap;
use kafka_node::{ConnectionFactory, KafkaAddress, KafkaNode, KafkaNodeState};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::fetch_request::FetchTopic;
use kafka_protocol::messages::fetch_response::LeaderIdAndEpoch as FetchResponseLeaderIdAndEpoch;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::messages::produce_request::TopicProduceData;
use kafka_protocol::messages::produce_response::LeaderIdAndEpoch as ProduceResponseLeaderIdAndEpoch;
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    GroupId, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, MetadataRequest,
    MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::ResponseError;
use metrics::{counter, Counter};
use rand::rngs::SmallRng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::SeedableRng;
use scram_over_mtls::{
    AuthorizeScramOverMtls, AuthorizeScramOverMtlsBuilder, AuthorizeScramOverMtlsConfig,
    OriginalScramState,
};
use serde::{Deserialize, Serialize};
use shotover_node::{ShotoverNode, ShotoverNodeConfig};
use std::collections::{HashMap, VecDeque};
use std::hash::Hasher;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use uuid::Uuid;

mod connections;
mod kafka_node;
mod scram_over_mtls;
pub mod shotover_node;

const SASL_SCRAM_MECHANISMS: [&str; 2] = ["SCRAM-SHA-256", "SCRAM-SHA-512"];

#[derive(thiserror::Error, Debug)]
enum FindCoordinatorError {
    #[error("Coordinator not available")]
    CoordinatorNotAvailable,
    #[error("{0:?}")]
    Unrecoverable(#[from] anyhow::Error),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkClusterConfig {
    pub first_contact_points: Vec<String>,
    pub shotover_nodes: Vec<ShotoverNodeConfig>,
    pub local_shotover_broker_id: i32,
    pub connect_timeout_ms: u64,
    pub read_timeout: Option<u64>,
    pub check_shotover_peers_delay_ms: Option<u64>,
    pub tls: Option<TlsConnectorConfig>,
    pub authorize_scram_over_mtls: Option<AuthorizeScramOverMtlsConfig>,
}

const NAME: &str = "KafkaSinkCluster";
#[typetag::serde(name = "KafkaSinkCluster")]
#[async_trait(?Send)]
impl TransformConfig for KafkaSinkClusterConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let tls = self.tls.as_ref().map(TlsConnector::new).transpose()?;

        let shotover_nodes: Result<Vec<_>> = self
            .shotover_nodes
            .iter()
            .cloned()
            .map(ShotoverNodeConfig::build)
            .collect();
        let mut shotover_nodes = shotover_nodes?;
        let rack = shotover_nodes
            .iter()
            .find(|x| x.broker_id.0 == self.local_shotover_broker_id)
            .map(|x| x.rack.clone())
            .ok_or_else(|| {
                anyhow!(
                    "local_shotover_broker_id {} was missing in shotover_nodes",
                    self.local_shotover_broker_id
                )
            })?;
        shotover_nodes.sort_by_key(|x| x.broker_id);

        let first_contact_points: Result<Vec<_>> = self
            .first_contact_points
            .iter()
            .map(|x| KafkaAddress::from_str(x))
            .collect();

        Ok(Box::new(KafkaSinkClusterBuilder::new(
            transform_context.chain_name,
            first_contact_points?,
            &self.authorize_scram_over_mtls,
            shotover_nodes,
            rack,
            self.connect_timeout_ms,
            self.read_timeout,
            tls,
        )?))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::Kafka])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::Terminating
    }
}

struct KafkaSinkClusterBuilder {
    // contains address and port
    first_contact_points: Vec<KafkaAddress>,
    shotover_nodes: Vec<ShotoverNode>,
    rack: StrBytes,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    controller_broker: Arc<AtomicBrokerId>,
    group_to_coordinator_broker: Arc<DashMap<GroupId, BrokerId>>,
    topic_by_name: Arc<DashMap<TopicName, Topic>>,
    topic_by_id: Arc<DashMap<Uuid, Topic>>,
    nodes_shared: Arc<RwLock<Vec<KafkaNode>>>,
    authorize_scram_over_mtls: Option<AuthorizeScramOverMtlsBuilder>,
    tls: Option<TlsConnector>,
    out_of_rack_requests: Counter,
}

impl KafkaSinkClusterBuilder {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        chain_name: String,
        first_contact_points: Vec<KafkaAddress>,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtlsConfig>,
        shotover_nodes: Vec<ShotoverNode>,
        rack: StrBytes,
        connect_timeout_ms: u64,
        timeout: Option<u64>,
        tls: Option<TlsConnector>,
    ) -> Result<KafkaSinkClusterBuilder> {
        let read_timeout = timeout.map(Duration::from_secs);
        let connect_timeout = Duration::from_millis(connect_timeout_ms);

        Ok(KafkaSinkClusterBuilder {
            first_contact_points,
            authorize_scram_over_mtls: authorize_scram_over_mtls
                .as_ref()
                .map(|x| x.get_builder(connect_timeout, read_timeout))
                .transpose()?,
            shotover_nodes,
            rack,
            connect_timeout,
            read_timeout,
            controller_broker: Arc::new(AtomicBrokerId::new()),
            group_to_coordinator_broker: Arc::new(DashMap::new()),
            topic_by_name: Arc::new(DashMap::new()),
            topic_by_id: Arc::new(DashMap::new()),
            nodes_shared: Arc::new(RwLock::new(vec![])),
            out_of_rack_requests: counter!("shotover_out_of_rack_requests_count", "chain" => chain_name, "transform" => NAME),
            tls,
        })
    }
}

impl TransformBuilder for KafkaSinkClusterBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(KafkaSinkCluster {
            connections: Connections::new(self.out_of_rack_requests.clone()),
            first_contact_points: self.first_contact_points.clone(),
            shotover_nodes: self.shotover_nodes.clone(),
            rack: self.rack.clone(),
            nodes: vec![],
            nodes_shared: self.nodes_shared.clone(),
            controller_broker: self.controller_broker.clone(),
            group_to_coordinator_broker: self.group_to_coordinator_broker.clone(),
            topic_by_name: self.topic_by_name.clone(),
            topic_by_id: self.topic_by_id.clone(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            auth_complete: false,
            connection_factory: ConnectionFactory::new(
                self.tls.clone(),
                self.connect_timeout,
                self.read_timeout,
                transform_context.force_run_chain,
            ),
            pending_requests: Default::default(),
            // TODO: this approach with `find_coordinator_requests` and `routed_to_coordinator_for_group`
            //       is prone to memory leaks and logic errors.
            //       We should replace these fields with extra state within `PendingRequestState::Received/Sent`.
            find_coordinator_requests: Default::default(),
            routed_to_coordinator_for_group: Default::default(),
            temp_responses_buffer: Default::default(),
            sasl_mechanism: None,
            authorize_scram_over_mtls: self.authorize_scram_over_mtls.as_ref().map(|x| x.build()),
        })
    }

    fn get_name(&self) -> &'static str {
        NAME
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

struct AtomicBrokerId(AtomicI64);

impl AtomicBrokerId {
    fn new() -> Self {
        AtomicBrokerId(i64::MAX.into())
    }

    fn set(&self, value: BrokerId) {
        self.0
            .store(value.0.into(), std::sync::atomic::Ordering::Relaxed)
    }

    fn clear(&self) {
        self.0.store(i64::MAX, std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns `None` when set has never been called.
    /// Otherwise returns `Some` containing the latest set value.
    fn get(&self) -> Option<BrokerId> {
        match self.0.load(std::sync::atomic::Ordering::Relaxed) {
            i64::MAX => None,
            other => Some(BrokerId(other as i32)),
        }
    }
}

struct KafkaSinkCluster {
    first_contact_points: Vec<KafkaAddress>,
    shotover_nodes: Vec<ShotoverNode>,
    rack: StrBytes,
    nodes: Vec<KafkaNode>,
    nodes_shared: Arc<RwLock<Vec<KafkaNode>>>,
    controller_broker: Arc<AtomicBrokerId>,
    group_to_coordinator_broker: Arc<DashMap<GroupId, BrokerId>>,
    topic_by_name: Arc<DashMap<TopicName, Topic>>,
    topic_by_id: Arc<DashMap<Uuid, Topic>>,
    rng: SmallRng,
    auth_complete: bool,
    connection_factory: ConnectionFactory,
    /// Maintains the state of each request/response pair.
    /// Ordering must be maintained to ensure responses match up with their request.
    pending_requests: VecDeque<PendingRequest>,
    find_coordinator_requests: MessageIdMap<FindCoordinator>,
    routed_to_coordinator_for_group: MessageIdMap<GroupId>,
    /// A temporary buffer used when receiving responses, only held onto in order to avoid reallocating.
    temp_responses_buffer: Vec<Message>,
    sasl_mechanism: Option<String>,
    authorize_scram_over_mtls: Option<AuthorizeScramOverMtls>,
    connections: Connections,
}

/// State of a Request/Response is maintained by this enum.
/// The state progresses from Routed -> Sent -> Received
#[derive(Debug)]
enum PendingRequestState {
    /// A route has been determined for this request but it has not yet been sent.
    Routed {
        destination: Destination,
        request: Message,
    },
    /// The request has been sent to the specified broker and we are now awaiting a response from that broker.
    Sent {
        destination: Destination,
        /// How many responses must be received before this response is received.
        /// When this is 0 the next response from the broker will be for this request.
        /// This field must be manually decremented when another response for this broker comes through.
        index: usize,
    },
    /// The broker has returned a Response to this request.
    /// Returning this response may be delayed until a response to an earlier request comes back from another broker.
    Received { response: Message },
}

impl PendingRequestState {
    fn routed(broker_id: BrokerId, request: Message) -> Self {
        Self::Routed {
            destination: Destination::Id(broker_id),
            request,
        }
    }
}

struct PendingRequest {
    state: PendingRequestState,
    /// Combine the next N responses into a single response
    /// This message should be considered the base message and will retain the shotover Message::id and kafka correlation_id
    combine_responses: usize,
}

#[async_trait]
impl Transform for KafkaSinkCluster {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        let mut responses = if chain_state.requests.is_empty() {
            // there are no requests, so no point sending any, but we should check for any responses without awaiting
            self.recv_responses()
                .await
                .context("Failed to receive responses (without sending requests)")?
        } else {
            self.update_local_nodes().await;

            for request in &mut chain_state.requests {
                let id = request.id();
                if let Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::FindCoordinator(find_coordinator),
                    ..
                })) = request.frame()
                {
                    self.find_coordinator_requests.insert(
                        id,
                        FindCoordinator {
                            key: find_coordinator.key.clone(),
                            key_type: find_coordinator.key_type,
                        },
                    );
                }
            }

            self.route_requests(std::mem::take(&mut chain_state.requests))
                .await
                .context("Failed to route requests")?;
            self.send_requests()
                .await
                .context("Failed to send requests")?;
            self.recv_responses()
                .await
                .context("Failed to receive responses")?
        };

        self.process_responses(&mut responses, &mut chain_state.close_client_connection)
            .await
            .context("Failed to process responses")?;
        Ok(responses)
    }
}

impl KafkaSinkCluster {
    /// Send a request over the control connection and immediately receive the response.
    /// Since we always await the response we know for sure that the response will not get mixed up with any other incoming responses.
    async fn control_send_receive(&mut self, request: Message) -> Result<Message> {
        match self.control_send_receive_inner(request.clone()).await {
            Ok(response) => Ok(response),
            Err(err) => {
                // first retry on the same connection in case it was a timeout
                match self
                    .connections
                    .handle_connection_error(
                        &self.connection_factory,
                        &self.authorize_scram_over_mtls,
                        &self.sasl_mechanism,
                        &self.nodes,
                        Destination::ControlConnection,
                        err,
                    )
                    .await
                {
                    // connection recreated succesfully, retry on the original node
                    // if the request fails at this point its a bad request.
                    Ok(()) => self.control_send_receive_inner(request).await,
                    // connection failed, could be a bad node, retry on all known nodes
                    Err(err) => {
                        tracing::warn!("Failed to recreate original control connection {err:?}");
                        loop {
                            // remove the old control connection to force control_send_receive_inner to create a new one.
                            self.connections
                                .connections
                                .remove(&Destination::ControlConnection);
                            match self.control_send_receive_inner(request.clone()).await {
                                // found a new node that works
                                Ok(response) => return Ok(response),
                                // this node also doesnt work, mark as bad and try a new one.
                                Err(err) => {
                                    if self.nodes.iter().all(|x| !x.is_up()) {
                                        return Err(err.context("Failed to recreate control connection, no more brokers to retry on. Last broker gave error"));
                                    } else {
                                        tracing::warn!(
                                            "Failed to recreate control connection against a new broker {err:?}"
                                        );
                                        // try another node
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn control_send_receive_inner(&mut self, request: Message) -> Result<Message> {
        assert!(
            self.auth_complete,
            "control_send_receive cannot be called until auth is complete. Otherwise it would collide with the control connection being used for regular routing."
        );
        let connection = self
            .connections
            .get_or_open_connection(
                &mut self.rng,
                &self.connection_factory,
                &self.authorize_scram_over_mtls,
                &self.sasl_mechanism,
                &self.nodes,
                &self.first_contact_points,
                &self.rack,
                Instant::now(),
                Destination::ControlConnection,
            )
            .await
            .context("Failed to get control connection")?;
        connection.send(vec![request])?;
        Ok(connection.recv().await?.remove(0))
    }

    fn store_topic_names(&self, topics: &mut Vec<TopicName>, topic: TopicName) {
        let cache_is_missing_or_outdated = match self.topic_by_name.get(&topic) {
            Some(topic) => topic.partitions.iter().any(|partition| {
                // refetch the metadata if the metadata believes that a partition is stored at a down node.
                // The possible results are:
                // * The node is actually up and the partition is there, the node will be marked as up once a request has been succesfully routed to it.
                // * The node is actually down and the partition has moved, refetching the metadata will allow us to find the new destination.
                // * The node is actually down and the partition has not yet moved, refetching the metadata will have us attempt to route to the down node.
                //       Shotover will close the connection and the client will retry the request.
                self.nodes
                    .iter()
                    .find(|node| node.broker_id == *partition.leader_id)
                    .map(|node| !node.is_up())
                    .unwrap_or(false)
            }),
            None => true,
        };
        if cache_is_missing_or_outdated && !topics.contains(&topic) && !topic.is_empty() {
            topics.push(topic);
        }
    }

    fn store_topic_ids(&self, topics: &mut Vec<Uuid>, topic: Uuid) {
        let cache_is_missing_or_outdated = match self.topic_by_id.get(&topic) {
            Some(topic) => topic.partitions.iter().any(|partition| {
                // refetch the metadata if the metadata believes that a partition is stored at a down node.
                // The possible results are:
                // * The node is actually up and the partition is there, the node will be marked as up once a request has been succesfully routed to it.
                // * The node is actually down and the partition has moved, refetching the metadata will allow us to find the new destination.
                // * The node is actually down and the partition has not yet moved, refetching the metadata will have us attempt to route to the down node.
                //       Shotover will close the connection and the client will retry the request.
                self.nodes
                    .iter()
                    .find(|node| node.broker_id == *partition.leader_id)
                    .map(|node| !node.is_up())
                    .unwrap_or(false)
            }),
            None => true,
        };
        if cache_is_missing_or_outdated && !topics.contains(&topic) && !topic.is_nil() {
            topics.push(topic);
        }
    }

    fn store_group(&self, groups: &mut Vec<GroupId>, group_id: GroupId) {
        let cache_is_missing_or_outdated = match self.group_to_coordinator_broker.get(&group_id) {
            Some(broker_id) => self
                .nodes
                .iter()
                .find(|node| node.broker_id == *broker_id)
                .map(|node| !node.is_up())
                .unwrap_or(true),
            None => true,
        };

        if cache_is_missing_or_outdated && !groups.contains(&group_id) {
            debug_assert!(group_id.0.as_str() != "");
            groups.push(group_id);
        }
    }

    async fn update_local_nodes(&mut self) {
        self.nodes.clone_from(&*self.nodes_shared.read().await);
    }

    async fn route_requests(&mut self, mut requests: Vec<Message>) -> Result<()> {
        if !self.auth_complete {
            let mut handshake_request_count = 0;
            for request in &mut requests {
                match request.frame() {
                    Some(Frame::Kafka(KafkaFrame::Request {
                        body: RequestBody::SaslHandshake(SaslHandshakeRequest { mechanism, .. }),
                        ..
                    })) => {
                        mechanism.as_str();

                        self.sasl_mechanism = Some(mechanism.as_str().to_owned());
                        self.connection_factory.add_auth_request(request.clone());
                        handshake_request_count += 1;
                    }
                    Some(Frame::Kafka(KafkaFrame::Request {
                        body:
                            RequestBody::SaslAuthenticate(SaslAuthenticateRequest {
                                auth_bytes, ..
                            }),
                        ..
                    })) => {
                        if let Some(scram_over_mtls) = &mut self.authorize_scram_over_mtls {
                            if let Some(username) = get_username_from_scram_request(auth_bytes) {
                                scram_over_mtls.set_username(username).await?;
                            }
                        }
                        self.connection_factory.add_auth_request(request.clone());
                        handshake_request_count += 1;
                    }
                    Some(Frame::Kafka(KafkaFrame::Request {
                        body: RequestBody::ApiVersions(_),
                        ..
                    })) => {
                        handshake_request_count += 1;
                    }
                    _ => {
                        // The client is no longer performing authentication, so consider auth completed

                        if let Some(scram_over_mtls) = &self.authorize_scram_over_mtls {
                            // When performing SCRAM over mTLS, we need this security check to ensure that the
                            // client cannot access delegation tokens that it has not successfully authenticated for.
                            //
                            // If the client were to send a request directly after the SCRAM requests,
                            // without waiting for responses to those scram requests first,
                            // this error would be triggered even if the SCRAM requests were successful.
                            // However that would be a violation of the SCRAM protocol as the client is supposed to check
                            // the server's signature contained in the server's final message in order to authenticate the server.
                            // So I dont think this problem is worth solving.
                            if !matches!(
                                scram_over_mtls.original_scram_state,
                                OriginalScramState::AuthSuccess
                            ) {
                                return Err(anyhow!("Client attempted to send requests before a successful auth was completed or after an unsuccessful auth"));
                            }
                        }

                        self.auth_complete = true;
                        break;
                    }
                }
            }
            // route all handshake messages
            for _ in 0..handshake_request_count {
                let request = requests.remove(0);
                self.route_to_control_connection(request);
            }

            if requests.is_empty() {
                // all messages received in this batch are handshake messages,
                // so dont continue with regular message handling
                return Ok(());
            } else {
                // the later messages in this batch are not handshake messages,
                // so continue onto the regular message handling
            }
        }

        let mut topic_names = vec![];
        let mut topic_ids = vec![];
        let mut groups = vec![];
        for request in &mut requests {
            match request.frame() {
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Produce(produce),
                    ..
                })) => {
                    for (name, _) in &produce.topic_data {
                        self.store_topic_names(&mut topic_names, name.clone());
                    }
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Fetch(fetch),
                    ..
                })) => {
                    for topic in &fetch.topics {
                        self.store_topic_names(&mut topic_names, topic.topic.clone());
                        self.store_topic_ids(&mut topic_ids, topic.topic_id);
                    }
                    fetch.session_id = 0;
                    fetch.session_epoch = -1;
                    request.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body:
                        RequestBody::Heartbeat(HeartbeatRequest { group_id, .. })
                        | RequestBody::SyncGroup(SyncGroupRequest { group_id, .. })
                        | RequestBody::JoinGroup(JoinGroupRequest { group_id, .. })
                        | RequestBody::LeaveGroup(LeaveGroupRequest { group_id, .. }),
                    ..
                })) => {
                    self.store_group(&mut groups, group_id.clone());
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::OffsetFetch(offset_fetch),
                    header,
                })) => {
                    if header.request_api_version <= 7 {
                        self.store_group(&mut groups, offset_fetch.group_id.clone());
                    } else {
                        for group in &offset_fetch.groups {
                            self.store_group(&mut groups, group.group_id.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        for group in groups {
            match self.find_coordinator_of_group(group.clone()).await {
                Ok(node) => {
                    tracing::debug!(
                        "Storing group_to_coordinator_broker metadata, group {:?} -> broker {}",
                        group.0,
                        node.broker_id.0
                    );
                    self.group_to_coordinator_broker
                        .insert(group, node.broker_id);
                    self.add_node_if_new(node).await;
                }
                Err(FindCoordinatorError::CoordinatorNotAvailable) => {
                    // We cant find the coordinator so do nothing so that the request will be routed to a random node:
                    // * If it happens to be the coordinator all is well
                    // * If its not the coordinator then it will return a NOT_COORDINATOR message to
                    //   the client prompting it to retry the whole process again.
                }
                Err(FindCoordinatorError::Unrecoverable(err)) => Err(err)?,
            }
        }

        // request and process metadata if we are missing topics or the controller broker id
        if !topic_names.is_empty()
            || !topic_ids.is_empty()
            || self.controller_broker.get().is_none()
        {
            let mut metadata = self.get_metadata_of_topics(topic_names, topic_ids).await?;
            match metadata.frame() {
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Metadata(metadata),
                    ..
                })) => {
                    for topic in metadata.topics.values() {
                        match ResponseError::try_from_code(topic.error_code) {
                            Some(ResponseError::UnknownTopicOrPartition) => {
                                // We need to look up all topics sent to us by the client
                                // but the client may request a topic that doesnt exist.
                            }
                            Some(err) => {
                                // Some other kind of error, better to terminate the connection
                                return Err(anyhow!(
                                    "Kafka responded to Metadata request with error {err:?}"
                                ));
                            }
                            None => {}
                        }
                    }
                    self.process_metadata_response(metadata).await
                }
                other => {
                    return Err(anyhow!(
                        "Unexpected message returned to metadata request {other:?}"
                    ))
                }
            }
        }

        for mut message in requests {
            // This routing is documented in transforms.md so make sure to update that when making changes here.
            match message.frame() {
                // route to partition leader
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Produce(_),
                    ..
                })) => self.route_produce_request(message)?,

                // route to random partition replica
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Fetch(_),
                    ..
                })) => self.route_fetch_request(message)?,

                // route to group coordinator
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::Heartbeat(heartbeat),
                    ..
                })) => {
                    let group_id = heartbeat.group_id.clone();
                    self.route_to_coordinator(message, group_id);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::SyncGroup(sync_group),
                    ..
                })) => {
                    let group_id = sync_group.group_id.clone();
                    self.route_to_coordinator(message, group_id);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::OffsetFetch(offset_fetch),
                    header,
                })) => {
                    let group_id = if header.request_api_version <= 7 {
                        offset_fetch.group_id.clone()
                    } else {
                        // This is possibly dangerous.
                        // The client could construct a message which is valid for a specific shotover node, but not for any single kafka broker.
                        // We may need to add some logic to split the request into multiple messages going to different destinations,
                        // and then reconstruct the response back into a single response
                        //
                        // For now just pick the first group as that is sufficient for the simple cases.
                        offset_fetch.groups.first().unwrap().group_id.clone()
                    };
                    self.route_to_coordinator(message, group_id);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::OffsetCommit(offset_commit),
                    ..
                })) => {
                    let group_id = offset_commit.group_id.clone();
                    self.route_to_coordinator(message, group_id);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::JoinGroup(join_group),
                    ..
                })) => {
                    let group_id = join_group.group_id.clone();
                    self.route_to_coordinator(message, group_id);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::LeaveGroup(leave_group),
                    ..
                })) => {
                    let group_id = leave_group.group_id.clone();
                    self.route_to_coordinator(message, group_id);
                }
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::DeleteGroups(groups),
                    ..
                })) => {
                    // TODO: we need to split this up into multiple requests so it can be correctly routed to all possible nodes
                    let group_id = groups.groups_names.first().unwrap().clone();
                    self.route_to_coordinator(message, group_id);
                }

                // route to controller broker
                Some(Frame::Kafka(KafkaFrame::Request {
                    body: RequestBody::CreateTopics(_),
                    ..
                })) => self.route_to_controller(message),
                // route to random node
                _ => {
                    let destination = random_broker_id(&self.nodes, &mut self.rng);
                    tracing::debug!("Routing request to random broker {}", destination.0);
                    self.pending_requests.push_back(PendingRequest {
                        state: PendingRequestState::routed(destination, message),
                        combine_responses: 1,
                    });
                }
            }
        }
        Ok(())
    }

    fn route_produce_request(&mut self, mut message: Message) -> Result<()> {
        if let Some(Frame::Kafka(KafkaFrame::Request {
            body: RequestBody::Produce(produce),
            ..
        })) = message.frame()
        {
            let routing = self.split_produce_request_by_destination(produce);

            if routing.is_empty() {
                // Produce contains no topics, so we can just pick a random destination.
                // The message is unchanged so we can just send as is.
                let destination = random_broker_id(&self.nodes, &mut self.rng);

                self.pending_requests.push_back(PendingRequest {
                    state: PendingRequestState::routed(destination, message),
                    combine_responses: 1,
                });
                tracing::debug!(
                    "Routing produce request to random broker {} due to being empty",
                    destination.0
                );
            } else if routing.len() == 1 {
                // Only 1 destination,
                // so we can just reconstruct the original message as is,
                // act like this never happened 😎,
                // we dont even need to invalidate the message's cache.
                let (destination, topic_data) = routing.into_iter().next().unwrap();
                let destination = if destination == -1 {
                    random_broker_id(&self.nodes, &mut self.rng)
                } else {
                    destination
                };

                produce.topic_data = topic_data;
                self.pending_requests.push_back(PendingRequest {
                    state: PendingRequestState::routed(destination, message),
                    combine_responses: 1,
                });
                tracing::debug!(
                    "Routing produce request to single broker {:?}",
                    destination.0
                );
            } else {
                // The message has been split so it may be delivered to multiple destinations.
                // We must generate a unique message for each destination.
                let combine_responses = routing.len();
                message.invalidate_cache();
                for (i, (destination, topic_data)) in routing.into_iter().enumerate() {
                    let destination = if destination == -1 {
                        random_broker_id(&self.nodes, &mut self.rng)
                    } else {
                        destination
                    };
                    let mut request = if i == 0 {
                        // First message acts as base and retains message id
                        message.clone()
                    } else {
                        message.clone_with_new_id()
                    };
                    if let Some(Frame::Kafka(KafkaFrame::Request {
                        body: RequestBody::Produce(produce),
                        ..
                    })) = request.frame()
                    {
                        produce.topic_data = topic_data;
                    }
                    self.pending_requests.push_back(PendingRequest {
                        state: PendingRequestState::routed(destination, request),
                        combine_responses,
                    });
                }
                tracing::debug!("Routing produce request to multiple brokers");
            }
        }

        Ok(())
    }

    /// This method removes all topics from the produce request and returns them split up by their destination
    /// If any topics are unroutable they will have their destination BrokerId set to -1
    fn split_produce_request_by_destination(
        &mut self,
        produce: &mut ProduceRequest,
    ) -> HashMap<BrokerId, IndexMap<TopicName, TopicProduceData>> {
        let mut result: HashMap<BrokerId, IndexMap<TopicName, TopicProduceData>> =
            Default::default();

        for (name, mut topic) in produce.topic_data.drain(..) {
            let topic_meta = self.topic_by_name.get(&name);
            if let Some(topic_meta) = topic_meta {
                for partition in std::mem::take(&mut topic.partition_data) {
                    let partition_index = partition.index as usize;
                    let destination = if let Some(partition) =
                        topic_meta.partitions.get(partition_index)
                    {
                        if partition.leader_id == -1 {
                            tracing::warn!("leader_id is unknown for topic {name:?} at partition index {partition_index}");
                        }
                        partition.leader_id
                    } else {
                        let partition_len = topic_meta.partitions.len();
                        tracing::warn!("no known partition replica for {name:?} at partition index {partition_index} out of {partition_len} partitions, routing request to a random broker so that a NOT_LEADER_OR_FOLLOWER or similar error is returned to the client");
                        BrokerId(-1)
                    };
                    tracing::debug!(
                        "Routing produce request portion of partition {partition_index} in topic {} to broker {}",
                        name.0,
                        destination.0
                    );

                    // Get the topics already routed to this destination
                    let routed_topics = result.entry(destination).or_default();

                    if let Some(routed_topic) = routed_topics.get_mut(&name) {
                        // we have already routed this topic to this broker, add another partition
                        routed_topic.partition_data.push(partition);
                    } else {
                        // we have not yet routed this topic to this broker, add the first partition
                        // Clone the original topic value, to ensure we carry over any `unknown_tagged_fields` values.
                        // The partition_data is empty at this point due to the previous `std::mem::take`
                        let mut topic = topic.clone();
                        topic.partition_data.push(partition);
                        routed_topics.insert(name.clone(), topic);
                    }
                }
            } else {
                tracing::debug!(
                    r#"no known partition leader for {name:?}
        routing request to a random broker so that:
        * if auto topic creation is enabled, auto topic creation will occur
        * if auto topic creation is disabled a NOT_LEADER_OR_FOLLOWER is returned to the client"#
                );
                let destination = BrokerId(-1);
                let dest_topics = result.entry(destination).or_default();
                dest_topics.insert(name, topic);
            }
        }

        result
    }

    /// This method removes all topics from the fetch request and returns them split up by their destination
    /// If any topics are unroutable they will have their BrokerId set to -1
    fn split_fetch_request_by_destination(
        &mut self,
        fetch: &mut FetchRequest,
    ) -> HashMap<BrokerId, Vec<FetchTopic>> {
        let mut result: HashMap<BrokerId, Vec<FetchTopic>> = Default::default();

        for mut topic in fetch.topics.drain(..) {
            // This way of constructing topic_meta is kind of crazy, but it works around borrow checker limitations
            // Old clients only specify the topic name and some newer clients only specify the topic id.
            // So we need to check the id first and then fallback to the name.
            let topic_by_id = self.topic_by_id.get(&topic.topic_id);
            let topic_by_name;
            let mut topic_meta = topic_by_id.as_deref();
            if topic_meta.is_none() {
                topic_by_name = self.topic_by_name.get(&topic.topic);
                topic_meta = topic_by_name.as_deref();
            }
            let format_topic_name = FormatTopicName(&topic.topic, &topic.topic_id);
            if let Some(topic_meta) = topic_meta {
                for partition in std::mem::take(&mut topic.partitions) {
                    let partition_index = partition.partition as usize;
                    let destination = if let Some(partition) =
                        topic_meta.partitions.get(partition_index)
                    {
                        // While technically kafka has some support for fetching from replicas, its quite weird.
                        // See https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica
                        // We should never route to replica_nodes from the metadata response ourselves.
                        // Instead, when its available, we can make use of preferred_read_replica field in the fetch response as an optimization.
                        // However its always correct to route to the partition.leader_id which is what we do here.
                        if partition.leader_id == -1 {
                            tracing::warn!(
                                "leader_id is unknown for {format_topic_name} at partition index {partition_index}"
                            );
                        }
                        partition.leader_id
                    } else {
                        let partition_len = topic_meta.partitions.len();
                        tracing::warn!("no known partition replica for {format_topic_name} at partition index {partition_index} out of {partition_len} partitions, routing message to a random broker so that a NOT_LEADER_OR_FOLLOWER or similar error is returned to the client");
                        BrokerId(-1)
                    };
                    tracing::debug!(
                        "Routing fetch request portion of partition {partition_index} in {format_topic_name} to broker {}",
                        destination.0
                    );
                    let dest_topics = result.entry(destination).or_default();
                    if let Some(dest_topic) = dest_topics
                        .iter_mut()
                        .find(|x| x.topic_id == topic.topic_id && x.topic == topic.topic)
                    {
                        dest_topic.partitions.push(partition);
                    } else {
                        let mut topic = topic.clone();
                        topic.partitions.push(partition);
                        dest_topics.push(topic);
                    }
                }
            } else {
                tracing::warn!("no known partition replica for {format_topic_name}, routing message to a random broker so that a NOT_LEADER_OR_FOLLOWER or similar error is returned to the client");
                let destination = BrokerId(-1);
                let dest_topics = result.entry(destination).or_default();
                dest_topics.push(topic);
            }
        }

        result
    }

    fn route_fetch_request(&mut self, mut message: Message) -> Result<()> {
        if let Some(Frame::Kafka(KafkaFrame::Request {
            body: RequestBody::Fetch(fetch),
            ..
        })) = message.frame()
        {
            let routing = self.split_fetch_request_by_destination(fetch);

            if routing.is_empty() {
                // Fetch contains no topics, so we can just pick a random destination.
                // The message is unchanged so we can just send as is.
                let destination = random_broker_id(&self.nodes, &mut self.rng);

                self.pending_requests.push_back(PendingRequest {
                    state: PendingRequestState::routed(destination, message),
                    combine_responses: 1,
                });
                tracing::debug!(
                    "Routing fetch request to random broker {} due to being empty",
                    destination.0
                );
            } else if routing.len() == 1 {
                // Only 1 destination,
                // so we can just reconstruct the original message as is,
                // act like this never happened 😎,
                // we dont even need to invalidate the message's cache.
                let (destination, topics) = routing.into_iter().next().unwrap();
                let destination = if destination == -1 {
                    random_broker_id(&self.nodes, &mut self.rng)
                } else {
                    destination
                };

                fetch.topics = topics;
                self.pending_requests.push_back(PendingRequest {
                    state: PendingRequestState::routed(destination, message),
                    combine_responses: 1,
                });
                tracing::debug!("Routing fetch request to single broker {}", destination.0);
            } else {
                // The message has been split so it may be delivered to multiple destinations.
                // We must generate a unique message for each destination.
                let combine_responses = routing.len();
                message.invalidate_cache();
                for (i, (destination, topics)) in routing.into_iter().enumerate() {
                    let destination = if destination == -1 {
                        random_broker_id(&self.nodes, &mut self.rng)
                    } else {
                        destination
                    };
                    let mut request = if i == 0 {
                        // First message acts as base and retains message id
                        message.clone()
                    } else {
                        message.clone_with_new_id()
                    };
                    if let Some(Frame::Kafka(KafkaFrame::Request {
                        body: RequestBody::Fetch(fetch),
                        ..
                    })) = request.frame()
                    {
                        fetch.topics = topics;
                    }
                    self.pending_requests.push_back(PendingRequest {
                        state: PendingRequestState::routed(destination, request),
                        combine_responses,
                    });
                }
                tracing::debug!("Routing fetch request to multiple brokers");
            }
        }

        Ok(())
    }

    async fn find_coordinator_of_group(
        &mut self,
        group: GroupId,
    ) -> Result<KafkaNode, FindCoordinatorError> {
        let request = Message::from_frame(Frame::Kafka(KafkaFrame::Request {
            header: RequestHeader::default()
                .with_request_api_key(ApiKey::FindCoordinatorKey as i16)
                .with_request_api_version(2)
                .with_correlation_id(0),
            body: RequestBody::FindCoordinator(
                FindCoordinatorRequest::default()
                    .with_key_type(0)
                    .with_key(group.0.clone()),
            ),
        }));

        let mut response = self
            .control_send_receive(request)
            .await
            .with_context(|| format!("Failed to query for coordinator of group {:?}", group.0))?;
        match response.frame() {
            Some(Frame::Kafka(KafkaFrame::Response {
                body: ResponseBody::FindCoordinator(coordinator),
                ..
            })) => match ResponseError::try_from_code(coordinator.error_code) {
                None => Ok(KafkaNode::new(
                    coordinator.node_id,
                    KafkaAddress::new(coordinator.host.clone(), coordinator.port),
                    None,
                )),
                Some(ResponseError::CoordinatorNotAvailable) => {
                    Err(FindCoordinatorError::CoordinatorNotAvailable)
                }
                Some(err) => Err(FindCoordinatorError::Unrecoverable(anyhow!(
                    "Unexpected server error from FindCoordinator {err}"
                ))),
            },
            other => Err(anyhow!(
                "Unexpected message returned to findcoordinator request {other:?}"
            ))?,
        }
    }

    async fn get_metadata_of_topics(
        &mut self,
        topic_names: Vec<TopicName>,
        topic_ids: Vec<Uuid>,
    ) -> Result<Message> {
        let api_version = if topic_ids.is_empty() { 4 } else { 12 };
        let request = Message::from_frame(Frame::Kafka(KafkaFrame::Request {
            header: RequestHeader::default()
                .with_request_api_key(ApiKey::MetadataKey as i16)
                .with_request_api_version(api_version)
                .with_correlation_id(0),
            body: RequestBody::Metadata(
                MetadataRequest::default().with_topics(Some(
                    topic_names
                        .into_iter()
                        .map(|name| MetadataRequestTopic::default().with_name(Some(name)))
                        .chain(topic_ids.into_iter().map(|id| {
                            MetadataRequestTopic::default()
                                .with_name(None)
                                .with_topic_id(id)
                        }))
                        .collect(),
                )),
            ),
        }));

        self.control_send_receive(request)
            .await
            .context("Failed to query metadata of topics")
    }

    /// Convert all PendingRequestState::Routed into PendingRequestState::Sent
    async fn send_requests(&mut self) -> Result<()> {
        struct RoutedRequests {
            requests: Vec<Message>,
            already_pending: usize,
        }

        let mut broker_to_routed_requests: HashMap<Destination, RoutedRequests> = HashMap::new();
        for i in 0..self.pending_requests.len() {
            if let PendingRequestState::Routed { destination, .. } = &self.pending_requests[i].state
            {
                let routed_requests = broker_to_routed_requests
                    .entry(*destination)
                    .or_insert_with(|| RoutedRequests {
                        requests: vec![],
                        already_pending: self
                            .pending_requests
                            .iter()
                            .filter(|pending_request| {
                                if let PendingRequestState::Sent {
                                    destination: check_destination,
                                    ..
                                } = &pending_request.state
                                {
                                    check_destination == destination
                                } else {
                                    false
                                }
                            })
                            .count(),
                    });
                let mut value = PendingRequestState::Sent {
                    destination: *destination,
                    index: routed_requests.requests.len() + routed_requests.already_pending,
                };
                std::mem::swap(&mut self.pending_requests[i].state, &mut value);
                if let PendingRequestState::Routed { request, .. } = value {
                    routed_requests.requests.push(request);
                }
            }
        }

        let recent_instant = Instant::now();
        for (destination, mut requests) in broker_to_routed_requests {
            match self
                .connections
                .get_or_open_connection(
                    &mut self.rng,
                    &self.connection_factory,
                    &self.authorize_scram_over_mtls,
                    &self.sasl_mechanism,
                    &self.nodes,
                    &self.first_contact_points,
                    &self.rack,
                    recent_instant,
                    destination,
                )
                .await
            {
                Ok(connection) => {
                    if let Err(err) = connection.send(requests.requests) {
                        // Dont retry the send on the new connection since we cant tell if the broker received the request or not.
                        self.connections
                            .handle_connection_error(
                                &self.connection_factory,
                                &self.authorize_scram_over_mtls,
                                &self.sasl_mechanism,
                                &self.nodes,
                                destination,
                                err.clone().into(),
                            )
                            .await?;
                        // If we succesfully recreate the outgoing connection we still need to terminate this incoming connection since the request is lost.
                        return Err(err.into());
                    }
                }
                Err(err) => {
                    // set node as down, the connection already failed to create so no point running through handle_connection_error,
                    // as that will recreate the connection which we already know just failed.
                    // Instead just directly set the node as down and return the error

                    // set node as down
                    self.nodes
                        .iter()
                        .find(|x| match destination {
                            Destination::Id(id) => x.broker_id == id,
                            Destination::ControlConnection => {
                                &x.kafka_address
                                    == self
                                        .connections
                                        .control_connection_address
                                        .as_ref()
                                        .unwrap()
                            }
                        })
                        .unwrap()
                        .set_state(KafkaNodeState::Down);

                    // bubble up error
                    let request_types: Vec<String> = requests
                        .requests
                        .iter_mut()
                        .map(|x| match x.frame() {
                            Some(Frame::Kafka(KafkaFrame::Request { header, .. })) => {
                                format!("{:?}", ApiKey::try_from(header.request_api_key).unwrap())
                            }
                            _ => "Unknown".to_owned(),
                        })
                        .collect();
                    return Err(err.context(format!(
                        "Failed to get connection to send requests {request_types:?}"
                    )));
                }
            }
        }

        Ok(())
    }

    /// Receive all responses from the outgoing connections, returns all responses that are ready to be returned.
    /// For response ordering reasons, some responses will remain in self.pending_requests until other responses are received.
    async fn recv_responses(&mut self) -> Result<Vec<Message>> {
        // To work around borrow checker issues, store connection errors in this temporary list before handling them.
        let mut connection_errors = vec![];

        // Convert all received PendingRequestState::Sent into PendingRequestState::Received
        for (connection_destination, connection) in &mut self.connections.connections {
            // skip recv when no pending requests to avoid timeouts on old connections
            if connection.pending_requests_count() != 0 {
                self.temp_responses_buffer.clear();
                match connection
                    .try_recv_into(&mut self.temp_responses_buffer)
                    .with_context(|| format!("Failed to receive from {connection_destination:?}"))
                {
                    Ok(()) => {
                        for response in self.temp_responses_buffer.drain(..) {
                            let mut response = Some(response);
                            for pending_request in &mut self.pending_requests {
                                if let PendingRequestState::Sent { destination, index } =
                                    &mut pending_request.state
                                {
                                    if destination == connection_destination {
                                        // Store the PendingRequestState::Received at the location of the next PendingRequestState::Sent
                                        // All other PendingRequestState::Sent need to be decremented, in order to determine the PendingRequestState::Sent
                                        // to be used next time, and the time after that, and ...
                                        if *index == 0 {
                                            pending_request.state = PendingRequestState::Received {
                                                response: response.take().unwrap(),
                                            };
                                        } else {
                                            *index -= 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => connection_errors.push((*connection_destination, err)),
                }
            }
        }

        for (destination, err) in connection_errors {
            // Since pending_requests_count > 0 we expect this to return an Err.
            self.connections
                .handle_connection_error(
                    &self.connection_factory,
                    &self.authorize_scram_over_mtls,
                    &self.sasl_mechanism,
                    &self.nodes,
                    destination,
                    err,
                )
                .await?;
        }

        // Remove and return all PendingRequestState::Received that are ready to be received.
        let mut responses = vec![];
        while let Some(pending_request) = self.pending_requests.front() {
            let all_combined_received = (0..pending_request.combine_responses).all(|i| {
                matches!(
                    self.pending_requests.get(i),
                    Some(PendingRequest {
                        state: PendingRequestState::Received { .. },
                        ..
                    })
                )
            });
            if all_combined_received {
                // The next response we are waiting on has been received, add it to responses
                if pending_request.combine_responses == 1 {
                    if let Some(PendingRequest {
                        state: PendingRequestState::Received { response },
                        ..
                    }) = self.pending_requests.pop_front()
                    {
                        responses.push(response);
                    }
                } else {
                    let drain = self
                        .pending_requests
                        .drain(..pending_request.combine_responses)
                        .map(|x| {
                            if let PendingRequest {
                                state: PendingRequestState::Received { response },
                                ..
                            } = x
                            {
                                response
                            } else {
                                unreachable!("Guaranteed by all_combined_received")
                            }
                        });
                    responses.push(Self::combine_responses(drain)?);
                }
            } else {
                // The pending_request is not received, we need to break to maintain response ordering.
                break;
            }
        }
        Ok(responses)
    }

    fn combine_responses(mut drain: impl Iterator<Item = Message>) -> Result<Message> {
        // Take this response as base.
        // Then iterate over all remaining combined responses and integrate them into the base.
        let mut base = drain.next().unwrap();
        base.invalidate_cache();

        match base.frame() {
            Some(Frame::Kafka(KafkaFrame::Response {
                body: ResponseBody::Fetch(base),
                ..
            })) => Self::combine_fetch_responses(base, drain)?,
            Some(Frame::Kafka(KafkaFrame::Response {
                body: ResponseBody::Produce(base),
                ..
            })) => Self::combine_produce_responses(base, drain)?,
            _ => {
                return Err(anyhow!(
                    "Combining of this message type is currently unsupported"
                ))
            }
        }

        Ok(base)
    }

    fn combine_fetch_responses(
        base_fetch: &mut FetchResponse,
        drain: impl Iterator<Item = Message>,
    ) -> Result<()> {
        for mut next in drain {
            if let Some(Frame::Kafka(KafkaFrame::Response {
                body: ResponseBody::Fetch(next_fetch),
                ..
            })) = next.frame()
            {
                for next_response in std::mem::take(&mut next_fetch.responses) {
                    if let Some(base_response) = base_fetch.responses.iter_mut().find(|response| {
                        response.topic == next_response.topic
                            && response.topic_id == next_response.topic_id
                    }) {
                        for next_partition in &next_response.partitions {
                            for base_partition in &base_response.partitions {
                                if next_partition.partition_index == base_partition.partition_index
                                {
                                    tracing::warn!("Duplicate partition indexes in combined fetch response, if this ever occurs we should investigate the repercussions")
                                }
                            }
                        }
                        // A partition can only be contained in one response so there is no risk of duplicating partitions
                        base_response.partitions.extend(next_response.partitions)
                    } else {
                        base_fetch.responses.push(next_response);
                    }
                }
            } else {
                return Err(anyhow!(
                    "Combining Fetch responses but received another message type"
                ));
            }
        }

        Ok(())
    }

    fn combine_produce_responses(
        base_produce: &mut ProduceResponse,
        drain: impl Iterator<Item = Message>,
    ) -> Result<()> {
        for mut next in drain {
            if let Some(Frame::Kafka(KafkaFrame::Response {
                body: ResponseBody::Produce(next_produce),
                ..
            })) = next.frame()
            {
                for (next_name, next_response) in std::mem::take(&mut next_produce.responses) {
                    if let Some(base_response) = base_produce.responses.get_mut(&next_name) {
                        for next_partition in &next_response.partition_responses {
                            for base_partition in &base_response.partition_responses {
                                if next_partition.index == base_partition.index {
                                    tracing::warn!("Duplicate partition indexes in combined produce response, if this ever occurs we should investigate the repercussions")
                                }
                            }
                        }
                        // A partition can only be contained in one response so there is no risk of duplicating partitions
                        base_response
                            .partition_responses
                            .extend(next_response.partition_responses)
                    } else {
                        base_produce.responses.insert(next_name, next_response);
                    }
                }
            } else {
                return Err(anyhow!(
                    "Combining Produce responses but received another message type"
                ));
            }
        }

        Ok(())
    }

    async fn process_responses(
        &mut self,
        responses: &mut [Message],
        close_client_connection: &mut bool,
    ) -> Result<()> {
        for response in responses.iter_mut() {
            let request_id = response.request_id().unwrap();
            match response.frame() {
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::FindCoordinator(find_coordinator),
                    version,
                    ..
                })) => {
                    let request = self
                        .find_coordinator_requests
                        .remove(&request_id)
                        .ok_or_else(|| anyhow!("Received find_coordinator but not requested"))?;

                    self.process_find_coordinator_response(*version, request, find_coordinator);
                    self.rewrite_find_coordinator_response(*version, find_coordinator);
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::SaslHandshake(handshake),
                    ..
                })) => {
                    // If authorize_scram_over_mtls is disabled there is no way that scram can work through KafkaSinkCluster
                    // since it is specifically designed such that replay attacks wont work.
                    // So when authorize_scram_over_mtls is disabled report to the user that SCRAM is not enabled.
                    if self.authorize_scram_over_mtls.is_none() {
                        // remove scram from supported mechanisms
                        handshake
                            .mechanisms
                            .retain(|x| !SASL_SCRAM_MECHANISMS.contains(&x.as_str()));

                        // declare unsupported if the client requested SCRAM
                        if let Some(sasl_mechanism) = &self.sasl_mechanism {
                            if SASL_SCRAM_MECHANISMS.contains(&sasl_mechanism.as_str()) {
                                handshake.error_code =
                                    ResponseError::UnsupportedSaslMechanism.code();
                            }
                        }

                        response.invalidate_cache();
                    }
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::SaslAuthenticate(authenticate),
                    ..
                })) => {
                    self.process_sasl_authenticate(authenticate, close_client_connection)?;
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Produce(produce),
                    ..
                })) => {
                    // Clear this optional field to avoid making clients try to bypass shotover
                    produce.node_endpoints.clear();
                    for (topic_name, response_topic) in &mut produce.responses {
                        for response_partition in &response_topic.partition_responses {
                            if let Some(ResponseError::NotLeaderOrFollower) =
                                ResponseError::try_from_code(response_partition.error_code)
                            {
                                if response_partition.current_leader.leader_id != -1 {
                                    // The broker has informed us who the new leader is, we can just directly update the leader
                                    if let Some(mut stored_topic) =
                                        self.topic_by_name.get_mut(topic_name)
                                    {
                                        if let Some(stored_partition) = stored_topic
                                            .partitions
                                            .get_mut(response_partition.index as usize)
                                        {
                                            if response_partition.current_leader.leader_epoch
                                                > stored_partition.leader_epoch
                                            {
                                                stored_partition.leader_id =
                                                    response_partition.current_leader.leader_id;
                                                stored_partition.leader_epoch =
                                                    response_partition.current_leader.leader_epoch;
                                            }
                                            tracing::info!(
                                                "Produce response included error NOT_LEADER_OR_FOLLOWER and so updated leader in topic {:?} partition {}",
                                                topic_name,
                                                response_partition.index
                                            );
                                        }
                                    }
                                } else {
                                    // The broker doesnt know who the new leader is, clear the entire topic.
                                    self.topic_by_name.remove(topic_name);
                                    tracing::info!(
                                        "Produce response included error NOT_LEADER_OR_FOLLOWER and so cleared topic {:?}",
                                        topic_name,
                                    );
                                    break;
                                }
                            }
                        }
                        for response_partition in &mut response_topic.partition_responses {
                            // Clear this optional field to avoid making clients try to bypass shotover
                            response_partition.current_leader =
                                ProduceResponseLeaderIdAndEpoch::default();
                        }
                    }
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Fetch(fetch),
                    ..
                })) => {
                    // Clear this optional field to avoid making clients try to bypass shotover
                    // partition.current_leader and partition.preferred_read_replica are cleared due to the same reason
                    fetch.node_endpoints.clear();
                    for fetch_response in &mut fetch.responses {
                        for partition in &mut fetch_response.partitions {
                            partition.current_leader = FetchResponseLeaderIdAndEpoch::default();
                            partition.preferred_read_replica = BrokerId(-1);
                            if let Some(ResponseError::NotLeaderOrFollower) =
                                ResponseError::try_from_code(partition.error_code)
                            {
                                // The fetch response includes the leader_id which a client could use to route a fetch request to,
                                // but we cant use it to fix our list of replicas, so our only option is to clear the whole thing.
                                self.topic_by_name.remove(&fetch_response.topic);
                                self.topic_by_id.remove(&fetch_response.topic_id);
                                tracing::info!(
                                    "Fetch response included error NOT_LEADER_OR_FOLLOWER and so cleared metadata for topic {:?} {:?}",
                                    fetch_response.topic,
                                    fetch_response.topic_id
                                );
                                break;
                            }
                        }
                    }
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Heartbeat(heartbeat),
                    ..
                })) => self.handle_coordinator_routing_error(request_id, heartbeat.error_code),
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::SyncGroup(sync_group),
                    ..
                })) => self.handle_coordinator_routing_error(request_id, sync_group.error_code),
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::OffsetFetch(offset_fetch),
                    ..
                })) => self.handle_coordinator_routing_error(request_id, offset_fetch.error_code),
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::JoinGroup(join_group),
                    ..
                })) => self.handle_coordinator_routing_error(request_id, join_group.error_code),
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::LeaveGroup(leave_group),
                    ..
                })) => self.handle_coordinator_routing_error(request_id, leave_group.error_code),
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::DeleteGroups(delete_groups),
                    ..
                })) => {
                    for (group_id, result) in &delete_groups.results {
                        if let Some(ResponseError::NotCoordinator) =
                            ResponseError::try_from_code(result.error_code)
                        {
                            // Need to run this to avoid memory leaks, since route_to_coordinator is called for DeleteGroup requests
                            self.routed_to_coordinator_for_group.remove(&request_id);

                            // Need to run this to ensure we remove for all groups
                            self.group_to_coordinator_broker.remove(group_id);
                        }
                    }
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::CreateTopics(create_topics),
                    ..
                })) => {
                    for topic in create_topics.topics.values() {
                        if let Some(ResponseError::NotController) =
                            ResponseError::try_from_code(topic.error_code)
                        {
                            tracing::info!(
                                "Response to CreateTopics included error NOT_CONTROLLER and so reset controller broker, previously was {:?}",
                                self.controller_broker.get()
                            );
                            self.controller_broker.clear();
                            break;
                        }
                    }
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::Metadata(metadata),
                    ..
                })) => {
                    self.process_metadata_response(metadata).await;
                    self.rewrite_metadata_response(metadata)?;
                    response.invalidate_cache();
                }
                Some(Frame::Kafka(KafkaFrame::Response {
                    body: ResponseBody::DescribeCluster(_),
                    ..
                })) => {
                    // If clients were to send this we would need to rewrite the broker information.
                    // However I dont think clients actually send this, so just error to ensure we dont break invariants.
                    return Err(anyhow!(
                        "I think this is a raft specific message and never sent by clients"
                    ));
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// This method must be called for every response to a request that was routed via `route_to_coordinator`
    fn handle_coordinator_routing_error(&mut self, request_id: u128, error_code: i16) {
        if let Some(ResponseError::NotCoordinator) = ResponseError::try_from_code(error_code) {
            if let Some(group_id) = self.routed_to_coordinator_for_group.remove(&request_id) {
                let broker_id = self.group_to_coordinator_broker.remove(&group_id);
                tracing::info!(
                    "Response was error NOT_COORDINATOR and so cleared group id {:?} coordinator mapping to broker {:?}",
                    group_id,
                    broker_id,
                );
            }
        }
    }

    fn process_sasl_authenticate(
        &mut self,
        authenticate: &mut SaslAuthenticateResponse,
        close_client_connection: &mut bool,
    ) -> Result<()> {
        // The broker always closes the connection after an auth failure response,
        // so we should do the same.
        if authenticate.error_code != 0 {
            tracing::debug!("Closing connection to client due to auth failure");
            *close_client_connection = true;
        }

        if let Some(sasl_mechanism) = &self.sasl_mechanism {
            if SASL_SCRAM_MECHANISMS.contains(&sasl_mechanism.as_str()) {
                if let Some(scram_over_mtls) = &mut self.authorize_scram_over_mtls {
                    match scram_over_mtls.original_scram_state {
                        OriginalScramState::WaitingOnServerFirst => {
                            scram_over_mtls.original_scram_state = if authenticate.error_code == 0 {
                                OriginalScramState::WaitingOnServerFinal
                            } else {
                                OriginalScramState::AuthFailed
                            };
                        }
                        OriginalScramState::WaitingOnServerFinal => {
                            scram_over_mtls.original_scram_state = if authenticate.error_code == 0 {
                                OriginalScramState::AuthSuccess
                            } else {
                                OriginalScramState::AuthFailed
                            };
                        }
                        OriginalScramState::AuthSuccess | OriginalScramState::AuthFailed => {
                            return Err(anyhow!(
                                "SCRAM protocol does not allow a third sasl response"
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn route_to_control_connection(&mut self, request: Message) {
        assert!(
            !self.auth_complete,
            "route_to_control_connection cannot be called after auth is complete. Otherwise it would collide with control_send_receive"
        );
        self.pending_requests.push_back(PendingRequest {
            state: PendingRequestState::Routed {
                destination: Destination::ControlConnection,
                request,
            },
            combine_responses: 1,
        });
        tracing::debug!("Routing request to control connection");
    }

    fn route_to_controller(&mut self, request: Message) {
        let broker_id = self.controller_broker.get().unwrap();

        let destination = if let Some(node) = self
            .nodes
            .iter_mut()
            .find(|x| x.broker_id == *broker_id && x.is_up())
        {
            node.broker_id
        } else {
            tracing::warn!("no known broker with id {broker_id:?} that is 'up', routing message to a random broker so that a NOT_CONTROLLER or similar error is returned to the client");
            random_broker_id(&self.nodes, &mut self.rng)
        };

        self.pending_requests.push_back(PendingRequest {
            state: PendingRequestState::routed(destination, request),
            combine_responses: 1,
        });
        tracing::debug!(
            "Routing request relating to controller to broker {}",
            destination.0
        );
    }

    fn route_to_coordinator(&mut self, request: Message, group_id: GroupId) {
        let destination = self.group_to_coordinator_broker.get(&group_id);
        self.routed_to_coordinator_for_group
            .insert(request.id(), group_id.clone());
        let destination = match destination {
            Some(destination) => *destination,
            None => {
                tracing::warn!("no known coordinator for {group_id:?}, routing message to a random broker so that a NOT_COORDINATOR or similar error is returned to the client");
                random_broker_id(&self.nodes, &mut self.rng)
            }
        };

        self.pending_requests.push_back(PendingRequest {
            state: PendingRequestState::routed(destination, request),
            combine_responses: 1,
        });
        tracing::debug!(
            "Routing request relating to group id {:?} to broker {}",
            group_id.0,
            destination.0
        );
    }

    async fn process_metadata_response(&mut self, metadata: &MetadataResponse) {
        for (id, broker) in &metadata.brokers {
            let node = KafkaNode::new(
                *id,
                KafkaAddress::new(broker.host.clone(), broker.port),
                broker.rack.clone(),
            );
            self.add_node_if_new(node).await;
        }

        tracing::debug!(
            "Storing controller metadata, controller is now broker {}",
            metadata.controller_id.0
        );
        self.controller_broker.set(metadata.controller_id);

        for (topic_name, topic) in &metadata.topics {
            if ResponseError::try_from_code(topic.error_code).is_none() {
                // We use the response's partitions list as a base
                // since if it has deleted an entry then we also want to delete that entry.
                let mut new_partitions: Vec<_> = topic
                    .partitions
                    .iter()
                    .map(|partition| Partition {
                        index: partition.partition_index,
                        leader_id: partition.leader_id,
                        leader_epoch: partition.leader_epoch,
                        shotover_rack_replica_nodes: partition
                            .replica_nodes
                            .iter()
                            .cloned()
                            .filter(|replica_node_id| self.broker_within_rack(*replica_node_id))
                            .collect(),
                        external_rack_replica_nodes: partition
                            .replica_nodes
                            .iter()
                            .cloned()
                            .filter(|replica_node_id| !self.broker_within_rack(*replica_node_id))
                            .collect(),
                    })
                    .collect();
                new_partitions.sort_by_key(|x| x.index);

                // If topic_by_name contains any partitions with a more recent leader_epoch use that instead.
                // The out of date epoch is probably caused by requesting metadata from a broker that is slightly out of date.
                // We use topic_by_name instead of topic_by_id since its always used regardless of protocol version.
                if let Some(topic) = self.topic_by_name.get(topic_name) {
                    for old_partition in &topic.partitions {
                        if let Some(new_partition) = new_partitions
                            .iter_mut()
                            .find(|p| p.index == old_partition.index)
                        {
                            if old_partition.leader_epoch > new_partition.leader_epoch {
                                new_partition.leader_id = old_partition.leader_id;
                                new_partition
                                    .shotover_rack_replica_nodes
                                    .clone_from(&old_partition.shotover_rack_replica_nodes);
                                new_partition
                                    .external_rack_replica_nodes
                                    .clone_from(&old_partition.external_rack_replica_nodes);
                            }
                        }
                    }
                };

                let has_topic_name = !topic_name.is_empty();
                let has_topic_id = !topic.topic_id.is_nil();

                // Since new_partitions can be quite long we avoid logging it twice to keep the debug logs somewhat readable.
                if has_topic_name && has_topic_id {
                    tracing::debug!(
                        "Storing topic_by_name and topic_by_id metadata: topic {:?} {} -> {:?}",
                        topic_name.0,
                        topic.topic_id,
                        new_partitions
                    );
                } else if has_topic_name {
                    tracing::debug!(
                        "Storing topic_by_name metadata: topic {:?} -> {new_partitions:?}",
                        topic_name.0
                    );
                } else if has_topic_id {
                    tracing::debug!(
                        "Storing topic_by_id metadata: topic {} -> {new_partitions:?}",
                        topic.topic_id
                    );
                }

                if has_topic_name {
                    self.topic_by_name.insert(
                        topic_name.clone(),
                        Topic {
                            partitions: new_partitions.clone(),
                        },
                    );
                }
                if has_topic_id {
                    self.topic_by_id.insert(
                        topic.topic_id,
                        Topic {
                            partitions: new_partitions,
                        },
                    );
                }
            }
        }
    }

    fn process_find_coordinator_response(
        &mut self,
        version: i16,
        request: FindCoordinator,
        find_coordinator: &FindCoordinatorResponse,
    ) {
        if request.key_type == 0 {
            if version <= 3 {
                if find_coordinator.error_code == 0 {
                    self.group_to_coordinator_broker
                        .insert(GroupId(request.key.clone()), find_coordinator.node_id);
                }
            } else {
                for coordinator in &find_coordinator.coordinators {
                    if coordinator.error_code == 0 {
                        self.group_to_coordinator_broker
                            .insert(GroupId(coordinator.key.clone()), coordinator.node_id);
                    }
                }
            }
        }
    }

    fn rewrite_find_coordinator_response(
        &self,
        version: i16,
        find_coordinator: &mut FindCoordinatorResponse,
    ) {
        if version <= 3 {
            // For version <= 3 we only have one coordinator to replace,
            // so we just pick the first shotover node in the rack of the coordinator.

            // skip rewriting on error
            if find_coordinator.error_code == 0 {
                let coordinator_rack = &self
                    .nodes
                    .iter()
                    .find(|x| x.broker_id == find_coordinator.node_id)
                    .unwrap()
                    .rack
                    .as_ref();
                let shotover_node = self
                    .shotover_nodes
                    .iter()
                    .find(|shotover_node| {
                        coordinator_rack
                            .map(|rack| rack == &shotover_node.rack)
                            .unwrap_or(true)
                    })
                    .unwrap();

                find_coordinator.host = shotover_node.address.host.clone();
                find_coordinator.port = shotover_node.address.port;
                find_coordinator.node_id = shotover_node.broker_id;
            }
        } else {
            // For version > 3 we have to replace multiple coordinators.
            // It may be tempting to include all shotover nodes in the rack of the coordinator, assuming the client will load balance between them.
            // However it doesnt work like that.
            // AFAIK there can only be one coordinator per unique `FindCoordinatorResponse::key`.
            // In fact the java driver doesnt even support multiple coordinators of different types yet:
            // https://github.com/apache/kafka/blob/4825c89d14e5f1b2da7e1f48dac97888602028d7/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java#L921
            //
            // So, just like with version <= 3, we just pick the first shotover node in the rack of the coordinator for each coordinator.
            for coordinator in &mut find_coordinator.coordinators {
                // skip rewriting on error
                if coordinator.error_code == 0 {
                    let coordinator_rack = &self
                        .nodes
                        .iter()
                        .find(|x| x.broker_id == coordinator.node_id)
                        .unwrap()
                        .rack
                        .as_ref();
                    let shotover_node = self
                        .shotover_nodes
                        .iter()
                        .find(|shotover_node| {
                            coordinator_rack
                                .map(|rack| rack == &shotover_node.rack)
                                .unwrap_or(true)
                        })
                        .unwrap();
                    coordinator.host = shotover_node.address.host.clone();
                    coordinator.port = shotover_node.address.port;
                    coordinator.node_id = shotover_node.broker_id;
                }
            }
        }
    }

    /// Rewrite metadata response to appear as if the shotover cluster is the real cluster and the real kafka brokers do not exist
    fn rewrite_metadata_response(&self, metadata: &mut MetadataResponse) -> Result<()> {
        // Overwrite list of brokers with the list of shotover nodes
        metadata.brokers = self
            .shotover_nodes
            .iter()
            .map(|shotover_node| {
                (
                    shotover_node.broker_id,
                    MetadataResponseBroker::default()
                        .with_host(shotover_node.address.host.clone())
                        .with_port(shotover_node.address.port)
                        .with_rack(Some(shotover_node.rack.clone())),
                )
            })
            .collect();

        // Overwrite the list of partitions to point at all shotover nodes within the same rack
        for (_, topic) in &mut metadata.topics {
            for partition in &mut topic.partitions {
                // Deterministically choose a single shotover node in the rack as leader based on topic + partition id
                if let Some(leader_rack) = self
                    .nodes
                    .iter()
                    .find(|x| x.broker_id == *partition.leader_id)
                    .map(|x| x.rack.clone())
                {
                    let shotover_nodes_in_rack: Vec<_> = self
                        .shotover_nodes
                        .iter()
                        .filter(|shotover_node| {
                            leader_rack
                                .as_ref()
                                .map(|rack| rack == &shotover_node.rack)
                                .unwrap_or(true)
                        })
                        .collect();
                    let hash = hash_partition(topic.topic_id, partition.partition_index);
                    let shotover_node =
                        &shotover_nodes_in_rack[hash % shotover_nodes_in_rack.len()];
                    partition.leader_id = shotover_node.broker_id;
                } else {
                    // If the cluster detects the broker is down it will not be included in the metadata list of brokers.
                    // In that case we wont find a broker and we should just mark the leader as not existing by setting it to -1
                    // The brokers also set the leader to -1 if they detect it is down, so this matches their behaviour.
                    partition.leader_id = BrokerId(-1);
                }

                // Every replica node has its entire corresponding shotover rack included.
                // Since we can set as many replica nodes as we like, we take this all out approach.
                // This ensures that:
                // * metadata is deterministic and therefore the same on all shotover nodes
                // * clients evenly distribute their queries across shotover nodes
                let mut shotover_replica_nodes = vec![];
                for replica_node in &partition.replica_nodes {
                    if let Some(rack) = self
                        .nodes
                        .iter()
                        .find(|x| x.broker_id == *replica_node)
                        .map(|x| x.rack.clone())
                    {
                        for shotover_node in &self.shotover_nodes {
                            // If broker has no rack - use all shotover nodes
                            // If broker has rack - use all shotover nodes with the same rack
                            if rack
                                .as_ref()
                                .map(|rack| rack == &shotover_node.rack)
                                .unwrap_or(true)
                                && !shotover_replica_nodes.contains(&shotover_node.broker_id)
                            {
                                shotover_replica_nodes.push(shotover_node.broker_id);
                            }
                        }
                    } else {
                        // If the cluster detects the broker is down it will not be included in the metadata list of brokers.
                        // In that case we wont find a broker and we should just skip this broker.
                    }
                }
                partition.replica_nodes = shotover_replica_nodes;
                // Every isr (in-sync-replica) node has its entire corresponding shotover rack included.
                // Since we can set as many isr nodes as we like, we take this all out approach.
                // This ensures that:
                // * metadata is deterministic and therefore the same on all shotover nodes
                // * clients evenly distribute their queries across shotover nodes
                let mut shotover_isr_nodes = vec![];
                for replica_node in &partition.isr_nodes {
                    if let Some(rack) = self
                        .nodes
                        .iter()
                        .find(|x| x.broker_id == *replica_node)
                        .map(|x| x.rack.clone())
                    {
                        for shotover_node in &self.shotover_nodes {
                            // If broker has no rack - use all shotover nodes
                            // If broker has rack - use all shotover nodes with the same rack
                            if rack
                                .as_ref()
                                .map(|rack| rack == &shotover_node.rack)
                                .unwrap_or(true)
                                && !shotover_isr_nodes.contains(&shotover_node.broker_id)
                            {
                                shotover_isr_nodes.push(shotover_node.broker_id);
                            }
                        }
                    } else {
                        // If the cluster detects the broker is down it will not be included in the metadata list of brokers.
                        // In that case we wont find a broker and we should just skip this broker.
                    }
                }
                partition.isr_nodes = shotover_isr_nodes;

                // TODO: handle this properly, for now its better to just clear it than do nothing
                partition.offline_replicas.clear();
            }
        }

        if let Some(controller_node) = self
            .nodes
            .iter()
            .find(|node| node.broker_id == metadata.controller_id)
        {
            // If broker has no rack - use the first shotover node
            // If broker has rack - use the first shotover node with the same rack
            // This is deterministic because the list of shotover nodes is sorted.
            if let Some(shotover_node) = self.shotover_nodes.iter().find(|shotover_node| {
                controller_node
                    .rack
                    .as_ref()
                    .map(|rack| rack == &shotover_node.rack)
                    .unwrap_or(true)
            }) {
                metadata.controller_id = shotover_node.broker_id;
            } else {
                tracing::warn!(
                    "No shotover node configured to handle kafka rack {:?}",
                    controller_node.rack
                );
            }
        } else {
            return Err(anyhow!(
                "Invalid metadata, controller points at unknown broker {:?}",
                metadata.controller_id
            ));
        }

        Ok(())
    }

    async fn add_node_if_new(&mut self, new_node: KafkaNode) {
        // perform an initial check with read access to allow concurrent access in the vast majority of cases.
        let missing_from_shared = self
            .nodes_shared
            .read()
            .await
            .iter()
            .all(|node| node.broker_id != new_node.broker_id);
        if missing_from_shared {
            // Need to reperform check now that we have exclusive access to nodes_shared
            let mut nodes_shared = self.nodes_shared.write().await;
            let missing_from_shared = nodes_shared
                .iter()
                .all(|node| node.broker_id != new_node.broker_id);
            if missing_from_shared {
                nodes_shared.push(new_node);
            }
        }

        // We need to run this every time, not just when missing_from_shared.
        // This is because nodes_shared could already contain new_node while its missing from `self.nodes`.
        // This could happen when another KafkaSinkCluster instance updates nodes_shared just before we read from it.
        self.update_local_nodes().await;
    }

    fn broker_within_rack(&self, broker_id: BrokerId) -> bool {
        self.nodes.iter().any(|node| {
            node.broker_id == broker_id
                && node
                    .rack
                    .as_ref()
                    .map(|rack| rack == &self.rack)
                    .unwrap_or(false)
        })
    }
}

fn hash_partition(topic_id: Uuid, partition_index: i32) -> usize {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    hasher.write(topic_id.as_bytes());
    hasher.write(&partition_index.to_be_bytes());
    hasher.finish() as usize
}

#[derive(Debug)]
struct Topic {
    partitions: Vec<Partition>,
}

#[derive(Debug, Clone)]
struct Partition {
    index: i32,
    leader_epoch: i32,
    leader_id: BrokerId,
    shotover_rack_replica_nodes: Vec<BrokerId>,
    external_rack_replica_nodes: Vec<BrokerId>,
}

struct FindCoordinator {
    key: StrBytes,
    key_type: i8,
}

fn get_username_from_scram_request(auth_request: &[u8]) -> Option<String> {
    for s in std::str::from_utf8(auth_request).ok()?.split(',') {
        let mut iter = s.splitn(2, '=');
        if let (Some(key), Some(value)) = (iter.next(), iter.next()) {
            if key == "n" {
                return Some(value.to_owned());
            }
        }
    }
    None
}

// Chooses a random broker id from the list of nodes, prioritizes "Up" nodes but fallsback to "down" nodes if needed.
fn random_broker_id(nodes: &[KafkaNode], rng: &mut SmallRng) -> BrokerId {
    match nodes.iter().filter(|node| node.is_up()).choose(rng) {
        Some(broker) => broker.broker_id,
        None => nodes.choose(rng).unwrap().broker_id,
    }
}

struct FormatTopicName<'a>(&'a TopicName, &'a Uuid);

impl<'a> std::fmt::Display for FormatTopicName<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            write!(f, "topic with id {}", self.1)
        } else {
            write!(f, "topic with name {:?}", self.0.as_str())
        }
    }
}
