use crate::codec::redis::RedisCodec;
use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::frame::{Frame, RedisFrame};
use crate::message::Message;
use crate::tls::TlsConfig;
use crate::transforms::redis::cluster::*;
use crate::transforms::redis::RedisError;
use crate::transforms::redis::TransformError;
use crate::transforms::util::cluster_connection_pool::ConnectionPool;
use crate::transforms::util::{Request, Response};
use crate::transforms::ResponseFuture;
use crate::transforms::CONTEXT_CHAIN_NAME;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use bytes_utils::string::Str;
use derivative::Derivative;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryFutureExt};
use metrics::{counter, register_counter};
use rand::prelude::SmallRng;
use rand::SeedableRng;
use redis_protocol::types::Redirection;
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio::time::Duration;
use tracing::{debug, error, info, trace, warn};

type ChannelMap = HashMap<String, Vec<UnboundedSender<Request>>>;

#[derive(Deserialize, Debug, Clone)]
pub struct RedisSinkClusterHandlingConfig {
    pub address: String,
    pub tls: Option<TlsConfig>,
    connection_count: Option<usize>,
}

impl RedisSinkClusterHandlingConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let mut cluster = RedisSinkClusterHandling::new(
            self.address.clone(),
            self.connection_count.unwrap_or(1),
            self.tls.clone(),
            chain_name,
        )?;

        match cluster.build_connections(None).await {
            Ok(()) => {
                info!("connected to upstream");
            }
            Err(TransformError::Upstream(RedisError::NotAuthenticated)) => {
                info!("upstream requires auth");
            }
            Err(e) => {
                return Err(anyhow!(e).context("failed to connect to upstream"));
            }
        }

        Ok(Transforms::RedisSinkClusterHandling(cluster))
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct RedisSinkClusterHandling {
    pub slots: SlotMap,
    pub channels: ChannelMap,
    load_scores: HashMap<(String, usize), usize>,
    rng: SmallRng,
    connection_count: usize,
    connection_pool: ConnectionPool<RedisCodec, RedisAuthenticator, UsernamePasswordToken>,
    connection_error: Option<&'static str>,
    rebuild_connections: bool,
    first_contact_points: Vec<String>,
    token: Option<UsernamePasswordToken>,
}

impl RedisSinkClusterHandling {
    pub fn new(
        address: String,
        connection_count: usize,
        tls: Option<TlsConfig>,
        chain_name: String,
    ) -> Result<Self> {
        let authenticator = RedisAuthenticator {};

        let connection_pool = ConnectionPool::new_with_auth(RedisCodec::new(), authenticator, tls)?;

        let sink_cluster = RedisSinkClusterHandling {
            slots: SlotMap::new(),
            channels: ChannelMap::new(),
            load_scores: HashMap::new(),
            rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
            first_contact_points: vec![address],
            connection_count,
            connection_pool,
            connection_error: None,
            rebuild_connections: false,
            token: None,
        };

        register_counter!("failed_requests", "chain" => chain_name, "transform" => sink_cluster.get_name());

        Ok(sink_cluster)
    }

    fn get_name(&self) -> &'static str {
        "RedisSinkCluster"
    }

    #[inline]
    async fn dispatch_message(&mut self, mut message: Message) -> Result<ResponseFuture> {
        let command = match message.frame() {
            Some(Frame::Redis(RedisFrame::Array(ref command))) => command,
            None => bail!("Failed to parse redis frame"),
            message => bail!("syntax error: bad command: {message:?}"),
        };

        let routing_info = RoutingInfo::for_command_frame(command)?;
        let channels = match self.get_channels(routing_info) {
            ChannelResult::Channels(channels) => channels,
            ChannelResult::Other(Command::AUTH) => {
                return self.on_auth(command).await;
            }
            ChannelResult::ShortCircuit(frame) => {
                let (one_tx, one_rx) = immediate_responder();
                send_frame_response(one_tx, frame)
                    .map_err(|_| anyhow!("Failed to send short circuited redis frame"))?;
                return Ok(Box::pin(one_rx));
            }
        };

        Ok(match channels.len() {
            // Return an error as we cant send anything if there are no channels.
            0 => {
                let (one_tx, one_rx) = immediate_responder();
                match self.connection_error {
                    Some(message) => {
                        self.send_error_response(one_tx, message).ok();
                    }
                    None => self.short_circuit(one_tx),
                };
                Box::pin(one_rx)
            }
            // Send to the single channel and return its response.
            1 => {
                let channel = channels.get(0).unwrap();
                let one_rx = self.choose_and_send(channel, message).await?;
                Box::pin(one_rx.map_err(|_| anyhow!("no response from single channel")))
            }
            // Send to all senders.
            // If any of the responses were a failure then return that failure.
            // Otherwise return the first successful result
            _ => {
                let responses = FuturesUnordered::new();

                for channel in channels {
                    responses.push(self.choose_and_send(&channel, message.clone()).await?);
                }
                Box::pin(async move {
                    let response = responses
                        .fold(None, |acc, response| async move {
                            if let Some(RedisFrame::Error(_)) = acc {
                                acc
                            } else {
                                match response {
                                    Ok(Response {
                                        response: Ok(mut messages),
                                        ..
                                    }) => Some(messages.pop().map_or(
                                        RedisFrame::Null,
                                        |mut message| match message.frame().unwrap() {
                                            Frame::Redis(frame) => {
                                                let new_frame = frame.take();
                                                match acc {
                                                    Some(prev_frame) => routing_info
                                                        .response_join()
                                                        .join(prev_frame, new_frame),
                                                    None => new_frame,
                                                }
                                            }
                                            _ => unreachable!("direct response from a redis sink"),
                                        },
                                    )),
                                    Ok(Response {
                                        response: Err(e), ..
                                    }) => Some(RedisFrame::Error(e.to_string().into())),
                                    Err(e) => Some(RedisFrame::Error(e.to_string().into())),
                                }
                            }
                        })
                        .await;

                    Ok(Response {
                        original: message,
                        response: ChainResponse::Ok(vec![Message::from_frame(Frame::Redis(
                            response.unwrap(),
                        ))]),
                    })
                })
            }
        })
    }

    fn latest_contact_points(&self) -> Vec<String> {
        if !self.slots.nodes.is_empty() {
            // Use latest node addresses as contact points.
            self.slots.nodes.iter().cloned().collect::<Vec<_>>()
        } else {
            // Fallback to initial contact points.
            self.first_contact_points.clone()
        }
    }

    async fn fetch_slot_map(
        &mut self,
        token: &Option<UsernamePasswordToken>,
    ) -> Result<SlotMap, TransformError> {
        debug!("fetching slot map");

        let addresses = self.latest_contact_points();

        let mut errors = Vec::new();
        let mut results = FuturesUnordered::new();

        for address in &addresses {
            results.push(
                self.connection_pool
                    .new_unpooled_connection(address, token)
                    .map_err(move |err| {
                        trace!("error fetching slot map from {}: {}", address, err);
                        TransformError::from(err)
                    })
                    .and_then(get_topology_from_node)
                    .map_ok(move |slots| {
                        trace!("fetched slot map from {}: {:?}", address, slots);
                        slots
                    }),
            );
        }

        while let Some(result) = results.next().await {
            match result {
                Ok(slots) => return Ok(slots),
                Err(err) => errors.push(err),
            }
        }

        debug!("failed to fetch slot map from all hosts");
        Err(TransformError::choose_upstream_or_first(errors).unwrap())
    }

    async fn build_connections(
        &mut self,
        token: Option<UsernamePasswordToken>,
    ) -> Result<(), TransformError> {
        debug!("building connections");

        match self.build_connections_inner(&token).await {
            Ok((slots, channels)) => {
                debug!("connected to cluster: {:?}", channels.keys());
                self.token = token;
                self.slots = slots;
                self.channels = channels;

                self.connection_error = None;
                self.rebuild_connections = false;
                Ok(())
            }
            Err(err @ TransformError::Upstream(RedisError::NotAuthenticated)) => {
                // Assume retry is pointless if authentication is required.
                self.connection_error = Some("NOAUTH Authentication required (cached)");
                self.rebuild_connections = false;
                Err(err)
            }
            Err(err) => {
                warn!("failed to build connections: {}", err);
                Err(err)
            }
        }
    }

    async fn build_connections_inner(
        &mut self,
        token: &Option<UsernamePasswordToken>,
    ) -> Result<(SlotMap, ChannelMap), TransformError> {
        // NOTE: Fetch slot map uses unpooled connections to check token validity before reusing pooled connections.
        let slots = self.fetch_slot_map(token).await?;

        let mut channels = ChannelMap::new();
        let mut errors = Vec::new();
        for node in slots.masters.values().chain(slots.replicas.values()) {
            match self
                .connection_pool
                .get_connections(node, token, self.connection_count)
                .await
            {
                Ok(connections) => {
                    channels.insert(node.to_string(), connections);
                }
                Err(e) => {
                    // Intentional debug! Some errors should be silently passed through.
                    debug!("failed to connect to {}: {}", node, e);
                    errors.push(e.into());
                }
            }
        }

        if channels.is_empty() && !errors.is_empty() {
            Err(TransformError::choose_upstream_or_first(errors).unwrap())
        } else {
            Ok((slots, channels))
        }
    }

    #[inline]
    async fn choose_and_send(
        &mut self,
        host: &str,
        message: Message,
    ) -> Result<oneshot::Receiver<Response>> {
        let (one_tx, one_rx) = oneshot::channel::<Response>();

        let channel = match self.channels.get_mut(host) {
            Some(channels) if channels.len() == 1 => channels.get_mut(0),
            Some(channels) if channels.len() > 1 => {
                let candidates = rand::seq::index::sample(&mut self.rng, channels.len(), 2);
                let aidx = candidates.index(0);
                let bidx = candidates.index(1);

                // TODO: Actually update or remove these "load balancing" scores.
                let aload = *self
                    .load_scores
                    .entry((host.to_string(), aidx))
                    .or_insert(0);
                let bload = *self
                    .load_scores
                    .entry((host.to_string(), bidx))
                    .or_insert(0);

                channels.get_mut(if aload <= bload { aidx } else { bidx })
            }
            _ => None,
        }
        .and_then(|channel| {
            // Treat closed connection as non-existent.
            if channel.is_closed() {
                None
            } else {
                Some(channel)
            }
        });

        let channel = if let Some(channel) = channel {
            channel
        } else {
            debug!("connection {} doesn't exist trying to connect", host);

            match timeout(
                Duration::from_millis(40),
                self.connection_pool
                    .get_connections(host, &self.token, self.connection_count),
            )
            .await
            {
                Ok(Ok(connections)) => {
                    debug!("Found {} live connections for {}", connections.len(), host);
                    self.channels.insert(host.to_string(), connections);
                    self.channels.get_mut(host).unwrap().get_mut(0).unwrap()
                }
                Ok(Err(e)) => {
                    debug!("failed to connect to {}: {}", host, e);
                    self.rebuild_connections = true;
                    self.short_circuit(one_tx);
                    return Ok(one_rx);
                }
                Err(_) => {
                    debug!("timed out connecting to {}", host);
                    self.rebuild_connections = true;
                    self.short_circuit(one_tx);
                    return Ok(one_rx);
                }
            }
        };

        if let Err(e) = channel.send(Request {
            message,
            return_chan: Some(one_tx),
        }) {
            if let Some(error_return) = e.0.return_chan {
                self.rebuild_connections = true;
                self.short_circuit(error_return);
            }
            self.channels.remove(host);
        }

        Ok(one_rx)
    }

    #[inline(always)]
    fn get_channels(&mut self, routing_info: RoutingInfo) -> ChannelResult {
        match routing_info {
            RoutingInfo::Slot(slot) => ChannelResult::Channels(
                if let Some((_, lookup)) = self.slots.masters.range(&slot..).next() {
                    vec![lookup.clone()]
                } else {
                    vec![]
                },
            ),
            RoutingInfo::AllNodes(_) => {
                ChannelResult::Channels(self.slots.nodes.iter().cloned().collect())
            }
            RoutingInfo::AllMasters(_) => {
                ChannelResult::Channels(self.slots.masters.values().cloned().collect())
            }
            RoutingInfo::Random => ChannelResult::Channels(
                self.slots
                    .masters
                    .values()
                    .next()
                    .map(|key| vec![key.clone()])
                    .unwrap_or_default(),
            ),
            RoutingInfo::Other(name) => ChannelResult::Other(name),
            RoutingInfo::Unsupported => ChannelResult::Channels(vec![]),
            RoutingInfo::ShortCircuitNil => ChannelResult::ShortCircuit(RedisFrame::Null),
            RoutingInfo::ShortCircuitOk => {
                ChannelResult::ShortCircuit(RedisFrame::SimpleString(Bytes::from("OK")))
            }
        }
    }

    async fn on_auth(&mut self, command: &[RedisFrame]) -> Result<ResponseFuture> {
        let mut args = command.iter().skip(1).rev().map(|f| match f {
            RedisFrame::BulkString(s) => Ok(s),
            _ => bail!("syntax error: expected bulk string"),
        });

        let password = args
            .next()
            .ok_or_else(|| anyhow!("syntax error: expected password"))??
            .clone();

        let username = args.next().transpose()?.cloned();

        if args.next().is_some() {
            bail!("syntax error: too many args")
        }

        let token = UsernamePasswordToken { username, password };

        let (one_tx, one_rx) = immediate_responder();

        match self.build_connections(Some(token)).await {
            Ok(()) => {
                send_simple_response(one_tx, "OK")?;
            }
            Err(TransformError::Upstream(RedisError::BadCredentials)) => {
                self.send_error_response(one_tx, "WRONGPASS invalid username-password")?;
            }
            Err(TransformError::Upstream(RedisError::NotAuthorized)) => {
                self.send_error_response(one_tx, "NOPERM upstream user lacks required permission")?;
            }
            Err(TransformError::Upstream(e)) => {
                self.send_error_response(one_tx, e.to_string().as_str())?;
            }
            Err(e) => {
                return Err(anyhow!(e).context("authentication failed"));
            }
        }

        Ok(Box::pin(one_rx))
    }

    #[inline(always)]
    fn send_error_response(&self, one_tx: oneshot::Sender<Response>, message: &str) -> Result<()> {
        if let Err(e) = CONTEXT_CHAIN_NAME.try_with(|chain_name| {
        counter!("failed_requests", 1, "chain" => chain_name.to_string(), "transform" => self.get_name());
    }) {
        error!("failed to count failed request - missing chain name: {}", e);
    }
        send_frame_response(one_tx, RedisFrame::Error(message.to_string().into()))
            .map_err(|_| anyhow!("failed to send error: {}", message))
    }

    #[inline(always)]
    fn short_circuit(&self, one_tx: oneshot::Sender<Response>) {
        warn!("Could not route request - short circuiting");
        if let Err(e) = self.send_error_response(
            one_tx,
            "ERR Shotover RedisSinkCluster does not not support this command used in this way",
        ) {
            trace!("short circuiting - couldn't send error - {:?}", e);
        }
    }
}

#[async_trait]
impl Transform for RedisSinkClusterHandling {
    fn is_terminating(&self) -> bool {
        true
    }

    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.rebuild_connections {
            self.build_connections(self.token.clone()).await.ok();
        }

        let mut responses = FuturesOrdered::new();

        for message in message_wrapper.messages {
            responses.push(match self.dispatch_message(message).await {
                Ok(response) => response,
                Err(e) => {
                    let (one_tx, one_rx) = immediate_responder();
                    self.send_error_response(one_tx, &format!("ERR {e}"))?;
                    Box::pin(one_rx)
                }
            });
        }

        trace!("Processing response");
        let mut response_buffer = vec![];

        while let Some(s) = responses.next().await {
            trace!("Got resp {:?}", s);
            let Response { original, response } = s.or_else(|_| -> Result<Response> {
                Ok(Response {
                    original: Message::from_frame(Frame::None),
                    response: Ok(vec![Message::from_frame(Frame::Redis(RedisFrame::Error(
                        Str::from_inner(Bytes::from_static(b"ERR Could not route request"))
                            .unwrap(),
                    )))]),
                })
            })?;

            let mut response = response?;
            assert_eq!(response.len(), 1);
            let mut response_m = response.remove(0);
            match response_m.frame() {
                Some(Frame::Redis(frame)) => {
                    match frame.to_redirection() {
                        Some(Redirection::Moved { slot, server }) => {
                            debug!("Got MOVE {} {}", slot, server);

                            // The destination of a MOVE should always be a master.
                            self.slots.masters.insert(slot, server.clone());

                            self.rebuild_connections = true;

                            let one_rx = self.choose_and_send(&server, original).await?;

                            responses.prepend(Box::pin(
                                one_rx.map_err(|e| anyhow!("Error while retrying MOVE - {}", e)),
                            ));
                        }
                        Some(Redirection::Ask { slot, server }) => {
                            debug!("Got ASK {} {}", slot, server);

                            let one_rx = self.choose_and_send(&server, original).await?;

                            responses.prepend(Box::pin(
                                one_rx.map_err(|e| anyhow!("Error while retrying ASK - {}", e)),
                            ));
                        }
                        None => response_buffer.push(response_m),
                    }
                }
                _ => response_buffer.push(response_m),
            }
        }
        Ok(response_buffer)
    }
}
