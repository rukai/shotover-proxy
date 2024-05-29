use super::node::{ConnectionFactory, KafkaAddress};
use crate::{
    connection::SinkConnection,
    frame::{
        kafka::{KafkaFrame, RequestBody, ResponseBody},
        Frame,
    },
    message::Message,
    tls::{TlsConnector, TlsConnectorConfig},
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose, Engine};
use futures::stream::FuturesUnordered;
use kafka_protocol::{
    messages::{
        describe_delegation_token_request::DescribeDelegationTokenOwner, ApiKey,
        CreateDelegationTokenRequest, CreateDelegationTokenResponse,
        DescribeDelegationTokenRequest, MetadataRequest, RequestHeader,
    },
    protocol::{Builder, StrBytes},
    ResponseError,
};
use rand::SeedableRng;
use rand::{rngs::SmallRng, seq::IteratorRandom};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Notify;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;

pub struct TokenRequest {
    username: String,
    response_tx: oneshot::Sender<DelegationToken>,
}

#[derive(Clone)]
pub struct TokenTask {
    tx: mpsc::Sender<TokenRequest>,
}

impl TokenTask {
    #[allow(clippy::new_without_default)]
    pub fn new(
        mtls_connection_factory: ConnectionFactory,
        mtls_port_contact_points: Vec<KafkaAddress>,
    ) -> TokenTask {
        let (tx, mut rx) = mpsc::channel::<TokenRequest>(1000);
        tokio::spawn(async move {
            loop {
                match task(&mut rx, &mtls_connection_factory, &mtls_port_contact_points).await {
                    Ok(()) => {
                        // shotover is shutting down, terminate the task
                        break;
                    }
                    Err(err) => {
                        tracing::error!("Token task restarting due to failure, error was {err:?}");
                    }
                }
            }
        });
        TokenTask { tx }
    }

    pub async fn get_token_for_user(&self, username: String) -> Result<DelegationToken> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(TokenRequest {
                username,
                response_tx,
            })
            .await
            .context("Failed to request delegation token from token task")?;
        response_rx
            .await
            .context("Token task encountered an error before it could respond to request for token")
    }
}

async fn task(
    rx: &mut mpsc::Receiver<TokenRequest>,
    mtls_connection_factory: &ConnectionFactory,
    mtls_addresses: &[KafkaAddress],
) -> Result<()> {
    let mut rng = SmallRng::from_rng(rand::thread_rng())?;
    let mut username_to_token = HashMap::new();

    let mut nodes = vec![];
    while let Some(request) = rx.recv().await {
        let instant = Instant::now();

        // initialize nodes if uninitialized
        if nodes.is_empty() {
            let mut futures = FuturesUnordered::new();
            for address in mtls_addresses {
                futures.push(async move {
                    let connection = match mtls_connection_factory
                        // Must be unauthed since mTLS is its own auth.
                        .create_connection_unauthed(address)
                        .await
                    {
                        Ok(connection) => Some(connection),
                        Err(err) => {
                            tracing::error!("Token Task: Failed to create connection for {address:?} during nodes list init {err}");
                            None
                        }
                    };
                    Node {
                        connection,
                        address: address.clone(),
                    }
                });
            }
            while let Some(node) = futures.next().await {
                nodes.push(node);
            }
        }

        let token = if let Some(token) = username_to_token.get(&request.username).cloned() {
            token
        } else {
            let username = StrBytes::from_string(request.username.clone());
            // We apply a 120s timeout to token creation:
            // * It needs to be low enough to avoid the task getting permanently stuck if the cluster gets in a bad state and never fully propagates the token.
            // * It needs to be high enough to avoid catching cases of slow token propagation.
            //   + From our testing delegation tokens should be propagated within 0.5s to 1s on unloaded kafka clusters of size 15 to 30 nodes.
            let token = tokio::time::timeout(
                Duration::from_secs(120),
                create_delegation_token_for_user_with_wait(
                    &mut nodes,
                    username.clone(),
                    &mut rng,
                    mtls_connection_factory,
                ),
            )
            .await
            .with_context(|| format!("Delegation token creation for {username:?} timedout"))?
            .with_context(|| format!("Failed to create delegation token for {username:?}"))?;

            username_to_token.insert(request.username, token.clone());
            tracing::info!(
                "Delegation token for {username:?} created in {:?}",
                instant.elapsed()
            );
            token
        };
        request.response_tx.send(token).ok();
    }

    // rx returned None which indicates shotover is shutting down
    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct AuthorizeScramOverMtlsConfig {
    pub mtls_port_contact_points: Vec<String>,
    pub tls: TlsConnectorConfig,
}

impl AuthorizeScramOverMtlsConfig {
    pub fn get_builder(
        &self,
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
    ) -> Result<AuthorizeScramOverMtlsBuilder> {
        let mtls_connection_factory = ConnectionFactory::new(
            Some(TlsConnector::new(self.tls.clone())?),
            connect_timeout,
            read_timeout,
            Arc::new(Notify::new()),
        );
        let contact_points: Result<Vec<_>> = self
            .mtls_port_contact_points
            .iter()
            .map(|x| KafkaAddress::from_str(x))
            .collect();
        Ok(AuthorizeScramOverMtlsBuilder {
            token_task: TokenTask::new(mtls_connection_factory, contact_points?),
        })
    }
}

pub struct AuthorizeScramOverMtlsBuilder {
    pub token_task: TokenTask,
}

impl AuthorizeScramOverMtlsBuilder {
    pub fn build(&self) -> AuthorizeScramOverMtls {
        AuthorizeScramOverMtls {
            original_scram_state: OriginalScramState::WaitingOnServerFirst,
            token_task: self.token_task.clone(),
            delegation_token: DelegationToken {
                token_id: String::new(),
                hmac: StrBytes::default(),
            },
        }
    }
}

pub struct AuthorizeScramOverMtls {
    /// Tracks the state of the original scram connections responses created from the clients actual requests
    pub original_scram_state: OriginalScramState,
    /// Shared task that fetches and caches delegation tokens
    pub token_task: TokenTask,
    /// The delegation token generated from the username used in the original scram auth
    pub delegation_token: DelegationToken,
}

pub enum OriginalScramState {
    WaitingOnServerFirst,
    WaitingOnServerFinal,
    AuthFailed,
    AuthSuccess,
}

async fn create_delegation_token_for_user_with_wait(
    nodes: &mut Vec<Node>,
    username: StrBytes,
    rng: &mut SmallRng,
    mtls_connection_factory: &ConnectionFactory,
) -> Result<DelegationToken> {
    let create_response = create_delegation_token_for_user(nodes, &username, rng).await?;
    // we specifically run find_new_brokers:
    // * after token creation since we are waiting for token propagation anyway.
    // * before waiting on brokers because we need to wait on the entire cluster,
    //   so we want our node list to be as up to date as possible.
    find_new_brokers(nodes, rng).await?;
    wait_until_delegation_token_ready_on_all_brokers(
        nodes,
        &create_response,
        username,
        mtls_connection_factory,
    )
    .await?;

    Ok(DelegationToken {
        token_id: create_response.token_id.as_str().to_owned(),
        hmac: StrBytes::from_string(general_purpose::STANDARD.encode(&create_response.hmac)),
    })
}

/// populate existing nodes
/// If no nodes have a connection open an error will be returned.
async fn find_new_brokers(nodes: &mut Vec<Node>, rng: &mut SmallRng) -> Result<()> {
    let Some(node) = nodes
        .iter_mut()
        .filter(|node| node.connection.is_some())
        .choose(rng)
    else {
        return Err(anyhow!("No nodes have an open connection"));
    };
    let connection = node
        .connection
        .as_mut()
        .expect("Gauranteed due to above filter");

    let request = Message::from_frame(Frame::Kafka(KafkaFrame::Request {
        header: RequestHeader::builder()
            .request_api_key(ApiKey::MetadataKey as i16)
            .request_api_version(4)
            .correlation_id(0)
            .build()
            .unwrap(),
        body: RequestBody::Metadata(MetadataRequest::builder().build().unwrap()),
    }));
    connection.send(vec![request])?;

    let response = connection.recv().await?.remove(0);
    match response.into_frame() {
        Some(Frame::Kafka(KafkaFrame::Response {
            body: ResponseBody::Metadata(metadata),
            ..
        })) => {
            let new_nodes: Vec<Node> = metadata
                .brokers
                .into_values()
                .filter_map(|broker| {
                    let address = KafkaAddress::new(broker.host, broker.port);
                    if nodes.iter().any(|node| node.address == address) {
                        None
                    } else {
                        Some(Node {
                            address,
                            connection: None,
                        })
                    }
                })
                .collect();
            nodes.extend(new_nodes);
            Ok(())
        }
        other => Err(anyhow!(
            "Unexpected message returned to metadata request {other:?}"
        )),
    }
}

/// Create a delegation token for the provided user.
/// If no nodes have a connection open an error will be returned.
async fn create_delegation_token_for_user(
    nodes: &mut [Node],
    username: &StrBytes,
    rng: &mut SmallRng,
) -> Result<CreateDelegationTokenResponse> {
    let Some(node) = nodes
        .iter_mut()
        .filter(|node| node.connection.is_some())
        .choose(rng)
    else {
        return Err(anyhow!("No nodes have an open connection"));
    };
    let connection = node
        .connection
        .as_mut()
        .expect("Gauranteed due to above filter");

    connection.send(vec![Message::from_frame(Frame::Kafka(
        KafkaFrame::Request {
            header: RequestHeader::builder()
                .request_api_key(ApiKey::CreateDelegationTokenKey as i16)
                .request_api_version(3)
                .build()
                .unwrap(),
            body: RequestBody::CreateDelegationToken(
                CreateDelegationTokenRequest::builder()
                    .owner_principal_type(Some(StrBytes::from_static_str("User")))
                    .owner_principal_name(Some(username.clone()))
                    .build()
                    .unwrap(),
            ),
        },
    ))])?;

    let response = connection.recv().await?.pop().unwrap();
    match response.into_frame() {
        Some(Frame::Kafka(KafkaFrame::Response {
            body: ResponseBody::CreateDelegationToken(response),
            ..
        })) => {
            if let Some(err) = ResponseError::try_from_code(response.error_code) {
                Err(anyhow!(
                    "kafka responded to CreateDelegationToken with error {err}",
                ))
            } else {
                Ok(response)
            }
        }
        response => Err(anyhow!(
            "Unexpected response to CreateDelegationToken {response:?}"
        )),
    }
}

/// Wait until delegation token is ready on all brokers.
/// Will create connections for all nodes that dont have one yet.
/// If a broker is inaccessible it will count as ready to prevent a node going down from stopping delegation token creation.
async fn wait_until_delegation_token_ready_on_all_brokers(
    nodes: &mut [Node],
    create_response: &CreateDelegationTokenResponse,
    username: StrBytes,
    mtls_connection_factory: &ConnectionFactory,
) -> Result<()> {
    let nodes_len = nodes.len();
    for (i, node) in nodes.iter_mut().enumerate() {
        let address = &node.address;
        if node.connection.is_none() {
            node.connection = match mtls_connection_factory
                // Must be unauthed since mTLS is its own auth.
                .create_connection_unauthed(address)
                .await
            {
                Ok(connection) => Some(connection),
                Err(err) => {
                    tracing::error!("Token Task: Failed to create connection for {address:?} during token wait {err}");
                    None
                }
            };
        }
        if let Some(connection) = &mut node.connection {
            while !is_delegation_token_ready(connection, create_response, username.clone())
                .await
                .with_context(|| {
                    format!("Failed to check delegation token was ready on broker {address:?}. Succesful connections {i}/{nodes_len}")
                })?
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tracing::debug!("finished checking token is ready on broker {address:?}");
        }
    }

    Ok(())
}

/// Returns Ok(true) if the token is ready
/// Returns Ok(false) if the token is not ready
/// Returns Err(_) if an error occured with the kafka connection.
async fn is_delegation_token_ready(
    connection: &mut SinkConnection,
    create_response: &CreateDelegationTokenResponse,
    username: StrBytes,
) -> Result<bool> {
    // TODO: Create a single request Message, convert it into raw bytes, and then reuse for all following requests
    //       This will avoid many allocations for each sent request
    //       It is left as a TODO since shotover does not currently support this. But we should support it in the future.
    connection.send(vec![Message::from_frame(Frame::Kafka(
        KafkaFrame::Request {
            header: RequestHeader::builder()
                .request_api_key(ApiKey::DescribeDelegationTokenKey as i16)
                .request_api_version(3)
                .build()
                .unwrap(),
            body: RequestBody::DescribeDelegationToken(
                DescribeDelegationTokenRequest::builder()
                    .owners(Some(vec![DescribeDelegationTokenOwner::builder()
                        .principal_type(StrBytes::from_static_str("User"))
                        .principal_name(username)
                        .build()
                        .unwrap()]))
                    .build()
                    .unwrap(),
            ),
        },
    ))])?;
    let mut response = connection.recv().await?.pop().unwrap();
    if let Some(Frame::Kafka(KafkaFrame::Response {
        body: ResponseBody::DescribeDelegationToken(response),
        ..
    })) = response.frame()
    {
        if let Some(err) = ResponseError::try_from_code(response.error_code) {
            return Err(anyhow!(
                "Kafka's response to DescribeDelegationToken was an error: {err}"
            ));
        }
        if response
            .tokens
            .iter()
            .any(|x| x.hmac == create_response.hmac && x.token_id == create_response.token_id)
        {
            Ok(true)
        } else {
            Ok(false)
        }
    } else {
        Err(anyhow!(
            "Unexpected response to CreateDelegationToken {response:?}"
        ))
    }
}

struct Node {
    address: KafkaAddress,
    connection: Option<SinkConnection>,
}

#[derive(Clone)]
pub struct DelegationToken {
    pub token_id: String,
    pub hmac: StrBytes,
}
