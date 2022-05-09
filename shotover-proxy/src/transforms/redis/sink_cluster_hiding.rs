use crate::codec::redis::RedisCodec;
use crate::error::ChainResponse;
use crate::tls::TlsConfig;
use crate::transforms::redis::cluster::*;
use crate::transforms::redis::RedisError;
use crate::transforms::redis::TransformError;
use crate::transforms::util::cluster_connection_pool::ConnectionPool;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use derivative::Derivative;
use metrics::register_counter;
use rand::prelude::SmallRng;
use rand::SeedableRng;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::info;

#[derive(Deserialize, Debug, Clone)]
pub struct RedisSinkClusterHidingConfig {
    pub first_contact_points: Vec<String>,
    pub tls: Option<TlsConfig>,
    connection_count: Option<usize>,
}

impl RedisSinkClusterHidingConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let mut cluster = RedisSinkClusterHiding::new(
            self.first_contact_points.clone(),
            self.connection_count.unwrap_or(1),
            self.tls.clone(),
            chain_name,
        )?;

        match cluster.hiding.build_connections(None).await {
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

        Ok(Transforms::RedisSinkClusterHiding(cluster))
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct RedisSinkClusterHiding {
    pub hiding: ClusterHiding,
}

impl RedisSinkClusterHiding {
    pub fn new(
        first_contact_points: Vec<String>,
        connection_count: usize,
        tls: Option<TlsConfig>,
        chain_name: String,
    ) -> Result<Self> {
        let authenticator = RedisAuthenticator {};

        let connection_pool = ConnectionPool::new_with_auth(RedisCodec::new(), authenticator, tls)?;

        let sink_cluster = RedisSinkClusterHiding {
            hiding: ClusterHiding {
                slots: SlotMap::new(),
                channels: ChannelMap::new(),
                load_scores: HashMap::new(),
                rng: SmallRng::from_rng(rand::thread_rng()).unwrap(),
                first_contact_points,
                connection_count,
                connection_pool,
                connection_error: None,
                rebuild_connections: false,
                token: None,
            },
        };

        register_counter!("failed_requests", "chain" => chain_name, "transform" => sink_cluster.get_name());

        Ok(sink_cluster)
    }

    fn get_name(&self) -> &'static str {
        "RedisSinkCluster"
    }
}

#[async_trait]
impl Transform for RedisSinkClusterHiding {
    fn is_terminating(&self) -> bool {
        true
    }

    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.hiding.step(message_wrapper).await
    }
}
