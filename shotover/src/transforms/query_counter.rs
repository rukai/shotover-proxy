use crate::frame::Frame;
use crate::message::Messages;
use crate::transforms::TransformConfig;
use crate::transforms::{Transform, TransformBuilder, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use metrics::{counter, register_counter};
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct QueryCounter {
    counter_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct QueryCounterConfig {
    pub name: String,
}

impl QueryCounter {
    pub fn new(counter_name: String) -> Self {
        register_counter!("shotover_query_count", "name" => counter_name.clone());

        QueryCounter { counter_name }
    }
}

impl TransformBuilder for QueryCounter {
    fn build(&self) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
    }
}

#[async_trait]
impl Transform for QueryCounter {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        for m in &mut requests_wrapper.requests {
            match m.frame() {
                Some(Frame::Cassandra(frame)) => {
                    for statement in frame.operation.queries() {
                        counter!("shotover_query_count", 1, "name" => self.counter_name.clone(), "query" => statement.short_name(), "type" => "cassandra");
                    }
                }
                Some(Frame::Redis(frame)) => {
                    if let Some(query_type) = crate::frame::redis::redis_query_name(frame) {
                        counter!("shotover_query_count", 1, "name" => self.counter_name.clone(), "query" => query_type, "type" => "redis");
                    } else {
                        counter!("shotover_query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "redis");
                    }
                }
                Some(Frame::Kafka(_)) => {
                    counter!("shotover_query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "kafka");
                }
                Some(Frame::Dummy) => {
                    // Dummy does not count as a message
                }
                Some(Frame::OpenSearch(_)) => {
                    todo!();
                }
                None => {
                    counter!("shotover_query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "none")
                }
            }
        }

        requests_wrapper.call_next_transform().await
    }
}

const NAME: &str = "QueryCounter";
#[typetag::serde(name = "QueryCounter")]
#[async_trait(?Send)]
impl TransformConfig for QueryCounterConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(QueryCounter::new(self.name.clone())))
    }
}
