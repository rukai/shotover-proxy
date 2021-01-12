use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, Messages, QueryResponse, Value};
use std::iter::*;

use crate::concurrency::FuturesOrdered;
use crate::protocols::RawFrame;
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, TransformsFromConfig, Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Buf;
use futures::StreamExt;
use itertools::Itertools;
use redis_protocol::types::Frame;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::mpsc::Sender;

const SLOT_SIZE: usize = 16384;

#[derive(Debug, Clone)]
pub struct SequentialMap {
    name: &'static str,
    chain: TransformChain,
    pub slots: SlotMap,
    pub channels: ChannelMap,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SequentialMapConfig {
    pub name: String,
    pub chain: Vec<TransformsConfig>,
}

#[async_trait]
impl TransformsFromConfig for SequentialMapConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::SequentialMap(SequentialMap {
            name: "SequentialMap",
            chain: build_chain_from_config(self.name.clone(), &self.chain, &topics).await?,
            slots: Default::default(),
            channels: Default::default(),
        }))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RoutingInfo {
    AllNodes,
    AllMasters,
    Random,
    Slot(u16),
}

fn get_hashtag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|v| *v == b'{');
    let open = match open {
        Some(open) => open,
        None => return None,
    };

    let close = key[open..].iter().position(|v| *v == b'}');
    let close = match close {
        Some(close) => close,
        None => return None,
    };

    let rv = &key[open + 1..open + close];
    if rv.is_empty() {
        None
    } else {
        Some(rv)
    }
}

impl RoutingInfo {
    pub fn for_command_frame(args: &Vec<Frame>) -> Option<RoutingInfo> {
        if let Some(Frame::BulkString(command_arg)) = args.get(0) {
            return match command_arg.bytes() {
                b"FLUSHALL" | b"FLUSHDB" | b"SCRIPT" | b"ACL" => Some(RoutingInfo::AllMasters),
                b"ECHO" | b"CONFIG" | b"CLIENT" | b"SLOWLOG" | b"DBSIZE" | b"LASTSAVE"
                | b"PING" | b"INFO" | b"BGREWRITEAOF" | b"BGSAVE" | b"CLIENT LIST" | b"SAVE"
                | b"TIME" | b"KEYS" | b"AUTH" => Some(RoutingInfo::AllNodes),
                b"SCAN" | b"CLIENT SETNAME" | b"SHUTDOWN" | b"SLAVEOF" | b"REPLICAOF"
                | b"SCRIPT KILL" | b"MOVE" | b"BITOP" => None,
                b"EVALSHA" | b"EVAL" => {
                    let foo = if let Some(Frame::Integer(key_count)) = args.get(2) {
                        if *key_count == 0 {
                            Some(RoutingInfo::Random)
                        } else {
                            args.get(3).and_then(RoutingInfo::for_key)
                        }
                    } else {
                        None
                    };
                    foo
                }
                b"XGROUP" | b"XINFO" => args.get(2).and_then(RoutingInfo::for_key),
                b"XREAD" | b"XREADGROUP" => {
                    let streams_position = args.iter().position(|a| match a {
                        Frame::BulkString(a) => a.bytes() == b"STREAMS",
                        _ => false,
                    })?;
                    args.get(streams_position + 1)
                        .and_then(RoutingInfo::for_key)
                }
                _ => match args.get(1) {
                    Some(key) => RoutingInfo::for_key(key),
                    None => Some(RoutingInfo::Random),
                },
            };
        }
        None
    }

    pub fn for_key(key: &Frame) -> Option<RoutingInfo> {
        if let Frame::BulkString(key) = key {
            let key = match get_hashtag(&key) {
                Some(tag) => tag,
                None => &key,
            };
            Some(RoutingInfo::Slot(
                crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE as u16,
            ))
        } else {
            None
        }
    }
}

type SlotMap = BTreeMap<u16, Sender<Request>>;
type ChannelMap = HashMap<String, Sender<Request>>;
type Response = (Message, ChainResponse);

#[derive(Debug)]
pub struct Request {
    pub messages: Message,
    pub return_chan: Option<tokio::sync::oneshot::Sender<Response>>,
}

fn short_circuit(one_tx: tokio::sync::oneshot::Sender<Response>) {
    one_tx.send((
        Message::new_bypass(RawFrame::NONE),
        Ok(Messages::new_single_response(
            QueryResponse::empty_with_error(Some(Value::Strings(
                "ERR Could not route request".to_string(),
            ))),
            true,
            RawFrame::NONE,
        )),
    ));
}

pub fn connect(host: &String, port: u16) -> Sender<Request> {
    unimplemented!()
}

#[async_trait]
impl Transform for SequentialMap {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        let mut responses = FuturesOrdered::new();
        for message in qd.message.into_iter() {
            let sender = match &message.original {
                RawFrame::Redis(Frame::Array(ref commands)) => {
                    match RoutingInfo::for_command_frame(&commands) {
                        Some(RoutingInfo::Slot(slot)) => {
                            if let Some((_, sender)) = self.slots.range_mut(&slot..).next() {
                                vec![sender]
                            } else {
                                vec![]
                            }
                        }
                        Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => {
                            self.channels.iter_mut().map(|(_, chan)| chan).collect_vec()
                        }
                        Some(RoutingInfo::Random) => {
                            let key = self
                                .channels
                                .keys()
                                .next()
                                .unwrap_or(&"nothing".to_string())
                                .clone();
                            self.channels.get_mut(&key).into_iter().collect_vec()
                        }
                        None => {
                            vec![]
                        }
                    }
                }
                _ => {
                    vec![]
                }
            };
            if sender.is_empty() {
                let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();
                short_circuit(one_tx);
                responses.push(one_rx)
            } else {
                for chan in sender.into_iter() {
                    let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();
                    chan.send(Request {
                        messages: message.clone(),
                        return_chan: Some(one_tx),
                    })
                    .await?;
                    responses.push(one_rx)
                }
            }
        }
        let mut response_buffer = vec![];
        loop {
            match responses.next().await {
                Some(s) => {
                    let (original, response) = s?;
                    let response_m = response?.messages.remove(0);
                    match response_m.original {
                        RawFrame::Redis(Frame::Moved { slot, host, port }) => {
                            let chan = match self.channels.get_mut(&*format!("{}:{}", host, port)) {
                                None => {
                                    //here we create a new connection if there isn't one, here we update the slot map
                                    let chan = connect(&host, port);
                                    self.slots.insert(slot, chan.clone());
                                    self.channels.insert(format!("{}:{}", &host, &port), chan);
                                    self.channels
                                        .get_mut(&*format!("{}:{}", &host, &port))
                                        .unwrap()
                                }
                                Some(chan) => chan,
                            };

                            let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();
                            chan.send(Request {
                                messages: original.clone(),
                                return_chan: Some(one_tx),
                            })
                            .await?;
                            responses.prepend(one_rx);

                            //update slots
                            //handle request
                        }
                        RawFrame::Redis(Frame::Ask {
                            slot: _,
                            host,
                            port,
                        }) => {
                            // see redis-protocol.rs for redirection struct so we dont need to parse the string again
                            let chan = match self.channels.get_mut(&*format!("{}:{}", host, port)) {
                                None => {
                                    //here we create a new connection if there isn't one, however we don't update the slot map
                                    let chan = connect(&host, port);
                                    self.channels
                                        .insert(format!("{}:{}", &host, &port), chan.clone());
                                    self.channels
                                        .get_mut(&*format!("{}:{}", &host, &port))
                                        .unwrap()
                                }
                                Some(chan) => chan,
                            };

                            let (one_tx, one_rx) = tokio::sync::oneshot::channel::<Response>();
                            chan.send(Request {
                                messages: original.clone(),
                                return_chan: Some(one_tx),
                            })
                            .await?;
                            responses.prepend(one_rx);
                        }
                        _ => response_buffer.push(response_m),
                    }
                }
                None => break,
            }
        }
        return Ok(Messages {
            messages: response_buffer,
        });
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
