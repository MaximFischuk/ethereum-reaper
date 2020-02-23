//! App
//!
//! This module contains the main logic of the application

use crate::configuration::settings::{Ethereum, EthBlock, EthLog, Address};
use crate::ethereum;
use crate::ethereum::client::{LogListener, BlockListener};
use crate::messaging::{SendMessage};
use web3::{Transport, BatchTransport};
use serde::export::fmt::Display;
use futures::{future};
use futures::stream::{StreamExt, Empty};
use web3::types::{Log, Block, H256};
use futures::executor::block_on;

pub struct App <'a, T: Transport + BatchTransport + Sync> {
    log_listener: Option<LogListener<'a, T>>,
    block_listener: Option<BlockListener<'a, T>>,

    log_filter: &'a Vec<EthLog>,
    block_filter: &'a Vec<EthBlock>
}

impl <'a, T: Transport + BatchTransport + Sync> App <'a, T> {

    pub fn new (config: &'a Ethereum, transport: &'a T) -> Self {
        let log_listener = if !config.logs.is_empty() {
            Some(ethereum::client::LogListener::new(
                transport,
                &config.logs,
                config.start_block,
                config.batch_size
            ))
        } else {
            None
        };
        let block_listener = if !config.blocks.is_empty() {
            Some(ethereum::client::BlockListener::new(
                transport,
                config.start_block,
                config.batch_size
            ))
        } else {
            None
        };

        App {
            log_listener,
            block_listener,
            log_filter: &config.logs,
            block_filter: &config.blocks
        }
    }

    pub fn run <MQ, R, E> (&self, broker: MQ)
        where
            MQ: SendMessage<Block<H256>, R, E> + SendMessage<Log, R, E>,
            E: Display
    {
        let block_stream = if let Some(block_listener) = &self.block_listener {
            info!("Enabled Ethereum blocks listener");
            let block_stream = block_listener.stream().then(|blocks| {
                for message in blocks {
                    info!("Received block {:?}", message);
                    for filter in self.block_filter {
                        if filter.filter.as_ref().map(|f| f.is_exact_filter(&message)).unwrap_or(true) {
                            info!("Sending message to {}", filter.destination);
                            match broker.send(&message, filter.destination.as_str()) {
                                Ok(_) => trace!("Successful sent message"),
                                Err(error) => error!("Failed to send message {}", error)
                            };
                        }
                    }

                }
                future::ready(())
            });
            Some(block_stream)
        } else {
            None
        };
        let log_stream = if let Some(log_listener) = &self.log_listener {
            info!("Enabled Ethereum logs listener");
            let log_stream = log_listener.stream().then(|logs| {
                for message in logs {
                    info!("Received message {:?}", message);
                    for filter in self.log_filter {
                        if is_log_equal_topic(&message, &filter) &&
                            filter.filter.as_ref().map(|f| f.is_exact_filter(&message)).unwrap_or(true) {
                            info!("Sending message to {}", filter.destination);
                            match broker.send(&message, filter.destination.as_str()) {
                                Ok(_) => trace!("Successful sent message"),
                                Err(error) => error!("Failed to send message {}", error)
                            };
                        }
                    }
                }
                future::ready(())
            });
            Some(log_stream)
        } else {
            None
        };
        block_on(async {
            if block_stream.is_some() && log_stream.is_some() {
                let mut blocks = block_stream.unwrap();
                let mut logs = log_stream.unwrap();
                loop {
                    blocks.next().await;
                    logs.next().await;
                }
            } else if block_stream.is_some() {
                let mut blocks = block_stream.unwrap();
                loop {
                    blocks.next().await;
                }
            } else if log_stream.is_some() {
                let mut logs = log_stream.unwrap();
                loop {
                    logs.next().await;
                }
            } else {
                warn!("No one listener are enabled");
            }
            info!("Exiting loop");
        });
    }

}

pub fn is_log_equal_topic(message: &Log, filter: &EthLog) -> bool {
    message.topics[0].0.eq(&filter.topic.0) &&
        filter.contracts.contains(&Address(message.address.0))
}

pub fn empty_stream<T>() -> Empty<T> {
    futures::stream::empty()
}
