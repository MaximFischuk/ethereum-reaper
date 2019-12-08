use web3::futures::{Future};
use web3::types::{BlockNumber, BlockId, U64, H256, Log, H160, FilterBuilder};
use futures::channel::mpsc;
use std::{thread};
use std::time::{Duration, SystemTime};

use crate::configuration::settings::EthLog;

use web3::transports::{Http, Batch};
use web3::Web3;
use futures::stream::Stream;
use std::pin::Pin;
use futures::task::{Context, Poll};

const BLOCK_UNINITIALIZED: u64 = std::u64::MAX;

pub struct LogListener <'a> {
    web3: Web3<&'a Http>,
    batch: Web3<Batch<&'a Http>>,
    logs: &'a Vec<EthLog>,
    batch_size: u64,
    start_block: BlockNumber,

    current: u64
}

impl <'a> LogListener <'a> {
    pub fn new (transport: &'a Http, logs: &'a Vec<EthLog>, start_block: BlockNumber, batch_size: u64) -> Self {
        let batch = web3::transports::Batch::new(transport);
        let web3 = web3::Web3::new(transport);
        let batch = web3::Web3::new(batch);

        LogListener{
            web3,
            batch,
            logs,
            batch_size,
            start_block,
            current: BLOCK_UNINITIALIZED
        }
    }

    pub fn current_block(&self) -> u64 {
        self.current
    }

    fn init_block_number(self: &mut Self) {
        self.current = match self.web3.eth().block(BlockId::Number(self.start_block)).wait() {
            Ok(block) => block.map(move |b| b.number.unwrap_or(U64([0]))).unwrap().0[0],
            Err(e) => {
                error!("Error while getting start block number {:?}", e);
                return;
            }
        };
    }

    pub async fn run(self: &mut Self, mut tchannel: mpsc::Sender<Log>) {
        loop {
            if self.current == BLOCK_UNINITIALIZED {
                self.init_block_number();
            }
            let head_block = match self.web3.eth().block_number().wait() {
                Ok(number) => number.0[0],
                Err(e) => {
                    error!("Error while getting head block number {:?}", e);
                    return;
                }
            };
            let current = self.current;
            let batch_size = self.batch_size;
            let poll_size: u64 = match head_block {
                head if head > current && head - current > batch_size => batch_size,
                head if head > current => head - current,
                _ => 0
            };
            if poll_size == 0 {
                info!("Has no new blocks, waiting...");
                continue;
            }
            info!("Preparing blocks {}..{}({}) for batch", current, current + poll_size, poll_size);
            for filter in self.logs {
                let current = FilterBuilder::default()
                    .from_block(BlockNumber::Number(U64([current])))
                    .to_block(BlockNumber::Number(U64([current + poll_size])))
                    .address(filter.contracts.iter().map(|a| H160(a.0)).collect())
                    .topics(Some(vec!(H256(filter.topic.0))), None, None, None)
                    .build();
                self.batch.eth().logs(current);
            }
            let requests = self.batch.transport().submit_batch();

            self.current = current + poll_size;

            match requests.wait() {
                Ok(items) => {
                    if items.len() == 0 {
                        continue;
                    }
                    info!("Loaded {} logs", items.len());
                    for res in items {
                        match res {
                            Ok(value) => {
                                let logs: Vec<Log> = match serde_json::from_value(value) {
                                    Ok(b) => b,
                                    Err(e) => {
                                        error!("Cannot to be serialized {}", e);
                                        continue
                                    }
                                };
                                for log in logs {
                                    match tchannel.start_send(log) {
                                        Ok(_) => trace!("Broadcast received event"),
                                        Err(e) => error!("Error broadcasting message {}", e)
                                    };
                                }
                            },
                            Err(e) => error!("Error log {:?}", e)
                        }
                    }
                },
                Err(e) => error!("Error result value {:?}", e)
            }
        }
    }
}

impl <'a> Stream for LogListener <'a> {
    type Item = Vec<Log>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let listener = self.get_mut();
        if listener.current == BLOCK_UNINITIALIZED {
            listener.init_block_number();
        }
        let head_block = match listener.web3.eth().block_number().wait() {
            Ok(number) => number.0[0],
            Err(e) => {
                error!("Error while getting head block number {:?}", e);
                return Poll::Ready(None);
            }
        };
        let current = listener.current;
        let batch_size = listener.batch_size;
        let poll_size: u64 = match head_block {
            head if head > current && head - current > batch_size => batch_size,
            head if head > current => head - current,
            _ => 0
        };
        if poll_size == 0 {
            info!("Has no new blocks, waiting...");
            return Poll::Pending;
        }
        info!("Preparing blocks {}..{}({}) for batch", current, current + poll_size, poll_size);
        for filter in listener.logs {
            let current = FilterBuilder::default()
                .from_block(BlockNumber::Number(U64([current])))
                .to_block(BlockNumber::Number(U64([current + poll_size])))
                .address(filter.contracts.iter().map(| a |H160(a.0)).collect())
                .topics(Some(vec!(H256(filter.topic.0))), None, None, None)
                .build();
            listener.batch.eth().logs(current);
        }
        let requests = listener.batch.transport().submit_batch();

        listener.current = current + poll_size;

        match requests.wait() {
            Ok(items) => {
                if items.len() == 0 {
                    return Poll::Pending;
                }
                info!("Loaded {} logs", items.len());
                for res in items {
                    match res {
                        Ok(value) => {
                            let _logs: Vec<Log> = match serde_json::from_value(value) {
                                Ok(b) => b,
                                Err(e) => {
                                    error!("Cannot to be serialized {}", e);
                                    continue
                                }
                            };
                        },
                        Err(e) => error!("Error log {:?}", e)
                    }
                }
            },
            Err(e) => error!("Error result value {:?}", e)
        }
        
        Poll::Ready(None)
    }
}