use web3::futures::{Future};
use web3::types::{BlockNumber, BlockId, U64, H256, Log, H160, FilterBuilder};

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
    start_block: BlockNumber
}

pub struct LogStream <'a> {
    listener: &'a LogListener<'a>,
    batch_size: u64,
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
            start_block
        }
    }

    pub fn stream(&self) -> LogStream {
        LogStream {
            listener: &self,
            batch_size: self.batch_size,
            current: BLOCK_UNINITIALIZED
        }
    }
}

impl <'a> Stream for LogStream <'a> {
    type Item = Vec<Log>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.get_mut();
        if this.current == BLOCK_UNINITIALIZED {
            this.current = match this.listener.web3.eth().block(BlockId::Number(this.listener.start_block)).wait() {
                Ok(block) => block.map(move |b| b.number.unwrap_or(U64([0]))).unwrap().0[0],
                Err(e) => {
                    error!("Error while getting start block number {:?}", e);
                    return Poll::Ready(None);
                }
            }
        }
        let head_block = match this.listener.web3.eth().block_number().wait() {
            Ok(number) => number.0[0],
            Err(e) => {
                error!("Error while getting head block number {:?}", e);
                return Poll::Ready(None);
            }
        };
        let current = this.current;
        let batch_size = this.batch_size;
        let poll_size: u64 = match head_block {
            head if head > current && head - current > batch_size => batch_size,
            head if head > current => head - current,
            _ => 0
        };
        if poll_size == 0 {
            debug!("Has no new blocks, waiting...");
            return Poll::Pending;
        }
        info!("Preparing blocks {}..{}({}) for batch", current, current + poll_size, poll_size);
        for filter in this.listener.logs {
            let current = FilterBuilder::default()
                .from_block(BlockNumber::Number(U64([current])))
                .to_block(BlockNumber::Number(U64([current + poll_size])))
                .address(filter.contracts.iter().map(|a| H160(a.0)).collect())
                .topics(Some(vec!(H256(filter.topic.0))), None, None, None)
                .build();
            this.listener.batch.eth().logs(current);
        }
        let requests = this.listener.batch.transport().submit_batch();

        this.current = current + poll_size;

        match requests.wait() {
            Ok(items) => {
                let logs: Vec<Log> = items.iter()
                    .filter(|&result| {
                        match result {
                            Ok(_) => true,
                            Err(e) => {
                                error!("Error log {:?}", e);
                                false
                            }
                        }
                    })
                    .map(|value| {
                        let logs: Vec<Log> = match serde_json::from_value(value.as_ref().unwrap().clone()) {
                            Ok(b) => b,
                            Err(e) => {
                                error!("Cannot to be serialized {}", e);
                                vec![]
                            }
                        };
                        logs
                    })
                    .flat_map(|logs|logs)
                    .collect();
                return Poll::Ready(Some(logs));
            },
            Err(e) => error!("Error result value {:?}", e)
        }
        Poll::Pending
    }
}
