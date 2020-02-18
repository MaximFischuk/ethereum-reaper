use web3::futures::{Future};
use web3::types::{BlockNumber, BlockId, U64, H256, Log, H160, FilterBuilder, Block, TransactionReceipt};

use crate::configuration::settings::{EthLog};

use web3::transports::{Batch};
use web3::{Web3, Error, Transport, BatchTransport};
use futures::stream::Stream;
use std::pin::Pin;
use futures::task::{Context, Poll};
use serde_json::Value;

const BLOCK_UNINITIALIZED: u64 = std::u64::MAX;

pub enum FetchTransactions {
    All,
    Only(Vec<H256>),
    None
}

pub struct LogListener <'a, T: Transport + BatchTransport> {
    web3: Web3<&'a T>,
    batch: Web3<Batch<&'a T>>,
    logs: &'a Vec<EthLog>,
    batch_size: u64,
    start_block: BlockNumber
}

pub struct LogStream <'a, T: Transport + BatchTransport> {
    listener: &'a LogListener<'a, T>,
    batch_size: u64,
    current: u64
}

impl <'a, T: Transport + BatchTransport> LogListener <'a, T> {
    pub fn new (transport: &'a T, logs: &'a Vec<EthLog>, start_block: BlockNumber, batch_size: u64) -> Self {
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

    pub fn stream(&self) -> LogStream<T> {
        LogStream {
            listener: &self,
            batch_size: self.batch_size,
            current: BLOCK_UNINITIALIZED
        }
    }
}

impl <'a, T: Transport + BatchTransport> Stream for LogStream <'a, T> {
    type Item = Vec<Log>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.get_mut();
        if this.listener.logs.is_empty() {
            return Poll::Ready(None);
        }
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
                    .filter(|&result| filter_request_result(result))
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

pub struct BlockListener <'a, T: Transport + BatchTransport> {
    web3: Web3<&'a T>,
    batch: Web3<Batch<&'a T>>,
    batch_size: u64,
    start_block: BlockNumber
}

pub struct BlockStream <'a, T: Transport + BatchTransport> {
    listener: &'a BlockListener<'a, T>,
    batch_size: u64,
    current: u64
}

impl <'a, T: Transport + BatchTransport> BlockListener <'a, T> {
    pub fn new (transport: &'a T, start_block: BlockNumber, batch_size: u64) -> Self {
        let batch = web3::transports::Batch::new(transport);
        let web3 = web3::Web3::new(transport);
        let batch = web3::Web3::new(batch);

        BlockListener{
            web3,
            batch,
            batch_size,
            start_block
        }
    }

    pub fn stream(&self) -> BlockStream<T> {
        BlockStream {
            listener: &self,
            batch_size: self.batch_size,
            current: BLOCK_UNINITIALIZED
        }
    }

    pub fn fetch_transactions(&self, blocks: Vec<Block<H256>>, which: FetchTransactions) -> Vec<TransactionReceipt> {
        debug!("Preparing to fetch transactions");
        let transactions_to_fetch: Vec<H256> = match which {
            FetchTransactions::All =>
                blocks.iter()
                    .map(|block| block.to_owned().transactions)
                    .flat_map(|txs| txs)
                    .collect(),
            FetchTransactions::Only(transactions) =>
                blocks.iter()
                    .map(|block| block.to_owned().transactions)
                    .flat_map(|txs| txs)
                    .filter(|tx| transactions.contains(tx))
                    .collect(),
            FetchTransactions::None => return vec![]
        };

        info!("Fetching {} transactions: {:?}", transactions_to_fetch.len(), &transactions_to_fetch);
        for transaction in transactions_to_fetch {
            self.batch.eth().transaction_receipt(transaction);
        }

        let requests = self.batch.transport().submit_batch();

        let result = match requests.wait() {
            Ok(items) => {
                let transaction_receipts: Vec<TransactionReceipt> = items.iter()
                    .filter(|&result| filter_request_result(result))
                    .map(|value| {
                        let transaction_receipt: TransactionReceipt = match serde_json::from_value(value.as_ref().unwrap().clone()) {
                            Ok(b) => b,
                            Err(e) => {
                                error!("Cannot to be serialized {}", e);
                                TransactionReceipt::default()
                            }
                        };
                        transaction_receipt
                    })
                    .filter(|tx| tx.block_number.is_some())
                    .collect();
                transaction_receipts
            },
            Err(e) => {
                error!("Error result value {:?}", e);
                vec![]
            }
        };
        result
    }
}

impl <'a, T: Transport + BatchTransport> Stream for BlockStream <'a, T> {
    type Item = Vec<Block<H256>>;

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
        info!("Loading {} blocks", poll_size);
        for offset in 0..poll_size {
            let block_num = current + offset;
            info!("Preparing block {}({}..{}) for batch", block_num, current, current + poll_size);
            this.listener.batch.eth().block(BlockId::Number(BlockNumber::Number(U64([block_num]))));
        }
        let requests = this.listener.batch.transport().submit_batch();

        this.current = current + poll_size;
        match requests.wait() {
            Ok(items) => {
                let blocks: Vec<Block<H256>> = items.iter()
                    .filter(|&result| filter_request_result(result))
                    .map(|value| {
                        let block: Block<H256> = match serde_json::from_value(value.as_ref().unwrap().clone()) {
                            Ok(b) => b,
                            Err(e) => {
                                error!("Cannot to be serialized {}", e);
                                Block::default()
                            }
                        };
                        block
                    })
                    .filter(|b| b.hash.is_some())
                    .collect();
                return Poll::Ready(Some(blocks));
            },
            Err(e) => error!("Error result value {:?}", e)
        }

        Poll::Pending
    }
}

fn filter_request_result(result: &Result<Value, Error>) -> bool {
    match result {
        Ok(_) => true,
        Err(e) => {
            error!("Error log {:?}", e);
            false
        }
    }
}
