use std::sync::Mutex;
use web3::futures::{Future};
use web3::types::{BlockNumber, BlockId, U64, H256, Log, H160, FilterBuilder, Block, TransactionReceipt, Transaction, TransactionId, Filter};

use crate::configuration::settings::{EthLog};

use web3::transports::{Batch};
use web3::{Web3, Error, Transport, BatchTransport};
use futures::stream::Stream;
use std::pin::Pin;
use futures::task::{Context, Poll};
use serde_json::Value;
use serde::de::DeserializeOwned;
use crate::coordination::zookeeper::interprocess_lock::InterProcessMutex;
use std::sync::Arc;
use std::time::Duration;
use zookeeper::ZooKeeper;
use crate::coordination::zookeeper::LoggingWatcher;
use crate::coordination::zookeeper::distributed_number::AtomicNumber;
use crate::ethereum::NumberStorage;

const BLOCK_UNINITIALIZED: u64 = std::u64::MAX;

pub enum FetchTransactions {
    All,
    Only(Vec<H256>),
    None
}

pub struct LocalStorage {
    current: Arc<Mutex<u64>>
}

impl NumberStorage for LocalStorage {

    fn read(&self) -> Option<u64> {
        let data = self.current.lock().unwrap();

        if *data == BLOCK_UNINITIALIZED {
            None
        } else {
            Some(*data)
        }
    }

    fn write(&self, num: u64) {
        let mut data = self.current.lock().unwrap();
        *data = num;
    }

}

impl Default for LocalStorage {

    fn default() -> Self {
        LocalStorage{
            current: Arc::new(Mutex::new(BLOCK_UNINITIALIZED))
        }
    }

}

pub struct LogListener <'a, T: Transport + BatchTransport> {
    web3: Web3<&'a T>,
    batch: Web3<Batch<&'a T>>,
    logs: &'a Vec<EthLog>,
    batch_size: u64,
    start_block: BlockNumber
}

pub struct LogStream <'a, T: Transport + BatchTransport, POS: NumberStorage> {
    listener: &'a LogListener<'a, T>,
    batch_size: u64,
    current: POS
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

    pub fn stream(&self) -> LogStream<T, LocalStorage> {
        LogStream {
            listener: &self,
            batch_size: self.batch_size,
            current: LocalStorage::default()
        }
    }
}

impl <'a, T, P> Stream for LogStream <'a, T, P>
    where
        T: Transport + BatchTransport,
        P: NumberStorage + Unpin
{
    type Item = Vec<Log>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.batch_size == 0 {
            return Poll::Ready(None);
        }
        if self.listener.logs.is_empty() {
            return Poll::Ready(None);
        }
        let current_stored = self.current.read().unwrap_or(BLOCK_UNINITIALIZED);
        let current = if current_stored == BLOCK_UNINITIALIZED {
            let start_block = match self.listener.web3.eth().block(BlockId::Number(self.listener.start_block)).wait() {
                Ok(block) => block.map(move |b| b.number.unwrap_or(U64([0]))).unwrap().0[0],
                Err(e) => {
                    error!("Error while getting start block number {:?}", e);
                    return Poll::Ready(None);
                }
            };
            self.current.write(start_block);
            start_block
        } else {
            current_stored
        };

        let head_block = match self.listener.web3.eth().block_number().wait() {
            Ok(number) => number.0[0],
            Err(e) => {
                error!("Error while getting head block number {:?}", e);
                return Poll::Ready(None);
            }
        };

        let batch_size = self.batch_size;
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
        for filter in self.listener.logs {
            let current = eth_filter(&filter, BlockNumber::Number(U64([current])), BlockNumber::Number(U64([current + poll_size])));
            self.listener.batch.eth().logs(current);
        }
        let requests = self.listener.batch.transport().submit_batch();

        self.current.write(current + poll_size);

        match requests.wait() {
            Ok(items) => {
                let logs: Vec<Vec<Log>> = deserialize_batch_result(&items);
                let result: Vec<Log> = logs.iter().flat_map(|logs|logs.clone()).collect();
                return Poll::Ready(Some(result));
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
    current: u64,
    lock: Option<Arc<InterProcessMutex>>
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
            current: BLOCK_UNINITIALIZED,
            lock: None
        }
    }

    pub fn distributed_stream(&self, zk_url: &str) -> BlockStream<T> {
        let zk = Arc::new(ZooKeeper::connect(zk_url, Duration::from_millis(50), LoggingWatcher).unwrap());
        let locker = InterProcessMutex::new(zk.clone(), "/reaper/app/interprocess_lock").unwrap();
        let _number = AtomicNumber::new(zk.clone(), "/reaper/app/number").unwrap();
        BlockStream {
            listener: &self,
            batch_size: self.batch_size,
            current: BLOCK_UNINITIALIZED,
            lock: Some(Arc::new(locker))
        }
    }

    pub fn fetch_transactions(&self, blocks: Vec<Block<H256>>, which: FetchTransactions) -> Vec<Transaction> {
        debug!("Preparing to fetch transactions");
        let transactions_to_fetch: Vec<H256> = Self::filter_transactions_from_blocks(blocks, which);

        info!("Fetching {} transactions: {:?}", transactions_to_fetch.len(), &transactions_to_fetch);
        for transaction in transactions_to_fetch {
            self.batch.eth().transaction(TransactionId::Hash(transaction));
        }

        let requests = self.batch.transport().submit_batch();

        let result = match requests.wait() {
            Ok(items) => {
                let transactions: Vec<Transaction> = deserialize_batch_result(&items);
                transactions
            },
            Err(e) => {
                error!("Error result value {:?}", e);
                vec![]
            }
        };
        result
    }

    pub fn fetch_receipts(&self, blocks: Vec<Block<H256>>, which: FetchTransactions) -> Vec<TransactionReceipt> {
        debug!("Preparing to fetch receipts");
        let transactions_to_fetch: Vec<H256> = Self::filter_transactions_from_blocks(blocks, which);

        info!("Fetching {} receipts: {:?}", transactions_to_fetch.len(), &transactions_to_fetch);
        for transaction in transactions_to_fetch {
            self.batch.eth().transaction_receipt(transaction);
        }

        let requests = self.batch.transport().submit_batch();

        let result = match requests.wait() {
            Ok(items) => {
                let transaction_receipts: Vec<TransactionReceipt> = deserialize_batch_result(&items);
                transaction_receipts
            },
            Err(e) => {
                error!("Error result value {:?}", e);
                vec![]
            }
        };
        result
    }

    fn filter_transactions_from_blocks(blocks: Vec<Block<H256>>, which: FetchTransactions) -> Vec<H256> {
        match which {
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
            FetchTransactions::None => vec![]
        }
    }
}

impl <'a, T: Transport + BatchTransport> Stream for BlockStream <'a, T> {
    type Item = Vec<Block<H256>>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.get_mut();
        if this.batch_size == 0 {
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

        if let Some(lock) = this.lock.as_ref() {
            if lock.acquire(Duration::from_millis(200)).is_err() {
                warn!("Failed acquire leadership");
                return Poll::Pending;
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
            this.listener.batch.eth().block(BlockId::Number(BlockNumber::Number(U64([block_num]))));
        }
        let requests = this.listener.batch.transport().submit_batch();

        this.current = current + poll_size;
        match requests.wait() {
            Ok(items) => {
                let blocks: Vec<Block<H256>> = deserialize_batch_result(&items);
                return Poll::Ready(Some(blocks));
            },
            Err(e) => error!("Error result value {:?}", e)
        }
        if let Some(lock) = this.lock.as_ref() {
            if lock.release().is_err() {
                warn!("Error releasing leadership");
            }
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

fn deserialize_batch_result<R>(result: &Vec<Result<Value, Error>>) -> Vec<R>
    where
        R: DeserializeOwned
{
    let rs: Vec<R> = result.iter()
        .filter(|&result| filter_request_result(result))
        .map(|value| {
            let r: Option<R> = match serde_json::from_value(value.as_ref().unwrap().clone()) {
                Ok(b) => Some(b),
                Err(e) => {
                    error!("Cannot to be serialized {}", e);
                    None
                }
            };
            r
        })
        .filter(|tx| tx.is_some())
        .map(|tx| tx.unwrap())
        .collect();
    rs
}

fn eth_filter(log_filter: &EthLog, from: BlockNumber, to: BlockNumber) -> Filter {
    FilterBuilder::default()
        .from_block(from)
        .to_block(to)
        .address(log_filter.contracts.iter().map(|a| H160(a.0)).collect())
        .topics(Some(vec!(H256(log_filter.topic.0))), None, None, None)
        .build()
}
