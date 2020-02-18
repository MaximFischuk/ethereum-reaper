use web3::types::{Log, Block, TransactionReceipt, H256};

pub mod kafka;
pub mod properties;

pub trait SendLog <V, E> {

    fn send(&self, log: &Log, topic: &str) -> Result<V, E>;

}

pub trait SendBlock <V, E> {

    fn send(&self, block: &Block<H256>, topic: &str) -> Result<V, E>;

}

pub trait SendReceipt <V, E> {

    fn send(&self, receipt: &TransactionReceipt, topic: &str) -> Result<V, E>;

}
