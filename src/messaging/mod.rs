use web3::types::Log;

pub mod kafka;
pub mod properties;

pub trait SendLog <V, E> {

    fn send_log(&self, log: &Log, topic: &str) -> Result<V, E>;

}
