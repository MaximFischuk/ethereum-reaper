use std::sync::{ Arc, Mutex };
use zookeeper::{ZkResult, ZooKeeper, CreateMode, Acl};
use zookeeper::recipes::cache::{Data};
use std::collections::HashMap;

pub struct AtomicNumber {
    path: Arc<String>,
    zk: Arc<ZooKeeper>,
    data: Arc<Mutex<Data>>,
}


impl AtomicNumber {

    pub fn new(zk: Arc<ZooKeeper>, path: &str) -> ZkResult<AtomicNumber> {
        let data = Arc::new(Mutex::new(HashMap::new()));

        Ok(AtomicNumber {
            path: Arc::new(path.to_owned()),
            zk,
            data,
        })
    }

    pub fn set_data(&self, number: u64) -> ZkResult<()> {
        self.zk.set_data(self.path.as_str(), Vec::from(&number.to_be_bytes()[..]), None)
            .map(|_|())
    }

    pub fn get_data(&self) -> ZkResult<u64> {
        self.zk.get_data(self.path.as_str(), false)
            .map(|(data, _stat)|{
                let mut array_data: [u8; 8] = Default::default();
                array_data.copy_from_slice(&data[0..8]);
                let number = u64::from_be_bytes(array_data);
                number
            })
    }

    pub fn init(&self, number: u64) -> ZkResult<()> {
        match self.zk.exists(self.path.as_str(), false) {
            Ok(Some(_stat)) => {
                return Ok(());
            },
            Ok(None) => {
                let data = Vec::from(&number.to_be_bytes()[..]);
                self.zk.create(self.path.as_str(), data.clone(), Acl::open_unsafe().clone(), CreateMode::Persistent)?;
                return self.zk
                    .set_data(self.path.as_str(), data, None)
                    .map(|_|());
            },
            Err(error) => return Err(error),
        }
    }

    pub fn clear(&self) {
        self.data.lock().unwrap().clear();
    }

}

#[cfg(test)]
mod tests {
    use zookeeper::{ZooKeeper, ZooKeeperExt};
    use std::time::Duration;
    use crate::coordination::zookeeper::LoggingWatcher;
    use crate::coordination::zookeeper::distributed_number::AtomicNumber;
    use std::sync::Arc;

    #[test]
    fn test_init_data() {
        let zk = Arc::new(ZooKeeper::connect("localhost:2181", Duration::from_millis(50), LoggingWatcher).unwrap());
        let atomic_number = AtomicNumber::new(zk.clone(), "/reaper/tests/atomic_number").unwrap();
        atomic_number.init(4242).unwrap();
        let result = atomic_number.get_data().unwrap();
        assert_eq!(result, 4242, "Number in ZK cluster must be equals to local");
        zk.delete_recursive("/reaper/tests/atomic_number").unwrap();
    }

    #[test]
    fn test_save_data() {
        let zk = Arc::new(ZooKeeper::connect("localhost:2181", Duration::from_millis(50), LoggingWatcher).unwrap());
        zk.ensure_path("/reaper/tests/atomic_number").unwrap();
        let atomic_number = AtomicNumber::new(zk.clone(), "/reaper/tests/atomic_number").unwrap();
        atomic_number.set_data(42).unwrap();
        let result = atomic_number.get_data().unwrap();
        assert_eq!(result, 42, "Number in ZK cluster must be equals to local");
        zk.delete_recursive("/reaper/tests/atomic_number").unwrap();
    }

}
