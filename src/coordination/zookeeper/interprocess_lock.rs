use zookeeper::{ZkResult, ZooKeeper, ZooKeeperExt, CreateMode, Acl, ZkError, WatchedEvent, WatchedEventType};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

const LOCK_PATH: &'static str = "lock-";

pub struct InterProcessMutex {
    path: Arc<String>,
    zk: Arc<ZooKeeper>,
    lock: Arc<Mutex<String>>
}

impl InterProcessMutex {

    pub fn new(zk: Arc<ZooKeeper>, path: &str) -> ZkResult<InterProcessMutex> {

        zk.ensure_path(path);

        Ok(InterProcessMutex {
            path: Arc::new(path.to_owned()),
            zk,
            lock: Arc::new(Mutex::new("".to_owned()))
        })
    }

    pub fn acquire(&self, timeout: Duration) -> ZkResult<()> {
        let result = self.zk.create(
            format!("{}/{}", self.path, LOCK_PATH).as_str(),
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::EphemeralSequential
        );
        let lock = match result {
            Ok(path) => path[self.path.len() + 1 ..].to_owned(),
            Err(e) => return Err(e)
        };
        let now = Instant::now();
        loop {
            let children = match self.zk.get_children(&self.path, false) {
                Ok(mut c) => {
                    c.sort();
                    c
                },
                Err(e) => return Err(e)
            };
            if children[0].eq(&lock) {
                let mut data_lock = self.lock.lock().unwrap();
                *data_lock = lock;
                return Ok(());
            } else {
                let done_lock: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
                let lock = done_lock.clone();
                let watcher = move |event: WatchedEvent| {
                    let mut done = lock.lock().unwrap();
                    match event.event_type {
                        WatchedEventType::NodeDeleted => *done = true,
                        _ => {/* ignored */}
                    }
                };
                let stats = self.zk.exists_w(format!("{}/{}", self.path, children[0]).as_str(), watcher);
                match stats {
                    Ok(None) => continue,
                    Err(e) => return Err(e),
                    _ => {/* ignored */}
                };
                loop {
                    let done = match done_lock.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    if *done {
                        break;
                    } else if now.elapsed().gt(&timeout) {
                        return Err(ZkError::OperationTimeout);
                    }
                }
            }
        }
    }

    pub fn release(&self) -> ZkResult<()> {
        let mut data_lock = self.lock.lock().unwrap();
        if data_lock.is_empty() {
            Err(ZkError::DataInconsistency)
        } else {
            match self.zk.delete(format!("{}/{}", self.path, data_lock).as_str(), None) {
                Ok(_) => {
                    *data_lock = String::default();
                    Ok(())
                },
                Err(e) => Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use zookeeper::{ZooKeeper};
    use std::time::Duration;
    use crate::coordination::zookeeper::LoggingWatcher;
    use crate::coordination::zookeeper::interprocess_lock::InterProcessMutex;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_aquire() {
        let zk = Arc::new(ZooKeeper::connect("localhost:2181", Duration::from_millis(50), LoggingWatcher).unwrap());
        let locker = InterProcessMutex::new(zk.clone(), "/reaper/tests/interprocess_lock").unwrap();
        let result = locker.acquire(Duration::from_millis(500));
        assert!(result.is_ok());
        locker.release();
    }

    #[test]
    fn test_aquire_3_locks() {
        let zk = Arc::new(ZooKeeper::connect("localhost:2181", Duration::from_millis(50), LoggingWatcher).unwrap());
        let locker1 = InterProcessMutex::new(zk.clone(), "/reaper/tests/interprocess_lock").unwrap();
        let locker2 = InterProcessMutex::new(zk.clone(), "/reaper/tests/interprocess_lock").unwrap();
        let locker3 = InterProcessMutex::new(zk.clone(), "/reaper/tests/interprocess_lock").unwrap();

        let t1 = thread::spawn(move || {
            let result = locker1.acquire(Duration::from_millis(100));
            thread::sleep(Duration::from_secs(1));
            let result2 = locker1.release();

            result.is_ok() && result2.is_ok()
        });
        let t2 = thread::spawn(move || {
            let result = locker2.acquire(Duration::from_secs(2));
            thread::sleep(Duration::from_secs(1));
            let result2 = locker2.release();

            result.is_ok() && result2.is_ok()
        });
        let t3 = thread::spawn(move || {
            let result = locker3.acquire(Duration::from_secs(3));
            let result2 = locker3.release();

            result.is_ok() && result2.is_ok()
        });
        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();
        let r3 = t3.join().unwrap();
        assert_eq!((r1, r2, r3), (true, true, true));
    }
}
