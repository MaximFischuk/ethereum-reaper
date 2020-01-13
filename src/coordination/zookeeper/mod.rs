use zookeeper::{Watcher, WatchedEvent};

pub mod distributed_number;
pub mod interprocess_lock;

struct LoggingWatcher;

impl Watcher for LoggingWatcher {
    fn handle(&self, e: WatchedEvent) {
        info!("{:?}", e)
    }
}
