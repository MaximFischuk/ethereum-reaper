pub mod client;

pub trait NumberStorage {

    fn read(&self) -> Option<u64>;
    fn write(&self, num: u64);

}
