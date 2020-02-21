pub mod kafka;
pub mod properties;

pub trait SendMessage <T, V, E> {

    fn send(&self, message: &T, topic: &str) -> Result<V, E>;

}
