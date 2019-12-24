pub mod cargo_env {
    pub const CARGO_PKG_NAME: &'static str = env!("CARGO_PKG_NAME");
    pub const CARGO_PKG_AUTHORS: &'static str = env!("CARGO_PKG_AUTHORS");
    pub const CARGO_PKG_VERSION: &'static str = env!("CARGO_PKG_VERSION");
}

pub mod common {
    pub const MAX_PARALLEL_REQUESTS: usize = 64;
}
