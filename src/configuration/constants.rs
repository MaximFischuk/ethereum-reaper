pub mod cargo_env {
    pub const CARGO_PKG_NAME: &'static str = env!("CARGO_PKG_NAME");
    pub const CARGO_PKG_AUTHORS: &'static str = env!("CARGO_PKG_AUTHORS");
    pub const CARGO_PKG_VERSION: &'static str = env!("CARGO_PKG_VERSION");
}

pub mod config_params {
    pub const CONFIG: &'static str = "config";
    pub const CONFIG_SHORT: &'static str = "c";
    pub const CONFIG_ENV: &'static str = "REAPER_CONFIG_FILE";
    pub const CONFIG_DESC: &'static str = "Set a custom configuration file. Supported: YAML, JSON, TOML, HJSON";

    pub const LOG: &'static str = "logging";
    pub const LOG_SHORT: &'static str = "L";
    pub const LOG_ENV: &'static str = "LOG_LEVEL";
    pub const LOG_DESC: &'static str = "Sets a logging level";

    pub const LOG_FILE: &'static str = "log_output_file";
    pub const LOG_FILE_SHORT: &'static str = "O";
    pub const LOG_FILE_ENV: &'static str = "LOG_OUTPUT_FILE";
    pub const LOG_FILE_DESC: &'static str = "FIle to which application will write logs";

    pub const NODE_URL: &'static str = "node_url";
    pub const NODE_URL_SHORT: &'static str = "u";
    pub const NODE_URL_ENV: &'static str = "NODE_URL";
    pub const NODE_URL_DESC: &'static str = "Url to opened ethereum web3 protocol endpoint";
}

pub mod common {
    pub const MAX_PARALLEL_REQUESTS: usize = 64;
}
