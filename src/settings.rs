use std::env;
use config::{ConfigError, Config, File, Environment};
use log::LevelFilter;
use serde::Deserialize;
use web3::types::{BlockNumber, U64};

#[derive(Deserialize)]
#[serde(remote = "LevelFilter")]
pub enum LevelFilterDef {
    Off, Error, Warn, Info, Debug, Trace,
}

#[derive(Deserialize)]
#[serde(remote = "BlockNumber")]
pub enum BlockNumberDef {
    Latest, Earliest, Pending, Number(U64),
}

#[derive(Debug, Deserialize)]
pub struct Log {
    #[serde(with = "LevelFilterDef")]
    pub level: LevelFilter
}

#[derive(Debug, Deserialize)]
pub struct EthLog {
    pub name: String,
    pub topic: String,
    pub contract: String
}

#[derive(Debug, Deserialize)]
pub struct Ethereum {
    pub url: String,
    #[serde(with = "BlockNumberDef")]
    pub start_block: BlockNumber,
    pub topics: Vec<EthLog>
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub log: Log,
    pub ethereum: Ethereum
}

impl Settings {
    pub fn new(filename: String) -> Result<Self, ConfigError> {
        let mut config = Config::new();
        config.merge(File::with_name(&filename)).expect("Error while loading configuration from file");
        config.merge(Environment::with_prefix("EP")).expect("Error while loading configurations from environment variables");

        config.try_into()
    }

    pub fn default() -> Self {
        Settings {
            log: Log { level: LevelFilter::Info },
            ethereum: Ethereum { url: "http://localhost:8545".to_owned(), start_block: BlockNumber::Latest, topics: vec![] }
        }
    }
}
