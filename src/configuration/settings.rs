use config::{ConfigError, Config, File, Environment};
use log::LevelFilter;
use serde::{Deserialize};
use web3::types::{BlockNumber, U64};
use std::collections::HashMap;
use regex::Regex;
use jsonpath::Selector;
use serde::export::fmt::{Debug};
use derivative::*;

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct Hash32(#[serde(with = "hex_serde")] pub [u8; 32]);

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct Address(#[serde(with = "hex_serde")] pub [u8; 20]);

#[derive(Deserialize)]
#[serde(remote = "LevelFilter")]
pub enum LevelFilterDef {
    Off, Error, Warn, Info, Debug, Trace,
}

#[derive(Deserialize)]
#[serde(remote = "BlockNumber")]
pub enum BlockNumberDef {
    Latest, Earliest, Pending, #[serde(with = "crate::configuration::deserialize")] Number(U64),
}

#[derive(Deserialize, Derivative)]
#[derivative(Debug)]
pub struct JsonFilter {
    #[serde(with = "serde_regex")]
    pub regex: Regex,
    #[derivative(Debug="ignore")]
    #[serde(with = "crate::configuration::deserialize::selector")]
    pub selector: Selector
}

#[derive(Debug, Deserialize)]
pub struct Log {
    #[serde(with = "LevelFilterDef")]
    pub level: LevelFilter
}

#[derive(Debug, Deserialize)]
pub struct EthLog {
    pub name: String,
    pub topic: Hash32,
    pub contracts: Vec<Address>,
    pub destination: String,
    pub filter: Option<JsonFilter>
}

#[derive(Debug, Deserialize)]
pub struct EthBlock {
    pub name: String,
    pub destination: String,
    pub filter: Option<JsonFilter>
}

#[derive(Debug, Deserialize)]
pub struct EthTransaction {
    pub name: String,
    pub contracts: Vec<Address>,
    pub functions: Vec<String>,
    pub destination: String,
    pub filter: Option<JsonFilter>
}

#[derive(Debug, Deserialize)]
pub struct Ethereum {
    pub url: String,
    #[serde(with = "BlockNumberDef")]
    pub start_block: BlockNumber,
    pub batch_size: u64,
    pub logs: Vec<EthLog>,
    pub transactions: Vec<EthTransaction>,
    pub blocks: Vec<EthBlock>
}

#[derive(Debug, Deserialize)]
pub struct MessageBroker {
    pub brokers: String,
    pub properties: Option<HashMap<String, String>>
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub log: Log,
    pub ethereum: Ethereum,
    pub kafka: MessageBroker
}

impl Settings {
    pub fn new(filename: String) -> Result<Self, ConfigError> {
        let mut config = Config::new();
        config.merge(File::with_name(&filename)).expect("Error while loading configuration from file");
        config.merge(Environment::with_prefix("EP")).expect("Error while loading configurations from environment variables");

        config.try_into()
    }
}

impl Default for Settings {

    fn default() -> Self {
        Settings {
            log: Log { level: LevelFilter::Info },
            ethereum: Ethereum {
                url: "http://localhost:8545".to_owned(),
                start_block: BlockNumber::Latest,
                logs: vec![],
                transactions: vec![],
                blocks: vec![],
                batch_size: 10
            },
            kafka: MessageBroker { brokers: "localhost:9092".to_owned(), properties: None }
        }
    }

}
