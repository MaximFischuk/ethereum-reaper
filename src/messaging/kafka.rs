use rdkafka::producer::{BaseProducer, BaseRecord};
use web3::types::{Log, Block, TransactionReceipt, H256};
use hex::encode;
use rdkafka::message::OwnedHeaders;
use rdkafka::ClientConfig;
use crate::messaging::{SendLog, SendBlock, SendReceipt};
use crate::messaging::properties::kafka;
use crate::configuration::settings::MessageBroker;
use rdkafka::error::KafkaError;

impl SendLog <(), KafkaError> for BaseProducer {

    fn send(&self, log: &Log, topic: &str) -> Result<(), KafkaError> {
        let serialized = serde_json::to_string(log).expect("Cannot serialize to string");
        let eth_topic = encode(log.topics[0].0);
        let message = BaseRecord::to(topic)
            .key(eth_topic.as_str())
            .headers(OwnedHeaders::new().add("CONTRACT", encode(log.address.0).as_str()))
            .payload(serialized.as_bytes());
        self.send(message).map_err(|(err, _)|err)
    }

}

impl SendBlock <(), KafkaError> for BaseProducer {

    fn send(&self, block: &Block<H256>, topic: &str) -> Result<(), KafkaError> {
        let serialized = serde_json::to_string(block).expect("Cannot serialize to string");
        let eth_block_number = block.number.unwrap().0[0].to_string();
        let message = BaseRecord::to(topic)
            .key(eth_block_number.as_str())
            .headers(OwnedHeaders::new().add("BLOCK_HASH", encode(block.hash.unwrap().0).as_str()))
            .headers(OwnedHeaders::new().add("BLOCK_NUMBER", eth_block_number.as_str()))
            .payload(serialized.as_bytes());
        self.send(message).map_err(|(err, _)|err)
    }

}

impl SendReceipt <(), KafkaError> for BaseProducer {

    fn send(&self, receipt: &TransactionReceipt, topic: &str) -> Result<(), KafkaError> {
        let serialized = serde_json::to_string(receipt).expect("Cannot serialize to string");
        let eth_tx_hash = encode(receipt.transaction_hash.0);
        let eth_block_number = receipt.block_number.unwrap().0[0].to_string();
        let message = BaseRecord::to(topic)
            .key(eth_tx_hash.as_str())
            .headers(OwnedHeaders::new().add("TRANSACTION_HASH", encode(receipt.transaction_hash.0).as_str()))
            .headers(OwnedHeaders::new().add("BLOCK_NUMBER", eth_block_number.as_str()))
            .headers(OwnedHeaders::new().add("TRANSACTION_STATUS", &receipt.status.unwrap().0[0].to_string()))
            .payload(serialized.as_bytes());
        self.send(message).map_err(|(err, _)|err)
    }

}

impl From<MessageBroker> for BaseProducer {

    fn from(broker: MessageBroker) -> BaseProducer {
        let mut config = ClientConfig::new();
        config.set(kafka::BOOTSTRAP_SERVERS, broker.brokers.as_str());
        for (key, value) in broker.properties {
            config.set(key.as_str(), value.as_str());
        }
        config.create()
            .expect("Producer creation error")
    }
}
