use rdkafka::producer::{BaseProducer, BaseRecord, FutureProducer, FutureRecord, DeliveryFuture};
use web3::types::{Log, Block, TransactionReceipt, Transaction};
use hex::encode;
use rdkafka::message::OwnedHeaders;
use rdkafka::ClientConfig;
use crate::messaging::{SendMessage};
use crate::messaging::properties::kafka;
use crate::configuration::settings::MessageBroker;
use rdkafka::error::KafkaError;
use std::collections::HashMap;

const BLOCK_MS: i64 = 500;

impl SendMessage <Log, (), KafkaError> for BaseProducer {

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

impl SendMessage <Block<Transaction>, (), KafkaError> for BaseProducer {

    fn send(&self, block: &Block<Transaction>, topic: &str) -> Result<(), KafkaError> {
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

impl SendMessage <TransactionReceipt, (), KafkaError> for BaseProducer {

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
        for (key, value) in broker.properties.unwrap_or(HashMap::default()) {
            config.set(key.as_str(), value.as_str());
        }
        config.create()
            .expect("Producer creation error")
    }
}

impl SendMessage <Log, DeliveryFuture, ()> for FutureProducer {

    fn send(&self, log: &Log, topic: &str) -> Result<DeliveryFuture, ()> {
        let serialized = serde_json::to_string(log).expect("Cannot serialize to string");
        let eth_topic = encode(log.topics[0].0);
        let message = FutureRecord::to(topic)
            .key(eth_topic.as_str())
            .headers(OwnedHeaders::new().add("CONTRACT", encode(log.address.0).as_str()))
            .payload(serialized.as_bytes());
        Ok(self.send(message, BLOCK_MS))
    }

}

impl SendMessage <Block<Transaction>, DeliveryFuture, ()> for FutureProducer {

    fn send(&self, block: &Block<Transaction>, topic: &str) -> Result<DeliveryFuture, ()> {
        let serialized = serde_json::to_string(block).expect("Cannot serialize to string");
        let eth_block_number = block.number.unwrap().0[0].to_string();
        let message = FutureRecord::to(topic)
            .key(eth_block_number.as_str())
            .headers(OwnedHeaders::new().add("BLOCK_HASH", encode(block.hash.unwrap().0).as_str()))
            .headers(OwnedHeaders::new().add("BLOCK_NUMBER", eth_block_number.as_str()))
            .payload(serialized.as_bytes());
        Ok(self.send(message, BLOCK_MS))
    }

}

impl SendMessage <TransactionReceipt, DeliveryFuture, ()> for FutureProducer {

    fn send(&self, receipt: &TransactionReceipt, topic: &str) -> Result<DeliveryFuture, ()> {
        let serialized = serde_json::to_string(receipt).expect("Cannot serialize to string");
        let eth_tx_hash = encode(receipt.transaction_hash.0);
        let eth_block_number = receipt.block_number.unwrap().0[0].to_string();
        let message = FutureRecord::to(topic)
            .key(eth_tx_hash.as_str())
            .headers(OwnedHeaders::new().add("TRANSACTION_HASH", encode(receipt.transaction_hash.0).as_str()))
            .headers(OwnedHeaders::new().add("BLOCK_NUMBER", eth_block_number.as_str()))
            .headers(OwnedHeaders::new().add("TRANSACTION_STATUS", &receipt.status.unwrap().0[0].to_string()))
            .payload(serialized.as_bytes());
        Ok(self.send(message, BLOCK_MS))
    }

}

impl From<MessageBroker> for FutureProducer {

    fn from(broker: MessageBroker) -> FutureProducer {
        let mut config = ClientConfig::new();
        config.set(kafka::BOOTSTRAP_SERVERS, broker.brokers.as_str());
        for (key, value) in broker.properties.unwrap_or(HashMap::default()) {
            config.set(key.as_str(), value.as_str());
        }
        config.create()
            .expect("Producer creation error")
    }
}
