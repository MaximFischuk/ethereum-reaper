use rdkafka::producer::{BaseProducer, BaseRecord};
use web3::types::Log;
use hex::encode;
use rdkafka::message::OwnedHeaders;
use rdkafka::ClientConfig;
use crate::messaging::SendLog;
use crate::messaging::properties::kafka;
use crate::configuration::settings::MessageBroker;
use rdkafka::error::KafkaError;

impl SendLog <(), KafkaError> for BaseProducer {

    fn send_log(&self, log: &Log) -> Result<(), KafkaError> {
        let serialized = serde_json::to_string(log).expect("Cannot serialize to string");
        let topic = encode(log.topics[0].0);
        let message = BaseRecord::to("test.logs")
            .key(topic.as_str())
            .headers(OwnedHeaders::new().add("CONTRACT", encode(log.address.0).as_str()))
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
