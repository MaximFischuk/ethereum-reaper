use crate::configuration::settings::EthLog;
use web3::types::{ H2048, H256, H160, Block, TransactionReceipt };
use ethbloom::Input;

fn check_log_exists(bloom: &H2048, filter: &Vec<EthLog>) -> bool {
    for f in filter {
        let topic_raw = Input::Raw(&f.topic.0[..]);
        if bloom.contains_input(topic_raw) {
            return true;
        }
    }
    false
}

fn check_address_exists(address: &H160, filter: &Vec<EthLog>) -> bool {
    filter
        .into_iter()
        .flat_map(|f| &f.contracts)
        .map(move |contract| H160(contract.0))
        .any(move |contract| contract.0 == address.0)
}

fn map_to_value(result: Result<serde_json::Value, web3::Error>) -> Option<serde_json::Value> {
    match result {
        Ok(value) => {
            Some(value)
        },
        Err(e) => {
            error!("{}", e);
            None
        }
    }
}

fn block_deserialize(value: serde_json::Value) -> Option<Block<H256>> {
    match serde_json::from_value(value) {
        Ok(b) => Some(b),
        Err(e) => {
            error!("Cannot to be serialized {}", e);
            None
        }
    }
}

fn transaction_receipt_deserialize(value: serde_json::Value) -> Option<TransactionReceipt> {
    match serde_json::from_value(value) {
        Ok(b) => Some(b),
        Err(e) => {
            error!("Cannot to be serialized {}", e);
            None
        }
    }
}