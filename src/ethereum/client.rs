use web3::futures::{Future};
use web3::types::{BlockNumber, BlockId, U64, H256, Log, H160, FilterBuilder};
use futures::channel::mpsc;
use std::{thread};
use std::time::{Duration, SystemTime};

use crate::configuration::settings::EthLog;

const MAX_PARALLEL_REQUESTS: usize = 64;

pub fn run_main_loop(node_url: &str, logs: &Vec<EthLog>, start_block: BlockNumber, batch_size: u64, mut tchannel: mpsc::Sender<Log>) {
    info!("Creating connection to {}", node_url);
    let (_eloop, transport) = web3::transports::Http::with_max_parallel(node_url, MAX_PARALLEL_REQUESTS).unwrap();
    let batch = web3::transports::Batch::new(transport.clone());
    let web3 = web3::Web3::new(transport);
    let web3_batch = web3::Web3::new(batch);

    let block_number = match web3.eth().block(BlockId::Number(start_block)).wait() {
        Ok(block) => block.map(move |b| b.number.unwrap_or(U64([0]))).unwrap().0[0],
        Err(e) => {
            error!("Error while getting start block number {:?}", e);
            return;
        }
    };

    let mut current: u64 = block_number;
    loop {
        let head_block = match web3.eth().block_number().wait() {
            Ok(number) => number.0[0],
            Err(e) => {
                error!("Error while getting head block number {:?}", e);
                return;
            }
        };
        let poll_size: u64 = match head_block {
            head if head > current && head - current > batch_size => batch_size,
            head if head > current => head - current,
            _ => 0
        };
        if poll_size == 0 {
            thread::sleep(Duration::from_secs(3));
            info!("Has no new blocks, waiting...");
            continue;
        }
        info!("Loading {} blocks", poll_size);
//        for offset in 0..poll_size {
//            let block_num = current + offset;
//            info!("Preparing block {}({}..{}) for batch", block_num, current, current + poll_size);
//            web3_batch.eth().block(BlockId::Number(BlockNumber::Number(U64([block_num]))));
//        }
        info!("Preparing blocks ({}..{}) for batch", current, current + poll_size);
        for filter in logs {
            let current = FilterBuilder::default()
                .from_block(BlockNumber::Number(U64([current])))
                .to_block(BlockNumber::Number(U64([current + poll_size])))
                .address(filter.contracts.iter().map(| a |H160(a.0)).collect())
                .topics(Some(vec!(H256(filter.topic.0))), None, None, None)
                .build();
            web3_batch.eth().logs(current);
        }

        current = current + poll_size;

        let now = SystemTime::now();
        let requests = web3_batch.transport().submit_batch();
        match requests.wait() {
            Ok(items) => {
                info!("Loaded logs");
                for res in items {
                    match res {
                        Ok(value) => {
                            let logs: Vec<Log> = match serde_json::from_value(value) {
                                Ok(b) => b,
                                Err(e) => {
                                    error!("Cannot to be serialized {}", e);
                                    continue
                                }
                            };
                            for log in logs {
                                match tchannel.start_send(log) {
                                    Ok(_) => trace!("Broadcast received event"),
                                    Err(e) => error!("Error broadcasting message {}", e)
                                };
                            }
                        },
                        Err(e) => error!("Error log {:?}", e)
                    }
                }
            },
            Err(e) => error!("Error result value {:?}", e)
        }
//        match requests.wait() {
//            Ok(items) => {
//                info!("Loaded blocks");
//                let blocks: Vec<Block<H256>> = items.into_iter()
//                    .map(self::map_to_value)
//                    .filter(|v| v.is_some())
//                    .map(|v| v.unwrap())
//                    .map(self::block_deserialize)
//                    .filter(|v| v.is_some())
//                    .map(|v| v.unwrap())
//                    .collect();
//                info!("Preparing to fetch transactions");
//                for block in blocks {
//                    let mut do_transactions_fetch: bool = false;
//                    for topic in &settings.ethereum.logs {
//                        let topic_raw = Input::Raw(&topic.topic.0[..]);
//                        if block.logs_bloom.contains_input(topic_raw) {
//                            info!("Block {} contains log with topic {}({:?})", block.number.unwrap(), topic.name, topic.topic);
//                            if !topic.contracts.is_empty() {
//                                for contract in &topic.contracts {
//                                    let address_raw = Input::Raw(&contract.0[..]);
//                                    if block.logs_bloom.contains_input(address_raw) {
//                                        do_transactions_fetch = true;
//                                        info!("Found emiter {:?} for topic {} in block {}", contract, topic.name, block.number.unwrap());
//                                    }
//                                }
//                            } else {
//                                do_transactions_fetch = true;
//                            }
//                        }
//                    }
//                    if !do_transactions_fetch {
//                        continue;
//                    }
//                    info!("Buffering {} requests", block.transactions.len());
//                    debug!("Loaded {:?}", block);
//                    for tx in block.transactions {
//                        trace!("Prepering to fetch transaction {}", tx);
//                        web3_batch.eth().transaction_receipt(tx);
//                    }
//
//                    match web3_batch.transport().submit_batch().wait() {
//                        Ok(response) => {
//                            response.into_iter()
//                                .map(self::map_to_value)
//                                .filter(|v| v.is_some())
//                                .map(|v| v.unwrap())
//                                .map(self::transaction_receipt_deserialize)
//                                .filter(|v| v.is_some())
//                                .map(|v| v.unwrap())
//                                .filter(|tx| check_log_exists(&tx.logs_bloom, &settings.ethereum.logs))
//                                .for_each(|transaction| {
//                                    debug!("Loaded transaction {:?}", transaction);
//                                    let logs = transaction.logs;
//                                    logs.into_iter()
//                                        .filter(
//                                            |l| check_address_exists(&l.address, &settings.ethereum.logs)
//                                        )
//                                        .for_each(|log| {
//                                            match tchannel.start_send(log) {
//                                                Ok(_) => trace!("Broadcast received event"),
//                                                Err(e) => error!("Error broadcasting message {}", e)
//                                            };
//                                        });
//                                });
//                        },
//                        Err(e) => error!("Error fetching transactions {}", e)
//                    }
//                }
//            }
//            Err(e) => {
//                error!("Error while batching blocks {}", e);
//            }
//        }
        match now.elapsed() {
            Ok(time) => info!("Loaded {} block in {} secs", poll_size, time.as_secs()),
            Err(e) => error!("{:?}", e)
        };
    }
}