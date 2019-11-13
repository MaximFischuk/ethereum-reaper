extern crate web3;
extern crate env_logger;
extern crate clap;
#[macro_use] extern crate log;

mod settings;

use clap::{ App, Arg, ArgMatches };
use web3::futures::Future;
use web3::types::{ BlockNumber, BlockId };
use std::convert::TryInto;

use settings::Settings;

const MAX_PARALLEL_REQUESTS: usize = 64;

fn main() {
    let matches = App::new("Ethereum-Poller")
        .author("Maxim Fischuk")
        .version("0.0.1")
        .arg(Arg::with_name("config")
             .short("c")
             .long("config")
             .value_name("FILE")
             .takes_value(true)
             .help("Set a custom configuration file. Supported: YAML, JSON, TOML, HJSON"))
        .arg(Arg::with_name("logging")
             .short("L")
             .long("logging")
             .value_name("LOG_LEVEL")
             .takes_value(true)
             .default_value("Info")
             .possible_values(&["Error", "Warn", "Info", "Debug", "Trace"])
             .required(false)
             .help("Sets a logging level"))
        .get_matches();
    cli(matches);

    run_poller_loop("https://mainnet.infura.io/v3/a6b0643f19d54ff685750d5ccd223d31");
}

fn cli(matches: ArgMatches) {
    let settings: Settings;
    match matches.value_of("config") {
        Some(v) => {
            settings = Settings::new(v.to_string()).unwrap();
        }
        None => {
            warn!("Config not set, using default");
            settings = Settings::default();
        }
    }
    env_logger::builder().filter_level(settings.log.level).init();
    info!("Loaded configurations {:?}", settings);
    info!("Logging level {} enabled", settings.log.level);
}

fn run_poller_loop(node_url: &str) {
    info!("Connecting to {}", node_url);
    let (_eloop, transport) = web3::transports::Http::with_max_parallel(node_url, MAX_PARALLEL_REQUESTS).unwrap();
    let web3 = web3::Web3::new(transport);
    let _ = web3.eth().accounts();
    info!("Connection established");

    let mut block_number: u64 = 0;
    loop {
        trace!("Loading block information");
        let block = web3.eth().block(BlockId::Number(BlockNumber::Latest)).wait().unwrap();
        if let Some(block_data) = block {
            let number = block_data.number.unwrap();
            if number.0[0] > block_number {
                block_number = number.0[0];
                info!("Loaded block number {:?}", block_number);
            }
        }
    }
}

fn test_runner() {
    let mut event_loop = tokio_core::reactor::Core::new().unwrap();
    let remote = event_loop.remote();

    let http = web3::transports::Http::with_event_loop("https://mainnet.infura.io/v3/a6b0643f19d54ff685750d5ccd223d31", &event_loop.handle(), MAX_PARALLEL_REQUESTS).unwrap();

    // let address = "EA674fdDe714fd979de3EdF0F56AA9716B898ec8".parse().unwrap();
    let web3 = web3::Web3::new(web3::transports::Batch::new(http));
    let _ = web3.eth().accounts();
    // info!("Loading balance for {}", address);
    // match web3.eth().balance(address, Some(BlockNumber::Latest)).wait() {
    //     Ok(accounts) => {
    //         info!("Loaded accounts {:?}", accounts);
    //     },
    //     Err(err) => {
    //         warn!("Polling error {}", err);
    //     }
    // }

    // match web3.eth().block_with_txs(BlockId::Number(BlockNumber::Latest)).wait() {
    //     Ok(block) => {
    //         info!("Loaded accounts {:?}", block.unwrap().number.unwrap());
    //     },
    //     Err(err) => {
    //         warn!("Polling error {}", err);
    //     }
    // }

    let block = web3.eth().block_number().then(|block| {
        info!("Best Block: {:?}", block);
        Ok(())
    });
    let result = web3.transport().submit_batch().then(|accounts| {
        info!("Result: {:?}", accounts);
        Ok(())
    });

    remote.spawn(move |_| block);
    remote.spawn(move |_| result);

    loop {
        event_loop.turn(None);
    }
}
