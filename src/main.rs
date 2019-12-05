extern crate web3;
extern crate clap;
extern crate futures;
extern crate tokio;
extern crate ethbloom;
extern crate fern;
extern crate chrono;
#[macro_use] extern crate log;

mod configuration;
mod ethereum;

use clap::{ App, Arg, ArgMatches };
use std::{thread};
use signal_hook::{iterator::Signals, SIGINT};
use futures::channel::mpsc;

use std::process::exit;
use log::LevelFilter;
use std::str::FromStr;

use configuration::settings::{ Settings };
use configuration::constants::{ cargo_env, config_params };

fn main() {
    let matches = App::new(cargo_env::CARGO_PKG_NAME)
        .author(cargo_env::CARGO_PKG_AUTHORS)
        .version(cargo_env::CARGO_PKG_VERSION)
        .arg(Arg::with_name(config_params::CONFIG)
             .short(config_params::CONFIG_SHORT)
             .long(config_params::CONFIG)
             .env(config_params::CONFIG_ENV)
             .value_name("FILE")
             .takes_value(true)
             .help(config_params::CONFIG_DESC))
        .arg(Arg::with_name(config_params::LOG)
             .short(config_params::LOG_SHORT)
             .long(config_params::LOG)
             .value_name(config_params::LOG_ENV)
             .takes_value(true)
             .default_value("Info")
             .possible_values(&["Off", "Error", "Warn", "Info", "Debug", "Trace"])
             .required(false)
             .help(config_params::LOG_DESC))
        .arg(Arg::with_name(config_params::LOG_FILE)
            .short(config_params::LOG_FILE_SHORT)
            .long(config_params::LOG_FILE)
            .value_name(config_params::LOG_FILE_ENV)
            .takes_value(true)
            .required(false)
            .help(config_params::LOG_FILE_DESC))
        .arg(Arg::with_name(config_params::NODE_URL)
             .short(config_params::NODE_URL_SHORT)
             .long(config_params::NODE_URL)
             .value_name(config_params::NODE_URL_ENV)
             .takes_value(true)
             .help(config_params::NODE_URL_DESC))
        .get_matches();
    let signals = Signals::new(&[SIGINT]).unwrap();

    thread::spawn(move || {
        for sig in signals.forever() {
            info!("Received signal {:?}, stopping", sig);
            exit(0);
        }
    });
    cli(matches);
}

fn cli(matches: ArgMatches) {
    let settings: Settings;
    match matches.value_of(config_params::CONFIG) {
        Some(v) => {
            match Settings::new(v.to_string()) {
                Ok(config) => settings = config,
                Err(e) => panic!("Error: {:?}", e)
            }
        }
        None => {
            warn!("Config not set, using default");
            settings = Settings::default();
        }
    }
    let log_level = matches.value_of(config_params::LOG)
        .map(|v| LevelFilter::from_str(v).unwrap()).unwrap_or(settings.log.level);
    let mut dispatcher = fern::Dispatch::new()
        // Perform allocation-free log formatting
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log_level)
        .chain(std::io::stdout());

    match matches.value_of(config_params::LOG_FILE) {
        Some(log_file) => dispatcher = dispatcher.chain(fern::log_file(log_file).unwrap()),
        _ => { /* ignored */ }
    }
    dispatcher.apply().unwrap();
    debug!("Loaded configurations {:?}", settings);
    info!("Logging level {} enabled", settings.log.level);

    let (tx, mut rx) = mpsc::channel(1_024);

    thread::spawn(move || {
        loop {
            match rx.try_next() {
                Ok(opt) => {
                    if let Some(message) = opt {
                        info!("Received message {:?}", message);
                    }
                },
                _ => {/* ignored */}
            };
        }
    });

    ethereum::client::run_main_loop(
        settings.ethereum.url.as_str(),
        &settings.ethereum.logs,
        settings.ethereum.start_block,
        settings.ethereum.batch_size,
        tx
    );
}
