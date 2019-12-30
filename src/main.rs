// TODO: Enable on release
//#![forbid(unsafe_code)]
//#![deny(non_upper_case_globals)]
//#![deny(non_camel_case_types)]
//#![deny(non_snake_case)]
//#![deny(unused_mut)]
//#![deny(dead_code)]
//#![deny(unused_imports)]
//#![deny(missing_docs)]
//#![forbid(unsafe_code)]

extern crate web3;
extern crate clap;
extern crate futures;
extern crate tokio;
extern crate ethbloom;
extern crate fern;
extern crate chrono;
extern crate rdkafka;
extern crate structopt;
#[macro_use] extern crate log;

mod configuration;
mod ethereum;
mod messaging;

use std::{thread};
use signal_hook::{iterator::Signals, SIGINT};
use futures::channel::mpsc;

use std::process::exit;
use log::LevelFilter;

use configuration::settings::{ Settings };
use configuration::constants::{ common };
use futures::executor::block_on;
use rdkafka::producer::BaseProducer;
use messaging::SendLog;
use configuration::command_line::{ Opt, LogLevel };
use structopt::StructOpt;
use std::path::PathBuf;

fn main() {
    let options = Opt::from_args();
    let signals = Signals::new(&[SIGINT]).unwrap();

    thread::spawn(move || {
        for sig in signals.forever() {
            info!("Received signal {:?}, stopping", sig);
            exit(0);
        }
    });
    cli(options);
}

fn init_logging(level: LevelFilter, output: &Option<PathBuf>) {
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
        .level(level)
        .chain(std::io::stdout());

    match output {
        Some(log_file) => dispatcher = dispatcher.chain(fern::log_file(log_file).unwrap()),
        _ => { /* ignored */ }
    }
    dispatcher.apply().unwrap();
    info!("Logging level {} enabled", level);
}

fn cli(options: Opt) {
    let settings: Settings;
    match options.config {
        Some(v) => {
            match Settings::new(v) {
                Ok(config) => settings = config,
                Err(e) => panic!("Error: {:?}", e)
            }
        }
        None => {
            warn!("Config not set, using default");
            settings = Settings::default();
        }
    }
    let log_level = options.logging
        .map(LogLevel::into)
        .unwrap_or(settings.log.level);
    init_logging(log_level, &options.log_output_file);
    debug!("Loaded configurations {:?}", settings);

    let (tx, mut rx) = mpsc::channel(1_024);

    let kafka = settings.kafka;
    thread::spawn(move || {
        let mq_producer = BaseProducer::from(kafka);
        loop {
            match rx.try_next() {
                Ok(Some(message)) => {
                    info!("Received message {:?}", message);
                    match mq_producer.send_log(&message) {
                        Ok(_) => trace!("Successful sent message"),
                        Err(error) => error!("Failed to send message {}", error)
                    };
                },
                _ => {/* ignored */}
            };
        }
    });

    info!("Creating connection to {}", settings.ethereum.url.as_str());
    let (_eloop, transport) = web3::transports::Http::with_max_parallel(settings.ethereum.url.as_str(), common::MAX_PARALLEL_REQUESTS).unwrap();
    let mut listener = ethereum::client::LogListener::new(
        &transport,
        &settings.ethereum.logs,
        settings.ethereum.start_block,
        settings.ethereum.batch_size
    );
    let loops = move || async {
        let event_listener = listener.run(tx);

        futures::join!(event_listener);
        listener
    };
    block_on(loops());
}
