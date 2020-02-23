// TODO: Enable on release
#![forbid(unsafe_code)]
#![deny(non_upper_case_globals)]
#![deny(non_camel_case_types)]
#![deny(non_snake_case)]
#![deny(unused_mut)]
#![deny(unused_variables)]
//#![deny(dead_code)]
#![deny(unused_imports)]
//#![deny(missing_docs)]

extern crate web3;
extern crate clap;
extern crate futures;
extern crate tokio;
extern crate ethbloom;
extern crate fern;
extern crate chrono;
extern crate rdkafka;
extern crate structopt;
extern crate serde_regex;
extern crate derivative;
#[macro_use] extern crate log;

mod app;
mod configuration;
mod ethereum;
mod messaging;
mod coordination;

use std::{thread};
use signal_hook::{iterator::Signals, SIGINT};

use std::process::exit;
use log::LevelFilter;

use configuration::settings::{ Settings };
use configuration::constants::{ common };
use rdkafka::producer::BaseProducer;
use configuration::command_line::{ Opt, LogLevel };
use structopt::StructOpt;
use std::path::PathBuf;
use app::app::App;

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
    let kafka = settings.kafka;
    let mq_producer = BaseProducer::from(kafka);

    info!("Creating connection to {}", settings.ethereum.url.as_str());
    let (_eloop, transport) = web3::transports::Http::with_max_parallel(settings.ethereum.url.as_str(), common::MAX_PARALLEL_REQUESTS).unwrap();
    let application = App::new(&settings.ethereum, &transport);
    application.run(mq_producer);
}
