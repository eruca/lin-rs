#![feature(more_struct_aliases)]
#![feature(test)]
#![feature(proc_macro)]

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

extern crate crossbeam;
extern crate env_logger;

extern crate futures;
extern crate futures_cpupool as pool;
#[macro_use]
extern crate hyper;
extern crate num_cpus;
extern crate serde;
extern crate serde_json;
extern crate test;
extern crate tokio_core as core;
extern crate fnv;
extern crate net2;

pub mod convert;
pub mod com;
pub mod lin;
pub mod net;
pub mod memset;
pub mod hash;

use std::io;
use std::num::ParseFloatError;
use std::convert::From;
use std::sync::Arc;
use std::thread;
use std::default::Default;

use core::reactor::Core;
use futures::Future;
use futures::stream::Stream;

pub use net::Collector;
pub use convert::{Converter, Proto, Index, Gauge, Count, Time, Delta};
pub use lin::{Point, Entry};
pub use memset::MetricSet;
pub use hash::HashRing;

/// Start a new bind at `bind` upd socket and read socket.
/// And send serialized byte code json request to `remote` url
pub fn run(bind: &str, remote: &str) {
    env_logger::init().unwrap();
    let ring = Arc::new(HashRing::default());
    let remote = remote.to_string();
    // start comsumer and serializer threads
    let consumers: Vec<_> = ring.ring_queues()
        .iter()
        .map(|queue| {
            let queue = queue.clone();
            let remote = remote.clone();
            thread::spawn(move || {
                let mut set = MetricSet::new(remote, queue);
                set.poll().unwrap()
            })
        })
        .collect();

    // start producer and collector threads
    let count = com::max(num_cpus::get() / 4, 1);
    info!("start {} produce thread", count);
    let producer: Vec<_> = (0..count)
        .into_iter()
        .map(|_| {
            let bind = bind.to_owned();
            let ring = ring.clone();
            thread::spawn(move || {
                run_producer(&bind, ring);
            })
        })
        .collect();

    for (con, pro) in consumers.into_iter().zip(producer.into_iter()) {
        con.join().unwrap();
        pro.join().unwrap();
    }
}

/// start a new collector thread to accept udp proto line packet
fn run_producer(bind: &str, ring: Arc<HashRing>) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let collecter = Collector::bind(bind, &handle);
    let service = collecter.map(|packet| Converter::new(packet))
        .flatten()
        .for_each(|proto| {
            ring.dispatch(proto);
            Ok(())
        });
    core.run(service).unwrap();
}

type Result<T> = ::std::result::Result<T, LinError>;

#[derive(Debug)]
pub enum LinError {
    None,
    WrongLine,
    ParseValueError(ParseFloatError),
    IoError(io::Error),
}

impl From<ParseFloatError> for LinError {
    fn from(oe: ParseFloatError) -> LinError {
        LinError::ParseValueError(oe)
    }
}

impl From<io::Error> for LinError {
    fn from(oe: io::Error) -> LinError {
        LinError::IoError(oe)
    }
}
