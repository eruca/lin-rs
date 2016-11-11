#![feature(proc_macro)]
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

extern crate crossbeam;
extern crate env_logger;
extern crate futures;
extern crate futures_cpupool as cpupool;
extern crate hyper;
extern crate num_cpus;
extern crate serde;
extern crate serde_json;
extern crate tokio_core as core;

pub mod net;

use std::io;
use std::num::ParseFloatError;
use std::convert::{From, Into};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Mutex, Arc};
use std::thread;
use std::mem;
use std::net::SocketAddr;

use cpupool::CpuPool;
use core::net::UdpSocket;
use core::reactor::{Core, Handle};
use crossbeam::sync::MsQueue;
use futures::{Future, IntoFuture, Poll, Async};
use futures::stream::Stream;
use serde::ser::{Serialize, Serializer};



pub struct Collector {
    socket: UdpSocket,
    local: [u8; 8 * 1024],
}

impl Collector {
    pub fn bind(addr: &str, handle: &Handle) -> Collector {
        let addr = addr.parse::<SocketAddr>().unwrap();
        let socket = UdpSocket::bind(&addr, handle).unwrap();
        Collector {
            socket: socket,
            local: [0u8; 8 * 1024],
        }
    }
}

impl Stream for Collector {
    type Item = Vec<u8>;
    type Error = LinError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.socket
            .recv_from(&mut self.local)
            .map_err(Into::<LinError>::into)
            .into_future()
            .map(|(size, remote)| {
                debug!("receive {} byte from {}", size, remote);
                Some(Vec::from(&self.local[..size]))
            })
            .poll()
    }
}

pub struct Converter<I>
    where I: Stream<Item = Vec<u8>, Error = LinError>
{
    collector: I,
}

impl<I> Stream for Converter<I>
    where I: Stream<Item = Vec<u8>, Error = LinError>
{
    type Item = Vec<u8>;
    type Error = LinError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // TODO: impl it
        Ok(Async::NotReady)
    }
}

#[derive(PartialEq, Eq, Hash, Copy,Clone)]
pub enum ProtoType {
    Count,
    Time,
    Gauge,
}

pub use ProtoType::{Count, Time, Gauge};

impl ProtoType {
    fn from_raw(c: char) -> Result<ProtoType> {
        match c {
            'c' => Ok(Count),
            't' => Ok(Time),
            'g' => Ok(Gauge),
            _ => Err(LinError::WrongLine),
        }
    }
}

#[derive(Hash, Clone, Eq, PartialEq,)]
pub struct Index {
    pub metric: String,
    pub tags: BTreeSet<String>,
    pub ptype: ProtoType,
}

impl Index {
    pub fn build(metric: String, tags: BTreeSet<String>, ptype: char) -> Result<Index> {
        let ptype = ProtoType::from_raw(ptype)?;
        Ok(Index {
            metric: metric,
            tags: tags,
            ptype: ptype,
        })
    }
}

pub struct Proto {
    pub index: Index,
    pub value: f64,
}

pub const CLCR: u8 = '\n' as u8;
pub const VLINE: u8 = '|' as u8;
pub const SPACE: u8 = ' ' as u8;

impl Proto {
    fn split_tags(input: &[u8]) -> Result<(String, BTreeSet<String>)> {
        let mut lsp = input.split(|x| x == &SPACE).filter(|bytes| bytes.len() != 0);

        let metric = lsp.next()
            .map(|bytes| {
                let metric = String::from_utf8_lossy(bytes);
                metric.trim().to_owned()
            })
            .unwrap();

        let tags: BTreeSet<_> = lsp.map(|bytes| {
                let tag = String::from_utf8_lossy(bytes);
                tag.trim().to_owned()
            })
            .collect();
        Ok((metric, tags))
    }

    pub fn from_raw(line: &[u8]) -> Result<Proto> {
        if line.len() == 0 {
            return Err(LinError::WrongLine);
        }

        let mut lsp = line.split(|x| x == &VLINE);
        let info = lsp.next().ok_or(LinError::WrongLine)?;
        let (metric, tags) = Proto::split_tags(info)?;

        let value = lsp.next()
            .and_then(|x| {
                let value = String::from_utf8_lossy(x);
                value.parse::<f64>()
                    .map_err(|err| {
                        error!("parse error at wrong line");
                        err
                    })
                    .ok()
            })
            .ok_or(LinError::WrongLine)?;

        let ptype = lsp.next()
            .and_then(|x| {
                let ptype = String::from_utf8_lossy(x);
                ptype.chars().filter(|c| !c.is_whitespace()).next()
            })
            .ok_or(LinError::WrongLine)?;

        let index = Index::build(metric, tags, ptype)?;

        Ok(Proto {
            index: index,
            value: value,
        })
    }
}

pub struct Point {
    kind: &'static str,
    value: f64,
}

impl Serialize for Point {
    fn serialize<S>(&self, serializer: &mut S) -> ::std::result::Result<(), S::Error>
        where S: Serializer
    {
        let mut state = serializer.serialize_map(Some(2))?;
        serializer.serialize_map_key(&mut state, "pointType")?;
        serializer.serialize_map_value(&mut state, self.kind)?;
        serializer.serialize_map_key(&mut state, "value")?;
        serializer.serialize_map_value(&mut state, self.value)?;
        serializer.serialize_map_end(state)
    }
}

pub struct Entry {
    index: Index,
    /// limit to 4 element
    time: u64,
    points: Vec<Point>,
}


impl Serialize for Entry {
    fn serialize<S>(&self, s: &mut S) -> ::std::result::Result<(), S::Error>
        where S: Serializer
    {
        let mut state = s.serialize_map(Some(4))?;

        // 1. Metric
        s.serialize_map_key(&mut state, "name")?;
        s.serialize_map_value(&mut state, &self.index.metric)?;

        // 2. tags
        s.serialize_map_key(&mut state, "tags")?;
        // tags values;
        {
            let len = self.index.tags.len();
            let mut tag_state = s.serialize_map(Some(len))?;
            for kv in &self.index.tags {
                let ksp: Vec<_> = kv.splitn(2, "=").collect();
                if ksp.len() != 2 {
                    continue;
                }

                s.serialize_map_key(&mut tag_state, ksp[0])?;
                s.serialize_map_value(&mut tag_state, ksp[1])?;
            }
            s.serialize_map_end(tag_state)?;
        }

        // 3. time
        s.serialize_map_key(&mut state, "timestamp")?;
        s.serialize_map_value(&mut state, self.time)?;

        // 4. points
        s.serialize_map_key(&mut state, "points")?;
        s.serialize_map_value(&mut state, &self.points)?;

        s.serialize_map_end(state)
    }
}


pub trait Truncate {
    fn truncate(&mut self) -> Self;
}

pub trait IntoEntries {
    fn into_entries(self) -> Vec<Entry>;
}

macro_rules! impl_truncate {
    ($set:ty) => {
        impl Truncate for $set {
            fn truncate(&mut self) -> Self {
                let mut inmap = HashMap::new();
                mem::swap(&mut self.inmap, &mut inmap);
                Self {
                    inmap:inmap,
                }
            }
        }
    }
}

impl_truncate!(TimeSet);
impl_truncate!(CountSet);
impl_truncate!(GaugeSet);

pub trait Push {
    fn push(&mut self, value: Proto);
}

pub struct TimeSet {
    inmap: HashMap<Index, Vec<f64>>,
}


impl IntoEntries for TimeSet {
    fn into_entries(self) -> Vec<Entry> {
        let now = com::now();
        self.inmap
            .into_iter()
            .flat_map(|(mut idx, list)| {
                idx.tags.insert("kind=time".to_string());
                let mut ets = Vec::new();
                let len = list.len();
                if len == 0 {
                    return ets.into_iter();
                }

                let upper = com::max(len * THRESHOLD / TOTAL, 1);
                let (mut sum, mut max, mut min) = (list[0], list[0], list[0]);
                for &val in &list[1..upper] {
                    sum += val;
                    max = com::max(max, val);
                    min = com::min(min, val);
                }
                let mut low_idx = idx.clone();
                low_idx.tags.insert(THRESHOLD_STR.to_owned());
                ets.push(Entry {
                    index: low_idx,
                    time: now,
                    points: vec![
                        Point{ kind: "SUM", value: sum},
                        Point{ kind: "MAX", value: max},
                        Point{ kind: "MIN", value: min},
                        Point{ kind: "COUNT", value: upper as f64},
                    ],
                });

                for &val in &list[upper..] {
                    sum += val;
                    max = com::max(max, val);
                    min = com::min(min, val);
                }
                ets.push(Entry {
                    index: idx,
                    time: now,
                    points: vec![
                        Point{ kind: "SUM", value: sum},
                        Point{ kind: "MAX", value: max},
                        Point{ kind: "MIN", value: min},
                        Point{ kind: "COUNT", value: len as f64},
                    ],
                });
                ets.into_iter()
            })
            .collect()
    }
}

impl Push for TimeSet {
    fn push(&mut self, proto: Proto) {
        let value = proto.value;
        self.inmap
            .entry(proto.index)
            .or_insert(Vec::new())
            .push(value);
    }
}

pub struct GaugeSet {
    inmap: HashMap<Index, f64>,
}

impl Push for GaugeSet {
    fn push(&mut self, proto: Proto) {
        let value = proto.value;
        self.inmap.insert(proto.index, value);
    }
}

impl IntoEntries for GaugeSet {
    fn into_entries(self) -> Vec<Entry> {
        let now = com::now();
        self.inmap
            .into_iter()
            .map(|(mut idx, val)| {
                idx.tags.insert("kind=gauge".to_owned());
                let pts = vec![
                    Point{ kind: "SUM", value: val},
                    Point{ kind: "MAX", value: val},
                    Point{ kind: "MIN", value: val},
                    Point{ kind: "COUNT", value: 1.0},
                ];
                Entry {
                    index: idx,
                    time: now,
                    points: pts,
                }
            })
            .collect()
    }
}

pub struct CountSet {
    inmap: HashMap<Index, (f64, u64)>,
}

impl IntoEntries for CountSet {
    fn into_entries(self) -> Vec<Entry> {
        let now = com::now();
        self.inmap
            .into_iter()
            .map(|(mut idx, val)| {
                idx.tags.insert("kind=count".to_owned());
                let vec = vec![
                    Point{ kind: "SUM", value: val.0},
                    Point{ kind: "COUNT", value: val.1 as f64},
                ];
                Entry {
                    index: idx,
                    time: now,
                    points: vec,
                }
            })
            .collect()
    }
}

impl Push for CountSet {
    fn push(&mut self, proto: Proto) {
        let value = proto.value;
        let item = self.inmap
            .entry(proto.index)
            .or_insert((value, 1));
        item.0 += value;
        item.1 += 1;
    }
}

pub struct MetricSet {
    time: Arc<Mutex<TimeSet>>,
    gauge: Arc<Mutex<GaugeSet>>,
    count: Arc<Mutex<CountSet>>,
    queue: Arc<MsQueue<Proto>>,
}

impl MetricSet {
    fn poll_run(&self) -> thread::JoinHandle<()> {
        let queue = self.queue.clone();
        let time = self.time.clone();
        let gauge = self.gauge.clone();
        let count = self.count.clone();
        thread::spawn(move || {
            loop {
                let proto = queue.pop();
                match proto.index.ptype {
                    Count => {
                        let mut count = count.lock().unwrap();
                        count.push(proto);
                    }
                    Time => {
                        let mut time = time.lock().unwrap();
                        time.push(proto);
                    }
                    Gauge => {
                        let mut gauge = gauge.lock().unwrap();
                        gauge.push(proto);
                    }
                };
            }
        })
    }

    fn into_entries(&mut self) -> Vec<Entry> {
        let mut time_guard = self.time.lock().unwrap();
        let mut gauge_guard = self.gauge.lock().unwrap();
        let mut count_guard = self.count.lock().unwrap();
        let time = time_guard.truncate();
        let gauge = gauge_guard.truncate();
        let count = count_guard.truncate();
        mem::drop(time_guard);
        mem::drop(gauge_guard);
        mem::drop(count_guard);
        let mut entries = time.into_entries();
        entries.extend(gauge.into_entries().into_iter());
        entries.extend(count.into_entries().into_iter());
        entries
    }
}

impl Future for MetricSet {
    type Item = ();
    type Error = LinError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let threads = com::max(num_cpus::get() / 8, 1);
        let jhs: Vec<_> = (0..threads)
            .into_iter()
            .map(|_| self.poll_run())
            .collect();

        let ticker = com::timer(10);
        loop {
            match ticker.recv() {
                Ok(_) => {}
                Err(err) => {
                    error!("timer recv error={}", err);
                    break;
                }
            };
            let entries = self.into_entries();
            if entries.len() == 0 {
                continue;
            }

            // json and send back to remote
        }

        for jh in jhs {
            jh.join().unwrap();
        }
        Ok(Async::NotReady)
    }
}


impl Push for MetricSet {
    fn push(&mut self, proto: Proto) {
        match proto.index.ptype {
            Count => {
                let mut lock = self.count.lock().unwrap();
                lock.push(proto);
            }
            Time => {
                let mut lock = self.time.lock().unwrap();
                lock.push(proto);
            }
            Gauge => {
                let mut lock = self.gauge.lock().unwrap();
                lock.push(proto);
            }
        }
    }
}

pub fn run() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let collector = Collector::bind(":8179", &handle);
    let converter = Converter { collector: collector };
    let serve = converter.for_each(|proto| Ok(()));
    let cpu_count = num_cpus::get();
    let pool = CpuPool::new(com::max(cpu_count / 2, 1));
    let service = pool.spawn(serve);
    core.run(service).unwrap();
}

pub const THRESHOLD: usize = 90;
pub const THRESHOLD_STR: &'static str = "upper=true";
pub const TOTAL: usize = 100;

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

pub mod com {
    use std::time;
    use std::sync::mpsc::{self, Receiver};
    use std::thread;

    pub fn timer(sleep: u64) -> Receiver<()> {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let duration = time::Duration::new(sleep, 0);
            loop {
                thread::sleep(duration);
                tx.send(()).unwrap();
            }
        });
        rx
    }

    pub fn now() -> u64 {
        let now_time = time::SystemTime::now();
        let esp = now_time.duration_since(time::UNIX_EPOCH).unwrap();
        esp.as_secs()
    }

    pub fn max<T: PartialOrd + Copy>(lhs: T, rhs: T) -> T {
        if lhs > rhs { lhs } else { rhs }
    }

    pub fn min<T: PartialOrd + Copy>(lhs: T, rhs: T) -> T {
        if lhs < rhs { lhs } else { rhs }
    }

}
