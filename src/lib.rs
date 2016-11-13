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
extern crate hyper;
extern crate num_cpus;
extern crate serde;
extern crate serde_json;
extern crate test;
extern crate tokio_core as core;
extern crate fnv;
extern crate net2;

use std::io;
use std::num::ParseFloatError;
use std::convert::{From, Into};
use std::collections::{BTreeSet, HashMap};
use std::hash::Hasher;
use std::io::ErrorKind;
use std::sync::{Mutex, Arc};
use std::thread;
use std::mem;
use std::default::Default;
use std::net::SocketAddr;

use core::net::UdpSocket;
use core::reactor::{Core, Handle};
use crossbeam::sync::MsQueue;
use fnv::FnvHasher;
use futures::{Future, IntoFuture, Poll, Async};
use futures::stream::Stream;
use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;
use serde::ser::{Serialize, Serializer};

#[derive(Debug)]
pub struct Collector {
    socket: UdpSocket,
    local: Vec<u8>,
}

impl Collector {
    pub fn bind(addr: &str, handle: &Handle) -> Collector {
        let addr = addr.parse::<SocketAddr>().unwrap();
        let builder = UdpBuilder::new_v4().expect("UdpBuilder not initial");
        let std_socket = builder.reuse_address(true)
            .expect("REUSE_ADDRESS not support in this os")
            .reuse_port(true)
            .expect("REUSE_PORT not support in this os")
            .bind(addr)
            .expect("Bind Error");
        let socket = UdpSocket::from_socket(std_socket, handle).unwrap();
        // let socket = UdpSocket::bind(&addr, handle).unwrap();
        let mut vec = Vec::new();
        // vec.resize(8, 0);
        vec.resize(8 * 1024 * 1024, 0);
        Collector {
            socket: socket,
            local: vec,
        }
    }
}

impl Stream for Collector {
    type Item = Vec<u8>;
    type Error = LinError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Async::NotReady = self.socket.poll_read() {
            return Ok(Async::NotReady);
        }

        let inner = match self.socket.recv_from(&mut self.local) {
            Ok(inner) => Ok(inner),
            Err(err) => {
                if err.kind() == ErrorKind::WouldBlock {
                    return Ok(Async::NotReady);
                }
                Err(err)
            }
        };
        inner.into_future()
            .map_err(|err| Into::<LinError>::into(err))
            .map(|(size, remote)| {
                debug!("receive {} byte from {}", size, remote);
                Some(Vec::from(&self.local[..size]))
            })
            .poll()
    }
}

pub struct Converter {
    local: Vec<u8>,
}

impl Stream for Converter {
    type Item = Proto;
    type Error = LinError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.local.len() == 0 {
            // poll from stream
            return Ok(Async::Ready(None));
        }
        let first = match self.local.iter().position(|x| x == &CLCR) {
            Some(pos) => pos,
            None => {
                warn!("wrong line meet at {}",
                      String::from_utf8_lossy(&self.local));
                self.local.clear();
                return Ok(Async::NotReady);
            }
        };

        if first == 0 {
            return Ok(Async::NotReady);
        }

        let drained = self.local.drain(..first + 1);
        let line: Vec<_> = drained.into_iter().collect();
        let line_len = line.len();
        let proto = match Proto::from_raw(&line[..line_len - 1]) {
            Ok(proto) => proto,
            Err(err) => {
                warn!("wrong line meet, and parse error :{:?}", err);
                return Ok(Async::NotReady);
            }
        };
        Ok(Async::Ready(Some(proto)))
    }
}


#[derive(PartialEq, Eq, Hash, Copy,Clone)]
pub enum ProtoType {
    Count,
    Time,
    Gauge,
}

pub use self::ProtoType::{Count, Time, Gauge};

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

        // 3. time
        s.serialize_map_key(&mut state, "timestamp")?;
        s.serialize_map_value(&mut state, self.time)?;

        // 4. points
        s.serialize_map_key(&mut state, "points")?;
        s.serialize_map_value(&mut state, &self.points)?;

        s.serialize_map_end(state)
    }
}

#[cfg(test)]
mod bench {
    use super::{encode, product};
    use test::Bencher;

    #[bench]
    fn bench_encode(b: &mut Bencher) {
        let entry = product();
        let mut len = 0usize;
        let mut count = 0usize;
        b.iter(|| for _ in 0..10000 {
            len += encode(&entry);
            count += 1;
        });
        let len = len as f64 / 1024.0 / 1024.0;
        let _avg = len / (count as f64);
        // println!("len is :{}, count: {}, avg: {} ",
        //          len ,
        //          count,
        //          avg,
        // );
    }
}

pub fn product() -> Entry {
    let mut tags = BTreeSet::new();
    tags.insert("from=localhost".to_owned());
    tags.insert("service=lin.rs".to_owned());
    tags.insert("iface=encode".to_owned());
    let idx = Index::build("helloWorld".to_string(), tags, 'c').unwrap();
    let vecs = vec![
        Point{kind: "SUM", value: 50000.0},
        Point{kind: "COUNT", value:11.0},
        Point{kind: "MAX", value:2013.0},
        Point{kind: "MIN", value:10.0},];
    let now = com::now();
    Entry {
        index: idx,
        points: vecs,
        time: now,
    }
}

pub fn encode(entry: &Entry) -> usize {
    let encoded = serde_json::to_string(&entry).unwrap();
    encoded.len()
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

impl Default for TimeSet {
    fn default() -> Self {
        TimeSet { inmap: HashMap::new() }
    }
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

impl Default for GaugeSet {
    fn default() -> Self {
        GaugeSet { inmap: HashMap::new() }
    }
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

impl Default for CountSet {
    fn default() -> Self {
        CountSet { inmap: HashMap::new() }
    }
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
    fn new(queue: Arc<MsQueue<Proto>>) -> MetricSet {
        MetricSet {
            time: Arc::new(Mutex::new(TimeSet::default())),
            gauge: Arc::new(Mutex::new(GaugeSet::default())),
            count: Arc::new(Mutex::new(CountSet::default())),
            queue: queue,
        }
    }

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
            info!("Flush ticked");
            // TODO: impl it
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

const BIT_SHIELD: usize = 0x7;
const WORKER_THREAD: usize = 8;

#[derive(Clone)]
pub struct HashRing {
    ring: Vec<Arc<MsQueue<Proto>>>,
}

impl HashRing {
    pub fn dispatch(&self, proto: Proto) {
        let mut hasher = FnvHasher::default();
        hasher.write(proto.index.metric.as_bytes());
        let hash_val = hasher.finish();
        let pos = hash_val as usize & BIT_SHIELD;
        self.ring[pos].push(proto);
    }

    pub fn ring_queues(&self) -> &[Arc<MsQueue<Proto>>] {
        &self.ring
    }
}

impl Default for HashRing {
    fn default() -> Self {
        let vecs: Vec<_> = (0..WORKER_THREAD)
            .into_iter()
            .map(|_| Arc::new(MsQueue::new()))
            .collect();
        HashRing { ring: vecs }
    }
}


unsafe impl Sync for HashRing {}

pub fn run(bind: &str) {
    env_logger::init().unwrap();
    let ring = Arc::new(HashRing::default());
    let consumers: Vec<_> = ring.ring_queues()
        .iter()
        .map(|queue| {
            let queue = queue.clone();
            thread::spawn(move || {
                let mut set = MetricSet::new(queue);
                set.poll().unwrap()
            })
        })
        .collect();

    info!("start to produce");
    run_producer(bind, ring.clone());
    for con in consumers.into_iter() {
        con.join().unwrap();
    }
}

fn run_producer(bind: &str, ring: Arc<HashRing>) {
    let mut core = Core::new().unwrap();
    // let count = com::max(num_cpus::get() / 4, 1);
    let handle = core.handle();
    let collecter = Collector::bind(bind, &handle);
    let service = collecter.map(|packet| Converter { local: packet })
        .flatten()
        .for_each(|proto| {
            info!("produce a proto");
            ring.dispatch(proto);
            Ok(())
        });
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
