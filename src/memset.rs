use std::collections::HashMap;
use std::io::Read;
use std::sync::{Mutex, Arc};
use std::thread;
use std::mem;
use std::default::Default;

use crossbeam::sync::MsQueue;
use futures::{Future, Poll, Async};
use hyper::status::StatusCode;
use hyper::Client;
use hyper::header::{Headers, Accept, ContentType, qitem};
use itertools::Itertools;
use num_cpus;
use serde_json;

use ::{Proto, Entry, LinError, com, Index, Point};
use ::{Count, Time, Gauge, Delta};

pub const THRESHOLD: usize = 90;
pub const THRESHOLD_STR: &'static str = "_threshold=90";
pub const TOTAL_STR: &'static str = "_threshold=100";
pub const TOTAL: usize = 100;
pub const CHUNK_SIZE: usize = 16;

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
                idx.tags.insert("_kind=time".to_string());
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

                idx.tags.insert(TOTAL_STR.to_owned());
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

pub trait DeltaSet: Push {
    fn delta(&mut self, proto: Proto);
}

impl DeltaSet for GaugeSet {
    fn delta(&mut self, proto: Proto) {
        let value = proto.value;
        *self.inmap
            .entry(proto.index)
            .or_insert(0.0) += value;
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
                idx.tags.insert("_kind=gauge".to_owned());
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
                idx.tags.insert("_kind=count".to_owned());
                // TODO: How to caculate each time value?
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
    remote: String,
    client: Client,
    headers: Headers,
}

impl MetricSet {
    pub fn new(remote: String, queue: Arc<MsQueue<Proto>>) -> MetricSet {
        let mut headers = Headers::new();
        headers.set(ContentType("application/json".parse().unwrap()));
        headers.set(Accept(vec![qitem("application/json".parse().unwrap())]));
        MetricSet {
            time: Arc::new(Mutex::new(TimeSet::default())),
            gauge: Arc::new(Mutex::new(GaugeSet::default())),
            count: Arc::new(Mutex::new(CountSet::default())),
            queue: queue,
            remote: remote,
            client: Client::default(),
            headers: headers,
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
                    Delta => {
                        let mut gauge = gauge.lock().unwrap();
                        gauge.delta(proto);
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
            debug!("Flush ticked");

            // json and send back to remote
            // limit too long body to avoid of buffer overflow in lindb
            for iter in entries.into_iter().chunks(CHUNK_SIZE).into_iter() {
                let entries: Vec<_> = iter.collect();
                let buf = match serde_json::to_vec(&entries) {
                    Ok(buf) => buf,
                    Err(err) => {
                        error!("error while encode json {:?}", err);
                        debug!("encode json={:?} error={:?}", &entries, err);
                        continue;
                    }
                };
                let request = self.client.put(&self.remote);
                let request = request.headers(self.headers.clone());
                let request = request.body(&buf[..]);
                match request.send() {
                    Ok(mut resp) => {
                        if resp.status != StatusCode::Ok {
                            let mut body = String::new();
                            resp.read_to_string(&mut body).unwrap();
                            warn!("send put request error, body: {:?}", body);
                        }
                    }
                    Err(err) => {
                        error!("send put request error: {:?}", err);
                        continue;
                    }
                };

            }

        }

        for jh in jhs {
            jh.join().unwrap();
        }
        Ok(Async::NotReady)
    }
}
