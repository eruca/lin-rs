#[macro_use]
extern crate log;
extern crate env_logger;
extern crate tokio_core as core;
extern crate futures;
extern crate futures_cpupool as cpupool;

use std::net::SocketAddr;
use std::io::Read;
use std::convert::From;
use std::num::ParseFloatError;
use std::time;

use cpupool::CpuPool;
use futures::{Future, Poll, Async};
use futures::stream::Stream;

use core::net::UdpSocket;
use core::io::Io;

#[test]
fn it_works() {
    assert_eq!(1, 1);
}

type Result<T> = ::std::result::Result<T, LinError>;

pub enum PointType {
    Time(f64),
    Count(f64),
    Gauge(f64),
}

impl PointType {
    fn from_raw(ptype: char, value: &[u8]) -> Result<PointType> {
        let value = String::from_utf8_lossy(value);
        let value: f64 = value.trim().parse::<f64>()?;
        match ptype {
            't' => Ok(PointType::Time(value)),
            'c' => Ok(PointType::Count(value)),
            'g' => Ok(PointType::Gauge(value)),
            _ => Err(LinError::WrongLine),
        }
    }
}

pub struct Point {
    metric: String,
    tags: Vec<String>,
    at: u64,
    ptype: PointType,
}


const VLINE: u8 = '|' as u8;
const SPACE: u8 = ' ' as u8;

impl Point {
    pub fn from_bytes(input: Vec<u8>) -> Result<Point> {
        let line_splited: Vec<_> = input.rsplitn(3, |byte| byte == &VLINE).collect();
        if line_splited.len() != 3 {
            return Err(LinError::WrongLine);
        }

        let ptype = line_splited[0];
        let value = line_splited[1];

        let metric_tags = line_splited[2];
        let mut iter = metric_tags.split(|byte| byte == &SPACE);
        let metric = match iter.next() {
            Some(bytes) => String::from_utf8_lossy(bytes).trim().to_owned(),
            None => {
                return Err(LinError::WrongLine);
            }
        };
        let tags: Vec<_> = iter.map(|bytes| String::from_utf8_lossy(bytes).trim().to_owned())
            .collect();
        let ptype_str = String::from_utf8_lossy(ptype);
        let ptype = match ptype_str.trim().chars().next() {
            Some(c) => PointType::from_raw(c, value)?,
            None => return Err(LinError::WrongLine),
        };
        Ok(Point {
            metric: metric,
            tags: tags,
            at: now(),
            ptype: ptype,
        })
    }
}

struct LineBuf {
    buf: Vec<u8>,
}

const CLCR: u8 = '\n' as u8;

impl LineBuf {
    fn merge_product(&mut self, input: &[u8]) {
        self.buf.extend_from_slice(input);
    }
}

impl Stream for LineBuf {
    type Item = Point;
    type Error = LinError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, LinError> {
        let cr_pos = match self.buf.iter().position(|sbyte| &CLCR == sbyte) {
            Some(pos) => pos,
            None => return Ok(Async::NotReady),
        };
        // build a new Point from pos
        let line: Vec<_> = self.buf.drain(0..(cr_pos + 1)).collect();

        match Point::from_bytes(line) {
            Ok(pt) => Ok(Async::Ready(Some(pt))),
            Err(err) => {
                warn!("wrong line meet,error: {:?}", err);
                Ok(Async::NotReady)
            }
        }

    }
}

pub struct Collector {
    sock: UdpSocket,
    addr: SocketAddr,
}

impl Stream for Collector {
    type Item = (Vec<u8>);
    type Error = LinError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Async::NotReady = self.sock.poll_read() {
            return Ok(Async::NotReady);
        }

        Ok(Async::NotReady)
    }
}

#[derive(Debug)]
pub enum LinError {
    None,
    WrongLine,
    ParseValueError(ParseFloatError),
}

impl From<ParseFloatError> for LinError {
    fn from(oe: ParseFloatError) -> LinError {
        LinError::ParseValueError(oe)
    }
}
// TODO: Impl it
// fn run(bind: String, interval: u64, thresholds: &[usize]) {}

fn now() -> u64 {
    let now_time = time::SystemTime::now();
    let esp = now_time.duration_since(time::UNIX_EPOCH).unwrap();
    esp.as_secs()
}
