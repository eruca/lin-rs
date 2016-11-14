use std::collections::BTreeSet;

use futures::{Poll, Async};
use futures::stream::Stream;

use ::{LinError, Result};

pub struct Converter {
    local: Vec<u8>,
}

impl Converter {
    pub fn new(buf: Vec<u8>) -> Converter {
        Converter { local: buf }
    }
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
#[derive(Debug)]
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

#[derive(Debug, Hash, Clone, Eq, PartialEq,)]
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
