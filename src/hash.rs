use std::hash::Hasher;
use std::sync::Arc;
use std::default::Default;

use crossbeam::sync::MsQueue;
use fnv::FnvHasher;

pub use net::Collector;
pub use convert::{Converter, Proto, Index, Gauge, Count, Time};
pub use lin::{Point, Entry};
pub use memset::MetricSet;

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
