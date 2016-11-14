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

/// HashRing provide a way to hash all the proto into more lower level locker
#[derive(Clone)]
pub struct HashRing {
    ring: Vec<Arc<MsQueue<Proto>>>,
}

impl HashRing {
    /// dispatch the input proto into the right MsQueue
    pub fn dispatch(&self, proto: Proto) {
        let mut hasher = FnvHasher::default();
        hasher.write(proto.index.metric.as_bytes());
        let hash_val = hasher.finish();
        let pos = hash_val as usize & BIT_SHIELD;
        self.ring[pos].push(proto);
    }

    /// show out all the MsQueue to open handle
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
