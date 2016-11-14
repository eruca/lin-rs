use std::time;
use std::sync::mpsc::{self, Receiver};
use std::thread;

/// timer make a channel to recveive each time
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

/// Get now Unix second between now and UNIX_EPOCH
pub fn now() -> u64 {
    let now_time = time::SystemTime::now();
    let esp = now_time.duration_since(time::UNIX_EPOCH).unwrap();
    esp.as_secs()
}

/// compare and return the max one
pub fn max<T: PartialOrd + Copy>(lhs: T, rhs: T) -> T {
    if lhs > rhs { lhs } else { rhs }
}

/// compare and return the min one
pub fn min<T: PartialOrd + Copy>(lhs: T, rhs: T) -> T {
    if lhs < rhs { lhs } else { rhs }
}
