//! Byte credits retained until the last shared input consumer releases them.
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

#[derive(Debug)]
pub struct InputBudget {
    limit: usize,
    used: Mutex<usize>,
    changed: Condvar,
    cancelled: Arc<AtomicBool>,
}

#[derive(Debug)]
pub struct InputPermit {
    budget: Arc<InputBudget>,
    bytes: usize,
}

impl InputBudget {
    pub fn new(limit: usize, cancelled: Arc<AtomicBool>) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: Mutex::new(0),
            changed: Condvar::new(),
            cancelled,
        })
    }

    /// None means cancellation or a request larger than the total budget.
    pub fn acquire(self: &Arc<Self>, bytes: usize) -> Option<InputPermit> {
        if bytes > self.limit {
            return None;
        }
        let mut used = self.used.lock().unwrap();
        loop {
            if self.cancelled.load(Ordering::Acquire) {
                return None;
            }
            if bytes <= self.limit - *used {
                *used += bytes;
                return Some(InputPermit {
                    budget: Arc::clone(self),
                    bytes,
                });
            }
            // Cancellation is shared with the surrounding reader. It may be
            // set without owning this budget, so bound the notification delay.
            used = self
                .changed
                .wait_timeout(used, Duration::from_millis(25))
                .unwrap()
                .0;
        }
    }
}

impl Drop for InputPermit {
    fn drop(&mut self) {
        let mut used = self.budget.used.lock().unwrap();
        *used -= self.bytes;
        self.budget.changed.notify_all();
    }
}

impl InputPermit {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, thread};

    #[test]
    fn credit_survives_shared_consumers_and_cancellation_wakes_waiter() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let budget = InputBudget::new(16, Arc::clone(&cancelled));
        assert!(budget.acquire(17).is_none());
        let first = Arc::new(budget.acquire(16).unwrap());
        let consumer = Arc::clone(&first);
        drop(first);
        assert_eq!(*budget.used.lock().unwrap(), 16);
        let b = Arc::clone(&budget);
        let (tx, rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            tx.send(b.acquire(1).is_some()).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(consumer);
        assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap());
        worker.join().unwrap();
        assert_eq!(*budget.used.lock().unwrap(), 0);
        let _held = budget.acquire(16).unwrap();
        let b = Arc::clone(&budget);
        let worker = thread::spawn(move || b.acquire(1).is_none());
        cancelled.store(true, Ordering::Release);
        assert!(worker.join().unwrap());
    }
}
