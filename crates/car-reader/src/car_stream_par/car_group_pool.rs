use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};

use crate::car_block_group::CarBlockGroup;

/// Pool of reusable `CarBlockGroup`s implemented as a bounded channel.
/// No mutex, no condvar, no lock contention.
pub struct CarBlockGroupPool {
    tx: Sender<CarBlockGroup>,
    rx: Receiver<CarBlockGroup>,
}

impl CarBlockGroupPool {
    /// Create a pool with `pool_size` preallocated groups.
    pub fn with_capacity(pool_size: usize) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(pool_size);

        for _ in 0..pool_size {
            tx.send(CarBlockGroup::new()).unwrap();
        }

        Self { tx, rx }
    }

    /// Checkout a group from the pool (blocks until available).
    #[inline]
    pub fn checkout(self: &Arc<Self>) -> PooledGroup {
        let mut g = self
            .rx
            .recv()
            .expect("CarBlockGroupPool: channel closed");

        // Reset logical contents but keep allocations.
        g.clear();
        // If you want:
        // g.block_range = (0, 0);

        PooledGroup {
            pool: Arc::clone(self),
            group: Some(g),
        }
    }

    #[inline]
    fn put_back(&self, g: CarBlockGroup) {
        // Do NOT clear here; clear on checkout only.
        // If the pool is dropped, ignore the error.
        let _ = self.tx.send(g);
    }
}

/// RAII wrapper returned by `checkout()`.
/// Automatically returns the group to the pool on drop.
pub struct PooledGroup {
    pool: Arc<CarBlockGroupPool>,
    group: Option<CarBlockGroup>,
}

impl PooledGroup {
    #[inline]
    pub fn as_ref(&self) -> &CarBlockGroup {
        self.group.as_ref().unwrap()
    }

    #[inline]
    pub fn as_mut(&mut self) -> &mut CarBlockGroup {
        self.group.as_mut().unwrap()
    }

    #[inline]
    pub fn into_inner(mut self) -> CarBlockGroup {
        self.group.take().unwrap()
    }
}

impl Drop for PooledGroup {
    fn drop(&mut self) {
        if let Some(g) = self.group.take() {
            self.pool.put_back(g);
        }
    }
}
