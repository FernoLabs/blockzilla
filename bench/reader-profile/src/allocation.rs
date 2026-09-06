//! Count Rust allocation requests immediately in a separate diagnostic run.
//! Allocation-mode timings include shared counter overhead and are not benchmarks.
//! This does not count malloc calls inside C zstd.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
};

pub const BUCKET_UPPER_BOUNDS: [Option<u64>; 6] = [
    Some(64),
    Some(256),
    Some(1024),
    Some(4096),
    Some(65536),
    None,
];
const BUCKET_COUNT: usize = BUCKET_UPPER_BOUNDS.len();
static COUNTERS: Counters = Counters::new();

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BucketCounts {
    pub allocation_calls: u64,
    pub allocation_bytes: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct Snapshot {
    pub allocation_calls: u64,
    pub allocation_bytes: u64,
    pub size_buckets: [BucketCounts; BUCKET_COUNT],
}

struct Counters {
    enabled: AtomicBool,
    calls: [AtomicU64; BUCKET_COUNT],
    bytes: [AtomicU64; BUCKET_COUNT],
}
impl Counters {
    const fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            calls: [const { AtomicU64::new(0) }; BUCKET_COUNT],
            bytes: [const { AtomicU64::new(0) }; BUCKET_COUNT],
        }
    }

    fn record(&self, bytes: usize) {
        if !self.enabled.load(Relaxed) {
            return;
        }
        let bucket = match bytes {
            0..=64 => 0,
            65..=256 => 1,
            257..=1024 => 2,
            1025..=4096 => 3,
            4097..=65536 => 4,
            _ => 5,
        };
        // Rayon pool destruction does not join its OS threads. Publish counts
        // in the allocator callback instead of waiting for thread-local Drop.
        // Atomic addition wraps without allocating or panicking on overflow.
        self.calls[bucket].fetch_add(1, Relaxed);
        self.bytes[bucket].fetch_add(bytes as u64, Relaxed);
    }

    fn start(&self) {
        self.enabled.store(false, Relaxed);
        for i in 0..BUCKET_COUNT {
            self.calls[i].store(0, Relaxed);
            self.bytes[i].store(0, Relaxed);
        }
        self.enabled.store(true, Relaxed);
    }

    fn stop(&self) -> Snapshot {
        // Scan jobs have finished their allocation callbacks. Worker threads
        // may still be alive, but no deferred counters remain on those threads.
        self.enabled.store(false, Relaxed);
        let size_buckets = std::array::from_fn(|i| BucketCounts {
            allocation_calls: self.calls[i].load(Relaxed),
            allocation_bytes: self.bytes[i].load(Relaxed),
        });
        Snapshot {
            allocation_calls: size_buckets.iter().map(|b| b.allocation_calls).sum(),
            allocation_bytes: size_buckets.iter().map(|b| b.allocation_bytes).sum(),
            size_buckets,
        }
    }
}
fn record(bytes: usize) {
    COUNTERS.record(bytes);
}
pub struct Allocator;
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the caller's exact layout to the system allocator.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size());
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the caller's exact layout to the system allocator.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size());
        }
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: forward the allocation, original layout, and requested size.
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        if !pointer.is_null() {
            record(size);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forward the original allocation and its layout.
        unsafe { System.dealloc(pointer, layout) }
    }
}
pub fn start() {
    COUNTERS.start();
}
pub fn stop() -> Snapshot {
    COUNTERS.stop()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requested_sizes_use_disjoint_inclusive_buckets_and_start_resets_them() {
        // Use an isolated counter set rather than the process allocator: test harness
        // allocations and parallel tests must not affect the expected values.
        let counts = Counters::new();
        counts.start();
        let sizes = [1, 64, 65, 256, 257, 1024, 1025, 4096, 4097, 65536, 65537];
        for size in sizes {
            counts.record(size);
        }
        let snapshot = counts.stop();
        let buckets = snapshot.size_buckets;
        assert_eq!(
            buckets.map(|b| (b.allocation_calls, b.allocation_bytes)),
            [
                (2, 65),
                (2, 321),
                (2, 1281),
                (2, 5121),
                (2, 69633),
                (1, 65537)
            ]
        );
        assert_eq!(buckets.iter().map(|b| b.allocation_calls).sum::<u64>(), 11);
        assert_eq!(
            buckets.iter().map(|b| b.allocation_bytes).sum::<u64>(),
            sizes.iter().map(|&n| n as u64).sum::<u64>()
        );
        assert_eq!(snapshot.allocation_calls, 11);
        assert_eq!(
            snapshot.allocation_bytes,
            sizes.iter().map(|&n| n as u64).sum::<u64>()
        );
        // A subsequent measurement must contain only its own requests.
        counts.record(4096); // Disabled requests are excluded.
        counts.start();
        counts.record(256);
        let next = counts.stop().size_buckets;
        assert_eq!(next[1].allocation_calls, 1);
        assert_eq!(next[1].allocation_bytes, 256);
        assert_eq!(next.iter().map(|b| b.allocation_calls).sum::<u64>(), 1);
    }

    #[test]
    fn records_from_a_live_thread_are_visible_before_it_exits() {
        let counts = Counters::new();
        let recorded = std::sync::Barrier::new(2);
        let release = std::sync::Barrier::new(2);
        counts.start();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                counts.record(65);
                counts.record(4097);
                recorded.wait();
                release.wait();
            });
            recorded.wait();
            let snapshot = counts.stop();
            let worker_is_alive = !worker.is_finished();
            release.wait();
            assert!(worker_is_alive);
            assert_eq!(snapshot.allocation_calls, 2);
            assert_eq!(snapshot.allocation_bytes, 4162);
            assert_eq!(snapshot.size_buckets[1].allocation_calls, 1);
            assert_eq!(snapshot.size_buckets[4].allocation_calls, 1);
        });
    }

    #[test]
    fn a_thread_that_outlives_a_window_cannot_flush_old_counts_into_the_next() {
        let counts = Counters::new();
        let recorded = std::sync::Barrier::new(2);
        let release = std::sync::Barrier::new(2);
        counts.start();
        let first = std::thread::scope(|scope| {
            scope.spawn(|| {
                counts.record(64);
                recorded.wait();
                release.wait();
            });
            recorded.wait();
            let first = counts.stop();
            counts.record(4096); // No recording between measurement windows.
            counts.start();
            counts.record(1024);
            release.wait();
            first
        });
        assert_eq!(first.allocation_bytes, 64);
        let next = counts.stop();
        assert_eq!(next.allocation_calls, 1);
        assert_eq!(next.allocation_bytes, 1024);
        assert_eq!(next.size_buckets[0], BucketCounts::default());
        assert_eq!(next.size_buckets[2].allocation_calls, 1);
    }
}
