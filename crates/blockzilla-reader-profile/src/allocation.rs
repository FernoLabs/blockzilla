//! Count Rust allocation requests in a separate run, without a shared counter
//! update on every allocation. This does not count malloc calls inside C zstd.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
};

static ENABLED: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
struct Counts(Cell<(u64, u64)>);
impl Counts {
    fn flush(&self) {
        let (calls, bytes) = self.0.replace((0, 0));
        CALLS.fetch_add(calls, Relaxed);
        BYTES.fetch_add(bytes, Relaxed);
    }
}
impl Drop for Counts {
    fn drop(&mut self) {
        self.flush();
    }
}
thread_local! {
    static LOCAL: Counts = const { Counts(Cell::new((0, 0))) };
}
fn record(bytes: usize) {
    if ENABLED.load(Relaxed) {
        let _ = LOCAL.try_with(|counts| {
            let (calls, total) = counts.0.get();
            counts.0.set((calls + 1, total + bytes as u64));
        });
    }
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
    LOCAL.with(Counts::flush);
    CALLS.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    ENABLED.store(true, Relaxed);
}
pub fn stop() -> (u64, u64) {
    // The scan has joined its workers; their thread-local counts were flushed.
    ENABLED.store(false, Relaxed);
    LOCAL.with(Counts::flush);
    (CALLS.load(Relaxed), BYTES.load(Relaxed))
}
