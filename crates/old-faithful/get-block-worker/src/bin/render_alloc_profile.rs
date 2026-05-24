use of_car_reader::{CarBlockReader, car_block_group::CarBlockGroup};
use of_get_block_worker::get_block::{
    GetBlockConfig, TransactionDetails, render_get_block_json_bytes_profiled,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{Cursor, Seek};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static CAR_BYTES: &[u8] =
    include_bytes!("../../../car-reader/benches/fixtures/epoch-822-biggest.car");

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static REALLOC_IN_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_OUT_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        REALLOC_IN_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        REALLOC_OUT_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Full,
    Accounts,
    Signatures,
    None,
}

impl Mode {
    fn all() -> [Self; 4] {
        [Self::Full, Self::Accounts, Self::Signatures, Self::None]
    }

    fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Accounts => "accounts",
            Self::Signatures => "signatures",
            Self::None => "none",
        }
    }

    fn transaction_details(self) -> TransactionDetails {
        match self {
            Self::Full => TransactionDetails::Full,
            Self::Accounts => TransactionDetails::Accounts,
            Self::Signatures => TransactionDetails::Signatures,
            Self::None => TransactionDetails::None,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Snapshot {
    elapsed_us: u128,
    alloc_calls: u64,
    alloc_bytes: u64,
    realloc_calls: u64,
    realloc_in_bytes: u64,
    realloc_out_bytes: u64,
    dealloc_calls: u64,
    dealloc_bytes: u64,
    output_bytes: usize,
    transactions: usize,
}

fn main() -> Result<(), String> {
    let iterations = std::env::args()
        .nth(1)
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|err| format!("invalid iteration count: {err}"))?
        .unwrap_or(20)
        .max(1);
    let previous_blockhash = Some([9u8; 32]);
    let block_bytes = fixture_block_bytes()?;

    println!(
        "mode\titerations\ttx\toutput_bytes\telapsed_p50_ms\telapsed_p90_ms\talloc_calls_p50\talloc_calls_p90\talloc_bytes_p50\trealloc_calls_p50\trealloc_in_bytes_p50\trealloc_out_bytes_p50\tdealloc_calls_p50\tdealloc_bytes_p50"
    );
    for mode in Mode::all() {
        let config = GetBlockConfig {
            rewards: false,
            transaction_details: mode.transaction_details(),
            ..Default::default()
        };
        let mut samples = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let input = block_bytes.clone();
            reset_alloc_counts();
            let started = Instant::now();
            let (bytes, profile) =
                render_get_block_json_bytes_profiled(input, previous_blockhash, config)?;
            let elapsed_us = started.elapsed().as_micros();
            std::hint::black_box(bytes.as_slice());
            let mut snapshot = snapshot_alloc_counts();
            snapshot.elapsed_us = elapsed_us;
            snapshot.output_bytes = bytes.len();
            snapshot.transactions = profile.transactions;
            samples.push(snapshot);
        }

        samples.sort_by_key(|sample| sample.elapsed_us);
        let p50 = percentile(&samples, 0.50);
        let p90 = percentile(&samples, 0.90);
        println!(
            "{}\t{}\t{}\t{}\t{:.3}\t{:.3}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            mode.label(),
            iterations,
            p50.transactions,
            p50.output_bytes,
            p50.elapsed_us as f64 / 1000.0,
            p90.elapsed_us as f64 / 1000.0,
            p50.alloc_calls,
            p90.alloc_calls,
            p50.alloc_bytes,
            p50.realloc_calls,
            p50.realloc_in_bytes,
            p50.realloc_out_bytes,
            p50.dealloc_calls,
            p50.dealloc_bytes,
        );
    }

    Ok(())
}

fn reset_alloc_counts() {
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    REALLOC_CALLS.store(0, Ordering::Relaxed);
    REALLOC_IN_BYTES.store(0, Ordering::Relaxed);
    REALLOC_OUT_BYTES.store(0, Ordering::Relaxed);
    DEALLOC_CALLS.store(0, Ordering::Relaxed);
    DEALLOC_BYTES.store(0, Ordering::Relaxed);
}

fn snapshot_alloc_counts() -> Snapshot {
    Snapshot {
        alloc_calls: ALLOC_CALLS.load(Ordering::Relaxed),
        alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        realloc_calls: REALLOC_CALLS.load(Ordering::Relaxed),
        realloc_in_bytes: REALLOC_IN_BYTES.load(Ordering::Relaxed),
        realloc_out_bytes: REALLOC_OUT_BYTES.load(Ordering::Relaxed),
        dealloc_calls: DEALLOC_CALLS.load(Ordering::Relaxed),
        dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
        ..Default::default()
    }
}

fn percentile(samples: &[Snapshot], percentile: f64) -> Snapshot {
    let index = ((samples.len() - 1) as f64 * percentile).round() as usize;
    samples[index]
}

fn fixture_block_bytes() -> Result<Vec<u8>, String> {
    let cursor = Cursor::new(CAR_BYTES);
    let mut car = CarBlockReader::with_capacity(cursor, 8 * 1024 * 1024);
    car.skip_header()
        .map_err(|err| format!("skip fixture CAR header: {err}"))?;
    let start = car
        .reader
        .stream_position()
        .map_err(|err| format!("read fixture start position: {err}"))?;
    let mut group = CarBlockGroup::without_rewards();
    let ok = car
        .read_until_block_into(&mut group)
        .map_err(|err| format!("read fixture block: {err}"))?;
    if !ok {
        return Err("fixture did not contain a block".to_string());
    }
    let end = car
        .reader
        .stream_position()
        .map_err(|err| format!("read fixture end position: {err}"))?;
    Ok(CAR_BYTES[start as usize..end as usize].to_vec())
}
