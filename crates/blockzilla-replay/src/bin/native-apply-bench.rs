//! Microbenchmark: instruction-atomic native apply (clone overlay) vs
//! replay-only in-place apply (no overlay clone).
//!
//! Hypothesis: System/Stake/Config/BPF-loader previously cloned the whole
//! transaction overlay on every instruction for instruction-level atomicity.
//! Replay already discards the overlay on failure, so the clone is pure
//! overhead. This bench measures that cost (and tracks AccountMap clone cost
//! as overlay cardinality grows).
//!
//! ```text
//! cargo run --release -p blockzilla-replay --bin native-apply-bench
//! ```

use std::{
    alloc::{GlobalAlloc, Layout, System},
    env,
    hint::black_box,
    process,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant},
};

use blockzilla_format::ArchiveV2SystemInstructionData;
use blockzilla_replay::{
    AccountMap, AccountSnapshot, LaunchAccountMeta, SYSTEM_PROGRAM_ID,
    apply_launch_system_instruction_for_epoch,
    apply_launch_system_instruction_for_epoch_in_place, default_system_account,
};

const DEFAULT_ITERS: usize = 50_000;
const DEFAULT_WARMUPS: usize = 2;
const DEFAULT_ROUNDS: usize = 7;
const DEFAULT_OVERLAY_SIZES: &[usize] = &[2, 4, 8, 16, 32, 64];
const ACCOUNT_DATA_BYTES: usize = 128;

static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            record_allocation(new_size);
        }
        new_pointer
    }
}

fn record_allocation(bytes: usize) {
    if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
struct AllocSnap {
    calls: u64,
    bytes: u64,
}

impl AllocSnap {
    fn now() -> Self {
        Self {
            calls: ALLOCATION_CALLS.load(Ordering::Relaxed),
            bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        }
    }

    fn delta(self, end: Self) -> Self {
        Self {
            calls: end.calls.saturating_sub(self.calls),
            bytes: end.bytes.saturating_sub(self.bytes),
        }
    }
}

struct AllocGuard;

impl AllocGuard {
    fn start() -> Self {
        COUNT_ALLOCATIONS.store(true, Ordering::Relaxed);
        Self
    }
}

impl Drop for AllocGuard {
    fn drop(&mut self) {
        COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Clone,
    InPlace,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Clone => "clone",
            Self::InPlace => "in_place",
        }
    }
}

fn pubkey(index: u64) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[..8].copy_from_slice(&index.to_le_bytes());
    key
}

fn seeded_account(index: u64, lamports: u64, with_data: bool) -> AccountSnapshot {
    let mut account = default_system_account();
    account.lamports = lamports;
    if with_data {
        // Non-empty data forces Arc-backed AccountData clones to pay Arc costs
        // (still cheap vs full memcpy, but realistic for mixed overlays).
        let mut data = vec![0u8; ACCOUNT_DATA_BYTES];
        data[0] = (index & 0xff) as u8;
        data[1] = ((index >> 8) & 0xff) as u8;
        account.data = data.into();
    }
    account
}

/// Build an overlay shaped like a multi-account System transfer transaction:
/// from + to plus filler keys already present (prior instructions in the same tx).
fn build_overlay(size: usize) -> AccountMap {
    assert!(size >= 2, "transfer needs from+to");
    let mut overlay = AccountMap::with_capacity(size);
    for i in 0..size {
        let with_data = i >= 2;
        let lamports = 1_000_000 + (i as u64) * 1_000;
        overlay.insert(pubkey(i as u64), seeded_account(i as u64, lamports, with_data));
    }
    overlay
}

fn transfer_metas() -> [LaunchAccountMeta; 2] {
    [
        LaunchAccountMeta {
            pubkey: pubkey(0),
            is_signer: true,
            is_writable: true,
        },
        LaunchAccountMeta {
            pubkey: pubkey(1),
            is_signer: false,
            is_writable: true,
        },
    ]
}

fn restore_transfer_pair(overlay: &mut AccountMap) {
    // Keep filler accounts; reset from/to balances so each iter is identical.
    overlay.insert(pubkey(0), seeded_account(0, 1_000_000, false));
    overlay.insert(pubkey(1), seeded_account(1, 1_001_000, false));
}

fn apply_once(
    mode: Mode,
    overlay: &mut AccountMap,
    instruction: &ArchiveV2SystemInstructionData,
    metas: &[LaunchAccountMeta],
) {
    match mode {
        Mode::Clone => {
            apply_launch_system_instruction_for_epoch(instruction, metas, overlay, 0)
                .expect("clone transfer");
        }
        Mode::InPlace => {
            apply_launch_system_instruction_for_epoch_in_place(instruction, metas, overlay, 0)
                .expect("in-place transfer");
        }
    }
}

fn timed_round(mode: Mode, overlay_size: usize, iters: usize) -> Duration {
    let mut overlay = build_overlay(overlay_size);
    let metas = transfer_metas();
    let instruction = ArchiveV2SystemInstructionData::Transfer { lamports: 7 };
    // Warm the map layout once outside the timer.
    apply_once(mode, &mut overlay, &instruction, &metas);
    restore_transfer_pair(&mut overlay);

    let start = Instant::now();
    for _ in 0..iters {
        apply_once(mode, &mut overlay, &instruction, &metas);
        restore_transfer_pair(&mut overlay);
        black_box(&overlay);
    }
    start.elapsed()
}

fn alloc_round(mode: Mode, overlay_size: usize, iters: usize) -> AllocSnap {
    let mut overlay = build_overlay(overlay_size);
    let metas = transfer_metas();
    let instruction = ArchiveV2SystemInstructionData::Transfer { lamports: 7 };
    apply_once(mode, &mut overlay, &instruction, &metas);
    restore_transfer_pair(&mut overlay);

    let _guard = AllocGuard::start();
    let before = AllocSnap::now();
    for _ in 0..iters {
        apply_once(mode, &mut overlay, &instruction, &metas);
        restore_transfer_pair(&mut overlay);
        black_box(&overlay);
    }
    before.delta(AllocSnap::now())
}

fn median_duration(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn median_alloc(samples: &mut [AllocSnap]) -> AllocSnap {
    samples.sort_unstable_by_key(|s| s.calls);
    samples[samples.len() / 2]
}

fn ns_per_op(total: Duration, iters: usize) -> f64 {
    total.as_secs_f64() * 1e9 / iters as f64
}

fn parse_usize_list(raw: &str) -> Option<Vec<usize>> {
    raw.split(',')
        .map(|part| part.trim().parse::<usize>().ok())
        .collect()
}

fn main() {
    let mut iters = DEFAULT_ITERS;
    let mut warmups = DEFAULT_WARMUPS;
    let mut rounds = DEFAULT_ROUNDS;
    let mut sizes = DEFAULT_OVERLAY_SIZES.to_vec();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--iters" => {
                iters = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| {
                        eprintln!("--iters requires a positive integer");
                        process::exit(2);
                    });
            }
            "--warmups" => {
                warmups = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| {
                        eprintln!("--warmups requires a non-negative integer");
                        process::exit(2);
                    });
            }
            "--rounds" => {
                rounds = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| {
                        eprintln!("--rounds requires a positive integer");
                        process::exit(2);
                    });
            }
            "--overlay-sizes" => {
                let raw = args.next().unwrap_or_else(|| {
                    eprintln!("--overlay-sizes requires a comma-separated list");
                    process::exit(2);
                });
                sizes = parse_usize_list(&raw).unwrap_or_else(|| {
                    eprintln!("invalid --overlay-sizes: {raw}");
                    process::exit(2);
                });
            }
            "--help" | "-h" => {
                eprintln!(
                    "native-apply-bench [--iters N] [--warmups N] [--rounds N] [--overlay-sizes 2,8,32]"
                );
                process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                process::exit(2);
            }
        }
    }

    if iters == 0 || rounds == 0 || sizes.is_empty() || sizes.iter().any(|&s| s < 2) {
        eprintln!("iters/rounds must be > 0 and every overlay size must be >= 2");
        process::exit(2);
    }

    // Correctness: clone and in-place produce the same post-state for Transfer.
    {
        let mut a = build_overlay(8);
        let mut b = build_overlay(8);
        let metas = transfer_metas();
        let instruction = ArchiveV2SystemInstructionData::Transfer { lamports: 42 };
        apply_once(Mode::Clone, &mut a, &instruction, &metas);
        apply_once(Mode::InPlace, &mut b, &instruction, &metas);
        assert_eq!(a, b, "clone and in-place System Transfer diverge");
        assert_eq!(a[&pubkey(0)].lamports, 1_000_000 - 42);
        assert_eq!(a[&pubkey(1)].lamports, 1_001_000 + 42);
        assert_eq!(a[&pubkey(0)].owner, SYSTEM_PROGRAM_ID);
    }

    println!(
        "native-apply-bench system_transfer iters={iters} warmups={warmups} rounds={rounds} account_data_bytes={ACCOUNT_DATA_BYTES}"
    );
    println!(
        "{:<8} {:<10} {:>12} {:>12} {:>14} {:>14} {:>10}",
        "size", "mode", "ns/op_med", "ops/s_med", "allocs/op_med", "bytes/op_med", "speedup"
    );

    for &size in &sizes {
        for _ in 0..warmups {
            let _ = timed_round(Mode::Clone, size, iters.min(1_000));
            let _ = timed_round(Mode::InPlace, size, iters.min(1_000));
        }

        let mut clone_times = Vec::with_capacity(rounds);
        let mut inplace_times = Vec::with_capacity(rounds);
        let mut clone_allocs = Vec::with_capacity(rounds);
        let mut inplace_allocs = Vec::with_capacity(rounds);

        for _ in 0..rounds {
            clone_times.push(timed_round(Mode::Clone, size, iters));
            inplace_times.push(timed_round(Mode::InPlace, size, iters));
        }
        // Fewer iters for allocation sampling — atomics are expensive.
        let sample_iters = iters.min(5_000).max(500);
        for _ in 0..rounds {
            clone_allocs.push(alloc_round(Mode::Clone, size, sample_iters));
            inplace_allocs.push(alloc_round(Mode::InPlace, size, sample_iters));
        }

        let clone_med = median_duration(&mut clone_times);
        let inplace_med = median_duration(&mut inplace_times);
        let clone_ns = ns_per_op(clone_med, iters);
        let inplace_ns = ns_per_op(inplace_med, iters);
        let speedup = clone_ns / inplace_ns.max(1e-9);

        let clone_a = median_alloc(&mut clone_allocs);
        let inplace_a = median_alloc(&mut inplace_allocs);
        let clone_allocs_per_op = clone_a.calls as f64 / sample_iters as f64;
        let inplace_allocs_per_op = inplace_a.calls as f64 / sample_iters as f64;
        let clone_bytes_per_op = clone_a.bytes as f64 / sample_iters as f64;
        let inplace_bytes_per_op = inplace_a.bytes as f64 / sample_iters as f64;

        for (mode, ns, allocs_per, bytes_per, spd) in [
            (
                Mode::Clone,
                clone_ns,
                clone_allocs_per_op,
                clone_bytes_per_op,
                1.0,
            ),
            (
                Mode::InPlace,
                inplace_ns,
                inplace_allocs_per_op,
                inplace_bytes_per_op,
                speedup,
            ),
        ] {
            let ops = 1e9 / ns;
            println!(
                "{:<8} {:<10} {:>12.1} {:>12.0} {:>14.3} {:>14.1} {:>9.2}x",
                size,
                mode.name(),
                ns,
                ops,
                allocs_per,
                bytes_per,
                spd
            );
        }
    }
}
