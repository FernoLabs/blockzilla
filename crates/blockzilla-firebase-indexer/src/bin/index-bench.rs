//! Microbenchmark for the `build` hot path: open once, then repeatedly
//! stream-decode + index the same generation to get a stable throughput
//! number, allocation profile, and (optionally) a CPU flamegraph.
//!
//! ```text
//! cargo run --release -p blockzilla-firebase-indexer --bin index-bench -- \
//!   /path/to/epoch-dir --epoch 822 --iterations 200 --flamegraph flamegraph.svg
//! ```

use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Instant,
};

use anyhow::{Context, Result, anyhow, bail};
use blockzilla_firebase_indexer::{build, format::IndexBuilder};
use blockzilla_read_sdk::ArchiveV2WireProfile;
use clap::Parser;

static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);

struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the exact layout received from the caller is forwarded.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the exact layout received from the caller is forwarded.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` and `layout` came from this allocator.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the original allocation and requested size are forwarded.
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

#[derive(Debug, Parser)]
#[command(
    name = "index-bench",
    about = "Microbenchmark blockzilla-firebase-indexer's scan+index hot path"
)]
struct Args {
    /// Path to the epoch's Archive V2 Compact generation directory.
    archive: PathBuf,
    #[arg(long)]
    epoch: u64,
    /// Unreported warmup iterations (JIT/cache warmup, allocator warmup).
    #[arg(long, default_value_t = 2)]
    warmups: usize,
    /// Reported, measured iterations. The whole generation is re-scanned
    /// into a fresh IndexBuilder each iteration.
    #[arg(long, default_value_t = 200)]
    iterations: usize,
    /// Write a CPU flamegraph (SVG) covering the measured iterations.
    #[arg(long)]
    flamegraph: Option<PathBuf>,
    #[arg(long, default_value_t = 1000)]
    profile_frequency_hz: i32,
    #[arg(long)]
    trust_local: bool,
    #[arg(long, requires = "trust_local", default_value = "mainnet-beta")]
    cluster_id: String,
    #[arg(long, requires = "trust_local", default_value = "index-bench")]
    generation_id: String,
    #[arg(long, requires = "trust_local")]
    wire_profile: Option<ArchiveV2WireProfile>,
    #[arg(long, requires = "trust_local", default_value_t = 432_000)]
    slots_per_epoch: u64,
    /// Override the registry size IndexBuilder is sized for, to measure the
    /// upfront-allocation cost at a realistic full-epoch scale even against
    /// a small local fixture (whose real registry may be tiny). Defaults to
    /// the archive's actual registry_entries().
    #[arg(long)]
    registry_entries: Option<u32>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.iterations == 0 {
        bail!("--iterations must be greater than zero");
    }

    let trust_local = if args.trust_local {
        Some(build::TrustLocal {
            cluster_id: args.cluster_id.clone(),
            generation_id: args.generation_id.clone(),
            slots_per_epoch: args.slots_per_epoch,
            wire_profile: args
                .wire_profile
                .context("--wire-profile is required with --trust-local")?,
        })
    } else {
        None
    };
    let archive = build::open_archive(&args.archive, args.epoch, trust_local)
        .with_context(|| format!("open {}", args.archive.display()))?;
    let total_blocks = archive.index().rows.len();
    let registry_entries = args
        .registry_entries
        .unwrap_or_else(|| archive.registry_entries());
    eprintln!(
        "opened {} (epoch {}, {total_blocks} blocks, registry_entries={registry_entries}{})",
        args.archive.display(),
        args.epoch,
        if args.registry_entries.is_some() {
            " [overridden]"
        } else {
            ""
        },
    );

    for round in 0..args.warmups {
        let mut builder = IndexBuilder::new(1, registry_entries, registry_entries);
        build::scan_into_builder(&archive, &mut builder, false)?;
        eprintln!(
            "warmup {}/{}: wallets={}",
            round + 1,
            args.warmups,
            builder.wallet_count()
        );
    }

    let guard = args
        .flamegraph
        .is_some()
        .then(|| start_profiler(args.profile_frequency_hz))
        .transpose()?;

    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.store(true, Ordering::Relaxed);

    let mut transactions_total: u64 = 0;
    let mut blocks_total: u64 = 0;
    let mut wallets_last = 0usize;
    let mut programs_last = 0usize;
    let start = Instant::now();
    for _ in 0..args.iterations {
        let mut builder = IndexBuilder::new(1, registry_entries, registry_entries);
        let stats = build::scan_into_builder(&archive, &mut builder, false)?;
        transactions_total += stats.transactions_scanned;
        blocks_total += stats.blocks_scanned;
        wallets_last = builder.wallet_count();
        programs_last = builder.distinct_program_count();
    }
    let elapsed = start.elapsed();

    COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    let allocation_calls = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let allocated_bytes = ALLOCATED_BYTES.load(Ordering::Relaxed);

    if let Some(guard) = guard {
        let path = args
            .flamegraph
            .as_ref()
            .expect("flamegraph path set when guard exists");
        write_flamegraph_outputs(guard, path)?;
    }

    let seconds = elapsed.as_secs_f64();
    let tx_per_sec = transactions_total as f64 / seconds;
    let iterations = args.iterations as u64;
    println!("iterations={iterations} elapsed={seconds:.3}s");
    println!(
        "transactions_total={transactions_total} blocks_total={blocks_total} tx_per_sec={tx_per_sec:.0}"
    );
    println!(
        "per_iteration: transactions={} blocks={} wallets={wallets_last} programs={programs_last} time={:.6}s",
        transactions_total / iterations,
        blocks_total / iterations,
        seconds / args.iterations as f64,
    );
    println!(
        "allocations_total={allocation_calls} ({:.2} per iteration, {:.2} per transaction) bytes_total={allocated_bytes} ({:.1} bytes/tx)",
        allocation_calls as f64 / iterations as f64,
        allocation_calls as f64 / transactions_total as f64,
        allocated_bytes as f64 / transactions_total as f64,
    );

    Ok(())
}

fn start_profiler(frequency: i32) -> Result<pprof::ProfilerGuard<'static>> {
    if frequency <= 0 {
        bail!("--profile-frequency-hz must be positive");
    }
    let mut builder = pprof::ProfilerGuardBuilder::default().frequency(frequency);
    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    {
        builder = builder.blocklist(&["libc", "libgcc", "pthread", "vdso"]);
    }
    builder
        .build()
        .map_err(|err| anyhow!("start pprof profiler: {err}"))
}

fn write_flamegraph_outputs(guard: pprof::ProfilerGuard<'static>, path: &Path) -> Result<()> {
    let report = guard
        .report()
        .build()
        .map_err(|err| anyhow!("build pprof report: {err}"))?;
    if report.data.is_empty() {
        bail!("CPU profile contains no accepted samples");
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let flamegraph = File::create(path).with_context(|| format!("create {}", path.display()))?;
    report
        .flamegraph(flamegraph)
        .map_err(|err| anyhow!("write flamegraph {}: {err}", path.display()))?;

    let mut top_path = path.to_path_buf();
    top_path.set_extension("top.tsv");
    write_pprof_top_tsv(&top_path, &report)?;
    eprintln!(
        "cpu profile: flamegraph={} top={}",
        path.display(),
        top_path.display()
    );
    Ok(())
}

fn write_pprof_top_tsv(path: &Path, report: &pprof::Report) -> Result<()> {
    let mut leaves = BTreeMap::<String, isize>::new();
    let mut total_samples = 0isize;
    for (frames, count) in &report.data {
        total_samples += *count;
        let leaf = frames
            .frames
            .first()
            .and_then(|symbols| symbols.first())
            .map(|symbol| symbol.name())
            .unwrap_or_else(|| "unknown".to_string());
        *leaves.entry(leaf).or_default() += *count;
    }
    let mut values = leaves.into_iter().collect::<Vec<_>>();
    values.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
    writeln!(writer, "rank\tleaf_samples\tpercent\tfunction")?;
    for (rank, (name, samples)) in values.into_iter().take(80).enumerate() {
        let percent = if total_samples > 0 {
            samples as f64 * 100.0 / total_samples as f64
        } else {
            0.0
        };
        writeln!(
            writer,
            "{}\t{}\t{percent:.2}\t{}",
            rank + 1,
            samples,
            name.replace(['\t', '\n'], " ")
        )?;
    }
    Ok(())
}
