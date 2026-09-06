//! Isolated Blockzilla Compact Archive V2 decode benchmark.
//!
//! This binary never opens a CAR file. It measures the public Compact V2
//! boundaries that the replay path actually uses:
//!
//! 1. control-plane/index admission (`ArchiveReader::open_with_options`);
//! 2. coalesced reads of selected compressed frames from `blocks.zstd`;
//! 3. borrowed hot-block decode (compressed read + zstd + outer schema);
//! 4. the public replay visitor (transaction/message decode, owned replay
//!    materialization, program histogram, and a minimal visitor fingerprint).
//!
//! Zstd and outer hot-block schema decode are private parts of one SDK call, so
//! their combined CPU time is estimated as borrowed-stream wall time minus the
//! time spent in its measured `RangeSource` reads. Likewise, the additional
//! replay-projection cost is an across-run estimate, not a nested timer. The
//! labels printed below make those boundaries explicit.
//!
//! Example:
//!
//! ```text
//! cargo run --release -p blockzilla-replay --bin compact-decode-bench -- \
//!   /path/to/compact/epoch-75 --start-row 0 --rows 20000 \
//!   --warmups 1 --rounds 5 --prefetch-mib 64
//! ```

use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, ensure};
use blockzilla_archive_v2::ArchiveV2HotBlockIndexRow;
use blockzilla_read_sdk::{
    ArchiveReader, HashVerification, LocalRangeSource, OpenOptions, RangeSource, SourceResult,
    manifest::BLOCKS_FILE,
};
use blockzilla_replay::{
    CompactVisitConfig, CompactVisitControl, CompactVisitEvent, CompactVisitSummary,
    visit_compact_generation,
};
use clap::Parser;
use sha2::{Digest, Sha256};

const DEFAULT_ROWS: usize = 10_000;
const DEFAULT_WARMUPS: usize = 1;
const DEFAULT_ROUNDS: usize = 5;
const DEFAULT_PREFETCH_MIB: usize = 64;
const MIB: usize = 1024 * 1024;
const SEMANTIC_FINGERPRINT_DOMAIN: &[u8] = b"blockzilla/compact-decode-bench/semantic/v1\0";

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
    name = "compact-decode-bench",
    about = "Benchmark Blockzilla Compact Archive V2 decode boundaries (never CAR)"
)]
struct Args {
    /// Root of one sealed Blockzilla Compact Archive V2 generation.
    generation: PathBuf,

    /// Zero-based hot-index row at which the bounded sample starts.
    #[arg(long, default_value_t = 0)]
    start_row: usize,

    /// Maximum number of present block rows to benchmark.
    #[arg(long, default_value_t = DEFAULT_ROWS)]
    rows: usize,

    /// Unreported cache/JIT warmup passes for every phase.
    #[arg(long, default_value_t = DEFAULT_WARMUPS)]
    warmups: usize,

    /// Reported samples per phase; the median is the primary result.
    #[arg(long, default_value_t = DEFAULT_ROUNDS)]
    rounds: usize,

    /// Maximum coalesced compressed-block read size, in MiB.
    #[arg(long, default_value_t = DEFAULT_PREFETCH_MIB)]
    prefetch_mib: usize,
}

#[derive(Debug, Clone)]
struct BenchConfig {
    generation: PathBuf,
    start_row: usize,
    rows: usize,
    warmups: usize,
    rounds: usize,
    prefetch_bytes: usize,
}

impl BenchConfig {
    fn from_args(args: Args) -> Result<Self> {
        ensure!(args.rows > 0, "--rows must be greater than zero");
        ensure!(args.rounds > 0, "--rounds must be greater than zero");
        ensure!(
            args.prefetch_mib > 0,
            "--prefetch-mib must be greater than zero"
        );
        let prefetch_bytes = args
            .prefetch_mib
            .checked_mul(MIB)
            .ok_or_else(|| anyhow!("--prefetch-mib overflows usize"))?;
        Ok(Self {
            generation: args.generation,
            start_row: args.start_row,
            rows: args.rows,
            warmups: args.warmups,
            rounds: args.rounds,
            prefetch_bytes,
        })
    }

    fn open_options(&self) -> OpenOptions {
        OpenOptions {
            hash_verification: HashVerification::ControlFiles,
            prefetch_bytes: self.prefetch_bytes,
            ..OpenOptions::default()
        }
    }
}

#[derive(Debug, Clone)]
struct Selection {
    start_row: usize,
    end_row: usize,
    start_slot: u64,
    rows: Vec<ArchiveV2HotBlockIndexRow>,
    batches: Vec<CompressedBatch>,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    transactions: u64,
}

impl Selection {
    fn from_index(
        index_rows: &[ArchiveV2HotBlockIndexRow],
        start_row: usize,
        requested_rows: usize,
        prefetch_bytes: usize,
    ) -> Result<Self> {
        ensure!(
            start_row < index_rows.len(),
            "--start-row {start_row} is outside the non-empty index (rows={})",
            index_rows.len()
        );
        let end_row = start_row
            .saturating_add(requested_rows)
            .min(index_rows.len());
        let rows = index_rows[start_row..end_row].to_vec();
        ensure!(!rows.is_empty(), "bounded selection contains no rows");
        let batches = compressed_batches(&rows, prefetch_bytes)?;
        let compressed_bytes = checked_sum(rows.iter().map(|row| u64::from(row.compressed_len)))?;
        let uncompressed_bytes =
            checked_sum(rows.iter().map(|row| u64::from(row.uncompressed_len)))?;
        let transactions = checked_sum(rows.iter().map(|row| u64::from(row.tx_count)))?;
        Ok(Self {
            start_row,
            end_row,
            start_slot: rows[0].slot,
            rows,
            batches,
            compressed_bytes,
            uncompressed_bytes,
            transactions,
        })
    }

    fn block_count(&self) -> u64 {
        self.rows.len() as u64
    }

    fn row_range(&self) -> std::ops::Range<usize> {
        self.start_row..self.end_row
    }

    fn visit_config(&self) -> CompactVisitConfig {
        CompactVisitConfig {
            start_slot: Some(self.start_slot),
            end_slot_exclusive: None,
            max_slots: Some(self.rows.len()),
        }
    }
}

fn checked_sum(values: impl IntoIterator<Item = u64>) -> Result<u64> {
    values.into_iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| anyhow!("benchmark counter overflow"))
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompressedBatch {
    offset: u64,
    length: usize,
}

fn compressed_batches(
    rows: &[ArchiveV2HotBlockIndexRow],
    prefetch_bytes: usize,
) -> Result<Vec<CompressedBatch>> {
    ensure!(prefetch_bytes > 0, "prefetch size must be positive");
    let mut batches = Vec::new();
    let mut position = 0usize;
    while position < rows.len() {
        let first = rows[position];
        let mut length = usize::try_from(first.compressed_len)
            .context("compressed frame length does not fit usize")?;
        let mut expected_next_offset = first
            .compressed_offset
            .checked_add(u64::from(first.compressed_len))
            .ok_or_else(|| anyhow!("compressed frame range overflow"))?;
        position += 1;
        while position < rows.len() {
            let row = rows[position];
            ensure!(
                row.compressed_offset == expected_next_offset,
                "selected compressed frames are not contiguous at slot {}: expected offset {}, found {}",
                row.slot,
                expected_next_offset,
                row.compressed_offset
            );
            let frame_len = usize::try_from(row.compressed_len)
                .context("compressed frame length does not fit usize")?;
            let Some(combined) = length.checked_add(frame_len) else {
                break;
            };
            if combined > prefetch_bytes {
                break;
            }
            length = combined;
            expected_next_offset = expected_next_offset
                .checked_add(u64::from(row.compressed_len))
                .ok_or_else(|| anyhow!("compressed frame range overflow"))?;
            position += 1;
        }
        batches.push(CompressedBatch {
            offset: first.compressed_offset,
            length,
        });
    }
    Ok(batches)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct AllocationStats {
    calls: u64,
    bytes: u64,
}

fn start_allocation_counting() {
    assert!(
        !COUNT_ALLOCATIONS.swap(false, Ordering::Relaxed),
        "allocation counting must not be nested"
    );
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.store(true, Ordering::Relaxed);
}

fn finish_allocation_counting() -> AllocationStats {
    COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    AllocationStats {
        calls: ALLOCATION_CALLS.load(Ordering::Relaxed),
        bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
    }
}

#[derive(Debug, Default)]
struct IoCounters {
    calls: AtomicU64,
    bytes: AtomicU64,
    nanoseconds: AtomicU64,
}

impl IoCounters {
    fn reset(&self) {
        self.calls.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
        self.nanoseconds.store(0, Ordering::Relaxed);
    }

    fn record(&self, length: usize, elapsed: Duration) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(length as u64, Ordering::Relaxed);
        self.nanoseconds.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> IoSnapshot {
        IoSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            elapsed: Duration::from_nanos(self.nanoseconds.load(Ordering::Relaxed)),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct IoSnapshot {
    calls: u64,
    bytes: u64,
    elapsed: Duration,
}

#[derive(Debug, Clone)]
struct MeteredLocalSource {
    inner: LocalRangeSource,
    block_reads: Arc<IoCounters>,
}

impl MeteredLocalSource {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalRangeSource::new(root),
            block_reads: Arc::new(IoCounters::default()),
        }
    }

    fn reset_block_reads(&self) {
        self.block_reads.reset();
    }

    fn block_reads(&self) -> IoSnapshot {
        self.block_reads.snapshot()
    }

    fn measure_block_read<T>(
        &self,
        object: &str,
        length: usize,
        operation: impl FnOnce() -> SourceResult<T>,
    ) -> SourceResult<T> {
        if object != BLOCKS_FILE {
            return operation();
        }
        let started = Instant::now();
        let result = operation();
        if result.is_ok() {
            self.block_reads.record(length, started.elapsed());
        }
        result
    }
}

impl RangeSource for MeteredLocalSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        self.inner.size(object)
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        self.measure_block_read(object, length, || {
            self.inner.read_range(object, offset, length)
        })
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        self.measure_block_read(object, length, || {
            self.inner
                .read_range_into(object, offset, length, destination)
        })
    }
}

#[derive(Debug)]
struct SemanticFingerprint(Sha256);

impl SemanticFingerprint {
    fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(SEMANTIC_FINGERPRINT_DOMAIN);
        Self(hasher)
    }

    #[allow(clippy::too_many_arguments)]
    fn block(
        &mut self,
        row_number: u64,
        block_id: u32,
        slot: u64,
        parent_slot: u64,
        block_time: Option<i64>,
        block_height: Option<u64>,
        blockhash_id: u32,
        previous_blockhash_id: u32,
        transaction_count: u32,
    ) {
        self.0.update(row_number.to_le_bytes());
        self.0.update(block_id.to_le_bytes());
        self.0.update(slot.to_le_bytes());
        self.0.update(parent_slot.to_le_bytes());
        update_optional_i64(&mut self.0, block_time);
        update_optional_u64(&mut self.0, block_height);
        self.0.update(blockhash_id.to_le_bytes());
        self.0.update(previous_blockhash_id.to_le_bytes());
        self.0.update(transaction_count.to_le_bytes());
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

fn update_optional_i64(hasher: &mut Sha256, value: Option<i64>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.to_le_bytes());
        }
        None => hasher.update([0]),
    }
}

fn update_optional_u64(hasher: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.to_le_bytes());
        }
        None => hasher.update([0]),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenFingerprint {
    epoch: u64,
    generation_id: String,
    index_rows: usize,
    registry_entries: u32,
}

#[derive(Debug)]
struct OpenRun {
    elapsed: Duration,
    fingerprint: OpenFingerprint,
    allocations: AllocationStats,
}

fn run_open(config: &BenchConfig, count_allocations: bool) -> Result<OpenRun> {
    if count_allocations {
        start_allocation_counting();
    }
    let started = Instant::now();
    let result = ArchiveReader::open_with_options(
        LocalRangeSource::new(&config.generation),
        config.open_options(),
    );
    let elapsed = started.elapsed();
    let allocations = if count_allocations {
        finish_allocation_counting()
    } else {
        AllocationStats::default()
    };
    let archive = result.with_context(|| format!("open {}", config.generation.display()))?;
    Ok(OpenRun {
        elapsed,
        fingerprint: OpenFingerprint {
            epoch: archive.manifest().epoch,
            generation_id: archive.manifest().generation_id.clone(),
            index_rows: archive.index().rows.len(),
            registry_entries: archive.registry_entries(),
        },
        allocations,
    })
}

#[derive(Debug)]
struct RawReadRun {
    elapsed: Duration,
    guard: u64,
    allocations: AllocationStats,
}

fn run_raw_read(
    config: &BenchConfig,
    selection: &Selection,
    count_allocations: bool,
) -> Result<RawReadRun> {
    let source = LocalRangeSource::new(&config.generation);
    if count_allocations {
        start_allocation_counting();
    }
    let started = Instant::now();
    let mut buffer = Vec::new();
    let mut guard = 0xcbf2_9ce4_8422_2325_u64;
    let result = (|| -> Result<()> {
        for batch in &selection.batches {
            source
                .read_range_into(BLOCKS_FILE, batch.offset, batch.length, &mut buffer)
                .with_context(|| {
                    format!(
                        "read compressed batch offset={} length={}",
                        batch.offset, batch.length
                    )
                })?;
            ensure!(
                buffer.len() == batch.length,
                "compressed read returned {} bytes, expected {}",
                buffer.len(),
                batch.length
            );
            guard = fold_buffer_guard(guard, &buffer);
            black_box(buffer.as_ptr());
        }
        Ok(())
    })();
    let elapsed = started.elapsed();
    let allocations = if count_allocations {
        finish_allocation_counting()
    } else {
        AllocationStats::default()
    };
    result?;
    Ok(RawReadRun {
        elapsed,
        guard: black_box(guard),
        allocations,
    })
}

fn fold_buffer_guard(mut guard: u64, bytes: &[u8]) -> u64 {
    guard ^= bytes.len() as u64;
    guard = guard.wrapping_mul(0x100_0000_01b3);
    if let Some(&byte) = bytes.first() {
        guard ^= u64::from(byte);
        guard = guard.wrapping_mul(0x100_0000_01b3);
    }
    if let Some(&byte) = bytes.get(bytes.len() / 2) {
        guard ^= u64::from(byte);
        guard = guard.wrapping_mul(0x100_0000_01b3);
    }
    if let Some(&byte) = bytes.last() {
        guard ^= u64::from(byte);
        guard = guard.wrapping_mul(0x100_0000_01b3);
    }
    guard
}

fn compressed_sha256(config: &BenchConfig, selection: &Selection) -> Result<[u8; 32]> {
    let source = LocalRangeSource::new(&config.generation);
    let mut buffer = Vec::new();
    let mut hasher = Sha256::new();
    for batch in &selection.batches {
        source
            .read_range_into(BLOCKS_FILE, batch.offset, batch.length, &mut buffer)
            .with_context(|| {
                format!(
                    "hash compressed batch offset={} length={}",
                    batch.offset, batch.length
                )
            })?;
        hasher.update(&buffer);
    }
    Ok(hasher.finalize().into())
}

#[derive(Debug)]
struct WireRun {
    elapsed: Duration,
    io: IoSnapshot,
    transactions: u64,
    owned_fallback_blocks: u64,
    fingerprint: [u8; 32],
    allocations: AllocationStats,
}

fn run_wire_decode(
    config: &BenchConfig,
    selection: &Selection,
    count_allocations: bool,
) -> Result<WireRun> {
    let source = MeteredLocalSource::new(config.generation.clone());
    let archive = ArchiveReader::open_with_options(source.clone(), config.open_options())
        .with_context(|| format!("open {} for wire decode", config.generation.display()))?;
    source.reset_block_reads();
    if count_allocations {
        start_allocation_counting();
    }
    let started = Instant::now();
    let result = (|| -> Result<(u64, u64, [u8; 32])> {
        let mut stream = archive
            .borrowed_blocks_without_rewards_range(selection.row_range())
            .context("create bounded borrowed block stream")?;
        let mut transactions = 0_u64;
        let mut owned_fallback_blocks = 0_u64;
        let mut fingerprint = SemanticFingerprint::new();
        for (relative_row, expected_row) in selection.rows.iter().enumerate() {
            let block = stream
                .next_block()
                .ok_or_else(|| anyhow!("borrowed block stream ended early"))??;
            ensure!(
                block.index_row.slot == expected_row.slot,
                "wire stream row mismatch: expected slot {}, found {}",
                expected_row.slot,
                block.index_row.slot
            );
            let tx_rows = u64::try_from(block.tx_rows_len())
                .context("wire transaction count does not fit u64")?;
            transactions = transactions
                .checked_add(tx_rows)
                .ok_or_else(|| anyhow!("wire transaction counter overflow"))?;
            owned_fallback_blocks += u64::from(block.uses_owned_fallback());
            let header = block.header();
            fingerprint.block(
                (selection.start_row + relative_row) as u64,
                block.index_row.block_id,
                header.slot,
                header.parent_slot,
                header.block_time,
                header.block_height,
                header.blockhash_id,
                header.previous_blockhash_id,
                block.tx_count(),
            );
            black_box(block.message_bytes().as_ptr());
        }
        ensure!(
            stream.next_block().is_none(),
            "borrowed stream exceeded bound"
        );
        Ok((transactions, owned_fallback_blocks, fingerprint.finish()))
    })();
    let elapsed = started.elapsed();
    let allocations = if count_allocations {
        finish_allocation_counting()
    } else {
        AllocationStats::default()
    };
    let io = source.block_reads();
    let (transactions, owned_fallback_blocks, fingerprint) = result?;
    ensure!(
        io.bytes == selection.compressed_bytes,
        "borrowed stream read {} compressed bytes, expected {}",
        io.bytes,
        selection.compressed_bytes
    );
    Ok(WireRun {
        elapsed,
        io,
        transactions,
        owned_fallback_blocks,
        fingerprint,
        allocations,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FullVisitFingerprint {
    semantic: [u8; 32],
    summary: CompactVisitSummary,
}

#[derive(Debug)]
struct FullVisitRun {
    total_elapsed: Duration,
    stream_elapsed: Duration,
    fingerprint: FullVisitFingerprint,
    allocations: AllocationStats,
}

fn run_full_visit(
    config: &BenchConfig,
    selection: &Selection,
    count_allocations: bool,
) -> Result<FullVisitRun> {
    let mut semantic = Some(SemanticFingerprint::new());
    let mut stream_started = None;
    let mut allocation_counting_started = false;
    let total_started = Instant::now();
    let result = visit_compact_generation(&config.generation, selection.visit_config(), |event| {
        match event {
            CompactVisitEvent::Generation(_) => {
                if count_allocations {
                    start_allocation_counting();
                    allocation_counting_started = true;
                }
                stream_started = Some(Instant::now());
            }
            CompactVisitEvent::Slot {
                row_number, slot, ..
            } => {
                semantic
                    .as_mut()
                    .expect("semantic fingerprint is finalized after visit")
                    .block(
                        row_number,
                        slot.block_id,
                        slot.slot,
                        slot.parent_slot,
                        slot.block_time,
                        slot.block_height,
                        slot.blockhash_id,
                        slot.previous_blockhash_id,
                        slot.transaction_count,
                    );
                black_box(slot.transactions.as_ptr());
            }
        }
        Ok(CompactVisitControl::Continue)
    });
    let total_elapsed = total_started.elapsed();
    let stream_elapsed = stream_started
        .map(|started| started.elapsed())
        .unwrap_or_default();
    let allocations = if allocation_counting_started {
        finish_allocation_counting()
    } else {
        AllocationStats::default()
    };
    let summary = result.with_context(|| {
        format!(
            "visit Compact generation {} from slot {} for {} rows",
            config.generation.display(),
            selection.start_slot,
            selection.rows.len()
        )
    })?;
    let program_instructions = checked_sum(summary.program_instruction_counts.values().copied())?;
    ensure!(
        program_instructions == summary.instructions_visited,
        "program histogram sums to {program_instructions}, summary reports {} instructions",
        summary.instructions_visited
    );
    Ok(FullVisitRun {
        total_elapsed,
        stream_elapsed,
        fingerprint: FullVisitFingerprint {
            semantic: semantic
                .take()
                .expect("semantic fingerprint exists")
                .finish(),
            summary,
        },
        allocations,
    })
}

#[derive(Debug, Clone, Copy)]
struct DurationStats {
    median: Duration,
    min: Duration,
    max: Duration,
}

fn duration_stats(mut samples: Vec<Duration>) -> Result<DurationStats> {
    ensure!(!samples.is_empty(), "cannot summarize zero samples");
    samples.sort_unstable();
    Ok(DurationStats {
        median: samples[samples.len() / 2],
        min: samples[0],
        max: samples[samples.len() - 1],
    })
}

fn ensure_same<T: PartialEq + std::fmt::Debug>(
    expected: &mut Option<T>,
    found: T,
    label: &str,
) -> Result<()> {
    match expected {
        Some(expected) => ensure!(
            *expected == found,
            "{label} changed between runs: expected {expected:?}, found {found:?}"
        ),
        None => *expected = Some(found),
    }
    Ok(())
}

fn warm_up(config: &BenchConfig, selection: &Selection) -> Result<()> {
    for round in 0..config.warmups {
        black_box(run_open(config, false)?);
        black_box(run_raw_read(config, selection, false)?);
        black_box(run_wire_decode(config, selection, false)?);
        black_box(run_full_visit(config, selection, false)?);
        eprintln!("warmup_complete round={}", round + 1);
    }
    Ok(())
}

fn print_phase(label: &str, stats: DurationStats, allocations: AllocationStats, operations: u64) {
    let seconds = stats.median.as_secs_f64();
    let operations_per_second = if seconds == 0.0 {
        f64::INFINITY
    } else {
        operations as f64 / seconds
    };
    println!(
        "phase={label} median_ms={:.3} min_ms={:.3} max_ms={:.3} operations={} operations_per_s={:.3} allocation_calls={} allocated_bytes={}",
        seconds * 1_000.0,
        stats.min.as_secs_f64() * 1_000.0,
        stats.max.as_secs_f64() * 1_000.0,
        operations,
        operations_per_second,
        allocations.calls,
        allocations.bytes,
    );
}

fn print_throughput(
    label: &str,
    elapsed: Duration,
    blocks: u64,
    transactions: u64,
    instructions: Option<u64>,
    compressed_bytes: u64,
) {
    let seconds = elapsed.as_secs_f64();
    let rate = |count: u64| {
        if seconds == 0.0 {
            f64::INFINITY
        } else {
            count as f64 / seconds
        }
    };
    let compressed_gb_per_s = if seconds == 0.0 {
        f64::INFINITY
    } else {
        compressed_bytes as f64 / 1_000_000_000.0 / seconds
    };
    let instructions_per_s = instructions.map(rate);
    println!(
        "throughput={label} seconds={seconds:.6} blocks_per_s={:.3} transactions_per_s={:.3} instructions_per_s={} compressed_gb_per_s={compressed_gb_per_s:.9}",
        rate(blocks),
        rate(transactions),
        instructions_per_s
            .map(|value| format!("{value:.3}"))
            .unwrap_or_else(|| "n/a".to_owned()),
    );
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn main() -> Result<()> {
    let config = BenchConfig::from_args(Args::parse())?;
    let admitted = ArchiveReader::open_with_options(
        LocalRangeSource::new(&config.generation),
        config.open_options(),
    )
    .with_context(|| format!("admit {}", config.generation.display()))?;
    let selection = Selection::from_index(
        &admitted.index().rows,
        config.start_row,
        config.rows,
        config.prefetch_bytes,
    )?;
    println!(
        "benchmark=compact-decode-v1 format=blockzilla-compact-archive-v2 car=false generation={} cluster={} epoch={} generation_id={} start_row={} end_row_exclusive={} first_slot={} last_slot={} blocks={} transactions={} compressed_bytes={} uncompressed_bytes={} compression_ratio={:.3} wire_prefetch_bytes={} full_public_visit_prefetch_bytes={} batches={} warmups={} rounds={} os_cache=uncontrolled",
        config.generation.display(),
        admitted.manifest().cluster_id,
        admitted.manifest().epoch,
        admitted.manifest().generation_id,
        selection.start_row,
        selection.end_row,
        selection.start_slot,
        selection.rows.last().expect("selection is non-empty").slot,
        selection.block_count(),
        selection.transactions,
        selection.compressed_bytes,
        selection.uncompressed_bytes,
        selection.uncompressed_bytes as f64 / selection.compressed_bytes.max(1) as f64,
        config.prefetch_bytes,
        DEFAULT_PREFETCH_MIB * MIB,
        selection.batches.len(),
        config.warmups,
        config.rounds,
    );
    drop(admitted);

    warm_up(&config, &selection)?;

    let mut open_samples = Vec::with_capacity(config.rounds);
    let mut open_fingerprint = None;
    for _ in 0..config.rounds {
        let run = run_open(&config, false)?;
        ensure_same(&mut open_fingerprint, run.fingerprint, "open fingerprint")?;
        open_samples.push(run.elapsed);
    }
    let open_allocations = run_open(&config, true)?;
    ensure_same(
        &mut open_fingerprint,
        open_allocations.fingerprint,
        "open allocation fingerprint",
    )?;
    let open_stats = duration_stats(open_samples)?;
    print_phase("control-open", open_stats, open_allocations.allocations, 1);

    let mut raw_samples = Vec::with_capacity(config.rounds);
    let mut raw_guard = None;
    for _ in 0..config.rounds {
        let run = run_raw_read(&config, &selection, false)?;
        ensure_same(&mut raw_guard, run.guard, "compressed read guard")?;
        raw_samples.push(run.elapsed);
    }
    let raw_allocations = run_raw_read(&config, &selection, true)?;
    ensure_same(
        &mut raw_guard,
        raw_allocations.guard,
        "compressed allocation read guard",
    )?;
    let raw_stats = duration_stats(raw_samples)?;
    print_phase(
        "compressed-read-only",
        raw_stats,
        raw_allocations.allocations,
        selection.compressed_bytes,
    );
    print_throughput(
        "compressed-read-only",
        raw_stats.median,
        selection.block_count(),
        selection.transactions,
        None,
        selection.compressed_bytes,
    );

    let mut wire_samples = Vec::with_capacity(config.rounds);
    let mut wire_io_samples = Vec::with_capacity(config.rounds);
    let mut wire_fingerprint = None;
    let mut wire_transactions = None;
    let mut wire_owned_fallback_blocks = None;
    let mut wire_io_shape = None;
    for _ in 0..config.rounds {
        let run = run_wire_decode(&config, &selection, false)?;
        ensure_same(
            &mut wire_fingerprint,
            run.fingerprint,
            "wire semantic fingerprint",
        )?;
        ensure_same(
            &mut wire_transactions,
            run.transactions,
            "wire transaction count",
        )?;
        ensure_same(
            &mut wire_owned_fallback_blocks,
            run.owned_fallback_blocks,
            "wire fallback count",
        )?;
        ensure_same(
            &mut wire_io_shape,
            (run.io.calls, run.io.bytes),
            "wire I/O shape",
        )?;
        wire_samples.push(run.elapsed);
        wire_io_samples.push(run.io.elapsed);
    }
    let wire_allocations = run_wire_decode(&config, &selection, true)?;
    ensure_same(
        &mut wire_fingerprint,
        wire_allocations.fingerprint,
        "wire allocation semantic fingerprint",
    )?;
    let wire_stats = duration_stats(wire_samples)?;
    let wire_io_stats = duration_stats(wire_io_samples)?;
    let wire_cpu_estimate = wire_stats.median.saturating_sub(wire_io_stats.median);
    print_phase(
        "borrowed-wire-read+zstd+outer-schema",
        wire_stats,
        wire_allocations.allocations,
        selection.block_count(),
    );
    print_phase(
        "borrowed-wire-compressed-read-nested",
        wire_io_stats,
        AllocationStats::default(),
        selection.compressed_bytes,
    );
    println!(
        "phase=borrowed-wire-zstd+outer-schema-estimate median_ms={:.3} derivation=wire_wall_minus_nested_block_read exact_boundary=false",
        wire_cpu_estimate.as_secs_f64() * 1_000.0,
    );
    print_throughput(
        "borrowed-wire",
        wire_stats.median,
        selection.block_count(),
        wire_transactions.expect("wire run exists"),
        None,
        selection.compressed_bytes,
    );

    let mut full_total_samples = Vec::with_capacity(config.rounds);
    let mut full_stream_samples = Vec::with_capacity(config.rounds);
    let mut full_fingerprint = None;
    for _ in 0..config.rounds {
        let run = run_full_visit(&config, &selection, false)?;
        ensure_same(
            &mut full_fingerprint,
            run.fingerprint,
            "full visitor fingerprint",
        )?;
        full_total_samples.push(run.total_elapsed);
        full_stream_samples.push(run.stream_elapsed);
    }
    let full_allocations = run_full_visit(&config, &selection, true)?;
    ensure_same(
        &mut full_fingerprint,
        full_allocations.fingerprint,
        "full allocation visitor fingerprint",
    )?;
    let full_total_stats = duration_stats(full_total_samples)?;
    let full_stream_stats = duration_stats(full_stream_samples)?;
    print_phase(
        "full-public-visit-including-open",
        full_total_stats,
        AllocationStats::default(),
        selection.block_count(),
    );
    print_phase(
        "full-public-visit-stream",
        full_stream_stats,
        full_allocations.allocations,
        selection.block_count(),
    );
    if config.prefetch_bytes == DEFAULT_PREFETCH_MIB * MIB {
        let materialization_estimate = full_stream_stats.median.saturating_sub(wire_stats.median);
        println!(
            "phase=transaction-schema+materialization+program-histogram+minimal-visitor-estimate median_ms={:.3} derivation=full_visit_stream_median_minus_wire_median exact_boundary=false",
            materialization_estimate.as_secs_f64() * 1_000.0,
        );
    } else {
        println!(
            "phase=transaction-schema+materialization+program-histogram+minimal-visitor-estimate median_ms=n/a reason=wire_and_public_visit_prefetch_differ exact_boundary=false"
        );
    }

    let full_fingerprint = full_fingerprint.expect("positive round count");
    let wire_fingerprint = wire_fingerprint.expect("positive round count");
    ensure!(
        wire_fingerprint == full_fingerprint.semantic,
        "wire/full semantic fingerprints differ: wire={} full={}",
        hex(&wire_fingerprint),
        hex(&full_fingerprint.semantic)
    );
    ensure!(
        full_fingerprint.summary.slots_visited == selection.block_count(),
        "full visitor reported {} blocks, expected {}",
        full_fingerprint.summary.slots_visited,
        selection.block_count()
    );
    ensure!(
        full_fingerprint.summary.transactions_visited == selection.transactions,
        "full visitor reported {} transactions, expected {}",
        full_fingerprint.summary.transactions_visited,
        selection.transactions
    );
    ensure!(
        full_fingerprint.summary.compressed_bytes_visited == selection.compressed_bytes,
        "full visitor reported {} compressed bytes, expected {}",
        full_fingerprint.summary.compressed_bytes_visited,
        selection.compressed_bytes
    );
    print_throughput(
        "full-public-visit-stream",
        full_stream_stats.median,
        full_fingerprint.summary.slots_visited,
        full_fingerprint.summary.transactions_visited,
        Some(full_fingerprint.summary.instructions_visited),
        full_fingerprint.summary.compressed_bytes_visited,
    );

    let compressed_digest = compressed_sha256(&config, &selection)?;
    println!(
        "correctness=PASS semantic_sha256={} compressed_selection_sha256={} blocks={} transactions={} instructions={} compressed_bytes={} program_ids={} wire_owned_fallback_blocks={} wire_read_calls={} note=semantic_fingerprint_covers_ordered_block_identity_and_counts",
        hex(&full_fingerprint.semantic),
        hex(&compressed_digest),
        full_fingerprint.summary.slots_visited,
        full_fingerprint.summary.transactions_visited,
        full_fingerprint.summary.instructions_visited,
        full_fingerprint.summary.compressed_bytes_visited,
        full_fingerprint.summary.program_instruction_counts.len(),
        wire_owned_fallback_blocks.expect("wire run exists"),
        wire_io_shape.expect("wire run exists").0,
    );
    println!(
        "boundary_note=public_sdk_does_not_expose_zstd_separately_from_outer_schema; estimates_are_saturating_differences_of_independent_medians; full_public_visit_collects_program_histogram; allocation_samples_are_separate_untimed_runs; os_page_cache_is_not_flushed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(slot: u64, offset: u64, compressed_len: u32) -> ArchiveV2HotBlockIndexRow {
        ArchiveV2HotBlockIndexRow {
            block_id: slot as u32,
            slot,
            compressed_offset: offset,
            compressed_len,
            uncompressed_len: compressed_len * 2,
            tx_count: slot as u32 + 1,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 0,
        }
    }

    #[test]
    fn compressed_batches_match_reader_prefetch_rule() {
        let rows = [row(10, 100, 40), row(11, 140, 60), row(12, 200, 70)];
        assert_eq!(
            compressed_batches(&rows, 100).unwrap(),
            vec![
                CompressedBatch {
                    offset: 100,
                    length: 100,
                },
                CompressedBatch {
                    offset: 200,
                    length: 70,
                },
            ]
        );
    }

    #[test]
    fn compressed_batches_reject_non_contiguous_rows() {
        let rows = [row(10, 100, 40), row(11, 141, 60)];
        let error = compressed_batches(&rows, 100).unwrap_err().to_string();
        assert!(error.contains("not contiguous"), "{error}");
    }

    #[test]
    fn semantic_fingerprint_is_stable_and_order_sensitive() {
        let mut first = SemanticFingerprint::new();
        first.block(3, 7, 42, 41, Some(99), Some(12), 8, 9, 11);
        let first = first.finish();

        let mut same = SemanticFingerprint::new();
        same.block(3, 7, 42, 41, Some(99), Some(12), 8, 9, 11);
        assert_eq!(first, same.finish());

        let mut different = SemanticFingerprint::new();
        different.block(4, 7, 42, 41, Some(99), Some(12), 8, 9, 11);
        assert_ne!(first, different.finish());
    }

    #[test]
    fn duration_summary_uses_sorted_middle_sample() {
        let stats = duration_stats(vec![
            Duration::from_millis(30),
            Duration::from_millis(10),
            Duration::from_millis(20),
        ])
        .unwrap();
        assert_eq!(stats.min, Duration::from_millis(10));
        assert_eq!(stats.median, Duration::from_millis(20));
        assert_eq!(stats.max, Duration::from_millis(30));
    }

    #[test]
    fn selection_is_bounded_by_available_rows() {
        let rows = [row(10, 100, 40), row(11, 140, 60), row(12, 200, 70)];
        let selection = Selection::from_index(&rows, 1, 99, 100).unwrap();
        assert_eq!(selection.start_row, 1);
        assert_eq!(selection.end_row, 3);
        assert_eq!(selection.start_slot, 11);
        assert_eq!(selection.block_count(), 2);
        assert_eq!(selection.compressed_bytes, 130);
        assert_eq!(selection.transactions, 25);
    }
}
