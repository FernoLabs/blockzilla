use anyhow::{Context, Result};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_SIGNATURES_FILE, WincodeArchiveV2PohRecord, WincodeLeb128FramedReader,
    WincodeLeb128FramedWriter, deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards,
    read_archive_v2_hot_block_index,
};
use memmap2::Mmap;
use rayon::prelude::*;
use serde::Serialize;
use sha2::{Digest, Sha256, block_api::compress256};
use tracing::warn;
use std::{
    cell::RefCell,
    fs::File,
    io::{BufReader, BufWriter, Read},
    path::Path,
    time::Instant,
};

const SIGNATURE_BYTES: usize = 64;
const POH_READER_BUFFER_BYTES: usize = 8 << 20;
const MAX_POH_FRAME_BYTES: usize = 64 << 20;

#[derive(Debug, Serialize)]
pub(crate) struct PohVerificationReport {
    archive: String,
    blocks_verified: u64,
    entries_verified: u64,
    transactions_consumed: u64,
    signatures_hashed: u64,
    hashes_recomputed: u128,
    external_blockhash_blocks: u64,
    /// Blocks verified from the PoH sidecar's own `signature_count` alone, without
    /// decompressing the hot block. Zero means every block fell back to decompression,
    /// e.g. an archive generation whose builder predates the `signature_count` field.
    fast_path_blocks: u64,
    compressed_bytes_read: u64,
    uncompressed_bytes_decoded: u64,
    elapsed_secs: f64,
    blocks_per_sec: f64,
    entries_per_sec: f64,
    hashes_per_sec: f64,
    compressed_mib_per_sec: f64,
    worker_threads: usize,
    peak_block_bytes: usize,
    peak_entries_per_block: usize,
    peak_signatures_per_entry: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct EntryJob<'a> {
    pub(crate) start_hash: [u8; 32],
    pub(crate) num_hashes: u64,
    pub(crate) transaction_count: u32,
    pub(crate) signatures: &'a [u8],
}

pub(crate) fn recompute_entry_hash_standalone(job: &EntryJob<'_>) -> [u8; 32] {
    recompute_entry_hash(job, &mut Vec::new())
}

#[derive(Clone, Copy)]
struct EntryJobRange {
    start_hash: [u8; 32],
    expected_hash: [u8; 32],
    num_hashes: u64,
    transaction_count: u32,
    signature_start: usize,
    signature_end: usize,
}

#[derive(Default)]
struct BlockWork {
    signature_prefixes: Vec<u32>,
    /// Per-entry (byte_start, byte_end) into the block's signature range, in entry order.
    /// Filled either directly from the PoH sidecar's `signature_count` (fast path) or, as a
    /// fallback, from the decompressed block's per-transaction signature counts (slow path).
    signature_ranges: Vec<(usize, usize)>,
    entry_jobs: Vec<EntryJobRange>,
}

thread_local! {
    /// Rayon workers keep this allocation across blocks. A per-`par_iter` `map_init` scratch
    /// would allocate and free a Merkle buffer for every block in a full epoch.
    static MERKLE_SCRATCH: RefCell<Vec<[u8; 32]>> = const { RefCell::new(Vec::new()) };
}

#[cfg(feature = "benchmark-tools")]
#[derive(Debug, Serialize)]
pub(crate) struct PohCoreBenchmarkReport {
    entries: usize,
    hashes_per_entry: u64,
    signatures_per_entry: usize,
    iterations: usize,
    worker_threads: usize,
    median_elapsed_secs: f64,
    median_hashes_per_sec: f64,
    best_hashes_per_sec: f64,
    worst_hashes_per_sec: f64,
    checksum: String,
}

#[cfg(feature = "benchmark-tools")]
pub(crate) fn bench_poh_core(
    entries: usize,
    hashes_per_entry: u64,
    signatures_per_entry: usize,
    iterations: usize,
    requested_threads: usize,
) -> Result<PohCoreBenchmarkReport> {
    anyhow::ensure!(entries > 0, "entries must be non-zero");
    anyhow::ensure!(iterations > 0, "iterations must be non-zero");
    anyhow::ensure!(hashes_per_entry > 0, "hashes-per-entry must be non-zero");
    let signature_bytes = signatures_per_entry
        .checked_mul(SIGNATURE_BYTES)
        .context("signature byte count overflow")?;
    let signatures = vec![0x5au8; signature_bytes];
    let worker_threads = if requested_threads == 0 {
        std::thread::available_parallelism().map_or(1, usize::from)
    } else {
        requested_threads
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_threads)
        .thread_name(|index| format!("bz-poh-bench-{index}"))
        .build()
        .context("build PoH benchmark thread pool")?;
    let jobs = (0..entries)
        .map(|index| {
            let mut start_hash = [0u8; 32];
            start_hash[..8].copy_from_slice(&(index as u64).to_le_bytes());
            EntryJob {
                start_hash,
                num_hashes: hashes_per_entry,
                transaction_count: u32::from(signatures_per_entry > 0),
                signatures: &signatures,
            }
        })
        .collect::<Vec<_>>();

    let run = || {
        pool.install(|| {
            jobs.par_iter()
                .map_init(Vec::<[u8; 32]>::new, |scratch, job| {
                    std::hint::black_box(recompute_entry_hash(job, scratch))
                })
                .reduce(
                    || [0; 32],
                    |mut left, right| {
                        for (left, right) in left.iter_mut().zip(right) {
                            *left ^= right;
                        }
                        left
                    },
                )
        })
    };
    std::hint::black_box(run());
    let total_hashes = (entries as u128)
        .checked_mul(u128::from(hashes_per_entry))
        .context("benchmark hash count overflow")?;
    let mut samples = Vec::with_capacity(iterations);
    let mut checksum = [0u8; 32];
    for _ in 0..iterations {
        let started = Instant::now();
        checksum = run();
        samples.push(started.elapsed().as_secs_f64());
    }
    samples.sort_by(f64::total_cmp);
    let median_elapsed_secs = samples[samples.len() / 2];
    let hashes_per_second = |elapsed: f64| total_hashes as f64 / elapsed.max(f64::EPSILON);
    Ok(PohCoreBenchmarkReport {
        entries,
        hashes_per_entry,
        signatures_per_entry,
        iterations,
        worker_threads,
        median_elapsed_secs,
        median_hashes_per_sec: hashes_per_second(median_elapsed_secs),
        best_hashes_per_sec: hashes_per_second(samples[0]),
        worst_hashes_per_sec: hashes_per_second(*samples.last().expect("nonempty checked")),
        checksum: hex32(&checksum),
    })
}

#[derive(Debug, Serialize)]
pub(crate) struct PohSignatureCountMigrationReport {
    archive: String,
    blocks_total: u64,
    /// Blocks whose PoH entries were decompressed and patched with real signature counts.
    blocks_patched: u64,
    /// Blocks left untouched because their signature_count already summed correctly (either a
    /// generation already migrated, or a genuinely empty block where 0 is the real count).
    blocks_already_current: u64,
    elapsed_secs: f64,
}

/// Backfills `signature_count` into an already-built archive's `poh.wincode`, without
/// recomputing any PoH hashes. For each block, only decompresses the hot block (to read
/// `tx_rows`) when the sidecar's current counts don't already sum correctly — see
/// `verify_archive_v2_poh`'s identical check. Writes to a temp file and publishes via
/// sync+rename, so a reader (or a crash mid-run) never observes a partially written sidecar.
///
/// Safe to run more than once: an already-migrated archive round-trips its records unchanged.
pub(crate) fn migrate_poh_signature_counts(
    archive_dir: &Path,
) -> Result<PohSignatureCountMigrationReport> {
    let index_path = archive_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let index = read_archive_v2_hot_block_index(&index_path)?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY == 0,
        "dictionary-compressed archives are not supported by this migration yet"
    );

    let block_file =
        File::open(archive_dir.join(ARCHIVE_V2_BLOCKS_FILE)).context("open compact block file")?;
    // SAFETY: the migration only reads this mapping; the publication contract for a committed
    // generation guarantees it isn't mutated concurrently.
    let block_map = unsafe { Mmap::map(&block_file) }.context("map compact block file")?;
    // Block index order is traversed strictly forward, so hint the kernel to evict pages
    // behind the read cursor instead of letting the whole (often multi-GB) mapping accumulate
    // as resident memory. Best-effort: a failed hint doesn't affect correctness.
    if let Err(error) = block_map.advise(memmap2::Advice::Sequential) {
        warn!("madvise(SEQUENTIAL) failed for compact block file: {error}");
    }
    anyhow::ensure!(
        block_map.len() as u64 == index.blob_file_bytes,
        "block file length differs from its index"
    );

    let poh_path = archive_dir.join(ARCHIVE_V2_POH_FILE);
    let poh_file = File::open(&poh_path).context("open compact PoH sidecar")?;
    let mut poh_reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(POH_READER_BUFFER_BYTES, poh_file));

    let tmp_path = archive_dir.join(format!("{ARCHIVE_V2_POH_FILE}.migrate.tmp"));
    if crate::file_nonempty(&tmp_path) {
        std::fs::remove_file(&tmp_path)
            .with_context(|| format!("remove stale {}", tmp_path.display()))?;
    }
    let tmp_file =
        File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
    let mut poh_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        POH_READER_BUFFER_BYTES,
        tmp_file,
    ));

    let started = Instant::now();
    let mut progress = crate::ProgressTracker::new("PoH Signature Count Migration");
    progress.set_estimated_total_blocks(index.rows.len() as u64);
    let mut decompressor = zstd::bulk::Decompressor::new().context("create zstd decompressor")?;
    let mut uncompressed = Vec::new();
    let mut tx_rows_scratch = Vec::new();
    let mut poh_write_scratch = Vec::new();
    let mut blocks_patched = 0u64;
    let mut blocks_already_current = 0u64;
    // Every frame in this sidecar shares one schema; probing per-frame would decode a legacy
    // (pre-`signature_count`) sidecar twice on every single block.
    let mut poh_schema = blockzilla_format::PohRecordSchema::default();

    let bench_phases = std::env::var("BENCH_PHASE_TIMING").is_ok();
    let mut t_poh_read = std::time::Duration::ZERO;
    let mut t_decompress = std::time::Duration::ZERO;
    let mut t_deserialize_block = std::time::Duration::ZERO;
    let mut t_tx_rows_collect = std::time::Duration::ZERO;
    let mut t_patch = std::time::Duration::ZERO;
    let mut t_poh_write = std::time::Duration::ZERO;

    for (position, row) in index.rows.iter().enumerate() {
        anyhow::ensure!(
            row.block_id as usize == position,
            "non-contiguous block id {} at index position {position}",
            row.block_id
        );
        let t0 = bench_phases.then(Instant::now);
        let (_, mut poh) = poh_reader
            .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| {
                blockzilla_format::deserialize_archive_v2_poh_record_with_schema(
                    bytes,
                    &mut poh_schema,
                )
                .map_err(anyhow::Error::from)
            })?
            .with_context(|| format!("PoH sidecar ended before block {}", row.block_id))?;
        if let Some(t0) = t0 {
            t_poh_read += t0.elapsed();
        }
        anyhow::ensure!(
            poh.block_id == row.block_id && poh.slot == row.slot,
            "PoH record does not match block {} slot {}",
            row.block_id,
            row.slot
        );

        let poh_signature_sum = poh.entries.iter().try_fold(0u32, |acc, entry| {
            acc.checked_add(entry.signature_count)
                .context("PoH entry signature_count overflow")
        })?;

        if poh_signature_sum == row.signature_count {
            blocks_already_current += 1;
        } else {
            let compressed_start = usize::try_from(row.compressed_offset)
                .context("compressed offset exceeds usize")?;
            let compressed_end = compressed_start
                .checked_add(row.compressed_len as usize)
                .context("compressed range overflows usize")?;
            let compressed = block_map
                .get(compressed_start..compressed_end)
                .with_context(|| format!("block {} points outside block file", row.block_id))?;
            let expected_len = row.uncompressed_len as usize;
            uncompressed.clear();
            uncompressed.reserve(expected_len);
            let t1 = bench_phases.then(Instant::now);
            let decoded = decompressor
                .decompress_to_buffer(compressed, &mut uncompressed)
                .with_context(|| format!("decompress block {} slot {}", row.block_id, row.slot))?;
            if let Some(t1) = t1 {
                t_decompress += t1.elapsed();
            }
            anyhow::ensure!(
                decoded == expected_len,
                "block {} decompressed to {decoded} bytes, expected {expected_len}",
                row.block_id
            );
            let t2 = bench_phases.then(Instant::now);
            let block = deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(
                &uncompressed,
            )
            .with_context(|| format!("decode block {} slot {}", row.block_id, row.slot))?;
            if let Some(t2) = t2 {
                t_deserialize_block += t2.elapsed();
            }
            anyhow::ensure!(
                block.header.slot == row.slot && block.tx_count == row.tx_count,
                "block {} header/index mismatch",
                row.block_id
            );

            let t3 = bench_phases.then(Instant::now);
            tx_rows_scratch.clear();
            tx_rows_scratch.extend(block.tx_rows());
            if let Some(t3) = t3 {
                t_tx_rows_collect += t3.elapsed();
            }
            let t4 = bench_phases.then(Instant::now);
            crate::archive_v2::patch_poh_entry_signature_counts(&mut poh.entries, &tx_rows_scratch)
                .with_context(|| format!("patch block {} signature counts", row.block_id))?;
            let patched_sum = poh
                .entries
                .iter()
                .map(|entry| u64::from(entry.signature_count))
                .sum::<u64>();
            if let Some(t4) = t4 {
                t_patch += t4.elapsed();
            }
            anyhow::ensure!(
                patched_sum == u64::from(row.signature_count),
                "block {} patched signature_count sum {patched_sum} still does not match index {}",
                row.block_id,
                row.signature_count
            );
            blocks_patched += 1;
        }

        let t5 = bench_phases.then(Instant::now);
        poh_writer
            .write_with_scratch(&poh, &mut poh_write_scratch)
            .with_context(|| format!("write migrated PoH record for block {}", row.block_id))?;
        if let Some(t5) = t5 {
            t_poh_write += t5.elapsed();
        }
        progress.update_slot(row.slot);
        progress.update(1, u64::from(row.tx_count));
    }

    if bench_phases {
        eprintln!(
            "PHASE_TIMING poh_read={:.3}s decompress={:.3}s deserialize_block={:.3}s tx_rows_collect={:.3}s patch={:.3}s poh_write={:.3}s total_measured={:.3}s wall_so_far={:.3}s",
            t_poh_read.as_secs_f64(),
            t_decompress.as_secs_f64(),
            t_deserialize_block.as_secs_f64(),
            t_tx_rows_collect.as_secs_f64(),
            t_patch.as_secs_f64(),
            t_poh_write.as_secs_f64(),
            (t_poh_read + t_decompress + t_deserialize_block + t_tx_rows_collect + t_patch + t_poh_write).as_secs_f64(),
            started.elapsed().as_secs_f64(),
        );
    }

    anyhow::ensure!(
        poh_reader
            .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| {
                blockzilla_format::deserialize_archive_v2_poh_record(bytes)
                    .map_err(anyhow::Error::from)
            })?
            .is_none(),
        "PoH sidecar has trailing records"
    );

    poh_writer.flush().context("flush migrated PoH sidecar")?;
    drop(poh_writer);
    File::open(&tmp_path)
        .with_context(|| format!("open {} for sync", tmp_path.display()))?
        .sync_all()
        .with_context(|| format!("sync {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, &poh_path).with_context(|| {
        format!(
            "rename {} to {}",
            tmp_path.display(),
            poh_path.display()
        )
    })?;
    crate::first_seen_finalization::sync_directory(archive_dir)?;
    progress.final_report();

    Ok(PohSignatureCountMigrationReport {
        archive: archive_dir.display().to_string(),
        blocks_total: index.rows.len() as u64,
        blocks_patched,
        blocks_already_current,
        elapsed_secs: started.elapsed().as_secs_f64(),
    })
}

/// Cheap, read-only correctness check for a completed `migrate_poh_signature_counts` run: sums
/// each block's `poh.wincode` entries and compares them against `archive-v2-blocks.index`'s
/// recorded `signature_count`, without decompressing or patching anything -- the same check the
/// migration performs internally (see the loop above), minus the decompression fallback, since a
/// correctly migrated sidecar's counts must already self-consistently sum.
///
/// A whole-sidecar read, so it is only cheap in the "once per completed job" sense, not the
/// "every poll" sense: the scheduler's `handle_child_exit` calls this once per migration job
/// exit, not once per status poll, to keep its cost from scaling with the size of the epoch
/// backlog.
pub(crate) fn poh_migration_epoch_verified(archive_dir: &Path) -> Result<bool> {
    let index_path = archive_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let index = read_archive_v2_hot_block_index(&index_path)?;

    let poh_path = archive_dir.join(ARCHIVE_V2_POH_FILE);
    let poh_file = File::open(&poh_path).context("open compact PoH sidecar")?;
    let mut poh_reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(POH_READER_BUFFER_BYTES, poh_file));
    // Every frame in this sidecar shares one schema; probing per-frame would decode a legacy
    // (pre-`signature_count`) sidecar twice on every single block.
    let mut poh_schema = blockzilla_format::PohRecordSchema::default();

    for (position, row) in index.rows.iter().enumerate() {
        if row.block_id as usize != position {
            return Ok(false);
        }
        let Some((_, poh)) = poh_reader.read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| {
            blockzilla_format::deserialize_archive_v2_poh_record_with_schema(bytes, &mut poh_schema)
                .map_err(anyhow::Error::from)
        })?
        else {
            return Ok(false);
        };
        let poh: WincodeArchiveV2PohRecord = poh;
        if poh.block_id != row.block_id || poh.slot != row.slot {
            return Ok(false);
        }
        let poh_signature_sum = poh.entries.iter().try_fold(0u32, |acc, entry| {
            acc.checked_add(entry.signature_count)
                .context("PoH entry signature_count overflow")
        })?;
        if poh_signature_sum != row.signature_count {
            return Ok(false);
        }
    }

    let trailing = poh_reader.read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| {
        blockzilla_format::deserialize_archive_v2_poh_record(bytes).map_err(anyhow::Error::from)
    })?;
    Ok(trailing.is_none())
}

pub(crate) fn verify_archive_v2_poh(
    archive_dir: &Path,
    requested_threads: usize,
    max_blocks: Option<u64>,
) -> Result<PohVerificationReport> {
    let index_path = archive_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let index = read_archive_v2_hot_block_index(&index_path)?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY == 0,
        "dictionary-compressed archives are not supported by this verifier yet"
    );

    let block_file =
        File::open(archive_dir.join(ARCHIVE_V2_BLOCKS_FILE)).context("open compact block file")?;
    let signature_file = File::open(archive_dir.join(ARCHIVE_V2_SIGNATURES_FILE))
        .context("open compact signature sidecar")?;
    // SAFETY: both mappings are immutable for the verifier's lifetime. Operators must verify a
    // committed generation; replacing files underneath a running verifier is outside the archive
    // publication contract and is also caught by length/index consistency checks.
    let block_map = unsafe { Mmap::map(&block_file) }.context("map compact block file")?;
    // SAFETY: see the immutable-generation argument above.
    let signature_map =
        unsafe { Mmap::map(&signature_file) }.context("map compact signature sidecar")?;
    // Both mappings are traversed strictly forward (block order, then non-decreasing signature
    // ordinals), so hint the kernel to evict pages behind the read cursor rather than letting
    // often multi-GB mappings accumulate as resident memory. Best-effort.
    if let Err(error) = block_map.advise(memmap2::Advice::Sequential) {
        warn!("madvise(SEQUENTIAL) failed for compact block file: {error}");
    }
    if let Err(error) = signature_map.advise(memmap2::Advice::Sequential) {
        warn!("madvise(SEQUENTIAL) failed for compact signature sidecar: {error}");
    }

    anyhow::ensure!(
        block_map.len() as u64 == index.blob_file_bytes,
        "block file length differs from its index"
    );

    let blockhashes = read_exact_file(archive_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))?;
    anyhow::ensure!(
        blockhashes.len()
            == index
                .rows
                .len()
                .checked_mul(32)
                .context("blockhash size overflow")?,
        "blockhash registry has {} bytes for {} indexed blocks",
        blockhashes.len(),
        index.rows.len()
    );
    let predecessor =
        read_predecessor_hash(&archive_dir.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE))?;

    let poh_file =
        File::open(archive_dir.join(ARCHIVE_V2_POH_FILE)).context("open compact PoH sidecar")?;
    let mut poh_reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(POH_READER_BUFFER_BYTES, poh_file));
    let worker_threads = if requested_threads == 0 {
        std::thread::available_parallelism().map_or(1, usize::from)
    } else {
        requested_threads
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_threads)
        .thread_name(|index| format!("bz-poh-{index}"))
        .build()
        .context("build PoH verification thread pool")?;

    let limit = max_blocks
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(usize::MAX)
        .min(index.rows.len());
    let started = Instant::now();
    let mut decompressor = zstd::bulk::Decompressor::new().context("create zstd decompressor")?;
    let mut uncompressed = Vec::new();
    let mut work = BlockWork::default();
    let mut blocks_verified = 0u64;
    let mut entries_verified = 0u64;
    let mut transactions_consumed = 0u64;
    let mut signatures_hashed = 0u64;
    let mut hashes_recomputed = 0u128;
    let mut external_blockhash_blocks = 0u64;
    let mut fast_path_blocks = 0u64;
    let mut compressed_bytes_read = 0u64;
    let mut uncompressed_bytes_decoded = 0u64;
    let mut peak_block_bytes = 0usize;
    let mut peak_entries_per_block = 0usize;
    let mut peak_signatures_per_entry = 0usize;
    // Every frame in this sidecar shares one schema; probing per-frame would decode a legacy
    // (pre-`signature_count`) sidecar twice on every single block.
    let mut poh_schema = blockzilla_format::PohRecordSchema::default();

    for (position, row) in index.rows.iter().take(limit).enumerate() {
        anyhow::ensure!(
            row.block_id as usize == position,
            "non-contiguous block id {} at index position {position}",
            row.block_id
        );

        let signature_start = usize::try_from(row.first_signature_ordinal)
            .context("signature ordinal exceeds usize")?
            .checked_mul(SIGNATURE_BYTES)
            .context("signature offset overflow")?;
        let signature_end = signature_start
            .checked_add(
                (row.signature_count as usize)
                    .checked_mul(SIGNATURE_BYTES)
                    .context("signature length overflow")?,
            )
            .context("signature range overflow")?;
        let block_signatures = signature_map
            .get(signature_start..signature_end)
            .with_context(|| format!("block {} signatures point outside sidecar", row.block_id))?;

        let (_, poh) = poh_reader
            .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| {
                blockzilla_format::deserialize_archive_v2_poh_record_with_schema(
                    bytes,
                    &mut poh_schema,
                )
                .map_err(anyhow::Error::from)
            })?
            .with_context(|| format!("PoH sidecar ended before block {}", row.block_id))?;
        let poh: WincodeArchiveV2PohRecord = poh;
        anyhow::ensure!(
            poh.block_id == row.block_id && poh.slot == row.slot,
            "PoH record does not match block {} slot {}",
            row.block_id,
            row.slot
        );

        // Prefer the PoH sidecar's own per-entry `signature_count` directly: it lets us skip
        // decompressing and decoding the hot block entirely. Only fall back to that decompression
        // path when the sidecar's counts don't self-consistently sum to the block index's total,
        // which flags an archive generation whose builder predates `signature_count` (left at its
        // `0` placeholder) rather than silently trusting unpopulated data.
        let poh_signature_sum = poh.entries.iter().try_fold(0u32, |acc, entry| {
            acc.checked_add(entry.signature_count)
                .context("PoH entry signature_count overflow")
        })?;
        work.signature_ranges.clear();
        work.signature_ranges.reserve(poh.entries.len());
        let mut decompressed_this_block = false;
        if poh_signature_sum == row.signature_count {
            let mut sig_cursor = 0usize;
            for entry in &poh.entries {
                let byte_start = sig_cursor
                    .checked_mul(SIGNATURE_BYTES)
                    .context("entry signature offset overflow")?;
                sig_cursor = sig_cursor
                    .checked_add(entry.signature_count as usize)
                    .context("signature cursor overflow")?;
                let byte_end = sig_cursor
                    .checked_mul(SIGNATURE_BYTES)
                    .context("entry signature offset overflow")?;
                work.signature_ranges.push((byte_start, byte_end));
            }
            fast_path_blocks += 1;
        } else {
            warn!(
                "block {} PoH sidecar signature_count sum ({poh_signature_sum}) does not match \
                 index signature_count ({}); falling back to decompression-based verification",
                row.block_id, row.signature_count
            );
            let compressed_start = usize::try_from(row.compressed_offset)
                .context("compressed offset exceeds usize")?;
            let compressed_end = compressed_start
                .checked_add(row.compressed_len as usize)
                .context("compressed range overflows usize")?;
            let compressed = block_map
                .get(compressed_start..compressed_end)
                .with_context(|| format!("block {} points outside block file", row.block_id))?;
            let expected_len = row.uncompressed_len as usize;
            uncompressed.clear();
            uncompressed.reserve(expected_len);
            let decoded = decompressor
                .decompress_to_buffer(compressed, &mut uncompressed)
                .with_context(|| format!("decompress block {} slot {}", row.block_id, row.slot))?;
            anyhow::ensure!(
                decoded == expected_len,
                "block {} decompressed to {decoded} bytes, expected {expected_len}",
                row.block_id
            );
            let block = deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(
                &uncompressed,
            )
            .with_context(|| format!("decode block {} slot {}", row.block_id, row.slot))?;
            anyhow::ensure!(
                block.header.slot == row.slot && block.tx_count == row.tx_count,
                "block {} header/index mismatch",
                row.block_id
            );

            work.signature_prefixes.clear();
            work.signature_prefixes.reserve(row.tx_count as usize + 1);
            work.signature_prefixes.push(0);
            let mut block_signature_count = 0u32;
            for tx in block.tx_rows() {
                block_signature_count = block_signature_count
                    .checked_add(u32::from(tx.signature_count))
                    .context("block signature count overflow")?;
                work.signature_prefixes.push(block_signature_count);
            }
            anyhow::ensure!(
                work.signature_prefixes.len() == row.tx_count as usize + 1
                    && block_signature_count == row.signature_count,
                "block {} transaction/signature accounting mismatch",
                row.block_id
            );

            let mut tx_cursor = 0usize;
            for entry in &poh.entries {
                let tx_end = tx_cursor
                    .checked_add(entry.tx_count as usize)
                    .context("entry transaction range overflow")?;
                anyhow::ensure!(
                    tx_end < work.signature_prefixes.len(),
                    "block {} entry consumes transactions beyond block",
                    row.block_id
                );
                let first_signature = work.signature_prefixes[tx_cursor] as usize;
                let last_signature = work.signature_prefixes[tx_end] as usize;
                let byte_start = first_signature
                    .checked_mul(SIGNATURE_BYTES)
                    .context("entry signature offset overflow")?;
                let byte_end = last_signature
                    .checked_mul(SIGNATURE_BYTES)
                    .context("entry signature offset overflow")?;
                work.signature_ranges.push((byte_start, byte_end));
                tx_cursor = tx_end;
            }
            anyhow::ensure!(
                tx_cursor == row.tx_count as usize,
                "block {} PoH entries consume {tx_cursor} of {} transactions",
                row.block_id,
                row.tx_count
            );
            decompressed_this_block = true;
        }

        let expected_blockhash = blockhash_at(&blockhashes, position)?;
        let block_start_hash = if position == 0 {
            predecessor
        } else {
            blockhash_at(&blockhashes, position - 1)?
        };
        if poh.entries.is_empty() {
            external_blockhash_blocks += 1;
        } else {
            work.entry_jobs.clear();
            work.entry_jobs.reserve(poh.entries.len());
            for (entry_index, entry) in poh.entries.iter().enumerate() {
                let (byte_start, byte_end) = work.signature_ranges[entry_index];
                let start_hash = if entry_index == 0 {
                    block_start_hash
                } else {
                    poh.entries[entry_index - 1].hash
                };
                work.entry_jobs.push(EntryJobRange {
                    start_hash,
                    expected_hash: entry.hash,
                    num_hashes: entry.num_hashes,
                    transaction_count: entry.tx_count,
                    signature_start: byte_start,
                    signature_end: byte_end,
                });
                peak_signatures_per_entry =
                    peak_signatures_per_entry.max((byte_end - byte_start) / SIGNATURE_BYTES);
            }
            let mismatch = pool.install(|| {
                work.entry_jobs
                    .par_iter()
                    .map(|job| {
                        let entry = EntryJob {
                            start_hash: job.start_hash,
                            num_hashes: job.num_hashes,
                            transaction_count: job.transaction_count,
                            signatures: &block_signatures[job.signature_start..job.signature_end],
                        };
                        let actual = recompute_entry_hash_reusing_scratch(&entry);
                        (actual != job.expected_hash).then_some(actual)
                    })
                    .enumerate()
                    .filter(|(_, mismatch)| mismatch.is_some())
                    .min_by_key(|(index, _)| *index)
            });
            if let Some((entry_index, Some(actual))) = mismatch {
                let job = &work.entry_jobs[entry_index];
                anyhow::bail!(
                    "PoH mismatch block={} slot={} entry={}/{} expected={} actual={} \
                     num_hashes={} tx_count={} signature_count={} start_hash={} prev_entry_hash={}",
                    row.block_id,
                    row.slot,
                    entry_index,
                    poh.entries.len(),
                    hex32(&poh.entries[entry_index].hash),
                    hex32(&actual),
                    job.num_hashes,
                    job.transaction_count,
                    (job.signature_end - job.signature_start) / SIGNATURE_BYTES,
                    hex32(&job.start_hash),
                    hex32(&if entry_index == 0 {
                        block_start_hash
                    } else {
                        poh.entries[entry_index - 1].hash
                    })
                );
            }
            let final_hash = poh.entries.last().expect("nonempty checked").hash;
            anyhow::ensure!(
                final_hash == expected_blockhash,
                "block {} slot {} final PoH hash differs from blockhash registry",
                row.block_id,
                row.slot
            );
            entries_verified += poh.entries.len() as u64;
            transactions_consumed += row.tx_count as u64;
            signatures_hashed += row.signature_count as u64;
            hashes_recomputed += poh
                .entries
                .iter()
                .map(|entry| u128::from(entry.num_hashes.max(u64::from(entry.tx_count > 0))))
                .sum::<u128>();
            peak_entries_per_block = peak_entries_per_block.max(poh.entries.len());
        }

        if decompressed_this_block {
            compressed_bytes_read += u64::from(row.compressed_len);
            uncompressed_bytes_decoded += u64::from(row.uncompressed_len);
            peak_block_bytes = peak_block_bytes.max(uncompressed.len());
        }
        blocks_verified += 1;
    }

    if limit == index.rows.len() {
        anyhow::ensure!(
            poh_reader.read::<WincodeArchiveV2PohRecord>()?.is_none(),
            "PoH sidecar has trailing records"
        );
    }
    let elapsed_secs = started.elapsed().as_secs_f64();
    let safe_elapsed = elapsed_secs.max(f64::EPSILON);
    Ok(PohVerificationReport {
        archive: archive_dir.display().to_string(),
        blocks_verified,
        entries_verified,
        transactions_consumed,
        signatures_hashed,
        hashes_recomputed,
        external_blockhash_blocks,
        fast_path_blocks,
        compressed_bytes_read,
        uncompressed_bytes_decoded,
        elapsed_secs,
        blocks_per_sec: blocks_verified as f64 / safe_elapsed,
        entries_per_sec: entries_verified as f64 / safe_elapsed,
        hashes_per_sec: hashes_recomputed as f64 / safe_elapsed,
        compressed_mib_per_sec: compressed_bytes_read as f64 / (1 << 20) as f64 / safe_elapsed,
        worker_threads,
        peak_block_bytes,
        peak_entries_per_block,
        peak_signatures_per_entry,
    })
}

#[inline]
fn recompute_entry_hash_reusing_scratch(job: &EntryJob<'_>) -> [u8; 32] {
    MERKLE_SCRATCH.with(|scratch| recompute_entry_hash(job, &mut scratch.borrow_mut()))
}

fn recompute_entry_hash(job: &EntryJob<'_>, merkle: &mut Vec<[u8; 32]>) -> [u8; 32] {
    let mut hash = job.start_hash;
    hash_chain(&mut hash, job.num_hashes.saturating_sub(1));
    if job.transaction_count == 0 {
        if job.num_hashes == 0 {
            job.start_hash
        } else {
            hash_one(&hash)
        }
    } else {
        let mixin = signature_merkle_root(job.signatures, merkle);
        hash_pair(&hash, &mixin)
    }
}

fn signature_merkle_root(signatures: &[u8], scratch: &mut Vec<[u8; 32]>) -> [u8; 32] {
    debug_assert_eq!(signatures.len() % SIGNATURE_BYTES, 0);
    scratch.clear();
    scratch.reserve(signatures.len() / SIGNATURE_BYTES);
    for signature in signatures.chunks_exact(SIGNATURE_BYTES) {
        scratch.push(hash_prefixed_65(0, signature, &[]));
    }
    if scratch.is_empty() {
        return [0; 32];
    }
    let mut len = scratch.len();
    while len > 1 {
        let next_len = len.div_ceil(2);
        for output in 0..next_len {
            let left = scratch[output * 2];
            // Bound against `len` (the current logical level length), not the scratch
            // Vec's physical length: the buffer is reused/overwritten in place across
            // levels rather than truncated, so `scratch.get` alone would spuriously
            // succeed against stale data left over from an earlier, longer level.
            let right = if output * 2 + 1 < len {
                scratch[output * 2 + 1]
            } else {
                left
            };
            scratch[output] = hash_prefixed_65(1, &left, &right);
        }
        len = next_len;
    }
    scratch[0]
}

/// Hashes a fixed 65-byte `[prefix][a][b]` message (`a.len() + b.len() == 64`) by driving
/// `compress256` over the two loop-invariant-shaped blocks directly, instead of
/// `Sha256::new()`/`update`/`finalize` per call. Used for every Merkle leaf (`prefix=0`, a
/// 64-byte signature) and internal node (`prefix=1`, a 32+32-byte hash pair) in
/// `signature_merkle_root`, which is `Sha256`-per-node hot: one call per transaction signature
/// per entry across a whole epoch. Padding tail (the `0x80` byte, zero run, and the fixed
/// 520-bit big-endian length) never changes, so only the leading 65 bytes are written per call.
#[inline]
fn hash_prefixed_65(prefix: u8, a: &[u8], b: &[u8]) -> [u8; 32] {
    debug_assert_eq!(a.len() + b.len(), 64);
    let mut buf = [0u8; 128];
    buf[0] = prefix;
    buf[1..1 + a.len()].copy_from_slice(a);
    buf[1 + a.len()..65].copy_from_slice(b);
    buf[65] = 0x80;
    // 520-bit (65-byte) message length, big-endian, at the last 8 bytes of block 1.
    buf[126] = 0x02;
    buf[127] = 0x08;
    let blocks: [[u8; 64]; 2] = [
        buf[..64].try_into().unwrap(),
        buf[64..].try_into().unwrap(),
    ];
    let mut state = SHA256_IV;
    compress256(&mut state, &blocks);
    let mut out = [0u8; 32];
    for (out_chunk, word) in out.chunks_exact_mut(4).zip(state) {
        out_chunk.copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[inline]
fn hash_one(value: &[u8; 32]) -> [u8; 32] {
    Sha256::digest(value).into()
}

/// SHA-256 IV, per FIPS 180-4 section 5.3.3.
const SHA256_IV: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

/// Runs `count` plain SHA-256 rounds over `hash` in place: the PoH tick chain, up to
/// `hashes_per_tick` (12,500 on mainnet) rounds per entry. Drives `compress256` directly
/// instead of calling `Sha256::digest` per round -- every round hashes a fixed 32-byte
/// message, so the padding is loop-invariant and only the leading 32 bytes change between
/// rounds, but `Sha256::digest` redoes the padding writes and IV load on every call.
/// `tick_hash_matches_repeated_sha256` below checks this against `hash_one` chained by hand.
fn hash_chain(hash: &mut [u8; 32], count: u64) {
    if count == 0 {
        return;
    }
    let mut block = [0u8; 64];
    block[32] = 0x80;
    block[62] = 0x01; // 256-bit message length, big-endian, at bytes 56..64
    for _ in 0..count {
        block[..32].copy_from_slice(hash);
        let mut state = SHA256_IV;
        compress256(&mut state, core::slice::from_ref(&block));
        for (out, word) in hash.chunks_exact_mut(4).zip(state) {
            out.copy_from_slice(&word.to_be_bytes());
        }
    }
}

#[inline]
fn hash_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

fn read_exact_file(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let path = path.as_ref();
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let len = usize::try_from(file.metadata()?.len()).context("file length exceeds usize")?;
    let mut bytes = Vec::with_capacity(len);
    file.read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    Ok(bytes)
}

fn read_predecessor_hash(path: &Path) -> Result<[u8; 32]> {
    let bytes = read_exact_file(path)?;
    anyhow::ensure!(
        !bytes.is_empty() && bytes.len() % 40 == 0,
        "{} is not a sequence of 40-byte predecessor rows",
        path.display()
    );
    Ok(bytes[bytes.len() - 40..bytes.len() - 8]
        .try_into()
        .expect("slice is 32 bytes"))
}

fn blockhash_at(bytes: &[u8], block_id: usize) -> Result<[u8; 32]> {
    let start = block_id
        .checked_mul(32)
        .context("blockhash offset overflow")?;
    let end = start.checked_add(32).context("blockhash range overflow")?;
    bytes
        .get(start..end)
        .context("blockhash id outside registry")?
        .try_into()
        .context("blockhash row is not 32 bytes")
}

fn hex32(value: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in value {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn poh_migration_verify_fixture_root(label: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "blockzilla-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn poh_migration_epoch_verified_detects_matching_and_mismatched_signature_sums() {
        use blockzilla_format::{
            ArchiveV2HotBlockIndexRow, CompactPohEntry, write_archive_v2_hot_block_index,
        };

        let root = poh_migration_verify_fixture_root("poh-migration-verify");

        let rows = vec![
            ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 100,
                compressed_offset: 0,
                compressed_len: 1,
                uncompressed_len: 1,
                tx_count: 1,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 1,
            },
            ArchiveV2HotBlockIndexRow {
                block_id: 1,
                slot: 101,
                compressed_offset: 1,
                compressed_len: 1,
                uncompressed_len: 1,
                tx_count: 0,
                first_tx_ordinal: 1,
                first_signature_ordinal: 1,
                signature_count: 0,
            },
        ];
        write_archive_v2_hot_block_index(&root.join(ARCHIVE_V2_BLOCK_INDEX_FILE), 2, 1, 0, &rows)
            .unwrap();

        let write_poh = |records: &[WincodeArchiveV2PohRecord]| {
            let mut writer =
                WincodeLeb128FramedWriter::new(File::create(root.join(ARCHIVE_V2_POH_FILE)).unwrap());
            for record in records {
                writer.write(record).unwrap();
            }
            writer.flush().unwrap();
        };

        // Matching: block 0's single entry carries the one signature the index expects; block 1
        // is empty, matching its recorded zero.
        write_poh(&[
            WincodeArchiveV2PohRecord {
                block_id: 0,
                slot: 100,
                entries: vec![CompactPohEntry {
                    num_hashes: 1,
                    hash: [0; 32],
                    tx_count: 1,
                    signature_count: 1,
                }],
            },
            WincodeArchiveV2PohRecord {
                block_id: 1,
                slot: 101,
                entries: vec![CompactPohEntry {
                    num_hashes: 1,
                    hash: [0; 32],
                    tx_count: 0,
                    signature_count: 0,
                }],
            },
        ]);
        assert!(poh_migration_epoch_verified(&root).unwrap());

        // Mismatching: block 0's sidecar entry still claims the unmigrated legacy placeholder of
        // zero signatures, which no longer sums to the index's recorded one.
        write_poh(&[
            WincodeArchiveV2PohRecord {
                block_id: 0,
                slot: 100,
                entries: vec![CompactPohEntry {
                    num_hashes: 1,
                    hash: [0; 32],
                    tx_count: 1,
                    signature_count: 0,
                }],
            },
            WincodeArchiveV2PohRecord {
                block_id: 1,
                slot: 101,
                entries: vec![CompactPohEntry {
                    num_hashes: 1,
                    hash: [0; 32],
                    tx_count: 0,
                    signature_count: 0,
                }],
            },
        ]);
        assert!(!poh_migration_epoch_verified(&root).unwrap());

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn tick_hash_matches_repeated_sha256() {
        let start = [7; 32];
        let expected = hash_one(&hash_one(&start));
        let job = EntryJob {
            start_hash: start,
            num_hashes: 2,
            transaction_count: 0,
            signatures: &[],
        };
        assert_eq!(recompute_entry_hash(&job, &mut Vec::new()), expected);
    }

    #[test]
    fn empty_tick_with_zero_hashes_keeps_start_hash() {
        let start = [9; 32];
        let job = EntryJob {
            start_hash: start,
            num_hashes: 0,
            transaction_count: 0,
            signatures: &[],
        };
        assert_eq!(recompute_entry_hash(&job, &mut Vec::new()), start);
    }

    #[test]
    fn signature_merkle_duplicates_odd_last_node() {
        let signatures = [1u8; SIGNATURE_BYTES * 3];
        let mut scratch = Vec::new();
        let root = signature_merkle_root(&signatures, &mut scratch);
        let leaf = {
            let mut hasher = Sha256::new();
            hasher.update([0]);
            hasher.update([1u8; SIGNATURE_BYTES]);
            <[u8; 32]>::from(hasher.finalize())
        };
        let pair = hash_pair_prefixed(&leaf, &leaf);
        assert_eq!(root, hash_pair_prefixed(&pair, &pair));
    }

    /// Regression test for a scratch-buffer reuse bug: the in-place compaction wrote
    /// results into a `Vec` that was never truncated between levels, so the odd-node
    /// duplication check (originally `scratch.get(...)`) bounds-checked against the
    /// buffer's original physical length instead of the current level's logical
    /// length, silently reading stale data left over from an earlier level. Leaf
    /// counts of 5 or 6 hit this (unlike the 3-leaf case above, which never does),
    /// discovered via a real PoH mismatch cross-checked against ground-truth CAR data
    /// for epoch-1 slot 523498 block 91498 entry 13.
    #[test]
    fn signature_merkle_root_matches_independent_reference_for_five_leaves() {
        let mut signatures = Vec::new();
        for index in 0..5u8 {
            signatures.extend(std::iter::repeat_n(index, SIGNATURE_BYTES));
        }
        let mut scratch = Vec::new();
        let root = signature_merkle_root(&signatures, &mut scratch);

        // Independent, non-in-place reference implementation: fresh Vec per level.
        fn reference_merkle_root(signatures: &[u8]) -> [u8; 32] {
            let mut layer: Vec<[u8; 32]> = signatures
                .chunks_exact(SIGNATURE_BYTES)
                .map(|signature| {
                    let mut hasher = Sha256::new();
                    hasher.update([0]);
                    hasher.update(signature);
                    hasher.finalize().into()
                })
                .collect();
            while layer.len() > 1 {
                layer = layer
                    .chunks(2)
                    .map(|pair| {
                        let left = pair[0];
                        let right = pair.get(1).copied().unwrap_or(left);
                        let mut hasher = Sha256::new();
                        hasher.update([1]);
                        hasher.update(left);
                        hasher.update(right);
                        hasher.finalize().into()
                    })
                    .collect();
            }
            layer[0]
        }

        assert_eq!(root, reference_merkle_root(&signatures));
    }

    fn hash_pair_prefixed(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update([1]);
        hasher.update(left);
        hasher.update(right);
        hasher.finalize().into()
    }
}
