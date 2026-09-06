use anyhow::{Context, Result};
use blockzilla_archive_v2::{ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE, ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN, ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC, ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN, ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE, ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY, ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE, ArchiveV2HotBlockIndexRow, ArchiveV2HotMetaRecord, ArchiveV2HotTxRow, BLOCK_TIME_GAP_FILE, BlockTimeGapSourceKind, PohRecordSchema, WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2PohRecord, WincodeArchiveV2PohRecordLegacyNoSignatureCount, WincodeArchiveV2ShreddingRecord, deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards, read_archive_v2_block_access_index, read_archive_v2_get_block_index, read_archive_v2_hot_block_index, read_block_time_gap_sidecar};
use blockzilla_primitives::{WincodeLeb128FramedReader, WincodeLeb128FramedWriter};
use memmap2::Mmap;
use rayon::prelude::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256, block_api::compress256};
use std::{
    cell::RefCell,
    fs::{self, File, Metadata, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
use tracing::warn;

#[cfg(test)]
use std::os::unix::fs::PermissionsExt;

const SIGNATURE_BYTES: usize = 64;
const POH_READER_BUFFER_BYTES: usize = 8 << 20;
const MAX_POH_FRAME_BYTES: usize = 64 << 20;
const MAX_META_FRAME_BYTES: usize = 64 << 20;
const MAX_SHREDDING_FRAME_BYTES: usize = 64 << 20;
const POH_MIGRATION_LOCK_FILE: &str = ".poh-signature-count-migration.lock";
const POH_ORPHAN_QUARANTINE_DIR: &str = ".poh-orphan-tail-quarantine";
/// Revision 3 binds both canonical `tx_index` ordering and fail-closed external-root handling.
/// It must not reuse revision 2 incident evidence, whose full verifier assumed that every prior
/// indexed row was the next block's PoH parent across source gaps.
const POH_ORPHAN_TAIL_REPAIR_ALGORITHM_REVISION: u32 = 3;
pub(crate) const EPOCH_998_POH_ORPHAN_REPAIR_EPOCH: u64 = 998;
const EPOCH_998_POH_ORPHAN_REPAIR_INDEXED_BLOCKS: u64 = 369_334;
const EPOCH_998_POH_ORPHAN_REPAIR_TRAILING_RECORDS: u32 = 5;
const EPOCH_998_POH_ORPHAN_REPAIR_FIRST_TRAILING_BLOCK_ID: u32 = 369_334;
const EPOCH_998_POH_ORPHAN_REPAIR_FIRST_TRAILING_SLOT: u64 = 431_559_125;
const EPOCH_998_POH_ORPHAN_REPAIR_LAST_TRAILING_SLOT: u64 = 431_559_129;
const EPOCH_998_POH_ORPHAN_REPAIR_OLD_POH_SHA256: &str =
    "b8d64f16f5da7f696cc15611c01575fac106d9e5faa5c9d7bc63ff73c0789eb0";
const EPOCH_998_POH_ORPHAN_REPAIR_PREDECESSOR_BLOCKHASH: &str =
    "c6df7153cc2d9070da2b8f663adbaf4ff0492d80d787b91658ac4ce981af3451";
const EPOCH_998_POH_ORPHAN_REPAIR_REVISION_3_INCIDENT: &str =
    "b59ba7a230d7181b0b6d2b2adadc6a101ff74b143c3187fbd6801aadcb17c435";
const REGISTRY_REPROCESS_RECEIPT_TEMP_FILE: &str =
    ".archive-v2-registry-reprocess.receipt.json.registry-access.tmp";
static POH_MIGRATION_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
/// Rows read and buffered per migration batch before all rows are checked on the worker pool.
/// Large enough to amortize the rayon fork/join per batch instead of per block; the buffered
/// state per row is small (a `WincodeArchiveV2PohRecord`, tens of entries) since each block's
/// transient decompressed bytes live only in worker-owned scratch, not in the batch itself, so
/// this does not materially increase peak memory.
const POH_MIGRATION_BATCH_ROWS: usize = 1024;

#[derive(Debug, Serialize)]
pub(crate) struct PohVerificationReport {
    archive: String,
    blocks_verified: u64,
    entries_verified: u64,
    transactions_consumed: u64,
    signatures_hashed: u64,
    hashes_recomputed: u128,
    /// Blocks whose first PoH entry cannot be rooted in the preceding indexed block, or whose
    /// empty entry list makes only the blockhash registry available as an external root.
    external_blockhash_blocks: u64,
    /// Retained for report compatibility. Deep verification now decodes every hot block and
    /// proves each PoH entry's transaction and signature boundary, so this is always zero.
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
    tx_rows: Vec<ArchiveV2HotTxRow>,
    canonical_tx_storage_positions: Vec<usize>,
    storage_signature_prefixes: Vec<u32>,
    signature_prefixes: Vec<u32>,
    /// `signatures.bin` follows hot-row storage order, while PoH entries follow canonical
    /// `tx_index` order. Populated only when those orders differ.
    canonical_signatures: Vec<u8>,
    /// Per-entry (byte_start, byte_end) into the block's signature range, in entry order.
    /// The range addresses canonical transaction order, either directly in `signatures.bin`
    /// or in `canonical_signatures` when hot rows are stored out of order.
    signature_ranges: Vec<(usize, usize)>,
    entry_jobs: Vec<EntryJobRange>,
}

/// Validates the hot-row transaction permutation and prepares signatures in the canonical
/// `tx_index` order used by PoH entries. Compact V2 stores each transaction's signatures next
/// to its hot row, so a permuted row table also permutes `signatures.bin` for that block.
///
/// Returns `true` when the storage order was already canonical. Otherwise
/// `canonical_signatures` contains the reordered bytes. Both prefix vectors are signature
/// counts (not byte offsets) and include their initial zero.
fn prepare_canonical_poh_signatures(
    block_id: u32,
    slot: u64,
    tx_rows: &[ArchiveV2HotTxRow],
    storage_signatures: &[u8],
    canonical_tx_storage_positions: &mut Vec<usize>,
    storage_signature_prefixes: &mut Vec<u32>,
    canonical_signature_prefixes: &mut Vec<u32>,
    canonical_signatures: &mut Vec<u8>,
) -> Result<bool> {
    let storage_order_is_canonical = crate::archive_v2::canonical_poh_tx_storage_positions(
        tx_rows,
        canonical_tx_storage_positions,
    )
    .with_context(|| format!("map canonical transaction order for block {block_id} slot {slot}"))?;

    storage_signature_prefixes.clear();
    storage_signature_prefixes.reserve(tx_rows.len() + 1);
    storage_signature_prefixes.push(0);
    let mut storage_total = 0u32;
    for row in tx_rows {
        storage_total = storage_total
            .checked_add(u32::from(row.signature_count))
            .context("block signature count overflow")?;
        storage_signature_prefixes.push(storage_total);
    }
    let expected_signature_bytes = usize::try_from(storage_total)
        .context("block signature count exceeds usize")?
        .checked_mul(SIGNATURE_BYTES)
        .context("block signature byte count overflow")?;
    anyhow::ensure!(
        storage_signatures.len() == expected_signature_bytes,
        "block {block_id} slot {slot} has {} storage signature bytes, expected {expected_signature_bytes}",
        storage_signatures.len()
    );

    canonical_signature_prefixes.clear();
    canonical_signature_prefixes.reserve(tx_rows.len() + 1);
    canonical_signature_prefixes.push(0);
    let mut canonical_total = 0u32;
    for &storage_position in canonical_tx_storage_positions.iter() {
        canonical_total = canonical_total
            .checked_add(u32::from(tx_rows[storage_position].signature_count))
            .context("canonical block signature count overflow")?;
        canonical_signature_prefixes.push(canonical_total);
    }
    anyhow::ensure!(
        canonical_total == storage_total,
        "block {block_id} slot {slot} canonical signature total differs from storage total"
    );

    canonical_signatures.clear();
    if !storage_order_is_canonical {
        canonical_signatures.reserve(storage_signatures.len());
        for &storage_position in canonical_tx_storage_positions.iter() {
            let byte_start = usize::try_from(storage_signature_prefixes[storage_position])
                .context("storage signature prefix exceeds usize")?
                .checked_mul(SIGNATURE_BYTES)
                .context("storage signature offset overflow")?;
            let byte_end = usize::try_from(storage_signature_prefixes[storage_position + 1])
                .context("storage signature prefix exceeds usize")?
                .checked_mul(SIGNATURE_BYTES)
                .context("storage signature offset overflow")?;
            let signatures = storage_signatures
                .get(byte_start..byte_end)
                .context("hot transaction signature range is outside block signature storage")?;
            canonical_signatures.extend_from_slice(signatures);
        }
        anyhow::ensure!(
            canonical_signatures.len() == storage_signatures.len(),
            "block {block_id} slot {slot} canonical signature bytes are incomplete"
        );
    }
    Ok(storage_order_is_canonical)
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
    /// Blocks whose PoH entries changed after exact canonical transaction-range validation.
    blocks_patched: u64,
    /// Blocks whose per-entry signature counts already matched the exact canonical ranges.
    blocks_already_current: u64,
    elapsed_secs: f64,
    worker_threads: usize,
}

#[derive(Debug)]
pub(crate) struct PohOrphanTailRepairOptions {
    pub(crate) archive_dir: PathBuf,
    pub(crate) epoch: u64,
    pub(crate) expected_indexed_blocks: u64,
    pub(crate) expected_trailing_records: u32,
    pub(crate) expected_first_trailing_block_id: u32,
    pub(crate) expected_first_trailing_slot: u64,
    pub(crate) expected_last_trailing_slot: u64,
    pub(crate) expected_old_poh_sha256: String,
    pub(crate) expected_predecessor_blockhash: String,
    pub(crate) threads: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PohOrphanTailRepairReport {
    archive: String,
    epoch: u64,
    indexed_blocks: u64,
    indexed_terminal_block_id: u32,
    indexed_terminal_slot: u64,
    predecessor_tail_present: bool,
    trailing_records_removed: u32,
    first_removed_block_id: u32,
    last_removed_block_id: u32,
    first_removed_slot: u64,
    last_removed_slot: u64,
    blocks_patched: u64,
    blocks_already_current: u64,
    metadata_entries: u64,
    indexed_poh_entries: u64,
    transaction_bearing_poh_entries: u64,
    old_poh_bytes: u64,
    new_poh_bytes: u64,
    old_poh_device: u64,
    old_poh_inode: u64,
    quarantine_device: u64,
    quarantine_inode: u64,
    quarantine_identity: Option<RepairFileIdentity>,
    old_poh_sha256: String,
    new_poh_sha256: String,
    quarantine_path: String,
    report_path: String,
    worker_threads: usize,
    elapsed_secs: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PohOrphanTailRepairBinding {
    algorithm_revision: u32,
    epoch: u64,
    expected_indexed_blocks: u64,
    expected_trailing_records: u32,
    expected_first_trailing_block_id: u32,
    expected_first_trailing_slot: u64,
    expected_last_trailing_slot: u64,
    expected_old_poh_sha256: String,
    expected_predecessor_blockhash: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PohOrphanTailRepairIntent {
    schema_version: u32,
    binding: PohOrphanTailRepairBinding,
    candidate: PohRepairCandidateBinding,
    report: PohOrphanTailRepairReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PohOrphanTailRepairWorkIntent {
    schema_version: u32,
    binding: PohOrphanTailRepairBinding,
    original_identity: RepairFileIdentity,
    old_poh_sha256: String,
    candidate_file_name: String,
    original_copy_temp_file_name: String,
    rollback_temp_file_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PohOrphanTailRepairPublicationReceipt {
    schema_version: u32,
    binding: PohOrphanTailRepairBinding,
    pre_rename_candidate: PohRepairCandidateBinding,
    published_identity: RepairFileIdentity,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PohRepairCandidateBinding {
    file_name: String,
    identity: RepairFileIdentity,
    sha256: String,
}

#[derive(Debug)]
struct PohRewriteStats {
    blocks_total: u64,
    blocks_patched: u64,
    blocks_already_current: u64,
    entries_total: u64,
    transaction_bearing_entries: u64,
    transactions: u64,
    worker_threads: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct RepairFileIdentity {
    bytes: u64,
    device: u64,
    inode: u64,
    modified_secs: i64,
    modified_nanos: i64,
    changed_secs: i64,
    changed_nanos: i64,
}

#[derive(Debug, Clone)]
struct RepairPlaneIdentity {
    path: PathBuf,
    identity: RepairFileIdentity,
}

struct StrictRepairJson<T> {
    value: T,
    bytes: Vec<u8>,
    plane: RepairPlaneIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RepairDirectoryIdentity {
    device: u64,
    inode: u64,
    owner: u32,
    mode: u32,
}

struct PohRepairLockGuard {
    file: File,
    archive_dir: PathBuf,
    archive_identity: RepairDirectoryIdentity,
    lock_path: PathBuf,
    lock_identity: RepairFileIdentity,
}

#[derive(Debug)]
struct PohRepairAuthority {
    indexed_blocks: u64,
    terminal_block_id: u32,
    terminal_slot: u64,
    metadata_entries: u64,
    metadata_transactions: u64,
    predecessor_tail_present: bool,
    plane_identities: Vec<RepairPlaneIdentity>,
    absent_plane_paths: Vec<PathBuf>,
}

/// Holds the hardened epoch PoH lock across the scheduler marker compare-and-replace. The
/// validated repair proof and every authority identity remain pinned until the caller finishes
/// that transition. Dropping this value releases the lock; it never owns a writable PoH handle.
pub(crate) struct PohOrphanTailRepairCompletionGuard {
    lock: PohRepairLockGuard,
    authority: PohRepairAuthority,
    quarantine_root: PathBuf,
    quarantine_root_identity: RepairDirectoryIdentity,
    quarantine_directory: PathBuf,
    quarantine_directory_identity: RepairDirectoryIdentity,
    proof_planes: Vec<RepairPlaneIdentity>,
    canonical: RepairPlaneIdentity,
    quarantined_original: RepairPlaneIdentity,
    absent_temp_paths: Vec<PathBuf>,
    marker_binding: String,
}

#[derive(Debug)]
struct PohOrphanTail {
    records: u32,
    first_block_id: u32,
    last_block_id: u32,
    first_slot: u64,
    last_slot: u64,
}

/// One batch row: the read-only index row it came from, its (possibly not-yet-patched) PoH
/// record, and whether its exact per-entry counts need a patch.
struct PohMigrationBatchItem<'idx> {
    row: &'idx ArchiveV2HotBlockIndexRow,
    poh: WincodeArchiveV2PohRecord,
    needs_patch: bool,
}

fn patch_poh_migration_item(
    block_map: &[u8],
    raw_blocks: bool,
    item: &mut PohMigrationBatchItem<'_>,
    decompressor: &mut zstd::bulk::Decompressor<'_>,
    uncompressed: &mut Vec<u8>,
    tx_rows_scratch: &mut Vec<ArchiveV2HotTxRow>,
    original_signature_counts_scratch: &mut Vec<u32>,
) -> Result<()> {
    let row = item.row;
    let compressed_start =
        usize::try_from(row.compressed_offset).context("compressed offset exceeds usize")?;
    let compressed_end = compressed_start
        .checked_add(row.compressed_len as usize)
        .context("compressed range overflows usize")?;
    let compressed = block_map
        .get(compressed_start..compressed_end)
        .with_context(|| format!("block {} points outside block file", row.block_id))?;
    let expected_len = row.uncompressed_len as usize;
    uncompressed.clear();
    uncompressed.reserve(expected_len);
    let decoded = if raw_blocks {
        uncompressed.extend_from_slice(compressed);
        uncompressed.len()
    } else {
        decompressor
            .decompress_to_buffer(compressed, uncompressed)
            .with_context(|| format!("decompress block {} slot {}", row.block_id, row.slot))?
    };
    anyhow::ensure!(
        decoded == expected_len,
        "block {} decompressed to {decoded} bytes, expected {expected_len}",
        row.block_id
    );
    let block =
        deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(uncompressed)
            .with_context(|| format!("decode block {} slot {}", row.block_id, row.slot))?;
    anyhow::ensure!(
        block.header.slot == row.slot && block.tx_count == row.tx_count,
        "block {} header/index mismatch",
        row.block_id
    );

    tx_rows_scratch.clear();
    tx_rows_scratch.extend(block.tx_rows());
    original_signature_counts_scratch.clear();
    original_signature_counts_scratch
        .extend(item.poh.entries.iter().map(|entry| entry.signature_count));
    crate::archive_v2::patch_poh_entry_signature_counts(&mut item.poh.entries, tx_rows_scratch)
        .with_context(|| format!("patch block {} signature counts", row.block_id))?;
    item.needs_patch = original_signature_counts_scratch.iter().copied().ne(item
        .poh
        .entries
        .iter()
        .map(|entry| entry.signature_count));
    let patched_sum = item
        .poh
        .entries
        .iter()
        .map(|entry| u64::from(entry.signature_count))
        .sum::<u64>();
    anyhow::ensure!(
        patched_sum == u64::from(row.signature_count),
        "block {} patched signature_count sum {patched_sum} still does not match index {}",
        row.block_id,
        row.signature_count
    );
    Ok(())
}

struct PohMigrationTemp {
    path: PathBuf,
    published: bool,
}

impl PohMigrationTemp {
    fn mark_published(&mut self) {
        self.published = true;
    }
}

impl Drop for PohMigrationTemp {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                "failed to remove incomplete PoH migration temp {}: {error}",
                self.path.display()
            );
        }
    }
}

fn acquire_poh_migration_lock(archive_dir: &Path) -> Result<File> {
    let archive_before = fs::symlink_metadata(archive_dir)
        .with_context(|| format!("inspect archive directory {}", archive_dir.display()))?;
    anyhow::ensure!(
        archive_before.file_type().is_dir(),
        "archive path is not one real directory: {}",
        archive_dir.display()
    );
    let path = archive_dir.join(POH_MIGRATION_LOCK_FILE);
    let mut options = OpenOptions::new();
    options
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(&path)
        .with_context(|| format!("open PoH migration lock {}", path.display()))?;
    let opened = file
        .metadata()
        .with_context(|| format!("stat PoH migration lock {}", path.display()))?;
    let linked = fs::symlink_metadata(&path)
        .with_context(|| format!("inspect PoH migration lock path {}", path.display()))?;
    let archive_after = fs::symlink_metadata(archive_dir)
        .with_context(|| format!("reinspect archive directory {}", archive_dir.display()))?;
    anyhow::ensure!(
        opened.file_type().is_file()
            && linked.file_type().is_file()
            && opened.nlink() == 1
            && opened.uid() == unsafe { libc::geteuid() }
            && opened.mode() & 0o022 == 0
            && opened.dev() == linked.dev()
            && opened.ino() == linked.ino()
            && archive_before.dev() == archive_after.dev()
            && archive_before.ino() == archive_after.ino(),
        "PoH migration lock must be one stable regular file with nlink=1 and no group/other write bits: {}",
        path.display()
    );
    // SAFETY: `file` owns this valid descriptor until the migration returns. Keeping the
    // descriptor alive makes the advisory lock cover reads, temp creation, publication, and the
    // final directory sync.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        let opened_after = file
            .metadata()
            .with_context(|| format!("restat PoH migration lock {}", path.display()))?;
        let linked_after = fs::symlink_metadata(&path)
            .with_context(|| format!("reinspect PoH migration lock {}", path.display()))?;
        anyhow::ensure!(
            repair_file_identity(&opened_after) == repair_file_identity(&opened)
                && repair_file_identity(&linked_after) == repair_file_identity(&opened)
                && opened_after.file_type().is_file()
                && opened_after.nlink() == 1
                && opened_after.uid() == unsafe { libc::geteuid() }
                && opened_after.mode() & 0o022 == 0,
            "PoH migration lock path changed while acquiring the lock: {}",
            path.display()
        );
        return Ok(file);
    }

    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EWOULDBLOCK) || error.raw_os_error() == Some(libc::EAGAIN)
    {
        anyhow::bail!(
            "another PoH signature-count migration already holds archive lock {}",
            path.display()
        );
    }
    Err(error).with_context(|| format!("lock PoH migration guard {}", path.display()))
}

fn secure_repair_directory_identity(path: &Path) -> Result<RepairDirectoryIdentity> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect secure repair directory {}", path.display()))?;
    let effective_uid = unsafe { libc::geteuid() };
    anyhow::ensure!(
        metadata.file_type().is_dir()
            && metadata.uid() == effective_uid
            && metadata.mode() & 0o022 == 0,
        "repair directory must be a real euid-owned directory with no group/other write bits: {}",
        path.display()
    );
    Ok(RepairDirectoryIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner: metadata.uid(),
        mode: metadata.mode(),
    })
}

fn validate_secure_repair_file(path: &Path) -> Result<RepairFileIdentity> {
    let (file, identity) = open_repair_regular_file(path)?;
    let opened = file
        .metadata()
        .with_context(|| format!("stat secure repair file {}", path.display()))?;
    anyhow::ensure!(
        opened.file_type().is_file()
            && opened.uid() == unsafe { libc::geteuid() }
            && opened.nlink() == 1
            && opened.mode() & 0o022 == 0,
        "repair file must be one euid-owned regular file with nlink=1 and no group/other write bits: {}",
        path.display()
    );
    Ok(identity)
}

impl PohRepairLockGuard {
    fn recheck(&self) -> Result<()> {
        let archive_before = secure_repair_directory_identity(&self.archive_dir)?;
        anyhow::ensure!(
            archive_before == self.archive_identity,
            "repair archive directory identity or mode changed while the repair lock was held: {}",
            self.archive_dir.display()
        );
        let opened = self
            .file
            .metadata()
            .with_context(|| format!("stat held repair lock {}", self.lock_path.display()))?;
        let linked = fs::symlink_metadata(&self.lock_path)
            .with_context(|| format!("reinspect held repair lock {}", self.lock_path.display()))?;
        anyhow::ensure!(
            opened.file_type().is_file()
                && linked.file_type().is_file()
                && repair_file_identity(&opened) == self.lock_identity
                && repair_file_identity(&linked) == self.lock_identity
                && opened.uid() == unsafe { libc::geteuid() }
                && opened.nlink() == 1
                && opened.mode() & 0o022 == 0,
            "held repair lock path, identity, owner, link count, or mode changed: {}",
            self.lock_path.display()
        );
        let result = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        anyhow::ensure!(
            result == 0,
            "held repair lock could not be revalidated: {}",
            self.lock_path.display()
        );
        let archive_after = secure_repair_directory_identity(&self.archive_dir)?;
        anyhow::ensure!(
            archive_after == self.archive_identity,
            "repair archive directory changed during lock revalidation: {}",
            self.archive_dir.display()
        );
        Ok(())
    }
}

fn acquire_poh_repair_lock(archive_dir: &Path) -> Result<PohRepairLockGuard> {
    let archive_identity = secure_repair_directory_identity(archive_dir)?;
    let file = acquire_poh_migration_lock(archive_dir)?;
    let lock_path = archive_dir.join(POH_MIGRATION_LOCK_FILE);
    let lock_identity = repair_file_identity(
        &file
            .metadata()
            .with_context(|| format!("stat repair lock {}", lock_path.display()))?,
    );
    let guard = PohRepairLockGuard {
        file,
        archive_dir: archive_dir.to_path_buf(),
        archive_identity,
        lock_path,
        lock_identity,
    };
    guard.recheck()?;
    Ok(guard)
}

fn create_poh_migration_temp(archive_dir: &Path) -> Result<(PohMigrationTemp, File)> {
    for _ in 0..1024 {
        let sequence = POH_MIGRATION_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = archive_dir.join(format!(
            ".{ARCHIVE_V2_POH_FILE}.migrate.{}.{sequence}.tmp",
            std::process::id()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&path)
        {
            Ok(file) => {
                return Ok((
                    PohMigrationTemp {
                        path,
                        published: false,
                    },
                    file,
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create PoH migration temp {}", path.display()));
            }
        }
    }
    anyhow::bail!(
        "could not create a unique PoH migration temp in {} after 1024 attempts",
        archive_dir.display()
    )
}

/// Backfills `signature_count` into an already-built archive's `poh.wincode`, without
/// recomputing any PoH hashes. It decompresses every hot block and compares each entry against
/// the exact canonical `tx_index` transaction range. A matching per-block total is not enough:
/// permuted hot-row storage can put that total on the wrong entries. Writes to a temp file and
/// publishes via sync+rename, so a reader (or a crash mid-run) never observes a partially written
/// sidecar.
///
/// The PoH sidecar is one framed stream, so reading and writing records stays strictly
/// sequential and in block order. A block's expensive work -- decompressing the hot block and
/// recomputing its signature counts -- has no cross-block dependency (each block is its own
/// independent zstd frame), so it runs on a worker pool: read a batch of
/// `POH_MIGRATION_BATCH_ROWS` records sequentially, verify or patch them across
/// `requested_threads` workers, then write the whole batch back out in its original order.
/// `requested_threads == 0` uses every available core, matching `verify_archive_v2_poh`.
///
/// Safe to run more than once: an already-migrated archive round-trips its records unchanged.
pub(crate) fn migrate_poh_signature_counts(
    archive_dir: &Path,
    requested_threads: usize,
) -> Result<PohSignatureCountMigrationReport> {
    let _migration_lock = acquire_poh_migration_lock(archive_dir)?;
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

    let (mut migration_temp, tmp_file) = create_poh_migration_temp(archive_dir)?;
    let mut poh_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(POH_READER_BUFFER_BYTES, tmp_file));

    let worker_threads = if requested_threads == 0 {
        std::thread::available_parallelism().map_or(1, usize::from)
    } else {
        requested_threads
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_threads)
        .thread_name(|index| format!("bz-poh-migrate-{index}"))
        .build()
        .context("build PoH migration thread pool")?;

    let started = Instant::now();
    let mut progress = crate::ProgressTracker::new("PoH Signature Count Migration");
    progress.set_estimated_total_blocks(index.rows.len() as u64);
    let mut poh_write_scratch = Vec::new();
    let mut blocks_patched = 0u64;
    let mut blocks_already_current = 0u64;
    // Every frame in this sidecar shares one schema; probing per-frame would decode a legacy
    // (pre-`signature_count`) sidecar twice on every single block.
    let mut poh_schema = blockzilla_archive_v2::PohRecordSchema::default();

    let bench_phases = std::env::var("BENCH_PHASE_TIMING").is_ok();
    let mut t_poh_read = std::time::Duration::ZERO;
    let mut t_parallel_process = std::time::Duration::ZERO;
    let mut t_poh_write = std::time::Duration::ZERO;

    let mut rows = index.rows.iter().enumerate();
    let mut batch: Vec<PohMigrationBatchItem<'_>> = Vec::with_capacity(POH_MIGRATION_BATCH_ROWS);
    loop {
        batch.clear();
        let t0 = bench_phases.then(Instant::now);
        for (position, row) in rows.by_ref().take(POH_MIGRATION_BATCH_ROWS) {
            anyhow::ensure!(
                row.block_id as usize == position,
                "non-contiguous block id {} at index position {position}",
                row.block_id
            );
            let (_, poh) = poh_reader
                .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| {
                    blockzilla_archive_v2::deserialize_archive_v2_poh_record_with_schema(
                        bytes,
                        &mut poh_schema,
                    )
                    .map_err(anyhow::Error::from)
                })?
                .with_context(|| format!("PoH sidecar ended before block {}", row.block_id))?;
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
            let needs_patch = poh_signature_sum != row.signature_count;
            batch.push(PohMigrationBatchItem {
                row,
                poh,
                needs_patch,
            });
        }
        if let Some(t0) = t0 {
            t_poh_read += t0.elapsed();
        }
        if batch.is_empty() {
            break;
        }

        let t1 = bench_phases.then(Instant::now);
        pool.install(|| {
            batch.par_iter_mut().try_for_each_init(
                || {
                    (
                        zstd::bulk::Decompressor::default(),
                        Vec::<u8>::new(),
                        Vec::<ArchiveV2HotTxRow>::new(),
                        Vec::<u32>::new(),
                    )
                },
                |(
                    decompressor,
                    uncompressed,
                    tx_rows_scratch,
                    original_signature_counts_scratch,
                ),
                 item|
                 -> Result<()> {
                    patch_poh_migration_item(
                        &block_map,
                        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0,
                        item,
                        decompressor,
                        uncompressed,
                        tx_rows_scratch,
                        original_signature_counts_scratch,
                    )
                },
            )
        })?;
        if let Some(t1) = t1 {
            t_parallel_process += t1.elapsed();
        }

        let t2 = bench_phases.then(Instant::now);
        for item in &batch {
            if item.needs_patch {
                blocks_patched += 1;
            } else {
                blocks_already_current += 1;
            }
            poh_writer
                .write_with_scratch(&item.poh, &mut poh_write_scratch)
                .with_context(|| {
                    format!("write migrated PoH record for block {}", item.row.block_id)
                })?;
            progress.update_slot(item.row.slot);
            progress.update(1, u64::from(item.row.tx_count));
        }
        if let Some(t2) = t2 {
            t_poh_write += t2.elapsed();
        }
    }

    if bench_phases {
        eprintln!(
            "PHASE_TIMING poh_read={:.3}s parallel_process={:.3}s poh_write={:.3}s total_measured={:.3}s wall_so_far={:.3}s worker_threads={worker_threads}",
            t_poh_read.as_secs_f64(),
            t_parallel_process.as_secs_f64(),
            t_poh_write.as_secs_f64(),
            (t_poh_read + t_parallel_process + t_poh_write).as_secs_f64(),
            started.elapsed().as_secs_f64(),
        );
    }

    anyhow::ensure!(
        poh_reader
            .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| {
                blockzilla_archive_v2::deserialize_archive_v2_poh_record(bytes)
                    .map_err(anyhow::Error::from)
            })?
            .is_none(),
        "PoH sidecar has trailing records"
    );

    poh_writer.flush().context("flush migrated PoH sidecar")?;
    let buffered_tmp_file = poh_writer.into_inner();
    buffered_tmp_file
        .get_ref()
        .sync_all()
        .with_context(|| format!("sync {}", migration_temp.path.display()))?;
    drop(buffered_tmp_file);
    std::fs::rename(&migration_temp.path, &poh_path).with_context(|| {
        format!(
            "rename {} to {}",
            migration_temp.path.display(),
            poh_path.display()
        )
    })?;
    migration_temp.mark_published();
    crate::first_seen_finalization::sync_directory(archive_dir)?;
    progress.final_report();

    Ok(PohSignatureCountMigrationReport {
        archive: archive_dir.display().to_string(),
        blocks_total: index.rows.len() as u64,
        blocks_patched,
        blocks_already_current,
        elapsed_secs: started.elapsed().as_secs_f64(),
        worker_threads,
    })
}

fn repair_file_identity(metadata: &Metadata) -> RepairFileIdentity {
    RepairFileIdentity {
        bytes: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_secs: metadata.mtime(),
        modified_nanos: metadata.mtime_nsec(),
        changed_secs: metadata.ctime(),
        changed_nanos: metadata.ctime_nsec(),
    }
}

fn open_repair_regular_file(path: &Path) -> Result<(File, RepairFileIdentity)> {
    let link_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect repair authority file {}", path.display()))?;
    anyhow::ensure!(
        link_metadata.file_type().is_file(),
        "repair authority path is not a regular non-symlink file: {}",
        path.display()
    );
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open repair authority file {}", path.display()))?;
    let identity = repair_file_identity(
        &file
            .metadata()
            .with_context(|| format!("stat repair authority file {}", path.display()))?,
    );
    anyhow::ensure!(
        identity == repair_file_identity(&link_metadata),
        "repair authority path changed while opening: {}",
        path.display()
    );
    Ok((file, identity))
}

fn capture_repair_file_identity(path: &Path) -> Result<RepairFileIdentity> {
    open_repair_regular_file(path).map(|(_, identity)| identity)
}

fn ensure_repair_file_identity(path: &Path, expected: RepairFileIdentity) -> Result<()> {
    let actual = capture_repair_file_identity(path)?;
    anyhow::ensure!(
        actual == expected,
        "repair authority file changed during preflight: {}",
        path.display()
    );
    Ok(())
}

fn capture_repair_plane(path: &Path) -> Result<RepairPlaneIdentity> {
    Ok(RepairPlaneIdentity {
        path: path.to_path_buf(),
        identity: capture_repair_file_identity(path)?,
    })
}

fn validate_repair_planes_unchanged(planes: &[RepairPlaneIdentity]) -> Result<()> {
    for plane in planes {
        ensure_repair_file_identity(&plane.path, plane.identity)?;
    }
    Ok(())
}

fn validate_repair_authority_unchanged(authority: &PohRepairAuthority) -> Result<()> {
    validate_repair_planes_unchanged(&authority.plane_identities)?;
    for path in &authority.absent_plane_paths {
        anyhow::ensure!(
            !optional_repair_plane_exists(path)?,
            "repair authority path appeared after preflight: {}",
            path.display()
        );
    }
    Ok(())
}

fn hash_repair_file(path: &Path) -> Result<(RepairFileIdentity, u64, String)> {
    let (mut file, identity) = open_repair_regular_file(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 8 << 20];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("hash {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = repair_file_identity(
        &file
            .metadata()
            .with_context(|| format!("stat hashed file {}", path.display()))?,
    );
    anyhow::ensure!(
        after == identity,
        "file changed while hashing: {}",
        path.display()
    );
    ensure_repair_file_identity(path, identity)?;
    let digest: [u8; 32] = hasher.finalize().into();
    Ok((identity, identity.bytes, hex32(&digest)))
}

fn update_repair_hash_from_file(
    hasher: &mut Sha256,
    path: &Path,
    expected: RepairFileIdentity,
) -> Result<()> {
    let (mut file, identity) = open_repair_regular_file(path)?;
    anyhow::ensure!(
        identity == expected,
        "block-time gap source changed before hashing: {}",
        path.display()
    );
    let mut buffer = vec![0u8; 8 << 20];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("hash block-time gap source {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    ensure_repair_file_identity(path, expected)
}

fn archive_hot_block_time_source_sha256(
    index_path: &Path,
    index_identity: RepairFileIdentity,
    blocks_path: &Path,
    blocks_identity: RepairFileIdentity,
    raw_blocks: bool,
) -> Result<[u8; 32]> {
    const DOMAIN: &[u8] = b"blockzilla:block-time-gaps:archive-v2-hot:v1";
    let mut hasher = Sha256::new();
    hasher.update((DOMAIN.len() as u64).to_le_bytes());
    hasher.update(DOMAIN);
    for (label, path, identity) in [
        (
            b"archive-v2-blocks.index".as_slice(),
            index_path,
            index_identity,
        ),
        (
            if raw_blocks {
                b"archive-v2-blocks.wincode".as_slice()
            } else {
                b"archive-v2-blocks.zstd".as_slice()
            },
            blocks_path,
            blocks_identity,
        ),
    ] {
        hasher.update((label.len() as u64).to_le_bytes());
        hasher.update(label);
        hasher.update(identity.bytes.to_le_bytes());
        update_repair_hash_from_file(&mut hasher, path, identity)?;
    }
    Ok(hasher.finalize().into())
}

fn optional_repair_plane_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.file_type().is_file(),
                "optional repair authority path is not a regular non-symlink file: {}",
                path.display()
            );
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

fn ensure_repair_path_absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
        Ok(_) => anyhow::bail!(
            "unexpected no-clobber repair path exists without a durable matching intent: {}",
            path.display()
        ),
    }
}

fn validate_expected_sha256(value: &str, label: &str) -> Result<()> {
    anyhow::ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{label} must be exactly 64 lowercase hexadecimal characters"
    );
    Ok(())
}

fn decode_expected_hash(value: &str, label: &str) -> Result<[u8; 32]> {
    validate_expected_sha256(value, label)?;
    let mut output = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let digit = |byte: u8| match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => unreachable!("validated lowercase hexadecimal"),
        };
        output[index] = (digit(pair[0]) << 4) | digit(pair[1]);
    }
    Ok(output)
}

fn reject_poh_binding_artifacts_before_lock(archive_dir: &Path) -> Result<()> {
    for file_name in [
        crate::archive_v2::registry_reprocess::REGISTRY_REPROCESS_RECEIPT_FILE,
        REGISTRY_REPROCESS_RECEIPT_TEMP_FILE,
        ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    ] {
        let path = archive_dir.join(file_name);
        anyhow::ensure!(
            !optional_repair_plane_exists(&path)?,
            "PoH repair refuses a generation bound by receipt or manifest {}",
            path.display()
        );
    }
    Ok(())
}

fn validate_expected_predecessor_before_writes(options: &PohOrphanTailRepairOptions) -> Result<()> {
    let predecessor = decode_expected_hash(
        &options.expected_predecessor_blockhash,
        "expected predecessor blockhash",
    )?;
    let predecessor_path = options
        .archive_dir
        .join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
    if optional_repair_plane_exists(&predecessor_path)? {
        let stored = read_predecessor_hash(&predecessor_path)?;
        anyhow::ensure!(
            stored == predecessor,
            "stored predecessor blockhash tail does not match the mandatory expected predecessor"
        );
    }
    let index =
        read_archive_v2_hot_block_index(&options.archive_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY == 0,
        "dictionary-compressed archives are not supported by this repair"
    );
    let row = index.rows.first().context("hot index is empty")?;

    let mut block_file = File::open(options.archive_dir.join(ARCHIVE_V2_BLOCKS_FILE))?;
    block_file.seek(SeekFrom::Start(row.compressed_offset))?;
    let mut stored = vec![0u8; row.compressed_len as usize];
    block_file.read_exact(&mut stored)?;
    let uncompressed = if index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0 {
        stored
    } else {
        zstd::bulk::decompress(&stored, row.uncompressed_len as usize)
            .context("decompress first hot block for predecessor preflight")?
    };
    anyhow::ensure!(
        uncompressed.len() == row.uncompressed_len as usize,
        "first hot block decoded length differs from its index"
    );
    let block =
        deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(&uncompressed)
            .context("decode first hot block for predecessor preflight")?;
    anyhow::ensure!(
        block.header.slot == row.slot && block.tx_count == row.tx_count,
        "first hot block header/index mismatch"
    );
    let tx_rows = block.tx_rows().collect::<Vec<_>>();

    let poh_file = File::open(options.archive_dir.join(ARCHIVE_V2_POH_FILE))?;
    let mut poh_reader = WincodeLeb128FramedReader::new(BufReader::new(poh_file));
    let mut decoder = ExactPohRecordDecoder::default();
    let (_, poh) = poh_reader
        .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| decoder.decode(bytes))?
        .context("PoH sidecar is empty")?;
    anyhow::ensure!(
        poh.block_id == row.block_id && poh.slot == row.slot,
        "first PoH record does not match the hot index"
    );
    anyhow::ensure!(
        !poh.entries.is_empty(),
        "first indexed block has no PoH entry to bind the predecessor"
    );
    let mut expected_entries = poh.entries.clone();
    crate::archive_v2::patch_poh_entry_signature_counts(&mut expected_entries, &tx_rows)
        .context("derive first PoH entry signature boundary")?;
    let signature_offset = row
        .first_signature_ordinal
        .checked_mul(SIGNATURE_BYTES as u64)
        .context("first signature offset overflow")?;
    let mut signatures_file = File::open(options.archive_dir.join(ARCHIVE_V2_SIGNATURES_FILE))?;
    signatures_file.seek(SeekFrom::Start(signature_offset))?;
    let storage_signature_bytes = usize::try_from(row.signature_count)
        .context("first block signature count exceeds usize")?
        .checked_mul(SIGNATURE_BYTES)
        .context("first block signature byte count overflow")?;
    let mut storage_signatures = vec![0u8; storage_signature_bytes];
    signatures_file.read_exact(&mut storage_signatures)?;
    let mut canonical_tx_storage_positions = Vec::new();
    let mut storage_signature_prefixes = Vec::new();
    let mut canonical_signature_prefixes = Vec::new();
    let mut canonical_signatures = Vec::new();
    let storage_order_is_canonical = prepare_canonical_poh_signatures(
        row.block_id,
        row.slot,
        &tx_rows,
        &storage_signatures,
        &mut canonical_tx_storage_positions,
        &mut storage_signature_prefixes,
        &mut canonical_signature_prefixes,
        &mut canonical_signatures,
    )?;
    let ordered_signatures = if storage_order_is_canonical {
        storage_signatures.as_slice()
    } else {
        canonical_signatures.as_slice()
    };
    let mut tx_cursor = 0usize;
    for (entry_index, entry) in expected_entries.iter().enumerate() {
        let tx_end = tx_cursor
            .checked_add(entry.tx_count as usize)
            .context("first block PoH transaction range overflow")?;
        anyhow::ensure!(
            tx_end < canonical_signature_prefixes.len(),
            "first block PoH entry {entry_index} consumes transactions beyond the hot block"
        );
        let signature_start = usize::try_from(canonical_signature_prefixes[tx_cursor])
            .context("first block signature prefix exceeds usize")?
            .checked_mul(SIGNATURE_BYTES)
            .context("first block signature offset overflow")?;
        let signature_end = usize::try_from(canonical_signature_prefixes[tx_end])
            .context("first block signature prefix exceeds usize")?
            .checked_mul(SIGNATURE_BYTES)
            .context("first block signature offset overflow")?;
        let signatures = ordered_signatures
            .get(signature_start..signature_end)
            .context("first block PoH entry signatures exceed the block signature range")?;
        let start_hash = if entry_index == 0 {
            predecessor
        } else {
            expected_entries[entry_index - 1].hash
        };
        let actual = recompute_entry_hash_standalone(&EntryJob {
            start_hash,
            num_hashes: entry.num_hashes,
            transaction_count: entry.tx_count,
            signatures,
        });
        anyhow::ensure!(
            actual == entry.hash,
            "expected predecessor blockhash does not verify PoH block {} slot {} entry {entry_index}",
            row.block_id,
            row.slot
        );
        tx_cursor = tx_end;
    }
    anyhow::ensure!(
        tx_cursor == tx_rows.len(),
        "first block PoH entries consume {tx_cursor} of {} hot transactions",
        tx_rows.len()
    );
    Ok(())
}

fn decode_meta_record(bytes: &[u8]) -> Result<ArchiveV2HotMetaRecord> {
    wincode::config::deserialize_exact(bytes, blockzilla_primitives::wincode_leb128_config())
        .map_err(anyhow::Error::from)
}

#[derive(Default)]
struct ExactPohRecordDecoder {
    schema: Option<PohRecordSchema>,
}

impl ExactPohRecordDecoder {
    fn decode(&mut self, bytes: &[u8]) -> Result<WincodeArchiveV2PohRecord> {
        let config = blockzilla_primitives::wincode_leb128_config();
        match self.schema {
            Some(PohRecordSchema::Current) => {
                wincode::config::deserialize_exact(bytes, config).map_err(anyhow::Error::from)
            }
            Some(PohRecordSchema::LegacyNoSignatureCount) => wincode::config::deserialize_exact::<
                WincodeArchiveV2PohRecordLegacyNoSignatureCount,
                _,
            >(bytes, config)
            .map(Into::into)
            .map_err(anyhow::Error::from),
            None => match wincode::config::deserialize_exact(bytes, config) {
                Ok(record) => {
                    self.schema = Some(PohRecordSchema::Current);
                    Ok(record)
                }
                Err(current_error) => match wincode::config::deserialize_exact::<
                    WincodeArchiveV2PohRecordLegacyNoSignatureCount,
                    _,
                >(bytes, config)
                {
                    Ok(record) => {
                        self.schema = Some(PohRecordSchema::LegacyNoSignatureCount);
                        Ok(record.into())
                    }
                    Err(_) => Err(anyhow::Error::from(current_error)),
                },
            },
        }
    }
}

#[derive(Debug)]
struct RepairMetadataAuthority {
    plane: RepairPlaneIdentity,
    entries: u64,
    transactions: u64,
}

fn validate_repair_meta(
    path: &Path,
    epoch: u64,
    indexed_blocks: u64,
    indexed_transactions: u64,
) -> Result<RepairMetadataAuthority> {
    let (file, identity) = open_repair_regular_file(path)?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::new(file));
    let (_, first) = reader
        .read_bytes_with_limit(MAX_META_FRAME_BYTES, decode_meta_record)?
        .context("archive metadata is empty")?;
    let ArchiveV2HotMetaRecord::Header(header) = first else {
        anyhow::bail!("archive metadata does not start with a header");
    };
    anyhow::ensure!(
        header.version == WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
        "archive metadata version {} is not supported by this repair",
        header.version
    );
    anyhow::ensure!(
        header.flags == WINCODE_ARCHIVE_V2_FLAG_LEB128,
        "archive metadata flags {:#x} are not the exact supported usage-sorted flags {:#x}",
        header.flags,
        WINCODE_ARCHIVE_V2_FLAG_LEB128
    );
    let (_, second) = reader
        .read_bytes_with_limit(MAX_META_FRAME_BYTES, decode_meta_record)?
        .context("archive metadata is missing its footer")?;
    let footer = match second {
        ArchiveV2HotMetaRecord::Footer(footer) => footer,
        ArchiveV2HotMetaRecord::Genesis(_) => {
            anyhow::ensure!(
                epoch == 0,
                "nonzero epoch metadata contains a genesis record"
            );
            let (_, third) = reader
                .read_bytes_with_limit(MAX_META_FRAME_BYTES, decode_meta_record)?
                .context("archive metadata is missing its footer after genesis")?;
            let ArchiveV2HotMetaRecord::Footer(footer) = third else {
                anyhow::bail!("archive metadata record after genesis is not a footer");
            };
            footer
        }
        ArchiveV2HotMetaRecord::Header(_) => {
            anyhow::bail!("archive metadata contains a duplicate header")
        }
    };
    anyhow::ensure!(
        reader
            .read_bytes_with_limit(MAX_META_FRAME_BYTES, decode_meta_record)?
            .is_none(),
        "archive metadata contains trailing records"
    );
    anyhow::ensure!(
        footer.blocks == indexed_blocks,
        "metadata footer blocks {} != indexed blocks {indexed_blocks}",
        footer.blocks
    );
    anyhow::ensure!(
        footer.transactions == indexed_transactions,
        "metadata footer transactions {} != indexed transactions {indexed_transactions}",
        footer.transactions
    );
    ensure_repair_file_identity(path, identity)?;
    Ok(RepairMetadataAuthority {
        plane: RepairPlaneIdentity {
            path: path.to_path_buf(),
            identity,
        },
        entries: footer.entries,
        transactions: footer.transactions,
    })
}

fn validate_repair_shredding(
    path: &Path,
    rows: &[ArchiveV2HotBlockIndexRow],
) -> Result<RepairPlaneIdentity> {
    let (file, identity) = open_repair_regular_file(path)?;
    let mut reader = WincodeLeb128FramedReader::new(BufReader::new(file));
    for row in rows {
        let (_, record) = reader
            .read_bytes_with_limit(MAX_SHREDDING_FRAME_BYTES, |bytes| {
                wincode::config::deserialize_exact(
                    bytes,
                    blockzilla_primitives::wincode_leb128_config(),
                )
                .map_err(anyhow::Error::from)
            })?
            .with_context(|| format!("shredding sidecar ended before block {}", row.block_id))?;
        let record: WincodeArchiveV2ShreddingRecord = record;
        anyhow::ensure!(
            record.block_id == row.block_id && record.slot == row.slot,
            "shredding record does not match block {} slot {}",
            row.block_id,
            row.slot
        );
    }
    anyhow::ensure!(
        reader
            .read_bytes_with_limit(MAX_SHREDDING_FRAME_BYTES, |bytes| {
                wincode::config::deserialize_exact::<WincodeArchiveV2ShreddingRecord, _>(
                    bytes,
                    blockzilla_primitives::wincode_leb128_config(),
                )
                .map_err(anyhow::Error::from)
            })?
            .is_none(),
        "shredding sidecar has records beyond the hot index terminal block"
    );
    ensure_repair_file_identity(path, identity)?;
    Ok(RepairPlaneIdentity {
        path: path.to_path_buf(),
        identity,
    })
}

fn validate_poh_repair_authority(
    options: &PohOrphanTailRepairOptions,
) -> Result<PohRepairAuthority> {
    let archive_dir = &options.archive_dir;
    anyhow::ensure!(
        archive_dir.file_name().and_then(|name| name.to_str())
            == Some(&format!("epoch-{}", options.epoch)),
        "archive directory name must be epoch-{}",
        options.epoch
    );
    anyhow::ensure!(
        options.expected_indexed_blocks > 0,
        "expected-indexed-blocks must be nonzero"
    );

    let index_path = archive_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let index_identity = capture_repair_plane(&index_path)?;
    let index = read_archive_v2_hot_block_index(&index_path)?;
    ensure_repair_file_identity(&index_path, index_identity.identity)?;
    anyhow::ensure!(
        index.rows.len() as u64 == options.expected_indexed_blocks,
        "hot index has {} blocks, expected {}",
        index.rows.len(),
        options.expected_indexed_blocks
    );
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY == 0,
        "dictionary-compressed archives are not supported by this repair"
    );

    let mut blob_cursor = 0u64;
    let mut tx_cursor = 0u64;
    let mut signature_cursor = 0u64;
    let mut previous_slot = None;
    for (position, row) in index.rows.iter().enumerate() {
        anyhow::ensure!(
            row.block_id as usize == position,
            "hot index block id {} is not contiguous at position {position}",
            row.block_id
        );
        anyhow::ensure!(
            row.compressed_offset == blob_cursor,
            "hot index block {} does not start at the prior blob terminal",
            row.block_id
        );
        anyhow::ensure!(
            row.first_tx_ordinal == tx_cursor,
            "hot index block {} transaction ordinal is not contiguous",
            row.block_id
        );
        anyhow::ensure!(
            row.first_signature_ordinal == signature_cursor,
            "hot index block {} signature ordinal is not contiguous",
            row.block_id
        );
        if let Some(previous_slot) = previous_slot {
            anyhow::ensure!(
                row.slot > previous_slot,
                "hot index slots are not strictly increasing at block {}",
                row.block_id
            );
        }
        previous_slot = Some(row.slot);
        blob_cursor = blob_cursor
            .checked_add(u64::from(row.compressed_len))
            .context("hot block blob length overflow")?;
        tx_cursor = tx_cursor
            .checked_add(u64::from(row.tx_count))
            .context("hot transaction count overflow")?;
        signature_cursor = signature_cursor
            .checked_add(u64::from(row.signature_count))
            .context("hot signature count overflow")?;
    }
    anyhow::ensure!(
        blob_cursor == index.blob_file_bytes,
        "hot index terminal blob offset {blob_cursor} != declared bytes {}",
        index.blob_file_bytes
    );

    let index_file_identity = index_identity.identity;
    let index_file_bytes = index_file_identity.bytes;
    let mut planes = vec![index_identity];
    let mut absent_planes = Vec::new();
    let blocks_path = archive_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let blocks = capture_repair_plane(&blocks_path)?;
    let blocks_file_identity = blocks.identity;
    anyhow::ensure!(
        blocks.identity.bytes == blob_cursor,
        "hot block blob bytes {} != indexed bytes {blob_cursor}",
        blocks.identity.bytes
    );
    planes.push(blocks);

    let blockhash_path = archive_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
    let blockhashes = capture_repair_plane(&blockhash_path)?;
    let blockhash_bytes = read_exact_file(&blockhash_path)?;
    ensure_repair_file_identity(&blockhash_path, blockhashes.identity)?;
    let expected_blockhash_bytes = options
        .expected_indexed_blocks
        .checked_mul(32)
        .context("blockhash registry length overflow")?;
    anyhow::ensure!(
        blockhashes.identity.bytes == expected_blockhash_bytes
            && blockhash_bytes.len() as u64 == expected_blockhash_bytes,
        "blockhash registry bytes {} != expected {expected_blockhash_bytes}",
        blockhashes.identity.bytes
    );
    planes.push(blockhashes);

    let metadata = validate_repair_meta(
        &archive_dir.join(ARCHIVE_V2_META_FILE),
        options.epoch,
        options.expected_indexed_blocks,
        tx_cursor,
    )?;
    let metadata_entries = metadata.entries;
    let metadata_transactions = metadata.transactions;
    planes.push(metadata.plane);

    let signature_path = archive_dir.join(ARCHIVE_V2_SIGNATURES_FILE);
    let signatures = capture_repair_plane(&signature_path)?;
    let expected_signature_bytes = signature_cursor
        .checked_mul(SIGNATURE_BYTES as u64)
        .context("signature sidecar length overflow")?;
    anyhow::ensure!(
        signatures.identity.bytes == expected_signature_bytes,
        "signature sidecar bytes {} != expected {expected_signature_bytes}",
        signatures.identity.bytes
    );
    planes.push(signatures);

    let predecessor_path = archive_dir.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
    let predecessor_tail_present = optional_repair_plane_exists(&predecessor_path)?;
    if predecessor_tail_present {
        let predecessor = capture_repair_plane(&predecessor_path)?;
        let stored = read_predecessor_hash(&predecessor_path)?;
        anyhow::ensure!(
            stored
                == decode_expected_hash(
                    &options.expected_predecessor_blockhash,
                    "expected predecessor blockhash",
                )?,
            "stored predecessor blockhash tail does not match the mandatory expected predecessor"
        );
        ensure_repair_file_identity(&predecessor_path, predecessor.identity)?;
        planes.push(predecessor);
    } else {
        absent_planes.push(predecessor_path.clone());
    }
    let first_seen_manifest = archive_dir.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE);
    anyhow::ensure!(
        !optional_repair_plane_exists(&first_seen_manifest)?,
        "generation has a first-seen manifest and is not eligible for focused PoH repair"
    );
    absent_planes.push(first_seen_manifest);

    let block_time_gap_path = archive_dir.join(BLOCK_TIME_GAP_FILE);
    let block_time_gap_present = optional_repair_plane_exists(&block_time_gap_path)?;
    if block_time_gap_present {
        let (file, identity) = open_repair_regular_file(&block_time_gap_path)?;
        let gap = read_block_time_gap_sidecar(BufReader::new(file))
            .with_context(|| format!("read {}", block_time_gap_path.display()))?;
        anyhow::ensure!(
            gap.header.source_kind == BlockTimeGapSourceKind::ArchiveV2Hot,
            "block-time gap sidecar is not bound to the Archive V2 hot plane"
        );
        anyhow::ensure!(
            gap.header.epoch == options.epoch,
            "block-time gap epoch {} != requested epoch {}",
            gap.header.epoch,
            options.epoch
        );
        anyhow::ensure!(
            gap.header.block_count == options.expected_indexed_blocks,
            "block-time gap block count {} != indexed blocks {}",
            gap.header.block_count,
            options.expected_indexed_blocks
        );
        let first = index.rows.first().context("hot index is empty")?;
        let terminal = index.rows.last().context("hot index is empty")?;
        anyhow::ensure!(
            gap.header.first_slot == first.slot && gap.header.last_slot == terminal.slot,
            "block-time gap endpoints {}..{} != hot index endpoints {}..{}",
            gap.header.first_slot,
            gap.header.last_slot,
            first.slot,
            terminal.slot
        );
        let expected_source_bytes = index_file_bytes
            .checked_add(blob_cursor)
            .context("block-time gap source length overflow")?;
        anyhow::ensure!(
            gap.header.source_bytes == expected_source_bytes,
            "block-time gap source bytes {} != hot index/blob bytes {expected_source_bytes}",
            gap.header.source_bytes
        );
        let expected_source_sha256 = archive_hot_block_time_source_sha256(
            &index_path,
            index_file_identity,
            &blocks_path,
            blocks_file_identity,
            index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0,
        )?;
        anyhow::ensure!(
            gap.header.source_sha256 == expected_source_sha256,
            "block-time gap source SHA-256 does not match the hot index/blob authority"
        );
        ensure_repair_file_identity(&block_time_gap_path, identity)?;
        planes.push(RepairPlaneIdentity {
            path: block_time_gap_path,
            identity,
        });
    } else {
        absent_planes.push(block_time_gap_path);
    }
    anyhow::ensure!(
        predecessor_tail_present || block_time_gap_present,
        "a usage-sorted generation without a predecessor tail requires a bound block-time gap authority"
    );

    let shredding_path = archive_dir.join(ARCHIVE_V2_SHREDDING_FILE);
    if optional_repair_plane_exists(&shredding_path)? {
        planes.push(validate_repair_shredding(&shredding_path, &index.rows)?);
    } else {
        absent_planes.push(shredding_path);
    }

    let access_path = archive_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let access_index_path = archive_dir.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let access_exists = optional_repair_plane_exists(&access_path)?;
    let access_index_exists = optional_repair_plane_exists(&access_index_path)?;
    anyhow::ensure!(
        access_exists == access_index_exists,
        "block-access blob and index must either both exist or both be absent"
    );
    let access_index = if access_exists {
        let access_index_identity = capture_repair_plane(&access_index_path)?;
        let access_index = read_archive_v2_block_access_index(&access_index_path)?;
        ensure_repair_file_identity(&access_index_path, access_index_identity.identity)?;
        anyhow::ensure!(
            access_index.rows.len() == index.rows.len(),
            "block-access index has {} rows for {} hot rows",
            access_index.rows.len(),
            index.rows.len()
        );
        let mut access_cursor = 0u64;
        for (hot, access) in index.rows.iter().zip(&access_index.rows) {
            anyhow::ensure!(
                access.block_id == hot.block_id
                    && access.slot == hot.slot
                    && access.tx_count == hot.tx_count
                    && access.signature_count == hot.signature_count,
                "block-access row does not match hot block {} slot {}",
                hot.block_id,
                hot.slot
            );
            anyhow::ensure!(
                access.access_offset == access_cursor,
                "block-access row {} does not start at the prior blob terminal",
                access.block_id
            );
            access_cursor = access_cursor
                .checked_add(u64::from(access.access_len))
                .context("block-access blob length overflow")?;
        }
        anyhow::ensure!(
            access_cursor == access_index.blob_file_bytes,
            "block-access terminal offset {access_cursor} != declared bytes {}",
            access_index.blob_file_bytes
        );
        let access_plane = capture_repair_plane(&access_path)?;
        anyhow::ensure!(
            access_plane.identity.bytes == access_cursor,
            "block-access blob bytes {} != indexed bytes {access_cursor}",
            access_plane.identity.bytes
        );
        planes.push(access_index_identity);
        planes.push(access_plane);
        Some(access_index)
    } else {
        absent_planes.push(access_path);
        absent_planes.push(access_index_path);
        None
    };

    let get_block_path = archive_dir.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE);
    if optional_repair_plane_exists(&get_block_path)? {
        let get_block_identity = capture_repair_plane(&get_block_path)?;
        let get_block = read_archive_v2_get_block_index(&get_block_path)?;
        ensure_repair_file_identity(&get_block_path, get_block_identity.identity)?;
        anyhow::ensure!(
            get_block.rows.len() == crate::SLOTS_PER_EPOCH as usize,
            "get-block index has {} slot rows, expected {}",
            get_block.rows.len(),
            crate::SLOTS_PER_EPOCH
        );
        let epoch_start = options
            .epoch
            .checked_mul(crate::SLOTS_PER_EPOCH)
            .context("epoch slot range overflow")?;
        let mut present_rows = 0u64;
        let mut last_present_slot = None;
        for (offset, row) in get_block.rows.iter().enumerate() {
            if row.is_missing() {
                anyhow::ensure!(
                    row.block_offset == 0
                        && row.block_len == 0
                        && row.access_offset == 0
                        && row.access_len == 0,
                    "get-block slot row {offset} is only partially missing"
                );
                continue;
            }
            anyhow::ensure!(
                access_exists,
                "get-block index has present rows but the block-access plane is absent"
            );
            let hot = index
                .rows
                .get(present_rows as usize)
                .context("get-block index has more present rows than the hot index")?;
            let access = access_index
                .as_ref()
                .and_then(|index| index.rows.get(present_rows as usize))
                .context("get-block index has no matching block-access row")?;
            let slot = epoch_start
                .checked_add(offset as u64)
                .context("get-block slot overflow")?;
            anyhow::ensure!(
                slot == hot.slot
                    && row.block_offset == hot.compressed_offset
                    && row.block_len == hot.compressed_len
                    && row.access_offset == access.access_offset
                    && row.access_len == access.access_len,
                "get-block row for slot {slot} does not match hot/access row {}",
                hot.block_id
            );
            present_rows += 1;
            last_present_slot = Some(slot);
        }
        anyhow::ensure!(
            present_rows == options.expected_indexed_blocks
                && last_present_slot == index.rows.last().map(|row| row.slot),
            "get-block terminal row does not match the hot index terminal"
        );
        planes.push(get_block_identity);
    } else {
        absent_planes.push(get_block_path);
    }

    let blockhash_v3_path = archive_dir.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
    if optional_repair_plane_exists(&blockhash_v3_path)? {
        let (mut file, identity) = open_repair_regular_file(&blockhash_v3_path)?;
        let mut header = [0u8; ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN];
        file.read_exact(&mut header)
            .context("read blockhash V3 authority header")?;
        let rows = u64::from_le_bytes(header[12..20].try_into().expect("fixed header"));
        anyhow::ensure!(
            &header[..8] == ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC
                && u16::from_le_bytes(header[8..10].try_into().expect("fixed header"))
                    == ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION
                && usize::from(u16::from_le_bytes(
                    header[10..12].try_into().expect("fixed header")
                )) == ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN
                && rows == options.expected_indexed_blocks,
            "blockhash V3 authority header does not match the indexed generation"
        );
        let expected_bytes = (ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN as u64)
            .checked_add(
                rows.checked_mul(ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN as u64)
                    .context("blockhash V3 length overflow")?,
            )
            .context("blockhash V3 length overflow")?;
        anyhow::ensure!(
            identity.bytes == expected_bytes,
            "blockhash V3 authority byte length does not match its rows"
        );
        let mut row_bytes = [0u8; ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN];
        for (position, hot) in index.rows.iter().enumerate() {
            file.read_exact(&mut row_bytes)
                .with_context(|| format!("read blockhash V3 row {position}"))?;
            let slot = u64::from_le_bytes(row_bytes[..8].try_into().expect("fixed row"));
            let expected_hash = blockhash_at(&blockhash_bytes, position)?;
            anyhow::ensure!(
                slot == hot.slot && row_bytes[8..40] == expected_hash,
                "blockhash V3 row {position} does not match hot slot/blockhash authority"
            );
        }
        ensure_repair_file_identity(&blockhash_v3_path, identity)?;
        planes.push(RepairPlaneIdentity {
            path: blockhash_v3_path,
            identity,
        });
    } else {
        absent_planes.push(blockhash_v3_path);
    }

    absent_planes.extend([
        archive_dir.join(crate::archive_v2::registry_reprocess::REGISTRY_REPROCESS_RECEIPT_FILE),
        archive_dir.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE),
    ]);

    validate_repair_planes_unchanged(&planes)?;
    for path in &absent_planes {
        anyhow::ensure!(
            !optional_repair_plane_exists(path)?,
            "repair authority path appeared during preflight: {}",
            path.display()
        );
    }
    let terminal = index.rows.last().context("hot index is empty")?;
    Ok(PohRepairAuthority {
        indexed_blocks: options.expected_indexed_blocks,
        terminal_block_id: terminal.block_id,
        terminal_slot: terminal.slot,
        metadata_entries,
        metadata_transactions,
        predecessor_tail_present,
        plane_identities: planes,
        absent_plane_paths: absent_planes,
    })
}

fn validate_expected_poh_orphan_tail(
    options: &PohOrphanTailRepairOptions,
    authority: &PohRepairAuthority,
    poh_reader: &mut WincodeLeb128FramedReader<BufReader<File>>,
    poh_decoder: &mut ExactPohRecordDecoder,
) -> Result<PohOrphanTail> {
    anyhow::ensure!(
        options.expected_trailing_records > 0,
        "expected-trailing-records must be nonzero"
    );
    let expected_first_block_id = authority
        .terminal_block_id
        .checked_add(1)
        .context("indexed terminal block id cannot have a following orphan")?;
    anyhow::ensure!(
        options.expected_first_trailing_block_id == expected_first_block_id,
        "expected first trailing block id {} does not follow indexed terminal {}",
        options.expected_first_trailing_block_id,
        authority.terminal_block_id
    );
    let expected_first_slot = authority
        .terminal_slot
        .checked_add(1)
        .context("indexed terminal slot cannot have a following orphan")?;
    anyhow::ensure!(
        options.expected_first_trailing_slot == expected_first_slot,
        "expected first trailing slot {} does not follow indexed terminal {}",
        options.expected_first_trailing_slot,
        authority.terminal_slot
    );
    let expected_last_slot = options
        .expected_first_trailing_slot
        .checked_add(u64::from(options.expected_trailing_records) - 1)
        .context("expected trailing slot range overflow")?;
    anyhow::ensure!(
        options.expected_last_trailing_slot == expected_last_slot,
        "expected last trailing slot {} is inconsistent with first slot {} and {} records",
        options.expected_last_trailing_slot,
        options.expected_first_trailing_slot,
        options.expected_trailing_records
    );
    let expected_last_block_id = options
        .expected_first_trailing_block_id
        .checked_add(options.expected_trailing_records - 1)
        .context("expected trailing block id range overflow")?;

    for offset in 0..options.expected_trailing_records {
        let (_, record) = poh_reader
            .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| poh_decoder.decode(bytes))?
            .with_context(|| {
                format!(
                    "PoH sidecar has fewer than {} expected trailing records",
                    options.expected_trailing_records
                )
            })?;
        let expected_block_id = options
            .expected_first_trailing_block_id
            .checked_add(offset)
            .context("trailing block id overflow")?;
        let expected_slot = options
            .expected_first_trailing_slot
            .checked_add(u64::from(offset))
            .context("trailing slot overflow")?;
        anyhow::ensure!(
            record.block_id == expected_block_id && record.slot == expected_slot,
            "PoH trailing record {offset} is block {} slot {}, expected block {expected_block_id} slot {expected_slot}",
            record.block_id,
            record.slot
        );
    }
    anyhow::ensure!(
        poh_reader
            .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| { poh_decoder.decode(bytes) })?
            .is_none(),
        "PoH sidecar has more than {} expected trailing records",
        options.expected_trailing_records
    );
    Ok(PohOrphanTail {
        records: options.expected_trailing_records,
        first_block_id: options.expected_first_trailing_block_id,
        last_block_id: expected_last_block_id,
        first_slot: options.expected_first_trailing_slot,
        last_slot: options.expected_last_trailing_slot,
    })
}

fn rewrite_poh_prefix_for_orphan_repair(
    options: &PohOrphanTailRepairOptions,
    authority: &PohRepairAuthority,
    candidate_path: &Path,
    _stop: PohRepairStopPoint,
) -> Result<(PohMigrationTemp, PohRewriteStats, PohOrphanTail)> {
    let archive_dir = &options.archive_dir;
    let index = read_archive_v2_hot_block_index(&archive_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    anyhow::ensure!(
        index.rows.len() as u64 == authority.indexed_blocks,
        "hot index changed after repair preflight"
    );
    let block_file =
        File::open(archive_dir.join(ARCHIVE_V2_BLOCKS_FILE)).context("open compact block file")?;
    // SAFETY: the shared epoch lock excludes the supported writer. Cross-plane file identities
    // are checked again before publication, so a non-cooperating replacement aborts the repair.
    let block_map = unsafe { Mmap::map(&block_file) }.context("map compact block file")?;
    if let Err(error) = block_map.advise(memmap2::Advice::Sequential) {
        warn!("madvise(SEQUENTIAL) failed for compact block file: {error}");
    }
    anyhow::ensure!(
        block_map.len() as u64 == index.blob_file_bytes,
        "block file length differs from its index"
    );

    let poh_file =
        File::open(archive_dir.join(ARCHIVE_V2_POH_FILE)).context("open compact PoH sidecar")?;
    let mut poh_reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(POH_READER_BUFFER_BYTES, poh_file));
    let (migration_temp, tmp_file) = create_bound_repair_temp(candidate_path)?;
    let mut poh_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(POH_READER_BUFFER_BYTES, tmp_file));
    let worker_threads = if options.threads == 0 {
        std::thread::available_parallelism().map_or(1, usize::from)
    } else {
        options.threads
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_threads)
        .thread_name(|index| format!("bz-poh-orphan-repair-{index}"))
        .build()
        .context("build PoH orphan-tail repair thread pool")?;

    let mut poh_decoder = ExactPohRecordDecoder::default();
    let mut rows = index.rows.iter().enumerate();
    let mut batch: Vec<PohMigrationBatchItem<'_>> = Vec::with_capacity(POH_MIGRATION_BATCH_ROWS);
    let mut write_scratch = Vec::new();
    let mut blocks_patched = 0u64;
    let mut blocks_already_current = 0u64;
    let mut entries_total = 0u64;
    let mut transaction_bearing_entries = 0u64;
    let mut transactions = 0u64;
    loop {
        batch.clear();
        for (position, row) in rows.by_ref().take(POH_MIGRATION_BATCH_ROWS) {
            anyhow::ensure!(
                row.block_id as usize == position,
                "non-contiguous block id {} at index position {position}",
                row.block_id
            );
            let (_, poh) = poh_reader
                .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| poh_decoder.decode(bytes))?
                .with_context(|| format!("PoH sidecar ended before block {}", row.block_id))?;
            anyhow::ensure!(
                poh.block_id == row.block_id && poh.slot == row.slot,
                "PoH record does not match block {} slot {}",
                row.block_id,
                row.slot
            );
            let signature_sum = poh.entries.iter().try_fold(0u32, |sum, entry| {
                sum.checked_add(entry.signature_count)
                    .context("PoH entry signature_count overflow")
            })?;
            entries_total = entries_total
                .checked_add(poh.entries.len() as u64)
                .context("indexed PoH entry count overflow")?;
            transaction_bearing_entries = transaction_bearing_entries
                .checked_add(
                    poh.entries
                        .iter()
                        .filter(|entry| entry.tx_count > 0)
                        .count() as u64,
                )
                .context("transaction-bearing PoH entry count overflow")?;
            transactions = poh.entries.iter().try_fold(transactions, |sum, entry| {
                sum.checked_add(u64::from(entry.tx_count))
                    .context("indexed PoH transaction count overflow")
            })?;
            batch.push(PohMigrationBatchItem {
                row,
                poh,
                needs_patch: signature_sum != row.signature_count,
            });
        }
        if batch.is_empty() {
            break;
        }
        pool.install(|| {
            batch.par_iter_mut().try_for_each_init(
                || {
                    (
                        zstd::bulk::Decompressor::default(),
                        Vec::<u8>::new(),
                        Vec::<ArchiveV2HotTxRow>::new(),
                        Vec::<u32>::new(),
                    )
                },
                |(
                    decompressor,
                    uncompressed,
                    tx_rows_scratch,
                    original_signature_counts_scratch,
                ),
                 item|
                 -> Result<()> {
                    patch_poh_migration_item(
                        &block_map,
                        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0,
                        item,
                        decompressor,
                        uncompressed,
                        tx_rows_scratch,
                        original_signature_counts_scratch,
                    )?;
                    Ok(())
                },
            )
        })?;
        for item in &batch {
            if item.needs_patch {
                blocks_patched += 1;
            } else {
                blocks_already_current += 1;
            }
            poh_writer
                .write_with_scratch(&item.poh, &mut write_scratch)
                .with_context(|| {
                    format!("write repaired PoH record for block {}", item.row.block_id)
                })?;
            #[cfg(test)]
            if _stop == PohRepairStopPoint::DuringCandidateWriteHardExit {
                poh_writer
                    .flush()
                    .context("flush partial repair candidate")?;
                std::process::exit(87);
            }
        }
    }

    anyhow::ensure!(
        transactions == authority.metadata_transactions,
        "indexed PoH entries consume {transactions} transactions, but metadata and hot index require {}",
        authority.metadata_transactions
    );
    let tail =
        validate_expected_poh_orphan_tail(options, authority, &mut poh_reader, &mut poh_decoder)?;
    poh_writer.flush().context("flush repaired PoH sidecar")?;
    let buffered_tmp_file = poh_writer.into_inner();
    buffered_tmp_file
        .get_ref()
        .sync_all()
        .with_context(|| format!("sync {}", migration_temp.path.display()))?;
    drop(buffered_tmp_file);
    crate::first_seen_finalization::sync_directory(archive_dir)?;
    Ok((
        migration_temp,
        PohRewriteStats {
            blocks_total: index.rows.len() as u64,
            blocks_patched,
            blocks_already_current,
            entries_total,
            transaction_bearing_entries,
            transactions,
            worker_threads,
        },
        tail,
    ))
}

struct PohRepairQuarantine {
    root: PathBuf,
    directory: PathBuf,
    original: PathBuf,
    original_copy_temp: PathBuf,
    rollback_temp: PathBuf,
    candidate: PathBuf,
    report: PathBuf,
    intent: PathBuf,
    work_intent: PathBuf,
    publication_receipt: PathBuf,
}

fn poh_repair_binding(options: &PohOrphanTailRepairOptions) -> PohOrphanTailRepairBinding {
    PohOrphanTailRepairBinding {
        algorithm_revision: POH_ORPHAN_TAIL_REPAIR_ALGORITHM_REVISION,
        epoch: options.epoch,
        expected_indexed_blocks: options.expected_indexed_blocks,
        expected_trailing_records: options.expected_trailing_records,
        expected_first_trailing_block_id: options.expected_first_trailing_block_id,
        expected_first_trailing_slot: options.expected_first_trailing_slot,
        expected_last_trailing_slot: options.expected_last_trailing_slot,
        expected_old_poh_sha256: options.expected_old_poh_sha256.clone(),
        expected_predecessor_blockhash: options.expected_predecessor_blockhash.clone(),
    }
}

fn poh_repair_incident_id(binding: &PohOrphanTailRepairBinding) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla-poh-orphan-tail-repair-v1\0");
    hasher.update(binding.algorithm_revision.to_le_bytes());
    hasher.update(binding.epoch.to_le_bytes());
    hasher.update(binding.expected_indexed_blocks.to_le_bytes());
    hasher.update(binding.expected_trailing_records.to_le_bytes());
    hasher.update(binding.expected_first_trailing_block_id.to_le_bytes());
    hasher.update(binding.expected_first_trailing_slot.to_le_bytes());
    hasher.update(binding.expected_last_trailing_slot.to_le_bytes());
    hasher.update(binding.expected_old_poh_sha256.as_bytes());
    hasher.update(binding.expected_predecessor_blockhash.as_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    hex32(&digest)
}

fn poh_repair_quarantine_paths(
    archive_dir: &Path,
    binding: &PohOrphanTailRepairBinding,
) -> PohRepairQuarantine {
    let root = archive_dir.join(POH_ORPHAN_QUARANTINE_DIR);
    let incident = format!(
        "epoch-{}-incident-{}",
        binding.epoch,
        poh_repair_incident_id(binding)
    );
    let directory = root.join(&incident);
    PohRepairQuarantine {
        original: directory.join("poh.wincode.original"),
        original_copy_temp: directory.join(".poh.wincode.original.copy.tmp"),
        rollback_temp: archive_dir.join(format!(
            ".{ARCHIVE_V2_POH_FILE}.orphan-repair.{incident}.rollback.tmp"
        )),
        candidate: archive_dir.join(format!(
            ".{ARCHIVE_V2_POH_FILE}.orphan-repair.{incident}.candidate.tmp"
        )),
        report: directory.join("repair-report.json"),
        intent: root.join(format!("{incident}.intent.json")),
        work_intent: root.join(format!("{incident}.work.json")),
        publication_receipt: root.join(format!("{incident}.published.json")),
        root,
        directory,
    }
}

fn secure_quarantine_directory_identity(path: &Path) -> Result<RepairDirectoryIdentity> {
    let identity = secure_repair_directory_identity(path)?;
    anyhow::ensure!(
        identity.mode & 0o077 == 0,
        "PoH repair quarantine directory must have no group/other permissions: {}",
        path.display()
    );
    Ok(identity)
}

fn ensure_poh_repair_quarantine_root(archive_dir: &Path, root: &Path) -> Result<()> {
    match fs::symlink_metadata(&root) {
        Ok(_) => {
            let _ = secure_quarantine_directory_identity(root)?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::DirBuilder::new()
                .mode(0o700)
                .create(&root)
                .with_context(|| format!("create PoH quarantine root {}", root.display()))?;
            let _ = secure_quarantine_directory_identity(root)?;
            crate::first_seen_finalization::sync_directory(archive_dir)?;
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect PoH quarantine root {}", root.display()));
        }
    }
    Ok(())
}

fn repair_path_basename(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned)
        .with_context(|| {
            format!(
                "repair path has no valid UTF-8 basename: {}",
                path.display()
            )
        })
}

fn poh_repair_work_intent(
    binding: &PohOrphanTailRepairBinding,
    original_identity: RepairFileIdentity,
    old_poh_sha256: &str,
    quarantine: &PohRepairQuarantine,
) -> Result<PohOrphanTailRepairWorkIntent> {
    Ok(PohOrphanTailRepairWorkIntent {
        schema_version: 1,
        binding: binding.clone(),
        original_identity,
        old_poh_sha256: old_poh_sha256.to_owned(),
        candidate_file_name: repair_path_basename(&quarantine.candidate)?,
        original_copy_temp_file_name: repair_path_basename(&quarantine.original_copy_temp)?,
        rollback_temp_file_name: repair_path_basename(&quarantine.rollback_temp)?,
    })
}

fn validate_poh_repair_work_intent(
    options: &PohOrphanTailRepairOptions,
    quarantine: &PohRepairQuarantine,
    expected_original: RepairFileIdentity,
    work: &PohOrphanTailRepairWorkIntent,
) -> Result<()> {
    anyhow::ensure!(
        work.schema_version == 1
            && work.binding == poh_repair_binding(options)
            && work.original_identity == expected_original
            && work.old_poh_sha256 == options.expected_old_poh_sha256
            && work.candidate_file_name == repair_path_basename(&quarantine.candidate)?
            && work.original_copy_temp_file_name
                == repair_path_basename(&quarantine.original_copy_temp)?
            && work.rollback_temp_file_name == repair_path_basename(&quarantine.rollback_temp)?,
        "durable PoH repair work intent conflicts with the requested incident, original, or deterministic temp paths"
    );
    Ok(())
}

fn remove_bound_repair_temp_if_present(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect repair-owned temp {}", path.display()));
        }
        Ok(_) => {}
    }
    let _ = validate_secure_repair_file(path).with_context(|| {
        format!(
            "refuse to remove a conflicting object at the repair-owned temp path {}",
            path.display()
        )
    })?;
    fs::remove_file(path)
        .with_context(|| format!("remove incomplete repair-owned temp {}", path.display()))?;
    crate::first_seen_finalization::sync_directory(
        path.parent().context("repair-owned temp has no parent")?,
    )?;
    Ok(())
}

fn remove_exact_two_link_publish_temp(temp_path: &Path, published_path: &Path) -> Result<()> {
    let temp_metadata = fs::symlink_metadata(temp_path)
        .with_context(|| format!("inspect published no-clobber temp {}", temp_path.display()))?;
    let published_metadata = fs::symlink_metadata(published_path).with_context(|| {
        format!(
            "inspect published no-clobber target {}",
            published_path.display()
        )
    })?;
    anyhow::ensure!(
        temp_metadata.file_type().is_file()
            && published_metadata.file_type().is_file()
            && temp_metadata.dev() == published_metadata.dev()
            && temp_metadata.ino() == published_metadata.ino()
            && temp_metadata.uid() == unsafe { libc::geteuid() }
            && temp_metadata.nlink() == 2
            && temp_metadata.mode() & 0o077 == 0,
        "publish temp conflicts with the published no-clobber target"
    );
    fs::remove_file(temp_path).with_context(|| {
        format!(
            "remove linked no-clobber publish temp {}",
            temp_path.display()
        )
    })?;
    crate::first_seen_finalization::sync_directory(
        temp_path
            .parent()
            .context("no-clobber publish temp has no parent")?,
    )?;
    Ok(())
}

fn create_bound_repair_temp(path: &Path) -> Result<(PohMigrationTemp, File)> {
    remove_bound_repair_temp_if_present(path)?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("create repair-owned temp {}", path.display()))?;
    let _ = validate_secure_repair_file(path)?;
    crate::first_seen_finalization::sync_directory(
        path.parent().context("repair-owned temp has no parent")?,
    )?;
    Ok((
        PohMigrationTemp {
            path: path.to_path_buf(),
            published: false,
        },
        file,
    ))
}

fn copy_repair_file_to_bound_temp(
    source_path: &Path,
    expected_source: RepairFileIdentity,
    target_path: &Path,
    _stop: PohRepairStopPoint,
    _hard_exit_point: PohRepairStopPoint,
    _hard_exit_code: i32,
) -> Result<PohMigrationTemp> {
    let (mut source, source_identity) = open_repair_regular_file(source_path)?;
    anyhow::ensure!(
        source_identity == expected_source,
        "repair copy source changed before opening: {}",
        source_path.display()
    );
    let (temp, mut target) = create_bound_repair_temp(target_path)?;
    let mut buffer = vec![0u8; 8 << 20];
    let mut copied = 0u64;
    loop {
        let read = source
            .read(&mut buffer)
            .with_context(|| format!("read repair copy source {}", source_path.display()))?;
        if read == 0 {
            break;
        }
        target
            .write_all(&buffer[..read])
            .with_context(|| format!("write repair-owned temp {}", target_path.display()))?;
        copied = copied
            .checked_add(read as u64)
            .context("repair copy byte count overflow")?;
        #[cfg(test)]
        if _stop == _hard_exit_point {
            target.flush().context("flush partial repair-owned copy")?;
            std::process::exit(_hard_exit_code);
        }
    }
    anyhow::ensure!(
        copied == expected_source.bytes,
        "repair copy wrote {copied} bytes, expected {}",
        expected_source.bytes
    );
    target.flush().context("flush repair-owned copy")?;
    target.sync_all().context("sync repair-owned copy")?;
    drop(target);
    ensure_repair_file_identity(source_path, source_identity)?;
    crate::first_seen_finalization::sync_directory(
        target_path
            .parent()
            .context("repair copy temp has no parent")?,
    )?;
    Ok(temp)
}

fn prepare_poh_repair_quarantine(
    archive_dir: &Path,
    quarantine: &PohRepairQuarantine,
    expected_original: RepairFileIdentity,
    expected_old_sha256: &str,
    lock: &PohRepairLockGuard,
    _stop: PohRepairStopPoint,
) -> Result<()> {
    lock.recheck()?;
    ensure_poh_repair_quarantine_root(archive_dir, &quarantine.root)?;

    match fs::DirBuilder::new()
        .mode(0o700)
        .create(&quarantine.directory)
    {
        Ok(()) => {
            let _ = secure_quarantine_directory_identity(&quarantine.directory)?;
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let _ = secure_quarantine_directory_identity(&quarantine.directory)?;
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("create PoH quarantine {}", quarantine.directory.display())
            });
        }
    }
    crate::first_seen_finalization::sync_directory(&quarantine.root)?;
    lock.recheck()?;

    let canonical = archive_dir.join(ARCHIVE_V2_POH_FILE);
    ensure_repair_file_identity(&canonical, expected_original)?;
    let original_exists = optional_repair_plane_exists(&quarantine.original)?;
    let copy_temp_exists = optional_repair_plane_exists(&quarantine.original_copy_temp)?;
    if original_exists {
        let (original_identity, original_bytes, original_sha256) =
            hash_repair_file(&quarantine.original)?;
        anyhow::ensure!(
            original_bytes == expected_original.bytes && original_sha256 == expected_old_sha256,
            "existing no-clobber PoH quarantine conflicts with the durable repair work intent"
        );
        if copy_temp_exists {
            let copy_identity = capture_repair_file_identity(&quarantine.original_copy_temp)?;
            if copy_identity.device == original_identity.device
                && copy_identity.inode == original_identity.inode
            {
                remove_exact_two_link_publish_temp(
                    &quarantine.original_copy_temp,
                    &quarantine.original,
                )?;
            } else {
                remove_bound_repair_temp_if_present(&quarantine.original_copy_temp)?;
            }
        }
    } else {
        let mut copy_temp = copy_repair_file_to_bound_temp(
            &canonical,
            expected_original,
            &quarantine.original_copy_temp,
            _stop,
            PohRepairStopPoint::DuringQuarantineCopyHardExit,
            88,
        )?;
        let (_, copied_bytes, copied_sha256) = hash_repair_file(&copy_temp.path)?;
        anyhow::ensure!(
            copied_bytes == expected_original.bytes && copied_sha256 == expected_old_sha256,
            "independent PoH quarantine temp differs from the canonical original"
        );
        lock.recheck()?;
        ensure_repair_file_identity(&canonical, expected_original)?;
        match fs::hard_link(&copy_temp.path, &quarantine.original) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let (_, existing_bytes, existing_sha256) = hash_repair_file(&quarantine.original)?;
                anyhow::ensure!(
                    existing_bytes == expected_original.bytes
                        && existing_sha256 == expected_old_sha256,
                    "concurrent no-clobber PoH quarantine conflicts with the repair copy"
                );
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "publish independent no-clobber PoH quarantine {}",
                        quarantine.original.display()
                    )
                });
            }
        }
        crate::first_seen_finalization::sync_directory(&quarantine.directory)?;
        #[cfg(test)]
        if _stop == PohRepairStopPoint::DuringQuarantinePublishHardExit {
            std::process::exit(90);
        }
        fs::remove_file(&copy_temp.path).with_context(|| {
            format!(
                "remove published quarantine copy temp {}",
                copy_temp.path.display()
            )
        })?;
        copy_temp.mark_published();
        crate::first_seen_finalization::sync_directory(&quarantine.directory)?;
    }
    lock.recheck()?;
    let canonical_after_copy = capture_repair_file_identity(&canonical)?;
    let (quarantine_identity, quarantine_bytes, quarantine_sha256) =
        hash_repair_file(&quarantine.original)?;
    let quarantine_metadata = fs::symlink_metadata(&quarantine.original)?;
    anyhow::ensure!(
        canonical_after_copy == expected_original
            && quarantine_identity.inode != canonical_after_copy.inode
            && quarantine_bytes == expected_original.bytes
            && quarantine_sha256 == expected_old_sha256
            && quarantine_metadata.nlink() == 1
            && quarantine_metadata.uid() == unsafe { libc::geteuid() }
            && quarantine_metadata.mode() & 0o077 == 0,
        "existing no-clobber PoH quarantine conflicts with the durable repair intent"
    );
    crate::first_seen_finalization::sync_directory(&quarantine.directory)?;
    crate::first_seen_finalization::sync_directory(&quarantine.root)?;
    crate::first_seen_finalization::sync_directory(archive_dir)?;
    Ok(())
}

fn ensure_canonical_matches_quarantined_original(
    archive_dir: &Path,
    quarantine: &PohRepairQuarantine,
    expected_canonical: RepairFileIdentity,
) -> Result<()> {
    let canonical = capture_repair_file_identity(&archive_dir.join(ARCHIVE_V2_POH_FILE))?;
    let original = capture_repair_file_identity(&quarantine.original)?;
    anyhow::ensure!(
        canonical == expected_canonical
            && canonical.inode != original.inode
            && canonical.bytes == original.bytes,
        "canonical PoH sidecar changed after the original was quarantined"
    );
    Ok(())
}

fn validate_published_repair_candidate(
    path: &Path,
    intent: &PohOrphanTailRepairIntent,
    receipt: &PohOrphanTailRepairPublicationReceipt,
) -> Result<RepairFileIdentity> {
    let secure_identity = validate_secure_repair_file(path)?;
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect published repair candidate {}", path.display()))?;
    let (identity, bytes, sha256) = hash_repair_file(path)?;
    anyhow::ensure!(
        secure_identity == identity
            && receipt.schema_version == 1
            && receipt.binding == intent.binding
            && receipt.pre_rename_candidate == intent.candidate
            && identity == receipt.published_identity
            && bytes == intent.report.new_poh_bytes
            && bytes == intent.candidate.identity.bytes
            && bytes == receipt.bytes
            && sha256 == intent.report.new_poh_sha256
            && sha256 == intent.candidate.sha256
            && sha256 == receipt.sha256
            && metadata.file_type().is_file()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.nlink() == 1
            && metadata.mode() & 0o777 == 0o600,
        "canonical PoH is not the exact secure intent-bound published repair candidate: {}",
        path.display()
    );
    Ok(identity)
}

fn capture_or_validate_published_repair_receipt(
    path: &Path,
    quarantine: &PohRepairQuarantine,
    intent: &PohOrphanTailRepairIntent,
    fault: RepairJsonPublishFault,
) -> Result<PohOrphanTailRepairPublicationReceipt> {
    let secure_identity = validate_secure_repair_file(path)?;
    let (identity, bytes, sha256) = hash_repair_file(path)?;
    anyhow::ensure!(
        secure_identity == identity
            && identity.device == intent.candidate.identity.device
            && identity.inode == intent.candidate.identity.inode
            && bytes == intent.candidate.identity.bytes
            && bytes == intent.report.new_poh_bytes
            && sha256 == intent.candidate.sha256
            && sha256 == intent.report.new_poh_sha256,
        "canonical PoH cannot be authorized as the exact atomically renamed intent candidate"
    );
    let expected = PohOrphanTailRepairPublicationReceipt {
        schema_version: 1,
        binding: intent.binding.clone(),
        pre_rename_candidate: intent.candidate.clone(),
        published_identity: identity,
        bytes,
        sha256,
    };
    if optional_repair_plane_exists(&quarantine.publication_receipt)? {
        let existing: PohOrphanTailRepairPublicationReceipt =
            read_bounded_repair_json(&quarantine.publication_receipt)?;
        anyhow::ensure!(
            existing.schema_version == expected.schema_version
                && existing.binding == expected.binding
                && existing.pre_rename_candidate == expected.pre_rename_candidate
                && existing.published_identity == expected.published_identity
                && existing.bytes == expected.bytes
                && existing.sha256 == expected.sha256,
            "existing PoH publication receipt conflicts with the exact canonical candidate"
        );
    } else {
        publish_repair_json_no_clobber_with_fault(
            &quarantine.publication_receipt,
            &expected,
            fault,
        )?;
    }
    let receipt: PohOrphanTailRepairPublicationReceipt =
        read_bounded_repair_json(&quarantine.publication_receipt)?;
    let _ = validate_published_repair_candidate(path, intent, &receipt)?;
    Ok(receipt)
}

fn restore_quarantined_poh_after_failed_verification(
    archive_dir: &Path,
    quarantine: &PohRepairQuarantine,
    intent: &PohOrphanTailRepairIntent,
    receipt: &PohOrphanTailRepairPublicationReceipt,
    lock: &PohRepairLockGuard,
    _stop: PohRepairStopPoint,
) -> Result<()> {
    lock.recheck()?;
    let canonical = archive_dir.join(ARCHIVE_V2_POH_FILE);
    let _ = validate_published_repair_candidate(&canonical, intent, receipt).context(
        "automatic rollback refuses to overwrite a canonical PoH that is not the exact published candidate",
    )?;
    let original_identity = validate_secure_repair_file(&quarantine.original)?;
    let mut rollback = copy_repair_file_to_bound_temp(
        &quarantine.original,
        original_identity,
        &quarantine.rollback_temp,
        _stop,
        PohRepairStopPoint::DuringRollbackCopyHardExit,
        89,
    )?;
    ensure_repair_file_identity(&quarantine.original, original_identity)?;
    let (_, rollback_bytes, rollback_sha256) = hash_repair_file(&rollback.path)?;
    anyhow::ensure!(
        rollback_bytes == original_identity.bytes
            && rollback_sha256 == intent.report.old_poh_sha256,
        "atomic PoH rollback copy differs from the quarantined original"
    );
    lock.recheck()?;
    let _ = validate_published_repair_candidate(&canonical, intent, receipt).context(
        "automatic rollback refuses to overwrite a canonical PoH that changed during rollback copy",
    )?;
    validate_quarantined_original_sha(
        &quarantine.original,
        original_identity,
        &intent.report.old_poh_sha256,
    )?;
    fs::rename(&rollback.path, &canonical).with_context(|| {
        format!(
            "restore quarantined PoH original {} to {}",
            quarantine.original.display(),
            canonical.display()
        )
    })?;
    rollback.mark_published();
    crate::first_seen_finalization::sync_directory(archive_dir)?;
    #[cfg(test)]
    if _stop == PohRepairStopPoint::AfterRollbackPublishHardExit {
        std::process::exit(91);
    }
    lock.recheck()?;
    let (_, _, restored_sha256) = hash_repair_file(&canonical)?;
    anyhow::ensure!(
        restored_sha256 == intent.report.old_poh_sha256,
        "restored PoH original SHA-256 differs from the pre-repair SHA-256"
    );
    Ok(())
}

fn validate_quarantined_original_sha(
    path: &Path,
    expected_identity: RepairFileIdentity,
    expected_sha256: &str,
) -> Result<()> {
    let (identity, bytes, sha256) = hash_repair_file(path)?;
    anyhow::ensure!(
        identity == expected_identity
            && bytes == expected_identity.bytes
            && sha256 == expected_sha256,
        "quarantined PoH original changed before rollback publication"
    );
    Ok(())
}

fn read_bounded_repair_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    const MAX_REPAIR_JSON_BYTES: u64 = 1 << 20;
    recover_repair_json_publish(path)?;
    let secure_identity = validate_secure_repair_json_file(path, 1)?;
    let (mut file, identity) = open_repair_regular_file(path)?;
    anyhow::ensure!(
        identity == secure_identity,
        "repair JSON changed between secure validation and open: {}",
        path.display()
    );
    anyhow::ensure!(
        identity.bytes <= MAX_REPAIR_JSON_BYTES,
        "repair JSON {} exceeds {} bytes",
        path.display(),
        MAX_REPAIR_JSON_BYTES
    );
    let mut bytes = Vec::with_capacity(identity.bytes as usize);
    file.read_to_end(&mut bytes)
        .with_context(|| format!("read repair JSON {}", path.display()))?;
    ensure_repair_file_identity(path, identity)?;
    serde_json::from_slice(&bytes).with_context(|| format!("decode repair JSON {}", path.display()))
}

/// Terminal completion proof must already be fully published. Unlike the repair resume reader,
/// this reader never repairs or removes a deterministic JSON publish temp.
fn read_bounded_repair_json_strict<T: DeserializeOwned>(
    path: &Path,
) -> Result<StrictRepairJson<T>> {
    const MAX_REPAIR_JSON_BYTES: u64 = 1 << 20;
    let publish_temp = repair_json_publish_temp_path(path)?;
    anyhow::ensure!(
        !optional_repair_plane_exists(&publish_temp)?,
        "terminal PoH repair proof has an unfinished JSON publish temp: {}",
        publish_temp.display()
    );
    let secure_identity = validate_secure_repair_json_file(path, 1)?;
    anyhow::ensure!(
        secure_identity.bytes <= MAX_REPAIR_JSON_BYTES,
        "repair JSON {} exceeds {} bytes",
        path.display(),
        MAX_REPAIR_JSON_BYTES
    );
    let (mut file, identity) = open_repair_regular_file(path)?;
    anyhow::ensure!(
        identity == secure_identity,
        "repair JSON changed between strict validation and open: {}",
        path.display()
    );
    let mut bytes = Vec::with_capacity(identity.bytes as usize);
    file.read_to_end(&mut bytes)
        .with_context(|| format!("read terminal repair JSON {}", path.display()))?;
    ensure_repair_file_identity(path, identity)?;
    anyhow::ensure!(
        !optional_repair_plane_exists(&publish_temp)?,
        "terminal PoH repair JSON publish temp appeared while reading: {}",
        publish_temp.display()
    );
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode terminal repair JSON {}", path.display()))?;
    Ok(StrictRepairJson {
        value,
        bytes,
        plane: RepairPlaneIdentity {
            path: path.to_path_buf(),
            identity,
        },
    })
}

fn publish_repair_json_no_clobber<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    publish_repair_json_no_clobber_with_fault(path, value, RepairJsonPublishFault::None)
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, PartialEq, Eq)]
enum RepairJsonPublishFault {
    None,
    ErrorAfterLink,
    HardExitAfterLink,
}

fn repair_json_publish_temp_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().context("repair JSON path has no parent")?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("repair JSON path has no valid UTF-8 basename")?;
    anyhow::ensure!(
        Path::new(file_name)
            .parent()
            .is_some_and(|value| value.as_os_str().is_empty()),
        "repair JSON path does not have one plain basename"
    );
    Ok(parent.join(format!(".{file_name}.publish.tmp")))
}

fn validate_secure_repair_json_file(
    path: &Path,
    expected_nlink: u64,
) -> Result<RepairFileIdentity> {
    let (file, identity) = open_repair_regular_file(path)?;
    let metadata = file
        .metadata()
        .with_context(|| format!("stat secure repair JSON {}", path.display()))?;
    anyhow::ensure!(
        metadata.file_type().is_file()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.nlink() == expected_nlink
            && metadata.mode() & 0o777 == 0o600,
        "repair JSON must be one euid-owned regular mode-0600 file with nlink={expected_nlink}: {}",
        path.display()
    );
    Ok(identity)
}

fn read_secure_repair_json_bytes(path: &Path, expected_nlink: u64) -> Result<Vec<u8>> {
    let expected = validate_secure_repair_json_file(path, expected_nlink)?;
    let bytes = read_exact_file(path)?;
    ensure_repair_file_identity(path, expected)?;
    Ok(bytes)
}

fn recover_repair_json_publish(path: &Path) -> Result<()> {
    let parent = path.parent().context("repair JSON path has no parent")?;
    let _ = secure_quarantine_directory_identity(parent)?;
    let temp = repair_json_publish_temp_path(path)?;
    let target_exists = optional_repair_plane_exists(path)?;
    let temp_exists = optional_repair_plane_exists(&temp)?;
    if !target_exists {
        return Ok(());
    }
    if temp_exists {
        let target_identity = validate_secure_repair_json_file(path, 2)?;
        let temp_identity = validate_secure_repair_json_file(&temp, 2)?;
        anyhow::ensure!(
            target_identity.device == temp_identity.device
                && target_identity.inode == temp_identity.inode,
            "repair JSON target and deterministic publish temp are conflicting file objects: {} and {}",
            path.display(),
            temp.display()
        );
        fs::remove_file(&temp).with_context(|| {
            format!(
                "remove recovered repair JSON publish temp {}",
                temp.display()
            )
        })?;
        crate::first_seen_finalization::sync_directory(parent)?;
    }
    let _ = validate_secure_repair_json_file(path, 1)?;
    Ok(())
}

fn publish_repair_json_no_clobber_with_fault<T: Serialize>(
    path: &Path,
    value: &T,
    fault: RepairJsonPublishFault,
) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value).context("serialize durable repair JSON")?;
    bytes.push(b'\n');
    let parent = path.parent().context("repair JSON path has no parent")?;
    let _ = secure_quarantine_directory_identity(parent)?;
    recover_repair_json_publish(path)?;
    if optional_repair_plane_exists(path)? {
        let existing = read_secure_repair_json_bytes(path, 1)?;
        anyhow::ensure!(
            existing == bytes,
            "existing no-clobber repair JSON conflicts with expected bytes: {}",
            path.display()
        );
        crate::first_seen_finalization::sync_directory(parent)?;
        return Ok(());
    }
    let temp = repair_json_publish_temp_path(path)?;
    if optional_repair_plane_exists(&temp)? {
        let existing = read_secure_repair_json_bytes(&temp, 1)?;
        if existing != bytes {
            fs::remove_file(&temp).with_context(|| {
                format!(
                    "remove incomplete deterministic repair JSON temp {}",
                    temp.display()
                )
            })?;
            crate::first_seen_finalization::sync_directory(parent)?;
        }
    }
    if !optional_repair_plane_exists(&temp)? {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&temp)
            .with_context(|| format!("create repair JSON temp {}", temp.display()))?;
        file.write_all(&bytes).context("write repair JSON temp")?;
        file.flush().context("flush repair JSON temp")?;
        file.sync_all().context("sync repair JSON temp")?;
        drop(file);
        let _ = validate_secure_repair_json_file(&temp, 1)?;
        crate::first_seen_finalization::sync_directory(parent)?;
    }
    let publish = fs::hard_link(&temp, path);
    match publish {
        Ok(()) => {
            if fault == RepairJsonPublishFault::ErrorAfterLink {
                anyhow::bail!("injected repair JSON parent-sync failure after final link creation");
            }
            if fault == RepairJsonPublishFault::HardExitAfterLink {
                std::process::exit(92);
            }
            crate::first_seen_finalization::sync_directory(parent)?;
            remove_exact_two_link_publish_temp(&temp, path)?;
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            recover_repair_json_publish(path)?;
            let existing = read_secure_repair_json_bytes(path, 1)?;
            anyhow::ensure!(
                existing == bytes,
                "concurrent no-clobber repair JSON conflicts with expected bytes: {}",
                path.display()
            );
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("publish no-clobber repair JSON {}", path.display()));
        }
    }
    crate::first_seen_finalization::sync_directory(parent)?;
    let existing = read_secure_repair_json_bytes(path, 1)?;
    anyhow::ensure!(
        existing == bytes,
        "published repair JSON differs from the exact expected bytes: {}",
        path.display()
    );
    Ok(())
}

fn poh_repair_candidate_path(
    archive_dir: &Path,
    candidate: &PohRepairCandidateBinding,
) -> Result<PathBuf> {
    let name = Path::new(&candidate.file_name);
    anyhow::ensure!(
        name.parent()
            .is_some_and(|parent| parent.as_os_str().is_empty())
            && name.file_name().and_then(|value| value.to_str())
                == Some(candidate.file_name.as_str())
            && candidate
                .file_name
                .starts_with(&format!(".{ARCHIVE_V2_POH_FILE}.orphan-repair."))
            && candidate.file_name.ends_with(".candidate.tmp"),
        "durable PoH repair intent has an invalid candidate basename"
    );
    Ok(archive_dir.join(name))
}

fn open_intent_bound_candidate(
    archive_dir: &Path,
    candidate: &PohRepairCandidateBinding,
) -> Result<PohMigrationTemp> {
    validate_expected_sha256(&candidate.sha256, "candidate SHA-256")?;
    let path = poh_repair_candidate_path(archive_dir, candidate)?;
    let secure_identity = validate_secure_repair_file(&path).with_context(|| {
        format!(
            "durable PoH repair candidate is not a secure repair-owned file at {}",
            path.display()
        )
    })?;
    let (identity, bytes, sha256) = hash_repair_file(&path).with_context(|| {
        format!(
            "durable PoH repair candidate is missing or unreadable at {}",
            path.display()
        )
    })?;
    anyhow::ensure!(
        secure_identity == identity
            && identity == candidate.identity
            && bytes == candidate.identity.bytes
            && sha256 == candidate.sha256,
        "durable PoH repair candidate conflicts with its exact path, identity, size, or SHA-256 binding"
    );
    Ok(PohMigrationTemp {
        path,
        published: false,
    })
}

fn validate_quarantined_original_against_intent(
    quarantine: &PohRepairQuarantine,
    intent: &PohOrphanTailRepairIntent,
) -> Result<RepairFileIdentity> {
    let _ = secure_quarantine_directory_identity(&quarantine.root)?;
    let _ = secure_quarantine_directory_identity(&quarantine.directory)?;
    let secure_identity = validate_secure_repair_file(&quarantine.original)?;
    let (identity, bytes, sha256) = hash_repair_file(&quarantine.original)?;
    let metadata = fs::symlink_metadata(&quarantine.original)?;
    anyhow::ensure!(
        secure_identity == identity
            && identity.inode != intent.report.old_poh_inode
            && bytes == intent.report.old_poh_bytes
            && sha256 == intent.report.old_poh_sha256
            && metadata.nlink() == 1
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.mode() & 0o777 == 0o600,
        "quarantined original no longer matches the durable repair intent"
    );
    Ok(identity)
}

fn preflight_poh_repair_before_lock(options: &PohOrphanTailRepairOptions) -> Result<()> {
    validate_expected_sha256(&options.expected_old_poh_sha256, "expected old PoH SHA-256")?;
    let _ = decode_expected_hash(
        &options.expected_predecessor_blockhash,
        "expected predecessor blockhash",
    )?;
    let archive_before = secure_repair_directory_identity(&options.archive_dir)?;
    let poh_path = options.archive_dir.join(ARCHIVE_V2_POH_FILE);
    let _ = validate_secure_repair_file(&poh_path)?;
    let archive_after = secure_repair_directory_identity(&options.archive_dir)?;
    anyhow::ensure!(
        archive_before == archive_after,
        "repair archive directory changed during preflight"
    );
    reject_poh_binding_artifacts_before_lock(&options.archive_dir)?;
    let (_, _, canonical_sha256) = hash_repair_file(&poh_path)?;
    if canonical_sha256 == options.expected_old_poh_sha256 {
        return validate_expected_predecessor_before_writes(options);
    }

    let binding = poh_repair_binding(options);
    let quarantine = poh_repair_quarantine_paths(&options.archive_dir, &binding);
    anyhow::ensure!(
        optional_repair_plane_exists(&quarantine.intent)?,
        "canonical PoH SHA-256 {canonical_sha256} does not match mandatory expected old SHA-256 {} and no matching durable repair intent exists",
        options.expected_old_poh_sha256
    );
    let _ = secure_quarantine_directory_identity(&quarantine.root)?;
    let intent: PohOrphanTailRepairIntent = read_bounded_repair_json(&quarantine.intent)?;
    anyhow::ensure!(
        intent.binding == binding
            && intent.report.old_poh_sha256 == options.expected_old_poh_sha256
            && intent.report.new_poh_sha256 == canonical_sha256,
        "canonical PoH differs from the expected old SHA-256 and the durable intent does not authorize its current SHA-256"
    );
    validate_expected_predecessor_before_writes(options)
}

/// Repairs one diagnosed PoH-only orphan suffix. This is deliberately separate from the normal
/// signature-count migration: the normal command continues to reject every trailing frame.
/// Exact incident coordinates, all authoritative terminal planes, a no-clobber quarantine, and
/// a full post-publication PoH verification are mandatory.
fn finish_poh_orphan_tail_repair(
    options: &PohOrphanTailRepairOptions,
    authority: &PohRepairAuthority,
    quarantine: &PohRepairQuarantine,
    intent: &PohOrphanTailRepairIntent,
    lock: &PohRepairLockGuard,
    _stop: PohRepairStopPoint,
) -> Result<PohOrphanTailRepairReport> {
    lock.recheck()?;
    let poh_path = options.archive_dir.join(ARCHIVE_V2_POH_FILE);
    #[cfg(test)]
    if _stop == PohRepairStopPoint::AfterPublishBeforeReceiptHardExit {
        std::process::exit(93);
    }
    let receipt_fault = match _stop {
        #[cfg(test)]
        PohRepairStopPoint::AfterPublicationReceiptLinkError => {
            RepairJsonPublishFault::ErrorAfterLink
        }
        #[cfg(test)]
        PohRepairStopPoint::DuringPublicationReceiptJsonHardExit => {
            RepairJsonPublishFault::HardExitAfterLink
        }
        _ => RepairJsonPublishFault::None,
    };
    let receipt =
        capture_or_validate_published_repair_receipt(&poh_path, quarantine, intent, receipt_fault)?;
    lock.recheck()?;
    #[cfg(test)]
    match _stop {
        PohRepairStopPoint::BeforePostVerifyReplaceCanonical => {
            let evidence = quarantine
                .directory
                .join("test-published-candidate-evidence");
            fs::rename(&poh_path, &evidence)?;
            let mut unknown = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&poh_path)?;
            unknown.write_all(b"unknown replacement canonical")?;
            unknown.sync_all()?;
            crate::first_seen_finalization::sync_directory(&options.archive_dir)?;
        }
        PohRepairStopPoint::BeforePostVerifyChmodCanonical => {
            fs::set_permissions(&poh_path, fs::Permissions::from_mode(0o644))?;
        }
        PohRepairStopPoint::BeforePostVerifyExtraHardlinkCanonical => {
            fs::hard_link(
                &poh_path,
                quarantine.directory.join("test-candidate-extra-link"),
            )?;
            crate::first_seen_finalization::sync_directory(&quarantine.directory)?;
        }
        _ => {}
    }
    let post_verify = verify_archive_v2_poh_with_predecessor_policy(
        &options.archive_dir,
        options.threads,
        None,
        Some(decode_expected_hash(
            &options.expected_predecessor_blockhash,
            "expected predecessor blockhash",
        )?),
    )
    .and_then(|_| {
        lock.recheck()?;
        validate_repair_authority_unchanged(authority)?;
        let _ = validate_published_repair_candidate(&poh_path, intent, &receipt).context(
            "published PoH sidecar differs from the durable repair intent during full post-verification",
        )?;
        validate_quarantined_original_against_intent(quarantine, intent)?;
        Ok(())
    });
    if let Err(verification_error) = post_verify {
        let rollback = restore_quarantined_poh_after_failed_verification(
            &options.archive_dir,
            quarantine,
            intent,
            &receipt,
            lock,
            _stop,
        );
        return match rollback {
            Ok(()) => Err(verification_error)
                .context("full post-verification failed; the original PoH sidecar was restored"),
            Err(rollback_error) => Err(anyhow::anyhow!(
                "full post-verification failed ({verification_error:#}); automatic rollback also failed ({rollback_error:#}); the original remains at {}",
                quarantine.original.display()
            )),
        };
    }
    let quarantine_identity = validate_quarantined_original_against_intent(quarantine, intent)?;
    let _ = validate_published_repair_candidate(&poh_path, intent, &receipt).context(
        "published PoH candidate changed after full verification and before success report",
    )?;
    let mut final_report = intent.report.clone();
    final_report.quarantine_device = quarantine_identity.device;
    final_report.quarantine_inode = quarantine_identity.inode;
    final_report.quarantine_identity = Some(quarantine_identity);
    lock.recheck()?;
    validate_repair_authority_unchanged(authority)?;
    publish_repair_json_no_clobber(&quarantine.report, &final_report)?;
    #[cfg(test)]
    match _stop {
        PohRepairStopPoint::AfterReportReplaceQuarantine => {
            let evidence = quarantine
                .directory
                .join("test-report-bound-original-evidence");
            fs::rename(&quarantine.original, &evidence)?;
            let (mut source, _) = open_repair_regular_file(&evidence)?;
            let mut replacement = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
                .open(&quarantine.original)?;
            io::copy(&mut source, &mut replacement)?;
            replacement.flush()?;
            replacement.sync_all()?;
            crate::first_seen_finalization::sync_directory(&quarantine.directory)?;
        }
        PohRepairStopPoint::AfterReportChmodQuarantine => {
            fs::set_permissions(&quarantine.original, fs::Permissions::from_mode(0o644))?;
        }
        PohRepairStopPoint::AfterReportHardlinkQuarantine => {
            fs::hard_link(
                &quarantine.original,
                quarantine
                    .directory
                    .join("test-report-bound-original-extra-link"),
            )?;
            crate::first_seen_finalization::sync_directory(&quarantine.directory)?;
        }
        _ => {}
    }
    lock.recheck()?;
    validate_repair_authority_unchanged(authority)?;
    let _ = validate_published_repair_candidate(&poh_path, intent, &receipt)
        .context("published PoH candidate changed after success report publication")?;
    let quarantine_after_report = validate_quarantined_original_against_intent(quarantine, intent)
        .context("quarantined PoH original changed after success report publication")?;
    anyhow::ensure!(
        quarantine_after_report == quarantine_identity
            && final_report.quarantine_identity == Some(quarantine_identity)
            && final_report.quarantine_device == quarantine_after_report.device
            && final_report.quarantine_inode == quarantine_after_report.inode,
        "quarantined PoH original full identity differs from the identity bound into the published success report"
    );
    Ok(final_report)
}

fn validate_poh_repair_intent(
    options: &PohOrphanTailRepairOptions,
    authority: &PohRepairAuthority,
    quarantine: &PohRepairQuarantine,
    work: &PohOrphanTailRepairWorkIntent,
    intent: &PohOrphanTailRepairIntent,
) -> Result<()> {
    anyhow::ensure!(
        intent.schema_version == 2,
        "unsupported PoH repair intent schema"
    );
    anyhow::ensure!(
        intent.binding == poh_repair_binding(options),
        "durable PoH repair intent does not match the requested repair options"
    );
    let report = &intent.report;
    anyhow::ensure!(
        report.archive == options.archive_dir.display().to_string()
            && report.epoch == options.epoch
            && report.indexed_blocks == authority.indexed_blocks
            && report.indexed_terminal_block_id == authority.terminal_block_id
            && report.indexed_terminal_slot == authority.terminal_slot
            && report.metadata_entries == authority.metadata_entries
            && report.predecessor_tail_present == authority.predecessor_tail_present
            && report.trailing_records_removed == options.expected_trailing_records
            && report.first_removed_block_id == options.expected_first_trailing_block_id
            && report.first_removed_slot == options.expected_first_trailing_slot
            && report.last_removed_slot == options.expected_last_trailing_slot
            && report.old_poh_sha256 == options.expected_old_poh_sha256
            && report.quarantine_device == 0
            && report.quarantine_inode == 0
            && report.quarantine_identity.is_none()
            && intent.binding.expected_predecessor_blockhash
                == options.expected_predecessor_blockhash
            && report.quarantine_path == quarantine.original.display().to_string()
            && report.report_path == quarantine.report.display().to_string(),
        "durable PoH repair intent report does not match the current authority and paths"
    );
    let expected_last_block_id = options
        .expected_first_trailing_block_id
        .checked_add(options.expected_trailing_records - 1)
        .context("trailing block id range overflow")?;
    anyhow::ensure!(
        report.last_removed_block_id == expected_last_block_id
            && report.old_poh_sha256.len() == 64
            && report.new_poh_sha256.len() == 64
            && report.old_poh_bytes > 0
            && report.new_poh_bytes > 0
            && report.old_poh_sha256 != report.new_poh_sha256,
        "durable PoH repair intent has invalid tail or file bindings"
    );
    validate_expected_sha256(&intent.candidate.sha256, "candidate SHA-256")?;
    let candidate_path = poh_repair_candidate_path(&options.archive_dir, &intent.candidate)?;
    anyhow::ensure!(
        intent.candidate.identity.bytes == report.new_poh_bytes
            && intent.candidate.sha256 == report.new_poh_sha256
            && candidate_path == quarantine.candidate
            && intent.candidate.file_name == work.candidate_file_name
            && work.original_identity.device == report.old_poh_device
            && work.original_identity.inode == report.old_poh_inode
            && work.original_identity.bytes == report.old_poh_bytes
            && work.old_poh_sha256 == report.old_poh_sha256,
        "durable PoH repair intent candidate does not match its report"
    );
    Ok(())
}

fn resume_poh_orphan_tail_repair(
    options: &PohOrphanTailRepairOptions,
    authority: &PohRepairAuthority,
    quarantine: &PohRepairQuarantine,
    work: PohOrphanTailRepairWorkIntent,
    intent: PohOrphanTailRepairIntent,
    lock: &PohRepairLockGuard,
    _stop: PohRepairStopPoint,
) -> Result<PohOrphanTailRepairReport> {
    validate_poh_repair_work_intent(options, quarantine, work.original_identity, &work)?;
    validate_poh_repair_intent(options, authority, quarantine, &work, &intent)?;
    lock.recheck()?;
    let poh_path = options.archive_dir.join(ARCHIVE_V2_POH_FILE);
    let (canonical_identity, canonical_bytes, canonical_sha256) = hash_repair_file(&poh_path)?;
    if canonical_sha256 == intent.report.new_poh_sha256
        && canonical_bytes == intent.report.new_poh_bytes
    {
        anyhow::ensure!(
            canonical_identity.device == intent.candidate.identity.device
                && canonical_identity.inode == intent.candidate.identity.inode,
            "published canonical PoH does not have the durable candidate file identity"
        );
        let candidate_path = poh_repair_candidate_path(&options.archive_dir, &intent.candidate)?;
        anyhow::ensure!(
            !optional_repair_plane_exists(&candidate_path)?,
            "published repair still has an unexpected intent-bound candidate temp"
        );
        crate::first_seen_finalization::sync_directory(&options.archive_dir)?;
        validate_quarantined_original_against_intent(quarantine, &intent).with_context(|| {
            format!(
                "published repair has no valid quarantined original at {}",
                quarantine.original.display()
            )
        })?;
        return finish_poh_orphan_tail_repair(options, authority, quarantine, &intent, lock, _stop);
    }
    let restored_old_copy = canonical_sha256 == intent.report.old_poh_sha256
        && canonical_bytes == intent.report.old_poh_bytes
        && (canonical_identity.device != intent.report.old_poh_device
            || canonical_identity.inode != intent.report.old_poh_inode);
    if restored_old_copy {
        validate_quarantined_original_against_intent(quarantine, &intent)?;
        anyhow::ensure!(
            !optional_repair_plane_exists(&quarantine.candidate)?
                && !optional_repair_plane_exists(&quarantine.rollback_temp)?,
            "restored old PoH state still has a repair candidate or rollback temp"
        );
        remove_bound_repair_temp_if_present(&quarantine.original_copy_temp)?;
        lock.recheck()?;
        anyhow::bail!(
            "a prior full post-verification failure restored an incident-bound copy of the original PoH; manual incident review is required"
        );
    }
    anyhow::ensure!(
        canonical_sha256 == intent.report.old_poh_sha256
            && canonical_bytes == intent.report.old_poh_bytes
            && canonical_identity.device == intent.report.old_poh_device
            && canonical_identity.inode == intent.report.old_poh_inode,
        "canonical PoH sidecar matches neither the old nor new SHA-256 in the durable repair intent"
    );
    if !optional_repair_plane_exists(&quarantine.candidate)? {
        if optional_repair_plane_exists(&quarantine.original)? {
            validate_quarantined_original_against_intent(quarantine, &intent)?;
            remove_bound_repair_temp_if_present(&quarantine.original_copy_temp)?;
            remove_bound_repair_temp_if_present(&quarantine.rollback_temp)?;
            anyhow::bail!(
                "a prior full post-verification failure already restored the original PoH; manual incident review is required"
            );
        }
        anyhow::bail!("durable PoH repair intent has no candidate and no completed rollback state");
    }

    let mut migration_temp = open_intent_bound_candidate(&options.archive_dir, &intent.candidate)?;
    // A validated candidate with a durable final intent is recovery state. Ordinary errors after
    // this point must not delete it; only the atomic canonical rename consumes its pathname.
    migration_temp.mark_published();
    anyhow::ensure!(
        poh_migration_file_verified(&options.archive_dir, &migration_temp.path)?,
        "intent-bound repaired PoH candidate does not exactly match the indexed prefix"
    );
    prepare_poh_repair_quarantine(
        &options.archive_dir,
        quarantine,
        canonical_identity,
        &intent.report.old_poh_sha256,
        lock,
        _stop,
    )?;
    lock.recheck()?;
    validate_repair_authority_unchanged(authority)?;
    ensure_canonical_matches_quarantined_original(
        &options.archive_dir,
        quarantine,
        canonical_identity,
    )?;
    validate_quarantined_original_against_intent(quarantine, &intent)?;
    ensure_repair_file_identity(&migration_temp.path, intent.candidate.identity)?;
    lock.recheck()?;
    fs::rename(&migration_temp.path, &poh_path).with_context(|| {
        format!(
            "atomically resume repaired PoH publication {} to {}",
            migration_temp.path.display(),
            poh_path.display()
        )
    })?;
    crate::first_seen_finalization::sync_directory(&options.archive_dir)?;
    lock.recheck()?;
    finish_poh_orphan_tail_repair(options, authority, quarantine, &intent, lock, _stop)
}

pub(crate) fn repair_poh_orphan_tail(
    options: &PohOrphanTailRepairOptions,
) -> Result<PohOrphanTailRepairReport> {
    repair_poh_orphan_tail_inner(options, PohRepairStopPoint::Complete)
}

fn epoch_998_poh_orphan_tail_repair_options(
    archive_dir: &Path,
    threads: usize,
) -> PohOrphanTailRepairOptions {
    PohOrphanTailRepairOptions {
        archive_dir: archive_dir.to_path_buf(),
        epoch: EPOCH_998_POH_ORPHAN_REPAIR_EPOCH,
        expected_indexed_blocks: EPOCH_998_POH_ORPHAN_REPAIR_INDEXED_BLOCKS,
        expected_trailing_records: EPOCH_998_POH_ORPHAN_REPAIR_TRAILING_RECORDS,
        expected_first_trailing_block_id: EPOCH_998_POH_ORPHAN_REPAIR_FIRST_TRAILING_BLOCK_ID,
        expected_first_trailing_slot: EPOCH_998_POH_ORPHAN_REPAIR_FIRST_TRAILING_SLOT,
        expected_last_trailing_slot: EPOCH_998_POH_ORPHAN_REPAIR_LAST_TRAILING_SLOT,
        expected_old_poh_sha256: EPOCH_998_POH_ORPHAN_REPAIR_OLD_POH_SHA256.to_string(),
        expected_predecessor_blockhash: EPOCH_998_POH_ORPHAN_REPAIR_PREDECESSOR_BLOCKHASH
            .to_string(),
        threads,
    }
}

fn update_completion_proof_identity(hasher: &mut Sha256, identity: RepairFileIdentity) {
    hasher.update(identity.bytes.to_le_bytes());
    hasher.update(identity.device.to_le_bytes());
    hasher.update(identity.inode.to_le_bytes());
    hasher.update(identity.modified_secs.to_le_bytes());
    hasher.update(identity.modified_nanos.to_le_bytes());
    hasher.update(identity.changed_secs.to_le_bytes());
    hasher.update(identity.changed_nanos.to_le_bytes());
}

fn update_completion_proof_label(hasher: &mut Sha256, label: &str) {
    hasher.update((label.len() as u64).to_le_bytes());
    hasher.update(label.as_bytes());
}

fn bind_completion_proof_directory(
    hasher: &mut Sha256,
    label: &str,
    path: &Path,
    identity: RepairDirectoryIdentity,
) {
    update_completion_proof_label(hasher, label);
    update_completion_proof_label(hasher, &path.display().to_string());
    hasher.update(identity.device.to_le_bytes());
    hasher.update(identity.inode.to_le_bytes());
    hasher.update(identity.owner.to_le_bytes());
    hasher.update(identity.mode.to_le_bytes());
}

fn bind_completion_proof_json<T>(hasher: &mut Sha256, label: &str, snapshot: &StrictRepairJson<T>) {
    let path = &snapshot.plane.path;
    let identity = snapshot.plane.identity;
    update_completion_proof_label(hasher, label);
    update_completion_proof_label(hasher, &path.display().to_string());
    update_completion_proof_identity(hasher, identity);
    hasher.update((snapshot.bytes.len() as u64).to_le_bytes());
    hasher.update(&snapshot.bytes);
}

fn bind_completion_proof_plane(hasher: &mut Sha256, label: &str, plane: &RepairPlaneIdentity) {
    update_completion_proof_label(hasher, label);
    update_completion_proof_label(hasher, &plane.path.display().to_string());
    update_completion_proof_identity(hasher, plane.identity);
}

impl PohOrphanTailRepairCompletionGuard {
    pub(crate) fn marker_binding(&self) -> &str {
        &self.marker_binding
    }

    /// Recheck every identity while the exact hardened lock remains held. The scheduler calls
    /// this immediately before its marker compare-and-replace.
    pub(crate) fn recheck(&self) -> Result<()> {
        self.lock.recheck()?;
        validate_repair_authority_unchanged(&self.authority)?;
        ensure_repair_file_identity(&self.canonical.path, self.canonical.identity)?;
        ensure_repair_file_identity(
            &self.quarantined_original.path,
            self.quarantined_original.identity,
        )?;
        validate_repair_planes_unchanged(&self.proof_planes)?;
        anyhow::ensure!(
            secure_quarantine_directory_identity(&self.quarantine_root)?
                == self.quarantine_root_identity
                && secure_quarantine_directory_identity(&self.quarantine_directory)?
                    == self.quarantine_directory_identity,
            "PoH repair quarantine directory identity or mode changed after proof validation"
        );
        for path in &self.absent_temp_paths {
            anyhow::ensure!(
                !optional_repair_plane_exists(path)?,
                "completed PoH repair temp appeared after proof validation: {}",
                path.display()
            );
        }
        self.lock.recheck()
    }
}

fn validate_completed_poh_orphan_tail_repair(
    options: &PohOrphanTailRepairOptions,
    required_incident_id: &str,
    completion_check: fn(&Path) -> Result<bool>,
) -> Result<PohOrphanTailRepairCompletionGuard> {
    anyhow::ensure!(
        options.epoch == EPOCH_998_POH_ORPHAN_REPAIR_EPOCH,
        "manual PoH repair completion is restricted to epoch {}",
        EPOCH_998_POH_ORPHAN_REPAIR_EPOCH
    );
    validate_expected_sha256(&options.expected_old_poh_sha256, "expected old PoH SHA-256")?;
    let _ = decode_expected_hash(
        &options.expected_predecessor_blockhash,
        "expected predecessor blockhash",
    )?;

    // Acquire the same hardened lock used by migration and repair before reading any proof or
    // authority plane. The returned guard keeps it held through the scheduler marker CAS.
    let lock = acquire_poh_repair_lock(&options.archive_dir)?;
    lock.recheck()?;
    reject_poh_binding_artifacts_before_lock(&options.archive_dir)?;
    validate_expected_predecessor_before_writes(options)?;
    let authority = validate_poh_repair_authority(options)?;
    lock.recheck()?;

    let binding = poh_repair_binding(options);
    anyhow::ensure!(
        binding.algorithm_revision == POH_ORPHAN_TAIL_REPAIR_ALGORITHM_REVISION,
        "manual PoH repair completion requires the canonical-order repair revision"
    );
    let incident_id = poh_repair_incident_id(&binding);
    anyhow::ensure!(
        incident_id == required_incident_id,
        "manual PoH repair completion does not identify the fixed epoch-998 revision-3 incident"
    );
    let quarantine = poh_repair_quarantine_paths(&options.archive_dir, &binding);
    let quarantine_root_identity = secure_quarantine_directory_identity(&quarantine.root)?;
    let quarantine_directory_identity =
        secure_quarantine_directory_identity(&quarantine.directory)?;

    let work_proof: StrictRepairJson<PohOrphanTailRepairWorkIntent> =
        read_bounded_repair_json_strict(&quarantine.work_intent)?;
    let intent_proof: StrictRepairJson<PohOrphanTailRepairIntent> =
        read_bounded_repair_json_strict(&quarantine.intent)?;
    let receipt_proof: StrictRepairJson<PohOrphanTailRepairPublicationReceipt> =
        read_bounded_repair_json_strict(&quarantine.publication_receipt)?;
    let report_proof: StrictRepairJson<PohOrphanTailRepairReport> =
        read_bounded_repair_json_strict(&quarantine.report)?;
    let work = &work_proof.value;
    let intent = &intent_proof.value;
    let receipt = &receipt_proof.value;
    let final_report = &report_proof.value;

    validate_poh_repair_work_intent(options, &quarantine, work.original_identity, work)?;
    validate_poh_repair_intent(options, &authority, &quarantine, work, intent)?;
    anyhow::ensure!(
        work.binding == binding
            && intent.binding == binding
            && receipt.binding == binding
            && intent.schema_version == 2
            && receipt.schema_version == 1,
        "durable PoH repair work, intent, or publication receipt is not the exact revision-3 incident proof"
    );

    let canonical_path = options.archive_dir.join(ARCHIVE_V2_POH_FILE);
    let canonical_identity = validate_published_repair_candidate(&canonical_path, intent, receipt)?;
    let quarantine_identity = validate_quarantined_original_against_intent(&quarantine, intent)?;
    let mut expected_final_report = intent.report.clone();
    expected_final_report.quarantine_device = quarantine_identity.device;
    expected_final_report.quarantine_inode = quarantine_identity.inode;
    expected_final_report.quarantine_identity = Some(quarantine_identity);
    anyhow::ensure!(
        *final_report == expected_final_report,
        "durable PoH repair success report is not the exact intent- and quarantine-bound final report"
    );
    anyhow::ensure!(
        !final_report.predecessor_tail_present
            && !optional_repair_plane_exists(
                &options
                    .archive_dir
                    .join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
            )?
            && optional_repair_plane_exists(&options.archive_dir.join(BLOCK_TIME_GAP_FILE))?,
        "epoch-998 repair completion requires the certified missing predecessor tail and its bound block-time-gap authority"
    );

    let mut absent_temp_paths = vec![
        quarantine.candidate.clone(),
        quarantine.original_copy_temp.clone(),
        quarantine.rollback_temp.clone(),
    ];
    for path in [
        &quarantine.work_intent,
        &quarantine.intent,
        &quarantine.publication_receipt,
        &quarantine.report,
    ] {
        absent_temp_paths.push(repair_json_publish_temp_path(path)?);
    }
    for path in &absent_temp_paths {
        anyhow::ensure!(
            !optional_repair_plane_exists(path)?,
            "completed PoH repair still has an intent-bound temp: {}",
            path.display()
        );
    }

    let mut proof_hasher = Sha256::new();
    proof_hasher.update(b"blockzilla:poh-orphan-repair-completion:v1\0");
    update_completion_proof_label(&mut proof_hasher, &incident_id);
    update_completion_proof_label(&mut proof_hasher, &intent.report.old_poh_sha256);
    update_completion_proof_label(&mut proof_hasher, &intent.report.new_poh_sha256);
    update_completion_proof_label(
        &mut proof_hasher,
        &intent.binding.expected_predecessor_blockhash,
    );
    let canonical = RepairPlaneIdentity {
        path: canonical_path,
        identity: canonical_identity,
    };
    let quarantined_original = RepairPlaneIdentity {
        path: quarantine.original.clone(),
        identity: quarantine_identity,
    };
    bind_completion_proof_plane(&mut proof_hasher, "canonical", &canonical);
    bind_completion_proof_plane(
        &mut proof_hasher,
        "quarantined-original",
        &quarantined_original,
    );
    bind_completion_proof_directory(
        &mut proof_hasher,
        "quarantine-root",
        &quarantine.root,
        quarantine_root_identity,
    );
    bind_completion_proof_directory(
        &mut proof_hasher,
        "quarantine-directory",
        &quarantine.directory,
        quarantine_directory_identity,
    );
    bind_completion_proof_json(&mut proof_hasher, "work-intent", &work_proof);
    bind_completion_proof_json(&mut proof_hasher, "final-intent", &intent_proof);
    bind_completion_proof_json(&mut proof_hasher, "publication-receipt", &receipt_proof);
    bind_completion_proof_json(&mut proof_hasher, "success-report", &report_proof);
    let proof_planes = vec![
        work_proof.plane.clone(),
        intent_proof.plane.clone(),
        receipt_proof.plane.clone(),
        report_proof.plane.clone(),
    ];
    for plane in &authority.plane_identities {
        bind_completion_proof_plane(&mut proof_hasher, "authority", plane);
    }
    for path in &authority.absent_plane_paths {
        update_completion_proof_label(&mut proof_hasher, "absent-authority");
        update_completion_proof_label(&mut proof_hasher, &path.display().to_string());
    }

    let _ = decode_expected_hash(
        &intent.binding.expected_predecessor_blockhash,
        "repair intent predecessor blockhash",
    )?;
    // The mode-0600, same-euid terminal report is published only after the repair command's full
    // canonical verifier and its post-report identity rechecks succeed. Repeating that multi-hour
    // verifier here would not strengthen the accepted same-euid evidence boundary. This bounded
    // whole-sidecar pass still proves exact index/frame coverage and signature-count totals while
    // the same hardened lock and every certified identity remain pinned.
    anyhow::ensure!(
        completion_check(&options.archive_dir)?,
        "repaired PoH sidecar failed the whole-sidecar completion check"
    );

    let marker_binding = format!(
        "poh_orphan_repair_completion_v1:{}",
        hex32(&proof_hasher.finalize().into())
    );
    let guard = PohOrphanTailRepairCompletionGuard {
        lock,
        authority,
        quarantine_root: quarantine.root,
        quarantine_root_identity,
        quarantine_directory: quarantine.directory,
        quarantine_directory_identity,
        proof_planes,
        canonical,
        quarantined_original,
        absent_temp_paths,
        marker_binding,
    };
    guard.recheck()?;
    Ok(guard)
}

/// Validate the one production revision-3 orphan-tail repair without rewriting `poh.wincode`.
/// The returned value keeps the exact epoch lock held until the scheduler finishes its marker
/// compare-and-replace.
pub(crate) fn validate_epoch_998_poh_orphan_repair_completion(
    archive_dir: &Path,
    threads: usize,
) -> Result<PohOrphanTailRepairCompletionGuard> {
    let options = epoch_998_poh_orphan_tail_repair_options(archive_dir, threads);
    validate_completed_poh_orphan_tail_repair(
        &options,
        EPOCH_998_POH_ORPHAN_REPAIR_REVISION_3_INCIDENT,
        poh_migration_epoch_verified,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, PartialEq, Eq)]
enum PohRepairStopPoint {
    Complete,
    #[cfg(test)]
    AfterIntent,
    #[cfg(test)]
    AfterIntentHardExit,
    #[cfg(test)]
    AfterFinalIntentLinkError,
    #[cfg(test)]
    DuringFinalIntentJsonPublishHardExit,
    #[cfg(test)]
    AfterQuarantine,
    #[cfg(test)]
    AfterAuthorityPathAppearance,
    #[cfg(test)]
    AfterMissingBlockTimeAppearance,
    #[cfg(test)]
    AfterMissingShreddingAppearance,
    #[cfg(test)]
    AfterPublish,
    #[cfg(test)]
    BeforePostVerifyReplaceCanonical,
    #[cfg(test)]
    BeforePostVerifyChmodCanonical,
    #[cfg(test)]
    BeforePostVerifyExtraHardlinkCanonical,
    #[cfg(test)]
    AfterPublishBeforeReceiptHardExit,
    #[cfg(test)]
    AfterPublicationReceiptLinkError,
    #[cfg(test)]
    DuringPublicationReceiptJsonHardExit,
    #[cfg(test)]
    AfterReportReplaceQuarantine,
    #[cfg(test)]
    AfterReportChmodQuarantine,
    #[cfg(test)]
    AfterReportHardlinkQuarantine,
    DuringCandidateWriteHardExit,
    DuringQuarantineCopyHardExit,
    DuringQuarantinePublishHardExit,
    DuringRollbackCopyHardExit,
    AfterRollbackPublishHardExit,
    #[cfg(test)]
    AfterLockPathReplacement,
}

fn repair_poh_orphan_tail_inner(
    options: &PohOrphanTailRepairOptions,
    _stop: PohRepairStopPoint,
) -> Result<PohOrphanTailRepairReport> {
    let started = Instant::now();
    preflight_poh_repair_before_lock(options)?;
    let repair_lock = acquire_poh_repair_lock(&options.archive_dir)?;
    let authority = validate_poh_repair_authority(options)?;
    repair_lock.recheck()?;
    let binding = poh_repair_binding(options);
    let quarantine = poh_repair_quarantine_paths(&options.archive_dir, &binding);
    #[cfg(test)]
    if _stop == PohRepairStopPoint::AfterLockPathReplacement {
        fs::remove_file(&repair_lock.lock_path)?;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&repair_lock.lock_path)?;
    }
    repair_lock.recheck()?;
    if optional_repair_plane_exists(&quarantine.intent)? {
        let _ = secure_quarantine_directory_identity(&quarantine.root)?;
        anyhow::ensure!(
            optional_repair_plane_exists(&quarantine.work_intent)?,
            "durable final PoH repair intent has no matching work intent"
        );
        let work = read_bounded_repair_json(&quarantine.work_intent)?;
        let intent = read_bounded_repair_json(&quarantine.intent)?;
        crate::first_seen_finalization::sync_directory(&quarantine.root)?;
        repair_lock.recheck()?;
        return resume_poh_orphan_tail_repair(
            options,
            &authority,
            &quarantine,
            work,
            intent,
            &repair_lock,
            _stop,
        );
    }
    let poh_path = options.archive_dir.join(ARCHIVE_V2_POH_FILE);
    let (original_identity, old_poh_bytes, old_poh_sha256) = hash_repair_file(&poh_path)?;
    anyhow::ensure!(
        old_poh_sha256 == options.expected_old_poh_sha256,
        "canonical PoH changed after the pre-lock expected-old-SHA check"
    );

    repair_lock.recheck()?;
    ensure_poh_repair_quarantine_root(&options.archive_dir, &quarantine.root)?;
    ensure_repair_path_absent(&quarantine.directory)?;
    let expected_work =
        poh_repair_work_intent(&binding, original_identity, &old_poh_sha256, &quarantine)?;
    if optional_repair_plane_exists(&quarantine.work_intent)? {
        let existing: PohOrphanTailRepairWorkIntent =
            read_bounded_repair_json(&quarantine.work_intent)?;
        validate_poh_repair_work_intent(options, &quarantine, original_identity, &existing)?;
        anyhow::ensure!(
            existing == expected_work,
            "existing durable PoH repair work intent conflicts with the current preflight"
        );
    } else {
        publish_repair_json_no_clobber(&quarantine.work_intent, &expected_work)?;
    }
    repair_lock.recheck()?;

    let (mut migration_temp, stats, tail) =
        rewrite_poh_prefix_for_orphan_repair(options, &authority, &quarantine.candidate, _stop)?;
    anyhow::ensure!(
        stats.blocks_total == authority.indexed_blocks,
        "repaired PoH prefix count changed after preflight"
    );
    anyhow::ensure!(
        stats.transactions == authority.metadata_transactions,
        "repaired PoH prefix transaction total differs from metadata authority"
    );
    anyhow::ensure!(
        poh_migration_file_verified(&options.archive_dir, &migration_temp.path)?,
        "repaired PoH temp does not exactly match the indexed prefix"
    );
    let (candidate_identity, new_poh_bytes, new_poh_sha256) =
        hash_repair_file(&migration_temp.path)?;
    let candidate_file_name = migration_temp
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .context("repaired PoH candidate name is not valid UTF-8")?
        .to_owned();

    repair_lock.recheck()?;
    validate_repair_authority_unchanged(&authority)?;
    ensure_repair_file_identity(&poh_path, original_identity)?;
    let report = PohOrphanTailRepairReport {
        archive: options.archive_dir.display().to_string(),
        epoch: options.epoch,
        indexed_blocks: authority.indexed_blocks,
        indexed_terminal_block_id: authority.terminal_block_id,
        indexed_terminal_slot: authority.terminal_slot,
        predecessor_tail_present: authority.predecessor_tail_present,
        trailing_records_removed: tail.records,
        first_removed_block_id: tail.first_block_id,
        last_removed_block_id: tail.last_block_id,
        first_removed_slot: tail.first_slot,
        last_removed_slot: tail.last_slot,
        blocks_patched: stats.blocks_patched,
        blocks_already_current: stats.blocks_already_current,
        metadata_entries: authority.metadata_entries,
        indexed_poh_entries: stats.entries_total,
        transaction_bearing_poh_entries: stats.transaction_bearing_entries,
        old_poh_bytes,
        new_poh_bytes,
        old_poh_device: original_identity.device,
        old_poh_inode: original_identity.inode,
        quarantine_device: 0,
        quarantine_inode: 0,
        quarantine_identity: None,
        old_poh_sha256,
        new_poh_sha256: new_poh_sha256.clone(),
        quarantine_path: quarantine.original.display().to_string(),
        report_path: quarantine.report.display().to_string(),
        worker_threads: stats.worker_threads,
        elapsed_secs: started.elapsed().as_secs_f64(),
    };
    let intent = PohOrphanTailRepairIntent {
        schema_version: 2,
        binding,
        candidate: PohRepairCandidateBinding {
            file_name: candidate_file_name,
            identity: candidate_identity,
            sha256: new_poh_sha256.clone(),
        },
        report,
    };
    // The durable work intent already owns this exact deterministic candidate path. Transfer
    // cleanup ownership before final-intent publication, because publication can create its
    // no-clobber entry and then fail while syncing or cleaning its deterministic JSON temp.
    migration_temp.mark_published();
    repair_lock.recheck()?;
    let _ = secure_quarantine_directory_identity(&quarantine.root)?;
    let json_fault = match _stop {
        #[cfg(test)]
        PohRepairStopPoint::AfterFinalIntentLinkError => RepairJsonPublishFault::ErrorAfterLink,
        #[cfg(test)]
        PohRepairStopPoint::DuringFinalIntentJsonPublishHardExit => {
            RepairJsonPublishFault::HardExitAfterLink
        }
        _ => RepairJsonPublishFault::None,
    };
    publish_repair_json_no_clobber_with_fault(&quarantine.intent, &intent, json_fault)?;
    repair_lock.recheck()?;
    #[cfg(test)]
    if _stop == PohRepairStopPoint::AfterIntent {
        anyhow::bail!("test stop after durable repair intent");
    }
    #[cfg(test)]
    if _stop == PohRepairStopPoint::AfterIntentHardExit {
        std::process::exit(86);
    }
    prepare_poh_repair_quarantine(
        &options.archive_dir,
        &quarantine,
        original_identity,
        &intent.report.old_poh_sha256,
        &repair_lock,
        _stop,
    )?;
    #[cfg(test)]
    if _stop == PohRepairStopPoint::AfterQuarantine {
        anyhow::bail!("test stop after original quarantine");
    }
    #[cfg(test)]
    if matches!(
        _stop,
        PohRepairStopPoint::AfterAuthorityPathAppearance
            | PohRepairStopPoint::AfterMissingBlockTimeAppearance
            | PohRepairStopPoint::AfterMissingShreddingAppearance
    ) {
        let path = match _stop {
            PohRepairStopPoint::AfterMissingBlockTimeAppearance => {
                options.archive_dir.join(BLOCK_TIME_GAP_FILE)
            }
            PohRepairStopPoint::AfterMissingShreddingAppearance => {
                options.archive_dir.join(ARCHIVE_V2_SHREDDING_FILE)
            }
            _ => authority
                .absent_plane_paths
                .first()
                .context("test fixture has no absent authority path")?
                .clone(),
        };
        anyhow::ensure!(
            authority.absent_plane_paths.contains(&path),
            "test-selected authority path was not absent during preflight"
        );
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
    }
    ensure_canonical_matches_quarantined_original(
        &options.archive_dir,
        &quarantine,
        original_identity,
    )?;
    let (_, quarantined_bytes, quarantined_sha256) = hash_repair_file(&quarantine.original)?;
    anyhow::ensure!(
        quarantined_bytes == intent.report.old_poh_bytes
            && quarantined_sha256 == intent.report.old_poh_sha256,
        "quarantined PoH original does not match its pre-repair bytes and SHA-256"
    );
    validate_repair_authority_unchanged(&authority)?;
    ensure_canonical_matches_quarantined_original(
        &options.archive_dir,
        &quarantine,
        original_identity,
    )?;
    validate_quarantined_original_against_intent(&quarantine, &intent)?;
    ensure_repair_file_identity(&migration_temp.path, intent.candidate.identity)?;
    repair_lock.recheck()?;

    fs::rename(&migration_temp.path, &poh_path).with_context(|| {
        format!(
            "atomically publish repaired PoH sidecar {} to {}",
            migration_temp.path.display(),
            poh_path.display()
        )
    })?;
    migration_temp.mark_published();
    crate::first_seen_finalization::sync_directory(&options.archive_dir)?;
    repair_lock.recheck()?;
    #[cfg(test)]
    if _stop == PohRepairStopPoint::AfterPublish {
        anyhow::bail!("test stop after repaired PoH publication");
    }

    finish_poh_orphan_tail_repair(
        options,
        &authority,
        &quarantine,
        &intent,
        &repair_lock,
        _stop,
    )
}

/// Cheap, read-only completion check for `migrate_poh_signature_counts`: sums each block's PoH
/// entry counts and compares the total with `archive-v2-blocks.index`, without decompressing hot
/// blocks. This does not prove each entry's canonical transaction boundary. The migration itself
/// performs that stronger check for every block; the deep verifier proves it cryptographically.
///
/// A whole-sidecar read, so it is only cheap in the "once per completed job" sense, not the
/// "every poll" sense: the scheduler's `handle_child_exit` calls this once per migration job
/// exit, not once per status poll, to keep its cost from scaling with the size of the epoch
/// backlog.
pub(crate) fn poh_migration_epoch_verified(archive_dir: &Path) -> Result<bool> {
    poh_migration_file_verified(archive_dir, &archive_dir.join(ARCHIVE_V2_POH_FILE))
}

fn poh_migration_file_verified(archive_dir: &Path, poh_path: &Path) -> Result<bool> {
    let index_path = archive_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let index = read_archive_v2_hot_block_index(&index_path)?;

    let poh_file = File::open(poh_path).context("open compact PoH sidecar")?;
    let mut poh_reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(POH_READER_BUFFER_BYTES, poh_file));
    // Every frame in this sidecar shares one schema; probing per-frame would decode a legacy
    // (pre-`signature_count`) sidecar twice on every single block.
    let mut poh_schema = blockzilla_archive_v2::PohRecordSchema::default();

    for (position, row) in index.rows.iter().enumerate() {
        if row.block_id as usize != position {
            return Ok(false);
        }
        let Some((_, poh)) = poh_reader.read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| {
            blockzilla_archive_v2::deserialize_archive_v2_poh_record_with_schema(bytes, &mut poh_schema)
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
        blockzilla_archive_v2::deserialize_archive_v2_poh_record(bytes).map_err(anyhow::Error::from)
    })?;
    Ok(trailing.is_none())
}

pub(crate) fn verify_archive_v2_poh(
    archive_dir: &Path,
    requested_threads: usize,
    max_blocks: Option<u64>,
) -> Result<PohVerificationReport> {
    verify_archive_v2_poh_with_predecessor_policy(archive_dir, requested_threads, max_blocks, None)
}

fn verify_archive_v2_poh_with_predecessor_policy(
    archive_dir: &Path,
    requested_threads: usize,
    max_blocks: Option<u64>,
    predecessor_override: Option<[u8; 32]>,
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
    let predecessor_path = archive_dir.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
    let predecessor = match (
        read_predecessor_hash(&predecessor_path),
        predecessor_override,
    ) {
        (Ok(stored), Some(expected)) => {
            anyhow::ensure!(
                stored == expected,
                "stored predecessor blockhash tail does not match the explicit expected predecessor"
            );
            stored
        }
        (Ok(stored), None) => stored,
        (Err(_), Some(expected))
            if matches!(
                fs::symlink_metadata(&predecessor_path),
                Err(ref metadata_error) if metadata_error.kind() == io::ErrorKind::NotFound
            ) =>
        {
            expected
        }
        (Err(error), _) => return Err(error),
    };

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
    let fast_path_blocks = 0u64;
    let mut compressed_bytes_read = 0u64;
    let mut uncompressed_bytes_decoded = 0u64;
    let mut peak_block_bytes = 0usize;
    let mut peak_entries_per_block = 0usize;
    let mut peak_signatures_per_entry = 0usize;
    let mut poh_decoder = ExactPohRecordDecoder::default();

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
            .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| poh_decoder.decode(bytes))?
            .with_context(|| format!("PoH sidecar ended before block {}", row.block_id))?;
        let poh: WincodeArchiveV2PohRecord = poh;
        anyhow::ensure!(
            poh.block_id == row.block_id && poh.slot == row.slot,
            "PoH record does not match block {} slot {}",
            row.block_id,
            row.slot
        );

        // Deep verification always decodes the hot block. A matching per-block signature total
        // is not enough: each PoH entry must consume the exact transaction range and exact
        // signature count derived from that range.
        work.signature_ranges.clear();
        work.signature_ranges.reserve(poh.entries.len());
        let compressed_start =
            usize::try_from(row.compressed_offset).context("compressed offset exceeds usize")?;
        let compressed_end = compressed_start
            .checked_add(row.compressed_len as usize)
            .context("compressed range overflows usize")?;
        let compressed = block_map
            .get(compressed_start..compressed_end)
            .with_context(|| format!("block {} points outside block file", row.block_id))?;
        let expected_len = row.uncompressed_len as usize;
        uncompressed.clear();
        uncompressed.reserve(expected_len);
        let decoded = if index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0 {
            uncompressed.extend_from_slice(compressed);
            uncompressed.len()
        } else {
            decompressor
                .decompress_to_buffer(compressed, &mut uncompressed)
                .with_context(|| format!("decompress block {} slot {}", row.block_id, row.slot))?
        };
        anyhow::ensure!(
            decoded == expected_len,
            "block {} decoded to {decoded} bytes, expected {expected_len}",
            row.block_id
        );
        let block =
            deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(&uncompressed)
                .with_context(|| format!("decode block {} slot {}", row.block_id, row.slot))?;
        anyhow::ensure!(
            block.header.slot == row.slot && block.tx_count == row.tx_count,
            "block {} header/index mismatch",
            row.block_id
        );
        let externally_rooted_start = if position == 0 {
            false
        } else {
            let previous_slot = index.rows[position - 1].slot;
            anyhow::ensure!(
                previous_slot < row.slot,
                "block {} slot {} follows non-increasing indexed slot {}",
                row.block_id,
                row.slot,
                previous_slot
            );
            if block.header.parent_slot == previous_slot {
                false
            } else {
                anyhow::ensure!(
                    previous_slot < block.header.parent_slot && block.header.parent_slot < row.slot,
                    "block {} slot {} has invalid parent_slot {} after indexed slot {}; \
                     an external PoH root requires previous_slot < parent_slot < current_slot",
                    row.block_id,
                    row.slot,
                    block.header.parent_slot,
                    previous_slot
                );
                true
            }
        };

        work.tx_rows.clear();
        work.tx_rows.extend(block.tx_rows());
        let storage_order_is_canonical = prepare_canonical_poh_signatures(
            row.block_id,
            row.slot,
            &work.tx_rows,
            block_signatures,
            &mut work.canonical_tx_storage_positions,
            &mut work.storage_signature_prefixes,
            &mut work.signature_prefixes,
            &mut work.canonical_signatures,
        )?;
        let block_signature_count = work.signature_prefixes.last().copied().unwrap_or(0);
        anyhow::ensure!(
            work.signature_prefixes.len() == row.tx_count as usize + 1
                && block_signature_count == row.signature_count,
            "block {} transaction/signature accounting mismatch",
            row.block_id
        );

        let mut tx_cursor = 0usize;
        for (entry_index, entry) in poh.entries.iter().enumerate() {
            let tx_end = tx_cursor
                .checked_add(entry.tx_count as usize)
                .context("entry transaction range overflow")?;
            anyhow::ensure!(
                tx_end < work.signature_prefixes.len(),
                "block {} entry {entry_index} consumes transactions beyond block",
                row.block_id,
            );
            let first_signature = work.signature_prefixes[tx_cursor];
            let last_signature = work.signature_prefixes[tx_end];
            let expected_entry_signatures = last_signature
                .checked_sub(first_signature)
                .context("entry signature prefix decreased")?;
            anyhow::ensure!(
                entry.signature_count == expected_entry_signatures,
                "block {} entry {entry_index} signature_count {} != exact hot transaction range count {expected_entry_signatures}",
                row.block_id,
                entry.signature_count
            );
            let byte_start = (first_signature as usize)
                .checked_mul(SIGNATURE_BYTES)
                .context("entry signature offset overflow")?;
            let byte_end = (last_signature as usize)
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

        let expected_blockhash = blockhash_at(&blockhashes, position)?;
        let block_start_hash = if position == 0 {
            predecessor
        } else {
            blockhash_at(&blockhashes, position - 1)?
        };
        if poh.entries.is_empty() {
            external_blockhash_blocks += 1;
        } else {
            if externally_rooted_start {
                external_blockhash_blocks += 1;
            }
            let canonical_block_signatures = if storage_order_is_canonical {
                block_signatures
            } else {
                work.canonical_signatures.as_slice()
            };
            work.entry_jobs.clear();
            let first_verifiable_entry = usize::from(externally_rooted_start);
            work.entry_jobs
                .reserve(poh.entries.len() - first_verifiable_entry);
            for (entry_index, entry) in poh.entries.iter().enumerate().skip(first_verifiable_entry)
            {
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
                            signatures: &canonical_block_signatures
                                [job.signature_start..job.signature_end],
                        };
                        let actual = recompute_entry_hash_reusing_scratch(&entry);
                        (actual != job.expected_hash).then_some(actual)
                    })
                    .enumerate()
                    .filter(|(_, mismatch)| mismatch.is_some())
                    .min_by_key(|(index, _)| *index)
            });
            if let Some((job_index, Some(actual))) = mismatch {
                let entry_index = job_index + first_verifiable_entry;
                let job = &work.entry_jobs[job_index];
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
            let verified_entries = &poh.entries[first_verifiable_entry..];
            entries_verified += verified_entries.len() as u64;
            transactions_consumed += verified_entries
                .iter()
                .map(|entry| u64::from(entry.tx_count))
                .sum::<u64>();
            signatures_hashed += verified_entries
                .iter()
                .map(|entry| u64::from(entry.signature_count))
                .sum::<u64>();
            hashes_recomputed += poh
                .entries
                .iter()
                .skip(first_verifiable_entry)
                .map(|entry| u128::from(entry.num_hashes.max(u64::from(entry.tx_count > 0))))
                .sum::<u128>();
            peak_entries_per_block = peak_entries_per_block.max(poh.entries.len());
        }

        compressed_bytes_read += u64::from(row.compressed_len);
        uncompressed_bytes_decoded += u64::from(row.uncompressed_len);
        peak_block_bytes = peak_block_bytes.max(uncompressed.len());
        blocks_verified += 1;
    }

    if limit == index.rows.len() {
        anyhow::ensure!(
            poh_reader
                .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| poh_decoder.decode(bytes))?
                .is_none(),
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
    let blocks: [[u8; 64]; 2] = [buf[..64].try_into().unwrap(), buf[64..].try_into().unwrap()];
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
    use std::os::unix::fs::PermissionsExt;

    struct PohOrphanRepairFixture {
        parent: PathBuf,
        root: PathBuf,
        old_poh_sha256: String,
        predecessor_blockhash: String,
    }

    impl PohOrphanRepairFixture {
        fn options(&self) -> PohOrphanTailRepairOptions {
            PohOrphanTailRepairOptions {
                archive_dir: self.root.clone(),
                epoch: 998,
                expected_indexed_blocks: 2,
                expected_trailing_records: 5,
                expected_first_trailing_block_id: 2,
                expected_first_trailing_slot: 431_559_125,
                expected_last_trailing_slot: 431_559_129,
                expected_old_poh_sha256: self.old_poh_sha256.clone(),
                expected_predecessor_blockhash: self.predecessor_blockhash.clone(),
                threads: 1,
            }
        }
    }

    impl Drop for PohOrphanRepairFixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.parent).ok();
        }
    }

    fn epoch_998_block_104794_fixture(
        label: &str,
        parent_slot: u64,
        previous_indexed_slot: u64,
    ) -> PathBuf {
        use blockzilla_archive_v2::{ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, ArchiveV2HotTxRow, WincodeArchiveV2PohRecord, write_archive_v2_hot_block_index};
        use blockzilla_compact::CompactPohEntry;

        let decode_hex = |value: &str| {
            value
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect::<Vec<_>>()
        };
        let current_slot = 431_284_886;
        let previous_indexed_hash = decode_expected_hash(
            "0175e187c84ea3a17043bb42df4406c922756f7861c0ba02de66b5504de2a947",
            "block 104793 hash",
        )
        .unwrap();
        let first_entry_hash = decode_expected_hash(
            "6fb70f3014f273c9bb6d9955c968c0054f5134c3c91ac05c050e52ede672723a",
            "block 104794 entry 0 hash",
        )
        .unwrap();
        let first_signature = decode_hex(
            "c047a36d99494b04dcb551c3f4f0f2319e02aabbf3fed732b0585d73339228152\
             ee3631dc6991a3dfb4656e0ba35ec175e7d9dc9fbf0aea985a95619f4b1970b",
        );
        assert_eq!(first_signature.len(), SIGNATURE_BYTES);
        let wrong_root_result = recompute_entry_hash_standalone(&EntryJob {
            start_hash: previous_indexed_hash,
            num_hashes: 17_196,
            transaction_count: 1,
            signatures: &first_signature,
        });
        assert_eq!(
            hex32(&wrong_root_result),
            "7e701f60341baaa0b63730f9fbeaf63e7285e17d6e85989c64a55abb5f802358"
        );
        assert_ne!(wrong_root_result, first_entry_hash);

        let blocks = [
            ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot: previous_indexed_slot,
                    parent_slot: previous_indexed_slot - 1,
                    blockhash_id: 0,
                    previous_blockhash_id: 0,
                    block_time: None,
                    block_height: None,
                    rewards: None,
                },
                tx_count: 0,
                tx_rows: Vec::new(),
                message_bytes: Vec::new(),
                metadata_bytes: Vec::new(),
            },
            ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot: current_slot,
                    parent_slot,
                    blockhash_id: 1,
                    previous_blockhash_id: 0,
                    block_time: None,
                    block_height: None,
                    rewards: None,
                },
                tx_count: 1,
                tx_rows: vec![ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: 0,
                    message_offset: 0,
                    message_len: 0,
                    metadata_offset: 0,
                    metadata_len: 0,
                    signature_count: 1,
                    reserved: [0; 3],
                }],
                message_bytes: Vec::new(),
                metadata_bytes: Vec::new(),
            },
        ];
        let root = poh_migration_verify_fixture_root(label);
        let mut block_bytes = Vec::new();
        let mut rows = Vec::new();
        let mut offset = 0u64;
        for (block_id, block) in blocks.iter().enumerate() {
            let uncompressed =
                wincode::config::serialize(block, blockzilla_primitives::wincode_leb128_config())
                    .unwrap();
            let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
            rows.push(ArchiveV2HotBlockIndexRow {
                block_id: block_id as u32,
                slot: block.header.slot,
                compressed_offset: offset,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: block.tx_count,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: u32::from(block_id == 1),
            });
            offset += compressed.len() as u64;
            block_bytes.extend_from_slice(&compressed);
        }
        std::fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &block_bytes).unwrap();
        write_archive_v2_hot_block_index(
            &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            block_bytes.len() as u64,
            1,
            0,
            &rows,
        )
        .unwrap();
        std::fs::write(root.join(ARCHIVE_V2_SIGNATURES_FILE), &first_signature).unwrap();

        let final_hash = hash_one(&first_entry_hash);
        std::fs::write(
            root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            [previous_indexed_hash.as_slice(), final_hash.as_slice()].concat(),
        )
        .unwrap();
        let mut predecessor_tail = previous_indexed_hash.to_vec();
        predecessor_tail.extend_from_slice(&(previous_indexed_slot - 1).to_le_bytes());
        std::fs::write(
            root.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE),
            predecessor_tail,
        )
        .unwrap();

        let mut writer =
            WincodeLeb128FramedWriter::new(File::create(root.join(ARCHIVE_V2_POH_FILE)).unwrap());
        writer
            .write(&WincodeArchiveV2PohRecord {
                block_id: 0,
                slot: previous_indexed_slot,
                entries: vec![CompactPohEntry {
                    num_hashes: 0,
                    hash: previous_indexed_hash,
                    tx_count: 0,
                    signature_count: 0,
                }],
            })
            .unwrap();
        writer
            .write(&WincodeArchiveV2PohRecord {
                block_id: 1,
                slot: current_slot,
                entries: vec![
                    CompactPohEntry {
                        num_hashes: 17_196,
                        hash: first_entry_hash,
                        tx_count: 1,
                        signature_count: 1,
                    },
                    CompactPohEntry {
                        num_hashes: 1,
                        hash: final_hash,
                        tx_count: 0,
                        signature_count: 0,
                    },
                ],
            })
            .unwrap();
        writer.flush().unwrap();
        root
    }

    fn rewrite_test_poh_records(
        root: &Path,
        update: impl FnOnce(&mut Vec<WincodeArchiveV2PohRecord>),
    ) {
        let path = root.join(ARCHIVE_V2_POH_FILE);
        let mut records = Vec::new();
        let mut reader = WincodeLeb128FramedReader::new(File::open(&path).unwrap());
        while let Some((_, record)) = reader.read::<WincodeArchiveV2PohRecord>().unwrap() {
            records.push(record);
        }
        update(&mut records);
        let mut writer = WincodeLeb128FramedWriter::new(File::create(path).unwrap());
        for record in records {
            writer.write(&record).unwrap();
        }
        writer.flush().unwrap();
    }

    #[test]
    fn canonical_poh_signature_order_maps_storage_slices_by_tx_index() {
        use blockzilla_archive_v2::ArchiveV2HotTxRow;
        use blockzilla_compact::CompactPohEntry;

        let row = |tx_index, signature_count| ArchiveV2HotTxRow {
            tx_index,
            flags: 0,
            message_offset: 0,
            message_len: 0,
            metadata_offset: 0,
            metadata_len: 0,
            signature_count,
            reserved: [0; 3],
        };
        // Storage transaction 1 owns the first two signatures. Canonical transaction 0 owns
        // the last signature. The unequal counts make a storage-order migration observably
        // wrong as well as making the signature byte selection wrong.
        let tx_rows = vec![row(1, 2), row(0, 1)];
        let storage_signatures = [
            [0x11; SIGNATURE_BYTES],
            [0x22; SIGNATURE_BYTES],
            [0x33; SIGNATURE_BYTES],
        ]
        .concat();
        let mut canonical_positions = Vec::new();
        let mut storage_prefixes = Vec::new();
        let mut canonical_prefixes = Vec::new();
        let mut canonical_signatures = Vec::new();
        let storage_order_is_canonical = prepare_canonical_poh_signatures(
            0,
            431_163_662,
            &tx_rows,
            &storage_signatures,
            &mut canonical_positions,
            &mut storage_prefixes,
            &mut canonical_prefixes,
            &mut canonical_signatures,
        )
        .unwrap();
        assert!(!storage_order_is_canonical);
        assert_eq!(canonical_positions, [1, 0]);
        assert_eq!(storage_prefixes, [0, 2, 3]);
        assert_eq!(canonical_prefixes, [0, 1, 3]);
        assert_eq!(
            canonical_signatures,
            [
                [0x33; SIGNATURE_BYTES],
                [0x11; SIGNATURE_BYTES],
                [0x22; SIGNATURE_BYTES],
            ]
            .concat()
        );

        let mut entries = vec![
            CompactPohEntry {
                num_hashes: 1,
                hash: [0; 32],
                tx_count: 1,
                signature_count: 0,
            },
            CompactPohEntry {
                num_hashes: 1,
                hash: [0; 32],
                tx_count: 1,
                signature_count: 0,
            },
        ];
        crate::archive_v2::patch_poh_entry_signature_counts(&mut entries, &tx_rows).unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.signature_count)
                .collect::<Vec<_>>(),
            [1, 2]
        );

        let start_hash = [9; 32];
        let canonical_hash = recompute_entry_hash_standalone(&EntryJob {
            start_hash,
            num_hashes: 1,
            transaction_count: 1,
            signatures: &canonical_signatures[..SIGNATURE_BYTES],
        });
        let storage_first_hash = recompute_entry_hash_standalone(&EntryJob {
            start_hash,
            num_hashes: 1,
            transaction_count: 1,
            signatures: &storage_signatures[..SIGNATURE_BYTES],
        });
        assert_ne!(canonical_hash, storage_first_hash);

        let duplicate_rows = vec![row(1, 2), row(1, 1)];
        let error =
            crate::archive_v2::patch_poh_entry_signature_counts(&mut entries, &duplicate_rows)
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate hot transaction index")
        );
    }

    #[test]
    fn epoch_998_entry_1_release_vector_requires_canonical_tx_zero_signature() {
        let decode_bytes = |value: &str| {
            assert_eq!(value.len() % 2, 0);
            value
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect::<Vec<_>>()
        };
        let start_hash = decode_expected_hash(
            "1f36b1636e2aa96c1ce1df9099e21de5af2113d4611fc23c8cd3f75e9e59cc03",
            "epoch 998 entry 1 start",
        )
        .unwrap();
        let canonical_signature = decode_bytes(
            "91b7030dfaca32b0f047ac4a0694dddcb7d467139781f0b78562afaa8c671f9e\
             fd1889c3fb228618d9f81322193b997fa56407529745ee2a092f6d492a238000",
        );
        let storage_first_signature = decode_bytes(
            "097d7373d82840b9a9c1591032c467e0c4c2505a113e8e7898977bcdb0fb609c\
             2d6955e01c2026afc4be7f878f372a73d6245d90804c97e70f3435acf3d4a309",
        );
        assert_eq!(canonical_signature.len(), SIGNATURE_BYTES);
        assert_eq!(storage_first_signature.len(), SIGNATURE_BYTES);

        let canonical = recompute_entry_hash_standalone(&EntryJob {
            start_hash,
            num_hashes: 55_501,
            transaction_count: 1,
            signatures: &canonical_signature,
        });
        assert_eq!(
            hex32(&canonical),
            "0ff6e750e98b3b7c3339a8459c5bfced7c766d3d619329a3f50c537bf61d1320"
        );

        let storage_first = recompute_entry_hash_standalone(&EntryJob {
            start_hash,
            num_hashes: 55_501,
            transaction_count: 1,
            signatures: &storage_first_signature,
        });
        assert_eq!(
            hex32(&storage_first),
            "66875a750a6b2eae9424843bdb87d33d3e702fb57763a64281d9c5123d5a122d"
        );
        assert_ne!(storage_first, canonical);
    }

    #[test]
    fn deep_poh_verifier_hashes_permuted_hot_rows_in_canonical_tx_order() {
        use blockzilla_archive_v2::{ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, ArchiveV2HotTxRow, WincodeArchiveV2PohRecord, write_archive_v2_hot_block_index};
        use blockzilla_compact::CompactPohEntry;

        let root = poh_migration_verify_fixture_root("poh-canonical-tx-order");
        let slot = 431_163_662;
        let predecessor = [0x09; 32];
        let row = |tx_index, signature_count| ArchiveV2HotTxRow {
            tx_index,
            flags: 0,
            message_offset: 0,
            message_len: 0,
            metadata_offset: 0,
            metadata_len: 0,
            signature_count,
            reserved: [0; 3],
        };
        let hot_block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot,
                parent_slot: slot - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: None,
            },
            tx_count: 2,
            // Signatures follow this storage order: canonical tx 1 first, then canonical tx 0.
            tx_rows: vec![row(1, 2), row(0, 1)],
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
        };
        let uncompressed =
            wincode::config::serialize(&hot_block, blockzilla_primitives::wincode_leb128_config())
                .unwrap();
        let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
        std::fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &compressed).unwrap();
        write_archive_v2_hot_block_index(
            &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed.len() as u64,
            1,
            0,
            &[ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot,
                compressed_offset: 0,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: 2,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 3,
            }],
        )
        .unwrap();

        let storage_signatures = [
            [0x11; SIGNATURE_BYTES],
            [0x22; SIGNATURE_BYTES],
            [0x33; SIGNATURE_BYTES],
        ]
        .concat();
        std::fs::write(root.join(ARCHIVE_V2_SIGNATURES_FILE), &storage_signatures).unwrap();
        let tick_hash = recompute_entry_hash_standalone(&EntryJob {
            start_hash: predecessor,
            num_hashes: 2,
            transaction_count: 0,
            signatures: &[],
        });
        let canonical_tx_0_hash = recompute_entry_hash_standalone(&EntryJob {
            start_hash: tick_hash,
            num_hashes: 3,
            transaction_count: 1,
            signatures: &storage_signatures[SIGNATURE_BYTES * 2..],
        });
        let canonical_tx_1_hash = recompute_entry_hash_standalone(&EntryJob {
            start_hash: canonical_tx_0_hash,
            num_hashes: 1,
            transaction_count: 1,
            signatures: &storage_signatures[..SIGNATURE_BYTES * 2],
        });
        std::fs::write(
            root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            canonical_tx_1_hash,
        )
        .unwrap();
        let mut predecessor_tail = predecessor.to_vec();
        predecessor_tail.extend_from_slice(&(slot - 1).to_le_bytes());
        std::fs::write(
            root.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE),
            predecessor_tail,
        )
        .unwrap();
        let mut poh =
            WincodeLeb128FramedWriter::new(File::create(root.join(ARCHIVE_V2_POH_FILE)).unwrap());
        poh.write(&WincodeArchiveV2PohRecord {
            block_id: 0,
            slot,
            entries: vec![
                CompactPohEntry {
                    num_hashes: 2,
                    hash: tick_hash,
                    tx_count: 0,
                    signature_count: 0,
                },
                CompactPohEntry {
                    num_hashes: 3,
                    hash: canonical_tx_0_hash,
                    tx_count: 1,
                    // Legacy storage-order patch: the total is right, but canonical tx 0 was
                    // incorrectly assigned storage tx 1's two signatures.
                    signature_count: 2,
                },
                CompactPohEntry {
                    num_hashes: 1,
                    hash: canonical_tx_1_hash,
                    tx_count: 1,
                    signature_count: 1,
                },
            ],
        })
        .unwrap();
        poh.flush().unwrap();

        validate_expected_predecessor_before_writes(&PohOrphanTailRepairOptions {
            archive_dir: root.clone(),
            epoch: 998,
            expected_indexed_blocks: 1,
            expected_trailing_records: 1,
            expected_first_trailing_block_id: 1,
            expected_first_trailing_slot: slot + 1,
            expected_last_trailing_slot: slot + 1,
            expected_old_poh_sha256: "00".repeat(32),
            expected_predecessor_blockhash: hex32(&predecessor),
            threads: 1,
        })
        .unwrap();
        let migration = migrate_poh_signature_counts(&root, 1).unwrap();
        assert_eq!(migration.blocks_patched, 1);
        assert_eq!(migration.blocks_already_current, 0);
        let mut migrated_reader = WincodeLeb128FramedReader::new(BufReader::new(
            File::open(root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
        ));
        let mut migrated_decoder = ExactPohRecordDecoder::default();
        let (_, migrated) = migrated_reader
            .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| migrated_decoder.decode(bytes))
            .unwrap()
            .unwrap();
        assert_eq!(
            migrated
                .entries
                .iter()
                .map(|entry| entry.signature_count)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        let report = verify_archive_v2_poh(&root, 1, None).unwrap();
        assert_eq!(report.blocks_verified, 1);
        assert_eq!(report.entries_verified, 3);
        assert_eq!(report.transactions_consumed, 2);
        assert_eq!(report.signatures_hashed, 3);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn epoch_998_block_104794_gap_start_skips_only_entry_zero() {
        let root = epoch_998_block_104794_fixture(
            "epoch-998-block-104794-external-root",
            431_284_885,
            431_269_087,
        );
        let report = verify_archive_v2_poh(&root, 1, None).unwrap();
        assert_eq!(report.blocks_verified, 2);
        assert_eq!(report.external_blockhash_blocks, 1);
        // Block 104793 entry 0 and block 104794 entry 1 are verified. The externally rooted
        // block 104794 entry 0 is the only entry excluded from cryptographic counters.
        assert_eq!(report.entries_verified, 2);
        assert_eq!(report.transactions_consumed, 0);
        assert_eq!(report.signatures_hashed, 0);
        assert_eq!(report.hashes_recomputed, 1);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn contiguous_indexed_parent_still_rejects_block_104794_entry_zero_vector() {
        let root = epoch_998_block_104794_fixture(
            "epoch-998-block-104794-contiguous-parent",
            431_269_087,
            431_269_087,
        );
        let error = verify_archive_v2_poh(&root, 1, None).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("PoH mismatch block=1"), "{error:#}");
        assert!(message.contains("entry=0/2"), "{error:#}");
        assert!(
            message.contains(
                "actual=7e701f60341baaa0b63730f9fbeaf63e7285e17d6e85989c64a55abb5f802358"
            ),
            "{error:#}"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn external_root_classification_rejects_backward_and_future_parent_slots() {
        for (label, parent_slot) in [
            ("backward", 431_269_086),
            ("self", 431_284_886),
            ("future", 431_284_887),
        ] {
            let root = epoch_998_block_104794_fixture(
                &format!("epoch-998-block-104794-invalid-parent-{label}"),
                parent_slot,
                431_269_087,
            );
            let error = verify_archive_v2_poh(&root, 1, None).unwrap_err();
            assert!(
                error.to_string().contains("has invalid parent_slot"),
                "{label}: {error:#}"
            );
            std::fs::remove_dir_all(root).ok();
        }
    }

    #[test]
    fn external_root_classification_rejects_non_increasing_index_slots() {
        for (label, previous_slot) in [
            ("equal-index-slot", 431_284_886),
            ("future-index-slot", 431_284_887),
        ] {
            let root = epoch_998_block_104794_fixture(
                &format!("epoch-998-block-104794-{label}"),
                previous_slot,
                previous_slot,
            );
            let error = verify_archive_v2_poh(&root, 1, None).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("follows non-increasing indexed slot"),
                "{label}: {error:#}"
            );
            std::fs::remove_dir_all(root).ok();
        }
    }

    #[test]
    fn external_root_block_still_rejects_inner_hash_final_hash_and_entry_zero_boundary_faults() {
        for fault in ["entry-one-hash", "final-registry", "entry-zero-boundary"] {
            let root = epoch_998_block_104794_fixture(
                &format!("epoch-998-block-104794-{fault}"),
                431_284_885,
                431_269_087,
            );
            match fault {
                "entry-one-hash" => rewrite_test_poh_records(&root, |records| {
                    records[1].entries[1].hash[0] ^= 1;
                }),
                "final-registry" => {
                    let path = root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
                    let mut registry = std::fs::read(&path).unwrap();
                    *registry.last_mut().unwrap() ^= 1;
                    std::fs::write(path, registry).unwrap();
                }
                "entry-zero-boundary" => rewrite_test_poh_records(&root, |records| {
                    records[1].entries[0].signature_count = 0;
                }),
                _ => unreachable!(),
            }
            let error = verify_archive_v2_poh(&root, 1, None).unwrap_err();
            let expected = match fault {
                "entry-one-hash" => "PoH mismatch block=1 slot=431284886 entry=1/2",
                "final-registry" => "final PoH hash differs from blockhash registry",
                "entry-zero-boundary" => {
                    "entry 0 signature_count 0 != exact hot transaction range count 1"
                }
                _ => unreachable!(),
            };
            assert!(error.to_string().contains(expected), "{fault}: {error:#}");
            std::fs::remove_dir_all(root).ok();
        }
    }

    #[test]
    fn verifier_contract_revision_gets_a_new_repair_incident() {
        let options = PohOrphanTailRepairOptions {
            archive_dir: PathBuf::from("/archives/epoch-998"),
            epoch: 998,
            expected_indexed_blocks: 369_334,
            expected_trailing_records: 5,
            expected_first_trailing_block_id: 369_334,
            expected_first_trailing_slot: 431_559_125,
            expected_last_trailing_slot: 431_559_129,
            expected_old_poh_sha256:
                "b8d64f16f5da7f696cc15611c01575fac106d9e5faa5c9d7bc63ff73c0789eb0".to_string(),
            expected_predecessor_blockhash:
                "c6df7153cc2d9070da2b8f663adbaf4ff0492d80d787b91658ac4ce981af3451".to_string(),
            threads: 1,
        };
        let revised = poh_repair_binding(&options);
        assert_eq!(
            revised.algorithm_revision,
            POH_ORPHAN_TAIL_REPAIR_ALGORITHM_REVISION
        );

        // This is the exact revision-1 incident digest. Revision 3 includes its explicit
        // algorithm revision before the same immutable coordinates.
        let mut legacy = Sha256::new();
        legacy.update(b"blockzilla-poh-orphan-tail-repair-v1\0");
        legacy.update(options.epoch.to_le_bytes());
        legacy.update(options.expected_indexed_blocks.to_le_bytes());
        legacy.update(options.expected_trailing_records.to_le_bytes());
        legacy.update(options.expected_first_trailing_block_id.to_le_bytes());
        legacy.update(options.expected_first_trailing_slot.to_le_bytes());
        legacy.update(options.expected_last_trailing_slot.to_le_bytes());
        legacy.update(options.expected_old_poh_sha256.as_bytes());
        legacy.update(options.expected_predecessor_blockhash.as_bytes());
        let legacy_digest: [u8; 32] = legacy.finalize().into();
        let legacy_incident = hex32(&legacy_digest);
        assert_eq!(
            legacy_incident,
            "2e2cea0c2728af0f5a372f435f03255b301f0fff1689220345116d8aa1088875"
        );
        let revised_incident = poh_repair_incident_id(&revised);
        assert_eq!(
            revised_incident,
            "b59ba7a230d7181b0b6d2b2adadc6a101ff74b143c3187fbd6801aadcb17c435"
        );
        assert_ne!(revised_incident, legacy_incident);

        let mut revision_2 = revised.clone();
        revision_2.algorithm_revision = 2;
        let revision_2_incident = poh_repair_incident_id(&revision_2);
        assert_eq!(
            revision_2_incident,
            "0b0ece2dd1272afa7104aaa4e43b2a1c17e655e81b76c54d2aebe6f697997d4b"
        );
        assert_ne!(revised_incident, revision_2_incident);

        let revised_paths = poh_repair_quarantine_paths(&options.archive_dir, &revised);
        let revision_2_paths = poh_repair_quarantine_paths(&options.archive_dir, &revision_2);
        assert_ne!(revised_paths.directory, revision_2_paths.directory);
    }

    fn poh_orphan_repair_fixture(label: &str) -> PohOrphanRepairFixture {
        use blockzilla_archive_v2::{ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, ArchiveV2HotMetaRecord, ArchiveV2HotTxRow, BLOCK_TIME_GAP_HEADER_LEN, BLOCK_TIME_GAP_MAGIC, BLOCK_TIME_GAP_MISSING_TIME, BLOCK_TIME_GAP_ROW_LEN, BLOCK_TIME_GAP_TIME_THRESHOLD_SECS, BLOCK_TIME_GAP_VERSION, BlockTimeGapHeader, BlockTimeGapSidecar, BlockTimeGapSourceKind, WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Header, WincodeArchiveV2ShreddingRecord, write_archive_v2_hot_block_index, write_block_time_gap_sidecar};
        use blockzilla_compact::CompactPohEntry;

        let parent = poh_migration_verify_fixture_root(label);
        let root = parent.join("epoch-998");
        std::fs::create_dir(&root).unwrap();
        let first_slot = 431_559_123;
        let last_slot = 431_559_124;
        let mut block_bytes = Vec::new();
        let mut rows = Vec::new();
        for (block_id, slot) in [(0, first_slot), (1, last_slot)] {
            let hot_block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot,
                    parent_slot: slot - 1,
                    blockhash_id: block_id + 1,
                    previous_blockhash_id: block_id,
                    block_time: None,
                    block_height: None,
                    rewards: None,
                },
                tx_count: 1,
                tx_rows: vec![ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: 0,
                    message_offset: 0,
                    message_len: 0,
                    metadata_offset: 0,
                    metadata_len: 0,
                    signature_count: 1,
                    reserved: [0; 3],
                }],
                message_bytes: Vec::new(),
                metadata_bytes: Vec::new(),
            };
            let uncompressed =
                wincode::config::serialize(&hot_block, blockzilla_primitives::wincode_leb128_config())
                    .unwrap();
            let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
            rows.push(ArchiveV2HotBlockIndexRow {
                block_id,
                slot,
                compressed_offset: block_bytes.len() as u64,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: 1,
                first_tx_ordinal: u64::from(block_id),
                first_signature_ordinal: u64::from(block_id),
                signature_count: 1,
            });
            block_bytes.extend_from_slice(&compressed);
        }
        std::fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &block_bytes).unwrap();
        write_archive_v2_hot_block_index(
            &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            block_bytes.len() as u64,
            3,
            0,
            &rows,
        )
        .unwrap();

        let signatures = [[3u8; SIGNATURE_BYTES], [4u8; SIGNATURE_BYTES]].concat();
        std::fs::write(root.join(ARCHIVE_V2_SIGNATURES_FILE), &signatures).unwrap();
        let predecessor = [9u8; 32];
        let block_0_hash = recompute_entry_hash(
            &EntryJob {
                start_hash: predecessor,
                num_hashes: 1,
                transaction_count: 1,
                signatures: &signatures[..SIGNATURE_BYTES],
            },
            &mut Vec::new(),
        );
        let block_1_hash = recompute_entry_hash(
            &EntryJob {
                start_hash: block_0_hash,
                num_hashes: 1,
                transaction_count: 1,
                signatures: &signatures[SIGNATURE_BYTES..],
            },
            &mut Vec::new(),
        );
        std::fs::write(
            root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            [block_0_hash.as_slice(), block_1_hash.as_slice()].concat(),
        )
        .unwrap();

        let mut meta =
            WincodeLeb128FramedWriter::new(File::create(root.join(ARCHIVE_V2_META_FILE)).unwrap());
        meta.write(&ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
        }))
        .unwrap();
        meta.write(&ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
            blocks: 2,
            transactions: 2,
            ..WincodeArchiveV2Footer::default()
        }))
        .unwrap();
        meta.flush().unwrap();

        let mut shredding = WincodeLeb128FramedWriter::new(
            File::create(root.join(ARCHIVE_V2_SHREDDING_FILE)).unwrap(),
        );
        for (block_id, slot) in [(0, first_slot), (1, last_slot)] {
            shredding
                .write(&WincodeArchiveV2ShreddingRecord {
                    block_id,
                    slot,
                    shredding: Vec::new(),
                })
                .unwrap();
        }
        shredding.flush().unwrap();

        let mut poh =
            WincodeLeb128FramedWriter::new(File::create(root.join(ARCHIVE_V2_POH_FILE)).unwrap());
        poh.write(&WincodeArchiveV2PohRecord {
            block_id: 0,
            slot: first_slot,
            entries: vec![CompactPohEntry {
                num_hashes: 1,
                hash: block_0_hash,
                tx_count: 1,
                signature_count: 1,
            }],
        })
        .unwrap();
        poh.write(&WincodeArchiveV2PohRecord {
            block_id: 1,
            slot: last_slot,
            entries: vec![CompactPohEntry {
                num_hashes: 1,
                hash: block_1_hash,
                tx_count: 1,
                signature_count: 1,
            }],
        })
        .unwrap();
        for offset in 0..5u32 {
            poh.write(&WincodeArchiveV2PohRecord {
                block_id: 2 + offset,
                slot: 431_559_125 + u64::from(offset),
                entries: Vec::new(),
            })
            .unwrap();
        }
        poh.flush().unwrap();

        let index_bytes = std::fs::read(root.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        let block_bytes = std::fs::read(root.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap();
        let mut source_hasher = Sha256::new();
        let domain = b"blockzilla:block-time-gaps:archive-v2-hot:v1";
        source_hasher.update((domain.len() as u64).to_le_bytes());
        source_hasher.update(domain);
        for (name, bytes) in [
            (
                b"archive-v2-blocks.index".as_slice(),
                index_bytes.as_slice(),
            ),
            (b"archive-v2-blocks.zstd".as_slice(), block_bytes.as_slice()),
        ] {
            source_hasher.update((name.len() as u64).to_le_bytes());
            source_hasher.update(name);
            source_hasher.update((bytes.len() as u64).to_le_bytes());
            source_hasher.update(bytes);
        }
        let gap = BlockTimeGapSidecar {
            header: BlockTimeGapHeader {
                magic: BLOCK_TIME_GAP_MAGIC,
                version: BLOCK_TIME_GAP_VERSION,
                header_len: BLOCK_TIME_GAP_HEADER_LEN as u16,
                row_len: BLOCK_TIME_GAP_ROW_LEN as u16,
                flags: 0,
                epoch: 998,
                slots_per_epoch: 432_000,
                source_kind: BlockTimeGapSourceKind::ArchiveV2Hot,
                time_gap_threshold_secs: BLOCK_TIME_GAP_TIME_THRESHOLD_SECS as u32,
                source_bytes: (index_bytes.len() + block_bytes.len()) as u64,
                source_sha256: source_hasher.finalize().into(),
                block_count: 2,
                gap_count: 0,
                missing_slot_count: 0,
                first_slot,
                first_block_time: BLOCK_TIME_GAP_MISSING_TIME,
                last_slot,
                last_block_time: BLOCK_TIME_GAP_MISSING_TIME,
                timed_gap_count: 0,
                missing_time_gap_count: 0,
                decreasing_time_gap_count: 0,
            },
            rows: Vec::new(),
        };
        let mut gap_file = File::create(root.join(BLOCK_TIME_GAP_FILE)).unwrap();
        write_block_time_gap_sidecar(&mut gap_file, &gap).unwrap();
        gap_file.sync_all().unwrap();

        let (_, _, old_poh_sha256) = hash_repair_file(&root.join(ARCHIVE_V2_POH_FILE)).unwrap();
        PohOrphanRepairFixture {
            parent,
            root,
            old_poh_sha256,
            predecessor_blockhash: hex32(&predecessor),
        }
    }

    fn count_poh_records(path: &Path) -> usize {
        let mut reader = WincodeLeb128FramedReader::new(File::open(path).unwrap());
        let mut count = 0;
        while reader
            .read::<WincodeArchiveV2PohRecord>()
            .unwrap()
            .is_some()
        {
            count += 1;
        }
        count
    }

    fn poh_migration_temp_paths(root: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(root)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.file_name().to_str().is_some_and(|name| {
                    (name.starts_with(&format!(".{ARCHIVE_V2_POH_FILE}.migrate."))
                        || name.starts_with(&format!(".{ARCHIVE_V2_POH_FILE}.orphan-repair.")))
                        && name.ends_with(".tmp")
                })
            })
            .map(|entry| entry.path())
            .collect()
    }

    fn rewrite_fixture_poh(
        fixture: &PohOrphanRepairFixture,
        update: impl FnOnce(&mut Vec<WincodeArchiveV2PohRecord>),
    ) {
        let canonical = fixture.root.join(ARCHIVE_V2_POH_FILE);
        let mut records = Vec::new();
        let mut reader = WincodeLeb128FramedReader::new(File::open(&canonical).unwrap());
        while let Some((_, record)) = reader.read::<WincodeArchiveV2PohRecord>().unwrap() {
            records.push(record);
        }
        update(&mut records);
        let mut writer = WincodeLeb128FramedWriter::new(File::create(canonical).unwrap());
        for record in records {
            writer.write(&record).unwrap();
        }
        writer.flush().unwrap();
    }

    fn add_poh_repair_lookup_planes(fixture: &PohOrphanRepairFixture) {
        use blockzilla_archive_v2::{ArchiveV2BlockAccessIndexRow, ArchiveV2GetBlockIndexRow, write_archive_v2_block_access_index, write_archive_v2_get_block_index};
        let first_slot = 431_559_123;
        let last_slot = 431_559_124;
        let hot_index =
            read_archive_v2_hot_block_index(&fixture.root.join(ARCHIVE_V2_BLOCK_INDEX_FILE))
                .unwrap();
        std::fs::write(fixture.root.join(ARCHIVE_V2_BLOCK_ACCESS_FILE), [5, 6]).unwrap();
        let access = [
            ArchiveV2BlockAccessIndexRow {
                block_id: 0,
                slot: first_slot,
                access_offset: 0,
                access_len: 1,
                tx_count: 1,
                signature_count: 1,
            },
            ArchiveV2BlockAccessIndexRow {
                block_id: 1,
                slot: last_slot,
                access_offset: 1,
                access_len: 1,
                tx_count: 1,
                signature_count: 1,
            },
        ];
        write_archive_v2_block_access_index(
            &fixture.root.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
            2,
            0,
            &access,
        )
        .unwrap();
        let mut get_block =
            vec![ArchiveV2GetBlockIndexRow::missing(); crate::SLOTS_PER_EPOCH as usize];
        for (position, (access_offset, slot)) in
            [(0, first_slot), (1, last_slot)].into_iter().enumerate()
        {
            let hot = &hot_index.rows[position];
            get_block[(slot % crate::SLOTS_PER_EPOCH) as usize] = ArchiveV2GetBlockIndexRow {
                block_offset: hot.compressed_offset,
                block_len: hot.compressed_len,
                access_offset,
                access_len: 1,
            };
        }
        write_archive_v2_get_block_index(
            &fixture.root.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE),
            &get_block,
        )
        .unwrap();

        let registry =
            std::fs::read(fixture.root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)).unwrap();
        let mut v3 = Vec::new();
        v3.extend_from_slice(ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC);
        v3.extend_from_slice(&ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION.to_le_bytes());
        v3.extend_from_slice(&(ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN as u16).to_le_bytes());
        v3.extend_from_slice(&2u64.to_le_bytes());
        for (position, slot) in [first_slot, last_slot].into_iter().enumerate() {
            v3.extend_from_slice(&slot.to_le_bytes());
            v3.extend_from_slice(&registry[position * 32..position * 32 + 32]);
            v3.extend_from_slice(&0i64.to_le_bytes());
        }
        std::fs::write(fixture.root.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE), v3).unwrap();
    }

    #[test]
    fn poh_orphan_tail_repair_succeeds_without_legacy_predecessor_and_is_idempotent() {
        let fixture = poh_orphan_repair_fixture("poh-orphan-repair-success");
        let options = fixture.options();
        let canonical = fixture.root.join(ARCHIVE_V2_POH_FILE);
        let original = std::fs::read(&canonical).unwrap();
        let report = repair_poh_orphan_tail(&options).unwrap();
        assert!(!report.predecessor_tail_present);
        assert_eq!(report.trailing_records_removed, 5);
        assert_eq!(count_poh_records(&canonical), 2);
        assert_eq!(std::fs::read(&report.quarantine_path).unwrap(), original);
        let canonical_identity = capture_repair_file_identity(&canonical).unwrap();
        let quarantine_metadata = fs::symlink_metadata(&report.quarantine_path).unwrap();
        assert_ne!(canonical_identity.inode, quarantine_metadata.ino());
        assert_eq!(quarantine_metadata.nlink(), 1);
        assert_eq!(quarantine_metadata.mode() & 0o077, 0);
        assert_eq!(report.quarantine_inode, quarantine_metadata.ino());
        assert!(Path::new(&report.report_path).is_file());
        assert_ne!(report.old_poh_sha256, report.new_poh_sha256);

        let published = std::fs::read(&canonical).unwrap();
        let mut rerun_options = fixture.options();
        rerun_options.threads = 2;
        let rerun = repair_poh_orphan_tail(&rerun_options).unwrap();
        assert_eq!(rerun.new_poh_sha256, report.new_poh_sha256);
        assert_eq!(std::fs::read(&canonical).unwrap(), published);
    }

    fn validate_fixture_repair_completion(
        options: &PohOrphanTailRepairOptions,
        completion_check: fn(&Path) -> Result<bool>,
    ) -> Result<PohOrphanTailRepairCompletionGuard> {
        let incident = poh_repair_incident_id(&poh_repair_binding(options));
        validate_completed_poh_orphan_tail_repair(options, &incident, completion_check)
    }

    #[test]
    fn repaired_poh_completion_proof_is_read_only_stable_and_repeatable() {
        let fixture = poh_orphan_repair_fixture("poh-repair-completion-proof");
        let options = fixture.options();
        repair_poh_orphan_tail(&options).unwrap();
        let paths = poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&options));
        let canonical = fixture.root.join(ARCHIVE_V2_POH_FILE);
        let tracked = [
            canonical.clone(),
            paths.original.clone(),
            paths.work_intent.clone(),
            paths.intent.clone(),
            paths.publication_receipt.clone(),
            paths.report.clone(),
        ];
        let before = tracked
            .iter()
            .map(|path| capture_repair_file_identity(path).unwrap())
            .collect::<Vec<_>>();
        let before_bytes = std::fs::read(&canonical).unwrap();
        let root_before = secure_quarantine_directory_identity(&paths.root).unwrap();
        let directory_before = secure_quarantine_directory_identity(&paths.directory).unwrap();

        let first =
            validate_fixture_repair_completion(&options, poh_migration_epoch_verified).unwrap();
        let binding = first.marker_binding().to_string();
        first.recheck().unwrap();
        drop(first);
        let second =
            validate_fixture_repair_completion(&options, poh_migration_epoch_verified).unwrap();
        assert_eq!(second.marker_binding(), binding);
        second.recheck().unwrap();
        drop(second);

        assert_eq!(std::fs::read(&canonical).unwrap(), before_bytes);
        assert_eq!(
            tracked
                .iter()
                .map(|path| capture_repair_file_identity(path).unwrap())
                .collect::<Vec<_>>(),
            before
        );
        assert_eq!(
            secure_quarantine_directory_identity(&paths.root).unwrap(),
            root_before
        );
        assert_eq!(
            secure_quarantine_directory_identity(&paths.directory).unwrap(),
            directory_before
        );
    }

    #[test]
    fn repaired_poh_completion_rejects_bad_report_receipt_and_canonical_sha() {
        let receipt_fixture = poh_orphan_repair_fixture("poh-completion-bad-receipt");
        let receipt_options = receipt_fixture.options();
        repair_poh_orphan_tail(&receipt_options).unwrap();
        let receipt_paths = poh_repair_quarantine_paths(
            &receipt_fixture.root,
            &poh_repair_binding(&receipt_options),
        );
        let mut receipt: PohOrphanTailRepairPublicationReceipt =
            read_bounded_repair_json_strict(&receipt_paths.publication_receipt)
                .unwrap()
                .value;
        receipt.sha256 = "00".repeat(32);
        std::fs::write(
            &receipt_paths.publication_receipt,
            serde_json::to_vec_pretty(&receipt).unwrap(),
        )
        .unwrap();
        assert!(
            validate_fixture_repair_completion(&receipt_options, poh_migration_epoch_verified)
                .is_err()
        );

        let report_fixture = poh_orphan_repair_fixture("poh-completion-bad-report");
        let report_options = report_fixture.options();
        repair_poh_orphan_tail(&report_options).unwrap();
        let report_paths =
            poh_repair_quarantine_paths(&report_fixture.root, &poh_repair_binding(&report_options));
        let mut report: PohOrphanTailRepairReport =
            read_bounded_repair_json_strict(&report_paths.report)
                .unwrap()
                .value;
        report.quarantine_inode = report.quarantine_inode.saturating_add(1);
        std::fs::write(
            &report_paths.report,
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        assert!(
            validate_fixture_repair_completion(&report_options, poh_migration_epoch_verified)
                .is_err()
        );

        let sha_fixture = poh_orphan_repair_fixture("poh-completion-bad-canonical-sha");
        let sha_options = sha_fixture.options();
        repair_poh_orphan_tail(&sha_options).unwrap();
        OpenOptions::new()
            .append(true)
            .open(sha_fixture.root.join(ARCHIVE_V2_POH_FILE))
            .unwrap()
            .write_all(b"tamper")
            .unwrap();
        assert!(
            validate_fixture_repair_completion(&sha_options, poh_migration_epoch_verified).is_err()
        );
    }

    #[test]
    fn repaired_poh_completion_rejects_lock_conflict_and_failed_completion_check() {
        let fixture = poh_orphan_repair_fixture("poh-completion-lock-conflict");
        let options = fixture.options();
        repair_poh_orphan_tail(&options).unwrap();
        let held_lock = acquire_poh_migration_lock(&fixture.root).unwrap();
        assert!(
            validate_fixture_repair_completion(&options, poh_migration_epoch_verified).is_err()
        );
        drop(held_lock);

        fn fail_completion_check(_archive: &Path) -> Result<bool> {
            Ok(false)
        }
        let canonical = fixture.root.join(ARCHIVE_V2_POH_FILE);
        let before = capture_repair_file_identity(&canonical).unwrap();
        assert!(validate_fixture_repair_completion(&options, fail_completion_check).is_err());
        assert_eq!(capture_repair_file_identity(&canonical).unwrap(), before);
    }

    #[test]
    fn repaired_poh_completion_requires_terminal_json_and_missing_predecessor_proof() {
        let temp_fixture = poh_orphan_repair_fixture("poh-completion-json-temp");
        let temp_options = temp_fixture.options();
        repair_poh_orphan_tail(&temp_options).unwrap();
        let temp_paths =
            poh_repair_quarantine_paths(&temp_fixture.root, &poh_repair_binding(&temp_options));
        let publish_temp = repair_json_publish_temp_path(&temp_paths.report).unwrap();
        std::fs::write(&publish_temp, b"unfinished").unwrap();
        assert!(
            validate_fixture_repair_completion(&temp_options, poh_migration_epoch_verified)
                .is_err()
        );
        assert_eq!(std::fs::read(&publish_temp).unwrap(), b"unfinished");

        let predecessor_fixture = poh_orphan_repair_fixture("poh-completion-predecessor-appeared");
        let predecessor_options = predecessor_fixture.options();
        repair_poh_orphan_tail(&predecessor_options).unwrap();
        let mut predecessor = decode_expected_hash(
            &predecessor_options.expected_predecessor_blockhash,
            "test predecessor",
        )
        .unwrap()
        .to_vec();
        predecessor.extend_from_slice(&431_559_122u64.to_le_bytes());
        std::fs::write(
            predecessor_fixture
                .root
                .join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE),
            predecessor,
        )
        .unwrap();
        assert!(
            validate_fixture_repair_completion(&predecessor_options, poh_migration_epoch_verified)
                .is_err()
        );
    }

    #[test]
    fn poh_orphan_tail_repair_resumes_each_durable_crash_boundary() {
        for (label, stop) in [
            ("intent", PohRepairStopPoint::AfterIntent),
            ("quarantine", PohRepairStopPoint::AfterQuarantine),
            ("publish", PohRepairStopPoint::AfterPublish),
        ] {
            let fixture = poh_orphan_repair_fixture(&format!("poh-repair-crash-{label}"));
            let options = fixture.options();
            let binding = poh_repair_binding(&options);
            let paths = poh_repair_quarantine_paths(&fixture.root, &binding);
            let original = std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap();
            assert!(repair_poh_orphan_tail_inner(&options, stop).is_err());
            assert!(paths.intent.is_file());
            if stop == PohRepairStopPoint::AfterIntent {
                assert!(!paths.original.exists());
                assert_eq!(
                    std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
                    original
                );
            } else {
                assert_eq!(std::fs::read(&paths.original).unwrap(), original);
            }
            if stop == PohRepairStopPoint::AfterPublish {
                assert_eq!(
                    count_poh_records(&fixture.root.join(ARCHIVE_V2_POH_FILE)),
                    2
                );
                assert!(!paths.report.exists());
            }
            let report = repair_poh_orphan_tail(&options).unwrap();
            assert_eq!(
                count_poh_records(&fixture.root.join(ARCHIVE_V2_POH_FILE)),
                2
            );
            assert!(paths.report.is_file());
            assert_eq!(report.new_poh_sha256.len(), 64);
        }
    }

    #[test]
    fn poh_orphan_tail_repair_intent_allows_a_larger_migrated_candidate() {
        let fixture = poh_orphan_repair_fixture("poh-repair-larger-candidate-resume");
        let options = fixture.options();
        let binding = poh_repair_binding(&options);
        let paths = poh_repair_quarantine_paths(&fixture.root, &binding);
        assert!(repair_poh_orphan_tail_inner(&options, PohRepairStopPoint::AfterIntent).is_err());

        let authority = validate_poh_repair_authority(&options).unwrap();
        let work: PohOrphanTailRepairWorkIntent =
            read_bounded_repair_json(&paths.work_intent).unwrap();
        let mut intent: PohOrphanTailRepairIntent =
            read_bounded_repair_json(&paths.intent).unwrap();
        assert_ne!(intent.report.old_poh_sha256, intent.report.new_poh_sha256);
        let expanded_bytes = intent.report.old_poh_bytes.checked_add(1).unwrap();
        intent.report.new_poh_bytes = expanded_bytes;
        intent.candidate.identity.bytes = expanded_bytes;

        validate_poh_repair_intent(&options, &authority, &paths, &work, &intent).unwrap();
    }

    #[test]
    fn poh_orphan_tail_repair_recovers_final_intent_link_then_sync_error() {
        let fixture = poh_orphan_repair_fixture("poh-repair-final-intent-sync-error");
        let options = fixture.options();
        let paths = poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&options));
        let error =
            repair_poh_orphan_tail_inner(&options, PohRepairStopPoint::AfterFinalIntentLinkError)
                .unwrap_err();
        assert!(
            error.to_string().contains("parent-sync failure"),
            "{error:#}"
        );
        let json_temp = repair_json_publish_temp_path(&paths.intent).unwrap();
        assert!(paths.work_intent.is_file());
        assert!(paths.candidate.is_file());
        assert!(paths.intent.is_file());
        assert!(json_temp.is_file());
        assert_eq!(fs::symlink_metadata(&paths.intent).unwrap().nlink(), 2);
        assert_eq!(fs::symlink_metadata(&json_temp).unwrap().nlink(), 2);
        assert!(!paths.directory.exists());

        let report = repair_poh_orphan_tail(&options).unwrap();
        assert_eq!(report.trailing_records_removed, 5);
        assert!(!paths.candidate.exists());
        assert!(!json_temp.exists());
        assert_eq!(fs::symlink_metadata(&paths.intent).unwrap().nlink(), 1);
        assert_eq!(
            count_poh_records(&fixture.root.join(ARCHIVE_V2_POH_FILE)),
            2
        );
    }

    #[test]
    fn poh_orphan_tail_repair_recovers_publication_receipt_link_then_sync_error() {
        let fixture = poh_orphan_repair_fixture("poh-repair-publication-receipt-sync-error");
        let options = fixture.options();
        let paths = poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&options));
        let error = repair_poh_orphan_tail_inner(
            &options,
            PohRepairStopPoint::AfterPublicationReceiptLinkError,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("parent-sync failure"),
            "{error:#}"
        );
        let receipt_temp = repair_json_publish_temp_path(&paths.publication_receipt).unwrap();
        assert!(!paths.candidate.exists());
        assert!(paths.intent.is_file());
        assert!(paths.original.is_file());
        assert!(paths.publication_receipt.is_file());
        assert!(receipt_temp.is_file());
        assert_eq!(
            fs::symlink_metadata(&paths.publication_receipt)
                .unwrap()
                .nlink(),
            2
        );
        assert_eq!(fs::symlink_metadata(&receipt_temp).unwrap().nlink(), 2);

        let report = repair_poh_orphan_tail(&options).unwrap();
        assert_eq!(report.trailing_records_removed, 5);
        assert!(!receipt_temp.exists());
        assert_eq!(
            fs::symlink_metadata(&paths.publication_receipt)
                .unwrap()
                .nlink(),
            1
        );
        assert_eq!(
            count_poh_records(&fixture.root.join(ARCHIVE_V2_POH_FILE)),
            2
        );
    }

    #[test]
    fn poh_orphan_tail_repair_resumes_after_child_hard_exit_with_stale_temp() {
        const CHILD_ARCHIVE_ENV: &str = "BLOCKZILLA_TEST_POH_REPAIR_HARD_EXIT_ARCHIVE";
        if let Some(root) = std::env::var_os(CHILD_ARCHIVE_ENV) {
            let archive_dir = PathBuf::from(root);
            let (_, _, expected_old_poh_sha256) =
                hash_repair_file(&archive_dir.join(ARCHIVE_V2_POH_FILE)).unwrap();
            let expected_predecessor_blockhash = hex32(&[9u8; 32]);
            let options = PohOrphanTailRepairOptions {
                archive_dir,
                epoch: 998,
                expected_indexed_blocks: 2,
                expected_trailing_records: 5,
                expected_first_trailing_block_id: 2,
                expected_first_trailing_slot: 431_559_125,
                expected_last_trailing_slot: 431_559_129,
                expected_old_poh_sha256,
                expected_predecessor_blockhash,
                threads: 1,
            };
            let _ = repair_poh_orphan_tail_inner(&options, PohRepairStopPoint::AfterIntentHardExit);
            panic!("hard-exit repair stop returned instead of terminating the child");
        }

        let fixture = poh_orphan_repair_fixture("poh-repair-child-hard-exit");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(
                "archive_verify::tests::poh_orphan_tail_repair_resumes_after_child_hard_exit_with_stale_temp",
            )
            .arg("--nocapture")
            .env(CHILD_ARCHIVE_ENV, &fixture.root)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "hard-exit child had unexpected status; stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stale_temps = poh_migration_temp_paths(&fixture.root).len();
        assert_eq!(stale_temps, 1, "hard exit did not preserve the stale temp");
        let binding = poh_repair_binding(&fixture.options());
        let paths = poh_repair_quarantine_paths(&fixture.root, &binding);
        let intent: PohOrphanTailRepairIntent = read_bounded_repair_json(&paths.intent).unwrap();
        let candidate_path = poh_repair_candidate_path(&fixture.root, &intent.candidate).unwrap();
        assert!(candidate_path.is_file());
        let report = repair_poh_orphan_tail(&fixture.options()).unwrap();
        assert_eq!(report.trailing_records_removed, 5);
        assert_eq!(
            count_poh_records(&fixture.root.join(ARCHIVE_V2_POH_FILE)),
            2
        );
        assert!(
            poh_migration_temp_paths(&fixture.root).is_empty(),
            "successful hard-exit resume left its intent-bound candidate temp"
        );
        assert!(!candidate_path.exists());
        assert_eq!(
            capture_repair_file_identity(&fixture.root.join(ARCHIVE_V2_POH_FILE))
                .unwrap()
                .inode,
            intent.candidate.identity.inode
        );
    }

    #[test]
    fn poh_orphan_tail_repair_recovers_all_deterministic_large_copy_crash_windows() {
        const CHILD_ARCHIVE_ENV: &str = "BLOCKZILLA_TEST_POH_REPAIR_BOUND_TEMP_ARCHIVE";
        const CHILD_MODE_ENV: &str = "BLOCKZILLA_TEST_POH_REPAIR_BOUND_TEMP_MODE";
        if let (Some(root), Some(mode)) = (
            std::env::var_os(CHILD_ARCHIVE_ENV),
            std::env::var_os(CHILD_MODE_ENV),
        ) {
            let archive_dir = PathBuf::from(root);
            let (_, _, expected_old_poh_sha256) =
                hash_repair_file(&archive_dir.join(ARCHIVE_V2_POH_FILE)).unwrap();
            let options = PohOrphanTailRepairOptions {
                archive_dir,
                epoch: 998,
                expected_indexed_blocks: 2,
                expected_trailing_records: 5,
                expected_first_trailing_block_id: 2,
                expected_first_trailing_slot: 431_559_125,
                expected_last_trailing_slot: 431_559_129,
                expected_old_poh_sha256,
                expected_predecessor_blockhash: hex32(&[9u8; 32]),
                threads: 1,
            };
            let stop = match mode.to_str().unwrap() {
                "candidate-write" => PohRepairStopPoint::DuringCandidateWriteHardExit,
                "final-intent-json-publish" => {
                    PohRepairStopPoint::DuringFinalIntentJsonPublishHardExit
                }
                "after-publish-before-receipt" => {
                    PohRepairStopPoint::AfterPublishBeforeReceiptHardExit
                }
                "publication-receipt-json" => {
                    PohRepairStopPoint::DuringPublicationReceiptJsonHardExit
                }
                "quarantine-copy" => PohRepairStopPoint::DuringQuarantineCopyHardExit,
                "quarantine-publish" => PohRepairStopPoint::DuringQuarantinePublishHardExit,
                "rollback-copy" => PohRepairStopPoint::DuringRollbackCopyHardExit,
                "rollback-publish" => PohRepairStopPoint::AfterRollbackPublishHardExit,
                other => panic!("unknown hard-exit mode {other}"),
            };
            let _ = repair_poh_orphan_tail_inner(&options, stop);
            panic!("hard-exit repair stop returned instead of terminating the child");
        }

        let test_name = "archive_verify::tests::poh_orphan_tail_repair_recovers_all_deterministic_large_copy_crash_windows";
        for (mode, exit_code) in [
            ("candidate-write", 87),
            ("final-intent-json-publish", 92),
            ("after-publish-before-receipt", 93),
            ("publication-receipt-json", 92),
            ("quarantine-copy", 88),
            ("quarantine-publish", 90),
        ] {
            let fixture = poh_orphan_repair_fixture(&format!("poh-repair-hard-exit-{mode}"));
            let paths =
                poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&fixture.options()));
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(test_name)
                .arg("--nocapture")
                .env(CHILD_ARCHIVE_ENV, &fixture.root)
                .env(CHILD_MODE_ENV, mode)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(exit_code),
                "{mode} child had unexpected status; stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(paths.work_intent.is_file());
            if !matches!(
                mode,
                "after-publish-before-receipt" | "publication-receipt-json"
            ) {
                assert!(paths.candidate.is_file());
            }
            match mode {
                "candidate-write" => {
                    assert!(!paths.intent.exists());
                    assert!(!paths.directory.exists());
                }
                "final-intent-json-publish" => {
                    assert!(paths.intent.is_file());
                    assert!(
                        repair_json_publish_temp_path(&paths.intent)
                            .unwrap()
                            .is_file()
                    );
                    assert_eq!(fs::symlink_metadata(&paths.intent).unwrap().nlink(), 2);
                    assert!(!paths.directory.exists());
                }
                "after-publish-before-receipt" => {
                    assert!(!paths.candidate.exists());
                    assert!(!paths.publication_receipt.exists());
                    assert_eq!(
                        count_poh_records(&fixture.root.join(ARCHIVE_V2_POH_FILE)),
                        2
                    );
                }
                "publication-receipt-json" => {
                    assert!(!paths.candidate.exists());
                    assert!(paths.publication_receipt.is_file());
                    let receipt_temp =
                        repair_json_publish_temp_path(&paths.publication_receipt).unwrap();
                    assert!(receipt_temp.is_file());
                    assert_eq!(
                        fs::symlink_metadata(&paths.publication_receipt)
                            .unwrap()
                            .nlink(),
                        2
                    );
                }
                "quarantine-copy" => {
                    assert!(paths.intent.is_file());
                    assert!(paths.original_copy_temp.is_file());
                    assert!(!paths.original.exists());
                }
                "quarantine-publish" => {
                    assert!(paths.intent.is_file());
                    assert!(paths.original_copy_temp.is_file());
                    assert!(paths.original.is_file());
                    assert_eq!(fs::symlink_metadata(&paths.original).unwrap().nlink(), 2);
                }
                _ => unreachable!(),
            }
            let report = repair_poh_orphan_tail(&fixture.options()).unwrap();
            assert_eq!(report.trailing_records_removed, 5);
            assert_eq!(
                count_poh_records(&fixture.root.join(ARCHIVE_V2_POH_FILE)),
                2
            );
            assert!(!paths.candidate.exists());
            assert!(!paths.original_copy_temp.exists());
            assert!(!paths.rollback_temp.exists());
            assert!(paths.publication_receipt.is_file());
            assert!(
                !repair_json_publish_temp_path(&paths.publication_receipt)
                    .unwrap()
                    .exists()
            );
            assert_eq!(
                fs::symlink_metadata(&paths.publication_receipt)
                    .unwrap()
                    .nlink(),
                1
            );
            assert!(
                !repair_json_publish_temp_path(&paths.intent)
                    .unwrap()
                    .exists()
            );
            assert_eq!(fs::symlink_metadata(&paths.intent).unwrap().nlink(), 1);
            assert_eq!(fs::symlink_metadata(&paths.original).unwrap().nlink(), 1);
        }

        for (mode, exit_code) in [("rollback-copy", 89), ("rollback-publish", 91)] {
            let fixture = poh_orphan_repair_fixture(&format!("poh-repair-hard-exit-{mode}"));
            let options = fixture.options();
            let original = std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap();
            let paths = poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&options));
            let registry_path = fixture.root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
            let mut registry = std::fs::read(&registry_path).unwrap();
            registry[32..].fill(9);
            std::fs::write(registry_path, registry).unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(test_name)
                .arg("--nocapture")
                .env(CHILD_ARCHIVE_ENV, &fixture.root)
                .env(CHILD_MODE_ENV, mode)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(exit_code),
                "{mode} child had unexpected status; stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            if mode == "rollback-copy" {
                assert!(paths.rollback_temp.is_file());
                assert_eq!(
                    count_poh_records(&fixture.root.join(ARCHIVE_V2_POH_FILE)),
                    2
                );
                let error = repair_poh_orphan_tail(&options).unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("original PoH sidecar was restored"),
                    "{error:#}"
                );
            } else {
                assert!(!paths.rollback_temp.exists());
                let error = repair_poh_orphan_tail(&options).unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("prior full post-verification failure"),
                    "{error:#}"
                );
            }
            assert_eq!(
                std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
                original
            );
            assert!(!paths.candidate.exists());
            assert!(!paths.original_copy_temp.exists());
            assert!(!paths.rollback_temp.exists());
            assert!(!paths.report.exists());
            let error = repair_poh_orphan_tail(&options).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("prior full post-verification failure"),
                "{error:#}"
            );
        }
    }

    #[test]
    fn poh_orphan_tail_repair_rejects_wrong_predecessor_and_entry_zero_corruption() {
        let fixture = poh_orphan_repair_fixture("poh-repair-wrong-predecessor");
        let mut options = fixture.options();
        options.expected_predecessor_blockhash = hex32(&[8u8; 32]);
        let error = repair_poh_orphan_tail(&options).unwrap_err();
        assert!(
            error.to_string().contains("expected predecessor blockhash"),
            "{error:#}"
        );
        assert!(!fixture.root.join(POH_ORPHAN_QUARANTINE_DIR).exists());

        let fixture = poh_orphan_repair_fixture("poh-repair-stored-predecessor-conflict");
        let mut tail = Vec::from([8u8; 32]);
        tail.extend_from_slice(&431_559_122u64.to_le_bytes());
        std::fs::write(fixture.root.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE), tail).unwrap();
        let error = repair_poh_orphan_tail(&fixture.options()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("stored predecessor blockhash tail"),
            "{error:#}"
        );
        assert!(!fixture.root.join(POH_MIGRATION_LOCK_FILE).exists());
        assert!(!fixture.root.join(POH_ORPHAN_QUARANTINE_DIR).exists());

        let fixture = poh_orphan_repair_fixture("poh-repair-entry-zero-corrupt");
        rewrite_fixture_poh(&fixture, |records| records[0].entries[0].num_hashes += 1);
        let mut options = fixture.options();
        options.expected_old_poh_sha256 = hash_repair_file(&fixture.root.join(ARCHIVE_V2_POH_FILE))
            .unwrap()
            .2;
        let error = repair_poh_orphan_tail(&options).unwrap_err();
        assert!(
            error.to_string().contains("expected predecessor blockhash"),
            "{error:#}"
        );
        assert!(!fixture.root.join(POH_ORPHAN_QUARANTINE_DIR).exists());
    }

    #[test]
    fn poh_orphan_tail_repair_deep_verifier_rejects_tampered_entry_tx_boundary() {
        let fixture = poh_orphan_repair_fixture("poh-repair-strict-tx-boundary");
        rewrite_fixture_poh(&fixture, |records| {
            // Keep the stored signature total and hash unchanged. Only the positive tx boundary
            // is false; the old verifier fast path accepted this shape.
            records[0].entries[0].tx_count = 2;
        });
        let predecessor =
            decode_expected_hash(&fixture.predecessor_blockhash, "test predecessor").unwrap();
        let error = verify_archive_v2_poh_with_predecessor_policy(
            &fixture.root,
            1,
            Some(1),
            Some(predecessor),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("consumes transactions beyond block"),
            "{error:#}"
        );
    }

    #[test]
    fn poh_orphan_tail_repair_rejects_binding_artifacts_before_lock_creation() {
        for file_name in [
            crate::archive_v2::registry_reprocess::REGISTRY_REPROCESS_RECEIPT_FILE,
            REGISTRY_REPROCESS_RECEIPT_TEMP_FILE,
            ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
        ] {
            let fixture = poh_orphan_repair_fixture(&format!(
                "poh-repair-binding-artifact-{}",
                file_name.replace('.', "-")
            ));
            std::fs::write(fixture.root.join(file_name), b"binding").unwrap();
            let error = repair_poh_orphan_tail(&fixture.options()).unwrap_err();
            assert!(
                error.to_string().contains("receipt or manifest"),
                "{error:#}"
            );
            assert!(!fixture.root.join(POH_MIGRATION_LOCK_FILE).exists());
            assert!(!fixture.root.join(POH_ORPHAN_QUARANTINE_DIR).exists());
            assert!(poh_migration_temp_paths(&fixture.root).is_empty());
        }
    }

    #[test]
    fn poh_orphan_tail_repair_rejects_weak_lock_and_authority_appearance() {
        for label in ["world-write", "multilink"] {
            let fixture = poh_orphan_repair_fixture(&format!("poh-repair-lock-{label}"));
            let lock = fixture.root.join(POH_MIGRATION_LOCK_FILE);
            std::fs::write(&lock, b"").unwrap();
            if label == "world-write" {
                std::fs::set_permissions(&lock, fs::Permissions::from_mode(0o666)).unwrap();
            } else {
                std::fs::hard_link(&lock, fixture.root.join("lock-alias")).unwrap();
            }
            let error = repair_poh_orphan_tail(&fixture.options()).unwrap_err();
            assert!(error.to_string().contains("nlink=1"), "{error:#}");
            assert!(!fixture.root.join(POH_ORPHAN_QUARANTINE_DIR).exists());
        }

        let fixture = poh_orphan_repair_fixture("poh-repair-authority-appearance");
        let original = std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap();
        let error = repair_poh_orphan_tail_inner(
            &fixture.options(),
            PohRepairStopPoint::AfterAuthorityPathAppearance,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("appeared after preflight"),
            "{error:#}"
        );
        assert_eq!(
            std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
            original
        );

        let fixture = poh_orphan_repair_fixture("poh-repair-missing-shredding-appearance");
        std::fs::remove_file(fixture.root.join(ARCHIVE_V2_SHREDDING_FILE)).unwrap();
        let original = std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap();
        let error = repair_poh_orphan_tail_inner(
            &fixture.options(),
            PohRepairStopPoint::AfterMissingShreddingAppearance,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("appeared after preflight"),
            "{error:#}"
        );
        assert_eq!(
            std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
            original
        );

        let fixture = poh_orphan_repair_fixture("poh-repair-missing-gap-appearance");
        std::fs::remove_file(fixture.root.join(BLOCK_TIME_GAP_FILE)).unwrap();
        let mut tail = Vec::from([9u8; 32]);
        tail.extend_from_slice(&431_559_122u64.to_le_bytes());
        std::fs::write(fixture.root.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE), tail).unwrap();
        let original = std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap();
        let error = repair_poh_orphan_tail_inner(
            &fixture.options(),
            PohRepairStopPoint::AfterMissingBlockTimeAppearance,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("appeared after preflight"),
            "{error:#}"
        );
        assert_eq!(
            std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
            original
        );
    }

    #[test]
    fn poh_orphan_tail_repair_rejects_insecure_archive_files_directories_and_replaced_lock() {
        let fixture = poh_orphan_repair_fixture("poh-repair-world-writable-archive");
        std::fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o777)).unwrap();
        let error = repair_poh_orphan_tail(&fixture.options()).unwrap_err();
        assert!(error.to_string().contains("repair directory"), "{error:#}");
        assert!(!fixture.root.join(POH_MIGRATION_LOCK_FILE).exists());

        let fixture = poh_orphan_repair_fixture("poh-repair-world-writable-poh");
        std::fs::set_permissions(
            fixture.root.join(ARCHIVE_V2_POH_FILE),
            fs::Permissions::from_mode(0o666),
        )
        .unwrap();
        let error = repair_poh_orphan_tail(&fixture.options()).unwrap_err();
        assert!(error.to_string().contains("repair file"), "{error:#}");
        assert!(!fixture.root.join(POH_MIGRATION_LOCK_FILE).exists());

        let fixture = poh_orphan_repair_fixture("poh-repair-multilink-poh");
        std::fs::hard_link(
            fixture.root.join(ARCHIVE_V2_POH_FILE),
            fixture.root.join("poh.alias"),
        )
        .unwrap();
        let error = repair_poh_orphan_tail(&fixture.options()).unwrap_err();
        assert!(error.to_string().contains("nlink=1"), "{error:#}");
        assert!(!fixture.root.join(POH_MIGRATION_LOCK_FILE).exists());

        let fixture = poh_orphan_repair_fixture("poh-repair-insecure-quarantine-root");
        let paths =
            poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&fixture.options()));
        std::fs::create_dir(&paths.root).unwrap();
        std::fs::set_permissions(&paths.root, fs::Permissions::from_mode(0o755)).unwrap();
        let error = repair_poh_orphan_tail(&fixture.options()).unwrap_err();
        assert!(
            error.to_string().contains("quarantine directory"),
            "{error:#}"
        );

        let fixture = poh_orphan_repair_fixture("poh-repair-insecure-incident-dir");
        let options = fixture.options();
        assert!(repair_poh_orphan_tail_inner(&options, PohRepairStopPoint::AfterIntent).is_err());
        let paths = poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&options));
        std::fs::create_dir(&paths.directory).unwrap();
        std::fs::set_permissions(&paths.directory, fs::Permissions::from_mode(0o755)).unwrap();
        let error = repair_poh_orphan_tail(&options).unwrap_err();
        assert!(
            error.to_string().contains("quarantine directory"),
            "{error:#}"
        );

        let fixture = poh_orphan_repair_fixture("poh-repair-lock-replacement");
        let error = repair_poh_orphan_tail_inner(
            &fixture.options(),
            PohRepairStopPoint::AfterLockPathReplacement,
        )
        .unwrap_err();
        assert!(error.to_string().contains("held repair lock"), "{error:#}");
        assert!(!fixture.root.join(POH_ORPHAN_QUARANTINE_DIR).exists());
    }

    #[test]
    fn poh_orphan_tail_repair_rejects_wrong_tail_without_publication() {
        let fixture = poh_orphan_repair_fixture("poh-orphan-wrong-tail");
        let mut options = fixture.options();
        let canonical = fixture.root.join(ARCHIVE_V2_POH_FILE);
        let original = std::fs::read(&canonical).unwrap();
        let mut records = Vec::new();
        let mut reader = WincodeLeb128FramedReader::new(File::open(&canonical).unwrap());
        while let Some((_, record)) = reader.read::<WincodeArchiveV2PohRecord>().unwrap() {
            records.push(record);
        }
        records[4].slot += 1;
        let mut writer = WincodeLeb128FramedWriter::new(File::create(&canonical).unwrap());
        for record in records {
            writer.write(&record).unwrap();
        }
        writer.flush().unwrap();
        let changed = std::fs::read(&canonical).unwrap();
        assert_ne!(changed, original);
        options.expected_old_poh_sha256 = hash_repair_file(&canonical).unwrap().2;
        let error = repair_poh_orphan_tail(&options).unwrap_err();
        assert!(
            error.to_string().contains("PoH trailing record"),
            "{error:#}"
        );
        assert_eq!(std::fs::read(&canonical).unwrap(), changed);
    }

    #[test]
    fn poh_orphan_tail_repair_rejects_an_extra_shredding_plane_record() {
        use blockzilla_archive_v2::WincodeArchiveV2ShreddingRecord;
        let fixture = poh_orphan_repair_fixture("poh-orphan-extra-shredding");
        let options = fixture.options();
        let shredding_path = fixture.root.join(ARCHIVE_V2_SHREDDING_FILE);
        let file = OpenOptions::new()
            .append(true)
            .open(&shredding_path)
            .unwrap();
        let mut writer = WincodeLeb128FramedWriter::new(file);
        writer
            .write(&WincodeArchiveV2ShreddingRecord {
                block_id: 2,
                slot: 431_559_125,
                shredding: Vec::new(),
            })
            .unwrap();
        writer.flush().unwrap();
        let canonical = std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap();
        let error = repair_poh_orphan_tail(&options).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("shredding sidecar has records beyond"),
            "{error:#}"
        );
        assert_eq!(
            std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
            canonical
        );
    }

    #[test]
    fn poh_orphan_tail_repair_rejects_stale_block_time_hot_binding() {
        let fixture = poh_orphan_repair_fixture("poh-orphan-stale-block-time-binding");
        let options = fixture.options();
        let gap_path = fixture.root.join(BLOCK_TIME_GAP_FILE);
        let mut gap =
            read_block_time_gap_sidecar(BufReader::new(File::open(&gap_path).unwrap())).unwrap();
        gap.header.source_sha256[0] ^= 1;
        let mut gap_file = File::create(&gap_path).unwrap();
        blockzilla_archive_v2::write_block_time_gap_sidecar(&mut gap_file, &gap).unwrap();
        gap_file.sync_all().unwrap();
        let canonical = std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap();
        let error = repair_poh_orphan_tail(&options).unwrap_err();
        assert!(
            error.to_string().contains("block-time gap source SHA-256"),
            "{error:#}"
        );
        assert_eq!(
            std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
            canonical
        );
    }

    #[test]
    fn poh_orphan_tail_repair_binds_present_get_block_and_blockhash_v3_planes() {
        let fixture = poh_orphan_repair_fixture("poh-orphan-lookup-planes");
        add_poh_repair_lookup_planes(&fixture);
        let report = repair_poh_orphan_tail(&fixture.options()).unwrap();
        assert_eq!(report.trailing_records_removed, 5);

        let fixture = poh_orphan_repair_fixture("poh-orphan-stale-get-block");
        add_poh_repair_lookup_planes(&fixture);
        let get_block_path = fixture.root.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE);
        let mut get_block = std::fs::read(&get_block_path).unwrap();
        let terminal_offset = (431_559_124 % crate::SLOTS_PER_EPOCH) as usize
            * blockzilla_archive_v2::ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN;
        get_block[terminal_offset..terminal_offset + 8].copy_from_slice(&9u64.to_le_bytes());
        std::fs::write(&get_block_path, get_block).unwrap();
        let error = repair_poh_orphan_tail(&fixture.options()).unwrap_err();
        assert!(error.to_string().contains("get-block row"), "{error:#}");

        let fixture = poh_orphan_repair_fixture("poh-orphan-stale-blockhash-v3");
        add_poh_repair_lookup_planes(&fixture);
        let v3_path = fixture.root.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
        let mut v3 = std::fs::read(&v3_path).unwrap();
        v3[ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN + 8] ^= 1;
        std::fs::write(&v3_path, v3).unwrap();
        let error = repair_poh_orphan_tail(&fixture.options()).unwrap_err();
        assert!(error.to_string().contains("blockhash V3 row"), "{error:#}");
    }

    #[test]
    fn poh_orphan_tail_repair_fails_closed_on_conflicting_quarantine() {
        let fixture = poh_orphan_repair_fixture("poh-orphan-conflicting-quarantine");
        let options = fixture.options();
        assert!(repair_poh_orphan_tail_inner(&options, PohRepairStopPoint::AfterIntent).is_err());
        let paths = poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&options));
        std::fs::create_dir(&paths.directory).unwrap();
        std::fs::set_permissions(&paths.directory, fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(&paths.original, b"conflicting quarantine").unwrap();
        let canonical = std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap();
        let error = repair_poh_orphan_tail(&options).unwrap_err();
        assert!(
            error.to_string().contains("not the canonical file object")
                || error.to_string().contains("conflicts"),
            "{error:#}"
        );
        assert_eq!(
            std::fs::read(fixture.root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
            canonical
        );
    }

    #[test]
    fn poh_orphan_tail_repair_rolls_back_a_failed_full_post_verify() {
        let fixture = poh_orphan_repair_fixture("poh-orphan-post-verify-rollback");
        let options = fixture.options();
        let canonical_path = fixture.root.join(ARCHIVE_V2_POH_FILE);
        let original = std::fs::read(&canonical_path).unwrap();
        let registry_path = fixture.root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
        let mut registry = std::fs::read(&registry_path).unwrap();
        registry[32..].fill(9);
        std::fs::write(registry_path, registry).unwrap();
        let error = repair_poh_orphan_tail(&options).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("original PoH sidecar was restored"),
            "{error:#}"
        );
        assert_eq!(std::fs::read(&canonical_path).unwrap(), original);
        let paths = poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&options));
        let canonical_identity = capture_repair_file_identity(&canonical_path).unwrap();
        let quarantine_identity = capture_repair_file_identity(&paths.original).unwrap();
        assert_ne!(canonical_identity.inode, quarantine_identity.inode);
        assert_eq!(
            hash_repair_file(&paths.original).unwrap().2,
            options.expected_old_poh_sha256
        );
        assert!(!paths.candidate.exists());
        assert!(!paths.original_copy_temp.exists());
        assert!(!paths.rollback_temp.exists());
        assert!(!paths.report.exists());
    }

    #[test]
    fn poh_orphan_tail_repair_never_rolls_back_over_unknown_or_insecure_canonical() {
        for (label, stop) in [
            (
                "replacement",
                PohRepairStopPoint::BeforePostVerifyReplaceCanonical,
            ),
            ("chmod", PohRepairStopPoint::BeforePostVerifyChmodCanonical),
            (
                "hardlink",
                PohRepairStopPoint::BeforePostVerifyExtraHardlinkCanonical,
            ),
        ] {
            let fixture = poh_orphan_repair_fixture(&format!("poh-rollback-refuses-{label}"));
            let options = fixture.options();
            let paths = poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&options));
            let canonical = fixture.root.join(ARCHIVE_V2_POH_FILE);
            let error = repair_poh_orphan_tail_inner(&options, stop).unwrap_err();
            assert!(
                error.to_string().contains("automatic rollback also failed"),
                "{label}: {error:#}"
            );
            assert!(paths.intent.is_file());
            assert!(paths.original.is_file());
            assert!(!paths.report.exists());
            assert!(!paths.rollback_temp.exists());
            let intent: PohOrphanTailRepairIntent =
                read_bounded_repair_json(&paths.intent).unwrap();
            assert_eq!(
                hash_repair_file(&paths.original).unwrap().2,
                intent.report.old_poh_sha256
            );
            if label == "replacement" {
                assert_eq!(
                    std::fs::read(&canonical).unwrap(),
                    b"unknown replacement canonical"
                );
                let evidence = paths.directory.join("test-published-candidate-evidence");
                assert_eq!(
                    hash_repair_file(&evidence).unwrap().2,
                    intent.report.new_poh_sha256
                );
            } else {
                assert_eq!(
                    hash_repair_file(&canonical).unwrap().2,
                    intent.report.new_poh_sha256
                );
                let metadata = fs::symlink_metadata(&canonical).unwrap();
                if label == "chmod" {
                    assert_eq!(metadata.mode() & 0o777, 0o644);
                } else {
                    assert_eq!(metadata.nlink(), 2);
                }
            }
        }
    }

    #[test]
    fn poh_orphan_tail_repair_report_binds_full_quarantine_identity_after_publication() {
        for (label, stop) in [
            (
                "replacement",
                PohRepairStopPoint::AfterReportReplaceQuarantine,
            ),
            ("chmod", PohRepairStopPoint::AfterReportChmodQuarantine),
            (
                "hardlink",
                PohRepairStopPoint::AfterReportHardlinkQuarantine,
            ),
        ] {
            let fixture = poh_orphan_repair_fixture(&format!("poh-report-quarantine-race-{label}"));
            let options = fixture.options();
            let paths = poh_repair_quarantine_paths(&fixture.root, &poh_repair_binding(&options));
            let error = repair_poh_orphan_tail_inner(&options, stop).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("after success report publication")
                    || error.to_string().contains("full identity differs"),
                "{label}: {error:#}"
            );
            assert!(paths.report.is_file());
            let report_bytes = std::fs::read(&paths.report).unwrap();
            let report: PohOrphanTailRepairReport =
                read_bounded_repair_json(&paths.report).unwrap();
            let bound_identity = report
                .quarantine_identity
                .expect("final report must bind full quarantine identity");
            assert_eq!(report.quarantine_device, bound_identity.device);
            assert_eq!(report.quarantine_inode, bound_identity.inode);
            assert_eq!(
                hash_repair_file(&paths.original).unwrap().2,
                options.expected_old_poh_sha256
            );
            let current_metadata = fs::symlink_metadata(&paths.original).unwrap();
            if label == "replacement" {
                let current_identity = validate_secure_repair_file(&paths.original).unwrap();
                assert_ne!(current_identity, bound_identity);
                assert_eq!(current_metadata.mode() & 0o777, 0o600);
                assert_eq!(current_metadata.nlink(), 1);
            } else if label == "chmod" {
                assert_eq!(current_metadata.mode() & 0o777, 0o644);
            } else {
                assert_eq!(current_metadata.nlink(), 2);
            }

            let replay_error = repair_poh_orphan_tail(&options).unwrap_err();
            assert!(
                replay_error.to_string().contains("quarantined original")
                    || replay_error.to_string().contains("repair JSON conflicts"),
                "{label} replay: {replay_error:#}"
            );
            assert_eq!(std::fs::read(&paths.report).unwrap(), report_bytes);
        }
    }

    #[test]
    fn normal_poh_migration_still_rejects_the_orphan_tail() {
        let fixture = poh_orphan_repair_fixture("normal-migration-rejects-orphan");
        let canonical_path = fixture.root.join(ARCHIVE_V2_POH_FILE);
        let original = std::fs::read(&canonical_path).unwrap();
        let error = migrate_poh_signature_counts(&fixture.root, 1).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("PoH sidecar has trailing records")
        );
        assert_eq!(std::fs::read(canonical_path).unwrap(), original);
    }

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
        use blockzilla_archive_v2::{ArchiveV2HotBlockIndexRow, write_archive_v2_hot_block_index};
        use blockzilla_compact::CompactPohEntry;

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
            let mut writer = WincodeLeb128FramedWriter::new(
                File::create(root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
            );
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
    fn migrate_poh_signature_counts_patches_batched_blocks_across_worker_threads() {
        use blockzilla_archive_v2::{ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, ArchiveV2HotTxRow, write_archive_v2_hot_block_index};
        use blockzilla_compact::CompactPohEntry;

        let root = poh_migration_verify_fixture_root("poh-migration-migrate");

        // Block 0: two transactions with signature_count 2 and 1 (real total 3), but its PoH
        // sidecar entries below still carry the unmigrated legacy placeholder of 0 -- this block
        // must go through decompression and patching.
        let hot_block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: 100,
                parent_slot: 99,
                blockhash_id: 1,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: None,
            },
            tx_count: 2,
            tx_rows: vec![
                ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: 0,
                    message_offset: 0,
                    message_len: 2,
                    metadata_offset: 0,
                    metadata_len: 1,
                    signature_count: 2,
                    reserved: [0; 3],
                },
                ArchiveV2HotTxRow {
                    tx_index: 1,
                    flags: 0,
                    message_offset: 2,
                    message_len: 3,
                    metadata_offset: 1,
                    metadata_len: 2,
                    signature_count: 1,
                    reserved: [0; 3],
                },
            ],
            message_bytes: vec![10, 11, 12, 13, 14],
            metadata_bytes: vec![20, 21, 22],
        };
        let uncompressed =
            wincode::config::serialize(&hot_block, blockzilla_primitives::wincode_leb128_config())
                .unwrap();
        let compressed = zstd::bulk::compress(&uncompressed, 3).unwrap();
        let empty_hot_block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: 101,
                parent_slot: 100,
                blockhash_id: 2,
                previous_blockhash_id: 1,
                block_time: None,
                block_height: None,
                rewards: None,
            },
            tx_count: 0,
            tx_rows: Vec::new(),
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
        };
        let empty_uncompressed = wincode::config::serialize(
            &empty_hot_block,
            blockzilla_primitives::wincode_leb128_config(),
        )
        .unwrap();
        let empty_compressed = zstd::bulk::compress(&empty_uncompressed, 3).unwrap();
        let mut block_bytes = compressed.clone();
        block_bytes.extend_from_slice(&empty_compressed);
        std::fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &block_bytes).unwrap();

        let rows = vec![
            ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 100,
                compressed_offset: 0,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: 2,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 3,
            },
            // Block 1 is already current. Strict migration still decodes it to prove the exact
            // per-entry boundary, then leaves its sidecar record byte-equivalent.
            ArchiveV2HotBlockIndexRow {
                block_id: 1,
                slot: 101,
                compressed_offset: compressed.len() as u64,
                compressed_len: empty_compressed.len() as u32,
                uncompressed_len: empty_uncompressed.len() as u32,
                tx_count: 0,
                first_tx_ordinal: 2,
                first_signature_ordinal: 3,
                signature_count: 0,
            },
        ];
        write_archive_v2_hot_block_index(
            &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            block_bytes.len() as u64,
            3,
            0,
            &rows,
        )
        .unwrap();

        let mut writer =
            WincodeLeb128FramedWriter::new(File::create(root.join(ARCHIVE_V2_POH_FILE)).unwrap());
        writer
            .write(&WincodeArchiveV2PohRecord {
                block_id: 0,
                slot: 100,
                entries: vec![
                    CompactPohEntry {
                        num_hashes: 1,
                        hash: [0; 32],
                        tx_count: 1,
                        signature_count: 0,
                    },
                    CompactPohEntry {
                        num_hashes: 1,
                        hash: [1; 32],
                        tx_count: 1,
                        signature_count: 0,
                    },
                ],
            })
            .unwrap();
        writer
            .write(&WincodeArchiveV2PohRecord {
                block_id: 1,
                slot: 101,
                entries: vec![CompactPohEntry {
                    num_hashes: 1,
                    hash: [0; 32],
                    tx_count: 0,
                    signature_count: 0,
                }],
            })
            .unwrap();
        writer.flush().unwrap();

        let report = migrate_poh_signature_counts(&root, 2).unwrap();
        assert_eq!(report.blocks_total, 2);
        assert_eq!(report.blocks_patched, 1);
        assert_eq!(report.blocks_already_current, 1);

        let mut reader =
            WincodeLeb128FramedReader::new(File::open(root.join(ARCHIVE_V2_POH_FILE)).unwrap());
        let (_, migrated_block_0) = reader
            .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| {
                blockzilla_archive_v2::deserialize_archive_v2_poh_record(bytes)
                    .map_err(anyhow::Error::from)
            })
            .unwrap()
            .unwrap();
        let migrated_block_0: WincodeArchiveV2PohRecord = migrated_block_0;
        assert_eq!(migrated_block_0.entries[0].signature_count, 2);
        assert_eq!(migrated_block_0.entries[1].signature_count, 1);

        let (_, migrated_block_1) = reader
            .read_bytes_with_limit(MAX_POH_FRAME_BYTES, |bytes| {
                blockzilla_archive_v2::deserialize_archive_v2_poh_record(bytes)
                    .map_err(anyhow::Error::from)
            })
            .unwrap()
            .unwrap();
        let migrated_block_1: WincodeArchiveV2PohRecord = migrated_block_1;
        assert_eq!(migrated_block_1.entries[0].signature_count, 0);

        assert!(poh_migration_epoch_verified(&root).unwrap());

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn poh_migration_lock_rejects_contender_without_touching_published_sidecar() {
        use blockzilla_archive_v2::{ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, write_archive_v2_hot_block_index};
        use blockzilla_compact::CompactPohEntry;

        const CHILD_ARCHIVE_ENV: &str = "BLOCKZILLA_TEST_POH_MIGRATION_LOCK_ARCHIVE";
        const CHILD_SENTINEL: &str = "poh-migration-lock-contention-observed";
        if let Some(root) = std::env::var_os(CHILD_ARCHIVE_ENV) {
            let error = migrate_poh_signature_counts(Path::new(&root), 1).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("another PoH signature-count migration already holds archive lock"),
                "unexpected contention error: {error:#}"
            );
            println!("{CHILD_SENTINEL}");
            return;
        }

        let root = poh_migration_verify_fixture_root("poh-migration-lock");
        // Strict migration verifies even already-current blocks before publishing.
        let hot_block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: 100,
                parent_slot: 99,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: None,
            },
            tx_count: 0,
            tx_rows: Vec::new(),
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
        };
        let uncompressed =
            wincode::config::serialize(&hot_block, blockzilla_primitives::wincode_leb128_config())
                .unwrap();
        let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
        std::fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &compressed).unwrap();
        write_archive_v2_hot_block_index(
            &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed.len() as u64,
            0,
            0,
            &[ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 100,
                compressed_offset: 0,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: 0,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 0,
            }],
        )
        .unwrap();
        let mut writer =
            WincodeLeb128FramedWriter::new(File::create(root.join(ARCHIVE_V2_POH_FILE)).unwrap());
        writer
            .write(&WincodeArchiveV2PohRecord {
                block_id: 0,
                slot: 100,
                entries: vec![CompactPohEntry {
                    num_hashes: 1,
                    hash: [0; 32],
                    tx_count: 0,
                    signature_count: 0,
                }],
            })
            .unwrap();
        writer.flush().unwrap();

        // Publish once, then hold the same archive lock while a separate process attempts the
        // migration. A process boundary is intentional: it exercises the OS lock instead of
        // relying on platform-specific same-process `flock` semantics.
        migrate_poh_signature_counts(&root, 1).unwrap();
        let published = std::fs::read(root.join(ARCHIVE_V2_POH_FILE)).unwrap();
        let lock = acquire_poh_migration_lock(&root).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(
                "archive_verify::tests::poh_migration_lock_rejects_contender_without_touching_published_sidecar",
            )
            .arg("--nocapture")
            .env(CHILD_ARCHIVE_ENV, &root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "contending migration child failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(CHILD_SENTINEL),
            "the child test did not execute its lock-contention branch:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read(root.join(ARCHIVE_V2_POH_FILE)).unwrap(),
            published,
            "contending migration changed the already-published canonical sidecar"
        );

        drop(lock);
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
