//! Generation-safe first-seen to usage-sorted registry migration.
//!
//! The migration intentionally reads an already committed Compact-V2 generation and writes a
//! separate generation.  It never mutates or hard-links the source.  Registry-independent
//! sidecars are reflinked when the host filesystem supports copy-on-write and byte-copied
//! otherwise.

use anyhow::{Context, Result, anyhow, bail, ensure};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN, ARCHIVE_V2_BLOCK_ACCESS_INDEX_MAGIC,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN, ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ArchiveV2BlockAccessBlob, ArchiveV2BlockAccessBlockhash, ArchiveV2BlockAccessIndexRow,
    ArchiveV2BlockAccessPubkey, ArchiveV2GetBlockIndexRow, ArchiveV2HotBlockBlob,
    ArchiveV2HotBlockHeader, ArchiveV2HotMessagePayload, ArchiveV2HotMetaRecord,
    ArchiveV2HotRewards, BLOCK_TIME_GAP_FILE, CompactInnerInstructions, CompactLogStream,
    CompactMetaV1, CompactPubkey, CompactReturnData, CompactReward, CompactShredding,
    CompactTokenBalance, CompactTransactionError, KeyIndex, Leb128, LogEvent,
    WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION, WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS,
    WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY, WINCODE_ARCHIVE_V2_FLAG_LEB128,
    WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Header,
    WincodeLeb128FramedWriter,
    program_logs::{
        ProgramLog,
        system_program::{PubkeyOrString, SystemAddress, SystemProgramLog},
        token_2022::Token2022Log,
    },
    read_archive_v2_block_access_index, read_archive_v2_hot_block_index, wincode_leb128_config,
    write_archive_v2_block_access_index, write_archive_v2_get_block_index,
    write_archive_v2_hot_block_index,
};
use memmap2::{Mmap, MmapOptions};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::Instant,
};
use tracing::info;
use wincode::{SchemaRead, SchemaWrite};

use crate::ProgressTracker;

fn bounded_wincode_config<const LIMIT: usize>() -> impl wincode::config::Config {
    wincode::config::Configuration::default()
        .with_preallocation_size_limit::<LIMIT>()
        .with_int_encoding::<Leb128>()
}

pub(crate) const REGISTRY_REPROCESS_RECEIPT_FILE: &str =
    "archive-v2-registry-reprocess.receipt.json";
const RECEIPT_VERSION: u32 = 1;
const RECEIPT_ALGORITHM: &str = "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v1";
const RECEIPT_MAX_BYTES: u64 = 8 << 20;
const MANIFEST_MAX_BYTES: u64 = 64 << 10;
const MAX_HOT_BLOCK_FRAME_BYTES: u64 = 512 << 20;
const MAX_HOT_BLOCK_FRAME_BYTES_USIZE: usize = MAX_HOT_BLOCK_FRAME_BYTES as usize;
// Limits aggregate advertised input plus decompressed bytes admitted to one parallel batch.
const HOT_BATCH_MEMORY_BUDGET_BYTES: u64 = 512 << 20;
const MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK: usize = 4 << 20;
const HOT_UNCOMPRESSED_WORKING_SET_MULTIPLIER: u64 = 5;
#[allow(dead_code)] // Retained for compatibility decoder tests and forensic tooling.
const MAX_ACCESS_FRAME_BYTES_USIZE: usize = ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES as usize;
const MAX_META_FRAME_BYTES: usize = 64 << 20;
const IO_BUFFER_BYTES: usize = 8 << 20;
const SORT_RECORD_BYTES: usize = 40;
const SORT_RUN_MAGIC: &[u8; 8] = b"BZRSRUN1";
const SEMANTIC_DOMAIN: &[u8] = b"blockzilla.registry-reprocess.semantic.v1";
const GENERATION_DOMAIN: &[u8] = b"blockzilla.registry-reprocess.generation.v1";
const REPROCESS_CHECKPOINT_FILE: &str = ".archive-v2-registry-reprocess.checkpoint.json";
const SOURCE_REGISTRY_SNAPSHOT_FILE: &str = ".source-registry.snapshot";
const REPROCESS_CHECKPOINT_VERSION: u32 = 1;

/// Options for one immutable-generation registry migration.
#[derive(Debug, Clone)]
pub(crate) struct RegistryReprocessOptions {
    pub(crate) source_dir: PathBuf,
    pub(crate) target_dir: PathBuf,
    pub(crate) epoch: u64,
    pub(crate) threads: usize,
    /// Hard cap for the external sort's auxiliary in-memory record chunk.  This does not include
    /// the required O(source keys) count/remap vector or bounded block worker buffers.
    pub(crate) sort_memory_mib: usize,
    pub(crate) level: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileBinding {
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SemanticBinding {
    pub(crate) blocks: u64,
    pub(crate) transactions: u64,
    pub(crate) pubkey_references: u64,
    pub(crate) reference_sha256: String,
    pub(crate) normalized_structure_sha256: String,
}

/// Publication-last proof binding the exact source and target generations and their semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegistryReprocessReceipt {
    pub(crate) version: u32,
    pub(crate) algorithm: String,
    pub(crate) epoch: u64,
    pub(crate) threads: usize,
    pub(crate) sort_memory_mib: usize,
    pub(crate) level: i32,
    pub(crate) source_anchor_sha256: String,
    pub(crate) source_dir: String,
    pub(crate) target_dir: String,
    pub(crate) source_generation_sha256: String,
    pub(crate) target_generation_sha256: String,
    pub(crate) source_files: BTreeMap<String, FileBinding>,
    pub(crate) target_files: BTreeMap<String, FileBinding>,
    pub(crate) source_registry_keys: u64,
    pub(crate) target_registry_keys: u64,
    pub(crate) eligible_references: u64,
    pub(crate) source_semantics: SemanticBinding,
    pub(crate) target_semantics: SemanticBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReprocessCheckpoint {
    version: u32,
    algorithm: String,
    source_dir: String,
    target_dir: String,
    epoch: u64,
    threads: usize,
    sort_memory_mib: usize,
    level: i32,
    source_anchor_sha256: String,
}

#[derive(Debug)]
struct SourceManifest {
    registry_keys: u64,
    references: u64,
}

struct MappedRegistry {
    _file: File,
    mmap: Mmap,
    len: usize,
}

impl MappedRegistry {
    fn open(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options
            .open(path)
            .with_context(|| format!("open immutable registry {}", path.display()))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("stat {}", path.display()))?;
        ensure!(
            metadata.file_type().is_file(),
            "registry is not a regular file: {}",
            path.display()
        );
        let bytes = metadata.len();
        ensure!(
            bytes > 0 && bytes.is_multiple_of(32),
            "registry {} has invalid byte length {bytes}; expected a non-zero multiple of 32",
            path.display()
        );
        let keys = bytes / 32;
        ensure!(
            keys <= u64::from(u32::MAX),
            "registry {} has {keys} keys, exceeding the u32 ID space",
            path.display()
        );
        let len = usize::try_from(keys).context("registry key count exceeds usize")?;
        // SAFETY: the file is held open for the mapping lifetime and only read through this type.
        let mmap = unsafe { MmapOptions::new().map(&file) }
            .with_context(|| format!("mmap {}", path.display()))?;
        Ok(Self {
            _file: file,
            mmap,
            len,
        })
    }

    #[inline]
    fn key(&self, id: u32) -> Result<[u8; 32]> {
        ensure!(id != 0, "compact pubkey uses reserved ID 0");
        let index = usize::try_from(id - 1).context("pubkey ID exceeds usize")?;
        let start = index
            .checked_mul(32)
            .context("registry byte offset overflow")?;
        let bytes = self
            .mmap
            .get(start..start + 32)
            .ok_or_else(|| anyhow!("pubkey registry ID {id} is outside 1..={} ", self.len))?;
        Ok(bytes
            .try_into()
            .expect("registry key slice has exact length"))
    }

    fn keys(&self) -> &[[u8; 32]] {
        // SAFETY: `[u8; 32]` has alignment one, every bit-pattern is valid, and the mapping's
        // length was checked to be exactly `len * 32` above.
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr().cast::<[u8; 32]>(), self.len) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReferenceClass {
    Eligible,
    Excluded,
}

#[derive(Debug)]
struct SemanticAccumulator {
    reference: Sha256,
    structure: Sha256,
    blocks: u64,
    transactions: u64,
    pubkey_references: u64,
}

impl SemanticAccumulator {
    fn new() -> Self {
        let mut reference = Sha256::new();
        reference.update(SEMANTIC_DOMAIN);
        reference.update(b".references");
        let mut structure = Sha256::new();
        structure.update(SEMANTIC_DOMAIN);
        structure.update(b".structure");
        Self {
            reference,
            structure,
            blocks: 0,
            transactions: 0,
            pubkey_references: 0,
        }
    }

    fn push(&mut self, block: &BlockSemantic) -> Result<()> {
        ensure!(
            block.block_id == self.blocks,
            "semantic block order mismatch: got {}, expected {}",
            block.block_id,
            self.blocks
        );
        self.reference.update(block.block_id.to_le_bytes());
        self.reference.update(block.slot.to_le_bytes());
        self.reference.update(block.references.to_le_bytes());
        self.reference.update(block.reference_sha256);
        self.structure.update(block.block_id.to_le_bytes());
        self.structure.update(block.slot.to_le_bytes());
        self.structure.update(block.normalized_len.to_le_bytes());
        self.structure.update(block.normalized_sha256);
        self.blocks = self
            .blocks
            .checked_add(1)
            .context("semantic block count overflow")?;
        self.transactions = self
            .transactions
            .checked_add(u64::from(block.transactions))
            .context("semantic transaction count overflow")?;
        self.pubkey_references = self
            .pubkey_references
            .checked_add(block.references)
            .context("semantic pubkey reference count overflow")?;
        Ok(())
    }

    fn finish(self) -> SemanticBinding {
        SemanticBinding {
            blocks: self.blocks,
            transactions: self.transactions,
            pubkey_references: self.pubkey_references,
            reference_sha256: hex_digest(self.reference.finalize()),
            normalized_structure_sha256: hex_digest(self.structure.finalize()),
        }
    }
}

#[derive(Debug)]
struct BlockSemantic {
    block_id: u64,
    slot: u64,
    transactions: u32,
    references: u64,
    reference_sha256: [u8; 32],
    normalized_len: u64,
    normalized_sha256: [u8; 32],
}

#[derive(Debug)]
struct CompressedBlockInput {
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    bytes: Vec<u8>,
    signatures: Option<Vec<u8>>,
}

#[derive(Debug)]
struct SourceBlockAnalysis {
    eligible: Vec<(u32, u32)>,
    all: Vec<(u32, u32)>,
    semantic: BlockSemantic,
}

#[derive(Debug)]
struct RewrittenBlock {
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    compressed: Vec<u8>,
    uncompressed_len: u32,
    semantic: BlockSemantic,
    access: Option<Vec<u8>>,
}

struct AccessBuildContext {
    blockhashes: Vec<[u8; 32]>,
    previous_tail: Vec<super::PreviousBlockhash>,
    vote_hashes: Vec<super::VoteHashRegistryRow>,
}

#[derive(Debug, Clone, Copy)]
struct SortRecord {
    count: u32,
    key: [u8; 32],
    old_id: u32,
}

impl SortRecord {
    fn cmp_canonical(&self, other: &Self) -> Ordering {
        other
            .count
            .cmp(&self.count)
            .then_with(|| self.key.cmp(&other.key))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct HeapRecord {
    record: SortRecord,
    run: usize,
}

impl Ord for HeapRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .cmp_canonical(&self.record)
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for HeapRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SortRecord {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count && self.key == other.key && self.old_id == other.old_id
    }
}

impl Eq for SortRecord {}

struct SortRunReader {
    reader: BufReader<File>,
    remaining: u64,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyCompactMetaV1 {
    err: Option<Vec<u8>>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<CompactInnerInstructions>>,
    logs: Option<CompactLogStream>,
    pre_token_balances: Vec<CompactTokenBalance>,
    post_token_balances: Vec<CompactTokenBalance>,
    rewards: Vec<CompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data: Option<CompactReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyHotBlockWithShredding {
    header: LegacyHotBlockHeaderWithShredding,
    tx_count: u32,
    tx_rows: Vec<blockzilla_format::ArchiveV2HotTxRow>,
    message_bytes: Vec<u8>,
    metadata_bytes: Vec<u8>,
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyHotBlockHeaderWithShredding {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    shredding: Vec<CompactShredding>,
    rewards: Option<ArchiveV2HotRewards>,
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyHotBlockWithRewardsVec {
    header: LegacyHotBlockHeaderWithRewardsVec,
    tx_count: u32,
    tx_rows: Vec<blockzilla_format::ArchiveV2HotTxRow>,
    message_bytes: Vec<u8>,
    metadata_bytes: Vec<u8>,
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyHotBlockHeaderWithRewardsVec {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    rewards: Vec<CompactReward>,
}

impl From<LegacyHotBlockWithShredding> for ArchiveV2HotBlockBlob {
    fn from(value: LegacyHotBlockWithShredding) -> Self {
        Self {
            header: ArchiveV2HotBlockHeader {
                slot: value.header.slot,
                parent_slot: value.header.parent_slot,
                blockhash_id: value.header.blockhash_id,
                previous_blockhash_id: value.header.previous_blockhash_id,
                block_time: value.header.block_time,
                block_height: value.header.block_height,
                rewards: value.header.rewards,
            },
            tx_count: value.tx_count,
            tx_rows: value.tx_rows,
            message_bytes: value.message_bytes,
            metadata_bytes: value.metadata_bytes,
        }
    }
}

impl From<LegacyHotBlockWithRewardsVec> for ArchiveV2HotBlockBlob {
    fn from(value: LegacyHotBlockWithRewardsVec) -> Self {
        let rewards = (!value.header.rewards.is_empty()).then_some(ArchiveV2HotRewards {
            num_partitions: None,
            decoded: value.header.rewards,
        });
        Self {
            header: ArchiveV2HotBlockHeader {
                slot: value.header.slot,
                parent_slot: value.header.parent_slot,
                blockhash_id: value.header.blockhash_id,
                previous_blockhash_id: value.header.previous_blockhash_id,
                block_time: value.header.block_time,
                block_height: value.header.block_height,
                rewards,
            },
            tx_count: value.tx_count,
            tx_rows: value.tx_rows,
            message_bytes: value.message_bytes,
            metadata_bytes: value.metadata_bytes,
        }
    }
}

impl TryFrom<LegacyCompactMetaV1> for CompactMetaV1 {
    type Error = anyhow::Error;

    fn try_from(value: LegacyCompactMetaV1) -> Result<Self> {
        let err = value
            .err
            .as_deref()
            .map(CompactTransactionError::from_stored_wincode_bytes)
            .transpose()?;
        Ok(Self {
            err,
            fee: value.fee,
            pre_balances: value.pre_balances,
            post_balances: value.post_balances,
            inner_instructions: value.inner_instructions,
            logs: value.logs,
            pre_token_balances: value.pre_token_balances,
            post_token_balances: value.post_token_balances,
            rewards: value.rewards,
            loaded_writable_addresses: value.loaded_writable_addresses,
            loaded_readonly_addresses: value.loaded_readonly_addresses,
            return_data: value.return_data,
            compute_units_consumed: value.compute_units_consumed,
            cost_units: value.cost_units,
        })
    }
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyBlockAccessBlobV1 {
    version: u16,
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    signature_counts: Vec<u8>,
    signatures: Vec<u8>,
    pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
    blockhashes: Vec<ArchiveV2BlockAccessBlockhash>,
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyBlockAccessBlobV2NoVotes {
    version: u16,
    flags: u32,
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    signature_counts: Vec<u8>,
    signatures: Vec<u8>,
    pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
    blockhashes: Vec<ArchiveV2BlockAccessBlockhash>,
}

struct StagingGuard {
    path: PathBuf,
    armed: bool,
}

impl StagingGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StagingGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Reprocess one committed first-seen generation and atomically publish a fresh usage-sorted
/// generation.  A matching, already-published target is deep-validated and returned unchanged;
/// any other existing target fails closed and is never overwritten.
pub(crate) fn reprocess_first_seen_registry(
    options: &RegistryReprocessOptions,
) -> Result<RegistryReprocessReceipt> {
    let started = Instant::now();
    validate_options(options)?;
    let source_dir = fs::canonicalize(&options.source_dir)
        .with_context(|| format!("canonicalize {}", options.source_dir.display()))?;
    ensure!(
        source_dir.is_dir(),
        "source is not a directory: {}",
        source_dir.display()
    );
    let target_dir = canonical_target_path(&options.target_dir)?;
    ensure!(
        source_dir != target_dir
            && !target_dir.starts_with(&source_dir)
            && !source_dir.starts_with(&target_dir),
        "source and target generations must be distinct, non-nested directories"
    );
    let target_parent = target_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| anyhow!("target has no parent: {}", target_dir.display()))?;
    fs::create_dir_all(target_parent)
        .with_context(|| format!("create target parent {}", target_parent.display()))?;
    let _lock = acquire_reprocess_lock(&source_dir, &target_dir, options)?;
    if target_dir.exists() {
        let receipt = validate_published_reprocess(&source_dir, &target_dir, options.epoch)
            .with_context(|| {
                format!(
                    "registry reprocess target already exists but is not the exact valid published result: {}",
                    target_dir.display()
                )
            })?;
        info!(
            source = %source_dir.display(),
            target = %target_dir.display(),
            epoch = options.epoch,
            "reused deep-validated immutable registry reprocess target"
        );
        return Ok(receipt);
    }
    let checkpoint = build_checkpoint(&source_dir, &target_dir, options)?;
    let staging = prepare_staging(&target_dir, &checkpoint)?;
    let mut staging_guard = StagingGuard::new(staging.clone());

    info!(
        source = %source_dir.display(),
        target = %target_dir.display(),
        epoch = options.epoch,
        threads = options.threads,
        sort_memory_mib = options.sort_memory_mib,
        zstd_level = options.level,
        "starting Compact-V2 registry reprocess"
    );

    let source_validation_started = Instant::now();
    let manifest = read_source_manifest(&source_dir)?;
    let source_registry_path = source_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let source_registry_snapshot_path = staging.join(SOURCE_REGISTRY_SNAPSHOT_FILE);
    let _snapshot_copy_binding =
        clone_or_copy_file(&source_registry_path, &source_registry_snapshot_path)?;
    let source_registry_metadata = regular_file_metadata(&source_registry_path)?;
    ensure!(
        regular_file_metadata(&source_registry_snapshot_path)?.len()
            == source_registry_metadata.len(),
        "private source registry snapshot has the wrong length"
    );
    let source_registry = MappedRegistry::open(&source_registry_snapshot_path)?;
    let source_registry_keys = source_registry.len as u64;
    validate_source_registry_index(&source_dir, &source_registry)?;
    ensure!(
        manifest.registry_keys == source_registry.len as u64,
        "first-seen manifest registry_keys={} but registry.bin has {} keys",
        manifest.registry_keys,
        source_registry.len
    );
    let mut source_counts = read_registry_counts(
        &source_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
        source_registry.len,
    )?;
    let source_count_sum = source_counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("source registry reference count overflow")
    })?;
    ensure!(
        source_count_sum == manifest.references,
        "first-seen manifest references={} but registry_counts sum={source_count_sum}",
        manifest.references
    );

    let source_meta = validate_and_rewrite_meta(&source_dir, &staging)?;
    let source_blocks_path = source_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let source_index_path = source_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let hot_index = read_archive_v2_hot_block_index(&source_index_path)?;
    validate_hot_index(&source_blocks_path, &hot_index, options.epoch)?;
    ensure!(
        source_meta.blocks == hot_index.rows.len() as u64,
        "metadata footer blocks={} but hot index has {} rows",
        source_meta.blocks,
        hot_index.rows.len()
    );
    let total_transactions = hot_index
        .rows
        .last()
        .map(|row| {
            row.first_tx_ordinal
                .checked_add(u64::from(row.tx_count))
                .context("final transaction ordinal overflow")
        })
        .transpose()?
        .unwrap_or(0);
    let total_signatures = hot_index
        .rows
        .last()
        .map(|row| {
            row.first_signature_ordinal
                .checked_add(u64::from(row.signature_count))
                .context("final signature ordinal overflow")
        })
        .transpose()?
        .unwrap_or(0);
    ensure!(
        source_meta.transactions == total_transactions,
        "metadata footer transactions={} but hot index covers {total_transactions}",
        source_meta.transactions
    );
    let signature_bytes = total_signatures
        .checked_mul(64)
        .context("signature sidecar byte length overflow")?;
    let signatures_metadata = regular_file_metadata(&source_dir.join(ARCHIVE_V2_SIGNATURES_FILE))?;
    ensure!(
        signatures_metadata.len() == signature_bytes,
        "signatures sidecar has {} bytes but index declares {total_signatures} signatures ({signature_bytes} bytes)",
        signatures_metadata.len()
    );
    info!(
        elapsed_secs = source_validation_started.elapsed().as_secs_f64(),
        blocks = hot_index.rows.len(),
        transactions = total_transactions,
        signatures = total_signatures,
        source_registry_keys = source_registry.len,
        source_reference_count = manifest.references,
        compressed_bytes = hot_index.blob_file_bytes,
        "validated first-seen source generation"
    );

    let total_progress = (hot_index.rows.len() as u64)
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .context("registry reprocess progress total overflow")?;
    let mut progress = ProgressTracker::new("registry reprocess");
    progress.set_estimated_total_blocks(total_progress);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.threads)
        .thread_name(|index| format!("registry-reprocess-{index}"))
        .build()
        .context("build registry reprocess worker pool")?;
    let batch_size = options.threads.saturating_mul(2).clamp(1, 64);
    let mut source_blocks = File::open(&source_blocks_path)
        .with_context(|| format!("open {}", source_blocks_path.display()))?;
    let mut source_blocks_hash = Sha256::new();
    let mut eligible_counts = vec![0u32; source_registry.len];
    let mut source_semantics = SemanticAccumulator::new();
    let mut input_bytes_done = 0u64;
    let pass1_started = Instant::now();

    let mut batch_start = 0usize;
    while batch_start < hot_index.rows.len() {
        let batch_end = hot_batch_end(&hot_index.rows, batch_start, batch_size, false)?;
        let rows = &hot_index.rows[batch_start..batch_end];
        let inputs =
            read_compressed_block_batch(&mut source_blocks, rows, Some(&mut source_blocks_hash))?;
        input_bytes_done = input_bytes_done
            .checked_add(
                inputs
                    .iter()
                    .map(|item| item.bytes.len() as u64)
                    .sum::<u64>(),
            )
            .context("pass1 input byte count overflow")?;
        let analyses = pool.install(|| {
            inputs
                .into_par_iter()
                .map(|input| analyze_source_block(input, &source_registry))
                .collect::<Result<Vec<_>>>()
        })?;
        for analysis in analyses {
            merge_count_runs(&mut eligible_counts, &analysis.eligible, false)?;
            merge_count_runs(&mut source_counts, &analysis.all, true)?;
            source_semantics.push(&analysis.semantic)?;
            progress.update_input_bytes(input_bytes_done);
            progress.update(1, u64::from(analysis.semantic.transactions));
        }
        batch_start = batch_end;
    }
    ensure!(
        source_counts.iter().all(|&remaining| remaining == 0),
        "registry_counts.bin does not exactly match typed CompactPubkey references"
    );
    drop(source_counts);
    let source_semantics = source_semantics.finish();
    ensure!(
        source_semantics.pubkey_references == manifest.references,
        "first-seen semantic traversal found {} references, manifest declares {}",
        source_semantics.pubkey_references,
        manifest.references
    );
    info!(
        elapsed_secs = pass1_started.elapsed().as_secs_f64(),
        blocks = source_semantics.blocks,
        transactions = source_semantics.transactions,
        typed_references = source_semantics.pubkey_references,
        compressed_bytes = input_bytes_done,
        "completed registry reprocess pass 1"
    );

    let eligible_references = eligible_counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("eligible reference count overflow")
    })?;
    let sort_started = Instant::now();
    let (old_to_new, target_registry_keys) = build_usage_sorted_registry(
        &source_registry,
        &eligible_counts,
        &staging,
        options.sort_memory_mib,
        &pool,
    )?;
    info!(
        elapsed_secs = sort_started.elapsed().as_secs_f64(),
        source_registry_keys = source_registry.len,
        target_registry_keys,
        eligible_references,
        auxiliary_sort_memory_mib = options.sort_memory_mib,
        "built canonical usage-sorted registry"
    );
    drop(eligible_counts);
    build_registry_index(&staging)?;
    let target_registry = MappedRegistry::open(&staging.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let access_context =
        load_access_build_context(&source_dir, hot_index.rows.len(), options.epoch)?;
    let mut source_signatures = access_context
        .as_ref()
        .map(|_| open_regular_read(&source_dir.join(ARCHIVE_V2_SIGNATURES_FILE)).map(|pair| pair.0))
        .transpose()?;

    source_blocks.seek(SeekFrom::Start(0))?;
    let target_blocks_path = staging.join(ARCHIVE_V2_BLOCKS_FILE);
    let mut target_blocks = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        File::create(&target_blocks_path)
            .with_context(|| format!("create {}", target_blocks_path.display()))?,
    );
    let mut target_rows = Vec::with_capacity(hot_index.rows.len());
    let mut target_access_writer = access_context
        .as_ref()
        .map(|_| {
            File::create(staging.join(ARCHIVE_V2_BLOCK_ACCESS_FILE))
                .map(|file| BufWriter::with_capacity(IO_BUFFER_BYTES, file))
        })
        .transpose()?;
    let mut target_access_rows =
        Vec::with_capacity(access_context.as_ref().map_or(0, |_| hot_index.rows.len()));
    let mut target_access_offset = 0u64;
    let mut target_access_hash = Sha256::new();
    let mut target_semantics = SemanticAccumulator::new();
    let mut target_blocks_hash = Sha256::new();
    let mut target_offset = 0u64;
    let pass2_started = Instant::now();
    let mut batch_start = 0usize;
    while batch_start < hot_index.rows.len() {
        let batch_end = hot_batch_end(
            &hot_index.rows,
            batch_start,
            batch_size,
            access_context.is_some(),
        )?;
        let rows = &hot_index.rows[batch_start..batch_end];
        let mut inputs = read_compressed_block_batch(&mut source_blocks, rows, None)?;
        if let Some(signatures) = source_signatures.as_mut() {
            attach_block_signatures(signatures, &mut inputs)?;
        }
        let rewritten = pool.install(|| {
            inputs
                .into_par_iter()
                .map(|input| {
                    rewrite_source_block(
                        input,
                        &source_registry,
                        &old_to_new,
                        &target_registry,
                        options.level,
                        access_context.as_ref(),
                    )
                })
                .collect::<Result<Vec<_>>>()
        })?;
        for item in rewritten {
            ensure!(
                item.compressed.len() as u64 <= MAX_HOT_BLOCK_FRAME_BYTES,
                "rewritten compressed block {} is {} bytes, exceeding {} byte limit",
                item.row.block_id,
                item.compressed.len(),
                MAX_HOT_BLOCK_FRAME_BYTES
            );
            let compressed_len = u32::try_from(item.compressed.len())
                .context("rewritten compressed block exceeds u32::MAX")?;
            target_blocks
                .write_all(&item.compressed)
                .with_context(|| format!("write {}", target_blocks_path.display()))?;
            target_blocks_hash.update(&item.compressed);
            let mut row = item.row;
            row.compressed_offset = target_offset;
            row.compressed_len = compressed_len;
            row.uncompressed_len = item.uncompressed_len;
            target_offset = target_offset
                .checked_add(u64::from(compressed_len))
                .context("target block offset overflow")?;
            match (target_access_writer.as_mut(), item.access) {
                (Some(writer), Some(access)) => {
                    ensure!(
                        access.len() as u64 <= ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
                        "rebuilt block-access {} exceeds shared frame limit",
                        row.block_id
                    );
                    let access_len =
                        u32::try_from(access.len()).context("rebuilt block-access exceeds u32")?;
                    writer.write_all(&access)?;
                    target_access_hash.update(&access);
                    target_access_rows.push(ArchiveV2BlockAccessIndexRow {
                        block_id: row.block_id,
                        slot: row.slot,
                        access_offset: target_access_offset,
                        access_len,
                        tx_count: row.tx_count,
                        signature_count: row.signature_count,
                    });
                    target_access_offset = target_access_offset
                        .checked_add(u64::from(access_len))
                        .context("target block-access offset overflow")?;
                }
                (None, None) => {}
                _ => bail!(
                    "block-access rebuild state mismatch at block_id {}",
                    row.block_id
                ),
            }
            target_semantics.push(&item.semantic)?;
            target_rows.push(row);
            progress.update(1, u64::from(item.semantic.transactions));
        }
        batch_start = batch_end;
    }
    target_blocks
        .flush()
        .with_context(|| format!("flush {}", target_blocks_path.display()))?;
    drop(target_blocks);
    let target_semantics_during_rewrite = target_semantics.finish();
    ensure!(
        target_semantics_during_rewrite == source_semantics,
        "source/target semantic mismatch during rewrite"
    );
    let target_blocks_binding = FileBinding {
        bytes: target_offset,
        sha256: hex_digest(target_blocks_hash.finalize()),
    };
    write_archive_v2_hot_block_index(
        &staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        target_offset,
        options.level,
        0,
        &target_rows,
    )?;
    let target_access_binding = if let Some(mut writer) = target_access_writer {
        writer.flush()?;
        drop(writer);
        ensure!(
            target_access_rows.len() == target_rows.len(),
            "rebuilt block-access row count mismatch"
        );
        write_archive_v2_block_access_index(
            &staging.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
            target_access_offset,
            0,
            &target_access_rows,
        )?;
        let get_block = build_get_block_rows(&target_rows, &target_access_rows)?;
        write_archive_v2_get_block_index(
            &staging.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE),
            &get_block,
        )?;
        Some(FileBinding {
            bytes: target_access_offset,
            sha256: hex_digest(target_access_hash.finalize()),
        })
    } else {
        None
    };
    drop(source_registry);
    fs::remove_file(&source_registry_snapshot_path)
        .context("remove private source registry snapshot")?;
    let copied = copy_independent_sidecars(&source_dir, &staging)?;
    info!(
        elapsed_secs = pass2_started.elapsed().as_secs_f64(),
        blocks = target_rows.len(),
        transactions = target_semantics_during_rewrite.transactions,
        typed_references = target_semantics_during_rewrite.pubkey_references,
        compressed_bytes = target_offset,
        "completed registry reprocess pass 2 and sidecar rewrite"
    );

    ensure!(
        build_checkpoint(&source_dir, &target_dir, options)? == checkpoint,
        "source generation changed while registry reprocess was running"
    );

    let mut source_files = source_file_bindings(
        &source_dir,
        &copied,
        FileBinding {
            bytes: hot_index.blob_file_bytes,
            sha256: hex_digest(source_blocks_hash.finalize()),
        },
    )?;
    // The first-seen manifest and seed are source-generation identity even though neither is
    // carried into the canonical target.
    add_binding_if_file(
        &mut source_files,
        &source_dir,
        ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    )?;
    add_binding_if_file(
        &mut source_files,
        &source_dir,
        ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    )?;
    let mut target_files = target_file_bindings(
        &staging,
        &copied,
        target_blocks_binding,
        target_access_binding,
    )?;
    ensure!(!target_files.contains_key(REGISTRY_REPROCESS_RECEIPT_FILE));
    let source_generation_sha256 = generation_digest(&source_files);
    let target_generation_sha256 = generation_digest(&target_files);
    let receipt = RegistryReprocessReceipt {
        version: RECEIPT_VERSION,
        algorithm: RECEIPT_ALGORITHM.to_owned(),
        epoch: options.epoch,
        threads: options.threads,
        sort_memory_mib: options.sort_memory_mib,
        level: options.level,
        source_anchor_sha256: checkpoint.source_anchor_sha256.clone(),
        source_dir: source_dir.display().to_string(),
        target_dir: target_dir.display().to_string(),
        source_generation_sha256,
        target_generation_sha256,
        source_files: std::mem::take(&mut source_files),
        target_files: std::mem::take(&mut target_files),
        source_registry_keys,
        target_registry_keys,
        eligible_references,
        source_semantics: source_semantics.clone(),
        target_semantics: target_semantics_during_rewrite,
    };
    validate_receipt_shape(&receipt, options.epoch)?;
    write_receipt(&staging, &receipt)?;
    sync_generation(&staging)?;
    fs::remove_file(staging.join(REPROCESS_CHECKPOINT_FILE))
        .context("remove staging-only registry reprocess checkpoint")?;
    sync_directory(&staging)?;
    ensure!(
        build_checkpoint(&source_dir, &target_dir, options)? == checkpoint,
        "source generation changed before registry reprocess publication"
    );
    ensure!(
        !target_dir.exists(),
        "registry reprocess target appeared before publication: {}",
        target_dir.display()
    );
    publish_directory_no_replace(&staging, &target_dir)?;
    sync_directory(target_parent)?;
    staging_guard.disarm();
    progress.update(1, 0);
    progress.final_report();
    info!(
        elapsed_secs = started.elapsed().as_secs_f64(),
        target = %target_dir.display(),
        receipt = %target_dir.join(REGISTRY_REPROCESS_RECEIPT_FILE).display(),
        target_registry_keys,
        eligible_references,
        "published canonical registry generation"
    );
    Ok(receipt)
}

/// Cheap bounded steady-state probe.  This parses only the publication-last receipt and checks
/// its identity fields; it deliberately does not hash archive payloads.
pub(crate) fn probe_published_reprocess(
    target: &Path,
    epoch: u64,
) -> Result<RegistryReprocessReceipt> {
    let target = fs::canonicalize(target)
        .with_context(|| format!("canonicalize published target {}", target.display()))?;
    let receipt = read_receipt(&target)?;
    validate_receipt_shape(&receipt, epoch)?;
    ensure!(
        Path::new(&receipt.target_dir) == target,
        "receipt target_dir={} does not identify published generation {}",
        receipt.target_dir,
        target.display()
    );
    ensure!(
        generation_digest(&receipt.target_files) == receipt.target_generation_sha256,
        "target generation digest mismatch in receipt"
    );
    ensure!(
        generation_digest(&receipt.source_files) == receipt.source_generation_sha256,
        "source generation digest mismatch in receipt"
    );
    validate_probe_core_files(&receipt.target_files, false)?;
    validate_probe_core_files(&receipt.source_files, true)?;
    probe_binding_sizes(&target, &receipt.target_files)?;
    let source = fs::canonicalize(&receipt.source_dir)
        .with_context(|| format!("canonicalize receipt source {}", receipt.source_dir))?;
    ensure!(source == Path::new(&receipt.source_dir));
    probe_binding_sizes(&source, &receipt.source_files)?;
    Ok(receipt)
}

fn validate_probe_core_files(
    files: &BTreeMap<String, FileBinding>,
    first_seen_source: bool,
) -> Result<()> {
    for name in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ARCHIVE_V2_POH_FILE,
        ARCHIVE_V2_SHREDDING_FILE,
    ] {
        ensure!(
            files.contains_key(name),
            "receipt omits core artifact {name}"
        );
    }
    ensure!(
        files.contains_key(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE) == first_seen_source,
        "receipt first-seen manifest presence mismatch"
    );
    let blob = files.contains_key(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let index = files.contains_key(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let get_block = files.contains_key(ARCHIVE_V2_GET_BLOCK_INDEX_FILE);
    ensure!(
        blob == index,
        "receipt contains only one block-access artifact"
    );
    if !first_seen_source {
        ensure!(
            get_block == blob,
            "target receipt contains an incomplete block-access/get-block artifact set"
        );
    }
    Ok(())
}

fn probe_binding_sizes(directory: &Path, files: &BTreeMap<String, FileBinding>) -> Result<()> {
    for (name, binding) in files {
        let (file, metadata) = open_regular_read(&directory.join(name))?;
        ensure!(
            metadata.len() == binding.bytes,
            "published artifact size mismatch for {name}"
        );
        ensure_open_file_unchanged(&directory.join(name), &file, &metadata)?;
    }
    Ok(())
}

/// Deep restart/exit validation.  This authenticates every bound source and target file, checks
/// canonical registry order and metadata flags, and repeats semantic normalization from both
/// generations.  It is intentionally unsuitable for a five-second scheduler poll.
pub(crate) fn validate_published_reprocess(
    source: &Path,
    target: &Path,
    epoch: u64,
) -> Result<RegistryReprocessReceipt> {
    let source = fs::canonicalize(source)
        .with_context(|| format!("canonicalize source {}", source.display()))?;
    let target = fs::canonicalize(target)
        .with_context(|| format!("canonicalize target {}", target.display()))?;
    let receipt = probe_published_reprocess(&target, epoch)?;
    ensure!(
        Path::new(&receipt.source_dir) == source,
        "receipt source_dir={} does not identify source generation {}",
        receipt.source_dir,
        source.display()
    );
    validate_bound_files_except_blocks(&source, &receipt.source_files)?;
    validate_bound_files_except_blocks(&target, &receipt.target_files)?;
    ensure!(
        generation_digest(&receipt.source_files) == receipt.source_generation_sha256,
        "source generation digest mismatch in receipt"
    );
    ensure!(
        generation_digest(&receipt.target_files) == receipt.target_generation_sha256,
        "target generation digest mismatch in receipt"
    );
    validate_canonical_registry(&target, receipt.target_registry_keys)?;
    validate_target_meta(&target)?;
    let (source_semantics, source_blocks) =
        recompute_source_canonical_counts(&source, &target, &receipt, epoch)?;
    ensure!(
        receipt.source_files.get(ARCHIVE_V2_BLOCKS_FILE) == Some(&source_blocks),
        "source block artifact binding mismatch"
    );
    let (target_semantics, target_blocks) = scan_target_generation_semantics(&target, epoch)?;
    ensure!(
        receipt.target_files.get(ARCHIVE_V2_BLOCKS_FILE) == Some(&target_blocks),
        "target block artifact binding mismatch"
    );
    ensure!(
        source_semantics == receipt.source_semantics,
        "source semantic receipt mismatch"
    );
    ensure!(
        target_semantics == receipt.target_semantics,
        "target semantic receipt mismatch"
    );
    ensure!(
        source_semantics == target_semantics,
        "published semantic parity mismatch"
    );
    Ok(receipt)
}

fn validate_options(options: &RegistryReprocessOptions) -> Result<()> {
    ensure!(
        options.epoch != 0,
        "first-seen registry reprocessing does not support epoch 0 genesis"
    );
    ensure!(options.threads > 0, "--threads must be greater than zero");
    ensure!(
        options.sort_memory_mib > 0,
        "--sort-memory-mib must be greater than zero"
    );
    Ok(())
}

fn canonical_target_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolve current directory")?
            .join(path)
    };
    let name = absolute
        .file_name()
        .ok_or_else(|| anyhow!("target has no final path component: {}", absolute.display()))?;
    let parent = absolute
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| anyhow!("target has no parent: {}", absolute.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create target parent {}", parent.display()))?;
    let parent = fs::canonicalize(parent)
        .with_context(|| format!("canonicalize target parent {}", parent.display()))?;
    Ok(parent.join(name))
}

fn staging_path(target: &Path) -> Result<PathBuf> {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("target name is not valid UTF-8: {}", target.display()))?;
    Ok(parent.join(format!(".{name}.registry-reprocess.staging")))
}

fn build_checkpoint(
    source: &Path,
    target: &Path,
    options: &RegistryReprocessOptions,
) -> Result<ReprocessCheckpoint> {
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla.registry-reprocess.source-anchor.v1");
    for name in [
        ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
    ] {
        let binding = hash_file(&source.join(name))?;
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(binding.bytes.to_le_bytes());
        hasher.update(binding.sha256.as_bytes());
    }
    for name in [
        ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
        ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ] {
        let path = source.join(name);
        if path.try_exists()? {
            let binding = hash_file(&path)?;
            hasher.update((name.len() as u64).to_le_bytes());
            hasher.update(name.as_bytes());
            hasher.update(binding.bytes.to_le_bytes());
            hasher.update(binding.sha256.as_bytes());
        }
    }
    for name in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ] {
        let metadata = regular_file_metadata(&source.join(name))?;
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(metadata.len().to_le_bytes());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            hasher.update(metadata.dev().to_le_bytes());
            hasher.update(metadata.ino().to_le_bytes());
            hasher.update(metadata.mtime().to_le_bytes());
            hasher.update(metadata.mtime_nsec().to_le_bytes());
            hasher.update(metadata.ctime().to_le_bytes());
            hasher.update(metadata.ctime_nsec().to_le_bytes());
        }
    }
    for name in
        std::iter::once(ARCHIVE_V2_BLOCK_ACCESS_FILE).chain(INDEPENDENT_SIDECARS.iter().copied())
    {
        let path = source.join(name);
        if !path.try_exists()? {
            continue;
        }
        let metadata = regular_file_metadata(&path)?;
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(metadata.len().to_le_bytes());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            hasher.update(metadata.dev().to_le_bytes());
            hasher.update(metadata.ino().to_le_bytes());
            hasher.update(metadata.mtime().to_le_bytes());
            hasher.update(metadata.mtime_nsec().to_le_bytes());
            hasher.update(metadata.ctime().to_le_bytes());
            hasher.update(metadata.ctime_nsec().to_le_bytes());
        }
    }
    Ok(ReprocessCheckpoint {
        version: REPROCESS_CHECKPOINT_VERSION,
        algorithm: RECEIPT_ALGORITHM.to_owned(),
        source_dir: source.display().to_string(),
        target_dir: target.display().to_string(),
        epoch: options.epoch,
        threads: options.threads,
        sort_memory_mib: options.sort_memory_mib,
        level: options.level,
        source_anchor_sha256: hex_digest(hasher.finalize()),
    })
}

fn regular_file_metadata(path: &Path) -> Result<fs::Metadata> {
    let link = fs::symlink_metadata(path).with_context(|| format!("inspect {}", path.display()))?;
    ensure!(
        link.file_type().is_file(),
        "source artifact is not a regular non-symlink file: {}",
        path.display()
    );
    Ok(link)
}

fn open_regular_read(path: &Path) -> Result<(File, fs::Metadata)> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .with_context(|| format!("open regular file {}", path.display()))?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.file_type().is_file(),
        "path is not a regular non-symlink file: {}",
        path.display()
    );
    let path_metadata = fs::symlink_metadata(path)?;
    ensure!(
        path_metadata.file_type().is_file() && same_file_snapshot(&metadata, &path_metadata),
        "file path changed while opening: {}",
        path.display()
    );
    Ok((file, metadata))
}

fn ensure_open_file_unchanged(path: &Path, file: &File, before: &fs::Metadata) -> Result<()> {
    let after = file.metadata()?;
    let path_metadata = fs::symlink_metadata(path)?;
    ensure!(
        same_file_snapshot(before, &after)
            && path_metadata.file_type().is_file()
            && same_file_snapshot(before, &path_metadata),
        "file changed while reading: {}",
        path.display()
    );
    Ok(())
}

fn prepare_staging(target: &Path, expected: &ReprocessCheckpoint) -> Result<PathBuf> {
    let staging = staging_path(target)?;
    match fs::symlink_metadata(&staging) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("inspect staging {}", staging.display()));
        }
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_dir(),
                "stale staging path is not a directory: {}",
                staging.display()
            );
            match read_checkpoint(&staging) {
                Ok(actual) => ensure!(
                    &actual == expected,
                    "stale staging checkpoint does not match this source/target/options; refusing to remove {}",
                    staging.display()
                ),
                Err(checkpoint_error) => {
                    let receipt = read_receipt(&staging).with_context(|| {
                        format!(
                            "stale staging has neither a valid checkpoint ({checkpoint_error}) nor a valid receipt"
                        )
                    })?;
                    ensure!(
                        receipt.version == RECEIPT_VERSION
                            && receipt.algorithm == expected.algorithm
                            && receipt.epoch == expected.epoch
                            && receipt.source_dir == expected.source_dir
                            && receipt.target_dir == expected.target_dir
                            && receipt.threads == expected.threads
                            && receipt.sort_memory_mib == expected.sort_memory_mib
                            && receipt.level == expected.level
                            && receipt.source_anchor_sha256 == expected.source_anchor_sha256,
                        "stale staging receipt does not match this source/target/options; refusing to remove {}",
                        staging.display()
                    );
                }
            }
            fs::remove_dir_all(&staging)
                .with_context(|| format!("restart matching stale staging {}", staging.display()))?;
        }
    }
    fs::create_dir(&staging).with_context(|| format!("create staging {}", staging.display()))?;
    let checkpoint_path = staging.join(REPROCESS_CHECKPOINT_FILE);
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&checkpoint_path)?,
    );
    serde_json::to_writer_pretty(&mut writer, expected)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    sync_directory(&staging)?;
    Ok(staging)
}

fn read_checkpoint(staging: &Path) -> Result<ReprocessCheckpoint> {
    let path = staging.join(REPROCESS_CHECKPOINT_FILE);
    let (mut file, metadata) = open_regular_read(&path)?;
    ensure!(
        metadata.len() > 0 && metadata.len() <= RECEIPT_MAX_BYTES,
        "invalid staging checkpoint size"
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(RECEIPT_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= RECEIPT_MAX_BYTES);
    ensure_open_file_unchanged(&path, &file, &metadata)?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse checkpoint {}", path.display()))
}

#[cfg(unix)]
fn acquire_reprocess_lock(
    source: &Path,
    target: &Path,
    options: &RegistryReprocessOptions,
) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .context("target name is not UTF-8")?;
    let path = parent.join(format!(".{name}.registry-reprocess.lock"));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)?;
    let lock_metadata = file.metadata()?;
    let lock_path_metadata = fs::symlink_metadata(&path)?;
    ensure!(
        lock_metadata.file_type().is_file()
            && lock_path_metadata.file_type().is_file()
            && same_file_identity(&lock_metadata, &lock_path_metadata),
        "registry reprocess lock is not a regular file: {}",
        path.display()
    );
    use std::os::fd::AsRawFd;
    // SAFETY: `file` owns a valid descriptor for the entire migration and flock does not retain
    // a pointer. LOCK_NB prevents a second scheduler/manual process from waiting indefinitely.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(io::Error::last_os_error())
            .with_context(|| format!("acquire registry reprocess lock {}", path.display()));
    }
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    writeln!(file, "version=1")?;
    writeln!(file, "pid={}", std::process::id())?;
    writeln!(file, "source={}", source.display())?;
    writeln!(file, "target={}", target.display())?;
    writeln!(file, "epoch={}", options.epoch)?;
    file.flush()?;
    Ok(file)
}

#[cfg(not(unix))]
fn acquire_reprocess_lock(
    _source: &Path,
    _target: &Path,
    _options: &RegistryReprocessOptions,
) -> Result<File> {
    bail!("registry reprocess locking is unsupported on this operating system")
}

fn read_source_manifest(source: &Path) -> Result<SourceManifest> {
    let path = source.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE);
    let (mut file, metadata) = open_regular_read(&path)?;
    ensure!(
        metadata.file_type().is_file()
            && metadata.len() > 0
            && metadata.len() <= MANIFEST_MAX_BYTES,
        "first-seen manifest must be a non-empty regular file no larger than {MANIFEST_MAX_BYTES} bytes: {}",
        path.display()
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(MANIFEST_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MANIFEST_MAX_BYTES,
        "first-seen manifest grew while reading"
    );
    ensure_open_file_unchanged(&path, &file, &metadata)?;
    let text = std::str::from_utf8(&bytes).context("first-seen manifest is not UTF-8")?;
    let mut values = BTreeMap::<&str, &str>::new();
    for (line_index, line) in text.lines().enumerate() {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| anyhow!("malformed first-seen manifest line {}", line_index + 1))?;
        ensure!(
            !key.is_empty(),
            "empty first-seen manifest key on line {}",
            line_index + 1
        );
        ensure!(
            values.insert(key, value).is_none(),
            "duplicate first-seen manifest key {key}"
        );
    }
    ensure!(
        values.get("version") == Some(&"1"),
        "first-seen manifest version is not 1"
    );
    ensure!(
        values.get("registry_order") == Some(&"first_seen_v1"),
        "source manifest does not declare registry_order=first_seen_v1"
    );
    ensure!(
        values.get("count_semantics") == Some(&"all_compact_pubkey_refs_v1"),
        "source manifest does not declare all-reference count semantics"
    );
    let registry_keys = values
        .get("registry_keys")
        .context("first-seen manifest missing registry_keys")?
        .parse::<u64>()
        .context("invalid first-seen registry_keys")?;
    let references = values
        .get("references")
        .context("first-seen manifest missing references")?
        .parse::<u64>()
        .context("invalid first-seen references")?;
    ensure!(registry_keys > 0 && registry_keys <= u64::from(u32::MAX));
    Ok(SourceManifest {
        registry_keys,
        references,
    })
}

fn read_registry_counts(path: &Path, expected: usize) -> Result<Vec<u32>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut counts = Vec::new();
    counts
        .try_reserve_exact(expected)
        .context("allocate registry count vector")?;
    for index in 0..expected {
        let count = read_canonical_u32_varint(&mut reader)?
            .ok_or_else(|| anyhow!("registry counts ended before row {}", index + 1))?;
        counts.push(count);
    }
    ensure!(
        read_canonical_u32_varint(&mut reader)?.is_none(),
        "registry counts contains more than {expected} rows"
    );
    Ok(counts)
}

fn read_canonical_u32_varint(reader: &mut impl Read) -> Result<Option<u32>> {
    let mut first = [0u8; 1];
    if reader.read(&mut first)? == 0 {
        return Ok(None);
    }
    let mut value = u32::from(first[0] & 0x7f);
    let mut byte = first[0];
    let mut bytes = 1usize;
    let mut shift = 7u32;
    while byte & 0x80 != 0 {
        ensure!(bytes < 5, "u32 varint exceeds five bytes");
        let mut next = [0u8; 1];
        reader
            .read_exact(&mut next)
            .context("truncated u32 varint")?;
        byte = next[0];
        let payload = u32::from(byte & 0x7f);
        ensure!(
            shift < 32 && payload <= (u32::MAX >> shift),
            "u32 varint overflow"
        );
        value |= payload << shift;
        shift += 7;
        bytes += 1;
    }
    let canonical_bytes = if value == 0 {
        1
    } else {
        ((u32::BITS - value.leading_zeros()) as usize).div_ceil(7)
    };
    ensure!(
        bytes == canonical_bytes,
        "non-canonical u32 varint encoding"
    );
    Ok(Some(value))
}

fn write_u32_varint(writer: &mut impl Write, mut value: u32) -> Result<()> {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            return Ok(());
        }
    }
}

fn validate_and_rewrite_meta(source: &Path, target: &Path) -> Result<WincodeArchiveV2Footer> {
    let source_path = source.join(ARCHIVE_V2_META_FILE);
    let target_path = target.join(ARCHIVE_V2_META_FILE);
    let source_file =
        File::open(&source_path).with_context(|| format!("open {}", source_path.display()))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, source_file);
    let first_bytes =
        read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?.context("hot metadata is empty")?;
    let first: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &first_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    let ArchiveV2HotMetaRecord::Header(header) = first else {
        bail!("first hot metadata record is not a header");
    };
    let expected_flags = WINCODE_ARCHIVE_V2_FLAG_LEB128
        | WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY
        | WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS;
    ensure!(
        header.version == WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
        "unsupported first-seen hot metadata version {}",
        header.version
    );
    ensure!(
        header.flags == expected_flags,
        "first-seen hot metadata flags {:#x} do not exactly match expected {expected_flags:#x}",
        header.flags
    );
    let second_bytes = read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?
        .context("hot metadata is missing footer")?;
    let second: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &second_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    let footer = match second {
        ArchiveV2HotMetaRecord::Footer(footer) => footer,
        ArchiveV2HotMetaRecord::Genesis(_) => {
            bail!("first-seen registry reprocessing safely rejects epoch-0 genesis metadata")
        }
        ArchiveV2HotMetaRecord::Header(_) => bail!("duplicate hot metadata header"),
    };
    ensure!(
        read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?.is_none(),
        "hot metadata contains trailing records after footer"
    );
    let output_file =
        File::create(&target_path).with_context(|| format!("create {}", target_path.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(IO_BUFFER_BYTES, output_file));
    writer.write(&ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
        version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
        flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
    }))?;
    writer.write(&ArchiveV2HotMetaRecord::Footer(footer.clone()))?;
    writer.flush()?;
    Ok(footer)
}

fn validate_target_meta(target: &Path) -> Result<()> {
    let path = target.join(ARCHIVE_V2_META_FILE);
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let first_bytes = read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?
        .context("target metadata is empty")?;
    let first: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &first_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    let ArchiveV2HotMetaRecord::Header(header) = first else {
        bail!("target metadata does not start with a header");
    };
    ensure!(
        header.version == WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION
            && header.flags == WINCODE_ARCHIVE_V2_FLAG_LEB128,
        "target metadata is not canonical LEB128 Compact-V2"
    );
    let second_bytes = read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?
        .context("target metadata is missing footer")?;
    let second: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &second_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    ensure!(
        matches!(second, ArchiveV2HotMetaRecord::Footer(_)),
        "target metadata is missing its footer or contains genesis"
    );
    ensure!(
        read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?.is_none(),
        "target metadata has trailing records"
    );
    Ok(())
}

fn read_bounded_frame(reader: &mut impl Read, max_bytes: usize) -> Result<Option<Vec<u8>>> {
    let Some(len) = read_canonical_u32_varint(reader)? else {
        return Ok(None);
    };
    let len = len as usize;
    ensure!(
        len <= max_bytes,
        "wincode frame length {len} exceeds {max_bytes} byte limit"
    );
    let mut bytes = vec![0u8; len];
    reader
        .read_exact(&mut bytes)
        .context("truncated wincode frame")?;
    Ok(Some(bytes))
}

fn validate_hot_index(
    blocks_path: &Path,
    index: &blockzilla_format::ArchiveV2HotBlockIndex,
    epoch: u64,
) -> Result<()> {
    ensure!(
        index.flags == 0,
        "first-seen source hot index flags must be zero, got {:#x}",
        index.flags
    );
    ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS == 0,
        "raw hot-block sources are not supported"
    );
    let actual_bytes = fs::metadata(blocks_path)
        .with_context(|| format!("stat {}", blocks_path.display()))?
        .len();
    ensure!(
        actual_bytes == index.blob_file_bytes,
        "hot blocks size {actual_bytes} != index blob_file_bytes {}",
        index.blob_file_bytes
    );
    let mut compressed_offset = 0u64;
    let mut tx_ordinal = 0u64;
    let mut signature_ordinal = 0u64;
    let mut previous_slot = None;
    for (position, row) in index.rows.iter().enumerate() {
        ensure!(
            row.block_id as usize == position,
            "non-contiguous hot block ID at row {position}"
        );
        ensure!(
            row.slot / crate::SLOTS_PER_EPOCH == epoch,
            "slot {} is outside epoch {epoch}",
            row.slot
        );
        if let Some(previous) = previous_slot {
            ensure!(
                row.slot > previous,
                "hot index slots are not strictly increasing"
            );
        }
        ensure!(
            row.compressed_offset == compressed_offset,
            "non-contiguous compressed offset at block {}",
            row.block_id
        );
        ensure!(
            row.compressed_len > 0 && row.uncompressed_len > 0,
            "empty hot block {}",
            row.block_id
        );
        ensure!(
            u64::from(row.compressed_len) <= MAX_HOT_BLOCK_FRAME_BYTES
                && u64::from(row.uncompressed_len) <= MAX_HOT_BLOCK_FRAME_BYTES,
            "hot block {} frame lengths compressed={} uncompressed={} exceed {} byte limit",
            row.block_id,
            row.compressed_len,
            row.uncompressed_len,
            MAX_HOT_BLOCK_FRAME_BYTES
        );
        ensure!(
            row.first_tx_ordinal == tx_ordinal,
            "transaction ordinal discontinuity at block {}",
            row.block_id
        );
        ensure!(
            row.first_signature_ordinal == signature_ordinal,
            "signature ordinal discontinuity at block {}",
            row.block_id
        );
        compressed_offset = compressed_offset
            .checked_add(u64::from(row.compressed_len))
            .context("hot block compressed offset overflow")?;
        tx_ordinal = tx_ordinal
            .checked_add(u64::from(row.tx_count))
            .context("hot block transaction ordinal overflow")?;
        signature_ordinal = signature_ordinal
            .checked_add(u64::from(row.signature_count))
            .context("hot block signature ordinal overflow")?;
        previous_slot = Some(row.slot);
    }
    ensure!(
        compressed_offset == index.blob_file_bytes,
        "hot index does not cover blocks file exactly"
    );
    Ok(())
}

fn read_compressed_block_batch(
    file: &mut File,
    rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    mut hasher: Option<&mut Sha256>,
) -> Result<Vec<CompressedBlockInput>> {
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        file.seek(SeekFrom::Start(row.compressed_offset))
            .with_context(|| format!("seek source block_id {}", row.block_id))?;
        let mut bytes = vec![0u8; row.compressed_len as usize];
        file.read_exact(&mut bytes)
            .with_context(|| format!("read source block_id {}", row.block_id))?;
        if let Some(hasher) = hasher.as_deref_mut() {
            hasher.update(&bytes);
        }
        output.push(CompressedBlockInput {
            row: *row,
            bytes,
            signatures: None,
        });
    }
    Ok(output)
}

fn hot_batch_end(
    rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    start: usize,
    max_rows: usize,
    include_access: bool,
) -> Result<usize> {
    ensure!(start < rows.len(), "hot batch start is outside index rows");
    ensure!(max_rows > 0, "hot batch row limit must be non-zero");
    let mut bytes = 0u64;
    let mut end = start;
    while end < rows.len() && end - start < max_rows {
        let row = &rows[end];
        let signature_bytes = u64::from(row.signature_count)
            .checked_mul(64)
            .context("hot batch signature byte total overflow")?;
        let uncompressed_working_set = u64::from(row.uncompressed_len)
            .checked_mul(HOT_UNCOMPRESSED_WORKING_SET_MULTIPLIER)
            .context("hot batch uncompressed working-set estimate overflow")?;
        let row_bytes = u64::from(row.compressed_len)
            .checked_add(uncompressed_working_set)
            .and_then(|bytes| bytes.checked_add(signature_bytes.saturating_mul(2)))
            .and_then(|bytes| {
                bytes.checked_add(if include_access {
                    ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES
                } else {
                    0
                })
            })
            .context("hot batch advertised byte total overflow")?;
        ensure!(
            row_bytes <= HOT_BATCH_MEMORY_BUDGET_BYTES,
            "hot block {} advertises {} compressed+uncompressed bytes, exceeding {} byte batch limit",
            row.block_id,
            row_bytes,
            HOT_BATCH_MEMORY_BUDGET_BYTES
        );
        let next = bytes
            .checked_add(row_bytes)
            .context("hot batch byte total overflow")?;
        if end != start && next > HOT_BATCH_MEMORY_BUDGET_BYTES {
            break;
        }
        bytes = next;
        end += 1;
    }
    ensure!(end > start, "hot batch builder made no progress");
    Ok(end)
}

fn decode_hot_block(input: &CompressedBlockInput) -> Result<ArchiveV2HotBlockBlob> {
    let decoded = zstd::bulk::decompress(&input.bytes, input.row.uncompressed_len as usize)
        .with_context(|| format!("zstd decompress block_id {}", input.row.block_id))?;
    ensure!(
        decoded.len() == input.row.uncompressed_len as usize,
        "block_id {} uncompressed length {} != index {}",
        input.row.block_id,
        decoded.len(),
        input.row.uncompressed_len
    );
    let block = match wincode::config::deserialize_exact::<ArchiveV2HotBlockBlob, _>(
        &decoded,
        bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
    ) {
        Ok(block) => block,
        Err(current_error) => {
            match wincode::config::deserialize_exact::<LegacyHotBlockWithShredding, _>(
                &decoded,
                bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
            ) {
                Ok(block) => block.into(),
                Err(shredding_error) => {
                    let legacy: LegacyHotBlockWithRewardsVec = wincode::config::deserialize_exact(
                    &decoded,
                    bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
                )
                .with_context(|| {
                    format!(
                        "decode hot block_id {}: current={current_error}; legacy-shredding={shredding_error}",
                        input.row.block_id
                    )
                })?;
                    legacy.into()
                }
            }
        }
    };
    ensure!(
        block.header.slot == input.row.slot,
        "block/index slot mismatch at block_id {}",
        input.row.block_id
    );
    ensure!(
        block.tx_count == input.row.tx_count,
        "block/index tx_count mismatch at block_id {}",
        input.row.block_id
    );
    ensure!(
        block.tx_rows.len() == input.row.tx_count as usize,
        "block tx row count mismatch at block_id {}",
        input.row.block_id
    );
    let signatures = block.tx_rows.iter().try_fold(0u32, |sum, row| {
        sum.checked_add(u32::from(row.signature_count))
            .context("block signature count overflow")
    })?;
    ensure!(
        signatures == input.row.signature_count,
        "block/index signature_count mismatch at block_id {}: rows={} index={}",
        input.row.block_id,
        signatures,
        input.row.signature_count
    );
    Ok(block)
}

fn analyze_source_block(
    input: CompressedBlockInput,
    registry: &MappedRegistry,
) -> Result<SourceBlockAnalysis> {
    let row = input.row;
    let mut block = decode_hot_block(&input)?;
    let mut eligible_ids = Vec::new();
    let mut all_ids = Vec::new();
    let semantic = normalize_block(
        &mut block,
        u64::from(row.block_id),
        row.slot,
        |key, class| {
            let CompactPubkey::Id(id) = *key else {
                bail!(
                    "strict first-seen block {} contains a raw typed CompactPubkey",
                    row.block_id
                );
            };
            let raw = registry.key(id)?;
            push_bounded_reference_id(&mut all_ids, id, row.block_id)?;
            if class == ReferenceClass::Eligible {
                push_bounded_reference_id(&mut eligible_ids, id, row.block_id)?;
            }
            Ok(raw)
        },
    )?;
    Ok(SourceBlockAnalysis {
        eligible: compress_id_counts(eligible_ids)?,
        all: compress_id_counts(all_ids)?,
        semantic,
    })
}

fn push_bounded_reference_id(ids: &mut Vec<u32>, id: u32, block_id: u32) -> Result<()> {
    ensure!(
        ids.len() < MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK,
        "block {block_id} exceeds the {} typed-pubkey-reference safety limit",
        MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK
    );
    if ids.len() == ids.capacity() {
        let remaining = MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK - ids.len();
        let additional = ids.capacity().max(1_024).min(remaining);
        ids.try_reserve_exact(additional)
            .context("allocate bounded block pubkey-reference vector")?;
    }
    ids.push(id);
    Ok(())
}

fn rewrite_source_block(
    input: CompressedBlockInput,
    source_registry: &MappedRegistry,
    old_to_new: &[u32],
    target_registry: &MappedRegistry,
    level: i32,
    access_context: Option<&AccessBuildContext>,
) -> Result<RewrittenBlock> {
    let row = input.row;
    let mut block = decode_hot_block(&input)?;
    rewrite_block_pubkeys(&mut block, |key, class| {
        let CompactPubkey::Id(old_id) = *key else {
            bail!(
                "strict first-seen block {} contains a raw typed CompactPubkey",
                row.block_id
            );
        };
        let index = usize::try_from(old_id - 1).context("old pubkey ID exceeds usize")?;
        let new_id = *old_to_new
            .get(index)
            .ok_or_else(|| anyhow!("old pubkey ID {old_id} is outside remap"))?;
        if class == ReferenceClass::Eligible {
            ensure!(
                new_id != 0,
                "eligible pubkey ID {old_id} was excluded from target registry"
            );
        }
        *key = if new_id == 0 {
            CompactPubkey::raw(source_registry.key(old_id)?)
        } else {
            CompactPubkey::id(new_id)
        };
        Ok(())
    })?;
    let access = access_context
        .map(|context| {
            let signatures = input
                .signatures
                .as_deref()
                .context("access rebuild is missing block signature bytes")?;
            let blob = super::build_archive_v2_block_access_blob_with_pubkey_resolver(
                &block,
                |id| target_registry.key(id),
                &context.blockhashes,
                &context.previous_tail,
                signatures,
                &context.vote_hashes,
            )?;
            let encoded_size = wincode::config::serialized_size(&blob, wincode_leb128_config())?;
            ensure!(
                encoded_size <= ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
                "rebuilt block-access {} would encode to {encoded_size} bytes, exceeding {}",
                row.block_id,
                ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES
            );
            wincode::config::serialize(&blob, wincode_leb128_config()).map_err(anyhow::Error::from)
        })
        .transpose()?;
    let encoded_size = wincode::config::serialized_size(&block, wincode_leb128_config())?;
    ensure!(
        encoded_size <= MAX_HOT_BLOCK_FRAME_BYTES,
        "rewritten hot block {} would encode to {encoded_size} bytes, exceeding {}",
        row.block_id,
        MAX_HOT_BLOCK_FRAME_BYTES
    );
    let encoded = wincode::config::serialize(&block, wincode_leb128_config())?;
    let uncompressed_len =
        u32::try_from(encoded.len()).context("rewritten hot block exceeds u32::MAX")?;
    let compressed = zstd::bulk::compress(&encoded, level)
        .with_context(|| format!("zstd compress rewritten block_id {}", row.block_id))?;
    let semantic = normalize_block(
        &mut block,
        u64::from(row.block_id),
        row.slot,
        |key, _class| resolve_compact_pubkey(*key, target_registry),
    )?;
    Ok(RewrittenBlock {
        row,
        compressed,
        uncompressed_len,
        semantic,
        access,
    })
}

fn resolve_compact_pubkey(key: CompactPubkey, registry: &MappedRegistry) -> Result<[u8; 32]> {
    match key {
        CompactPubkey::Id(id) => registry.key(id),
        CompactPubkey::Raw(raw) => Ok(raw),
    }
}

fn normalize_block(
    block: &mut ArchiveV2HotBlockBlob,
    block_id: u64,
    slot: u64,
    mut resolve: impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<[u8; 32]>,
) -> Result<BlockSemantic> {
    let mut reference = Sha256::new();
    reference.update(SEMANTIC_DOMAIN);
    reference.update(b".block-references");
    reference.update(block_id.to_le_bytes());
    reference.update(slot.to_le_bytes());
    let mut references = 0u64;
    rewrite_block_pubkeys(block, |key, class| {
        let raw = resolve(key, class)?;
        reference.update([match class {
            ReferenceClass::Eligible => 1,
            ReferenceClass::Excluded => 0,
        }]);
        reference.update(raw);
        references = references
            .checked_add(1)
            .context("block semantic pubkey reference overflow")?;
        // A one-byte registry ID is a representation-neutral placeholder. The ordered reference
        // digest above binds the actual resolved key and class, while this keeps normalization
        // from expanding each reference to a 33-byte raw sentinel.
        *key = CompactPubkey::id(1);
        Ok(())
    })?;
    let normalized_size = wincode::config::serialized_size(&*block, wincode_leb128_config())?;
    ensure!(
        normalized_size <= MAX_HOT_BLOCK_FRAME_BYTES,
        "normalized hot block {block_id} would encode to {normalized_size} bytes, exceeding {}",
        MAX_HOT_BLOCK_FRAME_BYTES
    );
    let normalized = wincode::config::serialize(block, wincode_leb128_config())?;
    Ok(BlockSemantic {
        block_id,
        slot,
        transactions: block.tx_count,
        references,
        reference_sha256: reference.finalize().into(),
        normalized_len: normalized.len() as u64,
        normalized_sha256: Sha256::digest(&normalized).into(),
    })
}

fn compress_id_counts(mut ids: Vec<u32>) -> Result<Vec<(u32, u32)>> {
    ids.sort_unstable();
    let mut runs = Vec::<(u32, u32)>::new();
    runs.try_reserve_exact(ids.len())
        .context("allocate bounded block pubkey count runs")?;
    for id in ids {
        if let Some((last_id, count)) = runs.last_mut()
            && *last_id == id
        {
            *count = count
                .checked_add(1)
                .context("per-block pubkey reference count overflow")?;
        } else {
            runs.push((id, 1u32));
        }
    }
    Ok(runs)
}

fn merge_count_runs(counts: &mut [u32], runs: &[(u32, u32)], subtract: bool) -> Result<()> {
    for &(id, value) in runs {
        ensure!(id != 0, "compact pubkey ID 0 is reserved");
        let slot = counts
            .get_mut((id - 1) as usize)
            .ok_or_else(|| anyhow!("pubkey ID {id} is outside registry count vector"))?;
        if subtract {
            *slot = slot.checked_sub(value).ok_or_else(|| {
                anyhow!("typed references for pubkey ID {id} exceed registry_counts.bin")
            })?;
        } else {
            *slot = slot.saturating_add(value);
        }
    }
    Ok(())
}

fn rewrite_block_pubkeys(
    block: &mut ArchiveV2HotBlockBlob,
    mut visit: impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    if let Some(rewards) = &mut block.header.rewards {
        for reward in &mut rewards.decoded {
            // Match the canonical direct-CAR split_compact/PreHot registry pass: block rewards
            // remain semantically encoded but do not influence usage-sorted registry IDs.
            visit(&mut reward.pubkey, ReferenceClass::Excluded)?;
        }
    }

    let source_messages = std::mem::take(&mut block.message_bytes);
    let source_metadata = std::mem::take(&mut block.metadata_bytes);
    let mut target_messages = Vec::with_capacity(source_messages.len());
    let mut target_metadata = Vec::with_capacity(source_metadata.len());
    let mut source_message_cursor = 0usize;
    let mut source_metadata_cursor = 0usize;
    for (position, row) in block.tx_rows.iter_mut().enumerate() {
        ensure!(
            row.tx_index as usize == position,
            "non-contiguous tx_index {} at row {position}",
            row.tx_index
        );
        ensure!(
            row.reserved == [0; 3],
            "tx row {} has non-zero reserved bytes",
            row.tx_index
        );
        ensure!(
            row.message_offset as usize == source_message_cursor,
            "tx row {} message offset {} is not canonical cursor {}",
            row.tx_index,
            row.message_offset,
            source_message_cursor
        );
        source_message_cursor = source_message_cursor
            .checked_add(row.message_len as usize)
            .context("source message region cursor overflow")?;
        let message = checked_region(
            &source_messages,
            row.message_offset,
            row.message_len,
            block.header.slot,
            row.tx_index,
            "message",
        )?;
        row.message_offset = u32::try_from(target_messages.len())
            .context("rewritten message region exceeds u32::MAX")?;
        if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            target_messages.extend_from_slice(message);
        } else {
            let mut decoded: ArchiveV2HotMessagePayload = wincode::config::deserialize_exact(
                message,
                bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
            )
            .with_context(|| format!("decode hot message tx_index={}", row.tx_index))?;
            visit_message_pubkeys(&mut decoded, &mut visit)?;
            let encoded_size = usize::try_from(wincode::config::serialized_size(
                &decoded,
                wincode_leb128_config(),
            )?)
            .context("rewritten message size exceeds usize")?;
            let projected = target_messages
                .len()
                .checked_add(target_metadata.len())
                .and_then(|size| size.checked_add(encoded_size))
                .context("rewritten hot payload size overflow")?;
            ensure!(
                projected <= MAX_HOT_BLOCK_FRAME_BYTES_USIZE,
                "rewritten hot payload exceeds {} byte limit at tx_index {}",
                MAX_HOT_BLOCK_FRAME_BYTES,
                row.tx_index
            );
            target_messages
                .try_reserve(encoded_size)
                .context("reserve rewritten message payload")?;
            wincode::config::serialize_into(
                &mut target_messages,
                &decoded,
                wincode_leb128_config(),
            )?;
        }
        row.message_len = u32::try_from(target_messages.len() - row.message_offset as usize)
            .context("rewritten message payload exceeds u32::MAX")?;

        let source_metadata_offset = row.metadata_offset;
        let source_metadata_len = row.metadata_len;
        ensure!(
            source_metadata_offset as usize == source_metadata_cursor,
            "tx row {} metadata offset {} is not canonical cursor {}",
            row.tx_index,
            source_metadata_offset,
            source_metadata_cursor
        );
        source_metadata_cursor = source_metadata_cursor
            .checked_add(source_metadata_len as usize)
            .context("source metadata region cursor overflow")?;
        row.metadata_offset = u32::try_from(target_metadata.len())
            .context("rewritten metadata region exceeds u32::MAX")?;
        if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            ensure!(
                source_metadata_len == 0,
                "tx row {} has metadata bytes without HAS_METADATA",
                row.tx_index
            );
        } else {
            ensure!(
                source_metadata_len > 0,
                "tx row {} declares empty metadata",
                row.tx_index
            );
            let metadata = checked_region(
                &source_metadata,
                source_metadata_offset,
                source_metadata_len,
                block.header.slot,
                row.tx_index,
                "metadata",
            )?;
            if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                target_metadata.extend_from_slice(metadata);
            } else {
                let mut decoded = decode_compact_metadata(metadata)
                    .with_context(|| format!("decode hot metadata tx_index={}", row.tx_index))?;
                visit_metadata_pubkeys(&mut decoded, &mut visit)?;
                let encoded_size = usize::try_from(wincode::config::serialized_size(
                    &decoded,
                    wincode_leb128_config(),
                )?)
                .context("rewritten metadata size exceeds usize")?;
                let projected = target_messages
                    .len()
                    .checked_add(target_metadata.len())
                    .and_then(|size| size.checked_add(encoded_size))
                    .context("rewritten hot payload size overflow")?;
                ensure!(
                    projected <= MAX_HOT_BLOCK_FRAME_BYTES_USIZE,
                    "rewritten hot payload exceeds {} byte limit at tx_index {}",
                    MAX_HOT_BLOCK_FRAME_BYTES,
                    row.tx_index
                );
                target_metadata
                    .try_reserve(encoded_size)
                    .context("reserve rewritten metadata payload")?;
                wincode::config::serialize_into(
                    &mut target_metadata,
                    &decoded,
                    wincode_leb128_config(),
                )?;
            }
        }
        row.metadata_len = u32::try_from(target_metadata.len() - row.metadata_offset as usize)
            .context("rewritten metadata payload exceeds u32::MAX")?;
    }
    ensure!(
        source_message_cursor == source_messages.len(),
        "hot block message rows cover {source_message_cursor} of {} bytes",
        source_messages.len()
    );
    ensure!(
        source_metadata_cursor == source_metadata.len(),
        "hot block metadata rows cover {source_metadata_cursor} of {} bytes",
        source_metadata.len()
    );
    block.message_bytes = target_messages;
    block.metadata_bytes = target_metadata;
    Ok(())
}

fn checked_region<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    slot: u64,
    tx_index: u32,
    label: &str,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let end = start
        .checked_add(len as usize)
        .context("hot block region offset overflow")?;
    bytes.get(start..end).ok_or_else(|| {
        anyhow!(
            "slot {slot} tx_index={tx_index} {label} slice offset={offset} len={len} is outside {} bytes",
            bytes.len()
        )
    })
}

fn decode_compact_metadata(bytes: &[u8]) -> Result<CompactMetaV1> {
    let current = wincode::config::deserialize_exact::<CompactMetaV1, _>(
        bytes,
        bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
    );
    let legacy = wincode::config::deserialize_exact::<LegacyCompactMetaV1, _>(
        bytes,
        bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
    )
    .map_err(anyhow::Error::from)
    .and_then(CompactMetaV1::try_from);
    let current_error = current.as_ref().err().map(ToString::to_string);
    let legacy_error = legacy.as_ref().err().map(ToString::to_string);
    match (current.ok(), legacy.ok()) {
        (Some(current), None) => Ok(current),
        (None, Some(legacy)) => Ok(legacy),
        (Some(current), Some(legacy)) => {
            let current_canonical = wincode::config::serialize(&current, wincode_leb128_config())?;
            let legacy_canonical = wincode::config::serialize(&legacy, wincode_leb128_config())?;
            ensure!(
                current_canonical == legacy_canonical,
                "ambiguous compact metadata decodes as different current and legacy values"
            );
            Ok(current)
        }
        (None, None) => bail!(
            "compact metadata is neither current nor legacy: current={}; legacy={}",
            current_error.as_deref().unwrap_or("unknown error"),
            legacy_error.as_deref().unwrap_or("unknown error")
        ),
    }
}

fn visit_message_pubkeys(
    message: &mut ArchiveV2HotMessagePayload,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => {
            for key in &mut message.account_keys {
                visit(key, ReferenceClass::Eligible)?;
            }
        }
        ArchiveV2HotMessagePayload::V1(message) => {
            for key in &mut message.account_keys {
                visit(key, ReferenceClass::Eligible)?;
            }
        }
        ArchiveV2HotMessagePayload::V0(message) => {
            for key in &mut message.account_keys {
                visit(key, ReferenceClass::Eligible)?;
            }
            for lookup in &mut message.address_table_lookups {
                visit(&mut lookup.account_key, ReferenceClass::Eligible)?;
            }
        }
    }
    Ok(())
}

fn visit_metadata_pubkeys(
    metadata: &mut CompactMetaV1,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    for key in metadata
        .loaded_writable_addresses
        .iter_mut()
        .chain(metadata.loaded_readonly_addresses.iter_mut())
    {
        visit(key, ReferenceClass::Eligible)?;
    }
    for balance in metadata
        .pre_token_balances
        .iter_mut()
        .chain(metadata.post_token_balances.iter_mut())
    {
        if let Some(key) = &mut balance.mint {
            visit(key, ReferenceClass::Eligible)?;
        }
        if let Some(key) = &mut balance.owner {
            visit(key, ReferenceClass::Eligible)?;
        }
        if let Some(key) = &mut balance.program_id {
            visit(key, ReferenceClass::Eligible)?;
        }
    }
    for reward in &mut metadata.rewards {
        visit(&mut reward.pubkey, ReferenceClass::Eligible)?;
    }
    if let Some(return_data) = &mut metadata.return_data {
        visit(&mut return_data.program_id, ReferenceClass::Eligible)?;
    }
    if let Some(logs) = &mut metadata.logs {
        visit_log_pubkeys(logs, visit)?;
    }
    Ok(())
}

fn visit_log_pubkeys(
    logs: &mut CompactLogStream,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    for event in &mut logs.events {
        match event {
            LogEvent::LoaderUpgradedProgram { program }
            | LogEvent::Invoke { program, .. }
            | LogEvent::BpfInvoke { program }
            | LogEvent::Consumed { program, .. }
            | LogEvent::Success { program }
            | LogEvent::BpfSuccess { program }
            | LogEvent::Failure { program, .. }
            | LogEvent::BpfFailure { program, .. }
            | LogEvent::FailureCustomProgramError { program, .. }
            | LogEvent::BpfFailureCustomProgramError { program, .. }
            | LogEvent::FailureInvalidAccountData { program }
            | LogEvent::BpfFailureInvalidAccountData { program }
            | LogEvent::FailureInvalidProgramArgument { program }
            | LogEvent::BpfFailureInvalidProgramArgument { program }
            | LogEvent::Return { program, .. } => visit(program, ReferenceClass::Excluded)?,
            LogEvent::ProgramIdLog { program, log } => {
                visit(program, ReferenceClass::Excluded)?;
                visit_program_log_pubkeys(log, visit)?;
            }
            LogEvent::LoaderFinalizedAccount { account }
            | LogEvent::RuntimeWritablePrivilegeEscalated { account }
            | LogEvent::RuntimeSignerPrivilegeEscalated { account }
            | LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => {
                visit(account, ReferenceClass::Excluded)?;
            }
            LogEvent::ProgramNotDeployed { program } | LogEvent::ProgramNotCached { program } => {
                if let Some(program) = program {
                    visit(program, ReferenceClass::Excluded)?;
                }
            }
            LogEvent::System(log) => visit_system_log_pubkeys(log, visit)?,
            LogEvent::ProgramLog(log) | LogEvent::ProgramPlainLog(log) => {
                visit_program_log_pubkeys(log, visit)?;
            }
            LogEvent::ProgramLogError { .. }
            | LogEvent::ProgramAccountNotWritable
            | LogEvent::ProgramIdMismatch
            | LogEvent::ProgramNotUpgradeable
            | LogEvent::ProgramAndProgramDataAccountMismatch
            | LogEvent::ProgramWasExtendedInThisBlockAlready
            | LogEvent::BpfConsumed { .. }
            | LogEvent::FailedToComplete { .. }
            | LogEvent::CustomProgramError { .. }
            | LogEvent::Data { .. }
            | LogEvent::Consumption { .. }
            | LogEvent::CbRequestUnits { .. }
            | LogEvent::UnknownProgram { .. }
            | LogEvent::UnknownAccount { .. }
            | LogEvent::VerifyEd25519
            | LogEvent::VerifySecp256k1
            | LogEvent::LogTruncated
            | LogEvent::StakeMergingAccounts
            | LogEvent::CloseContextState
            | LogEvent::Plain { .. }
            | LogEvent::Unparsed { .. } => {}
        }
    }
    Ok(())
}

fn visit_program_log_pubkeys(
    log: &mut ProgramLog,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    if let ProgramLog::Token2022(
        Token2022Log::ErrorHarvestingFrom { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom2 { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom3 { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom4 { account_key, .. },
    ) = log
    {
        visit(account_key, ReferenceClass::Excluded)?;
    }
    Ok(())
}

fn visit_system_log_pubkeys(
    log: &mut SystemProgramLog,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    match log {
        SystemProgramLog::CreateAddressMismatch {
            provided_addr,
            derived_addr,
        }
        | SystemProgramLog::TransferFromAddressMismatch {
            provided_addr,
            derived_addr,
        } => {
            visit(provided_addr, ReferenceClass::Excluded)?;
            visit_pubkey_or_string(derived_addr, visit)?;
        }
        SystemProgramLog::CreateAccountAlreadyInUse { addr }
        | SystemProgramLog::AllocateAlreadyInUse { addr }
        | SystemProgramLog::AllocateToMustSign { addr }
        | SystemProgramLog::AllocateAccountAlreadyInUse { addr }
        | SystemProgramLog::AssignAccountMustSign { addr }
        | SystemProgramLog::CreateAccountAccountAlreadyInUse { addr } => {
            visit_system_address(addr, visit)?;
        }
        SystemProgramLog::TransferFromMustSign { from } => {
            visit(from, ReferenceClass::Excluded)?;
        }
        SystemProgramLog::NonceAccountMustBeWriteable { account, .. }
        | SystemProgramLog::NonceAccountMustBeSigner { account, .. }
        | SystemProgramLog::NonceAccountMustSign { account, .. }
        | SystemProgramLog::NonceAccountStateInvalid { account, .. } => {
            visit_pubkey_or_string(account, visit)?;
        }
        SystemProgramLog::Instruction(_)
        | SystemProgramLog::AllocateRequestedTooLarge { .. }
        | SystemProgramLog::CreateAccountDataSizeLimitedInInnerInstructions { .. }
        | SystemProgramLog::TransferFromMustNotCarryData
        | SystemProgramLog::TransferInsufficient { .. }
        | SystemProgramLog::AdvanceNonceRecentBlockhashesEmpty
        | SystemProgramLog::InitializeNonceRecentBlockhashesEmpty
        | SystemProgramLog::AuthorizeNonceAccount { .. }
        | SystemProgramLog::NonceInsufficientLamports { .. }
        | SystemProgramLog::NonceCanOnlyAdvanceOncePerSlot { .. } => {}
    }
    Ok(())
}

fn visit_system_address(
    address: &mut SystemAddress,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    match address {
        SystemAddress::Pubkey(value) => visit_pubkey_or_string(value, visit),
        SystemAddress::Debug { address, base } => {
            visit_pubkey_or_string(address, visit)?;
            if let Some(base) = base {
                visit_pubkey_or_string(base, visit)?;
            }
            Ok(())
        }
    }
}

fn visit_pubkey_or_string(
    value: &mut PubkeyOrString,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    if let PubkeyOrString::Pubkey(key) = value {
        visit(key, ReferenceClass::Excluded)?;
    }
    Ok(())
}

fn compute_budget_key() -> [u8; 32] {
    solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111").to_bytes()
}

fn build_usage_sorted_registry(
    source: &MappedRegistry,
    counts: &[u32],
    target: &Path,
    sort_memory_mib: usize,
    pool: &rayon::ThreadPool,
) -> Result<(Vec<u32>, u64)> {
    ensure!(
        source.len == counts.len(),
        "registry/count vector length mismatch"
    );
    let scratch_bytes = sort_memory_mib
        .checked_mul(1 << 20)
        .context("sort memory byte count overflow")?;
    let chunk_records = (scratch_bytes / std::mem::size_of::<SortRecord>())
        .max(1)
        .min(source.len.saturating_add(1));
    let run_dir = target.join(".registry-reprocess-sort-runs");
    fs::create_dir(&run_dir).with_context(|| format!("create {}", run_dir.display()))?;
    let mut chunk = Vec::<SortRecord>::new();
    chunk
        .try_reserve_exact(chunk_records)
        .context("allocate bounded registry sort chunk")?;
    let mut runs = Vec::new();
    let builtin = compute_budget_key();
    let mut builtin_old_id = None;
    for (index, (key, &count)) in source.keys().iter().zip(counts).enumerate() {
        let old_id = u32::try_from(index + 1).context("source registry ID exceeds u32")?;
        if *key == builtin {
            ensure!(
                builtin_old_id.replace(old_id).is_none(),
                "duplicate ComputeBudget registry key"
            );
        }
        if count == 0 {
            continue;
        }
        chunk.push(SortRecord {
            count,
            key: *key,
            old_id,
        });
        if chunk.len() == chunk_records {
            spill_sort_run(&run_dir, &mut runs, &mut chunk, pool)?;
        }
    }
    let builtin_is_synthetic_prefix = counts
        .get(
            builtin_old_id
                .map(|id| id as usize - 1)
                .unwrap_or(usize::MAX),
        )
        .copied()
        .unwrap_or(0)
        == 0;
    if !chunk.is_empty() {
        spill_sort_run(&run_dir, &mut runs, &mut chunk, pool)?;
    }

    let registry_path = target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let counts_path = target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE);
    let mut registry_writer = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        File::create(&registry_path)
            .with_context(|| format!("create {}", registry_path.display()))?,
    );
    let mut counts_writer = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        File::create(&counts_path).with_context(|| format!("create {}", counts_path.display()))?,
    );
    let mut cursors = runs
        .iter()
        .map(|path| SortRunReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run, cursor) in cursors.iter_mut().enumerate() {
        if let Some(record) = cursor.next()? {
            heap.push(HeapRecord { record, run });
        }
    }
    let mut old_to_new = vec![0u32; source.len];
    let mut previous = None::<SortRecord>;
    let mut target_keys = 0u64;
    let mut emitted_count_sum = 0u64;
    if builtin_is_synthetic_prefix {
        target_keys = 1;
        registry_writer.write_all(&builtin)?;
        write_u32_varint(&mut counts_writer, 0)?;
        if let Some(old_id) = builtin_old_id {
            old_to_new[(old_id - 1) as usize] = 1;
        }
    }
    while let Some(item) = heap.pop() {
        if let Some(previous) = previous {
            ensure!(
                previous.cmp_canonical(&item.record) != Ordering::Greater,
                "external registry merge violated canonical ordering"
            );
            ensure!(
                previous.key != item.record.key,
                "duplicate key in source registry"
            );
        }
        target_keys = target_keys
            .checked_add(1)
            .context("target registry key count overflow")?;
        let new_id = u32::try_from(target_keys).context("target registry exceeds u32 ID space")?;
        registry_writer.write_all(&item.record.key)?;
        write_u32_varint(&mut counts_writer, item.record.count)?;
        emitted_count_sum = emitted_count_sum
            .checked_add(u64::from(item.record.count))
            .context("target registry count sum overflow")?;
        if item.record.old_id != 0 {
            let slot = old_to_new
                .get_mut((item.record.old_id - 1) as usize)
                .context("sort record old ID is outside remap")?;
            ensure!(
                *slot == 0,
                "duplicate old ID {} in sort output",
                item.record.old_id
            );
            *slot = new_id;
        }
        previous = Some(item.record);
        let cursor = cursors
            .get_mut(item.run)
            .context("sort heap references missing run")?;
        if let Some(record) = cursor.next()? {
            heap.push(HeapRecord {
                record,
                run: item.run,
            });
        }
    }
    for cursor in &cursors {
        ensure!(
            cursor.remaining == 0,
            "sort run was not consumed completely"
        );
    }
    let expected_count_sum = counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("eligible count sum overflow")
    })?;
    ensure!(
        emitted_count_sum == expected_count_sum,
        "target registry count sum mismatch"
    );
    registry_writer
        .flush()
        .with_context(|| format!("flush {}", registry_path.display()))?;
    counts_writer
        .flush()
        .with_context(|| format!("flush {}", counts_path.display()))?;
    drop(registry_writer);
    drop(counts_writer);
    drop(cursors);
    for path in runs {
        fs::remove_file(&path).with_context(|| format!("remove sort run {}", path.display()))?;
    }
    fs::remove_dir(&run_dir).with_context(|| format!("remove {}", run_dir.display()))?;
    Ok((old_to_new, target_keys))
}

fn spill_sort_run(
    directory: &Path,
    runs: &mut Vec<PathBuf>,
    records: &mut Vec<SortRecord>,
    pool: &rayon::ThreadPool,
) -> Result<()> {
    pool.install(|| records.par_sort_unstable_by(SortRecord::cmp_canonical));
    let path = directory.join(format!("run-{:05}.bin", runs.len()));
    let mut writer = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        File::create(&path).with_context(|| format!("create {}", path.display()))?,
    );
    writer.write_all(SORT_RUN_MAGIC)?;
    writer.write_all(&(records.len() as u64).to_le_bytes())?;
    for record in records.drain(..) {
        writer.write_all(&record.count.to_le_bytes())?;
        writer.write_all(&record.key)?;
        writer.write_all(&record.old_id.to_le_bytes())?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    runs.push(path);
    Ok(())
}

impl SortRunReader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let bytes = file.metadata()?.len();
        // A merge can have many runs under a small sort budget. Keep per-run buffering bounded;
        // all records are fixed-width and each cursor is read sequentially.
        let mut reader = BufReader::with_capacity(64 << 10, file);
        let mut header = [0u8; 16];
        reader.read_exact(&mut header)?;
        ensure!(
            &header[..8] == SORT_RUN_MAGIC,
            "invalid registry sort run magic"
        );
        let remaining = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let expected = 16u64
            .checked_add(
                remaining
                    .checked_mul(SORT_RECORD_BYTES as u64)
                    .context("sort run size overflow")?,
            )
            .context("sort run size overflow")?;
        ensure!(bytes == expected, "registry sort run length mismatch");
        Ok(Self { reader, remaining })
    }

    fn next(&mut self) -> Result<Option<SortRecord>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut bytes = [0u8; SORT_RECORD_BYTES];
        self.reader.read_exact(&mut bytes)?;
        self.remaining -= 1;
        Ok(Some(SortRecord {
            count: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
            key: bytes[4..36].try_into().unwrap(),
            old_id: u32::from_le_bytes(bytes[36..40].try_into().unwrap()),
        }))
    }
}

fn build_registry_index(target: &Path) -> Result<()> {
    let registry = MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let index = KeyIndex::build_from_slice_low_memory(registry.keys());
    let path = target.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    index.write(&path)?;
    ensure!(
        index.len() == registry.len,
        "built registry MPHF length mismatch"
    );
    Ok(())
}

fn validate_source_registry_index(source: &Path, registry: &MappedRegistry) -> Result<KeyIndex> {
    let path = source.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    let index = KeyIndex::load(&path)
        .with_context(|| format!("load strict source registry index {}", path.display()))?;
    ensure!(
        index.len() == registry.len,
        "source registry MPHF has {} keys, registry.bin has {}",
        index.len(),
        registry.len
    );
    for (offset, key) in registry.keys().iter().enumerate() {
        let expected = u32::try_from(offset + 1).context("source registry ID exceeds u32")?;
        ensure!(
            index.lookup(key) == Some(expected),
            "source registry contains a duplicate key or MPHF mismatch at ID {expected}"
        );
    }
    Ok(index)
}

fn validate_canonical_registry(target: &Path, expected_keys: u64) -> Result<()> {
    let registry = MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    ensure!(
        registry.len as u64 == expected_keys,
        "target registry key count mismatch"
    );
    let counts = read_registry_counts(
        &target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
        registry.len,
    )?;
    let synthetic_builtin_prefix =
        registry.keys().first() == Some(&compute_budget_key()) && counts.first() == Some(&0);
    let ordered_start = if synthetic_builtin_prefix { 2 } else { 1 };
    for index in ordered_start..registry.len {
        let previous = SortRecord {
            count: counts[index - 1],
            key: registry.keys()[index - 1],
            old_id: 0,
        };
        let current = SortRecord {
            count: counts[index],
            key: registry.keys()[index],
            old_id: 0,
        };
        ensure!(
            previous.cmp_canonical(&current) != Ordering::Greater,
            "target registry is not canonical at IDs {} and {}",
            index,
            index + 1
        );
    }
    let index = KeyIndex::load(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))?;
    ensure!(
        index.len() == registry.len,
        "target registry MPHF key count mismatch"
    );
    for (offset, key) in registry.keys().iter().enumerate() {
        ensure!(
            index.lookup(key) == Some((offset + 1) as u32),
            "target registry MPHF mismatch at ID {}",
            offset + 1
        );
    }
    ensure!(
        index.lookup(&compute_budget_key()).is_some(),
        "target registry omits ComputeBudget"
    );
    Ok(())
}

impl From<LegacyBlockAccessBlobV1> for ArchiveV2BlockAccessBlob {
    fn from(value: LegacyBlockAccessBlobV1) -> Self {
        Self {
            version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
            flags: 0,
            blockhash: value.blockhash,
            previous_blockhash: value.previous_blockhash,
            signature_counts: value.signature_counts,
            signatures: value.signatures,
            pubkeys: value.pubkeys,
            blockhashes: value.blockhashes,
            vote_hashes: Vec::new(),
        }
    }
}

impl From<LegacyBlockAccessBlobV2NoVotes> for ArchiveV2BlockAccessBlob {
    fn from(value: LegacyBlockAccessBlobV2NoVotes) -> Self {
        Self {
            version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
            flags: value.flags,
            blockhash: value.blockhash,
            previous_blockhash: value.previous_blockhash,
            signature_counts: value.signature_counts,
            signatures: value.signatures,
            pubkeys: value.pubkeys,
            blockhashes: value.blockhashes,
            vote_hashes: Vec::new(),
        }
    }
}

#[allow(dead_code)]
fn decode_access_blob(bytes: &[u8], block_id: u32) -> Result<ArchiveV2BlockAccessBlob> {
    let current_error = match wincode::config::deserialize_exact(
        bytes,
        bounded_wincode_config::<MAX_ACCESS_FRAME_BYTES_USIZE>(),
    ) {
        Ok(blob) => return Ok(blob),
        Err(error) => error,
    };
    let no_votes_error = match wincode::config::deserialize_exact::<LegacyBlockAccessBlobV2NoVotes, _>(
        bytes,
        bounded_wincode_config::<MAX_ACCESS_FRAME_BYTES_USIZE>(),
    ) {
        Ok(blob) if blob.version == WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION => {
            return Ok(blob.into());
        }
        Ok(blob) => anyhow!("decoded v2-no-votes payload with version {}", blob.version),
        Err(error) => anyhow!(error),
    };
    match wincode::config::deserialize_exact::<LegacyBlockAccessBlobV1, _>(
        bytes,
        bounded_wincode_config::<MAX_ACCESS_FRAME_BYTES_USIZE>(),
    ) {
        Ok(blob) if blob.version == 1 => Ok(blob.into()),
        Ok(blob) => bail!(
            "legacy block-access {block_id} has version {}",
            blob.version
        ),
        Err(v1_error) => bail!(
            "cannot decode block-access {block_id}: current={current_error}; v2-no-votes={no_votes_error:#}; v1={v1_error}"
        ),
    }
}

#[allow(dead_code)]
fn rewrite_access_if_present(
    source: &Path,
    target: &Path,
    source_registry: &MappedRegistry,
    old_to_new: &[u32],
    hot_rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    expected_access_ids: &[Vec<u32>],
    expected_signature_counts: &[Vec<u8>],
    target_registry: &MappedRegistry,
) -> Result<()> {
    ensure!(
        expected_access_ids.len() == hot_rows.len()
            && expected_signature_counts.len() == hot_rows.len(),
        "rewritten access expectations do not align with hot rows"
    );
    let source_blob_path = source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let source_index_path = source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let blob_exists = source_blob_path.exists();
    let index_exists = source_index_path.exists();
    ensure!(
        blob_exists == index_exists,
        "source has only one of block-access blob/index"
    );
    if !blob_exists {
        ensure!(
            !source.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE).exists(),
            "source get-block index exists without block-access sidecar"
        );
        return Ok(());
    }
    preflight_access_index(&source_index_path, hot_rows.len())?;
    let source_index = read_archive_v2_block_access_index(&source_index_path)?;
    ensure!(
        source_index.flags == 0,
        "unsupported source block-access index flags"
    );
    ensure!(
        source_index.rows.len() == hot_rows.len(),
        "block-access/hot index row count mismatch"
    );
    ensure!(
        fs::metadata(&source_blob_path)?.len() == source_index.blob_file_bytes,
        "block-access blob length does not match index"
    );
    let mut source_file = File::open(&source_blob_path)?;
    let mut signatures_file = File::open(source.join(ARCHIVE_V2_SIGNATURES_FILE))?;
    let target_blob_path = target.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let mut target_file =
        BufWriter::with_capacity(IO_BUFFER_BYTES, File::create(&target_blob_path)?);
    let mut target_rows = Vec::with_capacity(source_index.rows.len());
    let mut target_offset = 0u64;
    let mut source_offset = 0u64;
    for ((position, source_row), hot_row) in source_index.rows.iter().enumerate().zip(hot_rows) {
        ensure!(
            source_row.block_id as usize == position
                && source_row.block_id == hot_row.block_id
                && source_row.slot == hot_row.slot
                && source_row.tx_count == hot_row.tx_count
                && source_row.signature_count == hot_row.signature_count,
            "block-access index mismatch at block_id {}",
            hot_row.block_id
        );
        ensure!(
            source_row.access_len > 0,
            "empty block-access payload {}",
            source_row.block_id
        );
        ensure!(
            source_row.access_offset == source_offset,
            "non-contiguous block-access offset at block_id {}: got {}, expected {}",
            source_row.block_id,
            source_row.access_offset,
            source_offset
        );
        source_offset = source_offset
            .checked_add(u64::from(source_row.access_len))
            .context("source block-access offset overflow")?;
        ensure!(
            source_offset <= source_index.blob_file_bytes,
            "block-access row {} extends beyond source blob",
            source_row.block_id
        );
        source_file.seek(SeekFrom::Start(source_row.access_offset))?;
        let mut bytes = vec![0u8; source_row.access_len as usize];
        source_file.read_exact(&mut bytes)?;
        let mut blob = decode_access_blob(&bytes, source_row.block_id)?;
        ensure!(
            blob.version == WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION && blob.flags == 0,
            "unsupported block-access header at block_id {}: version={} flags={:#x}",
            source_row.block_id,
            blob.version,
            blob.flags
        );
        ensure!(
            blob.signature_counts == expected_signature_counts[position],
            "block-access signature-count content mismatch at block_id {}",
            source_row.block_id
        );
        ensure!(
            blob.signatures.len() == source_row.signature_count as usize * 64,
            "block-access signature bytes mismatch at block_id {}",
            source_row.block_id
        );
        let signature_offset = hot_row
            .first_signature_ordinal
            .checked_mul(64)
            .context("block signature offset overflow")?;
        signatures_file.seek(SeekFrom::Start(signature_offset))?;
        let mut expected_signatures = vec![0u8; blob.signatures.len()];
        signatures_file.read_exact(&mut expected_signatures)?;
        ensure!(
            blob.signatures == expected_signatures,
            "block-access signatures disagree with signatures sidecar at block_id {}",
            source_row.block_id
        );
        let mut remapped = Vec::with_capacity(blob.pubkeys.len());
        for entry in blob.pubkeys {
            ensure!(entry.id != 0, "block-access entry uses ID 0");
            ensure!(
                source_registry.key(entry.id)? == entry.pubkey,
                "block-access pubkey bytes disagree with source registry for ID {}",
                entry.id
            );
            let new_id = *old_to_new
                .get((entry.id - 1) as usize)
                .context("block-access ID is outside remap")?;
            if new_id != 0 {
                ensure!(
                    target_registry.key(new_id)? == entry.pubkey,
                    "remapped block-access pubkey bytes disagree with target registry for ID {new_id}"
                );
                remapped.push(ArchiveV2BlockAccessPubkey {
                    id: new_id,
                    pubkey: entry.pubkey,
                });
            }
        }
        remapped.sort_unstable_by_key(|entry| entry.id);
        for pair in remapped.windows(2) {
            ensure!(
                pair[0].id != pair[1].id,
                "duplicate remapped block-access ID {}",
                pair[0].id
            );
        }
        ensure!(
            remapped
                .iter()
                .map(|entry| entry.id)
                .eq(expected_access_ids[position].iter().copied()),
            "block-access pubkey set is incomplete or stale at block_id {}",
            source_row.block_id
        );
        blob.version = WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION;
        blob.pubkeys = remapped;
        let encoded = wincode::config::serialize(&blob, wincode_leb128_config())?;
        ensure!(
            encoded.len() as u64 <= ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
            "rewritten block-access {} exceeds shared frame limit",
            source_row.block_id
        );
        let access_len = u32::try_from(encoded.len()).context("block-access frame exceeds u32")?;
        target_file.write_all(&encoded)?;
        target_rows.push(ArchiveV2BlockAccessIndexRow {
            block_id: source_row.block_id,
            slot: source_row.slot,
            access_offset: target_offset,
            access_len,
            tx_count: source_row.tx_count,
            signature_count: source_row.signature_count,
        });
        target_offset = target_offset
            .checked_add(u64::from(access_len))
            .context("block-access target offset overflow")?;
    }
    ensure!(
        source_offset == source_index.blob_file_bytes,
        "block-access index covers {source_offset} bytes but declares {}",
        source_index.blob_file_bytes
    );
    target_file.flush()?;
    drop(target_file);
    write_archive_v2_block_access_index(
        &target.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
        target_offset,
        0,
        &target_rows,
    )?;
    let get_block = build_get_block_rows(hot_rows, &target_rows)?;
    write_archive_v2_get_block_index(&target.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE), &get_block)?;
    Ok(())
}

#[allow(dead_code)]
fn preflight_access_index(path: &Path, expected_rows: usize) -> Result<()> {
    let metadata = regular_file_metadata(path)?;
    ensure!(
        metadata.len() >= ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN as u64,
        "block-access index is shorter than its header"
    );
    let mut file = File::open(path)?;
    let mut header = [0u8; ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN];
    file.read_exact(&mut header)?;
    ensure!(
        &header[..8] == ARCHIVE_V2_BLOCK_ACCESS_INDEX_MAGIC,
        "invalid block-access index magic"
    );
    let row_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
    ensure!(
        row_count == expected_rows as u64,
        "block-access index declares {row_count} rows; hot index has {expected_rows}"
    );
    let expected_len = u64::try_from(ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN)?
        .checked_add(
            row_count
                .checked_mul(u64::try_from(ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN)?)
                .context("block-access index row byte count overflow")?,
        )
        .context("block-access index byte length overflow")?;
    ensure!(
        metadata.len() == expected_len,
        "block-access index has {} bytes; expected {expected_len}",
        metadata.len()
    );
    Ok(())
}

fn build_get_block_rows(
    hot_rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    access_rows: &[ArchiveV2BlockAccessIndexRow],
) -> Result<Vec<ArchiveV2GetBlockIndexRow>> {
    ensure!(hot_rows.len() == access_rows.len());
    let mut rows = vec![ArchiveV2GetBlockIndexRow::missing(); crate::SLOTS_PER_EPOCH as usize];
    for (hot, access) in hot_rows.iter().zip(access_rows) {
        ensure!(hot.block_id == access.block_id && hot.slot == access.slot);
        let offset = (hot.slot % crate::SLOTS_PER_EPOCH) as usize;
        ensure!(
            rows[offset].is_missing(),
            "duplicate get-block slot {}",
            hot.slot
        );
        rows[offset] = ArchiveV2GetBlockIndexRow {
            block_offset: hot.compressed_offset,
            block_len: hot.compressed_len,
            access_offset: access.access_offset,
            access_len: access.access_len,
        };
    }
    Ok(rows)
}

fn load_access_build_context(
    source: &Path,
    expected_blocks: usize,
    epoch: u64,
) -> Result<Option<AccessBuildContext>> {
    let blob_path = source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let index_path = source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let blob_exists = fs::symlink_metadata(&blob_path)
        .map(|metadata| metadata.file_type().is_file())
        .or_else(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(error)
            }
        })?;
    let index_exists = fs::symlink_metadata(&index_path)
        .map(|metadata| metadata.file_type().is_file())
        .or_else(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(error)
            }
        })?;
    ensure!(
        blob_exists == index_exists,
        "source has only one of block-access blob/index"
    );
    if !blob_exists {
        ensure!(
            !source.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE).try_exists()?,
            "source get-block index exists without block-access sidecar"
        );
        return Ok(None);
    }

    // The old access payload is not trusted or remapped. Rebuild canonical content from the
    // rewritten block and immutable registries, but still require the advertised source files to
    // be regular artifacts so the receipt can bind them.
    regular_file_metadata(&blob_path)?;
    regular_file_metadata(&index_path)?;
    let blockhash_path = source.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
    let blockhash_metadata = regular_file_metadata(&blockhash_path)?;
    let legacy_blockhash_bytes = u64::try_from(expected_blocks)?
        .checked_mul(32)
        .context("blockhash registry length overflow")?;
    let boundary_blockhash_bytes = u64::try_from(expected_blocks)?
        .checked_add(1)
        .and_then(|records| records.checked_mul(32))
        .context("boundary-prefixed blockhash registry length overflow")?;
    ensure!(
        matches!(blockhash_metadata.len(), len if len == legacy_blockhash_bytes || len == boundary_blockhash_bytes),
        "blockhash registry has invalid length {} for {expected_blocks} blocks (expected {legacy_blockhash_bytes} or {boundary_blockhash_bytes})",
        blockhash_metadata.len(),
    );
    let blockhashes = super::load_blockhash_registry_plain(&blockhash_path)?;
    let previous_tail = if blockhash_metadata.len() == legacy_blockhash_bytes {
        let tail = load_previous_blockhash_tail_bounded(source, epoch)?;
        ensure!(
            !tail.is_empty(),
            "legacy block-access rebuild requires a non-empty previous blockhash tail"
        );
        tail
    } else {
        Vec::new()
    };
    let vote_path = source.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE);
    let vote_hashes = if vote_path.try_exists()? {
        let metadata = regular_file_metadata(&vote_path)?;
        let max_vote_bytes = u64::try_from(expected_blocks)?
            .checked_mul(65)
            .context("vote hash registry bound overflow")?;
        ensure!(
            metadata.len() == max_vote_bytes,
            "vote hash registry has invalid length {} for {expected_blocks} blocks",
            metadata.len()
        );
        super::load_vote_hash_registry(&vote_path)?
    } else {
        Vec::new()
    };
    Ok(Some(AccessBuildContext {
        blockhashes,
        previous_tail,
        vote_hashes,
    }))
}

fn load_previous_blockhash_tail_bounded(
    source: &Path,
    epoch: u64,
) -> Result<Vec<super::PreviousBlockhash>> {
    let path = source.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
    let (mut file, metadata) = match open_regular_read(&path) {
        Ok(opened) => opened,
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::NotFound) =>
        {
            return Ok(Vec::new());
        }
        Err(error) => return Err(error),
    };
    let max_bytes = u64::try_from(super::ROLLING_BLOCKHASH_CAPACITY)?
        .checked_mul(40)
        .context("previous blockhash tail bound overflow")?;
    ensure!(
        metadata.len() <= max_bytes,
        "previous blockhash tail has {} bytes, exceeding {max_bytes}",
        metadata.len()
    );
    if metadata.len() == 0 {
        return Ok(Vec::new());
    }
    let mut bytes = vec![0u8; usize::try_from(metadata.len())?];
    file.read_exact(&mut bytes)?;
    ensure_open_file_unchanged(&path, &file, &metadata)?;
    decode_previous_blockhash_tail_bytes(&bytes, epoch)
}

fn decode_previous_blockhash_tail_bytes(
    bytes: &[u8],
    epoch: u64,
) -> Result<Vec<super::PreviousBlockhash>> {
    ensure!(epoch != 0, "genesis has no previous blockhash tail");
    ensure!(!bytes.is_empty(), "previous blockhash tail is empty");

    // A legacy hash-only tail can have a byte length divisible by both 32 and 40 (including the
    // normal 300-row/9,600-byte file).  Length alone therefore cannot select the schema.  Current
    // rows are accepted only when their slots form a strictly increasing sequence in the previous
    // epoch; if both schemas remain possible, fail closed rather than reinterpret hash bytes.
    let previous_epoch = epoch.checked_sub(1).context("previous epoch underflow")?;
    let previous_epoch_start = previous_epoch
        .checked_mul(crate::SLOTS_PER_EPOCH)
        .context("previous epoch slot range overflow")?;
    let epoch_start = epoch
        .checked_mul(crate::SLOTS_PER_EPOCH)
        .context("epoch slot range overflow")?;

    let current = if bytes.len().is_multiple_of(40)
        && bytes.len() / 40 <= super::ROLLING_BLOCKHASH_CAPACITY
    {
        let mut rows = Vec::new();
        rows.try_reserve_exact(bytes.len() / 40)
            .context("allocate current previous blockhash tail")?;
        let mut previous_slot = None;
        let mut slots_are_canonical = true;
        for chunk in bytes.chunks_exact(40) {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&chunk[..32]);
            let slot = u64::from_le_bytes(chunk[32..40].try_into().unwrap());
            if !(previous_epoch_start..epoch_start).contains(&slot)
                || previous_slot.is_some_and(|previous| slot <= previous)
            {
                slots_are_canonical = false;
                break;
            }
            rows.push(super::PreviousBlockhash { hash, slot });
            previous_slot = Some(slot);
        }
        slots_are_canonical.then_some(rows)
    } else {
        None
    };

    let legacy = if bytes.len().is_multiple_of(32)
        && bytes.len() / 32 <= super::ROLLING_BLOCKHASH_CAPACITY
    {
        let mut rows = Vec::new();
        rows.try_reserve_exact(bytes.len() / 32)
            .context("allocate legacy previous blockhash tail")?;
        for chunk in bytes.chunks_exact(32) {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(chunk);
            rows.push(super::PreviousBlockhash { hash, slot: 0 });
        }
        Some(rows)
    } else {
        None
    };

    match (current, legacy) {
        (Some(rows), None) | (None, Some(rows)) => Ok(rows),
        (Some(_), Some(_)) => bail!(
            "previous blockhash tail byte length {} is ambiguous between current and legacy schemas",
            bytes.len()
        ),
        (None, None) => bail!(
            "previous blockhash tail has no valid bounded current or legacy schema (bytes={}, epoch={epoch})",
            bytes.len()
        ),
    }
}

fn attach_block_signatures(file: &mut File, inputs: &mut [CompressedBlockInput]) -> Result<()> {
    for input in inputs {
        let offset = input
            .row
            .first_signature_ordinal
            .checked_mul(64)
            .context("block signature offset overflow")?;
        let len = usize::try_from(
            u64::from(input.row.signature_count)
                .checked_mul(64)
                .context("block signature byte length overflow")?,
        )?;
        file.seek(SeekFrom::Start(offset))?;
        let mut signatures = vec![0u8; len];
        file.read_exact(&mut signatures)?;
        input.signatures = Some(signatures);
    }
    Ok(())
}

const INDEPENDENT_SIDECARS: &[&str] = &[
    ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_SHREDDING_FILE,
    BLOCK_TIME_GAP_FILE,
];

fn copy_independent_sidecars(
    source: &Path,
    target: &Path,
) -> Result<BTreeMap<String, FileBinding>> {
    let mut copied = BTreeMap::new();
    for &name in INDEPENDENT_SIDECARS {
        let source_path = source.join(name);
        match fs::symlink_metadata(&source_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("inspect {}", source_path.display()));
            }
            Ok(metadata) => ensure!(
                metadata.file_type().is_file(),
                "sidecar is not a regular file: {}",
                source_path.display()
            ),
        }
        let target_path = target.join(name);
        let binding = match clone_or_copy_file(&source_path, &target_path)? {
            Some(binding) => binding,
            None => hash_file(&source_path)?,
        };
        let target_metadata = regular_file_metadata(&target_path)?;
        ensure!(
            target_metadata.len() == binding.bytes,
            "copied sidecar length mismatch for {name}: target={} source={}",
            target_metadata.len(),
            binding.bytes
        );
        copied.insert(name.to_owned(), binding);
    }
    for required in [
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ARCHIVE_V2_POH_FILE,
        ARCHIVE_V2_SHREDDING_FILE,
    ] {
        ensure!(
            copied.contains_key(required),
            "required independent sidecar is missing: {required}"
        );
    }
    Ok(copied)
}

#[cfg(target_os = "macos")]
fn clone_or_copy_file(source: &Path, target: &Path) -> Result<Option<FileBinding>> {
    use std::os::unix::ffi::OsStrExt;
    let source_c =
        CString::new(source.as_os_str().as_bytes()).context("source path contains NUL")?;
    let target_c =
        CString::new(target.as_os_str().as_bytes()).context("target path contains NUL")?;
    // SAFETY: both path strings remain valid, NUL-terminated C strings for the duration of the
    // call. `target` is inside a private fresh staging directory and does not exist.
    if unsafe { libc::clonefile(source_c.as_ptr(), target_c.as_ptr(), 0) } == 0 {
        return Ok(None);
    }
    if target.exists() {
        fs::remove_file(target)
            .with_context(|| format!("remove incomplete clone destination {}", target.display()))?;
    }
    copy_file_with_hash(source, target).map(Some)
}

fn analyze_target_block(
    input: CompressedBlockInput,
    registry: &MappedRegistry,
) -> Result<BlockSemantic> {
    let row = input.row;
    let mut block = decode_hot_block(&input)?;
    normalize_block(
        &mut block,
        u64::from(row.block_id),
        row.slot,
        |key, _class| resolve_compact_pubkey(*key, registry),
    )
}

fn recompute_source_canonical_counts(
    source: &Path,
    target: &Path,
    receipt: &RegistryReprocessReceipt,
    epoch: u64,
) -> Result<(SemanticBinding, FileBinding)> {
    let manifest = read_source_manifest(source)?;
    let footer = validate_source_meta_for_deep(source)?;
    let registry = MappedRegistry::open(&source.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    ensure!(
        manifest.registry_keys == registry.len as u64
            && receipt.source_registry_keys == registry.len as u64,
        "source registry key count disagrees with manifest or receipt"
    );
    let source_index = validate_source_registry_index(source, &registry)?;
    let mut declared_counts = read_registry_counts(
        &source.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
        registry.len,
    )?;
    let declared_sum = declared_counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("source registry reference count overflow")
    })?;
    ensure!(
        declared_sum == manifest.references,
        "source registry count sum {declared_sum} != manifest references {}",
        manifest.references
    );

    let blocks_path = source.join(ARCHIVE_V2_BLOCKS_FILE);
    let hot_index = read_archive_v2_hot_block_index(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    validate_hot_index(&blocks_path, &hot_index, epoch)?;
    ensure!(
        footer.blocks == hot_index.rows.len() as u64,
        "source footer block count mismatch during deep validation"
    );
    let final_transactions = hot_index
        .rows
        .last()
        .map(|row| {
            row.first_tx_ordinal
                .checked_add(u64::from(row.tx_count))
                .context("deep source transaction ordinal overflow")
        })
        .transpose()?
        .unwrap_or(0);
    let final_signatures = hot_index
        .rows
        .last()
        .map(|row| {
            row.first_signature_ordinal
                .checked_add(u64::from(row.signature_count))
                .context("deep source signature ordinal overflow")
        })
        .transpose()?
        .unwrap_or(0);
    ensure!(
        footer.transactions == final_transactions,
        "source footer transaction count mismatch during deep validation"
    );
    ensure!(
        regular_file_metadata(&source.join(ARCHIVE_V2_SIGNATURES_FILE))?.len()
            == final_signatures
                .checked_mul(64)
                .context("deep source signature byte count overflow")?,
        "source signatures sidecar length mismatch during deep validation"
    );

    let mut eligible_counts = Vec::new();
    eligible_counts
        .try_reserve_exact(registry.len)
        .context("allocate deep eligible-count vector")?;
    eligible_counts.resize(registry.len, 0u32);
    let threads = receipt.threads.clamp(1, 64);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("registry-deep-validate-{index}"))
        .build()
        .context("build deep registry validation pool")?;
    let max_rows = threads.saturating_mul(2).clamp(1, 64);
    let (mut blocks_file, blocks_metadata) = open_regular_read(&blocks_path)?;
    ensure!(blocks_metadata.len() == hot_index.blob_file_bytes);
    let mut block_hasher = Sha256::new();
    let mut semantics = SemanticAccumulator::new();
    let mut start = 0usize;
    while start < hot_index.rows.len() {
        let end = hot_batch_end(&hot_index.rows, start, max_rows, false)?;
        let inputs = read_compressed_block_batch(
            &mut blocks_file,
            &hot_index.rows[start..end],
            Some(&mut block_hasher),
        )?;
        let analyses = pool.install(|| {
            inputs
                .into_par_iter()
                .map(|input| analyze_source_block(input, &registry))
                .collect::<Result<Vec<_>>>()
        })?;
        for analysis in analyses {
            merge_count_runs(&mut eligible_counts, &analysis.eligible, false)?;
            merge_count_runs(&mut declared_counts, &analysis.all, true)?;
            semantics.push(&analysis.semantic)?;
        }
        start = end;
    }
    ensure_open_file_unchanged(&blocks_path, &blocks_file, &blocks_metadata)?;
    ensure!(
        declared_counts.iter().all(|&remaining| remaining == 0),
        "source registry counts do not match the deep typed-reference traversal"
    );
    let semantics = semantics.finish();
    ensure!(
        semantics.pubkey_references == manifest.references,
        "deep source traversal reference count mismatch"
    );
    let eligible_references = eligible_counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("deep eligible reference count overflow")
    })?;
    ensure!(
        eligible_references == receipt.eligible_references,
        "recomputed eligible references {eligible_references} != receipt {}",
        receipt.eligible_references
    );
    validate_target_registry_against_recomputed(
        &registry,
        &source_index,
        &eligible_counts,
        target,
        receipt.target_registry_keys,
        receipt.eligible_references,
    )?;
    Ok((
        semantics,
        FileBinding {
            bytes: hot_index.blob_file_bytes,
            sha256: hex_digest(block_hasher.finalize()),
        },
    ))
}

fn validate_target_registry_against_recomputed(
    source_registry: &MappedRegistry,
    source_index: &KeyIndex,
    eligible_counts: &[u32],
    target: &Path,
    receipt_target_keys: u64,
    receipt_eligible_references: u64,
) -> Result<()> {
    ensure!(source_registry.len == eligible_counts.len());
    let target_registry = MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let target_counts = read_registry_counts(
        &target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
        target_registry.len,
    )?;
    let eligible_sum = eligible_counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("eligible count sum overflow")
    })?;
    ensure!(
        eligible_sum == receipt_eligible_references,
        "eligible count sum {eligible_sum} != receipt {receipt_eligible_references}"
    );
    let positive_keys = eligible_counts.iter().filter(|&&count| count != 0).count();
    let builtin = compute_budget_key();
    let builtin_source_id = source_index.lookup(&builtin).filter(|&id| {
        source_registry
            .key(id)
            .is_ok_and(|source_key| source_key == builtin)
    });
    let builtin_count = builtin_source_id
        .and_then(|id| eligible_counts.get((id - 1) as usize).copied())
        .unwrap_or(0);
    let synthetic_builtin = builtin_count == 0;
    let expected_target_keys = positive_keys
        .checked_add(usize::from(synthetic_builtin))
        .context("expected target key count overflow")?;
    ensure!(
        target_registry.len == expected_target_keys
            && target_registry.len as u64 == receipt_target_keys,
        "target registry key set does not match recomputed eligible source keys"
    );

    let mut matched_positive = 0usize;
    let mut target_sum = 0u64;
    let mut saw_synthetic_builtin = false;
    for (position, (&key, &target_count)) in target_registry
        .keys()
        .iter()
        .zip(&target_counts)
        .enumerate()
    {
        if synthetic_builtin && key == builtin {
            ensure!(
                position == 0 && target_count == 0 && !saw_synthetic_builtin,
                "synthetic ComputeBudget must be the unique zero-count ID-1 prefix"
            );
            saw_synthetic_builtin = true;
            continue;
        }
        let source_id = source_index
            .lookup(&key)
            .filter(|&id| {
                source_registry
                    .key(id)
                    .is_ok_and(|source_key| source_key == key)
            })
            .with_context(|| {
                format!(
                    "target registry key at ID {} is absent from the source registry",
                    position + 1
                )
            })?;
        let expected_count = eligible_counts[(source_id - 1) as usize];
        ensure!(
            expected_count != 0,
            "target registry retains source key ID {source_id} with zero canonical usage"
        );
        ensure!(
            target_count == expected_count,
            "target registry count mismatch at target ID {}: target={target_count} recomputed={expected_count}",
            position + 1
        );
        matched_positive += 1;
        target_sum = target_sum
            .checked_add(u64::from(target_count))
            .context("target canonical count sum overflow")?;
    }
    ensure!(
        saw_synthetic_builtin == synthetic_builtin,
        "target synthetic ComputeBudget presence mismatch"
    );
    ensure!(
        matched_positive == positive_keys,
        "target registry omits or duplicates recomputed eligible source keys"
    );
    ensure!(
        target_sum == receipt_eligible_references,
        "target registry count sum {target_sum} != receipt {receipt_eligible_references}"
    );
    Ok(())
}

fn validate_source_meta_for_deep(source: &Path) -> Result<WincodeArchiveV2Footer> {
    let path = source.join(ARCHIVE_V2_META_FILE);
    let (file, metadata) = open_regular_read(&path)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let first_bytes = read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?
        .context("source metadata is empty")?;
    let first: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &first_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    let ArchiveV2HotMetaRecord::Header(header) = first else {
        bail!("source metadata does not start with a header");
    };
    let expected_flags = WINCODE_ARCHIVE_V2_FLAG_LEB128
        | WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY
        | WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS;
    ensure!(
        header.version == WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION && header.flags == expected_flags,
        "source metadata is not strict first-seen/all-reference Compact-V2"
    );
    let second_bytes = read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?
        .context("source metadata is missing footer")?;
    let second: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &second_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    let ArchiveV2HotMetaRecord::Footer(footer) = second else {
        bail!("source metadata footer is missing or is genesis");
    };
    ensure!(
        read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?.is_none(),
        "source metadata has trailing records"
    );
    ensure_open_file_unchanged(&path, reader.get_ref(), &metadata)?;
    Ok(footer)
}

fn scan_target_generation_semantics(
    target: &Path,
    epoch: u64,
) -> Result<(SemanticBinding, FileBinding)> {
    let blocks_path = target.join(ARCHIVE_V2_BLOCKS_FILE);
    let index = read_archive_v2_hot_block_index(&target.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    validate_hot_index(&blocks_path, &index, epoch)?;
    let registry = MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let (mut file, metadata) = open_regular_read(&blocks_path)?;
    ensure!(metadata.len() == index.blob_file_bytes);
    let mut hasher = Sha256::new();
    let mut semantics = SemanticAccumulator::new();
    for row in &index.rows {
        let mut input =
            read_compressed_block_batch(&mut file, std::slice::from_ref(row), Some(&mut hasher))?;
        let input = input.pop().expect("one requested block");
        semantics.push(&analyze_target_block(input, &registry)?)?;
    }
    ensure_open_file_unchanged(&blocks_path, &file, &metadata)?;
    Ok((
        semantics.finish(),
        FileBinding {
            bytes: index.blob_file_bytes,
            sha256: hex_digest(hasher.finalize()),
        },
    ))
}

fn hash_file(path: &Path) -> Result<FileBinding> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(path)
        .with_context(|| format!("open bound artifact {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("stat bound artifact {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "bound artifact is not a regular file: {}",
        path.display()
    );
    let mut file = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    let mut bytes = 0u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("artifact byte count overflow")?;
    }
    ensure!(
        bytes == metadata.len(),
        "artifact changed length while hashing: {}",
        path.display()
    );
    let after = file.get_ref().metadata()?;
    ensure!(
        same_file_snapshot(&metadata, &after),
        "artifact changed while hashing: {}",
        path.display()
    );
    let path_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("reinspect bound artifact {}", path.display()))?;
    ensure!(
        path_metadata.file_type().is_file() && same_file_snapshot(&metadata, &path_metadata),
        "artifact path changed while hashing: {}",
        path.display()
    );
    Ok(FileBinding {
        bytes,
        sha256: hex_digest(hasher.finalize()),
    })
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len()
}

#[cfg(unix)]
fn same_file_snapshot(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    same_file_identity(left, right)
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

#[cfg(not(unix))]
fn same_file_snapshot(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    same_file_identity(left, right) && left.modified().ok() == right.modified().ok()
}

fn add_binding_if_file(
    bindings: &mut BTreeMap<String, FileBinding>,
    directory: &Path,
    name: &str,
) -> Result<()> {
    let path = directory.join(name);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
        Ok(_) => {
            ensure!(
                bindings
                    .insert(name.to_owned(), hash_file(&path)?)
                    .is_none(),
                "duplicate file binding {name}"
            );
            Ok(())
        }
    }
}

fn source_file_bindings(
    source: &Path,
    copied: &BTreeMap<String, FileBinding>,
    blocks: FileBinding,
) -> Result<BTreeMap<String, FileBinding>> {
    let mut bindings = BTreeMap::new();
    bindings.insert(ARCHIVE_V2_BLOCKS_FILE.to_owned(), blocks);
    for name in [
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_BLOCK_ACCESS_FILE,
        ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    ] {
        add_binding_if_file(&mut bindings, source, name)?;
    }
    for (name, binding) in copied {
        ensure!(
            bindings.insert(name.clone(), binding.clone()).is_none(),
            "duplicate source binding {name}"
        );
    }
    Ok(bindings)
}

fn target_file_bindings(
    target: &Path,
    copied: &BTreeMap<String, FileBinding>,
    blocks: FileBinding,
    access: Option<FileBinding>,
) -> Result<BTreeMap<String, FileBinding>> {
    let mut bindings = BTreeMap::new();
    bindings.insert(ARCHIVE_V2_BLOCKS_FILE.to_owned(), blocks);
    if let Some(access) = access {
        bindings.insert(ARCHIVE_V2_BLOCK_ACCESS_FILE.to_owned(), access);
    }
    for name in [
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    ] {
        add_binding_if_file(&mut bindings, target, name)?;
    }
    for (name, binding) in copied {
        ensure!(
            bindings.insert(name.clone(), binding.clone()).is_none(),
            "duplicate target binding {name}"
        );
    }
    Ok(bindings)
}

fn validate_bound_files_except_blocks(
    directory: &Path,
    expected: &BTreeMap<String, FileBinding>,
) -> Result<()> {
    for (name, expected) in expected {
        if name == ARCHIVE_V2_BLOCKS_FILE {
            continue;
        }
        ensure!(
            !name.contains('/') && !name.contains('\\'),
            "receipt contains nested artifact name"
        );
        let actual = hash_file(&directory.join(name))?;
        ensure!(&actual == expected, "artifact binding mismatch for {name}");
    }
    Ok(())
}

fn generation_digest(files: &BTreeMap<String, FileBinding>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(GENERATION_DOMAIN);
    hasher.update((files.len() as u64).to_le_bytes());
    for (name, binding) in files {
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(binding.bytes.to_le_bytes());
        hasher.update(binding.sha256.as_bytes());
    }
    hex_digest(hasher.finalize())
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn validate_hex_sha256(value: &str, label: &str) -> Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{label} is not a lowercase SHA-256 digest"
    );
    Ok(())
}

fn validate_receipt_shape(receipt: &RegistryReprocessReceipt, epoch: u64) -> Result<()> {
    ensure!(
        receipt.version == RECEIPT_VERSION,
        "unsupported registry reprocess receipt version"
    );
    ensure!(
        receipt.algorithm == RECEIPT_ALGORITHM,
        "unsupported registry reprocess algorithm"
    );
    ensure!(
        receipt.epoch == epoch,
        "registry reprocess receipt epoch mismatch"
    );
    ensure!(
        receipt.source_registry_keys > 0 && receipt.source_registry_keys <= u64::from(u32::MAX)
    );
    ensure!(
        receipt.target_registry_keys > 0
            && receipt.target_registry_keys <= receipt.source_registry_keys + 1
    );
    ensure!(
        receipt.source_semantics == receipt.target_semantics,
        "receipt does not declare semantic parity"
    );
    ensure!(receipt.threads > 0 && receipt.sort_memory_mib > 0);
    validate_hex_sha256(&receipt.source_anchor_sha256, "source anchor digest")?;
    validate_hex_sha256(
        &receipt.source_generation_sha256,
        "source generation digest",
    )?;
    validate_hex_sha256(
        &receipt.target_generation_sha256,
        "target generation digest",
    )?;
    for (name, binding) in receipt.source_files.iter().chain(&receipt.target_files) {
        ensure!(!name.is_empty() && !name.contains('/') && !name.contains('\\'));
        validate_hex_sha256(&binding.sha256, "artifact digest")?;
    }
    Ok(())
}

fn write_receipt(target: &Path, receipt: &RegistryReprocessReceipt) -> Result<()> {
    let path = target.join(REGISTRY_REPROCESS_RECEIPT_FILE);
    ensure!(
        !path.exists(),
        "receipt already exists in staging generation"
    );
    let bytes = serde_json::to_vec_pretty(receipt)?;
    ensure!(
        bytes.len() as u64 <= RECEIPT_MAX_BYTES,
        "registry reprocess receipt exceeds size limit"
    );
    let mut file = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?,
    );
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.flush()?;
    file.get_ref().sync_all()?;
    Ok(())
}

fn read_receipt(target: &Path) -> Result<RegistryReprocessReceipt> {
    let path = target.join(REGISTRY_REPROCESS_RECEIPT_FILE);
    let (mut file, metadata) = open_regular_read(&path)?;
    ensure!(
        metadata.file_type().is_file() && metadata.len() > 0 && metadata.len() <= RECEIPT_MAX_BYTES
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(RECEIPT_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= RECEIPT_MAX_BYTES,
        "receipt grew while reading"
    );
    ensure_open_file_unchanged(&path, &file, &metadata)?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn sync_generation(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read staging directory {}", directory.display()))?
    {
        let entry = entry?;
        let metadata = entry.metadata()?;
        ensure!(
            metadata.is_file(),
            "unexpected non-file in completed staging generation: {}",
            entry.path().display()
        );
        File::open(entry.path())
            .with_context(|| format!("open {} for sync", entry.path().display()))?
            .sync_all()
            .with_context(|| format!("sync {}", entry.path().display()))?;
    }
    sync_directory(directory)
}

fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)
        .with_context(|| format!("open directory {} for sync", directory.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", directory.display()))
}

#[cfg(target_os = "linux")]
fn publish_directory_no_replace(source: &Path, target: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source_c =
        CString::new(source.as_os_str().as_bytes()).context("staging path contains NUL")?;
    let target_c =
        CString::new(target.as_os_str().as_bytes()).context("target path contains NUL")?;
    // SAFETY: both path strings remain live and NUL-terminated for the syscall.  Staging and
    // target have the same parent, so success is an atomic same-filesystem directory rename.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD as libc::c_long,
            source_c.as_ptr(),
            libc::AT_FDCWD as libc::c_long,
            target_c.as_ptr(),
            libc::RENAME_NOREPLACE as libc::c_long,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).with_context(|| {
            format!(
                "atomically publish {} as {} without replacement",
                source.display(),
                target.display()
            )
        })
    }
}

#[cfg(target_os = "macos")]
fn publish_directory_no_replace(source: &Path, target: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source_c =
        CString::new(source.as_os_str().as_bytes()).context("staging path contains NUL")?;
    let target_c =
        CString::new(target.as_os_str().as_bytes()).context("target path contains NUL")?;
    // SAFETY: both path strings remain live and NUL-terminated for the call. RENAME_EXCL gives
    // atomic no-replace semantics, including when an empty target directory appears concurrently.
    let result =
        unsafe { libc::renamex_np(source_c.as_ptr(), target_c.as_ptr(), libc::RENAME_EXCL) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).with_context(|| {
            format!(
                "atomically publish {} as {} without replacement",
                source.display(),
                target.display()
            )
        })
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn publish_directory_no_replace(source: &Path, target: &Path) -> Result<()> {
    let _ = (source, target);
    bail!("atomic no-replace directory publication is unsupported on this operating system")
}

#[cfg(target_os = "linux")]
fn clone_or_copy_file(source: &Path, target: &Path) -> Result<Option<FileBinding>> {
    use std::os::fd::AsRawFd;
    let mut source_file = File::open(source)?;
    let mut target_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)?;
    // SAFETY: both descriptors refer to open regular files and FICLONE does not outlive them.
    if unsafe {
        libc::ioctl(
            target_file.as_raw_fd(),
            libc::FICLONE,
            source_file.as_raw_fd(),
        )
    } == 0
    {
        return Ok(None);
    }
    target_file.set_len(0)?;
    source_file.seek(SeekFrom::Start(0))?;
    let metadata = source_file.metadata()?;
    ensure!(metadata.file_type().is_file());
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    loop {
        let read = source_file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        target_file.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("sidecar copy byte count overflow")?;
    }
    ensure!(
        bytes == metadata.len(),
        "source sidecar changed while copying"
    );
    Ok(Some(FileBinding {
        bytes,
        sha256: hex_digest(hasher.finalize()),
    }))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn clone_or_copy_file(source: &Path, target: &Path) -> Result<Option<FileBinding>> {
    copy_file_with_hash(source, target).map(Some)
}

fn copy_file_with_hash(source: &Path, target: &Path) -> Result<FileBinding> {
    let mut source_file = BufReader::with_capacity(
        IO_BUFFER_BYTES,
        File::open(source).with_context(|| format!("open {}", source.display()))?,
    );
    let metadata = source_file.get_ref().metadata()?;
    ensure!(metadata.file_type().is_file());
    let mut target_file = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)
            .with_context(|| format!("create {}", target.display()))?,
    );
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    loop {
        let read = source_file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        target_file.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("sidecar copy byte count overflow")?;
    }
    target_file.flush()?;
    ensure!(
        bytes == metadata.len(),
        "source sidecar changed while copying"
    );
    Ok(FileBinding {
        bytes,
        sha256: hex_digest(hasher.finalize()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use of_car_reader::stored_transaction::StoredTransactionError;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let id = NEXT_TEST_DIR.fetch_add(1, AtomicOrdering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "blockzilla-registry-reprocess-{}-{id}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn empty_current_meta(err: Option<CompactTransactionError>) -> CompactMetaV1 {
        CompactMetaV1 {
            err,
            fee: 5_000,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    fn empty_legacy_meta(err: Option<Vec<u8>>) -> LegacyCompactMetaV1 {
        LegacyCompactMetaV1 {
            err,
            fee: 5_000,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    #[test]
    fn metadata_decoder_accepts_current_account_in_use() {
        let bytes = wincode::config::serialize(
            &empty_current_meta(Some(CompactTransactionError::AccountInUse)),
            wincode_leb128_config(),
        )
        .unwrap();
        let decoded = decode_compact_metadata(&bytes).unwrap();
        assert!(matches!(
            decoded.err,
            Some(CompactTransactionError::AccountInUse)
        ));
    }

    #[test]
    fn metadata_decoder_accepts_legacy_stored_error() {
        let stored = wincode::serialize(&StoredTransactionError::AccountInUse).unwrap();
        let bytes =
            wincode::config::serialize(&empty_legacy_meta(Some(stored)), wincode_leb128_config())
                .unwrap();
        let decoded = decode_compact_metadata(&bytes).unwrap();
        assert!(matches!(
            decoded.err,
            Some(CompactTransactionError::AccountInUse)
        ));
    }

    #[test]
    fn canonical_counts_exclude_block_rewards_but_include_metadata_rewards() {
        let reward = |byte| CompactReward {
            pubkey: CompactPubkey::raw([byte; 32]),
            lamports: 1,
            post_balance: 1,
            reward_type: 0,
            commission: None,
        };
        let mut block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: crate::SLOTS_PER_EPOCH,
                parent_slot: crate::SLOTS_PER_EPOCH - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: Some(ArchiveV2HotRewards {
                    num_partitions: None,
                    decoded: vec![reward(7)],
                }),
            },
            tx_count: 0,
            tx_rows: Vec::new(),
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
        };
        let mut classes = Vec::new();
        rewrite_block_pubkeys(&mut block, |_key, class| {
            classes.push(class);
            Ok(())
        })
        .unwrap();

        let mut metadata = empty_current_meta(None);
        metadata.rewards.push(reward(9));
        visit_metadata_pubkeys(&mut metadata, &mut |_key, class| {
            classes.push(class);
            Ok(())
        })
        .unwrap();

        assert_eq!(
            classes,
            vec![ReferenceClass::Excluded, ReferenceClass::Eligible]
        );
    }

    #[test]
    fn hot_batch_rejects_single_row_over_memory_budget() {
        let rows = [blockzilla_format::ArchiveV2HotBlockIndexRow {
            block_id: 0,
            slot: crate::SLOTS_PER_EPOCH,
            compressed_offset: 0,
            compressed_len: (HOT_BATCH_MEMORY_BUDGET_BYTES / 2 + 1) as u32,
            uncompressed_len: (HOT_BATCH_MEMORY_BUDGET_BYTES / 2) as u32,
            tx_count: 0,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 0,
        }];
        assert!(hot_batch_end(&rows, 0, 1, false).is_err());
    }

    #[test]
    fn previous_tail_decodes_full_legacy_length_even_when_divisible_by_40() {
        let bytes = vec![0xa5; crate::archive_v2::ROLLING_BLOCKHASH_CAPACITY * 32];
        assert!(bytes.len().is_multiple_of(40));
        let tail = decode_previous_blockhash_tail_bytes(&bytes, 1).unwrap();
        assert_eq!(tail.len(), crate::archive_v2::ROLLING_BLOCKHASH_CAPACITY);
        assert!(
            tail.iter()
                .all(|row| row.hash == [0xa5; 32] && row.slot == 0)
        );
    }

    #[test]
    fn previous_tail_rejects_a_genuinely_ambiguous_160_byte_payload() {
        let mut bytes = Vec::new();
        let start = crate::SLOTS_PER_EPOCH;
        for index in 0..4u64 {
            bytes.extend_from_slice(&[(index + 1) as u8; 32]);
            bytes.extend_from_slice(&(start + index).to_le_bytes());
        }
        assert_eq!(bytes.len(), 160);
        let error = decode_previous_blockhash_tail_bytes(&bytes, 2).unwrap_err();
        assert!(error.to_string().contains("ambiguous"));
    }

    #[test]
    fn previous_tail_accepts_unambiguous_current_capacity() {
        let mut bytes = Vec::new();
        for index in 0..crate::archive_v2::ROLLING_BLOCKHASH_CAPACITY {
            bytes.extend_from_slice(&[(index % 251) as u8; 32]);
            bytes.extend_from_slice(&(index as u64).to_le_bytes());
        }
        assert_eq!(
            bytes.len(),
            crate::archive_v2::ROLLING_BLOCKHASH_CAPACITY * 40
        );
        let tail = decode_previous_blockhash_tail_bytes(&bytes, 1).unwrap();
        assert_eq!(tail.len(), crate::archive_v2::ROLLING_BLOCKHASH_CAPACITY);
        assert_eq!(tail.first().unwrap().slot, 0);
        assert_eq!(tail.last().unwrap().slot, 299);
    }

    #[test]
    fn usage_sort_is_thread_deterministic_and_prefixes_missing_compute_budget() {
        let root = TestDir::new();
        let source_path = root.0.join("source-registry.bin");
        let key_a = [1u8; 32];
        let key_b = [2u8; 32];
        let builtin = compute_budget_key();
        let source_bytes = [key_b, builtin, key_a].concat();
        fs::write(&source_path, source_bytes).unwrap();
        let source = MappedRegistry::open(&source_path).unwrap();
        let counts = [2u32, 0, 2];
        let mut outputs = Vec::new();
        for threads in [1usize, 2] {
            let target = root.0.join(format!("target-{threads}"));
            fs::create_dir(&target).unwrap();
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            let (mapping, keys) =
                build_usage_sorted_registry(&source, &counts, &target, 1, &pool).unwrap();
            assert_eq!(mapping, vec![3, 1, 2]);
            assert_eq!(keys, 3);
            let registry = fs::read(target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)).unwrap();
            let target_counts =
                read_registry_counts(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE), 3)
                    .unwrap();
            assert_eq!(
                registry,
                [builtin, key_a, key_b].concat(),
                "ComputeBudget must be the synthetic ID-1 prefix"
            );
            assert_eq!(target_counts, vec![0, 2, 2]);
            outputs.push((registry, target_counts));
        }
        assert_eq!(outputs[0], outputs[1]);
    }

    #[test]
    fn deep_counts_reject_internally_sorted_tampering_and_wrong_sum() {
        let root = TestDir::new();
        let source_path = root.0.join("source-registry.bin");
        let target = root.0.join("target");
        fs::create_dir(&target).unwrap();
        let key_a = [1u8; 32];
        let key_b = [2u8; 32];
        let builtin = compute_budget_key();
        fs::write(&source_path, [key_b, builtin, key_a].concat()).unwrap();
        fs::write(
            target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            [builtin, key_a, key_b].concat(),
        )
        .unwrap();
        fs::write(
            target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
            [0u8, 4, 3],
        )
        .unwrap();
        build_registry_index(&target).unwrap();
        assert!(validate_canonical_registry(&target, 3).is_ok());

        let source = MappedRegistry::open(&source_path).unwrap();
        let source_index = KeyIndex::build(source.keys().to_vec());
        let eligible = [4u32, 0, 5];
        assert!(
            validate_target_registry_against_recomputed(
                &source,
                &source_index,
                &eligible,
                &target,
                3,
                9,
            )
            .is_err(),
            "independently sorted but false target counts must be rejected"
        );

        fs::write(
            target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
            [0u8, 5, 4],
        )
        .unwrap();
        assert!(
            validate_target_registry_against_recomputed(
                &source,
                &source_index,
                &eligible,
                &target,
                3,
                8,
            )
            .is_err(),
            "receipt eligible-reference sum must be independently checked"
        );
        validate_target_registry_against_recomputed(
            &source,
            &source_index,
            &eligible,
            &target,
            3,
            9,
        )
        .unwrap();
    }

    #[test]
    fn target_probe_contract_rejects_first_seen_manifest() {
        let binding = FileBinding {
            bytes: 1,
            sha256: "00".repeat(32),
        };
        let mut files = BTreeMap::new();
        for name in [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_META_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
            ARCHIVE_V2_SIGNATURES_FILE,
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
            ARCHIVE_V2_POH_FILE,
            ARCHIVE_V2_SHREDDING_FILE,
            ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
        ] {
            files.insert(name.to_owned(), binding.clone());
        }
        assert!(validate_probe_core_files(&files, false).is_err());
        assert!(validate_probe_core_files(&files, true).is_ok());
    }

    #[test]
    fn existing_target_is_immutable_and_reported_before_source_processing() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&target).unwrap();
        fs::write(target.join("sentinel"), b"keep").unwrap();
        let error = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: source,
            target_dir: target.clone(),
            epoch: 1,
            threads: 1,
            sort_memory_mib: 1,
            level: 1,
        })
        .unwrap_err();
        assert!(error.to_string().contains("target already exists"));
        assert_eq!(fs::read(target.join("sentinel")).unwrap(), b"keep");
    }
}
