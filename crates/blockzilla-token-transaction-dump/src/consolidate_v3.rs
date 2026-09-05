//! Bounded schema-3 consolidation for exact SPYX raw epoch shards.
//!
//! This module deliberately builds a dump-local registry from only the public
//! keys referenced by selected records and account artifacts. It never merges
//! whole Archive V2 registries. Raw shards are validated and indexed once,
//! keys are externally sorted, and a second canonical-order pass rewrites only
//! typed `CompactPubkey` references while it copies signature occurrences.

mod dex_coverage;
mod spyx_portfolio_history;
mod spyx_replay;
mod token_report;

use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    fs::{self, File, OpenOptions as FsOpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_format::{
    ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES, ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX,
    ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_LOGS,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA,
    ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES, ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ArchiveV2WireFallbackReason, ArchiveV2WireIdentityVisitor, ArchiveV2WireMetadataErrorSchema,
    ArchiveV2WireReferenceClass, ArchiveV2WireRewriteLimits, ArchiveV2WireRewriteVisitor,
    CompactPubkey, WINCODE_LEB128_MAX_FRAME_BYTES, WincodeLeb128Config,
    bounded_wincode_leb128_config, encode_with_scratch,
    rewrite_archive_v2_metadata_wire_preserving_error_schema,
    rewrite_archive_v2_metadata_wire_preserving_selected_error_schema,
    validate_archive_v2_metadata_error_prefix_for_selected_schema, write_u32_varint,
};
use blockzilla_read_sdk::{
    ArchiveReader, ArchiveV2LoadedAddressSide, ArchiveV2MessageProjector,
    ArchiveV2MetadataProjectionLimits, ArchiveV2MetadataWireProfile, ArchiveV2WireProfile,
    BorrowedArchiveV2InnerTokenInstruction, BorrowedArchiveV2LogEvent,
    BorrowedArchiveV2LogEventKind, BorrowedArchiveV2LogTables, BorrowedArchiveV2ProgramLog,
    HashVerification, LogPayloadValidation, OpenOptions, PinnedLocalRangeSource,
    ProjectedArchiveV2MessageAccountSummary, ProjectedArchiveV2TokenMetadataSummary,
    manifest::{
        GenerationManifest, REGISTRY_FILE, REGISTRY_INDEX_FILE, SIGNATURES_FILE,
        TrustedGenerationIdentity, compute_generation_digest,
    },
    visit_archive_v2_compact_logs_exact_with_selected_error_schema,
    visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema,
};
use sha2::{Digest, Sha256};
use wincode::SchemaWrite;

use crate::consolidated_reader::{
    BorrowedDumpRecord, BorrowedTransactionRecord, decode_borrowed_frame,
};
use crate::extract::inspect_trusted_local_metadata_admission;
use crate::format::{
    ACCOUNT_ID_LOG_FILE, ACCOUNTS_FILE, AccountIdRole, DUMP_MANIFEST_FILE, DUMP_SCHEMA_VERSION,
    DiscoveredAccountList, DumpArtifactKind, DumpManifest, DumpSourceBinding, DumpStreamKind,
    DumpWireProfile, EPOCH_SHARDS_DIR, EpochAccountIdLog, PUBKEY_REGISTRY_FILE,
    PUBKEY_REGISTRY_ID_BASE, SIGNATURES_FILE as DUMP_SIGNATURES_FILE, TRANSACTIONS_FILE,
    TokenTransactionBlockContext, TokenTransactionDumpFooter, TokenTransactionDumpHeader,
    TokenTransactionDumpRecord, TokenTransactionRecord,
};

const KEY_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 64;
const IO_BUFFER_BYTES: usize = 8 << 20;
const KEY_SORT_MEMORY_BYTES: usize = 256 << 20;
const LOCATOR_SORT_MEMORY_BYTES: usize = 64 << 20;
const MERGE_FAN_IN: usize = 64;
const SOURCE_ID_MAP_ROW_BYTES: usize = 4 + KEY_BYTES;
const SOURCE_ID_MAP_READ_ROWS: usize = IO_BUFFER_BYTES / SOURCE_ID_MAP_ROW_BYTES;
const SOURCE_ID_MAP_READ_BYTES: usize = SOURCE_ID_MAP_READ_ROWS * SOURCE_ID_MAP_ROW_BYTES;
const REGISTRY_BULK_READ_MAX_GAP_BYTES: u64 = 256 << 10;
const REGISTRY_BULK_READ_MAX_BYTES: u64 = 8 << 20;
const REGISTRY_BULK_ID_BATCH_ROWS: usize = 64 << 10;
const GLOBAL_PREFIX_COUNT: usize = 1 << 16;
const SIGNATURE_BATCH_RANGES: usize = 16 << 10;
const SIGNATURE_BATCH_BYTES: usize = 64 << 20;
const SIGNATURE_READ_WORKERS: usize = 8;
const PASS_TWO_READ_BATCH_LOCATORS: usize = 4_096;
const PASS_TWO_READ_BATCH_PAYLOAD_BYTES: usize = 64 << 20;
const PASS_TWO_READ_MAX_GAP_BYTES: u64 = 64 << 10;
const PASS_TWO_READ_MAX_RANGE_BYTES: usize = 8 << 20;
const PASS_TWO_READ_MAX_EXTRA_GAP_BYTES: usize = 64 << 20;
const TRANSACTION_LINEAR_ID_DEDUP_LIMIT: usize = 64;
const PROGRAM_INVENTORY_PROGRESS_TRANSACTIONS: u64 = 250_000;
const PROGRAM_ACCUMULATOR_MISSING: u32 = u32::MAX;
const ARCHIVE_V2_TX_KNOWN_FLAGS: u32 = ARCHIVE_V2_TX_FLAG_HAS_METADATA
    | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
    | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
    | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK
    | ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA
    | ARCHIVE_V2_TX_FLAG_HAS_LOGS
    | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
    | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES
    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
    | ARCHIVE_V2_TX_FLAG_HAS_ERROR
    | ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX;
const MAX_IDENTIFIED_PROGRAM_SET_BYTES: u64 = 1 << 20;
const COMPACT_LOG_EVENT_TAG_COUNT: usize = 46;
const MAX_PROGRAM_LOG_HOLDOUT_DETAILS: usize = 256;
const MAX_PROGRAM_LOG_PATTERN_KEYS_PER_PROGRAM: usize = 256;
const MAX_PROGRAM_LOG_CALL_EDGES_PER_PROGRAM: usize = 256;
const MAX_PROGRAM_LOG_PATTERN_BYTES: usize = 512;
const MAX_PROGRAM_LOG_EXAMPLE_CHARS: usize = 360;
// Pass 1 retains one raw-stream and one source-ID-map descriptor per epoch.
// Keep headroom below the common 1,024-descriptor process limit.
const MAX_PINNED_EPOCHS: u64 = 384;
const MAX_SOURCE_REGISTRY_ENTRIES: u32 = 128_000_000;
const MAX_SLOTS_PER_EPOCH: u64 = 1_000_000;
const MAX_ROOT_MANIFEST_BYTES: u64 = 16 << 20;
const MAX_SHARD_MANIFEST_BYTES: u64 = 16 << 20;
const WORK_DIRECTORY: &str = ".consolidate-v3-work";
const RESUME_CHECKPOINT_FILE: &str = "resume-checkpoint.json";
const RESUME_CHECKPOINT_SCHEMA: u32 = 1;
const PRIOR_TRUSTED_FILE_SIZE_BINDING_DOMAIN: &[u8] =
    b"blockzilla/archive-v2-trusted-local-file-size-binding\0";

#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetBinding {
    mint: [u8; 32],
    mint_slot: u64,
    mint_signature: [u8; 64],
}

#[derive(Debug, Clone)]
struct InputRoot {
    manifest: DumpManifest,
    target: TargetBinding,
    frozen_bytes: Vec<u8>,
    frozen: DiscoveredAccountList,
}

#[derive(Clone, Copy)]
struct RawTransactionRecordView<'a> {
    source_epoch: u64,
    source_generation_digest: [u8; 32],
    source_wire_profile: DumpWireProfile,
    source_block_id: u32,
    block: &'a TokenTransactionBlockContext,
    tx_index: u32,
    flags: u32,
    source_first_signature_ordinal: u64,
    signature_count: u8,
    dump_signature_ordinal: Option<u64>,
    message_bytes: &'a [u8],
    metadata_bytes: &'a [u8],
}

impl<'a> From<&'a TokenTransactionRecord> for RawTransactionRecordView<'a> {
    fn from(record: &'a TokenTransactionRecord) -> Self {
        Self {
            source_epoch: record.source_epoch,
            source_generation_digest: record.source_generation_digest,
            source_wire_profile: record.source_wire_profile,
            source_block_id: record.source_block_id,
            block: &record.block,
            tx_index: record.tx_index,
            flags: record.flags,
            source_first_signature_ordinal: record.source_first_signature_ordinal,
            signature_count: record.signature_count,
            dump_signature_ordinal: record.dump_signature_ordinal,
            message_bytes: &record.message_bytes,
            metadata_bytes: &record.metadata_bytes,
        }
    }
}

impl<'record, 'payload: 'record> From<&'record BorrowedTransactionRecord<'payload>>
    for RawTransactionRecordView<'record>
{
    fn from(record: &'record BorrowedTransactionRecord<'payload>) -> Self {
        Self {
            source_epoch: record.source_epoch,
            source_generation_digest: record.source_generation_digest,
            source_wire_profile: record.source_wire_profile,
            source_block_id: record.source_block_id,
            block: &record.block,
            tx_index: record.tx_index,
            flags: record.flags,
            source_first_signature_ordinal: record.source_first_signature_ordinal,
            signature_count: record.signature_count,
            dump_signature_ordinal: record.dump_signature_ordinal,
            message_bytes: record.message_bytes,
            metadata_bytes: record.metadata_bytes,
        }
    }
}

#[derive(Debug)]
struct OpenEpochSource {
    source: PinnedLocalRangeSource,
    registry_entries: u32,
    slots_per_epoch: u64,
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
struct AggregateFooter {
    epochs: u64,
    blocks_scanned: u64,
    transactions_scanned: u64,
    transactions_written: u64,
    owned_block_fallbacks: u64,
}

impl AggregateFooter {
    fn add(&mut self, footer: TokenTransactionDumpFooter) -> Result<()> {
        self.epochs = self
            .epochs
            .checked_add(footer.epochs)
            .context("epoch count overflow")?;
        self.blocks_scanned = self
            .blocks_scanned
            .checked_add(footer.blocks_scanned)
            .context("block count overflow")?;
        self.transactions_scanned = self
            .transactions_scanned
            .checked_add(footer.transactions_scanned)
            .context("scanned transaction count overflow")?;
        self.transactions_written = self
            .transactions_written
            .checked_add(footer.transactions_written)
            .context("written transaction count overflow")?;
        self.owned_block_fallbacks = self
            .owned_block_fallbacks
            .checked_add(footer.owned_block_fallbacks)
            .context("owned fallback count overflow")?;
        Ok(())
    }
}

#[derive(Debug)]
struct EpochPlan {
    epoch: u64,
    stream_file: File,
    stream_stamp: FileStamp,
    locator_path: PathBuf,
    source_id_map_path: PathBuf,
    source_id_map_file: File,
    source_id_map: SourceIdMapBinding,
    source_generation_digest: [u8; 32],
    source_wire_profile: DumpWireProfile,
    registry_entries: u32,
    registry_stamp: FileStamp,
    signatures_stamp: FileStamp,
    slots_per_epoch: u64,
    transaction_count: u64,
    signature_count: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct EpochPlanCheckpoint {
    epoch: u64,
    stream_stamp: FileStamp,
    source_id_map: SourceIdMapBinding,
    source_generation_digest: [u8; 32],
    source_wire_profile: DumpWireProfile,
    registry_entries: u32,
    registry_stamp: FileStamp,
    signatures_stamp: FileStamp,
    slots_per_epoch: u64,
    transaction_count: u64,
    signature_count: u64,
}

impl From<&EpochPlan> for EpochPlanCheckpoint {
    fn from(plan: &EpochPlan) -> Self {
        Self {
            epoch: plan.epoch,
            stream_stamp: plan.stream_stamp.clone(),
            source_id_map: plan.source_id_map.clone(),
            source_generation_digest: plan.source_generation_digest,
            source_wire_profile: plan.source_wire_profile,
            registry_entries: plan.registry_entries,
            registry_stamp: plan.registry_stamp.clone(),
            signatures_stamp: plan.signatures_stamp.clone(),
            slots_per_epoch: plan.slots_per_epoch,
            transaction_count: plan.transaction_count,
            signature_count: plan.signature_count,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ConsolidationResumeCheckpoint {
    schema_version: u32,
    input_manifest_sha256: [u8; 32],
    first_epoch: u64,
    last_epoch: u64,
    allow_metadata_generation_drift: bool,
    aggregate: AggregateFooter,
    plans: Vec<EpochPlanCheckpoint>,
    registry_rows: u64,
    registry_sha256: [u8; 32],
    next_pass_two_plan: usize,
    transactions: u64,
    signatures: u64,
    transaction_output_bytes: u64,
    signature_output_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SourceIdMapBinding {
    rows: u64,
    bytes: u64,
    stamp: FileStamp,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SourceIdResolutionStats {
    rows: u64,
    read_calls: u64,
    read_bytes: u64,
}

impl SourceIdResolutionStats {
    fn add(&mut self, other: Self) -> Result<()> {
        self.rows = self
            .rows
            .checked_add(other.rows)
            .context("source ID resolution row count overflow")?;
        self.read_calls = self
            .read_calls
            .checked_add(other.read_calls)
            .context("source ID registry read-call count overflow")?;
        self.read_bytes = self
            .read_bytes
            .checked_add(other.read_bytes)
            .context("source ID registry read-byte count overflow")?;
        Ok(())
    }
}

#[derive(Debug)]
struct RegistryBuild {
    rows: u64,
    sha256: [u8; 32],
    keys: Arc<Vec<[u8; KEY_BYTES]>>,
    prefix_offsets: Arc<Vec<u64>>,
    file: File,
    stamp: FileStamp,
}

#[derive(Debug)]
struct ArtifactBinding {
    bytes: u64,
    sha256: [u8; 32],
    file: File,
    stamp: FileStamp,
}

impl ArtifactBinding {
    fn verify(&self, path: &Path, label: &str) -> Result<()> {
        self.stamp.verify(&self.file, label)?;
        verify_path_binding(path, &self.stamp, label)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct FileStamp {
    bytes: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    ctime_seconds: i64,
    #[cfg(unix)]
    ctime_nanoseconds: i64,
}

impl FileStamp {
    fn read(file: &File) -> Result<Self> {
        let metadata = file.metadata()?;
        ensure!(
            metadata.file_type().is_file(),
            "opened source is not a regular file"
        );
        Ok(Self::from_metadata(&metadata))
    }

    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            bytes: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            device: {
                use std::os::unix::fs::MetadataExt as _;
                metadata.dev()
            },
            #[cfg(unix)]
            inode: {
                use std::os::unix::fs::MetadataExt as _;
                metadata.ino()
            },
            #[cfg(unix)]
            ctime_seconds: {
                use std::os::unix::fs::MetadataExt as _;
                metadata.ctime()
            },
            #[cfg(unix)]
            ctime_nanoseconds: {
                use std::os::unix::fs::MetadataExt as _;
                metadata.ctime_nsec()
            },
        }
    }

    fn verify(&self, file: &File, label: &str) -> Result<()> {
        ensure!(
            &Self::read(file)? == self,
            "{label} changed while it was in use"
        );
        Ok(())
    }

    /// Compare byte-bearing identity across separately pinned passes. A hard
    /// link or permission update changes ctime without changing file bytes.
    /// Each pass still checks the complete stamp while its descriptor is open.
    fn same_content_identity(&self, other: &Self) -> bool {
        self.bytes == other.bytes && self.modified == other.modified && {
            #[cfg(unix)]
            {
                self.device == other.device && self.inode == other.inode
            }
            #[cfg(not(unix))]
            {
                true
            }
        }
    }
}

fn verify_path_binding(path: &Path, expected: &FileStamp, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {label} path {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "{label} path is not a regular file"
    );
    ensure!(
        FileStamp::from_metadata(&metadata) == *expected,
        "{label} path binding changed while it was in use"
    );
    Ok(())
}

#[inline]
fn hex_digest(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(64);
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn parse_hex_digest(value: &str, label: &str) -> Result<[u8; 32]> {
    ensure!(
        value.len() == 64,
        "{label} is not a 64-character SHA-256 value"
    );
    let mut output = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)
            .with_context(|| format!("{label} contains non-hexadecimal bytes"))?;
    }
    Ok(output)
}

fn prior_trusted_sizes_digest(
    manifest: &GenerationManifest,
    wire_profile: ArchiveV2WireProfile,
) -> Result<[u8; 32]> {
    let wire_profile = match wire_profile {
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::POST_UNKNOWN_NAME
        }
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PRE_UNKNOWN_NAME
        }
    };
    let mut prior = manifest.clone();
    for file in &mut prior.files {
        let mut hasher = Sha256::new();
        hasher.update(PRIOR_TRUSTED_FILE_SIZE_BINDING_DOMAIN);
        hasher.update(wire_profile.as_bytes());
        hasher.update([0]);
        hasher.update((file.name.len() as u64).to_le_bytes());
        hasher.update(file.name.as_bytes());
        hasher.update(file.size.to_le_bytes());
        file.sha256 = hex_digest(hasher.finalize().into());
    }
    parse_hex_digest(
        &compute_generation_digest(&prior)?,
        "prior trusted-local generation digest",
    )
}

fn zero_hash_trusted_sizes_digest(manifest: &GenerationManifest) -> Result<[u8; 32]> {
    let mut legacy = manifest.clone();
    let placeholder = "0".repeat(64);
    for file in &mut legacy.files {
        file.sha256.clone_from(&placeholder);
    }
    parse_hex_digest(
        &compute_generation_digest(&legacy)?,
        "legacy trusted-local generation digest",
    )
}

fn parse_pubkey(value: &str, label: &str) -> Result<[u8; 32]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode {label}"))?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{label} is not 32 bytes"))
}

fn parse_signature(value: &str) -> Result<[u8; 64]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .context("decode mint signature")?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("mint signature is not 64 bytes"))
}

fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn sync_file(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync {}", path.display()))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn create_new_file(path: &Path) -> Result<File> {
    let mut options = FsOpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options
        .open(path)
        .with_context(|| format!("create {}", path.display()))
}

fn create_new_read_write_file(path: &Path) -> Result<File> {
    let mut options = FsOpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options
        .open(path)
        .with_context(|| format!("create {}", path.display()))
}

fn publish_partial(partial: &Path, final_path: &Path) -> Result<()> {
    ensure!(
        !final_path.exists(),
        "refusing to replace existing output {}",
        final_path.display()
    );
    fs::rename(partial, final_path)
        .with_context(|| format!("publish {} as {}", partial.display(), final_path.display()))
}

fn read_bounded_regular(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let path_metadata =
        fs::symlink_metadata(path).with_context(|| format!("inspect {}", path.display()))?;
    ensure!(
        path_metadata.file_type().is_file(),
        "{} is not a regular file",
        path.display()
    );
    let path_stamp = FileStamp::from_metadata(&path_metadata);
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let stamp = FileStamp::read(&file)?;
    ensure!(
        stamp == path_stamp,
        "{} changed while it was opened",
        path.display()
    );
    ensure!(
        stamp.bytes <= maximum,
        "{} exceeds its size limit",
        path.display()
    );
    let capacity = usize::try_from(stamp.bytes).context("bounded file length exceeds usize")?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(capacity)?;
    let mut bounded = file.try_clone()?.take(
        maximum
            .checked_add(1)
            .context("bounded read limit overflow")?,
    );
    bounded
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    ensure!(
        u64::try_from(bytes.len())? <= maximum,
        "{} grew beyond its size limit while it was read",
        path.display()
    );
    stamp.verify(&file, &format!("bounded file {}", path.display()))?;
    verify_path_binding(path, &stamp, &format!("bounded file {}", path.display()))?;
    Ok(bytes)
}

#[derive(Debug)]
struct RawKeySorter {
    root: PathBuf,
    capacity: usize,
    keys: Vec<[u8; KEY_BYTES]>,
    runs: Vec<PathBuf>,
    next_run: u64,
}

impl RawKeySorter {
    fn new(root: &Path, memory_bytes: usize) -> Result<Self> {
        let capacity = memory_bytes / KEY_BYTES;
        ensure!(capacity != 0, "key-sort memory cannot hold one public key");
        fs::create_dir(root).with_context(|| format!("create {}", root.display()))?;
        let mut keys = Vec::new();
        keys.try_reserve_exact(capacity)
            .context("reserve external public-key sort buffer")?;
        Ok(Self {
            root: root.to_path_buf(),
            capacity,
            keys,
            runs: Vec::new(),
            next_run: 0,
        })
    }

    fn push(&mut self, key: [u8; KEY_BYTES]) -> Result<()> {
        self.keys.push(key);
        if self.keys.len() == self.capacity {
            self.flush_run()?;
        }
        Ok(())
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.keys.is_empty() {
            return Ok(());
        }
        self.keys.sort_unstable();
        self.keys.dedup();
        let path = self.next_path("keys");
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_file(&path)?);
        for key in &self.keys {
            writer.write_all(key)?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        self.runs.push(path);
        self.keys.clear();
        Ok(())
    }

    fn next_path(&mut self, phase: &str) -> PathBuf {
        let path = self.root.join(format!("{phase}-{:012}.bin", self.next_run));
        self.next_run += 1;
        path
    }

    fn finish(mut self, output: &Path) -> Result<RegistryBuild> {
        self.flush_run()?;
        // The external-sort buffer is no longer needed. Release its 256 MiB
        // reservation before the resident final registry is allocated.
        self.keys = Vec::new();
        ensure!(
            !self.runs.is_empty(),
            "the dump-local public-key registry is empty"
        );
        while self.runs.len() > 1 {
            let old = std::mem::take(&mut self.runs);
            let mut next = Vec::with_capacity(old.len().div_ceil(MERGE_FAN_IN));
            for group in old.chunks(MERGE_FAN_IN) {
                if group.len() == 1 {
                    next.push(group[0].clone());
                    continue;
                }
                let path = self.next_path("merge");
                merge_raw_key_runs(group, &path)?;
                for input in group {
                    fs::remove_file(input)?;
                }
                next.push(path);
            }
            self.runs = next;
        }
        let final_run = self.runs.pop().expect("validated nonempty key runs");
        let build = copy_registry_and_build_prefix(&final_run, output)?;
        fs::remove_file(final_run)?;
        Ok(build)
    }
}

struct KeyRunReader {
    reader: BufReader<File>,
    current: Option<[u8; KEY_BYTES]>,
}

impl KeyRunReader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        ensure!(
            file.metadata()?.len().is_multiple_of(KEY_BYTES as u64),
            "key run {} has a partial record",
            path.display()
        );
        let mut this = Self {
            reader: BufReader::with_capacity(1 << 20, file),
            current: None,
        };
        this.advance()?;
        Ok(this)
    }

    fn advance(&mut self) -> Result<()> {
        let mut key = [0u8; KEY_BYTES];
        match self.reader.read_exact(&mut key) {
            Ok(()) => self.current = Some(key),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.current = None;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

fn merge_raw_key_runs(inputs: &[PathBuf], output: &Path) -> Result<()> {
    let mut readers = inputs
        .iter()
        .map(|path| KeyRunReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::<Reverse<([u8; KEY_BYTES], usize)>>::new();
    for (index, reader) in readers.iter().enumerate() {
        if let Some(key) = reader.current {
            heap.push(Reverse((key, index)));
        }
    }
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_file(output)?);
    let mut previous = None;
    while let Some(Reverse((key, index))) = heap.pop() {
        if previous != Some(key) {
            writer.write_all(&key)?;
            previous = Some(key);
        }
        readers[index].advance()?;
        if let Some(next) = readers[index].current {
            heap.push(Reverse((next, index)));
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn copy_registry_and_build_prefix(input: &Path, output: &Path) -> Result<RegistryBuild> {
    let input_file = File::open(input)?;
    let input_bytes = input_file.metadata()?.len();
    ensure!(
        input_bytes % KEY_BYTES as u64 == 0,
        "final key run has a partial public-key row"
    );
    let input_rows_u64 = input_bytes / KEY_BYTES as u64;
    ensure!(
        input_rows_u64 < u64::from(u32::MAX),
        "global public-key registry exceeds the one-based u32 contract"
    );
    let input_rows = usize::try_from(input_rows_u64)
        .context("final key run row count exceeds addressable memory")?;
    let mut keys = Vec::<[u8; KEY_BYTES]>::new();
    keys.try_reserve_exact(input_rows)
        .context("reserve resident global public-key registry")?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, input_file);
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_read_write_file(output)?);
    let mut hasher = Sha256::new();
    let mut offsets = vec![0u64; GLOBAL_PREFIX_COUNT + 1];
    let mut next_prefix = 0usize;
    let mut rows = 0u64;
    let mut previous = None;
    loop {
        let mut key = [0u8; KEY_BYTES];
        match reader.read_exact(&mut key) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
        ensure!(
            previous.is_none_or(|value| value < key),
            "final key run is not strictly sorted"
        );
        let prefix = usize::from(u16::from_be_bytes([key[0], key[1]]));
        while next_prefix <= prefix {
            offsets[next_prefix] = rows;
            next_prefix += 1;
        }
        writer.write_all(&key)?;
        hasher.update(key);
        keys.push(key);
        previous = Some(key);
        rows = rows
            .checked_add(1)
            .context("global public-key count overflow")?;
        ensure!(
            rows < u64::from(u32::MAX),
            "global public-key registry exceeds the one-based u32 contract"
        );
    }
    while next_prefix <= GLOBAL_PREFIX_COUNT {
        offsets[next_prefix] = rows;
        next_prefix += 1;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let mut file = writer
        .into_inner()
        .map_err(std::io::IntoInnerError::into_error)?;
    let stamp = FileStamp::read(&file)?;
    file.seek(SeekFrom::Start(0))?;
    ensure!(
        keys.len() == input_rows && u64::try_from(keys.len())? == rows,
        "resident global registry row count differs from its source"
    );
    Ok(RegistryBuild {
        rows,
        sha256: hasher.finalize().into(),
        keys: Arc::new(keys),
        prefix_offsets: Arc::new(offsets),
        file,
        stamp,
    })
}

fn reopen_registry_build(
    path: &Path,
    expected_rows: u64,
    expected_sha256: [u8; 32],
) -> Result<RegistryBuild> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let stamp = FileStamp::read(&file)?;
    ensure!(
        stamp.bytes
            == expected_rows
                .checked_mul(KEY_BYTES as u64)
                .context("registry size overflow")?,
        "resume registry size differs from its checkpoint"
    );
    let rows = usize::try_from(expected_rows).context("resume registry rows exceed usize")?;
    let mut keys = Vec::<[u8; KEY_BYTES]>::new();
    keys.try_reserve_exact(rows)
        .context("reserve resumed global public-key registry")?;
    let mut offsets = vec![0u64; GLOBAL_PREFIX_COUNT + 1];
    let mut next_prefix = 0usize;
    let mut hasher = Sha256::new();
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file.try_clone()?);
    let mut previous = None;
    for row in 0..expected_rows {
        let mut key = [0u8; KEY_BYTES];
        reader.read_exact(&mut key)?;
        ensure!(
            previous.is_none_or(|value| value < key),
            "resume registry is not strictly sorted"
        );
        let prefix = usize::from(u16::from_be_bytes([key[0], key[1]]));
        while next_prefix <= prefix {
            offsets[next_prefix] = row;
            next_prefix += 1;
        }
        hasher.update(key);
        keys.push(key);
        previous = Some(key);
    }
    ensure!(
        reader.read(&mut [0u8; 1])? == 0,
        "resume registry has trailing bytes"
    );
    while next_prefix <= GLOBAL_PREFIX_COUNT {
        offsets[next_prefix] = expected_rows;
        next_prefix += 1;
    }
    let sha256 = hasher.finalize().into();
    ensure!(
        sha256 == expected_sha256,
        "resume registry digest differs from its checkpoint"
    );
    stamp.verify(&file, "resumed global public-key registry")?;
    file.seek(SeekFrom::Start(0))?;
    Ok(RegistryBuild {
        rows: expected_rows,
        sha256,
        keys: Arc::new(keys),
        prefix_offsets: Arc::new(offsets),
        file,
        stamp,
    })
}

const LOCATOR_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransactionLocator {
    slot: u64,
    source_block_id: u32,
    tx_index: u32,
    payload_offset: u64,
    payload_len: u32,
}

impl TransactionLocator {
    fn key(self) -> (u64, u32, u32) {
        (self.slot, self.source_block_id, self.tx_index)
    }

    fn encode(self) -> [u8; LOCATOR_BYTES] {
        let mut bytes = [0u8; LOCATOR_BYTES];
        bytes[0..8].copy_from_slice(&self.slot.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.source_block_id.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.tx_index.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.payload_offset.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.payload_len.to_le_bytes());
        bytes
    }

    fn decode(bytes: [u8; LOCATOR_BYTES]) -> Result<Self> {
        ensure!(
            bytes[28..32].iter().all(|byte| *byte == 0),
            "locator has non-zero reserved bytes"
        );
        Ok(Self {
            slot: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            source_block_id: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            tx_index: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            payload_offset: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            payload_len: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
        })
    }
}

impl Ord for TransactionLocator {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key()
            .cmp(&other.key())
            .then_with(|| self.payload_offset.cmp(&other.payload_offset))
            .then_with(|| self.payload_len.cmp(&other.payload_len))
    }
}

impl PartialOrd for TransactionLocator {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct LocatorSorter {
    root: PathBuf,
    epoch: u64,
    capacity: usize,
    records: Vec<TransactionLocator>,
    runs: Vec<PathBuf>,
    next_run: u64,
}

impl LocatorSorter {
    fn new(root: &Path, epoch: u64, memory_bytes: usize) -> Result<Self> {
        let capacity = memory_bytes / std::mem::size_of::<TransactionLocator>();
        ensure!(capacity != 0, "locator-sort memory cannot hold one record");
        let mut records = Vec::new();
        records
            .try_reserve_exact(capacity)
            .context("reserve transaction locator sort buffer")?;
        Ok(Self {
            root: root.to_path_buf(),
            epoch,
            capacity,
            records,
            runs: Vec::new(),
            next_run: 0,
        })
    }

    fn push(&mut self, record: TransactionLocator) -> Result<()> {
        self.records.push(record);
        if self.records.len() == self.capacity {
            self.flush_run()?;
        }
        Ok(())
    }

    fn next_path(&mut self, phase: &str) -> PathBuf {
        let path = self.root.join(format!(
            "epoch-{}-{phase}-{:010}.bin",
            self.epoch, self.next_run
        ));
        self.next_run += 1;
        path
    }

    fn flush_run(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        self.records.sort_unstable();
        for pair in self.records.windows(2) {
            ensure!(
                pair[0].key() != pair[1].key(),
                "epoch {} repeats transaction coordinate {:?}",
                self.epoch,
                pair[0].key()
            );
        }
        let path = self.next_path("locators");
        let mut writer = BufWriter::with_capacity(1 << 20, create_new_file(&path)?);
        for record in &self.records {
            writer.write_all(&record.encode())?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        self.runs.push(path);
        self.records.clear();
        Ok(())
    }

    fn finish(mut self, output: &Path) -> Result<u64> {
        self.flush_run()?;
        if self.runs.is_empty() {
            let file = create_new_file(output)?;
            file.sync_all()?;
            return Ok(0);
        }
        while self.runs.len() > 1 {
            let old = std::mem::take(&mut self.runs);
            let mut next = Vec::with_capacity(old.len().div_ceil(MERGE_FAN_IN));
            for group in old.chunks(MERGE_FAN_IN) {
                if group.len() == 1 {
                    next.push(group[0].clone());
                    continue;
                }
                let path = self.next_path("locator-merge");
                merge_locator_runs(self.epoch, group, &path)?;
                for input in group {
                    fs::remove_file(input)?;
                }
                next.push(path);
            }
            self.runs = next;
        }
        let run = self.runs.pop().unwrap();
        ensure!(!output.exists(), "locator output already exists");
        fs::rename(&run, output)?;
        let bytes = fs::metadata(output)?.len();
        ensure!(
            bytes.is_multiple_of(LOCATOR_BYTES as u64),
            "locator output is truncated"
        );
        Ok(bytes / LOCATOR_BYTES as u64)
    }
}

struct LocatorRunReader {
    reader: BufReader<File>,
    current: Option<TransactionLocator>,
}

impl LocatorRunReader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        ensure!(
            file.metadata()?.len().is_multiple_of(LOCATOR_BYTES as u64),
            "locator run is truncated"
        );
        let mut this = Self {
            reader: BufReader::with_capacity(1 << 20, file),
            current: None,
        };
        this.advance()?;
        Ok(this)
    }

    fn advance(&mut self) -> Result<()> {
        let mut bytes = [0u8; LOCATOR_BYTES];
        match self.reader.read_exact(&mut bytes) {
            Ok(()) => self.current = Some(TransactionLocator::decode(bytes)?),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => self.current = None,
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

fn merge_locator_runs(epoch: u64, inputs: &[PathBuf], output: &Path) -> Result<()> {
    let mut readers = inputs
        .iter()
        .map(|path| LocatorRunReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::<Reverse<(TransactionLocator, usize)>>::new();
    for (index, reader) in readers.iter().enumerate() {
        if let Some(record) = reader.current {
            heap.push(Reverse((record, index)));
        }
    }
    let mut writer = BufWriter::with_capacity(1 << 20, create_new_file(output)?);
    let mut previous_key = None;
    while let Some(Reverse((record, index))) = heap.pop() {
        ensure!(
            previous_key != Some(record.key()),
            "epoch {epoch} repeats transaction coordinate {:?}",
            record.key()
        );
        writer.write_all(&record.encode())?;
        previous_key = Some(record.key());
        readers[index].advance()?;
        if let Some(next) = readers[index].current {
            heap.push(Reverse((next, index)));
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

struct LocatorFileReader {
    reader: BufReader<File>,
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> std::io::Result<u64> {
    use std::os::unix::fs::FileExt as _;

    let mut calls = 0u64;
    while !bytes.is_empty() {
        let read = file.read_at(bytes, offset)?;
        calls += 1;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "positioned read reached end of file",
            ));
        }
        offset += read as u64;
        bytes = &mut bytes[read..];
    }
    Ok(calls)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> std::io::Result<u64> {
    use std::os::windows::fs::FileExt as _;

    let mut calls = 0u64;
    while !bytes.is_empty() {
        let read = file.seek_read(bytes, offset)?;
        calls += 1;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "positioned read reached end of file",
            ));
        }
        offset += read as u64;
        bytes = &mut bytes[read..];
    }
    Ok(calls)
}

struct GlobalRegistryLookup {
    file: File,
    stamp: FileStamp,
    keys: Arc<Vec<[u8; KEY_BYTES]>>,
    rows: u64,
    prefix_offsets: Arc<Vec<u64>>,
}

impl GlobalRegistryLookup {
    fn new(build: &RegistryBuild, path: &Path) -> Result<Self> {
        build
            .stamp
            .verify(&build.file, "completed partial global public-key registry")?;
        verify_path_binding(
            path,
            &build.stamp,
            "completed partial global public-key registry",
        )?;
        ensure!(
            build.stamp.bytes == build.rows * KEY_BYTES as u64,
            "global registry size differs from its row count"
        );
        ensure!(
            build.prefix_offsets.len() == GLOBAL_PREFIX_COUNT + 1,
            "global prefix table length differs"
        );
        ensure!(
            build.keys.len() == usize::try_from(build.rows)?,
            "resident global registry size differs from its row count"
        );
        Ok(Self {
            file: build.file.try_clone()?,
            stamp: build.stamp.clone(),
            keys: Arc::clone(&build.keys),
            rows: build.rows,
            prefix_offsets: Arc::clone(&build.prefix_offsets),
        })
    }

    fn lookup(&self, key: &[u8; KEY_BYTES]) -> Result<u32> {
        let prefix = usize::from(u16::from_be_bytes([key[0], key[1]]));
        let low = usize::try_from(self.prefix_offsets[prefix])?;
        let high = usize::try_from(self.prefix_offsets[prefix + 1])?;
        let rows = usize::try_from(self.rows)?;
        ensure!(
            low <= high && high <= rows,
            "global registry prefix points outside the file"
        );
        let relative = self.keys[low..high].binary_search(key).map_err(|_| {
            anyhow::anyhow!("public key is absent from the completed dump-local registry")
        })?;
        let id = low
            .checked_add(relative)
            .and_then(|index| index.checked_add(1))
            .context("global registry ID overflow")?;
        u32::try_from(id).context("global registry ID exceeds u32")
    }

    fn verify_unchanged(&self) -> Result<()> {
        self.stamp
            .verify(&self.file, "partial global public-key registry")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PendingPubkey {
    SourceId(u32),
    Raw([u8; KEY_BYTES]),
}

struct CollectingVisitor<'a> {
    pending: &'a mut Vec<PendingPubkey>,
    seen_source_ids: &'a [u64],
    registry_entries: u32,
}

impl ArchiveV2WireRewriteVisitor for CollectingVisitor<'_> {
    type Checkpoint = usize;

    fn checkpoint(&mut self) -> Self::Checkpoint {
        self.pending.len()
    }

    fn rewrite_pubkey(
        &mut self,
        pubkey: CompactPubkey,
        _class: ArchiveV2WireReferenceClass,
    ) -> anyhow::Result<CompactPubkey> {
        if let CompactPubkey::Id(id) = pubkey {
            ensure!(
                id != 0 && id <= self.registry_entries,
                "source public-key ID {id} is outside 1..={}",
                self.registry_entries
            );
            let index = usize::try_from(
                id.checked_sub(1)
                    .context("source public-key ID zero is reserved")?,
            )?;
            let seen = self
                .seen_source_ids
                .get(index / 64)
                .context("source public-key ID exceeds seen-ID bitset")?;
            // The normal transaction list is small. Bound this scan so a
            // hostile maximum-size record cannot turn collection quadratic;
            // publish_pending_keys sorts and removes any later duplicates.
            if seen & (1u64 << (index % 64)) != 0
                || self
                    .pending
                    .iter()
                    .take(TRANSACTION_LINEAR_ID_DEDUP_LIMIT)
                    .any(|entry| *entry == PendingPubkey::SourceId(id))
            {
                return Ok(pubkey);
            }
        }
        self.pending.push(match pubkey {
            CompactPubkey::Id(id) => PendingPubkey::SourceId(id),
            CompactPubkey::Raw(raw) => PendingPubkey::Raw(raw),
        });
        Ok(pubkey)
    }

    fn rollback(&mut self, checkpoint: Self::Checkpoint) {
        self.pending.truncate(checkpoint);
    }
}

struct RemappingVisitor<'a> {
    global: &'a GlobalRegistryLookup,
    dense_source_ids: &'a [u32],
}

impl ArchiveV2WireRewriteVisitor for RemappingVisitor<'_> {
    type Checkpoint = ();

    fn checkpoint(&mut self) -> Self::Checkpoint {}

    fn rewrite_pubkey(
        &mut self,
        pubkey: CompactPubkey,
        _class: ArchiveV2WireReferenceClass,
    ) -> anyhow::Result<CompactPubkey> {
        let global_id = match pubkey {
            CompactPubkey::Raw(raw) => self.global.lookup(&raw)?,
            CompactPubkey::Id(id) => {
                let index = usize::try_from(
                    id.checked_sub(1)
                        .context("source public-key ID zero is reserved")?,
                )?;
                let global_id = *self.dense_source_ids.get(index).with_context(|| {
                    format!("source public-key ID {id} is outside the dense epoch cache")
                })?;
                ensure!(
                    global_id != 0,
                    "source public-key ID {id} is absent from the authenticated epoch map"
                );
                global_id
            }
        };
        Ok(CompactPubkey::Id(global_id))
    }

    fn rollback(&mut self, _checkpoint: Self::Checkpoint) {
        // The source-ID table is immutable.
    }
}

fn projector(profile: DumpWireProfile) -> ArchiveV2MessageProjector {
    ArchiveV2MessageProjector::new(match profile {
        DumpWireProfile::PostUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        }
        DumpWireProfile::PreUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataSchemaSelection {
    Current,
    Legacy,
    Both,
}

fn probe_ambiguous_metadata_schema(
    input: &[u8],
    current_output: &mut Vec<u8>,
    legacy_output: &mut Vec<u8>,
) -> Result<MetadataSchemaSelection> {
    let limits = ArchiveV2WireRewriteLimits::default();
    current_output.clear();
    let mut current_visitor = ArchiveV2WireIdentityVisitor;
    let current = rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
        input,
        current_output,
        &mut current_visitor,
        limits,
        ArchiveV2WireMetadataErrorSchema::Current,
    )
    .is_ok_and(|_| current_output == input);

    legacy_output.clear();
    let mut legacy_visitor = ArchiveV2WireIdentityVisitor;
    let legacy = rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
        input,
        legacy_output,
        &mut legacy_visitor,
        limits,
        ArchiveV2WireMetadataErrorSchema::Legacy,
    )
    .is_ok_and(|_| legacy_output == input);

    match (current, legacy) {
        (true, false) => Ok(MetadataSchemaSelection::Current),
        (false, true) => Ok(MetadataSchemaSelection::Legacy),
        (true, true) => Ok(MetadataSchemaSelection::Both),
        (false, false) => {
            bail!("ambiguous metadata is not an exact complete value under either error schema")
        }
    }
}

fn selected_metadata_schema(
    selection: MetadataSchemaSelection,
) -> ArchiveV2WireMetadataErrorSchema {
    match selection {
        MetadataSchemaSelection::Current => ArchiveV2WireMetadataErrorSchema::Current,
        MetadataSchemaSelection::Legacy => ArchiveV2WireMetadataErrorSchema::Legacy,
        MetadataSchemaSelection::Both => unreachable!("both schemas require isolated rewrites"),
    }
}

fn is_metadata_schema_ambiguity(error: &blockzilla_format::ArchiveV2WireRewriteError) -> bool {
    error.fallback_reason() == Some(ArchiveV2WireFallbackReason::MetadataErrorSchemaAmbiguous)
}

fn collect_metadata_pubkeys(
    input: &[u8],
    pending: &mut Vec<PendingPubkey>,
    seen_source_ids: &[u64],
    registry_entries: u32,
    identity_output: &mut Vec<u8>,
    comparison_output: &mut Vec<u8>,
) -> Result<()> {
    identity_output.clear();
    let automatic = {
        let mut visitor = CollectingVisitor {
            pending,
            seen_source_ids,
            registry_entries,
        };
        rewrite_archive_v2_metadata_wire_preserving_error_schema(
            input,
            identity_output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
    };
    match automatic {
        Ok(_) => {
            ensure!(
                identity_output == input,
                "identity metadata rewrite changed source bytes"
            );
            return Ok(());
        }
        Err(error) if is_metadata_schema_ambiguity(&error) => {}
        Err(error) => return Err(error).context("validate and visit source metadata"),
    }

    match probe_ambiguous_metadata_schema(input, identity_output, comparison_output)? {
        selection @ (MetadataSchemaSelection::Current | MetadataSchemaSelection::Legacy) => {
            identity_output.clear();
            let mut visitor = CollectingVisitor {
                pending,
                seen_source_ids,
                registry_entries,
            };
            rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
                input,
                identity_output,
                &mut visitor,
                ArchiveV2WireRewriteLimits::default(),
                selected_metadata_schema(selection),
            )
            .context("visit metadata with its exact selected error schema")?;
            ensure!(
                identity_output == input,
                "selected identity metadata rewrite changed source bytes"
            );
        }
        MetadataSchemaSelection::Both => {
            // The registry does not exist yet. Collect the union of both valid
            // interpretations so Pass 2 can compare their actual remapped
            // bytes without discovering a missing key.
            identity_output.clear();
            {
                let mut visitor = CollectingVisitor {
                    pending,
                    seen_source_ids,
                    registry_entries,
                };
                rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
                    input,
                    identity_output,
                    &mut visitor,
                    ArchiveV2WireRewriteLimits::default(),
                    ArchiveV2WireMetadataErrorSchema::Current,
                )
                .context("visit ambiguous metadata as current schema")?;
            }
            comparison_output.clear();
            {
                let mut visitor = CollectingVisitor {
                    pending,
                    seen_source_ids,
                    registry_entries,
                };
                rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
                    input,
                    comparison_output,
                    &mut visitor,
                    ArchiveV2WireRewriteLimits::default(),
                    ArchiveV2WireMetadataErrorSchema::Legacy,
                )
                .context("visit ambiguous metadata as legacy schema")?;
            }
            ensure!(
                identity_output == comparison_output,
                "both metadata schemas preserve the source identity differently"
            );
            ensure!(
                identity_output == input,
                "ambiguous identity metadata rewrite changed source bytes"
            );
        }
    }
    Ok(())
}

fn collect_transaction_pubkeys(
    record: &BorrowedTransactionRecord<'_>,
    pending: &mut Vec<PendingPubkey>,
    seen_source_ids: &[u64],
    registry_entries: u32,
    identity_output: &mut Vec<u8>,
    comparison_output: &mut Vec<u8>,
) -> Result<()> {
    pending.clear();
    identity_output.clear();
    {
        let mut visitor = CollectingVisitor {
            pending,
            seen_source_ids,
            registry_entries,
        };
        projector(record.source_wire_profile)
            .rewrite_message_wire(
                record.message_bytes,
                identity_output,
                &mut visitor,
                ArchiveV2WireRewriteLimits::default(),
            )
            .context("validate and visit source message")?;
    }
    ensure!(
        identity_output == record.message_bytes,
        "identity message rewrite changed non-public-key bytes"
    );
    if !record.metadata_bytes.is_empty() {
        collect_metadata_pubkeys(
            record.metadata_bytes,
            pending,
            seen_source_ids,
            registry_entries,
            identity_output,
            comparison_output,
        )
        .context("validate and visit source metadata")?;
    }
    Ok(())
}

fn publish_pending_keys(
    pending: &mut Vec<PendingPubkey>,
    seen_source_ids: &mut [u64],
    sorter: &mut RawKeySorter,
) -> Result<()> {
    // Remove repeated raw references inside the transaction. Source IDs use
    // the epoch bitset and therefore do not need a transaction-local set.
    pending.sort_unstable();
    pending.dedup();
    for entry in pending.drain(..) {
        match entry {
            PendingPubkey::SourceId(id) => {
                let index = usize::try_from(
                    id.checked_sub(1)
                        .context("source public-key ID zero is reserved")?,
                )?;
                let word = seen_source_ids
                    .get_mut(index / 64)
                    .context("source public-key ID exceeds seen-ID bitset")?;
                *word |= 1u64 << (index % 64);
            }
            PendingPubkey::Raw(raw) => sorter.push(raw)?,
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn resolve_source_id_batch(
    registry: &File,
    registry_entries: u32,
    sorted_ids: &[u32],
    expected_id_aliases: &[(u32, [u8; KEY_BYTES])],
    alias_index: &mut usize,
    read_buffer: &mut Vec<u8>,
    map_writer: &mut BufWriter<File>,
    key_sorter: &mut RawKeySorter,
) -> Result<SourceIdResolutionStats> {
    let mut stats = SourceIdResolutionStats::default();
    let mut start = 0usize;
    while start < sorted_ids.len() {
        let first_id = sorted_ids[start];
        ensure!(
            first_id != 0 && first_id <= registry_entries,
            "source public-key ID {first_id} is outside 1..={registry_entries}"
        );
        let group_offset = u64::from(first_id - 1)
            .checked_mul(KEY_BYTES as u64)
            .context("source registry bulk-read offset overflow")?;
        let mut end = start + 1;
        let mut group_end = group_offset
            .checked_add(KEY_BYTES as u64)
            .context("source registry bulk-read end overflow")?;
        while let Some(&id) = sorted_ids.get(end) {
            ensure!(
                id != 0 && id <= registry_entries,
                "source public-key ID {id} is outside 1..={registry_entries}"
            );
            ensure!(
                id > sorted_ids[end - 1],
                "source registry bulk-read IDs are not strictly sorted and unique"
            );
            let offset = u64::from(id - 1)
                .checked_mul(KEY_BYTES as u64)
                .context("source registry bulk-read offset overflow")?;
            let next_end = offset
                .checked_add(KEY_BYTES as u64)
                .context("source registry bulk-read end overflow")?;
            let gap = offset.saturating_sub(group_end);
            let group_bytes = next_end
                .checked_sub(group_offset)
                .context("source registry bulk-read range underflow")?;
            if gap > REGISTRY_BULK_READ_MAX_GAP_BYTES || group_bytes > REGISTRY_BULK_READ_MAX_BYTES
            {
                break;
            }
            group_end = next_end;
            end += 1;
        }

        let byte_len = usize::try_from(
            group_end
                .checked_sub(group_offset)
                .context("source registry bulk-read range underflow")?,
        )
        .context("source registry bulk-read length exceeds usize")?;
        read_buffer.resize(byte_len, 0);
        let read_calls = read_exact_at(registry, read_buffer, group_offset).with_context(|| {
            format!(
                "bulk read source registry IDs {}..{}",
                sorted_ids[start],
                sorted_ids[end - 1]
            )
        })?;
        stats.read_calls = stats
            .read_calls
            .checked_add(read_calls)
            .context("source ID registry read-call count overflow")?;
        stats.read_bytes = stats
            .read_bytes
            .checked_add(u64::try_from(byte_len).context("registry read length exceeds u64")?)
            .context("source ID registry read-byte count overflow")?;
        for &id in &sorted_ids[start..end] {
            let offset = u64::from(id - 1)
                .checked_mul(KEY_BYTES as u64)
                .and_then(|offset| offset.checked_sub(group_offset))
                .and_then(|offset| usize::try_from(offset).ok())
                .context("source registry row offset exceeds bulk-read range")?;
            let raw: [u8; KEY_BYTES] = read_buffer
                .get(offset..offset + KEY_BYTES)
                .context("source registry row is outside bulk-read bytes")?
                .try_into()
                .expect("checked source registry row length");
            while expected_id_aliases
                .get(*alias_index)
                .is_some_and(|(expected_id, _)| *expected_id == id)
            {
                ensure!(
                    expected_id_aliases[*alias_index].1 == raw,
                    "account-log source ID {id} does not resolve to its raw key"
                );
                *alias_index += 1;
            }
            map_writer.write_all(&id.to_le_bytes())?;
            map_writer.write_all(&raw)?;
            key_sorter.push(raw)?;
            stats.rows = stats
                .rows
                .checked_add(1)
                .context("source ID map row overflow")?;
        }
        start = end;
    }
    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
fn write_source_id_map(
    epoch: u64,
    registry: &File,
    registry_entries: u32,
    registry_stamp: &FileStamp,
    seen_source_ids: &[u64],
    expected_id_aliases: &mut Vec<(u32, [u8; KEY_BYTES])>,
    path: &Path,
    key_sorter: &mut RawKeySorter,
) -> Result<(File, SourceIdMapBinding, SourceIdResolutionStats)> {
    let expected_registry_bytes = u64::from(registry_entries)
        .checked_mul(KEY_BYTES as u64)
        .context("source registry byte length overflow")?;
    ensure!(
        registry_stamp.bytes == expected_registry_bytes,
        "epoch {epoch} source registry size differs from its entry count"
    );
    expected_id_aliases.sort_unstable();
    let mut alias_index = 0usize;
    let mut ids = Vec::with_capacity(REGISTRY_BULK_ID_BATCH_ROWS);
    let mut read_buffer = Vec::new();
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_read_write_file(path)?);
    let mut stats = SourceIdResolutionStats::default();
    for (word_index, &word) in seen_source_ids.iter().enumerate() {
        let mut remaining = word;
        while remaining != 0 {
            let bit = remaining.trailing_zeros() as usize;
            let index = word_index
                .checked_mul(64)
                .and_then(|base| base.checked_add(bit))
                .context("source ID bitset index overflow")?;
            let id = u32::try_from(index.checked_add(1).context("source ID overflow")?)?;
            ensure!(
                id <= registry_entries,
                "source public-key ID {id} exceeds the admitted registry"
            );
            ids.push(id);
            remaining &= remaining - 1;
            if ids.len() == REGISTRY_BULK_ID_BATCH_ROWS {
                stats.add(resolve_source_id_batch(
                    registry,
                    registry_entries,
                    &ids,
                    expected_id_aliases,
                    &mut alias_index,
                    &mut read_buffer,
                    &mut writer,
                    key_sorter,
                )?)?;
                ids.clear();
            }
        }
    }
    if !ids.is_empty() {
        stats.add(resolve_source_id_batch(
            registry,
            registry_entries,
            &ids,
            expected_id_aliases,
            &mut alias_index,
            &mut read_buffer,
            &mut writer,
            key_sorter,
        )?)?;
    }
    ensure!(
        alias_index == expected_id_aliases.len(),
        "epoch {epoch} account-log source ID is absent from the collected source-ID map"
    );
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let mut file = writer
        .into_inner()
        .map_err(std::io::IntoInnerError::into_error)?;
    let expected_bytes = stats
        .rows
        .checked_mul(SOURCE_ID_MAP_ROW_BYTES as u64)
        .context("source ID map byte length overflow")?;
    let stamp = FileStamp::read(&file)?;
    ensure!(
        stamp.bytes == expected_bytes,
        "epoch {epoch} source ID map size differs from its row count"
    );
    file.seek(SeekFrom::Start(0))?;
    registry_stamp.verify(registry, &format!("epoch {epoch} source registry"))?;
    Ok((
        file,
        SourceIdMapBinding {
            rows: stats.rows,
            bytes: stamp.bytes,
            stamp,
        },
        stats,
    ))
}

fn rewrite_metadata_pubkeys(
    input: &[u8],
    global: &GlobalRegistryLookup,
    dense_source_ids: &[u32],
    output: &mut Vec<u8>,
    comparison_output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let automatic = {
        let mut visitor = RemappingVisitor {
            global,
            dense_source_ids,
        };
        rewrite_archive_v2_metadata_wire_preserving_error_schema(
            input,
            output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
    };
    match automatic {
        Ok(_) => return Ok(()),
        Err(error) if is_metadata_schema_ambiguity(&error) => {}
        Err(error) => return Err(error).context("rewrite metadata public keys"),
    }

    match probe_ambiguous_metadata_schema(input, output, comparison_output)? {
        selection @ (MetadataSchemaSelection::Current | MetadataSchemaSelection::Legacy) => {
            output.clear();
            let mut visitor = RemappingVisitor {
                global,
                dense_source_ids,
            };
            rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
                input,
                output,
                &mut visitor,
                ArchiveV2WireRewriteLimits::default(),
                selected_metadata_schema(selection),
            )
            .context("rewrite metadata with its exact selected error schema")?;
        }
        MetadataSchemaSelection::Both => {
            output.clear();
            {
                let mut visitor = RemappingVisitor {
                    global,
                    dense_source_ids,
                };
                rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
                    input,
                    output,
                    &mut visitor,
                    ArchiveV2WireRewriteLimits::default(),
                    ArchiveV2WireMetadataErrorSchema::Current,
                )
                .context("rewrite ambiguous metadata as current schema")?;
            }
            comparison_output.clear();
            {
                let mut visitor = RemappingVisitor {
                    global,
                    dense_source_ids,
                };
                rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
                    input,
                    comparison_output,
                    &mut visitor,
                    ArchiveV2WireRewriteLimits::default(),
                    ArchiveV2WireMetadataErrorSchema::Legacy,
                )
                .context("rewrite ambiguous metadata as legacy schema")?;
            }
            ensure!(
                output == comparison_output,
                "ambiguous metadata schemas produce different public-key rewrites"
            );
        }
    }
    Ok(())
}

fn rewrite_transaction_pubkeys(
    record: RawTransactionRecordView<'_>,
    global: &GlobalRegistryLookup,
    dense_source_ids: &[u32],
    message_output: &mut Vec<u8>,
    metadata_output: &mut Vec<u8>,
    metadata_comparison_output: &mut Vec<u8>,
) -> Result<()> {
    message_output.clear();
    metadata_output.clear();
    {
        let mut visitor = RemappingVisitor {
            global,
            dense_source_ids,
        };
        projector(record.source_wire_profile)
            .rewrite_message_wire(
                record.message_bytes,
                message_output,
                &mut visitor,
                ArchiveV2WireRewriteLimits::default(),
            )
            .context("rewrite message public keys")?;
    }
    if !record.metadata_bytes.is_empty() {
        rewrite_metadata_pubkeys(
            record.metadata_bytes,
            global,
            dense_source_ids,
            metadata_output,
            metadata_comparison_output,
        )?;
    }
    Ok(())
}

fn read_input_root(input: &Path) -> Result<InputRoot> {
    let manifest_path = input.join(DUMP_MANIFEST_FILE);
    let manifest: DumpManifest = serde_json::from_slice(&read_bounded_regular(
        &manifest_path,
        MAX_ROOT_MANIFEST_BYTES,
    )?)
    .with_context(|| format!("parse {}", manifest_path.display()))?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::RawExtractionRoot
            && manifest.complete
            && manifest.workers != 0,
        "input is not a complete schema-{DUMP_SCHEMA_VERSION} raw extraction"
    );
    validate_source_binding(&manifest.source_binding)?;
    ensure!(
        manifest.transaction_stream == EPOCH_SHARDS_DIR
            && manifest.transaction_stream_sha256.is_none(),
        "raw extraction root has an invalid transaction-stream binding"
    );
    ensure!(
        manifest.signature_stream.is_none()
            && manifest.signature_stream_sha256.is_none()
            && manifest.signatures.is_none()
            && manifest.pubkeys.is_none()
            && manifest.pubkey_registry.is_none()
            && manifest.pubkey_registry_sha256.is_none()
            && manifest.registry_maps.is_none(),
        "raw extraction root already claims consolidated sidecars"
    );
    ensure!(
        manifest.account_id_log.is_none() && manifest.account_id_log_sha256.is_none(),
        "raw extraction root claims one epoch account log"
    );
    ensure!(
        manifest.discovered_accounts_sha256.is_some()
            && manifest.discovered_account_count.is_some(),
        "raw extraction root has incomplete frozen-account bindings"
    );
    let frozen_name = manifest
        .discovered_accounts
        .as_deref()
        .context("raw extraction root has no frozen account artifact")?;
    ensure!(
        frozen_name == ACCOUNTS_FILE,
        "raw extraction root uses an unexpected account artifact"
    );
    let frozen_path = input.join(frozen_name);
    let frozen_bytes = read_bounded_regular(
        &frozen_path,
        ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES as u64,
    )?;
    let expected_frozen = parse_hex_digest(
        manifest
            .discovered_accounts_sha256
            .as_deref()
            .context("raw extraction root has no frozen account digest")?,
        "frozen account digest",
    )?;
    ensure!(
        sha256_bytes(&frozen_bytes) == expected_frozen,
        "frozen account digest differs from the root manifest"
    );
    let frozen: DiscoveredAccountList = wincode::config::deserialize_exact(
        &frozen_bytes,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )?;
    let mint = parse_pubkey(&manifest.mint, "mint")?;
    ensure!(
        frozen.schema_version == DUMP_SCHEMA_VERSION && frozen.mint == mint,
        "frozen account header differs from the root manifest"
    );
    ensure!(
        frozen
            .accounts
            .windows(2)
            .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey),
        "frozen accounts are not strictly sorted and unique"
    );
    ensure!(
        frozen.anchor_position.slot == manifest.mint_slot
            && frozen.anchor_position.signature_count != 0,
        "frozen anchor position differs from the root target"
    );
    ensure!(
        frozen.accounts.iter().all(|account| {
            account.raw_pubkey != mint
                && account.first_creation.slot >= manifest.mint_slot
                && (manifest.first_epoch..=manifest.last_epoch)
                    .contains(&account.first_creation.epoch)
        }),
        "frozen accounts contain an invalid key or creation epoch"
    );
    ensure!(
        manifest.discovered_account_count
            == Some(
                u64::try_from(frozen.accounts.len()).context("frozen account count exceeds u64")?
            ),
        "frozen account count differs from the root manifest"
    );
    ensure!(
        manifest.first_epoch <= manifest.last_epoch,
        "root epoch range is reversed"
    );
    ensure!(
        (manifest.first_epoch..=manifest.last_epoch).contains(&frozen.anchor_position.epoch),
        "frozen anchor epoch is outside the root range"
    );
    let target = TargetBinding {
        mint,
        mint_slot: manifest.mint_slot,
        mint_signature: parse_signature(&manifest.mint_signature)?,
    };
    Ok(InputRoot {
        manifest,
        target,
        frozen_bytes,
        frozen,
    })
}

fn validate_source_binding(binding: &DumpSourceBinding) -> Result<()> {
    let DumpSourceBinding::TrustedLocalSizesOnly {
        cluster_id,
        slots_per_epoch,
        ..
    } = binding;
    ensure!(
        !cluster_id.is_empty() && (1..=MAX_SLOTS_PER_EPOCH).contains(slots_per_epoch),
        "trusted-local source binding is invalid"
    );
    Ok(())
}

fn source_wire_profile(value: DumpWireProfile) -> ArchiveV2WireProfile {
    match value {
        DumpWireProfile::PostUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        }
        DumpWireProfile::PreUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
        }
    }
}

fn open_epoch_source(
    archive_root: &Path,
    source_binding: &DumpSourceBinding,
    epoch: u64,
    expected_generation_digest: [u8; 32],
    expected_wire_profile: DumpWireProfile,
    verification_override: Option<HashVerification>,
    allow_metadata_generation_drift: bool,
) -> Result<OpenEpochSource> {
    validate_source_binding(source_binding)?;
    let epoch_path = archive_root.join(format!("epoch-{epoch}"));
    let source = PinnedLocalRangeSource::open_directory(&epoch_path)
        .with_context(|| format!("open descriptor-rooted source epoch {epoch}"))?;
    let DumpSourceBinding::TrustedLocalSizesOnly {
        cluster_id,
        slots_per_epoch,
        wire_profile,
    } = source_binding;
    ensure!(
        *wire_profile == expected_wire_profile,
        "trusted source wire profile differs from the shard"
    );
    let (_, published) = inspect_trusted_local_metadata_admission(
        &source,
        epoch,
        cluster_id,
        *slots_per_epoch,
        source_wire_profile(*wire_profile),
    )?;
    let (reader, allow_historical_digest_compatibility) = if let Some(manifest) = published {
        ensure!(
            parse_hex_digest(&manifest.generation_digest, "source generation digest")?
                == expected_generation_digest,
            "trusted-local current publication differs from the raw shard"
        );
        (
            ArchiveReader::open_candidate(
                source.clone(),
                manifest,
                OpenOptions {
                    hash_verification: verification_override.unwrap_or(HashVerification::SizesOnly),
                    ..OpenOptions::default()
                },
            )?,
            false,
        )
    } else {
        (
            ArchiveReader::open_trusted_with_additional_files_and_metadata_profile(
                source.clone(),
                TrustedGenerationIdentity {
                    cluster_id: cluster_id.clone(),
                    epoch,
                    generation_id: "token-transaction-dump-trusted-local-sizes-v1".to_owned(),
                    slots_per_epoch: *slots_per_epoch,
                    wire_profile: source_wire_profile(*wire_profile),
                },
                &[SIGNATURES_FILE, REGISTRY_INDEX_FILE],
                &[
                    blockzilla_format::ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
                    blockzilla_format::ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
                ],
                ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
                OpenOptions {
                    hash_verification: HashVerification::SizesOnly,
                    ..OpenOptions::default()
                },
            )?,
            true,
        )
    };
    let opened_generation_digest = reader.binding().generation_digest;
    let prior_generation_digest = allow_historical_digest_compatibility
        .then(|| {
            prior_trusted_sizes_digest(
                reader.manifest(),
                source_wire_profile(expected_wire_profile),
            )
        })
        .transpose()?;
    let zero_hash_generation_digest = allow_historical_digest_compatibility
        .then(|| zero_hash_trusted_sizes_digest(reader.manifest()))
        .transpose()?;
    let generation_matches = opened_generation_digest == expected_generation_digest
        || prior_generation_digest == Some(expected_generation_digest)
        || zero_hash_generation_digest == Some(expected_generation_digest);
    ensure!(
        generation_matches || allow_metadata_generation_drift,
        "source epoch {epoch} generation differs from the raw shard: raw={}, current={}, prior_sizes={}, zero_hash_sizes={}",
        hex_digest(expected_generation_digest),
        hex_digest(opened_generation_digest),
        prior_generation_digest.map_or_else(|| "not-applicable".to_owned(), hex_digest),
        zero_hash_generation_digest.map_or_else(|| "not-applicable".to_owned(), hex_digest),
    );
    ensure!(
        reader.manifest().epoch == epoch,
        "source manifest epoch differs from its directory"
    );
    ensure!(
        reader.message_projector().wire_profile() == source_wire_profile(expected_wire_profile),
        "source epoch {epoch} message profile differs from the raw shard"
    );
    ensure!(
        reader.signatures_available(),
        "source epoch {epoch} has no signatures sidecar"
    );
    let registry_entries = reader.registry_entries();
    let slots_per_epoch = reader.manifest().slots_per_epoch;
    ensure!(
        registry_entries <= MAX_SOURCE_REGISTRY_ENTRIES,
        "source epoch {epoch} registry has {registry_entries} rows; the 8 GiB consolidation limit is {MAX_SOURCE_REGISTRY_ENTRIES}"
    );
    ensure!(
        (1..=MAX_SLOTS_PER_EPOCH).contains(&slots_per_epoch),
        "source epoch {epoch} slots_per_epoch exceeds the consolidation limit"
    );
    Ok(OpenEpochSource {
        source,
        registry_entries,
        slots_per_epoch,
    })
}

fn read_frame_hashed(
    reader: &mut BufReader<File>,
    logical_offset: &mut u64,
    hasher: &mut Sha256,
    payload: &mut Vec<u8>,
) -> Result<Option<(u64, u32)>> {
    let mut value = 0u32;
    let mut shift = 0u32;
    let mut prefix = [0u8; 5];
    let mut prefix_len = 0usize;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            ensure!(
                prefix_len == 0,
                "raw stream ends inside a frame-length prefix"
            );
            return Ok(None);
        }
        let mut consumed = 0usize;
        let mut complete = false;
        for &byte in available.iter().take(prefix.len() - prefix_len) {
            prefix[prefix_len] = byte;
            prefix_len += 1;
            consumed += 1;
            if shift == 28 {
                ensure!(byte & 0xf0 == 0, "raw stream frame length overflows u32");
            }
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                ensure!(
                    prefix_len == 1 || byte & 0x7f != 0,
                    "raw stream has a non-minimal frame length"
                );
                complete = true;
                break;
            }
            shift += 7;
            ensure!(shift <= 28, "raw stream frame length overflows u32");
        }
        reader.consume(consumed);
        if complete {
            break;
        }
    }
    hasher.update(&prefix[..prefix_len]);
    let length = usize::try_from(value).context("frame length exceeds usize")?;
    ensure!(
        length <= WINCODE_LEB128_MAX_FRAME_BYTES,
        "raw stream frame exceeds the Wincode limit"
    );
    let payload_offset = logical_offset
        .checked_add(u64::try_from(prefix_len).context("frame prefix length exceeds u64")?)
        .context("raw stream payload offset overflow")?;
    payload.resize(length, 0);
    reader.read_exact(payload)?;
    hasher.update(&*payload);
    *logical_offset = payload_offset
        .checked_add(value.into())
        .context("raw stream logical offset overflow")?;
    Ok(Some((payload_offset, value)))
}

fn decode_frame(payload: &[u8]) -> Result<TokenTransactionDumpRecord> {
    wincode::config::deserialize_exact(
        payload,
        bounded_wincode_leb128_config::<WINCODE_LEB128_MAX_FRAME_BYTES>(),
    )
    .map_err(Into::into)
}

fn validate_record_basic(
    epoch: u64,
    slots_per_epoch: u64,
    generation: [u8; 32],
    profile: DumpWireProfile,
    record: RawTransactionRecordView<'_>,
) -> Result<()> {
    ensure!(record.source_epoch == epoch, "raw record epoch differs");
    ensure!(
        record.source_generation_digest == generation,
        "raw record generation differs"
    );
    ensure!(
        record.source_wire_profile == profile,
        "raw record message profile differs"
    );
    let first_slot = epoch
        .checked_mul(slots_per_epoch)
        .context("epoch first slot overflow")?;
    let last_slot = first_slot
        .checked_add(slots_per_epoch - 1)
        .context("epoch last slot overflow")?;
    ensure!(
        (first_slot..=last_slot).contains(&record.block.slot),
        "raw record slot is outside its epoch"
    );
    ensure!(
        record.block.parent_slot < record.block.slot,
        "raw block parent is not earlier than its slot"
    );
    ensure!(
        u64::from(record.source_block_id) < slots_per_epoch,
        "raw source block ID is outside its epoch"
    );
    ensure!(
        record.block.transaction_count != 0 && record.tx_index < record.block.transaction_count,
        "raw transaction index is outside its block"
    );
    ensure!(
        record.signature_count != 0,
        "raw transaction has no signatures"
    );
    record
        .source_first_signature_ordinal
        .checked_add(u64::from(record.signature_count))
        .context("raw signature range overflow")?;
    ensure!(
        record.dump_signature_ordinal.is_none(),
        "raw transaction already has a dump signature ordinal"
    );
    ensure!(
        !record.message_bytes.is_empty(),
        "raw transaction has an empty message"
    );
    let has_metadata = record.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
    ensure!(
        has_metadata == !record.metadata_bytes.is_empty(),
        "raw metadata flag differs from its bytes"
    );
    let has_error = record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0;
    ensure!(
        has_error == (record.metadata_bytes.first() == Some(&1)),
        "raw transaction-error flag differs from its metadata bytes"
    );
    ensure!(
        record.flags
            & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
            == 0,
        "raw transaction has an opaque fallback"
    );
    Ok(())
}

fn validate_shard_manifest(path: &Path, root: &InputRoot, epoch: u64) -> Result<DumpManifest> {
    let manifest: DumpManifest =
        serde_json::from_slice(&read_bounded_regular(path, MAX_SHARD_MANIFEST_BYTES)?)?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::RawEpochShard
            && manifest.complete,
        "epoch {epoch} shard manifest is not complete schema 3"
    );
    ensure!(
        manifest.first_epoch == epoch && manifest.last_epoch == epoch,
        "epoch {epoch} shard range differs"
    );
    ensure!(
        manifest.mint == root.manifest.mint
            && manifest.mint_slot == root.manifest.mint_slot
            && manifest.mint_signature == root.manifest.mint_signature,
        "epoch {epoch} shard target differs"
    );
    ensure!(
        manifest.workers == root.manifest.workers,
        "epoch {epoch} worker binding differs"
    );
    ensure!(
        manifest.source_binding == root.manifest.source_binding,
        "epoch {epoch} source admission differs"
    );
    ensure!(
        manifest.transaction_stream == TRANSACTIONS_FILE,
        "epoch {epoch} transaction file name differs"
    );
    ensure!(
        manifest.account_id_log.as_deref() == Some(ACCOUNT_ID_LOG_FILE),
        "epoch {epoch} account log name differs"
    );
    ensure!(
        manifest.signatures.is_none()
            && manifest.pubkeys.is_none()
            && manifest.signature_stream.is_none()
            && manifest.signature_stream_sha256.is_none()
            && manifest.pubkey_registry.is_none()
            && manifest.pubkey_registry_sha256.is_none()
            && manifest.registry_maps.is_none()
            && manifest.discovered_accounts.is_none()
            && manifest.discovered_accounts_sha256.is_none()
            && manifest.discovered_account_count.is_none(),
        "epoch {epoch} raw shard claims consolidated artifacts"
    );
    ensure!(
        manifest.transaction_stream_sha256.is_some() && manifest.account_id_log_sha256.is_some(),
        "epoch {epoch} raw shard has incomplete artifact hashes"
    );
    Ok(manifest)
}

fn validate_exact_shard_files(directory: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(directory)
        .with_context(|| format!("inspect raw shard {}", directory.display()))?;
    ensure!(
        metadata.file_type().is_dir(),
        "raw shard is not a direct directory"
    );
    let mut observed = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        ensure!(
            entry.file_type()?.is_file(),
            "raw shard member is not a regular file"
        );
        observed.push(
            entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("raw shard has a non-UTF-8 file name"))?,
        );
    }
    observed.sort_unstable();
    let mut expected = vec![
        ACCOUNT_ID_LOG_FILE.to_owned(),
        DUMP_MANIFEST_FILE.to_owned(),
        TRANSACTIONS_FILE.to_owned(),
    ];
    expected.sort_unstable();
    ensure!(
        observed == expected,
        "raw shard has unexpected or missing files"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockIdentity {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    transaction_count: u32,
}

impl From<&TokenTransactionBlockContext> for BlockIdentity {
    fn from(value: &TokenTransactionBlockContext) -> Self {
        Self {
            slot: value.slot,
            parent_slot: value.parent_slot,
            blockhash_id: value.blockhash_id,
            previous_blockhash_id: value.previous_blockhash_id,
            block_time: value.block_time,
            block_height: value.block_height,
            transaction_count: value.transaction_count,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn add_account_log_keys(
    root: &InputRoot,
    epoch: u64,
    expected_generation: [u8; 32],
    path: &Path,
    expected_sha256: [u8; 32],
    registry_entries: u32,
    seen_source_ids: &mut [u64],
    expected_id_aliases: &mut Vec<(u32, [u8; KEY_BYTES])>,
) -> Result<()> {
    let bytes = read_bounded_regular(path, ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES as u64)?;
    ensure!(
        sha256_bytes(&bytes) == expected_sha256,
        "epoch {epoch} account log digest differs"
    );
    let log: EpochAccountIdLog = wincode::config::deserialize_exact(
        &bytes,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )
    .with_context(|| format!("decode {}", path.display()))?;
    ensure!(
        log.schema_version == DUMP_SCHEMA_VERSION
            && log.epoch == epoch
            && log.source_generation_digest == expected_generation,
        "epoch {epoch} account log header differs"
    );
    ensure!(
        log.entries
            .windows(2)
            .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey),
        "epoch {epoch} account log is not strictly sorted and unique"
    );
    let mut expected_token_accounts = root
        .frozen
        .accounts
        .iter()
        .filter(|account| account.first_creation.epoch <= epoch);
    let mut mint_count = 0usize;
    for entry in &log.entries {
        match entry.role {
            AccountIdRole::TargetMint => {
                mint_count += 1;
                ensure!(
                    entry.raw_pubkey == root.target.mint && entry.first_creation.is_none(),
                    "epoch {epoch} target-mint account-log row differs"
                );
            }
            AccountIdRole::TokenAccount => {
                let first_creation = entry
                    .first_creation
                    .context("token-account log row has no creation coordinate")?;
                let expected = expected_token_accounts.next().with_context(|| {
                    format!("epoch {epoch} account log has an extra token-account row")
                })?;
                ensure!(
                    entry.raw_pubkey == expected.raw_pubkey
                        && first_creation == expected.first_creation,
                    "epoch {epoch} account-log token-account row differs from the frozen prefix"
                );
            }
        }
        if let Some(id) = entry.local_id {
            ensure!(
                id != 0 && id <= registry_entries,
                "epoch {epoch} account-log source ID {id} is outside 1..={registry_entries}"
            );
            let index = usize::try_from(id - 1)?;
            let word = &mut seen_source_ids[index / 64];
            *word |= 1u64 << (index % 64);
            expected_id_aliases.push((id, entry.raw_pubkey));
        }
    }
    ensure!(
        mint_count == 1 && expected_token_accounts.next().is_none(),
        "epoch {epoch} account log does not exactly cover the target mint and frozen account prefix"
    );
    Ok(())
}

fn preflight_epoch_source(
    archive_root: &Path,
    root: &InputRoot,
    epoch: u64,
    shard_directory: &Path,
    allow_metadata_generation_drift: bool,
) -> Result<()> {
    validate_exact_shard_files(shard_directory)?;
    let _ = validate_shard_manifest(&shard_directory.join(DUMP_MANIFEST_FILE), root, epoch)?;
    let stream_path = shard_directory.join(TRANSACTIONS_FILE);
    let stream_file = File::open(&stream_path)?;
    let stream_stamp = FileStamp::read(&stream_file)?;
    let mut reader = BufReader::with_capacity(1 << 20, stream_file);
    let mut logical_offset = 0u64;
    let mut ignored_hash = Sha256::new();
    let mut payload = Vec::new();
    read_frame_hashed(
        &mut reader,
        &mut logical_offset,
        &mut ignored_hash,
        &mut payload,
    )?
    .context("raw transaction stream is empty during source preflight")?;
    let TokenTransactionDumpRecord::Header(header) = decode_frame(&payload)? else {
        bail!("epoch {epoch} raw stream does not start with a header")
    };
    ensure!(
        header.schema_version == DUMP_SCHEMA_VERSION
            && header.stream_kind == DumpStreamKind::RawEpochShard
            && header.source_epoch == Some(epoch)
            && header.pubkey_registry_id_base == PUBKEY_REGISTRY_ID_BASE
            && header.mint == root.target.mint
            && header.mint_slot == root.target.mint_slot
            && header.mint_signature == root.target.mint_signature,
        "epoch {epoch} raw header is invalid during source preflight"
    );
    let generation = header
        .source_generation_digest
        .context("raw preflight header has no source generation")?;
    let profile = header
        .source_wire_profile
        .context("raw preflight header has no wire profile")?;
    let opened = open_epoch_source(
        archive_root,
        &root.manifest.source_binding,
        epoch,
        generation,
        profile,
        Some(HashVerification::SizesOnly),
        allow_metadata_generation_drift,
    )?;
    let registry = opened.source.open_file(REGISTRY_FILE)?;
    ensure!(
        FileStamp::read(&registry)?.bytes
            == u64::from(opened.registry_entries)
                .checked_mul(KEY_BYTES as u64)
                .context("source registry byte length overflow")?,
        "epoch {epoch} registry size differs during preflight"
    );
    let signatures = opened.source.open_file(SIGNATURES_FILE)?;
    ensure!(
        FileStamp::read(&signatures)?
            .bytes
            .is_multiple_of(SIGNATURE_BYTES as u64),
        "epoch {epoch} signatures size differs during preflight"
    );
    opened.source.verify_unchanged()?;
    stream_stamp.verify(
        &reader.into_inner(),
        &format!("epoch {epoch} raw preflight stream"),
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_epoch_pass_one(
    archive_root: &Path,
    root: &InputRoot,
    epoch: u64,
    shard_directory: &Path,
    work: &Path,
    key_sorter: &mut RawKeySorter,
    aggregate: &mut AggregateFooter,
    allow_metadata_generation_drift: bool,
) -> Result<EpochPlan> {
    validate_exact_shard_files(shard_directory)?;
    let shard_manifest =
        validate_shard_manifest(&shard_directory.join(DUMP_MANIFEST_FILE), root, epoch)?;
    let expected_stream_sha = parse_hex_digest(
        shard_manifest
            .transaction_stream_sha256
            .as_deref()
            .context("shard manifest has no transaction digest")?,
        "raw transaction stream digest",
    )?;
    let expected_account_sha = parse_hex_digest(
        shard_manifest
            .account_id_log_sha256
            .as_deref()
            .context("shard manifest has no account-log digest")?,
        "account log digest",
    )?;
    let stream_path = shard_directory.join(TRANSACTIONS_FILE);
    let stream_file =
        File::open(&stream_path).with_context(|| format!("open {}", stream_path.display()))?;
    let stream_stamp = FileStamp::read(&stream_file)?;
    let stream_size = stream_stamp.bytes;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, stream_file);
    let mut stream_hasher = Sha256::new();
    let mut logical_offset = 0u64;
    let mut payload = Vec::new();
    let (_, _) = read_frame_hashed(
        &mut reader,
        &mut logical_offset,
        &mut stream_hasher,
        &mut payload,
    )?
    .context("raw transaction stream is empty")?;
    let decoded_header = decode_frame(&payload)?;
    let TokenTransactionDumpRecord::Header(header) = decoded_header else {
        bail!("epoch {epoch} raw stream does not start with a header")
    };
    ensure!(
        header.schema_version == DUMP_SCHEMA_VERSION
            && header.stream_kind == DumpStreamKind::RawEpochShard
            && header.source_epoch == Some(epoch)
            && header.source_generation_digest.is_some()
            && header.source_wire_profile.is_some()
            && header.pubkey_registry_id_base == PUBKEY_REGISTRY_ID_BASE,
        "epoch {epoch} raw stream header is invalid"
    );
    ensure!(
        header.mint == root.target.mint
            && header.mint_slot == root.target.mint_slot
            && header.mint_signature == root.target.mint_signature,
        "epoch {epoch} raw stream target differs"
    );
    let source_generation_digest = header.source_generation_digest.unwrap();
    let source_wire_profile = header.source_wire_profile.unwrap();
    let opened = open_epoch_source(
        archive_root,
        &root.manifest.source_binding,
        epoch,
        source_generation_digest,
        source_wire_profile,
        None,
        allow_metadata_generation_drift,
    )?;
    let registry_file = opened.source.open_file(REGISTRY_FILE)?;
    let registry_stamp = FileStamp::read(&registry_file)?;
    ensure!(
        registry_stamp.bytes
            == u64::from(opened.registry_entries)
                .checked_mul(KEY_BYTES as u64)
                .context("source registry byte length overflow")?,
        "epoch {epoch} source registry size differs from its entry count"
    );
    let bit_words = usize::try_from(opened.registry_entries)?.div_ceil(64);
    let mut seen_source_ids = vec![0u64; bit_words];
    let mut expected_id_aliases = Vec::new();
    let signatures_file = opened.source.open_file(SIGNATURES_FILE)?;
    let signatures_stamp = FileStamp::read(&signatures_file)?;
    let signature_file_bytes = signatures_stamp.bytes;
    ensure!(
        signature_file_bytes.is_multiple_of(SIGNATURE_BYTES as u64),
        "epoch {epoch} signature sidecar has a partial row"
    );
    let source_signature_count = signature_file_bytes / SIGNATURE_BYTES as u64;

    let locator_path = work.join(format!("epoch-{epoch}-canonical.locators"));
    let mut locator_sorter = LocatorSorter::new(work, epoch, LOCATOR_SORT_MEMORY_BYTES)?;
    let mut pending = Vec::new();
    let mut identity_output = Vec::new();
    let mut identity_comparison_output = Vec::new();
    let epoch_slots =
        usize::try_from(opened.slots_per_epoch).context("slots per epoch exceeds usize")?;
    let mut blocks = Vec::new();
    blocks
        .try_reserve_exact(epoch_slots)
        .context("reserve source-block identity table")?;
    blocks.resize(epoch_slots, None::<BlockIdentity>);
    let mut slot_block_ids = Vec::new();
    slot_block_ids
        .try_reserve_exact(epoch_slots)
        .context("reserve slot-to-source-block table")?;
    slot_block_ids.resize(epoch_slots, u32::MAX);
    let epoch_first_slot = epoch
        .checked_mul(opened.slots_per_epoch)
        .context("epoch first slot overflow")?;
    let mut selected_block_count = 0u64;
    let mut transaction_count = 0u64;
    let mut signature_count = 0u64;
    let mut anchor_count = 0u64;
    let footer = loop {
        let Some((payload_offset, payload_len)) = read_frame_hashed(
            &mut reader,
            &mut logical_offset,
            &mut stream_hasher,
            &mut payload,
        )?
        else {
            bail!("epoch {epoch} raw stream has no footer")
        };
        let decoded = decode_borrowed_frame(&payload)?;
        match decoded {
            BorrowedDumpRecord::Header(_) => {
                bail!("epoch {epoch} raw stream has more than one header")
            }
            BorrowedDumpRecord::Footer(footer) => break footer,
            BorrowedDumpRecord::Transaction(record) => {
                validate_record_basic(
                    epoch,
                    opened.slots_per_epoch,
                    source_generation_digest,
                    source_wire_profile,
                    (&record).into(),
                )?;
                let signature_end = record
                    .source_first_signature_ordinal
                    .checked_add(u64::from(record.signature_count))
                    .context("source signature range overflow")?;
                ensure!(
                    signature_end <= source_signature_count,
                    "epoch {epoch} transaction signature range exceeds signatures.bin"
                );
                collect_transaction_pubkeys(
                    &record,
                    &mut pending,
                    &seen_source_ids,
                    opened.registry_entries,
                    &mut identity_output,
                    &mut identity_comparison_output,
                )
                .with_context(|| {
                    format!(
                        "epoch {epoch} slot {} transaction {}",
                        record.block.slot, record.tx_index
                    )
                })?;
                publish_pending_keys(&mut pending, &mut seen_source_ids, key_sorter)?;
                let identity = BlockIdentity::from(&record.block);
                let block_index = usize::try_from(record.source_block_id)?;
                match blocks[block_index] {
                    Some(previous) => ensure!(
                        previous == identity,
                        "epoch {epoch} source block ID has conflicting context"
                    ),
                    None => {
                        blocks[block_index] = Some(identity);
                        selected_block_count = selected_block_count
                            .checked_add(1)
                            .context("selected block count overflow")?;
                    }
                }
                let slot_index = usize::try_from(record.block.slot - epoch_first_slot)?;
                let previous = slot_block_ids[slot_index];
                if previous == u32::MAX {
                    slot_block_ids[slot_index] = record.source_block_id;
                } else {
                    ensure!(
                        previous == record.source_block_id,
                        "epoch {epoch} slot has conflicting source block IDs"
                    );
                }
                locator_sorter.push(TransactionLocator {
                    slot: record.block.slot,
                    source_block_id: record.source_block_id,
                    tx_index: record.tx_index,
                    payload_offset,
                    payload_len,
                })?;
                transaction_count = transaction_count
                    .checked_add(1)
                    .context("transaction count overflow")?;
                signature_count = signature_count
                    .checked_add(u64::from(record.signature_count))
                    .context("selected signature count overflow")?;
                let is_anchor = (
                    record.source_epoch,
                    record.block.slot,
                    record.source_block_id,
                    record.tx_index,
                ) == (
                    root.frozen.anchor_position.epoch,
                    root.frozen.anchor_position.slot,
                    root.frozen.anchor_position.source_block_id,
                    root.frozen.anchor_position.tx_index,
                );
                if is_anchor {
                    ensure!(
                        record.source_first_signature_ordinal
                            == root.frozen.anchor_position.source_first_signature_ordinal
                            && record.signature_count
                                == root.frozen.anchor_position.signature_count,
                        "epoch {epoch} anchor signature range differs from the frozen position"
                    );
                }
                anchor_count = anchor_count
                    .checked_add(u64::from(is_anchor))
                    .context("anchor count overflow")?;
            }
        }
    };
    ensure!(
        read_frame_hashed(
            &mut reader,
            &mut logical_offset,
            &mut stream_hasher,
            &mut payload,
        )?
        .is_none(),
        "epoch {epoch} raw stream has records after its footer"
    );
    ensure!(
        logical_offset == stream_size,
        "epoch {epoch} raw stream size changed while it was read"
    );
    ensure!(
        <[u8; 32]>::from(stream_hasher.finalize()) == expected_stream_sha,
        "epoch {epoch} raw stream digest differs from its manifest"
    );
    ensure!(
        transaction_count == shard_manifest.transactions,
        "epoch {epoch} transaction count differs from its manifest"
    );
    ensure!(
        footer.epochs == 1
            && footer.transactions_written == transaction_count
            && footer.transactions_scanned >= transaction_count
            && footer.blocks_scanned <= opened.slots_per_epoch
            && selected_block_count <= footer.blocks_scanned
            && footer.owned_block_fallbacks <= footer.blocks_scanned
            && footer.pubkeys == 0
            && footer.signatures == 0
            && footer.raw_transaction_fallbacks == 0
            && footer.raw_metadata_fallbacks == 0,
        "epoch {epoch} raw footer is invalid"
    );
    ensure!(
        anchor_count == u64::from(epoch == root.frozen.anchor_position.epoch),
        "epoch {epoch} anchor occurrence count differs"
    );
    let locator_count = locator_sorter.finish(&locator_path)?;
    ensure!(
        locator_count == transaction_count,
        "epoch {epoch} locator count differs"
    );
    add_account_log_keys(
        root,
        epoch,
        source_generation_digest,
        &shard_directory.join(ACCOUNT_ID_LOG_FILE),
        expected_account_sha,
        opened.registry_entries,
        &mut seen_source_ids,
        &mut expected_id_aliases,
    )?;
    let source_id_map_path = work.join(format!("epoch-{epoch}-source-pubkeys.map"));
    let source_id_resolution_started = Instant::now();
    let (source_id_map_file, source_id_map, source_id_stats) = write_source_id_map(
        epoch,
        &registry_file,
        opened.registry_entries,
        &registry_stamp,
        &seen_source_ids,
        &mut expected_id_aliases,
        &source_id_map_path,
        key_sorter,
    )?;
    eprintln!(
        "consolidate source IDs: epoch {epoch}, {} unique IDs, {} registry read calls, {} registry bytes requested, {:.3}s resolution",
        source_id_stats.rows,
        source_id_stats.read_calls,
        source_id_stats.read_bytes,
        source_id_resolution_started.elapsed().as_secs_f64(),
    );
    let stream_file = reader.into_inner();
    stream_stamp.verify(
        &stream_file,
        &format!("epoch {epoch} raw transaction stream"),
    )?;
    registry_stamp.verify(&registry_file, &format!("epoch {epoch} source registry"))?;
    signatures_stamp.verify(
        &signatures_file,
        &format!("epoch {epoch} source signatures"),
    )?;
    opened.source.verify_unchanged()?;
    aggregate.add(footer)?;
    Ok(EpochPlan {
        epoch,
        stream_file,
        stream_stamp,
        locator_path,
        source_id_map_path,
        source_id_map_file,
        source_id_map,
        source_generation_digest,
        source_wire_profile,
        registry_entries: opened.registry_entries,
        registry_stamp,
        signatures_stamp,
        slots_per_epoch: opened.slots_per_epoch,
        transaction_count,
        signature_count,
    })
}

impl LocatorFileReader {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: BufReader::with_capacity(1 << 20, File::open(path)?),
        })
    }

    fn next(&mut self) -> Result<Option<TransactionLocator>> {
        let mut bytes = [0u8; LOCATOR_BYTES];
        match self.reader.read_exact(&mut bytes) {
            Ok(()) => Ok(Some(TransactionLocator::decode(bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

struct DigestWriter {
    writer: BufWriter<File>,
    hasher: Sha256,
    bytes: u64,
}

impl DigestWriter {
    fn create(path: &Path) -> Result<Self> {
        Ok(Self {
            writer: BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_file(path)?),
            hasher: Sha256::new(),
            bytes: 0,
        })
    }

    fn resume(path: &Path, committed_bytes: u64) -> Result<Self> {
        let mut file = FsOpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open resumable output {}", path.display()))?;
        let actual = file.metadata()?.len();
        ensure!(
            actual >= committed_bytes,
            "resumable output {} is shorter than its checkpoint",
            path.display()
        );
        if actual != committed_bytes {
            file.set_len(committed_bytes)
                .with_context(|| format!("truncate incomplete output {}", path.display()))?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::Start(0))?;
        let mut hasher = Sha256::new();
        let mut remaining = committed_bytes;
        let mut buffer = vec![0u8; IO_BUFFER_BYTES];
        while remaining != 0 {
            let take = usize::try_from(remaining.min(IO_BUFFER_BYTES as u64))?;
            file.read_exact(&mut buffer[..take])?;
            hasher.update(&buffer[..take]);
            remaining -= take as u64;
        }
        file.seek(SeekFrom::Start(committed_bytes))?;
        Ok(Self {
            writer: BufWriter::with_capacity(IO_BUFFER_BYTES, file),
            hasher,
            bytes: committed_bytes,
        })
    }

    fn checkpoint(&mut self) -> Result<u64> {
        self.flush()?;
        self.writer.get_ref().sync_all()?;
        ensure!(self.writer.get_ref().metadata()?.len() == self.bytes);
        Ok(self.bytes)
    }

    fn finish(mut self) -> Result<ArtifactBinding> {
        self.flush()?;
        self.writer.get_ref().sync_all()?;
        let file = self
            .writer
            .into_inner()
            .map_err(std::io::IntoInnerError::into_error)?;
        let stamp = FileStamp::read(&file)?;
        ensure!(
            stamp.bytes == self.bytes,
            "finished artifact size differs from its written byte count"
        );
        Ok(ArtifactBinding {
            bytes: self.bytes,
            sha256: self.hasher.finalize().into(),
            file,
            stamp,
        })
    }
}

impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.bytes = self
            .bytes
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("artifact byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

fn write_frame<T>(writer: &mut DigestWriter, record: &T, scratch: &mut Vec<u8>) -> Result<()>
where
    T: SchemaWrite<WincodeLeb128Config, Src = T> + ?Sized,
{
    encode_with_scratch(record, scratch)?;
    ensure!(
        scratch.len() <= WINCODE_LEB128_MAX_FRAME_BYTES,
        "consolidated transaction frame exceeds the Wincode limit"
    );
    write_u32_varint(
        writer,
        u32::try_from(scratch.len()).context("consolidated frame exceeds u32")?,
    )?;
    writer.write_all(scratch)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SignatureRange {
    source_offset: u64,
    destination_start: usize,
    bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct SignatureExpectation {
    destination_start: usize,
    expected: [u8; SIGNATURE_BYTES],
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SignatureReadStats {
    input_ranges: u64,
    input_bytes: u64,
    physical_read_ranges: u64,
    physical_read_calls: u64,
    physical_read_bytes: u64,
}

impl SignatureReadStats {
    fn checked_sub(self, earlier: Self) -> Result<Self> {
        Ok(Self {
            input_ranges: self
                .input_ranges
                .checked_sub(earlier.input_ranges)
                .context("signature input-range counter decreased")?,
            input_bytes: self
                .input_bytes
                .checked_sub(earlier.input_bytes)
                .context("signature input-byte counter decreased")?,
            physical_read_ranges: self
                .physical_read_ranges
                .checked_sub(earlier.physical_read_ranges)
                .context("signature physical read-range counter decreased")?,
            physical_read_calls: self
                .physical_read_calls
                .checked_sub(earlier.physical_read_calls)
                .context("signature physical read-call counter decreased")?,
            physical_read_bytes: self
                .physical_read_bytes
                .checked_sub(earlier.physical_read_bytes)
                .context("signature physical read-byte counter decreased")?,
        })
    }
}

#[derive(Debug, Default)]
struct SignatureBatch {
    ranges: Vec<SignatureRange>,
    bytes: Vec<u8>,
    byte_len: usize,
    expectations: Vec<SignatureExpectation>,
    stats: SignatureReadStats,
}

impl SignatureBatch {
    fn stats(&self) -> SignatureReadStats {
        self.stats
    }

    fn should_flush(&self, signature_count: u8) -> Result<bool> {
        let added = usize::from(signature_count)
            .checked_mul(SIGNATURE_BYTES)
            .context("signature batch range overflow")?;
        Ok(!self.ranges.is_empty()
            && (self.ranges.len() == SIGNATURE_BATCH_RANGES
                || self
                    .byte_len
                    .checked_add(added)
                    .is_none_or(|sum| sum > SIGNATURE_BATCH_BYTES)))
    }

    fn push(
        &mut self,
        source_first_ordinal: u64,
        signature_count: u8,
        expected_first: Option<[u8; SIGNATURE_BYTES]>,
    ) -> Result<()> {
        ensure!(
            signature_count != 0,
            "cannot queue an empty signature range"
        );
        let source_offset = source_first_ordinal
            .checked_mul(SIGNATURE_BYTES as u64)
            .context("source signature byte offset overflow")?;
        let bytes = usize::from(signature_count)
            .checked_mul(SIGNATURE_BYTES)
            .context("signature range length overflow")?;
        let source_bytes = u64::try_from(bytes).context("signature range length exceeds u64")?;
        source_offset
            .checked_add(source_bytes)
            .context("source signature byte range overflow")?;
        let destination_start = self.byte_len;
        let destination_end = destination_start
            .checked_add(bytes)
            .context("signature batch length overflow")?;
        ensure!(
            destination_end <= SIGNATURE_BATCH_BYTES,
            "one signature batch exceeds its byte limit"
        );
        let next_input_ranges = self
            .stats
            .input_ranges
            .checked_add(1)
            .context("signature input-range count overflow")?;
        let next_input_bytes = self
            .stats
            .input_bytes
            .checked_add(u64::try_from(bytes).context("signature input length exceeds u64")?)
            .context("signature input-byte count overflow")?;
        if self.bytes.len() < destination_end {
            self.bytes.resize(destination_end, 0);
        }
        let mut merged = false;
        if let Some(previous) = self.ranges.last_mut() {
            let previous_source_end = previous
                .source_offset
                .checked_add(
                    u64::try_from(previous.bytes)
                        .context("previous signature range length exceeds u64")?,
                )
                .context("previous source signature byte range overflow")?;
            let previous_destination_end =
                previous
                    .destination_start
                    .checked_add(previous.bytes)
                    .context("previous signature destination range overflow")?;
            ensure!(
                previous_destination_end == destination_start,
                "signature batch destinations are not contiguous"
            );
            if previous_source_end == source_offset {
                previous.bytes = previous
                    .bytes
                    .checked_add(bytes)
                    .context("merged signature range length overflow")?;
                merged = true;
            }
        }
        if !merged {
            self.ranges.push(SignatureRange {
                source_offset,
                destination_start,
                bytes,
            });
        }
        if let Some(expected) = expected_first {
            self.expectations.push(SignatureExpectation {
                destination_start,
                expected,
            });
        }
        self.stats.input_ranges = next_input_ranges;
        self.stats.input_bytes = next_input_bytes;
        self.byte_len = destination_end;
        Ok(())
    }

    fn flush(&mut self, source: &File, output: &mut DigestWriter) -> Result<()> {
        if self.ranges.is_empty() {
            return Ok(());
        }
        let available = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        let workers = available.min(SIGNATURE_READ_WORKERS).min(self.ranges.len());
        let physical_read_calls = if workers == 1 {
            let mut read_calls = 0u64;
            let mut destination_base = 0usize;
            for range in &self.ranges {
                ensure!(
                    range.destination_start == destination_base,
                    "signature batch destinations are not contiguous"
                );
                let destination_end = range
                    .destination_start
                    .checked_add(range.bytes)
                    .context("signature batch destination overflow")?;
                ensure!(
                    destination_end <= self.byte_len,
                    "signature range exceeds its logical batch bytes"
                );
                read_calls = read_calls
                    .checked_add(read_exact_at(
                        source,
                        &mut self.bytes[range.destination_start..destination_end],
                        range.source_offset,
                    )?)
                    .context("signature physical read-call count overflow")?;
                destination_base = destination_end;
            }
            ensure!(
                destination_base == self.byte_len,
                "signature batch has unassigned destination bytes"
            );
            read_calls
        } else {
            let ranges_per_worker = self.ranges.len().div_ceil(workers);
            std::thread::scope(|scope| -> Result<u64> {
                let mut remaining = &mut self.bytes[..self.byte_len];
                let mut destination_base = 0usize;
                let mut handles = Vec::with_capacity(workers);
                for ranges in self.ranges.chunks(ranges_per_worker) {
                    let first = ranges.first().expect("nonempty signature range chunk");
                    ensure!(
                        first.destination_start == destination_base,
                        "signature batch destinations are not contiguous"
                    );
                    let last = ranges.last().expect("nonempty signature range chunk");
                    let destination_end = last
                        .destination_start
                        .checked_add(last.bytes)
                        .context("signature batch destination overflow")?;
                    let chunk_bytes = destination_end - destination_base;
                    let (target, tail) = remaining.split_at_mut(chunk_bytes);
                    remaining = tail;
                    let base = destination_base;
                    handles.push(scope.spawn(move || -> std::io::Result<u64> {
                        let mut read_calls = 0u64;
                        for range in ranges {
                            let start = range.destination_start - base;
                            read_calls = read_calls
                                .checked_add(read_exact_at(
                                    source,
                                    &mut target[start..start + range.bytes],
                                    range.source_offset,
                                )?)
                                .ok_or_else(|| {
                                    std::io::Error::other(
                                        "signature physical read-call count overflow",
                                    )
                                })?;
                        }
                        Ok(read_calls)
                    }));
                    destination_base = destination_end;
                }
                ensure!(
                    remaining.is_empty(),
                    "signature batch has unassigned destination bytes"
                );
                let mut read_calls = 0u64;
                for handle in handles {
                    read_calls = read_calls
                        .checked_add(
                            handle
                                .join()
                                .map_err(|_| anyhow::anyhow!("signature read worker panicked"))??,
                        )
                        .context("signature physical read-call count overflow")?;
                }
                Ok(read_calls)
            })?
        };
        for expectation in &self.expectations {
            ensure!(
                self.bytes[expectation.destination_start
                    ..expectation.destination_start + SIGNATURE_BYTES]
                    == expectation.expected,
                "mint anchor first signature differs from the frozen target"
            );
        }
        let mut next_stats = self.stats;
        next_stats.physical_read_ranges = next_stats
            .physical_read_ranges
            .checked_add(u64::try_from(self.ranges.len())?)
            .context("signature physical read-range count overflow")?;
        next_stats.physical_read_calls = next_stats
            .physical_read_calls
            .checked_add(physical_read_calls)
            .context("signature physical read-call count overflow")?;
        next_stats.physical_read_bytes = next_stats
            .physical_read_bytes
            .checked_add(u64::try_from(self.byte_len)?)
            .context("signature physical read-byte count overflow")?;
        output.write_all(&self.bytes[..self.byte_len])?;
        self.stats = next_stats;
        self.ranges.clear();
        self.byte_len = 0;
        self.expectations.clear();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PassTwoArenaSpan {
    offset: usize,
    bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PassTwoReadRange {
    source_offset: u64,
    arena_offset: usize,
    bytes: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PassTwoReadStats {
    locator_payload_bytes: u64,
    physical_read_ranges: u64,
    physical_read_calls: u64,
    physical_read_bytes: u64,
    arena_high_water_bytes: u64,
}

#[derive(Default)]
struct PassTwoReadBatch {
    locators: Vec<TransactionLocator>,
    physical_order: Vec<usize>,
    canonical_spans: Vec<PassTwoArenaSpan>,
    ranges: Vec<PassTwoReadRange>,
    arena: Vec<u8>,
    arena_bytes: usize,
    pending: Option<TransactionLocator>,
    payload_bytes: usize,
    extra_gap_bytes: usize,
    direct_singleton: bool,
}

impl PassTwoReadBatch {
    fn reset_epoch(&mut self) {
        self.locators.clear();
        self.physical_order.clear();
        self.canonical_spans.clear();
        self.ranges.clear();
        self.arena_bytes = 0;
        self.pending = None;
        self.payload_bytes = 0;
        self.extra_gap_bytes = 0;
        self.direct_singleton = false;
    }

    fn fill(
        &mut self,
        reader: &mut LocatorFileReader,
        epoch: u64,
        stream_bytes: u64,
        previous_key: &mut Option<(u64, u32, u32)>,
    ) -> Result<bool> {
        self.locators.clear();
        self.payload_bytes = 0;
        loop {
            let locator = match self.pending.take() {
                Some(locator) => locator,
                None => match reader.next()? {
                    Some(locator) => locator,
                    None => break,
                },
            };
            ensure!(
                previous_key.is_none_or(|key| key < locator.key()),
                "epoch {epoch} canonical locators are not strictly sorted"
            );
            let payload_len = usize::try_from(locator.payload_len)
                .context("transaction locator payload length exceeds usize")?;
            ensure!(
                payload_len <= WINCODE_LEB128_MAX_FRAME_BYTES,
                "epoch {epoch} locator frame exceeds the Wincode limit"
            );
            let payload_end = locator
                .payload_offset
                .checked_add(u64::from(locator.payload_len))
                .context("raw transaction payload range overflow")?;
            ensure!(
                payload_end <= stream_bytes,
                "epoch {epoch} locator points outside the pinned raw stream"
            );
            let next_payload_bytes = self
                .payload_bytes
                .checked_add(payload_len)
                .context("Phase 2 transaction batch payload length overflow")?;
            let needs_direct_singleton = payload_len > PASS_TWO_READ_MAX_RANGE_BYTES;
            if !self.locators.is_empty()
                && (needs_direct_singleton
                    || self.locators.len() == PASS_TWO_READ_BATCH_LOCATORS
                    || next_payload_bytes > PASS_TWO_READ_BATCH_PAYLOAD_BYTES)
            {
                self.pending = Some(locator);
                break;
            }
            self.locators.push(locator);
            self.payload_bytes = next_payload_bytes;
            *previous_key = Some(locator.key());
            if needs_direct_singleton
                || self.locators.len() == PASS_TWO_READ_BATCH_LOCATORS
                || self.payload_bytes == PASS_TWO_READ_BATCH_PAYLOAD_BYTES
            {
                break;
            }
        }
        if self.locators.is_empty() {
            return Ok(false);
        }
        self.build_plan()?;
        Ok(true)
    }

    fn build_plan(&mut self) -> Result<()> {
        self.physical_order.clear();
        self.canonical_spans.clear();
        self.ranges.clear();
        self.arena_bytes = 0;
        self.extra_gap_bytes = 0;
        self.direct_singleton = false;
        ensure!(
            self.locators.len() <= PASS_TWO_READ_BATCH_LOCATORS,
            "Phase 2 read batch exceeds its locator bound"
        );
        let planned_payload_bytes =
            self.locators
                .iter()
                .try_fold(0usize, |total, locator| -> Result<usize> {
                    total
                        .checked_add(usize::try_from(locator.payload_len)?)
                        .context("Phase 2 planned payload byte count overflow")
                })?;
        ensure!(
            planned_payload_bytes == self.payload_bytes,
            "Phase 2 read batch payload total differs from its locators"
        );

        self.physical_order
            .try_reserve(self.locators.len())
            .context("reserve Phase 2 physical read order")?;
        self.physical_order.extend(0..self.locators.len());
        self.physical_order.sort_unstable_by(|left, right| {
            let left_index = *left;
            let right_index = *right;
            let left = self.locators[left_index];
            let right = self.locators[right_index];
            (left.payload_offset, left.payload_len, left_index).cmp(&(
                right.payload_offset,
                right.payload_len,
                right_index,
            ))
        });
        self.canonical_spans
            .try_reserve(self.locators.len())
            .context("reserve Phase 2 canonical arena spans")?;
        self.canonical_spans
            .resize(self.locators.len(), PassTwoArenaSpan::default());
        self.ranges
            .try_reserve(self.locators.len())
            .context("reserve Phase 2 coalesced read ranges")?;

        let mut previous_end = None;
        for &index in &self.physical_order {
            let locator = self.locators[index];
            let end = locator
                .payload_offset
                .checked_add(u64::from(locator.payload_len))
                .context("Phase 2 physical read range overflow")?;
            ensure!(
                previous_end.is_none_or(|previous| previous <= locator.payload_offset),
                "Phase 2 transaction payload ranges overlap"
            );
            previous_end = Some(end);
        }

        if self.locators.len() == 1
            && usize::try_from(self.locators[0].payload_len)? > PASS_TWO_READ_MAX_RANGE_BYTES
        {
            let bytes = usize::try_from(self.locators[0].payload_len)?;
            ensure!(
                bytes <= WINCODE_LEB128_MAX_FRAME_BYTES,
                "Phase 2 direct singleton exceeds the Wincode limit"
            );
            self.direct_singleton = true;
            self.canonical_spans[0] = PassTwoArenaSpan { offset: 0, bytes };
            self.ranges.push(PassTwoReadRange {
                source_offset: self.locators[0].payload_offset,
                arena_offset: 0,
                bytes,
            });
            self.grow_arena(bytes, "reserve Phase 2 direct singleton arena")?;
            return Ok(());
        }
        ensure!(
            self.payload_bytes <= PASS_TWO_READ_BATCH_PAYLOAD_BYTES,
            "Phase 2 normal read batch exceeds its payload bound"
        );

        let &first_index = self
            .physical_order
            .first()
            .context("Phase 2 read plan has no locator")?;
        let first = self.locators[first_index];
        let first_bytes = usize::try_from(first.payload_len)?;
        ensure!(
            first_bytes <= PASS_TWO_READ_MAX_RANGE_BYTES,
            "large Phase 2 payload was not isolated as a direct singleton"
        );
        let mut group_source_start = first.payload_offset;
        let mut group_source_end = first
            .payload_offset
            .checked_add(u64::from(first.payload_len))
            .context("Phase 2 coalesced read range overflow")?;
        let mut group_arena_offset = 0usize;
        self.canonical_spans[first_index] = PassTwoArenaSpan {
            offset: group_arena_offset,
            bytes: first_bytes,
        };

        for &index in &self.physical_order[1..] {
            let locator = self.locators[index];
            let bytes = usize::try_from(locator.payload_len)?;
            ensure!(
                bytes <= PASS_TWO_READ_MAX_RANGE_BYTES,
                "large Phase 2 payload was not isolated as a direct singleton"
            );
            let source_end = locator
                .payload_offset
                .checked_add(u64::from(locator.payload_len))
                .context("Phase 2 coalesced read range overflow")?;
            let gap = locator
                .payload_offset
                .checked_sub(group_source_end)
                .context("Phase 2 coalesced read ranges overlap")?;
            let candidate_range_bytes = usize::try_from(
                source_end
                    .checked_sub(group_source_start)
                    .context("Phase 2 coalesced read range underflow")?,
            )
            .context("Phase 2 coalesced read range exceeds usize")?;
            let gap_bytes = usize::try_from(gap).context("Phase 2 read gap exceeds usize")?;
            let candidate_extra_gap_bytes = self
                .extra_gap_bytes
                .checked_add(gap_bytes)
                .context("Phase 2 extra-gap byte count overflow")?;
            if gap <= PASS_TWO_READ_MAX_GAP_BYTES
                && candidate_range_bytes <= PASS_TWO_READ_MAX_RANGE_BYTES
                && candidate_extra_gap_bytes <= PASS_TWO_READ_MAX_EXTRA_GAP_BYTES
            {
                let relative_offset = usize::try_from(
                    locator
                        .payload_offset
                        .checked_sub(group_source_start)
                        .context("Phase 2 canonical arena offset underflow")?,
                )
                .context("Phase 2 canonical arena offset exceeds usize")?;
                self.canonical_spans[index] = PassTwoArenaSpan {
                    offset: group_arena_offset
                        .checked_add(relative_offset)
                        .context("Phase 2 canonical arena offset overflow")?,
                    bytes,
                };
                group_source_end = source_end;
                self.extra_gap_bytes = candidate_extra_gap_bytes;
                continue;
            }

            let range_bytes = usize::try_from(
                group_source_end
                    .checked_sub(group_source_start)
                    .context("Phase 2 coalesced read range underflow")?,
            )?;
            self.ranges.push(PassTwoReadRange {
                source_offset: group_source_start,
                arena_offset: group_arena_offset,
                bytes: range_bytes,
            });
            let next_arena_offset = group_arena_offset
                .checked_add(range_bytes)
                .context("Phase 2 read arena length overflow")?;
            group_source_start = locator.payload_offset;
            group_source_end = source_end;
            group_arena_offset = next_arena_offset;
            self.canonical_spans[index] = PassTwoArenaSpan {
                offset: group_arena_offset,
                bytes,
            };
        }

        let final_range_bytes = usize::try_from(
            group_source_end
                .checked_sub(group_source_start)
                .context("Phase 2 final coalesced read range underflow")?,
        )?;
        self.ranges.push(PassTwoReadRange {
            source_offset: group_source_start,
            arena_offset: group_arena_offset,
            bytes: final_range_bytes,
        });
        let arena_bytes = group_arena_offset
            .checked_add(final_range_bytes)
            .context("Phase 2 read arena length overflow")?;
        ensure!(
            arena_bytes
                == self
                    .payload_bytes
                    .checked_add(self.extra_gap_bytes)
                    .context("Phase 2 bounded read arena length overflow")?,
            "Phase 2 read arena length differs from payload and gap bytes"
        );
        ensure!(
            self.extra_gap_bytes <= PASS_TWO_READ_MAX_EXTRA_GAP_BYTES
                && arena_bytes
                    <= PASS_TWO_READ_BATCH_PAYLOAD_BYTES
                        .checked_add(PASS_TWO_READ_MAX_EXTRA_GAP_BYTES)
                        .context("Phase 2 normal arena bound overflow")?,
            "Phase 2 normal read arena exceeds its memory bounds"
        );
        self.grow_arena(arena_bytes, "reserve Phase 2 coalesced read arena")?;
        Ok(())
    }

    fn grow_arena(&mut self, bytes: usize, context: &'static str) -> Result<()> {
        if bytes > self.arena.len() {
            self.arena
                .try_reserve_exact(bytes - self.arena.len())
                .context(context)?;
            self.arena.resize(bytes, 0);
        }
        self.arena_bytes = bytes;
        Ok(())
    }

    fn read_exact(&mut self, file: &File, stats: &mut PassTwoReadStats) -> Result<()> {
        let mut physical_read_calls = 0u64;
        let mut physical_read_bytes = 0u64;
        for range in self.ranges.iter().copied() {
            let end = range
                .arena_offset
                .checked_add(range.bytes)
                .context("Phase 2 read arena range overflow")?;
            ensure!(
                end <= self.arena_bytes,
                "Phase 2 read range exceeds its logical arena length"
            );
            let target = self
                .arena
                .get_mut(range.arena_offset..end)
                .context("Phase 2 read range is outside its arena")?;
            let source_end = range
                .source_offset
                .checked_add(
                    u64::try_from(range.bytes).context("Phase 2 read range length exceeds u64")?,
                )
                .context("Phase 2 source read range overflow")?;
            let calls = read_exact_at(file, target, range.source_offset).with_context(|| {
                format!(
                    "read Phase 2 raw transaction bytes {}..{}",
                    range.source_offset, source_end
                )
            })?;
            physical_read_calls = physical_read_calls
                .checked_add(calls)
                .context("Phase 2 physical read-call count overflow")?;
            physical_read_bytes = physical_read_bytes
                .checked_add(
                    u64::try_from(range.bytes)
                        .context("Phase 2 physical read length exceeds u64")?,
                )
                .context("Phase 2 physical read-byte count overflow")?;
        }
        stats.locator_payload_bytes = stats
            .locator_payload_bytes
            .checked_add(
                u64::try_from(self.payload_bytes)
                    .context("Phase 2 locator payload length exceeds u64")?,
            )
            .context("Phase 2 locator payload-byte count overflow")?;
        stats.physical_read_ranges = stats
            .physical_read_ranges
            .checked_add(
                u64::try_from(self.ranges.len())
                    .context("Phase 2 physical read-range count exceeds u64")?,
            )
            .context("Phase 2 physical read-range count overflow")?;
        stats.physical_read_calls = stats
            .physical_read_calls
            .checked_add(physical_read_calls)
            .context("Phase 2 physical read-call count overflow")?;
        stats.physical_read_bytes = stats
            .physical_read_bytes
            .checked_add(physical_read_bytes)
            .context("Phase 2 physical read-byte count overflow")?;
        stats.arena_high_water_bytes = stats
            .arena_high_water_bytes
            .max(u64::try_from(self.arena.len()).context("Phase 2 arena length exceeds u64")?);
        Ok(())
    }

    fn payload(&self, canonical_index: usize) -> Result<&[u8]> {
        let locator = self
            .locators
            .get(canonical_index)
            .context("Phase 2 canonical locator index is outside its batch")?;
        let span = *self
            .canonical_spans
            .get(canonical_index)
            .context("Phase 2 canonical arena span is absent")?;
        ensure!(
            span.bytes == usize::try_from(locator.payload_len)?,
            "Phase 2 canonical arena span length differs from its locator"
        );
        let end = span
            .offset
            .checked_add(span.bytes)
            .context("Phase 2 canonical arena span overflow")?;
        ensure!(
            end <= self.arena_bytes,
            "Phase 2 canonical span exceeds its logical arena length"
        );
        self.arena
            .get(span.offset..end)
            .context("Phase 2 canonical arena span is outside its bytes")
    }
}

#[derive(Default)]
struct PassTwoScratch {
    read_batch: PassTwoReadBatch,
    message: Vec<u8>,
    metadata: Vec<u8>,
    metadata_comparison: Vec<u8>,
    frame: Vec<u8>,
    dense_source_ids: Vec<u32>,
    dense_touched: Vec<u32>,
    source_id_map_buffer: Vec<u8>,
    signatures: SignatureBatch,
}

fn reset_dense_source_ids(
    dense_len: usize,
    dense: &mut Vec<u32>,
    touched: &mut Vec<u32>,
) -> Result<()> {
    for &raw_index in touched.iter() {
        let index = usize::try_from(raw_index)?;
        let slot = dense
            .get_mut(index)
            .context("touched dense source-ID index is outside its table")?;
        ensure!(
            *slot != 0,
            "touched dense source-ID entry contains the zero sentinel"
        );
        *slot = 0;
    }
    touched.clear();

    if dense.len() > dense_len {
        dense.truncate(dense_len);
    } else if dense.len() < dense_len {
        dense
            .try_reserve_exact(dense_len - dense.len())
            .context("grow dense epoch source-ID translation table")?;
        dense.resize(dense_len, 0);
    }
    Ok(())
}

fn load_dense_source_id_map(
    plan: &EpochPlan,
    global: &GlobalRegistryLookup,
    dense: &mut Vec<u32>,
    touched: &mut Vec<u32>,
    read_buffer: &mut Vec<u8>,
) -> Result<()> {
    load_dense_source_id_map_with_chunk_limit(
        plan,
        global,
        dense,
        touched,
        read_buffer,
        SOURCE_ID_MAP_READ_BYTES,
    )
}

fn load_dense_source_id_map_with_chunk_limit(
    plan: &EpochPlan,
    global: &GlobalRegistryLookup,
    dense: &mut Vec<u32>,
    touched: &mut Vec<u32>,
    read_buffer: &mut Vec<u8>,
    chunk_limit: usize,
) -> Result<()> {
    ensure!(
        chunk_limit != 0 && chunk_limit.is_multiple_of(SOURCE_ID_MAP_ROW_BYTES),
        "source ID map read limit must contain whole rows"
    );
    ensure!(
        plan.source_id_map.bytes
            == plan
                .source_id_map
                .rows
                .checked_mul(SOURCE_ID_MAP_ROW_BYTES as u64)
                .context("source ID map byte length overflow")?,
        "epoch {} source ID map binding size differs from its row count",
        plan.epoch
    );
    plan.source_id_map.stamp.verify(
        &plan.source_id_map_file,
        &format!("epoch {} pinned source ID map", plan.epoch),
    )?;
    ensure!(
        plan.source_id_map.stamp.bytes == plan.source_id_map.bytes,
        "epoch {} source ID map size differs from its Pass 1 binding",
        plan.epoch
    );
    let dense_len = usize::try_from(plan.registry_entries)?;
    reset_dense_source_ids(dense_len, dense, touched)?;
    ensure!(
        plan.source_id_map.rows <= u64::from(plan.registry_entries),
        "epoch {} source ID map row count exceeds its source registry",
        plan.epoch
    );
    let map_rows = usize::try_from(plan.source_id_map.rows)
        .context("source ID map row count exceeds addressable memory")?;
    touched
        .try_reserve_exact(map_rows)
        .context("reserve touched dense source-ID entries")?;

    let chunk_limit_u64 = u64::try_from(chunk_limit)?;
    let max_read_bytes = usize::try_from(plan.source_id_map.bytes.min(chunk_limit_u64))?;
    if read_buffer.len() < max_read_bytes {
        read_buffer
            .try_reserve_exact(max_read_bytes - read_buffer.len())
            .context("grow reusable source ID map read buffer")?;
        read_buffer.resize(max_read_bytes, 0);
    }

    let mut previous_id = None;
    let mut offset = 0u64;
    let mut parsed_rows = 0u64;
    while offset < plan.source_id_map.bytes {
        let remaining = plan.source_id_map.bytes - offset;
        let chunk_bytes = usize::try_from(remaining.min(chunk_limit_u64))?;
        let chunk_bytes_u64 = u64::try_from(chunk_bytes)?;
        ensure!(
            chunk_bytes.is_multiple_of(SOURCE_ID_MAP_ROW_BYTES),
            "epoch {} source ID map read chunk is not row-aligned",
            plan.epoch
        );
        read_exact_at(
            &plan.source_id_map_file,
            &mut read_buffer[..chunk_bytes],
            offset,
        )
        .with_context(|| {
            format!(
                "read epoch {} source ID map bytes {}..{}",
                plan.epoch,
                offset,
                offset + chunk_bytes_u64
            )
        })?;
        let rows = read_buffer[..chunk_bytes].chunks_exact(SOURCE_ID_MAP_ROW_BYTES);
        ensure!(
            rows.remainder().is_empty(),
            "epoch {} source ID map read chunk has a partial row",
            plan.epoch
        );
        for row in rows {
            let id = u32::from_le_bytes(row[..4].try_into().unwrap());
            ensure!(
                id != 0 && id <= plan.registry_entries,
                "epoch {} source ID map contains ID {id} outside 1..={}",
                plan.epoch,
                plan.registry_entries
            );
            ensure!(
                previous_id.is_none_or(|previous| previous < id),
                "epoch {} source ID map is not strictly sorted and unique",
                plan.epoch
            );
            let raw: [u8; KEY_BYTES] = row[4..].try_into().unwrap();
            let global_id = global.lookup(&raw)?;
            ensure!(global_id != 0, "global registry returned the zero sentinel");
            let index = usize::try_from(id - 1)?;
            let slot = dense
                .get_mut(index)
                .context("source ID map points outside its dense table")?;
            ensure!(
                *slot == 0,
                "epoch {} dense source-ID entry {id} was not cleared",
                plan.epoch
            );
            *slot = global_id;
            touched.push(id - 1);
            previous_id = Some(id);
            parsed_rows = parsed_rows
                .checked_add(1)
                .context("parsed source ID map row count overflow")?;
        }
        offset = offset
            .checked_add(chunk_bytes_u64)
            .context("source ID map read offset overflow")?;
    }
    ensure!(
        parsed_rows == plan.source_id_map.rows,
        "epoch {} parsed source ID map row count differs from its binding",
        plan.epoch,
    );
    plan.source_id_map.stamp.verify(
        &plan.source_id_map_file,
        &format!("epoch {} source ID map", plan.epoch),
    )?;
    verify_path_binding(
        &plan.source_id_map_path,
        &plan.source_id_map.stamp,
        &format!("epoch {} source ID map", plan.epoch),
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_epoch_pass_two(
    archive_root: &Path,
    root: &InputRoot,
    plan: &EpochPlan,
    global: &GlobalRegistryLookup,
    transaction_writer: &mut DigestWriter,
    signature_writer: &mut DigestWriter,
    scratch: &mut PassTwoScratch,
    total_transactions: &mut u64,
    total_signatures: &mut u64,
) -> Result<PassTwoReadStats> {
    plan.stream_stamp.verify(
        &plan.stream_file,
        &format!("epoch {} pinned raw transaction stream", plan.epoch),
    )?;
    let source =
        PinnedLocalRangeSource::open_directory(archive_root.join(format!("epoch-{}", plan.epoch)))?;
    let registry_file = source.open_file(REGISTRY_FILE)?;
    let registry_stamp = FileStamp::read(&registry_file)?;
    ensure!(
        registry_stamp.same_content_identity(&plan.registry_stamp),
        "epoch {} source registry identity changed between passes",
        plan.epoch
    );
    ensure!(
        registry_stamp.bytes
            == u64::from(plan.registry_entries)
                .checked_mul(KEY_BYTES as u64)
                .context("source registry byte length overflow")?,
        "epoch {} source registry size differs from its entry count",
        plan.epoch
    );
    let signatures_file = source.open_file(SIGNATURES_FILE)?;
    let signatures_stamp = FileStamp::read(&signatures_file)?;
    ensure!(
        signatures_stamp.same_content_identity(&plan.signatures_stamp),
        "epoch {} source signatures identity changed between passes",
        plan.epoch
    );
    ensure!(
        signatures_stamp
            .bytes
            .is_multiple_of(SIGNATURE_BYTES as u64),
        "epoch {} signatures sidecar has a partial row",
        plan.epoch
    );
    let source_signature_count = signatures_stamp.bytes / SIGNATURE_BYTES as u64;

    // Pass 2 uses only the authenticated Pass 1 work map for key resolution.
    // The source registry is open only so its file identity can be checked.
    load_dense_source_id_map(
        plan,
        global,
        &mut scratch.dense_source_ids,
        &mut scratch.dense_touched,
        &mut scratch.source_id_map_buffer,
    )?;

    let locator_bytes = fs::metadata(&plan.locator_path)
        .with_context(|| format!("inspect {}", plan.locator_path.display()))?
        .len();
    ensure!(
        locator_bytes
            == plan
                .transaction_count
                .checked_mul(LOCATOR_BYTES as u64)
                .context("epoch locator size overflow")?,
        "epoch {} canonical locator file size differs",
        plan.epoch
    );
    let mut locators = LocatorFileReader::open(&plan.locator_path)?;
    let mut epoch_transactions = 0u64;
    let mut epoch_signatures = 0u64;
    let mut anchor_count = 0u64;
    let mut previous_key = None;
    let mut read_stats = PassTwoReadStats::default();
    scratch.read_batch.reset_epoch();
    while scratch.read_batch.fill(
        &mut locators,
        plan.epoch,
        plan.stream_stamp.bytes,
        &mut previous_key,
    )? {
        scratch
            .read_batch
            .read_exact(&plan.stream_file, &mut read_stats)?;
        for canonical_index in 0..scratch.read_batch.locators.len() {
            let locator = scratch.read_batch.locators[canonical_index];
            let payload = scratch.read_batch.payload(canonical_index)?;
            let BorrowedDumpRecord::Transaction(record) = decode_borrowed_frame(payload)? else {
                bail!(
                    "epoch {} locator does not point to a transaction",
                    plan.epoch
                )
            };
            validate_record_basic(
                plan.epoch,
                plan.slots_per_epoch,
                plan.source_generation_digest,
                plan.source_wire_profile,
                (&record).into(),
            )?;
            ensure!(
                (record.block.slot, record.source_block_id, record.tx_index) == locator.key(),
                "epoch {} locator coordinate differs from its raw record",
                plan.epoch
            );
            let signature_end = record
                .source_first_signature_ordinal
                .checked_add(u64::from(record.signature_count))
                .context("source signature range overflow")?;
            ensure!(
                signature_end <= source_signature_count,
                "epoch {} signature range exceeds the pinned source sidecar",
                plan.epoch
            );

            rewrite_transaction_pubkeys(
                (&record).into(),
                global,
                &scratch.dense_source_ids,
                &mut scratch.message,
                &mut scratch.metadata,
                &mut scratch.metadata_comparison,
            )
            .with_context(|| {
                format!(
                    "rewrite epoch {} slot {} transaction {}",
                    plan.epoch, record.block.slot, record.tx_index
                )
            })?;

            if scratch.signatures.should_flush(record.signature_count)? {
                scratch
                    .signatures
                    .flush(&signatures_file, signature_writer)?;
            }
            let is_anchor = (
                record.source_epoch,
                record.block.slot,
                record.source_block_id,
                record.tx_index,
            ) == (
                root.frozen.anchor_position.epoch,
                root.frozen.anchor_position.slot,
                root.frozen.anchor_position.source_block_id,
                root.frozen.anchor_position.tx_index,
            );
            if is_anchor {
                ensure!(
                    record.source_first_signature_ordinal
                        == root.frozen.anchor_position.source_first_signature_ordinal
                        && record.signature_count == root.frozen.anchor_position.signature_count,
                    "epoch {} anchor signature range differs in Pass 2",
                    plan.epoch
                );
            }
            scratch.signatures.push(
                record.source_first_signature_ordinal,
                record.signature_count,
                is_anchor.then_some(root.target.mint_signature),
            )?;

            let output_signature_count = record.signature_count;
            let output_record = BorrowedDumpRecord::Transaction(BorrowedTransactionRecord {
                source_epoch: record.source_epoch,
                source_generation_digest: record.source_generation_digest,
                source_wire_profile: record.source_wire_profile,
                source_block_id: record.source_block_id,
                block: record.block,
                tx_index: record.tx_index,
                flags: record.flags,
                source_first_signature_ordinal: record.source_first_signature_ordinal,
                signature_count: record.signature_count,
                dump_signature_ordinal: Some(*total_signatures),
                message_bytes: &scratch.message,
                metadata_bytes: &scratch.metadata,
            });
            write_frame(transaction_writer, &output_record, &mut scratch.frame)?;

            epoch_transactions = epoch_transactions
                .checked_add(1)
                .context("epoch transaction count overflow")?;
            *total_transactions = total_transactions
                .checked_add(1)
                .context("consolidated transaction count overflow")?;
            let count = u64::from(output_signature_count);
            epoch_signatures = epoch_signatures
                .checked_add(count)
                .context("epoch signature count overflow")?;
            *total_signatures = total_signatures
                .checked_add(count)
                .context("consolidated signature count overflow")?;
            anchor_count = anchor_count
                .checked_add(u64::from(is_anchor))
                .context("anchor count overflow")?;
        }
    }
    scratch
        .signatures
        .flush(&signatures_file, signature_writer)?;
    ensure!(
        epoch_transactions == plan.transaction_count && epoch_signatures == plan.signature_count,
        "epoch {} Pass 2 counts differ from Pass 1",
        plan.epoch
    );
    ensure!(
        anchor_count == u64::from(plan.epoch == root.frozen.anchor_position.epoch),
        "epoch {} Pass 2 anchor occurrence count differs",
        plan.epoch
    );
    plan.stream_stamp.verify(
        &plan.stream_file,
        &format!("epoch {} pinned raw transaction stream", plan.epoch),
    )?;
    registry_stamp.verify(
        &registry_file,
        &format!("epoch {} source registry", plan.epoch),
    )?;
    signatures_stamp.verify(
        &signatures_file,
        &format!("epoch {} source signatures", plan.epoch),
    )?;
    source.verify_unchanged()?;
    Ok(read_stats)
}

fn partial_path(output: &Path, name: &str) -> PathBuf {
    output.join(format!(".{name}.partial"))
}

fn write_resume_checkpoint(path: &Path, checkpoint: &ConsolidationResumeCheckpoint) -> Result<()> {
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(checkpoint)?;
    let mut writer = BufWriter::new(create_new_file(&temporary)?);
    writer.write_all(&bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&temporary, path)
        .with_context(|| format!("publish resume checkpoint {}", path.display()))?;
    sync_directory(path.parent().context("resume checkpoint has no parent")?)
}

fn read_resume_checkpoint(path: &Path) -> Result<ConsolidationResumeCheckpoint> {
    serde_json::from_slice(&read_bounded_regular(path, MAX_ROOT_MANIFEST_BYTES)?)
        .with_context(|| format!("decode resume checkpoint {}", path.display()))
}

fn restore_epoch_plan(
    checkpoint: &EpochPlanCheckpoint,
    shard_root: &Path,
    work: &Path,
) -> Result<EpochPlan> {
    let stream_path = shard_root
        .join(format!("epoch-{}", checkpoint.epoch))
        .join(TRANSACTIONS_FILE);
    let stream_file = File::open(&stream_path)?;
    ensure!(
        FileStamp::read(&stream_file)? == checkpoint.stream_stamp,
        "epoch {} raw stream differs from its resume plan",
        checkpoint.epoch
    );
    let locator_path = work.join(format!("epoch-{}-canonical.locators", checkpoint.epoch));
    ensure!(
        fs::metadata(&locator_path)?.len()
            == checkpoint
                .transaction_count
                .checked_mul(LOCATOR_BYTES as u64)
                .context("resume locator size overflow")?,
        "epoch {} locator differs from its resume plan",
        checkpoint.epoch
    );
    let source_id_map_path = work.join(format!("epoch-{}-source-pubkeys.map", checkpoint.epoch));
    let source_id_map_file = File::open(&source_id_map_path)?;
    checkpoint.source_id_map.stamp.verify(
        &source_id_map_file,
        &format!("epoch {} resume source ID map", checkpoint.epoch),
    )?;
    Ok(EpochPlan {
        epoch: checkpoint.epoch,
        stream_file,
        stream_stamp: checkpoint.stream_stamp.clone(),
        locator_path,
        source_id_map_path,
        source_id_map_file,
        source_id_map: checkpoint.source_id_map.clone(),
        source_generation_digest: checkpoint.source_generation_digest,
        source_wire_profile: checkpoint.source_wire_profile,
        registry_entries: checkpoint.registry_entries,
        registry_stamp: checkpoint.registry_stamp.clone(),
        signatures_stamp: checkpoint.signatures_stamp.clone(),
        slots_per_epoch: checkpoint.slots_per_epoch,
        transaction_count: checkpoint.transaction_count,
        signature_count: checkpoint.signature_count,
    })
}

fn eta_seconds(started: Instant, done: u64, total: u64) -> f64 {
    if done == 0 || done >= total {
        return 0.0;
    }
    started.elapsed().as_secs_f64() * (total - done) as f64 / done as f64
}

fn prepare_output_path(input: &Path, output: &Path, resume: bool) -> Result<PathBuf> {
    ensure!(
        output.file_name().is_some(),
        "consolidated output must name one new directory"
    );
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent)
        .with_context(|| format!("resolve output parent {}", parent.display()))?;
    let output = parent.join(output.file_name().unwrap());
    ensure!(
        !output.starts_with(input),
        "consolidated output must not be inside the raw extraction"
    );
    if output.exists() {
        ensure!(resume, "output {} already exists", output.display());
        ensure!(output.is_dir(), "resume output is not a directory");
        ensure!(
            !output.join(DUMP_MANIFEST_FILE).exists(),
            "completed consolidated output cannot be resumed"
        );
        return Ok(output);
    }
    fs::create_dir(&output).with_context(|| format!("create {}", output.display()))?;
    sync_directory(&parent)?;
    Ok(output)
}

/// Consolidate complete schema-3 raw SPYX epoch shards into one portable dump.
///
/// The function never hashes an entire source registry or source signature
/// sidecar. It pins each source epoch while it resolves referenced keys or
/// copies selected signature ranges. It hashes each raw shard during Pass 1
/// and each final artifact during its only output write. The final manifest is
/// the last published path.
pub fn consolidate_epoch_shards_v3(
    archive_root: &Path,
    input: &Path,
    output: &Path,
    allow_metadata_generation_drift: bool,
    resume: bool,
) -> Result<()> {
    let consolidation_started = Instant::now();
    let archive_root = fs::canonicalize(archive_root)
        .with_context(|| format!("resolve archive root {}", archive_root.display()))?;
    let input = fs::canonicalize(input)
        .with_context(|| format!("resolve raw extraction {}", input.display()))?;
    ensure!(archive_root.is_dir(), "archive root is not a directory");
    ensure!(input.is_dir(), "raw extraction root is not a directory");
    let input_manifest_sha256 = sha256_bytes(&read_bounded_regular(
        &input.join(DUMP_MANIFEST_FILE),
        MAX_ROOT_MANIFEST_BYTES,
    )?);
    let root = read_input_root(&input)?;
    let epoch_count = root
        .manifest
        .last_epoch
        .checked_sub(root.manifest.first_epoch)
        .and_then(|span| span.checked_add(1))
        .context("root epoch count overflow")?;
    ensure!(
        epoch_count <= MAX_PINNED_EPOCHS,
        "the {epoch_count}-epoch range exceeds the {MAX_PINNED_EPOCHS}-epoch two-file pinning limit"
    );
    let shard_root = input.join(EPOCH_SHARDS_DIR);
    let preflight_started = Instant::now();
    for (index, epoch) in (root.manifest.first_epoch..=root.manifest.last_epoch).enumerate() {
        preflight_epoch_source(
            &archive_root,
            &root,
            epoch,
            &shard_root.join(format!("epoch-{epoch}")),
            allow_metadata_generation_drift,
        )?;
        let done = u64::try_from(index + 1).context("preflight epoch count exceeds u64")?;
        eprintln!(
            "consolidate preflight: {done}/{epoch_count} epochs, {} transactions declared, {:.1}s elapsed, {:.1}s ETA",
            root.manifest.transactions,
            preflight_started.elapsed().as_secs_f64(),
            eta_seconds(preflight_started, done, epoch_count),
        );
    }
    let output = prepare_output_path(&input, output, resume)?;
    let work = output.join(WORK_DIRECTORY);
    if resume {
        ensure!(work.is_dir(), "resume work directory is missing");
    } else {
        fs::create_dir(&work).with_context(|| format!("create {}", work.display()))?;
    }
    let key_work = work.join("keys");
    let checkpoint_path = work.join(RESUME_CHECKPOINT_FILE);
    let registry_partial = partial_path(&output, PUBKEY_REGISTRY_FILE);
    let (plans, aggregate, registry, mut resume_checkpoint) = if resume {
        let checkpoint = read_resume_checkpoint(&checkpoint_path)?;
        ensure!(
            checkpoint.schema_version == RESUME_CHECKPOINT_SCHEMA
                && checkpoint.input_manifest_sha256 == input_manifest_sha256
                && checkpoint.first_epoch == root.manifest.first_epoch
                && checkpoint.last_epoch == root.manifest.last_epoch
                && checkpoint.allow_metadata_generation_drift == allow_metadata_generation_drift,
            "resume checkpoint does not bind this consolidation request"
        );
        ensure!(
            checkpoint.plans.len() == usize::try_from(epoch_count)?
                && checkpoint.next_pass_two_plan <= checkpoint.plans.len(),
            "resume checkpoint has an invalid epoch plan count"
        );
        let mut plans = Vec::new();
        plans.try_reserve_exact(checkpoint.plans.len())?;
        for (index, saved) in checkpoint.plans.iter().enumerate() {
            ensure!(
                saved.epoch == root.manifest.first_epoch + u64::try_from(index)?,
                "resume checkpoint epoch order is invalid"
            );
            plans.push(restore_epoch_plan(saved, &shard_root, &work)?);
        }
        let expected_transactions = checkpoint.plans[..checkpoint.next_pass_two_plan]
            .iter()
            .try_fold(0u64, |sum, plan| {
                sum.checked_add(plan.transaction_count)
                    .context("resume transaction count overflow")
            })?;
        let expected_signatures = checkpoint.plans[..checkpoint.next_pass_two_plan]
            .iter()
            .try_fold(0u64, |sum, plan| {
                sum.checked_add(plan.signature_count)
                    .context("resume signature count overflow")
            })?;
        ensure!(
            checkpoint.transactions == expected_transactions
                && checkpoint.signatures == expected_signatures
                && checkpoint.signature_output_bytes
                    == expected_signatures
                        .checked_mul(SIGNATURE_BYTES as u64)
                        .context("resume signature size overflow")?,
            "resume checkpoint counters do not match its completed epoch prefix"
        );
        let registry = reopen_registry_build(
            &registry_partial,
            checkpoint.registry_rows,
            checkpoint.registry_sha256,
        )?;
        eprintln!(
            "consolidate resume: pass 1 and registry complete; continuing Pass 2 at {}/{} epochs",
            checkpoint.next_pass_two_plan,
            checkpoint.plans.len()
        );
        (plans, checkpoint.aggregate, registry, checkpoint)
    } else {
        let mut key_sorter = RawKeySorter::new(&key_work, KEY_SORT_MEMORY_BYTES)?;
        key_sorter.push(root.target.mint)?;
        for account in &root.frozen.accounts {
            key_sorter.push(account.raw_pubkey)?;
        }
        let mut aggregate = AggregateFooter::default();
        let plan_capacity = usize::try_from(epoch_count).context("epoch count exceeds usize")?;
        let mut plans = Vec::new();
        plans
            .try_reserve_exact(plan_capacity)
            .context("reserve epoch consolidation plans")?;
        let pass_one_started = Instant::now();
        for epoch in root.manifest.first_epoch..=root.manifest.last_epoch {
            let plan = scan_epoch_pass_one(
                &archive_root,
                &root,
                epoch,
                &shard_root.join(format!("epoch-{epoch}")),
                &work,
                &mut key_sorter,
                &mut aggregate,
                allow_metadata_generation_drift,
            )?;
            plans.push(plan);
            let done = u64::try_from(plans.len()).context("completed epoch count exceeds u64")?;
            eprintln!(
                "consolidate pass 1: {done}/{epoch_count} epochs, {} transactions, {:.1}s elapsed, {:.1}s ETA",
                aggregate.transactions_written,
                pass_one_started.elapsed().as_secs_f64(),
                eta_seconds(pass_one_started, done, epoch_count),
            );
        }
        ensure!(
            aggregate.epochs == epoch_count
                && aggregate.transactions_written == root.manifest.transactions,
            "raw root counters differ from its validated epoch shards"
        );
        eprintln!(
            "consolidate registry merge: start, {epoch_count}/{epoch_count} epochs, {} transactions, {:.1}s elapsed, 0.0s ETA",
            aggregate.transactions_written,
            consolidation_started.elapsed().as_secs_f64(),
        );
        let registry_started = Instant::now();
        let registry = key_sorter.finish(&registry_partial)?;
        eprintln!(
            "consolidate registry merge: complete, {} public keys, {} transactions, {:.1}s elapsed, 0.0s ETA",
            registry.rows,
            aggregate.transactions_written,
            registry_started.elapsed().as_secs_f64(),
        );
        let checkpoint = ConsolidationResumeCheckpoint {
            schema_version: RESUME_CHECKPOINT_SCHEMA,
            input_manifest_sha256,
            first_epoch: root.manifest.first_epoch,
            last_epoch: root.manifest.last_epoch,
            allow_metadata_generation_drift,
            aggregate,
            plans: plans.iter().map(EpochPlanCheckpoint::from).collect(),
            registry_rows: registry.rows,
            registry_sha256: registry.sha256,
            next_pass_two_plan: 0,
            transactions: 0,
            signatures: 0,
            transaction_output_bytes: 0,
            signature_output_bytes: 0,
        };
        (plans, aggregate, registry, checkpoint)
    };
    let expected_signatures = plans.iter().try_fold(0u64, |sum, plan| {
        sum.checked_add(plan.signature_count)
            .context("selected signature count overflow")
    })?;
    let registry_bytes = registry
        .rows
        .checked_mul(KEY_BYTES as u64)
        .context("global registry byte length overflow")?;
    ensure!(
        fs::metadata(&registry_partial)?.len() == registry_bytes,
        "global registry output size differs from its row count"
    );
    let global = GlobalRegistryLookup::new(&registry, &registry_partial)?;

    let transaction_partial = partial_path(&output, TRANSACTIONS_FILE);
    let signature_partial = partial_path(&output, DUMP_SIGNATURES_FILE);
    let account_partial = partial_path(&output, ACCOUNTS_FILE);
    let mut frame_scratch = Vec::new();
    let (mut transaction_writer, mut signature_writer) = if resume {
        (
            DigestWriter::resume(
                &transaction_partial,
                resume_checkpoint.transaction_output_bytes,
            )?,
            DigestWriter::resume(&signature_partial, resume_checkpoint.signature_output_bytes)?,
        )
    } else {
        let mut transaction_writer = DigestWriter::create(&transaction_partial)?;
        let signature_writer = DigestWriter::create(&signature_partial)?;
        write_frame(
            &mut transaction_writer,
            &TokenTransactionDumpRecord::Header(TokenTransactionDumpHeader {
                schema_version: DUMP_SCHEMA_VERSION,
                stream_kind: DumpStreamKind::Consolidated,
                mint: root.target.mint,
                mint_slot: root.target.mint_slot,
                mint_signature: root.target.mint_signature,
                source_epoch: None,
                source_generation_digest: None,
                source_wire_profile: None,
                pubkey_registry_id_base: PUBKEY_REGISTRY_ID_BASE,
            }),
            &mut frame_scratch,
        )?;
        resume_checkpoint.transaction_output_bytes = transaction_writer.checkpoint()?;
        resume_checkpoint.signature_output_bytes = 0;
        write_resume_checkpoint(&checkpoint_path, &resume_checkpoint)?;
        (transaction_writer, signature_writer)
    };
    let mut pass_two_scratch = PassTwoScratch::default();
    let mut total_transactions = resume_checkpoint.transactions;
    let mut total_signatures = resume_checkpoint.signatures;
    let pass_two_started = Instant::now();
    for (index, plan) in plans
        .iter()
        .enumerate()
        .skip(resume_checkpoint.next_pass_two_plan)
    {
        let signature_stats_before = pass_two_scratch.signatures.stats();
        let read_stats = write_epoch_pass_two(
            &archive_root,
            &root,
            plan,
            &global,
            &mut transaction_writer,
            &mut signature_writer,
            &mut pass_two_scratch,
            &mut total_transactions,
            &mut total_signatures,
        )?;
        let signature_stats = pass_two_scratch
            .signatures
            .stats()
            .checked_sub(signature_stats_before)?;
        let done = u64::try_from(index + 1).context("completed epoch count exceeds u64")?;
        eprintln!(
            "consolidate pass 2: {done}/{epoch_count} epochs, epoch {}, {total_transactions} transactions, {} locator payload bytes, {} transaction read ranges, {} transaction read calls, {} transaction read bytes, {} arena high-water bytes, {} signature input ranges, {} signature input bytes, {} signature read ranges, {} signature read calls, {} signature read bytes, {:.1}s elapsed, {:.1}s ETA",
            plan.epoch,
            read_stats.locator_payload_bytes,
            read_stats.physical_read_ranges,
            read_stats.physical_read_calls,
            read_stats.physical_read_bytes,
            read_stats.arena_high_water_bytes,
            signature_stats.input_ranges,
            signature_stats.input_bytes,
            signature_stats.physical_read_ranges,
            signature_stats.physical_read_calls,
            signature_stats.physical_read_bytes,
            pass_two_started.elapsed().as_secs_f64(),
            eta_seconds(pass_two_started, done, epoch_count),
        );
        resume_checkpoint.next_pass_two_plan = index + 1;
        resume_checkpoint.transactions = total_transactions;
        resume_checkpoint.signatures = total_signatures;
        resume_checkpoint.transaction_output_bytes = transaction_writer.checkpoint()?;
        resume_checkpoint.signature_output_bytes = signature_writer.checkpoint()?;
        write_resume_checkpoint(&checkpoint_path, &resume_checkpoint)?;
    }
    ensure!(
        total_transactions == root.manifest.transactions
            && total_transactions == aggregate.transactions_written
            && total_signatures == expected_signatures,
        "consolidated output counts differ from Pass 1"
    );
    write_frame(
        &mut transaction_writer,
        &TokenTransactionDumpRecord::Footer(TokenTransactionDumpFooter {
            epochs: aggregate.epochs,
            blocks_scanned: aggregate.blocks_scanned,
            transactions_scanned: aggregate.transactions_scanned,
            transactions_written: total_transactions,
            pubkeys: registry.rows,
            signatures: total_signatures,
            owned_block_fallbacks: aggregate.owned_block_fallbacks,
            raw_transaction_fallbacks: 0,
            raw_metadata_fallbacks: 0,
        }),
        &mut frame_scratch,
    )?;
    let transaction_binding = transaction_writer.finish()?;
    let signature_binding = signature_writer.finish()?;
    ensure!(
        signature_binding.bytes
            == total_signatures
                .checked_mul(SIGNATURE_BYTES as u64)
                .context("signature output byte length overflow")?,
        "signature output size differs from its occurrence count"
    );

    let mut account_writer = DigestWriter::create(&account_partial)?;
    account_writer.write_all(&root.frozen_bytes)?;
    let account_binding = account_writer.finish()?;
    ensure!(
        account_binding.sha256 == sha256_bytes(&root.frozen_bytes),
        "copied frozen account bytes changed"
    );
    sync_file(&registry_partial)?;
    global.verify_unchanged()?;

    transaction_binding.verify(&transaction_partial, "partial transaction stream")?;
    publish_partial(&transaction_partial, &output.join(TRANSACTIONS_FILE))?;
    signature_binding.verify(&signature_partial, "partial signature stream")?;
    publish_partial(&signature_partial, &output.join(DUMP_SIGNATURES_FILE))?;
    verify_path_binding(
        &registry_partial,
        &global.stamp,
        "partial global public-key registry",
    )?;
    publish_partial(&registry_partial, &output.join(PUBKEY_REGISTRY_FILE))?;
    account_binding.verify(&account_partial, "partial discovered-account list")?;
    publish_partial(&account_partial, &output.join(ACCOUNTS_FILE))?;
    fs::remove_dir_all(&work)
        .with_context(|| format!("remove completed work directory {}", work.display()))?;
    sync_directory(&output)?;

    let manifest = DumpManifest {
        schema_version: DUMP_SCHEMA_VERSION,
        artifact_kind: DumpArtifactKind::Consolidated,
        complete: true,
        mint: root.manifest.mint.clone(),
        mint_slot: root.manifest.mint_slot,
        mint_signature: root.manifest.mint_signature.clone(),
        workers: root.manifest.workers,
        source_binding: root.manifest.source_binding.clone(),
        first_epoch: root.manifest.first_epoch,
        last_epoch: root.manifest.last_epoch,
        transactions: total_transactions,
        signatures: Some(total_signatures),
        pubkeys: Some(registry.rows),
        transaction_stream: TRANSACTIONS_FILE.to_owned(),
        transaction_stream_sha256: Some(hex_digest(transaction_binding.sha256)),
        account_id_log: None,
        account_id_log_sha256: None,
        discovered_accounts: Some(ACCOUNTS_FILE.to_owned()),
        discovered_accounts_sha256: Some(hex_digest(account_binding.sha256)),
        discovered_account_count: root.manifest.discovered_account_count,
        signature_stream: Some(DUMP_SIGNATURES_FILE.to_owned()),
        signature_stream_sha256: Some(hex_digest(signature_binding.sha256)),
        pubkey_registry: Some(PUBKEY_REGISTRY_FILE.to_owned()),
        pubkey_registry_sha256: Some(hex_digest(registry.sha256)),
        registry_maps: None,
    };
    let manifest_partial = partial_path(&output, DUMP_MANIFEST_FILE);
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    let mut manifest_file = BufWriter::new(create_new_file(&manifest_partial)?);
    manifest_file.write_all(&manifest_bytes)?;
    manifest_file.flush()?;
    manifest_file.get_ref().sync_all()?;
    drop(manifest_file);
    publish_partial(&manifest_partial, &output.join(DUMP_MANIFEST_FILE))?;
    sync_directory(&output)?;
    eprintln!(
        "consolidate complete: {epoch_count}/{epoch_count} epochs, {total_transactions} transactions, {:.1}s elapsed, 0.0s ETA",
        consolidation_started.elapsed().as_secs_f64(),
    );
    Ok(())
}

struct FinalRegistryVisitor {
    registry_entries: u32,
}

impl ArchiveV2WireRewriteVisitor for FinalRegistryVisitor {
    type Checkpoint = ();

    fn checkpoint(&mut self) -> Self::Checkpoint {}

    fn rewrite_pubkey(
        &mut self,
        pubkey: CompactPubkey,
        _class: ArchiveV2WireReferenceClass,
    ) -> anyhow::Result<CompactPubkey> {
        match pubkey {
            CompactPubkey::Id(id) if id != 0 && id <= self.registry_entries => Ok(pubkey),
            CompactPubkey::Id(id) => bail!(
                "consolidated public-key ID {id} is outside 1..={}",
                self.registry_entries
            ),
            CompactPubkey::Raw(_) => bail!("consolidated payload contains an inline public key"),
        }
    }

    fn rollback(&mut self, _checkpoint: Self::Checkpoint) {}
}

fn validate_final_metadata_wire(
    input: &[u8],
    registry_entries: u32,
    output: &mut Vec<u8>,
    comparison_output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let automatic = {
        let mut visitor = FinalRegistryVisitor { registry_entries };
        rewrite_archive_v2_metadata_wire_preserving_error_schema(
            input,
            output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
    };
    match automatic {
        Ok(_) => ensure!(
            output == input,
            "metadata identity validation changed bytes"
        ),
        Err(error) if is_metadata_schema_ambiguity(&error) => {
            match probe_ambiguous_metadata_schema(input, output, comparison_output)? {
                selection
                @ (MetadataSchemaSelection::Current | MetadataSchemaSelection::Legacy) => {
                    output.clear();
                    let mut visitor = FinalRegistryVisitor { registry_entries };
                    rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
                        input,
                        output,
                        &mut visitor,
                        ArchiveV2WireRewriteLimits::default(),
                        selected_metadata_schema(selection),
                    )?;
                    ensure!(output == input, "selected metadata identity changed bytes");
                }
                MetadataSchemaSelection::Both => {
                    output.clear();
                    let mut current_visitor = FinalRegistryVisitor { registry_entries };
                    rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
                        input,
                        output,
                        &mut current_visitor,
                        ArchiveV2WireRewriteLimits::default(),
                        ArchiveV2WireMetadataErrorSchema::Current,
                    )?;
                    comparison_output.clear();
                    let mut legacy_visitor = FinalRegistryVisitor { registry_entries };
                    rewrite_archive_v2_metadata_wire_preserving_selected_error_schema(
                        input,
                        comparison_output,
                        &mut legacy_visitor,
                        ArchiveV2WireRewriteLimits::default(),
                        ArchiveV2WireMetadataErrorSchema::Legacy,
                    )?;
                    ensure!(
                        output == comparison_output && output == input,
                        "ambiguous metadata schemas do not have one exact final identity"
                    );
                }
            }
        }
        Err(error) => return Err(error).context("validate consolidated metadata wire"),
    }
    Ok(())
}

fn validate_final_record_wire(
    record: &TokenTransactionRecord,
    registry_entries: u32,
    output: &mut Vec<u8>,
    comparison_output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let mut visitor = FinalRegistryVisitor { registry_entries };
    projector(record.source_wire_profile).rewrite_message_wire(
        &record.message_bytes,
        output,
        &mut visitor,
        ArchiveV2WireRewriteLimits::default(),
    )?;
    ensure!(
        output == &record.message_bytes,
        "message identity validation changed bytes"
    );
    if !record.metadata_bytes.is_empty() {
        validate_final_metadata_wire(
            &record.metadata_bytes,
            registry_entries,
            output,
            comparison_output,
        )?;
    }
    Ok(())
}

fn hash_regular_file(path: &Path, expected_bytes: u64) -> Result<[u8; 32]> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("inspect {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "{} is not a regular file",
        path.display()
    );
    ensure!(
        metadata.len() == expected_bytes,
        "{} has an unexpected size",
        path.display()
    );
    let file = File::open(path)?;
    let stamp = FileStamp::read(&file)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, &file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    stamp.verify(&file, &format!("artifact {}", path.display()))?;
    Ok(hasher.finalize().into())
}

fn validate_final_registry(
    path: &Path,
    expected_rows: u64,
    expected_sha256: [u8; 32],
) -> Result<u32> {
    ensure!(
        expected_rows != 0 && expected_rows < u64::from(u32::MAX),
        "invalid registry row count"
    );
    let expected_bytes = expected_rows
        .checked_mul(KEY_BYTES as u64)
        .context("registry byte length overflow")?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.file_type().is_file() && metadata.len() == expected_bytes,
        "registry size differs"
    );
    let file = File::open(path)?;
    let stamp = FileStamp::read(&file)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, &file);
    let mut hasher = Sha256::new();
    let mut previous = None;
    for _ in 0..expected_rows {
        let mut key = [0u8; KEY_BYTES];
        reader.read_exact(&mut key)?;
        ensure!(
            previous.is_none_or(|value| value < key),
            "registry is not strictly sorted and unique"
        );
        hasher.update(key);
        previous = Some(key);
    }
    let mut trailing = [0u8; 1];
    ensure!(
        reader.read(&mut trailing)? == 0,
        "registry has trailing bytes"
    );
    stamp.verify(&file, "consolidated public-key registry")?;
    ensure!(
        <[u8; 32]>::from(hasher.finalize()) == expected_sha256,
        "registry digest differs"
    );
    u32::try_from(expected_rows).context("registry row count exceeds u32")
}

fn validate_exact_final_files(output: &Path) -> Result<()> {
    let mut observed = Vec::new();
    for entry in fs::read_dir(output)? {
        let entry = entry?;
        ensure!(
            entry.file_type()?.is_file(),
            "consolidated output contains a non-file member"
        );
        observed.push(
            entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("consolidated output has a non-UTF-8 name"))?,
        );
    }
    observed.sort_unstable();
    let mut expected = vec![
        ACCOUNTS_FILE.to_owned(),
        DUMP_MANIFEST_FILE.to_owned(),
        PUBKEY_REGISTRY_FILE.to_owned(),
        DUMP_SIGNATURES_FILE.to_owned(),
        TRANSACTIONS_FILE.to_owned(),
    ];
    expected.sort_unstable();
    ensure!(
        observed == expected,
        "consolidated output does not contain exactly five final files"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum ProgramInventoryOrigin {
    Outer,
    Inner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
struct ProgramInventoryCoordinate {
    source_epoch: u64,
    slot: u64,
    source_block_id: u32,
    tx_index: u32,
}

impl ProgramInventoryCoordinate {
    fn from_record(record: &BorrowedTransactionRecord<'_>) -> Self {
        Self {
            source_epoch: record.source_epoch,
            slot: record.block.slot,
            source_block_id: record.source_block_id,
            tx_index: record.tx_index,
        }
    }

    fn canonical_key(self) -> (u64, u64, u32, u32) {
        (
            self.source_epoch,
            self.slot,
            self.source_block_id,
            self.tx_index,
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct ProgramAccumulator {
    registry_id: u32,
    outer_occurrences: u64,
    inner_occurrences: u64,
    target_account_inner_occurrences: u64,
    target_mint_inner_occurrences: u64,
    target_token_account_inner_occurrences: u64,
    target_account_inner_references: u64,
    target_mint_inner_references: u64,
    target_token_account_inner_references: u64,
    transactions: u64,
    outer_transactions: u64,
    inner_transactions: u64,
    target_account_inner_transactions: u64,
    last_transaction_ordinal: u64,
    last_outer_transaction_ordinal: u64,
    last_inner_transaction_ordinal: u64,
    last_target_account_inner_transaction_ordinal: u64,
    first_transaction: ProgramInventoryCoordinate,
    first_origin: ProgramInventoryOrigin,
}

impl ProgramAccumulator {
    fn new(
        registry_id: u32,
        transaction_ordinal: u64,
        coordinate: ProgramInventoryCoordinate,
        origin: ProgramInventoryOrigin,
    ) -> Self {
        Self {
            registry_id,
            outer_occurrences: u64::from(origin == ProgramInventoryOrigin::Outer),
            inner_occurrences: u64::from(origin == ProgramInventoryOrigin::Inner),
            target_account_inner_occurrences: 0,
            target_mint_inner_occurrences: 0,
            target_token_account_inner_occurrences: 0,
            target_account_inner_references: 0,
            target_mint_inner_references: 0,
            target_token_account_inner_references: 0,
            transactions: 1,
            outer_transactions: u64::from(origin == ProgramInventoryOrigin::Outer),
            inner_transactions: u64::from(origin == ProgramInventoryOrigin::Inner),
            target_account_inner_transactions: 0,
            last_transaction_ordinal: transaction_ordinal,
            last_outer_transaction_ordinal: if origin == ProgramInventoryOrigin::Outer {
                transaction_ordinal
            } else {
                u64::MAX
            },
            last_inner_transaction_ordinal: if origin == ProgramInventoryOrigin::Inner {
                transaction_ordinal
            } else {
                u64::MAX
            },
            last_target_account_inner_transaction_ordinal: u64::MAX,
            first_transaction: coordinate,
            first_origin: origin,
        }
    }

    fn record_count(
        &mut self,
        transaction_ordinal: u64,
        origin: ProgramInventoryOrigin,
        count: u64,
    ) -> Result<()> {
        ensure!(count != 0, "program occurrence increment is zero");
        match origin {
            ProgramInventoryOrigin::Outer => {
                self.outer_occurrences = self
                    .outer_occurrences
                    .checked_add(count)
                    .context("outer program occurrence count overflow")?;
                if self.last_outer_transaction_ordinal != transaction_ordinal {
                    self.outer_transactions = self
                        .outer_transactions
                        .checked_add(1)
                        .context("outer program transaction count overflow")?;
                    self.last_outer_transaction_ordinal = transaction_ordinal;
                }
            }
            ProgramInventoryOrigin::Inner => {
                self.inner_occurrences = self
                    .inner_occurrences
                    .checked_add(count)
                    .context("inner program occurrence count overflow")?;
                if self.last_inner_transaction_ordinal != transaction_ordinal {
                    self.inner_transactions = self
                        .inner_transactions
                        .checked_add(1)
                        .context("inner program transaction count overflow")?;
                    self.last_inner_transaction_ordinal = transaction_ordinal;
                }
            }
        }
        if self.last_transaction_ordinal != transaction_ordinal {
            self.transactions = self
                .transactions
                .checked_add(1)
                .context("program transaction count overflow")?;
            self.last_transaction_ordinal = transaction_ordinal;
        }
        Ok(())
    }

    fn record_target_account_inner(
        &mut self,
        transaction_ordinal: u64,
        target_mint_references: u64,
        target_token_account_references: u64,
    ) -> Result<bool> {
        let target_account_references = target_mint_references
            .checked_add(target_token_account_references)
            .context("target-account inner reference count overflow")?;
        ensure!(
            target_account_references != 0,
            "target-account inner occurrence has no target reference"
        );
        checked_increment(
            &mut self.target_account_inner_occurrences,
            "per-program target-account inner occurrence count",
        )?;
        if target_mint_references != 0 {
            checked_increment(
                &mut self.target_mint_inner_occurrences,
                "per-program target-mint inner occurrence count",
            )?;
        }
        if target_token_account_references != 0 {
            checked_increment(
                &mut self.target_token_account_inner_occurrences,
                "per-program target-token-account inner occurrence count",
            )?;
        }
        self.target_account_inner_references = self
            .target_account_inner_references
            .checked_add(target_account_references)
            .context("per-program target-account inner reference count overflow")?;
        self.target_mint_inner_references = self
            .target_mint_inner_references
            .checked_add(target_mint_references)
            .context("per-program target-mint inner reference count overflow")?;
        self.target_token_account_inner_references = self
            .target_token_account_inner_references
            .checked_add(target_token_account_references)
            .context("per-program target-token-account inner reference count overflow")?;
        let first_target_occurrence_in_transaction =
            self.last_target_account_inner_transaction_ordinal != transaction_ordinal;
        if first_target_occurrence_in_transaction {
            checked_increment(
                &mut self.target_account_inner_transactions,
                "per-program target-account inner transaction count",
            )?;
            self.last_target_account_inner_transaction_ordinal = transaction_ordinal;
        }
        Ok(first_target_occurrence_in_transaction)
    }

    fn total_occurrences(self) -> Result<u64> {
        self.outer_occurrences
            .checked_add(self.inner_occurrences)
            .context("program occurrence count overflow")
    }
}

#[derive(Debug)]
struct ProgramAccumulatorTable {
    by_registry_id: Vec<u32>,
    programs: Vec<ProgramAccumulator>,
}

impl ProgramAccumulatorTable {
    fn new(registry_entries: u32) -> Result<Self> {
        let entries = usize::try_from(registry_entries).context("registry size exceeds usize")?;
        let dense_len = entries
            .checked_add(1)
            .context("dense program-index length overflow")?;
        let mut by_registry_id = Vec::new();
        by_registry_id
            .try_reserve_exact(dense_len)
            .context("reserve dense registry-to-program index")?;
        by_registry_id.resize(dense_len, PROGRAM_ACCUMULATOR_MISSING);
        let mut programs = Vec::new();
        // Program IDs are sparse relative to the full registry. Growth can
        // occur only when a new unique program is committed, never for each
        // instruction occurrence.
        programs
            .try_reserve_exact(entries.min(4_096))
            .context("reserve program accumulators")?;
        Ok(Self {
            by_registry_id,
            programs,
        })
    }

    fn record(
        &mut self,
        registry_id: u32,
        transaction_ordinal: u64,
        coordinate: ProgramInventoryCoordinate,
        origin: ProgramInventoryOrigin,
    ) -> Result<()> {
        self.record_count(registry_id, transaction_ordinal, coordinate, origin, 1)
    }

    fn record_count(
        &mut self,
        registry_id: u32,
        transaction_ordinal: u64,
        coordinate: ProgramInventoryCoordinate,
        origin: ProgramInventoryOrigin,
        count: u64,
    ) -> Result<()> {
        ensure!(count != 0, "program occurrence increment is zero");
        let dense = self
            .by_registry_id
            .get_mut(usize::try_from(registry_id)?)
            .context("resolved program registry ID is outside the dump registry")?;
        ensure!(
            registry_id != 0,
            "resolved program registry ID zero is reserved"
        );
        if *dense == PROGRAM_ACCUMULATOR_MISSING {
            let index =
                u32::try_from(self.programs.len()).context("distinct program count exceeds u32")?;
            let mut accumulator =
                ProgramAccumulator::new(registry_id, transaction_ordinal, coordinate, origin);
            if count != 1 {
                accumulator.record_count(transaction_ordinal, origin, count - 1)?;
            }
            self.programs.push(accumulator);
            *dense = index;
            return Ok(());
        }
        self.programs[usize::try_from(*dense)?].record_count(transaction_ordinal, origin, count)
    }

    fn record_target_account_inner(
        &mut self,
        registry_id: u32,
        transaction_ordinal: u64,
        target_mint_references: u64,
        target_token_account_references: u64,
    ) -> Result<bool> {
        let index = *self
            .by_registry_id
            .get(usize::try_from(registry_id)?)
            .context("target-account inner program ID is outside the dump registry")?;
        ensure!(
            index != PROGRAM_ACCUMULATOR_MISSING,
            "target-account inner program was not recorded as an inner program"
        );
        self.programs[usize::try_from(index)?].record_target_account_inner(
            transaction_ordinal,
            target_mint_references,
            target_token_account_references,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum InnerProgramLocation {
    Static,
    LoadedWritable,
    LoadedReadonly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StagedInnerProgram {
    registry_id_or_account_index: u32,
    location: InnerProgramLocation,
    account_start: usize,
    account_len: usize,
    target_mint_references: u64,
    target_token_account_references: u64,
}

#[derive(Debug)]
struct MetadataProgramStage {
    programs: Vec<StagedInnerProgram>,
    account_indices: Vec<u8>,
    loaded_ids: [u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    loaded_generation: [u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    generation: u32,
}

impl MetadataProgramStage {
    fn new() -> Self {
        Self {
            programs: Vec::with_capacity(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS),
            account_indices: Vec::with_capacity(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS),
            loaded_ids: [0; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
            loaded_generation: [0; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
            generation: 0,
        }
    }

    fn begin(&mut self) {
        self.programs.clear();
        self.account_indices.clear();
        if self.generation == u32::MAX {
            self.loaded_generation.fill(0);
            self.generation = 1;
        } else {
            self.generation += 1;
        }
    }
}

#[derive(Debug)]
struct ProgramInventoryScratch {
    static_ids: [u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    outer_counts: [u64; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    outer_touched: [u8; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    outer_touched_len: usize,
    current_metadata: MetadataProgramStage,
    legacy_metadata: MetadataProgramStage,
}

impl ProgramInventoryScratch {
    fn new() -> Self {
        Self {
            static_ids: [0; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
            outer_counts: [0; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
            outer_touched: [0; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
            outer_touched_len: 0,
            current_metadata: MetadataProgramStage::new(),
            legacy_metadata: MetadataProgramStage::new(),
        }
    }

    fn clear_outer(&mut self) {
        for &index in &self.outer_touched[..self.outer_touched_len] {
            self.outer_counts[usize::from(index)] = 0;
        }
        self.outer_touched_len = 0;
    }
}

#[derive(Debug, Default, serde::Serialize)]
struct ProgramInventoryCounters {
    transactions: u64,
    distinct_programs: u64,
    transactions_with_outer_instructions: u64,
    transactions_with_inner_instructions: u64,
    transactions_with_target_account_inner_instructions: u64,
    outer_occurrences: u64,
    inner_occurrences: u64,
    target_account_inner_occurrences: u64,
    target_account_inner_transactions: u64,
    target_mint_inner_occurrences: u64,
    target_token_account_inner_occurrences: u64,
    target_account_inner_references: u64,
    target_mint_inner_references: u64,
    target_token_account_inner_references: u64,
    outer_static_resolutions: u64,
    inner_static_resolutions: u64,
    inner_loaded_writable_resolutions: u64,
    inner_loaded_readonly_resolutions: u64,
    unresolved_program_references: u64,
    inline_raw_program_keys: u64,
    metadata_absent: u64,
    metadata_without_error: u64,
    metadata_current_only: u64,
    metadata_legacy_only: u64,
    metadata_both_same_program_resolution: u64,
    metadata_both_divergent: u64,
    post_profile_messages: u64,
    pre_profile_messages: u64,
}

#[derive(Debug, serde::Serialize)]
struct ProgramInventorySource {
    mint: String,
    manifest_sha256: String,
    transaction_stream_sha256: String,
    pubkey_registry_sha256: String,
    transactions: u64,
    signatures: u64,
    registry_entries: u32,
    first_epoch: u64,
    last_epoch: u64,
}

#[derive(Debug, serde::Serialize)]
struct ProgramInventoryTargetAccountSource {
    file: &'static str,
    sha256: String,
    discovered_token_accounts: u64,
    target_addresses: u64,
    membership_definition: &'static str,
}

#[derive(Debug)]
struct ProgramInventoryTargetAccounts {
    target_mint_id: u32,
    token_account_by_registry_id: Vec<u8>,
    source: ProgramInventoryTargetAccountSource,
}

#[derive(Debug, serde::Serialize)]
struct ProgramInventoryBoundSource {
    #[serde(flatten)]
    general: ProgramInventorySource,
    target_accounts: ProgramInventoryTargetAccountSource,
}

#[derive(Debug, serde::Serialize)]
struct ProgramInventoryProgram {
    registry_id: u32,
    program_id: String,
    raw_pubkey_hex: String,
    total_occurrences: u64,
    outer_occurrences: u64,
    inner_occurrences: u64,
    transactions: u64,
    outer_transactions: u64,
    inner_transactions: u64,
    first_transaction: ProgramInventoryCoordinate,
    first_origin: ProgramInventoryOrigin,
}

#[derive(Debug, serde::Serialize)]
struct ProgramInventoryTargetProgram {
    #[serde(flatten)]
    general: ProgramInventoryProgram,
    target_account_inner_occurrences: u64,
    target_account_inner_transactions: u64,
    target_mint_inner_occurrences: u64,
    target_token_account_inner_occurrences: u64,
    target_account_inner_references: u64,
    target_mint_inner_references: u64,
    target_token_account_inner_references: u64,
}

#[derive(Debug, serde::Serialize)]
struct ProgramInventoryReport {
    schema_version: u16,
    artifact_kind: &'static str,
    complete: bool,
    instruction_program_resolution_complete: bool,
    program_order: &'static str,
    source: ProgramInventoryBoundSource,
    counters: ProgramInventoryCounters,
    programs: Vec<ProgramInventoryTargetProgram>,
}

#[derive(Debug, Default, serde::Serialize)]
struct ProgramCoverageCounters {
    transactions: u64,
    fully_covered_transactions: u64,
    partially_covered_transactions: u64,
    touched_transactions: u64,
    uncovered_transactions: u64,
    transactions_without_instructions: u64,
    outer_occurrences: u64,
    identified_outer_occurrences: u64,
    unidentified_outer_occurrences: u64,
    inner_occurrences: u64,
    identified_inner_occurrences: u64,
    unidentified_inner_occurrences: u64,
}

#[derive(Debug, serde::Serialize)]
struct ProgramCoverageIdentifiedSet {
    input_sha256: String,
    input_format: &'static str,
    requested_programs: u64,
    programs_present_in_registry: u64,
    programs_used_by_instructions: u64,
    program_ids: Vec<String>,
    program_ids_absent_from_registry: Vec<String>,
    program_ids_not_used_by_instructions: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
struct ProgramAnalysisGenerator {
    crate_name: &'static str,
    crate_version: &'static str,
    executable_sha256: String,
}

#[derive(Debug, serde::Serialize)]
struct ProgramCoverageReport {
    schema_version: u16,
    artifact_kind: &'static str,
    complete: bool,
    generator: ProgramAnalysisGenerator,
    instruction_program_resolution_complete: bool,
    transaction_classes: &'static str,
    unknown_program_order: &'static str,
    source: ProgramInventorySource,
    identified_set: ProgramCoverageIdentifiedSet,
    counters: ProgramCoverageCounters,
    distinct_programs: u64,
    unknown_programs: Vec<ProgramInventoryProgram>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagedProgramLogEventKind {
    Neutral,
    Unkeyed,
    Explicit {
        program: CompactPubkey,
    },
    Invoke {
        program: CompactPubkey,
        depth: Option<u8>,
    },
    Terminal {
        program: CompactPubkey,
    },
    AmbiguousCustomFailure {
        program: CompactPubkey,
    },
    Truncated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
enum ProgramLogTextTrustLane {
    ExplicitProgramIdLog = 0,
    CleanStackProgramLog = 1,
    CleanStackProgramLogError = 2,
    CleanStackLowTrustContext = 3,
    ExplicitRuntimeErrorContext = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
enum ProgramLogTextKind {
    Unknown = 0,
    AnchorInstruction = 1,
    AnchorErrorFile = 2,
    AnchorErrorCode = 3,
    AnchorErrorMessage = 4,
    ProgramLogError = 5,
    Plain = 6,
    Unparsed = 7,
    FailureReason = 8,
    FailedToComplete = 9,
}

impl ProgramLogTextKind {
    fn is_exact(self) -> bool {
        matches!(
            self,
            Self::AnchorInstruction
                | Self::AnchorErrorFile
                | Self::AnchorErrorCode
                | Self::AnchorErrorMessage
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StagedProgramLogText<'de> {
    trust_lane: ProgramLogTextTrustLane,
    kind: ProgramLogTextKind,
    text: &'de str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StagedProgramLogEvent {
    ordinal: usize,
    tag: u32,
    kind: StagedProgramLogEventKind,
}

#[derive(Debug)]
struct ProgramLogPatternAccumulator {
    trust_lane: ProgramLogTextTrustLane,
    kind: ProgramLogTextKind,
    events: u64,
    example: String,
}

#[derive(Debug)]
struct ProgramLogAccumulator {
    raw_key: [u8; KEY_BYTES],
    registry_id: Option<u32>,
    explicit_evidence_events: u64,
    explicit_evidence_transactions: u64,
    attributed_unkeyed_events: u64,
    attributed_unkeyed_transactions: u64,
    last_explicit_transaction: u64,
    last_unkeyed_transaction: u64,
    explicit_event_tags: [u64; COMPACT_LOG_EVENT_TAG_COUNT],
    unkeyed_event_tags: [u64; COMPACT_LOG_EVENT_TAG_COUNT],
    text_pattern_observations: u64,
    skipped_new_text_patterns: u64,
    text_patterns: HashMap<String, ProgramLogPatternAccumulator>,
    skipped_new_callers: u64,
    skipped_new_callees: u64,
    callers: HashMap<u32, u64>,
    callees: HashMap<u32, u64>,
}

impl ProgramLogAccumulator {
    fn new(raw_key: [u8; KEY_BYTES], registry_id: Option<u32>) -> Self {
        Self {
            raw_key,
            registry_id,
            explicit_evidence_events: 0,
            explicit_evidence_transactions: 0,
            attributed_unkeyed_events: 0,
            attributed_unkeyed_transactions: 0,
            last_explicit_transaction: u64::MAX,
            last_unkeyed_transaction: u64::MAX,
            explicit_event_tags: [0; COMPACT_LOG_EVENT_TAG_COUNT],
            unkeyed_event_tags: [0; COMPACT_LOG_EVENT_TAG_COUNT],
            text_pattern_observations: 0,
            skipped_new_text_patterns: 0,
            text_patterns: HashMap::new(),
            skipped_new_callers: 0,
            skipped_new_callees: 0,
            callers: HashMap::new(),
            callees: HashMap::new(),
        }
    }

    fn record_explicit(&mut self, transaction: u64, tag: u32) -> Result<()> {
        checked_increment(
            &mut self.explicit_evidence_events,
            "selected explicit log-evidence count",
        )?;
        checked_increment(
            self.explicit_event_tags
                .get_mut(usize::try_from(tag)?)
                .context("compact log event tag is outside the known schema")?,
            "selected explicit log-event-tag count",
        )?;
        if self.last_explicit_transaction != transaction {
            checked_increment(
                &mut self.explicit_evidence_transactions,
                "selected explicit log-evidence transaction count",
            )?;
            self.last_explicit_transaction = transaction;
        }
        Ok(())
    }

    fn record_unkeyed(&mut self, transaction: u64, tag: u32) -> Result<()> {
        checked_increment(
            &mut self.attributed_unkeyed_events,
            "selected attributed unkeyed log count",
        )?;
        checked_increment(
            self.unkeyed_event_tags
                .get_mut(usize::try_from(tag)?)
                .context("compact log event tag is outside the known schema")?,
            "selected attributed unkeyed log-event-tag count",
        )?;
        if self.last_unkeyed_transaction != transaction {
            checked_increment(
                &mut self.attributed_unkeyed_transactions,
                "selected attributed unkeyed log transaction count",
            )?;
            self.last_unkeyed_transaction = transaction;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SelectedProgramLogSet {
    input_sha256: [u8; 32],
    by_registry_id: Vec<u32>,
    programs: Vec<ProgramLogAccumulator>,
    programs_present_in_registry: u64,
}

impl SelectedProgramLogSet {
    fn selected_index(&self, program: CompactPubkey) -> Option<usize> {
        match program {
            CompactPubkey::Id(id) => self
                .by_registry_id
                .get(usize::try_from(id).ok()?)
                .copied()
                .filter(|index| *index != PROGRAM_ACCUMULATOR_MISSING)
                .and_then(|index| usize::try_from(index).ok()),
            CompactPubkey::Raw(raw) => self
                .programs
                .binary_search_by_key(&raw, |program| program.raw_key)
                .ok(),
        }
    }
}

#[derive(Debug, Default, serde::Serialize)]
struct ProgramLogInventoryCounters {
    transactions: u64,
    transactions_with_logs: u64,
    compact_log_events: u64,
    explicit_program_evidence_events: u64,
    selected_explicit_program_evidence_events: u64,
    selected_explicit_program_evidence_transactions: u64,
    unkeyed_evidence_events: u64,
    selected_attributed_unkeyed_events: u64,
    selected_attributed_unkeyed_transactions: u64,
    unselected_attributed_unkeyed_events: u64,
    unattributed_unkeyed_events: u64,
    transactions_with_unattributed_unkeyed_events: u64,
    invoke_events: u64,
    definite_terminal_events: u64,
    ambiguous_custom_failure_events: u64,
    log_truncated_events: u64,
    stack_resynchronizations: u64,
    inline_raw_program_references: u64,
    selected_text_pattern_observations: u64,
    skipped_new_text_patterns: u64,
    selected_caller_edge_observations: u64,
    selected_callee_edge_observations: u64,
    skipped_new_call_edges: u64,
    holdout_diagnostics: u64,
    holdout_details_retained: u64,
    metadata_absent: u64,
    metadata_without_error: u64,
    metadata_current_only: u64,
    metadata_legacy_only: u64,
    metadata_both_same_log_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum ProgramLogHoldoutReason {
    UnkeyedWithoutActiveFrame,
    UnkeyedWhileStackDirty,
    InvokeDepthZero,
    InvokeDepthMismatch,
    BpfInvokeWhileStackDirty,
    TerminalWithoutActiveFrame,
    TerminalProgramMismatch,
    AmbiguousCustomProgramError,
    LogTruncated,
    UnclosedFramesAtTransactionEnd,
    DirtyStackAtTransactionEnd,
    InlineRawProgramReference,
}

#[derive(Debug, serde::Serialize)]
struct ProgramLogHoldout {
    transaction: ProgramInventoryCoordinate,
    event_ordinal: Option<usize>,
    event_tag: Option<u32>,
    reason: ProgramLogHoldoutReason,
    active_stack_depth: usize,
    program_reference: Option<CompactPubkey>,
}

#[derive(Debug, serde::Serialize)]
struct ProgramLogEventCount {
    tag: u32,
    kind: &'static str,
    events: u64,
}

#[derive(Debug, serde::Serialize)]
struct ProgramLogEvidenceClass {
    events: u64,
    transactions: u64,
    event_kinds: Vec<ProgramLogEventCount>,
}

#[derive(Debug, serde::Serialize)]
struct ProgramLogTextPatternReport {
    trust_lane: ProgramLogTextTrustLane,
    text_kind: ProgramLogTextKind,
    pattern: String,
    example: String,
    events: u64,
}

#[derive(Debug, serde::Serialize)]
struct ProgramLogCallEdgeReport {
    registry_id: u32,
    program_id: String,
    invokes: u64,
}

#[derive(Debug, serde::Serialize)]
struct ProgramLogProgramReport {
    registry_id: Option<u32>,
    program_id: String,
    raw_pubkey_hex: String,
    explicit_id_evidence: ProgramLogEvidenceClass,
    attributed_unkeyed_evidence: ProgramLogEvidenceClass,
    text_pattern_observations: u64,
    skipped_new_text_patterns: u64,
    text_patterns: Vec<ProgramLogTextPatternReport>,
    skipped_new_callers: u64,
    skipped_new_callees: u64,
    callers: Vec<ProgramLogCallEdgeReport>,
    callees: Vec<ProgramLogCallEdgeReport>,
}

#[derive(Debug, serde::Serialize)]
struct ProgramLogSelectedSetReport {
    input_sha256: String,
    input_format: &'static str,
    requested_programs: u64,
    programs_present_in_registry: u64,
    programs_with_explicit_id_evidence: u64,
    programs_with_attributed_unkeyed_evidence: u64,
    program_ids_absent_from_registry: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
struct ProgramLogInventoryReport {
    schema_version: u16,
    artifact_kind: &'static str,
    complete: bool,
    explicit_id_evidence_complete: bool,
    unkeyed_attribution_complete: bool,
    attribution_policy: &'static str,
    custom_program_error_policy: &'static str,
    program_order: &'static str,
    source: ProgramInventorySource,
    selected_set: ProgramLogSelectedSetReport,
    counters: ProgramLogInventoryCounters,
    programs: Vec<ProgramLogProgramReport>,
    holdout_details_limit: usize,
    holdout_details_truncated: bool,
    holdouts: Vec<ProgramLogHoldout>,
}

#[derive(Debug, Default)]
struct ProgramLogAttributionStack {
    frames: Vec<CompactPubkey>,
    clean: bool,
    complete_prefix: bool,
    suffix_usable: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct ProgramLogTextAttribution {
    explicit_selected: Option<u32>,
    contextual_selected: Option<u32>,
}

impl ProgramLogAttributionStack {
    fn begin_transaction(&mut self) {
        self.frames.clear();
        self.clean = true;
        self.complete_prefix = true;
        self.suffix_usable = true;
    }
}

fn compact_log_event_name(tag: u32) -> &'static str {
    const NAMES: [&str; COMPACT_LOG_EVENT_TAG_COUNT] = [
        "system",
        "log_truncated",
        "stake_merging_accounts",
        "loader_upgraded_program",
        "loader_finalized_account",
        "program_log",
        "program_log_error",
        "program_id_log",
        "program_plain_log",
        "program_account_not_writable",
        "program_id_mismatch",
        "program_not_upgradeable",
        "program_and_program_data_account_mismatch",
        "program_was_extended_in_this_block_already",
        "invoke",
        "bpf_invoke",
        "consumed",
        "bpf_consumed",
        "success",
        "bpf_success",
        "failure",
        "bpf_failure",
        "failure_custom_program_error",
        "bpf_failure_custom_program_error",
        "failure_invalid_account_data",
        "bpf_failure_invalid_account_data",
        "failure_invalid_program_argument",
        "bpf_failure_invalid_program_argument",
        "failed_to_complete",
        "custom_program_error",
        "return",
        "data",
        "consumption",
        "cb_request_units",
        "program_not_deployed",
        "program_not_cached",
        "unknown_program",
        "unknown_account",
        "verify_ed25519",
        "verify_secp256k1",
        "runtime_writable_privilege_escalated",
        "runtime_signer_privilege_escalated",
        "runtime_account_owner_balance_verification_failed",
        "close_context_state",
        "plain",
        "unparsed",
    ];
    usize::try_from(tag)
        .ok()
        .and_then(|index| NAMES.get(index))
        .copied()
        .unwrap_or("unknown_tag")
}

fn stage_program_log_text<'de>(
    log: BorrowedArchiveV2ProgramLog<'de>,
    trust_lane: ProgramLogTextTrustLane,
    tables: &mut BorrowedArchiveV2LogTables<'de>,
    output: &mut [Option<StagedProgramLogText<'de>>; 3],
) -> wincode::ReadResult<()> {
    let mut set = |index: usize, kind: ProgramLogTextKind, text| {
        output[index] = Some(StagedProgramLogText {
            trust_lane,
            kind,
            text,
        });
    };
    match log {
        BorrowedArchiveV2ProgramLog::AnchorInstruction { name } => {
            set(
                0,
                ProgramLogTextKind::AnchorInstruction,
                tables.string(name)?,
            );
        }
        BorrowedArchiveV2ProgramLog::AnchorErrorOccurred {
            code,
            number: _,
            message,
        } => {
            set(0, ProgramLogTextKind::AnchorErrorCode, tables.string(code)?);
            set(
                1,
                ProgramLogTextKind::AnchorErrorMessage,
                tables.string(message)?,
            );
        }
        BorrowedArchiveV2ProgramLog::AnchorErrorThrown {
            file,
            line: _,
            code,
            number: _,
            message,
        } => {
            set(0, ProgramLogTextKind::AnchorErrorFile, tables.string(file)?);
            set(1, ProgramLogTextKind::AnchorErrorCode, tables.string(code)?);
            set(
                2,
                ProgramLogTextKind::AnchorErrorMessage,
                tables.string(message)?,
            );
        }
        BorrowedArchiveV2ProgramLog::Unknown { text } => {
            set(0, ProgramLogTextKind::Unknown, tables.string(text)?);
        }
        BorrowedArchiveV2ProgramLog::Empty
        | BorrowedArchiveV2ProgramLog::Token(_)
        | BorrowedArchiveV2ProgramLog::Token2022(_)
        | BorrowedArchiveV2ProgramLog::Ata(_)
        | BorrowedArchiveV2ProgramLog::AddressLookupTable(_)
        | BorrowedArchiveV2ProgramLog::LoaderV3(_)
        | BorrowedArchiveV2ProgramLog::LoaderV4(_)
        | BorrowedArchiveV2ProgramLog::Memo(_)
        | BorrowedArchiveV2ProgramLog::Record(_)
        | BorrowedArchiveV2ProgramLog::TransferHook(_)
        | BorrowedArchiveV2ProgramLog::AccountCompression(_)
        | BorrowedArchiveV2ProgramLog::Stake(_)
        | BorrowedArchiveV2ProgramLog::ZkElgamalProof(_)
        | BorrowedArchiveV2ProgramLog::Known(_) => {}
    }
    Ok(())
}

fn stage_borrowed_program_log_event_kind(
    event: BorrowedArchiveV2LogEvent<'_>,
) -> StagedProgramLogEvent {
    let kind = match event.kind {
        BorrowedArchiveV2LogEventKind::LogTruncated => StagedProgramLogEventKind::Truncated,
        BorrowedArchiveV2LogEventKind::ProgramIdLog { program, .. } => {
            StagedProgramLogEventKind::Explicit { program }
        }
        BorrowedArchiveV2LogEventKind::Invoke { program, depth } => {
            StagedProgramLogEventKind::Invoke {
                program,
                depth: Some(depth),
            }
        }
        BorrowedArchiveV2LogEventKind::BpfInvoke { program } => StagedProgramLogEventKind::Invoke {
            program,
            depth: None,
        },
        BorrowedArchiveV2LogEventKind::Consumed { program, .. }
        | BorrowedArchiveV2LogEventKind::Return { program, .. } => {
            StagedProgramLogEventKind::Explicit { program }
        }
        BorrowedArchiveV2LogEventKind::Success { program }
        | BorrowedArchiveV2LogEventKind::BpfSuccess { program }
        | BorrowedArchiveV2LogEventKind::Failure { program, .. }
        | BorrowedArchiveV2LogEventKind::BpfFailure { program, .. }
        | BorrowedArchiveV2LogEventKind::FailureInvalidAccountData { program }
        | BorrowedArchiveV2LogEventKind::BpfFailureInvalidAccountData { program }
        | BorrowedArchiveV2LogEventKind::FailureInvalidProgramArgument { program }
        | BorrowedArchiveV2LogEventKind::BpfFailureInvalidProgramArgument { program } => {
            StagedProgramLogEventKind::Terminal { program }
        }
        BorrowedArchiveV2LogEventKind::FailureCustomProgramError { program, .. } => {
            StagedProgramLogEventKind::AmbiguousCustomFailure { program }
        }
        BorrowedArchiveV2LogEventKind::BpfFailureCustomProgramError { program, .. } => {
            StagedProgramLogEventKind::Terminal { program }
        }
        BorrowedArchiveV2LogEventKind::ProgramLog { .. }
        | BorrowedArchiveV2LogEventKind::ProgramPlainLog { .. }
        | BorrowedArchiveV2LogEventKind::ProgramLogError { .. }
        | BorrowedArchiveV2LogEventKind::BpfConsumed { .. }
        | BorrowedArchiveV2LogEventKind::FailedToComplete { .. }
        | BorrowedArchiveV2LogEventKind::CustomProgramError { .. }
        | BorrowedArchiveV2LogEventKind::Data { .. }
        | BorrowedArchiveV2LogEventKind::Consumption { .. }
        | BorrowedArchiveV2LogEventKind::Plain { .. }
        | BorrowedArchiveV2LogEventKind::Unparsed { .. } => StagedProgramLogEventKind::Unkeyed,
        BorrowedArchiveV2LogEventKind::System { .. }
        | BorrowedArchiveV2LogEventKind::StakeMergingAccounts
        | BorrowedArchiveV2LogEventKind::LoaderUpgradedProgram { .. }
        | BorrowedArchiveV2LogEventKind::LoaderFinalizedAccount { .. }
        | BorrowedArchiveV2LogEventKind::ProgramAccountNotWritable
        | BorrowedArchiveV2LogEventKind::ProgramIdMismatch
        | BorrowedArchiveV2LogEventKind::ProgramNotUpgradeable
        | BorrowedArchiveV2LogEventKind::ProgramAndProgramDataAccountMismatch
        | BorrowedArchiveV2LogEventKind::ProgramWasExtendedInThisBlockAlready
        | BorrowedArchiveV2LogEventKind::CbRequestUnits { .. }
        | BorrowedArchiveV2LogEventKind::ProgramNotDeployed { .. }
        | BorrowedArchiveV2LogEventKind::ProgramNotCached { .. }
        | BorrowedArchiveV2LogEventKind::UnknownProgram { .. }
        | BorrowedArchiveV2LogEventKind::UnknownAccount { .. }
        | BorrowedArchiveV2LogEventKind::VerifyEd25519
        | BorrowedArchiveV2LogEventKind::VerifySecp256k1
        | BorrowedArchiveV2LogEventKind::RuntimeWritablePrivilegeEscalated { .. }
        | BorrowedArchiveV2LogEventKind::RuntimeSignerPrivilegeEscalated { .. }
        | BorrowedArchiveV2LogEventKind::RuntimeAccountOwnerBalanceVerificationFailed { .. }
        | BorrowedArchiveV2LogEventKind::CloseContextState => StagedProgramLogEventKind::Neutral,
    };
    StagedProgramLogEvent {
        ordinal: event.ordinal,
        tag: event.tag,
        kind,
    }
}

fn stage_borrowed_program_log_event<'de>(
    event: BorrowedArchiveV2LogEvent<'de>,
    tables: &mut BorrowedArchiveV2LogTables<'de>,
) -> wincode::ReadResult<(
    StagedProgramLogEvent,
    [Option<StagedProgramLogText<'de>>; 3],
)> {
    let mut text = [None; 3];
    let kind = match event.kind {
        BorrowedArchiveV2LogEventKind::LogTruncated => StagedProgramLogEventKind::Truncated,
        BorrowedArchiveV2LogEventKind::ProgramLog { log }
        | BorrowedArchiveV2LogEventKind::ProgramPlainLog { log } => {
            stage_program_log_text(
                log,
                ProgramLogTextTrustLane::CleanStackProgramLog,
                tables,
                &mut text,
            )?;
            StagedProgramLogEventKind::Unkeyed
        }
        BorrowedArchiveV2LogEventKind::ProgramIdLog { program, log } => {
            stage_program_log_text(
                log,
                ProgramLogTextTrustLane::ExplicitProgramIdLog,
                tables,
                &mut text,
            )?;
            StagedProgramLogEventKind::Explicit { program }
        }
        BorrowedArchiveV2LogEventKind::ProgramLogError { message } => {
            text[0] = Some(StagedProgramLogText {
                trust_lane: ProgramLogTextTrustLane::CleanStackProgramLogError,
                kind: ProgramLogTextKind::ProgramLogError,
                text: tables.string(message)?,
            });
            StagedProgramLogEventKind::Unkeyed
        }
        BorrowedArchiveV2LogEventKind::Invoke { program, depth } => {
            StagedProgramLogEventKind::Invoke {
                program,
                depth: Some(depth),
            }
        }
        BorrowedArchiveV2LogEventKind::BpfInvoke { program } => StagedProgramLogEventKind::Invoke {
            program,
            depth: None,
        },
        BorrowedArchiveV2LogEventKind::Consumed { program, .. }
        | BorrowedArchiveV2LogEventKind::Return { program, .. } => {
            StagedProgramLogEventKind::Explicit { program }
        }
        BorrowedArchiveV2LogEventKind::Success { program }
        | BorrowedArchiveV2LogEventKind::BpfSuccess { program }
        | BorrowedArchiveV2LogEventKind::FailureInvalidAccountData { program }
        | BorrowedArchiveV2LogEventKind::BpfFailureInvalidAccountData { program }
        | BorrowedArchiveV2LogEventKind::FailureInvalidProgramArgument { program }
        | BorrowedArchiveV2LogEventKind::BpfFailureInvalidProgramArgument { program } => {
            StagedProgramLogEventKind::Terminal { program }
        }
        BorrowedArchiveV2LogEventKind::Failure { program, reason }
        | BorrowedArchiveV2LogEventKind::BpfFailure { program, reason } => {
            text[0] = Some(StagedProgramLogText {
                trust_lane: ProgramLogTextTrustLane::ExplicitRuntimeErrorContext,
                kind: ProgramLogTextKind::FailureReason,
                text: tables.string(reason)?,
            });
            StagedProgramLogEventKind::Terminal { program }
        }
        BorrowedArchiveV2LogEventKind::FailureCustomProgramError { program, .. } => {
            StagedProgramLogEventKind::AmbiguousCustomFailure { program }
        }
        BorrowedArchiveV2LogEventKind::BpfFailureCustomProgramError { program, .. } => {
            StagedProgramLogEventKind::Terminal { program }
        }
        BorrowedArchiveV2LogEventKind::FailedToComplete { reason } => {
            text[0] = Some(StagedProgramLogText {
                trust_lane: ProgramLogTextTrustLane::CleanStackLowTrustContext,
                kind: ProgramLogTextKind::FailedToComplete,
                text: tables.string(reason)?,
            });
            StagedProgramLogEventKind::Unkeyed
        }
        BorrowedArchiveV2LogEventKind::Plain { text: value } => {
            text[0] = Some(StagedProgramLogText {
                trust_lane: ProgramLogTextTrustLane::CleanStackLowTrustContext,
                kind: ProgramLogTextKind::Plain,
                text: tables.string(value)?,
            });
            StagedProgramLogEventKind::Unkeyed
        }
        BorrowedArchiveV2LogEventKind::Unparsed { text: value } => {
            text[0] = Some(StagedProgramLogText {
                trust_lane: ProgramLogTextTrustLane::CleanStackLowTrustContext,
                kind: ProgramLogTextKind::Unparsed,
                text: tables.string(value)?,
            });
            StagedProgramLogEventKind::Unkeyed
        }
        BorrowedArchiveV2LogEventKind::BpfConsumed { .. }
        | BorrowedArchiveV2LogEventKind::CustomProgramError { .. }
        | BorrowedArchiveV2LogEventKind::Data { .. }
        | BorrowedArchiveV2LogEventKind::Consumption { .. } => StagedProgramLogEventKind::Unkeyed,
        BorrowedArchiveV2LogEventKind::System { .. }
        | BorrowedArchiveV2LogEventKind::StakeMergingAccounts
        | BorrowedArchiveV2LogEventKind::LoaderUpgradedProgram { .. }
        | BorrowedArchiveV2LogEventKind::LoaderFinalizedAccount { .. }
        | BorrowedArchiveV2LogEventKind::ProgramAccountNotWritable
        | BorrowedArchiveV2LogEventKind::ProgramIdMismatch
        | BorrowedArchiveV2LogEventKind::ProgramNotUpgradeable
        | BorrowedArchiveV2LogEventKind::ProgramAndProgramDataAccountMismatch
        | BorrowedArchiveV2LogEventKind::ProgramWasExtendedInThisBlockAlready
        | BorrowedArchiveV2LogEventKind::CbRequestUnits { .. }
        | BorrowedArchiveV2LogEventKind::ProgramNotDeployed { .. }
        | BorrowedArchiveV2LogEventKind::ProgramNotCached { .. }
        | BorrowedArchiveV2LogEventKind::UnknownProgram { .. }
        | BorrowedArchiveV2LogEventKind::UnknownAccount { .. }
        | BorrowedArchiveV2LogEventKind::VerifyEd25519
        | BorrowedArchiveV2LogEventKind::VerifySecp256k1
        | BorrowedArchiveV2LogEventKind::RuntimeWritablePrivilegeEscalated { .. }
        | BorrowedArchiveV2LogEventKind::RuntimeSignerPrivilegeEscalated { .. }
        | BorrowedArchiveV2LogEventKind::RuntimeAccountOwnerBalanceVerificationFailed { .. }
        | BorrowedArchiveV2LogEventKind::CloseContextState => StagedProgramLogEventKind::Neutral,
    };
    Ok((
        StagedProgramLogEvent {
            ordinal: event.ordinal,
            tag: event.tag,
            kind,
        },
        text,
    ))
}

fn collect_program_log_stage(
    output: &mut Vec<StagedProgramLogEvent>,
    metadata: &[u8],
    error_schema: ArchiveV2WireMetadataErrorSchema,
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
) -> Result<blockzilla_read_sdk::ProjectedArchiveV2CompactLogsSummary> {
    output.clear();
    let summary = visit_archive_v2_compact_logs_exact_with_selected_error_schema(
        metadata,
        error_schema,
        limits,
        registry_entries,
        |event, _| {
            output.push(stage_borrowed_program_log_event_kind(event));
            Ok(())
        },
    )?;
    ensure!(
        summary.event_count == output.len(),
        "compact log callback count differs from metadata summary"
    );
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn collect_selected_program_log_text_patterns(
    metadata: &[u8],
    error_schema: ArchiveV2WireMetadataErrorSchema,
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
    expected_events: &[StagedProgramLogEvent],
    attributions: &[ProgramLogTextAttribution],
    selected: &mut SelectedProgramLogSet,
    counters: &mut ProgramLogInventoryCounters,
    pattern_scratch: &mut String,
) -> Result<()> {
    ensure!(
        expected_events.len() == attributions.len(),
        "compact log text attributions do not cover every event"
    );
    let mut callback_error = None;
    let summary = visit_archive_v2_compact_logs_exact_with_selected_error_schema(
        metadata,
        error_schema,
        limits,
        registry_entries,
        |borrowed, tables| {
            let (event, observations) = stage_borrowed_program_log_event(borrowed, tables)?;
            if callback_error.is_some() {
                return Ok(());
            }
            let result = (|| -> Result<()> {
                let expected = expected_events
                    .get(event.ordinal)
                    .context("compact log event ordinal exceeds staged events")?;
                ensure!(
                    *expected == event,
                    "compact log event changed between evidence and text passes"
                );
                let attribution = attributions[event.ordinal];
                for observation in observations.into_iter().flatten() {
                    let selected_index = match observation.trust_lane {
                        ProgramLogTextTrustLane::ExplicitProgramIdLog
                        | ProgramLogTextTrustLane::ExplicitRuntimeErrorContext => {
                            attribution.explicit_selected
                        }
                        ProgramLogTextTrustLane::CleanStackProgramLog
                        | ProgramLogTextTrustLane::CleanStackProgramLogError
                        | ProgramLogTextTrustLane::CleanStackLowTrustContext => {
                            attribution.contextual_selected
                        }
                    };
                    if let Some(index) = selected_index {
                        record_selected_text_pattern(
                            selected,
                            counters,
                            usize::try_from(index)?,
                            observation,
                            pattern_scratch,
                        )?;
                    }
                }
                Ok(())
            })();
            if let Err(error) = result {
                callback_error = Some(error);
            }
            Ok(())
        },
    )?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    ensure!(
        summary.event_count == expected_events.len(),
        "compact log event count changed between evidence and text passes"
    );
    Ok(())
}

fn serialize_program_log_event_counts(
    counts: &[u64; COMPACT_LOG_EVENT_TAG_COUNT],
) -> Result<Vec<ProgramLogEventCount>> {
    counts
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, count)| *count != 0)
        .map(|(tag, events)| {
            let tag = u32::try_from(tag).context("compact log event tag exceeds u32")?;
            Ok(ProgramLogEventCount {
                tag,
                kind: compact_log_event_name(tag),
                events,
            })
        })
        .collect()
}

fn normalize_log_token(token: &str) -> &str {
    let trimmed = token.trim_matches(|character: char| {
        matches!(
            character,
            ',' | ';' | ':' | '.' | '(' | ')' | '[' | ']' | '{' | '}' | '"' | '\''
        )
    });
    if (32..=44).contains(&trimmed.len())
        && trimmed.bytes().all(|byte| {
            b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(&byte)
        })
    {
        "<pubkey>"
    } else if trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        "<hex>"
    } else if trimmed.len() >= 3 && trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        "<num>"
    } else if trimmed.len() >= 32
        && trimmed
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'='))
    {
        "<blob>"
    } else {
        token
    }
}

fn push_bounded_pattern_text(output: &mut String, text: &str) {
    let mut characters = text.chars();
    for _ in 0..MAX_PROGRAM_LOG_PATTERN_BYTES {
        let Some(character) = characters.next() else {
            return;
        };
        output.push(character);
    }
    if characters.next().is_some() {
        output.push_str("...");
    }
}

fn normalize_log_text_pattern_into(text: &str, output: &mut String) {
    let mut written = 0usize;
    for (index, token) in text.split_whitespace().enumerate() {
        if index != 0 {
            if written == MAX_PROGRAM_LOG_PATTERN_BYTES {
                output.push_str("...");
                return;
            }
            output.push(' ');
            written += 1;
        }
        for character in normalize_log_token(token).chars() {
            if written == MAX_PROGRAM_LOG_PATTERN_BYTES {
                output.push_str("...");
                return;
            }
            output.push(character);
            written += 1;
        }
    }
    if written == 0 {
        output.push_str("<empty>");
    }
}

fn truncate_program_log_example(text: &str) -> String {
    let mut output = String::new();
    for (index, character) in text.chars().enumerate() {
        if index == MAX_PROGRAM_LOG_EXAMPLE_CHARS {
            output.push_str("...");
            break;
        }
        output.push(character);
    }
    output
}

fn record_selected_text_pattern(
    selected: &mut SelectedProgramLogSet,
    counters: &mut ProgramLogInventoryCounters,
    selected_index: usize,
    observation: StagedProgramLogText<'_>,
    pattern_scratch: &mut String,
) -> Result<()> {
    let program = selected
        .programs
        .get_mut(selected_index)
        .context("selected log program index is outside the selected set")?;
    checked_increment(
        &mut program.text_pattern_observations,
        "selected program text-pattern observation count",
    )?;
    checked_increment(
        &mut counters.selected_text_pattern_observations,
        "selected text-pattern observation count",
    )?;

    pattern_scratch.clear();
    pattern_scratch.push(char::from(b'0' + observation.trust_lane as u8));
    pattern_scratch.push('|');
    pattern_scratch.push(char::from(b'0' + observation.kind as u8));
    pattern_scratch.push('|');
    if observation.kind.is_exact() {
        if observation.text.is_empty() {
            pattern_scratch.push_str("<empty>");
        } else {
            push_bounded_pattern_text(pattern_scratch, observation.text);
        }
    } else {
        normalize_log_text_pattern_into(observation.text, pattern_scratch);
    }

    if let Some(pattern) = program.text_patterns.get_mut(pattern_scratch.as_str()) {
        return checked_increment(&mut pattern.events, "program log text-pattern count");
    }
    if program.text_patterns.len() >= MAX_PROGRAM_LOG_PATTERN_KEYS_PER_PROGRAM {
        checked_increment(
            &mut program.skipped_new_text_patterns,
            "selected program skipped text-pattern count",
        )?;
        return checked_increment(
            &mut counters.skipped_new_text_patterns,
            "skipped selected text-pattern count",
        );
    }
    program.text_patterns.insert(
        pattern_scratch.clone(),
        ProgramLogPatternAccumulator {
            trust_lane: observation.trust_lane,
            kind: observation.kind,
            events: 1,
            example: truncate_program_log_example(observation.text),
        },
    );
    Ok(())
}

fn record_program_log_holdout(
    counters: &mut ProgramLogInventoryCounters,
    holdouts: &mut Vec<ProgramLogHoldout>,
    transaction: ProgramInventoryCoordinate,
    event: Option<(usize, u32)>,
    reason: ProgramLogHoldoutReason,
    active_stack_depth: usize,
    program_reference: Option<CompactPubkey>,
) -> Result<()> {
    checked_increment(
        &mut counters.holdout_diagnostics,
        "program log holdout diagnostic count",
    )?;
    if holdouts.len() < MAX_PROGRAM_LOG_HOLDOUT_DETAILS {
        holdouts.push(ProgramLogHoldout {
            transaction,
            event_ordinal: event.map(|value| value.0),
            event_tag: event.map(|value| value.1),
            reason,
            active_stack_depth,
            program_reference,
        });
        checked_increment(
            &mut counters.holdout_details_retained,
            "retained program log holdout count",
        )?;
    }
    Ok(())
}

fn record_explicit_program_log_evidence(
    selected: &mut SelectedProgramLogSet,
    counters: &mut ProgramLogInventoryCounters,
    transaction_ordinal: u64,
    tag: u32,
    program: CompactPubkey,
) -> Result<Option<usize>> {
    if matches!(program, CompactPubkey::Raw(_)) {
        return Ok(None);
    }
    checked_increment(
        &mut counters.explicit_program_evidence_events,
        "explicit program log-evidence count",
    )?;
    let selected_index = selected.selected_index(program);
    if let Some(index) = selected_index {
        selected.programs[index].record_explicit(transaction_ordinal, tag)?;
        checked_increment(
            &mut counters.selected_explicit_program_evidence_events,
            "selected explicit program log-evidence count",
        )?;
    }
    Ok(selected_index)
}

fn record_program_log_call_edge(
    selected: &mut SelectedProgramLogSet,
    counters: &mut ProgramLogInventoryCounters,
    caller: CompactPubkey,
    callee: CompactPubkey,
) -> Result<()> {
    let (CompactPubkey::Id(caller_id), CompactPubkey::Id(callee_id)) = (caller, callee) else {
        return Ok(());
    };
    let caller_selected = selected.selected_index(caller);
    let callee_selected = selected.selected_index(callee);
    if let Some(index) = callee_selected {
        checked_increment(
            &mut counters.selected_caller_edge_observations,
            "selected program caller-edge observation count",
        )?;
        let program = &mut selected.programs[index];
        if let Some(count) = program.callers.get_mut(&caller_id) {
            checked_increment(count, "selected program caller-edge count")?;
        } else if program.callers.len() < MAX_PROGRAM_LOG_CALL_EDGES_PER_PROGRAM {
            program.callers.insert(caller_id, 1);
        } else {
            checked_increment(
                &mut program.skipped_new_callers,
                "selected program skipped caller count",
            )?;
            checked_increment(
                &mut counters.skipped_new_call_edges,
                "skipped selected program call-edge count",
            )?;
        }
    }
    if let Some(index) = caller_selected {
        checked_increment(
            &mut counters.selected_callee_edge_observations,
            "selected program callee-edge observation count",
        )?;
        let program = &mut selected.programs[index];
        if let Some(count) = program.callees.get_mut(&callee_id) {
            checked_increment(count, "selected program callee-edge count")?;
        } else if program.callees.len() < MAX_PROGRAM_LOG_CALL_EDGES_PER_PROGRAM {
            program.callees.insert(callee_id, 1);
        } else {
            checked_increment(
                &mut program.skipped_new_callees,
                "selected program skipped callee count",
            )?;
            checked_increment(
                &mut counters.skipped_new_call_edges,
                "skipped selected program call-edge count",
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_staged_program_log_events(
    events: &[StagedProgramLogEvent],
    transaction_ordinal: u64,
    transaction: ProgramInventoryCoordinate,
    stack: &mut ProgramLogAttributionStack,
    selected: &mut SelectedProgramLogSet,
    counters: &mut ProgramLogInventoryCounters,
    holdouts: &mut Vec<ProgramLogHoldout>,
    text_attributions: &mut Vec<ProgramLogTextAttribution>,
) -> Result<()> {
    stack.begin_transaction();
    text_attributions.clear();
    let mut transaction_has_selected_explicit = false;
    let mut transaction_has_selected_unkeyed = false;
    let mut transaction_has_unattributed_unkeyed = false;

    for event in events {
        ensure!(
            usize::try_from(event.tag).is_ok_and(|tag| tag < COMPACT_LOG_EVENT_TAG_COUNT),
            "compact log event tag is outside the known schema"
        );
        checked_increment(&mut counters.compact_log_events, "compact log event count")?;
        let event_coordinate = Some((event.ordinal, event.tag));
        let mut explicit_selected = None;
        let mut contextual_selected = None;
        let event_program = match event.kind {
            StagedProgramLogEventKind::Explicit { program }
            | StagedProgramLogEventKind::Invoke { program, .. }
            | StagedProgramLogEventKind::Terminal { program }
            | StagedProgramLogEventKind::AmbiguousCustomFailure { program } => Some(program),
            StagedProgramLogEventKind::Neutral
            | StagedProgramLogEventKind::Unkeyed
            | StagedProgramLogEventKind::Truncated => None,
        };
        let raw_program = matches!(event_program, Some(CompactPubkey::Raw(_)));
        if raw_program {
            checked_increment(
                &mut counters.inline_raw_program_references,
                "inline raw program reference count",
            )?;
            record_program_log_holdout(
                counters,
                holdouts,
                transaction,
                event_coordinate,
                ProgramLogHoldoutReason::InlineRawProgramReference,
                stack.frames.len(),
                event_program,
            )?;
        }

        match event.kind {
            StagedProgramLogEventKind::Neutral => {}
            StagedProgramLogEventKind::Unkeyed => {
                checked_increment(
                    &mut counters.unkeyed_evidence_events,
                    "unkeyed log-evidence count",
                )?;
                if stack.clean {
                    if let Some(&program) = stack.frames.last() {
                        contextual_selected = selected.selected_index(program);
                        if let Some(index) = contextual_selected {
                            selected.programs[index]
                                .record_unkeyed(transaction_ordinal, event.tag)?;
                            checked_increment(
                                &mut counters.selected_attributed_unkeyed_events,
                                "selected attributed unkeyed log count",
                            )?;
                            transaction_has_selected_unkeyed = true;
                        } else {
                            checked_increment(
                                &mut counters.unselected_attributed_unkeyed_events,
                                "unselected attributed unkeyed log count",
                            )?;
                        }
                    } else {
                        checked_increment(
                            &mut counters.unattributed_unkeyed_events,
                            "unattributed unkeyed log count",
                        )?;
                        transaction_has_unattributed_unkeyed = true;
                        record_program_log_holdout(
                            counters,
                            holdouts,
                            transaction,
                            event_coordinate,
                            ProgramLogHoldoutReason::UnkeyedWithoutActiveFrame,
                            0,
                            None,
                        )?;
                    }
                } else {
                    checked_increment(
                        &mut counters.unattributed_unkeyed_events,
                        "unattributed unkeyed log count",
                    )?;
                    transaction_has_unattributed_unkeyed = true;
                    record_program_log_holdout(
                        counters,
                        holdouts,
                        transaction,
                        event_coordinate,
                        ProgramLogHoldoutReason::UnkeyedWhileStackDirty,
                        stack.frames.len(),
                        stack.frames.last().copied(),
                    )?;
                }
            }
            StagedProgramLogEventKind::Explicit { program } => {
                explicit_selected = record_explicit_program_log_evidence(
                    selected,
                    counters,
                    transaction_ordinal,
                    event.tag,
                    program,
                )?;
            }
            StagedProgramLogEventKind::Invoke { program, depth } => {
                checked_increment(&mut counters.invoke_events, "program invoke event count")?;
                if stack.clean
                    && stack.complete_prefix
                    && stack.suffix_usable
                    && !raw_program
                    && depth.is_none_or(|depth| {
                        usize::from(depth) == stack.frames.len().saturating_add(1)
                    })
                    && let Some(&caller) = stack.frames.last()
                {
                    record_program_log_call_edge(selected, counters, caller, program)?;
                }
                explicit_selected = record_explicit_program_log_evidence(
                    selected,
                    counters,
                    transaction_ordinal,
                    event.tag,
                    program,
                )?;
                if raw_program {
                    stack.clean = false;
                    stack.complete_prefix = false;
                    text_attributions.push(ProgramLogTextAttribution {
                        explicit_selected: explicit_selected
                            .map(u32::try_from)
                            .transpose()
                            .context("selected explicit program index exceeds u32")?,
                        contextual_selected: None,
                    });
                    transaction_has_selected_explicit |= explicit_selected.is_some();
                    continue;
                }
                match depth {
                    None if stack.clean && stack.complete_prefix => stack.frames.push(program),
                    None => {
                        record_program_log_holdout(
                            counters,
                            holdouts,
                            transaction,
                            event_coordinate,
                            ProgramLogHoldoutReason::BpfInvokeWhileStackDirty,
                            stack.frames.len(),
                            Some(program),
                        )?;
                        stack.clean = false;
                    }
                    Some(0) => {
                        record_program_log_holdout(
                            counters,
                            holdouts,
                            transaction,
                            event_coordinate,
                            ProgramLogHoldoutReason::InvokeDepthZero,
                            stack.frames.len(),
                            Some(program),
                        )?;
                        stack.frames.clear();
                        stack.clean = false;
                        stack.complete_prefix = false;
                    }
                    Some(depth) => {
                        let depth = usize::from(depth);
                        let expected = stack.frames.len().saturating_add(1);
                        if depth == 1 {
                            if !stack.clean || !stack.complete_prefix || expected != 1 {
                                record_program_log_holdout(
                                    counters,
                                    holdouts,
                                    transaction,
                                    event_coordinate,
                                    ProgramLogHoldoutReason::InvokeDepthMismatch,
                                    stack.frames.len(),
                                    Some(program),
                                )?;
                                if stack.suffix_usable {
                                    checked_increment(
                                        &mut counters.stack_resynchronizations,
                                        "program log stack resynchronization count",
                                    )?;
                                }
                            }
                            stack.frames.clear();
                            stack.frames.push(program);
                            stack.clean = stack.suffix_usable;
                            stack.complete_prefix = true;
                        } else if stack.complete_prefix && depth <= expected {
                            if !stack.clean || depth != expected {
                                record_program_log_holdout(
                                    counters,
                                    holdouts,
                                    transaction,
                                    event_coordinate,
                                    ProgramLogHoldoutReason::InvokeDepthMismatch,
                                    stack.frames.len(),
                                    Some(program),
                                )?;
                                if stack.suffix_usable {
                                    checked_increment(
                                        &mut counters.stack_resynchronizations,
                                        "program log stack resynchronization count",
                                    )?;
                                }
                            }
                            stack.frames.truncate(depth - 1);
                            stack.frames.push(program);
                            stack.clean = stack.suffix_usable;
                        } else {
                            record_program_log_holdout(
                                counters,
                                holdouts,
                                transaction,
                                event_coordinate,
                                ProgramLogHoldoutReason::InvokeDepthMismatch,
                                stack.frames.len(),
                                Some(program),
                            )?;
                            stack.frames.clear();
                            stack.frames.push(program);
                            stack.clean = false;
                            stack.complete_prefix = false;
                        }
                    }
                }
            }
            StagedProgramLogEventKind::Terminal { program } => {
                checked_increment(
                    &mut counters.definite_terminal_events,
                    "definite program terminal event count",
                )?;
                explicit_selected = record_explicit_program_log_evidence(
                    selected,
                    counters,
                    transaction_ordinal,
                    event.tag,
                    program,
                )?;
                if raw_program {
                    stack.clean = false;
                    stack.complete_prefix = false;
                    text_attributions.push(ProgramLogTextAttribution {
                        explicit_selected: explicit_selected
                            .map(u32::try_from)
                            .transpose()
                            .context("selected explicit program index exceeds u32")?,
                        contextual_selected: None,
                    });
                    transaction_has_selected_explicit |= explicit_selected.is_some();
                    continue;
                }
                if let Some(index) = stack.frames.iter().rposition(|frame| *frame == program) {
                    let was_clean = stack.clean;
                    let matched_top = index + 1 == stack.frames.len();
                    if !matched_top {
                        record_program_log_holdout(
                            counters,
                            holdouts,
                            transaction,
                            event_coordinate,
                            ProgramLogHoldoutReason::TerminalProgramMismatch,
                            stack.frames.len(),
                            Some(program),
                        )?;
                    }
                    stack.frames.truncate(index);
                    if stack.complete_prefix {
                        stack.clean = stack.suffix_usable;
                        if !was_clean && stack.suffix_usable {
                            checked_increment(
                                &mut counters.stack_resynchronizations,
                                "program log stack resynchronization count",
                            )?;
                        }
                    }
                } else {
                    record_program_log_holdout(
                        counters,
                        holdouts,
                        transaction,
                        event_coordinate,
                        if stack.frames.is_empty() {
                            ProgramLogHoldoutReason::TerminalWithoutActiveFrame
                        } else {
                            ProgramLogHoldoutReason::TerminalProgramMismatch
                        },
                        stack.frames.len(),
                        Some(program),
                    )?;
                    stack.clean = false;
                }
            }
            StagedProgramLogEventKind::AmbiguousCustomFailure { program } => {
                checked_increment(
                    &mut counters.ambiguous_custom_failure_events,
                    "ambiguous custom-program-error event count",
                )?;
                explicit_selected = record_explicit_program_log_evidence(
                    selected,
                    counters,
                    transaction_ordinal,
                    event.tag,
                    program,
                )?;
                record_program_log_holdout(
                    counters,
                    holdouts,
                    transaction,
                    event_coordinate,
                    ProgramLogHoldoutReason::AmbiguousCustomProgramError,
                    stack.frames.len(),
                    Some(program),
                )?;
                stack.clean = false;
            }
            StagedProgramLogEventKind::Truncated => {
                checked_increment(
                    &mut counters.log_truncated_events,
                    "log-truncated event count",
                )?;
                record_program_log_holdout(
                    counters,
                    holdouts,
                    transaction,
                    event_coordinate,
                    ProgramLogHoldoutReason::LogTruncated,
                    stack.frames.len(),
                    stack.frames.last().copied(),
                )?;
                stack.clean = false;
                stack.suffix_usable = false;
            }
        }

        transaction_has_selected_explicit |= explicit_selected.is_some();
        text_attributions.push(ProgramLogTextAttribution {
            explicit_selected: explicit_selected
                .map(u32::try_from)
                .transpose()
                .context("selected explicit program index exceeds u32")?,
            contextual_selected: contextual_selected
                .map(u32::try_from)
                .transpose()
                .context("selected contextual program index exceeds u32")?,
        });
    }

    if !stack.frames.is_empty() {
        record_program_log_holdout(
            counters,
            holdouts,
            transaction,
            None,
            ProgramLogHoldoutReason::UnclosedFramesAtTransactionEnd,
            stack.frames.len(),
            stack.frames.last().copied(),
        )?;
    }
    if !stack.clean {
        record_program_log_holdout(
            counters,
            holdouts,
            transaction,
            None,
            ProgramLogHoldoutReason::DirtyStackAtTransactionEnd,
            stack.frames.len(),
            stack.frames.last().copied(),
        )?;
    }
    if transaction_has_selected_explicit {
        checked_increment(
            &mut counters.selected_explicit_program_evidence_transactions,
            "transaction with selected explicit log evidence count",
        )?;
    }
    if transaction_has_selected_unkeyed {
        checked_increment(
            &mut counters.selected_attributed_unkeyed_transactions,
            "transaction with selected attributed unkeyed logs count",
        )?;
    }
    if transaction_has_unattributed_unkeyed {
        checked_increment(
            &mut counters.transactions_with_unattributed_unkeyed_events,
            "transaction with unattributed unkeyed logs count",
        )?;
    }
    Ok(())
}

#[derive(Debug)]
struct ProgramCoverageTracker {
    input_sha256: [u8; 32],
    requested_keys: Vec<[u8; KEY_BYTES]>,
    present_in_registry: u64,
    identified_by_registry_id: Vec<u8>,
    counters: ProgramCoverageCounters,
    transaction_has_identified: bool,
    transaction_has_unidentified: bool,
}

impl ProgramCoverageTracker {
    fn record(
        &mut self,
        registry_id: u32,
        origin: ProgramInventoryOrigin,
        count: u64,
    ) -> Result<()> {
        ensure!(count != 0, "program coverage occurrence increment is zero");
        let identified = *self
            .identified_by_registry_id
            .get(usize::try_from(registry_id)?)
            .context("coverage program registry ID is outside the dump registry")?
            != 0;
        let (total, selected) = match (origin, identified) {
            (ProgramInventoryOrigin::Outer, true) => (
                &mut self.counters.outer_occurrences,
                &mut self.counters.identified_outer_occurrences,
            ),
            (ProgramInventoryOrigin::Outer, false) => (
                &mut self.counters.outer_occurrences,
                &mut self.counters.unidentified_outer_occurrences,
            ),
            (ProgramInventoryOrigin::Inner, true) => (
                &mut self.counters.inner_occurrences,
                &mut self.counters.identified_inner_occurrences,
            ),
            (ProgramInventoryOrigin::Inner, false) => (
                &mut self.counters.inner_occurrences,
                &mut self.counters.unidentified_inner_occurrences,
            ),
        };
        *total = total
            .checked_add(count)
            .context("coverage total occurrence count overflow")?;
        *selected = selected
            .checked_add(count)
            .context("coverage selected occurrence count overflow")?;
        self.transaction_has_identified |= identified;
        self.transaction_has_unidentified |= !identified;
        Ok(())
    }

    fn finish_transaction(&mut self) -> Result<()> {
        checked_increment(
            &mut self.counters.transactions,
            "coverage transaction count",
        )?;
        match (
            self.transaction_has_identified,
            self.transaction_has_unidentified,
        ) {
            (true, false) => {
                checked_increment(
                    &mut self.counters.fully_covered_transactions,
                    "fully covered transaction count",
                )?;
                checked_increment(
                    &mut self.counters.touched_transactions,
                    "touched transaction count",
                )?;
            }
            (true, true) => {
                checked_increment(
                    &mut self.counters.partially_covered_transactions,
                    "partially covered transaction count",
                )?;
                checked_increment(
                    &mut self.counters.touched_transactions,
                    "touched transaction count",
                )?;
            }
            (false, true) => checked_increment(
                &mut self.counters.uncovered_transactions,
                "uncovered transaction count",
            )?,
            (false, false) => {
                // An instruction-free transaction needs no program decoder.
                checked_increment(
                    &mut self.counters.fully_covered_transactions,
                    "fully covered transaction count",
                )?;
                checked_increment(
                    &mut self.counters.transactions_without_instructions,
                    "instruction-free transaction count",
                )?;
            }
        }
        self.transaction_has_identified = false;
        self.transaction_has_unidentified = false;
        Ok(())
    }
}

fn validate_inventory_message_summary(
    message: &ProjectedArchiveV2MessageAccountSummary,
    flags: u32,
    signature_count: u8,
) -> Result<()> {
    ensure!(signature_count != 0, "transaction has no signatures");
    ensure!(
        message.num_required_signatures == signature_count,
        "message signature count differs from transaction"
    );
    ensure!(
        message.is_v0 == (flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
        "message version differs from transaction flags"
    );
    ensure!(
        message.has_compact_vote_instruction
            == (flags & ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX != 0),
        "compact-vote presence differs from transaction flags"
    );
    Ok(())
}

fn validate_inventory_metadata_summary(
    metadata: &ProjectedArchiveV2TokenMetadataSummary,
    message: &ProjectedArchiveV2MessageAccountSummary,
    flags: u32,
) -> Result<()> {
    let has_token_balances =
        metadata.pre_token_balance_count != 0 || metadata.post_token_balance_count != 0;
    let has_loaded = metadata.loaded_writable_count != 0 || metadata.loaded_readonly_count != 0;
    ensure!(
        metadata.has_error == (flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
            && metadata.inner_instructions_present
                == (flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0)
            && metadata.logs_present == (flags & ARCHIVE_V2_TX_FLAG_HAS_LOGS != 0)
            && has_token_balances == (flags & ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES != 0)
            && metadata.return_data_present == (flags & ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA != 0),
        "metadata facts differ from transaction flags"
    );
    ensure!(
        metadata.pre_balance_count == metadata.post_balance_count
            && (metadata.pre_balance_count == 0
                || metadata.pre_balance_count >= message.minimum_balance_accounts),
        "metadata balances cannot cover the writable message-account prefix"
    );
    ensure!(
        metadata.loaded_writable_count == message.expected_loaded_writable
            && metadata.loaded_readonly_count == message.expected_loaded_readonly
            && has_loaded == (flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0),
        "loaded addresses differ from message lookups or transaction flags"
    );
    Ok(())
}

fn validate_inventory_absent_metadata(
    message: &ProjectedArchiveV2MessageAccountSummary,
    flags: u32,
) -> Result<()> {
    const METADATA_DERIVED_FLAGS: u32 = ARCHIVE_V2_TX_FLAG_HAS_ERROR
        | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
        | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
        | ARCHIVE_V2_TX_FLAG_HAS_LOGS
        | ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA
        | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
    ensure!(
        flags & METADATA_DERIVED_FLAGS == 0,
        "transaction declares metadata facts without metadata"
    );
    ensure!(
        message.expected_loaded_writable == 0 && message.expected_loaded_readonly == 0,
        "message needs loaded addresses but metadata is absent"
    );
    Ok(())
}

#[derive(Clone, Copy)]
struct ProgramInventoryMetadataContext<'a> {
    registry_entries: u32,
    flags: u32,
    target_accounts: Option<&'a ProgramInventoryTargetAccounts>,
}

fn parse_inventory_metadata_stage(
    stage: &mut MetadataProgramStage,
    bytes: &[u8],
    error_schema: ArchiveV2WireMetadataErrorSchema,
    message: &ProjectedArchiveV2MessageAccountSummary,
    static_ids: &[u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    context: ProgramInventoryMetadataContext<'_>,
) -> Result<()> {
    stage.begin();
    let total_accounts = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("resolved message-account count overflow")?;
    ensure!(
        total_accounts <= blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS,
        "resolved message-account count exceeds its format cap"
    );
    let programs = &mut stage.programs;
    let account_indices = &mut stage.account_indices;
    let loaded_ids = &mut stage.loaded_ids;
    let loaded_generation = &mut stage.loaded_generation;
    let generation = stage.generation;
    let mut invalid_loaded_key = false;
    let summary = visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
        bytes,
        error_schema,
        ArchiveV2MetadataProjectionLimits {
            total_message_accounts: total_accounts,
            top_level_instruction_count: message.instruction_count,
        },
        context.registry_entries,
        LogPayloadValidation::StructureOnly,
        |_, instruction: BorrowedArchiveV2InnerTokenInstruction<'_>| {
            let account_start = account_indices.len();
            account_indices.extend_from_slice(instruction.accounts);
            programs.push(StagedInnerProgram {
                registry_id_or_account_index: instruction.program_id_index,
                location: InnerProgramLocation::Static,
                account_start,
                account_len: instruction.accounts.len(),
                target_mint_references: 0,
                target_token_account_references: 0,
            });
        },
        |_, _| {},
        |side, ordinal, reference| {
            let absolute = match side {
                ArchiveV2LoadedAddressSide::Writable => {
                    message.static_account_count.checked_add(ordinal)
                }
                ArchiveV2LoadedAddressSide::Readonly => message
                    .static_account_count
                    .checked_add(message.expected_loaded_writable)
                    .and_then(|start| start.checked_add(ordinal)),
            };
            let Some(absolute) = absolute.filter(|index| *index < loaded_ids.len()) else {
                invalid_loaded_key = true;
                return;
            };
            match reference {
                CompactPubkey::Id(id) if id != 0 && id <= context.registry_entries => {
                    loaded_ids[absolute] = id;
                    loaded_generation[absolute] = generation;
                }
                CompactPubkey::Id(_) | CompactPubkey::Raw(_) => invalid_loaded_key = true,
            }
        },
    )?;
    ensure!(
        !invalid_loaded_key,
        "metadata contains an unresolved loaded key"
    );
    ensure!(
        summary.inner_instruction_count == programs.len(),
        "inner-instruction callback count differs from metadata summary"
    );
    validate_inventory_metadata_summary(&summary, message, context.flags)?;

    let writable_end = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .context("loaded writable account boundary overflow")?;
    for program in programs {
        let account_index = usize::try_from(program.registry_id_or_account_index)
            .context("inner program account index exceeds usize")?;
        let (registry_id, location) = if account_index < message.static_account_count {
            (static_ids[account_index], InnerProgramLocation::Static)
        } else {
            ensure!(
                account_index < total_accounts && loaded_generation[account_index] == generation,
                "inner program account index was not resolved"
            );
            (
                loaded_ids[account_index],
                if account_index < writable_end {
                    InnerProgramLocation::LoadedWritable
                } else {
                    InnerProgramLocation::LoadedReadonly
                },
            )
        };
        ensure!(
            registry_id != 0 && registry_id <= context.registry_entries,
            "resolved inner program ID is outside the dump registry"
        );
        program.registry_id_or_account_index = registry_id;
        program.location = location;
        if let Some(target_accounts) = context.target_accounts {
            let account_end = program
                .account_start
                .checked_add(program.account_len)
                .context("inner instruction account range overflow")?;
            let instruction_accounts = account_indices
                .get(program.account_start..account_end)
                .context("inner instruction account range is outside its stage")?;
            for &instruction_account_index in instruction_accounts {
                let id = resolve_inventory_message_account_id(
                    usize::from(instruction_account_index),
                    message,
                    static_ids,
                    loaded_ids,
                    loaded_generation,
                    generation,
                )?;
                if id == target_accounts.target_mint_id {
                    checked_increment(
                        &mut program.target_mint_references,
                        "staged target-mint inner reference count",
                    )?;
                } else if target_accounts
                    .token_account_by_registry_id
                    .get(usize::try_from(id)?)
                    .is_some_and(|present| *present != 0)
                {
                    checked_increment(
                        &mut program.target_token_account_references,
                        "staged target-token-account inner reference count",
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn resolve_inventory_message_account_id(
    account_index: usize,
    message: &ProjectedArchiveV2MessageAccountSummary,
    static_ids: &[u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    loaded_ids: &[u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    loaded_generation: &[u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    generation: u32,
) -> Result<u32> {
    if account_index < message.static_account_count {
        return static_ids
            .get(account_index)
            .copied()
            .filter(|id| *id != 0)
            .context("instruction static account index was not resolved");
    }
    let total_accounts = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("resolved message-account count overflow")?;
    ensure!(
        account_index < total_accounts && loaded_generation.get(account_index) == Some(&generation),
        "instruction loaded account index was not resolved"
    );
    loaded_ids
        .get(account_index)
        .copied()
        .filter(|id| *id != 0)
        .context("instruction loaded account ID is zero")
}

fn resolved_inner_sequences_equal(
    left: &[StagedInnerProgram],
    right: &[StagedInnerProgram],
) -> bool {
    left == right
}

fn checked_increment(value: &mut u64, label: &'static str) -> Result<()> {
    *value = value
        .checked_add(1)
        .with_context(|| format!("{} overflow", label))?;
    Ok(())
}

fn record_inner_occurrence(
    table: &mut ProgramAccumulatorTable,
    counters: &mut ProgramInventoryCounters,
    program: StagedInnerProgram,
    transaction_ordinal: u64,
    coordinate: ProgramInventoryCoordinate,
) -> Result<()> {
    table.record(
        program.registry_id_or_account_index,
        transaction_ordinal,
        coordinate,
        ProgramInventoryOrigin::Inner,
    )?;
    checked_increment(&mut counters.inner_occurrences, "inner occurrence count")?;
    match program.location {
        InnerProgramLocation::Static => checked_increment(
            &mut counters.inner_static_resolutions,
            "inner static resolution count",
        ),
        InnerProgramLocation::LoadedWritable => checked_increment(
            &mut counters.inner_loaded_writable_resolutions,
            "inner loaded-writable resolution count",
        ),
        InnerProgramLocation::LoadedReadonly => checked_increment(
            &mut counters.inner_loaded_readonly_resolutions,
            "inner loaded-readonly resolution count",
        ),
    }?;
    let target_account_references = program
        .target_mint_references
        .checked_add(program.target_token_account_references)
        .context("target-account inner reference count overflow")?;
    if target_account_references == 0 {
        return Ok(());
    }
    let first_program_target_occurrence_in_transaction = table.record_target_account_inner(
        program.registry_id_or_account_index,
        transaction_ordinal,
        program.target_mint_references,
        program.target_token_account_references,
    )?;
    checked_increment(
        &mut counters.target_account_inner_occurrences,
        "target-account inner occurrence count",
    )?;
    if first_program_target_occurrence_in_transaction {
        checked_increment(
            &mut counters.target_account_inner_transactions,
            "target-account inner program-transaction count",
        )?;
    }
    if program.target_mint_references != 0 {
        checked_increment(
            &mut counters.target_mint_inner_occurrences,
            "target-mint inner occurrence count",
        )?;
    }
    if program.target_token_account_references != 0 {
        checked_increment(
            &mut counters.target_token_account_inner_occurrences,
            "target-token-account inner occurrence count",
        )?;
    }
    counters.target_account_inner_references = counters
        .target_account_inner_references
        .checked_add(target_account_references)
        .context("target-account inner reference count overflow")?;
    counters.target_mint_inner_references = counters
        .target_mint_inner_references
        .checked_add(program.target_mint_references)
        .context("target-mint inner reference count overflow")?;
    counters.target_token_account_inner_references = counters
        .target_token_account_inner_references
        .checked_add(program.target_token_account_references)
        .context("target-token-account inner reference count overflow")?;
    Ok(())
}

fn registry_key_at(registry: &[u8], registry_id: u32) -> Result<[u8; KEY_BYTES]> {
    let row = usize::try_from(
        registry_id
            .checked_sub(1)
            .context("registry ID zero is reserved")?,
    )?;
    let start = row
        .checked_mul(KEY_BYTES)
        .context("registry byte offset overflow")?;
    let end = start
        .checked_add(KEY_BYTES)
        .context("registry byte range overflow")?;
    registry
        .get(start..end)
        .context("registry ID is outside registry bytes")?
        .try_into()
        .map_err(|_| anyhow::anyhow!("registry row is not 32 bytes"))
}

fn registry_id_for_key(registry: &[u8], key: &[u8; KEY_BYTES]) -> Option<u32> {
    let mut left = 0usize;
    let mut right = registry.len() / KEY_BYTES;
    while left < right {
        let middle = left + (right - left) / 2;
        let start = middle * KEY_BYTES;
        match registry[start..start + KEY_BYTES].cmp(key) {
            Ordering::Less => left = middle + 1,
            Ordering::Greater => right = middle,
            Ordering::Equal => return u32::try_from(middle + 1).ok(),
        }
    }
    None
}

fn load_program_coverage_tracker(
    identified_programs: &Path,
    registry: &[u8],
    registry_entries: u32,
) -> Result<ProgramCoverageTracker> {
    let bytes = read_bounded_regular(identified_programs, MAX_IDENTIFIED_PROGRAM_SET_BYTES)?;
    let text = std::str::from_utf8(&bytes).context("identified program set is not UTF-8")?;
    let mut requested_keys = Vec::new();
    for (line_index, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        requested_keys.push(parse_pubkey(
            line,
            &format!("identified program at line {}", line_index + 1),
        )?);
    }
    ensure!(
        !requested_keys.is_empty(),
        "identified program set contains no program IDs"
    );
    requested_keys.sort_unstable();
    ensure!(
        requested_keys.windows(2).all(|pair| pair[0] != pair[1]),
        "identified program set contains a duplicate program ID"
    );

    let dense_len = usize::try_from(registry_entries)?
        .checked_add(1)
        .context("coverage registry flag length overflow")?;
    let mut identified_by_registry_id = Vec::new();
    identified_by_registry_id
        .try_reserve_exact(dense_len)
        .context("reserve coverage registry flags")?;
    identified_by_registry_id.resize(dense_len, 0);
    let mut present_in_registry = 0u64;
    for key in &requested_keys {
        if let Some(registry_id) = registry_id_for_key(registry, key) {
            identified_by_registry_id[usize::try_from(registry_id)?] = 1;
            checked_increment(
                &mut present_in_registry,
                "identified programs present in registry count",
            )?;
        }
    }
    Ok(ProgramCoverageTracker {
        input_sha256: sha256_bytes(&bytes),
        requested_keys,
        present_in_registry,
        identified_by_registry_id,
        counters: ProgramCoverageCounters::default(),
        transaction_has_identified: false,
        transaction_has_unidentified: false,
    })
}

fn load_selected_program_log_set(
    programs_path: &Path,
    registry: &[u8],
    registry_entries: u32,
) -> Result<SelectedProgramLogSet> {
    let bytes = read_bounded_regular(programs_path, MAX_IDENTIFIED_PROGRAM_SET_BYTES)?;
    let text = std::str::from_utf8(&bytes).context("selected program set is not UTF-8")?;
    let mut requested_keys = Vec::new();
    for (line_index, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        requested_keys.push(parse_pubkey(
            line,
            &format!("selected program at line {}", line_index + 1),
        )?);
    }
    ensure!(
        !requested_keys.is_empty(),
        "selected program set contains no program IDs"
    );
    requested_keys.sort_unstable();
    ensure!(
        requested_keys.windows(2).all(|pair| pair[0] != pair[1]),
        "selected program set contains a duplicate program ID"
    );

    let dense_len = usize::try_from(registry_entries)?
        .checked_add(1)
        .context("selected program registry-index length overflow")?;
    let mut by_registry_id = Vec::new();
    by_registry_id
        .try_reserve_exact(dense_len)
        .context("reserve selected program registry index")?;
    by_registry_id.resize(dense_len, PROGRAM_ACCUMULATOR_MISSING);
    let mut programs = Vec::new();
    programs
        .try_reserve_exact(requested_keys.len())
        .context("reserve selected log programs")?;
    let mut programs_present_in_registry = 0u64;
    for raw_key in requested_keys {
        let registry_id = registry_id_for_key(registry, &raw_key);
        let index = u32::try_from(programs.len()).context("selected program count exceeds u32")?;
        if let Some(id) = registry_id {
            by_registry_id[usize::try_from(id)?] = index;
            checked_increment(
                &mut programs_present_in_registry,
                "selected programs present in registry count",
            )?;
        }
        programs.push(ProgramLogAccumulator::new(raw_key, registry_id));
    }
    Ok(SelectedProgramLogSet {
        input_sha256: sha256_bytes(&bytes),
        by_registry_id,
        programs,
        programs_present_in_registry,
    })
}

fn serialize_program_log_call_edges(
    edges: HashMap<u32, u64>,
    registry: &[u8],
) -> Result<Vec<ProgramLogCallEdgeReport>> {
    let mut output = edges
        .into_iter()
        .map(|(registry_id, invokes)| {
            Ok(ProgramLogCallEdgeReport {
                registry_id,
                program_id: bs58::encode(registry_key_at(registry, registry_id)?).into_string(),
                invokes,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    output.sort_unstable_by_key(|edge| edge.registry_id);
    Ok(output)
}

fn serialize_program_log_program(
    mut program: ProgramLogAccumulator,
    registry: &[u8],
) -> Result<ProgramLogProgramReport> {
    let mut text_patterns = program
        .text_patterns
        .drain()
        .map(|(mut key, value)| {
            ensure!(
                key.as_bytes().get(1) == Some(&b'|') && key.as_bytes().get(3) == Some(&b'|'),
                "program log pattern key prefix is invalid"
            );
            key.drain(..4);
            Ok(ProgramLogTextPatternReport {
                trust_lane: value.trust_lane,
                text_kind: value.kind,
                pattern: key,
                example: value.example,
                events: value.events,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    text_patterns.sort_unstable_by(|left, right| {
        left.trust_lane
            .cmp(&right.trust_lane)
            .then_with(|| left.text_kind.cmp(&right.text_kind))
            .then_with(|| right.events.cmp(&left.events))
            .then_with(|| left.pattern.cmp(&right.pattern))
    });
    let callers = serialize_program_log_call_edges(program.callers, registry)?;
    let callees = serialize_program_log_call_edges(program.callees, registry)?;
    Ok(ProgramLogProgramReport {
        registry_id: program.registry_id,
        program_id: bs58::encode(program.raw_key).into_string(),
        raw_pubkey_hex: hex_digest(program.raw_key),
        explicit_id_evidence: ProgramLogEvidenceClass {
            events: program.explicit_evidence_events,
            transactions: program.explicit_evidence_transactions,
            event_kinds: serialize_program_log_event_counts(&program.explicit_event_tags)?,
        },
        attributed_unkeyed_evidence: ProgramLogEvidenceClass {
            events: program.attributed_unkeyed_events,
            transactions: program.attributed_unkeyed_transactions,
            event_kinds: serialize_program_log_event_counts(&program.unkeyed_event_tags)?,
        },
        text_pattern_observations: program.text_pattern_observations,
        skipped_new_text_patterns: program.skipped_new_text_patterns,
        text_patterns,
        skipped_new_callers: program.skipped_new_callers,
        skipped_new_callees: program.skipped_new_callees,
        callers,
        callees,
    })
}

fn sort_program_accumulators(programs: &mut [ProgramAccumulator], registry: &[u8]) {
    programs.sort_unstable_by(|left, right| {
        let left_total = left
            .outer_occurrences
            .checked_add(left.inner_occurrences)
            .expect("validated program count");
        let right_total = right
            .outer_occurrences
            .checked_add(right.inner_occurrences)
            .expect("validated program count");
        right_total.cmp(&left_total).then_with(|| {
            let left_key =
                registry_key_at(registry, left.registry_id).expect("validated program registry ID");
            let right_key = registry_key_at(registry, right.registry_id)
                .expect("validated program registry ID");
            left_key.cmp(&right_key)
        })
    });
}

fn sort_unknown_program_accumulators(programs: &mut [ProgramAccumulator], registry: &[u8]) {
    programs.sort_unstable_by(|left, right| {
        right
            .transactions
            .cmp(&left.transactions)
            .then_with(|| {
                right
                    .total_occurrences()
                    .expect("validated program count")
                    .cmp(&left.total_occurrences().expect("validated program count"))
            })
            .then_with(|| {
                let left_key = registry_key_at(registry, left.registry_id)
                    .expect("validated program registry ID");
                let right_key = registry_key_at(registry, right.registry_id)
                    .expect("validated program registry ID");
                left_key.cmp(&right_key)
            })
    });
}

fn serialize_program_accumulator(
    program: ProgramAccumulator,
    registry: &[u8],
) -> Result<ProgramInventoryProgram> {
    let raw_key = registry_key_at(registry, program.registry_id)?;
    Ok(ProgramInventoryProgram {
        registry_id: program.registry_id,
        program_id: bs58::encode(raw_key).into_string(),
        raw_pubkey_hex: hex_digest(raw_key),
        total_occurrences: program.total_occurrences()?,
        outer_occurrences: program.outer_occurrences,
        inner_occurrences: program.inner_occurrences,
        transactions: program.transactions,
        outer_transactions: program.outer_transactions,
        inner_transactions: program.inner_transactions,
        first_transaction: program.first_transaction,
        first_origin: program.first_origin,
    })
}

fn serialize_program_inventory_target_accumulator(
    program: ProgramAccumulator,
    registry: &[u8],
) -> Result<ProgramInventoryTargetProgram> {
    Ok(ProgramInventoryTargetProgram {
        general: serialize_program_accumulator(program, registry)?,
        target_account_inner_occurrences: program.target_account_inner_occurrences,
        target_account_inner_transactions: program.target_account_inner_transactions,
        target_mint_inner_occurrences: program.target_mint_inner_occurrences,
        target_token_account_inner_occurrences: program.target_token_account_inner_occurrences,
        target_account_inner_references: program.target_account_inner_references,
        target_mint_inner_references: program.target_mint_inner_references,
        target_token_account_inner_references: program.target_token_account_inner_references,
    })
}

fn commit_outer_programs(
    scratch: &ProgramInventoryScratch,
    table: &mut ProgramAccumulatorTable,
    counters: &mut ProgramInventoryCounters,
    transaction_ordinal: u64,
    coordinate: ProgramInventoryCoordinate,
) -> Result<()> {
    for &program_index in &scratch.outer_touched[..scratch.outer_touched_len] {
        let index = usize::from(program_index);
        let count = scratch.outer_counts[index];
        ensure!(count != 0, "touched outer program has zero occurrences");
        let registry_id = scratch.static_ids[index];
        table.record_count(
            registry_id,
            transaction_ordinal,
            coordinate,
            ProgramInventoryOrigin::Outer,
            count,
        )?;
        counters.outer_occurrences = counters
            .outer_occurrences
            .checked_add(count)
            .context("outer occurrence count overflow")?;
        counters.outer_static_resolutions = counters
            .outer_static_resolutions
            .checked_add(count)
            .context("outer static resolution count overflow")?;
    }
    Ok(())
}

fn publish_program_inventory_report(report: &Path, bytes: &[u8]) -> Result<PathBuf> {
    let raw_parent = report.parent().unwrap_or_else(|| Path::new("."));
    let raw_parent = if raw_parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        raw_parent
    };
    let parent = fs::canonicalize(raw_parent)
        .with_context(|| format!("resolve report directory {}", raw_parent.display()))?;
    ensure!(
        parent.is_dir(),
        "program inventory report parent is not a directory"
    );
    let file_name = report
        .file_name()
        .context("program inventory report path has no file name")?;
    let final_path = parent.join(file_name);
    ensure!(
        !final_path.exists(),
        "refusing to replace existing program inventory report {}",
        final_path.display()
    );

    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("system time is before the Unix epoch")?
        .as_nanos();
    let mut temporary = None;
    for attempt in 0..100u32 {
        let candidate = parent.join(format!(
            ".{}.program-inventory-{}-{nonce}-{attempt}.partial",
            file_name.to_string_lossy(),
            std::process::id(),
        ));
        match create_new_file(&candidate) {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if candidate.exists() => {
                drop(error);
            }
            Err(error) => return Err(error),
        }
    }
    let (temporary_path, file) =
        temporary.context("cannot create a unique program inventory temporary file")?;
    let result = (|| -> Result<()> {
        let mut writer = BufWriter::with_capacity(1 << 20, file);
        writer.write_all(bytes)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        // A hard link is an atomic create-without-replace publication.
        fs::hard_link(&temporary_path, &final_path).with_context(|| {
            format!(
                "publish program inventory {} as {}",
                temporary_path.display(),
                final_path.display()
            )
        })?;
        sync_directory(&parent)?;
        fs::remove_file(&temporary_path)
            .with_context(|| format!("remove {}", temporary_path.display()))?;
        sync_directory(&parent)?;
        Ok(())
    })();
    if result.is_err() && temporary_path.exists() {
        let _ = fs::remove_file(&temporary_path);
    }
    result?;
    Ok(final_path)
}

fn inventory_progress(
    analysis_name: &str,
    started: Instant,
    transactions: u64,
    total_transactions: u64,
    logical_bytes: u64,
) {
    let elapsed = started.elapsed().as_secs_f64();
    let percent = if total_transactions == 0 {
        100.0
    } else {
        transactions as f64 * 100.0 / total_transactions as f64
    };
    let mib_per_second = if elapsed == 0.0 {
        0.0
    } else {
        logical_bytes as f64 / (1 << 20) as f64 / elapsed
    };
    let eta = if transactions == 0 {
        0.0
    } else {
        elapsed * total_transactions.saturating_sub(transactions) as f64 / transactions as f64
    };
    eprintln!(
        "{analysis_name}: {transactions}/{total_transactions} transactions ({percent:.2}%), {mib_per_second:.1} MiB/s, {elapsed:.1}s elapsed, {eta:.1}s ETA"
    );
}

/// Build one exact outer-and-inner program inventory from a completed schema-3 dump.
pub fn inventory_consolidated_programs_v3(dump: &Path, report: &Path) -> Result<()> {
    build_program_analysis_v3(dump, None, report)
}

/// Build one metadata-derived SPYx balance, holder, volume, and RPC-cost report.
pub fn build_consolidated_token_history_report_v3(dump: &Path, report: &Path) -> Result<()> {
    token_report::build_consolidated_token_history_report_v3(dump, report)
}

/// Build one fail-closed, instruction-derived public-balance replay report.
pub fn replay_consolidated_spyx_balances_v3(
    dump: &Path,
    report: &Path,
    max_transactions: Option<u64>,
) -> Result<()> {
    spyx_replay::replay_consolidated_spyx_balances_v3(dump, report, max_transactions)
}

/// Stream owner-linked target transactions from the strict SPYx replay.
pub fn visit_consolidated_spyx_owner_postings_v3<F>(
    dump: &Path,
    max_transactions: Option<u64>,
    visit: F,
) -> Result<crate::consolidate::SpyxOwnerReplaySummary>
where
    F: FnMut(u64, &[u32]) -> Result<()>,
{
    spyx_replay::visit_consolidated_spyx_owner_postings_v3(dump, max_transactions, visit)
}

/// Stream owner-linked transactions and exact owner balance changes from the
/// strict SPYx replay.
pub fn visit_consolidated_spyx_owner_balance_history_v3<F>(
    dump: &Path,
    max_transactions: Option<u64>,
    visit: F,
) -> Result<crate::consolidate::SpyxOwnerReplaySummary>
where
    F: for<'a> FnMut(crate::consolidate::SpyxOwnerBalanceTransaction<'a>) -> Result<()>,
{
    spyx_replay::visit_consolidated_spyx_owner_balance_history_v3(dump, max_transactions, visit)
}

/// Measure exact DEX parser coverage over one completed consolidated dump.
pub fn measure_dex_parser_coverage_v3(dump: &Path, report: &Path) -> Result<()> {
    dex_coverage::measure_dex_parser_coverage_v3(dump, report)
}

/// Measure exact instruction and transaction coverage for a set of program IDs.
pub fn measure_identified_program_coverage_v3(
    dump: &Path,
    identified_programs: &Path,
    report: &Path,
) -> Result<()> {
    build_program_analysis_v3(dump, Some(identified_programs), report)
}

/// Inventory compact log evidence for a selected program set from one completed dump.
pub fn inventory_consolidated_program_logs_v3(
    dump: &Path,
    programs_path: &Path,
    report: &Path,
) -> Result<()> {
    let started = Instant::now();
    let dump = fs::canonicalize(dump)
        .with_context(|| format!("resolve consolidated dump {}", dump.display()))?;
    ensure!(dump.is_dir(), "consolidated dump is not a directory");
    validate_exact_final_files(&dump)?;

    let report_parent = report.parent().unwrap_or_else(|| Path::new("."));
    let report_parent = if report_parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        report_parent
    };
    let canonical_report_parent = fs::canonicalize(report_parent)
        .with_context(|| format!("resolve report directory {}", report_parent.display()))?;
    ensure!(
        canonical_report_parent != dump,
        "program log inventory report must not modify the immutable dump directory"
    );
    let report_name = report
        .file_name()
        .context("program log inventory report path has no file name")?;
    ensure!(
        !canonical_report_parent.join(report_name).exists(),
        "refusing to replace an existing program log inventory report"
    );

    let manifest_bytes =
        read_bounded_regular(&dump.join(DUMP_MANIFEST_FILE), MAX_ROOT_MANIFEST_BYTES)?;
    let manifest_sha256 = sha256_bytes(&manifest_bytes);
    let manifest: DumpManifest = serde_json::from_slice(&manifest_bytes)?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::Consolidated
            && manifest.complete
            && manifest.workers != 0
            && manifest.first_epoch <= manifest.last_epoch
            && manifest.transactions != 0,
        "invalid consolidated manifest header"
    );
    ensure!(
        manifest.transaction_stream == TRANSACTIONS_FILE
            && manifest.signature_stream.as_deref() == Some(DUMP_SIGNATURES_FILE)
            && manifest.pubkey_registry.as_deref() == Some(PUBKEY_REGISTRY_FILE)
            && manifest.discovered_accounts.as_deref() == Some(ACCOUNTS_FILE)
            && manifest.account_id_log.is_none()
            && manifest.account_id_log_sha256.is_none()
            && manifest.registry_maps.is_none(),
        "consolidated manifest file bindings differ"
    );
    validate_source_binding(&manifest.source_binding)?;
    let expected_transaction_sha256 = parse_hex_digest(
        manifest
            .transaction_stream_sha256
            .as_deref()
            .context("missing transaction digest")?,
        "transaction digest",
    )?;
    let expected_registry_sha256 = parse_hex_digest(
        manifest
            .pubkey_registry_sha256
            .as_deref()
            .context("missing registry digest")?,
        "registry digest",
    )?;
    let expected_signatures = manifest.signatures.context("missing signature count")?;
    let expected_registry_rows = manifest.pubkeys.context("missing public-key count")?;
    ensure!(
        expected_registry_rows != 0 && expected_registry_rows < u64::from(u32::MAX),
        "invalid registry row count"
    );
    let registry_entries = u32::try_from(expected_registry_rows)?;
    let expected_registry_bytes = expected_registry_rows
        .checked_mul(KEY_BYTES as u64)
        .context("registry byte length overflow")?;
    let registry = read_bounded_regular(&dump.join(PUBKEY_REGISTRY_FILE), expected_registry_bytes)?;
    ensure!(
        u64::try_from(registry.len())? == expected_registry_bytes,
        "registry size differs from its manifest"
    );
    let actual_registry_sha256 = sha256_bytes(&registry);
    ensure!(
        actual_registry_sha256 == expected_registry_sha256,
        "registry digest differs from its manifest"
    );
    ensure!(
        registry
            .chunks_exact(KEY_BYTES)
            .zip(registry.chunks_exact(KEY_BYTES).skip(1))
            .all(|(left, right)| left < right),
        "registry is not strictly sorted and unique"
    );
    let mut selected = load_selected_program_log_set(programs_path, &registry, registry_entries)?;

    let expected_signature_bytes = expected_signatures
        .checked_mul(SIGNATURE_BYTES as u64)
        .context("signature byte length overflow")?;
    let signature_metadata = fs::symlink_metadata(dump.join(DUMP_SIGNATURES_FILE))?;
    ensure!(
        signature_metadata.file_type().is_file()
            && signature_metadata.len() == expected_signature_bytes,
        "signature sidecar size differs from its manifest"
    );

    let target = TargetBinding {
        mint: parse_pubkey(&manifest.mint, "mint")?,
        mint_slot: manifest.mint_slot,
        mint_signature: parse_signature(&manifest.mint_signature)?,
    };
    let transaction_path = dump.join(TRANSACTIONS_FILE);
    let transaction_file = File::open(&transaction_path)?;
    let transaction_stamp = FileStamp::read(&transaction_file)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, transaction_file);
    let mut transaction_hasher = Sha256::new();
    let mut logical_offset = 0u64;
    let mut payload = Vec::new();
    read_frame_hashed(
        &mut reader,
        &mut logical_offset,
        &mut transaction_hasher,
        &mut payload,
    )?
    .context("consolidated transaction stream is empty")?;
    let BorrowedDumpRecord::Header(header) = decode_borrowed_frame(&payload)? else {
        bail!("consolidated transaction stream does not start with a header")
    };
    ensure!(
        header.schema_version == DUMP_SCHEMA_VERSION
            && header.stream_kind == DumpStreamKind::Consolidated
            && header.mint == target.mint
            && header.mint_slot == target.mint_slot
            && header.mint_signature == target.mint_signature
            && header.source_epoch.is_none()
            && header.source_generation_digest.is_none()
            && header.source_wire_profile.is_none()
            && header.pubkey_registry_id_base == PUBKEY_REGISTRY_ID_BASE,
        "consolidated stream header differs from its manifest"
    );

    let mut counters = ProgramLogInventoryCounters::default();
    let mut current_events = Vec::new();
    let mut legacy_events = Vec::new();
    let mut text_attributions = Vec::new();
    let mut stack = ProgramLogAttributionStack::default();
    let mut holdouts = Vec::new();
    holdouts
        .try_reserve_exact(MAX_PROGRAM_LOG_HOLDOUT_DETAILS)
        .context("reserve program log holdout details")?;
    let mut pattern_scratch = String::new();
    let mut signatures = 0u64;
    let mut previous_coordinate = None;
    let mut previous_slot = None::<(u64, u64, u32, BlockIdentity)>;
    let footer = loop {
        current_events.clear();
        legacy_events.clear();
        read_frame_hashed(
            &mut reader,
            &mut logical_offset,
            &mut transaction_hasher,
            &mut payload,
        )?
        .context("consolidated transaction stream has no footer")?;
        match decode_borrowed_frame(&payload)? {
            BorrowedDumpRecord::Header(_) => {
                bail!("consolidated transaction stream repeats its header")
            }
            BorrowedDumpRecord::Footer(footer) => break footer,
            BorrowedDumpRecord::Transaction(record) => {
                let coordinate = ProgramInventoryCoordinate::from_record(&record);
                ensure!(
                    previous_coordinate
                        .is_none_or(|previous| previous < coordinate.canonical_key()),
                    "consolidated transactions are not in canonical order"
                );
                previous_coordinate = Some(coordinate.canonical_key());
                ensure!(
                    (manifest.first_epoch..=manifest.last_epoch).contains(&record.source_epoch)
                        && record.block.parent_slot < record.block.slot
                        && record.block.transaction_count != 0
                        && record.tx_index < record.block.transaction_count
                        && record.signature_count != 0
                        && !record.message_bytes.is_empty()
                        && record.flags & !ARCHIVE_V2_TX_KNOWN_FLAGS == 0
                        && record.flags
                            & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                                | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
                            == 0,
                    "consolidated transaction has invalid source fields"
                );
                let DumpSourceBinding::TrustedLocalSizesOnly {
                    slots_per_epoch,
                    wire_profile,
                    ..
                } = &manifest.source_binding;
                let first_slot = record
                    .source_epoch
                    .checked_mul(*slots_per_epoch)
                    .context("source epoch first slot overflow")?;
                ensure!(
                    record.source_wire_profile == *wire_profile
                        && record.block.slot >= first_slot
                        && record.block.slot - first_slot < *slots_per_epoch
                        && u64::from(record.source_block_id) < *slots_per_epoch,
                    "consolidated transaction differs from its trusted source binding"
                );
                ensure!(
                    (record.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0)
                        == !record.metadata_bytes.is_empty()
                        && (record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
                            == (record.metadata_bytes.first() == Some(&1)),
                    "consolidated transaction flags differ from metadata bytes"
                );
                ensure!(
                    record.dump_signature_ordinal == Some(signatures),
                    "consolidated signature ordinals are not contiguous"
                );
                record
                    .source_first_signature_ordinal
                    .checked_add(u64::from(record.signature_count))
                    .context("source signature range overflow")?;
                let identity = BlockIdentity::from(&record.block);
                if let Some((epoch, slot, block_id, previous_identity)) = previous_slot
                    && epoch == record.source_epoch
                    && slot == record.block.slot
                {
                    ensure!(
                        block_id == record.source_block_id && previous_identity == identity,
                        "one source slot has conflicting block context"
                    );
                }
                previous_slot = Some((
                    record.source_epoch,
                    record.block.slot,
                    record.source_block_id,
                    identity,
                ));

                let mut static_count = 0usize;
                let mut invalid_static_key = false;
                let message = projector(record.source_wire_profile)
                    .visit_static_accounts_and_instructions_exact(
                        record.message_bytes,
                        registry_entries,
                        |ordinal, reference| {
                            static_count = ordinal + 1;
                            if !matches!(
                                reference,
                                CompactPubkey::Id(id) if id != 0 && id <= registry_entries
                            ) {
                                invalid_static_key = true;
                            }
                        },
                        |_| {},
                    )
                    .with_context(|| {
                        format!(
                            "decode message at epoch {} slot {} transaction {}",
                            record.source_epoch, record.block.slot, record.tx_index
                        )
                    })?;
                ensure!(
                    !invalid_static_key && static_count == message.static_account_count,
                    "message contains an unresolved static key"
                );
                validate_inventory_message_summary(&message, record.flags, record.signature_count)?;
                let limits = ArchiveV2MetadataProjectionLimits {
                    total_message_accounts: message
                        .static_account_count
                        .checked_add(message.expected_loaded_writable)
                        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
                        .context("total message account count overflow")?,
                    top_level_instruction_count: message.instruction_count,
                };

                let (selected_schema, summary) = if record.metadata_bytes.is_empty() {
                    checked_increment(&mut counters.metadata_absent, "metadata-absent count")?;
                    validate_inventory_absent_metadata(&message, record.flags)?;
                    (None, None)
                } else if record.metadata_bytes.first() == Some(&0) {
                    let summary = collect_program_log_stage(
                        &mut current_events,
                        record.metadata_bytes,
                        ArchiveV2WireMetadataErrorSchema::Current,
                        limits,
                        registry_entries,
                    )?;
                    checked_increment(
                        &mut counters.metadata_without_error,
                        "metadata-without-error count",
                    )?;
                    (
                        Some(ArchiveV2WireMetadataErrorSchema::Current),
                        Some(summary),
                    )
                } else {
                    let current_prefix_valid =
                        validate_archive_v2_metadata_error_prefix_for_selected_schema(
                            record.metadata_bytes,
                            ArchiveV2WireMetadataErrorSchema::Current,
                            record.metadata_bytes.len(),
                        )
                        .is_ok();
                    let legacy_prefix_valid =
                        validate_archive_v2_metadata_error_prefix_for_selected_schema(
                            record.metadata_bytes,
                            ArchiveV2WireMetadataErrorSchema::Legacy,
                            record.metadata_bytes.len(),
                        )
                        .is_ok();
                    let current_summary = current_prefix_valid
                        .then(|| {
                            collect_program_log_stage(
                                &mut current_events,
                                record.metadata_bytes,
                                ArchiveV2WireMetadataErrorSchema::Current,
                                limits,
                                registry_entries,
                            )
                        })
                        .transpose()
                        .ok()
                        .flatten();
                    let legacy_summary = legacy_prefix_valid
                        .then(|| {
                            collect_program_log_stage(
                                &mut legacy_events,
                                record.metadata_bytes,
                                ArchiveV2WireMetadataErrorSchema::Legacy,
                                limits,
                                registry_entries,
                            )
                        })
                        .transpose()
                        .ok()
                        .flatten();
                    ensure!(
                        current_summary.is_some() || legacy_summary.is_some(),
                        "metadata is invalid under both selected error schemas at epoch {} slot {} transaction {}",
                        record.source_epoch,
                        record.block.slot,
                        record.tx_index
                    );
                    match (current_summary, legacy_summary) {
                        (Some(summary), None) => {
                            checked_increment(
                                &mut counters.metadata_current_only,
                                "current-only metadata count",
                            )?;
                            (
                                Some(ArchiveV2WireMetadataErrorSchema::Current),
                                Some(summary),
                            )
                        }
                        (None, Some(summary)) => {
                            checked_increment(
                                &mut counters.metadata_legacy_only,
                                "legacy-only metadata count",
                            )?;
                            std::mem::swap(&mut current_events, &mut legacy_events);
                            (
                                Some(ArchiveV2WireMetadataErrorSchema::Legacy),
                                Some(summary),
                            )
                        }
                        (Some(current), Some(legacy)) => {
                            let current_tail =
                                validate_archive_v2_metadata_error_prefix_for_selected_schema(
                                    record.metadata_bytes,
                                    ArchiveV2WireMetadataErrorSchema::Current,
                                    record.metadata_bytes.len(),
                                )
                                .context("revalidate dual-valid current metadata prefix")?;
                            let legacy_tail =
                                validate_archive_v2_metadata_error_prefix_for_selected_schema(
                                    record.metadata_bytes,
                                    ArchiveV2WireMetadataErrorSchema::Legacy,
                                    record.metadata_bytes.len(),
                                )
                                .context("revalidate dual-valid legacy metadata prefix")?;
                            ensure!(
                                current_tail.bytes == legacy_tail.bytes
                                    && current_tail.error_index == legacy_tail.error_index
                                    && current == legacy
                                    && current_events == legacy_events,
                                "dual-valid metadata resolves to divergent metadata at epoch {} slot {} transaction {}",
                                record.source_epoch,
                                record.block.slot,
                                record.tx_index
                            );
                            checked_increment(
                                &mut counters.metadata_both_same_log_sequence,
                                "dual-valid same-log-sequence metadata count",
                            )?;
                            (
                                Some(ArchiveV2WireMetadataErrorSchema::Current),
                                Some(current),
                            )
                        }
                        (None, None) => unreachable!("both-invalid metadata was rejected"),
                    }
                };

                if let Some(summary) = summary {
                    validate_inventory_metadata_summary(&summary.metadata, &message, record.flags)?;
                    ensure!(
                        summary.has_error == (record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
                            && summary.logs_present
                                == (record.flags & ARCHIVE_V2_TX_FLAG_HAS_LOGS != 0),
                        "compact log summary differs from transaction flags"
                    );
                    if summary.logs_present {
                        checked_increment(
                            &mut counters.transactions_with_logs,
                            "transaction with compact logs count",
                        )?;
                    }
                }

                scan_staged_program_log_events(
                    &current_events,
                    counters.transactions,
                    coordinate,
                    &mut stack,
                    &mut selected,
                    &mut counters,
                    &mut holdouts,
                    &mut text_attributions,
                )?;
                if let Some(error_schema) =
                    selected_schema.filter(|_| summary.is_some_and(|value| value.logs_present))
                {
                    collect_selected_program_log_text_patterns(
                        record.metadata_bytes,
                        error_schema,
                        limits,
                        registry_entries,
                        &current_events,
                        &text_attributions,
                        &mut selected,
                        &mut counters,
                        &mut pattern_scratch,
                    )?;
                }

                checked_increment(&mut counters.transactions, "transaction count")?;
                signatures = signatures
                    .checked_add(u64::from(record.signature_count))
                    .context("signature count overflow")?;
                if counters
                    .transactions
                    .is_multiple_of(PROGRAM_INVENTORY_PROGRESS_TRANSACTIONS)
                {
                    inventory_progress(
                        "program log inventory",
                        started,
                        counters.transactions,
                        manifest.transactions,
                        logical_offset,
                    );
                }
            }
        }
    };
    ensure!(
        read_frame_hashed(
            &mut reader,
            &mut logical_offset,
            &mut transaction_hasher,
            &mut payload,
        )?
        .is_none(),
        "consolidated transaction stream has records after its footer"
    );
    let transaction_file = reader.into_inner();
    transaction_stamp.verify(&transaction_file, "consolidated transaction stream")?;
    verify_path_binding(
        &transaction_path,
        &transaction_stamp,
        "consolidated transaction stream",
    )?;
    ensure!(
        logical_offset == transaction_stamp.bytes,
        "transaction stream size changed while it was read"
    );
    let actual_transaction_sha256: [u8; 32] = transaction_hasher.finalize().into();
    ensure!(
        actual_transaction_sha256 == expected_transaction_sha256,
        "transaction digest differs from its manifest"
    );
    let epoch_count = manifest
        .last_epoch
        .checked_sub(manifest.first_epoch)
        .and_then(|span| span.checked_add(1))
        .context("manifest epoch count overflow")?;
    ensure!(
        counters.transactions == manifest.transactions
            && signatures == expected_signatures
            && footer.epochs == epoch_count
            && footer.transactions_written == counters.transactions
            && footer.transactions_scanned >= counters.transactions
            && footer.pubkeys == expected_registry_rows
            && footer.signatures == signatures
            && footer.owned_block_fallbacks <= footer.blocks_scanned
            && footer.raw_transaction_fallbacks == 0
            && footer.raw_metadata_fallbacks == 0,
        "consolidated stream counters differ from its manifest"
    );
    let metadata_records = counters
        .metadata_absent
        .checked_add(counters.metadata_without_error)
        .and_then(|count| count.checked_add(counters.metadata_current_only))
        .and_then(|count| count.checked_add(counters.metadata_legacy_only))
        .and_then(|count| count.checked_add(counters.metadata_both_same_log_sequence))
        .context("program log metadata classification count overflow")?;
    ensure!(
        metadata_records == counters.transactions,
        "program log metadata classifications do not cover every transaction"
    );

    let requested_programs = u64::try_from(selected.programs.len())?;
    let programs_with_explicit_id_evidence = u64::try_from(
        selected
            .programs
            .iter()
            .filter(|program| program.explicit_evidence_events != 0)
            .count(),
    )?;
    let programs_with_attributed_unkeyed_evidence = u64::try_from(
        selected
            .programs
            .iter()
            .filter(|program| program.attributed_unkeyed_events != 0)
            .count(),
    )?;
    let program_ids_absent_from_registry = selected
        .programs
        .iter()
        .filter(|program| program.registry_id.is_none())
        .map(|program| bs58::encode(program.raw_key).into_string())
        .collect();
    let selected_set = ProgramLogSelectedSetReport {
        input_sha256: hex_digest(selected.input_sha256),
        input_format: "one_base58_program_id_per_line_blank_and_hash_comment_lines_ignored",
        requested_programs,
        programs_present_in_registry: selected.programs_present_in_registry,
        programs_with_explicit_id_evidence,
        programs_with_attributed_unkeyed_evidence,
        program_ids_absent_from_registry,
    };
    let source = ProgramInventorySource {
        mint: manifest.mint,
        manifest_sha256: hex_digest(manifest_sha256),
        transaction_stream_sha256: hex_digest(actual_transaction_sha256),
        pubkey_registry_sha256: hex_digest(actual_registry_sha256),
        transactions: counters.transactions,
        signatures,
        registry_entries,
        first_epoch: manifest.first_epoch,
        last_epoch: manifest.last_epoch,
    };
    let explicit_id_evidence_complete = counters.inline_raw_program_references == 0;
    let unkeyed_attribution_complete = counters.unattributed_unkeyed_events == 0
        && counters.log_truncated_events == 0
        && counters.inline_raw_program_references == 0;
    let holdout_details_truncated = counters.holdout_diagnostics
        > u64::try_from(holdouts.len()).context("holdout detail count exceeds u64")?;
    let programs = selected
        .programs
        .into_iter()
        .map(|program| serialize_program_log_program(program, &registry))
        .collect::<Result<Vec<_>>>()?;
    let report_value = ProgramLogInventoryReport {
        schema_version: 1,
        artifact_kind: "program_log_inventory",
        complete: true,
        explicit_id_evidence_complete,
        unkeyed_attribution_complete,
        attribution_policy: "explicit_program_evidence_counts_program_emitters_and_runtime_frames; loader_and_deployment_status_subjects_are_neutral; unkeyed_program_evidence_requires_a_clean_validated_invocation_stack",
        custom_program_error_policy: "failure_custom_program_error_is_a_provenance_ambiguous_non_terminal; bpf_failure_custom_program_error_is_a_definite_terminal",
        program_order: "raw_pubkey_ascending",
        source,
        selected_set,
        counters,
        programs,
        holdout_details_limit: MAX_PROGRAM_LOG_HOLDOUT_DETAILS,
        holdout_details_truncated,
        holdouts,
    };
    let mut report_bytes = serde_json::to_vec_pretty(&report_value)?;
    report_bytes.push(b'\n');
    let report_sha256 = sha256_bytes(&report_bytes);
    let report_path = publish_program_inventory_report(report, &report_bytes)?;
    inventory_progress(
        "program log inventory",
        started,
        manifest.transactions,
        manifest.transactions,
        logical_offset,
    );
    eprintln!(
        "program log inventory complete: {} transactions, {} selected programs, {} explicit events, {} attributed unkeyed events, {} unattributed unkeyed events, {:.1}s elapsed, report_sha256={}, report={}",
        manifest.transactions,
        requested_programs,
        report_value
            .counters
            .selected_explicit_program_evidence_events,
        report_value.counters.selected_attributed_unkeyed_events,
        report_value.counters.unattributed_unkeyed_events,
        started.elapsed().as_secs_f64(),
        hex_digest(report_sha256),
        report_path.display(),
    );
    Ok(())
}

fn load_program_inventory_target_accounts(
    dump: &Path,
    manifest: &DumpManifest,
    target: &TargetBinding,
    registry: &[u8],
    registry_entries: u32,
) -> Result<ProgramInventoryTargetAccounts> {
    let expected_sha256 = parse_hex_digest(
        manifest
            .discovered_accounts_sha256
            .as_deref()
            .context("missing discovered-account digest")?,
        "discovered-account digest",
    )?;
    let bytes = read_bounded_regular(
        &dump.join(ACCOUNTS_FILE),
        ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES as u64,
    )?;
    let actual_sha256 = sha256_bytes(&bytes);
    ensure!(
        actual_sha256 == expected_sha256,
        "discovered-account artifact digest differs from its manifest"
    );
    let accounts: DiscoveredAccountList = wincode::config::deserialize_exact(
        &bytes,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )?;
    ensure!(
        accounts.schema_version == DUMP_SCHEMA_VERSION
            && accounts.mint == target.mint
            && accounts.anchor_position.slot == target.mint_slot
            && accounts.anchor_position.signature_count != 0
            && (manifest.first_epoch..=manifest.last_epoch)
                .contains(&accounts.anchor_position.epoch)
            && accounts
                .accounts
                .windows(2)
                .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey)
            && accounts.accounts.iter().all(|account| {
                account.raw_pubkey != target.mint
                    && account.first_creation.slot >= target.mint_slot
                    && (manifest.first_epoch..=manifest.last_epoch)
                        .contains(&account.first_creation.epoch)
            })
            && manifest.discovered_account_count == Some(accounts.accounts.len() as u64),
        "frozen discovered-account artifact is invalid"
    );
    let target_mint_id = registry_id_for_key(registry, &target.mint)
        .context("target mint is absent from the consolidated registry")?;
    let dense_len = usize::try_from(registry_entries)?
        .checked_add(1)
        .context("target-account dense table length overflow")?;
    let mut token_account_by_registry_id = vec![0u8; dense_len];
    for account in &accounts.accounts {
        let registry_id = registry_id_for_key(registry, &account.raw_pubkey)
            .context("discovered token account is absent from the consolidated registry")?;
        let present = token_account_by_registry_id
            .get_mut(usize::try_from(registry_id)?)
            .context("discovered token account ID is outside the dense target table")?;
        ensure!(*present == 0, "duplicate discovered token account");
        *present = 1;
    }
    let discovered_token_accounts = u64::try_from(accounts.accounts.len())?;
    let target_addresses = discovered_token_accounts
        .checked_add(1)
        .context("target address count overflow")?;
    Ok(ProgramInventoryTargetAccounts {
        target_mint_id,
        token_account_by_registry_id,
        source: ProgramInventoryTargetAccountSource {
            file: ACCOUNTS_FILE,
            sha256: hex_digest(actual_sha256),
            discovered_token_accounts,
            target_addresses,
            membership_definition: "target mint or any token account in the final discovered-account artifact",
        },
    })
}

fn build_program_analysis_v3(
    dump: &Path,
    identified_programs: Option<&Path>,
    report: &Path,
) -> Result<()> {
    let started = Instant::now();
    let progress_name = if identified_programs.is_some() {
        "program coverage"
    } else {
        "program inventory"
    };
    let dump = fs::canonicalize(dump)
        .with_context(|| format!("resolve consolidated dump {}", dump.display()))?;
    ensure!(dump.is_dir(), "consolidated dump is not a directory");
    validate_exact_final_files(&dump)?;

    let report_parent = report.parent().unwrap_or_else(|| Path::new("."));
    let report_parent = if report_parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        report_parent
    };
    let canonical_report_parent = fs::canonicalize(report_parent)
        .with_context(|| format!("resolve report directory {}", report_parent.display()))?;
    ensure!(
        canonical_report_parent != dump,
        "program inventory report must not modify the immutable dump directory"
    );
    let report_name = report
        .file_name()
        .context("program inventory report path has no file name")?;
    ensure!(
        !canonical_report_parent.join(report_name).exists(),
        "refusing to replace an existing program inventory report"
    );

    let manifest_path = dump.join(DUMP_MANIFEST_FILE);
    let manifest_bytes = read_bounded_regular(&manifest_path, MAX_ROOT_MANIFEST_BYTES)?;
    let manifest_sha256 = sha256_bytes(&manifest_bytes);
    let manifest: DumpManifest = serde_json::from_slice(&manifest_bytes)?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::Consolidated
            && manifest.complete
            && manifest.workers != 0
            && manifest.first_epoch <= manifest.last_epoch
            && manifest.transactions != 0,
        "invalid consolidated manifest header"
    );
    ensure!(
        manifest.transaction_stream == TRANSACTIONS_FILE
            && manifest.signature_stream.as_deref() == Some(DUMP_SIGNATURES_FILE)
            && manifest.pubkey_registry.as_deref() == Some(PUBKEY_REGISTRY_FILE)
            && manifest.discovered_accounts.as_deref() == Some(ACCOUNTS_FILE)
            && manifest.account_id_log.is_none()
            && manifest.account_id_log_sha256.is_none()
            && manifest.registry_maps.is_none(),
        "consolidated manifest file bindings differ"
    );
    validate_source_binding(&manifest.source_binding)?;
    let expected_transaction_sha256 = parse_hex_digest(
        manifest
            .transaction_stream_sha256
            .as_deref()
            .context("missing transaction digest")?,
        "transaction digest",
    )?;
    let expected_registry_sha256 = parse_hex_digest(
        manifest
            .pubkey_registry_sha256
            .as_deref()
            .context("missing registry digest")?,
        "registry digest",
    )?;
    let expected_signatures = manifest.signatures.context("missing signature count")?;
    let expected_registry_rows = manifest.pubkeys.context("missing public-key count")?;
    ensure!(
        expected_registry_rows != 0 && expected_registry_rows < u64::from(u32::MAX),
        "invalid registry row count"
    );
    let registry_entries =
        u32::try_from(expected_registry_rows).context("registry row count exceeds u32")?;
    let expected_registry_bytes = expected_registry_rows
        .checked_mul(KEY_BYTES as u64)
        .context("registry byte length overflow")?;
    let registry = read_bounded_regular(&dump.join(PUBKEY_REGISTRY_FILE), expected_registry_bytes)?;
    ensure!(
        u64::try_from(registry.len())? == expected_registry_bytes,
        "registry size differs from its manifest"
    );
    let actual_registry_sha256 = sha256_bytes(&registry);
    ensure!(
        actual_registry_sha256 == expected_registry_sha256,
        "registry digest differs from its manifest"
    );
    let mut previous_registry_key = None;
    for row in registry.chunks_exact(KEY_BYTES) {
        let key: [u8; KEY_BYTES] = row.try_into().expect("exact registry chunks");
        ensure!(
            previous_registry_key.is_none_or(|previous| previous < key),
            "registry is not strictly sorted and unique"
        );
        previous_registry_key = Some(key);
    }
    let mut coverage = identified_programs
        .map(|path| load_program_coverage_tracker(path, &registry, registry_entries))
        .transpose()?;

    let signature_bytes = expected_signatures
        .checked_mul(SIGNATURE_BYTES as u64)
        .context("signature byte length overflow")?;
    let signature_metadata = fs::symlink_metadata(dump.join(DUMP_SIGNATURES_FILE))?;
    ensure!(
        signature_metadata.file_type().is_file() && signature_metadata.len() == signature_bytes,
        "signature sidecar size differs from its manifest"
    );

    let target = TargetBinding {
        mint: parse_pubkey(&manifest.mint, "mint")?,
        mint_slot: manifest.mint_slot,
        mint_signature: parse_signature(&manifest.mint_signature)?,
    };
    let target_accounts = if identified_programs.is_none() {
        Some(load_program_inventory_target_accounts(
            &dump,
            &manifest,
            &target,
            &registry,
            registry_entries,
        )?)
    } else {
        None
    };
    let transaction_path = dump.join(TRANSACTIONS_FILE);
    let transaction_file = File::open(&transaction_path)?;
    let transaction_stamp = FileStamp::read(&transaction_file)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, transaction_file);
    let mut transaction_hasher = Sha256::new();
    let mut logical_offset = 0u64;
    let mut payload = Vec::new();
    read_frame_hashed(
        &mut reader,
        &mut logical_offset,
        &mut transaction_hasher,
        &mut payload,
    )?
    .context("consolidated transaction stream is empty")?;
    let BorrowedDumpRecord::Header(header) = decode_borrowed_frame(&payload)? else {
        bail!("consolidated transaction stream does not start with a header")
    };
    ensure!(
        header.schema_version == DUMP_SCHEMA_VERSION
            && header.stream_kind == DumpStreamKind::Consolidated
            && header.mint == target.mint
            && header.mint_slot == target.mint_slot
            && header.mint_signature == target.mint_signature
            && header.source_epoch.is_none()
            && header.source_generation_digest.is_none()
            && header.source_wire_profile.is_none()
            && header.pubkey_registry_id_base == PUBKEY_REGISTRY_ID_BASE,
        "consolidated stream header differs from its manifest"
    );

    let mut table = ProgramAccumulatorTable::new(registry_entries)?;
    let mut counters = ProgramInventoryCounters::default();
    let mut scratch = ProgramInventoryScratch::new();
    let mut signatures = 0u64;
    let mut previous_coordinate = None;
    let mut previous_slot = None::<(u64, u64, u32, BlockIdentity)>;
    let footer = loop {
        read_frame_hashed(
            &mut reader,
            &mut logical_offset,
            &mut transaction_hasher,
            &mut payload,
        )?
        .context("consolidated transaction stream has no footer")?;
        match decode_borrowed_frame(&payload)? {
            BorrowedDumpRecord::Header(_) => {
                bail!("consolidated transaction stream repeats its header")
            }
            BorrowedDumpRecord::Footer(footer) => break footer,
            BorrowedDumpRecord::Transaction(record) => {
                let coordinate = ProgramInventoryCoordinate::from_record(&record);
                ensure!(
                    previous_coordinate
                        .is_none_or(|previous| previous < coordinate.canonical_key()),
                    "consolidated transactions are not in canonical order"
                );
                previous_coordinate = Some(coordinate.canonical_key());
                ensure!(
                    (manifest.first_epoch..=manifest.last_epoch).contains(&record.source_epoch)
                        && record.block.parent_slot < record.block.slot
                        && record.block.transaction_count != 0
                        && record.tx_index < record.block.transaction_count
                        && record.signature_count != 0
                        && !record.message_bytes.is_empty()
                        && record.flags
                            & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                                | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
                            == 0,
                    "consolidated transaction has invalid source fields"
                );
                let DumpSourceBinding::TrustedLocalSizesOnly {
                    slots_per_epoch,
                    wire_profile,
                    ..
                } = &manifest.source_binding;
                let first_slot = record
                    .source_epoch
                    .checked_mul(*slots_per_epoch)
                    .context("source epoch first slot overflow")?;
                ensure!(
                    record.source_wire_profile == *wire_profile
                        && record.block.slot >= first_slot
                        && record.block.slot - first_slot < *slots_per_epoch
                        && u64::from(record.source_block_id) < *slots_per_epoch,
                    "consolidated transaction differs from its trusted source binding"
                );
                ensure!(
                    (record.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0)
                        == !record.metadata_bytes.is_empty()
                        && (record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
                            == (record.metadata_bytes.first() == Some(&1)),
                    "consolidated transaction flags differ from metadata bytes"
                );
                ensure!(
                    record.dump_signature_ordinal == Some(signatures),
                    "consolidated signature ordinals are not contiguous"
                );
                record
                    .source_first_signature_ordinal
                    .checked_add(u64::from(record.signature_count))
                    .context("source signature range overflow")?;
                let identity = BlockIdentity::from(&record.block);
                if let Some((epoch, slot, block_id, previous_identity)) = previous_slot
                    && epoch == record.source_epoch
                    && slot == record.block.slot
                {
                    ensure!(
                        block_id == record.source_block_id && previous_identity == identity,
                        "one source slot has conflicting block context"
                    );
                }
                previous_slot = Some((
                    record.source_epoch,
                    record.block.slot,
                    record.source_block_id,
                    identity,
                ));

                scratch.clear_outer();
                let static_ids = &mut scratch.static_ids;
                let outer_counts = &mut scratch.outer_counts;
                let outer_touched = &mut scratch.outer_touched;
                let outer_touched_len = &mut scratch.outer_touched_len;
                let mut static_count = 0usize;
                let mut invalid_static_key = false;
                let mut outer_overflow = false;
                let message = projector(record.source_wire_profile)
                    .visit_static_accounts_and_instructions_exact(
                        record.message_bytes,
                        registry_entries,
                        |ordinal, reference| {
                            if ordinal >= static_ids.len() {
                                invalid_static_key = true;
                                return;
                            }
                            match reference {
                                CompactPubkey::Id(id) if id != 0 && id <= registry_entries => {
                                    static_ids[ordinal] = id;
                                    static_count = ordinal + 1;
                                }
                                CompactPubkey::Id(_) | CompactPubkey::Raw(_) => {
                                    invalid_static_key = true;
                                }
                            }
                        },
                        |instruction| {
                            let index = usize::from(instruction.program_id_index);
                            if outer_counts[index] == 0 {
                                outer_touched[*outer_touched_len] = instruction.program_id_index;
                                *outer_touched_len += 1;
                            }
                            match outer_counts[index].checked_add(1) {
                                Some(count) => outer_counts[index] = count,
                                None => outer_overflow = true,
                            }
                        },
                    )
                    .with_context(|| {
                        format!(
                            "decode message at epoch {} slot {} transaction {}",
                            record.source_epoch, record.block.slot, record.tx_index
                        )
                    })?;
                ensure!(
                    !invalid_static_key,
                    "message contains an unresolved static program key"
                );
                ensure!(!outer_overflow, "outer instruction count overflow");
                ensure!(
                    static_count == message.static_account_count,
                    "static-account callback count differs from message summary"
                );
                let staged_outer_occurrences = scratch.outer_touched[..scratch.outer_touched_len]
                    .iter()
                    .try_fold(0u64, |sum, index| {
                        sum.checked_add(scratch.outer_counts[usize::from(*index)])
                            .context("staged outer instruction count overflow")
                    })?;
                ensure!(
                    staged_outer_occurrences == u64::try_from(message.instruction_count)?,
                    "outer-instruction callbacks differ from message summary"
                );
                validate_inventory_message_summary(&message, record.flags, record.signature_count)?;
                let metadata_context = ProgramInventoryMetadataContext {
                    registry_entries,
                    flags: record.flags,
                    target_accounts: target_accounts.as_ref(),
                };

                let selected_inner = if record.metadata_bytes.is_empty() {
                    validate_inventory_absent_metadata(&message, record.flags)?;
                    checked_increment(&mut counters.metadata_absent, "metadata-absent count")?;
                    None
                } else if record.metadata_bytes.first() == Some(&0) {
                    parse_inventory_metadata_stage(
                        &mut scratch.current_metadata,
                        record.metadata_bytes,
                        ArchiveV2WireMetadataErrorSchema::Current,
                        &message,
                        &scratch.static_ids,
                        metadata_context,
                    )
                    .with_context(|| {
                        format!(
                            "decode successful metadata at epoch {} slot {} transaction {}",
                            record.source_epoch, record.block.slot, record.tx_index
                        )
                    })?;
                    checked_increment(
                        &mut counters.metadata_without_error,
                        "metadata-without-error count",
                    )?;
                    Some(ArchiveV2WireMetadataErrorSchema::Current)
                } else {
                    let current_prefix_valid =
                        validate_archive_v2_metadata_error_prefix_for_selected_schema(
                            record.metadata_bytes,
                            ArchiveV2WireMetadataErrorSchema::Current,
                            record.metadata_bytes.len(),
                        )
                        .is_ok();
                    let legacy_prefix_valid =
                        validate_archive_v2_metadata_error_prefix_for_selected_schema(
                            record.metadata_bytes,
                            ArchiveV2WireMetadataErrorSchema::Legacy,
                            record.metadata_bytes.len(),
                        )
                        .is_ok();
                    let current_valid = current_prefix_valid
                        && parse_inventory_metadata_stage(
                            &mut scratch.current_metadata,
                            record.metadata_bytes,
                            ArchiveV2WireMetadataErrorSchema::Current,
                            &message,
                            &scratch.static_ids,
                            metadata_context,
                        )
                        .is_ok();
                    let legacy_valid = legacy_prefix_valid
                        && parse_inventory_metadata_stage(
                            &mut scratch.legacy_metadata,
                            record.metadata_bytes,
                            ArchiveV2WireMetadataErrorSchema::Legacy,
                            &message,
                            &scratch.static_ids,
                            metadata_context,
                        )
                        .is_ok();
                    ensure!(
                        current_valid || legacy_valid,
                        "metadata is invalid under both selected error schemas at epoch {} slot {} transaction {}",
                        record.source_epoch,
                        record.block.slot,
                        record.tx_index
                    );
                    match (current_valid, legacy_valid) {
                        (true, false) => {
                            checked_increment(
                                &mut counters.metadata_current_only,
                                "current-only metadata count",
                            )?;
                            Some(ArchiveV2WireMetadataErrorSchema::Current)
                        }
                        (false, true) => {
                            checked_increment(
                                &mut counters.metadata_legacy_only,
                                "legacy-only metadata count",
                            )?;
                            Some(ArchiveV2WireMetadataErrorSchema::Legacy)
                        }
                        (true, true) => {
                            ensure!(
                                resolved_inner_sequences_equal(
                                    &scratch.current_metadata.programs,
                                    &scratch.legacy_metadata.programs,
                                ),
                                "dual-valid metadata resolves to divergent inner program sequences at epoch {} slot {} transaction {}",
                                record.source_epoch,
                                record.block.slot,
                                record.tx_index
                            );
                            checked_increment(
                                &mut counters.metadata_both_same_program_resolution,
                                "dual-valid same-program-resolution metadata count",
                            )?;
                            Some(ArchiveV2WireMetadataErrorSchema::Current)
                        }
                        (false, false) => unreachable!("both-invalid metadata was rejected"),
                    }
                };

                let transaction_ordinal = counters.transactions;
                if scratch.outer_touched_len != 0 {
                    checked_increment(
                        &mut counters.transactions_with_outer_instructions,
                        "transactions-with-outer count",
                    )?;
                }
                commit_outer_programs(
                    &scratch,
                    &mut table,
                    &mut counters,
                    transaction_ordinal,
                    coordinate,
                )?;
                if let Some(coverage) = coverage.as_mut() {
                    for &program_index in &scratch.outer_touched[..scratch.outer_touched_len] {
                        let index = usize::from(program_index);
                        coverage.record(
                            scratch.static_ids[index],
                            ProgramInventoryOrigin::Outer,
                            scratch.outer_counts[index],
                        )?;
                    }
                }
                let selected_programs: &[StagedInnerProgram] = match selected_inner {
                    Some(ArchiveV2WireMetadataErrorSchema::Current) => {
                        &scratch.current_metadata.programs
                    }
                    Some(ArchiveV2WireMetadataErrorSchema::Legacy) => {
                        &scratch.legacy_metadata.programs
                    }
                    None => &[],
                };
                if !selected_programs.is_empty() {
                    checked_increment(
                        &mut counters.transactions_with_inner_instructions,
                        "transactions-with-inner count",
                    )?;
                }
                let mut transaction_has_target_account_inner_instruction = false;
                for &program in selected_programs {
                    transaction_has_target_account_inner_instruction |= program
                        .target_mint_references
                        .checked_add(program.target_token_account_references)
                        .context("target-account inner reference count overflow")?
                        != 0;
                    record_inner_occurrence(
                        &mut table,
                        &mut counters,
                        program,
                        transaction_ordinal,
                        coordinate,
                    )?;
                    if let Some(coverage) = coverage.as_mut() {
                        coverage.record(
                            program.registry_id_or_account_index,
                            ProgramInventoryOrigin::Inner,
                            1,
                        )?;
                    }
                }
                if transaction_has_target_account_inner_instruction {
                    checked_increment(
                        &mut counters.transactions_with_target_account_inner_instructions,
                        "transactions-with-target-account-inner-instructions count",
                    )?;
                }
                if let Some(coverage) = coverage.as_mut() {
                    coverage.finish_transaction()?;
                }
                match record.source_wire_profile {
                    DumpWireProfile::PostUnknownInstructionFallbacksV1 => checked_increment(
                        &mut counters.post_profile_messages,
                        "Post-profile message count",
                    )?,
                    DumpWireProfile::PreUnknownInstructionFallbacksV1 => checked_increment(
                        &mut counters.pre_profile_messages,
                        "Pre-profile message count",
                    )?,
                }
                checked_increment(&mut counters.transactions, "transaction count")?;
                signatures = signatures
                    .checked_add(u64::from(record.signature_count))
                    .context("signature count overflow")?;
                if counters
                    .transactions
                    .is_multiple_of(PROGRAM_INVENTORY_PROGRESS_TRANSACTIONS)
                {
                    inventory_progress(
                        progress_name,
                        started,
                        counters.transactions,
                        manifest.transactions,
                        logical_offset,
                    );
                }
            }
        }
    };
    ensure!(
        read_frame_hashed(
            &mut reader,
            &mut logical_offset,
            &mut transaction_hasher,
            &mut payload,
        )?
        .is_none(),
        "consolidated transaction stream has records after its footer"
    );
    let transaction_file = reader.into_inner();
    transaction_stamp.verify(&transaction_file, "consolidated transaction stream")?;
    verify_path_binding(
        &transaction_path,
        &transaction_stamp,
        "consolidated transaction stream",
    )?;
    ensure!(
        logical_offset == transaction_stamp.bytes,
        "transaction stream size changed while it was read"
    );
    let actual_transaction_sha256: [u8; 32] = transaction_hasher.finalize().into();
    ensure!(
        actual_transaction_sha256 == expected_transaction_sha256,
        "transaction digest differs from its manifest"
    );
    let epoch_count = manifest
        .last_epoch
        .checked_sub(manifest.first_epoch)
        .and_then(|span| span.checked_add(1))
        .context("manifest epoch count overflow")?;
    ensure!(
        counters.transactions == manifest.transactions
            && signatures == expected_signatures
            && footer.epochs == epoch_count
            && footer.transactions_written == counters.transactions
            && footer.transactions_scanned >= counters.transactions
            && footer.pubkeys == expected_registry_rows
            && footer.signatures == signatures
            && footer.owned_block_fallbacks <= footer.blocks_scanned
            && footer.raw_transaction_fallbacks == 0
            && footer.raw_metadata_fallbacks == 0,
        "consolidated stream counters differ from its manifest"
    );
    ensure!(
        counters.outer_occurrences == counters.outer_static_resolutions
            && counters.inner_occurrences
                == counters
                    .inner_static_resolutions
                    .checked_add(counters.inner_loaded_writable_resolutions)
                    .and_then(|count| {
                        count.checked_add(counters.inner_loaded_readonly_resolutions)
                    })
                    .context("inner resolution count overflow")?
            && counters.unresolved_program_references == 0
            && counters.inline_raw_program_keys == 0,
        "program resolution counters are incomplete"
    );
    let metadata_records = counters
        .metadata_absent
        .checked_add(counters.metadata_without_error)
        .and_then(|count| count.checked_add(counters.metadata_current_only))
        .and_then(|count| count.checked_add(counters.metadata_legacy_only))
        .and_then(|count| count.checked_add(counters.metadata_both_same_program_resolution))
        .context("metadata classification count overflow")?;
    ensure!(
        metadata_records == counters.transactions
            && counters
                .post_profile_messages
                .checked_add(counters.pre_profile_messages)
                .context("message profile count overflow")?
                == counters.transactions
            && counters.metadata_both_divergent == 0,
        "inventory classifications do not cover every transaction exactly once"
    );

    counters.distinct_programs =
        u64::try_from(table.programs.len()).context("distinct program count exceeds u64")?;
    let (program_outer_occurrences, program_inner_occurrences) = table.programs.iter().try_fold(
        (0u64, 0u64),
        |(outer, inner), program| -> Result<(u64, u64)> {
            ensure!(
                program.outer_transactions <= program.transactions
                    && program.inner_transactions <= program.transactions,
                "per-program transaction counters are inconsistent"
            );
            Ok((
                outer
                    .checked_add(program.outer_occurrences)
                    .context("program outer occurrence total overflow")?,
                inner
                    .checked_add(program.inner_occurrences)
                    .context("program inner occurrence total overflow")?,
            ))
        },
    )?;
    ensure!(
        program_outer_occurrences == counters.outer_occurrences
            && program_inner_occurrences == counters.inner_occurrences,
        "program rows do not cover all resolved instruction occurrences"
    );
    let mut program_target_account_inner_occurrences = 0u64;
    let mut program_target_account_inner_transactions = 0u64;
    let mut program_target_mint_inner_occurrences = 0u64;
    let mut program_target_token_account_inner_occurrences = 0u64;
    let mut program_target_account_inner_references = 0u64;
    let mut program_target_mint_inner_references = 0u64;
    let mut program_target_token_account_inner_references = 0u64;
    for program in &table.programs {
        ensure!(
            program.target_account_inner_occurrences <= program.inner_occurrences
                && program.target_account_inner_transactions <= program.inner_transactions
                && program.target_mint_inner_occurrences
                    <= program.target_account_inner_occurrences
                && program.target_token_account_inner_occurrences
                    <= program.target_account_inner_occurrences
                && program.target_account_inner_occurrences
                    <= program.target_account_inner_references
                && program.target_mint_inner_occurrences <= program.target_mint_inner_references
                && program.target_token_account_inner_occurrences
                    <= program.target_token_account_inner_references
                && program.target_account_inner_references
                    == program
                        .target_mint_inner_references
                        .checked_add(program.target_token_account_inner_references)
                        .context("per-program target reference partition overflow")?,
            "per-program target-account inner counters are inconsistent"
        );
        program_target_account_inner_occurrences = program_target_account_inner_occurrences
            .checked_add(program.target_account_inner_occurrences)
            .context("program target-account inner occurrence total overflow")?;
        program_target_account_inner_transactions = program_target_account_inner_transactions
            .checked_add(program.target_account_inner_transactions)
            .context("program target-account inner transaction total overflow")?;
        program_target_mint_inner_occurrences = program_target_mint_inner_occurrences
            .checked_add(program.target_mint_inner_occurrences)
            .context("program target-mint inner occurrence total overflow")?;
        program_target_token_account_inner_occurrences =
            program_target_token_account_inner_occurrences
                .checked_add(program.target_token_account_inner_occurrences)
                .context("program target-token-account inner occurrence total overflow")?;
        program_target_account_inner_references = program_target_account_inner_references
            .checked_add(program.target_account_inner_references)
            .context("program target-account inner reference total overflow")?;
        program_target_mint_inner_references = program_target_mint_inner_references
            .checked_add(program.target_mint_inner_references)
            .context("program target-mint inner reference total overflow")?;
        program_target_token_account_inner_references =
            program_target_token_account_inner_references
                .checked_add(program.target_token_account_inner_references)
                .context("program target-token-account inner reference total overflow")?;
    }
    ensure!(
        counters.transactions_with_target_account_inner_instructions <= counters.transactions
            && counters.target_account_inner_occurrences <= counters.inner_occurrences
            && counters.target_mint_inner_occurrences <= counters.target_account_inner_occurrences
            && counters.target_token_account_inner_occurrences
                <= counters.target_account_inner_occurrences
            && counters.target_account_inner_references
                == counters
                    .target_mint_inner_references
                    .checked_add(counters.target_token_account_inner_references)
                    .context("target reference partition overflow")?
            && program_target_account_inner_occurrences
                == counters.target_account_inner_occurrences
            && program_target_account_inner_transactions
                == counters.target_account_inner_transactions
            && program_target_mint_inner_occurrences == counters.target_mint_inner_occurrences
            && program_target_token_account_inner_occurrences
                == counters.target_token_account_inner_occurrences
            && program_target_account_inner_references == counters.target_account_inner_references
            && program_target_mint_inner_references == counters.target_mint_inner_references
            && program_target_token_account_inner_references
                == counters.target_token_account_inner_references,
        "program target-account inner counters do not match global counters"
    );
    let transaction_count = counters.transactions;
    let distinct_programs = counters.distinct_programs;
    let outer_occurrences = counters.outer_occurrences;
    let inner_occurrences = counters.inner_occurrences;
    let source = ProgramInventorySource {
        mint: manifest.mint,
        manifest_sha256: hex_digest(manifest_sha256),
        transaction_stream_sha256: hex_digest(actual_transaction_sha256),
        pubkey_registry_sha256: hex_digest(actual_registry_sha256),
        transactions: transaction_count,
        signatures,
        registry_entries,
        first_epoch: manifest.first_epoch,
        last_epoch: manifest.last_epoch,
    };
    let (analysis_name, mut report_bytes) = if let Some(coverage) = coverage {
        ensure!(
            coverage.counters.transactions == transaction_count
                && coverage.counters.outer_occurrences == outer_occurrences
                && coverage.counters.inner_occurrences == inner_occurrences
                && coverage
                    .counters
                    .identified_outer_occurrences
                    .checked_add(coverage.counters.unidentified_outer_occurrences)
                    .context("coverage outer occurrence partition overflow")?
                    == coverage.counters.outer_occurrences
                && coverage
                    .counters
                    .identified_inner_occurrences
                    .checked_add(coverage.counters.unidentified_inner_occurrences)
                    .context("coverage inner occurrence partition overflow")?
                    == coverage.counters.inner_occurrences,
            "program coverage occurrence counters are inconsistent"
        );
        ensure!(
            coverage
                .counters
                .fully_covered_transactions
                .checked_add(coverage.counters.partially_covered_transactions)
                .and_then(|count| count.checked_add(coverage.counters.uncovered_transactions))
                .context("coverage transaction partition overflow")?
                == transaction_count
                && coverage.counters.touched_transactions
                    == coverage
                        .counters
                        .fully_covered_transactions
                        .checked_sub(coverage.counters.transactions_without_instructions)
                        .and_then(|count| {
                            count.checked_add(coverage.counters.partially_covered_transactions)
                        })
                        .context("coverage touched transaction invariant overflow")?,
            "program coverage transaction counters are inconsistent"
        );

        let programs_used_by_instructions = u64::try_from(
            table
                .programs
                .iter()
                .filter(|program| {
                    coverage.identified_by_registry_id
                        [usize::try_from(program.registry_id).expect("validated registry ID")]
                        != 0
                })
                .count(),
        )?;
        let mut unknown_programs = table
            .programs
            .iter()
            .copied()
            .filter(|program| {
                coverage.identified_by_registry_id
                    [usize::try_from(program.registry_id).expect("validated registry ID")]
                    == 0
            })
            .collect::<Vec<_>>();
        sort_unknown_program_accumulators(&mut unknown_programs, &registry);
        let unknown_programs = unknown_programs
            .into_iter()
            .map(|program| serialize_program_accumulator(program, &registry))
            .collect::<Result<Vec<_>>>()?;

        let mut program_ids = Vec::with_capacity(coverage.requested_keys.len());
        let mut absent_from_registry = Vec::new();
        let mut not_used_by_instructions = Vec::new();
        for key in &coverage.requested_keys {
            let encoded = bs58::encode(key).into_string();
            match registry_id_for_key(&registry, key) {
                None => absent_from_registry.push(encoded.clone()),
                Some(registry_id)
                    if table.by_registry_id[usize::try_from(registry_id)?]
                        == PROGRAM_ACCUMULATOR_MISSING =>
                {
                    not_used_by_instructions.push(encoded.clone());
                }
                Some(_) => {}
            }
            program_ids.push(encoded);
        }
        let report_value = ProgramCoverageReport {
            schema_version: 1,
            artifact_kind: "program_identification_coverage",
            complete: true,
            generator: ProgramAnalysisGenerator {
                crate_name: env!("CARGO_PKG_NAME"),
                crate_version: env!("CARGO_PKG_VERSION"),
                executable_sha256: hex_digest(hash_running_executable(
                    "program coverage executable",
                )?),
            },
            instruction_program_resolution_complete: true,
            transaction_classes: "fully_covered_all_programs_identified; partially_covered_identified_and_unidentified_programs; uncovered_has_instructions_but_no_identified_program; instruction_free_is_fully_covered",
            unknown_program_order: "transactions_desc_then_total_occurrences_desc_then_raw_pubkey_asc",
            source,
            identified_set: ProgramCoverageIdentifiedSet {
                input_sha256: hex_digest(coverage.input_sha256),
                input_format: "one_base58_program_id_per_line_blank_and_hash_comment_lines_ignored",
                requested_programs: u64::try_from(coverage.requested_keys.len())?,
                programs_present_in_registry: coverage.present_in_registry,
                programs_used_by_instructions,
                program_ids,
                program_ids_absent_from_registry: absent_from_registry,
                program_ids_not_used_by_instructions: not_used_by_instructions,
            },
            counters: coverage.counters,
            distinct_programs,
            unknown_programs,
        };
        (
            "program coverage",
            serde_json::to_vec_pretty(&report_value)?,
        )
    } else {
        let target_account_source = target_accounts
            .context("program inventory target-account source was not loaded")?
            .source;
        sort_program_accumulators(&mut table.programs, &registry);
        let mut programs = Vec::new();
        programs
            .try_reserve_exact(table.programs.len())
            .context("reserve serialized program inventory")?;
        for program in table.programs {
            programs.push(serialize_program_inventory_target_accumulator(
                program, &registry,
            )?);
        }
        let report_value = ProgramInventoryReport {
            schema_version: 2,
            artifact_kind: "program_inventory",
            complete: true,
            instruction_program_resolution_complete: true,
            program_order: "total_occurrences_desc_then_raw_pubkey_asc",
            source: ProgramInventoryBoundSource {
                general: source,
                target_accounts: target_account_source,
            },
            counters,
            programs,
        };
        (
            "program inventory",
            serde_json::to_vec_pretty(&report_value)?,
        )
    };
    report_bytes.push(b'\n');
    let report_sha256 = sha256_bytes(&report_bytes);
    let report_path = publish_program_inventory_report(report, &report_bytes)?;
    inventory_progress(
        progress_name,
        started,
        transaction_count,
        transaction_count,
        logical_offset,
    );
    eprintln!(
        "{analysis_name} complete: {transaction_count} transactions, {distinct_programs} programs, {outer_occurrences} outer instructions, {inner_occurrences} inner instructions, {:.1}s elapsed, report_sha256={}, report={}",
        started.elapsed().as_secs_f64(),
        hex_digest(report_sha256),
        report_path.display(),
    );
    Ok(())
}

fn hash_running_executable(label: &str) -> Result<[u8; 32]> {
    let path = std::env::current_exe().with_context(|| format!("resolve {label}"))?;
    let file = File::open(&path).with_context(|| format!("open {label} {}", path.display()))?;
    let stamp = FileStamp::read(&file)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let count = reader
            .read(&mut buffer)
            .with_context(|| format!("hash {label} {}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let file = reader.into_inner();
    stamp.verify(&file, label)?;
    verify_path_binding(&path, &stamp, label)?;
    Ok(hasher.finalize().into())
}

/// Perform a separate full audit of a completed schema-3 consolidated dump.
///
/// This reads and hashes all five final files. Consolidation does not call it
/// automatically because that would repeat the large transaction-stream read.
pub fn validate_completed_consolidated_dump_v3(output: &Path) -> Result<()> {
    let output = fs::canonicalize(output)
        .with_context(|| format!("resolve consolidated output {}", output.display()))?;
    ensure!(output.is_dir(), "consolidated output is not a directory");
    validate_exact_final_files(&output)?;
    let manifest_path = output.join(DUMP_MANIFEST_FILE);
    let manifest: DumpManifest = serde_json::from_slice(&read_bounded_regular(
        &manifest_path,
        MAX_ROOT_MANIFEST_BYTES,
    )?)?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::Consolidated
            && manifest.complete
            && manifest.workers != 0
            && manifest.first_epoch <= manifest.last_epoch,
        "invalid consolidated manifest header"
    );
    ensure!(
        manifest.transaction_stream == TRANSACTIONS_FILE
            && manifest.signature_stream.as_deref() == Some(DUMP_SIGNATURES_FILE)
            && manifest.pubkey_registry.as_deref() == Some(PUBKEY_REGISTRY_FILE)
            && manifest.discovered_accounts.as_deref() == Some(ACCOUNTS_FILE)
            && manifest.account_id_log.is_none()
            && manifest.account_id_log_sha256.is_none()
            && manifest.registry_maps.is_none(),
        "consolidated manifest file bindings differ"
    );
    validate_source_binding(&manifest.source_binding)?;
    let transaction_sha = parse_hex_digest(
        manifest
            .transaction_stream_sha256
            .as_deref()
            .context("missing transaction digest")?,
        "transaction digest",
    )?;
    let signature_sha = parse_hex_digest(
        manifest
            .signature_stream_sha256
            .as_deref()
            .context("missing signature digest")?,
        "signature digest",
    )?;
    let registry_sha = parse_hex_digest(
        manifest
            .pubkey_registry_sha256
            .as_deref()
            .context("missing registry digest")?,
        "registry digest",
    )?;
    let account_sha = parse_hex_digest(
        manifest
            .discovered_accounts_sha256
            .as_deref()
            .context("missing account digest")?,
        "account digest",
    )?;
    let signature_count = manifest.signatures.context("missing signature count")?;
    let pubkey_count = manifest.pubkeys.context("missing public-key count")?;
    let registry_entries = validate_final_registry(
        &output.join(PUBKEY_REGISTRY_FILE),
        pubkey_count,
        registry_sha,
    )?;
    let signature_bytes = signature_count
        .checked_mul(SIGNATURE_BYTES as u64)
        .context("signature byte length overflow")?;
    ensure!(
        hash_regular_file(&output.join(DUMP_SIGNATURES_FILE), signature_bytes)? == signature_sha,
        "signature digest differs"
    );

    let account_bytes = read_bounded_regular(
        &output.join(ACCOUNTS_FILE),
        ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES as u64,
    )?;
    ensure!(
        sha256_bytes(&account_bytes) == account_sha,
        "account digest differs"
    );
    let accounts: DiscoveredAccountList = wincode::config::deserialize_exact(
        &account_bytes,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )?;
    let target = TargetBinding {
        mint: parse_pubkey(&manifest.mint, "mint")?,
        mint_slot: manifest.mint_slot,
        mint_signature: parse_signature(&manifest.mint_signature)?,
    };
    ensure!(
        accounts.schema_version == DUMP_SCHEMA_VERSION
            && accounts.mint == target.mint
            && accounts.anchor_position.slot == target.mint_slot
            && accounts.anchor_position.signature_count != 0
            && (manifest.first_epoch..=manifest.last_epoch)
                .contains(&accounts.anchor_position.epoch)
            && accounts
                .accounts
                .windows(2)
                .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey)
            && accounts.accounts.iter().all(|account| {
                account.raw_pubkey != target.mint
                    && account.first_creation.slot >= target.mint_slot
                    && (manifest.first_epoch..=manifest.last_epoch)
                        .contains(&account.first_creation.epoch)
            })
            && manifest.discovered_account_count == Some(accounts.accounts.len() as u64),
        "frozen account artifact is invalid"
    );

    let transaction_path = output.join(TRANSACTIONS_FILE);
    let transaction_file = File::open(&transaction_path)?;
    let transaction_stamp = FileStamp::read(&transaction_file)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, transaction_file);
    let mut hasher = Sha256::new();
    let mut logical_offset = 0u64;
    let mut payload = Vec::new();
    read_frame_hashed(&mut reader, &mut logical_offset, &mut hasher, &mut payload)?
        .context("consolidated transaction stream is empty")?;
    let TokenTransactionDumpRecord::Header(header) = decode_frame(&payload)? else {
        bail!("consolidated stream does not start with a header")
    };
    ensure!(
        header.schema_version == DUMP_SCHEMA_VERSION
            && header.stream_kind == DumpStreamKind::Consolidated
            && header.mint == target.mint
            && header.mint_slot == target.mint_slot
            && header.mint_signature == target.mint_signature
            && header.source_epoch.is_none()
            && header.source_generation_digest.is_none()
            && header.source_wire_profile.is_none()
            && header.pubkey_registry_id_base == PUBKEY_REGISTRY_ID_BASE,
        "consolidated stream header differs from its manifest"
    );
    let signatures_file = File::open(output.join(DUMP_SIGNATURES_FILE))?;
    let mut wire_output = Vec::new();
    let mut wire_comparison_output = Vec::new();
    let mut transactions = 0u64;
    let mut signatures = 0u64;
    let mut anchor_count = 0u64;
    let mut previous_coordinate = None;
    let mut previous_slot = None::<(u64, u64, u32, BlockIdentity)>;
    let footer = loop {
        read_frame_hashed(&mut reader, &mut logical_offset, &mut hasher, &mut payload)?
            .context("consolidated transaction stream has no footer")?;
        match decode_frame(&payload)? {
            TokenTransactionDumpRecord::Header(_) => {
                bail!("consolidated stream repeats its header")
            }
            TokenTransactionDumpRecord::Footer(footer) => break footer,
            TokenTransactionDumpRecord::Transaction(record) => {
                let coordinate = (
                    record.source_epoch,
                    record.block.slot,
                    record.source_block_id,
                    record.tx_index,
                );
                ensure!(
                    previous_coordinate.is_none_or(|previous| previous < coordinate),
                    "consolidated transactions are not in canonical order"
                );
                previous_coordinate = Some(coordinate);
                ensure!(
                    (manifest.first_epoch..=manifest.last_epoch).contains(&record.source_epoch)
                        && record.block.parent_slot < record.block.slot
                        && record.block.transaction_count != 0
                        && record.tx_index < record.block.transaction_count
                        && record.signature_count != 0
                        && !record.message_bytes.is_empty()
                        && record.flags
                            & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                                | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
                            == 0,
                    "consolidated transaction has invalid source fields"
                );
                let DumpSourceBinding::TrustedLocalSizesOnly {
                    slots_per_epoch,
                    wire_profile,
                    ..
                } = &manifest.source_binding;
                let first_slot = record
                    .source_epoch
                    .checked_mul(*slots_per_epoch)
                    .context("source epoch first slot overflow")?;
                ensure!(
                    record.source_wire_profile == *wire_profile
                        && record.block.slot >= first_slot
                        && record.block.slot - first_slot < *slots_per_epoch
                        && u64::from(record.source_block_id) < *slots_per_epoch,
                    "consolidated transaction differs from its trusted source binding"
                );
                ensure!(
                    (record.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0)
                        == !record.metadata_bytes.is_empty()
                        && (record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
                            == (record.metadata_bytes.first() == Some(&1)),
                    "consolidated transaction flags differ from metadata bytes"
                );
                ensure!(
                    record.dump_signature_ordinal == Some(signatures),
                    "consolidated signature ordinals are not contiguous"
                );
                record
                    .source_first_signature_ordinal
                    .checked_add(u64::from(record.signature_count))
                    .context("source signature range overflow")?;
                let identity = BlockIdentity::from(&record.block);
                if let Some((epoch, slot, block_id, previous_identity)) = previous_slot
                    && epoch == record.source_epoch
                    && slot == record.block.slot
                {
                    ensure!(
                        block_id == record.source_block_id && previous_identity == identity,
                        "one source slot has conflicting block context"
                    );
                }
                previous_slot = Some((
                    record.source_epoch,
                    record.block.slot,
                    record.source_block_id,
                    identity,
                ));
                validate_final_record_wire(
                    &record,
                    registry_entries,
                    &mut wire_output,
                    &mut wire_comparison_output,
                )?;
                let is_anchor = coordinate
                    == (
                        accounts.anchor_position.epoch,
                        accounts.anchor_position.slot,
                        accounts.anchor_position.source_block_id,
                        accounts.anchor_position.tx_index,
                    );
                if is_anchor {
                    ensure!(
                        record.source_first_signature_ordinal
                            == accounts.anchor_position.source_first_signature_ordinal
                            && record.signature_count == accounts.anchor_position.signature_count,
                        "anchor source signature range differs"
                    );
                    let mut first_signature = [0u8; SIGNATURE_BYTES];
                    read_exact_at(
                        &signatures_file,
                        &mut first_signature,
                        signatures
                            .checked_mul(SIGNATURE_BYTES as u64)
                            .context("anchor signature byte offset overflow")?,
                    )?;
                    ensure!(
                        first_signature == target.mint_signature,
                        "anchor signature differs"
                    );
                }
                anchor_count = anchor_count
                    .checked_add(u64::from(is_anchor))
                    .context("anchor count overflow")?;
                transactions = transactions
                    .checked_add(1)
                    .context("transaction count overflow")?;
                signatures = signatures
                    .checked_add(u64::from(record.signature_count))
                    .context("signature count overflow")?;
            }
        }
    };
    ensure!(
        read_frame_hashed(&mut reader, &mut logical_offset, &mut hasher, &mut payload)?.is_none(),
        "consolidated stream has records after its footer"
    );
    let transaction_file = reader.into_inner();
    transaction_stamp.verify(&transaction_file, "consolidated transaction stream")?;
    ensure!(
        logical_offset == transaction_stamp.bytes,
        "transaction stream size changed"
    );
    ensure!(
        <[u8; 32]>::from(hasher.finalize()) == transaction_sha,
        "transaction digest differs"
    );
    let epoch_count = manifest
        .last_epoch
        .checked_sub(manifest.first_epoch)
        .and_then(|span| span.checked_add(1))
        .context("manifest epoch count overflow")?;
    ensure!(
        transactions == manifest.transactions
            && signatures == signature_count
            && anchor_count == 1
            && footer.epochs == epoch_count
            && footer.transactions_written == transactions
            && footer.transactions_scanned >= transactions
            && footer.pubkeys == pubkey_count
            && footer.signatures == signatures
            && footer.owned_block_fallbacks <= footer.blocks_scanned
            && footer.raw_transaction_fallbacks == 0
            && footer.raw_metadata_fallbacks == 0,
        "consolidated stream counters differ from its manifest"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{
        ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
        ArchiveV2HotMessagePayload, ArchiveV2HotV0Message, CompactInnerInstruction,
        CompactInnerInstructions, CompactLogStream, CompactMessageHeader, CompactMetaV1,
        CompactTransactionError, DataTable, LogEvent, OwnedCompactAddressTableLookup,
        OwnedCompactRecentBlockhash, StringTable, program_logs::ProgramLog, wincode_leb128_config,
    };
    use tempfile::tempdir;

    fn key(byte: u8) -> [u8; KEY_BYTES] {
        [byte; KEY_BYTES]
    }

    fn read_locator(slot: u64, payload_offset: u64, payload_len: u32) -> TransactionLocator {
        TransactionLocator {
            slot,
            source_block_id: u32::try_from(slot).unwrap(),
            tx_index: 0,
            payload_offset,
            payload_len,
        }
    }

    fn write_locator_fixture(path: &Path, locators: &[TransactionLocator]) {
        let bytes = locators
            .iter()
            .flat_map(|locator| locator.encode())
            .collect::<Vec<_>>();
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn prior_trusted_size_binding_matches_the_spyx_extraction_identity() {
        let files = [
            ("archive-v2-blocks.index", 22_427_376),
            ("archive-v2-blocks.zstd", 87_207_941_646),
            ("archive-v2-meta.wincode", 66),
            ("blockhash_registry.bin", 13_801_440),
            ("registry.bin", 1_262_495_936),
            ("registry.mphf", 484_082_107),
            ("signatures.bin", 46_134_926_144),
            ("vote_hash_registry.bin", 28_034_175),
        ]
        .into_iter()
        .map(
            |(name, size)| blockzilla_read_sdk::manifest::GenerationFile {
                name: name.to_owned(),
                size,
                sha256: "0".repeat(64),
            },
        )
        .collect();
        let manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 801,
            generation_id: "token-transaction-dump-trusted-local-sizes-v1".to_owned(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: 432_000,
            complete: true,
            files,
        };

        assert_eq!(
            hex_digest(
                prior_trusted_sizes_digest(
                    &manifest,
                    ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                )
                .unwrap()
            ),
            "6b78a2ebbf23805ac9a6b7a852d9926a42a4ad458b43aa97bceff9a5de71777c"
        );
    }

    #[test]
    fn external_key_runs_sort_deduplicate_and_build_lookup_prefixes() {
        let temporary = tempdir().unwrap();
        let runs = temporary.path().join("key-runs");
        let output = temporary.path().join("registry.bin");
        let mut sorter = RawKeySorter::new(&runs, KEY_BYTES * 2).unwrap();
        for value in [key(4), key(1), key(3), key(1), key(2), key(4)] {
            sorter.push(value).unwrap();
        }
        let build = sorter.finish(&output).unwrap();
        let expected = [key(1), key(2), key(3), key(4)].concat();
        assert_eq!(fs::read(&output).unwrap(), expected);
        assert_eq!(build.rows, 4);
        assert_eq!(build.sha256, sha256_bytes(&expected));

        let lookup = GlobalRegistryLookup::new(&build, &output).unwrap();
        assert_eq!(lookup.lookup(&key(1)).unwrap(), 1);
        assert_eq!(lookup.lookup(&key(4)).unwrap(), 4);
        assert!(lookup.lookup(&key(5)).is_err());
        lookup.verify_unchanged().unwrap();

        let displaced = temporary.path().join("registry-displaced.bin");
        fs::rename(&output, &displaced).unwrap();
        fs::write(&output, &expected).unwrap();
        assert!(
            verify_path_binding(&output, &lookup.stamp, "test registry")
                .unwrap_err()
                .to_string()
                .contains("path binding changed")
        );

        let mutation_path = temporary.path().join("registry-mutated.bin");
        let mutation_runs = temporary.path().join("mutation-runs");
        let mut mutation_sorter = RawKeySorter::new(&mutation_runs, KEY_BYTES * 2).unwrap();
        for value in [key(1), key(2), key(3), key(4)] {
            mutation_sorter.push(value).unwrap();
        }
        let mutation_build = mutation_sorter.finish(&mutation_path).unwrap();
        let mutation_lookup = GlobalRegistryLookup::new(&mutation_build, &mutation_path).unwrap();
        let mut changed = expected;
        changed[..KEY_BYTES].copy_from_slice(&key(4));
        fs::write(&mutation_path, changed).unwrap();
        assert!(
            mutation_lookup
                .verify_unchanged()
                .unwrap_err()
                .to_string()
                .contains("changed while it was in use")
        );
    }

    fn locator(slot: u64, block: u32, tx: u32, offset: u64) -> TransactionLocator {
        TransactionLocator {
            slot,
            source_block_id: block,
            tx_index: tx,
            payload_offset: offset,
            payload_len: 10,
        }
    }

    #[test]
    fn external_locator_runs_produce_canonical_order_and_reject_duplicates() {
        let temporary = tempdir().unwrap();
        let output = temporary.path().join("locators.bin");
        let mut sorter = LocatorSorter::new(
            temporary.path(),
            7,
            std::mem::size_of::<TransactionLocator>() * 2,
        )
        .unwrap();
        sorter.push(locator(12, 2, 1, 300)).unwrap();
        sorter.push(locator(10, 1, 1, 200)).unwrap();
        sorter.push(locator(10, 1, 0, 100)).unwrap();
        assert_eq!(sorter.finish(&output).unwrap(), 3);
        let mut reader = LocatorFileReader::open(&output).unwrap();
        assert_eq!(reader.next().unwrap().unwrap().key(), (10, 1, 0));
        assert_eq!(reader.next().unwrap().unwrap().key(), (10, 1, 1));
        assert_eq!(reader.next().unwrap().unwrap().key(), (12, 2, 1));
        assert!(reader.next().unwrap().is_none());

        let duplicate_output = temporary.path().join("duplicate.bin");
        let mut duplicate = LocatorSorter::new(
            temporary.path(),
            8,
            std::mem::size_of::<TransactionLocator>(),
        )
        .unwrap();
        duplicate.push(locator(20, 4, 0, 100)).unwrap();
        duplicate.push(locator(20, 4, 0, 200)).unwrap();
        assert!(duplicate.finish(&duplicate_output).is_err());
    }

    #[test]
    fn phase_two_read_arena_sorts_only_the_physical_plan_and_reads_exact_ranges() {
        let temporary = tempdir().unwrap();
        let source_path = temporary.path().join("raw-stream.bin");
        let source_bytes = (0..120_000usize)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&source_path, &source_bytes).unwrap();
        let source = File::open(source_path).unwrap();
        let locator_path = temporary.path().join("locators.bin");
        let canonical = [
            read_locator(1, 100_000, 4),
            read_locator(2, 10_000, 4),
            read_locator(3, 10_010, 3),
        ];
        write_locator_fixture(&locator_path, &canonical);

        let mut reader = LocatorFileReader::open(&locator_path).unwrap();
        let mut previous_key = None;
        let mut batch = PassTwoReadBatch::default();
        assert!(
            batch
                .fill(&mut reader, 7, source_bytes.len() as u64, &mut previous_key,)
                .unwrap()
        );
        assert_eq!(batch.locators, canonical);
        assert_eq!(batch.physical_order, [1, 2, 0]);
        assert_eq!(
            batch.ranges,
            [
                PassTwoReadRange {
                    source_offset: 10_000,
                    arena_offset: 0,
                    bytes: 13,
                },
                PassTwoReadRange {
                    source_offset: 100_000,
                    arena_offset: 13,
                    bytes: 4,
                },
            ]
        );
        assert_eq!(batch.payload_bytes, 11);
        assert_eq!(batch.extra_gap_bytes, 6);
        assert_eq!(batch.arena_bytes, 17);

        let mut stats = PassTwoReadStats::default();
        batch.read_exact(&source, &mut stats).unwrap();
        for (index, locator) in canonical.iter().enumerate() {
            let start = usize::try_from(locator.payload_offset).unwrap();
            let end = start + usize::try_from(locator.payload_len).unwrap();
            assert_eq!(batch.payload(index).unwrap(), &source_bytes[start..end]);
        }
        assert_eq!(stats.locator_payload_bytes, 11);
        assert_eq!(stats.physical_read_ranges, 2);
        assert!(stats.physical_read_calls >= stats.physical_read_ranges);
        assert_eq!(stats.physical_read_bytes, 17);
        assert_eq!(stats.arena_high_water_bytes, 17);
        assert!(
            !batch
                .fill(&mut reader, 7, source_bytes.len() as u64, &mut previous_key,)
                .unwrap()
        );
    }

    #[test]
    fn phase_two_read_arena_rejects_overlaps_and_checked_offset_overflow() {
        let temporary = tempdir().unwrap();
        let overlap_path = temporary.path().join("overlap.bin");
        write_locator_fixture(
            &overlap_path,
            &[read_locator(1, 10, 10), read_locator(2, 15, 5)],
        );
        let mut reader = LocatorFileReader::open(&overlap_path).unwrap();
        let error = PassTwoReadBatch::default()
            .fill(&mut reader, 7, 100, &mut None)
            .unwrap_err();
        assert!(error.to_string().contains("payload ranges overlap"));

        let overflow_path = temporary.path().join("overflow.bin");
        write_locator_fixture(&overflow_path, &[read_locator(1, u64::MAX, 1)]);
        let mut reader = LocatorFileReader::open(&overflow_path).unwrap();
        let error = PassTwoReadBatch::default()
            .fill(&mut reader, 7, u64::MAX, &mut None)
            .unwrap_err();
        assert!(error.to_string().contains("payload range overflow"));
    }

    #[test]
    fn phase_two_read_batches_enforce_count_range_and_singleton_bounds_with_reuse() {
        let temporary = tempdir().unwrap();
        let count_path = temporary.path().join("count-bound.bin");
        let count_locators = (0..PASS_TWO_READ_BATCH_LOCATORS + 1)
            .map(|index| {
                read_locator(
                    u64::try_from(index + 1).unwrap(),
                    u64::try_from(index * 2).unwrap(),
                    1,
                )
            })
            .collect::<Vec<_>>();
        write_locator_fixture(&count_path, &count_locators);
        let mut reader = LocatorFileReader::open(&count_path).unwrap();
        let mut previous_key = None;
        let mut batch = PassTwoReadBatch::default();
        assert!(
            batch
                .fill(
                    &mut reader,
                    7,
                    u64::try_from(count_locators.len() * 2).unwrap(),
                    &mut previous_key,
                )
                .unwrap()
        );
        assert_eq!(batch.locators.len(), PASS_TWO_READ_BATCH_LOCATORS);
        assert!(
            batch
                .fill(
                    &mut reader,
                    7,
                    u64::try_from(count_locators.len() * 2).unwrap(),
                    &mut previous_key,
                )
                .unwrap()
        );
        assert_eq!(batch.locators.len(), 1);

        let exact_range = u32::try_from(PASS_TWO_READ_MAX_RANGE_BYTES).unwrap();
        let direct_bytes = exact_range + 1;
        let range_path = temporary.path().join("range-bound.bin");
        let range_locators = [
            read_locator(1, 0, exact_range),
            read_locator(2, u64::from(exact_range), 1),
            read_locator(3, 20 << 20, direct_bytes),
            read_locator(4, 30 << 20, 4),
        ];
        write_locator_fixture(&range_path, &range_locators);
        let mut reader = LocatorFileReader::open(&range_path).unwrap();
        let mut previous_key = None;
        batch.reset_epoch();
        assert!(
            batch
                .fill(&mut reader, 7, 40 << 20, &mut previous_key)
                .unwrap()
        );
        assert_eq!(batch.locators.len(), 2);
        assert!(!batch.direct_singleton);
        assert_eq!(batch.ranges.len(), 2);
        assert!(
            batch
                .ranges
                .iter()
                .all(|range| range.bytes <= PASS_TWO_READ_MAX_RANGE_BYTES)
        );
        let arena_pointer = batch.arena.as_ptr();
        let arena_high_water = batch.arena.len();

        assert!(
            batch
                .fill(&mut reader, 7, 40 << 20, &mut previous_key)
                .unwrap()
        );
        assert_eq!(batch.locators.len(), 1);
        assert!(batch.direct_singleton);
        assert_eq!(
            batch.ranges[0].bytes,
            usize::try_from(direct_bytes).unwrap()
        );
        assert_eq!(batch.arena.as_ptr(), arena_pointer);
        assert_eq!(batch.arena.len(), arena_high_water);

        assert!(
            batch
                .fill(&mut reader, 7, 40 << 20, &mut previous_key)
                .unwrap()
        );
        assert_eq!(batch.locators.len(), 1);
        assert!(!batch.direct_singleton);
        assert_eq!(batch.arena_bytes, 4);
        assert_eq!(batch.arena.as_ptr(), arena_pointer);
        assert_eq!(batch.arena.len(), arena_high_water);
    }

    #[test]
    fn signature_batch_preserves_occurrence_order_and_repetitions() {
        let temporary = tempdir().unwrap();
        let source_path = temporary.path().join("source-signatures.bin");
        let mut source_bytes = Vec::new();
        for value in 0u8..8 {
            source_bytes.extend_from_slice(&[value; SIGNATURE_BYTES]);
        }
        fs::write(&source_path, source_bytes).unwrap();
        let source = File::open(source_path).unwrap();
        let output_path = temporary.path().join("signatures.bin");
        let mut output = DigestWriter::create(&output_path).unwrap();
        let mut batch = SignatureBatch::default();
        batch.push(3, 2, Some([3; SIGNATURE_BYTES])).unwrap();
        batch.push(1, 1, None).unwrap();
        batch.push(3, 1, None).unwrap();
        batch.flush(&source, &mut output).unwrap();
        let binding = output.finish().unwrap();

        let expected = [
            [3; SIGNATURE_BYTES],
            [4; SIGNATURE_BYTES],
            [1; SIGNATURE_BYTES],
            [3; SIGNATURE_BYTES],
        ]
        .concat();
        assert_eq!(fs::read(output_path).unwrap(), expected);
        assert_eq!(binding.bytes, expected.len() as u64);
        assert_eq!(binding.sha256, sha256_bytes(&expected));
        assert_eq!(
            batch.stats(),
            SignatureReadStats {
                input_ranges: 3,
                input_bytes: expected.len() as u64,
                physical_read_ranges: 3,
                physical_read_calls: 3,
                physical_read_bytes: expected.len() as u64,
            }
        );
    }

    #[test]
    fn signature_batch_merges_only_exactly_adjacent_source_ranges() {
        let temporary = tempdir().unwrap();
        let source_path = temporary.path().join("source-signatures.bin");
        let mut source_bytes = Vec::new();
        for value in 0u8..8 {
            source_bytes.extend_from_slice(&[value; SIGNATURE_BYTES]);
        }
        fs::write(&source_path, source_bytes).unwrap();
        let source = File::open(source_path).unwrap();
        let output_path = temporary.path().join("signatures.bin");
        let mut output = DigestWriter::create(&output_path).unwrap();
        let mut batch = SignatureBatch::default();

        batch.push(1, 1, None).unwrap();
        batch.push(2, 2, Some([2; SIGNATURE_BYTES])).unwrap();
        batch.push(5, 1, None).unwrap();
        batch.push(6, 1, None).unwrap();
        // This repeated source occurrence is not adjacent to the preceding
        // physical source range and must remain a separate read range.
        batch.push(2, 1, None).unwrap();

        assert_eq!(batch.ranges.len(), 3);
        assert_eq!(
            batch.ranges[0],
            SignatureRange {
                source_offset: SIGNATURE_BYTES as u64,
                destination_start: 0,
                bytes: 3 * SIGNATURE_BYTES,
            }
        );
        assert_eq!(
            batch.ranges[1],
            SignatureRange {
                source_offset: 5 * SIGNATURE_BYTES as u64,
                destination_start: 3 * SIGNATURE_BYTES,
                bytes: 2 * SIGNATURE_BYTES,
            }
        );
        assert_eq!(
            batch.ranges[2],
            SignatureRange {
                source_offset: 2 * SIGNATURE_BYTES as u64,
                destination_start: 5 * SIGNATURE_BYTES,
                bytes: SIGNATURE_BYTES,
            }
        );

        batch.flush(&source, &mut output).unwrap();
        let binding = output.finish().unwrap();
        let expected = [
            [1; SIGNATURE_BYTES],
            [2; SIGNATURE_BYTES],
            [3; SIGNATURE_BYTES],
            [5; SIGNATURE_BYTES],
            [6; SIGNATURE_BYTES],
            [2; SIGNATURE_BYTES],
        ]
        .concat();
        assert_eq!(fs::read(output_path).unwrap(), expected);
        assert_eq!(binding.bytes, expected.len() as u64);
        assert_eq!(binding.sha256, sha256_bytes(&expected));
        assert_eq!(
            batch.stats(),
            SignatureReadStats {
                input_ranges: 5,
                input_bytes: expected.len() as u64,
                physical_read_ranges: 3,
                physical_read_calls: 3,
                physical_read_bytes: expected.len() as u64,
            }
        );
    }

    #[test]
    fn signature_batch_reuses_high_water_bytes_and_reads_one_range_directly() {
        let temporary = tempdir().unwrap();
        let source_path = temporary.path().join("source-signatures.bin");
        let mut source_bytes = Vec::new();
        for value in 0u8..8 {
            source_bytes.extend_from_slice(&[value; SIGNATURE_BYTES]);
        }
        fs::write(&source_path, source_bytes).unwrap();
        let source = File::open(source_path).unwrap();
        let output_path = temporary.path().join("signatures.bin");
        let mut output = DigestWriter::create(&output_path).unwrap();
        let mut batch = SignatureBatch::default();

        batch.push(3, 3, Some([3; SIGNATURE_BYTES])).unwrap();
        let high_water_pointer = batch.bytes.as_ptr();
        let high_water_len = batch.bytes.len();
        let high_water_capacity = batch.bytes.capacity();
        batch.flush(&source, &mut output).unwrap();
        assert_eq!(batch.byte_len, 0);
        assert_eq!(batch.bytes.as_ptr(), high_water_pointer);
        assert_eq!(batch.bytes.len(), high_water_len);
        assert_eq!(batch.bytes.capacity(), high_water_capacity);

        // One physical range selects the current-thread read path. The smaller
        // batch must use only its logical bytes, without clearing or growing
        // the retained high-water buffer.
        batch.push(1, 1, Some([1; SIGNATURE_BYTES])).unwrap();
        assert_eq!(batch.ranges.len(), 1);
        assert_eq!(batch.byte_len, SIGNATURE_BYTES);
        assert_eq!(batch.bytes.as_ptr(), high_water_pointer);
        assert_eq!(batch.bytes.len(), high_water_len);
        assert_eq!(batch.bytes.capacity(), high_water_capacity);
        batch.flush(&source, &mut output).unwrap();
        let binding = output.finish().unwrap();

        let expected = [
            [3; SIGNATURE_BYTES],
            [4; SIGNATURE_BYTES],
            [5; SIGNATURE_BYTES],
            [1; SIGNATURE_BYTES],
        ]
        .concat();
        assert_eq!(fs::read(output_path).unwrap(), expected);
        assert_eq!(binding.bytes, expected.len() as u64);
        assert_eq!(binding.sha256, sha256_bytes(&expected));
        assert_eq!(
            batch.stats(),
            SignatureReadStats {
                input_ranges: 2,
                input_bytes: expected.len() as u64,
                physical_read_ranges: 2,
                physical_read_calls: 2,
                physical_read_bytes: expected.len() as u64,
            }
        );
    }

    #[test]
    fn source_id_map_is_sorted_compact_and_pinned() {
        let temporary = tempdir().unwrap();
        let registry_path = temporary.path().join("source-registry.bin");
        let mut registry_bytes = vec![0u8; 132 * KEY_BYTES];
        registry_bytes[..KEY_BYTES].copy_from_slice(&key(4));
        registry_bytes[KEY_BYTES..2 * KEY_BYTES].copy_from_slice(&key(1));
        registry_bytes[131 * KEY_BYTES..].copy_from_slice(&key(3));
        fs::write(&registry_path, registry_bytes).unwrap();
        let registry_file = File::open(&registry_path).unwrap();
        let registry_stamp = FileStamp::read(&registry_file).unwrap();

        let mut seen = vec![0u64; 132usize.div_ceil(64)];
        for id in [1u32, 2, 132] {
            let index = usize::try_from(id - 1).unwrap();
            seen[index / 64] |= 1u64 << (index % 64);
        }
        let mut aliases = vec![(2, key(1))];
        let key_runs = temporary.path().join("key-runs");
        let mut sorter = RawKeySorter::new(&key_runs, KEY_BYTES * 2).unwrap();
        let map_path = temporary.path().join("source-id.map");
        let (source_id_map_file, binding, stats) = write_source_id_map(
            7,
            &registry_file,
            132,
            &registry_stamp,
            &seen,
            &mut aliases,
            &map_path,
            &mut sorter,
        )
        .unwrap();
        assert_eq!(binding.rows, 3);
        assert_eq!(binding.bytes, 3 * SOURCE_ID_MAP_ROW_BYTES as u64);
        assert_eq!(
            stats,
            SourceIdResolutionStats {
                rows: 3,
                read_calls: 1,
                read_bytes: 132 * KEY_BYTES as u64,
            }
        );
        let map_bytes = fs::read(&map_path).unwrap();
        let expected = [(1u32, key(4)), (2, key(1)), (132, key(3))]
            .into_iter()
            .flat_map(|(id, raw)| id.to_le_bytes().into_iter().chain(raw))
            .collect::<Vec<_>>();
        assert_eq!(map_bytes, expected);
        assert_eq!(binding.stamp.bytes, binding.bytes);

        let global_path = temporary.path().join("global-registry.bin");
        let build = sorter.finish(&global_path).unwrap();
        let global = GlobalRegistryLookup::new(&build, &global_path).unwrap();
        let stream_path = temporary.path().join("stream");
        fs::write(&stream_path, []).unwrap();
        let stream_file = File::open(stream_path).unwrap();
        let stream_stamp = FileStamp::read(&stream_file).unwrap();
        let signatures_path = temporary.path().join("signatures");
        fs::write(&signatures_path, []).unwrap();
        let signatures_file = File::open(signatures_path).unwrap();
        let plan = EpochPlan {
            epoch: 7,
            stream_file,
            stream_stamp,
            locator_path: temporary.path().join("locators"),
            source_id_map_path: map_path.clone(),
            source_id_map_file,
            source_id_map: binding,
            source_generation_digest: [0; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            registry_entries: 132,
            registry_stamp,
            signatures_stamp: FileStamp::read(&signatures_file).unwrap(),
            slots_per_epoch: 432_000,
            transaction_count: 0,
            signature_count: 0,
        };
        let mut dense = Vec::new();
        let mut touched = Vec::new();
        let mut read_buffer = Vec::new();
        load_dense_source_id_map_with_chunk_limit(
            &plan,
            &global,
            &mut dense,
            &mut touched,
            &mut read_buffer,
            2 * SOURCE_ID_MAP_ROW_BYTES,
        )
        .unwrap();
        assert_eq!([dense[0], dense[1], dense[131]], [3, 1, 2]);
        assert_eq!(touched, [0, 1, 131]);
        assert_eq!(read_buffer.len(), 2 * SOURCE_ID_MAP_ROW_BYTES);
        let dense_pointer = dense.as_ptr();
        let dense_capacity = dense.capacity();
        let read_buffer_pointer = read_buffer.as_ptr();
        load_dense_source_id_map_with_chunk_limit(
            &plan,
            &global,
            &mut dense,
            &mut touched,
            &mut read_buffer,
            2 * SOURCE_ID_MAP_ROW_BYTES,
        )
        .unwrap();
        assert_eq!(dense.as_ptr(), dense_pointer);
        assert_eq!(dense.capacity(), dense_capacity);
        assert_eq!(read_buffer.as_ptr(), read_buffer_pointer);
        assert_eq!(touched, [0, 1, 131]);

        let mut changed = map_bytes;
        changed[4 + SOURCE_ID_MAP_ROW_BYTES..4 + SOURCE_ID_MAP_ROW_BYTES + KEY_BYTES]
            .copy_from_slice(&key(4));
        fs::write(&map_path, changed).unwrap();
        let error =
            load_dense_source_id_map(&plan, &global, &mut dense, &mut touched, &mut read_buffer)
                .unwrap_err();
        assert!(error.to_string().contains("changed while it was in use"));
    }

    #[test]
    fn dense_source_id_scratch_clears_touched_entries_across_resize_and_reuse() {
        let mut dense = vec![0u32; 6];
        let mut touched = vec![1u32, 5];
        dense[1] = 10;
        dense[5] = 20;

        reset_dense_source_ids(3, &mut dense, &mut touched).unwrap();
        assert_eq!(dense, [0, 0, 0]);
        assert!(touched.is_empty());

        dense[2] = 30;
        touched.push(2);
        reset_dense_source_ids(8, &mut dense, &mut touched).unwrap();
        assert_eq!(dense, [0; 8]);
        assert!(touched.is_empty());

        dense[7] = 40;
        touched.push(7);
        reset_dense_source_ids(4, &mut dense, &mut touched).unwrap();
        assert_eq!(dense, [0; 4]);
        assert!(touched.is_empty());

        dense[1] = 50;
        touched.push(1);
        reset_dense_source_ids(4, &mut dense, &mut touched).unwrap();
        assert_eq!(dense, [0; 4]);
        assert!(touched.is_empty());
    }

    #[test]
    fn pass_one_transaction_decode_borrows_wire_byte_fields() {
        let message_bytes = vec![11, 12, 13, 14, 15];
        let metadata_bytes = vec![1, 21, 22, 23];
        let owned = TokenTransactionDumpRecord::Transaction(TokenTransactionRecord {
            source_epoch: 801,
            source_generation_digest: [8; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 9,
            block: TokenTransactionBlockContext {
                slot: 346_066_298,
                parent_slot: 346_066_297,
                blockhash_id: u32::MAX,
                previous_blockhash_id: 7,
                block_time: Some(10),
                block_height: Some(11),
                transaction_count: 3,
            },
            tx_index: 2,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_ERROR,
            source_first_signature_ordinal: 42,
            signature_count: 2,
            dump_signature_ordinal: None,
            message_bytes: message_bytes.clone(),
            metadata_bytes: metadata_bytes.clone(),
        });
        let mut wire = Vec::new();
        encode_with_scratch(&owned, &mut wire).unwrap();

        let borrowed = decode_borrowed_frame(&wire).unwrap();
        let BorrowedDumpRecord::Transaction(record) = &borrowed else {
            panic!("borrowed record variant changed")
        };
        assert_eq!(record.message_bytes, message_bytes);
        assert_eq!(record.metadata_bytes, metadata_bytes);
        let wire_start = wire.as_ptr() as usize;
        let wire_end = wire_start + wire.len();
        for bytes in [record.message_bytes, record.metadata_bytes] {
            let start = bytes.as_ptr() as usize;
            assert!(start >= wire_start && start + bytes.len() <= wire_end);
        }

        let mut borrowed_wire = Vec::new();
        encode_with_scratch(&borrowed, &mut borrowed_wire).unwrap();
        assert_eq!(borrowed_wire, wire);
        let mut trailing = wire;
        trailing.push(0);
        assert!(decode_borrowed_frame(&trailing).is_err());
    }

    #[test]
    fn ambiguous_metadata_probe_reuses_caller_scratch() {
        let metadata = CompactMetaV1 {
            err: Some(CompactTransactionError::AccountInUse),
            fee: 0,
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
        };
        let mut input = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        assert_eq!(input[0], 1);
        input[1] = 4;

        let mut current_output = Vec::new();
        let mut legacy_output = Vec::new();
        assert_eq!(
            probe_ambiguous_metadata_schema(&input, &mut current_output, &mut legacy_output)
                .unwrap(),
            MetadataSchemaSelection::Current
        );
        let current_pointer = current_output.as_ptr();
        let current_capacity = current_output.capacity();
        let legacy_pointer = legacy_output.as_ptr();
        let legacy_capacity = legacy_output.capacity();
        assert_eq!(
            probe_ambiguous_metadata_schema(&input, &mut current_output, &mut legacy_output)
                .unwrap(),
            MetadataSchemaSelection::Current
        );
        assert_eq!(current_output, input);
        assert_eq!(current_output.as_ptr(), current_pointer);
        assert_eq!(current_output.capacity(), current_capacity);
        assert_eq!(legacy_output.as_ptr(), legacy_pointer);
        assert_eq!(legacy_output.capacity(), legacy_capacity);

        let mut pending = Vec::new();
        collect_metadata_pubkeys(
            &input,
            &mut pending,
            &[],
            0,
            &mut current_output,
            &mut legacy_output,
        )
        .unwrap();
        assert!(pending.is_empty());
        assert_eq!(current_output, input);
        assert_eq!(current_output.as_ptr(), current_pointer);
        assert_eq!(legacy_output.as_ptr(), legacy_pointer);

        let mut trailing = input;
        trailing.push(0);
        assert!(
            probe_ambiguous_metadata_schema(&trailing, &mut current_output, &mut legacy_output)
                .is_err()
        );
    }

    #[test]
    fn remap_rewrites_only_pubkeys_and_preserves_blockhash_ids() {
        let temporary = tempdir().unwrap();
        let key_runs = temporary.path().join("global-runs");
        let global_path = temporary.path().join("global-registry.bin");
        let mut sorter = RawKeySorter::new(&key_runs, KEY_BYTES * 2).unwrap();
        sorter.push(key(4)).unwrap();
        sorter.push(key(1)).unwrap();
        let build = sorter.finish(&global_path).unwrap();
        let global = GlobalRegistryLookup::new(&build, &global_path).unwrap();

        let source_message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Raw(key(1))],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(-17),
            instructions: Vec::new(),
        });
        let metadata = CompactMetaV1 {
            err: Some(CompactTransactionError::CommitCancelled),
            fee: 5_000,
            pre_balances: vec![1, 2],
            post_balances: vec![1, 2],
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: vec![CompactPubkey::Id(2)],
            loaded_readonly_addresses: vec![CompactPubkey::Raw(key(4))],
            return_data: None,
            compute_units_consumed: Some(10),
            cost_units: Some(11),
        };
        let record = TokenTransactionRecord {
            source_epoch: 1,
            source_generation_digest: [9; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 2,
            block: TokenTransactionBlockContext {
                slot: 105,
                parent_slot: 104,
                blockhash_id: 0xf000_0001,
                previous_blockhash_id: 0x8000_0002,
                block_time: None,
                block_height: None,
                transaction_count: 1,
            },
            tx_index: 0,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_ERROR,
            source_first_signature_ordinal: 0,
            signature_count: 1,
            dump_signature_ordinal: None,
            message_bytes: wincode::config::serialize(&source_message, wincode_leb128_config())
                .unwrap(),
            metadata_bytes: wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap(),
        };
        let original_block = BlockIdentity::from(&record.block);
        // Source ID 1 is key(4), and source ID 2 is key(1).
        let dense = vec![2, 1];
        let mut message_output = Vec::new();
        let mut metadata_output = Vec::new();
        let mut metadata_comparison_output = Vec::new();
        rewrite_transaction_pubkeys(
            (&record).into(),
            &global,
            &dense,
            &mut message_output,
            &mut metadata_output,
            &mut metadata_comparison_output,
        )
        .unwrap();
        assert_eq!(BlockIdentity::from(&record.block), original_block);

        let rewritten_message: ArchiveV2HotMessagePayload =
            wincode::config::deserialize_exact(&message_output, wincode_leb128_config()).unwrap();
        let ArchiveV2HotMessagePayload::Legacy(rewritten_message) = rewritten_message else {
            panic!("message variant changed")
        };
        assert_eq!(
            rewritten_message.account_keys,
            [CompactPubkey::Id(2), CompactPubkey::Id(1)]
        );
        assert!(matches!(
            rewritten_message.recent_blockhash,
            OwnedCompactRecentBlockhash::Id(-17)
        ));

        let rewritten_metadata: CompactMetaV1 =
            wincode::config::deserialize_exact(&metadata_output, wincode_leb128_config()).unwrap();
        assert_eq!(
            rewritten_metadata.loaded_writable_addresses,
            [CompactPubkey::Id(1)]
        );
        assert_eq!(
            rewritten_metadata.loaded_readonly_addresses,
            [CompactPubkey::Id(2)]
        );
        assert!(matches!(
            rewritten_metadata.err,
            Some(CompactTransactionError::CommitCancelled)
        ));

        let dump_signature_ordinal = Some(77);
        let borrowed_output = BorrowedDumpRecord::Transaction(BorrowedTransactionRecord {
            source_epoch: record.source_epoch,
            source_generation_digest: record.source_generation_digest,
            source_wire_profile: record.source_wire_profile,
            source_block_id: record.source_block_id,
            block: record.block.clone(),
            tx_index: record.tx_index,
            flags: record.flags,
            source_first_signature_ordinal: record.source_first_signature_ordinal,
            signature_count: record.signature_count,
            dump_signature_ordinal,
            message_bytes: &message_output,
            metadata_bytes: &metadata_output,
        });
        let mut borrowed_wire = Vec::new();
        encode_with_scratch(&borrowed_output, &mut borrowed_wire).unwrap();
        let owned_output = TokenTransactionDumpRecord::Transaction(TokenTransactionRecord {
            source_epoch: record.source_epoch,
            source_generation_digest: record.source_generation_digest,
            source_wire_profile: record.source_wire_profile,
            source_block_id: record.source_block_id,
            block: record.block.clone(),
            tx_index: record.tx_index,
            flags: record.flags,
            source_first_signature_ordinal: record.source_first_signature_ordinal,
            signature_count: record.signature_count,
            dump_signature_ordinal,
            message_bytes: message_output,
            metadata_bytes: metadata_output,
        });
        let mut owned_wire = Vec::new();
        encode_with_scratch(&owned_output, &mut owned_wire).unwrap();
        assert_eq!(borrowed_wire, owned_wire);
    }

    #[test]
    fn program_accumulator_tracks_origin_and_distinct_transactions() {
        let coordinate = ProgramInventoryCoordinate {
            source_epoch: 801,
            slot: 346_066_298,
            source_block_id: 34_188,
            tx_index: 1_509,
        };
        let mut table = ProgramAccumulatorTable::new(8).unwrap();
        table
            .record_count(3, 0, coordinate, ProgramInventoryOrigin::Outer, 2)
            .unwrap();
        table
            .record(3, 0, coordinate, ProgramInventoryOrigin::Inner)
            .unwrap();
        assert!(table.record_target_account_inner(3, 0, 2, 1).unwrap());
        table
            .record(3, 0, coordinate, ProgramInventoryOrigin::Inner)
            .unwrap();
        assert!(!table.record_target_account_inner(3, 0, 0, 2).unwrap());
        table
            .record(3, 1, coordinate, ProgramInventoryOrigin::Outer)
            .unwrap();
        table
            .record(3, 1, coordinate, ProgramInventoryOrigin::Inner)
            .unwrap();
        assert!(table.record_target_account_inner(3, 1, 1, 1).unwrap());

        let program = table.programs[0];
        assert_eq!(program.outer_occurrences, 3);
        assert_eq!(program.inner_occurrences, 3);
        assert_eq!(program.transactions, 2);
        assert_eq!(program.outer_transactions, 2);
        assert_eq!(program.inner_transactions, 2);
        assert_eq!(program.target_account_inner_occurrences, 3);
        assert_eq!(program.target_account_inner_transactions, 2);
        assert_eq!(program.target_mint_inner_occurrences, 2);
        assert_eq!(program.target_token_account_inner_occurrences, 3);
        assert_eq!(program.target_account_inner_references, 7);
        assert_eq!(program.target_mint_inner_references, 3);
        assert_eq!(program.target_token_account_inner_references, 4);
        assert_eq!(program.first_transaction, coordinate);
        assert_eq!(program.first_origin, ProgramInventoryOrigin::Outer);
    }

    fn test_selected_program_log_set() -> SelectedProgramLogSet {
        let mut by_registry_id = vec![PROGRAM_ACCUMULATOR_MISSING; 6];
        by_registry_id[2] = 0;
        by_registry_id[3] = 1;
        SelectedProgramLogSet {
            input_sha256: [9; 32],
            by_registry_id,
            programs: vec![
                ProgramLogAccumulator::new(key(2), Some(2)),
                ProgramLogAccumulator::new(key(3), Some(3)),
            ],
            programs_present_in_registry: 2,
        }
    }

    #[test]
    fn only_non_bpf_custom_failure_has_ambiguous_provenance() {
        let compact = CompactPubkey::Id(2);
        let non_bpf = stage_borrowed_program_log_event_kind(BorrowedArchiveV2LogEvent {
            ordinal: 0,
            tag: 22,
            wire: &[],
            kind: BorrowedArchiveV2LogEventKind::FailureCustomProgramError {
                program: compact,
                code: 7,
            },
        });
        let bpf = stage_borrowed_program_log_event_kind(BorrowedArchiveV2LogEvent {
            ordinal: 1,
            tag: 23,
            wire: &[],
            kind: BorrowedArchiveV2LogEventKind::BpfFailureCustomProgramError {
                program: compact,
                code: 7,
            },
        });

        assert!(matches!(
            non_bpf.kind,
            StagedProgramLogEventKind::AmbiguousCustomFailure { program }
                if program == compact
        ));
        assert!(matches!(
            bpf.kind,
            StagedProgramLogEventKind::Terminal { program } if program == compact
        ));
    }

    #[test]
    fn strict_log_stack_holds_out_ambiguous_suffix_until_terminal_resync() {
        let events = [
            StagedProgramLogEvent {
                ordinal: 0,
                tag: 14,
                kind: StagedProgramLogEventKind::Invoke {
                    program: CompactPubkey::Id(2),
                    depth: Some(1),
                },
            },
            StagedProgramLogEvent {
                ordinal: 1,
                tag: 5,
                kind: StagedProgramLogEventKind::Unkeyed,
            },
            StagedProgramLogEvent {
                ordinal: 2,
                tag: 14,
                kind: StagedProgramLogEventKind::Invoke {
                    program: CompactPubkey::Id(3),
                    depth: Some(2),
                },
            },
            StagedProgramLogEvent {
                ordinal: 3,
                tag: 7,
                kind: StagedProgramLogEventKind::Explicit {
                    program: CompactPubkey::Id(3),
                },
            },
            StagedProgramLogEvent {
                ordinal: 4,
                tag: 5,
                kind: StagedProgramLogEventKind::Unkeyed,
            },
            StagedProgramLogEvent {
                ordinal: 5,
                tag: 22,
                kind: StagedProgramLogEventKind::AmbiguousCustomFailure {
                    program: CompactPubkey::Id(3),
                },
            },
            StagedProgramLogEvent {
                ordinal: 6,
                tag: 5,
                kind: StagedProgramLogEventKind::Unkeyed,
            },
            StagedProgramLogEvent {
                ordinal: 7,
                tag: 18,
                kind: StagedProgramLogEventKind::Terminal {
                    program: CompactPubkey::Id(3),
                },
            },
            StagedProgramLogEvent {
                ordinal: 8,
                tag: 5,
                kind: StagedProgramLogEventKind::Unkeyed,
            },
            StagedProgramLogEvent {
                ordinal: 9,
                tag: 18,
                kind: StagedProgramLogEventKind::Terminal {
                    program: CompactPubkey::Id(2),
                },
            },
        ];
        let coordinate = ProgramInventoryCoordinate {
            source_epoch: 1,
            slot: 1_001,
            source_block_id: 1,
            tx_index: 0,
        };
        let mut selected = test_selected_program_log_set();
        let mut counters = ProgramLogInventoryCounters::default();
        let mut stack = ProgramLogAttributionStack::default();
        let mut holdouts = Vec::new();
        let mut text_attributions = Vec::new();
        scan_staged_program_log_events(
            &events,
            0,
            coordinate,
            &mut stack,
            &mut selected,
            &mut counters,
            &mut holdouts,
            &mut text_attributions,
        )
        .unwrap();

        assert_eq!(selected.programs[0].explicit_evidence_events, 2);
        assert_eq!(selected.programs[0].attributed_unkeyed_events, 2);
        assert_eq!(selected.programs[1].explicit_evidence_events, 4);
        assert_eq!(selected.programs[1].attributed_unkeyed_events, 1);
        assert_eq!(counters.unattributed_unkeyed_events, 1);
        assert_eq!(counters.ambiguous_custom_failure_events, 1);
        assert_eq!(counters.stack_resynchronizations, 1);
        assert_eq!(text_attributions[6].contextual_selected, None);
        assert_eq!(text_attributions[8].contextual_selected, Some(0));
        assert!(holdouts.iter().any(|holdout| {
            holdout.reason == ProgramLogHoldoutReason::AmbiguousCustomProgramError
        }));
    }

    #[test]
    fn log_truncation_sticks_and_raw_boundaries_never_establish_a_stack() {
        let events = [
            StagedProgramLogEvent {
                ordinal: 0,
                tag: 14,
                kind: StagedProgramLogEventKind::Invoke {
                    program: CompactPubkey::Id(2),
                    depth: Some(1),
                },
            },
            StagedProgramLogEvent {
                ordinal: 1,
                tag: 1,
                kind: StagedProgramLogEventKind::Truncated,
            },
            StagedProgramLogEvent {
                ordinal: 2,
                tag: 14,
                kind: StagedProgramLogEventKind::Invoke {
                    program: CompactPubkey::Id(2),
                    depth: Some(1),
                },
            },
            StagedProgramLogEvent {
                ordinal: 3,
                tag: 5,
                kind: StagedProgramLogEventKind::Unkeyed,
            },
            StagedProgramLogEvent {
                ordinal: 4,
                tag: 14,
                kind: StagedProgramLogEventKind::Invoke {
                    program: CompactPubkey::Raw(key(3)),
                    depth: Some(1),
                },
            },
            StagedProgramLogEvent {
                ordinal: 5,
                tag: 5,
                kind: StagedProgramLogEventKind::Unkeyed,
            },
        ];
        let coordinate = ProgramInventoryCoordinate {
            source_epoch: 1,
            slot: 1_001,
            source_block_id: 1,
            tx_index: 0,
        };
        let mut selected = test_selected_program_log_set();
        let mut counters = ProgramLogInventoryCounters::default();
        let mut stack = ProgramLogAttributionStack::default();
        let mut holdouts = Vec::new();
        let mut text_attributions = Vec::new();
        scan_staged_program_log_events(
            &events,
            0,
            coordinate,
            &mut stack,
            &mut selected,
            &mut counters,
            &mut holdouts,
            &mut text_attributions,
        )
        .unwrap();

        assert_eq!(counters.log_truncated_events, 1);
        assert_eq!(counters.inline_raw_program_references, 1);
        assert_eq!(counters.unattributed_unkeyed_events, 2);
        assert_eq!(selected.programs[0].attributed_unkeyed_events, 0);
        assert!(holdouts.iter().any(|holdout| {
            holdout.reason == ProgramLogHoldoutReason::InlineRawProgramReference
        }));
    }

    #[test]
    fn mismatched_invoke_depth_does_not_create_a_call_edge() {
        let events = [
            StagedProgramLogEvent {
                ordinal: 0,
                tag: 14,
                kind: StagedProgramLogEventKind::Invoke {
                    program: CompactPubkey::Id(2),
                    depth: Some(1),
                },
            },
            StagedProgramLogEvent {
                ordinal: 1,
                tag: 14,
                kind: StagedProgramLogEventKind::Invoke {
                    program: CompactPubkey::Id(3),
                    depth: Some(3),
                },
            },
        ];
        let coordinate = ProgramInventoryCoordinate {
            source_epoch: 1,
            slot: 1_001,
            source_block_id: 1,
            tx_index: 0,
        };
        let mut selected = test_selected_program_log_set();
        let mut counters = ProgramLogInventoryCounters::default();
        let mut stack = ProgramLogAttributionStack::default();
        let mut holdouts = Vec::new();
        let mut text_attributions = Vec::new();
        scan_staged_program_log_events(
            &events,
            0,
            coordinate,
            &mut stack,
            &mut selected,
            &mut counters,
            &mut holdouts,
            &mut text_attributions,
        )
        .unwrap();

        assert_eq!(counters.selected_caller_edge_observations, 0);
        assert_eq!(counters.selected_callee_edge_observations, 0);
        assert!(
            selected
                .programs
                .iter()
                .all(|program| { program.callers.is_empty() && program.callees.is_empty() })
        );
    }

    #[test]
    fn program_inventory_order_is_total_descending_then_raw_key() {
        let coordinate = ProgramInventoryCoordinate {
            source_epoch: 801,
            slot: 1,
            source_block_id: 1,
            tx_index: 0,
        };
        let mut table = ProgramAccumulatorTable::new(3).unwrap();
        table
            .record_count(2, 0, coordinate, ProgramInventoryOrigin::Outer, 3)
            .unwrap();
        table
            .record_count(3, 0, coordinate, ProgramInventoryOrigin::Inner, 5)
            .unwrap();
        table
            .record_count(1, 0, coordinate, ProgramInventoryOrigin::Outer, 3)
            .unwrap();
        let registry = [key(1), key(2), key(3)].concat();
        sort_program_accumulators(&mut table.programs, &registry);
        assert_eq!(
            table
                .programs
                .iter()
                .map(|program| program.registry_id)
                .collect::<Vec<_>>(),
            [3, 1, 2]
        );
    }

    fn inventory_metadata_with_loaded_programs(
        loaded_writable: CompactPubkey,
        loaded_readonly: CompactPubkey,
    ) -> CompactMetaV1 {
        CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![
                    CompactInnerInstruction {
                        program_id_index: 1,
                        accounts: Vec::new(),
                        data: Vec::new(),
                        stack_height: Some(2),
                    },
                    CompactInnerInstruction {
                        program_id_index: 2,
                        accounts: Vec::new(),
                        data: Vec::new(),
                        stack_height: Some(2),
                    },
                    CompactInnerInstruction {
                        program_id_index: 3,
                        accounts: Vec::new(),
                        data: Vec::new(),
                        stack_height: Some(2),
                    },
                ],
            }]),
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: vec![loaded_writable],
            loaded_readonly_addresses: vec![loaded_readonly],
            return_data: None,
            compute_units_consumed: Some(100),
            cost_units: None,
        }
    }

    fn inventory_metadata_message_summary() -> ProjectedArchiveV2MessageAccountSummary {
        ProjectedArchiveV2MessageAccountSummary {
            is_v0: true,
            num_required_signatures: 1,
            static_account_count: 2,
            instruction_count: 1,
            has_compact_vote_instruction: false,
            minimum_balance_accounts: 0,
            expected_loaded_writable: 1,
            expected_loaded_readonly: 1,
        }
    }

    #[test]
    fn inventory_metadata_resolves_static_and_both_loaded_program_locations() {
        let bytes = wincode::config::serialize(
            &inventory_metadata_with_loaded_programs(CompactPubkey::Id(7), CompactPubkey::Id(8)),
            wincode_leb128_config(),
        )
        .unwrap();
        let mut stage = MetadataProgramStage::new();
        let mut static_ids = [0; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];
        static_ids[0] = 5;
        static_ids[1] = 6;
        parse_inventory_metadata_stage(
            &mut stage,
            &bytes,
            ArchiveV2WireMetadataErrorSchema::Current,
            &inventory_metadata_message_summary(),
            &static_ids,
            ProgramInventoryMetadataContext {
                registry_entries: 10,
                flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
                    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                target_accounts: None,
            },
        )
        .unwrap();

        assert_eq!(
            stage.programs,
            [
                StagedInnerProgram {
                    registry_id_or_account_index: 6,
                    location: InnerProgramLocation::Static,
                    account_start: 0,
                    account_len: 0,
                    target_mint_references: 0,
                    target_token_account_references: 0,
                },
                StagedInnerProgram {
                    registry_id_or_account_index: 7,
                    location: InnerProgramLocation::LoadedWritable,
                    account_start: 0,
                    account_len: 0,
                    target_mint_references: 0,
                    target_token_account_references: 0,
                },
                StagedInnerProgram {
                    registry_id_or_account_index: 8,
                    location: InnerProgramLocation::LoadedReadonly,
                    account_start: 0,
                    account_len: 0,
                    target_mint_references: 0,
                    target_token_account_references: 0,
                },
            ]
        );
    }

    #[test]
    fn inventory_metadata_counts_each_target_account_reference_and_each_target_kind() {
        let mut metadata =
            inventory_metadata_with_loaded_programs(CompactPubkey::Id(7), CompactPubkey::Id(8));
        let instructions = &mut metadata.inner_instructions.as_mut().unwrap()[0].instructions;
        instructions[0].accounts = vec![0, 3, 0];
        instructions[1].accounts = vec![3];
        instructions[2].accounts = vec![1];
        let bytes = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let mut stage = MetadataProgramStage::new();
        let mut static_ids = [0; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];
        static_ids[0] = 5;
        static_ids[1] = 6;
        let mut token_account_by_registry_id = vec![0; 11];
        token_account_by_registry_id[8] = 1;
        let target_accounts = ProgramInventoryTargetAccounts {
            target_mint_id: 5,
            token_account_by_registry_id,
            source: ProgramInventoryTargetAccountSource {
                file: ACCOUNTS_FILE,
                sha256: hex_digest([0; 32]),
                discovered_token_accounts: 1,
                target_addresses: 2,
                membership_definition: "test target set",
            },
        };

        parse_inventory_metadata_stage(
            &mut stage,
            &bytes,
            ArchiveV2WireMetadataErrorSchema::Current,
            &inventory_metadata_message_summary(),
            &static_ids,
            ProgramInventoryMetadataContext {
                registry_entries: 10,
                flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
                    | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                target_accounts: Some(&target_accounts),
            },
        )
        .unwrap();

        assert_eq!(stage.programs[0].target_mint_references, 2);
        assert_eq!(stage.programs[0].target_token_account_references, 1);
        assert_eq!(stage.programs[1].target_mint_references, 0);
        assert_eq!(stage.programs[1].target_token_account_references, 1);
        assert_eq!(stage.programs[2].target_mint_references, 0);
        assert_eq!(stage.programs[2].target_token_account_references, 0);
    }

    #[test]
    fn inventory_metadata_rejects_raw_or_unresolved_loaded_programs() {
        let message = inventory_metadata_message_summary();
        let mut static_ids = [0; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];
        static_ids[0] = 5;
        static_ids[1] = 6;
        let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;

        for invalid_writable in [CompactPubkey::Raw(key(7)), CompactPubkey::Id(0)] {
            let bytes = wincode::config::serialize(
                &inventory_metadata_with_loaded_programs(invalid_writable, CompactPubkey::Id(8)),
                wincode_leb128_config(),
            )
            .unwrap();
            let mut stage = MetadataProgramStage::new();
            assert!(
                parse_inventory_metadata_stage(
                    &mut stage,
                    &bytes,
                    ArchiveV2WireMetadataErrorSchema::Current,
                    &message,
                    &static_ids,
                    ProgramInventoryMetadataContext {
                        registry_entries: 10,
                        flags,
                        target_accounts: None,
                    },
                )
                .is_err()
            );
        }
    }

    fn write_completed_program_inventory_fixture(
        dump: &Path,
    ) -> ([u8; 32], [u8; 32], [u8; 32], [u8; 32]) {
        fs::create_dir(dump).unwrap();

        let mint = key(9);
        let mint_signature = [7; SIGNATURE_BYTES];
        let source_generation_digest = [6; 32];
        let registry = [key(1), key(2), key(3), key(4), key(5), key(6), key(9)].concat();
        let registry_sha256 = sha256_bytes(&registry);
        fs::write(dump.join(PUBKEY_REGISTRY_FILE), &registry).unwrap();

        let signature_sha256 = sha256_bytes(&mint_signature);
        fs::write(dump.join(DUMP_SIGNATURES_FILE), mint_signature).unwrap();

        let accounts = DiscoveredAccountList {
            schema_version: DUMP_SCHEMA_VERSION,
            mint,
            anchor_position: crate::format::SourceTransactionCoordinate {
                epoch: 1,
                slot: 1_001,
                source_block_id: 1,
                tx_index: 0,
                source_first_signature_ordinal: 0,
                signature_count: 1,
            },
            accounts: vec![crate::format::DiscoveredAccount {
                raw_pubkey: key(6),
                first_creation: crate::format::SourceInstructionCoordinate {
                    epoch: 1,
                    slot: 1_001,
                    source_block_id: 1,
                    tx_index: 0,
                    instruction_index: 0,
                },
            }],
        };
        let accounts = wincode::config::serialize(&accounts, wincode_leb128_config()).unwrap();
        let accounts_sha256 = sha256_bytes(&accounts);
        fs::write(dump.join(ACCOUNTS_FILE), accounts).unwrap();

        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: Vec::new(),
                    data: ArchiveV2HotInstructionData::Raw(vec![10]),
                },
                ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: Vec::new(),
                    data: ArchiveV2HotInstructionData::Raw(vec![11]),
                },
            ],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(5),
                writable_indexes: vec![0, 1],
                readonly_indexes: vec![2, 3],
            }],
        });
        let message = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
        let mut metadata =
            inventory_metadata_with_loaded_programs(CompactPubkey::Id(3), CompactPubkey::Id(4));
        metadata.loaded_writable_addresses = vec![CompactPubkey::Id(3), CompactPubkey::Id(6)];
        metadata.loaded_readonly_addresses = vec![CompactPubkey::Id(4), CompactPubkey::Id(7)];
        let instructions = &mut metadata.inner_instructions.as_mut().unwrap()[0].instructions;
        instructions[0].accounts = vec![5, 3, 5];
        instructions[1].accounts = vec![3];
        instructions[2].program_id_index = 4;
        instructions[2].accounts = vec![0];
        let mut strings = StringTable::default();
        let outer_unknown = strings.push("owner 11111111111111111111111111111111 amount 12345");
        let anchor_name = strings.push("initialize_pool");
        let inner_unknown = strings.push("vault 22222222222222222222222222222222 nonce 67890");
        let plain = strings.push("sequence 123456");
        metadata.logs = Some(CompactLogStream {
            events: vec![
                LogEvent::Invoke {
                    program: CompactPubkey::Id(2),
                    depth: 1,
                },
                LogEvent::ProgramLog(ProgramLog::Unknown(outer_unknown)),
                LogEvent::ProgramLog(ProgramLog::AnchorInstruction { name: anchor_name }),
                LogEvent::Invoke {
                    program: CompactPubkey::Id(3),
                    depth: 2,
                },
                LogEvent::ProgramIdLog {
                    program: CompactPubkey::Id(3),
                    log: ProgramLog::Unknown(inner_unknown),
                },
                LogEvent::BpfConsumed {
                    used: 10,
                    limit: 20,
                },
                LogEvent::Success {
                    program: CompactPubkey::Id(3),
                },
                LogEvent::Plain { text: plain },
                LogEvent::Success {
                    program: CompactPubkey::Id(2),
                },
            ],
            strings,
            data: DataTable::default(),
        });
        let metadata = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let flags = ARCHIVE_V2_TX_FLAG_MESSAGE_V0
            | ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
            | ARCHIVE_V2_TX_FLAG_HAS_LOGS;

        let mut transaction_writer = DigestWriter::create(&dump.join(TRANSACTIONS_FILE)).unwrap();
        let mut frame_scratch = Vec::new();
        write_frame(
            &mut transaction_writer,
            &BorrowedDumpRecord::Header(TokenTransactionDumpHeader {
                schema_version: DUMP_SCHEMA_VERSION,
                stream_kind: DumpStreamKind::Consolidated,
                mint,
                mint_slot: 1_001,
                mint_signature,
                source_epoch: None,
                source_generation_digest: None,
                source_wire_profile: None,
                pubkey_registry_id_base: PUBKEY_REGISTRY_ID_BASE,
            }),
            &mut frame_scratch,
        )
        .unwrap();
        write_frame(
            &mut transaction_writer,
            &BorrowedDumpRecord::Transaction(BorrowedTransactionRecord {
                source_epoch: 1,
                source_generation_digest,
                source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
                source_block_id: 1,
                block: TokenTransactionBlockContext {
                    slot: 1_001,
                    parent_slot: 1_000,
                    blockhash_id: 1,
                    previous_blockhash_id: 2,
                    block_time: Some(1_700_000_000),
                    block_height: Some(1_000),
                    transaction_count: 1,
                },
                tx_index: 0,
                flags,
                source_first_signature_ordinal: 0,
                signature_count: 1,
                dump_signature_ordinal: Some(0),
                message_bytes: &message,
                metadata_bytes: &metadata,
            }),
            &mut frame_scratch,
        )
        .unwrap();
        write_frame(
            &mut transaction_writer,
            &BorrowedDumpRecord::Footer(TokenTransactionDumpFooter {
                epochs: 1,
                blocks_scanned: 1,
                transactions_scanned: 1,
                transactions_written: 1,
                pubkeys: 7,
                signatures: 1,
                owned_block_fallbacks: 0,
                raw_transaction_fallbacks: 0,
                raw_metadata_fallbacks: 0,
            }),
            &mut frame_scratch,
        )
        .unwrap();
        let transaction_binding = transaction_writer.finish().unwrap();
        let transaction_sha256 = transaction_binding.sha256;
        drop(transaction_binding);

        let manifest = DumpManifest {
            schema_version: DUMP_SCHEMA_VERSION,
            artifact_kind: DumpArtifactKind::Consolidated,
            complete: true,
            mint: bs58::encode(mint).into_string(),
            mint_slot: 1_001,
            mint_signature: bs58::encode(mint_signature).into_string(),
            workers: 1,
            source_binding: DumpSourceBinding::TrustedLocalSizesOnly {
                cluster_id: "testnet-fixture".to_owned(),
                slots_per_epoch: 1_000,
                wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            },
            first_epoch: 1,
            last_epoch: 1,
            transactions: 1,
            signatures: Some(1),
            pubkeys: Some(7),
            transaction_stream: TRANSACTIONS_FILE.to_owned(),
            transaction_stream_sha256: Some(hex_digest(transaction_sha256)),
            account_id_log: None,
            account_id_log_sha256: None,
            discovered_accounts: Some(ACCOUNTS_FILE.to_owned()),
            discovered_accounts_sha256: Some(hex_digest(accounts_sha256)),
            discovered_account_count: Some(1),
            signature_stream: Some(DUMP_SIGNATURES_FILE.to_owned()),
            signature_stream_sha256: Some(hex_digest(signature_sha256)),
            pubkey_registry: Some(PUBKEY_REGISTRY_FILE.to_owned()),
            pubkey_registry_sha256: Some(hex_digest(registry_sha256)),
            registry_maps: None,
        };
        let manifest = serde_json::to_vec_pretty(&manifest).unwrap();
        let manifest_sha256 = sha256_bytes(&manifest);
        fs::write(dump.join(DUMP_MANIFEST_FILE), manifest).unwrap();
        (
            manifest_sha256,
            transaction_sha256,
            registry_sha256,
            accounts_sha256,
        )
    }

    #[test]
    fn completed_program_inventory_reports_all_outer_and_inner_program_locations() {
        let temporary = tempdir().unwrap();
        let dump = temporary.path().join("completed-dump");
        let report = temporary.path().join("program-inventory.json");
        let (manifest_sha256, transaction_sha256, registry_sha256, accounts_sha256) =
            write_completed_program_inventory_fixture(&dump);

        inventory_consolidated_programs_v3(&dump, &report).unwrap();
        let inventory: serde_json::Value =
            serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();

        assert_eq!(inventory["schema_version"], 2);
        assert_eq!(inventory["artifact_kind"], "program_inventory");
        assert_eq!(inventory["complete"], true);
        assert_eq!(inventory["instruction_program_resolution_complete"], true);
        assert_eq!(
            inventory["source"]["manifest_sha256"],
            hex_digest(manifest_sha256)
        );
        assert_eq!(
            inventory["source"]["transaction_stream_sha256"],
            hex_digest(transaction_sha256)
        );
        assert_eq!(
            inventory["source"]["pubkey_registry_sha256"],
            hex_digest(registry_sha256)
        );
        assert_eq!(inventory["source"]["transactions"], 1);
        assert_eq!(inventory["source"]["signatures"], 1);
        assert_eq!(inventory["source"]["registry_entries"], 7);
        assert_eq!(
            inventory["source"]["target_accounts"]["file"],
            ACCOUNTS_FILE
        );
        assert_eq!(
            inventory["source"]["target_accounts"]["sha256"],
            hex_digest(accounts_sha256)
        );
        assert_eq!(
            inventory["source"]["target_accounts"]["discovered_token_accounts"],
            1
        );
        assert_eq!(
            inventory["source"]["target_accounts"]["target_addresses"],
            2
        );

        let counters = &inventory["counters"];
        assert_eq!(counters["transactions"], 1);
        assert_eq!(counters["distinct_programs"], 3);
        assert_eq!(counters["transactions_with_outer_instructions"], 1);
        assert_eq!(counters["transactions_with_inner_instructions"], 1);
        assert_eq!(
            counters["transactions_with_target_account_inner_instructions"],
            1
        );
        assert_eq!(counters["outer_occurrences"], 2);
        assert_eq!(counters["inner_occurrences"], 3);
        assert_eq!(counters["target_account_inner_occurrences"], 2);
        assert_eq!(counters["target_account_inner_transactions"], 2);
        assert_eq!(counters["target_mint_inner_occurrences"], 1);
        assert_eq!(counters["target_token_account_inner_occurrences"], 2);
        assert_eq!(counters["target_account_inner_references"], 4);
        assert_eq!(counters["target_mint_inner_references"], 2);
        assert_eq!(counters["target_token_account_inner_references"], 2);
        assert_eq!(counters["outer_static_resolutions"], 2);
        assert_eq!(counters["inner_static_resolutions"], 1);
        assert_eq!(counters["inner_loaded_writable_resolutions"], 1);
        assert_eq!(counters["inner_loaded_readonly_resolutions"], 1);
        assert_eq!(counters["unresolved_program_references"], 0);

        let programs = inventory["programs"].as_array().unwrap();
        assert_eq!(programs.len(), 3);
        assert_eq!(
            programs
                .iter()
                .map(|program| program["registry_id"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [2, 3, 4]
        );
        assert_eq!(
            programs[0]["program_id"],
            bs58::encode(key(2)).into_string()
        );
        assert_eq!(programs[0]["total_occurrences"], 3);
        assert_eq!(programs[0]["outer_occurrences"], 2);
        assert_eq!(programs[0]["inner_occurrences"], 1);
        assert_eq!(programs[0]["transactions"], 1);
        assert_eq!(programs[0]["target_account_inner_occurrences"], 1);
        assert_eq!(programs[0]["target_account_inner_transactions"], 1);
        assert_eq!(programs[0]["target_mint_inner_occurrences"], 1);
        assert_eq!(programs[0]["target_token_account_inner_occurrences"], 1);
        assert_eq!(programs[0]["target_account_inner_references"], 3);
        assert_eq!(programs[0]["target_mint_inner_references"], 2);
        assert_eq!(programs[0]["target_token_account_inner_references"], 1);
        assert_eq!(
            programs[1]["program_id"],
            bs58::encode(key(3)).into_string()
        );
        assert_eq!(programs[1]["target_account_inner_occurrences"], 1);
        assert_eq!(programs[1]["target_account_inner_transactions"], 1);
        assert_eq!(programs[1]["target_mint_inner_occurrences"], 0);
        assert_eq!(programs[1]["target_token_account_inner_occurrences"], 1);
        assert_eq!(programs[1]["target_account_inner_references"], 1);
        assert_eq!(programs[1]["target_mint_inner_references"], 0);
        assert_eq!(programs[1]["target_token_account_inner_references"], 1);
        assert_eq!(
            programs[2]["program_id"],
            bs58::encode(key(4)).into_string()
        );
        assert_eq!(programs[2]["target_account_inner_occurrences"], 0);
        assert_eq!(programs[2]["target_account_inner_transactions"], 0);
        assert_eq!(programs[2]["target_account_inner_references"], 0);

        let error = inventory_consolidated_programs_v3(&dump, &report).unwrap_err();
        assert!(error.to_string().contains("refusing to replace"));
        assert!(report.is_file());
    }

    #[test]
    fn completed_program_log_inventory_attributes_and_normalizes_selected_logs() {
        let temporary = tempdir().unwrap();
        let dump = temporary.path().join("completed-dump");
        let selected = temporary.path().join("selected-programs.txt");
        let report = temporary.path().join("program-logs.json");
        let (manifest_sha256, transaction_sha256, registry_sha256, _) =
            write_completed_program_inventory_fixture(&dump);
        let selected_bytes = format!(
            "# selected programs\n{}\n{}\n",
            bs58::encode(key(2)).into_string(),
            bs58::encode(key(3)).into_string(),
        )
        .into_bytes();
        fs::write(&selected, &selected_bytes).unwrap();

        inventory_consolidated_program_logs_v3(&dump, &selected, &report).unwrap();
        let inventory: serde_json::Value =
            serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();

        assert_eq!(inventory["schema_version"], 1);
        assert_eq!(inventory["artifact_kind"], "program_log_inventory");
        assert_eq!(inventory["complete"], true);
        assert_eq!(inventory["explicit_id_evidence_complete"], true);
        assert_eq!(inventory["unkeyed_attribution_complete"], true);
        assert_eq!(
            inventory["source"]["manifest_sha256"],
            hex_digest(manifest_sha256)
        );
        assert_eq!(
            inventory["source"]["transaction_stream_sha256"],
            hex_digest(transaction_sha256)
        );
        assert_eq!(
            inventory["source"]["pubkey_registry_sha256"],
            hex_digest(registry_sha256)
        );
        assert_eq!(
            inventory["selected_set"]["input_sha256"],
            hex_digest(sha256_bytes(&selected_bytes))
        );
        assert_eq!(inventory["selected_set"]["requested_programs"], 2);
        assert_eq!(inventory["selected_set"]["programs_present_in_registry"], 2);

        let counters = &inventory["counters"];
        assert_eq!(counters["transactions"], 1);
        assert_eq!(counters["transactions_with_logs"], 1);
        assert_eq!(counters["compact_log_events"], 9);
        assert_eq!(counters["explicit_program_evidence_events"], 5);
        assert_eq!(counters["selected_explicit_program_evidence_events"], 5);
        assert_eq!(counters["unkeyed_evidence_events"], 4);
        assert_eq!(counters["selected_attributed_unkeyed_events"], 4);
        assert_eq!(counters["unattributed_unkeyed_events"], 0);
        assert_eq!(counters["selected_caller_edge_observations"], 1);
        assert_eq!(counters["selected_callee_edge_observations"], 1);
        assert_eq!(counters["holdout_diagnostics"], 0);

        let programs = inventory["programs"].as_array().unwrap();
        assert_eq!(programs.len(), 2);
        assert_eq!(programs[0]["registry_id"], 2);
        assert_eq!(programs[0]["explicit_id_evidence"]["events"], 2);
        assert_eq!(programs[0]["attributed_unkeyed_evidence"]["events"], 3);
        let outer_patterns = programs[0]["text_patterns"].as_array().unwrap();
        assert!(outer_patterns.iter().any(|pattern| {
            pattern["trust_lane"] == "clean_stack_program_log"
                && pattern["text_kind"] == "unknown"
                && pattern["pattern"] == "owner <pubkey> amount <num>"
        }));
        assert!(outer_patterns.iter().any(|pattern| {
            pattern["text_kind"] == "anchor_instruction" && pattern["pattern"] == "initialize_pool"
        }));
        assert!(outer_patterns.iter().any(|pattern| {
            pattern["trust_lane"] == "clean_stack_low_trust_context"
                && pattern["pattern"] == "sequence <num>"
        }));
        assert_eq!(programs[0]["callers"].as_array().unwrap().len(), 0);
        assert_eq!(programs[0]["callees"][0]["registry_id"], 3);
        assert_eq!(programs[0]["callees"][0]["invokes"], 1);

        assert_eq!(programs[1]["registry_id"], 3);
        assert_eq!(programs[1]["explicit_id_evidence"]["events"], 3);
        assert_eq!(programs[1]["attributed_unkeyed_evidence"]["events"], 1);
        let inner_patterns = programs[1]["text_patterns"].as_array().unwrap();
        assert!(inner_patterns.iter().any(|pattern| {
            pattern["trust_lane"] == "explicit_program_id_log"
                && pattern["text_kind"] == "unknown"
                && pattern["pattern"] == "vault <pubkey> nonce <num>"
        }));
        assert_eq!(programs[1]["callers"][0]["registry_id"], 2);
        assert_eq!(programs[1]["callers"][0]["invokes"], 1);
        assert_eq!(programs[1]["callees"].as_array().unwrap().len(), 0);
        assert_eq!(inventory["holdouts"].as_array().unwrap().len(), 0);

        let error = inventory_consolidated_program_logs_v3(&dump, &selected, &report).unwrap_err();
        assert!(error.to_string().contains("refusing to replace"));
        assert!(report.is_file());
    }

    #[test]
    fn completed_program_coverage_counts_transaction_union_once() {
        let temporary = tempdir().unwrap();
        let dump = temporary.path().join("completed-dump");
        let identified = temporary.path().join("identified-programs.txt");
        let report = temporary.path().join("program-coverage.json");
        write_completed_program_inventory_fixture(&dump);

        let identified_bytes = format!(
            "# two identified programs\n{}\n\n{}\n",
            bs58::encode(key(2)).into_string(),
            bs58::encode(key(3)).into_string(),
        )
        .into_bytes();
        fs::write(&identified, &identified_bytes).unwrap();
        measure_identified_program_coverage_v3(&dump, &identified, &report).unwrap();
        let coverage: serde_json::Value =
            serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();

        assert_eq!(coverage["schema_version"], 1);
        assert_eq!(coverage["artifact_kind"], "program_identification_coverage");
        assert_eq!(coverage["complete"], true);
        assert_eq!(
            coverage["generator"]["crate_name"],
            "blockzilla-token-transaction-dump"
        );
        assert_eq!(
            coverage["generator"]["executable_sha256"]
                .as_str()
                .unwrap()
                .len(),
            64
        );
        assert_eq!(coverage["source"]["transactions"], 1);
        assert_eq!(coverage["distinct_programs"], 3);
        assert_eq!(
            coverage["identified_set"]["input_sha256"],
            hex_digest(sha256_bytes(&identified_bytes))
        );
        assert_eq!(coverage["identified_set"]["requested_programs"], 2);
        assert_eq!(
            coverage["identified_set"]["programs_present_in_registry"],
            2
        );
        assert_eq!(
            coverage["identified_set"]["programs_used_by_instructions"],
            2
        );

        let counters = &coverage["counters"];
        assert_eq!(counters["transactions"], 1);
        assert_eq!(counters["fully_covered_transactions"], 0);
        assert_eq!(counters["partially_covered_transactions"], 1);
        assert_eq!(counters["touched_transactions"], 1);
        assert_eq!(counters["uncovered_transactions"], 0);
        assert_eq!(counters["transactions_without_instructions"], 0);
        assert_eq!(counters["outer_occurrences"], 2);
        assert_eq!(counters["identified_outer_occurrences"], 2);
        assert_eq!(counters["unidentified_outer_occurrences"], 0);
        assert_eq!(counters["inner_occurrences"], 3);
        assert_eq!(counters["identified_inner_occurrences"], 2);
        assert_eq!(counters["unidentified_inner_occurrences"], 1);

        let unknown = coverage["unknown_programs"].as_array().unwrap();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown[0]["registry_id"], 4);
        assert_eq!(unknown[0]["transactions"], 1);
        assert_eq!(unknown[0]["total_occurrences"], 1);

        let error =
            measure_identified_program_coverage_v3(&dump, &identified, &report).unwrap_err();
        assert!(error.to_string().contains("refusing to replace"));
        assert!(report.is_file());
    }

    #[test]
    fn completed_dex_parser_coverage_scans_outer_and_inner_once() {
        let temporary = tempdir().unwrap();
        let dump = temporary.path().join("completed-dump");
        let report = temporary.path().join("dex-parser-coverage.json");
        let (manifest_sha256, transaction_sha256, registry_sha256, _) =
            write_completed_program_inventory_fixture(&dump);

        measure_dex_parser_coverage_v3(&dump, &report).unwrap();
        let coverage: serde_json::Value =
            serde_json::from_slice(&fs::read(&report).unwrap()).unwrap();

        assert_eq!(coverage["schema_version"], 1);
        assert_eq!(coverage["artifact_kind"], "dex_parser_coverage");
        assert_eq!(coverage["complete"], true);
        assert_eq!(
            coverage["source"]["manifest_sha256"],
            hex_digest(manifest_sha256)
        );
        assert_eq!(
            coverage["source"]["transaction_stream_sha256"],
            hex_digest(transaction_sha256)
        );
        assert_eq!(
            coverage["source"]["pubkey_registry_sha256"],
            hex_digest(registry_sha256)
        );
        assert_eq!(coverage["transactions"]["scanned"], 1);
        assert_eq!(coverage["transactions"]["successful"], 1);
        assert_eq!(coverage["transactions"]["failed"], 0);
        assert_eq!(coverage["transactions"]["unknown_status"], 0);
        assert_eq!(coverage["transactions"]["candidate"], 0);
        assert_eq!(coverage["instructions"]["occurrences"]["all"], 5);
        assert_eq!(coverage["instructions"]["occurrences"]["outer"], 2);
        assert_eq!(coverage["instructions"]["occurrences"]["inner"], 3);
        assert_eq!(
            coverage["instructions"]["supported_address_hits"]["supported_address_hits"],
            0
        );
        assert_eq!(
            coverage["programs"].as_array().unwrap().len(),
            blockzilla_dex_parser::PROGRAM_SPECS.len()
        );
        assert_eq!(
            coverage["parser_set"]["semantic_version"],
            blockzilla_dex_parser::PARSER_SEMANTIC_VERSION
        );
        assert_eq!(
            coverage["parser_set"]["implementation_fingerprint"],
            blockzilla_dex_parser::PARSER_IMPLEMENTATION_FINGERPRINT
        );
        assert!(
            coverage["definitions"]["all_instruction_denominator"]
                .as_str()
                .unwrap()
                .contains("stored top-level")
        );

        let error = measure_dex_parser_coverage_v3(&dump, &report).unwrap_err();
        assert!(error.to_string().contains("refusing to replace"));
        assert!(report.is_file());
    }

    #[test]
    fn digest_writer_resume_discards_only_uncommitted_suffix() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("resumable.bin");
        let mut writer = DigestWriter::create(&path).unwrap();
        writer.write_all(b"committed").unwrap();
        let committed = writer.checkpoint().unwrap();
        writer.write_all(b"incomplete").unwrap();
        drop(writer);

        let mut resumed = DigestWriter::resume(&path, committed).unwrap();
        resumed.write_all(b"-continued").unwrap();
        let binding = resumed.finish().unwrap();
        let expected = b"committed-continued";
        assert_eq!(fs::read(&path).unwrap(), expected);
        assert_eq!(binding.bytes, expected.len() as u64);
        assert_eq!(binding.sha256, sha256_bytes(expected));
    }
}
