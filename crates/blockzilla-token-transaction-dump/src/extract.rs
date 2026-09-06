use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufReader, BufWriter, Read},
    ops::Range,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Mutex,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::FileExt as _;
#[cfg(windows)]
use std::os::windows::fs::FileExt as _;

use anyhow::{Context, Result, anyhow, ensure};
use blockzilla_archive_v2::{ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES, ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX, ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_LOGS, ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA, ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES, ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE};
use blockzilla_primitives::{CompactPubkey, WincodeLeb128FramedWriter, bounded_wincode_leb128_config, encode_with_scratch};
use blockzilla_registry::FileBackedKeyIndex;
use blockzilla_read_sdk::{
    ArchiveReader, ArchiveV2LoadedAddressSide, ArchiveV2MessageProjector,
    ArchiveV2MetadataProfileAdmission, ArchiveV2MetadataProjectionLimits,
    ArchiveV2MetadataWireProfile, ArchiveV2WireProfile, BatchBarrierBlockStats,
    BorrowedArchiveV2InnerTokenInstruction, BorrowedArchiveV2Instruction, BorrowedDecodedBlock,
    CURRENT_TYPED_ERRORS_MARKER_BYTES, CURRENT_TYPED_ERRORS_MARKER_FILE, Error as ReadError,
    HashVerification, LogPayloadValidation, MAX_MESSAGE_ACCOUNTS,
    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES, OpenOptions, OrderedParallelBlockConfig,
    OrderedParallelBlockStats, PinnedLocalEntryKind, PinnedLocalRangeSource,
    ProjectedArchiveV2MessageAccountSummary, ProjectedArchiveV2TokenMetadataSummary, RangeSource,
    SignatureReference,
    manifest::{
        GENERATION_MANIFEST_FILE, GenerationManifest, SIGNATURES_FILE, TrustedGenerationIdentity,
    },
    visit_archive_v2_token_metadata_exact_ordered, wire_profile_marker, wire_profile_marker_bytes,
};
use sha2::{Digest, Sha256};
use solana_pubkey::Pubkey;
use wincode::SchemaWrite;

use crate::{
    consolidate::{
        ResumeTargetBinding, read_epoch_account_id_log, validate_epoch_shard_for_resume,
    },
    format::{
        ACCOUNT_ID_LOG_FILE, ACCOUNTS_FILE, AccountIdRole, CREATIONS_FILE, DISCOVERY_SHARDS_DIR,
        DUMP_MANIFEST_FILE, DUMP_SCHEMA_VERSION, DiscoveredAccount, DiscoveredAccountList,
        DumpArtifactKind, DumpManifest, DumpSourceBinding, DumpStreamKind, DumpWireProfile,
        EPOCH_SHARDS_DIR, EpochAccountIdEntry, EpochAccountIdLog, EpochCreationEntry,
        EpochCreationLog, PUBKEY_REGISTRY_ID_BASE, SourceInstructionCoordinate,
        SourceTransactionCoordinate, TRANSACTIONS_FILE, TokenTransactionBlockContext,
        TokenTransactionDumpFooter, TokenTransactionDumpHeader, TokenTransactionDumpRecord,
    },
    pipeline::{ExtractConfig, ExtractSourceMode, ProbeConfig, ProbeReaderStats, ProbeReport},
    progress::{ExtractionProgress, PassMetrics},
    resume::{
        ResumeCheckpointPayload, ResumeDiscoveryBinding, ResumeExtractionMode,
        ResumeFrozenAccountBinding, ResumeIdentity, ResumeShardBinding, ResumeStage,
        commit_partial_artifact_file, commit_partial_shard, create_partial_artifact_file,
        create_partial_shard_directory, discover_resume_shard_layout,
        load_pending_resume_checkpoint, load_resume_checkpoint, pending_checkpoint_staging_path,
        promote_pending_resume_checkpoint, quarantine_complete_shard,
        quarantine_partial_artifact_file, quarantine_partial_shard,
        quarantine_pending_checkpoint_staging, quarantine_pending_resume_checkpoint,
        stage_resume_checkpoint, write_resume_checkpoint_atomic,
    },
};

#[cfg(test)]
use crate::format::TokenTransactionRecord;

const MAX_WORKERS: usize = 64;
const SINGLE_READ_MATCH_HINT_BUDGET_BYTES: usize = 64 << 20;
const MAX_TRUSTED_LOCAL_MANIFEST_BYTES: usize = 4 << 20;
const METADATA_SCHEMA_MARKER_PREFIX: &[u8] = b"archive-v2-metadata-schema-";
const MIB: f64 = (1024 * 1024) as f64;
const SPL_TOKEN_PROGRAM_ID: [u8; 32] =
    solana_pubkey::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").to_bytes();
const SPL_TOKEN_2022_PROGRAM_ID: [u8; 32] =
    solana_pubkey::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").to_bytes();

type DiscoveredAccountMap = BTreeMap<[u8; 32], SourceInstructionCoordinate>;

#[derive(Debug, Clone)]
struct EpochInput {
    epoch: u64,
    path: PathBuf,
    manifest: GenerationManifest,
    trusted_metadata_admission: Option<TrustedLocalMetadataAdmission>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrustedLocalMetadataAdmission {
    UnmarkedHistoricalCompatibility,
    PublishedCurrentTypedErrors,
}

#[derive(Debug, Default, Clone, Copy)]
struct EpochScanStats {
    blocks: u64,
    transactions: u64,
    owned_block_fallbacks: u64,
    compressed_bytes: u64,
}

#[cfg(any())]
#[derive(Debug)]
struct EpochSelection {
    input: EpochInput,
    selected: BTreeMap<usize, BTreeSet<u32>>,
    account_ids: EpochAccountIdLog,
    stats: EpochScanStats,
}

#[cfg(any())]
#[derive(Debug)]
struct ProjectedTransactionFacts {
    tx_index: u32,
    signatures: SignatureReference,
    facts: CompactTransactionFacts,
}

#[cfg(any())]
#[derive(Debug)]
struct ProjectedBlockFacts {
    slot: u64,
    source_block_id: u32,
    owned_fallback: bool,
    transactions: Vec<ProjectedTransactionFacts>,
}

#[derive(Debug, Clone)]
struct ShardSummary {
    transactions: u64,
    compressed_bytes: u64,
    anchor_transactions: u64,
}

#[cfg(any())]
#[derive(Debug, Default)]
struct RootSummary {
    transactions: u64,
    anchor_transactions: u64,
}

#[cfg(any())]
#[derive(Debug)]
struct IndexedInstruction {
    outer_instruction_index: usize,
    program_id_index: usize,
    accounts: Vec<u8>,
    data: Vec<u8>,
}

#[cfg(any())]
#[derive(Debug, Default)]
struct MetadataFacts {
    has_error: bool,
    loaded_writable: Vec<CompactPubkey>,
    loaded_readonly: Vec<CompactPubkey>,
    inner: Vec<IndexedInstruction>,
}

#[cfg(any())]
#[derive(Debug, Clone)]
struct CompactInstructionFact {
    program_id: CompactPubkey,
    accounts: Vec<CompactPubkey>,
    data: Vec<u8>,
}

#[cfg(any())]
#[derive(Debug, Clone)]
struct CompactTransactionFacts {
    has_error: bool,
    instructions: Vec<CompactInstructionFact>,
}

#[derive(Debug, Clone, Copy)]
struct CreationCandidate {
    source_reference: CompactPubkey,
    coordinate: SourceInstructionCoordinate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingCreationCandidate {
    coordinate: SourceInstructionCoordinate,
    ledger_sequence: u64,
}

#[derive(Debug, Clone, Copy)]
struct ResolvedCreationCandidate {
    source_reference: CompactPubkey,
    raw_pubkey: [u8; 32],
    local_id: Option<u32>,
    coordinate: SourceInstructionCoordinate,
    ledger_sequence: u64,
}

#[derive(Debug, Clone, Copy)]
struct ResolvedDiscoveredAccount {
    raw_pubkey: [u8; 32],
    first_creation: SourceInstructionCoordinate,
    local_id: Option<u32>,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SingleReadExtractorStats {
    pub(crate) creation_candidates: u64,
    pub(crate) unique_candidate_ids: u64,
    pub(crate) unique_candidate_raw_refs: u64,
    pub(crate) new_accounts: u64,
    pub(crate) registry: RegistryResolutionStats,
    pub(crate) registry_resolution_time: Duration,
    pub(crate) target_build_time: Duration,
    pub(crate) target_finalize_time: Duration,
    pub(crate) clean_hint_batches: u64,
    pub(crate) dirty_hint_batches: u64,
    pub(crate) hint_direct_matches: u64,
    pub(crate) hint_skips_without_decode: u64,
    pub(crate) hint_exact_reparses: u64,
    pub(crate) metadata_owned_fallbacks: u64,
}

#[derive(Debug, Clone, Copy)]
struct InitInstructionIndices {
    program: usize,
    account: usize,
    mint: usize,
}

#[derive(Debug, Clone, Copy)]
struct DeferredInitCandidate {
    indices: InitInstructionIndices,
    outer_instruction_index: usize,
    inner_instruction_index: Option<usize>,
}

#[derive(Debug)]
struct DiscoveryScratch {
    static_accounts: [CompactPubkey; MAX_MESSAGE_ACCOUNTS],
    static_account_count: usize,
    candidates: Vec<DeferredInitCandidate>,
    inner_counts: Vec<u32>,
    loaded_accounts: [CompactPubkey; MAX_MESSAGE_ACCOUNTS],
    loaded_generation: [u32; MAX_MESSAGE_ACCOUNTS],
    generation: u32,
    canonical_metadata: Vec<u8>,
    metadata_owned_fallbacks: u64,
}

impl DiscoveryScratch {
    fn new() -> Self {
        Self {
            static_accounts: [CompactPubkey::Id(1); MAX_MESSAGE_ACCOUNTS],
            static_account_count: 0,
            candidates: Vec::with_capacity(MAX_MESSAGE_ACCOUNTS),
            inner_counts: Vec::with_capacity(MAX_MESSAGE_ACCOUNTS),
            loaded_accounts: [CompactPubkey::Id(1); MAX_MESSAGE_ACCOUNTS],
            loaded_generation: [0; MAX_MESSAGE_ACCOUNTS],
            generation: 0,
            canonical_metadata: Vec::new(),
            metadata_owned_fallbacks: 0,
        }
    }

    fn begin_transaction(&mut self) {
        self.static_account_count = 0;
        self.candidates.clear();
        self.advance_loaded_generation();
    }

    fn prepare_inner_counts(&mut self, instruction_count: usize) {
        if self.inner_counts.len() < instruction_count {
            self.inner_counts.resize(instruction_count, 0);
        }
        for count in &mut self.inner_counts[..instruction_count] {
            *count = 0;
        }
    }

    fn advance_loaded_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.loaded_generation.fill(0);
            self.generation = 1;
        }
    }

    fn resolve_account(&self, index: usize) -> Option<CompactPubkey> {
        if index < self.static_account_count {
            return Some(self.static_accounts[index]);
        }
        (index < MAX_MESSAGE_ACCOUNTS && self.loaded_generation[index] == self.generation)
            .then(|| self.loaded_accounts[index])
    }
}

#[derive(Debug, Clone, Copy)]
struct DiscoveryMatcher {
    mint: [u8; 32],
    mint_id: Option<u32>,
    token_program_ids: [Option<u32>; 2],
}

#[derive(Debug)]
struct MatchedBlock {
    slot: u64,
    transactions_scanned: u64,
    owned_fallback: bool,
    transactions_written: u64,
    anchor_transactions: u64,
    hint_direct_matches: u64,
    hint_skips_without_decode: u64,
    hint_exact_reparses: u64,
    metadata_owned_fallbacks: u64,
}

#[derive(Debug)]
struct MatchScratch {
    encoded_record: Vec<u8>,
    canonical_metadata: Vec<u8>,
    metadata_owned_fallbacks: u64,
}

impl MatchScratch {
    fn new() -> Self {
        Self {
            encoded_record: Vec::with_capacity(2 << 20),
            canonical_metadata: Vec::new(),
            metadata_owned_fallbacks: 0,
        }
    }
}

struct SharedRawWriter {
    framed: WincodeLeb128FramedWriter<BufWriter<File>>,
    first_error: Option<String>,
}

impl SharedRawWriter {
    fn write_encoded(&mut self, bytes: &[u8]) -> Result<()> {
        if let Some(error) = &self.first_error {
            return Err(anyhow!("raw transaction writer already failed: {error}"));
        }
        if let Err(error) = self.framed.write_bytes(bytes) {
            let message = error.to_string();
            self.first_error = Some(message.clone());
            return Err(anyhow!(message));
        }
        Ok(())
    }
}

#[derive(SchemaWrite)]
enum BorrowedRawDumpRecord<'a> {
    #[wincode(tag = 1)]
    Transaction(BorrowedRawTransactionRecord<'a>),
}

#[derive(SchemaWrite)]
struct BorrowedRawTransactionRecord<'a> {
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

#[derive(Debug)]
struct DiscoveryBlock {
    slot: u64,
    source_block_id: u32,
    transactions_scanned: u64,
    owned_fallback: bool,
    first_signatures: Vec<(u32, SignatureReference)>,
    creations: Vec<CreationCandidate>,
    metadata_owned_fallbacks: u64,
}

#[derive(Debug)]
struct EpochDiscoveryResult {
    log: EpochCreationLog,
    anchor_position: Option<SourceTransactionCoordinate>,
    stats: EpochScanStats,
}

#[derive(Debug)]
struct SingleReadWorkerScratch {
    discovery: DiscoveryScratch,
    matching: MatchScratch,
}

impl SingleReadWorkerScratch {
    fn new() -> Self {
        Self {
            discovery: DiscoveryScratch::new(),
            matching: MatchScratch::new(),
        }
    }
}

struct SingleReadEpochCoordinator<'a> {
    epoch: u64,
    mint: [u8; 32],
    mint_id: Option<u32>,
    global_accounts: &'a mut DiscoveredAccountMap,
    resolved_accounts: Vec<ResolvedDiscoveredAccount>,
    resolved_account_merge_scratch: Vec<ResolvedDiscoveredAccount>,
    known_id_mappings: Vec<(u32, [u8; 32])>,
    new_id_mappings: Vec<(u32, [u8; 32])>,
    known_id_merge_scratch: Vec<(u32, [u8; 32])>,
    epoch_creations: BTreeMap<[u8; 32], EpochCreationEntry>,
    pending_candidate_ids: Vec<(u32, PendingCreationCandidate)>,
    pending_candidate_raw: Vec<([u8; 32], PendingCreationCandidate)>,
    next_candidate_sequence: u64,
    registry_scratch: RegistryResolutionScratch,
    resolved_candidates: Vec<ResolvedCreationCandidate>,
    cached_candidates: Vec<ResolvedCreationCandidate>,
    new_accounts: Vec<ResolvedDiscoveredAccount>,
    anchor_position: &'a mut Option<SourceTransactionCoordinate>,
    anchor_count: &'a mut u64,
    anchor_signature_bytes: Vec<u8>,
    target_table: Option<EpochTargetTable>,
    batch_hints_dirty: bool,
    writer: Mutex<SharedRawWriter>,
    stats: EpochScanStats,
    extractor_stats: SingleReadExtractorStats,
}

#[derive(Debug)]
struct SingleReadEpochResult {
    discovery: EpochDiscoveryResult,
    shard: ShardSummary,
    reader: BatchBarrierBlockStats,
    extractor: SingleReadExtractorStats,
    account_ids: EpochAccountIdLog,
}

struct VerifiedEpochRegistry {
    registry_file: File,
    index: FileBackedKeyIndex,
    entries: u32,
    registry_path: PathBuf,
}

#[derive(Debug, Default)]
struct RegistryResolutionScratch {
    requested_ids: Vec<u32>,
    raw_local_ids: Vec<Option<u32>>,
    raw_verified_ids: Vec<u32>,
    rows: Vec<(u32, [u8; 32])>,
    read_buffer: Vec<u8>,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct RegistryResolutionStats {
    pub(crate) registry_rows_read: u64,
    pub(crate) registry_coalesced_read_calls: u64,
    pub(crate) registry_read_bytes: u64,
    pub(crate) mphf_lookups: u64,
}

impl RegistryResolutionStats {
    fn add(&mut self, other: Self) -> Result<()> {
        self.registry_rows_read = self
            .registry_rows_read
            .checked_add(other.registry_rows_read)
            .context("registry row-read count overflow")?;
        self.registry_coalesced_read_calls = self
            .registry_coalesced_read_calls
            .checked_add(other.registry_coalesced_read_calls)
            .context("registry read-call count overflow")?;
        self.registry_read_bytes = self
            .registry_read_bytes
            .checked_add(other.registry_read_bytes)
            .context("registry read-byte count overflow")?;
        self.mphf_lookups = self
            .mphf_lookups
            .checked_add(other.mphf_lookups)
            .context("registry MPHF lookup count overflow")?;
        Ok(())
    }
}

const REGISTRY_BULK_READ_MAX_GAP_BYTES: u64 = 4 << 10;
const REGISTRY_BULK_READ_MAX_BYTES: u64 = 8 << 20;

#[derive(Debug)]
struct EpochTargetTable {
    epoch: u64,
    mint: [u8; 32],
    mint_id: Option<u32>,
    mint_creation: SourceTransactionCoordinate,
    prior_id_bits: Vec<u64>,
    current_id_bits: Vec<u64>,
    current_ids: Vec<(u32, SourceInstructionCoordinate)>,
    raw: Vec<([u8; 32], SourceInstructionCoordinate)>,
    raw_delta_scratch: Vec<([u8; 32], SourceInstructionCoordinate)>,
    raw_merge_scratch: Vec<([u8; 32], SourceInstructionCoordinate)>,
    current_id_delta_scratch: Vec<(u32, SourceInstructionCoordinate)>,
    current_ids_merge_scratch: Vec<(u32, SourceInstructionCoordinate)>,
    account_id_entries: Vec<EpochAccountIdEntry>,
    source_generation_digest: [u8; 32],
}

impl VerifiedEpochRegistry {
    fn open(
        source: PinnedLocalRangeSource,
        manifest: &GenerationManifest,
        registry_entries: u32,
    ) -> Result<Self> {
        let registry = manifest.required_file(blockzilla_read_sdk::manifest::REGISTRY_FILE)?;
        let registry_index =
            manifest.required_file(blockzilla_read_sdk::manifest::REGISTRY_INDEX_FILE)?;
        ensure!(
            registry.size == u64::from(registry_entries) * 32,
            "registry.bin size differs from the opened reader entry count"
        );
        ensure!(registry_index.size != 0, "registry.mphf is empty");

        // Pin both files before loading the file-backed MPHF. All exact row checks below use the
        // same pinned source view.
        let registry_file = source
            .open_file(blockzilla_read_sdk::manifest::REGISTRY_FILE)
            .context("open pinned registry.bin")?;
        ensure!(
            registry_file.metadata()?.len() == registry.size,
            "pinned registry.bin size differs from its admitted size"
        );
        let index_path = source
            .root()
            .join(blockzilla_read_sdk::manifest::REGISTRY_INDEX_FILE);
        let index_file = source
            .open_file(blockzilla_read_sdk::manifest::REGISTRY_INDEX_FILE)
            .context("open pinned registry.mphf")?;
        ensure!(
            index_file.metadata()?.len() == registry_index.size,
            "pinned registry.mphf size differs from its admitted size"
        );
        let index = FileBackedKeyIndex::load_file(index_file, &index_path)
            .with_context(|| format!("load {}", index_path.display()))?;
        ensure!(
            index.len() == registry_entries as usize,
            "registry.mphf has {} entries but registry.bin has {registry_entries}",
            index.len()
        );
        Ok(Self {
            registry_file,
            index,
            entries: registry_entries,
            registry_path: manifest_path_for_error(manifest),
        })
    }

    fn verified_id(&self, raw: &[u8; 32]) -> Result<Option<u32>> {
        let Some(id) = self.index.lookup(raw).context("lookup registry.mphf")? else {
            return Ok(None);
        };
        let actual = self.read_id(id)?;
        ensure!(
            &actual == raw,
            "registry.mphf candidate ID {id} does not match registry.bin"
        );
        Ok(Some(id))
    }

    fn read_id(&self, id: u32) -> Result<[u8; 32]> {
        ensure!(
            id != 0 && id <= self.entries,
            "registry ID {id} is outside 1..={}",
            self.entries
        );
        let offset = u64::from(id - 1)
            .checked_mul(32)
            .context("registry byte offset overflow")?;
        let mut bytes = [0u8; 32];
        read_file_exact_at(&self.registry_file, &mut bytes, offset).with_context(|| {
            format!(
                "read registry ID {id} from {}",
                self.registry_path.display()
            )
        })?;
        Ok(bytes)
    }

    fn read_ids_bulk(
        &self,
        sorted_ids: &[u32],
        rows: &mut Vec<(u32, [u8; 32])>,
        read_buffer: &mut Vec<u8>,
    ) -> Result<RegistryResolutionStats> {
        rows.clear();
        rows.reserve(sorted_ids.len());
        let mut stats = RegistryResolutionStats::default();
        let mut start = 0usize;
        while start < sorted_ids.len() {
            let first_id = sorted_ids[start];
            ensure!(
                first_id != 0 && first_id <= self.entries,
                "registry ID {first_id} is outside 1..={}",
                self.entries
            );
            let group_offset = u64::from(first_id - 1)
                .checked_mul(32)
                .context("registry bulk-read offset overflow")?;
            let mut end = start + 1;
            let mut group_end = group_offset
                .checked_add(32)
                .context("registry bulk-read end overflow")?;
            while let Some(&id) = sorted_ids.get(end) {
                ensure!(
                    id != 0 && id <= self.entries,
                    "registry ID {id} is outside 1..={}",
                    self.entries
                );
                ensure!(
                    id > sorted_ids[end - 1],
                    "bulk registry IDs are not strictly sorted and unique"
                );
                let offset = u64::from(id - 1)
                    .checked_mul(32)
                    .context("registry bulk-read offset overflow")?;
                let next_end = offset
                    .checked_add(32)
                    .context("registry bulk-read end overflow")?;
                let gap = offset.saturating_sub(group_end);
                let group_bytes = next_end
                    .checked_sub(group_offset)
                    .context("registry bulk-read range underflow")?;
                if gap > REGISTRY_BULK_READ_MAX_GAP_BYTES
                    || group_bytes > REGISTRY_BULK_READ_MAX_BYTES
                {
                    break;
                }
                group_end = next_end;
                end += 1;
            }

            let byte_len = usize::try_from(
                group_end
                    .checked_sub(group_offset)
                    .context("registry bulk-read range underflow")?,
            )
            .context("registry bulk-read length exceeds usize")?;
            read_buffer.resize(byte_len, 0);
            let calls = read_file_exact_at(&self.registry_file, read_buffer, group_offset)
                .with_context(|| {
                    format!(
                        "bulk read registry IDs {}..{} from {}",
                        sorted_ids[start],
                        sorted_ids[end - 1],
                        self.registry_path.display()
                    )
                })?;
            stats.registry_coalesced_read_calls = stats
                .registry_coalesced_read_calls
                .checked_add(calls)
                .context("registry read-call count overflow")?;
            stats.registry_read_bytes = stats
                .registry_read_bytes
                .checked_add(u64::try_from(byte_len).context("registry read length exceeds u64")?)
                .context("registry read-byte count overflow")?;
            for &id in &sorted_ids[start..end] {
                let offset = u64::from(id - 1)
                    .checked_mul(32)
                    .and_then(|offset| offset.checked_sub(group_offset))
                    .and_then(|offset| usize::try_from(offset).ok())
                    .context("registry row offset exceeds bulk-read range")?;
                let raw: [u8; 32] = read_buffer
                    .get(offset..offset + 32)
                    .context("registry row is outside bulk-read bytes")?
                    .try_into()
                    .expect("checked registry row length");
                rows.push((id, raw));
            }
            stats.registry_rows_read = stats
                .registry_rows_read
                .checked_add(u64::try_from(end - start).context("registry row count exceeds u64")?)
                .context("registry row-read count overflow")?;
            start = end;
        }
        Ok(stats)
    }

    fn resolve_raw_accounts_bulk(
        &self,
        accounts: &[DiscoveredAccount],
        rows: &mut Vec<(u32, [u8; 32])>,
        read_buffer: &mut Vec<u8>,
    ) -> Result<(Vec<ResolvedDiscoveredAccount>, RegistryResolutionStats)> {
        let mut stats = RegistryResolutionStats::default();
        let mut requested_ids = Vec::with_capacity(accounts.len());
        let mut local_ids = Vec::with_capacity(accounts.len());
        for account in accounts {
            let local_id = self
                .index
                .lookup(&account.raw_pubkey)
                .context("lookup registry.mphf")?;
            stats.mphf_lookups = stats
                .mphf_lookups
                .checked_add(1)
                .context("registry MPHF lookup count overflow")?;
            if let Some(id) = local_id {
                requested_ids.push(id);
            }
            local_ids.push(local_id);
        }
        requested_ids.sort_unstable();
        requested_ids.dedup();
        stats.add(self.read_ids_bulk(&requested_ids, rows, read_buffer)?)?;

        let mut resolved = Vec::with_capacity(accounts.len());
        for (account, local_id) in accounts.iter().zip(local_ids) {
            if let Some(id) = local_id {
                let (_, actual) = rows
                    .binary_search_by_key(&id, |(candidate, _)| *candidate)
                    .ok()
                    .and_then(|index| rows.get(index))
                    .context("resolved registry ID is absent from its bulk read")?;
                ensure!(
                    *actual == account.raw_pubkey,
                    "registry.mphf candidate ID {id} does not match registry.bin"
                );
            }
            resolved.push(ResolvedDiscoveredAccount {
                raw_pubkey: account.raw_pubkey,
                first_creation: account.first_creation,
                local_id,
            });
        }
        Ok((resolved, stats))
    }

    fn resolve_creation_candidates_bulk(
        &self,
        ids: &[(u32, PendingCreationCandidate)],
        raw_refs: &[([u8; 32], PendingCreationCandidate)],
        scratch: &mut RegistryResolutionScratch,
        resolved: &mut Vec<ResolvedCreationCandidate>,
    ) -> Result<RegistryResolutionStats> {
        resolved.clear();
        resolved.reserve(ids.len().saturating_add(raw_refs.len()));
        let mut stats = RegistryResolutionStats::default();
        scratch.requested_ids.clear();
        scratch
            .requested_ids
            .reserve(ids.len().saturating_add(raw_refs.len()));
        scratch.requested_ids.extend(ids.iter().map(|(id, _)| *id));
        scratch.raw_local_ids.clear();
        scratch.raw_local_ids.reserve(raw_refs.len());
        scratch.raw_verified_ids.clear();
        scratch.raw_verified_ids.reserve(raw_refs.len());
        for (raw, _) in raw_refs {
            let local_id = self.index.lookup(raw).context("lookup registry.mphf")?;
            stats.mphf_lookups = stats
                .mphf_lookups
                .checked_add(1)
                .context("registry MPHF lookup count overflow")?;
            if let Some(id) = local_id {
                scratch.requested_ids.push(id);
                scratch.raw_verified_ids.push(id);
            }
            scratch.raw_local_ids.push(local_id);
        }
        scratch.requested_ids.sort_unstable();
        scratch.requested_ids.dedup();
        scratch.raw_verified_ids.sort_unstable();
        scratch.raw_verified_ids.dedup();
        stats.add(self.read_ids_bulk(
            &scratch.requested_ids,
            &mut scratch.rows,
            &mut scratch.read_buffer,
        )?)?;

        for &(id, raw) in &scratch.rows {
            if scratch.raw_verified_ids.binary_search(&id).is_ok() {
                continue;
            }
            ensure!(
                self.index
                    .lookup(&raw)
                    .context("round-trip registry.mphf")?
                    == Some(id),
                "registry ID {id} does not round-trip through registry.mphf"
            );
            stats.mphf_lookups = stats
                .mphf_lookups
                .checked_add(1)
                .context("registry MPHF lookup count overflow")?;
        }

        for &(id, pending) in ids {
            let raw = scratch
                .rows
                .binary_search_by_key(&id, |(candidate, _)| *candidate)
                .ok()
                .and_then(|index| scratch.rows.get(index))
                .map(|(_, raw)| *raw)
                .context("creation registry ID is absent from its bulk read")?;
            resolved.push(ResolvedCreationCandidate {
                source_reference: CompactPubkey::Id(id),
                raw_pubkey: raw,
                local_id: Some(id),
                coordinate: pending.coordinate,
                ledger_sequence: pending.ledger_sequence,
            });
        }
        for (&(raw, pending), &local_id) in raw_refs.iter().zip(&scratch.raw_local_ids) {
            if let Some(id) = local_id {
                let actual = scratch
                    .rows
                    .binary_search_by_key(&id, |(candidate, _)| *candidate)
                    .ok()
                    .and_then(|index| scratch.rows.get(index))
                    .map(|(_, actual)| *actual)
                    .context("raw creation registry ID is absent from its bulk read")?;
                ensure!(
                    actual == raw,
                    "registry.mphf candidate ID {id} does not match registry.bin"
                );
            }
            resolved.push(ResolvedCreationCandidate {
                source_reference: CompactPubkey::Raw(raw),
                raw_pubkey: raw,
                local_id,
                coordinate: pending.coordinate,
                ledger_sequence: pending.ledger_sequence,
            });
        }
        Ok(stats)
    }

    fn resolve_verified(&self, reference: CompactPubkey) -> Result<[u8; 32]> {
        match reference {
            CompactPubkey::Raw(raw) => Ok(raw),
            CompactPubkey::Id(id) => {
                let raw = self.read_id(id)?;
                ensure!(
                    self.index
                        .lookup(&raw)
                        .context("round-trip registry.mphf")?
                        == Some(id),
                    "registry ID {id} does not round-trip through registry.mphf"
                );
                Ok(raw)
            }
        }
    }
}

fn read_file_exact_at(file: &File, bytes: &mut [u8], offset: u64) -> std::io::Result<u64> {
    let mut read = 0usize;
    let mut calls = 0u64;
    while read < bytes.len() {
        #[cfg(unix)]
        let count = file.read_at(&mut bytes[read..], offset + read as u64)?;
        #[cfg(windows)]
        let count = file.seek_read(&mut bytes[read..], offset + read as u64)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "positioned registry read ended early",
            ));
        }
        calls = calls.saturating_add(1);
        read += count;
    }
    Ok(calls)
}

fn manifest_path_for_error(manifest: &GenerationManifest) -> PathBuf {
    PathBuf::from(format!("epoch-{}", manifest.epoch))
        .join(blockzilla_read_sdk::manifest::REGISTRY_FILE)
}

#[cfg(any())]
impl EpochLocalTracker {
    fn tracked_account_count(&self) -> usize {
        self.raw_accounts.len()
    }

    fn compile(tracker: &TokenAccountTracker, registry: &VerifiedEpochRegistry) -> Result<Self> {
        let mint = tracker.mint();
        let mint_id = registry.verified_id(&mint)?;
        let token_program_ids = [
            registry.verified_id(&SPL_TOKEN_PROGRAM_ID)?,
            registry.verified_id(&SPL_TOKEN_2022_PROGRAM_ID)?,
        ];
        let raw_accounts = tracker.tracked_accounts().copied().collect::<HashSet<_>>();
        let seen_raw_accounts = raw_accounts.clone();
        let mut account_ids = HashSet::with_capacity(raw_accounts.len());
        for raw in &raw_accounts {
            if let Some(id) = registry.verified_id(raw)? {
                account_ids.insert(id);
            }
        }
        Ok(Self {
            mint,
            mint_id,
            token_program_ids,
            raw_accounts,
            seen_raw_accounts,
            account_ids,
        })
    }

    fn select(
        &mut self,
        transaction: &CompactTransactionFacts,
        registry: &VerifiedEpochRegistry,
    ) -> Result<bool> {
        if transaction.has_error {
            let mut temporary = self.clone();
            return temporary.simulate(transaction, registry);
        }
        self.simulate(transaction, registry)
    }

    fn simulate(
        &mut self,
        transaction: &CompactTransactionFacts,
        registry: &VerifiedEpochRegistry,
    ) -> Result<bool> {
        let mut selected = self.apply_balances(&transaction.pre_token_balances, registry)?;
        for instruction in &transaction.instructions {
            selected |= instruction
                .accounts
                .iter()
                .any(|reference| self.touches(*reference));
            if self.is_token_program(instruction.program_id) {
                self.apply_token_instruction(instruction, registry)?;
            }
            selected |= instruction
                .accounts
                .iter()
                .any(|reference| self.touches(*reference));
        }
        selected |= self.apply_balances(&transaction.post_token_balances, registry)?;
        selected |= transaction.instructions.iter().any(|instruction| {
            instruction
                .accounts
                .iter()
                .any(|reference| self.touches(*reference))
        });
        Ok(selected)
    }

    fn apply_balances(
        &mut self,
        balances: &[CompactTokenBalanceFact],
        registry: &VerifiedEpochRegistry,
    ) -> Result<bool> {
        let mut selected = false;
        for balance in balances {
            if self.matches_key(balance.mint, self.mint, self.mint_id) {
                selected = true;
                self.add_account(balance.account, registry)?;
            } else {
                self.remove_account(balance.account, registry)?;
            }
        }
        Ok(selected)
    }

    fn apply_token_instruction(
        &mut self,
        instruction: &CompactInstructionFact,
        registry: &VerifiedEpochRegistry,
    ) -> Result<()> {
        let accounts = instruction.accounts.as_slice();
        match (instruction.data.first().copied(), accounts) {
            (Some(1 | 16 | 18), [account, mint, ..]) => {
                self.set_account_mint(*account, *mint, registry)?
            }
            (Some(3), [source, destination, ..]) => {
                if self.touches(*source) || self.touches(*destination) {
                    self.add_account(*source, registry)?;
                    self.add_account(*destination, registry)?;
                }
            }
            (Some(7 | 14), [mint, destination, ..]) => {
                self.set_account_mint(*destination, *mint, registry)?
            }
            (Some(8 | 10 | 11 | 13 | 15), [account, mint, ..]) => {
                self.set_account_mint(*account, *mint, registry)?
            }
            (Some(12), [source, mint, destination, ..]) => {
                self.set_account_mint(*source, *mint, registry)?;
                self.set_account_mint(*destination, *mint, registry)?;
            }
            (Some(9), [account, ..]) => self.remove_account(*account, registry)?,
            _ => {}
        }
        Ok(())
    }

    fn set_account_mint(
        &mut self,
        account: CompactPubkey,
        mint: CompactPubkey,
        registry: &VerifiedEpochRegistry,
    ) -> Result<()> {
        if self.matches_key(mint, self.mint, self.mint_id) {
            self.add_account(account, registry)
        } else {
            self.remove_account(account, registry)
        }
    }

    fn add_account(
        &mut self,
        reference: CompactPubkey,
        registry: &VerifiedEpochRegistry,
    ) -> Result<()> {
        let raw = registry.resolve_verified(reference)?;
        self.raw_accounts.insert(raw);
        self.seen_raw_accounts.insert(raw);
        if let Some(id) = registry.verified_id(&raw)? {
            self.account_ids.insert(id);
        }
        Ok(())
    }

    fn remove_account(
        &mut self,
        reference: CompactPubkey,
        registry: &VerifiedEpochRegistry,
    ) -> Result<()> {
        let raw = registry.resolve_verified(reference)?;
        self.raw_accounts.remove(&raw);
        if let Some(id) = registry.verified_id(&raw)? {
            self.account_ids.remove(&id);
        }
        Ok(())
    }

    fn touches(&self, reference: CompactPubkey) -> bool {
        self.matches_key(reference, self.mint, self.mint_id)
            || match reference {
                CompactPubkey::Id(id) => self.account_ids.contains(&id),
                CompactPubkey::Raw(raw) => self.raw_accounts.contains(&raw),
            }
    }

    fn is_token_program(&self, reference: CompactPubkey) -> bool {
        self.matches_key(reference, SPL_TOKEN_PROGRAM_ID, self.token_program_ids[0])
            || self.matches_key(
                reference,
                SPL_TOKEN_2022_PROGRAM_ID,
                self.token_program_ids[1],
            )
    }

    fn matches_key(&self, reference: CompactPubkey, raw: [u8; 32], id: Option<u32>) -> bool {
        match reference {
            CompactPubkey::Id(reference_id) => Some(reference_id) == id,
            CompactPubkey::Raw(reference_raw) => reference_raw == raw,
        }
    }

    fn finish(
        self,
        epoch: u64,
        generation_digest: [u8; 32],
        registry: &VerifiedEpochRegistry,
    ) -> Result<TrackerTransitionLog> {
        let mut seen_raw_accounts = self.seen_raw_accounts.into_iter().collect::<Vec<_>>();
        seen_raw_accounts.push(self.mint);
        seen_raw_accounts.sort_unstable();
        seen_raw_accounts.dedup();
        let mut verified_ids = HashSet::with_capacity(self.raw_accounts.len());
        let mut entries = Vec::with_capacity(seen_raw_accounts.len());
        for raw_pubkey in seen_raw_accounts {
            let local_id = registry.verified_id(&raw_pubkey)?;
            let role = if raw_pubkey == self.mint {
                TrackerTransitionRole::TargetMint
            } else {
                TrackerTransitionRole::TokenAccount
            };
            let active_at_epoch_end = role == TrackerTransitionRole::TargetMint
                || self.raw_accounts.contains(&raw_pubkey);
            if active_at_epoch_end
                && role == TrackerTransitionRole::TokenAccount
                && let Some(id) = local_id
            {
                verified_ids.insert(id);
            }
            entries.push(TrackerTransitionEntry {
                local_id,
                raw_pubkey,
                role,
                active_at_epoch_end,
            });
        }
        ensure!(
            verified_ids == self.account_ids,
            "epoch-local tracked ID set differs from verified raw-key mappings"
        );
        Ok(TrackerTransitionLog {
            schema_version: DUMP_SCHEMA_VERSION,
            epoch,
            source_generation_digest: generation_digest,
            entries,
        })
    }
}

impl DiscoveryMatcher {
    fn build(mint: [u8; 32], registry: &VerifiedEpochRegistry) -> Result<Self> {
        let mint_id = registry.verified_id(&mint)?;
        Self::build_with_mint_id(mint, mint_id, registry)
    }

    fn build_with_mint_id(
        mint: [u8; 32],
        mint_id: Option<u32>,
        registry: &VerifiedEpochRegistry,
    ) -> Result<Self> {
        Ok(Self::with_ids(
            mint,
            mint_id,
            [
                registry.verified_id(&SPL_TOKEN_PROGRAM_ID)?,
                registry.verified_id(&SPL_TOKEN_2022_PROGRAM_ID)?,
            ],
        ))
    }

    fn with_ids(mint: [u8; 32], mint_id: Option<u32>, token_program_ids: [Option<u32>; 2]) -> Self {
        Self {
            mint,
            mint_id,
            token_program_ids,
        }
    }

    fn is_mint(self, reference: CompactPubkey) -> bool {
        match reference {
            CompactPubkey::Id(id) => Some(id) == self.mint_id,
            CompactPubkey::Raw(raw) => raw == self.mint,
        }
    }

    fn is_token_program(self, reference: CompactPubkey) -> bool {
        match reference {
            CompactPubkey::Id(id) => self.token_program_ids.contains(&Some(id)),
            CompactPubkey::Raw(raw) => {
                raw == SPL_TOKEN_PROGRAM_ID || raw == SPL_TOKEN_2022_PROGRAM_ID
            }
        }
    }
}

impl EpochTargetTable {
    fn build(
        epoch: u64,
        mint: [u8; 32],
        mint_creation: SourceTransactionCoordinate,
        accounts: &[DiscoveredAccount],
        registry: &VerifiedEpochRegistry,
        generation_digest: [u8; 32],
    ) -> Result<(Self, EpochAccountIdLog, RegistryResolutionStats)> {
        let mut admitted = Vec::with_capacity(accounts.len() + 1);
        admitted.push(DiscoveredAccount {
            raw_pubkey: mint,
            first_creation: SourceInstructionCoordinate {
                epoch: mint_creation.epoch,
                slot: mint_creation.slot,
                source_block_id: mint_creation.source_block_id,
                tx_index: mint_creation.tx_index,
                instruction_index: 0,
            },
        });
        admitted.extend(
            accounts
                .iter()
                .copied()
                .filter(|account| account.first_creation.epoch <= epoch),
        );
        let mut rows = Vec::new();
        let mut read_buffer = Vec::new();
        let (resolved, registry_stats) =
            registry.resolve_raw_accounts_bulk(&admitted, &mut rows, &mut read_buffer)?;
        let mint_id = resolved
            .first()
            .context("bulk registry result has no target mint")?
            .local_id;
        let mut table = Self::build_resolved(
            epoch,
            mint,
            mint_id,
            mint_creation,
            &resolved[1..],
            registry.entries,
            generation_digest,
        )?;
        let account_ids = table.account_id_log();
        Ok((table, account_ids, registry_stats))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_resolved(
        epoch: u64,
        mint: [u8; 32],
        mint_id: Option<u32>,
        mint_creation: SourceTransactionCoordinate,
        accounts: &[ResolvedDiscoveredAccount],
        registry_entries: u32,
        generation_digest: [u8; 32],
    ) -> Result<Self> {
        let bit_words = usize::try_from(registry_entries)
            .context("registry entry count exceeds usize")?
            .div_ceil(64);
        let mut prior_id_bits = vec![0u64; bit_words];
        let mut current_id_bits = vec![0u64; bit_words];
        let mut current_ids = Vec::new();
        let mut raw = Vec::with_capacity(accounts.len());
        let mut entries = Vec::with_capacity(accounts.len() + 1);
        entries.push(EpochAccountIdEntry {
            local_id: mint_id,
            raw_pubkey: mint,
            role: AccountIdRole::TargetMint,
            first_creation: None,
        });
        for account in accounts {
            if account.first_creation.epoch <= epoch {
                let local_id = account.local_id;
                raw.push((account.raw_pubkey, account.first_creation));
                if let Some(id) = local_id {
                    let zero_based =
                        usize::try_from(id - 1).context("registry ID exceeds usize")?;
                    if account.first_creation.epoch < epoch {
                        prior_id_bits[zero_based / 64] |= 1u64 << (zero_based % 64);
                    } else {
                        current_id_bits[zero_based / 64] |= 1u64 << (zero_based % 64);
                        current_ids.push((id, account.first_creation));
                    }
                }
                entries.push(EpochAccountIdEntry {
                    local_id,
                    raw_pubkey: account.raw_pubkey,
                    role: AccountIdRole::TokenAccount,
                    first_creation: Some(account.first_creation),
                });
            }
        }
        entries.sort_unstable_by_key(|entry| entry.raw_pubkey);
        ensure!(
            entries
                .windows(2)
                .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey),
            "target mint duplicates a discovered token account"
        );
        current_ids.sort_unstable_by_key(|(id, _)| *id);
        ensure!(
            current_ids.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "two discovered accounts map to the same epoch {epoch} registry ID"
        );
        raw.sort_unstable_by_key(|(raw_pubkey, _)| *raw_pubkey);
        ensure!(
            raw.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "duplicate discovered account"
        );
        Ok(Self {
            epoch,
            mint,
            mint_id,
            mint_creation,
            prior_id_bits,
            current_id_bits,
            current_ids,
            raw,
            raw_delta_scratch: Vec::new(),
            raw_merge_scratch: Vec::new(),
            current_id_delta_scratch: Vec::new(),
            current_ids_merge_scratch: Vec::new(),
            account_id_entries: entries,
            source_generation_digest: generation_digest,
        })
    }

    fn account_id_log(&mut self) -> EpochAccountIdLog {
        self.account_id_entries
            .sort_unstable_by_key(|entry| entry.raw_pubkey);
        EpochAccountIdLog {
            schema_version: DUMP_SCHEMA_VERSION,
            epoch: self.epoch,
            source_generation_digest: self.source_generation_digest,
            entries: self.account_id_entries.clone(),
        }
    }

    fn into_account_id_log(mut self) -> EpochAccountIdLog {
        self.account_id_entries
            .sort_unstable_by_key(|entry| entry.raw_pubkey);
        EpochAccountIdLog {
            schema_version: DUMP_SCHEMA_VERSION,
            epoch: self.epoch,
            source_generation_digest: self.source_generation_digest,
            entries: self.account_id_entries,
        }
    }

    fn reference_is_eligible(
        &self,
        reference: CompactPubkey,
        slot: u64,
        source_block_id: u32,
        tx_index: u32,
    ) -> bool {
        let is_mint = match reference {
            CompactPubkey::Id(id) => Some(id) == self.mint_id,
            CompactPubkey::Raw(raw) => raw == self.mint,
        };
        if is_mint {
            return (self.epoch, slot, source_block_id, tx_index)
                >= (
                    self.mint_creation.epoch,
                    self.mint_creation.slot,
                    self.mint_creation.source_block_id,
                    self.mint_creation.tx_index,
                );
        }
        match reference {
            CompactPubkey::Id(id) => {
                let zero_based = match usize::try_from(id.saturating_sub(1)) {
                    Ok(value) if id != 0 => value,
                    _ => return false,
                };
                if self
                    .prior_id_bits
                    .get(zero_based / 64)
                    .is_some_and(|word| word & (1u64 << (zero_based % 64)) != 0)
                {
                    return true;
                }
                if self
                    .current_id_bits
                    .get(zero_based / 64)
                    .is_none_or(|word| word & (1u64 << (zero_based % 64)) == 0)
                {
                    return false;
                }
                self.current_ids
                    .binary_search_by_key(&id, |(candidate, _)| *candidate)
                    .ok()
                    .and_then(|index| self.current_ids.get(index))
                    .is_some_and(|(_, creation)| {
                        transaction_at_or_after_creation(
                            self.epoch,
                            slot,
                            source_block_id,
                            tx_index,
                            *creation,
                        )
                    })
            }
            CompactPubkey::Raw(raw) => self
                .raw
                .binary_search_by_key(&raw, |(candidate, _)| *candidate)
                .ok()
                .and_then(|index| self.raw.get(index))
                .is_some_and(|(_, creation)| {
                    transaction_at_or_after_creation(
                        self.epoch,
                        slot,
                        source_block_id,
                        tx_index,
                        *creation,
                    )
                }),
        }
    }

    /// Add accounts first discovered in the current epoch after one batch
    /// barrier. The caller supplies only globally new keys in ledger order.
    fn extend_current_accounts(&mut self, accounts: &[ResolvedDiscoveredAccount]) -> Result<()> {
        self.raw_delta_scratch.clear();
        self.raw_delta_scratch.reserve(accounts.len());
        self.current_id_delta_scratch.clear();
        self.current_id_delta_scratch.reserve(accounts.len());
        for account in accounts {
            ensure!(
                account.first_creation.epoch == self.epoch,
                "batch extension contains an account from epoch {} while scanning epoch {}",
                account.first_creation.epoch,
                self.epoch
            );
            ensure!(
                account.raw_pubkey != self.mint,
                "target mint is also a discovered token account"
            );
            self.raw_delta_scratch
                .push((account.raw_pubkey, account.first_creation));
            if let Some(id) = account.local_id {
                let zero_based = usize::try_from(id - 1).context("registry ID exceeds usize")?;
                let word = self
                    .current_id_bits
                    .get_mut(zero_based / 64)
                    .context("current registry ID exceeds the membership bitset")?;
                *word |= 1u64 << (zero_based % 64);
                self.current_id_delta_scratch
                    .push((id, account.first_creation));
            }
            self.account_id_entries.push(EpochAccountIdEntry {
                local_id: account.local_id,
                raw_pubkey: account.raw_pubkey,
                role: AccountIdRole::TokenAccount,
                first_creation: Some(account.first_creation),
            });
        }
        ensure!(
            self.raw_delta_scratch
                .windows(2)
                .all(|pair| pair[0].0 < pair[1].0),
            "batch extension raw keys are not strictly sorted and unique"
        );
        self.current_id_delta_scratch
            .sort_unstable_by_key(|(id, _)| *id);
        ensure!(
            self.current_id_delta_scratch
                .windows(2)
                .all(|pair| pair[0].0 < pair[1].0),
            "two batch accounts map to the same epoch {} registry ID",
            self.epoch
        );
        merge_sorted_raw_accounts(
            &mut self.raw,
            &self.raw_delta_scratch,
            &mut self.raw_merge_scratch,
        )?;
        merge_sorted_current_ids(
            &mut self.current_ids,
            &self.current_id_delta_scratch,
            &mut self.current_ids_merge_scratch,
        )?;
        Ok(())
    }
}

fn merge_sorted_raw_accounts(
    current: &mut Vec<([u8; 32], SourceInstructionCoordinate)>,
    delta: &[([u8; 32], SourceInstructionCoordinate)],
    scratch: &mut Vec<([u8; 32], SourceInstructionCoordinate)>,
) -> Result<()> {
    scratch.clear();
    scratch.reserve(current.len().saturating_add(delta.len()));
    let (mut left, mut right) = (0usize, 0usize);
    while left < current.len() && right < delta.len() {
        match current[left].0.cmp(&delta[right].0) {
            std::cmp::Ordering::Less => {
                scratch.push(current[left]);
                left += 1;
            }
            std::cmp::Ordering::Greater => {
                scratch.push(delta[right]);
                right += 1;
            }
            std::cmp::Ordering::Equal => anyhow::bail!("batch extension repeats a tracked account"),
        }
    }
    scratch.extend_from_slice(&current[left..]);
    scratch.extend_from_slice(&delta[right..]);
    std::mem::swap(current, scratch);
    Ok(())
}

fn merge_sorted_current_ids(
    current: &mut Vec<(u32, SourceInstructionCoordinate)>,
    delta: &[(u32, SourceInstructionCoordinate)],
    scratch: &mut Vec<(u32, SourceInstructionCoordinate)>,
) -> Result<()> {
    scratch.clear();
    scratch.reserve(current.len().saturating_add(delta.len()));
    let (mut left, mut right) = (0usize, 0usize);
    while left < current.len() && right < delta.len() {
        match current[left].0.cmp(&delta[right].0) {
            std::cmp::Ordering::Less => {
                scratch.push(current[left]);
                left += 1;
            }
            std::cmp::Ordering::Greater => {
                scratch.push(delta[right]);
                right += 1;
            }
            std::cmp::Ordering::Equal => {
                anyhow::bail!("two tracked accounts map to the same current-epoch registry ID")
            }
        }
    }
    scratch.extend_from_slice(&current[left..]);
    scratch.extend_from_slice(&delta[right..]);
    std::mem::swap(current, scratch);
    Ok(())
}

fn transaction_at_or_after_creation(
    epoch: u64,
    slot: u64,
    source_block_id: u32,
    tx_index: u32,
    creation: SourceInstructionCoordinate,
) -> bool {
    (epoch, slot, source_block_id, tx_index)
        >= (
            creation.epoch,
            creation.slot,
            creation.source_block_id,
            creation.tx_index,
        )
}

/// Extract one complete, independently readable transaction shard for each source epoch.
///
/// Selection is temporal. The coordinator owns one token-account tracker and applies projected
/// transaction facts in canonical ledger order across all selected epochs.
#[cfg(any())]
pub fn extract_epoch_shards_stateful_removed(config: &ExtractConfig) -> Result<()> {
    ensure!(
        (1..=MAX_WORKERS).contains(&config.workers),
        "workers must be 1..={MAX_WORKERS}"
    );
    ensure!(
        !config.allow_indeterminate,
        "opaque transaction and metadata fallbacks cannot be written as standalone records"
    );
    validate_source_mode(config)?;
    let mint = parse_pubkey(&config.mint, "mint")?;
    let anchor_signature = parse_signature(&config.mint_signature)?;
    let inputs = discover_epochs(config)?;
    let first_epoch = inputs.first().expect("nonempty epoch discovery").epoch;
    let last_epoch = inputs.last().expect("nonempty epoch discovery").epoch;
    let shard_root = config.output.join(EPOCH_SHARDS_DIR);
    prepare_extraction_directories(config, &shard_root)?;
    let identity = ResumeIdentity {
        dump_schema_version: DUMP_SCHEMA_VERSION,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        first_epoch,
        last_epoch,
        cluster_id: inputs[0].manifest.cluster_id.clone(),
        slots_per_epoch: inputs[0].manifest.slots_per_epoch,
        source_binding: dump_source_binding(config),
        extraction_mode: ResumeExtractionMode::TwoPass,
        single_read_match_hints: false,
    };
    let mut layout = discover_resume_shard_layout(&shard_root, first_epoch)?;
    if let Some((epoch, _)) = layout.partial {
        let quarantined = quarantine_partial_shard(&shard_root, epoch)?
            .context("discovered partial shard disappeared before quarantine")?;
        let _ = quarantine_pending_resume_checkpoint(&config.output)?;
        eprintln!(
            "token-dump recovery: preserved incomplete epoch {epoch} shard as {}",
            quarantined.display()
        );
        layout = discover_resume_shard_layout(&shard_root, first_epoch)?;
    }

    let observed =
        validate_resume_shards(config, &inputs, &layout.complete, mint, anchor_signature)?;
    let committed = load_resume_checkpoint(&config.output, &identity)?;
    let pending = load_pending_resume_checkpoint(&config.output, &identity)?;
    let checkpoint =
        reconcile_resume_checkpoints(&config.output, &identity, committed, pending, &observed)?;

    let (mut tracker, mut anchor_count, mut shard_bindings, mut summary) = match checkpoint {
        Some(checkpoint) => {
            checkpoint.payload.validate_shard_prefix(&observed)?;
            let summary = RootSummary {
                transactions: checkpoint.payload.cumulative.transactions,
            };
            (
                checkpoint.tracker,
                checkpoint.payload.anchor_occurrences,
                checkpoint.payload.shards,
                summary,
            )
        }
        None => {
            ensure!(
                observed.is_empty(),
                "validated epoch shards exist without a matching authenticated resume checkpoint"
            );
            (
                TokenAccountTracker::new(mint),
                0,
                Vec::new(),
                RootSummary::default(),
            )
        }
    };
    let resumed_epochs = shard_bindings.len();
    let mut progress = ExtractionProgress::start(first_epoch, last_epoch, resumed_epochs);

    for input in inputs.into_iter().skip(resumed_epochs) {
        let epoch_started = Instant::now();
        progress.epoch_start(input.epoch, tracker.tracked_account_count());
        let select_timer = progress.pass_start(input.epoch, "select");
        let selection = discover_epoch_selection(
            config,
            input,
            &mut tracker,
            anchor_signature,
            &mut anchor_count,
        )?;
        let selected_transactions = selected_transaction_count(&selection.selected);
        select_timer.complete(PassMetrics {
            blocks: selection.stats.blocks,
            transactions: selection.stats.transactions,
            selected_transactions,
            tracked_accounts: tracker.tracked_account_count(),
            compressed_bytes: selection.stats.compressed_bytes,
            output_transactions: 0,
        });

        let partial_path = create_partial_shard_directory(&shard_root, selection.input.epoch)?;
        let shard_timer = progress.pass_start(selection.input.epoch, "build_shard");
        let shard = write_epoch_shard(config, &partial_path, &selection, mint, anchor_signature)?;
        shard_timer.complete(PassMetrics {
            blocks: selection.stats.blocks.saturating_mul(2),
            transactions: selection.stats.transactions.saturating_mul(2),
            selected_transactions,
            tracked_accounts: tracker.tracked_account_count(),
            compressed_bytes: shard.compressed_bytes,
            output_transactions: shard.transactions,
        });
        let input = &selection.input;
        let source_generation_digest = opened_generation_digest(input, config)?;
        let binding = validate_epoch_shard_for_resume(
            input.epoch,
            &partial_path,
            resume_target_binding(config, mint, anchor_signature),
            &identity.source_binding,
            source_generation_digest,
        )?;
        let mut next_bindings = shard_bindings.clone();
        next_bindings.push(binding);
        let next_checkpoint = ResumeCheckpointPayload::new(
            identity.clone(),
            anchor_count,
            next_bindings.clone(),
            &tracker,
        )?;
        stage_resume_checkpoint(&config.output, &next_checkpoint)?;
        commit_partial_shard(&shard_root, input.epoch)?;
        let committed = promote_pending_resume_checkpoint(&config.output, &identity)?;
        ensure!(
            committed.payload == next_checkpoint,
            "promoted checkpoint differs from the staged epoch checkpoint"
        );
        shard_bindings = next_bindings;
        summary.transactions = summary
            .transactions
            .checked_add(shard.transactions)
            .context("root transaction count overflow")?;
        progress.epoch_complete(
            input.epoch,
            epoch_started.elapsed(),
            PassMetrics {
                blocks: selection.stats.blocks,
                transactions: selection.stats.transactions,
                selected_transactions,
                tracked_accounts: tracker.tracked_account_count(),
                compressed_bytes: selection
                    .stats
                    .compressed_bytes
                    .saturating_add(shard.compressed_bytes),
                output_transactions: shard.transactions,
            },
        );
    }
    ensure!(
        anchor_count == 1,
        "mint signature occurs {anchor_count} times at slot {}, expected exactly once",
        config.mint_slot
    );
    let root_manifest = DumpManifest {
        schema_version: DUMP_SCHEMA_VERSION,
        artifact_kind: DumpArtifactKind::RawExtractionRoot,
        complete: true,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        source_binding: dump_source_binding(config),
        first_epoch,
        last_epoch,
        transactions: summary.transactions,
        signatures: None,
        pubkeys: None,
        transaction_stream: EPOCH_SHARDS_DIR.to_owned(),
        transaction_stream_sha256: None,
        tracker_transition_log: None,
        tracker_transition_log_sha256: None,
        signature_stream: None,
        signature_stream_sha256: None,
        pubkey_registry: None,
        pubkey_registry_sha256: None,
        registry_maps: None,
    };
    let root_manifest_path = config.output.join(DUMP_MANIFEST_FILE);
    fs::write(
        &root_manifest_path,
        serde_json::to_vec_pretty(&root_manifest)?,
    )
    .with_context(|| format!("write {}", root_manifest_path.display()))?;
    sync_file(&root_manifest_path)?;
    sync_directory(&config.output)?;
    progress.run_complete();
    Ok(())
}

/// Run the two-pass, creation-only exact-byte extractor.
pub fn extract_epoch_shards(config: &ExtractConfig) -> Result<()> {
    ensure!(
        !config.single_read_match_hints || config.single_read_batches,
        "single-read match hints require single-read batches"
    );
    if config.single_read_batches {
        return extract_epoch_shards_single_read_batches(config);
    }
    if config.epoch_barrier {
        return extract_epoch_shards_epoch_barrier(config);
    }
    ensure!(
        (1..=MAX_WORKERS).contains(&config.workers),
        "workers must be 1..={MAX_WORKERS}"
    );
    ensure!(
        !config.allow_indeterminate,
        "opaque transaction and metadata fallbacks cannot be written as standalone records"
    );
    validate_source_mode(config)?;
    let mint = parse_pubkey(&config.mint, "mint")?;
    let anchor_signature = parse_signature(&config.mint_signature)?;
    let inputs = discover_epochs(config)?;
    let first_epoch = inputs.first().context("no input epochs")?.epoch;
    let last_epoch = inputs.last().context("no input epochs")?.epoch;

    let discoveries_root = config.output.join(DISCOVERY_SHARDS_DIR);
    let shard_root = config.output.join(EPOCH_SHARDS_DIR);
    let identity = ResumeIdentity {
        dump_schema_version: DUMP_SCHEMA_VERSION,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        first_epoch,
        last_epoch,
        cluster_id: inputs[0].manifest.cluster_id.clone(),
        slots_per_epoch: inputs[0].manifest.slots_per_epoch,
        source_binding: dump_source_binding(config),
        extraction_mode: ResumeExtractionMode::TwoPass,
        single_read_match_hints: false,
    };
    let recovery_notes = prepare_two_pass_extraction_directories(
        config,
        &discoveries_root,
        &shard_root,
        first_epoch,
    )?;

    let discovery_layout = discover_resume_shard_layout(&discoveries_root, first_epoch)?;
    let raw_layout = discover_resume_shard_layout(&shard_root, first_epoch)?;
    let accounts_path = config.output.join(ACCOUNTS_FILE);
    let root_manifest_path = config.output.join(DUMP_MANIFEST_FILE);
    let accounts_exist = regular_file_exists(&accounts_path)?;
    let root_manifest_exists = regular_file_exists(&root_manifest_path)?;
    let committed_checkpoint = load_resume_checkpoint(&config.output, &identity)?;
    let artifacts_exist = !discovery_layout.complete.is_empty()
        || accounts_exist
        || !raw_layout.complete.is_empty()
        || root_manifest_exists;
    ensure!(
        !artifacts_exist || committed_checkpoint.is_some(),
        "immutable extraction artifacts exist without an authenticated resume checkpoint"
    );
    let authenticated_complete_root = if root_manifest_exists {
        ensure!(
            committed_checkpoint
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.payload.stage == ResumeStage::Complete),
            "complete root manifest has no complete authenticated checkpoint"
        );
        true
    } else {
        false
    };
    let mut discovery_progress = ExtractionProgress::start_phase(
        first_epoch,
        last_epoch,
        discovery_layout.complete.len(),
        "account_discovery",
    );
    let mut raw_progress = ExtractionProgress::start_phase(
        first_epoch,
        last_epoch,
        raw_layout.complete.len(),
        "raw_copy",
    );
    for note in &recovery_notes {
        discovery_progress.note(None, "quarantined", note);
    }

    discovery_progress.note(None, "validating", "validate committed discovery artifacts");
    let (mut discovery_bindings, mut global_accounts) =
        validate_resume_discoveries(config, &inputs, &discovery_layout.complete)?;
    let epoch_count = inputs.len();
    let mut discovered_accounts = if accounts_exist {
        ensure!(
            discovery_bindings.len() == epoch_count,
            "the frozen account list exists before all discovery shards are complete"
        );
        let accounts = load_and_validate_frozen_accounts(
            &accounts_path,
            mint,
            config.mint_slot,
            first_epoch,
            last_epoch,
            &global_accounts,
        )?;
        if !authenticated_complete_root {
            let anchor_input = input_for_epoch(&inputs, accounts.anchor_position.epoch)?;
            ensure!(
                locate_anchor_transaction(config, anchor_input, anchor_signature)?
                    == accounts.anchor_position,
                "frozen mint-anchor coordinate differs from the admitted source"
            );
        }
        Some(accounts)
    } else {
        None
    };

    ensure!(
        discovered_accounts.is_some() || raw_layout.complete.is_empty(),
        "raw epoch shards exist without a frozen account list"
    );
    raw_progress.note(None, "validating", "validate committed raw epoch shards");
    let mut raw_bindings = if let Some(accounts) = discovered_accounts.as_ref() {
        validate_resume_shards(
            config,
            &inputs,
            &raw_layout.complete,
            mint,
            anchor_signature,
            accounts,
        )?
    } else {
        Vec::new()
    };
    let frozen_binding = discovered_accounts
        .as_ref()
        .map(|accounts| resume_frozen_binding(&accounts_path, accounts))
        .transpose()?;
    if let Some(checkpoint) = committed_checkpoint.as_ref() {
        checkpoint.payload.validate_artifact_prefixes(
            &discovery_bindings,
            frozen_binding.as_ref(),
            &raw_bindings,
        )?;
    }
    persist_resume_checkpoint(
        &config.output,
        &identity,
        &discovery_bindings,
        frozen_binding.as_ref(),
        &raw_bindings,
    )?;

    if root_manifest_exists {
        ensure!(
            discovery_bindings.len() == epoch_count
                && discovered_accounts.is_some()
                && raw_bindings.len() == epoch_count,
            "complete root manifest exists before all immutable artifacts are complete"
        );
        validate_raw_root_manifest(
            &root_manifest_path,
            config,
            first_epoch,
            last_epoch,
            frozen_binding.as_ref().expect("checked frozen binding"),
            &raw_bindings,
        )?;
        discovery_progress.run_complete();
        raw_progress.run_complete();
        return Ok(());
    }

    let mut anchor_position = discovered_accounts
        .as_ref()
        .map(|accounts| accounts.anchor_position);
    let mut anchor_count = 0u64;
    for input in inputs.iter().skip(discovery_bindings.len()) {
        let epoch_started = Instant::now();
        discovery_progress.epoch_start(input.epoch, global_accounts.len());
        let timer = discovery_progress.pass_start(input.epoch, "discover_accounts");
        let discovery =
            discover_epoch_creations(config, input, mint, anchor_signature, &mut anchor_count)?;
        if let Some(position) = discovery.anchor_position {
            ensure!(
                anchor_position.is_none(),
                "more than one mint-anchor coordinate was found"
            );
            anchor_position = Some(position);
        }
        ensure!(
            anchor_count <= 1,
            "more than one mint-anchor signature was found"
        );
        let partial = create_partial_shard_directory(&discoveries_root, input.epoch)?;
        let path = partial.join(CREATIONS_FILE);
        let bytes = wincode::config::serialize(
            &discovery.log,
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        )?;
        write_synced_bytes(&path, &bytes)?;
        sync_directory(&partial)?;
        let (binding, validated_log) = validate_resume_discovery(config, input, &partial, None)?;
        ensure!(
            validated_log == discovery.log,
            "validated discovery artifact differs from the in-memory epoch result"
        );
        commit_partial_shard(&discoveries_root, input.epoch)?;
        merge_discovery_accounts(&mut global_accounts, &validated_log);
        discovery_bindings.push(binding);
        persist_resume_checkpoint(
            &config.output,
            &identity,
            &discovery_bindings,
            None,
            &raw_bindings,
        )?;
        let metrics = PassMetrics {
            blocks: discovery.stats.blocks,
            transactions: discovery.stats.transactions,
            selected_transactions: 0,
            tracked_accounts: global_accounts.len(),
            compressed_bytes: discovery.stats.compressed_bytes,
            output_transactions: 0,
        };
        let elapsed = timer.complete(metrics);
        discovery_progress.epoch_complete(
            input.epoch,
            epoch_started.elapsed().max(elapsed),
            metrics,
        );
    }
    ensure!(
        discovery_bindings.len() == epoch_count,
        "discovery shard prefix is incomplete"
    );
    sync_directory(&discoveries_root)?;

    if discovered_accounts.is_none() {
        discovery_progress.note(None, "running", "freeze the global account list");
        let anchor_position = match anchor_position {
            Some(position) => position,
            None => {
                let anchor_input = inputs
                    .iter()
                    .find(|input| {
                        (input.manifest.epoch_start_slot()..=input.manifest.epoch_end_slot())
                            .contains(&config.mint_slot)
                    })
                    .context("no input epoch contains the mint slot")?;
                locate_anchor_transaction(config, anchor_input, anchor_signature)?
            }
        };
        let accounts = DiscoveredAccountList {
            schema_version: DUMP_SCHEMA_VERSION,
            mint,
            anchor_position,
            accounts: global_accounts
                .iter()
                .map(|(raw_pubkey, first_creation)| DiscoveredAccount {
                    raw_pubkey: *raw_pubkey,
                    first_creation: *first_creation,
                })
                .collect(),
        };
        validate_frozen_account_structure(
            &accounts,
            mint,
            config.mint_slot,
            first_epoch,
            last_epoch,
            &global_accounts,
        )?;
        let bytes = wincode::config::serialize(
            &accounts,
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        )?;
        let partial = create_partial_artifact_file(&config.output, ACCOUNTS_FILE, &bytes)?;
        ensure!(
            load_and_validate_frozen_accounts(
                &partial,
                mint,
                config.mint_slot,
                first_epoch,
                last_epoch,
                &global_accounts,
            )? == accounts,
            "partial frozen account artifact differs from its source value"
        );
        commit_partial_artifact_file(&config.output, ACCOUNTS_FILE)?;
        discovered_accounts = Some(accounts);
        let frozen = resume_frozen_binding(
            &accounts_path,
            discovered_accounts
                .as_ref()
                .expect("stored frozen accounts"),
        )?;
        persist_resume_checkpoint(
            &config.output,
            &identity,
            &discovery_bindings,
            Some(&frozen),
            &raw_bindings,
        )?;
        discovery_progress.note(None, "complete", "frozen global account list is durable");
    }
    discovery_progress.run_complete();
    let discovered_accounts = discovered_accounts.expect("frozen account list is present");
    let frozen_binding = resume_frozen_binding(&accounts_path, &discovered_accounts)?;

    for input in inputs.iter().skip(raw_bindings.len()) {
        let epoch_started = Instant::now();
        raw_progress.epoch_start(input.epoch, discovered_accounts.accounts.len());
        let timer = raw_progress.pass_start(input.epoch, "copy_raw_transactions");
        let partial = create_partial_shard_directory(&shard_root, input.epoch)?;
        let shard = write_frozen_epoch_shard(
            config,
            input,
            &partial,
            mint,
            anchor_signature,
            &discovered_accounts.accounts,
            discovered_accounts.anchor_position,
        )?;
        let binding = validate_resume_shard(
            config,
            input,
            &partial,
            mint,
            anchor_signature,
            &discovered_accounts,
            None,
        )?;
        ensure!(
            shard.transactions == binding.counters.transactions
                && shard.anchor_transactions == binding.counters.anchor_transactions,
            "validated raw shard counters differ from the epoch writer"
        );
        commit_partial_shard(&shard_root, input.epoch)?;
        raw_bindings.push(binding.clone());
        persist_resume_checkpoint(
            &config.output,
            &identity,
            &discovery_bindings,
            Some(&frozen_binding),
            &raw_bindings,
        )?;
        let metrics = PassMetrics {
            blocks: binding.counters.blocks_scanned,
            transactions: binding.counters.transactions_scanned,
            selected_transactions: binding.counters.transactions,
            tracked_accounts: discovered_accounts.accounts.len(),
            compressed_bytes: shard.compressed_bytes,
            output_transactions: binding.counters.transactions,
        };
        let elapsed = timer.complete(metrics);
        raw_progress.epoch_complete(input.epoch, epoch_started.elapsed().max(elapsed), metrics);
    }
    let cumulative = raw_bindings
        .iter()
        .try_fold(crate::resume::ResumeCounters::default(), |sum, binding| {
            sum.checked_add(binding.counters)
        })?;
    ensure!(
        cumulative.anchor_transactions == 1,
        "Pass B emitted the mint anchor {} times, expected exactly once",
        cumulative.anchor_transactions
    );

    let root_manifest = DumpManifest {
        schema_version: DUMP_SCHEMA_VERSION,
        artifact_kind: DumpArtifactKind::RawExtractionRoot,
        complete: true,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        source_binding: dump_source_binding(config),
        first_epoch,
        last_epoch,
        transactions: cumulative.transactions,
        signatures: None,
        pubkeys: None,
        transaction_stream: EPOCH_SHARDS_DIR.to_owned(),
        transaction_stream_sha256: None,
        account_id_log: None,
        account_id_log_sha256: None,
        discovered_accounts: Some(ACCOUNTS_FILE.to_owned()),
        discovered_accounts_sha256: Some(frozen_binding.accounts_sha256.clone()),
        discovered_account_count: Some(discovered_accounts.accounts.len() as u64),
        signature_stream: None,
        signature_stream_sha256: None,
        pubkey_registry: None,
        pubkey_registry_sha256: None,
        registry_maps: None,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&root_manifest)?;
    create_partial_artifact_file(&config.output, DUMP_MANIFEST_FILE, &manifest_bytes)?;
    commit_partial_artifact_file(&config.output, DUMP_MANIFEST_FILE)?;
    validate_raw_root_manifest(
        &root_manifest_path,
        config,
        first_epoch,
        last_epoch,
        &frozen_binding,
        &raw_bindings,
    )?;
    raw_progress.run_complete();
    Ok(())
}

/// Validate and reopen a completed single-read extraction for a larger epoch range.
///
/// Operators should run this against a hard-linked staging copy. The function keeps the
/// completed root controls under epoch-qualified backup names and writes a new authenticated
/// checkpoint only after it validates every existing discovery and raw shard.
pub fn prepare_completed_single_read_extension(config: &ExtractConfig) -> Result<()> {
    ensure!(config.resume, "prepare-extension requires --resume");
    ensure!(
        config.single_read_batches,
        "prepare-extension requires --single-read-batches"
    );
    ensure!(
        !config.epoch_barrier,
        "prepare-extension does not support --epoch-barrier"
    );
    validate_source_mode(config)?;

    let mint = parse_pubkey(&config.mint, "mint")?;
    let anchor_signature = parse_signature(&config.mint_signature)?;
    let inputs = discover_epochs(config)?;
    let first_epoch = inputs.first().context("no input epochs")?.epoch;
    let new_last_epoch = inputs.last().context("no input epochs")?.epoch;
    let manifest_path = config.output.join(DUMP_MANIFEST_FILE);
    let old_manifest: DumpManifest =
        serde_json::from_slice(&read_bounded_regular_file(&manifest_path, 16 << 20)?)
            .with_context(|| format!("parse {}", manifest_path.display()))?;
    ensure!(
        old_manifest.schema_version == DUMP_SCHEMA_VERSION
            && old_manifest.artifact_kind == DumpArtifactKind::RawExtractionRoot
            && old_manifest.complete,
        "prepare-extension input is not a complete raw extraction"
    );
    ensure!(
        old_manifest.first_epoch == first_epoch,
        "completed extraction starts at epoch {}, but the admitted source starts at epoch {first_epoch}",
        old_manifest.first_epoch
    );
    let old_last_epoch = old_manifest.last_epoch;
    ensure!(
        new_last_epoch > old_last_epoch,
        "new last epoch {new_last_epoch} must be after completed epoch {old_last_epoch}"
    );
    ensure!(
        inputs
            .get(usize::try_from(old_last_epoch - first_epoch)?)
            .is_some_and(|input| input.epoch == old_last_epoch),
        "completed extraction range is not a prefix of the admitted source"
    );

    let discoveries_root = config.output.join(DISCOVERY_SHARDS_DIR);
    let shard_root = config.output.join(EPOCH_SHARDS_DIR);
    let discovery_layout = discover_resume_shard_layout(&discoveries_root, first_epoch)?;
    let raw_layout = discover_resume_shard_layout(&shard_root, first_epoch)?;
    ensure!(
        discovery_layout.partial.is_none() && raw_layout.partial.is_none(),
        "prepare-extension input contains a partial epoch artifact"
    );
    let old_epoch_count = usize::try_from(old_last_epoch - first_epoch + 1)?;
    ensure!(
        discovery_layout.complete.len() == old_epoch_count
            && raw_layout.complete.len() == old_epoch_count,
        "completed extraction does not contain one discovery and raw shard per old epoch"
    );

    let old_identity = ResumeIdentity {
        dump_schema_version: DUMP_SCHEMA_VERSION,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        first_epoch,
        last_epoch: old_last_epoch,
        cluster_id: inputs[0].manifest.cluster_id.clone(),
        slots_per_epoch: inputs[0].manifest.slots_per_epoch,
        source_binding: dump_source_binding(config),
        extraction_mode: ResumeExtractionMode::SingleReadBatches,
        single_read_match_hints: false,
    };
    let old_checkpoint = load_resume_checkpoint(&config.output, &old_identity)?
        .context("completed extraction has no authenticated checkpoint")?;
    ensure!(
        old_checkpoint.payload.stage == ResumeStage::Complete,
        "completed extraction checkpoint is not complete"
    );

    let discovery_bindings = old_checkpoint.payload.discovery_shards.clone();
    let mut global_accounts = DiscoveredAccountMap::new();
    for ((epoch, path), binding) in discovery_layout.complete.iter().zip(&discovery_bindings) {
        ensure!(
            *epoch == binding.epoch,
            "discovery checkpoint has an epoch gap"
        );
        let log = validate_checkpoint_discovery(
            config,
            *epoch,
            path,
            binding,
            old_identity.slots_per_epoch,
        )
        .with_context(|| format!("validate checkpoint-bound discovery epoch {epoch}"))?;
        merge_discovery_accounts(&mut global_accounts, &log);
    }

    let accounts_path = config.output.join(ACCOUNTS_FILE);
    let accounts = load_and_validate_frozen_accounts(
        &accounts_path,
        mint,
        config.mint_slot,
        first_epoch,
        old_last_epoch,
        &global_accounts,
    )?;
    let frozen = resume_frozen_binding(&accounts_path, &accounts)?;
    let mut raw_bindings = Vec::with_capacity(old_epoch_count);
    for ((epoch, path), expected) in raw_layout
        .complete
        .iter()
        .zip(&old_checkpoint.payload.raw_shards)
    {
        ensure!(*epoch == expected.epoch, "raw checkpoint has an epoch gap");
        let generation_digest = parse_checkpoint_digest(
            &expected.source_generation_digest,
            "source generation digest",
        )?;
        let binding = validate_epoch_shard_for_resume(
            *epoch,
            path,
            resume_target_binding(config, mint, anchor_signature),
            &dump_source_binding(config),
            generation_digest,
            old_identity.slots_per_epoch,
            &accounts.accounts,
            accounts.anchor_position,
        )
        .with_context(|| format!("validate checkpoint-bound raw epoch {epoch}"))?;
        ensure!(
            &binding == expected,
            "raw epoch {epoch} differs from its completed checkpoint"
        );
        raw_bindings.push(binding);
    }
    validate_raw_root_manifest(
        &manifest_path,
        config,
        first_epoch,
        old_last_epoch,
        &frozen,
        &raw_bindings,
    )?;

    old_checkpoint
        .payload
        .validate_artifacts(&discovery_bindings, Some(&frozen), &raw_bindings)?;

    let new_identity = ResumeIdentity {
        last_epoch: new_last_epoch,
        ..old_identity
    };
    let reopened = ResumeCheckpointPayload::new_single_read_batches(
        new_identity,
        Some(accounts.anchor_position),
        discovery_bindings,
        None,
        raw_bindings,
    )?;
    ensure!(
        reopened.stage == ResumeStage::Extraction,
        "extension checkpoint is unexpectedly complete"
    );

    let checkpoint_path = config.output.join(crate::resume::RESUME_CHECKPOINT_FILE);
    let checkpoint_backup = config.output.join(format!(
        "resume-checkpoint.completed-e{old_last_epoch}.json"
    ));
    let accounts_backup = config
        .output
        .join(format!("accounts.completed-e{old_last_epoch}.wincode"));
    let manifest_backup = config
        .output
        .join(format!("manifest.completed-e{old_last_epoch}.json"));
    for backup in [&checkpoint_backup, &accounts_backup, &manifest_backup] {
        ensure!(
            !backup.exists(),
            "extension backup already exists: {}",
            backup.display()
        );
    }
    ensure!(
        !config
            .output
            .join(crate::resume::RESUME_CHECKPOINT_PENDING_FILE)
            .exists(),
        "a pending resume checkpoint already exists"
    );

    stage_resume_checkpoint(&config.output, &reopened)?;
    fs::rename(&checkpoint_path, &checkpoint_backup)?;
    fs::rename(&accounts_path, &accounts_backup)?;
    fs::rename(&manifest_path, &manifest_backup)?;
    promote_pending_resume_checkpoint(&config.output, &reopened.identity)?;
    sync_directory(&config.output)?;
    eprintln!(
        "prepared single-read extension: epochs {first_epoch}-{old_last_epoch} validated; next epoch={}",
        old_last_epoch + 1
    );
    Ok(())
}

fn extract_epoch_shards_single_read_batches(config: &ExtractConfig) -> Result<()> {
    ensure!(
        !config.epoch_barrier,
        "--single-read-batches conflicts with --epoch-barrier"
    );
    ensure!(
        (1..=MAX_WORKERS).contains(&config.workers),
        "workers must be 1..={MAX_WORKERS}"
    );
    ensure!(
        !config.allow_indeterminate,
        "opaque transaction and metadata fallbacks cannot be written as standalone records"
    );
    validate_source_mode(config)?;

    let mint = parse_pubkey(&config.mint, "mint")?;
    let anchor_signature = parse_signature(&config.mint_signature)?;
    let inputs = discover_epochs(config)?;
    let first_epoch = inputs.first().context("no input epochs")?.epoch;
    let last_epoch = inputs.last().context("no input epochs")?.epoch;
    let epoch_count = inputs.len();
    let discoveries_root = config.output.join(DISCOVERY_SHARDS_DIR);
    let shard_root = config.output.join(EPOCH_SHARDS_DIR);
    let identity = ResumeIdentity {
        dump_schema_version: DUMP_SCHEMA_VERSION,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        first_epoch,
        last_epoch,
        cluster_id: inputs[0].manifest.cluster_id.clone(),
        slots_per_epoch: inputs[0].manifest.slots_per_epoch,
        source_binding: dump_source_binding(config),
        extraction_mode: ResumeExtractionMode::SingleReadBatches,
        single_read_match_hints: false,
    };
    authenticate_single_read_resume_before_recovery(config, &identity)?;
    let recovery_notes = prepare_single_read_extraction_directories(
        config,
        &discoveries_root,
        &shard_root,
        first_epoch,
    )?;
    let discovery_layout = discover_resume_shard_layout(&discoveries_root, first_epoch)?;
    let raw_layout = discover_resume_shard_layout(&shard_root, first_epoch)?;
    ensure!(
        discovery_layout.complete.len() == raw_layout.complete.len(),
        "single-read recovery did not produce paired epoch artifacts"
    );
    ensure!(
        discovery_layout
            .complete
            .iter()
            .zip(&raw_layout.complete)
            .all(|((left, _), (right, _))| left == right),
        "single-read discovery and raw epoch prefixes differ"
    );

    let mut committed_checkpoint = load_resume_checkpoint(&config.output, &identity)?;
    let pending_checkpoint = load_pending_resume_checkpoint(&config.output, &identity)?;
    if committed_checkpoint.is_none() && pending_checkpoint.is_none() {
        let initial = ResumeCheckpointPayload::new_single_read_batches(
            identity.clone(),
            None,
            Vec::new(),
            None,
            Vec::new(),
        )?;
        committed_checkpoint = Some(write_resume_checkpoint_atomic(&config.output, &initial)?);
    }
    let completed_epochs = discovery_layout.complete.len();
    let accounts_path = config.output.join(ACCOUNTS_FILE);
    let root_manifest_path = config.output.join(DUMP_MANIFEST_FILE);
    let accounts_exist = regular_file_exists(&accounts_path)?;
    let root_manifest_exists = regular_file_exists(&root_manifest_path)?;
    ensure!(
        !accounts_exist || completed_epochs == epoch_count,
        "frozen accounts exist before every single-read epoch is complete"
    );
    ensure!(
        !root_manifest_exists || accounts_exist,
        "single-read root manifest exists without frozen accounts"
    );

    let mut discovery_bindings = Vec::with_capacity(completed_epochs);
    let mut global_accounts = DiscoveredAccountMap::new();
    for (index, ((epoch, path), input)) in discovery_layout.complete.iter().zip(&inputs).enumerate()
    {
        ensure!(
            *epoch == input.epoch,
            "single-read discovery prefix has a gap"
        );
        let checkpoint_binding = pending_checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.payload.discovery_shards.get(index))
            .or_else(|| {
                committed_checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.payload.discovery_shards.get(index))
            });
        let (binding, log) = if let Some(expected) = checkpoint_binding {
            (
                expected.clone(),
                validate_checkpoint_discovery(
                    config,
                    *epoch,
                    path,
                    expected,
                    identity.slots_per_epoch,
                )
                .with_context(|| {
                    format!("validate checkpoint-bound single-read epoch {epoch} discovery")
                })?,
            )
        } else {
            validate_resume_discovery(config, input, path, None)
                .with_context(|| format!("validate single-read epoch {epoch} discovery"))?
        };
        merge_discovery_accounts(&mut global_accounts, &log);
        discovery_bindings.push(binding);
    }

    let checkpoint_anchor = pending_checkpoint
        .as_ref()
        .filter(|checkpoint| {
            checkpoint.payload.discovery_shards.len() == completed_epochs
                && checkpoint.payload.raw_shards.len() == completed_epochs
        })
        .and_then(|checkpoint| checkpoint.payload.anchor_position)
        .or_else(|| {
            committed_checkpoint
                .as_ref()
                .filter(|checkpoint| {
                    checkpoint.payload.discovery_shards.len() == completed_epochs
                        && checkpoint.payload.raw_shards.len() == completed_epochs
                })
                .and_then(|checkpoint| checkpoint.payload.anchor_position)
        });
    let mut discovered_accounts = if accounts_exist {
        Some(load_and_validate_frozen_accounts(
            &accounts_path,
            mint,
            config.mint_slot,
            first_epoch,
            last_epoch,
            &global_accounts,
        )?)
    } else {
        None
    };
    let mut anchor_position = discovered_accounts
        .as_ref()
        .map(|accounts| accounts.anchor_position)
        .or(checkpoint_anchor);
    ensure!(
        completed_epochs == 0 || anchor_position.is_some(),
        "completed single-read artifacts have no authenticated mint anchor"
    );

    let mut raw_bindings = Vec::with_capacity(completed_epochs);
    if let Some(anchor) = anchor_position {
        let prefix_accounts = DiscoveredAccountList {
            schema_version: DUMP_SCHEMA_VERSION,
            mint,
            anchor_position: anchor,
            accounts: global_accounts
                .iter()
                .map(|(raw_pubkey, first_creation)| DiscoveredAccount {
                    raw_pubkey: *raw_pubkey,
                    first_creation: *first_creation,
                })
                .collect(),
        };
        for (index, ((epoch, path), input)) in raw_layout.complete.iter().zip(&inputs).enumerate() {
            ensure!(*epoch == input.epoch, "single-read raw prefix has a gap");
            let checkpoint_binding = pending_checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.payload.raw_shards.get(index))
                .or_else(|| {
                    committed_checkpoint
                        .as_ref()
                        .and_then(|checkpoint| checkpoint.payload.raw_shards.get(index))
                });
            let binding = if let Some(expected) = checkpoint_binding {
                let generation_digest = parse_checkpoint_digest(
                    &expected.source_generation_digest,
                    "source generation digest",
                )?;
                let actual = validate_epoch_shard_for_resume(
                    *epoch,
                    path,
                    resume_target_binding(config, mint, anchor_signature),
                    &dump_source_binding(config),
                    generation_digest,
                    identity.slots_per_epoch,
                    &prefix_accounts.accounts,
                    prefix_accounts.anchor_position,
                )
                .with_context(|| {
                    format!("validate checkpoint-bound single-read epoch {epoch} raw shard")
                })?;
                ensure!(
                    &actual == expected,
                    "single-read raw epoch {epoch} differs from its checkpoint"
                );
                actual
            } else {
                validate_resume_shard(
                    config,
                    input,
                    path,
                    mint,
                    anchor_signature,
                    &prefix_accounts,
                    None,
                )
                .with_context(|| format!("validate single-read epoch {epoch} raw shard"))?
            };
            raw_bindings.push(binding);
        }
    }
    ensure!(
        raw_bindings.len() == completed_epochs,
        "single-read raw artifact validation is incomplete"
    );

    let frozen_binding = discovered_accounts
        .as_ref()
        .map(|accounts| resume_frozen_binding(&accounts_path, accounts))
        .transpose()?;
    let mut checkpoint_reconciled = false;
    if let Some(pending) = pending_checkpoint.as_ref()
        && pending
            .payload
            .validate_artifacts(&discovery_bindings, frozen_binding.as_ref(), &raw_bindings)
            .is_ok()
    {
        let promoted = promote_pending_resume_checkpoint(&config.output, &identity)?;
        ensure!(
            promoted.payload == pending.payload,
            "promoted single-read checkpoint differs from its authenticated pending value"
        );
        checkpoint_reconciled = true;
    }
    if !checkpoint_reconciled {
        if pending_checkpoint.is_some() {
            let _ = quarantine_pending_resume_checkpoint(&config.output)?;
        }
        match committed_checkpoint.as_ref() {
            Some(committed) => committed.payload.validate_artifacts(
                &discovery_bindings,
                frozen_binding.as_ref(),
                &raw_bindings,
            )?,
            None => ensure!(
                completed_epochs == 0 && frozen_binding.is_none(),
                "single-read artifacts exist without an authenticated checkpoint"
            ),
        }
    }

    if root_manifest_exists {
        ensure!(
            completed_epochs == epoch_count && discovered_accounts.is_some(),
            "complete single-read root has an incomplete epoch prefix"
        );
        validate_raw_root_manifest(
            &root_manifest_path,
            config,
            first_epoch,
            last_epoch,
            frozen_binding
                .as_ref()
                .context("complete single-read root has no frozen binding")?,
            &raw_bindings,
        )?;
        return Ok(());
    }

    let mut progress = ExtractionProgress::start_phase(
        first_epoch,
        last_epoch,
        completed_epochs,
        "single_read_batches",
    );
    for note in recovery_notes {
        progress.note(None, "quarantined", &note);
    }
    let mut anchor_count = u64::from(anchor_position.is_some());
    for input in inputs.iter().skip(completed_epochs) {
        let epoch_started = Instant::now();
        progress.epoch_start(input.epoch, global_accounts.len());
        let timer = progress.pass_start(input.epoch, "discover_then_copy_retained_batches");
        let discovery_partial = create_partial_shard_directory(&discoveries_root, input.epoch)?;
        let raw_partial = create_partial_shard_directory(&shard_root, input.epoch)?;
        let result = write_single_read_epoch(
            config,
            input,
            &raw_partial,
            mint,
            anchor_signature,
            &mut global_accounts,
            &mut anchor_position,
            &mut anchor_count,
        )?;
        progress.single_read_reader_stats(input.epoch, &result.reader);
        ensure!(
            result.reader.read_call_count == result.reader.batch_count
                && result.reader.decompression_count == result.reader.block_count
                && result.reader.stage_a_block_count == result.reader.block_count
                && result.reader.stage_b_block_count == result.reader.block_count
                && result.reader.borrowed_storage_blocks == result.reader.block_count
                && result.reader.owned_schema_fallback_blocks == 0,
            "single-read reader repeated or skipped a stage, or used an owned block fallback"
        );
        ensure!(
            anchor_count == 1,
            "mint signature occurs {anchor_count} times, expected exactly once"
        );

        let discovery_path = discovery_partial.join(CREATIONS_FILE);
        write_synced_bytes(
            &discovery_path,
            &wincode::config::serialize(
                &result.discovery.log,
                bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
            )?,
        )?;
        sync_directory(&discovery_partial)?;
        let discovery_validation_started = Instant::now();
        let (discovery_binding, validated_log) = validate_resume_discovery(
            config,
            input,
            &discovery_partial,
            Some(&result.discovery.log),
        )?;
        let discovery_validation_time = discovery_validation_started.elapsed();
        ensure!(
            validated_log == result.discovery.log,
            "validated single-read discovery differs from its in-memory result"
        );
        let prefix_accounts = DiscoveredAccountList {
            schema_version: DUMP_SCHEMA_VERSION,
            mint,
            anchor_position: anchor_position.context("single-read mint anchor is absent")?,
            accounts: global_accounts
                .iter()
                .map(|(raw_pubkey, first_creation)| DiscoveredAccount {
                    raw_pubkey: *raw_pubkey,
                    first_creation: *first_creation,
                })
                .collect(),
        };
        let raw_validation_started = Instant::now();
        let raw_binding = validate_resume_shard(
            config,
            input,
            &raw_partial,
            mint,
            anchor_signature,
            &prefix_accounts,
            Some(&result.account_ids),
        )?;
        let raw_validation_time = raw_validation_started.elapsed();
        progress.single_read_extractor_stats(
            input.epoch,
            &result.extractor,
            discovery_validation_time,
            raw_validation_time,
        );
        ensure!(
            result.shard.transactions == raw_binding.counters.transactions
                && result.shard.anchor_transactions == raw_binding.counters.anchor_transactions,
            "validated single-read shard counters differ from the epoch writer"
        );

        let mut next_discovery = discovery_bindings.clone();
        next_discovery.push(discovery_binding.clone());
        let mut next_raw = raw_bindings.clone();
        next_raw.push(raw_binding.clone());
        let next_checkpoint = ResumeCheckpointPayload::new_single_read_batches(
            identity.clone(),
            anchor_position,
            next_discovery.clone(),
            None,
            next_raw.clone(),
        )?;
        stage_resume_checkpoint(&config.output, &next_checkpoint)?;
        commit_partial_shard(&discoveries_root, input.epoch)?;
        commit_partial_shard(&shard_root, input.epoch)?;
        let promoted = promote_pending_resume_checkpoint(&config.output, &identity)?;
        ensure!(
            promoted.payload == next_checkpoint,
            "promoted checkpoint differs from the staged single-read epoch checkpoint"
        );
        discovery_bindings = next_discovery;
        raw_bindings = next_raw;

        let metrics = PassMetrics {
            blocks: result.discovery.stats.blocks,
            transactions: result.discovery.stats.transactions,
            selected_transactions: result.shard.transactions,
            tracked_accounts: global_accounts.len(),
            compressed_bytes: result.shard.compressed_bytes,
            output_transactions: result.shard.transactions,
        };
        let elapsed = timer.complete(metrics);
        progress.epoch_complete(input.epoch, epoch_started.elapsed().max(elapsed), metrics);
    }
    ensure!(
        discovery_bindings.len() == epoch_count && raw_bindings.len() == epoch_count,
        "single-read extraction did not commit every epoch"
    );
    ensure!(
        anchor_count == 1,
        "mint signature occurs {anchor_count} times, expected exactly once"
    );

    if discovered_accounts.is_none() {
        let accounts = DiscoveredAccountList {
            schema_version: DUMP_SCHEMA_VERSION,
            mint,
            anchor_position: anchor_position.context("single-read mint anchor is absent")?,
            accounts: global_accounts
                .iter()
                .map(|(raw_pubkey, first_creation)| DiscoveredAccount {
                    raw_pubkey: *raw_pubkey,
                    first_creation: *first_creation,
                })
                .collect(),
        };
        validate_frozen_account_structure(
            &accounts,
            mint,
            config.mint_slot,
            first_epoch,
            last_epoch,
            &global_accounts,
        )?;
        let bytes = wincode::config::serialize(
            &accounts,
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        )?;
        let partial = create_partial_artifact_file(&config.output, ACCOUNTS_FILE, &bytes)?;
        ensure!(
            load_and_validate_frozen_accounts(
                &partial,
                mint,
                config.mint_slot,
                first_epoch,
                last_epoch,
                &global_accounts,
            )? == accounts,
            "partial single-read frozen accounts differ from the in-memory list"
        );
        let frozen = resume_frozen_binding(&partial, &accounts)?;
        let complete_checkpoint = ResumeCheckpointPayload::new_single_read_batches(
            identity.clone(),
            anchor_position,
            discovery_bindings.clone(),
            Some(frozen),
            raw_bindings.clone(),
        )?;
        stage_resume_checkpoint(&config.output, &complete_checkpoint)?;
        commit_partial_artifact_file(&config.output, ACCOUNTS_FILE)?;
        let promoted = promote_pending_resume_checkpoint(&config.output, &identity)?;
        ensure!(
            promoted.payload == complete_checkpoint,
            "promoted single-read completion checkpoint differs"
        );
        discovered_accounts = Some(accounts);
    }
    let discovered_accounts =
        discovered_accounts.context("single-read frozen accounts are absent")?;
    let frozen_binding = resume_frozen_binding(&accounts_path, &discovered_accounts)?;
    let cumulative = raw_bindings
        .iter()
        .try_fold(crate::resume::ResumeCounters::default(), |sum, binding| {
            sum.checked_add(binding.counters)
        })?;
    ensure!(
        cumulative.anchor_transactions == 1,
        "single-read dump emitted the mint anchor {} times, expected exactly once",
        cumulative.anchor_transactions
    );
    let root_manifest = DumpManifest {
        schema_version: DUMP_SCHEMA_VERSION,
        artifact_kind: DumpArtifactKind::RawExtractionRoot,
        complete: true,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        source_binding: dump_source_binding(config),
        first_epoch,
        last_epoch,
        transactions: cumulative.transactions,
        signatures: None,
        pubkeys: None,
        transaction_stream: EPOCH_SHARDS_DIR.to_owned(),
        transaction_stream_sha256: None,
        account_id_log: None,
        account_id_log_sha256: None,
        discovered_accounts: Some(ACCOUNTS_FILE.to_owned()),
        discovered_accounts_sha256: Some(frozen_binding.accounts_sha256.clone()),
        discovered_account_count: Some(discovered_accounts.accounts.len() as u64),
        signature_stream: None,
        signature_stream_sha256: None,
        pubkey_registry: None,
        pubkey_registry_sha256: None,
        registry_maps: None,
    };
    create_partial_artifact_file(
        &config.output,
        DUMP_MANIFEST_FILE,
        &serde_json::to_vec_pretty(&root_manifest)?,
    )?;
    commit_partial_artifact_file(&config.output, DUMP_MANIFEST_FILE)?;
    validate_raw_root_manifest(
        &root_manifest_path,
        config,
        first_epoch,
        last_epoch,
        &frozen_binding,
        &raw_bindings,
    )?;
    progress.run_complete();
    Ok(())
}

fn prepare_single_read_extraction_directories(
    config: &ExtractConfig,
    discoveries_root: &Path,
    shard_root: &Path,
    first_epoch: u64,
) -> Result<Vec<String>> {
    if !config.resume {
        return prepare_two_pass_extraction_directories(
            config,
            discoveries_root,
            shard_root,
            first_epoch,
        );
    }

    match fs::symlink_metadata(&config.output) {
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir(),
            "resume output {} is not a direct directory",
            config.output.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(&config.output)
                .with_context(|| format!("create resume output {}", config.output.display()))?;
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect resume output {}", config.output.display()));
        }
    }
    ensure_or_create_direct_directory(&config.output, discoveries_root, "discovery shard root")?;
    ensure_or_create_direct_directory(&config.output, shard_root, "raw shard root")?;

    let mut notes = Vec::new();
    let mut rolled_back_epoch = false;
    for file_name in [ACCOUNTS_FILE, DUMP_MANIFEST_FILE] {
        if let Some(path) = quarantine_partial_artifact_file(&config.output, file_name)? {
            notes.push(format!(
                "preserved partial {file_name} as {}",
                path.display()
            ));
            rolled_back_epoch = true;
        }
    }
    for (label, root) in [("discovery", discoveries_root), ("raw", shard_root)] {
        let layout = discover_resume_shard_layout(root, first_epoch)?;
        if let Some((epoch, _)) = layout.partial {
            let path = quarantine_partial_shard(root, epoch)?
                .context("partial single-read artifact disappeared before quarantine")?;
            notes.push(format!(
                "preserved partial {label} epoch {epoch} as {}",
                path.display()
            ));
            rolled_back_epoch = true;
        }
    }

    loop {
        let discovery = discover_resume_shard_layout(discoveries_root, first_epoch)?;
        let raw = discover_resume_shard_layout(shard_root, first_epoch)?;
        if discovery.complete.len() == raw.complete.len() {
            break;
        }
        let (label, root, epoch) = if discovery.complete.len() > raw.complete.len() {
            (
                "discovery",
                discoveries_root,
                discovery
                    .complete
                    .last()
                    .context("longer discovery prefix is empty")?
                    .0,
            )
        } else {
            (
                "raw",
                shard_root,
                raw.complete.last().context("longer raw prefix is empty")?.0,
            )
        };
        let path = quarantine_complete_shard(root, epoch)?
            .context("unpaired complete single-read artifact disappeared")?;
        notes.push(format!(
            "preserved unpaired complete {label} epoch {epoch} as {}",
            path.display()
        ));
        rolled_back_epoch = true;
    }
    if rolled_back_epoch && let Some(path) = quarantine_pending_resume_checkpoint(&config.output)? {
        notes.push(format!(
            "preserved interrupted pending checkpoint as {}",
            path.display()
        ));
    }
    Ok(notes)
}

fn authenticate_single_read_resume_before_recovery(
    config: &ExtractConfig,
    identity: &ResumeIdentity,
) -> Result<()> {
    if !config.resume {
        return Ok(());
    }
    match fs::symlink_metadata(&config.output) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir(),
            "resume output {} is not a direct directory",
            config.output.display()
        ),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect resume output {}", config.output.display()));
        }
    }

    let committed = load_resume_checkpoint(&config.output, identity)?;
    if committed.is_some() {
        if load_pending_resume_checkpoint(&config.output, identity).is_err() {
            quarantine_pending_resume_checkpoint(&config.output)?;
        }
        quarantine_pending_checkpoint_staging(&config.output)?;
        return Ok(());
    }
    let pending = load_pending_resume_checkpoint(&config.output, identity)?;
    if pending.is_some() {
        quarantine_pending_checkpoint_staging(&config.output)?;
        return Ok(());
    }

    for entry in fs::read_dir(&config.output)? {
        let entry = entry?;
        if entry.path() == pending_checkpoint_staging_path(&config.output) {
            ensure!(
                entry.file_type()?.is_file(),
                "staged pending checkpoint is not a regular file"
            );
            continue;
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("resume output contains a non-UTF-8 name"))?;
        ensure!(
            name == DISCOVERY_SHARDS_DIR || name == EPOCH_SHARDS_DIR,
            "resume output contains artifacts but no authenticated single-read checkpoint"
        );
        ensure!(
            entry.file_type()?.is_dir(),
            "unauthenticated resume lane is not a direct directory"
        );
        ensure!(
            fs::read_dir(entry.path())?.next().is_none(),
            "resume output contains artifacts but no authenticated single-read checkpoint"
        );
    }
    Ok(())
}

fn extract_epoch_shards_epoch_barrier(config: &ExtractConfig) -> Result<()> {
    ensure!(
        !config.resume,
        "epoch-barrier mode does not support --resume"
    );
    ensure!(
        (1..=MAX_WORKERS).contains(&config.workers),
        "workers must be 1..={MAX_WORKERS}"
    );
    ensure!(
        !config.allow_indeterminate,
        "opaque transaction and metadata fallbacks cannot be written as standalone records"
    );

    validate_source_mode(config)?;
    let mint = parse_pubkey(&config.mint, "mint")?;
    let anchor_signature = parse_signature(&config.mint_signature)?;
    let inputs = discover_epochs(config)?;
    let first_epoch = inputs.first().context("no input epochs")?.epoch;
    let last_epoch = inputs.last().context("no input epochs")?.epoch;

    let discoveries_root = config.output.join(DISCOVERY_SHARDS_DIR);
    let shard_root = config.output.join(EPOCH_SHARDS_DIR);
    let _notes = prepare_two_pass_extraction_directories(
        config,
        &discoveries_root,
        &shard_root,
        first_epoch,
    )?;

    let mut discovery_progress =
        ExtractionProgress::start_phase(first_epoch, last_epoch, inputs.len(), "account_discovery");
    let mut raw_progress =
        ExtractionProgress::start_phase(first_epoch, last_epoch, inputs.len(), "raw_copy");

    let anchor_input = inputs
        .iter()
        .find(|input| {
            (input.manifest.epoch_start_slot()..=input.manifest.epoch_end_slot())
                .contains(&config.mint_slot)
        })
        .context("no input epoch contains the mint slot")?;
    let anchor_position = locate_anchor_transaction(config, anchor_input, anchor_signature)?;

    let mut discovery_bindings = Vec::new();
    let mut raw_bindings = Vec::new();
    let mut global_accounts = BTreeMap::<[u8; 32], SourceInstructionCoordinate>::new();
    let mut discovered_accounts = DiscoveredAccountList {
        schema_version: DUMP_SCHEMA_VERSION,
        mint,
        anchor_position,
        accounts: Vec::new(),
    };
    let root_manifest_path = config.output.join(DUMP_MANIFEST_FILE);

    let mut anchor_count = 0u64;
    for input in inputs.iter() {
        let epoch_started = Instant::now();
        discovery_progress.epoch_start(input.epoch, global_accounts.len());
        let timer = discovery_progress.pass_start(input.epoch, "discover_accounts");
        let discovery =
            discover_epoch_creations(config, input, mint, anchor_signature, &mut anchor_count)?;
        if let Some(position) = discovery.anchor_position {
            ensure!(
                position == anchor_position,
                "mint anchor changed in epoch {}: expected {anchor_position:?}, found {position:?}",
                input.epoch
            );
        }
        ensure!(
            anchor_count <= 1,
            "more than one mint-anchor signature was found"
        );
        merge_discovery_accounts(&mut global_accounts, &discovery.log);

        let partial = create_partial_shard_directory(&discoveries_root, input.epoch)?;
        let discovery_path = partial.join(CREATIONS_FILE);
        let discovery_bytes = wincode::config::serialize(
            &discovery.log,
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        )?;
        write_synced_bytes(&discovery_path, &discovery_bytes)?;
        sync_directory(&partial)?;
        let (binding, validated_log) = validate_resume_discovery(config, input, &partial, None)?;
        ensure!(
            validated_log == discovery.log,
            "validated discovery artifact differs from the in-memory epoch result"
        );
        commit_partial_shard(&discoveries_root, input.epoch)?;
        discovery_bindings.push(binding);

        discovered_accounts.accounts = global_accounts
            .iter()
            .map(|(raw_pubkey, first_creation)| DiscoveredAccount {
                raw_pubkey: *raw_pubkey,
                first_creation: *first_creation,
            })
            .collect();
        validate_frozen_account_structure(
            &discovered_accounts,
            mint,
            config.mint_slot,
            first_epoch,
            last_epoch,
            &global_accounts,
        )?;

        let discovery_metrics = PassMetrics {
            blocks: discovery.stats.blocks,
            transactions: discovery.stats.transactions,
            selected_transactions: 0,
            tracked_accounts: global_accounts.len(),
            compressed_bytes: discovery.stats.compressed_bytes,
            output_transactions: 0,
        };
        let elapsed = timer.complete(discovery_metrics);
        discovery_progress.epoch_complete(
            input.epoch,
            epoch_started.elapsed().max(elapsed),
            discovery_metrics,
        );

        let raw_started = Instant::now();
        raw_progress.epoch_start(input.epoch, discovered_accounts.accounts.len());
        let raw_timer = raw_progress.pass_start(input.epoch, "copy_raw_transactions");
        let partial = create_partial_shard_directory(&shard_root, input.epoch)?;
        let shard = write_frozen_epoch_shard(
            config,
            input,
            &partial,
            mint,
            anchor_signature,
            &discovered_accounts.accounts,
            discovered_accounts.anchor_position,
        )?;
        let binding = validate_resume_shard(
            config,
            input,
            &partial,
            mint,
            anchor_signature,
            &discovered_accounts,
            None,
        )?;
        ensure!(
            shard.transactions == binding.counters.transactions
                && shard.anchor_transactions == binding.counters.anchor_transactions,
            "validated raw shard counters differ from the epoch writer"
        );
        commit_partial_shard(&shard_root, input.epoch)?;
        raw_bindings.push(binding.clone());

        let raw_metrics = PassMetrics {
            blocks: binding.counters.blocks_scanned,
            transactions: binding.counters.transactions_scanned,
            selected_transactions: binding.counters.transactions,
            tracked_accounts: discovered_accounts.accounts.len(),
            compressed_bytes: shard.compressed_bytes,
            output_transactions: binding.counters.transactions,
        };
        let elapsed = raw_timer.complete(raw_metrics);
        raw_progress.epoch_complete(input.epoch, raw_started.elapsed().max(elapsed), raw_metrics);
    }

    ensure!(
        anchor_count == 1,
        "mint anchor occurs {anchor_count} times, expected exactly once"
    );
    ensure!(
        discovery_bindings.len() == inputs.len() && raw_bindings.len() == inputs.len(),
        "incomplete barrier extraction output"
    );

    let accounts_path = config.output.join(ACCOUNTS_FILE);
    {
        let bytes = wincode::config::serialize(
            &discovered_accounts,
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        )?;
        let partial = create_partial_artifact_file(&config.output, ACCOUNTS_FILE, &bytes)?;
        ensure!(
            load_and_validate_frozen_accounts(
                &partial,
                mint,
                config.mint_slot,
                first_epoch,
                last_epoch,
                &global_accounts,
            )? == discovered_accounts,
            "partial frozen account artifact differs from its source value"
        );
        commit_partial_artifact_file(&config.output, ACCOUNTS_FILE)?;
    }
    let frozen_binding = resume_frozen_binding(&accounts_path, &discovered_accounts)?;

    let cumulative = raw_bindings
        .iter()
        .try_fold(crate::resume::ResumeCounters::default(), |sum, binding| {
            sum.checked_add(binding.counters)
        })?;
    ensure!(
        cumulative.anchor_transactions == 1,
        "Pass B emitted the mint anchor {} times, expected exactly once",
        cumulative.anchor_transactions
    );

    let root_manifest = DumpManifest {
        schema_version: DUMP_SCHEMA_VERSION,
        artifact_kind: DumpArtifactKind::RawExtractionRoot,
        complete: true,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        source_binding: dump_source_binding(config),
        first_epoch,
        last_epoch,
        transactions: cumulative.transactions,
        signatures: None,
        pubkeys: None,
        transaction_stream: EPOCH_SHARDS_DIR.to_owned(),
        transaction_stream_sha256: None,
        account_id_log: None,
        account_id_log_sha256: None,
        discovered_accounts: Some(ACCOUNTS_FILE.to_owned()),
        discovered_account_count: Some(discovered_accounts.accounts.len() as u64),
        discovered_accounts_sha256: Some(frozen_binding.accounts_sha256.clone()),
        signature_stream: None,
        signature_stream_sha256: None,
        pubkey_registry: None,
        pubkey_registry_sha256: None,
        registry_maps: None,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&root_manifest)?;
    create_partial_artifact_file(&config.output, DUMP_MANIFEST_FILE, &manifest_bytes)?;
    commit_partial_artifact_file(&config.output, DUMP_MANIFEST_FILE)?;

    sync_directory(&config.output)?;

    validate_raw_root_manifest(
        &root_manifest_path,
        config,
        first_epoch,
        last_epoch,
        &frozen_binding,
        &raw_bindings,
    )?;

    discovery_progress.run_complete();
    raw_progress.run_complete();
    Ok(())
}

/// Measure the exact first-pass selector on a bounded range of one trusted
/// local epoch. No output files are created and no dump trust rule is changed.
#[cfg(any())]
pub fn probe_epoch_speed_stateful_removed(config: &ProbeConfig) -> Result<ProbeReport> {
    ensure!(
        (1..=MAX_WORKERS).contains(&config.workers),
        "workers must be 1..={MAX_WORKERS}"
    );
    ensure!(
        config.slots_per_epoch != 0,
        "slots per epoch must not be zero"
    );
    ensure!(config.max_blocks != 0, "max blocks must not be zero");
    let epoch_start_slot = config
        .epoch
        .checked_mul(config.slots_per_epoch)
        .context("epoch start slot overflow")?;
    let epoch_end_slot = epoch_start_slot
        .checked_add(config.slots_per_epoch - 1)
        .context("epoch end slot overflow")?;
    ensure!(
        (epoch_start_slot..=epoch_end_slot).contains(&config.start_slot),
        "start slot {} is outside epoch {} slot range {epoch_start_slot}..={epoch_end_slot}",
        config.start_slot,
        config.epoch
    );

    let mint = parse_pubkey(&config.mint, "mint")?;
    let anchor_signature = parse_signature(&config.mint_signature)?;
    let source = PinnedLocalRangeSource::new(&config.epoch_path);
    let reader = ArchiveReader::open_trusted_with_additional_files_and_metadata_profile(
        source.clone(),
        TrustedGenerationIdentity {
            cluster_id: config.cluster_id.clone(),
            epoch: config.epoch,
            generation_id: "read-only-token-transaction-probe".to_owned(),
            slots_per_epoch: config.slots_per_epoch,
            wire_profile: config.wire_profile,
        },
        &[
            blockzilla_read_sdk::manifest::SIGNATURES_FILE,
            blockzilla_read_sdk::manifest::REGISTRY_INDEX_FILE,
        ],
        &[],
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )
    .with_context(|| format!("open trusted Compact V2 epoch {}", config.epoch))?;
    ensure!(
        reader.signatures_available(),
        "epoch {} has no signatures file",
        config.epoch
    );
    let range = exact_probe_row_range(
        &reader.index().rows,
        config.start_slot,
        config.expected_start_row,
        config.max_blocks,
    )?;
    let first_slot = reader.index().rows[range.start].slot;
    let last_slot = reader.index().rows[range.end - 1].slot;
    let registry =
        VerifiedEpochRegistry::open(source.clone(), reader.manifest(), reader.registry_entries())?;
    let projector = reader.message_projector();
    let registry_entries = reader.registry_entries();
    let generation_digest = reader.binding().generation_digest;
    let tracker = TokenAccountTracker::new(mint);
    let mut epoch_tracker = EpochLocalTracker::compile(&tracker, &registry)?;
    let mut anchor_count = 0u64;
    let mut transactions = 0u64;
    let mut selected_transactions = 0u64;
    let mut owned_block_fallbacks = 0u64;

    let started = Instant::now();
    let ordered_stats = reader.process_borrowed_blocks_parallel_ordered(
        range.clone(),
        ordered_config(config.workers, true),
        |_| (),
        |_, _, block| {
            project_block_facts(
                projector,
                registry_entries,
                generation_digest,
                reader_id,
                block,
            )
            .map_err(|error| invalid_block_error(error.slot, error.message))
        },
        |_, _, block| {
            transactions = transactions
                .checked_add(block.transactions.len() as u64)
                .ok_or_else(|| {
                    invalid_block_error(block.slot, "transaction scan count overflow".to_owned())
                })?;
            owned_block_fallbacks = owned_block_fallbacks
                .checked_add(u64::from(block.owned_fallback))
                .ok_or_else(|| {
                    invalid_block_error(block.slot, "owned fallback count overflow".to_owned())
                })?;
            select_projected_block(
                &reader,
                &mut epoch_tracker,
                &registry,
                config.start_slot,
                anchor_signature,
                &mut anchor_count,
                block,
                |_| {
                    selected_transactions =
                        selected_transactions.checked_add(1).ok_or_else(|| {
                            invalid_block_error(
                                config.start_slot,
                                "selected transaction count overflow".to_owned(),
                            )
                        })?;
                    Ok(())
                },
            )
        },
    )?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    ensure!(
        anchor_count == 1,
        "mint signature occurs {anchor_count} times at slot {}, expected exactly once",
        config.start_slot
    );
    source
        .verify_unchanged()
        .with_context(|| format!("verify epoch {} did not change", config.epoch))?;

    let seconds_for_rate = if elapsed_seconds == 0.0 {
        None
    } else {
        Some(elapsed_seconds)
    };
    let blocks_per_second = seconds_for_rate
        .map(|seconds| ordered_stats.block_count as f64 / seconds)
        .unwrap_or(0.0);
    let transactions_per_second = seconds_for_rate
        .map(|seconds| transactions as f64 / seconds)
        .unwrap_or(0.0);
    let compressed_mib_per_second = seconds_for_rate
        .map(|seconds| ordered_stats.compressed_bytes as f64 / MIB / seconds)
        .unwrap_or(0.0);

    Ok(ProbeReport {
        schema_version: 1,
        kind: "blockzilla-token-transaction-probe",
        epoch_path: config.epoch_path.clone(),
        epoch: config.epoch,
        wire_profile: wire_profile_name(config.wire_profile),
        workers: config.workers,
        requested_start_slot: config.start_slot,
        start_row: range.start,
        end_row_exclusive: range.end,
        first_slot,
        last_slot,
        blocks: ordered_stats.block_count,
        transactions,
        selected_transactions,
        tracked_accounts: epoch_tracker.tracked_account_count(),
        owned_block_fallbacks,
        compressed_bytes: ordered_stats.compressed_bytes,
        elapsed_seconds,
        blocks_per_second,
        transactions_per_second,
        compressed_mib_per_second,
        reader: probe_reader_stats(ordered_stats),
    })
}

pub fn probe_epoch_speed(config: &ProbeConfig) -> Result<ProbeReport> {
    ensure!(
        (1..=MAX_WORKERS).contains(&config.workers),
        "workers must be 1..={MAX_WORKERS}"
    );
    ensure!(
        config.slots_per_epoch != 0,
        "slots per epoch must not be zero"
    );
    ensure!(config.max_blocks != 0, "max blocks must not be zero");
    let mint = parse_pubkey(&config.mint, "mint")?;
    let anchor_signature = parse_signature(&config.mint_signature)?;
    let source = PinnedLocalRangeSource::new(&config.epoch_path);
    let reader = ArchiveReader::open_trusted_with_additional_files_and_metadata_profile(
        source.clone(),
        TrustedGenerationIdentity {
            cluster_id: config.cluster_id.clone(),
            epoch: config.epoch,
            generation_id: "read-only-token-creation-probe".to_owned(),
            slots_per_epoch: config.slots_per_epoch,
            wire_profile: config.wire_profile,
        },
        &[
            blockzilla_read_sdk::manifest::SIGNATURES_FILE,
            blockzilla_read_sdk::manifest::REGISTRY_INDEX_FILE,
        ],
        &[],
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )?;
    let range = exact_probe_row_range(
        &reader.index().rows,
        config.start_slot,
        config.expected_start_row,
        config.max_blocks,
    )?;
    let first_slot = reader.index().rows[range.start].slot;
    let last_slot = reader.index().rows[range.end - 1].slot;
    let registry =
        VerifiedEpochRegistry::open(source.clone(), reader.manifest(), reader.registry_entries())?;
    let matcher = DiscoveryMatcher::build(mint, &registry)?;
    let projector = reader.message_projector();
    let generation_digest = reader.binding().generation_digest;
    let mut transactions = 0u64;
    let mut owned_block_fallbacks = 0u64;
    let mut anchor_count = 0u64;
    let mut creation_transactions = BTreeSet::new();
    let mut accounts = BTreeSet::new();
    let started = Instant::now();
    let ordered_stats = reader.process_borrowed_blocks_parallel_ordered(
        range.clone(),
        ordered_config(config.workers, true),
        |_| DiscoveryScratch::new(),
        |scratch, _, block| {
            project_creation_discovery_block(
                scratch,
                config.epoch,
                config.start_slot,
                matcher,
                projector,
                reader.registry_entries(),
                generation_digest,
                reader.reader_id(),
                block,
            )
            .map_err(|error| invalid_block_error(error.slot, error.message))
        },
        |_, block| {
            transactions = transactions
                .checked_add(block.transactions_scanned)
                .ok_or_else(|| {
                    invalid_block_error(block.slot, "transaction count overflow".to_owned())
                })?;
            owned_block_fallbacks += u64::from(block.owned_fallback);
            for (tx_index, reference) in block.first_signatures {
                let signature = reader.read_transaction_signatures(SignatureReference {
                    count: 1,
                    ..reference
                })?;
                anchor_count += u64::from(signature.first() == Some(&anchor_signature));
                let _ = tx_index;
            }
            for creation in block.creations {
                let raw = registry
                    .resolve_verified(creation.source_reference)
                    .map_err(|error| invalid_block_error(block.slot, error.to_string()))?;
                accounts.insert(raw);
                creation_transactions.insert((
                    creation.coordinate.slot,
                    creation.coordinate.source_block_id,
                    creation.coordinate.tx_index,
                ));
            }
            Ok(())
        },
    )?;
    ensure!(
        anchor_count == 1,
        "mint signature occurs {anchor_count} times at slot {}, expected exactly once",
        config.start_slot
    );
    source.verify_unchanged()?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let seconds = elapsed_seconds.max(f64::MIN_POSITIVE);
    Ok(ProbeReport {
        schema_version: 2,
        kind: "blockzilla-token-creation-probe",
        epoch_path: config.epoch_path.clone(),
        epoch: config.epoch,
        wire_profile: wire_profile_name(config.wire_profile),
        workers: config.workers,
        requested_start_slot: config.start_slot,
        start_row: range.start,
        end_row_exclusive: range.end,
        first_slot,
        last_slot,
        blocks: ordered_stats.block_count,
        transactions,
        selected_transactions: creation_transactions.len() as u64,
        tracked_accounts: accounts.len(),
        owned_block_fallbacks,
        compressed_bytes: ordered_stats.compressed_bytes,
        elapsed_seconds,
        blocks_per_second: ordered_stats.block_count as f64 / seconds,
        transactions_per_second: transactions as f64 / seconds,
        compressed_mib_per_second: ordered_stats.compressed_bytes as f64 / MIB / seconds,
        reader: probe_reader_stats(ordered_stats),
    })
}

#[cfg(any())]
fn discover_epoch_selection(
    config: &ExtractConfig,
    input: EpochInput,
    tracker: &mut TokenAccountTracker,
    anchor_signature: [u8; 64],
    anchor_count: &mut u64,
) -> Result<EpochSelection> {
    let (source, reader) = open_epoch(&input, config)?;
    let registry =
        VerifiedEpochRegistry::open(source.clone(), reader.manifest(), reader.registry_entries())?;
    let mut epoch_tracker = EpochLocalTracker::compile(tracker, &registry)?;
    let projector = reader.message_projector();
    let registry_entries = reader.registry_entries();
    let generation_digest = reader.binding().generation_digest;
    let first_slot = config.mint_slot.max(input.manifest.epoch_start_slot());
    let range = epoch_row_range(&reader, first_slot);
    let mut selected = BTreeMap::<usize, BTreeSet<u32>>::new();
    let mut scan_stats = EpochScanStats::default();

    let ordered_stats = reader.process_borrowed_blocks_parallel_ordered(
        range,
        ordered_config(config.workers, true),
        |_| (),
        |_, _, block| {
            project_block_facts(
                projector,
                registry_entries,
                generation_digest,
                reader_id,
                block,
            )
            .map_err(|error| invalid_block_error(error.slot, error.message))
        },
        |row_number, block| {
            scan_stats.transactions = scan_stats
                .transactions
                .checked_add(block.transactions.len() as u64)
                .ok_or_else(|| {
                    invalid_block_error(block.slot, "transaction scan count overflow".to_owned())
                })?;
            scan_stats.owned_block_fallbacks += u64::from(block.owned_fallback);
            select_projected_block(
                &reader,
                &mut epoch_tracker,
                &registry,
                config.mint_slot,
                anchor_signature,
                anchor_count,
                block,
                |tx_index| {
                    selected.entry(row_number).or_default().insert(tx_index);
                    Ok(())
                },
            )
        },
    )?;
    scan_stats.blocks = ordered_stats.block_count;
    scan_stats.compressed_bytes = ordered_stats.compressed_bytes;
    let tracker_transition = epoch_tracker.finish(input.epoch, generation_digest, &registry)?;
    tracker.replace_tracked_accounts(tracker_transition.entries.iter().filter_map(|entry| {
        (entry.role == TrackerTransitionRole::TokenAccount && entry.active_at_epoch_end)
            .then_some(entry.raw_pubkey)
    }));
    source
        .verify_unchanged()
        .with_context(|| format!("verify epoch {} did not change", input.epoch))?;
    Ok(EpochSelection {
        input,
        selected,
        tracker_transition,
        stats: scan_stats,
    })
}

#[cfg(any())]
fn select_projected_block<S: RangeSource>(
    reader: &ArchiveReader<S>,
    tracker: &mut EpochLocalTracker,
    registry: &VerifiedEpochRegistry,
    anchor_slot: u64,
    anchor_signature: [u8; 64],
    anchor_count: &mut u64,
    block: ProjectedBlockFacts,
    mut on_selected: impl FnMut(u32) -> std::result::Result<(), ReadError>,
) -> std::result::Result<(), ReadError> {
    for transaction in block.transactions {
        let mut keep = tracker
            .select(&transaction.facts, registry)
            .map_err(|error| invalid_block_error(block.slot, error.to_string()))?;
        if block.slot == anchor_slot {
            let first_signature = reader
                .read_transaction_signatures(SignatureReference {
                    count: 1,
                    ..transaction.signatures
                })
                .map_err(|error| {
                    invalid_block_error(
                        block.slot,
                        format!(
                            "transaction {} signature read: {error}",
                            transaction.tx_index
                        ),
                    )
                })?;
            let is_anchor = first_signature.first() == Some(&anchor_signature);
            *anchor_count = anchor_count
                .checked_add(u64::from(is_anchor))
                .ok_or_else(|| {
                    invalid_block_error(block.slot, "anchor occurrence count overflow".to_owned())
                })?;
            keep |= is_anchor;
        }
        if keep {
            on_selected(transaction.tx_index)?;
        }
    }
    Ok(())
}

#[cfg(any())]
fn write_epoch_shard(
    config: &ExtractConfig,
    shard_path: &Path,
    selection: &EpochSelection,
    mint: [u8; 32],
    anchor_signature: [u8; 64],
) -> Result<ShardSummary> {
    let (source, reader) = open_epoch(&selection.input, config)?;
    let generation_digest = reader.binding().generation_digest;
    let source_wire_profile = dump_wire_profile(reader.message_projector().wire_profile());
    let first_slot = config
        .mint_slot
        .max(selection.input.manifest.epoch_start_slot());
    let range = epoch_row_range(&reader, first_slot);

    let stream_path = shard_path.join(TRANSACTIONS_FILE);
    let stream_file =
        File::create(&stream_path).with_context(|| format!("create {}", stream_path.display()))?;
    let mut framed = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(8 << 20, stream_file));
    let mut scratch = Vec::with_capacity(2 << 20);
    let header = TokenTransactionDumpRecord::Header(TokenTransactionDumpHeader {
        schema_version: DUMP_SCHEMA_VERSION,
        stream_kind: DumpStreamKind::RawEpochShard,
        mint,
        mint_slot: config.mint_slot,
        mint_signature: anchor_signature,
        source_epoch: Some(selection.input.epoch),
        source_generation_digest: Some(generation_digest),
        source_wire_profile: Some(source_wire_profile),
        pubkey_registry_id_base: PUBKEY_REGISTRY_ID_BASE,
    });
    framed.write_with_scratch(&header, &mut scratch)?;
    let mut transactions_written = 0u64;
    let write_stats = reader.process_borrowed_blocks_parallel_ordered(
        range,
        ordered_config(config.workers, true),
        |_| (),
        |_, row_number, block| {
            collect_selected_block(
                selection.input.epoch,
                selection.selected.get(&row_number),
                generation_digest,
                source_wire_profile,
                block,
            )
            .map_err(|error| invalid_block_error(error.slot, error.message))
        },
        |_, records| {
            for pending_record in records {
                let slot = pending_record.record.block.slot;
                let tx_index = pending_record.record.tx_index;
                framed
                    .write_with_scratch(
                        &TokenTransactionDumpRecord::Transaction(pending_record.record),
                        &mut scratch,
                    )
                    .map_err(|error| {
                        invalid_block_error(
                            slot,
                            format!("transaction {tx_index} stream write: {error}"),
                        )
                    })?;
                transactions_written = transactions_written.checked_add(1).ok_or_else(|| {
                    invalid_block_error(slot, "transaction write count overflow".to_owned())
                })?;
            }
            Ok(())
        },
    )?;
    ensure!(
        transactions_written == selected_transaction_count(&selection.selected),
        "epoch {} wrote {transactions_written} transactions, expected {}",
        selection.input.epoch,
        selected_transaction_count(&selection.selected)
    );
    source
        .verify_unchanged()
        .with_context(|| format!("verify epoch {} did not change", selection.input.epoch))?;
    let footer = TokenTransactionDumpFooter {
        epochs: 1,
        blocks_scanned: selection.stats.blocks,
        transactions_scanned: selection.stats.transactions,
        transactions_written,
        pubkeys: 0,
        signatures: 0,
        owned_block_fallbacks: selection.stats.owned_block_fallbacks,
        raw_transaction_fallbacks: 0,
        raw_metadata_fallbacks: 0,
    };
    framed.write_with_scratch(&TokenTransactionDumpRecord::Footer(footer), &mut scratch)?;
    framed.flush()?;
    drop(framed);
    sync_file(&stream_path)?;

    let transition_path = shard_path.join(TRACKER_TRANSITION_FILE);
    let transition_bytes = wincode::config::serialize(
        &selection.tracker_transition,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )?;
    fs::write(&transition_path, transition_bytes)
        .with_context(|| format!("write {}", transition_path.display()))?;
    sync_file(&transition_path)?;

    let manifest = DumpManifest {
        schema_version: DUMP_SCHEMA_VERSION,
        artifact_kind: DumpArtifactKind::RawEpochShard,
        complete: true,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        source_binding: dump_source_binding(config),
        first_epoch: selection.input.epoch,
        last_epoch: selection.input.epoch,
        transactions: footer.transactions_written,
        signatures: None,
        pubkeys: None,
        transaction_stream: TRANSACTIONS_FILE.to_owned(),
        transaction_stream_sha256: Some(sha256_file(&stream_path)?),
        tracker_transition_log: Some(TRACKER_TRANSITION_FILE.to_owned()),
        tracker_transition_log_sha256: Some(sha256_file(&transition_path)?),
        signature_stream: None,
        signature_stream_sha256: None,
        pubkey_registry: None,
        pubkey_registry_sha256: None,
        registry_maps: None,
    };
    let manifest_path = shard_path.join(DUMP_MANIFEST_FILE);
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    fs::write(&manifest_path, manifest_bytes)
        .with_context(|| format!("write {}", manifest_path.display()))?;
    sync_file(&manifest_path)?;
    sync_directory(shard_path)?;
    Ok(ShardSummary {
        transactions: footer.transactions_written,
        compressed_bytes: write_stats.compressed_bytes,
    })
}

fn discover_epoch_creations(
    config: &ExtractConfig,
    input: &EpochInput,
    mint: [u8; 32],
    anchor_signature: [u8; 64],
    anchor_count: &mut u64,
) -> Result<EpochDiscoveryResult> {
    let (source, reader) = open_epoch(input, config)?;
    let registry =
        VerifiedEpochRegistry::open(source.clone(), reader.manifest(), reader.registry_entries())?;
    let matcher = DiscoveryMatcher::build(mint, &registry)?;
    let projector = reader.message_projector();
    let registry_entries = reader.registry_entries();
    let generation_digest = reader.binding().generation_digest;
    let first_slot = config.mint_slot.max(input.manifest.epoch_start_slot());
    let range = epoch_row_range(&reader, first_slot);
    let mut creations = BTreeMap::<[u8; 32], EpochCreationEntry>::new();
    let mut anchor_position = None;
    let mut stats = EpochScanStats::default();

    let ordered_stats = reader.process_borrowed_blocks_parallel_ordered(
        range,
        ordered_config(config.workers, true),
        |_| DiscoveryScratch::new(),
        |scratch, _, block| {
            project_creation_discovery_block(
                scratch,
                input.epoch,
                config.mint_slot,
                matcher,
                projector,
                registry_entries,
                generation_digest,
                reader.reader_id(),
                block,
            )
            .map_err(|error| invalid_block_error(error.slot, error.message))
        },
        |_, block| {
            stats.transactions = stats
                .transactions
                .checked_add(block.transactions_scanned)
                .ok_or_else(|| {
                    invalid_block_error(block.slot, "transaction count overflow".to_owned())
                })?;
            stats.owned_block_fallbacks = stats
                .owned_block_fallbacks
                .checked_add(u64::from(block.owned_fallback))
                .ok_or_else(|| {
                    invalid_block_error(block.slot, "owned fallback count overflow".to_owned())
                })?;
            for (tx_index, reference) in block.first_signatures {
                let signature = reader
                    .read_transaction_signatures(SignatureReference {
                        count: 1,
                        ..reference
                    })
                    .map_err(|error| {
                        invalid_block_error(
                            block.slot,
                            format!("transaction {tx_index} first signature read: {error}"),
                        )
                    })?;
                if signature.first() == Some(&anchor_signature) {
                    *anchor_count = anchor_count.checked_add(1).ok_or_else(|| {
                        invalid_block_error(block.slot, "anchor count overflow".to_owned())
                    })?;
                    anchor_position = Some(SourceTransactionCoordinate {
                        epoch: input.epoch,
                        slot: block.slot,
                        source_block_id: block.source_block_id,
                        tx_index,
                        source_first_signature_ordinal: reference.first_ordinal,
                        signature_count: reference.count,
                    });
                }
            }
            for candidate in block.creations {
                let raw_pubkey = registry
                    .resolve_verified(candidate.source_reference)
                    .map_err(|error| invalid_block_error(block.slot, error.to_string()))?;
                let entry = EpochCreationEntry {
                    source_reference: candidate.source_reference,
                    raw_pubkey,
                    first_creation: candidate.coordinate,
                };
                creations
                    .entry(raw_pubkey)
                    .and_modify(|current| {
                        if entry.first_creation < current.first_creation {
                            *current = entry;
                        }
                    })
                    .or_insert(entry);
            }
            Ok(())
        },
    )?;
    stats.blocks = ordered_stats.block_count;
    stats.compressed_bytes = ordered_stats.compressed_bytes;
    source
        .verify_unchanged()
        .with_context(|| format!("verify epoch {} discovery source", input.epoch))?;
    Ok(EpochDiscoveryResult {
        log: EpochCreationLog {
            schema_version: DUMP_SCHEMA_VERSION,
            epoch: input.epoch,
            source_generation_digest: generation_digest,
            mint,
            entries: creations.into_values().collect(),
        },
        anchor_position,
        stats,
    })
}

#[allow(clippy::too_many_arguments)]
fn project_creation_discovery_block(
    scratch: &mut DiscoveryScratch,
    epoch: u64,
    anchor_slot: u64,
    matcher: DiscoveryMatcher,
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    generation_digest: [u8; 32],
    reader_id: u64,
    block: BorrowedDecodedBlock<'_>,
) -> std::result::Result<DiscoveryBlock, BlockProjectionError> {
    project_creation_discovery_block_impl(
        scratch,
        epoch,
        anchor_slot,
        matcher,
        projector,
        registry_entries,
        generation_digest,
        reader_id,
        None,
        None,
        block,
    )
}

#[allow(clippy::too_many_arguments)]
fn project_creation_discovery_block_with_hints(
    scratch: &mut DiscoveryScratch,
    epoch: u64,
    anchor_slot: u64,
    matcher: DiscoveryMatcher,
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    generation_digest: [u8; 32],
    reader_id: u64,
    pre_merge_table: Option<&EpochTargetTable>,
    transaction_hints: &mut [u8],
    block: BorrowedDecodedBlock<'_>,
) -> std::result::Result<DiscoveryBlock, BlockProjectionError> {
    project_creation_discovery_block_impl(
        scratch,
        epoch,
        anchor_slot,
        matcher,
        projector,
        registry_entries,
        generation_digest,
        reader_id,
        pre_merge_table,
        Some(transaction_hints),
        block,
    )
}

#[allow(clippy::too_many_arguments)]
fn project_creation_discovery_block_impl(
    scratch: &mut DiscoveryScratch,
    epoch: u64,
    anchor_slot: u64,
    matcher: DiscoveryMatcher,
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    generation_digest: [u8; 32],
    reader_id: u64,
    pre_merge_table: Option<&EpochTargetTable>,
    mut transaction_hints: Option<&mut [u8]>,
    block: BorrowedDecodedBlock<'_>,
) -> std::result::Result<DiscoveryBlock, BlockProjectionError> {
    let slot = block.header().slot;
    let source_block_id = block.index_row.block_id;
    let transactions_scanned = u64::from(block.tx_count());
    if let Some(hints) = transaction_hints.as_ref()
        && hints.len() != block.tx_rows_len()
    {
        return Err(BlockProjectionError::new(
            slot,
            u32::MAX,
            "transaction match-hint count differs from storage transaction count",
        ));
    }
    let mut creations = Vec::new();
    let metadata_owned_fallbacks_before = scratch.metadata_owned_fallbacks;
    let mut first_signatures = if slot == anchor_slot {
        Vec::with_capacity(block.tx_rows_len())
    } else {
        Vec::new()
    };
    for (storage_index, located) in block.storage_transaction_rows().enumerate() {
        reject_opaque_flags(located.row.flags, located.row.tx_index, slot)?;
        let has_metadata = located.row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
        if has_metadata != (located.row.metadata_len != 0) {
            return Err(BlockProjectionError::new(
                slot,
                located.row.tx_index,
                "metadata presence flag disagrees with metadata byte length",
            ));
        }
        let message_bytes = checked_region(
            block.message_bytes(),
            located.row.message_offset,
            located.row.message_len,
            "message",
            located.row.tx_index,
            slot,
        )?;
        let metadata_bytes = if has_metadata {
            Some(checked_region(
                block.metadata_bytes(),
                located.row.metadata_offset,
                located.row.metadata_len,
                "metadata",
                located.row.tx_index,
                slot,
            )?)
        } else {
            None
        };
        if transaction_hints.is_some() {
            let account_match = project_transaction_creations_and_match(
                scratch,
                projector,
                registry_entries,
                located.row.flags,
                located.row.signature_count,
                matcher,
                message_bytes,
                metadata_bytes,
                true,
                |instruction_index, source_reference| {
                    creations.push(CreationCandidate {
                        source_reference,
                        coordinate: SourceInstructionCoordinate {
                            epoch,
                            slot,
                            source_block_id,
                            tx_index: located.row.tx_index,
                            instruction_index,
                        },
                    });
                },
                |reference| {
                    pre_merge_table.is_some_and(|table| {
                        table.reference_is_eligible(
                            reference,
                            slot,
                            source_block_id,
                            located.row.tx_index,
                        )
                    })
                },
            )
            .map_err(|error| BlockProjectionError::new(slot, located.row.tx_index, error))?;
            transaction_hints
                .as_deref_mut()
                .expect("hint mode was checked above")[storage_index] = u8::from(account_match);
        } else if located.row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR == 0 {
            project_transaction_creations(
                scratch,
                projector,
                registry_entries,
                located.row.flags,
                located.row.signature_count,
                matcher,
                message_bytes,
                metadata_bytes,
                |instruction_index, source_reference| {
                    creations.push(CreationCandidate {
                        source_reference,
                        coordinate: SourceInstructionCoordinate {
                            epoch,
                            slot,
                            source_block_id,
                            tx_index: located.row.tx_index,
                            instruction_index,
                        },
                    });
                },
            )
            .map_err(|error| BlockProjectionError::new(slot, located.row.tx_index, error))?;
        }

        if slot == anchor_slot {
            let first_ordinal = block
                .index_row
                .first_signature_ordinal
                .checked_add(u64::from(located.first_signature_offset))
                .ok_or_else(|| {
                    BlockProjectionError::new(
                        slot,
                        located.row.tx_index,
                        "signature ordinal overflow",
                    )
                })?;
            first_signatures.push((
                located.row.tx_index,
                SignatureReference {
                    generation_digest,
                    reader_id,
                    first_ordinal,
                    count: located.row.signature_count,
                },
            ));
        }
    }
    Ok(DiscoveryBlock {
        slot,
        source_block_id,
        transactions_scanned,
        owned_fallback: block.uses_owned_fallback(),
        first_signatures,
        creations,
        metadata_owned_fallbacks: scratch
            .metadata_owned_fallbacks
            .checked_sub(metadata_owned_fallbacks_before)
            .expect("metadata owned-fallback counter is monotonic"),
    })
}

#[allow(clippy::too_many_arguments)]
fn project_transaction_creations(
    scratch: &mut DiscoveryScratch,
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    row_flags: u32,
    signature_count: u8,
    matcher: DiscoveryMatcher,
    message_bytes: &[u8],
    metadata_bytes: Option<&[u8]>,
    mut on_creation: impl FnMut(u32, CompactPubkey),
) -> Result<()> {
    if row_flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0 {
        return Ok(());
    }
    project_transaction_creations_and_match(
        scratch,
        projector,
        registry_entries,
        row_flags,
        signature_count,
        matcher,
        message_bytes,
        metadata_bytes,
        false,
        &mut on_creation,
        |_| false,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn project_transaction_creations_and_match(
    scratch: &mut DiscoveryScratch,
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    row_flags: u32,
    signature_count: u8,
    matcher: DiscoveryMatcher,
    message_bytes: &[u8],
    metadata_bytes: Option<&[u8]>,
    validate_account_metadata: bool,
    mut on_creation: impl FnMut(u32, CompactPubkey),
    mut matches_account: impl FnMut(CompactPubkey) -> bool,
) -> Result<bool> {
    let discover_creations = row_flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR == 0;
    scratch.begin_transaction();
    let static_accounts = &mut scratch.static_accounts;
    let static_account_count = &mut scratch.static_account_count;
    let candidates = &mut scratch.candidates;
    let mut account_match = false;
    let mut outer_instruction_index = 0usize;
    let message = projector.visit_static_accounts_and_instructions_exact(
        message_bytes,
        registry_entries,
        |ordinal, reference| {
            debug_assert_eq!(ordinal, *static_account_count);
            static_accounts[ordinal] = reference;
            *static_account_count = ordinal + 1;
            account_match |= matches_account(reference);
        },
        |instruction: BorrowedArchiveV2Instruction<'_>| {
            if discover_creations
                && let Some(indices) = init_indices(
                    usize::from(instruction.program_id_index),
                    instruction.accounts,
                    instruction.raw_data.unwrap_or_default(),
                )
            {
                candidates.push(DeferredInitCandidate {
                    indices,
                    outer_instruction_index,
                    inner_instruction_index: None,
                });
            }
            outer_instruction_index += 1;
        },
    )?;
    validate_message_summary(&message, row_flags, signature_count)?;
    ensure!(
        scratch.static_account_count == message.static_account_count
            && outer_instruction_index == message.instruction_count,
        "message discovery callbacks differ from their exact summary"
    );
    ensure!(
        (message.expected_loaded_writable != 0 || message.expected_loaded_readonly != 0)
            == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0),
        "message loaded-address lookups differ from transaction-row flags"
    );

    retain_potential_candidates(scratch, matcher);
    scratch.prepare_inner_counts(message.instruction_count);
    let has_inner = row_flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0;
    let has_loaded = message.expected_loaded_writable != 0 || message.expected_loaded_readonly != 0;
    let outer_needs_loaded = scratch.candidates.iter().any(|candidate| {
        candidate.indices.program >= message.static_account_count
            || candidate.indices.account >= message.static_account_count
            || candidate.indices.mint >= message.static_account_count
    });
    let mut used_metadata_owned_fallback = false;
    if has_loaded || (discover_creations && (has_inner || outer_needs_loaded)) {
        let source_bytes = metadata_bytes.context(
            "account scan needs inner instructions or loaded addresses but metadata is absent",
        )?;
        used_metadata_owned_fallback = source_bytes.first() != Some(&0);
        let summary = if used_metadata_owned_fallback {
            // A present metadata error has two historical wire schemas. Select one with the
            // bounded, ambiguity-safe owned decoder before callbacks can publish partial state,
            // then run the same exact selective visitor over canonical current bytes.
            let mut canonical_metadata = std::mem::take(&mut scratch.canonical_metadata);
            let result = (|| {
                canonical_metadata =
                    blockzilla_archive_v2::canonicalize_archive_v2_metadata_owned(source_bytes)?.0;
                parse_discovery_metadata(
                    scratch,
                    &message,
                    matcher,
                    registry_entries,
                    &canonical_metadata,
                    discover_creations,
                )
            })();
            scratch.canonical_metadata = canonical_metadata;
            result?
        } else {
            parse_discovery_metadata(
                scratch,
                &message,
                matcher,
                registry_entries,
                source_bytes,
                discover_creations,
            )?
        };
        if validate_account_metadata {
            validate_metadata_summary(&summary, &message, row_flags)?;
        } else {
            validate_discovery_metadata_summary(&summary, &message, row_flags)?;
        }

        let loaded_start = message.static_account_count;
        let loaded_end = loaded_start
            .checked_add(message.expected_loaded_writable)
            .and_then(|count| count.checked_add(message.expected_loaded_readonly))
            .context("loaded account range overflow")?;
        for index in loaded_start..loaded_end {
            let reference = scratch
                .resolve_account(index)
                .with_context(|| format!("loaded account index {index} was not resolved"))?;
            account_match |= matches_account(reference);
        }
    }

    for candidate in scratch.candidates.iter().copied() {
        let Some(account) = resolve_init_account(candidate.indices, scratch, matcher)? else {
            continue;
        };
        let preceding_inner = scratch.inner_counts[..candidate.outer_instruction_index]
            .iter()
            .try_fold(0usize, |sum, count| {
                sum.checked_add(*count as usize)
                    .context("instruction coordinate overflow")
            })?;
        let instruction_index = candidate
            .outer_instruction_index
            .checked_add(preceding_inner)
            .and_then(|index| {
                candidate
                    .inner_instruction_index
                    .map_or(Some(index), |inner| index.checked_add(1 + inner))
            })
            .context("instruction coordinate overflow")?;
        on_creation(
            u32::try_from(instruction_index).context("instruction coordinate exceeds u32")?,
            account,
        );
    }
    if used_metadata_owned_fallback {
        scratch.metadata_owned_fallbacks = scratch
            .metadata_owned_fallbacks
            .checked_add(1)
            .context("metadata owned-fallback count overflow")?;
    }
    Ok(account_match)
}

fn parse_discovery_metadata(
    scratch: &mut DiscoveryScratch,
    message: &ProjectedArchiveV2MessageAccountSummary,
    matcher: DiscoveryMatcher,
    registry_entries: u32,
    metadata_bytes: &[u8],
    discover_creations: bool,
) -> Result<ProjectedArchiveV2TokenMetadataSummary> {
    let total_message_accounts = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("message account count overflow")?;
    let static_accounts = &scratch.static_accounts[..scratch.static_account_count];
    let candidates = &mut scratch.candidates;
    let inner_counts = &mut scratch.inner_counts;
    let loaded_accounts = &mut scratch.loaded_accounts;
    let loaded_generation = &mut scratch.loaded_generation;
    let generation = scratch.generation;
    Ok(visit_archive_v2_token_metadata_exact_ordered(
        metadata_bytes,
        ArchiveV2MetadataProjectionLimits {
            total_message_accounts,
            top_level_instruction_count: message.instruction_count,
        },
        registry_entries,
        // Discovery only reads inner instructions and loaded addresses. The raw dump copies
        // metadata bytes verbatim, so log content is never interpreted here or downstream.
        LogPayloadValidation::StructureOnly,
        |outer_index, instruction: BorrowedArchiveV2InnerTokenInstruction<'_>| {
            let outer_index = outer_index as usize;
            let inner_index = inner_counts[outer_index] as usize;
            inner_counts[outer_index] = inner_counts[outer_index].saturating_add(1);
            if discover_creations
                && let Some(indices) = init_indices(
                    instruction.program_id_index as usize,
                    instruction.accounts,
                    instruction.data,
                )
                && retain_potential_init(indices, static_accounts, matcher)
            {
                candidates.push(DeferredInitCandidate {
                    indices,
                    outer_instruction_index: outer_index,
                    inner_instruction_index: Some(inner_index),
                });
            }
        },
        |_, _| {},
        |side, ordinal, reference| {
            let absolute = match side {
                ArchiveV2LoadedAddressSide::Writable => message.static_account_count + ordinal,
                ArchiveV2LoadedAddressSide::Readonly => {
                    message.static_account_count + message.expected_loaded_writable + ordinal
                }
            };
            loaded_accounts[absolute] = reference;
            loaded_generation[absolute] = generation;
        },
    )?)
}

fn init_indices(program: usize, accounts: &[u8], data: &[u8]) -> Option<InitInstructionIndices> {
    if !matches!(data.first().copied(), Some(1 | 16 | 18)) {
        return None;
    }
    let [account, mint, ..] = accounts else {
        return None;
    };
    Some(InitInstructionIndices {
        program,
        account: usize::from(*account),
        mint: usize::from(*mint),
    })
}

fn retain_potential_init(
    indices: InitInstructionIndices,
    static_accounts: &[CompactPubkey],
    matcher: DiscoveryMatcher,
) -> bool {
    if let Some(program) = static_accounts.get(indices.program)
        && !matcher.is_token_program(*program)
    {
        return false;
    }
    if let Some(mint) = static_accounts.get(indices.mint)
        && !matcher.is_mint(*mint)
    {
        return false;
    }
    true
}

fn retain_potential_candidates(scratch: &mut DiscoveryScratch, matcher: DiscoveryMatcher) {
    let static_accounts = &scratch.static_accounts[..scratch.static_account_count];
    let mut write = 0usize;
    for read in 0..scratch.candidates.len() {
        let candidate = scratch.candidates[read];
        if retain_potential_init(candidate.indices, static_accounts, matcher) {
            scratch.candidates[write] = candidate;
            write += 1;
        }
    }
    scratch.candidates.truncate(write);
}

fn resolve_init_account(
    indices: InitInstructionIndices,
    scratch: &DiscoveryScratch,
    matcher: DiscoveryMatcher,
) -> Result<Option<CompactPubkey>> {
    let resolve = |index: usize| {
        scratch
            .resolve_account(index)
            .with_context(|| format!("creation instruction account index {index} was not resolved"))
    };
    let program = resolve(indices.program)?;
    let mint = resolve(indices.mint)?;
    if !matcher.is_token_program(program) || !matcher.is_mint(mint) {
        return Ok(None);
    }
    Ok(Some(resolve(indices.account)?))
}

fn merge_single_read_discovery_block(
    state: &mut SingleReadEpochCoordinator<'_>,
    source: &PinnedLocalRangeSource,
    anchor_signature: [u8; 64],
    block: DiscoveryBlock,
) -> Result<()> {
    state.stats.transactions = state
        .stats
        .transactions
        .checked_add(block.transactions_scanned)
        .context("single-read transaction count overflow")?;
    state.stats.owned_block_fallbacks = state
        .stats
        .owned_block_fallbacks
        .checked_add(u64::from(block.owned_fallback))
        .context("single-read owned-fallback count overflow")?;
    state.extractor_stats.metadata_owned_fallbacks = state
        .extractor_stats
        .metadata_owned_fallbacks
        .checked_add(block.metadata_owned_fallbacks)
        .context("single-read metadata owned-fallback count overflow")?;

    merge_single_read_anchor_signatures(state, source, anchor_signature, &block)?;

    for candidate in block.creations {
        let ledger_sequence = state.next_candidate_sequence;
        state.next_candidate_sequence = state
            .next_candidate_sequence
            .checked_add(1)
            .context("creation candidate sequence overflow")?;
        state.extractor_stats.creation_candidates = state
            .extractor_stats
            .creation_candidates
            .checked_add(1)
            .context("creation candidate count overflow")?;
        let pending = PendingCreationCandidate {
            coordinate: candidate.coordinate,
            ledger_sequence,
        };
        let cached = match candidate.source_reference {
            CompactPubkey::Id(id) if Some(id) == state.mint_id => Some((state.mint, Some(id))),
            CompactPubkey::Id(id) => state
                .known_id_mappings
                .binary_search_by_key(&id, |(candidate, _)| *candidate)
                .ok()
                .and_then(|index| state.known_id_mappings.get(index))
                .map(|(_, raw)| (*raw, Some(id))),
            CompactPubkey::Raw(raw) if raw == state.mint => Some((raw, state.mint_id)),
            CompactPubkey::Raw(raw) => state
                .resolved_accounts
                .binary_search_by_key(&raw, |account| account.raw_pubkey)
                .ok()
                .and_then(|index| state.resolved_accounts.get(index))
                .map(|account| (raw, account.local_id)),
        };
        if let Some((raw_pubkey, local_id)) = cached {
            state.cached_candidates.push(ResolvedCreationCandidate {
                source_reference: candidate.source_reference,
                raw_pubkey,
                local_id,
                coordinate: candidate.coordinate,
                ledger_sequence,
            });
        } else {
            match candidate.source_reference {
                CompactPubkey::Id(id) => state.pending_candidate_ids.push((id, pending)),
                CompactPubkey::Raw(raw) => state.pending_candidate_raw.push((raw, pending)),
            }
        }
    }
    Ok(())
}

fn merge_single_read_anchor_signatures(
    state: &mut SingleReadEpochCoordinator<'_>,
    source: &PinnedLocalRangeSource,
    anchor_signature: [u8; 64],
    block: &DiscoveryBlock,
) -> Result<()> {
    let Some((_, first)) = block.first_signatures.first() else {
        return Ok(());
    };
    let mut first_ordinal = first.first_ordinal;
    let mut end_ordinal = first
        .first_ordinal
        .checked_add(1)
        .context("anchor signature ordinal overflow")?;
    for (_, reference) in &block.first_signatures {
        ensure!(
            reference.count != 0,
            "epoch {} slot {} transaction has no first signature",
            state.epoch,
            block.slot
        );
        first_ordinal = first_ordinal.min(reference.first_ordinal);
        end_ordinal = end_ordinal.max(
            reference
                .first_ordinal
                .checked_add(1)
                .context("anchor signature ordinal overflow")?,
        );
    }
    let signature_count = end_ordinal
        .checked_sub(first_ordinal)
        .context("reversed anchor signature range")?;
    let byte_len = usize::try_from(
        signature_count
            .checked_mul(64)
            .context("anchor signature byte length overflow")?,
    )
    .context("anchor signature byte length exceeds usize")?;
    let byte_offset = first_ordinal
        .checked_mul(64)
        .context("anchor signature byte offset overflow")?;
    source
        .read_range_into(
            SIGNATURES_FILE,
            byte_offset,
            byte_len,
            &mut state.anchor_signature_bytes,
        )
        .with_context(|| {
            format!(
                "read epoch {} slot {} first-signature span",
                state.epoch, block.slot
            )
        })?;

    for (tx_index, reference) in &block.first_signatures {
        let relative = reference
            .first_ordinal
            .checked_sub(first_ordinal)
            .and_then(|ordinal| ordinal.checked_mul(64))
            .and_then(|offset| usize::try_from(offset).ok())
            .context("anchor signature relative offset overflow")?;
        let signature = state
            .anchor_signature_bytes
            .get(relative..relative + 64)
            .context("anchor signature range is shorter than its references")?;
        if signature != anchor_signature {
            continue;
        }
        *state.anchor_count = state
            .anchor_count
            .checked_add(1)
            .context("mint-anchor count overflow")?;
        ensure!(
            *state.anchor_count == 1,
            "mint signature occurs more than once at slot {}",
            block.slot
        );
        let position = SourceTransactionCoordinate {
            epoch: state.epoch,
            slot: block.slot,
            source_block_id: block.source_block_id,
            tx_index: *tx_index,
            source_first_signature_ordinal: reference.first_ordinal,
            signature_count: reference.count,
        };
        ensure!(
            state.anchor_position.is_none(),
            "mint-anchor position was already set before its signature was found"
        );
        *state.anchor_position = Some(position);
    }
    Ok(())
}

fn finish_single_read_discovery_batch(
    state: &mut SingleReadEpochCoordinator<'_>,
    registry: &VerifiedEpochRegistry,
    generation_digest: [u8; 32],
) -> Result<()> {
    let anchor_position = (*state.anchor_position)
        .context("the mint signature was not found in the first retained batch at the mint slot")?;
    deduplicate_pending_id_candidates(&mut state.pending_candidate_ids);
    deduplicate_pending_raw_candidates(&mut state.pending_candidate_raw);
    state.new_accounts.clear();
    if !state.pending_candidate_ids.is_empty() || !state.pending_candidate_raw.is_empty() {
        state.extractor_stats.unique_candidate_ids = state
            .extractor_stats
            .unique_candidate_ids
            .checked_add(
                u64::try_from(state.pending_candidate_ids.len())
                    .context("unique creation ID count exceeds u64")?,
            )
            .context("unique creation ID count overflow")?;
        state.extractor_stats.unique_candidate_raw_refs = state
            .extractor_stats
            .unique_candidate_raw_refs
            .checked_add(
                u64::try_from(state.pending_candidate_raw.len())
                    .context("unique raw creation count exceeds u64")?,
            )
            .context("unique raw creation count overflow")?;
        let registry_started = Instant::now();
        let registry_stats = registry.resolve_creation_candidates_bulk(
            &state.pending_candidate_ids,
            &state.pending_candidate_raw,
            &mut state.registry_scratch,
            &mut state.resolved_candidates,
        )?;
        state.extractor_stats.registry_resolution_time = state
            .extractor_stats
            .registry_resolution_time
            .saturating_add(registry_started.elapsed());
        state.extractor_stats.registry.add(registry_stats)?;
        state.pending_candidate_ids.clear();
        state.pending_candidate_raw.clear();
    } else {
        state.resolved_candidates.clear();
    }
    state
        .resolved_candidates
        .append(&mut state.cached_candidates);
    state.resolved_candidates.sort_unstable_by(|left, right| {
        left.raw_pubkey
            .cmp(&right.raw_pubkey)
            .then_with(|| left.coordinate.cmp(&right.coordinate))
            .then_with(|| left.ledger_sequence.cmp(&right.ledger_sequence))
    });

    if !state.resolved_candidates.is_empty() {
        let mut candidate = 0usize;
        while candidate < state.resolved_candidates.len() {
            let raw_pubkey = state.resolved_candidates[candidate].raw_pubkey;
            let first = state.resolved_candidates[candidate];
            let mut end = candidate + 1;
            while state
                .resolved_candidates
                .get(end)
                .is_some_and(|next| next.raw_pubkey == raw_pubkey)
            {
                ensure!(
                    state.resolved_candidates[end].local_id == first.local_id,
                    "raw and ID creation references disagree on their epoch registry mapping"
                );
                end += 1;
            }
            let entry = EpochCreationEntry {
                source_reference: first.source_reference,
                raw_pubkey,
                first_creation: first.coordinate,
            };
            state
                .epoch_creations
                .entry(raw_pubkey)
                .and_modify(|current| {
                    if entry.first_creation < current.first_creation {
                        *current = entry;
                    }
                })
                .or_insert(entry);

            let should_update = state
                .global_accounts
                .get(&raw_pubkey)
                .is_none_or(|current| first.coordinate < *current);
            if should_update {
                state.global_accounts.insert(raw_pubkey, first.coordinate);
                let resolved = ResolvedDiscoveredAccount {
                    raw_pubkey,
                    first_creation: first.coordinate,
                    local_id: first.local_id,
                };
                state.new_accounts.push(resolved);
            }
            candidate = end;
        }
        state.extractor_stats.new_accounts = state
            .extractor_stats
            .new_accounts
            .checked_add(
                u64::try_from(state.new_accounts.len()).context("new account count exceeds u64")?,
            )
            .context("new account count overflow")?;
    }

    let update_started = Instant::now();
    if !state.new_accounts.is_empty() {
        merge_sorted_resolved_accounts(
            &mut state.resolved_accounts,
            &state.new_accounts,
            &mut state.resolved_account_merge_scratch,
        )?;
        merge_new_known_id_mappings(
            &mut state.known_id_mappings,
            &state.new_accounts,
            &mut state.new_id_mappings,
            &mut state.known_id_merge_scratch,
        )?;
    }
    if state.target_table.is_none() {
        state.target_table = Some(EpochTargetTable::build_resolved(
            state.epoch,
            state.mint,
            state.mint_id,
            anchor_position,
            &state.resolved_accounts,
            registry.entries,
            generation_digest,
        )?);
    } else if !state.new_accounts.is_empty() {
        state
            .target_table
            .as_mut()
            .context("single-read target table is absent after anchor discovery")?
            .extend_current_accounts(&state.new_accounts)?;
    }
    state.extractor_stats.target_build_time = state
        .extractor_stats
        .target_build_time
        .saturating_add(update_started.elapsed());
    Ok(())
}

fn finish_single_read_discovery_batch_with_hints(
    state: &mut SingleReadEpochCoordinator<'_>,
    registry: &VerifiedEpochRegistry,
    generation_digest: [u8; 32],
) -> Result<()> {
    let table_was_absent = state.target_table.is_none();
    finish_single_read_discovery_batch(state, registry, generation_digest)?;
    state.batch_hints_dirty = table_was_absent || !state.new_accounts.is_empty();
    if state.batch_hints_dirty {
        state.extractor_stats.dirty_hint_batches = state
            .extractor_stats
            .dirty_hint_batches
            .checked_add(1)
            .context("dirty match-hint batch count overflow")?;
    } else {
        state.extractor_stats.clean_hint_batches = state
            .extractor_stats
            .clean_hint_batches
            .checked_add(1)
            .context("clean match-hint batch count overflow")?;
    }
    Ok(())
}

fn deduplicate_pending_id_candidates(candidates: &mut Vec<(u32, PendingCreationCandidate)>) {
    candidates.sort_unstable_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| {
            (left.1.coordinate, left.1.ledger_sequence)
                .cmp(&(right.1.coordinate, right.1.ledger_sequence))
        })
    });
    let mut previous = None;
    candidates.retain(|(id, _)| {
        let keep = previous != Some(*id);
        previous = Some(*id);
        keep
    });
}

fn deduplicate_pending_raw_candidates(candidates: &mut Vec<([u8; 32], PendingCreationCandidate)>) {
    candidates.sort_unstable_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| {
            (left.1.coordinate, left.1.ledger_sequence)
                .cmp(&(right.1.coordinate, right.1.ledger_sequence))
        })
    });
    let mut previous = None;
    candidates.retain(|(raw, _)| {
        let keep = previous != Some(*raw);
        previous = Some(*raw);
        keep
    });
}

fn merge_sorted_resolved_accounts(
    current: &mut Vec<ResolvedDiscoveredAccount>,
    delta: &[ResolvedDiscoveredAccount],
    scratch: &mut Vec<ResolvedDiscoveredAccount>,
) -> Result<()> {
    ensure!(
        delta
            .windows(2)
            .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey),
        "new resolved accounts are not strictly sorted and unique"
    );
    scratch.clear();
    scratch.reserve(current.len().saturating_add(delta.len()));
    let (mut left, mut right) = (0usize, 0usize);
    while left < current.len() && right < delta.len() {
        match current[left].raw_pubkey.cmp(&delta[right].raw_pubkey) {
            std::cmp::Ordering::Less => {
                scratch.push(current[left]);
                left += 1;
            }
            std::cmp::Ordering::Greater => {
                scratch.push(delta[right]);
                right += 1;
            }
            std::cmp::Ordering::Equal => {
                anyhow::bail!("new resolved account repeats an epoch cache entry")
            }
        }
    }
    scratch.extend_from_slice(&current[left..]);
    scratch.extend_from_slice(&delta[right..]);
    std::mem::swap(current, scratch);
    Ok(())
}

fn merge_new_known_id_mappings(
    current: &mut Vec<(u32, [u8; 32])>,
    accounts: &[ResolvedDiscoveredAccount],
    delta: &mut Vec<(u32, [u8; 32])>,
    scratch: &mut Vec<(u32, [u8; 32])>,
) -> Result<()> {
    delta.clear();
    delta.reserve(accounts.len());
    delta.extend(
        accounts
            .iter()
            .filter_map(|account| account.local_id.map(|id| (id, account.raw_pubkey))),
    );
    delta.sort_unstable_by_key(|(id, _)| *id);
    ensure!(
        delta.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "two new accounts map to the same epoch registry ID"
    );
    scratch.clear();
    scratch.reserve(current.len().saturating_add(delta.len()));
    let (mut left, mut right) = (0usize, 0usize);
    while left < current.len() && right < delta.len() {
        match current[left].0.cmp(&delta[right].0) {
            std::cmp::Ordering::Less => {
                scratch.push(current[left]);
                left += 1;
            }
            std::cmp::Ordering::Greater => {
                scratch.push(delta[right]);
                right += 1;
            }
            std::cmp::Ordering::Equal => {
                anyhow::bail!("new account repeats an epoch registry ID mapping")
            }
        }
    }
    scratch.extend_from_slice(&current[left..]);
    scratch.extend_from_slice(&delta[right..]);
    std::mem::swap(current, scratch);
    Ok(())
}

fn consume_single_read_matched_block(
    state: &mut SingleReadEpochCoordinator<'_>,
    block: MatchedBlock,
    anchor_transactions: &mut u64,
    transactions_written: &mut u64,
) -> std::result::Result<(), ReadError> {
    *anchor_transactions = anchor_transactions
        .checked_add(block.anchor_transactions)
        .ok_or_else(|| {
            invalid_block_error(block.slot, "anchor transaction count overflow".to_owned())
        })?;
    *transactions_written = transactions_written
        .checked_add(block.transactions_written)
        .ok_or_else(|| {
            invalid_block_error(block.slot, "transaction write count overflow".to_owned())
        })?;
    state.extractor_stats.hint_direct_matches = state
        .extractor_stats
        .hint_direct_matches
        .checked_add(block.hint_direct_matches)
        .ok_or_else(|| {
            invalid_block_error(block.slot, "direct match-hint count overflow".into())
        })?;
    state.extractor_stats.hint_skips_without_decode = state
        .extractor_stats
        .hint_skips_without_decode
        .checked_add(block.hint_skips_without_decode)
        .ok_or_else(|| invalid_block_error(block.slot, "match-hint skip count overflow".into()))?;
    state.extractor_stats.hint_exact_reparses = state
        .extractor_stats
        .hint_exact_reparses
        .checked_add(block.hint_exact_reparses)
        .ok_or_else(|| {
            invalid_block_error(block.slot, "exact match-hint reparse count overflow".into())
        })?;
    state.extractor_stats.metadata_owned_fallbacks = state
        .extractor_stats
        .metadata_owned_fallbacks
        .checked_add(block.metadata_owned_fallbacks)
        .ok_or_else(|| {
            invalid_block_error(block.slot, "metadata owned-fallback count overflow".into())
        })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_single_read_epoch(
    config: &ExtractConfig,
    input: &EpochInput,
    shard_path: &Path,
    mint: [u8; 32],
    anchor_signature: [u8; 64],
    global_accounts: &mut DiscoveredAccountMap,
    anchor_position: &mut Option<SourceTransactionCoordinate>,
    anchor_count: &mut u64,
) -> Result<SingleReadEpochResult> {
    write_single_read_epoch_with_block_config(
        config,
        input,
        shard_path,
        mint,
        anchor_signature,
        global_accounts,
        anchor_position,
        anchor_count,
        ordered_config(config.workers, true),
    )
}

#[allow(clippy::too_many_arguments)]
fn write_single_read_epoch_with_block_config(
    config: &ExtractConfig,
    input: &EpochInput,
    shard_path: &Path,
    mint: [u8; 32],
    anchor_signature: [u8; 64],
    global_accounts: &mut DiscoveredAccountMap,
    anchor_position: &mut Option<SourceTransactionCoordinate>,
    anchor_count: &mut u64,
    block_config: OrderedParallelBlockConfig,
) -> Result<SingleReadEpochResult> {
    let (source, reader) = open_epoch(input, config)?;
    let registry =
        VerifiedEpochRegistry::open(source.clone(), reader.manifest(), reader.registry_entries())?;
    let generation_digest = reader.binding().generation_digest;
    let source_wire_profile = dump_wire_profile(reader.message_projector().wire_profile());
    let projector = reader.message_projector();
    let registry_entries = reader.registry_entries();

    let stream_path = shard_path.join(TRANSACTIONS_FILE);
    let stream_file =
        File::create(&stream_path).with_context(|| format!("create {}", stream_path.display()))?;
    let mut framed = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(8 << 20, stream_file));
    let mut framing_scratch = Vec::with_capacity(2 << 20);
    framed.write_with_scratch(
        &TokenTransactionDumpRecord::Header(TokenTransactionDumpHeader {
            schema_version: DUMP_SCHEMA_VERSION,
            stream_kind: DumpStreamKind::RawEpochShard,
            mint,
            mint_slot: config.mint_slot,
            mint_signature: anchor_signature,
            source_epoch: Some(input.epoch),
            source_generation_digest: Some(generation_digest),
            source_wire_profile: Some(source_wire_profile),
            pubkey_registry_id_base: PUBKEY_REGISTRY_ID_BASE,
        }),
        &mut framing_scratch,
    )?;

    let mapping_coordinate = SourceInstructionCoordinate {
        epoch: input.epoch,
        slot: config.mint_slot,
        source_block_id: 0,
        tx_index: 0,
        instruction_index: 0,
    };
    let mut admitted = Vec::with_capacity(global_accounts.len() + 3);
    admitted.extend(
        [mint, SPL_TOKEN_PROGRAM_ID, SPL_TOKEN_2022_PROGRAM_ID]
            .into_iter()
            .map(|raw_pubkey| DiscoveredAccount {
                raw_pubkey,
                first_creation: mapping_coordinate,
            }),
    );
    admitted.extend(
        global_accounts
            .iter()
            .map(|(raw_pubkey, first_creation)| DiscoveredAccount {
                raw_pubkey: *raw_pubkey,
                first_creation: *first_creation,
            }),
    );
    let mut resolved_rows = Vec::new();
    let mut registry_read_buffer = Vec::new();
    let registry_resolution_started = Instant::now();
    let (resolved, initial_registry_stats) = registry.resolve_raw_accounts_bulk(
        &admitted,
        &mut resolved_rows,
        &mut registry_read_buffer,
    )?;
    let initial_registry_resolution_time = registry_resolution_started.elapsed();
    let mint_id = resolved
        .first()
        .context("initial registry mapping has no target mint")?
        .local_id;
    let token_program_ids = [resolved[1].local_id, resolved[2].local_id];
    let matcher = DiscoveryMatcher::with_ids(mint, mint_id, token_program_ids);
    let target_build_started = Instant::now();
    let resolved_accounts = resolved[3..].to_vec();
    let mut known_id_mappings = resolved_accounts
        .iter()
        .filter_map(|account| account.local_id.map(|id| (id, account.raw_pubkey)))
        .collect::<Vec<_>>();
    if let Some(id) = mint_id {
        known_id_mappings.push((id, mint));
    }
    known_id_mappings.sort_unstable_by_key(|(id, _)| *id);
    ensure!(
        known_id_mappings
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0),
        "two initial tracked keys map to the same epoch registry ID"
    );
    let initial_table = if let Some(position) = *anchor_position {
        Some(EpochTargetTable::build_resolved(
            input.epoch,
            mint,
            mint_id,
            position,
            &resolved_accounts,
            registry_entries,
            generation_digest,
        )?)
    } else {
        None
    };
    let initial_target_build_time = target_build_started.elapsed();
    let mut state = SingleReadEpochCoordinator {
        epoch: input.epoch,
        mint,
        mint_id,
        global_accounts,
        resolved_accounts,
        resolved_account_merge_scratch: Vec::new(),
        known_id_mappings,
        new_id_mappings: Vec::new(),
        known_id_merge_scratch: Vec::new(),
        epoch_creations: BTreeMap::new(),
        pending_candidate_ids: Vec::new(),
        pending_candidate_raw: Vec::new(),
        next_candidate_sequence: 0,
        registry_scratch: RegistryResolutionScratch {
            rows: resolved_rows,
            read_buffer: registry_read_buffer,
            ..RegistryResolutionScratch::default()
        },
        resolved_candidates: Vec::new(),
        cached_candidates: Vec::new(),
        new_accounts: Vec::new(),
        anchor_position,
        anchor_count,
        anchor_signature_bytes: Vec::new(),
        target_table: initial_table,
        batch_hints_dirty: false,
        writer: Mutex::new(SharedRawWriter {
            framed,
            first_error: None,
        }),
        stats: EpochScanStats::default(),
        extractor_stats: SingleReadExtractorStats {
            registry: initial_registry_stats,
            registry_resolution_time: initial_registry_resolution_time,
            target_build_time: initial_target_build_time,
            ..SingleReadExtractorStats::default()
        },
    };

    let first_slot = config.mint_slot.max(input.manifest.epoch_start_slot());
    let range = epoch_row_range(&reader, first_slot);
    let mut transactions_written = 0u64;
    let mut anchor_transactions = 0u64;
    let ordered_stats = if config.single_read_match_hints {
        reader.process_borrowed_blocks_parallel_batch_barrier_with_transaction_state(
            range,
            block_config,
            SINGLE_READ_MATCH_HINT_BUDGET_BYTES,
            &mut state,
            |_| SingleReadWorkerScratch::new(),
            |scratch, pre_merge_state, _, block, transaction_hints: &mut [u8]| {
                project_creation_discovery_block_with_hints(
                    &mut scratch.discovery,
                    input.epoch,
                    config.mint_slot,
                    matcher,
                    projector,
                    registry_entries,
                    generation_digest,
                    reader.reader_id(),
                    pre_merge_state.target_table.as_ref(),
                    transaction_hints,
                    block,
                )
                .map_err(|error| invalid_block_error(error.slot, error.message))
            },
            |state, _, block| {
                let slot = block.slot;
                merge_single_read_discovery_block(state, &source, anchor_signature, block)
                    .map_err(|error| invalid_block_error(slot, error.to_string()))
            },
            |state, _| {
                finish_single_read_discovery_batch_with_hints(state, &registry, generation_digest)
                    .map_err(|error| invalid_block_error(first_slot, error.to_string()))
            },
            |scratch, state, _, block, transaction_hints: &[u8]| {
                let slot = block.header().slot;
                let table = state.target_table.as_ref().ok_or_else(|| {
                    invalid_block_error(slot, "single-read target table is absent".to_owned())
                })?;
                let anchor = (*state.anchor_position).ok_or_else(|| {
                    invalid_block_error(slot, "single-read mint anchor is absent".to_owned())
                })?;
                collect_matching_block_with_hints(
                    &mut scratch.matching,
                    &state.writer,
                    input.epoch,
                    generation_digest,
                    source_wire_profile,
                    projector,
                    registry_entries,
                    table,
                    anchor,
                    transaction_hints,
                    state.batch_hints_dirty,
                    block,
                )
                .map_err(|error| invalid_block_error(error.slot, error.message))
            },
            |state, _, block| {
                consume_single_read_matched_block(
                    state,
                    block,
                    &mut anchor_transactions,
                    &mut transactions_written,
                )
            },
        )?
    } else {
        reader.process_borrowed_blocks_parallel_batch_barrier(
            range,
            block_config,
            &mut state,
            |_| SingleReadWorkerScratch::new(),
            |scratch, _, block| {
                project_creation_discovery_block(
                    &mut scratch.discovery,
                    input.epoch,
                    config.mint_slot,
                    matcher,
                    projector,
                    registry_entries,
                    generation_digest,
                    reader.reader_id(),
                    block,
                )
                .map_err(|error| invalid_block_error(error.slot, error.message))
            },
            |state, _, block| {
                let slot = block.slot;
                merge_single_read_discovery_block(state, &source, anchor_signature, block)
                    .map_err(|error| invalid_block_error(slot, error.to_string()))
            },
            |state, _| {
                finish_single_read_discovery_batch(state, &registry, generation_digest)
                    .map_err(|error| invalid_block_error(first_slot, error.to_string()))
            },
            |scratch, state, _, block| {
                let slot = block.header().slot;
                let table = state.target_table.as_ref().ok_or_else(|| {
                    invalid_block_error(slot, "single-read target table is absent".to_owned())
                })?;
                let anchor = (*state.anchor_position).ok_or_else(|| {
                    invalid_block_error(slot, "single-read mint anchor is absent".to_owned())
                })?;
                collect_matching_block(
                    &mut scratch.matching,
                    &state.writer,
                    input.epoch,
                    generation_digest,
                    source_wire_profile,
                    projector,
                    registry_entries,
                    table,
                    anchor,
                    block,
                )
                .map_err(|error| invalid_block_error(error.slot, error.message))
            },
            |state, _, block| {
                consume_single_read_matched_block(
                    state,
                    block,
                    &mut anchor_transactions,
                    &mut transactions_written,
                )
            },
        )?
    };
    state.stats.blocks = ordered_stats.block_count;
    state.stats.compressed_bytes = ordered_stats.compressed_bytes;
    source
        .verify_unchanged()
        .with_context(|| format!("verify epoch {} single-read source", input.epoch))?;
    let position = (*state.anchor_position).context("single-read mint anchor is absent")?;

    let SingleReadEpochCoordinator {
        epoch_creations,
        writer,
        stats,
        mut target_table,
        mut extractor_stats,
        ..
    } = state;
    let footer = TokenTransactionDumpFooter {
        epochs: 1,
        blocks_scanned: stats.blocks,
        transactions_scanned: stats.transactions,
        transactions_written,
        pubkeys: 0,
        signatures: 0,
        owned_block_fallbacks: stats.owned_block_fallbacks,
        raw_transaction_fallbacks: 0,
        raw_metadata_fallbacks: 0,
    };
    let mut writer = writer
        .into_inner()
        .map_err(|_| anyhow!("raw transaction writer mutex is poisoned"))?;
    ensure!(
        writer.first_error.is_none(),
        "raw transaction writer failed without stopping the scan"
    );
    writer.framed.write_with_scratch(
        &TokenTransactionDumpRecord::Footer(footer),
        &mut framing_scratch,
    )?;
    writer.framed.flush()?;
    drop(writer);
    sync_file(&stream_path)?;

    let target_finalize_started = Instant::now();
    let account_id_log = target_table
        .take()
        .context("single-read target table is absent after the scan")?
        .into_account_id_log();
    let account_id_path = shard_path.join(ACCOUNT_ID_LOG_FILE);
    write_synced_bytes(
        &account_id_path,
        &wincode::config::serialize(
            &account_id_log,
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        )?,
    )?;
    extractor_stats.target_finalize_time = target_finalize_started.elapsed();

    let manifest = DumpManifest {
        schema_version: DUMP_SCHEMA_VERSION,
        artifact_kind: DumpArtifactKind::RawEpochShard,
        complete: true,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        source_binding: dump_source_binding(config),
        first_epoch: input.epoch,
        last_epoch: input.epoch,
        transactions: transactions_written,
        signatures: None,
        pubkeys: None,
        transaction_stream: TRANSACTIONS_FILE.to_owned(),
        transaction_stream_sha256: Some(sha256_file(&stream_path)?),
        account_id_log: Some(ACCOUNT_ID_LOG_FILE.to_owned()),
        account_id_log_sha256: Some(sha256_file(&account_id_path)?),
        discovered_accounts: None,
        discovered_accounts_sha256: None,
        discovered_account_count: None,
        signature_stream: None,
        signature_stream_sha256: None,
        pubkey_registry: None,
        pubkey_registry_sha256: None,
        registry_maps: None,
    };
    let manifest_path = shard_path.join(DUMP_MANIFEST_FILE);
    write_synced_bytes(&manifest_path, &serde_json::to_vec_pretty(&manifest)?)?;
    sync_directory(shard_path)?;

    Ok(SingleReadEpochResult {
        discovery: EpochDiscoveryResult {
            log: EpochCreationLog {
                schema_version: DUMP_SCHEMA_VERSION,
                epoch: input.epoch,
                source_generation_digest: generation_digest,
                mint,
                entries: epoch_creations.into_values().collect(),
            },
            anchor_position: (input.epoch == position.epoch).then_some(position),
            stats,
        },
        shard: ShardSummary {
            transactions: transactions_written,
            compressed_bytes: stats.compressed_bytes,
            anchor_transactions,
        },
        reader: ordered_stats,
        extractor: extractor_stats,
        account_ids: account_id_log,
    })
}

fn write_frozen_epoch_shard(
    config: &ExtractConfig,
    input: &EpochInput,
    shard_path: &Path,
    mint: [u8; 32],
    anchor_signature: [u8; 64],
    accounts: &[DiscoveredAccount],
    anchor_position: SourceTransactionCoordinate,
) -> Result<ShardSummary> {
    let (source, reader) = open_epoch(input, config)?;
    let registry =
        VerifiedEpochRegistry::open(source.clone(), reader.manifest(), reader.registry_entries())?;
    let generation_digest = reader.binding().generation_digest;
    let source_wire_profile = dump_wire_profile(reader.message_projector().wire_profile());
    let (table, account_id_log, _) = EpochTargetTable::build(
        input.epoch,
        mint,
        anchor_position,
        accounts,
        &registry,
        generation_digest,
    )?;
    let account_id_path = shard_path.join(ACCOUNT_ID_LOG_FILE);
    let account_id_bytes = wincode::config::serialize(
        &account_id_log,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )?;
    write_synced_bytes(&account_id_path, &account_id_bytes)?;

    let stream_path = shard_path.join(TRANSACTIONS_FILE);
    let stream_file =
        File::create(&stream_path).with_context(|| format!("create {}", stream_path.display()))?;
    let mut framed = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(8 << 20, stream_file));
    let mut framing_scratch = Vec::with_capacity(2 << 20);
    framed.write_with_scratch(
        &TokenTransactionDumpRecord::Header(TokenTransactionDumpHeader {
            schema_version: DUMP_SCHEMA_VERSION,
            stream_kind: DumpStreamKind::RawEpochShard,
            mint,
            mint_slot: config.mint_slot,
            mint_signature: anchor_signature,
            source_epoch: Some(input.epoch),
            source_generation_digest: Some(generation_digest),
            source_wire_profile: Some(source_wire_profile),
            pubkey_registry_id_base: PUBKEY_REGISTRY_ID_BASE,
        }),
        &mut framing_scratch,
    )?;
    let writer = Mutex::new(SharedRawWriter {
        framed,
        first_error: None,
    });

    let first_slot = config.mint_slot.max(input.manifest.epoch_start_slot());
    let range = epoch_row_range(&reader, first_slot);
    let projector = reader.message_projector();
    let registry_entries = reader.registry_entries();
    let mut stats = EpochScanStats::default();
    let mut transactions_written = 0u64;
    let mut anchor_transactions = 0u64;
    let ordered_stats = reader.process_borrowed_blocks_parallel_ordered(
        range,
        ordered_config(config.workers, true),
        |_| MatchScratch::new(),
        |scratch, _, block| {
            collect_matching_block(
                scratch,
                &writer,
                input.epoch,
                generation_digest,
                source_wire_profile,
                projector,
                registry_entries,
                &table,
                anchor_position,
                block,
            )
            .map_err(|error| invalid_block_error(error.slot, error.message))
        },
        |_, block| {
            stats.transactions = stats
                .transactions
                .checked_add(block.transactions_scanned)
                .ok_or_else(|| {
                    invalid_block_error(block.slot, "transaction count overflow".to_owned())
                })?;
            stats.owned_block_fallbacks += u64::from(block.owned_fallback);
            anchor_transactions = anchor_transactions
                .checked_add(block.anchor_transactions)
                .ok_or_else(|| {
                    invalid_block_error(block.slot, "anchor transaction count overflow".to_owned())
                })?;
            transactions_written = transactions_written
                .checked_add(block.transactions_written)
                .ok_or_else(|| {
                    invalid_block_error(block.slot, "transaction write count overflow".to_owned())
                })?;
            Ok(())
        },
    )?;
    stats.blocks = ordered_stats.block_count;
    stats.compressed_bytes = ordered_stats.compressed_bytes;
    source
        .verify_unchanged()
        .with_context(|| format!("verify epoch {} raw-copy source", input.epoch))?;

    let footer = TokenTransactionDumpFooter {
        epochs: 1,
        blocks_scanned: stats.blocks,
        transactions_scanned: stats.transactions,
        transactions_written,
        pubkeys: 0,
        signatures: 0,
        owned_block_fallbacks: stats.owned_block_fallbacks,
        raw_transaction_fallbacks: 0,
        raw_metadata_fallbacks: 0,
    };
    let mut writer = writer
        .into_inner()
        .map_err(|_| anyhow!("raw transaction writer mutex is poisoned"))?;
    ensure!(
        writer.first_error.is_none(),
        "raw transaction writer failed without stopping the scan"
    );
    writer.framed.write_with_scratch(
        &TokenTransactionDumpRecord::Footer(footer),
        &mut framing_scratch,
    )?;
    writer.framed.flush()?;
    drop(writer);
    sync_file(&stream_path)?;

    let manifest = DumpManifest {
        schema_version: DUMP_SCHEMA_VERSION,
        artifact_kind: DumpArtifactKind::RawEpochShard,
        complete: true,
        mint: bs58::encode(mint).into_string(),
        mint_slot: config.mint_slot,
        mint_signature: bs58::encode(anchor_signature).into_string(),
        workers: config.workers,
        source_binding: dump_source_binding(config),
        first_epoch: input.epoch,
        last_epoch: input.epoch,
        transactions: transactions_written,
        signatures: None,
        pubkeys: None,
        transaction_stream: TRANSACTIONS_FILE.to_owned(),
        transaction_stream_sha256: Some(sha256_file(&stream_path)?),
        account_id_log: Some(ACCOUNT_ID_LOG_FILE.to_owned()),
        account_id_log_sha256: Some(sha256_file(&account_id_path)?),
        discovered_accounts: None,
        discovered_accounts_sha256: None,
        discovered_account_count: None,
        signature_stream: None,
        signature_stream_sha256: None,
        pubkey_registry: None,
        pubkey_registry_sha256: None,
        registry_maps: None,
    };
    let manifest_path = shard_path.join(DUMP_MANIFEST_FILE);
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)
        .with_context(|| format!("write {}", manifest_path.display()))?;
    sync_file(&manifest_path)?;
    sync_directory(shard_path)?;
    Ok(ShardSummary {
        transactions: transactions_written,
        compressed_bytes: stats.compressed_bytes,
        anchor_transactions,
    })
}

#[allow(clippy::too_many_arguments)]
fn collect_matching_block(
    scratch: &mut MatchScratch,
    writer: &Mutex<SharedRawWriter>,
    epoch: u64,
    generation_digest: [u8; 32],
    source_wire_profile: DumpWireProfile,
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    table: &EpochTargetTable,
    anchor_position: SourceTransactionCoordinate,
    block: BorrowedDecodedBlock<'_>,
) -> std::result::Result<MatchedBlock, BlockProjectionError> {
    collect_matching_block_impl(
        scratch,
        writer,
        epoch,
        generation_digest,
        source_wire_profile,
        projector,
        registry_entries,
        table,
        anchor_position,
        None,
        block,
    )
}

#[allow(clippy::too_many_arguments)]
fn collect_matching_block_with_hints(
    scratch: &mut MatchScratch,
    writer: &Mutex<SharedRawWriter>,
    epoch: u64,
    generation_digest: [u8; 32],
    source_wire_profile: DumpWireProfile,
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    table: &EpochTargetTable,
    anchor_position: SourceTransactionCoordinate,
    transaction_hints: &[u8],
    batch_hints_dirty: bool,
    block: BorrowedDecodedBlock<'_>,
) -> std::result::Result<MatchedBlock, BlockProjectionError> {
    collect_matching_block_impl(
        scratch,
        writer,
        epoch,
        generation_digest,
        source_wire_profile,
        projector,
        registry_entries,
        table,
        anchor_position,
        Some((transaction_hints, batch_hints_dirty)),
        block,
    )
}

#[allow(clippy::too_many_arguments)]
fn collect_matching_block_impl(
    scratch: &mut MatchScratch,
    writer: &Mutex<SharedRawWriter>,
    epoch: u64,
    generation_digest: [u8; 32],
    source_wire_profile: DumpWireProfile,
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    table: &EpochTargetTable,
    anchor_position: SourceTransactionCoordinate,
    match_hints: Option<(&[u8], bool)>,
    block: BorrowedDecodedBlock<'_>,
) -> std::result::Result<MatchedBlock, BlockProjectionError> {
    let slot = block.header().slot;
    let source_block_id = block.index_row.block_id;
    if let Some((hints, _)) = match_hints
        && hints.len() != block.tx_rows_len()
    {
        return Err(BlockProjectionError::new(
            slot,
            u32::MAX,
            "transaction match-hint count differs from storage transaction count",
        ));
    }
    let context = TokenTransactionBlockContext {
        slot,
        parent_slot: block.header().parent_slot,
        blockhash_id: block.header().blockhash_id,
        previous_blockhash_id: block.header().previous_blockhash_id,
        block_time: block.header().block_time,
        block_height: block.header().block_height,
        transaction_count: block.tx_count(),
    };
    let mut transactions_written = 0u64;
    let mut anchor_transactions = 0u64;
    let mut hint_direct_matches = 0u64;
    let mut hint_skips_without_decode = 0u64;
    let mut hint_exact_reparses = 0u64;
    let metadata_owned_fallbacks_before = scratch.metadata_owned_fallbacks;
    for (storage_index, located) in block.storage_transaction_rows().enumerate() {
        reject_opaque_flags(located.row.flags, located.row.tx_index, slot)?;
        let has_metadata = located.row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
        if has_metadata != (located.row.metadata_len != 0) {
            return Err(BlockProjectionError::new(
                slot,
                located.row.tx_index,
                "metadata presence flag disagrees with metadata byte length",
            ));
        }
        let message_bytes = checked_region(
            block.message_bytes(),
            located.row.message_offset,
            located.row.message_len,
            "message",
            located.row.tx_index,
            slot,
        )?;
        let metadata_bytes = if has_metadata {
            Some(checked_region(
                block.metadata_bytes(),
                located.row.metadata_offset,
                located.row.metadata_len,
                "metadata",
                located.row.tx_index,
                slot,
            )?)
        } else {
            None
        };
        let is_anchor = anchor_position.epoch == epoch
            && anchor_position.slot == slot
            && anchor_position.source_block_id == source_block_id
            && anchor_position.tx_index == located.row.tx_index;
        let account_candidate = if is_anchor && match_hints.is_some() {
            false
        } else if let Some((hints, dirty)) = match_hints {
            match hints[storage_index] {
                1 => {
                    hint_direct_matches = hint_direct_matches.checked_add(1).ok_or_else(|| {
                        BlockProjectionError::new(
                            slot,
                            located.row.tx_index,
                            "direct match-hint count overflow",
                        )
                    })?;
                    true
                }
                0 if !dirty => {
                    hint_skips_without_decode =
                        hint_skips_without_decode.checked_add(1).ok_or_else(|| {
                            BlockProjectionError::new(
                                slot,
                                located.row.tx_index,
                                "match-hint skip count overflow",
                            )
                        })?;
                    false
                }
                0 => {
                    hint_exact_reparses = hint_exact_reparses.checked_add(1).ok_or_else(|| {
                        BlockProjectionError::new(
                            slot,
                            located.row.tx_index,
                            "exact match-hint reparse count overflow",
                        )
                    })?;
                    transaction_account_list_matches(
                        scratch,
                        projector,
                        registry_entries,
                        located.row.flags,
                        located.row.signature_count,
                        message_bytes,
                        metadata_bytes,
                        |reference| {
                            table.reference_is_eligible(
                                reference,
                                slot,
                                source_block_id,
                                located.row.tx_index,
                            )
                        },
                    )
                    .map_err(|error| BlockProjectionError::new(slot, located.row.tx_index, error))?
                }
                value => {
                    return Err(BlockProjectionError::new(
                        slot,
                        located.row.tx_index,
                        format!("invalid transaction match hint {value}"),
                    ));
                }
            }
        } else {
            transaction_account_list_matches(
                scratch,
                projector,
                registry_entries,
                located.row.flags,
                located.row.signature_count,
                message_bytes,
                metadata_bytes,
                |reference| {
                    table.reference_is_eligible(
                        reference,
                        slot,
                        source_block_id,
                        located.row.tx_index,
                    )
                },
            )
            .map_err(|error| BlockProjectionError::new(slot, located.row.tx_index, error))?
        };
        let keep = is_anchor || account_candidate;
        if !keep {
            continue;
        }
        let first_ordinal = block
            .index_row
            .first_signature_ordinal
            .checked_add(u64::from(located.first_signature_offset))
            .ok_or_else(|| {
                BlockProjectionError::new(slot, located.row.tx_index, "signature ordinal overflow")
            })?;
        if is_anchor {
            if first_ordinal != anchor_position.source_first_signature_ordinal
                || located.row.signature_count != anchor_position.signature_count
            {
                return Err(BlockProjectionError::new(
                    slot,
                    located.row.tx_index,
                    "mint anchor signature reference changed after discovery",
                ));
            }
            anchor_transactions = anchor_transactions.checked_add(1).ok_or_else(|| {
                BlockProjectionError::new(slot, located.row.tx_index, "anchor count overflow")
            })?;
        }
        let borrowed_record = BorrowedRawDumpRecord::Transaction(BorrowedRawTransactionRecord {
            source_epoch: epoch,
            source_generation_digest: generation_digest,
            source_wire_profile,
            source_block_id,
            block: &context,
            tx_index: located.row.tx_index,
            flags: located.row.flags,
            source_first_signature_ordinal: first_ordinal,
            signature_count: located.row.signature_count,
            dump_signature_ordinal: None,
            message_bytes,
            metadata_bytes: metadata_bytes.unwrap_or_default(),
        });
        encode_with_scratch(&borrowed_record, &mut scratch.encoded_record)
            .map_err(|error| BlockProjectionError::new(slot, located.row.tx_index, error))?;
        writer
            .lock()
            .map_err(|_| {
                BlockProjectionError::new(
                    slot,
                    located.row.tx_index,
                    "raw transaction writer mutex is poisoned",
                )
            })?
            .write_encoded(&scratch.encoded_record)
            .map_err(|error| BlockProjectionError::new(slot, located.row.tx_index, error))?;
        transactions_written = transactions_written.checked_add(1).ok_or_else(|| {
            BlockProjectionError::new(slot, located.row.tx_index, "transaction count overflow")
        })?;
    }
    Ok(MatchedBlock {
        slot,
        transactions_scanned: u64::from(block.tx_count()),
        owned_fallback: block.uses_owned_fallback(),
        transactions_written,
        anchor_transactions,
        hint_direct_matches,
        hint_skips_without_decode,
        hint_exact_reparses,
        metadata_owned_fallbacks: scratch
            .metadata_owned_fallbacks
            .checked_sub(metadata_owned_fallbacks_before)
            .expect("metadata owned-fallback counter is monotonic"),
    })
}

#[allow(clippy::too_many_arguments)]
fn transaction_account_list_matches(
    scratch: &mut MatchScratch,
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    row_flags: u32,
    signature_count: u8,
    message_bytes: &[u8],
    metadata_bytes: Option<&[u8]>,
    mut matches: impl FnMut(CompactPubkey) -> bool,
) -> Result<bool> {
    let mut static_match = false;
    let message = projector.visit_static_accounts_exact(
        message_bytes,
        registry_entries,
        |_, reference| static_match |= matches(reference),
    )?;
    validate_message_summary(&message, row_flags, signature_count)?;
    let message_has_loaded_addresses =
        message.expected_loaded_writable != 0 || message.expected_loaded_readonly != 0;
    ensure!(
        message_has_loaded_addresses == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0),
        "message loaded-address lookups differ from transaction-row flags"
    );
    if static_match {
        return Ok(true);
    }
    if !message_has_loaded_addresses {
        return Ok(false);
    }
    let total_accounts = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("message account count overflow")?;
    let mut loaded_match = false;
    let mut used_metadata_owned_fallback = false;
    if let Some(source_bytes) = metadata_bytes {
        used_metadata_owned_fallback = source_bytes.first() != Some(&0);
        let exact = if used_metadata_owned_fallback {
            scratch.canonical_metadata =
                blockzilla_archive_v2::canonicalize_archive_v2_metadata_owned(source_bytes)?.0;
            visit_loaded_accounts_exact(
                &scratch.canonical_metadata,
                total_accounts,
                message.instruction_count,
                registry_entries,
                |reference| loaded_match |= matches(reference),
            )?
        } else {
            visit_loaded_accounts_exact(
                source_bytes,
                total_accounts,
                message.instruction_count,
                registry_entries,
                |reference| loaded_match |= matches(reference),
            )?
        };
        validate_metadata_summary(&exact, &message, row_flags)?;
    } else {
        validate_absent_metadata(&message, row_flags)?;
    }
    if used_metadata_owned_fallback {
        scratch.metadata_owned_fallbacks = scratch
            .metadata_owned_fallbacks
            .checked_add(1)
            .context("metadata owned-fallback count overflow")?;
    }
    Ok(static_match || loaded_match)
}

fn visit_loaded_accounts_exact(
    metadata_bytes: &[u8],
    total_message_accounts: usize,
    top_level_instruction_count: usize,
    registry_entries: u32,
    mut on_loaded: impl FnMut(CompactPubkey),
) -> Result<ProjectedArchiveV2TokenMetadataSummary> {
    Ok(visit_archive_v2_token_metadata_exact_ordered(
        metadata_bytes,
        ArchiveV2MetadataProjectionLimits {
            total_message_accounts,
            top_level_instruction_count,
        },
        registry_entries,
        // Pass B only needs the loaded-address lane. Selected transactions keep their exact source
        // metadata bytes, so log content is copied without ever being decoded.
        LogPayloadValidation::StructureOnly,
        |_, _| {},
        |_, _| {},
        |_, _, reference| on_loaded(reference),
    )?)
}

fn validate_message_summary(
    message: &ProjectedArchiveV2MessageAccountSummary,
    row_flags: u32,
    signature_count: u8,
) -> Result<()> {
    ensure!(signature_count != 0, "transaction row has no signatures");
    ensure!(
        message.num_required_signatures == signature_count,
        "message signature count differs from transaction row"
    );
    ensure!(
        message.is_v0 == (row_flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
        "message version differs from transaction row"
    );
    ensure!(
        message.has_compact_vote_instruction
            == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX != 0),
        "compact-vote presence differs from transaction row"
    );
    Ok(())
}

fn validate_metadata_summary(
    metadata: &ProjectedArchiveV2TokenMetadataSummary,
    message: &ProjectedArchiveV2MessageAccountSummary,
    row_flags: u32,
) -> Result<()> {
    let has_token_balances =
        metadata.pre_token_balance_count != 0 || metadata.post_token_balance_count != 0;
    let has_loaded_addresses =
        metadata.loaded_writable_count != 0 || metadata.loaded_readonly_count != 0;
    ensure!(
        metadata.has_error == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
            && metadata.inner_instructions_present
                == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0)
            && metadata.logs_present == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_LOGS != 0)
            && has_token_balances == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES != 0)
            && metadata.return_data_present
                == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA != 0),
        "typed metadata differs from transaction-row flags"
    );
    ensure!(
        metadata.pre_balance_count == metadata.post_balance_count
            && (metadata.pre_balance_count == 0
                || metadata.pre_balance_count >= message.minimum_balance_accounts),
        "metadata balance vectors cannot cover the writable message-account prefix"
    );
    ensure!(
        metadata.loaded_writable_count == message.expected_loaded_writable
            && metadata.loaded_readonly_count == message.expected_loaded_readonly
            && has_loaded_addresses == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0),
        "loaded addresses differ from message lookups or transaction-row flags"
    );
    Ok(())
}

fn validate_discovery_metadata_summary(
    metadata: &ProjectedArchiveV2TokenMetadataSummary,
    message: &ProjectedArchiveV2MessageAccountSummary,
    row_flags: u32,
) -> Result<()> {
    ensure!(
        !metadata.has_error,
        "successful discovery transaction has error metadata"
    );
    ensure!(
        metadata.inner_instructions_present == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0),
        "metadata inner-instruction presence differs from transaction-row flags"
    );
    ensure!(
        metadata.loaded_writable_count == message.expected_loaded_writable
            && metadata.loaded_readonly_count == message.expected_loaded_readonly,
        "metadata loaded-address counts differ from the message"
    );
    Ok(())
}

fn validate_absent_metadata(
    message: &ProjectedArchiveV2MessageAccountSummary,
    row_flags: u32,
) -> Result<()> {
    const METADATA_DERIVED_FLAGS: u32 = ARCHIVE_V2_TX_FLAG_HAS_ERROR
        | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
        | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
        | ARCHIVE_V2_TX_FLAG_HAS_LOGS
        | ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA
        | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
    ensure!(
        row_flags & METADATA_DERIVED_FLAGS == 0,
        "transaction row declares typed metadata facts without metadata"
    );
    ensure!(
        message.expected_loaded_writable == 0 && message.expected_loaded_readonly == 0,
        "v0 message needs loaded addresses but metadata is absent"
    );
    Ok(())
}

fn write_synced_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))?;
    sync_file(path)
}

#[cfg(any())]
fn project_block_facts(
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    generation_digest: [u8; 32],
    reader_id: u64,
    block: BorrowedDecodedBlock<'_>,
) -> std::result::Result<ProjectedBlockFacts, BlockProjectionError> {
    let slot = block.header().slot;
    let mut transactions = Vec::with_capacity(block.tx_rows_len());
    for located in block.transaction_row_order().canonical_rows() {
        reject_opaque_flags(located.row.flags, located.row.tx_index, slot)?;
        let has_metadata = located.row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
        if has_metadata != (located.row.metadata_len != 0) {
            return Err(BlockProjectionError::new(
                slot,
                located.row.tx_index,
                "metadata presence flag disagrees with metadata byte length",
            ));
        }
        let message_bytes = checked_region(
            block.message_bytes(),
            located.row.message_offset,
            located.row.message_len,
            "message",
            located.row.tx_index,
            slot,
        )?;
        let metadata_bytes = if has_metadata {
            Some(checked_region(
                block.metadata_bytes(),
                located.row.metadata_offset,
                located.row.metadata_len,
                "metadata",
                located.row.tx_index,
                slot,
            )?)
        } else {
            None
        };
        let facts = project_transaction_facts(
            projector,
            registry_entries,
            located.row.flags,
            located.row.signature_count,
            message_bytes,
            metadata_bytes,
        )
        .map_err(|error| BlockProjectionError::new(slot, located.row.tx_index, error))?;
        let first_ordinal = block
            .index_row
            .first_signature_ordinal
            .checked_add(u64::from(located.first_signature_offset))
            .ok_or_else(|| {
                BlockProjectionError::new(slot, located.row.tx_index, "signature ordinal overflow")
            })?;
        transactions.push(ProjectedTransactionFacts {
            tx_index: located.row.tx_index,
            signatures: SignatureReference {
                generation_digest,
                first_ordinal,
                count: located.row.signature_count,
            },
            facts,
        });
    }
    Ok(ProjectedBlockFacts {
        slot,
        source_block_id: block.index_row.block_id,
        owned_fallback: block.uses_owned_fallback(),
        transactions,
    })
}

#[cfg(any())]
fn project_transaction_facts(
    projector: ArchiveV2MessageProjector,
    registry_entries: u32,
    row_flags: u32,
    signature_count: u8,
    message_bytes: &[u8],
    metadata_bytes: Option<&[u8]>,
) -> Result<CompactTransactionFacts> {
    let mut outer = Vec::<IndexedInstruction>::new();
    let message = projector.project(
        message_bytes,
        |instruction: BorrowedArchiveV2Instruction<'_>| {
            let outer_instruction_index = outer.len();
            outer.push(IndexedInstruction {
                outer_instruction_index,
                program_id_index: usize::from(instruction.program_id_index),
                accounts: instruction.accounts.to_vec(),
                data: instruction.raw_data.unwrap_or_default().to_vec(),
            });
        },
    )?;
    let total_accounts = message
        .account_keys
        .len()
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("message account count overflow")?;
    for reference in message
        .account_keys
        .iter()
        .chain(&message.address_table_keys)
    {
        validate_compact_pubkey(*reference, registry_entries)?;
    }

    ensure!(
        message.is_v0 == (row_flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
        "message version disagrees with transaction-row flags"
    );
    ensure!(
        message.num_required_signatures == signature_count,
        "message requires {} signatures but transaction row declares {signature_count}",
        message.num_required_signatures
    );
    ensure!(
        message.has_compact_vote_instruction
            == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX != 0),
        "compact-vote presence disagrees with transaction-row flags"
    );
    if let Some(bytes) = metadata_bytes {
        let exact = validate_archive_v2_metadata_exact(
            bytes,
            ArchiveV2MetadataProjectionLimits {
                total_message_accounts: total_accounts,
                top_level_instruction_count: message.instruction_count,
            },
            registry_entries,
        )?;
        ensure!(
            exact.has_error == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
                && exact.inner_instructions_present
                    == (row_flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0)
                && exact.logs_present == Some(row_flags & ARCHIVE_V2_TX_FLAG_HAS_LOGS != 0)
                && exact.token_balances_present
                    == Some(row_flags & ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES != 0)
                && exact.return_data_present
                    == Some(row_flags & ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA != 0),
            "typed metadata disagrees with transaction-row flags"
        );
        ensure!(
            exact.pre_balance_count == exact.post_balance_count
                && (exact.pre_balance_count == 0
                    || exact.pre_balance_count >= message.minimum_balance_accounts),
            "metadata balance vectors cannot cover the writable message-account prefix"
        );
        let (loaded_writable, loaded_readonly) = exact
            .loaded_addresses
            .context("exact metadata validation did not return loaded addresses")?;
        let loaded_are_absent = loaded_writable.is_empty() && loaded_readonly.is_empty();
        ensure!(
            loaded_writable.len() == message.expected_loaded_writable
                && loaded_readonly.len() == message.expected_loaded_readonly
                && (row_flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0) != loaded_are_absent,
            "loaded addresses disagree with message lookups or transaction-row flags"
        );
    } else {
        const METADATA_DERIVED_FLAGS: u32 = ARCHIVE_V2_TX_FLAG_HAS_ERROR
            | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
            | ARCHIVE_V2_TX_FLAG_HAS_LOGS
            | ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA
            | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
        ensure!(
            row_flags & METADATA_DERIVED_FLAGS == 0,
            "transaction row declares typed metadata facts without metadata"
        );
    }

    let metadata = match metadata_bytes {
        Some(bytes) if bytes.first() == Some(&0) => project_current_metadata_facts(
            bytes,
            total_accounts,
            message.instruction_count,
            registry_entries,
        )?,
        Some(bytes) => project_owned_metadata_facts(bytes)?,
        None => {
            ensure!(
                message.expected_loaded_writable == 0 && message.expected_loaded_readonly == 0,
                "v0 message needs loaded addresses but metadata is absent"
            );
            MetadataFacts::default()
        }
    };
    ensure!(
        metadata.loaded_writable.len() == message.expected_loaded_writable
            && metadata.loaded_readonly.len() == message.expected_loaded_readonly,
        "loaded-address counts differ from message lookups"
    );

    let mut compact_accounts = Vec::with_capacity(total_accounts);
    for reference in message
        .account_keys
        .iter()
        .chain(&metadata.loaded_writable)
        .chain(&metadata.loaded_readonly)
    {
        validate_compact_pubkey(*reference, registry_entries)?;
        compact_accounts.push(*reference);
    }
    ensure!(
        compact_accounts.len() == total_accounts,
        "compact message account count differs from projection"
    );

    let execution_order = interleave_indexed_instructions(outer, metadata.inner)?;

    let mut instructions = Vec::with_capacity(execution_order.len());
    for instruction in execution_order {
        let program_id = compact_accounts
            .get(instruction.program_id_index)
            .copied()
            .context("instruction program index is outside message accounts")?;
        let accounts = instruction
            .accounts
            .iter()
            .map(|index| {
                compact_accounts
                    .get(usize::from(*index))
                    .copied()
                    .context("instruction account index is outside message accounts")
            })
            .collect::<Result<Vec<_>>>()?;
        instructions.push(CompactInstructionFact {
            program_id,
            accounts,
            data: instruction.data,
        });
    }

    Ok(CompactTransactionFacts {
        has_error: metadata.has_error,
        instructions,
    })
}

#[cfg(any())]
fn interleave_indexed_instructions(
    outer: Vec<IndexedInstruction>,
    inner: Vec<IndexedInstruction>,
) -> Result<Vec<IndexedInstruction>> {
    let instruction_capacity = outer.len() + inner.len();
    let mut inner_by_outer = (0..outer.len()).map(|_| Vec::new()).collect::<Vec<_>>();
    for instruction in inner {
        inner_by_outer
            .get_mut(instruction.outer_instruction_index)
            .context("inner instruction group is outside top-level instructions")?
            .push(instruction);
    }
    let mut execution_order = Vec::with_capacity(instruction_capacity);
    for (outer_index, instruction) in outer.into_iter().enumerate() {
        execution_order.push(instruction);
        execution_order.append(&mut inner_by_outer[outer_index]);
    }
    Ok(execution_order)
}

#[cfg(any())]
fn project_current_metadata_facts(
    bytes: &[u8],
    total_accounts: usize,
    instruction_count: usize,
    registry_entries: u32,
) -> Result<MetadataFacts> {
    let mut inner = Vec::new();
    let projected = project_archive_v2_token_metadata_exact_ordered(
        bytes,
        ArchiveV2MetadataProjectionLimits {
            total_message_accounts: total_accounts,
            top_level_instruction_count: instruction_count,
        },
        registry_entries,
        |outer_instruction_index, instruction: BorrowedArchiveV2InnerTokenInstruction<'_>| {
            inner.push(IndexedInstruction {
                outer_instruction_index: outer_instruction_index as usize,
                program_id_index: instruction.program_id_index as usize,
                accounts: instruction.accounts.to_vec(),
                data: instruction.data.to_vec(),
            });
        },
        |_, _| {},
    )?;
    let mut metadata = MetadataFacts {
        has_error: projected.has_error,
        loaded_writable: projected.loaded_addresses.0,
        loaded_readonly: projected.loaded_addresses.1,
        inner,
        ..MetadataFacts::default()
    };
    Ok(metadata)
}

#[cfg(any())]
fn project_owned_metadata_facts(bytes: &[u8]) -> Result<MetadataFacts> {
    let metadata = decode_owned_metadata(bytes)?;
    let mut facts = MetadataFacts {
        has_error: metadata.err.is_some(),
        loaded_writable: metadata.loaded_writable_addresses.clone(),
        loaded_readonly: metadata.loaded_readonly_addresses.clone(),
        ..MetadataFacts::default()
    };
    for group in metadata.inner_instructions.iter().flatten() {
        for instruction in &group.instructions {
            facts.inner.push(IndexedInstruction {
                outer_instruction_index: group.index as usize,
                program_id_index: instruction.program_id_index as usize,
                accounts: instruction.accounts.clone(),
                data: instruction.data.clone(),
            });
        }
    }
    Ok(facts)
}

#[cfg(any())]
fn collect_selected_block(
    epoch: u64,
    selected: Option<&BTreeSet<u32>>,
    generation_digest: [u8; 32],
    source_wire_profile: DumpWireProfile,
    block: BorrowedDecodedBlock<'_>,
) -> std::result::Result<Vec<PendingRecord>, BlockProjectionError> {
    let Some(selected) = selected else {
        return Ok(Vec::new());
    };
    let slot = block.header().slot;
    let context = TokenTransactionBlockContext {
        slot,
        parent_slot: block.header().parent_slot,
        blockhash_id: block.header().blockhash_id,
        previous_blockhash_id: block.header().previous_blockhash_id,
        block_time: block.header().block_time,
        block_height: block.header().block_height,
        transaction_count: block.tx_count(),
    };
    let mut records = Vec::with_capacity(selected.len());
    for located in block.transaction_row_order().canonical_rows() {
        if !selected.contains(&located.row.tx_index) {
            continue;
        }
        reject_opaque_flags(located.row.flags, located.row.tx_index, slot)?;
        let message_bytes = checked_region(
            block.message_bytes(),
            located.row.message_offset,
            located.row.message_len,
            "message",
            located.row.tx_index,
            slot,
        )?;
        let metadata_bytes = if located.row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0 {
            checked_region(
                block.metadata_bytes(),
                located.row.metadata_offset,
                located.row.metadata_len,
                "metadata",
                located.row.tx_index,
                slot,
            )?
            .to_vec()
        } else {
            Vec::new()
        };
        let first_ordinal = block
            .index_row
            .first_signature_ordinal
            .checked_add(u64::from(located.first_signature_offset))
            .ok_or_else(|| {
                BlockProjectionError::new(slot, located.row.tx_index, "signature ordinal overflow")
            })?;
        records.push(PendingRecord {
            record: TokenTransactionRecord {
                source_epoch: epoch,
                source_generation_digest: generation_digest,
                source_wire_profile,
                source_block_id: block.index_row.block_id,
                block: context.clone(),
                tx_index: located.row.tx_index,
                flags: located.row.flags,
                source_first_signature_ordinal: first_ordinal,
                signature_count: located.row.signature_count,
                dump_signature_ordinal: None,
                message_bytes: message_bytes.to_vec(),
                metadata_bytes,
            },
        });
    }
    if records.len() != selected.len() {
        return Err(BlockProjectionError::new(
            slot,
            u32::MAX,
            format!(
                "selected {} transactions but decoded {}",
                selected.len(),
                records.len()
            ),
        ));
    }
    Ok(records)
}

#[cfg(any())]
fn decode_owned_metadata(bytes: &[u8]) -> Result<CompactMetaV1> {
    let canonical;
    let bytes = if bytes.first() == Some(&0) {
        bytes
    } else {
        canonical = blockzilla_archive_v2::canonicalize_archive_v2_metadata_owned(bytes)?.0;
        &canonical
    };
    wincode::config::deserialize_exact(
        bytes,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )
    .map_err(Into::into)
}

fn validate_source_mode(config: &ExtractConfig) -> Result<()> {
    let ExtractSourceMode::TrustedLocal {
        cluster_id,
        slots_per_epoch,
        ..
    } = &config.source_mode;
    ensure!(!cluster_id.is_empty(), "trusted-local cluster ID is empty");
    ensure!(
        *slots_per_epoch != 0,
        "trusted-local slots per epoch must not be zero"
    );
    Ok(())
}

fn dump_source_binding(config: &ExtractConfig) -> DumpSourceBinding {
    let ExtractSourceMode::TrustedLocal {
        cluster_id,
        slots_per_epoch,
        wire_profile,
    } = &config.source_mode;
    DumpSourceBinding::TrustedLocalSizesOnly {
        cluster_id: cluster_id.clone(),
        slots_per_epoch: *slots_per_epoch,
        wire_profile: dump_wire_profile(*wire_profile),
    }
}

fn dump_wire_profile(profile: ArchiveV2WireProfile) -> DumpWireProfile {
    match profile {
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
            DumpWireProfile::PostUnknownInstructionFallbacksV1
        }
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
            DumpWireProfile::PreUnknownInstructionFallbacksV1
        }
    }
}

fn discover_epochs(config: &ExtractConfig) -> Result<Vec<EpochInput>> {
    let mut inputs = Vec::new();
    let archive_root = if matches!(&config.source_mode, ExtractSourceMode::TrustedLocal { .. }) {
        fs::canonicalize(&config.archive_root).with_context(|| {
            format!(
                "resolve trusted-local archive root {}",
                config.archive_root.display()
            )
        })?
    } else {
        config.archive_root.clone()
    };
    for entry in fs::read_dir(&archive_root)
        .with_context(|| format!("read archive root {}", archive_root.display()))?
    {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(epoch) = name
            .strip_prefix("epoch-")
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        ensure!(
            file_type.is_dir() && !file_type.is_symlink(),
            "epoch path {} is not a direct directory",
            entry.path().display()
        );
        if config.last_epoch.is_some_and(|last| epoch > last) {
            continue;
        }
        let path = entry.path();
        let (manifest, trusted_metadata_admission) = {
            let ExtractSourceMode::TrustedLocal {
                cluster_id,
                slots_per_epoch,
                wire_profile,
            } = &config.source_mode;
            let epoch_end_slot = epoch
                .checked_mul(*slots_per_epoch)
                .and_then(|start| start.checked_add(*slots_per_epoch - 1))
                .context("trusted-local epoch slot range overflow")?;
            if epoch_end_slot < config.mint_slot {
                continue;
            }
            let source = PinnedLocalRangeSource::open_directory(&path)
                .with_context(|| format!("open descriptor-rooted trusted-local epoch {epoch}"))?;
            let (reader, admission) = open_trusted_epoch_source(
                source.clone(),
                epoch,
                cluster_id,
                *slots_per_epoch,
                *wire_profile,
            )
            .with_context(|| format!("validate trusted-local epoch {epoch}"))?;
            let manifest = reader.manifest().clone();
            source
                .verify_unchanged()
                .with_context(|| format!("verify trusted-local epoch {epoch} did not change"))?;
            (manifest, Some(admission))
        };
        if manifest.epoch_end_slot() >= config.mint_slot {
            inputs.push(EpochInput {
                epoch,
                path,
                manifest,
                trusted_metadata_admission,
            });
        }
    }
    inputs.sort_unstable_by_key(|input| input.epoch);
    ensure!(
        !inputs.is_empty(),
        "no complete epoch covers mint slot {}",
        config.mint_slot
    );
    ensure!(
        inputs[0].manifest.epoch_start_slot() <= config.mint_slot,
        "first discovered epoch starts after mint slot {}",
        config.mint_slot
    );
    let cluster = inputs[0].manifest.cluster_id.clone();
    let slots_per_epoch = inputs[0].manifest.slots_per_epoch;
    for input in &inputs {
        ensure!(
            input.manifest.cluster_id == cluster,
            "epoch {} belongs to a different cluster",
            input.epoch
        );
        ensure!(
            input.manifest.slots_per_epoch == slots_per_epoch,
            "epoch {} uses a different slots-per-epoch value",
            input.epoch
        );
    }
    for pair in inputs.windows(2) {
        ensure!(
            pair[1].epoch == pair[0].epoch + 1,
            "archive epoch gap between {} and {}",
            pair[0].epoch,
            pair[1].epoch
        );
    }
    Ok(inputs)
}

fn open_epoch(
    input: &EpochInput,
    config: &ExtractConfig,
) -> Result<(
    PinnedLocalRangeSource,
    ArchiveReader<PinnedLocalRangeSource>,
)> {
    let source = PinnedLocalRangeSource::open_directory(&input.path)
        .with_context(|| format!("open descriptor-rooted trusted-local epoch {}", input.epoch))?;
    let ExtractSourceMode::TrustedLocal {
        cluster_id,
        slots_per_epoch,
        wire_profile,
    } = &config.source_mode;
    let (reader, admission) = open_trusted_epoch_source(
        source.clone(),
        input.epoch,
        cluster_id,
        *slots_per_epoch,
        *wire_profile,
    )
    .with_context(|| format!("open Compact V2 epoch {}", input.epoch))?;
    ensure!(
        input.trusted_metadata_admission == Some(admission),
        "epoch {} trusted-local metadata admission changed after discovery",
        input.epoch
    );
    ensure!(
        reader.manifest().epoch == input.epoch,
        "opened epoch differs"
    );
    ensure!(
        reader.manifest().generation_digest == input.manifest.generation_digest,
        "epoch {} changed after discovery",
        input.epoch
    );
    ensure!(
        reader.signatures_available(),
        "epoch {} has no signatures file",
        input.epoch
    );
    ensure!(
        reader
            .manifest()
            .required_file(blockzilla_read_sdk::manifest::REGISTRY_INDEX_FILE)?
            .size
            != 0,
        "epoch {} has no non-empty registry.mphf",
        input.epoch
    );
    Ok((source, reader))
}

fn open_trusted_epoch_source(
    source: PinnedLocalRangeSource,
    epoch: u64,
    cluster_id: &str,
    slots_per_epoch: u64,
    wire_profile: ArchiveV2WireProfile,
) -> Result<(
    ArchiveReader<PinnedLocalRangeSource>,
    TrustedLocalMetadataAdmission,
)> {
    let (admission, published_manifest) = inspect_trusted_local_metadata_admission(
        &source,
        epoch,
        cluster_id,
        slots_per_epoch,
        wire_profile,
    )?;
    let reader = match published_manifest {
        Some(manifest) => ArchiveReader::open_candidate(
            source,
            manifest,
            OpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..OpenOptions::default()
            },
        )?,
        None => open_historical_trusted_epoch_source(
            source,
            epoch,
            cluster_id,
            slots_per_epoch,
            wire_profile,
        )?,
    };
    Ok((reader, admission))
}

pub(crate) fn inspect_trusted_local_metadata_admission(
    source: &PinnedLocalRangeSource,
    epoch: u64,
    cluster_id: &str,
    slots_per_epoch: u64,
    wire_profile: ArchiveV2WireProfile,
) -> Result<(TrustedLocalMetadataAdmission, Option<GenerationManifest>)> {
    let inventory = source
        .inventory()
        .context("inventory trusted-local epoch controls through its directory descriptor")?;
    let metadata_markers = inventory
        .iter()
        .filter(|entry| {
            entry
                .name
                .as_encoded_bytes()
                .starts_with(METADATA_SCHEMA_MARKER_PREFIX)
        })
        .collect::<Vec<_>>();
    for marker in &metadata_markers {
        ensure!(
            marker.kind == PinnedLocalEntryKind::RegularFile,
            "trusted-local metadata marker {} is not a regular file",
            marker.name.to_string_lossy()
        );
    }
    ensure!(
        metadata_markers.len() <= 1,
        "trusted-local epoch has conflicting metadata markers: {}",
        metadata_markers
            .iter()
            .map(|entry| entry.name.to_string_lossy())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if let Some(marker) = metadata_markers.first() {
        ensure!(
            marker.name.as_encoded_bytes() == CURRENT_TYPED_ERRORS_MARKER_FILE.as_bytes(),
            "trusted-local epoch has unsupported metadata marker {}",
            marker.name.to_string_lossy()
        );
    }
    let current_marker_size = source
        .size(CURRENT_TYPED_ERRORS_MARKER_FILE)
        .context("pin trusted-local current metadata marker presence")?;
    ensure!(
        current_marker_size == metadata_markers.first().map(|entry| entry.bytes),
        "trusted-local current metadata marker changed during control inventory"
    );

    let manifest_entry = inventory
        .iter()
        .find(|entry| entry.name.as_encoded_bytes() == GENERATION_MANIFEST_FILE.as_bytes());
    if let Some(entry) = manifest_entry {
        ensure!(
            entry.kind == PinnedLocalEntryKind::RegularFile,
            "trusted-local generation manifest is not a regular file"
        );
    }
    let manifest_size = source
        .size(GENERATION_MANIFEST_FILE)
        .context("pin trusted-local generation manifest presence")?;
    ensure!(
        manifest_size == manifest_entry.map(|entry| entry.bytes),
        "trusted-local generation manifest changed during control inventory"
    );
    let manifest = manifest_entry
        .map(|_| {
            let bytes = source
                .read_all_bounded(GENERATION_MANIFEST_FILE, MAX_TRUSTED_LOCAL_MANIFEST_BYTES)
                .context(
                    "read trusted-local generation manifest through its directory descriptor",
                )?;
            GenerationManifest::parse(&bytes).context("validate trusted-local generation manifest")
        })
        .transpose()?;

    if metadata_markers.is_empty() {
        if let Some(manifest) = &manifest {
            let metadata_profile = ArchiveV2MetadataWireProfile::for_manifest(
                manifest,
                ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
            )
            .context("validate metadata bindings in the unmarked trusted-local manifest")?;
            ensure!(
                metadata_profile == ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
                "trusted-local manifest claims current metadata but the exact physical marker is absent"
            );
        }
        return Ok((
            TrustedLocalMetadataAdmission::UnmarkedHistoricalCompatibility,
            None,
        ));
    }

    let manifest = manifest.context(
        "trusted-local current metadata marker exists without archive-v2-generation.json",
    )?;
    ensure!(
        manifest.complete,
        "trusted-local published epoch is incomplete"
    );
    ensure!(
        manifest.epoch == epoch,
        "trusted-local published manifest epoch {} differs from directory epoch {epoch}",
        manifest.epoch
    );
    ensure!(
        manifest.cluster_id == cluster_id,
        "trusted-local published manifest cluster {:?} differs from asserted cluster {:?}",
        manifest.cluster_id,
        cluster_id
    );
    ensure!(
        manifest.slots_per_epoch == slots_per_epoch,
        "trusted-local published manifest slots_per_epoch {} differs from asserted {}",
        manifest.slots_per_epoch,
        slots_per_epoch
    );
    let selected_wire_profile = ArchiveV2WireProfile::for_published_manifest(&manifest)
        .context("validate trusted-local published message wire-profile binding")?;
    ensure!(
        selected_wire_profile == wire_profile,
        "trusted-local published message wire profile {selected_wire_profile} differs from asserted {wire_profile}"
    );
    let metadata_profile = ArchiveV2MetadataWireProfile::for_manifest(
        &manifest,
        ArchiveV2MetadataProfileAdmission::RequireCurrentTypedErrors,
    )
    .context("validate trusted-local published metadata wire-profile binding")?;
    ensure!(
        metadata_profile == ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1,
        "trusted-local published manifest did not select current typed errors"
    );

    let physical_metadata_marker = source
        .read_all_bounded(
            CURRENT_TYPED_ERRORS_MARKER_FILE,
            CURRENT_TYPED_ERRORS_MARKER_BYTES.len(),
        )
        .context("read trusted-local current metadata marker")?;
    ensure!(
        physical_metadata_marker == CURRENT_TYPED_ERRORS_MARKER_BYTES,
        "trusted-local current metadata marker bytes are not canonical"
    );
    let message_marker = wire_profile_marker(wire_profile);
    let expected_message_marker_bytes = wire_profile_marker_bytes(wire_profile);
    let physical_message_marker = source
        .read_all_bounded(&message_marker.name, expected_message_marker_bytes.len())
        .with_context(|| {
            format!(
                "read trusted-local message wire-profile marker {}",
                message_marker.name
            )
        })?;
    ensure!(
        physical_message_marker == expected_message_marker_bytes,
        "trusted-local message wire-profile marker {} bytes are not canonical",
        message_marker.name
    );

    Ok((
        TrustedLocalMetadataAdmission::PublishedCurrentTypedErrors,
        Some(manifest),
    ))
}

fn open_historical_trusted_epoch_source(
    source: PinnedLocalRangeSource,
    epoch: u64,
    cluster_id: &str,
    slots_per_epoch: u64,
    wire_profile: ArchiveV2WireProfile,
) -> blockzilla_read_sdk::Result<ArchiveReader<PinnedLocalRangeSource>> {
    ArchiveReader::open_trusted_with_additional_files_and_metadata_profile(
        source,
        TrustedGenerationIdentity {
            cluster_id: cluster_id.to_owned(),
            epoch,
            generation_id: "token-transaction-dump-trusted-local-sizes-v1".to_owned(),
            slots_per_epoch,
            wire_profile,
        },
        &[
            blockzilla_read_sdk::manifest::SIGNATURES_FILE,
            blockzilla_read_sdk::manifest::REGISTRY_INDEX_FILE,
        ],
        &[
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
            ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
        ],
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )
}

fn epoch_row_range(
    reader: &ArchiveReader<PinnedLocalRangeSource>,
    first_slot: u64,
) -> Range<usize> {
    let start = reader
        .index()
        .rows
        .partition_point(|row| row.slot < first_slot);
    start..reader.index().rows.len()
}

fn exact_probe_row_range(
    rows: &[blockzilla_archive_v2::ArchiveV2HotBlockIndexRow],
    start_slot: u64,
    expected_start_row: Option<usize>,
    max_blocks: usize,
) -> Result<Range<usize>> {
    ensure!(max_blocks != 0, "max blocks must not be zero");
    let start = rows.partition_point(|row| row.slot < start_slot);
    let row = rows
        .get(start)
        .with_context(|| format!("start slot {start_slot} is after the final indexed block"))?;
    ensure!(
        row.slot == start_slot,
        "start slot {start_slot} is skipped; next indexed block is row {start} at slot {}",
        row.slot
    );
    if let Some(expected) = expected_start_row {
        ensure!(
            start == expected,
            "start slot {start_slot} is row {start}, not expected row {expected}"
        );
    }
    let end = start.saturating_add(max_blocks).min(rows.len());
    Ok(start..end)
}

fn wire_profile_name(profile: ArchiveV2WireProfile) -> &'static str {
    match profile {
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::POST_UNKNOWN_NAME
        }
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PRE_UNKNOWN_NAME
        }
    }
}

fn probe_reader_stats(stats: OrderedParallelBlockStats) -> ProbeReaderStats {
    ProbeReaderStats {
        block_count: stats.block_count,
        batch_count: stats.batch_count,
        read_call_count: stats.read_call_count,
        compressed_bytes: stats.compressed_bytes,
        producer_read_seconds: stats.producer_read_wall_time.as_secs_f64(),
        decode_and_project_seconds: stats.coordinator_decode_project_wall_time.as_secs_f64(),
        producer_wait_for_buffer_seconds: stats.producer_wait_for_free_buffer_time.as_secs_f64(),
        coordinator_wait_for_batch_seconds: stats
            .coordinator_wait_for_ready_batch_time
            .as_secs_f64(),
        max_compressed_batch_bytes: stats.max_compressed_batch_bytes,
        max_declared_uncompressed_batch_bytes: stats.max_declared_uncompressed_batch_bytes,
    }
}

fn ordered_config(workers: usize, discard_rewards: bool) -> OrderedParallelBlockConfig {
    let retained = MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES / workers;
    OrderedParallelBlockConfig {
        compressed_batch_target_bytes: 16 << 20,
        uncompressed_batch_budget_bytes: 512 << 20,
        max_blocks_per_batch: 8_192,
        compressed_buffer_count: 3,
        decode_workers: workers,
        retained_decompressed_bytes_per_worker: retained.min(64 << 20),
        discard_rewards,
    }
}

fn reject_opaque_flags(
    flags: u32,
    tx_index: u32,
    slot: u64,
) -> std::result::Result<(), BlockProjectionError> {
    if flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return Err(BlockProjectionError::new(
            slot,
            tx_index,
            "raw transaction fallback cannot be decoded completely",
        ));
    }
    if flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        return Err(BlockProjectionError::new(
            slot,
            tx_index,
            "raw metadata fallback cannot be decoded completely",
        ));
    }
    Ok(())
}

fn checked_region<'a>(
    bytes: &'a [u8],
    offset: u32,
    length: u32,
    label: &str,
    tx_index: u32,
    slot: u64,
) -> std::result::Result<&'a [u8], BlockProjectionError> {
    let start = offset as usize;
    let end = start.checked_add(length as usize).ok_or_else(|| {
        BlockProjectionError::new(slot, tx_index, format!("{label} range overflow"))
    })?;
    bytes.get(start..end).ok_or_else(|| {
        BlockProjectionError::new(
            slot,
            tx_index,
            format!(
                "{label} range {start}..{end} is outside {} bytes",
                bytes.len()
            ),
        )
    })
}

#[cfg(any())]
fn resolve_account_index(accounts: &[CompactPubkey], index: u32) -> Result<CompactPubkey> {
    accounts
        .get(index as usize)
        .copied()
        .with_context(|| format!("token balance account index {index} is outside message accounts"))
}

#[cfg(any())]
fn validate_compact_pubkey(reference: CompactPubkey, registry_entries: u32) -> Result<()> {
    if let CompactPubkey::Id(id) = reference {
        ensure!(
            id != 0 && id <= registry_entries,
            "CompactPubkey ID {id} is outside 1..={registry_entries}"
        );
    }
    Ok(())
}

#[cfg(any())]
fn selected_transaction_count(selected: &BTreeMap<usize, BTreeSet<u32>>) -> u64 {
    selected.values().map(|rows| rows.len() as u64).sum()
}

fn prepare_two_pass_extraction_directories(
    config: &ExtractConfig,
    discoveries_root: &Path,
    shard_root: &Path,
    first_epoch: u64,
) -> Result<Vec<String>> {
    if !config.resume {
        prepare_empty_directory(&config.output, "raw extraction output")?;
        fs::create_dir(discoveries_root)
            .with_context(|| format!("create {}", discoveries_root.display()))?;
        fs::create_dir(shard_root).with_context(|| format!("create {}", shard_root.display()))?;
        sync_directory(&config.output)?;
        return Ok(Vec::new());
    }

    let mut notes = Vec::new();
    match fs::symlink_metadata(&config.output) {
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir(),
            "resume output {} is not a direct directory",
            config.output.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(&config.output)
                .with_context(|| format!("create resume output {}", config.output.display()))?;
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect resume output {}", config.output.display()));
        }
    }
    ensure_or_create_direct_directory(&config.output, discoveries_root, "discovery shard root")?;
    ensure_or_create_direct_directory(&config.output, shard_root, "raw shard root")?;

    if let Some(path) = quarantine_pending_resume_checkpoint(&config.output)? {
        notes.push(format!(
            "preserved pending checkpoint as {}",
            path.display()
        ));
    }
    for file_name in [ACCOUNTS_FILE, DUMP_MANIFEST_FILE] {
        if let Some(path) = quarantine_partial_artifact_file(&config.output, file_name)? {
            notes.push(format!(
                "preserved partial {file_name} as {}",
                path.display()
            ));
        }
    }
    for (label, root) in [("discovery", discoveries_root), ("raw", shard_root)] {
        let layout = discover_resume_shard_layout(root, first_epoch)?;
        if let Some((epoch, _)) = layout.partial {
            let path = quarantine_partial_shard(root, epoch)?
                .context("partial artifact disappeared before quarantine")?;
            notes.push(format!(
                "preserved partial {label} epoch {epoch} as {}",
                path.display()
            ));
        }
    }
    Ok(notes)
}

fn ensure_or_create_direct_directory(parent: &Path, path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir(),
            "{label} {} is not a direct directory",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).with_context(|| format!("create {label} {}", path.display()))?;
            sync_directory(parent)?;
        }
        Err(error) => {
            return Err(error).with_context(|| format!("inspect {label} {}", path.display()));
        }
    }
    Ok(())
}

fn validate_resume_discoveries(
    config: &ExtractConfig,
    inputs: &[EpochInput],
    shard_paths: &[(u64, PathBuf)],
) -> Result<(Vec<ResumeDiscoveryBinding>, DiscoveredAccountMap)> {
    let mut bindings = Vec::with_capacity(shard_paths.len());
    let mut accounts = BTreeMap::new();
    for (epoch, path) in shard_paths {
        let input = input_for_epoch(inputs, *epoch)?;
        let (binding, log) = validate_resume_discovery(config, input, path, None)
            .with_context(|| format!("validate resumed epoch {epoch} discovery"))?;
        merge_discovery_accounts(&mut accounts, &log);
        bindings.push(binding);
    }
    Ok((bindings, accounts))
}

fn validate_checkpoint_discovery(
    config: &ExtractConfig,
    epoch: u64,
    directory: &Path,
    expected: &ResumeDiscoveryBinding,
    slots_per_epoch: u64,
) -> Result<EpochCreationLog> {
    ensure_exact_directory_files(directory, &[CREATIONS_FILE])?;
    let path = directory.join(CREATIONS_FILE);
    let log: EpochCreationLog = wincode::config::deserialize_exact(
        &read_bounded_regular_file(&path, 1 << 30)?,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )
    .with_context(|| format!("decode {}", path.display()))?;
    let expected_mint = parse_pubkey(&config.mint, "mint")?;
    ensure!(
        log.schema_version == DUMP_SCHEMA_VERSION
            && log.epoch == epoch
            && log.mint == expected_mint
            && hex_digest(log.source_generation_digest) == expected.source_generation_digest,
        "epoch {epoch} discovery header differs from its completed checkpoint"
    );
    ensure!(
        sha256_file(&path)? == expected.creation_log_sha256
            && u64::try_from(log.entries.len())? == expected.creations,
        "epoch {epoch} discovery content differs from its completed checkpoint"
    );
    ensure!(
        log.entries
            .windows(2)
            .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey),
        "epoch {epoch} discoveries are not strictly sorted and unique"
    );
    let epoch_start_slot = epoch
        .checked_mul(slots_per_epoch)
        .context("epoch start slot overflow")?;
    let epoch_end_slot = epoch_start_slot
        .checked_add(slots_per_epoch - 1)
        .context("epoch end slot overflow")?;
    for entry in &log.entries {
        ensure!(
            entry.first_creation.epoch == epoch
                && (epoch_start_slot..=epoch_end_slot).contains(&entry.first_creation.slot)
                && entry.first_creation.slot >= config.mint_slot,
            "epoch {epoch} discovery has an invalid creation coordinate"
        );
    }
    Ok(log)
}

fn parse_checkpoint_digest(value: &str, label: &str) -> Result<[u8; 32]> {
    ensure!(value.len() == 64, "{label} is not 64 hex digits");
    let mut output = [0u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(output)
}

fn validate_resume_discovery(
    config: &ExtractConfig,
    input: &EpochInput,
    directory: &Path,
    expected_log: Option<&EpochCreationLog>,
) -> Result<(ResumeDiscoveryBinding, EpochCreationLog)> {
    ensure_exact_directory_files(directory, &[CREATIONS_FILE])?;
    let path = directory.join(CREATIONS_FILE);
    let log: EpochCreationLog = wincode::config::deserialize_exact(
        &read_bounded_regular_file(&path, 1 << 30)?,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )
    .with_context(|| format!("decode {}", path.display()))?;
    let (source, reader) = open_epoch(input, config)?;
    let generation_digest = reader.binding().generation_digest;
    let expected_mint = parse_pubkey(&config.mint, "mint")?;
    ensure!(
        log.schema_version == DUMP_SCHEMA_VERSION
            && log.epoch == input.epoch
            && log.mint == expected_mint
            && log.source_generation_digest == generation_digest,
        "epoch {} discovery header differs from the admitted source",
        input.epoch
    );
    if let Some(expected) = expected_log {
        ensure!(
            &log == expected,
            "durable creation log differs from its verified in-memory result"
        );
    }
    ensure!(
        log.entries
            .windows(2)
            .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey),
        "epoch {} discoveries are not strictly sorted and unique",
        input.epoch
    );
    for entry in &log.entries {
        ensure!(
            entry.first_creation.epoch == input.epoch
                && (input.manifest.epoch_start_slot()..=input.manifest.epoch_end_slot())
                    .contains(&entry.first_creation.slot)
                && entry.first_creation.slot >= config.mint_slot,
            "epoch {} discovery has an invalid creation coordinate",
            input.epoch
        );
    }
    if expected_log.is_none() {
        let registry = VerifiedEpochRegistry::open(
            source.clone(),
            reader.manifest(),
            reader.registry_entries(),
        )?;
        for entry in &log.entries {
            ensure!(
                registry.resolve_verified(entry.source_reference)? == entry.raw_pubkey,
                "epoch {} discovery source reference differs from its raw key",
                input.epoch
            );
        }
    }
    source
        .verify_unchanged()
        .with_context(|| format!("verify epoch {} discovery source", input.epoch))?;
    Ok((
        ResumeDiscoveryBinding {
            epoch: input.epoch,
            source_generation_digest: hex_digest(generation_digest),
            creation_log_sha256: sha256_file(&path)?,
            creations: u64::try_from(log.entries.len()).context("discovery count exceeds u64")?,
        },
        log,
    ))
}

fn merge_discovery_accounts(
    accounts: &mut BTreeMap<[u8; 32], SourceInstructionCoordinate>,
    log: &EpochCreationLog,
) {
    for entry in &log.entries {
        accounts
            .entry(entry.raw_pubkey)
            .and_modify(|coordinate| *coordinate = (*coordinate).min(entry.first_creation))
            .or_insert(entry.first_creation);
    }
}

fn validate_resume_shards(
    config: &ExtractConfig,
    inputs: &[EpochInput],
    shard_paths: &[(u64, PathBuf)],
    mint: [u8; 32],
    anchor_signature: [u8; 64],
    accounts: &DiscoveredAccountList,
) -> Result<Vec<ResumeShardBinding>> {
    shard_paths
        .iter()
        .map(|(epoch, path)| {
            let input = input_for_epoch(inputs, *epoch)?;
            validate_resume_shard(config, input, path, mint, anchor_signature, accounts, None)
                .with_context(|| format!("validate resumed epoch {epoch} shard"))
        })
        .collect()
}

fn validate_resume_shard(
    config: &ExtractConfig,
    input: &EpochInput,
    directory: &Path,
    mint: [u8; 32],
    anchor_signature: [u8; 64],
    accounts: &DiscoveredAccountList,
    expected_account_ids: Option<&EpochAccountIdLog>,
) -> Result<ResumeShardBinding> {
    let (source, reader) = open_epoch(input, config)?;
    let generation_digest = reader.binding().generation_digest;
    let rebuilt_account_ids;
    let expected_account_ids = if let Some(expected) = expected_account_ids {
        ensure!(
            expected.epoch == input.epoch && expected.source_generation_digest == generation_digest,
            "in-memory epoch account-ID log is bound to a different source"
        );
        expected
    } else {
        let registry = VerifiedEpochRegistry::open(
            source.clone(),
            reader.manifest(),
            reader.registry_entries(),
        )?;
        rebuilt_account_ids = EpochTargetTable::build(
            input.epoch,
            mint,
            accounts.anchor_position,
            &accounts.accounts,
            &registry,
            generation_digest,
        )?
        .1;
        &rebuilt_account_ids
    };
    ensure!(
        read_epoch_account_id_log(&directory.join(ACCOUNT_ID_LOG_FILE))? == *expected_account_ids,
        "epoch {} account-ID log differs from verified source mappings",
        input.epoch
    );
    source
        .verify_unchanged()
        .with_context(|| format!("verify epoch {} account-ID source", input.epoch))?;
    validate_epoch_shard_for_resume(
        input.epoch,
        directory,
        resume_target_binding(config, mint, anchor_signature),
        &dump_source_binding(config),
        generation_digest,
        input.manifest.slots_per_epoch,
        &accounts.accounts,
        accounts.anchor_position,
    )
}

fn resume_target_binding(
    config: &ExtractConfig,
    mint: [u8; 32],
    anchor_signature: [u8; 64],
) -> ResumeTargetBinding {
    ResumeTargetBinding {
        mint,
        mint_slot: config.mint_slot,
        mint_signature: anchor_signature,
        workers: config.workers,
    }
}

fn input_for_epoch(inputs: &[EpochInput], epoch: u64) -> Result<&EpochInput> {
    inputs
        .binary_search_by_key(&epoch, |input| input.epoch)
        .ok()
        .and_then(|index| inputs.get(index))
        .with_context(|| format!("epoch {epoch} is outside the admitted input range"))
}

fn locate_anchor_transaction(
    config: &ExtractConfig,
    input: &EpochInput,
    anchor_signature: [u8; 64],
) -> Result<SourceTransactionCoordinate> {
    ensure!(
        (input.manifest.epoch_start_slot()..=input.manifest.epoch_end_slot())
            .contains(&config.mint_slot),
        "epoch {} does not contain mint slot {}",
        input.epoch,
        config.mint_slot
    );
    let (source, reader) = open_epoch(input, config)?;
    let rows = &reader.index().rows;
    let start = rows.partition_point(|row| row.slot < config.mint_slot);
    let end = rows.partition_point(|row| row.slot <= config.mint_slot);
    ensure!(start < end, "mint slot {} is not indexed", config.mint_slot);
    let projector = reader.message_projector();
    let registry_entries = reader.registry_entries();
    let generation_digest = reader.binding().generation_digest;
    let registry =
        VerifiedEpochRegistry::open(source.clone(), reader.manifest(), registry_entries)?;
    let matcher = DiscoveryMatcher::build(parse_pubkey(&config.mint, "mint")?, &registry)?;
    let mut found = None;
    reader.process_borrowed_blocks_parallel_ordered(
        start..end,
        ordered_config(config.workers, true),
        |_| DiscoveryScratch::new(),
        |scratch, _, block| {
            project_creation_discovery_block(
                scratch,
                input.epoch,
                config.mint_slot,
                matcher,
                projector,
                registry_entries,
                generation_digest,
                reader.reader_id(),
                block,
            )
            .map_err(|error| invalid_block_error(error.slot, error.message))
        },
        |_, block| {
            for (tx_index, signatures) in block.first_signatures {
                let signature = reader
                    .read_transaction_signatures(SignatureReference {
                        count: 1,
                        ..signatures
                    })
                    .map_err(|error| {
                        invalid_block_error(
                            block.slot,
                            format!("transaction {tx_index} first signature read: {error}"),
                        )
                    })?;
                if signature.first() == Some(&anchor_signature) {
                    if found.is_some() {
                        return Err(invalid_block_error(
                            block.slot,
                            "mint signature occurs more than once at the mint slot".to_owned(),
                        ));
                    }
                    found = Some(SourceTransactionCoordinate {
                        epoch: input.epoch,
                        slot: block.slot,
                        source_block_id: block.source_block_id,
                        tx_index,
                        source_first_signature_ordinal: signatures.first_ordinal,
                        signature_count: signatures.count,
                    });
                }
            }
            Ok(())
        },
    )?;
    source
        .verify_unchanged()
        .with_context(|| format!("verify epoch {} mint-anchor source", input.epoch))?;
    found.context("mint signature was not found as a first signature at the mint slot")
}

fn load_and_validate_frozen_accounts(
    path: &Path,
    mint: [u8; 32],
    mint_slot: u64,
    first_epoch: u64,
    last_epoch: u64,
    expected: &BTreeMap<[u8; 32], SourceInstructionCoordinate>,
) -> Result<DiscoveredAccountList> {
    let accounts: DiscoveredAccountList = wincode::config::deserialize_exact(
        &read_bounded_regular_file(path, 1 << 30)?,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )
    .with_context(|| format!("decode {}", path.display()))?;
    validate_frozen_account_structure(
        &accounts,
        mint,
        mint_slot,
        first_epoch,
        last_epoch,
        expected,
    )?;
    Ok(accounts)
}

fn validate_frozen_account_structure(
    accounts: &DiscoveredAccountList,
    mint: [u8; 32],
    mint_slot: u64,
    first_epoch: u64,
    last_epoch: u64,
    expected: &BTreeMap<[u8; 32], SourceInstructionCoordinate>,
) -> Result<()> {
    ensure!(
        accounts.schema_version == DUMP_SCHEMA_VERSION && accounts.mint == mint,
        "frozen account artifact header differs from this run"
    );
    ensure!(
        accounts.anchor_position.slot == mint_slot
            && (first_epoch..=last_epoch).contains(&accounts.anchor_position.epoch)
            && accounts.anchor_position.signature_count != 0,
        "frozen mint-anchor coordinate is invalid"
    );
    ensure!(
        accounts
            .accounts
            .windows(2)
            .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey),
        "frozen account list is not strictly sorted and unique"
    );
    ensure!(
        accounts.accounts.len() == expected.len(),
        "frozen account count differs from discovery shards"
    );
    for account in &accounts.accounts {
        ensure!(
            account.raw_pubkey != mint,
            "target mint is listed as a token account"
        );
        ensure!(
            (first_epoch..=last_epoch).contains(&account.first_creation.epoch)
                && account.first_creation.slot >= mint_slot,
            "frozen account has an invalid first-creation coordinate"
        );
        ensure!(
            expected.get(&account.raw_pubkey) == Some(&account.first_creation),
            "frozen account list differs from deterministic discovery merge"
        );
    }
    Ok(())
}

fn resume_frozen_binding(
    path: &Path,
    accounts: &DiscoveredAccountList,
) -> Result<ResumeFrozenAccountBinding> {
    Ok(ResumeFrozenAccountBinding {
        accounts_sha256: sha256_file(path)?,
        account_count: u64::try_from(accounts.accounts.len())
            .context("frozen account count exceeds u64")?,
    })
}

fn persist_resume_checkpoint(
    root: &Path,
    identity: &ResumeIdentity,
    discoveries: &[ResumeDiscoveryBinding],
    frozen: Option<&ResumeFrozenAccountBinding>,
    raw: &[ResumeShardBinding],
) -> Result<()> {
    let payload = ResumeCheckpointPayload::new(
        identity.clone(),
        discoveries.to_vec(),
        frozen.cloned(),
        raw.to_vec(),
    )?;
    let committed = write_resume_checkpoint_atomic(root, &payload)?;
    ensure!(
        committed.payload == payload,
        "committed resume checkpoint differs from the staged payload"
    );
    Ok(())
}

fn validate_raw_root_manifest(
    path: &Path,
    config: &ExtractConfig,
    first_epoch: u64,
    last_epoch: u64,
    frozen: &ResumeFrozenAccountBinding,
    raw: &[ResumeShardBinding],
) -> Result<()> {
    let manifest: DumpManifest =
        serde_json::from_slice(&read_bounded_regular_file(path, 16 << 20)?)
            .with_context(|| format!("parse {}", path.display()))?;
    let transactions = raw.iter().try_fold(0u64, |sum, binding| {
        sum.checked_add(binding.counters.transactions)
            .context("root transaction count overflow")
    })?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::RawExtractionRoot
            && manifest.complete,
        "root manifest is not a complete schema-{DUMP_SCHEMA_VERSION} raw extraction"
    );
    ensure!(
        manifest.mint == bs58::encode(parse_pubkey(&config.mint, "mint")?).into_string()
            && manifest.mint_slot == config.mint_slot
            && manifest.mint_signature
                == bs58::encode(parse_signature(&config.mint_signature)?).into_string()
            && manifest.workers == config.workers
            && manifest.source_binding == dump_source_binding(config),
        "root manifest target or source admission differs from this run"
    );
    ensure!(
        manifest.first_epoch == first_epoch
            && manifest.last_epoch == last_epoch
            && manifest.transactions == transactions,
        "root manifest epoch range or transaction count differs from raw shards"
    );
    ensure!(
        manifest.transaction_stream == EPOCH_SHARDS_DIR
            && manifest.transaction_stream_sha256.is_none()
            && manifest.account_id_log.is_none()
            && manifest.account_id_log_sha256.is_none()
            && manifest.discovered_accounts.as_deref() == Some(ACCOUNTS_FILE)
            && manifest.discovered_accounts_sha256.as_deref()
                == Some(frozen.accounts_sha256.as_str())
            && manifest.discovered_account_count == Some(frozen.account_count),
        "root manifest artifact bindings differ from validated phase-1 artifacts"
    );
    ensure!(
        manifest.signatures.is_none()
            && manifest.pubkeys.is_none()
            && manifest.signature_stream.is_none()
            && manifest.signature_stream_sha256.is_none()
            && manifest.pubkey_registry.is_none()
            && manifest.pubkey_registry_sha256.is_none()
            && manifest.registry_maps.is_none(),
        "root manifest claims a phase-2 sidecar"
    );
    Ok(())
}

fn regular_file_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file(),
                "required path {} is not a regular file",
                path.display()
            );
            Ok(true)
        }
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

fn read_bounded_regular_file(path: &Path, maximum_bytes: u64) -> Result<Vec<u8>> {
    ensure!(
        regular_file_exists(path)?,
        "required file {} is absent",
        path.display()
    );
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.len() <= maximum_bytes,
        "file {} exceeds the {}-byte validation limit",
        path.display(),
        maximum_bytes
    );
    fs::read(path).with_context(|| format!("read {}", path.display()))
}

fn ensure_exact_directory_files(directory: &Path, expected: &[&str]) -> Result<()> {
    let metadata = fs::symlink_metadata(directory)
        .with_context(|| format!("inspect artifact directory {}", directory.display()))?;
    ensure!(
        metadata.file_type().is_dir(),
        "artifact path {} is not a direct directory",
        directory.display()
    );
    let expected = expected
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        ensure!(
            entry.file_type()?.is_file(),
            "artifact member {} is not a regular file",
            entry.path().display()
        );
        observed.insert(
            entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("artifact contains a non-UTF-8 file name"))?,
        );
    }
    ensure!(
        observed == expected,
        "artifact directory {} has unexpected or missing files",
        directory.display()
    );
    Ok(())
}

fn hex_digest(digest: [u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sync_file(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync {}", path.display()))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

fn prepare_empty_directory(path: &Path, label: &str) -> Result<()> {
    if path.exists() {
        ensure!(
            path.is_dir(),
            "{label} {} is not a directory",
            path.display()
        );
        ensure!(
            fs::read_dir(path)?.next().is_none(),
            "{label} {} is not empty",
            path.display()
        );
    } else {
        fs::create_dir_all(path).with_context(|| format!("create {label} {}", path.display()))?;
    }
    Ok(())
}

fn parse_pubkey(value: &str, label: &str) -> Result<[u8; 32]> {
    Pubkey::from_str(value)
        .with_context(|| format!("parse {label} {value}"))
        .map(|key| key.to_bytes())
}

fn parse_signature(value: &str) -> Result<[u8; 64]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("parse mint signature {value}"))?;
    ensure!(bytes.len() == 64, "mint signature is not 64 bytes");
    Ok(bytes.try_into().expect("checked 64-byte signature"))
}

fn sha256_file(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("open {} for hashing", path.display()))?;
    let mut reader = BufReader::with_capacity(8 << 20, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 8 << 20];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("hash {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

#[derive(Debug)]
struct BlockProjectionError {
    slot: u64,
    message: String,
}

impl BlockProjectionError {
    fn new(slot: u64, tx_index: u32, error: impl std::fmt::Display) -> Self {
        Self {
            slot,
            message: if tx_index == u32::MAX {
                error.to_string()
            } else {
                format!("transaction {tx_index}: {error}")
            },
        }
    }
}

fn invalid_block_error(slot: u64, message: String) -> ReadError {
    ReadError::InvalidBlock { slot, message }
}

#[cfg(test)]
mod tests {
    use blockzilla_archive_v2::{ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_META_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SIGNATURES_FILE, ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload, ArchiveV2HotMetaRecord, ArchiveV2HotTxRow, ArchiveV2HotV0Message, WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Header, write_archive_v2_hot_block_index};
    use blockzilla_compact::{CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader, CompactMetaV1, CompactTransactionError, OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash};
    use blockzilla_primitives::{WincodeLeb128FramedReader, wincode_leb128_config};
    use blockzilla_registry::{KeyIndex, write_registry};
    use blockzilla_read_sdk::manifest::{
        GENERATION_MANIFEST_SCHEMA_VERSION, GenerationFile, compute_generation_digest,
    };

    use super::*;

    fn serialize_message(message: &ArchiveV2HotMessagePayload) -> Vec<u8> {
        wincode::config::serialize(message, wincode_leb128_config()).unwrap()
    }

    fn metadata(
        total_accounts: usize,
        loaded_writable_addresses: Vec<CompactPubkey>,
        loaded_readonly_addresses: Vec<CompactPubkey>,
        inner_instructions: Option<Vec<CompactInnerInstructions>>,
        err: Option<CompactTransactionError>,
    ) -> CompactMetaV1 {
        CompactMetaV1 {
            err,
            fee: 5_000,
            pre_balances: vec![0; total_accounts],
            post_balances: vec![0; total_accounts],
            inner_instructions,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses,
            loaded_readonly_addresses,
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    fn legacy_error_metadata(successful_tail: &CompactMetaV1) -> Vec<u8> {
        let successful =
            wincode::config::serialize(successful_tail, wincode_leb128_config()).unwrap();
        assert_eq!(successful.first(), Some(&0));
        let stored_error: [u8; 9] = [
            8, 0, 0, 0, // StoredTransactionError::InstructionError
            0, // instruction index
            44, 0, 0, 0, // historical unit BorshIoError
        ];
        let mut legacy =
            wincode::config::serialize(&Some(stored_error.to_vec()), wincode_leb128_config())
                .unwrap();
        legacy.extend_from_slice(&successful[1..]);
        legacy
    }

    fn post_projector() -> ArchiveV2MessageProjector {
        ArchiveV2MessageProjector::new(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1)
    }

    #[test]
    fn borrowed_raw_transaction_record_is_wire_compatible_with_the_owned_schema() {
        let context = TokenTransactionBlockContext {
            slot: 123,
            parent_slot: 122,
            blockhash_id: u32::MAX,
            previous_blockhash_id: 7,
            block_time: Some(10),
            block_height: Some(11),
            transaction_count: 3,
        };
        let message_bytes = [1, 2, 3, 4];
        let metadata_bytes = [5, 6, 7];
        let owned = TokenTransactionDumpRecord::Transaction(TokenTransactionRecord {
            source_epoch: 801,
            source_generation_digest: [8; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 9,
            block: context.clone(),
            tx_index: 2,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA,
            source_first_signature_ordinal: 42,
            signature_count: 2,
            dump_signature_ordinal: None,
            message_bytes: message_bytes.to_vec(),
            metadata_bytes: metadata_bytes.to_vec(),
        });
        let borrowed = BorrowedRawDumpRecord::Transaction(BorrowedRawTransactionRecord {
            source_epoch: 801,
            source_generation_digest: [8; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 9,
            block: &context,
            tx_index: 2,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA,
            source_first_signature_ordinal: 42,
            signature_count: 2,
            dump_signature_ordinal: None,
            message_bytes: &message_bytes,
            metadata_bytes: &metadata_bytes,
        });
        let mut owned_bytes = Vec::new();
        let mut borrowed_bytes = Vec::new();
        encode_with_scratch(&owned, &mut owned_bytes).unwrap();
        encode_with_scratch(&borrowed, &mut borrowed_bytes).unwrap();
        assert_eq!(borrowed_bytes, owned_bytes);
    }

    fn index_row(block_id: u32, slot: u64) -> blockzilla_archive_v2::ArchiveV2HotBlockIndexRow {
        blockzilla_archive_v2::ArchiveV2HotBlockIndexRow {
            block_id,
            slot,
            compressed_offset: u64::from(block_id) * 100,
            compressed_len: 100,
            uncompressed_len: 200,
            tx_count: 1,
            first_tx_ordinal: u64::from(block_id),
            first_signature_ordinal: u64::from(block_id),
            signature_count: 1,
        }
    }

    fn creation_coordinate(
        epoch: u64,
        slot: u64,
        source_block_id: u32,
        tx_index: u32,
        instruction_index: u32,
    ) -> SourceInstructionCoordinate {
        SourceInstructionCoordinate {
            epoch,
            slot,
            source_block_id,
            tx_index,
            instruction_index,
        }
    }

    fn test_verified_registry(root: &Path, keys: &[[u8; 32]]) -> VerifiedEpochRegistry {
        let registry_path = root.join("registry.bin");
        let index_path = root.join("registry.mphf");
        write_registry(&registry_path, keys).unwrap();
        KeyIndex::build(keys.to_vec()).write(&index_path).unwrap();
        VerifiedEpochRegistry {
            registry_file: File::open(&registry_path).unwrap(),
            index: FileBackedKeyIndex::load_file(File::open(&index_path).unwrap(), &index_path)
                .unwrap(),
            entries: u32::try_from(keys.len()).unwrap(),
            registry_path,
        }
    }

    #[test]
    fn probe_range_requires_the_exact_slot_and_expected_row() {
        let rows = [index_row(0, 100), index_row(1, 102), index_row(2, 103)];
        assert_eq!(
            exact_probe_row_range(&rows, 102, Some(1), 10).unwrap(),
            1..3
        );

        let missing = exact_probe_row_range(&rows, 101, None, 1)
            .unwrap_err()
            .to_string();
        assert!(missing.contains("start slot 101 is skipped"));

        let wrong_row = exact_probe_row_range(&rows, 102, Some(2), 1)
            .unwrap_err()
            .to_string();
        assert!(wrong_row.contains("row 1, not expected row 2"));
    }

    #[test]
    fn probe_range_is_bounded_by_max_blocks() {
        let rows = [index_row(0, 100), index_row(1, 101), index_row(2, 102)];
        assert_eq!(exact_probe_row_range(&rows, 100, Some(0), 2).unwrap(), 0..2);
        assert!(exact_probe_row_range(&rows, 100, Some(0), 0).is_err());
    }

    #[test]
    fn pass_b_matches_an_unused_static_account_without_instruction_confirmation() {
        let target = CompactPubkey::Id(8);
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), target],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
        });
        let bytes = serialize_message(&message);
        let mut scratch = MatchScratch::new();
        assert!(
            transaction_account_list_matches(
                &mut scratch,
                post_projector(),
                100,
                0,
                1,
                &bytes,
                None,
                |reference| reference == target,
            )
            .unwrap()
        );
    }

    #[test]
    fn target_mint_uses_the_full_anchor_coordinate_as_its_creation_floor() {
        let mint = [8; 32];
        let table = EpochTargetTable {
            epoch: 801,
            mint,
            mint_id: Some(8),
            mint_creation: SourceTransactionCoordinate {
                epoch: 801,
                slot: 100,
                source_block_id: 7,
                tx_index: 2,
                source_first_signature_ordinal: 11,
                signature_count: 1,
            },
            prior_id_bits: Vec::new(),
            current_id_bits: Vec::new(),
            current_ids: Vec::new(),
            raw: Vec::new(),
            raw_delta_scratch: Vec::new(),
            raw_merge_scratch: Vec::new(),
            current_id_delta_scratch: Vec::new(),
            current_ids_merge_scratch: Vec::new(),
            account_id_entries: Vec::new(),
            source_generation_digest: [0; 32],
        };

        assert!(!table.reference_is_eligible(CompactPubkey::Raw(mint), 100, 6, 99));
        assert!(!table.reference_is_eligible(CompactPubkey::Id(8), 100, 7, 1));
        assert!(table.reference_is_eligible(CompactPubkey::Raw(mint), 100, 7, 2));
        assert!(table.reference_is_eligible(CompactPubkey::Id(8), 100, 7, 3));
        assert!(table.reference_is_eligible(CompactPubkey::Raw(mint), 100, 8, 0));
    }

    #[test]
    fn pending_candidate_vectors_keep_the_earliest_entry_and_reuse_capacity() {
        let later = PendingCreationCandidate {
            coordinate: creation_coordinate(801, 110, 2, 1, 3),
            ledger_sequence: 2,
        };
        let earlier = PendingCreationCandidate {
            coordinate: creation_coordinate(801, 109, 7, 4, 2),
            ledger_sequence: 9,
        };
        let same_coordinate_earlier_sequence = PendingCreationCandidate {
            coordinate: earlier.coordinate,
            ledger_sequence: 1,
        };
        let mut ids = Vec::with_capacity(8);
        ids.extend([
            (5, later),
            (5, earlier),
            (5, same_coordinate_earlier_sequence),
        ]);
        let capacity = ids.capacity();

        deduplicate_pending_id_candidates(&mut ids);
        assert_eq!(ids, vec![(5, same_coordinate_earlier_sequence)]);
        ids.clear();
        ids.extend([(6, later), (6, earlier)]);
        deduplicate_pending_id_candidates(&mut ids);
        assert_eq!(ids, vec![(6, earlier)]);
        assert_eq!(ids.capacity(), capacity);

        let mut raw = vec![([7; 32], later), ([7; 32], earlier)];
        deduplicate_pending_raw_candidates(&mut raw);
        assert_eq!(raw, vec![([7; 32], earlier)]);
    }

    #[test]
    fn bulk_resolution_reuses_raw_id_alias_proof_and_keeps_earliest_source() {
        let directory = tempfile::tempdir().unwrap();
        let keys = [[10; 32], [20; 32], [30; 32]];
        let registry = test_verified_registry(directory.path(), &keys);
        let id_coordinate = creation_coordinate(801, 120, 3, 1, 2);
        let raw_coordinate = creation_coordinate(801, 119, 8, 4, 1);
        let mut ids = vec![
            (
                2,
                PendingCreationCandidate {
                    coordinate: id_coordinate,
                    ledger_sequence: 1,
                },
            ),
            (
                2,
                PendingCreationCandidate {
                    coordinate: creation_coordinate(801, 121, 1, 0, 0),
                    ledger_sequence: 2,
                },
            ),
        ];
        let mut raws = vec![(
            keys[1],
            PendingCreationCandidate {
                coordinate: raw_coordinate,
                ledger_sequence: 3,
            },
        )];
        deduplicate_pending_id_candidates(&mut ids);
        deduplicate_pending_raw_candidates(&mut raws);
        let mut scratch = RegistryResolutionScratch::default();
        let mut resolved = Vec::new();

        let stats = registry
            .resolve_creation_candidates_bulk(&ids, &raws, &mut scratch, &mut resolved)
            .unwrap();
        resolved.sort_unstable_by(|left, right| {
            left.raw_pubkey
                .cmp(&right.raw_pubkey)
                .then_with(|| left.coordinate.cmp(&right.coordinate))
                .then_with(|| left.ledger_sequence.cmp(&right.ledger_sequence))
        });

        assert_eq!(ids.len(), 1);
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].raw_pubkey, keys[1]);
        assert_eq!(resolved[0].source_reference, CompactPubkey::Raw(keys[1]));
        assert_eq!(resolved[0].coordinate, raw_coordinate);
        assert_eq!(resolved[1].source_reference, CompactPubkey::Id(2));
        assert_eq!(stats.registry_rows_read, 1);
        assert_eq!(stats.registry_coalesced_read_calls, 1);
        assert_eq!(stats.registry_read_bytes, 32);
        assert_eq!(stats.mphf_lookups, 1);
    }

    #[test]
    fn bulk_registry_reads_merge_near_rows_and_split_large_gaps() {
        let directory = tempfile::tempdir().unwrap();
        let keys = (1u8..=132).map(|value| [value; 32]).collect::<Vec<_>>();
        let registry = test_verified_registry(directory.path(), &keys);
        let pending = PendingCreationCandidate {
            coordinate: creation_coordinate(801, 100, 0, 0, 0),
            ledger_sequence: 0,
        };
        let ids = vec![(1, pending), (2, pending), (132, pending)];
        let mut scratch = RegistryResolutionScratch::default();
        let mut resolved = Vec::new();

        let stats = registry
            .resolve_creation_candidates_bulk(&ids, &[], &mut scratch, &mut resolved)
            .unwrap();

        assert_eq!(resolved.len(), 3);
        assert_eq!(stats.registry_rows_read, 3);
        assert_eq!(stats.registry_coalesced_read_calls, 2);
        assert_eq!(stats.registry_read_bytes, 96);
        assert_eq!(stats.mphf_lookups, 3);
    }

    #[test]
    fn current_id_bitset_and_sorted_deltas_cover_word_boundaries() {
        let mint = [1; 32];
        let prior = ResolvedDiscoveredAccount {
            raw_pubkey: [10; 32],
            first_creation: creation_coordinate(800, 90, 0, 0, 0),
            local_id: Some(64),
        };
        let current = ResolvedDiscoveredAccount {
            raw_pubkey: [20; 32],
            first_creation: creation_coordinate(801, 110, 1, 2, 0),
            local_id: Some(65),
        };
        let mut table = EpochTargetTable::build_resolved(
            801,
            mint,
            Some(1),
            SourceTransactionCoordinate {
                epoch: 801,
                slot: 100,
                source_block_id: 0,
                tx_index: 0,
                source_first_signature_ordinal: 0,
                signature_count: 1,
            },
            &[prior, current],
            130,
            [9; 32],
        )
        .unwrap();

        assert!(table.reference_is_eligible(CompactPubkey::Id(64), 1, 0, 0));
        assert!(!table.reference_is_eligible(CompactPubkey::Id(65), 110, 1, 1));
        assert!(table.reference_is_eligible(CompactPubkey::Id(65), 110, 1, 2));
        assert!(!table.reference_is_eligible(CompactPubkey::Id(66), 999, 9, 9));

        table
            .extend_current_accounts(&[
                ResolvedDiscoveredAccount {
                    raw_pubkey: [30; 32],
                    first_creation: creation_coordinate(801, 120, 2, 0, 0),
                    local_id: Some(129),
                },
                ResolvedDiscoveredAccount {
                    raw_pubkey: [40; 32],
                    first_creation: creation_coordinate(801, 121, 3, 0, 0),
                    local_id: Some(66),
                },
            ])
            .unwrap();
        let raw_delta_capacity = table.raw_delta_scratch.capacity();
        let id_delta_capacity = table.current_id_delta_scratch.capacity();
        assert_eq!(
            table
                .current_ids
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            vec![65, 66, 129]
        );
        assert!(table.reference_is_eligible(CompactPubkey::Id(129), 120, 2, 0));
        assert!(!table.reference_is_eligible(CompactPubkey::Id(66), 121, 2, 9));
        assert!(table.reference_is_eligible(CompactPubkey::Id(66), 121, 3, 0));

        table
            .extend_current_accounts(&[ResolvedDiscoveredAccount {
                raw_pubkey: [50; 32],
                first_creation: creation_coordinate(801, 122, 4, 0, 0),
                local_id: Some(128),
            }])
            .unwrap();
        assert_eq!(table.raw_delta_scratch.capacity(), raw_delta_capacity);
        assert_eq!(table.current_id_delta_scratch.capacity(), id_delta_capacity);
        assert_eq!(
            table
                .current_ids
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            vec![65, 66, 128, 129]
        );
        assert!(table.reference_is_eligible(CompactPubkey::Id(128), 122, 4, 0));
        assert!(!table.reference_is_eligible(CompactPubkey::Id(127), 999, 9, 9));
    }

    #[test]
    fn pass_b_streams_loaded_accounts_keeps_failed_txs_and_excludes_alt_descriptors() {
        let descriptor = CompactPubkey::Id(7);
        let loaded_target = CompactPubkey::Id(8);
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: descriptor,
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let message_bytes = serialize_message(&message);
        let metadata_bytes = wincode::config::serialize(
            &metadata(
                4,
                vec![loaded_target],
                vec![CompactPubkey::Id(9)],
                None,
                Some(CompactTransactionError::AccountInUse),
            ),
            wincode_leb128_config(),
        )
        .unwrap();
        let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
            | ARCHIVE_V2_TX_FLAG_HAS_ERROR;
        let mut scratch = MatchScratch::new();
        assert!(
            transaction_account_list_matches(
                &mut scratch,
                post_projector(),
                100,
                flags,
                1,
                &message_bytes,
                Some(&metadata_bytes),
                |reference| reference == loaded_target,
            )
            .unwrap()
        );
        assert!(
            !transaction_account_list_matches(
                &mut scratch,
                post_projector(),
                100,
                flags,
                1,
                &message_bytes,
                Some(&metadata_bytes),
                |reference| reference == descriptor,
            )
            .unwrap()
        );

        let discovery_matcher = DiscoveryMatcher {
            mint: [99; 32],
            mint_id: None,
            token_program_ids: [None, None],
        };
        let mut discovery_scratch = DiscoveryScratch::new();
        let mut creations = Vec::new();
        assert!(
            project_transaction_creations_and_match(
                &mut discovery_scratch,
                post_projector(),
                100,
                flags,
                1,
                discovery_matcher,
                &message_bytes,
                Some(&metadata_bytes),
                true,
                |instruction_index, reference| creations.push((instruction_index, reference)),
                |reference| reference == loaded_target,
            )
            .unwrap(),
            "stage-A hints must retain a failed transaction that matches only a loaded address"
        );
        assert!(
            creations.is_empty(),
            "a failed transaction must not discover a token account"
        );
        assert!(
            !project_transaction_creations_and_match(
                &mut discovery_scratch,
                post_projector(),
                100,
                flags,
                1,
                discovery_matcher,
                &message_bytes,
                Some(&metadata_bytes),
                true,
                |_, _| panic!("failed transaction discovered an account"),
                |reference| reference == descriptor,
            )
            .unwrap(),
            "the address-table descriptor must not become an account-match hint"
        );
    }

    #[test]
    fn post_message_with_legacy_error_metadata_uses_owned_fallback_in_both_stages() {
        let loaded_target = CompactPubkey::Id(8);
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: Vec::new(),
                data: ArchiveV2HotInstructionData::Raw(Vec::new()),
            }],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(7),
                writable_indexes: vec![0],
                readonly_indexes: Vec::new(),
            }],
        });
        let message_bytes = serialize_message(&message);
        let metadata_bytes =
            legacy_error_metadata(&metadata(3, vec![loaded_target], Vec::new(), None, None));
        let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
            | ARCHIVE_V2_TX_FLAG_HAS_ERROR;

        let mut match_scratch = MatchScratch::new();
        assert!(
            transaction_account_list_matches(
                &mut match_scratch,
                post_projector(),
                100,
                flags,
                1,
                &message_bytes,
                Some(&metadata_bytes),
                |reference| reference == loaded_target,
            )
            .unwrap()
        );
        assert_eq!(match_scratch.metadata_owned_fallbacks, 1);

        let mut discovery_scratch = DiscoveryScratch::new();
        assert!(
            project_transaction_creations_and_match(
                &mut discovery_scratch,
                post_projector(),
                100,
                flags,
                1,
                DiscoveryMatcher {
                    mint: [99; 32],
                    mint_id: None,
                    token_program_ids: [None, None],
                },
                &message_bytes,
                Some(&metadata_bytes),
                true,
                |_, _| panic!("failed transaction discovered an account"),
                |reference| reference == loaded_target,
            )
            .unwrap()
        );
        assert_eq!(discovery_scratch.metadata_owned_fallbacks, 1);
    }

    #[test]
    fn malformed_or_ambiguous_error_metadata_fails_closed() {
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(7),
                writable_indexes: vec![0],
                readonly_indexes: Vec::new(),
            }],
        });
        let message_bytes = serialize_message(&message);
        let flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
            | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
            | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
            | ARCHIVE_V2_TX_FLAG_HAS_ERROR;
        let mut malformed = legacy_error_metadata(&metadata(
            3,
            vec![CompactPubkey::Id(8)],
            Vec::new(),
            None,
            None,
        ));
        malformed.truncate(malformed.len() - 1);
        let ambiguous = [vec![1, 4, 0, 0, 0, 0], vec![0; 13]].concat();

        for metadata_bytes in [&malformed, &ambiguous] {
            let mut scratch = MatchScratch::new();
            assert!(
                transaction_account_list_matches(
                    &mut scratch,
                    post_projector(),
                    100,
                    flags,
                    1,
                    &message_bytes,
                    Some(metadata_bytes),
                    |_| false,
                )
                .is_err()
            );
            assert_eq!(scratch.metadata_owned_fallbacks, 0);
        }
    }

    #[test]
    fn pass_a_discovers_static_program_with_loaded_account_and_mint() {
        let account = CompactPubkey::Raw([41; 32]);
        let mint = CompactPubkey::Raw([42; 32]);
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![
                CompactPubkey::Id(1),
                CompactPubkey::Raw(SPL_TOKEN_PROGRAM_ID),
            ],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![2, 3],
                data: ArchiveV2HotInstructionData::Raw(vec![18]),
            }],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(2),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let message_bytes = serialize_message(&message);
        let metadata_bytes = wincode::config::serialize(
            &metadata(4, vec![account], vec![mint], None, None),
            wincode_leb128_config(),
        )
        .unwrap();
        let matcher = DiscoveryMatcher {
            mint: [42; 32],
            mint_id: None,
            token_program_ids: [None, None],
        };
        let mut scratch = DiscoveryScratch::new();
        let mut creations = Vec::new();
        project_transaction_creations(
            &mut scratch,
            post_projector(),
            100,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
            1,
            matcher,
            &message_bytes,
            Some(&metadata_bytes),
            |instruction_index, reference| creations.push((instruction_index, reference)),
        )
        .unwrap();
        assert_eq!(creations, vec![(0, account)]);
    }

    #[test]
    fn pass_a_discovers_inner_init_with_loaded_program_and_excludes_failed_creation() {
        let account = CompactPubkey::Raw([51; 32]);
        let mint = CompactPubkey::Raw([52; 32]);
        let token_program = CompactPubkey::Raw(SPL_TOKEN_2022_PROGRAM_ID);
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: ArchiveV2HotInstructionData::Raw(vec![0]),
            }],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(3),
                writable_indexes: vec![0, 1],
                readonly_indexes: vec![2],
            }],
        });
        let message_bytes = serialize_message(&message);
        let inner = Some(vec![CompactInnerInstructions {
            index: 0,
            instructions: vec![CompactInnerInstruction {
                program_id_index: 3,
                accounts: vec![2, 4],
                data: vec![16],
                stack_height: Some(2),
            }],
        }]);
        let matcher = DiscoveryMatcher {
            mint: [52; 32],
            mint_id: None,
            token_program_ids: [None, None],
        };
        for (error, expected) in [
            (None, vec![(1, account)]),
            (Some(CompactTransactionError::AccountInUse), Vec::new()),
        ] {
            let metadata_bytes = wincode::config::serialize(
                &metadata(
                    5,
                    vec![account, token_program],
                    vec![mint],
                    inner.clone(),
                    error,
                ),
                wincode_leb128_config(),
            )
            .unwrap();
            let mut flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
            if expected.is_empty() {
                flags |= ARCHIVE_V2_TX_FLAG_HAS_ERROR;
            }
            let mut scratch = DiscoveryScratch::new();
            let mut creations = Vec::new();
            project_transaction_creations(
                &mut scratch,
                post_projector(),
                100,
                flags,
                1,
                matcher,
                &message_bytes,
                Some(&metadata_bytes),
                |instruction_index, reference| creations.push((instruction_index, reference)),
            )
            .unwrap();
            assert_eq!(creations, expected);
        }
    }

    fn write_test_varint(output: &mut Vec<u8>, mut value: u32) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }

    fn write_determinism_archive(root: &Path) -> PathBuf {
        write_determinism_archive_variant(root, false, false)
    }

    fn write_determinism_archive_with_repeat(
        root: &Path,
        repeat_creation_in_next_batch: bool,
    ) -> PathBuf {
        write_determinism_archive_variant(root, repeat_creation_in_next_batch, false)
    }

    fn write_match_hint_archive(root: &Path) -> PathBuf {
        write_determinism_archive_variant(root, false, true)
    }

    fn write_determinism_archive_variant(
        root: &Path,
        repeat_creation_in_next_batch: bool,
        include_clean_batch_nonmatch: bool,
    ) -> PathBuf {
        const EPOCH: u64 = 801;
        let archive_root = root.join("archive");
        let epoch_root = archive_root.join(format!("epoch-{EPOCH}"));
        fs::create_dir_all(&epoch_root).unwrap();

        let fee_payer = [11; 32];
        let mint = parse_pubkey(crate::format::SPYX_MINT, "mint").unwrap();
        let token_account = [44; 32];
        let later_token_account = [66; 32];
        let touch_program = [55; 32];
        let registry = [
            fee_payer,
            SPL_TOKEN_PROGRAM_ID,
            mint,
            token_account,
            touch_program,
            later_token_account,
        ];
        write_registry(&epoch_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), &registry).unwrap();
        KeyIndex::build(registry.to_vec())
            .write(&epoch_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();

        let anchor_message = serialize_message(&ArchiveV2HotMessagePayload::Legacy(
            ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 2,
                },
                account_keys: vec![
                    CompactPubkey::Id(1),
                    CompactPubkey::Id(3),
                    CompactPubkey::Id(5),
                ],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![ArchiveV2HotInstruction {
                    program_id_index: 2,
                    accounts: vec![1],
                    data: ArchiveV2HotInstructionData::Raw(vec![0]),
                }],
            },
        ));
        let init_message = serialize_message(&ArchiveV2HotMessagePayload::Legacy(
            ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 2,
                },
                account_keys: vec![
                    CompactPubkey::Id(1),
                    CompactPubkey::Id(4),
                    CompactPubkey::Id(3),
                    CompactPubkey::Id(2),
                ],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![ArchiveV2HotInstruction {
                    program_id_index: 3,
                    accounts: vec![1, 2],
                    data: ArchiveV2HotInstructionData::Raw(vec![18]),
                }],
            },
        ));
        let touch_message = serialize_message(&ArchiveV2HotMessagePayload::Legacy(
            ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![
                    CompactPubkey::Id(1),
                    CompactPubkey::Id(4),
                    CompactPubkey::Id(5),
                ],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![ArchiveV2HotInstruction {
                    program_id_index: 2,
                    accounts: vec![1],
                    data: ArchiveV2HotInstructionData::Raw(vec![99]),
                }],
            },
        ));
        let irrelevant_message = serialize_message(&ArchiveV2HotMessagePayload::Legacy(
            ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(5)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: ArchiveV2HotInstructionData::Raw(vec![99]),
                }],
            },
        ));
        let init_metadata = wincode::config::serialize(
            &metadata(4, Vec::new(), Vec::new(), None, None),
            wincode_leb128_config(),
        )
        .unwrap();
        let ata_creation_slot = 346_083_696;
        let precreation_touch_slot = ata_creation_slot - 1;
        let same_batch_touch_slot = ata_creation_slot + 1;
        let next_batch_touch_slot = ata_creation_slot + 2;
        let mut fixtures = vec![
            (crate::format::SPYX_MINT_SLOT, anchor_message, Vec::new(), 0),
            (precreation_touch_slot, touch_message.clone(), Vec::new(), 0),
            (
                ata_creation_slot,
                init_message.clone(),
                init_metadata.clone(),
                ARCHIVE_V2_TX_FLAG_HAS_METADATA,
            ),
            (same_batch_touch_slot, touch_message.clone(), Vec::new(), 0),
            (next_batch_touch_slot, touch_message, Vec::new(), 0),
        ];
        if repeat_creation_in_next_batch {
            fixtures.push((
                next_batch_touch_slot + 1,
                init_message,
                init_metadata,
                ARCHIVE_V2_TX_FLAG_HAS_METADATA,
            ));
        }
        if include_clean_batch_nonmatch {
            fixtures.push((next_batch_touch_slot + 2, irrelevant_message, Vec::new(), 0));
        }
        let fixture_count = fixtures.len();
        let mut compressed_blocks = Vec::new();
        let mut rows = Vec::new();
        for (block_id, (slot, message_bytes, metadata_bytes, flags)) in
            fixtures.into_iter().enumerate()
        {
            let block_id = u32::try_from(block_id).unwrap();
            let block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot,
                    parent_slot: slot - 1,
                    blockhash_id: block_id + 1,
                    previous_blockhash_id: block_id,
                    block_time: Some(1_750_000_000 + i64::from(block_id)),
                    block_height: Some(300_000_000 + u64::from(block_id)),
                    rewards: None,
                },
                tx_count: 1,
                tx_rows: vec![ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags,
                    message_offset: 0,
                    message_len: u32::try_from(message_bytes.len()).unwrap(),
                    metadata_offset: 0,
                    metadata_len: u32::try_from(metadata_bytes.len()).unwrap(),
                    signature_count: 1,
                    reserved: [0; 3],
                }],
                message_bytes,
                metadata_bytes,
            };
            let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
            let compressed = zstd::bulk::compress(&uncompressed, 3).unwrap();
            rows.push(ArchiveV2HotBlockIndexRow {
                block_id,
                slot,
                compressed_offset: u64::try_from(compressed_blocks.len()).unwrap(),
                compressed_len: u32::try_from(compressed.len()).unwrap(),
                uncompressed_len: u32::try_from(uncompressed.len()).unwrap(),
                tx_count: 1,
                first_tx_ordinal: u64::from(block_id),
                first_signature_ordinal: u64::from(block_id),
                signature_count: 1,
            });
            compressed_blocks.extend_from_slice(&compressed);
        }
        fs::write(epoch_root.join(ARCHIVE_V2_BLOCKS_FILE), &compressed_blocks).unwrap();
        write_archive_v2_hot_block_index(
            &epoch_root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed_blocks.len() as u64,
            i32::try_from(fixture_count).unwrap(),
            0,
            &rows,
        )
        .unwrap();

        let anchor = parse_signature(crate::format::SPYX_MINT_SIGNATURE).unwrap();
        let mut signatures = Vec::with_capacity(fixture_count * 64);
        signatures.extend_from_slice(&anchor);
        for block_id in 1..fixture_count {
            signatures.extend_from_slice(&[96 + u8::try_from(block_id).unwrap(); 64]);
        }
        fs::write(epoch_root.join(ARCHIVE_V2_SIGNATURES_FILE), signatures).unwrap();
        let mut archive_metadata = Vec::new();
        for record in [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks: u64::try_from(fixture_count).unwrap(),
                transactions: u64::try_from(fixture_count).unwrap(),
                ..WincodeArchiveV2Footer::default()
            }),
        ] {
            let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
            write_test_varint(&mut archive_metadata, u32::try_from(bytes.len()).unwrap());
            archive_metadata.extend_from_slice(&bytes);
        }
        fs::write(epoch_root.join(ARCHIVE_V2_META_FILE), archive_metadata).unwrap();
        archive_root
    }

    fn write_test_generation_manifest(
        epoch_root: &Path,
        epoch: u64,
        cluster_id: &str,
        slots_per_epoch: u64,
        wire_profile: ArchiveV2WireProfile,
        bind_current_metadata: bool,
    ) -> GenerationManifest {
        for profile in [
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        ] {
            let path = epoch_root.join(wire_profile_marker(profile).name);
            if path.exists() {
                fs::remove_file(path).unwrap();
            }
        }
        let message_marker = wire_profile_marker(wire_profile);
        fs::write(
            epoch_root.join(&message_marker.name),
            wire_profile_marker_bytes(wire_profile),
        )
        .unwrap();
        let metadata_marker_path = epoch_root.join(CURRENT_TYPED_ERRORS_MARKER_FILE);
        if bind_current_metadata {
            fs::write(&metadata_marker_path, CURRENT_TYPED_ERRORS_MARKER_BYTES).unwrap();
        } else if metadata_marker_path.exists() {
            fs::remove_file(&metadata_marker_path).unwrap();
        }

        let mut names = vec![
            ARCHIVE_V2_BLOCKS_FILE.to_owned(),
            ARCHIVE_V2_BLOCK_INDEX_FILE.to_owned(),
            ARCHIVE_V2_META_FILE.to_owned(),
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE.to_owned(),
            ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE.to_owned(),
            ARCHIVE_V2_SIGNATURES_FILE.to_owned(),
            message_marker.name,
        ];
        if bind_current_metadata {
            names.push(CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned());
        }
        let files = names
            .into_iter()
            .map(|name| {
                let path = epoch_root.join(&name);
                GenerationFile {
                    name,
                    size: fs::metadata(&path).unwrap().len(),
                    sha256: sha256_file(&path).unwrap(),
                }
            })
            .collect();
        let mut manifest = GenerationManifest {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            cluster_id: cluster_id.to_owned(),
            epoch,
            generation_id: format!("test-published-epoch-{epoch}"),
            generation_digest: "0".repeat(64),
            slots_per_epoch,
            complete: true,
            files,
        };
        persist_test_generation_manifest(epoch_root, &mut manifest);
        manifest
    }

    fn persist_test_generation_manifest(epoch_root: &Path, manifest: &mut GenerationManifest) {
        manifest.generation_digest = compute_generation_digest(manifest).unwrap();
        fs::write(
            epoch_root.join(GENERATION_MANIFEST_FILE),
            serde_json::to_vec_pretty(manifest).unwrap(),
        )
        .unwrap();
    }

    fn inspect_test_trusted_epoch(
        epoch_root: &Path,
    ) -> Result<(TrustedLocalMetadataAdmission, Option<GenerationManifest>)> {
        let source = PinnedLocalRangeSource::open_directory(fs::canonicalize(epoch_root)?)?;
        inspect_trusted_local_metadata_admission(
            &source,
            801,
            "determinism-fixture",
            432_000,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        )
    }

    fn write_followup_touch_epoch(archive_root: &Path, epoch: u64) {
        let epoch_root = archive_root.join(format!("epoch-{epoch}"));
        fs::create_dir_all(&epoch_root).unwrap();
        let fee_payer = [11; 32];
        let mint = parse_pubkey(crate::format::SPYX_MINT, "mint").unwrap();
        let token_account = [44; 32];
        let later_token_account = [66; 32];
        let touch_program = [55; 32];
        let registry = [
            fee_payer,
            SPL_TOKEN_PROGRAM_ID,
            mint,
            token_account,
            touch_program,
            later_token_account,
        ];
        write_registry(&epoch_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), &registry).unwrap();
        KeyIndex::build(registry.to_vec())
            .write(&epoch_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();
        let touch_message = serialize_message(&ArchiveV2HotMessagePayload::Legacy(
            ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![
                    CompactPubkey::Id(1),
                    CompactPubkey::Id(4),
                    CompactPubkey::Id(5),
                ],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![ArchiveV2HotInstruction {
                    program_id_index: 2,
                    accounts: vec![1],
                    data: ArchiveV2HotInstructionData::Raw(vec![99]),
                }],
            },
        ));
        let init_message = serialize_message(&ArchiveV2HotMessagePayload::Legacy(
            ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 2,
                },
                account_keys: vec![
                    CompactPubkey::Id(1),
                    CompactPubkey::Id(6),
                    CompactPubkey::Id(3),
                    CompactPubkey::Id(2),
                ],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![ArchiveV2HotInstruction {
                    program_id_index: 3,
                    accounts: vec![1, 2],
                    data: ArchiveV2HotInstructionData::Raw(vec![18]),
                }],
            },
        ));
        let first_slot = epoch * 432_000;
        let mut compressed_blocks = Vec::new();
        let mut rows = Vec::new();
        for (block_id, (slot, message_bytes)) in
            [(first_slot, touch_message), (first_slot + 1, init_message)]
                .into_iter()
                .enumerate()
        {
            let block_id = u32::try_from(block_id).unwrap();
            let block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot,
                    parent_slot: slot - 1,
                    blockhash_id: block_id + 1,
                    previous_blockhash_id: block_id,
                    block_time: Some(1_750_100_000 + i64::from(block_id)),
                    block_height: Some(300_100_000 + u64::from(block_id)),
                    rewards: None,
                },
                tx_count: 1,
                tx_rows: vec![ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: 0,
                    message_offset: 0,
                    message_len: u32::try_from(message_bytes.len()).unwrap(),
                    metadata_offset: 0,
                    metadata_len: 0,
                    signature_count: 1,
                    reserved: [0; 3],
                }],
                message_bytes,
                metadata_bytes: Vec::new(),
            };
            let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
            let compressed = zstd::bulk::compress(&uncompressed, 3).unwrap();
            rows.push(ArchiveV2HotBlockIndexRow {
                block_id,
                slot,
                compressed_offset: u64::try_from(compressed_blocks.len()).unwrap(),
                compressed_len: u32::try_from(compressed.len()).unwrap(),
                uncompressed_len: u32::try_from(uncompressed.len()).unwrap(),
                tx_count: 1,
                first_tx_ordinal: u64::from(block_id),
                first_signature_ordinal: u64::from(block_id),
                signature_count: 1,
            });
            compressed_blocks.extend_from_slice(&compressed);
        }
        fs::write(epoch_root.join(ARCHIVE_V2_BLOCKS_FILE), &compressed_blocks).unwrap();
        write_archive_v2_hot_block_index(
            &epoch_root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed_blocks.len() as u64,
            2,
            0,
            &rows,
        )
        .unwrap();
        fs::write(epoch_root.join(ARCHIVE_V2_SIGNATURES_FILE), [77; 128]).unwrap();
        let mut archive_metadata = Vec::new();
        for record in [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks: 2,
                transactions: 2,
                ..WincodeArchiveV2Footer::default()
            }),
        ] {
            let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
            write_test_varint(&mut archive_metadata, u32::try_from(bytes.len()).unwrap());
            archive_metadata.extend_from_slice(&bytes);
        }
        fs::write(epoch_root.join(ARCHIVE_V2_META_FILE), archive_metadata).unwrap();
    }

    fn write_noncanonical_match_hint_archive(root: &Path) -> PathBuf {
        const EPOCH: u64 = 801;
        let archive_root = root.join("noncanonical-archive");
        let epoch_root = archive_root.join(format!("epoch-{EPOCH}"));
        fs::create_dir_all(&epoch_root).unwrap();

        let mint = parse_pubkey(crate::format::SPYX_MINT, "mint").unwrap();
        let registry = [[11; 32], SPL_TOKEN_PROGRAM_ID, mint, [44; 32], [55; 32]];
        write_registry(&epoch_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), &registry).unwrap();
        KeyIndex::build(registry.to_vec())
            .write(&epoch_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();

        let message = |keys: Vec<CompactPubkey>, program_id_index, accounts, data| {
            serialize_message(&ArchiveV2HotMessagePayload::Legacy(
                ArchiveV2HotLegacyMessage {
                    header: CompactMessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 1,
                    },
                    account_keys: keys,
                    recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                    instructions: vec![ArchiveV2HotInstruction {
                        program_id_index,
                        accounts,
                        data: ArchiveV2HotInstructionData::Raw(data),
                    }],
                },
            ))
        };
        let anchor = message(
            vec![CompactPubkey::Id(1), CompactPubkey::Id(3)],
            1,
            vec![1],
            vec![0],
        );
        let initialize = message(
            vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(4),
                CompactPubkey::Id(3),
                CompactPubkey::Id(2),
            ],
            3,
            vec![1, 2],
            vec![18],
        );
        let touch = message(
            vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(4),
                CompactPubkey::Id(5),
            ],
            2,
            vec![1],
            vec![99],
        );
        let irrelevant = message(
            vec![CompactPubkey::Id(1), CompactPubkey::Id(5)],
            1,
            vec![0],
            vec![99],
        );
        let blocks = [
            (
                crate::format::SPYX_MINT_SLOT,
                vec![(0u32, anchor), (1u32, initialize)],
            ),
            (
                crate::format::SPYX_MINT_SLOT + 1,
                // Storage order intentionally differs from canonical tx-index order.
                vec![(1u32, touch), (0u32, irrelevant)],
            ),
        ];
        let mut compressed_blocks = Vec::new();
        let mut index_rows = Vec::new();
        let mut first_tx_ordinal = 0u64;
        let mut first_signature_ordinal = 0u64;
        for (block_id, (slot, storage_transactions)) in blocks.into_iter().enumerate() {
            let mut message_bytes = Vec::new();
            let mut tx_rows = Vec::new();
            for (tx_index, bytes) in storage_transactions {
                let message_offset = u32::try_from(message_bytes.len()).unwrap();
                let message_len = u32::try_from(bytes.len()).unwrap();
                message_bytes.extend_from_slice(&bytes);
                tx_rows.push(ArchiveV2HotTxRow {
                    tx_index,
                    flags: 0,
                    message_offset,
                    message_len,
                    metadata_offset: 0,
                    metadata_len: 0,
                    signature_count: 1,
                    reserved: [0; 3],
                });
            }
            let tx_count = u32::try_from(tx_rows.len()).unwrap();
            let block_id = u32::try_from(block_id).unwrap();
            let block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot,
                    parent_slot: slot - 1,
                    blockhash_id: block_id + 1,
                    previous_blockhash_id: block_id,
                    block_time: Some(1_750_200_000 + i64::from(block_id)),
                    block_height: Some(300_200_000 + u64::from(block_id)),
                    rewards: None,
                },
                tx_count,
                tx_rows,
                message_bytes,
                metadata_bytes: Vec::new(),
            };
            let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
            let compressed = zstd::bulk::compress(&uncompressed, 3).unwrap();
            index_rows.push(ArchiveV2HotBlockIndexRow {
                block_id,
                slot,
                compressed_offset: u64::try_from(compressed_blocks.len()).unwrap(),
                compressed_len: u32::try_from(compressed.len()).unwrap(),
                uncompressed_len: u32::try_from(uncompressed.len()).unwrap(),
                tx_count,
                first_tx_ordinal,
                first_signature_ordinal,
                signature_count: tx_count,
            });
            first_tx_ordinal += u64::from(tx_count);
            first_signature_ordinal += u64::from(tx_count);
            compressed_blocks.extend_from_slice(&compressed);
        }
        fs::write(epoch_root.join(ARCHIVE_V2_BLOCKS_FILE), &compressed_blocks).unwrap();
        write_archive_v2_hot_block_index(
            &epoch_root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed_blocks.len() as u64,
            2,
            0,
            &index_rows,
        )
        .unwrap();

        let mut signatures = Vec::new();
        signatures.extend_from_slice(&parse_signature(crate::format::SPYX_MINT_SIGNATURE).unwrap());
        signatures.extend_from_slice(&[71; 64]);
        signatures.extend_from_slice(&[72; 64]);
        signatures.extend_from_slice(&[73; 64]);
        fs::write(epoch_root.join(ARCHIVE_V2_SIGNATURES_FILE), signatures).unwrap();
        let mut archive_metadata = Vec::new();
        for record in [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks: 2,
                transactions: 4,
                ..WincodeArchiveV2Footer::default()
            }),
        ] {
            let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
            write_test_varint(&mut archive_metadata, u32::try_from(bytes.len()).unwrap());
            archive_metadata.extend_from_slice(&bytes);
        }
        fs::write(epoch_root.join(ARCHIVE_V2_META_FILE), archive_metadata).unwrap();
        archive_root
    }

    fn determinism_config(archive_root: &Path, output: PathBuf, workers: usize) -> ExtractConfig {
        ExtractConfig {
            archive_root: archive_root.to_path_buf(),
            output,
            mint: crate::format::SPYX_MINT.to_owned(),
            mint_slot: crate::format::SPYX_MINT_SLOT,
            mint_signature: crate::format::SPYX_MINT_SIGNATURE.to_owned(),
            workers,
            last_epoch: Some(801),
            source_mode: ExtractSourceMode::TrustedLocal {
                cluster_id: "determinism-fixture".to_owned(),
                slots_per_epoch: 432_000,
                wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            },
            resume: false,
            epoch_barrier: false,
            single_read_batches: false,
            single_read_match_hints: false,
            allow_indeterminate: false,
        }
    }

    #[test]
    fn trusted_local_mixed_metadata_admission_keeps_old_digest_and_selects_current() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let epoch_root = archive_root.join("epoch-801");
        let config = determinism_config(&archive_root, directory.path().join("unused"), 1);

        let historical = discover_epochs(&config).unwrap().remove(0);
        assert_eq!(
            historical.trusted_metadata_admission,
            Some(TrustedLocalMetadataAdmission::UnmarkedHistoricalCompatibility)
        );
        let historical_digest = historical.manifest.generation_digest.clone();

        write_test_generation_manifest(
            &epoch_root,
            801,
            "determinism-fixture",
            432_000,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            false,
        );
        let old_manifest = discover_epochs(&config).unwrap().remove(0);
        assert_eq!(
            old_manifest.trusted_metadata_admission,
            Some(TrustedLocalMetadataAdmission::UnmarkedHistoricalCompatibility)
        );
        assert_eq!(old_manifest.manifest.generation_digest, historical_digest);

        let published = write_test_generation_manifest(
            &epoch_root,
            801,
            "determinism-fixture",
            432_000,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            true,
        );
        let current = discover_epochs(&config).unwrap().remove(0);
        assert_eq!(
            current.trusted_metadata_admission,
            Some(TrustedLocalMetadataAdmission::PublishedCurrentTypedErrors)
        );
        assert_eq!(
            current.manifest.generation_digest,
            published.generation_digest
        );
        assert_ne!(current.manifest.generation_digest, historical_digest);
        let (_, reader) = open_epoch(&current, &config).unwrap();
        assert_eq!(
            reader.metadata_wire_profile(),
            ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1
        );
    }

    #[test]
    fn trusted_local_metadata_controls_fail_closed() {
        let directory = tempfile::tempdir().unwrap();

        let marker_only_archive = write_determinism_archive(&directory.path().join("marker-only"));
        let marker_only_epoch = marker_only_archive.join("epoch-801");
        fs::write(
            marker_only_epoch.join(CURRENT_TYPED_ERRORS_MARKER_FILE),
            CURRENT_TYPED_ERRORS_MARKER_BYTES,
        )
        .unwrap();
        let error = inspect_test_trusted_epoch(&marker_only_epoch)
            .unwrap_err()
            .to_string();
        assert!(error.contains("exists without archive-v2-generation.json"));

        let unknown_archive = write_determinism_archive(&directory.path().join("unknown"));
        let unknown_epoch = unknown_archive.join("epoch-801");
        fs::write(
            unknown_epoch.join("archive-v2-metadata-schema-unknown-v9.marker"),
            b"unknown\n",
        )
        .unwrap();
        let error = inspect_test_trusted_epoch(&unknown_epoch)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported metadata marker"));

        let missing_marker_archive =
            write_determinism_archive(&directory.path().join("missing-marker"));
        let missing_marker_epoch = missing_marker_archive.join("epoch-801");
        write_test_generation_manifest(
            &missing_marker_epoch,
            801,
            "determinism-fixture",
            432_000,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            true,
        );
        fs::remove_file(missing_marker_epoch.join(CURRENT_TYPED_ERRORS_MARKER_FILE)).unwrap();
        let error = inspect_test_trusted_epoch(&missing_marker_epoch)
            .unwrap_err()
            .to_string();
        assert!(error.contains("claims current metadata"));

        let wrong_bytes_archive = write_determinism_archive(&directory.path().join("wrong-bytes"));
        let wrong_bytes_epoch = wrong_bytes_archive.join("epoch-801");
        write_test_generation_manifest(
            &wrong_bytes_epoch,
            801,
            "determinism-fixture",
            432_000,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            true,
        );
        let mut wrong_bytes = CURRENT_TYPED_ERRORS_MARKER_BYTES.to_vec();
        wrong_bytes[0] ^= 1;
        fs::write(
            wrong_bytes_epoch.join(CURRENT_TYPED_ERRORS_MARKER_FILE),
            wrong_bytes,
        )
        .unwrap();
        let error = inspect_test_trusted_epoch(&wrong_bytes_epoch)
            .unwrap_err()
            .to_string();
        assert!(error.contains("marker bytes are not canonical"));

        let conflicting_archive = write_determinism_archive(&directory.path().join("conflicting"));
        let conflicting_epoch = conflicting_archive.join("epoch-801");
        write_test_generation_manifest(
            &conflicting_epoch,
            801,
            "determinism-fixture",
            432_000,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            true,
        );
        fs::write(
            conflicting_epoch.join("archive-v2-metadata-schema-other-v1.marker"),
            b"other\n",
        )
        .unwrap();
        let error = inspect_test_trusted_epoch(&conflicting_epoch)
            .unwrap_err()
            .to_string();
        assert!(error.contains("conflicting metadata markers"));
    }

    #[test]
    fn trusted_local_current_manifest_must_match_asserted_identity_and_profile() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let epoch_root = archive_root.join("epoch-801");
        let base = write_test_generation_manifest(
            &epoch_root,
            801,
            "determinism-fixture",
            432_000,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            true,
        );

        for (mut manifest, expected) in [
            (
                {
                    let mut manifest = base.clone();
                    manifest.epoch = 802;
                    manifest
                },
                "differs from directory epoch",
            ),
            (
                {
                    let mut manifest = base.clone();
                    manifest.cluster_id = "other-cluster".to_owned();
                    manifest
                },
                "differs from asserted cluster",
            ),
            (
                {
                    let mut manifest = base.clone();
                    manifest.slots_per_epoch = 431_999;
                    manifest
                },
                "differs from asserted",
            ),
        ] {
            persist_test_generation_manifest(&epoch_root, &mut manifest);
            let error = inspect_test_trusted_epoch(&epoch_root)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "unexpected error: {error}");
        }

        write_test_generation_manifest(
            &epoch_root,
            801,
            "determinism-fixture",
            432_000,
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            true,
        );
        let error = inspect_test_trusted_epoch(&epoch_root)
            .unwrap_err()
            .to_string();
        assert!(error.contains("differs from asserted"));
    }

    #[test]
    fn trusted_local_rechecks_cutover_and_resume_generation_bindings() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let epoch_root = archive_root.join("epoch-801");
        let historical_output = directory.path().join("historical-output");
        let mut historical_config = determinism_config(&archive_root, historical_output.clone(), 1);
        extract_epoch_shards(&historical_config).unwrap();

        write_test_generation_manifest(
            &epoch_root,
            801,
            "determinism-fixture",
            432_000,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            true,
        );
        let discovered_current = discover_epochs(&historical_config).unwrap().remove(0);
        fs::remove_file(epoch_root.join(CURRENT_TYPED_ERRORS_MARKER_FILE)).unwrap();
        let error = format!(
            "{:#}",
            open_epoch(&discovered_current, &historical_config).unwrap_err()
        );
        assert!(
            error.contains("physical marker is absent"),
            "unexpected cutover recheck error: {error}"
        );

        fs::write(
            epoch_root.join(CURRENT_TYPED_ERRORS_MARKER_FILE),
            CURRENT_TYPED_ERRORS_MARKER_BYTES,
        )
        .unwrap();
        historical_config.resume = true;
        let error = format!(
            "{:#}",
            extract_epoch_shards(&historical_config).unwrap_err()
        );
        assert!(
            error.contains("differs from the admitted source"),
            "unexpected resume error: {error}"
        );

        let current_output = directory.path().join("current-output");
        let mut current_config = determinism_config(&archive_root, current_output.clone(), 1);
        extract_epoch_shards(&current_config).unwrap();
        fs::remove_file(current_output.join(DUMP_MANIFEST_FILE)).unwrap();
        current_config.resume = true;
        extract_epoch_shards(&current_config).unwrap();
        assert!(current_output.join(DUMP_MANIFEST_FILE).is_file());
    }

    type RawRecordKey = (u64, u64, u32, u32);
    type NormalizedRawStream = (Vec<u8>, Vec<(RawRecordKey, Vec<u8>)>, Vec<u8>);

    fn normalized_raw_stream(path: &Path) -> NormalizedRawStream {
        let file = File::open(path).unwrap();
        let mut reader = WincodeLeb128FramedReader::new(BufReader::new(file));
        let mut header = None;
        let mut transactions = Vec::new();
        let mut footer = None;
        while let Some((_, frame)) = reader.read_bytes().unwrap() {
            let record: TokenTransactionDumpRecord = wincode::config::deserialize_exact(
                &frame,
                bounded_wincode_leb128_config::<
                    { blockzilla_primitives::WINCODE_LEB128_MAX_FRAME_BYTES },
                >(),
            )
            .unwrap();
            match record {
                TokenTransactionDumpRecord::Header(_) => {
                    assert!(header.replace(frame).is_none());
                }
                TokenTransactionDumpRecord::Transaction(transaction) => {
                    transactions.push((
                        (
                            transaction.source_epoch,
                            transaction.block.slot,
                            transaction.source_block_id,
                            transaction.tx_index,
                        ),
                        frame,
                    ));
                }
                TokenTransactionDumpRecord::Footer(_) => {
                    assert!(footer.replace(frame).is_none());
                }
            }
        }
        transactions.sort_unstable_by_key(|(key, _)| *key);
        (header.unwrap(), transactions, footer.unwrap())
    }

    #[test]
    fn two_pass_content_is_identical_with_one_and_twelve_workers() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let one = directory.path().join("one-worker");
        let twelve = directory.path().join("twelve-workers");
        extract_epoch_shards(&determinism_config(&archive_root, one.clone(), 1)).unwrap();
        extract_epoch_shards(&determinism_config(&archive_root, twelve.clone(), 12)).unwrap();

        let artifacts = [
            PathBuf::from(DISCOVERY_SHARDS_DIR)
                .join("epoch-801")
                .join(CREATIONS_FILE),
            PathBuf::from(ACCOUNTS_FILE),
            PathBuf::from(EPOCH_SHARDS_DIR)
                .join("epoch-801")
                .join(ACCOUNT_ID_LOG_FILE),
        ];
        for relative in artifacts {
            assert_eq!(
                fs::read(one.join(&relative)).unwrap(),
                fs::read(twelve.join(&relative)).unwrap(),
                "artifact differs with worker count: {}",
                relative.display()
            );
        }
        let raw_relative = PathBuf::from(EPOCH_SHARDS_DIR)
            .join("epoch-801")
            .join(TRANSACTIONS_FILE);
        assert_eq!(
            normalized_raw_stream(&one.join(&raw_relative)),
            normalized_raw_stream(&twelve.join(&raw_relative)),
            "raw transaction content differs with worker count"
        );

        let manifest: DumpManifest = serde_json::from_slice(
            &fs::read(
                one.join(EPOCH_SHARDS_DIR)
                    .join("epoch-801")
                    .join(DUMP_MANIFEST_FILE),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(manifest.transactions, 4);
    }

    #[test]
    fn initialize_mint_floor_includes_later_ata_creation_and_touch() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let output = directory.path().join("anchor-floor");
        extract_epoch_shards(&determinism_config(&archive_root, output.clone(), 1)).unwrap();

        let accounts: DiscoveredAccountList = wincode::config::deserialize_exact(
            &fs::read(output.join(ACCOUNTS_FILE)).unwrap(),
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        )
        .unwrap();
        assert_eq!(accounts.anchor_position.slot, crate::format::SPYX_MINT_SLOT);
        assert_eq!(accounts.accounts.len(), 1);
        assert_eq!(accounts.accounts[0].raw_pubkey, [44; 32]);
        assert_eq!(accounts.accounts[0].first_creation.slot, 346_083_696);

        let raw_path = output
            .join(EPOCH_SHARDS_DIR)
            .join("epoch-801")
            .join(TRANSACTIONS_FILE);
        let (_, transactions, _) = normalized_raw_stream(&raw_path);
        assert_eq!(
            transactions.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec![
                (801, crate::format::SPYX_MINT_SLOT, 0, 0),
                (801, 346_083_696, 2, 0),
                (801, 346_083_697, 3, 0),
                (801, 346_083_698, 4, 0),
            ]
        );
    }

    #[test]
    fn single_read_batches_match_two_pass_and_resume_after_root_commit_gap() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let two_pass = directory.path().join("two-pass");
        let single_read = directory.path().join("single-read");
        let hinted = directory.path().join("single-read-hints");

        extract_epoch_shards(&determinism_config(&archive_root, two_pass.clone(), 1)).unwrap();
        let mut single_config = determinism_config(&archive_root, single_read.clone(), 1);
        single_config.single_read_batches = true;
        extract_epoch_shards(&single_config).unwrap();
        let mut hinted_config = determinism_config(&archive_root, hinted.clone(), 1);
        hinted_config.single_read_batches = true;
        hinted_config.single_read_match_hints = true;
        extract_epoch_shards(&hinted_config).unwrap();

        for relative in [
            PathBuf::from(DISCOVERY_SHARDS_DIR)
                .join("epoch-801")
                .join(CREATIONS_FILE),
            PathBuf::from(ACCOUNTS_FILE),
            PathBuf::from(EPOCH_SHARDS_DIR)
                .join("epoch-801")
                .join(ACCOUNT_ID_LOG_FILE),
        ] {
            assert_eq!(
                fs::read(two_pass.join(&relative)).unwrap(),
                fs::read(single_read.join(&relative)).unwrap(),
                "single-read artifact differs from two-pass: {}",
                relative.display()
            );
            assert_eq!(
                fs::read(single_read.join(&relative)).unwrap(),
                fs::read(hinted.join(&relative)).unwrap(),
                "match-hint artifact differs from baseline single-read: {}",
                relative.display()
            );
        }
        let raw_relative = PathBuf::from(EPOCH_SHARDS_DIR)
            .join("epoch-801")
            .join(TRANSACTIONS_FILE);
        let normalized = normalized_raw_stream(&single_read.join(&raw_relative));
        assert_eq!(
            normalized,
            normalized_raw_stream(&two_pass.join(&raw_relative))
        );
        assert_eq!(
            normalized,
            normalized_raw_stream(&hinted.join(&raw_relative)),
            "match hints changed the selected raw transaction content"
        );
        let baseline_checkpoint: serde_json::Value = serde_json::from_slice(
            &fs::read(single_read.join(crate::resume::RESUME_CHECKPOINT_FILE)).unwrap(),
        )
        .unwrap();
        let hinted_checkpoint: serde_json::Value = serde_json::from_slice(
            &fs::read(hinted.join(crate::resume::RESUME_CHECKPOINT_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            baseline_checkpoint["payload"]["identity"], hinted_checkpoint["payload"]["identity"],
            "a performance option changed the resume identity"
        );
        assert_eq!(
            baseline_checkpoint["payload_sha256"], hinted_checkpoint["payload_sha256"],
            "a performance option changed the checkpoint hash"
        );
        assert_eq!(
            normalized.1.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec![
                (801, crate::format::SPYX_MINT_SLOT, 0, 0),
                (801, 346_083_696, 2, 0),
                (801, 346_083_697, 3, 0),
                (801, 346_083_698, 4, 0),
            ],
            "single-read mode did not preserve the account-creation floor"
        );

        // A crash can occur after the complete checkpoint is durable and
        // before the root manifest rename. Resume must validate the epoch and
        // recreate only that root file. Hints must accept this baseline
        // checkpoint, which omitted the performance flag.
        fs::remove_file(single_read.join(DUMP_MANIFEST_FILE)).unwrap();
        single_config.resume = true;
        single_config.single_read_match_hints = true;
        extract_epoch_shards(&single_config).unwrap();
        assert!(single_read.join(DUMP_MANIFEST_FILE).is_file());
        assert_eq!(
            normalized_raw_stream(&single_read.join(&raw_relative)),
            normalized
        );
    }

    #[test]
    fn wrong_resume_mode_does_not_move_existing_artifacts() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let output = directory.path().join("two-pass-root");
        extract_epoch_shards(&determinism_config(&archive_root, output.clone(), 1)).unwrap();
        let partial = create_partial_shard_directory(&output.join(EPOCH_SHARDS_DIR), 802).unwrap();
        fs::write(partial.join("kept"), b"unchanged").unwrap();

        let mut wrong_mode = determinism_config(&archive_root, output, 1);
        wrong_mode.single_read_batches = true;
        wrong_mode.resume = true;
        let error = extract_epoch_shards(&wrong_mode).unwrap_err().to_string();

        assert!(error.contains("identity differs"));
        assert_eq!(fs::read(partial.join("kept")).unwrap(), b"unchanged");
    }

    #[test]
    fn authenticated_resume_quarantines_truncated_pending_and_partial_pair() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let output = directory.path().join("single-read-root");
        let mut config = determinism_config(&archive_root, output.clone(), 1);
        config.single_read_batches = true;
        extract_epoch_shards(&config).unwrap();

        let discovery_partial =
            create_partial_shard_directory(&output.join(DISCOVERY_SHARDS_DIR), 802).unwrap();
        let raw_partial =
            create_partial_shard_directory(&output.join(EPOCH_SHARDS_DIR), 802).unwrap();
        fs::write(discovery_partial.join("kept"), b"discovery").unwrap();
        fs::write(raw_partial.join("kept"), b"raw").unwrap();
        fs::write(crate::resume::pending_checkpoint_path(&output), b"{").unwrap();

        config.resume = true;
        extract_epoch_shards(&config).unwrap();

        assert!(!discovery_partial.exists());
        assert!(!raw_partial.exists());
        assert!(!crate::resume::pending_checkpoint_path(&output).exists());
        assert!(
            fs::read_dir(&output)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".abandoned-resume-checkpoint"))
        );
    }

    #[test]
    fn empty_resume_recovers_truncated_checkpoint_staging_file() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let output = directory.path().join("empty-single-read-root");
        fs::create_dir(&output).unwrap();
        fs::create_dir(output.join(DISCOVERY_SHARDS_DIR)).unwrap();
        fs::create_dir(output.join(EPOCH_SHARDS_DIR)).unwrap();
        fs::write(pending_checkpoint_staging_path(&output), b"{").unwrap();

        let mut config = determinism_config(&archive_root, output.clone(), 1);
        config.single_read_batches = true;
        config.resume = true;
        extract_epoch_shards(&config).unwrap();

        assert!(output.join(DUMP_MANIFEST_FILE).is_file());
        assert!(!pending_checkpoint_staging_path(&output).exists());
        assert!(
            fs::read_dir(&output)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".abandoned-resume-checkpoint-staging"))
        );
    }

    #[test]
    fn single_read_epoch_reads_and_decompresses_each_fixture_block_once() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let output = directory.path().join("direct-single-read");
        fs::create_dir(&output).unwrap();
        let mut config = determinism_config(&archive_root, output.clone(), 2);
        config.single_read_batches = true;
        let input = discover_epochs(&config).unwrap().remove(0);
        let mut global_accounts = DiscoveredAccountMap::new();
        let mut anchor_position = None;
        let mut anchor_count = 0;

        let result = write_single_read_epoch(
            &config,
            &input,
            &output,
            parse_pubkey(&config.mint, "mint").unwrap(),
            parse_signature(&config.mint_signature).unwrap(),
            &mut global_accounts,
            &mut anchor_position,
            &mut anchor_count,
        )
        .unwrap();

        assert_eq!(result.reader.block_count, 5);
        assert_eq!(result.reader.read_call_count, result.reader.batch_count);
        assert_eq!(result.reader.decompression_count, result.reader.block_count);
        assert_eq!(result.reader.stage_a_block_count, result.reader.block_count);
        assert_eq!(result.reader.stage_b_block_count, result.reader.block_count);
        assert_eq!(result.shard.transactions, 4);
        assert_eq!(anchor_count, 1);
    }

    #[test]
    fn single_read_account_floor_applies_in_creation_batch_and_next_batch() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_match_hint_archive(directory.path());
        let output = directory.path().join("cross-batch-single-read");
        fs::create_dir(&output).unwrap();
        let mut config = determinism_config(&archive_root, output.clone(), 2);
        config.single_read_batches = true;
        config.single_read_match_hints = true;
        let input = discover_epochs(&config).unwrap().remove(0);
        let mut global_accounts = DiscoveredAccountMap::new();
        let mut anchor_position = None;
        let mut anchor_count = 0;
        let mut block_config = ordered_config(config.workers, true);
        block_config.max_blocks_per_batch = 4;

        let result = write_single_read_epoch_with_block_config(
            &config,
            &input,
            &output,
            parse_pubkey(&config.mint, "mint").unwrap(),
            parse_signature(&config.mint_signature).unwrap(),
            &mut global_accounts,
            &mut anchor_position,
            &mut anchor_count,
            block_config,
        )
        .unwrap();

        assert_eq!(result.reader.batch_count, 2);
        assert_eq!(result.reader.read_call_count, 2);
        assert_eq!(result.reader.decompression_count, 6);
        assert_eq!(result.reader.max_live_transaction_state_bytes, 4);
        assert_eq!(result.extractor.dirty_hint_batches, 1);
        assert_eq!(result.extractor.clean_hint_batches, 1);
        assert_eq!(result.extractor.hint_direct_matches, 1);
        assert_eq!(result.extractor.hint_skips_without_decode, 1);
        assert_eq!(result.extractor.hint_exact_reparses, 3);
        assert_eq!(global_accounts.get(&[44; 32]).unwrap().source_block_id, 2);
        let (_, transactions, _) = normalized_raw_stream(&output.join(TRANSACTIONS_FILE));
        assert_eq!(
            transactions.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec![
                (801, crate::format::SPYX_MINT_SLOT, 0, 0),
                (801, 346_083_696, 2, 0),
                (801, 346_083_697, 3, 0),
                (801, 346_083_698, 4, 0),
            ]
        );
    }

    #[test]
    fn match_hints_follow_noncanonical_storage_positions() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_noncanonical_match_hint_archive(directory.path());
        let output = directory.path().join("noncanonical-hints");
        fs::create_dir(&output).unwrap();
        let mut config = determinism_config(&archive_root, output.clone(), 2);
        config.single_read_batches = true;
        config.single_read_match_hints = true;
        let input = discover_epochs(&config).unwrap().remove(0);
        let mut global_accounts = DiscoveredAccountMap::new();
        let mut anchor_position = None;
        let mut anchor_count = 0;
        let mut block_config = ordered_config(config.workers, true);
        block_config.max_blocks_per_batch = 1;

        let result = write_single_read_epoch_with_block_config(
            &config,
            &input,
            &output,
            parse_pubkey(&config.mint, "mint").unwrap(),
            parse_signature(&config.mint_signature).unwrap(),
            &mut global_accounts,
            &mut anchor_position,
            &mut anchor_count,
            block_config,
        )
        .unwrap();

        assert_eq!(result.reader.batch_count, 2);
        assert_eq!(result.extractor.dirty_hint_batches, 1);
        assert_eq!(result.extractor.clean_hint_batches, 1);
        assert_eq!(result.extractor.hint_direct_matches, 1);
        assert_eq!(result.extractor.hint_skips_without_decode, 1);
        assert_eq!(result.extractor.hint_exact_reparses, 1);
        let (_, transactions, _) = normalized_raw_stream(&output.join(TRANSACTIONS_FILE));
        assert_eq!(
            transactions.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec![
                (801, crate::format::SPYX_MINT_SLOT, 0, 0),
                (801, crate::format::SPYX_MINT_SLOT, 0, 1),
                (801, crate::format::SPYX_MINT_SLOT + 1, 1, 1),
            ],
            "a storage-position hint selected the wrong canonical transaction index"
        );
    }

    #[test]
    fn repeated_creation_in_a_later_batch_uses_the_epoch_mapping_cache() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive_with_repeat(directory.path(), true);
        let output = directory.path().join("cached-cross-batch-single-read");
        fs::create_dir(&output).unwrap();
        let mut config = determinism_config(&archive_root, output.clone(), 2);
        config.single_read_batches = true;
        let input = discover_epochs(&config).unwrap().remove(0);
        let mut global_accounts = DiscoveredAccountMap::new();
        let mut anchor_position = None;
        let mut anchor_count = 0;
        let mut block_config = ordered_config(config.workers, true);
        block_config.max_blocks_per_batch = 4;

        let result = write_single_read_epoch_with_block_config(
            &config,
            &input,
            &output,
            parse_pubkey(&config.mint, "mint").unwrap(),
            parse_signature(&config.mint_signature).unwrap(),
            &mut global_accounts,
            &mut anchor_position,
            &mut anchor_count,
            block_config,
        )
        .unwrap();

        assert_eq!(result.reader.batch_count, 2);
        assert_eq!(result.extractor.creation_candidates, 2);
        assert_eq!(result.extractor.unique_candidate_ids, 1);
        assert_eq!(result.extractor.unique_candidate_raw_refs, 0);
        assert_eq!(result.extractor.new_accounts, 1);
        // The initial registry mapping reads the mint and the present token
        // program. The first account candidate adds one row. The repeated ID
        // in batch two must add no row, read call, byte, or MPHF lookup.
        assert_eq!(result.extractor.registry.registry_rows_read, 3);
        assert_eq!(result.extractor.registry.registry_coalesced_read_calls, 2);
        assert_eq!(result.extractor.registry.registry_read_bytes, 96);
        assert_eq!(result.extractor.registry.mphf_lookups, 4);
        assert_eq!(result.discovery.log.entries.len(), 1);
        assert_eq!(
            result.discovery.log.entries[0]
                .first_creation
                .source_block_id,
            2
        );
    }

    #[test]
    fn completed_single_read_dump_can_be_prepared_and_extended() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        let output = directory.path().join("prepared-extension");

        let mut first = determinism_config(&archive_root, output.clone(), 1);
        first.single_read_batches = true;
        extract_epoch_shards(&first).unwrap();
        fs::write(
            archive_root
                .join("epoch-801")
                .join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            [91; 32],
        )
        .unwrap();
        write_followup_touch_epoch(&archive_root, 802);

        let mut extended = determinism_config(&archive_root, output.clone(), 1);
        extended.single_read_batches = true;
        extended.last_epoch = Some(802);
        extended.resume = true;
        prepare_completed_single_read_extension(&extended).unwrap();

        assert!(output.join("manifest.completed-e801.json").is_file());
        assert!(output.join("accounts.completed-e801.wincode").is_file());
        assert!(
            output
                .join("resume-checkpoint.completed-e801.json")
                .is_file()
        );
        assert!(!output.join(DUMP_MANIFEST_FILE).exists());
        assert!(!output.join(ACCOUNTS_FILE).exists());

        extract_epoch_shards(&extended).unwrap();
        assert!(output.join(EPOCH_SHARDS_DIR).join("epoch-802").is_dir());
        let manifest: DumpManifest =
            serde_json::from_slice(&fs::read(output.join(DUMP_MANIFEST_FILE)).unwrap()).unwrap();
        assert_eq!(manifest.first_epoch, 801);
        assert_eq!(manifest.last_epoch, 802);
        assert!(manifest.complete);
    }

    #[test]
    fn single_read_resume_continues_from_a_validated_epoch_pair() {
        let directory = tempfile::tempdir().unwrap();
        let archive_root = write_determinism_archive(directory.path());
        write_followup_touch_epoch(&archive_root, 802);
        let output = directory.path().join("epoch-resume");

        let mut first = determinism_config(&archive_root, output.clone(), 1);
        first.single_read_batches = true;
        extract_epoch_shards(&first).unwrap();
        let accounts: DiscoveredAccountList = wincode::config::deserialize_exact(
            &fs::read(output.join(ACCOUNTS_FILE)).unwrap(),
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        )
        .unwrap();
        fs::remove_file(output.join(ACCOUNTS_FILE)).unwrap();
        fs::remove_file(output.join(DUMP_MANIFEST_FILE)).unwrap();

        let mut resumed = determinism_config(&archive_root, output.clone(), 1);
        resumed.single_read_batches = true;
        resumed.last_epoch = Some(802);
        resumed.resume = true;
        let inputs = discover_epochs(&resumed).unwrap();
        let discovery_path = output.join(DISCOVERY_SHARDS_DIR).join("epoch-801");
        let raw_path = output.join(EPOCH_SHARDS_DIR).join("epoch-801");
        let (discovery_binding, log) =
            validate_resume_discovery(&resumed, &inputs[0], &discovery_path, None).unwrap();
        let mut global_accounts = DiscoveredAccountMap::new();
        merge_discovery_accounts(&mut global_accounts, &log);
        let prefix = DiscoveredAccountList {
            schema_version: DUMP_SCHEMA_VERSION,
            mint: accounts.mint,
            anchor_position: accounts.anchor_position,
            accounts: global_accounts
                .iter()
                .map(|(raw_pubkey, first_creation)| DiscoveredAccount {
                    raw_pubkey: *raw_pubkey,
                    first_creation: *first_creation,
                })
                .collect(),
        };
        let raw_binding = validate_resume_shard(
            &resumed,
            &inputs[0],
            &raw_path,
            accounts.mint,
            parse_signature(&resumed.mint_signature).unwrap(),
            &prefix,
            None,
        )
        .unwrap();
        let identity = ResumeIdentity {
            dump_schema_version: DUMP_SCHEMA_VERSION,
            mint: resumed.mint.clone(),
            mint_slot: resumed.mint_slot,
            mint_signature: resumed.mint_signature.clone(),
            workers: resumed.workers,
            first_epoch: 801,
            last_epoch: 802,
            cluster_id: inputs[0].manifest.cluster_id.clone(),
            slots_per_epoch: inputs[0].manifest.slots_per_epoch,
            source_binding: dump_source_binding(&resumed),
            extraction_mode: ResumeExtractionMode::SingleReadBatches,
            single_read_match_hints: false,
        };
        let checkpoint = ResumeCheckpointPayload::new_single_read_batches(
            identity,
            Some(accounts.anchor_position),
            vec![discovery_binding],
            None,
            vec![raw_binding],
        )
        .unwrap();
        write_resume_checkpoint_atomic(&output, &checkpoint).unwrap();

        extract_epoch_shards(&resumed).unwrap();
        assert!(output.join(EPOCH_SHARDS_DIR).join("epoch-802").is_dir());
        let (_, transactions, _) = normalized_raw_stream(
            &output
                .join(EPOCH_SHARDS_DIR)
                .join("epoch-802")
                .join(TRANSACTIONS_FILE),
        );
        assert_eq!(transactions.len(), 2);
        assert_eq!(transactions[0].0, (802, 802 * 432_000, 0, 0));
        assert_eq!(transactions[1].0, (802, 802 * 432_000 + 1, 1, 0));
        let final_accounts: DiscoveredAccountList = wincode::config::deserialize_exact(
            &fs::read(output.join(ACCOUNTS_FILE)).unwrap(),
            bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
        )
        .unwrap();
        assert_eq!(
            final_accounts
                .accounts
                .iter()
                .map(|account| account.raw_pubkey)
                .collect::<Vec<_>>(),
            vec![[44; 32], [66; 32]]
        );
        let epoch_801_ids = read_epoch_account_id_log(
            &output
                .join(EPOCH_SHARDS_DIR)
                .join("epoch-801")
                .join(ACCOUNT_ID_LOG_FILE),
        )
        .unwrap();
        let epoch_802_ids = read_epoch_account_id_log(
            &output
                .join(EPOCH_SHARDS_DIR)
                .join("epoch-802")
                .join(ACCOUNT_ID_LOG_FILE),
        )
        .unwrap();
        assert_eq!(epoch_801_ids.entries.len(), 2);
        assert!(
            !epoch_801_ids
                .entries
                .iter()
                .any(|entry| entry.raw_pubkey == [66; 32])
        );
        assert_eq!(epoch_802_ids.entries.len(), 3);
        assert!(
            epoch_802_ids
                .entries
                .iter()
                .any(|entry| entry.raw_pubkey == [66; 32])
        );
        let loaded = load_resume_checkpoint(&output, &checkpoint.identity)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.payload.stage, ResumeStage::Complete);
        assert_eq!(loaded.payload.raw_shards.len(), 2);

        // A complete resume validates both prefix sidecars against the final
        // frozen account list without changing either epoch.
        extract_epoch_shards(&resumed).unwrap();
    }
}
