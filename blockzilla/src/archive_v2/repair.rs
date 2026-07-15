//! Bounded, restartable materialization of a published live repair bundle.
//!
//! This deliberately stops one step before canonical finalization. The output contains a
//! produced-ordinal no-registry stream, an exact bounded pubkey-run set, and the complete
//! blockhash registry. The incomplete PoH sidecar remains below the repair directory; this
//! module never emits canonical PoH, shredding, READY, or hot-archive metadata.

use super::{
    LIVE_FINALIZER_MAX_FRAME_SIZE, LiveNoRegistryBlockLegacy, PreviousBlockhash,
    build_block_access_sidecar_with_previous_tail, build_get_block_index_rows,
    read_prev_blockhash_tail, to_no_registry_transaction, write_prev_blockhash_tail,
};
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES, ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    ARCHIVE_V2_META_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, ArchiveV2BlockAccessBlob, ArchiveV2HotMetaRecord,
    CompactBlockHeader, CompactInnerInstruction, CompactInnerInstructions, CompactInstructionError,
    CompactTransactionError, WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
    WincodeArchiveV2NoRegistryBlock, WincodeArchiveV2NoRegistryBlockHeader,
    WincodeArchiveV2NoRegistryLogs, WincodeArchiveV2NoRegistryMeta,
    WincodeArchiveV2NoRegistryReturnData, WincodeArchiveV2NoRegistryReward,
    WincodeArchiveV2NoRegistryRewards, WincodeArchiveV2NoRegistryTokenBalance,
    WincodeArchiveV2NoRegistryTransaction, WincodeArchiveV2Payload, WincodeArchiveV2PohRecord,
    WincodeLeb128FramedReader, encode_with_scratch, framed::read_u32_varint,
    framed::write_u32_varint, read_archive_v2_block_access_index, read_archive_v2_get_block_index,
    read_archive_v2_hot_block_index, wincode_leb128_config,
};
use gxhash::{GxBuildHasher, HashMap as GxHashMap};
use of_car_reader::versioned_transaction::VersionedTransaction;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{info, warn};

const REPAIR_MARKER: &str = "REPAIR-REQUIRED.json";
const MATERIALIZED_MARKER: &str = "REPAIR-MATERIALIZED.json";
const COMPACTED_MARKER: &str = "REPAIR-COMPACTED.json";
const HOT_PROGRESS_JSON: &str = "repair/hot-progress.json";
const SOURCE_MATERIALIZED_MARKER: &str = "repair/source-REPAIR-MATERIALIZED.json";
const REPAIR_PLAN_DEFAULT: &str = "repair/live-merge-plan.jsonl";
const PRODUCED_BLOCKHASHES: &str = "repair/produced-blockhashes.bin";
const SOURCE_AVAILABLE_POH: &str = "repair/available-poh.wincode";
const OUTPUT_BLOCKS: &str = "blocks/live-no-registry-blocks.bin";
const OUTPUT_BLOCKHASHES: &str = "index/blockhash_registry.bin";
const OUTPUT_PUBKEY_RUNS: &str = "index/pubkey-runs";
const OUTPUT_AVAILABLE_POH: &str = "repair/available-poh.wincode";
const CHECKPOINT: &str = "repair/materialization-checkpoint.json";
/// Atomic progress snapshot consumed by Hive while the final output is still hidden.
pub(crate) const PROGRESS_JSON: &str = "repair/materialization-progress.json";
const MANIFEST_MAX_BYTES: u64 = 16 << 20;
const MARKER_MAX_BYTES: u64 = 2 << 20;
const PLAN_LINE_MAX_BYTES: usize = 64 << 10;
const POH_FRAME_MAX_BYTES: usize = 64 << 20;
const IO_BUFFER_BYTES: usize = 8 << 20;
const SOURCE_SCRATCH_RETAIN_BYTES: usize = 32 << 20;
const REPAIR_BLOCK_ACCESS_MAX_RPC_JSON_BYTES: u64 = 256 << 20;
const MATERIALIZER_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RepairMaterializeOptions {
    pub max_rpc_json_bytes: u64,
    pub checkpoint_every: u32,
    pub pubkey_run_max_keys: usize,
    /// Counts additional blocks in this invocation and leaves the hidden stage resumable.
    pub max_blocks: Option<u64>,
}

impl Default for RepairMaterializeOptions {
    fn default() -> Self {
        Self {
            max_rpc_json_bytes: 32 << 20,
            checkpoint_every: 256,
            pubkey_run_max_keys: 250_000,
            max_blocks: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RepairManifest {
    version: u16,
    state: String,
    epoch: u64,
    epoch_start_slot: u64,
    epoch_end_slot: u64,
    live_blocks: u64,
    rpc_only_blocks: u64,
    produced_blocks: u64,
    blockhash_records: u64,
    duplicate_live_blocks: u64,
    first_produced_slot: Option<u64>,
    last_produced_slot: Option<u64>,
    in_epoch_produced_parent_chain_validated: bool,
    poh: RepairPohManifest,
    normalized_frames: NormalizedFrameManifest,
    block_sources: Vec<RepairSourceManifest>,
    rpc_only_slots: Vec<RpcOnlyManifest>,
    merge_plan: String,
    pubkey_runs: RepairPubkeyRunManifest,
    publication_ready: bool,
}

#[derive(Debug, Deserialize)]
struct RepairPohManifest {
    path: String,
    records: u64,
    entries: u64,
    block_ids_rewritten: bool,
    rpc_only_records_omitted: u64,
    produced_id_space: u64,
    record_ids_have_explicit_rpc_gaps: bool,
    missing_record_ids: Vec<u32>,
}

#[derive(Debug, Deserialize)]
struct NormalizedFrameManifest {
    storage: String,
    header_blockhash_ids: String,
    header_previous_blockhash_ids: String,
    canonical_produced_ids_only_in_merge_plan: bool,
    materializer_must_remap_header_ids: bool,
    current_live_finalizer_compatible: bool,
}

#[derive(Debug, Deserialize)]
struct RepairPubkeyRunManifest {
    path: String,
    coverage: String,
    rpc_pubkeys_not_indexed: bool,
    registry_complete: bool,
}

#[derive(Debug, Deserialize)]
struct RepairSourceManifest {
    source_id: u32,
    retained_block_path: String,
    selected_blocks: u64,
    contributed_blocks: u64,
    selected_block_end_offset: u64,
    retained_block_bytes: u64,
    retention: String,
}

#[derive(Debug, Deserialize)]
struct RpcOnlyManifest {
    slot: u64,
    parent_slot: u64,
    blockhash: String,
    previous_blockhash: String,
    transactions: u64,
    source_path: String,
    source_bytes: u64,
    source_sha256: String,
    source_modified_nanos: u128,
    source_device: Option<u64>,
    source_inode: Option<u64>,
    state: String,
}

#[derive(Debug, Deserialize)]
struct MergePlanHeader {
    kind: String,
    version: u16,
    epoch: u64,
    expected_live_blocks: u64,
    expected_rpc_blocks: u64,
    expected_produced_blocks: u64,
    block_id_space: String,
    live_rows_have_explicit_rpc_gaps: bool,
    sources: Vec<MergePlanSource>,
}

#[derive(Debug, Deserialize)]
struct MergePlanSource {
    source_id: u32,
    block_path: String,
}

#[derive(Debug, Deserialize)]
struct MergePlanBlock {
    kind: String,
    block_id: u32,
    slot: u64,
    parent_slot: u64,
    source_id: u32,
    source_block_id: u32,
    source_offset: u64,
    block_len: u32,
    tx_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileStamp {
    relative_path: String,
    len: u64,
    modified_nanos: u128,
    device: Option<u64>,
    inode: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MaterializeCheckpoint {
    version: u16,
    manifest_sha256: String,
    plan_sha256: String,
    source_stamps: Vec<FileStamp>,
    next_block_id: u32,
    next_plan_offset: u64,
    block_stream_bytes: u64,
    run_count: u32,
    live_blocks: u64,
    rpc_blocks: u64,
    transactions: u64,
    started_unix_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct MaterializeProgress {
    version: u16,
    phase: String,
    epoch: u64,
    blocks_done: u64,
    blocks_total: u64,
    live_blocks_done: u64,
    live_blocks_total: u64,
    rpc_blocks_done: u64,
    rpc_blocks_total: u64,
    transactions_done: u64,
    output_block_bytes: u64,
    pubkey_run_files: u32,
    rss_bytes: Option<u64>,
    started_unix_secs: u64,
    elapsed_secs: u64,
    blocks_per_sec: f64,
    eta_secs: Option<u64>,
    updated_unix_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct MaterializedMarker {
    version: u16,
    state: String,
    canonical: bool,
    publication_ready: bool,
    epoch: u64,
    epoch_start_slot: u64,
    epoch_end_slot: u64,
    live_blocks: u64,
    rpc_only_blocks: u64,
    produced_blocks: u64,
    transactions: u64,
    first_produced_slot: Option<u64>,
    last_produced_slot: Option<u64>,
    normalized_blocks: String,
    produced_blockhashes: String,
    available_poh: String,
    exact_pubkey_runs: String,
    missing_poh_and_shredding_blocks: u64,
    manifest_sha256: String,
    merge_plan_sha256: String,
    progress_json: String,
    limitations: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct MaterializedForHot {
    pub epoch: u64,
    pub epoch_start_slot: u64,
    pub epoch_end_slot: u64,
    pub live_blocks: u64,
    pub rpc_only_blocks: u64,
    pub produced_blocks: u64,
    pub available_poh_records: u64,
    pub available_poh_entries: u64,
    pub missing_poh_ids: Vec<u32>,
    pub source_marker_sha256: String,
    pub source_manifest_sha256: String,
    pub source_plan_sha256: String,
    root: PathBuf,
    input_fingerprint: String,
}

pub(crate) struct DegradedHotStage {
    pub path: PathBuf,
    output: PathBuf,
    output_parent: PathBuf,
    _lock: File,
}

pub(crate) struct DegradedHotStats {
    pub blocks: u64,
    pub transactions: u64,
    pub signatures: u64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub zstd_level: i32,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DegradedHotFiles {
    blocks: String,
    index: String,
    meta: String,
    registry: String,
    registry_counts: String,
    registry_index: String,
    blockhashes: String,
    signatures: String,
    vote_hashes: String,
    available_poh: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    block_access: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    block_access_index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    get_block_index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous_blockhash_tail: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DegradedPohCoverage {
    available_records: u64,
    available_entries: u64,
    missing_records: u64,
    produced_id_space: u64,
    record_ids_have_explicit_gaps: bool,
    missing_record_ids: Vec<u32>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DegradedShreddingCoverage {
    available_records: u64,
    missing_records: u64,
    canonical_sidecar_emitted: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DegradedHotMarker {
    version: u16,
    state: String,
    canonical: bool,
    publication_ready: bool,
    block_archive_ready: bool,
    block_access_ready: bool,
    epoch: u64,
    epoch_start_slot: u64,
    epoch_end_slot: u64,
    live_blocks: u64,
    rpc_only_blocks: u64,
    produced_blocks: u64,
    transactions: u64,
    signatures: u64,
    zstd_level: i32,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    files: DegradedHotFiles,
    poh_coverage: DegradedPohCoverage,
    shredding_coverage: DegradedShreddingCoverage,
    source_materialized_marker_sha256: String,
    source_manifest_sha256: String,
    source_merge_plan_sha256: String,
    limitations: Vec<String>,
}

struct ValidatedContext {
    repair_root: PathBuf,
    manifest: RepairManifest,
    manifest_sha256: String,
    plan_path: PathBuf,
    plan_sha256: String,
    plan_header_end: u64,
    source_paths: Vec<PathBuf>,
    source_stamps: Vec<FileStamp>,
    rpc_block_ids: Vec<u32>,
}

struct SourceReader {
    file: File,
    selected_end_offset: u64,
    last_source_block_id: Option<u32>,
    last_source_end: u64,
}

struct PubkeyRuns {
    dir: PathBuf,
    counts: GxHashMap<[u8; 32], u32>,
    max_keys: usize,
    next_run: u32,
}

impl PubkeyRuns {
    fn new(dir: PathBuf, max_keys: usize, next_run: u32) -> Self {
        Self {
            dir,
            counts: GxHashMap::with_capacity_and_hasher(
                max_keys.min(1_000_000),
                GxBuildHasher::default(),
            ),
            max_keys,
            next_run,
        }
    }

    fn add(&mut self, key: [u8; 32]) -> Result<()> {
        self.counts
            .entry(key)
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
        if self.counts.len() >= self.max_keys {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.counts.is_empty() {
            return Ok(());
        }
        let mut records = self.counts.drain().collect::<Vec<_>>();
        records.sort_unstable_by_key(|(key, _)| *key);
        let path = self.dir.join(format!("run-{:08}.bin", self.next_run));
        let temp = self.dir.join(format!(".run-{:08}.tmp", self.next_run));
        let file = File::create(&temp).with_context(|| format!("create {}", temp.display()))?;
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
        for (key, count) in records {
            writer.write_all(&key)?;
            writer.write_all(&count.to_le_bytes())?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        fs::rename(&temp, &path)
            .with_context(|| format!("publish pubkey run {}", path.display()))?;
        sync_dir(&self.dir)?;
        self.next_run = self
            .next_run
            .checked_add(1)
            .context("pubkey run id overflow")?;
        Ok(())
    }
}

/// Build a produced-ordinal normalized stream from a published live/RPC repair view.
///
/// The authoritative completion receipt is output/REPAIR-MATERIALIZED.json. During work,
/// hidden-stage/repair/materialization-progress.json is replaced atomically at every durable
/// checkpoint. A partial invocation never creates the requested final output directory.
pub(crate) fn materialize_live_repair(
    repair_dir: &Path,
    output_dir: &Path,
    options: RepairMaterializeOptions,
) -> Result<()> {
    validate_options(options)?;
    let repair_root = fs::canonicalize(repair_dir)
        .with_context(|| format!("canonicalize repair dir {}", repair_dir.display()))?;
    anyhow::ensure!(repair_root.is_dir(), "repair input is not a directory");
    let output_parent = output_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(output_parent)
        .with_context(|| format!("create output parent {}", output_parent.display()))?;
    let output_parent = fs::canonicalize(output_parent)
        .with_context(|| format!("canonicalize output parent {}", output_parent.display()))?;
    let output_name = output_dir
        .file_name()
        .context("repair materialized output must have a final path component")?;
    let output = output_parent.join(output_name);
    anyhow::ensure!(
        !output.starts_with(&repair_root) && !repair_root.starts_with(&output),
        "repair output {} and input {} must not contain each other",
        output.display(),
        repair_root.display()
    );
    let stage = output_parent.join(format!(
        ".{}.repair-materialize-stage",
        output_name.to_string_lossy()
    ));
    let _lock = acquire_lock(&output_parent, output_name)?;

    let mut context = validate_repair_bundle(repair_root)?;
    validate_plan_and_partial_poh(&mut context)?;

    if output.exists() {
        validate_existing_output(&output, &context)?;
        info!(
            "Repair materialization already complete: {}",
            output.display()
        );
        return Ok(());
    }
    if stage.join(MATERIALIZED_MARKER).is_file() {
        validate_existing_output(&stage, &context)?;
        fs::rename(&stage, &output)
            .with_context(|| format!("publish completed repair stage {}", output.display()))?;
        sync_dir(&output_parent)?;
        return Ok(());
    }

    let checkpoint_path = stage.join(CHECKPOINT);
    let mut checkpoint = if checkpoint_path.is_file() {
        let checkpoint: MaterializeCheckpoint =
            read_json_bounded(&checkpoint_path, MARKER_MAX_BYTES)?;
        validate_checkpoint(&checkpoint, &context)?;
        checkpoint
    } else {
        if stage.exists() {
            fs::remove_dir_all(&stage)
                .with_context(|| format!("remove uncheckpointed stage {}", stage.display()))?;
        }
        initialize_stage(&stage, &context)?;
        let checkpoint = MaterializeCheckpoint {
            version: MATERIALIZER_VERSION,
            manifest_sha256: context.manifest_sha256.clone(),
            plan_sha256: context.plan_sha256.clone(),
            source_stamps: context.source_stamps.clone(),
            next_block_id: 0,
            next_plan_offset: context.plan_header_end,
            block_stream_bytes: 0,
            run_count: 0,
            live_blocks: 0,
            rpc_blocks: 0,
            transactions: 0,
            started_unix_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        write_checkpoint_and_progress(&stage, &context, &checkpoint, "materializing")?;
        checkpoint
    };

    revalidate_source_stamps(&context)?;
    remove_uncommitted_runs(&stage.join(OUTPUT_PUBKEY_RUNS), checkpoint.run_count)?;
    let blocks_path = stage.join(OUTPUT_BLOCKS);
    let blocks_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&blocks_path)
        .with_context(|| format!("open materialized blocks {}", blocks_path.display()))?;
    blocks_file.set_len(checkpoint.block_stream_bytes)?;
    let mut blocks_file = blocks_file;
    blocks_file.seek(SeekFrom::Start(checkpoint.block_stream_bytes))?;
    let mut blocks_writer = BufWriter::with_capacity(IO_BUFFER_BYTES, blocks_file);

    let mut plan_reader =
        BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&context.plan_path)?);
    plan_reader.seek(SeekFrom::Start(checkpoint.next_plan_offset))?;
    let mut sources = open_source_readers(&context)?;
    restore_source_positions(&context, checkpoint.next_plan_offset, &mut sources)?;
    let mut runs = PubkeyRuns::new(
        stage.join(OUTPUT_PUBKEY_RUNS),
        options.pubkey_run_max_keys,
        checkpoint.run_count,
    );
    let mut rpc_index = context
        .rpc_block_ids
        .partition_point(|block_id| *block_id < checkpoint.next_block_id);
    anyhow::ensure!(
        rpc_index == checkpoint.rpc_blocks as usize,
        "checkpoint RPC count does not match missing block ids"
    );
    let mut source_scratch = Vec::with_capacity(2 << 20);
    let mut output_scratch = Vec::with_capacity(2 << 20);
    let mut invocation_blocks = 0u64;

    while u64::from(checkpoint.next_block_id) < context.manifest.produced_blocks {
        let block_id = checkpoint.next_block_id;
        let is_rpc = context.rpc_block_ids.get(rpc_index).copied() == Some(block_id);
        let (mut block, plan_offset) = if is_rpc {
            let rpc = context
                .manifest
                .rpc_only_slots
                .get(rpc_index)
                .context("RPC manifest index out of bounds")?;
            let block = materialize_rpc_block(
                &context.repair_root,
                rpc,
                block_id,
                options.max_rpc_json_bytes,
            )
            .with_context(|| format!("materialize RPC slot {}", rpc.slot))?;
            rpc_index += 1;
            checkpoint.rpc_blocks = checkpoint.rpc_blocks.saturating_add(1);
            (block, checkpoint.next_plan_offset)
        } else {
            let line = read_bounded_line(&mut plan_reader, PLAN_LINE_MAX_BYTES)?
                .context("repair merge plan ended before all live blocks")?;
            let row: MergePlanBlock =
                serde_json::from_slice(&line).context("decode repair merge-plan block")?;
            validate_plan_row_for_materialization(&row, block_id, &mut sources)?;
            let block = read_source_block(
                &mut sources[row.source_id as usize],
                &row,
                &mut source_scratch,
            )?;
            anyhow::ensure!(
                block.txs.len() == row.tx_count as usize,
                "plan slot {} tx count {} != normalized frame {}",
                row.slot,
                row.tx_count,
                block.txs.len()
            );
            checkpoint.live_blocks = checkpoint.live_blocks.saturating_add(1);
            (block, plan_reader.stream_position()?)
        };

        remap_block_header(&mut block, block_id)?;
        visit_no_registry_pubkeys(&block, |key| runs.add(key))?;
        checkpoint.transactions = checkpoint
            .transactions
            .checked_add(block.txs.len() as u64)
            .context("materialized transaction count overflow")?;
        encode_with_scratch(&block, &mut output_scratch)?;
        anyhow::ensure!(
            output_scratch.len() <= LIVE_FINALIZER_MAX_FRAME_SIZE,
            "materialized slot {} frame is {} bytes, exceeding {}",
            block.header.compact.slot,
            output_scratch.len(),
            LIVE_FINALIZER_MAX_FRAME_SIZE
        );
        write_u32_varint(
            &mut blocks_writer,
            u32::try_from(output_scratch.len()).context("materialized frame exceeds u32")?,
        )?;
        blocks_writer.write_all(&output_scratch)?;
        checkpoint.next_block_id = checkpoint
            .next_block_id
            .checked_add(1)
            .context("materialized block id overflow")?;
        checkpoint.next_plan_offset = plan_offset;
        invocation_blocks = invocation_blocks.saturating_add(1);

        if source_scratch.capacity() > SOURCE_SCRATCH_RETAIN_BYTES {
            source_scratch = Vec::with_capacity(2 << 20);
        }
        if output_scratch.capacity() > SOURCE_SCRATCH_RETAIN_BYTES {
            output_scratch = Vec::with_capacity(2 << 20);
        }

        let checkpoint_due = checkpoint.next_block_id % options.checkpoint_every == 0;
        let invocation_limit = options
            .max_blocks
            .is_some_and(|limit| invocation_blocks >= limit);
        if checkpoint_due || invocation_limit {
            durable_checkpoint(
                &stage,
                &context,
                &mut checkpoint,
                &mut blocks_writer,
                &mut runs,
            )?;
        }
        if invocation_limit {
            info!(
                "Repair materialization paused at block {}/{}; rerun the same command to resume (stage={})",
                checkpoint.next_block_id,
                context.manifest.produced_blocks,
                stage.display()
            );
            return Ok(());
        }
    }

    durable_checkpoint(
        &stage,
        &context,
        &mut checkpoint,
        &mut blocks_writer,
        &mut runs,
    )?;
    drop(blocks_writer);
    let extra = read_bounded_line(&mut plan_reader, PLAN_LINE_MAX_BYTES)?;
    anyhow::ensure!(extra.is_none(), "repair merge plan has extra live rows");
    anyhow::ensure!(
        checkpoint.live_blocks == context.manifest.live_blocks
            && checkpoint.rpc_blocks == context.manifest.rpc_only_blocks,
        "materialized source counts do not match manifest"
    );
    revalidate_source_stamps(&context)?;

    let marker = MaterializedMarker {
        version: MATERIALIZER_VERSION,
        state: "repair_materialized_missing_poh_and_shredding".to_string(),
        canonical: false,
        publication_ready: false,
        epoch: context.manifest.epoch,
        epoch_start_slot: context.manifest.epoch_start_slot,
        epoch_end_slot: context.manifest.epoch_end_slot,
        live_blocks: checkpoint.live_blocks,
        rpc_only_blocks: checkpoint.rpc_blocks,
        produced_blocks: u64::from(checkpoint.next_block_id),
        transactions: checkpoint.transactions,
        first_produced_slot: context.manifest.first_produced_slot,
        last_produced_slot: context.manifest.last_produced_slot,
        normalized_blocks: OUTPUT_BLOCKS.to_string(),
        produced_blockhashes: OUTPUT_BLOCKHASHES.to_string(),
        available_poh: OUTPUT_AVAILABLE_POH.to_string(),
        exact_pubkey_runs: OUTPUT_PUBKEY_RUNS.to_string(),
        missing_poh_and_shredding_blocks: context.manifest.rpc_only_blocks,
        manifest_sha256: context.manifest_sha256.clone(),
        merge_plan_sha256: context.plan_sha256.clone(),
        progress_json: PROGRESS_JSON.to_string(),
        limitations: vec![
            "RPC-only slots have complete getBlock transaction/meta materialization but no original PoH entries or shred-boundary metadata.".to_string(),
            "The PoH sidecar is intentionally retained only at repair/available-poh.wincode with explicit produced-id gaps.".to_string(),
            "This directory is not canonical and must not be promoted to completed/finalized state until missing sidecars are repaired or policy explicitly accepts the degraded tier.".to_string(),
        ],
    };
    write_progress(&stage, &context, &checkpoint, "complete_noncanonical")?;
    sync_tree_candidates(&stage)?;
    write_json_atomic(&stage.join(MATERIALIZED_MARKER), &marker)?;
    sync_dir(&stage)?;
    fs::rename(&stage, &output)
        .with_context(|| format!("publish repair materialization {}", output.display()))?;
    sync_dir(&output_parent)?;
    info!(
        "Repair materialization complete (noncanonical): epoch={} blocks={} live={} rpc={} txs={} output={}",
        marker.epoch,
        marker.produced_blocks,
        marker.live_blocks,
        marker.rpc_only_blocks,
        marker.transactions,
        output.display()
    );
    Ok(())
}

fn validate_options(options: RepairMaterializeOptions) -> Result<()> {
    anyhow::ensure!(
        options.max_rpc_json_bytes > 0,
        "max RPC JSON bytes must be positive"
    );
    anyhow::ensure!(
        options.checkpoint_every > 0,
        "checkpoint interval must be positive"
    );
    anyhow::ensure!(
        options.pubkey_run_max_keys > 0,
        "pubkey run key cap must be positive"
    );
    Ok(())
}

fn validate_repair_bundle(repair_root: PathBuf) -> Result<ValidatedContext> {
    let marker_path = repair_root.join(REPAIR_MARKER);
    ensure_regular_file(&marker_path, MANIFEST_MAX_BYTES)?;
    let manifest_bytes =
        fs::read(&marker_path).with_context(|| format!("read {}", marker_path.display()))?;
    let manifest_sha256 = hex_digest(Sha256::digest(&manifest_bytes));
    let manifest: RepairManifest = serde_json::from_slice(&manifest_bytes)
        .with_context(|| format!("decode {}", marker_path.display()))?;
    validate_manifest(&manifest)?;

    let plan_path = resolve_relative(&repair_root, &manifest.merge_plan, "merge plan")?;
    ensure_regular_file(&plan_path, u64::MAX)?;
    let plan_sha256 = sha256_file_hex(&plan_path)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&plan_path)?);
    let header_line = read_bounded_line(&mut reader, PLAN_LINE_MAX_BYTES)?
        .context("repair merge plan is empty")?;
    let header: MergePlanHeader =
        serde_json::from_slice(&header_line).context("decode repair merge-plan header")?;
    validate_plan_header(&header, &manifest)?;
    let plan_header_end = reader.stream_position()?;

    let blockhash_path = resolve_relative(
        &repair_root,
        PRODUCED_BLOCKHASHES,
        "produced blockhash registry",
    )?;
    let poh_path = resolve_relative(&repair_root, &manifest.poh.path, "available PoH")?;
    let mut source_paths = Vec::with_capacity(manifest.block_sources.len());
    let mut source_stamps = [&marker_path, &plan_path, &blockhash_path, &poh_path]
        .into_iter()
        .map(|path| file_stamp(&repair_root, path))
        .collect::<Result<Vec<_>>>()?;
    for (index, source) in manifest.block_sources.iter().enumerate() {
        anyhow::ensure!(
            source.source_id as usize == index,
            "repair source ids must be contiguous"
        );
        let path = resolve_relative(&repair_root, &source.retained_block_path, "block source")?;
        ensure_regular_file(&path, u64::MAX)?;
        let metadata = fs::metadata(&path)?;
        anyhow::ensure!(
            metadata.len() == source.retained_block_bytes
                && source.selected_block_end_offset <= metadata.len(),
            "repair source {} length changed or selected prefix is out of bounds",
            path.display()
        );
        source_stamps.push(file_stamp(&repair_root, &path)?);
        source_paths.push(path);
    }
    anyhow::ensure!(
        header.sources.len() == source_paths.len(),
        "plan/header source count mismatch"
    );
    for (index, source) in header.sources.iter().enumerate() {
        anyhow::ensure!(
            source.source_id as usize == index,
            "plan source ids must be contiguous"
        );
        anyhow::ensure!(
            source.block_path == manifest.block_sources[index].retained_block_path,
            "plan source path differs from manifest source {}",
            index
        );
    }

    let rpc_block_ids = manifest.poh.missing_record_ids.clone();
    Ok(ValidatedContext {
        repair_root,
        manifest,
        manifest_sha256,
        plan_path,
        plan_sha256,
        plan_header_end,
        source_paths,
        source_stamps,
        rpc_block_ids,
    })
}

fn validate_manifest(manifest: &RepairManifest) -> Result<()> {
    anyhow::ensure!(
        manifest.version == 1,
        "unsupported repair manifest version {}",
        manifest.version
    );
    anyhow::ensure!(
        manifest.state == "rpc_fallback_missing_poh_and_shredding",
        "unsupported repair state {}",
        manifest.state
    );
    anyhow::ensure!(
        !manifest.publication_ready,
        "repair manifest unexpectedly publication-ready"
    );
    anyhow::ensure!(
        manifest.epoch_end_slot >= manifest.epoch_start_slot,
        "invalid epoch bounds"
    );
    let epoch_slots = manifest
        .epoch_end_slot
        .checked_sub(manifest.epoch_start_slot)
        .and_then(|value| value.checked_add(1))
        .context("epoch span overflow")?;
    anyhow::ensure!(
        epoch_slots <= crate::SLOTS_PER_EPOCH,
        "repair epoch span is too large"
    );
    anyhow::ensure!(
        manifest.live_blocks.checked_add(manifest.rpc_only_blocks)
            == Some(manifest.produced_blocks),
        "repair produced block accounting mismatch"
    );
    anyhow::ensure!(
        manifest.produced_blocks > 0 && manifest.produced_blocks <= epoch_slots,
        "repair produced block count is outside epoch bounds"
    );
    anyhow::ensure!(
        manifest.blockhash_records == manifest.produced_blocks,
        "blockhash count mismatch"
    );
    anyhow::ensure!(
        manifest.in_epoch_produced_parent_chain_validated,
        "repair parent chain is not validated"
    );
    anyhow::ensure!(
        manifest.first_produced_slot.is_some() && manifest.last_produced_slot.is_some(),
        "non-empty repair manifest has no produced slot bounds"
    );
    anyhow::ensure!(
        manifest.poh.path == SOURCE_AVAILABLE_POH
            && manifest.poh.records == manifest.live_blocks
            && manifest.poh.rpc_only_records_omitted == manifest.rpc_only_blocks
            && manifest.poh.produced_id_space == manifest.produced_blocks
            && manifest.poh.block_ids_rewritten
            && manifest.poh.record_ids_have_explicit_rpc_gaps,
        "repair PoH declaration is incompatible"
    );
    anyhow::ensure!(
        manifest.poh.missing_record_ids.len() as u64 == manifest.rpc_only_blocks,
        "repair PoH missing-id count mismatch"
    );
    let mut previous = None;
    for id in &manifest.poh.missing_record_ids {
        anyhow::ensure!(
            u64::from(*id) < manifest.produced_blocks,
            "missing PoH id is out of bounds"
        );
        anyhow::ensure!(
            previous.is_none_or(|value| *id > value),
            "missing PoH ids are not sorted/unique"
        );
        previous = Some(*id);
    }
    anyhow::ensure!(
        manifest.normalized_frames.storage == "hard_linked_unmodified"
            && manifest.normalized_frames.header_blockhash_ids == "source_local"
            && manifest.normalized_frames.header_previous_blockhash_ids == "source_local"
            && manifest
                .normalized_frames
                .canonical_produced_ids_only_in_merge_plan
            && manifest
                .normalized_frames
                .materializer_must_remap_header_ids
            && !manifest.normalized_frames.current_live_finalizer_compatible,
        "repair normalized-frame contract is incompatible"
    );
    anyhow::ensure!(
        manifest.pubkey_runs.coverage == "live_blocks_only"
            && manifest.pubkey_runs.rpc_pubkeys_not_indexed
            && !manifest.pubkey_runs.registry_complete
            && manifest.pubkey_runs.path == "repair/live-pubkey-runs",
        "repair pubkey-run contract is incompatible"
    );
    anyhow::ensure!(
        manifest.merge_plan == REPAIR_PLAN_DEFAULT,
        "unexpected repair merge-plan path"
    );
    anyhow::ensure!(
        !manifest.block_sources.is_empty() && manifest.block_sources.len() <= 64,
        "repair source count must be in 1..=64"
    );
    anyhow::ensure!(
        manifest.rpc_only_slots.len() as u64 == manifest.rpc_only_blocks,
        "RPC slot count mismatch"
    );
    let contributed = manifest
        .block_sources
        .iter()
        .try_fold(0u64, |sum, source| {
            anyhow::ensure!(
                source.retention == "hard_link",
                "unsupported source retention"
            );
            anyhow::ensure!(source.selected_blocks > 0, "empty selected source slice");
            sum.checked_add(source.contributed_blocks)
                .context("contributed count overflow")
        })?;
    anyhow::ensure!(
        contributed == manifest.live_blocks,
        "source contributed count mismatch"
    );
    let selected = manifest
        .block_sources
        .iter()
        .try_fold(0u64, |sum, source| {
            sum.checked_add(source.selected_blocks)
                .context("selected source count overflow")
        })?;
    anyhow::ensure!(
        selected
            == manifest
                .live_blocks
                .checked_add(manifest.duplicate_live_blocks)
                .context("live/duplicate count overflow")?,
        "selected source rows do not match live plus duplicate rows"
    );
    let mut previous_slot = None;
    for rpc in &manifest.rpc_only_slots {
        anyhow::ensure!(rpc.state == "rpc_fallback", "unsupported RPC repair state");
        anyhow::ensure!(
            (manifest.epoch_start_slot..=manifest.epoch_end_slot).contains(&rpc.slot),
            "RPC slot {} outside epoch",
            rpc.slot
        );
        anyhow::ensure!(
            previous_slot.is_none_or(|slot| rpc.slot > slot),
            "RPC slots are not sorted/unique"
        );
        anyhow::ensure!(
            rpc.transactions <= u32::MAX as u64,
            "RPC transaction count exceeds u32"
        );
        anyhow::ensure!(rpc.source_bytes > 0, "RPC source is empty");
        decode_hex_32(&rpc.source_sha256).context("invalid RPC source sha256")?;
        decode_base58_32(&rpc.blockhash, "RPC blockhash")?;
        decode_base58_32(&rpc.previous_blockhash, "RPC previousBlockhash")?;
        previous_slot = Some(rpc.slot);
    }
    Ok(())
}

fn validate_plan_header(header: &MergePlanHeader, manifest: &RepairManifest) -> Result<()> {
    anyhow::ensure!(
        header.kind == "header" && header.version == 1,
        "unsupported merge-plan header"
    );
    anyhow::ensure!(header.epoch == manifest.epoch, "merge-plan epoch mismatch");
    anyhow::ensure!(
        header.expected_live_blocks == manifest.live_blocks
            && header.expected_rpc_blocks == manifest.rpc_only_blocks
            && header.expected_produced_blocks == manifest.produced_blocks,
        "merge-plan count mismatch"
    );
    anyhow::ensure!(
        header.block_id_space == "produced_ordinal" && header.live_rows_have_explicit_rpc_gaps,
        "merge-plan id-space contract is incompatible"
    );
    Ok(())
}

fn validate_plan_and_partial_poh(context: &mut ValidatedContext) -> Result<()> {
    let blockhash_path = resolve_relative(
        &context.repair_root,
        PRODUCED_BLOCKHASHES,
        "produced blockhash registry",
    )?;
    ensure_regular_file(&blockhash_path, u64::MAX)?;
    let expected_hash_bytes = context
        .manifest
        .produced_blocks
        .checked_mul(32)
        .context("produced blockhash byte length overflow")?;
    anyhow::ensure!(
        fs::metadata(&blockhash_path)?.len() == expected_hash_bytes,
        "produced blockhash registry byte length mismatch"
    );
    let mut blockhash_reader =
        BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&blockhash_path)?);

    let poh_path = resolve_relative(
        &context.repair_root,
        &context.manifest.poh.path,
        "available PoH",
    )?;
    ensure_regular_file(&poh_path, u64::MAX)?;
    let mut poh_reader = WincodeLeb128FramedReader::new(BufReader::with_capacity(
        IO_BUFFER_BYTES,
        File::open(&poh_path)?,
    ));

    let mut plan_reader =
        BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&context.plan_path)?);
    let _header = read_bounded_line(&mut plan_reader, PLAN_LINE_MAX_BYTES)?
        .context("repair merge plan is empty")?;
    anyhow::ensure!(
        plan_reader.stream_position()? == context.plan_header_end,
        "merge-plan header offset changed"
    );

    let mut source_last = vec![None::<(u32, u64)>; context.source_paths.len()];
    let mut source_rows = vec![0u64; context.source_paths.len()];
    let mut missing_index = 0usize;
    let mut live_rows = 0u64;
    let mut rpc_rows = 0u64;
    let mut poh_entries = 0u64;
    let mut previous: Option<(u64, [u8; 32])> = None;
    let mut first_slot = None;
    let mut last_slot = None;

    for produced_id_u64 in 0..context.manifest.produced_blocks {
        let produced_id =
            u32::try_from(produced_id_u64).context("produced block id exceeds u32")?;
        let mut current_hash = [0u8; 32];
        blockhash_reader
            .read_exact(&mut current_hash)
            .with_context(|| format!("read produced blockhash id {}", produced_id))?;

        let is_rpc = context.rpc_block_ids.get(missing_index).copied() == Some(produced_id);
        let (slot, parent_slot) = if is_rpc {
            let rpc = context
                .manifest
                .rpc_only_slots
                .get(missing_index)
                .context("missing RPC manifest row")?;
            let rpc_path =
                resolve_relative(&context.repair_root, &rpc.source_path, "RPC getBlock")?;
            validate_rpc_source_stamp(&rpc_path, rpc, false)?;
            anyhow::ensure!(
                decode_base58_32(&rpc.blockhash, "RPC blockhash")? == current_hash,
                "RPC slot {} blockhash differs from produced registry",
                rpc.slot
            );
            if let Some((previous_slot, previous_hash)) = previous {
                anyhow::ensure!(
                    rpc.parent_slot == previous_slot,
                    "RPC slot {} parent {} does not follow produced slot {}",
                    rpc.slot,
                    rpc.parent_slot,
                    previous_slot
                );
                anyhow::ensure!(
                    decode_base58_32(&rpc.previous_blockhash, "RPC previousBlockhash")?
                        == previous_hash,
                    "RPC slot {} previousBlockhash differs from produced predecessor",
                    rpc.slot
                );
            }
            missing_index += 1;
            rpc_rows += 1;
            (rpc.slot, rpc.parent_slot)
        } else {
            let line = read_bounded_line(&mut plan_reader, PLAN_LINE_MAX_BYTES)?
                .context("merge plan ended before expected live block count")?;
            let row: MergePlanBlock =
                serde_json::from_slice(&line).context("decode merge-plan block row")?;
            validate_plan_row_static(context, &row, produced_id, &mut source_last)?;
            source_rows[row.source_id as usize] =
                source_rows[row.source_id as usize].saturating_add(1);
            if let Some((previous_slot, _)) = previous {
                anyhow::ensure!(
                    row.parent_slot == previous_slot,
                    "live slot {} parent {} does not follow produced slot {}",
                    row.slot,
                    row.parent_slot,
                    previous_slot
                );
            }
            let poh = poh_reader
                .read_bytes_with_limit(POH_FRAME_MAX_BYTES, |bytes| {
                    wincode::config::deserialize::<WincodeArchiveV2PohRecord, _>(
                        bytes,
                        wincode_leb128_config(),
                    )
                    .map_err(|error| anyhow!("{error}"))
                })?
                .map(|(_, record)| record)
                .context("available PoH ended before merge plan")?;
            anyhow::ensure!(
                poh.block_id == row.block_id && poh.slot == row.slot,
                "PoH row ({}, {}) differs from plan ({}, {})",
                poh.block_id,
                poh.slot,
                row.block_id,
                row.slot
            );
            poh_entries = poh_entries
                .checked_add(poh.entries.len() as u64)
                .context("PoH entry count overflow")?;
            live_rows += 1;
            (row.slot, row.parent_slot)
        };
        anyhow::ensure!(
            (context.manifest.epoch_start_slot..=context.manifest.epoch_end_slot).contains(&slot),
            "produced slot {} outside epoch",
            slot
        );
        if let Some((previous_slot, _)) = previous {
            anyhow::ensure!(
                slot > previous_slot,
                "produced slots are not strictly increasing: {} after {}",
                slot,
                previous_slot
            );
        }
        let _ = parent_slot;
        first_slot.get_or_insert(slot);
        last_slot = Some(slot);
        previous = Some((slot, current_hash));
    }

    anyhow::ensure!(
        missing_index == context.rpc_block_ids.len()
            && rpc_rows == context.manifest.rpc_only_blocks,
        "RPC produced-id coverage mismatch"
    );
    anyhow::ensure!(
        live_rows == context.manifest.live_blocks,
        "live plan row count mismatch"
    );
    anyhow::ensure!(
        poh_entries == context.manifest.poh.entries,
        "available PoH entry count {} != manifest {}",
        poh_entries,
        context.manifest.poh.entries
    );
    anyhow::ensure!(
        first_slot == context.manifest.first_produced_slot
            && last_slot == context.manifest.last_produced_slot,
        "produced slot bounds differ from manifest"
    );
    for (index, rows) in source_rows.into_iter().enumerate() {
        anyhow::ensure!(
            rows == context.manifest.block_sources[index].contributed_blocks,
            "source {} contributed {} plan rows, expected {}",
            index,
            rows,
            context.manifest.block_sources[index].contributed_blocks
        );
    }
    anyhow::ensure!(
        read_bounded_line(&mut plan_reader, PLAN_LINE_MAX_BYTES)?.is_none(),
        "merge plan has extra block rows"
    );
    anyhow::ensure!(
        poh_reader
            .read_bytes_with_limit(POH_FRAME_MAX_BYTES, |_| Ok(()))?
            .is_none(),
        "available PoH has extra records"
    );
    let mut trailing = [0u8; 1];
    anyhow::ensure!(
        blockhash_reader.read(&mut trailing)? == 0,
        "produced blockhash registry has trailing bytes"
    );
    Ok(())
}

fn validate_plan_row_static(
    context: &ValidatedContext,
    row: &MergePlanBlock,
    expected_block_id: u32,
    source_last: &mut [Option<(u32, u64)>],
) -> Result<()> {
    anyhow::ensure!(
        row.kind == "block",
        "unexpected merge-plan row kind {}",
        row.kind
    );
    anyhow::ensure!(
        row.block_id == expected_block_id,
        "merge-plan block id {} != expected {}",
        row.block_id,
        expected_block_id
    );
    let source_index = row.source_id as usize;
    let source = context
        .manifest
        .block_sources
        .get(source_index)
        .context("merge-plan source id out of bounds")?;
    anyhow::ensure!(
        row.block_len > 0 && row.block_len as usize <= LIVE_FINALIZER_MAX_FRAME_SIZE,
        "merge-plan frame length {} is invalid",
        row.block_len
    );
    let end = row
        .source_offset
        .checked_add(varint_len(row.block_len) as u64)
        .and_then(|value| value.checked_add(u64::from(row.block_len)))
        .context("merge-plan source frame end overflow")?;
    anyhow::ensure!(
        end <= source.selected_block_end_offset,
        "merge-plan slot {} frame ends outside selected source prefix",
        row.slot
    );
    if let Some((previous_source_id, previous_end)) = source_last[source_index] {
        anyhow::ensure!(
            row.source_block_id > previous_source_id && row.source_offset >= previous_end,
            "merge-plan source {} rows overlap or regress",
            source_index
        );
    }
    source_last[source_index] = Some((row.source_block_id, end));
    Ok(())
}

fn initialize_stage(stage: &Path, context: &ValidatedContext) -> Result<()> {
    for relative in ["blocks", "index/pubkey-runs", "repair"] {
        fs::create_dir_all(stage.join(relative))
            .with_context(|| format!("create materializer stage {}", relative))?;
    }
    let source_blockhashes = resolve_relative(
        &context.repair_root,
        PRODUCED_BLOCKHASHES,
        "produced blockhash registry",
    )?;
    hard_link_or_copy_atomic(&source_blockhashes, &stage.join(OUTPUT_BLOCKHASHES))?;
    let source_poh = resolve_relative(
        &context.repair_root,
        &context.manifest.poh.path,
        "available PoH",
    )?;
    hard_link_or_copy_atomic(&source_poh, &stage.join(OUTPUT_AVAILABLE_POH))?;
    sync_dir(&stage.join("blocks"))?;
    sync_dir(&stage.join("index/pubkey-runs"))?;
    sync_dir(&stage.join("index"))?;
    sync_dir(&stage.join("repair"))?;
    sync_dir(stage)?;
    Ok(())
}

fn open_source_readers(context: &ValidatedContext) -> Result<Vec<SourceReader>> {
    context
        .source_paths
        .iter()
        .zip(&context.manifest.block_sources)
        .map(|(path, manifest)| {
            Ok(SourceReader {
                file: File::open(path)
                    .with_context(|| format!("open source block file {}", path.display()))?,
                selected_end_offset: manifest.selected_block_end_offset,
                last_source_block_id: None,
                last_source_end: 0,
            })
        })
        .collect()
}

fn restore_source_positions(
    context: &ValidatedContext,
    checkpoint_plan_offset: u64,
    sources: &mut [SourceReader],
) -> Result<()> {
    if checkpoint_plan_offset == context.plan_header_end {
        return Ok(());
    }
    anyhow::ensure!(
        checkpoint_plan_offset > context.plan_header_end,
        "checkpoint plan offset precedes the header"
    );
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, File::open(&context.plan_path)?);
    reader.seek(SeekFrom::Start(context.plan_header_end))?;
    loop {
        let before = reader.stream_position()?;
        if before == checkpoint_plan_offset {
            return Ok(());
        }
        anyhow::ensure!(
            before < checkpoint_plan_offset,
            "checkpoint plan offset is not at a row boundary"
        );
        let line = read_bounded_line(&mut reader, PLAN_LINE_MAX_BYTES)?
            .context("checkpoint plan offset is past EOF")?;
        let row: MergePlanBlock =
            serde_json::from_slice(&line).context("decode prior merge-plan row")?;
        validate_plan_row_for_materialization(&row, row.block_id, sources)?;
    }
}

fn validate_plan_row_for_materialization(
    row: &MergePlanBlock,
    expected_block_id: u32,
    sources: &mut [SourceReader],
) -> Result<()> {
    anyhow::ensure!(
        row.kind == "block",
        "unexpected merge-plan row kind {}",
        row.kind
    );
    anyhow::ensure!(
        row.block_id == expected_block_id,
        "merge-plan block id {} != expected {}",
        row.block_id,
        expected_block_id
    );
    anyhow::ensure!(
        row.block_len > 0 && row.block_len as usize <= LIVE_FINALIZER_MAX_FRAME_SIZE,
        "merge-plan frame length {} is invalid",
        row.block_len
    );
    let source = sources
        .get_mut(row.source_id as usize)
        .context("merge-plan source id out of bounds")?;
    let end = row
        .source_offset
        .checked_add(varint_len(row.block_len) as u64)
        .and_then(|value| value.checked_add(u64::from(row.block_len)))
        .context("source frame end overflow")?;
    anyhow::ensure!(
        end <= source.selected_end_offset,
        "source frame ends outside selected source prefix"
    );
    if let Some(previous_id) = source.last_source_block_id {
        anyhow::ensure!(
            row.source_block_id > previous_id && row.source_offset >= source.last_source_end,
            "source rows overlap or regress"
        );
    }
    source.last_source_block_id = Some(row.source_block_id);
    source.last_source_end = end;
    Ok(())
}

fn read_source_block(
    source: &mut SourceReader,
    row: &MergePlanBlock,
    scratch: &mut Vec<u8>,
) -> Result<WincodeArchiveV2NoRegistryBlock> {
    source.file.seek(SeekFrom::Start(row.source_offset))?;
    let len = read_u32_varint(&mut source.file)?.context("source frame has no length prefix")?;
    anyhow::ensure!(
        len == row.block_len,
        "source frame declared length {} != plan {}",
        len,
        row.block_len
    );
    scratch.resize(len as usize, 0);
    source
        .file
        .read_exact(scratch)
        .context("read normalized source frame")?;
    anyhow::ensure!(
        source.file.stream_position()? == source.last_source_end,
        "source frame uses a noncanonical length prefix or differs from planned end"
    );
    let block = match wincode::config::deserialize::<WincodeArchiveV2NoRegistryBlock, _>(
        scratch,
        wincode_leb128_config(),
    ) {
        Ok(block) => block,
        Err(current_error) => {
            let legacy: LiveNoRegistryBlockLegacy =
                wincode::config::deserialize(scratch, wincode_leb128_config()).with_context(
                    || format!("decode source frame; current format failed: {current_error}"),
                )?;
            legacy.into_current()
        }
    };
    scratch.clear();
    anyhow::ensure!(
        block.header.compact.slot == row.slot,
        "source frame slot {} != plan {}",
        block.header.compact.slot,
        row.slot
    );
    anyhow::ensure!(
        block.header.compact.parent_slot == row.parent_slot,
        "source frame slot {} parent {} != plan {}",
        row.slot,
        block.header.compact.parent_slot,
        row.parent_slot
    );
    anyhow::ensure!(
        block.header.compact.blockhash == row.source_block_id,
        "source frame slot {} blockhash id {} != source id {}",
        row.slot,
        block.header.compact.blockhash,
        row.source_block_id
    );
    Ok(block)
}

fn remap_block_header(block: &mut WincodeArchiveV2NoRegistryBlock, block_id: u32) -> Result<()> {
    anyhow::ensure!(
        block.header.compact.poh_entries.is_empty() && block.header.compact.shredding.is_empty(),
        "normalized slot {} unexpectedly embeds canonical PoH/shredding",
        block.header.compact.slot
    );
    block.header.compact.blockhash = block_id;
    block.header.compact.previous_blockhash = block_id.saturating_sub(1);
    Ok(())
}

fn durable_checkpoint(
    stage: &Path,
    context: &ValidatedContext,
    checkpoint: &mut MaterializeCheckpoint,
    blocks_writer: &mut BufWriter<File>,
    runs: &mut PubkeyRuns,
) -> Result<()> {
    runs.flush()?;
    blocks_writer.flush()?;
    blocks_writer.get_ref().sync_all()?;
    checkpoint.block_stream_bytes = blocks_writer.get_mut().stream_position()?;
    checkpoint.run_count = runs.next_run;
    write_checkpoint_and_progress(stage, context, checkpoint, "materializing")
}

fn write_checkpoint_and_progress(
    stage: &Path,
    context: &ValidatedContext,
    checkpoint: &MaterializeCheckpoint,
    phase: &str,
) -> Result<()> {
    write_json_atomic(&stage.join(CHECKPOINT), checkpoint)?;
    write_progress(stage, context, checkpoint, phase)
}

fn write_progress(
    stage: &Path,
    context: &ValidatedContext,
    checkpoint: &MaterializeCheckpoint,
    phase: &str,
) -> Result<()> {
    let updated_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let elapsed_secs = updated_unix_secs.saturating_sub(checkpoint.started_unix_secs);
    let blocks_done = u64::from(checkpoint.next_block_id);
    let blocks_per_sec = if elapsed_secs > 0 {
        blocks_done as f64 / elapsed_secs as f64
    } else {
        0.0
    };
    let remaining = context.manifest.produced_blocks.saturating_sub(blocks_done);
    let eta_secs = (blocks_per_sec > 0.0).then(|| {
        (remaining as f64 / blocks_per_sec)
            .ceil()
            .min(u64::MAX as f64) as u64
    });
    let progress = MaterializeProgress {
        version: MATERIALIZER_VERSION,
        phase: phase.to_string(),
        epoch: context.manifest.epoch,
        blocks_done,
        blocks_total: context.manifest.produced_blocks,
        live_blocks_done: checkpoint.live_blocks,
        live_blocks_total: context.manifest.live_blocks,
        rpc_blocks_done: checkpoint.rpc_blocks,
        rpc_blocks_total: context.manifest.rpc_only_blocks,
        transactions_done: checkpoint.transactions,
        output_block_bytes: checkpoint.block_stream_bytes,
        pubkey_run_files: checkpoint.run_count,
        rss_bytes: current_rss_bytes(),
        started_unix_secs: checkpoint.started_unix_secs,
        elapsed_secs,
        blocks_per_sec,
        eta_secs,
        updated_unix_secs,
    };
    write_json_atomic(&stage.join(PROGRESS_JSON), &progress)
}

fn validate_checkpoint(
    checkpoint: &MaterializeCheckpoint,
    context: &ValidatedContext,
) -> Result<()> {
    anyhow::ensure!(
        checkpoint.version == MATERIALIZER_VERSION,
        "unsupported materialization checkpoint version"
    );
    anyhow::ensure!(
        checkpoint.manifest_sha256 == context.manifest_sha256
            && checkpoint.plan_sha256 == context.plan_sha256,
        "repair inputs differ from checkpoint"
    );
    anyhow::ensure!(
        checkpoint.source_stamps == context.source_stamps,
        "repair block sources differ from checkpoint"
    );
    anyhow::ensure!(
        u64::from(checkpoint.next_block_id) <= context.manifest.produced_blocks,
        "checkpoint block id exceeds manifest"
    );
    anyhow::ensure!(
        checkpoint.live_blocks.checked_add(checkpoint.rpc_blocks)
            == Some(u64::from(checkpoint.next_block_id)),
        "checkpoint source counts do not add up"
    );
    anyhow::ensure!(
        checkpoint.started_unix_secs > 0
            && checkpoint.started_unix_secs
                <= SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
        "checkpoint start time is invalid"
    );
    Ok(())
}

fn remove_uncommitted_runs(dir: &Path, committed: u32) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let remove = if name.starts_with(".run-") && name.ends_with(".tmp") {
            true
        } else {
            name.strip_prefix("run-")
                .and_then(|value| value.strip_suffix(".bin"))
                .and_then(|value| value.parse::<u32>().ok())
                .is_some_and(|run| run >= committed)
        };
        if remove {
            fs::remove_file(entry.path())
                .with_context(|| format!("remove uncommitted run {}", entry.path().display()))?;
        }
    }
    sync_dir(dir)
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcGetBlock {
    blockhash: String,
    previous_blockhash: String,
    parent_slot: u64,
    block_time: Option<i64>,
    block_height: Option<u64>,
    num_reward_partitions: Option<u64>,
    rewards: Option<Vec<RpcReward>>,
    transactions: Vec<RpcTransaction>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RpcTransaction {
    transaction: [String; 2],
    meta: Option<RpcMeta>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcMeta {
    err: Value,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<RpcInnerInstructions>>,
    log_messages: Option<Vec<String>>,
    pre_token_balances: Vec<RpcTokenBalance>,
    post_token_balances: Vec<RpcTokenBalance>,
    rewards: Option<Vec<RpcReward>>,
    loaded_addresses: Option<RpcLoadedAddresses>,
    return_data: Option<RpcReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RpcInnerInstructions {
    index: u32,
    instructions: Vec<RpcInnerInstruction>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcInnerInstruction {
    program_id_index: u32,
    accounts: Vec<u8>,
    data: String,
    stack_height: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcTokenBalance {
    account_index: u32,
    mint: Option<String>,
    owner: Option<String>,
    program_id: Option<String>,
    ui_token_amount: RpcUiTokenAmount,
}

#[derive(Debug, Deserialize, Serialize)]
struct RpcUiTokenAmount {
    amount: String,
    decimals: u8,
}

#[derive(Debug, Deserialize, Serialize)]
struct RpcLoadedAddresses {
    writable: Vec<String>,
    readonly: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcReturnData {
    program_id: String,
    data: [String; 2],
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcReward {
    pubkey: String,
    lamports: i64,
    post_balance: u64,
    reward_type: Option<String>,
    commission: Option<RpcCommission>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum RpcCommission {
    Number(u8),
    Text(String),
}

fn materialize_rpc_block(
    repair_root: &Path,
    manifest: &RpcOnlyManifest,
    block_id: u32,
    max_bytes: u64,
) -> Result<WincodeArchiveV2NoRegistryBlock> {
    let path = resolve_relative(repair_root, &manifest.source_path, "RPC getBlock")?;
    ensure_regular_file(&path, max_bytes)?;
    validate_rpc_source_stamp(&path, manifest, false)?;
    let bytes = fs::read(&path).with_context(|| format!("read RPC getBlock {}", path.display()))?;
    anyhow::ensure!(
        bytes.len() as u64 <= max_bytes,
        "RPC getBlock {} exceeds byte limit",
        path.display()
    );
    anyhow::ensure!(
        hex_digest(Sha256::digest(&bytes)) == manifest.source_sha256,
        "RPC getBlock digest differs from manifest: {}",
        path.display()
    );
    let rpc: RpcGetBlock =
        serde_json::from_slice(&bytes).with_context(|| format!("decode {}", path.display()))?;
    validate_rpc_source_stamp(&path, manifest, false)?;
    anyhow::ensure!(
        rpc.parent_slot == manifest.parent_slot
            && rpc.blockhash == manifest.blockhash
            && rpc.previous_blockhash == manifest.previous_blockhash,
        "RPC getBlock chain fields differ from manifest for slot {}",
        manifest.slot
    );
    anyhow::ensure!(
        rpc.transactions.len() as u64 == manifest.transactions,
        "RPC slot {} transaction count {} != manifest {}",
        manifest.slot,
        rpc.transactions.len(),
        manifest.transactions
    );
    let rewards = rpc.rewards.context("RPC block has no rewards field")?;
    let reward_source_len = serde_json::to_vec(&rewards)?.len() as u64;
    let rewards = rewards
        .iter()
        .map(convert_rpc_reward)
        .collect::<Result<Vec<_>>>()?;
    let mut txs = Vec::with_capacity(rpc.transactions.len());
    for (tx_index, transaction) in rpc.transactions.into_iter().enumerate() {
        anyhow::ensure!(
            transaction.transaction[1] == "base64",
            "RPC slot {} tx#{} encoding is not base64",
            manifest.slot,
            tx_index
        );
        let wire = BASE64_STANDARD
            .decode(&transaction.transaction[0])
            .with_context(|| format!("RPC slot {} tx#{} base64", manifest.slot, tx_index))?;
        anyhow::ensure!(
            BASE64_STANDARD.encode(&wire) == transaction.transaction[0],
            "RPC slot {} tx#{} base64 is not canonical",
            manifest.slot,
            tx_index
        );
        let decoded = wincode::deserialize::<VersionedTransaction<'_>>(&wire)
            .map_err(|error| anyhow!("{error}"))
            .with_context(|| format!("RPC slot {} tx#{} wire", manifest.slot, tx_index))?;
        let tx = to_no_registry_transaction(decoded);
        let metadata = transaction
            .meta
            .map(|meta| {
                let source_len = serde_json::to_vec(&meta)?.len() as u64;
                Ok::<_, anyhow::Error>(WincodeArchiveV2Payload::Decoded {
                    source_len,
                    value: convert_rpc_meta(meta).with_context(|| {
                        format!("RPC slot {} tx#{} meta", manifest.slot, tx_index)
                    })?,
                })
            })
            .transpose()?;
        txs.push(WincodeArchiveV2NoRegistryTransaction {
            tx_index: u32::try_from(tx_index).context("RPC tx index exceeds u32")?,
            tx: WincodeArchiveV2Payload::Decoded {
                source_len: wire.len() as u64,
                value: tx,
            },
            metadata,
        });
    }
    Ok(WincodeArchiveV2NoRegistryBlock {
        header: WincodeArchiveV2NoRegistryBlockHeader {
            compact: CompactBlockHeader {
                slot: manifest.slot,
                parent_slot: manifest.parent_slot,
                blockhash: block_id,
                previous_blockhash: block_id.saturating_sub(1),
                block_time: rpc.block_time,
                block_height: rpc.block_height,
                shredding: Vec::new(),
                poh_entries: Vec::new(),
                rewards: None,
            },
            rewards: Some(WincodeArchiveV2NoRegistryRewards {
                source_len: reward_source_len,
                num_partitions: rpc.num_reward_partitions,
                decoded: Some(rewards),
                raw_fallback: None,
                decode_error: None,
            }),
        },
        txs,
    })
}

fn convert_rpc_meta(meta: RpcMeta) -> Result<WincodeArchiveV2NoRegistryMeta> {
    let err = if meta.err.is_null() {
        None
    } else {
        Some(decode_rpc_transaction_error(meta.err)?)
    };
    let inner_instructions = meta
        .inner_instructions
        .map(|groups| {
            groups
                .into_iter()
                .map(|group| {
                    Ok(CompactInnerInstructions {
                        index: group.index,
                        instructions: group
                            .instructions
                            .into_iter()
                            .map(|instruction| {
                                Ok(CompactInnerInstruction {
                                    program_id_index: instruction.program_id_index,
                                    accounts: instruction.accounts,
                                    data: bs58::decode(&instruction.data)
                                        .into_vec()
                                        .context("decode inner instruction base58")?,
                                    stack_height: instruction.stack_height,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let logs = meta.log_messages.map(WincodeArchiveV2NoRegistryLogs::Raw);
    let pre_token_balances = meta
        .pre_token_balances
        .into_iter()
        .map(convert_rpc_token_balance)
        .collect::<Result<Vec<_>>>()?;
    let post_token_balances = meta
        .post_token_balances
        .into_iter()
        .map(convert_rpc_token_balance)
        .collect::<Result<Vec<_>>>()?;
    let rewards = meta
        .rewards
        .unwrap_or_default()
        .iter()
        .map(convert_rpc_reward)
        .collect::<Result<Vec<_>>>()?;
    let loaded = meta.loaded_addresses.unwrap_or(RpcLoadedAddresses {
        writable: Vec::new(),
        readonly: Vec::new(),
    });
    let loaded_writable_addresses = loaded
        .writable
        .iter()
        .map(|value| decode_base58_32(value, "loaded writable address"))
        .collect::<Result<Vec<_>>>()?;
    let loaded_readonly_addresses = loaded
        .readonly
        .iter()
        .map(|value| decode_base58_32(value, "loaded readonly address"))
        .collect::<Result<Vec<_>>>()?;
    let return_data = meta
        .return_data
        .map(|value| {
            anyhow::ensure!(
                value.data[1] == "base64",
                "returnData encoding is not base64"
            );
            let data = BASE64_STANDARD
                .decode(&value.data[0])
                .context("decode returnData base64")?;
            anyhow::ensure!(
                BASE64_STANDARD.encode(&data) == value.data[0],
                "returnData base64 is not canonical"
            );
            Ok::<_, anyhow::Error>(WincodeArchiveV2NoRegistryReturnData {
                program_id: decode_base58_32(&value.program_id, "returnData program id")?,
                data,
            })
        })
        .transpose()?;
    Ok(WincodeArchiveV2NoRegistryMeta {
        err,
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions,
        logs,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_writable_addresses,
        loaded_readonly_addresses,
        return_data,
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
}

fn decode_rpc_transaction_error(value: Value) -> Result<CompactTransactionError> {
    // Old Solana metadata encoded BorshIoError as a unit variant because its message was
    // unavailable. The RPC renderer preserves that shape as the string "BorshIoError", while
    // the current compact enum carries the optional historical message as a newtype. Keep this
    // compatibility path deliberately exact; every other shape still goes through serde's
    // strict enum decoder.
    if let Some(error) = decode_legacy_unit_borsh_io_error(&value) {
        return Ok(error);
    }

    serde_json::from_value(value).context("decode transaction error")
}

fn decode_legacy_unit_borsh_io_error(value: &Value) -> Option<CompactTransactionError> {
    let object = value.as_object()?;
    if object.len() != 1 {
        return None;
    }
    let fields = object.get("InstructionError")?.as_array()?;
    if fields.len() != 2 || fields[1].as_str() != Some("BorshIoError") {
        return None;
    }
    let instruction_index = u8::try_from(fields[0].as_u64()?).ok()?;
    Some(CompactTransactionError::InstructionError(
        instruction_index,
        CompactInstructionError::BorshIoError(String::new()),
    ))
}

fn convert_rpc_token_balance(
    balance: RpcTokenBalance,
) -> Result<WincodeArchiveV2NoRegistryTokenBalance> {
    Ok(WincodeArchiveV2NoRegistryTokenBalance {
        account_index: balance.account_index,
        mint: decode_optional_pubkey(balance.mint.as_deref(), "token mint")?,
        owner: decode_optional_pubkey(balance.owner.as_deref(), "token owner")?,
        program_id: decode_optional_pubkey(balance.program_id.as_deref(), "token program id")?,
        amount: balance
            .ui_token_amount
            .amount
            .parse::<u64>()
            .context("parse token amount")?,
        decimals: balance.ui_token_amount.decimals,
    })
}

fn convert_rpc_reward(reward: &RpcReward) -> Result<WincodeArchiveV2NoRegistryReward> {
    let reward_type = match reward.reward_type.as_deref() {
        None => 0,
        Some("Fee") => 1,
        Some("Rent") => 2,
        Some("Staking") => 3,
        Some("Voting") => 4,
        Some(other) => bail!("unsupported RPC reward type {other}"),
    };
    let commission = reward
        .commission
        .as_ref()
        .map(|commission| match commission {
            RpcCommission::Number(value) => Ok(*value),
            RpcCommission::Text(value) => value
                .parse::<u8>()
                .with_context(|| format!("parse reward commission {value}")),
        })
        .transpose()?;
    Ok(WincodeArchiveV2NoRegistryReward {
        pubkey: decode_base58_32(&reward.pubkey, "reward pubkey")?,
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type,
        commission,
    })
}

fn decode_optional_pubkey(value: Option<&str>, field: &str) -> Result<Option<[u8; 32]>> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| decode_base58_32(value, field))
        .transpose()
}

fn visit_no_registry_pubkeys(
    block: &WincodeArchiveV2NoRegistryBlock,
    mut visit: impl FnMut([u8; 32]) -> Result<()>,
) -> Result<()> {
    if let Some(rewards) = block.header.rewards.as_ref() {
        anyhow::ensure!(
            rewards.raw_fallback.is_none() && rewards.decode_error.is_none(),
            "slot {} has raw rewards fallback",
            block.header.compact.slot
        );
        let decoded = rewards
            .decoded
            .as_ref()
            .context("rewards present without decoded rewards")?;
        for reward in decoded {
            visit(reward.pubkey)?;
        }
    }
    for (tx_index, tx) in block.txs.iter().enumerate() {
        let WincodeArchiveV2Payload::Decoded { value, .. } = &tx.tx else {
            bail!(
                "slot {} tx#{} has raw transaction payload",
                block.header.compact.slot,
                tx_index
            );
        };
        match &value.message {
            blockzilla_format::WincodeArchiveV2NoRegistryMessage::Legacy(message) => {
                for pubkey in &message.account_keys {
                    visit(*pubkey)?;
                }
            }
            blockzilla_format::WincodeArchiveV2NoRegistryMessage::V0(message) => {
                for pubkey in &message.account_keys {
                    visit(*pubkey)?;
                }
                for lookup in &message.address_table_lookups {
                    visit(lookup.account_key)?;
                }
            }
        }
        if let Some(metadata) = tx.metadata.as_ref() {
            let WincodeArchiveV2Payload::Decoded { value, .. } = metadata else {
                bail!(
                    "slot {} tx#{} has raw metadata payload",
                    block.header.compact.slot,
                    tx_index
                );
            };
            for pubkey in &value.loaded_writable_addresses {
                visit(*pubkey)?;
            }
            for pubkey in &value.loaded_readonly_addresses {
                visit(*pubkey)?;
            }
            for balance in value
                .pre_token_balances
                .iter()
                .chain(value.post_token_balances.iter())
            {
                for pubkey in [balance.mint, balance.owner, balance.program_id]
                    .into_iter()
                    .flatten()
                {
                    visit(pubkey)?;
                }
            }
            for reward in &value.rewards {
                visit(reward.pubkey)?;
            }
            if let Some(return_data) = value.return_data.as_ref() {
                visit(return_data.program_id)?;
            }
        }
    }
    Ok(())
}

fn validate_rpc_source_stamp(
    path: &Path,
    manifest: &RpcOnlyManifest,
    with_hash: bool,
) -> Result<()> {
    ensure_regular_file(path, manifest.source_bytes)?;
    let metadata = fs::metadata(path)?;
    anyhow::ensure!(
        metadata.len() == manifest.source_bytes,
        "RPC source length changed: {}",
        path.display()
    );
    anyhow::ensure!(
        modified_nanos(&metadata)? == manifest.source_modified_nanos,
        "RPC source mtime changed: {}",
        path.display()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Some(device) = manifest.source_device {
            anyhow::ensure!(
                metadata.dev() == device,
                "RPC source device changed: {}",
                path.display()
            );
        }
        if let Some(inode) = manifest.source_inode {
            anyhow::ensure!(
                metadata.ino() == inode,
                "RPC source inode changed: {}",
                path.display()
            );
        }
    }
    if with_hash {
        anyhow::ensure!(
            sha256_file_hex(path)? == manifest.source_sha256,
            "RPC source digest changed: {}",
            path.display()
        );
    }
    Ok(())
}

fn revalidate_source_stamps(context: &ValidatedContext) -> Result<()> {
    let current = context
        .source_stamps
        .iter()
        .map(|stamp| {
            file_stamp(
                &context.repair_root,
                &context.repair_root.join(&stamp.relative_path),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    anyhow::ensure!(
        current == context.source_stamps,
        "repair input identity changed during materialization"
    );
    anyhow::ensure!(
        sha256_file_hex(&context.repair_root.join(REPAIR_MARKER))? == context.manifest_sha256,
        "repair manifest digest changed during materialization"
    );
    anyhow::ensure!(
        sha256_file_hex(&context.plan_path)? == context.plan_sha256,
        "repair merge-plan digest changed during materialization"
    );
    Ok(())
}

fn validate_existing_output(output: &Path, context: &ValidatedContext) -> Result<()> {
    anyhow::ensure!(output.is_dir(), "existing repair output is not a directory");
    let marker_path = output.join(MATERIALIZED_MARKER);
    let marker: MaterializedMarker = read_json_bounded(&marker_path, MARKER_MAX_BYTES)?;
    anyhow::ensure!(
        marker.version == MATERIALIZER_VERSION
            && marker.state == "repair_materialized_missing_poh_and_shredding"
            && !marker.canonical
            && !marker.publication_ready,
        "existing materialized marker has incompatible state"
    );
    anyhow::ensure!(
        marker.epoch == context.manifest.epoch
            && marker.produced_blocks == context.manifest.produced_blocks
            && marker.live_blocks == context.manifest.live_blocks
            && marker.rpc_only_blocks == context.manifest.rpc_only_blocks,
        "existing materialized output counts differ from repair input"
    );
    anyhow::ensure!(
        marker.manifest_sha256 == context.manifest_sha256
            && marker.merge_plan_sha256 == context.plan_sha256,
        "existing materialized output was built from different inputs"
    );
    for path in [
        output.join(OUTPUT_BLOCKS),
        output.join(OUTPUT_BLOCKHASHES),
        output.join(OUTPUT_AVAILABLE_POH),
    ] {
        ensure_regular_file(&path, u64::MAX)?;
    }
    anyhow::ensure!(
        fs::metadata(output.join(OUTPUT_BLOCKHASHES))?.len()
            == context.manifest.produced_blocks * 32,
        "existing output blockhash registry length mismatch"
    );
    let run_dir = output.join(OUTPUT_PUBKEY_RUNS);
    anyhow::ensure!(
        fs::read_dir(&run_dir)?.any(|entry| {
            entry
                .ok()
                .and_then(|entry| entry.file_name().to_str().map(str::to_owned))
                .is_some_and(|name| name.starts_with("run-") && name.ends_with(".bin"))
        }),
        "existing output has no exact pubkey runs"
    );
    for forbidden in [
        "READY",
        "poh.wincode",
        "shredding.wincode",
        "poh/poh.wincode",
        "archive-v2-meta.wincode",
    ] {
        anyhow::ensure!(
            !output.join(forbidden).exists(),
            "noncanonical repair output contains forbidden canonical artifact {forbidden}"
        );
    }
    Ok(())
}

pub(crate) fn validate_materialized_for_hot(materialized_dir: &Path) -> Result<MaterializedForHot> {
    let root = fs::canonicalize(materialized_dir)
        .with_context(|| format!("canonicalize {}", materialized_dir.display()))?;
    anyhow::ensure!(
        root.is_dir(),
        "materialized repair input is not a directory"
    );
    let marker_path = root.join(MATERIALIZED_MARKER);
    ensure_regular_file(&marker_path, MARKER_MAX_BYTES)?;
    let marker_bytes = fs::read(&marker_path)?;
    let marker_sha256 = hex_digest(Sha256::digest(&marker_bytes));
    let marker: MaterializedMarker =
        serde_json::from_slice(&marker_bytes).context("decode REPAIR-MATERIALIZED.json")?;
    anyhow::ensure!(
        marker.version == MATERIALIZER_VERSION
            && marker.state == "repair_materialized_missing_poh_and_shredding"
            && !marker.canonical
            && !marker.publication_ready,
        "materialized repair marker has incompatible state"
    );
    anyhow::ensure!(
        marker.live_blocks.checked_add(marker.rpc_only_blocks) == Some(marker.produced_blocks)
            && marker.produced_blocks > 0,
        "materialized repair count accounting mismatch"
    );
    let expected_start = marker
        .epoch
        .checked_mul(crate::SLOTS_PER_EPOCH)
        .context("materialized epoch start overflow")?;
    let expected_end = expected_start
        .checked_add(crate::SLOTS_PER_EPOCH - 1)
        .context("materialized epoch end overflow")?;
    anyhow::ensure!(
        marker.epoch_start_slot == expected_start
            && marker.epoch_end_slot == expected_end
            && marker.produced_blocks <= crate::SLOTS_PER_EPOCH,
        "materialized marker epoch bounds/count are invalid"
    );
    anyhow::ensure!(
        marker
            .first_produced_slot
            .is_some_and(|slot| { (expected_start..=expected_end).contains(&slot) })
            && marker
                .last_produced_slot
                .is_some_and(|slot| { (expected_start..=expected_end).contains(&slot) })
            && marker.first_produced_slot <= marker.last_produced_slot,
        "materialized produced slot bounds are invalid"
    );
    anyhow::ensure!(
        marker.normalized_blocks == OUTPUT_BLOCKS
            && marker.produced_blockhashes == OUTPUT_BLOCKHASHES
            && marker.available_poh == OUTPUT_AVAILABLE_POH
            && marker.exact_pubkey_runs == OUTPUT_PUBKEY_RUNS
            && marker.progress_json == PROGRESS_JSON,
        "materialized repair marker uses unexpected file paths"
    );
    decode_hex_32(&marker.manifest_sha256)?;
    decode_hex_32(&marker.merge_plan_sha256)?;
    for forbidden in [
        "READY",
        "poh.wincode",
        "shredding.wincode",
        "poh/poh.wincode",
        ARCHIVE_V2_META_FILE,
    ] {
        anyhow::ensure!(
            !root.join(forbidden).exists(),
            "materialized input contains forbidden canonical artifact {forbidden}"
        );
    }
    for path in [
        root.join(OUTPUT_BLOCKS),
        root.join(OUTPUT_BLOCKHASHES),
        root.join(OUTPUT_AVAILABLE_POH),
    ] {
        ensure_regular_file(&path, u64::MAX)?;
    }
    let materialized_blockhash_bytes = marker
        .produced_blocks
        .checked_mul(32)
        .context("materialized blockhash byte length overflow")?;
    anyhow::ensure!(
        fs::metadata(root.join(OUTPUT_BLOCKHASHES))?.len() == materialized_blockhash_bytes,
        "materialized blockhash registry length mismatch"
    );
    validate_pubkey_run_directory(&root.join(OUTPUT_PUBKEY_RUNS))?;

    let poh_file = File::open(root.join(OUTPUT_AVAILABLE_POH))?;
    let mut poh_reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(IO_BUFFER_BYTES, poh_file));
    let mut available_records = 0u64;
    let mut available_entries = 0u64;
    let mut expected_id = 0u32;
    let missing_capacity =
        usize::try_from(marker.rpc_only_blocks).context("RPC-only count exceeds usize")?;
    anyhow::ensure!(
        missing_capacity <= crate::SLOTS_PER_EPOCH as usize,
        "RPC-only count exceeds one epoch"
    );
    let mut missing_ids = Vec::with_capacity(missing_capacity);
    let mut previous_slot = None;
    while let Some((_len, record)) =
        poh_reader.read_bytes_with_limit(POH_FRAME_MAX_BYTES, |bytes| {
            wincode::config::deserialize::<WincodeArchiveV2PohRecord, _>(
                bytes,
                wincode_leb128_config(),
            )
            .map_err(|error| anyhow!("{error}"))
        })?
    {
        anyhow::ensure!(
            u64::from(record.block_id) < marker.produced_blocks,
            "available PoH id {} outside produced space",
            record.block_id
        );
        anyhow::ensure!(
            record.block_id >= expected_id,
            "available PoH ids are not sorted/unique"
        );
        while expected_id < record.block_id {
            missing_ids.push(expected_id);
            expected_id = expected_id.checked_add(1).context("PoH id overflow")?;
        }
        expected_id = expected_id.checked_add(1).context("PoH id overflow")?;
        anyhow::ensure!(
            (marker.epoch_start_slot..=marker.epoch_end_slot).contains(&record.slot),
            "available PoH slot outside epoch"
        );
        anyhow::ensure!(
            previous_slot.is_none_or(|slot| record.slot > slot),
            "available PoH slots are not strictly increasing"
        );
        previous_slot = Some(record.slot);
        available_records = available_records.saturating_add(1);
        available_entries = available_entries
            .checked_add(record.entries.len() as u64)
            .context("available PoH entry count overflow")?;
    }
    while u64::from(expected_id) < marker.produced_blocks {
        missing_ids.push(expected_id);
        expected_id = expected_id.checked_add(1).context("PoH id overflow")?;
    }
    anyhow::ensure!(
        available_records == marker.live_blocks
            && missing_ids.len() as u64 == marker.rpc_only_blocks
            && marker.missing_poh_and_shredding_blocks == marker.rpc_only_blocks,
        "materialized partial PoH coverage differs from marker"
    );

    let input_fingerprint = materialized_input_fingerprint(&root)?;
    Ok(MaterializedForHot {
        epoch: marker.epoch,
        epoch_start_slot: marker.epoch_start_slot,
        epoch_end_slot: marker.epoch_end_slot,
        live_blocks: marker.live_blocks,
        rpc_only_blocks: marker.rpc_only_blocks,
        produced_blocks: marker.produced_blocks,
        available_poh_records: available_records,
        available_poh_entries: available_entries,
        missing_poh_ids: missing_ids,
        source_marker_sha256: marker_sha256,
        source_manifest_sha256: marker.manifest_sha256,
        source_plan_sha256: marker.merge_plan_sha256,
        root,
        input_fingerprint,
    })
}

pub(crate) fn begin_degraded_hot_stage(
    materialized_dir: &Path,
    output_dir: &Path,
    receipt: &MaterializedForHot,
) -> Result<Option<DegradedHotStage>> {
    let input = fs::canonicalize(materialized_dir)?;
    anyhow::ensure!(input == receipt.root, "materialized input path changed");
    let output_parent = output_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(output_parent)?;
    let output_parent = fs::canonicalize(output_parent)?;
    let output_name = output_dir
        .file_name()
        .context("degraded hot output must have a final component")?;
    let output = output_parent.join(output_name);
    anyhow::ensure!(
        !output.starts_with(&input) && !input.starts_with(&output),
        "degraded hot output and materialized input must not contain each other"
    );
    let lock = acquire_lock(&output_parent, output_name)?;
    if output.exists() {
        validate_degraded_hot_output(&output, receipt)?;
        return Ok(None);
    }
    let stage = output_parent.join(format!(
        ".{}.repair-hot-stage",
        output_name.to_string_lossy()
    ));
    if stage.join(COMPACTED_MARKER).is_file() {
        validate_degraded_hot_output(&stage, receipt)?;
        fs::rename(&stage, &output)?;
        sync_dir(&output_parent)?;
        return Ok(None);
    }
    if stage.exists() {
        fs::remove_dir_all(&stage)
            .with_context(|| format!("remove stale degraded hot stage {}", stage.display()))?;
    }
    fs::create_dir_all(stage.join("repair"))?;
    hard_link_or_copy_atomic(
        &input.join(OUTPUT_AVAILABLE_POH),
        &stage.join(OUTPUT_AVAILABLE_POH),
    )?;
    hard_link_or_copy_atomic(
        &input.join(MATERIALIZED_MARKER),
        &stage.join(SOURCE_MATERIALIZED_MARKER),
    )?;
    write_json_atomic(
        &stage.join(HOT_PROGRESS_JSON),
        &serde_json::json!({
            "version": 1,
            "phase": "building_hot_archive",
            "epoch": receipt.epoch,
            "blocks_done": 0,
            "blocks_total": receipt.produced_blocks,
            "updated_unix_secs": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        }),
    )?;
    sync_dir(&stage)?;
    Ok(Some(DegradedHotStage {
        path: stage,
        output,
        output_parent,
        _lock: lock,
    }))
}

pub(crate) fn publish_degraded_hot_stage(
    materialized_dir: &Path,
    output_dir: &Path,
    stage: DegradedHotStage,
    receipt: &MaterializedForHot,
    stats: DegradedHotStats,
) -> Result<()> {
    anyhow::ensure!(
        fs::canonicalize(materialized_dir)? == receipt.root,
        "materialized source path changed before hot publication"
    );
    anyhow::ensure!(
        stage.output
            == stage.output_parent.join(
                output_dir
                    .file_name()
                    .context("degraded output has no final component")?
            ),
        "degraded output path changed during build"
    );
    anyhow::ensure!(
        materialized_input_fingerprint(&receipt.root)? == receipt.input_fingerprint,
        "materialized source changed during degraded hot build"
    );
    ensure_block_access_artifacts_absent(&stage.path)?;
    validate_degraded_hot_candidates(&stage.path, receipt, &stats)?;
    let marker = DegradedHotMarker {
        version: MATERIALIZER_VERSION,
        state: "degraded_hot_archive_missing_poh_and_shredding".to_string(),
        canonical: false,
        publication_ready: false,
        block_archive_ready: true,
        block_access_ready: false,
        epoch: receipt.epoch,
        epoch_start_slot: receipt.epoch_start_slot,
        epoch_end_slot: receipt.epoch_end_slot,
        live_blocks: receipt.live_blocks,
        rpc_only_blocks: receipt.rpc_only_blocks,
        produced_blocks: receipt.produced_blocks,
        transactions: stats.transactions,
        signatures: stats.signatures,
        zstd_level: stats.zstd_level,
        compressed_bytes: stats.compressed_bytes,
        uncompressed_bytes: stats.uncompressed_bytes,
        files: degraded_hot_files(false),
        poh_coverage: DegradedPohCoverage {
            available_records: receipt.available_poh_records,
            available_entries: receipt.available_poh_entries,
            missing_records: receipt.missing_poh_ids.len() as u64,
            produced_id_space: receipt.produced_blocks,
            record_ids_have_explicit_gaps: true,
            missing_record_ids: receipt.missing_poh_ids.clone(),
        },
        shredding_coverage: DegradedShreddingCoverage {
            available_records: 0,
            missing_records: receipt.produced_blocks,
            canonical_sidecar_emitted: false,
        },
        source_materialized_marker_sha256: receipt.source_marker_sha256.clone(),
        source_manifest_sha256: receipt.source_manifest_sha256.clone(),
        source_merge_plan_sha256: receipt.source_plan_sha256.clone(),
        limitations: vec![
            "The block archive is readable and contains all produced slots, including getBlock materialization for RPC-only slots.".to_string(),
            "Canonical PoH and shredding sidecars are intentionally absent; available live PoH remains repair-specific with explicit gaps.".to_string(),
            "Block-access sidecars are deferred and this marker must never be interpreted as canonical finalization.".to_string(),
        ],
    };
    write_json_atomic(
        &stage.path.join(HOT_PROGRESS_JSON),
        &serde_json::json!({
            "version": 1,
            "phase": "complete_noncanonical",
            "epoch": receipt.epoch,
            "blocks_done": stats.blocks,
            "blocks_total": receipt.produced_blocks,
            "transactions_done": stats.transactions,
            "compressed_bytes": stats.compressed_bytes,
            "updated_unix_secs": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        }),
    )?;
    sync_degraded_hot_candidates(&stage.path)?;
    write_json_atomic(&stage.path.join(COMPACTED_MARKER), &marker)?;
    sync_dir(&stage.path)?;
    validate_degraded_hot_output(&stage.path, receipt)?;
    fs::rename(&stage.path, &stage.output)
        .with_context(|| format!("publish degraded hot archive {}", stage.output.display()))?;
    sync_dir(&stage.output_parent)?;
    Ok(())
}

fn degraded_hot_files(block_access_ready: bool) -> DegradedHotFiles {
    DegradedHotFiles {
        blocks: ARCHIVE_V2_BLOCKS_FILE.to_string(),
        index: ARCHIVE_V2_BLOCK_INDEX_FILE.to_string(),
        meta: ARCHIVE_V2_META_FILE.to_string(),
        registry: ARCHIVE_V2_PUBKEY_REGISTRY_FILE.to_string(),
        registry_counts: ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE.to_string(),
        registry_index: ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE.to_string(),
        blockhashes: ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE.to_string(),
        signatures: ARCHIVE_V2_SIGNATURES_FILE.to_string(),
        vote_hashes: ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE.to_string(),
        available_poh: OUTPUT_AVAILABLE_POH.to_string(),
        block_access: block_access_ready.then(|| ARCHIVE_V2_BLOCK_ACCESS_FILE.to_string()),
        block_access_index: block_access_ready
            .then(|| ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE.to_string()),
        get_block_index: block_access_ready.then(|| ARCHIVE_V2_GET_BLOCK_INDEX_FILE.to_string()),
        previous_blockhash_tail: block_access_ready
            .then(|| ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE.to_string()),
    }
}

fn validate_degraded_hot_candidates(
    root: &Path,
    receipt: &MaterializedForHot,
    stats: &DegradedHotStats,
) -> Result<()> {
    anyhow::ensure!(
        stats.blocks == receipt.produced_blocks,
        "hot candidate block count mismatch"
    );
    for forbidden in [
        "READY",
        "poh.wincode",
        "shredding.wincode",
        "poh/poh.wincode",
    ] {
        anyhow::ensure!(
            !root.join(forbidden).exists(),
            "degraded hot candidate contains forbidden artifact {forbidden}"
        );
    }
    for relative in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
        OUTPUT_AVAILABLE_POH,
        SOURCE_MATERIALIZED_MARKER,
    ] {
        ensure_regular_file(&root.join(relative), u64::MAX)?;
    }
    let signature_path = root.join(ARCHIVE_V2_SIGNATURES_FILE);
    let signature_metadata = fs::symlink_metadata(&signature_path)
        .with_context(|| format!("stat {}", signature_path.display()))?;
    anyhow::ensure!(
        signature_metadata.file_type().is_file(),
        "signature sidecar is not a regular file"
    );
    let expected_signature_bytes = stats
        .signatures
        .checked_mul(64)
        .context("signature sidecar byte length overflow")?;
    anyhow::ensure!(
        signature_metadata.len() == expected_signature_bytes,
        "signature sidecar byte length mismatch"
    );
    let expected_blockhash_bytes = receipt
        .produced_blocks
        .checked_mul(32)
        .context("hot blockhash byte length overflow")?;
    anyhow::ensure!(
        fs::metadata(root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))?.len()
            == expected_blockhash_bytes,
        "hot blockhash registry byte length mismatch"
    );
    anyhow::ensure!(
        sha256_file_hex(&root.join(SOURCE_MATERIALIZED_MARKER))? == receipt.source_marker_sha256,
        "retained source REPAIR-MATERIALIZED marker digest mismatch"
    );
    anyhow::ensure!(
        fs::metadata(root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?.len() % 32 == 0,
        "hot pubkey registry byte length is invalid"
    );

    let index = read_archive_v2_hot_block_index(&root.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    anyhow::ensure!(
        index.rows.len() as u64 == receipt.produced_blocks,
        "hot index row count mismatch"
    );
    anyhow::ensure!(
        index.level == stats.zstd_level,
        "hot index zstd level mismatch"
    );
    anyhow::ensure!(
        index.blob_file_bytes == stats.compressed_bytes
            && fs::metadata(root.join(ARCHIVE_V2_BLOCKS_FILE))?.len() == stats.compressed_bytes,
        "hot block blob byte length mismatch"
    );
    let mut next_offset = 0u64;
    let mut transactions = 0u64;
    let mut signatures = 0u64;
    let mut uncompressed = 0u64;
    let mut previous_slot = None;
    for (expected_id, row) in index.rows.iter().enumerate() {
        anyhow::ensure!(
            row.block_id as usize == expected_id,
            "hot index block ids are not contiguous"
        );
        anyhow::ensure!(
            row.compressed_offset == next_offset,
            "hot index compressed offsets are not contiguous"
        );
        anyhow::ensure!(
            row.first_tx_ordinal == transactions && row.first_signature_ordinal == signatures,
            "hot index ordinals are not contiguous"
        );
        anyhow::ensure!(
            (receipt.epoch_start_slot..=receipt.epoch_end_slot).contains(&row.slot),
            "hot index slot outside epoch"
        );
        anyhow::ensure!(
            previous_slot.is_none_or(|slot| row.slot > slot),
            "hot index slots are not strictly increasing"
        );
        previous_slot = Some(row.slot);
        next_offset = next_offset
            .checked_add(u64::from(row.compressed_len))
            .context("hot compressed offset overflow")?;
        transactions = transactions
            .checked_add(u64::from(row.tx_count))
            .context("hot transaction count overflow")?;
        signatures = signatures
            .checked_add(u64::from(row.signature_count))
            .context("hot signature count overflow")?;
        uncompressed = uncompressed
            .checked_add(u64::from(row.uncompressed_len))
            .context("hot uncompressed count overflow")?;
    }
    anyhow::ensure!(
        next_offset == index.blob_file_bytes
            && transactions == stats.transactions
            && signatures == stats.signatures
            && uncompressed == stats.uncompressed_bytes,
        "hot index totals differ from build report"
    );

    let meta_file = File::open(root.join(ARCHIVE_V2_META_FILE))?;
    let mut meta_reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(IO_BUFFER_BYTES, meta_file));
    let header = meta_reader
        .read_bytes_with_limit(MARKER_MAX_BYTES as usize, |bytes| {
            wincode::config::deserialize::<ArchiveV2HotMetaRecord, _>(
                bytes,
                wincode_leb128_config(),
            )
            .map_err(|error| anyhow!("{error}"))
        })?
        .map(|(_, record)| record)
        .context("hot metadata is empty")?;
    anyhow::ensure!(
        matches!(header, ArchiveV2HotMetaRecord::Header(_)),
        "hot metadata does not begin with a header"
    );
    let footer = meta_reader
        .read_bytes_with_limit(MARKER_MAX_BYTES as usize, |bytes| {
            wincode::config::deserialize::<ArchiveV2HotMetaRecord, _>(
                bytes,
                wincode_leb128_config(),
            )
            .map_err(|error| anyhow!("{error}"))
        })?
        .map(|(_, record)| record)
        .context("hot metadata has no footer")?;
    let ArchiveV2HotMetaRecord::Footer(footer) = footer else {
        bail!("hot metadata second record is not a footer")
    };
    anyhow::ensure!(
        footer.blocks == stats.blocks && footer.transactions == stats.transactions,
        "hot metadata footer totals differ from build report"
    );
    anyhow::ensure!(
        meta_reader
            .read_bytes_with_limit(MARKER_MAX_BYTES as usize, |_| Ok(()))?
            .is_none(),
        "hot metadata has unexpected extra records"
    );
    Ok(())
}

fn validate_degraded_hot_output(root: &Path, receipt: &MaterializedForHot) -> Result<()> {
    let marker: DegradedHotMarker =
        read_json_bounded(&root.join(COMPACTED_MARKER), MANIFEST_MAX_BYTES)?;
    anyhow::ensure!(
        marker.version == MATERIALIZER_VERSION
            && marker.state == "degraded_hot_archive_missing_poh_and_shredding"
            && !marker.canonical
            && !marker.publication_ready
            && marker.block_archive_ready,
        "REPAIR-COMPACTED marker has incompatible state"
    );
    anyhow::ensure!(
        marker.epoch == receipt.epoch
            && marker.epoch_start_slot == receipt.epoch_start_slot
            && marker.epoch_end_slot == receipt.epoch_end_slot
            && marker.live_blocks == receipt.live_blocks
            && marker.rpc_only_blocks == receipt.rpc_only_blocks
            && marker.produced_blocks == receipt.produced_blocks
            && marker.live_blocks.checked_add(marker.rpc_only_blocks)
                == Some(marker.produced_blocks),
        "REPAIR-COMPACTED epoch/count fields differ from materialized receipt"
    );
    anyhow::ensure!(
        marker.files == degraded_hot_files(marker.block_access_ready),
        "REPAIR-COMPACTED file map is not the fixed v1 layout"
    );
    anyhow::ensure!(
        marker.poh_coverage.available_records == receipt.available_poh_records
            && marker.poh_coverage.available_entries == receipt.available_poh_entries
            && marker.poh_coverage.missing_records == receipt.missing_poh_ids.len() as u64
            && marker.poh_coverage.produced_id_space == receipt.produced_blocks
            && marker.poh_coverage.record_ids_have_explicit_gaps
            && marker.poh_coverage.missing_record_ids == receipt.missing_poh_ids,
        "REPAIR-COMPACTED PoH coverage differs from materialized receipt"
    );
    anyhow::ensure!(
        marker.shredding_coverage.available_records == 0
            && marker.shredding_coverage.missing_records == receipt.produced_blocks
            && !marker.shredding_coverage.canonical_sidecar_emitted,
        "REPAIR-COMPACTED shredding coverage is invalid"
    );
    anyhow::ensure!(
        marker.source_materialized_marker_sha256 == receipt.source_marker_sha256
            && marker.source_manifest_sha256 == receipt.source_manifest_sha256
            && marker.source_merge_plan_sha256 == receipt.source_plan_sha256,
        "REPAIR-COMPACTED source digests differ"
    );
    for digest in [
        &marker.source_materialized_marker_sha256,
        &marker.source_manifest_sha256,
        &marker.source_merge_plan_sha256,
    ] {
        decode_hex_32(digest)?;
        anyhow::ensure!(
            digest.bytes().all(|byte| !byte.is_ascii_uppercase()),
            "marker digest is not lowercase"
        );
    }
    validate_degraded_hot_candidates(
        root,
        receipt,
        &DegradedHotStats {
            blocks: marker.produced_blocks,
            transactions: marker.transactions,
            signatures: marker.signatures,
            compressed_bytes: marker.compressed_bytes,
            uncompressed_bytes: marker.uncompressed_bytes,
            zstd_level: marker.zstd_level,
        },
    )?;
    if marker.block_access_ready {
        let previous_tail = read_prev_blockhash_tail(root)?
            .context("block-access-ready output has no previous blockhash tail")?;
        let predecessor = *previous_tail
            .last()
            .context("block-access-ready output has an empty previous blockhash tail")?;
        validate_repair_access_sidecars(
            root,
            root,
            predecessor,
            marker.epoch,
            marker.epoch_start_slot,
            marker.produced_blocks,
        )?;
    }
    Ok(())
}

fn ensure_block_access_artifacts_absent(root: &Path) -> Result<()> {
    for relative in [
        ARCHIVE_V2_BLOCK_ACCESS_FILE,
        ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
        ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ] {
        anyhow::ensure!(
            !root.join(relative).exists(),
            "degraded hot candidate contains premature block-access artifact {relative}"
        );
    }
    Ok(())
}

/// Add the registry-free getBlock access tier to a validated noncanonical repair archive.
///
/// All new artifacts are built below a hidden sibling. They are published before the existing
/// REPAIR-COMPACTED marker is atomically replaced, so `block_access_ready=true` is the commit
/// point and a retry after a partial publication remains safe.
pub(crate) fn build_repair_block_access(repair_dir: &Path, hot_output: &Path) -> Result<()> {
    anyhow::ensure!(
        fs::symlink_metadata(repair_dir)?.file_type().is_dir(),
        "repair input must be a direct non-symlink directory"
    );
    let repair_root = fs::canonicalize(repair_dir)
        .with_context(|| format!("canonicalize repair dir {}", repair_dir.display()))?;
    anyhow::ensure!(repair_root.is_dir(), "repair input is not a directory");
    anyhow::ensure!(
        fs::symlink_metadata(hot_output)?.file_type().is_dir(),
        "repair hot output must be a direct non-symlink directory"
    );
    let hot_root = fs::canonicalize(hot_output)
        .with_context(|| format!("canonicalize hot output {}", hot_output.display()))?;
    anyhow::ensure!(hot_root.is_dir(), "repair hot output is not a directory");
    anyhow::ensure!(
        !hot_root.starts_with(&repair_root) && !repair_root.starts_with(&hot_root),
        "repair bundle and hot output must not contain each other"
    );
    let output_parent = hot_root
        .parent()
        .context("repair hot output has no parent directory")?;
    let output_name = hot_root
        .file_name()
        .context("repair hot output has no final path component")?;
    // Share the compaction lock: block-access publication must never race the marker or the
    // archive files written by the degraded hot builder.
    let _lock = acquire_lock(output_parent, output_name)?;
    let stage = output_parent.join(format!(
        ".{}.repair-block-access-stage",
        output_name.to_string_lossy()
    ));

    let mut context = validate_repair_bundle(repair_root)?;
    validate_plan_and_partial_poh(&mut context)?;
    let predecessor = derive_first_rpc_predecessor(&context)?;
    let marker = validate_repair_hot_output(&hot_root, &context, predecessor)?;
    if marker.block_access_ready {
        info!(
            "Repair block-access tier already complete and valid: {}",
            hot_root.display()
        );
        return Ok(());
    }

    if stage.exists() {
        anyhow::ensure!(
            fs::symlink_metadata(&stage)?.file_type().is_dir(),
            "repair access stage path is not a directory: {}",
            stage.display()
        );
        fs::remove_dir_all(&stage)
            .with_context(|| format!("remove stale access stage {}", stage.display()))?;
    }
    fs::create_dir(&stage).with_context(|| format!("create access stage {}", stage.display()))?;
    write_prev_blockhash_tail(&stage, &[predecessor])?;
    build_block_access_sidecar_with_previous_tail(
        &hot_root.join(ARCHIVE_V2_BLOCKS_FILE),
        &stage,
        &hot_root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        &[predecessor],
    )?;
    validate_repair_access_sidecars(
        &hot_root,
        &stage,
        predecessor,
        context.manifest.epoch,
        context.manifest.epoch_start_slot,
        context.manifest.produced_blocks,
    )?;
    sync_repair_access_candidates(&stage)?;

    revalidate_source_stamps(&context)?;
    anyhow::ensure!(
        derive_first_rpc_predecessor(&context)? == predecessor,
        "first RPC predecessor changed during block-access build"
    );
    anyhow::ensure!(
        validate_repair_hot_output(&hot_root, &context, predecessor)? == marker,
        "REPAIR-COMPACTED output changed during block-access build"
    );

    for relative in repair_access_artifacts() {
        publish_repair_access_candidate(&stage.join(relative), &hot_root.join(relative))?;
    }
    sync_dir(&hot_root)?;
    validate_repair_access_sidecars(
        &hot_root,
        &hot_root,
        predecessor,
        context.manifest.epoch,
        context.manifest.epoch_start_slot,
        context.manifest.produced_blocks,
    )?;
    revalidate_source_stamps(&context)?;
    anyhow::ensure!(
        derive_first_rpc_predecessor(&context)? == predecessor,
        "first RPC predecessor changed before marker publication"
    );
    let mut marker = validate_repair_hot_output(&hot_root, &context, predecessor)?;
    anyhow::ensure!(
        !marker.block_access_ready,
        "REPAIR-COMPACTED marker changed during sidecar publication"
    );
    marker.canonical = false;
    marker.publication_ready = false;
    marker.block_archive_ready = true;
    marker.block_access_ready = true;
    marker.files = degraded_hot_files(true);
    marker.limitations = vec![
        "The block archive and block-access/getBlock indexes are readable for every produced slot, including RPC-only slots.".to_string(),
        "Canonical PoH and shredding sidecars remain intentionally absent; available live PoH stays repair-specific with explicit gaps.".to_string(),
        "This archive remains noncanonical and must not be interpreted as a fully finalized epoch.".to_string(),
    ];
    write_json_atomic(&hot_root.join(COMPACTED_MARKER), &marker)?;
    let published = validate_repair_hot_output(&hot_root, &context, predecessor)?;
    anyhow::ensure!(
        published.block_access_ready,
        "published REPAIR-COMPACTED marker did not commit block access"
    );
    fs::remove_dir(&stage)
        .with_context(|| format!("remove empty access stage {}", stage.display()))?;
    sync_dir(output_parent)?;
    info!(
        "Repair block-access tier complete (noncanonical): epoch={} blocks={} output={}",
        marker.epoch,
        marker.produced_blocks,
        hot_root.display()
    );
    Ok(())
}

fn repair_access_artifacts() -> [&'static str; 4] {
    [
        ARCHIVE_V2_BLOCK_ACCESS_FILE,
        ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
        ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ]
}

fn ensure_repair_predecessor_before_epoch(
    predecessor_slot: u64,
    epoch_start_slot: u64,
) -> Result<()> {
    anyhow::ensure!(
        predecessor_slot < epoch_start_slot,
        "repair predecessor slot {predecessor_slot} is not before repaired epoch start {epoch_start_slot}"
    );
    Ok(())
}

fn derive_first_rpc_predecessor(context: &ValidatedContext) -> Result<PreviousBlockhash> {
    anyhow::ensure!(
        context.rpc_block_ids.first().copied() == Some(0),
        "repair block-access requires the first produced block (id 0) to be RPC"
    );
    let manifest = context
        .manifest
        .rpc_only_slots
        .first()
        .context("repair manifest has no first RPC row")?;
    anyhow::ensure!(
        context.manifest.first_produced_slot == Some(manifest.slot),
        "first RPC slot does not match first produced slot"
    );
    anyhow::ensure!(
        manifest.source_bytes <= REPAIR_BLOCK_ACCESS_MAX_RPC_JSON_BYTES,
        "first RPC getBlock JSON is {} bytes, exceeding repair access limit {}",
        manifest.source_bytes,
        REPAIR_BLOCK_ACCESS_MAX_RPC_JSON_BYTES
    );
    let path = resolve_relative(&context.repair_root, &manifest.source_path, "RPC getBlock")?;
    validate_rpc_source_stamp(&path, manifest, true)?;
    let rpc: RpcGetBlock = read_json_bounded(&path, REPAIR_BLOCK_ACCESS_MAX_RPC_JSON_BYTES)?;
    validate_rpc_source_stamp(&path, manifest, true)?;
    anyhow::ensure!(
        rpc.parent_slot == manifest.parent_slot
            && rpc.blockhash == manifest.blockhash
            && rpc.previous_blockhash == manifest.previous_blockhash,
        "first RPC getBlock chain fields differ from REPAIR-REQUIRED"
    );
    anyhow::ensure!(
        rpc.transactions.len() as u64 == manifest.transactions,
        "first RPC getBlock transaction count differs from REPAIR-REQUIRED"
    );
    ensure_repair_predecessor_before_epoch(manifest.parent_slot, context.manifest.epoch_start_slot)
        .context("first RPC parentSlot is not a valid repair predecessor")?;
    let json_previous = decode_base58_32(&rpc.previous_blockhash, "RPC previousBlockhash")?;
    let manifest_previous = decode_base58_32(
        &manifest.previous_blockhash,
        "manifest RPC previousBlockhash",
    )?;
    anyhow::ensure!(
        json_previous == manifest_previous,
        "first RPC predecessor hash differs between manifest and JSON"
    );
    Ok(PreviousBlockhash {
        hash: json_previous,
        slot: rpc.parent_slot,
    })
}

fn validate_repair_hot_output(
    root: &Path,
    context: &ValidatedContext,
    predecessor: PreviousBlockhash,
) -> Result<DegradedHotMarker> {
    let marker: DegradedHotMarker =
        read_json_bounded(&root.join(COMPACTED_MARKER), MANIFEST_MAX_BYTES)?;
    anyhow::ensure!(
        marker.epoch == context.manifest.epoch
            && marker.epoch_start_slot == context.manifest.epoch_start_slot
            && marker.epoch_end_slot == context.manifest.epoch_end_slot
            && marker.live_blocks == context.manifest.live_blocks
            && marker.rpc_only_blocks == context.manifest.rpc_only_blocks
            && marker.produced_blocks == context.manifest.produced_blocks,
        "REPAIR-COMPACTED counts do not match REPAIR-REQUIRED"
    );
    anyhow::ensure!(
        marker.source_manifest_sha256 == context.manifest_sha256
            && marker.source_merge_plan_sha256 == context.plan_sha256,
        "REPAIR-COMPACTED provenance does not match REPAIR-REQUIRED"
    );

    let source_marker_path = root.join(SOURCE_MATERIALIZED_MARKER);
    ensure_regular_file(&source_marker_path, MARKER_MAX_BYTES)?;
    let source_marker_bytes = fs::read(&source_marker_path)?;
    let source_marker_sha256 = hex_digest(Sha256::digest(&source_marker_bytes));
    let source_marker: MaterializedMarker = serde_json::from_slice(&source_marker_bytes)
        .context("decode retained source REPAIR-MATERIALIZED marker")?;
    anyhow::ensure!(
        source_marker.version == MATERIALIZER_VERSION
            && source_marker.state == "repair_materialized_missing_poh_and_shredding"
            && !source_marker.canonical
            && !source_marker.publication_ready,
        "retained source REPAIR-MATERIALIZED marker has incompatible state"
    );
    anyhow::ensure!(
        source_marker.epoch == context.manifest.epoch
            && source_marker.epoch_start_slot == context.manifest.epoch_start_slot
            && source_marker.epoch_end_slot == context.manifest.epoch_end_slot
            && source_marker.live_blocks == context.manifest.live_blocks
            && source_marker.rpc_only_blocks == context.manifest.rpc_only_blocks
            && source_marker.produced_blocks == context.manifest.produced_blocks
            && source_marker.first_produced_slot == context.manifest.first_produced_slot
            && source_marker.last_produced_slot == context.manifest.last_produced_slot
            && source_marker.transactions == marker.transactions
            && source_marker.missing_poh_and_shredding_blocks == context.manifest.rpc_only_blocks
            && source_marker.normalized_blocks == OUTPUT_BLOCKS
            && source_marker.produced_blockhashes == OUTPUT_BLOCKHASHES
            && source_marker.available_poh == OUTPUT_AVAILABLE_POH
            && source_marker.exact_pubkey_runs == OUTPUT_PUBKEY_RUNS,
        "retained source REPAIR-MATERIALIZED marker differs from REPAIR-REQUIRED"
    );
    anyhow::ensure!(
        source_marker.manifest_sha256 == context.manifest_sha256
            && source_marker.merge_plan_sha256 == context.plan_sha256
            && source_marker_sha256 == marker.source_materialized_marker_sha256,
        "retained materialization provenance differs from REPAIR-COMPACTED"
    );

    let receipt = MaterializedForHot {
        epoch: context.manifest.epoch,
        epoch_start_slot: context.manifest.epoch_start_slot,
        epoch_end_slot: context.manifest.epoch_end_slot,
        live_blocks: context.manifest.live_blocks,
        rpc_only_blocks: context.manifest.rpc_only_blocks,
        produced_blocks: context.manifest.produced_blocks,
        available_poh_records: context.manifest.poh.records,
        available_poh_entries: context.manifest.poh.entries,
        missing_poh_ids: context.manifest.poh.missing_record_ids.clone(),
        source_marker_sha256,
        source_manifest_sha256: context.manifest_sha256.clone(),
        source_plan_sha256: context.plan_sha256.clone(),
        root: PathBuf::new(),
        input_fingerprint: String::new(),
    };
    validate_degraded_hot_output(root, &receipt)?;
    let hot_index = read_archive_v2_hot_block_index(&root.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    anyhow::ensure!(
        hot_index.rows.first().map(|row| row.slot) == context.manifest.first_produced_slot,
        "hot archive first slot differs from REPAIR-REQUIRED"
    );
    if marker.block_access_ready {
        validate_repair_access_sidecars(
            root,
            root,
            predecessor,
            context.manifest.epoch,
            context.manifest.epoch_start_slot,
            context.manifest.produced_blocks,
        )?;
    }
    Ok(marker)
}

fn checked_repair_block_access_frame_len(block_id: u32, access_len: u32) -> Result<usize> {
    anyhow::ensure!(
        u64::from(access_len) <= ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
        "repair block-access payload {block_id} is {access_len} bytes, exceeding the shared {} byte limit",
        ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES
    );
    usize::try_from(access_len).context("repair block-access length exceeds usize")
}

fn validate_repair_access_sidecars(
    hot_root: &Path,
    access_root: &Path,
    predecessor: PreviousBlockhash,
    epoch: u64,
    epoch_start_slot: u64,
    produced_blocks: u64,
) -> Result<()> {
    ensure_regular_file(&access_root.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE), 40)?;
    let tail = read_prev_blockhash_tail(access_root)?
        .context("repair block-access previous blockhash tail is missing")?;
    anyhow::ensure!(
        tail == [predecessor],
        "repair block-access previous blockhash tail is not the validated first RPC predecessor"
    );
    ensure_repair_predecessor_before_epoch(predecessor.slot, epoch_start_slot)
        .context("previous-blockhash tail is not valid for repaired epoch")?;

    let hot_index = read_archive_v2_hot_block_index(&hot_root.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    anyhow::ensure!(
        hot_index.rows.len() as u64 == produced_blocks,
        "hot block index row count differs from repair produced count"
    );
    let access_path = access_root.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    ensure_regular_file(&access_path, u64::MAX)?;
    let access_index_path = access_root.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    ensure_regular_file(&access_index_path, u64::MAX)?;
    let access_index = read_archive_v2_block_access_index(&access_index_path)?;
    let access_bytes = fs::metadata(&access_path)?.len();
    anyhow::ensure!(
        access_index.flags == 0
            && access_index.blob_file_bytes == access_bytes
            && access_index.rows.len() == hot_index.rows.len(),
        "block-access index header/totals differ from hot archive"
    );
    let mut next_access_offset = 0u64;
    for (expected_id, (hot, access)) in hot_index
        .rows
        .iter()
        .zip(access_index.rows.iter())
        .enumerate()
    {
        checked_repair_block_access_frame_len(access.block_id, access.access_len)?;
        anyhow::ensure!(
            hot.block_id as usize == expected_id
                && access.block_id == hot.block_id
                && access.slot == hot.slot
                && access.tx_count == hot.tx_count
                && access.signature_count == hot.signature_count
                && access.access_offset == next_access_offset
                && access.access_len > 0,
            "block-access index row {} differs from hot block index",
            expected_id
        );
        next_access_offset = next_access_offset
            .checked_add(u64::from(access.access_len))
            .context("block-access index offset overflow")?;
    }
    anyhow::ensure!(
        next_access_offset == access_bytes,
        "block-access index does not cover its blob exactly"
    );

    let get_block_path = access_root.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE);
    ensure_regular_file(&get_block_path, u64::MAX)?;
    let get_block = read_archive_v2_get_block_index(&get_block_path)?;
    let expected_get = build_get_block_index_rows(&hot_index.rows, &access_index.rows, epoch)?;
    anyhow::ensure!(
        get_block.rows.len() == expected_get.len(),
        "get-block index row count differs from one epoch"
    );
    for (slot_offset, (actual, expected)) in
        get_block.rows.iter().zip(expected_get.iter()).enumerate()
    {
        anyhow::ensure!(
            actual.block_offset == expected.block_offset
                && actual.block_len == expected.block_len
                && actual.access_offset == expected.access_offset
                && actual.access_len == expected.access_len,
            "get-block index differs at epoch slot offset {}",
            slot_offset
        );
    }

    let first_hot = hot_index
        .rows
        .first()
        .context("repair hot archive has no first block")?;
    anyhow::ensure!(
        predecessor.slot < first_hot.slot,
        "first RPC predecessor slot is not before the first produced slot"
    );
    let first_access = access_index
        .rows
        .first()
        .context("repair block-access index has no first row")?;
    let first_access_len =
        checked_repair_block_access_frame_len(first_access.block_id, first_access.access_len)?;
    let mut bytes = vec![0u8; first_access_len];
    let mut file = File::open(&access_path)?;
    file.seek(SeekFrom::Start(first_access.access_offset))?;
    file.read_exact(&mut bytes)?;
    let first: ArchiveV2BlockAccessBlob =
        wincode::config::deserialize(&bytes, wincode_leb128_config())
            .context("decode first repair block-access payload")?;
    anyhow::ensure!(
        first.version == WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION
            && first.flags == 0
            && first.previous_blockhash == predecessor.hash,
        "first block-access payload does not contain the validated RPC predecessor hash"
    );
    let mut first_registry_hash = [0u8; 32];
    File::open(hot_root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))?
        .read_exact(&mut first_registry_hash)?;
    anyhow::ensure!(
        first.blockhash == first_registry_hash,
        "first block-access blockhash differs from blockhash registry"
    );
    Ok(())
}

fn sync_repair_access_candidates(stage: &Path) -> Result<()> {
    for relative in repair_access_artifacts() {
        File::open(stage.join(relative))?
            .sync_all()
            .with_context(|| format!("sync repair access candidate {relative}"))?;
    }
    sync_dir(stage)
}

fn publish_repair_access_candidate(source: &Path, target: &Path) -> Result<()> {
    match fs::symlink_metadata(target) {
        Ok(metadata) => anyhow::ensure!(
            metadata.file_type().is_file(),
            "existing repair access target is not a regular file: {}",
            target.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("stat access target {}", target.display()));
        }
    }
    fs::rename(source, target)
        .with_context(|| format!("publish {} to {}", source.display(), target.display()))
}

fn validate_pubkey_run_directory(dir: &Path) -> Result<()> {
    anyhow::ensure!(dir.is_dir(), "pubkey run directory is missing");
    let mut files = 0u64;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !(name.starts_with("run-") && name.ends_with(".bin")) {
            continue;
        }
        ensure_regular_file(&entry.path(), u64::MAX)?;
        anyhow::ensure!(
            fs::metadata(entry.path())?.len() % 36 == 0,
            "pubkey run {} has invalid byte length",
            entry.path().display()
        );
        files += 1;
    }
    anyhow::ensure!(files > 0, "pubkey run directory has no runs");
    Ok(())
}

fn materialized_input_fingerprint(root: &Path) -> Result<String> {
    let mut paths = vec![
        root.join(MATERIALIZED_MARKER),
        root.join(OUTPUT_BLOCKS),
        root.join(OUTPUT_BLOCKHASHES),
        root.join(OUTPUT_AVAILABLE_POH),
    ];
    for entry in fs::read_dir(root.join(OUTPUT_PUBKEY_RUNS))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("run-") && name.ends_with(".bin") {
            paths.push(entry.path());
        }
    }
    paths.sort();
    let mut hasher = Sha256::new();
    for path in paths {
        let metadata = fs::symlink_metadata(&path)?;
        anyhow::ensure!(
            metadata.file_type().is_file(),
            "materialized fingerprint input is not a regular file: {}",
            path.display()
        );
        let relative = path
            .strip_prefix(root)?
            .to_str()
            .context("materialized fingerprint path is not UTF-8")?;
        hasher.update((relative.len() as u64).to_le_bytes());
        hasher.update(relative.as_bytes());
        hasher.update(metadata.len().to_le_bytes());
        hasher.update(modified_nanos(&metadata)?.to_le_bytes());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            hasher.update(metadata.dev().to_le_bytes());
            hasher.update(metadata.ino().to_le_bytes());
        }
    }
    Ok(hex_digest(hasher.finalize()))
}

fn sync_degraded_hot_candidates(root: &Path) -> Result<()> {
    for relative in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
        OUTPUT_AVAILABLE_POH,
        SOURCE_MATERIALIZED_MARKER,
        HOT_PROGRESS_JSON,
    ] {
        let path = root.join(relative);
        File::open(&path)
            .with_context(|| format!("open degraded candidate {}", path.display()))?
            .sync_all()
            .with_context(|| format!("sync degraded candidate {}", path.display()))?;
    }
    sync_dir(&root.join("repair"))?;
    sync_dir(root)
}

fn sync_tree_candidates(stage: &Path) -> Result<()> {
    for path in [
        stage.join(OUTPUT_BLOCKS),
        stage.join(OUTPUT_BLOCKHASHES),
        stage.join(OUTPUT_AVAILABLE_POH),
        stage.join(CHECKPOINT),
        stage.join(PROGRESS_JSON),
    ] {
        File::open(&path)
            .with_context(|| format!("open candidate {}", path.display()))?
            .sync_all()
            .with_context(|| format!("sync candidate {}", path.display()))?;
    }
    for path in [
        stage.join("blocks"),
        stage.join(OUTPUT_PUBKEY_RUNS),
        stage.join("index"),
        stage.join("repair"),
    ] {
        sync_dir(&path)?;
    }
    Ok(())
}

fn hard_link_or_copy_atomic(source: &Path, target: &Path) -> Result<()> {
    if target.exists() {
        anyhow::ensure!(
            fs::metadata(source)?.len() == fs::metadata(target)?.len(),
            "staged sidecar length differs from source: {}",
            target.display()
        );
        return Ok(());
    }
    let temp = target.with_extension("materialize.tmp");
    if temp.exists() {
        fs::remove_file(&temp)?;
    }
    match fs::hard_link(source, &temp) {
        Ok(()) => {}
        Err(error) => {
            warn!(
                "Hard-link unavailable for repair sidecar {} -> {} ({error}); copying",
                source.display(),
                target.display()
            );
            let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, File::open(source)?);
            let file = File::create(&temp)?;
            let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
            std::io::copy(&mut reader, &mut writer)?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
    }
    File::open(&temp)?.sync_all()?;
    fs::rename(&temp, target)?;
    sync_dir(target.parent().context("sidecar target has no parent")?)
}

fn resolve_relative(root: &Path, relative: &str, kind: &str) -> Result<PathBuf> {
    anyhow::ensure!(
        !relative.is_empty() && relative.len() <= 1024,
        "{kind} relative path has invalid length"
    );
    let relative_path = Path::new(relative);
    anyhow::ensure!(
        !relative_path.is_absolute()
            && relative_path
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
        "{kind} path must be a simple relative path: {relative}"
    );
    let path = fs::canonicalize(root.join(relative_path))
        .with_context(|| format!("canonicalize {kind} {}", root.join(relative_path).display()))?;
    anyhow::ensure!(
        path.starts_with(root),
        "{kind} escapes repair root: {}",
        path.display()
    );
    Ok(path)
}

fn ensure_regular_file(path: &Path, max_bytes: u64) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("stat {}", path.display()))?;
    anyhow::ensure!(
        metadata.file_type().is_file(),
        "path is not a regular non-symlink file: {}",
        path.display()
    );
    anyhow::ensure!(
        metadata.len() > 0 && metadata.len() <= max_bytes,
        "file {} has invalid byte length {} (max {})",
        path.display(),
        metadata.len(),
        max_bytes
    );
    Ok(())
}

fn read_json_bounded<T: for<'de> Deserialize<'de>>(path: &Path, max_bytes: u64) -> Result<T> {
    ensure_regular_file(path, max_bytes)?;
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    serde_json::from_reader(BufReader::new(file))
        .with_context(|| format!("decode JSON {}", path.display()))
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("JSON output has no parent")?;
    fs::create_dir_all(parent)?;
    let temp = path.with_extension("json.tmp");
    let file = File::create(&temp).with_context(|| format!("create {}", temp.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(&temp, path).with_context(|| format!("publish JSON {}", path.display()))?;
    sync_dir(parent)
}

fn read_bounded_line<R: BufRead>(reader: &mut R, max_bytes: usize) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            break;
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        anyhow::ensure!(
            line.len().saturating_add(take) <= max_bytes,
            "JSONL line exceeds configured {} byte limit",
            max_bytes
        );
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if line.last() == Some(&b'\n') {
            break;
        }
    }
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    Ok(Some(line))
}

fn file_stamp(root: &Path, path: &Path) -> Result<FileStamp> {
    let metadata = fs::metadata(path)?;
    let relative = path
        .strip_prefix(root)
        .with_context(|| format!("{} is outside {}", path.display(), root.display()))?
        .to_str()
        .context("source relative path is not UTF-8")?
        .to_string();
    #[cfg(unix)]
    let (device, inode) = {
        use std::os::unix::fs::MetadataExt;
        (Some(metadata.dev()), Some(metadata.ino()))
    };
    #[cfg(not(unix))]
    let (device, inode) = (None, None);
    Ok(FileStamp {
        relative_path: relative,
        len: metadata.len(),
        modified_nanos: modified_nanos(&metadata)?,
        device,
        inode,
    })
}

fn modified_nanos(metadata: &fs::Metadata) -> Result<u128> {
    Ok(metadata
        .modified()
        .context("read file modification time")?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos())
}

fn sha256_file_hex(path: &Path) -> Result<String> {
    let mut file = BufReader::with_capacity(IO_BUFFER_BYTES, File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_digest(hasher.finalize()))
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    anyhow::ensure!(value.len() == 64, "digest is not 64 hex characters");
    let mut output = [0u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .context("decode hex digest")?;
    }
    Ok(output)
}

fn decode_base58_32(value: &str, field: &str) -> Result<[u8; 32]> {
    let decoded = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode {field} base58"))?;
    decoded
        .try_into()
        .map_err(|value: Vec<u8>| anyhow!("{field} decoded to {} bytes, expected 32", value.len()))
}

fn varint_len(mut value: u32) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

fn acquire_lock(parent: &Path, output_name: &std::ffi::OsStr) -> Result<File> {
    let lock_path = parent.join(format!(
        ".{}.repair-materialize.lock",
        output_name.to_string_lossy()
    ));
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open lock {}", lock_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        anyhow::ensure!(
            result == 0,
            "another repair materializer holds {}",
            lock_path.display()
        );
    }
    file.set_len(0)?;
    writeln!(
        file,
        "pid={} started_unix={}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    )?;
    file.sync_all()?;
    Ok(file)
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_size > 0).then(|| pages.saturating_mul(page_size as u64))
}

#[cfg(not(target_os = "linux"))]
fn current_rss_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{
        CompactPohEntry, WincodeArchiveV2NoRegistryRewards, WincodeLeb128FramedWriter,
    };

    fn temp_dir(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-repair-materializer-{label}-{}-{stamp}",
            std::process::id()
        ))
    }

    fn hash(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    fn b58(bytes: [u8; 32]) -> String {
        bs58::encode(bytes).into_string()
    }

    fn valid_wire_transaction(signature: u8, account: u8) -> String {
        let mut bytes = Vec::new();
        bytes.push(1);
        bytes.extend_from_slice(&[signature; 64]);
        bytes.extend_from_slice(&[1, 0, 0]);
        bytes.push(1);
        bytes.extend_from_slice(&[account; 32]);
        bytes.extend_from_slice(&hash(10));
        bytes.push(0);
        BASE64_STANDARD.encode(bytes)
    }

    #[test]
    fn decodes_rpc_legacy_unit_borsh_io_instruction_error() {
        // Exact shape retained for epoch 1000, slot 432000723, transaction 80.
        let decoded = decode_rpc_transaction_error(serde_json::json!({
            "InstructionError": [1, "BorshIoError"]
        }))
        .unwrap();

        assert!(matches!(
            decoded,
            CompactTransactionError::InstructionError(
                1,
                CompactInstructionError::BorshIoError(ref message)
            ) if message.is_empty()
        ));

        assert!(
            decode_rpc_transaction_error(serde_json::json!({
                "InstructionError": [1, "BorshIoError", "unexpected"]
            }))
            .is_err(),
            "the legacy compatibility path must reject noncanonical shapes"
        );
    }

    #[test]
    fn repair_access_contract_rejects_oversized_later_rows_and_in_epoch_predecessors() {
        let limit = u32::try_from(ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES).unwrap();
        assert_eq!(
            checked_repair_block_access_frame_len(0, limit).unwrap(),
            limit as usize
        );
        let error = checked_repair_block_access_frame_len(1, limit + 1).unwrap_err();
        assert!(error.to_string().contains("payload 1"));
        assert!(error.to_string().contains("exceeding the shared"));

        let epoch_start = crate::SLOTS_PER_EPOCH;
        ensure_repair_predecessor_before_epoch(epoch_start - 1, epoch_start).unwrap();
        let error = ensure_repair_predecessor_before_epoch(epoch_start, epoch_start).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("not before repaired epoch start")
        );
    }

    fn write_fixture(root: &Path) -> Result<()> {
        write_ordered_fixture(root, false)
    }

    fn write_rpc_first_fixture(root: &Path) -> Result<()> {
        write_ordered_fixture(root, true)
    }

    fn write_ordered_fixture(root: &Path, rpc_first: bool) -> Result<()> {
        let epoch = if rpc_first { 1u64 } else { 0 };
        let epoch_start_slot = epoch * crate::SLOTS_PER_EPOCH;
        let epoch_end_slot = epoch_start_slot + crate::SLOTS_PER_EPOCH - 1;
        let first_produced_slot = epoch_start_slot + 10;
        let last_produced_slot = epoch_start_slot + 11;
        fs::create_dir_all(root.join("sources/source-000"))?;
        fs::create_dir_all(root.join(format!("repair/rpc-get-block/epoch-{epoch}")))?;
        let live_slot = if rpc_first {
            last_produced_slot
        } else {
            first_produced_slot
        };
        let live_parent_slot = if rpc_first {
            first_produced_slot
        } else {
            epoch_start_slot + 9
        };
        let live_block_id = if rpc_first { 1 } else { 0 };
        let rpc_slot = if rpc_first {
            first_produced_slot
        } else {
            last_produced_slot
        };
        let rpc_parent_slot = if rpc_first {
            epoch_start_slot - 1
        } else {
            first_produced_slot
        };
        let rpc_hash = if rpc_first { hash(10) } else { hash(11) };
        let rpc_previous_hash = if rpc_first { hash(9) } else { hash(10) };
        let rpc_block_id = if rpc_first { 0 } else { 1 };
        let live_block = WincodeArchiveV2NoRegistryBlock {
            header: WincodeArchiveV2NoRegistryBlockHeader {
                compact: CompactBlockHeader {
                    slot: live_slot,
                    parent_slot: live_parent_slot,
                    blockhash: 0,
                    previous_blockhash: 0,
                    block_time: Some(live_slot as i64),
                    block_height: Some(live_slot),
                    shredding: Vec::new(),
                    poh_entries: Vec::new(),
                    rewards: None,
                },
                rewards: Some(WincodeArchiveV2NoRegistryRewards {
                    source_len: 1,
                    num_partitions: None,
                    decoded: Some(Vec::new()),
                    raw_fallback: None,
                    decode_error: None,
                }),
            },
            txs: Vec::new(),
        };
        let payload = wincode::config::serialize(&live_block, wincode_leb128_config())?;
        let source_path = root.join("sources/source-000/live-no-registry-blocks.bin");
        let mut source = BufWriter::new(File::create(&source_path)?);
        write_u32_varint(&mut source, payload.len() as u32)?;
        source.write_all(&payload)?;
        source.flush()?;
        let source_len = fs::metadata(&source_path)?.len();

        let plan_path = root.join(REPAIR_PLAN_DEFAULT);
        let mut plan = BufWriter::new(File::create(&plan_path)?);
        serde_json::to_writer(
            &mut plan,
            &serde_json::json!({
                "kind": "header",
                "version": 1,
                "epoch": epoch,
                "expected_live_blocks": 1,
                "expected_rpc_blocks": 1,
                "expected_produced_blocks": 2,
                "block_id_space": "produced_ordinal",
                "live_rows_have_explicit_rpc_gaps": true,
                "sources": [{
                    "source_id": 0,
                    "block_path": "sources/source-000/live-no-registry-blocks.bin"
                }]
            }),
        )?;
        plan.write_all(b"\n")?;
        serde_json::to_writer(
            &mut plan,
            &serde_json::json!({
                "kind": "block",
                "block_id": live_block_id,
                "slot": live_slot,
                "parent_slot": live_parent_slot,
                "source_id": 0,
                "source_block_id": 0,
                "source_offset": 0,
                "block_len": payload.len(),
                "tx_count": 0
            }),
        )?;
        plan.write_all(b"\n")?;
        plan.flush()?;

        let poh_path = root.join(SOURCE_AVAILABLE_POH);
        let mut poh = WincodeLeb128FramedWriter::new(BufWriter::new(File::create(&poh_path)?));
        poh.write(&WincodeArchiveV2PohRecord {
            block_id: live_block_id,
            slot: live_slot,
            entries: vec![CompactPohEntry {
                num_hashes: 1,
                hash: hash((live_slot - epoch_start_slot) as u8),
                tx_count: 0,
            }],
        })?;
        poh.flush()?;
        fs::write(
            root.join(PRODUCED_BLOCKHASHES),
            [hash(10).as_slice(), hash(11).as_slice()].concat(),
        )?;

        let rpc_relative =
            format!("repair/rpc-get-block/epoch-{epoch}/slot-{rpc_slot}.getBlock.json");
        let rpc_path = root.join(&rpc_relative);
        let rpc = serde_json::json!({
            "blockhash": b58(rpc_hash),
            "previousBlockhash": b58(rpc_previous_hash),
            "parentSlot": rpc_parent_slot,
            "blockTime": rpc_slot,
            "blockHeight": rpc_slot,
            "numRewardPartitions": null,
            "rewards": [],
            "transactions": [{
                "transaction": [valid_wire_transaction(3, 7), "base64"],
                "meta": {
                    "err": {"InstructionError": [1, "BorshIoError"]},
                    "fee": 5000,
                    "preBalances": [10000],
                    "postBalances": [5000],
                    "innerInstructions": [],
                    "logMessages": [],
                    "preTokenBalances": [],
                    "postTokenBalances": [],
                    "rewards": [],
                    "loadedAddresses": {"writable": [], "readonly": []},
                    "returnData": null,
                    "computeUnitsConsumed": 10,
                    "costUnits": 20
                }
            }]
        });
        fs::write(&rpc_path, serde_json::to_vec(&rpc)?)?;
        let rpc_metadata = fs::metadata(&rpc_path)?;
        #[cfg(unix)]
        let (rpc_device, rpc_inode) = {
            use std::os::unix::fs::MetadataExt;
            (Some(rpc_metadata.dev()), Some(rpc_metadata.ino()))
        };
        #[cfg(not(unix))]
        let (rpc_device, rpc_inode) = (None, None);
        let rpc_sha = sha256_file_hex(&rpc_path)?;

        let marker = serde_json::json!({
            "version": 1,
            "state": "rpc_fallback_missing_poh_and_shredding",
            "epoch": epoch,
            "epoch_start_slot": epoch_start_slot,
            "epoch_end_slot": epoch_end_slot,
            "live_blocks": 1,
            "rpc_only_blocks": 1,
            "produced_blocks": 2,
            "blockhash_records": 2,
            "duplicate_live_blocks": 0,
            "first_produced_slot": first_produced_slot,
            "last_produced_slot": last_produced_slot,
            "in_epoch_produced_parent_chain_validated": true,
            "poh": {
                "path": SOURCE_AVAILABLE_POH,
                "records": 1,
                "entries": 1,
                "block_ids_rewritten": true,
                "rpc_only_records_omitted": 1,
                "produced_id_space": 2,
                "record_ids_have_explicit_rpc_gaps": true,
                "missing_record_ids": [rpc_block_id]
            },
            "normalized_frames": {
                "storage": "hard_linked_unmodified",
                "header_blockhash_ids": "source_local",
                "header_previous_blockhash_ids": "source_local",
                "canonical_produced_ids_only_in_merge_plan": true,
                "materializer_must_remap_header_ids": true,
                "current_live_finalizer_compatible": false
            },
            "block_sources": [{
                "source_id": 0,
                "retained_block_path": "sources/source-000/live-no-registry-blocks.bin",
                "selected_blocks": 1,
                "contributed_blocks": 1,
                "selected_block_end_offset": source_len,
                "retained_block_bytes": source_len,
                "retention": "hard_link"
            }],
            "rpc_only_slots": [{
                "slot": rpc_slot,
                "parent_slot": rpc_parent_slot,
                "blockhash": b58(rpc_hash),
                "previous_blockhash": b58(rpc_previous_hash),
                "transactions": 1,
                "source_path": rpc_relative,
                "source_bytes": rpc_metadata.len(),
                "source_sha256": rpc_sha,
                "source_modified_nanos": modified_nanos(&rpc_metadata)?,
                "source_device": rpc_device,
                "source_inode": rpc_inode,
                "state": "rpc_fallback"
            }],
            "merge_plan": REPAIR_PLAN_DEFAULT,
            "pubkey_runs": {
                "path": "repair/live-pubkey-runs",
                "coverage": "live_blocks_only",
                "rpc_pubkeys_not_indexed": true,
                "registry_complete": false
            },
            "publication_ready": false
        });
        fs::write(
            root.join(REPAIR_MARKER),
            serde_json::to_vec_pretty(&marker)?,
        )?;
        Ok(())
    }

    #[test]
    fn materializes_and_resumes_without_canonical_sidecars() {
        let root = temp_dir("resume");
        let repair = root.join("repair-bundle");
        let output = root.join("epoch-0-materialized");
        fs::create_dir_all(&repair).unwrap();
        write_fixture(&repair).unwrap();
        let fixture_rpc: RpcGetBlock = read_json_bounded(
            &repair.join("repair/rpc-get-block/epoch-0/slot-11.getBlock.json"),
            1 << 20,
        )
        .unwrap();
        let expected_rpc_meta_source_len = serde_json::to_vec(
            fixture_rpc.transactions[0]
                .meta
                .as_ref()
                .expect("fixture RPC metadata"),
        )
        .unwrap()
        .len() as u64;

        let mut options = RepairMaterializeOptions {
            max_rpc_json_bytes: 1 << 20,
            checkpoint_every: 1,
            pubkey_run_max_keys: 2,
            max_blocks: Some(1),
        };
        materialize_live_repair(&repair, &output, options).unwrap();
        assert!(!output.exists());
        let stage = root.join(".epoch-0-materialized.repair-materialize-stage");
        let progress: MaterializeProgress =
            read_json_bounded(&stage.join(PROGRESS_JSON), MARKER_MAX_BYTES).unwrap();
        assert_eq!(progress.blocks_done, 1);
        assert_eq!(progress.phase, "materializing");
        assert!(progress.started_unix_secs > 0);
        assert!(progress.blocks_per_sec.is_finite());

        options.max_blocks = None;
        materialize_live_repair(&repair, &output, options).unwrap();
        assert!(output.join(MATERIALIZED_MARKER).is_file());
        assert!(!output.join("READY").exists());
        assert!(!output.join("poh/poh.wincode").exists());
        assert!(!output.join("poh.wincode").exists());
        assert!(!output.join("shredding.wincode").exists());
        assert!(!output.join("archive-v2-meta.wincode").exists());

        let marker: MaterializedMarker =
            read_json_bounded(&output.join(MATERIALIZED_MARKER), MARKER_MAX_BYTES).unwrap();
        assert!(!marker.canonical);
        assert_eq!(marker.produced_blocks, 2);
        assert_eq!(marker.rpc_only_blocks, 1);
        let progress: MaterializeProgress =
            read_json_bounded(&output.join(PROGRESS_JSON), MARKER_MAX_BYTES).unwrap();
        assert_eq!(progress.phase, "complete_noncanonical");
        assert_eq!(progress.blocks_done, 2);

        let blocks = File::open(output.join(OUTPUT_BLOCKS)).unwrap();
        let mut reader = WincodeLeb128FramedReader::new(BufReader::new(blocks));
        let (_, live) = reader
            .read::<WincodeArchiveV2NoRegistryBlock>()
            .unwrap()
            .unwrap();
        let (_, rpc) = reader
            .read::<WincodeArchiveV2NoRegistryBlock>()
            .unwrap()
            .unwrap();
        assert!(
            reader
                .read::<WincodeArchiveV2NoRegistryBlock>()
                .unwrap()
                .is_none()
        );
        assert_eq!(live.header.compact.blockhash, 0);
        assert_eq!(rpc.header.compact.blockhash, 1);
        assert_eq!(rpc.header.compact.slot, 11);
        assert_eq!(rpc.txs.len(), 1);
        let Some(WincodeArchiveV2Payload::Decoded {
            source_len,
            value: meta,
        }) = rpc.txs[0].metadata.as_ref()
        else {
            panic!("RPC metadata was not decoded")
        };
        assert_eq!(*source_len, expected_rpc_meta_source_len);
        assert!(matches!(
            meta.err,
            Some(CompactTransactionError::InstructionError(
                1,
                CompactInstructionError::BorshIoError(ref message)
            )) if message.is_empty()
        ));
        assert!(
            fs::read_dir(output.join(OUTPUT_PUBKEY_RUNS))
                .unwrap()
                .any(|entry| entry.unwrap().metadata().unwrap().len() >= 36)
        );

        let hot_output = root.join("epoch-0-degraded-hot");
        super::super::build_degraded_hot_blocks_from_repair(&output, &hot_output, 1, None).unwrap();
        let compacted: DegradedHotMarker =
            read_json_bounded(&hot_output.join(COMPACTED_MARKER), MANIFEST_MAX_BYTES).unwrap();
        assert!(!compacted.canonical);
        assert!(compacted.block_archive_ready);
        assert!(!compacted.block_access_ready);
        assert_eq!(compacted.produced_blocks, 2);
        assert_eq!(compacted.poh_coverage.available_records, 1);
        assert_eq!(compacted.poh_coverage.missing_record_ids, vec![1]);
        assert_eq!(compacted.shredding_coverage.available_records, 0);
        for forbidden in [
            "READY",
            "poh.wincode",
            "shredding.wincode",
            "archive-v2-block-access.wincode",
            "archive-v2-block-access.index",
        ] {
            assert!(!hot_output.join(forbidden).exists(), "{forbidden}");
        }
        let index =
            read_archive_v2_hot_block_index(&hot_output.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        assert_eq!(index.rows.len(), 2);
        assert_eq!(
            index.blob_file_bytes,
            fs::metadata(hot_output.join(ARCHIVE_V2_BLOCKS_FILE))
                .unwrap()
                .len()
        );
        let mut meta_reader = WincodeLeb128FramedReader::new(BufReader::new(
            File::open(hot_output.join(ARCHIVE_V2_META_FILE)).unwrap(),
        ));
        assert!(matches!(
            meta_reader
                .read::<ArchiveV2HotMetaRecord>()
                .unwrap()
                .unwrap()
                .1,
            ArchiveV2HotMetaRecord::Header(_)
        ));
        let (_, meta_footer) = meta_reader
            .read::<ArchiveV2HotMetaRecord>()
            .unwrap()
            .unwrap();
        let ArchiveV2HotMetaRecord::Footer(meta_footer) = meta_footer else {
            panic!("missing hot footer")
        };
        assert_eq!(meta_footer.blocks, 2);
        assert_eq!(meta_footer.transactions, 1);
        assert!(
            meta_reader
                .read::<ArchiveV2HotMetaRecord>()
                .unwrap()
                .is_none()
        );
        let first_row = index.rows[0];
        let mut compressed = vec![0u8; first_row.compressed_len as usize];
        let mut blocks_file = File::open(hot_output.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap();
        blocks_file
            .seek(SeekFrom::Start(first_row.compressed_offset))
            .unwrap();
        blocks_file.read_exact(&mut compressed).unwrap();
        let decoded =
            zstd::bulk::decompress(&compressed, first_row.uncompressed_len as usize).unwrap();
        let hot_block = blockzilla_format::deserialize_archive_v2_hot_block_blob(&decoded).unwrap();
        assert_eq!(hot_block.header.slot, 10);
        let error = build_repair_block_access(&repair, &hot_output).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("first produced block (id 0) to be RPC")
        );
        let still_unready: DegradedHotMarker =
            read_json_bounded(&hot_output.join(COMPACTED_MARKER), MANIFEST_MAX_BYTES).unwrap();
        assert!(!still_unready.block_access_ready);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn rpc_first_repair_builds_block_access_marker_last_and_reruns_as_noop() {
        let root = temp_dir("rpc-first-access");
        let repair = root.join("repair-bundle");
        let materialized = root.join("epoch-1-materialized");
        let hot_output = root.join("epoch-1-degraded-hot");
        fs::create_dir_all(&repair).unwrap();
        write_rpc_first_fixture(&repair).unwrap();

        materialize_live_repair(
            &repair,
            &materialized,
            RepairMaterializeOptions {
                max_rpc_json_bytes: 1 << 20,
                checkpoint_every: 8,
                pubkey_run_max_keys: 16,
                max_blocks: None,
            },
        )
        .unwrap();
        super::super::build_degraded_hot_blocks_from_repair(&materialized, &hot_output, 1, None)
            .unwrap();

        let before: DegradedHotMarker =
            read_json_bounded(&hot_output.join(COMPACTED_MARKER), MANIFEST_MAX_BYTES).unwrap();
        assert!(!before.block_access_ready);
        assert_eq!(before.files.block_access, None);
        let original_marker = fs::read(hot_output.join(COMPACTED_MARKER)).unwrap();
        let mut wrong_provenance: Value = serde_json::from_slice(&original_marker).unwrap();
        wrong_provenance["source_manifest_sha256"] = Value::String("00".repeat(32));
        fs::write(
            hot_output.join(COMPACTED_MARKER),
            serde_json::to_vec_pretty(&wrong_provenance).unwrap(),
        )
        .unwrap();
        assert!(build_repair_block_access(&repair, &hot_output).is_err());
        assert!(!hot_output.join(ARCHIVE_V2_BLOCK_ACCESS_FILE).exists());
        fs::write(hot_output.join(COMPACTED_MARKER), &original_marker).unwrap();

        build_repair_block_access(&repair, &hot_output).unwrap();

        let after: DegradedHotMarker =
            read_json_bounded(&hot_output.join(COMPACTED_MARKER), MANIFEST_MAX_BYTES).unwrap();
        assert!(!after.canonical);
        assert!(after.block_archive_ready);
        assert!(after.block_access_ready);
        assert_eq!(after.files, degraded_hot_files(true));
        for required in repair_access_artifacts() {
            assert!(hot_output.join(required).is_file(), "{required}");
        }
        for forbidden in [
            "READY",
            "poh.wincode",
            "shredding.wincode",
            "poh/poh.wincode",
        ] {
            assert!(!hot_output.join(forbidden).exists(), "{forbidden}");
        }
        let tail = read_prev_blockhash_tail(&hot_output).unwrap().unwrap();
        assert_eq!(
            tail,
            vec![PreviousBlockhash {
                hash: hash(9),
                slot: crate::SLOTS_PER_EPOCH - 1
            }]
        );
        validate_repair_access_sidecars(
            &hot_output,
            &hot_output,
            tail[0],
            1,
            crate::SLOTS_PER_EPOCH,
            2,
        )
        .unwrap();

        let committed_marker = fs::read(hot_output.join(COMPACTED_MARKER)).unwrap();
        build_repair_block_access(&repair, &hot_output).unwrap();
        assert_eq!(
            fs::read(hot_output.join(COMPACTED_MARKER)).unwrap(),
            committed_marker,
            "a valid true-marker rerun must be a no-op"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn rejects_parent_components_in_manifest_paths() {
        let root = temp_dir("path");
        fs::create_dir_all(&root).unwrap();
        let error = resolve_relative(&root, "../escape", "test").unwrap_err();
        assert!(error.to_string().contains("simple relative path"));
        fs::remove_dir_all(&root).unwrap();
    }
}
