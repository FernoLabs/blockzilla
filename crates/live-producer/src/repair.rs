use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Component, Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use blockzilla_format::{
    LIVE_PUBKEY_RUN_RECORD_LEN, SplitCompactIndexHeader, SplitCompactIndexRecord,
    WincodeArchiveV2PohRecord, WincodeLeb128FramedReader, WincodeLeb128FramedWriter,
    live_producer::LiveBlockMissingField,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use of_car_reader::versioned_transaction::VersionedTransaction;

use crate::epoch::OLD_FAITHFUL_SLOTS_PER_EPOCH;

pub const LIVE_REPAIR_PLAN_FILE: &str = "repair/live-merge-plan.jsonl";
pub const LIVE_REPAIR_MANIFEST_FILE: &str = "repair/epoch-repair-manifest.json";
pub const LIVE_REPAIR_REQUIRED_MARKER: &str = "REPAIR-REQUIRED.json";
pub const LIVE_REPAIR_AVAILABLE_POH_FILE: &str = "repair/available-poh.wincode";
pub const LIVE_REPAIR_PRODUCED_BLOCKHASHES_FILE: &str = "repair/produced-blockhashes.bin";
pub const LIVE_REPAIR_PUBKEY_RUNS_DIR: &str = "repair/live-pubkey-runs";
const LIVE_REPAIR_PLAN_VERSION: u16 = 1;
const LIVE_REPAIR_MANIFEST_VERSION: u16 = 1;
const SMALL_FILE_DIGEST_LIMIT: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochRepairCaptureSlice {
    pub capture_dir: PathBuf,
    /// Exact number of index/PoH rows to retain from this capture. The command stops by count,
    /// then checks `max_slot` when supplied. This prevents a longer PoH tail from leaking across
    /// an already audited capture handoff.
    pub selected_blocks: u64,
    /// Inclusive source cutoff. This is useful when a resumed capture has an epoch tail or when
    /// the authoritative handoff between two overlapping captures is already known.
    pub max_slot: Option<u64>,
    /// Prebuilt, bounded pubkey runs for exactly this capture slice.
    pub pubkey_run_dir: PathBuf,
    /// Append-only capture journal for this exact source. It supplies parent_slot without forcing
    /// the preparation pass to decode hundreds of GiB of normalized block frames.
    pub journal_path: PathBuf,
    /// Immutable audited/sealed receipt for this exact capture slice.
    pub sealed_marker: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareEpochRepairConfig {
    pub captures: Vec<EpochRepairCaptureSlice>,
    pub rpc_repair_dirs: Vec<PathBuf>,
    /// Explicit completion receipt created only after all RPC workers have exited.
    pub rpc_complete_marker: PathBuf,
    pub output_dir: PathBuf,
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub expected_live_blocks: u64,
    pub expected_rpc_blocks: u64,
    pub expected_duplicate_live_blocks: Option<u64>,
    pub max_rpc_json_bytes: u64,
}

impl Default for PrepareEpochRepairConfig {
    fn default() -> Self {
        Self {
            captures: Vec::new(),
            rpc_repair_dirs: Vec::new(),
            output_dir: PathBuf::from("epoch-repair-view"),
            epoch: 0,
            slots_per_epoch: OLD_FAITHFUL_SLOTS_PER_EPOCH,
            expected_live_blocks: 0,
            expected_rpc_blocks: 0,
            expected_duplicate_live_blocks: None,
            rpc_complete_marker: PathBuf::new(),
            max_rpc_json_bytes: 32 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareEpochRepairReport {
    pub output_dir: PathBuf,
    pub manifest_path: PathBuf,
    pub merge_plan_path: PathBuf,
    pub epoch: u64,
    pub live_blocks: u64,
    pub rpc_only_blocks: u64,
    pub produced_blocks: u64,
    pub blockhash_records: u64,
    pub duplicate_live_blocks: u64,
    pub poh_records: u64,
    pub poh_entries: u64,
    pub pubkey_run_files: u64,
    pub pubkey_run_records: u64,
    pub retained_source_block_bytes: u64,
    pub first_produced_slot: Option<u64>,
    pub last_produced_slot: Option<u64>,
    pub publication_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepairManifest {
    version: u16,
    state: RepairManifestState,
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
    capture_and_completion_receipts: Vec<InputReceiptManifest>,
    block_sources: Vec<RepairSourceManifest>,
    rpc_only_slots: Vec<RpcOnlyManifest>,
    merge_plan: String,
    pubkey_runs: RepairPubkeyRunManifest,
    publication_ready: bool,
    canonical_limitations: Vec<String>,
    cleanup_scope: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RepairManifestState {
    RpcFallbackMissingPohAndShredding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NormalizedFrameManifest {
    storage: String,
    header_blockhash_ids: String,
    header_previous_blockhash_ids: String,
    canonical_produced_ids_only_in_merge_plan: bool,
    materializer_must_remap_header_ids: bool,
    current_live_finalizer_compatible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepairPubkeyRunManifest {
    path: String,
    coverage: String,
    rpc_pubkeys_not_indexed: bool,
    registry_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InputReceiptManifest {
    path: PathBuf,
    bytes: u64,
    modified_nanos: u128,
    device: Option<u64>,
    inode: Option<u64>,
    sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepairSourceManifest {
    source_id: u32,
    original_capture_dir: PathBuf,
    retained_block_path: String,
    max_slot: Option<u64>,
    selected_blocks: u64,
    contributed_blocks: u64,
    selected_block_end_offset: u64,
    retained_block_bytes: u64,
    retention: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    missing: Vec<LiveBlockMissingField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MergePlanHeader {
    kind: &'static str,
    version: u16,
    epoch: u64,
    expected_live_blocks: u64,
    expected_rpc_blocks: u64,
    expected_produced_blocks: u64,
    block_id_space: &'static str,
    live_rows_have_explicit_rpc_gaps: bool,
    sources: Vec<MergePlanSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MergePlanSource {
    source_id: u32,
    block_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MergePlanBlock {
    kind: &'static str,
    block_id: u32,
    slot: u64,
    parent_slot: u64,
    source_id: u32,
    source_block_id: u32,
    source_offset: u64,
    block_len: u32,
    tx_count: u32,
}

#[derive(Debug, Clone)]
struct RpcRepairBlock {
    slot: u64,
    parent_slot: u64,
    blockhash: String,
    blockhash_bytes: [u8; 32],
    previous_blockhash: String,
    previous_blockhash_bytes: [u8; 32],
    transactions: u64,
    path: PathBuf,
    source_fingerprint: FileFingerprint,
    sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileFingerprint {
    len: u64,
    modified_nanos: u128,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

#[derive(Debug, Clone)]
struct TrackedLink {
    source: PathBuf,
    target: PathBuf,
    fingerprint: FileFingerprint,
    sha256: Option<[u8; 32]>,
}

#[derive(Debug, Clone)]
struct TrackedFile {
    path: PathBuf,
    fingerprint: FileFingerprint,
    sha256: Option<[u8; 32]>,
}

struct Sha256Reader<R> {
    inner: R,
    hasher: Sha256,
}

impl<R> Sha256Reader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn finish(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

impl<R: Read> Read for Sha256Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        Ok(read)
    }
}

#[derive(Debug, Clone)]
struct SourceRow {
    slot: u64,
    parent_slot: u64,
    source_block_id: u32,
    block_offset: u64,
    block_len: u32,
    tx_count: u32,
    blockhash: [u8; 32],
    poh_entries: Vec<blockzilla_format::CompactPohEntry>,
}

struct CaptureCursor {
    source_id: u32,
    spec: EpochRepairCaptureSlice,
    block_path: PathBuf,
    block_file_len: u64,
    index_reader: WincodeLeb128FramedReader<BufReader<File>>,
    poh_reader: WincodeLeb128FramedReader<BufReader<File>>,
    blockhash_reader: BufReader<File>,
    journal_reader: BufReader<File>,
    next_source_block_id: u32,
    next_block_offset: u64,
    previous_slot: Option<u64>,
    current: Option<SourceRow>,
    rows_loaded: u64,
    rows_merged: u64,
    rows_contributed: u64,
    stopped: bool,
}

#[derive(Debug, Deserialize)]
struct CaptureJournalRow {
    slot: u64,
    block_id: u32,
    parent_slot: u64,
}

#[derive(Debug, Clone, Copy)]
struct ProducedTail {
    slot: u64,
    blockhash: [u8; 32],
}

impl CaptureCursor {
    fn open(
        source_id: u32,
        spec: EpochRepairCaptureSlice,
        epoch_start: u64,
        epoch_end: u64,
    ) -> Result<Self> {
        let block_path = spec.capture_dir.join("blocks/live-no-registry-blocks.bin");
        let index_path = spec.capture_dir.join("index/block-index.bin");
        let poh_path = spec.capture_dir.join("poh/poh.wincode");
        let blockhash_path = spec.capture_dir.join("index/blockhash_registry.bin");
        for path in [
            &block_path,
            &index_path,
            &poh_path,
            &blockhash_path,
            &spec.journal_path,
        ] {
            ensure_regular_nonempty_file(path)?;
        }
        let block_file_len = fs::metadata(&block_path)?.len();
        let mut index_reader = WincodeLeb128FramedReader::new(BufReader::new(
            File::open(&index_path).with_context(|| format!("open {}", index_path.display()))?,
        ));
        let header = index_reader
            .read::<SplitCompactIndexHeader>()?
            .map(|(_, header)| header)
            .context("live block index is missing its header")?;
        anyhow::ensure!(
            header.version == 1,
            "unsupported live block index version {}",
            header.version
        );

        let mut cursor = Self {
            source_id,
            spec: spec.clone(),
            block_path: block_path.clone(),
            block_file_len,
            index_reader,
            poh_reader: WincodeLeb128FramedReader::new(BufReader::new(
                File::open(&poh_path).with_context(|| format!("open {}", poh_path.display()))?,
            )),
            blockhash_reader: BufReader::new(
                File::open(&blockhash_path)
                    .with_context(|| format!("open {}", blockhash_path.display()))?,
            ),
            journal_reader: BufReader::new(
                File::open(&spec.journal_path)
                    .with_context(|| format!("open {}", spec.journal_path.display()))?,
            ),
            next_source_block_id: 0,
            next_block_offset: 0,
            previous_slot: None,
            current: None,
            rows_loaded: 0,
            rows_merged: 0,
            rows_contributed: 0,
            stopped: false,
        };
        cursor.advance(epoch_start, epoch_end)?;
        Ok(cursor)
    }

    fn advance(&mut self, epoch_start: u64, epoch_end: u64) -> Result<()> {
        self.current = None;
        if self.stopped {
            return Ok(());
        }
        if self.rows_loaded >= self.spec.selected_blocks {
            if let Some(max_slot) = self.spec.max_slot {
                anyhow::ensure!(
                    self.previous_slot == Some(max_slot),
                    "capture {} selected {} rows but ended at {:?}, expected max slot {}",
                    self.spec.capture_dir.display(),
                    self.rows_loaded,
                    self.previous_slot,
                    max_slot
                );
            }
            self.stopped = true;
            return Ok(());
        }
        loop {
            let Some((_, row)) = self.index_reader.read::<SplitCompactIndexRecord>()? else {
                self.stopped = true;
                return Ok(());
            };
            anyhow::ensure!(
                row.block_id == self.next_source_block_id,
                "capture {} index block_id {} is not expected {}",
                self.spec.capture_dir.display(),
                row.block_id,
                self.next_source_block_id
            );
            anyhow::ensure!(
                row.block_offset == self.next_block_offset,
                "capture {} block {} offset {} is not contiguous expected {}",
                self.spec.capture_dir.display(),
                row.block_id,
                row.block_offset,
                self.next_block_offset
            );
            if let Some(previous) = self.previous_slot {
                anyhow::ensure!(
                    row.slot > previous,
                    "capture {} slots are not strictly increasing: {} after {}",
                    self.spec.capture_dir.display(),
                    row.slot,
                    previous
                );
            }
            let frame_len = frame_len(row.block_len);
            self.next_block_offset = row
                .block_offset
                .checked_add(frame_len)
                .context("source block offset overflow")?;
            anyhow::ensure!(
                self.next_block_offset <= self.block_file_len,
                "capture {} index row {} ends beyond block file",
                self.spec.capture_dir.display(),
                row.block_id
            );
            self.previous_slot = Some(row.slot);
            self.next_source_block_id = self
                .next_source_block_id
                .checked_add(1)
                .context("source block id overflow")?;

            let poh = self
                .poh_reader
                .read::<WincodeArchiveV2PohRecord>()?
                .map(|(_, poh)| poh)
                .with_context(|| {
                    format!(
                        "capture {} PoH ended before index block {}",
                        self.spec.capture_dir.display(),
                        row.block_id
                    )
                })?;
            anyhow::ensure!(
                poh.block_id == row.block_id && poh.slot == row.slot,
                "capture {} PoH row ({}, {}) does not match index ({}, {})",
                self.spec.capture_dir.display(),
                poh.block_id,
                poh.slot,
                row.block_id,
                row.slot
            );
            let mut blockhash = [0u8; 32];
            self.blockhash_reader
                .read_exact(&mut blockhash)
                .with_context(|| {
                    format!(
                        "capture {} blockhash registry ended before block {}",
                        self.spec.capture_dir.display(),
                        row.block_id
                    )
                })?;
            let journal =
                read_capture_journal_row(&mut self.journal_reader)?.with_context(|| {
                    format!(
                        "capture journal {} ended before index block {}",
                        self.spec.journal_path.display(),
                        row.block_id
                    )
                })?;
            anyhow::ensure!(
                journal.block_id == row.block_id && journal.slot == row.slot,
                "capture journal {} row ({}, {}) does not match index ({}, {})",
                self.spec.journal_path.display(),
                journal.block_id,
                journal.slot,
                row.block_id,
                row.slot
            );

            if row.slot < epoch_start {
                bail!(
                    "capture {} begins before requested epoch at slot {}; use an epoch-bounded capture",
                    self.spec.capture_dir.display(),
                    row.slot
                );
            }
            anyhow::ensure!(
                row.slot <= epoch_end,
                "capture {} reached epoch tail slot {} before selected row count {}",
                self.spec.capture_dir.display(),
                row.slot,
                self.spec.selected_blocks
            );
            anyhow::ensure!(
                !self.spec.max_slot.is_some_and(|max| row.slot > max),
                "capture {} passed max slot {} before selected row count {}",
                self.spec.capture_dir.display(),
                self.spec.max_slot.unwrap(),
                self.spec.selected_blocks
            );
            self.rows_loaded = self.rows_loaded.saturating_add(1);
            self.current = Some(SourceRow {
                slot: row.slot,
                parent_slot: journal.parent_slot,
                source_block_id: row.block_id,
                block_offset: row.block_offset,
                block_len: row.block_len,
                tx_count: row.tx_count,
                blockhash,
                poh_entries: poh.entries,
            });
            return Ok(());
        }
    }
}

pub fn prepare_epoch_repair(config: PrepareEpochRepairConfig) -> Result<PrepareEpochRepairReport> {
    validate_config(&config)?;
    let mut tracked_inputs = Vec::with_capacity(config.captures.len() * 6 + 1);
    tracked_inputs.push(
        track_stable_small_file(&config.rpc_complete_marker).with_context(|| {
            format!(
                "RPC COMPLETE marker is missing, unstable, or not durable: {}",
                config.rpc_complete_marker.display()
            )
        })?,
    );
    for capture in &config.captures {
        tracked_inputs.push(
            track_stable_small_file(&capture.sealed_marker).with_context(|| {
                format!(
                    "capture audited/sealed marker is missing, unstable, or not durable: {}",
                    capture.sealed_marker.display()
                )
            })?,
        );
        for path in [
            capture
                .capture_dir
                .join("blocks/live-no-registry-blocks.bin"),
            capture.capture_dir.join("index/block-index.bin"),
            capture.capture_dir.join("poh/poh.wincode"),
            capture.capture_dir.join("index/blockhash_registry.bin"),
            capture.journal_path.clone(),
        ] {
            tracked_inputs.push(track_stable_file(&path).with_context(|| {
                format!(
                    "capture source must be stopped, immutable, and durable: {}",
                    path.display()
                )
            })?);
        }
    }
    let epoch_start = config
        .epoch
        .checked_mul(config.slots_per_epoch)
        .context("epoch start overflow")?;
    let epoch_end = epoch_start
        .checked_add(config.slots_per_epoch - 1)
        .context("epoch end overflow")?;
    let capture_and_completion_receipts = tracked_inputs.clone();
    let (rpc_blocks, rpc_inputs) = collect_rpc_blocks(&config, epoch_start, epoch_end)?;
    tracked_inputs.extend(rpc_inputs);

    let output_parent = config.output_dir.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(output_parent)
        .with_context(|| format!("create {}", output_parent.display()))?;
    anyhow::ensure!(
        !config.output_dir.exists(),
        "repair output already exists: {}",
        config.output_dir.display()
    );
    let output_name = config
        .output_dir
        .file_name()
        .and_then(|name| name.to_str())
        .context("repair output must have a UTF-8 file name")?;
    let canonical_output = fs::canonicalize(output_parent)
        .with_context(|| format!("canonicalize output parent {}", output_parent.display()))?
        .join(output_name);
    validate_output_source_separation(&canonical_output, &config)?;
    let staging = output_parent.join(format!(
        ".{output_name}.prepare-epoch-repair-{}",
        std::process::id()
    ));
    let canonical_staging = fs::canonicalize(output_parent)
        .with_context(|| format!("canonicalize output parent {}", output_parent.display()))?
        .join(
            staging
                .file_name()
                .context("repair staging has no file name")?,
        );
    validate_output_source_separation(&canonical_staging, &config)?;
    anyhow::ensure!(
        !staging.exists(),
        "stale repair staging directory exists: {}",
        staging.display()
    );
    fs::create_dir(&staging).with_context(|| format!("create {}", staging.display()))?;

    let mut tracked_links = Vec::new();
    let result = prepare_epoch_repair_staged(
        &config,
        &rpc_blocks,
        epoch_start,
        epoch_end,
        &capture_and_completion_receipts,
        &staging,
        &mut tracked_links,
    );
    match result {
        Ok(mut report) => {
            let stable = (|| -> Result<()> {
                for input in &tracked_inputs {
                    revalidate_tracked_file(input)?;
                }
                for link in &tracked_links {
                    revalidate_tracked_link(link)?;
                }
                Ok(())
            })();
            if let Err(err) = stable {
                let _ = fs::remove_dir_all(&staging);
                return Err(err.context("repair inputs changed before atomic publication"));
            }
            if let Err(err) = sync_dir(&staging) {
                let _ = fs::remove_dir_all(&staging);
                return Err(err);
            }
            if let Err(err) = fs::rename(&staging, &config.output_dir).with_context(|| {
                format!(
                    "publish repair view {} -> {}",
                    staging.display(),
                    config.output_dir.display()
                )
            }) {
                let _ = fs::remove_dir_all(&staging);
                return Err(err);
            }
            sync_dir(output_parent).with_context(|| {
                format!(
                    "repair view was renamed to {} but parent directory sync failed",
                    config.output_dir.display()
                )
            })?;
            report.output_dir = config.output_dir.clone();
            report.manifest_path = config.output_dir.join(LIVE_REPAIR_MANIFEST_FILE);
            report.merge_plan_path = config.output_dir.join(LIVE_REPAIR_PLAN_FILE);
            Ok(report)
        }
        Err(err) => {
            let cleanup = fs::remove_dir_all(&staging);
            if let Err(cleanup_err) = cleanup {
                Err(err.context(format!(
                    "also failed to remove staging directory {}: {cleanup_err}",
                    staging.display()
                )))
            } else {
                Err(err)
            }
        }
    }
}

fn prepare_epoch_repair_staged(
    config: &PrepareEpochRepairConfig,
    rpc_blocks: &BTreeMap<u64, RpcRepairBlock>,
    epoch_start: u64,
    epoch_end: u64,
    capture_and_completion_receipts: &[TrackedFile],
    staging: &Path,
    tracked_links: &mut Vec<TrackedLink>,
) -> Result<PrepareEpochRepairReport> {
    for relative in [
        "sources",
        LIVE_REPAIR_PUBKEY_RUNS_DIR,
        "repair/rpc-get-block",
    ] {
        fs::create_dir_all(staging.join(relative))?;
    }

    let mut cursors = Vec::with_capacity(config.captures.len());
    let mut plan_sources = Vec::with_capacity(config.captures.len());
    let mut source_manifests = Vec::with_capacity(config.captures.len());
    let mut retained_source_block_bytes = 0u64;
    for (source_index, spec) in config.captures.iter().cloned().enumerate() {
        let source_id = u32::try_from(source_index).context("too many repair sources")?;
        let cursor = CaptureCursor::open(source_id, spec.clone(), epoch_start, epoch_end)?;
        let relative = format!("sources/source-{source_id:03}/live-no-registry-blocks.bin");
        let retained = staging.join(&relative);
        fs::create_dir_all(retained.parent().unwrap())?;
        tracked_links.push(
            stable_hard_link(&cursor.block_path, &retained, None).with_context(|| {
                format!(
                    "retain normalized source block file {}",
                    cursor.block_path.display()
                )
            })?,
        );
        let bytes = fs::metadata(&retained)?.len();
        retained_source_block_bytes = retained_source_block_bytes.saturating_add(bytes);
        plan_sources.push(MergePlanSource {
            source_id,
            block_path: relative.clone(),
        });
        source_manifests.push(RepairSourceManifest {
            source_id,
            original_capture_dir: spec.capture_dir.clone(),
            retained_block_path: relative,
            max_slot: spec.max_slot,
            selected_blocks: 0,
            contributed_blocks: 0,
            selected_block_end_offset: 0,
            retained_block_bytes: bytes,
            retention: "hard_link".to_string(),
        });
        cursors.push(cursor);
    }

    let plan_path = staging.join(LIVE_REPAIR_PLAN_FILE);
    fs::create_dir_all(plan_path.parent().unwrap())?;
    let plan_file = File::create(&plan_path)?;
    let mut plan_writer = BufWriter::with_capacity(1024 * 1024, plan_file);
    write_json_line(
        &mut plan_writer,
        &MergePlanHeader {
            kind: "header",
            version: LIVE_REPAIR_PLAN_VERSION,
            epoch: config.epoch,
            expected_live_blocks: config.expected_live_blocks,
            expected_rpc_blocks: config.expected_rpc_blocks,
            expected_produced_blocks: config
                .expected_live_blocks
                .checked_add(config.expected_rpc_blocks)
                .context("expected produced-block count overflow")?,
            block_id_space: "produced_ordinal",
            live_rows_have_explicit_rpc_gaps: true,
            sources: plan_sources,
        },
    )?;

    let poh_path = staging.join(LIVE_REPAIR_AVAILABLE_POH_FILE);
    let mut poh_writer = WincodeLeb128FramedWriter::new(BufWriter::with_capacity(
        1024 * 1024,
        File::create(&poh_path)?,
    ));
    let blockhash_path = staging.join(LIVE_REPAIR_PRODUCED_BLOCKHASHES_FILE);
    let mut blockhash_writer =
        BufWriter::with_capacity(1024 * 1024, File::create(&blockhash_path)?);
    let (pubkey_run_files, pubkey_run_records) = retain_bounded_pubkey_runs(
        &config.captures,
        &staging.join(LIVE_REPAIR_PUBKEY_RUNS_DIR),
        tracked_links,
    )?;

    let rpc_values = rpc_blocks.values().cloned().collect::<Vec<_>>();
    let mut rpc_index = 0usize;
    let mut produced_tail = None;
    let mut first_produced_slot = None;
    let mut last_produced_slot = None;
    let mut live_blocks = 0u64;
    let mut produced_blocks = 0u64;
    let mut missing_poh_record_ids = Vec::with_capacity(rpc_blocks.len());
    let mut duplicate_live_blocks = 0u64;
    let mut poh_entries = 0u64;

    loop {
        let next_slot = cursors
            .iter()
            .filter_map(|cursor| cursor.current.as_ref().map(|row| row.slot))
            .min();
        let Some(slot) = next_slot else { break };

        while rpc_index < rpc_values.len() && rpc_values[rpc_index].slot < slot {
            validate_rpc_chain(&rpc_values[rpc_index], produced_tail)?;
            let produced_id = u32::try_from(produced_blocks)
                .context("produced block count exceeds canonical u32 id space")?;
            missing_poh_record_ids.push(produced_id);
            blockhash_writer.write_all(&rpc_values[rpc_index].blockhash_bytes)?;
            produced_blocks = produced_blocks.saturating_add(1);
            update_produced_bounds(
                rpc_values[rpc_index].slot,
                &mut first_produced_slot,
                &mut last_produced_slot,
            );
            produced_tail = Some(ProducedTail {
                slot: rpc_values[rpc_index].slot,
                blockhash: rpc_values[rpc_index].blockhash_bytes,
            });
            rpc_index += 1;
        }
        anyhow::ensure!(
            rpc_index >= rpc_values.len() || rpc_values[rpc_index].slot != slot,
            "RPC repair slot {slot} overlaps a retained live block"
        );

        let candidate_indexes = cursors
            .iter()
            .enumerate()
            .filter_map(|(index, cursor)| {
                cursor
                    .current
                    .as_ref()
                    .is_some_and(|row| row.slot == slot)
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        let winner_index = *candidate_indexes
            .first()
            .context("merge candidate disappeared")?;
        let winner_row = cursors[winner_index].current.clone().unwrap();
        for duplicate_index in candidate_indexes.iter().copied().skip(1) {
            validate_duplicate_rows(
                &winner_row,
                cursors[duplicate_index].current.as_ref().unwrap(),
                &cursors[winner_index].spec.capture_dir,
                &cursors[duplicate_index].spec.capture_dir,
            )?;
            duplicate_live_blocks = duplicate_live_blocks.saturating_add(1);
        }

        if let Some(previous) = produced_tail {
            anyhow::ensure!(
                winner_row.parent_slot == previous.slot,
                "slot {} parent {} does not follow previous produced slot {}",
                slot,
                winner_row.parent_slot,
                previous.slot
            );
        }

        let global_block_id = u32::try_from(produced_blocks)
            .context("produced block count exceeds canonical u32 id space")?;
        write_json_line(
            &mut plan_writer,
            &MergePlanBlock {
                kind: "block",
                block_id: global_block_id,
                slot,
                parent_slot: winner_row.parent_slot,
                source_id: cursors[winner_index].source_id,
                source_block_id: winner_row.source_block_id,
                source_offset: winner_row.block_offset,
                block_len: winner_row.block_len,
                tx_count: winner_row.tx_count,
            },
        )?;
        poh_writer.write(&WincodeArchiveV2PohRecord {
            block_id: global_block_id,
            slot,
            entries: winner_row.poh_entries.clone(),
        })?;
        blockhash_writer.write_all(&winner_row.blockhash)?;
        poh_entries = poh_entries.saturating_add(winner_row.poh_entries.len() as u64);
        live_blocks = live_blocks.saturating_add(1);
        produced_blocks = produced_blocks.saturating_add(1);
        update_produced_bounds(slot, &mut first_produced_slot, &mut last_produced_slot);
        produced_tail = Some(ProducedTail {
            slot,
            blockhash: winner_row.blockhash,
        });
        cursors[winner_index].rows_contributed =
            cursors[winner_index].rows_contributed.saturating_add(1);

        for index in candidate_indexes {
            cursors[index].rows_merged = cursors[index].rows_merged.saturating_add(1);
            cursors[index].advance(epoch_start, epoch_end)?;
        }
    }

    while rpc_index < rpc_values.len() {
        validate_rpc_chain(&rpc_values[rpc_index], produced_tail)?;
        let produced_id = u32::try_from(produced_blocks)
            .context("produced block count exceeds canonical u32 id space")?;
        missing_poh_record_ids.push(produced_id);
        blockhash_writer.write_all(&rpc_values[rpc_index].blockhash_bytes)?;
        produced_blocks = produced_blocks.saturating_add(1);
        update_produced_bounds(
            rpc_values[rpc_index].slot,
            &mut first_produced_slot,
            &mut last_produced_slot,
        );
        produced_tail = Some(ProducedTail {
            slot: rpc_values[rpc_index].slot,
            blockhash: rpc_values[rpc_index].blockhash_bytes,
        });
        rpc_index += 1;
    }

    anyhow::ensure!(
        live_blocks == config.expected_live_blocks,
        "merged live block count {live_blocks} does not match expected {}",
        config.expected_live_blocks
    );
    anyhow::ensure!(
        produced_blocks == live_blocks.saturating_add(rpc_blocks.len() as u64),
        "produced block id accounting mismatch"
    );
    if let Some(expected) = config.expected_duplicate_live_blocks {
        anyhow::ensure!(
            duplicate_live_blocks == expected,
            "duplicate live block count {duplicate_live_blocks} does not match expected {expected}"
        );
    }

    poh_writer.flush()?;
    let poh_writer = poh_writer.into_inner();
    poh_writer.get_ref().sync_all()?;
    blockhash_writer.flush()?;
    blockhash_writer.get_ref().sync_all()?;
    plan_writer.flush()?;
    plan_writer.get_ref().sync_all()?;
    for (manifest, cursor) in source_manifests.iter_mut().zip(&cursors) {
        anyhow::ensure!(
            cursor.rows_loaded == cursor.spec.selected_blocks,
            "capture {} loaded {} selected rows, expected {}",
            cursor.spec.capture_dir.display(),
            cursor.rows_loaded,
            cursor.spec.selected_blocks
        );
        manifest.selected_blocks = cursor.rows_loaded;
        manifest.contributed_blocks = cursor.rows_contributed;
        manifest.selected_block_end_offset = cursor.next_block_offset;
    }

    let rpc_output_dir = staging
        .join("repair/rpc-get-block")
        .join(format!("epoch-{}", config.epoch));
    fs::create_dir_all(&rpc_output_dir)?;
    let mut rpc_manifest = Vec::with_capacity(rpc_blocks.len());
    for rpc in rpc_blocks.values() {
        let file_name = format!("slot-{}.getBlock.json", rpc.slot);
        let target = rpc_output_dir.join(&file_name);
        tracked_links.push(
            stable_hard_link(
                &rpc.path,
                &target,
                Some((&rpc.source_fingerprint, Some(rpc.sha256))),
            )
            .with_context(|| format!("retain stable RPC repair {}", rpc.path.display()))?,
        );
        rpc_manifest.push(RpcOnlyManifest {
            slot: rpc.slot,
            parent_slot: rpc.parent_slot,
            blockhash: rpc.blockhash.clone(),
            previous_blockhash: rpc.previous_blockhash.clone(),
            transactions: rpc.transactions,
            source_path: format!("repair/rpc-get-block/epoch-{}/{}", config.epoch, file_name),
            source_bytes: rpc.source_fingerprint.len,
            source_sha256: hex_bytes(&rpc.sha256),
            source_modified_nanos: rpc.source_fingerprint.modified_nanos,
            source_device: fingerprint_device(&rpc.source_fingerprint),
            source_inode: fingerprint_inode(&rpc.source_fingerprint),
            state: "rpc_fallback".to_string(),
            missing: vec![
                LiveBlockMissingField::PohEntries,
                LiveBlockMissingField::Shredding,
            ],
        });
    }

    let mut canonical_limitations = vec![
        "RPC-only blocks remain exact getBlock JSON sidecars and are not injected into the normalized block stream.".to_string(),
        "RPC does not contain complete PoH entries or shred-boundary metadata for RPC-only slots.".to_string(),
        "Hard-linked normalized block frames retain source-local blockhash and previous-blockhash IDs. A future materializer must remap both header IDs to produced ordinals from live-merge-plan.jsonl.".to_string(),
        "This bundle is never valid input to the current live hot finalizer. It requires a repair-aware materializer that also inserts RPC blocks and regenerates canonical sidecars.".to_string(),
        "Reused pubkey runs cover live blocks only. RPC transaction/message keys, loaded addresses, metadata/reward keys, and return-data program IDs are not indexed and must be merged before registry construction.".to_string(),
    ];
    if duplicate_live_blocks > 0 {
        canonical_limitations.push(
            "Live block/PoH overlap is deduplicated, but reused per-source pubkey runs still include overlap counts; this can affect frequency ordering, not key coverage. Use non-overlapping audited slices for exact counts."
                .to_string(),
        );
    }
    let manifest = RepairManifest {
        version: LIVE_REPAIR_MANIFEST_VERSION,
        state: RepairManifestState::RpcFallbackMissingPohAndShredding,
        epoch: config.epoch,
        epoch_start_slot: epoch_start,
        epoch_end_slot: epoch_end,
        live_blocks,
        rpc_only_blocks: rpc_blocks.len() as u64,
        produced_blocks,
        blockhash_records: produced_blocks,
        duplicate_live_blocks,
        first_produced_slot,
        last_produced_slot,
        in_epoch_produced_parent_chain_validated: true,
        poh: RepairPohManifest {
            path: LIVE_REPAIR_AVAILABLE_POH_FILE.to_string(),
            records: live_blocks,
            entries: poh_entries,
            block_ids_rewritten: true,
            rpc_only_records_omitted: rpc_blocks.len() as u64,
            produced_id_space: produced_blocks,
            record_ids_have_explicit_rpc_gaps: true,
            missing_record_ids: missing_poh_record_ids,
        },
        normalized_frames: NormalizedFrameManifest {
            storage: "hard_linked_unmodified".to_string(),
            header_blockhash_ids: "source_local".to_string(),
            header_previous_blockhash_ids: "source_local".to_string(),
            canonical_produced_ids_only_in_merge_plan: true,
            materializer_must_remap_header_ids: true,
            current_live_finalizer_compatible: false,
        },
        capture_and_completion_receipts: capture_and_completion_receipts
            .iter()
            .map(|tracked| InputReceiptManifest {
                path: tracked.path.clone(),
                bytes: tracked.fingerprint.len,
                modified_nanos: tracked.fingerprint.modified_nanos,
                device: fingerprint_device(&tracked.fingerprint),
                inode: fingerprint_inode(&tracked.fingerprint),
                sha256: tracked.sha256.as_ref().map(|digest| hex_bytes(digest)),
            })
            .collect(),
        block_sources: source_manifests,
        rpc_only_slots: rpc_manifest,
        merge_plan: LIVE_REPAIR_PLAN_FILE.to_string(),
        pubkey_runs: RepairPubkeyRunManifest {
            path: LIVE_REPAIR_PUBKEY_RUNS_DIR.to_string(),
            coverage: "live_blocks_only".to_string(),
            rpc_pubkeys_not_indexed: true,
            registry_complete: false,
        },
        publication_ready: false,
        canonical_limitations,
        cleanup_scope: vec![
            "Normalized source block files are hard-linked into this view and survive removal of their original directory entries.".to_string(),
            "All retained live PoH records are rewritten to produced-ordinal IDs at a repair-specific path; RPC-only ordinals are explicit gaps.".to_string(),
            "Raw gRPC protobuf files, source journals, non-selected tail indexes/PoH, and shred inputs are not retained by this command; do not delete them under this command's guarantee. A hard-linked block blob may physically contain unselected tail bytes, but they are not indexed by this view.".to_string(),
        ],
    };
    let manifest_path = staging.join(LIVE_REPAIR_MANIFEST_FILE);
    write_json_atomic(&manifest_path, &manifest)?;
    // The root marker is deliberately published last inside the still-hidden staging directory.
    // No READY marker is ever created for an RPC-only epoch.
    write_json_atomic(&staging.join(LIVE_REPAIR_REQUIRED_MARKER), &manifest)?;
    for path in [
        staging.join(LIVE_REPAIR_PUBKEY_RUNS_DIR),
        rpc_output_dir,
        staging.join("repair/rpc-get-block"),
        staging.join("repair"),
        staging.join("sources"),
    ] {
        sync_dir(&path)?;
    }

    Ok(PrepareEpochRepairReport {
        output_dir: staging.to_path_buf(),
        manifest_path,
        merge_plan_path: plan_path,
        epoch: config.epoch,
        live_blocks,
        rpc_only_blocks: rpc_blocks.len() as u64,
        produced_blocks,
        blockhash_records: produced_blocks,
        duplicate_live_blocks,
        poh_records: live_blocks,
        poh_entries,
        pubkey_run_files,
        pubkey_run_records,
        retained_source_block_bytes,
        first_produced_slot,
        last_produced_slot,
        publication_ready: false,
    })
}

fn validate_config(config: &PrepareEpochRepairConfig) -> Result<()> {
    anyhow::ensure!(
        !config.captures.is_empty(),
        "at least one --capture is required"
    );
    anyhow::ensure!(
        config.slots_per_epoch > 0,
        "slots-per-epoch must be positive"
    );
    anyhow::ensure!(
        config.max_rpc_json_bytes > 0,
        "max-rpc-json-bytes must be positive"
    );
    for capture in &config.captures {
        ensure_safe_source_path(&capture.capture_dir)?;
        ensure_safe_source_path(&capture.pubkey_run_dir)?;
        ensure_safe_source_path(&capture.journal_path)?;
        ensure_safe_source_path(&capture.sealed_marker)?;
        anyhow::ensure!(
            capture.selected_blocks > 0,
            "capture {} selected-blocks must be positive",
            capture.capture_dir.display()
        );
    }
    for path in &config.rpc_repair_dirs {
        ensure_safe_source_path(path)?;
    }
    ensure_safe_source_path(&config.rpc_complete_marker)?;
    Ok(())
}

fn validate_output_source_separation(
    canonical_output: &Path,
    config: &PrepareEpochRepairConfig,
) -> Result<()> {
    let mut sources = Vec::new();
    for capture in &config.captures {
        sources.extend([
            capture.capture_dir.as_path(),
            capture.pubkey_run_dir.as_path(),
            capture.journal_path.as_path(),
            capture.sealed_marker.as_path(),
        ]);
    }
    sources.extend(config.rpc_repair_dirs.iter().map(PathBuf::as_path));
    sources.push(config.rpc_complete_marker.as_path());
    for source in sources {
        let source = fs::canonicalize(source)
            .with_context(|| format!("canonicalize repair input {}", source.display()))?;
        anyhow::ensure!(
            !canonical_output.starts_with(&source) && !source.starts_with(canonical_output),
            "repair output {} must neither contain nor be nested in input {}",
            canonical_output.display(),
            source.display()
        );
    }
    Ok(())
}

fn collect_rpc_blocks(
    config: &PrepareEpochRepairConfig,
    epoch_start: u64,
    epoch_end: u64,
) -> Result<(BTreeMap<u64, RpcRepairBlock>, Vec<TrackedFile>)> {
    let mut blocks = BTreeMap::new();
    let mut tracked_inputs = Vec::new();
    for dir in &config.rpc_repair_dirs {
        anyhow::ensure!(
            dir.is_dir(),
            "RPC repair directory is missing: {}",
            dir.display()
        );
        for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(slot_text) = name
                .strip_prefix("slot-")
                .and_then(|value| value.strip_suffix(".getBlock.json"))
            else {
                continue;
            };
            let slot = slot_text
                .parse::<u64>()
                .with_context(|| format!("parse RPC repair file name {name}"))?;
            anyhow::ensure!(
                (epoch_start..=epoch_end).contains(&slot),
                "RPC repair slot {slot} is outside epoch {}",
                config.epoch
            );
            let path = entry.path();
            let block = read_rpc_block(&path, slot, config.max_rpc_json_bytes)?;
            tracked_inputs.push(TrackedFile {
                path: block.path.clone(),
                fingerprint: block.source_fingerprint.clone(),
                sha256: Some(block.sha256),
            });
            if let Some(previous) = blocks.insert(slot, block.clone()) {
                anyhow::ensure!(
                    rpc_blocks_equivalent(&previous, &block),
                    "conflicting RPC repair files for slot {slot}: {} and {}",
                    previous.path.display(),
                    block.path.display()
                );
            }
        }
    }
    anyhow::ensure!(
        blocks.len() as u64 == config.expected_rpc_blocks,
        "RPC repair block count {} does not match expected {}",
        blocks.len(),
        config.expected_rpc_blocks
    );
    Ok((blocks, tracked_inputs))
}

fn retain_bounded_pubkey_runs(
    captures: &[EpochRepairCaptureSlice],
    output_dir: &Path,
    tracked_links: &mut Vec<TrackedLink>,
) -> Result<(u64, u64)> {
    fs::create_dir_all(output_dir).with_context(|| format!("create {}", output_dir.display()))?;
    let mut files = 0u64;
    let mut records = 0u64;
    for (source_index, capture) in captures.iter().enumerate() {
        anyhow::ensure!(
            capture.pubkey_run_dir.is_dir(),
            "bounded pubkey-run directory is missing: {}",
            capture.pubkey_run_dir.display()
        );
        let mut source_files = 0u64;
        for entry in fs::read_dir(&capture.pubkey_run_dir)
            .with_context(|| format!("read {}", capture.pubkey_run_dir.display()))?
        {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.file_type().is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !(name == "hot-run.bin" || (name.starts_with("run-") && name.ends_with(".bin"))) {
                continue;
            }
            anyhow::ensure!(
                metadata.len() > 0 && metadata.len() % LIVE_PUBKEY_RUN_RECORD_LEN as u64 == 0,
                "pubkey run {} has invalid byte length {}",
                entry.path().display(),
                metadata.len()
            );
            let stem = name.strip_suffix(".bin").unwrap();
            let target = output_dir.join(format!("run-source-{source_index:03}-{stem}.bin"));
            tracked_links.push(stable_hard_link(&entry.path(), &target, None).with_context(
                || format!("retain bounded pubkey run {}", entry.path().display()),
            )?);
            files = files.saturating_add(1);
            source_files = source_files.saturating_add(1);
            records = records.saturating_add(metadata.len() / LIVE_PUBKEY_RUN_RECORD_LEN as u64);
        }
        anyhow::ensure!(
            source_files > 0,
            "bounded pubkey-run directory has no non-empty run files: {}",
            capture.pubkey_run_dir.display()
        );
    }
    Ok((files, records))
}

fn read_rpc_block(path: &Path, slot: u64, max_bytes: u64) -> Result<RpcRepairBlock> {
    ensure_regular_nonempty_file(path)?;
    sync_file(path)?;
    let before = fingerprint_path(path)?;
    let bytes = before.len;
    anyhow::ensure!(
        bytes <= max_bytes,
        "RPC repair {} is {} bytes, exceeding configured maximum {}",
        path.display(),
        bytes,
        max_bytes
    );
    let file = File::open(path).with_context(|| format!("open RPC repair {}", path.display()))?;
    let mut reader = Sha256Reader::new(BufReader::new(file));
    let value: Value = serde_json::from_reader(&mut reader)
        .with_context(|| format!("decode RPC repair {}", path.display()))?;
    let sha256 = reader.finish();
    sync_file(path)?;
    let after = fingerprint_path(path)?;
    anyhow::ensure!(
        before == after,
        "RPC repair source changed while parsing: {}",
        path.display()
    );
    let object = value
        .as_object()
        .with_context(|| format!("RPC repair {} is not a JSON object", path.display()))?;
    let parent_slot = object
        .get("parentSlot")
        .and_then(Value::as_u64)
        .with_context(|| format!("RPC repair slot {slot} has no parentSlot"))?;
    let blockhash = object
        .get("blockhash")
        .and_then(Value::as_str)
        .with_context(|| format!("RPC repair slot {slot} has no blockhash"))?
        .to_string();
    let previous_blockhash = object
        .get("previousBlockhash")
        .and_then(Value::as_str)
        .with_context(|| format!("RPC repair slot {slot} has no previousBlockhash"))?
        .to_string();
    let transactions = object
        .get("transactions")
        .and_then(Value::as_array)
        .with_context(|| format!("RPC repair slot {slot} has no transactions array"))?;
    for (tx_index, transaction) in transactions.iter().enumerate() {
        let transaction = transaction.as_object().with_context(|| {
            format!("RPC repair slot {slot} transaction {tx_index} is not an object")
        })?;
        let encoded = transaction
            .get("transaction")
            .and_then(Value::as_array)
            .with_context(|| {
                format!("RPC repair slot {slot} transaction {tx_index} has no base64 tuple")
            })?;
        let payload = encoded.first().and_then(Value::as_str);
        anyhow::ensure!(
            encoded.len() == 2
                && payload.is_some()
                && encoded.get(1).and_then(Value::as_str) == Some("base64"),
            "RPC repair slot {slot} transaction {tx_index} is not [payload, \"base64\"]"
        );
        let payload = payload.unwrap();
        let decoded = BASE64_STANDARD.decode(payload).with_context(|| {
            format!("RPC repair slot {slot} transaction {tx_index} has invalid base64")
        })?;
        anyhow::ensure!(
            BASE64_STANDARD.encode(&decoded) == payload,
            "RPC repair slot {slot} transaction {tx_index} base64 is not canonical"
        );
        wincode::deserialize::<VersionedTransaction<'_>>(&decoded).with_context(|| {
            format!(
                "RPC repair slot {slot} transaction {tx_index} has invalid Solana wire transaction"
            )
        })?;
        anyhow::ensure!(
            transaction.contains_key("meta"),
            "RPC repair slot {slot} transaction {tx_index} has no meta field"
        );
    }
    Ok(RpcRepairBlock {
        slot,
        parent_slot,
        blockhash_bytes: decode_base58_32(&blockhash, "blockhash")?,
        previous_blockhash_bytes: decode_base58_32(&previous_blockhash, "previousBlockhash")?,
        blockhash,
        previous_blockhash,
        transactions: transactions.len() as u64,
        path: path.to_path_buf(),
        source_fingerprint: after,
        sha256,
    })
}

fn validate_rpc_chain(rpc: &RpcRepairBlock, previous: Option<ProducedTail>) -> Result<()> {
    if let Some(previous) = previous {
        anyhow::ensure!(
            rpc.parent_slot == previous.slot,
            "RPC slot {} parent {} does not follow previous produced slot {}",
            rpc.slot,
            rpc.parent_slot,
            previous.slot
        );
        anyhow::ensure!(
            rpc.previous_blockhash_bytes == previous.blockhash,
            "RPC slot {} previousBlockhash does not match slot {} blockhash",
            rpc.slot,
            previous.slot
        );
    }
    Ok(())
}

fn validate_duplicate_rows(
    left: &SourceRow,
    right: &SourceRow,
    left_source: &Path,
    right_source: &Path,
) -> Result<()> {
    anyhow::ensure!(left.slot == right.slot, "internal duplicate slot mismatch");
    anyhow::ensure!(
        left.parent_slot == right.parent_slot,
        "duplicate slot {} has parent slots {} and {}",
        left.slot,
        left.parent_slot,
        right.parent_slot
    );
    anyhow::ensure!(
        left.blockhash == right.blockhash,
        "duplicate slot {} has conflicting blockhashes in {} and {}",
        left.slot,
        left_source.display(),
        right_source.display()
    );
    anyhow::ensure!(
        left.tx_count == right.tx_count,
        "duplicate slot {} has transaction counts {} and {}",
        left.slot,
        left.tx_count,
        right.tx_count
    );
    anyhow::ensure!(
        poh_entries_equal(&left.poh_entries, &right.poh_entries),
        "duplicate slot {} has conflicting PoH entries in {} and {}",
        left.slot,
        left_source.display(),
        right_source.display()
    );
    Ok(())
}

fn read_capture_journal_row(reader: &mut impl BufRead) -> Result<Option<CaptureJournalRow>> {
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        if line.trim().is_empty() {
            continue;
        }
        return serde_json::from_str(&line)
            .context("decode capture journal row")
            .map(Some);
    }
}

fn poh_entries_equal(
    left: &[blockzilla_format::CompactPohEntry],
    right: &[blockzilla_format::CompactPohEntry],
) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.num_hashes == right.num_hashes
                && left.hash == right.hash
                && left.tx_count == right.tx_count
        })
}

fn rpc_blocks_equivalent(left: &RpcRepairBlock, right: &RpcRepairBlock) -> bool {
    left.slot == right.slot && left.sha256 == right.sha256
}

fn decode_base58_32(value: &str, field: &str) -> Result<[u8; 32]> {
    let mut bytes = [0u8; 32];
    five8::decode_32(value, &mut bytes)
        .map_err(|err| anyhow!("decode {field} {value}: {err:?}"))?;
    Ok(bytes)
}

fn update_produced_bounds(slot: u64, first: &mut Option<u64>, last: &mut Option<u64>) {
    first.get_or_insert(slot);
    *last = Some(slot);
}

fn frame_len(payload_len: u32) -> u64 {
    u64::from(payload_len) + u64::from(varint_len(payload_len))
}

fn varint_len(mut value: u32) -> u8 {
    let mut len = 1u8;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn ensure_regular_nonempty_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("stat required file {}", path.display()))?;
    anyhow::ensure!(
        metadata.file_type().is_file() && metadata.len() > 0,
        "required file is not a non-empty regular file: {}",
        path.display()
    );
    Ok(())
}

fn sync_file(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync {}", path.display()))
}

fn fingerprint_path(path: &Path) -> Result<FileFingerprint> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("stat stable input {}", path.display()))?;
    anyhow::ensure!(
        metadata.file_type().is_file() && metadata.len() > 0,
        "stable input is not a non-empty regular file: {}",
        path.display()
    );
    let modified_nanos = metadata
        .modified()
        .with_context(|| format!("read mtime for {}", path.display()))?
        .duration_since(std::time::UNIX_EPOCH)
        .with_context(|| format!("mtime for {} predates UNIX epoch", path.display()))?
        .as_nanos();
    Ok(FileFingerprint {
        len: metadata.len(),
        modified_nanos,
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
    })
}

#[cfg(unix)]
fn fingerprint_device(fingerprint: &FileFingerprint) -> Option<u64> {
    Some(fingerprint.device)
}

#[cfg(not(unix))]
fn fingerprint_device(_fingerprint: &FileFingerprint) -> Option<u64> {
    None
}

#[cfg(unix)]
fn fingerprint_inode(fingerprint: &FileFingerprint) -> Option<u64> {
    Some(fingerprint.inode)
}

#[cfg(not(unix))]
fn fingerprint_inode(_fingerprint: &FileFingerprint) -> Option<u64> {
    None
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("write to String");
    }
    encoded
}

fn sha256_file(path: &Path) -> Result<[u8; 32]> {
    let file = File::open(path).with_context(|| format!("open {} for digest", path.display()))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

fn stable_hard_link(
    source: &Path,
    target: &Path,
    expected: Option<(&FileFingerprint, Option<[u8; 32]>)>,
) -> Result<TrackedLink> {
    ensure_regular_nonempty_file(source)?;
    sync_file(source)?;
    let before = fingerprint_path(source)?;
    if let Some((expected_fingerprint, _)) = expected {
        anyhow::ensure!(
            &before == expected_fingerprint,
            "source changed before hard-link: {}",
            source.display()
        );
    }
    let expected_sha = expected.and_then(|(_, sha)| sha);
    let sha256 = if let Some(expected_sha) = expected_sha {
        let actual = sha256_file(source)?;
        anyhow::ensure!(
            actual == expected_sha,
            "source digest changed before hard-link: {}",
            source.display()
        );
        Some(actual)
    } else if before.len <= SMALL_FILE_DIGEST_LIMIT {
        Some(sha256_file(source)?)
    } else {
        None
    };
    anyhow::ensure!(
        fingerprint_path(source)? == before,
        "source changed while preparing hard-link: {}",
        source.display()
    );
    fs::hard_link(source, target).with_context(|| {
        format!(
            "hard-link {} to {}; repair view must be on the same filesystem",
            source.display(),
            target.display()
        )
    })?;
    // The target is the same inode. Sync it after publication, then require both directory names
    // to still resolve to the exact metadata snapshot observed before the link.
    sync_file(target)?;
    let after_source = fingerprint_path(source)?;
    let after_target = fingerprint_path(target)?;
    anyhow::ensure!(
        after_source == before && after_target == before,
        "source or hard-link target changed during publication: {} -> {}",
        source.display(),
        target.display()
    );
    sync_dir(target.parent().context("hard-link target has no parent")?)?;
    Ok(TrackedLink {
        source: source.to_path_buf(),
        target: target.to_path_buf(),
        fingerprint: before,
        sha256,
    })
}

fn track_stable_file(path: &Path) -> Result<TrackedFile> {
    ensure_regular_nonempty_file(path)?;
    sync_file(path)?;
    let before = fingerprint_path(path)?;
    let sha256 = (before.len <= SMALL_FILE_DIGEST_LIMIT)
        .then(|| sha256_file(path))
        .transpose()?;
    sync_file(path)?;
    anyhow::ensure!(
        fingerprint_path(path)? == before,
        "tracked input changed while reading: {}",
        path.display()
    );
    Ok(TrackedFile {
        path: path.to_path_buf(),
        fingerprint: before,
        sha256,
    })
}

fn track_stable_small_file(path: &Path) -> Result<TrackedFile> {
    let tracked = track_stable_file(path)?;
    anyhow::ensure!(
        tracked.sha256.is_some(),
        "receipt/marker {} is unexpectedly large: {} bytes",
        path.display(),
        tracked.fingerprint.len
    );
    Ok(tracked)
}

fn revalidate_tracked_file(tracked: &TrackedFile) -> Result<()> {
    sync_file(&tracked.path)?;
    anyhow::ensure!(
        fingerprint_path(&tracked.path)? == tracked.fingerprint,
        "tracked input changed: {}",
        tracked.path.display()
    );
    if let Some(expected) = tracked.sha256 {
        anyhow::ensure!(
            sha256_file(&tracked.path)? == expected,
            "tracked input digest changed: {}",
            tracked.path.display()
        );
    }
    Ok(())
}

fn revalidate_tracked_link(tracked: &TrackedLink) -> Result<()> {
    sync_file(&tracked.source)?;
    sync_file(&tracked.target)?;
    anyhow::ensure!(
        fingerprint_path(&tracked.source)? == tracked.fingerprint,
        "hard-link source changed before final publication: {}",
        tracked.source.display()
    );
    anyhow::ensure!(
        fingerprint_path(&tracked.target)? == tracked.fingerprint,
        "hard-link target changed before final publication: {}",
        tracked.target.display()
    );
    if let Some(expected) = tracked.sha256 {
        anyhow::ensure!(
            sha256_file(&tracked.source)? == expected,
            "hard-link source digest changed before final publication: {}",
            tracked.source.display()
        );
    }
    Ok(())
}

fn ensure_safe_source_path(path: &Path) -> Result<()> {
    anyhow::ensure!(
        !path
            .components()
            .any(|part| matches!(part, Component::ParentDir)),
        "source path must not contain '..': {}",
        path.display()
    );
    Ok(())
}

fn write_json_line(writer: &mut impl Write, value: &impl Serialize) -> Result<()> {
    serde_json::to_writer(&mut *writer, value)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    let file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(&tmp, path)
        .with_context(|| format!("publish {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

/// Read only the header of a merge plan. This is intentionally small and public so a future hot
/// finalizer can reject incompatible plans before opening any very large source block file.
pub fn read_live_repair_plan_header(path: &Path) -> Result<(u16, u64, u64)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut line = String::new();
    BufReader::new(file).read_line(&mut line)?;
    let value: Value = serde_json::from_str(&line).context("decode live repair plan header")?;
    let version = value
        .get("version")
        .and_then(Value::as_u64)
        .context("repair plan header has no version")?;
    let epoch = value
        .get("epoch")
        .and_then(Value::as_u64)
        .context("repair plan header has no epoch")?;
    let blocks = value
        .get("expected_live_blocks")
        .and_then(Value::as_u64)
        .context("repair plan header has no expected_live_blocks")?;
    Ok((u16::try_from(version)?, epoch, blocks))
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{
        CompactBlockHeader, CompactPohEntry, WincodeArchiveV2NoRegistryBlock,
        WincodeArchiveV2NoRegistryBlockHeader, WincodeArchiveV2NoRegistryRewards,
        WincodeLeb128FramedWriter, wincode_leb128_config, write_u32_varint,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-live-repair-{label}-{}-{stamp}",
            std::process::id()
        ))
    }

    fn blockhash(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    fn base58(bytes: [u8; 32]) -> String {
        let mut out = [0u8; 44];
        let len = five8::encode_32(&bytes, &mut out) as usize;
        std::str::from_utf8(&out[..len]).unwrap().to_string()
    }

    fn write_capture(root: &Path, slots: &[(u64, u64, u8)]) {
        fs::create_dir_all(root.join("blocks")).unwrap();
        fs::create_dir_all(root.join("index")).unwrap();
        fs::create_dir_all(root.join("index/pubkey-runs")).unwrap();
        fs::create_dir_all(root.join("poh")).unwrap();
        fs::create_dir_all(root.join("journal")).unwrap();
        let mut blocks =
            BufWriter::new(File::create(root.join("blocks/live-no-registry-blocks.bin")).unwrap());
        let mut index = WincodeLeb128FramedWriter::new(BufWriter::new(
            File::create(root.join("index/block-index.bin")).unwrap(),
        ));
        index
            .write(&SplitCompactIndexHeader { version: 1 })
            .unwrap();
        let mut poh = WincodeLeb128FramedWriter::new(BufWriter::new(
            File::create(root.join("poh/poh.wincode")).unwrap(),
        ));
        let mut hashes =
            BufWriter::new(File::create(root.join("index/blockhash_registry.bin")).unwrap());
        let mut journal =
            BufWriter::new(File::create(root.join("journal/grpc-blocks.jsonl")).unwrap());
        let mut offset = 0u64;
        for (id, (slot, parent, hash_byte)) in slots.iter().copied().enumerate() {
            let block = WincodeArchiveV2NoRegistryBlock {
                header: WincodeArchiveV2NoRegistryBlockHeader {
                    compact: CompactBlockHeader {
                        slot,
                        parent_slot: parent,
                        blockhash: id as u32,
                        previous_blockhash: (id as u32).saturating_sub(1),
                        block_time: None,
                        block_height: None,
                        shredding: Vec::new(),
                        poh_entries: Vec::new(),
                        rewards: None,
                    },
                    rewards: None::<WincodeArchiveV2NoRegistryRewards>,
                },
                txs: Vec::new(),
            };
            let payload = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
            write_u32_varint(&mut blocks, payload.len() as u32).unwrap();
            blocks.write_all(&payload).unwrap();
            index
                .write(&SplitCompactIndexRecord {
                    slot,
                    block_id: id as u32,
                    block_offset: offset,
                    block_len: payload.len() as u32,
                    runtime_offset: 0,
                    runtime_len: 0,
                    tx_count: 0,
                })
                .unwrap();
            offset += frame_len(payload.len() as u32);
            poh.write(&WincodeArchiveV2PohRecord {
                block_id: id as u32,
                slot,
                entries: vec![CompactPohEntry {
                    num_hashes: 1,
                    hash: blockhash(hash_byte),
                    tx_count: 0,
                }],
            })
            .unwrap();
            hashes.write_all(&blockhash(hash_byte)).unwrap();
            serde_json::to_writer(
                &mut journal,
                &serde_json::json!({
                    "slot": slot,
                    "epoch": slot / 10,
                    "epoch_slot_index": slot % 10,
                    "block_id": id,
                    "parent_slot": parent,
                    "transactions": 0,
                    "entries": 1,
                    "executed_transaction_count": 0,
                    "entries_count": 1,
                    "updated_account_count": 0,
                    "missing": []
                }),
            )
            .unwrap();
            journal.write_all(b"\n").unwrap();
        }
        blocks.flush().unwrap();
        index.flush().unwrap();
        poh.flush().unwrap();
        hashes.flush().unwrap();
        journal.flush().unwrap();
        let mut run = Vec::with_capacity(LIVE_PUBKEY_RUN_RECORD_LEN);
        run.extend_from_slice(&[42u8; 32]);
        run.extend_from_slice(&1u32.to_le_bytes());
        fs::write(root.join("index/pubkey-runs/run-000000.bin"), run).unwrap();
        fs::write(root.join("CAPTURE-SEALED.json"), b"{\"sealed\":true}\n").unwrap();
    }

    fn write_rpc(dir: &Path, slot: u64, parent: u64, hash: u8, previous: u8) {
        write_rpc_payload(dir, slot, parent, hash, previous, None, false);
    }

    fn valid_wire_transaction(signature_byte: u8) -> String {
        let mut bytes = Vec::new();
        bytes.push(1); // one signature, ShortU16
        bytes.extend_from_slice(&[signature_byte; 64]);
        bytes.extend_from_slice(&[1, 0, 0]); // legacy message header
        bytes.push(1); // one account key, ShortU16
        bytes.extend_from_slice(&[7; 32]);
        bytes.extend_from_slice(&[8; 32]); // recent blockhash
        bytes.push(0); // no instructions, ShortU16
        BASE64_STANDARD.encode(bytes)
    }

    fn write_rpc_payload(
        dir: &Path,
        slot: u64,
        parent: u64,
        hash: u8,
        previous: u8,
        payload: Option<&str>,
        pretty: bool,
    ) {
        fs::create_dir_all(dir).unwrap();
        let path = dir.join(format!("slot-{slot}.getBlock.json"));
        let transactions = payload.map_or_else(Vec::new, |payload| {
            vec![serde_json::json!({
                "transaction": [payload, "base64"],
                "meta": null
            })]
        });
        let value = serde_json::json!({
            "blockhash": base58(blockhash(hash)),
            "previousBlockhash": base58(blockhash(previous)),
            "parentSlot": parent,
            "blockTime": null,
            "blockHeight": null,
            "transactions": transactions
        });
        let bytes = if pretty {
            serde_json::to_vec_pretty(&value).unwrap()
        } else {
            serde_json::to_vec(&value).unwrap()
        };
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn atomic_repair_view_merges_captures_and_rewrites_poh_ids() {
        let root = temp_dir("success");
        let early = root.join("early");
        let late = root.join("late");
        let rpc = root.join("rpc");
        let output = root.join("out");
        write_capture(&early, &[(10, 9, 10), (11, 10, 11)]);
        write_capture(&late, &[(13, 12, 13)]);
        write_rpc(&rpc, 12, 11, 12, 11);
        let rpc_complete = root.join("RPC-COMPLETE.json");
        fs::write(&rpc_complete, b"{\"complete\":true}\n").unwrap();

        let report = prepare_epoch_repair(PrepareEpochRepairConfig {
            captures: vec![
                EpochRepairCaptureSlice {
                    pubkey_run_dir: early.join("index/pubkey-runs"),
                    journal_path: early.join("journal/grpc-blocks.jsonl"),
                    capture_dir: early.clone(),
                    selected_blocks: 2,
                    max_slot: None,
                    sealed_marker: early.join("CAPTURE-SEALED.json"),
                },
                EpochRepairCaptureSlice {
                    pubkey_run_dir: late.join("index/pubkey-runs"),
                    journal_path: late.join("journal/grpc-blocks.jsonl"),
                    capture_dir: late.clone(),
                    selected_blocks: 1,
                    max_slot: None,
                    sealed_marker: late.join("CAPTURE-SEALED.json"),
                },
            ],
            rpc_repair_dirs: vec![rpc],
            rpc_complete_marker: rpc_complete,
            output_dir: output.clone(),
            epoch: 1,
            slots_per_epoch: 10,
            expected_live_blocks: 3,
            expected_rpc_blocks: 1,
            expected_duplicate_live_blocks: Some(0),
            max_rpc_json_bytes: 1024 * 1024,
        })
        .unwrap();

        assert_eq!(report.live_blocks, 3);
        assert_eq!(report.rpc_only_blocks, 1);
        assert_eq!(report.produced_blocks, 4);
        assert_eq!(report.blockhash_records, 4);
        assert!(!report.publication_ready);
        assert!(output.join(LIVE_REPAIR_REQUIRED_MARKER).is_file());
        assert!(!output.join("READY").exists());
        assert!(!output.join("poh/poh.wincode").exists());
        assert!(!output.join("index/blockhash_registry.bin").exists());
        assert!(!output.join("index/pubkey-runs").exists());
        assert!(output.join(LIVE_REPAIR_PUBKEY_RUNS_DIR).is_dir());
        let mut reader = WincodeLeb128FramedReader::new(BufReader::new(
            File::open(output.join(LIVE_REPAIR_AVAILABLE_POH_FILE)).unwrap(),
        ));
        let mut rows = Vec::new();
        while let Some((_, row)) = reader.read::<WincodeArchiveV2PohRecord>().unwrap() {
            rows.push((row.block_id, row.slot));
        }
        assert_eq!(rows, vec![(0, 10), (1, 11), (3, 13)]);
        let hashes = fs::read(output.join(LIVE_REPAIR_PRODUCED_BLOCKHASHES_FILE)).unwrap();
        assert_eq!(hashes.len(), 4 * 32);
        assert_eq!(&hashes[0..32], &blockhash(10));
        assert_eq!(&hashes[32..64], &blockhash(11));
        assert_eq!(&hashes[64..96], &blockhash(12));
        assert_eq!(&hashes[96..128], &blockhash(13));
        let manifest: Value =
            serde_json::from_slice(&fs::read(output.join(LIVE_REPAIR_MANIFEST_FILE)).unwrap())
                .unwrap();
        assert_eq!(
            manifest["normalized_frames"]["current_live_finalizer_compatible"],
            false
        );
        assert_eq!(manifest["pubkey_runs"]["rpc_pubkeys_not_indexed"], true);
        assert_eq!(
            manifest["poh"]["missing_record_ids"],
            serde_json::json!([2])
        );
        assert_eq!(
            manifest["rpc_only_slots"][0]["source_sha256"]
                .as_str()
                .unwrap()
                .len(),
            64
        );
        let (version, epoch, blocks) =
            read_live_repair_plan_header(&output.join(LIVE_REPAIR_PLAN_FILE)).unwrap();
        assert_eq!((version, epoch, blocks), (1, 1, 3));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn count_mismatch_removes_staging_and_never_publishes_output() {
        let root = temp_dir("rollback");
        let capture = root.join("capture");
        let output = root.join("out");
        write_capture(&capture, &[(10, 9, 10)]);
        let rpc_complete = root.join("RPC-COMPLETE.json");
        fs::write(&rpc_complete, b"{\"complete\":true}\n").unwrap();
        let err = prepare_epoch_repair(PrepareEpochRepairConfig {
            captures: vec![EpochRepairCaptureSlice {
                pubkey_run_dir: capture.join("index/pubkey-runs"),
                journal_path: capture.join("journal/grpc-blocks.jsonl"),
                capture_dir: capture.clone(),
                selected_blocks: 1,
                max_slot: None,
                sealed_marker: capture.join("CAPTURE-SEALED.json"),
            }],
            rpc_repair_dirs: Vec::new(),
            rpc_complete_marker: rpc_complete,
            output_dir: output.clone(),
            epoch: 1,
            slots_per_epoch: 10,
            expected_live_blocks: 2,
            expected_rpc_blocks: 0,
            expected_duplicate_live_blocks: Some(0),
            max_rpc_json_bytes: 1024 * 1024,
        })
        .unwrap_err();
        assert!(err.to_string().contains("does not match expected"));
        assert!(!output.exists());
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("prepare-epoch-repair")
        }));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn conflicting_overlap_fails_closed() {
        let root = temp_dir("conflict");
        let left = root.join("left");
        let right = root.join("right");
        let output = root.join("out");
        write_capture(&left, &[(10, 9, 10)]);
        write_capture(&right, &[(10, 9, 99)]);
        let rpc_complete = root.join("RPC-COMPLETE.json");
        fs::write(&rpc_complete, b"{\"complete\":true}\n").unwrap();
        let err = prepare_epoch_repair(PrepareEpochRepairConfig {
            captures: vec![
                EpochRepairCaptureSlice {
                    pubkey_run_dir: left.join("index/pubkey-runs"),
                    journal_path: left.join("journal/grpc-blocks.jsonl"),
                    capture_dir: left.clone(),
                    selected_blocks: 1,
                    max_slot: None,
                    sealed_marker: left.join("CAPTURE-SEALED.json"),
                },
                EpochRepairCaptureSlice {
                    pubkey_run_dir: right.join("index/pubkey-runs"),
                    journal_path: right.join("journal/grpc-blocks.jsonl"),
                    capture_dir: right.clone(),
                    selected_blocks: 1,
                    max_slot: None,
                    sealed_marker: right.join("CAPTURE-SEALED.json"),
                },
            ],
            rpc_repair_dirs: Vec::new(),
            rpc_complete_marker: rpc_complete,
            output_dir: output.clone(),
            epoch: 1,
            slots_per_epoch: 10,
            expected_live_blocks: 1,
            expected_rpc_blocks: 0,
            expected_duplicate_live_blocks: Some(1),
            max_rpc_json_bytes: 1024 * 1024,
        })
        .unwrap_err();
        assert!(err.to_string().contains("conflicting blockhashes"));
        assert!(!output.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn compatible_overlap_is_deduplicated_once() {
        let root = temp_dir("dedup");
        let left = root.join("left");
        let right = root.join("right");
        let output = root.join("out");
        write_capture(&left, &[(10, 9, 10), (11, 10, 11)]);
        write_capture(&right, &[(11, 10, 11), (12, 11, 12)]);
        let rpc_complete = root.join("RPC-COMPLETE.json");
        fs::write(&rpc_complete, b"{\"complete\":true}\n").unwrap();
        let report = prepare_epoch_repair(PrepareEpochRepairConfig {
            captures: vec![
                EpochRepairCaptureSlice {
                    pubkey_run_dir: left.join("index/pubkey-runs"),
                    journal_path: left.join("journal/grpc-blocks.jsonl"),
                    capture_dir: left.clone(),
                    selected_blocks: 2,
                    max_slot: None,
                    sealed_marker: left.join("CAPTURE-SEALED.json"),
                },
                EpochRepairCaptureSlice {
                    pubkey_run_dir: right.join("index/pubkey-runs"),
                    journal_path: right.join("journal/grpc-blocks.jsonl"),
                    capture_dir: right.clone(),
                    selected_blocks: 2,
                    max_slot: None,
                    sealed_marker: right.join("CAPTURE-SEALED.json"),
                },
            ],
            rpc_repair_dirs: Vec::new(),
            rpc_complete_marker: rpc_complete,
            output_dir: output.clone(),
            epoch: 1,
            slots_per_epoch: 10,
            expected_live_blocks: 3,
            expected_rpc_blocks: 0,
            expected_duplicate_live_blocks: Some(1),
            max_rpc_json_bytes: 1024 * 1024,
        })
        .unwrap();
        assert_eq!(report.live_blocks, 3);
        assert_eq!(report.duplicate_live_blocks, 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rpc_transaction_rejects_invalid_base64() {
        let root = temp_dir("invalid-base64");
        write_rpc_payload(&root, 12, 11, 12, 11, Some("%%%not-base64%%%"), false);
        let error =
            read_rpc_block(&root.join("slot-12.getBlock.json"), 12, 1024 * 1024).unwrap_err();
        assert!(error.to_string().contains("invalid base64"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn duplicate_rpc_payloads_must_be_byte_identical() {
        let root = temp_dir("rpc-duplicate");
        let left = root.join("left");
        let right = root.join("right");
        let left_tx = valid_wire_transaction(1);
        let right_tx = valid_wire_transaction(2);
        write_rpc_payload(&left, 12, 11, 12, 11, Some(&left_tx), false);
        write_rpc_payload(&right, 12, 11, 12, 11, Some(&right_tx), false);
        let config = PrepareEpochRepairConfig {
            rpc_repair_dirs: vec![left, right],
            expected_rpc_blocks: 1,
            max_rpc_json_bytes: 1024 * 1024,
            ..PrepareEpochRepairConfig::default()
        };
        let error = collect_rpc_blocks(&config, 10, 19).unwrap_err();
        assert!(error.to_string().contains("conflicting RPC repair files"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rpc_source_change_after_parse_blocks_hard_link() {
        let root = temp_dir("rpc-unstable");
        let rpc = root.join("rpc");
        let target = root.join("retained.json");
        write_rpc(&rpc, 12, 11, 12, 11);
        let source = rpc.join("slot-12.getBlock.json");
        let parsed = read_rpc_block(&source, 12, 1024 * 1024).unwrap();
        fs::write(&source, b"{\"changed\":true}\n").unwrap();
        let error = stable_hard_link(
            &source,
            &target,
            Some((&parsed.source_fingerprint, Some(parsed.sha256))),
        )
        .unwrap_err();
        assert!(error.to_string().contains("changed before hard-link"));
        assert!(!target.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tracked_input_digest_detects_same_length_mutation() {
        let root = temp_dir("tracked-input-unstable");
        fs::create_dir_all(&root).unwrap();
        let source = root.join("sealed-input.bin");
        fs::write(&source, b"immutable-audit").unwrap();
        let tracked = track_stable_file(&source).unwrap();
        fs::write(&source, b"mutable---audit").unwrap();

        let error = revalidate_tracked_file(&tracked).unwrap_err();
        assert!(
            error.to_string().contains("tracked input changed")
                || error.to_string().contains("tracked input digest changed")
        );
        fs::remove_dir_all(root).unwrap();
    }
}
