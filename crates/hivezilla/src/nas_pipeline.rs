use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN, ARCHIVE_V2_BLOCK_ACCESS_INDEX_MAGIC,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN, ARCHIVE_V2_BLOCK_ACCESS_INDEX_VERSION,
    ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN, ARCHIVE_V2_HOT_INDEX_HEADER_LEN,
    ARCHIVE_V2_HOT_INDEX_MAGIC, ARCHIVE_V2_HOT_INDEX_ROW_LEN, ARCHIVE_V2_HOT_INDEX_VERSION,
    ArchiveV2BlockAccessBlob, WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
    WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
    WincodeLeb128FramedReader, write_u32_varint,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    convert::Infallible,
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom},
    net::SocketAddr,
    os::{fd::AsRawFd, unix::ffi::OsStrExt, unix::fs::MetadataExt},
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    process::{Child, Command},
    sync::{Mutex, RwLock, broadcast},
};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
};

const STATUS_SCHEMA_VERSION: u32 = 3;
// Ownership, lock, and acquisition receipts remain on their existing schema.
// Status evolution must not invalidate durable scheduler markers on restart.
const SCHEMA_VERSION: u32 = 2;
const SLOTS_PER_EPOCH: u64 = 432_000;
const SCAN_MARKER: &str = "archive-v2-first-seen-scan-complete.v1";
const META_FILE: &str = "archive-v2-meta.wincode";
const REGISTRY_FILE: &str = "registry.bin";
const REGISTRY_COUNTS_FILE: &str = "registry_counts.bin";
const REGISTRY_INDEX_FILE: &str = "registry.mphf";
const FIRST_SEEN_MANIFEST_FILE: &str = "registry-first-seen.manifest";
const BLOCKHASH_REGISTRY_FILE: &str = "blockhash_registry.bin";
const BLOCKS_FILE: &str = "archive-v2-blocks.zstd";
const BLOCK_INDEX_FILE: &str = "archive-v2-blocks.index";
const POH_FILE: &str = "poh.wincode";
const SHREDDING_FILE: &str = "shredding.wincode";
const SIGNATURES_FILE: &str = "signatures.bin";
const VOTE_HASH_REGISTRY_FILE: &str = "vote_hash_registry.bin";
const BLOCK_ACCESS_FILE: &str = "archive-v2-block-access.wincode";
const BLOCK_ACCESS_INDEX_FILE: &str = "archive-v2-block-access.index";
const GET_BLOCK_INDEX_FILE: &str = "archive-v2-get-block.index";
const HOT_SEED_FILE: &str = "registry-hot-seed.bin";
const PREVIOUS_BLOCKHASH_TAIL_FILE: &str = "prev_blockhash_tail.bin";
const LIVE_FINALIZE_MARKER: &str = "FINALIZE-NEXT.md";
const LIVE_READY_MARKER: &str = "READY-TO-PACKAGE";
const LIVE_REGISTRY_READY_MARKER: &str = "archive-v2-live-registry-prepared.v1";
const LIVE_REPAIR_REQUIRED_MARKER: &str = "REPAIR-REQUIRED.json";
const LIVE_REPAIR_COMPACTED_MARKER: &str = "REPAIR-COMPACTED.json";
const LIVE_REPAIR_PLAN_FILE: &str = "repair/live-merge-plan.jsonl";
const LIVE_REPAIR_AVAILABLE_POH_FILE: &str = "repair/available-poh.wincode";
const LIVE_REPAIR_SOURCE_MATERIALIZED_MARKER: &str = "repair/source-REPAIR-MATERIALIZED.json";
const MAX_LIVE_REPAIR_MARKER_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LIVE_REPAIR_META_BYTES: u64 = 2 * 1024 * 1024;
const MAX_LIVE_REPAIR_POH_FRAME_BYTES: u64 = 64 * 1024 * 1024;
const MAX_LIVE_REPAIR_PLAN_BYTES: u64 = 256 * 1024 * 1024;
const MAX_LIVE_REPAIR_SOURCES: usize = 256;
const MAX_LIVE_REPAIR_SOURCE_PATH_BYTES: usize = 4 * 1024;
const PROGRESS_STALE_SECS: u64 = 120;
const PROGRESS_MONITOR_INTERVAL: Duration = Duration::from_secs(1);
const MAX_ERRORS: usize = 100;
const OWNERSHIP_MARKER: &str = ".hivezilla-pipeline-owned.v1.json";
const FINALIZER_BUILD_OVERHEAD_BYTES: u64 = 256 * 1024 * 1024;
const FINALIZER_REWRITE_OVERHEAD_BYTES: u64 = 512 * 1024 * 1024;
const DOWNLOAD_MAX_ATTEMPTS: u8 = 3;
const PREFLIGHT_IO_BUFFER_MIB: u64 = 8;
// zstd preflight accepts a windowLog up to 31 (2 GiB). Budget that window plus
// decoder/process overhead instead of treating the small I/O buffer as the
// task's peak memory.
const PREFLIGHT_MEMORY_MIB: u64 = 2_304;
const MAX_PREFLIGHT_RECEIPT_BYTES: u64 = 1024 * 1024;
const MAX_FIRST_SEEN_MANIFEST_BYTES: u64 = 64 * 1024;
const SCAN_MARKER_MAGIC: &str = "blockzilla-first-seen-scan-complete-v1";
const REGISTRY_INDEX_MAGIC: &[u8; 8] = b"BZKIDX1!";
const REGISTRY_INDEX_VERSION: u16 = 2;
const REGISTRY_INDEX_HEADER_LEN: u16 = 8 + 2 + 2 + 8;
const PIPELINE_LOCK_FILE: &str = "pipeline.lock";
const ACQUISITION_FAILURES_FILE: &str = "acquisition-failures.json";
const MIN_CAR_DOWNLOAD_PROJECTION_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const MIN_SCAN_OUTPUT_PROJECTION_BYTES: u64 = 1024 * 1024 * 1024;
const MIN_FINALIZER_SCRATCH_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct NasPipelineConfig {
    pub bind: SocketAddr,
    pub blockzilla_bin: PathBuf,
    /// Optional repair-capable binary. Production may keep the historical
    /// scanner pinned while repair materialization runs a newer build.
    pub repair_blockzilla_bin: Option<PathBuf>,
    pub car_root: PathBuf,
    pub archive_root: PathBuf,
    pub live_root: PathBuf,
    pub state_root: PathBuf,
    pub scan_concurrency: usize,
    pub scan_memory_mib: u64,
    pub finalizer_memory_mib: u64,
    pub memory_reserve_mib: u64,
    pub disk_reserve_gib: u64,
    pub level: i32,
    pub execute: bool,
    pub no_access: bool,
    pub start_epoch: Option<u64>,
    pub end_epoch: Option<u64>,
    /// Optional work-conserving historical priority band. Runnable candidates
    /// in the band are preferred newest-first; normal ordering remains the fallback.
    pub priority_epoch_start: Option<u64>,
    pub priority_epoch_end: Option<u64>,
    pub car_source_url_template: Option<String>,
    pub download_concurrency: usize,
    pub preflight_car: bool,
    pub poll_interval: Duration,
    pub finalizer_lock: PathBuf,
    pub ui_dir: Option<PathBuf>,
    pub control_token: Option<String>,
    pub allow_unauthenticated_controls: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalState {
    Queued,
    Scanning,
    ScanReady,
    Finalizing,
    Complete,
    Failed,
    Blocked,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryOrder {
    UsageSorted,
    FirstSeen,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveState {
    Capturing,
    RepairGate,
    RepairRequired,
    ReadyToPackage,
    Packaging,
    Packaged,
    Complete,
    Failed,
    Blocked,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProgressSnapshot {
    pub phase: Option<String>,
    pub state: Option<String>,
    pub pid: Option<u32>,
    pub blocks_done: u64,
    pub blocks_total: u64,
    pub transactions_done: u64,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub progress_pct: Option<f64>,
    pub elapsed_secs: Option<f64>,
    pub blocks_per_sec: Option<f64>,
    /// Slot advance rate. Live capture ETA is based on slots rather than
    /// blocks because skipped slots still move the epoch boundary forward.
    #[serde(default)]
    pub slots_per_sec: Option<f64>,
    pub input_mib_per_sec: Option<f64>,
    /// Linux process-attributed storage reads for this worker tree. This is
    /// sampled from `/proc/<pid>/io`; it is not raw block-device bus traffic.
    #[serde(default)]
    pub disk_read_mib_per_sec: Option<f64>,
    /// Linux process-attributed storage writes for this worker tree.
    #[serde(default)]
    pub disk_write_mib_per_sec: Option<f64>,
    pub eta_secs: Option<f64>,
    pub rss_bytes: Option<u64>,
    /// Process lifetime RSS high-water mark (`VmHWM` on Linux).
    #[serde(default)]
    pub peak_rss_bytes: Option<u64>,
    pub updated_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EpochSnapshot {
    pub epoch: u64,
    pub state: HistoricalState,
    #[serde(default)]
    pub registry_order: RegistryOrder,
    pub input_path: Option<PathBuf>,
    pub output_path: PathBuf,
    pub car_bytes: u64,
    pub artifacts: Vec<ArtifactSnapshot>,
    pub progress: ProgressSnapshot,
    pub message: Option<String>,
    pub updated_unix_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    Car,
    CarPreflight,
    SourcePohInfo,
    SourceShreddingInfo,
    ScanMarker,
    Metadata,
    Registry,
    RegistryCounts,
    RegistryIndex,
    FirstSeenManifest,
    HotSeed,
    BlockhashRegistry,
    Blocks,
    BlockIndex,
    Poh,
    Shredding,
    Signatures,
    VoteHashRegistry,
    BlockAccess,
    BlockAccessIndex,
    PreviousBlockhashTail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactState {
    Missing,
    Pending,
    Building,
    Candidate,
    Present,
    Verified,
    Invalid,
    NotApplicable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRequirement {
    ScanInput,
    ScanOutput,
    FinalOutput,
    Optional,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactSnapshot {
    pub kind: ArtifactKind,
    pub state: ArtifactState,
    pub requirement: ArtifactRequirement,
    pub required_now: bool,
    pub bytes: u64,
    pub modified_unix_secs: Option<u64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneSnapshot {
    pub id: String,
    pub kind: String,
    pub epoch: Option<u64>,
    pub capture_id: Option<String>,
    pub phase: String,
    pub state: String,
    pub pid: Option<u32>,
    pub progress: ProgressSnapshot,
    pub rss_bytes: Option<u64>,
    pub started_unix_secs: Option<u64>,
    pub updated_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveCaptureSnapshot {
    pub id: String,
    pub epoch: Option<u64>,
    /// True for the single capture that represents the current live epoch.
    /// Closed captures waiting for compaction remain visible but are never
    /// marked current.
    pub is_current: bool,
    pub state: LiveState,
    pub capture_dir: PathBuf,
    pub output_path: Option<PathBuf>,
    pub ready_to_package: bool,
    pub repair_gate: bool,
    /// Capture directories retained as inputs by an atomically published
    /// repair bundle. Only repair-bundle snapshots populate this list.
    #[serde(default)]
    pub source_capture_ids: Vec<String>,
    /// Published repair bundle that makes this source-level workflow obsolete.
    /// Active, repair-gated, and cross-epoch sources are never superseded.
    #[serde(default)]
    pub superseded_by: Option<String>,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub blocks_written: u64,
    pub artifacts: Vec<ArtifactSnapshot>,
    pub progress: ProgressSnapshot,
    /// Explicit live-index aliases keep the status API easy to consume while
    /// the nested progress object remains backward compatible.
    #[serde(default)]
    pub eta_secs: Option<f64>,
    #[serde(default)]
    pub slots_per_sec: Option<f64>,
    #[serde(default)]
    pub rss_bytes: Option<u64>,
    #[serde(default)]
    pub peak_rss_bytes: Option<u64>,
    pub message: Option<String>,
    pub updated_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizerQueueItem {
    pub kind: String,
    pub epoch: Option<u64>,
    pub id: String,
    pub phase: String,
    pub state: String,
    pub estimated_memory_bytes: u64,
    pub estimated_disk_bytes: u64,
    pub deferred_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineError {
    pub at_unix_secs: u64,
    pub scope: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineSummary {
    pub epochs_total: usize,
    pub queued: usize,
    pub scanning: usize,
    pub scan_ready: usize,
    pub finalizing: usize,
    pub complete: usize,
    pub failed: usize,
    pub blocked: usize,
    pub progress_pct: f64,
    pub blocks_done: u64,
    pub blocks_total: u64,
    pub blocks_per_sec: f64,
    /// Sum of fresh process-attributed storage rates for running worker lanes.
    #[serde(default)]
    pub disk_read_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub disk_write_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub disk_io_active_roots: usize,
    #[serde(default)]
    pub disk_io_sampled_roots: usize,
    /// Backward-compatible alias for `queue_eta_secs`.
    pub eta_secs: Option<f64>,
    /// Wall-clock estimate to drain queued and active historical worker jobs.
    /// Blocked and failed epochs are deliberately excluded.
    #[serde(default)]
    pub queue_eta_secs: Option<f64>,
    #[serde(default)]
    pub queue_eta_reason: Option<String>,
    #[serde(default)]
    pub queue_jobs_remaining: usize,
    #[serde(default)]
    pub queue_capacity: usize,
    #[serde(default)]
    pub queue_job_duration_secs: Option<f64>,
    #[serde(default)]
    pub queue_duration_samples: usize,
    /// Estimate for the historical scan sweep only. This deliberately does
    /// not claim to include the exclusive finalizer or live compaction.
    pub scan_eta_secs: Option<f64>,
    /// End-to-end ETA until every historical and live-managed epoch is a
    /// complete archive. Unknown when a required phase has no duration model.
    pub archive_eta_secs: Option<f64>,
    pub archive_eta_reason: Option<String>,
    pub scan_capacity_configured: usize,
    pub scan_capacity_admitted: usize,
    pub admission_blocked_reason: Option<String>,
    pub finalizer_wait_reason: Option<String>,
    pub finalizer_admission_blocked_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MachineSnapshot {
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_available_bytes: u64,
    pub swap_used_bytes: u64,
    pub swap_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub disk_available_bytes: u64,
    pub car_disk_used_bytes: u64,
    pub car_disk_total_bytes: u64,
    pub car_disk_available_bytes: u64,
    #[serde(default)]
    pub car_disk_shared_with_archive: bool,
    /// Linux block-device identity backing `archive_root`, when it can be
    /// matched to `/proc/diskstats`.
    #[serde(default)]
    pub archive_device_major: Option<u32>,
    #[serde(default)]
    pub archive_device_minor: Option<u32>,
    #[serde(default)]
    pub archive_device_name: Option<String>,
    /// Whole-device throughput from `/proc/diskstats`. Unlike the summary's
    /// worker rates, this includes I/O from every process using the device.
    #[serde(default)]
    pub archive_device_read_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub archive_device_write_mib_per_sec: Option<f64>,
    pub load_1m: f64,
    pub service_rss_bytes: u64,
    pub children_rss_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSnapshot {
    pub schema_version: u32,
    pub sequence: u64,
    pub now_unix_secs: u64,
    /// Epoch currently being captured by the canonical live producer.
    pub current_epoch: Option<u64>,
    pub observer_mode: bool,
    pub capabilities: CapabilitySnapshot,
    pub scheduler: SchedulerSnapshot,
    pub inventory: InventorySnapshot,
    pub scan_sweep: ScanSweepSnapshot,
    pub summary: PipelineSummary,
    pub machine: MachineSnapshot,
    pub epochs: Vec<EpochSnapshot>,
    pub lanes: Vec<LaneSnapshot>,
    pub live: Vec<LiveCaptureSnapshot>,
    pub finalizer_queue: Vec<FinalizerQueueItem>,
    pub errors: Vec<PipelineError>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InventorySnapshot {
    pub generation: u64,
    pub complete: bool,
    pub epochs_discovered: usize,
    pub epochs_classified: usize,
    pub started_unix_secs: u64,
    pub completed_unix_secs: Option<u64>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScanSweepSnapshot {
    pub generation: u64,
    pub complete: bool,
    pub pending: usize,
    pub active: usize,
    pub terminal_gaps: usize,
    pub deferred_finalizers: usize,
    pub wait_reason: Option<String>,
    pub blocked_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapabilitySnapshot {
    pub control_enabled: bool,
    pub authenticated_controls_required: bool,
    pub can_pause_scheduler: bool,
    pub can_retry_failed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchedulerSnapshot {
    pub paused: bool,
    pub updated_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
struct RealtimeEnvelope<T> {
    #[serde(rename = "type")]
    event_type: &'static str,
    sequence: u64,
    data: T,
}

#[derive(Debug, Clone, Serialize)]
struct SnapshotPatch {
    schema_version: u32,
    sequence: u64,
    now_unix_secs: u64,
    current_epoch: Option<u64>,
    observer_mode: bool,
    capabilities: CapabilitySnapshot,
    scheduler: SchedulerSnapshot,
    inventory: InventorySnapshot,
    scan_sweep: ScanSweepSnapshot,
    summary: PipelineSummary,
    machine: MachineSnapshot,
    epochs_changed: Vec<EpochSnapshot>,
    epochs_removed: Vec<u64>,
    lanes: Vec<LaneSnapshot>,
    live: Vec<LiveCaptureSnapshot>,
    finalizer_queue: Vec<FinalizerQueueItem>,
    errors: Vec<PipelineError>,
}

impl SnapshotPatch {
    fn between(previous: &PipelineSnapshot, current: &PipelineSnapshot) -> Self {
        let previous_epochs = previous
            .epochs
            .iter()
            .map(|epoch| (epoch.epoch, epoch))
            .collect::<BTreeMap<_, _>>();
        let current_epoch_ids = current
            .epochs
            .iter()
            .map(|epoch| epoch.epoch)
            .collect::<BTreeSet<_>>();
        let epochs_changed = current
            .epochs
            .iter()
            .filter(|epoch| previous_epochs.get(&epoch.epoch).copied() != Some(*epoch))
            .cloned()
            .collect();
        let epochs_removed = previous_epochs
            .keys()
            .filter(|epoch| !current_epoch_ids.contains(epoch))
            .copied()
            .collect();

        Self {
            schema_version: current.schema_version,
            sequence: current.sequence,
            now_unix_secs: current.now_unix_secs,
            current_epoch: current.current_epoch,
            observer_mode: current.observer_mode,
            capabilities: current.capabilities.clone(),
            scheduler: current.scheduler.clone(),
            inventory: current.inventory.clone(),
            scan_sweep: current.scan_sweep.clone(),
            summary: current.summary.clone(),
            machine: current.machine.clone(),
            epochs_changed,
            epochs_removed,
            lanes: current.lanes.clone(),
            live: current.live.clone(),
            finalizer_queue: current.finalizer_queue.clone(),
            errors: current.errors.clone(),
        }
    }

    fn active_progress(current: &PipelineSnapshot, epochs_changed: Vec<EpochSnapshot>) -> Self {
        Self {
            schema_version: current.schema_version,
            sequence: current.sequence,
            now_unix_secs: current.now_unix_secs,
            current_epoch: current.current_epoch,
            observer_mode: current.observer_mode,
            capabilities: current.capabilities.clone(),
            scheduler: current.scheduler.clone(),
            inventory: current.inventory.clone(),
            scan_sweep: current.scan_sweep.clone(),
            summary: current.summary.clone(),
            machine: current.machine.clone(),
            epochs_changed,
            epochs_removed: Vec::new(),
            lanes: current.lanes.clone(),
            live: current.live.clone(),
            finalizer_queue: current.finalizer_queue.clone(),
            errors: current.errors.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ResyncNotice {
    reason: &'static str,
    skipped: u64,
    status_url: &'static str,
}

#[derive(Debug)]
enum RealtimeMessage {
    SnapshotPatch(RealtimeEnvelope<SnapshotPatch>),
    Resync(RealtimeEnvelope<ResyncNotice>),
}

impl RealtimeMessage {
    fn event_name(&self) -> &'static str {
        match self {
            Self::SnapshotPatch(_) => "snapshot_patch",
            Self::Resync(_) => "resync",
        }
    }

    fn into_sse_event(self) -> Event {
        let event_name = self.event_name();
        match self {
            Self::SnapshotPatch(envelope) => sse_event(event_name, &envelope),
            Self::Resync(envelope) => sse_event(event_name, &envelope),
        }
    }
}

#[derive(Debug)]
struct AppState {
    config: NasPipelineConfig,
    snapshot: RwLock<PipelineSnapshot>,
    updates: broadcast::Sender<RealtimeEnvelope<SnapshotPatch>>,
    sequence: AtomicU64,
    publication: Mutex<()>,
    runtime: Mutex<RuntimeState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgressWorkerIdentity {
    pid: Option<u32>,
    phase: Option<String>,
    started_unix_secs: Option<u64>,
}

#[derive(Debug, Default)]
struct ActiveProgressTargets {
    lanes: Vec<LaneProgressTarget>,
    live: Vec<LiveProgressTarget>,
}

#[derive(Debug)]
struct LaneProgressTarget {
    id: String,
    paths: Vec<PathBuf>,
    baseline: ProgressSnapshot,
    baseline_identity: ProgressWorkerIdentity,
}

#[derive(Debug)]
struct LiveProgressTarget {
    id: String,
    state: LiveState,
    capture_dir: PathBuf,
    paths: Vec<PathBuf>,
    baseline: ProgressSnapshot,
    baseline_identity: ProgressWorkerIdentity,
}

#[derive(Debug)]
struct LaneProgressUpdate {
    id: String,
    baseline: ProgressSnapshot,
    baseline_identity: ProgressWorkerIdentity,
    progress: ProgressSnapshot,
}

#[derive(Debug)]
struct LiveProgressUpdate {
    id: String,
    baseline: ProgressSnapshot,
    baseline_identity: ProgressWorkerIdentity,
    progress: ProgressSnapshot,
}

#[derive(Debug, Default)]
struct RuntimeState {
    acquisitions: BTreeMap<u64, ManagedChild>,
    scans: BTreeMap<u64, ManagedChild>,
    finalizer: Option<ManagedChild>,
    errors: VecDeque<PipelineError>,
    failures: BTreeMap<String, String>,
    scheduler_paused: bool,
    scheduler_updated_unix_secs: u64,
    paused_jobs: BTreeSet<String>,
    process_io_samples: BTreeMap<String, ProcessIoSample>,
    archive_device_io_sample: Option<BlockDeviceIoSample>,
    inventory_generation: u64,
}

#[derive(Debug, Clone)]
struct ProcessIoSample {
    members: BTreeMap<(u32, u64), ProcessIoCounters>,
    sampled_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct ProcessIoCounters {
    read_bytes: u64,
    write_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockDeviceIoCounters {
    major: u32,
    minor: u32,
    name: String,
    sectors_read: u64,
    sectors_written: u64,
}

#[derive(Debug, Clone)]
struct BlockDeviceIoSample {
    counters: BlockDeviceIoCounters,
    sampled_at: Instant,
}

#[derive(Debug)]
struct ManagedChild {
    child: Child,
    pid: Option<u32>,
    kind: ChildKind,
    started_unix_secs: u64,
    progress_path: PathBuf,
    log_path: PathBuf,
    _exclusive_lock: Option<File>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OwnershipMarker {
    schema_version: u32,
    kind: String,
    id: String,
    state: String,
    created_unix_secs: u64,
    updated_unix_secs: u64,
    message: Option<String>,
    #[serde(default)]
    pid: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AcquisitionMarker {
    schema_version: u32,
    epoch: u64,
    kind: String,
    pid: u32,
    expected_path: PathBuf,
    receipt_path: PathBuf,
    updated_unix_secs: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedAcquisitionFailures {
    failures: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
enum ChildKind {
    CarDownload {
        epoch: u64,
        canonical_path: PathBuf,
        receipt_path: PathBuf,
    },
    CarPreflight {
        epoch: u64,
        input_path: PathBuf,
        receipt_path: PathBuf,
    },
    HistoricalScan {
        epoch: u64,
    },
    HistoricalFinalizer {
        epoch: u64,
    },
    LiveFinalizer {
        id: String,
        epoch: Option<u64>,
        phase: LiveFinalizerPhase,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveFinalizerPhase {
    Registry,
    Mphf,
    Rewrite,
}

impl LiveFinalizerPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Registry => "registry_merge",
            Self::Mphf => "mphf_build",
            Self::Rewrite => "hot_rewrite",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "registry_merge" => Some(Self::Registry),
            "mphf_build" => Some(Self::Mphf),
            "hot_rewrite" => Some(Self::Rewrite),
            _ => None,
        }
    }
}

impl ChildKind {
    fn key(&self) -> String {
        match self {
            Self::CarDownload { epoch, .. } => format!("download:{epoch}"),
            Self::CarPreflight { epoch, .. } => format!("preflight:{epoch}"),
            Self::HistoricalScan { epoch } => format!("scan:{epoch}"),
            Self::HistoricalFinalizer { epoch } => format!("finalize:{epoch}"),
            Self::LiveFinalizer { id, .. } => format!("live:{id}"),
        }
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
    mode: &'static str,
}

fn effective_repair_blockzilla_bin(config: &NasPipelineConfig) -> &Path {
    config
        .repair_blockzilla_bin
        .as_deref()
        .unwrap_or(&config.blockzilla_bin)
}

pub async fn run_nas_pipeline(config: NasPipelineConfig) -> Result<()> {
    anyhow::ensure!(
        config.scan_concurrency > 0,
        "scan concurrency must be positive"
    );
    anyhow::ensure!(
        config.finalizer_memory_mib > 0,
        "finalizer memory budget must be positive"
    );
    anyhow::ensure!(
        config.download_concurrency > 0,
        "download concurrency must be positive"
    );
    match (config.priority_epoch_start, config.priority_epoch_end) {
        (Some(start), Some(end)) => anyhow::ensure!(
            start <= end,
            "priority epoch start must not exceed priority epoch end"
        ),
        (None, None) => {}
        _ => anyhow::bail!("priority epoch start and end must be configured together"),
    }
    if let Some(template) = config.car_source_url_template.as_deref() {
        anyhow::ensure!(
            template.contains("{epoch}"),
            "CAR source URL template must contain {{epoch}}"
        );
        anyhow::ensure!(
            config.start_epoch.is_some() && config.end_epoch.is_some(),
            "CAR acquisition requires explicit start and end epoch bounds"
        );
        for epoch in [config.start_epoch.unwrap(), config.end_epoch.unwrap()] {
            let rendered = template.replace("{epoch}", &epoch.to_string());
            car_source_suffix(&rendered)?;
        }
    }
    fs::create_dir_all(config.state_root.join("logs"))
        .with_context(|| format!("create pipeline state root {}", config.state_root.display()))?;
    fs::create_dir_all(config.state_root.join("progress")).with_context(|| {
        format!(
            "create pipeline progress root {}",
            config.state_root.display()
        )
    })?;
    // This process owns status.json and the scheduler for this state root even
    // in observer mode. Hold the lock for the whole API lifetime so two
    // controllers cannot reconcile or mutate the same state concurrently.
    let _pipeline_lock = acquire_pipeline_lock(&config.state_root)?;
    if config.execute {
        anyhow::ensure!(
            is_nonempty_file(&config.blockzilla_bin),
            "blockzilla executable is missing or empty: {}",
            config.blockzilla_bin.display()
        );
        let repair_bin = effective_repair_blockzilla_bin(&config);
        anyhow::ensure!(
            is_nonempty_file(repair_bin),
            "repair-capable blockzilla executable is missing or empty: {}",
            repair_bin.display()
        );
    }

    let initial = empty_snapshot(!config.execute);
    // SSE consumers only need recent snapshots; retaining dozens of cloned
    // epoch inventories makes controller memory grow with corpus size.
    let (updates, _) = broadcast::channel(4);
    let state = Arc::new(AppState {
        config: config.clone(),
        snapshot: RwLock::new(initial),
        updates,
        sequence: AtomicU64::new(0),
        publication: Mutex::new(()),
        runtime: Mutex::new(RuntimeState::default()),
    });
    load_persisted_errors(&state).await;
    load_control_state(&state).await;
    load_acquisition_failures(&state).await;

    // Bind before the first reconciliation. This also protects an upgrade
    // from an older binary that did not yet participate in pipeline.lock: a
    // port collision cannot launch work and then fail startup.
    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("bind NAS pipeline on {}", config.bind))?;
    reconcile_and_schedule(&state).await;

    let scheduler_state = Arc::clone(&state);
    let scheduler = tokio::spawn(async move {
        let mut interval = tokio::time::interval(scheduler_state.config.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            reconcile_and_schedule(&scheduler_state).await;
        }
    });
    let progress_state = Arc::clone(&state);
    let progress_monitor = tokio::spawn(async move {
        let mut interval = tokio::time::interval(PROGRESS_MONITOR_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            monitor_active_progress(&progress_state).await;
        }
    });

    let mut app = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/v1/status", get(status))
        .route("/api/v1/events", get(events))
        .route("/api/v1/control/pause", post(pause_scheduler))
        .route("/api/v1/control/resume", post(resume_scheduler))
        .route("/api/v1/jobs/{kind}/{id}/pause", post(pause_job))
        .route("/api/v1/jobs/{kind}/{id}/resume", post(resume_job))
        .route("/api/v1/jobs/{kind}/{id}/retry", post(retry_job))
        .layer(CorsLayer::permissive())
        .with_state(state);
    if let Some(ui_dir) = config.ui_dir.as_deref() {
        let index = ui_dir.join("index.html");
        if index.is_file() {
            app = app
                .fallback_service(ServeDir::new(ui_dir).not_found_service(ServeFile::new(index)));
        }
    }

    let result = axum::serve(listener, app)
        .await
        .context("run NAS pipeline API");
    scheduler.abort();
    let _ = scheduler.await;
    progress_monitor.abort();
    let _ = progress_monitor.await;
    result
}

async fn healthz(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        mode: if state.config.execute {
            "execute"
        } else {
            "observer"
        },
    })
}

async fn status(State(state): State<Arc<AppState>>) -> Json<PipelineSnapshot> {
    Json(state.snapshot.read().await.clone())
}

async fn events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    // Subscribe first so an update published while the initial snapshot is
    // cloned is queued behind it instead of being lost between the two steps.
    let receiver = state.updates.subscribe();
    let initial_snapshot = state.snapshot.read().await.clone();
    let initial = RealtimeEnvelope {
        event_type: "snapshot",
        sequence: initial_snapshot.sequence,
        data: initial_snapshot,
    };
    let initial_stream = tokio_stream::once(Ok(sse_event("snapshot", &initial)));
    let update_state = Arc::clone(&state);
    let update_stream = BroadcastStream::new(receiver).map(move |item| {
        let sequence = update_state.sequence.load(Ordering::Relaxed);
        Ok(realtime_message(item, sequence).into_sse_event())
    });
    Sse::new(initial_stream.chain(update_stream)).keep_alive(KeepAlive::default())
}

fn realtime_message(
    item: Result<
        RealtimeEnvelope<SnapshotPatch>,
        tokio_stream::wrappers::errors::BroadcastStreamRecvError,
    >,
    current_sequence: u64,
) -> RealtimeMessage {
    match item {
        Ok(envelope) => RealtimeMessage::SnapshotPatch(envelope),
        Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(skipped)) => {
            RealtimeMessage::Resync(RealtimeEnvelope {
                event_type: "resync",
                sequence: current_sequence,
                data: ResyncNotice {
                    reason: "subscriber_lagged",
                    skipped,
                    status_url: "/api/v1/status",
                },
            })
        }
    }
}

fn sse_event<T: Serialize>(event_name: &'static str, envelope: &RealtimeEnvelope<T>) -> Event {
    Event::default()
        .event(event_name)
        .id(envelope.sequence.to_string())
        .json_data(envelope)
        .unwrap_or_else(|_| Event::default().event(event_name).data("{}"))
}

#[derive(Debug, Serialize)]
struct ControlResponse {
    ok: bool,
    action: String,
    target: String,
    message: String,
    snapshot_sequence: u64,
}

#[derive(Debug)]
enum ControlError {
    Disabled(String),
    Unauthorized,
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    Internal(anyhow::Error),
}

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Disabled(message) => (StatusCode::FORBIDDEN, message),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_string()),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::Internal(error) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")),
        };
        (
            status,
            Json(ControlResponse {
                ok: false,
                action: "error".to_string(),
                target: "control".to_string(),
                message,
                snapshot_sequence: 0,
            }),
        )
            .into_response()
    }
}

fn authorize_control(config: &NasPipelineConfig, headers: &HeaderMap) -> Result<(), ControlError> {
    if !config.execute {
        return Err(ControlError::Disabled(
            "controls are disabled in observer mode".to_string(),
        ));
    }
    if let Some(token) = config.control_token.as_deref() {
        let expected = format!("Bearer {token}");
        let actual = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        return (actual == Some(expected.as_str()))
            .then_some(())
            .ok_or(ControlError::Unauthorized);
    }
    if config.allow_unauthenticated_controls {
        Ok(())
    } else {
        Err(ControlError::Disabled(
            "set HIVEZILLA_CONTROL_TOKEN or explicitly allow unauthenticated controls".to_string(),
        ))
    }
}

async fn pause_scheduler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ControlResponse>, ControlError> {
    scheduler_control(state, headers, true).await
}

async fn resume_scheduler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ControlResponse>, ControlError> {
    scheduler_control(state, headers, false).await
}

async fn scheduler_control(
    state: Arc<AppState>,
    headers: HeaderMap,
    paused: bool,
) -> Result<Json<ControlResponse>, ControlError> {
    authorize_control(&state.config, &headers)?;
    {
        let mut runtime = state.runtime.lock().await;
        runtime.scheduler_paused = paused;
        runtime.scheduler_updated_unix_secs = unix_now();
        persist_control_state(&state.config, &runtime).map_err(ControlError::Internal)?;
        append_control_event(
            &state.config,
            if paused { "pause" } else { "resume" },
            "scheduler",
        )
        .map_err(ControlError::Internal)?;
    }
    reconcile_and_schedule(&state).await;
    let sequence = state.snapshot.read().await.sequence;
    Ok(Json(ControlResponse {
        ok: true,
        action: if paused { "pause" } else { "resume" }.to_string(),
        target: "scheduler".to_string(),
        message: if paused {
            "scheduler paused; active children continue draining"
        } else {
            "scheduler resumed"
        }
        .to_string(),
        snapshot_sequence: sequence,
    }))
}

async fn pause_job(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath((kind, id)): AxumPath<(String, String)>,
) -> Result<Json<ControlResponse>, ControlError> {
    job_signal_control(state, headers, kind, id, true).await
}

async fn resume_job(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath((kind, id)): AxumPath<(String, String)>,
) -> Result<Json<ControlResponse>, ControlError> {
    job_signal_control(state, headers, kind, id, false).await
}

async fn retry_job(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    AxumPath((kind, id)): AxumPath<(String, String)>,
) -> Result<Json<ControlResponse>, ControlError> {
    authorize_control(&state.config, &headers)?;
    let snapshot = state.snapshot.read().await.clone();
    if matches!(kind.as_str(), "car_download" | "car_preflight") {
        let epoch = id.parse::<u64>().map_err(|_| {
            ControlError::BadRequest("acquisition retry id must be an epoch number".to_string())
        })?;
        let epoch_state = snapshot
            .epochs
            .iter()
            .find(|candidate| candidate.epoch == epoch)
            .ok_or_else(|| ControlError::NotFound(format!("epoch {epoch} is not tracked")))?;
        if epoch_state.state != HistoricalState::Failed {
            return Err(ControlError::Conflict(format!(
                "epoch {epoch} is {:?}, not failed",
                epoch_state.state
            )));
        }
        if acquisition_claim_active(&state.config, epoch) {
            return Err(ControlError::Conflict(
                "acquisition process is still running".to_string(),
            ));
        }
        let mut runtime = state.runtime.lock().await;
        clear_runtime_failure(&state.config, &mut runtime, &format!("download:{epoch}"));
        clear_runtime_failure(&state.config, &mut runtime, &format!("preflight:{epoch}"));
        runtime.paused_jobs.remove(&format!("download:{epoch}"));
        runtime.paused_jobs.remove(&format!("preflight:{epoch}"));
        let _ = fs::remove_file(acquisition_marker_path(&state.config.state_root, epoch));
        let _ = fs::remove_file(car_preflight_receipt_path(&state.config.state_root, epoch));
        persist_control_state(&state.config, &runtime).map_err(ControlError::Internal)?;
        append_control_event(&state.config, "retry", &format!("{kind}/{id}"))
            .map_err(ControlError::Internal)?;
        drop(runtime);
        reconcile_and_schedule(&state).await;
        let sequence = state.snapshot.read().await.sequence;
        return Ok(Json(ControlResponse {
            ok: true,
            action: "retry".to_string(),
            target: format!("{kind}/{id}"),
            message: "acquisition failure cleared; resumable partial download preserved"
                .to_string(),
            snapshot_sequence: sequence,
        }));
    }
    let (target, progress_path, failure_keys) = match kind.as_str() {
        "historical_scan" | "historical_finalizer" => {
            let epoch = id.parse::<u64>().map_err(|_| {
                ControlError::BadRequest("historical retry id must be an epoch number".to_string())
            })?;
            let epoch_state = snapshot
                .epochs
                .iter()
                .find(|candidate| candidate.epoch == epoch)
                .ok_or_else(|| ControlError::NotFound(format!("epoch {epoch} is not tracked")))?;
            if epoch_state.state != HistoricalState::Failed {
                return Err(ControlError::Conflict(format!(
                    "epoch {epoch} is {:?}, not failed",
                    epoch_state.state
                )));
            }
            (
                epoch_state.output_path.clone(),
                historical_progress_path(&state.config.state_root, epoch),
                vec![format!("scan:{epoch}"), format!("finalize:{epoch}")],
            )
        }
        "live_finalizer" => {
            let capture = snapshot
                .live
                .iter()
                .find(|capture| capture.id == id)
                .ok_or_else(|| {
                    ControlError::NotFound(format!("live capture {id} is not tracked"))
                })?;
            if let Some(bundle_id) = capture.superseded_by.as_deref() {
                return Err(ControlError::Conflict(format!(
                    "live capture {id} is superseded by repair bundle {bundle_id}"
                )));
            }
            if capture.state != LiveState::Failed {
                return Err(ControlError::Conflict(format!(
                    "live capture {id} is {:?}, not failed",
                    capture.state
                )));
            }
            let target = capture.output_path.clone().ok_or_else(|| {
                ControlError::Conflict("failed live capture has no target epoch".to_string())
            })?;
            (
                target,
                state
                    .config
                    .state_root
                    .join("progress")
                    .join(format!("live-{}-package.json", safe_segment(&id))),
                vec![format!("live:{id}")],
            )
        }
        _ => {
            return Err(ControlError::BadRequest(format!(
                "unsupported retry kind {kind}"
            )));
        }
    };
    let target_requires_manifest = pipeline_owned_first_seen(&target);
    if historical_archive_strict_complete(
        &target,
        !state.config.no_access,
        target_requires_manifest,
    ) {
        return Err(ControlError::Conflict(
            "completed output cannot be retried".to_string(),
        ));
    }
    let owner = read_ownership(&target).ok_or_else(|| {
        ControlError::Conflict(format!(
            "refusing to retry unowned or ambiguous output {}",
            target.display()
        ))
    })?;
    if !ownership_matches_retry(&owner, &kind, &id) {
        return Err(ControlError::Conflict(format!(
            "ownership marker {}/{} does not match retry target {kind}/{id}",
            owner.kind, owner.id
        )));
    }
    if !matches!(
        owner.state.as_str(),
        "failed"
            | "running"
            | "finalizing"
            | "packaging"
            | "registry_merge"
            | "mphf_build"
            | "hot_rewrite"
    ) {
        return Err(ControlError::Conflict(format!(
            "ownership marker state {} is not retryable",
            owner.state
        )));
    }
    if owner
        .pid
        .is_some_and(|pid| process_cmdline_contains(pid, &target))
    {
        return Err(ControlError::Conflict(
            "pipeline-owned process is still running".to_string(),
        ));
    }
    let retry_message = if kind == "live_finalizer" {
        // Live finalization is staged and each completed boundary is durable.
        // Keep valid registry/MPHF artifacts so retry resumes at the first
        // missing stage instead of repeating the whole epoch.
        let recovered_state = if is_nonempty_file(&target.join(REGISTRY_INDEX_FILE)) {
            "mphf_ready"
        } else if target.join(LIVE_REGISTRY_READY_MARKER).is_file()
            && is_nonempty_file(&target.join(REGISTRY_FILE))
            && is_nonempty_file(&target.join(REGISTRY_COUNTS_FILE))
        {
            "registry_ready"
        } else {
            "retry_ready"
        };
        write_ownership(&target, "live_finalizer", &id, recovered_state, None)
            .map_err(ControlError::Internal)?;
        set_ownership_pid(&target, None).map_err(ControlError::Internal)?;
        format!("preserved completed live stages in {}", target.display())
    } else {
        let quarantine_root = state.config.archive_root.join(".pipeline-quarantine");
        fs::create_dir_all(&quarantine_root)
            .map_err(|error| ControlError::Internal(error.into()))?;
        let quarantine = quarantine_root.join(format!(
            "{}-{}",
            safe_segment(
                target
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(&id)
            ),
            unix_now()
        ));
        fs::rename(&target, &quarantine).map_err(|error| {
            ControlError::Internal(anyhow::Error::from(error).context(format!(
                "quarantine {} -> {}",
                target.display(),
                quarantine.display()
            )))
        })?;
        format!("partial output quarantined at {}", quarantine.display())
    };
    let _ = fs::remove_file(&progress_path);
    {
        let mut runtime = state.runtime.lock().await;
        for key in failure_keys {
            runtime.failures.remove(&key);
            runtime.paused_jobs.remove(&key);
        }
        persist_control_state(&state.config, &runtime).map_err(ControlError::Internal)?;
        append_control_event(&state.config, "retry", &format!("{kind}/{id}"))
            .map_err(ControlError::Internal)?;
    }
    reconcile_and_schedule(&state).await;
    let sequence = state.snapshot.read().await.sequence;
    Ok(Json(ControlResponse {
        ok: true,
        action: "retry".to_string(),
        target: format!("{kind}/{id}"),
        message: retry_message,
        snapshot_sequence: sequence,
    }))
}

fn ownership_matches_retry(owner: &OwnershipMarker, kind: &str, id: &str) -> bool {
    match kind {
        "historical_scan" | "historical_finalizer" => {
            matches!(
                owner.kind.as_str(),
                "historical_scan" | "historical_finalizer"
            ) && owner.id == id
        }
        "live_finalizer" => owner.kind == "live_finalizer" && owner.id == id,
        _ => false,
    }
}

async fn job_signal_control(
    state: Arc<AppState>,
    headers: HeaderMap,
    kind: String,
    id: String,
    pause: bool,
) -> Result<Json<ControlResponse>, ControlError> {
    authorize_control(&state.config, &headers)?;
    let key = control_job_key(&kind, &id)?;
    let snapshot = state.snapshot.read().await.clone();
    let (pid, expected_path) = controlled_job_pid(&state, &snapshot, &kind, &id, &key).await?;
    validate_process_identity(pid, &state.config.blockzilla_bin, &expected_path, &kind)?;
    let signal = if pause { libc::SIGSTOP } else { libc::SIGCONT };
    // SAFETY: kill sends a signal to the positively identified child process.
    let signal_target = if matches!(kind.as_str(), "car_download" | "car_preflight") {
        -(pid as libc::pid_t)
    } else {
        pid as libc::pid_t
    };
    if unsafe { libc::kill(signal_target, signal) } != 0 {
        return Err(ControlError::Internal(
            std::io::Error::last_os_error().into(),
        ));
    }
    {
        let mut runtime = state.runtime.lock().await;
        if pause {
            runtime.paused_jobs.insert(key.clone());
        } else {
            runtime.paused_jobs.remove(&key);
        }
        persist_control_state(&state.config, &runtime).map_err(ControlError::Internal)?;
        append_control_event(&state.config, if pause { "pause" } else { "resume" }, &key)
            .map_err(ControlError::Internal)?;
    }
    reconcile_and_schedule(&state).await;
    let sequence = state.snapshot.read().await.sequence;
    Ok(Json(ControlResponse {
        ok: true,
        action: if pause { "pause" } else { "resume" }.to_string(),
        target: format!("{kind}/{id}"),
        message: format!("sent signal {signal} to pid {pid}"),
        snapshot_sequence: sequence,
    }))
}

fn control_job_key(kind: &str, id: &str) -> Result<String, ControlError> {
    match kind {
        "car_download" => id
            .parse::<u64>()
            .map(|epoch| format!("download:{epoch}"))
            .map_err(|_| ControlError::BadRequest("download id must be an epoch".to_string())),
        "car_preflight" => id
            .parse::<u64>()
            .map(|epoch| format!("preflight:{epoch}"))
            .map_err(|_| ControlError::BadRequest("preflight id must be an epoch".to_string())),
        "historical_scan" => id
            .parse::<u64>()
            .map(|epoch| format!("scan:{epoch}"))
            .map_err(|_| ControlError::BadRequest("scan id must be an epoch".to_string())),
        "historical_finalizer" => id
            .parse::<u64>()
            .map(|epoch| format!("finalize:{epoch}"))
            .map_err(|_| ControlError::BadRequest("finalizer id must be an epoch".to_string())),
        "live_finalizer" => Ok(format!("live:{id}")),
        _ => Err(ControlError::BadRequest(format!(
            "unsupported job kind {kind}"
        ))),
    }
}

async fn controlled_job_pid(
    state: &Arc<AppState>,
    snapshot: &PipelineSnapshot,
    kind: &str,
    id: &str,
    key: &str,
) -> Result<(u32, PathBuf), ControlError> {
    let runtime = state.runtime.lock().await;
    let runtime_pid = runtime
        .acquisitions
        .values()
        .chain(runtime.scans.values())
        .chain(runtime.finalizer.iter())
        .find(|child| child.kind.key() == key)
        .and_then(|child| child.pid);
    drop(runtime);
    match kind {
        "car_download" | "car_preflight" => {
            let epoch = id.parse::<u64>().map_err(|_| {
                ControlError::BadRequest("acquisition id must be an epoch".to_string())
            })?;
            let marker = active_acquisition_marker(&state.config, epoch).ok_or_else(|| {
                ControlError::NotFound(format!("no active acquisition for epoch {epoch}"))
            })?;
            let pid = runtime_pid.unwrap_or(marker.pid);
            Ok((pid, marker.expected_path))
        }
        "historical_scan" | "historical_finalizer" => {
            let epoch = id.parse::<u64>().map_err(|_| {
                ControlError::BadRequest("historical id must be an epoch".to_string())
            })?;
            let epoch_state = snapshot
                .epochs
                .iter()
                .find(|candidate| candidate.epoch == epoch)
                .ok_or_else(|| ControlError::NotFound(format!("epoch {epoch} is not tracked")))?;
            let pid = runtime_pid
                .or(epoch_state.progress.pid)
                .ok_or_else(|| ControlError::NotFound(format!("no active pid for {kind}/{id}")))?;
            Ok((pid, epoch_state.output_path.clone()))
        }
        "live_finalizer" => {
            let capture = snapshot
                .live
                .iter()
                .find(|capture| capture.id == id)
                .ok_or_else(|| ControlError::NotFound(format!("capture {id} is not tracked")))?;
            let lane_pid = snapshot
                .lanes
                .iter()
                .find(|lane| lane.kind == kind && lane.capture_id.as_deref() == Some(id))
                .and_then(|lane| lane.pid);
            let pid = runtime_pid
                .or(lane_pid)
                .ok_or_else(|| ControlError::NotFound(format!("no active pid for {kind}/{id}")))?;
            let output = capture.output_path.clone().ok_or_else(|| {
                ControlError::Conflict("live finalizer has no target epoch".to_string())
            })?;
            Ok((pid, output))
        }
        _ => Err(ControlError::BadRequest(format!(
            "unsupported job kind {kind}"
        ))),
    }
}

fn validate_process_identity(
    pid: u32,
    blockzilla_bin: &Path,
    expected_path: &Path,
    kind: &str,
) -> Result<(), ControlError> {
    let bytes = fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline"))
        .map_err(|_| ControlError::NotFound(format!("pid {pid} is not running")))?;
    if !argv_matches_job(&bytes, blockzilla_bin, expected_path, kind) {
        return Err(ControlError::Conflict(format!(
            "pid {pid} command does not match expected {kind} pipeline target"
        )));
    }
    Ok(())
}

fn argv_matches_job(bytes: &[u8], blockzilla_bin: &Path, expected_path: &Path, kind: &str) -> bool {
    let args = bytes
        .split(|byte| *byte == 0)
        .filter(|arg| !arg.is_empty())
        .collect::<Vec<_>>();
    let expected_path = expected_path.as_os_str().as_bytes();
    let has_expected_path = args.iter().any(|arg| {
        *arg == expected_path
            || arg
                .strip_prefix(expected_path)
                .is_some_and(|suffix| suffix.first() == Some(&b'/'))
    });
    if !has_expected_path {
        return false;
    }
    if kind == "car_download" {
        return args.first().is_some_and(|arg| arg.ends_with(b"/sh"))
            && args.iter().any(|arg| *arg == b"hivezilla-car-download");
    }
    if args.first().copied() != Some(blockzilla_bin.as_os_str().as_bytes()) {
        return false;
    }
    match kind {
        "car_preflight" => args.get(1).copied() == Some(b"preflight-car"),
        "historical_scan" => {
            args.get(1).copied() == Some(b"build-archive-v2-hot-blocks")
                && args.iter().any(|arg| *arg == b"--first-seen-scan-only")
        }
        "historical_finalizer" => args.get(1).copied() == Some(b"finalize-archive-v2-first-seen"),
        "live_finalizer" => matches!(
            args.get(1).copied(),
            Some(b"prepare-archive-v2-live-registry")
                | Some(b"build-archive-v2-registry-index")
                | Some(b"build-archive-v2-hot-blocks-from-live")
        ),
        _ => false,
    }
}

fn empty_snapshot(observer_mode: bool) -> PipelineSnapshot {
    PipelineSnapshot {
        schema_version: STATUS_SCHEMA_VERSION,
        sequence: 0,
        now_unix_secs: unix_now(),
        current_epoch: None,
        observer_mode,
        capabilities: CapabilitySnapshot::default(),
        scheduler: SchedulerSnapshot::default(),
        inventory: InventorySnapshot::default(),
        scan_sweep: ScanSweepSnapshot::default(),
        summary: PipelineSummary::default(),
        machine: MachineSnapshot::default(),
        epochs: Vec::new(),
        lanes: Vec::new(),
        live: Vec::new(),
        finalizer_queue: Vec::new(),
        errors: Vec::new(),
    }
}

async fn reconcile_and_schedule(state: &Arc<AppState>) {
    let mut runtime = state.runtime.lock().await;
    reap_children(state, &mut runtime).await;
    reconcile_acquisition_state(&state.config, &mut runtime);
    runtime.inventory_generation = runtime.inventory_generation.saturating_add(1);
    let inventory_generation = runtime.inventory_generation;

    let mut snapshot = reconcile_filesystem(&state.config, &runtime, inventory_generation);
    if state.config.execute {
        if let Err(error) = schedule_work(&state.config, &snapshot, &mut runtime).await {
            record_error(
                &state.config,
                &mut runtime,
                "scheduler",
                format!("{error:#}"),
            );
        }
        snapshot = reconcile_filesystem(&state.config, &runtime, inventory_generation);
    }
    snapshot.errors = runtime.errors.iter().cloned().collect();
    let status_bytes = {
        let _publication = state.publication.lock().await;
        let mut published = state.snapshot.write().await;
        preserve_newer_published_progress(&mut snapshot, &published);
        sample_worker_disk_io(&mut snapshot, &mut runtime);
        sample_archive_device_disk_io(
            &state.config.archive_root,
            &mut snapshot.machine,
            &mut runtime,
        );
        let sequence = state.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        snapshot.sequence = sequence;
        let status_bytes =
            serde_json::to_vec_pretty(&snapshot).context("serialize pipeline status");
        let patch = SnapshotPatch::between(&published, &snapshot);
        *published = snapshot;
        let _ = state.updates.send(RealtimeEnvelope {
            event_type: "snapshot_patch",
            sequence,
            data: patch,
        });
        status_bytes
    };
    let persisted =
        status_bytes.and_then(|bytes| persist_snapshot_bytes(&state.config.state_root, bytes));
    if let Err(error) = persisted {
        record_error(
            &state.config,
            &mut runtime,
            "state",
            format!("persist status: {error:#}"),
        );
    }
}

async fn monitor_active_progress(state: &Arc<AppState>) {
    let targets = {
        let snapshot = state.snapshot.read().await;
        collect_active_progress_targets(&state.config, &snapshot)
    };
    if targets.lanes.is_empty() && targets.live.is_empty() {
        return;
    }

    // These are bounded progress JSON reads and one bounded journal-tail read
    // per active capture. They deliberately happen without holding either the
    // scheduler/runtime lock or the published-snapshot lock.
    let now = unix_now();
    let lane_updates = targets
        .lanes
        .iter()
        .filter_map(|target| read_lane_progress_update(target, now))
        .collect::<Vec<_>>();
    let live_updates = targets
        .live
        .iter()
        .filter_map(|target| read_live_progress_update(target, now))
        .collect::<Vec<_>>();
    if lane_updates.is_empty() && live_updates.is_empty() {
        return;
    }

    publish_monitored_progress(state, lane_updates, live_updates, now).await;
}

async fn publish_monitored_progress(
    state: &Arc<AppState>,
    lane_updates: Vec<LaneProgressUpdate>,
    live_updates: Vec<LiveProgressUpdate>,
    now: u64,
) -> bool {
    // Publication is the only serialized section. Progress reads never wait
    // for the scheduler/runtime lock or the full filesystem reconciliation.
    let _publication = state.publication.lock().await;
    let mut snapshot = state.snapshot.write().await;
    let Some(epochs_changed) =
        apply_active_progress_updates(&mut snapshot, &lane_updates, &live_updates, now)
    else {
        return false;
    };
    let sequence = state.sequence.fetch_add(1, Ordering::Relaxed) + 1;
    snapshot.sequence = sequence;
    snapshot.now_unix_secs = snapshot.now_unix_secs.max(now);
    let patch = SnapshotPatch::active_progress(&snapshot, epochs_changed);
    let _ = state.updates.send(RealtimeEnvelope {
        event_type: "snapshot_patch",
        sequence,
        data: patch,
    });
    true
}

fn collect_active_progress_targets(
    config: &NasPipelineConfig,
    snapshot: &PipelineSnapshot,
) -> ActiveProgressTargets {
    let lanes = snapshot
        .lanes
        .iter()
        .filter(|lane| lane.state == "running")
        .filter_map(|lane| {
            let paths = lane_progress_paths(config, lane);
            (!paths.is_empty()).then(|| LaneProgressTarget {
                id: lane.id.clone(),
                paths,
                baseline: lane.progress.clone(),
                baseline_identity: lane_progress_worker_identity(lane),
            })
        })
        .collect();
    let live = snapshot
        .live
        .iter()
        .filter(|capture| {
            matches!(capture.state, LiveState::Capturing | LiveState::Packaging)
                && capture.superseded_by.is_none()
        })
        .map(|capture| LiveProgressTarget {
            id: capture.id.clone(),
            state: capture.state,
            capture_dir: capture.capture_dir.clone(),
            paths: live_progress_paths(config, capture),
            baseline: capture.progress.clone(),
            baseline_identity: live_progress_worker_identity(capture),
        })
        .collect();
    ActiveProgressTargets { lanes, live }
}

fn lane_progress_worker_identity(lane: &LaneSnapshot) -> ProgressWorkerIdentity {
    ProgressWorkerIdentity {
        pid: lane.pid.or(lane.progress.pid),
        phase: lane
            .progress
            .phase
            .clone()
            .or_else(|| (!lane.phase.is_empty()).then(|| lane.phase.clone())),
        started_unix_secs: lane.started_unix_secs,
    }
}

fn live_progress_worker_identity(capture: &LiveCaptureSnapshot) -> ProgressWorkerIdentity {
    ProgressWorkerIdentity {
        pid: capture.progress.pid,
        phase: capture.progress.phase.clone(),
        started_unix_secs: None,
    }
}

fn lane_progress_paths(config: &NasPipelineConfig, lane: &LaneSnapshot) -> Vec<PathBuf> {
    match (lane.kind.as_str(), lane.epoch) {
        ("car_download", Some(epoch)) => vec![
            config
                .state_root
                .join("progress")
                .join(format!("epoch-{epoch}-download.json")),
        ],
        ("car_preflight", Some(epoch)) => vec![
            config
                .state_root
                .join("progress")
                .join(format!("epoch-{epoch}-preflight.json")),
        ],
        ("historical_scan" | "historical_finalizer" | "historical_compact_reuse", Some(epoch)) => {
            vec![historical_progress_path(&config.state_root, epoch)]
        }
        ("live_finalizer", _) => lane.capture_id.as_ref().map_or_else(Vec::new, |id| {
            vec![
                config
                    .state_root
                    .join("progress")
                    .join(format!("live-{}-package.json", safe_segment(id))),
            ]
        }),
        _ => Vec::new(),
    }
}

fn live_progress_paths(config: &NasPipelineConfig, capture: &LiveCaptureSnapshot) -> Vec<PathBuf> {
    let mut paths = vec![
        capture.capture_dir.join("progress.json"),
        capture.capture_dir.join("journal/progress.json"),
    ];
    if capture.state == LiveState::Packaging {
        paths.push(
            config
                .state_root
                .join("progress")
                .join(format!("live-{}-package.json", safe_segment(&capture.id))),
        );
        if let Some(epoch) = capture.epoch {
            paths.push(repair_materialization_progress_path(config, epoch));
            paths.push(repair_hot_progress_path(config, epoch));
        }
    }
    paths
}

fn read_freshest_monitored_progress(paths: &[PathBuf]) -> Option<ProgressSnapshot> {
    paths
        .iter()
        .filter_map(|path| {
            read_progress(path).map(|mut progress| {
                let updated = progress
                    .updated_unix_secs
                    .or_else(|| modified_unix_secs(path))
                    .unwrap_or_default();
                if progress.updated_unix_secs.is_none() && updated > 0 {
                    progress.updated_unix_secs = Some(updated);
                }
                (updated, progress.blocks_done, progress)
            })
        })
        .max_by_key(|(updated, blocks, _)| (*updated, *blocks))
        .map(|(_, _, progress)| progress)
}

fn merge_monitored_process_metrics(
    mut progress: ProgressSnapshot,
    baseline: &ProgressSnapshot,
) -> ProgressSnapshot {
    progress.phase = progress.phase.or_else(|| baseline.phase.clone());
    progress.pid = progress.pid.or(baseline.pid);
    progress.rss_bytes = progress.rss_bytes.or_else(|| {
        progress
            .pid
            .and_then(process_tree_rss_bytes)
            .or(baseline.rss_bytes)
    });
    progress.peak_rss_bytes = progress
        .peak_rss_bytes
        .or_else(|| progress.pid.and_then(process_peak_rss_bytes))
        .or(baseline.peak_rss_bytes)
        .map(|peak| peak.max(progress.rss_bytes.unwrap_or(0)));
    // These rates are sampled by the controller, not written to job progress
    // JSON. Preserve them across the faster progress-monitor publications.
    progress.disk_read_mib_per_sec = baseline.disk_read_mib_per_sec;
    progress.disk_write_mib_per_sec = baseline.disk_write_mib_per_sec;
    progress
}

fn read_lane_progress_update(target: &LaneProgressTarget, now: u64) -> Option<LaneProgressUpdate> {
    let mut progress = merge_monitored_process_metrics(
        read_freshest_monitored_progress(&target.paths)?,
        &target.baseline,
    );
    hide_stale_lane_rates(&mut progress, now);
    progress_source_changed(&target.baseline, &progress).then(|| LaneProgressUpdate {
        id: target.id.clone(),
        baseline: target.baseline.clone(),
        baseline_identity: target.baseline_identity.clone(),
        progress,
    })
}

fn read_live_progress_update(target: &LiveProgressTarget, now: u64) -> Option<LiveProgressUpdate> {
    let source = read_freshest_monitored_progress(&target.paths);
    let source_updated = source
        .as_ref()
        .and_then(|progress| progress.updated_unix_secs)
        .unwrap_or_default();
    let explicit_slot_rate = source.as_ref().and_then(|progress| progress.slots_per_sec);
    let baseline_updated = target.baseline.updated_unix_secs.unwrap_or_default();
    let mut progress = match source {
        Some(progress) if source_updated >= baseline_updated => progress,
        _ => target.baseline.clone(),
    };
    progress = merge_monitored_process_metrics(progress, &target.baseline);

    if target.state == LiveState::Capturing {
        let journal_path = target.capture_dir.join("journal/grpc-blocks.jsonl");
        let journal = read_live_journal_tail(&journal_path);
        let journal_updated = journal
            .as_ref()
            .and_then(|_| modified_unix_secs(&journal_path))
            .unwrap_or_default();
        merge_live_journal_progress(
            &mut progress,
            journal.as_ref(),
            (journal_updated > 0).then_some(journal_updated),
        );
        let latest_updated = progress.updated_unix_secs.unwrap_or_default();
        if source_updated >= journal_updated {
            if let Some(rate) = explicit_slot_rate {
                progress.slots_per_sec = Some(rate);
            }
        } else if latest_updated > baseline_updated {
            progress.slots_per_sec =
                target
                    .baseline
                    .last_slot
                    .zip(progress.last_slot)
                    .map(|(previous, current)| {
                        current.saturating_sub(previous) as f64
                            / latest_updated.saturating_sub(baseline_updated).max(1) as f64
                    });
        }
        refresh_live_epoch_metrics(&mut progress);
        refresh_live_producer_process_metrics(&mut progress, &target.capture_dir, now);
    }
    hide_stale_live_rates(&mut progress, now);
    progress_source_changed(&target.baseline, &progress).then(|| LiveProgressUpdate {
        id: target.id.clone(),
        baseline: target.baseline.clone(),
        baseline_identity: target.baseline_identity.clone(),
        progress,
    })
}

fn progress_source_changed(current: &ProgressSnapshot, candidate: &ProgressSnapshot) -> bool {
    let current_updated = current.updated_unix_secs.unwrap_or_default();
    let candidate_updated = candidate.updated_unix_secs.unwrap_or_default();
    if candidate_updated < current_updated || candidate == current {
        return false;
    }
    if candidate.phase != current.phase {
        return candidate.phase.is_some() && candidate_updated > current_updated;
    }
    candidate.blocks_done >= current.blocks_done
        && optional_counter_does_not_regress(current.last_slot, candidate.last_slot)
}

fn optional_counter_does_not_regress(current: Option<u64>, candidate: Option<u64>) -> bool {
    match (current, candidate) {
        (Some(current), Some(candidate)) => candidate >= current,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

fn monitored_progress_can_apply(
    current: &ProgressSnapshot,
    baseline: &ProgressSnapshot,
    baseline_identity: &ProgressWorkerIdentity,
    current_identity: &ProgressWorkerIdentity,
    candidate: &ProgressSnapshot,
) -> bool {
    if !worker_identity_matches(baseline_identity, current_identity) {
        return false;
    }
    if !progress_source_changed(current, candidate) {
        return false;
    }
    if current == baseline {
        return true;
    }

    let current_updated = current.updated_unix_secs.unwrap_or_default();
    let candidate_updated = candidate.updated_unix_secs.unwrap_or_default();
    candidate_updated > current_updated
        || candidate.blocks_done > current.blocks_done
        || matches!(
            (current.last_slot, candidate.last_slot),
            (Some(current), Some(candidate)) if candidate > current
        )
}

fn worker_identity_matches(
    baseline: &ProgressWorkerIdentity,
    current: &ProgressWorkerIdentity,
) -> bool {
    baseline.pid == current.pid
        && baseline.phase == current.phase
        && baseline.started_unix_secs == current.started_unix_secs
}

fn apply_active_progress_updates(
    snapshot: &mut PipelineSnapshot,
    lane_updates: &[LaneProgressUpdate],
    live_updates: &[LiveProgressUpdate],
    now: u64,
) -> Option<Vec<EpochSnapshot>> {
    let mut changed = false;
    let mut changed_epochs = BTreeMap::new();

    for update in lane_updates {
        let Some(lane) = snapshot.lanes.iter_mut().find(|lane| lane.id == update.id) else {
            continue;
        };
        let current_identity = lane_progress_worker_identity(lane);
        if lane.state != "running"
            || !monitored_progress_can_apply(
                &lane.progress,
                &update.baseline,
                &update.baseline_identity,
                &current_identity,
                &update.progress,
            )
        {
            continue;
        }
        let mut progress = update.progress.clone();
        preserve_controller_disk_rates(&mut progress, &lane.progress);
        apply_progress_to_lane(lane, progress);
        changed = true;

        if matches!(
            lane.kind.as_str(),
            "historical_scan" | "historical_finalizer" | "historical_compact_reuse"
        ) && let Some(epoch_number) = lane.epoch
            && let Ok(epoch_index) = snapshot
                .epochs
                .binary_search_by_key(&epoch_number, |epoch| epoch.epoch)
            && matches!(
                snapshot.epochs[epoch_index].state,
                HistoricalState::Scanning | HistoricalState::Finalizing
            )
        {
            let epoch = &mut snapshot.epochs[epoch_index];
            epoch.progress = lane.progress.clone();
            epoch.updated_unix_secs = lane.updated_unix_secs;
            changed_epochs.insert(epoch_number, epoch.clone());
        }

        if lane.kind == "live_finalizer"
            && let Some(capture_id) = lane.capture_id.as_deref()
            && let Some(capture) = snapshot
                .live
                .iter_mut()
                .find(|capture| capture.id == capture_id && capture.state == LiveState::Packaging)
        {
            let mut progress = lane.progress.clone();
            preserve_controller_disk_rates(&mut progress, &capture.progress);
            apply_progress_to_live_capture(capture, progress);
        }
    }

    for update in live_updates {
        let Some(capture_index) = snapshot
            .live
            .iter()
            .position(|capture| capture.id == update.id)
        else {
            continue;
        };
        let capture = &snapshot.live[capture_index];
        let current_identity = live_progress_worker_identity(capture);
        if !matches!(capture.state, LiveState::Capturing | LiveState::Packaging)
            || !monitored_progress_can_apply(
                &capture.progress,
                &update.baseline,
                &update.baseline_identity,
                &current_identity,
                &update.progress,
            )
        {
            continue;
        }
        let packaging = capture.state == LiveState::Packaging;
        let mut progress = update.progress.clone();
        preserve_controller_disk_rates(&mut progress, &capture.progress);
        apply_progress_to_live_capture(&mut snapshot.live[capture_index], progress);
        if packaging {
            let progress = snapshot.live[capture_index].progress.clone();
            sync_packaging_lane(&mut snapshot.lanes, &update.id, &progress);
        }
        changed = true;
    }

    if !changed {
        return None;
    }
    snapshot.summary.blocks_per_sec = active_block_processing_rate(&snapshot.lanes, now);
    Some(changed_epochs.into_values().collect())
}

fn preserve_controller_disk_rates(candidate: &mut ProgressSnapshot, current: &ProgressSnapshot) {
    candidate.disk_read_mib_per_sec = current.disk_read_mib_per_sec;
    candidate.disk_write_mib_per_sec = current.disk_write_mib_per_sec;
}

fn apply_progress_to_lane(lane: &mut LaneSnapshot, progress: ProgressSnapshot) {
    lane.pid = progress.pid;
    lane.rss_bytes = progress.rss_bytes;
    if let Some(phase) = progress.phase.clone() {
        lane.phase = phase;
    }
    lane.updated_unix_secs = progress.updated_unix_secs.unwrap_or(lane.updated_unix_secs);
    lane.progress = progress;
}

fn sync_packaging_lane(lanes: &mut [LaneSnapshot], capture_id: &str, progress: &ProgressSnapshot) {
    let Some(lane) = lanes.iter_mut().find(|lane| {
        lane.kind == "live_finalizer"
            && lane.capture_id.as_deref() == Some(capture_id)
            && lane.state == "running"
    }) else {
        return;
    };
    let aliases_differ = lane.progress == *progress
        && (lane.pid != progress.pid
            || lane.rss_bytes != progress.rss_bytes
            || progress
                .phase
                .as_ref()
                .is_some_and(|phase| phase != &lane.phase)
            || progress.updated_unix_secs != Some(lane.updated_unix_secs));
    if progress_source_changed(&lane.progress, progress) || aliases_differ {
        let mut progress = progress.clone();
        preserve_controller_disk_rates(&mut progress, &lane.progress);
        apply_progress_to_lane(lane, progress);
    }
}

fn apply_progress_to_live_capture(
    capture: &mut LiveCaptureSnapshot,
    mut progress: ProgressSnapshot,
) {
    progress.first_slot = match (capture.first_slot, progress.first_slot) {
        (Some(current), Some(candidate)) => Some(current.min(candidate)),
        (current, candidate) => current.or(candidate),
    };
    progress.last_slot = match (capture.last_slot, progress.last_slot) {
        (Some(current), Some(candidate)) => Some(current.max(candidate)),
        (current, candidate) => current.or(candidate),
    };
    if capture.state == LiveState::Capturing {
        capture.blocks_written = capture.blocks_written.max(progress.blocks_done);
        progress.blocks_done = capture.blocks_written;
    }
    capture.first_slot = progress.first_slot;
    capture.last_slot = progress.last_slot;
    capture.updated_unix_secs = progress
        .updated_unix_secs
        .unwrap_or(capture.updated_unix_secs);
    capture.eta_secs = progress.eta_secs;
    capture.slots_per_sec = (capture.state == LiveState::Capturing)
        .then_some(progress.slots_per_sec)
        .flatten();
    capture.rss_bytes = progress.rss_bytes;
    capture.peak_rss_bytes = progress.peak_rss_bytes;
    capture.progress = progress;
}

fn preserve_newer_published_progress(next: &mut PipelineSnapshot, published: &PipelineSnapshot) {
    next.now_unix_secs = next.now_unix_secs.max(published.now_unix_secs);
    let mut historical_updates = Vec::new();
    for lane in next.lanes.iter_mut().filter(|lane| lane.state == "running") {
        let Some(current) = published
            .lanes
            .iter()
            .find(|current| current.id == lane.id && current.state == "running")
        else {
            continue;
        };
        if lane.started_unix_secs != current.started_unix_secs
            || lane.pid != current.pid
            || !progress_source_changed(&lane.progress, &current.progress)
        {
            continue;
        }
        apply_progress_to_lane(lane, current.progress.clone());
        if matches!(
            lane.kind.as_str(),
            "historical_scan" | "historical_finalizer" | "historical_compact_reuse"
        ) && let Some(epoch) = lane.epoch
        {
            historical_updates.push((epoch, lane.progress.clone(), lane.updated_unix_secs));
        }
    }
    for (epoch_number, progress, updated) in historical_updates {
        if let Ok(index) = next
            .epochs
            .binary_search_by_key(&epoch_number, |epoch| epoch.epoch)
            && matches!(
                next.epochs[index].state,
                HistoricalState::Scanning | HistoricalState::Finalizing
            )
        {
            next.epochs[index].progress = progress;
            next.epochs[index].updated_unix_secs = updated;
        }
    }

    let mut packaging_updates = Vec::new();
    for capture in next
        .live
        .iter_mut()
        .filter(|capture| matches!(capture.state, LiveState::Capturing | LiveState::Packaging))
    {
        let Some(current) = published
            .live
            .iter()
            .find(|current| current.id == capture.id && current.state == capture.state)
        else {
            continue;
        };
        if capture.progress.pid != current.progress.pid
            || !progress_source_changed(&capture.progress, &current.progress)
        {
            continue;
        }
        apply_progress_to_live_capture(capture, current.progress.clone());
        if capture.state == LiveState::Packaging {
            packaging_updates.push((capture.id.clone(), capture.progress.clone()));
        }
    }
    for (capture_id, progress) in packaging_updates {
        sync_packaging_lane(&mut next.lanes, &capture_id, &progress);
    }
    next.summary.blocks_per_sec = active_block_processing_rate(&next.lanes, next.now_unix_secs);
}

fn reconcile_filesystem(
    config: &NasPipelineConfig,
    runtime: &RuntimeState,
    inventory_generation: u64,
) -> PipelineSnapshot {
    let now = unix_now();
    let discovery = discover_inventory(config);
    let mut classified_live = discovery
        .live_paths
        .iter()
        .cloned()
        .map(|path| classify_live_capture(config, runtime, path, now))
        .collect::<Vec<_>>();
    apply_repair_supersession(&mut classified_live);
    let (live, current_epoch) = canonicalize_live_captures(classified_live);
    let live_epochs = live
        .iter()
        .filter_map(|capture| capture.epoch)
        .collect::<BTreeSet<_>>();
    // A live capture is the authoritative workflow for its epoch. Keeping the
    // same epoch in the historical census would turn a healthy active capture
    // (or a closed capture waiting for packaging) into a second, red
    // "input CAR is missing" row and could make the historical scheduler try
    // to acquire/scan it. The dashboard can merge `epochs` and `live` for a
    // unified timeline without duplicating scheduling ownership.
    let epochs = discovery
        .epochs
        .iter()
        .copied()
        .filter(|epoch| !live_epochs.contains(epoch))
        .map(|epoch| classify_epoch_with_context(config, runtime, epoch, now, true))
        .collect::<Vec<_>>();
    let mut lanes = runtime_lanes(runtime);
    for epoch in &epochs {
        if runtime.acquisitions.contains_key(&epoch.epoch) {
            continue;
        }
        let Some(marker) = active_acquisition_marker(config, epoch.epoch) else {
            continue;
        };
        let id = format!(
            "{}:{}",
            marker.kind.strip_prefix("car_").unwrap_or(&marker.kind),
            epoch.epoch
        );
        if lanes.iter().any(|lane| lane.id == id) {
            continue;
        }
        let rss_bytes = process_tree_rss_bytes(marker.pid);
        let lane_state = if runtime.paused_jobs.contains(&id) {
            "paused"
        } else {
            "running"
        };
        lanes.push(LaneSnapshot {
            id,
            kind: marker.kind.clone(),
            epoch: Some(epoch.epoch),
            capture_id: None,
            phase: marker
                .kind
                .strip_prefix("car_")
                .unwrap_or(&marker.kind)
                .to_string(),
            state: lane_state.to_string(),
            pid: Some(marker.pid),
            progress: ProgressSnapshot {
                phase: Some(marker.kind.clone()),
                state: Some(lane_state.to_string()),
                pid: Some(marker.pid),
                rss_bytes,
                ..ProgressSnapshot::default()
            },
            rss_bytes,
            started_unix_secs: None,
            updated_unix_secs: marker.updated_unix_secs,
        });
    }
    for epoch in epochs
        .iter()
        .filter(|epoch| epoch.state == HistoricalState::Scanning)
    {
        let id = format!("scan:{}", epoch.epoch);
        if lanes.iter().any(|lane| lane.id == id) {
            continue;
        }
        lanes.push(LaneSnapshot {
            id: id.clone(),
            kind: "historical_scan".to_string(),
            epoch: Some(epoch.epoch),
            capture_id: None,
            phase: epoch
                .progress
                .phase
                .clone()
                .unwrap_or_else(|| "scan".to_string()),
            state: if runtime.paused_jobs.contains(&id) {
                "paused".to_string()
            } else {
                "running".to_string()
            },
            pid: epoch.progress.pid,
            progress: epoch.progress.clone(),
            rss_bytes: epoch.progress.rss_bytes,
            started_unix_secs: epoch.progress.updated_unix_secs.and_then(|updated| {
                epoch
                    .progress
                    .elapsed_secs
                    .map(|elapsed| updated.saturating_sub(elapsed as u64))
            }),
            updated_unix_secs: epoch.updated_unix_secs,
        });
    }
    for epoch in epochs
        .iter()
        .filter(|epoch| epoch.state == HistoricalState::Finalizing)
    {
        let id = format!("finalize:{}", epoch.epoch);
        if lanes.iter().any(|lane| lane.id == id) {
            continue;
        }
        lanes.push(LaneSnapshot {
            id: id.clone(),
            kind: "historical_finalizer".to_string(),
            epoch: Some(epoch.epoch),
            capture_id: None,
            phase: "finalize".to_string(),
            state: if runtime.paused_jobs.contains(&id) {
                "paused".to_string()
            } else {
                "running".to_string()
            },
            pid: epoch.progress.pid,
            progress: epoch.progress.clone(),
            rss_bytes: epoch.progress.rss_bytes,
            started_unix_secs: None,
            updated_unix_secs: epoch.updated_unix_secs,
        });
    }
    for capture in live
        .iter()
        .filter(|capture| capture.state == LiveState::Packaging)
    {
        let id = format!("live:{}", capture.id);
        if lanes.iter().any(|lane| lane.id == id) {
            continue;
        }
        lanes.push(LaneSnapshot {
            id: id.clone(),
            kind: "live_finalizer".to_string(),
            epoch: capture.epoch,
            capture_id: Some(capture.id.clone()),
            phase: "package".to_string(),
            state: if runtime.paused_jobs.contains(&id) {
                "paused".to_string()
            } else {
                "running".to_string()
            },
            pid: capture.progress.pid,
            progress: capture.progress.clone(),
            rss_bytes: capture.progress.rss_bytes,
            started_unix_secs: None,
            updated_unix_secs: capture.updated_unix_secs,
        });
    }
    let mut finalizer_queue = epochs
        .iter()
        .filter(|epoch| epoch.state == HistoricalState::ScanReady)
        .map(|epoch| FinalizerQueueItem {
            kind: "historical".to_string(),
            epoch: Some(epoch.epoch),
            id: format!("epoch-{}", epoch.epoch),
            phase: "mphf_build".to_string(),
            state: "scan_ready".to_string(),
            estimated_memory_bytes: estimate_mphf_build_bytes(
                config,
                file_len(&epoch.output_path.join(REGISTRY_FILE)),
            ),
            estimated_disk_bytes: estimate_finalizer_scratch_bytes(file_len(
                &epoch.output_path.join(REGISTRY_FILE),
            )),
            deferred_reason: None,
        })
        .collect::<Vec<_>>();
    finalizer_queue.extend(
        live.iter()
            .filter(|capture| {
                capture.state == LiveState::ReadyToPackage && capture.superseded_by.is_none()
            })
            .filter_map(|capture| live_finalizer_queue_item(config, capture)),
    );
    finalizer_queue.sort_by_key(|item| {
        let kind_priority = if item.kind == "live" { 0 } else { 1 };
        let epoch_priority = if item.kind == "live" {
            (0, item.epoch.unwrap_or(u64::MAX))
        } else {
            item.epoch
                .map(|epoch| historical_schedule_priority_key(config, epoch))
                .unwrap_or((u8::MAX, u64::MAX))
        };
        (kind_priority, epoch_priority, item.id.clone())
    });
    let children_rss_bytes = lanes.iter().filter_map(|lane| lane.rss_bytes).sum();
    let acquisition_rss_bytes = lanes
        .iter()
        .filter(|lane| matches!(lane.kind.as_str(), "car_download" | "car_preflight"))
        .filter_map(|lane| lane.rss_bytes)
        .sum();
    let machine = machine_snapshot(&config.archive_root, &config.car_root, children_rss_bytes);
    let admission = admission_snapshot(config, &machine, &epochs);
    let mut summary = summarize_epochs(&epochs);
    summary.blocks_per_sec = active_block_processing_rate(&lanes, now);
    summary.scan_capacity_configured = config.scan_concurrency;
    summary.scan_capacity_admitted = admission.scan_capacity;
    summary.admission_blocked_reason = admission.blocked_reason.clone();
    let scan_pending = epochs
        .iter()
        .filter(|epoch| {
            epoch.state == HistoricalState::Queued
                && !runtime.acquisitions.contains_key(&epoch.epoch)
                && !acquisition_claim_active(config, epoch.epoch)
        })
        .count();
    let adopted_acquisitions = epochs
        .iter()
        .filter(|epoch| {
            !runtime.acquisitions.contains_key(&epoch.epoch)
                && acquisition_claim_active(config, epoch.epoch)
        })
        .count();
    let scan_active = runtime.acquisitions.len()
        + adopted_acquisitions
        + active_scan_count(&epochs, runtime.scans.keys().copied());
    let terminal_gaps = epochs
        .iter()
        .filter(|epoch| {
            matches!(
                epoch.state,
                HistoricalState::Blocked | HistoricalState::Failed
            )
        })
        .count();
    let sweep_complete = discovery.errors.is_empty() && scan_pending == 0 && scan_active == 0;
    let deferred_finalizers = finalizer_queue
        .iter()
        .filter(|item| item.kind == "historical")
        .count();
    let download_pending = epochs
        .iter()
        .any(|epoch| epoch.state == HistoricalState::Queued && epoch.input_path.is_none());
    let active_download_disk_bytes = active_download_projection(config, &epochs, runtime);
    let next_download_disk_bytes = prioritized_epochs(config, &epochs)
        .into_iter()
        .find(|epoch| acquisition_action(config, epoch) == Some(AcquisitionAction::Download))
        .map(|epoch| car_download_remaining_projection(config, &epochs, epoch.epoch))
        .unwrap_or(0);
    let acquisition_pending = epochs
        .iter()
        .any(|epoch| acquisition_action(config, epoch).is_some());
    let sweep_progress_reason = || {
        format!(
            "historical scan sweep in progress; historical finalizers are deferred: pending={scan_pending} active={scan_active}"
        )
    };
    let (sweep_wait_reason, sweep_blocked_reason) = if !discovery.errors.is_empty() {
        (
            None,
            Some("inventory is incomplete; no new work will be scheduled".to_string()),
        )
    } else if download_pending {
        let disk_blocker = car_disk_admission_blocked_reason(
            config,
            &machine,
            active_download_disk_bytes.saturating_add(next_download_disk_bytes),
        );
        match disk_blocker {
            Some(reason) => (None, Some(reason)),
            None => (Some(sweep_progress_reason()), None),
        }
    } else if acquisition_pending
        && acquisition_memory_capacity(
            config,
            &machine,
            runtime
                .acquisitions
                .len()
                .saturating_add(adopted_acquisitions),
            acquisition_rss_bytes,
        ) == 0
    {
        (
            None,
            Some(format!(
                "CAR preflight admission blocked: available {:.1} MiB, reserve {} MiB, task budget {} MiB",
                machine.memory_available_bytes as f64 / 1024f64.powi(2),
                config.memory_reserve_mib,
                PREFLIGHT_MEMORY_MIB,
            )),
        )
    } else if scan_pending > 0 && admission.scan_capacity == 0 {
        (
            None,
            admission.blocked_reason.clone().or_else(|| {
                Some("historical scan admission is blocked by resource limits".to_string())
            }),
        )
    } else if scan_pending > 0 || scan_active > 0 {
        (Some(sweep_progress_reason()), None)
    } else {
        (None, None)
    };
    let sweep_deferred_reason = sweep_blocked_reason
        .clone()
        .or_else(|| sweep_wait_reason.clone());
    if !sweep_complete {
        for item in &mut finalizer_queue {
            if item.kind == "historical" {
                item.deferred_reason = sweep_deferred_reason.clone();
            }
        }
    }
    summary.finalizer_admission_blocked_reason = if finalizer_queue
        .first()
        .is_some_and(|item| item.kind == "live")
    {
        if scan_active > 0 {
            summary.finalizer_wait_reason = Some(format!(
                "live compaction is ready; waiting for {scan_active} active acquisition/scan lane(s) to drain"
            ));
            None
        } else {
            finalizer_queue_admission_blocked_reason(config, &machine, &finalizer_queue)
        }
    } else if !sweep_complete && !finalizer_queue.is_empty() {
        summary.finalizer_wait_reason = sweep_wait_reason.clone();
        sweep_blocked_reason.clone()
    } else {
        finalizer_queue_admission_blocked_reason(config, &machine, &finalizer_queue)
    };
    summary.scan_eta_secs = estimate_summary_eta(&epochs, admission.scan_capacity);
    let queue_eta = estimate_runnable_queue_eta(
        &epochs,
        &lanes,
        admission.scan_capacity,
        runtime.scheduler_paused,
    );
    summary.eta_secs = queue_eta.eta_secs;
    summary.queue_eta_secs = queue_eta.eta_secs;
    summary.queue_eta_reason = queue_eta.reason;
    summary.queue_jobs_remaining = queue_eta.jobs_remaining;
    summary.queue_capacity = queue_eta.capacity;
    summary.queue_job_duration_secs = queue_eta.job_duration_secs;
    summary.queue_duration_samples = queue_eta.duration_samples;
    let (archive_eta_secs, archive_eta_reason) = estimate_archive_eta(&epochs, &live);
    summary.archive_eta_secs = archive_eta_secs;
    summary.archive_eta_reason = archive_eta_reason;
    let inventory_epoch_count = discovery.epochs.union(&live_epochs).count();
    PipelineSnapshot {
        schema_version: STATUS_SCHEMA_VERSION,
        sequence: 0,
        now_unix_secs: now,
        current_epoch,
        observer_mode: !config.execute,
        capabilities: {
            let enabled = config.execute
                && (config.control_token.is_some() || config.allow_unauthenticated_controls);
            CapabilitySnapshot {
                control_enabled: enabled,
                authenticated_controls_required: config.control_token.is_some(),
                can_pause_scheduler: enabled,
                can_retry_failed: enabled,
            }
        },
        scheduler: SchedulerSnapshot {
            paused: runtime.scheduler_paused,
            updated_unix_secs: runtime.scheduler_updated_unix_secs,
        },
        inventory: InventorySnapshot {
            generation: inventory_generation,
            complete: discovery.errors.is_empty(),
            epochs_discovered: inventory_epoch_count,
            epochs_classified: inventory_epoch_count,
            started_unix_secs: now,
            completed_unix_secs: discovery.errors.is_empty().then_some(unix_now()),
            errors: discovery.errors,
        },
        scan_sweep: ScanSweepSnapshot {
            generation: inventory_generation,
            complete: sweep_complete,
            pending: scan_pending,
            active: scan_active,
            terminal_gaps,
            deferred_finalizers,
            wait_reason: sweep_wait_reason,
            blocked_reason: sweep_blocked_reason,
        },
        summary,
        machine,
        epochs,
        lanes,
        live,
        finalizer_queue,
        errors: runtime.errors.iter().cloned().collect(),
    }
}

#[derive(Debug, Default)]
struct InventoryDiscovery {
    epochs: BTreeSet<u64>,
    live_paths: Vec<PathBuf>,
    errors: Vec<String>,
}

fn discover_inventory(config: &NasPipelineConfig) -> InventoryDiscovery {
    let mut discovery = InventoryDiscovery::default();
    discover_epoch_entries(
        &config.car_root,
        "CAR",
        parse_car_epoch_name,
        false,
        &mut discovery,
    );
    discover_epoch_entries(
        &config.archive_root,
        "archive",
        parse_archive_epoch_name,
        true,
        &mut discovery,
    );
    match fs::read_dir(&config.live_root) {
        Ok(entries) => {
            for entry in entries {
                match entry {
                    Ok(entry)
                        if entry.file_name().to_str().is_some_and(|name| {
                            name.starts_with('.') && name.contains(".prepare-epoch-repair-")
                        }) =>
                    {
                        // `prepare-epoch-repair` publishes with one final directory rename.
                        // Its hidden same-filesystem staging directory is never inventory.
                    }
                    Ok(entry) => match entry.file_type() {
                        Ok(kind) if kind.is_dir() => {
                            let path = entry.path();
                            if let Some(epoch) =
                                entry.file_name().to_str().and_then(parse_epoch_name)
                                && epoch_in_scope(config, epoch)
                            {
                                discovery.epochs.insert(epoch);
                            }
                            discovery.live_paths.push(path);
                        }
                        Ok(_) => {}
                        Err(error) => discovery.errors.push(format!(
                            "stat live entry {}: {error}",
                            entry.path().display()
                        )),
                    },
                    Err(error) => discovery.errors.push(format!(
                        "read entry in live root {}: {error}",
                        config.live_root.display()
                    )),
                }
            }
        }
        Err(error) => discovery.errors.push(format!(
            "read live root {}: {error}",
            config.live_root.display()
        )),
    }
    if let Some(start) = config.start_epoch {
        let end = config.end_epoch.unwrap_or(start);
        discovery
            .epochs
            .retain(|epoch| (start..=end).contains(epoch));
        discovery.epochs.extend(start..=end);
    }
    discovery
        .live_paths
        .sort_by(|left, right| left.as_os_str().cmp(right.as_os_str()));
    discovery
}

fn discover_epoch_entries(
    root: &Path,
    label: &str,
    parse: fn(&str) -> Option<u64>,
    require_directory: bool,
    discovery: &mut InventoryDiscovery,
) {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) => {
            discovery
                .errors
                .push(format!("read {label} root {}: {error}", root.display()));
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                discovery.errors.push(format!(
                    "read entry in {label} root {}: {error}",
                    root.display()
                ));
                continue;
            }
        };
        let Some(epoch) = entry.file_name().to_str().and_then(parse) else {
            continue;
        };
        match entry.file_type() {
            Ok(kind)
                if (require_directory && kind.is_dir())
                    || (!require_directory && kind.is_file()) =>
            {
                if epoch_in_scope_from_bounds(epoch, None, None) {
                    discovery.epochs.insert(epoch);
                }
            }
            Ok(_) => {}
            Err(error) => discovery.errors.push(format!(
                "stat {label} entry {}: {error}",
                entry.path().display()
            )),
        }
    }
}

fn epoch_in_scope(config: &NasPipelineConfig, epoch: u64) -> bool {
    epoch_in_scope_from_bounds(epoch, config.start_epoch, config.end_epoch)
}

/// Return a stable, work-conserving scheduling key without changing the
/// canonical epoch inventory order used by progress merging and SSE clients.
/// The configured band comes first in newest-first order; everything outside
/// it retains the historical ascending order.
fn historical_schedule_priority_key(config: &NasPipelineConfig, epoch: u64) -> (u8, u64) {
    match (config.priority_epoch_start, config.priority_epoch_end) {
        (Some(start), Some(end)) if (start..=end).contains(&epoch) => {
            (0, end.saturating_sub(epoch))
        }
        _ => (1, epoch),
    }
}

fn prioritized_epochs<'a>(
    config: &NasPipelineConfig,
    epochs: &'a [EpochSnapshot],
) -> impl Iterator<Item = &'a EpochSnapshot> + 'a {
    let priority_range = config.priority_epoch_start.zip(config.priority_epoch_end);
    // `epochs` is canonically ascending. Walk the preferred slice in reverse,
    // then the rest forward, without allocating another epoch inventory.
    epochs
        .iter()
        .rev()
        .filter(move |epoch| {
            priority_range.is_some_and(|(start, end)| (start..=end).contains(&epoch.epoch))
        })
        .chain(epochs.iter().filter(move |epoch| {
            !priority_range.is_some_and(|(start, end)| (start..=end).contains(&epoch.epoch))
        }))
}

fn epoch_in_scope_from_bounds(epoch: u64, start: Option<u64>, end: Option<u64>) -> bool {
    start.is_none_or(|start| epoch >= start) && end.is_none_or(|end| epoch <= end)
}

fn parse_car_epoch_name(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("epoch-")?;
    let digits = rest
        .strip_suffix(".car.zst")
        .or_else(|| rest.strip_suffix(".car"))?;
    (!digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

fn parse_archive_epoch_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("epoch-")?;
    (!digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

fn parse_epoch_name(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("epoch-")?;
    let digits = rest.split(|ch: char| !ch.is_ascii_digit()).next()?;
    (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
}

#[cfg(test)]
fn classify_epoch(
    config: &NasPipelineConfig,
    runtime: &RuntimeState,
    epoch: u64,
    now: u64,
) -> EpochSnapshot {
    let allow_legacy_no_access = !fs::read_dir(&config.live_root).is_ok_and(|entries| {
        entries.filter_map(std::result::Result::ok).any(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_dir())
                && entry
                    .file_name()
                    .to_str()
                    .and_then(parse_epoch_name)
                    .is_some_and(|live_epoch| live_epoch == epoch)
        })
    });
    classify_epoch_with_context(config, runtime, epoch, now, allow_legacy_no_access)
}

fn classify_epoch_with_context(
    config: &NasPipelineConfig,
    runtime: &RuntimeState,
    epoch: u64,
    now: u64,
    allow_legacy_no_access: bool,
) -> EpochSnapshot {
    let output = config.archive_root.join(format!("epoch-{epoch}"));
    let input = car_path(&config.car_root, epoch);
    let progress_path = historical_progress_path(&config.state_root, epoch);
    let mut progress = read_progress(&progress_path).unwrap_or_default();
    if progress.blocks_total == 0 {
        progress.blocks_total = SLOTS_PER_EPOCH;
    }
    if let Some(pid) = progress.pid {
        progress.rss_bytes = process_rss_bytes(pid).or(progress.rss_bytes);
        progress.peak_rss_bytes = process_peak_rss_bytes(pid)
            .or(progress.peak_rss_bytes)
            .map(|peak| peak.max(progress.rss_bytes.unwrap_or(0)));
    }
    let owner = read_ownership(&output);
    let owner_matches_epoch = owner
        .as_ref()
        .is_some_and(|owner| ownership_is_first_seen(owner) && owner.id == epoch.to_string());
    let require_first_seen_manifest = owner
        .as_ref()
        .is_some_and(|owner| owner_matches_epoch && ownership_is_first_seen(owner));
    let legacy_no_access_complete = allow_legacy_no_access
        && legacy_no_access_archive_complete(
            &output,
            !config.no_access,
            require_first_seen_manifest,
        );
    let output_complete =
        historical_archive_strict_complete(&output, !config.no_access, require_first_seen_manifest)
            || legacy_no_access_complete;
    let scan_marker = scan_marker_is_valid(&output.join(SCAN_MARKER));
    let ambiguous_car = car_paths_ambiguous(&config.car_root, epoch);
    let active_scan = runtime.scans.contains_key(&epoch);
    let active_acquisition = runtime.acquisitions.get(&epoch);
    let adopted_acquisition = active_acquisition
        .is_none()
        .then(|| active_acquisition_marker(config, epoch))
        .flatten();
    let claimed_acquisition = active_acquisition.is_none()
        && adopted_acquisition.is_none()
        && acquisition_claim_active(config, epoch);
    let active_finalizer = matches!(
        runtime.finalizer.as_ref().map(|child| &child.kind),
        Some(ChildKind::HistoricalFinalizer { epoch: active }) if *active == epoch
    );
    let progress_alive = progress_is_alive(&progress, now);
    let failure_key_scan = format!("scan:{epoch}");
    let failure_key_finalize = format!("finalize:{epoch}");
    let failure = runtime
        .failures
        .get(&failure_key_scan)
        .or_else(|| runtime.failures.get(&failure_key_finalize))
        .or_else(|| runtime.failures.get(&format!("download:{epoch}")))
        .or_else(|| runtime.failures.get(&format!("preflight:{epoch}")))
        .cloned();
    let owner_scanning = owner.as_ref().is_some_and(|owner| {
        owner_matches_epoch
            && owner.state == "running"
            && owner.pid.is_some_and(|pid| {
                process_cmdline_matches_job(pid, &config.blockzilla_bin, &output, "historical_scan")
            })
    });
    let owner_finalizing = owner.as_ref().is_some_and(|owner| {
        owner_matches_epoch
            && owner.state == "finalizing"
            && owner.pid.is_some_and(|pid| {
                process_cmdline_matches_job(
                    pid,
                    &config.blockzilla_bin,
                    &output,
                    "historical_finalizer",
                )
            })
    });
    let owner_process_active = owner_scanning || owner_finalizing;
    let ownership_failure = owner
        .as_ref()
        .filter(|_| owner_matches_epoch)
        .and_then(|owner| {
            if owner.state == "failed" {
                owner.message.clone()
            } else if matches!(owner.state.as_str(), "running" | "finalizing")
                && owner.pid.is_some()
                && !owner_process_active
            {
                Some("pipeline-owned process is no longer running".to_string())
            } else {
                None
            }
        });
    if owner_finalizing {
        progress.pid = owner.as_ref().and_then(|owner| owner.pid);
        progress.rss_bytes = progress.pid.and_then(process_rss_bytes);
        progress.peak_rss_bytes = progress.pid.and_then(process_peak_rss_bytes);
        progress.phase = Some("finalize".to_string());
        progress.state = Some("running".to_string());
    }
    let output_has_files = directory_has_entries(&output);

    let (state, message) = if output_complete {
        (
            HistoricalState::Complete,
            legacy_no_access_complete.then(|| {
                "accepted legacy no-access archive; both block-access sidecars were intentionally absent in the previous format"
                    .to_string()
            }),
        )
    } else if active_finalizer || owner_finalizing {
        (HistoricalState::Finalizing, None)
    } else if active_scan || owner_scanning {
        (HistoricalState::Scanning, None)
    } else if scan_marker {
        (HistoricalState::ScanReady, None)
    } else if ambiguous_car {
        (
            HistoricalState::Blocked,
            Some(format!(
                "both epoch-{epoch}.car and epoch-{epoch}.car.zst are present; refusing ambiguous input"
            )),
        )
    } else if let Some(message) = failure.or(ownership_failure) {
        (HistoricalState::Failed, Some(message))
    } else if progress_alive {
        (HistoricalState::Scanning, None)
    } else if output_has_files {
        (
            HistoricalState::Blocked,
            Some(historical_incomplete_message(
                &output,
                !config.no_access,
                require_first_seen_manifest,
            )),
        )
    } else if active_acquisition.is_some() || adopted_acquisition.is_some() || claimed_acquisition {
        let phase = match active_acquisition.map(|child| &child.kind) {
            Some(ChildKind::CarDownload { .. }) => "download",
            Some(ChildKind::CarPreflight { .. }) => "preflight",
            _ => adopted_acquisition
                .as_ref()
                .map(|marker| marker.kind.strip_prefix("car_").unwrap_or(&marker.kind))
                .or_else(|| {
                    read_acquisition_marker(config, epoch).map(|marker| {
                        if marker.kind == "car_download" {
                            "download"
                        } else if marker.kind == "car_preflight" {
                            "preflight"
                        } else {
                            "acquisition"
                        }
                    })
                })
                .unwrap_or("acquisition"),
        };
        (
            HistoricalState::Queued,
            Some(format!("CAR {phase} is running")),
        )
    } else if input.is_none() && config.car_source_url_template.is_some() {
        (
            HistoricalState::Queued,
            Some("CAR download is queued".to_string()),
        )
    } else if input.is_none() {
        (
            HistoricalState::Blocked,
            Some("input CAR is missing".to_string()),
        )
    } else if config.preflight_car
        && !car_preflight_status(config, epoch, input.as_deref()).complete()
    {
        (
            HistoricalState::Queued,
            Some("CAR preflight is queued".to_string()),
        )
    } else {
        (HistoricalState::Queued, None)
    };

    let artifacts = epoch_artifacts(
        config,
        runtime,
        epoch,
        input.as_deref(),
        &output,
        state,
        require_first_seen_manifest,
        legacy_no_access_complete,
    );
    let registry_order = classify_registry_order(&output, state);

    let updated_unix_secs = progress
        .updated_unix_secs
        .or_else(|| modified_unix_secs(&output))
        .or_else(|| input.as_deref().and_then(modified_unix_secs))
        .unwrap_or(now);
    EpochSnapshot {
        epoch,
        state,
        registry_order,
        car_bytes: input.as_deref().map(file_len).unwrap_or(0),
        artifacts,
        input_path: input,
        output_path: output,
        progress,
        message,
        updated_unix_secs,
    }
}

fn car_path(root: &Path, epoch: u64) -> Option<PathBuf> {
    let compressed = root.join(format!("epoch-{epoch}.car.zst"));
    let raw = root.join(format!("epoch-{epoch}.car"));
    let compressed_present = is_nonempty_file(&compressed);
    let raw_present = is_nonempty_file(&raw);
    match (compressed_present, raw_present) {
        (true, false) => Some(compressed),
        (false, true) => Some(raw),
        // Never silently choose a suffix when two producers published the
        // same epoch. Reconciliation classifies this as an operator-visible
        // blocked state.
        _ => None,
    }
}

fn car_paths_ambiguous(root: &Path, epoch: u64) -> bool {
    is_nonempty_file(&root.join(format!("epoch-{epoch}.car.zst")))
        && is_nonempty_file(&root.join(format!("epoch-{epoch}.car")))
}

#[derive(Debug, Default)]
struct CarPreflightStatus {
    source_matches: bool,
    poh_info: bool,
    shredding_info: bool,
    poh_summary: Option<String>,
    shredding_summary: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictPohCoverage {
    records: u64,
    blocks_with_entries: u64,
    blocks_without_entries: u64,
    entries: u64,
    transaction_references: u64,
    // The producer intentionally serializes this u128-compatible total as a
    // decimal string; all other PoH/shredding counters must be JSON integers.
    num_hashes: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictShreddingCoverage {
    records: u64,
    blocks_with_spans: u64,
    blocks_without_spans: u64,
    spans: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictCarPreflightReceipt {
    schema_version: u32,
    validation_level: String,
    structurally_valid: bool,
    clean_eof: bool,
    eligible_for_compaction: bool,
    epoch: u64,
    source_path: PathBuf,
    source_bytes: u64,
    source_modified_unix_secs: u64,
    source_modified_subsec_nanos: u32,
    compressed: bool,
    io_buffer_bytes: u64,
    decompressed_car_bytes: u64,
    blocks: u64,
    blocks_in_epoch: u64,
    present_slots: u64,
    duplicate_slots: u64,
    out_of_epoch_blocks: u64,
    non_monotonic_slots: u64,
    transactions: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    poh: StrictPohCoverage,
    shredding: StrictShreddingCoverage,
    started_unix_secs: u64,
    completed_unix_secs: u64,
    elapsed_secs: f64,
}

impl CarPreflightStatus {
    fn complete(&self) -> bool {
        self.source_matches && self.poh_info && self.shredding_info
    }
}

fn car_preflight_receipt_path(state_root: &Path, epoch: u64) -> PathBuf {
    state_root
        .join("preflight")
        .join(format!("epoch-{epoch}.json"))
}

fn car_preflight_status(
    config: &NasPipelineConfig,
    epoch: u64,
    input: Option<&Path>,
) -> CarPreflightStatus {
    let Some(input) = input else {
        return CarPreflightStatus::default();
    };
    let receipt_path = car_preflight_receipt_path(&config.state_root, epoch);
    read_car_preflight_status(epoch, input, &receipt_path)
}

fn receipt_matches_source(epoch: u64, input: &Path, receipt_path: &Path) -> bool {
    read_car_preflight_status(epoch, input, receipt_path).complete()
}

fn read_car_preflight_status(epoch: u64, input: &Path, receipt_path: &Path) -> CarPreflightStatus {
    let Ok(metadata) = fs::metadata(input) else {
        return CarPreflightStatus::default();
    };
    let Ok(file) = File::open(receipt_path) else {
        return CarPreflightStatus::default();
    };
    if file
        .metadata()
        .ok()
        .is_none_or(|metadata| metadata.len() == 0 || metadata.len() > MAX_PREFLIGHT_RECEIPT_BYTES)
    {
        return CarPreflightStatus::default();
    }
    let mut bytes = Vec::new();
    if file
        .take(MAX_PREFLIGHT_RECEIPT_BYTES)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return CarPreflightStatus::default();
    }
    let Ok(receipt) = serde_json::from_slice::<StrictCarPreflightReceipt>(&bytes) else {
        return CarPreflightStatus::default();
    };
    let actual_modified = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| (duration.as_secs(), u64::from(duration.subsec_nanos())));
    let num_hashes_valid = !receipt.poh.num_hashes.is_empty()
        && receipt
            .poh
            .num_hashes
            .bytes()
            .all(|byte| byte.is_ascii_digit());
    // Touch every contract field so accidental partial receipts cannot become
    // accepted merely because a subset happens to match the source.
    let complete_numeric_contract = receipt.io_buffer_bytes > 0
        && !receipt.source_path.as_os_str().is_empty()
        && receipt.decompressed_car_bytes > 0
        && (receipt.compressed || receipt.decompressed_car_bytes == receipt.source_bytes)
        && receipt.blocks > 0
        && receipt.blocks_in_epoch > 0
        && receipt.present_slots > 0
        && receipt.duplicate_slots == 0
        && receipt.out_of_epoch_blocks == 0
        && receipt.non_monotonic_slots == 0
        && receipt.started_unix_secs <= receipt.completed_unix_secs
        && receipt.elapsed_secs.is_finite()
        && receipt.poh.records == receipt.blocks
        && receipt.poh.transaction_references == receipt.transactions
        && receipt.shredding.records == receipt.blocks
        && receipt
            .poh
            .blocks_with_entries
            .saturating_add(receipt.poh.blocks_without_entries)
            == receipt.poh.records
        && receipt
            .shredding
            .blocks_with_spans
            .saturating_add(receipt.shredding.blocks_without_spans)
            == receipt.shredding.records
        && receipt.first_slot <= receipt.last_slot
        && num_hashes_valid;
    CarPreflightStatus {
        source_matches: receipt.source_bytes == metadata.len()
            && receipt.source_modified_unix_secs
                == actual_modified
                    .map(|modified| modified.0)
                    .unwrap_or(u64::MAX)
            && u64::from(receipt.source_modified_subsec_nanos)
                == actual_modified
                    .map(|modified| modified.1)
                    .unwrap_or(u64::MAX)
            && receipt.epoch == epoch
            && receipt.schema_version == 1
            && receipt.validation_level == "structural"
            && receipt.structurally_valid
            && receipt.clean_eof
            && receipt.eligible_for_compaction
            && complete_numeric_contract,
        poh_info: complete_numeric_contract,
        shredding_info: complete_numeric_contract,
        poh_summary: Some(format!(
            "entries={} blocks_without_entries={} transaction_references={}",
            receipt.poh.entries,
            receipt.poh.blocks_without_entries,
            receipt.poh.transaction_references,
        )),
        shredding_summary: Some(format!(
            "spans={} blocks_without_spans={}",
            receipt.shredding.spans, receipt.shredding.blocks_without_spans,
        )),
    }
}

fn scan_marker_is_valid(path: &Path) -> bool {
    let Ok(text) = fs::read_to_string(path) else {
        return false;
    };
    let mut lines = text.lines();
    if lines.next() != Some(SCAN_MARKER_MAGIC) {
        return false;
    }
    let mut registry_keys = false;
    let mut references = false;
    let mut include_access = false;
    for line in lines {
        let Some((name, value)) = line.split_once('=') else {
            return false;
        };
        match name {
            "registry_keys" => registry_keys = value.parse::<u64>().is_ok(),
            "references" => references = value.parse::<u64>().is_ok(),
            "include_access" => include_access = matches!(value, "0" | "1"),
            _ => return false,
        }
    }
    registry_keys && references && include_access
}

fn epoch_artifacts(
    config: &NasPipelineConfig,
    runtime: &RuntimeState,
    epoch: u64,
    input: Option<&Path>,
    output: &Path,
    epoch_state: HistoricalState,
    require_first_seen_manifest: bool,
    legacy_no_access_complete: bool,
) -> Vec<ArtifactSnapshot> {
    let scan_already_durable = matches!(
        epoch_state,
        HistoricalState::ScanReady | HistoricalState::Finalizing | HistoricalState::Complete
    );
    let active_kind = runtime.acquisitions.get(&epoch).map(|child| &child.kind);
    let adopted_kind = active_kind
        .is_none()
        .then(|| {
            active_acquisition_marker(config, epoch).or_else(|| {
                acquisition_claim_active(config, epoch)
                    .then(|| read_acquisition_marker(config, epoch))
                    .flatten()
            })
        })
        .flatten()
        .map(|marker| marker.kind);
    let preflight = car_preflight_status(config, epoch, input);
    let receipt_exists = car_preflight_receipt_path(&config.state_root, epoch).is_file();
    let archive_committed = epoch_state == HistoricalState::Complete;
    let car_state = if let Some(input) = input {
        if config.preflight_car && !scan_already_durable && preflight.complete() {
            ArtifactState::Verified
        } else if is_nonempty_file(input) {
            ArtifactState::Present
        } else {
            ArtifactState::Invalid
        }
    } else if archive_committed {
        ArtifactState::NotApplicable
    } else if matches!(active_kind, Some(ChildKind::CarDownload { .. }))
        || adopted_kind.as_deref() == Some("car_download")
    {
        ArtifactState::Building
    } else if config.car_source_url_template.is_some() {
        ArtifactState::Pending
    } else {
        ArtifactState::Missing
    };
    let mut artifacts = vec![ArtifactSnapshot {
        kind: ArtifactKind::Car,
        state: car_state,
        requirement: ArtifactRequirement::ScanInput,
        required_now: !scan_already_durable,
        bytes: input.map(file_len).unwrap_or(0),
        modified_unix_secs: input.and_then(modified_unix_secs),
        message: match car_state {
            ArtifactState::Pending => Some("waiting for bounded CAR acquisition".to_string()),
            ArtifactState::Verified => Some(
                "structural receipt matches size+mtime; CAR CIDs were not recomputed".to_string(),
            ),
            ArtifactState::NotApplicable if archive_committed => Some(
                "source CAR is not required after the compact archive commit; deletion is expected"
                    .to_string(),
            ),
            ArtifactState::Present if archive_committed => Some(
                "source CAR is retained but is not required after the compact archive commit"
                    .to_string(),
            ),
            _ => None,
        },
    }];

    let preflight_applicable = config.preflight_car && !scan_already_durable;
    let preflight_state = if !preflight_applicable {
        ArtifactState::NotApplicable
    } else if matches!(
        active_kind,
        Some(ChildKind::CarDownload { .. } | ChildKind::CarPreflight { .. })
    ) || matches!(
        adopted_kind.as_deref(),
        Some("car_download" | "car_preflight")
    ) {
        ArtifactState::Building
    } else if preflight.complete() {
        ArtifactState::Verified
    } else if receipt_exists {
        ArtifactState::Invalid
    } else {
        ArtifactState::Pending
    };
    artifacts.push(ArtifactSnapshot {
        kind: ArtifactKind::CarPreflight,
        state: preflight_state,
        requirement: ArtifactRequirement::ScanInput,
        required_now: preflight_applicable,
        bytes: file_len(&car_preflight_receipt_path(&config.state_root, epoch)),
        modified_unix_secs: modified_unix_secs(&car_preflight_receipt_path(
            &config.state_root,
            epoch,
        )),
        message: if preflight_state == ArtifactState::Invalid {
            Some("receipt is stale, malformed, or failed structural validation".to_string())
        } else if preflight_state == ArtifactState::Verified {
            Some("structural receipt matches size+mtime; CAR CIDs were not recomputed".to_string())
        } else {
            None
        },
    });
    for (kind, present, summary) in [
        (
            ArtifactKind::SourcePohInfo,
            preflight.poh_info,
            preflight.poh_summary.clone(),
        ),
        (
            ArtifactKind::SourceShreddingInfo,
            preflight.shredding_info,
            preflight.shredding_summary.clone(),
        ),
    ] {
        artifacts.push(ArtifactSnapshot {
            kind,
            state: if !preflight_applicable {
                ArtifactState::NotApplicable
            } else if preflight_state == ArtifactState::Building {
                ArtifactState::Building
            } else if preflight.source_matches && present {
                ArtifactState::Verified
            } else if receipt_exists {
                ArtifactState::Invalid
            } else {
                ArtifactState::Pending
            },
            requirement: ArtifactRequirement::ScanInput,
            required_now: preflight_applicable,
            bytes: 0,
            modified_unix_secs: None,
            message: summary,
        });
    }

    let scan_outputs_required = matches!(
        epoch_state,
        HistoricalState::ScanReady | HistoricalState::Finalizing | HistoricalState::Complete
    );
    let final_outputs_required = epoch_state == HistoricalState::Complete;
    let access_outputs_required = scan_outputs_required
        || (!config.no_access
            && historical_archive_core_complete(output, require_first_seen_manifest)
            && !legacy_no_access_complete);
    artifacts.push(marker_artifact(output.join(SCAN_MARKER), epoch_state));
    artifacts.extend([
        candidate_or_file_artifact(
            ArtifactKind::Metadata,
            output.join(META_FILE),
            ArtifactRequirement::FinalOutput,
            final_outputs_required,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::Registry,
            output.join(REGISTRY_FILE),
            ArtifactRequirement::ScanOutput,
            scan_outputs_required,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::RegistryCounts,
            output.join(REGISTRY_COUNTS_FILE),
            ArtifactRequirement::ScanOutput,
            scan_outputs_required,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::RegistryIndex,
            output.join(REGISTRY_INDEX_FILE),
            ArtifactRequirement::FinalOutput,
            final_outputs_required,
            false,
        ),
        candidate_or_file_artifact(
            ArtifactKind::FirstSeenManifest,
            output.join(FIRST_SEEN_MANIFEST_FILE),
            if require_first_seen_manifest {
                ArtifactRequirement::FinalOutput
            } else {
                ArtifactRequirement::Optional
            },
            final_outputs_required && require_first_seen_manifest,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::HotSeed,
            output.join(HOT_SEED_FILE),
            ArtifactRequirement::Optional,
            false,
            true,
        ),
        archive_file_artifact(
            ArtifactKind::BlockhashRegistry,
            output.join(BLOCKHASH_REGISTRY_FILE),
            ArtifactRequirement::ScanOutput,
            scan_outputs_required,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::Blocks,
            output.join(BLOCKS_FILE),
            ArtifactRequirement::ScanOutput,
            scan_outputs_required,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::BlockIndex,
            output.join(BLOCK_INDEX_FILE),
            ArtifactRequirement::ScanOutput,
            scan_outputs_required,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::Poh,
            output.join(POH_FILE),
            ArtifactRequirement::ScanOutput,
            scan_outputs_required,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::Shredding,
            output.join(SHREDDING_FILE),
            ArtifactRequirement::ScanOutput,
            scan_outputs_required,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::Signatures,
            output.join(SIGNATURES_FILE),
            ArtifactRequirement::ScanOutput,
            scan_outputs_required,
            true,
        ),
        archive_file_artifact(
            ArtifactKind::VoteHashRegistry,
            output.join(VOTE_HASH_REGISTRY_FILE),
            ArtifactRequirement::ScanOutput,
            scan_outputs_required,
            true,
        ),
        access_artifact(
            ArtifactKind::BlockAccess,
            output.join(BLOCK_ACCESS_FILE),
            !config.no_access,
            access_outputs_required,
            legacy_no_access_complete,
        ),
        access_artifact(
            ArtifactKind::BlockAccessIndex,
            output.join(BLOCK_ACCESS_INDEX_FILE),
            !config.no_access,
            access_outputs_required,
            legacy_no_access_complete,
        ),
        archive_file_artifact(
            ArtifactKind::PreviousBlockhashTail,
            output.join(PREVIOUS_BLOCKHASH_TAIL_FILE),
            ArtifactRequirement::Optional,
            false,
            true,
        ),
    ]);
    if let Some(registry) = artifacts
        .iter_mut()
        .find(|artifact| artifact.kind == ArtifactKind::Registry)
        && registry.bytes > 0
        && !registry.bytes.is_multiple_of(32)
    {
        registry.state = ArtifactState::Invalid;
        registry.message = Some("registry length is not divisible by 32".to_string());
    }
    artifacts
}

fn marker_artifact(path: PathBuf, epoch_state: HistoricalState) -> ArtifactSnapshot {
    let exists = path.is_file();
    let valid = exists && scan_marker_is_valid(&path);
    ArtifactSnapshot {
        kind: ArtifactKind::ScanMarker,
        state: if valid {
            ArtifactState::Verified
        } else if exists {
            ArtifactState::Invalid
        } else if epoch_state == HistoricalState::Scanning {
            ArtifactState::Building
        } else if epoch_state == HistoricalState::ScanReady {
            ArtifactState::Missing
        } else {
            ArtifactState::NotApplicable
        },
        requirement: ArtifactRequirement::ScanOutput,
        required_now: epoch_state == HistoricalState::ScanReady,
        bytes: file_len(&path),
        modified_unix_secs: modified_unix_secs(&path),
        message: (exists && !valid).then(|| "malformed durable scan marker".to_string()),
    }
}

fn candidate_or_file_artifact(
    kind: ArtifactKind,
    path: PathBuf,
    requirement: ArtifactRequirement,
    required_now: bool,
    allow_empty: bool,
) -> ArtifactSnapshot {
    if path.is_file() {
        return archive_file_artifact(kind, path, requirement, required_now, allow_empty);
    }
    let candidate = path.with_file_name(format!(
        "{}.prehot.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("candidate")
    ));
    if candidate.is_file() {
        return ArtifactSnapshot {
            kind,
            state: if allow_empty || file_len(&candidate) > 0 {
                ArtifactState::Candidate
            } else {
                ArtifactState::Invalid
            },
            requirement,
            required_now,
            bytes: file_len(&candidate),
            modified_unix_secs: modified_unix_secs(&candidate),
            message: None,
        };
    }
    archive_file_artifact(kind, path, requirement, required_now, allow_empty)
}

fn archive_file_artifact(
    kind: ArtifactKind,
    path: PathBuf,
    requirement: ArtifactRequirement,
    required_now: bool,
    allow_empty: bool,
) -> ArtifactSnapshot {
    let exists = path.is_file();
    let bytes = file_len(&path);
    ArtifactSnapshot {
        kind,
        state: if !exists {
            if required_now {
                ArtifactState::Missing
            } else {
                ArtifactState::Pending
            }
        } else if bytes == 0 && !allow_empty {
            ArtifactState::Invalid
        } else {
            ArtifactState::Present
        },
        requirement,
        required_now,
        bytes,
        modified_unix_secs: modified_unix_secs(&path),
        message: None,
    }
}

fn access_artifact(
    kind: ArtifactKind,
    path: PathBuf,
    access_enabled: bool,
    required_now: bool,
    legacy_no_access_complete: bool,
) -> ArtifactSnapshot {
    if legacy_no_access_complete {
        return ArtifactSnapshot {
            kind,
            state: ArtifactState::NotApplicable,
            requirement: ArtifactRequirement::Optional,
            required_now: false,
            bytes: file_len(&path),
            modified_unix_secs: modified_unix_secs(&path),
            message: Some(
                "accepted legacy no-access archive; this sidecar was not emitted by the previous format"
                    .to_string(),
            ),
        };
    }
    if !access_enabled {
        return ArtifactSnapshot {
            kind,
            state: ArtifactState::NotApplicable,
            requirement: ArtifactRequirement::Optional,
            required_now: false,
            bytes: file_len(&path),
            modified_unix_secs: modified_unix_secs(&path),
            message: Some("block-access generation is disabled".to_string()),
        };
    }
    archive_file_artifact(
        kind,
        path,
        ArtifactRequirement::FinalOutput,
        required_now,
        false,
    )
}

fn historical_archive_strict_complete(
    path: &Path,
    require_access: bool,
    require_first_seen_manifest: bool,
) -> bool {
    historical_archive_core_complete(path, require_first_seen_manifest)
        && (!require_access
            || [BLOCK_ACCESS_FILE, BLOCK_ACCESS_INDEX_FILE]
                .iter()
                .all(|name| is_nonempty_file(&path.join(name))))
}

fn legacy_no_access_archive_complete(
    path: &Path,
    require_access: bool,
    require_first_seen_manifest: bool,
) -> bool {
    require_access
        && !path.join(OWNERSHIP_MARKER).exists()
        && historical_archive_core_complete(path, require_first_seen_manifest)
        && [BLOCK_ACCESS_FILE, BLOCK_ACCESS_INDEX_FILE]
            .iter()
            .all(|name| !path.join(name).exists())
}

fn historical_archive_core_complete(path: &Path, require_first_seen_manifest: bool) -> bool {
    let reader_core_complete = !path.join(SCAN_MARKER).exists()
        && [
            META_FILE,
            REGISTRY_FILE,
            REGISTRY_COUNTS_FILE,
            REGISTRY_INDEX_FILE,
            BLOCKHASH_REGISTRY_FILE,
            BLOCKS_FILE,
            BLOCK_INDEX_FILE,
            POH_FILE,
            SHREDDING_FILE,
        ]
        .iter()
        .all(|name| is_nonempty_file(&path.join(name)));
    let optional_empty_files_exist = [SIGNATURES_FILE, VOTE_HASH_REGISTRY_FILE]
        .iter()
        .all(|name| path.join(name).is_file());
    reader_core_complete
        && optional_empty_files_exist
        && (!require_first_seen_manifest
            || first_seen_manifest_declares_first_seen(&path.join(FIRST_SEEN_MANIFEST_FILE)))
}

fn classify_registry_order(path: &Path, state: HistoricalState) -> RegistryOrder {
    let Some(registry_keys) = structurally_valid_registry_key_count(&path.join(REGISTRY_FILE))
    else {
        return RegistryOrder::Unknown;
    };
    let manifest_status = first_seen_manifest_status(&path.join(FIRST_SEEN_MANIFEST_FILE));

    if matches!(
        state,
        HistoricalState::ScanReady | HistoricalState::Finalizing | HistoricalState::Complete
    ) && manifest_status == FirstSeenManifestStatus::FirstSeen
    {
        return RegistryOrder::FirstSeen;
    }

    if state == HistoricalState::Complete
        && manifest_status == FirstSeenManifestStatus::Absent
        && registry_index_matches_key_count(&path.join(REGISTRY_INDEX_FILE), registry_keys)
    {
        return RegistryOrder::UsageSorted;
    }

    RegistryOrder::Unknown
}

fn structurally_valid_registry_key_count(path: &Path) -> Option<u64> {
    let metadata = fs::metadata(path).ok()?;
    let bytes = metadata.is_file().then_some(metadata.len())?;
    (bytes > 0 && bytes.is_multiple_of(32))
        .then_some(bytes / 32)
        .filter(|keys| *keys <= u64::from(u32::MAX))
}

fn registry_index_matches_key_count(path: &Path, registry_keys: u64) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    let Some(minimum_len) = registry_keys
        .checked_mul(12)
        .and_then(|rows| rows.checked_add(u64::from(REGISTRY_INDEX_HEADER_LEN)))
        .and_then(|rows| rows.checked_add(1))
    else {
        return false;
    };
    if !metadata.is_file() || metadata.len() < minimum_len {
        return false;
    }

    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let mut header = [0u8; REGISTRY_INDEX_HEADER_LEN as usize];
    if file.read_exact(&mut header).is_err() {
        return false;
    }
    header[..8] == *REGISTRY_INDEX_MAGIC
        && u16::from_le_bytes([header[8], header[9]]) == REGISTRY_INDEX_VERSION
        && u16::from_le_bytes([header[10], header[11]]) == REGISTRY_INDEX_HEADER_LEN
        && u64::from_le_bytes(header[12..20].try_into().unwrap()) == registry_keys
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FirstSeenManifestStatus {
    Absent,
    FirstSeen,
    Invalid,
}

fn first_seen_manifest_status(path: &Path) -> FirstSeenManifestStatus {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return FirstSeenManifestStatus::Absent;
        }
        Err(_) => return FirstSeenManifestStatus::Invalid,
    }

    let Ok(metadata) = fs::metadata(path) else {
        return FirstSeenManifestStatus::Invalid;
    };
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_FIRST_SEEN_MANIFEST_BYTES
    {
        return FirstSeenManifestStatus::Invalid;
    }

    let Ok(file) = File::open(path) else {
        return FirstSeenManifestStatus::Invalid;
    };
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    if file
        .take(MAX_FIRST_SEEN_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .is_err()
        || bytes.len() as u64 > MAX_FIRST_SEEN_MANIFEST_BYTES
    {
        return FirstSeenManifestStatus::Invalid;
    }
    let Ok(manifest) = std::str::from_utf8(&bytes) else {
        return FirstSeenManifestStatus::Invalid;
    };

    let mut registry_order = None;
    for line in manifest.lines() {
        let Some(value) = line.strip_prefix("registry_order=") else {
            continue;
        };
        if registry_order.replace(value).is_some() {
            return FirstSeenManifestStatus::Invalid;
        }
    }
    if registry_order == Some("first_seen_v1") {
        FirstSeenManifestStatus::FirstSeen
    } else {
        FirstSeenManifestStatus::Invalid
    }
}

fn first_seen_manifest_declares_first_seen(path: &Path) -> bool {
    first_seen_manifest_status(path) == FirstSeenManifestStatus::FirstSeen
}

fn ownership_is_first_seen(owner: &OwnershipMarker) -> bool {
    matches!(
        owner.kind.as_str(),
        "historical_scan" | "historical_finalizer"
    )
}

fn pipeline_owned_first_seen(path: &Path) -> bool {
    read_ownership(path)
        .as_ref()
        .is_some_and(ownership_is_first_seen)
}

fn historical_incomplete_message(
    path: &Path,
    require_access: bool,
    require_first_seen_manifest: bool,
) -> String {
    if historical_archive_core_complete(path, require_first_seen_manifest)
        && require_access
        && ![BLOCK_ACCESS_FILE, BLOCK_ACCESS_INDEX_FILE]
            .iter()
            .all(|name| is_nonempty_file(&path.join(name)))
    {
        return "reader core is complete but required block-access sidecars are missing or empty"
            .to_string();
    }
    if require_first_seen_manifest
        && historical_archive_core_complete(path, false)
        && !first_seen_manifest_declares_first_seen(&path.join(FIRST_SEEN_MANIFEST_FILE))
    {
        return "pipeline-owned first-seen output is missing a valid final manifest".to_string();
    }
    "output exists without a complete reader core or scan-ready marker".to_string()
}

fn live_archive_packaged(path: &Path) -> bool {
    !path.join(SCAN_MARKER).exists()
        && [
            META_FILE,
            REGISTRY_FILE,
            REGISTRY_COUNTS_FILE,
            REGISTRY_INDEX_FILE,
            BLOCKHASH_REGISTRY_FILE,
            BLOCKS_FILE,
            BLOCK_INDEX_FILE,
            POH_FILE,
            SHREDDING_FILE,
        ]
        .iter()
        .all(|name| is_nonempty_file(&path.join(name)))
        && [SIGNATURES_FILE, VOTE_HASH_REGISTRY_FILE]
            .iter()
            .all(|name| path.join(name).is_file())
}

fn live_finalizer_queue_item(
    config: &NasPipelineConfig,
    capture: &LiveCaptureSnapshot,
) -> Option<FinalizerQueueItem> {
    if capture.superseded_by.is_some() {
        return None;
    }
    let output = capture.output_path.as_deref()?;
    let registry_ready = output.join(LIVE_REGISTRY_READY_MARKER).is_file()
        && is_nonempty_file(&output.join(REGISTRY_FILE))
        && is_nonempty_file(&output.join(REGISTRY_COUNTS_FILE));
    let (phase, estimated_memory_bytes, estimated_disk_bytes) = if !registry_ready {
        (
            LiveFinalizerPhase::Registry,
            finalizer_memory_floor_bytes(config),
            MIN_FINALIZER_SCRATCH_BYTES,
        )
    } else if !is_nonempty_file(&output.join(REGISTRY_INDEX_FILE)) {
        (
            LiveFinalizerPhase::Mphf,
            estimate_mphf_build_bytes(config, file_len(&output.join(REGISTRY_FILE))),
            estimate_finalizer_scratch_bytes(file_len(&output.join(REGISTRY_FILE))),
        )
    } else {
        (
            LiveFinalizerPhase::Rewrite,
            finalizer_memory_floor_bytes(config).max(
                file_len(&output.join(REGISTRY_INDEX_FILE))
                    .saturating_add(FINALIZER_REWRITE_OVERHEAD_BYTES),
            ),
            MIN_FINALIZER_SCRATCH_BYTES.max(
                file_len(&output.join(REGISTRY_INDEX_FILE))
                    .saturating_add(FINALIZER_REWRITE_OVERHEAD_BYTES),
            ),
        )
    };
    Some(FinalizerQueueItem {
        kind: "live".to_string(),
        epoch: capture.epoch,
        id: capture.id.clone(),
        phase: phase.as_str().to_string(),
        state: "ready_to_package".to_string(),
        estimated_memory_bytes,
        estimated_disk_bytes,
        deferred_reason: None,
    })
}

fn finalizer_memory_floor_bytes(config: &NasPipelineConfig) -> u64 {
    config.finalizer_memory_mib.saturating_mul(1024 * 1024)
}

fn estimate_mphf_build_bytes(config: &NasPipelineConfig, registry_bytes: u64) -> u64 {
    // The bounded builder maps registry.bin and disables ph's per-key hash
    // cache. A production run over a 399,597,536-byte registry peaked at
    // 557,972 KiB RSS. Keep a deliberately wider 2x input allowance plus the
    // fixed build overhead for allocator, MPHF, values, and tags.
    finalizer_memory_floor_bytes(config).max(
        registry_bytes
            .saturating_mul(2)
            .saturating_add(FINALIZER_BUILD_OVERHEAD_BYTES),
    )
}

fn estimate_finalizer_scratch_bytes(registry_bytes: u64) -> u64 {
    MIN_FINALIZER_SCRATCH_BYTES.max(registry_bytes.saturating_mul(2))
}

fn finalizer_admission_blocked_reason(
    config: &NasPipelineConfig,
    machine: &MachineSnapshot,
    task: &FinalizerQueueItem,
) -> Option<String> {
    let disk_reserve = config.disk_reserve_gib.saturating_mul(1024 * 1024 * 1024);
    if machine.disk_total_bytes == 0 {
        return Some(
            "finalizer disk admission blocked: filesystem capacity unavailable".to_string(),
        );
    }
    let required_disk = disk_reserve.saturating_add(task.estimated_disk_bytes);
    if machine.disk_available_bytes < required_disk {
        return Some(format!(
            "disk admission blocked: available {:.1} GiB, projected scratch {:.1} GiB, reserve {} GiB",
            machine.disk_available_bytes as f64 / 1024f64.powi(3),
            task.estimated_disk_bytes as f64 / 1024f64.powi(3),
            config.disk_reserve_gib
        ));
    }
    if machine.memory_total_bytes == 0 {
        return None;
    }
    let reserve_bytes = config.memory_reserve_mib.saturating_mul(1024 * 1024);
    let required = reserve_bytes.saturating_add(task.estimated_memory_bytes);
    (machine.memory_available_bytes < required).then(|| {
        format!(
            "finalizer memory admission blocked: phase={} available={:.1} MiB required={:.1} MiB (estimate={:.1} MiB reserve={} MiB)",
            task.phase,
            machine.memory_available_bytes as f64 / 1024f64.powi(2),
            required as f64 / 1024f64.powi(2),
            task.estimated_memory_bytes as f64 / 1024f64.powi(2),
            config.memory_reserve_mib,
        )
    })
}

fn finalizer_is_admissible(
    config: &NasPipelineConfig,
    machine: &MachineSnapshot,
    task: &FinalizerQueueItem,
) -> bool {
    task.deferred_reason.is_none()
        && finalizer_admission_blocked_reason(config, machine, task).is_none()
}

fn first_admissible_finalizer<'a>(
    config: &NasPipelineConfig,
    machine: &MachineSnapshot,
    queue: &'a [FinalizerQueueItem],
) -> Option<&'a FinalizerQueueItem> {
    queue
        .iter()
        .find(|task| finalizer_is_admissible(config, machine, task))
}

fn finalizer_queue_admission_blocked_reason(
    config: &NasPipelineConfig,
    machine: &MachineSnapshot,
    queue: &[FinalizerQueueItem],
) -> Option<String> {
    if first_admissible_finalizer(config, machine, queue).is_some() {
        return None;
    }
    queue
        .iter()
        .filter(|task| task.deferred_reason.is_none())
        .find_map(|task| finalizer_admission_blocked_reason(config, machine, task))
}

fn summarize_epochs(epochs: &[EpochSnapshot]) -> PipelineSummary {
    let mut summary = PipelineSummary {
        epochs_total: epochs.len(),
        ..PipelineSummary::default()
    };
    for epoch in epochs {
        match epoch.state {
            HistoricalState::Queued => summary.queued += 1,
            HistoricalState::Scanning => summary.scanning += 1,
            HistoricalState::ScanReady => summary.scan_ready += 1,
            HistoricalState::Finalizing => summary.finalizing += 1,
            HistoricalState::Complete => summary.complete += 1,
            HistoricalState::Failed => summary.failed += 1,
            HistoricalState::Blocked => summary.blocked += 1,
        }
        summary.blocks_done = summary
            .blocks_done
            .saturating_add(epoch.progress.blocks_done);
        summary.blocks_total = summary
            .blocks_total
            .saturating_add(epoch.progress.blocks_total);
    }
    summary.progress_pct = if summary.epochs_total == 0 {
        0.0
    } else {
        let fractional = epochs
            .iter()
            .map(|epoch| match epoch.state {
                HistoricalState::Complete => 1.0,
                _ => epoch.progress.progress_pct.unwrap_or(0.0) / 100.0,
            })
            .sum::<f64>();
        fractional * 100.0 / summary.epochs_total as f64
    };
    summary
}

/// Sum only fresh rates reported by active block-processing lanes.
///
/// Persisted progress for completed epochs intentionally remains available for
/// duration modelling, but it is historical evidence rather than current
/// throughput. Live capture advances in slots and is represented outside the
/// managed lane list, so its slot rate is never mixed into this blocks/s value.
fn active_block_processing_rate(lanes: &[LaneSnapshot], now: u64) -> f64 {
    let total = lanes
        .iter()
        .filter(|lane| {
            matches!(
                lane.kind.as_str(),
                "historical_scan"
                    | "historical_compact_reuse"
                    | "historical_finalizer"
                    | "live_finalizer"
            ) && !matches!(
                lane.state.as_str(),
                "paused" | "idle" | "done" | "complete" | "failed" | "stopped"
            ) && !matches!(
                lane.progress.state.as_deref(),
                Some("paused" | "idle" | "done" | "complete" | "failed" | "stopped")
            ) && lane
                .progress
                .updated_unix_secs
                .is_some_and(|updated| now.saturating_sub(updated) <= PROGRESS_STALE_SECS)
        })
        .filter_map(|lane| lane.progress.blocks_per_sec)
        .filter(|rate| rate.is_finite() && *rate >= 0.0)
        .sum::<f64>();
    if total.is_finite() && total > 0.0 {
        total
    } else {
        0.0
    }
}

fn estimate_summary_eta(epochs: &[EpochSnapshot], concurrency: usize) -> Option<f64> {
    if epochs
        .iter()
        .all(|epoch| epoch.state == HistoricalState::Complete)
    {
        return Some(0.0);
    }
    let mut duration_samples = epochs
        .iter()
        .filter_map(|epoch| {
            let elapsed = epoch.progress.elapsed_secs?;
            let fraction = epoch.progress.progress_pct? / 100.0;
            (fraction > 0.01 && elapsed.is_finite()).then(|| elapsed / fraction)
        })
        .collect::<Vec<_>>();
    duration_samples.sort_by(f64::total_cmp);
    let median_duration = duration_samples
        .get(duration_samples.len().saturating_sub(1) / 2)
        .copied();
    let active_eta = epochs
        .iter()
        .filter(|epoch| epoch.state == HistoricalState::Scanning)
        .filter_map(|epoch| epoch.progress.eta_secs)
        .filter(|eta| eta.is_finite())
        .max_by(f64::total_cmp);
    let queued = epochs
        .iter()
        .filter(|epoch| epoch.state == HistoricalState::Queued)
        .count();
    if queued == 0 {
        return active_eta;
    }
    let capacity = concurrency.max(1);
    let waves = queued.div_ceil(capacity) as f64;
    median_duration.map(|duration| active_eta.unwrap_or(0.0) + waves * duration)
}

#[derive(Debug, Default, PartialEq)]
struct RunnableQueueEta {
    eta_secs: Option<f64>,
    reason: Option<String>,
    jobs_remaining: usize,
    capacity: usize,
    job_duration_secs: Option<f64>,
    duration_samples: usize,
}

fn median_duration(mut samples: Vec<f64>) -> Option<f64> {
    samples.retain(|sample| sample.is_finite() && *sample > 0.0);
    samples.sort_by(f64::total_cmp);
    let upper = *samples.get(samples.len() / 2)?;
    if samples.len() % 2 == 0 {
        Some((samples[samples.len() / 2 - 1] + upper) / 2.0)
    } else {
        Some(upper)
    }
}

fn estimate_runnable_queue_eta(
    epochs: &[EpochSnapshot],
    lanes: &[LaneSnapshot],
    admitted_capacity: usize,
    scheduler_paused: bool,
) -> RunnableQueueEta {
    const MIN_DURATION_SECS: u64 = 60;
    const MAX_DURATION_SECS: u64 = 7 * 24 * 60 * 60;
    const COMPLETED_SAMPLE_LIMIT: usize = 32;

    let queued = epochs
        .iter()
        .filter(|epoch| epoch.state == HistoricalState::Queued)
        .count();
    let excluded = epochs
        .iter()
        .filter(|epoch| {
            matches!(
                epoch.state,
                HistoricalState::Blocked | HistoricalState::Failed
            )
        })
        .count();
    let active = lanes
        .iter()
        .filter(|lane| {
            matches!(
                lane.kind.as_str(),
                "historical_scan" | "historical_compact_reuse"
            ) && !matches!(lane.state.as_str(), "idle" | "done" | "complete")
        })
        .collect::<Vec<_>>();
    let jobs_remaining = queued.saturating_add(active.len());
    let running = active
        .iter()
        .filter(|lane| lane.state.as_str() != "paused")
        .count();
    let capacity = if running > 0 {
        running
    } else {
        admitted_capacity
    };

    // First filter by the persisted job progress so reconciliation only opens
    // ownership markers for plausible recent samples, rather than re-reading
    // every completed archive on every status refresh.
    let mut completed = epochs
        .iter()
        .filter_map(|epoch| {
            if !matches!(
                epoch.state,
                HistoricalState::Complete | HistoricalState::ScanReady
            ) {
                return None;
            }
            let duration = epoch.progress.elapsed_secs?;
            if !duration.is_finite()
                || !(MIN_DURATION_SECS as f64..=MAX_DURATION_SECS as f64).contains(&duration)
            {
                return None;
            }
            let owner = read_ownership(&epoch.output_path)?;
            let terminal = match owner.kind.as_str() {
                "historical_compact_reuse" => owner.state == "complete",
                "historical_scan" => owner.state == "scan_ready",
                _ => false,
            };
            terminal.then_some((owner.updated_unix_secs, duration))
        })
        .collect::<Vec<_>>();
    completed.sort_by_key(|(updated, _)| std::cmp::Reverse(*updated));
    completed.truncate(COMPLETED_SAMPLE_LIMIT);
    let completed_samples = completed
        .iter()
        .map(|(_, duration)| *duration)
        .collect::<Vec<_>>();
    let active_samples = active
        .iter()
        .filter_map(|lane| {
            let elapsed = lane.progress.elapsed_secs?;
            let eta = lane.progress.eta_secs?;
            let duration = elapsed + eta;
            (elapsed >= MIN_DURATION_SECS as f64
                && eta >= 0.0
                && duration <= MAX_DURATION_SECS as f64
                && duration.is_finite())
            .then_some(duration)
        })
        .collect::<Vec<_>>();
    let completed_median = median_duration(completed_samples.clone());
    let active_median = median_duration(active_samples.clone());
    // A queue ETA should not become more optimistic when current jobs slow
    // down. Use the slower of recent completed work and current projections.
    let job_duration_secs = match (completed_median, active_median) {
        (Some(completed), Some(active)) => Some(completed.max(active)),
        (Some(completed), None) => Some(completed),
        (None, Some(active)) => Some(active),
        (None, None) => None,
    };
    let duration_samples = completed_samples.len() + active_samples.len();

    if jobs_remaining == 0 {
        return RunnableQueueEta {
            eta_secs: Some(0.0),
            reason: Some(format!(
                "runnable historical queue is empty; {excluded} action-required historical item(s) excluded"
            )),
            jobs_remaining,
            capacity,
            job_duration_secs,
            duration_samples,
        };
    }
    if scheduler_paused {
        return RunnableQueueEta {
            reason: Some(
                "runnable queue ETA is unavailable while the scheduler is paused".to_string(),
            ),
            jobs_remaining,
            capacity,
            job_duration_secs,
            duration_samples,
            ..RunnableQueueEta::default()
        };
    }
    if capacity == 0 || (running == 0 && !active.is_empty()) {
        return RunnableQueueEta {
            reason: Some(
                "runnable queue ETA is unavailable while no historical worker is advancing"
                    .to_string(),
            ),
            jobs_remaining,
            capacity,
            job_duration_secs,
            duration_samples,
            ..RunnableQueueEta::default()
        };
    }
    let Some(job_duration_secs) = job_duration_secs else {
        return RunnableQueueEta {
            reason: Some(
                "runnable queue ETA is learning job duration from completed or mature active work"
                    .to_string(),
            ),
            jobs_remaining,
            capacity,
            duration_samples,
            ..RunnableQueueEta::default()
        };
    };

    let active_remaining = active
        .iter()
        .map(|lane| {
            lane.progress
                .eta_secs
                .filter(|eta| eta.is_finite() && *eta >= 0.0)
                .unwrap_or(job_duration_secs)
        })
        .collect::<Vec<_>>();
    // Model a work-conserving scheduler: each queued job starts on the lane
    // that becomes free first. This avoids the systematic overestimate from
    // adding full queue waves after the slowest current worker, while keeping
    // indivisible jobs (which a total-work/capacity average would miss).
    let mut lane_finish_secs = active
        .iter()
        .zip(active_remaining.iter().copied())
        .filter_map(|(lane, remaining)| (lane.state != "paused").then_some(remaining))
        .collect::<Vec<_>>();
    lane_finish_secs.resize(capacity, 0.0);
    let mut pending_durations = active
        .iter()
        .zip(active_remaining.iter().copied())
        .filter_map(|(lane, remaining)| (lane.state == "paused").then_some(remaining))
        .collect::<Vec<_>>();
    pending_durations.extend(std::iter::repeat_n(job_duration_secs, queued));
    for duration in pending_durations {
        let lane = lane_finish_secs
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| left.total_cmp(right))
            .map(|(index, _)| index)
            .unwrap_or_default();
        lane_finish_secs[lane] += duration;
    }
    let eta_secs = lane_finish_secs
        .into_iter()
        .max_by(f64::total_cmp)
        .unwrap_or(0.0);
    RunnableQueueEta {
        eta_secs: Some(eta_secs),
        reason: Some(format!(
            "{jobs_remaining} runnable historical job(s) at {capacity} worker(s); duration model uses {} completed and {} active sample(s); {excluded} action-required historical item(s) excluded",
            completed_samples.len(),
            active_samples.len(),
        )),
        jobs_remaining,
        capacity,
        job_duration_secs: Some(job_duration_secs),
        duration_samples,
    }
}

fn estimate_archive_eta(
    epochs: &[EpochSnapshot],
    live: &[LiveCaptureSnapshot],
) -> (Option<f64>, Option<String>) {
    let historical_blocked = epochs
        .iter()
        .filter(|epoch| epoch.state == HistoricalState::Blocked)
        .count();
    let historical_failed = epochs
        .iter()
        .filter(|epoch| epoch.state == HistoricalState::Failed)
        .count();
    let live_blocked = live
        .iter()
        .filter(|capture| capture.superseded_by.is_none() && capture.state == LiveState::Blocked)
        .count();
    let live_failed = live
        .iter()
        .filter(|capture| capture.superseded_by.is_none() && capture.state == LiveState::Failed)
        .count();
    if historical_blocked + historical_failed + live_blocked + live_failed > 0 {
        return (
            None,
            Some(format!(
                "archive completion blocked: historical blocked={historical_blocked} failed={historical_failed}; live blocked={live_blocked} failed={live_failed}"
            )),
        );
    }

    let historical_incomplete = epochs
        .iter()
        .filter(|epoch| epoch.state != HistoricalState::Complete)
        .count();
    let live_incomplete = live
        .iter()
        .filter(|capture| capture.superseded_by.is_none() && capture.state != LiveState::Complete)
        .count();
    if historical_incomplete == 0 && live_incomplete == 0 {
        return (Some(0.0), None);
    }

    let finalization_pending = epochs.iter().any(|epoch| {
        matches!(
            epoch.state,
            HistoricalState::ScanReady | HistoricalState::Finalizing
        )
    }) || live.iter().any(|capture| {
        if capture.superseded_by.is_some() {
            return false;
        }
        matches!(
            capture.state,
            LiveState::RepairGate
                | LiveState::RepairRequired
                | LiveState::ReadyToPackage
                | LiveState::Packaging
                | LiveState::Packaged
        )
    });
    let reason = if finalization_pending {
        "full-archive ETA unavailable: finalization/compaction phases do not publish a grounded duration estimate"
    } else {
        "full-archive ETA unavailable: scan/capture ETA excludes required finalization/compaction time"
    };
    (None, Some(reason.to_string()))
}

#[derive(Debug, Deserialize)]
struct PublishedRepairMarker {
    version: u16,
    state: String,
    epoch: u64,
    epoch_start_slot: u64,
    epoch_end_slot: u64,
    live_blocks: u64,
    rpc_only_blocks: u64,
    produced_blocks: u64,
    first_produced_slot: Option<u64>,
    last_produced_slot: Option<u64>,
    block_sources: Vec<PublishedRepairSource>,
    merge_plan: String,
    publication_ready: bool,
}

#[derive(Debug, Deserialize)]
struct PublishedRepairSource {
    original_capture_dir: PathBuf,
    selected_blocks: u64,
}

#[derive(Debug)]
struct PublishedRepairBundle {
    epoch: u64,
    epoch_start_slot: u64,
    epoch_end_slot: u64,
    live_blocks: u64,
    rpc_only_blocks: u64,
    produced_blocks: u64,
    first_produced_slot: u64,
    last_produced_slot: u64,
    source_capture_ids: Vec<String>,
    updated_unix_secs: u64,
}

#[derive(Debug, Deserialize)]
struct PublishedRepairCompactedMarker {
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
    files: PublishedRepairCompactedFiles,
    poh_coverage: PublishedRepairPohCoverage,
    shredding_coverage: PublishedRepairShreddingCoverage,
    source_materialized_marker_sha256: String,
    source_manifest_sha256: String,
    source_merge_plan_sha256: String,
    limitations: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PublishedRepairCompactedFiles {
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
    block_access: Option<String>,
    block_access_index: Option<String>,
    get_block_index: Option<String>,
    previous_blockhash_tail: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PublishedRepairPohCoverage {
    available_records: u64,
    available_entries: u64,
    missing_records: u64,
    produced_id_space: u64,
    record_ids_have_explicit_gaps: bool,
    missing_record_ids: Vec<u32>,
}

#[derive(Debug, Deserialize)]
struct PublishedRepairShreddingCoverage {
    available_records: u64,
    missing_records: u64,
    canonical_sidecar_emitted: bool,
}

#[derive(Debug, Deserialize)]
struct PublishedRepairSourceMaterializedMarker {
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
    manifest_sha256: String,
    merge_plan_sha256: String,
}

#[derive(Debug, Clone)]
struct PublishedRepairCompacted {
    updated_unix_secs: u64,
    block_access_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepairCompactedFileFingerprint {
    device: u64,
    inode: u64,
    bytes: u64,
    modified: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepairCompactedFingerprint {
    files: Vec<Option<RepairCompactedFileFingerprint>>,
    epoch: u64,
    epoch_start_slot: u64,
    epoch_end_slot: u64,
    live_blocks: u64,
    rpc_only_blocks: u64,
    produced_blocks: u64,
}

const MAX_REPAIR_COMPACTED_CACHE_ENTRIES: usize = 32;
static REPAIR_COMPACTED_VALIDATION_CACHE: OnceLock<
    StdMutex<
        VecDeque<(
            PathBuf,
            RepairCompactedFingerprint,
            PublishedRepairCompacted,
        )>,
    >,
> = OnceLock::new();

fn read_published_repair_bundle(
    live_root: &Path,
    capture_dir: &Path,
) -> Result<Option<PublishedRepairBundle>> {
    let marker_path = capture_dir.join(LIVE_REPAIR_REQUIRED_MARKER);
    let marker_metadata = match fs::symlink_metadata(&marker_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("stat {}", marker_path.display()));
        }
    };
    anyhow::ensure!(
        marker_metadata.file_type().is_file(),
        "{} is not a regular repair publication marker",
        marker_path.display()
    );
    anyhow::ensure!(
        marker_metadata.len() > 0 && marker_metadata.len() <= MAX_LIVE_REPAIR_MARKER_BYTES,
        "{} has {} bytes; expected 1..={MAX_LIVE_REPAIR_MARKER_BYTES}",
        marker_path.display(),
        marker_metadata.len()
    );

    // Deserialize only the publication fields used by Hivezilla. Serde skips the
    // large rpc_only_slots array without retaining it, keeping heap use bounded by
    // the 16 MiB file cap and the explicitly capped source list.
    let marker_file =
        File::open(&marker_path).with_context(|| format!("open {}", marker_path.display()))?;
    let marker: PublishedRepairMarker = serde_json::from_reader(BufReader::with_capacity(
        64 * 1024,
        marker_file.take(MAX_LIVE_REPAIR_MARKER_BYTES + 1),
    ))
    .with_context(|| format!("parse {}", marker_path.display()))?;

    anyhow::ensure!(
        marker.version == 1,
        "unsupported repair marker version {}",
        marker.version
    );
    anyhow::ensure!(
        marker.state.len() <= 128,
        "repair marker state exceeds 128 bytes"
    );
    anyhow::ensure!(
        marker.state == "rpc_fallback_missing_poh_and_shredding",
        "unsupported repair marker state"
    );
    anyhow::ensure!(
        !marker.publication_ready,
        "repair-required marker unexpectedly declares publication_ready"
    );
    let epoch_start_slot = marker
        .epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .context("repair epoch start overflows u64")?;
    let epoch_end_slot = epoch_start_slot
        .checked_add(SLOTS_PER_EPOCH - 1)
        .context("repair epoch end overflows u64")?;
    anyhow::ensure!(
        marker.epoch_start_slot == epoch_start_slot && marker.epoch_end_slot == epoch_end_slot,
        "repair marker epoch bounds do not match epoch {}",
        marker.epoch
    );
    anyhow::ensure!(
        marker.live_blocks.checked_add(marker.rpc_only_blocks) == Some(marker.produced_blocks),
        "repair marker produced-block accounting is inconsistent"
    );
    anyhow::ensure!(
        marker.produced_blocks > 0 && marker.produced_blocks <= SLOTS_PER_EPOCH,
        "repair marker produced_blocks {} is outside 1..={SLOTS_PER_EPOCH}",
        marker.produced_blocks
    );
    let first_produced_slot = marker
        .first_produced_slot
        .context("repair marker has no first produced slot")?;
    let last_produced_slot = marker
        .last_produced_slot
        .context("repair marker has no last produced slot")?;
    anyhow::ensure!(
        first_produced_slot >= epoch_start_slot
            && first_produced_slot <= last_produced_slot
            && last_produced_slot <= epoch_end_slot,
        "repair marker produced-slot bounds are outside its epoch"
    );
    anyhow::ensure!(
        marker.merge_plan.len() <= 256,
        "repair marker merge-plan path exceeds 256 bytes"
    );
    anyhow::ensure!(
        marker.merge_plan == LIVE_REPAIR_PLAN_FILE,
        "repair marker points at an unexpected merge plan"
    );
    let plan_path = capture_dir.join(LIVE_REPAIR_PLAN_FILE);
    let plan_metadata = fs::symlink_metadata(&plan_path)
        .with_context(|| format!("stat repair merge plan {}", plan_path.display()))?;
    anyhow::ensure!(
        plan_metadata.file_type().is_file()
            && plan_metadata.len() > 0
            && plan_metadata.len() <= MAX_LIVE_REPAIR_PLAN_BYTES,
        "repair merge plan {} must be a regular file with 1..={MAX_LIVE_REPAIR_PLAN_BYTES} bytes (found {})",
        plan_path.display(),
        plan_metadata.len()
    );
    anyhow::ensure!(
        !marker.block_sources.is_empty() && marker.block_sources.len() <= MAX_LIVE_REPAIR_SOURCES,
        "repair marker has {} sources; expected 1..={MAX_LIVE_REPAIR_SOURCES}",
        marker.block_sources.len()
    );

    let mut source_capture_ids = Vec::with_capacity(marker.block_sources.len());
    let mut unique_ids = BTreeSet::new();
    for source in marker.block_sources {
        anyhow::ensure!(
            source.selected_blocks > 0,
            "repair source selects zero blocks"
        );
        anyhow::ensure!(
            source.original_capture_dir.as_os_str().as_bytes().len()
                <= MAX_LIVE_REPAIR_SOURCE_PATH_BYTES,
            "repair source path exceeds {MAX_LIVE_REPAIR_SOURCE_PATH_BYTES} bytes"
        );
        let relative = source
            .original_capture_dir
            .strip_prefix(live_root)
            .with_context(|| {
                format!(
                    "repair source {} is outside live root {}",
                    source.original_capture_dir.display(),
                    live_root.display()
                )
            })?;
        let mut components = relative.components();
        let source_id = match (components.next(), components.next()) {
            (Some(Component::Normal(name)), None) => name
                .to_str()
                .context("repair source directory name is not UTF-8")?
                .to_string(),
            _ => anyhow::bail!(
                "repair source {} is not a direct child of {}",
                source.original_capture_dir.display(),
                live_root.display()
            ),
        };
        anyhow::ensure!(
            !source_id.is_empty() && source_id.len() <= 255 && !source_id.contains('\0'),
            "repair source directory name is invalid"
        );
        anyhow::ensure!(
            unique_ids.insert(source_id.clone()),
            "repair marker repeats source capture {source_id}"
        );
        source_capture_ids.push(source_id);
    }

    Ok(Some(PublishedRepairBundle {
        epoch: marker.epoch,
        epoch_start_slot,
        epoch_end_slot,
        live_blocks: marker.live_blocks,
        rpc_only_blocks: marker.rpc_only_blocks,
        produced_blocks: marker.produced_blocks,
        first_produced_slot,
        last_produced_slot,
        source_capture_ids,
        updated_unix_secs: modified_unix_secs(&marker_path).unwrap_or_default(),
    }))
}

fn repair_regular_file_metadata(
    path: &Path,
    max_bytes: u64,
    allow_empty: bool,
) -> Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("stat repair artifact {}", path.display()))?;
    anyhow::ensure!(
        metadata.file_type().is_file(),
        "repair artifact {} is not a regular non-symlink file",
        path.display()
    );
    anyhow::ensure!(
        metadata.len() <= max_bytes && (allow_empty || metadata.len() > 0),
        "repair artifact {} has invalid byte length {} (expected {}..={max_bytes})",
        path.display(),
        metadata.len(),
        if allow_empty { 0 } else { 1 }
    );
    Ok(metadata)
}

fn read_repair_json_bounded<T: for<'de> Deserialize<'de>>(
    path: &Path,
    max_bytes: u64,
) -> Result<T> {
    repair_regular_file_metadata(path, max_bytes, false)?;
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    serde_json::from_reader(BufReader::with_capacity(
        64 * 1024,
        file.take(max_bytes + 1),
    ))
    .with_context(|| format!("parse {}", path.display()))
}

fn repair_sha256_file(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("hash {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let mut digest = String::with_capacity(64);
    for byte in hasher.finalize() {
        use std::fmt::Write as _;
        write!(&mut digest, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(digest)
}

fn validate_repair_sha256(value: &str, field: &str) -> Result<()> {
    anyhow::ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{field} is not a lowercase 64-character SHA-256 digest"
    );
    Ok(())
}

fn read_repair_leb128_u64<R: Read>(reader: &mut R, field: &str) -> Result<u64> {
    let mut value = 0u64;
    for index in 0..10 {
        let mut byte = [0u8; 1];
        reader
            .read_exact(&mut byte)
            .with_context(|| format!("read {field}"))?;
        let byte = byte[0];
        if index == 9 {
            anyhow::ensure!(byte <= 1, "{field} LEB128 value overflows u64");
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            anyhow::ensure!(
                index == 0 || byte != 0,
                "{field} uses non-canonical LEB128 encoding"
            );
            return Ok(value);
        }
    }
    anyhow::bail!("{field} LEB128 value is unterminated")
}

fn read_repair_frame_len<R: Read>(reader: &mut R, field: &str) -> Result<Option<u32>> {
    let mut first = [0u8; 1];
    let read = reader
        .read(&mut first)
        .with_context(|| format!("read {field} frame length"))?;
    if read == 0 {
        return Ok(None);
    }
    let mut value = u32::from(first[0] & 0x7f);
    if first[0] & 0x80 == 0 {
        return Ok(Some(value));
    }
    for index in 1..5 {
        let mut byte = [0u8; 1];
        reader
            .read_exact(&mut byte)
            .with_context(|| format!("read {field} frame length"))?;
        let byte = byte[0];
        if index == 4 {
            anyhow::ensure!(byte <= 0x0f, "{field} frame length overflows u32");
        }
        value |= u32::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            anyhow::ensure!(byte != 0, "{field} frame length is not canonical LEB128");
            return Ok(Some(value));
        }
    }
    anyhow::bail!("{field} frame length is unterminated")
}

fn validate_repair_hot_index(output: &Path, marker: &PublishedRepairCompactedMarker) -> Result<()> {
    let index_path = output.join(BLOCK_INDEX_FILE);
    let index_metadata = repair_regular_file_metadata(&index_path, u64::MAX, false)?;
    let expected_len = u64::try_from(ARCHIVE_V2_HOT_INDEX_HEADER_LEN)
        .expect("hot index header length fits u64")
        .checked_add(
            marker
                .produced_blocks
                .checked_mul(
                    u64::try_from(ARCHIVE_V2_HOT_INDEX_ROW_LEN)
                        .expect("hot index row length fits u64"),
                )
                .context("repair hot-index length overflows u64")?,
        )
        .context("repair hot-index length overflows u64")?;
    anyhow::ensure!(
        index_metadata.len() == expected_len,
        "repair hot index has {} bytes; expected {expected_len} for {} rows",
        index_metadata.len(),
        marker.produced_blocks
    );

    let mut reader = BufReader::with_capacity(
        64 * 1024,
        File::open(&index_path).with_context(|| format!("open {}", index_path.display()))?,
    );
    let mut header = [0u8; ARCHIVE_V2_HOT_INDEX_HEADER_LEN];
    reader
        .read_exact(&mut header)
        .with_context(|| format!("read {}", index_path.display()))?;
    anyhow::ensure!(
        &header[..8] == ARCHIVE_V2_HOT_INDEX_MAGIC,
        "repair block index has invalid magic"
    );
    anyhow::ensure!(
        u16::from_le_bytes(header[8..10].try_into().unwrap()) == ARCHIVE_V2_HOT_INDEX_VERSION
            && header[10..12] == [0, 0],
        "repair block index has unsupported version or reserved flags"
    );
    let rows = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let blob_bytes = u64::from_le_bytes(header[20..28].try_into().unwrap());
    let level = i32::from_le_bytes(header[28..32].try_into().unwrap());
    let flags = u32::from_le_bytes(header[32..36].try_into().unwrap());
    anyhow::ensure!(
        rows == marker.produced_blocks,
        "repair hot-index row count mismatch"
    );
    anyhow::ensure!(
        blob_bytes == marker.compressed_bytes && level == marker.zstd_level && flags == 0,
        "repair hot-index header differs from compacted marker"
    );

    let mut next_offset = 0u64;
    let mut transactions = 0u64;
    let mut signatures = 0u64;
    let mut uncompressed = 0u64;
    let mut previous_slot = None;
    let mut row = [0u8; ARCHIVE_V2_HOT_INDEX_ROW_LEN];
    for expected_id in 0..marker.produced_blocks {
        reader
            .read_exact(&mut row)
            .with_context(|| format!("read repair hot-index row {expected_id}"))?;
        let block_id = u32::from_le_bytes(row[0..4].try_into().unwrap());
        let slot = u64::from_le_bytes(row[4..12].try_into().unwrap());
        let compressed_offset = u64::from_le_bytes(row[12..20].try_into().unwrap());
        let compressed_len = u32::from_le_bytes(row[20..24].try_into().unwrap());
        let uncompressed_len = u32::from_le_bytes(row[24..28].try_into().unwrap());
        let tx_count = u32::from_le_bytes(row[28..32].try_into().unwrap());
        let first_tx_ordinal = u64::from_le_bytes(row[32..40].try_into().unwrap());
        let first_signature_ordinal = u64::from_le_bytes(row[40..48].try_into().unwrap());
        let signature_count = u32::from_le_bytes(row[48..52].try_into().unwrap());
        anyhow::ensure!(
            u64::from(block_id) == expected_id,
            "repair hot-index block ids are not contiguous"
        );
        anyhow::ensure!(
            (marker.epoch_start_slot..=marker.epoch_end_slot).contains(&slot)
                && previous_slot.is_none_or(|previous| slot > previous),
            "repair hot-index slots are outside the epoch or not strictly increasing"
        );
        anyhow::ensure!(
            compressed_len > 0
                && uncompressed_len > 0
                && compressed_offset == next_offset
                && first_tx_ordinal == transactions
                && first_signature_ordinal == signatures,
            "repair hot-index offsets or ordinals are inconsistent"
        );
        next_offset = next_offset
            .checked_add(u64::from(compressed_len))
            .context("repair compressed byte count overflows u64")?;
        uncompressed = uncompressed
            .checked_add(u64::from(uncompressed_len))
            .context("repair uncompressed byte count overflows u64")?;
        transactions = transactions
            .checked_add(u64::from(tx_count))
            .context("repair transaction count overflows u64")?;
        signatures = signatures
            .checked_add(u64::from(signature_count))
            .context("repair signature count overflows u64")?;
        previous_slot = Some(slot);
    }
    anyhow::ensure!(
        next_offset == marker.compressed_bytes
            && uncompressed == marker.uncompressed_bytes
            && transactions == marker.transactions
            && signatures == marker.signatures,
        "repair hot-index totals differ from compacted marker"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct RepairBlockAccessExpectedRow {
    slot: u64,
    block_offset: u64,
    block_len: u32,
    access_offset: u64,
    access_len: u32,
    tx_count: u32,
    signature_count: u32,
}

fn decode_repair_first_block_access_blob<R: Read>(
    reader: &mut R,
    access_len: u32,
    tx_count: u32,
    signature_count: u32,
) -> Result<ArchiveV2BlockAccessBlob> {
    anyhow::ensure!(
        u64::from(access_len) <= blockzilla_format::ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
        "first repair block-access payload has unreasonably large byte length {access_len}"
    );
    let payload_len = usize::try_from(access_len).context("block-access length exceeds usize")?;
    let mut framed = Vec::with_capacity(payload_len.saturating_add(5));
    write_u32_varint(&mut framed, access_len)?;
    let prefix_len = framed.len();
    framed.resize(prefix_len + payload_len, 0);
    reader
        .read_exact(&mut framed[prefix_len..])
        .context("read first repair block-access payload")?;
    let mut framed_reader = WincodeLeb128FramedReader::new(std::io::Cursor::new(framed));
    let (_, blob) = framed_reader
        .read::<ArchiveV2BlockAccessBlob>()
        .context("decode first repair block-access payload")?
        .context("repair block-access payload is empty")?;
    anyhow::ensure!(
        blob.version == WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION && blob.flags == 0,
        "first repair block-access payload has incompatible version or flags"
    );
    let expected_signature_bytes = u64::from(signature_count)
        .checked_mul(64)
        .context("repair block-access signature byte count overflows u64")?;
    let declared_signatures = blob
        .signature_counts
        .iter()
        .map(|count| u64::from(*count))
        .sum::<u64>();
    anyhow::ensure!(
        blob.signature_counts.len() == tx_count as usize
            && declared_signatures == u64::from(signature_count)
            && blob.signatures.len() as u64 == expected_signature_bytes,
        "first repair block-access payload signature accounting differs from its index row"
    );
    anyhow::ensure!(
        blob.pubkeys.first().is_none_or(|entry| entry.id > 0)
            && blob.pubkeys.windows(2).all(|pair| pair[0].id < pair[1].id)
            && blob
                .blockhashes
                .windows(2)
                .all(|pair| pair[0].id < pair[1].id)
            && blob
                .vote_hashes
                .windows(2)
                .all(|pair| pair[0].block_id < pair[1].block_id),
        "first repair block-access payload mappings are not sorted and unique"
    );
    Ok(blob)
}

fn read_repair_block_access_row<R: Read, A: Read>(
    hot_reader: &mut R,
    access_reader: &mut A,
    expected_id: u64,
    expected_access_offset: u64,
    access_blob_bytes: u64,
    marker: &PublishedRepairCompactedMarker,
) -> Result<(RepairBlockAccessExpectedRow, u64)> {
    let mut hot = [0u8; ARCHIVE_V2_HOT_INDEX_ROW_LEN];
    hot_reader
        .read_exact(&mut hot)
        .with_context(|| format!("read repair hot-index row {expected_id} for block access"))?;
    let mut access = [0u8; ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN];
    access_reader
        .read_exact(&mut access)
        .with_context(|| format!("read repair block-access index row {expected_id}"))?;

    let hot_block_id = u32::from_le_bytes(hot[0..4].try_into().unwrap());
    let hot_slot = u64::from_le_bytes(hot[4..12].try_into().unwrap());
    let block_offset = u64::from_le_bytes(hot[12..20].try_into().unwrap());
    let block_len = u32::from_le_bytes(hot[20..24].try_into().unwrap());
    let hot_tx_count = u32::from_le_bytes(hot[28..32].try_into().unwrap());
    let hot_signature_count = u32::from_le_bytes(hot[48..52].try_into().unwrap());

    let access_block_id = u32::from_le_bytes(access[0..4].try_into().unwrap());
    let access_slot = u64::from_le_bytes(access[4..12].try_into().unwrap());
    let access_offset = u64::from_le_bytes(access[12..20].try_into().unwrap());
    let access_len = u32::from_le_bytes(access[20..24].try_into().unwrap());
    let access_tx_count = u32::from_le_bytes(access[24..28].try_into().unwrap());
    let access_signature_count = u32::from_le_bytes(access[28..32].try_into().unwrap());

    anyhow::ensure!(
        u64::from(hot_block_id) == expected_id && u64::from(access_block_id) == expected_id,
        "repair hot and block-access index block ids are not contiguous"
    );
    anyhow::ensure!(
        hot_slot == access_slot
            && hot_tx_count == access_tx_count
            && hot_signature_count == access_signature_count,
        "repair block-access index row {expected_id} differs from the hot index"
    );
    anyhow::ensure!(
        (marker.epoch_start_slot..=marker.epoch_end_slot).contains(&hot_slot),
        "repair block-access index slot is outside the repaired epoch"
    );
    anyhow::ensure!(
        block_len > 0 && access_len > 0 && access_offset == expected_access_offset,
        "repair block-access index offsets or lengths are inconsistent"
    );
    anyhow::ensure!(
        u64::from(access_len) <= blockzilla_format::ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
        "repair block-access index row {expected_id} exceeds the maximum payload size"
    );
    let next_access_offset = access_offset
        .checked_add(u64::from(access_len))
        .context("repair block-access byte count overflows u64")?;
    anyhow::ensure!(
        next_access_offset <= access_blob_bytes,
        "repair block-access index points beyond its blob"
    );

    Ok((
        RepairBlockAccessExpectedRow {
            slot: hot_slot,
            block_offset,
            block_len,
            access_offset,
            access_len,
            tx_count: access_tx_count,
            signature_count: access_signature_count,
        },
        next_access_offset,
    ))
}

fn validate_repair_previous_blockhash_tail(
    output: &Path,
    marker: &PublishedRepairCompactedMarker,
) -> Result<[u8; 32]> {
    let path = output.join(PREVIOUS_BLOCKHASH_TAIL_FILE);
    let metadata = repair_regular_file_metadata(&path, 40, false)?;
    anyhow::ensure!(
        metadata.len() == 40,
        "repair previous-blockhash tail has {} bytes; expected exactly one 40-byte predecessor row",
        metadata.len()
    );
    let mut reader = BufReader::with_capacity(
        16 * 1024,
        File::open(&path).with_context(|| format!("open {}", path.display()))?,
    );
    let mut previous_slot = None;
    let mut predecessor_hash = None;
    let mut row = [0u8; 40];
    for row_index in 0..metadata.len() / 40 {
        reader
            .read_exact(&mut row)
            .with_context(|| format!("read repair previous-blockhash tail row {row_index}"))?;
        let slot = u64::from_le_bytes(row[32..40].try_into().unwrap());
        anyhow::ensure!(
            slot < marker.epoch_start_slot && previous_slot.is_none_or(|previous| slot > previous),
            "repair previous-blockhash tail slots are not strictly increasing before the repaired epoch"
        );
        previous_slot = Some(slot);
        predecessor_hash = Some(row[..32].try_into().expect("tail hash has 32 bytes"));
    }
    predecessor_hash.context("repair previous-blockhash tail is empty")
}

fn validate_repair_block_access(
    output: &Path,
    marker: &PublishedRepairCompactedMarker,
) -> Result<()> {
    let access_path = output.join(BLOCK_ACCESS_FILE);
    let access_metadata = repair_regular_file_metadata(&access_path, u64::MAX, false)?;
    let access_index_path = output.join(BLOCK_ACCESS_INDEX_FILE);
    let access_index_metadata = repair_regular_file_metadata(&access_index_path, u64::MAX, false)?;
    let expected_access_index_len = u64::try_from(ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN)
        .expect("block-access index header length fits u64")
        .checked_add(
            marker
                .produced_blocks
                .checked_mul(
                    u64::try_from(ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN)
                        .expect("block-access index row length fits u64"),
                )
                .context("repair block-access index length overflows u64")?,
        )
        .context("repair block-access index length overflows u64")?;
    anyhow::ensure!(
        access_index_metadata.len() == expected_access_index_len,
        "repair block-access index has {} bytes; expected {expected_access_index_len} for {} rows",
        access_index_metadata.len(),
        marker.produced_blocks
    );

    let mut access_reader = BufReader::with_capacity(
        64 * 1024,
        File::open(&access_index_path)
            .with_context(|| format!("open {}", access_index_path.display()))?,
    );
    let mut access_header = [0u8; ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN];
    access_reader
        .read_exact(&mut access_header)
        .with_context(|| format!("read {}", access_index_path.display()))?;
    anyhow::ensure!(
        &access_header[..8] == ARCHIVE_V2_BLOCK_ACCESS_INDEX_MAGIC,
        "repair block-access index has invalid magic"
    );
    anyhow::ensure!(
        u16::from_le_bytes(access_header[8..10].try_into().unwrap())
            == ARCHIVE_V2_BLOCK_ACCESS_INDEX_VERSION
            && access_header[10..12] == [0, 0],
        "repair block-access index has unsupported version or reserved flags"
    );
    let access_rows = u64::from_le_bytes(access_header[12..20].try_into().unwrap());
    let access_blob_bytes = u64::from_le_bytes(access_header[20..28].try_into().unwrap());
    let access_flags = u32::from_le_bytes(access_header[28..32].try_into().unwrap());
    anyhow::ensure!(
        access_rows == marker.produced_blocks
            && access_blob_bytes == access_metadata.len()
            && access_flags == 0,
        "repair block-access index header differs from the repaired archive"
    );
    let mut access_blob_reader = BufReader::with_capacity(
        64 * 1024,
        File::open(&access_path).with_context(|| format!("open {}", access_path.display()))?,
    );
    let predecessor_hash = validate_repair_previous_blockhash_tail(output, marker)?;
    let blockhash_path = output.join(BLOCKHASH_REGISTRY_FILE);
    let mut first_blockhash = [0u8; 32];
    File::open(&blockhash_path)
        .with_context(|| format!("open {}", blockhash_path.display()))?
        .read_exact(&mut first_blockhash)
        .with_context(|| format!("read first blockhash from {}", blockhash_path.display()))?;

    let get_block_path = output.join(GET_BLOCK_INDEX_FILE);
    let get_block_metadata = repair_regular_file_metadata(&get_block_path, u64::MAX, false)?;
    let expected_get_block_len = SLOTS_PER_EPOCH
        .checked_mul(
            u64::try_from(ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN)
                .expect("get-block index row length fits u64"),
        )
        .context("repair get-block index length overflows u64")?;
    anyhow::ensure!(
        get_block_metadata.len() == expected_get_block_len,
        "repair get-block index has {} bytes; expected {expected_get_block_len}",
        get_block_metadata.len()
    );

    let hot_index_path = output.join(BLOCK_INDEX_FILE);
    let mut hot_reader = BufReader::with_capacity(
        64 * 1024,
        File::open(&hot_index_path)
            .with_context(|| format!("open {}", hot_index_path.display()))?,
    );
    let mut hot_header = [0u8; ARCHIVE_V2_HOT_INDEX_HEADER_LEN];
    hot_reader
        .read_exact(&mut hot_header)
        .with_context(|| format!("read {}", hot_index_path.display()))?;
    let mut get_block_reader = BufReader::with_capacity(
        64 * 1024,
        File::open(&get_block_path)
            .with_context(|| format!("open {}", get_block_path.display()))?,
    );

    let mut next_id = 0u64;
    let (first, mut next_access_offset) = read_repair_block_access_row(
        &mut hot_reader,
        &mut access_reader,
        next_id,
        0,
        access_blob_bytes,
        marker,
    )?;
    let first_blob = decode_repair_first_block_access_blob(
        &mut access_blob_reader,
        first.access_len,
        first.tx_count,
        first.signature_count,
    )?;
    anyhow::ensure!(
        first_blob.previous_blockhash == predecessor_hash,
        "first repair block-access previous blockhash differs from the predecessor tail"
    );
    anyhow::ensure!(
        first_blob.blockhash == first_blockhash,
        "first repair block-access blockhash differs from blockhash registry row 0"
    );
    let mut next = Some(first);
    let mut get_block_row = [0u8; ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN];
    for slot_offset in 0..SLOTS_PER_EPOCH {
        get_block_reader
            .read_exact(&mut get_block_row)
            .with_context(|| format!("read repair get-block index row {slot_offset}"))?;
        let slot = marker
            .epoch_start_slot
            .checked_add(slot_offset)
            .context("repair get-block slot overflows u64")?;
        anyhow::ensure!(
            next.is_none_or(|row| row.slot >= slot),
            "repair hot-index slots are not strictly increasing"
        );
        if next.is_some_and(|row| row.slot == slot) {
            let expected = next.take().expect("matching access row is present");
            let block_offset = u64::from_le_bytes(get_block_row[0..8].try_into().unwrap());
            let block_len = u32::from_le_bytes(get_block_row[8..12].try_into().unwrap());
            let access_offset = u64::from_le_bytes(get_block_row[12..20].try_into().unwrap());
            let access_len = u32::from_le_bytes(get_block_row[20..24].try_into().unwrap());
            anyhow::ensure!(
                block_offset == expected.block_offset
                    && block_len == expected.block_len
                    && access_offset == expected.access_offset
                    && access_len == expected.access_len,
                "repair get-block index row for slot {slot} differs from the hot/access indexes"
            );
            next_id += 1;
            if next_id < marker.produced_blocks {
                let (row, offset) = read_repair_block_access_row(
                    &mut hot_reader,
                    &mut access_reader,
                    next_id,
                    next_access_offset,
                    access_blob_bytes,
                    marker,
                )?;
                next_access_offset = offset;
                next = Some(row);
            }
        } else {
            anyhow::ensure!(
                get_block_row.iter().all(|byte| *byte == 0),
                "repair get-block index has an undeclared row for slot {slot}"
            );
        }
    }
    anyhow::ensure!(
        next.is_none()
            && next_id == marker.produced_blocks
            && next_access_offset == access_blob_bytes,
        "repair block-access indexes do not cover the complete repaired archive"
    );
    Ok(())
}

fn validate_repair_hot_meta(output: &Path, marker: &PublishedRepairCompactedMarker) -> Result<()> {
    let meta_path = output.join(META_FILE);
    repair_regular_file_metadata(&meta_path, MAX_LIVE_REPAIR_META_BYTES, false)?;
    let mut reader = BufReader::with_capacity(
        64 * 1024,
        File::open(&meta_path).with_context(|| format!("open {}", meta_path.display()))?,
    );

    let header_len = read_repair_frame_len(&mut reader, "repair metadata header")?
        .context("repair metadata is empty")?;
    anyhow::ensure!(
        u64::from(header_len) <= MAX_LIVE_REPAIR_META_BYTES,
        "repair metadata header frame is too large"
    );
    let mut header = (&mut reader).take(u64::from(header_len));
    anyhow::ensure!(
        read_repair_leb128_u64(&mut header, "repair metadata header tag")? == 0,
        "repair metadata does not begin with a header"
    );
    anyhow::ensure!(
        read_repair_leb128_u64(&mut header, "repair metadata version")?
            == u64::from(WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION)
            && read_repair_leb128_u64(&mut header, "repair metadata flags")?
                == u64::from(WINCODE_ARCHIVE_V2_FLAG_LEB128)
            && header.limit() == 0,
        "repair metadata header is incompatible"
    );

    let footer_len = read_repair_frame_len(&mut reader, "repair metadata footer")?
        .context("repair metadata has no footer")?;
    anyhow::ensure!(
        u64::from(footer_len) <= MAX_LIVE_REPAIR_META_BYTES,
        "repair metadata footer frame is too large"
    );
    let mut footer = (&mut reader).take(u64::from(footer_len));
    anyhow::ensure!(
        read_repair_leb128_u64(&mut footer, "repair metadata footer tag")? == 2,
        "repair metadata second record is not a footer"
    );
    let blocks = read_repair_leb128_u64(&mut footer, "repair metadata block count")?;
    let transactions = read_repair_leb128_u64(&mut footer, "repair metadata transaction count")?;
    for field in [
        "entries",
        "rewards",
        "dataframes",
        "subset nodes",
        "epoch nodes",
        "CAR entries",
        "CAR payload bytes",
        "decoded node payload bytes",
        "transaction source bytes",
        "metadata source bytes",
        "reward source bytes",
        "transaction raw fallbacks",
        "metadata raw fallbacks",
        "reward raw fallbacks",
        "nonce recent blockhashes",
    ] {
        let _ = read_repair_leb128_u64(&mut footer, field)?;
    }
    let decode_errors = read_repair_leb128_u64(&mut footer, "repair metadata decode errors")?;
    anyhow::ensure!(
        blocks == marker.produced_blocks
            && transactions == marker.transactions
            && decode_errors == 0
            && footer.limit() == 0,
        "repair metadata footer totals or shape differ from compacted marker"
    );
    anyhow::ensure!(
        read_repair_frame_len(&mut reader, "repair metadata trailing record")?.is_none(),
        "repair metadata has unexpected extra records"
    );
    Ok(())
}

fn validate_repair_available_poh(
    output: &Path,
    marker: &PublishedRepairCompactedMarker,
) -> Result<()> {
    let path = output.join(LIVE_REPAIR_AVAILABLE_POH_FILE);
    repair_regular_file_metadata(&path, u64::MAX, marker.poh_coverage.available_records == 0)?;
    let mut reader = BufReader::with_capacity(
        64 * 1024,
        File::open(&path).with_context(|| format!("open {}", path.display()))?,
    );
    let missing = &marker.poh_coverage.missing_record_ids;
    let mut missing_index = 0usize;
    let mut expected_id = 0u64;
    let mut records = 0u64;
    let mut entries = 0u64;
    let mut previous_slot = None;
    while let Some(frame_len) = read_repair_frame_len(&mut reader, "available PoH")? {
        anyhow::ensure!(
            frame_len > 0 && u64::from(frame_len) <= MAX_LIVE_REPAIR_POH_FRAME_BYTES,
            "available PoH frame has invalid byte length {frame_len}"
        );
        anyhow::ensure!(
            records < marker.poh_coverage.available_records,
            "available PoH has more records than declared"
        );
        let mut frame = (&mut reader).take(u64::from(frame_len));
        let block_id = read_repair_leb128_u64(&mut frame, "available PoH block id")?;
        anyhow::ensure!(
            block_id <= u64::from(u32::MAX),
            "available PoH block id exceeds u32"
        );
        let slot = read_repair_leb128_u64(&mut frame, "available PoH slot")?;
        let entry_count = read_repair_leb128_u64(&mut frame, "available PoH entry count")?;
        anyhow::ensure!(
            entry_count <= frame.limit() / 34,
            "available PoH entry count cannot fit in its frame"
        );
        for _ in 0..entry_count {
            let _ = read_repair_leb128_u64(&mut frame, "available PoH num_hashes")?;
            let mut hash = [0u8; 32];
            frame
                .read_exact(&mut hash)
                .context("read available PoH entry hash")?;
            let tx_count = read_repair_leb128_u64(&mut frame, "available PoH tx_count")?;
            anyhow::ensure!(
                tx_count <= u64::from(u32::MAX),
                "available PoH tx_count exceeds u32"
            );
        }
        anyhow::ensure!(frame.limit() == 0, "available PoH frame has trailing bytes");

        while missing_index < missing.len() && u64::from(missing[missing_index]) == expected_id {
            expected_id += 1;
            missing_index += 1;
        }
        anyhow::ensure!(
            block_id == expected_id,
            "available PoH record ids are not the exact complement of declared gaps"
        );
        anyhow::ensure!(
            (marker.epoch_start_slot..=marker.epoch_end_slot).contains(&slot)
                && previous_slot.is_none_or(|previous| slot > previous),
            "available PoH slots are outside the epoch or not strictly increasing"
        );
        expected_id += 1;
        records += 1;
        entries = entries
            .checked_add(entry_count)
            .context("available PoH entry count overflows u64")?;
        previous_slot = Some(slot);
    }
    while missing_index < missing.len() && u64::from(missing[missing_index]) == expected_id {
        expected_id += 1;
        missing_index += 1;
    }
    anyhow::ensure!(
        records == marker.poh_coverage.available_records
            && entries == marker.poh_coverage.available_entries
            && missing_index == missing.len()
            && expected_id == marker.produced_blocks,
        "available PoH contents differ from declared coverage"
    );
    Ok(())
}

fn repair_compacted_fingerprint(
    output: &Path,
    bundle: &PublishedRepairBundle,
) -> Result<RepairCompactedFingerprint> {
    let mut files = Vec::with_capacity(17);
    for path in [
        output.to_path_buf(),
        output.join(LIVE_REPAIR_COMPACTED_MARKER),
        output.join(BLOCKS_FILE),
        output.join(BLOCK_INDEX_FILE),
        output.join(META_FILE),
        output.join(REGISTRY_FILE),
        output.join(REGISTRY_COUNTS_FILE),
        output.join(REGISTRY_INDEX_FILE),
        output.join(BLOCKHASH_REGISTRY_FILE),
        output.join(SIGNATURES_FILE),
        output.join(VOTE_HASH_REGISTRY_FILE),
        output.join(LIVE_REPAIR_AVAILABLE_POH_FILE),
        output.join(LIVE_REPAIR_SOURCE_MATERIALIZED_MARKER),
    ] {
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("fingerprint repair artifact {}", path.display()))?;
        files.push(Some(RepairCompactedFileFingerprint {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            modified: metadata
                .modified()
                .with_context(|| format!("read modification time for {}", path.display()))?,
        }));
    }
    // Access-ready repaired archives add these four artifacts, while legacy
    // degraded markers require all four to remain absent. Fingerprint presence and
    // absence so either transition invalidates a cached validation receipt.
    for path in [
        output.join(BLOCK_ACCESS_FILE),
        output.join(BLOCK_ACCESS_INDEX_FILE),
        output.join(GET_BLOCK_INDEX_FILE),
        output.join(PREVIOUS_BLOCKHASH_TAIL_FILE),
    ] {
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("fingerprint repair artifact {}", path.display()));
            }
        };
        files.push(
            metadata
                .map(|metadata| -> Result<RepairCompactedFileFingerprint> {
                    Ok(RepairCompactedFileFingerprint {
                        device: metadata.dev(),
                        inode: metadata.ino(),
                        bytes: metadata.len(),
                        modified: metadata.modified().with_context(|| {
                            format!("read modification time for {}", path.display())
                        })?,
                    })
                })
                .transpose()?,
        );
    }
    Ok(RepairCompactedFingerprint {
        files,
        epoch: bundle.epoch,
        epoch_start_slot: bundle.epoch_start_slot,
        epoch_end_slot: bundle.epoch_end_slot,
        live_blocks: bundle.live_blocks,
        rpc_only_blocks: bundle.rpc_only_blocks,
        produced_blocks: bundle.produced_blocks,
    })
}

fn cached_repair_compacted(
    output: &Path,
    fingerprint: &RepairCompactedFingerprint,
) -> Option<PublishedRepairCompacted> {
    let cache = REPAIR_COMPACTED_VALIDATION_CACHE
        .get_or_init(|| StdMutex::new(VecDeque::with_capacity(MAX_REPAIR_COMPACTED_CACHE_ENTRIES)));
    let cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache
        .iter()
        .find(|(path, cached_fingerprint, _)| path == output && cached_fingerprint == fingerprint)
        .map(|(_, _, receipt)| receipt.clone())
}

fn cache_repair_compacted(
    output: &Path,
    fingerprint: RepairCompactedFingerprint,
    receipt: PublishedRepairCompacted,
) {
    let cache = REPAIR_COMPACTED_VALIDATION_CACHE
        .get_or_init(|| StdMutex::new(VecDeque::with_capacity(MAX_REPAIR_COMPACTED_CACHE_ENTRIES)));
    let mut cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.retain(|(path, _, _)| path != output);
    cache.push_back((output.to_path_buf(), fingerprint, receipt));
    while cache.len() > MAX_REPAIR_COMPACTED_CACHE_ENTRIES {
        cache.pop_front();
    }
}

fn repair_materialized_output(config: &NasPipelineConfig, epoch: u64) -> PathBuf {
    config
        .state_root
        .join("live-repair-materialized")
        .join(format!("epoch-{epoch}"))
}

fn repair_materialization_progress_path(config: &NasPipelineConfig, epoch: u64) -> PathBuf {
    config
        .state_root
        .join("live-repair-materialized")
        .join(format!(".epoch-{epoch}.repair-materialize-stage"))
        .join("repair/materialization-progress.json")
}

fn repair_hot_progress_path(config: &NasPipelineConfig, epoch: u64) -> PathBuf {
    config
        .archive_root
        .join(format!(".epoch-{epoch}.repair-hot-stage"))
        .join("repair/hot-progress.json")
}

fn repair_process_argv_matches(
    bytes: &[u8],
    repair_bin: &Path,
    command: &str,
    input: &Path,
    output: &Path,
) -> bool {
    let mut args = bytes.split(|byte| *byte == 0);
    args.next() == Some(repair_bin.as_os_str().as_bytes())
        && args.next() == Some(command.as_bytes())
        && args.next() == Some(input.as_os_str().as_bytes())
        && args.next() == Some(output.as_os_str().as_bytes())
}

fn repair_process_matches(
    pid: u32,
    repair_bin: &Path,
    command: &str,
    input: &Path,
    output: &Path,
) -> bool {
    fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline"))
        .is_ok_and(|bytes| repair_process_argv_matches(&bytes, repair_bin, command, input, output))
}

fn find_repair_process(
    repair_bin: &Path,
    command: &str,
    input: &Path,
    output: &Path,
) -> Option<u32> {
    fs::read_dir("/proc")
        .ok()?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let pid = entry.file_name().to_str()?.parse::<u32>().ok()?;
            repair_process_matches(pid, repair_bin, command, input, output).then_some(pid)
        })
        .min()
}

fn read_active_repair_progress(
    path: &Path,
    epoch: u64,
    produced_blocks: u64,
    now: u64,
    repair_bin: &Path,
    command: &str,
    input: &Path,
    output: &Path,
    allowed_phases: &[&str],
) -> Option<ProgressSnapshot> {
    const MAX_PROGRESS_BYTES: u64 = 1024 * 1024;
    let metadata = fs::symlink_metadata(path).ok()?;
    if !metadata.file_type().is_file() || metadata.len() == 0 || metadata.len() > MAX_PROGRESS_BYTES
    {
        return None;
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .ok()?
        .take(MAX_PROGRESS_BYTES + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > MAX_PROGRESS_BYTES {
        return None;
    }
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    if value
        .get("epoch")
        .and_then(Value::as_u64)
        .is_some_and(|progress_epoch| progress_epoch != epoch)
    {
        return None;
    }
    let mut progress = parse_progress_bytes(&bytes).ok()?;
    let phase = progress.phase.as_deref()?;
    if !allowed_phases.contains(&phase)
        || progress
            .state
            .as_deref()
            .is_some_and(|state| state != "running")
    {
        return None;
    }
    let updated = progress
        .updated_unix_secs
        .or_else(|| modified_unix_secs(path))?;
    if now.saturating_sub(updated) > PROGRESS_STALE_SECS {
        return None;
    }
    let pid = match progress.pid {
        Some(pid) if repair_process_matches(pid, repair_bin, command, input, output) => pid,
        Some(_) => return None,
        None => find_repair_process(repair_bin, command, input, output)?,
    };
    if progress.blocks_done > produced_blocks {
        return None;
    }
    progress.pid = Some(pid);
    progress.state = Some("running".to_string());
    progress.blocks_total = produced_blocks;
    progress.updated_unix_secs = Some(updated);
    progress.rss_bytes = progress.rss_bytes.or_else(|| process_rss_bytes(pid));
    progress.peak_rss_bytes = progress
        .peak_rss_bytes
        .or_else(|| process_peak_rss_bytes(pid))
        .map(|peak| peak.max(progress.rss_bytes.unwrap_or(0)));
    progress.progress_pct = (produced_blocks > 0)
        .then(|| (progress.blocks_done as f64 / produced_blocks as f64 * 100.0).min(100.0));
    progress.eta_secs = progress.blocks_per_sec.and_then(|rate| {
        positive_finite_option(Some(
            produced_blocks.saturating_sub(progress.blocks_done) as f64 / rate,
        ))
        .or_else(|| (progress.blocks_done == produced_blocks).then_some(0.0))
    });
    Some(progress)
}

fn active_repair_progress(
    config: &NasPipelineConfig,
    capture_dir: &Path,
    bundle: &PublishedRepairBundle,
    now: u64,
) -> Option<ProgressSnapshot> {
    let repair_bin = effective_repair_blockzilla_bin(config);
    let materialized_output = repair_materialized_output(config, bundle.epoch);
    let archive_output = config.archive_root.join(format!("epoch-{}", bundle.epoch));
    let materialization = read_active_repair_progress(
        &repair_materialization_progress_path(config, bundle.epoch),
        bundle.epoch,
        bundle.produced_blocks,
        now,
        repair_bin,
        "materialize-archive-v2-live-repair",
        capture_dir,
        &materialized_output,
        &["materializing"],
    );
    let hot = read_active_repair_progress(
        &repair_hot_progress_path(config, bundle.epoch),
        bundle.epoch,
        bundle.produced_blocks,
        now,
        repair_bin,
        "build-archive-v2-degraded-hot-blocks-from-repair",
        &materialized_output,
        &archive_output,
        &["building_hot_archive", "Archive V2 Live Hot Write"],
    );
    [materialization, hot]
        .into_iter()
        .flatten()
        .max_by_key(|progress| progress.updated_unix_secs.unwrap_or_default())
}

fn read_published_repair_compacted(
    output: &Path,
    bundle: &PublishedRepairBundle,
) -> Result<Option<PublishedRepairCompacted>> {
    let marker_path = output.join(LIVE_REPAIR_COMPACTED_MARKER);
    let marker_metadata = match fs::symlink_metadata(&marker_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("stat {}", marker_path.display())),
    };
    let output_metadata = fs::symlink_metadata(output)
        .with_context(|| format!("stat repair compacted output {}", output.display()))?;
    anyhow::ensure!(
        output_metadata.file_type().is_dir(),
        "repair compacted output is not a regular non-symlink directory"
    );
    anyhow::ensure!(
        marker_metadata.file_type().is_file()
            && marker_metadata.len() > 0
            && marker_metadata.len() <= MAX_LIVE_REPAIR_MARKER_BYTES,
        "{} must be a regular file with 1..={MAX_LIVE_REPAIR_MARKER_BYTES} bytes",
        marker_path.display()
    );
    let fingerprint = repair_compacted_fingerprint(output, bundle)?;
    if let Some(receipt) = cached_repair_compacted(output, &fingerprint) {
        return Ok(Some(receipt));
    }
    let marker: PublishedRepairCompactedMarker =
        read_repair_json_bounded(&marker_path, MAX_LIVE_REPAIR_MARKER_BYTES)?;

    anyhow::ensure!(
        marker.version == 1,
        "unsupported repair compacted marker version"
    );
    anyhow::ensure!(
        marker.state.len() <= 128
            && marker.state == "degraded_hot_archive_missing_poh_and_shredding"
            && !marker.canonical
            && !marker.publication_ready
            && marker.block_archive_ready,
        "repair compacted marker has incompatible state or readiness flags"
    );
    anyhow::ensure!(
        marker.epoch == bundle.epoch
            && marker.epoch_start_slot == bundle.epoch_start_slot
            && marker.epoch_end_slot == bundle.epoch_end_slot
            && marker.live_blocks == bundle.live_blocks
            && marker.rpc_only_blocks == bundle.rpc_only_blocks
            && marker.produced_blocks == bundle.produced_blocks
            && marker.live_blocks.checked_add(marker.rpc_only_blocks)
                == Some(marker.produced_blocks)
            && marker.produced_blocks > 0
            && marker.produced_blocks <= SLOTS_PER_EPOCH,
        "repair compacted epoch or block accounting differs from published repair bundle"
    );
    anyhow::ensure!(
        marker.compressed_bytes > 0 && marker.uncompressed_bytes > 0,
        "repair compacted byte accounting must be nonzero"
    );
    anyhow::ensure!(
        marker.files.blocks == BLOCKS_FILE
            && marker.files.index == BLOCK_INDEX_FILE
            && marker.files.meta == META_FILE
            && marker.files.registry == REGISTRY_FILE
            && marker.files.registry_counts == REGISTRY_COUNTS_FILE
            && marker.files.registry_index == REGISTRY_INDEX_FILE
            && marker.files.blockhashes == BLOCKHASH_REGISTRY_FILE
            && marker.files.signatures == SIGNATURES_FILE
            && marker.files.vote_hashes == VOTE_HASH_REGISTRY_FILE
            && marker.files.available_poh == LIVE_REPAIR_AVAILABLE_POH_FILE,
        "repair compacted marker does not use the fixed v1 file layout"
    );
    let access_file_layout_ready = marker.files.block_access.as_deref() == Some(BLOCK_ACCESS_FILE)
        && marker.files.block_access_index.as_deref() == Some(BLOCK_ACCESS_INDEX_FILE)
        && marker.files.get_block_index.as_deref() == Some(GET_BLOCK_INDEX_FILE)
        && marker.files.previous_blockhash_tail.as_deref() == Some(PREVIOUS_BLOCKHASH_TAIL_FILE);
    let access_file_layout_absent = marker.files.block_access.is_none()
        && marker.files.block_access_index.is_none()
        && marker.files.get_block_index.is_none()
        && marker.files.previous_blockhash_tail.is_none();
    anyhow::ensure!(
        (marker.block_access_ready && access_file_layout_ready)
            || (!marker.block_access_ready && access_file_layout_absent),
        "repair compacted block-access readiness does not match its fixed file layout"
    );
    anyhow::ensure!(
        marker.poh_coverage.available_records == marker.live_blocks
            && marker.poh_coverage.missing_records == marker.rpc_only_blocks
            && marker.poh_coverage.produced_id_space == marker.produced_blocks
            && marker.poh_coverage.record_ids_have_explicit_gaps
            && marker
                .poh_coverage
                .available_records
                .checked_add(marker.poh_coverage.missing_records)
                == Some(marker.produced_blocks)
            && marker.poh_coverage.missing_record_ids.len() as u64
                == marker.poh_coverage.missing_records,
        "repair compacted PoH coverage accounting is incompatible"
    );
    let mut previous_gap = None;
    for id in &marker.poh_coverage.missing_record_ids {
        anyhow::ensure!(
            u64::from(*id) < marker.produced_blocks
                && previous_gap.is_none_or(|previous| *id > previous),
            "repair compacted missing PoH ids are out of bounds or not sorted/unique"
        );
        previous_gap = Some(*id);
    }
    anyhow::ensure!(
        marker.shredding_coverage.available_records == 0
            && marker.shredding_coverage.missing_records == marker.produced_blocks
            && !marker.shredding_coverage.canonical_sidecar_emitted,
        "repair compacted shredding coverage is incompatible"
    );
    anyhow::ensure!(
        !marker.limitations.is_empty()
            && marker.limitations.len() <= 64
            && marker
                .limitations
                .iter()
                .all(|value| !value.is_empty() && value.len() <= 4096),
        "repair compacted limitations are empty or unreasonably large"
    );
    for (value, field) in [
        (
            marker.source_materialized_marker_sha256.as_str(),
            "source_materialized_marker_sha256",
        ),
        (
            marker.source_manifest_sha256.as_str(),
            "source_manifest_sha256",
        ),
        (
            marker.source_merge_plan_sha256.as_str(),
            "source_merge_plan_sha256",
        ),
    ] {
        validate_repair_sha256(value, field)?;
    }

    for forbidden in [
        "READY",
        LIVE_READY_MARKER,
        POH_FILE,
        SHREDDING_FILE,
        "poh/poh.wincode",
        "shredding/shredding.wincode",
    ] {
        anyhow::ensure!(
            !output.join(forbidden).exists(),
            "degraded repair output contains forbidden canonical artifact {forbidden}"
        );
    }
    if !marker.block_access_ready {
        for forbidden in [
            BLOCK_ACCESS_FILE,
            BLOCK_ACCESS_INDEX_FILE,
            GET_BLOCK_INDEX_FILE,
            PREVIOUS_BLOCKHASH_TAIL_FILE,
        ] {
            let path = output.join(forbidden);
            match fs::symlink_metadata(&path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Ok(_) => anyhow::bail!(
                    "legacy degraded repair output contains undeclared block-access artifact {forbidden}"
                ),
                Err(error) => {
                    return Err(error).with_context(|| format!("stat {}", path.display()));
                }
            }
        }
    }

    let blocks_metadata = repair_regular_file_metadata(&output.join(BLOCKS_FILE), u64::MAX, false)?;
    repair_regular_file_metadata(&output.join(BLOCK_INDEX_FILE), u64::MAX, false)?;
    repair_regular_file_metadata(&output.join(META_FILE), MAX_LIVE_REPAIR_META_BYTES, false)?;
    let registry_metadata =
        repair_regular_file_metadata(&output.join(REGISTRY_FILE), u64::MAX, false)?;
    repair_regular_file_metadata(&output.join(REGISTRY_COUNTS_FILE), u64::MAX, false)?;
    repair_regular_file_metadata(&output.join(REGISTRY_INDEX_FILE), u64::MAX, false)?;
    let blockhash_metadata =
        repair_regular_file_metadata(&output.join(BLOCKHASH_REGISTRY_FILE), u64::MAX, false)?;
    let signatures_metadata =
        repair_regular_file_metadata(&output.join(SIGNATURES_FILE), u64::MAX, true)?;
    repair_regular_file_metadata(&output.join(VOTE_HASH_REGISTRY_FILE), u64::MAX, false)?;
    repair_regular_file_metadata(
        &output.join(LIVE_REPAIR_AVAILABLE_POH_FILE),
        u64::MAX,
        marker.poh_coverage.available_records == 0,
    )?;
    anyhow::ensure!(
        blocks_metadata.len() == marker.compressed_bytes,
        "repair compressed block blob byte length differs from marker"
    );
    anyhow::ensure!(
        registry_metadata.len().is_multiple_of(32),
        "repair pubkey registry byte length is not a multiple of 32"
    );
    anyhow::ensure!(
        blockhash_metadata.len()
            == marker
                .produced_blocks
                .checked_mul(32)
                .context("repair blockhash byte count overflows u64")?,
        "repair blockhash registry byte length differs from produced block count"
    );
    anyhow::ensure!(
        signatures_metadata.len()
            == marker
                .signatures
                .checked_mul(64)
                .context("repair signature byte count overflows u64")?,
        "repair signature sidecar byte length differs from marker"
    );

    let source_marker_path = output.join(LIVE_REPAIR_SOURCE_MATERIALIZED_MARKER);
    repair_regular_file_metadata(&source_marker_path, MAX_LIVE_REPAIR_MARKER_BYTES, false)?;
    anyhow::ensure!(
        repair_sha256_file(&source_marker_path)? == marker.source_materialized_marker_sha256,
        "repair source materialized marker digest differs from compacted marker"
    );
    let source: PublishedRepairSourceMaterializedMarker =
        read_repair_json_bounded(&source_marker_path, MAX_LIVE_REPAIR_MARKER_BYTES)?;
    anyhow::ensure!(
        source.version == 1
            && source.state == "repair_materialized_missing_poh_and_shredding"
            && !source.canonical
            && !source.publication_ready
            && source.epoch == marker.epoch
            && source.epoch_start_slot == marker.epoch_start_slot
            && source.epoch_end_slot == marker.epoch_end_slot
            && source.live_blocks == marker.live_blocks
            && source.rpc_only_blocks == marker.rpc_only_blocks
            && source.produced_blocks == marker.produced_blocks
            && source.transactions == marker.transactions
            && source.manifest_sha256 == marker.source_manifest_sha256
            && source.merge_plan_sha256 == marker.source_merge_plan_sha256,
        "repair source materialized receipt differs from compacted marker"
    );

    validate_repair_hot_index(output, &marker)?;
    validate_repair_hot_meta(output, &marker)?;
    validate_repair_available_poh(output, &marker)?;
    if marker.block_access_ready {
        validate_repair_block_access(output, &marker)?;
    }

    let receipt = PublishedRepairCompacted {
        updated_unix_secs: modified_unix_secs(&marker_path).unwrap_or_default(),
        block_access_ready: marker.block_access_ready,
    };
    cache_repair_compacted(output, fingerprint, receipt.clone());
    Ok(Some(receipt))
}

fn find_live_producer_process(capture_dir: &Path) -> Option<u32> {
    let matches = fs::read_dir("/proc").ok()?.flatten().filter_map(|entry| {
        let pid = entry.file_name().to_str()?.parse::<u32>().ok()?;
        process_cmdline_matches_live_producer(pid, capture_dir).then_some(pid)
    });
    unique_live_producer_pid(matches)
}

fn unique_live_producer_pid(pids: impl IntoIterator<Item = u32>) -> Option<u32> {
    let mut pids = pids.into_iter();
    let only = pids.next()?;
    pids.next().is_none().then_some(only)
}

fn process_cmdline_matches_live_producer(pid: u32, capture_dir: &Path) -> bool {
    fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline"))
        .is_ok_and(|bytes| live_producer_argv_matches_bytes(&bytes, capture_dir))
}

fn live_producer_argv_matches_bytes(bytes: &[u8], capture_dir: &Path) -> bool {
    if bytes.last() != Some(&0) {
        return false;
    }
    let args = bytes[..bytes.len().saturating_sub(1)]
        .split(|byte| *byte == 0)
        .collect::<Vec<_>>();
    let executable = args
        .first()
        .and_then(|arg| arg.rsplit(|byte| *byte == b'/').next());
    if executable != Some(b"blockzilla-live-producer".as_slice())
        || args.get(1).copied() != Some(b"capture-grpc")
    {
        return false;
    }

    let expected = capture_dir.as_os_str().as_bytes();
    let mut archive_dirs = Vec::new();
    for (index, arg) in args.iter().enumerate().skip(2) {
        if *arg == b"--archive-dir" {
            if let Some(value) = args.get(index + 1) {
                archive_dirs.push(*value);
            }
        } else if let Some(value) = arg.strip_prefix(b"--archive-dir=") {
            archive_dirs.push(value);
        }
    }
    archive_dirs.as_slice() == [expected]
}

fn refresh_live_producer_process_metrics(
    progress: &mut ProgressSnapshot,
    capture_dir: &Path,
    _now: u64,
) {
    if !matches!(
        progress.state.as_deref(),
        Some("capturing" | "running" | "starting") | None
    ) {
        progress.pid = None;
        progress.rss_bytes = None;
        progress.peak_rss_bytes = None;
        return;
    }
    // Always enumerate every exact match. A valid PID hint must not bypass
    // ambiguity detection when a second producer claims the same directory.
    let pid = find_live_producer_process(capture_dir);
    progress.pid = pid;
    if let Some(pid) = pid {
        progress.rss_bytes = process_rss_bytes(pid);
        progress.peak_rss_bytes =
            process_peak_rss_bytes(pid).map(|peak| peak.max(progress.rss_bytes.unwrap_or(0)));
    } else {
        progress.rss_bytes = None;
        progress.peak_rss_bytes = None;
        progress.blocks_per_sec = None;
        progress.slots_per_sec = None;
        progress.eta_secs = None;
    }
}

fn apply_repair_supersession(captures: &mut [LiveCaptureSnapshot]) {
    let bundles = captures
        .iter()
        .filter(|capture| {
            !capture.source_capture_ids.is_empty()
                && matches!(
                    capture.state,
                    LiveState::RepairRequired | LiveState::Packaging | LiveState::Packaged
                )
        })
        .map(|capture| {
            (
                capture.id.clone(),
                capture.epoch,
                capture
                    .source_capture_ids
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                capture.updated_unix_secs,
            )
        })
        .collect::<Vec<_>>();

    for source in captures.iter_mut() {
        source.superseded_by = None;
        if source.epoch.is_none()
            || matches!(
                source.state,
                LiveState::Capturing
                    | LiveState::RepairGate
                    | LiveState::RepairRequired
                    | LiveState::Packaging
            )
        {
            continue;
        }
        source.superseded_by = bundles
            .iter()
            .filter(|(bundle_id, epoch, source_ids, _)| {
                bundle_id != &source.id && *epoch == source.epoch && source_ids.contains(&source.id)
            })
            .max_by(|left, right| left.3.cmp(&right.3).then_with(|| left.0.cmp(&right.0)))
            .map(|(bundle_id, _, _, _)| bundle_id.clone());
    }
}

fn canonicalize_live_captures(
    mut captures: Vec<LiveCaptureSnapshot>,
) -> (Vec<LiveCaptureSnapshot>, Option<u64>) {
    let current_index = captures
        .iter()
        .enumerate()
        .filter(|(_, capture)| capture.state == LiveState::Capturing)
        .max_by(|(_, left), (_, right)| {
            left.epoch
                .cmp(&right.epoch)
                .then_with(|| left.updated_unix_secs.cmp(&right.updated_unix_secs))
                .then_with(|| left.id.cmp(&right.id))
        })
        .map(|(index, _)| index);
    let Some(current_index) = current_index else {
        return (captures, None);
    };
    captures[current_index].is_current = true;
    let current_id = captures[current_index].id.clone();
    let current_epoch = captures[current_index].epoch;
    for (index, capture) in captures.iter_mut().enumerate() {
        if index == current_index || capture.state != LiveState::Capturing {
            continue;
        }
        capture.state = LiveState::Blocked;
        capture.message = Some(format!(
            "concurrent live capture is not canonical; current capture is {current_id}"
        ));
    }
    (captures, current_epoch)
}

fn classify_live_capture(
    config: &NasPipelineConfig,
    runtime: &RuntimeState,
    capture_dir: PathBuf,
    now: u64,
) -> LiveCaptureSnapshot {
    let id = capture_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("capture")
        .to_string();
    match read_published_repair_bundle(&config.live_root, &capture_dir) {
        Ok(Some(bundle)) => {
            let output_path = config.archive_root.join(format!("epoch-{}", bundle.epoch));
            let (state, phase, progress_state, message, state_updated, active_progress) =
                match read_published_repair_compacted(&output_path, &bundle) {
                    Ok(Some(compacted)) => {
                        let incomplete = if compacted.block_access_ready {
                            "only canonical PoH/shredding remain incomplete"
                        } else {
                            "canonical PoH/shredding and block-access sidecars remain incomplete"
                        };
                        (
                            LiveState::Packaged,
                            "repair_compacted",
                            "packaged",
                            format!(
                                "degraded repair block archive covers slots {}..{}; {incomplete}",
                                bundle.epoch_start_slot, bundle.epoch_end_slot
                            ),
                            compacted.updated_unix_secs,
                            None,
                        )
                    }
                    Ok(None) => {
                        if let Some(progress) =
                            active_repair_progress(config, &capture_dir, &bundle, now)
                        {
                            let phase = progress.phase.as_deref().unwrap_or("repair").to_string();
                            let updated = progress.updated_unix_secs.unwrap_or_default();
                            (
                                LiveState::Packaging,
                                "repair_packaging",
                                "running",
                                format!(
                                    "repair materialization/compaction is active ({phase}); canonical PoH/shredding remain incomplete"
                                ),
                                updated,
                                Some(progress),
                            )
                        } else {
                            (
                                LiveState::RepairRequired,
                                "repair_bundle",
                                "repair_required",
                                format!(
                                    "atomic repair bundle covers slots {}..{}; repair-aware compact materialization is required and canonical PoH/shredding remain incomplete",
                                    bundle.epoch_start_slot, bundle.epoch_end_slot
                                ),
                                0,
                                None,
                            )
                        }
                    }
                    Err(error) => (
                        LiveState::RepairRequired,
                        "repair_bundle",
                        "repair_required",
                        format!(
                            "invalid {LIVE_REPAIR_COMPACTED_MARKER}; degraded output was not accepted as packaged and repair remains required: {error:#}"
                        ),
                        modified_unix_secs(&output_path.join(LIVE_REPAIR_COMPACTED_MARKER))
                            .unwrap_or_default(),
                        None,
                    ),
                };
            let updated_unix_secs = bundle.updated_unix_secs.max(state_updated);
            let updated_unix_secs = if updated_unix_secs == 0 {
                now
            } else {
                updated_unix_secs
            };
            let mut progress = active_progress.unwrap_or_else(|| ProgressSnapshot {
                phase: Some(phase.to_string()),
                state: Some(progress_state.to_string()),
                blocks_done: bundle.produced_blocks,
                blocks_total: bundle.produced_blocks,
                first_slot: Some(bundle.first_produced_slot),
                last_slot: Some(bundle.last_produced_slot),
                progress_pct: Some(100.0),
                updated_unix_secs: Some(updated_unix_secs),
                ..ProgressSnapshot::default()
            });
            progress.first_slot = progress.first_slot.or(Some(bundle.first_produced_slot));
            progress.last_slot = progress.last_slot.or(Some(bundle.last_produced_slot));
            hide_stale_live_rates(&mut progress, now);
            let blocks_written = bundle.produced_blocks;
            let eta_secs = (state == LiveState::Packaging)
                .then_some(progress.eta_secs)
                .flatten();
            let rss_bytes = (state == LiveState::Packaging)
                .then_some(progress.rss_bytes)
                .flatten();
            let peak_rss_bytes = (state == LiveState::Packaging)
                .then_some(progress.peak_rss_bytes)
                .flatten();
            return LiveCaptureSnapshot {
                id,
                epoch: Some(bundle.epoch),
                is_current: false,
                state,
                capture_dir,
                output_path: Some(output_path),
                ready_to_package: false,
                repair_gate: false,
                source_capture_ids: bundle.source_capture_ids,
                superseded_by: None,
                first_slot: Some(bundle.first_produced_slot),
                last_slot: Some(bundle.last_produced_slot),
                blocks_written,
                artifacts: Vec::new(),
                progress,
                eta_secs,
                slots_per_sec: None,
                rss_bytes,
                peak_rss_bytes,
                message: Some(message),
                updated_unix_secs,
            };
        }
        Ok(None) => {}
        Err(error) => {
            let epoch = parse_epoch_name(&id);
            return LiveCaptureSnapshot {
                id,
                epoch,
                is_current: false,
                state: LiveState::Blocked,
                capture_dir: capture_dir.clone(),
                output_path: epoch.map(|epoch| config.archive_root.join(format!("epoch-{epoch}"))),
                ready_to_package: false,
                repair_gate: false,
                source_capture_ids: Vec::new(),
                superseded_by: None,
                first_slot: None,
                last_slot: None,
                blocks_written: 0,
                artifacts: Vec::new(),
                progress: ProgressSnapshot::default(),
                eta_secs: None,
                slots_per_sec: None,
                rss_bytes: None,
                peak_rss_bytes: None,
                message: Some(format!(
                    "invalid {LIVE_REPAIR_REQUIRED_MARKER}; no source capture was superseded: {error:#}"
                )),
                updated_unix_secs: modified_unix_secs(&capture_dir).unwrap_or(now),
            };
        }
    }
    let mut progress = [
        capture_dir.join("progress.json"),
        capture_dir.join("journal/progress.json"),
    ]
    .into_iter()
    .filter_map(|path| {
        read_progress(&path).map(|mut progress| {
            let freshness = progress
                .updated_unix_secs
                .or_else(|| modified_unix_secs(&path))
                .unwrap_or_default();
            if progress.updated_unix_secs.is_none() && freshness > 0 {
                progress.updated_unix_secs = Some(freshness);
            }
            (freshness, progress)
        })
    })
    .max_by_key(|(freshness, _)| *freshness)
    .map(|(_, progress)| progress)
    .unwrap_or_default();
    let journal_path = capture_dir.join("journal/grpc-blocks.jsonl");
    let journal = read_live_journal_tail(&journal_path);
    merge_live_journal_progress(
        &mut progress,
        journal.as_ref(),
        journal
            .as_ref()
            .and_then(|_| modified_unix_secs(&journal_path)),
    );
    refresh_live_epoch_metrics(&mut progress);
    refresh_live_producer_process_metrics(&mut progress, &capture_dir, now);
    // Capture directories are epoch-scoped. A repair/tail block can belong to
    // the next epoch, so `last_slot` must be the weakest fallback; otherwise an
    // epoch-1000 capture with a 1001 tail is mislabeled as epoch 1001 and
    // collides with the real current capture.
    let epoch = parse_epoch_name(&id)
        .or_else(|| progress.first_slot.map(|slot| slot / SLOTS_PER_EPOCH))
        .or_else(|| journal.as_ref().and_then(|row| json_u64(row, &["epoch"])))
        .or_else(|| progress.last_slot.map(|slot| slot / SLOTS_PER_EPOCH));
    let output_path = epoch.map(|epoch| config.archive_root.join(format!("epoch-{epoch}")));
    let ready = capture_dir.join(LIVE_READY_MARKER).is_file();
    let finalize_needed = capture_dir.join(LIVE_FINALIZE_MARKER).is_file();
    let active = matches!(
        runtime.finalizer.as_ref().map(|child| &child.kind),
        Some(ChildKind::LiveFinalizer { id: active_id, .. }) if active_id == &id
    );
    let failure = runtime.failures.get(&format!("live:{id}")).cloned();
    let output_complete = output_path
        .as_deref()
        .is_some_and(|path| historical_archive_strict_complete(path, !config.no_access, false));
    let output_packaged = output_path.as_deref().is_some_and(live_archive_packaged);
    let output_owner = output_path.as_deref().and_then(read_ownership);
    let owner_collision = output_owner
        .as_ref()
        .is_some_and(|owner| owner.kind != "live_finalizer" || owner.id != id);
    let owner = output_owner.filter(|owner| owner.kind == "live_finalizer" && owner.id == id);
    let owner_process_active = owner.as_ref().is_some_and(|owner| {
        owner.pid.is_some_and(|pid| {
            output_path
                .as_deref()
                .is_some_and(|output| process_cmdline_contains(pid, output))
        })
    });
    let owner_finalizing = owner_process_active
        && owner.as_ref().is_some_and(|owner| {
            matches!(
                owner.state.as_str(),
                "registry_merge" | "mphf_build" | "hot_rewrite" | "packaging"
            )
        });
    if owner_finalizing {
        progress.pid = owner.as_ref().and_then(|owner| owner.pid);
        progress.rss_bytes = progress.pid.and_then(process_rss_bytes);
        progress.peak_rss_bytes = progress.pid.and_then(process_peak_rss_bytes);
        progress.phase = owner.as_ref().map(|owner| owner.state.clone());
        progress.state = Some("running".to_string());
    }
    let owned_partial = owner.is_some()
        && output_path.as_deref().is_some_and(|output| {
            output.join(LIVE_REGISTRY_READY_MARKER).is_file()
                || !is_nonempty_file(&output.join(REGISTRY_FILE))
        });
    let output_ambiguous = output_path.as_deref().is_some_and(directory_has_entries)
        && !output_complete
        && !output_packaged
        && !owned_partial;
    let ownership_failure = owner.as_ref().and_then(|owner| {
        if owner.state == "failed" {
            owner.message.clone()
        } else if matches!(
            owner.state.as_str(),
            "registry_merge" | "mphf_build" | "hot_rewrite" | "packaging"
        ) && owner.pid.is_some()
            && !owner_process_active
        {
            Some(format!(
                "pipeline-owned live finalizer stage {} is no longer running",
                owner.state
            ))
        } else {
            None
        }
    });
    if let Some(pid) = progress.pid {
        progress.rss_bytes = process_rss_bytes(pid).or(progress.rss_bytes);
        progress.peak_rss_bytes = process_peak_rss_bytes(pid)
            .or(progress.peak_rss_bytes)
            .map(|peak| peak.max(progress.rss_bytes.unwrap_or(0)));
    }
    if progress.first_slot.is_none() {
        progress.first_slot = journal
            .as_ref()
            .and_then(|row| json_u64(row, &["first_slot"]));
    }
    let progress_state_active = match progress.state.as_deref() {
        Some("capturing" | "running" | "starting") => true,
        // In particular, the producer publishes `closed` at an epoch boundary
        // and `stopped` on shutdown. Freshness alone must not turn either
        // terminal snapshot back into an active capture.
        Some(_) => false,
        None => true,
    };
    let progress_active = progress_state_active && progress_is_alive(&progress, now);

    let (state, message) = if owner_collision {
        (
            LiveState::Blocked,
            Some("target epoch is owned by a different pipeline item".to_string()),
        )
    } else if output_complete {
        (LiveState::Complete, None)
    } else if output_packaged {
        (
            LiveState::Packaged,
            Some(
                "compact archive packaged; first-seen manifest/access sidecars are not canonical"
                    .to_string(),
            ),
        )
    } else if active || owner_finalizing {
        (LiveState::Packaging, None)
    } else if let Some(message) = failure.or(ownership_failure) {
        (LiveState::Failed, Some(message))
    } else if progress_active {
        (
            LiveState::Capturing,
            ready.then(|| {
                "READY-TO-PACKAGE is ignored while the capture is still active".to_string()
            }),
        )
    } else if ready && !finalize_needed {
        (
            LiveState::Blocked,
            Some("READY-TO-PACKAGE exists without a closed-capture marker".to_string()),
        )
    } else if ready && output_path.is_none() {
        (
            LiveState::Blocked,
            Some("ready live capture has no derivable target epoch".to_string()),
        )
    } else if ready && output_ambiguous {
        (
            LiveState::Blocked,
            Some("target epoch output already exists but is not complete".to_string()),
        )
    } else if ready {
        (LiveState::ReadyToPackage, None)
    } else if finalize_needed {
        (
            LiveState::RepairGate,
            Some("capture is closed; waiting for READY-TO-PACKAGE repair approval".to_string()),
        )
    } else {
        (
            LiveState::Blocked,
            Some("capture is neither active nor marked ready for packaging".to_string()),
        )
    };
    hide_stale_live_rates(&mut progress, now);
    let updated_unix_secs = progress
        .updated_unix_secs
        .or_else(|| modified_unix_secs(&capture_dir))
        .unwrap_or(now);
    let artifacts = live_capture_artifacts(config, &capture_dir, output_path.as_deref(), state);
    let (eta_secs, slots_per_sec) = if state == LiveState::Capturing {
        (progress.eta_secs, progress.slots_per_sec)
    } else {
        (None, None)
    };
    let (rss_bytes, peak_rss_bytes) =
        if matches!(state, LiveState::Capturing | LiveState::Packaging) {
            (progress.rss_bytes, progress.peak_rss_bytes)
        } else {
            (None, None)
        };
    LiveCaptureSnapshot {
        id,
        epoch,
        is_current: false,
        state,
        capture_dir,
        output_path,
        ready_to_package: ready,
        repair_gate: finalize_needed && !ready,
        source_capture_ids: Vec::new(),
        superseded_by: None,
        first_slot: progress.first_slot,
        last_slot: progress.last_slot,
        blocks_written: progress.blocks_done,
        artifacts,
        progress,
        eta_secs,
        slots_per_sec,
        rss_bytes,
        peak_rss_bytes,
        message,
        updated_unix_secs,
    }
}

fn live_capture_artifacts(
    config: &NasPipelineConfig,
    capture_dir: &Path,
    output: Option<&Path>,
    state: LiveState,
) -> Vec<ArtifactSnapshot> {
    let source_poh = [
        capture_dir.join("poh/poh.wincode"),
        capture_dir.join(POH_FILE),
    ]
    .into_iter()
    .find(|path| path.is_file());
    let source_shredding = [
        capture_dir.join("shredding/shredding.wincode"),
        capture_dir.join(SHREDDING_FILE),
    ]
    .into_iter()
    .find(|path| path.is_file());
    let source_artifact = |kind, path: Option<&Path>| ArtifactSnapshot {
        kind,
        state: path.map_or(ArtifactState::Missing, |path| {
            if is_nonempty_file(path) {
                ArtifactState::Present
            } else {
                ArtifactState::Invalid
            }
        }),
        requirement: ArtifactRequirement::ScanInput,
        required_now: matches!(state, LiveState::ReadyToPackage | LiveState::Packaging),
        bytes: path.map(file_len).unwrap_or(0),
        modified_unix_secs: path.and_then(modified_unix_secs),
        message: None,
    };
    let mut artifacts = vec![
        source_artifact(ArtifactKind::SourcePohInfo, source_poh.as_deref()),
        source_artifact(
            ArtifactKind::SourceShreddingInfo,
            source_shredding.as_deref(),
        ),
    ];
    let Some(output) = output else {
        return artifacts;
    };
    let packaged = matches!(state, LiveState::Packaged | LiveState::Complete);
    artifacts.extend([
        archive_file_artifact(
            ArtifactKind::Metadata,
            output.join(META_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::Registry,
            output.join(REGISTRY_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::RegistryCounts,
            output.join(REGISTRY_COUNTS_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::RegistryIndex,
            output.join(REGISTRY_INDEX_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::BlockhashRegistry,
            output.join(BLOCKHASH_REGISTRY_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::Blocks,
            output.join(BLOCKS_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::BlockIndex,
            output.join(BLOCK_INDEX_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::Poh,
            output.join(POH_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::Shredding,
            output.join(SHREDDING_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            false,
        ),
        archive_file_artifact(
            ArtifactKind::Signatures,
            output.join(SIGNATURES_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            true,
        ),
        archive_file_artifact(
            ArtifactKind::VoteHashRegistry,
            output.join(VOTE_HASH_REGISTRY_FILE),
            ArtifactRequirement::FinalOutput,
            packaged,
            true,
        ),
        access_artifact(
            ArtifactKind::BlockAccess,
            output.join(BLOCK_ACCESS_FILE),
            !config.no_access,
            state == LiveState::Complete,
            false,
        ),
        access_artifact(
            ArtifactKind::BlockAccessIndex,
            output.join(BLOCK_ACCESS_INDEX_FILE),
            !config.no_access,
            state == LiveState::Complete,
            false,
        ),
    ]);
    artifacts
}

fn runtime_lanes(runtime: &RuntimeState) -> Vec<LaneSnapshot> {
    let now = unix_now();
    let mut lanes = runtime
        .acquisitions
        .iter()
        .map(|(epoch, child)| lane_from_child(child, Some(*epoch), None, now, runtime))
        .chain(
            runtime
                .scans
                .iter()
                .map(|(epoch, child)| lane_from_child(child, Some(*epoch), None, now, runtime)),
        )
        .collect::<Vec<_>>();
    if let Some(finalizer) = runtime.finalizer.as_ref() {
        let (epoch, capture_id) = match &finalizer.kind {
            ChildKind::CarDownload { epoch, .. } | ChildKind::CarPreflight { epoch, .. } => {
                (Some(*epoch), None)
            }
            ChildKind::HistoricalFinalizer { epoch } => (Some(*epoch), None),
            ChildKind::LiveFinalizer { id, epoch, .. } => (*epoch, Some(id.clone())),
            ChildKind::HistoricalScan { epoch } => (Some(*epoch), None),
        };
        lanes.push(lane_from_child(finalizer, epoch, capture_id, now, runtime));
    }
    lanes
}

fn lane_from_child(
    child: &ManagedChild,
    epoch: Option<u64>,
    capture_id: Option<String>,
    now: u64,
    runtime: &RuntimeState,
) -> LaneSnapshot {
    let mut progress = read_progress(&child.progress_path).unwrap_or_default();
    let rss_bytes = child.pid.and_then(|pid| {
        if matches!(
            &child.kind,
            ChildKind::CarDownload { .. } | ChildKind::CarPreflight { .. }
        ) {
            process_tree_rss_bytes(pid)
        } else {
            process_rss_bytes(pid)
        }
    });
    progress.rss_bytes = rss_bytes;
    let (kind, phase) = match child.kind {
        ChildKind::CarDownload { .. } => ("car_download", "download"),
        ChildKind::CarPreflight { .. } => ("car_preflight", "preflight"),
        ChildKind::HistoricalScan { .. } => ("historical_scan", "scan"),
        ChildKind::HistoricalFinalizer { .. } => ("historical_finalizer", "finalize"),
        ChildKind::LiveFinalizer { phase, .. } => ("live_finalizer", phase.as_str()),
    };
    LaneSnapshot {
        id: child.kind.key(),
        kind: kind.to_string(),
        epoch,
        capture_id,
        phase: progress.phase.clone().unwrap_or_else(|| phase.to_string()),
        state: if runtime.paused_jobs.contains(&child.kind.key()) {
            "paused".to_string()
        } else {
            "running".to_string()
        },
        pid: child.pid,
        progress,
        rss_bytes,
        started_unix_secs: Some(child.started_unix_secs),
        updated_unix_secs: now,
    }
}

async fn schedule_work(
    config: &NasPipelineConfig,
    snapshot: &PipelineSnapshot,
    runtime: &mut RuntimeState,
) -> Result<()> {
    if runtime.scheduler_paused {
        return Ok(());
    }
    if !snapshot.inventory.complete {
        return Ok(());
    }
    let finalizer_active = runtime.finalizer.is_some()
        || snapshot
            .epochs
            .iter()
            .any(|epoch| epoch.state == HistoricalState::Finalizing)
        || snapshot
            .live
            .iter()
            .any(|capture| capture.state == LiveState::Packaging);
    if finalizer_active {
        return Ok(());
    }

    // A closed capture with explicit READY approval already had live priority
    // before the sweep policy. Preserve that narrow exception, but never overlap
    // it with an acquisition or scan. Historical finalizers remain deferred.
    let adopted_active_scans = active_scan_count(&snapshot.epochs, runtime.scans.keys().copied());
    let adopted_acquisitions = snapshot
        .epochs
        .iter()
        .filter(|epoch| {
            !runtime.acquisitions.contains_key(&epoch.epoch)
                && acquisition_claim_active(config, epoch.epoch)
        })
        .count();
    if snapshot
        .finalizer_queue
        .first()
        .is_some_and(|task| task.kind == "live")
    {
        if !runtime.acquisitions.is_empty() || adopted_acquisitions > 0 || adopted_active_scans > 0
        {
            return Ok(());
        }
        if let Some(live_task) = snapshot
            .finalizer_queue
            .iter()
            .filter(|task| task.kind == "live")
            .find(|task| finalizer_is_admissible(config, &snapshot.machine, task))
        {
            attempt_finalizer(config, snapshot, runtime, live_task).await?;
            // The cross-process finalizer lock may defer this task. Only a real
            // spawn earns live priority; otherwise continue the census so a
            // blocked READY capture cannot starve acquisitions/scans forever.
            if runtime.finalizer.is_some() {
                return Ok(());
            }
        }
    }

    // Acquisition and preflight are a global census phase. Do not start a new
    // compact scan until every runnable acquisition task in this inventory
    // generation has either completed or reached a terminal failure/gap.
    let acquisition_candidates = prioritized_epochs(config, &snapshot.epochs)
        .into_iter()
        .filter_map(|epoch| acquisition_action(config, epoch).map(|action| (epoch, action)))
        .collect::<Vec<_>>();
    if !acquisition_candidates.is_empty()
        || !runtime.acquisitions.is_empty()
        || adopted_acquisitions > 0
    {
        let slots = config
            .download_concurrency
            .saturating_sub(
                runtime
                    .acquisitions
                    .len()
                    .saturating_add(adopted_acquisitions),
            )
            .min(acquisition_memory_capacity(
                config,
                &snapshot.machine,
                runtime
                    .acquisitions
                    .len()
                    .saturating_add(adopted_acquisitions),
                snapshot
                    .lanes
                    .iter()
                    .filter(|lane| matches!(lane.kind.as_str(), "car_download" | "car_preflight"))
                    .filter_map(|lane| lane.rss_bytes)
                    .sum(),
            ));
        let disk_reserve = config.disk_reserve_gib.saturating_mul(1024 * 1024 * 1024);
        let active_download_bytes = active_download_projection(config, &snapshot.epochs, runtime);
        let mut car_disk_headroom = (snapshot.machine.car_disk_total_bytes > 0).then(|| {
            snapshot
                .machine
                .car_disk_available_bytes
                .saturating_sub(disk_reserve)
                .saturating_sub(active_download_bytes)
        });
        let mut admitted = 0usize;
        let mut acquisition_claimed_during_spawn = false;
        for (epoch, action) in acquisition_candidates {
            if admitted >= slots {
                break;
            }
            let download_projection = if action == AcquisitionAction::Download {
                let projected =
                    car_download_remaining_projection(config, &snapshot.epochs, epoch.epoch);
                if car_disk_headroom.is_none_or(|headroom| headroom < projected) {
                    continue;
                }
                projected
            } else {
                0
            };
            let result = match action {
                AcquisitionAction::Download => spawn_car_download(config, epoch.epoch).await,
                AcquisitionAction::Preflight => {
                    spawn_car_preflight(config, epoch.epoch, epoch.input_path.as_deref()).await
                }
            };
            match result {
                Ok(Some(child)) => {
                    runtime.acquisitions.insert(epoch.epoch, child);
                    admitted = admitted.saturating_add(1);
                    if let Some(headroom) = &mut car_disk_headroom {
                        *headroom = headroom.saturating_sub(download_projection);
                    }
                }
                Ok(None) => acquisition_claimed_during_spawn = true,
                Err(error) => {
                    let key = match action {
                        AcquisitionAction::Download => format!("download:{}", epoch.epoch),
                        AcquisitionAction::Preflight => format!("preflight:{}", epoch.epoch),
                    };
                    let message = format!("{key} spawn failed: {error:#}");
                    set_runtime_failure(config, runtime, key, message.clone());
                    record_error(config, runtime, "acquisition", message);
                }
            }
        }
        if !runtime.acquisitions.is_empty()
            || adopted_acquisitions > 0
            || acquisition_claimed_during_spawn
        {
            return Ok(());
        }
    }

    // A scan publishes its durable marker immediately before exiting. Keep the
    // child counted until it has actually been reaped, while also accounting
    // for adopted scans discovered from filesystem/process state after restart.
    let active_scans = adopted_active_scans;
    let slots = snapshot
        .summary
        .scan_capacity_admitted
        .saturating_sub(active_scans);
    let queued = prioritized_epochs(config, &snapshot.epochs)
        .into_iter()
        .filter(|epoch| epoch.state == HistoricalState::Queued)
        .filter(|epoch| acquisition_action(config, epoch).is_none())
        .take(slots)
        .cloned()
        .collect::<Vec<_>>();
    for epoch in queued {
        let child = spawn_historical_scan(config, &epoch).await?;
        runtime.scans.insert(epoch.epoch, child);
    }
    if active_scan_count(&snapshot.epochs, runtime.scans.keys().copied()) > 0
        || snapshot
            .epochs
            .iter()
            .any(|epoch| epoch.state == HistoricalState::Queued)
    {
        return Ok(());
    }

    let Some(task) =
        first_admissible_finalizer(config, &snapshot.machine, &snapshot.finalizer_queue)
    else {
        return Ok(());
    };
    attempt_finalizer(config, snapshot, runtime, task).await
}

async fn attempt_finalizer(
    config: &NasPipelineConfig,
    snapshot: &PipelineSnapshot,
    runtime: &mut RuntimeState,
    task: &FinalizerQueueItem,
) -> Result<()> {
    if finalizer_admission_blocked_reason(config, &snapshot.machine, task).is_some() {
        return Ok(());
    }
    match task.kind.as_str() {
        "live" => {
            let capture = snapshot
                .live
                .iter()
                .find(|capture| capture.id == task.id)
                .with_context(|| format!("queued live capture {} disappeared", task.id))?;
            let phase = LiveFinalizerPhase::parse(&task.phase)
                .with_context(|| format!("unknown live finalizer phase {}", task.phase))?;
            if let Some(child) = spawn_live_finalizer(config, capture, phase).await? {
                runtime.finalizer = Some(child);
            }
        }
        "historical" => {
            let epoch = task.epoch.context("historical finalizer has no epoch")?;
            runtime.finalizer = Some(spawn_historical_finalizer(config, epoch).await?);
        }
        kind => anyhow::bail!("unknown finalizer queue kind {kind}"),
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcquisitionAction {
    Download,
    Preflight,
}

fn acquisition_action(
    config: &NasPipelineConfig,
    epoch: &EpochSnapshot,
) -> Option<AcquisitionAction> {
    if epoch.state != HistoricalState::Queued {
        return None;
    }
    if acquisition_claim_active(config, epoch.epoch) {
        return None;
    }
    if epoch.input_path.is_none() && config.car_source_url_template.is_some() {
        return Some(AcquisitionAction::Download);
    }
    if config.preflight_car
        && !car_preflight_status(config, epoch.epoch, epoch.input_path.as_deref()).complete()
    {
        return Some(AcquisitionAction::Preflight);
    }
    None
}

fn active_scan_count(
    epochs: &[EpochSnapshot],
    managed_epochs: impl IntoIterator<Item = u64>,
) -> usize {
    let mut active = managed_epochs.into_iter().collect::<BTreeSet<_>>();
    active.extend(
        epochs
            .iter()
            .filter(|epoch| epoch.state == HistoricalState::Scanning)
            .map(|epoch| epoch.epoch),
    );
    active.len()
}

async fn spawn_car_download(
    config: &NasPipelineConfig,
    epoch: u64,
) -> Result<Option<ManagedChild>> {
    let Some(acquisition_lock) = try_acquire_acquisition_lock(config, epoch)? else {
        return Ok(None);
    };
    let (url, canonical_path, part_path) = car_download_paths(config, epoch)?;
    let alternate_canonical =
        if canonical_path.extension().and_then(|value| value.to_str()) == Some("zst") {
            config.car_root.join(format!("epoch-{epoch}.car"))
        } else {
            config.car_root.join(format!("epoch-{epoch}.car.zst"))
        };
    anyhow::ensure!(
        !config.car_root.join(format!("epoch-{epoch}.car")).exists()
            && !config
                .car_root
                .join(format!("epoch-{epoch}.car.zst"))
                .exists(),
        "refusing to download epoch {epoch}: a canonical CAR path already exists"
    );
    if let Some(parent) = part_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create CAR download directory {}", parent.display()))?;
    }
    let receipt_path = car_preflight_receipt_path(&config.state_root, epoch);
    if let Some(parent) = receipt_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create CAR preflight directory {}", parent.display()))?;
    }
    let progress_path = config
        .state_root
        .join("progress")
        .join(format!("epoch-{epoch}-download.json"));
    let log_path = config
        .state_root
        .join("logs")
        .join(format!("epoch-{epoch}-download.log"));
    const SCRIPT: &str = r#"
set -eu
url=$1
part=$2
canonical=$3
blockzilla=$4
epoch=$5
receipt=$6
attempts=$7
progress=$8
alternate=$9
required_memory_mib=${10}
[ ! -e "$canonical" ]
[ ! -e "$alternate" ]
attempt=1
download_ok=0
while [ "$attempt" -le "$attempts" ]; do
  if command -v aria2c >/dev/null 2>&1; then
    if aria2c --continue=true --allow-overwrite=true --auto-file-renaming=false --file-allocation=none --max-connection-per-server=4 --split=4 --min-split-size=64M --dir="$(dirname "$part")" --out="$(basename "$part")" "$url"; then
      download_ok=1
      break
    fi
  fi
  if command -v wget >/dev/null 2>&1 && wget -c -O "$part" "$url"; then
    download_ok=1
    break
  fi
  attempt=$((attempt + 1))
done
[ "$download_ok" -eq 1 ]
[ -s "$part" ]
[ ! -e "$canonical" ]
[ ! -e "$alternate" ]
sync -f "$part"
if [ -r /proc/meminfo ]; then
  required_memory_kib=$((required_memory_mib * 1024))
  while :; do
    available_memory_kib=$(awk '/^MemAvailable:/ { print $2; exit }' /proc/meminfo)
    [ -n "$available_memory_kib" ]
    if [ "$available_memory_kib" -ge "$required_memory_kib" ]; then
      break
    fi
    sleep 10
  done
fi
"$blockzilla" preflight-car "$part" --epoch "$epoch" --receipt "$receipt" --io-buffer-mib 8 --progress-json "$progress"
grep -Eq '"structurally_valid"[[:space:]]*:[[:space:]]*true' "$receipt"
grep -Eq '"clean_eof"[[:space:]]*:[[:space:]]*true' "$receipt"
grep -Eq '"eligible_for_compaction"[[:space:]]*:[[:space:]]*true' "$receipt"
sync -f "$receipt"
mv -n "$part" "$canonical"
[ ! -e "$part" ]
[ -s "$canonical" ]
if [ -e "$alternate" ]; then
  exit 1
fi
sync -f "$(dirname "$canonical")"
"#;
    let args = vec![
        "-c".into(),
        SCRIPT.into(),
        "hivezilla-car-download".into(),
        url.into(),
        part_path.clone().into_os_string(),
        canonical_path.clone().into_os_string(),
        config.blockzilla_bin.clone().into_os_string(),
        epoch.to_string().into(),
        receipt_path.clone().into_os_string(),
        DOWNLOAD_MAX_ATTEMPTS.to_string().into(),
        progress_path.clone().into_os_string(),
        alternate_canonical.into_os_string(),
        config
            .memory_reserve_mib
            .saturating_add(PREFLIGHT_MEMORY_MIB)
            .to_string()
            .into(),
    ];
    // Publish the claim before spawn. pid=0 is a deliberate pre-spawn state;
    // the inherited epoch lock distinguishes it from a stale failed claim.
    write_acquisition_marker(
        config,
        epoch,
        "car_download",
        0,
        &canonical_path,
        &receipt_path,
    )?;
    let result = spawn_command_child(
        config,
        Path::new("/bin/sh"),
        args,
        ChildKind::CarDownload {
            epoch,
            canonical_path: canonical_path.clone(),
            receipt_path: receipt_path.clone(),
        },
        progress_path,
        log_path,
        Some(acquisition_lock),
    )
    .await;
    let mut child = match result {
        Ok(child) => child,
        Err(error) => return Err(error),
    };
    let pid = child.pid.context("download child has no pid")?;
    if let Err(error) = write_acquisition_marker(
        config,
        epoch,
        "car_download",
        pid,
        &canonical_path,
        &receipt_path,
    ) {
        terminate_child_group(&mut child).await;
        return Err(error);
    }
    Ok(Some(child))
}

async fn spawn_car_preflight(
    config: &NasPipelineConfig,
    epoch: u64,
    input: Option<&Path>,
) -> Result<Option<ManagedChild>> {
    let Some(acquisition_lock) = try_acquire_acquisition_lock(config, epoch)? else {
        return Ok(None);
    };
    let input = input.context("preflight task has no canonical CAR input")?;
    let receipt_path = car_preflight_receipt_path(&config.state_root, epoch);
    if let Some(parent) = receipt_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create CAR preflight directory {}", parent.display()))?;
    }
    let progress_path = config
        .state_root
        .join("progress")
        .join(format!("epoch-{epoch}-preflight.json"));
    let log_path = config
        .state_root
        .join("logs")
        .join(format!("epoch-{epoch}-preflight.log"));
    let args = vec![
        "preflight-car".into(),
        input.as_os_str().to_owned(),
        "--epoch".into(),
        epoch.to_string().into(),
        "--receipt".into(),
        receipt_path.clone().into_os_string(),
        "--io-buffer-mib".into(),
        PREFLIGHT_IO_BUFFER_MIB.to_string().into(),
        "--progress-json".into(),
        progress_path.clone().into_os_string(),
    ];
    write_acquisition_marker(config, epoch, "car_preflight", 0, input, &receipt_path)?;
    let result = spawn_child(
        config,
        args,
        ChildKind::CarPreflight {
            epoch,
            input_path: input.to_path_buf(),
            receipt_path: receipt_path.clone(),
        },
        progress_path,
        log_path,
        Some(acquisition_lock),
    )
    .await;
    let mut child = match result {
        Ok(child) => child,
        Err(error) => return Err(error),
    };
    let pid = child.pid.context("preflight child has no pid")?;
    if let Err(error) =
        write_acquisition_marker(config, epoch, "car_preflight", pid, input, &receipt_path)
    {
        terminate_child_group(&mut child).await;
        return Err(error);
    }
    Ok(Some(child))
}

fn car_download_paths(
    config: &NasPipelineConfig,
    epoch: u64,
) -> Result<(String, PathBuf, PathBuf)> {
    let template = config
        .car_source_url_template
        .as_deref()
        .context("CAR download requested without a source URL template")?;
    let url = template.replace("{epoch}", &epoch.to_string());
    let suffix = car_source_suffix(&url)?;
    let canonical = config.car_root.join(format!("epoch-{epoch}{suffix}"));
    let part = config
        .car_root
        .join(".downloads")
        .join(format!("epoch-{epoch}{suffix}.part"));
    Ok((url, canonical, part))
}

fn car_source_suffix(url: &str) -> Result<&'static str> {
    let path_without_query = url.split(['?', '#']).next().unwrap_or(url);
    if path_without_query.ends_with(".car.zst") {
        Ok(".car.zst")
    } else if path_without_query.ends_with(".car") {
        Ok(".car")
    } else {
        anyhow::bail!(
            "rendered CAR source URL must end in .car or .car.zst before query/fragment: {url}"
        )
    }
}

async fn spawn_historical_scan(
    config: &NasPipelineConfig,
    epoch: &EpochSnapshot,
) -> Result<ManagedChild> {
    let input = epoch
        .input_path
        .as_deref()
        .context("queued epoch has no input CAR")?;
    anyhow::ensure!(
        !directory_has_entries(&epoch.output_path),
        "refusing to scan epoch {} into existing output {}",
        epoch.epoch,
        epoch.output_path.display()
    );
    write_ownership(
        &epoch.output_path,
        "historical_scan",
        &epoch.epoch.to_string(),
        "running",
        None,
    )?;
    let progress_path = historical_progress_path(&config.state_root, epoch.epoch);
    let log_path = config
        .state_root
        .join("logs")
        .join(format!("epoch-{}-scan.log", epoch.epoch));
    let mut args = vec![
        "build-archive-v2-hot-blocks".into(),
        input.as_os_str().to_owned(),
        epoch.output_path.as_os_str().to_owned(),
        "--level".into(),
        config.level.to_string().into(),
        "--first-seen-registry".into(),
        "--first-seen-scan-only".into(),
        "--first-seen-finalizer-lock".into(),
        config.finalizer_lock.as_os_str().to_owned(),
    ];
    if config.no_access {
        args.push("--no-access".into());
    }
    if let Some(previous_car) = epoch
        .epoch
        .checked_sub(1)
        .and_then(|previous| car_path(&config.car_root, previous))
    {
        args.push("--previous-car".into());
        args.push(previous_car.into_os_string());
    }
    if let Some(previous_epoch) = epoch.epoch.checked_sub(1) {
        let previous_registry = config
            .archive_root
            .join(format!("epoch-{previous_epoch}/registry.bin"));
        if is_nonempty_file(&previous_registry) {
            args.push("--first-seen-seed-registry".into());
            args.push(previous_registry.into_os_string());
        }
    }
    let result = spawn_child(
        config,
        args,
        ChildKind::HistoricalScan { epoch: epoch.epoch },
        progress_path,
        log_path,
        None,
    )
    .await;
    if let Err(error) = &result {
        let _ = write_ownership(
            &epoch.output_path,
            "historical_scan",
            &epoch.epoch.to_string(),
            "failed",
            Some(format!("spawn failed: {error:#}")),
        );
    }
    result
}

async fn spawn_historical_finalizer(
    config: &NasPipelineConfig,
    epoch: u64,
) -> Result<ManagedChild> {
    let output = config.archive_root.join(format!("epoch-{epoch}"));
    anyhow::ensure!(
        output.join(SCAN_MARKER).is_file(),
        "epoch {epoch} has no first-seen scan marker"
    );
    let epoch_id = epoch.to_string();
    if let Some(owner) = read_ownership(&output) {
        anyhow::ensure!(
            ownership_is_first_seen(&owner) && owner.id == epoch_id,
            "output {} ownership {}/{} does not match historical epoch {epoch}",
            output.display(),
            owner.kind,
            owner.id
        );
        write_ownership(&output, &owner.kind, &owner.id, "finalizing", None)?;
    } else {
        write_ownership(
            &output,
            "historical_finalizer",
            &epoch_id,
            "finalizing",
            None,
        )?;
    }
    let progress_path = historical_progress_path(&config.state_root, epoch);
    let log_path = config
        .state_root
        .join("logs")
        .join(format!("epoch-{epoch}-finalize.log"));
    let args = vec![
        "finalize-archive-v2-first-seen".into(),
        output.into_os_string(),
        "--finalizer-lock".into(),
        config.finalizer_lock.as_os_str().to_owned(),
    ];
    let result = spawn_child(
        config,
        args,
        ChildKind::HistoricalFinalizer { epoch },
        progress_path,
        log_path,
        None,
    )
    .await;
    if let Err(error) = &result
        && let Some(owner) = read_ownership(&config.archive_root.join(format!("epoch-{epoch}")))
    {
        let _ = write_ownership(
            &config.archive_root.join(format!("epoch-{epoch}")),
            &owner.kind,
            &owner.id,
            "failed",
            Some(format!("finalizer spawn failed: {error:#}")),
        );
    }
    result
}

async fn spawn_live_finalizer(
    config: &NasPipelineConfig,
    capture: &LiveCaptureSnapshot,
    phase: LiveFinalizerPhase,
) -> Result<Option<ManagedChild>> {
    let output = capture
        .output_path
        .as_deref()
        .context("ready live capture has no epoch/output mapping")?;
    if directory_has_entries(output) {
        let owner = read_ownership(output).with_context(|| {
            format!(
                "refusing to continue live capture {} in unowned output {}",
                capture.id,
                output.display()
            )
        })?;
        anyhow::ensure!(
            owner.kind == "live_finalizer" && owner.id == capture.id,
            "live output ownership {}/{} does not match capture {}",
            owner.kind,
            owner.id,
            capture.id
        );
    }
    let Some(lock) = try_exclusive_lock(&config.finalizer_lock)? else {
        return Ok(None);
    };
    write_ownership(output, "live_finalizer", &capture.id, phase.as_str(), None)?;
    let progress_path = config
        .state_root
        .join("progress")
        .join(format!("live-{}-package.json", safe_segment(&capture.id)));
    let log_path = config.state_root.join("logs").join(format!(
        "live-{}-{}.log",
        safe_segment(&capture.id),
        phase.as_str()
    ));
    let args = match phase {
        LiveFinalizerPhase::Registry => vec![
            "prepare-archive-v2-live-registry".into(),
            capture.capture_dir.as_os_str().to_owned(),
            output.as_os_str().to_owned(),
        ],
        LiveFinalizerPhase::Mphf => vec![
            "build-archive-v2-registry-index".into(),
            output.join(REGISTRY_FILE).into_os_string(),
            "--output".into(),
            output.join(REGISTRY_INDEX_FILE).into_os_string(),
        ],
        LiveFinalizerPhase::Rewrite => vec![
            "build-archive-v2-hot-blocks-from-live".into(),
            capture.capture_dir.as_os_str().to_owned(),
            output.as_os_str().to_owned(),
            "--registry-source".into(),
            "runs".into(),
            "--level".into(),
            config.level.to_string().into(),
        ],
    };
    let result = spawn_child(
        config,
        args,
        ChildKind::LiveFinalizer {
            id: capture.id.clone(),
            epoch: capture.epoch,
            phase,
        },
        progress_path,
        log_path,
        Some(lock),
    )
    .await;
    if let Err(error) = &result {
        let _ = write_ownership(
            output,
            "live_finalizer",
            &capture.id,
            "failed",
            Some(format!("live package spawn failed: {error:#}")),
        );
    }
    result.map(Some)
}

async fn spawn_child(
    config: &NasPipelineConfig,
    args: Vec<std::ffi::OsString>,
    kind: ChildKind,
    progress_path: PathBuf,
    log_path: PathBuf,
    exclusive_lock: Option<File>,
) -> Result<ManagedChild> {
    spawn_command_child(
        config,
        &config.blockzilla_bin,
        args,
        kind,
        progress_path,
        log_path,
        exclusive_lock,
    )
    .await
}

async fn spawn_command_child(
    config: &NasPipelineConfig,
    executable: &Path,
    args: Vec<std::ffi::OsString>,
    kind: ChildKind,
    progress_path: PathBuf,
    log_path: PathBuf,
    exclusive_lock: Option<File>,
) -> Result<ManagedChild> {
    if let Some(parent) = progress_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create progress directory {}", parent.display()))?;
    }
    if progress_path.exists() {
        fs::remove_file(&progress_path)
            .with_context(|| format!("remove stale progress {}", progress_path.display()))?;
    }
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create log directory {}", parent.display()))?;
    }
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("open child log {}", log_path.display()))?;
    let stderr = log
        .try_clone()
        .with_context(|| format!("clone child log {}", log_path.display()))?;
    let mut command = Command::new(executable);
    command
        .args(args)
        .env("BLOCKZILLA_PROGRESS_FILE", &progress_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(false);
    if matches!(
        &kind,
        ChildKind::CarDownload { .. } | ChildKind::CarPreflight { .. }
    ) {
        command.process_group(0);
    }
    // Acquisition children inherit a duplicate of the epoch lock. The child
    // therefore keeps ownership if the controller crashes, while the parent's
    // normal lock descriptor remains CLOEXEC and cannot leak into later jobs.
    let inherited_lock = if matches!(
        &kind,
        ChildKind::CarDownload { .. } | ChildKind::CarPreflight { .. }
    ) {
        exclusive_lock
            .as_ref()
            .map(|lock| {
                let inherited = lock.try_clone().context("clone acquisition lock")?;
                set_close_on_exec(&inherited, false)?;
                Ok::<_, anyhow::Error>(inherited)
            })
            .transpose()?
    } else {
        None
    };
    let child_result = command
        .spawn()
        .with_context(|| format!("spawn {} with {}", kind.key(), executable.display()));
    drop(inherited_lock);
    let child = child_result?;
    let pid = child.id();
    let owned_output = match &kind {
        ChildKind::CarDownload { .. } | ChildKind::CarPreflight { .. } => None,
        ChildKind::HistoricalScan { epoch } | ChildKind::HistoricalFinalizer { epoch } => {
            Some(config.archive_root.join(format!("epoch-{epoch}")))
        }
        ChildKind::LiveFinalizer { epoch, .. } => {
            epoch.map(|epoch| config.archive_root.join(format!("epoch-{epoch}")))
        }
    };
    if let Some(output) = owned_output.as_deref() {
        let _ = set_ownership_pid(output, pid);
    }
    Ok(ManagedChild {
        child,
        pid,
        kind,
        started_unix_secs: unix_now(),
        progress_path,
        log_path,
        _exclusive_lock: exclusive_lock,
    })
}

async fn terminate_child_group(child: &mut ManagedChild) {
    if let Some(pid) = child.pid {
        // SAFETY: acquisition children are spawned as leaders of their own
        // process groups; negative pid targets that isolated group.
        let _ = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
    }
    let _ = child.child.wait().await;
}

async fn reap_children(state: &Arc<AppState>, runtime: &mut RuntimeState) {
    let acquisition_epochs = runtime.acquisitions.keys().copied().collect::<Vec<_>>();
    for epoch in acquisition_epochs {
        let result = runtime
            .acquisitions
            .get_mut(&epoch)
            .and_then(|child| child.child.try_wait().transpose());
        match result {
            Some(Ok(status)) => {
                let child = runtime
                    .acquisitions
                    .remove(&epoch)
                    .expect("acquisition child exists");
                handle_child_exit(&state.config, runtime, child, status.success());
            }
            Some(Err(error)) => {
                let child = runtime
                    .acquisitions
                    .remove(&epoch)
                    .expect("acquisition child exists");
                let message = format!("poll {}: {error:#}", child.kind.key());
                set_runtime_failure(&state.config, runtime, child.kind.key(), message.clone());
                record_error(&state.config, runtime, "child", message);
            }
            None => {}
        }
    }
    let scan_epochs = runtime.scans.keys().copied().collect::<Vec<_>>();
    for epoch in scan_epochs {
        let result = runtime
            .scans
            .get_mut(&epoch)
            .and_then(|child| child.child.try_wait().transpose());
        match result {
            Some(Ok(status)) => {
                let child = runtime.scans.remove(&epoch).expect("scan child exists");
                handle_child_exit(&state.config, runtime, child, status.success());
            }
            Some(Err(error)) => {
                let child = runtime.scans.remove(&epoch).expect("scan child exists");
                let message = format!("poll {}: {error:#}", child.kind.key());
                runtime.failures.insert(child.kind.key(), message.clone());
                record_error(&state.config, runtime, "child", message);
            }
            None => {}
        }
    }
    let finalizer_result = runtime
        .finalizer
        .as_mut()
        .and_then(|child| child.child.try_wait().transpose());
    match finalizer_result {
        Some(Ok(status)) => {
            let child = runtime.finalizer.take().expect("finalizer exists");
            handle_child_exit(&state.config, runtime, child, status.success());
        }
        Some(Err(error)) => {
            let child = runtime.finalizer.take().expect("finalizer exists");
            let message = format!("poll {}: {error:#}", child.kind.key());
            runtime.failures.insert(child.kind.key(), message.clone());
            record_error(&state.config, runtime, "child", message);
        }
        None => {}
    }
}

fn handle_child_exit(
    config: &NasPipelineConfig,
    runtime: &mut RuntimeState,
    child: ManagedChild,
    success: bool,
) {
    let key = child.kind.key();
    let output = match &child.kind {
        ChildKind::CarDownload { canonical_path, .. } => canonical_path.clone(),
        ChildKind::CarPreflight { input_path, .. } => input_path.clone(),
        ChildKind::HistoricalScan { epoch } | ChildKind::HistoricalFinalizer { epoch } => {
            config.archive_root.join(format!("epoch-{epoch}"))
        }
        ChildKind::LiveFinalizer { epoch, .. } => epoch
            .map(|epoch| config.archive_root.join(format!("epoch-{epoch}")))
            .unwrap_or_else(|| config.archive_root.join("unknown-live-epoch")),
    };
    let valid = match &child.kind {
        ChildKind::CarDownload {
            epoch,
            canonical_path,
            receipt_path,
        } => {
            !car_paths_ambiguous(&config.car_root, *epoch)
                && is_nonempty_file(canonical_path)
                && receipt_matches_source(*epoch, canonical_path, receipt_path)
        }
        ChildKind::CarPreflight {
            epoch,
            input_path,
            receipt_path,
        } => receipt_matches_source(*epoch, input_path, receipt_path),
        ChildKind::HistoricalScan { epoch } => config
            .archive_root
            .join(format!("epoch-{epoch}/{SCAN_MARKER}"))
            .is_file(),
        ChildKind::HistoricalFinalizer { epoch } => historical_archive_strict_complete(
            &config.archive_root.join(format!("epoch-{epoch}")),
            !config.no_access,
            true,
        ),
        ChildKind::LiveFinalizer { epoch, phase, .. } => epoch.is_some_and(|epoch| {
            let output = config.archive_root.join(format!("epoch-{epoch}"));
            match phase {
                LiveFinalizerPhase::Registry => {
                    output.join(LIVE_REGISTRY_READY_MARKER).is_file()
                        && is_nonempty_file(&output.join(REGISTRY_FILE))
                        && is_nonempty_file(&output.join(REGISTRY_COUNTS_FILE))
                }
                LiveFinalizerPhase::Mphf => is_nonempty_file(&output.join(REGISTRY_INDEX_FILE)),
                LiveFinalizerPhase::Rewrite => live_archive_packaged(&output),
            }
        }),
    };
    let acquisition = matches!(
        &child.kind,
        ChildKind::CarDownload { .. } | ChildKind::CarPreflight { .. }
    );
    // Filesystem publication is authoritative. A wrapper can be interrupted
    // after the canonical rename/receipt sync; do not turn durable success into
    // a retry that might overwrite or duplicate acquisition work.
    if acquisition && valid {
        if let ChildKind::CarDownload { epoch, .. } | ChildKind::CarPreflight { epoch, .. } =
            &child.kind
        {
            let _ = fs::remove_file(acquisition_marker_path(&config.state_root, *epoch));
        }
        clear_runtime_failure(config, runtime, &key);
        return;
    }
    if success && valid {
        clear_runtime_failure(config, runtime, &key);
        if let Some(owner) = read_ownership(&output) {
            let state = match child.kind {
                ChildKind::CarDownload { .. } | ChildKind::CarPreflight { .. } => unreachable!(),
                ChildKind::HistoricalScan { .. } => "scan_ready",
                ChildKind::HistoricalFinalizer { .. } => "complete",
                ChildKind::LiveFinalizer { phase, .. } => match phase {
                    LiveFinalizerPhase::Registry => "registry_ready",
                    LiveFinalizerPhase::Mphf => "mphf_ready",
                    LiveFinalizerPhase::Rewrite => "packaged",
                },
            };
            let _ = write_ownership(&output, &owner.kind, &owner.id, state, None);
            let _ = set_ownership_pid(&output, None);
        }
        return;
    }
    let message = format!(
        "{} exited {} but filesystem validation {}; log={}",
        key,
        if success {
            "successfully"
        } else {
            "with failure"
        },
        if valid { "passed" } else { "failed" },
        child.log_path.display()
    );
    set_runtime_failure(config, runtime, key, message.clone());
    if let Some(owner) = read_ownership(&output) {
        let _ = write_ownership(
            &output,
            &owner.kind,
            &owner.id,
            "failed",
            Some(message.clone()),
        );
        let _ = set_ownership_pid(&output, None);
    }
    record_error(config, runtime, "child_exit", message);
}

fn try_exclusive_lock(path: &Path) -> Result<Option<File>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create lock directory {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open exclusive lock {}", path.display()))?;
    // SAFETY: file owns this valid descriptor until the managed child exits.
    let result = unsafe {
        libc::flock(
            std::os::fd::AsRawFd::as_raw_fd(&file),
            libc::LOCK_EX | libc::LOCK_NB,
        )
    };
    if result == 0 {
        Ok(Some(file))
    } else {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
            Ok(None)
        } else {
            Err(error).with_context(|| format!("lock exclusive guard {}", path.display()))
        }
    }
}

fn acquire_pipeline_lock(state_root: &Path) -> Result<File> {
    let path = state_root.join(PIPELINE_LOCK_FILE);
    try_exclusive_lock(&path)?.with_context(|| {
        format!(
            "another Hivezilla controller already owns state root {} (lock {})",
            state_root.display(),
            path.display()
        )
    })
}

fn acquisition_lock_path(root: &Path, epoch: u64) -> PathBuf {
    root.join("acquisitions")
        .join(format!("epoch-{epoch}.lock"))
}

fn try_acquire_acquisition_lock(config: &NasPipelineConfig, epoch: u64) -> Result<Option<File>> {
    try_exclusive_lock(&acquisition_lock_path(&config.state_root, epoch))
}

fn acquisition_lock_held(config: &NasPipelineConfig, epoch: u64) -> bool {
    match try_acquire_acquisition_lock(config, epoch) {
        Ok(Some(lock)) => {
            drop(lock);
            false
        }
        Ok(None) | Err(_) => true,
    }
}

fn set_close_on_exec(file: &File, close_on_exec: bool) -> Result<()> {
    // SAFETY: fcntl only reads/updates flags on this live descriptor.
    let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error()).context("read lock descriptor flags");
    }
    let updated = if close_on_exec {
        flags | libc::FD_CLOEXEC
    } else {
        flags & !libc::FD_CLOEXEC
    };
    // SAFETY: the descriptor remains owned by file for the duration of this call.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, updated) } < 0 {
        return Err(std::io::Error::last_os_error()).context("update lock descriptor flags");
    }
    Ok(())
}

fn historical_progress_path(root: &Path, epoch: u64) -> PathBuf {
    root.join("progress").join(format!("epoch-{epoch}.json"))
}

fn acquisition_marker_path(root: &Path, epoch: u64) -> PathBuf {
    root.join("acquisitions")
        .join(format!("epoch-{epoch}.json"))
}

fn write_acquisition_marker(
    config: &NasPipelineConfig,
    epoch: u64,
    kind: &str,
    pid: u32,
    expected_path: &Path,
    receipt_path: &Path,
) -> Result<()> {
    let path = acquisition_marker_path(&config.state_root, epoch);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create acquisition marker root {}", parent.display()))?;
    }
    let marker = AcquisitionMarker {
        schema_version: SCHEMA_VERSION,
        epoch,
        kind: kind.to_string(),
        pid,
        expected_path: expected_path.to_path_buf(),
        receipt_path: receipt_path.to_path_buf(),
        updated_unix_secs: unix_now(),
    };
    let temp = path.with_extension(format!("json.{}.tmp", std::process::id()));
    fs::write(&temp, serde_json::to_vec_pretty(&marker)?)
        .with_context(|| format!("write acquisition marker {}", temp.display()))?;
    fs::rename(&temp, &path)
        .with_context(|| format!("publish acquisition marker {}", path.display()))
}

fn read_acquisition_marker(config: &NasPipelineConfig, epoch: u64) -> Option<AcquisitionMarker> {
    serde_json::from_slice(&fs::read(acquisition_marker_path(&config.state_root, epoch)).ok()?).ok()
}

fn active_acquisition_marker(config: &NasPipelineConfig, epoch: u64) -> Option<AcquisitionMarker> {
    let marker = read_acquisition_marker(config, epoch)?;
    let valid = marker.schema_version == SCHEMA_VERSION
        && marker.epoch == epoch
        && process_cmdline_matches_acquisition(
            marker.pid,
            &config.blockzilla_bin,
            &marker.expected_path,
            &marker.kind,
        );
    if valid { Some(marker) } else { None }
}

fn acquisition_claim_active(config: &NasPipelineConfig, epoch: u64) -> bool {
    active_acquisition_marker(config, epoch).is_some()
        || read_acquisition_marker(config, epoch).is_some_and(|marker| {
            marker.schema_version == SCHEMA_VERSION
                && marker.epoch == epoch
                && acquisition_lock_held(config, epoch)
        })
}

fn process_cmdline_matches_acquisition(
    pid: u32,
    blockzilla_bin: &Path,
    expected_path: &Path,
    kind: &str,
) -> bool {
    let Ok(bytes) = fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline")) else {
        return false;
    };
    let args = bytes
        .split(|byte| *byte == 0)
        .filter(|arg| !arg.is_empty())
        .collect::<Vec<_>>();
    let expected = expected_path.as_os_str().as_bytes();
    if !args.contains(&expected) {
        return false;
    }
    match kind {
        "car_download" => {
            args.iter().any(|arg| *arg == b"hivezilla-car-download")
                && args.first().is_some_and(|arg| arg.ends_with(b"/sh"))
        }
        "car_preflight" => {
            args.first().copied() == Some(blockzilla_bin.as_os_str().as_bytes())
                && args.get(1).copied() == Some(b"preflight-car")
        }
        _ => false,
    }
}

fn write_ownership(
    output: &Path,
    kind: &str,
    id: &str,
    state: &str,
    message: Option<String>,
) -> Result<()> {
    fs::create_dir_all(output)
        .with_context(|| format!("create pipeline-owned output {}", output.display()))?;
    let path = output.join(OWNERSHIP_MARKER);
    let created_unix_secs = read_ownership(output)
        .map(|owner| owner.created_unix_secs)
        .unwrap_or_else(unix_now);
    let owner = OwnershipMarker {
        schema_version: SCHEMA_VERSION,
        kind: kind.to_string(),
        id: id.to_string(),
        state: state.to_string(),
        created_unix_secs,
        updated_unix_secs: unix_now(),
        message,
        pid: read_ownership(output).and_then(|owner| owner.pid),
    };
    let temp = output.join(format!(".{OWNERSHIP_MARKER}.{}.tmp", std::process::id()));
    fs::write(&temp, serde_json::to_vec_pretty(&owner)?)
        .with_context(|| format!("write ownership temp {}", temp.display()))?;
    fs::rename(&temp, &path)
        .with_context(|| format!("publish ownership {} -> {}", temp.display(), path.display()))
}

fn read_ownership(output: &Path) -> Option<OwnershipMarker> {
    serde_json::from_slice(&fs::read(output.join(OWNERSHIP_MARKER)).ok()?).ok()
}

fn set_ownership_pid(output: &Path, pid: Option<u32>) -> Result<()> {
    let Some(mut owner) = read_ownership(output) else {
        return Ok(());
    };
    owner.pid = pid;
    owner.updated_unix_secs = unix_now();
    let path = output.join(OWNERSHIP_MARKER);
    let temp = output.join(format!(".{OWNERSHIP_MARKER}.{}.tmp", std::process::id()));
    fs::write(&temp, serde_json::to_vec_pretty(&owner)?)
        .with_context(|| format!("write ownership pid {}", temp.display()))?;
    fs::rename(&temp, &path).with_context(|| format!("publish ownership pid {}", path.display()))
}

fn read_progress(path: &Path) -> Option<ProgressSnapshot> {
    const MAX_PROGRESS_BYTES: u64 = 1024 * 1024;
    let mut bytes = Vec::new();
    File::open(path)
        .ok()?
        .take(MAX_PROGRESS_BYTES)
        .read_to_end(&mut bytes)
        .ok()?;
    parse_progress_bytes(&bytes).ok()
}

fn parse_progress_bytes(bytes: &[u8]) -> Result<ProgressSnapshot> {
    let value: Value = serde_json::from_slice(bytes).context("parse progress JSON")?;
    let pid = json_u64(&value, &["pid"]).and_then(|value| u32::try_from(value).ok());
    let rss_bytes = pid
        .and_then(process_rss_bytes)
        .or_else(|| json_u64(&value, &["rss_bytes"]));
    let peak_rss_bytes = pid
        .and_then(process_peak_rss_bytes)
        .or_else(|| json_u64(&value, &["peak_rss_bytes"]))
        .map(|peak| peak.max(rss_bytes.unwrap_or(0)));
    let blocks_done =
        json_u64(&value, &["blocks_done", "blocks_written", "block_frames"]).unwrap_or(0);
    let blocks_total = json_u64(&value, &["blocks_total", "blocks_total_estimate"]).unwrap_or(0);
    let first_slot = json_u64(&value, &["first_slot"]);
    let last_slot = json_u64(&value, &["last_slot"]);
    let progress_pct = json_f64(&value, &["progress_pct"]).or_else(|| {
        first_slot
            .zip(last_slot)
            .map(|(first, last)| last.saturating_sub(first) as f64 / SLOTS_PER_EPOCH as f64 * 100.0)
    });
    Ok(ProgressSnapshot {
        phase: json_string(&value, &["phase"]),
        state: json_string(&value, &["state", "status"]),
        pid,
        blocks_done,
        blocks_total,
        transactions_done: json_u64(
            &value,
            &["transactions_done", "transactions_written", "txs_done"],
        )
        .unwrap_or(0),
        first_slot,
        last_slot,
        progress_pct: finite_option(progress_pct),
        elapsed_secs: finite_option(json_f64(&value, &["elapsed_secs"])),
        blocks_per_sec: finite_option(json_f64(&value, &["blocks_per_sec"])),
        slots_per_sec: nonnegative_finite_option(json_f64(&value, &["slots_per_sec"])),
        input_mib_per_sec: finite_option(json_f64(&value, &["input_mib_per_sec", "mb_per_sec"])),
        disk_read_mib_per_sec: nonnegative_finite_option(json_f64(
            &value,
            &["disk_read_mib_per_sec"],
        )),
        disk_write_mib_per_sec: nonnegative_finite_option(json_f64(
            &value,
            &["disk_write_mib_per_sec"],
        )),
        eta_secs: finite_option(json_f64(&value, &["eta_secs"])),
        rss_bytes,
        peak_rss_bytes,
        updated_unix_secs: json_u64(&value, &["updated_unix_secs"]),
    })
}

fn finite_option(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite())
}

fn positive_finite_option(value: Option<f64>) -> Option<f64> {
    finite_option(value).filter(|value| *value > 0.0)
}

fn nonnegative_finite_option(value: Option<f64>) -> Option<f64> {
    finite_option(value).filter(|value| *value >= 0.0)
}

fn json_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_u64))
}

fn json_f64(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_f64))
}

fn json_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str).map(str::to_string))
}

fn read_live_journal_tail(path: &Path) -> Option<Value> {
    let mut file = File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let read_len = len.min(128 * 1024);
    file.seek(SeekFrom::End(-(read_len as i64))).ok()?;
    let mut bytes = Vec::with_capacity(read_len as usize);
    file.take(read_len).read_to_end(&mut bytes).ok()?;
    bytes.split(|byte| *byte == b'\n').rev().find_map(|line| {
        (!line.is_empty())
            .then(|| serde_json::from_slice(line).ok())
            .flatten()
    })
}

fn merge_live_journal_progress(
    progress: &mut ProgressSnapshot,
    journal: Option<&Value>,
    journal_updated_unix_secs: Option<u64>,
) {
    let Some(journal) = journal else {
        return;
    };

    // Keep the progress-file timestamp as the rate baseline before the
    // append-only journal advances freshness. Older producers only wrote a
    // minimal progress file at startup, so losing this value made their live
    // ETA permanently unknowable even while the journal kept advancing.
    let baseline_updated = progress.updated_unix_secs;
    let baseline_last_slot = progress.last_slot;
    let baseline_elapsed_secs = progress.elapsed_secs;

    if let Some(last_slot) = json_u64(journal, &["slot", "last_slot"]) {
        progress.last_slot = Some(
            progress
                .last_slot
                .map_or(last_slot, |current| current.max(last_slot)),
        );
    }
    if let Some(blocks_done) = json_u64(journal, &["block_id"]).map(|id| id.saturating_add(1)) {
        progress.blocks_done = progress.blocks_done.max(blocks_done);
    }
    if let Some(last_slot) = progress.last_slot {
        progress.progress_pct = Some(
            ((last_slot % SLOTS_PER_EPOCH).saturating_add(1) as f64 / SLOTS_PER_EPOCH as f64
                * 100.0)
                .min(100.0),
        );
    }
    if let Some(journal_updated) = journal_updated_unix_secs {
        let journal_advanced = progress
            .last_slot
            .zip(baseline_last_slot)
            .is_some_and(|(last_slot, baseline_last_slot)| last_slot > baseline_last_slot)
            || (progress.last_slot.is_some() && baseline_last_slot.is_none());
        if journal_advanced {
            if let Some(delta_secs) = baseline_updated
                .map(|baseline| journal_updated.saturating_sub(baseline))
                .filter(|delta| *delta > 0)
            {
                // A minimal legacy progress file has no elapsed clock. Its
                // last slot and timestamp form the exact observed interval;
                // using first_slot here would combine unlike intervals and
                // overstate the rate.
                if baseline_elapsed_secs.is_none() {
                    progress.slots_per_sec = baseline_last_slot.zip(progress.last_slot).and_then(
                        |(baseline_slot, last_slot)| {
                            let advanced = last_slot.saturating_sub(baseline_slot);
                            positive_finite_option(Some(advanced as f64 / delta_secs as f64))
                        },
                    );
                }
                progress.elapsed_secs =
                    Some(progress.elapsed_secs.unwrap_or(0.0).max(0.0) + delta_secs as f64);
            }
        }
        progress.updated_unix_secs = Some(
            progress
                .updated_unix_secs
                .map_or(journal_updated, |current| current.max(journal_updated)),
        );
    }
}

fn refresh_live_epoch_metrics(progress: &mut ProgressSnapshot) {
    // An explicit zero is meaningful: the producer is fresh but has not
    // advanced a slot in its latest interval. Preserve it instead of falling
    // back to a misleading lifetime average.
    progress.slots_per_sec = nonnegative_finite_option(progress.slots_per_sec).or_else(|| {
        progress
            .first_slot
            .zip(progress.last_slot)
            .zip(progress.elapsed_secs)
            .and_then(|((first_slot, last_slot), elapsed_secs)| {
                let slots_advanced = last_slot.saturating_sub(first_slot);
                (slots_advanced > 0 && elapsed_secs.is_finite() && elapsed_secs > 0.0)
                    .then_some(slots_advanced as f64 / elapsed_secs)
            })
            .and_then(|rate| positive_finite_option(Some(rate)))
    });

    let Some(last_slot) = progress.last_slot else {
        return;
    };
    let completed_slots = (last_slot % SLOTS_PER_EPOCH).saturating_add(1);
    let remaining_slots = SLOTS_PER_EPOCH.saturating_sub(completed_slots);
    progress.progress_pct =
        Some((completed_slots as f64 / SLOTS_PER_EPOCH as f64 * 100.0).min(100.0));
    progress.eta_secs = if remaining_slots == 0 {
        Some(0.0)
    } else {
        progress
            .slots_per_sec
            .and_then(|rate| positive_finite_option(Some(remaining_slots as f64 / rate)))
    };
}

fn hide_stale_live_rates(progress: &mut ProgressSnapshot, now: u64) {
    let fresh = progress
        .updated_unix_secs
        .is_some_and(|updated| now.saturating_sub(updated) <= PROGRESS_STALE_SECS);
    if !fresh {
        progress.blocks_per_sec = None;
        progress.slots_per_sec = None;
        progress.eta_secs = None;
    }
}

fn hide_stale_lane_rates(progress: &mut ProgressSnapshot, now: u64) {
    let fresh = progress
        .updated_unix_secs
        .is_some_and(|updated| now.saturating_sub(updated) <= PROGRESS_STALE_SECS);
    if !fresh {
        progress.blocks_per_sec = None;
        progress.input_mib_per_sec = None;
        progress.eta_secs = None;
    }
}

fn progress_is_alive(progress: &ProgressSnapshot, now: u64) -> bool {
    let fresh = progress
        .updated_unix_secs
        .is_some_and(|updated| now.saturating_sub(updated) <= PROGRESS_STALE_SECS);
    progress.pid.is_some_and(process_exists) || fresh
}

fn process_exists(pid: u32) -> bool {
    Path::new("/proc").join(pid.to_string()).exists()
}

fn process_stat_identity(pid: u32) -> Option<(char, u64)> {
    let stat = fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("stat")).ok()?;
    // comm may contain spaces and parentheses, so fields only become stable
    // after the final ") ". The remaining token 0 is field 3 (state), and
    // token 19 is field 22 (process start time in clock ticks).
    let fields = stat
        .rsplit_once(") ")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let state = fields.first()?.chars().next()?;
    let start_ticks = fields.get(19)?.parse().ok()?;
    Some((state, start_ticks))
}

fn process_cmdline_contains(pid: u32, expected_path: &Path) -> bool {
    let Ok(bytes) = fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline")) else {
        return false;
    };
    let normalized = bytes
        .into_iter()
        .map(|byte| if byte == 0 { b' ' } else { byte })
        .collect::<Vec<_>>();
    String::from_utf8_lossy(&normalized).contains(&expected_path.to_string_lossy().to_string())
}

fn process_cmdline_matches_job(
    pid: u32,
    blockzilla_bin: &Path,
    expected_path: &Path,
    kind: &str,
) -> bool {
    fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline"))
        .is_ok_and(|bytes| argv_matches_job(&bytes, blockzilla_bin, expected_path, kind))
}

fn process_rss_bytes(pid: u32) -> Option<u64> {
    let status =
        fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("status")).ok()?;
    parse_status_kib(&status, "VmRSS:").map(|kib| kib.saturating_mul(1024))
}

fn process_peak_rss_bytes(pid: u32) -> Option<u64> {
    let status =
        fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("status")).ok()?;
    let current = parse_status_kib(&status, "VmRSS:").unwrap_or(0);
    parse_status_kib(&status, "VmHWM:")
        .map(|peak| peak.max(current).saturating_mul(1024))
        .or_else(|| (current > 0).then(|| current.saturating_mul(1024)))
}

fn process_tree_rss_bytes(root_pid: u32) -> Option<u64> {
    let mut queue = VecDeque::from([root_pid]);
    let mut seen = BTreeSet::new();
    let mut total = 0u64;
    while let Some(pid) = queue.pop_front() {
        if seen.len() >= 256 || !seen.insert(pid) {
            continue;
        }
        total = total.saturating_add(process_rss_bytes(pid).unwrap_or(0));
        let children_path = Path::new("/proc")
            .join(pid.to_string())
            .join("task")
            .join(pid.to_string())
            .join("children");
        if let Ok(children) = fs::read_to_string(children_path) {
            queue.extend(
                children
                    .split_whitespace()
                    .filter_map(|child| child.parse::<u32>().ok()),
            );
        }
    }
    (!seen.is_empty()).then_some(total)
}

fn parse_process_io_bytes(contents: &str) -> Option<(u64, u64)> {
    let value = |key: &str| {
        contents.lines().find_map(|line| {
            line.strip_prefix(key)?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })
    };
    Some((value("read_bytes:")?, value("write_bytes:")?))
}

fn process_tree_io_counters(root_pid: u32) -> Option<BTreeMap<(u32, u64), ProcessIoCounters>> {
    let mut queue = VecDeque::from([root_pid]);
    let mut seen = BTreeSet::new();
    let mut members = BTreeMap::new();
    while let Some(pid) = queue.pop_front() {
        if !seen.insert(pid) {
            continue;
        }
        // A truncated tree is not a valid aggregate sample. Skipping this
        // interval is safer than publishing a deceptively low rate.
        if seen.len() > 256 {
            return None;
        }
        let (_, start_ticks) = process_stat_identity(pid)?;
        let contents =
            fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("io")).ok()?;
        let (read_bytes, write_bytes) = parse_process_io_bytes(&contents)?;
        let children_path = Path::new("/proc")
            .join(pid.to_string())
            .join("task")
            .join(pid.to_string())
            .join("children");
        let children = fs::read_to_string(children_path).ok()?;
        // Re-read starttime after the other files so PID reuse or an exit in
        // the middle of traversal invalidates the whole sample.
        if process_stat_identity(pid).map(|(_, ticks)| ticks) != Some(start_ticks) {
            return None;
        }
        members.insert(
            (pid, start_ticks),
            ProcessIoCounters {
                read_bytes,
                write_bytes,
            },
        );
        queue.extend(
            children
                .split_whitespace()
                .map(str::parse::<u32>)
                .collect::<Result<Vec<_>, _>>()
                .ok()?,
        );
    }
    (!members.is_empty()).then_some(members)
}

fn process_io_rate_mib(
    previous: &ProcessIoSample,
    current: &ProcessIoSample,
) -> Option<(f64, f64)> {
    if previous.members.keys().ne(current.members.keys()) {
        return None;
    }
    let elapsed = current
        .sampled_at
        .duration_since(previous.sampled_at)
        .as_secs_f64();
    if !elapsed.is_finite() || elapsed < 0.25 {
        return None;
    }
    let mut read_bytes = 0u64;
    let mut write_bytes = 0u64;
    for (identity, counters) in &current.members {
        let previous = previous.members.get(identity)?;
        read_bytes =
            read_bytes.checked_add(counters.read_bytes.checked_sub(previous.read_bytes)?)?;
        write_bytes =
            write_bytes.checked_add(counters.write_bytes.checked_sub(previous.write_bytes)?)?;
    }
    let read = read_bytes as f64 / (1024.0 * 1024.0) / elapsed;
    let write = write_bytes as f64 / (1024.0 * 1024.0) / elapsed;
    (read.is_finite() && write.is_finite()).then_some((read, write))
}

fn sample_worker_disk_io(snapshot: &mut PipelineSnapshot, runtime: &mut RuntimeState) {
    let sampled_at = Instant::now();
    let mut active_ids = BTreeSet::new();
    let mut total_read = 0.0;
    let mut total_write = 0.0;
    let mut sampled_rates = 0usize;
    let mut active_roots = 0usize;
    let mut aggregate_members = BTreeSet::new();
    let mut aggregate_disjoint = true;

    for lane in &mut snapshot.lanes {
        lane.progress.disk_read_mib_per_sec = None;
        lane.progress.disk_write_mib_per_sec = None;
        if lane.state != "running" {
            continue;
        }
        active_roots = active_roots.saturating_add(1);
        let Some(pid) = lane.pid.or(lane.progress.pid) else {
            continue;
        };
        let Some(members) = process_tree_io_counters(pid) else {
            continue;
        };
        active_ids.insert(lane.id.clone());
        let current = ProcessIoSample {
            members,
            sampled_at,
        };
        if let Some((read, write)) = runtime
            .process_io_samples
            .get(&lane.id)
            .and_then(|previous| process_io_rate_mib(previous, &current))
        {
            lane.progress.disk_read_mib_per_sec = Some(read);
            lane.progress.disk_write_mib_per_sec = Some(write);
            sampled_rates = sampled_rates.saturating_add(1);
            let identities = current.members.keys().copied().collect::<Vec<_>>();
            if identities
                .iter()
                .any(|identity| aggregate_members.contains(identity))
            {
                aggregate_disjoint = false;
            } else {
                aggregate_members.extend(identities);
                total_read += read;
                total_write += write;
            }
        }
        runtime.process_io_samples.insert(lane.id.clone(), current);
    }
    for capture in snapshot
        .live
        .iter_mut()
        .filter(|capture| capture.state == LiveState::Capturing)
    {
        capture.progress.disk_read_mib_per_sec = None;
        capture.progress.disk_write_mib_per_sec = None;
        let Some(pid) = capture.progress.pid else {
            continue;
        };
        active_roots = active_roots.saturating_add(1);
        let Some(members) = process_tree_io_counters(pid) else {
            continue;
        };
        let id = format!("live_capture:{}", capture.id);
        active_ids.insert(id.clone());
        let current = ProcessIoSample {
            members,
            sampled_at,
        };
        if let Some((read, write)) = runtime
            .process_io_samples
            .get(&id)
            .and_then(|previous| process_io_rate_mib(previous, &current))
        {
            capture.progress.disk_read_mib_per_sec = Some(read);
            capture.progress.disk_write_mib_per_sec = Some(write);
            sampled_rates = sampled_rates.saturating_add(1);
            let identities = current.members.keys().copied().collect::<Vec<_>>();
            if identities
                .iter()
                .any(|identity| aggregate_members.contains(identity))
            {
                aggregate_disjoint = false;
            } else {
                aggregate_members.extend(identities);
                total_read += read;
                total_write += write;
            }
        }
        runtime.process_io_samples.insert(id, current);
    }
    runtime
        .process_io_samples
        .retain(|id, _| active_ids.contains(id));
    let aggregate_complete =
        active_roots > 0 && sampled_rates == active_roots && aggregate_disjoint;
    snapshot.summary.disk_read_mib_per_sec = aggregate_complete.then_some(total_read);
    snapshot.summary.disk_write_mib_per_sec = aggregate_complete.then_some(total_write);
    snapshot.summary.disk_io_active_roots = active_roots;
    snapshot.summary.disk_io_sampled_roots = sampled_rates;
}

const DISKSTATS_SECTOR_BYTES: f64 = 512.0;

fn block_device_io_rate_mib(
    previous: &BlockDeviceIoSample,
    current: &BlockDeviceIoSample,
) -> Option<(f64, f64)> {
    if previous.counters.major != current.counters.major
        || previous.counters.minor != current.counters.minor
        || previous.counters.name != current.counters.name
    {
        return None;
    }
    let elapsed = current
        .sampled_at
        .duration_since(previous.sampled_at)
        .as_secs_f64();
    if !elapsed.is_finite() || elapsed < 0.25 {
        return None;
    }
    let read_sectors = current
        .counters
        .sectors_read
        .checked_sub(previous.counters.sectors_read)?;
    let written_sectors = current
        .counters
        .sectors_written
        .checked_sub(previous.counters.sectors_written)?;
    let mib = 1024.0 * 1024.0;
    let read = read_sectors as f64 * DISKSTATS_SECTOR_BYTES / mib / elapsed;
    let write = written_sectors as f64 * DISKSTATS_SECTOR_BYTES / mib / elapsed;
    (read.is_finite() && write.is_finite()).then_some((read, write))
}

#[cfg(any(target_os = "linux", test))]
fn parse_diskstats_device(
    diskstats: &str,
    expected_major: u32,
    expected_minor: u32,
) -> Option<BlockDeviceIoCounters> {
    diskstats.lines().find_map(|line| {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 10
            || fields[0].parse::<u32>().ok()? != expected_major
            || fields[1].parse::<u32>().ok()? != expected_minor
        {
            return None;
        }
        Some(BlockDeviceIoCounters {
            major: expected_major,
            minor: expected_minor,
            name: fields[2].to_string(),
            sectors_read: fields[5].parse().ok()?,
            sectors_written: fields[9].parse().ok()?,
        })
    })
}

#[cfg(target_os = "linux")]
fn linux_device_numbers(device: u64) -> (u32, u32) {
    // Linux's dev_t layout is non-contiguous; this is the same split used by
    // the kernel's MAJOR/MINOR helpers and glibc's sysmacros.h.
    let major = ((device >> 8) & 0x0000_0fff) | ((device >> 32) & 0xffff_f000);
    let minor = (device & 0x0000_00ff) | ((device >> 12) & 0xffff_ff00);
    (major as u32, minor as u32)
}

#[cfg(target_os = "linux")]
fn read_archive_device_io_sample(
    archive_path: &Path,
    sampled_at: Instant,
) -> Option<BlockDeviceIoSample> {
    let device = fs::metadata(archive_path).ok()?.dev();
    let (major, minor) = linux_device_numbers(device);
    let diskstats = fs::read_to_string("/proc/diskstats").ok()?;
    Some(BlockDeviceIoSample {
        counters: parse_diskstats_device(&diskstats, major, minor)?,
        sampled_at,
    })
}

#[cfg(not(target_os = "linux"))]
fn read_archive_device_io_sample(
    _archive_path: &Path,
    _sampled_at: Instant,
) -> Option<BlockDeviceIoSample> {
    None
}

fn sample_archive_device_disk_io(
    archive_path: &Path,
    machine: &mut MachineSnapshot,
    runtime: &mut RuntimeState,
) {
    machine.archive_device_major = None;
    machine.archive_device_minor = None;
    machine.archive_device_name = None;
    machine.archive_device_read_mib_per_sec = None;
    machine.archive_device_write_mib_per_sec = None;

    let Some(current) = read_archive_device_io_sample(archive_path, Instant::now()) else {
        runtime.archive_device_io_sample = None;
        return;
    };
    machine.archive_device_major = Some(current.counters.major);
    machine.archive_device_minor = Some(current.counters.minor);
    machine.archive_device_name = Some(current.counters.name.clone());
    if let Some((read, write)) = runtime
        .archive_device_io_sample
        .as_ref()
        .and_then(|previous| block_device_io_rate_mib(previous, &current))
    {
        machine.archive_device_read_mib_per_sec = Some(read);
        machine.archive_device_write_mib_per_sec = Some(write);
    }
    runtime.archive_device_io_sample = Some(current);
}

fn parse_status_kib(status: &str, key: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        line.strip_prefix(key)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

fn machine_snapshot(
    archive_path: &Path,
    car_path: &Path,
    children_rss_bytes: u64,
) -> MachineSnapshot {
    let memory = fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let memory_total_bytes = parse_meminfo_kib(&memory, "MemTotal:").saturating_mul(1024);
    let memory_available_bytes = parse_meminfo_kib(&memory, "MemAvailable:").saturating_mul(1024);
    let swap_total_bytes = parse_meminfo_kib(&memory, "SwapTotal:").saturating_mul(1024);
    let swap_free_bytes = parse_meminfo_kib(&memory, "SwapFree:").saturating_mul(1024);
    let load_1m = fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|line| line.split_whitespace().next()?.parse().ok())
        .unwrap_or(0.0);
    let (disk_total_bytes, disk_available_bytes) =
        filesystem_capacity(archive_path).unwrap_or_default();
    let (car_disk_total_bytes, car_disk_available_bytes) =
        filesystem_capacity(car_path).unwrap_or_default();
    MachineSnapshot {
        memory_used_bytes: memory_total_bytes.saturating_sub(memory_available_bytes),
        memory_total_bytes,
        memory_available_bytes,
        swap_used_bytes: swap_total_bytes.saturating_sub(swap_free_bytes),
        swap_total_bytes,
        disk_used_bytes: disk_total_bytes.saturating_sub(disk_available_bytes),
        disk_total_bytes,
        disk_available_bytes,
        car_disk_used_bytes: car_disk_total_bytes.saturating_sub(car_disk_available_bytes),
        car_disk_total_bytes,
        car_disk_available_bytes,
        car_disk_shared_with_archive: paths_share_filesystem_device(archive_path, car_path),
        archive_device_major: None,
        archive_device_minor: None,
        archive_device_name: None,
        archive_device_read_mib_per_sec: None,
        archive_device_write_mib_per_sec: None,
        load_1m,
        service_rss_bytes: process_rss_bytes(std::process::id()).unwrap_or(0),
        children_rss_bytes,
    }
}

fn paths_share_filesystem_device(left: &Path, right: &Path) -> bool {
    fs::metadata(left)
        .ok()
        .zip(fs::metadata(right).ok())
        .is_some_and(|(left, right)| left.dev() == right.dev())
}

fn car_disk_admission_blocked_reason(
    config: &NasPipelineConfig,
    machine: &MachineSnapshot,
    projected_bytes: u64,
) -> Option<String> {
    let reserve = config.disk_reserve_gib.saturating_mul(1024 * 1024 * 1024);
    if machine.car_disk_total_bytes == 0 {
        return Some(format!(
            "CAR download admission blocked: filesystem capacity is unavailable for {}",
            config.car_root.display()
        ));
    }
    let required = reserve.saturating_add(projected_bytes);
    (machine.car_disk_available_bytes < required).then(|| {
        format!(
            "CAR download admission blocked: available {:.1} GiB, projected remaining {:.1} GiB, reserve {} GiB on {}",
            machine.car_disk_available_bytes as f64 / 1024f64.powi(3),
            projected_bytes as f64 / 1024f64.powi(3),
            config.disk_reserve_gib,
            config.car_root.display(),
        )
    })
}

fn car_download_part_bytes(config: &NasPipelineConfig, epoch: u64) -> u64 {
    [".car.part", ".car.zst.part"]
        .into_iter()
        .map(|suffix| {
            file_len(
                &config
                    .car_root
                    .join(".downloads")
                    .join(format!("epoch-{epoch}{suffix}")),
            )
        })
        .max()
        .unwrap_or(0)
}

fn car_download_remaining_projection(
    config: &NasPipelineConfig,
    epochs: &[EpochSnapshot],
    epoch: u64,
) -> u64 {
    let observed_complete = epochs
        .iter()
        .map(|epoch| epoch.car_bytes)
        .max()
        .unwrap_or(0);
    let expected_total = MIN_CAR_DOWNLOAD_PROJECTION_BYTES.max(observed_complete);
    let part_bytes = car_download_part_bytes(config, epoch);
    expected_total
        .saturating_sub(part_bytes)
        // If an unexpected source already exceeds the estimate, retain a
        // minimum growth allowance instead of treating it as free to finish.
        .max(1024 * 1024 * 1024)
}

fn active_download_projection(
    config: &NasPipelineConfig,
    epochs: &[EpochSnapshot],
    runtime: &RuntimeState,
) -> u64 {
    epochs.iter().fold(0u64, |sum, epoch| {
        let managed_download = runtime
            .acquisitions
            .get(&epoch.epoch)
            .is_some_and(|child| matches!(child.kind, ChildKind::CarDownload { .. }));
        let claimed_download = !runtime.acquisitions.contains_key(&epoch.epoch)
            && acquisition_claim_active(config, epoch.epoch)
            && read_acquisition_marker(config, epoch.epoch)
                .is_some_and(|marker| marker.kind == "car_download");
        if managed_download || claimed_download {
            sum.saturating_add(car_download_remaining_projection(
                config,
                epochs,
                epoch.epoch,
            ))
        } else {
            sum
        }
    })
}

fn acquisition_memory_capacity(
    config: &NasPipelineConfig,
    machine: &MachineSnapshot,
    active: usize,
    active_rss_bytes: u64,
) -> usize {
    let configured_remaining = config.download_concurrency.saturating_sub(active);
    if machine.memory_total_bytes == 0 {
        return configured_remaining;
    }
    let reserve = config.memory_reserve_mib.saturating_mul(1024 * 1024);
    let task_budget = PREFLIGHT_MEMORY_MIB.saturating_mul(1024 * 1024).max(1);
    let projected_active_growth = task_budget
        .saturating_mul(u64::try_from(active).unwrap_or(u64::MAX))
        .saturating_sub(active_rss_bytes);
    let by_memory = machine
        .memory_available_bytes
        .saturating_sub(reserve)
        .saturating_sub(projected_active_growth)
        / task_budget;
    configured_remaining.min(usize::try_from(by_memory).unwrap_or(usize::MAX))
}

#[derive(Debug)]
struct AdmissionSnapshot {
    scan_capacity: usize,
    blocked_reason: Option<String>,
}

fn scan_remaining_disk_projection(epoch: &EpochSnapshot) -> u64 {
    let published_output_bytes = epoch
        .artifacts
        .iter()
        .filter(|artifact| artifact.requirement != ArtifactRequirement::ScanInput)
        .fold(0u64, |sum, artifact| sum.saturating_add(artifact.bytes));
    MIN_SCAN_OUTPUT_PROJECTION_BYTES
        .max(epoch.car_bytes)
        .saturating_sub(published_output_bytes)
        .max(MIN_SCAN_OUTPUT_PROJECTION_BYTES)
}

fn admission_snapshot(
    config: &NasPipelineConfig,
    machine: &MachineSnapshot,
    epochs: &[EpochSnapshot],
) -> AdmissionSnapshot {
    let active = epochs
        .iter()
        .filter(|epoch| epoch.state == HistoricalState::Scanning)
        .collect::<Vec<_>>();
    let active_count = active.len().min(config.scan_concurrency);
    let scan_bytes = config.scan_memory_mib.saturating_mul(1024 * 1024).max(1);
    let reserve_bytes = config.memory_reserve_mib.saturating_mul(1024 * 1024);
    let future_growth = active.iter().fold(0u64, |sum, epoch| {
        sum.saturating_add(scan_bytes.saturating_sub(epoch.progress.rss_bytes.unwrap_or(0)))
    });
    let disk_reserve = config.disk_reserve_gib.saturating_mul(1024 * 1024 * 1024);
    if machine.disk_total_bytes == 0 {
        return AdmissionSnapshot {
            scan_capacity: active_count,
            blocked_reason: Some(
                "disk admission blocked: archive filesystem capacity unavailable".to_string(),
            ),
        };
    }
    let active_disk_growth = active.iter().fold(0u64, |sum, epoch| {
        sum.saturating_add(scan_remaining_disk_projection(epoch))
    });
    let mut disk_headroom = machine
        .disk_available_bytes
        .saturating_sub(disk_reserve)
        .saturating_sub(active_disk_growth);
    let mut additional_by_disk = 0usize;
    for projection in prioritized_epochs(config, epochs)
        .into_iter()
        .filter(|epoch| epoch.state == HistoricalState::Queued && epoch.input_path.is_some())
        .map(scan_remaining_disk_projection)
    {
        if active_count.saturating_add(additional_by_disk) >= config.scan_concurrency
            || projection > disk_headroom
        {
            break;
        }
        disk_headroom = disk_headroom.saturating_sub(projection);
        additional_by_disk = additional_by_disk.saturating_add(1);
    }
    if machine.disk_available_bytes < disk_reserve.saturating_add(active_disk_growth) {
        return AdmissionSnapshot {
            scan_capacity: active_count,
            blocked_reason: Some(format!(
                "disk admission blocked: available {:.1} GiB, projected active output {:.1} GiB, reserve {} GiB",
                machine.disk_available_bytes as f64 / 1024f64.powi(3),
                active_disk_growth as f64 / 1024f64.powi(3),
                config.disk_reserve_gib
            )),
        };
    }
    if machine.memory_total_bytes == 0 {
        return AdmissionSnapshot {
            scan_capacity: config.scan_concurrency,
            blocked_reason: None,
        };
    }
    let headroom = machine
        .memory_available_bytes
        .saturating_sub(reserve_bytes)
        .saturating_sub(future_growth);
    let additional_by_memory = usize::try_from(headroom / scan_bytes).unwrap_or(usize::MAX);
    let memory_capacity = active_count
        .saturating_add(additional_by_memory)
        .min(config.scan_concurrency);
    let disk_capacity = if epochs
        .iter()
        .any(|epoch| epoch.state == HistoricalState::Queued && epoch.input_path.is_some())
    {
        active_count.saturating_add(additional_by_disk)
    } else {
        config.scan_concurrency
    };
    let scan_capacity = memory_capacity.min(disk_capacity);
    AdmissionSnapshot {
        scan_capacity,
        blocked_reason: (scan_capacity <= active_count && active_count < config.scan_concurrency)
            .then(|| {
                if disk_capacity <= active_count {
                    format!(
                        "disk admission blocked: available {:.1} GiB, projected active output {:.1} GiB, reserve {} GiB",
                        machine.disk_available_bytes as f64 / 1024f64.powi(3),
                        active_disk_growth as f64 / 1024f64.powi(3),
                        config.disk_reserve_gib,
                    )
                } else {
                    format!(
                        "memory admission blocked: available {:.1} MiB, projected active growth {:.1} MiB, reserve {} MiB, lane budget {} MiB",
                        machine.memory_available_bytes as f64 / 1024f64.powi(2),
                        future_growth as f64 / 1024f64.powi(2),
                        config.memory_reserve_mib,
                        config.scan_memory_mib
                    )
                }
            }),
    }
}

fn parse_meminfo_kib(meminfo: &str, key: &str) -> u64 {
    meminfo
        .lines()
        .find_map(|line| {
            line.strip_prefix(key)?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
        .unwrap_or(0)
}

fn filesystem_capacity(path: &Path) -> Option<(u64, u64)> {
    let existing = path.ancestors().find(|candidate| candidate.exists())?;
    let c_path = CString::new(existing.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: c_path is NUL-terminated and stats points to writable memory.
    if unsafe { libc::statvfs(c_path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: statvfs returned success and initialized stats.
    let stats = unsafe { stats.assume_init() };
    let fragment_size = stats.f_frsize;
    Some((
        (stats.f_blocks as u64).saturating_mul(fragment_size),
        (stats.f_bavail as u64).saturating_mul(fragment_size),
    ))
}

fn directory_has_entries(path: &Path) -> bool {
    fs::read_dir(path)
        .ok()
        .and_then(|mut entries| entries.next())
        .is_some()
}

fn is_nonempty_file(path: &Path) -> bool {
    fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.len() > 0)
        .unwrap_or(false)
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn modified_unix_secs(path: &Path) -> Option<u64> {
    fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn safe_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn persist_snapshot_bytes(root: &Path, bytes: Vec<u8>) -> Result<()> {
    let path = root.join("status.json");
    let temp = root.join(format!(".status.{}.tmp", std::process::id()));
    fs::write(&temp, bytes).with_context(|| format!("write {}", temp.display()))?;
    fs::rename(&temp, &path)
        .with_context(|| format!("publish {} -> {}", temp.display(), path.display()))
}

fn record_error(
    config: &NasPipelineConfig,
    runtime: &mut RuntimeState,
    scope: impl Into<String>,
    message: impl Into<String>,
) {
    let error = PipelineError {
        at_unix_secs: unix_now(),
        scope: scope.into(),
        message: message.into(),
    };
    if runtime.errors.len() == MAX_ERRORS {
        runtime.errors.pop_front();
    }
    runtime.errors.push_back(error.clone());
    let path = config.state_root.join("errors.jsonl");
    if let Ok(mut line) = serde_json::to_vec(&error) {
        line.push(b'\n');
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
            let _ = std::io::Write::write_all(&mut file, &line);
        }
    }
}

async fn load_persisted_errors(state: &Arc<AppState>) {
    let path = state.config.state_root.join("errors.jsonl");
    const MAX_ERROR_TAIL_BYTES: u64 = 1024 * 1024;
    let Ok(mut file) = File::open(path) else {
        return;
    };
    let Ok(len) = file.metadata().map(|metadata| metadata.len()) else {
        return;
    };
    let read_len = len.min(MAX_ERROR_TAIL_BYTES);
    if file.seek(SeekFrom::End(-(read_len as i64))).is_err() {
        return;
    }
    let mut bytes = Vec::with_capacity(read_len as usize);
    if file.read_to_end(&mut bytes).is_err() {
        return;
    }
    let text = String::from_utf8_lossy(&bytes);
    let errors = text
        .lines()
        .rev()
        .take(MAX_ERRORS)
        .filter_map(|line| serde_json::from_str::<PipelineError>(line).ok())
        .collect::<Vec<_>>();
    let mut runtime = state.runtime.lock().await;
    runtime.errors.extend(errors.into_iter().rev());
}

fn is_acquisition_failure_key(key: &str) -> bool {
    key.starts_with("download:") || key.starts_with("preflight:")
}

fn persist_acquisition_failures(config: &NasPipelineConfig, runtime: &RuntimeState) -> Result<()> {
    let value = PersistedAcquisitionFailures {
        failures: runtime
            .failures
            .iter()
            .filter(|(key, _)| is_acquisition_failure_key(key))
            .map(|(key, message)| (key.clone(), message.clone()))
            .collect(),
    };
    let path = config.state_root.join(ACQUISITION_FAILURES_FILE);
    let temp = config.state_root.join(format!(
        ".{ACQUISITION_FAILURES_FILE}.{}.tmp",
        std::process::id()
    ));
    fs::write(&temp, serde_json::to_vec_pretty(&value)?)
        .with_context(|| format!("write acquisition failures {}", temp.display()))?;
    fs::rename(&temp, &path)
        .with_context(|| format!("publish acquisition failures {}", path.display()))
}

async fn load_acquisition_failures(state: &Arc<AppState>) {
    let path = state.config.state_root.join(ACQUISITION_FAILURES_FILE);
    let Ok(bytes) = fs::read(&path) else {
        return;
    };
    let Ok(saved) = serde_json::from_slice::<PersistedAcquisitionFailures>(&bytes) else {
        let mut runtime = state.runtime.lock().await;
        record_error(
            &state.config,
            &mut runtime,
            "state",
            format!(
                "ignore malformed acquisition failure state {}",
                path.display()
            ),
        );
        return;
    };
    let mut runtime = state.runtime.lock().await;
    runtime.failures.extend(saved.failures);
}

fn set_runtime_failure(
    config: &NasPipelineConfig,
    runtime: &mut RuntimeState,
    key: String,
    message: String,
) {
    let acquisition = is_acquisition_failure_key(&key);
    runtime.failures.insert(key, message);
    if acquisition && let Err(error) = persist_acquisition_failures(config, runtime) {
        record_error(
            config,
            runtime,
            "state",
            format!("persist acquisition failure: {error:#}"),
        );
    }
}

fn clear_runtime_failure(config: &NasPipelineConfig, runtime: &mut RuntimeState, key: &str) {
    let removed = runtime.failures.remove(key).is_some();
    if removed
        && is_acquisition_failure_key(key)
        && let Err(error) = persist_acquisition_failures(config, runtime)
    {
        record_error(
            config,
            runtime,
            "state",
            format!("persist cleared acquisition failure: {error:#}"),
        );
    }
}

fn acquisition_marker_artifact_valid(
    config: &NasPipelineConfig,
    marker: &AcquisitionMarker,
) -> bool {
    if car_paths_ambiguous(&config.car_root, marker.epoch) {
        return false;
    }
    match marker.kind.as_str() {
        "car_download" => {
            is_nonempty_file(&marker.expected_path)
                && receipt_matches_source(marker.epoch, &marker.expected_path, &marker.receipt_path)
        }
        "car_preflight" => {
            receipt_matches_source(marker.epoch, &marker.expected_path, &marker.receipt_path)
        }
        _ => false,
    }
}

fn reconcile_acquisition_state(config: &NasPipelineConfig, runtime: &mut RuntimeState) {
    let completed = runtime
        .failures
        .keys()
        .filter(|key| is_acquisition_failure_key(key))
        .filter_map(|key| {
            let (_, epoch) = key.split_once(':')?;
            let epoch = epoch.parse::<u64>().ok()?;
            let input = car_path(&config.car_root, epoch)?;
            receipt_matches_source(
                epoch,
                &input,
                &car_preflight_receipt_path(&config.state_root, epoch),
            )
            .then(|| key.clone())
        })
        .collect::<Vec<_>>();
    for key in completed {
        clear_runtime_failure(config, runtime, &key);
    }

    let marker_root = config.state_root.join("acquisitions");
    let Ok(entries) = fs::read_dir(&marker_root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let Ok(bytes) = fs::read(&path) else {
            continue;
        };
        let Ok(marker) = serde_json::from_slice::<AcquisitionMarker>(&bytes) else {
            continue;
        };
        if marker.schema_version != SCHEMA_VERSION {
            continue;
        }
        if acquisition_marker_artifact_valid(config, &marker) {
            let _ = fs::remove_file(&path);
            clear_runtime_failure(config, runtime, &acquisition_marker_key(&marker));
            continue;
        }
        if marker.pid != 0
            && process_cmdline_matches_acquisition(
                marker.pid,
                &config.blockzilla_bin,
                &marker.expected_path,
                &marker.kind,
            )
        {
            continue;
        }
        if acquisition_lock_held(config, marker.epoch) {
            continue;
        }
        let key = acquisition_marker_key(&marker);
        let message = format!(
            "{key} ownership ended without a valid canonical CAR/receipt; explicit retry required"
        );
        if !runtime.failures.contains_key(&key) {
            set_runtime_failure(config, runtime, key, message);
        }
    }
}

fn acquisition_marker_key(marker: &AcquisitionMarker) -> String {
    format!(
        "{}:{}",
        marker.kind.strip_prefix("car_").unwrap_or(&marker.kind),
        marker.epoch
    )
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedControlState {
    scheduler_paused: bool,
    scheduler_updated_unix_secs: u64,
    paused_jobs: BTreeSet<String>,
}

fn persist_control_state(config: &NasPipelineConfig, runtime: &RuntimeState) -> Result<()> {
    let value = PersistedControlState {
        scheduler_paused: runtime.scheduler_paused,
        scheduler_updated_unix_secs: runtime.scheduler_updated_unix_secs,
        paused_jobs: runtime.paused_jobs.clone(),
    };
    let path = config.state_root.join("control-state.json");
    let temp = config
        .state_root
        .join(format!(".control-state.{}.tmp", std::process::id()));
    fs::write(&temp, serde_json::to_vec_pretty(&value)?)
        .with_context(|| format!("write control state {}", temp.display()))?;
    fs::rename(&temp, &path).with_context(|| format!("publish control state {}", path.display()))
}

async fn load_control_state(state: &Arc<AppState>) {
    let path = state.config.state_root.join("control-state.json");
    let Ok(bytes) = fs::read(path) else {
        return;
    };
    let Ok(saved) = serde_json::from_slice::<PersistedControlState>(&bytes) else {
        return;
    };
    let mut runtime = state.runtime.lock().await;
    runtime.scheduler_paused = saved.scheduler_paused;
    runtime.scheduler_updated_unix_secs = saved.scheduler_updated_unix_secs;
    runtime.paused_jobs = saved.paused_jobs;
}

fn append_control_event(config: &NasPipelineConfig, action: &str, target: &str) -> Result<()> {
    let event = serde_json::json!({
        "at_unix_secs": unix_now(),
        "action": action,
        "target": target,
    });
    let mut line = serde_json::to_vec(&event)?;
    line.push(b'\n');
    let path = config.state_root.join("control-events.jsonl");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open control events {}", path.display()))?;
    std::io::Write::write_all(&mut file, &line)
        .with_context(|| format!("append control event {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(root: &Path) -> NasPipelineConfig {
        NasPipelineConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            blockzilla_bin: root.join("blockzilla"),
            repair_blockzilla_bin: None,
            car_root: root.join("cars"),
            archive_root: root.join("archives"),
            live_root: root.join("live"),
            state_root: root.join("state"),
            scan_concurrency: 4,
            scan_memory_mib: 800,
            finalizer_memory_mib: 512,
            memory_reserve_mib: 256,
            disk_reserve_gib: 256,
            level: 1,
            execute: false,
            no_access: true,
            start_epoch: Some(700),
            end_epoch: Some(700),
            priority_epoch_start: None,
            priority_epoch_end: None,
            car_source_url_template: None,
            download_concurrency: 1,
            preflight_car: false,
            poll_interval: Duration::from_secs(5),
            finalizer_lock: root.join("finalizer.lock"),
            ui_dir: None,
            control_token: None,
            allow_unauthenticated_controls: false,
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "hivezilla-pipeline-{label}-{}-{unique}",
            std::process::id()
        ))
    }

    fn write_historical_candidate(output: &Path, include_access: bool) {
        for name in [
            META_FILE,
            REGISTRY_COUNTS_FILE,
            BLOCKHASH_REGISTRY_FILE,
            BLOCKS_FILE,
            BLOCK_INDEX_FILE,
            POH_FILE,
            SHREDDING_FILE,
        ] {
            fs::write(output.join(name), b"ok").unwrap();
        }
        write_structural_registry_and_index(output, 1);
        write_valid_first_seen_manifest(output);
        for name in [
            SIGNATURES_FILE,
            VOTE_HASH_REGISTRY_FILE,
            "registry-hot-seed.bin",
        ] {
            fs::write(output.join(name), b"").unwrap();
        }
        if include_access {
            for name in [BLOCK_ACCESS_FILE, BLOCK_ACCESS_INDEX_FILE] {
                fs::write(output.join(name), b"ok").unwrap();
            }
        }
    }

    fn write_valid_first_seen_manifest(output: &Path) {
        fs::write(
            output.join(FIRST_SEEN_MANIFEST_FILE),
            b"version=1\nregistry_order=first_seen_v1\n",
        )
        .unwrap();
    }

    fn write_structural_registry_and_index(output: &Path, registry_keys: u64) {
        fs::write(
            output.join(REGISTRY_FILE),
            vec![0; usize::try_from(registry_keys * 32).unwrap()],
        )
        .unwrap();
        let mut index = Vec::from(*REGISTRY_INDEX_MAGIC);
        index.extend_from_slice(&REGISTRY_INDEX_VERSION.to_le_bytes());
        index.extend_from_slice(&REGISTRY_INDEX_HEADER_LEN.to_le_bytes());
        index.extend_from_slice(&registry_keys.to_le_bytes());
        index.resize(
            usize::try_from(u64::from(REGISTRY_INDEX_HEADER_LEN) + registry_keys * 12 + 1).unwrap(),
            0,
        );
        fs::write(output.join(REGISTRY_INDEX_FILE), index).unwrap();
    }

    fn write_scan_marker(output: &Path) {
        fs::write(
            output.join(SCAN_MARKER),
            format!("{SCAN_MARKER_MAGIC}\nregistry_keys=1\nreferences=1\ninclude_access=1\n"),
        )
        .unwrap();
    }

    fn test_epoch(root: &Path, epoch: u64, state: HistoricalState) -> EpochSnapshot {
        EpochSnapshot {
            epoch,
            state,
            registry_order: RegistryOrder::Unknown,
            input_path: Some(root.join("cars").join(format!("epoch-{epoch}.car"))),
            output_path: root.join("archives").join(format!("epoch-{epoch}")),
            car_bytes: 1,
            artifacts: Vec::new(),
            progress: ProgressSnapshot::default(),
            message: None,
            updated_unix_secs: 0,
        }
    }

    #[test]
    fn priority_band_orders_candidates_without_reordering_inventory() {
        let root = Path::new("/tmp/hivezilla-priority-order-test");
        let mut config = test_config(root);
        config.priority_epoch_start = Some(863);
        config.priority_epoch_end = Some(899);
        let epochs = [862, 863, 864, 899, 900]
            .into_iter()
            .map(|epoch| test_epoch(root, epoch, HistoricalState::Queued))
            .collect::<Vec<_>>();

        let ordered = prioritized_epochs(&config, &epochs)
            .into_iter()
            .map(|epoch| epoch.epoch)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec![899, 864, 863, 862, 900]);
        assert_eq!(
            epochs.iter().map(|epoch| epoch.epoch).collect::<Vec<_>>(),
            vec![862, 863, 864, 899, 900],
            "the canonical inventory must remain ascending for SSE patch merging"
        );

        config.priority_epoch_start = None;
        config.priority_epoch_end = None;
        assert_eq!(
            prioritized_epochs(&config, &epochs)
                .into_iter()
                .map(|epoch| epoch.epoch)
                .collect::<Vec<_>>(),
            vec![862, 863, 864, 899, 900]
        );
    }

    #[test]
    fn process_io_parser_and_rate_use_storage_byte_deltas() {
        assert_eq!(
            parse_process_io_bytes(
                "rchar: 999\nwchar: 888\nread_bytes: 1048576\nwrite_bytes: 2097152\n"
            ),
            Some((1024 * 1024, 2 * 1024 * 1024))
        );
        let sampled_at = Instant::now();
        let previous = ProcessIoSample {
            members: BTreeMap::from([(
                (7, 11),
                ProcessIoCounters {
                    read_bytes: 1024 * 1024,
                    write_bytes: 2 * 1024 * 1024,
                },
            )]),
            sampled_at,
        };
        let current = ProcessIoSample {
            members: BTreeMap::from([(
                (7, 11),
                ProcessIoCounters {
                    read_bytes: 5 * 1024 * 1024,
                    write_bytes: 4 * 1024 * 1024,
                },
            )]),
            sampled_at: sampled_at + Duration::from_secs(2),
        };
        assert_eq!(process_io_rate_mib(&previous, &current), Some((2.0, 1.0)));

        let regressed = ProcessIoSample {
            members: BTreeMap::from([(
                (7, 11),
                ProcessIoCounters {
                    read_bytes: 0,
                    write_bytes: 4 * 1024 * 1024,
                },
            )]),
            ..current.clone()
        };
        assert_eq!(process_io_rate_mib(&previous, &regressed), None);

        let child_started = ProcessIoSample {
            members: BTreeMap::from([
                ((7, 11), current.members[&(7, 11)]),
                (
                    (8, 12),
                    ProcessIoCounters {
                        read_bytes: 1,
                        write_bytes: 1,
                    },
                ),
            ]),
            ..current
        };
        assert_eq!(process_io_rate_mib(&previous, &child_started), None);
    }

    #[test]
    fn diskstats_parser_and_rate_use_matching_device_sector_deltas() {
        let diskstats = concat!(
            "   8       0 sda 100 0 9999 0 200 0 8888 0 0 0 0 0\n",
            "   8       1 sda1 10 1 2048 3 20 4 4096 5 0 0 0 0\n",
        );
        let counters = parse_diskstats_device(diskstats, 8, 1).unwrap();
        assert_eq!(
            counters,
            BlockDeviceIoCounters {
                major: 8,
                minor: 1,
                name: "sda1".to_string(),
                sectors_read: 2048,
                sectors_written: 4096,
            }
        );
        assert_eq!(parse_diskstats_device(diskstats, 8, 2), None);
        assert_eq!(parse_diskstats_device("8 1 truncated", 8, 1), None);

        let sampled_at = Instant::now();
        let previous = BlockDeviceIoSample {
            counters: counters.clone(),
            sampled_at,
        };
        let current = BlockDeviceIoSample {
            counters: BlockDeviceIoCounters {
                sectors_read: 10_240,
                sectors_written: 8_192,
                ..counters.clone()
            },
            sampled_at: sampled_at + Duration::from_secs(2),
        };
        assert_eq!(
            block_device_io_rate_mib(&previous, &current),
            Some((2.0, 1.0))
        );

        let reset = BlockDeviceIoSample {
            counters: BlockDeviceIoCounters {
                sectors_read: 0,
                ..current.counters.clone()
            },
            ..current.clone()
        };
        assert_eq!(block_device_io_rate_mib(&previous, &reset), None);

        let renamed = BlockDeviceIoSample {
            counters: BlockDeviceIoCounters {
                name: "dm-0".to_string(),
                ..current.counters.clone()
            },
            ..current
        };
        assert_eq!(block_device_io_rate_mib(&previous, &renamed), None);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn archive_device_sampling_is_safely_unavailable_off_linux() {
        assert!(read_archive_device_io_sample(Path::new("/"), Instant::now()).is_none());
    }

    fn queue_lane(epoch: u64, state: &str, elapsed_secs: f64, eta_secs: f64) -> LaneSnapshot {
        LaneSnapshot {
            id: format!("scan:{epoch}"),
            kind: "historical_scan".to_string(),
            epoch: Some(epoch),
            capture_id: None,
            phase: "scan".to_string(),
            state: state.to_string(),
            pid: None,
            progress: ProgressSnapshot {
                elapsed_secs: Some(elapsed_secs),
                eta_secs: Some(eta_secs),
                ..ProgressSnapshot::default()
            },
            rss_bytes: None,
            started_unix_secs: None,
            updated_unix_secs: 0,
        }
    }

    #[test]
    fn active_block_rate_counts_only_fresh_running_block_lanes() {
        let now = 10_000;
        let lane = |id: &str,
                    kind: &str,
                    lane_state: &str,
                    progress_state: Option<&str>,
                    rate: f64,
                    updated: u64| LaneSnapshot {
            id: id.to_string(),
            kind: kind.to_string(),
            epoch: Some(1),
            capture_id: None,
            phase: "work".to_string(),
            state: lane_state.to_string(),
            pid: None,
            progress: ProgressSnapshot {
                state: progress_state.map(str::to_string),
                blocks_per_sec: Some(rate),
                updated_unix_secs: Some(updated),
                ..ProgressSnapshot::default()
            },
            rss_bytes: None,
            started_unix_secs: None,
            updated_unix_secs: now,
        };
        let lanes = vec![
            lane(
                "scan:1",
                "historical_scan",
                "running",
                Some("running"),
                40.0,
                now,
            ),
            lane(
                "repair:1",
                "live_finalizer",
                "running",
                Some("packaging"),
                2.5,
                now - PROGRESS_STALE_SECS,
            ),
            lane(
                "paused",
                "historical_compact_reuse",
                "paused",
                Some("running"),
                100.0,
                now,
            ),
            lane(
                "stale",
                "historical_compact_reuse",
                "running",
                Some("running"),
                100.0,
                now - PROGRESS_STALE_SECS - 1,
            ),
            lane(
                "terminal-progress",
                "historical_finalizer",
                "running",
                Some("complete"),
                100.0,
                now,
            ),
            lane(
                "download",
                "car_download",
                "running",
                Some("running"),
                100.0,
                now,
            ),
            lane(
                "live-slots",
                "live_producer",
                "running",
                Some("running"),
                100.0,
                now,
            ),
            lane(
                "invalid",
                "historical_scan",
                "running",
                Some("running"),
                f64::NAN,
                now,
            ),
        ];
        assert_eq!(active_block_processing_rate(&lanes, now), 42.5);
        assert_eq!(
            active_block_processing_rate(&[], now).to_bits(),
            0.0_f64.to_bits()
        );

        let root = temp_root("terminal-rate");
        let mut complete = test_epoch(&root, 1, HistoricalState::Complete);
        complete.progress.blocks_per_sec = Some(999.0);
        assert_eq!(summarize_epochs(&[complete]).blocks_per_sec, 0.0);
    }

    #[test]
    fn snapshot_patch_contains_only_changed_and_removed_epochs() {
        let root = temp_root("snapshot-patch");
        let mut previous = empty_snapshot(true);
        previous.sequence = 7;
        previous.epochs = vec![
            test_epoch(&root, 10, HistoricalState::Complete),
            test_epoch(&root, 11, HistoricalState::Queued),
        ];

        let mut current = previous.clone();
        current.sequence = 8;
        current.now_unix_secs = previous.now_unix_secs.saturating_add(5);
        current.epochs.remove(1);
        current.epochs[0].progress.blocks_done = 42;
        current
            .epochs
            .push(test_epoch(&root, 12, HistoricalState::Scanning));

        let patch = SnapshotPatch::between(&previous, &current);
        assert_eq!(patch.sequence, 8);
        assert_eq!(
            patch
                .epochs_changed
                .iter()
                .map(|epoch| epoch.epoch)
                .collect::<Vec<_>>(),
            vec![10, 12]
        );
        assert_eq!(patch.epochs_removed, vec![11]);
    }

    #[test]
    fn realtime_updates_use_patch_events_and_lag_requests_resync() {
        let previous = empty_snapshot(true);
        let mut current = previous.clone();
        current.sequence = 4;
        let envelope = RealtimeEnvelope {
            event_type: "snapshot_patch",
            sequence: current.sequence,
            data: SnapshotPatch::between(&previous, &current),
        };
        let message = realtime_message(Ok(envelope), current.sequence);
        assert_eq!(message.event_name(), "snapshot_patch");
        let RealtimeMessage::SnapshotPatch(envelope) = message else {
            panic!("expected snapshot patch")
        };
        let value = serde_json::to_value(envelope).unwrap();
        assert_eq!(value["type"], "snapshot_patch");
        assert_eq!(value["sequence"], 4);

        let lagged = realtime_message(
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(3)),
            9,
        );
        assert_eq!(lagged.event_name(), "resync");
        let RealtimeMessage::Resync(envelope) = lagged else {
            panic!("expected resync")
        };
        assert_eq!(envelope.sequence, 9);
        assert_eq!(envelope.data.reason, "subscriber_lagged");
        assert_eq!(envelope.data.skipped, 3);
        assert_eq!(envelope.data.status_url, "/api/v1/status");
    }

    #[test]
    fn active_progress_updates_emit_once_and_skip_completed_epochs() {
        let root = temp_root("active-progress-patch");
        let now = 10_000;
        let mut snapshot = empty_snapshot(true);
        snapshot.epochs = vec![
            test_epoch(&root, 699, HistoricalState::Complete),
            test_epoch(&root, 700, HistoricalState::Scanning),
        ];
        snapshot.epochs[0].progress.blocks_done = 999;
        snapshot.lanes = vec![LaneSnapshot {
            id: "scan:700".to_string(),
            kind: "historical_scan".to_string(),
            epoch: Some(700),
            capture_id: None,
            phase: "scan".to_string(),
            state: "running".to_string(),
            pid: None,
            progress: ProgressSnapshot {
                state: Some("running".to_string()),
                updated_unix_secs: Some(now - 1),
                ..ProgressSnapshot::default()
            },
            rss_bytes: None,
            started_unix_secs: Some(now - 10),
            updated_unix_secs: now - 1,
        }];
        let update = LaneProgressUpdate {
            id: "scan:700".to_string(),
            baseline: snapshot.lanes[0].progress.clone(),
            baseline_identity: lane_progress_worker_identity(&snapshot.lanes[0]),
            progress: ProgressSnapshot {
                phase: Some("scan_blocks".to_string()),
                state: Some("running".to_string()),
                blocks_done: 42,
                transactions_done: 123,
                blocks_per_sec: Some(12.5),
                updated_unix_secs: Some(now),
                ..ProgressSnapshot::default()
            },
        };

        let changed =
            apply_active_progress_updates(&mut snapshot, std::slice::from_ref(&update), &[], now)
                .expect("new progress must produce a patch");
        assert_eq!(
            changed.iter().map(|epoch| epoch.epoch).collect::<Vec<_>>(),
            vec![700]
        );
        assert_eq!(snapshot.epochs[0].progress.blocks_done, 999);
        assert_eq!(snapshot.epochs[1].progress.blocks_done, 42);
        assert_eq!(snapshot.summary.blocks_per_sec, 12.5);

        assert!(
            apply_active_progress_updates(&mut snapshot, &[update], &[], now).is_none(),
            "an unchanged source must not emit an idle timer patch"
        );
    }

    #[test]
    fn active_progress_targets_do_not_walk_completed_epoch_rows() {
        let root = temp_root("active-progress-targets");
        let config = test_config(&root);
        let mut snapshot = empty_snapshot(true);
        snapshot.epochs = (0..1_000)
            .map(|epoch| test_epoch(&root, epoch, HistoricalState::Complete))
            .collect();

        let idle = collect_active_progress_targets(&config, &snapshot);
        assert!(idle.lanes.is_empty());
        assert!(idle.live.is_empty());

        snapshot.lanes.push(LaneSnapshot {
            id: "scan:1000".to_string(),
            kind: "historical_scan".to_string(),
            epoch: Some(1_000),
            capture_id: None,
            phase: "scan".to_string(),
            state: "running".to_string(),
            pid: None,
            progress: ProgressSnapshot::default(),
            rss_bytes: None,
            started_unix_secs: None,
            updated_unix_secs: 0,
        });
        let active = collect_active_progress_targets(&config, &snapshot);
        assert_eq!(active.lanes.len(), 1);
        assert!(active.live.is_empty());
    }

    #[tokio::test]
    async fn monitor_publication_is_ordered_without_taking_runtime_lock() {
        let root = temp_root("monitor-publication-lock");
        let mut snapshot = empty_snapshot(true);
        let baseline = ProgressSnapshot {
            phase: Some("scan".to_string()),
            state: Some("running".to_string()),
            pid: Some(10),
            blocks_done: 10,
            last_slot: Some(110),
            updated_unix_secs: Some(99),
            ..ProgressSnapshot::default()
        };
        snapshot.lanes.push(LaneSnapshot {
            id: "scan:700".to_string(),
            kind: "historical_scan".to_string(),
            epoch: Some(700),
            capture_id: None,
            phase: "scan".to_string(),
            state: "running".to_string(),
            pid: baseline.pid,
            progress: baseline.clone(),
            rss_bytes: None,
            started_unix_secs: Some(90),
            updated_unix_secs: 99,
        });
        let (updates, _) = broadcast::channel(4);
        let mut receiver = updates.subscribe();
        let state = Arc::new(AppState {
            config: test_config(&root),
            snapshot: RwLock::new(snapshot),
            updates,
            sequence: AtomicU64::new(0),
            publication: Mutex::new(()),
            runtime: Mutex::new(RuntimeState::default()),
        });
        let baseline_identity = {
            let snapshot = state.snapshot.read().await;
            lane_progress_worker_identity(&snapshot.lanes[0])
        };
        let mut first = baseline.clone();
        first.blocks_done = 11;
        first.last_slot = Some(111);
        first.updated_unix_secs = Some(100);

        let publication = state.publication.lock().await;
        let publish_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            publish_monitored_progress(
                &publish_state,
                vec![LaneProgressUpdate {
                    id: "scan:700".to_string(),
                    baseline,
                    baseline_identity,
                    progress: first,
                }],
                Vec::new(),
                100,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(state.runtime.try_lock().is_ok());
        assert!(!task.is_finished());
        drop(publication);
        assert!(task.await.unwrap());
        let first_event = receiver.recv().await.unwrap();
        assert_eq!(first_event.sequence, 1);

        let (baseline, baseline_identity) = {
            let snapshot = state.snapshot.read().await;
            (
                snapshot.lanes[0].progress.clone(),
                lane_progress_worker_identity(&snapshot.lanes[0]),
            )
        };
        let mut second = baseline.clone();
        second.blocks_done += 1;
        second.last_slot = second.last_slot.map(|slot| slot + 1);
        second.updated_unix_secs = Some(101);
        assert!(
            publish_monitored_progress(
                &state,
                vec![LaneProgressUpdate {
                    id: "scan:700".to_string(),
                    baseline,
                    baseline_identity,
                    progress: second,
                }],
                Vec::new(),
                101,
            )
            .await
        );
        let second_event = receiver.recv().await.unwrap();
        assert_eq!(second_event.sequence, 2);
        assert_eq!(state.snapshot.read().await.sequence, 2);
    }

    #[test]
    fn monitor_cannot_overwrite_progress_published_by_a_newer_reconcile() {
        let mut snapshot = empty_snapshot(true);
        let baseline = ProgressSnapshot {
            pid: Some(10),
            blocks_done: 10,
            transactions_done: 100,
            last_slot: Some(110),
            blocks_per_sec: Some(10.0),
            rss_bytes: Some(100),
            updated_unix_secs: Some(99),
            ..ProgressSnapshot::default()
        };
        let reconciled = ProgressSnapshot {
            pid: Some(20),
            blocks_done: 20,
            transactions_done: 200,
            last_slot: Some(120),
            blocks_per_sec: Some(20.0),
            rss_bytes: Some(200),
            updated_unix_secs: Some(100),
            ..ProgressSnapshot::default()
        };
        snapshot.lanes.push(LaneSnapshot {
            id: "scan:700".to_string(),
            kind: "historical_scan".to_string(),
            epoch: Some(700),
            capture_id: None,
            phase: "scan".to_string(),
            state: "running".to_string(),
            pid: reconciled.pid,
            progress: reconciled.clone(),
            rss_bytes: reconciled.rss_bytes,
            started_unix_secs: Some(90),
            updated_unix_secs: 100,
        });

        let mut older_counters = baseline.clone();
        older_counters.blocks_done = 15;
        older_counters.transactions_done = 150;
        older_counters.last_slot = Some(115);
        older_counters.updated_unix_secs = Some(100);
        assert!(
            apply_active_progress_updates(
                &mut snapshot,
                &[LaneProgressUpdate {
                    id: "scan:700".to_string(),
                    baseline: baseline.clone(),
                    baseline_identity: ProgressWorkerIdentity {
                        pid: baseline.pid,
                        phase: Some("scan".to_string()),
                        started_unix_secs: Some(90),
                    },
                    progress: older_counters,
                }],
                &[],
                100,
            )
            .is_none()
        );

        let mut older_metrics = reconciled.clone();
        older_metrics.pid = baseline.pid;
        older_metrics.rss_bytes = baseline.rss_bytes;
        older_metrics.blocks_per_sec = baseline.blocks_per_sec;
        assert!(
            apply_active_progress_updates(
                &mut snapshot,
                &[LaneProgressUpdate {
                    id: "scan:700".to_string(),
                    baseline: baseline.clone(),
                    baseline_identity: ProgressWorkerIdentity {
                        pid: baseline.pid,
                        phase: Some("scan".to_string()),
                        started_unix_secs: Some(90),
                    },
                    progress: older_metrics,
                }],
                &[],
                100,
            )
            .is_none()
        );
        assert_eq!(snapshot.lanes[0].progress, reconciled);

        let fresh_baseline = snapshot.lanes[0].progress.clone();
        let fresh_identity = lane_progress_worker_identity(&snapshot.lanes[0]);
        let mut newer = reconciled;
        newer.pid = Some(30);
        newer.blocks_done = 21;
        newer.transactions_done = 210;
        newer.last_slot = Some(121);
        newer.updated_unix_secs = Some(101);
        assert!(
            apply_active_progress_updates(
                &mut snapshot,
                &[LaneProgressUpdate {
                    id: "scan:700".to_string(),
                    baseline: fresh_baseline,
                    baseline_identity: fresh_identity,
                    progress: newer.clone(),
                }],
                &[],
                101,
            )
            .is_some()
        );
        assert_eq!(snapshot.lanes[0].progress, newer);
    }

    #[test]
    fn progress_source_change_coalesces_timestamp_and_counters() {
        let baseline = ProgressSnapshot {
            blocks_done: 10,
            last_slot: Some(100),
            updated_unix_secs: Some(50),
            ..ProgressSnapshot::default()
        };
        assert!(!progress_source_changed(&baseline, &baseline));

        let mut advanced = baseline.clone();
        advanced.blocks_done += 1;
        assert!(progress_source_changed(&baseline, &advanced));

        let mut older = advanced;
        older.updated_unix_secs = Some(49);
        assert!(!progress_source_changed(&baseline, &older));

        let restarted = ProgressSnapshot {
            blocks_done: 11,
            transactions_done: 0,
            last_slot: Some(101),
            updated_unix_secs: Some(51),
            ..ProgressSnapshot::default()
        };
        assert!(progress_source_changed(&baseline, &restarted));

        let next_phase = ProgressSnapshot {
            phase: Some("hot_build".to_string()),
            blocks_done: 0,
            transactions_done: 0,
            last_slot: None,
            updated_unix_secs: Some(51),
            ..ProgressSnapshot::default()
        };
        assert!(progress_source_changed(&baseline, &next_phase));

        let same_phase_baseline = ProgressSnapshot {
            phase: Some("materialize".to_string()),
            blocks_done: 100,
            last_slot: Some(200),
            updated_unix_secs: Some(60),
            ..ProgressSnapshot::default()
        };
        let same_phase_regression = ProgressSnapshot {
            phase: Some("materialize".to_string()),
            blocks_done: 90,
            last_slot: Some(190),
            updated_unix_secs: Some(61),
            ..ProgressSnapshot::default()
        };
        assert!(!progress_source_changed(
            &same_phase_baseline,
            &same_phase_regression
        ));
    }

    #[test]
    fn monitor_progress_preserves_newer_controller_disk_sample() {
        let baseline = ProgressSnapshot {
            phase: Some("scan".to_string()),
            pid: Some(10),
            blocks_done: 10,
            last_slot: Some(110),
            disk_read_mib_per_sec: Some(1.0),
            disk_write_mib_per_sec: Some(2.0),
            updated_unix_secs: Some(99),
            ..ProgressSnapshot::default()
        };
        let mut current = baseline.clone();
        current.disk_read_mib_per_sec = Some(100.0);
        current.disk_write_mib_per_sec = Some(20.0);
        let mut snapshot = empty_snapshot(true);
        snapshot.lanes.push(LaneSnapshot {
            id: "scan:700".to_string(),
            kind: "historical_scan".to_string(),
            epoch: Some(700),
            capture_id: None,
            phase: "scan".to_string(),
            state: "running".to_string(),
            pid: current.pid,
            progress: current,
            rss_bytes: None,
            started_unix_secs: Some(90),
            updated_unix_secs: 99,
        });
        let mut candidate = baseline.clone();
        candidate.blocks_done = 11;
        candidate.last_slot = Some(111);
        candidate.updated_unix_secs = Some(100);

        assert!(
            apply_active_progress_updates(
                &mut snapshot,
                &[LaneProgressUpdate {
                    id: "scan:700".to_string(),
                    baseline,
                    baseline_identity: ProgressWorkerIdentity {
                        pid: Some(10),
                        phase: Some("scan".to_string()),
                        started_unix_secs: Some(90),
                    },
                    progress: candidate,
                }],
                &[],
                100,
            )
            .is_some()
        );
        assert_eq!(snapshot.lanes[0].progress.blocks_done, 11);
        assert_eq!(
            snapshot.lanes[0].progress.disk_read_mib_per_sec,
            Some(100.0)
        );
        assert_eq!(
            snapshot.lanes[0].progress.disk_write_mib_per_sec,
            Some(20.0)
        );
    }

    #[test]
    fn live_progress_emits_once_when_unchanged_source_becomes_stale() {
        let updated = 1_000;
        let baseline = ProgressSnapshot {
            blocks_done: 10,
            last_slot: Some(100),
            blocks_per_sec: Some(20.0),
            slots_per_sec: Some(25.0),
            eta_secs: Some(30.0),
            updated_unix_secs: Some(updated),
            ..ProgressSnapshot::default()
        };
        let mut stale = baseline.clone();
        hide_stale_live_rates(&mut stale, updated + PROGRESS_STALE_SECS + 1);

        assert_eq!(stale.blocks_done, baseline.blocks_done);
        assert_eq!(stale.last_slot, baseline.last_slot);
        assert!(progress_source_changed(&baseline, &stale));
        assert!(!progress_source_changed(&stale, &stale));
    }

    #[test]
    fn lane_progress_emits_one_stale_transition_and_clears_global_rate() {
        let updated = 1_000;
        let now = updated + PROGRESS_STALE_SECS + 1;
        let baseline = ProgressSnapshot {
            phase: Some("scan".to_string()),
            state: Some("running".to_string()),
            blocks_done: 10,
            last_slot: Some(100),
            blocks_per_sec: Some(20.0),
            input_mib_per_sec: Some(30.0),
            eta_secs: Some(40.0),
            updated_unix_secs: Some(updated),
            ..ProgressSnapshot::default()
        };
        let mut stale = baseline.clone();
        hide_stale_lane_rates(&mut stale, now);
        let mut snapshot = empty_snapshot(true);
        snapshot.lanes.push(LaneSnapshot {
            id: "scan:700".to_string(),
            kind: "historical_scan".to_string(),
            epoch: Some(700),
            capture_id: None,
            phase: "scan".to_string(),
            state: "running".to_string(),
            pid: None,
            progress: baseline.clone(),
            rss_bytes: None,
            started_unix_secs: Some(900),
            updated_unix_secs: updated,
        });
        let update = LaneProgressUpdate {
            id: "scan:700".to_string(),
            baseline,
            baseline_identity: lane_progress_worker_identity(&snapshot.lanes[0]),
            progress: stale,
        };
        assert!(
            apply_active_progress_updates(&mut snapshot, std::slice::from_ref(&update), &[], now,)
                .is_some()
        );
        assert_eq!(snapshot.lanes[0].progress.blocks_per_sec, None);
        assert_eq!(snapshot.lanes[0].progress.input_mib_per_sec, None);
        assert_eq!(snapshot.lanes[0].progress.eta_secs, None);
        assert_eq!(snapshot.summary.blocks_per_sec, 0.0);
        assert!(apply_active_progress_updates(&mut snapshot, &[update], &[], now).is_none());
    }

    #[test]
    fn packaging_progress_preserves_live_source_block_count() {
        let root = temp_root("packaging-source-count");
        let mut capture = test_live_capture(&root, "epoch-700", 700, LiveState::Packaging, 10);
        capture.blocks_written = 431_781;

        apply_progress_to_live_capture(
            &mut capture,
            ProgressSnapshot {
                blocks_done: 162_304,
                blocks_total: 431_781,
                updated_unix_secs: Some(11),
                ..ProgressSnapshot::default()
            },
        );
        assert_eq!(capture.blocks_written, 431_781);
        assert_eq!(capture.progress.blocks_done, 162_304);

        capture.state = LiveState::Capturing;
        apply_progress_to_live_capture(
            &mut capture,
            ProgressSnapshot {
                blocks_done: 431_900,
                updated_unix_secs: Some(12),
                ..ProgressSnapshot::default()
            },
        );
        assert_eq!(capture.blocks_written, 431_900);
    }

    #[test]
    fn capturing_progress_keeps_monotonic_source_bounds_and_count() {
        let root = temp_root("capturing-monotonic");
        let mut capture = test_live_capture(&root, "epoch-700", 700, LiveState::Capturing, 10);
        capture.first_slot = Some(100);
        capture.last_slot = Some(200);
        capture.blocks_written = 50;

        apply_progress_to_live_capture(
            &mut capture,
            ProgressSnapshot {
                blocks_done: 40,
                first_slot: Some(150),
                last_slot: Some(190),
                updated_unix_secs: Some(11),
                ..ProgressSnapshot::default()
            },
        );
        assert_eq!(capture.blocks_written, 50);
        assert_eq!(capture.first_slot, Some(100));
        assert_eq!(capture.last_slot, Some(200));
        assert_eq!(capture.progress.blocks_done, 50);
        assert_eq!(capture.progress.first_slot, Some(100));
        assert_eq!(capture.progress.last_slot, Some(200));
    }

    #[test]
    fn packaging_live_update_synchronizes_lane_and_global_rate() {
        let root = temp_root("packaging-lane-sync");
        let now = 100;
        let mut snapshot = empty_snapshot(true);
        let baseline = ProgressSnapshot {
            phase: Some("materialize".to_string()),
            state: Some("running".to_string()),
            pid: Some(42),
            blocks_done: 100,
            blocks_total: 200,
            first_slot: Some(700 * SLOTS_PER_EPOCH),
            last_slot: Some(700 * SLOTS_PER_EPOCH + 100),
            blocks_per_sec: Some(2.0),
            rss_bytes: Some(1_000),
            updated_unix_secs: Some(now - 1),
            ..ProgressSnapshot::default()
        };
        let mut capture = test_live_capture(&root, "epoch-700", 700, LiveState::Packaging, now - 1);
        capture.blocks_written = 200;
        capture.progress = baseline.clone();
        snapshot.live.push(capture);
        snapshot.lanes.push(LaneSnapshot {
            id: "live:epoch-700".to_string(),
            kind: "live_finalizer".to_string(),
            epoch: Some(700),
            capture_id: Some("epoch-700".to_string()),
            phase: "materialize".to_string(),
            state: "running".to_string(),
            pid: baseline.pid,
            progress: baseline.clone(),
            rss_bytes: baseline.rss_bytes,
            started_unix_secs: Some(90),
            updated_unix_secs: now - 1,
        });
        let progress = ProgressSnapshot {
            phase: Some("materialize".to_string()),
            state: Some("running".to_string()),
            pid: Some(42),
            blocks_done: 120,
            blocks_total: 200,
            first_slot: baseline.first_slot,
            last_slot: Some(700 * SLOTS_PER_EPOCH + 120),
            blocks_per_sec: Some(7.5),
            rss_bytes: Some(2_000),
            updated_unix_secs: Some(now),
            ..ProgressSnapshot::default()
        };
        let baseline_identity = live_progress_worker_identity(&snapshot.live[0]);

        assert!(
            apply_active_progress_updates(
                &mut snapshot,
                &[],
                &[LiveProgressUpdate {
                    id: "epoch-700".to_string(),
                    baseline,
                    baseline_identity,
                    progress: progress.clone(),
                }],
                now,
            )
            .is_some()
        );
        assert_eq!(snapshot.live[0].blocks_written, 200);
        assert_eq!(snapshot.lanes[0].progress, progress);
        assert_eq!(snapshot.lanes[0].phase, "materialize");
        assert_eq!(snapshot.lanes[0].pid, Some(42));
        assert_eq!(snapshot.lanes[0].rss_bytes, Some(2_000));
        assert_eq!(snapshot.lanes[0].updated_unix_secs, now);
        assert_eq!(snapshot.summary.blocks_per_sec, 7.5);
    }

    #[test]
    fn reconcile_preserves_only_newer_same_identity_live_progress_and_time() {
        let root = temp_root("reconcile-live-preserve");
        let mut next = empty_snapshot(true);
        next.now_unix_secs = 100;
        let mut next_capture =
            test_live_capture(&root, "epoch-700", 700, LiveState::Capturing, 100);
        next_capture.progress = ProgressSnapshot {
            phase: Some("capturing".to_string()),
            state: Some("capturing".to_string()),
            pid: Some(10),
            blocks_done: 10,
            first_slot: Some(100),
            last_slot: Some(110),
            updated_unix_secs: Some(100),
            ..ProgressSnapshot::default()
        };
        next_capture.first_slot = Some(100);
        next_capture.last_slot = Some(110);
        next_capture.blocks_written = 10;
        next.live.push(next_capture);

        let mut published = next.clone();
        published.now_unix_secs = 101;
        published.live[0].progress.blocks_done = 11;
        published.live[0].progress.last_slot = Some(111);
        published.live[0].progress.updated_unix_secs = Some(101);
        published.live[0].blocks_written = 11;
        published.live[0].last_slot = Some(111);

        preserve_newer_published_progress(&mut next, &published);
        assert_eq!(next.now_unix_secs, 101);
        assert_eq!(next.live[0].blocks_written, 11);
        assert_eq!(next.live[0].last_slot, Some(111));

        let mut restarted_next = next.clone();
        restarted_next.live[0].progress.pid = Some(20);
        restarted_next.live[0].progress.blocks_done = 20;
        restarted_next.live[0].progress.last_slot = Some(120);
        restarted_next.live[0].progress.updated_unix_secs = Some(102);
        restarted_next.live[0].blocks_written = 20;
        restarted_next.live[0].last_slot = Some(120);
        let before = restarted_next.live[0].clone();
        let mut old_published = published;
        old_published.now_unix_secs = 103;
        old_published.live[0].progress.updated_unix_secs = Some(103);

        preserve_newer_published_progress(&mut restarted_next, &old_published);
        assert_eq!(restarted_next.now_unix_secs, 103);
        assert_eq!(restarted_next.live[0].progress, before.progress);
        assert_eq!(restarted_next.live[0].blocks_written, before.blocks_written);
    }

    #[test]
    fn runnable_queue_eta_drains_lanes_and_excludes_action_required_epochs() {
        let root = temp_root("runnable-queue-eta");
        let mut complete = test_epoch(&root, 1, HistoricalState::ScanReady);
        complete.progress.elapsed_secs = Some(600.0);
        write_ownership(
            &complete.output_path,
            "historical_scan",
            "1",
            "scan_ready",
            None,
        )
        .unwrap();
        let epochs = vec![
            complete,
            test_epoch(&root, 2, HistoricalState::Queued),
            test_epoch(&root, 3, HistoricalState::Queued),
            test_epoch(&root, 4, HistoricalState::Scanning),
            test_epoch(&root, 5, HistoricalState::Scanning),
            test_epoch(&root, 6, HistoricalState::Blocked),
            test_epoch(&root, 7, HistoricalState::Failed),
        ];
        let lanes = vec![
            queue_lane(4, "running", 100.0, 500.0),
            queue_lane(5, "running", 200.0, 400.0),
        ];

        let eta = estimate_runnable_queue_eta(&epochs, &lanes, 2, false);
        assert_eq!(eta.eta_secs, Some(1_100.0));
        assert_eq!(eta.jobs_remaining, 4);
        assert_eq!(eta.capacity, 2);
        assert_eq!(eta.job_duration_secs, Some(600.0));
        assert_eq!(eta.duration_samples, 3);
        assert!(eta.reason.as_deref().unwrap().contains("2 action-required"));

        let empty = estimate_runnable_queue_eta(
            &[test_epoch(&root, 8, HistoricalState::Blocked)],
            &[],
            2,
            false,
        );
        assert_eq!(empty.eta_secs, Some(0.0));
        assert_eq!(empty.jobs_remaining, 0);

        let paused = estimate_runnable_queue_eta(&epochs, &lanes, 2, true);
        assert_eq!(paused.eta_secs, None);
        assert!(
            paused
                .reason
                .as_deref()
                .unwrap()
                .contains("scheduler is paused")
        );
        fs::remove_dir_all(root).unwrap();
    }

    fn test_live_capture(
        root: &Path,
        id: &str,
        epoch: u64,
        state: LiveState,
        updated_unix_secs: u64,
    ) -> LiveCaptureSnapshot {
        LiveCaptureSnapshot {
            id: id.to_string(),
            epoch: Some(epoch),
            is_current: false,
            state,
            capture_dir: root.join("live").join(id),
            output_path: Some(root.join("archives").join(format!("epoch-{epoch}"))),
            ready_to_package: false,
            repair_gate: state == LiveState::RepairGate,
            source_capture_ids: Vec::new(),
            superseded_by: None,
            first_slot: Some(epoch * SLOTS_PER_EPOCH),
            last_slot: Some(epoch * SLOTS_PER_EPOCH),
            blocks_written: 1,
            artifacts: Vec::new(),
            progress: ProgressSnapshot::default(),
            eta_secs: None,
            slots_per_sec: None,
            rss_bytes: None,
            peak_rss_bytes: None,
            message: None,
            updated_unix_secs,
        }
    }

    fn write_test_repair_bundle(
        config: &NasPipelineConfig,
        bundle_id: &str,
        epoch: u64,
        source_ids: &[&str],
    ) -> PathBuf {
        let bundle = config.live_root.join(bundle_id);
        fs::create_dir_all(bundle.join("repair")).unwrap();
        fs::write(
            bundle.join(LIVE_REPAIR_PLAN_FILE),
            b"{\"kind\":\"header\"}\n",
        )
        .unwrap();
        let start = epoch * SLOTS_PER_EPOCH;
        let end = start + SLOTS_PER_EPOCH - 1;
        let marker = serde_json::json!({
            "version": 1,
            "state": "rpc_fallback_missing_poh_and_shredding",
            "epoch": epoch,
            "epoch_start_slot": start,
            "epoch_end_slot": end,
            "live_blocks": 2,
            "rpc_only_blocks": 1,
            "produced_blocks": 3,
            "first_produced_slot": start,
            "last_produced_slot": end,
            "block_sources": source_ids.iter().map(|id| serde_json::json!({
                "original_capture_dir": config.live_root.join(id),
                "selected_blocks": 1
            })).collect::<Vec<_>>(),
            "rpc_only_slots": [{"slot": start + 1, "ignored": "large fields are skipped"}],
            "merge_plan": LIVE_REPAIR_PLAN_FILE,
            "publication_ready": false
        });
        fs::write(
            bundle.join(LIVE_REPAIR_REQUIRED_MARKER),
            serde_json::to_vec(&marker).unwrap(),
        )
        .unwrap();
        bundle
    }

    fn write_test_repair_compacted(config: &NasPipelineConfig, epoch: u64) -> PathBuf {
        use blockzilla_format::{
            ArchiveV2HotBlockIndexRow, ArchiveV2HotMetaRecord, WincodeArchiveV2Footer,
            WincodeArchiveV2Header, WincodeArchiveV2PohRecord, WincodeLeb128FramedWriter,
            write_archive_v2_hot_block_index,
        };

        let output = config.archive_root.join(format!("epoch-{epoch}"));
        fs::create_dir_all(output.join("repair")).unwrap();
        let start = epoch * SLOTS_PER_EPOCH;
        let rows = (0..3u32)
            .map(|block_id| ArchiveV2HotBlockIndexRow {
                block_id,
                slot: start + u64::from(block_id),
                compressed_offset: u64::from(block_id),
                compressed_len: 1,
                uncompressed_len: 2,
                tx_count: 0,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 0,
            })
            .collect::<Vec<_>>();
        fs::write(output.join(BLOCKS_FILE), [1, 2, 3]).unwrap();
        write_archive_v2_hot_block_index(&output.join(BLOCK_INDEX_FILE), 3, 1, 0, &rows).unwrap();

        let mut meta = WincodeLeb128FramedWriter::new(std::io::BufWriter::new(
            File::create(output.join(META_FILE)).unwrap(),
        ));
        meta.write(&ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
        }))
        .unwrap();
        meta.write(&ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
            blocks: 3,
            ..WincodeArchiveV2Footer::default()
        }))
        .unwrap();
        meta.flush().unwrap();

        fs::write(output.join(REGISTRY_FILE), [7; 32]).unwrap();
        fs::write(output.join(REGISTRY_COUNTS_FILE), [1]).unwrap();
        fs::write(output.join(REGISTRY_INDEX_FILE), [1]).unwrap();
        fs::write(output.join(BLOCKHASH_REGISTRY_FILE), [9; 96]).unwrap();
        fs::write(output.join(SIGNATURES_FILE), []).unwrap();
        fs::write(output.join(VOTE_HASH_REGISTRY_FILE), [1]).unwrap();

        let mut poh = WincodeLeb128FramedWriter::new(std::io::BufWriter::new(
            File::create(output.join(LIVE_REPAIR_AVAILABLE_POH_FILE)).unwrap(),
        ));
        for (block_id, slot) in [(0, start), (2, start + 2)] {
            poh.write(&WincodeArchiveV2PohRecord {
                block_id,
                slot,
                entries: Vec::new(),
            })
            .unwrap();
        }
        poh.flush().unwrap();

        let manifest_sha = "11".repeat(32);
        let plan_sha = "22".repeat(32);
        let source_marker = serde_json::json!({
            "version": 1,
            "state": "repair_materialized_missing_poh_and_shredding",
            "canonical": false,
            "publication_ready": false,
            "epoch": epoch,
            "epoch_start_slot": start,
            "epoch_end_slot": start + SLOTS_PER_EPOCH - 1,
            "live_blocks": 2,
            "rpc_only_blocks": 1,
            "produced_blocks": 3,
            "transactions": 0,
            "manifest_sha256": manifest_sha,
            "merge_plan_sha256": plan_sha
        });
        let source_marker_path = output.join(LIVE_REPAIR_SOURCE_MATERIALIZED_MARKER);
        fs::write(
            &source_marker_path,
            serde_json::to_vec(&source_marker).unwrap(),
        )
        .unwrap();
        let source_marker_sha = repair_sha256_file(&source_marker_path).unwrap();
        let marker = serde_json::json!({
            "version": 1,
            "state": "degraded_hot_archive_missing_poh_and_shredding",
            "canonical": false,
            "publication_ready": false,
            "block_archive_ready": true,
            "block_access_ready": false,
            "epoch": epoch,
            "epoch_start_slot": start,
            "epoch_end_slot": start + SLOTS_PER_EPOCH - 1,
            "live_blocks": 2,
            "rpc_only_blocks": 1,
            "produced_blocks": 3,
            "transactions": 0,
            "signatures": 0,
            "zstd_level": 1,
            "compressed_bytes": 3,
            "uncompressed_bytes": 6,
            "files": {
                "blocks": BLOCKS_FILE,
                "index": BLOCK_INDEX_FILE,
                "meta": META_FILE,
                "registry": REGISTRY_FILE,
                "registry_counts": REGISTRY_COUNTS_FILE,
                "registry_index": REGISTRY_INDEX_FILE,
                "blockhashes": BLOCKHASH_REGISTRY_FILE,
                "signatures": SIGNATURES_FILE,
                "vote_hashes": VOTE_HASH_REGISTRY_FILE,
                "available_poh": LIVE_REPAIR_AVAILABLE_POH_FILE
            },
            "poh_coverage": {
                "available_records": 2,
                "available_entries": 0,
                "missing_records": 1,
                "produced_id_space": 3,
                "record_ids_have_explicit_gaps": true,
                "missing_record_ids": [1]
            },
            "shredding_coverage": {
                "available_records": 0,
                "missing_records": 3,
                "canonical_sidecar_emitted": false
            },
            "source_materialized_marker_sha256": source_marker_sha,
            "source_manifest_sha256": manifest_sha,
            "source_merge_plan_sha256": plan_sha,
            "limitations": ["canonical PoH and shredding are incomplete"]
        });
        fs::write(
            output.join(LIVE_REPAIR_COMPACTED_MARKER),
            serde_json::to_vec(&marker).unwrap(),
        )
        .unwrap();
        output
    }

    fn add_test_repair_block_access_with_first_hashes(
        output: &Path,
        epoch: u64,
        first_blockhash: [u8; 32],
        first_previous_blockhash: [u8; 32],
    ) {
        use blockzilla_format::{
            ArchiveV2BlockAccessIndexRow, ArchiveV2GetBlockIndexRow,
            write_archive_v2_block_access_index, write_archive_v2_get_block_index,
        };

        let start = epoch * SLOTS_PER_EPOCH;
        let mut access_bytes = Vec::new();
        let mut scratch = Vec::new();
        let mut access_rows = Vec::new();
        for block_id in 0..3u32 {
            scratch.clear();
            blockzilla_format::encode_with_scratch(
                &ArchiveV2BlockAccessBlob {
                    version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
                    flags: 0,
                    blockhash: if block_id == 0 {
                        first_blockhash
                    } else {
                        [block_id as u8; 32]
                    },
                    previous_blockhash: if block_id == 0 {
                        first_previous_blockhash
                    } else {
                        [7; 32]
                    },
                    signature_counts: Vec::new(),
                    signatures: Vec::new(),
                    pubkeys: Vec::new(),
                    blockhashes: Vec::new(),
                    vote_hashes: Vec::new(),
                },
                &mut scratch,
            )
            .unwrap();
            let access_len = u32::try_from(scratch.len()).unwrap();
            access_rows.push(ArchiveV2BlockAccessIndexRow {
                block_id,
                slot: start + u64::from(block_id),
                access_offset: access_bytes.len() as u64,
                access_len,
                tx_count: 0,
                signature_count: 0,
            });
            access_bytes.extend_from_slice(&scratch);
        }
        fs::write(output.join(BLOCK_ACCESS_FILE), &access_bytes).unwrap();
        write_archive_v2_block_access_index(
            &output.join(BLOCK_ACCESS_INDEX_FILE),
            access_bytes.len() as u64,
            0,
            &access_rows,
        )
        .unwrap();

        let mut get_block_rows =
            vec![ArchiveV2GetBlockIndexRow::missing(); SLOTS_PER_EPOCH as usize];
        for block_id in 0..3usize {
            let access = access_rows[block_id];
            get_block_rows[block_id] = ArchiveV2GetBlockIndexRow {
                block_offset: block_id as u64,
                block_len: 1,
                access_offset: access.access_offset,
                access_len: access.access_len,
            };
        }
        write_archive_v2_get_block_index(&output.join(GET_BLOCK_INDEX_FILE), &get_block_rows)
            .unwrap();

        assert!(
            start > 0,
            "test repaired access tail requires a prior epoch"
        );
        let mut previous_tail = vec![7; 32];
        previous_tail.extend_from_slice(&(start - 1).to_le_bytes());
        fs::write(output.join(PREVIOUS_BLOCKHASH_TAIL_FILE), previous_tail).unwrap();

        let marker_path = output.join(LIVE_REPAIR_COMPACTED_MARKER);
        let mut marker: Value = serde_json::from_slice(&fs::read(&marker_path).unwrap()).unwrap();
        marker["block_access_ready"] = serde_json::json!(true);
        marker["files"]["block_access"] = serde_json::json!(BLOCK_ACCESS_FILE);
        marker["files"]["block_access_index"] = serde_json::json!(BLOCK_ACCESS_INDEX_FILE);
        marker["files"]["get_block_index"] = serde_json::json!(GET_BLOCK_INDEX_FILE);
        marker["files"]["previous_blockhash_tail"] =
            serde_json::json!(PREVIOUS_BLOCKHASH_TAIL_FILE);
        fs::write(marker_path, serde_json::to_vec(&marker).unwrap()).unwrap();
    }

    fn add_test_repair_block_access(output: &Path, epoch: u64) {
        add_test_repair_block_access_with_first_hashes(output, epoch, [9; 32], [7; 32]);
    }

    #[test]
    fn atomic_repair_bundle_supersedes_only_closed_same_epoch_sources() {
        let root = temp_root("repair-bundle-supersession");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path = write_test_repair_bundle(
            &config,
            "epoch-1000-union-repair-view",
            1000,
            &["early", "late", "active"],
        );
        let bundle =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(bundle.state, LiveState::RepairRequired);
        assert_eq!(bundle.blocks_written, 3);
        assert_eq!(
            bundle.source_capture_ids,
            vec![
                "early".to_string(),
                "late".to_string(),
                "active".to_string()
            ]
        );

        let mut captures = vec![
            test_live_capture(&root, "early", 1000, LiveState::Blocked, 1),
            test_live_capture(&root, "late", 1001, LiveState::RepairGate, 2),
            test_live_capture(&root, "active", 1000, LiveState::Capturing, 3),
            bundle,
        ];
        apply_repair_supersession(&mut captures);
        assert_eq!(
            captures[0].superseded_by.as_deref(),
            Some("epoch-1000-union-repair-view")
        );
        assert_eq!(captures[1].superseded_by, None);
        assert_eq!(captures[2].superseded_by, None);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn validated_repair_compacted_archive_is_packaged_and_keeps_source_grouping() {
        let root = temp_root("repair-compacted-packaged");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-union-repair-view", 1000, &["source"]);
        write_test_repair_compacted(&config, 1000);

        let packaged =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(packaged.state, LiveState::Packaged);
        assert_eq!(packaged.progress.phase.as_deref(), Some("repair_compacted"));
        assert_eq!(packaged.progress.state.as_deref(), Some("packaged"));
        assert_eq!(packaged.source_capture_ids, vec!["source".to_string()]);
        assert!(
            packaged
                .message
                .as_deref()
                .unwrap()
                .contains("canonical PoH/shredding")
        );

        let mut captures = vec![
            test_live_capture(&root, "source", 1000, LiveState::Blocked, 1),
            packaged,
        ];
        apply_repair_supersession(&mut captures);
        assert_eq!(
            captures[0].superseded_by.as_deref(),
            Some("epoch-1000-union-repair-view")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repaired_block_access_archive_is_packaged_with_only_canonical_sidecars_incomplete() {
        let root = temp_root("repair-compacted-block-access");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-union-repair-view", 1000, &["source"]);
        let output = write_test_repair_compacted(&config, 1000);
        add_test_repair_block_access(&output, 1000);

        let packaged =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(packaged.state, LiveState::Packaged);
        let message = packaged.message.as_deref().unwrap();
        assert!(message.contains("only canonical PoH/shredding remain incomplete"));
        assert!(!message.contains("block-access sidecars remain incomplete"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repaired_block_access_provenance_hashes_must_match_tail_and_registry() {
        for mutation in ["tail", "multi_tail", "previous", "current"] {
            let root = temp_root(&format!("repair-compacted-hash-{mutation}"));
            let config = test_config(&root);
            fs::create_dir_all(&config.live_root).unwrap();
            let bundle_path = write_test_repair_bundle(
                &config,
                "epoch-1000-union-repair-view",
                1000,
                &["source"],
            );
            let output = write_test_repair_compacted(&config, 1000);
            add_test_repair_block_access(&output, 1000);
            let valid = classify_live_capture(
                &config,
                &RuntimeState::default(),
                bundle_path.clone(),
                unix_now(),
            );
            assert_eq!(valid.state, LiveState::Packaged, "valid {mutation}");

            let expected_message = match mutation {
                "tail" => {
                    let tail_path = output.join(PREVIOUS_BLOCKHASH_TAIL_FILE);
                    let mut tail = fs::read(&tail_path).unwrap();
                    tail[0] ^= 1;
                    fs::write(tail_path, tail).unwrap();
                    "previous blockhash differs"
                }
                "multi_tail" => {
                    let tail_path = output.join(PREVIOUS_BLOCKHASH_TAIL_FILE);
                    let tail = fs::read(&tail_path).unwrap();
                    let mut rows = vec![6; 32];
                    rows.extend_from_slice(&(1000 * SLOTS_PER_EPOCH - 2).to_le_bytes());
                    rows.extend_from_slice(&tail);
                    fs::write(tail_path, rows).unwrap();
                    "invalid byte length"
                }
                "previous" => {
                    add_test_repair_block_access_with_first_hashes(&output, 1000, [9; 32], [8; 32]);
                    "previous blockhash differs"
                }
                "current" => {
                    add_test_repair_block_access_with_first_hashes(&output, 1000, [8; 32], [7; 32]);
                    "blockhash differs from blockhash registry"
                }
                _ => unreachable!(),
            };
            let invalid =
                classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
            assert_eq!(
                invalid.state,
                LiveState::RepairRequired,
                "mutated {mutation}"
            );
            assert!(
                invalid
                    .message
                    .as_deref()
                    .unwrap()
                    .contains(expected_message),
                "{}",
                invalid.message.as_deref().unwrap()
            );
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn repaired_block_access_archive_rejects_each_missing_or_truncated_access_artifact() {
        for artifact in [
            BLOCK_ACCESS_FILE,
            BLOCK_ACCESS_INDEX_FILE,
            GET_BLOCK_INDEX_FILE,
            PREVIOUS_BLOCKHASH_TAIL_FILE,
        ] {
            let root = temp_root(&format!("repair-compacted-invalid-{artifact}"));
            let config = test_config(&root);
            fs::create_dir_all(&config.live_root).unwrap();
            let bundle_path = write_test_repair_bundle(
                &config,
                "epoch-1000-union-repair-view",
                1000,
                &["source"],
            );
            let output = write_test_repair_compacted(&config, 1000);
            add_test_repair_block_access(&output, 1000);

            let valid = classify_live_capture(
                &config,
                &RuntimeState::default(),
                bundle_path.clone(),
                unix_now(),
            );
            assert_eq!(valid.state, LiveState::Packaged, "valid {artifact}");

            let artifact_path = output.join(artifact);
            let original = fs::read(&artifact_path).unwrap();
            fs::remove_file(&artifact_path).unwrap();
            let missing = classify_live_capture(
                &config,
                &RuntimeState::default(),
                bundle_path.clone(),
                unix_now(),
            );
            assert_eq!(
                missing.state,
                LiveState::RepairRequired,
                "missing {artifact}"
            );

            fs::write(&artifact_path, &original).unwrap();
            let restored = classify_live_capture(
                &config,
                &RuntimeState::default(),
                bundle_path.clone(),
                unix_now(),
            );
            assert_eq!(restored.state, LiveState::Packaged, "restored {artifact}");

            fs::write(&artifact_path, &original[..original.len() - 1]).unwrap();
            let truncated =
                classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
            assert_eq!(
                truncated.state,
                LiveState::RepairRequired,
                "truncated {artifact}"
            );
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn legacy_repair_compacted_marker_rejects_undeclared_access_artifacts() {
        let root = temp_root("repair-compacted-legacy-access");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-union-repair-view", 1000, &["source"]);
        let output = write_test_repair_compacted(&config, 1000);
        fs::write(output.join(GET_BLOCK_INDEX_FILE), [0]).unwrap();

        let classified =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(classified.state, LiveState::RepairRequired);
        assert!(
            classified
                .message
                .as_deref()
                .unwrap()
                .contains("undeclared block-access artifact")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupt_or_canonical_repair_compacted_output_stays_repair_required() {
        let root = temp_root("repair-compacted-invalid");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-union-repair-view", 1000, &["source"]);
        let output = write_test_repair_compacted(&config, 1000);
        fs::write(output.join(BLOCKHASH_REGISTRY_FILE), [0]).unwrap();

        let corrupt = classify_live_capture(
            &config,
            &RuntimeState::default(),
            bundle_path.clone(),
            unix_now(),
        );
        assert_eq!(corrupt.state, LiveState::RepairRequired);
        assert_eq!(corrupt.source_capture_ids, vec!["source".to_string()]);
        assert!(
            corrupt
                .message
                .as_deref()
                .unwrap()
                .contains("blockhash registry byte length")
        );

        fs::write(output.join(BLOCKHASH_REGISTRY_FILE), [9; 96]).unwrap();
        fs::write(output.join(POH_FILE), b"canonical-sidecar").unwrap();
        let canonical =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(canonical.state, LiveState::RepairRequired);
        assert!(
            canonical
                .message
                .as_deref()
                .unwrap()
                .contains("forbidden canonical artifact")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn symlinked_repair_compacted_marker_fails_closed_without_losing_bundle_relationships() {
        let root = temp_root("repair-compacted-symlink");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-union-repair-view", 1000, &["source"]);
        let output = write_test_repair_compacted(&config, 1000);
        let marker = output.join(LIVE_REPAIR_COMPACTED_MARKER);
        let external = root.join("external-repair-compacted.json");
        fs::rename(&marker, &external).unwrap();
        std::os::unix::fs::symlink(&external, &marker).unwrap();

        let classified =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(classified.state, LiveState::RepairRequired);
        assert_eq!(classified.source_capture_ids, vec!["source".to_string()]);
        assert!(
            classified
                .message
                .as_deref()
                .unwrap()
                .contains("regular file")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repair_compacted_poh_gap_declaration_must_match_available_records() {
        let root = temp_root("repair-compacted-poh-gaps");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-union-repair-view", 1000, &["source"]);
        let output = write_test_repair_compacted(&config, 1000);
        let marker_path = output.join(LIVE_REPAIR_COMPACTED_MARKER);
        let mut marker: Value = serde_json::from_slice(&fs::read(&marker_path).unwrap()).unwrap();
        marker["poh_coverage"]["missing_record_ids"] = serde_json::json!([2]);
        fs::write(&marker_path, serde_json::to_vec(&marker).unwrap()).unwrap();

        let classified =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(classified.state, LiveState::RepairRequired);
        assert!(
            classified
                .message
                .as_deref()
                .unwrap()
                .contains("exact complement")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repair_process_matching_uses_effective_repair_binary_and_exact_arguments() {
        let root = temp_root("repair-process-argv");
        let mut config = test_config(&root);
        assert_eq!(
            effective_repair_blockzilla_bin(&config),
            config.blockzilla_bin.as_path()
        );
        let repair_bin = root.join("repair-blockzilla");
        config.repair_blockzilla_bin = Some(repair_bin.clone());
        assert_eq!(
            effective_repair_blockzilla_bin(&config),
            repair_bin.as_path()
        );

        let input = root.join("live/epoch-1000-repair");
        let output = repair_materialized_output(&config, 1000);
        let command = "materialize-archive-v2-live-repair";
        let argv = [
            repair_bin.as_os_str().as_bytes(),
            command.as_bytes(),
            input.as_os_str().as_bytes(),
            output.as_os_str().as_bytes(),
        ]
        .into_iter()
        .flat_map(|arg| arg.iter().copied().chain(std::iter::once(0)))
        .collect::<Vec<_>>();
        assert!(repair_process_argv_matches(
            &argv,
            &repair_bin,
            command,
            &input,
            &output
        ));
        assert!(!repair_process_argv_matches(
            &argv,
            &config.blockzilla_bin,
            command,
            &input,
            &output
        ));
        assert!(!repair_process_argv_matches(
            &argv,
            &repair_bin,
            "build-archive-v2-degraded-hot-blocks-from-repair",
            &input,
            &output
        ));
        let mut extra = argv.clone();
        extra.extend_from_slice(b"--progress-json\0/tmp/repair-progress.json\0");
        assert!(repair_process_argv_matches(
            &extra,
            &repair_bin,
            command,
            &input,
            &output
        ));
        let wrong_input = root.join("live/wrong-repair");
        assert!(!repair_process_argv_matches(
            &argv,
            &repair_bin,
            command,
            &wrong_input,
            &output
        ));
        let wrong_output = root.join("state/wrong-output");
        assert!(!repair_process_argv_matches(
            &argv,
            &repair_bin,
            command,
            &input,
            &wrong_output
        ));
    }

    #[test]
    fn invalid_or_oversized_repair_marker_never_supersedes_sources() {
        let root = temp_root("invalid-repair-bundle");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle = config.live_root.join("epoch-1000-invalid-repair");
        fs::create_dir_all(bundle.join("repair")).unwrap();
        fs::write(bundle.join(LIVE_REPAIR_PLAN_FILE), b"plan").unwrap();
        fs::write(bundle.join(LIVE_REPAIR_REQUIRED_MARKER), b"{truncated").unwrap();
        let invalid = classify_live_capture(
            &config,
            &RuntimeState::default(),
            bundle.clone(),
            unix_now(),
        );
        assert_eq!(invalid.state, LiveState::Blocked);
        assert!(invalid.source_capture_ids.is_empty());
        assert!(
            invalid
                .message
                .as_deref()
                .unwrap()
                .contains("invalid REPAIR-REQUIRED")
        );

        let marker = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(bundle.join(LIVE_REPAIR_REQUIRED_MARKER))
            .unwrap();
        marker.set_len(MAX_LIVE_REPAIR_MARKER_BYTES + 1).unwrap();
        let oversized =
            classify_live_capture(&config, &RuntimeState::default(), bundle, unix_now());
        assert_eq!(oversized.state, LiveState::Blocked);
        assert!(oversized.source_capture_ids.is_empty());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn symlinked_repair_merge_plan_fails_closed() {
        let root = temp_root("symlinked-repair-plan");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle =
            write_test_repair_bundle(&config, "epoch-1000-symlinked-repair", 1000, &["source"]);
        let plan = bundle.join(LIVE_REPAIR_PLAN_FILE);
        fs::remove_file(&plan).unwrap();
        let external = root.join("external-plan.jsonl");
        fs::write(&external, b"plan").unwrap();
        std::os::unix::fs::symlink(&external, &plan).unwrap();

        let classified =
            classify_live_capture(&config, &RuntimeState::default(), bundle, unix_now());
        assert_eq!(classified.state, LiveState::Blocked);
        assert!(classified.source_capture_ids.is_empty());
        assert!(
            classified
                .message
                .as_deref()
                .unwrap()
                .contains("merge plan")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn hidden_repair_staging_is_not_live_inventory() {
        let root = temp_root("repair-staging-inventory");
        let config = test_config(&root);
        for path in [&config.car_root, &config.archive_root, &config.live_root] {
            fs::create_dir_all(path).unwrap();
        }
        fs::create_dir_all(
            config
                .live_root
                .join(".epoch-1000-view.prepare-epoch-repair-123"),
        )
        .unwrap();
        fs::create_dir_all(config.live_root.join("epoch-1000-visible")).unwrap();
        let discovery = discover_inventory(&config);
        assert_eq!(
            discovery
                .live_paths
                .iter()
                .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
                .collect::<Vec<_>>(),
            vec!["epoch-1000-visible".to_string()]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn superseded_sources_are_not_queued_or_archive_eta_blockers() {
        let root = temp_root("repair-superseded-queue");
        let config = test_config(&root);
        let mut source = test_live_capture(&root, "source", 1000, LiveState::ReadyToPackage, 1);
        source.superseded_by = Some("repair".to_string());
        assert!(live_finalizer_queue_item(&config, &source).is_none());

        source.state = LiveState::Blocked;
        let mut repair = test_live_capture(&root, "repair", 1000, LiveState::RepairRequired, 2);
        repair.source_capture_ids = vec!["source".to_string()];
        let (eta, reason) = estimate_archive_eta(&[], &[source, repair]);
        assert_eq!(eta, None);
        let reason = reason.unwrap();
        assert!(reason.contains("finalization/compaction"));
        assert!(!reason.contains("live blocked"));
        fs::remove_dir_all(root).ok();
    }

    fn historical_queue_item(epoch: u64) -> FinalizerQueueItem {
        FinalizerQueueItem {
            kind: "historical".to_string(),
            epoch: Some(epoch),
            id: format!("epoch-{epoch}"),
            phase: "mphf_build".to_string(),
            state: "scan_ready".to_string(),
            estimated_memory_bytes: 1,
            estimated_disk_bytes: 1,
            deferred_reason: None,
        }
    }

    fn schedulable_snapshot(root: &Path, epochs: Vec<EpochSnapshot>) -> PipelineSnapshot {
        let mut snapshot = empty_snapshot(false);
        snapshot.inventory.complete = true;
        snapshot.inventory.generation = 1;
        snapshot.scan_sweep.generation = 1;
        snapshot.summary.scan_capacity_admitted = 4;
        snapshot.epochs = epochs;
        for epoch in &snapshot.epochs {
            if epoch.state == HistoricalState::ScanReady {
                snapshot
                    .finalizer_queue
                    .push(historical_queue_item(epoch.epoch));
            }
            if let Some(input) = epoch.input_path.as_deref() {
                fs::create_dir_all(input.parent().unwrap()).unwrap();
                fs::write(input, b"car").unwrap();
            }
        }
        fs::create_dir_all(root.join("archives")).unwrap();
        snapshot
    }

    fn write_live_candidate(output: &Path) {
        for name in [
            META_FILE,
            REGISTRY_FILE,
            REGISTRY_COUNTS_FILE,
            REGISTRY_INDEX_FILE,
            BLOCKHASH_REGISTRY_FILE,
            BLOCKS_FILE,
            BLOCK_INDEX_FILE,
            POH_FILE,
            SHREDDING_FILE,
        ] {
            fs::write(output.join(name), b"ok").unwrap();
        }
        for name in [SIGNATURES_FILE, VOTE_HASH_REGISTRY_FILE] {
            fs::write(output.join(name), b"").unwrap();
        }
    }

    #[test]
    fn classifies_queued_scan_ready_and_complete_from_filesystem_truth() {
        let root = temp_root("classify");
        let config = test_config(&root);
        fs::create_dir_all(&config.car_root).unwrap();
        fs::create_dir_all(&config.archive_root).unwrap();
        fs::write(config.car_root.join("epoch-700.car.zst"), b"car").unwrap();
        let mut runtime = RuntimeState::default();

        let queued = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(queued.state, HistoricalState::Queued);

        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        write_scan_marker(&output);
        let scan_ready = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(scan_ready.state, HistoricalState::ScanReady);
        assert_eq!(scan_ready.registry_order, RegistryOrder::Unknown);
        fs::write(output.join(REGISTRY_FILE), [0; 32]).unwrap();
        write_valid_first_seen_manifest(&output);
        let published_manifest = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(published_manifest.state, HistoricalState::ScanReady);
        assert_eq!(published_manifest.registry_order, RegistryOrder::FirstSeen);
        write_ownership(&output, "historical_scan", "700", "running", None).unwrap();
        set_ownership_pid(&output, Some(u32::MAX)).unwrap();
        runtime
            .failures
            .insert("scan:700".to_string(), "late child failure".to_string());
        let durable_marker_wins = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(durable_marker_wins.state, HistoricalState::ScanReady);

        fs::remove_file(output.join(SCAN_MARKER)).unwrap();
        runtime.failures.clear();
        set_ownership_pid(&output, None).unwrap();
        write_historical_candidate(&output, false);
        write_ownership(&output, "historical_scan", "700", "complete", None).unwrap();
        fs::remove_file(output.join(FIRST_SEEN_MANIFEST_FILE)).unwrap();
        let missing_manifest = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(missing_manifest.state, HistoricalState::Blocked);
        assert_eq!(missing_manifest.registry_order, RegistryOrder::Unknown);
        fs::write(
            output.join(FIRST_SEEN_MANIFEST_FILE),
            b"version=1\nregistry_order=usage_sorted\n",
        )
        .unwrap();
        let invalid_manifest = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(invalid_manifest.state, HistoricalState::Blocked);
        assert_eq!(invalid_manifest.registry_order, RegistryOrder::Unknown);
        write_valid_first_seen_manifest(&output);
        let complete = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(complete.state, HistoricalState::Complete);
        assert_eq!(complete.registry_order, RegistryOrder::FirstSeen);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn first_seen_manifest_parser_is_bounded_and_exact() {
        let root = temp_root("registry-order-manifest");
        fs::create_dir_all(&root).unwrap();
        let manifest = root.join(FIRST_SEEN_MANIFEST_FILE);

        fs::write(&manifest, b"registry_order=first_seen_v10\n").unwrap();
        assert!(!first_seen_manifest_declares_first_seen(&manifest));

        fs::write(&manifest, b"not_registry_order=first_seen_v1\n").unwrap();
        assert!(!first_seen_manifest_declares_first_seen(&manifest));

        fs::write(
            &manifest,
            b"version=1\nregistry_order=first_seen_v1\nregistry_order=usage_sorted\n",
        )
        .unwrap();
        assert!(!first_seen_manifest_declares_first_seen(&manifest));

        fs::write(
            &manifest,
            b"version=1\nregistry_order=first_seen_v1\nreferences=1\n",
        )
        .unwrap();
        assert!(first_seen_manifest_declares_first_seen(&manifest));

        let mut oversized = b"registry_order=first_seen_v1\n".to_vec();
        oversized.resize(MAX_FIRST_SEEN_MANIFEST_BYTES as usize + 1, b'x');
        fs::write(&manifest, oversized).unwrap();
        assert!(!first_seen_manifest_declares_first_seen(&manifest));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn complete_legacy_archive_requires_coherent_registry_index_for_usage_sorted() {
        let root = temp_root("registry-order-legacy");
        let config = test_config(&root);
        fs::create_dir_all(&config.archive_root).unwrap();
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        write_historical_candidate(&output, false);
        fs::remove_file(output.join(FIRST_SEEN_MANIFEST_FILE)).unwrap();

        let usage_sorted = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(usage_sorted.state, HistoricalState::Complete);
        assert_eq!(usage_sorted.registry_order, RegistryOrder::UsageSorted);

        let manifest_path = output.join(FIRST_SEEN_MANIFEST_FILE);
        fs::write(&manifest_path, b"version=1\nregistry_order=usage_sorted\n").unwrap();
        let invalid_manifest = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(invalid_manifest.state, HistoricalState::Complete);
        assert_eq!(invalid_manifest.registry_order, RegistryOrder::Unknown);

        fs::remove_file(&manifest_path).unwrap();
        fs::create_dir(&manifest_path).unwrap();
        let manifest_directory = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(manifest_directory.state, HistoricalState::Complete);
        assert_eq!(manifest_directory.registry_order, RegistryOrder::Unknown);
        fs::remove_dir(&manifest_path).unwrap();

        let mut oversized_manifest = b"registry_order=first_seen_v1\n".to_vec();
        oversized_manifest.resize(MAX_FIRST_SEEN_MANIFEST_BYTES as usize + 1, b'x');
        fs::write(&manifest_path, oversized_manifest).unwrap();
        let oversized_manifest = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(oversized_manifest.state, HistoricalState::Complete);
        assert_eq!(oversized_manifest.registry_order, RegistryOrder::Unknown);
        fs::remove_file(&manifest_path).unwrap();

        let index_path = output.join(REGISTRY_INDEX_FILE);
        let mut index = fs::read(&index_path).unwrap();
        index[12..20].copy_from_slice(&2u64.to_le_bytes());
        fs::write(&index_path, index).unwrap();
        let incoherent = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(incoherent.state, HistoricalState::Complete);
        assert_eq!(incoherent.registry_order, RegistryOrder::Unknown);

        fs::remove_file(output.join(REGISTRY_FILE)).unwrap();
        let missing_registry = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_ne!(missing_registry.state, HistoricalState::Complete);
        assert_eq!(missing_registry.registry_order, RegistryOrder::Unknown);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn historical_finalizer_adopts_unowned_output_and_preserves_existing_owner() {
        let root = temp_root("historical-finalizer-owner");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        write_scan_marker(&output);

        let mut adopted = spawn_historical_finalizer(&config, 700).await.unwrap();
        let owner = read_ownership(&output).unwrap();
        assert_eq!(owner.kind, "historical_finalizer");
        assert_eq!(owner.id, "700");
        assert_eq!(owner.state, "finalizing");
        adopted.child.wait().await.unwrap();

        write_ownership(&output, "historical_scan", "700", "scan_ready", None).unwrap();
        set_ownership_pid(&output, None).unwrap();
        let mut resumed = spawn_historical_finalizer(&config, 700).await.unwrap();
        let owner = read_ownership(&output).unwrap();
        assert_eq!(owner.kind, "historical_scan");
        assert_eq!(owner.id, "700");
        assert_eq!(owner.state, "finalizing");
        resumed.child.wait().await.unwrap();

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pipeline_owned_access_enabled_completion_requires_both_access_sidecars() {
        let root = temp_root("classify-access");
        let mut config = test_config(&root);
        config.no_access = false;
        fs::create_dir_all(&config.car_root).unwrap();
        fs::create_dir_all(&config.archive_root).unwrap();
        fs::write(config.car_root.join("epoch-700.car.zst"), b"car").unwrap();
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        write_historical_candidate(&output, false);
        write_ownership(&output, "historical_scan", "700", "complete", None).unwrap();
        let runtime = RuntimeState::default();

        let missing_access = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(missing_access.state, HistoricalState::Blocked);
        assert_eq!(
            missing_access.message.as_deref(),
            Some("reader core is complete but required block-access sidecars are missing or empty")
        );
        fs::write(output.join(BLOCK_ACCESS_FILE), b"ok").unwrap();
        let missing_access_index = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(missing_access_index.state, HistoricalState::Blocked);
        fs::write(output.join(BLOCK_ACCESS_INDEX_FILE), b"ok").unwrap();
        let complete = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(complete.state, HistoricalState::Complete);

        fs::remove_file(output.join(POH_FILE)).unwrap();
        let missing_reader_sidecar = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(missing_reader_sidecar.state, HistoricalState::Blocked);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unowned_legacy_no_access_archive_is_complete_without_source_car() {
        let root = temp_root("classify-legacy-no-access");
        let mut config = test_config(&root);
        config.no_access = false;
        fs::create_dir_all(&config.archive_root).unwrap();
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        write_historical_candidate(&output, false);
        fs::remove_file(output.join(FIRST_SEEN_MANIFEST_FILE)).unwrap();
        fs::remove_file(output.join(HOT_SEED_FILE)).unwrap();

        let legacy = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(legacy.state, HistoricalState::Complete);
        assert_eq!(legacy.registry_order, RegistryOrder::UsageSorted);
        assert_eq!(legacy.input_path, None);
        assert_eq!(
            legacy.message.as_deref(),
            Some(
                "accepted legacy no-access archive; both block-access sidecars were intentionally absent in the previous format"
            )
        );

        let car = legacy
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == ArtifactKind::Car)
            .unwrap();
        assert_eq!(car.state, ArtifactState::NotApplicable);
        assert!(!car.required_now);
        assert!(
            car.message
                .as_deref()
                .unwrap()
                .contains("deletion is expected")
        );
        for kind in [ArtifactKind::BlockAccess, ArtifactKind::BlockAccessIndex] {
            let artifact = legacy
                .artifacts
                .iter()
                .find(|artifact| artifact.kind == kind)
                .unwrap();
            assert_eq!(artifact.state, ArtifactState::NotApplicable);
            assert_eq!(artifact.requirement, ArtifactRequirement::Optional);
            assert!(!artifact.required_now);
            assert!(
                artifact
                    .message
                    .as_deref()
                    .unwrap()
                    .contains("accepted legacy no-access archive")
            );
        }

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unowned_reader_core_with_one_access_sidecar_remains_incomplete() {
        let root = temp_root("classify-legacy-partial-access");
        let mut config = test_config(&root);
        config.no_access = false;
        fs::create_dir_all(&config.archive_root).unwrap();
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        write_historical_candidate(&output, false);
        fs::write(output.join(BLOCK_ACCESS_FILE), b"partial").unwrap();

        let partial = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(partial.state, HistoricalState::Blocked);
        assert_eq!(
            partial.message.as_deref(),
            Some("reader core is complete but required block-access sidecars are missing or empty")
        );
        let access = partial
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == ArtifactKind::BlockAccess)
            .unwrap();
        let index = partial
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == ArtifactKind::BlockAccessIndex)
            .unwrap();
        assert_eq!(access.state, ArtifactState::Present);
        assert_eq!(index.state, ArtifactState::Missing);
        assert!(access.required_now);
        assert!(index.required_now);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unowned_registry_only_directory_is_not_accepted_as_legacy_complete() {
        let root = temp_root("classify-legacy-registry-only");
        let mut config = test_config(&root);
        config.no_access = false;
        fs::create_dir_all(&config.archive_root).unwrap();
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        for name in [REGISTRY_FILE, REGISTRY_COUNTS_FILE, REGISTRY_INDEX_FILE] {
            fs::write(output.join(name), b"partial").unwrap();
        }

        let registry_only = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(registry_only.state, HistoricalState::Blocked);
        assert_eq!(
            registry_only.message.as_deref(),
            Some("output exists without a complete reader core or scan-ready marker")
        );
        assert!(!historical_archive_strict_complete(&output, true, false));
        assert!(!legacy_no_access_archive_complete(&output, true, false));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_compact_output_is_packaged_but_not_historically_complete() {
        let root = temp_root("classify-live-packaged");
        let mut config = test_config(&root);
        config.no_access = false;
        fs::create_dir_all(&config.car_root).unwrap();
        fs::create_dir_all(&config.archive_root).unwrap();
        fs::create_dir_all(&config.live_root).unwrap();
        fs::write(config.car_root.join("epoch-700.car.zst"), b"car").unwrap();
        let capture = config.live_root.join("epoch-700-capture-test");
        fs::create_dir_all(&capture).unwrap();
        fs::write(capture.join(LIVE_FINALIZE_MARKER), b"closed").unwrap();
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        write_live_candidate(&output);
        let runtime = RuntimeState::default();

        let live = classify_live_capture(&config, &runtime, capture.clone(), unix_now());
        assert_eq!(live.state, LiveState::Packaged);
        let historical = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(historical.state, HistoricalState::Blocked);

        fs::remove_file(output.join(SIGNATURES_FILE)).unwrap();
        let incomplete_live = classify_live_capture(&config, &runtime, capture, unix_now());
        assert_ne!(incomplete_live.state, LiveState::Packaged);
        assert!(!live_archive_packaged(&output));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_capture_cannot_claim_an_epoch_owned_by_another_capture() {
        let root = temp_root("live-owner-collision");
        let mut config = test_config(&root);
        config.no_access = false;
        fs::create_dir_all(&config.archive_root).unwrap();
        fs::create_dir_all(&config.live_root).unwrap();
        let first = config.live_root.join("epoch-700-capture-first");
        let second = config.live_root.join("epoch-700-capture-second");
        for capture in [&first, &second] {
            fs::create_dir_all(capture).unwrap();
            fs::write(capture.join(LIVE_FINALIZE_MARKER), b"closed").unwrap();
            fs::write(capture.join(LIVE_READY_MARKER), b"ready").unwrap();
        }
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        write_live_candidate(&output);
        write_ownership(
            &output,
            "live_finalizer",
            "epoch-700-capture-first",
            "packaged",
            None,
        )
        .unwrap();

        let first_state =
            classify_live_capture(&config, &RuntimeState::default(), first, unix_now());
        let second_state =
            classify_live_capture(&config, &RuntimeState::default(), second, unix_now());
        assert_eq!(first_state.state, LiveState::Packaged);
        assert_eq!(second_state.state, LiveState::Blocked);
        assert!(
            second_state
                .message
                .as_deref()
                .unwrap()
                .contains("different pipeline item")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_reader_core_does_not_require_first_seen_manifest_or_hot_seed() {
        let root = temp_root("classify-legacy");
        let config = test_config(&root);
        fs::create_dir_all(&config.car_root).unwrap();
        fs::create_dir_all(&config.archive_root).unwrap();
        fs::write(config.car_root.join("epoch-700.car.zst"), b"car").unwrap();
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        write_historical_candidate(&output, false);
        fs::remove_file(output.join(FIRST_SEEN_MANIFEST_FILE)).unwrap();
        fs::remove_file(output.join("registry-hot-seed.bin")).unwrap();

        let legacy = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(legacy.state, HistoricalState::Complete);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn parses_blockzilla_progress_json_and_rejects_invalid_json() {
        let progress = parse_progress_bytes(
            br#"{"schema_version":1,"phase":"scan","state":"running","blocks_done":120,"transactions_done":900,"blocks_total_estimate":432000,"first_slot":302400000,"last_slot":302400120,"blocks_per_sec":20.5,"slots_per_sec":21.25,"eta_secs":12.0,"rss_bytes":1048576,"peak_rss_bytes":2097152,"updated_unix_secs":99}"#,
        )
        .unwrap();
        assert_eq!(progress.pid, None);
        assert_eq!(progress.blocks_done, 120);
        assert_eq!(progress.blocks_total, 432_000);
        assert_eq!(progress.blocks_per_sec, Some(20.5));
        assert_eq!(progress.slots_per_sec, Some(21.25));
        assert_eq!(progress.rss_bytes, Some(1_048_576));
        assert_eq!(progress.peak_rss_bytes, Some(2_097_152));
        assert!(parse_progress_bytes(b"not json").is_err());
    }

    #[test]
    fn live_rate_preserves_fresh_zero_and_hides_stale_values() {
        let now = 1_000;
        let mut fresh = parse_progress_bytes(
            br#"{"state":"capturing","first_slot":432000000,"last_slot":432000100,"elapsed_secs":10,"slots_per_sec":0,"eta_secs":99,"updated_unix_secs":1000}"#,
        )
        .unwrap();
        refresh_live_epoch_metrics(&mut fresh);
        hide_stale_live_rates(&mut fresh, now);
        assert_eq!(fresh.slots_per_sec, Some(0.0));
        assert_eq!(fresh.eta_secs, None);

        let mut stale = ProgressSnapshot {
            blocks_per_sec: Some(25.0),
            slots_per_sec: Some(30.0),
            eta_secs: Some(45.0),
            updated_unix_secs: Some(now - PROGRESS_STALE_SECS - 1),
            ..ProgressSnapshot::default()
        };
        hide_stale_live_rates(&mut stale, now);
        assert_eq!(stale.blocks_per_sec, None);
        assert_eq!(stale.slots_per_sec, None);
        assert_eq!(stale.eta_secs, None);
    }

    #[test]
    fn live_journal_fallback_computes_eta_from_pre_merge_progress_baseline() {
        let first_slot = 432_655_313;
        let baseline_slot = 432_663_030;
        let journal_slot = 432_728_503;
        let baseline_updated = 1_783_953_787;
        let journal_updated = 1_783_980_737;
        let mut progress = ProgressSnapshot {
            state: Some("capturing".to_string()),
            first_slot: Some(first_slot),
            last_slot: Some(baseline_slot),
            updated_unix_secs: Some(baseline_updated),
            ..ProgressSnapshot::default()
        };
        let journal = serde_json::json!({
            "slot": journal_slot,
            "block_id": 73_124
        });

        merge_live_journal_progress(&mut progress, Some(&journal), Some(journal_updated));
        refresh_live_epoch_metrics(&mut progress);

        let elapsed_secs = journal_updated - baseline_updated;
        let expected_rate = (journal_slot - baseline_slot) as f64 / elapsed_secs as f64;
        let remaining_slots = SLOTS_PER_EPOCH - (journal_slot % SLOTS_PER_EPOCH + 1);
        assert_eq!(progress.elapsed_secs, Some(elapsed_secs as f64));
        assert_eq!(progress.last_slot, Some(journal_slot));
        assert!((progress.slots_per_sec.unwrap() - expected_rate).abs() < 1e-9);
        assert!((progress.eta_secs.unwrap() - remaining_slots as f64 / expected_rate).abs() < 1e-6);
    }

    #[test]
    fn live_producer_argv_match_requires_exact_capture_target() {
        let capture = Path::new("/volume1/blockzilla-live/epoch-1001");
        let exact = b"/opt/bin/blockzilla-live-producer\0capture-grpc\0--endpoint\0https://example.invalid\0--archive-dir\0/volume1/blockzilla-live/epoch-1001\0";
        assert!(live_producer_argv_matches_bytes(exact, capture));

        let prefix_only = b"/opt/bin/blockzilla-live-producer\0capture-grpc\0--archive-dir\0/volume1/blockzilla-live/epoch-1001-old\0";
        assert!(!live_producer_argv_matches_bytes(prefix_only, capture));
        let wrong_command = b"/opt/bin/blockzilla-live-producer\0record-grpc-raw\0--archive-dir\0/volume1/blockzilla-live/epoch-1001\0";
        assert!(!live_producer_argv_matches_bytes(wrong_command, capture));
        let duplicate = b"/opt/bin/blockzilla-live-producer\0capture-grpc\0--archive-dir\0/volume1/blockzilla-live/epoch-1001\0--archive-dir=/volume1/blockzilla-live/epoch-1001\0";
        assert!(!live_producer_argv_matches_bytes(duplicate, capture));
        assert!(!live_producer_argv_matches_bytes(
            &exact[..exact.len() - 1],
            capture
        ));
        assert_eq!(unique_live_producer_pid([41]), Some(41));
        assert_eq!(unique_live_producer_pid([41, 42]), None);
    }

    #[test]
    fn live_producer_metrics_drop_an_unverified_pid() {
        let now = unix_now();
        let baseline = ProgressSnapshot {
            state: Some("capturing".to_string()),
            pid: Some(u32::MAX),
            blocks_per_sec: Some(10.0),
            slots_per_sec: Some(12.0),
            eta_secs: Some(30.0),
            rss_bytes: Some(123),
            peak_rss_bytes: Some(456),
            updated_unix_secs: Some(now),
            ..ProgressSnapshot::default()
        };
        let mut progress = merge_monitored_process_metrics(
            ProgressSnapshot {
                state: Some("capturing".to_string()),
                updated_unix_secs: Some(now),
                ..ProgressSnapshot::default()
            },
            &baseline,
        );
        assert_eq!(progress.pid, baseline.pid);
        refresh_live_producer_process_metrics(
            &mut progress,
            Path::new("/capture/that/no-process-owns"),
            now,
        );
        assert_eq!(progress.pid, None);
        assert_eq!(progress.rss_bytes, None);
        assert_eq!(progress.peak_rss_bytes, None);
        assert_eq!(progress.blocks_per_sec, None);
        assert_eq!(progress.slots_per_sec, None);
        assert_eq!(progress.eta_secs, None);
        assert!(progress_source_changed(&baseline, &progress));
    }

    #[test]
    fn filesystem_device_identity_is_stable_and_fails_closed() {
        let root = temp_root("filesystem-device");
        let archive = root.join("archive");
        let cars = root.join("cars");
        fs::create_dir_all(&archive).unwrap();
        fs::create_dir_all(&cars).unwrap();
        assert!(paths_share_filesystem_device(&archive, &cars));
        assert!(!paths_share_filesystem_device(
            &archive,
            &root.join("missing")
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_journal_monotonically_overrides_stale_progress_and_ignores_partial_tail() {
        let root = temp_root("live-journal-progress");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let capture = config.live_root.join("epoch-700-capture-active");
        fs::create_dir_all(capture.join("journal")).unwrap();
        let first_slot = 700 * SLOTS_PER_EPOCH + 10;
        let journal_slot = first_slot + 31;
        fs::write(
            capture.join("progress.json"),
            format!(
                "{{\"state\":\"capturing\",\"blocks_done\":10,\"first_slot\":{first_slot},\"last_slot\":{first_slot},\"progress_pct\":0.1,\"updated_unix_secs\":1}}"
            ),
        )
        .unwrap();
        fs::write(
            capture.join("journal/grpc-blocks.jsonl"),
            format!("{{\"slot\":{journal_slot},\"epoch\":700,\"block_id\":40}}\n{{\"slot\":"),
        )
        .unwrap();

        let classified =
            classify_live_capture(&config, &RuntimeState::default(), capture, unix_now());
        assert_eq!(classified.state, LiveState::Capturing);
        assert_eq!(classified.first_slot, Some(first_slot));
        assert_eq!(classified.last_slot, Some(journal_slot));
        assert_eq!(classified.blocks_written, 41);
        assert_eq!(classified.progress.last_slot, Some(journal_slot));
        assert_eq!(
            classified.progress.updated_unix_secs,
            Some(classified.updated_unix_secs)
        );
        assert!(classified.updated_unix_secs > 1);
        let expected_pct =
            ((journal_slot % SLOTS_PER_EPOCH + 1) as f64 / SLOTS_PER_EPOCH as f64) * 100.0;
        assert_eq!(classified.progress.progress_pct, Some(expected_pct));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_capture_selects_the_freshest_valid_progress_snapshot() {
        let root = temp_root("live-freshest-progress");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let capture = config.live_root.join("epoch-700-capture-active");
        fs::create_dir_all(capture.join("journal")).unwrap();
        let first_slot = 700 * SLOTS_PER_EPOCH + 10;
        fs::write(
            capture.join("progress.json"),
            format!(
                "{{\"phase\":\"stale root\",\"state\":\"capturing\",\"blocks_done\":10,\"first_slot\":{first_slot},\"last_slot\":{first_slot},\"updated_unix_secs\":1}}"
            ),
        )
        .unwrap();
        fs::write(
            capture.join("journal/progress.json"),
            format!(
                "{{\"phase\":\"fresh journal\",\"state\":\"capturing\",\"blocks_done\":20,\"first_slot\":{first_slot},\"last_slot\":{},\"updated_unix_secs\":{}}}",
                first_slot + 10,
                unix_now()
            ),
        )
        .unwrap();

        let classified =
            classify_live_capture(&config, &RuntimeState::default(), capture, unix_now());
        assert_eq!(classified.state, LiveState::Capturing);
        assert_eq!(classified.blocks_written, 20);
        assert_eq!(classified.last_slot, Some(first_slot + 10));
        assert_eq!(classified.progress.phase.as_deref(), Some("fresh journal"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fresh_journal_does_not_reopen_a_terminal_live_capture() {
        let root = temp_root("live-terminal-journal");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let capture = config.live_root.join("epoch-700-capture-closed");
        fs::create_dir_all(capture.join("journal")).unwrap();
        fs::write(capture.join(LIVE_FINALIZE_MARKER), b"closed").unwrap();
        fs::write(capture.join(LIVE_READY_MARKER), b"ready").unwrap();
        let first_slot = 700 * SLOTS_PER_EPOCH + 10;
        fs::write(
            capture.join("progress.json"),
            format!(
                "{{\"state\":\"closed\",\"blocks_done\":10,\"first_slot\":{first_slot},\"last_slot\":{first_slot},\"updated_unix_secs\":1}}"
            ),
        )
        .unwrap();
        fs::write(
            capture.join("journal/grpc-blocks.jsonl"),
            format!(
                "{{\"slot\":{},\"epoch\":700,\"block_id\":40}}\n",
                first_slot + 31
            ),
        )
        .unwrap();

        let classified =
            classify_live_capture(&config, &RuntimeState::default(), capture, unix_now());
        assert_eq!(classified.state, LiveState::ReadyToPackage);
        assert_eq!(classified.blocks_written, 41);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn snapshot_json_has_stable_monitoring_contract() {
        let mut snapshot = empty_snapshot(true);
        snapshot.sequence = 7;
        snapshot.summary.queued = 2;
        snapshot
            .epochs
            .push(test_epoch(Path::new("/tmp"), 700, HistoricalState::Queued));
        let mut live = test_live_capture(
            Path::new("/tmp"),
            "epoch-701-live",
            701,
            LiveState::Capturing,
            99,
        );
        live.eta_secs = Some(120.0);
        live.slots_per_sec = Some(2.5);
        live.rss_bytes = Some(128 * 1024 * 1024);
        live.peak_rss_bytes = Some(192 * 1024 * 1024);
        snapshot.live.push(live);
        let value = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(value["schema_version"], 3);
        assert_eq!(value["sequence"], 7);
        assert_eq!(value["observer_mode"], true);
        assert!(value.get("summary").is_some());
        assert!(value.get("inventory").is_some());
        assert!(value.get("scan_sweep").is_some());
        assert!(value.get("machine").is_some());
        assert!(value.get("epochs").is_some());
        assert!(value.get("lanes").is_some());
        assert!(value.get("live").is_some());
        assert!(value.get("finalizer_queue").is_some());
        assert!(value.get("errors").is_some());
        assert_eq!(value["epochs"][0]["registry_order"], "unknown");
        assert_eq!(value["live"][0]["eta_secs"], 120.0);
        assert_eq!(value["live"][0]["slots_per_sec"], 2.5);
        assert_eq!(value["live"][0]["rss_bytes"], 128 * 1024 * 1024);
        assert_eq!(value["live"][0]["peak_rss_bytes"], 192 * 1024 * 1024);
        assert_eq!(
            value["live"][0]["source_capture_ids"],
            serde_json::json!([])
        );
        assert_eq!(value["live"][0]["superseded_by"], Value::Null);

        let mut legacy_epoch = value["epochs"][0].clone();
        legacy_epoch
            .as_object_mut()
            .unwrap()
            .remove("registry_order");
        let legacy_epoch: EpochSnapshot = serde_json::from_value(legacy_epoch).unwrap();
        assert_eq!(legacy_epoch.registry_order, RegistryOrder::Unknown);
    }

    #[test]
    fn admission_caps_scans_by_memory_and_disk_reserve() {
        let root = temp_root("admission");
        let config = test_config(&root);
        let mut machine = MachineSnapshot {
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            memory_available_bytes: 2_000 * 1024 * 1024,
            disk_total_bytes: 2 * 1024 * 1024 * 1024 * 1024,
            disk_available_bytes: 500 * 1024 * 1024 * 1024,
            ..MachineSnapshot::default()
        };
        let admission = admission_snapshot(&config, &machine, &[]);
        assert_eq!(admission.scan_capacity, 2);
        assert!(admission.blocked_reason.is_none());

        machine.disk_available_bytes = 128 * 1024 * 1024 * 1024;
        let disk_blocked = admission_snapshot(&config, &machine, &[]);
        assert_eq!(disk_blocked.scan_capacity, 0);
        assert!(
            disk_blocked
                .blocked_reason
                .as_deref()
                .unwrap()
                .starts_with("disk admission blocked")
        );
    }

    #[test]
    fn finalizer_admission_reserves_memory_at_the_exact_boundary() {
        let root = temp_root("finalizer-admission");
        let config = test_config(&root);
        let task = FinalizerQueueItem {
            kind: "live".to_string(),
            epoch: Some(700),
            id: "capture".to_string(),
            phase: "registry_merge".to_string(),
            state: "ready_to_package".to_string(),
            estimated_memory_bytes: 512 * 1024 * 1024,
            estimated_disk_bytes: 1024 * 1024 * 1024,
            deferred_reason: None,
        };
        let required = (config.memory_reserve_mib + 512) * 1024 * 1024;
        let mut machine = MachineSnapshot {
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            memory_available_bytes: required,
            disk_total_bytes: 2 * 1024 * 1024 * 1024 * 1024,
            disk_available_bytes: 500 * 1024 * 1024 * 1024,
            ..MachineSnapshot::default()
        };
        assert!(finalizer_admission_blocked_reason(&config, &machine, &task).is_none());

        machine.memory_available_bytes -= 1;
        let reason = finalizer_admission_blocked_reason(&config, &machine, &task).unwrap();
        assert!(reason.starts_with("finalizer memory admission blocked"));
    }

    #[test]
    fn mphf_memory_estimate_uses_bounded_builder_envelope() {
        let root = temp_root("bounded-mphf-estimate");
        let config = test_config(&root);

        assert_eq!(
            estimate_mphf_build_bytes(&config, 1024 * 1024 * 1024),
            2 * 1024 * 1024 * 1024 + FINALIZER_BUILD_OVERHEAD_BYTES
        );
        assert_eq!(
            estimate_mphf_build_bytes(&config, 1024),
            finalizer_memory_floor_bytes(&config)
        );
    }

    #[test]
    fn finalizer_queue_is_blocked_only_when_no_non_deferred_task_fits() {
        let root = temp_root("finalizer-queue-admission");
        let config = test_config(&root);
        let machine = MachineSnapshot {
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            memory_available_bytes: 2 * 1024 * 1024 * 1024,
            disk_total_bytes: 2 * 1024 * 1024 * 1024 * 1024,
            disk_available_bytes: 500 * 1024 * 1024 * 1024,
            ..MachineSnapshot::default()
        };
        let mut queue = vec![historical_queue_item(305), historical_queue_item(405)];
        queue[0].estimated_memory_bytes = 4 * 1024 * 1024 * 1024;
        queue[1].estimated_memory_bytes = 512 * 1024 * 1024;

        assert_eq!(
            first_admissible_finalizer(&config, &machine, &queue).map(|task| task.epoch),
            Some(Some(405))
        );
        assert!(finalizer_queue_admission_blocked_reason(&config, &machine, &queue).is_none());

        queue[1].deferred_reason = Some("historical scan sweep in progress".to_string());
        assert!(first_admissible_finalizer(&config, &machine, &queue).is_none());
        assert!(
            finalizer_queue_admission_blocked_reason(&config, &machine, &queue)
                .unwrap()
                .starts_with("finalizer memory admission blocked")
        );
    }

    #[test]
    fn live_finalizer_queue_advances_through_durable_stages() {
        let root = temp_root("live-finalizer-stages");
        let config = test_config(&root);
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        let capture = LiveCaptureSnapshot {
            id: "epoch-700-capture-test".to_string(),
            epoch: Some(700),
            is_current: false,
            state: LiveState::ReadyToPackage,
            capture_dir: config.live_root.join("epoch-700-capture-test"),
            output_path: Some(output.clone()),
            ready_to_package: true,
            repair_gate: false,
            source_capture_ids: Vec::new(),
            superseded_by: None,
            first_slot: Some(700 * SLOTS_PER_EPOCH),
            last_slot: Some(700 * SLOTS_PER_EPOCH + 1),
            blocks_written: 2,
            artifacts: Vec::new(),
            progress: ProgressSnapshot::default(),
            eta_secs: None,
            slots_per_sec: None,
            rss_bytes: None,
            peak_rss_bytes: None,
            message: None,
            updated_unix_secs: 0,
        };

        let registry = live_finalizer_queue_item(&config, &capture).unwrap();
        assert_eq!(registry.phase, "registry_merge");
        assert_eq!(registry.estimated_memory_bytes, 512 * 1024 * 1024);

        fs::write(output.join(REGISTRY_FILE), vec![0; 64]).unwrap();
        fs::write(output.join(REGISTRY_COUNTS_FILE), [1]).unwrap();
        fs::write(output.join(LIVE_REGISTRY_READY_MARKER), b"ready").unwrap();
        let mphf = live_finalizer_queue_item(&config, &capture).unwrap();
        assert_eq!(mphf.phase, "mphf_build");
        assert!(mphf.estimated_memory_bytes >= 512 * 1024 * 1024);

        fs::write(output.join(REGISTRY_INDEX_FILE), vec![0; 128]).unwrap();
        let rewrite = live_finalizer_queue_item(&config, &capture).unwrap();
        assert_eq!(rewrite.phase, "hot_rewrite");
        assert!(rewrite.estimated_memory_bytes >= 512 * 1024 * 1024);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn active_live_capture_cannot_be_queued_by_a_ready_marker() {
        let root = temp_root("live-ready-active");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let capture = config.live_root.join("epoch-700-capture-active");
        fs::create_dir_all(capture.join("journal")).unwrap();
        fs::write(capture.join(LIVE_FINALIZE_MARKER), b"closed").unwrap();
        fs::write(capture.join(LIVE_READY_MARKER), b"ready").unwrap();
        fs::write(
            capture.join("journal/progress.json"),
            format!(
                "{{\"state\":\"running\",\"last_slot\":{},\"updated_unix_secs\":{}}}",
                700 * SLOTS_PER_EPOCH,
                unix_now()
            ),
        )
        .unwrap();

        let classified =
            classify_live_capture(&config, &RuntimeState::default(), capture, unix_now());
        assert_eq!(classified.state, LiveState::Capturing);
        assert!(classified.message.as_deref().unwrap().contains("ignored"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn live_epochs_have_one_current_capture_and_do_not_become_historical_gaps() {
        let root = temp_root("live-unified-census");
        let mut config = test_config(&root);
        config.start_epoch = Some(997);
        // Historical backfill stops at 999; the retained/live captures must
        // still extend the operator timeline through the current epoch.
        config.end_epoch = Some(999);
        for path in [
            &config.car_root,
            &config.archive_root,
            &config.live_root,
            &config.state_root,
        ] {
            fs::create_dir_all(path).unwrap();
        }
        for epoch in [998, 999] {
            let output = config.archive_root.join(format!("epoch-{epoch}"));
            fs::create_dir_all(&output).unwrap();
            write_historical_candidate(&output, false);
        }

        let closed = config.live_root.join("epoch-1000-capture-closed");
        fs::create_dir_all(closed.join("journal")).unwrap();
        fs::write(closed.join(LIVE_FINALIZE_MARKER), b"closed").unwrap();
        fs::write(
            closed.join("journal/progress.json"),
            format!(
                "{{\"state\":\"closed\",\"first_slot\":{},\"last_slot\":{},\"updated_unix_secs\":{}}}",
                1000 * SLOTS_PER_EPOCH,
                1001 * SLOTS_PER_EPOCH + 7,
                unix_now()
            ),
        )
        .unwrap();

        let active = config.live_root.join("epoch-1001-capture-active");
        fs::create_dir_all(active.join("journal")).unwrap();
        fs::write(
            active.join("journal/progress.json"),
            format!(
                "{{\"state\":\"capturing\",\"first_slot\":{},\"last_slot\":{},\"updated_unix_secs\":{}}}",
                1001 * SLOTS_PER_EPOCH,
                1001 * SLOTS_PER_EPOCH + 9,
                unix_now()
            ),
        )
        .unwrap();

        let snapshot = reconcile_filesystem(&config, &RuntimeState::default(), 1);
        assert_eq!(snapshot.current_epoch, Some(1001));
        assert_eq!(snapshot.inventory.epochs_discovered, 5);
        assert_eq!(snapshot.inventory.epochs_classified, 5);
        assert_eq!(
            snapshot
                .epochs
                .iter()
                .map(|epoch| epoch.epoch)
                .collect::<Vec<_>>(),
            vec![997, 998, 999]
        );
        assert_eq!(snapshot.summary.blocked, 1);

        let closed = snapshot
            .live
            .iter()
            .find(|capture| capture.id == "epoch-1000-capture-closed")
            .unwrap();
        assert_eq!(closed.epoch, Some(1000));
        assert_eq!(closed.state, LiveState::RepairGate);
        assert!(!closed.is_current);
        let active = snapshot
            .live
            .iter()
            .find(|capture| capture.id == "epoch-1001-capture-active")
            .unwrap();
        assert_eq!(active.state, LiveState::Capturing);
        assert!(active.is_current);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn canonical_live_capture_preserves_closed_compaction_backlog() {
        let root = temp_root("canonical-live");
        let captures = vec![
            test_live_capture(&root, "epoch-1000-waiting", 1000, LiveState::RepairGate, 30),
            test_live_capture(
                &root,
                "epoch-1001-old-producer",
                1001,
                LiveState::Capturing,
                10,
            ),
            test_live_capture(&root, "epoch-1001-current", 1001, LiveState::Capturing, 20),
        ];
        let (captures, current_epoch) = canonicalize_live_captures(captures);
        assert_eq!(current_epoch, Some(1001));
        assert_eq!(
            captures
                .iter()
                .filter(|capture| capture.state == LiveState::Capturing)
                .count(),
            1
        );
        assert!(
            captures
                .iter()
                .find(|capture| capture.id == "epoch-1001-current")
                .unwrap()
                .is_current
        );
        assert_eq!(
            captures
                .iter()
                .find(|capture| capture.id == "epoch-1000-waiting")
                .unwrap()
                .state,
            LiveState::RepairGate
        );
        let duplicate = captures
            .iter()
            .find(|capture| capture.id == "epoch-1001-old-producer")
            .unwrap();
        assert_eq!(duplicate.state, LiveState::Blocked);
        assert!(
            duplicate
                .message
                .as_deref()
                .unwrap()
                .contains("not canonical")
        );
    }

    #[test]
    fn full_archive_eta_is_unknown_until_all_required_phases_complete() {
        let root = temp_root("archive-eta");
        let blocked = vec![test_epoch(&root, 997, HistoricalState::Blocked)];
        let (eta, reason) = estimate_archive_eta(&blocked, &[]);
        assert_eq!(eta, None);
        assert!(reason.as_deref().unwrap().contains("historical blocked=1"));

        let scan_ready = vec![test_epoch(&root, 998, HistoricalState::ScanReady)];
        let (eta, reason) = estimate_archive_eta(&scan_ready, &[]);
        assert_eq!(eta, None);
        assert!(
            reason
                .as_deref()
                .unwrap()
                .contains("finalization/compaction")
        );

        let complete = vec![test_epoch(&root, 999, HistoricalState::Complete)];
        let live_complete = vec![test_live_capture(
            &root,
            "epoch-1000-complete",
            1000,
            LiveState::Complete,
            0,
        )];
        assert_eq!(
            estimate_archive_eta(&complete, &live_complete),
            (Some(0.0), None)
        );
    }

    #[test]
    fn scan_sweep_distinguishes_healthy_waiting_from_resource_blocking() {
        let root = temp_root("sweep-wait-vs-blocked");
        let mut config = test_config(&root);
        config.start_epoch = Some(700);
        config.end_epoch = Some(701);
        config.disk_reserve_gib = 0;
        config.memory_reserve_mib = 0;
        config.scan_memory_mib = 1;
        for path in [
            &config.car_root,
            &config.archive_root,
            &config.live_root,
            &config.state_root,
        ] {
            fs::create_dir_all(path).unwrap();
        }
        fs::write(config.car_root.join("epoch-700.car"), b"car").unwrap();
        let scan_ready = config.archive_root.join("epoch-701");
        fs::create_dir_all(&scan_ready).unwrap();
        write_scan_marker(&scan_ready);

        let waiting = reconcile_filesystem(&config, &RuntimeState::default(), 1);
        assert!(waiting.scan_sweep.wait_reason.is_some());
        assert!(waiting.scan_sweep.blocked_reason.is_none());
        assert!(waiting.summary.finalizer_wait_reason.is_some());
        assert!(waiting.summary.finalizer_admission_blocked_reason.is_none());
        assert!(
            waiting.finalizer_queue[0]
                .deferred_reason
                .as_deref()
                .unwrap()
                .contains("sweep in progress")
        );

        config.disk_reserve_gib = u64::MAX;
        let blocked = reconcile_filesystem(&config, &RuntimeState::default(), 2);
        assert!(blocked.scan_sweep.wait_reason.is_none());
        assert!(blocked.scan_sweep.blocked_reason.is_some());
        assert!(blocked.summary.finalizer_wait_reason.is_none());
        assert!(
            blocked
                .summary
                .finalizer_admission_blocked_reason
                .as_deref()
                .unwrap()
                .contains("disk admission blocked")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn managed_scan_remains_counted_after_publishing_scan_marker() {
        let epoch = |epoch, state| EpochSnapshot {
            epoch,
            state,
            registry_order: RegistryOrder::Unknown,
            input_path: None,
            output_path: PathBuf::from(format!("/archives/epoch-{epoch}")),
            car_bytes: 0,
            artifacts: Vec::new(),
            progress: ProgressSnapshot::default(),
            message: None,
            updated_unix_secs: 0,
        };
        let epochs = vec![
            epoch(700, HistoricalState::ScanReady),
            epoch(701, HistoricalState::Scanning),
        ];
        assert_eq!(active_scan_count(&epochs, [700, 702]), 3);
    }

    #[test]
    fn acquisition_action_precedes_new_historical_scan() {
        let root = temp_root("acquisition-action");
        let mut config = test_config(&root);
        config.preflight_car = true;
        let mut epoch = EpochSnapshot {
            epoch: 700,
            state: HistoricalState::Queued,
            registry_order: RegistryOrder::Unknown,
            input_path: Some(config.car_root.join("epoch-700.car")),
            output_path: config.archive_root.join("epoch-700"),
            car_bytes: 1,
            artifacts: Vec::new(),
            progress: ProgressSnapshot::default(),
            message: None,
            updated_unix_secs: 0,
        };
        assert_eq!(
            acquisition_action(&config, &epoch),
            Some(AcquisitionAction::Preflight)
        );
        epoch.input_path = None;
        config.car_source_url_template = Some("https://example.invalid/epoch-{epoch}.car".into());
        assert_eq!(
            acquisition_action(&config, &epoch),
            Some(AcquisitionAction::Download)
        );
    }

    #[tokio::test]
    async fn historical_finalizer_waits_for_queued_scan_and_scan_lanes_refill() {
        let root = temp_root("scan-sweep-order");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        config.scan_concurrency = 2;
        let mut snapshot = schedulable_snapshot(
            &root,
            vec![
                test_epoch(&root, 700, HistoricalState::Queued),
                test_epoch(&root, 701, HistoricalState::ScanReady),
            ],
        );
        snapshot.summary.scan_capacity_admitted = 2;
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.finalizer.is_none());
        assert!(runtime.scans.contains_key(&700));

        snapshot.epochs[0].state = HistoricalState::Scanning;
        snapshot
            .epochs
            .push(test_epoch(&root, 702, HistoricalState::Queued));
        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.finalizer.is_none());
        assert!(runtime.scans.contains_key(&702));
        for child in runtime.scans.values_mut() {
            let _ = child.child.wait().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn memory_blocked_queue_head_does_not_starve_fitting_finalizer() {
        let root = temp_root("finalizer-head-of-line");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        let mut snapshot = schedulable_snapshot(
            &root,
            vec![
                test_epoch(&root, 305, HistoricalState::ScanReady),
                test_epoch(&root, 405, HistoricalState::ScanReady),
            ],
        );
        snapshot.machine = MachineSnapshot {
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            memory_available_bytes: 2 * 1024 * 1024 * 1024,
            disk_total_bytes: 2 * 1024 * 1024 * 1024 * 1024,
            disk_available_bytes: 500 * 1024 * 1024 * 1024,
            ..MachineSnapshot::default()
        };
        snapshot.finalizer_queue[0].estimated_memory_bytes = 4 * 1024 * 1024 * 1024;
        snapshot.finalizer_queue[1].estimated_memory_bytes = 512 * 1024 * 1024;
        for epoch in &snapshot.epochs {
            fs::create_dir_all(&epoch.output_path).unwrap();
            write_scan_marker(&epoch.output_path);
        }
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(matches!(
            runtime.finalizer.as_ref().map(|child| &child.kind),
            Some(ChildKind::HistoricalFinalizer { epoch: 405 })
        ));
        let _ = runtime.finalizer.as_mut().unwrap().child.wait().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn acquisition_phase_blocks_scans_and_incomplete_inventory_blocks_everything() {
        let root = temp_root("acquisition-barrier");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        config.preflight_car = true;
        let mut snapshot = schedulable_snapshot(
            &root,
            vec![
                test_epoch(&root, 700, HistoricalState::Queued),
                test_epoch(&root, 701, HistoricalState::Queued),
            ],
        );
        let mut runtime = RuntimeState::default();
        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert_eq!(runtime.acquisitions.len(), 1);
        assert!(runtime.scans.is_empty());
        let _ = runtime
            .acquisitions
            .get_mut(&700)
            .unwrap()
            .child
            .wait()
            .await;

        runtime.acquisitions.clear();
        snapshot.inventory.complete = false;
        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.acquisitions.is_empty());
        assert!(runtime.scans.is_empty());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pipeline_and_acquisition_locks_are_exclusive() {
        let root = temp_root("exclusive-locks");
        let config = test_config(&root);
        fs::create_dir_all(&config.state_root).unwrap();

        let pipeline = acquire_pipeline_lock(&config.state_root).unwrap();
        assert!(acquire_pipeline_lock(&config.state_root).is_err());
        drop(pipeline);
        drop(acquire_pipeline_lock(&config.state_root).unwrap());

        let acquisition = try_acquire_acquisition_lock(&config, 700).unwrap().unwrap();
        assert!(
            try_acquire_acquisition_lock(&config, 700)
                .unwrap()
                .is_none()
        );
        drop(acquisition);
        assert!(
            try_acquire_acquisition_lock(&config, 700)
                .unwrap()
                .is_some()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stale_acquisition_claim_becomes_durable_failure_after_lock_releases() {
        let root = temp_root("durable-acquisition-failure");
        let config = test_config(&root);
        fs::create_dir_all(&config.state_root).unwrap();
        let expected = config.car_root.join("epoch-700.car");
        let receipt = car_preflight_receipt_path(&config.state_root, 700);
        let lock = try_acquire_acquisition_lock(&config, 700).unwrap().unwrap();
        write_acquisition_marker(&config, 700, "car_download", 0, &expected, &receipt).unwrap();

        let mut runtime = RuntimeState::default();
        reconcile_acquisition_state(&config, &mut runtime);
        assert!(runtime.failures.is_empty());

        drop(lock);
        reconcile_acquisition_state(&config, &mut runtime);
        assert!(runtime.failures.contains_key("download:700"));
        assert!(acquisition_marker_path(&config.state_root, 700).is_file());
        let persisted: PersistedAcquisitionFailures = serde_json::from_slice(
            &fs::read(config.state_root.join(ACQUISITION_FAILURES_FILE)).unwrap(),
        )
        .unwrap();
        assert!(persisted.failures.contains_key("download:700"));
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn blocked_live_ready_task_does_not_starve_acquisition() {
        let root = temp_root("blocked-live-census");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        config.preflight_car = true;
        let mut snapshot =
            schedulable_snapshot(&root, vec![test_epoch(&root, 700, HistoricalState::Queued)]);
        snapshot.machine = MachineSnapshot {
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            memory_available_bytes: 3 * 1024 * 1024 * 1024,
            disk_total_bytes: 2 * 1024 * 1024 * 1024 * 1024,
            disk_available_bytes: 500 * 1024 * 1024 * 1024,
            car_disk_total_bytes: 2 * 1024 * 1024 * 1024 * 1024,
            car_disk_available_bytes: 500 * 1024 * 1024 * 1024,
            ..MachineSnapshot::default()
        };
        snapshot.finalizer_queue.insert(
            0,
            FinalizerQueueItem {
                kind: "live".to_string(),
                epoch: Some(700),
                id: "ready-capture".to_string(),
                phase: "registry_merge".to_string(),
                state: "ready_to_package".to_string(),
                estimated_memory_bytes: 4 * 1024 * 1024 * 1024,
                estimated_disk_bytes: 1024 * 1024 * 1024,
                deferred_reason: None,
            },
        );
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.finalizer.is_none());
        assert!(runtime.acquisitions.contains_key(&700));
        let _ = runtime
            .acquisitions
            .get_mut(&700)
            .unwrap()
            .child
            .wait()
            .await;
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn acquisition_memory_reserves_full_future_growth_of_active_lanes() {
        let root = temp_root("acquisition-memory-growth");
        let mut config = test_config(&root);
        config.download_concurrency = 2;
        let budget = PREFLIGHT_MEMORY_MIB * 1024 * 1024;
        let reserve = config.memory_reserve_mib * 1024 * 1024;
        let mut machine = MachineSnapshot {
            memory_total_bytes: 16 * 1024 * 1024 * 1024,
            memory_available_bytes: reserve + budget,
            ..MachineSnapshot::default()
        };
        assert_eq!(acquisition_memory_capacity(&config, &machine, 1, 0), 0);
        machine.memory_available_bytes = reserve + 2 * budget;
        assert_eq!(acquisition_memory_capacity(&config, &machine, 1, 0), 1);
    }

    #[test]
    fn dual_car_suffixes_are_blocked_instead_of_preferred() {
        let root = temp_root("dual-car-suffix");
        let config = test_config(&root);
        fs::create_dir_all(&config.car_root).unwrap();
        fs::create_dir_all(&config.archive_root).unwrap();
        fs::write(config.car_root.join("epoch-700.car"), b"raw").unwrap();
        fs::write(config.car_root.join("epoch-700.car.zst"), b"compressed").unwrap();
        assert!(car_path(&config.car_root, 700).is_none());
        let epoch = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(epoch.state, HistoricalState::Blocked);
        assert!(epoch.message.as_deref().unwrap().contains("both epoch-700"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn discovery_is_exact_and_reports_unreadable_roots() {
        let root = temp_root("inventory-errors");
        let mut config = test_config(&root);
        config.start_epoch = None;
        config.end_epoch = None;
        fs::create_dir_all(&config.car_root).unwrap();
        fs::create_dir_all(&config.archive_root).unwrap();
        fs::create_dir_all(&config.live_root).unwrap();
        fs::write(config.car_root.join("epoch-700.car.zst.part"), b"partial").unwrap();
        fs::write(config.car_root.join("epoch-701.car"), b"car").unwrap();
        fs::create_dir_all(config.archive_root.join("epoch-702")).unwrap();

        let discovery = discover_inventory(&config);
        assert!(discovery.errors.is_empty());
        assert_eq!(
            discovery.epochs.into_iter().collect::<Vec<_>>(),
            vec![701, 702]
        );

        fs::remove_dir_all(&config.live_root).unwrap();
        let failed = discover_inventory(&config);
        assert!(!failed.errors.is_empty());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn preflight_receipt_requires_matching_size_mtime_and_structural_facts() {
        let root = temp_root("preflight-receipt");
        let config = test_config(&root);
        fs::create_dir_all(&config.car_root).unwrap();
        let input = config.car_root.join("epoch-700.car");
        fs::write(&input, b"car").unwrap();
        let metadata = fs::metadata(&input).unwrap();
        let modified = metadata
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap();
        let receipt = car_preflight_receipt_path(&config.state_root, 700);
        fs::create_dir_all(receipt.parent().unwrap()).unwrap();
        fs::write(
            &receipt,
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "validation_level": "structural",
                "structurally_valid": true,
                "clean_eof": true,
                "eligible_for_compaction": true,
                "epoch": 700,
                "source_path": input,
                "source_bytes": metadata.len(),
                "source_modified_unix_secs": modified.as_secs(),
                "source_modified_subsec_nanos": modified.subsec_nanos(),
                "compressed": false,
                "io_buffer_bytes": 8 * 1024 * 1024,
                "decompressed_car_bytes": metadata.len(),
                "blocks": 1,
                "blocks_in_epoch": 1,
                "present_slots": 1,
                "duplicate_slots": 0,
                "out_of_epoch_blocks": 0,
                "non_monotonic_slots": 0,
                "transactions": 1,
                "first_slot": 700 * SLOTS_PER_EPOCH,
                "last_slot": 700 * SLOTS_PER_EPOCH,
                "poh": {
                    "records": 1,
                    "blocks_with_entries": 1,
                    "blocks_without_entries": 0,
                    "entries": 1,
                    "transaction_references": 1,
                    "num_hashes": "1"
                },
                "shredding": {
                    "records": 1,
                    "blocks_with_spans": 1,
                    "blocks_without_spans": 0,
                    "spans": 1
                },
                "started_unix_secs": 1,
                "completed_unix_secs": 2,
                "elapsed_secs": 1.0
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(receipt_matches_source(700, &input, &receipt));
        let valid_receipt = fs::read(&receipt).unwrap();
        let mut invalid_numeric: Value = serde_json::from_slice(&valid_receipt).unwrap();
        invalid_numeric["poh"]["entries"] = serde_json::json!("1");
        fs::write(&receipt, serde_json::to_vec(&invalid_numeric).unwrap()).unwrap();
        assert!(!receipt_matches_source(700, &input, &receipt));
        fs::write(&receipt, valid_receipt).unwrap();
        fs::write(&input, b"car-changed").unwrap();
        assert!(!receipt_matches_source(700, &input, &receipt));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn durable_acquisition_marker_adopts_surviving_process_group() {
        let root = temp_root("acquisition-adoption");
        let config = test_config(&root);
        let expected = config.car_root.join("epoch-700.car");
        let receipt = car_preflight_receipt_path(&config.state_root, 700);
        let mut child = Command::new("/bin/sh");
        child
            .args([
                std::ffi::OsString::from("-c"),
                std::ffi::OsString::from("sleep 30 & wait"),
                std::ffi::OsString::from("hivezilla-car-download"),
                expected.clone().into_os_string(),
            ])
            .process_group(0);
        let mut child = child.spawn().unwrap();
        let pid = child.id().unwrap();
        write_acquisition_marker(&config, 700, "car_download", pid, &expected, &receipt).unwrap();
        assert_eq!(active_acquisition_marker(&config, 700).unwrap().pid, pid);
        // SAFETY: this test created pid as a dedicated process-group leader.
        let _ = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
        let _ = child.wait().await;
        assert!(active_acquisition_marker(&config, 700).is_none());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn artifact_states_preserve_empty_valid_files_and_reject_bad_marker() {
        let root = temp_root("artifact-states");
        let config = test_config(&root);
        fs::create_dir_all(&config.car_root).unwrap();
        fs::create_dir_all(&config.archive_root).unwrap();
        fs::write(config.car_root.join("epoch-700.car"), b"car").unwrap();
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        fs::write(output.join(SCAN_MARKER), b"bad marker").unwrap();
        let classified = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(classified.state, HistoricalState::Blocked);
        assert_eq!(
            classified
                .artifacts
                .iter()
                .find(|artifact| artifact.kind == ArtifactKind::ScanMarker)
                .unwrap()
                .state,
            ArtifactState::Invalid
        );

        fs::remove_file(output.join(SCAN_MARKER)).unwrap();
        write_historical_candidate(&output, false);
        let complete = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        for kind in [ArtifactKind::Signatures, ArtifactKind::VoteHashRegistry] {
            assert_eq!(
                complete
                    .artifacts
                    .iter()
                    .find(|artifact| artifact.kind == kind)
                    .unwrap()
                    .state,
                ArtifactState::Present
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_state_requires_pipeline_ownership() {
        let root = temp_root("ownership");
        let config = test_config(&root);
        fs::create_dir_all(&config.car_root).unwrap();
        fs::create_dir_all(&config.archive_root).unwrap();
        fs::write(config.car_root.join("epoch-700.car.zst"), b"car").unwrap();
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        fs::write(output.join("partial"), b"partial").unwrap();
        let runtime = RuntimeState::default();
        let unowned = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(unowned.state, HistoricalState::Blocked);

        write_ownership(
            &output,
            "historical_scan",
            "700",
            "failed",
            Some("simulated failure".to_string()),
        )
        .unwrap();
        let owned = classify_epoch(&config, &runtime, 700, unix_now());
        assert_eq!(owned.state, HistoricalState::Failed);
        assert_eq!(owned.message.as_deref(), Some("simulated failure"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn controls_require_execute_mode_and_matching_bearer() {
        let root = temp_root("auth");
        let mut config = test_config(&root);
        let empty = HeaderMap::new();
        assert!(matches!(
            authorize_control(&config, &empty),
            Err(ControlError::Disabled(_))
        ));

        config.execute = true;
        config.control_token = Some("secret".to_string());
        assert!(matches!(
            authorize_control(&config, &empty),
            Err(ControlError::Unauthorized)
        ));
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer secret".parse().unwrap(),
        );
        assert!(authorize_control(&config, &headers).is_ok());
    }

    #[test]
    fn process_identity_routes_scanners_and_finalizers_by_exact_argv() {
        let bin = Path::new("/opt/blockzilla/bin/blockzilla");
        let output = Path::new("/archives/epoch-700");
        let scanner = b"/opt/blockzilla/bin/blockzilla\0build-archive-v2-hot-blocks\0/cars/epoch-700.car.zst\0/archives/epoch-700\0--first-seen-registry\0--first-seen-scan-only\0";
        assert!(argv_matches_job(scanner, bin, output, "historical_scan"));
        assert!(!argv_matches_job(
            scanner,
            bin,
            output,
            "historical_finalizer"
        ));

        let finalizer = b"/opt/blockzilla/bin/blockzilla\0finalize-archive-v2-first-seen\0/archives/epoch-700\0--finalizer-lock\0/tmp/finalizer.lock\0";
        assert!(argv_matches_job(
            finalizer,
            bin,
            output,
            "historical_finalizer"
        ));
        assert!(!argv_matches_job(finalizer, bin, output, "historical_scan"));

        for live in [
            b"/opt/blockzilla/bin/blockzilla\0prepare-archive-v2-live-registry\0/live/capture\0/archives/epoch-700\0".as_slice(),
            b"/opt/blockzilla/bin/blockzilla\0build-archive-v2-registry-index\0/archives/epoch-700/registry.bin\0--output\0/archives/epoch-700/registry.mphf\0".as_slice(),
            b"/opt/blockzilla/bin/blockzilla\0build-archive-v2-hot-blocks-from-live\0/live/capture\0/archives/epoch-700\0--registry-source\0runs\0".as_slice(),
        ] {
            assert!(argv_matches_job(live, bin, output, "live_finalizer"));
            assert!(!argv_matches_job(live, bin, output, "historical_finalizer"));
        }

        let wrong_path = b"/opt/blockzilla/bin/blockzilla\0finalize-archive-v2-first-seen\0/archives/epoch-700-extra\0";
        assert!(!argv_matches_job(
            wrong_path,
            bin,
            output,
            "historical_finalizer"
        ));
    }

    #[test]
    fn retry_ownership_rejects_cross_kind_and_wrong_id() {
        let owner = OwnershipMarker {
            schema_version: SCHEMA_VERSION,
            kind: "historical_scan".to_string(),
            id: "700".to_string(),
            state: "failed".to_string(),
            created_unix_secs: 1,
            updated_unix_secs: 2,
            message: None,
            pid: None,
        };
        assert!(ownership_matches_retry(&owner, "historical_scan", "700"));
        assert!(ownership_matches_retry(
            &owner,
            "historical_finalizer",
            "700"
        ));
        assert!(!ownership_matches_retry(&owner, "historical_scan", "701"));
        assert!(!ownership_matches_retry(&owner, "live_finalizer", "700"));

        let live_owner = OwnershipMarker {
            kind: "live_finalizer".to_string(),
            ..owner
        };
        assert!(ownership_matches_retry(
            &live_owner,
            "live_finalizer",
            "700"
        ));
        assert!(!ownership_matches_retry(
            &live_owner,
            "historical_finalizer",
            "700"
        ));
    }
}
