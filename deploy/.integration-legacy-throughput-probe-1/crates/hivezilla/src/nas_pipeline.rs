use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    convert::Infallible,
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    net::SocketAddr,
    os::{fd::AsRawFd, unix::ffi::OsStrExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
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

const SCHEMA_VERSION: u32 = 2;
const SLOTS_PER_EPOCH: u64 = 432_000;
const SCAN_MARKER: &str = "archive-v2-first-seen-scan-complete.v1";
const META_FILE: &str = "archive-v2-meta.wincode";
const REGISTRY_FILE: &str = "registry.bin";
const REGISTRY_COUNTS_FILE: &str = "registry_counts.bin";
const REGISTRY_INDEX_FILE: &str = "registry.mphf";
const BLOCKHASH_INDEX_V3_FILE: &str = "blockhash_index_v3.bin";
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
const HOT_SEED_FILE: &str = "registry-hot-seed.bin";
const PREVIOUS_BLOCKHASH_TAIL_FILE: &str = "prev_blockhash_tail.bin";
const LIVE_FINALIZE_MARKER: &str = "FINALIZE-NEXT.md";
const LIVE_READY_MARKER: &str = "READY-TO-PACKAGE";
const LIVE_REGISTRY_READY_MARKER: &str = "archive-v2-live-registry-prepared.v1";
const PROGRESS_STALE_SECS: u64 = 120;
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
const PIPELINE_LOCK_FILE: &str = "pipeline.lock";
const ACQUISITION_FAILURES_FILE: &str = "acquisition-failures.json";
const MIN_CAR_DOWNLOAD_PROJECTION_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const MIN_SCAN_OUTPUT_PROJECTION_BYTES: u64 = 1024 * 1024 * 1024;
const MIN_FINALIZER_SCRATCH_BYTES: u64 = 1024 * 1024 * 1024;
const LEGACY_COMPACT_MIN_MEMORY_MIB: u64 = 1_024;
const LEGACY_COMPACT_MEMORY_OVERHEAD_MIB: u64 = 384;
const LEGACY_COMPACT_OWNERSHIP_KIND: &str = "historical_compact_reuse";
const LEGACY_BLOCKHASH_LOCK_DIR: &str = ".blockhash.lock";
const REGISTRY_INDEX_MAGIC: &[u8; 8] = b"BZKIDX1!";
const REGISTRY_INDEX_VERSION: u16 = 2;
const REGISTRY_INDEX_HEADER_LEN: usize = 20;
const HOT_BLOCK_INDEX_MAGIC: &[u8; 8] = b"BZV2HIX1";
const HOT_BLOCK_INDEX_VERSION: u16 = 1;
const HOT_BLOCK_INDEX_HEADER_LEN: usize = 36;
const HOT_BLOCK_INDEX_ROW_LEN: u64 = 52;
const BLOCKHASH_INDEX_V3_MAGIC: &[u8; 8] = b"BZBHIX3!";
const BLOCKHASH_INDEX_V3_VERSION: u16 = 3;
const BLOCKHASH_INDEX_V3_HEADER_LEN: usize = 20;
const BLOCKHASH_INDEX_V3_ROW_LEN: u64 = 48;

#[derive(Debug, Clone)]
pub struct NasPipelineConfig {
    pub bind: SocketAddr,
    pub blockzilla_bin: PathBuf,
    pub car_root: PathBuf,
    pub archive_root: PathBuf,
    pub live_root: PathBuf,
    pub state_root: PathBuf,
    pub scan_concurrency: usize,
    pub legacy_compact_concurrency: usize,
    pub legacy_compact_cpu_cores_per_worker: u64,
    pub legacy_compact_cpu_budget_cores: u64,
    pub legacy_compact_io_mib_per_sec_per_worker: u64,
    pub legacy_compact_io_budget_mib_per_sec: u64,
    pub legacy_compact_auto_pause: bool,
    pub legacy_compact_min_running: usize,
    pub legacy_compact_memory_guard_mib: u64,
    pub legacy_compact_io_pause_full_avg10: f64,
    pub legacy_compact_io_resume_full_avg10: f64,
    pub legacy_compact_pause_cooldown: Duration,
    pub legacy_compact_throughput_probe_window: Duration,
    pub legacy_compact_throughput_min_gain_pct: f64,
    pub legacy_compact_throughput_probe_backoff: Duration,
    pub scan_memory_mib: u64,
    pub finalizer_memory_mib: u64,
    pub memory_reserve_mib: u64,
    pub disk_reserve_gib: u64,
    pub level: i32,
    pub execute: bool,
    pub no_access: bool,
    pub start_epoch: Option<u64>,
    pub end_epoch: Option<u64>,
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
    ReadyToPackage,
    Packaging,
    Packaged,
    Complete,
    Failed,
    Blocked,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    pub input_mib_per_sec: Option<f64>,
    pub eta_secs: Option<f64>,
    pub rss_bytes: Option<u64>,
    pub updated_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    #[serde(default)]
    pub auto_paused: bool,
    #[serde(default)]
    pub auto_pause_reason: Option<String>,
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
    pub state: LiveState,
    pub capture_dir: PathBuf,
    pub output_path: Option<PathBuf>,
    pub ready_to_package: bool,
    pub repair_gate: bool,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub blocks_written: u64,
    pub artifacts: Vec<ArtifactSnapshot>,
    pub progress: ProgressSnapshot,
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
    pub eta_secs: Option<f64>,
    pub scan_capacity_configured: usize,
    pub scan_capacity_admitted: usize,
    pub admission_blocked_reason: Option<String>,
    #[serde(default)]
    pub legacy_compact_capacity_configured: usize,
    #[serde(default)]
    pub legacy_compact_capacity_effective: usize,
    #[serde(default)]
    pub legacy_compact_capacity_admitted: usize,
    #[serde(default)]
    pub legacy_compact_active: usize,
    #[serde(default)]
    pub legacy_compact_running: usize,
    #[serde(default)]
    pub legacy_compact_paused: usize,
    #[serde(default)]
    pub legacy_compact_auto_paused: usize,
    #[serde(default)]
    pub legacy_compact_auto_pause_enabled: bool,
    #[serde(default)]
    pub legacy_compact_min_running: usize,
    #[serde(default)]
    pub legacy_compact_memory_guard_mib: u64,
    #[serde(default)]
    pub legacy_compact_memory_pause_available_mib: u64,
    #[serde(default)]
    pub legacy_compact_memory_resume_available_mib: u64,
    #[serde(default)]
    pub legacy_compact_io_pause_full_avg10: f64,
    #[serde(default)]
    pub legacy_compact_io_resume_full_avg10: f64,
    #[serde(default)]
    pub legacy_compact_pause_cooldown_secs: u64,
    #[serde(default)]
    pub legacy_compact_last_action_unix_secs: Option<u64>,
    #[serde(default)]
    pub legacy_compact_last_action: Option<String>,
    #[serde(default)]
    pub legacy_compact_throughput_probe_state: String,
    #[serde(default)]
    pub legacy_compact_throughput_probe_window_secs: u64,
    #[serde(default)]
    pub legacy_compact_throughput_min_gain_pct: f64,
    #[serde(default)]
    pub legacy_compact_throughput_probe_backoff_secs: u64,
    #[serde(default)]
    pub legacy_compact_throughput_blocks_per_sec: Option<f64>,
    #[serde(default)]
    pub legacy_compact_throughput_read_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub legacy_compact_throughput_baseline_blocks_per_sec: Option<f64>,
    #[serde(default)]
    pub legacy_compact_throughput_trial_blocks_per_sec: Option<f64>,
    #[serde(default)]
    pub legacy_compact_throughput_confirmation_blocks_per_sec: Option<f64>,
    #[serde(default)]
    pub legacy_compact_throughput_retry_unix_secs: Option<u64>,
    #[serde(default)]
    pub legacy_compact_throughput_next_audit_unix_secs: Option<u64>,
    #[serde(default)]
    pub legacy_compact_cpu_cores_per_worker: u64,
    #[serde(default)]
    pub legacy_compact_cpu_budget_cores: u64,
    #[serde(default)]
    pub legacy_compact_io_mib_per_sec_per_worker: u64,
    #[serde(default)]
    pub legacy_compact_io_budget_mib_per_sec: u64,
    #[serde(default)]
    pub legacy_compact_admission_blocked_reason: Option<String>,
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
    pub load_1m: f64,
    #[serde(default)]
    pub io_pressure_some_avg10: Option<f64>,
    #[serde(default)]
    pub io_pressure_full_avg10: Option<f64>,
    #[serde(default)]
    pub memory_pressure_some_avg10: Option<f64>,
    #[serde(default)]
    pub memory_pressure_full_avg10: Option<f64>,
    pub service_rss_bytes: u64,
    pub children_rss_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSnapshot {
    pub schema_version: u32,
    pub sequence: u64,
    pub now_unix_secs: u64,
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
struct RealtimeEnvelope {
    #[serde(rename = "type")]
    event_type: &'static str,
    sequence: u64,
    data: PipelineSnapshot,
}

#[derive(Debug)]
struct AppState {
    config: NasPipelineConfig,
    snapshot: RwLock<PipelineSnapshot>,
    updates: broadcast::Sender<RealtimeEnvelope>,
    sequence: AtomicU64,
    runtime: Mutex<RuntimeState>,
}

#[derive(Debug, Default)]
struct RuntimeState {
    acquisitions: BTreeMap<u64, ManagedChild>,
    scans: BTreeMap<u64, ManagedChild>,
    legacy_compacts: BTreeMap<u64, ManagedChild>,
    finalizer: Option<ManagedChild>,
    errors: VecDeque<PipelineError>,
    failures: BTreeMap<String, String>,
    scheduler_paused: bool,
    scheduler_updated_unix_secs: u64,
    paused_jobs: BTreeSet<String>,
    auto_paused_legacy: BTreeMap<u64, AutoPausedLegacy>,
    legacy_last_adaptive_action_unix_secs: u64,
    legacy_last_adaptive_action_reason: Option<String>,
    legacy_throughput: LegacyThroughputRuntime,
    adopted_legacy_compacts: BTreeMap<u64, AdoptedLegacyCompact>,
    inventory_generation: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LegacyAutoPauseCause {
    #[default]
    Memory,
    ThroughputProbe,
    ThroughputSaturated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AutoPausedLegacy {
    epoch: u64,
    pid: u32,
    #[serde(default)]
    process_start_ticks: Option<u64>,
    reason: String,
    paused_unix_secs: u64,
    #[serde(default)]
    cause: LegacyAutoPauseCause,
}

#[derive(Debug, Clone, PartialEq)]
struct LegacyThroughputCounter {
    pid: u32,
    phase: Option<String>,
    blocks_done: u64,
    read_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
struct LegacyThroughputWindow {
    running_epochs: Vec<u64>,
    started_unix_secs: u64,
    blocks_done: u64,
    read_bytes: u64,
    read_bytes_observed: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct LegacyThroughputMeasurement {
    running_epochs: Vec<u64>,
    started_unix_secs: u64,
    ended_unix_secs: u64,
    blocks_per_sec: f64,
    read_mib_per_sec: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
enum LegacyThroughputProbe {
    Trial {
        baseline: LegacyThroughputMeasurement,
        trial_epoch: u64,
    },
    Confirm {
        baseline: LegacyThroughputMeasurement,
        trial: LegacyThroughputMeasurement,
        trial_epoch: u64,
    },
    AuditPaused {
        loaded: LegacyThroughputMeasurement,
        trial_epoch: u64,
    },
    AuditReloaded {
        loaded: LegacyThroughputMeasurement,
        reduced: LegacyThroughputMeasurement,
        trial_epoch: u64,
    },
    Saturated {
        baseline: LegacyThroughputMeasurement,
        trial: LegacyThroughputMeasurement,
        confirmation: LegacyThroughputMeasurement,
        trial_epoch: u64,
        retry_unix_secs: u64,
    },
}

#[derive(Debug, Clone, Default)]
struct LegacyThroughputRuntime {
    counters: BTreeMap<u64, LegacyThroughputCounter>,
    window: Option<LegacyThroughputWindow>,
    last_measurement: Option<LegacyThroughputMeasurement>,
    baseline: Option<LegacyThroughputMeasurement>,
    probe: Option<LegacyThroughputProbe>,
    next_audit_unix_secs: u64,
    probe_invalidated_by_memory: bool,
}

#[derive(Debug, Clone)]
struct AdoptedLegacyCompact {
    epoch: u64,
    pid: u32,
    owner_schema_version: u32,
    process_start_ticks: u64,
    progress_path: PathBuf,
    identity_tainted: bool,
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
    HistoricalCompactReuse {
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
            Self::HistoricalCompactReuse { epoch } => format!("compact_reuse:{epoch}"),
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
    let legacy_resource = legacy_compact_resource_capacity(&config);
    anyhow::ensure!(
        !config.legacy_compact_auto_pause
            || config.legacy_compact_min_running <= legacy_resource.effective_slots,
        "legacy compact minimum running ({}) exceeds effective capacity ({})",
        config.legacy_compact_min_running,
        legacy_resource.effective_slots,
    );
    anyhow::ensure!(
        !config.legacy_compact_auto_pause || config.legacy_compact_min_running > 0,
        "legacy compact minimum running must be positive when adaptive probing is enabled",
    );
    anyhow::ensure!(
        config.legacy_compact_io_pause_full_avg10.is_finite()
            && config.legacy_compact_io_resume_full_avg10.is_finite()
            && config.legacy_compact_io_resume_full_avg10 >= 0.0
            && config.legacy_compact_io_pause_full_avg10 <= 100.0
            && config.legacy_compact_io_resume_full_avg10 <= 100.0
            && config.legacy_compact_io_pause_full_avg10
                > config.legacy_compact_io_resume_full_avg10,
        "legacy compact IO pause full avg10 must be finite and greater than the non-negative resume threshold"
    );
    anyhow::ensure!(
        !config.legacy_compact_pause_cooldown.is_zero(),
        "legacy compact pause cooldown must be positive"
    );
    anyhow::ensure!(
        config.legacy_compact_throughput_probe_window >= config.poll_interval.saturating_mul(3),
        "legacy compact throughput probe window must span at least three scheduler polls"
    );
    anyhow::ensure!(
        config.legacy_compact_throughput_probe_window >= config.legacy_compact_pause_cooldown,
        "legacy compact throughput probe window must not be shorter than the adaptive action cooldown"
    );
    anyhow::ensure!(
        config.legacy_compact_throughput_min_gain_pct.is_finite()
            && (0.0..=100.0).contains(&config.legacy_compact_throughput_min_gain_pct),
        "legacy compact throughput minimum gain must be finite and between 0 and 100"
    );
    anyhow::ensure!(
        !config.legacy_compact_throughput_probe_backoff.is_zero(),
        "legacy compact throughput probe backoff must be positive"
    );
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
        runtime: Mutex::new(RuntimeState::default()),
    });
    load_persisted_errors(&state).await;
    load_control_state(&state).await?;
    load_acquisition_failures(&state).await;

    // Bind before the first reconciliation. This also protects an upgrade
    // from an older binary that did not yet participate in pipeline.lock: a
    // port collision cannot launch work and then fail startup.
    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("bind NAS pipeline on {}", config.bind))?;
    if config.execute {
        recover_auto_paused_legacy(&state).await?;
        track_adopted_legacy_compacts(&state).await?;
        recover_manual_paused_legacy(&state).await?;
    }
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
    let initial_snapshot = state.snapshot.read().await.clone();
    let initial = RealtimeEnvelope {
        event_type: "snapshot",
        sequence: initial_snapshot.sequence,
        data: initial_snapshot,
    };
    let initial_stream = tokio_stream::once(Ok(sse_event(&initial)));
    let update_stream = BroadcastStream::new(state.updates.subscribe())
        .filter_map(|item| item.ok().map(|envelope| Ok(sse_event(&envelope))));
    Sse::new(initial_stream.chain(update_stream)).keep_alive(KeepAlive::default())
}

fn sse_event(envelope: &RealtimeEnvelope) -> Event {
    Event::default()
        .event("snapshot")
        .id(envelope.sequence.to_string())
        .json_data(envelope)
        .unwrap_or_else(|_| Event::default().event("snapshot").data("{}"))
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
        "historical_scan" | "historical_compact_reuse" | "historical_finalizer" => {
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
                vec![
                    format!("scan:{epoch}"),
                    format!("compact_reuse:{epoch}"),
                    format!("finalize:{epoch}"),
                ],
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
    ) || pipeline_owned_legacy_compact_complete(&target)
    {
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
            | "compact_reuse"
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
    let retry_message = if owner.kind == LEGACY_COMPACT_OWNERSHIP_KIND {
        // This lane adopted a vetted legacy registry-only output. Its four
        // registry sidecars are the migration input, not disposable partial
        // output. The compact builder truncates/replaces its own generated
        // reader files on --resume, so retry only resets safe ownership.
        write_ownership(
            &target,
            LEGACY_COMPACT_OWNERSHIP_KIND,
            &owner.id,
            "retry_ready",
            None,
        )
        .map_err(ControlError::Internal)?;
        set_ownership_pid(&target, None).map_err(ControlError::Internal)?;
        format!(
            "preserved legacy registry sidecars and partial reader files in {}",
            target.display()
        )
    } else if kind == "live_finalizer" {
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
        "historical_scan" | "historical_compact_reuse" | "historical_finalizer" => {
            matches!(
                owner.kind.as_str(),
                "historical_scan" | "historical_compact_reuse" | "historical_finalizer"
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
    let legacy_epoch = (kind == "historical_compact_reuse")
        .then(|| id.parse::<u64>().ok())
        .flatten();
    let mut runtime = state.runtime.lock().await;
    // Serialize the full persisted-state/signal transaction with scheduling,
    // reaping, and concurrent controls. Revalidate after taking the mutex so
    // the PID cannot be swapped by a completed/retried runtime child.
    validate_process_identity(pid, &state.config.blockzilla_bin, &expected_path, &kind)?;
    let managed_legacy = legacy_epoch.is_some_and(|epoch| {
        runtime
            .legacy_compacts
            .get(&epoch)
            .is_some_and(|child| child.pid == Some(pid))
    });
    let adopted_legacy = legacy_epoch.and_then(|epoch| {
        runtime
            .adopted_legacy_compacts
            .get(&epoch)
            .filter(|child| child.pid == pid)
    });
    let adopted_identity_trusted = adopted_legacy.is_some_and(|child| {
        !child.identity_tainted
            && process_stat_identity(pid)
                .is_some_and(|(_, start_ticks)| start_ticks == child.process_start_ticks)
            && legacy_epoch.is_some_and(|epoch| {
                process_cmdline_matches_legacy_exact(&state.config, epoch, pid) == Some(true)
            })
    });
    let managed_identity_trusted = managed_legacy
        && legacy_epoch.is_some_and(|epoch| {
            process_cmdline_matches_legacy_exact(&state.config, epoch, pid) == Some(true)
        });
    if (managed_legacy || adopted_legacy.is_some())
        && !(managed_identity_trusted || adopted_identity_trusted)
    {
        return Err(ControlError::Conflict(format!(
            "tracked legacy pid {pid} failed stable starttime and byte-exact argv validation"
        )));
    }
    let trusted_legacy = managed_identity_trusted || adopted_identity_trusted;
    let trusted_legacy_group = trusted_legacy && process_is_group_leader(pid);
    let signal = if pause { libc::SIGSTOP } else { libc::SIGCONT };
    // SAFETY: kill sends a signal to the positively identified child process.
    let signal_target = controlled_signal_target(&kind, pid, trusted_legacy_group);

    if pause {
        // Publish the manual ownership transfer before SIGSTOP. A crash can
        // then never leave an unrecorded stop or an auto record that startup
        // would resume against the operator's explicit pause.
        let was_manual = runtime.paused_jobs.contains(&key);
        let previous_auto =
            legacy_epoch.and_then(|epoch| runtime.auto_paused_legacy.get(&epoch).cloned());
        set_manual_pause_state(&mut runtime, &key, legacy_epoch, true);
        if let Err(error) = persist_control_state(&state.config, &runtime) {
            restore_manual_pause_state(&mut runtime, &key, legacy_epoch, was_manual, previous_auto);
            return Err(ControlError::Internal(error));
        }
        // SAFETY: argv was validated and negative targets are used only for
        // controller-created process-group leaders.
        if unsafe { libc::kill(signal_target, signal) } != 0 {
            let signal_error = std::io::Error::last_os_error();
            restore_manual_pause_state(&mut runtime, &key, legacy_epoch, was_manual, previous_auto);
            let rollback = persist_control_state(&state.config, &runtime).err();
            return Err(ControlError::Internal(anyhow::anyhow!(
                "manual pause signal failed: {signal_error}; intent rollback={rollback:?}"
            )));
        }
    } else {
        let was_manual = runtime.paused_jobs.contains(&key);
        let previous_auto =
            legacy_epoch.and_then(|epoch| runtime.auto_paused_legacy.get(&epoch).cloned());
        // Resume signal first: duplicate SIGCONT during crash recovery is
        // harmless, whereas clearing the record first could strand a stop.
        if unsafe { libc::kill(signal_target, signal) } != 0 {
            return Err(ControlError::Internal(
                std::io::Error::last_os_error().into(),
            ));
        }
        set_manual_pause_state(&mut runtime, &key, legacy_epoch, false);
        if let Err(error) = persist_control_state(&state.config, &runtime) {
            restore_manual_pause_state(&mut runtime, &key, legacy_epoch, was_manual, previous_auto);
            // SAFETY: restore the stopped state that remains on disk.
            let rollback_signal = unsafe { libc::kill(signal_target, libc::SIGSTOP) };
            return Err(ControlError::Internal(anyhow::anyhow!(
                "persist manual resume failed: {error:#}; SIGSTOP rollback result={rollback_signal}"
            )));
        }
    }
    append_control_event(&state.config, if pause { "pause" } else { "resume" }, &key)
        .map_err(ControlError::Internal)?;
    drop(runtime);
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

fn controlled_signal_target(kind: &str, pid: u32, trusted_legacy_group: bool) -> libc::pid_t {
    if matches!(kind, "car_download" | "car_preflight") || trusted_legacy_group {
        -(pid as libc::pid_t)
    } else {
        pid as libc::pid_t
    }
}

fn set_manual_pause_state(
    runtime: &mut RuntimeState,
    key: &str,
    legacy_epoch: Option<u64>,
    pause: bool,
) {
    if pause {
        if let Some(epoch) = legacy_epoch {
            runtime.auto_paused_legacy.remove(&epoch);
        }
        runtime.paused_jobs.insert(key.to_string());
    } else {
        runtime.paused_jobs.remove(key);
        if let Some(epoch) = legacy_epoch {
            runtime.auto_paused_legacy.remove(&epoch);
        }
    }
}

fn restore_manual_pause_state(
    runtime: &mut RuntimeState,
    key: &str,
    legacy_epoch: Option<u64>,
    was_manual: bool,
    previous_auto: Option<AutoPausedLegacy>,
) {
    if was_manual {
        runtime.paused_jobs.insert(key.to_string());
    } else {
        runtime.paused_jobs.remove(key);
    }
    if let Some(epoch) = legacy_epoch {
        runtime.auto_paused_legacy.remove(&epoch);
        if let Some(record) = previous_auto {
            runtime.auto_paused_legacy.insert(epoch, record);
        }
    }
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
        "historical_compact_reuse" => id
            .parse::<u64>()
            .map(|epoch| format!("compact_reuse:{epoch}"))
            .map_err(|_| ControlError::BadRequest("compact/reuse id must be an epoch".to_string())),
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
        .chain(runtime.legacy_compacts.values())
        .chain(runtime.finalizer.iter())
        .find(|child| child.kind.key() == key)
        .and_then(|child| child.pid)
        .or_else(|| {
            runtime
                .adopted_legacy_compacts
                .values()
                .find(|child| format!("compact_reuse:{}", child.epoch) == key)
                .map(|child| child.pid)
        });
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
        "historical_scan" | "historical_compact_reuse" | "historical_finalizer" => {
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
        "historical_compact_reuse" => {
            args.get(1).copied() == Some(b"build-archive-v2-hot-blocks")
                && args.get(3).copied() == Some(expected_path)
                && args
                    .windows(2)
                    .any(|pair| pair[0] == b"--registry-dir" && pair[1] == expected_path)
                && args.iter().any(|arg| *arg == b"--resume")
                && args.iter().any(|arg| *arg == b"--no-access")
                && !args.iter().any(|arg| *arg == b"--first-seen-registry")
                && !args.iter().any(|arg| *arg == b"--first-seen-scan-only")
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
        schema_version: SCHEMA_VERSION,
        sequence: 0,
        now_unix_secs: unix_now(),
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
    reap_adopted_legacy_compacts(&state.config, &mut runtime);
    reconcile_acquisition_state(&state.config, &mut runtime);
    runtime.inventory_generation = runtime.inventory_generation.saturating_add(1);
    let inventory_generation = runtime.inventory_generation;

    let mut snapshot = reconcile_filesystem(&state.config, &runtime, inventory_generation);
    if state.config.execute {
        adjust_legacy_workers_for_pressure(&state.config, &snapshot, &mut runtime, unix_now());
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
    let sequence = state.sequence.fetch_add(1, Ordering::Relaxed) + 1;
    snapshot.sequence = sequence;
    snapshot.errors = runtime.errors.iter().cloned().collect();
    if let Err(error) = persist_snapshot(&state.config.state_root, &snapshot) {
        record_error(
            &state.config,
            &mut runtime,
            "state",
            format!("persist status: {error:#}"),
        );
        snapshot.errors = runtime.errors.iter().cloned().collect();
    }
    *state.snapshot.write().await = snapshot.clone();
    let _ = state.updates.send(RealtimeEnvelope {
        event_type: "snapshot",
        sequence,
        data: snapshot,
    });
}

fn reconcile_filesystem(
    config: &NasPipelineConfig,
    runtime: &RuntimeState,
    inventory_generation: u64,
) -> PipelineSnapshot {
    let now = unix_now();
    let discovery = discover_inventory(config);
    let live_epochs = discovery
        .live_paths
        .iter()
        .filter_map(|path| path.file_name()?.to_str().and_then(parse_epoch_name))
        .collect::<BTreeSet<_>>();
    let epochs = discovery
        .epochs
        .iter()
        .copied()
        .map(|epoch| {
            classify_epoch_with_context(config, runtime, epoch, now, !live_epochs.contains(&epoch))
        })
        .collect::<Vec<_>>();
    let live = discovery
        .live_paths
        .iter()
        .cloned()
        .map(|path| classify_live_capture(config, runtime, path, now))
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
            auto_paused: false,
            auto_pause_reason: None,
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
        let legacy_compact = read_ownership(&epoch.output_path).is_some_and(|owner| {
            owner.kind == LEGACY_COMPACT_OWNERSHIP_KIND && owner.id == epoch.epoch.to_string()
        });
        let (id, kind, default_phase) = if legacy_compact {
            (
                format!("compact_reuse:{}", epoch.epoch),
                "historical_compact_reuse",
                "compact_reuse",
            )
        } else {
            (format!("scan:{}", epoch.epoch), "historical_scan", "scan")
        };
        if lanes.iter().any(|lane| lane.id == id) {
            continue;
        }
        lanes.push(LaneSnapshot {
            id: id.clone(),
            kind: kind.to_string(),
            epoch: Some(epoch.epoch),
            capture_id: None,
            phase: epoch
                .progress
                .phase
                .clone()
                .unwrap_or_else(|| default_phase.to_string()),
            state: if runtime.paused_jobs.contains(&id) {
                "paused".to_string()
            } else {
                "running".to_string()
            },
            auto_paused: false,
            auto_pause_reason: None,
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
            auto_paused: false,
            auto_pause_reason: None,
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
            auto_paused: false,
            auto_pause_reason: None,
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
            .filter(|capture| capture.state == LiveState::ReadyToPackage)
            .filter_map(|capture| live_finalizer_queue_item(config, capture)),
    );
    finalizer_queue.sort_by_key(|item| {
        (
            if item.kind == "live" { 0 } else { 1 },
            item.epoch.unwrap_or(u64::MAX),
            item.id.clone(),
        )
    });
    let children_rss_bytes = lanes.iter().filter_map(|lane| lane.rss_bytes).sum();
    let acquisition_rss_bytes = lanes
        .iter()
        .filter(|lane| matches!(lane.kind.as_str(), "car_download" | "car_preflight"))
        .filter_map(|lane| lane.rss_bytes)
        .sum();
    let machine = machine_snapshot(&config.archive_root, &config.car_root, children_rss_bytes);
    let admission = admission_snapshot(config, &machine, &epochs);
    let legacy_resource = legacy_compact_resource_capacity(config);
    let legacy_active_rss = sampled_legacy_compact_rss(&lanes, runtime);
    let (legacy_admitted, legacy_blocked_reason) = legacy_compact_capacity_admission(
        config,
        &machine,
        &epochs,
        &legacy_active_rss,
        &runtime.failures,
    );
    let mut summary = summarize_epochs(&epochs);
    summary.scan_capacity_configured = config.scan_concurrency;
    summary.scan_capacity_admitted = admission.scan_capacity;
    summary.admission_blocked_reason = admission.blocked_reason;
    summary.legacy_compact_capacity_configured = config.legacy_compact_concurrency;
    summary.legacy_compact_capacity_effective = legacy_resource.effective_slots;
    summary.legacy_compact_capacity_admitted = legacy_admitted;
    summary.legacy_compact_active = legacy_active_rss.len();
    summary.legacy_compact_running = lanes
        .iter()
        .filter(|lane| lane.kind == "historical_compact_reuse" && lane.state.as_str() != "paused")
        .count();
    summary.legacy_compact_paused = lanes
        .iter()
        .filter(|lane| lane.kind == "historical_compact_reuse" && lane.state.as_str() == "paused")
        .count();
    summary.legacy_compact_auto_paused = lanes
        .iter()
        .filter(|lane| lane.kind == "historical_compact_reuse" && lane.auto_paused)
        .count();
    summary.legacy_compact_auto_pause_enabled = config.legacy_compact_auto_pause;
    summary.legacy_compact_min_running = config.legacy_compact_min_running;
    summary.legacy_compact_memory_guard_mib = config.legacy_compact_memory_guard_mib;
    summary.legacy_compact_memory_pause_available_mib = config
        .memory_reserve_mib
        .saturating_add(config.legacy_compact_memory_guard_mib);
    summary.legacy_compact_memory_resume_available_mib = config
        .memory_reserve_mib
        .saturating_add(config.legacy_compact_memory_guard_mib.saturating_mul(2));
    summary.legacy_compact_io_pause_full_avg10 = config.legacy_compact_io_pause_full_avg10;
    summary.legacy_compact_io_resume_full_avg10 = config.legacy_compact_io_resume_full_avg10;
    summary.legacy_compact_pause_cooldown_secs = config.legacy_compact_pause_cooldown.as_secs();
    summary.legacy_compact_last_action_unix_secs = (runtime.legacy_last_adaptive_action_unix_secs
        > 0)
    .then_some(runtime.legacy_last_adaptive_action_unix_secs);
    summary.legacy_compact_last_action = runtime.legacy_last_adaptive_action_reason.clone();
    summary.legacy_compact_throughput_probe_window_secs =
        config.legacy_compact_throughput_probe_window.as_secs();
    summary.legacy_compact_throughput_min_gain_pct = config.legacy_compact_throughput_min_gain_pct;
    summary.legacy_compact_throughput_probe_backoff_secs =
        config.legacy_compact_throughput_probe_backoff.as_secs();
    summary.legacy_compact_throughput_blocks_per_sec = runtime
        .legacy_throughput
        .last_measurement
        .as_ref()
        .map(|measurement| measurement.blocks_per_sec);
    summary.legacy_compact_throughput_read_mib_per_sec = runtime
        .legacy_throughput
        .last_measurement
        .as_ref()
        .and_then(|measurement| measurement.read_mib_per_sec);
    summary.legacy_compact_throughput_next_audit_unix_secs =
        (runtime.legacy_throughput.next_audit_unix_secs > 0)
            .then_some(runtime.legacy_throughput.next_audit_unix_secs);
    if !config.legacy_compact_auto_pause {
        summary.legacy_compact_throughput_probe_state = "disabled".to_string();
    } else {
        match runtime.legacy_throughput.probe.as_ref() {
            None => {
                summary.legacy_compact_throughput_probe_state =
                    if runtime.legacy_throughput.baseline.is_some() {
                        "baseline_ready"
                    } else {
                        "measuring_baseline"
                    }
                    .to_string();
                summary.legacy_compact_throughput_baseline_blocks_per_sec = runtime
                    .legacy_throughput
                    .baseline
                    .as_ref()
                    .map(|measurement| measurement.blocks_per_sec);
            }
            Some(LegacyThroughputProbe::Trial { baseline, .. }) => {
                summary.legacy_compact_throughput_probe_state = "trial_b".to_string();
                summary.legacy_compact_throughput_baseline_blocks_per_sec =
                    Some(baseline.blocks_per_sec);
            }
            Some(LegacyThroughputProbe::Confirm {
                baseline, trial, ..
            }) => {
                summary.legacy_compact_throughput_probe_state = "confirm_a2".to_string();
                summary.legacy_compact_throughput_baseline_blocks_per_sec =
                    Some(baseline.blocks_per_sec);
                summary.legacy_compact_throughput_trial_blocks_per_sec = Some(trial.blocks_per_sec);
            }
            Some(LegacyThroughputProbe::AuditPaused { loaded, .. }) => {
                summary.legacy_compact_throughput_probe_state = "audit_n_minus_1".to_string();
                summary.legacy_compact_throughput_trial_blocks_per_sec =
                    Some(loaded.blocks_per_sec);
            }
            Some(LegacyThroughputProbe::AuditReloaded {
                loaded, reduced, ..
            }) => {
                summary.legacy_compact_throughput_probe_state = "audit_n_recheck".to_string();
                summary.legacy_compact_throughput_trial_blocks_per_sec =
                    Some(loaded.blocks_per_sec);
                summary.legacy_compact_throughput_confirmation_blocks_per_sec =
                    Some(reduced.blocks_per_sec);
            }
            Some(LegacyThroughputProbe::Saturated {
                baseline,
                trial,
                confirmation,
                retry_unix_secs,
                ..
            }) => {
                summary.legacy_compact_throughput_probe_state = "ceiling_confirmed".to_string();
                summary.legacy_compact_throughput_baseline_blocks_per_sec =
                    Some(baseline.blocks_per_sec);
                summary.legacy_compact_throughput_trial_blocks_per_sec = Some(trial.blocks_per_sec);
                summary.legacy_compact_throughput_confirmation_blocks_per_sec =
                    Some(confirmation.blocks_per_sec);
                summary.legacy_compact_throughput_retry_unix_secs = Some(*retry_unix_secs);
            }
        }
    }
    summary.legacy_compact_cpu_cores_per_worker = config.legacy_compact_cpu_cores_per_worker;
    summary.legacy_compact_cpu_budget_cores = config.legacy_compact_cpu_budget_cores;
    summary.legacy_compact_io_mib_per_sec_per_worker =
        config.legacy_compact_io_mib_per_sec_per_worker;
    summary.legacy_compact_io_budget_mib_per_sec = config.legacy_compact_io_budget_mib_per_sec;
    summary.legacy_compact_admission_blocked_reason = legacy_blocked_reason;
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
    let next_download_disk_bytes = epochs
        .iter()
        .find(|epoch| acquisition_action(config, epoch) == Some(AcquisitionAction::Download))
        .map(|epoch| car_download_remaining_projection(config, &epochs, epoch.epoch))
        .unwrap_or(0);
    let acquisition_pending = epochs
        .iter()
        .any(|epoch| acquisition_action(config, epoch).is_some());
    let sweep_blocked_reason = if !discovery.errors.is_empty() {
        Some("inventory is incomplete; no new work will be scheduled".to_string())
    } else if download_pending {
        car_disk_admission_blocked_reason(
            config,
            &machine,
            active_download_disk_bytes.saturating_add(next_download_disk_bytes),
        )
        .or_else(|| {
            Some(format!(
                "historical scan sweep in progress; historical finalizers are deferred: pending={scan_pending} active={scan_active}"
            ))
        })
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
        Some(format!(
            "CAR preflight admission blocked: available {:.1} MiB, reserve {} MiB, task budget {} MiB",
            machine.memory_available_bytes as f64 / 1024f64.powi(2),
            config.memory_reserve_mib,
            PREFLIGHT_MEMORY_MIB,
        ))
    } else if scan_pending > 0 || scan_active > 0 {
        Some(format!(
            "historical scan sweep in progress; historical finalizers are deferred: pending={scan_pending} active={scan_active}"
        ))
    } else {
        None
    };
    if !sweep_complete {
        for item in &mut finalizer_queue {
            if item.kind == "historical" {
                item.deferred_reason = sweep_blocked_reason.clone();
            }
        }
    }
    summary.finalizer_admission_blocked_reason = if finalizer_queue
        .first()
        .is_some_and(|item| item.kind == "live")
    {
        finalizer_queue_admission_blocked_reason(config, &machine, &finalizer_queue)
    } else if !sweep_complete && !finalizer_queue.is_empty() {
        sweep_blocked_reason.clone()
    } else {
        finalizer_queue_admission_blocked_reason(config, &machine, &finalizer_queue)
    };
    summary.eta_secs = estimate_summary_eta(&epochs, admission.scan_capacity);
    PipelineSnapshot {
        schema_version: SCHEMA_VERSION,
        sequence: 0,
        now_unix_secs: now,
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
            epochs_discovered: discovery.epochs.len(),
            epochs_classified: epochs.len(),
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
        progress.rss_bytes = process_rss_bytes(pid);
    }
    let owner = read_ownership(&output);
    let owner_is_first_seen = owner.as_ref().is_some_and(ownership_is_first_seen);
    let owner_is_legacy_compact = owner
        .as_ref()
        .is_some_and(|owner| owner.kind == LEGACY_COMPACT_OWNERSHIP_KIND);
    let owner_matches_epoch = owner.as_ref().is_some_and(|owner| {
        (owner_is_first_seen || owner_is_legacy_compact) && owner.id == epoch.to_string()
    });
    let require_first_seen_manifest = owner_matches_epoch && owner_is_first_seen;
    let legacy_no_access_complete = allow_legacy_no_access
        && legacy_no_access_archive_complete(
            &output,
            !config.no_access,
            require_first_seen_manifest,
        );
    // A compact/reuse child publishes directly into its adopted directory.
    // Require the controller's successful-exit commit before accepting those
    // files; otherwise a controller restart near EOF could bless a torn core.
    let legacy_compact_reuse_complete = owner_matches_epoch
        && owner_is_legacy_compact
        && owner
            .as_ref()
            .is_some_and(|owner| owner.state == "complete")
        && legacy_compact_reader_complete(&output);
    let output_complete = (!owner_is_legacy_compact
        && historical_archive_strict_complete(
            &output,
            !config.no_access,
            require_first_seen_manifest,
        ))
        || legacy_no_access_complete
        || legacy_compact_reuse_complete;
    let scan_marker = scan_marker_is_valid(&output.join(SCAN_MARKER));
    let ambiguous_car = car_paths_ambiguous(&config.car_root, epoch);
    let active_scan = runtime.scans.contains_key(&epoch);
    let active_legacy_compact = runtime.legacy_compacts.contains_key(&epoch)
        || runtime.adopted_legacy_compacts.contains_key(&epoch);
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
    let failure_key_compact_reuse = format!("compact_reuse:{epoch}");
    let failure_key_finalize = format!("finalize:{epoch}");
    let failure = runtime
        .failures
        .get(&failure_key_scan)
        .or_else(|| runtime.failures.get(&failure_key_compact_reuse))
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
    let owner_compacting = owner.as_ref().is_some_and(|owner| {
        owner_matches_epoch
            && owner_is_legacy_compact
            && owner.state == "compact_reuse"
            && owner.pid.is_some_and(|pid| {
                process_cmdline_matches_job(
                    pid,
                    &config.blockzilla_bin,
                    &output,
                    "historical_compact_reuse",
                )
            })
    });
    let owner_process_active = owner_scanning || owner_compacting || owner_finalizing;
    let ownership_failure = owner
        .as_ref()
        .filter(|_| owner_matches_epoch)
        .and_then(|owner| {
            if owner.state == "failed" {
                owner.message.clone()
            } else if owner_is_legacy_compact
                && owner.state == "complete"
                && !legacy_compact_reader_complete(&output)
            {
                Some("legacy compact/reuse commit failed reader-core validation".to_string())
            } else if matches!(
                owner.state.as_str(),
                "running" | "compact_reuse" | "finalizing"
            ) && owner.pid.is_some()
                && !owner_process_active
            {
                Some("pipeline-owned process is no longer running".to_string())
            } else {
                None
            }
        });
    if owner_compacting {
        progress.pid = owner.as_ref().and_then(|owner| owner.pid);
        progress.rss_bytes = progress.pid.and_then(process_rss_bytes);
        progress.phase = Some("compact_reuse".to_string());
        progress.state = Some("running".to_string());
    } else if owner_finalizing {
        progress.pid = owner.as_ref().and_then(|owner| owner.pid);
        progress.rss_bytes = progress.pid.and_then(process_rss_bytes);
        progress.phase = Some("finalize".to_string());
        progress.state = Some("running".to_string());
    }
    let legacy_compact_status = legacy_compact_reuse_status(config, epoch);
    let legacy_compact_managed =
        owner_is_legacy_compact || legacy_compact_status != LegacyCompactReuseStatus::NotCandidate;
    let output_has_files = directory_has_entries(&output);

    let (state, message) = if output_complete {
        (
            HistoricalState::Complete,
            if legacy_compact_reuse_complete {
                Some(
                    "legacy compact/reuse light-complete: reader core validated; block-access sidecars intentionally absent"
                        .to_string(),
                )
            } else {
                legacy_no_access_complete.then(|| {
                    "accepted legacy no-access archive; both block-access sidecars were intentionally absent in the previous format"
                        .to_string()
                })
            },
        )
    } else if active_finalizer || owner_finalizing {
        (HistoricalState::Finalizing, None)
    } else if active_legacy_compact || owner_compacting {
        (
            HistoricalState::Scanning,
            Some("legacy compact/reuse is streaming and validating the CAR once".to_string()),
        )
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
    } else if legacy_compact_status == LegacyCompactReuseStatus::Ready {
        (
            HistoricalState::Queued,
            Some(
                "legacy registry sidecars are ready for one-pass compact/reuse; separate CAR preflight is bypassed"
                    .to_string(),
            ),
        )
    } else if legacy_compact_status == LegacyCompactReuseStatus::WaitingForPrevious {
        (
            HistoricalState::Queued,
            Some(
                "legacy compact/reuse is waiting for a usable previous blockhash tail or predecessor reader sidecars"
                    .to_string(),
            ),
        )
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
        legacy_compact_reuse_complete,
        legacy_compact_managed,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyCompactReuseStatus {
    NotCandidate,
    WaitingForPrevious,
    Ready,
}

fn read_fixed_header<const N: usize>(path: &Path) -> Option<[u8; N]> {
    let mut header = [0u8; N];
    File::open(path).ok()?.read_exact(&mut header).ok()?;
    Some(header)
}

fn registry_index_matches_registry(output: &Path) -> bool {
    let registry = output.join(REGISTRY_FILE);
    let registry_bytes = file_len(&registry);
    if registry_bytes == 0 || !registry_bytes.is_multiple_of(32) {
        return false;
    }
    let path = output.join(REGISTRY_INDEX_FILE);
    let Some(header) = read_fixed_header::<REGISTRY_INDEX_HEADER_LEN>(&path) else {
        return false;
    };
    let keys = u64::from_le_bytes(header[12..20].try_into().unwrap());
    header[..8] == REGISTRY_INDEX_MAGIC[..]
        && u16::from_le_bytes(header[8..10].try_into().unwrap()) == REGISTRY_INDEX_VERSION
        && usize::from(u16::from_le_bytes(header[10..12].try_into().unwrap()))
            == REGISTRY_INDEX_HEADER_LEN
        && keys == registry_bytes / 32
        // values[u32] + tags[u64] precede the serialized MPHF body.
        && file_len(&path)
            > (REGISTRY_INDEX_HEADER_LEN as u64).saturating_add(keys.saturating_mul(12))
}

fn blockhash_registry_valid(output: &Path) -> bool {
    let bytes = file_len(&output.join(BLOCKHASH_REGISTRY_FILE));
    bytes > 0 && bytes.is_multiple_of(32)
}

fn optional_blockhash_v3_valid(output: &Path) -> bool {
    let path = output.join(BLOCKHASH_INDEX_V3_FILE);
    if !path.exists() {
        return true;
    }
    let Some(header) = read_fixed_header::<BLOCKHASH_INDEX_V3_HEADER_LEN>(&path) else {
        return false;
    };
    let rows = u64::from_le_bytes(header[12..20].try_into().unwrap());
    header[..8] == BLOCKHASH_INDEX_V3_MAGIC[..]
        && u16::from_le_bytes(header[8..10].try_into().unwrap()) == BLOCKHASH_INDEX_V3_VERSION
        && u16::from_le_bytes(header[10..12].try_into().unwrap())
            == BLOCKHASH_INDEX_V3_ROW_LEN as u16
        && rows == file_len(&output.join(BLOCKHASH_REGISTRY_FILE)) / 32
        && file_len(&path)
            == (BLOCKHASH_INDEX_V3_HEADER_LEN as u64)
                .saturating_add(rows.saturating_mul(BLOCKHASH_INDEX_V3_ROW_LEN))
}

fn legacy_reusable_sidecars_valid(output: &Path) -> bool {
    is_nonempty_file(&output.join(REGISTRY_COUNTS_FILE))
        && registry_index_matches_registry(output)
        && blockhash_registry_valid(output)
        && optional_blockhash_v3_valid(output)
}

fn directory_contains_only_legacy_entries(path: &Path, allowed_files: &[&str]) -> bool {
    let Ok(entries) = fs::read_dir(path) else {
        return false;
    };
    entries.into_iter().all(|entry| {
        let Ok(entry) = entry else {
            return false;
        };
        let Ok(kind) = entry.file_type() else {
            return false;
        };
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return false;
        };
        if kind.is_file() {
            return allowed_files.contains(&name.as_str());
        }
        if kind.is_dir() && name == LEGACY_BLOCKHASH_LOCK_DIR {
            // Two legacy outputs retain this old lock directory. It is safe
            // migration metadata only when it is still empty; nested files,
            // symlinks, and every other directory remain a hard rejection.
            return fs::read_dir(entry.path()).is_ok_and(|mut nested| nested.next().is_none());
        }
        false
    })
}

fn previous_tail_valid(output: &Path) -> bool {
    let bytes = file_len(&output.join(PREVIOUS_BLOCKHASH_TAIL_FILE));
    bytes > 0 && bytes.is_multiple_of(40)
}

fn legacy_registry_only_shape(output: &Path) -> bool {
    legacy_reusable_sidecars_valid(output)
        && (!output.join(PREVIOUS_BLOCKHASH_TAIL_FILE).exists() || previous_tail_valid(output))
        // Seven otherwise registry-only legacy directories contain a stale
        // standalone PoH file. build_hot_blocks opens PoH with File::create,
        // so this one known generated file is safe to overwrite in place.
        && directory_contains_only_legacy_entries(
            output,
            &[
                REGISTRY_FILE,
                REGISTRY_COUNTS_FILE,
                REGISTRY_INDEX_FILE,
                BLOCKHASH_REGISTRY_FILE,
                BLOCKHASH_INDEX_V3_FILE,
                PREVIOUS_BLOCKHASH_TAIL_FILE,
                POH_FILE,
            ],
        )
}

fn legacy_owned_retry_shape(output: &Path) -> bool {
    legacy_reusable_sidecars_valid(output)
        && (!output.join(PREVIOUS_BLOCKHASH_TAIL_FILE).exists() || previous_tail_valid(output))
        && directory_contains_only_legacy_entries(
            output,
            &[
                OWNERSHIP_MARKER,
                REGISTRY_FILE,
                REGISTRY_COUNTS_FILE,
                REGISTRY_INDEX_FILE,
                BLOCKHASH_REGISTRY_FILE,
                BLOCKHASH_INDEX_V3_FILE,
                PREVIOUS_BLOCKHASH_TAIL_FILE,
                META_FILE,
                BLOCKS_FILE,
                BLOCK_INDEX_FILE,
                "archive-v2-blocks.index.tmp",
                POH_FILE,
                SHREDDING_FILE,
                SIGNATURES_FILE,
                VOTE_HASH_REGISTRY_FILE,
            ],
        )
}

fn legacy_compact_previous_car(config: &NasPipelineConfig, epoch: u64) -> Option<PathBuf> {
    let previous = epoch.checked_sub(1)?;
    // --previous-car is also the predecessor-epoch hint used by Blockzilla's
    // sidecar lookup. A completed predecessor's CAR may already be deleted;
    // the dependency gate below guarantees the hinted path is never opened as
    // a fallback when it is synthetic.
    Some(
        car_path(&config.car_root, previous)
            .unwrap_or_else(|| config.car_root.join(format!("epoch-{previous}.car.zst"))),
    )
}

fn predecessor_seed_sidecars_usable(config: &NasPipelineConfig, epoch: u64) -> bool {
    let Some(previous) = epoch.checked_sub(1) else {
        return true;
    };
    let output = config.archive_root.join(format!("epoch-{previous}"));
    let ownership_path = output.join(OWNERSHIP_MARKER);
    if let Some(owner) = read_ownership(&output) {
        // Generated reader files can become structurally complete just before
        // their child exits. Only the controller's completed commit is a safe
        // predecessor boundary; active and failed pipeline-owned predecessors
        // must not release their successor. A successor with its own durable
        // prev tail bypasses this function in legacy_compact_dependency_ready.
        if owner.id != previous.to_string()
            || owner.state != "complete"
            || !matches!(
                owner.kind.as_str(),
                "historical_scan" | "historical_finalizer" | LEGACY_COMPACT_OWNERSHIP_KIND
            )
        {
            return false;
        }
    } else if fs::symlink_metadata(&ownership_path).is_ok() {
        // A present-but-unreadable marker is not the same thing as an old
        // unowned archive. Fail closed so corruption cannot release a
        // successor while predecessor state is unknown.
        return false;
    }
    historical_archive_core_complete(&output, false)
        && blockhash_registry_valid(&output)
        && (hot_block_index_matches_blob_and_blockhashes(&output)
            || is_nonempty_file(&output.join(POH_FILE)))
}

fn legacy_compact_dependency_ready(config: &NasPipelineConfig, epoch: u64) -> bool {
    if epoch == 0 {
        return true;
    }
    previous_tail_valid(&config.archive_root.join(format!("epoch-{epoch}")))
        || predecessor_seed_sidecars_usable(config, epoch)
}

fn legacy_compact_reuse_status(config: &NasPipelineConfig, epoch: u64) -> LegacyCompactReuseStatus {
    if car_paths_ambiguous(&config.car_root, epoch) || car_path(&config.car_root, epoch).is_none() {
        return LegacyCompactReuseStatus::NotCandidate;
    }
    let output = config.archive_root.join(format!("epoch-{epoch}"));
    let safe_shape = match read_ownership(&output) {
        None => legacy_registry_only_shape(&output),
        Some(owner) => {
            owner.kind == LEGACY_COMPACT_OWNERSHIP_KIND
                && owner.id == epoch.to_string()
                && owner.state == "retry_ready"
                && owner.pid.is_none()
                && legacy_owned_retry_shape(&output)
        }
    };
    if !safe_shape {
        return LegacyCompactReuseStatus::NotCandidate;
    }
    if legacy_compact_dependency_ready(config, epoch) {
        LegacyCompactReuseStatus::Ready
    } else {
        LegacyCompactReuseStatus::WaitingForPrevious
    }
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
    legacy_compact_reuse_complete: bool,
    legacy_compact_managed: bool,
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

    // This migration lane opens/decompresses the CAR once and validates every
    // decoded block against the adopted blockhash registry while compacting.
    let preflight_applicable =
        config.preflight_car && !scan_already_durable && !legacy_compact_managed;
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
        message: if legacy_compact_managed {
            Some(
                "bypassed for legacy compact/reuse; the one-pass builder validates the CAR while processing"
                    .to_string(),
            )
        } else if preflight_state == ArtifactState::Invalid {
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
            && !legacy_no_access_complete
            && !legacy_compact_reuse_complete);
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
            legacy_compact_reuse_complete,
        ),
        access_artifact(
            ArtifactKind::BlockAccessIndex,
            output.join(BLOCK_ACCESS_INDEX_FILE),
            !config.no_access,
            access_outputs_required,
            legacy_no_access_complete,
            legacy_compact_reuse_complete,
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
    legacy_compact_reuse_complete: bool,
) -> ArtifactSnapshot {
    if legacy_compact_reuse_complete {
        return ArtifactSnapshot {
            kind,
            state: ArtifactState::NotApplicable,
            requirement: ArtifactRequirement::Optional,
            required_now: false,
            bytes: file_len(&path),
            modified_unix_secs: modified_unix_secs(&path),
            message: Some(
                "legacy compact/reuse is intentionally committed as a light reader core without block-access"
                    .to_string(),
            ),
        };
    }
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

fn hot_block_index_matches_blob_and_blockhashes(path: &Path) -> bool {
    let index_path = path.join(BLOCK_INDEX_FILE);
    let Some(header) = read_fixed_header::<HOT_BLOCK_INDEX_HEADER_LEN>(&index_path) else {
        return false;
    };
    let rows = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let blob_bytes = u64::from_le_bytes(header[20..28].try_into().unwrap());
    let actual_blob_bytes = file_len(&path.join(BLOCKS_FILE));
    rows > 0
        && header[..8] == HOT_BLOCK_INDEX_MAGIC[..]
        && u16::from_le_bytes(header[8..10].try_into().unwrap()) == HOT_BLOCK_INDEX_VERSION
        && file_len(&index_path)
            == (HOT_BLOCK_INDEX_HEADER_LEN as u64)
                .saturating_add(rows.saturating_mul(HOT_BLOCK_INDEX_ROW_LEN))
        && blob_bytes > 0
        && blob_bytes == actual_blob_bytes
        && rows == file_len(&path.join(BLOCKHASH_REGISTRY_FILE)) / 32
}

fn legacy_compact_reader_complete(path: &Path) -> bool {
    historical_archive_core_complete(path, false)
        && legacy_reusable_sidecars_valid(path)
        && hot_block_index_matches_blob_and_blockhashes(path)
        && [BLOCK_ACCESS_FILE, BLOCK_ACCESS_INDEX_FILE]
            .iter()
            .all(|name| !path.join(name).exists())
}

fn pipeline_owned_legacy_compact_complete(path: &Path) -> bool {
    read_ownership(path).is_some_and(|owner| {
        owner.kind == LEGACY_COMPACT_OWNERSHIP_KIND
            && owner.state == "complete"
            && legacy_compact_reader_complete(path)
    })
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
    if structurally_valid_registry_key_count(&path.join(REGISTRY_FILE)).is_none() {
        return RegistryOrder::Unknown;
    }
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
        && registry_index_matches_registry(path)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LegacyCompactResourceCapacity {
    cpu_slots: usize,
    io_slots: usize,
    effective_slots: usize,
}

fn legacy_compact_resource_capacity(config: &NasPipelineConfig) -> LegacyCompactResourceCapacity {
    let cpu_slots = usize::try_from(
        config
            .legacy_compact_cpu_budget_cores
            .checked_div(config.legacy_compact_cpu_cores_per_worker)
            .unwrap_or_default(),
    )
    .unwrap_or(usize::MAX);
    let io_slots = usize::try_from(
        config
            .legacy_compact_io_budget_mib_per_sec
            .checked_div(config.legacy_compact_io_mib_per_sec_per_worker)
            .unwrap_or_default(),
    )
    .unwrap_or(usize::MAX);
    LegacyCompactResourceCapacity {
        cpu_slots,
        io_slots,
        effective_slots: config
            .legacy_compact_concurrency
            .min(cpu_slots)
            .min(io_slots),
    }
}

fn legacy_compact_resource_blocked_reason(
    config: &NasPipelineConfig,
    capacity: LegacyCompactResourceCapacity,
) -> Option<String> {
    (capacity.effective_slots < config.legacy_compact_concurrency).then(|| {
        format!(
            "legacy compact resource admission capped: effective={} hard_cap={} cpu_slots={} (budget={} cores, reservation={} cores/worker) io_slots={} (budget={} MiB/s, reservation={} MiB/s/worker)",
            capacity.effective_slots,
            config.legacy_compact_concurrency,
            capacity.cpu_slots,
            config.legacy_compact_cpu_budget_cores,
            config.legacy_compact_cpu_cores_per_worker,
            capacity.io_slots,
            config.legacy_compact_io_budget_mib_per_sec,
            config.legacy_compact_io_mib_per_sec_per_worker,
        )
    })
}

fn legacy_compact_memory_reservation_bytes(output: &Path) -> u64 {
    let sidecars = [
        REGISTRY_FILE,
        REGISTRY_INDEX_FILE,
        REGISTRY_COUNTS_FILE,
        BLOCKHASH_REGISTRY_FILE,
    ]
    .iter()
    .fold(0u64, |sum, name| {
        sum.saturating_add(file_len(&output.join(name)))
    });
    sidecars
        .saturating_add(LEGACY_COMPACT_MEMORY_OVERHEAD_MIB.saturating_mul(1024 * 1024))
        .max(LEGACY_COMPACT_MIN_MEMORY_MIB.saturating_mul(1024 * 1024))
}

#[derive(Debug)]
struct LegacyCompactAdmission {
    disk_headroom: Option<u64>,
    memory_headroom: Option<u64>,
}

impl LegacyCompactAdmission {
    fn new(
        config: &NasPipelineConfig,
        machine: &MachineSnapshot,
        epochs: &[EpochSnapshot],
        active_rss: &BTreeMap<u64, u64>,
    ) -> Self {
        let active_disk_growth = active_rss.keys().fold(0u64, |sum, epoch| {
            sum.saturating_add(
                epochs
                    .iter()
                    .find(|candidate| candidate.epoch == *epoch)
                    .map(scan_remaining_disk_projection)
                    .unwrap_or(MIN_SCAN_OUTPUT_PROJECTION_BYTES),
            )
        });
        let active_memory_growth = active_rss.iter().fold(0u64, |sum, (epoch, rss)| {
            let output = epochs
                .iter()
                .find(|candidate| candidate.epoch == *epoch)
                .map(|candidate| candidate.output_path.clone())
                .unwrap_or_else(|| config.archive_root.join(format!("epoch-{epoch}")));
            // MemAvailable already excludes current RSS. Reserve only the
            // lane's possible future growth; swap is deliberately not treated
            // as admission headroom.
            sum.saturating_add(
                legacy_compact_memory_reservation_bytes(&output).saturating_sub(*rss),
            )
        });
        let disk_reserve = config.disk_reserve_gib.saturating_mul(1024 * 1024 * 1024);
        let memory_reserve = config.memory_reserve_mib.saturating_mul(1024 * 1024);
        Self {
            disk_headroom: (machine.disk_total_bytes > 0).then(|| {
                machine
                    .disk_available_bytes
                    .saturating_sub(disk_reserve)
                    .saturating_sub(active_disk_growth)
            }),
            memory_headroom: (machine.memory_total_bytes > 0).then(|| {
                machine
                    .memory_available_bytes
                    .saturating_sub(memory_reserve)
                    .saturating_sub(active_memory_growth)
            }),
        }
    }

    fn blocked_reason(
        &self,
        config: &NasPipelineConfig,
        machine: &MachineSnapshot,
        epoch: &EpochSnapshot,
    ) -> Option<String> {
        let projected_disk = scan_remaining_disk_projection(epoch);
        if self
            .disk_headroom
            .is_none_or(|headroom| headroom < projected_disk)
        {
            return Some(format!(
                "legacy compact/reuse disk admission blocked: available {:.1} GiB, next remaining output {:.1} GiB, reserve {} GiB",
                machine.disk_available_bytes as f64 / 1024f64.powi(3),
                projected_disk as f64 / 1024f64.powi(3),
                config.disk_reserve_gib,
            ));
        }
        let estimate = legacy_compact_memory_reservation_bytes(&epoch.output_path);
        if self
            .memory_headroom
            .is_none_or(|headroom| headroom < estimate)
        {
            return Some(format!(
                "legacy compact/reuse memory admission blocked: available {:.1} MiB, next reservation {:.1} MiB, reserve {} MiB",
                machine.memory_available_bytes as f64 / 1024f64.powi(2),
                estimate as f64 / 1024f64.powi(2),
                config.memory_reserve_mib,
            ));
        }
        None
    }

    fn reserve(&mut self, epoch: &EpochSnapshot) {
        let projected_disk = scan_remaining_disk_projection(epoch);
        let estimate = legacy_compact_memory_reservation_bytes(&epoch.output_path);
        if let Some(headroom) = &mut self.disk_headroom {
            *headroom = headroom.saturating_sub(projected_disk);
        }
        if let Some(headroom) = &mut self.memory_headroom {
            *headroom = headroom.saturating_sub(estimate);
        }
    }
}

fn legacy_compact_capacity_admission(
    config: &NasPipelineConfig,
    machine: &MachineSnapshot,
    epochs: &[EpochSnapshot],
    active_rss: &BTreeMap<u64, u64>,
    failures: &BTreeMap<String, String>,
) -> (usize, Option<String>) {
    let resource = legacy_compact_resource_capacity(config);
    let mut admitted = active_rss.len().min(resource.effective_slots);
    let mut admission = LegacyCompactAdmission::new(config, machine, epochs, active_rss);
    let mut first_blocked = None;
    for epoch in epochs.iter().filter(|epoch| {
        legacy_compact_reuse_status(config, epoch.epoch) == LegacyCompactReuseStatus::Ready
    }) {
        if admitted >= resource.effective_slots {
            break;
        }
        if active_rss.contains_key(&epoch.epoch) {
            continue;
        }
        let failure_key = format!("compact_reuse:{}", epoch.epoch);
        if failures.contains_key(&failure_key) {
            first_blocked.get_or_insert_with(|| {
                format!(
                    "legacy compact/reuse epoch {} requires explicit retry after failure",
                    epoch.epoch
                )
            });
            continue;
        }
        if let Some(reason) = admission.blocked_reason(config, machine, epoch) {
            first_blocked.get_or_insert(reason);
            continue;
        }
        admission.reserve(epoch);
        admitted = admitted.saturating_add(1);
    }
    let resource_reason = legacy_compact_resource_blocked_reason(config, resource);
    let blocked_reason = if admitted < resource.effective_slots {
        first_blocked.or(resource_reason)
    } else {
        resource_reason.or(first_blocked)
    };
    (admitted, blocked_reason)
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
        summary.blocks_per_sec += epoch.progress.blocks_per_sec.unwrap_or(0.0);
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
    let progress_path = [
        capture_dir.join("progress.json"),
        capture_dir.join("journal/progress.json"),
    ]
    .into_iter()
    .find(|path| path.is_file());
    let mut progress = progress_path
        .as_deref()
        .and_then(read_progress)
        .unwrap_or_default();
    let journal = read_live_journal_tail(&capture_dir.join("journal/grpc-blocks.jsonl"));
    let epoch = progress
        .last_slot
        .map(|slot| slot / SLOTS_PER_EPOCH)
        .or_else(|| journal.as_ref().and_then(|row| json_u64(row, &["epoch"])))
        .or_else(|| parse_epoch_name(&id));
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
        progress.rss_bytes = process_rss_bytes(pid);
    }
    if progress.first_slot.is_none() {
        progress.first_slot = journal
            .as_ref()
            .and_then(|row| json_u64(row, &["first_slot"]));
    }
    if progress.last_slot.is_none() {
        progress.last_slot = journal
            .as_ref()
            .and_then(|row| json_u64(row, &["slot", "last_slot"]));
    }
    if progress.blocks_done == 0 {
        progress.blocks_done = journal
            .as_ref()
            .and_then(|row| json_u64(row, &["block_id"]))
            .map_or(0, |block_id| block_id.saturating_add(1));
    }
    let progress_active =
        progress_is_alive(&progress, now) && progress.state.as_deref() != Some("complete");

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
    let updated_unix_secs = progress
        .updated_unix_secs
        .or_else(|| modified_unix_secs(&capture_dir))
        .unwrap_or(now);
    let artifacts = live_capture_artifacts(config, &capture_dir, output_path.as_deref(), state);
    LiveCaptureSnapshot {
        id,
        epoch,
        state,
        capture_dir,
        output_path,
        ready_to_package: ready,
        repair_gate: finalize_needed && !ready,
        first_slot: progress.first_slot,
        last_slot: progress.last_slot,
        blocks_written: progress.blocks_done,
        artifacts,
        progress,
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
            false,
        ),
        access_artifact(
            ArtifactKind::BlockAccessIndex,
            output.join(BLOCK_ACCESS_INDEX_FILE),
            !config.no_access,
            state == LiveState::Complete,
            false,
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
    lanes.extend(
        runtime
            .legacy_compacts
            .iter()
            .map(|(epoch, compact)| lane_from_child(compact, Some(*epoch), None, now, runtime)),
    );
    lanes.extend(
        runtime
            .adopted_legacy_compacts
            .values()
            .map(|compact| lane_from_adopted_legacy(compact, now, runtime)),
    );
    if let Some(finalizer) = runtime.finalizer.as_ref() {
        let (epoch, capture_id) = match &finalizer.kind {
            ChildKind::CarDownload { epoch, .. } | ChildKind::CarPreflight { epoch, .. } => {
                (Some(*epoch), None)
            }
            ChildKind::HistoricalFinalizer { epoch } => (Some(*epoch), None),
            ChildKind::LiveFinalizer { id, epoch, .. } => (*epoch, Some(id.clone())),
            ChildKind::HistoricalScan { epoch } | ChildKind::HistoricalCompactReuse { epoch } => {
                (Some(*epoch), None)
            }
        };
        lanes.push(lane_from_child(finalizer, epoch, capture_id, now, runtime));
    }
    lanes
}

fn lane_from_adopted_legacy(
    compact: &AdoptedLegacyCompact,
    now: u64,
    runtime: &RuntimeState,
) -> LaneSnapshot {
    let key = format!("compact_reuse:{}", compact.epoch);
    let mut progress = read_progress(&compact.progress_path).unwrap_or_default();
    let rss_bytes = process_rss_bytes(compact.pid);
    progress.pid = Some(compact.pid);
    progress.rss_bytes = rss_bytes;
    progress.phase = Some("compact_reuse".to_string());
    progress.state = Some(if runtime.paused_jobs.contains(&key) {
        "paused".to_string()
    } else {
        "running".to_string()
    });
    LaneSnapshot {
        id: key.clone(),
        kind: "historical_compact_reuse".to_string(),
        epoch: Some(compact.epoch),
        capture_id: None,
        phase: "compact_reuse".to_string(),
        state: if runtime.paused_jobs.contains(&key) {
            "paused".to_string()
        } else {
            "running".to_string()
        },
        auto_paused: false,
        auto_pause_reason: None,
        pid: Some(compact.pid),
        progress,
        rss_bytes,
        started_unix_secs: None,
        updated_unix_secs: now,
    }
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
        ChildKind::HistoricalCompactReuse { .. } => ("historical_compact_reuse", "compact_reuse"),
        ChildKind::HistoricalFinalizer { .. } => ("historical_finalizer", "finalize"),
        ChildKind::LiveFinalizer { phase, .. } => ("live_finalizer", phase.as_str()),
    };
    let key = child.kind.key();
    let auto_pause = match child.kind {
        ChildKind::HistoricalCompactReuse { epoch } => runtime.auto_paused_legacy.get(&epoch),
        _ => None,
    };
    let paused = runtime.paused_jobs.contains(&key) || auto_pause.is_some();
    if paused {
        progress.state = Some("paused".to_string());
    }
    LaneSnapshot {
        id: key.clone(),
        kind: kind.to_string(),
        epoch,
        capture_id,
        phase: progress.phase.clone().unwrap_or_else(|| phase.to_string()),
        state: if paused {
            "paused".to_string()
        } else {
            "running".to_string()
        },
        auto_paused: auto_pause.is_some(),
        auto_pause_reason: auto_pause.map(|record| record.reason.clone()),
        pid: child.pid,
        progress,
        rss_bytes,
        started_unix_secs: Some(child.started_unix_secs),
        updated_unix_secs: now,
    }
}

fn sampled_legacy_compact_rss(
    lanes: &[LaneSnapshot],
    runtime: &RuntimeState,
) -> BTreeMap<u64, u64> {
    let mut active = BTreeMap::new();
    for lane in lanes
        .iter()
        .filter(|lane| lane.kind == "historical_compact_reuse")
    {
        let Some(epoch) = lane.epoch else {
            continue;
        };
        let rss = lane
            .rss_bytes
            .or(lane.progress.rss_bytes)
            .unwrap_or_default();
        active.insert(epoch, rss);
    }
    // The machine's MemAvailable value was sampled after snapshot.lanes. Do
    // not mix a newer /proc RSS sample into that older capacity snapshot: it
    // can double-subtract growth or admit against inconsistent observations.
    // A runtime-only child is conservatively treated as having consumed no
    // reservation yet, so its full estimate remains reserved.
    for epoch in runtime.legacy_compacts.keys() {
        active.entry(*epoch).or_insert(0);
    }
    for epoch in runtime.adopted_legacy_compacts.keys() {
        active.entry(*epoch).or_insert(0);
    }
    active
}

fn active_legacy_compact_rss(
    snapshot: &PipelineSnapshot,
    runtime: &RuntimeState,
) -> BTreeMap<u64, u64> {
    sampled_legacy_compact_rss(&snapshot.lanes, runtime)
}

fn active_ordinary_scan_count(snapshot: &PipelineSnapshot, runtime: &RuntimeState) -> usize {
    let mut active = runtime.scans.keys().copied().collect::<BTreeSet<_>>();
    active.extend(
        snapshot
            .lanes
            .iter()
            .filter(|lane| lane.kind == "historical_scan")
            .filter_map(|lane| lane.epoch),
    );
    active.len()
}

#[derive(Debug, Clone, PartialEq)]
enum LegacyPressureState {
    Pause(String),
    Hold,
    Resume,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LegacyAdaptiveDecision {
    Pause {
        epoch: u64,
        reason: String,
        cause: LegacyAutoPauseCause,
    },
    Resume {
        epoch: u64,
        reason: String,
        cause: LegacyAutoPauseCause,
    },
}

fn legacy_pressure_state(
    config: &NasPipelineConfig,
    machine: &MachineSnapshot,
) -> LegacyPressureState {
    let mib = 1024 * 1024;
    let pause_available = config
        .memory_reserve_mib
        .saturating_add(config.legacy_compact_memory_guard_mib)
        .saturating_mul(mib);
    let resume_available = config
        .memory_reserve_mib
        .saturating_add(config.legacy_compact_memory_guard_mib.saturating_mul(2))
        .saturating_mul(mib);
    let memory_known = machine.memory_total_bytes > 0;
    let memory_low = memory_known && machine.memory_available_bytes < pause_available;
    if memory_low {
        return LegacyPressureState::Pause(format!(
            "MemAvailable {:.1} MiB is below pause threshold {} MiB; SIGSTOP arrests growth but RSS remains fully reserved",
            machine.memory_available_bytes as f64 / mib as f64,
            pause_available / mib,
        ));
    }

    let memory_recovered = !memory_known || machine.memory_available_bytes >= resume_available;
    // System-wide PSI is retained as telemetry, but it is not a lane-saturation
    // signal. An unrelated task can raise it, and our A/B evidence showed it
    // moving down while aggregate useful throughput moved up.
    if memory_recovered {
        LegacyPressureState::Resume
    } else {
        LegacyPressureState::Hold
    }
}

fn legacy_adaptive_cooldown_elapsed(
    config: &NasPipelineConfig,
    last_action_unix_secs: u64,
    now: u64,
) -> bool {
    last_action_unix_secs == 0
        || now.saturating_sub(last_action_unix_secs)
            >= config.legacy_compact_pause_cooldown.as_secs()
}

fn plan_legacy_adaptive_action(
    config: &NasPipelineConfig,
    machine: &MachineSnapshot,
    total_running: usize,
    managed_running_epochs: &[u64],
    auto_paused: &[AutoPausedLegacy],
    last_action_unix_secs: u64,
    now: u64,
) -> Option<LegacyAdaptiveDecision> {
    if !config.legacy_compact_auto_pause
        || !legacy_adaptive_cooldown_elapsed(config, last_action_unix_secs, now)
    {
        return None;
    }
    match legacy_pressure_state(config, machine) {
        LegacyPressureState::Pause(reason) => {
            if total_running <= config.legacy_compact_min_running {
                return None;
            }
            managed_running_epochs.iter().copied().max().map(|epoch| {
                LegacyAdaptiveDecision::Pause {
                    epoch,
                    reason,
                    cause: LegacyAutoPauseCause::Memory,
                }
            })
        }
        LegacyPressureState::Resume => auto_paused
            .iter()
            .min_by_key(|record| (record.paused_unix_secs, record.epoch))
            .map(|record| LegacyAdaptiveDecision::Resume {
                epoch: record.epoch,
                reason: "MemAvailable crossed the resume threshold".to_string(),
                cause: LegacyAutoPauseCause::Memory,
            }),
        LegacyPressureState::Hold => None,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum LegacyThroughputAction {
    Signal(LegacyAdaptiveDecision),
    Record(String),
    ConfirmedSaturation { epoch: u64, reason: String },
}

fn legacy_running_epochs(snapshot: &PipelineSnapshot) -> Vec<u64> {
    let mut epochs = snapshot
        .lanes
        .iter()
        .filter(|lane| lane.kind == "historical_compact_reuse" && lane.state.as_str() != "paused")
        .filter_map(|lane| lane.epoch)
        .collect::<Vec<_>>();
    epochs.sort_unstable();
    epochs.dedup();
    epochs
}

fn reset_legacy_throughput_window(
    runtime: &mut LegacyThroughputRuntime,
    running_epochs: Vec<u64>,
    counters: BTreeMap<u64, LegacyThroughputCounter>,
    now: u64,
) {
    runtime.counters = counters;
    runtime.window = (!running_epochs.is_empty()).then_some(LegacyThroughputWindow {
        running_epochs,
        started_unix_secs: now,
        blocks_done: 0,
        read_bytes: 0,
        read_bytes_observed: true,
    });
}

fn invalidate_legacy_throughput_sampling(runtime: &mut LegacyThroughputRuntime, now: u64) {
    runtime.probe_invalidated_by_memory |= matches!(
        runtime.probe,
        Some(
            LegacyThroughputProbe::Trial { .. }
                | LegacyThroughputProbe::Confirm { .. }
                | LegacyThroughputProbe::AuditPaused { .. }
                | LegacyThroughputProbe::AuditReloaded { .. }
        )
    );
    reset_legacy_throughput_window(runtime, Vec::new(), BTreeMap::new(), now);
    runtime.last_measurement = None;
    runtime.baseline = None;
}

fn observe_legacy_throughput(
    config: &NasPipelineConfig,
    snapshot: &PipelineSnapshot,
    runtime: &mut LegacyThroughputRuntime,
    now: u64,
) -> Option<LegacyThroughputMeasurement> {
    let running_epochs = legacy_running_epochs(snapshot);
    let mut counters = BTreeMap::new();
    for lane in snapshot
        .lanes
        .iter()
        .filter(|lane| lane.kind == "historical_compact_reuse" && lane.state.as_str() != "paused")
    {
        let (Some(epoch), Some(pid), Some(updated)) =
            (lane.epoch, lane.pid, lane.progress.updated_unix_secs)
        else {
            reset_legacy_throughput_window(runtime, Vec::new(), BTreeMap::new(), now);
            return None;
        };
        if now.saturating_sub(updated) > PROGRESS_STALE_SECS {
            reset_legacy_throughput_window(runtime, Vec::new(), BTreeMap::new(), now);
            return None;
        }
        counters.insert(
            epoch,
            LegacyThroughputCounter {
                pid,
                phase: lane.progress.phase.clone(),
                blocks_done: lane.progress.blocks_done,
                read_bytes: process_io_counter_bytes(pid, "read_bytes:"),
            },
        );
    }
    if counters.len() != running_epochs.len() || running_epochs.is_empty() {
        reset_legacy_throughput_window(runtime, Vec::new(), BTreeMap::new(), now);
        return None;
    }

    let Some(window) = runtime.window.as_mut() else {
        reset_legacy_throughput_window(runtime, running_epochs, counters, now);
        return None;
    };
    if window.running_epochs != running_epochs {
        reset_legacy_throughput_window(runtime, running_epochs, counters, now);
        return None;
    }

    let mut blocks_delta = 0u64;
    let mut read_delta = 0u64;
    let mut read_observed = true;
    for (epoch, current) in &counters {
        let Some(previous) = runtime.counters.get(epoch) else {
            reset_legacy_throughput_window(runtime, running_epochs, counters, now);
            return None;
        };
        if previous.pid != current.pid
            || previous.phase != current.phase
            || current.blocks_done < previous.blocks_done
        {
            reset_legacy_throughput_window(runtime, running_epochs, counters, now);
            return None;
        }
        blocks_delta = blocks_delta.saturating_add(current.blocks_done - previous.blocks_done);
        match (previous.read_bytes, current.read_bytes) {
            (Some(previous), Some(current)) if current >= previous => {
                read_delta = read_delta.saturating_add(current - previous);
            }
            _ => read_observed = false,
        }
    }
    runtime.counters = counters;
    window.blocks_done = window.blocks_done.saturating_add(blocks_delta);
    window.read_bytes = window.read_bytes.saturating_add(read_delta);
    window.read_bytes_observed &= read_observed;

    let elapsed = now.saturating_sub(window.started_unix_secs);
    if elapsed < config.legacy_compact_throughput_probe_window.as_secs() || elapsed == 0 {
        return None;
    }
    let measurement = LegacyThroughputMeasurement {
        running_epochs: window.running_epochs.clone(),
        started_unix_secs: window.started_unix_secs,
        ended_unix_secs: now,
        blocks_per_sec: window.blocks_done as f64 / elapsed as f64,
        read_mib_per_sec: window
            .read_bytes_observed
            .then_some(window.read_bytes as f64 / elapsed as f64 / 1024f64.powi(2)),
    };
    runtime.last_measurement = Some(measurement.clone());
    reset_legacy_throughput_window(runtime, running_epochs, runtime.counters.clone(), now);
    Some(measurement)
}

fn throughput_gain_pct(baseline: f64, candidate: f64) -> Option<f64> {
    (baseline > 0.0 && baseline.is_finite() && candidate.is_finite())
        .then_some((candidate / baseline - 1.0) * 100.0)
}

fn measurement_io_label(measurement: &LegacyThroughputMeasurement) -> String {
    measurement.read_mib_per_sec.map_or_else(
        || "physical read unavailable".to_string(),
        |rate| format!("physical read {rate:.1} MiB/s"),
    )
}

fn evaluate_legacy_throughput_probe(
    config: &NasPipelineConfig,
    snapshot: &PipelineSnapshot,
    runtime: &mut LegacyThroughputRuntime,
    auto_paused: &BTreeMap<u64, AutoPausedLegacy>,
    steady_audit_epoch: Option<u64>,
    now: u64,
) -> Option<LegacyThroughputAction> {
    let running_epochs = legacy_running_epochs(snapshot);
    if runtime.probe_invalidated_by_memory {
        let invalidated = runtime.probe.clone();
        runtime.probe_invalidated_by_memory = false;
        if !matches!(invalidated, Some(LegacyThroughputProbe::Saturated { .. })) {
            runtime.probe = None;
            runtime.baseline = None;
            runtime.next_audit_unix_secs =
                now.saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
            return match invalidated {
                Some(LegacyThroughputProbe::Confirm { trial_epoch, .. })
                | Some(LegacyThroughputProbe::AuditPaused { trial_epoch, .. }) => {
                    if auto_paused
                        .get(&trial_epoch)
                        .is_some_and(|record| record.cause == LegacyAutoPauseCause::ThroughputProbe)
                    {
                        Some(LegacyThroughputAction::Signal(
                            LegacyAdaptiveDecision::Resume {
                                epoch: trial_epoch,
                                reason: "throughput comparison was invalidated by memory pressure"
                                    .to_string(),
                                cause: LegacyAutoPauseCause::ThroughputProbe,
                            },
                        ))
                    } else {
                        Some(LegacyThroughputAction::Record(format!(
                            "cleared memory-invalidated throughput probe for compact_reuse:{trial_epoch}; automatic pause ownership was already gone or transferred"
                        )))
                    }
                }
                Some(LegacyThroughputProbe::Trial { trial_epoch, .. })
                | Some(LegacyThroughputProbe::AuditReloaded { trial_epoch, .. }) => {
                    Some(LegacyThroughputAction::Record(format!(
                        "discarded throughput comparison for compact_reuse:{trial_epoch} after memory pressure"
                    )))
                }
                Some(LegacyThroughputProbe::Saturated { .. }) | None => None,
            };
        }
    }
    let measurement = observe_legacy_throughput(config, snapshot, runtime, now);
    let probe = runtime.probe.clone();
    match probe {
        Some(LegacyThroughputProbe::Saturated {
            baseline: _,
            trial,
            confirmation,
            trial_epoch,
            retry_unix_secs,
        }) => {
            if !auto_paused.contains_key(&trial_epoch) {
                runtime.probe = None;
                runtime.baseline = None;
                return None;
            }
            let lane_set_changed = running_epochs != confirmation.running_epochs;
            let below_minimum = running_epochs.len() < config.legacy_compact_min_running;
            let retry_due = now >= retry_unix_secs;
            if lane_set_changed || below_minimum || retry_due {
                let recent_baseline = runtime
                    .last_measurement
                    .as_ref()
                    .filter(|measurement| {
                        measurement.running_epochs == running_epochs
                            && now.saturating_sub(measurement.ended_unix_secs)
                                <= config
                                    .legacy_compact_throughput_probe_window
                                    .as_secs()
                                    .saturating_add(config.poll_interval.as_secs())
                    })
                    .cloned();
                if retry_due && !lane_set_changed && !below_minimum && recent_baseline.is_none() {
                    // Do not compare a new B sample with the A2 captured one
                    // backoff ago. Stay paused until a fresh, stable A sample
                    // for the unchanged lane set is available.
                    return None;
                }
                let retry_baseline = recent_baseline.unwrap_or_else(|| confirmation.clone());
                runtime.probe = Some(LegacyThroughputProbe::Trial {
                    baseline: retry_baseline.clone(),
                    trial_epoch,
                });
                return Some(LegacyThroughputAction::Signal(
                    LegacyAdaptiveDecision::Resume {
                        epoch: trial_epoch,
                        reason: if running_epochs.len() < config.legacy_compact_min_running {
                            "running lane count fell below the configured minimum".to_string()
                        } else if running_epochs != confirmation.running_epochs {
                            "the confirmed ceiling lane set changed; re-probing with the available work"
                                .to_string()
                        } else {
                            format!(
                                "throughput ceiling backoff expired; re-probing after baseline {:.1}, trial {:.1} blocks/s",
                                retry_baseline.blocks_per_sec, trial.blocks_per_sec
                            )
                        },
                        cause: LegacyAutoPauseCause::ThroughputSaturated,
                    },
                ));
            }
        }
        Some(LegacyThroughputProbe::Trial {
            baseline,
            trial_epoch,
        }) => {
            if !running_epochs.contains(&trial_epoch) {
                runtime.probe = None;
                runtime.baseline = None;
                return None;
            }
            if let Some(trial) = measurement {
                let mut expected = baseline.running_epochs.clone();
                expected.push(trial_epoch);
                expected.sort_unstable();
                if trial.running_epochs != expected {
                    runtime.probe = None;
                    runtime.baseline = Some(trial);
                    runtime.next_audit_unix_secs = now
                        .saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                    return Some(LegacyThroughputAction::Record(format!(
                        "accepted compact_reuse:{trial_epoch} without a causal verdict because the comparison lane set changed"
                    )));
                }
                let gain = throughput_gain_pct(baseline.blocks_per_sec, trial.blocks_per_sec);
                if gain.is_some_and(|gain| gain >= config.legacy_compact_throughput_min_gain_pct) {
                    runtime.probe = None;
                    runtime.baseline = Some(trial.clone());
                    runtime.next_audit_unix_secs = now
                        .saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                    return Some(LegacyThroughputAction::Record(format!(
                        "accepted compact_reuse:{trial_epoch}: aggregate useful throughput {:.1} -> {:.1} blocks/s ({:+.1}%, {}; {})",
                        baseline.blocks_per_sec,
                        trial.blocks_per_sec,
                        gain.unwrap_or_default(),
                        measurement_io_label(&baseline),
                        measurement_io_label(&trial),
                    )));
                }
                let gain_label =
                    gain.map_or_else(|| "unavailable".to_string(), |gain| format!("{gain:+.1}%"));
                runtime.probe = Some(LegacyThroughputProbe::Confirm {
                    baseline: baseline.clone(),
                    trial: trial.clone(),
                    trial_epoch,
                });
                return Some(LegacyThroughputAction::Signal(
                    LegacyAdaptiveDecision::Pause {
                        epoch: trial_epoch,
                        reason: format!(
                            "A/B trial did not clear the {:.1}% aggregate gain floor: {:.1} -> {:.1} blocks/s ({gain_label}; {}; {}); pausing the added lane for the confirming A sample",
                            config.legacy_compact_throughput_min_gain_pct,
                            baseline.blocks_per_sec,
                            trial.blocks_per_sec,
                            measurement_io_label(&baseline),
                            measurement_io_label(&trial),
                        ),
                        cause: LegacyAutoPauseCause::ThroughputProbe,
                    },
                ));
            }
        }
        Some(LegacyThroughputProbe::Confirm {
            baseline,
            trial,
            trial_epoch,
        }) => {
            if !auto_paused.contains_key(&trial_epoch) {
                runtime.probe = None;
                runtime.baseline = None;
                return None;
            }
            if running_epochs != baseline.running_epochs {
                runtime.probe = None;
                runtime.baseline = None;
                runtime.next_audit_unix_secs =
                    now.saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                return Some(LegacyThroughputAction::Signal(
                    LegacyAdaptiveDecision::Resume {
                        epoch: trial_epoch,
                        reason:
                            "A/B/A confirmation invalidated because the comparison lane set changed"
                                .to_string(),
                        cause: LegacyAutoPauseCause::ThroughputProbe,
                    },
                ));
            }
            if let Some(confirmation) = measurement {
                let recovery =
                    throughput_gain_pct(trial.blocks_per_sec, confirmation.blocks_per_sec);
                let baseline_floor = baseline.blocks_per_sec
                    * (1.0 - config.legacy_compact_throughput_min_gain_pct / 100.0);
                let stopping_helped = recovery
                    .is_some_and(|gain| gain >= config.legacy_compact_throughput_min_gain_pct)
                    && confirmation.blocks_per_sec >= baseline_floor;
                if stopping_helped {
                    let retry_unix_secs = now
                        .saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                    runtime.probe = Some(LegacyThroughputProbe::Saturated {
                        baseline: baseline.clone(),
                        trial: trial.clone(),
                        confirmation: confirmation.clone(),
                        trial_epoch,
                        retry_unix_secs,
                    });
                    runtime.baseline = Some(confirmation.clone());
                    return Some(LegacyThroughputAction::ConfirmedSaturation {
                        epoch: trial_epoch,
                        reason: format!(
                            "confirmed throughput ceiling at {} lanes: A {:.1}, B {:.1}, A2 {:.1} blocks/s; stopping recovered {:+.1}% and A2 stayed above {:.1} blocks/s (retry at {retry_unix_secs})",
                            baseline.running_epochs.len(),
                            baseline.blocks_per_sec,
                            trial.blocks_per_sec,
                            confirmation.blocks_per_sec,
                            recovery.unwrap_or_default(),
                            baseline_floor,
                        ),
                    });
                }
                runtime.probe = None;
                runtime.baseline = None;
                runtime.next_audit_unix_secs =
                    now.saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                return Some(LegacyThroughputAction::Signal(
                    LegacyAdaptiveDecision::Resume {
                        epoch: trial_epoch,
                        reason: format!(
                            "A/B/A did not confirm saturation: A {:.1}, B {:.1}, A2 {:.1} blocks/s (stop recovery {}); keeping the added lane",
                            baseline.blocks_per_sec,
                            trial.blocks_per_sec,
                            confirmation.blocks_per_sec,
                            recovery.map_or_else(
                                || "unavailable".to_string(),
                                |gain| format!("{gain:+.1}%")
                            ),
                        ),
                        cause: LegacyAutoPauseCause::ThroughputProbe,
                    },
                ));
            }
        }
        Some(LegacyThroughputProbe::AuditPaused {
            loaded,
            trial_epoch,
        }) => {
            if !auto_paused
                .get(&trial_epoch)
                .is_some_and(|record| record.cause == LegacyAutoPauseCause::ThroughputProbe)
            {
                runtime.probe = None;
                runtime.baseline = None;
                return None;
            }
            let expected = loaded
                .running_epochs
                .iter()
                .copied()
                .filter(|epoch| *epoch != trial_epoch)
                .collect::<Vec<_>>();
            if running_epochs != expected {
                runtime.probe = None;
                runtime.baseline = None;
                runtime.next_audit_unix_secs =
                    now.saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                return Some(LegacyThroughputAction::Signal(
                    LegacyAdaptiveDecision::Resume {
                        epoch: trial_epoch,
                        reason: "steady-state N-1 audit was invalidated because the comparison lane set changed"
                            .to_string(),
                        cause: LegacyAutoPauseCause::ThroughputProbe,
                    },
                ));
            }
            if let Some(reduced) = measurement {
                let stop_gain = throughput_gain_pct(loaded.blocks_per_sec, reduced.blocks_per_sec);
                if stop_gain
                    .is_some_and(|gain| gain >= config.legacy_compact_throughput_min_gain_pct)
                {
                    runtime.probe = Some(LegacyThroughputProbe::AuditReloaded {
                        loaded: loaded.clone(),
                        reduced: reduced.clone(),
                        trial_epoch,
                    });
                    return Some(LegacyThroughputAction::Signal(
                        LegacyAdaptiveDecision::Resume {
                            epoch: trial_epoch,
                            reason: format!(
                                "steady-state N-1 audit improved aggregate throughput {:.1} -> {:.1} blocks/s ({:+.1}%); reloading the lane for B2 confirmation",
                                loaded.blocks_per_sec,
                                reduced.blocks_per_sec,
                                stop_gain.unwrap_or_default(),
                            ),
                            cause: LegacyAutoPauseCause::ThroughputProbe,
                        },
                    ));
                }
                runtime.probe = None;
                runtime.baseline = None;
                runtime.next_audit_unix_secs =
                    now.saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                return Some(LegacyThroughputAction::Signal(
                    LegacyAdaptiveDecision::Resume {
                        epoch: trial_epoch,
                        reason: format!(
                            "steady-state N-1 audit did not improve aggregate throughput: {:.1} -> {:.1} blocks/s ({}); retaining the lane",
                            loaded.blocks_per_sec,
                            reduced.blocks_per_sec,
                            stop_gain.map_or_else(
                                || "gain unavailable".to_string(),
                                |gain| format!("{gain:+.1}%")
                            ),
                        ),
                        cause: LegacyAutoPauseCause::ThroughputProbe,
                    },
                ));
            }
        }
        Some(LegacyThroughputProbe::AuditReloaded {
            loaded,
            reduced,
            trial_epoch,
        }) => {
            if !running_epochs.contains(&trial_epoch) {
                runtime.probe = None;
                runtime.baseline = None;
                return None;
            }
            if let Some(reloaded) = measurement {
                if reloaded.running_epochs != loaded.running_epochs {
                    runtime.probe = None;
                    runtime.baseline = Some(reloaded);
                    runtime.next_audit_unix_secs = now
                        .saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                    return Some(LegacyThroughputAction::Record(format!(
                        "discarded steady-state throughput audit for compact_reuse:{trial_epoch} because the B2 lane set changed"
                    )));
                }
                let repeated_stop_gain =
                    throughput_gain_pct(reloaded.blocks_per_sec, reduced.blocks_per_sec);
                if repeated_stop_gain
                    .is_some_and(|gain| gain >= config.legacy_compact_throughput_min_gain_pct)
                {
                    let retry_unix_secs = now
                        .saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                    runtime.probe = Some(LegacyThroughputProbe::Saturated {
                        baseline: reduced.clone(),
                        trial: reloaded.clone(),
                        confirmation: reduced.clone(),
                        trial_epoch,
                        retry_unix_secs,
                    });
                    runtime.baseline = Some(reduced.clone());
                    return Some(LegacyThroughputAction::Signal(
                        LegacyAdaptiveDecision::Pause {
                            epoch: trial_epoch,
                            reason: format!(
                                "steady-state B/A/B audit confirmed congestion at {} lanes: B {:.1}, A {:.1}, B2 {:.1} blocks/s; removing the lane recovered {:+.1}% on the repeated comparison (retry at {retry_unix_secs})",
                                loaded.running_epochs.len(),
                                loaded.blocks_per_sec,
                                reduced.blocks_per_sec,
                                reloaded.blocks_per_sec,
                                repeated_stop_gain.unwrap_or_default(),
                            ),
                            cause: LegacyAutoPauseCause::ThroughputSaturated,
                        },
                    ));
                }
                runtime.probe = None;
                runtime.baseline = Some(reloaded.clone());
                runtime.next_audit_unix_secs =
                    now.saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                return Some(LegacyThroughputAction::Record(format!(
                    "steady-state B/A/B audit did not repeat the stop benefit for compact_reuse:{trial_epoch}: A {:.1}, B2 {:.1} blocks/s ({})",
                    reduced.blocks_per_sec,
                    reloaded.blocks_per_sec,
                    repeated_stop_gain.map_or_else(
                        || "gain unavailable".to_string(),
                        |gain| format!("{gain:+.1}%")
                    ),
                )));
            }
        }
        None => {
            if let Some(measurement) = measurement {
                runtime.baseline = Some(measurement.clone());
                if runtime.next_audit_unix_secs == 0 {
                    runtime.next_audit_unix_secs = now
                        .saturating_add(config.legacy_compact_throughput_probe_backoff.as_secs());
                } else if now >= runtime.next_audit_unix_secs
                    && running_epochs.len() > config.legacy_compact_min_running
                    && auto_paused.is_empty()
                    && steady_audit_epoch.is_some_and(|epoch| running_epochs.contains(&epoch))
                {
                    let trial_epoch = steady_audit_epoch.unwrap_or_default();
                    runtime.probe = Some(LegacyThroughputProbe::AuditPaused {
                        loaded: measurement,
                        trial_epoch,
                    });
                    runtime.baseline = None;
                    return Some(LegacyThroughputAction::Signal(
                        LegacyAdaptiveDecision::Pause {
                            epoch: trial_epoch,
                            reason:
                                "starting periodic steady-state N→N-1→N aggregate-throughput audit"
                                    .to_string(),
                            cause: LegacyAutoPauseCause::ThroughputProbe,
                        },
                    ));
                }
            }
        }
    }
    None
}

fn record_legacy_adaptive_action(
    config: &NasPipelineConfig,
    runtime: &mut RuntimeState,
    at_unix_secs: u64,
    action: String,
) {
    runtime.legacy_last_adaptive_action_unix_secs = at_unix_secs;
    runtime.legacy_last_adaptive_action_reason = Some(action.clone());
    if let Err(error) = persist_control_state(config, runtime) {
        record_error(
            config,
            runtime,
            "legacy_auto_pause",
            format!("persist adaptive state: {error:#}"),
        );
    }
    if let Err(error) = append_control_event(config, "legacy_adaptive", &action) {
        record_error(
            config,
            runtime,
            "legacy_auto_pause",
            format!("append adaptive event: {error:#}"),
        );
    }
}

fn execute_legacy_adaptive_decision(
    config: &NasPipelineConfig,
    runtime: &mut RuntimeState,
    now: u64,
    decision: LegacyAdaptiveDecision,
) -> bool {
    match decision {
        LegacyAdaptiveDecision::Pause {
            epoch,
            reason,
            cause,
        } => {
            let Some(pid) = runtime
                .legacy_compacts
                .get(&epoch)
                .and_then(|child| child.pid)
            else {
                return false;
            };
            if process_cmdline_matches_legacy_exact(config, epoch, pid) != Some(true)
                || !process_is_group_leader(pid)
            {
                record_error(
                    config,
                    runtime,
                    "legacy_auto_pause",
                    format!("refused to pause compact_reuse:{epoch}: pid {pid} identity changed"),
                );
                return false;
            }
            let Some((_, process_start_ticks)) = process_stat_identity(pid) else {
                record_error(
                    config,
                    runtime,
                    "legacy_auto_pause",
                    format!(
                        "refused to pause compact_reuse:{epoch}: pid {pid} starttime unavailable"
                    ),
                );
                return false;
            };
            let action = format!("auto-paused compact_reuse:{epoch}: {reason}");
            let previous_last_at = runtime.legacy_last_adaptive_action_unix_secs;
            let previous_last_reason = runtime.legacy_last_adaptive_action_reason.clone();
            runtime.auto_paused_legacy.insert(
                epoch,
                AutoPausedLegacy {
                    epoch,
                    pid,
                    process_start_ticks: Some(process_start_ticks),
                    reason: reason.clone(),
                    paused_unix_secs: now,
                    cause,
                },
            );
            runtime.legacy_last_adaptive_action_unix_secs = now;
            runtime.legacy_last_adaptive_action_reason = Some(action.clone());
            // Persist intent before SIGSTOP. A crash after this point but
            // before the signal is harmless because startup SIGCONT is
            // idempotent; the inverse ordering could strand an unrecorded stop.
            if let Err(error) = persist_control_state(config, runtime) {
                runtime.auto_paused_legacy.remove(&epoch);
                runtime.legacy_last_adaptive_action_unix_secs = previous_last_at;
                runtime.legacy_last_adaptive_action_reason = previous_last_reason;
                record_error(
                    config,
                    runtime,
                    "legacy_auto_pause",
                    format!(
                        "refused to stop compact_reuse:{epoch}: persist pause intent: {error:#}"
                    ),
                );
                return false;
            }
            // SAFETY: this managed legacy child was spawned as an isolated
            // process-group leader and its exact argv was validated above.
            if unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGSTOP) } != 0 {
                let signal_error = std::io::Error::last_os_error();
                runtime.auto_paused_legacy.remove(&epoch);
                runtime.legacy_last_adaptive_action_unix_secs = previous_last_at;
                runtime.legacy_last_adaptive_action_reason = previous_last_reason;
                let rollback_error = persist_control_state(config, runtime).err();
                record_error(
                    config,
                    runtime,
                    "legacy_auto_pause",
                    format!(
                        "pause compact_reuse:{epoch} process group {pid}: {signal_error}; rollback persist={rollback_error:?}"
                    ),
                );
                return false;
            }
            if let Err(error) = append_control_event(config, "legacy_adaptive", &action) {
                record_error(
                    config,
                    runtime,
                    "legacy_auto_pause",
                    format!("append adaptive pause event: {error:#}"),
                );
            }
            true
        }
        LegacyAdaptiveDecision::Resume {
            epoch,
            reason,
            cause,
        } => {
            let Some(record) = runtime.auto_paused_legacy.get(&epoch).cloned() else {
                return false;
            };
            if runtime
                .paused_jobs
                .contains(&format!("compact_reuse:{epoch}"))
                || record.cause != cause
                || process_cmdline_matches_legacy_exact(config, epoch, record.pid) != Some(true)
                || !process_is_group_leader(record.pid)
            {
                return false;
            }
            // SAFETY: the auto-paused record can only be created for a managed
            // process-group leader after exact argv validation.
            if unsafe { libc::kill(-(record.pid as libc::pid_t), libc::SIGCONT) } != 0 {
                record_error(
                    config,
                    runtime,
                    "legacy_auto_pause",
                    format!(
                        "resume compact_reuse:{epoch} process group {}: {}",
                        record.pid,
                        std::io::Error::last_os_error()
                    ),
                );
                return false;
            }
            runtime.auto_paused_legacy.remove(&epoch);
            record_legacy_adaptive_action(
                config,
                runtime,
                now,
                format!("auto-resumed compact_reuse:{epoch}: {reason}"),
            );
            true
        }
    }
}

fn legacy_priority_drain_requested(
    config: &NasPipelineConfig,
    snapshot: &PipelineSnapshot,
    runtime: &RuntimeState,
) -> bool {
    let active_legacy_compacts = active_legacy_compact_rss(snapshot, runtime);
    if active_legacy_compacts.is_empty() {
        return false;
    }
    let adopted_active_scans = active_ordinary_scan_count(snapshot, runtime);
    let adopted_acquisitions = snapshot
        .epochs
        .iter()
        .filter(|epoch| {
            !runtime.acquisitions.contains_key(&epoch.epoch)
                && acquisition_claim_active(config, epoch.epoch)
        })
        .count();
    if !runtime.acquisitions.is_empty() || adopted_acquisitions > 0 || adopted_active_scans > 0 {
        return true;
    }

    let disk_reserve = config.disk_reserve_gib.saturating_mul(1024 * 1024 * 1024);
    let active_download_bytes = active_download_projection(config, &snapshot.epochs, runtime);
    let acquisition_car_disk_headroom = (snapshot.machine.car_disk_total_bytes > 0).then(|| {
        snapshot
            .machine
            .car_disk_available_bytes
            .saturating_sub(disk_reserve)
            .saturating_sub(active_download_bytes)
    });
    let mut post_legacy_machine = snapshot.machine.clone();
    let recoverable_legacy_rss = active_legacy_compacts
        .values()
        .copied()
        .fold(0u64, u64::saturating_add);
    post_legacy_machine.memory_available_bytes = post_legacy_machine
        .memory_available_bytes
        .saturating_add(recoverable_legacy_rss);
    if post_legacy_machine.memory_total_bytes > 0 {
        post_legacy_machine.memory_available_bytes = post_legacy_machine
            .memory_available_bytes
            .min(post_legacy_machine.memory_total_bytes);
    }
    let acquisition_slots_after_legacy_drain = config.download_concurrency.min(
        acquisition_memory_capacity(config, &post_legacy_machine, 0, 0),
    );
    let acquisition_admissible_after_legacy_drain = acquisition_slots_after_legacy_drain > 0
        && snapshot.epochs.iter().any(|epoch| {
            acquisition_action(config, epoch).is_some_and(|action| {
                action == AcquisitionAction::Preflight
                    || acquisition_car_disk_headroom.is_some_and(|headroom| {
                        headroom
                            >= car_download_remaining_projection(
                                config,
                                &snapshot.epochs,
                                epoch.epoch,
                            )
                    })
            })
        });
    let live_ready_pending = snapshot.finalizer_queue.iter().any(|task| {
        task.kind == "live" && finalizer_is_admissible(config, &post_legacy_machine, task)
    });
    live_ready_pending || acquisition_admissible_after_legacy_drain
}

fn plan_priority_drain_throughput_resume(runtime: &RuntimeState) -> Option<LegacyAdaptiveDecision> {
    runtime
        .auto_paused_legacy
        .values()
        .filter(|record| {
            matches!(
                record.cause,
                LegacyAutoPauseCause::ThroughputProbe | LegacyAutoPauseCause::ThroughputSaturated
            ) && !runtime
                .paused_jobs
                .contains(&format!("compact_reuse:{}", record.epoch))
        })
        .min_by_key(|record| (record.paused_unix_secs, record.epoch))
        .map(|record| LegacyAdaptiveDecision::Resume {
            epoch: record.epoch,
            reason: "higher-priority live/acquisition/scan work requested a legacy drain"
                .to_string(),
            cause: record.cause,
        })
}

fn adjust_legacy_workers_for_pressure(
    config: &NasPipelineConfig,
    snapshot: &PipelineSnapshot,
    runtime: &mut RuntimeState,
    now: u64,
) {
    if !config.legacy_compact_auto_pause {
        return;
    }
    let total_running = snapshot
        .lanes
        .iter()
        .filter(|lane| lane.kind == "historical_compact_reuse" && lane.state.as_str() != "paused")
        .count();
    let managed_running_epochs = runtime
        .legacy_compacts
        .iter()
        .filter_map(|(epoch, child)| {
            let key = format!("compact_reuse:{epoch}");
            (!runtime.paused_jobs.contains(&key)
                && !runtime.auto_paused_legacy.contains_key(epoch)
                && child.pid.is_some())
            .then_some(*epoch)
        })
        .collect::<Vec<_>>();
    let resumable_memory = runtime
        .auto_paused_legacy
        .values()
        .filter(|record| {
            record.cause == LegacyAutoPauseCause::Memory
                && !runtime
                    .paused_jobs
                    .contains(&format!("compact_reuse:{}", record.epoch))
                && runtime
                    .legacy_compacts
                    .get(&record.epoch)
                    .is_some_and(|child| child.pid == Some(record.pid))
        })
        .cloned()
        .collect::<Vec<_>>();
    if let Some(decision) = plan_legacy_adaptive_action(
        config,
        &snapshot.machine,
        total_running,
        &managed_running_epochs,
        &resumable_memory,
        runtime.legacy_last_adaptive_action_unix_secs,
        now,
    ) {
        invalidate_legacy_throughput_sampling(&mut runtime.legacy_throughput, now);
        let _ = execute_legacy_adaptive_decision(config, runtime, now, decision);
        return;
    }
    if !matches!(
        legacy_pressure_state(config, &snapshot.machine),
        LegacyPressureState::Resume
    ) {
        invalidate_legacy_throughput_sampling(&mut runtime.legacy_throughput, now);
        return;
    }

    if legacy_priority_drain_requested(config, snapshot, runtime) {
        let throughput_before = runtime.legacy_throughput.clone();
        runtime.legacy_throughput = LegacyThroughputRuntime::default();
        if let Some(decision) = plan_priority_drain_throughput_resume(runtime) {
            if !legacy_adaptive_cooldown_elapsed(
                config,
                runtime.legacy_last_adaptive_action_unix_secs,
                now,
            ) || !execute_legacy_adaptive_decision(config, runtime, now, decision)
            {
                runtime.legacy_throughput = throughput_before;
            }
        }
        return;
    }

    let throughput_before = runtime.legacy_throughput.clone();
    let steady_audit_epoch = managed_running_epochs.iter().copied().max();
    let action = evaluate_legacy_throughput_probe(
        config,
        snapshot,
        &mut runtime.legacy_throughput,
        &runtime.auto_paused_legacy,
        steady_audit_epoch,
        now,
    );
    match action {
        Some(LegacyThroughputAction::Signal(decision)) => {
            if !legacy_adaptive_cooldown_elapsed(
                config,
                runtime.legacy_last_adaptive_action_unix_secs,
                now,
            ) {
                runtime.legacy_throughput = throughput_before;
                return;
            }
            if !execute_legacy_adaptive_decision(config, runtime, now, decision) {
                runtime.legacy_throughput = throughput_before;
            }
        }
        Some(LegacyThroughputAction::Record(action)) => {
            record_legacy_adaptive_action(config, runtime, now, action);
        }
        Some(LegacyThroughputAction::ConfirmedSaturation { epoch, reason }) => {
            let Some(record) = runtime.auto_paused_legacy.get_mut(&epoch) else {
                runtime.legacy_throughput = throughput_before;
                return;
            };
            if record.cause != LegacyAutoPauseCause::ThroughputProbe {
                runtime.legacy_throughput = throughput_before;
                return;
            }
            record.cause = LegacyAutoPauseCause::ThroughputSaturated;
            record.reason = reason.clone();
            record_legacy_adaptive_action(config, runtime, now, reason);
        }
        None => {}
    }
}

async fn top_up_legacy_compacts(
    config: &NasPipelineConfig,
    snapshot: &PipelineSnapshot,
    runtime: &mut RuntimeState,
) -> usize {
    let active_rss = active_legacy_compact_rss(snapshot, runtime);
    let mut active_epochs = active_rss.keys().copied().collect::<BTreeSet<_>>();
    let mut admission =
        LegacyCompactAdmission::new(config, &snapshot.machine, &snapshot.epochs, &active_rss);
    let resource_capacity = legacy_compact_resource_capacity(config).effective_slots;
    let now = unix_now();
    if config.legacy_compact_auto_pause
        && (!matches!(
            legacy_pressure_state(config, &snapshot.machine),
            LegacyPressureState::Resume
        ) || !legacy_adaptive_cooldown_elapsed(
            config,
            runtime.legacy_last_adaptive_action_unix_secs,
            now,
        ))
    {
        return 0;
    }
    let running_epochs = legacy_running_epochs(snapshot);
    let probe_baseline = if config.legacy_compact_auto_pause
        && running_epochs.len() >= config.legacy_compact_min_running
    {
        if !runtime.auto_paused_legacy.is_empty() || runtime.legacy_throughput.probe.is_some() {
            return 0;
        }
        let Some(baseline) = runtime
            .legacy_throughput
            .baseline
            .as_ref()
            .filter(|baseline| baseline.running_epochs == running_epochs)
            .cloned()
        else {
            // Observe a stable aggregate baseline before adding load. The
            // next lane is then an explicit B sample, not an unmeasured ramp.
            return 0;
        };
        Some(baseline)
    } else {
        None
    };
    let start_limit = if config.legacy_compact_auto_pause {
        1
    } else {
        usize::MAX
    };
    let mut started = 0usize;
    for epoch in snapshot.epochs.iter().filter(|epoch| {
        legacy_compact_reuse_status(config, epoch.epoch) == LegacyCompactReuseStatus::Ready
    }) {
        if active_epochs.len() >= resource_capacity {
            break;
        }
        let failure_key = format!("compact_reuse:{}", epoch.epoch);
        if runtime.failures.contains_key(&failure_key)
            || active_epochs.contains(&epoch.epoch)
            || admission
                .blocked_reason(config, &snapshot.machine, epoch)
                .is_some()
        {
            // A large or otherwise blocked range head must not prevent a
            // smaller independent range from using the remaining envelope.
            continue;
        }
        match spawn_legacy_compact_reuse(config, epoch).await {
            Ok(child) => {
                runtime.legacy_compacts.insert(epoch.epoch, child);
                active_epochs.insert(epoch.epoch);
                admission.reserve(epoch);
                started = started.saturating_add(1);
                if config.legacy_compact_auto_pause {
                    if let Some(baseline) = probe_baseline.clone() {
                        runtime.legacy_throughput.baseline = None;
                        runtime.legacy_throughput.probe = Some(LegacyThroughputProbe::Trial {
                            baseline,
                            trial_epoch: epoch.epoch,
                        });
                    } else {
                        runtime.legacy_throughput.baseline = None;
                    }
                    record_legacy_adaptive_action(
                        config,
                        runtime,
                        now,
                        if probe_baseline.is_some() {
                            format!(
                                "started compact_reuse:{} as an aggregate-throughput B probe",
                                epoch.epoch
                            )
                        } else {
                            format!(
                                "started compact_reuse:{} to reach the configured minimum running lanes",
                                epoch.epoch
                            )
                        },
                    );
                }
            }
            Err(error) => {
                // Treat spawn failures like every other child failure, but do
                // not impose head-of-line blocking on independent ranges.
                let message = format!("{failure_key} spawn failed: {error:#}");
                set_runtime_failure(config, runtime, failure_key, message.clone());
                record_error(config, runtime, "legacy_compact", message);
            }
        }
        if started >= start_limit {
            break;
        }
    }
    started
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
    let active_legacy_compacts = active_legacy_compact_rss(snapshot, runtime);
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

    let adopted_active_scans = active_ordinary_scan_count(snapshot, runtime);
    let adopted_acquisitions = snapshot
        .epochs
        .iter()
        .filter(|epoch| {
            !runtime.acquisitions.contains_key(&epoch.epoch)
                && acquisition_claim_active(config, epoch.epoch)
        })
        .count();
    let acquisition_candidates = snapshot
        .epochs
        .iter()
        .filter_map(|epoch| acquisition_action(config, epoch).map(|action| (epoch, action)))
        .collect::<Vec<_>>();
    let active_acquisition_count = runtime
        .acquisitions
        .len()
        .saturating_add(adopted_acquisitions);
    let acquisition_rss_bytes = snapshot
        .lanes
        .iter()
        .filter(|lane| matches!(lane.kind.as_str(), "car_download" | "car_preflight"))
        .filter_map(|lane| lane.rss_bytes)
        .sum();
    let acquisition_slots = config
        .download_concurrency
        .saturating_sub(active_acquisition_count)
        .min(acquisition_memory_capacity(
            config,
            &snapshot.machine,
            active_acquisition_count,
            acquisition_rss_bytes,
        ));
    let disk_reserve = config.disk_reserve_gib.saturating_mul(1024 * 1024 * 1024);
    let active_download_bytes = active_download_projection(config, &snapshot.epochs, runtime);
    let acquisition_car_disk_headroom = (snapshot.machine.car_disk_total_bytes > 0).then(|| {
        snapshot
            .machine
            .car_disk_available_bytes
            .saturating_sub(disk_reserve)
            .saturating_sub(active_download_bytes)
    });
    let mut post_legacy_machine = snapshot.machine.clone();
    let recoverable_legacy_rss = active_legacy_compacts
        .values()
        .copied()
        .fold(0u64, u64::saturating_add);
    post_legacy_machine.memory_available_bytes = post_legacy_machine
        .memory_available_bytes
        .saturating_add(recoverable_legacy_rss);
    if post_legacy_machine.memory_total_bytes > 0 {
        post_legacy_machine.memory_available_bytes = post_legacy_machine
            .memory_available_bytes
            .min(post_legacy_machine.memory_total_bytes);
    }
    let acquisition_slots_after_legacy_drain = config.download_concurrency.min(
        acquisition_memory_capacity(config, &post_legacy_machine, 0, 0),
    );
    let acquisition_admissible_after_legacy_drain = acquisition_slots_after_legacy_drain > 0
        && acquisition_candidates.iter().any(|(epoch, action)| {
            *action == AcquisitionAction::Preflight
                || acquisition_car_disk_headroom.is_some_and(|headroom| {
                    headroom
                        >= car_download_remaining_projection(config, &snapshot.epochs, epoch.epoch)
                })
        });
    let live_ready_pending = snapshot.finalizer_queue.iter().any(|task| {
        task.kind == "live" && finalizer_is_admissible(config, &post_legacy_machine, task)
    });

    // Legacy lanes may run together, but remain mutually exclusive with every
    // acquisition, ordinary scan, and finalizer class. Once higher-priority
    // live/acquisition work appears, stop topping up and let the current set
    // drain so that priority work cannot be starved by a continuously refilled
    // legacy queue.
    if !active_legacy_compacts.is_empty() {
        if runtime.acquisitions.is_empty()
            && adopted_acquisitions == 0
            && adopted_active_scans == 0
            && !live_ready_pending
            && !acquisition_admissible_after_legacy_drain
        {
            top_up_legacy_compacts(config, snapshot, runtime).await;
        }
        return Ok(());
    }

    // A closed capture with explicit READY approval already had live priority
    // before the sweep policy. Preserve that narrow exception, but never overlap
    // it with an acquisition or scan. Historical finalizers remain deferred.
    if let Some(live_task) = snapshot
        .finalizer_queue
        .iter()
        .filter(|task| task.kind == "live")
        .find(|task| finalizer_is_admissible(config, &snapshot.machine, task))
    {
        if !runtime.acquisitions.is_empty() || adopted_acquisitions > 0 || adopted_active_scans > 0
        {
            return Ok(());
        }
        attempt_finalizer(config, snapshot, runtime, live_task).await?;
        // Admission or the cross-process finalizer lock may defer this task.
        // Only a real spawn earns live priority; otherwise continue the census
        // so a blocked READY capture cannot starve acquisitions/scans forever.
        if runtime.finalizer.is_some() {
            return Ok(());
        }
    }

    // Acquisition and preflight are a global census phase. Do not start a new
    // compact scan until every runnable acquisition task in this inventory
    // generation has either completed or reached a terminal failure/gap.
    if !acquisition_candidates.is_empty()
        || !runtime.acquisitions.is_empty()
        || adopted_acquisitions > 0
    {
        let mut car_disk_headroom = acquisition_car_disk_headroom;
        let mut admitted = 0usize;
        let mut acquisition_claimed_during_spawn = false;
        for (epoch, action) in &acquisition_candidates {
            if admitted >= acquisition_slots {
                break;
            }
            let download_projection = if *action == AcquisitionAction::Download {
                let projected =
                    car_download_remaining_projection(config, &snapshot.epochs, epoch.epoch);
                if car_disk_headroom.is_none_or(|headroom| headroom < projected) {
                    continue;
                }
                projected
            } else {
                0
            };
            let result = match *action {
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
                    let key = match *action {
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
    if active_scans == 0 && runtime.scans.is_empty() && !live_ready_pending {
        if top_up_legacy_compacts(config, snapshot, runtime).await > 0 {
            return Ok(());
        }
    }
    let slots = snapshot
        .summary
        .scan_capacity_admitted
        .saturating_sub(active_scans);
    let queued = snapshot
        .epochs
        .iter()
        .filter(|epoch| epoch.state == HistoricalState::Queued)
        .filter(|epoch| acquisition_action(config, epoch).is_none())
        .filter(|epoch| {
            legacy_compact_reuse_status(config, epoch.epoch)
                == LegacyCompactReuseStatus::NotCandidate
        })
        .take(slots)
        .cloned()
        .collect::<Vec<_>>();
    for epoch in queued {
        let child = spawn_historical_scan(config, &epoch).await?;
        runtime.scans.insert(epoch.epoch, child);
    }
    if active_scan_count(&snapshot.epochs, runtime.scans.keys().copied()) > 0 {
        return Ok(());
    }

    // Reaching this point proves that no acquisition, legacy lane, or ordinary
    // scan could start in this pass. A historical task may therefore bypass a
    // stale sweep deferral caused only by currently inadmissible queued work;
    // its own memory/disk admission still has to pass.
    let Some(task) = snapshot.finalizer_queue.iter().find(|task| {
        (task.deferred_reason.is_none() || task.kind == "historical")
            && finalizer_admission_blocked_reason(config, &snapshot.machine, task).is_none()
    }) else {
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
    if legacy_compact_reuse_status(config, epoch.epoch) != LegacyCompactReuseStatus::NotCandidate {
        // The compact/reuse child performs the only CAR decompression and
        // validates records as it rewrites; a standalone preflight would make
        // this migration read every multi-terabyte input twice.
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

fn legacy_compact_reuse_args(
    config: &NasPipelineConfig,
    input: &Path,
    output: &Path,
    previous_car: Option<&Path>,
) -> Vec<std::ffi::OsString> {
    let mut args = vec![
        "build-archive-v2-hot-blocks".into(),
        input.as_os_str().to_owned(),
        output.as_os_str().to_owned(),
        "--registry-dir".into(),
        output.as_os_str().to_owned(),
        "--resume".into(),
        "--no-access".into(),
        "--level".into(),
        config.level.to_string().into(),
    ];
    if let Some(previous_car) = previous_car {
        args.push("--previous-car".into());
        args.push(previous_car.as_os_str().to_owned());
    }
    args
}

async fn spawn_legacy_compact_reuse(
    config: &NasPipelineConfig,
    epoch: &EpochSnapshot,
) -> Result<ManagedChild> {
    anyhow::ensure!(
        legacy_compact_reuse_status(config, epoch.epoch) == LegacyCompactReuseStatus::Ready,
        "epoch {} is not a dependency-ready legacy compact/reuse candidate",
        epoch.epoch
    );
    let input = epoch
        .input_path
        .as_deref()
        .context("legacy compact/reuse candidate has no unambiguous input CAR")?;
    let previous_car = if epoch.epoch == 0 {
        None
    } else {
        Some(
            legacy_compact_previous_car(config, epoch.epoch)
                .context("legacy compact/reuse candidate has no unambiguous predecessor CAR")?,
        )
    };
    match read_ownership(&epoch.output_path) {
        None => anyhow::ensure!(
            legacy_registry_only_shape(&epoch.output_path),
            "legacy compact/reuse output shape changed before claim: {}",
            epoch.output_path.display()
        ),
        Some(owner) => anyhow::ensure!(
            owner.kind == LEGACY_COMPACT_OWNERSHIP_KIND
                && owner.id == epoch.epoch.to_string()
                && owner.state == "retry_ready"
                && owner.pid.is_none()
                && legacy_owned_retry_shape(&epoch.output_path),
            "legacy compact/reuse retry ownership or output shape changed for epoch {}",
            epoch.epoch
        ),
    }
    write_ownership(
        &epoch.output_path,
        LEGACY_COMPACT_OWNERSHIP_KIND,
        &epoch.epoch.to_string(),
        "compact_reuse",
        Some(
            "reusing validated legacy registry sidecars in a one-pass no-access compact"
                .to_string(),
        ),
    )?;
    let progress_path = historical_progress_path(&config.state_root, epoch.epoch);
    let log_path = config
        .state_root
        .join("logs")
        .join(format!("epoch-{}-compact-reuse.log", epoch.epoch));
    let args =
        legacy_compact_reuse_args(config, input, &epoch.output_path, previous_car.as_deref());
    let result = spawn_child(
        config,
        args,
        ChildKind::HistoricalCompactReuse { epoch: epoch.epoch },
        progress_path,
        log_path,
        None,
    )
    .await;
    if let Err(error) = &result {
        let _ = write_ownership(
            &epoch.output_path,
            LEGACY_COMPACT_OWNERSHIP_KIND,
            &epoch.epoch.to_string(),
            "failed",
            Some(format!("legacy compact/reuse spawn failed: {error:#}")),
        );
        let _ = set_ownership_pid(&epoch.output_path, None);
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
        ChildKind::CarDownload { .. }
            | ChildKind::CarPreflight { .. }
            | ChildKind::HistoricalCompactReuse { .. }
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
    let mut child = child_result?;
    let pid = child.id();
    let owned_output = match &kind {
        ChildKind::CarDownload { .. } | ChildKind::CarPreflight { .. } => None,
        ChildKind::HistoricalScan { epoch }
        | ChildKind::HistoricalCompactReuse { epoch }
        | ChildKind::HistoricalFinalizer { epoch } => {
            Some(config.archive_root.join(format!("epoch-{epoch}")))
        }
        ChildKind::LiveFinalizer { epoch, .. } => {
            epoch.map(|epoch| config.archive_root.join(format!("epoch-{epoch}")))
        }
    };
    if let ChildKind::HistoricalCompactReuse { epoch } = &kind {
        let output = config.archive_root.join(format!("epoch-{epoch}"));
        let publication = (|| -> Result<()> {
            let pid = pid.context("spawned legacy compact has no pid")?;
            set_ownership_pid(&output, Some(pid))?;
            let owner = read_ownership(&output)
                .context("legacy compact ownership marker disappeared after spawn")?;
            anyhow::ensure!(
                owner_matches_legacy_identity(&owner, *epoch, pid, SCHEMA_VERSION),
                "legacy compact owner PID publication did not preserve exact schema/kind/id/state/pid"
            );
            Ok(())
        })();
        if let Err(error) = publication {
            if let Some(pid) = pid {
                // SAFETY: this command was requested with process_group(0);
                // kill the whole group before returning a failed admission.
                let _ = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
            } else {
                let _ = child.kill().await;
            }
            let _ = child.wait().await;
            return Err(error.context(format!(
                "establish recoverable ownership for compact_reuse:{epoch}"
            )));
        }
    } else if let Some(output) = owned_output.as_deref() {
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
    reap_legacy_compacts(&state.config, runtime);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdoptedLegacyProcessState {
    Running,
    Gone,
    LiveIdentityChanged,
}

fn adopted_legacy_process_state(
    config: &NasPipelineConfig,
    compact: &AdoptedLegacyCompact,
) -> AdoptedLegacyProcessState {
    let Some((state, start_ticks)) = process_stat_identity(compact.pid) else {
        return if process_exists(compact.pid) {
            // A transiently unreadable /proc record is unknown. Retain its
            // capacity and never turn uncertainty into a completion commit.
            AdoptedLegacyProcessState::Running
        } else {
            AdoptedLegacyProcessState::Gone
        };
    };
    if start_ticks != compact.process_start_ticks {
        return AdoptedLegacyProcessState::LiveIdentityChanged;
    }
    if state == 'Z' {
        return AdoptedLegacyProcessState::Running;
    }
    match process_cmdline_matches_legacy_exact(config, compact.epoch, compact.pid) {
        Some(true) => AdoptedLegacyProcessState::Running,
        Some(false) => AdoptedLegacyProcessState::LiveIdentityChanged,
        None => AdoptedLegacyProcessState::Running,
    }
}

fn commit_adopted_legacy_complete(
    config: &NasPipelineConfig,
    compact: &AdoptedLegacyCompact,
) -> Result<()> {
    anyhow::ensure!(
        !compact.identity_tainted,
        "live PID/starttime/argv identity was previously tainted"
    );
    let output = config.archive_root.join(format!("epoch-{}", compact.epoch));
    let mut owner = read_ownership(&output).context("ownership marker is missing or invalid")?;
    anyhow::ensure!(
        owner_matches_legacy_identity(
            &owner,
            compact.epoch,
            compact.pid,
            compact.owner_schema_version,
        ),
        "ownership schema/kind/id/state/pid changed"
    );
    let progress = read_progress(&compact.progress_path).context("progress JSON is missing")?;
    anyhow::ensure!(
        progress.pid == Some(compact.pid),
        "progress pid {:?} does not match tracked pid {}",
        progress.pid,
        compact.pid
    );
    anyhow::ensure!(
        progress.state.as_deref() == Some("complete"),
        "progress state {:?} is not complete",
        progress.state
    );
    anyhow::ensure!(
        legacy_compact_reader_complete(&output),
        "reader-core validation failed"
    );
    owner.state = "complete".to_string();
    owner.pid = None;
    owner.updated_unix_secs = unix_now();
    owner.message = Some(
        "trusted adopted compact/reuse exit: exact owner, progress pid/state, and reader core validated"
            .to_string(),
    );
    publish_ownership_marker(&output, &owner)
}

fn fail_adopted_legacy(
    config: &NasPipelineConfig,
    runtime: &mut RuntimeState,
    compact: &AdoptedLegacyCompact,
    detail: String,
) {
    let key = format!("compact_reuse:{}", compact.epoch);
    let message = format!(
        "trusted adopted {key}/pid:{} failed closed: {detail}",
        compact.pid
    );
    set_runtime_failure(config, runtime, key, message.clone());
    let output = config.archive_root.join(format!("epoch-{}", compact.epoch));
    if let Some(mut owner) = read_ownership(&output)
        && owner_matches_legacy_identity(
            &owner,
            compact.epoch,
            compact.pid,
            compact.owner_schema_version,
        )
    {
        owner.state = "failed".to_string();
        owner.pid = None;
        owner.updated_unix_secs = unix_now();
        owner.message = Some(message.clone());
        if let Err(error) = publish_ownership_marker(&output, &owner) {
            record_error(
                config,
                runtime,
                "adopted_legacy",
                format!("{message}; failed to publish failure marker: {error:#}"),
            );
            return;
        }
    }
    record_error(config, runtime, "adopted_legacy", message);
}

fn reap_adopted_legacy_compacts(config: &NasPipelineConfig, runtime: &mut RuntimeState) {
    let epochs = runtime
        .adopted_legacy_compacts
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for epoch in epochs {
        let Some(compact) = runtime.adopted_legacy_compacts.get(&epoch).cloned() else {
            continue;
        };
        match adopted_legacy_process_state(config, &compact) {
            AdoptedLegacyProcessState::Running => continue,
            AdoptedLegacyProcessState::LiveIdentityChanged => {
                if !compact.identity_tainted {
                    if let Some(tracked) = runtime.adopted_legacy_compacts.get_mut(&epoch) {
                        tracked.identity_tainted = true;
                    }
                    let message = format!(
                        "compact_reuse:{epoch}/pid:{} live PID starttime or exact argv identity changed; retaining full capacity until disappearance and permanently forbidding trusted commit",
                        compact.pid
                    );
                    set_runtime_failure(
                        config,
                        runtime,
                        format!("compact_reuse:{epoch}"),
                        message.clone(),
                    );
                    record_error(config, runtime, "adopted_legacy", message);
                }
            }
            AdoptedLegacyProcessState::Gone => {
                runtime.adopted_legacy_compacts.remove(&epoch);
                if compact.identity_tainted {
                    fail_adopted_legacy(
                        config,
                        runtime,
                        &compact,
                        "live identity was tainted before PID disappearance".to_string(),
                    );
                    continue;
                }
                match commit_adopted_legacy_complete(config, &compact) {
                    Ok(()) => {
                        clear_runtime_failure(config, runtime, &format!("compact_reuse:{epoch}"));
                        if let Err(error) = append_control_event(
                            config,
                            "adopted_legacy_complete",
                            &format!("compact_reuse:{epoch}/pid:{}", compact.pid),
                        ) {
                            record_error(
                                config,
                                runtime,
                                "adopted_legacy",
                                format!("record trusted adopted completion: {error:#}"),
                            );
                        }
                    }
                    Err(error) => {
                        fail_adopted_legacy(config, runtime, &compact, format!("{error:#}"));
                    }
                }
            }
        }
    }
}

fn reap_legacy_compacts(config: &NasPipelineConfig, runtime: &mut RuntimeState) {
    let epochs = runtime.legacy_compacts.keys().copied().collect::<Vec<_>>();
    for epoch in epochs {
        let result = runtime
            .legacy_compacts
            .get_mut(&epoch)
            .and_then(|child| child.child.try_wait().transpose());
        match result {
            Some(Ok(status)) => {
                let child = runtime
                    .legacy_compacts
                    .remove(&epoch)
                    .expect("legacy compact child exists");
                clear_legacy_pause_state_after_exit(config, runtime, epoch);
                handle_child_exit(config, runtime, child, status.success());
            }
            Some(Err(error)) => {
                let child = runtime
                    .legacy_compacts
                    .remove(&epoch)
                    .expect("legacy compact child exists");
                clear_legacy_pause_state_after_exit(config, runtime, epoch);
                let message = format!("poll {}: {error:#}", child.kind.key());
                set_runtime_failure(config, runtime, child.kind.key(), message.clone());
                let output = config.archive_root.join(format!("epoch-{epoch}"));
                let _ = write_ownership(
                    &output,
                    LEGACY_COMPACT_OWNERSHIP_KIND,
                    &epoch.to_string(),
                    "failed",
                    Some(message.clone()),
                );
                let _ = set_ownership_pid(&output, None);
                record_error(config, runtime, "child", message);
            }
            None => {}
        }
    }
}

fn clear_legacy_pause_state_after_exit(
    config: &NasPipelineConfig,
    runtime: &mut RuntimeState,
    epoch: u64,
) {
    let changed = runtime.auto_paused_legacy.remove(&epoch).is_some()
        | runtime
            .paused_jobs
            .remove(&format!("compact_reuse:{epoch}"));
    if changed && let Err(error) = persist_control_state(config, runtime) {
        record_error(
            config,
            runtime,
            "legacy_auto_pause",
            format!("persist exited compact/reuse pause cleanup: {error:#}"),
        );
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
        ChildKind::HistoricalScan { epoch }
        | ChildKind::HistoricalCompactReuse { epoch }
        | ChildKind::HistoricalFinalizer { epoch } => {
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
        ChildKind::HistoricalCompactReuse { epoch } => {
            legacy_compact_reader_complete(&config.archive_root.join(format!("epoch-{epoch}")))
        }
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
                ChildKind::HistoricalCompactReuse { .. } => "complete",
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
    publish_ownership_marker(output, &owner)
}

fn publish_ownership_marker(output: &Path, owner: &OwnershipMarker) -> Result<()> {
    let path = output.join(OWNERSHIP_MARKER);
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
        input_mib_per_sec: finite_option(json_f64(&value, &["input_mib_per_sec", "mb_per_sec"])),
        eta_secs: finite_option(json_f64(&value, &["eta_secs"])),
        rss_bytes: pid.and_then(process_rss_bytes),
        updated_unix_secs: json_u64(&value, &["updated_unix_secs"]),
    })
}

fn finite_option(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite())
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
    file.read_to_end(&mut bytes).ok()?;
    bytes.split(|byte| *byte == b'\n').rev().find_map(|line| {
        (!line.is_empty())
            .then(|| serde_json::from_slice(line).ok())
            .flatten()
    })
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

fn process_is_group_leader(pid: u32) -> bool {
    // SAFETY: getpgid only queries the supplied process identifier.
    unsafe { libc::getpgid(pid as libc::pid_t) == pid as libc::pid_t }
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

fn expected_legacy_compact_argv(config: &NasPipelineConfig, epoch: u64) -> Option<Vec<Vec<u8>>> {
    if car_paths_ambiguous(&config.car_root, epoch) {
        return None;
    }
    let input = car_path(&config.car_root, epoch)?;
    let output = config.archive_root.join(format!("epoch-{epoch}"));
    let previous_car = (epoch > 0)
        .then(|| legacy_compact_previous_car(config, epoch))
        .flatten();
    if epoch > 0 && previous_car.is_none() {
        return None;
    }
    let mut expected = vec![config.blockzilla_bin.as_os_str().as_bytes().to_vec()];
    expected.extend(
        legacy_compact_reuse_args(config, &input, &output, previous_car.as_deref())
            .into_iter()
            .map(|arg| arg.as_os_str().as_bytes().to_vec()),
    );
    Some(expected)
}

/// `None` means the process cmdline or scheduler inputs were not readable.
/// Callers must retain capacity/fail closed rather than treating unknown as a
/// mismatch or a completed process.
fn process_cmdline_matches_legacy_exact(
    config: &NasPipelineConfig,
    epoch: u64,
    pid: u32,
) -> Option<bool> {
    let bytes = fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline")).ok()?;
    legacy_compact_argv_matches_bytes(config, epoch, &bytes)
}

fn legacy_compact_argv_matches_bytes(
    config: &NasPipelineConfig,
    epoch: u64,
    bytes: &[u8],
) -> Option<bool> {
    let expected = expected_legacy_compact_argv(config, epoch)?;
    if bytes.last() != Some(&0) {
        return Some(false);
    }
    let actual = bytes[..bytes.len().saturating_sub(1)]
        .split(|byte| *byte == 0)
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    Some(actual == expected)
}

fn process_rss_bytes(pid: u32) -> Option<u64> {
    let status =
        fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("status")).ok()?;
    parse_status_kib(&status, "VmRSS:").map(|kib| kib.saturating_mul(1024))
}

fn parse_process_io_counter(io: &str, key: &str) -> Option<u64> {
    io.lines()
        .find_map(|line| line.strip_prefix(key)?.trim().parse::<u64>().ok())
}

fn process_io_counter_bytes(pid: u32, key: &str) -> Option<u64> {
    let io = fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("io")).ok()?;
    parse_process_io_counter(&io, key)
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

fn parse_status_kib(status: &str, key: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        line.strip_prefix(key)?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

fn parse_psi_avg10(pressure: &str, class: &str) -> Option<f64> {
    pressure.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        if fields.next()? != class {
            return None;
        }
        fields.find_map(|field| {
            field
                .strip_prefix("avg10=")?
                .parse::<f64>()
                .ok()
                .filter(|value| value.is_finite() && *value >= 0.0)
        })
    })
}

fn pressure_avg10(path: &str) -> (Option<f64>, Option<f64>) {
    let Ok(pressure) = fs::read_to_string(path) else {
        return (None, None);
    };
    (
        parse_psi_avg10(&pressure, "some"),
        parse_psi_avg10(&pressure, "full"),
    )
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
    let (io_pressure_some_avg10, io_pressure_full_avg10) = pressure_avg10("/proc/pressure/io");
    let (memory_pressure_some_avg10, memory_pressure_full_avg10) =
        pressure_avg10("/proc/pressure/memory");
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
        load_1m,
        io_pressure_some_avg10,
        io_pressure_full_avg10,
        memory_pressure_some_avg10,
        memory_pressure_full_avg10,
        service_rss_bytes: process_rss_bytes(std::process::id()).unwrap_or(0),
        children_rss_bytes,
    }
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
    for projection in epochs
        .iter()
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

fn persist_snapshot(root: &Path, snapshot: &PipelineSnapshot) -> Result<()> {
    let path = root.join("status.json");
    let temp = root.join(format!(".status.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec_pretty(snapshot).context("serialize pipeline status")?;
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
    #[serde(default)]
    auto_paused_legacy: BTreeMap<u64, AutoPausedLegacy>,
    #[serde(default)]
    legacy_last_adaptive_action_unix_secs: u64,
    #[serde(default)]
    legacy_last_adaptive_action_reason: Option<String>,
}

fn persist_control_state(config: &NasPipelineConfig, runtime: &RuntimeState) -> Result<()> {
    let value = PersistedControlState {
        scheduler_paused: runtime.scheduler_paused,
        scheduler_updated_unix_secs: runtime.scheduler_updated_unix_secs,
        paused_jobs: runtime.paused_jobs.clone(),
        auto_paused_legacy: runtime.auto_paused_legacy.clone(),
        legacy_last_adaptive_action_unix_secs: runtime.legacy_last_adaptive_action_unix_secs,
        legacy_last_adaptive_action_reason: runtime.legacy_last_adaptive_action_reason.clone(),
    };
    let path = config.state_root.join("control-state.json");
    let temp = config
        .state_root
        .join(format!(".control-state.{}.tmp", std::process::id()));
    fs::write(&temp, serde_json::to_vec_pretty(&value)?)
        .with_context(|| format!("write control state {}", temp.display()))?;
    fs::rename(&temp, &path).with_context(|| format!("publish control state {}", path.display()))
}

async fn load_control_state(state: &Arc<AppState>) -> Result<()> {
    let path = state.config.state_root.join("control-state.json");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("read existing control state {}", path.display()));
        }
    };
    let saved = serde_json::from_slice::<PersistedControlState>(&bytes)
        .with_context(|| format!("parse existing control state {}", path.display()))?;
    let mut runtime = state.runtime.lock().await;
    runtime.scheduler_paused = saved.scheduler_paused;
    runtime.scheduler_updated_unix_secs = saved.scheduler_updated_unix_secs;
    runtime.paused_jobs = saved.paused_jobs;
    runtime.auto_paused_legacy = saved.auto_paused_legacy;
    runtime.legacy_last_adaptive_action_unix_secs = saved.legacy_last_adaptive_action_unix_secs;
    runtime.legacy_last_adaptive_action_reason = saved.legacy_last_adaptive_action_reason;
    Ok(())
}

fn owner_matches_legacy_identity(
    owner: &OwnershipMarker,
    epoch: u64,
    pid: u32,
    schema_version: u32,
) -> bool {
    owner.schema_version == schema_version
        && owner.kind == LEGACY_COMPACT_OWNERSHIP_KIND
        && owner.id == epoch.to_string()
        && owner.state == "compact_reuse"
        && owner.pid == Some(pid)
}

fn trusted_adopted_legacy_candidate(
    config: &NasPipelineConfig,
    epoch: u64,
) -> Option<AdoptedLegacyCompact> {
    let output = config.archive_root.join(format!("epoch-{epoch}"));
    let first_owner = read_ownership(&output)?;
    let pid = first_owner.pid?;
    if !owner_matches_legacy_identity(&first_owner, epoch, pid, SCHEMA_VERSION) {
        return None;
    }
    let (first_state, first_start_ticks) = process_stat_identity(pid)?;
    if first_state == 'Z' || process_cmdline_matches_legacy_exact(config, epoch, pid) != Some(true)
    {
        return None;
    }
    let (second_state, second_start_ticks) = process_stat_identity(pid)?;
    let second_owner = read_ownership(&output)?;
    if second_state == 'Z'
        || first_start_ticks != second_start_ticks
        || !owner_matches_legacy_identity(&second_owner, epoch, pid, first_owner.schema_version)
    {
        return None;
    }
    Some(AdoptedLegacyCompact {
        epoch,
        pid,
        owner_schema_version: first_owner.schema_version,
        process_start_ticks: first_start_ticks,
        progress_path: historical_progress_path(&config.state_root, epoch),
        identity_tainted: false,
    })
}

async fn track_adopted_legacy_compacts(state: &Arc<AppState>) -> Result<()> {
    let entries = fs::read_dir(&state.config.archive_root).with_context(|| {
        format!(
            "audit all archive epochs for adopted legacy workers in {}",
            state.config.archive_root.display()
        )
    })?;
    let mut audit_epochs = BTreeSet::new();
    for entry in entries {
        let entry = entry.with_context(|| {
            format!(
                "read archive entry while auditing adopted legacy workers in {}",
                state.config.archive_root.display()
            )
        })?;
        let file_type = entry.file_type().with_context(|| {
            format!(
                "stat archive entry while auditing adopted legacy workers: {}",
                entry.path().display()
            )
        })?;
        if file_type.is_dir()
            && let Some(epoch) = entry
                .file_name()
                .to_str()
                .and_then(parse_archive_epoch_name)
        {
            audit_epochs.insert(epoch);
        }
    }
    let mut candidates = Vec::new();
    for epoch in audit_epochs {
        let output = state.config.archive_root.join(format!("epoch-{epoch}"));
        let claim = read_ownership(&output).filter(|owner| {
            owner.kind == LEGACY_COMPACT_OWNERSHIP_KIND && owner.state == "compact_reuse"
        });
        if claim.as_ref().is_some_and(|owner| owner.pid.is_none()) {
            anyhow::bail!(
                "compact/reuse owner epoch {epoch} (schema {}) has no PID; refusing scheduler startup because a crash may have occurred between spawn and recoverable PID publication",
                claim.as_ref().map_or(0, |owner| owner.schema_version),
            );
        }
        let live_claim = claim.as_ref().and_then(|owner| owner.pid);
        match trusted_adopted_legacy_candidate(&state.config, epoch) {
            Some(candidate) => candidates.push(candidate),
            None if live_claim.is_some_and(process_exists) => {
                let pid = live_claim.unwrap();
                anyhow::bail!(
                    "live compact/reuse owner epoch {epoch} pid {pid} could not prove exact current-schema owner, stable starttime, and byte-exact scheduler argv; refusing to start scheduler without counting it"
                );
            }
            None => {}
        }
    }
    let mut runtime = state.runtime.lock().await;
    for candidate in candidates {
        if runtime.legacy_compacts.contains_key(&candidate.epoch) {
            continue;
        }
        let epoch = candidate.epoch;
        let pid = candidate.pid;
        runtime.adopted_legacy_compacts.insert(epoch, candidate);
        let target = format!("compact_reuse:{epoch}/pid:{pid}");
        if let Err(error) = append_control_event(&state.config, "adopted_legacy_track", &target) {
            record_error(
                &state.config,
                &mut runtime,
                "adopted_legacy",
                format!("record adopted legacy lane {target}: {error:#}"),
            );
        }
    }
    Ok(())
}

async fn recover_manual_paused_legacy(state: &Arc<AppState>) -> Result<()> {
    let mut runtime = state.runtime.lock().await;
    let paused_epochs = runtime
        .paused_jobs
        .iter()
        .filter_map(|key| key.strip_prefix("compact_reuse:")?.parse::<u64>().ok())
        .collect::<Vec<_>>();
    let mut changed = false;
    for epoch in paused_epochs {
        let key = format!("compact_reuse:{epoch}");
        let Some(compact) = runtime.adopted_legacy_compacts.get(&epoch).cloned() else {
            let owner = read_ownership(&state.config.archive_root.join(format!("epoch-{epoch}")));
            let demonstrably_terminal = owner.as_ref().is_some_and(|owner| {
                owner.kind == LEGACY_COMPACT_OWNERSHIP_KIND
                    && owner.id == epoch.to_string()
                    && (owner.pid.is_some_and(|pid| !process_exists(pid))
                        || (owner.pid.is_none()
                            && matches!(
                                owner.state.as_str(),
                                "complete" | "failed" | "retry_ready"
                            )))
            });
            if demonstrably_terminal {
                runtime.paused_jobs.remove(&key);
                changed = true;
                record_error(
                    &state.config,
                    &mut runtime,
                    "manual_pause_recovery",
                    format!(
                        "cleared stale manual pause {key} only after exact ownership proved the PID gone or terminal"
                    ),
                );
                continue;
            }
            anyhow::bail!(
                "manual pause {key} has no trusted adopted identity and ownership cannot prove a gone/terminal PID; retained pause and refused startup"
            );
        };
        let trusted = !compact.identity_tainted
            && process_stat_identity(compact.pid)
                .is_some_and(|(_, start_ticks)| start_ticks == compact.process_start_ticks)
            && process_cmdline_matches_legacy_exact(&state.config, compact.epoch, compact.pid)
                == Some(true);
        if !trusted {
            anyhow::bail!(
                "manual pause {key}/pid:{} could not prove stable starttime and byte-exact argv; retained pause and refused startup",
                compact.pid
            );
        }
        let group = process_is_group_leader(compact.pid);
        let target = controlled_signal_target("historical_compact_reuse", compact.pid, group);
        // SAFETY: the tracked PID has stable starttime and byte-exact argv;
        // negative scope is selected only when it is the group leader.
        if unsafe { libc::kill(target, libc::SIGSTOP) } != 0 {
            anyhow::bail!(
                "reapply manual pause {key}/pid:{} failed: {}",
                compact.pid,
                std::io::Error::last_os_error()
            );
        }
        append_control_event(
            &state.config,
            "manual_pause_recovery",
            &format!("{key}/pid:{}", compact.pid),
        )?;
    }
    if changed {
        persist_control_state(&state.config, &runtime)
            .context("persist stale manual pause cleanup")?;
    }
    Ok(())
}

async fn recover_auto_paused_legacy(state: &Arc<AppState>) -> Result<()> {
    let mut runtime = state.runtime.lock().await;
    if runtime.auto_paused_legacy.is_empty() {
        return Ok(());
    }
    let records = runtime
        .auto_paused_legacy
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let mut last_recovery = None;
    for record in records {
        let key = format!("compact_reuse:{}", record.epoch);
        if runtime.paused_jobs.contains(&key) {
            record_error(
                &state.config,
                &mut runtime,
                "legacy_auto_pause_recovery",
                format!(
                    "left manually paused {key} stopped and discarded overlapping automatic pause record"
                ),
            );
            runtime.auto_paused_legacy.remove(&record.epoch);
            continue;
        }
        let output = state
            .config
            .archive_root
            .join(format!("epoch-{}", record.epoch));
        let trusted = read_ownership(&output).is_some_and(|owner| {
            owner_matches_legacy_identity(&owner, record.epoch, record.pid, SCHEMA_VERSION)
        }) && record.process_start_ticks.is_some_and(|expected| {
            process_stat_identity(record.pid).is_some_and(|(_, observed)| observed == expected)
        }) && process_is_group_leader(record.pid)
            && process_cmdline_matches_legacy_exact(&state.config, record.epoch, record.pid)
                == Some(true);
        if !trusted {
            if !process_exists(record.pid) {
                record_error(
                    &state.config,
                    &mut runtime,
                    "legacy_auto_pause_recovery",
                    format!(
                        "cleared automatic pause record {key}/pid:{} only after the PID demonstrably disappeared",
                        record.pid
                    ),
                );
                runtime.auto_paused_legacy.remove(&record.epoch);
                continue;
            }
            let message = format!(
                "automatic pause record {key}/pid:{} is still live but exact owner, byte-exact argv, or process-group identity is unprovable; retained record and refused scheduler startup",
                record.pid
            );
            record_error(
                &state.config,
                &mut runtime,
                "legacy_auto_pause_recovery",
                message.clone(),
            );
            persist_control_state(&state.config, &runtime)
                .context("persist unprovable live automatic pause record")?;
            anyhow::bail!(message);
        }
        // SAFETY: the persisted record originated from an auto-paused managed
        // group; current owner, argv, and group-leader identity were rechecked.
        if unsafe { libc::kill(-(record.pid as libc::pid_t), libc::SIGCONT) } == 0 {
            let action = format!("startup auto-resumed {key}/pid:{}", record.pid);
            last_recovery = Some(action.clone());
            runtime.auto_paused_legacy.remove(&record.epoch);
            if let Err(error) =
                append_control_event(&state.config, "legacy_auto_resume_recovery", &action)
            {
                record_error(
                    &state.config,
                    &mut runtime,
                    "legacy_auto_pause_recovery",
                    format!("record {action}: {error:#}"),
                );
            }
        } else {
            let signal_error = std::io::Error::last_os_error();
            record_error(
                &state.config,
                &mut runtime,
                "legacy_auto_pause_recovery",
                format!(
                    "failed to resume {key}/pid:{}: {}",
                    record.pid, signal_error
                ),
            );
            // Retain the trusted record and fail startup. Erasing the only
            // recovery evidence could strand a stopped process permanently.
            persist_control_state(&state.config, &runtime).with_context(|| {
                format!(
                    "persist trusted auto-pause recovery failure for {key}/pid:{}",
                    record.pid
                )
            })?;
            anyhow::bail!(
                "trusted auto-paused {key}/pid:{} could not be resumed: {signal_error}",
                record.pid
            );
        }
    }
    if let Some(action) = last_recovery {
        runtime.legacy_last_adaptive_action_unix_secs = unix_now();
        runtime.legacy_last_adaptive_action_reason = Some(action);
    }
    persist_control_state(&state.config, &runtime)
        .context("persist recovered automatic pause state")?;
    Ok(())
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
            car_root: root.join("cars"),
            archive_root: root.join("archives"),
            live_root: root.join("live"),
            state_root: root.join("state"),
            scan_concurrency: 4,
            legacy_compact_concurrency: 1,
            legacy_compact_cpu_cores_per_worker: 1,
            legacy_compact_cpu_budget_cores: 1,
            legacy_compact_io_mib_per_sec_per_worker: 120,
            legacy_compact_io_budget_mib_per_sec: 120,
            legacy_compact_auto_pause: false,
            legacy_compact_min_running: 1,
            legacy_compact_memory_guard_mib: 512,
            legacy_compact_io_pause_full_avg10: 20.0,
            legacy_compact_io_resume_full_avg10: 5.0,
            legacy_compact_pause_cooldown: Duration::from_secs(30),
            legacy_compact_throughput_probe_window: Duration::from_secs(120),
            legacy_compact_throughput_min_gain_pct: 5.0,
            legacy_compact_throughput_probe_backoff: Duration::from_secs(900),
            scan_memory_mib: 800,
            finalizer_memory_mib: 512,
            memory_reserve_mib: 256,
            disk_reserve_gib: 256,
            level: 1,
            execute: false,
            no_access: true,
            start_epoch: Some(700),
            end_epoch: Some(700),
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

    fn throughput_lane(
        epoch: u64,
        pid: u32,
        blocks_done: u64,
        now: u64,
        state: &str,
    ) -> LaneSnapshot {
        LaneSnapshot {
            id: format!("compact_reuse:{epoch}"),
            kind: "historical_compact_reuse".to_string(),
            epoch: Some(epoch),
            capture_id: None,
            phase: "compact_reuse".to_string(),
            state: state.to_string(),
            auto_paused: state == "paused",
            auto_pause_reason: None,
            pid: Some(pid),
            progress: ProgressSnapshot {
                phase: Some("Archive V2 Hot Write".to_string()),
                state: Some(state.to_string()),
                pid: Some(pid),
                blocks_done,
                updated_unix_secs: Some(now),
                ..ProgressSnapshot::default()
            },
            rss_bytes: None,
            started_unix_secs: Some(now.saturating_sub(60)),
            updated_unix_secs: now,
        }
    }

    fn throughput_snapshot(lanes: Vec<LaneSnapshot>, now: u64) -> PipelineSnapshot {
        let mut snapshot = empty_snapshot(false);
        snapshot.now_unix_secs = now;
        snapshot.lanes = lanes;
        snapshot
    }

    fn throughput_measurement(epochs: &[u64], rate: f64) -> LegacyThroughputMeasurement {
        LegacyThroughputMeasurement {
            running_epochs: epochs.to_vec(),
            started_unix_secs: 0,
            ended_unix_secs: 10,
            blocks_per_sec: rate,
            read_mib_per_sec: None,
        }
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
        index.extend_from_slice(
            &u16::try_from(REGISTRY_INDEX_HEADER_LEN)
                .unwrap()
                .to_le_bytes(),
        );
        index.extend_from_slice(&registry_keys.to_le_bytes());
        index.resize(
            usize::try_from(
                u64::try_from(REGISTRY_INDEX_HEADER_LEN).unwrap() + registry_keys * 12 + 1,
            )
            .unwrap(),
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

    fn write_legacy_registry_sidecars(output: &Path, with_v3: bool) {
        fs::create_dir_all(output).unwrap();
        let registry = vec![7u8; 64];
        fs::write(output.join(REGISTRY_FILE), &registry).unwrap();
        fs::write(output.join(REGISTRY_COUNTS_FILE), [1u8, 1]).unwrap();
        fs::write(output.join(BLOCKHASH_REGISTRY_FILE), [9u8; 64]).unwrap();

        let mut mphf = Vec::new();
        mphf.extend_from_slice(REGISTRY_INDEX_MAGIC);
        mphf.extend_from_slice(&REGISTRY_INDEX_VERSION.to_le_bytes());
        mphf.extend_from_slice(&(REGISTRY_INDEX_HEADER_LEN as u16).to_le_bytes());
        mphf.extend_from_slice(&2u64.to_le_bytes());
        mphf.extend_from_slice(&[0u8; 25]);
        fs::write(output.join(REGISTRY_INDEX_FILE), mphf).unwrap();

        if with_v3 {
            let mut v3 = Vec::new();
            v3.extend_from_slice(BLOCKHASH_INDEX_V3_MAGIC);
            v3.extend_from_slice(&BLOCKHASH_INDEX_V3_VERSION.to_le_bytes());
            v3.extend_from_slice(&(BLOCKHASH_INDEX_V3_ROW_LEN as u16).to_le_bytes());
            v3.extend_from_slice(&2u64.to_le_bytes());
            v3.extend_from_slice(&vec![0u8; 2 * BLOCKHASH_INDEX_V3_ROW_LEN as usize]);
            fs::write(output.join(BLOCKHASH_INDEX_V3_FILE), v3).unwrap();
        }
    }

    fn write_valid_hot_index(output: &Path, rows: u64, blob_bytes: u64) {
        fs::create_dir_all(output).unwrap();
        fs::write(output.join(BLOCKS_FILE), vec![3u8; blob_bytes as usize]).unwrap();
        let mut index = Vec::new();
        index.extend_from_slice(HOT_BLOCK_INDEX_MAGIC);
        index.extend_from_slice(&HOT_BLOCK_INDEX_VERSION.to_le_bytes());
        index.extend_from_slice(&0u16.to_le_bytes());
        index.extend_from_slice(&rows.to_le_bytes());
        index.extend_from_slice(&blob_bytes.to_le_bytes());
        index.extend_from_slice(&1i32.to_le_bytes());
        index.extend_from_slice(&0u32.to_le_bytes());
        index.extend_from_slice(&vec![0u8; rows as usize * HOT_BLOCK_INDEX_ROW_LEN as usize]);
        fs::write(output.join(BLOCK_INDEX_FILE), index).unwrap();
    }

    fn write_legacy_reader_core(output: &Path) {
        for name in [META_FILE, POH_FILE, SHREDDING_FILE] {
            fs::write(output.join(name), b"complete").unwrap();
        }
        for name in [SIGNATURES_FILE, VOTE_HASH_REGISTRY_FILE] {
            fs::write(output.join(name), b"").unwrap();
        }
        write_valid_hot_index(output, 2, 16);
    }

    async fn make_finished_child(
        kind: ChildKind,
        progress_path: PathBuf,
        log_path: PathBuf,
    ) -> ManagedChild {
        let mut child = Command::new("/usr/bin/true").spawn().unwrap();
        let pid = child.id();
        let _ = child.wait().await.unwrap();
        ManagedChild {
            pid,
            child,
            kind,
            started_unix_secs: unix_now(),
            progress_path,
            log_path,
            _exclusive_lock: None,
        }
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

    fn make_legacy_range_ready(config: &NasPipelineConfig, epoch: u64) {
        fs::create_dir_all(&config.car_root).unwrap();
        fs::write(config.car_root.join(format!("epoch-{epoch}.car")), b"car").unwrap();
        let predecessor = config.archive_root.join(format!("epoch-{}", epoch - 1));
        write_legacy_registry_sidecars(&predecessor, false);
        write_legacy_reader_core(&predecessor);
        write_legacy_registry_sidecars(&config.archive_root.join(format!("epoch-{epoch}")), false);
    }

    fn legacy_scheduler_machine(
        config: &NasPipelineConfig,
        memory_mib: u64,
        disk_gib: u64,
    ) -> MachineSnapshot {
        MachineSnapshot {
            memory_total_bytes: 16 * 1024 * 1024 * 1024,
            memory_available_bytes: config
                .memory_reserve_mib
                .saturating_add(memory_mib)
                .saturating_mul(1024 * 1024),
            disk_total_bytes: 2 * 1024 * 1024 * 1024 * 1024,
            disk_available_bytes: config
                .disk_reserve_gib
                .saturating_add(disk_gib)
                .saturating_mul(1024 * 1024 * 1024),
            ..MachineSnapshot::default()
        }
    }

    fn allow_legacy_workers(config: &mut NasPipelineConfig, workers: usize) {
        config.legacy_compact_concurrency = workers;
        config.legacy_compact_cpu_cores_per_worker = 1;
        config.legacy_compact_cpu_budget_cores = workers as u64;
        config.legacy_compact_io_mib_per_sec_per_worker = 120;
        config.legacy_compact_io_budget_mib_per_sec = (workers as u64).saturating_mul(120);
    }

    fn write_adopted_legacy_proof(
        config: &NasPipelineConfig,
        epoch: u64,
        pid: u32,
        progress_pid: u32,
        progress_state: &str,
        valid_core: bool,
    ) -> AdoptedLegacyCompact {
        let output = config.archive_root.join(format!("epoch-{epoch}"));
        write_legacy_registry_sidecars(&output, false);
        if valid_core {
            write_legacy_reader_core(&output);
        }
        let owner = OwnershipMarker {
            schema_version: SCHEMA_VERSION,
            kind: LEGACY_COMPACT_OWNERSHIP_KIND.to_string(),
            id: epoch.to_string(),
            state: "compact_reuse".to_string(),
            created_unix_secs: 1,
            updated_unix_secs: 2,
            message: None,
            pid: Some(pid),
        };
        publish_ownership_marker(&output, &owner).unwrap();
        let progress_path = historical_progress_path(&config.state_root, epoch);
        fs::create_dir_all(progress_path.parent().unwrap()).unwrap();
        fs::write(
            &progress_path,
            serde_json::to_vec(&serde_json::json!({
                "pid": progress_pid,
                "state": progress_state,
                "phase": "compact_reuse",
            }))
            .unwrap(),
        )
        .unwrap();
        AdoptedLegacyCompact {
            epoch,
            pid,
            owner_schema_version: SCHEMA_VERSION,
            process_start_ticks: 1,
            progress_path,
            identity_tainted: false,
        }
    }

    fn test_app_state(config: NasPipelineConfig) -> Arc<AppState> {
        let (updates, _) = broadcast::channel(4);
        Arc::new(AppState {
            config,
            snapshot: RwLock::new(empty_snapshot(false)),
            updates,
            sequence: AtomicU64::new(0),
            runtime: Mutex::new(RuntimeState::default()),
        })
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
    fn legacy_registry_only_shape_is_conservative_and_classifies_queued_without_preflight() {
        let root = temp_root("legacy-compact-detect");
        let mut config = test_config(&root);
        config.preflight_car = true;
        for path in [&config.car_root, &config.archive_root, &config.live_root] {
            fs::create_dir_all(path).unwrap();
        }
        fs::write(config.car_root.join("epoch-700.car.zst"), b"car").unwrap();
        let predecessor = config.archive_root.join("epoch-699");
        write_legacy_registry_sidecars(&predecessor, false);
        write_legacy_reader_core(&predecessor);
        let output = config.archive_root.join("epoch-700");
        write_legacy_registry_sidecars(&output, true);
        // Known stale standalone PoH is safe because the builder truncates it.
        fs::write(output.join(POH_FILE), b"stale").unwrap();

        // Two legacy epochs also retain an old empty blockhash lock
        // directory. Empty is harmless, but any nested entry is rejected.
        let stale_lock = output.join(LEGACY_BLOCKHASH_LOCK_DIR);
        fs::create_dir(&stale_lock).unwrap();
        assert!(legacy_registry_only_shape(&output));
        fs::write(stale_lock.join("owner"), b"unexpected").unwrap();
        assert!(!legacy_registry_only_shape(&output));
        fs::remove_file(stale_lock.join("owner")).unwrap();
        fs::remove_dir(&stale_lock).unwrap();

        assert!(legacy_registry_only_shape(&output));
        assert_eq!(
            legacy_compact_reuse_status(&config, 700),
            LegacyCompactReuseStatus::Ready
        );
        let queued = classify_epoch(&config, &RuntimeState::default(), 700, unix_now());
        assert_eq!(queued.state, HistoricalState::Queued);
        assert!(
            queued
                .message
                .as_deref()
                .unwrap()
                .contains("one-pass compact/reuse")
        );
        assert!(acquisition_action(&config, &queued).is_none());
        let preflight = queued
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == ArtifactKind::CarPreflight)
            .unwrap();
        assert_eq!(preflight.state, ArtifactState::NotApplicable);
        assert!(preflight.message.as_deref().unwrap().contains("bypassed"));

        // Epoch 863's partial reader core is intentionally outside the first
        // migration shape even though all four reusable sidecars remain valid.
        fs::write(output.join(BLOCKS_FILE), b"partial").unwrap();
        assert!(!legacy_registry_only_shape(&output));
        assert_eq!(
            legacy_compact_reuse_status(&config, 700),
            LegacyCompactReuseStatus::NotCandidate
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_compact_waits_for_complete_predecessor_and_uses_synthetic_epoch_hint() {
        let root = temp_root("legacy-compact-dependency");
        let config = test_config(&root);
        fs::create_dir_all(&config.car_root).unwrap();
        fs::create_dir_all(&config.archive_root).unwrap();
        fs::write(config.car_root.join("epoch-700.car.zst"), b"car").unwrap();
        let output = config.archive_root.join("epoch-700");
        write_legacy_registry_sidecars(&output, false);
        assert_eq!(
            legacy_compact_reuse_status(&config, 700),
            LegacyCompactReuseStatus::WaitingForPrevious
        );

        let predecessor = config.archive_root.join("epoch-699");
        write_legacy_registry_sidecars(&predecessor, false);
        fs::write(predecessor.join(POH_FILE), b"stale-only").unwrap();
        assert!(!predecessor_seed_sidecars_usable(&config, 700));
        write_legacy_reader_core(&predecessor);
        assert!(predecessor_seed_sidecars_usable(&config, 700));
        assert_eq!(
            legacy_compact_reuse_status(&config, 700),
            LegacyCompactReuseStatus::Ready
        );
        assert_eq!(
            legacy_compact_previous_car(&config, 700),
            Some(config.car_root.join("epoch-699.car.zst"))
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_compact_never_races_active_or_failed_pipeline_predecessor() {
        let root = temp_root("legacy-compact-predecessor-race");
        let config = test_config(&root);
        fs::create_dir_all(&config.car_root).unwrap();
        fs::write(config.car_root.join("epoch-700.car.zst"), b"car").unwrap();
        let output = config.archive_root.join("epoch-700");
        write_legacy_registry_sidecars(&output, false);
        let predecessor = config.archive_root.join("epoch-699");
        write_legacy_registry_sidecars(&predecessor, false);
        write_legacy_reader_core(&predecessor);

        write_ownership(
            &predecessor,
            LEGACY_COMPACT_OWNERSHIP_KIND,
            "699",
            "compact_reuse",
            None,
        )
        .unwrap();
        assert!(!predecessor_seed_sidecars_usable(&config, 700));
        assert_eq!(
            legacy_compact_reuse_status(&config, 700),
            LegacyCompactReuseStatus::WaitingForPrevious
        );

        write_ownership(
            &predecessor,
            LEGACY_COMPACT_OWNERSHIP_KIND,
            "699",
            "failed",
            Some("simulated failure".to_string()),
        )
        .unwrap();
        assert!(!predecessor_seed_sidecars_usable(&config, 700));

        fs::write(predecessor.join(OWNERSHIP_MARKER), b"{malformed").unwrap();
        assert!(!predecessor_seed_sidecars_usable(&config, 700));
        assert_eq!(
            legacy_compact_reuse_status(&config, 700),
            LegacyCompactReuseStatus::WaitingForPrevious
        );

        // A candidate's own durable tail is authoritative and allows an
        // independent start even while the predecessor is still active.
        write_ownership(
            &predecessor,
            LEGACY_COMPACT_OWNERSHIP_KIND,
            "699",
            "compact_reuse",
            None,
        )
        .unwrap();
        fs::write(output.join(PREVIOUS_BLOCKHASH_TAIL_FILE), [0u8; 40]).unwrap();
        assert_eq!(
            legacy_compact_reuse_status(&config, 700),
            LegacyCompactReuseStatus::Ready
        );
        fs::remove_file(output.join(PREVIOUS_BLOCKHASH_TAIL_FILE)).unwrap();

        write_ownership(
            &predecessor,
            LEGACY_COMPACT_OWNERSHIP_KIND,
            "699",
            "complete",
            None,
        )
        .unwrap();
        assert!(predecessor_seed_sidecars_usable(&config, 700));
        assert_eq!(
            legacy_compact_reuse_status(&config, 700),
            LegacyCompactReuseStatus::Ready
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_compact_args_reuse_sidecars_in_place_and_force_no_access() {
        let root = temp_root("legacy-compact-args");
        let mut config = test_config(&root);
        config.level = 7;
        let input = config.car_root.join("epoch-700.car.zst");
        let output = config.archive_root.join("epoch-700");
        let previous = config.car_root.join("epoch-699.car.zst");
        let args = legacy_compact_reuse_args(&config, &input, &output, Some(&previous));
        let args = args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            args,
            vec![
                "build-archive-v2-hot-blocks",
                input.to_str().unwrap(),
                output.to_str().unwrap(),
                "--registry-dir",
                output.to_str().unwrap(),
                "--resume",
                "--no-access",
                "--level",
                "7",
                "--previous-car",
                previous.to_str().unwrap(),
            ]
        );
    }

    #[tokio::test]
    async fn legacy_compact_exit_commits_only_valid_light_core_and_preserves_failure() {
        let root = temp_root("legacy-compact-exit");
        let config = test_config(&root);
        fs::create_dir_all(config.state_root.join("logs")).unwrap();
        let output = config.archive_root.join("epoch-700");
        write_legacy_registry_sidecars(&output, false);
        write_legacy_reader_core(&output);
        write_ownership(
            &output,
            LEGACY_COMPACT_OWNERSHIP_KIND,
            "700",
            "compact_reuse",
            None,
        )
        .unwrap();
        let log = config.state_root.join("logs/success.log");
        fs::write(&log, b"").unwrap();
        let child = make_finished_child(
            ChildKind::HistoricalCompactReuse { epoch: 700 },
            historical_progress_path(&config.state_root, 700),
            log,
        )
        .await;
        let mut runtime = RuntimeState::default();
        handle_child_exit(&config, &mut runtime, child, true);
        assert_eq!(read_ownership(&output).unwrap().state, "complete");
        assert!(pipeline_owned_legacy_compact_complete(&output));

        let failed_output = config.archive_root.join("epoch-701");
        write_legacy_registry_sidecars(&failed_output, false);
        let registry_before = fs::read(failed_output.join(REGISTRY_FILE)).unwrap();
        fs::write(failed_output.join(BLOCKS_FILE), b"partial").unwrap();
        write_ownership(
            &failed_output,
            LEGACY_COMPACT_OWNERSHIP_KIND,
            "701",
            "compact_reuse",
            None,
        )
        .unwrap();
        let log = config.state_root.join("logs/failure.log");
        fs::write(&log, b"").unwrap();
        let child = make_finished_child(
            ChildKind::HistoricalCompactReuse { epoch: 701 },
            historical_progress_path(&config.state_root, 701),
            log,
        )
        .await;
        handle_child_exit(&config, &mut runtime, child, false);
        assert_eq!(read_ownership(&failed_output).unwrap().state, "failed");
        assert_eq!(
            fs::read(failed_output.join(REGISTRY_FILE)).unwrap(),
            registry_before
        );
        assert!(failed_output.join(BLOCKS_FILE).is_file());
        assert!(!config.archive_root.join(".pipeline-quarantine").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn legacy_reaper_removes_every_finished_worker_from_map() {
        let root = temp_root("legacy-multi-reap");
        let config = test_config(&root);
        fs::create_dir_all(config.state_root.join("logs")).unwrap();
        let mut runtime = RuntimeState::default();
        for epoch in [700, 800] {
            let output = config.archive_root.join(format!("epoch-{epoch}"));
            write_legacy_registry_sidecars(&output, false);
            write_legacy_reader_core(&output);
            write_ownership(
                &output,
                LEGACY_COMPACT_OWNERSHIP_KIND,
                &epoch.to_string(),
                "compact_reuse",
                None,
            )
            .unwrap();
            let log = config
                .state_root
                .join("logs")
                .join(format!("epoch-{epoch}.log"));
            fs::write(&log, b"").unwrap();
            let child = make_finished_child(
                ChildKind::HistoricalCompactReuse { epoch },
                historical_progress_path(&config.state_root, epoch),
                log,
            )
            .await;
            runtime.legacy_compacts.insert(epoch, child);
        }

        reap_legacy_compacts(&config, &mut runtime);
        assert!(runtime.legacy_compacts.is_empty());
        for epoch in [700, 800] {
            assert_eq!(
                read_ownership(&config.archive_root.join(format!("epoch-{epoch}")))
                    .unwrap()
                    .state,
                "complete"
            );
        }
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
            br#"{"schema_version":1,"pid":42,"phase":"scan","state":"running","blocks_done":120,"transactions_done":900,"blocks_total_estimate":432000,"first_slot":302400000,"last_slot":302400120,"blocks_per_sec":20.5,"eta_secs":12.0,"updated_unix_secs":99}"#,
        )
        .unwrap();
        assert_eq!(progress.pid, Some(42));
        assert_eq!(progress.blocks_done, 120);
        assert_eq!(progress.blocks_total, 432_000);
        assert_eq!(progress.blocks_per_sec, Some(20.5));
        assert!(parse_progress_bytes(b"not json").is_err());
    }

    #[test]
    fn snapshot_json_has_stable_monitoring_contract() {
        let mut snapshot = empty_snapshot(true);
        snapshot.sequence = 7;
        snapshot.summary.queued = 2;
        snapshot
            .epochs
            .push(test_epoch(Path::new("/tmp"), 700, HistoricalState::Queued));
        let value = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(value["schema_version"], 2);
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
        assert_eq!(value["summary"]["legacy_compact_capacity_configured"], 0);
        assert_eq!(value["summary"]["legacy_compact_capacity_effective"], 0);
        assert_eq!(value["summary"]["legacy_compact_capacity_admitted"], 0);
        assert_eq!(value["summary"]["legacy_compact_active"], 0);
        assert_eq!(value["epochs"][0]["registry_order"], "unknown");

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
    fn legacy_compact_admission_aggregates_same_pass_and_active_growth() {
        let root = temp_root("legacy-multi-admission");
        let config = test_config(&root);
        let epochs = [700, 800, 900]
            .into_iter()
            .map(|epoch| {
                let snapshot = test_epoch(&root, epoch, HistoricalState::Queued);
                write_legacy_registry_sidecars(&snapshot.output_path, false);
                snapshot
            })
            .collect::<Vec<_>>();
        let gib = 1024 * 1024 * 1024;
        let mib = 1024 * 1024;
        let machine = MachineSnapshot {
            memory_total_bytes: 8 * gib,
            memory_available_bytes: config.memory_reserve_mib * mib + 2 * gib,
            // Swap must never increase admission headroom.
            swap_total_bytes: 64 * gib,
            disk_total_bytes: 2 * 1024 * gib,
            disk_available_bytes: config.disk_reserve_gib * gib + 2 * gib,
            ..MachineSnapshot::default()
        };
        let mut admission =
            LegacyCompactAdmission::new(&config, &machine, &epochs, &BTreeMap::new());
        for epoch in &epochs[..2] {
            assert!(admission.blocked_reason(&config, &machine, epoch).is_none());
            admission.reserve(epoch);
        }
        assert!(
            admission
                .blocked_reason(&config, &machine, &epochs[2])
                .is_some()
        );

        let active = BTreeMap::from([(700, 512 * mib)]);
        let active_machine = MachineSnapshot {
            memory_total_bytes: 8 * gib,
            // The active 1 GiB lane retains 512 MiB of future reservation;
            // exactly one more 1 GiB lane fits in the remaining envelope.
            memory_available_bytes: config.memory_reserve_mib * mib + 1536 * mib,
            swap_total_bytes: 64 * gib,
            disk_total_bytes: 2 * 1024 * gib,
            disk_available_bytes: config.disk_reserve_gib * gib + 2 * gib,
            ..MachineSnapshot::default()
        };
        let mut admission = LegacyCompactAdmission::new(&config, &active_machine, &epochs, &active);
        assert!(
            admission
                .blocked_reason(&config, &active_machine, &epochs[1])
                .is_none()
        );
        admission.reserve(&epochs[1]);
        assert!(
            admission
                .blocked_reason(&config, &active_machine, &epochs[2])
                .is_some()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn large_legacy_candidate_self_throttles_without_blocking_smaller_range() {
        let root = temp_root("legacy-large-self-throttle");
        let config = test_config(&root);
        let large = test_epoch(&root, 700, HistoricalState::Queued);
        let small = test_epoch(&root, 800, HistoricalState::Queued);
        write_legacy_registry_sidecars(&large.output_path, false);
        write_legacy_registry_sidecars(&small.output_path, false);
        OpenOptions::new()
            .write(true)
            .open(large.output_path.join(BLOCKHASH_REGISTRY_FILE))
            .unwrap()
            .set_len(2 * 1024 * 1024 * 1024)
            .unwrap();
        assert!(
            legacy_compact_memory_reservation_bytes(&large.output_path) > 2 * 1024 * 1024 * 1024
        );
        assert_eq!(
            legacy_compact_memory_reservation_bytes(&small.output_path),
            1024 * 1024 * 1024
        );
        let gib = 1024 * 1024 * 1024;
        let machine = MachineSnapshot {
            memory_total_bytes: 8 * gib,
            memory_available_bytes: config.memory_reserve_mib * 1024 * 1024 + gib,
            disk_total_bytes: 2 * 1024 * gib,
            disk_available_bytes: config.disk_reserve_gib * gib + 2 * gib,
            ..MachineSnapshot::default()
        };
        let admission = LegacyCompactAdmission::new(
            &config,
            &machine,
            &[large.clone(), small.clone()],
            &BTreeMap::new(),
        );
        assert!(
            admission
                .blocked_reason(&config, &machine, &large)
                .is_some()
        );
        assert!(
            admission
                .blocked_reason(&config, &machine, &small)
                .is_none()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_resource_capacity_is_minimum_of_hard_cpu_and_io_budgets() {
        let root = temp_root("legacy-resource-capacity");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 4;
        config.legacy_compact_cpu_cores_per_worker = 1;
        config.legacy_compact_cpu_budget_cores = 8;
        config.legacy_compact_io_mib_per_sec_per_worker = 120;
        config.legacy_compact_io_budget_mib_per_sec = 250;
        let capacity = legacy_compact_resource_capacity(&config);
        assert_eq!(capacity.cpu_slots, 8);
        assert_eq!(capacity.io_slots, 2);
        assert_eq!(capacity.effective_slots, 2);
        assert!(
            legacy_compact_resource_blocked_reason(&config, capacity)
                .unwrap()
                .contains("io_slots=2")
        );

        config.legacy_compact_cpu_cores_per_worker = 4;
        config.legacy_compact_cpu_budget_cores = 2;
        let capacity = legacy_compact_resource_capacity(&config);
        assert_eq!(capacity.effective_slots, 0);
        assert!(
            legacy_compact_resource_blocked_reason(&config, capacity)
                .unwrap()
                .contains("cpu_slots=0")
        );
    }

    #[test]
    fn legacy_capacity_reason_prefers_tighter_candidate_memory_blocker() {
        let root = temp_root("legacy-capacity-reason");
        let mut config = test_config(&root);
        allow_legacy_workers(&mut config, 4);
        config.legacy_compact_io_budget_mib_per_sec = 250;
        let large = test_epoch(&root, 700, HistoricalState::Queued);
        let small = test_epoch(&root, 800, HistoricalState::Queued);
        for epoch in [700, 800] {
            make_legacy_range_ready(&config, epoch);
        }
        OpenOptions::new()
            .write(true)
            .open(large.output_path.join(BLOCKHASH_REGISTRY_FILE))
            .unwrap()
            .set_len(2 * 1024 * 1024 * 1024)
            .unwrap();
        let machine = legacy_scheduler_machine(&config, 1024, 2);
        let (admitted, reason) = legacy_compact_capacity_admission(
            &config,
            &machine,
            &[large, small],
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert_eq!(legacy_compact_resource_capacity(&config).effective_slots, 2);
        assert_eq!(admitted, 1);
        assert!(reason.unwrap().contains("memory admission blocked"));
        fs::remove_dir_all(root).unwrap();
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
            state: LiveState::ReadyToPackage,
            capture_dir: config.live_root.join("epoch-700-capture-test"),
            output_path: Some(output.clone()),
            ready_to_package: true,
            repair_gate: false,
            first_slot: Some(700 * SLOTS_PER_EPOCH),
            last_slot: Some(700 * SLOTS_PER_EPOCH + 1),
            blocks_written: 2,
            artifacts: Vec::new(),
            progress: ProgressSnapshot::default(),
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
    async fn legacy_scheduler_fills_two_independent_heads_to_configured_cap() {
        let root = temp_root("legacy-cap-two");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        allow_legacy_workers(&mut config, 2);
        let mut snapshot = schedulable_snapshot(
            &root,
            [700, 800, 900]
                .into_iter()
                .map(|epoch| test_epoch(&root, epoch, HistoricalState::Queued))
                .collect(),
        );
        for epoch in [700, 800, 900] {
            make_legacy_range_ready(&config, epoch);
        }
        snapshot.machine = legacy_scheduler_machine(&config, 3 * 1024, 3);
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert_eq!(
            runtime.legacy_compacts.keys().copied().collect::<Vec<_>>(),
            vec![700, 800]
        );
        for child in runtime.legacy_compacts.values_mut() {
            let _ = child.child.wait().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn legacy_scheduler_uses_io_budget_as_effective_cap() {
        let root = temp_root("legacy-io-effective-cap");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        allow_legacy_workers(&mut config, 4);
        config.legacy_compact_cpu_budget_cores = 8;
        config.legacy_compact_io_budget_mib_per_sec = 250;
        let mut snapshot = schedulable_snapshot(
            &root,
            [700, 800, 900]
                .into_iter()
                .map(|epoch| test_epoch(&root, epoch, HistoricalState::Queued))
                .collect(),
        );
        for epoch in [700, 800, 900] {
            make_legacy_range_ready(&config, epoch);
        }
        snapshot.machine = legacy_scheduler_machine(&config, 4 * 1024, 4);
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert_eq!(legacy_compact_resource_capacity(&config).effective_slots, 2);
        assert_eq!(
            runtime.legacy_compacts.keys().copied().collect::<Vec<_>>(),
            vec![700, 800]
        );
        for child in runtime.legacy_compacts.values_mut() {
            let _ = child.child.wait().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn adopted_legacy_lane_counts_toward_cap_and_can_be_topped_up() {
        let root = temp_root("legacy-adopted-top-up");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        allow_legacy_workers(&mut config, 2);
        let mut snapshot = schedulable_snapshot(
            &root,
            vec![
                test_epoch(&root, 700, HistoricalState::Scanning),
                test_epoch(&root, 800, HistoricalState::Queued),
            ],
        );
        for epoch in [700, 800] {
            make_legacy_range_ready(&config, epoch);
        }
        let adopted_rss = 512 * 1024 * 1024;
        snapshot.machine = legacy_scheduler_machine(&config, 1536, 2);
        snapshot.lanes.push(LaneSnapshot {
            id: "compact_reuse:700".to_string(),
            kind: "historical_compact_reuse".to_string(),
            epoch: Some(700),
            capture_id: None,
            phase: "compact_reuse".to_string(),
            state: "running".to_string(),
            auto_paused: false,
            auto_pause_reason: None,
            pid: None,
            progress: ProgressSnapshot {
                rss_bytes: Some(adopted_rss),
                ..ProgressSnapshot::default()
            },
            rss_bytes: Some(adopted_rss),
            started_unix_secs: None,
            updated_unix_secs: unix_now(),
        });
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert_eq!(
            runtime.legacy_compacts.keys().copied().collect::<Vec<_>>(),
            vec![800]
        );
        let active = active_legacy_compact_rss(&snapshot, &runtime);
        assert_eq!(active.keys().copied().collect::<Vec<_>>(), vec![700, 800]);
        assert_eq!(active[&700], adopted_rss);
        assert_eq!(active[&800], 0);
        let _ = runtime
            .legacy_compacts
            .get_mut(&800)
            .unwrap()
            .child
            .wait()
            .await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn acquisition_that_fits_after_legacy_rss_release_stops_refill() {
        let root = temp_root("legacy-drain-for-acquisition");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        config.preflight_car = true;
        allow_legacy_workers(&mut config, 2);
        let mut snapshot = schedulable_snapshot(
            &root,
            vec![
                test_epoch(&root, 700, HistoricalState::Scanning),
                test_epoch(&root, 800, HistoricalState::Queued),
                test_epoch(&root, 900, HistoricalState::Queued),
            ],
        );
        for epoch in [700, 800] {
            make_legacy_range_ready(&config, epoch);
        }
        let adopted_rss = 2 * 1024 * 1024 * 1024;
        snapshot.machine = legacy_scheduler_machine(&config, 1024, 2);
        snapshot.lanes.push(LaneSnapshot {
            id: "compact_reuse:700".to_string(),
            kind: "historical_compact_reuse".to_string(),
            epoch: Some(700),
            capture_id: None,
            phase: "compact_reuse".to_string(),
            state: "running".to_string(),
            auto_paused: false,
            auto_pause_reason: None,
            pid: None,
            progress: ProgressSnapshot {
                rss_bytes: Some(adopted_rss),
                ..ProgressSnapshot::default()
            },
            rss_bytes: Some(adopted_rss),
            started_unix_secs: None,
            updated_unix_secs: unix_now(),
        });
        assert_eq!(
            acquisition_action(&config, &snapshot.epochs[2]),
            Some(AcquisitionAction::Preflight)
        );
        assert_eq!(
            acquisition_memory_capacity(&config, &snapshot.machine, 0, 0),
            0
        );
        let mut runtime = RuntimeState::default();
        assert!(legacy_priority_drain_requested(
            &config, &snapshot, &runtime
        ));

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.legacy_compacts.is_empty());
        assert!(runtime.acquisitions.is_empty());
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn ready_live_work_stops_legacy_top_up_until_adopted_lane_drains() {
        let root = temp_root("legacy-drain-for-live");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        allow_legacy_workers(&mut config, 2);
        let mut snapshot = schedulable_snapshot(
            &root,
            vec![
                test_epoch(&root, 700, HistoricalState::Scanning),
                test_epoch(&root, 800, HistoricalState::Queued),
            ],
        );
        for epoch in [700, 800] {
            make_legacy_range_ready(&config, epoch);
        }
        snapshot.machine = legacy_scheduler_machine(&config, 1536, 2);
        let adopted_rss = 1024 * 1024 * 1024;
        snapshot.lanes.push(LaneSnapshot {
            id: "compact_reuse:700".to_string(),
            kind: "historical_compact_reuse".to_string(),
            epoch: Some(700),
            capture_id: None,
            phase: "compact_reuse".to_string(),
            state: "running".to_string(),
            auto_paused: false,
            auto_pause_reason: None,
            pid: None,
            progress: ProgressSnapshot {
                rss_bytes: Some(adopted_rss),
                ..ProgressSnapshot::default()
            },
            rss_bytes: Some(adopted_rss),
            started_unix_secs: None,
            updated_unix_secs: unix_now(),
        });
        snapshot.finalizer_queue.push(FinalizerQueueItem {
            kind: "live".to_string(),
            epoch: Some(900),
            id: "ready-capture".to_string(),
            phase: "registry_merge".to_string(),
            state: "ready_to_package".to_string(),
            // This does not fit current MemAvailable, but does fit after the
            // consistently sampled adopted lane RSS is released.
            estimated_memory_bytes: 2 * 1024 * 1024 * 1024,
            estimated_disk_bytes: 1024 * 1024 * 1024,
            deferred_reason: None,
        });
        let mut runtime = RuntimeState::default();
        assert!(legacy_priority_drain_requested(
            &config, &snapshot, &runtime
        ));

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.legacy_compacts.is_empty());
        assert!(runtime.finalizer.is_none());

        // A live item that cannot pass admission must not suppress reuse lanes
        // forever; it will be reconsidered after machine headroom changes.
        snapshot.finalizer_queue[0].estimated_memory_bytes = 4 * 1024 * 1024 * 1024;
        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.legacy_compacts.contains_key(&800));
        let _ = runtime
            .legacy_compacts
            .get_mut(&800)
            .unwrap()
            .child
            .wait()
            .await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn legacy_spawn_failure_does_not_block_next_independent_head() {
        let root = temp_root("legacy-spawn-hol");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        allow_legacy_workers(&mut config, 2);
        let mut snapshot = schedulable_snapshot(
            &root,
            vec![
                test_epoch(&root, 700, HistoricalState::Queued),
                test_epoch(&root, 800, HistoricalState::Queued),
            ],
        );
        for epoch in [700, 800] {
            make_legacy_range_ready(&config, epoch);
        }
        // The filesystem inventory made 700 ready, but simulate its output
        // path changing before claim. The following independent head must
        // still be attempted in this scheduler pass.
        snapshot.epochs[0].output_path = root.join("changed-before-claim/epoch-700");
        snapshot.machine = legacy_scheduler_machine(&config, 2 * 1024, 2);
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.failures.contains_key("compact_reuse:700"));
        assert!(runtime.legacy_compacts.contains_key(&800));
        let errors_after_first_attempt = runtime.errors.len();
        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert_eq!(runtime.errors.len(), errors_after_first_attempt);
        let _ = runtime
            .legacy_compacts
            .get_mut(&800)
            .unwrap()
            .child
            .wait()
            .await;
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

    #[tokio::test]
    async fn disk_blocked_large_download_falls_through_to_fitting_legacy_head() {
        let root = temp_root("blocked-download-legacy-fallback");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        config.car_source_url_template =
            Some("https://example.invalid/epoch-{epoch}.car".to_string());
        let mut download = test_epoch(&root, 700, HistoricalState::Queued);
        download.input_path = None;
        let legacy = test_epoch(&root, 800, HistoricalState::Queued);
        let mut snapshot = schedulable_snapshot(&root, vec![download, legacy]);
        make_legacy_range_ready(&config, 800);
        snapshot.machine = legacy_scheduler_machine(&config, 4 * 1024, 2);
        snapshot.machine.car_disk_total_bytes = 2 * 1024 * 1024 * 1024 * 1024;
        snapshot.machine.car_disk_available_bytes =
            (config.disk_reserve_gib + 500) * 1024 * 1024 * 1024;
        assert_eq!(
            acquisition_action(&config, &snapshot.epochs[0]),
            Some(AcquisitionAction::Download)
        );
        assert!(
            car_download_remaining_projection(&config, &snapshot.epochs, 700)
                >= 1024 * 1024 * 1024 * 1024
        );
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.acquisitions.is_empty());
        assert!(runtime.legacy_compacts.contains_key(&800));
        let _ = runtime
            .legacy_compacts
            .get_mut(&800)
            .unwrap()
            .child
            .wait()
            .await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn blocked_acquisition_falls_through_stale_sweep_deferral_to_finalizer() {
        let root = temp_root("blocked-download-finalizer-fallback");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        config.car_source_url_template =
            Some("https://example.invalid/epoch-{epoch}.car".to_string());
        let mut download = test_epoch(&root, 700, HistoricalState::Queued);
        download.input_path = None;
        let finalizable = test_epoch(&root, 701, HistoricalState::ScanReady);
        let mut snapshot = schedulable_snapshot(&root, vec![download, finalizable]);
        snapshot.machine = legacy_scheduler_machine(&config, 4 * 1024, 2);
        snapshot.machine.car_disk_total_bytes = 2 * 1024 * 1024 * 1024 * 1024;
        snapshot.machine.car_disk_available_bytes =
            (config.disk_reserve_gib + 500) * 1024 * 1024 * 1024;
        snapshot.finalizer_queue[0].deferred_reason =
            Some("historical scan sweep in progress".to_string());
        let output = config.archive_root.join("epoch-701");
        fs::create_dir_all(&output).unwrap();
        write_scan_marker(&output);
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.acquisitions.is_empty());
        assert!(matches!(
            runtime.finalizer.as_ref().map(|child| &child.kind),
            Some(ChildKind::HistoricalFinalizer { epoch: 701 })
        ));
        let _ = runtime.finalizer.as_mut().unwrap().child.wait().await;
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
    fn parses_linux_psi_avg10() {
        let pressure = "some avg10=12.34 avg60=4.00 avg300=1.00 total=9\nfull avg10=5.67 avg60=2.00 avg300=0.50 total=3\n";
        assert_eq!(parse_psi_avg10(pressure, "some"), Some(12.34));
        assert_eq!(parse_psi_avg10(pressure, "full"), Some(5.67));
        assert_eq!(parse_psi_avg10(pressure, "missing"), None);
        assert_eq!(parse_psi_avg10("full avg10=NaN total=1", "full"), None);
        let process_io = "rchar: 1024\nread_bytes: 4096\nwrite_bytes: 8192\n";
        assert_eq!(
            parse_process_io_counter(process_io, "read_bytes:"),
            Some(4096)
        );
        assert_eq!(parse_process_io_counter(process_io, "missing:"), None);
    }

    #[test]
    fn adaptive_decision_uses_memory_hysteresis_and_treats_psi_as_telemetry() {
        let root = temp_root("adaptive-decision");
        let mut config = test_config(&root);
        allow_legacy_workers(&mut config, 3);
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_min_running = 1;
        config.legacy_compact_pause_cooldown = Duration::from_secs(30);
        let mib = 1024 * 1024;
        let resume_mib = config
            .memory_reserve_mib
            .saturating_add(config.legacy_compact_memory_guard_mib * 2);
        let mut machine = MachineSnapshot {
            memory_total_bytes: 16 * 1024 * 1024 * 1024,
            memory_available_bytes: resume_mib * mib,
            io_pressure_full_avg10: Some(25.0),
            ..MachineSnapshot::default()
        };
        assert_eq!(
            legacy_pressure_state(&config, &machine),
            LegacyPressureState::Resume,
            "high system-wide IO PSI alone must not pause useful work"
        );
        assert!(
            plan_legacy_adaptive_action(&config, &machine, 3, &[700, 900, 800], &[], 0, 100)
                .is_none()
        );

        let pause_mib = config
            .memory_reserve_mib
            .saturating_add(config.legacy_compact_memory_guard_mib);
        machine.memory_available_bytes = (pause_mib - 1) * mib;
        assert_eq!(
            plan_legacy_adaptive_action(&config, &machine, 3, &[700, 900, 800], &[], 0, 100,),
            Some(LegacyAdaptiveDecision::Pause {
                epoch: 900,
                reason: format!(
                    "MemAvailable {:.1} MiB is below pause threshold {} MiB; SIGSTOP arrests growth but RSS remains fully reserved",
                    (pause_mib - 1) as f64,
                    pause_mib,
                ),
                cause: LegacyAutoPauseCause::Memory,
            })
        );
        assert!(
            plan_legacy_adaptive_action(&config, &machine, 3, &[700, 800, 900], &[], 90, 100,)
                .is_none(),
            "cooldown must permit only one lane action"
        );
        assert!(
            plan_legacy_adaptive_action(&config, &machine, 1, &[900], &[], 0, 100,).is_none(),
            "minimum running must stop further pauses"
        );

        machine.memory_available_bytes = (resume_mib - 1) * mib;
        assert_eq!(
            legacy_pressure_state(&config, &machine),
            LegacyPressureState::Hold
        );

        machine.memory_available_bytes = resume_mib * mib;
        let paused = vec![
            AutoPausedLegacy {
                epoch: 800,
                pid: 8,
                process_start_ticks: None,
                reason: "pressure".to_string(),
                paused_unix_secs: 80,
                cause: LegacyAutoPauseCause::Memory,
            },
            AutoPausedLegacy {
                epoch: 900,
                pid: 9,
                process_start_ticks: None,
                reason: "pressure".to_string(),
                paused_unix_secs: 70,
                cause: LegacyAutoPauseCause::Memory,
            },
        ];
        assert_eq!(
            plan_legacy_adaptive_action(&config, &machine, 1, &[], &paused, 0, 100),
            Some(LegacyAdaptiveDecision::Resume {
                epoch: 900,
                reason: "MemAvailable crossed the resume threshold".to_string(),
                cause: LegacyAutoPauseCause::Memory,
            }),
            "the oldest memory auto-pause resumes first"
        );

        machine.memory_available_bytes = config
            .memory_reserve_mib
            .saturating_add(config.legacy_compact_memory_guard_mib)
            .saturating_sub(1)
            * mib;
        assert!(matches!(
            legacy_pressure_state(&config, &machine),
            LegacyPressureState::Pause(reason) if reason.contains("MemAvailable")
        ));
    }

    #[test]
    fn throughput_probe_accepts_the_measured_92_to_143_aggregate_gain() {
        let root = temp_root("throughput-accept");
        let mut config = test_config(&root);
        config.legacy_compact_throughput_probe_window = Duration::from_secs(10);
        config.legacy_compact_throughput_min_gain_pct = 5.0;
        let baseline = throughput_measurement(&[283], 92.0);
        let mut runtime = LegacyThroughputRuntime {
            probe: Some(LegacyThroughputProbe::Trial {
                baseline,
                trial_epoch: 406,
            }),
            ..LegacyThroughputRuntime::default()
        };
        let empty_paused = BTreeMap::new();
        let start = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 1_000, 100, "running"),
                throughput_lane(406, 40_600, 0, 100, "running"),
            ],
            100,
        );
        assert!(
            evaluate_legacy_throughput_probe(
                &config,
                &start,
                &mut runtime,
                &empty_paused,
                None,
                100,
            )
            .is_none()
        );
        let end = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 1_700, 110, "running"),
                throughput_lane(406, 40_600, 730, 110, "running"),
            ],
            110,
        );
        assert!(matches!(
            evaluate_legacy_throughput_probe(
                &config,
                &end,
                &mut runtime,
                &empty_paused,
                None,
                110,
            ),
            Some(LegacyThroughputAction::Record(action))
                if action.contains("92.0 -> 143.0 blocks/s")
        ));
        assert!(runtime.probe.is_none());
        assert_eq!(
            runtime
                .baseline
                .as_ref()
                .map(|measurement| measurement.blocks_per_sec),
            Some(143.0)
        );
    }

    #[test]
    fn throughput_probe_requires_stop_recovery_before_confirming_a_ceiling() {
        let root = temp_root("throughput-confirm");
        let mut config = test_config(&root);
        config.legacy_compact_throughput_probe_window = Duration::from_secs(10);
        config.legacy_compact_throughput_min_gain_pct = 5.0;
        let mut runtime = LegacyThroughputRuntime {
            probe: Some(LegacyThroughputProbe::Trial {
                baseline: throughput_measurement(&[283], 100.0),
                trial_epoch: 406,
            }),
            ..LegacyThroughputRuntime::default()
        };
        let trial_start = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 1_000, 100, "running"),
                throughput_lane(406, 40_600, 0, 100, "running"),
            ],
            100,
        );
        let empty_paused = BTreeMap::new();
        assert!(
            evaluate_legacy_throughput_probe(
                &config,
                &trial_start,
                &mut runtime,
                &empty_paused,
                None,
                100,
            )
            .is_none()
        );
        let trial_end = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 1_450, 110, "running"),
                throughput_lane(406, 40_600, 450, 110, "running"),
            ],
            110,
        );
        assert!(matches!(
            evaluate_legacy_throughput_probe(
                &config,
                &trial_end,
                &mut runtime,
                &empty_paused,
                None,
                110,
            ),
            Some(LegacyThroughputAction::Signal(
                LegacyAdaptiveDecision::Pause {
                    epoch: 406,
                    cause: LegacyAutoPauseCause::ThroughputProbe,
                    ..
                }
            ))
        ));
        assert!(matches!(
            runtime.probe,
            Some(LegacyThroughputProbe::Confirm {
                trial_epoch: 406,
                ..
            })
        ));
        let paused = BTreeMap::from([(
            406,
            AutoPausedLegacy {
                epoch: 406,
                pid: 40_600,
                process_start_ticks: None,
                reason: "confirmation".to_string(),
                paused_unix_secs: 110,
                cause: LegacyAutoPauseCause::ThroughputProbe,
            },
        )]);
        let confirm_start = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 1_450, 110, "running"),
                throughput_lane(406, 40_600, 450, 110, "paused"),
            ],
            110,
        );
        assert!(
            evaluate_legacy_throughput_probe(
                &config,
                &confirm_start,
                &mut runtime,
                &paused,
                None,
                110,
            )
            .is_none()
        );
        let confirm_end = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 2_550, 120, "running"),
                throughput_lane(406, 40_600, 450, 120, "paused"),
            ],
            120,
        );
        assert!(matches!(
            evaluate_legacy_throughput_probe(
                &config,
                &confirm_end,
                &mut runtime,
                &paused,
                None,
                120,
            ),
            Some(LegacyThroughputAction::ConfirmedSaturation { epoch: 406, reason })
                if reason.contains("A 100.0, B 90.0, A2 110.0")
        ));
        assert!(matches!(
            runtime.probe,
            Some(LegacyThroughputProbe::Saturated {
                trial_epoch: 406,
                ..
            })
        ));
    }

    #[test]
    fn throughput_probe_resumes_when_stopping_does_not_restore_throughput() {
        let root = temp_root("throughput-inconclusive");
        let mut config = test_config(&root);
        config.legacy_compact_throughput_probe_window = Duration::from_secs(10);
        let mut runtime = LegacyThroughputRuntime {
            probe: Some(LegacyThroughputProbe::Confirm {
                baseline: throughput_measurement(&[283], 100.0),
                trial: throughput_measurement(&[283, 406], 90.0),
                trial_epoch: 406,
            }),
            ..LegacyThroughputRuntime::default()
        };
        let paused = BTreeMap::from([(
            406,
            AutoPausedLegacy {
                epoch: 406,
                pid: 40_600,
                process_start_ticks: None,
                reason: "confirmation".to_string(),
                paused_unix_secs: 100,
                cause: LegacyAutoPauseCause::ThroughputProbe,
            },
        )]);
        let start = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 1_000, 100, "running"),
                throughput_lane(406, 40_600, 900, 100, "paused"),
            ],
            100,
        );
        let _ = evaluate_legacy_throughput_probe(&config, &start, &mut runtime, &paused, None, 100);
        let end = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 1_700, 110, "running"),
                throughput_lane(406, 40_600, 900, 110, "paused"),
            ],
            110,
        );
        assert!(matches!(
            evaluate_legacy_throughput_probe(&config, &end, &mut runtime, &paused, None, 110,),
            Some(LegacyThroughputAction::Signal(
                LegacyAdaptiveDecision::Resume {
                    epoch: 406,
                    cause: LegacyAutoPauseCause::ThroughputProbe,
                    ..
                }
            ))
        ));
        assert!(runtime.probe.is_none());
    }

    #[test]
    fn throughput_retry_waits_for_and_uses_a_fresh_baseline() {
        let root = temp_root("throughput-retry-baseline");
        let mut config = test_config(&root);
        config.legacy_compact_throughput_probe_window = Duration::from_secs(10);
        let original_a = throughput_measurement(&[283], 100.0);
        let trial = throughput_measurement(&[283, 406], 90.0);
        let original_a2 = throughput_measurement(&[283], 110.0);
        let mut runtime = LegacyThroughputRuntime {
            probe: Some(LegacyThroughputProbe::Saturated {
                baseline: original_a,
                trial,
                confirmation: original_a2,
                trial_epoch: 406,
                retry_unix_secs: 100,
            }),
            ..LegacyThroughputRuntime::default()
        };
        let paused = BTreeMap::from([(
            406,
            AutoPausedLegacy {
                epoch: 406,
                pid: 40_600,
                process_start_ticks: None,
                reason: "saturated".to_string(),
                paused_unix_secs: 90,
                cause: LegacyAutoPauseCause::ThroughputSaturated,
            },
        )]);
        let snapshot = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 10_000, 100, "running"),
                throughput_lane(406, 40_600, 900, 100, "paused"),
            ],
            100,
        );
        assert!(
            evaluate_legacy_throughput_probe(&config, &snapshot, &mut runtime, &paused, None, 100,)
                .is_none(),
            "backoff expiry must not reuse the stale A2"
        );
        assert!(matches!(
            runtime.probe,
            Some(LegacyThroughputProbe::Saturated { .. })
        ));

        runtime.last_measurement = Some(LegacyThroughputMeasurement {
            running_epochs: vec![283],
            started_unix_secs: 90,
            ended_unix_secs: 100,
            blocks_per_sec: 80.0,
            read_mib_per_sec: Some(40.0),
        });
        assert!(matches!(
            evaluate_legacy_throughput_probe(&config, &snapshot, &mut runtime, &paused, None, 101,),
            Some(LegacyThroughputAction::Signal(
                LegacyAdaptiveDecision::Resume {
                    epoch: 406,
                    cause: LegacyAutoPauseCause::ThroughputSaturated,
                    ..
                }
            ))
        ));
        assert!(matches!(
            runtime.probe,
            Some(LegacyThroughputProbe::Trial { baseline, trial_epoch: 406 })
                if baseline.blocks_per_sec == 80.0
        ));
    }

    #[test]
    fn priority_drain_selects_only_a_scheduler_owned_throughput_pause() {
        let mut runtime = RuntimeState::default();
        for (epoch, paused_at, cause) in [
            (283, 1, LegacyAutoPauseCause::Memory),
            (406, 2, LegacyAutoPauseCause::ThroughputSaturated),
            (512, 3, LegacyAutoPauseCause::ThroughputProbe),
        ] {
            runtime.auto_paused_legacy.insert(
                epoch,
                AutoPausedLegacy {
                    epoch,
                    pid: epoch as u32,
                    process_start_ticks: None,
                    reason: "test".to_string(),
                    paused_unix_secs: paused_at,
                    cause,
                },
            );
        }
        runtime.paused_jobs.insert("compact_reuse:406".to_string());

        assert!(matches!(
            plan_priority_drain_throughput_resume(&runtime),
            Some(LegacyAdaptiveDecision::Resume {
                epoch: 512,
                cause: LegacyAutoPauseCause::ThroughputProbe,
                ..
            })
        ));
    }

    #[test]
    fn memory_invalidated_probe_clears_when_auto_pause_ownership_is_gone() {
        let root = temp_root("throughput-memory-owner-gone");
        let config = test_config(&root);
        let mut runtime = LegacyThroughputRuntime {
            probe: Some(LegacyThroughputProbe::Confirm {
                baseline: throughput_measurement(&[283], 100.0),
                trial: throughput_measurement(&[283, 406], 90.0),
                trial_epoch: 406,
            }),
            probe_invalidated_by_memory: true,
            ..LegacyThroughputRuntime::default()
        };
        let snapshot = throughput_snapshot(
            vec![throughput_lane(283, 28_300, 1_000, 100, "running")],
            100,
        );
        assert!(matches!(
            evaluate_legacy_throughput_probe(
                &config,
                &snapshot,
                &mut runtime,
                &BTreeMap::new(),
                None,
                100,
            ),
            Some(LegacyThroughputAction::Record(action))
                if action.contains("ownership was already gone or transferred")
        ));
        assert!(runtime.probe.is_none());
        assert!(!runtime.probe_invalidated_by_memory);
    }

    #[test]
    fn steady_state_audit_repeats_stop_benefit_before_pausing_a_lane() {
        let root = temp_root("throughput-steady-audit");
        let mut config = test_config(&root);
        config.legacy_compact_throughput_probe_window = Duration::from_secs(10);
        config.legacy_compact_throughput_min_gain_pct = 5.0;
        config.legacy_compact_throughput_probe_backoff = Duration::from_secs(30);
        let mut runtime = LegacyThroughputRuntime {
            next_audit_unix_secs: 100,
            ..LegacyThroughputRuntime::default()
        };
        let none_paused = BTreeMap::new();
        let loaded_start = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 1_000, 90, "running"),
                throughput_lane(406, 40_600, 0, 90, "running"),
            ],
            90,
        );
        assert!(
            evaluate_legacy_throughput_probe(
                &config,
                &loaded_start,
                &mut runtime,
                &none_paused,
                Some(406),
                90,
            )
            .is_none()
        );
        let loaded_end = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 1_500, 100, "running"),
                throughput_lane(406, 40_600, 500, 100, "running"),
            ],
            100,
        );
        assert!(matches!(
            evaluate_legacy_throughput_probe(
                &config,
                &loaded_end,
                &mut runtime,
                &none_paused,
                Some(406),
                100,
            ),
            Some(LegacyThroughputAction::Signal(
                LegacyAdaptiveDecision::Pause {
                    epoch: 406,
                    cause: LegacyAutoPauseCause::ThroughputProbe,
                    ..
                }
            ))
        ));

        let paused = BTreeMap::from([(
            406,
            AutoPausedLegacy {
                epoch: 406,
                pid: 40_600,
                process_start_ticks: None,
                reason: "audit".to_string(),
                paused_unix_secs: 100,
                cause: LegacyAutoPauseCause::ThroughputProbe,
            },
        )]);
        let reduced_start = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 1_500, 100, "running"),
                throughput_lane(406, 40_600, 500, 100, "paused"),
            ],
            100,
        );
        let _ = evaluate_legacy_throughput_probe(
            &config,
            &reduced_start,
            &mut runtime,
            &paused,
            None,
            100,
        );
        let reduced_end = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 2_600, 110, "running"),
                throughput_lane(406, 40_600, 500, 110, "paused"),
            ],
            110,
        );
        assert!(matches!(
            evaluate_legacy_throughput_probe(
                &config,
                &reduced_end,
                &mut runtime,
                &paused,
                None,
                110,
            ),
            Some(LegacyThroughputAction::Signal(
                LegacyAdaptiveDecision::Resume {
                    epoch: 406,
                    cause: LegacyAutoPauseCause::ThroughputProbe,
                    ..
                }
            ))
        ));

        let reloaded_start = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 2_600, 110, "running"),
                throughput_lane(406, 40_600, 500, 110, "running"),
            ],
            110,
        );
        let _ = evaluate_legacy_throughput_probe(
            &config,
            &reloaded_start,
            &mut runtime,
            &none_paused,
            None,
            110,
        );
        let reloaded_end = throughput_snapshot(
            vec![
                throughput_lane(283, 28_300, 3_050, 120, "running"),
                throughput_lane(406, 40_600, 950, 120, "running"),
            ],
            120,
        );
        assert!(matches!(
            evaluate_legacy_throughput_probe(
                &config,
                &reloaded_end,
                &mut runtime,
                &none_paused,
                None,
                120,
            ),
            Some(LegacyThroughputAction::Signal(
                LegacyAdaptiveDecision::Pause {
                    epoch: 406,
                    cause: LegacyAutoPauseCause::ThroughputSaturated,
                    ..
                }
            ))
        ));
        assert!(matches!(
            runtime.probe,
            Some(LegacyThroughputProbe::Saturated {
                trial_epoch: 406,
                ..
            })
        ));
    }

    #[test]
    fn manual_pause_transfers_auto_ownership_and_resume_clears_both() {
        let mut runtime = RuntimeState::default();
        runtime.auto_paused_legacy.insert(
            700,
            AutoPausedLegacy {
                epoch: 700,
                pid: 70,
                process_start_ticks: None,
                reason: "IO pressure".to_string(),
                paused_unix_secs: 1,
                cause: LegacyAutoPauseCause::Memory,
            },
        );
        set_manual_pause_state(&mut runtime, "compact_reuse:700", Some(700), true);
        assert!(runtime.paused_jobs.contains("compact_reuse:700"));
        assert!(!runtime.auto_paused_legacy.contains_key(&700));
        set_manual_pause_state(&mut runtime, "compact_reuse:700", Some(700), false);
        assert!(!runtime.paused_jobs.contains("compact_reuse:700"));
        assert!(!runtime.auto_paused_legacy.contains_key(&700));
    }

    #[tokio::test]
    async fn paused_lane_api_and_exit_cleanup_are_consistent() {
        let root = temp_root("paused-lane-api");
        let config = test_config(&root);
        fs::create_dir_all(&config.state_root).unwrap();
        let progress_path = historical_progress_path(&config.state_root, 700);
        fs::create_dir_all(progress_path.parent().unwrap()).unwrap();
        fs::write(
            &progress_path,
            br#"{"pid":1,"state":"running","phase":"compact_reuse"}"#,
        )
        .unwrap();
        let child = make_finished_child(
            ChildKind::HistoricalCompactReuse { epoch: 700 },
            progress_path,
            config.state_root.join("lane.log"),
        )
        .await;
        let mut runtime = RuntimeState::default();
        runtime.paused_jobs.insert("compact_reuse:700".to_string());
        runtime.auto_paused_legacy.insert(
            700,
            AutoPausedLegacy {
                epoch: 700,
                pid: child.pid.unwrap_or_default(),
                process_start_ticks: None,
                reason: "memory pressure".to_string(),
                paused_unix_secs: 1,
                cause: LegacyAutoPauseCause::Memory,
            },
        );
        let lane = lane_from_child(&child, Some(700), None, unix_now(), &runtime);
        assert_eq!(lane.state, "paused");
        assert_eq!(lane.progress.state.as_deref(), Some("paused"));
        assert!(lane.auto_paused);
        clear_legacy_pause_state_after_exit(&config, &mut runtime, 700);
        assert!(!runtime.paused_jobs.contains("compact_reuse:700"));
        assert!(!runtime.auto_paused_legacy.contains_key(&700));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn paused_legacy_lane_remains_in_hard_rss_capacity() {
        let root = temp_root("paused-capacity");
        let snapshot_lane = LaneSnapshot {
            id: "compact_reuse:700".to_string(),
            kind: "historical_compact_reuse".to_string(),
            epoch: Some(700),
            capture_id: None,
            phase: "compact_reuse".to_string(),
            state: "paused".to_string(),
            auto_paused: true,
            auto_pause_reason: Some("IO pressure".to_string()),
            pid: Some(70),
            progress: ProgressSnapshot::default(),
            rss_bytes: Some(700 * 1024 * 1024),
            started_unix_secs: None,
            updated_unix_secs: 1,
        };
        let active = sampled_legacy_compact_rss(&[snapshot_lane], &RuntimeState::default());
        assert_eq!(active.len(), 1);
        assert_eq!(active[&700], 700 * 1024 * 1024);
        let _ = root;
    }

    #[tokio::test]
    async fn adaptive_ramp_starts_one_lane_per_cooldown() {
        let root = temp_root("adaptive-ramp");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        allow_legacy_workers(&mut config, 3);
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_min_running = 1;
        let mut snapshot = schedulable_snapshot(
            &root,
            [700, 800, 900]
                .into_iter()
                .map(|epoch| test_epoch(&root, epoch, HistoricalState::Queued))
                .collect(),
        );
        for epoch in [700, 800, 900] {
            make_legacy_range_ready(&config, epoch);
        }
        snapshot.machine = legacy_scheduler_machine(&config, 4 * 1024, 4);
        let mut runtime = RuntimeState::default();
        assert_eq!(
            top_up_legacy_compacts(&config, &snapshot, &mut runtime).await,
            1
        );
        assert_eq!(runtime.legacy_compacts.len(), 1);
        assert_eq!(
            top_up_legacy_compacts(&config, &snapshot, &mut runtime).await,
            0
        );
        for child in runtime.legacy_compacts.values_mut() {
            let _ = child.child.wait().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn trusted_adopted_completion_commits_only_exact_terminal_proof() {
        let root = temp_root("adopted-complete");
        let config = test_config(&root);
        let pid = u32::MAX - 100;
        assert!(!process_exists(pid));
        let compact = write_adopted_legacy_proof(&config, 700, pid, pid, "complete", true);
        let mut runtime = RuntimeState::default();
        runtime.adopted_legacy_compacts.insert(700, compact);
        reap_adopted_legacy_compacts(&config, &mut runtime);
        let owner = read_ownership(&config.archive_root.join("epoch-700")).unwrap();
        assert_eq!(owner.state, "complete");
        assert_eq!(owner.pid, None);
        assert!(!runtime.failures.contains_key("compact_reuse:700"));
        assert!(
            fs::read_to_string(config.state_root.join("control-events.jsonl"))
                .unwrap()
                .contains("adopted_legacy_complete")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn adopted_completion_rejects_progress_pid_state_and_invalid_core() {
        for (label, progress_pid_delta, state, valid_core) in [
            ("pid", 1u32, "complete", true),
            ("state", 0u32, "running", true),
            ("core", 0u32, "complete", false),
        ] {
            let root = temp_root(&format!("adopted-reject-{label}"));
            let config = test_config(&root);
            let pid = u32::MAX - 200;
            let compact = write_adopted_legacy_proof(
                &config,
                700,
                pid,
                pid - progress_pid_delta,
                state,
                valid_core,
            );
            let mut runtime = RuntimeState::default();
            runtime.adopted_legacy_compacts.insert(700, compact);
            reap_adopted_legacy_compacts(&config, &mut runtime);
            let owner = read_ownership(&config.archive_root.join("epoch-700")).unwrap();
            assert_eq!(owner.state, "failed", "case={label}");
            assert_ne!(owner.state, "complete", "case={label}");
            assert!(runtime.failures.contains_key("compact_reuse:700"));
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn adopted_completion_never_overwrites_changed_owner() {
        for variant in 0..5 {
            let root = temp_root(&format!("adopted-owner-change-{variant}"));
            let config = test_config(&root);
            let pid = u32::MAX - 300;
            let compact = write_adopted_legacy_proof(&config, 700, pid, pid, "complete", true);
            let output = config.archive_root.join("epoch-700");
            let mut owner = read_ownership(&output).unwrap();
            match variant {
                0 => owner.schema_version += 1,
                1 => owner.kind = "historical_scan".to_string(),
                2 => owner.id = "701".to_string(),
                3 => owner.state = "retry_ready".to_string(),
                4 => owner.pid = Some(pid - 1),
                _ => unreachable!(),
            }
            publish_ownership_marker(&output, &owner).unwrap();
            let before = fs::read(output.join(OWNERSHIP_MARKER)).unwrap();
            let mut runtime = RuntimeState::default();
            runtime.adopted_legacy_compacts.insert(700, compact);
            reap_adopted_legacy_compacts(&config, &mut runtime);
            let after = fs::read(output.join(OWNERSHIP_MARKER)).unwrap();
            assert_eq!(before, after, "variant={variant}");
            assert!(runtime.failures.contains_key("compact_reuse:700"));
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn live_pid_reuse_or_argv_change_is_tainted_and_kept_in_capacity() {
        let root = temp_root("adopted-taint");
        let config = test_config(&root);
        let pid = std::process::id();
        let mut compact = write_adopted_legacy_proof(&config, 700, pid, pid, "complete", true);
        compact.process_start_ticks = process_stat_identity(pid).unwrap().1.saturating_add(1);
        assert_eq!(
            adopted_legacy_process_state(&config, &compact),
            AdoptedLegacyProcessState::LiveIdentityChanged
        );
        let mut runtime = RuntimeState::default();
        runtime.adopted_legacy_compacts.insert(700, compact);
        reap_adopted_legacy_compacts(&config, &mut runtime);
        assert!(runtime.adopted_legacy_compacts.contains_key(&700));
        assert!(runtime.adopted_legacy_compacts[&700].identity_tainted);
        assert!(runtime.failures.contains_key("compact_reuse:700"));
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn auto_pause_recovery_clears_gone_but_retains_live_unprovable_pid() {
        let gone_root = temp_root("auto-recovery-gone");
        let gone_config = test_config(&gone_root);
        fs::create_dir_all(&gone_config.state_root).unwrap();
        let gone_state = test_app_state(gone_config);
        gone_state.runtime.lock().await.auto_paused_legacy.insert(
            700,
            AutoPausedLegacy {
                epoch: 700,
                pid: u32::MAX - 400,
                process_start_ticks: None,
                reason: "pressure".to_string(),
                paused_unix_secs: 1,
                cause: LegacyAutoPauseCause::Memory,
            },
        );
        recover_auto_paused_legacy(&gone_state).await.unwrap();
        assert!(
            gone_state
                .runtime
                .lock()
                .await
                .auto_paused_legacy
                .is_empty()
        );
        fs::remove_dir_all(gone_root).unwrap();

        let live_root = temp_root("auto-recovery-live");
        let live_config = test_config(&live_root);
        fs::create_dir_all(&live_config.state_root).unwrap();
        let live_state = test_app_state(live_config);
        let pid = std::process::id();
        live_state.runtime.lock().await.auto_paused_legacy.insert(
            700,
            AutoPausedLegacy {
                epoch: 700,
                pid,
                process_start_ticks: None,
                reason: "pressure".to_string(),
                paused_unix_secs: 1,
                cause: LegacyAutoPauseCause::Memory,
            },
        );
        assert!(recover_auto_paused_legacy(&live_state).await.is_err());
        assert!(
            live_state
                .runtime
                .lock()
                .await
                .auto_paused_legacy
                .contains_key(&700)
        );
        fs::remove_dir_all(live_root).unwrap();
    }

    #[tokio::test]
    async fn malformed_existing_control_state_fails_closed() {
        let root = temp_root("malformed-control-state");
        let config = test_config(&root);
        fs::create_dir_all(&config.state_root).unwrap();
        fs::write(config.state_root.join("control-state.json"), b"not-json").unwrap();
        let state = test_app_state(config);
        assert!(load_control_state(&state).await.is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn manual_pause_recovery_clears_only_gone_owner_and_retains_live_unknown() {
        let gone_root = temp_root("manual-recovery-gone");
        let gone_config = test_config(&gone_root);
        fs::create_dir_all(&gone_config.state_root).unwrap();
        let output = gone_config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        publish_ownership_marker(
            &output,
            &OwnershipMarker {
                schema_version: SCHEMA_VERSION,
                kind: LEGACY_COMPACT_OWNERSHIP_KIND.to_string(),
                id: "700".to_string(),
                state: "compact_reuse".to_string(),
                created_unix_secs: 1,
                updated_unix_secs: 1,
                message: None,
                pid: Some(u32::MAX - 500),
            },
        )
        .unwrap();
        let gone_state = test_app_state(gone_config);
        gone_state
            .runtime
            .lock()
            .await
            .paused_jobs
            .insert("compact_reuse:700".to_string());
        recover_manual_paused_legacy(&gone_state).await.unwrap();
        assert!(gone_state.runtime.lock().await.paused_jobs.is_empty());
        fs::remove_dir_all(gone_root).unwrap();

        let live_root = temp_root("manual-recovery-live");
        let live_config = test_config(&live_root);
        fs::create_dir_all(&live_config.state_root).unwrap();
        let output = live_config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        publish_ownership_marker(
            &output,
            &OwnershipMarker {
                schema_version: SCHEMA_VERSION,
                kind: LEGACY_COMPACT_OWNERSHIP_KIND.to_string(),
                id: "700".to_string(),
                state: "compact_reuse".to_string(),
                created_unix_secs: 1,
                updated_unix_secs: 1,
                message: None,
                pid: Some(std::process::id()),
            },
        )
        .unwrap();
        let live_state = test_app_state(live_config);
        live_state
            .runtime
            .lock()
            .await
            .paused_jobs
            .insert("compact_reuse:700".to_string());
        assert!(recover_manual_paused_legacy(&live_state).await.is_err());
        assert!(
            live_state
                .runtime
                .lock()
                .await
                .paused_jobs
                .contains("compact_reuse:700")
        );
        fs::remove_dir_all(live_root).unwrap();
    }

    #[tokio::test]
    async fn startup_rejects_live_unprovable_and_pidless_compact_owner() {
        let live_root = temp_root("track-live-mismatch");
        let live_config = test_config(&live_root);
        let output = live_config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        publish_ownership_marker(
            &output,
            &OwnershipMarker {
                schema_version: SCHEMA_VERSION,
                kind: LEGACY_COMPACT_OWNERSHIP_KIND.to_string(),
                id: "700".to_string(),
                state: "compact_reuse".to_string(),
                created_unix_secs: 1,
                updated_unix_secs: 1,
                message: None,
                pid: Some(std::process::id()),
            },
        )
        .unwrap();
        let state = test_app_state(live_config);
        assert!(track_adopted_legacy_compacts(&state).await.is_err());
        fs::remove_dir_all(live_root).unwrap();

        let pidless_root = temp_root("track-pidless");
        let mut pidless_config = test_config(&pidless_root);
        pidless_config.start_epoch = Some(800);
        pidless_config.end_epoch = Some(800);
        let output = pidless_config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        publish_ownership_marker(
            &output,
            &OwnershipMarker {
                schema_version: SCHEMA_VERSION,
                kind: LEGACY_COMPACT_OWNERSHIP_KIND.to_string(),
                id: "700".to_string(),
                state: "compact_reuse".to_string(),
                created_unix_secs: 1,
                updated_unix_secs: 1,
                message: None,
                pid: None,
            },
        )
        .unwrap();
        let state = test_app_state(pidless_config.clone());
        assert!(track_adopted_legacy_compacts(&state).await.is_err());
        let mut old_owner = read_ownership(&output).unwrap();
        old_owner.schema_version = SCHEMA_VERSION.saturating_sub(1);
        publish_ownership_marker(&output, &old_owner).unwrap();
        let state = test_app_state(pidless_config);
        assert!(
            track_adopted_legacy_compacts(&state).await.is_err(),
            "old-schema pidless and out-of-scope active claims must also fail closed"
        );
        fs::remove_dir_all(pidless_root).unwrap();
    }

    #[test]
    fn legacy_adoption_requires_byte_exact_scheduler_argv() {
        let root = temp_root("exact-legacy-argv");
        let config = test_config(&root);
        make_legacy_range_ready(&config, 700);
        let expected = expected_legacy_compact_argv(&config, 700).unwrap();
        let encode = |argv: &[Vec<u8>]| {
            argv.iter()
                .flat_map(|arg| arg.iter().copied().chain(std::iter::once(0)))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            legacy_compact_argv_matches_bytes(&config, 700, &encode(&expected)),
            Some(true)
        );
        let mut wrong_input = expected.clone();
        wrong_input[2] = b"/wrong/epoch-700.car".to_vec();
        assert_eq!(
            legacy_compact_argv_matches_bytes(&config, 700, &encode(&wrong_input)),
            Some(false)
        );
        let mut extra = expected.clone();
        extra.push(b"--unexpected".to_vec());
        assert_eq!(
            legacy_compact_argv_matches_bytes(&config, 700, &encode(&extra)),
            Some(false)
        );
        let mut extra_empty = expected.clone();
        extra_empty.push(Vec::new());
        assert_eq!(
            legacy_compact_argv_matches_bytes(&config, 700, &encode(&extra_empty)),
            Some(false)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn legacy_spawn_is_group_leader_and_removes_stale_progress() {
        let root = temp_root("legacy-pg-spawn");
        let config = test_config(&root);
        fs::create_dir_all(&config.state_root).unwrap();
        let output = config.archive_root.join("epoch-700");
        fs::create_dir_all(&output).unwrap();
        write_ownership(
            &output,
            LEGACY_COMPACT_OWNERSHIP_KIND,
            "700",
            "compact_reuse",
            None,
        )
        .unwrap();
        let progress = historical_progress_path(&config.state_root, 700);
        fs::create_dir_all(progress.parent().unwrap()).unwrap();
        fs::write(&progress, br#"{"pid":1,"state":"complete"}"#).unwrap();
        let mut child = spawn_command_child(
            &config,
            Path::new("/bin/sleep"),
            vec!["60".into()],
            ChildKind::HistoricalCompactReuse { epoch: 700 },
            progress.clone(),
            config.state_root.join("sleep.log"),
            None,
        )
        .await
        .unwrap();
        let pid = child.pid.unwrap();
        assert!(process_is_group_leader(pid));
        assert_eq!(
            controlled_signal_target("historical_compact_reuse", pid, true),
            -(pid as libc::pid_t)
        );
        assert!(
            !progress.exists(),
            "stale complete progress must be removed before exec"
        );
        // SAFETY: the test child is an isolated group leader.
        let _ = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
        let _ = child.child.wait().await;

        let mut ordinary = Command::new("/bin/sleep").arg("60").spawn().unwrap();
        let ordinary_pid = ordinary.id().unwrap();
        assert!(!process_is_group_leader(ordinary_pid));
        assert_eq!(
            controlled_signal_target("historical_compact_reuse", ordinary_pid, false),
            ordinary_pid as libc::pid_t
        );
        let _ = ordinary.kill().await;
        let _ = ordinary.wait().await;
        fs::remove_dir_all(root).unwrap();
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
