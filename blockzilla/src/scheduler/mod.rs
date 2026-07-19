use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
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
    os::{
        fd::AsRawFd,
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
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
use tower_http::cors::CorsLayer;

mod cli;

pub use cli::SchedulerArgs;

#[allow(dead_code)]
#[path = "live_materialize_task_model.rs"]
mod live_materialize_task_model;

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
const BLOCKHASH_INDEX_V3_FILE: &str = "blockhash_index_v3.bin";
const BLOCKHASH_INDEX_V3_TMP_FILE: &str = "blockhash_index_v3.bin.tmp";
const BLOCK_TIME_GAP_FILE: &str = "block-time-gaps.bin";
const BLOCK_TIME_GAP_LOCK_FILE: &str = ".block-time-gaps.bin.lock";
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
const LIVE_REPAIR_SOURCE_MATERIALIZED_MARKER: &str = "repair/source-REPAIR-MATERIALIZED.json";
const LIVE_REPAIR_PLAN_FILE: &str = "repair/live-merge-plan.jsonl";
const LIVE_REPAIR_AVAILABLE_POH_FILE: &str = "repair/available-poh.wincode";
const MAX_LIVE_REPAIR_MARKER_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LIVE_REPAIR_COMPACTED_MARKER_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LIVE_REPAIR_META_BYTES: u64 = 2 * 1024 * 1024;
const MAX_LIVE_REPAIR_POH_FRAME_BYTES: u64 = 64 * 1024 * 1024;
const MAX_LIVE_REPAIR_PLAN_BYTES: u64 = 256 * 1024 * 1024;
const MAX_LIVE_REPAIR_SOURCES: usize = 256;
const MAX_LIVE_REPAIR_SOURCE_PATH_BYTES: usize = 4 * 1024;
const PROGRESS_STALE_SECS: u64 = 120;
const PROGRESS_MONITOR_INTERVAL: Duration = Duration::from_secs(1);
const LIVE_SLOT_RATE_MIN_WINDOW_SECS: u64 = 15;
const LIVE_SLOT_RATE_WINDOW_SECS: u64 = 60;
const LIVE_SLOT_RATE_MAX_SAMPLES: usize = 128;
const MAX_ERRORS: usize = 100;
// Kept byte-for-byte so a renamed scheduler can adopt durable work from
// deployments that created this marker before scheduling moved to Blockzilla.
const OWNERSHIP_MARKER: &str = ".hivezilla-pipeline-owned.v1.json";
// Shell argv[0] is also a durable process-identity boundary for already
// running downloads. Renaming it would prevent safe adoption after upgrade.
const LEGACY_CAR_DOWNLOAD_ARGV0: &str = "hivezilla-car-download";
const HIVEZILLA_EXECUTABLE: &[u8] = b"hivezilla";
const LEGACY_LIVE_PRODUCER_EXECUTABLE: &[u8] = b"blockzilla-live-producer";
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
const LEGACY_COMPACT_MEMORY_OVERHEAD_MIB: u64 = 512;
const LEGACY_TUNER_SETTLE_SECS: u64 = 180;
const LEGACY_TUNER_MIN_SAMPLES: usize = 12;
const LEGACY_TUNER_PROBE_COOLDOWN_SECS: u64 = 60;
const LEGACY_TUNER_BACKOFF_SECS: u64 = 300;
const LEGACY_TUNER_CONFIRMED_DOWNSHIFT_BACKOFF_SECS: u64 = 3_600;
const LEGACY_TUNER_MIN_GAIN_MIB_PER_SEC: f64 = 8.0;
const LEGACY_TUNER_MIN_GAIN_RATIO: f64 = 0.05;
const LEGACY_TUNER_DEGRADATION_MIN_MIB_PER_SEC: f64 = 16.0;
const LEGACY_TUNER_DEGRADATION_MIN_RATIO: f64 = 0.10;
const LEGACY_TUNER_DEGRADATION_WINDOWS: usize = 2;
const LEGACY_TUNER_DOWNSHIFT_TIMEOUT_SECS: u64 = LEGACY_TUNER_SETTLE_SECS * 2;
const LEGACY_TUNER_ACTION_TIMEOUT_SECS: u64 = 30;
const LEGACY_TUNER_RATE_FRESH_SECS: u64 = 30;
const LEGACY_TUNER_LIVE_STALL_SECS: u64 = 60;
const SOURCE_READ_RATE_MIN_WINDOW_SECS: f64 = 15.0;
const SOURCE_READ_RATE_MAX_WINDOW_SECS: f64 = 60.0;
const SOURCE_READ_RATE_MAX_SAMPLES: usize = 32;
const LEGACY_TUNER_PAUSE_PREFIX: &str = "throughput probe rejected:";
const LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX: &str = "throughput tuner downshift:";
const LEGACY_TUNER_GUARD_PAUSE_PREFIX: &str = "throughput tuner hard guard:";
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
pub struct SchedulerConfig {
    /// Public, read-only monitoring API. The default CLI value is loopback.
    pub status_bind: SocketAddr,
    /// Optional mutation API. It is intentionally limited to loopback.
    pub management_bind: Option<SocketAddr>,
    pub blockzilla_bin: PathBuf,
    pub repair_blockzilla_bin: Option<PathBuf>,
    pub car_root: PathBuf,
    pub archive_root: PathBuf,
    pub live_root: PathBuf,
    pub state_root: PathBuf,
    pub scan_concurrency: usize,
    /// Zero removes the arbitrary lane-count ceiling. CPU, memory, disk,
    /// pressure, and measured marginal throughput remain hard admission gates.
    pub legacy_compact_concurrency: usize,
    /// Optional compatibility ceiling while a finalizer is active. In adaptive
    /// mode zero discovers useful overlap without a cap; in fixed mode zero is
    /// strict finalizer exclusivity.
    pub legacy_compact_finalizer_overlap: usize,
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
    /// Bytes consumed from the concrete CAR source file. Unlike
    /// `input_mib_per_sec`, these bytes stay in the same domain as
    /// `EpochSnapshot::car_bytes`, including for `.car.zst` inputs.
    #[serde(default)]
    pub source_bytes_done: Option<u64>,
    #[serde(default)]
    pub source_bytes_total: Option<u64>,
    /// Controller-sampled advance rate of the concrete CAR file descriptor.
    #[serde(default)]
    pub source_read_mib_per_sec: Option<f64>,
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
    #[serde(serialize_with = "serialize_optional_path_basename")]
    pub input_path: Option<PathBuf>,
    #[serde(serialize_with = "serialize_path_basename")]
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
    /// True for the single capture that represents the current live epoch.
    /// Closed captures waiting for compaction remain visible but are never
    /// marked current.
    #[serde(default)]
    pub is_current: bool,
    pub state: LiveState,
    #[serde(serialize_with = "serialize_path_basename")]
    pub capture_dir: PathBuf,
    #[serde(serialize_with = "serialize_optional_path_basename")]
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
    /// Explicit aliases keep the monitoring API convenient while the nested
    /// progress object remains backward compatible.
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

fn serialize_path_basename<S>(path: &Path, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let basename = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_default();
    serializer.serialize_str(&basename)
}

fn serialize_optional_path_basename<S>(
    path: &Option<PathBuf>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match path.as_deref().and_then(Path::file_name) {
        Some(name) => serializer.serialize_some(&name.to_string_lossy()),
        None => serializer.serialize_none(),
    }
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
    /// Runnable CAR bytes that remain unread. This is the numerator of the
    /// global queue ETA and deliberately excludes action-required items.
    #[serde(default)]
    pub queue_bytes_remaining: u64,
    /// Aggregate concrete CAR-source read rate. Worker count is never used to
    /// scale the queue ETA.
    #[serde(default)]
    pub queue_read_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub queue_read_active_workers: usize,
    #[serde(default)]
    pub queue_read_sampled_workers: usize,
    pub scan_capacity_configured: usize,
    pub scan_capacity_admitted: usize,
    pub admission_blocked_reason: Option<String>,
    #[serde(default)]
    pub legacy_compact_capacity_configured: usize,
    #[serde(default)]
    pub legacy_compact_capacity_unbounded: bool,
    #[serde(default)]
    pub legacy_compact_capacity_effective: usize,
    #[serde(default)]
    pub legacy_compact_capacity_admitted: usize,
    #[serde(default)]
    pub legacy_compact_finalizer_overlap: usize,
    #[serde(default)]
    pub legacy_compact_tuning_enabled: bool,
    #[serde(default)]
    pub legacy_compact_tuning_state: Option<String>,
    #[serde(default)]
    pub legacy_compact_tuning_target: usize,
    #[serde(default)]
    pub legacy_compact_tuning_accepted_lanes: usize,
    #[serde(default)]
    pub legacy_compact_tuning_baseline_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub legacy_compact_tuning_objective_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub legacy_compact_tuning_rate_source: Option<String>,
    #[serde(default)]
    pub legacy_compact_useful_input_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub legacy_compact_useful_input_active_lanes: usize,
    #[serde(default)]
    pub legacy_compact_useful_input_sampled_lanes: usize,
    #[serde(default)]
    pub legacy_compact_tuning_backoff_until_unix_secs: Option<u64>,
    #[serde(default)]
    pub legacy_compact_tuning_last_decision: Option<String>,
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
    config: SchedulerConfig,
    snapshot: RwLock<PipelineSnapshot>,
    updates: broadcast::Sender<RealtimeEnvelope<SnapshotPatch>>,
    sequence: AtomicU64,
    publication: Mutex<PublicationState>,
    runtime: Mutex<RuntimeState>,
}

#[derive(Debug, Default)]
struct PublicationState {
    live_slot_rate_windows: BTreeMap<String, LiveSlotRateWindow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LiveSlotRateSample {
    updated_unix_secs: u64,
    last_slot: u64,
}

#[derive(Debug, Default)]
struct LiveSlotRateWindow {
    pid: Option<u32>,
    samples: VecDeque<LiveSlotRateSample>,
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
    adopted_legacy_compacts: BTreeMap<u64, AdoptedLegacyCompact>,
    process_io_samples: BTreeMap<String, ProcessIoSample>,
    source_input_samples: BTreeMap<String, VecDeque<SourceInputSample>>,
    source_reader_topology: BTreeSet<(String, u32, u64)>,
    archive_device_io_sample: Option<BlockDeviceIoSample>,
    legacy_throughput_tuner: LegacyThroughputTuner,
    legacy_tuner_profiles: LegacyTunerProfiles,
    inventory_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyTunerContext {
    Bulk,
    FinalizerOverlap,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
struct LegacyTunerProfile {
    #[serde(default)]
    accepted_lanes: usize,
    #[serde(default)]
    accepted_useful_mib_per_sec: Option<f64>,
    #[serde(default)]
    accepted_rate_source: Option<LegacyTunerRateSource>,
    #[serde(default)]
    accepted_device_mib_per_sec: Option<f64>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
struct LegacyTunerProfiles {
    #[serde(default)]
    bulk: LegacyTunerProfile,
    #[serde(default)]
    finalizer_overlap: LegacyTunerProfile,
}

#[derive(Debug, Default)]
struct LegacyThroughputTuner {
    context: Option<LegacyTunerContext>,
    phase: LegacyTunerPhase,
    guard_held: bool,
    accepted_lanes: usize,
    accepted_useful_mib_per_sec: Option<f64>,
    accepted_rate_source: Option<LegacyTunerRateSource>,
    accepted_device_mib_per_sec: Option<f64>,
    next_probe_unix_secs: u64,
    backoff_until_unix_secs: u64,
    window: Option<LegacyTunerWindow>,
    pending_action: Option<LegacyTunerAction>,
    pending_action_started_unix_secs: u64,
    probe_identity: Option<(u64, u32, LegacyLaneStartIdentity)>,
    current_useful_mib_per_sec: Option<f64>,
    current_rate_source: Option<LegacyTunerRateSource>,
    current_active_lanes: usize,
    current_sampled_lanes: usize,
    last_decision: Option<String>,
    degraded_windows: usize,
    degraded_identities: Option<BTreeSet<(u64, u32, LegacyLaneStartIdentity)>>,
    baseline_ready: bool,
    audit_loaded_identities: Option<BTreeSet<(u64, u32, LegacyLaneStartIdentity)>>,
    audit_survivor_identities: Option<BTreeSet<(u64, u32, LegacyLaneStartIdentity)>>,
    audit_started_unix_secs: u64,
    live_slot_watch: BTreeMap<String, LegacyLiveSlotWatch>,
}

#[derive(Debug, Clone, Copy)]
struct LegacyLiveSlotWatch {
    pid: u32,
    slot: u64,
    last_advanced_unix_secs: u64,
}

#[derive(Debug, Default, Clone, Copy)]
enum LegacyTunerPhase {
    #[default]
    ObserveBaseline,
    ProbeUp {
        baseline_lanes: usize,
        baseline_mib_per_sec: f64,
        probe_epoch: u64,
    },
    VerifyPause {
        baseline_lanes: usize,
        baseline_mib_per_sec: f64,
        probe_mib_per_sec: f64,
        probe_epoch: u64,
    },
    VerifyDownshift {
        loaded_lanes: usize,
        loaded_mib_per_sec: f64,
        audit_epoch: u64,
    },
    VerifyDownshiftReloaded {
        loaded_lanes: usize,
        loaded_mib_per_sec: f64,
        reduced_mib_per_sec: f64,
        reduced_device_mib_per_sec: Option<f64>,
        audit_epoch: u64,
    },
    CommitDownshift {
        loaded_lanes: usize,
        reduced_mib_per_sec: f64,
        reduced_device_mib_per_sec: Option<f64>,
        audit_epoch: u64,
    },
    Backoff,
}

#[derive(Debug)]
struct LegacyTunerWindow {
    identities: BTreeSet<(u64, u32, LegacyLaneStartIdentity)>,
    started_unix_secs: u64,
    useful_samples: VecDeque<f64>,
    device_samples: VecDeque<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LegacyLaneStartIdentity {
    /// Stable kernel process start time since boot. This is the preferred
    /// identity on Linux and remains valid even when a legacy worker has no
    /// wall-clock progress timestamp.
    LinuxStartTicks(u64),
    /// Portable fallback used by tests and non-Linux observers.
    #[cfg_attr(target_os = "linux", allow(dead_code))]
    UnixSecs(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LegacyTunerAction {
    Pause {
        epoch: u64,
        expected_identity: Option<(u64, u32, LegacyLaneStartIdentity)>,
        reason: String,
    },
    Resume {
        epoch: u64,
        expected_identity: Option<(u64, u32, LegacyLaneStartIdentity)>,
        reason: String,
    },
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

#[derive(Debug, Clone)]
struct SourceInputSample {
    pid: u32,
    process_start_ticks: u64,
    device: u64,
    inode: u64,
    bytes_done: u64,
    bytes_total: u64,
    running: bool,
    sampled_at: Instant,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AutoPausedLegacy {
    epoch: u64,
    pid: u32,
    #[serde(default)]
    process_start_ticks: Option<u64>,
    reason: String,
    paused_unix_secs: u64,
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

fn effective_repair_blockzilla_bin(config: &SchedulerConfig) -> &Path {
    config
        .repair_blockzilla_bin
        .as_deref()
        .unwrap_or(&config.blockzilla_bin)
}

pub async fn run_scheduler(config: SchedulerConfig) -> Result<()> {
    validate_management_bind(config.management_bind)?;
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
    let legacy_resource = legacy_compact_resource_capacity(&config);
    anyhow::ensure!(
        !config.legacy_compact_auto_pause
            || config.legacy_compact_min_running <= legacy_resource.effective_slots,
        "legacy compact minimum running ({}) exceeds effective capacity ({})",
        config.legacy_compact_min_running,
        legacy_resource.effective_slots,
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
        if let Some(repair_bin) = config.repair_blockzilla_bin.as_deref() {
            anyhow::ensure!(
                is_nonempty_file(repair_bin),
                "repair blockzilla executable is missing or empty: {}",
                repair_bin.display()
            );
        }
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
        publication: Mutex::new(PublicationState::default()),
        runtime: Mutex::new(RuntimeState::default()),
    });
    load_persisted_errors(&state).await;
    load_control_state(&state).await?;
    load_acquisition_failures(&state).await;

    // Bind before the first reconciliation. This also protects an upgrade
    // from an older binary that did not yet participate in pipeline.lock: a
    // port collision cannot launch work and then fail startup.
    let status_listener = tokio::net::TcpListener::bind(config.status_bind)
        .await
        .with_context(|| {
            format!(
                "bind Blockzilla scheduler status API on {}",
                config.status_bind
            )
        })?;
    let management_listener = if let Some(bind) = config.management_bind {
        Some(
            tokio::net::TcpListener::bind(bind)
                .await
                .with_context(|| format!("bind Blockzilla scheduler management API on {bind}"))?,
        )
    } else {
        None
    };
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

    let progress_state = Arc::clone(&state);
    let progress_monitor = tokio::spawn(async move {
        let mut interval = tokio::time::interval(PROGRESS_MONITOR_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            monitor_active_progress(&progress_state).await;
        }
    });

    let status_app = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/v1/status", get(status))
        .route("/api/v1/events", get(events))
        .layer(CorsLayer::permissive())
        .with_state(Arc::clone(&state));
    let result = if let Some(listener) = management_listener {
        let management_app = Router::new()
            .route("/api/v1/control/pause", post(pause_scheduler))
            .route("/api/v1/control/resume", post(resume_scheduler))
            .route("/api/v1/jobs/{kind}/{id}/pause", post(pause_job))
            .route("/api/v1/jobs/{kind}/{id}/resume", post(resume_job))
            .route("/api/v1/jobs/{kind}/{id}/retry", post(retry_job))
            .with_state(Arc::clone(&state))
            .layer(middleware::from_fn_with_state(
                state,
                enforce_management_request,
            ));
        tokio::select! {
            result = axum::serve(status_listener, status_app) => {
                result.context("run Blockzilla scheduler status API")
            }
            result = axum::serve(listener, management_app) => {
                result.context("run Blockzilla scheduler management API")
            }
        }
    } else {
        axum::serve(status_listener, status_app)
            .await
            .context("run Blockzilla scheduler status API")
    };
    scheduler.abort();
    let _ = scheduler.await;
    progress_monitor.abort();
    let _ = progress_monitor.await;
    result
}

fn validate_management_bind(bind: Option<SocketAddr>) -> Result<()> {
    if let Some(bind) = bind {
        anyhow::ensure!(
            bind.ip().is_loopback(),
            "scheduler management API must bind to a loopback address, got {bind}"
        );
        anyhow::ensure!(
            bind.port() != 0,
            "scheduler management API requires an explicit nonzero port"
        );
    }
    Ok(())
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
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    Internal(anyhow::Error),
}

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Disabled(message) => (StatusCode::FORBIDDEN, message),
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

fn authorize_control(config: &SchedulerConfig) -> Result<(), ControlError> {
    if !config.execute {
        return Err(ControlError::Disabled(
            "controls are disabled in observer mode".to_string(),
        ));
    }
    if config.management_bind.is_none() {
        return Err(ControlError::Disabled(
            "scheduler management API is disabled".to_string(),
        ));
    }
    Ok(())
}

fn authorize_management_request(
    config: &SchedulerConfig,
    headers: &HeaderMap,
) -> Result<(), ControlError> {
    authorize_control(config)?;
    let bind = config.management_bind.ok_or_else(|| {
        ControlError::Disabled("scheduler management API is disabled".to_string())
    })?;
    let expected_host = bind.to_string();
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ControlError::Disabled("management Host header is required".to_string()))?;
    if host != expected_host {
        return Err(ControlError::Disabled(
            "management Host must match the configured loopback listener".to_string(),
        ));
    }

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if !content_type.is_some_and(|value| value.eq_ignore_ascii_case("application/json")) {
        return Err(ControlError::Disabled(
            "management requests require Content-Type: application/json".to_string(),
        ));
    }

    if let Some(origin) = headers.get(header::ORIGIN) {
        let origin = origin.to_str().map_err(|_| {
            ControlError::Disabled("management Origin header is invalid".to_string())
        })?;
        let http_origin = format!("http://{expected_host}");
        let https_origin = format!("https://{expected_host}");
        if origin != http_origin && origin != https_origin {
            return Err(ControlError::Disabled(
                "cross-origin management requests are forbidden".to_string(),
            ));
        }
    }

    if let Some(fetch_site) = headers.get("sec-fetch-site") {
        let fetch_site = fetch_site.to_str().map_err(|_| {
            ControlError::Disabled("management Sec-Fetch-Site header is invalid".to_string())
        })?;
        if fetch_site != "same-origin" {
            return Err(ControlError::Disabled(
                "cross-site management requests are forbidden".to_string(),
            ));
        }
    }
    Ok(())
}

async fn enforce_management_request(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    match authorize_management_request(&state.config, request.headers()) {
        Ok(()) => next.run(request).await,
        Err(error) => error.into_response(),
    }
}

async fn pause_scheduler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ControlResponse>, ControlError> {
    scheduler_control(state, true).await
}

async fn resume_scheduler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ControlResponse>, ControlError> {
    scheduler_control(state, false).await
}

async fn scheduler_control(
    state: Arc<AppState>,
    paused: bool,
) -> Result<Json<ControlResponse>, ControlError> {
    authorize_control(&state.config)?;
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
    AxumPath((kind, id)): AxumPath<(String, String)>,
) -> Result<Json<ControlResponse>, ControlError> {
    job_signal_control(state, kind, id, true).await
}

async fn resume_job(
    State(state): State<Arc<AppState>>,
    AxumPath((kind, id)): AxumPath<(String, String)>,
) -> Result<Json<ControlResponse>, ControlError> {
    job_signal_control(state, kind, id, false).await
}

async fn retry_job(
    State(state): State<Arc<AppState>>,
    AxumPath((kind, id)): AxumPath<(String, String)>,
) -> Result<Json<ControlResponse>, ControlError> {
    authorize_control(&state.config)?;
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
    kind: String,
    id: String,
    pause: bool,
) -> Result<Json<ControlResponse>, ControlError> {
    authorize_control(&state.config)?;
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
            let tuner_targeted = runtime
                .legacy_throughput_tuner
                .pending_action
                .as_ref()
                .is_some_and(|action| match action {
                    LegacyTunerAction::Pause { epoch: target, .. }
                    | LegacyTunerAction::Resume { epoch: target, .. } => *target == epoch,
                })
                || active_tuner_probe_epoch(&runtime.legacy_throughput_tuner) == Some(epoch);
            if tuner_targeted {
                cancel_legacy_tuner_experiment(
                    &mut runtime.legacy_throughput_tuner,
                    unix_now(),
                    format!(
                        "manual pause took ownership of compact_reuse:{epoch}; cancelled automatic throughput experiment"
                    ),
                );
            }
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
            && args
                .iter()
                .any(|arg| *arg == LEGACY_CAR_DOWNLOAD_ARGV0.as_bytes());
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
    reap_adopted_legacy_compacts(&state.config, &mut runtime);
    reconcile_acquisition_state(&state.config, &mut runtime);
    runtime.inventory_generation = runtime.inventory_generation.saturating_add(1);
    let inventory_generation = runtime.inventory_generation;

    let mut snapshot = reconcile_filesystem(&state.config, &runtime, inventory_generation);
    if legacy_tuning_enabled(&state.config) {
        let context = legacy_tuner_context(&runtime, &snapshot);
        if runtime.legacy_throughput_tuner.context != Some(context) {
            reset_legacy_tuner_context(&mut runtime, context, unix_now());
        }
    }
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
    snapshot.errors = runtime.errors.iter().cloned().collect();
    let status_bytes = {
        let mut publication = state.publication.lock().await;
        let mut published = state.snapshot.write().await;
        preserve_newer_published_progress(&mut snapshot, &published);
        sample_worker_disk_io(&mut snapshot, &mut runtime);
        let queue_eta = estimate_runnable_queue_eta(&snapshot.epochs, &snapshot.lanes);
        apply_runnable_queue_eta(&mut snapshot.summary, queue_eta);
        sample_archive_device_disk_io(
            &state.config.archive_root,
            &mut snapshot.machine,
            &mut runtime,
        );
        let now = snapshot.now_unix_secs.max(unix_now());
        smooth_live_slot_rates(
            &mut snapshot.live,
            &mut publication.live_slot_rate_windows,
            now,
        );
        let tuner_profiles_before = runtime.legacy_tuner_profiles.clone();
        observe_legacy_throughput_tuner(&state.config, &snapshot, &mut runtime, now);
        remember_legacy_tuner_profile(&mut runtime);
        if runtime.legacy_tuner_profiles != tuner_profiles_before
            && let Err(error) = persist_control_state(&state.config, &runtime)
        {
            record_error(
                &state.config,
                &mut runtime,
                "legacy_throughput_tuner",
                format!("persist proven throughput topology: {error:#}"),
            );
        }
        let tuning_target = legacy_tuner_capacity_limit(&state.config, &snapshot, &runtime, now);
        publish_legacy_tuner_summary(
            &state.config,
            &mut snapshot.summary,
            &runtime.legacy_throughput_tuner,
            tuning_target,
            now,
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
    let mut publication = state.publication.lock().await;
    let mut snapshot = state.snapshot.write().await;
    let Some(epochs_changed) =
        apply_active_progress_updates(&mut snapshot, &lane_updates, &live_updates, now)
    else {
        return false;
    };
    let sequence = state.sequence.fetch_add(1, Ordering::Relaxed) + 1;
    snapshot.sequence = sequence;
    snapshot.now_unix_secs = snapshot.now_unix_secs.max(now);
    smooth_live_slot_rates(
        &mut snapshot.live,
        &mut publication.live_slot_rate_windows,
        now,
    );
    let patch = SnapshotPatch::active_progress(&snapshot, epochs_changed);
    let _ = state.updates.send(RealtimeEnvelope {
        event_type: "snapshot_patch",
        sequence,
        data: patch,
    });
    true
}

fn collect_active_progress_targets(
    config: &SchedulerConfig,
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

fn lane_progress_paths(config: &SchedulerConfig, lane: &LaneSnapshot) -> Vec<PathBuf> {
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

fn live_progress_paths(config: &SchedulerConfig, capture: &LiveCaptureSnapshot) -> Vec<PathBuf> {
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
    progress.source_bytes_done = baseline.source_bytes_done;
    progress.source_bytes_total = baseline.source_bytes_total;
    progress.source_read_mib_per_sec = baseline.source_read_mib_per_sec;
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
        let fresh_explicit_slot_rate = explicit_slot_rate.filter(|_| {
            source_updated > 0 && now.saturating_sub(source_updated) <= PROGRESS_STALE_SECS
        });
        if let Some(rate) = fresh_explicit_slot_rate {
            // A journal append can be newer than the producer's periodic
            // progress file by a second or two. Keep the producer's stable
            // window rate while using the journal's newer slot for ETA.
            progress.slots_per_sec = Some(rate);
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
    live_progress_source_changed(&target.baseline, &progress, now).then(|| LiveProgressUpdate {
        id: target.id.clone(),
        baseline: target.baseline.clone(),
        baseline_identity: target.baseline_identity.clone(),
        progress,
    })
}

fn live_progress_source_changed(
    current: &ProgressSnapshot,
    candidate: &ProgressSnapshot,
    now: u64,
) -> bool {
    let same_source_cursor = candidate.updated_unix_secs == current.updated_unix_secs
        && candidate.last_slot == current.last_slot
        && candidate.blocks_done == current.blocks_done;
    let candidate_is_fresh = candidate
        .updated_unix_secs
        .is_some_and(|updated| now.saturating_sub(updated) <= PROGRESS_STALE_SECS);
    if !same_source_cursor || !candidate_is_fresh {
        return progress_source_changed(current, candidate);
    }

    // The publication layer replaces these two derived values with its rolling
    // window. Do not emit a no-op patch merely because the unchanged source
    // still contains its raw rate; retain all other process-metric changes.
    let mut normalized = candidate.clone();
    normalized.slots_per_sec = current.slots_per_sec;
    normalized.eta_secs = current.eta_secs;
    progress_source_changed(current, &normalized)
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
    candidate.source_bytes_done = current.source_bytes_done;
    candidate.source_bytes_total = current.source_bytes_total;
    candidate.source_read_mib_per_sec = current.source_read_mib_per_sec;
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
    config: &SchedulerConfig,
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
    let mut classified_live = discovery
        .live_paths
        .iter()
        .cloned()
        .map(|path| classify_live_capture(config, runtime, path, now))
        .collect::<Vec<_>>();
    apply_repair_supersession(&mut classified_live);
    let (live, current_epoch) = canonicalize_live_captures(classified_live);
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
    let legacy_resource = legacy_compact_resource_capacity(config);
    let legacy_active_rss = sampled_legacy_compact_rss(&lanes, runtime);
    let legacy_exclusive_hold_reason =
        legacy_compact_exclusive_hold_reason(runtime, &epochs, &live);
    let tuner_context = if legacy_exclusive_hold_reason.is_some() {
        LegacyTunerContext::FinalizerOverlap
    } else {
        LegacyTunerContext::Bulk
    };
    let tuner_limit = if legacy_tuning_enabled(config) {
        legacy_tuner_capacity_limit_for_state(
            config,
            tuner_context,
            legacy_active_rss.len(),
            &runtime.legacy_throughput_tuner,
            now,
        )
    } else {
        usize::MAX
    };
    let explicit_overlap_limit =
        if legacy_exclusive_hold_reason.is_some() && config.legacy_compact_finalizer_overlap > 0 {
            config.legacy_compact_finalizer_overlap
        } else {
            usize::MAX
        };
    let adaptive_limit = tuner_limit.min(explicit_overlap_limit);
    let reported_memory_policy = settled_tuner_admission(
        config,
        &lanes,
        runtime,
        &legacy_active_rss,
        tuner_context,
        adaptive_limit,
    )
    .map_or(LegacyCompactMemoryPolicy::Conservative, |settled| {
        LegacyCompactMemoryPolicy::SettledTuner {
            peak_rebound_bytes: settled.peak_rebound_bytes,
        }
    });
    let mut legacy_admission_config = config.clone();
    if adaptive_limit != usize::MAX {
        legacy_admission_config.legacy_compact_concurrency = adaptive_limit;
    }
    let overlap_cap_reached = legacy_exclusive_hold_reason.is_some()
        && config.legacy_compact_finalizer_overlap > 0
        && legacy_active_rss.len() >= config.legacy_compact_finalizer_overlap;
    let static_finalizer_exclusive = legacy_exclusive_hold_reason.is_some()
        && !legacy_tuning_enabled(config)
        && config.legacy_compact_finalizer_overlap == 0;
    let tuner_target_reached =
        legacy_tuning_enabled(config) && legacy_active_rss.len() >= tuner_limit;
    let reported_hold_reason = static_finalizer_exclusive
        .then(|| legacy_exclusive_hold_reason.clone().unwrap_or_default())
        .or_else(|| {
            overlap_cap_reached.then(|| {
                format!(
                    "{}; finalizer overlap cap {} reached",
                    legacy_exclusive_hold_reason.as_deref().unwrap_or_default(),
                    config.legacy_compact_finalizer_overlap,
                )
            })
        })
        .or_else(|| {
            tuner_target_reached.then(|| {
                format!(
                    "adaptive throughput target {tuner_limit} reached; observing before the next one-lane probe"
                )
            })
        });
    let (legacy_admitted, legacy_blocked_reason) = reported_legacy_compact_admission(
        &legacy_admission_config,
        &machine,
        &epochs,
        &legacy_active_rss,
        &runtime.failures,
        reported_hold_reason.as_deref(),
        reported_memory_policy,
    );
    let mut summary = summarize_epochs(&epochs);
    summary.blocks_per_sec = active_block_processing_rate(&lanes, now);
    summary.scan_capacity_configured = config.scan_concurrency;
    summary.scan_capacity_admitted = admission.scan_capacity;
    summary.admission_blocked_reason = admission.blocked_reason.clone();
    summary.legacy_compact_capacity_configured = config.legacy_compact_concurrency;
    summary.legacy_compact_capacity_unbounded = config.legacy_compact_concurrency == 0;
    summary.legacy_compact_capacity_effective = if legacy_tuning_enabled(config) {
        tuner_limit
    } else {
        legacy_resource.effective_slots
    };
    summary.legacy_compact_capacity_admitted = legacy_admitted;
    summary.legacy_compact_finalizer_overlap = config.legacy_compact_finalizer_overlap;
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
        finalizer_queue_admission_blocked_reason(config, &machine, &finalizer_queue)
    } else if !sweep_complete && !finalizer_queue.is_empty() {
        sweep_blocked_reason.clone()
    } else {
        finalizer_queue_admission_blocked_reason(config, &machine, &finalizer_queue)
    };
    let queue_eta = estimate_runnable_queue_eta(&epochs, &lanes);
    apply_runnable_queue_eta(&mut summary, queue_eta);
    PipelineSnapshot {
        schema_version: STATUS_SCHEMA_VERSION,
        sequence: 0,
        now_unix_secs: now,
        current_epoch,
        observer_mode: !config.execute,
        capabilities: {
            let enabled = config.execute && config.management_bind.is_some();
            CapabilitySnapshot {
                control_enabled: enabled,
                authenticated_controls_required: false,
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

fn discover_inventory(config: &SchedulerConfig) -> InventoryDiscovery {
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

fn epoch_in_scope(config: &SchedulerConfig, epoch: u64) -> bool {
    epoch_in_scope_from_bounds(epoch, config.start_epoch, config.end_epoch)
}

/// Return a stable, work-conserving scheduling key without changing the
/// canonical epoch inventory order used by progress merging and SSE clients.
/// The configured band comes first in newest-first order; everything outside
/// it retains the historical ascending order.
fn historical_schedule_priority_key(config: &SchedulerConfig, epoch: u64) -> (u8, u64) {
    match (config.priority_epoch_start, config.priority_epoch_end) {
        (Some(start), Some(end)) if (start..=end).contains(&epoch) => {
            (0, end.saturating_sub(epoch))
        }
        _ => (1, epoch),
    }
}

fn prioritized_epochs<'a>(
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
                BLOCK_TIME_GAP_FILE,
                BLOCK_TIME_GAP_LOCK_FILE,
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
                BLOCKHASH_INDEX_V3_TMP_FILE,
                BLOCK_TIME_GAP_FILE,
                BLOCK_TIME_GAP_LOCK_FILE,
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

fn legacy_compact_previous_car(config: &SchedulerConfig, epoch: u64) -> Option<PathBuf> {
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

fn predecessor_seed_sidecars_usable(config: &SchedulerConfig, epoch: u64) -> bool {
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

fn legacy_compact_dependency_ready(config: &SchedulerConfig, epoch: u64) -> bool {
    if epoch == 0 {
        return true;
    }
    previous_tail_valid(&config.archive_root.join(format!("epoch-{epoch}")))
        || predecessor_seed_sidecars_usable(config, epoch)
}

fn legacy_compact_reuse_status(config: &SchedulerConfig, epoch: u64) -> LegacyCompactReuseStatus {
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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

fn finalizer_memory_floor_bytes(config: &SchedulerConfig) -> u64 {
    config.finalizer_memory_mib.saturating_mul(1024 * 1024)
}

fn estimate_mphf_build_bytes(config: &SchedulerConfig, registry_bytes: u64) -> u64 {
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
    config: &SchedulerConfig,
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

fn legacy_compact_resource_capacity(config: &SchedulerConfig) -> LegacyCompactResourceCapacity {
    // CPU load and device bandwidth are measured dynamically. Dividing either
    // budget by a per-worker estimate recreates an arbitrary lane-count cap
    // and prevents I/O-bound lanes from using otherwise idle resources.
    let cpu_slots = usize::MAX;
    let io_slots = usize::MAX;
    let configured_slots = if config.legacy_compact_concurrency == 0 {
        usize::MAX
    } else {
        config.legacy_compact_concurrency
    };
    LegacyCompactResourceCapacity {
        cpu_slots,
        io_slots,
        effective_slots: configured_slots.min(cpu_slots),
    }
}

fn legacy_compact_resource_blocked_reason(
    config: &SchedulerConfig,
    capacity: LegacyCompactResourceCapacity,
) -> Option<String> {
    let configured_cap_applies = config.legacy_compact_concurrency > 0
        && capacity.effective_slots < config.legacy_compact_concurrency;
    configured_cap_applies.then(|| {
        format!(
            "legacy compact resource admission: effective={} configured_cap={}; CPU load ceiling={} and device ceiling={} MiB/s are measured globally and never converted to worker slots",
            capacity.effective_slots,
            config.legacy_compact_concurrency,
            config.legacy_compact_cpu_budget_cores,
            config.legacy_compact_io_budget_mib_per_sec,
        )
    })
}

fn legacy_compact_memory_reservation_bytes(output: &Path) -> u64 {
    // Compact/reuse is always spawned with --no-access. In that path
    // Blockzilla normally loads the serialized KeyIndex and plain blockhash
    // registry, while registry.bin is only statted for its key count and
    // registry_counts.bin is not resident. An invalid KeyIndex can fall back
    // to a read-only registry mmap, but that mapping is reclaimable file cache
    // rather than anonymous RSS. Charging all four sidecars as heap prevented
    // safe tuner probes on otherwise idle memory. Keep a 1 GiB floor plus a
    // deliberately broad process/buffer allowance, and reserve the two
    // sidecars that are retained in the normal resume path.
    let resident_sidecars = [REGISTRY_INDEX_FILE, BLOCKHASH_REGISTRY_FILE]
        .iter()
        .fold(0u64, |sum, name| {
            sum.saturating_add(file_len(&output.join(name)))
        });
    resident_sidecars
        .saturating_add(LEGACY_COMPACT_MEMORY_OVERHEAD_MIB.saturating_mul(1024 * 1024))
        .max(LEGACY_COMPACT_MIN_MEMORY_MIB.saturating_mul(1024 * 1024))
}

#[derive(Debug)]
struct LegacyCompactAdmission {
    disk_headroom: Option<u64>,
    memory_headroom: Option<u64>,
    memory_floor_bytes: u64,
    active_memory_label: &'static str,
}

#[derive(Debug, Clone, Copy)]
enum LegacyCompactMemoryPolicy {
    Conservative,
    SettledTuner { peak_rebound_bytes: u64 },
}

impl LegacyCompactAdmission {
    #[cfg_attr(not(test), allow(dead_code))]
    fn new(
        config: &SchedulerConfig,
        machine: &MachineSnapshot,
        epochs: &[EpochSnapshot],
        active_rss: &BTreeMap<u64, u64>,
    ) -> Self {
        Self::with_memory_policy(
            config,
            machine,
            epochs,
            active_rss,
            LegacyCompactMemoryPolicy::Conservative,
        )
    }

    fn with_memory_policy(
        config: &SchedulerConfig,
        machine: &MachineSnapshot,
        epochs: &[EpochSnapshot],
        active_rss: &BTreeMap<u64, u64>,
        memory_policy: LegacyCompactMemoryPolicy,
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
        let (memory_floor_mib, active_memory_growth, active_memory_label) = match memory_policy {
            LegacyCompactMemoryPolicy::Conservative => (
                config.memory_reserve_mib,
                active_rss.iter().fold(0u64, |sum, (epoch, rss)| {
                    let output = epochs
                        .iter()
                        .find(|candidate| candidate.epoch == *epoch)
                        .map(|candidate| candidate.output_path.clone())
                        .unwrap_or_else(|| config.archive_root.join(format!("epoch-{epoch}")));
                    // MemAvailable already excludes current RSS. Reserve only
                    // each lane's full possible future growth.
                    sum.saturating_add(
                        legacy_compact_memory_reservation_bytes(&output).saturating_sub(*rss),
                    )
                }),
                "full active/admitted growth",
            ),
            LegacyCompactMemoryPolicy::SettledTuner { peak_rebound_bytes } => (
                config
                    .memory_reserve_mib
                    .saturating_add(config.legacy_compact_memory_guard_mib),
                peak_rebound_bytes,
                "settled active peak rebound",
            ),
        };
        let disk_reserve = config.disk_reserve_gib.saturating_mul(1024 * 1024 * 1024);
        let memory_floor_bytes = memory_floor_mib.saturating_mul(1024 * 1024);
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
                    .saturating_sub(memory_floor_bytes)
                    .saturating_sub(active_memory_growth)
            }),
            memory_floor_bytes,
            active_memory_label,
        }
    }

    fn blocked_reason(
        &self,
        config: &SchedulerConfig,
        machine: &MachineSnapshot,
        epoch: &EpochSnapshot,
    ) -> Option<String> {
        let projected_disk = scan_remaining_disk_projection(epoch);
        let Some(disk_headroom) = self.disk_headroom else {
            return Some(
                "legacy compact/reuse next-lane capacity limit: disk admission telemetry is unavailable"
                    .to_string(),
            );
        };
        if disk_headroom < projected_disk {
            return Some(format!(
                "legacy compact/reuse next-lane capacity limit: remaining disk admission headroom {:.1} GiB after reserve and active/admitted growth is below next remaining output {:.1} GiB (raw available {:.1} GiB, reserve {} GiB)",
                disk_headroom as f64 / 1024f64.powi(3),
                projected_disk as f64 / 1024f64.powi(3),
                machine.disk_available_bytes as f64 / 1024f64.powi(3),
                config.disk_reserve_gib,
            ));
        }
        let estimate = legacy_compact_memory_reservation_bytes(&epoch.output_path);
        let Some(memory_headroom) = self.memory_headroom else {
            return Some(
                "legacy compact/reuse next-lane capacity limit: memory admission telemetry is unavailable"
                    .to_string(),
            );
        };
        if memory_headroom < estimate {
            return Some(format!(
                "legacy compact/reuse next-lane capacity limit: remaining memory admission headroom {:.1} MiB after the {:.1} MiB safety floor and {} is below next reservation {:.1} MiB (raw MemAvailable {:.1} MiB)",
                memory_headroom as f64 / 1024f64.powi(2),
                self.memory_floor_bytes as f64 / 1024f64.powi(2),
                self.active_memory_label,
                estimate as f64 / 1024f64.powi(2),
                machine.memory_available_bytes as f64 / 1024f64.powi(2),
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

#[cfg_attr(not(test), allow(dead_code))]
fn legacy_compact_capacity_admission(
    config: &SchedulerConfig,
    machine: &MachineSnapshot,
    epochs: &[EpochSnapshot],
    active_rss: &BTreeMap<u64, u64>,
    failures: &BTreeMap<String, String>,
) -> (usize, Option<String>) {
    legacy_compact_capacity_admission_with_policy(
        config,
        machine,
        epochs,
        active_rss,
        failures,
        LegacyCompactMemoryPolicy::Conservative,
    )
}

fn legacy_compact_capacity_admission_with_policy(
    config: &SchedulerConfig,
    machine: &MachineSnapshot,
    epochs: &[EpochSnapshot],
    active_rss: &BTreeMap<u64, u64>,
    failures: &BTreeMap<String, String>,
    memory_policy: LegacyCompactMemoryPolicy,
) -> (usize, Option<String>) {
    let resource = legacy_compact_resource_capacity(config);
    let mut admitted = active_rss.len().min(resource.effective_slots);
    let mut admission = LegacyCompactAdmission::with_memory_policy(
        config,
        machine,
        epochs,
        active_rss,
        memory_policy,
    );
    let mut first_blocked = None;
    for epoch in prioritized_epochs(config, epochs)
        .into_iter()
        .filter(|epoch| {
            legacy_compact_reuse_status(config, epoch.epoch) == LegacyCompactReuseStatus::Ready
        })
    {
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

fn reported_legacy_compact_admission(
    config: &SchedulerConfig,
    machine: &MachineSnapshot,
    epochs: &[EpochSnapshot],
    active_rss: &BTreeMap<u64, u64>,
    failures: &BTreeMap<String, String>,
    exclusive_hold_reason: Option<&str>,
    memory_policy: LegacyCompactMemoryPolicy,
) -> (usize, Option<String>) {
    if let Some(reason) = exclusive_hold_reason {
        // SIGSTOP and scheduler exclusivity do not release RSS or future
        // growth reservations. Report every sampled/adopted active lane as
        // admitted, but never advertise hypothetical refill capacity while
        // a finalizer prevents the scheduler from using it.
        return (active_rss.len(), Some(reason.to_string()));
    }
    legacy_compact_capacity_admission_with_policy(
        config,
        machine,
        epochs,
        active_rss,
        failures,
        memory_policy,
    )
}

fn finalizer_is_admissible(
    config: &SchedulerConfig,
    machine: &MachineSnapshot,
    task: &FinalizerQueueItem,
) -> bool {
    task.deferred_reason.is_none()
        && finalizer_admission_blocked_reason(config, machine, task).is_none()
}

fn first_admissible_finalizer<'a>(
    config: &SchedulerConfig,
    machine: &MachineSnapshot,
    queue: &'a [FinalizerQueueItem],
) -> Option<&'a FinalizerQueueItem> {
    queue
        .iter()
        .find(|task| finalizer_is_admissible(config, machine, task))
}

fn finalizer_queue_admission_blocked_reason(
    config: &SchedulerConfig,
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

#[derive(Debug, Default, PartialEq)]
struct RunnableQueueEta {
    eta_secs: Option<f64>,
    reason: Option<String>,
    jobs_remaining: usize,
    capacity: usize,
    job_duration_secs: Option<f64>,
    duration_samples: usize,
    bytes_remaining: u64,
    read_mib_per_sec: Option<f64>,
    active_readers: usize,
    sampled_readers: usize,
}

fn estimate_runnable_queue_eta(
    epochs: &[EpochSnapshot],
    lanes: &[LaneSnapshot],
) -> RunnableQueueEta {
    let active = lanes
        .iter()
        .filter(|lane| {
            matches!(
                lane.kind.as_str(),
                "historical_scan" | "historical_compact_reuse"
            ) && !matches!(lane.state.as_str(), "idle" | "done" | "complete")
        })
        .collect::<Vec<_>>();
    let active_epochs = active
        .iter()
        .filter_map(|lane| lane.epoch)
        .collect::<BTreeSet<_>>();
    let runnable_queued = epochs
        .iter()
        .filter(|epoch| {
            epoch.state == HistoricalState::Queued
                && !active_epochs.contains(&epoch.epoch)
                && epoch.input_path.is_some()
                && epoch.car_bytes > 0
        })
        .collect::<Vec<_>>();
    let unknown_source = epochs
        .iter()
        .filter(|epoch| {
            epoch.state == HistoricalState::Queued
                && !active_epochs.contains(&epoch.epoch)
                && (epoch.input_path.is_none() || epoch.car_bytes == 0)
        })
        .count();
    let action_required = epochs
        .iter()
        .filter(|epoch| {
            matches!(
                epoch.state,
                HistoricalState::Blocked | HistoricalState::Failed
            )
        })
        .count();
    let jobs_remaining = runnable_queued.len().saturating_add(active.len());
    let active_readers = active
        .iter()
        .filter(|lane| {
            lane.state == "running"
                && lane
                    .progress
                    .source_bytes_done
                    .zip(lane.progress.source_bytes_total)
                    .is_none_or(|(done, total)| done < total)
        })
        .count();
    let sampled_readers = active
        .iter()
        .filter(|lane| {
            lane.state == "running"
                && lane
                    .progress
                    .source_bytes_done
                    .zip(lane.progress.source_bytes_total)
                    .is_some_and(|(done, total)| done < total)
                && lane.progress.source_read_mib_per_sec.is_some()
        })
        .count();
    let capacity = active.iter().filter(|lane| lane.state == "running").count();
    let queued_bytes = runnable_queued
        .iter()
        .fold(0u64, |sum, epoch| sum.saturating_add(epoch.car_bytes));
    let mut active_bytes = 0u64;
    let mut active_byte_coverage = 0usize;
    for lane in &active {
        if let Some((done, total)) = lane
            .progress
            .source_bytes_done
            .zip(lane.progress.source_bytes_total)
        {
            active_bytes = active_bytes.saturating_add(total.saturating_sub(done));
            active_byte_coverage = active_byte_coverage.saturating_add(1);
        }
    }
    let bytes_remaining = queued_bytes.saturating_add(active_bytes);
    let read_mib_per_sec = (sampled_readers == active_readers && active_readers > 0)
        .then(|| {
            active
                .iter()
                .filter(|lane| {
                    lane.state == "running"
                        && lane
                            .progress
                            .source_bytes_done
                            .zip(lane.progress.source_bytes_total)
                            .is_some_and(|(done, total)| done < total)
                })
                .filter_map(|lane| lane.progress.source_read_mib_per_sec)
                .sum::<f64>()
        })
        .filter(|rate| rate.is_finite() && *rate > 0.0);
    let base = RunnableQueueEta {
        jobs_remaining,
        capacity,
        bytes_remaining,
        read_mib_per_sec,
        active_readers,
        sampled_readers,
        ..RunnableQueueEta::default()
    };

    if jobs_remaining == 0 {
        return RunnableQueueEta {
            eta_secs: Some(0.0),
            reason: Some(format!(
                "runnable historical source-byte queue is empty; {action_required} action-required and {unknown_source} unknown-source item(s) excluded"
            )),
            ..base
        };
    }
    if active_byte_coverage != active.len() {
        return RunnableQueueEta {
            reason: Some(format!(
                "source-byte ETA is learning active CAR offsets ({active_byte_coverage}/{} active job(s) covered); worker count is not used",
                active.len(),
            )),
            ..base
        };
    }
    if bytes_remaining == 0 {
        return RunnableQueueEta {
            eta_secs: Some(0.0),
            reason: Some(format!(
                "all known runnable CAR source bytes have been read; {action_required} action-required and {unknown_source} unknown-source item(s) excluded"
            )),
            ..base
        };
    }
    let Some(read_mib_per_sec) = read_mib_per_sec else {
        return RunnableQueueEta {
            reason: Some(format!(
                "source-byte ETA is learning aggregate CAR read speed ({sampled_readers}/{active_readers} advancing reader(s) sampled); {:.1} GiB remain; worker count is not used",
                bytes_remaining as f64 / 1024f64.powi(3),
            )),
            ..base
        };
    };
    let eta_secs = bytes_remaining as f64 / (read_mib_per_sec * 1024.0 * 1024.0);
    RunnableQueueEta {
        eta_secs: Some(eta_secs),
        reason: Some(format!(
            "{:.1} GiB of runnable CAR source bytes remain at {:.1} MiB/s aggregate source-read speed ({sampled_readers}/{active_readers} advancing reader(s)); ETA is bytes/rate only; {action_required} action-required and {unknown_source} unknown-source item(s) excluded",
            bytes_remaining as f64 / 1024f64.powi(3),
            read_mib_per_sec,
        )),
        ..base
    }
}

fn apply_runnable_queue_eta(summary: &mut PipelineSummary, queue_eta: RunnableQueueEta) {
    summary.eta_secs = queue_eta.eta_secs;
    summary.queue_eta_secs = queue_eta.eta_secs;
    summary.queue_eta_reason = queue_eta.reason;
    summary.queue_jobs_remaining = queue_eta.jobs_remaining;
    summary.queue_capacity = queue_eta.capacity;
    summary.queue_job_duration_secs = queue_eta.job_duration_secs;
    summary.queue_duration_samples = queue_eta.duration_samples;
    summary.queue_bytes_remaining = queue_eta.bytes_remaining;
    summary.queue_read_mib_per_sec = queue_eta.read_mib_per_sec;
    summary.queue_read_active_workers = queue_eta.active_readers;
    summary.queue_read_sampled_workers = queue_eta.sampled_readers;
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
    capture_dir: &Path,
    output: &Path,
    bundle: &PublishedRepairBundle,
) -> Result<RepairCompactedFingerprint> {
    let mut files = Vec::with_capacity(19);
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
        capture_dir.join(LIVE_REPAIR_REQUIRED_MARKER),
        capture_dir.join(LIVE_REPAIR_PLAN_FILE),
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

#[allow(dead_code)]
fn read_published_repair_compacted_legacy(
    capture_dir: &Path,
    output_dir: &Path,
    bundle: &PublishedRepairBundle,
) -> Result<Option<PublishedRepairCompacted>> {
    let marker_path = output_dir.join(LIVE_REPAIR_COMPACTED_MARKER);
    let marker_metadata = match fs::symlink_metadata(&marker_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("stat {}", marker_path.display()));
        }
    };
    anyhow::ensure!(
        marker_metadata.file_type().is_file(),
        "{} is not a regular degraded-compaction marker",
        marker_path.display()
    );
    let output_metadata = fs::symlink_metadata(output_dir)
        .with_context(|| format!("stat degraded-compaction output {}", output_dir.display()))?;
    anyhow::ensure!(
        output_metadata.file_type().is_dir(),
        "degraded-compaction output is not a regular non-symlink directory: {}",
        output_dir.display()
    );
    anyhow::ensure!(
        marker_metadata.len() > 0
            && marker_metadata.len() <= MAX_LIVE_REPAIR_COMPACTED_MARKER_BYTES,
        "{} has {} bytes; expected 1..={MAX_LIVE_REPAIR_COMPACTED_MARKER_BYTES}",
        marker_path.display(),
        marker_metadata.len()
    );
    let marker_file =
        File::open(&marker_path).with_context(|| format!("open {}", marker_path.display()))?;
    let marker: PublishedRepairCompactedMarker = serde_json::from_reader(BufReader::with_capacity(
        64 * 1024,
        marker_file.take(MAX_LIVE_REPAIR_COMPACTED_MARKER_BYTES + 1),
    ))
    .with_context(|| format!("parse {}", marker_path.display()))?;

    anyhow::ensure!(
        marker.version == 1
            && marker.state == "degraded_hot_archive_missing_poh_and_shredding"
            && !marker.canonical
            && !marker.publication_ready
            && marker.block_archive_ready
            && !marker.block_access_ready,
        "degraded-compaction marker has incompatible state or readiness flags"
    );
    anyhow::ensure!(
        marker.epoch == bundle.epoch
            && marker.epoch_start_slot == bundle.epoch_start_slot
            && marker.epoch_end_slot == bundle.epoch_end_slot,
        "degraded-compaction epoch bounds differ from the published repair bundle"
    );
    anyhow::ensure!(
        marker.live_blocks == bundle.live_blocks
            && marker.rpc_only_blocks == bundle.rpc_only_blocks
            && marker.produced_blocks == bundle.produced_blocks
            && marker.live_blocks.checked_add(marker.rpc_only_blocks)
                == Some(marker.produced_blocks)
            && marker.produced_blocks > 0
            && marker.produced_blocks <= SLOTS_PER_EPOCH,
        "degraded-compaction block counts differ from the published repair bundle"
    );
    anyhow::ensure!(
        marker.compressed_bytes > 0 && marker.uncompressed_bytes > 0,
        "degraded-compaction byte accounting is empty"
    );
    anyhow::ensure!(
        marker.signatures >= marker.transactions,
        "degraded-compaction signature count is below transaction count"
    );
    let _zstd_level = marker.zstd_level;

    let expected_files = [
        (&marker.files.blocks, BLOCKS_FILE),
        (&marker.files.index, BLOCK_INDEX_FILE),
        (&marker.files.meta, META_FILE),
        (&marker.files.registry, REGISTRY_FILE),
        (&marker.files.registry_counts, REGISTRY_COUNTS_FILE),
        (&marker.files.registry_index, REGISTRY_INDEX_FILE),
        (&marker.files.blockhashes, BLOCKHASH_REGISTRY_FILE),
        (&marker.files.signatures, SIGNATURES_FILE),
        (&marker.files.vote_hashes, VOTE_HASH_REGISTRY_FILE),
        (&marker.files.available_poh, "repair/available-poh.wincode"),
    ];
    for (actual, expected) in expected_files {
        anyhow::ensure!(
            actual == expected,
            "degraded-compaction file path for {expected} is not fixed"
        );
        let path = output_dir.join(expected);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("stat degraded-compaction file {}", path.display()))?;
        anyhow::ensure!(
            metadata.file_type().is_file() && metadata.len() > 0,
            "degraded-compaction file {} must be regular, non-symlink, and nonempty",
            path.display()
        );
    }
    anyhow::ensure!(
        file_len(&output_dir.join(BLOCKS_FILE)) == marker.compressed_bytes,
        "degraded-compaction compressed byte count differs from the blocks file"
    );
    anyhow::ensure!(
        file_len(&output_dir.join(BLOCKHASH_REGISTRY_FILE))
            == marker.produced_blocks.saturating_mul(32),
        "degraded-compaction blockhash registry length is inconsistent"
    );
    anyhow::ensure!(
        file_len(&output_dir.join(SIGNATURES_FILE)) == marker.signatures.saturating_mul(64),
        "degraded-compaction signature sidecar length is inconsistent"
    );
    anyhow::ensure!(
        hot_block_index_matches_blob_and_blockhashes(output_dir),
        "degraded-compaction hot block index does not match blocks and blockhashes"
    );
    let index_header =
        read_fixed_header::<HOT_BLOCK_INDEX_HEADER_LEN>(&output_dir.join(BLOCK_INDEX_FILE))
            .context("read degraded-compaction hot block index header")?;
    anyhow::ensure!(
        i32::from_le_bytes(index_header[28..32].try_into().unwrap()) == marker.zstd_level,
        "degraded-compaction zstd level differs from the hot block index"
    );
    anyhow::ensure!(
        registry_index_matches_registry(output_dir),
        "degraded-compaction registry index does not match registry.bin"
    );

    anyhow::ensure!(
        marker
            .poh_coverage
            .available_records
            .checked_add(marker.poh_coverage.missing_records)
            == Some(marker.produced_blocks)
            && marker.poh_coverage.available_records == bundle.live_blocks
            && marker.poh_coverage.missing_records == bundle.rpc_only_blocks
            && marker.poh_coverage.produced_id_space == marker.produced_blocks
            && marker.poh_coverage.record_ids_have_explicit_gaps
            && marker.poh_coverage.missing_record_ids.len() as u64
                == marker.poh_coverage.missing_records,
        "degraded-compaction PoH coverage accounting is inconsistent"
    );
    let _available_poh_entries = marker.poh_coverage.available_entries;
    anyhow::ensure!(
        marker
            .poh_coverage
            .missing_record_ids
            .iter()
            .all(|id| u64::from(*id) < marker.produced_blocks)
            && marker
                .poh_coverage
                .missing_record_ids
                .windows(2)
                .all(|ids| ids[0] < ids[1]),
        "degraded-compaction missing PoH ids are not sorted, unique, and in range"
    );
    anyhow::ensure!(
        marker.shredding_coverage.available_records == 0
            && marker.shredding_coverage.missing_records == marker.produced_blocks
            && !marker.shredding_coverage.canonical_sidecar_emitted,
        "degraded-compaction shredding coverage is inconsistent"
    );

    for forbidden in [
        POH_FILE,
        SHREDDING_FILE,
        "READY",
        BLOCK_ACCESS_FILE,
        BLOCK_ACCESS_INDEX_FILE,
    ] {
        let path = output_dir.join(forbidden);
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => anyhow::bail!(
                "degraded-compaction output contains forbidden canonical artifact {forbidden}"
            ),
            Err(error) => {
                return Err(error).with_context(|| format!("stat forbidden {}", path.display()));
            }
        }
    }

    validate_repair_compacted_sources(capture_dir, output_dir, bundle, &marker)?;
    anyhow::ensure!(
        !marker.limitations.is_empty()
            && marker
                .limitations
                .iter()
                .all(|limitation| !limitation.trim().is_empty()),
        "degraded-compaction limitations must be explicit"
    );

    Ok(Some(PublishedRepairCompacted {
        updated_unix_secs: modified_unix_secs(&marker_path).unwrap_or_default(),
        block_access_ready: false,
    }))
}

fn read_published_repair_compacted(
    capture_dir: &Path,
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
            && marker_metadata.len() <= MAX_LIVE_REPAIR_COMPACTED_MARKER_BYTES,
        "{} must be a regular file with 1..={MAX_LIVE_REPAIR_COMPACTED_MARKER_BYTES} bytes",
        marker_path.display()
    );
    let fingerprint = repair_compacted_fingerprint(capture_dir, output, bundle)?;
    if let Some(receipt) = cached_repair_compacted(output, &fingerprint) {
        return Ok(Some(receipt));
    }
    let marker: PublishedRepairCompactedMarker =
        read_repair_json_bounded(&marker_path, MAX_LIVE_REPAIR_COMPACTED_MARKER_BYTES)?;

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
        marker.signatures >= marker.transactions,
        "repair compacted signature count is below transaction count"
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
        let path = output.join(forbidden);
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => anyhow::bail!(
                "degraded repair output contains forbidden canonical artifact {forbidden}"
            ),
            Err(error) => return Err(error).with_context(|| format!("stat {}", path.display())),
        }
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

    validate_repair_compacted_sources(capture_dir, output, bundle, &marker)?;
    validate_repair_hot_index(output, &marker)?;
    validate_repair_hot_meta(output, &marker)?;
    validate_repair_available_poh(output, &marker)?;
    anyhow::ensure!(
        registry_index_matches_registry(output),
        "repair compacted registry index does not match registry.bin"
    );
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

fn validate_repair_compacted_sources(
    capture_dir: &Path,
    output_dir: &Path,
    bundle: &PublishedRepairBundle,
    marker: &PublishedRepairCompactedMarker,
) -> Result<()> {
    for (field, digest) in [
        (
            "source_materialized_marker_sha256",
            marker.source_materialized_marker_sha256.as_str(),
        ),
        (
            "source_manifest_sha256",
            marker.source_manifest_sha256.as_str(),
        ),
        (
            "source_merge_plan_sha256",
            marker.source_merge_plan_sha256.as_str(),
        ),
    ] {
        anyhow::ensure!(
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "degraded-compaction {field} is not a lowercase SHA-256 digest"
        );
    }

    let source_marker_path = output_dir.join(LIVE_REPAIR_SOURCE_MATERIALIZED_MARKER);
    let source_marker_metadata = fs::symlink_metadata(&source_marker_path)
        .with_context(|| format!("stat {}", source_marker_path.display()))?;
    anyhow::ensure!(
        source_marker_metadata.file_type().is_file()
            && source_marker_metadata.len() > 0
            && source_marker_metadata.len() <= MAX_LIVE_REPAIR_COMPACTED_MARKER_BYTES,
        "copied source materialized marker must be a bounded regular non-symlink file"
    );
    anyhow::ensure!(
        sha256_file_hex(&source_marker_path)? == marker.source_materialized_marker_sha256,
        "copied source materialized marker digest differs from degraded-compaction marker"
    );
    let source_marker_file = File::open(&source_marker_path)?;
    let source_marker: PublishedRepairSourceMaterializedMarker =
        serde_json::from_reader(BufReader::with_capacity(
            64 * 1024,
            source_marker_file.take(MAX_LIVE_REPAIR_COMPACTED_MARKER_BYTES + 1),
        ))
        .with_context(|| format!("parse {}", source_marker_path.display()))?;
    anyhow::ensure!(
        source_marker.version == 1
            && source_marker.state == "repair_materialized_missing_poh_and_shredding"
            && !source_marker.canonical
            && !source_marker.publication_ready
            && source_marker.epoch == bundle.epoch
            && source_marker.epoch_start_slot == bundle.epoch_start_slot
            && source_marker.epoch_end_slot == bundle.epoch_end_slot
            && source_marker.live_blocks == bundle.live_blocks
            && source_marker.rpc_only_blocks == bundle.rpc_only_blocks
            && source_marker.produced_blocks == bundle.produced_blocks
            && source_marker.transactions == marker.transactions,
        "copied source materialized marker differs from the repair bundle"
    );
    anyhow::ensure!(
        source_marker.manifest_sha256 == marker.source_manifest_sha256
            && source_marker.merge_plan_sha256 == marker.source_merge_plan_sha256,
        "degraded-compaction source digests differ from copied materialized marker"
    );
    anyhow::ensure!(
        cached_repair_sha256_file_hex(&capture_dir.join(LIVE_REPAIR_REQUIRED_MARKER))?
            == marker.source_manifest_sha256,
        "degraded-compaction source manifest digest differs from repair publication"
    );
    anyhow::ensure!(
        cached_repair_sha256_file_hex(&capture_dir.join(LIVE_REPAIR_PLAN_FILE))?
            == marker.source_merge_plan_sha256,
        "degraded-compaction merge-plan digest differs from repair publication"
    );
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepairDigestCacheKey {
    path: PathBuf,
    device: u64,
    inode: u64,
    bytes: u64,
    modified_nanos: u128,
}

static REPAIR_DIGEST_CACHE: OnceLock<std::sync::Mutex<VecDeque<(RepairDigestCacheKey, String)>>> =
    OnceLock::new();

fn repair_digest_cache_key(path: &Path) -> Result<RepairDigestCacheKey> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("stat repair digest source {}", path.display()))?;
    anyhow::ensure!(
        metadata.file_type().is_file() && metadata.len() > 0,
        "repair digest source must be a regular non-symlink file: {}",
        path.display()
    );
    let modified_nanos = metadata
        .modified()
        .context("read repair digest source mtime")?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    Ok(RepairDigestCacheKey {
        path: path.to_path_buf(),
        device: metadata.dev(),
        inode: metadata.ino(),
        bytes: metadata.len(),
        modified_nanos,
    })
}

fn cached_repair_sha256_file_hex(path: &Path) -> Result<String> {
    const MAX_CACHE_ENTRIES: usize = 32;
    let before = repair_digest_cache_key(path)?;
    let cache = REPAIR_DIGEST_CACHE
        .get_or_init(|| std::sync::Mutex::new(VecDeque::with_capacity(MAX_CACHE_ENTRIES)));
    {
        let cache = cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((_, digest)) = cache.iter().find(|(key, _)| key == &before) {
            return Ok(digest.clone());
        }
    }

    let digest = sha256_file_hex(path)?;
    let after = repair_digest_cache_key(path)?;
    anyhow::ensure!(
        before == after,
        "repair digest source changed while hashing: {}",
        path.display()
    );
    let mut cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.retain(|(key, _)| key.path != before.path);
    cache.push_back((before, digest.clone()));
    while cache.len() > MAX_CACHE_ENTRIES {
        cache.pop_front();
    }
    Ok(digest)
}

fn sha256_file_hex(path: &Path) -> Result<String> {
    let mut reader = BufReader::with_capacity(
        64 * 1024,
        File::open(path).with_context(|| format!("open {}", path.display()))?,
    );
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(hex)
}

#[derive(Debug, Clone, Copy)]
enum RepairProcessKind {
    Materialize,
    Compact,
}

fn repair_materialized_output(config: &SchedulerConfig, epoch: u64) -> PathBuf {
    config
        .state_root
        .join("live-repair-materialized")
        .join(format!("epoch-{epoch}"))
}

fn repair_materialization_progress_path(config: &SchedulerConfig, epoch: u64) -> PathBuf {
    config
        .state_root
        .join("live-repair-materialized")
        .join(format!(".epoch-{epoch}.repair-materialize-stage"))
        .join("repair/materialization-progress.json")
}

fn repair_hot_progress_path(config: &SchedulerConfig, epoch: u64) -> PathBuf {
    config
        .archive_root
        .join(format!(".epoch-{epoch}.repair-hot-stage"))
        .join("repair/hot-progress.json")
}

fn active_repair_progress(
    config: &SchedulerConfig,
    capture_dir: &Path,
    bundle: &PublishedRepairBundle,
    now: u64,
) -> Option<ProgressSnapshot> {
    let repair_bin = effective_repair_blockzilla_bin(config);
    let materialized_output = repair_materialized_output(config, bundle.epoch);
    let archive_output = config.archive_root.join(format!("epoch-{}", bundle.epoch));
    [
        (
            repair_materialization_progress_path(config, bundle.epoch),
            RepairProcessKind::Materialize,
            &["materializing"][..],
        ),
        (
            repair_hot_progress_path(config, bundle.epoch),
            RepairProcessKind::Compact,
            &["building_hot_archive", "Archive V2 Live Hot Write"][..],
        ),
    ]
    .into_iter()
    .filter_map(|(path, kind, allowed_phases)| {
        read_active_repair_progress(
            &path,
            bundle.epoch,
            bundle.produced_blocks,
            now,
            repair_bin,
            kind,
            capture_dir,
            &materialized_output,
            &archive_output,
            allowed_phases,
        )
    })
    .max_by_key(|progress| progress.updated_unix_secs.unwrap_or_default())
}

fn read_active_repair_progress(
    path: &Path,
    epoch: u64,
    produced_blocks: u64,
    now: u64,
    repair_bin: &Path,
    kind: RepairProcessKind,
    capture_dir: &Path,
    materialized_output: &Path,
    archive_output: &Path,
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
        Some(pid)
            if process_cmdline_matches_repair(
                pid,
                repair_bin,
                kind,
                capture_dir,
                materialized_output,
                archive_output,
            ) =>
        {
            pid
        }
        Some(_) => return None,
        None => find_repair_process(
            repair_bin,
            kind,
            capture_dir,
            materialized_output,
            archive_output,
        )?,
    };
    if progress.blocks_done > produced_blocks {
        return None;
    }
    progress.pid = Some(pid);
    progress.state = Some("running".to_string());
    progress.blocks_total = produced_blocks;
    progress.updated_unix_secs = Some(updated);
    progress.rss_bytes = process_tree_rss_bytes(pid).or(progress.rss_bytes);
    progress.peak_rss_bytes = process_peak_rss_bytes(pid)
        .or(progress.peak_rss_bytes)
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

fn find_repair_process(
    blockzilla_bin: &Path,
    kind: RepairProcessKind,
    capture_dir: &Path,
    materialized_output: &Path,
    archive_output: &Path,
) -> Option<u32> {
    fs::read_dir("/proc")
        .ok()?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let pid = entry.file_name().to_str()?.parse::<u32>().ok()?;
            process_cmdline_matches_repair(
                pid,
                blockzilla_bin,
                kind,
                capture_dir,
                materialized_output,
                archive_output,
            )
            .then_some(pid)
        })
        .min()
}

fn process_cmdline_matches_repair(
    pid: u32,
    blockzilla_bin: &Path,
    kind: RepairProcessKind,
    capture_dir: &Path,
    materialized_output: &Path,
    archive_output: &Path,
) -> bool {
    fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline")).is_ok_and(|bytes| {
        repair_argv_matches_bytes(
            &bytes,
            blockzilla_bin,
            kind,
            capture_dir,
            materialized_output,
            archive_output,
        )
    })
}

fn repair_argv_matches_bytes(
    bytes: &[u8],
    blockzilla_bin: &Path,
    kind: RepairProcessKind,
    capture_dir: &Path,
    materialized_output: &Path,
    archive_output: &Path,
) -> bool {
    if bytes.last() != Some(&0) {
        return false;
    }
    let args = bytes[..bytes.len().saturating_sub(1)]
        .split(|byte| *byte == 0)
        .collect::<Vec<_>>();
    if args.len() != 4 || args.iter().any(|arg| arg.is_empty()) {
        return false;
    }
    let expected = match kind {
        RepairProcessKind::Materialize => (
            b"materialize-archive-v2-live-repair".as_slice(),
            capture_dir,
            materialized_output,
        ),
        RepairProcessKind::Compact => (
            b"build-archive-v2-degraded-hot-blocks-from-repair".as_slice(),
            materialized_output,
            archive_output,
        ),
    };
    args.first().copied() == Some(blockzilla_bin.as_os_str().as_bytes())
        && args.get(1).copied() == Some(expected.0)
        && args.get(2).copied() == Some(expected.1.as_os_str().as_bytes())
        && args.get(3).copied() == Some(expected.2.as_os_str().as_bytes())
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
    let recognized_executable = executable == Some(HIVEZILLA_EXECUTABLE)
        || executable == Some(LEGACY_LIVE_PRODUCER_EXECUTABLE);
    if !recognized_executable || args.get(1).copied() != Some(b"capture-grpc") {
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
            matches!(
                capture.state,
                LiveState::RepairRequired | LiveState::Packaging | LiveState::Packaged
            ) && !capture.source_capture_ids.is_empty()
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
        capture.eta_secs = None;
        capture.slots_per_sec = None;
        capture.rss_bytes = None;
        capture.peak_rss_bytes = None;
        capture.message = Some(format!(
            "concurrent live capture is not canonical; current capture is {current_id}"
        ));
    }
    (captures, current_epoch)
}

fn classify_live_capture(
    config: &SchedulerConfig,
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
            let bundle_updated_unix_secs = if bundle.updated_unix_secs == 0 {
                now
            } else {
                bundle.updated_unix_secs
            };
            let output_path = config.archive_root.join(format!("epoch-{}", bundle.epoch));
            let active_progress = active_repair_progress(config, &capture_dir, &bundle, now);
            let (state, progress_state, message, compacted_updated_unix_secs, operation_progress) =
                match read_published_repair_compacted(&capture_dir, &output_path, &bundle) {
                    Ok(Some(compacted)) => {
                        let message = if compacted.block_access_ready {
                            format!(
                                "repaired hot block archive with block access covers slots {}..{}; canonical PoH and shredding sidecars remain intentionally incomplete",
                                bundle.epoch_start_slot, bundle.epoch_end_slot
                            )
                        } else {
                            format!(
                                "degraded hot block archive covers slots {}..{}; canonical PoH, shredding, and block-access sidecars remain intentionally incomplete",
                                bundle.epoch_start_slot, bundle.epoch_end_slot
                            )
                        };
                        (
                            LiveState::Packaged,
                            "packaged_degraded",
                            message,
                            compacted.updated_unix_secs,
                            None,
                        )
                    }
                    Ok(None) => match active_progress {
                        Some(progress) => (
                            LiveState::Packaging,
                            "packaging",
                            "repair materialization/degraded hot compaction is active; canonical PoH, shredding, and block-access sidecars remain incomplete".to_string(),
                            progress.updated_unix_secs.unwrap_or_default(),
                            Some(progress),
                        ),
                        None => (
                            LiveState::RepairRequired,
                            "repair_required",
                            format!(
                                "atomic repair bundle covers slots {}..{}; repair-aware compact materialization is required and canonical PoH/shredding remain incomplete",
                                bundle.epoch_start_slot, bundle.epoch_end_slot
                            ),
                            0,
                            None,
                        ),
                    },
                    Err(error) => (
                        LiveState::RepairRequired,
                        "repair_required",
                        format!(
                            "invalid {LIVE_REPAIR_COMPACTED_MARKER}; degraded archive is not accepted and repair-aware compaction must be retried: {error:#}"
                        ),
                        modified_unix_secs(&output_path.join(LIVE_REPAIR_COMPACTED_MARKER))
                            .unwrap_or_default(),
                        None,
                    ),
                };
            let updated_unix_secs = bundle_updated_unix_secs.max(compacted_updated_unix_secs);
            let mut progress = operation_progress.unwrap_or_default();
            progress.phase = progress.phase.or_else(|| Some("repair_bundle".to_string()));
            progress.state = Some(progress_state.to_string());
            progress.blocks_total = bundle.produced_blocks;
            if state == LiveState::Packaging {
                progress.progress_pct = Some(
                    progress.blocks_done.min(bundle.produced_blocks) as f64
                        / bundle.produced_blocks as f64
                        * 100.0,
                );
            } else {
                progress.blocks_done = bundle.produced_blocks;
                progress.progress_pct = Some(100.0);
            }
            progress.first_slot = progress.first_slot.or(Some(bundle.first_produced_slot));
            progress.last_slot = progress.last_slot.or(Some(bundle.last_produced_slot));
            progress.updated_unix_secs = Some(updated_unix_secs);
            hide_stale_live_rates(&mut progress, now);
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
                blocks_written: bundle.produced_blocks,
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
    // epoch-1000 capture with a 1001 tail is mislabeled as epoch 1001.
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
    config: &SchedulerConfig,
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
    let auto_pause = runtime.auto_paused_legacy.get(&compact.epoch);
    let paused = runtime.paused_jobs.contains(&key) || auto_pause.is_some();
    progress.pid = Some(compact.pid);
    progress.rss_bytes = rss_bytes;
    progress.state = Some(if paused {
        "paused".to_string()
    } else {
        "running".to_string()
    });
    let phase = progress
        .phase
        .clone()
        .unwrap_or_else(|| "compact_reuse".to_string());
    LaneSnapshot {
        id: key.clone(),
        kind: "historical_compact_reuse".to_string(),
        epoch: Some(compact.epoch),
        capture_id: None,
        phase,
        state: if paused {
            "paused".to_string()
        } else {
            "running".to_string()
        },
        auto_paused: auto_pause.is_some(),
        auto_pause_reason: auto_pause.map(|record| record.reason.clone()),
        pid: Some(compact.pid),
        progress,
        rss_bytes,
        // Adopted workers predate this controller, so they do not have a
        // ManagedChild spawn timestamp. Recover the stable wall-clock start
        // from their already-trusted Linux /proc start-tick identity instead.
        started_unix_secs: process_started_unix_secs(compact.pid, compact.process_start_ticks),
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

fn legacy_compact_exclusive_hold_reason(
    runtime: &RuntimeState,
    epochs: &[EpochSnapshot],
    live: &[LiveCaptureSnapshot],
) -> Option<String> {
    if let Some(finalizer) = runtime.finalizer.as_ref() {
        let key = finalizer.kind.key();
        let state = if runtime.paused_jobs.contains(&key) {
            "paused"
        } else {
            "active"
        };
        return Some(format!(
            "legacy compact refill held while finalizer {key} is {state}; existing legacy lanes remain admitted and reserved"
        ));
    }
    if let Some(epoch) = epochs
        .iter()
        .find(|epoch| epoch.state == HistoricalState::Finalizing)
    {
        let phase = epoch.progress.phase.as_deref().unwrap_or("finalizing");
        return Some(format!(
            "legacy compact refill held while historical finalizer epoch {} (phase {phase}) is active; existing legacy lanes remain admitted and reserved",
            epoch.epoch
        ));
    }
    live.iter()
        .find(|capture| capture.state == LiveState::Packaging)
        .map(|capture| {
            let phase = capture.progress.phase.as_deref().unwrap_or("packaging");
            format!(
                "legacy compact refill held while live finalizer {} (phase {phase}) is active; existing legacy lanes remain admitted and reserved",
                capture.id
            )
        })
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
    Pause { epoch: u64, reason: String },
    Resume { epoch: u64, reason: String },
}

#[derive(Debug)]
struct LegacyThroughputObservation {
    identities: BTreeSet<(u64, u32, LegacyLaneStartIdentity)>,
    active_lanes: usize,
    sampled_lanes: usize,
    useful_mib_per_sec: Option<f64>,
    rate_source: Option<LegacyTunerRateSource>,
    device_mib_per_sec: Option<f64>,
}

fn legacy_lane_start_identity(
    started_unix_secs: Option<u64>,
    linux_start_ticks: Option<u64>,
) -> Option<LegacyLaneStartIdentity> {
    #[cfg(target_os = "linux")]
    {
        // Linux controls require the kernel's monotonic process start tick.
        // A wall-clock second is display metadata, not PID-reuse proof.
        let _ = started_unix_secs;
        linux_start_ticks.map(LegacyLaneStartIdentity::LinuxStartTicks)
    }
    #[cfg(not(target_os = "linux"))]
    {
        linux_start_ticks
            .map(LegacyLaneStartIdentity::LinuxStartTicks)
            .or_else(|| started_unix_secs.map(LegacyLaneStartIdentity::UnixSecs))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LegacyTunerRateSource {
    LogicalInput,
    ProcessIo,
}

impl LegacyTunerRateSource {
    fn api_name(self) -> &'static str {
        match self {
            Self::LogicalInput => "logical_input",
            Self::ProcessIo => "process_io",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::LogicalInput => "logical input",
            Self::ProcessIo => "process I/O fallback",
        }
    }
}

impl LegacyTunerProfiles {
    fn get(&self, context: LegacyTunerContext) -> &LegacyTunerProfile {
        match context {
            LegacyTunerContext::Bulk => &self.bulk,
            LegacyTunerContext::FinalizerOverlap => &self.finalizer_overlap,
        }
    }

    fn get_mut(&mut self, context: LegacyTunerContext) -> &mut LegacyTunerProfile {
        match context {
            LegacyTunerContext::Bulk => &mut self.bulk,
            LegacyTunerContext::FinalizerOverlap => &mut self.finalizer_overlap,
        }
    }
}

fn remember_legacy_tuner_profile(runtime: &mut RuntimeState) {
    let tuner = &runtime.legacy_throughput_tuner;
    let Some(context) = tuner.context else {
        return;
    };
    // Zero means "not measured in this controller", never evidence that a
    // previously proven topology became worse. Only paired pause verification
    // is allowed to lower a non-zero accepted topology.
    if tuner.accepted_lanes == 0 {
        return;
    }
    *runtime.legacy_tuner_profiles.get_mut(context) = LegacyTunerProfile {
        accepted_lanes: tuner.accepted_lanes,
        accepted_useful_mib_per_sec: tuner.accepted_useful_mib_per_sec,
        accepted_rate_source: tuner.accepted_rate_source,
        accepted_device_mib_per_sec: tuner.accepted_device_mib_per_sec,
    };
}

fn legacy_tuning_enabled(config: &SchedulerConfig) -> bool {
    config.legacy_compact_concurrency == 0 && config.legacy_compact_auto_pause
}

fn legacy_tuner_context(runtime: &RuntimeState, snapshot: &PipelineSnapshot) -> LegacyTunerContext {
    if legacy_compact_exclusive_hold_reason(runtime, &snapshot.epochs, &snapshot.live).is_some() {
        LegacyTunerContext::FinalizerOverlap
    } else {
        LegacyTunerContext::Bulk
    }
}

fn legacy_tuner_bootstrap_lanes(config: &SchedulerConfig, context: LegacyTunerContext) -> usize {
    match context {
        LegacyTunerContext::Bulk => config.legacy_compact_min_running.max(1),
        LegacyTunerContext::FinalizerOverlap => 1,
    }
}

fn legacy_tuner_capacity_limit(
    config: &SchedulerConfig,
    snapshot: &PipelineSnapshot,
    runtime: &RuntimeState,
    now: u64,
) -> usize {
    if !legacy_tuning_enabled(config) {
        return usize::MAX;
    }
    let context = legacy_tuner_context(runtime, snapshot);
    let active = active_legacy_compact_rss(snapshot, runtime).len();
    legacy_tuner_capacity_limit_for_state(
        config,
        context,
        active,
        &runtime.legacy_throughput_tuner,
        now,
    )
}

fn legacy_tuner_capacity_limit_for_state(
    config: &SchedulerConfig,
    context: LegacyTunerContext,
    active: usize,
    tuner: &LegacyThroughputTuner,
    now: u64,
) -> usize {
    if tuner.guard_held {
        return 0;
    }
    if tuner.context != Some(context) || tuner.accepted_lanes == 0 {
        return active.max(legacy_tuner_bootstrap_lanes(config, context));
    }
    if tuner.backoff_until_unix_secs > now {
        return tuner.accepted_lanes;
    }
    if active < tuner.accepted_lanes {
        return tuner.accepted_lanes;
    }
    match tuner.phase {
        LegacyTunerPhase::ProbeUp { baseline_lanes, .. } => baseline_lanes.saturating_add(1),
        LegacyTunerPhase::VerifyPause { baseline_lanes, .. } => baseline_lanes,
        LegacyTunerPhase::VerifyDownshift { loaded_lanes, .. } => loaded_lanes.saturating_sub(1),
        LegacyTunerPhase::VerifyDownshiftReloaded { loaded_lanes, .. } => loaded_lanes,
        LegacyTunerPhase::CommitDownshift { loaded_lanes, .. } => loaded_lanes.saturating_sub(1),
        LegacyTunerPhase::Backoff => tuner.accepted_lanes,
        LegacyTunerPhase::ObserveBaseline
            if tuner.baseline_ready
                && tuner.degraded_windows == 0
                && now >= tuner.next_probe_unix_secs =>
        {
            tuner.accepted_lanes.saturating_add(1)
        }
        LegacyTunerPhase::ObserveBaseline => tuner.accepted_lanes,
    }
}

fn legacy_throughput_observation(
    snapshot: &PipelineSnapshot,
    now: u64,
) -> LegacyThroughputObservation {
    let lanes = snapshot
        .lanes
        .iter()
        .filter(|lane| lane.kind == "historical_compact_reuse" && lane.state == "running")
        .collect::<Vec<_>>();
    let active_lanes = lanes.len();
    let mut identities = BTreeSet::new();
    let mut logical_sampled = 0usize;
    let mut logical_mib_per_sec = 0.0;
    let mut process_io_sampled = 0usize;
    let mut process_io_mib_per_sec = 0.0;
    for lane in lanes {
        let linux_start_ticks = lane.pid.and_then(|pid| {
            process_stat_identity(pid)
                .filter(|(state, _)| *state != 'Z')
                .map(|(_, start_ticks)| start_ticks)
        });
        let start_identity = legacy_lane_start_identity(lane.started_unix_secs, linux_start_ticks);
        if let Some(((epoch, pid), started)) = lane.epoch.zip(lane.pid).zip(start_identity) {
            identities.insert((epoch, pid, started));
        }
        if let Some(rate) = lane.progress.input_mib_per_sec.filter(|rate| {
            rate.is_finite()
                && *rate >= 0.0
                && lane.progress.updated_unix_secs.is_some_and(|updated| {
                    now.saturating_sub(updated) <= LEGACY_TUNER_RATE_FRESH_SECS
                })
        }) {
            logical_mib_per_sec += rate;
            logical_sampled = logical_sampled.saturating_add(1);
        }
        if let Some(rate) = lane
            .progress
            .disk_read_mib_per_sec
            .zip(lane.progress.disk_write_mib_per_sec)
            .map(|(read, write)| read + write)
            .filter(|rate| rate.is_finite() && *rate >= 0.0)
        {
            process_io_mib_per_sec += rate;
            process_io_sampled = process_io_sampled.saturating_add(1);
        }
    }
    let identities_complete = active_lanes > 0 && identities.len() == active_lanes;
    let (sampled_lanes, useful_mib_per_sec, rate_source) =
        if identities_complete && logical_sampled == active_lanes {
            (
                logical_sampled,
                Some(logical_mib_per_sec),
                Some(LegacyTunerRateSource::LogicalInput),
            )
        } else if identities_complete && process_io_sampled == active_lanes {
            (
                process_io_sampled,
                Some(process_io_mib_per_sec),
                Some(LegacyTunerRateSource::ProcessIo),
            )
        } else {
            (logical_sampled.max(process_io_sampled), None, None)
        };
    let device_mib_per_sec = snapshot
        .machine
        .archive_device_read_mib_per_sec
        .zip(snapshot.machine.archive_device_write_mib_per_sec)
        .map(|(read, write)| read + write)
        .filter(|rate| rate.is_finite() && *rate >= 0.0);
    LegacyThroughputObservation {
        identities,
        active_lanes,
        sampled_lanes,
        useful_mib_per_sec,
        rate_source,
        device_mib_per_sec,
    }
}

fn update_current_live_ingest_health(
    snapshot: &PipelineSnapshot,
    tuner: &mut LegacyThroughputTuner,
    now: u64,
) -> bool {
    let mut current_ids = BTreeSet::new();
    let mut healthy = true;
    for capture in snapshot
        .live
        .iter()
        .filter(|capture| capture.is_current && capture.state == LiveState::Capturing)
    {
        current_ids.insert(capture.id.clone());
        let Some(pid) = capture.progress.pid else {
            healthy = false;
            continue;
        };
        let Some(slot) = capture.progress.last_slot.or(capture.last_slot) else {
            healthy = false;
            continue;
        };
        if !capture
            .progress
            .updated_unix_secs
            .is_some_and(|updated| now.saturating_sub(updated) <= LEGACY_TUNER_RATE_FRESH_SECS)
        {
            healthy = false;
            continue;
        }
        let watch =
            tuner
                .live_slot_watch
                .entry(capture.id.clone())
                .or_insert(LegacyLiveSlotWatch {
                    pid,
                    slot,
                    last_advanced_unix_secs: now,
                });
        if watch.pid != pid {
            *watch = LegacyLiveSlotWatch {
                pid,
                slot,
                last_advanced_unix_secs: now,
            };
        } else if slot > watch.slot {
            watch.slot = slot;
            watch.last_advanced_unix_secs = now;
        } else if slot < watch.slot
            || now.saturating_sub(watch.last_advanced_unix_secs) > LEGACY_TUNER_LIVE_STALL_SECS
        {
            healthy = false;
        }
    }
    tuner
        .live_slot_watch
        .retain(|capture_id, _| current_ids.contains(capture_id));
    healthy
}

fn newest_tuner_controlled_running_legacy_epoch(
    config: &SchedulerConfig,
    runtime: &RuntimeState,
    identities: &BTreeSet<(u64, u32, LegacyLaneStartIdentity)>,
) -> Option<u64> {
    identities
        .iter()
        .copied()
        .filter(|identity| legacy_identity_matches_controlled_lane(config, runtime, *identity))
        .max_by_key(|(_, _, start)| *start)
        .map(|(epoch, _, _)| epoch)
}

fn median_rate(samples: &VecDeque<f64>) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut values = samples.iter().copied().collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    Some(if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    })
}

fn legacy_tuner_window_result(
    tuner: &mut LegacyThroughputTuner,
    now: u64,
) -> Option<(f64, Option<f64>)> {
    let window = tuner.window.as_ref()?;
    if now.saturating_sub(window.started_unix_secs) < LEGACY_TUNER_SETTLE_SECS
        || window.useful_samples.len() < LEGACY_TUNER_MIN_SAMPLES
    {
        return None;
    }
    let useful = median_rate(&window.useful_samples)?;
    let device = median_rate(&window.device_samples);
    tuner.window = None;
    Some((useful, device))
}

fn legacy_tuner_degradation(
    accepted_mib_per_sec: f64,
    measured_mib_per_sec: f64,
) -> Option<(f64, f64)> {
    if !accepted_mib_per_sec.is_finite()
        || !measured_mib_per_sec.is_finite()
        || accepted_mib_per_sec <= 0.0
        || measured_mib_per_sec < 0.0
    {
        return None;
    }
    let drop = accepted_mib_per_sec - measured_mib_per_sec;
    let required = LEGACY_TUNER_DEGRADATION_MIN_MIB_PER_SEC
        .max(accepted_mib_per_sec * LEGACY_TUNER_DEGRADATION_MIN_RATIO);
    (drop >= required).then_some((drop, required))
}

fn clear_legacy_tuner_degradation(tuner: &mut LegacyThroughputTuner) {
    tuner.degraded_windows = 0;
    tuner.degraded_identities = None;
}

fn clear_legacy_tuner_audit(tuner: &mut LegacyThroughputTuner) {
    tuner.audit_loaded_identities = None;
    tuner.audit_survivor_identities = None;
    tuner.audit_started_unix_secs = 0;
}

fn queue_legacy_tuner_action(
    tuner: &mut LegacyThroughputTuner,
    action: LegacyTunerAction,
    now: u64,
) {
    tuner.pending_action = Some(action);
    tuner.pending_action_started_unix_secs = now;
}

fn cancel_legacy_tuner_experiment(
    tuner: &mut LegacyThroughputTuner,
    now: u64,
    reason: impl Into<String>,
) {
    tuner.phase = LegacyTunerPhase::ObserveBaseline;
    tuner.pending_action = None;
    tuner.pending_action_started_unix_secs = 0;
    tuner.probe_identity = None;
    tuner.window = None;
    tuner.baseline_ready = false;
    clear_legacy_tuner_degradation(tuner);
    clear_legacy_tuner_audit(tuner);
    tuner.next_probe_unix_secs = now.saturating_add(LEGACY_TUNER_BACKOFF_SECS);
    tuner.backoff_until_unix_secs = 0;
    tuner.last_decision = Some(reason.into());
}

fn legacy_identity_matches_controlled_lane(
    config: &SchedulerConfig,
    runtime: &RuntimeState,
    identity: (u64, u32, LegacyLaneStartIdentity),
) -> bool {
    let (epoch, pid, start) = identity;
    if let Some(child) = runtime.legacy_compacts.get(&epoch) {
        if child.pid != Some(pid) {
            return false;
        }
        return match start {
            LegacyLaneStartIdentity::LinuxStartTicks(expected) => {
                process_stat_identity(pid).is_some_and(|(_, actual)| actual == expected)
                    && process_cmdline_matches_legacy_exact(config, epoch, pid) == Some(true)
                    && process_is_group_leader(pid)
            }
            // Portable test/non-Linux fallback. Production Linux observations
            // always carry kernel start ticks and therefore take the stronger arm.
            LegacyLaneStartIdentity::UnixSecs(expected) => {
                #[cfg(target_os = "linux")]
                {
                    // A wall-clock second is not a safe signal identity on Linux:
                    // PID reuse can occur inside that interval. Missing /proc
                    // start ticks therefore disables the action rather than
                    // weakening SIGSTOP/SIGCONT ownership proof.
                    let _ = expected;
                    false
                }
                #[cfg(not(target_os = "linux"))]
                {
                    child.started_unix_secs == expected
                }
            }
        };
    }

    let Some(adopted) = runtime.adopted_legacy_compacts.get(&epoch) else {
        return false;
    };
    if adopted.identity_tainted || adopted.pid != pid {
        return false;
    }
    let LegacyLaneStartIdentity::LinuxStartTicks(expected) = start else {
        // Adopted workers only become signal-eligible when Linux gives us a
        // monotonic start-tick identity. Their wall-clock display timestamp is
        // never sufficient ownership proof.
        return false;
    };
    if adopted.process_start_ticks != expected
        || !process_stat_identity(pid)
            .is_some_and(|(state, actual)| state != 'Z' && actual == adopted.process_start_ticks)
        || process_cmdline_matches_legacy_exact(config, epoch, pid) != Some(true)
        || !process_is_group_leader(pid)
    {
        return false;
    }
    read_ownership(&config.archive_root.join(format!("epoch-{epoch}"))).is_some_and(|owner| {
        owner_matches_legacy_identity(&owner, epoch, pid, adopted.owner_schema_version)
    })
}

fn controlled_legacy_pid(
    config: &SchedulerConfig,
    runtime: &RuntimeState,
    epoch: u64,
) -> Option<u32> {
    let (pid, start) = if let Some(child) = runtime.legacy_compacts.get(&epoch) {
        let pid = child.pid?;
        let start = process_stat_identity(pid)?.1;
        (pid, LegacyLaneStartIdentity::LinuxStartTicks(start))
    } else {
        let adopted = runtime.adopted_legacy_compacts.get(&epoch)?;
        (
            adopted.pid,
            LegacyLaneStartIdentity::LinuxStartTicks(adopted.process_start_ticks),
        )
    };
    legacy_identity_matches_controlled_lane(config, runtime, (epoch, pid, start)).then_some(pid)
}

fn exact_tuner_pause_owned(
    config: &SchedulerConfig,
    runtime: &RuntimeState,
    identity: (u64, u32, LegacyLaneStartIdentity),
) -> bool {
    let (epoch, pid, start) = identity;
    if runtime
        .paused_jobs
        .contains(&format!("compact_reuse:{epoch}"))
        || !legacy_identity_matches_controlled_lane(config, runtime, identity)
    {
        return false;
    }
    runtime
        .auto_paused_legacy
        .get(&epoch)
        .is_some_and(|record| {
            record.pid == pid
                && legacy_tuner_owns_pause_reason(&record.reason)
                && match start {
                    LegacyLaneStartIdentity::LinuxStartTicks(expected) => {
                        record.process_start_ticks == Some(expected)
                    }
                    LegacyLaneStartIdentity::UnixSecs(_) => true,
                }
        })
}

fn reset_legacy_tuner_context(runtime: &mut RuntimeState, context: LegacyTunerContext, now: u64) {
    remember_legacy_tuner_profile(runtime);
    let paused_probe = runtime
        .auto_paused_legacy
        .values()
        .find(|record| legacy_tuner_owns_pause_reason(&record.reason))
        .map(|record| {
            (
                record.epoch,
                record.pid,
                record
                    .process_start_ticks
                    .map(LegacyLaneStartIdentity::LinuxStartTicks),
            )
        });
    let profile = runtime.legacy_tuner_profiles.get(context).clone();
    runtime.legacy_throughput_tuner = LegacyThroughputTuner {
        context: Some(context),
        accepted_lanes: profile.accepted_lanes,
        accepted_useful_mib_per_sec: profile.accepted_useful_mib_per_sec,
        accepted_rate_source: profile.accepted_rate_source,
        accepted_device_mib_per_sec: profile.accepted_device_mib_per_sec,
        next_probe_unix_secs: now.saturating_add(LEGACY_TUNER_PROBE_COOLDOWN_SECS),
        pending_action: paused_probe.map(|(epoch, pid, start)| LegacyTunerAction::Resume {
            epoch,
            expected_identity: start.map(|start| (epoch, pid, start)),
            reason: "scheduler context changed; re-observe resumed probe".to_string(),
        }),
        pending_action_started_unix_secs: paused_probe.is_some().then_some(now).unwrap_or_default(),
        ..LegacyThroughputTuner::default()
    };
}

fn legacy_tuner_owns_pause_reason(reason: &str) -> bool {
    reason.starts_with(LEGACY_TUNER_PAUSE_PREFIX)
        || reason.starts_with(LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX)
        || reason.starts_with(LEGACY_TUNER_GUARD_PAUSE_PREFIX)
}

fn observe_legacy_throughput_tuner(
    config: &SchedulerConfig,
    snapshot: &PipelineSnapshot,
    runtime: &mut RuntimeState,
    now: u64,
) {
    if !legacy_tuning_enabled(config) {
        return;
    }
    let context = legacy_tuner_context(runtime, snapshot);
    if runtime.legacy_throughput_tuner.context != Some(context) {
        reset_legacy_tuner_context(runtime, context, now);
    }

    let mut observation = legacy_throughput_observation(snapshot, now);
    let newest_controlled_epoch =
        newest_tuner_controlled_running_legacy_epoch(config, runtime, &observation.identities);
    let paused_throughput_probe = runtime
        .auto_paused_legacy
        .values()
        .find(|record| {
            record.reason.starts_with(LEGACY_TUNER_PAUSE_PREFIX)
                || record
                    .reason
                    .starts_with(LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX)
        })
        .map(|record| record.epoch);
    let paused_by_guard = runtime
        .auto_paused_legacy
        .values()
        .find(|record| record.reason.starts_with(LEGACY_TUNER_GUARD_PAUSE_PREFIX))
        .map(|record| record.epoch);
    let legacy_pause_present = !runtime.auto_paused_legacy.is_empty()
        || runtime
            .paused_jobs
            .iter()
            .any(|key| key.starts_with("compact_reuse:"));
    let pressure = legacy_pressure_state(config, &snapshot.machine);
    let pressure_recovered = matches!(&pressure, LegacyPressureState::Resume);
    // One lane is the functional floor: `legacy_compact_min_running` is the
    // bootstrap target, not a permanent claim that two lanes must be faster.
    let downshift_floor = 1usize;
    let audit_identity = runtime.legacy_throughput_tuner.probe_identity;
    let exact_probe_pause_owned =
        audit_identity.is_some_and(|identity| exact_tuner_pause_owned(config, runtime, identity));
    let audit_pause_owned = exact_probe_pause_owned
        && audit_identity.is_some_and(|identity| {
            runtime
                .auto_paused_legacy
                .get(&identity.0)
                .is_some_and(|record| {
                    record
                        .reason
                        .starts_with(LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX)
                })
        });
    let rejected_probe_pause_owned = exact_probe_pause_owned
        && audit_identity.is_some_and(|identity| {
            runtime
                .auto_paused_legacy
                .get(&identity.0)
                .is_some_and(|record| record.reason.starts_with(LEGACY_TUNER_PAUSE_PREFIX))
        });
    let audit_manually_paused = audit_identity.is_some_and(|(epoch, _, _)| {
        runtime
            .paused_jobs
            .contains(&format!("compact_reuse:{epoch}"))
    });
    let paused_tuner_identities = runtime
        .auto_paused_legacy
        .values()
        .filter(|record| legacy_tuner_owns_pause_reason(&record.reason))
        .filter_map(|record| {
            record.process_start_ticks.map(|ticks| {
                (
                    record.epoch,
                    (
                        record.epoch,
                        record.pid,
                        LegacyLaneStartIdentity::LinuxStartTicks(ticks),
                    ),
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    let tuner = &mut runtime.legacy_throughput_tuner;
    // Once native logical-input telemetry is established, a transient stale
    // progress sample must not downgrade the objective and invalidate a good
    // baseline. Process I/O is only a compatibility fallback for old workers.
    if tuner.accepted_rate_source == Some(LegacyTunerRateSource::LogicalInput)
        && observation.rate_source == Some(LegacyTunerRateSource::ProcessIo)
    {
        observation.useful_mib_per_sec = None;
        observation.rate_source = None;
    }
    let rate_source_changed = tuner.current_rate_source.is_some()
        && observation.rate_source.is_some()
        && tuner.current_rate_source != observation.rate_source;
    if rate_source_changed {
        tuner.window = None;
        tuner.baseline_ready = false;
        clear_legacy_tuner_degradation(tuner);
        tuner.last_decision = Some("throughput metric source changed; re-observing".into());
    }
    if tuner.accepted_rate_source.is_some()
        && observation.rate_source.is_some()
        && tuner.accepted_rate_source != observation.rate_source
    {
        tuner.phase = LegacyTunerPhase::ObserveBaseline;
        tuner.accepted_useful_mib_per_sec = None;
        tuner.accepted_rate_source = None;
        tuner.accepted_device_mib_per_sec = None;
        tuner.probe_identity = None;
        tuner.window = None;
        tuner.baseline_ready = false;
        clear_legacy_tuner_degradation(tuner);
        clear_legacy_tuner_audit(tuner);
        tuner.next_probe_unix_secs = now.saturating_add(LEGACY_TUNER_PROBE_COOLDOWN_SECS);
        tuner.last_decision = Some(format!(
            "throughput metric source changed; recalibrating rate without discarding the proven {}-lane topology",
            tuner.accepted_lanes,
        ));
    }
    tuner.current_active_lanes = observation.active_lanes;
    tuner.current_sampled_lanes = observation.sampled_lanes;
    tuner.current_useful_mib_per_sec = observation.useful_mib_per_sec;
    tuner.current_rate_source = observation.rate_source;
    if !pressure_recovered {
        tuner.baseline_ready = false;
    }

    if tuner.phase_matches_backoff() && now >= tuner.backoff_until_unix_secs {
        if let Some(identity @ (epoch, _, _)) = tuner.probe_identity
            && paused_throughput_probe == Some(epoch)
        {
            queue_legacy_tuner_action(
                tuner,
                LegacyTunerAction::Resume {
                    epoch,
                    expected_identity: Some(identity),
                    reason: "throughput-probe backoff elapsed".to_string(),
                },
                now,
            );
            tuner.phase = LegacyTunerPhase::ProbeUp {
                baseline_lanes: tuner.accepted_lanes,
                baseline_mib_per_sec: tuner.accepted_useful_mib_per_sec.unwrap_or_default(),
                probe_epoch: epoch,
            };
            tuner.window = None;
            tuner.last_decision = Some(format!(
                "retrying lane {} after throughput-probe backoff",
                epoch
            ));
            return;
        }
        tuner.phase = LegacyTunerPhase::ObserveBaseline;
        tuner.next_probe_unix_secs = now;
        tuner.backoff_until_unix_secs = 0;
        tuner.probe_identity = None;
        tuner.baseline_ready = false;
    }

    // Live ingest health is operational telemetry, not a compaction resource
    // guard. A disconnected or half-open producer must be repaired by its own
    // supervisor/watchdog; stopping historical compaction neither repairs the
    // stream nor releases worker RSS. Keep observing slot advancement so the
    // live status remains meaningful, but never turn a producer failure into
    // zero historical throughput.
    let _live_ingest_healthy = update_current_live_ingest_health(snapshot, tuner, now);
    let hard_guard = if config.legacy_compact_io_budget_mib_per_sec > 0
        && observation
            .device_mib_per_sec
            .is_some_and(|rate| rate >= config.legacy_compact_io_budget_mib_per_sec as f64)
    {
        Some(format!(
            "archive-device throughput reached configured ceiling {} MiB/s",
            config.legacy_compact_io_budget_mib_per_sec
        ))
    } else {
        None
    };
    if let Some(reason) = hard_guard {
        tuner.guard_held = true;
        tuner.window = None;
        clear_legacy_tuner_degradation(tuner);
        // A hard guard invalidates any queued resume from an earlier context.
        // Keep at most the next pause request; target=0 prevents refills while
        // exactly controlled lanes are stopped one reconciliation pass at a time.
        tuner.pending_action = newest_controlled_epoch.map(|epoch| LegacyTunerAction::Pause {
            epoch,
            expected_identity: observation
                .identities
                .iter()
                .find(|(candidate, _, _)| *candidate == epoch)
                .copied(),
            reason: format!("{LEGACY_TUNER_GUARD_PAUSE_PREFIX} {reason}"),
        });
        tuner.pending_action_started_unix_secs = tuner
            .pending_action
            .is_some()
            .then_some(now)
            .unwrap_or_default();
        tuner.last_decision = Some(format!("held throughput probing: {reason}"));
        return;
    }
    if tuner.guard_held {
        tuner.window = None;
        if let Some(epoch) = paused_by_guard {
            if tuner.pending_action.is_none() {
                queue_legacy_tuner_action(
                    tuner,
                    LegacyTunerAction::Resume {
                        epoch,
                        expected_identity: paused_tuner_identities.get(&epoch).copied(),
                        reason: "throughput-tuner hard guard recovered".to_string(),
                    },
                    now,
                );
            }
            tuner.last_decision = Some(format!(
                "hard guard recovered; resuming compact_reuse:{epoch} before probing"
            ));
            return;
        }
        if tuner.pending_action.is_some() {
            return;
        }
        tuner.guard_held = false;
        tuner.phase = LegacyTunerPhase::ObserveBaseline;
        tuner.probe_identity = None;
        clear_legacy_tuner_degradation(tuner);
        tuner.next_probe_unix_secs = now.saturating_add(LEGACY_TUNER_PROBE_COOLDOWN_SECS);
        tuner.last_decision =
            Some("hard guard recovered; restoring the proven topology before probing".to_string());
        return;
    }
    // A pause/resume request is part of the experiment. Do not score samples
    // until the signal has succeeded and the next reconciled topology proves
    // that the requested state is in effect.
    if tuner.pending_action.is_some() {
        tuner.window = None;
        return;
    }
    let protected_probe_epoch = match tuner.phase {
        LegacyTunerPhase::VerifyPause { probe_epoch, .. } => Some(probe_epoch),
        LegacyTunerPhase::VerifyDownshift { audit_epoch, .. }
        | LegacyTunerPhase::VerifyDownshiftReloaded { audit_epoch, .. }
        | LegacyTunerPhase::CommitDownshift { audit_epoch, .. } => Some(audit_epoch),
        LegacyTunerPhase::Backoff => paused_throughput_probe,
        _ => None,
    };
    if let Some(epoch) = runtime
        .auto_paused_legacy
        .values()
        .find(|record| {
            legacy_tuner_owns_pause_reason(&record.reason)
                && Some(record.epoch) != protected_probe_epoch
        })
        .map(|record| record.epoch)
    {
        let expected_identity = paused_tuner_identities.get(&epoch).copied();
        queue_legacy_tuner_action(
            tuner,
            LegacyTunerAction::Resume {
                epoch,
                expected_identity,
                reason: "recovering an orphaned throughput-tuner pause".to_string(),
            },
            now,
        );
        tuner.window = None;
        tuner.last_decision = Some(format!(
            "resuming orphaned tuner pause compact_reuse:{epoch} before probing"
        ));
        return;
    }

    let downshift_phase = matches!(
        tuner.phase,
        LegacyTunerPhase::VerifyDownshift { .. }
            | LegacyTunerPhase::VerifyDownshiftReloaded { .. }
            | LegacyTunerPhase::CommitDownshift { .. }
    );
    if downshift_phase
        && tuner.audit_started_unix_secs > 0
        && now.saturating_sub(tuner.audit_started_unix_secs) >= LEGACY_TUNER_DOWNSHIFT_TIMEOUT_SECS
    {
        let identity = tuner.probe_identity;
        cancel_legacy_tuner_experiment(
            tuner,
            now,
            "downshift audit timed out; preserving the proven topology",
        );
        if audit_pause_owned && let Some(identity @ (epoch, _, _)) = identity {
            queue_legacy_tuner_action(
                tuner,
                LegacyTunerAction::Resume {
                    epoch,
                    expected_identity: Some(identity),
                    reason: "downshift audit timed out; resume the exactly owned lane".to_string(),
                },
                now,
            );
        }
        return;
    }
    if downshift_phase && !pressure_recovered {
        match pressure {
            LegacyPressureState::Pause(reason) => {
                let identity = tuner.probe_identity;
                cancel_legacy_tuner_experiment(
                    tuner,
                    now,
                    format!(
                        "resource pressure interrupted the downshift audit ({reason}); preserving the proven topology"
                    ),
                );
                if audit_pause_owned && let Some(identity @ (epoch, _, _)) = identity {
                    queue_legacy_tuner_action(
                        tuner,
                        LegacyTunerAction::Resume {
                            epoch,
                            expected_identity: Some(identity),
                            reason: "resource-pressure audit interruption; resume after recovery"
                                .to_string(),
                        },
                        now,
                    );
                }
            }
            LegacyPressureState::Hold => {
                tuner.window = None;
                tuner.baseline_ready = false;
                tuner.last_decision = Some(
                    "waiting for resource pressure to recover before scoring the downshift audit"
                        .to_string(),
                );
            }
            LegacyPressureState::Resume => unreachable!(),
        }
        return;
    }
    if let LegacyTunerPhase::CommitDownshift {
        loaded_lanes,
        reduced_mib_per_sec,
        reduced_device_mib_per_sec,
        audit_epoch,
    } = tuner.phase
    {
        let exact_survivors =
            tuner.audit_survivor_identities.as_ref() == Some(&observation.identities);
        if !audit_pause_owned || !exact_survivors {
            let identity = tuner.probe_identity;
            cancel_legacy_tuner_experiment(
                tuner,
                now,
                format!(
                    "could not prove final pause ownership for compact_reuse:{audit_epoch}; preserving {loaded_lanes} lanes"
                ),
            );
            if audit_pause_owned && let Some(identity @ (epoch, _, _)) = identity {
                queue_legacy_tuner_action(
                    tuner,
                    LegacyTunerAction::Resume {
                        epoch,
                        expected_identity: Some(identity),
                        reason: "downshift commit topology changed; preserve accepted lane"
                            .to_string(),
                    },
                    now,
                );
            }
            return;
        }
        tuner.accepted_lanes = loaded_lanes.saturating_sub(1).max(1);
        tuner.accepted_useful_mib_per_sec = Some(reduced_mib_per_sec);
        tuner.accepted_rate_source = Some(LegacyTunerRateSource::LogicalInput);
        tuner.accepted_device_mib_per_sec = reduced_device_mib_per_sec;
        tuner.phase = LegacyTunerPhase::Backoff;
        tuner.backoff_until_unix_secs =
            now.saturating_add(LEGACY_TUNER_CONFIRMED_DOWNSHIFT_BACKOFF_SECS);
        tuner.window = None;
        tuner.baseline_ready = false;
        clear_legacy_tuner_degradation(tuner);
        clear_legacy_tuner_audit(tuner);
        tuner.last_decision = Some(format!(
            "accepted {} lanes only after N→N-1→N confirmation; compact_reuse:{audit_epoch} remains paused until probe backoff {}",
            tuner.accepted_lanes, tuner.backoff_until_unix_secs,
        ));
        return;
    }

    let Some(useful) = observation.useful_mib_per_sec else {
        tuner.window = None;
        if matches!(tuner.phase, LegacyTunerPhase::ObserveBaseline) {
            clear_legacy_tuner_degradation(tuner);
        }
        tuner.last_decision = Some(format!(
            "waiting for complete useful-throughput coverage ({}/{})",
            observation.sampled_lanes, observation.active_lanes
        ));
        return;
    };
    match tuner.phase {
        LegacyTunerPhase::ProbeUp {
            baseline_lanes,
            probe_epoch,
            ..
        } => {
            if tuner.audit_loaded_identities.is_none()
                && let Some(probe_identity) = tuner.probe_identity
            {
                tuner.audit_loaded_identities = Some(observation.identities.clone());
                tuner.audit_survivor_identities = Some(
                    observation
                        .identities
                        .iter()
                        .copied()
                        .filter(|identity| *identity != probe_identity)
                        .collect(),
                );
                tuner.window = None;
                tuner.last_decision = Some(format!(
                    "captured exact {}-lane probe topology for compact_reuse:{probe_epoch}",
                    observation.active_lanes,
                ));
                return;
            }
            let probe_present = tuner.probe_identity.map_or_else(
                || {
                    observation
                        .identities
                        .iter()
                        .any(|(epoch, _, _)| *epoch == probe_epoch)
                },
                |identity| observation.identities.contains(&identity),
            );
            let exact_probe_topology = tuner
                .audit_loaded_identities
                .as_ref()
                .is_none_or(|expected| expected == &observation.identities);
            if observation.active_lanes != baseline_lanes.saturating_add(1)
                || !probe_present
                || !exact_probe_topology
            {
                cancel_legacy_tuner_experiment(
                    tuner,
                    now,
                    format!(
                        "probe compact_reuse:{probe_epoch} topology changed; preserving the accepted {baseline_lanes}-lane floor"
                    ),
                );
                return;
            }
        }
        LegacyTunerPhase::VerifyPause {
            baseline_lanes,
            probe_epoch,
            ..
        } => {
            let probe_present = observation
                .identities
                .iter()
                .any(|(epoch, _, _)| *epoch == probe_epoch);
            let exact_survivors = tuner
                .audit_survivor_identities
                .as_ref()
                .is_none_or(|expected| expected == &observation.identities);
            if observation.active_lanes != baseline_lanes
                || probe_present
                || !exact_survivors
                || (tuner.probe_identity.is_some() && !rejected_probe_pause_owned)
            {
                let identity = tuner.probe_identity;
                cancel_legacy_tuner_experiment(
                    tuner,
                    now,
                    format!(
                        "probe compact_reuse:{probe_epoch} lost exact pause ownership or survivor identity; preserving {baseline_lanes} lanes"
                    ),
                );
                if rejected_probe_pause_owned && let Some(identity @ (epoch, _, _)) = identity {
                    queue_legacy_tuner_action(
                        tuner,
                        LegacyTunerAction::Resume {
                            epoch,
                            expected_identity: Some(identity),
                            reason: "probe verification topology changed".to_string(),
                        },
                        now,
                    );
                }
                return;
            }
        }
        LegacyTunerPhase::VerifyDownshift {
            loaded_lanes,
            audit_epoch,
            ..
        } => {
            let exact_survivors =
                tuner.audit_survivor_identities.as_ref() == Some(&observation.identities);
            if !audit_pause_owned || !exact_survivors {
                let identity = tuner.probe_identity;
                cancel_legacy_tuner_experiment(
                    tuner,
                    now,
                    format!(
                        "downshift audit compact_reuse:{audit_epoch} lost exact pause ownership or survivor identity; preserving {loaded_lanes} lanes"
                    ),
                );
                if audit_pause_owned && let Some(identity @ (epoch, _, _)) = identity {
                    queue_legacy_tuner_action(
                        tuner,
                        LegacyTunerAction::Resume {
                            epoch,
                            expected_identity: Some(identity),
                            reason: "downshift audit topology changed; preserve accepted lane"
                                .to_string(),
                        },
                        now,
                    );
                }
                return;
            }
        }
        LegacyTunerPhase::VerifyDownshiftReloaded {
            loaded_lanes,
            audit_epoch,
            ..
        } => {
            let exact_loaded =
                tuner.audit_loaded_identities.as_ref() == Some(&observation.identities);
            if !exact_loaded || audit_pause_owned || audit_manually_paused {
                cancel_legacy_tuner_experiment(
                    tuner,
                    now,
                    format!(
                        "reloaded downshift audit compact_reuse:{audit_epoch} changed identity; preserving {loaded_lanes} lanes"
                    ),
                );
                return;
            }
        }
        LegacyTunerPhase::CommitDownshift { .. } => unreachable!("handled before telemetry gating"),
        LegacyTunerPhase::Backoff => return,
        LegacyTunerPhase::ObserveBaseline => {}
    }
    if matches!(tuner.phase, LegacyTunerPhase::ObserveBaseline)
        && tuner.accepted_lanes > observation.active_lanes
    {
        tuner.window = None;
        tuner.baseline_ready = false;
        clear_legacy_tuner_degradation(tuner);
        tuner.last_decision = Some(format!(
            "restoring proven {}-lane topology ({}/{} currently active)",
            tuner.accepted_lanes, observation.active_lanes, tuner.accepted_lanes,
        ));
        return;
    }
    if matches!(tuner.phase, LegacyTunerPhase::ObserveBaseline)
        && tuner.accepted_lanes > 0
        && observation.active_lanes > tuner.accepted_lanes.saturating_add(1)
    {
        tuner.window = None;
        tuner.baseline_ready = false;
        clear_legacy_tuner_degradation(tuner);
        tuner.last_decision = Some(format!(
            "holding the proven {}-lane target while an unattributed {}-lane topology converges; additions are evaluated one lane at a time",
            tuner.accepted_lanes, observation.active_lanes,
        ));
        return;
    }
    if matches!(tuner.phase, LegacyTunerPhase::ObserveBaseline) && !pressure_recovered {
        tuner.window = None;
        tuner.baseline_ready = false;
        tuner.last_decision = Some(
            "waiting for memory, CPU, and I/O pressure to recover before measuring throughput"
                .to_string(),
        );
        return;
    }
    let identities_changed = tuner
        .window
        .as_ref()
        .is_none_or(|window| window.identities != observation.identities);
    if identities_changed {
        tuner.window = Some(LegacyTunerWindow {
            identities: observation.identities.clone(),
            started_unix_secs: now,
            useful_samples: VecDeque::new(),
            device_samples: VecDeque::new(),
        });
    }
    if let Some(window) = tuner.window.as_mut() {
        window.useful_samples.push_back(useful);
        if let Some(device) = observation.device_mib_per_sec {
            window.device_samples.push_back(device);
        }
        while window.useful_samples.len() > 64 {
            window.useful_samples.pop_front();
        }
        while window.device_samples.len() > 64 {
            window.device_samples.pop_front();
        }
    }

    if matches!(tuner.phase, LegacyTunerPhase::ObserveBaseline)
        && tuner.accepted_lanes > 0
        && observation.active_lanes == tuner.accepted_lanes.saturating_add(1)
    {
        let Some(probe_epoch) = newest_controlled_epoch else {
            tuner.window = None;
            tuner.last_decision = Some(
                "cannot attribute the extra lane to an exactly controlled probe; re-observing topology"
                    .to_string(),
            );
            return;
        };
        tuner.probe_identity = observation
            .identities
            .iter()
            .find(|(epoch, _, _)| *epoch == probe_epoch)
            .copied();
        tuner.audit_loaded_identities = Some(observation.identities.clone());
        tuner.audit_survivor_identities = tuner.probe_identity.map(|probe_identity| {
            observation
                .identities
                .iter()
                .copied()
                .filter(|identity| *identity != probe_identity)
                .collect()
        });
        tuner.phase = LegacyTunerPhase::ProbeUp {
            baseline_lanes: tuner.accepted_lanes,
            baseline_mib_per_sec: tuner.accepted_useful_mib_per_sec.unwrap_or_default(),
            probe_epoch,
        };
        tuner.baseline_ready = false;
        tuner.window = None;
        tuner.last_decision = Some(format!(
            "probing {} lanes with compact_reuse:{}",
            observation.active_lanes, probe_epoch
        ));
        return;
    }

    let Some((measured, device)) = legacy_tuner_window_result(tuner, now) else {
        return;
    };
    let rate_source = observation
        .rate_source
        .expect("a completed tuner window must have a rate source");
    let rate_label = rate_source.label();
    match tuner.phase {
        LegacyTunerPhase::ObserveBaseline => {
            let exact_accepted_topology =
                tuner.accepted_lanes > 0 && observation.active_lanes == tuner.accepted_lanes;
            let controlled_audit_epoch = newest_controlled_epoch.filter(|epoch| {
                observation
                    .identities
                    .iter()
                    .any(|(candidate, _, _)| candidate == epoch)
            });
            let throughput_comparable = exact_accepted_topology
                && observation.active_lanes > downshift_floor
                && tuner.accepted_rate_source == Some(LegacyTunerRateSource::LogicalInput)
                && rate_source == LegacyTunerRateSource::LogicalInput
                && pressure_recovered
                && !legacy_pause_present;
            let downshift_eligible = throughput_comparable && controlled_audit_epoch.is_some();
            let degradation = downshift_eligible
                .then(|| tuner.accepted_useful_mib_per_sec)
                .flatten()
                .and_then(|accepted| legacy_tuner_degradation(accepted, measured));
            if let Some((drop, required_drop)) = degradation {
                if tuner.degraded_identities.as_ref() == Some(&observation.identities) {
                    tuner.degraded_windows = tuner.degraded_windows.saturating_add(1);
                } else {
                    tuner.degraded_windows = 1;
                    tuner.degraded_identities = Some(observation.identities.clone());
                }
                tuner.window = None;
                tuner.baseline_ready = false;
                // The next window starts on the following reconciliation
                // pass. Keep the upward-probe gate closed for one additional
                // cooldown so the full comparison window can settle first.
                tuner.next_probe_unix_secs = now
                    .saturating_add(LEGACY_TUNER_SETTLE_SECS)
                    .saturating_add(LEGACY_TUNER_PROBE_COOLDOWN_SECS);
                if tuner.degraded_windows < LEGACY_TUNER_DEGRADATION_WINDOWS {
                    tuner.last_decision = Some(format!(
                        "sustained-throughput check {}/{}: {} lanes measured {:.1} MiB/s, down {:.1} from the {:.1} baseline (trigger {:.1})",
                        tuner.degraded_windows,
                        LEGACY_TUNER_DEGRADATION_WINDOWS,
                        observation.active_lanes,
                        measured,
                        drop,
                        tuner.accepted_useful_mib_per_sec.unwrap_or_default(),
                        required_drop,
                    ));
                    return;
                }
                let audit_epoch = controlled_audit_epoch
                    .expect("eligible downshift has an exactly controlled lane");
                let reason = format!(
                    "{LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX} sustained degradation at {} lanes ({:.1} vs {:.1} MiB/s); testing whether N-1 improves aggregate logical input",
                    observation.active_lanes,
                    measured,
                    tuner.accepted_useful_mib_per_sec.unwrap_or_default(),
                );
                let audit_identity = observation
                    .identities
                    .iter()
                    .find(|(epoch, _, _)| *epoch == audit_epoch)
                    .copied()
                    .expect("eligible downshift has a complete managed identity");
                queue_legacy_tuner_action(
                    tuner,
                    LegacyTunerAction::Pause {
                        epoch: audit_epoch,
                        expected_identity: Some(audit_identity),
                        reason: reason.clone(),
                    },
                    now,
                );
                tuner.phase = LegacyTunerPhase::VerifyDownshift {
                    loaded_lanes: observation.active_lanes,
                    loaded_mib_per_sec: measured,
                    audit_epoch,
                };
                tuner.probe_identity = Some(audit_identity);
                tuner.audit_loaded_identities = Some(observation.identities.clone());
                tuner.audit_survivor_identities = Some(
                    observation
                        .identities
                        .iter()
                        .copied()
                        .filter(|identity| *identity != audit_identity)
                        .collect(),
                );
                tuner.audit_started_unix_secs = now;
                tuner.baseline_ready = false;
                clear_legacy_tuner_degradation(tuner);
                tuner.last_decision = Some(reason);
                return;
            }
            let suspected_decline = throughput_comparable
                .then(|| tuner.accepted_useful_mib_per_sec)
                .flatten()
                .and_then(|accepted| {
                    let drop = accepted - measured;
                    let watch = LEGACY_TUNER_MIN_GAIN_MIB_PER_SEC
                        .max(accepted * LEGACY_TUNER_MIN_GAIN_RATIO);
                    (drop >= watch).then_some((accepted, drop, watch))
                });
            if let Some((accepted, drop, watch)) = suspected_decline {
                clear_legacy_tuner_degradation(tuner);
                tuner.baseline_ready = false;
                tuner.next_probe_unix_secs = now.saturating_add(LEGACY_TUNER_SETTLE_SECS);
                tuner.last_decision = Some(format!(
                    "holding {} lanes while aggregate {rate_label} is {:.1} MiB/s below the {:.1} proven baseline (watch threshold {:.1}); waiting for recovery or sustained audit evidence",
                    observation.active_lanes, drop, accepted, watch,
                ));
                return;
            }
            clear_legacy_tuner_degradation(tuner);
            let same_proven_topology = tuner.accepted_lanes == observation.active_lanes
                && tuner.accepted_rate_source == Some(rate_source);
            let proven_rate = tuner.accepted_useful_mib_per_sec;
            let bootstrapped = tuner.accepted_lanes == 0;
            if bootstrapped {
                // A controller restart may inherit several byte-exact,
                // start-tick-verified workers. Establish that existing
                // topology as the initial baseline without disrupting it;
                // every future addition still passes through ProbeUp.
                tuner.accepted_lanes = observation.active_lanes;
            }
            tuner.accepted_useful_mib_per_sec = Some(
                proven_rate
                    .filter(|_| same_proven_topology)
                    .map_or(measured, |proven| proven.max(measured)),
            );
            tuner.accepted_rate_source = Some(rate_source);
            tuner.accepted_device_mib_per_sec = device;
            tuner.probe_identity = None;
            clear_legacy_tuner_audit(tuner);
            tuner.baseline_ready = true;
            tuner.next_probe_unix_secs = now.saturating_add(LEGACY_TUNER_PROBE_COOLDOWN_SECS);
            tuner.last_decision = Some(format!(
                "{} {}-lane baseline at {:.1} MiB/s ({rate_label}); future additions are one lane at a time",
                if bootstrapped {
                    "bootstrapped"
                } else {
                    "refreshed"
                },
                observation.active_lanes,
                measured,
            ));
        }
        LegacyTunerPhase::ProbeUp {
            baseline_lanes,
            baseline_mib_per_sec,
            probe_epoch,
        } => {
            let required = LEGACY_TUNER_MIN_GAIN_MIB_PER_SEC
                .max(baseline_mib_per_sec * LEGACY_TUNER_MIN_GAIN_RATIO);
            let gain = measured - baseline_mib_per_sec;
            if gain >= required {
                tuner.accepted_lanes = observation.active_lanes;
                tuner.accepted_useful_mib_per_sec = Some(measured);
                tuner.accepted_rate_source = Some(rate_source);
                tuner.accepted_device_mib_per_sec = device;
                tuner.probe_identity = None;
                clear_legacy_tuner_audit(tuner);
                tuner.phase = LegacyTunerPhase::ObserveBaseline;
                tuner.backoff_until_unix_secs = 0;
                tuner.next_probe_unix_secs = now.saturating_add(LEGACY_TUNER_PROBE_COOLDOWN_SECS);
                tuner.baseline_ready = true;
                tuner.last_decision = Some(format!(
                    "accepted {} lanes: aggregate {rate_label} +{:.1} MiB/s",
                    observation.active_lanes, gain,
                ));
            } else {
                let reason = format!(
                    "{LEGACY_TUNER_PAUSE_PREFIX} {} lanes produced {:.1} MiB/s {rate_label} vs {:.1} baseline (required +{:.1})",
                    observation.active_lanes, measured, baseline_mib_per_sec, required,
                );
                queue_legacy_tuner_action(
                    tuner,
                    LegacyTunerAction::Pause {
                        epoch: probe_epoch,
                        expected_identity: tuner.probe_identity,
                        reason: reason.clone(),
                    },
                    now,
                );
                tuner.phase = LegacyTunerPhase::VerifyPause {
                    baseline_lanes,
                    baseline_mib_per_sec,
                    probe_mib_per_sec: measured,
                    probe_epoch,
                };
                tuner.window = None;
                tuner.last_decision = Some(reason);
            }
        }
        LegacyTunerPhase::VerifyPause {
            baseline_lanes,
            baseline_mib_per_sec,
            probe_mib_per_sec,
            probe_epoch,
        } => {
            let pause_restored =
                measured >= probe_mib_per_sec * 1.03 || measured >= baseline_mib_per_sec * 0.97;
            if pause_restored {
                tuner.accepted_lanes = baseline_lanes;
                tuner.accepted_useful_mib_per_sec = Some(measured);
                tuner.accepted_rate_source = Some(rate_source);
                tuner.phase = LegacyTunerPhase::Backoff;
                tuner.backoff_until_unix_secs = now.saturating_add(LEGACY_TUNER_BACKOFF_SECS);
                tuner.baseline_ready = false;
                tuner.last_decision = Some(format!(
                    "rejected lane {} after paired pause restored {:.1} MiB/s {rate_label}; retry after {}",
                    probe_epoch, measured, tuner.backoff_until_unix_secs,
                ));
            } else {
                tuner.accepted_lanes = baseline_lanes.saturating_add(1);
                tuner.accepted_useful_mib_per_sec = Some(probe_mib_per_sec);
                tuner.accepted_rate_source = Some(rate_source);
                let expected_identity = tuner.probe_identity;
                queue_legacy_tuner_action(
                    tuner,
                    LegacyTunerAction::Resume {
                        epoch: probe_epoch,
                        expected_identity,
                        reason: format!(
                            "paired pause reduced aggregate throughput to {:.1} MiB/s; keep probe",
                            measured
                        ),
                    },
                    now,
                );
                tuner.phase = LegacyTunerPhase::ObserveBaseline;
                tuner.next_probe_unix_secs = now.saturating_add(LEGACY_TUNER_PROBE_COOLDOWN_SECS);
                tuner.baseline_ready = false;
                tuner.last_decision = Some(format!(
                    "accepted lane {} because pause reduced aggregate {rate_label}",
                    probe_epoch,
                ));
            }
        }
        LegacyTunerPhase::VerifyDownshift {
            loaded_lanes,
            loaded_mib_per_sec,
            audit_epoch,
        } => {
            clear_legacy_tuner_degradation(tuner);
            let identity = tuner
                .probe_identity
                .expect("verified downshift has an exact audit identity");
            queue_legacy_tuner_action(
                tuner,
                LegacyTunerAction::Resume {
                    epoch: audit_epoch,
                    expected_identity: Some(identity),
                    reason: "remeasure the original N-lane topology before deciding a downshift"
                        .to_string(),
                },
                now,
            );
            tuner.phase = LegacyTunerPhase::VerifyDownshiftReloaded {
                loaded_lanes,
                loaded_mib_per_sec,
                reduced_mib_per_sec: measured,
                reduced_device_mib_per_sec: device,
                audit_epoch,
            };
            tuner.window = None;
            tuner.audit_started_unix_secs = now;
            tuner.baseline_ready = false;
            tuner.last_decision = Some(format!(
                "N-1 measured {:.1} MiB/s {rate_label}; restoring {loaded_lanes} lanes for causal confirmation",
                measured,
            ));
        }
        LegacyTunerPhase::VerifyDownshiftReloaded {
            loaded_lanes,
            loaded_mib_per_sec,
            reduced_mib_per_sec,
            reduced_device_mib_per_sec,
            audit_epoch,
        } => {
            let comparison = loaded_mib_per_sec.max(measured);
            let required =
                LEGACY_TUNER_MIN_GAIN_MIB_PER_SEC.max(comparison * LEGACY_TUNER_MIN_GAIN_RATIO);
            let gain = reduced_mib_per_sec - comparison;
            let identity = tuner
                .probe_identity
                .expect("reloaded downshift has an exact audit identity");
            if gain >= required {
                let reason = format!(
                    "{LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX} N-1 produced {:.1} MiB/s vs {:.1} across both N-lane windows; committing lower topology",
                    reduced_mib_per_sec, comparison,
                );
                queue_legacy_tuner_action(
                    tuner,
                    LegacyTunerAction::Pause {
                        epoch: audit_epoch,
                        expected_identity: Some(identity),
                        reason: reason.clone(),
                    },
                    now,
                );
                tuner.phase = LegacyTunerPhase::CommitDownshift {
                    loaded_lanes,
                    reduced_mib_per_sec,
                    reduced_device_mib_per_sec,
                    audit_epoch,
                };
                tuner.window = None;
                tuner.audit_started_unix_secs = now;
                tuner.last_decision = Some(reason);
            } else {
                tuner.accepted_lanes = loaded_lanes;
                tuner.accepted_useful_mib_per_sec = Some(comparison);
                tuner.accepted_rate_source = Some(rate_source);
                tuner.accepted_device_mib_per_sec = device;
                tuner.phase = LegacyTunerPhase::ObserveBaseline;
                tuner.probe_identity = None;
                tuner.window = None;
                tuner.next_probe_unix_secs = now.saturating_add(LEGACY_TUNER_BACKOFF_SECS);
                tuner.baseline_ready = false;
                clear_legacy_tuner_audit(tuner);
                tuner.last_decision = Some(format!(
                    "retained {loaded_lanes} lanes after N→N-1→N: N-1 gain {:+.1} MiB/s, required +{:.1}",
                    gain, required,
                ));
            }
        }
        LegacyTunerPhase::CommitDownshift { .. } => {
            unreachable!("downshift commit is finalized after exact topology validation")
        }
        LegacyTunerPhase::Backoff => {}
    }
}

impl LegacyThroughputTuner {
    fn phase_matches_backoff(&self) -> bool {
        matches!(self.phase, LegacyTunerPhase::Backoff)
    }

    fn phase_name(&self) -> &'static str {
        match self.phase {
            LegacyTunerPhase::ObserveBaseline => "observe_baseline",
            LegacyTunerPhase::ProbeUp { .. } => "probe_up",
            LegacyTunerPhase::VerifyPause { .. } => "verify_pause",
            LegacyTunerPhase::VerifyDownshift { .. } => "verify_downshift",
            LegacyTunerPhase::VerifyDownshiftReloaded { .. } => "verify_downshift_reloaded",
            LegacyTunerPhase::CommitDownshift { .. } => "commit_downshift",
            LegacyTunerPhase::Backoff => "backoff",
        }
    }
}

fn active_tuner_probe_epoch(tuner: &LegacyThroughputTuner) -> Option<u64> {
    match tuner.phase {
        LegacyTunerPhase::ProbeUp { probe_epoch, .. }
        | LegacyTunerPhase::VerifyPause { probe_epoch, .. } => Some(probe_epoch),
        LegacyTunerPhase::VerifyDownshift { audit_epoch, .. }
        | LegacyTunerPhase::VerifyDownshiftReloaded { audit_epoch, .. }
        | LegacyTunerPhase::CommitDownshift { audit_epoch, .. } => Some(audit_epoch),
        LegacyTunerPhase::ObserveBaseline | LegacyTunerPhase::Backoff => None,
    }
}

fn hard_pressure_legacy_epoch(
    config: &SchedulerConfig,
    tuner: &LegacyThroughputTuner,
    controlled_running_epochs: &[u64],
) -> Option<u64> {
    active_tuner_probe_epoch(tuner)
        .filter(|epoch| controlled_running_epochs.contains(epoch))
        .or_else(|| {
            controlled_running_epochs
                .iter()
                .copied()
                .max_by_key(|epoch| historical_schedule_priority_key(config, *epoch))
        })
}

fn publish_legacy_tuner_summary(
    config: &SchedulerConfig,
    summary: &mut PipelineSummary,
    tuner: &LegacyThroughputTuner,
    target: usize,
    now: u64,
) {
    summary.legacy_compact_tuning_enabled = legacy_tuning_enabled(config);
    if !summary.legacy_compact_tuning_enabled {
        return;
    }
    summary.legacy_compact_tuning_state = Some(if tuner.guard_held {
        "resource_guard".to_string()
    } else {
        tuner.phase_name().to_string()
    });
    summary.legacy_compact_tuning_target = target;
    summary.legacy_compact_tuning_accepted_lanes = tuner.accepted_lanes;
    summary.legacy_compact_tuning_baseline_mib_per_sec = tuner.accepted_useful_mib_per_sec;
    summary.legacy_compact_tuning_objective_mib_per_sec = tuner.current_useful_mib_per_sec;
    summary.legacy_compact_tuning_rate_source = tuner
        .current_rate_source
        .map(|source| source.api_name().to_string());
    summary.legacy_compact_useful_input_mib_per_sec = (tuner.current_rate_source
        == Some(LegacyTunerRateSource::LogicalInput))
    .then_some(tuner.current_useful_mib_per_sec)
    .flatten();
    summary.legacy_compact_useful_input_active_lanes = tuner.current_active_lanes;
    summary.legacy_compact_useful_input_sampled_lanes = tuner.current_sampled_lanes;
    summary.legacy_compact_tuning_backoff_until_unix_secs =
        (tuner.backoff_until_unix_secs > now).then_some(tuner.backoff_until_unix_secs);
    summary.legacy_compact_tuning_last_decision = tuner.last_decision.clone();
}

fn legacy_pressure_state(
    config: &SchedulerConfig,
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
    let io_high = machine
        .io_pressure_full_avg10
        .is_some_and(|value| value >= config.legacy_compact_io_pause_full_avg10);
    let cpu_load_ceiling = config.legacy_compact_cpu_budget_cores as f64;
    let cpu_high = cpu_load_ceiling > 0.0 && machine.load_1m >= cpu_load_ceiling;
    if memory_low || io_high || cpu_high {
        let mut reasons = Vec::new();
        if memory_low {
            reasons.push(format!(
                "MemAvailable {:.1} MiB is below pause threshold {} MiB; SIGSTOP arrests growth but RSS remains fully reserved",
                machine.memory_available_bytes as f64 / mib as f64,
                pause_available / mib,
            ));
        }
        if io_high {
            reasons.push(format!(
                "IO PSI full avg10 {:.2} reached pause threshold {:.2}",
                machine.io_pressure_full_avg10.unwrap_or_default(),
                config.legacy_compact_io_pause_full_avg10,
            ));
        }
        if cpu_high {
            reasons.push(format!(
                "load average {:.2} reached CPU load ceiling {:.2}",
                machine.load_1m, cpu_load_ceiling,
            ));
        }
        return LegacyPressureState::Pause(reasons.join("; "));
    }

    let memory_recovered = !memory_known || machine.memory_available_bytes >= resume_available;
    // PSI absence is unknown telemetry, not a reason to hold a safe resume.
    let io_recovered = machine
        .io_pressure_full_avg10
        .is_none_or(|value| value <= config.legacy_compact_io_resume_full_avg10);
    let cpu_recovered = cpu_load_ceiling == 0.0 || machine.load_1m <= cpu_load_ceiling * 0.85;
    if memory_recovered && io_recovered && cpu_recovered {
        LegacyPressureState::Resume
    } else {
        LegacyPressureState::Hold
    }
}

fn legacy_adaptive_cooldown_elapsed(
    config: &SchedulerConfig,
    last_action_unix_secs: u64,
    now: u64,
) -> bool {
    last_action_unix_secs == 0
        || now.saturating_sub(last_action_unix_secs)
            >= config.legacy_compact_pause_cooldown.as_secs()
}

fn plan_legacy_adaptive_action(
    config: &SchedulerConfig,
    machine: &MachineSnapshot,
    total_running: usize,
    controlled_running_epochs: &[u64],
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
            controlled_running_epochs
                .iter()
                .copied()
                .max_by_key(|epoch| historical_schedule_priority_key(config, *epoch))
                .map(|epoch| LegacyAdaptiveDecision::Pause { epoch, reason })
        }
        LegacyPressureState::Resume => auto_paused
            .iter()
            .min_by_key(|record| {
                let (band, order) = historical_schedule_priority_key(config, record.epoch);
                (
                    band,
                    if band == 0 { order } else { 0 },
                    record.paused_unix_secs,
                    record.epoch,
                )
            })
            .map(|record| LegacyAdaptiveDecision::Resume {
                epoch: record.epoch,
                reason: "MemAvailable and IO PSI crossed resume thresholds".to_string(),
            }),
        LegacyPressureState::Hold => None,
    }
}

fn record_legacy_adaptive_action(
    config: &SchedulerConfig,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyTunerActionResolution {
    Applied,
    Retryable,
    Obsolete,
}

fn legacy_tuner_action_resolution(
    config: &SchedulerConfig,
    runtime: &RuntimeState,
    action: &LegacyTunerAction,
    now: u64,
) -> LegacyTunerActionResolution {
    let timed_out = runtime
        .legacy_throughput_tuner
        .pending_action_started_unix_secs
        > 0
        && now.saturating_sub(
            runtime
                .legacy_throughput_tuner
                .pending_action_started_unix_secs,
        ) >= LEGACY_TUNER_ACTION_TIMEOUT_SECS;
    match action {
        LegacyTunerAction::Pause {
            epoch,
            expected_identity,
            ..
        } => {
            if runtime
                .paused_jobs
                .contains(&format!("compact_reuse:{epoch}"))
            {
                return LegacyTunerActionResolution::Obsolete;
            }
            if let Some(identity) = expected_identity {
                if exact_tuner_pause_owned(config, runtime, *identity) {
                    return LegacyTunerActionResolution::Applied;
                }
                if !legacy_identity_matches_controlled_lane(config, runtime, *identity)
                    || runtime.auto_paused_legacy.contains_key(epoch)
                    || timed_out
                {
                    return LegacyTunerActionResolution::Obsolete;
                }
                LegacyTunerActionResolution::Retryable
            } else if runtime
                .auto_paused_legacy
                .get(epoch)
                .is_some_and(|record| legacy_tuner_owns_pause_reason(&record.reason))
            {
                LegacyTunerActionResolution::Applied
            } else if runtime
                .legacy_compacts
                .get(epoch)
                .and_then(|child| child.pid)
                .is_some()
                && !timed_out
            {
                LegacyTunerActionResolution::Retryable
            } else {
                LegacyTunerActionResolution::Obsolete
            }
        }
        LegacyTunerAction::Resume {
            epoch,
            expected_identity,
            ..
        } => {
            if runtime
                .paused_jobs
                .contains(&format!("compact_reuse:{epoch}"))
            {
                return LegacyTunerActionResolution::Obsolete;
            }
            if let Some(identity) = expected_identity {
                if exact_tuner_pause_owned(config, runtime, *identity) {
                    // Never forget an exactly owned SIGSTOP merely because a
                    // SIGCONT retry is slow; keeping the action is safer.
                    return LegacyTunerActionResolution::Retryable;
                }
                if runtime.auto_paused_legacy.contains_key(epoch)
                    || !legacy_identity_matches_controlled_lane(config, runtime, *identity)
                {
                    return LegacyTunerActionResolution::Obsolete;
                }
                LegacyTunerActionResolution::Applied
            } else if runtime
                .auto_paused_legacy
                .get(epoch)
                .is_some_and(|record| legacy_tuner_owns_pause_reason(&record.reason))
            {
                LegacyTunerActionResolution::Retryable
            } else {
                LegacyTunerActionResolution::Applied
            }
        }
    }
}

fn adjust_legacy_workers_for_pressure(
    config: &SchedulerConfig,
    snapshot: &PipelineSnapshot,
    runtime: &mut RuntimeState,
    now: u64,
) {
    if !config.legacy_compact_auto_pause {
        return;
    }
    if let Some(action) = runtime.legacy_throughput_tuner.pending_action.clone() {
        match legacy_tuner_action_resolution(config, runtime, &action, now) {
            LegacyTunerActionResolution::Applied => {
                runtime.legacy_throughput_tuner.pending_action = None;
                runtime
                    .legacy_throughput_tuner
                    .pending_action_started_unix_secs = 0;
                return;
            }
            LegacyTunerActionResolution::Obsolete => {
                let epoch = match action {
                    LegacyTunerAction::Pause { epoch, .. }
                    | LegacyTunerAction::Resume { epoch, .. } => epoch,
                };
                cancel_legacy_tuner_experiment(
                    &mut runtime.legacy_throughput_tuner,
                    now,
                    format!(
                        "cancelled stale topology action for compact_reuse:{epoch}; preserving the proven worker target"
                    ),
                );
                return;
            }
            LegacyTunerActionResolution::Retryable => {
                let pending_since = runtime
                    .legacy_throughput_tuner
                    .pending_action_started_unix_secs;
                if pending_since > 0
                    && now.saturating_sub(pending_since) >= LEGACY_TUNER_ACTION_TIMEOUT_SECS
                    && let LegacyTunerAction::Resume { epoch, .. } = &action
                {
                    runtime.legacy_throughput_tuner.last_decision = Some(format!(
                        "retrying exact resume for compact_reuse:{epoch}; replacement starts remain blocked until SIGCONT succeeds"
                    ));
                }
            }
        }
    }
    let pressure = legacy_pressure_state(config, &snapshot.machine);
    // A throughput minimum is a target, never permission to violate a hard
    // memory or I/O guard. Finalizer-overlap lanes are opportunistic as well.
    let mut adaptive_config = config.clone();
    if matches!(pressure, LegacyPressureState::Pause(_))
        || legacy_compact_exclusive_hold_reason(runtime, &snapshot.epochs, &snapshot.live).is_some()
    {
        adaptive_config.legacy_compact_min_running = 0;
    }
    let total_running = snapshot
        .lanes
        .iter()
        .filter(|lane| lane.kind == "historical_compact_reuse" && lane.state.as_str() != "paused")
        .count();
    let controlled_running_epochs = runtime
        .legacy_compacts
        .iter()
        .filter_map(|(epoch, child)| {
            let key = format!("compact_reuse:{epoch}");
            (!runtime.paused_jobs.contains(&key)
                && !runtime.auto_paused_legacy.contains_key(epoch)
                && child
                    .pid
                    .is_some_and(|pid| controlled_legacy_pid(config, runtime, *epoch) == Some(pid)))
            .then_some(*epoch)
        })
        .chain(
            runtime
                .adopted_legacy_compacts
                .iter()
                .filter_map(|(epoch, adopted)| {
                    let key = format!("compact_reuse:{epoch}");
                    (!runtime.paused_jobs.contains(&key)
                        && !runtime.auto_paused_legacy.contains_key(epoch)
                        && controlled_legacy_pid(config, runtime, *epoch) == Some(adopted.pid))
                    .then_some(*epoch)
                }),
        )
        .collect::<Vec<_>>();
    let ordinary_resumable = runtime
        .auto_paused_legacy
        .values()
        .filter(|record| {
            !legacy_tuner_owns_pause_reason(&record.reason)
                && !runtime
                    .paused_jobs
                    .contains(&format!("compact_reuse:{}", record.epoch))
                && controlled_legacy_pid(config, runtime, record.epoch) == Some(record.pid)
        })
        .cloned()
        .collect::<Vec<_>>();
    let tuner_decision = runtime
        .legacy_throughput_tuner
        .pending_action
        .as_ref()
        .map(|action| match action {
            LegacyTunerAction::Pause { epoch, reason, .. } => LegacyAdaptiveDecision::Pause {
                epoch: *epoch,
                reason: reason.clone(),
            },
            LegacyTunerAction::Resume { epoch, reason, .. } => LegacyAdaptiveDecision::Resume {
                epoch: *epoch,
                reason: reason.clone(),
            },
        });
    let tuner_expected_identity = runtime
        .legacy_throughput_tuner
        .pending_action
        .as_ref()
        .and_then(|action| match action {
            LegacyTunerAction::Pause {
                expected_identity, ..
            }
            | LegacyTunerAction::Resume {
                expected_identity, ..
            } => *expected_identity,
        });
    let tuner_guard_pause = runtime
        .legacy_throughput_tuner
        .pending_action
        .as_ref()
        .is_some_and(|action| {
            matches!(
                action,
                LegacyTunerAction::Pause { reason, .. }
                    if reason.starts_with(LEGACY_TUNER_GUARD_PAUSE_PREFIX)
            )
        });
    let cooldown_elapsed = legacy_adaptive_cooldown_elapsed(
        config,
        runtime.legacy_last_adaptive_action_unix_secs,
        now,
    );
    let (decision, from_tuner) = match pressure {
        // Hard pressure bypasses the anti-flapping cooldown. Stop one managed
        // lane per reconciliation pass until the guard clears; resume remains
        // hysteretic and cooldown-limited.
        LegacyPressureState::Pause(reason) => (
            hard_pressure_legacy_epoch(
                config,
                &runtime.legacy_throughput_tuner,
                &controlled_running_epochs,
            )
            .map(|epoch| LegacyAdaptiveDecision::Pause { epoch, reason }),
            false,
        ),
        LegacyPressureState::Hold => (None, false),
        LegacyPressureState::Resume
            if tuner_guard_pause || (cooldown_elapsed && tuner_decision.is_some()) =>
        {
            (tuner_decision, true)
        }
        LegacyPressureState::Resume => (
            plan_legacy_adaptive_action(
                &adaptive_config,
                &snapshot.machine,
                total_running,
                &controlled_running_epochs,
                &ordinary_resumable,
                runtime.legacy_last_adaptive_action_unix_secs,
                now,
            ),
            false,
        ),
    };
    let Some(decision) = decision else {
        return;
    };

    match decision {
        LegacyAdaptiveDecision::Pause { epoch, reason } => {
            if from_tuner
                && tuner_expected_identity.is_some_and(|identity| {
                    !legacy_identity_matches_controlled_lane(config, runtime, identity)
                })
            {
                cancel_legacy_tuner_experiment(
                    &mut runtime.legacy_throughput_tuner,
                    now,
                    format!("compact_reuse:{epoch} identity changed before tuner pause"),
                );
                return;
            }
            let Some(pid) = controlled_legacy_pid(config, runtime, epoch) else {
                return;
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
                return;
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
                return;
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
                return;
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
                return;
            }
            if from_tuner {
                runtime.legacy_throughput_tuner.pending_action = None;
                runtime
                    .legacy_throughput_tuner
                    .pending_action_started_unix_secs = 0;
            }
            if let Err(error) = append_control_event(config, "legacy_adaptive", &action) {
                record_error(
                    config,
                    runtime,
                    "legacy_auto_pause",
                    format!("append adaptive pause event: {error:#}"),
                );
            }
        }
        LegacyAdaptiveDecision::Resume { epoch, reason } => {
            if from_tuner
                && tuner_expected_identity
                    .is_some_and(|identity| !exact_tuner_pause_owned(config, runtime, identity))
            {
                cancel_legacy_tuner_experiment(
                    &mut runtime.legacy_throughput_tuner,
                    now,
                    format!("compact_reuse:{epoch} pause ownership changed before tuner resume"),
                );
                return;
            }
            let Some(record) = runtime.auto_paused_legacy.get(&epoch).cloned() else {
                return;
            };
            if runtime
                .paused_jobs
                .contains(&format!("compact_reuse:{epoch}"))
                || process_cmdline_matches_legacy_exact(config, epoch, record.pid) != Some(true)
                || !process_is_group_leader(record.pid)
            {
                return;
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
                return;
            }
            runtime.auto_paused_legacy.remove(&epoch);
            if from_tuner {
                runtime.legacy_throughput_tuner.pending_action = None;
                runtime
                    .legacy_throughput_tuner
                    .pending_action_started_unix_secs = 0;
            }
            record_legacy_adaptive_action(
                config,
                runtime,
                now,
                format!("auto-resumed compact_reuse:{epoch}: {reason}"),
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SettledTunerAdmission {
    peak_rebound_bytes: u64,
    proven_refill: bool,
}

fn settled_tuner_admission(
    config: &SchedulerConfig,
    lanes: &[LaneSnapshot],
    runtime: &RuntimeState,
    active_rss: &BTreeMap<u64, u64>,
    context: LegacyTunerContext,
    capacity_limit: usize,
) -> Option<SettledTunerAdmission> {
    if !legacy_tuning_enabled(config) {
        return None;
    }
    let tuner = &runtime.legacy_throughput_tuner;
    if tuner.context != Some(context)
        || tuner.guard_held
        || tuner.pending_action.is_some()
        || tuner.accepted_lanes == 0
        || active_rss.len() >= capacity_limit
    {
        return None;
    }
    let proven_refill = active_rss.len() < tuner.accepted_lanes;
    let controlled_probe = active_rss.len() == tuner.accepted_lanes
        && capacity_limit == tuner.accepted_lanes.saturating_add(1)
        && matches!(tuner.phase, LegacyTunerPhase::ObserveBaseline);
    if !proven_refill && !controlled_probe {
        return None;
    }
    let mut peak_rebound_bytes = 0u64;
    for (epoch, sampled_rss) in active_rss {
        let lane = lanes.iter().find(|lane| {
            lane.kind == "historical_compact_reuse"
                && lane.epoch == Some(*epoch)
                && lane.state == "running"
        })?;
        let current = lane.rss_bytes.or(lane.progress.rss_bytes)?;
        if current != *sampled_rss {
            return None;
        }
        let peak = lane.progress.peak_rss_bytes?.max(current);
        peak_rebound_bytes = peak_rebound_bytes.saturating_add(peak.saturating_sub(current));
    }
    Some(SettledTunerAdmission {
        peak_rebound_bytes,
        proven_refill,
    })
}

async fn top_up_legacy_compacts(
    config: &SchedulerConfig,
    snapshot: &PipelineSnapshot,
    runtime: &mut RuntimeState,
    capacity_limit: usize,
) -> usize {
    // A pause/resume handshake owns the topology until the exact signal has
    // either succeeded or been cancelled. Starting a replacement while a
    // tuner-owned lane is still stopped would turn a later resume into N+1
    // and invalidate the paired throughput experiment.
    if runtime.legacy_throughput_tuner.pending_action.is_some() {
        return 0;
    }
    let active_rss = active_legacy_compact_rss(snapshot, runtime);
    let mut active_epochs = active_rss.keys().copied().collect::<BTreeSet<_>>();
    let mut running_count = active_epochs
        .iter()
        .filter(|epoch| {
            !runtime.auto_paused_legacy.contains_key(epoch)
                && !runtime
                    .paused_jobs
                    .contains(&format!("compact_reuse:{epoch}"))
        })
        .count();
    let resource_capacity = legacy_compact_resource_capacity(config)
        .effective_slots
        .min(capacity_limit);
    let settled_tuner = settled_tuner_admission(
        config,
        &snapshot.lanes,
        runtime,
        &active_rss,
        legacy_tuner_context(runtime, snapshot),
        resource_capacity,
    );
    let memory_policy = settled_tuner.map_or(LegacyCompactMemoryPolicy::Conservative, |settled| {
        LegacyCompactMemoryPolicy::SettledTuner {
            peak_rebound_bytes: settled.peak_rebound_bytes,
        }
    });
    let mut admission = LegacyCompactAdmission::with_memory_policy(
        config,
        &snapshot.machine,
        &snapshot.epochs,
        &active_rss,
        memory_policy,
    );
    let now = unix_now();
    if config.legacy_compact_auto_pause
        && (!matches!(
            legacy_pressure_state(config, &snapshot.machine),
            LegacyPressureState::Resume
        ) || (!settled_tuner.is_some_and(|settled| settled.proven_refill)
            && !legacy_adaptive_cooldown_elapsed(
                config,
                runtime.legacy_last_adaptive_action_unix_secs,
                now,
            )))
    {
        return 0;
    }
    let start_limit = if config.legacy_compact_auto_pause {
        1
    } else {
        usize::MAX
    };
    let mut started = 0usize;
    for epoch in prioritized_epochs(config, &snapshot.epochs)
        .into_iter()
        .filter(|epoch| {
            legacy_compact_reuse_status(config, epoch.epoch) == LegacyCompactReuseStatus::Ready
        })
    {
        if running_count >= resource_capacity {
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
                running_count = running_count.saturating_add(1);
                admission.reserve(epoch);
                started = started.saturating_add(1);
                if config.legacy_compact_auto_pause {
                    record_legacy_adaptive_action(
                        config,
                        runtime,
                        now,
                        format!(
                            "started compact_reuse:{} after pressure recovery",
                            epoch.epoch
                        ),
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
    config: &SchedulerConfig,
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
    let adaptive_legacy_limit = legacy_tuner_capacity_limit(config, snapshot, runtime, unix_now());
    if legacy_compact_exclusive_hold_reason(runtime, &snapshot.epochs, &snapshot.live).is_some() {
        if config.legacy_compact_finalizer_overlap == 0 && !legacy_tuning_enabled(config) {
            return Ok(());
        }
        let overlap_limit = if config.legacy_compact_finalizer_overlap == 0 {
            adaptive_legacy_limit
        } else {
            adaptive_legacy_limit.min(config.legacy_compact_finalizer_overlap)
        };
        top_up_legacy_compacts(config, snapshot, runtime, overlap_limit).await;
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
    let acquisition_candidates = prioritized_epochs(config, &snapshot.epochs)
        .into_iter()
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
    let finalizer_ready_pending = snapshot
        .finalizer_queue
        .iter()
        .any(|task| finalizer_is_admissible(config, &post_legacy_machine, task));

    // Legacy lanes may run together, but remain mutually exclusive with every
    // acquisition, ordinary scan, and finalizer class. Once higher-priority
    // live/acquisition work appears, stop topping up and let the current set
    // drain so that priority work cannot be starved by a continuously refilled
    // legacy queue.
    if !active_legacy_compacts.is_empty() {
        if runtime.acquisitions.is_empty()
            && adopted_acquisitions == 0
            && adopted_active_scans == 0
            && !finalizer_ready_pending
            && !acquisition_admissible_after_legacy_drain
        {
            top_up_legacy_compacts(config, snapshot, runtime, adaptive_legacy_limit).await;
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
        if !finalizer_ready_pending
            && top_up_legacy_compacts(config, snapshot, runtime, adaptive_legacy_limit).await > 0
        {
            return Ok(());
        }
    }
    let slots = snapshot
        .summary
        .scan_capacity_admitted
        .saturating_sub(active_scans);
    let queued = prioritized_epochs(config, &snapshot.epochs)
        .into_iter()
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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

async fn spawn_car_download(config: &SchedulerConfig, epoch: u64) -> Result<Option<ManagedChild>> {
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
        LEGACY_CAR_DOWNLOAD_ARGV0.into(),
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
    config: &SchedulerConfig,
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

fn car_download_paths(config: &SchedulerConfig, epoch: u64) -> Result<(String, PathBuf, PathBuf)> {
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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

async fn spawn_historical_finalizer(config: &SchedulerConfig, epoch: u64) -> Result<ManagedChild> {
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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

fn reap_adopted_legacy_compacts(config: &SchedulerConfig, runtime: &mut RuntimeState) {
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
                clear_legacy_pause_state_after_exit(config, runtime, epoch);
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

fn reap_legacy_compacts(config: &SchedulerConfig, runtime: &mut RuntimeState) {
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
    config: &SchedulerConfig,
    runtime: &mut RuntimeState,
    epoch: u64,
) {
    let tuner_targeted_exit = runtime
        .legacy_throughput_tuner
        .pending_action
        .as_ref()
        .is_some_and(|action| match action {
            LegacyTunerAction::Pause { epoch: target, .. }
            | LegacyTunerAction::Resume { epoch: target, .. } => *target == epoch,
        })
        || active_tuner_probe_epoch(&runtime.legacy_throughput_tuner) == Some(epoch);
    if tuner_targeted_exit {
        cancel_legacy_tuner_experiment(
            &mut runtime.legacy_throughput_tuner,
            unix_now(),
            format!(
                "compact_reuse:{epoch} exited during throughput tuning; preserving the proven target and re-observing"
            ),
        );
    }
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
    config: &SchedulerConfig,
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

fn try_acquire_acquisition_lock(config: &SchedulerConfig, epoch: u64) -> Result<Option<File>> {
    try_exclusive_lock(&acquisition_lock_path(&config.state_root, epoch))
}

fn acquisition_lock_held(config: &SchedulerConfig, epoch: u64) -> bool {
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
    config: &SchedulerConfig,
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

fn read_acquisition_marker(config: &SchedulerConfig, epoch: u64) -> Option<AcquisitionMarker> {
    serde_json::from_slice(&fs::read(acquisition_marker_path(&config.state_root, epoch)).ok()?).ok()
}

fn active_acquisition_marker(config: &SchedulerConfig, epoch: u64) -> Option<AcquisitionMarker> {
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

fn acquisition_claim_active(config: &SchedulerConfig, epoch: u64) -> bool {
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
            args.iter()
                .any(|arg| *arg == LEGACY_CAR_DOWNLOAD_ARGV0.as_bytes())
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
        source_bytes_done: None,
        source_bytes_total: None,
        source_read_mib_per_sec: None,
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

impl LiveSlotRateWindow {
    fn observe(&mut self, pid: Option<u32>, sample: LiveSlotRateSample) -> Option<f64> {
        let pid_changed = self
            .pid
            .zip(pid)
            .is_some_and(|(previous, current)| previous != current);
        let counters_regressed = self.samples.back().is_some_and(|previous| {
            sample.updated_unix_secs < previous.updated_unix_secs
                || sample.last_slot < previous.last_slot
        });
        if pid_changed || counters_regressed {
            self.samples.clear();
        }
        if pid.is_some() {
            self.pid = pid;
        }

        if self
            .samples
            .back()
            .is_some_and(|previous| previous.updated_unix_secs == sample.updated_unix_secs)
        {
            self.samples.pop_back();
        }
        self.samples.push_back(sample);
        while self.samples.len() > LIVE_SLOT_RATE_MAX_SAMPLES
            || self.samples.front().is_some_and(|oldest| {
                sample
                    .updated_unix_secs
                    .saturating_sub(oldest.updated_unix_secs)
                    > LIVE_SLOT_RATE_WINDOW_SECS
            })
        {
            self.samples.pop_front();
        }

        let oldest = self.samples.front()?;
        let newest = self.samples.back()?;
        let elapsed = newest
            .updated_unix_secs
            .checked_sub(oldest.updated_unix_secs)?;
        if elapsed < LIVE_SLOT_RATE_MIN_WINDOW_SECS {
            return None;
        }
        let slots_advanced = newest.last_slot.checked_sub(oldest.last_slot)?;
        nonnegative_finite_option(Some(slots_advanced as f64 / elapsed as f64))
    }
}

fn smooth_live_slot_rates(
    captures: &mut [LiveCaptureSnapshot],
    windows: &mut BTreeMap<String, LiveSlotRateWindow>,
    now: u64,
) {
    let mut sampled_capture_ids = BTreeSet::new();
    for capture in captures
        .iter_mut()
        .filter(|capture| capture.state == LiveState::Capturing && capture.superseded_by.is_none())
    {
        let Some(updated_unix_secs) = capture
            .progress
            .updated_unix_secs
            .filter(|updated| now.saturating_sub(*updated) <= PROGRESS_STALE_SECS)
        else {
            continue;
        };
        let Some(last_slot) = capture.progress.last_slot.or(capture.last_slot) else {
            continue;
        };
        let Some(pid) = capture.progress.pid else {
            // Missing or ambiguous process identity deliberately suppresses
            // live rates. Never let an older rolling window resurrect them.
            continue;
        };
        sampled_capture_ids.insert(capture.id.clone());
        let sample = LiveSlotRateSample {
            updated_unix_secs,
            last_slot,
        };
        let Some(rate) = windows
            .entry(capture.id.clone())
            .or_default()
            .observe(Some(pid), sample)
        else {
            continue;
        };

        capture.progress.slots_per_sec = Some(rate);
        refresh_live_epoch_metrics(&mut capture.progress);
        capture.slots_per_sec = capture.progress.slots_per_sec;
        capture.eta_secs = capture.progress.eta_secs;
    }
    windows.retain(|capture_id, _| sampled_capture_ids.contains(capture_id));
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
        progress.source_read_mib_per_sec = None;
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

fn process_started_unix_secs_from_proc_stat(
    proc_stat: &str,
    process_start_ticks: u64,
    clock_ticks_per_sec: u64,
) -> Option<u64> {
    if clock_ticks_per_sec == 0 {
        return None;
    }
    let boot_unix_secs = proc_stat.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        (fields.next()? == "btime")
            .then(|| fields.next()?.parse::<u64>().ok())
            .flatten()
    })?;
    boot_unix_secs.checked_add(process_start_ticks / clock_ticks_per_sec)
}

fn process_started_unix_secs(pid: u32, expected_start_ticks: u64) -> Option<u64> {
    // Revalidate the exact identity before publishing a timestamp so a PID
    // reused between adoption/reaping and snapshotting cannot inherit the old
    // lane's tuner identity.
    let (_, observed_start_ticks) = process_stat_identity(pid)?;
    if observed_start_ticks != expected_start_ticks {
        return None;
    }
    let from_boot_ticks = fs::read_to_string("/proc/stat").ok().and_then(|proc_stat| {
        // SAFETY: sysconf only queries an immutable system configuration value.
        let clock_ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        let clock_ticks_per_sec = u64::try_from(clock_ticks_per_sec).ok()?;
        process_started_unix_secs_from_proc_stat(
            &proc_stat,
            observed_start_ticks,
            clock_ticks_per_sec,
        )
    });
    // Some NAS libc/proc combinations expose the stable PID-directory birth
    // timestamp while rejecting the sysconf conversion above. It is a valid
    // wall-clock display fallback, but the tuner independently keys on the
    // stronger kernel start-tick identity.
    let from_proc_dir = fs::metadata(Path::new("/proc").join(pid.to_string()))
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|elapsed| elapsed.as_secs());
    let started = from_boot_ticks.or(from_proc_dir)?;
    // Close the race around all timestamp reads. A reused PID must never be
    // published with the previous process's start identity.
    let (_, final_start_ticks) = process_stat_identity(pid)?;
    (final_start_ticks == expected_start_ticks).then_some(started)
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

fn expected_legacy_compact_argv(config: &SchedulerConfig, epoch: u64) -> Option<Vec<Vec<u8>>> {
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
    config: &SchedulerConfig,
    epoch: u64,
    pid: u32,
) -> Option<bool> {
    let bytes = fs::read(Path::new("/proc").join(pid.to_string()).join("cmdline")).ok()?;
    legacy_compact_argv_matches_bytes(config, epoch, &bytes)
}

fn legacy_compact_argv_matches_bytes(
    config: &SchedulerConfig,
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

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_fdinfo_position(contents: &str) -> Option<u64> {
    contents.lines().find_map(|line| {
        line.strip_prefix("pos:")?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()
    })
}

#[cfg(target_os = "linux")]
fn process_source_input_sample(
    pid: u32,
    input: &Path,
    running: bool,
    sampled_at: Instant,
) -> Option<SourceInputSample> {
    let (_, process_start_ticks) = process_stat_identity(pid)?;
    let input_metadata = fs::metadata(input).ok()?;
    let device = input_metadata.dev();
    let inode = input_metadata.ino();
    let bytes_total = input_metadata.len();
    let fd_root = Path::new("/proc").join(pid.to_string()).join("fd");
    let mut bytes_done = None;
    for entry in fs::read_dir(fd_root).ok()?.flatten() {
        let metadata = fs::metadata(entry.path()).ok();
        if !metadata.is_some_and(|metadata| metadata.dev() == device && metadata.ino() == inode) {
            continue;
        }
        let fd = entry.file_name();
        let fdinfo = Path::new("/proc")
            .join(pid.to_string())
            .join("fdinfo")
            .join(fd);
        let position = fs::read_to_string(fdinfo)
            .ok()
            .and_then(|contents| parse_fdinfo_position(&contents));
        if let Some(position) = position {
            bytes_done = Some(bytes_done.map_or(position, |current: u64| current.max(position)));
        }
    }
    // The worker may exit or the PID may be reused while descriptors are
    // inspected. Reject the sample unless its kernel start identity survived.
    if process_stat_identity(pid).map(|(_, ticks)| ticks) != Some(process_start_ticks) {
        return None;
    }
    let final_metadata = fs::metadata(input).ok()?;
    if final_metadata.dev() != device
        || final_metadata.ino() != inode
        || final_metadata.len() != bytes_total
    {
        return None;
    }
    Some(SourceInputSample {
        pid,
        process_start_ticks,
        device,
        inode,
        bytes_done: bytes_done?.min(bytes_total),
        bytes_total,
        running,
        sampled_at,
    })
}

#[cfg(not(target_os = "linux"))]
fn process_source_input_sample(
    _pid: u32,
    _input: &Path,
    _running: bool,
    _sampled_at: Instant,
) -> Option<SourceInputSample> {
    None
}

fn source_input_rate_mib(previous: &SourceInputSample, current: &SourceInputSample) -> Option<f64> {
    if !source_input_identity_matches(previous, current) {
        return None;
    }
    let elapsed = current
        .sampled_at
        .duration_since(previous.sampled_at)
        .as_secs_f64();
    if !elapsed.is_finite() || elapsed < 0.25 {
        return None;
    }
    let bytes = current.bytes_done.checked_sub(previous.bytes_done)?;
    let rate = bytes as f64 / (1024.0 * 1024.0) / elapsed;
    (rate.is_finite() && rate >= 0.0).then_some(rate)
}

fn source_input_identity_matches(
    previous: &SourceInputSample,
    current: &SourceInputSample,
) -> bool {
    previous.pid == current.pid
        && previous.process_start_ticks == current.process_start_ticks
        && previous.device == current.device
        && previous.inode == current.inode
        && previous.bytes_total == current.bytes_total
}

fn source_input_window_rate_mib(
    history: &VecDeque<SourceInputSample>,
    current: &SourceInputSample,
) -> Option<f64> {
    history
        .iter()
        .filter(|previous| source_input_identity_matches(previous, current))
        .filter_map(|previous| {
            let elapsed = current
                .sampled_at
                .duration_since(previous.sampled_at)
                .as_secs_f64();
            (elapsed >= SOURCE_READ_RATE_MIN_WINDOW_SECS
                && elapsed <= SOURCE_READ_RATE_MAX_WINDOW_SECS)
                .then_some((elapsed, previous))
        })
        .max_by(|(left, _), (right, _)| left.total_cmp(right))
        .and_then(|(_, previous)| source_input_rate_mib(previous, current))
}

fn push_source_input_sample(history: &mut VecDeque<SourceInputSample>, current: SourceInputSample) {
    if history.back().is_some_and(|previous| {
        !source_input_identity_matches(previous, &current) || previous.running != current.running
    }) {
        history.clear();
    }
    history.push_back(current.clone());
    while history.len() > SOURCE_READ_RATE_MAX_SAMPLES
        || history.front().is_some_and(|oldest| {
            current
                .sampled_at
                .duration_since(oldest.sampled_at)
                .as_secs_f64()
                > SOURCE_READ_RATE_MAX_WINDOW_SECS
        })
    {
        history.pop_front();
    }
}

fn sample_worker_disk_io(snapshot: &mut PipelineSnapshot, runtime: &mut RuntimeState) {
    let sampled_at = Instant::now();
    let historical_inputs = snapshot
        .epochs
        .iter()
        .filter_map(|epoch| {
            epoch
                .input_path
                .as_ref()
                .map(|path| (epoch.epoch, path.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let source_reader_topology = snapshot
        .lanes
        .iter()
        .filter(|lane| {
            lane.state == "running"
                && matches!(
                    lane.kind.as_str(),
                    "historical_scan" | "historical_compact_reuse"
                )
                && lane
                    .epoch
                    .is_some_and(|epoch| historical_inputs.contains_key(&epoch))
        })
        .filter_map(|lane| {
            let pid = lane.pid.or(lane.progress.pid)?;
            let (_, start_ticks) = process_stat_identity(pid)?;
            Some((lane.id.clone(), pid, start_ticks))
        })
        .collect::<BTreeSet<_>>();
    // Rates from different lane topologies are not additive. Relearn the
    // whole aggregate whenever a reader starts, exits, pauses, or resumes.
    if runtime.source_reader_topology != source_reader_topology {
        runtime.source_input_samples.clear();
        runtime.source_reader_topology = source_reader_topology;
    }
    let mut active_ids = BTreeSet::new();
    let mut source_active_ids = BTreeSet::new();
    let mut total_read = 0.0;
    let mut total_write = 0.0;
    let mut sampled_rates = 0usize;
    let mut active_roots = 0usize;
    let mut aggregate_members = BTreeSet::new();
    let mut aggregate_disjoint = true;

    for lane in &mut snapshot.lanes {
        lane.progress.source_read_mib_per_sec = None;
        lane.progress.disk_read_mib_per_sec = None;
        lane.progress.disk_write_mib_per_sec = None;
        let Some(pid) = lane.pid.or(lane.progress.pid) else {
            continue;
        };
        if matches!(
            lane.kind.as_str(),
            "historical_scan" | "historical_compact_reuse"
        ) && !matches!(
            lane.state.as_str(),
            "done" | "complete" | "failed" | "stopped"
        ) && let Some(input) = lane.epoch.and_then(|epoch| historical_inputs.get(&epoch))
        {
            source_active_ids.insert(lane.id.clone());
            if let Some(current) =
                process_source_input_sample(pid, input, lane.state == "running", sampled_at)
            {
                lane.progress.source_bytes_done = Some(current.bytes_done);
                lane.progress.source_bytes_total = Some(current.bytes_total);
                let history = runtime
                    .source_input_samples
                    .entry(lane.id.clone())
                    .or_default();
                if lane.state == "running" {
                    lane.progress.source_read_mib_per_sec =
                        source_input_window_rate_mib(history, &current);
                }
                // Paused lanes update their baseline too, preventing resume
                // from averaging a stationary pause into the next rate.
                push_source_input_sample(history, current);
            }
        }
        if lane.state != "running" {
            continue;
        }
        active_roots = active_roots.saturating_add(1);
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
    runtime
        .source_input_samples
        .retain(|id, _| source_active_ids.contains(id));
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
        car_disk_shared_with_archive: paths_share_filesystem_device(archive_path, car_path),
        archive_device_major: None,
        archive_device_minor: None,
        archive_device_name: None,
        archive_device_read_mib_per_sec: None,
        archive_device_write_mib_per_sec: None,
        load_1m,
        io_pressure_some_avg10,
        io_pressure_full_avg10,
        memory_pressure_some_avg10,
        memory_pressure_full_avg10,
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
    config: &SchedulerConfig,
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

fn car_download_part_bytes(config: &SchedulerConfig, epoch: u64) -> u64 {
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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
    config: &SchedulerConfig,
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

fn persist_acquisition_failures(config: &SchedulerConfig, runtime: &RuntimeState) -> Result<()> {
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
    config: &SchedulerConfig,
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

fn clear_runtime_failure(config: &SchedulerConfig, runtime: &mut RuntimeState, key: &str) {
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

fn acquisition_marker_artifact_valid(config: &SchedulerConfig, marker: &AcquisitionMarker) -> bool {
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

fn reconcile_acquisition_state(config: &SchedulerConfig, runtime: &mut RuntimeState) {
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
    #[serde(default)]
    legacy_tuner_profiles: LegacyTunerProfiles,
}

fn persist_control_state(config: &SchedulerConfig, runtime: &RuntimeState) -> Result<()> {
    let value = PersistedControlState {
        scheduler_paused: runtime.scheduler_paused,
        scheduler_updated_unix_secs: runtime.scheduler_updated_unix_secs,
        paused_jobs: runtime.paused_jobs.clone(),
        auto_paused_legacy: runtime.auto_paused_legacy.clone(),
        legacy_last_adaptive_action_unix_secs: runtime.legacy_last_adaptive_action_unix_secs,
        legacy_last_adaptive_action_reason: runtime.legacy_last_adaptive_action_reason.clone(),
        legacy_tuner_profiles: runtime.legacy_tuner_profiles.clone(),
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
    runtime.legacy_tuner_profiles = saved.legacy_tuner_profiles;
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
    config: &SchedulerConfig,
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

fn append_control_event(config: &SchedulerConfig, action: &str, target: &str) -> Result<()> {
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

    fn test_config(root: &Path) -> SchedulerConfig {
        SchedulerConfig {
            status_bind: "127.0.0.1:0".parse().unwrap(),
            management_bind: None,
            blockzilla_bin: root.join("blockzilla"),
            repair_blockzilla_bin: None,
            car_root: root.join("cars"),
            archive_root: root.join("archives"),
            live_root: root.join("live"),
            state_root: root.join("state"),
            scan_concurrency: 4,
            legacy_compact_concurrency: 1,
            legacy_compact_finalizer_overlap: 0,
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
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-scheduler-{label}-{}-{unique}",
            std::process::id()
        ))
    }

    #[cfg(target_os = "linux")]
    fn compile_idle_legacy_worker(root: &Path) -> PathBuf {
        fs::create_dir_all(root).unwrap();
        let source = root.join("idle-legacy-worker.c");
        let binary = root.join("idle-legacy-worker");
        fs::write(
            &source,
            b"#include <signal.h>\n#include <unistd.h>\nint main(void) { for (;;) pause(); }\n",
        )
        .unwrap();
        let compiler = std::env::var_os("CC").unwrap_or_else(|| "cc".into());
        let status = std::process::Command::new(compiler)
            .arg("-O0")
            .arg("-o")
            .arg(&binary)
            .arg(&source)
            .status()
            .unwrap();
        assert!(status.success(), "compile Linux idle worker fixture");
        binary
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
        config: &SchedulerConfig,
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

    #[allow(dead_code)]
    fn write_test_repair_compacted_legacy_fixture(
        config: &SchedulerConfig,
        bundle: &Path,
        epoch: u64,
    ) -> PathBuf {
        let output = config.archive_root.join(format!("epoch-{epoch}"));
        fs::create_dir_all(output.join("repair")).unwrap();
        write_valid_hot_index(&output, 3, 16);
        write_structural_registry_and_index(&output, 1);
        for (name, bytes) in [
            (META_FILE, b"meta".as_slice()),
            (REGISTRY_COUNTS_FILE, b"counts".as_slice()),
            (VOTE_HASH_REGISTRY_FILE, b"votes".as_slice()),
            ("repair/available-poh.wincode", b"partial-poh".as_slice()),
        ] {
            fs::write(output.join(name), bytes).unwrap();
        }
        fs::write(output.join(SIGNATURES_FILE), vec![0u8; 5 * 64]).unwrap();
        fs::write(output.join(BLOCKHASH_REGISTRY_FILE), vec![0u8; 3 * 32]).unwrap();

        let source_manifest_sha256 =
            sha256_file_hex(&bundle.join(LIVE_REPAIR_REQUIRED_MARKER)).unwrap();
        let source_merge_plan_sha256 =
            sha256_file_hex(&bundle.join(LIVE_REPAIR_PLAN_FILE)).unwrap();
        let source_marker = serde_json::json!({
            "version": 1,
            "state": "repair_materialized_missing_poh_and_shredding",
            "canonical": false,
            "publication_ready": false,
            "epoch": epoch,
            "epoch_start_slot": epoch * SLOTS_PER_EPOCH,
            "epoch_end_slot": (epoch + 1) * SLOTS_PER_EPOCH - 1,
            "live_blocks": 2,
            "rpc_only_blocks": 1,
            "produced_blocks": 3,
            "manifest_sha256": source_manifest_sha256,
            "merge_plan_sha256": source_merge_plan_sha256,
        });
        let source_marker_path = output.join(LIVE_REPAIR_SOURCE_MATERIALIZED_MARKER);
        fs::write(
            &source_marker_path,
            serde_json::to_vec(&source_marker).unwrap(),
        )
        .unwrap();
        let source_materialized_marker_sha256 = sha256_file_hex(&source_marker_path).unwrap();
        let marker = serde_json::json!({
            "version": 1,
            "state": "degraded_hot_archive_missing_poh_and_shredding",
            "canonical": false,
            "publication_ready": false,
            "block_archive_ready": true,
            "block_access_ready": false,
            "epoch": epoch,
            "epoch_start_slot": epoch * SLOTS_PER_EPOCH,
            "epoch_end_slot": (epoch + 1) * SLOTS_PER_EPOCH - 1,
            "live_blocks": 2,
            "rpc_only_blocks": 1,
            "produced_blocks": 3,
            "transactions": 5,
            "signatures": 5,
            "zstd_level": 1,
            "compressed_bytes": 16,
            "uncompressed_bytes": 32,
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
                "available_poh": "repair/available-poh.wincode"
            },
            "poh_coverage": {
                "available_records": 2,
                "available_entries": 10,
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
            "source_materialized_marker_sha256": source_materialized_marker_sha256,
            "source_manifest_sha256": source_manifest_sha256,
            "source_merge_plan_sha256": source_merge_plan_sha256,
            "limitations": ["noncanonical test fixture"]
        });
        fs::write(
            output.join(LIVE_REPAIR_COMPACTED_MARKER),
            serde_json::to_vec(&marker).unwrap(),
        )
        .unwrap();
        output
    }

    fn write_test_repair_compacted(config: &SchedulerConfig, bundle: &Path, epoch: u64) -> PathBuf {
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

        write_structural_registry_and_index(&output, 1);
        fs::write(output.join(REGISTRY_COUNTS_FILE), [1]).unwrap();
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

        let source_manifest_sha256 =
            sha256_file_hex(&bundle.join(LIVE_REPAIR_REQUIRED_MARKER)).unwrap();
        let source_merge_plan_sha256 =
            sha256_file_hex(&bundle.join(LIVE_REPAIR_PLAN_FILE)).unwrap();
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
            "manifest_sha256": source_manifest_sha256,
            "merge_plan_sha256": source_merge_plan_sha256
        });
        let source_marker_path = output.join(LIVE_REPAIR_SOURCE_MATERIALIZED_MARKER);
        fs::write(
            &source_marker_path,
            serde_json::to_vec(&source_marker).unwrap(),
        )
        .unwrap();
        let source_marker_sha = sha256_file_hex(&source_marker_path).unwrap();
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
            "source_manifest_sha256": source_manifest_sha256,
            "source_merge_plan_sha256": source_merge_plan_sha256,
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
        assert_eq!(bundle.progress.progress_pct, Some(100.0));
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
        let root = temp_root("repair-compacted-valid");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-repair-view", 1000, &["source"]);
        write_test_repair_compacted(&config, &bundle_path, 1000);

        let bundle =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(bundle.state, LiveState::Packaged);
        assert_eq!(bundle.progress.state.as_deref(), Some("packaged_degraded"));
        assert!(bundle.message.as_deref().unwrap().contains("degraded hot"));
        let mut captures = vec![
            test_live_capture(&root, "source", 1000, LiveState::Blocked, 1),
            bundle,
        ];
        apply_repair_supersession(&mut captures);
        assert_eq!(
            captures[0].superseded_by.as_deref(),
            Some("epoch-1000-repair-view")
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
        let output = write_test_repair_compacted(&config, &bundle_path, 1000);
        add_test_repair_block_access(&output, 1000);

        let packaged =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(packaged.state, LiveState::Packaged);
        let message = packaged.message.as_deref().unwrap();
        assert!(message.contains("with block access"));
        assert!(message.contains("canonical PoH and shredding sidecars"));
        assert!(!message.contains("block-access sidecars remain"));
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
            let output = write_test_repair_compacted(&config, &bundle_path, 1000);
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
            let output = write_test_repair_compacted(&config, &bundle_path, 1000);
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
        let output = write_test_repair_compacted(&config, &bundle_path, 1000);
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
        let root = temp_root("repair-compacted-corrupt-or-canonical");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-union-repair-view", 1000, &["source"]);
        let output = write_test_repair_compacted(&config, &bundle_path, 1000);
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
    fn repair_compacted_poh_gap_declaration_must_match_available_records() {
        let root = temp_root("repair-compacted-poh-gaps");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-union-repair-view", 1000, &["source"]);
        let output = write_test_repair_compacted(&config, &bundle_path, 1000);
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
    fn invalid_degraded_repair_compaction_stays_repair_required() {
        let root = temp_root("repair-compacted-invalid");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-repair-view", 1000, &["source"]);
        let output = write_test_repair_compacted(&config, &bundle_path, 1000);
        fs::write(output.join(POH_FILE), b"forbidden").unwrap();

        let bundle =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(bundle.state, LiveState::RepairRequired);
        assert_eq!(bundle.source_capture_ids, vec!["source".to_string()]);
        assert!(
            bundle
                .message
                .as_deref()
                .unwrap()
                .contains("invalid REPAIR-COMPACTED")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn symlinked_repair_compacted_marker_fails_closed_without_losing_bundle_relationships() {
        let root = temp_root("repair-compacted-symlink");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let bundle_path =
            write_test_repair_bundle(&config, "epoch-1000-repair-view", 1000, &["source"]);
        let output = write_test_repair_compacted(&config, &bundle_path, 1000);
        let marker_path = output.join(LIVE_REPAIR_COMPACTED_MARKER);
        let external = root.join("external-compacted.json");
        fs::rename(&marker_path, &external).unwrap();
        std::os::unix::fs::symlink(&external, &marker_path).unwrap();

        let bundle =
            classify_live_capture(&config, &RuntimeState::default(), bundle_path, unix_now());
        assert_eq!(bundle.state, LiveState::RepairRequired);
        assert_eq!(bundle.source_capture_ids, vec!["source".to_string()]);
        assert!(bundle.message.as_deref().unwrap().contains("regular file"));
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
        let bin = root.join("repair-blockzilla");
        config.repair_blockzilla_bin = Some(bin.clone());
        assert_eq!(effective_repair_blockzilla_bin(&config), bin.as_path());
        let capture = root.join("live/epoch-1000-repair-view");
        let materialized = repair_materialized_output(&config, 1000);
        let output = root.join("archives/epoch-1000");
        let materialize = [
            bin.as_os_str().as_bytes(),
            b"materialize-archive-v2-live-repair",
            capture.as_os_str().as_bytes(),
            materialized.as_os_str().as_bytes(),
        ]
        .into_iter()
        .flat_map(|arg| arg.iter().copied().chain(std::iter::once(0)))
        .collect::<Vec<_>>();
        assert!(repair_argv_matches_bytes(
            &materialize,
            &bin,
            RepairProcessKind::Materialize,
            &capture,
            &materialized,
            &output,
        ));
        assert!(!repair_argv_matches_bytes(
            &materialize,
            &config.blockzilla_bin,
            RepairProcessKind::Materialize,
            &capture,
            &materialized,
            &output,
        ));
        let mut wrong = materialize.clone();
        wrong.extend_from_slice(b"--wrong\0");
        assert!(!repair_argv_matches_bytes(
            &wrong,
            &bin,
            RepairProcessKind::Materialize,
            &capture,
            &materialized,
            &output,
        ));
        let wrong_capture = root.join("live/wrong");
        assert!(!repair_argv_matches_bytes(
            &materialize,
            &bin,
            RepairProcessKind::Materialize,
            &wrong_capture,
            &materialized,
            &output,
        ));
    }

    #[test]
    fn repair_digest_cache_invalidates_on_atomic_source_replacement() {
        let root = temp_root("repair-digest-cache");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("manifest.json");
        fs::write(&path, b"first").unwrap();
        let first = cached_repair_sha256_file_hex(&path).unwrap();
        assert_eq!(cached_repair_sha256_file_hex(&path).unwrap(), first);

        let replacement = root.join("replacement.json");
        fs::write(&replacement, b"other").unwrap();
        fs::rename(&replacement, &path).unwrap();
        let second = cached_repair_sha256_file_hex(&path).unwrap();
        assert_ne!(first, second);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn epoch_scoped_capture_name_wins_over_next_epoch_tail_slot() {
        let root = temp_root("live-epoch-tail");
        let config = test_config(&root);
        fs::create_dir_all(&config.live_root).unwrap();
        let capture = config.live_root.join("epoch-1000-capture-tail");
        fs::create_dir_all(&capture).unwrap();
        fs::write(capture.join(LIVE_FINALIZE_MARKER), b"closed").unwrap();
        fs::write(
            capture.join("progress.json"),
            format!(
                "{{\"state\":\"closed\",\"first_slot\":{},\"last_slot\":{},\"updated_unix_secs\":1}}",
                1000 * SLOTS_PER_EPOCH,
                1001 * SLOTS_PER_EPOCH
            ),
        )
        .unwrap();
        let classified =
            classify_live_capture(&config, &RuntimeState::default(), capture, unix_now());
        assert_eq!(classified.epoch, Some(1000));
        assert_eq!(classified.state, LiveState::RepairGate);
        fs::remove_dir_all(root).unwrap();
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
    fn superseded_ready_source_is_not_queued() {
        let root = temp_root("repair-superseded-queue");
        let config = test_config(&root);
        let mut source = test_live_capture(&root, "source", 1000, LiveState::ReadyToPackage, 1);
        source.superseded_by = Some("repair".to_string());
        assert!(live_finalizer_queue_item(&config, &source).is_none());
        fs::remove_dir_all(root).ok();
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

    async fn make_running_child(
        kind: ChildKind,
        progress_path: PathBuf,
        log_path: PathBuf,
    ) -> ManagedChild {
        let child = Command::new("sleep").arg("60").spawn().unwrap();
        let pid = child.id();
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

    #[test]
    fn priority_band_orders_candidates_without_reordering_inventory() {
        let root = Path::new("/test/blockzilla-scheduler-priority-order");
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
    fn proc_start_ticks_convert_to_stable_unix_seconds() {
        let proc_stat = "cpu  1 2 3 4\nprocesses 99\nbtime 1700000000\n";
        assert_eq!(
            process_started_unix_secs_from_proc_stat(proc_stat, 250, 100),
            Some(1_700_000_002)
        );
        assert_eq!(
            process_started_unix_secs_from_proc_stat(proc_stat, 299, 100),
            Some(1_700_000_002),
            "sub-second ticks must floor to the same stable Unix second"
        );
        assert_eq!(
            process_started_unix_secs_from_proc_stat(proc_stat, 250, 0),
            None
        );
        assert_eq!(
            process_started_unix_secs_from_proc_stat("btime nope\n", 250, 100),
            None
        );
        assert_eq!(
            process_started_unix_secs_from_proc_stat(&format!("btime {}\n", u64::MAX), 100, 100,),
            None,
            "overflowing boot-time arithmetic must fail closed"
        );
    }

    #[test]
    fn tuner_identity_prefers_namespaced_linux_start_ticks() {
        assert_eq!(
            legacy_lane_start_identity(Some(77), Some(77)),
            Some(LegacyLaneStartIdentity::LinuxStartTicks(77))
        );
        #[cfg(not(target_os = "linux"))]
        assert_eq!(
            legacy_lane_start_identity(Some(77), None),
            Some(LegacyLaneStartIdentity::UnixSecs(77))
        );
        #[cfg(target_os = "linux")]
        assert_eq!(legacy_lane_start_identity(Some(77), None), None);
        assert_ne!(
            LegacyLaneStartIdentity::LinuxStartTicks(77),
            LegacyLaneStartIdentity::UnixSecs(77),
            "clock domains must not collide even when their numeric values do"
        );
        assert_eq!(legacy_lane_start_identity(None, None), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn current_process_start_time_is_stable_and_not_in_the_future() {
        let pid = std::process::id();
        let (_, start_ticks) = process_stat_identity(pid).unwrap();
        let first = process_started_unix_secs(pid, start_ticks).unwrap();
        let second = process_started_unix_secs(pid, start_ticks).unwrap();
        assert_eq!(first, second);
        assert!(first <= unix_now());
        assert_eq!(
            process_started_unix_secs(pid, start_ticks.saturating_add(1)),
            None,
            "a mismatched tracked start tick must never publish an identity"
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
    fn source_fdinfo_and_rolling_rate_stay_in_one_source_identity() {
        assert_eq!(
            parse_fdinfo_position("pos:\t12345\nflags:\t02100000\n"),
            Some(12_345)
        );
        assert_eq!(parse_fdinfo_position("flags:\t02100000\n"), None);
        assert_eq!(parse_fdinfo_position("pos:\tnot-a-number\n"), None);

        let mib = 1024 * 1024;
        let started = Instant::now();
        let sample = |done_mib: u64, after_secs: u64, running: bool| SourceInputSample {
            pid: 7,
            process_start_ticks: 11,
            device: 22,
            inode: 33,
            bytes_done: done_mib * mib,
            bytes_total: 1_000 * mib,
            running,
            sampled_at: started + Duration::from_secs(after_secs),
        };
        let mut history = VecDeque::new();
        push_source_input_sample(&mut history, sample(0, 0, true));
        let early = sample(100, 10, true);
        assert_eq!(source_input_window_rate_mib(&history, &early), None);
        push_source_input_sample(&mut history, early);
        let settled = sample(150, 15, true);
        assert_eq!(source_input_window_rate_mib(&history, &settled), Some(10.0));
        push_source_input_sample(&mut history, settled);

        let mut replacement = sample(160, 20, true);
        replacement.inode = 44;
        push_source_input_sample(&mut history, replacement);
        assert_eq!(history.len(), 1, "source replacement resets the window");
        push_source_input_sample(&mut history, sample(160, 25, false));
        assert_eq!(history.len(), 1, "pause transition resets the window");
        push_source_input_sample(&mut history, sample(160, 30, true));
        assert_eq!(history.len(), 1, "resume transition resets the window");

        let previous = sample(200, 40, true);
        let regressed = sample(100, 60, true);
        assert_eq!(source_input_rate_mib(&previous, &regressed), None);
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

    fn queue_lane(
        epoch: u64,
        state: &str,
        source_bytes_done: u64,
        source_bytes_total: u64,
        source_read_mib_per_sec: Option<f64>,
    ) -> LaneSnapshot {
        LaneSnapshot {
            id: format!("compact_reuse:{epoch}"),
            kind: LEGACY_COMPACT_OWNERSHIP_KIND.to_string(),
            epoch: Some(epoch),
            capture_id: None,
            phase: "compact_reuse".to_string(),
            state: state.to_string(),
            auto_paused: false,
            auto_pause_reason: None,
            pid: None,
            progress: ProgressSnapshot {
                source_bytes_done: Some(source_bytes_done),
                source_bytes_total: Some(source_bytes_total),
                source_read_mib_per_sec,
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
            auto_paused: false,
            auto_pause_reason: None,
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
            auto_paused: false,
            auto_pause_reason: None,
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
            auto_paused: false,
            auto_pause_reason: None,
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
            auto_paused: false,
            auto_pause_reason: None,
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
            publication: Mutex::new(PublicationState::default()),
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
        assert_eq!(receiver.recv().await.unwrap().sequence, 1);
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
            auto_paused: false,
            auto_pause_reason: None,
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
            auto_paused: false,
            auto_pause_reason: None,
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
            auto_paused: false,
            auto_pause_reason: None,
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
            auto_paused: false,
            auto_pause_reason: None,
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
        assert_eq!(snapshot.lanes[0].rss_bytes, Some(2_000));
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

        let mut restarted_next = next.clone();
        restarted_next.live[0].progress.pid = Some(20);
        restarted_next.live[0].progress.blocks_done = 20;
        restarted_next.live[0].progress.updated_unix_secs = Some(102);
        restarted_next.live[0].blocks_written = 20;
        let before = restarted_next.live[0].clone();
        let mut old_published = published;
        old_published.now_unix_secs = 103;
        old_published.live[0].progress.updated_unix_secs = Some(103);
        preserve_newer_published_progress(&mut restarted_next, &old_published);
        assert_eq!(restarted_next.now_unix_secs, 103);
        assert_eq!(restarted_next.live[0].progress, before.progress);
    }

    #[test]
    fn runnable_queue_eta_is_only_remaining_source_bytes_over_aggregate_read_rate() {
        let root = temp_root("runnable-queue-eta");
        let mib = 1024 * 1024;
        let mut queued_two = test_epoch(&root, 2, HistoricalState::Queued);
        queued_two.car_bytes = 1_000 * mib;
        let mut queued_three = test_epoch(&root, 3, HistoricalState::Queued);
        queued_three.car_bytes = 2_000 * mib;
        let epochs = vec![
            test_epoch(&root, 1, HistoricalState::Complete),
            queued_two,
            queued_three,
            test_epoch(&root, 4, HistoricalState::Scanning),
            test_epoch(&root, 5, HistoricalState::Scanning),
            test_epoch(&root, 6, HistoricalState::Blocked),
            test_epoch(&root, 7, HistoricalState::Failed),
        ];
        let lanes = vec![
            queue_lane(4, "running", 400 * mib, 1_000 * mib, Some(60.0)),
            queue_lane(5, "running", 200 * mib, 1_000 * mib, Some(40.0)),
        ];

        let eta = estimate_runnable_queue_eta(&epochs, &lanes);
        assert_eq!(eta.eta_secs, Some(44.0));
        assert_eq!(eta.jobs_remaining, 4);
        assert_eq!(eta.capacity, 2);
        assert_eq!(eta.bytes_remaining, 4_400 * mib);
        assert_eq!(eta.read_mib_per_sec, Some(100.0));
        assert!(eta.reason.as_deref().unwrap().contains("2 action-required"));
        assert!(eta.reason.as_deref().unwrap().contains("bytes/rate only"));

        let empty =
            estimate_runnable_queue_eta(&[test_epoch(&root, 8, HistoricalState::Blocked)], &[]);
        assert_eq!(empty.eta_secs, Some(0.0));
        assert_eq!(empty.jobs_remaining, 0);

        let mut partial = lanes.clone();
        partial[1].progress.source_read_mib_per_sec = None;
        let learning = estimate_runnable_queue_eta(&epochs, &partial);
        assert_eq!(learning.eta_secs, None);
        assert!(learning.reason.as_deref().unwrap().contains("1/2"));

        let mut stalled = lanes.clone();
        for lane in &mut stalled {
            lane.progress.source_read_mib_per_sec = Some(0.0);
        }
        assert_eq!(
            estimate_runnable_queue_eta(&epochs, &stalled).eta_secs,
            None,
            "zero aggregate read speed must never divide the byte backlog"
        );

        let mut post_read_lanes = lanes.clone();
        post_read_lanes.push(queue_lane(10, "running", 1_000 * mib, 1_000 * mib, None));
        let mut post_read_epochs = epochs.clone();
        post_read_epochs.push(test_epoch(&root, 10, HistoricalState::Scanning));
        let post_read = estimate_runnable_queue_eta(&post_read_epochs, &post_read_lanes);
        assert_eq!(post_read.eta_secs, Some(44.0));
        assert_eq!(post_read.active_readers, 2);
        assert_eq!(post_read.sampled_readers, 2);

        let mut with_pause = lanes;
        with_pause.push(queue_lane(9, "paused", 200 * mib, 1_000 * mib, None));
        let mut paused_epoch = test_epoch(&root, 9, HistoricalState::Scanning);
        paused_epoch.car_bytes = 1_000 * mib;
        let mut paused_epochs = epochs;
        paused_epochs.push(paused_epoch);
        let paused = estimate_runnable_queue_eta(&paused_epochs, &with_pause);
        assert_eq!(paused.eta_secs, Some(52.0));
        assert_eq!(paused.read_mib_per_sec, Some(100.0));
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

    fn make_legacy_range_ready(config: &SchedulerConfig, epoch: u64) {
        fs::create_dir_all(&config.car_root).unwrap();
        fs::write(config.car_root.join(format!("epoch-{epoch}.car")), b"car").unwrap();
        let predecessor = config.archive_root.join(format!("epoch-{}", epoch - 1));
        write_legacy_registry_sidecars(&predecessor, false);
        write_legacy_reader_core(&predecessor);
        write_legacy_registry_sidecars(&config.archive_root.join(format!("epoch-{epoch}")), false);
    }

    fn legacy_scheduler_machine(
        config: &SchedulerConfig,
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

    fn allow_legacy_workers(config: &mut SchedulerConfig, workers: usize) {
        config.legacy_compact_concurrency = workers;
        config.legacy_compact_cpu_cores_per_worker = 1;
        config.legacy_compact_cpu_budget_cores = workers as u64;
        config.legacy_compact_io_mib_per_sec_per_worker = 120;
        config.legacy_compact_io_budget_mib_per_sec = (workers as u64).saturating_mul(120);
    }

    fn useful_throughput_lane(
        epoch: u64,
        pid: u32,
        started_unix_secs: u64,
        input_mib_per_sec: f64,
        now: u64,
    ) -> LaneSnapshot {
        #[cfg(target_os = "linux")]
        let pid = {
            let _ = pid;
            // Synthetic tuner lanes use a guaranteed live /proc identity.
            // Tests that exercise a managed child overwrite this with its
            // real PID explicitly.
            std::process::id()
        };
        LaneSnapshot {
            id: format!("compact_reuse:{epoch}"),
            kind: LEGACY_COMPACT_OWNERSHIP_KIND.to_string(),
            epoch: Some(epoch),
            capture_id: None,
            phase: "compact_reuse".to_string(),
            state: "running".to_string(),
            auto_paused: false,
            auto_pause_reason: None,
            pid: Some(pid),
            progress: ProgressSnapshot {
                input_mib_per_sec: Some(input_mib_per_sec),
                updated_unix_secs: Some(now),
                ..ProgressSnapshot::default()
            },
            rss_bytes: Some(512 * 1024 * 1024),
            started_unix_secs: Some(started_unix_secs),
            updated_unix_secs: now,
        }
    }

    fn settled_tuner_window(
        snapshot: &PipelineSnapshot,
        now: u64,
        useful_mib_per_sec: f64,
    ) -> LegacyTunerWindow {
        let observation = legacy_throughput_observation(snapshot, now);
        LegacyTunerWindow {
            identities: observation.identities,
            started_unix_secs: now.saturating_sub(LEGACY_TUNER_SETTLE_SECS),
            useful_samples: (0..LEGACY_TUNER_MIN_SAMPLES)
                .map(|_| useful_mib_per_sec)
                .collect(),
            device_samples: VecDeque::new(),
        }
    }

    fn write_adopted_legacy_proof(
        config: &SchedulerConfig,
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

    fn test_app_state(config: SchedulerConfig) -> Arc<AppState> {
        let (updates, _) = broadcast::channel(4);
        Arc::new(AppState {
            config,
            snapshot: RwLock::new(empty_snapshot(false)),
            updates,
            sequence: AtomicU64::new(0),
            publication: Mutex::new(PublicationState::default()),
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

        // Timestamp/gap artifacts are derived from the immutable CAR and are
        // invalidated before compact/reuse publishes replacements. The gap
        // extractor intentionally leaves its regular lock file in place.
        fs::write(output.join(BLOCK_TIME_GAP_FILE), b"derived").unwrap();
        fs::write(output.join(BLOCK_TIME_GAP_LOCK_FILE), b"").unwrap();
        assert!(legacy_registry_only_shape(&output));

        // Keep interrupted standalone publications visible to an operator;
        // only the two canonical derived paths above are migration metadata.
        let unexpected_gap_temp = output.join(".block-time-gaps.bin.123.456.tmp");
        fs::write(&unexpected_gap_temp, b"partial").unwrap();
        assert!(!legacy_registry_only_shape(&output));
        fs::remove_file(unexpected_gap_temp).unwrap();
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
    fn legacy_owned_retry_shape_accepts_known_timestamp_build_residue_only() {
        let root = temp_root("legacy-compact-retry-timestamps");
        let config = test_config(&root);
        let output = config.archive_root.join("epoch-700");
        write_legacy_registry_sidecars(&output, true);
        write_ownership(
            &output,
            LEGACY_COMPACT_OWNERSHIP_KIND,
            "700",
            "retry_ready",
            None,
        )
        .unwrap();
        for name in [
            BLOCKHASH_INDEX_V3_TMP_FILE,
            BLOCK_TIME_GAP_FILE,
            BLOCK_TIME_GAP_LOCK_FILE,
            META_FILE,
            BLOCKS_FILE,
            "archive-v2-blocks.index.tmp",
        ] {
            fs::write(output.join(name), b"known residue").unwrap();
        }
        assert!(legacy_owned_retry_shape(&output));

        fs::write(output.join("block-time-gaps.bin.tmp"), b"unknown").unwrap();
        assert!(!legacy_owned_retry_shape(&output));
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
    fn live_slot_rate_window_is_time_weighted_bounded_and_resets() {
        let sample = |updated_unix_secs, last_slot| LiveSlotRateSample {
            updated_unix_secs,
            last_slot,
        };
        let mut window = LiveSlotRateWindow::default();
        assert_eq!(window.observe(Some(7), sample(100, 1_000)), None);
        assert_eq!(window.observe(Some(7), sample(110, 1_030)), None);

        let rate = window.observe(Some(7), sample(125, 1_060)).unwrap();
        assert!((rate - 2.4).abs() < 1e-9, "rate={rate}");

        let bounded_rate = window.observe(Some(7), sample(161, 1_146)).unwrap();
        assert_eq!(window.samples.front().unwrap().updated_unix_secs, 110);
        assert!((bounded_rate - (116.0 / 51.0)).abs() < 1e-9);

        assert_eq!(window.observe(Some(8), sample(162, 1_148)), None);
        assert_eq!(window.samples.len(), 1, "PID change resets the window");
        assert_eq!(window.observe(Some(8), sample(163, 900)), None);
        assert_eq!(window.samples.len(), 1, "slot regression resets the window");
    }

    #[test]
    fn live_progress_ignores_only_derived_rate_changes_at_the_same_cursor() {
        let current = ProgressSnapshot {
            pid: Some(7),
            blocks_done: 100,
            last_slot: Some(1_000),
            slots_per_sec: Some(2.4),
            eta_secs: Some(100.0),
            updated_unix_secs: Some(1_000),
            ..ProgressSnapshot::default()
        };
        let mut raw = current.clone();
        raw.slots_per_sec = Some(3.0);
        raw.eta_secs = Some(80.0);
        assert!(!live_progress_source_changed(&current, &raw, 1_000));

        raw.rss_bytes = Some(1024);
        assert!(live_progress_source_changed(&current, &raw, 1_000));
        raw.rss_bytes = None;
        raw.last_slot = Some(1_001);
        assert!(live_progress_source_changed(&current, &raw, 1_000));

        let mut stale = current.clone();
        stale.slots_per_sec = None;
        stale.eta_secs = None;
        assert!(live_progress_source_changed(
            &current,
            &stale,
            1_000 + PROGRESS_STALE_SECS + 1
        ));
    }

    #[test]
    fn live_slot_rate_window_updates_rate_and_eta_together() {
        let root = temp_root("live-slot-rate-window");
        let mut capture =
            test_live_capture(&root, "capture-test", 1_001, LiveState::Capturing, 1_000);
        let epoch_start = 1_001 * SLOTS_PER_EPOCH;
        capture.progress = ProgressSnapshot {
            pid: Some(7),
            state: Some("capturing".to_string()),
            first_slot: Some(epoch_start),
            last_slot: Some(epoch_start + 100),
            slots_per_sec: Some(3.0),
            updated_unix_secs: Some(1_000),
            ..ProgressSnapshot::default()
        };
        capture.last_slot = capture.progress.last_slot;
        capture.slots_per_sec = capture.progress.slots_per_sec;
        let mut windows = BTreeMap::new();
        smooth_live_slot_rates(std::slice::from_mut(&mut capture), &mut windows, 1_000);
        assert_eq!(
            capture.slots_per_sec,
            Some(3.0),
            "raw rate is kept during warm-up"
        );

        capture.progress.last_slot = Some(epoch_start + 136);
        capture.progress.slots_per_sec = Some(2.0);
        capture.progress.updated_unix_secs = Some(1_015);
        capture.last_slot = capture.progress.last_slot;
        smooth_live_slot_rates(std::slice::from_mut(&mut capture), &mut windows, 1_015);

        let rate = capture.slots_per_sec.unwrap();
        assert!((rate - 2.4).abs() < 1e-9);
        assert_eq!(capture.progress.slots_per_sec, capture.slots_per_sec);
        assert_eq!(capture.progress.eta_secs, capture.eta_secs);
        let remaining_slots = SLOTS_PER_EPOCH - 137;
        assert!((capture.eta_secs.unwrap() - remaining_slots as f64 / rate).abs() < 1e-6);

        capture.progress.pid = None;
        capture.progress.slots_per_sec = None;
        capture.progress.eta_secs = None;
        capture.slots_per_sec = None;
        capture.eta_secs = None;
        smooth_live_slot_rates(std::slice::from_mut(&mut capture), &mut windows, 1_015);
        assert_eq!(capture.slots_per_sec, None);
        assert!(
            windows.is_empty(),
            "missing producer identity must discard the mature rate window"
        );

        capture.progress.updated_unix_secs = Some(1_015 - PROGRESS_STALE_SECS - 1);
        hide_stale_live_rates(&mut capture.progress, 1_015);
        capture.slots_per_sec = capture.progress.slots_per_sec;
        capture.eta_secs = capture.progress.eta_secs;
        smooth_live_slot_rates(std::slice::from_mut(&mut capture), &mut windows, 1_015);
        assert_eq!(capture.slots_per_sec, None);
        assert!(
            windows.is_empty(),
            "stale captures cannot retain a rate window"
        );
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
        let capture = Path::new("/test/live/epoch-1001");
        let canonical = b"/test/bin/hivezilla\0capture-grpc\0--endpoint\0https://example.invalid\0--archive-dir\0/test/live/epoch-1001\0";
        assert!(live_producer_argv_matches_bytes(canonical, capture));
        let legacy = b"/test/bin/blockzilla-live-producer\0capture-grpc\0--endpoint\0https://example.invalid\0--archive-dir\0/test/live/epoch-1001\0";
        assert!(live_producer_argv_matches_bytes(legacy, capture));

        let prefix_only = b"/test/bin/blockzilla-live-producer\0capture-grpc\0--archive-dir\0/test/live/epoch-1001-old\0";
        assert!(!live_producer_argv_matches_bytes(prefix_only, capture));
        let wrong_command = b"/test/bin/blockzilla-live-producer\0record-grpc-raw\0--archive-dir\0/test/live/epoch-1001\0";
        assert!(!live_producer_argv_matches_bytes(wrong_command, capture));
        let duplicate = b"/test/bin/blockzilla-live-producer\0capture-grpc\0--archive-dir\0/test/live/epoch-1001\0--archive-dir=/test/live/epoch-1001\0";
        assert!(!live_producer_argv_matches_bytes(duplicate, capture));
        let unrecognized =
            b"/test/bin/not-hivezilla\0capture-grpc\0--archive-dir\0/test/live/epoch-1001\0";
        assert!(!live_producer_argv_matches_bytes(unrecognized, capture));
        assert!(!live_producer_argv_matches_bytes(
            &canonical[..canonical.len() - 1],
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
    fn canonical_live_capture_marks_one_current_and_preserves_closed_backlog() {
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
    }

    #[test]
    fn snapshot_json_has_stable_monitoring_contract() {
        let mut snapshot = empty_snapshot(true);
        snapshot.sequence = 7;
        snapshot.current_epoch = Some(701);
        snapshot.summary.queued = 2;
        snapshot
            .epochs
            .push(test_epoch(Path::new("/tmp"), 700, HistoricalState::Queued));
        snapshot.live.push(test_live_capture(
            Path::new("/tmp"),
            "epoch-701-live",
            701,
            LiveState::Capturing,
            99,
        ));
        let value = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(value["schema_version"], 3);
        assert_eq!(value["sequence"], 7);
        assert_eq!(value["current_epoch"], 701);
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
        assert_eq!(value["epochs"][0]["input_path"], "epoch-700.car");
        assert_eq!(value["epochs"][0]["output_path"], "epoch-700");
        assert_eq!(value["live"][0]["capture_dir"], "epoch-701-live");
        assert_eq!(value["live"][0]["output_path"], "epoch-701");
        assert!(
            !serde_json::to_string(&value).unwrap().contains("/tmp"),
            "public monitoring JSON must not expose absolute storage paths"
        );
        assert_eq!(
            value["live"][0]["source_capture_ids"],
            serde_json::json!([])
        );
        assert_eq!(value["live"][0]["superseded_by"], Value::Null);
        assert_eq!(value["live"][0]["is_current"], false);
        assert_eq!(value["live"][0]["eta_secs"], Value::Null);
        assert_eq!(value["live"][0]["slots_per_sec"], Value::Null);
        assert_eq!(value["live"][0]["rss_bytes"], Value::Null);
        assert_eq!(value["live"][0]["peak_rss_bytes"], Value::Null);

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
        let blocked = admission
            .blocked_reason(&config, &machine, &epochs[2])
            .unwrap();
        assert!(blocked.contains("next-lane capacity limit"));
        assert!(blocked.contains("remaining disk admission headroom"));

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
        let blocked = admission
            .blocked_reason(&config, &active_machine, &epochs[2])
            .unwrap();
        assert!(blocked.contains("next-lane capacity limit"));
        assert!(blocked.contains("raw available"));
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
    fn legacy_reuse_memory_reservation_charges_only_resident_sidecars() {
        let root = temp_root("legacy-resident-sidecars");
        let epoch = test_epoch(&root, 700, HistoricalState::Queued);
        write_legacy_registry_sidecars(&epoch.output_path, false);
        let set_len = |name: &str, len: u64| {
            OpenOptions::new()
                .write(true)
                .open(epoch.output_path.join(name))
                .unwrap()
                .set_len(len)
                .unwrap();
        };

        // These are disk-backed inputs in the scheduler's mandatory
        // --no-access reuse mode and must not consume the RSS envelope.
        set_len(REGISTRY_FILE, 2 * 1024 * 1024 * 1024);
        set_len(REGISTRY_COUNTS_FILE, 512 * 1024 * 1024);
        assert_eq!(
            legacy_compact_memory_reservation_bytes(&epoch.output_path),
            1024 * 1024 * 1024
        );

        // KeyIndex::load and load_blockhash_registry_plain do retain these in
        // process memory, so they are charged above the fixed overhead.
        set_len(REGISTRY_INDEX_FILE, 800 * 1024 * 1024);
        set_len(BLOCKHASH_REGISTRY_FILE, 100 * 1024 * 1024);
        assert_eq!(
            legacy_compact_memory_reservation_bytes(&epoch.output_path),
            (800 + 100 + LEGACY_COMPACT_MEMORY_OVERHEAD_MIB) * 1024 * 1024
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn settled_tuner_probe_uses_peak_rebound_and_hard_guard_envelope() {
        let root = temp_root("legacy-settled-probe-memory");
        let mib = 1024 * 1024;
        let gib = 1024 * mib;
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.memory_reserve_mib = 1_536;
        config.legacy_compact_memory_guard_mib = 512;
        config.disk_reserve_gib = 256;
        let active_epoch = test_epoch(&root, 294, HistoricalState::Scanning);
        let next_epoch = test_epoch(&root, 865, HistoricalState::Queued);
        write_legacy_registry_sidecars(&active_epoch.output_path, false);
        write_legacy_registry_sidecars(&next_epoch.output_path, false);
        let epochs = vec![active_epoch, next_epoch.clone()];
        let active_rss = BTreeMap::from([(294, 190 * mib)]);
        let machine = MachineSnapshot {
            memory_total_bytes: 8 * gib,
            memory_available_bytes: 3_252 * mib,
            disk_total_bytes: 512 * gib,
            disk_available_bytes: (config.disk_reserve_gib + 10) * gib,
            ..MachineSnapshot::default()
        };

        let conservative = LegacyCompactAdmission::new(&config, &machine, &epochs, &active_rss);
        assert!(
            conservative
                .blocked_reason(&config, &machine, &next_epoch)
                .is_some(),
            "the full theoretical future-growth model reproduces the NAS block"
        );
        let settled = LegacyCompactAdmission::with_memory_policy(
            &config,
            &machine,
            &epochs,
            &active_rss,
            LegacyCompactMemoryPolicy::SettledTuner {
                peak_rebound_bytes: 84 * mib,
            },
        );
        assert!(
            settled
                .blocked_reason(&config, &machine, &next_epoch)
                .is_none(),
            "3252 - (1536 + 512) - 84 leaves 1120 MiB for the full 1 GiB probe"
        );

        let mut lane = queue_lane(294, "running", 0, 1, None);
        lane.rss_bytes = Some(190 * mib);
        lane.progress.rss_bytes = Some(190 * mib);
        lane.progress.peak_rss_bytes = Some(274 * mib);
        let runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                accepted_lanes: 1,
                next_probe_unix_secs: 0,
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };
        let policy = settled_tuner_admission(
            &config,
            &[lane.clone()],
            &runtime,
            &active_rss,
            LegacyTunerContext::Bulk,
            2,
        )
        .unwrap();
        assert_eq!(policy.peak_rebound_bytes, 84 * mib);
        assert!(!policy.proven_refill);

        lane.progress.peak_rss_bytes = None;
        assert!(
            settled_tuner_admission(
                &config,
                &[lane],
                &runtime,
                &active_rss,
                LegacyTunerContext::Bulk,
                2,
            )
            .is_none(),
            "unknown peak telemetry must fall back to conservative admission"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_resource_capacity_does_not_turn_cpu_or_io_telemetry_into_lane_slots() {
        let root = temp_root("legacy-resource-capacity");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 4;
        config.legacy_compact_cpu_cores_per_worker = 1;
        config.legacy_compact_cpu_budget_cores = 8;
        config.legacy_compact_io_mib_per_sec_per_worker = 120;
        config.legacy_compact_io_budget_mib_per_sec = 250;
        let capacity = legacy_compact_resource_capacity(&config);
        assert_eq!(capacity.cpu_slots, usize::MAX);
        assert_eq!(capacity.io_slots, usize::MAX);
        assert_eq!(capacity.effective_slots, 4);
        assert!(legacy_compact_resource_blocked_reason(&config, capacity).is_none());

        // Per-worker estimates and global CPU/device budgets are dynamic
        // pressure guards, not integer lane-slot calculations.
        config.legacy_compact_cpu_cores_per_worker = 4;
        config.legacy_compact_cpu_budget_cores = 2;
        config.legacy_compact_io_budget_mib_per_sec = 1;
        let capacity = legacy_compact_resource_capacity(&config);
        assert_eq!(capacity.cpu_slots, usize::MAX);
        assert_eq!(capacity.io_slots, usize::MAX);
        assert_eq!(capacity.effective_slots, 4);
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
        assert_eq!(legacy_compact_resource_capacity(&config).effective_slots, 4);
        assert_eq!(admitted, 1);
        let reason = reason.unwrap();
        assert!(reason.contains("next-lane capacity limit"));
        assert!(reason.contains("remaining memory admission headroom"));
        assert!(reason.contains("raw MemAvailable"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn exclusive_finalizer_clamps_reported_legacy_capacity_to_active_reservations() {
        let root = temp_root("legacy-finalizer-hold-capacity");
        let mut config = test_config(&root);
        allow_legacy_workers(&mut config, 4);
        let epochs = [700, 800, 900]
            .into_iter()
            .map(|epoch| test_epoch(&root, epoch, HistoricalState::Queued))
            .collect::<Vec<_>>();
        for epoch in [700, 800, 900] {
            make_legacy_range_ready(&config, epoch);
        }
        let machine = legacy_scheduler_machine(&config, 4 * 1024, 4);
        // `sampled_legacy_compact_rss` also includes paused and adopted lanes;
        // those lanes retain RSS and future-growth reservations while stopped.
        let active = BTreeMap::from([(700, 512 * 1024 * 1024)]);
        let failures = BTreeMap::new();

        let (unheld_admitted, unheld_reason) = reported_legacy_compact_admission(
            &config,
            &machine,
            &epochs,
            &active,
            &failures,
            None,
            LegacyCompactMemoryPolicy::Conservative,
        );
        assert_eq!(unheld_admitted, 3);
        assert_eq!(unheld_reason, None);

        let mut repair = test_live_capture(
            &root,
            "epoch-1000-union-repair-view",
            1000,
            LiveState::Packaging,
            10,
        );
        repair.progress.phase = Some("materializing".to_string());
        let hold =
            legacy_compact_exclusive_hold_reason(&RuntimeState::default(), &epochs, &[repair])
                .unwrap();
        let (held_admitted, held_reason) = reported_legacy_compact_admission(
            &config,
            &machine,
            &epochs,
            &active,
            &failures,
            Some(&hold),
            LegacyCompactMemoryPolicy::Conservative,
        );
        assert_eq!(held_admitted, 1);
        let held_reason = held_reason.unwrap();
        assert!(held_reason.contains("live finalizer epoch-1000-union-repair-view"));
        assert!(held_reason.contains("phase materializing"));
        assert!(held_reason.contains("admitted and reserved"));
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
    fn scan_sweep_wait_is_not_reported_as_resource_blocking() {
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
        assert!(waiting.scan_sweep.blocked_reason.is_none());
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
        assert!(blocked.scan_sweep.blocked_reason.is_some());
        assert!(blocked.summary.finalizer_admission_blocked_reason.is_some());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn managed_scan_remains_counted_after_publishing_scan_marker() {
        let epoch = |epoch, state| EpochSnapshot {
            epoch,
            state,
            registry_order: RegistryOrder::Unknown,
            input_path: None,
            output_path: PathBuf::from(format!("/test/archive/epoch-{epoch}")),
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
    async fn active_packaging_finalizer_remains_exclusive_with_legacy_scheduler() {
        let root = temp_root("legacy-packaging-exclusive");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        allow_legacy_workers(&mut config, 2);
        let mut snapshot =
            schedulable_snapshot(&root, vec![test_epoch(&root, 700, HistoricalState::Queued)]);
        make_legacy_range_ready(&config, 700);
        snapshot.machine = legacy_scheduler_machine(&config, 3 * 1024, 3);
        let mut repair = test_live_capture(
            &root,
            "epoch-1000-union-repair-view",
            1000,
            LiveState::Packaging,
            10,
        );
        repair.progress.phase = Some("materializing".to_string());
        snapshot.live.push(repair);
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.legacy_compacts.is_empty());
        assert!(runtime.scans.is_empty());
        assert!(runtime.finalizer.is_none());
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

    #[test]
    fn zero_configured_lane_cap_has_no_static_cpu_or_io_ceiling() {
        let root = temp_root("legacy-resource-capacity");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_cpu_cores_per_worker = 2;
        config.legacy_compact_cpu_budget_cores = 10;
        // A tiny configured device ceiling must not be divided by the old
        // per-worker estimate and silently converted into a lane-count cap.
        config.legacy_compact_io_mib_per_sec_per_worker = 120;
        config.legacy_compact_io_budget_mib_per_sec = 1;

        let capacity = legacy_compact_resource_capacity(&config);
        assert!(legacy_tuning_enabled(&config));
        assert_eq!(capacity.cpu_slots, usize::MAX);
        assert_eq!(capacity.io_slots, usize::MAX);
        assert_eq!(capacity.effective_slots, usize::MAX);

        // A positive value remains a compatibility cap; zero is the adaptive
        // mode with no configured numeric ceiling.
        config.legacy_compact_concurrency = 3;
        assert!(!legacy_tuning_enabled(&config));
        assert_eq!(legacy_compact_resource_capacity(&config).effective_slots, 3);
    }

    #[test]
    fn uncapped_tuner_hill_climbs_and_bootstraps_finalizer_overlap() {
        let root = temp_root("legacy-tuner-capacity");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_min_running = 2;
        let now = 1_000;

        assert_eq!(
            legacy_tuner_capacity_limit_for_state(
                &config,
                LegacyTunerContext::Bulk,
                0,
                &LegacyThroughputTuner::default(),
                now,
            ),
            2
        );
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(
                &config,
                LegacyTunerContext::FinalizerOverlap,
                0,
                &LegacyThroughputTuner::default(),
                now,
            ),
            1
        );

        let observing = LegacyThroughputTuner {
            context: Some(LegacyTunerContext::Bulk),
            accepted_lanes: 7,
            next_probe_unix_secs: now,
            baseline_ready: true,
            ..LegacyThroughputTuner::default()
        };
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(
                &config,
                LegacyTunerContext::Bulk,
                7,
                &observing,
                now,
            ),
            8
        );

        let backing_off = LegacyThroughputTuner {
            phase: LegacyTunerPhase::Backoff,
            backoff_until_unix_secs: now + LEGACY_TUNER_BACKOFF_SECS,
            ..observing
        };
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(
                &config,
                LegacyTunerContext::Bulk,
                7,
                &backing_off,
                now,
            ),
            7
        );

        let guarded = LegacyThroughputTuner {
            context: Some(LegacyTunerContext::Bulk),
            guard_held: true,
            accepted_lanes: 7,
            ..LegacyThroughputTuner::default()
        };
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(
                &config,
                LegacyTunerContext::Bulk,
                7,
                &guarded,
                now,
            ),
            0,
            "a hard guard must prevent paused lanes from being refilled"
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn tuner_downshifts_only_after_sustained_degradation_and_a_b_gain() {
        let root = temp_root("legacy-tuner-downshift");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_min_running = 2;
        config.legacy_compact_cpu_budget_cores = 12;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let first_now = 30_000;
        let mut loaded = empty_snapshot(false);
        let managed = make_finished_child(
            ChildKind::HistoricalCompactReuse { epoch: 701 },
            root.join("progress.json"),
            root.join("worker.log"),
        )
        .await;
        let managed_pid = managed.pid.unwrap();
        let managed_started = managed.started_unix_secs;
        loaded.lanes = vec![
            useful_throughput_lane(700, 7_000, 100, 45.0, first_now),
            useful_throughput_lane(701, managed_pid, managed_started, 45.0, first_now),
        ];
        let mut runtime = RuntimeState {
            legacy_compacts: BTreeMap::from([(701, managed)]),
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                accepted_lanes: 2,
                accepted_useful_mib_per_sec: Some(120.0),
                accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                window: Some(settled_tuner_window(&loaded, first_now, 90.0)),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &loaded, &mut runtime, first_now);
        let tuner = &runtime.legacy_throughput_tuner;
        assert_eq!(tuner.degraded_windows, 1);
        assert_eq!(tuner.accepted_lanes, 2);
        assert_eq!(tuner.accepted_useful_mib_per_sec, Some(120.0));
        assert!(tuner.pending_action.is_none());
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(
                &config,
                LegacyTunerContext::Bulk,
                2,
                tuner,
                first_now,
            ),
            2,
            "a single degraded window must hold the topology instead of probing up"
        );

        let second_now = first_now + LEGACY_TUNER_SETTLE_SECS;
        for lane in &mut loaded.lanes {
            lane.progress.updated_unix_secs = Some(second_now);
            lane.updated_unix_secs = second_now;
        }
        runtime.legacy_throughput_tuner.window =
            Some(settled_tuner_window(&loaded, second_now, 90.0));
        observe_legacy_throughput_tuner(&config, &loaded, &mut runtime, second_now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert!(matches!(
            tuner.phase,
            LegacyTunerPhase::VerifyDownshift {
                loaded_lanes: 2,
                audit_epoch: 701,
                ..
            }
        ));
        assert!(matches!(
            tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Pause { epoch: 701, reason, .. })
                if reason.starts_with(LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX)
        ));
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(
                &config,
                LegacyTunerContext::Bulk,
                2,
                tuner,
                second_now,
            ),
            1,
            "the scheduler must not replace the deliberately paused audit lane"
        );

        let verify_now = second_now + LEGACY_TUNER_SETTLE_SECS;
        let mut reduced = empty_snapshot(false);
        reduced.lanes = vec![useful_throughput_lane(700, 7_000, 100, 100.0, verify_now)];
        runtime.auto_paused_legacy.insert(
            701,
            AutoPausedLegacy {
                epoch: 701,
                pid: managed_pid,
                process_start_ticks: None,
                reason: format!("{LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX} test downshift"),
                paused_unix_secs: second_now,
            },
        );
        runtime.legacy_throughput_tuner.pending_action = None;
        runtime.legacy_throughput_tuner.window =
            Some(settled_tuner_window(&reduced, verify_now, 100.0));
        observe_legacy_throughput_tuner(&config, &reduced, &mut runtime, verify_now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert_eq!(tuner.accepted_lanes, 2, "N-1 is only provisional");
        assert!(matches!(
            tuner.phase,
            LegacyTunerPhase::VerifyDownshiftReloaded { .. }
        ));
        assert!(matches!(
            tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Resume { epoch: 701, .. })
        ));

        let reload_now = verify_now + LEGACY_TUNER_SETTLE_SECS;
        for lane in &mut loaded.lanes {
            lane.progress.updated_unix_secs = Some(reload_now);
            lane.updated_unix_secs = reload_now;
        }
        runtime.auto_paused_legacy.remove(&701);
        runtime.legacy_throughput_tuner.pending_action = None;
        runtime.legacy_throughput_tuner.window =
            Some(settled_tuner_window(&loaded, reload_now, 91.0));
        observe_legacy_throughput_tuner(&config, &loaded, &mut runtime, reload_now);

        assert!(matches!(
            runtime.legacy_throughput_tuner.phase,
            LegacyTunerPhase::CommitDownshift { .. }
        ));
        assert!(matches!(
            runtime.legacy_throughput_tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Pause { epoch: 701, .. })
        ));

        let commit_now = reload_now + 1;
        runtime.auto_paused_legacy.insert(
            701,
            AutoPausedLegacy {
                epoch: 701,
                pid: managed_pid,
                process_start_ticks: None,
                reason: format!("{LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX} commit test"),
                paused_unix_secs: commit_now,
            },
        );
        runtime.legacy_throughput_tuner.pending_action = None;
        for lane in &mut reduced.lanes {
            lane.progress.updated_unix_secs = Some(commit_now);
            lane.updated_unix_secs = commit_now;
        }
        observe_legacy_throughput_tuner(&config, &reduced, &mut runtime, commit_now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert_eq!(tuner.accepted_lanes, 1);
        assert_eq!(tuner.accepted_useful_mib_per_sec, Some(100.0));
        assert!(matches!(tuner.phase, LegacyTunerPhase::Backoff));
        assert_eq!(
            tuner.backoff_until_unix_secs,
            commit_now + LEGACY_TUNER_CONFIRMED_DOWNSHIFT_BACKOFF_SECS
        );
        fs::remove_dir_all(root).ok();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn tuner_linux_adopted_exact_identity_pauses_resumes_and_blocks_replacement() {
        let root = temp_root("legacy-tuner-linux-handshake");
        let mut config = test_config(&root);
        config.blockzilla_bin = compile_idle_legacy_worker(&root.join("fixture"));
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_min_running = 2;
        config.legacy_compact_cpu_budget_cores = 12;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        config.legacy_compact_pause_cooldown = Duration::ZERO;
        config.start_epoch = Some(0);
        config.end_epoch = Some(1_000);
        make_legacy_range_ready(&config, 701);
        let epoch = test_epoch(&root, 701, HistoricalState::Queued);
        let mut managed = spawn_legacy_compact_reuse(&config, &epoch).await.unwrap();
        let managed_pid = managed.pid.unwrap();
        let managed_started = managed.started_unix_secs;
        let (_, start_ticks) = process_stat_identity(managed_pid).unwrap();
        let adopted = trusted_adopted_legacy_candidate(&config, 701)
            .expect("spawned worker is an exactly trusted restart survivor");
        let audit_identity = (
            701,
            managed_pid,
            LegacyLaneStartIdentity::LinuxStartTicks(start_ticks),
        );
        let survivor_pid = u32::MAX - 1;
        let first_now = 90_000;
        let mut loaded = empty_snapshot(false);
        loaded.lanes = vec![
            useful_throughput_lane(700, survivor_pid, 100, 45.0, first_now),
            useful_throughput_lane(701, managed_pid, managed_started, 45.0, first_now),
        ];
        loaded.lanes[1].pid = Some(managed_pid);
        let mut runtime = RuntimeState {
            adopted_legacy_compacts: BTreeMap::from([(701, adopted)]),
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                accepted_lanes: 2,
                accepted_useful_mib_per_sec: Some(120.0),
                accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                window: Some(settled_tuner_window(&loaded, first_now, 90.0)),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        assert!(legacy_identity_matches_controlled_lane(
            &config,
            &runtime,
            audit_identity,
        ));
        runtime
            .adopted_legacy_compacts
            .get_mut(&701)
            .unwrap()
            .identity_tainted = true;
        assert!(!legacy_identity_matches_controlled_lane(
            &config,
            &runtime,
            audit_identity,
        ));
        runtime
            .adopted_legacy_compacts
            .get_mut(&701)
            .unwrap()
            .identity_tainted = false;
        let output = config.archive_root.join("epoch-701");
        let mut owner = read_ownership(&output).unwrap();
        owner.pid = None;
        publish_ownership_marker(&output, &owner).unwrap();
        assert!(!legacy_identity_matches_controlled_lane(
            &config,
            &runtime,
            audit_identity,
        ));
        owner.pid = Some(managed_pid);
        publish_ownership_marker(&output, &owner).unwrap();
        runtime.paused_jobs.insert("compact_reuse:701".to_string());
        assert!(!exact_tuner_pause_owned(&config, &runtime, audit_identity,));
        runtime.paused_jobs.remove("compact_reuse:701");

        // A resumed restart survivor at exactly accepted+1 is still a causal
        // one-lane probe even though this controller did not spawn it.
        runtime.legacy_throughput_tuner = LegacyThroughputTuner {
            context: Some(LegacyTunerContext::Bulk),
            accepted_lanes: 1,
            accepted_useful_mib_per_sec: Some(60.0),
            accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
            ..LegacyThroughputTuner::default()
        };
        observe_legacy_throughput_tuner(&config, &loaded, &mut runtime, first_now);
        assert!(matches!(
            runtime.legacy_throughput_tuner.phase,
            LegacyTunerPhase::ProbeUp {
                baseline_lanes: 1,
                probe_epoch: 701,
                ..
            }
        ));
        assert_eq!(
            runtime.legacy_throughput_tuner.probe_identity,
            Some(audit_identity)
        );

        // The absolute archive-device ceiling must also be able to queue an
        // exact pause when every eligible lane was adopted after restart.
        runtime.legacy_throughput_tuner = LegacyThroughputTuner {
            context: Some(LegacyTunerContext::Bulk),
            accepted_lanes: 2,
            accepted_useful_mib_per_sec: Some(120.0),
            accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
            ..LegacyThroughputTuner::default()
        };
        config.legacy_compact_io_budget_mib_per_sec = 80;
        loaded.machine.archive_device_read_mib_per_sec = Some(90.0);
        loaded.machine.archive_device_write_mib_per_sec = Some(1.0);
        observe_legacy_throughput_tuner(&config, &loaded, &mut runtime, first_now + 1);
        assert!(matches!(
            runtime.legacy_throughput_tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Pause {
                epoch: 701,
                expected_identity: Some(identity),
                reason,
            }) if *identity == audit_identity
                && reason.starts_with(LEGACY_TUNER_GUARD_PAUSE_PREFIX)
        ));
        config.legacy_compact_io_budget_mib_per_sec = 0;
        loaded.machine.archive_device_read_mib_per_sec = None;
        loaded.machine.archive_device_write_mib_per_sec = None;
        runtime.legacy_throughput_tuner = LegacyThroughputTuner {
            context: Some(LegacyTunerContext::Bulk),
            accepted_lanes: 2,
            accepted_useful_mib_per_sec: Some(120.0),
            accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
            window: Some(settled_tuner_window(&loaded, first_now, 90.0)),
            ..LegacyThroughputTuner::default()
        };

        observe_legacy_throughput_tuner(&config, &loaded, &mut runtime, first_now);
        assert_eq!(runtime.legacy_throughput_tuner.degraded_windows, 1);
        let second_now = first_now + LEGACY_TUNER_SETTLE_SECS;
        for lane in &mut loaded.lanes {
            lane.progress.updated_unix_secs = Some(second_now);
            lane.updated_unix_secs = second_now;
        }
        runtime.legacy_throughput_tuner.window =
            Some(settled_tuner_window(&loaded, second_now, 90.0));
        observe_legacy_throughput_tuner(&config, &loaded, &mut runtime, second_now);
        assert!(matches!(
            runtime.legacy_throughput_tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Pause {
                expected_identity: Some(identity),
                ..
            }) if *identity == audit_identity
        ));

        adjust_legacy_workers_for_pressure(&config, &loaded, &mut runtime, second_now + 1);
        let paused_record = runtime.auto_paused_legacy.get(&701).unwrap();
        assert_eq!(paused_record.process_start_ticks, Some(start_ticks));
        assert!(
            paused_record
                .reason
                .starts_with(LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX)
        );
        assert!(runtime.legacy_throughput_tuner.pending_action.is_none());

        let verify_now = second_now + LEGACY_TUNER_SETTLE_SECS;
        let mut reduced = empty_snapshot(false);
        reduced.lanes = vec![useful_throughput_lane(
            700,
            survivor_pid,
            100,
            100.0,
            verify_now,
        )];
        runtime.legacy_throughput_tuner.window =
            Some(settled_tuner_window(&reduced, verify_now, 100.0));
        observe_legacy_throughput_tuner(&config, &reduced, &mut runtime, verify_now);
        assert!(matches!(
            runtime.legacy_throughput_tuner.phase,
            LegacyTunerPhase::VerifyDownshiftReloaded { .. }
        ));
        assert!(matches!(
            runtime.legacy_throughput_tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Resume {
                expected_identity: Some(identity),
                ..
            }) if *identity == audit_identity
        ));

        // Production order is adjust -> schedule -> fresh snapshot -> observe.
        // A retryable resume must prevent the scheduler from replacing the
        // stopped audit lane before SIGCONT succeeds.
        make_legacy_range_ready(&config, 800);
        let mut schedulable =
            schedulable_snapshot(&root, vec![test_epoch(&root, 800, HistoricalState::Queued)]);
        schedulable.lanes = reduced.lanes.clone();
        schedulable.machine = legacy_scheduler_machine(&config, 8 * 1024, 2 * 1024);
        assert_eq!(
            top_up_legacy_compacts(&config, &schedulable, &mut runtime, usize::MAX).await,
            0
        );
        assert!(!runtime.legacy_compacts.contains_key(&800));

        adjust_legacy_workers_for_pressure(&config, &reduced, &mut runtime, verify_now + 1);
        assert!(!runtime.auto_paused_legacy.contains_key(&701));
        assert!(runtime.legacy_throughput_tuner.pending_action.is_none());
        assert_eq!(process_stat_identity(managed_pid).unwrap().1, start_ticks);

        let reload_now = verify_now + LEGACY_TUNER_SETTLE_SECS;
        for lane in &mut loaded.lanes {
            lane.progress.updated_unix_secs = Some(reload_now);
            lane.updated_unix_secs = reload_now;
        }
        runtime.legacy_throughput_tuner.window =
            Some(settled_tuner_window(&loaded, reload_now, 91.0));
        observe_legacy_throughput_tuner(&config, &loaded, &mut runtime, reload_now);
        assert!(matches!(
            runtime.legacy_throughput_tuner.phase,
            LegacyTunerPhase::CommitDownshift { .. }
        ));

        adjust_legacy_workers_for_pressure(&config, &loaded, &mut runtime, reload_now + 1);
        assert!(runtime.auto_paused_legacy.contains_key(&701));
        let commit_now = reload_now + 2;
        for lane in &mut reduced.lanes {
            lane.progress.updated_unix_secs = Some(commit_now);
            lane.updated_unix_secs = commit_now;
        }
        observe_legacy_throughput_tuner(&config, &reduced, &mut runtime, commit_now);
        assert_eq!(runtime.legacy_throughput_tuner.accepted_lanes, 1);
        assert!(matches!(
            runtime.legacy_throughput_tuner.phase,
            LegacyTunerPhase::Backoff
        ));

        // A restart survivor also remains eligible for the ordinary hard
        // resource guard, independently of the throughput experiment.
        assert_eq!(
            unsafe { libc::kill(-(managed_pid as libc::pid_t), libc::SIGCONT) },
            0
        );
        runtime.auto_paused_legacy.remove(&701);
        runtime.legacy_throughput_tuner.pending_action = None;
        loaded.machine.load_1m = config.legacy_compact_cpu_budget_cores as f64 + 1.0;
        adjust_legacy_workers_for_pressure(&config, &loaded, &mut runtime, commit_now + 1);
        assert!(
            runtime
                .auto_paused_legacy
                .get(&701)
                .is_some_and(|record| record.reason.contains("CPU load ceiling"))
        );

        // SAFETY: the fixture was spawned by spawn_child with process_group(0).
        let _ = unsafe { libc::kill(-(managed_pid as libc::pid_t), libc::SIGCONT) };
        let _ = unsafe { libc::kill(-(managed_pid as libc::pid_t), libc::SIGKILL) };
        let _ = managed.child.wait().await;
        runtime.adopted_legacy_compacts.remove(&701);
        fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn tuner_keeps_sticky_reference_during_gradual_degradation() {
        let root = temp_root("legacy-tuner-gradual-degradation");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_min_running = 2;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 35_000;
        let managed = make_finished_child(
            ChildKind::HistoricalCompactReuse { epoch: 701 },
            root.join("progress.json"),
            root.join("worker.log"),
        )
        .await;
        let mut snapshot = empty_snapshot(false);
        snapshot.lanes = vec![
            useful_throughput_lane(700, 7_000, 100, 137.5, now),
            useful_throughput_lane(
                701,
                managed.pid.unwrap(),
                managed.started_unix_secs,
                137.5,
                now,
            ),
        ];
        let mut runtime = RuntimeState {
            legacy_compacts: BTreeMap::from([(701, managed)]),
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                accepted_lanes: 2,
                accepted_useful_mib_per_sec: Some(300.0),
                accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                baseline_ready: true,
                window: Some(settled_tuner_window(&snapshot, now, 275.0)),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &snapshot, &mut runtime, now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert_eq!(tuner.accepted_useful_mib_per_sec, Some(300.0));
        assert!(!tuner.baseline_ready);
        assert!(tuner.pending_action.is_none());
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(&config, LegacyTunerContext::Bulk, 2, tuner, now,),
            2,
            "a gradual decline must be observed at N instead of erased or hidden by N+1"
        );
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn downshift_audit_resumes_lane_when_less_parallelism_is_not_faster() {
        let root = temp_root("legacy-tuner-downshift-reject");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 40_000;
        let mut loaded = empty_snapshot(false);
        loaded.lanes = vec![
            useful_throughput_lane(700, 7_000, 100, 45.0, now),
            useful_throughput_lane(701, 7_001, 200, 45.0, now),
        ];
        let identities = legacy_throughput_observation(&loaded, now).identities;
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                phase: LegacyTunerPhase::VerifyDownshiftReloaded {
                    loaded_lanes: 2,
                    loaded_mib_per_sec: 90.0,
                    reduced_mib_per_sec: 82.0,
                    reduced_device_mib_per_sec: None,
                    audit_epoch: 701,
                },
                accepted_lanes: 2,
                accepted_useful_mib_per_sec: Some(120.0),
                accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                probe_identity: identities.iter().find(|id| id.0 == 701).copied(),
                audit_loaded_identities: Some(identities),
                audit_started_unix_secs: now - LEGACY_TUNER_SETTLE_SECS,
                window: Some(settled_tuner_window(&loaded, now, 90.0)),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &loaded, &mut runtime, now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert_eq!(tuner.accepted_lanes, 2);
        assert_eq!(tuner.accepted_useful_mib_per_sec, Some(90.0));
        assert!(matches!(tuner.phase, LegacyTunerPhase::ObserveBaseline));
        assert!(tuner.pending_action.is_none());
        assert_eq!(tuner.next_probe_unix_secs, now + LEGACY_TUNER_BACKOFF_SECS);
    }

    #[test]
    fn throughput_degradation_requires_both_absolute_and_relative_evidence() {
        assert_eq!(legacy_tuner_degradation(300.0, 269.0), Some((31.0, 30.0)));
        assert_eq!(legacy_tuner_degradation(300.0, 271.0), None);
        assert_eq!(legacy_tuner_degradation(100.0, 84.0), Some((16.0, 16.0)));
        assert_eq!(legacy_tuner_degradation(100.0, 85.0), None);
        assert_eq!(legacy_tuner_degradation(f64::NAN, 80.0), None);
    }

    #[test]
    fn stale_tuner_pause_action_cancels_without_lowering_the_proven_target() {
        let root = temp_root("legacy-tuner-stale-pause");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 50_000;
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                accepted_lanes: 3,
                accepted_useful_mib_per_sec: Some(180.0),
                accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                pending_action: Some(LegacyTunerAction::Pause {
                    epoch: 703,
                    expected_identity: Some((
                        703,
                        7_003,
                        LegacyLaneStartIdentity::LinuxStartTicks(300),
                    )),
                    reason: format!("{LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX} stale test"),
                }),
                pending_action_started_unix_secs: now - LEGACY_TUNER_ACTION_TIMEOUT_SECS,
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };
        let snapshot = empty_snapshot(false);

        adjust_legacy_workers_for_pressure(&config, &snapshot, &mut runtime, now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert!(tuner.pending_action.is_none());
        assert!(matches!(tuner.phase, LegacyTunerPhase::ObserveBaseline));
        assert_eq!(tuner.accepted_lanes, 3);
        assert_eq!(tuner.accepted_useful_mib_per_sec, Some(180.0));
        assert!(
            tuner
                .last_decision
                .as_deref()
                .is_some_and(|message| message.contains("cancelled stale topology action"))
        );
    }

    #[test]
    fn manual_pause_ownership_cancels_tuner_action_without_auto_resume() {
        let now = 60_000;
        let identity = (701, 7_001, LegacyLaneStartIdentity::LinuxStartTicks(200));
        let mut runtime = RuntimeState {
            auto_paused_legacy: BTreeMap::from([(
                701,
                AutoPausedLegacy {
                    epoch: 701,
                    pid: 7_001,
                    process_start_ticks: Some(200),
                    reason: format!("{LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX} ownership test"),
                    paused_unix_secs: now - 1,
                },
            )]),
            legacy_throughput_tuner: LegacyThroughputTuner {
                phase: LegacyTunerPhase::VerifyDownshift {
                    loaded_lanes: 2,
                    loaded_mib_per_sec: 90.0,
                    audit_epoch: 701,
                },
                accepted_lanes: 2,
                probe_identity: Some(identity),
                pending_action: Some(LegacyTunerAction::Resume {
                    epoch: 701,
                    expected_identity: Some(identity),
                    reason: "test resume".to_string(),
                }),
                pending_action_started_unix_secs: now,
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        set_manual_pause_state(&mut runtime, "compact_reuse:701", Some(701), true);

        assert!(runtime.paused_jobs.contains("compact_reuse:701"));
        assert!(!runtime.auto_paused_legacy.contains_key(&701));
        assert!(runtime.legacy_throughput_tuner.pending_action.is_none());
        assert!(matches!(
            runtime.legacy_throughput_tuner.phase,
            LegacyTunerPhase::ObserveBaseline
        ));
        assert_eq!(runtime.legacy_throughput_tuner.accepted_lanes, 2);
    }

    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn downshift_identity_churn_and_timeout_resume_only_the_owned_lane() {
        let root = temp_root("legacy-tuner-downshift-identity");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 70_000;
        let managed = make_finished_child(
            ChildKind::HistoricalCompactReuse { epoch: 701 },
            root.join("progress.json"),
            root.join("worker.log"),
        )
        .await;
        let audit_identity = (
            701,
            managed.pid.unwrap(),
            LegacyLaneStartIdentity::UnixSecs(managed.started_unix_secs),
        );
        let expected_survivor = (700, 7_000, LegacyLaneStartIdentity::UnixSecs(100));
        let mut changed = empty_snapshot(false);
        changed.lanes = vec![useful_throughput_lane(700, 7_002, 100, 100.0, now)];
        let paused = AutoPausedLegacy {
            epoch: 701,
            pid: audit_identity.1,
            process_start_ticks: None,
            reason: format!("{LEGACY_TUNER_DOWNSHIFT_PAUSE_PREFIX} identity test"),
            paused_unix_secs: now - LEGACY_TUNER_SETTLE_SECS,
        };
        let mut runtime = RuntimeState {
            legacy_compacts: BTreeMap::from([(701, managed)]),
            auto_paused_legacy: BTreeMap::from([(701, paused.clone())]),
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                phase: LegacyTunerPhase::VerifyDownshift {
                    loaded_lanes: 2,
                    loaded_mib_per_sec: 90.0,
                    audit_epoch: 701,
                },
                accepted_lanes: 2,
                accepted_useful_mib_per_sec: Some(120.0),
                accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                probe_identity: Some(audit_identity),
                audit_survivor_identities: Some(BTreeSet::from([expected_survivor])),
                audit_started_unix_secs: now - LEGACY_TUNER_SETTLE_SECS,
                window: Some(settled_tuner_window(&changed, now, 100.0)),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &changed, &mut runtime, now);
        assert_eq!(runtime.legacy_throughput_tuner.accepted_lanes, 2);
        assert!(matches!(
            runtime.legacy_throughput_tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Resume {
                epoch: 701,
                expected_identity: Some(identity),
                ..
            }) if *identity == audit_identity
        ));

        runtime.legacy_throughput_tuner.pending_action = None;
        runtime.legacy_throughput_tuner.phase = LegacyTunerPhase::VerifyDownshift {
            loaded_lanes: 2,
            loaded_mib_per_sec: 90.0,
            audit_epoch: 701,
        };
        runtime.legacy_throughput_tuner.probe_identity = Some(audit_identity);
        runtime.legacy_throughput_tuner.audit_survivor_identities =
            Some(BTreeSet::from([expected_survivor]));
        runtime.legacy_throughput_tuner.audit_started_unix_secs =
            now - LEGACY_TUNER_DOWNSHIFT_TIMEOUT_SECS;
        runtime.auto_paused_legacy.insert(701, paused);
        let mut expected = empty_snapshot(false);
        expected.lanes = vec![useful_throughput_lane(700, 7_000, 100, 0.0, now)];
        expected.lanes[0].progress.input_mib_per_sec = None;

        observe_legacy_throughput_tuner(&config, &expected, &mut runtime, now);
        assert!(matches!(
            runtime.legacy_throughput_tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Resume {
                epoch: 701,
                expected_identity: Some(identity),
                ..
            }) if *identity == audit_identity
        ));
        assert_eq!(runtime.legacy_throughput_tuner.accepted_lanes, 2);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn pressure_hold_never_rebases_or_unlocks_a_throughput_probe() {
        let root = temp_root("legacy-tuner-pressure-hold");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 80_000;
        let mut snapshot = empty_snapshot(false);
        snapshot.lanes = vec![
            useful_throughput_lane(700, 7_000, 100, 45.0, now),
            useful_throughput_lane(701, 7_001, 200, 45.0, now),
        ];
        let mib = 1024 * 1024;
        let pause = config
            .memory_reserve_mib
            .saturating_add(config.legacy_compact_memory_guard_mib)
            .saturating_mul(mib);
        let resume = config
            .memory_reserve_mib
            .saturating_add(config.legacy_compact_memory_guard_mib.saturating_mul(2))
            .saturating_mul(mib);
        snapshot.machine.memory_total_bytes = 8 * 1024 * 1024 * 1024;
        snapshot.machine.memory_available_bytes = (pause + resume) / 2;
        assert_eq!(
            legacy_pressure_state(&config, &snapshot.machine),
            LegacyPressureState::Hold
        );
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                accepted_lanes: 2,
                accepted_useful_mib_per_sec: Some(120.0),
                accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                baseline_ready: true,
                window: Some(settled_tuner_window(&snapshot, now, 90.0)),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &snapshot, &mut runtime, now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert_eq!(tuner.accepted_useful_mib_per_sec, Some(120.0));
        assert!(!tuner.baseline_ready);
        assert!(tuner.window.is_none());
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(&config, LegacyTunerContext::Bulk, 2, tuner, now,),
            2
        );

        runtime.legacy_throughput_tuner.phase = LegacyTunerPhase::VerifyDownshift {
            loaded_lanes: 2,
            loaded_mib_per_sec: 90.0,
            audit_epoch: 701,
        };
        runtime.legacy_throughput_tuner.audit_started_unix_secs = now;
        runtime.legacy_throughput_tuner.window = Some(settled_tuner_window(&snapshot, now, 90.0));
        observe_legacy_throughput_tuner(&config, &snapshot, &mut runtime, now);
        assert!(matches!(
            runtime.legacy_throughput_tuner.phase,
            LegacyTunerPhase::VerifyDownshift { .. }
        ));
        assert_eq!(
            runtime.legacy_throughput_tuner.accepted_useful_mib_per_sec,
            Some(120.0)
        );

        snapshot.machine.memory_available_bytes = pause.saturating_sub(1);
        observe_legacy_throughput_tuner(&config, &snapshot, &mut runtime, now + 1);
        assert!(matches!(
            runtime.legacy_throughput_tuner.phase,
            LegacyTunerPhase::ObserveBaseline
        ));
        assert_eq!(runtime.legacy_throughput_tuner.accepted_lanes, 2);
        assert!(
            runtime
                .legacy_throughput_tuner
                .last_decision
                .as_deref()
                .is_some_and(|decision| decision.contains("resource pressure interrupted"))
        );
    }

    #[test]
    fn proven_tuner_topology_survives_underfill_source_change_and_probe_churn() {
        let root = temp_root("legacy-tuner-proven-floor");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 8_000;
        let mut one_lane = empty_snapshot(false);
        one_lane.lanes = vec![useful_throughput_lane(700, 7_000, 100, 100.0, now)];
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                accepted_lanes: 2,
                accepted_useful_mib_per_sec: Some(124.0),
                accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                current_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };
        observe_legacy_throughput_tuner(&config, &one_lane, &mut runtime, now);
        assert_eq!(runtime.legacy_throughput_tuner.accepted_lanes, 2);
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(
                &config,
                LegacyTunerContext::Bulk,
                1,
                &runtime.legacy_throughput_tuner,
                now,
            ),
            2,
            "a normal worker completion must request replacement, not redefine the optimum"
        );

        let mut two_lanes = one_lane.clone();
        two_lanes
            .lanes
            .push(useful_throughput_lane(701, 7_001, 200, 24.0, now));
        runtime.legacy_throughput_tuner.accepted_rate_source =
            Some(LegacyTunerRateSource::ProcessIo);
        runtime.legacy_throughput_tuner.current_rate_source =
            Some(LegacyTunerRateSource::ProcessIo);
        observe_legacy_throughput_tuner(&config, &two_lanes, &mut runtime, now + 1);
        assert_eq!(runtime.legacy_throughput_tuner.accepted_lanes, 2);
        assert_eq!(
            runtime.legacy_throughput_tuner.accepted_useful_mib_per_sec, None,
            "a telemetry upgrade recalibrates rate without discarding topology"
        );

        runtime.legacy_throughput_tuner.phase = LegacyTunerPhase::ProbeUp {
            baseline_lanes: 2,
            baseline_mib_per_sec: 124.0,
            probe_epoch: 702,
        };
        runtime.legacy_throughput_tuner.accepted_useful_mib_per_sec = Some(124.0);
        runtime.legacy_throughput_tuner.accepted_rate_source =
            Some(LegacyTunerRateSource::LogicalInput);
        observe_legacy_throughput_tuner(&config, &two_lanes, &mut runtime, now + 2);
        assert_eq!(runtime.legacy_throughput_tuner.accepted_lanes, 2);
        assert!(matches!(
            runtime.legacy_throughput_tuner.phase,
            LegacyTunerPhase::ObserveBaseline
        ));
    }

    #[tokio::test]
    async fn proven_tuner_profiles_persist_and_restore_per_scheduler_context() {
        let root = temp_root("legacy-tuner-profile-persist");
        let config = test_config(&root);
        fs::create_dir_all(&config.state_root).unwrap();
        let runtime = RuntimeState {
            legacy_tuner_profiles: LegacyTunerProfiles {
                bulk: LegacyTunerProfile {
                    accepted_lanes: 2,
                    accepted_useful_mib_per_sec: Some(124.0),
                    accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                    accepted_device_mib_per_sec: Some(280.0),
                },
                finalizer_overlap: LegacyTunerProfile {
                    accepted_lanes: 1,
                    accepted_useful_mib_per_sec: Some(70.0),
                    accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                    accepted_device_mib_per_sec: Some(180.0),
                },
            },
            ..RuntimeState::default()
        };
        persist_control_state(&config, &runtime).unwrap();

        let state = test_app_state(config.clone());
        load_control_state(&state).await.unwrap();
        let mut loaded = state.runtime.lock().await;
        reset_legacy_tuner_context(&mut loaded, LegacyTunerContext::Bulk, 10_000);
        assert_eq!(loaded.legacy_throughput_tuner.accepted_lanes, 2);
        assert_eq!(
            loaded.legacy_throughput_tuner.accepted_rate_source,
            Some(LegacyTunerRateSource::LogicalInput)
        );
        reset_legacy_tuner_context(&mut loaded, LegacyTunerContext::FinalizerOverlap, 10_001);
        assert_eq!(loaded.legacy_throughput_tuner.accepted_lanes, 1);
        drop(loaded);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn hard_pressure_stops_the_probe_before_the_accepted_baseline() {
        let root = temp_root("legacy-pressure-probe-first");
        let mut config = test_config(&root);
        config.priority_epoch_start = Some(863);
        config.priority_epoch_end = Some(899);
        let tuner = LegacyThroughputTuner {
            phase: LegacyTunerPhase::ProbeUp {
                baseline_lanes: 1,
                baseline_mib_per_sec: 100.0,
                probe_epoch: 899,
            },
            accepted_lanes: 1,
            ..LegacyThroughputTuner::default()
        };
        assert_eq!(
            hard_pressure_legacy_epoch(&config, &tuner, &[294, 899]),
            Some(899)
        );
        assert_eq!(
            hard_pressure_legacy_epoch(&config, &LegacyThroughputTuner::default(), &[294, 899],),
            Some(294),
            "without a tracked probe the existing priority fallback is preserved"
        );
    }

    #[test]
    fn tuner_does_not_score_a_window_while_a_topology_action_is_pending() {
        let root = temp_root("legacy-tuner-pending-action");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 5_000;
        let mut snapshot = empty_snapshot(false);
        snapshot.lanes = vec![useful_throughput_lane(700, 7_000, 100, 100.0, now)];
        let window = settled_tuner_window(&snapshot, now, 100.0);
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                pending_action: Some(LegacyTunerAction::Pause {
                    epoch: 700,
                    expected_identity: None,
                    reason: format!("{LEGACY_TUNER_PAUSE_PREFIX} test handshake"),
                }),
                window: Some(window),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &snapshot, &mut runtime, now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert_eq!(tuner.accepted_lanes, 0);
        assert_eq!(tuner.accepted_useful_mib_per_sec, None);
        assert!(tuner.window.is_none());
        assert!(matches!(
            tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Pause { epoch: 700, .. })
        ));
    }

    #[test]
    fn tuner_never_rebases_across_an_unattributed_multi_lane_jump() {
        let root = temp_root("legacy-tuner-multi-lane-jump");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 6_000;
        let mut snapshot = empty_snapshot(false);
        snapshot.lanes = vec![
            useful_throughput_lane(700, 7_000, 100, 40.0, now),
            useful_throughput_lane(701, 7_001, 200, 40.0, now),
            useful_throughput_lane(702, 7_002, 300, 40.0, now),
        ];
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                accepted_lanes: 1,
                accepted_useful_mib_per_sec: Some(50.0),
                accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                baseline_ready: true,
                window: Some(settled_tuner_window(&snapshot, now, 120.0)),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &snapshot, &mut runtime, now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert_eq!(tuner.accepted_lanes, 1);
        assert!(!tuner.baseline_ready);
        assert!(tuner.window.is_none());
        assert!(
            tuner
                .last_decision
                .as_deref()
                .is_some_and(|decision| decision.contains("one lane at a time"))
        );
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(&config, LegacyTunerContext::Bulk, 3, tuner, now),
            1,
        );
    }

    #[test]
    fn tuner_uses_complete_process_io_only_as_an_old_worker_fallback() {
        let now = 7_000;
        let mut snapshot = empty_snapshot(false);
        snapshot.lanes = vec![
            useful_throughput_lane(700, 7_000, 100, 60.0, now),
            useful_throughput_lane(701, 7_001, 200, 60.0, now),
        ];
        for lane in &mut snapshot.lanes {
            lane.progress.input_mib_per_sec = None;
            lane.progress.disk_read_mib_per_sec = Some(40.0);
            lane.progress.disk_write_mib_per_sec = Some(20.0);
        }

        let fallback = legacy_throughput_observation(&snapshot, now);
        assert_eq!(fallback.sampled_lanes, 2);
        assert_eq!(fallback.useful_mib_per_sec, Some(120.0));
        assert_eq!(fallback.rate_source, Some(LegacyTunerRateSource::ProcessIo));

        for lane in &mut snapshot.lanes {
            lane.progress.input_mib_per_sec = Some(55.0);
        }
        let native = legacy_throughput_observation(&snapshot, now);
        assert_eq!(native.useful_mib_per_sec, Some(110.0));
        assert_eq!(
            native.rate_source,
            Some(LegacyTunerRateSource::LogicalInput)
        );
    }

    #[test]
    fn live_ingest_guard_requires_slot_advancement_not_just_a_fresh_heartbeat() {
        let root = temp_root("legacy-tuner-live-advance");
        let mut capture =
            test_live_capture(&root, "epoch-1001-current", 1001, LiveState::Capturing, 100);
        capture.is_current = true;
        capture.last_slot = Some(100);
        capture.progress.pid = Some(42);
        capture.progress.last_slot = Some(100);
        capture.progress.updated_unix_secs = Some(1_000);
        let mut snapshot = empty_snapshot(false);
        snapshot.live.push(capture);
        let mut tuner = LegacyThroughputTuner::default();

        assert!(update_current_live_ingest_health(
            &snapshot, &mut tuner, 1_000
        ));
        snapshot.live[0].progress.updated_unix_secs = Some(1_059);
        assert!(update_current_live_ingest_health(
            &snapshot, &mut tuner, 1_059
        ));

        snapshot.live[0].progress.updated_unix_secs = Some(1_061);
        assert!(!update_current_live_ingest_health(
            &snapshot, &mut tuner, 1_061
        ));

        snapshot.live[0].last_slot = Some(101);
        snapshot.live[0].progress.last_slot = Some(101);
        snapshot.live[0].progress.updated_unix_secs = Some(1_062);
        assert!(update_current_live_ingest_health(
            &snapshot, &mut tuner, 1_062
        ));
    }

    #[test]
    fn stale_live_ingest_is_advisory_and_does_not_pause_compaction() {
        let root = temp_root("legacy-tuner-live-advisory");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 10_000;
        let mut snapshot = empty_snapshot(false);
        snapshot.lanes = vec![
            useful_throughput_lane(700, 7_000, 100, 60.0, now),
            useful_throughput_lane(701, 7_001, 200, 60.0, now),
        ];
        let mut capture =
            test_live_capture(&root, "epoch-1001-current", 1001, LiveState::Capturing, 100);
        capture.is_current = true;
        capture.last_slot = Some(100);
        capture.progress.pid = Some(42);
        capture.progress.last_slot = Some(100);
        let seed_time = now - LEGACY_TUNER_LIVE_STALL_SECS - 1;
        capture.progress.updated_unix_secs = Some(seed_time);
        snapshot.live.push(capture);
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                accepted_lanes: 2,
                accepted_useful_mib_per_sec: Some(120.0),
                accepted_rate_source: Some(LegacyTunerRateSource::LogicalInput),
                baseline_ready: true,
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        assert!(update_current_live_ingest_health(
            &snapshot,
            &mut runtime.legacy_throughput_tuner,
            seed_time,
        ));
        // Reproduce the production failure: the producer process still emits
        // a fresh heartbeat, but its durable slot has not advanced for longer
        // than the live-stall window.
        snapshot.live[0].progress.updated_unix_secs = Some(now);

        observe_legacy_throughput_tuner(&config, &snapshot, &mut runtime, now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert!(!tuner.guard_held);
        assert!(tuner.pending_action.is_none());
        assert_eq!(tuner.current_active_lanes, 2);
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(&config, LegacyTunerContext::Bulk, 2, tuner, now,),
            3,
            "stalled live telemetry must neither collapse the accepted topology nor suppress a safe probe",
        );
        let mut summary = PipelineSummary::default();
        publish_legacy_tuner_summary(&config, &mut summary, tuner, 3, now);
        assert_ne!(
            summary.legacy_compact_tuning_state.as_deref(),
            Some("resource_guard")
        );
        assert_eq!(summary.legacy_compact_tuning_target, 3);
    }

    #[test]
    fn deprecated_live_guard_pause_is_resumed_while_live_remains_stalled() {
        let root = temp_root("legacy-tuner-live-guard-migration");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 10_000;
        let mut snapshot = empty_snapshot(false);
        let mut capture =
            test_live_capture(&root, "epoch-1001-current", 1001, LiveState::Capturing, 100);
        capture.is_current = true;
        capture.last_slot = Some(100);
        capture.progress.pid = Some(42);
        capture.progress.last_slot = Some(100);
        capture.progress.updated_unix_secs = Some(now);
        snapshot.live.push(capture);
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                guard_held: true,
                accepted_lanes: 1,
                live_slot_watch: BTreeMap::from([(
                    "epoch-1001-current".to_string(),
                    LegacyLiveSlotWatch {
                        pid: 42,
                        slot: 100,
                        last_advanced_unix_secs: now - LEGACY_TUNER_LIVE_STALL_SECS - 1,
                    },
                )]),
                ..LegacyThroughputTuner::default()
            },
            auto_paused_legacy: BTreeMap::from([(
                700,
                AutoPausedLegacy {
                    epoch: 700,
                    pid: 7_000,
                    process_start_ticks: Some(100),
                    reason: format!(
                        "{LEGACY_TUNER_GUARD_PAUSE_PREFIX} current live ingest telemetry is stale or not advancing"
                    ),
                    paused_unix_secs: now - 1,
                },
            )]),
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &snapshot, &mut runtime, now);

        assert!(runtime.legacy_throughput_tuner.guard_held);
        assert!(matches!(
            runtime.legacy_throughput_tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Resume { epoch: 700, .. })
        ));
        assert!(
            runtime
                .legacy_throughput_tuner
                .last_decision
                .as_deref()
                .is_some_and(|decision| decision.contains("hard guard recovered"))
        );
    }

    #[test]
    fn configured_device_throughput_ceiling_remains_a_hard_guard() {
        let root = temp_root("legacy-tuner-device-hard-guard");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 100;
        let now = 10_000;
        let mut snapshot = empty_snapshot(false);
        snapshot.lanes = vec![useful_throughput_lane(700, 7_000, 100, 120.0, now)];
        snapshot.machine.archive_device_read_mib_per_sec = Some(70.0);
        snapshot.machine.archive_device_write_mib_per_sec = Some(40.0);
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                accepted_lanes: 1,
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &snapshot, &mut runtime, now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert!(tuner.guard_held);
        assert!(
            tuner
                .last_decision
                .as_deref()
                .is_some_and(|reason| reason.contains("archive-device throughput"))
        );
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(&config, LegacyTunerContext::Bulk, 1, tuner, now,),
            0,
        );
    }

    #[test]
    fn tuner_accepts_a_probe_when_aggregate_useful_throughput_improves() {
        let root = temp_root("legacy-tuner-accept");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let now = 10_000;
        let mut snapshot = empty_snapshot(false);
        snapshot.lanes = vec![
            useful_throughput_lane(700, 7_000, 100, 60.0, now),
            useful_throughput_lane(701, 7_001, 200, 60.0, now),
        ];
        let window = settled_tuner_window(&snapshot, now, 120.0);
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                phase: LegacyTunerPhase::ProbeUp {
                    baseline_lanes: 1,
                    baseline_mib_per_sec: 100.0,
                    probe_epoch: 701,
                },
                accepted_lanes: 1,
                accepted_useful_mib_per_sec: Some(100.0),
                window: Some(window),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &snapshot, &mut runtime, now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert_eq!(tuner.accepted_lanes, 2);
        assert_eq!(tuner.accepted_useful_mib_per_sec, Some(120.0));
        assert!(matches!(tuner.phase, LegacyTunerPhase::ObserveBaseline));
        assert!(tuner.pending_action.is_none());
        assert_eq!(
            tuner.next_probe_unix_secs,
            now + LEGACY_TUNER_PROBE_COOLDOWN_SECS
        );
    }

    #[test]
    fn tuner_rejects_a_flat_probe_and_enters_backoff_after_pause_verification() {
        let root = temp_root("legacy-tuner-backoff");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_io_budget_mib_per_sec = 0;
        let probe_now = 20_000;
        let mut probe_snapshot = empty_snapshot(false);
        probe_snapshot.lanes = vec![
            useful_throughput_lane(700, 7_000, 100, 34.0, probe_now),
            useful_throughput_lane(701, 7_001, 200, 35.0, probe_now),
            useful_throughput_lane(702, 7_002, 300, 35.0, probe_now),
        ];
        let probe_window = settled_tuner_window(&probe_snapshot, probe_now, 104.0);
        let mut runtime = RuntimeState {
            legacy_throughput_tuner: LegacyThroughputTuner {
                context: Some(LegacyTunerContext::Bulk),
                phase: LegacyTunerPhase::ProbeUp {
                    baseline_lanes: 2,
                    baseline_mib_per_sec: 100.0,
                    probe_epoch: 702,
                },
                accepted_lanes: 2,
                accepted_useful_mib_per_sec: Some(100.0),
                window: Some(probe_window),
                ..LegacyThroughputTuner::default()
            },
            ..RuntimeState::default()
        };

        observe_legacy_throughput_tuner(&config, &probe_snapshot, &mut runtime, probe_now);

        assert!(matches!(
            runtime.legacy_throughput_tuner.phase,
            LegacyTunerPhase::VerifyPause {
                baseline_lanes: 2,
                probe_epoch: 702,
                ..
            }
        ));
        assert!(matches!(
            runtime.legacy_throughput_tuner.pending_action.as_ref(),
            Some(LegacyTunerAction::Pause { epoch: 702, reason, .. })
                if reason.starts_with(LEGACY_TUNER_PAUSE_PREFIX)
        ));

        // Simulate the requested probe pause. Restoring the accepted two-lane
        // baseline confirms that the third lane had no useful marginal gain.
        let verify_now = probe_now + LEGACY_TUNER_SETTLE_SECS;
        let mut paused_snapshot = empty_snapshot(false);
        paused_snapshot.lanes = vec![
            useful_throughput_lane(700, 7_000, 100, 50.0, verify_now),
            useful_throughput_lane(701, 7_001, 200, 49.0, verify_now),
        ];
        runtime.legacy_throughput_tuner.pending_action = None;
        runtime.legacy_throughput_tuner.window =
            Some(settled_tuner_window(&paused_snapshot, verify_now, 99.0));

        observe_legacy_throughput_tuner(&config, &paused_snapshot, &mut runtime, verify_now);

        let tuner = &runtime.legacy_throughput_tuner;
        assert_eq!(tuner.accepted_lanes, 2);
        assert_eq!(tuner.accepted_useful_mib_per_sec, Some(99.0));
        assert!(matches!(tuner.phase, LegacyTunerPhase::Backoff));
        assert_eq!(
            tuner.backoff_until_unix_secs,
            verify_now + LEGACY_TUNER_BACKOFF_SECS
        );
        assert_eq!(
            legacy_tuner_capacity_limit_for_state(
                &config,
                LegacyTunerContext::Bulk,
                2,
                tuner,
                verify_now,
            ),
            2
        );
    }

    #[tokio::test]
    async fn zero_finalizer_overlap_uses_adaptive_one_lane_bootstrap() {
        let root = temp_root("legacy-finalizer-adaptive-overlap");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_finalizer_overlap = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_min_running = 3;
        config.legacy_compact_cpu_budget_cores = 12;
        config.legacy_compact_io_budget_mib_per_sec = 0;
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
        runtime.finalizer = Some(
            make_running_child(
                ChildKind::HistoricalFinalizer { epoch: 600 },
                root.join("finalizer-progress.json"),
                root.join("finalizer.log"),
            )
            .await,
        );

        assert_eq!(
            legacy_tuner_capacity_limit(&config, &snapshot, &runtime, unix_now()),
            1,
            "finalizer overlap starts with one measured probe, independent of bulk minimum"
        );
        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert_eq!(runtime.legacy_compacts.len(), 1);

        for child in runtime.legacy_compacts.values_mut() {
            let _ = child.child.wait().await;
        }
        if let Some(finalizer) = runtime.finalizer.as_mut() {
            let _ = finalizer.child.kill().await;
            let _ = finalizer.child.wait().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn active_finalizer_allows_only_configured_compact_overlap() {
        let root = temp_root("legacy-finalizer-overlap");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        allow_legacy_workers(&mut config, 3);
        config.legacy_compact_finalizer_overlap = 1;
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
        runtime.finalizer = Some(
            make_running_child(
                ChildKind::HistoricalFinalizer { epoch: 600 },
                root.join("finalizer-progress.json"),
                root.join("finalizer.log"),
            )
            .await,
        );

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert_eq!(runtime.legacy_compacts.len(), 1);
        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert_eq!(runtime.legacy_compacts.len(), 1);

        for child in runtime.legacy_compacts.values_mut() {
            let _ = child.child.wait().await;
        }
        if let Some(finalizer) = runtime.finalizer.as_mut() {
            let _ = finalizer.child.kill().await;
            let _ = finalizer.child.wait().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn legacy_top_up_starts_newest_ready_priority_epoch_first() {
        let root = temp_root("legacy-priority-top-up");
        let mut config = test_config(&root);
        config.blockzilla_bin = PathBuf::from("/usr/bin/true");
        config.priority_epoch_start = Some(863);
        config.priority_epoch_end = Some(899);
        allow_legacy_workers(&mut config, 1);
        let mut snapshot = schedulable_snapshot(
            &root,
            [700, 863, 865]
                .into_iter()
                .map(|epoch| test_epoch(&root, epoch, HistoricalState::Queued))
                .collect(),
        );
        for epoch in [700, 863, 865] {
            make_legacy_range_ready(&config, epoch);
        }
        snapshot.machine = legacy_scheduler_machine(&config, 4 * 1024, 4);
        let mut runtime = RuntimeState::default();

        assert_eq!(
            top_up_legacy_compacts(&config, &snapshot, &mut runtime, usize::MAX).await,
            1
        );
        assert_eq!(
            runtime.legacy_compacts.keys().copied().collect::<Vec<_>>(),
            vec![865]
        );
        let _ = runtime
            .legacy_compacts
            .get_mut(&865)
            .unwrap()
            .child
            .wait()
            .await;
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
    async fn admissible_historical_finalizer_stops_legacy_refill_until_drain() {
        let root = temp_root("legacy-drain-for-historical-finalizer");
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
        snapshot.machine = legacy_scheduler_machine(&config, 2 * 1024, 2);
        let adopted_rss = 512 * 1024 * 1024;
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
            kind: "historical".to_string(),
            epoch: Some(900),
            id: "historical:900".to_string(),
            phase: "mphf".to_string(),
            state: "ready".to_string(),
            estimated_memory_bytes: 512 * 1024 * 1024,
            estimated_disk_bytes: 1024 * 1024 * 1024,
            deferred_reason: None,
        });
        let mut runtime = RuntimeState::default();

        schedule_work(&config, &snapshot, &mut runtime)
            .await
            .unwrap();
        assert!(runtime.legacy_compacts.is_empty());
        assert!(runtime.finalizer.is_none());
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
                std::ffi::OsString::from(LEGACY_CAR_DOWNLOAD_ARGV0),
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
    fn controls_require_execute_mode_and_local_management_api() {
        let root = temp_root("controls");
        let mut config = test_config(&root);
        assert!(matches!(
            authorize_control(&config),
            Err(ControlError::Disabled(_))
        ));

        config.execute = true;
        assert!(matches!(
            authorize_control(&config),
            Err(ControlError::Disabled(_))
        ));
        config.management_bind = Some("127.0.0.1:8788".parse().unwrap());
        assert!(authorize_control(&config).is_ok());
    }

    #[test]
    fn management_api_rejects_non_loopback_bind() {
        assert!(validate_management_bind(None).is_ok());
        assert!(validate_management_bind(Some("127.0.0.1:8788".parse().unwrap())).is_ok());
        assert!(validate_management_bind(Some("[::1]:8788".parse().unwrap())).is_ok());
        assert!(validate_management_bind(Some("127.0.0.1:0".parse().unwrap())).is_err());
        assert!(validate_management_bind(Some("0.0.0.0:8788".parse().unwrap())).is_err());
    }

    #[test]
    fn management_requests_require_loopback_host_and_non_simple_content_type() {
        let root = temp_root("management-request-headers");
        let mut config = test_config(&root);
        config.execute = true;
        config.management_bind = Some("127.0.0.1:8788".parse().unwrap());

        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "127.0.0.1:8788".parse().unwrap());
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        assert!(authorize_management_request(&config, &headers).is_ok());

        headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
        assert!(authorize_management_request(&config, &headers).is_err());
        headers.insert(
            header::CONTENT_TYPE,
            "application/json; charset=utf-8".parse().unwrap(),
        );
        headers.insert(header::HOST, "localhost:8788".parse().unwrap());
        assert!(authorize_management_request(&config, &headers).is_err());
    }

    #[test]
    fn management_requests_reject_cross_origin_browser_contexts() {
        let root = temp_root("management-request-origin");
        let mut config = test_config(&root);
        config.execute = true;
        config.management_bind = Some("[::1]:8788".parse().unwrap());

        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "[::1]:8788".parse().unwrap());
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(header::ORIGIN, "http://[::1]:8788".parse().unwrap());
        headers.insert("sec-fetch-site", "same-origin".parse().unwrap());
        assert!(authorize_management_request(&config, &headers).is_ok());

        headers.insert(header::ORIGIN, "https://attacker.example".parse().unwrap());
        assert!(authorize_management_request(&config, &headers).is_err());
        headers.insert(header::ORIGIN, "http://[::1]:8788".parse().unwrap());
        headers.insert("sec-fetch-site", "cross-site".parse().unwrap());
        assert!(authorize_management_request(&config, &headers).is_err());
    }

    #[test]
    fn parses_linux_psi_avg10() {
        let pressure = "some avg10=12.34 avg60=4.00 avg300=1.00 total=9\nfull avg10=5.67 avg60=2.00 avg300=0.50 total=3\n";
        assert_eq!(parse_psi_avg10(pressure, "some"), Some(12.34));
        assert_eq!(parse_psi_avg10(pressure, "full"), Some(5.67));
        assert_eq!(parse_psi_avg10(pressure, "missing"), None);
        assert_eq!(parse_psi_avg10("full avg10=NaN total=1", "full"), None);
    }

    #[test]
    fn cpu_budget_is_a_dynamic_pressure_guard_with_resume_hysteresis() {
        let root = temp_root("adaptive-cpu-pressure");
        let mut config = test_config(&root);
        config.legacy_compact_concurrency = 0;
        config.legacy_compact_auto_pause = true;
        config.legacy_compact_cpu_budget_cores = 4;
        let mut machine = MachineSnapshot {
            memory_total_bytes: 16 * 1024 * 1024 * 1024,
            memory_available_bytes: 8 * 1024 * 1024 * 1024,
            io_pressure_full_avg10: None,
            load_1m: 4.0,
            ..MachineSnapshot::default()
        };

        assert!(matches!(
            legacy_pressure_state(&config, &machine),
            LegacyPressureState::Pause(reason)
                if reason.contains("load average 4.00 reached CPU load ceiling 4.00")
        ));

        machine.load_1m = 3.6;
        assert_eq!(
            legacy_pressure_state(&config, &machine),
            LegacyPressureState::Hold
        );

        machine.load_1m = 3.4;
        assert_eq!(
            legacy_pressure_state(&config, &machine),
            LegacyPressureState::Resume
        );
    }

    #[test]
    fn adaptive_decision_applies_pressure_hysteresis_cooldown_and_lane_order() {
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
            plan_legacy_adaptive_action(&config, &machine, 3, &[700, 900, 800], &[], 0, 100,),
            Some(LegacyAdaptiveDecision::Pause {
                epoch: 900,
                reason: "IO PSI full avg10 25.00 reached pause threshold 20.00".to_string(),
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

        machine.io_pressure_full_avg10 = Some(10.0);
        assert_eq!(
            legacy_pressure_state(&config, &machine),
            LegacyPressureState::Hold
        );
        machine.io_pressure_full_avg10 = Some(5.0);
        machine.memory_available_bytes = (resume_mib - 1) * mib;
        assert_eq!(
            legacy_pressure_state(&config, &machine),
            LegacyPressureState::Hold
        );

        machine.memory_available_bytes = resume_mib * mib;
        machine.io_pressure_full_avg10 = None;
        let paused = vec![
            AutoPausedLegacy {
                epoch: 800,
                pid: 8,
                process_start_ticks: None,
                reason: "pressure".to_string(),
                paused_unix_secs: 80,
            },
            AutoPausedLegacy {
                epoch: 900,
                pid: 9,
                process_start_ticks: None,
                reason: "pressure".to_string(),
                paused_unix_secs: 70,
            },
        ];
        assert_eq!(
            plan_legacy_adaptive_action(&config, &machine, 1, &[], &paused, 0, 100),
            Some(LegacyAdaptiveDecision::Resume {
                epoch: 900,
                reason: "MemAvailable and IO PSI crossed resume thresholds".to_string(),
            }),
            "missing PSI is unknown and the oldest auto-pause resumes first"
        );

        config.priority_epoch_start = Some(863);
        config.priority_epoch_end = Some(899);
        let priority_paused = vec![
            AutoPausedLegacy {
                epoch: 700,
                pid: 7,
                process_start_ticks: None,
                reason: "pressure".to_string(),
                paused_unix_secs: 60,
            },
            AutoPausedLegacy {
                epoch: 865,
                pid: 8,
                process_start_ticks: None,
                reason: "pressure".to_string(),
                paused_unix_secs: 80,
            },
        ];
        assert_eq!(
            plan_legacy_adaptive_action(&config, &machine, 1, &[], &priority_paused, 0, 100,),
            Some(LegacyAdaptiveDecision::Resume {
                epoch: 865,
                reason: "MemAvailable and IO PSI crossed resume thresholds".to_string(),
            }),
            "a preferred epoch resumes before an older ordinary pause"
        );
        machine.io_pressure_full_avg10 = Some(25.0);
        assert!(matches!(
            plan_legacy_adaptive_action(&config, &machine, 2, &[700, 865], &[], 0, 100),
            Some(LegacyAdaptiveDecision::Pause { epoch: 700, .. })
        ));

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
            },
        );
        set_manual_pause_state(&mut runtime, "compact_reuse:700", Some(700), true);
        assert!(runtime.paused_jobs.contains("compact_reuse:700"));
        assert!(!runtime.auto_paused_legacy.contains_key(&700));
        set_manual_pause_state(&mut runtime, "compact_reuse:700", Some(700), false);
        assert!(!runtime.paused_jobs.contains("compact_reuse:700"));
        assert!(!runtime.auto_paused_legacy.contains_key(&700));
    }

    #[test]
    fn adopted_legacy_lane_preserves_reported_progress_phase() {
        let root = temp_root("adopted-phase");
        let progress_path = root.join("epoch-700.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            &progress_path,
            br#"{"phase":"Archive V2 Hot Write","state":"running","updated_unix_secs":10}"#,
        )
        .unwrap();
        let compact = AdoptedLegacyCompact {
            epoch: 700,
            pid: u32::MAX - 100,
            owner_schema_version: SCHEMA_VERSION,
            process_start_ticks: 1,
            progress_path,
            identity_tainted: false,
        };

        let lane = lane_from_adopted_legacy(&compact, 10, &RuntimeState::default());
        assert_eq!(lane.phase, "Archive V2 Hot Write");
        assert_eq!(lane.progress.phase.as_deref(), Some("Archive V2 Hot Write"));
        fs::remove_dir_all(root).unwrap();
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
            top_up_legacy_compacts(&config, &snapshot, &mut runtime, usize::MAX).await,
            1
        );
        assert_eq!(runtime.legacy_compacts.len(), 1);
        assert_eq!(
            top_up_legacy_compacts(&config, &snapshot, &mut runtime, usize::MAX).await,
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

    #[cfg(target_os = "linux")]
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

    #[cfg(target_os = "linux")]
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

    #[cfg(target_os = "linux")]
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

    #[cfg(target_os = "linux")]
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
        let bin = Path::new("/test/bin/blockzilla");
        let output = Path::new("/test/archive/epoch-700");
        let scanner = b"/test/bin/blockzilla\0build-archive-v2-hot-blocks\0/test/cars/epoch-700.car.zst\0/test/archive/epoch-700\0--first-seen-registry\0--first-seen-scan-only\0";
        assert!(argv_matches_job(scanner, bin, output, "historical_scan"));
        assert!(!argv_matches_job(
            scanner,
            bin,
            output,
            "historical_finalizer"
        ));

        let finalizer = b"/test/bin/blockzilla\0finalize-archive-v2-first-seen\0/test/archive/epoch-700\0--finalizer-lock\0/test/state/finalizer.lock\0";
        assert!(argv_matches_job(
            finalizer,
            bin,
            output,
            "historical_finalizer"
        ));
        assert!(!argv_matches_job(finalizer, bin, output, "historical_scan"));

        for live in [
            b"/test/bin/blockzilla\0prepare-archive-v2-live-registry\0/test/live/capture\0/test/archive/epoch-700\0".as_slice(),
            b"/test/bin/blockzilla\0build-archive-v2-registry-index\0/test/archive/epoch-700/registry.bin\0--output\0/test/archive/epoch-700/registry.mphf\0".as_slice(),
            b"/test/bin/blockzilla\0build-archive-v2-hot-blocks-from-live\0/test/live/capture\0/test/archive/epoch-700\0--registry-source\0runs\0".as_slice(),
        ] {
            assert!(argv_matches_job(live, bin, output, "live_finalizer"));
            assert!(!argv_matches_job(live, bin, output, "historical_finalizer"));
        }

        let wrong_path = b"/test/bin/blockzilla\0finalize-archive-v2-first-seen\0/test/archive/epoch-700-extra\0";
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
