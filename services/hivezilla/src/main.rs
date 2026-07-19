use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use hivezilla::{
    app::LiveProducerApp,
    config::ProducerConfig,
    epoch::{OLD_FAITHFUL_SLOTS_PER_EPOCH, plan_epoch_backfill},
    fixture_bench::{
        GrpcFixtureBenchConfig, GrpcFixtureBlockWriteStrategy, GrpcFixtureDecodeMode,
        GrpcFixtureHashBackend, GrpcFixturePubkeyStringCache, GrpcFixtureWriteMode,
        bench_grpc_fixture,
    },
    grpc::{
        GrpcCaptureConfig, GrpcCompactLogsBackfillConfig, GrpcEpochCommitment,
        GrpcEpochWatchConfig, GrpcProbeConfig, GrpcPubkeyIndexMode, GrpcPubkeyRunBackfillConfig,
        GrpcRawBlockStorage, backfill_compact_logs, backfill_pubkey_runs, capture_grpc_blocks,
        inspect_capture, probe_grpc, watch_grpc_epoch_boundaries,
    },
    grpc_raw::{
        GrpcRawMaterializeConfig, GrpcRawRecordConfig, inspect_grpc_raw_blocks,
        materialize_grpc_raw_blocks, record_grpc_raw_blocks, verify_grpc_raw_poh,
    },
    ingest::IngestConfig,
    repair::{EpochRepairCaptureSlice, PrepareEpochRepairConfig, prepare_epoch_repair},
    rpc::{
        RpcBackfillConfig, RpcEpochSyncConfig, RpcRateLimitConfig, backfill_get_blocks,
        sync_epoch_info,
    },
    supervisor::{
        BackoffPolicy, RestartPolicy, SupervisorConfig, SupervisorNotificationKind,
        notify_supervisor, run_supervisor,
    },
};
use std::{collections::BTreeSet, ffi::OsString, time::Duration};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(name = "hivezilla")]
#[command(about = "Build Blockzilla archives from live feeds with CAR repair support")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init(ProducerConfig),
    Plan(ProducerConfig),
    Run(RunArgs),
    ProbeGrpc(ProbeGrpcArgs),
    CaptureGrpc(CaptureGrpcArgs),
    /// Durably record confirmed protobuf update envelopes without archive conversion or sidecars.
    RecordGrpcRaw(RecordGrpcRawArgs),
    /// Inspect or fully validate an independently compressed raw gRPC spool.
    InspectGrpcRaw(InspectGrpcRawArgs),
    /// Replay a raw gRPC spool and require complete, reconstructable PoH entries in every block.
    VerifyGrpcRawPoh(VerifyGrpcRawPohArgs),
    /// Materialize one committed epoch slice from a stopped raw spool into a staged capture.
    MaterializeGrpcRaw(MaterializeGrpcRawArgs),
    SyncRpcEpoch(SyncRpcEpochArgs),
    WatchEpochsGrpc(WatchEpochsGrpcArgs),
    PlanEpochBackfill(PlanEpochBackfillArgs),
    BackfillRpc(BackfillRpcArgs),
    InspectCapture(InspectCaptureArgs),
    BackfillPubkeyRuns(BackfillPubkeyRunsArgs),
    BackfillCompactLogs(BackfillCompactLogsArgs),
    /// Atomically consolidate audited live capture slices and RPC-only repair sidecars.
    PrepareEpochRepair(PrepareEpochRepairArgs),
    BenchFixture(BenchFixtureArgs),
    /// Validate a redundant-ingest JSON config and print only its redacted summary.
    ValidateIngestConfig(ValidateIngestConfigArgs),
    /// Portably supervise one long-lived service with bounded restart and health policy.
    Supervise(SuperviseArgs),
    /// Notify a parent Hivezilla supervisor of readiness or one heartbeat.
    NotifySupervisor(NotifySupervisorArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    #[command(flatten)]
    config: ProducerConfig,

    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
struct ValidateIngestConfigArgs {
    #[arg(long)]
    config: std::path::PathBuf,
}

#[derive(Debug, Args)]
struct SuperviseArgs {
    #[arg(long)]
    name: String,

    #[arg(long)]
    state_dir: std::path::PathBuf,

    #[arg(long)]
    working_dir: Option<std::path::PathBuf>,

    #[arg(long, value_enum, default_value_t = RestartPolicyArg::OnFailure)]
    restart: RestartPolicyArg,

    #[arg(long, default_value_t = 1_000)]
    initial_backoff_ms: u64,

    #[arg(long, default_value_t = 60_000)]
    max_backoff_ms: u64,

    /// Integer fixed point: 2000 means 2.0x.
    #[arg(long, default_value_t = 2_000)]
    backoff_factor_milli: u32,

    /// Maximum restarts permitted inside the restart window.
    #[arg(long, default_value_t = 5)]
    restart_burst: usize,

    #[arg(long, default_value_t = 60)]
    restart_window_secs: u64,

    /// Uptime that resets crash-loop history and exponential backoff.
    #[arg(long, default_value_t = 300)]
    healthy_after_secs: u64,

    /// Require a tokenized ready notification within this time. Zero disables.
    #[arg(long, default_value_t = 0)]
    readiness_timeout_secs: u64,

    /// Require notifications to keep advancing after readiness. Zero disables.
    #[arg(long, default_value_t = 0)]
    heartbeat_timeout_secs: u64,

    #[arg(long, default_value_t = 45)]
    stop_timeout_secs: u64,

    /// Executable and arguments, placed after `--`.
    #[arg(last = true, required = true, num_args = 1.., allow_hyphen_values = true)]
    command: Vec<OsString>,
}

impl SuperviseArgs {
    fn into_config(mut self) -> Result<SupervisorConfig> {
        anyhow::ensure!(
            !self.command.is_empty(),
            "supervise requires a command after --"
        );
        let program = self.command.remove(0);
        Ok(SupervisorConfig {
            name: self.name,
            state_dir: self.state_dir,
            program,
            args: self.command,
            working_dir: self.working_dir,
            restart_policy: self.restart.into(),
            backoff: BackoffPolicy {
                initial: Duration::from_millis(self.initial_backoff_ms),
                maximum: Duration::from_millis(self.max_backoff_ms),
                factor_milli: self.backoff_factor_milli,
            },
            restart_burst: self.restart_burst,
            restart_window: Duration::from_secs(self.restart_window_secs),
            healthy_after: Duration::from_secs(self.healthy_after_secs),
            readiness_timeout: Duration::from_secs(self.readiness_timeout_secs),
            heartbeat_timeout: Duration::from_secs(self.heartbeat_timeout_secs),
            stop_timeout: Duration::from_secs(self.stop_timeout_secs),
        })
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RestartPolicyArg {
    Never,
    OnFailure,
    Always,
}

impl From<RestartPolicyArg> for RestartPolicy {
    fn from(value: RestartPolicyArg) -> Self {
        match value {
            RestartPolicyArg::Never => Self::Never,
            RestartPolicyArg::OnFailure => Self::OnFailure,
            RestartPolicyArg::Always => Self::Always,
        }
    }
}

#[derive(Debug, Args)]
struct NotifySupervisorArgs {
    #[arg(value_enum)]
    kind: SupervisorNotificationKindArg,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SupervisorNotificationKindArg {
    Ready,
    Heartbeat,
}

impl From<SupervisorNotificationKindArg> for SupervisorNotificationKind {
    fn from(value: SupervisorNotificationKindArg) -> Self {
        match value {
            SupervisorNotificationKindArg::Ready => Self::Ready,
            SupervisorNotificationKindArg::Heartbeat => Self::Heartbeat,
        }
    }
}

#[derive(Debug, Args)]
struct ProbeGrpcArgs {
    #[arg(long)]
    endpoint: String,

    #[arg(long, default_value_t = 64)]
    max_updates: usize,

    #[arg(long, default_value_t = 20)]
    timeout_secs: u64,
}

#[derive(Debug, Args)]
struct CaptureGrpcArgs {
    #[arg(long)]
    endpoint: String,

    #[arg(long, default_value = "blockzilla-live")]
    archive_dir: std::path::PathBuf,

    #[arg(long, default_value_t = 1)]
    max_blocks: usize,

    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,

    #[arg(long)]
    from_slot: Option<u64>,

    #[arg(long, default_value_t = OLD_FAITHFUL_SLOTS_PER_EPOCH)]
    slots_per_epoch: u64,

    #[arg(long)]
    stop_at_epoch_boundary: bool,

    #[arg(long, value_enum, default_value_t = RawBlockStorageArg::All)]
    raw_block_storage: RawBlockStorageArg,

    #[arg(long, value_enum, default_value_t = PubkeyIndexModeArg::Runs)]
    pubkey_index_mode: PubkeyIndexModeArg,

    /// Previous registry.bin to keep its first N pubkeys in an exact in-memory hot cache for runs mode.
    #[arg(long)]
    pubkey_hot_registry: Option<std::path::PathBuf>,

    /// Number of previous-registry pubkeys to keep in the exact hot cache.
    #[arg(long, default_value_t = 1000)]
    pubkey_hot_count: usize,
}

#[derive(Debug, Args)]
struct RecordGrpcRawArgs {
    #[arg(long)]
    endpoint: String,

    #[arg(long, default_value = "blockzilla-grpc-raw")]
    output_dir: std::path::PathBuf,

    #[arg(long, default_value_t = 1_000_000)]
    max_blocks: usize,

    #[arg(long, default_value_t = 86_400)]
    timeout_secs: u64,

    /// Exit so a supervisor can reconnect after this many seconds without a durable block.
    /// Zero disables the idle watchdog.
    #[arg(long, default_value_t = 180)]
    idle_timeout_secs: u64,

    #[arg(long)]
    from_slot: Option<u64>,

    /// Atomically publish a secret-free JSON event if the provider skips the inclusive resume slot.
    #[arg(long)]
    resume_coverage_warning_file: Option<std::path::PathBuf>,

    #[arg(long, default_value_t = OLD_FAITHFUL_SLOTS_PER_EPOCH)]
    slots_per_epoch: u64,

    #[arg(long)]
    stop_at_epoch_boundary: bool,

    /// Independent per-update zstd compression level. Level 1 is fast and restart-safe.
    #[arg(long, default_value_t = 1)]
    compression_level: i32,

    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    segment_target_bytes: u64,

    /// Per-update protobuf/WAL limit and tonic decoded-message ceiling.
    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    max_record_bytes: u64,

    /// Stop cleanly before the filesystem falls below this many free bytes. Zero disables.
    #[arg(long, default_value_t = 16 * 1024 * 1024 * 1024)]
    min_free_bytes: u64,

    /// Reject a block unless its embedded entries form complete, reconstructable PoH.
    #[arg(long)]
    require_complete_poh: bool,

    #[arg(long, default_value = "solana-mainnet")]
    cluster_id: String,

    #[arg(long, default_value = "mac-bridge")]
    origin_node_id: String,

    #[arg(long, default_value = "grpc-raw")]
    source_id: String,
}

#[derive(Debug, Args)]
struct InspectGrpcRawArgs {
    #[arg(long, default_value = "blockzilla-grpc-raw")]
    output_dir: std::path::PathBuf,

    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    max_record_bytes: u64,

    /// Decompress, checksum, and prost-decode every record while retaining only one block.
    #[arg(long)]
    verify_payloads: bool,
}

#[derive(Debug, Args)]
struct VerifyGrpcRawPohArgs {
    #[arg(long, default_value = "blockzilla-grpc-raw")]
    output_dir: std::path::PathBuf,

    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    max_record_bytes: u64,

    /// Fail unless at least this many complete records are verified. Zero permits an empty spool.
    #[arg(long, default_value_t = 1)]
    min_records: u64,
}

#[derive(Debug, Args)]
struct MaterializeGrpcRawArgs {
    /// Stopped raw-recorder directory or a filesystem snapshot of it.
    #[arg(long)]
    input_dir: std::path::PathBuf,

    /// New capture directory. Neither it nor its deterministic staging sibling may exist.
    #[arg(long)]
    archive_dir: std::path::PathBuf,

    /// Epoch to select from the immutable raw spool snapshot.
    #[arg(long)]
    epoch: u64,

    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    max_record_bytes: u64,

    /// Previous registry.bin used only as a bounded hot-key cache for pubkey runs.
    #[arg(long)]
    pubkey_hot_registry: Option<std::path::PathBuf>,

    #[arg(long, default_value_t = 1000)]
    pubkey_hot_count: usize,
}

#[derive(Debug, Args)]
struct SyncRpcEpochArgs {
    #[arg(long)]
    rpc_url: String,

    #[arg(long, default_value = "finalized")]
    commitment: String,

    #[arg(long, default_value_t = 10)]
    timeout_secs: u64,

    #[command(flatten)]
    rpc_client: RpcClientArgs,
}

#[derive(Debug, Args)]
struct WatchEpochsGrpcArgs {
    #[arg(long)]
    endpoint: String,

    /// Optional JSON-RPC URL used once at startup to seed current epoch state.
    #[arg(long)]
    startup_rpc_url: Option<String>,

    #[arg(long, default_value = "finalized")]
    startup_rpc_commitment: String,

    #[command(flatten)]
    startup_rpc_client: RpcClientArgs,

    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,

    /// Stop after this many stream updates. Zero means no update cap.
    #[arg(long, default_value_t = 0)]
    max_updates: usize,

    #[arg(long, default_value_t = 1)]
    max_boundaries: usize,

    #[arg(long)]
    from_slot: Option<u64>,

    #[arg(long, default_value_t = OLD_FAITHFUL_SLOTS_PER_EPOCH)]
    slots_per_epoch: u64,

    #[arg(long, value_enum, default_value_t = GrpcEpochCommitmentArg::Finalized)]
    commitment: GrpcEpochCommitmentArg,
}

#[derive(Debug, Args)]
struct PlanEpochBackfillArgs {
    #[arg(long)]
    epoch: u64,

    /// Slots already present in the live archive/index. Repeat the flag for many slots.
    #[arg(long = "observed-slot")]
    observed_slots: Vec<u64>,

    /// Inclusive slot ranges already present, written as START-END.
    #[arg(long = "observed-range")]
    observed_ranges: Vec<SlotRangeArg>,

    #[arg(long, default_value_t = OLD_FAITHFUL_SLOTS_PER_EPOCH)]
    slots_per_epoch: u64,
}

#[derive(Debug, Args)]
struct BackfillRpcArgs {
    #[arg(long)]
    rpc_url: String,

    #[arg(long, default_value = "blockzilla-live")]
    archive_dir: std::path::PathBuf,

    #[arg(long)]
    start_slot: u64,

    #[arg(long)]
    end_slot: u64,

    #[arg(long, default_value = "finalized")]
    commitment: String,

    #[arg(long, default_value_t = 30)]
    timeout_secs: u64,

    #[arg(long, default_value_t = OLD_FAITHFUL_SLOTS_PER_EPOCH)]
    slots_per_epoch: u64,

    #[arg(long, default_value_t = true)]
    skip_existing: bool,

    #[command(flatten)]
    rpc_client: RpcClientArgs,
}

#[derive(Debug, Args)]
struct InspectCaptureArgs {
    #[arg(long, default_value = "blockzilla-live")]
    archive_dir: std::path::PathBuf,
}

#[derive(Debug, Args)]
struct BackfillPubkeyRunsArgs {
    #[arg(long, default_value = "blockzilla-live")]
    archive_dir: std::path::PathBuf,

    /// Directory that receives run-*.bin files. Defaults to ARCHIVE/index/pubkey-runs.
    #[arg(long)]
    output_run_dir: Option<std::path::PathBuf>,

    /// First block id to scan. Non-zero values seek through index/block-index.bin.
    #[arg(long, default_value_t = 0)]
    start_block_id: u32,

    #[arg(long)]
    max_blocks: Option<usize>,

    /// Remove the output run directory before writing.
    #[arg(long)]
    reset_output_dir: bool,

    /// Previous registry.bin to keep its first N pubkeys in an exact in-memory hot cache.
    #[arg(long)]
    pubkey_hot_registry: Option<std::path::PathBuf>,

    /// Number of previous-registry pubkeys to keep in the exact hot cache.
    #[arg(long, default_value_t = 1000)]
    pubkey_hot_count: usize,
}

#[derive(Debug, Args)]
struct BackfillCompactLogsArgs {
    /// Existing Hivezilla capture directory to read.
    #[arg(long, default_value = "blockzilla-live")]
    archive_dir: std::path::PathBuf,

    /// New capture directory to write with compact-log block frames.
    #[arg(long)]
    output_archive_dir: std::path::PathBuf,

    /// Stop after N blocks. Intended for smoke tests.
    #[arg(long)]
    max_blocks: Option<usize>,

    /// First block id to scan. Non-zero values seek through input index/block-index.bin.
    #[arg(long, default_value_t = 0)]
    start_block_id: u32,

    /// Append compacted blocks/index rows to an existing output capture.
    #[arg(long)]
    append_output: bool,

    /// Remove output_archive_dir before writing.
    #[arg(long)]
    overwrite_output: bool,
}

#[derive(Debug, Args)]
struct PrepareEpochRepairArgs {
    /// Capture directory. Repeat in the same order as the other capture-* arguments.
    #[arg(long = "capture-dir", required = true)]
    capture_dirs: Vec<std::path::PathBuf>,

    /// Exact number of index/PoH rows selected from the corresponding capture.
    #[arg(long = "capture-blocks", required = true)]
    capture_blocks: Vec<u64>,

    /// Inclusive last selected slot in the corresponding capture.
    #[arg(long = "capture-max-slot", required = true)]
    capture_max_slots: Vec<u64>,

    /// Prebuilt bounded pubkey-run directory for the corresponding capture slice.
    #[arg(long = "capture-pubkey-runs", required = true)]
    capture_pubkey_runs: Vec<std::path::PathBuf>,

    /// Capture journal JSONL corresponding to the selected capture.
    #[arg(long = "capture-journal", required = true)]
    capture_journals: Vec<std::path::PathBuf>,

    /// Audited/sealed receipt corresponding to the selected capture slice.
    #[arg(long = "capture-sealed-marker", required = true)]
    capture_sealed_markers: Vec<std::path::PathBuf>,

    /// Directory containing slot-N.getBlock.json files. Repeat to combine repair sources.
    #[arg(long = "rpc-repair-dir")]
    rpc_repair_dirs: Vec<std::path::PathBuf>,

    /// Completion marker created only after every RPC repair worker has exited.
    #[arg(long)]
    rpc_complete_marker: std::path::PathBuf,

    #[arg(long)]
    output_dir: std::path::PathBuf,

    #[arg(long)]
    epoch: u64,

    #[arg(long, default_value_t = OLD_FAITHFUL_SLOTS_PER_EPOCH)]
    slots_per_epoch: u64,

    /// Exact unique live-block union expected after overlap deduplication.
    #[arg(long)]
    expected_live_blocks: u64,

    /// Exact RPC-only block count required before the view may be published.
    #[arg(long)]
    expected_rpc_blocks: u64,

    /// Exact live overlap count expected across selected capture slices.
    #[arg(long)]
    expected_duplicate_live_blocks: u64,

    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    max_rpc_json_bytes: u64,
}

#[derive(Debug, Args)]
struct BenchFixtureArgs {
    #[arg(long, default_value = "blockzilla-v1/live-grpc-100")]
    archive_dir: std::path::PathBuf,

    #[arg(long, default_value_t = 3)]
    iterations: usize,

    #[arg(long)]
    max_blocks: Option<usize>,

    #[arg(long, value_enum, default_value_t = BenchDecodeModeArg::Prost)]
    decode_mode: BenchDecodeModeArg,

    #[arg(long, default_value_t = 0)]
    initial_pubkey_capacity: usize,

    #[arg(long = "heavy-hitter-capacity", value_delimiter = ',')]
    heavy_hitter_capacities: Vec<usize>,

    #[arg(long, value_enum, default_value_t = BenchPubkeyStringCacheArg::Enabled)]
    pubkey_string_cache: BenchPubkeyStringCacheArg,

    #[arg(long)]
    log_parse_stats: bool,

    #[arg(long, value_enum, default_value_t = BenchHashBackendArg::All)]
    hash_backend: BenchHashBackendArg,

    #[arg(long, value_enum, default_value_t = BenchWriteModeArg::None)]
    write_mode: BenchWriteModeArg,

    #[arg(long, value_enum, default_value_t = BenchBlockWriteStrategyArg::All)]
    block_write_strategy: BenchBlockWriteStrategyArg,

    #[arg(long, default_value = "target/hivezilla-bench")]
    output_dir: std::path::PathBuf,
}

#[derive(Debug, Clone, ValueEnum)]
enum BenchHashBackendArg {
    All,
    StdRandom,
    StdAhash,
    StdFxhash,
    HashbrownAhash,
    GxhashU64,
    GxhashU32,
}

#[derive(Debug, Clone, ValueEnum)]
enum BenchDecodeModeArg {
    Prost,
    Borrowed,
    BorrowedFast,
}

#[derive(Debug, Clone, ValueEnum)]
enum BenchWriteModeArg {
    None,
    Archive,
}

#[derive(Debug, Clone, ValueEnum)]
enum BenchBlockWriteStrategyArg {
    All,
    Current,
    ScratchOnce,
}

#[derive(Debug, Clone, ValueEnum)]
enum BenchPubkeyStringCacheArg {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, ValueEnum)]
enum GrpcEpochCommitmentArg {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RawBlockStorageArg {
    All,
    Failure,
    None,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PubkeyIndexModeArg {
    Counts,
    Touches,
    Runs,
    CountsAndTouches,
    CountsAndRuns,
    None,
}

#[derive(Debug, Clone)]
struct SlotRangeArg {
    start_slot: u64,
    end_slot: u64,
}

#[derive(Debug, Clone, Args)]
struct RpcClientArgs {
    /// Client-side RPC pacing. Zero disables local pacing.
    #[arg(long, default_value_t = 0.0)]
    rpc_rate_limit_per_sec: f64,

    /// Honor server rate-limit responses such as HTTP 429 and Retry-After.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    rpc_follow_rate_limit: bool,

    /// Retries allowed after server rate-limit responses.
    #[arg(long, default_value_t = 8)]
    rpc_rate_limit_retries: u32,

    /// Base retry delay when server does not provide Retry-After.
    #[arg(long, default_value_t = 1_000)]
    rpc_rate_limit_base_delay_ms: u64,
}

impl From<ProbeGrpcArgs> for GrpcProbeConfig {
    fn from(value: ProbeGrpcArgs) -> Self {
        Self {
            endpoint: value.endpoint,
            max_updates: value.max_updates,
            timeout_secs: value.timeout_secs,
        }
    }
}

impl From<WatchEpochsGrpcArgs> for GrpcEpochWatchConfig {
    fn from(value: WatchEpochsGrpcArgs) -> Self {
        Self {
            endpoint: value.endpoint,
            startup_rpc_url: value.startup_rpc_url,
            startup_rpc_commitment: value.startup_rpc_commitment,
            startup_rpc_rate_limit: value.startup_rpc_client.into(),
            timeout_secs: value.timeout_secs,
            max_updates: value.max_updates,
            max_boundaries: value.max_boundaries,
            from_slot: value.from_slot,
            slots_per_epoch: value.slots_per_epoch,
            commitment: value.commitment.into(),
        }
    }
}

impl From<GrpcEpochCommitmentArg> for GrpcEpochCommitment {
    fn from(value: GrpcEpochCommitmentArg) -> Self {
        match value {
            GrpcEpochCommitmentArg::Processed => Self::Processed,
            GrpcEpochCommitmentArg::Confirmed => Self::Confirmed,
            GrpcEpochCommitmentArg::Finalized => Self::Finalized,
        }
    }
}

impl From<CaptureGrpcArgs> for GrpcCaptureConfig {
    fn from(value: CaptureGrpcArgs) -> Self {
        Self {
            endpoint: value.endpoint,
            archive_dir: value.archive_dir,
            max_blocks: value.max_blocks,
            timeout_secs: value.timeout_secs,
            from_slot: value.from_slot,
            slots_per_epoch: value.slots_per_epoch,
            stop_at_epoch_boundary: value.stop_at_epoch_boundary,
            raw_block_storage: value.raw_block_storage.into(),
            pubkey_index_mode: value.pubkey_index_mode.into(),
            pubkey_hot_registry_path: value.pubkey_hot_registry,
            pubkey_hot_count: value.pubkey_hot_count,
        }
    }
}

impl From<RecordGrpcRawArgs> for GrpcRawRecordConfig {
    fn from(value: RecordGrpcRawArgs) -> Self {
        Self {
            endpoint: value.endpoint,
            output_dir: value.output_dir,
            max_blocks: value.max_blocks,
            timeout_secs: value.timeout_secs,
            idle_timeout_secs: value.idle_timeout_secs,
            from_slot: value.from_slot,
            resume_coverage_warning_file: value.resume_coverage_warning_file,
            slots_per_epoch: value.slots_per_epoch,
            stop_at_epoch_boundary: value.stop_at_epoch_boundary,
            compression_level: value.compression_level,
            segment_target_bytes: value.segment_target_bytes,
            max_record_bytes: value.max_record_bytes,
            min_free_bytes: value.min_free_bytes,
            require_complete_poh: value.require_complete_poh,
            cluster_id: value.cluster_id,
            origin_node_id: value.origin_node_id,
            source_id: value.source_id,
        }
    }
}

impl From<RawBlockStorageArg> for GrpcRawBlockStorage {
    fn from(value: RawBlockStorageArg) -> Self {
        match value {
            RawBlockStorageArg::All => Self::All,
            RawBlockStorageArg::Failure => Self::Failure,
            RawBlockStorageArg::None => Self::None,
        }
    }
}

impl From<PubkeyIndexModeArg> for GrpcPubkeyIndexMode {
    fn from(value: PubkeyIndexModeArg) -> Self {
        match value {
            PubkeyIndexModeArg::Counts => Self::Counts,
            PubkeyIndexModeArg::Touches => Self::Touches,
            PubkeyIndexModeArg::Runs => Self::Runs,
            PubkeyIndexModeArg::CountsAndTouches => Self::CountsAndTouches,
            PubkeyIndexModeArg::CountsAndRuns => Self::CountsAndRuns,
            PubkeyIndexModeArg::None => Self::None,
        }
    }
}

impl From<SyncRpcEpochArgs> for RpcEpochSyncConfig {
    fn from(value: SyncRpcEpochArgs) -> Self {
        Self {
            rpc_url: value.rpc_url,
            commitment: value.commitment,
            timeout_secs: value.timeout_secs,
            rate_limit: value.rpc_client.into(),
        }
    }
}

impl From<BackfillRpcArgs> for RpcBackfillConfig {
    fn from(value: BackfillRpcArgs) -> Self {
        Self {
            rpc_url: value.rpc_url,
            archive_dir: value.archive_dir,
            start_slot: value.start_slot,
            end_slot: value.end_slot,
            commitment: value.commitment,
            timeout_secs: value.timeout_secs,
            slots_per_epoch: value.slots_per_epoch,
            skip_existing: value.skip_existing,
            rate_limit: value.rpc_client.into(),
        }
    }
}

impl From<BackfillPubkeyRunsArgs> for GrpcPubkeyRunBackfillConfig {
    fn from(value: BackfillPubkeyRunsArgs) -> Self {
        Self {
            archive_dir: value.archive_dir,
            output_run_dir: value.output_run_dir,
            start_block_id: value.start_block_id,
            max_blocks: value.max_blocks,
            reset_output_dir: value.reset_output_dir,
            pubkey_hot_registry_path: value.pubkey_hot_registry,
            pubkey_hot_count: value.pubkey_hot_count,
        }
    }
}

impl From<BackfillCompactLogsArgs> for GrpcCompactLogsBackfillConfig {
    fn from(value: BackfillCompactLogsArgs) -> Self {
        Self {
            archive_dir: value.archive_dir,
            output_archive_dir: value.output_archive_dir,
            max_blocks: value.max_blocks,
            start_block_id: value.start_block_id,
            append_output: value.append_output,
            overwrite_output: value.overwrite_output,
        }
    }
}

impl PrepareEpochRepairArgs {
    fn into_config(self) -> Result<PrepareEpochRepairConfig> {
        let capture_count = self.capture_dirs.len();
        for (name, count) in [
            ("capture-blocks", self.capture_blocks.len()),
            ("capture-max-slot", self.capture_max_slots.len()),
            ("capture-pubkey-runs", self.capture_pubkey_runs.len()),
            ("capture-journal", self.capture_journals.len()),
            ("capture-sealed-marker", self.capture_sealed_markers.len()),
        ] {
            anyhow::ensure!(
                count == capture_count,
                "--{name} was provided {count} times but --capture-dir was provided {capture_count} times"
            );
        }

        let captures = self
            .capture_dirs
            .into_iter()
            .zip(self.capture_blocks)
            .zip(self.capture_max_slots)
            .zip(self.capture_pubkey_runs)
            .zip(self.capture_journals)
            .zip(self.capture_sealed_markers)
            .map(
                |(
                    ((((capture_dir, selected_blocks), max_slot), pubkey_run_dir), journal_path),
                    sealed_marker,
                )| EpochRepairCaptureSlice {
                    capture_dir,
                    selected_blocks,
                    max_slot: Some(max_slot),
                    pubkey_run_dir,
                    journal_path,
                    sealed_marker,
                },
            )
            .collect();
        Ok(PrepareEpochRepairConfig {
            captures,
            rpc_repair_dirs: self.rpc_repair_dirs,
            rpc_complete_marker: self.rpc_complete_marker,
            output_dir: self.output_dir,
            epoch: self.epoch,
            slots_per_epoch: self.slots_per_epoch,
            expected_live_blocks: self.expected_live_blocks,
            expected_rpc_blocks: self.expected_rpc_blocks,
            expected_duplicate_live_blocks: Some(self.expected_duplicate_live_blocks),
            max_rpc_json_bytes: self.max_rpc_json_bytes,
        })
    }
}

impl From<RpcClientArgs> for RpcRateLimitConfig {
    fn from(value: RpcClientArgs) -> Self {
        Self {
            requests_per_second: value.rpc_rate_limit_per_sec,
            follow_server_rate_limits: value.rpc_follow_rate_limit,
            max_retries: value.rpc_rate_limit_retries,
            base_retry_delay_ms: value.rpc_rate_limit_base_delay_ms,
        }
    }
}

impl From<BenchFixtureArgs> for GrpcFixtureBenchConfig {
    fn from(value: BenchFixtureArgs) -> Self {
        Self {
            archive_dir: value.archive_dir,
            iterations: value.iterations,
            max_blocks: value.max_blocks,
            decode_mode: match value.decode_mode {
                BenchDecodeModeArg::Prost => GrpcFixtureDecodeMode::Prost,
                BenchDecodeModeArg::Borrowed => GrpcFixtureDecodeMode::Borrowed,
                BenchDecodeModeArg::BorrowedFast => GrpcFixtureDecodeMode::BorrowedFast,
            },
            initial_pubkey_capacity: value.initial_pubkey_capacity,
            heavy_hitter_capacities: value.heavy_hitter_capacities,
            pubkey_string_cache: match value.pubkey_string_cache {
                BenchPubkeyStringCacheArg::Enabled => GrpcFixturePubkeyStringCache::Enabled,
                BenchPubkeyStringCacheArg::Disabled => GrpcFixturePubkeyStringCache::Disabled,
            },
            log_parse_stats: value.log_parse_stats,
            hash_backends: match value.hash_backend {
                BenchHashBackendArg::All => GrpcFixtureHashBackend::all(),
                BenchHashBackendArg::StdRandom => vec![GrpcFixtureHashBackend::StdRandom],
                BenchHashBackendArg::StdAhash => vec![GrpcFixtureHashBackend::StdAhash],
                BenchHashBackendArg::StdFxhash => vec![GrpcFixtureHashBackend::StdFxhash],
                BenchHashBackendArg::HashbrownAhash => {
                    vec![GrpcFixtureHashBackend::HashbrownAhash]
                }
                BenchHashBackendArg::GxhashU64 => vec![GrpcFixtureHashBackend::GxhashU64],
                BenchHashBackendArg::GxhashU32 => vec![GrpcFixtureHashBackend::GxhashU32],
            },
            write_mode: match value.write_mode {
                BenchWriteModeArg::None => GrpcFixtureWriteMode::None,
                BenchWriteModeArg::Archive => GrpcFixtureWriteMode::Archive,
            },
            block_write_strategies: match value.block_write_strategy {
                BenchBlockWriteStrategyArg::All => GrpcFixtureBlockWriteStrategy::all(),
                BenchBlockWriteStrategyArg::Current => vec![GrpcFixtureBlockWriteStrategy::Current],
                BenchBlockWriteStrategyArg::ScratchOnce => {
                    vec![GrpcFixtureBlockWriteStrategy::ScratchOnce]
                }
            },
            output_dir: value.output_dir,
        }
    }
}

impl std::str::FromStr for SlotRangeArg {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let (start, end) = value
            .split_once('-')
            .ok_or_else(|| anyhow::anyhow!("slot range must be START-END"))?;
        let start_slot = start.parse::<u64>()?;
        let end_slot = end.parse::<u64>()?;
        anyhow::ensure!(start_slot <= end_slot, "slot range start must be <= end");
        Ok(Self {
            start_slot,
            end_slot,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Command::Init(config) => {
            let layout = LiveProducerApp::new(config).init()?;
            println!("{}", serde_json::to_string_pretty(&layout)?);
        }
        Command::Plan(config) => {
            println!("{}", serde_json::to_string_pretty(&config)?);
        }
        Command::Run(args) => {
            let app = LiveProducerApp::new(args.config);
            if args.dry_run {
                let layout = app.dry_run()?;
                println!("{}", serde_json::to_string_pretty(&layout)?);
            } else {
                app.run()?;
            }
        }
        Command::ProbeGrpc(args) => {
            let report = probe_grpc(args.into()).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::CaptureGrpc(args) => {
            let report = capture_grpc_blocks(args.into()).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::RecordGrpcRaw(args) => {
            let report = record_grpc_raw_blocks(args.into()).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::InspectGrpcRaw(args) => {
            let report = inspect_grpc_raw_blocks(
                args.output_dir,
                args.max_record_bytes,
                args.verify_payloads,
            )?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::VerifyGrpcRawPoh(args) => {
            let report =
                verify_grpc_raw_poh(args.output_dir, args.max_record_bytes, args.min_records)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::MaterializeGrpcRaw(args) => {
            let report = materialize_grpc_raw_blocks(GrpcRawMaterializeConfig {
                input_dir: args.input_dir,
                archive_dir: args.archive_dir,
                epoch: args.epoch,
                max_record_bytes: args.max_record_bytes,
                pubkey_hot_registry_path: args.pubkey_hot_registry,
                pubkey_hot_count: args.pubkey_hot_count,
            })?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::SyncRpcEpoch(args) => {
            let report = sync_epoch_info(args.into()).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::WatchEpochsGrpc(args) => {
            let report = watch_grpc_epoch_boundaries(args.into()).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::PlanEpochBackfill(args) => {
            let mut observed_slots = args.observed_slots.into_iter().collect::<BTreeSet<_>>();
            for range in args.observed_ranges {
                observed_slots.extend(range.start_slot..=range.end_slot);
            }
            let plan = plan_epoch_backfill(args.epoch, &observed_slots, args.slots_per_epoch);
            println!("{}", serde_json::to_string_pretty(&plan)?);
        }
        Command::BackfillRpc(args) => {
            let report = backfill_get_blocks(args.into()).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::InspectCapture(args) => {
            let report = inspect_capture(args.archive_dir)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::BackfillPubkeyRuns(args) => {
            let report = backfill_pubkey_runs(args.into())?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::BackfillCompactLogs(args) => {
            let report = backfill_compact_logs(args.into())?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::PrepareEpochRepair(args) => {
            let report = prepare_epoch_repair(args.into_config()?)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::BenchFixture(args) => {
            let report = bench_grpc_fixture(args.into())?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::ValidateIngestConfig(args) => {
            let json = std::fs::read_to_string(&args.config)
                .with_context(|| format!("read ingest config {}", args.config.display()))?;
            let config = IngestConfig::from_json(&json)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&config.redacted_summary())?
            );
        }
        Command::Supervise(args) => {
            let report = run_supervisor(args.into_config()?).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            anyhow::ensure!(
                report.successful,
                "supervised service did not exit successfully"
            );
        }
        Command::NotifySupervisor(args) => {
            notify_supervisor(args.kind.into())?;
            println!("{{\"notified\":true}}");
        }
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}
