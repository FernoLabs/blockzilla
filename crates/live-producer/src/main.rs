use anyhow::{Context, Result};
use blockzilla_live_producer::{
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
        GrpcRawCommittedCursor, GrpcRawCommittedReadLimits, GrpcRawMaterializeConfig,
        GrpcRawRecordConfig, inspect_grpc_raw_blocks, materialize_grpc_raw_blocks,
        open_grpc_raw_replication_generation_cursors, record_grpc_raw_blocks,
        seed_grpc_raw_generation, verify_grpc_raw_poh,
    },
    ingest::{
        CumulativeAckWal, IngestConfig, IngestRoleConfig, RawReplicationServerRuntime,
        ReconnectConfig, ReplicaUpstreamConfig, ReplicationSendOutcome, ReplicationSender,
        ReplicationSenderErrorKind, ReplicationStreamId, load_ingest_receiver_config,
    },
    rpc::{
        RpcBackfillConfig, RpcEpochSyncConfig, RpcRateLimitConfig, backfill_get_blocks,
        sync_epoch_info,
    },
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(name = "blockzilla-live-producer")]
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
    /// Seed an empty rolling generation with a stopped, verified generation's durable tail.
    SeedGrpcRawGeneration(SeedGrpcRawGenerationArgs),
    /// Materialize one committed epoch slice from a stopped raw spool into a staged capture.
    MaterializeGrpcRaw(MaterializeGrpcRawArgs),
    SyncRpcEpoch(SyncRpcEpochArgs),
    WatchEpochsGrpc(WatchEpochsGrpcArgs),
    PlanEpochBackfill(PlanEpochBackfillArgs),
    BackfillRpc(BackfillRpcArgs),
    InspectCapture(InspectCaptureArgs),
    BackfillPubkeyRuns(BackfillPubkeyRunsArgs),
    BackfillCompactLogs(BackfillCompactLogsArgs),
    BenchFixture(BenchFixtureArgs),
    /// Validate a redundant-ingest JSON config and print only its redacted summary.
    ValidateIngestConfig(ValidateIngestConfigArgs),
    /// Run the bounded, mTLS-only durable inbound replication receiver.
    ServeIngestReceiver(ServeIngestReceiverArgs),
    /// Replicate the local raw-gRPC hot queue to the configured primary.
    ReplicateGrpcRaw(ReplicateGrpcRawArgs),
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
struct ServeIngestReceiverArgs {
    /// Strict schema-v2 primary ingest configuration containing role.replica_listener.
    #[arg(long)]
    config: std::path::PathBuf,
}

#[derive(Debug, Args)]
struct ReplicateGrpcRawArgs {
    /// Strict schema-v2 replica ingest configuration containing role.upstream.
    #[arg(long)]
    config: std::path::PathBuf,

    /// Raw-gRPC hot-generation root; must lexically equal config.spool.root.
    #[arg(long)]
    cache_root: std::path::PathBuf,

    /// Delay while the active generation is caught up and awaiting new durable rows or rotation.
    #[arg(long, default_value_t = 1_000)]
    poll_interval_ms: u64,
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

    /// Maximum time allowed to establish the Yellowstone transport.
    #[arg(long, default_value_t = 30)]
    connect_timeout_secs: u64,

    /// Maximum time allowed to open the bidirectional Yellowstone subscription.
    #[arg(long, default_value_t = 30)]
    subscribe_timeout_secs: u64,

    /// Reconnect after this many seconds without a fully flushed block append.
    #[arg(long, default_value_t = 120)]
    idle_timeout_secs: u64,

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

    /// Never subscribe below this slot, even when the durable journal tail is older.
    #[arg(long)]
    min_resume_slot: Option<u64>,

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

    /// Stop before this self-contained generation would exceed the limit. Zero disables.
    #[arg(long, default_value_t = 0)]
    max_generation_bytes: u64,

    /// Keep the subscription open and atomically roll `<root>/active` into `<root>/sealed`.
    /// The supervisor must recover any pre-existing `.rotation` transaction before startup.
    #[arg(long)]
    hot_generation_root: Option<std::path::PathBuf>,

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

    /// Opt-in internal Yellowstone relay listener. Requires --relay-x-token-file.
    #[arg(long)]
    relay_bind: Option<std::net::SocketAddr>,

    /// Dedicated downstream x-token file; never falls back to the upstream token/environment.
    #[arg(long)]
    relay_x_token_file: Option<std::path::PathBuf>,

    #[arg(long, default_value_t = 128)]
    relay_max_records: usize,

    /// Retained protobuf byte cap; must be at least --max-record-bytes.
    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    relay_max_encoded_bytes: usize,

    #[arg(long, default_value_t = 4)]
    relay_max_clients: usize,
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
struct SeedGrpcRawGenerationArgs {
    #[arg(long)]
    source_dir: std::path::PathBuf,

    #[arg(long)]
    target_dir: std::path::PathBuf,

    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    max_record_bytes: u64,
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
    /// Existing live producer capture directory to read.
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

    #[arg(long, default_value = "target/blockzilla-live-producer-bench")]
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
            connect_timeout_secs: value.connect_timeout_secs,
            subscribe_timeout_secs: value.subscribe_timeout_secs,
            idle_timeout_secs: value.idle_timeout_secs,
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
            min_resume_slot: value.min_resume_slot,
            resume_coverage_warning_file: value.resume_coverage_warning_file,
            slots_per_epoch: value.slots_per_epoch,
            stop_at_epoch_boundary: value.stop_at_epoch_boundary,
            compression_level: value.compression_level,
            segment_target_bytes: value.segment_target_bytes,
            max_record_bytes: value.max_record_bytes,
            max_generation_bytes: value.max_generation_bytes,
            hot_generation_root: value.hot_generation_root,
            min_free_bytes: value.min_free_bytes,
            require_complete_poh: value.require_complete_poh,
            cluster_id: value.cluster_id,
            origin_node_id: value.origin_node_id,
            source_id: value.source_id,
            relay_bind: value.relay_bind,
            relay_x_token_file: value.relay_x_token_file,
            relay_max_records: value.relay_max_records,
            relay_max_encoded_bytes: value.relay_max_encoded_bytes,
            relay_max_clients: value.relay_max_clients,
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
    tracing::debug!("live producer CLI started");

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
        Command::SeedGrpcRawGeneration(args) => {
            let report =
                seed_grpc_raw_generation(args.source_dir, args.target_dir, args.max_record_bytes)?;
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
        Command::ServeIngestReceiver(args) => {
            let config = load_ingest_receiver_config(&args.config)?;
            let runtime = RawReplicationServerRuntime::from_ingest_config(&config)?;
            tracing::info!(
                bind = %runtime.bind_address(),
                primary_term = runtime.primary_term(),
                "starting mTLS ingest replication receiver"
            );
            runtime.serve_with_shutdown(shutdown_signal()).await?;
            tracing::info!("mTLS ingest replication receiver stopped cleanly");
        }
        Command::ReplicateGrpcRaw(args) => replicate_grpc_raw(args).await?,
    }

    Ok(())
}

struct PreparedGrpcRawGeneration {
    cursor: GrpcRawCommittedCursor,
    stream: ReplicationStreamId,
    physical_stream: ReplicationStreamId,
    discovered_sealed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicateGenerationOutcome {
    SealedCaughtUp,
    Shutdown,
}

struct ReplicationRetryBackoff {
    reconnect: ReconnectConfig,
    retries: u32,
    next_delay_ms: u64,
}

impl ReplicationRetryBackoff {
    fn new(reconnect: &ReconnectConfig) -> Self {
        Self {
            reconnect: reconnect.clone(),
            retries: 0,
            next_delay_ms: reconnect.initial_delay_ms,
        }
    }

    fn reset(&mut self) {
        self.retries = 0;
        self.next_delay_ms = self.reconnect.initial_delay_ms;
    }

    fn next_delay(&mut self) -> Option<(u32, Duration)> {
        if self
            .reconnect
            .max_attempts
            .is_some_and(|maximum| self.retries >= maximum)
        {
            return None;
        }

        self.retries = self.retries.saturating_add(1);
        let base_delay_ms = self.next_delay_ms.min(self.reconnect.max_delay_ms);
        let scaled = (u128::from(base_delay_ms)
            .saturating_mul(u128::from(self.reconnect.backoff_factor_milli))
            / 1_000)
            .min(u128::from(self.reconnect.max_delay_ms));
        self.next_delay_ms = scaled as u64;

        let spread = u128::from(base_delay_ms)
            .saturating_mul(u128::from(self.reconnect.jitter_percent))
            / 100;
        let delay_ms = if spread == 0 {
            base_delay_ms
        } else {
            let entropy = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| duration.as_nanos())
                ^ (u128::from(std::process::id()) << 64)
                ^ u128::from(self.retries);
            let lower = u128::from(base_delay_ms).saturating_sub(spread);
            let width = spread.saturating_mul(2).saturating_add(1);
            (lower.saturating_add(entropy % width)).min(u128::from(self.reconnect.max_delay_ms))
                as u64
        };
        Some((self.retries, Duration::from_millis(delay_ms.max(1))))
    }
}

async fn replicate_grpc_raw(args: ReplicateGrpcRawArgs) -> Result<()> {
    anyhow::ensure!(
        args.poll_interval_ms > 0,
        "--poll-interval-ms must be non-zero"
    );

    let config = load_ingest_receiver_config(&args.config)?;
    let (node_id, upstream) = match &config.role {
        IngestRoleConfig::Replica { node_id, upstream } => (node_id.clone(), upstream.clone()),
        IngestRoleConfig::Primary { .. } => {
            anyhow::bail!("replicate-grpc-raw requires role.mode=replica")
        }
    };
    anyhow::ensure!(
        args.cache_root == config.spool.root,
        "--cache-root must lexically equal config.spool.root"
    );

    let read_limits = GrpcRawCommittedReadLimits {
        max_compressed_record_bytes: upstream.batch.max_compressed_event_bytes,
        max_uncompressed_record_bytes: upstream.batch.max_uncompressed_event_bytes,
    };
    let poll_interval = Duration::from_millis(args.poll_interval_ms);
    let mut completed_sealed_generations = HashSet::new();
    let mut shutdown = replication_shutdown_receiver();

    tracing::info!(
        node_id = %node_id,
        cluster_id = %config.cluster_id,
        poll_interval_ms = args.poll_interval_ms,
        "starting raw gRPC replica"
    );

    'replication: loop {
        if replication_shutdown_requested(&shutdown) {
            break;
        }

        // Discovery and cursor opening share one rotation lock. Keeping each cursor alive pins
        // its exact stream identity even if the active pathname rotates while older generations
        // are still draining.
        let prepared =
            prepare_grpc_raw_generations(&args.cache_root, read_limits, &config.cluster_id)?;
        let locally_durable_through = {
            let ack_wal = CumulativeAckWal::open(&upstream.cumulative_ack_wal_file)
                .context("open cumulative ACK WAL for generation coverage")?;
            prepared
                .iter()
                .filter_map(|generation| {
                    ack_wal
                        .latest_stream_ack(&generation.stream)
                        .map(|ack| (generation.stream.clone(), ack.through_sequence))
                })
                .collect::<HashMap<_, _>>()
        };
        let present_sealed_generations = prepared
            .iter()
            .filter(|generation| generation.discovered_sealed)
            .map(|generation| generation.physical_stream.clone())
            .collect::<HashSet<_>>();
        completed_sealed_generations.retain(|stream| present_sealed_generations.contains(stream));

        for generation in prepared {
            if generation.discovered_sealed {
                if let Some(through_sequence) = locally_durable_through.get(&generation.stream) {
                    if generation
                        .cursor
                        .sealed_generation_is_covered_by(*through_sequence)
                        .context("check sealed generation against local durable ACK")?
                    {
                        tracing::debug!(
                            origin_node_id = %generation.stream.origin_node_id,
                            source_id = %generation.stream.source_id,
                            "sealed raw gRPC generation is already covered by the local durable ACK"
                        );
                        completed_sealed_generations.insert(generation.physical_stream);
                        continue;
                    }
                }
            }
            if generation.discovered_sealed
                && completed_sealed_generations.contains(&generation.physical_stream)
            {
                tracing::debug!(
                    origin_node_id = %generation.stream.origin_node_id,
                    source_id = %generation.stream.source_id,
                    "sealed raw gRPC generation remains caught up"
                );
                continue;
            }

            tracing::info!(
                origin_node_id = %generation.stream.origin_node_id,
                source_id = %generation.stream.source_id,
                discovered_sealed = generation.discovered_sealed,
                "replicating raw gRPC generation"
            );
            match replicate_grpc_raw_generation(
                &generation,
                &upstream,
                poll_interval,
                &mut shutdown,
            )
            .await?
            {
                ReplicateGenerationOutcome::SealedCaughtUp => {
                    completed_sealed_generations.insert(generation.physical_stream);
                }
                ReplicateGenerationOutcome::Shutdown => break 'replication,
            }
        }
    }

    tracing::info!("raw gRPC replica stopped cleanly");
    Ok(())
}

fn prepare_grpc_raw_generations(
    cache_root: &std::path::Path,
    read_limits: GrpcRawCommittedReadLimits,
    cluster_id: &str,
) -> Result<Vec<PreparedGrpcRawGeneration>> {
    let generations = open_grpc_raw_replication_generation_cursors(cache_root, read_limits)
        .context("atomically discover and open raw gRPC replication generations")?;
    let mut prepared = Vec::with_capacity(generations.len());
    for generation in generations {
        let cursor = generation.cursor;
        let stream = cursor.stream();
        let physical_stream = cursor.physical_stream();
        anyhow::ensure!(
            stream.cluster_id == cluster_id,
            "raw gRPC generation stream.cluster_id does not match config.cluster_id"
        );
        prepared.push(PreparedGrpcRawGeneration {
            cursor,
            stream,
            physical_stream,
            discovered_sealed: generation.sealed,
        });
    }
    Ok(prepared)
}

async fn replicate_grpc_raw_generation(
    generation: &PreparedGrpcRawGeneration,
    upstream: &ReplicaUpstreamConfig,
    poll_interval: Duration,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<ReplicateGenerationOutcome> {
    let mut retry = ReplicationRetryBackoff::new(&upstream.reconnect);
    let mut sender = loop {
        let connection = tokio::select! {
            biased;
            _ = replication_shutdown_notified(shutdown) => {
                return Ok(ReplicateGenerationOutcome::Shutdown);
            }
            connection = ReplicationSender::connect_preopened(
                generation.cursor.clone(),
                upstream,
            ) => {
                connection
            }
        };
        match connection {
            Ok(sender) => {
                anyhow::ensure!(
                    sender.stream().cluster_id == generation.stream.cluster_id,
                    "connected raw gRPC stream cluster differs from the pre-opened generation"
                );
                if sender.stream() != &generation.stream {
                    if generation.discovered_sealed {
                        anyhow::bail!(
                            "sealed raw gRPC generation identity changed before connection"
                        );
                    }
                    // The active path rotated after it was pre-opened but before connect fenced
                    // it. Drop this successor and resolve the original cursor into sealed.
                    drop(sender);
                    tracing::debug!(
                        "active raw gRPC generation rotated before sender binding; retrying its predecessor"
                    );
                    if replication_wait_or_shutdown(poll_interval, shutdown).await {
                        return Ok(ReplicateGenerationOutcome::Shutdown);
                    }
                    continue;
                }
                retry.reset();
                break sender;
            }
            Err(error) if error.kind() == ReplicationSenderErrorKind::Transport => {
                let Some((attempt, delay)) = retry.next_delay() else {
                    anyhow::bail!("raw gRPC connection transport retry limit exhausted");
                };
                tracing::warn!(
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    "raw gRPC primary is unavailable; retrying connection"
                );
                if replication_wait_or_shutdown(delay, shutdown).await {
                    return Ok(ReplicateGenerationOutcome::Shutdown);
                }
            }
            Err(error) => {
                return Err(
                    anyhow::Error::new(error).context("raw gRPC sender connection failed closed")
                );
            }
        }
    };

    loop {
        let outcome = tokio::select! {
            biased;
            _ = replication_shutdown_notified(shutdown) => {
                return Ok(ReplicateGenerationOutcome::Shutdown);
            }
            outcome = sender.send_once() => outcome,
        };
        match outcome {
            Ok(ReplicationSendOutcome::Advanced {
                records,
                compressed_bytes,
                through_sequence,
                primary_term,
            }) => {
                retry.reset();
                tracing::debug!(
                    records,
                    compressed_bytes,
                    through_sequence,
                    primary_term,
                    "raw gRPC replication advanced"
                );
            }
            Ok(ReplicationSendOutcome::CaughtUp) => {
                retry.reset();
                if sender
                    .bound_generation_is_sealed()
                    .context("resolve caught-up raw gRPC generation identity")?
                {
                    tracing::info!(
                        next_sequence = sender.next_sequence(),
                        "sealed raw gRPC generation is durably caught up"
                    );
                    return Ok(ReplicateGenerationOutcome::SealedCaughtUp);
                }
                tracing::debug!(
                    next_sequence = sender.next_sequence(),
                    "active raw gRPC generation is caught up; polling"
                );
                if replication_wait_or_shutdown(poll_interval, shutdown).await {
                    return Ok(ReplicateGenerationOutcome::Shutdown);
                }
            }
            Err(error) if error.kind() == ReplicationSenderErrorKind::Transport => {
                let Some((attempt, delay)) = retry.next_delay() else {
                    anyhow::bail!("raw gRPC send transport retry limit exhausted");
                };
                tracing::warn!(
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    "raw gRPC replication transport failed; retrying exact batch"
                );
                if replication_wait_or_shutdown(delay, shutdown).await {
                    return Ok(ReplicateGenerationOutcome::Shutdown);
                }
            }
            Err(error) => {
                return Err(anyhow::Error::new(error).context("raw gRPC sender failed closed"));
            }
        }
    }
}

fn replication_shutdown_receiver() -> tokio::sync::watch::Receiver<bool> {
    let (sender, receiver) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = sender.send(true);
    });
    receiver
}

fn replication_shutdown_requested(receiver: &tokio::sync::watch::Receiver<bool>) -> bool {
    *receiver.borrow()
}

async fn replication_shutdown_notified(receiver: &mut tokio::sync::watch::Receiver<bool>) {
    if replication_shutdown_requested(receiver) {
        return;
    }
    let _ = receiver.changed().await;
}

async fn replication_wait_or_shutdown(
    delay: Duration,
    receiver: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        biased;
        _ = replication_shutdown_notified(receiver) => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(error) => {
                    tracing::error!(%error, "failed to install SIGTERM handler");
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(%error, "failed to await interrupt signal");
                }
            }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to await interrupt signal");
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Every report-producing command reserves stdout for its JSON contract.
    // Operational diagnostics must never contaminate supervisor-parsed output.
    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}
