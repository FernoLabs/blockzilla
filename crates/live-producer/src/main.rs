use anyhow::Result;
use blockzilla_live_producer::{
    app::LiveProducerApp,
    config::ProducerConfig,
    epoch::{OLD_FAITHFUL_SLOTS_PER_EPOCH, plan_epoch_backfill},
    fixture_bench::{
        GrpcFixtureBenchConfig, GrpcFixtureBlockWriteStrategy, GrpcFixtureHashBackend,
        GrpcFixtureWriteMode, bench_grpc_fixture,
    },
    grpc::{
        GrpcCaptureConfig, GrpcEpochCommitment, GrpcEpochWatchConfig, GrpcProbeConfig,
        capture_grpc_blocks, inspect_capture, probe_grpc, watch_grpc_epoch_boundaries,
    },
    rpc::{
        RpcBackfillConfig, RpcEpochSyncConfig, RpcRateLimitConfig, backfill_get_blocks,
        sync_epoch_info,
    },
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::collections::BTreeSet;
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
    SyncRpcEpoch(SyncRpcEpochArgs),
    WatchEpochsGrpc(WatchEpochsGrpcArgs),
    PlanEpochBackfill(PlanEpochBackfillArgs),
    BackfillRpc(BackfillRpcArgs),
    InspectCapture(InspectCaptureArgs),
    BenchFixture(BenchFixtureArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    #[command(flatten)]
    config: ProducerConfig,

    #[arg(long)]
    dry_run: bool,
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
struct BenchFixtureArgs {
    #[arg(long, default_value = "blockzilla-v1/live-grpc-100")]
    archive_dir: std::path::PathBuf,

    #[arg(long, default_value_t = 3)]
    iterations: usize,

    #[arg(long)]
    max_blocks: Option<usize>,

    #[arg(long, default_value_t = 0)]
    initial_pubkey_capacity: usize,

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
enum GrpcEpochCommitmentArg {
    Processed,
    Confirmed,
    Finalized,
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
            initial_pubkey_capacity: value.initial_pubkey_capacity,
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
        Command::BenchFixture(args) => {
            let report = bench_grpc_fixture(args.into())?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}
