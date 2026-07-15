use anyhow::{Context, Result};
use blockzilla_hivezilla::coordinator::{gb_to_bytes, tb_to_bytes};
use blockzilla_hivezilla::{
    BuildMode, CapacityEstimate, CapacityEstimateRequest, CoordinatorConfig, HivezillaPlan,
    MachineSpec, NasPipelineConfig, PlanRequest, ProviderKind, ProviderSpec,
    RenderWorkerScriptRequest, SLOTS_PER_EPOCH, build_plan, estimate_capacity, hetzner_server_type,
    hetzner_server_types, render_worker_script, run_coordinator, run_nas_pipeline,
};
use clap::{Args, Parser, Subcommand};
use std::{
    collections::BTreeMap,
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Debug, Parser)]
#[command(name = "hivezilla")]
#[command(about = "Plan distributed Blockzilla CAR crunching jobs")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Build a JSON work plan and assign jobs across workers.
    Plan(PlanArgs),
    /// Run the NAS-side event listener that receives completed worker jobs.
    Coordinate(CoordinateArgs),
    /// Observe or execute the local NAS historical and live compaction pipeline.
    #[command(name = "pipeline", alias = "nas-pipeline")]
    NasPipeline(NasPipelineArgs),
    /// Estimate Hetzner Cloud machines, wall time, scratch disk, and compute cost.
    EstimateHetzner(EstimateHetznerArgs),
    /// Print a short summary for an existing plan.
    Summary(PlanPathArgs),
    /// Render the bash script a worker should run.
    RenderWorkerScript(RenderWorkerScriptArgs),
}

#[derive(Debug, Args)]
struct PlanArgs {
    /// Optional stable run id. Defaults to hivezilla-<unix-seconds>.
    #[arg(long)]
    run_id: Option<String>,

    /// Single epoch to include. Repeat for sparse sets.
    #[arg(long = "epoch")]
    epochs: Vec<u64>,

    /// First epoch in an inclusive range.
    #[arg(long)]
    start_epoch: Option<u64>,

    /// Last epoch in an inclusive range. Defaults to start-epoch when omitted.
    #[arg(long)]
    end_epoch: Option<u64>,
    /// Input CAR template. Supports {epoch}, {shard}, {start_slot}, {end_slot}, {worker}.
    #[arg(long, default_value = "/data/old-faithful/epoch-{epoch}.car.zst")]
    input_template: String,

    /// Previous epoch CAR template used by whole-epoch builders that need strict blockhash seeding.
    #[arg(long)]
    previous_car_template: Option<String>,

    /// Output directory/remote template. Supports {epoch}, {shard}, {start_slot}, {end_slot}, {worker}.
    #[arg(long, default_value = "/data/blockzilla/epoch-{epoch}")]
    output_template: String,

    #[arg(long, default_value_t = 1)]
    worker_count: usize,

    #[arg(long, value_enum, default_value_t = BuildMode::WholeEpoch)]
    mode: BuildMode,

    /// Slots per shard. Defaults to a full epoch for whole-epoch mode and epoch/workers for slice mode.
    #[arg(long)]
    chunk_slots: Option<u64>,

    #[arg(long, default_value_t = 1)]
    compression_level: i32,

    #[arg(long, default_value_t = SLOTS_PER_EPOCH)]
    slots_per_epoch: u64,

    /// Base URL used by old-faithful-slot-slices fetch jobs.
    #[arg(long)]
    old_faithful_base_url: Option<String>,

    #[arg(long, value_enum, default_value_t = ProviderKind::Manual)]
    provider: ProviderKind,

    #[arg(long)]
    provider_region: Option<String>,

    #[arg(long)]
    machine_type: Option<String>,

    #[arg(long)]
    image: Option<String>,

    #[arg(long)]
    max_price_per_hour_usd: Option<f64>,

    #[arg(long, default_value_t = 16)]
    cpu_cores: u16,

    #[arg(long, default_value_t = 64)]
    memory_gib: u16,

    #[arg(long, default_value_t = 2_000)]
    disk_gib: u32,

    #[arg(long, default_value = "nvme")]
    disk_kind: String,

    /// Label stored in the plan as key=value. Repeat for many labels.
    #[arg(long = "label")]
    labels: Vec<KeyValueArg>,

    #[arg(long, default_value = "hivezilla-plan.json")]
    plan_out: PathBuf,
}

#[derive(Debug, Args)]
struct PlanPathArgs {
    #[arg(long, default_value = "hivezilla-plan.json")]
    plan: PathBuf,
}

#[derive(Debug, Args)]
struct RenderWorkerScriptArgs {
    #[arg(long, default_value = "hivezilla-plan.json")]
    plan: PathBuf,

    #[arg(long)]
    worker_id: String,

    #[arg(long, default_value = "/opt/blockzilla")]
    repo_dir: String,

    #[arg(long, default_value = "/mnt/hivezilla")]
    scratch_dir: String,

    /// Coordinator base URL on the NAS. If omitted, worker scripts read HIVEZILLA_COORDINATOR_URL.
    #[arg(long)]
    coordinator_url: Option<String>,

    /// Environment variable containing the bearer token used to notify the NAS.
    #[arg(long, default_value = "HIVEZILLA_COORDINATOR_TOKEN")]
    coordinator_token_env: String,
}

#[derive(Debug, Args)]
struct CoordinateArgs {
    /// Address the NAS-side listener binds to.
    #[arg(long, default_value = "0.0.0.0:8787")]
    bind: SocketAddr,

    /// Directory where incoming event JSON and processing logs are written.
    #[arg(long, default_value = "hivezilla-events")]
    event_dir: PathBuf,

    /// Optional NAS artifact root. Successful events are copied under <artifact-dir>/<run>/<job>/.
    #[arg(long)]
    artifact_dir: Option<PathBuf>,

    /// Environment variable containing the expected bearer token. If unset or missing, auth is disabled.
    #[arg(long, default_value = "HIVEZILLA_COORDINATOR_TOKEN")]
    token_env: String,

    /// Delete the reported Hetzner server after artifact pull succeeds.
    #[arg(long)]
    destroy_hetzner: bool,

    /// Log pull/delete actions without executing rclone/rsync/hcloud.
    #[arg(long)]
    dry_run: bool,

    /// Logical corpus target shown by the dashboard.
    #[arg(long, default_value_t = 443.7)]
    target_tb: f64,

    /// Fallback input size credited when an event does not include input_bytes.
    #[arg(long, default_value_t = 390.8)]
    default_job_input_gb: f64,
}

#[derive(Debug, Args)]
struct NasPipelineArgs {
    /// Address for the monitoring API and dashboard.
    #[arg(long, default_value = "0.0.0.0:8788")]
    bind: SocketAddr,
    #[arg(long, default_value = "./target/release/blockzilla")]
    blockzilla_bin: PathBuf,
    #[arg(long, default_value = "/volume1/blockzilla")]
    car_root: PathBuf,
    #[arg(long, default_value = "/volume1/@home/ach/dev/blockzilla-v2")]
    archive_root: PathBuf,
    #[arg(long, default_value = "/volume1/@home/ach/dev/blockzilla-live")]
    live_root: PathBuf,
    #[arg(long, default_value = "nas-pipeline-state")]
    state_root: PathBuf,
    #[arg(long, default_value_t = 4)]
    scan_concurrency: usize,
    /// Maximum concurrent one-pass legacy registry reuse lanes.
    #[arg(long, default_value_t = 1)]
    legacy_compact_concurrency: usize,
    /// Reserved CPU cores for each legacy compact lane.
    #[arg(long, default_value_t = 1)]
    legacy_compact_cpu_cores_per_worker: u64,
    /// CPU-core admission budget shared by legacy compact lanes.
    #[arg(long, default_value_t = 1)]
    legacy_compact_cpu_budget_cores: u64,
    /// Reserved aggregate disk throughput for each legacy compact lane.
    #[arg(long, default_value_t = 120)]
    legacy_compact_io_mib_per_sec_per_worker: u64,
    /// Aggregate disk-throughput admission budget shared by legacy compact lanes.
    #[arg(long, default_value_t = 120)]
    legacy_compact_io_budget_mib_per_sec: u64,
    /// Adaptively stop/resume managed legacy lane process groups under pressure.
    #[arg(long)]
    legacy_compact_auto_pause: bool,
    /// Minimum number of legacy lanes kept running by adaptive pause.
    #[arg(long, default_value_t = 1)]
    legacy_compact_min_running: usize,
    /// Extra MemAvailable guard above the hard scheduler reserve.
    #[arg(long, default_value_t = 512)]
    legacy_compact_memory_guard_mib: u64,
    /// Legacy IO PSI telemetry threshold retained for API/config compatibility (not a saturation control).
    #[arg(long, default_value_t = 20.0)]
    legacy_compact_io_pause_full_avg10: f64,
    /// Legacy IO PSI telemetry threshold retained for API/config compatibility (not a saturation control).
    #[arg(long, default_value_t = 5.0)]
    legacy_compact_io_resume_full_avg10: f64,
    /// Minimum seconds between one-lane adaptive actions.
    #[arg(long, default_value_t = 30)]
    legacy_compact_pause_cooldown_secs: u64,
    /// Stable A/B/A sampling window for aggregate useful throughput.
    #[arg(long, default_value_t = 120)]
    legacy_compact_throughput_probe_window_secs: u64,
    /// Minimum aggregate blocks/s gain required to keep an added lane without confirmation.
    #[arg(long, default_value_t = 5.0)]
    legacy_compact_throughput_min_gain_pct: f64,
    /// Delay before re-probing a throughput ceiling confirmed by A/B/A.
    #[arg(long, default_value_t = 900)]
    legacy_compact_throughput_probe_backoff_secs: u64,
    /// Expected peak RSS for one full historical scan lane.
    #[arg(long, default_value_t = 800)]
    scan_memory_mib: u64,
    /// Minimum RSS budget for a finalizer stage. MPHF stages scale this from registry size.
    #[arg(long, default_value_t = 512)]
    finalizer_memory_mib: u64,
    /// Memory left available after projected scan growth.
    #[arg(long, default_value_t = 256)]
    memory_reserve_mib: u64,
    /// Free archive filesystem space below which no new work starts.
    #[arg(long, default_value_t = 256)]
    disk_reserve_gib: u64,
    #[arg(long, default_value_t = 1)]
    level: i32,
    /// Actually launch compaction children. Omit for the safe observer mode.
    #[arg(long)]
    execute: bool,
    /// Omit the block-access sidecars from historical first-seen scans.
    #[arg(long)]
    no_access: bool,
    #[arg(long)]
    start_epoch: Option<u64>,
    #[arg(long)]
    end_epoch: Option<u64>,
    /// Optional CAR source URL containing `{epoch}`. Requires explicit start/end bounds.
    #[arg(long)]
    car_source_url_template: Option<String>,
    /// Maximum concurrent CAR download/preflight children.
    #[arg(long, default_value_t = 1)]
    download_concurrency: usize,
    /// Structurally preflight canonical CARs before launching new historical scans.
    #[arg(long)]
    preflight_car: bool,
    #[arg(long, default_value_t = 5)]
    poll_interval_secs: u64,
    #[arg(long, default_value = "/tmp/blockzilla-first-seen-finalizer.lock")]
    finalizer_lock: PathBuf,
    /// Optional built dashboard directory. index.html is used as the SPA fallback.
    #[arg(long)]
    ui_dir: Option<PathBuf>,
    /// Environment variable containing the bearer token for control mutations.
    #[arg(long, default_value = "HIVEZILLA_CONTROL_TOKEN")]
    control_token_env: String,
    /// Permit mutation endpoints without a bearer token. Intended only for trusted LANs.
    #[arg(long)]
    allow_unauthenticated_controls: bool,
}

#[derive(Debug, Args)]
struct EstimateHetznerArgs {
    /// Number of whole epochs to process.
    #[arg(long, default_value_t = 1)]
    epoch_count: u64,

    /// Desired wall-clock deadline.
    #[arg(long, default_value_t = 24.0)]
    target_hours: f64,

    /// Measured or assumed hours to build/compress one reference-size epoch on one machine.
    #[arg(long, default_value_t = 3.0, alias = "hours-per-epoch")]
    hours_per_reference_epoch: f64,

    /// Input size represented by --hours-per-reference-epoch.
    #[arg(long, default_value_t = 390.8)]
    reference_input_gb: f64,

    /// Input CAR.zst size for a modern reference epoch.
    #[arg(long, default_value_t = 390.8)]
    input_gb_per_epoch: f64,

    /// Output Archive V2 directory size for a modern reference epoch.
    #[arg(long, default_value_t = 163.6)]
    output_gb_per_epoch: f64,

    /// Extra per-epoch builder registry/index/temp storage beyond input and final output.
    #[arg(long, default_value_t = 0.0)]
    builder_scratch_gb_per_epoch: f64,

    /// Extra scratch headroom for temp files, indexes, logs, package build output, and upload overlap.
    #[arg(long, default_value_t = 25.0)]
    scratch_overhead_pct: f64,

    /// Account for keeping the previous epoch CAR locally while building.
    #[arg(long)]
    include_previous_car: bool,

    /// Previous epoch CAR size when --include-previous-car is set.
    #[arg(long, default_value_t = 390.8)]
    previous_car_gb: f64,

    /// Cap machine count for quota/capacity experiments.
    #[arg(long)]
    max_machines: Option<u64>,

    /// Hetzner CCX type.
    #[arg(long, default_value = "ccx63")]
    machine_type: String,

    /// Disable per-machine rounding up to whole billable hours.
    #[arg(long, default_value_t = true)]
    hourly_billing: bool,

    /// Use the measured historical totals from docs for epochs 0-963.
    #[arg(long)]
    all_history_0_963: bool,
}

#[derive(Debug, Clone)]
struct KeyValueArg {
    key: String,
    value: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Plan(args) => {
            let plan = build_plan(args.into_request()?)?;
            write_plan(&args.plan_out, &plan)?;
            print_plan_summary(&plan);
        }
        Command::Coordinate(args) => {
            if args.destroy_hetzner && args.artifact_dir.is_none() && !args.dry_run {
                anyhow::bail!("--destroy-hetzner requires --artifact-dir unless --dry-run is set");
            }
            let token = env::var(&args.token_env).ok();
            println!(
                "hivezilla coordinator bind={} events={} artifacts={} destroy_hetzner={} dry_run={} auth={}",
                args.bind,
                args.event_dir.display(),
                args.artifact_dir
                    .as_deref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "disabled".to_string()),
                args.destroy_hetzner,
                args.dry_run,
                token.is_some()
            );
            run_coordinator(CoordinatorConfig {
                bind: args.bind,
                event_dir: args.event_dir,
                artifact_dir: args.artifact_dir,
                token,
                destroy_hetzner: args.destroy_hetzner,
                dry_run: args.dry_run,
                target_bytes: tb_to_bytes(args.target_tb),
                default_job_input_bytes: gb_to_bytes(args.default_job_input_gb),
            })
            .await?;
        }
        Command::NasPipeline(args) => {
            if args.car_source_url_template.is_some() {
                anyhow::ensure!(
                    args.start_epoch.is_some() && args.end_epoch.is_some(),
                    "--car-source-url-template requires explicit --start-epoch and --end-epoch"
                );
                anyhow::ensure!(
                    args.car_source_url_template
                        .as_deref()
                        .is_some_and(|template| template.contains("{epoch}")),
                    "--car-source-url-template must contain {{epoch}}"
                );
            }
            let end_epoch = args.end_epoch.or(args.start_epoch);
            if let (Some(start), Some(end)) = (args.start_epoch, end_epoch) {
                anyhow::ensure!(start <= end, "--start-epoch must not exceed --end-epoch");
            } else if args.end_epoch.is_some() {
                anyhow::bail!("--end-epoch requires --start-epoch");
            }
            anyhow::ensure!(
                args.scan_concurrency > 0,
                "--scan-concurrency must be positive"
            );
            anyhow::ensure!(
                args.legacy_compact_concurrency > 0,
                "--legacy-compact-concurrency must be positive"
            );
            anyhow::ensure!(
                args.legacy_compact_cpu_cores_per_worker > 0,
                "--legacy-compact-cpu-cores-per-worker must be positive"
            );
            anyhow::ensure!(
                args.legacy_compact_cpu_budget_cores > 0,
                "--legacy-compact-cpu-budget-cores must be positive"
            );
            anyhow::ensure!(
                args.legacy_compact_io_mib_per_sec_per_worker > 0,
                "--legacy-compact-io-mib-per-sec-per-worker must be positive"
            );
            anyhow::ensure!(
                args.legacy_compact_io_budget_mib_per_sec > 0,
                "--legacy-compact-io-budget-mib-per-sec must be positive"
            );
            let legacy_effective_capacity = args
                .legacy_compact_concurrency
                .min(
                    usize::try_from(
                        args.legacy_compact_cpu_budget_cores
                            / args.legacy_compact_cpu_cores_per_worker,
                    )
                    .unwrap_or(usize::MAX),
                )
                .min(
                    usize::try_from(
                        args.legacy_compact_io_budget_mib_per_sec
                            / args.legacy_compact_io_mib_per_sec_per_worker,
                    )
                    .unwrap_or(usize::MAX),
                );
            anyhow::ensure!(
                !args.legacy_compact_auto_pause
                    || args.legacy_compact_min_running <= legacy_effective_capacity,
                "--legacy-compact-min-running must not exceed effective legacy capacity ({legacy_effective_capacity})"
            );
            anyhow::ensure!(
                !args.legacy_compact_auto_pause || args.legacy_compact_min_running > 0,
                "--legacy-compact-min-running must be positive when adaptive probing is enabled"
            );
            anyhow::ensure!(
                args.legacy_compact_io_pause_full_avg10.is_finite()
                    && args.legacy_compact_io_resume_full_avg10.is_finite()
                    && args.legacy_compact_io_resume_full_avg10 >= 0.0
                    && args.legacy_compact_io_pause_full_avg10 <= 100.0
                    && args.legacy_compact_io_resume_full_avg10 <= 100.0
                    && args.legacy_compact_io_pause_full_avg10
                        > args.legacy_compact_io_resume_full_avg10,
                "--legacy-compact-io-pause-full-avg10 must be finite and greater than the non-negative resume threshold"
            );
            anyhow::ensure!(
                args.legacy_compact_pause_cooldown_secs > 0,
                "--legacy-compact-pause-cooldown-secs must be positive"
            );
            anyhow::ensure!(
                args.legacy_compact_throughput_probe_window_secs
                    >= args.poll_interval_secs.saturating_mul(3),
                "--legacy-compact-throughput-probe-window-secs must span at least three scheduler polls"
            );
            anyhow::ensure!(
                args.legacy_compact_throughput_probe_window_secs
                    >= args.legacy_compact_pause_cooldown_secs,
                "--legacy-compact-throughput-probe-window-secs must not be shorter than the adaptive action cooldown"
            );
            anyhow::ensure!(
                args.legacy_compact_throughput_min_gain_pct.is_finite()
                    && (0.0..=100.0).contains(&args.legacy_compact_throughput_min_gain_pct),
                "--legacy-compact-throughput-min-gain-pct must be finite and between 0 and 100"
            );
            anyhow::ensure!(
                args.legacy_compact_throughput_probe_backoff_secs > 0,
                "--legacy-compact-throughput-probe-backoff-secs must be positive"
            );
            anyhow::ensure!(
                args.finalizer_memory_mib > 0,
                "--finalizer-memory-mib must be positive"
            );
            anyhow::ensure!(
                args.download_concurrency > 0,
                "--download-concurrency must be positive"
            );
            anyhow::ensure!(
                args.poll_interval_secs > 0,
                "--poll-interval-secs must be positive"
            );
            run_nas_pipeline(NasPipelineConfig {
                bind: args.bind,
                blockzilla_bin: args.blockzilla_bin,
                car_root: args.car_root,
                archive_root: args.archive_root,
                live_root: args.live_root,
                state_root: args.state_root,
                scan_concurrency: args.scan_concurrency,
                legacy_compact_concurrency: args.legacy_compact_concurrency,
                legacy_compact_cpu_cores_per_worker: args.legacy_compact_cpu_cores_per_worker,
                legacy_compact_cpu_budget_cores: args.legacy_compact_cpu_budget_cores,
                legacy_compact_io_mib_per_sec_per_worker: args
                    .legacy_compact_io_mib_per_sec_per_worker,
                legacy_compact_io_budget_mib_per_sec: args.legacy_compact_io_budget_mib_per_sec,
                legacy_compact_auto_pause: args.legacy_compact_auto_pause,
                legacy_compact_min_running: args.legacy_compact_min_running,
                legacy_compact_memory_guard_mib: args.legacy_compact_memory_guard_mib,
                legacy_compact_io_pause_full_avg10: args.legacy_compact_io_pause_full_avg10,
                legacy_compact_io_resume_full_avg10: args.legacy_compact_io_resume_full_avg10,
                legacy_compact_pause_cooldown: std::time::Duration::from_secs(
                    args.legacy_compact_pause_cooldown_secs,
                ),
                legacy_compact_throughput_probe_window: std::time::Duration::from_secs(
                    args.legacy_compact_throughput_probe_window_secs,
                ),
                legacy_compact_throughput_min_gain_pct: args.legacy_compact_throughput_min_gain_pct,
                legacy_compact_throughput_probe_backoff: std::time::Duration::from_secs(
                    args.legacy_compact_throughput_probe_backoff_secs,
                ),
                scan_memory_mib: args.scan_memory_mib,
                finalizer_memory_mib: args.finalizer_memory_mib,
                memory_reserve_mib: args.memory_reserve_mib,
                disk_reserve_gib: args.disk_reserve_gib,
                level: args.level,
                execute: args.execute,
                no_access: args.no_access,
                start_epoch: args.start_epoch,
                end_epoch,
                car_source_url_template: args.car_source_url_template,
                download_concurrency: args.download_concurrency,
                preflight_car: args.preflight_car,
                poll_interval: std::time::Duration::from_secs(args.poll_interval_secs),
                finalizer_lock: args.finalizer_lock,
                ui_dir: args.ui_dir,
                control_token: env::var(&args.control_token_env).ok(),
                allow_unauthenticated_controls: args.allow_unauthenticated_controls,
            })
            .await?;
        }
        Command::EstimateHetzner(args) => {
            let estimate = estimate_capacity(args.into_request()?)?;
            print_hetzner_estimate(&estimate);
        }
        Command::Summary(args) => {
            let plan = read_plan(&args.plan)?;
            print_plan_summary(&plan);
        }
        Command::RenderWorkerScript(args) => {
            let plan = read_plan(&args.plan)?;
            let script = render_worker_script(RenderWorkerScriptRequest {
                plan: &plan,
                worker_id: &args.worker_id,
                repo_dir: &args.repo_dir,
                scratch_dir: &args.scratch_dir,
                coordinator_url: args.coordinator_url.as_deref(),
                coordinator_token_env: &args.coordinator_token_env,
            })?;
            print!("{script}");
        }
    }

    Ok(())
}

impl EstimateHetznerArgs {
    fn into_request(&self) -> Result<CapacityEstimateRequest> {
        let machine = hetzner_server_type(&self.machine_type).with_context(|| {
            format!(
                "unknown Hetzner machine type {}; known types: {}",
                self.machine_type,
                hetzner_server_types()
                    .iter()
                    .map(|server_type| server_type.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
        let (epoch_count, input_gb_per_epoch, output_gb_per_epoch) = if self.all_history_0_963 {
            (964, 186_480.0 / 964.0, 130_300.0 / 964.0)
        } else {
            (
                self.epoch_count,
                self.input_gb_per_epoch,
                self.output_gb_per_epoch,
            )
        };

        Ok(CapacityEstimateRequest {
            epoch_count,
            target_hours: self.target_hours,
            hours_per_reference_epoch: self.hours_per_reference_epoch,
            reference_input_gb: self.reference_input_gb,
            input_gb_per_epoch,
            output_gb_per_epoch,
            builder_scratch_gb_per_epoch: self.builder_scratch_gb_per_epoch,
            scratch_overhead_pct: self.scratch_overhead_pct,
            include_previous_car: self.include_previous_car,
            previous_car_gb: self.previous_car_gb,
            max_machines: self.max_machines,
            hourly_billing: self.hourly_billing,
            machine,
        })
    }
}

impl PlanArgs {
    fn into_request(&self) -> Result<PlanRequest> {
        let mut epochs = self.epochs.clone();
        if let Some(start_epoch) = self.start_epoch {
            let end_epoch = self.end_epoch.unwrap_or(start_epoch);
            anyhow::ensure!(
                start_epoch <= end_epoch,
                "start-epoch must be less than or equal to end-epoch"
            );
            epochs.extend(start_epoch..=end_epoch);
        } else if self.end_epoch.is_some() {
            anyhow::bail!("end-epoch requires start-epoch");
        }
        epochs.sort_unstable();
        epochs.dedup();
        anyhow::ensure!(
            !epochs.is_empty(),
            "provide --epoch or --start-epoch/--end-epoch"
        );

        let labels = self
            .labels
            .iter()
            .map(|label| (label.key.clone(), label.value.clone()))
            .collect::<BTreeMap<_, _>>();

        Ok(PlanRequest {
            run_id: self.run_id.clone(),
            epochs,
            input_template: self.input_template.clone(),
            previous_car_template: self.previous_car_template.clone(),
            output_template: self.output_template.clone(),
            worker_count: self.worker_count,
            provider: ProviderSpec {
                kind: self.provider,
                region: self.provider_region.clone(),
                machine_type: self.machine_type.clone(),
                image: self.image.clone(),
                max_price_per_hour_usd: self.max_price_per_hour_usd,
                machine: MachineSpec {
                    cpu_cores: self.cpu_cores,
                    memory_gib: self.memory_gib,
                    disk_gib: self.disk_gib,
                    disk_kind: self.disk_kind.clone(),
                },
            },
            build_mode: self.mode,
            chunk_slots: self.chunk_slots,
            compression_level: self.compression_level,
            slots_per_epoch: self.slots_per_epoch,
            old_faithful_base_url: self.old_faithful_base_url.clone(),
            labels,
        })
    }
}

impl std::str::FromStr for KeyValueArg {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let (key, value) = value
            .split_once('=')
            .context("labels must be written as key=value")?;
        anyhow::ensure!(!key.is_empty(), "label key cannot be empty");
        Ok(Self {
            key: key.to_string(),
            value: value.to_string(),
        })
    }
}

fn write_plan(path: &Path, plan: &HivezillaPlan) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create plan directory {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(plan).context("serialize hivezilla plan")?;
    fs::write(path, format!("{json}\n")).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn read_plan(path: &Path) -> Result<HivezillaPlan> {
    let json = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&json).with_context(|| format!("parse {}", path.display()))
}

fn print_plan_summary(plan: &HivezillaPlan) {
    println!(
        "hivezilla plan={} mode={} workers={} jobs={} chunk_slots={}",
        plan.run_id,
        plan.strategy.build_mode.as_str(),
        plan.workers.len(),
        plan.jobs.len(),
        plan.strategy.chunk_slots
    );
    for worker in &plan.workers {
        let jobs = plan.jobs_for_worker(&worker.id);
        println!("  {} jobs={}", worker.id, jobs.len());
    }
}

fn print_hetzner_estimate(estimate: &CapacityEstimate) {
    let machine = estimate.machine;
    println!(
        "hivezilla hetzner estimate machine={} vcpu={} ram={}GB disk={}GB price=€{:.4}/h",
        machine.name, machine.vcpus, machine.memory_gb, machine.local_disk_gb, machine.hourly_eur
    );
    println!(
        "  epochs={} target_hours={:.2} hours_per_reference_epoch={:.2}@{:.1}GB effective_hours_per_epoch={:.2} machines={} elapsed_hours={:.2}",
        estimate.epoch_count,
        estimate.target_hours,
        estimate.hours_per_reference_epoch,
        estimate.reference_input_gb,
        estimate.hours_per_epoch,
        estimate.selected_machines,
        estimate.estimated_elapsed_hours
    );
    println!(
        "  total_input={:.2}TB total_output={:.2}TB machine_hours={:.2} billable_hours={:.2} compute_cost=€{:.2}",
        estimate.total_input_tb,
        estimate.total_output_tb,
        estimate.total_machine_hours,
        estimate.total_billable_hours,
        estimate.total_cost_eur
    );
    println!(
        "  scratch_per_epoch={:.1}GB builder_scratch={:.1}GB disk_margin={:.1}GB disk_fits={}",
        estimate.scratch_required_gb,
        estimate.builder_scratch_gb_per_epoch,
        estimate.disk_margin_gb,
        estimate.disk_fits
    );

    if estimate.estimated_elapsed_hours > estimate.target_hours {
        println!(
            "  warning=target_not_reachable_with_whole_epoch_parallelism parallelism_limit={}",
            estimate.parallelism_limit
        );
    }
    if !estimate.disk_fits {
        println!(
            "  warning=local_disk_too_small choose a larger machine, lower scratch usage, or stream inputs"
        );
    }
}
