use anyhow::{Context, Result};
use blockzilla_hivezilla::coordinator::{gb_to_bytes, tb_to_bytes};
use blockzilla_hivezilla::{
    BuildMode, CapacityEstimate, CapacityEstimateRequest, CoordinatorConfig, HivezillaPlan,
    MachineSpec, PlanRequest, ProviderKind, ProviderSpec, RenderWorkerScriptRequest,
    SLOTS_PER_EPOCH, build_plan, estimate_capacity, hetzner_server_type, hetzner_server_types,
    render_worker_script, run_coordinator,
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
