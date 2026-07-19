use super::{SchedulerConfig, validate_management_bind};
use anyhow::{Context, Result};
use clap::{Args, Parser};
use std::{ffi::OsString, net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Debug, Parser)]
#[command(
    name = "blockzilla scheduler",
    about = "Run the Blockzilla storage scheduler and its read-only status API"
)]
struct SchedulerCli {
    #[command(flatten)]
    scheduler: SchedulerArgs,
}

/// Run the Blockzilla storage scheduler and its read-only status API.
#[derive(Debug, Clone, Args)]
pub struct SchedulerArgs {
    /// Address for the read-only health, status, and event-stream API.
    #[arg(long, default_value = "127.0.0.1:8787")]
    status_bind: SocketAddr,

    /// Optional loopback-only address for pause, resume, and retry operations.
    /// Leave unset to disable all management endpoints.
    #[arg(long)]
    management_bind: Option<SocketAddr>,

    /// Blockzilla executable launched for archive jobs. Defaults to this executable.
    #[arg(long)]
    blockzilla_bin: Option<PathBuf>,

    /// Optional alternate Blockzilla executable used only for repair jobs.
    #[arg(long)]
    repair_blockzilla_bin: Option<PathBuf>,

    /// Directory containing source CAR and CAR.ZST files.
    #[arg(long)]
    car_root: PathBuf,

    /// Directory containing and receiving Archive V2 epochs.
    #[arg(long)]
    archive_root: PathBuf,

    /// Directory containing live-capture workspaces.
    #[arg(long)]
    live_root: PathBuf,

    /// Durable scheduler state, progress, and logs.
    #[arg(long, default_value = "blockzilla-scheduler-state")]
    state_root: PathBuf,

    /// Maximum concurrent historical scan jobs.
    #[arg(long, default_value_t = 1)]
    scan_concurrency: usize,

    /// Compact-job ceiling. Zero enables adaptive probing without a fixed ceiling.
    #[arg(long, default_value_t = 0)]
    compact_concurrency: usize,

    /// Compact-job ceiling while a finalizer is active. Zero keeps strict
    /// exclusivity in fixed mode and remains uncapped in adaptive mode.
    #[arg(long, default_value_t = 0)]
    compact_finalizer_overlap: usize,

    /// CPU estimate retained in status output for each compact worker.
    #[arg(long, default_value_t = 1)]
    compact_cpu_cores_per_worker: u64,

    /// One-minute load ceiling used by adaptive pause and resume. Defaults to
    /// the host's available parallelism.
    #[arg(long)]
    compact_cpu_budget_cores: Option<u64>,

    /// Estimated aggregate disk throughput for one compact worker.
    #[arg(long, default_value_t = 120)]
    compact_io_mib_per_sec_per_worker: u64,

    /// Optional measured archive-device throughput ceiling. Zero disables it.
    #[arg(long, default_value_t = 0)]
    compact_io_budget_mib_per_sec: u64,

    /// Adaptively stop and resume managed compact process groups under pressure.
    #[arg(long)]
    compact_auto_pause: bool,

    /// Initial compact lane target. Hard resource guards may pause below it.
    #[arg(long, default_value_t = 1)]
    compact_min_running: usize,

    /// Extra MemAvailable guard above the scheduler reserve.
    #[arg(long, default_value_t = 512)]
    compact_memory_guard_mib: u64,

    /// Pause when Linux I/O PSI full avg10 reaches this percentage.
    #[arg(long, default_value_t = 20.0)]
    compact_io_pause_full_avg10: f64,

    /// Resume when Linux I/O PSI full avg10 falls to this percentage.
    #[arg(long, default_value_t = 5.0)]
    compact_io_resume_full_avg10: f64,

    /// Minimum seconds between adaptive compact-lane actions.
    #[arg(long, default_value_t = 30)]
    compact_pause_cooldown_secs: u64,

    /// Expected peak RSS for one historical scan job.
    #[arg(long, default_value_t = 800)]
    scan_memory_mib: u64,

    /// Minimum RSS budget for a finalizer stage.
    #[arg(long, default_value_t = 512)]
    finalizer_memory_mib: u64,

    /// Memory kept available after projected job growth.
    #[arg(long, default_value_t = 512)]
    memory_reserve_mib: u64,

    /// Free archive-filesystem space below which new work does not start.
    #[arg(long, default_value_t = 16)]
    disk_reserve_gib: u64,

    /// Zstd compression level used by archive builders.
    #[arg(long, default_value_t = 1)]
    level: i32,

    /// Launch jobs. Omit for observer mode.
    #[arg(long)]
    execute: bool,

    /// Omit block-access sidecars from historical first-seen scans.
    #[arg(long)]
    no_access: bool,

    /// First epoch included in scheduler inventory.
    #[arg(long)]
    start_epoch: Option<u64>,

    /// Last epoch included in scheduler inventory. Requires --start-epoch.
    #[arg(long)]
    end_epoch: Option<u64>,

    /// First epoch in an optional preferred historical range.
    #[arg(long)]
    priority_epoch_start: Option<u64>,

    /// Last epoch in an optional preferred historical range.
    #[arg(long)]
    priority_epoch_end: Option<u64>,

    /// Optional CAR URL template containing `{epoch}`. Requires an explicit range.
    #[arg(long)]
    car_source_url_template: Option<String>,

    /// Maximum concurrent CAR download and preflight jobs.
    #[arg(long, default_value_t = 1)]
    download_concurrency: usize,

    /// Structurally preflight canonical CAR files before new historical scans.
    #[arg(long)]
    preflight_car: bool,

    /// Scheduler reconciliation interval.
    #[arg(long, default_value_t = 5)]
    poll_interval_secs: u64,

    /// Advisory lock shared by first-seen scan and finalizer jobs. Defaults
    /// beneath --state-root.
    #[arg(long)]
    finalizer_lock: Option<PathBuf>,
}

impl SchedulerArgs {
    pub fn parse_from<I, T>(args: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        SchedulerCli::parse_from(args).scheduler
    }

    #[cfg(test)]
    fn try_parse_from<I, T>(args: I) -> clap::error::Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        SchedulerCli::try_parse_from(args).map(|cli| cli.scheduler)
    }

    pub fn into_config(self) -> Result<SchedulerConfig> {
        validate_management_bind(self.management_bind)?;

        let end_epoch = match (self.start_epoch, self.end_epoch) {
            (Some(start), Some(end)) => {
                anyhow::ensure!(start <= end, "--start-epoch must not exceed --end-epoch");
                Some(end)
            }
            (Some(start), None) => Some(start),
            (None, Some(_)) => anyhow::bail!("--end-epoch requires --start-epoch"),
            (None, None) => None,
        };
        let priority_epoch_end = match (self.priority_epoch_start, self.priority_epoch_end) {
            (Some(start), Some(end)) => {
                anyhow::ensure!(
                    start <= end,
                    "--priority-epoch-start must not exceed --priority-epoch-end"
                );
                Some(end)
            }
            (Some(start), None) => Some(start),
            (None, Some(_)) => {
                anyhow::bail!("--priority-epoch-end requires --priority-epoch-start")
            }
            (None, None) => None,
        };

        if let Some(template) = self.car_source_url_template.as_deref() {
            anyhow::ensure!(
                self.start_epoch.is_some() && self.end_epoch.is_some(),
                "--car-source-url-template requires explicit --start-epoch and --end-epoch"
            );
            anyhow::ensure!(
                template.contains("{epoch}"),
                "--car-source-url-template must contain {{epoch}}"
            );
        }

        anyhow::ensure!(
            self.scan_concurrency > 0,
            "--scan-concurrency must be positive"
        );
        anyhow::ensure!(
            self.compact_cpu_cores_per_worker > 0,
            "--compact-cpu-cores-per-worker must be positive"
        );
        anyhow::ensure!(
            self.compact_io_mib_per_sec_per_worker > 0,
            "--compact-io-mib-per-sec-per-worker must be positive"
        );
        let compact_cpu_budget_cores = match self.compact_cpu_budget_cores {
            Some(value) => value,
            None => u64::try_from(
                std::thread::available_parallelism()
                    .context("detect host parallelism")?
                    .get(),
            )
            .context("host parallelism does not fit u64")?,
        };
        anyhow::ensure!(
            compact_cpu_budget_cores > 0,
            "--compact-cpu-budget-cores must be positive"
        );
        let compact_effective_capacity = if self.compact_concurrency == 0 {
            usize::MAX
        } else {
            self.compact_concurrency
        };
        anyhow::ensure!(
            !self.compact_auto_pause || self.compact_min_running <= compact_effective_capacity,
            "--compact-min-running must not exceed effective compact capacity ({compact_effective_capacity})"
        );
        anyhow::ensure!(
            self.compact_finalizer_overlap <= compact_effective_capacity,
            "--compact-finalizer-overlap must not exceed effective compact capacity ({compact_effective_capacity})"
        );
        anyhow::ensure!(
            self.compact_io_pause_full_avg10.is_finite()
                && self.compact_io_resume_full_avg10.is_finite()
                && self.compact_io_resume_full_avg10 >= 0.0
                && self.compact_io_pause_full_avg10 <= 100.0
                && self.compact_io_resume_full_avg10 <= 100.0
                && self.compact_io_pause_full_avg10 > self.compact_io_resume_full_avg10,
            "--compact-io-pause-full-avg10 must be finite and greater than the non-negative resume threshold"
        );
        anyhow::ensure!(
            self.compact_pause_cooldown_secs > 0,
            "--compact-pause-cooldown-secs must be positive"
        );
        anyhow::ensure!(
            self.finalizer_memory_mib > 0,
            "--finalizer-memory-mib must be positive"
        );
        anyhow::ensure!(
            self.download_concurrency > 0,
            "--download-concurrency must be positive"
        );
        anyhow::ensure!(
            self.poll_interval_secs > 0,
            "--poll-interval-secs must be positive"
        );

        let blockzilla_bin = match self.blockzilla_bin {
            Some(path) => path,
            None => std::env::current_exe().context("resolve current Blockzilla executable")?,
        };
        let finalizer_lock = self
            .finalizer_lock
            .unwrap_or_else(|| self.state_root.join("first-seen-finalizer.lock"));

        Ok(SchedulerConfig {
            status_bind: self.status_bind,
            management_bind: self.management_bind,
            blockzilla_bin,
            repair_blockzilla_bin: self.repair_blockzilla_bin,
            car_root: self.car_root,
            archive_root: self.archive_root,
            live_root: self.live_root,
            state_root: self.state_root,
            scan_concurrency: self.scan_concurrency,
            legacy_compact_concurrency: self.compact_concurrency,
            legacy_compact_finalizer_overlap: self.compact_finalizer_overlap,
            legacy_compact_cpu_cores_per_worker: self.compact_cpu_cores_per_worker,
            legacy_compact_cpu_budget_cores: compact_cpu_budget_cores,
            legacy_compact_io_mib_per_sec_per_worker: self.compact_io_mib_per_sec_per_worker,
            legacy_compact_io_budget_mib_per_sec: self.compact_io_budget_mib_per_sec,
            legacy_compact_auto_pause: self.compact_auto_pause,
            legacy_compact_min_running: self.compact_min_running,
            legacy_compact_memory_guard_mib: self.compact_memory_guard_mib,
            legacy_compact_io_pause_full_avg10: self.compact_io_pause_full_avg10,
            legacy_compact_io_resume_full_avg10: self.compact_io_resume_full_avg10,
            legacy_compact_pause_cooldown: Duration::from_secs(self.compact_pause_cooldown_secs),
            scan_memory_mib: self.scan_memory_mib,
            finalizer_memory_mib: self.finalizer_memory_mib,
            memory_reserve_mib: self.memory_reserve_mib,
            disk_reserve_gib: self.disk_reserve_gib,
            level: self.level,
            execute: self.execute,
            no_access: self.no_access,
            start_epoch: self.start_epoch,
            end_epoch,
            priority_epoch_start: self.priority_epoch_start,
            priority_epoch_end,
            car_source_url_template: self.car_source_url_template,
            download_concurrency: self.download_concurrency,
            preflight_car: self.preflight_car,
            poll_interval: Duration::from_secs(self.poll_interval_secs),
            finalizer_lock,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn required_args() -> [&'static str; 6] {
        [
            "test",
            "--car-root",
            "cars",
            "--archive-root",
            "archives",
            "--live-root",
        ]
    }

    #[test]
    fn portable_roots_and_loopback_defaults_parse() {
        let mut args = required_args().to_vec();
        args.push("live");
        let parsed = SchedulerArgs::try_parse_from(args).unwrap();
        assert_eq!(parsed.status_bind, "127.0.0.1:8787".parse().unwrap());
        assert_eq!(
            parsed.state_root,
            PathBuf::from("blockzilla-scheduler-state")
        );
        assert!(parsed.management_bind.is_none());
    }

    #[test]
    fn non_loopback_management_bind_is_rejected() {
        let mut args = required_args().to_vec();
        args.extend(["live", "--management-bind", "0.0.0.0:8788"]);
        let parsed = SchedulerArgs::try_parse_from(args).unwrap();
        assert!(parsed.into_config().is_err());
    }

    #[test]
    fn source_template_requires_explicit_bounded_range() {
        let mut args = required_args().to_vec();
        args.extend([
            "live",
            "--car-source-url-template",
            "https://example.invalid/{epoch}.car.zst",
            "--start-epoch",
            "10",
        ]);
        let parsed = SchedulerArgs::try_parse_from(args).unwrap();
        assert!(parsed.into_config().is_err());
    }
}
