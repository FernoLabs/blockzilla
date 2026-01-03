use anyhow::Result;
use clap::{Parser, Subcommand};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tracing::{Level, info};

pub const BUFFER_SIZE: usize = 256 << 20;
pub const PROGRESS_REPORT_INTERVAL_SECS: u64 = 3;
pub const SLOTS_PER_EPOCH: u64 = 432_000;

mod build;
mod build_all;
mod build_blockhash_registry;
mod build_registry;
mod compact;

pub(crate) fn file_nonempty(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.len() > 0)
        .unwrap_or(false)
}

// ----- CLI -----

#[derive(Parser)]
#[command(name = "blockzilla-optimizer")]
#[command(
    about = "Two-pass optimizer: build registry (counts) then build compact (postcard framed)"
)]
#[command(version)]
pub(crate) struct Cli {
    /// Cache directory containing CAR files
    #[arg(long, default_value = "cache", global = true)]
    pub(crate) cache_dir: PathBuf,

    /// Output directory for blockzilla archives
    #[arg(long, default_value = "blockzilla-v1", global = true)]
    pub(crate) output_dir: PathBuf,

    /// Resume: if outputs exist, skip finished phases
    #[arg(long, default_value_t = true, global = true)]
    pub(crate) resume: bool,

    /// Skip transactions that fail decode/convert instead of aborting the whole run
    #[arg(long, default_value_t = false, global = true)]
    pub(crate) skip_bad_txs: bool,

    #[command(subcommand)]
    pub(crate) cmd: Cmd,
}

#[derive(Subcommand)]
pub(crate) enum Cmd {
    /// Run both passes: build registry.bin then compact.bin
    Build {
        /// Epoch number
        epoch: u64,

        /// Keep registry and compact, do not remove anything
        #[arg(long, default_value_t = true)]
        keep: bool,
    },

    /// Pass 1 only: build registry.bin from CAR
    BuildRegistry {
        epoch: u64,
    },

    BuildBlockhashRegistry {
        epoch: u64,
    },

    /// Pass 2 only: build compact.bin from CAR + registry.bin
    Compact {
        epoch: u64,
    },

    /// Process all epochs found in the cache directory
    BuildAll,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();

    match cli.cmd {
        Cmd::Build { epoch, .. } => build::run(&cli, epoch),
        Cmd::BuildRegistry { epoch } => build_registry::run(&cli, epoch),
        Cmd::BuildBlockhashRegistry { epoch } => build_blockhash_registry::run(&cli, epoch),

        Cmd::Compact { epoch } => compact::run(&cli, epoch),
        Cmd::BuildAll => build_all::run(&cli),
    }
}

pub(crate) fn epoch_paths(cli: &Cli, epoch: u64) -> (PathBuf, PathBuf, PathBuf, PathBuf, PathBuf) {
    let car_path = cli.cache_dir.join(format!("epoch-{}.car.zst", epoch));
    let epoch_dir = cli.output_dir.join(format!("epoch-{}", epoch));
    let registry_path = epoch_dir.join("registry.bin");
    let bh_path = epoch_dir.join("blockhash_registry.bin");
    let compact_path = epoch_dir.join("compact.bin");
    (car_path, epoch_dir, registry_path, bh_path, compact_path)
}

// ----- progress -----

pub(crate) fn format_duration(seconds: f64) -> String {
    let total_secs = seconds as u64;

    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let minutes = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    if days > 0 {
        format!("{}d {}h {}m {}s", days, hours, minutes, secs)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}

pub(crate) struct ProgressTracker {
    start_time: Instant,
    last_report: Instant,
    blocks: u64,
    txs: u64,
    report_interval: Duration,
    estimated_total_blocks: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    blocks_since_report: u64,
    txs_since_report: u64,
    phase: &'static str,
}

impl ProgressTracker {
    pub(crate) fn new(phase: &'static str) -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            last_report: now,
            blocks: 0,
            txs: 0,
            report_interval: Duration::from_secs(PROGRESS_REPORT_INTERVAL_SECS),
            estimated_total_blocks: SLOTS_PER_EPOCH,
            first_slot: None,
            last_slot: None,
            blocks_since_report: 0,
            txs_since_report: 0,
            phase,
        }
    }

    #[inline(always)]
    pub(crate) fn update_slot(&mut self, slot: u64) {
        if self.first_slot.is_none() {
            self.first_slot = Some(slot);
        }
        self.last_slot = Some(slot);
    }

    #[inline(always)]
    pub(crate) fn update(&mut self, blocks_delta: u64, txs_delta: u64) {
        self.blocks += blocks_delta;
        self.txs += txs_delta;
        self.blocks_since_report += blocks_delta;
        self.txs_since_report += txs_delta;

        let now = Instant::now();
        if now.duration_since(self.last_report) >= self.report_interval {
            self.report();
            self.last_report = now;
            self.blocks_since_report = 0;
            self.txs_since_report = 0;
        }
    }

    fn report(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed < 0.001 {
            return;
        }

        let blocks_per_sec = self.blocks as f64 / elapsed;
        let txs_per_sec = self.txs as f64 / elapsed;

        if let (Some(first), Some(last)) = (self.first_slot, self.last_slot) {
            let slots_processed = last.saturating_sub(first);
            let progress_pct =
                (slots_processed as f64 / self.estimated_total_blocks as f64) * 100.0;

            if blocks_per_sec > 0.0 && slots_processed > 0 {
                let slots_remaining = self.estimated_total_blocks.saturating_sub(slots_processed);
                let blocks_per_slot = self.blocks as f64 / slots_processed as f64;
                let estimated_remaining_blocks = (slots_remaining as f64 * blocks_per_slot) as u64;
                let eta_seconds = estimated_remaining_blocks as f64 / blocks_per_sec;

                let global_eta_msg = if self.phase == "Phase 1/2" {
                    let global_eta = (eta_seconds + elapsed) * 2.0;
                    format!(" (global ETA: {})", format_duration(global_eta))
                } else {
                    String::new()
                };

                info!(
                    "[{}] progress={:.1}% ETA={}{} | blocks={} ({:.0} blk/s) txs={} ({:.0} tx/s) | slots={}-{} ({}) | elapsed={}",
                    self.phase,
                    progress_pct,
                    format_duration(eta_seconds),
                    global_eta_msg,
                    self.blocks,
                    blocks_per_sec,
                    self.txs,
                    txs_per_sec,
                    first,
                    last,
                    slots_processed,
                    format_duration(elapsed)
                );
            } else {
                info!(
                    "[{}] progress={:.1}% | blocks={} ({:.0} blk/s) txs={} ({:.0} tx/s) | slots={}-{} ({}) | elapsed={}",
                    self.phase,
                    progress_pct,
                    self.blocks,
                    blocks_per_sec,
                    self.txs,
                    txs_per_sec,
                    first,
                    last,
                    slots_processed,
                    format_duration(elapsed)
                );
            }
        } else {
            info!(
                "[{}] blocks={} ({:.0} blk/s) txs={} ({:.0} tx/s) | elapsed={}",
                self.phase,
                self.blocks,
                blocks_per_sec,
                self.txs,
                txs_per_sec,
                format_duration(elapsed)
            );
        }
    }

    pub(crate) fn final_report(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let blocks_per_sec = self.blocks as f64 / elapsed;
        let txs_per_sec = self.txs as f64 / elapsed;

        let mut msg = format!(
            "[{}] Complete: blocks={} txs={} | {:.0} blk/s, {:.0} tx/s | elapsed={}",
            self.phase,
            self.blocks,
            self.txs,
            blocks_per_sec,
            txs_per_sec,
            format_duration(elapsed)
        );

        if let (Some(first), Some(last)) = (self.first_slot, self.last_slot) {
            let slots_processed = last.saturating_sub(first);
            msg.push_str(&format!(
                " | slots={}-{} ({})",
                first, last, slots_processed
            ));
        }

        info!("{}", msg);
    }
}

pub fn derived_uncompressed_path(car_path: &Path) -> Option<PathBuf> {
    let name = car_path.file_name()?.to_string_lossy();

    if name.ends_with(".car.zst") {
        let base = name.strip_suffix(".zst").unwrap();
        return Some(car_path.with_file_name(base));
    }

    None
}
