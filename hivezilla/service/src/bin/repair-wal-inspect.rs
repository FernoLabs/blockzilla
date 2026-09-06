//! Read-only validation and exact terminal-cursor discovery for an accepted repair-shred WAL.

#[allow(dead_code)]
#[path = "shred_epoch_audit/repair_wal.rs"]
mod repair_wal;

use std::{io, path::PathBuf};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use repair_wal::{RepairWalCompleteScanConfig, scan_complete_repair_wal};

#[derive(Debug, Parser)]
#[command(about = "Validate a frozen accepted-repair WAL and print its exact terminal cursor")]
struct Args {
    /// Accepted-repair WAL base file, or a directory containing exactly one base file.
    #[arg(long)]
    repair_wal: PathBuf,
    /// Optional inclusive slot coverage lower bound reported by the scan.
    #[arg(long, default_value_t = 0)]
    min_slot: u64,
    /// Optional inclusive slot coverage upper bound reported by the scan.
    #[arg(long, default_value_t = u64::MAX)]
    max_slot: u64,
    #[arg(long, default_value_t = 1_000_000)]
    max_records: u64,
    #[arg(long, default_value_t = 1_073_741_824)]
    max_payload_bytes: u64,
    #[arg(long, default_value_t = 4096)]
    max_segments: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.min_slot <= args.max_slot, "min-slot exceeds max-slot");
    let config = RepairWalCompleteScanConfig {
        path: args.repair_wal,
        max_records: args.max_records,
        max_payload_bytes: args.max_payload_bytes,
        max_segments: args.max_segments,
    };
    let report = scan_complete_repair_wal(&config, args.min_slot, args.max_slot, |_| Ok(()))?;
    serde_json::to_writer_pretty(io::stdout().lock(), &report)
        .context("encode repair WAL inspection report")?;
    println!();
    Ok(())
}
