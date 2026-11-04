mod check;
mod convert;
mod ledger;
mod markers;
mod source;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::convert::build_archive;
use crate::ledger::{open_read_only, scan_range};

#[derive(Parser)]
#[command(name = "hivezilla-ledger-archive")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check one inclusive slot range in a snapshot or stopped ledger.
    CheckLedgerRange {
        ledger: PathBuf,
        #[arg(long)]
        start_slot: u64,
        #[arg(long)]
        end_slot: u64,
    },
    /// Write a Compact V2 hot archive for one inclusive slot range.
    BuildArchiveV2FromLedger {
        ledger: PathBuf,
        output: PathBuf,
        #[arg(long)]
        start_slot: u64,
        #[arg(long)]
        end_slot: u64,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    match Cli::parse().command {
        Command::CheckLedgerRange {
            ledger,
            start_slot,
            end_slot,
        } => {
            let blockstore = open_read_only(&ledger)?;
            let inspected = scan_range(&blockstore, start_slot, end_slot, |_| Ok(()))?;
            print_report(&inspected);
            Ok(())
        }
        Command::BuildArchiveV2FromLedger {
            ledger,
            output,
            start_slot,
            end_slot,
        } => {
            let inspected = build_archive(&ledger, &output, start_slot, end_slot)?;
            print_report(&inspected);
            println!("Output: {}", output.display());
            Ok(())
        }
    }
}

fn print_report(inspected: &check::Inspected) {
    let first = inspected.block_slots.first().copied().unwrap_or(0);
    let last = inspected.block_slots.last().copied().unwrap_or(0);
    println!("Blocks: {}", inspected.block_slots.len());
    println!("Proven skips: {}", inspected.skips.len());
    println!("Abandoned forks: {}", inspected.forks.len());
    println!("First block: {first}");
    println!("Last block: {last}");
    println!(
        "Boundary hash: {}",
        bs58::encode(inspected.boundary_hash).into_string()
    );
}
