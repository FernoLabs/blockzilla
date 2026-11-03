mod check;
mod convert;
mod ledger;
mod markers;
mod source;

use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};

use crate::check::inspect;
use crate::convert::write_archive;
use crate::ledger::load_range;

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
            let inspected = inspect_ledger(&ledger, start_slot, end_slot)?;
            print_report(&inspected);
            Ok(())
        }
        Command::BuildArchiveV2FromLedger {
            ledger,
            output,
            start_slot,
            end_slot,
        } => {
            let (view, inspected) = load_and_inspect(&ledger, start_slot, end_slot)?;
            write_archive(&output, &view, &inspected)?;
            print_report(&inspected);
            println!("Output: {}", output.display());
            Ok(())
        }
    }
}

fn inspect_ledger(ledger: &std::path::Path, start_slot: u64, end_slot: u64) -> Result<check::Inspected> {
    Ok(load_and_inspect(ledger, start_slot, end_slot)?.1)
}

fn load_and_inspect(
    ledger: &std::path::Path,
    start_slot: u64,
    end_slot: u64,
) -> Result<(source::LedgerView, check::Inspected)> {
    if start_slot > end_slot {
        bail!("start slot {start_slot} is after end slot {end_slot}");
    }
    let view = load_range(ledger, start_slot, end_slot)?;
    let inspected = inspect(&view)?;
    Ok((view, inspected))
}

fn print_report(inspected: &check::Inspected) {
    let first = inspected.block_slots.first().copied().unwrap_or(0);
    let last = inspected.block_slots.last().copied().unwrap_or(0);
    println!("Blocks: {}", inspected.block_slots.len());
    println!("Proven skips: {}", inspected.skips.len());
    println!("First block: {first}");
    println!("Last block: {last}");
    println!("Boundary hash: {}", bs58::encode(inspected.boundary_hash).into_string());
}
