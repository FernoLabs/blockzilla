mod check;
mod convert;
mod ledger;
mod markers;
mod source;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::convert::build_archive;
use crate::ledger::{
    ensure_epoch_is_stored, ensure_no_published_archive_file, epoch_output_dir, epoch_slot_range,
    open_read_only, scan_range,
};

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
        /// zstd compression level for each independent block frame.
        #[arg(long, default_value_t = convert::ZSTD_LEVEL)]
        level: i32,
        /// Parallel shred reads. 1 reads one slot at a time.
        #[arg(long, default_value_t = 1)]
        read_threads: u8,
    },
    /// Write one epoch under OUTPUT/epoch-N.
    ///
    /// The epoch uses 432000 slots. The directory is created only when the
    /// ledger still stores the first slot and a root has reached the last slot.
    BuildEpochArchive {
        ledger: PathBuf,
        /// Parent directory. The archive is written to epoch-N inside it.
        output: PathBuf,
        #[arg(long)]
        epoch: u64,
        /// zstd compression level for each independent block frame.
        #[arg(long, default_value_t = convert::ZSTD_LEVEL)]
        level: i32,
        /// Parallel shred reads. 1 reads one slot at a time.
        #[arg(long, default_value_t = 1)]
        read_threads: u8,
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
            let inspected = scan_range(&blockstore, start_slot, end_slot, 1, |_| Ok(()))?;
            print_report(&inspected);
            Ok(())
        }
        Command::BuildArchiveV2FromLedger {
            ledger,
            output,
            start_slot,
            end_slot,
            level,
            read_threads,
        } => {
            let inspected =
                build_archive(&ledger, &output, start_slot, end_slot, level, read_threads)?;
            print_report(&inspected);
            println!("Output: {}", output.display());
            Ok(())
        }
        Command::BuildEpochArchive {
            ledger,
            output,
            epoch,
            level,
            read_threads,
        } => {
            let (start_slot, end_slot) = epoch_slot_range(epoch)?;
            let blockstore = open_read_only(&ledger)?;
            ensure_epoch_is_stored(
                epoch,
                start_slot,
                end_slot,
                blockstore.lowest_slot(),
                blockstore.max_root(),
            )?;
            drop(blockstore);
            let output = epoch_output_dir(&output, epoch);
            ensure_no_published_archive_file(&output)?;
            let inspected =
                build_archive(&ledger, &output, start_slot, end_slot, level, read_threads)?;
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
