mod check;
mod convert;
mod ledger;
mod markers;
mod notify;
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
    /// Write one epoch under OUTPUT/epoch-N.
    ///
    /// The epoch uses 432000 slots. The directory is created only when the
    /// ledger still stores the first slot and a root has reached the last slot.
    /// An existing final archive file in that directory stops the command.
    /// Each rooted block is one independent zstd frame. The index is written
    /// after the frames. `--level` is the zstd effort, default 1.
    /// `--read-threads` is the number of parallel shred reads, from 1 to 32.
    /// A failure removes the temporary files and does not publish the index.
    /// A published epoch sends one Telegram line when the bot token and the
    /// chat id are both set. A Telegram failure still exits 0.
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
        /// Telegram bot token. The default is empty and sends nothing.
        #[arg(long, default_value = notify::DEFAULT_TELEGRAM_BOT_TOKEN)]
        telegram_bot_token: String,
        /// Telegram channel id. The default is empty and sends nothing.
        #[arg(
            long,
            default_value = notify::DEFAULT_TELEGRAM_CHAT_ID,
            allow_hyphen_values = true
        )]
        telegram_chat_id: String,
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
        Command::BuildEpochArchive {
            ledger,
            output,
            epoch,
            level,
            read_threads,
            telegram_bot_token,
            telegram_chat_id,
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
            notify::notify_published_epoch(
                epoch,
                &output,
                &telegram_bot_token,
                &telegram_chat_id,
            )?;
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
