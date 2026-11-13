mod check;
mod convert;
mod ledger;
mod markers;
mod notify;
mod publish;
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
    /// A published epoch sends one Telegram line when TELEGRAM_BOT_TOKEN and
    /// TELEGRAM_CHAT_ID are set. A Telegram failure still exits 0.
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
    /// Send one archived epoch to Cloudflare R2.
    ///
    /// The eleven published files go to s3://R2_BUCKET/R2_PREFIX/epoch-N/.
    /// R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET, R2_PREFIX, and
    /// R2_ENDPOINT or R2_S3_ENDPOINT come from the environment. A missing
    /// archive file stops the command before the first upload. An object of
    /// the same size stays in place. A different
    /// size stops the command. Telegram runs after every object is confirmed,
    /// and a Telegram failure still exits 0.
    #[command(name = "public-epoch-to-cf")]
    PublicEpochToCf {
        /// Parent directory. The archive is read from epoch-N inside it.
        output: PathBuf,
        #[arg(long)]
        epoch: u64,
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
            notify::notify_published_epoch(epoch, &output)?;
            Ok(())
        }
        Command::PublicEpochToCf { output, epoch } => {
            let settings = publish::resolve_settings(&|name| std::env::var(name).ok())?;
            let output = ledger::epoch_output_dir(&output, epoch);
            let files = publish::require_epoch_files(&output)?;
            publish::upload_epoch(&settings, epoch, &files)?;
            notify::notify_sent_to_cf(epoch, &output)?;
            let folder = publish::epoch_folder(&settings.prefix, epoch);
            println!("Output: s3://{}/{folder}", settings.bucket);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_epoch_to_cf_accepts_the_epoch_directory() {
        let cli = Cli::try_parse_from([
            "hivezilla-ledger-archive",
            "public-epoch-to-cf",
            "/home/sol/ledger-archive-out",
            "--epoch",
            "1084",
        ])
        .unwrap();
        match cli.command {
            Command::PublicEpochToCf { epoch, output, .. } => {
                assert_eq!(epoch, 1084);
                assert_eq!(output, PathBuf::from("/home/sol/ledger-archive-out"));
            }
            _ => panic!("wrong command"),
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
