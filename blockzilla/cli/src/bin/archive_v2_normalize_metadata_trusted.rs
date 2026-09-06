//! Repair historical Compact Archive V2 metadata in one fresh local staging directory.

#[path = "../archive_v2/trusted_metadata_normalize.rs"]
mod trusted_metadata_normalize;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use trusted_metadata_normalize::{TrustedMetadataNormalizeOptions, normalize_trusted_metadata};

const DEFAULT_SLOTS_PER_EPOCH: u64 = 432_000;
const DEFAULT_MAX_METADATA_BYTES: usize = 64 << 20;
const DEFAULT_PROGRESS_BLOCKS: u64 = 10_000;

#[derive(Debug, Parser)]
#[command(
    name = "archive-v2-normalize-metadata-trusted",
    version,
    about = "Repair historical Archive V2 metadata into a fresh local staging directory"
)]
struct Args {
    /// Existing local Archive V2 epoch directory. It is never changed.
    #[arg(long)]
    source: PathBuf,

    /// Fresh output directory. It must not exist and must not be inside source.
    #[arg(long)]
    staging: PathBuf,

    /// Exact source and target epoch.
    #[arg(long)]
    epoch: u64,

    /// Fixed number of slots in the epoch.
    #[arg(long, default_value_t = DEFAULT_SLOTS_PER_EPOCH)]
    slots_per_epoch: u64,

    /// Zstd level for repaired block frames.
    #[arg(long, default_value_t = 1)]
    zstd_level: i32,

    /// Maximum accepted bytes for one metadata record.
    #[arg(long, default_value_t = DEFAULT_MAX_METADATA_BYTES)]
    max_metadata_bytes: usize,

    /// Print progress after this many blocks. Zero disables progress.
    #[arg(long, default_value_t = DEFAULT_PROGRESS_BLOCKS)]
    progress_blocks: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let summary = normalize_trusted_metadata(&TrustedMetadataNormalizeOptions {
        source: args.source,
        staging: args.staging,
        epoch: args.epoch,
        slots_per_epoch: args.slots_per_epoch,
        zstd_level: args.zstd_level,
        max_metadata_bytes: args.max_metadata_bytes,
        progress_blocks: args.progress_blocks,
    })?;
    println!("{summary}");
    Ok(())
}
