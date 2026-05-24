#![allow(clippy::too_many_arguments)]

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tracing::{Level, info};

pub const BUFFER_SIZE: usize = 256 << 20;
pub const PROGRESS_REPORT_INTERVAL_SECS: u64 = 3;
pub const SLOTS_PER_EPOCH: u64 = 432_000;

mod archive_v2;
mod build;
mod build_all;
mod build_registry;
mod car_block_index;
mod compact;
mod genesis_epoch0;
mod lossless_v2;
mod pass1;
mod split_compact;

pub(crate) fn file_nonempty(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.len() > 0)
        .unwrap_or(false)
}

#[derive(Parser)]
#[command(name = "blockzilla-optimizer")]
#[command(
    about = "Legacy Blockzilla archive conversion CLI; use `blockzilla` for new Archive V2 work"
)]
#[command(version)]
pub(crate) struct Cli {
    /// Cache directory containing CAR files
    #[arg(long, default_value = "epochs", global = true)]
    pub(crate) cache_dir: PathBuf,

    /// Output directory for legacy blockzilla archives
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
    /// Legacy V1 path: build registry.bin then compact.bin
    Build {
        /// Epoch number
        epoch: u64,

        /// Keep registry and compact, do not remove anything
        #[arg(long, default_value_t = true)]
        keep: bool,
    },

    /// Pass 1 only: build registry.bin from CAR
    BuildRegistry { epoch: u64 },

    /// Legacy V1 path: build compact.bin from CAR + registry.bin
    Compact { epoch: u64 },

    /// Process all epochs found in the cache directory
    BuildAll,

    /// Build the additive lossless-v2 split archive from a .car or .car.zst file.
    BuildLosslessV2 {
        /// Input CAR or CAR.ZST file
        input: PathBuf,
        /// Output directory for historical/runtime/reconstruct files
        output_dir: PathBuf,
    },

    /// Reconstruct a CAR file from a lossless-v2 split archive directory.
    ExtractLosslessV2 {
        /// Input directory containing historical.bin, runtime.bin and reconstruct.bin
        input_dir: PathBuf,
        /// Output CAR file
        output: PathBuf,
    },

    /// Build the compressed first-pass archive with indexes and unoptimized metadata.
    BuildPass1 {
        /// Input CAR or CAR.ZST file
        input: PathBuf,
        /// Output directory for the pass1 archive
        output_dir: PathBuf,
    },

    /// Reconstruct a CAR file from a compressed first-pass archive directory.
    ExtractPass1 {
        /// Input directory containing historical.bin.zst, metadata-unoptimized.bin.zst and reconstruct.bin.zst
        input_dir: PathBuf,
        /// Output CAR file
        output: PathBuf,
    },

    /// Build a split compact archive with block data and runtime metadata separated.
    BuildSplitCompact {
        /// Input CAR or CAR.ZST file
        input: PathBuf,
        /// Output directory for the split compact archive
        output_dir: PathBuf,
    },

    /// Build semantic Solana Archive V2 with wincode/LEB128 records.
    BuildArchiveV2 {
        /// Input CAR or CAR.ZST file
        input: PathBuf,
        /// Output directory for the archive-v2 files
        output_dir: PathBuf,
        /// Previous epoch CAR/CAR.ZST used to seed strict recent blockhash resolution.
        #[arg(long)]
        previous_car: Option<PathBuf>,
    },

    /// Build semantic Archive V2 in one pass, storing pubkeys inline instead of using registries.
    BuildArchiveV2NoRegistry {
        /// Input CAR or CAR.ZST file
        input: PathBuf,
        /// Output directory for the no-registry archive-v2 file
        output_dir: PathBuf,
    },

    /// Build only PoH and blockhash sidecars from a CAR file, skipping tx/metadata decode.
    BuildBlockhashRegistry {
        /// Input CAR or CAR.ZST file
        input: PathBuf,
        /// Output directory for blockhash_registry.bin and poh.wincode
        output_dir: PathBuf,
    },

    /// Build a pubkey registry from a no-registry Archive V2, then rewrite it with compact IDs.
    OptimizeArchiveV2NoRegistry {
        /// Input archive-v2-no-registry.wincode file
        input: PathBuf,
        /// Output directory for registry.bin and archive-v2.wincode
        output_dir: PathBuf,
        /// Previous epoch CAR/CAR.ZST used only to seed the recent blockhash window.
        #[arg(long)]
        previous_car: Option<PathBuf>,
    },

    /// Benchmark sequential Archive V2 read/decode speed.
    BenchArchiveV2 {
        /// Input archive-v2.wincode file
        input: PathBuf,
        /// Number of full sequential passes to run
        #[arg(long, default_value_t = 1)]
        iterations: usize,
    },

    /// Benchmark sequential no-registry Archive V2 read/decode speed.
    BenchArchiveV2NoRegistry {
        /// Input archive-v2-no-registry.wincode file
        input: PathBuf,
        /// Number of full sequential passes to run
        #[arg(long, default_value_t = 1)]
        iterations: usize,
    },

    /// Build a physical block-offset sidecar for indexed Archive V2 reads.
    BuildArchiveV2Index {
        /// Input archive-v2.wincode file
        input: PathBuf,
        /// Output index path. Defaults to <input-stem>.index next to the input archive.
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Build a CAR block index and slot-range sidecar from one CAR/CAR.ZST file.
    BuildCarBlockIndex {
        /// Input CAR or CAR.ZST file
        input: PathBuf,
        /// Output directory for car-block-index.wincode and car-slot-ranges.raw
        output_dir: PathBuf,
        /// Epoch number for validating slot-range positions. Defaults to inferring from slots only.
        #[arg(long)]
        epoch: Option<u64>,
        /// Stop after N block records. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Build CAR block indexes for all epoch-*.car / epoch-*.car.zst files in cache.
    BuildCarBlockIndexes {
        /// First epoch to include.
        #[arg(long)]
        start_epoch: Option<u64>,
        /// Last epoch to include.
        #[arg(long)]
        end_epoch: Option<u64>,
        /// Number of epoch workers to run concurrently.
        #[arg(long, default_value_t = 2)]
        jobs: usize,
        /// Rebuild existing index files even when resume is enabled.
        #[arg(long, default_value_t = false)]
        overwrite: bool,
        /// Stop after N block records per epoch. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Benchmark indexed parallel Archive V2 block reads.
    BenchArchiveV2Indexed {
        /// Input archive-v2.wincode file
        input: PathBuf,
        /// Index path. Defaults to <input-stem>.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Worker threads for block decode.
        #[arg(long, default_value_t = 4)]
        workers: usize,
        /// Number of consecutive index rows claimed per worker batch.
        #[arg(long, default_value_t = 64)]
        chunk_size: usize,
    },

    /// Repack Archive V2 blocks as independently zstd-compressed blobs with an external index.
    RepackArchiveV2ZstdBlocks {
        /// Input archive-v2.wincode file
        input: PathBuf,
        /// Output directory for archive-v2-blocks.zstd, archive-v2-blocks.index, and archive-v2-meta.wincode
        output_dir: PathBuf,
        /// zstd compression level for each independent block frame.
        #[arg(long, default_value_t = 1)]
        level: i32,
        /// Optional shared zstd dictionary bytes. The same dictionary is required for reads.
        #[arg(long)]
        dict: Option<PathBuf>,
        /// Stop after N block records. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Build hot-block Archive V2 directly from CAR with signatures and vote hashes in sidecars.
    BuildArchiveV2HotBlocks {
        /// Input CAR or CAR.ZST file
        input: PathBuf,
        /// Output directory for archive-v2-blocks.zstd and sidecars
        output_dir: PathBuf,
        /// Previous epoch CAR/CAR.ZST used to seed strict recent blockhash resolution.
        #[arg(long)]
        previous_car: Option<PathBuf>,
        /// zstd compression level for each independent block frame.
        #[arg(long, default_value_t = 1)]
        level: i32,
        /// Stop after N block records. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Benchmark indexed parallel reads of hot-block Archive V2.
    BenchArchiveV2HotBlocks {
        /// Input archive-v2-blocks.zstd file
        input: PathBuf,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Optional shared zstd dictionary bytes used when the blocks were compressed.
        #[arg(long)]
        dict: Option<PathBuf>,
        /// Worker threads for compressed block read/decode.
        #[arg(long, default_value_t = 4)]
        workers: usize,
        /// Number of consecutive index rows claimed per worker batch.
        #[arg(long, default_value_t = 512)]
        chunk_size: usize,
    },

    /// Benchmark sequential CAR/CAR.ZST reads and node decoding.
    BenchCarArchive {
        /// Input CAR or CAR.ZST file
        input: PathBuf,
        /// Input storage format.
        #[arg(long, value_enum, default_value_t = CarBenchInputFormat::Auto)]
        format: CarBenchInputFormat,
        /// Stop after N block nodes. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Prototype a per-block impacted-account table from hot-block Archive V2.
    BenchArchiveV2HotBlockAccounts {
        /// Input archive-v2-blocks.zstd file
        input: PathBuf,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Pubkey registry path. Defaults to registry.bin next to the input archive.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Target pubkey to test block filtering against.
        #[arg(long)]
        target: Option<String>,
        /// Include metadata keys such as loaded addresses and token-balance mint/owner/program.
        #[arg(long)]
        include_metadata: bool,
        /// Worker threads for compressed block read/decode.
        #[arg(long, default_value_t = 4)]
        workers: usize,
        /// Number of consecutive index rows claimed per worker batch.
        #[arg(long, default_value_t = 64)]
        chunk_size: usize,
    },

    /// Convert independent zstd hot blocks to raw blocks plus a whole-file zstd companion.
    RepackArchiveV2HotBlocksRaw {
        /// Input archive-v2-blocks.zstd file
        input: PathBuf,
        /// Output directory for archive-v2-blocks.wincode, archive-v2-blocks.wincode.zst, and sidecars.
        output_dir: PathBuf,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Optional shared zstd dictionary bytes used when the input blocks were compressed.
        #[arg(long)]
        dict: Option<PathBuf>,
        /// Whole-file zstd compression level for archive-v2-blocks.wincode.zst.
        #[arg(long, default_value_t = 1)]
        level: i32,
        /// Limit repack to the first N block records.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Benchmark raw hot-block Archive V2 reads. Supports raw and whole-file-zstd modes.
    BenchArchiveV2HotBlocksRaw {
        /// Input archive-v2-blocks.wincode or archive-v2-blocks.wincode.zst file
        input: PathBuf,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Treat input as a whole-file zstd stream.
        #[arg(long)]
        whole_zstd: bool,
        /// Worker threads for raw random-read decode. Ignored for --whole-zstd.
        #[arg(long, default_value_t = 4)]
        workers: usize,
        /// Number of consecutive index rows claimed per worker batch.
        #[arg(long, default_value_t = 64)]
        chunk_size: usize,
    },

    /// Extract the largest hot-block Archive V2 block into a single framed wincode record.
    ExtractLargestArchiveV2HotBlock {
        /// Input archive-v2-blocks.zstd file
        input: PathBuf,
        /// Output single-block .wincode file
        output: PathBuf,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Optional shared zstd dictionary bytes used when the blocks were compressed.
        #[arg(long)]
        dict: Option<PathBuf>,
        /// Choose the largest block by transaction count instead of uncompressed byte length.
        #[arg(long)]
        by_tx_count: bool,
    },

    /// Benchmark decoding a single framed hot-block Archive V2 block record.
    BenchArchiveV2HotBlock {
        /// Input single-block .wincode file
        input: PathBuf,
        /// Number of decode passes to run.
        #[arg(long, default_value_t = 100)]
        iterations: usize,
    },

    /// Reparse compact log streams in an Archive V2 file with the current log parser.
    ReparseArchiveV2Logs {
        /// Input archive-v2.wincode file
        input: PathBuf,
        /// Output archive-v2.wincode file
        output: PathBuf,
        /// Pubkey registry path. Defaults to registry.bin next to the input archive.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Rebuild every log stream instead of only streams with raw/unknown log leaves.
        #[arg(long)]
        full: bool,
    },

    /// Reparse compact log streams in hot-block Archive V2 and rewrite zstd blocks.
    RepackArchiveV2HotLogs {
        /// Input archive-v2-blocks.zstd file
        input: PathBuf,
        /// Output directory for archive-v2-blocks.zstd and archive-v2-blocks.index
        output_dir: PathBuf,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Optional shared zstd dictionary bytes used when the blocks were compressed.
        #[arg(long)]
        dict: Option<PathBuf>,
        /// Pubkey registry path. Defaults to registry.bin next to the input archive.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Limit repack to the first N block records.
        #[arg(long)]
        max_blocks: Option<u64>,
        /// Zstd compression level for rewritten blocks.
        #[arg(long, default_value_t = 1)]
        level: i32,
        /// Rebuild every log stream instead of only streams with raw/unknown log leaves.
        #[arg(long)]
        full: bool,
    },

    /// Analyze raw/unknown Archive V2 log leaves and normalized repetition patterns.
    AnalyzeArchiveV2Logs {
        /// Input archive-v2.wincode file
        input: PathBuf,
        /// Pubkey registry path. Defaults to registry.bin next to the input archive when present.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Limit analysis to the first N block records.
        #[arg(long)]
        max_blocks: Option<u64>,
        /// Number of top rows to print per category.
        #[arg(long, default_value_t = 50)]
        top: usize,
        /// Keep at most this many distinct keys per exact/pattern bucket.
        #[arg(long, default_value_t = 200_000)]
        max_keys: usize,
    },

    /// Analyze raw/unknown log leaves in hot-block Archive V2.
    AnalyzeArchiveV2HotLogs {
        /// Input archive-v2-blocks.zstd file
        input: PathBuf,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Optional shared zstd dictionary bytes used when the blocks were compressed.
        #[arg(long)]
        dict: Option<PathBuf>,
        /// Pubkey registry path. Defaults to registry.bin next to the input archive when present.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Limit analysis to the first N block records.
        #[arg(long)]
        max_blocks: Option<u64>,
        /// Number of top rows to print per category.
        #[arg(long, default_value_t = 50)]
        top: usize,
        /// Keep at most this many distinct keys per exact/pattern bucket.
        #[arg(long, default_value_t = 200_000)]
        max_keys: usize,
    },

    /// Analyze Archive V2 instruction data bytes and repetition patterns.
    AnalyzeArchiveV2InstructionData {
        /// Input archive-v2.wincode file
        input: PathBuf,
        /// Pubkey registry path. Defaults to registry.bin next to the input archive when present.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Limit analysis to the first N block records.
        #[arg(long)]
        max_blocks: Option<u64>,
        /// Number of top rows to print per category.
        #[arg(long, default_value_t = 50)]
        top: usize,
        /// Keep at most this many distinct exact/prefix/program keys per bucket.
        #[arg(long, default_value_t = 300_000)]
        max_keys: usize,
        /// Number of leading instruction-data bytes to group as a prefix.
        #[arg(long, default_value_t = 8)]
        prefix_len: usize,
    },

    /// Analyze instruction data bytes and repetition patterns in hot-block Archive V2.
    AnalyzeArchiveV2HotInstructionData {
        /// Input archive-v2-blocks.zstd file
        input: PathBuf,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Optional shared zstd dictionary bytes used when the blocks were compressed.
        #[arg(long)]
        dict: Option<PathBuf>,
        /// Pubkey registry path. Defaults to registry.bin next to the input archive when present.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Limit analysis to the first N block records.
        #[arg(long)]
        max_blocks: Option<u64>,
        /// Number of top rows to print per category.
        #[arg(long, default_value_t = 50)]
        top: usize,
        /// Keep at most this many distinct exact/prefix/program keys per bucket.
        #[arg(long, default_value_t = 300_000)]
        max_keys: usize,
        /// Number of leading instruction-data bytes to group as a prefix.
        #[arg(long, default_value_t = 8)]
        prefix_len: usize,
    },

    /// Inspect Archive V2 block-level POH/reward storage.
    InspectArchiveV2 {
        /// Input archive-v2.wincode file
        input: PathBuf,
        /// Limit analysis to the first N block records.
        #[arg(long)]
        max_blocks: Option<u64>,
        /// Number of largest space buckets to print.
        #[arg(long, default_value_t = 24)]
        top: usize,
    },

    /// Inspect whether CAR transaction/entry/reward nodes are already in block order.
    InspectCarOrder {
        /// Input CAR or CAR.ZST file
        input: PathBuf,
        /// Limit analysis to the first N block records.
        #[arg(long)]
        max_blocks: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum CarBenchInputFormat {
    Auto,
    Car,
    CarZstd,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();
    let resume = cli.resume;

    match cli.cmd {
        Cmd::Build { epoch, .. } => build::run(&cli, epoch),
        Cmd::BuildRegistry { epoch } => build_registry::run(&cli, epoch),
        Cmd::Compact { epoch } => compact::run(&cli, epoch),
        Cmd::BuildAll => build_all::run(&cli),
        Cmd::BuildLosslessV2 { input, output_dir } => lossless_v2::build(&input, &output_dir),
        Cmd::ExtractLosslessV2 { input_dir, output } => lossless_v2::extract(&input_dir, &output),
        Cmd::BuildPass1 { input, output_dir } => pass1::build(&input, &output_dir),
        Cmd::ExtractPass1 { input_dir, output } => pass1::extract(&input_dir, &output),
        Cmd::BuildSplitCompact { input, output_dir } => {
            split_compact::build(&input, &output_dir, resume)
        }
        Cmd::BuildArchiveV2 {
            input,
            output_dir,
            previous_car,
        } => archive_v2::build(&input, &output_dir, previous_car.as_deref(), resume),
        Cmd::BuildArchiveV2NoRegistry { input, output_dir } => {
            archive_v2::build_no_registry(&input, &output_dir)
        }
        Cmd::BuildBlockhashRegistry { input, output_dir } => {
            archive_v2::build_blockhash_registry(&input, &output_dir)
        }
        Cmd::OptimizeArchiveV2NoRegistry {
            input,
            output_dir,
            previous_car,
        } => archive_v2::optimize_no_registry(&input, &output_dir, previous_car.as_deref(), resume),
        Cmd::BenchArchiveV2 { input, iterations } => archive_v2::bench(&input, iterations),
        Cmd::BenchArchiveV2NoRegistry { input, iterations } => {
            archive_v2::bench_no_registry(&input, iterations)
        }
        Cmd::BuildArchiveV2Index { input, output } => {
            archive_v2::build_index(&input, output.as_deref())
        }
        Cmd::BuildCarBlockIndex {
            input,
            output_dir,
            epoch,
            max_blocks,
        } => car_block_index::build(&input, &output_dir, epoch, max_blocks, resume).map(|_| ()),
        Cmd::BuildCarBlockIndexes {
            start_epoch,
            end_epoch,
            jobs,
            overwrite,
            max_blocks,
        } => car_block_index::build_all(&cli, start_epoch, end_epoch, jobs, overwrite, max_blocks),
        Cmd::BenchArchiveV2Indexed {
            input,
            index,
            workers,
            chunk_size,
        } => archive_v2::bench_indexed(&input, index.as_deref(), workers, chunk_size),
        Cmd::RepackArchiveV2ZstdBlocks {
            input,
            output_dir,
            level,
            dict,
            max_blocks,
        } => {
            archive_v2::repack_zstd_blocks(&input, &output_dir, level, dict.as_deref(), max_blocks)
        }
        Cmd::BuildArchiveV2HotBlocks {
            input,
            output_dir,
            previous_car,
            level,
            max_blocks,
        } => archive_v2::build_hot_blocks(
            &input,
            &output_dir,
            previous_car.as_deref(),
            level,
            max_blocks,
            resume,
        ),
        Cmd::BenchArchiveV2HotBlocks {
            input,
            index,
            dict,
            workers,
            chunk_size,
        } => archive_v2::bench_zstd_blocks(
            &input,
            index.as_deref(),
            dict.as_deref(),
            workers,
            chunk_size,
        ),
        Cmd::BenchCarArchive {
            input,
            format,
            max_blocks,
        } => archive_v2::bench_car_archive(&input, format, max_blocks),
        Cmd::BenchArchiveV2HotBlockAccounts {
            input,
            index,
            registry,
            target,
            include_metadata,
            workers,
            chunk_size,
        } => archive_v2::bench_hot_block_accounts(
            &input,
            index.as_deref(),
            registry.as_deref(),
            target.as_deref(),
            include_metadata,
            workers,
            chunk_size,
        ),
        Cmd::RepackArchiveV2HotBlocksRaw {
            input,
            output_dir,
            index,
            dict,
            level,
            max_blocks,
        } => archive_v2::repack_hot_blocks_raw(
            &input,
            &output_dir,
            index.as_deref(),
            dict.as_deref(),
            level,
            max_blocks,
        ),
        Cmd::BenchArchiveV2HotBlocksRaw {
            input,
            index,
            whole_zstd,
            workers,
            chunk_size,
        } => archive_v2::bench_raw_hot_blocks(
            &input,
            index.as_deref(),
            whole_zstd,
            workers,
            chunk_size,
        ),
        Cmd::ExtractLargestArchiveV2HotBlock {
            input,
            output,
            index,
            dict,
            by_tx_count,
        } => archive_v2::extract_largest_zstd_block(
            &input,
            index.as_deref(),
            dict.as_deref(),
            &output,
            by_tx_count,
        ),
        Cmd::BenchArchiveV2HotBlock { input, iterations } => {
            archive_v2::bench_single_block(&input, iterations)
        }
        Cmd::ReparseArchiveV2Logs {
            input,
            output,
            registry,
            full,
        } => archive_v2::reparse_logs(
            &input,
            &output,
            registry.as_deref(),
            if full {
                archive_v2::ArchiveV2LogReparseMode::Full
            } else {
                archive_v2::ArchiveV2LogReparseMode::Targeted
            },
        ),
        Cmd::RepackArchiveV2HotLogs {
            input,
            output_dir,
            index,
            dict,
            registry,
            max_blocks,
            level,
            full,
        } => archive_v2::repack_hot_logs(
            &input,
            &output_dir,
            index.as_deref(),
            dict.as_deref(),
            registry.as_deref(),
            max_blocks,
            level,
            if full {
                archive_v2::ArchiveV2LogReparseMode::Full
            } else {
                archive_v2::ArchiveV2LogReparseMode::Targeted
            },
        ),
        Cmd::AnalyzeArchiveV2Logs {
            input,
            registry,
            max_blocks,
            top,
            max_keys,
        } => archive_v2::analyze_logs(&input, registry.as_deref(), max_blocks, top, max_keys),
        Cmd::AnalyzeArchiveV2HotLogs {
            input,
            index,
            dict,
            registry,
            max_blocks,
            top,
            max_keys,
        } => archive_v2::analyze_hot_logs(
            &input,
            index.as_deref(),
            dict.as_deref(),
            registry.as_deref(),
            max_blocks,
            top,
            max_keys,
        ),
        Cmd::AnalyzeArchiveV2InstructionData {
            input,
            registry,
            max_blocks,
            top,
            max_keys,
            prefix_len,
        } => archive_v2::analyze_instruction_data(
            &input,
            registry.as_deref(),
            max_blocks,
            top,
            max_keys,
            prefix_len,
        ),
        Cmd::AnalyzeArchiveV2HotInstructionData {
            input,
            index,
            dict,
            registry,
            max_blocks,
            top,
            max_keys,
            prefix_len,
        } => archive_v2::analyze_hot_instruction_data(
            &input,
            index.as_deref(),
            dict.as_deref(),
            registry.as_deref(),
            max_blocks,
            top,
            max_keys,
            prefix_len,
        ),
        Cmd::InspectArchiveV2 {
            input,
            max_blocks,
            top,
        } => archive_v2::inspect(&input, max_blocks, top),
        Cmd::InspectCarOrder { input, max_blocks } => {
            archive_v2::inspect_car_order(&input, max_blocks)
        }
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
