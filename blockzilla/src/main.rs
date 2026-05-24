#![allow(clippy::too_many_arguments)]

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tracing::{Level, info};

mod archive_v2;
mod commands;
mod genesis_epoch0;
mod split_compact;
mod token_events;

use commands::{
    analyze::{analyze_epoch_file, print_epoch_report},
    dump_log_strings::dump_log_strings,
};

pub const BUFFER_SIZE: usize = 256 << 20;
pub const PROGRESS_REPORT_INTERVAL_SECS: u64 = 3;
pub const SLOTS_PER_EPOCH: u64 = 432_000;

#[derive(Parser)]
#[command(name = "blockzilla")]
#[command(about = "Build, inspect, and benchmark Blockzilla Archive V2 data")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Print progress every N blocks for legacy compact analyzers (0 disables).
    #[arg(long, default_value_t = 10_000)]
    progress_every: u64,

    /// Resume Archive V2 builders when complete sidecars already exist.
    #[arg(long, default_value_t = true, global = true)]
    resume: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Build semantic Solana Archive V2 with wincode/LEB128 records.
    BuildArchiveV2 {
        /// Input CAR or CAR.ZST file.
        input: PathBuf,
        /// Output directory for the archive-v2 files.
        output_dir: PathBuf,
        /// Previous epoch CAR/CAR.ZST used to seed strict recent blockhash resolution.
        #[arg(long)]
        previous_car: Option<PathBuf>,
    },

    /// Build semantic Archive V2 in one pass, storing pubkeys inline instead of using registries.
    BuildArchiveV2NoRegistry {
        /// Input CAR or CAR.ZST file.
        input: PathBuf,
        /// Output directory for the no-registry archive-v2 file.
        output_dir: PathBuf,
    },

    /// Build blockhash_registry.bin plus blockhash_index_v3.bin from a CAR file, skipping tx/metadata decode.
    BuildBlockhashRegistry {
        /// Input CAR or CAR.ZST file.
        input: PathBuf,
        /// Output directory for blockhash_registry.bin and blockhash_index_v3.bin.
        output_dir: PathBuf,
        /// Rewrite sidecars even when they already exist.
        #[arg(long)]
        force: bool,
    },

    /// Build a pubkey registry from a no-registry Archive V2, then rewrite it with compact IDs.
    OptimizeArchiveV2NoRegistry {
        /// Input archive-v2-no-registry.wincode file.
        input: PathBuf,
        /// Output directory for registry.bin and archive-v2.wincode.
        output_dir: PathBuf,
        /// Previous epoch CAR/CAR.ZST used only to seed the recent blockhash window.
        #[arg(long)]
        previous_car: Option<PathBuf>,
    },

    /// Benchmark sequential Archive V2 read/decode speed.
    BenchArchiveV2 {
        /// Input archive-v2.wincode file.
        input: PathBuf,
        /// Number of full sequential passes to run.
        #[arg(long, default_value_t = 1)]
        iterations: usize,
    },

    /// Benchmark sequential no-registry Archive V2 read/decode speed.
    BenchArchiveV2NoRegistry {
        /// Input archive-v2-no-registry.wincode file.
        input: PathBuf,
        /// Number of full sequential passes to run.
        #[arg(long, default_value_t = 1)]
        iterations: usize,
    },

    /// Build a physical block-offset sidecar for indexed Archive V2 reads.
    BuildArchiveV2Index {
        /// Input archive-v2.wincode file.
        input: PathBuf,
        /// Output index path. Defaults to <input-stem>.index next to the input archive.
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Benchmark indexed parallel Archive V2 block reads.
    BenchArchiveV2Indexed {
        /// Input archive-v2.wincode file.
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
        /// Input archive-v2.wincode file.
        input: PathBuf,
        /// Output directory for archive-v2-blocks.zstd, archive-v2-blocks.index, and archive-v2-meta.wincode.
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
        /// Input CAR or CAR.ZST file.
        input: PathBuf,
        /// Output directory for archive-v2-blocks.zstd and sidecars.
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
        /// Input archive-v2-blocks.zstd file.
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
        /// Input CAR or CAR.ZST file.
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
        /// Input archive-v2-blocks.zstd file.
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
        /// Input archive-v2-blocks.zstd file.
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

    /// Build per-block wincode access blobs for hot-block Archive V2.
    BuildArchiveV2BlockAccess {
        /// Input archive-v2-blocks.zstd or archive-v2-blocks.wincode file.
        input: PathBuf,
        /// Output directory for archive-v2-block-access.wincode and its index. Defaults beside input.
        #[arg(long)]
        output_dir: Option<PathBuf>,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Optional shared zstd dictionary bytes used when the input blocks were compressed.
        #[arg(long)]
        dict: Option<PathBuf>,
        /// Limit build to the first N block records.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Build a direct per-slot getBlock offset index for the hot-block and block-access blobs.
    BuildArchiveV2GetBlockIndex {
        /// Input archive-v2-blocks.zstd file.
        input: PathBuf,
        /// Hot-block index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Block-access index path. Defaults to archive-v2-block-access.index next to the input archive.
        #[arg(long)]
        access_index: Option<PathBuf>,
        /// Output index path. Defaults to archive-v2-get-block.index next to the input archive.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Epoch number. Defaults to the epoch inferred from the hot-block index.
        #[arg(long)]
        epoch: Option<u64>,
    },

    /// Benchmark raw hot-block Archive V2 reads. Supports raw and whole-file-zstd modes.
    BenchArchiveV2HotBlocksRaw {
        /// Input archive-v2-blocks.wincode or archive-v2-blocks.wincode.zst file.
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

    /// Dump compact USDC token-account events from CAR and hot-block Archive V2 sources.
    DumpUsdcTokenEvents {
        /// Input .car, archive-v2-blocks.zstd, or archive-v2-blocks.wincode.
        input: PathBuf,
        /// Output directory for events.bin, wallets.bin, token_accounts.bin, signatures.bin, and meta.txt.
        output_dir: PathBuf,
        /// Input storage format.
        #[arg(long, value_enum, default_value_t = TokenEventInputFormat::Auto)]
        format: TokenEventInputFormat,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Pubkey registry path. Defaults to registry.bin next to the input archive. For CAR inputs, pass the matching Blockzilla epoch registry.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Signatures sidecar path. Defaults to signatures.bin next to the input archive.
        #[arg(long)]
        signatures: Option<PathBuf>,
        /// Store wallet/token-account fields in local debug registries instead of Blockzilla registry ids.
        #[arg(long)]
        local_account_ids: bool,
        /// Worker threads for hot block archives in Blockzilla account-id mode.
        #[arg(long, default_value_t = 1)]
        workers: usize,
        /// Number of consecutive hot index rows claimed per worker batch.
        #[arg(long, default_value_t = 512)]
        chunk_size: usize,
        /// Mint to track. Defaults to native USDC.
        #[arg(long, default_value = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")]
        mint: String,
        /// Stop after N blocks. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Dump decoded token-program instructions from CAR and hot-block Archive V2 sources.
    DumpTokenInstructions {
        /// Input archive-v2-blocks.zstd or archive-v2-blocks.wincode.
        input: PathBuf,
        /// Output directory for token_instructions.bin and meta.txt.
        output_dir: PathBuf,
        /// Input storage format.
        #[arg(long, value_enum, default_value_t = TokenEventInputFormat::Auto)]
        format: TokenEventInputFormat,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Pubkey registry path. Defaults to registry.bin next to the input archive.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Worker threads for hot block archives.
        #[arg(long, default_value_t = 1)]
        workers: usize,
        /// Number of consecutive hot index rows claimed per worker batch.
        #[arg(long, default_value_t = 512)]
        chunk_size: usize,
        /// Optional direct mint filter. Unchecked transfers do not carry a mint and will be skipped by this filter.
        #[arg(long)]
        mint: Option<String>,
        /// Benchmark mode: scan and count token instructions but do not write token_instructions.bin.
        #[arg(long)]
        no_output: bool,
        /// Benchmark mode: scan only outer instructions and skip metadata/inner instructions.
        #[arg(long)]
        outer_only: bool,
        /// Stop after N blocks. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Fast-scan hot-block Archive V2 files for Pump.fun/program transaction touches.
    DumpPumpfunTransactions {
        /// Input archive-v2-blocks.zstd or archive-v2-blocks.wincode.
        input: PathBuf,
        /// Output directory for program_touches.bin shards and meta.txt.
        output_dir: PathBuf,
        /// Input storage format.
        #[arg(long, value_enum, default_value_t = TokenEventInputFormat::Auto)]
        format: TokenEventInputFormat,
        /// Index path. Defaults to archive-v2-blocks.index next to the input archive.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Pubkey registry path. Defaults to registry.bin next to the input archive.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Program id to match. Defaults to the Pump.fun program. May be repeated.
        #[arg(long = "program")]
        programs: Vec<String>,
        /// Worker threads for hot block archives.
        #[arg(long, default_value_t = 1)]
        workers: usize,
        /// Number of consecutive hot index rows claimed per worker batch.
        #[arg(long, default_value_t = 512)]
        chunk_size: usize,
        /// Benchmark mode: scan and count matches but do not write program_touches.bin.
        #[arg(long)]
        no_output: bool,
        /// Benchmark mode: scan only outer instructions and skip metadata/inner instructions.
        #[arg(long)]
        outer_only: bool,
        /// Stop after N blocks. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Extract the largest hot-block Archive V2 block into a single framed wincode record.
    ExtractLargestArchiveV2HotBlock {
        /// Input archive-v2-blocks.zstd file.
        input: PathBuf,
        /// Output single-block .wincode file.
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
        /// Input single-block .wincode file.
        input: PathBuf,
        /// Number of decode passes to run.
        #[arg(long, default_value_t = 100)]
        iterations: usize,
    },

    /// Reparse compact log streams in an Archive V2 file with the current log parser.
    ReparseArchiveV2Logs {
        /// Input archive-v2.wincode file.
        input: PathBuf,
        /// Output archive-v2.wincode file.
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
        /// Input archive-v2-blocks.zstd file.
        input: PathBuf,
        /// Output directory for archive-v2-blocks.zstd and archive-v2-blocks.index.
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
        /// Input archive-v2.wincode file.
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
        /// Input archive-v2-blocks.zstd file.
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
        /// Input archive-v2.wincode file.
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
        /// Input archive-v2-blocks.zstd file.
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
        /// Input archive-v2.wincode file.
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
        /// Input CAR or CAR.ZST file.
        input: PathBuf,
        /// Limit analysis to the first N block records.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Legacy analyzer for the removed compact.bin V1 format.
    AnalyzeCompact {
        /// Input file containing varint_u32_len + postcard(CompactBlockRecord).
        #[arg(short, long)]
        input: PathBuf,
        /// If set, stop after N blocks.
        #[arg(long)]
        limit_blocks: Option<u64>,
    },

    /// Legacy log-string dumper for the removed compact.bin V1 format.
    DumpCompactLogStrings {
        /// Input file containing varint_u32_len + postcard(CompactBlockRecord).
        #[arg(short, long)]
        input: PathBuf,
        /// Output path (defaults to stdout). Tip: /dev/null to benchmark parse-only.
        #[arg(long)]
        out: Option<PathBuf>,
        /// If set, stop after N blocks.
        #[arg(long)]
        limit_blocks: Option<u64>,
        /// If > 0, stop after writing N lines.
        #[arg(long, default_value_t = 0)]
        max_lines: u64,
        /// Also dump decoded data table entries as base64 strings.
        #[arg(long, default_value_t = false)]
        include_data: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum TokenEventInputFormat {
    Auto,
    Car,
    CarZstd,
    HotZstd,
    HotRaw,
    HotRawZstd,
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

    match cli.command {
        Commands::BuildArchiveV2 {
            input,
            output_dir,
            previous_car,
        } => archive_v2::build(&input, &output_dir, previous_car.as_deref(), resume),
        Commands::BuildArchiveV2NoRegistry { input, output_dir } => {
            archive_v2::build_no_registry(&input, &output_dir)
        }
        Commands::BuildBlockhashRegistry {
            input,
            output_dir,
            force,
        } => archive_v2::build_blockhash_registry(&input, &output_dir, force),
        Commands::OptimizeArchiveV2NoRegistry {
            input,
            output_dir,
            previous_car,
        } => archive_v2::optimize_no_registry(&input, &output_dir, previous_car.as_deref(), resume),
        Commands::BenchArchiveV2 { input, iterations } => archive_v2::bench(&input, iterations),
        Commands::BenchArchiveV2NoRegistry { input, iterations } => {
            archive_v2::bench_no_registry(&input, iterations)
        }
        Commands::BuildArchiveV2Index { input, output } => {
            archive_v2::build_index(&input, output.as_deref())
        }
        Commands::BenchArchiveV2Indexed {
            input,
            index,
            workers,
            chunk_size,
        } => archive_v2::bench_indexed(&input, index.as_deref(), workers, chunk_size),
        Commands::RepackArchiveV2ZstdBlocks {
            input,
            output_dir,
            level,
            dict,
            max_blocks,
        } => {
            archive_v2::repack_zstd_blocks(&input, &output_dir, level, dict.as_deref(), max_blocks)
        }
        Commands::BuildArchiveV2HotBlocks {
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
        Commands::BenchArchiveV2HotBlocks {
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
        Commands::BenchCarArchive {
            input,
            format,
            max_blocks,
        } => archive_v2::bench_car_archive(&input, format, max_blocks),
        Commands::BenchArchiveV2HotBlockAccounts {
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
        Commands::RepackArchiveV2HotBlocksRaw {
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
        Commands::BuildArchiveV2BlockAccess {
            input,
            output_dir,
            index,
            dict,
            max_blocks,
        } => archive_v2::build_block_access_sidecar(
            &input,
            output_dir.as_deref(),
            index.as_deref(),
            dict.as_deref(),
            max_blocks,
        ),
        Commands::BuildArchiveV2GetBlockIndex {
            input,
            index,
            access_index,
            output,
            epoch,
        } => archive_v2::build_get_block_index(
            &input,
            index.as_deref(),
            access_index.as_deref(),
            output.as_deref(),
            epoch,
        ),
        Commands::BenchArchiveV2HotBlocksRaw {
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
        Commands::DumpUsdcTokenEvents {
            input,
            output_dir,
            format,
            index,
            registry,
            signatures,
            local_account_ids,
            workers,
            chunk_size,
            mint,
            max_blocks,
        } => token_events::dump_usdc_token_events(token_events::TokenEventDumpConfig {
            input,
            output_dir,
            format,
            index,
            registry,
            signatures,
            blockzilla_account_ids: !local_account_ids,
            workers,
            chunk_size,
            mint,
            max_blocks,
        }),
        Commands::DumpTokenInstructions {
            input,
            output_dir,
            format,
            index,
            registry,
            workers,
            chunk_size,
            mint,
            no_output,
            outer_only,
            max_blocks,
        } => token_events::dump_token_instructions(token_events::TokenInstructionDumpConfig {
            input,
            output_dir,
            format,
            index,
            registry,
            workers,
            chunk_size,
            mint_filter: mint,
            no_output,
            outer_only,
            max_blocks,
        }),
        Commands::DumpPumpfunTransactions {
            input,
            output_dir,
            format,
            index,
            registry,
            programs,
            workers,
            chunk_size,
            no_output,
            outer_only,
            max_blocks,
        } => token_events::dump_pumpfun_transactions(token_events::ProgramTransactionDumpConfig {
            input,
            output_dir,
            format,
            index,
            registry,
            programs,
            workers,
            chunk_size,
            no_output,
            outer_only,
            max_blocks,
        }),
        Commands::ExtractLargestArchiveV2HotBlock {
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
        Commands::BenchArchiveV2HotBlock { input, iterations } => {
            archive_v2::bench_single_block(&input, iterations)
        }
        Commands::ReparseArchiveV2Logs {
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
        Commands::RepackArchiveV2HotLogs {
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
        Commands::AnalyzeArchiveV2Logs {
            input,
            registry,
            max_blocks,
            top,
            max_keys,
        } => archive_v2::analyze_logs(&input, registry.as_deref(), max_blocks, top, max_keys),
        Commands::AnalyzeArchiveV2HotLogs {
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
        Commands::AnalyzeArchiveV2InstructionData {
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
        Commands::AnalyzeArchiveV2HotInstructionData {
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
        Commands::InspectArchiveV2 {
            input,
            max_blocks,
            top,
        } => archive_v2::inspect(&input, max_blocks, top),
        Commands::InspectCarOrder { input, max_blocks } => {
            archive_v2::inspect_car_order(&input, max_blocks)
        }
        Commands::AnalyzeCompact {
            input,
            limit_blocks,
        } => analyze_epoch_file(&input, cli.progress_every, limit_blocks)
            .map(|r| print_epoch_report(&r)),
        Commands::DumpCompactLogStrings {
            input,
            out,
            limit_blocks,
            max_lines,
            include_data,
        } => dump_log_strings(
            &input,
            out.as_deref(),
            limit_blocks,
            cli.progress_every,
            max_lines,
            include_data,
        ),
    }
}

pub(crate) fn file_nonempty(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.len() > 0)
        .unwrap_or(false)
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

                info!(
                    "[{}] progress={:.1}% ETA={} | blocks={} ({:.0} blk/s) txs={} ({:.0} tx/s) | slots={}-{} ({}) | elapsed={}",
                    self.phase,
                    progress_pct,
                    format_duration(eta_seconds),
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
