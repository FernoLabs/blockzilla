#![allow(clippy::too_many_arguments)]

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::{Level, info};

mod archive_v2;
mod block_time_gaps;
mod car_preflight;
mod first_seen_finalization;
mod genesis_epoch0;
mod pre_hot;
mod split_compact;
mod token_events;

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

    /// Resume Archive V2 builders when complete sidecars already exist.
    #[arg(long, default_value_t = true, global = true)]
    resume: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Stream a CAR to clean EOF and atomically publish a bounded-memory structural receipt.
    PreflightCar {
        /// Input CAR or CAR.ZST file, including an in-progress `.car[.zst].part` path.
        input: PathBuf,
        /// Epoch expected for slots in this CAR.
        #[arg(long)]
        epoch: u64,
        /// JSON receipt path. Published atomically only after a successful full scan.
        #[arg(long)]
        receipt: PathBuf,
        /// I/O buffer size. Transaction payloads are skipped without allocation.
        #[arg(
            long,
            default_value_t = 8,
            value_parser = clap::value_parser!(u16).range(1..=256)
        )]
        io_buffer_mib: u16,
        /// Optional atomic progress JSON path for scheduler or monitoring integrations.
        #[arg(long)]
        progress_json: Option<PathBuf>,
    },

    /// Build a sparse slot-gap and block-time sidecar from a local Archive V2 timestamp index.
    BuildBlockTimeGaps {
        /// Input epoch directory or blockhash_index_v3.bin file. No RPC is used.
        input: PathBuf,
        /// Epoch expected for every indexed slot.
        #[arg(long)]
        epoch: u64,
        /// Timestamp source. Auto prefers V3 and otherwise scans Archive V2 hot blocks.
        #[arg(long, value_enum, default_value_t = BlockTimeGapSourceArg::Auto)]
        source: BlockTimeGapSourceArg,
        /// Output path. Defaults to block-time-gaps.bin beside the input index.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Atomic progress JSON for monitoring long Archive V2 scans.
        #[arg(long)]
        progress_json: Option<PathBuf>,
        /// Replace an existing sidecar when its source fingerprint or contents differ.
        #[arg(long)]
        force: bool,
    },

    /// Strictly validate an existing block-time gap sidecar without reading its archive source.
    VerifyBlockTimeGaps {
        /// block-time-gaps.bin path.
        input: PathBuf,
        /// Optional expected epoch.
        #[arg(long)]
        epoch: Option<u64>,
    },

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

    /// Stream a CAR/CAR.ZST URL into no-registry Archive V2 without storing the CAR locally.
    BuildArchiveV2NoRegistryFromUrl {
        /// Input CAR or CAR.ZST URL.
        url: String,
        /// Output directory for the no-registry archive-v2 file.
        output_dir: PathBuf,
    },

    /// Build only registry.bin, registry_counts.bin, and blockhash_registry.bin from a CAR/CAR.ZST.
    BuildArchiveV2Registries {
        /// Input CAR or CAR.ZST file.
        input: PathBuf,
        /// Output directory for registry.bin, registry_counts.bin, and blockhash_registry.bin.
        output_dir: PathBuf,
        /// Explicit slot/blockhash provenance file for documented PoH-gap blocks.
        #[arg(long)]
        external_blockhashes: Option<PathBuf>,
        /// Rewrite registries even when they already exist.
        #[arg(long)]
        force: bool,
    },

    /// Benchmark CAR pubkey-registry strategies without building the full archive.
    BenchCarRegistry {
        /// Input CAR or CAR.ZST file.
        input: PathBuf,
        /// Registry counting/finalization strategy to benchmark.
        #[arg(long, value_enum, default_value_t = CarRegistryBenchStrategyArg::ExactStream)]
        strategy: CarRegistryBenchStrategyArg,
        /// Initial hash-map capacity for exact/unique key storage.
        #[arg(long, default_value_t = 8_000_000)]
        initial_capacity: usize,
        /// Candidate capacity for the SpaceSaving heavy-hitter head.
        #[arg(long, default_value_t = 262_144)]
        heavy_hitter_capacity: usize,
        /// Optional output directory for registry sidecars from this strategy.
        #[arg(long)]
        output_dir: Option<PathBuf>,
        /// Stop after N block records. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
    },

    /// Build registry.mphf from an existing registry.bin so compact jobs can reload the account lookup.
    BuildArchiveV2RegistryIndex {
        /// Input registry.bin file.
        registry: PathBuf,
        /// Output registry index path. Defaults to registry.mphf next to registry.bin.
        #[arg(long)]
        output: Option<PathBuf>,
        /// Rewrite registry.mphf even when it already exists.
        #[arg(long)]
        force: bool,
    },

    /// Prepare registry.bin and registry_counts.bin from a live capture's sorted pubkey runs.
    PrepareArchiveV2LiveRegistry {
        /// Live producer capture directory.
        capture_dir: PathBuf,
        /// Output directory for registry.bin, registry_counts.bin, and the durable stage marker.
        output_dir: PathBuf,
    },

    /// Build the deferred MPHF for a completed first-seen scan and publish metadata last.
    FinalizeArchiveV2FirstSeen {
        /// Candidate output directory containing the scan-complete marker and sidecars.
        output_dir: PathBuf,
        /// Advisory lock shared by scan-only jobs and held exclusively by the finalizer.
        /// Defaults to a machine-wide lock in the system temporary directory.
        #[arg(long)]
        finalizer_lock: Option<PathBuf>,
    },

    /// Build blockhash_registry.bin, blockhash_index_v3.bin, and its gap sidecar from a CAR file.
    BuildBlockhashRegistry {
        /// Input CAR or CAR.ZST file.
        input: PathBuf,
        /// Output directory for blockhash_registry.bin, blockhash_index_v3.bin, and block-time-gaps.bin.
        output_dir: PathBuf,
        /// Explicit slot/blockhash provenance file for documented PoH-gap blocks.
        #[arg(long)]
        external_blockhashes: Option<PathBuf>,
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
        /// Existing Archive V2 registry sidecar directory to reuse.
        #[arg(long)]
        registry_dir: Option<PathBuf>,
        /// Explicit slot/blockhash provenance file for documented PoH-gap blocks.
        #[arg(long)]
        external_blockhashes: Option<PathBuf>,
        /// zstd compression level for each independent block frame.
        #[arg(long, default_value_t = 1)]
        level: i32,
        /// Stop after N block records. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
        /// Decode CAR records once into a compressed typed PreHot spool, then finalize from it.
        #[arg(long)]
        pre_hot: bool,
        /// Keep the compressed PreHot spool after a successful build for finalize-only benchmarks.
        #[arg(long)]
        keep_pre_hot: bool,
        /// Optional directory for the temporary PreHot spool (for example, local SSD scratch).
        #[arg(long)]
        pre_hot_dir: Option<PathBuf>,
        /// Initial exact pubkey-counter capacity for the PreHot extraction pass.
        #[arg(long, default_value_t = 8_000_000)]
        pre_hot_registry_capacity: usize,
        /// Explicitly finalize from an existing PreHot spool and matching sidecars.
        #[arg(long)]
        reuse_pre_hot: bool,
        /// Assign final pubkey IDs on first occurrence and write hot blocks in one CAR pass.
        #[arg(
            long,
            conflicts_with_all = ["pre_hot", "reuse_pre_hot", "registry_dir"]
        )]
        first_seen_registry: bool,
        /// Optional frequency-sorted registry.bin used to preseed short first-seen IDs.
        #[arg(long, requires = "first_seen_registry")]
        first_seen_seed_registry: Option<PathBuf>,
        /// Keys loaded from the input seed and written to registry-hot-seed.bin for the next epoch.
        #[arg(long, default_value_t = 65_536, requires = "first_seen_registry")]
        first_seen_seed_keys: usize,
        /// Expected unique-key capacity for the first-seen ID table.
        #[arg(long, default_value_t = 34_000_000, requires = "first_seen_registry")]
        first_seen_registry_capacity: usize,
        /// Parallel transaction/metadata decoders used only by the first-seen builder.
        #[arg(
            long,
            default_value_t = 4,
            requires = "first_seen_registry",
            value_parser = clap::value_parser!(u8).range(1..=8)
        )]
        first_seen_decode_workers: u8,
        /// Decompressed CAR prefetch chunk size per buffer (two fixed buffers), only for local .car.zst first-seen builds.
        /// Omit to use the measured 4 MiB default for .car.zst; pass 0 to disable.
        #[arg(
            long,
            requires = "first_seen_registry",
            value_parser = clap::value_parser!(u8).range(0..=64)
        )]
        car_zstd_prefetch_mib: Option<u8>,
        /// Stop after the CAR scan and durable sidecar writes, deferring the memory-intensive
        /// registry MPHF build and final metadata publication to finalize-archive-v2-first-seen.
        #[arg(long, requires = "first_seen_registry")]
        first_seen_scan_only: bool,
        /// Machine-wide first-seen memory lock. Scan-only jobs share it; inline builds and the
        /// deferred finalizer take it exclusively. Defaults to the system temporary directory.
        #[arg(long, requires = "first_seen_registry")]
        first_seen_finalizer_lock: Option<PathBuf>,
        /// Skip archive-v2-block-access.wincode and archive-v2-block-access.index generation.
        #[arg(long)]
        no_access: bool,
    },

    /// Build hot-block Archive V2 from a Hivezilla capture directory.
    BuildArchiveV2HotBlocksFromLive {
        /// Live producer capture directory.
        capture_dir: PathBuf,
        /// Output directory for archive-v2-blocks.zstd and sidecars.
        output_dir: PathBuf,
        /// zstd compression level for each independent block frame.
        #[arg(long, default_value_t = 1)]
        level: i32,
        /// Stop after N block records. Intended for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
        /// Pubkey registry source for live captures.
        #[arg(long, value_enum, default_value_t = LiveRegistrySourceArg::Auto)]
        registry_source: LiveRegistrySourceArg,
    },

    /// Materialize a published RPC-fallback live repair bundle without declaring it canonical.
    MaterializeArchiveV2LiveRepair {
        /// Published repair bundle containing REPAIR-REQUIRED.json and its merge plan.
        repair_dir: PathBuf,
        /// Fresh final output directory. Work is resumed through a hidden sibling staging dir.
        output_dir: PathBuf,
        /// Maximum accepted size of one retained getBlock JSON response.
        #[arg(
            long,
            default_value_t = 32,
            value_parser = clap::value_parser!(u16).range(1..=256)
        )]
        max_rpc_json_mib: u16,
        /// Flush and publish a durable restart checkpoint every N produced blocks.
        #[arg(
            long,
            default_value_t = 256,
            value_parser = clap::value_parser!(u32).range(1..=16384)
        )]
        checkpoint_every: u32,
        /// Maximum distinct keys retained in memory before spilling a sorted pubkey run.
        #[arg(
            long,
            default_value_t = 250_000,
            value_parser = clap::value_parser!(u32).range(1024..=4000000)
        )]
        pubkey_run_max_keys: u32,
        /// Stop after this many additional blocks, leaving a resumable hidden stage.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        max_blocks: Option<u64>,
    },

    /// Build a readable degraded hot archive from a validated repair materialization.
    BuildArchiveV2DegradedHotBlocksFromRepair {
        /// Completed noncanonical directory containing REPAIR-MATERIALIZED.json.
        materialized_dir: PathBuf,
        /// Fresh final output. The archive stays in a hidden sibling until its repair marker is durable.
        output_dir: PathBuf,
        /// Per-block zstd compression level.
        #[arg(long, default_value_t = 1)]
        level: i32,
        /// Build a hidden smoke prefix only. A bounded run is never published as complete.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        max_blocks: Option<u64>,
    },

    /// Add validated block-access/getBlock sidecars to a noncanonical repair hot archive.
    BuildArchiveV2RepairBlockAccess {
        /// Published repair bundle containing the authoritative REPAIR-REQUIRED.json.
        repair_dir: PathBuf,
        /// Existing degraded hot archive containing REPAIR-COMPACTED.json.
        hot_output: PathBuf,
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

    /// List CAR blocks whose block node has no PoH entry refs.
    FindPohGaps {
        /// Input CAR or CAR.ZST file.
        input: PathBuf,
        /// Optional TSV output path. Defaults to stdout.
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BlockTimeGapSourceArg {
    Auto,
    V3,
    Archive,
}

impl From<BlockTimeGapSourceArg> for block_time_gaps::BuildBlockTimeGapsSource {
    fn from(value: BlockTimeGapSourceArg) -> Self {
        match value {
            BlockTimeGapSourceArg::Auto => Self::Auto,
            BlockTimeGapSourceArg::V3 => Self::BlockhashIndexV3,
            BlockTimeGapSourceArg::Archive => Self::ArchiveV2Hot,
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CarRegistryBenchStrategyArg {
    ExactOld,
    ExactStream,
    UniqueSpaceSaving,
}

impl From<CarRegistryBenchStrategyArg> for split_compact::CarRegistryBenchStrategy {
    fn from(value: CarRegistryBenchStrategyArg) -> Self {
        match value {
            CarRegistryBenchStrategyArg::ExactOld => Self::ExactOld,
            CarRegistryBenchStrategyArg::ExactStream => Self::ExactStream,
            CarRegistryBenchStrategyArg::UniqueSpaceSaving => Self::UniqueSpaceSaving,
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum LiveRegistrySourceArg {
    /// Use live pubkey runs when present, then counts, then touches, otherwise scan live blocks.
    Auto,
    /// Require index/pubkey-counts.bin and build registry from it.
    Counts,
    /// Require index/pubkey-runs/*.bin and build registry by merging sorted count runs.
    Runs,
    /// Require index/pubkey-touches.bin and build registry by external sorting raw touches.
    Touches,
    /// Ignore live counts and scan live-no-registry-blocks.bin.
    Scan,
}

impl From<LiveRegistrySourceArg> for archive_v2::LiveRegistrySource {
    fn from(value: LiveRegistrySourceArg) -> Self {
        match value {
            LiveRegistrySourceArg::Auto => Self::Auto,
            LiveRegistrySourceArg::Counts => Self::Counts,
            LiveRegistrySourceArg::Runs => Self::Runs,
            LiveRegistrySourceArg::Touches => Self::Touches,
            LiveRegistrySourceArg::Scan => Self::Scan,
        }
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();
    let resume = cli.resume;

    match cli.command {
        Commands::PreflightCar {
            input,
            epoch,
            receipt,
            io_buffer_mib,
            progress_json,
        } => car_preflight::preflight_car(car_preflight::CarPreflightConfig {
            input: &input,
            epoch,
            receipt: &receipt,
            io_buffer_bytes: usize::from(io_buffer_mib) * 1024 * 1024,
            progress_json: progress_json.as_deref(),
        })
        .map(|_| ()),
        Commands::BuildBlockTimeGaps {
            input,
            epoch,
            source,
            output,
            progress_json,
            force,
        } => block_time_gaps::build_block_time_gaps(block_time_gaps::BuildBlockTimeGapsConfig {
            input: &input,
            epoch: Some(epoch),
            output: output.as_deref(),
            force,
            source: source.into(),
            progress_json: progress_json.as_deref(),
        })
        .map(|_| ()),
        Commands::VerifyBlockTimeGaps { input, epoch } => {
            block_time_gaps::verify_block_time_gaps(&input, epoch)
        }
        Commands::BuildArchiveV2 {
            input,
            output_dir,
            previous_car,
        } => archive_v2::build(&input, &output_dir, previous_car.as_deref(), resume),
        Commands::BuildArchiveV2NoRegistry { input, output_dir } => {
            archive_v2::build_no_registry(&input, &output_dir)
        }
        Commands::BuildArchiveV2NoRegistryFromUrl { url, output_dir } => {
            archive_v2::build_no_registry_from_url(&url, &output_dir)
        }
        Commands::BuildArchiveV2Registries {
            input,
            output_dir,
            external_blockhashes,
            force,
        } => archive_v2::build_registries(
            &input,
            &output_dir,
            external_blockhashes.as_deref(),
            force,
        ),
        Commands::BenchCarRegistry {
            input,
            strategy,
            initial_capacity,
            heavy_hitter_capacity,
            output_dir,
            max_blocks,
        } => split_compact::bench_car_registry(split_compact::CarRegistryBenchConfig {
            input: &input,
            output_dir: output_dir.as_deref(),
            strategy: strategy.into(),
            initial_capacity,
            heavy_hitter_capacity,
            max_blocks,
        }),
        Commands::BuildArchiveV2RegistryIndex {
            registry,
            output,
            force,
        } => archive_v2::build_registry_index(&registry, output.as_deref(), force),
        Commands::PrepareArchiveV2LiveRegistry {
            capture_dir,
            output_dir,
        } => archive_v2::prepare_live_registry_from_runs(&capture_dir, &output_dir, resume),
        Commands::FinalizeArchiveV2FirstSeen {
            output_dir,
            finalizer_lock,
        } => first_seen_finalization::finalize_first_seen_scan(
            &output_dir,
            finalizer_lock.as_deref(),
        ),
        Commands::BuildBlockhashRegistry {
            input,
            output_dir,
            external_blockhashes,
            force,
        } => archive_v2::build_blockhash_registry(
            &input,
            &output_dir,
            external_blockhashes.as_deref(),
            force,
        ),
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
            registry_dir,
            external_blockhashes,
            level,
            max_blocks,
            pre_hot,
            keep_pre_hot,
            pre_hot_dir,
            pre_hot_registry_capacity,
            reuse_pre_hot,
            first_seen_registry,
            first_seen_seed_registry,
            first_seen_seed_keys,
            first_seen_registry_capacity,
            first_seen_decode_workers,
            car_zstd_prefetch_mib,
            first_seen_scan_only,
            first_seen_finalizer_lock,
            no_access,
        } => {
            if first_seen_registry {
                let car_zstd_prefetch_mib = car_zstd_prefetch_mib.map_or_else(
                    || {
                        input
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.to_ascii_lowercase().ends_with(".car.zst"))
                            .then_some(4)
                            .unwrap_or(0)
                    },
                    usize::from,
                );
                let _memory_lock = if first_seen_scan_only {
                    first_seen_finalization::acquire_scan_lock(
                        first_seen_finalizer_lock.as_deref(),
                    )?
                } else {
                    first_seen_finalization::acquire_inline_build_lock(
                        first_seen_finalizer_lock.as_deref(),
                    )?
                };
                archive_v2::build_hot_blocks_first_seen(
                    &input,
                    &output_dir,
                    previous_car.as_deref(),
                    external_blockhashes.as_deref(),
                    level,
                    max_blocks,
                    resume,
                    !no_access,
                    first_seen_seed_registry.as_deref(),
                    first_seen_seed_keys,
                    first_seen_registry_capacity,
                    usize::from(first_seen_decode_workers),
                    car_zstd_prefetch_mib,
                    first_seen_scan_only,
                )
            } else if pre_hot {
                archive_v2::build_hot_blocks_pre_hot(
                    &input,
                    &output_dir,
                    previous_car.as_deref(),
                    registry_dir.as_deref(),
                    external_blockhashes.as_deref(),
                    level,
                    max_blocks,
                    resume,
                    !no_access,
                    keep_pre_hot,
                    pre_hot_dir.as_deref(),
                    pre_hot_registry_capacity,
                    reuse_pre_hot,
                )
            } else {
                archive_v2::build_hot_blocks(
                    &input,
                    &output_dir,
                    previous_car.as_deref(),
                    registry_dir.as_deref(),
                    external_blockhashes.as_deref(),
                    level,
                    max_blocks,
                    resume,
                    !no_access,
                )
            }
        }
        Commands::BuildArchiveV2HotBlocksFromLive {
            capture_dir,
            output_dir,
            level,
            max_blocks,
            registry_source,
        } => archive_v2::build_hot_blocks_from_live_capture(
            &capture_dir,
            &output_dir,
            level,
            max_blocks,
            resume,
            registry_source.into(),
        ),
        Commands::MaterializeArchiveV2LiveRepair {
            repair_dir,
            output_dir,
            max_rpc_json_mib,
            checkpoint_every,
            pubkey_run_max_keys,
            max_blocks,
        } => archive_v2::repair::materialize_live_repair(
            &repair_dir,
            &output_dir,
            archive_v2::repair::RepairMaterializeOptions {
                max_rpc_json_bytes: u64::from(max_rpc_json_mib) << 20,
                checkpoint_every,
                pubkey_run_max_keys: pubkey_run_max_keys as usize,
                max_blocks,
            },
        ),
        Commands::BuildArchiveV2DegradedHotBlocksFromRepair {
            materialized_dir,
            output_dir,
            level,
            max_blocks,
        } => archive_v2::build_degraded_hot_blocks_from_repair(
            &materialized_dir,
            &output_dir,
            level,
            max_blocks,
        ),
        Commands::BuildArchiveV2RepairBlockAccess {
            repair_dir,
            hot_output,
        } => archive_v2::repair::build_repair_block_access(&repair_dir, &hot_output),
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
        Commands::FindPohGaps { input, output } => {
            archive_v2::find_poh_gaps(&input, output.as_deref())
        }
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
    input_bytes_done: Option<u64>,
    input_bytes_at_last_report: u64,
    input_mib_per_sec: Option<f64>,
    phase: &'static str,
    progress_path: Option<PathBuf>,
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
            input_bytes_done: None,
            input_bytes_at_last_report: 0,
            input_mib_per_sec: None,
            phase,
            progress_path: std::env::var_os("BLOCKZILLA_PROGRESS_FILE").map(PathBuf::from),
        }
    }

    #[inline(always)]
    pub(crate) fn update_slot(&mut self, slot: u64) {
        if self.first_slot.is_none() {
            self.first_slot = Some(slot);
        }
        self.last_slot = Some(slot);
    }

    /// Records cumulative useful input bytes consumed by this phase. This is
    /// logical payload progress, not physical device I/O; the scheduler uses
    /// it to compare aggregate useful throughput across lane counts.
    #[inline(always)]
    pub(crate) fn update_input_bytes(&mut self, cumulative_bytes: u64) {
        self.input_bytes_done = Some(
            self.input_bytes_done
                .map_or(cumulative_bytes, |current| current.max(cumulative_bytes)),
        );
    }

    #[inline(always)]
    pub(crate) fn update(&mut self, blocks_delta: u64, txs_delta: u64) {
        self.blocks += blocks_delta;
        self.txs += txs_delta;
        self.blocks_since_report += blocks_delta;
        self.txs_since_report += txs_delta;

        let now = Instant::now();
        if now.duration_since(self.last_report) >= self.report_interval {
            let interval_secs = now
                .duration_since(self.last_report)
                .as_secs_f64()
                .max(0.001);
            self.input_mib_per_sec = self.input_bytes_done.and_then(|bytes| {
                let delta = bytes.checked_sub(self.input_bytes_at_last_report)?;
                let rate = delta as f64 / (1024.0 * 1024.0) / interval_secs;
                rate.is_finite().then_some(rate)
            });
            self.input_bytes_at_last_report = self.input_bytes_done.unwrap_or_default();
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

        self.write_progress_snapshot("running");

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

        self.write_progress_snapshot("complete");

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

    fn write_progress_snapshot(&self, state: &str) {
        let Some(path) = self.progress_path.as_deref() else {
            return;
        };
        let elapsed_secs = self.start_time.elapsed().as_secs_f64().max(0.001);
        let blocks_per_sec = self.blocks as f64 / elapsed_secs;
        let txs_per_sec = self.txs as f64 / elapsed_secs;
        let slots_processed = self
            .first_slot
            .zip(self.last_slot)
            .map(|(first, last)| last.saturating_sub(first));
        let progress_pct = slots_processed.map(|slots| {
            (slots as f64 / self.estimated_total_blocks.max(1) as f64 * 100.0).min(100.0)
        });
        let eta_secs = slots_processed.and_then(|slots| {
            if slots == 0 || blocks_per_sec <= 0.0 {
                return None;
            }
            let slots_remaining = self.estimated_total_blocks.saturating_sub(slots);
            let blocks_per_slot = self.blocks as f64 / slots as f64;
            Some(slots_remaining as f64 * blocks_per_slot / blocks_per_sec)
        });
        let updated_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs());
        let mut json = String::with_capacity(512);
        let _ = write!(
            json,
            "{{\"schema_version\":1,\"pid\":{},\"phase\":\"{}\",\"state\":\"{}\",\"blocks_done\":{},\"transactions_done\":{},\"blocks_total_estimate\":{},\"first_slot\":{},\"last_slot\":{},\"slots_processed\":{},\"elapsed_secs\":{:.3},\"blocks_per_sec\":{:.3},\"transactions_per_sec\":{:.3},\"input_bytes_done\":{},\"input_mib_per_sec\":{},\"progress_pct\":{},\"eta_secs\":{},\"updated_unix_secs\":{}}}\n",
            std::process::id(),
            self.phase,
            state,
            self.blocks,
            self.txs,
            self.estimated_total_blocks,
            json_u64(self.first_slot),
            json_u64(self.last_slot),
            json_u64(slots_processed),
            elapsed_secs,
            blocks_per_sec,
            txs_per_sec,
            json_u64(self.input_bytes_done),
            json_f64(self.input_mib_per_sec),
            json_f64(progress_pct),
            json_f64(eta_secs),
            updated_unix_secs,
        );
        if let Some(parent) = path.parent()
            && std::fs::create_dir_all(parent).is_err()
        {
            return;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("progress.json");
        let temp_path = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
        if std::fs::write(&temp_path, json).is_ok() {
            let _ = std::fs::rename(temp_path, path);
        }
    }
}

fn json_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_string(), |value| value.to_string())
}

fn json_f64(value: Option<f64>) -> String {
    value
        .filter(|value| value.is_finite())
        .map_or_else(|| "null".to_string(), |value| format!("{value:.3}"))
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn compact_v1_commands_are_not_exposed() {
        for command in ["analyze-compact", "dump-compact-log-strings"] {
            let error = Cli::try_parse_from(["blockzilla", command])
                .err()
                .expect("Compact V1 commands must remain outside the public CLI");
            assert!(error.to_string().contains("unrecognized subcommand"));
        }
    }

    #[test]
    fn preflight_car_cli_has_bounded_default_and_receipt() {
        let cli = Cli::try_parse_from([
            "blockzilla",
            "preflight-car",
            "/cars/.downloads/epoch-900.car.zst.part",
            "--epoch",
            "900",
            "--receipt",
            "/state/epoch-900.json",
        ])
        .unwrap();
        let Commands::PreflightCar {
            input,
            epoch,
            receipt,
            io_buffer_mib,
            progress_json,
        } = cli.command
        else {
            panic!("expected preflight-car command");
        };
        assert_eq!(
            input,
            PathBuf::from("/cars/.downloads/epoch-900.car.zst.part")
        );
        assert_eq!(epoch, 900);
        assert_eq!(receipt, PathBuf::from("/state/epoch-900.json"));
        assert_eq!(io_buffer_mib, 8);
        assert!(progress_json.is_none());
    }

    #[test]
    fn block_time_gap_cli_parses_local_source_and_output() {
        let cli = Cli::try_parse_from([
            "blockzilla",
            "build-block-time-gaps",
            "/archive-v2/epoch-314",
            "--epoch",
            "314",
            "--output",
            "/state/epoch-314/block-time-gaps.bin",
        ])
        .unwrap();
        let Commands::BuildBlockTimeGaps {
            input,
            epoch,
            source,
            output,
            progress_json,
            force,
        } = cli.command
        else {
            panic!("expected build-block-time-gaps command");
        };
        assert_eq!(input, PathBuf::from("/archive-v2/epoch-314"));
        assert_eq!(epoch, 314);
        assert_eq!(source, BlockTimeGapSourceArg::Auto);
        assert_eq!(
            output,
            Some(PathBuf::from("/state/epoch-314/block-time-gaps.bin"))
        );
        assert!(progress_json.is_none());
        assert!(!force);
    }

    #[test]
    fn block_time_gap_cli_can_force_archive_source_with_progress() {
        let cli = Cli::try_parse_from([
            "blockzilla",
            "build-block-time-gaps",
            "/archive-v2/epoch-314",
            "--epoch",
            "314",
            "--source",
            "archive",
            "--progress-json",
            "/state/epoch-314/gap-progress.json",
        ])
        .unwrap();
        let Commands::BuildBlockTimeGaps {
            source,
            progress_json,
            ..
        } = cli.command
        else {
            panic!("expected build-block-time-gaps command");
        };
        assert_eq!(source, BlockTimeGapSourceArg::Archive);
        assert_eq!(
            progress_json,
            Some(PathBuf::from("/state/epoch-314/gap-progress.json"))
        );
    }

    #[test]
    fn verify_block_time_gap_cli_parses_expected_epoch() {
        let cli = Cli::try_parse_from([
            "blockzilla",
            "verify-block-time-gaps",
            "/archive-v2/epoch-314/block-time-gaps.bin",
            "--epoch",
            "314",
        ])
        .unwrap();
        let Commands::VerifyBlockTimeGaps { input, epoch } = cli.command else {
            panic!("expected verify-block-time-gaps command");
        };
        assert_eq!(
            input,
            PathBuf::from("/archive-v2/epoch-314/block-time-gaps.bin")
        );
        assert_eq!(epoch, Some(314));
    }

    #[test]
    fn progress_tracker_publishes_atomic_json_snapshot() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "blockzilla-progress-{}-{unique}.json",
            std::process::id()
        ));
        let mut tracker = ProgressTracker::new("test phase");
        tracker.progress_path = Some(path.clone());
        tracker.update_input_bytes(8 * 1024 * 1024);
        tracker.update_slot(432_000);
        tracker.update(1, 7);
        tracker.update_slot(432_100);
        tracker.final_report();

        let json = std::fs::read_to_string(&path).unwrap();
        assert!(json.contains("\"state\":\"complete\""));
        assert!(json.contains("\"phase\":\"test phase\""));
        assert!(json.contains("\"blocks_done\":1"));
        assert!(json.contains("\"input_bytes_done\":8388608"));
        assert!(json.contains("\"last_slot\":432100"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn first_seen_scan_only_requires_first_seen_mode() {
        let error = Cli::try_parse_from([
            "blockzilla",
            "build-archive-v2-hot-blocks",
            "epoch.car.zst",
            "epoch-out",
            "--first-seen-scan-only",
        ])
        .err()
        .expect("scan-only without first-seen mode must fail");
        assert!(error.to_string().contains("--first-seen-registry"));
    }

    #[test]
    fn first_seen_scan_only_and_finalizer_commands_parse() {
        let cli = Cli::try_parse_from([
            "blockzilla",
            "build-archive-v2-hot-blocks",
            "epoch.car.zst",
            "epoch-out",
            "--first-seen-registry",
            "--first-seen-scan-only",
            "--first-seen-finalizer-lock",
            "/tmp/test-finalizer.lock",
        ])
        .unwrap();
        match cli.command {
            Commands::BuildArchiveV2HotBlocks {
                first_seen_registry,
                first_seen_scan_only,
                first_seen_finalizer_lock,
                ..
            } => {
                assert!(first_seen_registry);
                assert!(first_seen_scan_only);
                assert_eq!(
                    first_seen_finalizer_lock.as_deref(),
                    Some(Path::new("/tmp/test-finalizer.lock"))
                );
            }
            _ => panic!("unexpected command"),
        }

        let cli = Cli::try_parse_from([
            "blockzilla",
            "finalize-archive-v2-first-seen",
            "epoch-out",
            "--finalizer-lock",
            "/tmp/test-finalizer.lock",
        ])
        .unwrap();
        match cli.command {
            Commands::FinalizeArchiveV2FirstSeen {
                output_dir,
                finalizer_lock,
            } => {
                assert_eq!(output_dir, Path::new("epoch-out"));
                assert_eq!(
                    finalizer_lock.as_deref(),
                    Some(Path::new("/tmp/test-finalizer.lock"))
                );
            }
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn prepare_live_registry_command_parses() {
        let cli = Cli::try_parse_from([
            "blockzilla",
            "prepare-archive-v2-live-registry",
            "capture",
            "epoch-out",
        ])
        .unwrap();
        match cli.command {
            Commands::PrepareArchiveV2LiveRegistry {
                capture_dir,
                output_dir,
            } => {
                assert_eq!(capture_dir, Path::new("capture"));
                assert_eq!(output_dir, Path::new("epoch-out"));
            }
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn repair_materialize_and_degraded_hot_commands_parse() {
        let cli = Cli::try_parse_from([
            "blockzilla",
            "materialize-archive-v2-live-repair",
            "repair-view",
            "materialized",
            "--max-rpc-json-mib",
            "24",
            "--checkpoint-every",
            "64",
            "--pubkey-run-max-keys",
            "100000",
            "--max-blocks",
            "1",
        ])
        .unwrap();
        match cli.command {
            Commands::MaterializeArchiveV2LiveRepair {
                repair_dir,
                output_dir,
                max_rpc_json_mib,
                checkpoint_every,
                pubkey_run_max_keys,
                max_blocks,
            } => {
                assert_eq!(repair_dir, Path::new("repair-view"));
                assert_eq!(output_dir, Path::new("materialized"));
                assert_eq!(max_rpc_json_mib, 24);
                assert_eq!(checkpoint_every, 64);
                assert_eq!(pubkey_run_max_keys, 100_000);
                assert_eq!(max_blocks, Some(1));
            }
            _ => panic!("unexpected command"),
        }

        let cli = Cli::try_parse_from([
            "blockzilla",
            "build-archive-v2-degraded-hot-blocks-from-repair",
            "materialized",
            "hot",
            "--level",
            "2",
        ])
        .unwrap();
        match cli.command {
            Commands::BuildArchiveV2DegradedHotBlocksFromRepair {
                materialized_dir,
                output_dir,
                level,
                max_blocks,
            } => {
                assert_eq!(materialized_dir, Path::new("materialized"));
                assert_eq!(output_dir, Path::new("hot"));
                assert_eq!(level, 2);
                assert!(max_blocks.is_none());
            }
            _ => panic!("unexpected command"),
        }

        let cli = Cli::try_parse_from([
            "blockzilla",
            "build-archive-v2-repair-block-access",
            "repair-view",
            "hot",
        ])
        .unwrap();
        match cli.command {
            Commands::BuildArchiveV2RepairBlockAccess {
                repair_dir,
                hot_output,
            } => {
                assert_eq!(repair_dir, Path::new("repair-view"));
                assert_eq!(hot_output, Path::new("hot"));
            }
            _ => panic!("unexpected command"),
        }
    }
}
