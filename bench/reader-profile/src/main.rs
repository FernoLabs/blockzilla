//! Diagnostic harness, separate from the small public examples. Calls the same
//! format SDKs and workload sinks; discards output bytes, not workload work.
mod allocation;

use anyhow::{Context, Result, ensure};
use blockzilla_archive_v3_reader::{IndexerV3Archive, IndexerV3CacheProfile, IndexerV3OpenOptions};
use blockzilla_compact_v2_reader::archive::{
    CompactV2Archive, CompactV2LocalDescriptor, CompactV2ParallelScanConfig,
};
use blockzilla_example_workloads::{
    IndexedUsdcBalanceSink, MAINNET_PUMP_FUN_PROGRAM, MAINNET_USDC_MINT, PumpSink,
    TransactionIdentityDumpSink, UsdcBalanceSink, UserProgramIndexSink, pump_scan_request,
    usdc_scan_request, user_program_index_scan_request,
};
use blockzilla_model::{
    ArchiveInstructionSource, ArchiveIoSnapshot, BlockSink, BlockView, ScanRange, ScanReceipt,
    ScanRequest, SourceIdentity,
};
use clap::{Parser, ValueEnum};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Write},
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    time::Instant,
};

#[global_allocator]
static ALLOCATOR: allocation::Allocator = allocation::Allocator;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Format {
    V2,
    V3,
}
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Workload {
    Count,
    Transactions,
    Usdc,
    Pumpfun,
    UserProgramIndex,
}
#[derive(Parser)]
struct Args {
    /// Directory containing compact-v2/EPOCH and indexer-v3/EPOCH.
    #[arg(long, required_unless_present = "origin", conflicts_with = "origin")]
    archive_root: Option<PathBuf>,
    /// HTTPS sample gateway. Uses the same format SDK as the public examples.
    #[arg(long, requires = "cache_root")]
    origin: Option<String>,
    #[arg(long, requires = "origin")]
    cache_root: Option<PathBuf>,
    #[arg(long)]
    epoch: u64,
    #[arg(long, value_enum)]
    format: Format,
    #[arg(long, value_enum)]
    workload: Workload,
    #[arg(long, default_value_t = 0)]
    first_block: u32,
    /// Diagnostic range only; public examples still read the whole epoch.
    #[arg(long, default_value = "2048")]
    blocks: NonZeroU32,
    #[arg(long, default_value = "1")]
    workers: NonZeroUsize,
    #[arg(long, default_value_t = 3)]
    iterations: usize,
    #[arg(long, default_value_t = 1)]
    warmups: usize,
    #[arg(long, conflicts_with = "flamegraph")]
    allocations: bool,
    /// Measure the V2 compact-ID USDC projection and its discovery dictionary.
    #[arg(long)]
    indexed_usdc: bool,
    #[arg(long)]
    flamegraph: Option<PathBuf>,
    /// Include archive admission and sidecar downloads in the CPU profile.
    #[arg(long, requires = "flamegraph")]
    profile_setup: bool,
    /// Use dense V3 scanning to isolate projection from reverse lookup.
    #[arg(long)]
    dense: bool,
    /// Explicit diagnostic override; the SDK's normal limit remains unchanged.
    #[arg(long, default_value_t = 1024)]
    registry_mib: u64,
    /// Use the previous V2 input schedule for controlled comparisons.
    #[arg(long)]
    v2_legacy_input: bool,
    #[arg(long)]
    v3_legacy_input: bool,
    /// Download the sealed V3 signature sidecar once, then read it locally.
    #[arg(long, requires = "origin")]
    v3_cache_signatures: bool,
    #[arg(long, default_value = "5LikTUsx695BHRipWoRrn6YmTQEcPrvbR8YaHxdSRQo8")]
    wallet: String,
}

enum Archive {
    V2(CompactV2Archive),
    V3(IndexerV3Archive),
}
enum Sink {
    Transactions(TransactionIdentityDumpSink<io::Sink>),
    Count { blocks: u64, tx: u64, inner: u64 },
    Usdc(UsdcBalanceSink<io::Sink>),
    Pump(PumpSink<io::Sink>),
    UserProgramIndex(UserProgramIndexSink<io::Sink>),
}
impl BlockSink for Sink {
    fn visit_block(&mut self, block: BlockView<'_>) -> blockzilla_model::Result<()> {
        match self {
            Self::Count { blocks, tx, inner } => {
                let counts = block.counts.ok_or_else(|| {
                    blockzilla_model::Error::InvalidStream("native count view missing".into())
                })?;
                *blocks += 1;
                *tx += counts.transactions;
                *inner += counts.recorded_inner_instructions;
                Ok(())
            }
            Self::Transactions(s) => s.visit_block(block),
            Self::Usdc(s) => s.visit_block(block),
            Self::Pump(s) => s.visit_block(block),
            Self::UserProgramIndex(s) => s.visit_block(block),
        }
    }
}
impl Sink {
    fn new(workload: Workload, wallet: [u8; 32], identity: &SourceIdentity) -> Result<Self> {
        Ok(match workload {
            Workload::Count => Self::Count {
                blocks: 0,
                tx: 0,
                inner: 0,
            },
            Workload::Transactions => Self::Transactions(TransactionIdentityDumpSink::new(
                io::sink(),
                identity.epoch,
                identity.first_slot,
                identity
                    .first_slot
                    .checked_add(identity.slots_per_epoch)
                    .context("slot range overflow")?,
            )?),
            Workload::Usdc => Self::Usdc(UsdcBalanceSink::mainnet(io::sink())?),
            Workload::Pumpfun => Self::Pump(PumpSink::mainnet(io::sink())?),
            Workload::UserProgramIndex => {
                Self::UserProgramIndex(UserProgramIndexSink::new(io::sink(), wallet)?)
            }
        })
    }
    fn finish(self) -> Result<String> {
        // Transaction identities hash every output byte. Other workloads compare
        // counters and coverage; their full examples provide output-file parity.
        Ok(match self {
            Self::Count { blocks, tx, inner } => format!("blocks={blocks} tx={tx} inner={inner}"),
            Self::Transactions(s) => {
                let r = s.finish()?.report;
                format!(
                    "records={} bytes={} sha256={} first_slot={:?} last_slot={:?}",
                    r.records,
                    r.output_bytes,
                    r.output_sha256_hex(),
                    r.first_slot,
                    r.last_slot
                )
            }
            Self::Usdc(s) => format!("{:?}", s.finish()?.report),
            Self::Pump(s) => format!("{:?}", s.finish()?.report),
            Self::UserProgramIndex(s) => format!("{:?}", s.finish()?.report),
        })
    }
}
fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.iterations > 0, "iterations must be positive");
    ensure!(args.workers.get() <= 64, "at most 64 workers");
    ensure!(
        !args.indexed_usdc || matches!((args.format, args.workload), (Format::V2, Workload::Usdc)),
        "--indexed-usdc requires --format v2 --workload usdc"
    );
    let mut wallet = [0; 32];
    ensure!(
        bs58::decode(&args.wallet).onto(&mut wallet)? == 32,
        "wallet must decode to 32 bytes"
    );
    let registry_bytes = args
        .registry_mib
        .checked_mul(1 << 20)
        .context("registry limit overflow")?;
    ensure!(
        !args.profile_setup || args.warmups == 0,
        "--profile-setup requires --warmups 0"
    );
    let start_profiler = || {
        pprof::ProfilerGuardBuilder::default()
            .frequency(199)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
    };
    let mut profiler = if args.profile_setup {
        Some(start_profiler()?)
    } else {
        None
    };
    let total_started = Instant::now();
    eprintln!("phase=setup unix_ms={}", unix_ms());
    let mut archive = match args.format {
        Format::V2 => Archive::V2(if let Some(origin) = &args.origin {
            CompactV2Archive::open(origin, args.epoch, args.cache_root.as_ref().unwrap())?
        } else {
            CompactV2Archive::open_local(
                args.archive_root
                    .as_ref()
                    .unwrap()
                    .join("compact-v2")
                    .join(args.epoch.to_string()),
                CompactV2LocalDescriptor::mainnet(args.epoch, "reader-profile")?,
            )?
        }),
        Format::V3 => {
            let mut archive = if let Some(origin) = &args.origin {
                let options = IndexerV3OpenOptions {
                    cache_profile: if args.v3_cache_signatures {
                        IndexerV3CacheProfile::SignatureLocal
                    } else {
                        IndexerV3CacheProfile::Streaming
                    },
                    ..IndexerV3OpenOptions::default()
                };
                IndexerV3Archive::open_with_options(
                    origin,
                    args.epoch,
                    args.cache_root.as_ref().unwrap(),
                    options,
                )?
            } else {
                IndexerV3Archive::open_local(args.archive_root.as_ref().unwrap(), args.epoch)?
            };
            archive.set_full_registry_limit(registry_bytes);
            if args.v3_legacy_input {
                archive.set_network_input_config(None)?;
            }
            Archive::V3(archive)
        }
    };
    let setup_seconds = total_started.elapsed().as_secs_f64();
    let (identity, setup_transport) = match &archive {
        Archive::V2(a) => (a.identity().clone(), a.transport_snapshot().http_and_cache),
        Archive::V3(a) => (a.identity().clone(), a.transport_snapshot().http_and_cache),
    };
    println!(
        "{}",
        json!({"phase":"setup", "seconds":setup_seconds,
        "transport":setup_transport, "identity":identity})
    );
    let request = ScanRequest::all();
    let mut request = match args.workload {
        Workload::Count => request
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .count_instructions_only(),
        Workload::Transactions => request
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instructions()
            .without_instruction_accounts()
            .without_instruction_data()
            .without_required_signers()
            .without_execution_status(),
        Workload::Usdc => usdc_scan_request(request, MAINNET_USDC_MINT),
        Workload::Pumpfun => pump_scan_request(request),
        Workload::UserProgramIndex => {
            user_program_index_scan_request(request).with_required_signer(wallet)
        }
    };
    request.range = Some(ScanRange {
        first_block: args.first_block,
        block_count: args.blocks,
    });
    let mut oracle = None;
    for iteration in 0..args.warmups + args.iterations {
        let measured = iteration >= args.warmups;
        if iteration == args.warmups && args.flamegraph.is_some() && profiler.is_none() {
            profiler = Some(start_profiler()?);
        }
        let mut sink = Sink::new(args.workload, wallet, &identity)?;
        // No persistent files are produced here. The full example records the
        // actual source scope; this diagnostic dictionary is discarded per scan.
        let mut indexed_sink = args
            .indexed_usdc
            .then(|| IndexedUsdcBalanceSink::mainnet(io::sink(), io::sink(), [0; 32]))
            .transpose()?;
        if measured && args.allocations {
            allocation::start();
        }
        let transport_before = archive.http_snapshot();
        eprintln!("phase=scan iteration={iteration} unix_ms={}", unix_ms());
        let started = Instant::now();
        let (scan, stages): (ScanReceipt, _) = match &mut archive {
            Archive::V2(a) => {
                let mut config = CompactV2ParallelScanConfig::new(args.workers.get())
                    .with_full_registry_limit(registry_bytes);
                if args.v2_legacy_input {
                    config.network_input = None;
                }
                let r = if let Some(indexed) = &mut indexed_sink {
                    a.scan_token_balances_indexed_parallel(&request, indexed, config)?
                } else {
                    a.scan_ordered_parallel(&request, &mut sink, config)?
                };
                let p = r.pipeline;
                (
                    r.scan,
                    json!({
                        "input_workers":p.input_workers,
                        "input_buffer_count":p.input_buffer_count,
                        "input_buffer_capacity_bytes":p.input_buffer_capacity_bytes,
                        "read_s":p.producer_read_wall_time.as_secs_f64(),
                        "input_wait_s":p.coordinator_wait_for_ready_batch_time.as_secs_f64(),
                        "decode_sum_s":p.worker_decompress_decode_sum_time.as_secs_f64(),
                        "projection_sum_s":p.worker_projection_sum_time.as_secs_f64(),
                        "decode_project_wall_s":p.coordinator_decode_project_wall_time.as_secs_f64(),
                        "consume_s":p.coordinator_consume_wall_time.as_secs_f64(),
                        "projection_buffer_wait_s":p.coordinator_wait_for_projection_buffer_time.as_secs_f64(),
                        "result_send_wait_s":p.coordinator_wait_to_send_result_time.as_secs_f64(),
                        "producer_buffer_wait_s":p.producer_wait_for_free_buffer_time.as_secs_f64(),
                        "signature_read_s":r.signature_read_wall_time.as_secs_f64(),
                        "signature_assign_s":r.signature_assign_wall_time.as_secs_f64(),
                        "publish_s":r.publish_wall_time.as_secs_f64(),
                        "requested_workers":r.requested_workers,
                        "effective_workers":r.effective_workers,
                        "max_active_workers":r.max_active_workers,
                        "block_count":p.block_count,
                        "batch_count":p.batch_count,
                        "max_blocks_per_batch":p.max_blocks_per_batch,
                        "max_transactions_per_batch":p.max_transactions_per_batch,
                        "max_in_flight_blocks":p.max_in_flight_blocks,
                        "max_in_flight_transactions":p.max_in_flight_transactions,
                        "max_in_flight_declared_uncompressed_bytes":p.max_in_flight_declared_uncompressed_bytes,
                        "read_call_count":p.read_call_count,
                        "compressed_bytes":p.compressed_bytes,
                        "compressed_buffer_count":r.compressed_buffer_count,
                        "max_compressed_batch_bytes":p.max_compressed_batch_bytes,
                        "max_declared_uncompressed_batch_bytes":p.max_declared_uncompressed_batch_bytes,
                        "max_retained_decompressed_buffer_bytes":p.max_retained_decompressed_buffer_bytes,
                        "max_projected_block_bytes":r.max_projected_block_bytes,
                        "max_projected_batch_bytes":r.max_projected_batch_bytes,
                        "registry_mode":format!("{:?}",r.registry.mode),
                        "registry_prefetch_read_calls":r.registry.prefetch_read_calls,
                        "registry_prefetch_read_bytes":r.registry.prefetch_read_bytes,
                        "registry_resident_bound_bytes":r.registry.resident_bound_bytes,
                    }),
                )
            }
            Archive::V3(a)
                if !args.dense
                    && matches!(
                        args.workload,
                        Workload::Pumpfun | Workload::UserProgramIndex
                    ) =>
            {
                let r = if matches!(args.workload, Workload::Pumpfun) {
                    a.for_each_reached_program_candidate_block_parallel(
                        &MAINNET_PUMP_FUN_PROGRAM,
                        &request,
                        args.workers,
                        |block| sink.visit_block(block),
                    )?
                } else {
                    a.for_each_signer_wallet_candidate_block_parallel(
                        &wallet,
                        &request,
                        args.workers,
                        |block| sink.visit_block(block),
                    )?
                };
                (r.scan.scan_receipt, json!({"path":"reverse-candidates"}))
            }
            Archive::V3(a) => {
                let r = a.scan_ordered_parallel(&request, args.workers, &mut sink)?;
                (
                    r.scan,
                    json!({"path":"dense", "parallel":r.parallel, "registry":r.registry}),
                )
            }
        };
        let seconds = started.elapsed().as_secs_f64();
        let transport = archive.http_snapshot().saturating_sub(transport_before);
        eprintln!("phase=finalize iteration={iteration} unix_ms={}", unix_ms());
        let allocations = if measured && args.allocations {
            Some(allocation::stop())
        } else {
            None
        };
        let (result, indexed_dictionary_rows, indexed_dictionary_bytes) =
            if let Some(indexed) = indexed_sink {
                let (data, dictionary) = indexed.finish()?;
                (
                    format!("{:?}; dictionary={:?}", data.report, dictionary.report),
                    Some(dictionary.report.row_count),
                    Some(dictionary.report.output_bytes),
                )
            } else {
                (sink.finish()?, None, None)
            };
        if let Some(expected) = &oracle {
            ensure!(
                *expected == result,
                "workload output changed between iterations"
            );
        } else {
            oracle = Some(result.clone());
        }
        if measured {
            // Serialize histogram rows after stop(), so reporting the histogram
            // cannot itself add allocation requests to its buckets. Buckets are
            // disjoint; the final null upper bound means greater than 65,536.
            let allocation_size_buckets = allocations.map(|counts| {
                counts
                    .size_buckets
                    .iter()
                    .zip(allocation::BUCKET_UPPER_BOUNDS)
                    .map(|(bucket, upper)| {
                        json!({
                            "upper_bound_bytes_inclusive": upper,
                            "allocation_calls": bucket.allocation_calls,
                            "allocation_bytes": bucket.allocation_bytes,
                        })
                    })
                    .collect::<Vec<_>>()
            });
            println!(
                "{}",
                json!({"iteration":iteration-args.warmups,"format":format!("{:?}",args.format),"workload":format!("{:?}",args.workload),
                "phase":"scan", "transport":transport, "blocks":scan.blocks, "oracle":&result,
                "workers":args.workers.get(),"seconds":seconds,"transactions":scan.transactions,"instructions":scan.instructions,
                "tps":scan.transactions as f64/seconds,"source_bytes":scan.io.source_read_bytes,"source_calls":scan.io.source_read_calls,
                "allocation_calls":allocations.map(|x|x.allocation_calls),"allocation_bytes":allocations.map(|x|x.allocation_bytes),
                "allocation_size_buckets":allocation_size_buckets,"stages":stages,
                "indexed_usdc":args.indexed_usdc,"indexed_dictionary_rows":indexed_dictionary_rows,
                "indexed_dictionary_bytes":indexed_dictionary_bytes})
            );
        }
    }
    match &archive {
        Archive::V2(a) => a.verify_local_unchanged()?,
        Archive::V3(a) => a.verify_local_unchanged()?,
    }
    println!(
        "{}",
        json!({"phase":"complete", "seconds":total_started.elapsed().as_secs_f64(),
        "transport":archive.http_snapshot(), "oracle":oracle})
    );
    eprintln!("phase=profile_report unix_ms={}", unix_ms());
    eprintln!("workload_oracle={}", oracle.unwrap());
    if let (Some(profiler), Some(path)) = (profiler, &args.flamegraph) {
        let report = profiler.report().build()?;
        ensure!(!report.data.is_empty(), "CPU profile contains no samples");
        ensure!(
            report
                .data
                .keys()
                .any(|frames| frames.frames.iter().any(|symbols| !symbols.is_empty())),
            "CPU samples have no symbols; rebuild with frame-profiler and frame pointers in all dependencies"
        );
        report.flamegraph(File::create(path)?)?;
        let mut top = BTreeMap::<String, isize>::new();
        for (frames, count) in &report.data {
            let name = frames
                .frames
                .first()
                .and_then(|s| s.first())
                .map(|s| s.name())
                .unwrap_or_default();
            *top.entry(name).or_default() += count;
        }
        let mut top = top.into_iter().collect::<Vec<_>>();
        top.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
        let mut out = File::create(path.with_extension("top.tsv"))?;
        writeln!(out, "samples\tfunction")?;
        for (name, count) in top {
            writeln!(out, "{count}\t{}", name.replace(['\n', '\t'], " "))?;
        }
    }
    Ok(())
}

impl Archive {
    fn http_snapshot(&self) -> ArchiveIoSnapshot {
        match self {
            Self::V2(a) => a.transport_snapshot().http_and_cache,
            Self::V3(a) => a.transport_snapshot().http_and_cache,
        }
    }
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis()
}
