//! Diagnostic harness, separate from the small public examples. Calls the same
//! format SDKs and workload sinks; discards output bytes, not workload work.
mod allocation;

use anyhow::{Context, Result, ensure};
use blockzilla_archive_v3_reader::IndexerV3Archive;
use blockzilla_compact_v2_reader::archive::{
    CompactV2Archive, CompactV2LocalDescriptor, CompactV2ParallelScanConfig,
};
use blockzilla_example_workloads::{
    FirewatchSink, MAINNET_PUMP_FUN_PROGRAM, MAINNET_USDC_MINT, PumpSink, UsdcBalanceSink,
    firewatch_scan_request, pump_scan_request, usdc_scan_request,
};
use blockzilla_model::{BlockSink, BlockView, ScanRange, ScanReceipt, ScanRequest};
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
    Usdc,
    Pumpfun,
    Firewatch,
}
#[derive(Parser)]
struct Args {
    /// Directory containing compact-v2/EPOCH and indexer-v3/EPOCH.
    #[arg(long)]
    archive_root: PathBuf,
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
    #[arg(long)]
    flamegraph: Option<PathBuf>,
    /// Use dense V3 scanning to isolate projection from reverse lookup.
    #[arg(long)]
    dense: bool,
    /// Explicit diagnostic override; the SDK's normal limit remains unchanged.
    #[arg(long, default_value_t = 1024)]
    registry_mib: u64,
    #[arg(long, default_value = "5LikTUsx695BHRipWoRrn6YmTQEcPrvbR8YaHxdSRQo8")]
    wallet: String,
}

enum Archive {
    V2(CompactV2Archive),
    V3(IndexerV3Archive),
}
enum Sink {
    Count { blocks: u64, tx: u64, inner: u64 },
    Usdc(UsdcBalanceSink<io::Sink>),
    Pump(PumpSink<io::Sink>),
    Firewatch(FirewatchSink<io::Sink>),
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
            Self::Usdc(s) => s.visit_block(block),
            Self::Pump(s) => s.visit_block(block),
            Self::Firewatch(s) => s.visit_block(block),
        }
    }
}
impl Sink {
    fn new(workload: Workload, wallet: [u8; 32]) -> Result<Self> {
        Ok(match workload {
            Workload::Count => Self::Count {
                blocks: 0,
                tx: 0,
                inner: 0,
            },
            Workload::Usdc => Self::Usdc(UsdcBalanceSink::mainnet(io::sink())?),
            Workload::Pumpfun => Self::Pump(PumpSink::mainnet(io::sink())?),
            Workload::Firewatch => Self::Firewatch(FirewatchSink::new(io::sink(), wallet)?),
        })
    }
    fn finish(self) -> Result<String> {
        // Check counters and coverage between iterations. This is not a check
        // of every output byte; use the full examples for output-file parity.
        Ok(match self {
            Self::Count { blocks, tx, inner } => format!("blocks={blocks} tx={tx} inner={inner}"),
            Self::Usdc(s) => format!("{:?}", s.finish()?.report),
            Self::Pump(s) => format!("{:?}", s.finish()?.report),
            Self::Firewatch(s) => format!("{:?}", s.finish()?.report),
        })
    }
}
fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.iterations > 0, "iterations must be positive");
    ensure!(args.workers.get() <= 64, "at most 64 workers");
    let mut wallet = [0; 32];
    ensure!(
        bs58::decode(&args.wallet).onto(&mut wallet)? == 32,
        "wallet must decode to 32 bytes"
    );
    let registry_bytes = args
        .registry_mib
        .checked_mul(1 << 20)
        .context("registry limit overflow")?;
    let mut archive = match args.format {
        Format::V2 => Archive::V2(CompactV2Archive::open_local(
            args.archive_root
                .join("compact-v2")
                .join(args.epoch.to_string()),
            CompactV2LocalDescriptor::mainnet(args.epoch, "reader-profile")?,
        )?),
        Format::V3 => {
            let mut archive = IndexerV3Archive::open_local(&args.archive_root, args.epoch)?;
            archive.set_full_registry_limit(registry_bytes);
            Archive::V3(archive)
        }
    };
    let request = ScanRequest::all();
    let mut request = match args.workload {
        Workload::Count => request
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .count_instructions_only(),
        Workload::Usdc => usdc_scan_request(request, MAINNET_USDC_MINT),
        Workload::Pumpfun => pump_scan_request(request),
        Workload::Firewatch => firewatch_scan_request(request).with_required_signer(wallet),
    };
    request.range = Some(ScanRange {
        first_block: args.first_block,
        block_count: args.blocks,
    });
    let mut oracle = None;
    let mut profiler = None;
    for iteration in 0..args.warmups + args.iterations {
        let measured = iteration >= args.warmups;
        if iteration == args.warmups && args.flamegraph.is_some() {
            profiler = Some(
                pprof::ProfilerGuardBuilder::default()
                    .frequency(199)
                    .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                    .build()?,
            );
        }
        let mut sink = Sink::new(args.workload, wallet)?;
        if measured && args.allocations {
            allocation::start();
        }
        let started = Instant::now();
        let (scan, stages): (ScanReceipt, _) = match &mut archive {
            Archive::V2(a) => {
                let r = a.scan_ordered_parallel(
                    &request,
                    &mut sink,
                    CompactV2ParallelScanConfig::new(args.workers.get())
                        .with_full_registry_limit(registry_bytes),
                )?;
                let p = r.pipeline;
                (
                    r.scan,
                    json!({
                        "read_s":p.producer_read_wall_time.as_secs_f64(),
                        "input_wait_s":p.coordinator_wait_for_ready_batch_time.as_secs_f64(),
                        "decode_sum_s":p.worker_decompress_decode_sum_time.as_secs_f64(),
                        "projection_sum_s":p.worker_projection_sum_time.as_secs_f64(),
                    "consume_s":p.coordinator_consume_wall_time.as_secs_f64(),
                    "projection_buffer_wait_s":p.coordinator_wait_for_projection_buffer_time.as_secs_f64(),
                    "result_send_wait_s":p.coordinator_wait_to_send_result_time.as_secs_f64(),
                    "signature_read_s":r.signature_read_wall_time.as_secs_f64(),
                    "signature_assign_s":r.signature_assign_wall_time.as_secs_f64(),
                    "publish_s":r.publish_wall_time.as_secs_f64(),
                    }),
                )
            }
            Archive::V3(a)
                if !args.dense
                    && matches!(args.workload, Workload::Pumpfun | Workload::Firewatch) =>
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
            Archive::V3(a) => (
                a.scan_ordered_parallel(&request, args.workers, &mut sink)?
                    .scan,
                json!({"path":"dense"}),
            ),
        };
        let seconds = started.elapsed().as_secs_f64();
        let allocations = if measured && args.allocations {
            Some(allocation::stop())
        } else {
            None
        };
        let result = sink.finish()?;
        if let Some(expected) = &oracle {
            ensure!(
                *expected == result,
                "workload output changed between iterations"
            );
        } else {
            oracle = Some(result);
        }
        if measured {
            println!(
                "{}",
                json!({"iteration":iteration-args.warmups,"format":format!("{:?}",args.format),"workload":format!("{:?}",args.workload),
                "workers":args.workers.get(),"seconds":seconds,"transactions":scan.transactions,"instructions":scan.instructions,
                "tps":scan.transactions as f64/seconds,"source_bytes":scan.io.source_read_bytes,"source_calls":scan.io.source_read_calls,
                "allocation_calls":allocations.map(|x|x.0),"allocation_bytes":allocations.map(|x|x.1),"stages":stages})
            );
        }
    }
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
