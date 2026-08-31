use std::{error::Error, time::Instant};

use blockzilla_compact_v2_read_sdk::{
    CompactV2Archive, CompactV2LocalDescriptor, CompactV2ParallelScanConfig, ScanRequest,
};
use blockzilla_example_workloads::{MAINNET_USDC_MINT, UsdcBalanceSink, usdc_scan_request};
use blockzilla_read_compact_v2::{RunTiming, Source, arguments, finish_workload, output_file};

fn main() -> Result<(), Box<dyn Error>> {
    let args = arguments("read-compact-v2-usdc")?;
    let started = Instant::now();
    let mut archive = match &args.source {
        Source::Network { origin, cache_root } => {
            CompactV2Archive::open(origin, args.epoch, cache_root)?
        }
        Source::Local {
            epoch_root,
            candidate_id,
        } => CompactV2Archive::open_local(
            epoch_root,
            CompactV2LocalDescriptor::mainnet(args.epoch, candidate_id.clone())?,
        )?,
    };
    let timing = RunTiming::after_open(started, &archive);

    // Shared workload code selects USDC balances and writes the common output format.
    let request = usdc_scan_request(ScanRequest::all(), MAINNET_USDC_MINT);
    let mut sink = UsdcBalanceSink::mainnet(output_file(&args.output)?)?;
    let config = CompactV2ParallelScanConfig::new(args.threads);
    let scan = Instant::now();
    let parallel = archive.scan_ordered_parallel(&request, &mut sink, config)?;
    let elapsed = scan.elapsed().as_secs_f64();
    let finished = sink.finish()?;

    finish_workload(&args, archive, timing, parallel, elapsed, finished)
}
