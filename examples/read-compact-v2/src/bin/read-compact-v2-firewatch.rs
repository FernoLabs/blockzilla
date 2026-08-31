use std::{error::Error, time::Instant};

use blockzilla_compact_v2_read_sdk::{
    CompactV2Archive, CompactV2LocalDescriptor, CompactV2ParallelScanConfig, ScanRequest,
};
use blockzilla_example_workloads::{FirewatchSink, firewatch_scan_request};
use blockzilla_read_compact_v2::{
    RunTiming, Source, finish_workload, output_file, parse_pubkey, target_arguments,
};

fn main() -> Result<(), Box<dyn Error>> {
    let args = target_arguments("read-compact-v2-firewatch", "wallet")?;
    let wallet = parse_pubkey(
        "wallet",
        args.target.as_deref().expect("wallet is required"),
    )?;
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
            CompactV2LocalDescriptor::mainnet_current(args.epoch, candidate_id.clone())?,
        )?,
    };
    let timing = RunTiming::after_open(started, &archive);

    // Shared workload code maps this signer wallet to every reached program.
    let request = firewatch_scan_request(ScanRequest::all());
    let mut sink = FirewatchSink::new(output_file(&args.output)?, wallet)?;
    let config = CompactV2ParallelScanConfig::new(args.threads);
    let scan = Instant::now();
    let parallel = archive.scan_ordered_parallel(&request, &mut sink, config)?;
    let elapsed = scan.elapsed().as_secs_f64();
    let finished = sink.finish()?;

    finish_workload(&args, archive, timing, parallel, elapsed, finished)
}
