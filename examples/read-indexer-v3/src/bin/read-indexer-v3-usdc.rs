use blockzilla_example_workloads::ProgressSink;

use std::{error::Error, time::Instant};

use blockzilla_example_workloads::{MAINNET_USDC_MINT, UsdcBalanceSink, usdc_scan_request};
use blockzilla_indexer_v3_read_sdk::{ArchiveInstructionSource, IndexerV3Archive, ScanRequest};
use blockzilla_read_indexer_v3::{
    RunTiming, WorkloadSource, finish_ordered_workload, output_file, workload_arguments,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_arguments("read-indexer-v3-usdc", "indexer-v3-usdc.bin")?;
    let started = Instant::now();
    let mut archive = match &arguments.source {
        WorkloadSource::Network { origin, cache_root } => {
            IndexerV3Archive::open(origin, arguments.epoch, cache_root)?
        }
        WorkloadSource::LocalArchive { archive_root } => {
            IndexerV3Archive::open_local(archive_root, arguments.epoch)?
        }
    };
    let timing = RunTiming::after_open(started, &archive);
    let request = usdc_scan_request(ScanRequest::all(), MAINNET_USDC_MINT);
    let mut sink = UsdcBalanceSink::mainnet(output_file(&arguments.output)?)?;

    let scan = Instant::now();
    let expected_blocks = u64::from(archive.identity().block_count);
    let parallel = archive.scan_ordered_parallel(
        &request,
        arguments.threads,
        &mut ProgressSink::new(&mut sink, expected_blocks),
    )?;
    let scan_seconds = scan.elapsed().as_secs_f64();
    let finished = sink.finish()?;

    finish_ordered_workload(
        &arguments,
        archive,
        timing,
        parallel,
        scan_seconds,
        finished,
    )
}
