use blockzilla_example_workloads::ProgressSink;

use std::{error::Error, time::Instant};

use blockzilla_archive_v3_reader::{ArchiveInstructionSource, ScanRequest};
use blockzilla_example_workloads::{MAINNET_USDC_MINT, UsdcBalanceSink, usdc_scan_request};
use blockzilla_read_archive_v3::{
    RunTiming, finish_ordered_workload, open_workload_archive, output_file, workload_arguments,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_arguments("read-archive-v3-usdc", "indexer-v3-usdc.bin")?;
    let started = Instant::now();
    let mut archive = open_workload_archive(
        &arguments.source,
        arguments.epoch,
        arguments.cache_signatures,
        false,
    )?;
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
