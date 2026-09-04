use blockzilla_example_workloads::ReadProgress;

use std::{error::Error, time::Instant};

use blockzilla_example_workloads::{MAINNET_PUMP_FUN_PROGRAM, PumpSink, pump_scan_request};
use blockzilla_indexer_v3_read_sdk::{IndexerV3Archive, QueryError, ScanRequest};
use blockzilla_read_indexer_v3::{
    RunTiming, WorkloadSource, finish_targeted_workload, output_file, workload_arguments,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_arguments("read-indexer-v3-pumpfun", "indexer-v3-pumpfun.bin")?;
    let started = Instant::now();
    let mut archive = match &arguments.source {
        WorkloadSource::Network { origin, cache_root } => {
            IndexerV3Archive::open_selective(origin, arguments.epoch, cache_root)?
        }
        WorkloadSource::LocalArchive { archive_root } => {
            IndexerV3Archive::open_local(archive_root, arguments.epoch)?
        }
    };
    let timing = RunTiming::after_open(started, &archive);
    let request = pump_scan_request(ScanRequest::all());
    let mut sink = PumpSink::mainnet(output_file(&arguments.output)?)?;

    // Reverse lookup returns sound candidates. The sink confirms exact matches.
    let scan = Instant::now();
    let mut progress = ReadProgress::new(None);
    let targeted = archive.for_each_reached_program_candidate_block_parallel(
        &MAINNET_PUMP_FUN_PROGRAM,
        &request,
        arguments.threads,
        |block| {
            sink.process_block(block).map_err(QueryError::sink)?;
            progress.observe(block);
            Ok(())
        },
    )?;
    drop(progress);
    let scan_seconds = scan.elapsed().as_secs_f64();
    let finished = sink.finish()?;

    finish_targeted_workload(
        &arguments,
        archive,
        timing,
        targeted,
        scan_seconds,
        finished,
    )
}
