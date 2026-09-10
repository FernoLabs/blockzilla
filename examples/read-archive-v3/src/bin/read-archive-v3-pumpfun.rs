use blockzilla_example_workloads::ReadProgress;

use std::{error::Error, time::Instant};

use blockzilla_archive_v3_reader::{QueryError, ScanRequest};
use blockzilla_example_workloads::{MAINNET_PUMP_FUN_PROGRAM, PumpSink, pump_scan_request};
use blockzilla_read_archive_v3::{
    RunTiming, finish_targeted_workload, open_workload_archive, output_file, workload_arguments,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_arguments("read-archive-v3-pumpfun", "indexer-v3-pumpfun.bin")?;
    let started = Instant::now();
    let mut archive = open_workload_archive(
        &arguments.source,
        arguments.epoch,
        arguments.cache_signatures,
        true,
    )?;
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
