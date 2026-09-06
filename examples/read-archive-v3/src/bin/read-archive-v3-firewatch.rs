use blockzilla_example_workloads::ReadProgress;

use std::{error::Error, time::Instant};

use blockzilla_archive_v3_reader::{IndexerV3Archive, QueryError, ScanRequest};
use blockzilla_example_workloads::{FirewatchSink, firewatch_scan_request};
use blockzilla_read_archive_v3::{
    DEFAULT_FIREWATCH_WALLET, RunTiming, WorkloadSource, finish_targeted_workload, output_file,
    parse_pubkey, workload_target_arguments,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_target_arguments(
        "read-archive-v3-firewatch",
        "wallet",
        DEFAULT_FIREWATCH_WALLET,
        "indexer-v3-firewatch.bin",
    )?;
    let wallet_text = arguments
        .target
        .as_deref()
        .ok_or("the FireWatch wallet is missing")?;
    let wallet = parse_pubkey("wallet", wallet_text)?;
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
    let request = firewatch_scan_request(ScanRequest::all()).with_required_signer(wallet);
    let mut sink = FirewatchSink::new(output_file(&arguments.output)?, wallet)?;

    // Reverse lookup returns sound candidates. The sink confirms exact matches.
    let scan = Instant::now();
    let mut progress = ReadProgress::new(None);
    let targeted = archive.for_each_signer_wallet_candidate_block_parallel(
        &wallet,
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
