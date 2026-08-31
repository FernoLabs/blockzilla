use std::error::Error;

use blockzilla_example_workloads::{MAINNET_PUMP_FUN_PROGRAM, PumpSink, pump_scan_request};
use blockzilla_indexer_v3_read_sdk::{IndexerV3Archive, QueryError, ScanRequest};
use blockzilla_read_indexer_v3::{WorkloadSource, finish_archive, output_file, workload_arguments};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_arguments("read-indexer-v3-pumpfun", "indexer-v3-pumpfun.bin")?;
    let mut archive = match &arguments.source {
        WorkloadSource::Network { origin, cache_root } => {
            IndexerV3Archive::open_selective(origin, arguments.epoch, cache_root)?
        }
        WorkloadSource::LocalArchive { archive_root } => {
            IndexerV3Archive::open_local(archive_root, arguments.epoch)?
        }
    };
    let request = pump_scan_request(ScanRequest::all());
    let mut sink = PumpSink::mainnet(output_file(&arguments.output)?)?;

    // Reverse lookup returns sound candidates. The sink confirms exact matches.
    let receipt = archive.for_each_reached_program_candidate_block_parallel(
        &MAINNET_PUMP_FUN_PROGRAM,
        &request,
        arguments.threads,
        |block| sink.process_block(block).map_err(QueryError::sink),
    )?;
    let (writer, report) = sink.finish()?.into_parts();
    writer.into_inner()?;
    finish_archive(archive)?;

    println!(
        "decoded {} candidate blocks; wrote {} Pump.fun transactions to {}",
        receipt.scan.candidate_blocks,
        report.output.row_count,
        arguments.output.display()
    );
    Ok(())
}
