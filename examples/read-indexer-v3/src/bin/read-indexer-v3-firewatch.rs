use std::error::Error;

use blockzilla_example_workloads::{FirewatchSink, firewatch_scan_request};
use blockzilla_indexer_v3_read_sdk::{IndexerV3Archive, QueryError, ScanRequest};
use blockzilla_read_indexer_v3::{
    DEFAULT_FIREWATCH_WALLET, WorkloadSource, finish_archive, output_file, parse_pubkey,
    workload_target_arguments,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_target_arguments(
        "read-indexer-v3-firewatch",
        "wallet",
        DEFAULT_FIREWATCH_WALLET,
        "indexer-v3-firewatch.bin",
    )?;
    let wallet_text = arguments
        .target
        .as_deref()
        .ok_or("the FireWatch wallet is missing")?;
    let wallet = parse_pubkey("wallet", wallet_text)?;
    let mut archive = match &arguments.source {
        WorkloadSource::Network { origin, cache_root } => {
            IndexerV3Archive::open_selective(origin, arguments.epoch, cache_root)?
        }
        WorkloadSource::LocalArchive { archive_root } => {
            IndexerV3Archive::open_local(archive_root, arguments.epoch)?
        }
    };
    let request = firewatch_scan_request(ScanRequest::all());
    let mut sink = FirewatchSink::new(output_file(&arguments.output)?, wallet)?;

    // Reverse lookup returns sound candidates. The sink confirms exact matches.
    let receipt = archive.for_each_signer_wallet_candidate_block_parallel(
        &wallet,
        &request,
        arguments.threads,
        |block| sink.process_block(block).map_err(QueryError::sink),
    )?;
    let (writer, report) = sink.finish()?.into_parts();
    writer.into_inner()?;
    finish_archive(archive)?;

    println!(
        "decoded {} candidate blocks; wrote {} wallet-program rows to {}",
        receipt.scan.candidate_blocks,
        report.output.row_count,
        arguments.output.display()
    );
    Ok(())
}
