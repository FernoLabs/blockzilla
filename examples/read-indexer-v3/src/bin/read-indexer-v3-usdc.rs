use std::error::Error;

use blockzilla_example_workloads::{MAINNET_USDC_MINT, UsdcBalanceSink, usdc_scan_request};
use blockzilla_indexer_v3_read_sdk::{IndexerV3Archive, ScanRequest};
use blockzilla_read_indexer_v3::{WorkloadSource, finish_archive, output_file, workload_arguments};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_arguments("read-indexer-v3-usdc", "indexer-v3-usdc.bin")?;
    let mut archive = match &arguments.source {
        WorkloadSource::Network { origin, cache_root } => {
            IndexerV3Archive::open(origin, arguments.epoch, cache_root)?
        }
        WorkloadSource::LocalArchive { archive_root } => {
            IndexerV3Archive::open_local(archive_root, arguments.epoch)?
        }
    };
    let request = usdc_scan_request(ScanRequest::all(), MAINNET_USDC_MINT);
    let mut sink = UsdcBalanceSink::mainnet(output_file(&arguments.output)?)?;

    let receipt = archive.scan_ordered_parallel(&request, arguments.threads, &mut sink)?;
    let (writer, report) = sink.finish()?.into_parts();
    writer.into_inner()?;
    finish_archive(archive)?;

    println!(
        "read {} blocks and {} transactions; wrote {} USDC balance rows to {}",
        receipt.scan.blocks,
        receipt.scan.transactions,
        report.output.row_count,
        arguments.output.display()
    );
    Ok(())
}
