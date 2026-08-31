use std::{error::Error, time::Instant};

use blockzilla_car_read_sdk::{ArchiveInstructionSource, CarArchive, ScanRequest};
use blockzilla_example_workloads::{MAINNET_USDC_MINT_BASE58, UsdcBalanceSink, usdc_scan_request};
use blockzilla_read_car::{
    OutputFacts, RunFacts, WorkloadSource, create_output, validate_workload_counts,
    workload_arguments,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_arguments("read-car-usdc", "car-usdc.bin")?;
    let total_started = Instant::now();

    let mut archive = match &arguments.source {
        WorkloadSource::Network { origin } => {
            CarArchive::open(origin, arguments.epoch, arguments.expected_blocks)?
        }
        WorkloadSource::LocalArchive { archive_root } => {
            CarArchive::open_local(archive_root, arguments.epoch, arguments.expected_blocks)?
        }
    };
    let setup_seconds = total_started.elapsed().as_secs_f64();
    let verification = archive.identity().verification;
    let bound_source_size_bytes = archive.bound_source_size_bytes();
    let base_request = ScanRequest::all();
    let requested_blocks = archive.identity().block_count;
    let request = usdc_scan_request(
        base_request,
        blockzilla_example_workloads::MAINNET_USDC_MINT,
    );

    let mut sink = UsdcBalanceSink::mainnet(create_output(&arguments.output)?)?;
    let setup_io = archive.io_snapshot();
    let scan_started = Instant::now();
    let receipt = archive.scan_ordered(&request, &mut sink)?;
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let scan_io = archive.io_snapshot().saturating_sub(setup_io);
    let finished = sink.finish()?;
    let (writer, report) = finished.into_parts();
    writer.into_inner()?;
    let total_io = archive.finish_io();
    let total_seconds = total_started.elapsed().as_secs_f64();
    validate_workload_counts(&receipt, report.blocks_seen, report.transactions_seen)?;

    let run = RunFacts {
        epoch: arguments.epoch,
        verification,
        requested_blocks,
        bound_source_size_bytes,
        receipt,
        setup_seconds,
        scan_seconds,
        total_seconds,
        setup_io,
        scan_io,
        total_io,
    };
    let output = OutputFacts {
        workload: "usdc-recorded-balances",
        output_complete: report.output_complete,
        output: &report.output,
        coverage: &report.coverage,
    };
    println!(
        "{run} {output} mint={} matching_transactions={} pre_rows={} post_rows={} token_balances_unavailable_transactions={} token_mint_unavailable_transactions={}",
        MAINNET_USDC_MINT_BASE58,
        report.matching_transactions,
        report.pre_rows,
        report.post_rows,
        report.token_balances_unavailable_transactions,
        report.token_mint_unavailable_transactions,
    );
    Ok(())
}
