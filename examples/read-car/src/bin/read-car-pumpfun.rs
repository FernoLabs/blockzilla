use std::{error::Error, time::Instant};

use blockzilla_car_read_sdk::{ArchiveInstructionSource, CarArchive, ScanRequest};
use blockzilla_example_workloads::{MAINNET_PUMP_FUN_PROGRAM_BASE58, PumpSink, pump_scan_request};
use blockzilla_read_car::{
    OutputFacts, Route, RunFacts, WorkloadSource, create_output, validate_workload_counts,
    workload_arguments,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_arguments("read-car-pumpfun", "car-pumpfun.bin")?;
    let total_started = Instant::now();

    let mut archive = match &arguments.source {
        WorkloadSource::Network { route, origin } => match route {
            Route::Worker => CarArchive::open(origin, arguments.epoch, arguments.expected_blocks)?,
            Route::OldFaithful => {
                CarArchive::open_old_faithful(origin, arguments.epoch, arguments.expected_blocks)?
            }
            Route::OldFaithfulOperatorTrusted => CarArchive::open_old_faithful_operator_trusted(
                origin,
                arguments.epoch,
                arguments.expected_blocks,
            )?,
        },
        WorkloadSource::LocalArchive { archive_root } => {
            CarArchive::open_local(archive_root, arguments.epoch, arguments.expected_blocks)?
        }
    };
    let setup_seconds = total_started.elapsed().as_secs_f64();
    let verification = archive.identity().verification;
    let bound_source_size_bytes = archive.bound_source_size_bytes();
    let http_profile = archive.http_profile();
    let base_request = match arguments.max_blocks {
        Some(max_blocks) => ScanRequest::bounded(archive.bounded_range(0, max_blocks)?),
        None => ScanRequest::all(),
    };
    let requested_blocks = base_request
        .range
        .map_or(archive.identity().block_count, |range| {
            range.block_count.get()
        });
    let request = pump_scan_request(base_request);

    let mut sink = PumpSink::mainnet(create_output(&arguments.output)?)?;
    let setup_io = archive.io_snapshot();
    let scan_started = Instant::now();
    let receipt = archive.scan_ordered(&request, &mut sink)?;
    let finished = sink.finish()?;
    let (writer, report) = finished.into_parts();
    writer.into_inner()?;
    let total_io = archive.finish_io();
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let total_seconds = total_started.elapsed().as_secs_f64();
    let scan_io = total_io.saturating_sub(setup_io);
    validate_workload_counts(&receipt, report.blocks_seen, report.transactions_seen)?;

    let run = RunFacts {
        epoch: arguments.epoch,
        verification,
        requested_blocks,
        bound_source_size_bytes,
        http_profile,
        receipt,
        setup_seconds,
        scan_seconds,
        total_seconds,
        setup_io,
        scan_io,
        total_io,
    };
    let output = OutputFacts {
        workload: "pumpfun-transactions",
        output_complete: report.output_complete,
        output: &report.output,
        coverage: &report.coverage,
    };
    println!(
        "{run} {output} program={} matching_transactions={} written_transactions={} direct_invocations={} cpi_invocations={} incomplete_instruction_transactions={} incomplete_cpi_transactions={} matches_without_primary_signature={}",
        MAINNET_PUMP_FUN_PROGRAM_BASE58,
        report.matching_transactions,
        report.written_transactions,
        report.direct_invocations,
        report.cpi_invocations,
        report.incomplete_instruction_transactions,
        report.incomplete_cpi_transactions,
        report.matches_without_primary_signature,
    );
    Ok(())
}
