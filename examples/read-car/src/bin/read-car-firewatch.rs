use std::{error::Error, time::Instant};

use blockzilla_car_read_sdk::{ArchiveInstructionSource, CarArchive, ScanRequest};
use blockzilla_example_workloads::{FirewatchSink, firewatch_scan_request};
use blockzilla_read_car::{
    DEFAULT_FIREWATCH_WALLET, OutputFacts, Route, RunFacts, WorkloadSource, create_output,
    parse_pubkey, validate_workload_counts, workload_target_arguments,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = workload_target_arguments(
        "read-car-firewatch",
        "wallet",
        DEFAULT_FIREWATCH_WALLET,
        "car-firewatch.bin",
    )?;
    let wallet = parse_pubkey(
        arguments
            .target
            .as_deref()
            .ok_or("the FireWatch wallet is missing")?,
    )?;
    let wallet_text = bs58::encode(wallet).into_string();
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
    let request = firewatch_scan_request(base_request);

    let mut sink = FirewatchSink::new(create_output(&arguments.output)?, wallet)?;
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
        workload: "firewatch-wallet-programs",
        output_complete: report.output_complete,
        output: &report.output,
        coverage: &report.coverage,
    };
    println!(
        "{run} {output} wallet={} signer_transactions={} successful_signer_transactions={} failed_signer_transactions={} unknown_execution_signer_transactions={} incomplete_instruction_transactions={} incomplete_cpi_transactions={} reached_instructions={} distinct_programs={}",
        wallet_text,
        report.signer_transactions,
        report.successful_signer_transactions,
        report.failed_signer_transactions,
        report.unknown_execution_signer_transactions,
        report.incomplete_instruction_transactions,
        report.incomplete_cpi_transactions,
        report.reached_instructions,
        report.distinct_programs,
    );
    Ok(())
}
