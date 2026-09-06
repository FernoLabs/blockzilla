//! Write the common identity record for every transaction in one CAR scan.

use std::{env, error::Error, io::Write, num::NonZeroU32, path::PathBuf, time::Instant};

use blockzilla_example_workloads::TransactionIdentityDumpSink;
use blockzilla_read_car::{Route, create_output};
use of_car_reader::archive::{
    ArchiveInstructionSource, CarArchive, CarArchiveOptions, ScanRequest,
};

const USAGE: &str = "usage: read-car-transactions <worker|old-faithful|old-faithful-operator-trusted> <origin> <epoch> <canonical-block-count> <output-file> [max-blocks] --http-workers <N> --http-window-chunks <N> --http-chunk-bytes <N> [--allow-insecure-http]";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Arguments {
    route: Route,
    origin: String,
    epoch: u64,
    expected_blocks: NonZeroU32,
    output: PathBuf,
    max_blocks: Option<NonZeroU32>,
    options: CarArchiveOptions,
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = arguments_from(env::args().skip(1))?;
    let total_started = Instant::now();
    let mut archive = match arguments.route {
        Route::Worker => CarArchive::open_with_options(
            &arguments.origin,
            arguments.epoch,
            arguments.expected_blocks,
            arguments.options,
        )?,
        Route::OldFaithful => CarArchive::open_old_faithful_with_options(
            &arguments.origin,
            arguments.epoch,
            arguments.expected_blocks,
            arguments.options,
        )?,
        Route::OldFaithfulOperatorTrusted => {
            CarArchive::open_old_faithful_operator_trusted_with_options(
                &arguments.origin,
                arguments.epoch,
                arguments.expected_blocks,
                arguments.options,
            )?
        }
    };
    let identity = archive.identity().clone();
    let bound_source_size_bytes = archive.bound_source_size_bytes();
    let http_profile = archive
        .http_profile()
        .ok_or("the network CAR reader has no HTTP profile")?;
    let setup_io = archive.io_snapshot();
    let setup_seconds = total_started.elapsed().as_secs_f64();
    let request = transaction_request(match arguments.max_blocks {
        Some(count) => ScanRequest::bounded(archive.bounded_range(0, count)?),
        None => ScanRequest::all(),
    });
    let end_slot_exclusive = identity
        .first_slot
        .checked_add(identity.slots_per_epoch)
        .ok_or("epoch slot range overflows u64")?;
    let mut sink = TransactionIdentityDumpSink::new(
        create_output(&arguments.output)?,
        identity.epoch,
        identity.first_slot,
        end_slot_exclusive,
    )?;

    let scan_started = Instant::now();
    let receipt = archive.scan_ordered(&request, &mut sink)?;
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let output_finalize_started = Instant::now();
    let (mut writer, report) = sink.finish()?.into_parts();
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let output_finalize_seconds = output_finalize_started.elapsed().as_secs_f64();
    let total_io = archive.finish_io();
    let total_seconds = total_started.elapsed().as_secs_f64();
    if receipt.transactions != report.records {
        return Err("transaction dump records do not match the CAR receipt".into());
    }

    let scan_seconds = scan_seconds.max(f64::MIN_POSITIVE);
    let total_seconds = total_seconds.max(f64::MIN_POSITIVE);
    let scan_io = total_io.saturating_sub(setup_io);
    let source_bytes = receipt.io.source_read_bytes.unwrap_or(0);
    println!(
        "format=car workload=transaction-identity epoch={} source_cluster={} source_epoch={} source_first_slot={} source_end_slot_exclusive={} source_blocks={} verification={} source_binding={} bound_source_size_bytes={} http_verification={} http_object_binding={} http_content_hash=none http_workers={} http_window_chunks={} http_chunk_bytes={} http_body_window_bytes={} blocks={} transactions={} records={} output_bytes={} output_sha256={} output_first_slot={} output_last_slot={} setup_s={:.6} scan_s={:.6} output_finalize_s={:.6} total_s={:.6} scan_tps={:.3} total_tps={:.3} source_read_calls={} source_read_bytes={} scan_source_mb_s={:.6} setup_network_bytes={} scan_network_bytes={} total_network_bytes={} scan_network_mb_s={:.6} total_network_mb_s={:.6} durable_output_mb_s={:.6} output_path={}",
        arguments.epoch,
        identity.cluster_id.as_deref().unwrap_or("none"),
        identity.epoch,
        identity.first_slot,
        end_slot_exclusive,
        identity.block_count,
        identity.verification,
        identity.binding.as_deref().unwrap_or("none"),
        bound_source_size_bytes,
        http_profile.verification,
        http_profile.verification.object_binding_kind(),
        http_profile.workers,
        http_profile.window_chunks,
        http_profile.chunk_bytes,
        http_profile.body_window_bytes,
        receipt.blocks,
        receipt.transactions,
        report.records,
        report.output_bytes,
        report.output_sha256_hex(),
        slot_token(report.first_slot),
        slot_token(report.last_slot),
        setup_seconds,
        scan_seconds,
        output_finalize_seconds,
        total_seconds,
        receipt.transactions as f64 / scan_seconds,
        receipt.transactions as f64 / total_seconds,
        receipt.io.source_read_calls.unwrap_or(0),
        source_bytes,
        decimal_mb_s(source_bytes, scan_seconds),
        setup_io.network_body_bytes,
        scan_io.network_body_bytes,
        total_io.network_body_bytes,
        decimal_mb_s(scan_io.network_body_bytes, scan_seconds),
        decimal_mb_s(total_io.network_body_bytes, total_seconds),
        decimal_mb_s(report.output_bytes, total_seconds),
        arguments.output.display(),
    );
    Ok(())
}

fn transaction_request(request: ScanRequest) -> ScanRequest {
    request
        .allow_incomplete_instructions()
        .allow_incomplete_cpi()
        .allow_unknown_execution()
        .without_instructions()
        .without_instruction_accounts()
        .without_instruction_data()
        .without_required_signers()
        .without_execution_status()
}

fn arguments_from(values: impl IntoIterator<Item = String>) -> Result<Arguments, Box<dyn Error>> {
    let mut values = values.into_iter().collect::<Vec<_>>();
    let allow_insecure_http = take_switch(&mut values, "--allow-insecure-http")?;
    let http_workers = take_required_option(&mut values, "--http-workers")?;
    let http_window_chunks = take_required_option(&mut values, "--http-window-chunks")?;
    let http_chunk_bytes = take_required_option(&mut values, "--http-chunk-bytes")?;
    let mut values = values.into_iter();
    let route = match values.next().as_deref() {
        Some("worker") => Route::Worker,
        Some("old-faithful") => Route::OldFaithful,
        Some("old-faithful-operator-trusted") => Route::OldFaithfulOperatorTrusted,
        _ => return Err(USAGE.into()),
    };
    let origin = values.next().ok_or(USAGE)?;
    let epoch = values.next().ok_or(USAGE)?.parse()?;
    let expected_blocks = parse_nonzero(values.next().ok_or(USAGE)?, "canonical-block-count")?;
    let output = PathBuf::from(values.next().ok_or(USAGE)?);
    let max_blocks = values
        .next()
        .map(|value| parse_nonzero(value, "max-blocks"))
        .transpose()?;
    if values.next().is_some() {
        return Err(USAGE.into());
    }
    if allow_insecure_http {
        if matches!(route, Route::OldFaithfulOperatorTrusted) {
            return Err(
                "--allow-insecure-http cannot be used with old-faithful-operator-trusted".into(),
            );
        }
        if !is_loopback_http_origin(&origin) {
            return Err("--allow-insecure-http requires an http:// loopback origin".into());
        }
    }
    let options = CarArchiveOptions {
        allow_insecure_http,
        http_workers: http_workers.parse()?,
        http_window_chunks: http_window_chunks.parse()?,
        http_chunk_bytes: http_chunk_bytes.parse()?,
    };
    let _ = options.http_body_window_bytes()?;
    Ok(Arguments {
        route,
        origin,
        epoch,
        expected_blocks,
        output,
        max_blocks,
        options,
    })
}

fn take_switch(values: &mut Vec<String>, flag: &str) -> Result<bool, Box<dyn Error>> {
    let matches = values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| (value == flag).then_some(index))
        .collect::<Vec<_>>();
    if matches.len() > 1 {
        return Err(format!("{flag} was provided more than once").into());
    }
    if let Some(index) = matches.first().copied() {
        values.remove(index);
        return Ok(true);
    }
    Ok(false)
}

fn take_required_option(values: &mut Vec<String>, flag: &str) -> Result<String, Box<dyn Error>> {
    let matches = values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| (value == flag).then_some(index))
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(format!("{flag} must be provided exactly once\n{USAGE}").into());
    }
    let index = matches[0];
    if index + 1 >= values.len() {
        return Err(format!("{flag} requires a value\n{USAGE}").into());
    }
    values.remove(index);
    Ok(values.remove(index))
}

fn parse_nonzero(value: String, name: &str) -> Result<NonZeroU32, Box<dyn Error>> {
    NonZeroU32::new(value.parse()?)
        .ok_or_else(|| format!("{name} must be greater than zero").into())
}

fn is_loopback_http_origin(origin: &str) -> bool {
    let Some(host) = origin.strip_prefix("http://") else {
        return false;
    };
    let host = host.split('/').next().unwrap_or_default();
    host == "localhost"
        || host.starts_with("localhost:")
        || host == "[::1]"
        || host.starts_with("[::1]:")
        || host == "127.0.0.1"
        || host.starts_with("127.0.0.1:")
}

fn decimal_mb_s(bytes: u64, seconds: f64) -> f64 {
    bytes as f64 / 1_000_000.0 / seconds.max(f64::MIN_POSITIVE)
}

fn slot_token(slot: Option<u64>) -> String {
    slot.map_or_else(|| "none".to_owned(), |slot| slot.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_explicit_http_controls() {
        let arguments = arguments_from(
            [
                "old-faithful",
                "https://mirror.example",
                "900",
                "431858",
                "/tmp/out.bin",
                "--http-workers",
                "4",
                "--http-window-chunks",
                "8",
                "--http-chunk-bytes",
                "33554432",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(arguments.epoch, 900);
        assert_eq!(arguments.options.http_workers, 4);
        assert!(!arguments.options.allow_insecure_http);
    }

    #[test]
    fn accepts_loopback_http_only_for_strict_routes() {
        let arguments = arguments_from(
            [
                "old-faithful",
                "http://127.0.0.1:8080",
                "900",
                "431858",
                "/tmp/out.bin",
                "--http-workers",
                "4",
                "--http-window-chunks",
                "8",
                "--http-chunk-bytes",
                "33554432",
                "--allow-insecure-http",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        assert!(arguments.options.allow_insecure_http);
        assert!(
            arguments_from(
                [
                    "old-faithful-operator-trusted",
                    "http://127.0.0.1:8080",
                    "900",
                    "431858",
                    "/tmp/out.bin",
                    "--http-workers",
                    "4",
                    "--http-window-chunks",
                    "8",
                    "--http-chunk-bytes",
                    "33554432",
                    "--allow-insecure-http",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .is_err()
        );
    }
}
