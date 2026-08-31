use std::{env, error::Error, num::NonZeroU32, time::Instant};

use blockzilla_car_read_sdk::{
    ArchiveInstructionSource, ArchiveInstructionSourceExt, CarArchive, CarArchiveHttpProfile,
    CarArchiveOptions, ScanRequest,
};

const DEFAULT_MAX_BLOCKS: u32 = 1_024;

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = arguments()?;
    let total_started = Instant::now();

    let mut archive = match (arguments.route, arguments.http_options) {
        (Route::Worker, None) => CarArchive::open(
            &arguments.origin,
            arguments.epoch,
            arguments.expected_blocks,
        )?,
        (Route::Worker, Some(options)) => CarArchive::open_with_options(
            &arguments.origin,
            arguments.epoch,
            arguments.expected_blocks,
            options,
        )?,
        (Route::OldFaithful, None) => CarArchive::open_old_faithful(
            &arguments.origin,
            arguments.epoch,
            arguments.expected_blocks,
        )?,
        (Route::OldFaithful, Some(options)) => CarArchive::open_old_faithful_with_options(
            &arguments.origin,
            arguments.epoch,
            arguments.expected_blocks,
            options,
        )?,
        (Route::OldFaithfulOperatorTrusted, None) => {
            CarArchive::open_old_faithful_operator_trusted(
                &arguments.origin,
                arguments.epoch,
                arguments.expected_blocks,
            )?
        }
        (Route::OldFaithfulOperatorTrusted, Some(options)) => {
            CarArchive::open_old_faithful_operator_trusted_with_options(
                &arguments.origin,
                arguments.epoch,
                arguments.expected_blocks,
                options,
            )?
        }
    };
    let setup_seconds = total_started.elapsed().as_secs_f64();
    let bound_source_size_bytes = archive.bound_source_size_bytes();
    let http_profile = archive
        .http_profile()
        .ok_or("the network CAR reader has no HTTP profile")?;
    let verification = archive.identity().verification;
    let range = archive.bounded_range(0, arguments.max_blocks)?;
    let request = benchmark_request(range);

    let setup_io = archive.io_snapshot();
    let mut first_slot = None;
    let mut last_slot = None;
    let scan_started = Instant::now();
    let (receipt, block_universe) = archive.for_each_block_fingerprinted(&request, |block| {
        first_slot.get_or_insert(block.header.slot);
        last_slot = Some(block.header.slot);
        Ok(())
    })?;
    let total_io = archive.finish_io();
    let scan_seconds = scan_started.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
    let scan_io = total_io.saturating_sub(setup_io);
    let total_seconds = total_started.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
    let first_slot = first_slot.ok_or("scan returned no blocks")?;
    let last_slot = last_slot.ok_or("scan returned no blocks")?;
    let block_universe_sha256 = block_universe.sha256_hex();

    print_result(
        "car",
        arguments.epoch,
        verification,
        range.block_count.get(),
        bound_source_size_bytes,
        receipt.blocks,
        receipt.transactions,
        block_universe.records(),
        &block_universe_sha256,
        first_slot,
        last_slot,
        http_profile,
        setup_seconds,
        scan_seconds,
        total_seconds,
        setup_io,
        scan_io,
        total_io,
    );
    Ok(())
}

fn benchmark_request(range: blockzilla_car_read_sdk::ScanRange) -> ScanRequest {
    ScanRequest::bounded(range)
        .allow_incomplete_instructions()
        .allow_incomplete_cpi()
        .allow_unknown_execution()
        .without_instruction_data()
}

#[allow(clippy::too_many_arguments)]
fn print_result(
    format: &str,
    epoch: u64,
    verification: blockzilla_car_read_sdk::SourceVerification,
    requested_blocks: u32,
    bound_source_size_bytes: u64,
    blocks: u64,
    transactions: u64,
    block_universe_records: u64,
    block_universe_sha256: &str,
    first_slot: u64,
    last_slot: u64,
    http_profile: CarArchiveHttpProfile,
    setup_seconds: f64,
    scan_seconds: f64,
    total_seconds: f64,
    setup_io: blockzilla_car_read_sdk::ArchiveIoSnapshot,
    scan_io: blockzilla_car_read_sdk::ArchiveIoSnapshot,
    total_io: blockzilla_car_read_sdk::ArchiveIoSnapshot,
) {
    let scan_tps = transactions as f64 / scan_seconds;
    let total_tps = transactions as f64 / total_seconds;
    let scan_aggregate_io_bytes = scan_io
        .network_body_bytes
        .saturating_add(scan_io.cache_read_bytes);
    let total_aggregate_io_bytes = total_io
        .network_body_bytes
        .saturating_add(total_io.cache_read_bytes);
    println!(
        "format={format} epoch={epoch} verification={verification} selected_blocks={requested_blocks} bound_source_size_bytes={bound_source_size_bytes} blocks={blocks} transactions={transactions} block_universe_records={block_universe_records} block_universe_sha256={block_universe_sha256} first_slot={first_slot} last_slot={last_slot} http_verification={} http_object_binding={} http_content_hash=none http_workers={} http_window_chunks={} http_chunk_bytes={} http_body_window_bytes={} setup_s={setup_seconds:.6} scan_s={scan_seconds:.6} total_s={total_seconds:.6} scan_tps={scan_tps:.3} total_tps={total_tps:.3} setup_head_requests={} scan_head_requests={} total_head_requests={} setup_get_requests={} scan_get_requests={} total_get_requests={} setup_network_bytes={} scan_network_bytes={} total_network_bytes={} setup_cache_hits={} scan_cache_hits={} total_cache_hits={} setup_cache_downloads={} scan_cache_downloads={} total_cache_downloads={} setup_cache_read_calls={} scan_cache_read_calls={} total_cache_read_calls={} setup_cache_bytes={} scan_cache_bytes={} total_cache_bytes={} scan_network_mb_s={:.6} scan_aggregate_io_mb_s={:.6} total_network_mb_s={:.6} total_aggregate_io_mb_s={:.6}",
        http_profile.verification,
        http_profile.verification.object_binding_kind(),
        http_profile.workers,
        http_profile.window_chunks,
        http_profile.chunk_bytes,
        http_profile.body_window_bytes,
        setup_io.head_requests,
        scan_io.head_requests,
        total_io.head_requests,
        setup_io.get_requests,
        scan_io.get_requests,
        total_io.get_requests,
        setup_io.network_body_bytes,
        scan_io.network_body_bytes,
        total_io.network_body_bytes,
        setup_io.cache_hits,
        scan_io.cache_hits,
        total_io.cache_hits,
        setup_io.cache_downloads,
        scan_io.cache_downloads,
        total_io.cache_downloads,
        setup_io.cache_read_calls,
        scan_io.cache_read_calls,
        total_io.cache_read_calls,
        setup_io.cache_read_bytes,
        scan_io.cache_read_bytes,
        total_io.cache_read_bytes,
        decimal_mb_s(scan_io.network_body_bytes, scan_seconds),
        decimal_mb_s(scan_aggregate_io_bytes, scan_seconds),
        decimal_mb_s(total_io.network_body_bytes, total_seconds),
        decimal_mb_s(total_aggregate_io_bytes, total_seconds),
    );
}

fn decimal_mb_s(bytes: u64, seconds: f64) -> f64 {
    bytes as f64 / 1_000_000.0 / seconds
}

#[derive(Debug, Clone, Copy)]
enum Route {
    Worker,
    OldFaithful,
    OldFaithfulOperatorTrusted,
}

#[derive(Debug)]
struct Arguments {
    route: Route,
    origin: String,
    epoch: u64,
    expected_blocks: NonZeroU32,
    max_blocks: NonZeroU32,
    http_options: Option<CarArchiveOptions>,
}

fn arguments() -> Result<Arguments, Box<dyn Error>> {
    arguments_from(env::args().skip(1))
}

fn arguments_from(args: impl IntoIterator<Item = String>) -> Result<Arguments, Box<dyn Error>> {
    let mut args = args.into_iter();
    let usage = "usage: read-car <worker|old-faithful|old-faithful-operator-trusted> <origin> <epoch> <canonical-block-count> [max-blocks [http-workers http-window-chunks http-chunk-bytes]]";
    let route = match args.next().as_deref() {
        Some("worker") => Route::Worker,
        Some("old-faithful") => Route::OldFaithful,
        Some("old-faithful-operator-trusted") => Route::OldFaithfulOperatorTrusted,
        _ => return Err(usage.into()),
    };
    let origin = args.next().ok_or(usage)?;
    let epoch = args.next().ok_or(usage)?.parse()?;
    let expected_blocks = args.next().ok_or(usage)?.parse()?;
    let expected_blocks = NonZeroU32::new(expected_blocks)
        .ok_or("canonical-block-count must be greater than zero")?;
    let remaining = args.collect::<Vec<_>>();
    let (max_blocks, http_options) = match remaining.as_slice() {
        [] => (DEFAULT_MAX_BLOCKS, None),
        [max_blocks] => (max_blocks.parse()?, None),
        [
            max_blocks,
            http_workers,
            http_window_chunks,
            http_chunk_bytes,
        ] => {
            let options = CarArchiveOptions {
                http_workers: http_workers.parse()?,
                http_window_chunks: http_window_chunks.parse()?,
                http_chunk_bytes: http_chunk_bytes.parse()?,
                ..CarArchiveOptions::default()
            };
            let _ = options.http_body_window_bytes()?;
            (max_blocks.parse()?, Some(options))
        }
        _ => {
            return Err(
                format!("{usage}; HTTP tuning requires all three values after max-blocks").into(),
            );
        }
    };
    let max_blocks = NonZeroU32::new(max_blocks).ok_or("max-blocks must be greater than zero")?;
    Ok(Arguments {
        route,
        origin,
        epoch,
        expected_blocks,
        max_blocks,
        http_options,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(values: &[&str]) -> Result<Arguments, Box<dyn Error>> {
        arguments_from(values.iter().map(|value| (*value).to_owned()))
    }

    #[test]
    fn accepts_the_simple_default_form() {
        let arguments = parse(&["worker", "https://archive.example", "0", "431548"]).unwrap();
        assert_eq!(arguments.max_blocks.get(), DEFAULT_MAX_BLOCKS);
        assert!(arguments.http_options.is_none());
    }

    #[test]
    fn accepts_the_explicit_operator_trusted_route() {
        let arguments = parse(&[
            "old-faithful-operator-trusted",
            "https://files.old-faithful.net",
            "100",
            "430000",
        ])
        .unwrap();
        assert!(matches!(arguments.route, Route::OldFaithfulOperatorTrusted));
    }

    #[test]
    fn accepts_one_complete_http_profile() {
        let arguments = parse(&[
            "worker",
            "https://archive.example",
            "0",
            "431548",
            "1024",
            "2",
            "4",
            "1048576",
        ])
        .unwrap();
        let options = arguments.http_options.unwrap();
        assert_eq!(arguments.max_blocks.get(), 1024);
        assert_eq!(options.http_workers, 2);
        assert_eq!(options.http_window_chunks, 4);
        assert_eq!(options.http_chunk_bytes, 1_048_576);
        assert_eq!(options.http_body_window_bytes().unwrap(), 4_194_304);
    }

    #[test]
    fn rejects_partial_http_profiles() {
        for values in [
            vec![
                "worker",
                "https://archive.example",
                "0",
                "431548",
                "1024",
                "2",
            ],
            vec![
                "worker",
                "https://archive.example",
                "0",
                "431548",
                "1024",
                "2",
                "4",
            ],
        ] {
            let error = parse(&values).unwrap_err();
            assert!(error.to_string().contains("requires all three values"));
        }
    }

    #[test]
    fn rejects_invalid_http_profiles_before_network_access() {
        for tuning in [
            ["0", "4", "1048576"],
            ["5", "4", "1048576"],
            ["2", "4", "0"],
        ] {
            let mut values = vec!["worker", "https://archive.example", "0", "431548", "1024"];
            values.extend(tuning);
            assert!(parse(&values).is_err(), "accepted {tuning:?}");
        }
    }
}
