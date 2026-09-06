use std::{env, error::Error, num::NonZeroU32, path::PathBuf, time::Instant};

use blockzilla_compact_v2_reader::archive::{
    ArchiveInstructionSource, ArchiveInstructionSourceExt, CompactV2Archive,
    CompactV2LocalDescriptor, CompactV2TransportReceipt, ScanRequest,
};

const DEFAULT_MAX_BLOCKS: u32 = 1_024;

fn main() -> Result<(), Box<dyn Error>> {
    let (input, max_blocks) = arguments()?;
    let total_started = Instant::now();

    let (mut archive, epoch) = match input {
        Input::Network {
            origin,
            epoch,
            cache_root,
        } => (CompactV2Archive::open(&origin, epoch, cache_root)?, epoch),
        Input::Local {
            root,
            epoch,
            candidate_id,
        } => {
            let descriptor = CompactV2LocalDescriptor::mainnet(epoch, candidate_id)?;
            (CompactV2Archive::open_local(root, descriptor)?, epoch)
        }
    };
    let setup_seconds = total_started.elapsed().as_secs_f64();
    let bound_source_size_bytes = archive.bound_source_size_bytes();
    let verification = archive.identity().verification;
    let transport_kind = archive.transport_kind();
    let candidate_id = archive.candidate_id().unwrap_or("none").to_owned();
    let range = archive.bounded_range(0, max_blocks)?;
    let request = benchmark_request(range);

    let setup_io = archive.transport_snapshot();
    let mut first_slot = None;
    let mut last_slot = None;
    let scan_started = Instant::now();
    let receipt = archive.for_each_block(&request, |block| {
        first_slot.get_or_insert(block.header.slot);
        last_slot = Some(block.header.slot);
        Ok(())
    })?;
    archive.verify_local_unchanged()?;
    let total_io = archive.finish_transport_io();
    let scan_seconds = scan_started.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
    let scan_io = total_io.saturating_sub(setup_io);
    let total_seconds = total_started.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
    let first_slot = first_slot.ok_or("scan returned no blocks")?;
    let last_slot = last_slot.ok_or("scan returned no blocks")?;
    print_result(
        "compact-v2",
        epoch,
        verification,
        transport_kind,
        &candidate_id,
        range.block_count.get(),
        bound_source_size_bytes,
        receipt.blocks,
        receipt.transactions,
        first_slot,
        last_slot,
        setup_seconds,
        scan_seconds,
        total_seconds,
        setup_io,
        scan_io,
        total_io,
    );
    Ok(())
}

fn benchmark_request(range: blockzilla_compact_v2_reader::archive::ScanRange) -> ScanRequest {
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
    verification: blockzilla_compact_v2_reader::archive::SourceVerification,
    transport_kind: blockzilla_compact_v2_reader::archive::CompactV2TransportKind,
    candidate_id: &str,
    requested_blocks: u32,
    bound_source_size_bytes: u64,
    blocks: u64,
    transactions: u64,
    first_slot: u64,
    last_slot: u64,
    setup_seconds: f64,
    scan_seconds: f64,
    total_seconds: f64,
    setup_io: CompactV2TransportReceipt,
    scan_io: CompactV2TransportReceipt,
    total_io: CompactV2TransportReceipt,
) {
    let scan_tps = transactions as f64 / scan_seconds;
    let total_tps = transactions as f64 / total_seconds;
    let setup_http = setup_io.http_and_cache;
    let scan_http = scan_io.http_and_cache;
    let total_http = total_io.http_and_cache;
    let scan_aggregate_io_bytes = scan_http
        .network_body_bytes
        .saturating_add(scan_http.cache_read_bytes)
        .saturating_add(scan_io.local_read_bytes);
    let total_aggregate_io_bytes = total_http
        .network_body_bytes
        .saturating_add(total_http.cache_read_bytes)
        .saturating_add(total_io.local_read_bytes);
    println!(
        "format={format} epoch={epoch} verification={verification} transport_kind={transport_kind} candidate_id={candidate_id} selected_blocks={requested_blocks} bound_source_size_bytes={bound_source_size_bytes} blocks={blocks} transactions={transactions} first_slot={first_slot} last_slot={last_slot} setup_s={setup_seconds:.6} scan_s={scan_seconds:.6} total_s={total_seconds:.6} scan_tps={scan_tps:.3} total_tps={total_tps:.3} setup_head_requests={} scan_head_requests={} total_head_requests={} setup_get_requests={} scan_get_requests={} total_get_requests={} setup_network_bytes={} scan_network_bytes={} total_network_bytes={} setup_cache_hits={} scan_cache_hits={} total_cache_hits={} setup_cache_downloads={} scan_cache_downloads={} total_cache_downloads={} setup_cache_read_calls={} scan_cache_read_calls={} total_cache_read_calls={} setup_cache_bytes={} scan_cache_bytes={} total_cache_bytes={} setup_local_read_calls={} scan_local_read_calls={} total_local_read_calls={} setup_local_read_bytes={} scan_local_read_bytes={} total_local_read_bytes={} setup_local_mb_s={:.6} scan_local_mb_s={:.6} total_local_mb_s={:.6} scan_network_mb_s={:.6} scan_aggregate_io_mb_s={:.6} total_network_mb_s={:.6} total_aggregate_io_mb_s={:.6}",
        setup_http.head_requests,
        scan_http.head_requests,
        total_http.head_requests,
        setup_http.get_requests,
        scan_http.get_requests,
        total_http.get_requests,
        setup_http.network_body_bytes,
        scan_http.network_body_bytes,
        total_http.network_body_bytes,
        setup_http.cache_hits,
        scan_http.cache_hits,
        total_http.cache_hits,
        setup_http.cache_downloads,
        scan_http.cache_downloads,
        total_http.cache_downloads,
        setup_http.cache_read_calls,
        scan_http.cache_read_calls,
        total_http.cache_read_calls,
        setup_http.cache_read_bytes,
        scan_http.cache_read_bytes,
        total_http.cache_read_bytes,
        setup_io.local_read_calls,
        scan_io.local_read_calls,
        total_io.local_read_calls,
        setup_io.local_read_bytes,
        scan_io.local_read_bytes,
        total_io.local_read_bytes,
        decimal_mb_s(setup_io.local_read_bytes, setup_seconds),
        decimal_mb_s(scan_io.local_read_bytes, scan_seconds),
        decimal_mb_s(total_io.local_read_bytes, total_seconds),
        decimal_mb_s(scan_http.network_body_bytes, scan_seconds),
        decimal_mb_s(scan_aggregate_io_bytes, scan_seconds),
        decimal_mb_s(total_http.network_body_bytes, total_seconds),
        decimal_mb_s(total_aggregate_io_bytes, total_seconds),
    );
}

fn decimal_mb_s(bytes: u64, seconds: f64) -> f64 {
    bytes as f64 / 1_000_000.0 / seconds
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Input {
    Network {
        origin: String,
        epoch: u64,
        cache_root: PathBuf,
    },
    Local {
        root: PathBuf,
        epoch: u64,
        candidate_id: String,
    },
}

fn arguments() -> Result<(Input, NonZeroU32), Box<dyn Error>> {
    arguments_from(env::args().skip(1))
}

fn arguments_from(
    mut args: impl Iterator<Item = String>,
) -> Result<(Input, NonZeroU32), Box<dyn Error>> {
    let usage = "usage:\n  read-compact-v2 <worker-origin> <epoch> <absolute-cache-root> [max-blocks]\n  read-compact-v2 local <absolute-compact-root> <epoch> <candidate-id> [max-blocks]";
    let first = args.next().ok_or(usage)?;
    let input = if first == "local" {
        let root = PathBuf::from(args.next().ok_or(usage)?);
        let epoch = args.next().ok_or(usage)?.parse()?;
        let candidate_id = args.next().ok_or(usage)?;
        Input::Local {
            root,
            epoch,
            candidate_id,
        }
    } else {
        let origin = first;
        let epoch = args.next().ok_or(usage)?.parse()?;
        let cache_root = PathBuf::from(args.next().ok_or(usage)?);
        Input::Network {
            origin,
            epoch,
            cache_root,
        }
    };
    let max_blocks = args
        .next()
        .map_or(Ok(DEFAULT_MAX_BLOCKS), |value| value.parse())?;
    let max_blocks = NonZeroU32::new(max_blocks).ok_or("max-blocks must be greater than zero")?;
    if args.next().is_some() {
        return Err(usage.into());
    }
    Ok((input, max_blocks))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_local_candidate_mode() {
        let (input, blocks) = arguments_from(
            [
                "local",
                "/nas/epoch-900",
                "900",
                "epoch-900-corrected-v2",
                "4096",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(blocks.get(), 4096);
        assert_eq!(
            input,
            Input::Local {
                root: PathBuf::from("/nas/epoch-900"),
                epoch: 900,
                candidate_id: "epoch-900-corrected-v2".into(),
            }
        );
    }

    #[test]
    fn network_arguments_default_to_current_wire_grammars() {
        let (input, blocks) = arguments_from(
            ["https://example.test", "0", "/tmp/cache"]
                .into_iter()
                .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(blocks.get(), DEFAULT_MAX_BLOCKS);
        let Input::Network { epoch, .. } = input else {
            panic!("expected network input");
        };
        assert_eq!(epoch, 0);
    }

    #[test]
    fn rejects_extra_arguments() {
        let result = arguments_from(
            [
                "https://example.test",
                "0",
                "/tmp/cache",
                "1024",
                "unexpected",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert!(result.is_err());
    }
}
