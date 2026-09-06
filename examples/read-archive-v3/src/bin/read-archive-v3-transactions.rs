//! Write the common identity record for every transaction in one V3 scan.

use std::{error::Error, io::Write, time::Instant};

use blockzilla_archive_v3_reader::ArchiveInstructionSource;
use blockzilla_example_workloads::TransactionIdentityDumpSink;
use blockzilla_read_archive_v3::{
    ReadProfile, arguments, finish_archive, open_archive, output_file, scan_request,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = arguments("read-archive-v3-transactions")?;
    let total_started = Instant::now();
    let mut archive = open_archive(&arguments, ReadProfile::OrderedFullScan)?;
    let identity = archive.identity().clone();
    let bound_source_size_bytes = archive.bound_source_size_bytes();
    let setup_io = archive.transport_snapshot();
    let setup_seconds = total_started.elapsed().as_secs_f64();
    let request = transaction_request(scan_request(&archive, arguments.max_blocks)?);
    let end_slot_exclusive = identity
        .first_slot
        .checked_add(identity.slots_per_epoch)
        .ok_or("epoch slot range overflows u64")?;
    let mut sink = TransactionIdentityDumpSink::new(
        output_file(&arguments.output)?,
        identity.epoch,
        identity.first_slot,
        end_slot_exclusive,
    )?;

    let scan_started = Instant::now();
    let parallel = archive.scan_ordered_parallel(&request, arguments.threads, &mut sink)?;
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let receipt = parallel.scan;
    let output_finalize_started = Instant::now();
    let (mut writer, report) = sink.finish()?.into_parts();
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let output_finalize_seconds = output_finalize_started.elapsed().as_secs_f64();
    let total_io = finish_archive(archive)?;
    let total_seconds = total_started.elapsed().as_secs_f64();
    if receipt.transactions != report.records {
        return Err("transaction dump records do not match the Indexer V3 receipt".into());
    }

    let scan_seconds = scan_seconds.max(f64::MIN_POSITIVE);
    let total_seconds = total_seconds.max(f64::MIN_POSITIVE);
    let scan_io = subtract_transport(total_io, setup_io);
    let setup_http = setup_io.http_and_cache;
    let scan_http = scan_io.http_and_cache;
    let total_http = total_io.http_and_cache;
    let source_bytes = receipt.io.source_read_bytes.unwrap_or(0);
    println!(
        "format=indexer-v3 workload=transaction-identity scan_kind=ordered-full-scan epoch={} source_cluster={} source_epoch={} source_first_slot={} source_end_slot_exclusive={} source_blocks={} verification={} source_binding={} bound_source_size_bytes={} source={} transport_kind={} threads={} requested_workers={} effective_workers={} max_active_workers={} parallel_jobs={} projected_blocks={} blocks={} transactions={} records={} output_bytes={} output_sha256={} output_first_slot={} output_last_slot={} setup_s={:.6} scan_s={:.6} output_finalize_s={:.6} total_s={:.6} scan_tps={:.3} total_tps={:.3} scan_logical_read_calls={} scan_logical_read_bytes={} scan_logical_read_mb_s={:.6} setup_network_bytes={} scan_network_bytes={} total_network_bytes={} setup_incomplete_body_retries={} scan_incomplete_body_retries={} total_incomplete_body_retries={} scan_network_mb_s={:.6} total_network_mb_s={:.6} setup_cache_read_bytes={} scan_cache_read_bytes={} total_cache_read_bytes={} setup_local_read_calls={} scan_local_read_calls={} total_local_read_calls={} setup_local_read_bytes={} scan_local_read_bytes={} total_local_read_bytes={} scan_local_read_mb_s={:.6} total_local_read_mb_s={:.6} durable_output_mb_s={:.6} output_path={}",
        arguments.epoch,
        identity.cluster_id.as_deref().unwrap_or("none"),
        identity.epoch,
        identity.first_slot,
        end_slot_exclusive,
        identity.block_count,
        identity.verification,
        identity.binding.as_deref().unwrap_or("none"),
        bound_source_size_bytes,
        match &arguments.source {
            blockzilla_read_archive_v3::Source::Network { .. } => "network",
            blockzilla_read_archive_v3::Source::LocalSplit { .. } => "local-split",
        },
        transport_name(total_io.kind),
        arguments.threads,
        parallel.parallel.requested_workers,
        parallel.parallel.effective_workers,
        parallel.parallel.max_active_workers,
        parallel.parallel.jobs,
        parallel.parallel.projected_blocks,
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
        setup_http.network_body_bytes,
        scan_http.network_body_bytes,
        total_http.network_body_bytes,
        setup_http.incomplete_body_retries,
        scan_http.incomplete_body_retries,
        total_http.incomplete_body_retries,
        decimal_mb_s(scan_http.network_body_bytes, scan_seconds),
        decimal_mb_s(total_http.network_body_bytes, total_seconds),
        setup_http.cache_read_bytes,
        scan_http.cache_read_bytes,
        total_http.cache_read_bytes,
        setup_io.local_read_calls,
        scan_io.local_read_calls,
        total_io.local_read_calls,
        setup_io.local_read_bytes,
        scan_io.local_read_bytes,
        total_io.local_read_bytes,
        decimal_mb_s(scan_io.local_read_bytes, scan_seconds),
        decimal_mb_s(total_io.local_read_bytes, total_seconds),
        decimal_mb_s(report.output_bytes, total_seconds),
        arguments.output.display(),
    );
    Ok(())
}

fn transaction_request(
    request: blockzilla_archive_v3_reader::ScanRequest,
) -> blockzilla_archive_v3_reader::ScanRequest {
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

fn subtract_transport(
    later: blockzilla_archive_v3_reader::IndexerV3TransportReceipt,
    earlier: blockzilla_archive_v3_reader::IndexerV3TransportReceipt,
) -> blockzilla_archive_v3_reader::IndexerV3TransportReceipt {
    blockzilla_archive_v3_reader::IndexerV3TransportReceipt {
        kind: later.kind,
        http_and_cache: later.http_and_cache.saturating_sub(earlier.http_and_cache),
        local_read_calls: later
            .local_read_calls
            .saturating_sub(earlier.local_read_calls),
        local_read_bytes: later
            .local_read_bytes
            .saturating_sub(earlier.local_read_bytes),
    }
}

fn transport_name(kind: blockzilla_archive_v3_reader::IndexerV3TransportKind) -> &'static str {
    match kind {
        blockzilla_archive_v3_reader::IndexerV3TransportKind::HttpCached => "http-cached",
        blockzilla_archive_v3_reader::IndexerV3TransportKind::LocalSplit => "local-split",
    }
}

fn decimal_mb_s(bytes: u64, seconds: f64) -> f64 {
    bytes as f64 / 1_000_000.0 / seconds.max(f64::MIN_POSITIVE)
}

fn slot_token(slot: Option<u64>) -> String {
    slot.map_or_else(|| "none".to_owned(), |slot| slot.to_string())
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, num::NonZeroUsize};

    #[test]
    fn parses_a_local_parallel_transaction_dump() {
        let arguments = blockzilla_read_archive_v3::arguments_from(
            "read-archive-v3-transactions",
            None,
            [
                "local-split",
                "/ledger",
                "/retained",
                "900",
                "candidate-900",
                "/tmp/out.bin",
                "--threads",
                "12",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(arguments.epoch, 900);
        assert_eq!(arguments.threads, NonZeroUsize::new(12).unwrap());
        assert!(arguments.max_blocks.is_none());
    }
}
