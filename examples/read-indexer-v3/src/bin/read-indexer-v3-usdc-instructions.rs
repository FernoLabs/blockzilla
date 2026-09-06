//! Bounded correctness runner for the instruction-derived USDC event ledger.
//!
//! The V3 SDK decodes exact selected Token instruction bytes with bounded
//! parallel projection and publishes blocks to SQLite in ledger order. This
//! command keeps its local-source contract explicit and caps the SQLite
//! workload at 10,000 blocks.

use std::{
    error::Error,
    ffi::OsString,
    fmt::Write as _,
    fs,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    time::Instant,
};

use blockzilla_dump::{
    TokenEventDatabase, TokenEventRunSpec, TokenEventScanOptions, TokenEventScanTiming,
    scan_remaining_token_events, token_event_database::TOKEN_EVENT_SCHEMA_VERSION,
};
use blockzilla_example_workloads::{MAINNET_USDC_MINT, MAINNET_USDC_MINT_BASE58};
use blockzilla_indexer_v3_read_sdk::{
    ArchiveInstructionSource, IndexerV3Archive, IndexerV3ParallelScanStats,
    IndexerV3RegistryReadMode, IndexerV3RegistryReadReceipt, IndexerV3TransportReceipt,
    MAX_INDEXER_V3_PARALLEL_WORKERS, ScanReceipt, SourceIdentity,
};
use blockzilla_query_sdk::token::{HistoryCoverage, TargetMintTracker};

const MAX_BLOCKS: u32 = 10_000;
const USAGE: &str = "usage:\n  read-indexer-v3-usdc-instructions local-split <absolute-v3-ledger-root> <absolute-retained-root> <epoch> <candidate-id> <absolute-output.sqlite> <max-blocks> --threads <1..=64>";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Arguments {
    ledger_root: PathBuf,
    retained_root: PathBuf,
    epoch: u64,
    candidate_id: String,
    output: PathBuf,
    max_blocks: NonZeroU32,
    threads: NonZeroUsize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = arguments_from(std::env::args_os().skip(1))?;
    validate_new_output(&arguments.output)?;

    let total_started = Instant::now();
    let mut archive = IndexerV3Archive::open_local_split(
        &arguments.ledger_root,
        &arguments.retained_root,
        arguments.epoch,
        &arguments.candidate_id,
    )?;
    let range = archive.bounded_range(0, arguments.max_blocks)?;
    let bound_source_size_bytes = archive.bound_source_size_bytes();
    let source_identity = archive.identity().clone();
    let setup_io = archive.transport_snapshot();
    let setup_seconds = total_started.elapsed().as_secs_f64();

    let opening = TargetMintTracker::from_sparse_start(MAINNET_USDC_MINT).snapshot();
    let spec =
        TokenEventRunSpec::classic(source_identity.clone(), MAINNET_USDC_MINT, range, opening);
    // `create` is intentional. A prior database or SQLite sidecar is an error;
    // this correctness runner never replaces or resumes an output artifact.
    let mut database = TokenEventDatabase::create(&arguments.output, spec)?;

    let scan_started = Instant::now();
    let (scan, v3_receipt) = {
        let mut parallel_source = archive.parallel_instruction_source(arguments.threads)?;
        let scan = scan_remaining_token_events(
            &mut parallel_source,
            &mut database,
            TokenEventScanOptions::default(),
        )?;
        if scan.already_complete {
            return Err("a newly created token-event database was already complete".into());
        }
        let receipt = parallel_source
            .take_last_receipt()
            .ok_or("V3 parallel scan returned no execution statistics")?;
        (scan, receipt)
    };
    let parallel = v3_receipt.parallel;
    let registry = v3_receipt.registry;
    if parallel.projected_blocks != scan.receipt.blocks {
        return Err(format!(
            "V3 parallel projection reported {} blocks, but the common scan reported {}",
            parallel.projected_blocks, scan.receipt.blocks
        )
        .into());
    }
    let checkpoint_started = Instant::now();
    database.checkpoint_wal()?;
    let checkpoint_seconds = checkpoint_started.elapsed().as_secs_f64();
    let retained_accounts = database.tracker().retained_account_count();
    let committed_pubkey_cache_entries = database.committed_pubkey_cache_entries();
    let pipeline_plus_checkpoint_seconds = scan_started.elapsed().as_secs_f64();
    drop(database);
    archive.verify_local_unchanged()?;
    let total_io = archive.finish_transport_io();
    let total_seconds = total_started.elapsed().as_secs_f64();

    let audit_started = Instant::now();
    let audit = TokenEventDatabase::audit_read_only(&arguments.output)?;
    if audit.resume.tracker.history_coverage() != HistoryCoverage::Partial {
        return Err("a sparse USDC scan did not retain partial history coverage".into());
    }
    let output_bytes = fs::metadata(&arguments.output)?.len();
    let audit_seconds = audit_started.elapsed().as_secs_f64();

    print_result(
        &arguments,
        &source_identity,
        range.block_count.get(),
        bound_source_size_bytes,
        scan.receipt,
        scan.timing,
        parallel,
        registry,
        setup_seconds,
        pipeline_plus_checkpoint_seconds,
        checkpoint_seconds,
        total_seconds,
        audit_seconds,
        setup_io,
        total_io,
        retained_accounts,
        committed_pubkey_cache_entries,
        output_bytes,
        audit.resume.next_block_ordinal,
        &audit.digest_head,
        &audit.tracker_digest,
    );
    Ok(())
}

fn arguments_from(values: impl IntoIterator<Item = OsString>) -> Result<Arguments, Box<dyn Error>> {
    let mut values = values.into_iter().collect::<Vec<_>>();
    if values.len() < 2
        || values
            .get(values.len().saturating_sub(2))
            .and_then(|value| value.to_str())
            != Some("--threads")
    {
        return Err(format!("--threads <worker-count> is required\n{USAGE}").into());
    }
    let threads = text(values.pop(), USAGE)?.parse::<usize>()?;
    values.pop();
    let threads = NonZeroUsize::new(threads).ok_or("threads must be greater than zero")?;
    if threads.get() > MAX_INDEXER_V3_PARALLEL_WORKERS {
        return Err(format!("threads must be in 1..={MAX_INDEXER_V3_PARALLEL_WORKERS}").into());
    }

    let mut values = values.into_iter();
    if text(values.next(), USAGE)? != "local-split" {
        return Err(format!("this bounded runner accepts only a local V3 source\n{USAGE}").into());
    }
    let ledger_root = PathBuf::from(values.next().ok_or(USAGE)?);
    let retained_root = PathBuf::from(values.next().ok_or(USAGE)?);
    let epoch = text(values.next(), USAGE)?.parse::<u64>()?;
    let candidate_id = text(values.next(), USAGE)?;
    let output = PathBuf::from(values.next().ok_or(USAGE)?);
    let max_blocks = text(values.next(), USAGE)?.parse::<u32>()?;
    if values.next().is_some() {
        return Err(USAGE.into());
    }

    if !ledger_root.is_absolute() {
        return Err("V3 ledger root must be absolute".into());
    }
    if !retained_root.is_absolute() {
        return Err("retained root must be absolute".into());
    }
    if candidate_id.is_empty() || candidate_id.chars().any(char::is_whitespace) {
        return Err("candidate-id must be one nonempty token".into());
    }
    if !output.is_absolute() {
        return Err("output path must be absolute".into());
    }
    let max_blocks = NonZeroU32::new(max_blocks).ok_or("max-blocks must be greater than zero")?;
    if max_blocks.get() > MAX_BLOCKS {
        return Err(format!(
            "max-blocks must be in 1..={MAX_BLOCKS}; this SQLite command is a bounded correctness runner"
        )
        .into());
    }

    Ok(Arguments {
        ledger_root,
        retained_root,
        epoch,
        candidate_id,
        output,
        max_blocks,
        threads,
    })
}

fn validate_new_output(path: &Path) -> Result<(), Box<dyn Error>> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or("output path has no parent directory")?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err("output parent must be an existing non-symlink directory".into());
    }
    for candidate in sqlite_paths(path) {
        match fs::symlink_metadata(&candidate) {
            Ok(_) => {
                return Err(format!(
                    "refusing to replace existing SQLite output or sidecar {}",
                    candidate.display()
                )
                .into());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn sqlite_paths(path: &Path) -> [PathBuf; 4] {
    [
        path.to_path_buf(),
        with_suffix(path, "-journal"),
        with_suffix(path, "-wal"),
        with_suffix(path, "-shm"),
    ]
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

#[allow(clippy::too_many_arguments)]
fn print_result(
    arguments: &Arguments,
    source: &SourceIdentity,
    selected_blocks: u32,
    bound_source_size_bytes: u64,
    receipt: ScanReceipt,
    timing: TokenEventScanTiming,
    parallel: IndexerV3ParallelScanStats,
    registry: IndexerV3RegistryReadReceipt,
    setup_seconds: f64,
    pipeline_plus_checkpoint_seconds: f64,
    checkpoint_seconds: f64,
    total_seconds: f64,
    audit_seconds: f64,
    setup_io: IndexerV3TransportReceipt,
    total_io: IndexerV3TransportReceipt,
    retained_accounts: usize,
    committed_pubkey_cache_entries: usize,
    output_bytes: u64,
    next_block_ordinal: u32,
    ledger_digest_head: &[u8; 32],
    tracker_digest: &[u8; 32],
) {
    let artifact_ready_seconds_nonzero = pipeline_plus_checkpoint_seconds.max(f64::MIN_POSITIVE);
    let pipeline_seconds = timing.scan_elapsed.as_secs_f64();
    let pipeline_seconds_nonzero = pipeline_seconds.max(f64::MIN_POSITIVE);
    let database_seconds = timing.database_block_elapsed.as_secs_f64();
    let coordinator_outside_database_seconds =
        timing.coordinator_outside_database_elapsed.as_secs_f64();
    let total_seconds_nonzero = total_seconds.max(f64::MIN_POSITIVE);
    let scan_local_read_calls = total_io
        .local_read_calls
        .saturating_sub(setup_io.local_read_calls);
    let scan_local_read_bytes = total_io
        .local_read_bytes
        .saturating_sub(setup_io.local_read_bytes);
    let source_read_bytes = receipt.io.source_read_bytes.unwrap_or(0);
    let decoded_bytes = receipt.io.decoded_bytes.unwrap_or(0);
    let threads = arguments.threads.get();
    let requested_workers = parallel.requested_workers;
    let effective_workers = parallel.effective_workers;
    let max_active_workers = parallel.max_active_workers;
    let parallel_jobs = parallel.jobs;
    let projected_blocks = parallel.projected_blocks;
    let blocks_per_job_limit = parallel.blocks_per_job_limit;
    let job_window_limit = parallel.job_window_limit;
    let max_in_flight_jobs = parallel.max_in_flight_jobs;
    let max_coordinator_pending_results = parallel.max_coordinator_pending_results;
    let max_result_channel_backlog = parallel.max_result_channel_backlog;
    let max_coordinator_pending_projected_blocks =
        parallel.max_coordinator_pending_projected_blocks;
    let parallel_max_in_flight_transactions = parallel.max_in_flight_transactions;
    let parallel_max_in_flight_declared_decoded_bytes =
        parallel.max_in_flight_declared_decoded_bytes;
    let parallel_max_owned_payload_block_bytes = parallel.max_owned_payload_block_bytes;
    let parallel_max_in_flight_owned_payload_bytes = parallel.max_in_flight_owned_payload_bytes;
    let registry_mode = registry_mode_name(registry.mode);
    let registry_prefetch_read_calls = registry.prefetch_read_calls;
    let registry_prefetch_read_bytes = registry.prefetch_read_bytes;
    let registry_resolutions = registry.resolutions;
    let registry_hits = registry.hits;
    let registry_misses = registry.misses;
    let registry_evictions = registry.evictions;
    let registry_resident_payload_bytes = registry.resident_payload_bytes;
    let db = timing.database;
    let db_source_validation_digest_seconds = db.source_validation_and_digest_elapsed.as_secs_f64();
    let db_transaction_setup_seconds = db.sqlite_transaction_setup_elapsed.as_secs_f64();
    let db_token_tracking_seconds = db.token_tracking_elapsed.as_secs_f64();
    let db_tracked_row_write_seconds = db.tracked_row_write_elapsed.as_secs_f64();
    let db_block_header_write_seconds = db.block_header_write_elapsed.as_secs_f64();
    let db_durable_digest_checkpoint_seconds =
        db.durable_digest_and_checkpoint_elapsed.as_secs_f64();
    let db_commit_seconds = db.sqlite_commit_elapsed.as_secs_f64();
    let db_error_recovery_seconds = db.error_recovery_elapsed.as_secs_f64();
    let epoch = arguments.epoch;
    let verification = source.verification;
    let candidate_id = &arguments.candidate_id;
    let blocks = receipt.blocks;
    let transactions = receipt.transactions;
    let source_instructions = receipt.instructions;
    let incomplete_instructions = receipt.transactions_with_incomplete_instructions;
    let incomplete_cpi = receipt.transactions_with_incomplete_cpi;
    let unknown_execution = receipt.transactions_with_unknown_execution;
    let instructions_not_requested = receipt.instructions_not_requested;
    let instructions_with_unknown_data = receipt.instructions_with_unknown_data;
    let scan_source_read_calls = receipt.io.source_read_calls.unwrap_or(0);
    let setup_local_read_calls = setup_io.local_read_calls;
    let total_local_read_calls = total_io.local_read_calls;
    let setup_local_read_bytes = setup_io.local_read_bytes;
    let total_local_read_bytes = total_io.local_read_bytes;
    let output_path = arguments.output.display();
    let ledger_digest = hex_lower(ledger_digest_head);
    let tracker_digest = hex_lower(tracker_digest);
    println!(
        concat!(
            "format=indexer-v3 workload=usdc-instruction-ledger source=local ",
            "scanner=ordered-parallel threads={threads} requested_workers={requested_workers} ",
            "effective_workers={effective_workers} ",
            "max_active_workers={max_active_workers} parallel_jobs={parallel_jobs} ",
            "projected_blocks={projected_blocks} blocks_per_job_limit={blocks_per_job_limit} ",
            "job_window_limit={job_window_limit} max_in_flight_jobs={max_in_flight_jobs} ",
            "max_coordinator_pending_results={max_coordinator_pending_results} ",
            "max_result_channel_backlog={max_result_channel_backlog} ",
            "max_coordinator_pending_projected_blocks={max_coordinator_pending_projected_blocks} ",
            "parallel_max_in_flight_transactions={parallel_max_in_flight_transactions} ",
            "parallel_max_in_flight_declared_decoded_bytes={parallel_max_in_flight_declared_decoded_bytes} ",
            "parallel_max_owned_payload_block_bytes={parallel_max_owned_payload_block_bytes} ",
            "parallel_max_in_flight_owned_payload_bytes={parallel_max_in_flight_owned_payload_bytes} ",
            "registry_mode={registry_mode} ",
            "registry_prefetch_read_calls={registry_prefetch_read_calls} ",
            "registry_prefetch_read_bytes={registry_prefetch_read_bytes} ",
            "registry_resolutions={registry_resolutions} registry_hits={registry_hits} ",
            "registry_misses={registry_misses} registry_evictions={registry_evictions} ",
            "registry_resident_payload_bytes={registry_resident_payload_bytes} ",
            "epoch={epoch} verification={verification} candidate_id={candidate_id} ",
            "mint={MAINNET_USDC_MINT_BASE58} history_start=sparse history_after=partial ",
            "coverage_complete=false selected_blocks={selected_blocks} blocks={blocks} ",
            "transactions={transactions} source_instructions={source_instructions} ",
            "retained_account_records={retained_accounts} ",
            "transactions_with_incomplete_instructions={incomplete_instructions} ",
            "transactions_with_incomplete_cpi={incomplete_cpi} ",
            "transactions_with_unknown_execution={unknown_execution} ",
            "instructions_not_requested={instructions_not_requested} ",
            "instructions_with_unknown_data={instructions_with_unknown_data} ",
            "setup_s={setup_seconds:.6} pipeline_s={pipeline_seconds:.6} ",
            "coordinator_outside_database_s={coordinator_outside_database_seconds:.6} ",
            "database_callback_s={database_seconds:.6} checkpoint_s={checkpoint_seconds:.6} ",
            "scan_s={pipeline_seconds:.6} ",
            "pipeline_plus_checkpoint_s={pipeline_plus_checkpoint_seconds:.6} ",
            "total_s={total_seconds:.6} audit_s={audit_seconds:.6} ",
            "scan_tps={:.3} artifact_ready_tps={:.3} total_tps={:.3} ",
            "db_block_callbacks={} db_committed_blocks={} db_visited_transactions={} ",
            "db_tracker_state_updates={} db_tracker_state_noop_writes_skipped={} ",
            "db_pubkey_cache_hits={} db_pubkey_pending_hits={} db_pubkey_sql_misses={} ",
            "committed_pubkey_cache_entries={committed_pubkey_cache_entries} ",
            "db_source_validation_digest_s={db_source_validation_digest_seconds:.6} ",
            "db_transaction_setup_s={db_transaction_setup_seconds:.6} ",
            "db_token_tracking_s={db_token_tracking_seconds:.6} ",
            "db_tracked_row_write_s={db_tracked_row_write_seconds:.6} ",
            "db_block_header_write_s={db_block_header_write_seconds:.6} ",
            "db_durable_digest_checkpoint_s={db_durable_digest_checkpoint_seconds:.6} ",
            "db_commit_s={db_commit_seconds:.6} db_error_recovery_s={db_error_recovery_seconds:.6} ",
            "bound_source_size_bytes={bound_source_size_bytes} ",
            "scan_source_read_calls={scan_source_read_calls} ",
            "scan_source_read_bytes={source_read_bytes} scan_decoded_bytes={decoded_bytes} ",
            "scan_effective_source_mb_s={:.6} artifact_ready_effective_source_mb_s={:.6} ",
            "setup_local_read_calls={setup_local_read_calls} ",
            "scan_local_read_calls={scan_local_read_calls} total_local_read_calls={total_local_read_calls} ",
            "setup_local_read_bytes={setup_local_read_bytes} ",
            "scan_local_read_bytes={scan_local_read_bytes} total_local_read_bytes={total_local_read_bytes} ",
            "scan_effective_local_read_mb_s={:.6} ",
            "artifact_ready_effective_local_read_mb_s={:.6} total_effective_local_read_mb_s={:.6} ",
            "output_path={output_path} output_schema=token-event-sqlite ",
            "output_schema_version={TOKEN_EVENT_SCHEMA_VERSION} output_bytes={output_bytes} ",
            "next_block_ordinal={next_block_ordinal} ledger_digest={ledger_digest} ",
            "tracker_digest={tracker_digest} already_complete=false"
        ),
        transactions as f64 / pipeline_seconds_nonzero,
        transactions as f64 / artifact_ready_seconds_nonzero,
        transactions as f64 / total_seconds_nonzero,
        db.block_operations,
        db.committed_blocks,
        db.visited_transactions,
        db.tracker_state_updates,
        db.tracker_state_noop_writes_skipped,
        db.pubkey_cache_hits,
        db.pubkey_pending_hits,
        db.pubkey_sql_misses,
        decimal_mb_s(source_read_bytes, pipeline_seconds_nonzero),
        decimal_mb_s(source_read_bytes, artifact_ready_seconds_nonzero),
        decimal_mb_s(scan_local_read_bytes, pipeline_seconds_nonzero),
        decimal_mb_s(scan_local_read_bytes, artifact_ready_seconds_nonzero),
        decimal_mb_s(total_io.local_read_bytes, total_seconds_nonzero),
        threads = threads,
        requested_workers = requested_workers,
        effective_workers = effective_workers,
        max_active_workers = max_active_workers,
        parallel_jobs = parallel_jobs,
        projected_blocks = projected_blocks,
        blocks_per_job_limit = blocks_per_job_limit,
        job_window_limit = job_window_limit,
        max_in_flight_jobs = max_in_flight_jobs,
        max_coordinator_pending_results = max_coordinator_pending_results,
        max_result_channel_backlog = max_result_channel_backlog,
        max_coordinator_pending_projected_blocks = max_coordinator_pending_projected_blocks,
        parallel_max_in_flight_transactions = parallel_max_in_flight_transactions,
        parallel_max_in_flight_declared_decoded_bytes =
            parallel_max_in_flight_declared_decoded_bytes,
        parallel_max_owned_payload_block_bytes = parallel_max_owned_payload_block_bytes,
        parallel_max_in_flight_owned_payload_bytes = parallel_max_in_flight_owned_payload_bytes,
        registry_mode = registry_mode,
        registry_prefetch_read_calls = registry_prefetch_read_calls,
        registry_prefetch_read_bytes = registry_prefetch_read_bytes,
        registry_resolutions = registry_resolutions,
        registry_hits = registry_hits,
        registry_misses = registry_misses,
        registry_evictions = registry_evictions,
        registry_resident_payload_bytes = registry_resident_payload_bytes,
        epoch = epoch,
        verification = verification,
        candidate_id = candidate_id,
        MAINNET_USDC_MINT_BASE58 = MAINNET_USDC_MINT_BASE58,
        selected_blocks = selected_blocks,
        blocks = blocks,
        transactions = transactions,
        source_instructions = source_instructions,
        retained_accounts = retained_accounts,
        committed_pubkey_cache_entries = committed_pubkey_cache_entries,
        incomplete_instructions = incomplete_instructions,
        incomplete_cpi = incomplete_cpi,
        unknown_execution = unknown_execution,
        instructions_not_requested = instructions_not_requested,
        instructions_with_unknown_data = instructions_with_unknown_data,
        setup_seconds = setup_seconds,
        pipeline_seconds = pipeline_seconds,
        coordinator_outside_database_seconds = coordinator_outside_database_seconds,
        database_seconds = database_seconds,
        checkpoint_seconds = checkpoint_seconds,
        pipeline_plus_checkpoint_seconds = pipeline_plus_checkpoint_seconds,
        total_seconds = total_seconds,
        audit_seconds = audit_seconds,
        db_source_validation_digest_seconds = db_source_validation_digest_seconds,
        db_transaction_setup_seconds = db_transaction_setup_seconds,
        db_token_tracking_seconds = db_token_tracking_seconds,
        db_tracked_row_write_seconds = db_tracked_row_write_seconds,
        db_block_header_write_seconds = db_block_header_write_seconds,
        db_durable_digest_checkpoint_seconds = db_durable_digest_checkpoint_seconds,
        db_commit_seconds = db_commit_seconds,
        db_error_recovery_seconds = db_error_recovery_seconds,
        bound_source_size_bytes = bound_source_size_bytes,
        scan_source_read_calls = scan_source_read_calls,
        source_read_bytes = source_read_bytes,
        decoded_bytes = decoded_bytes,
        setup_local_read_calls = setup_local_read_calls,
        scan_local_read_calls = scan_local_read_calls,
        total_local_read_calls = total_local_read_calls,
        setup_local_read_bytes = setup_local_read_bytes,
        scan_local_read_bytes = scan_local_read_bytes,
        total_local_read_bytes = total_local_read_bytes,
        output_path = output_path,
        TOKEN_EVENT_SCHEMA_VERSION = TOKEN_EVENT_SCHEMA_VERSION,
        output_bytes = output_bytes,
        next_block_ordinal = next_block_ordinal,
        ledger_digest = ledger_digest,
        tracker_digest = tracker_digest,
    );
}

fn decimal_mb_s(bytes: u64, seconds: f64) -> f64 {
    bytes as f64 / 1_000_000.0 / seconds.max(f64::MIN_POSITIVE)
}

fn registry_mode_name(mode: IndexerV3RegistryReadMode) -> &'static str {
    match mode {
        IndexerV3RegistryReadMode::Unused => "unused",
        IndexerV3RegistryReadMode::SparseChunkCache => "sparse-chunk-cache",
        IndexerV3RegistryReadMode::FullRegistry => "full-registry",
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn text(value: Option<OsString>, usage: &str) -> Result<String, Box<dyn Error>> {
    value
        .ok_or_else(|| usage.to_owned().into())
        .and_then(|value| {
            value
                .into_string()
                .map_err(|_| "argument is not UTF-8".into())
        })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    fn parse(values: &[&str]) -> Result<Arguments, Box<dyn Error>> {
        arguments_from(values.iter().map(OsString::from))
    }

    fn valid_arguments() -> [&'static str; 9] {
        [
            "local-split",
            "/archive/v3/epoch-900",
            "/archive/retained/epoch-900",
            "900",
            "epoch-900-corrected",
            "/private/output/events.sqlite",
            "1024",
            "--threads",
            "12",
        ]
    }

    #[test]
    fn parses_the_bounded_local_contract() {
        let arguments = parse(&valid_arguments()).unwrap();
        assert_eq!(arguments.epoch, 900);
        assert_eq!(arguments.max_blocks.get(), 1_024);
        assert_eq!(arguments.threads.get(), 12);
        assert_eq!(arguments.output, Path::new("/private/output/events.sqlite"));
    }

    #[test]
    fn rejects_a_network_source() {
        let mut values = valid_arguments();
        values[0] = "https://archive.example";
        let error = parse(&values).unwrap_err();
        assert!(error.to_string().contains("only a local V3 source"));
    }

    #[test]
    fn requires_the_explicit_block_limit() {
        let error = parse(&[
            "local-split",
            "/archive/v3/epoch-900",
            "/archive/retained/epoch-900",
            "900",
            "candidate",
            "/private/output/events.sqlite",
            "--threads",
            "1",
        ])
        .unwrap_err();
        assert!(error.to_string().contains("usage:"));
    }

    #[test]
    fn caps_the_sqlite_correctness_run() {
        let mut values = valid_arguments();
        values[6] = "10001";
        let error = parse(&values).unwrap_err();
        assert!(error.to_string().contains("1..=10000"));
    }

    #[test]
    fn rejects_zero_or_excessive_parallel_workers() {
        for threads in ["0", "65"] {
            let mut values = valid_arguments();
            values[8] = threads;
            let error = parse(&values).unwrap_err();
            assert!(error.to_string().contains("threads must be"));
        }
    }

    #[test]
    fn refuses_an_existing_sqlite_sidecar_before_archive_setup() {
        let unique = format!(
            "blockzilla-v3-usdc-test-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed),
        );
        let directory = std::env::temp_dir().join(unique);
        fs::create_dir(&directory).unwrap();
        let output = directory.join("events.sqlite");
        let sidecar = with_suffix(&output, "-wal");
        fs::write(&sidecar, b"existing").unwrap();

        let error = validate_new_output(&output).unwrap_err();
        assert!(error.to_string().contains("refusing to replace"));
        assert_eq!(fs::read(&sidecar).unwrap(), b"existing");

        fs::remove_file(sidecar).unwrap();
        fs::remove_dir(directory).unwrap();
    }
}
