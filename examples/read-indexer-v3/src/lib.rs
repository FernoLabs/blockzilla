//! Small shared command-line and measurement support for the Indexer V3 examples.

use std::{
    error::Error,
    ffi::OsString,
    fmt::Display,
    io::Write,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    time::Instant,
};

use blockzilla_example_workloads::{
    CoverageReport, FinishedOutput, FirewatchReport, OutputReport, PumpReport, UsdcReport,
};
use blockzilla_indexer_v3_read_sdk::{
    IndexerV3Archive, IndexerV3ParallelScanReceipt, IndexerV3ParallelScanStats,
    IndexerV3RegistryReadMode, IndexerV3RegistryReadReceipt, IndexerV3TargetedScanReceipt,
    IndexerV3TransportReceipt, MAX_INDEXER_V3_PARALLEL_WORKERS, ScanIoReceipt, ScanRequest,
    default_worker_count,
};

pub const DEFAULT_PUBLIC_ORIGIN: &str =
    "https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev";
pub const DEFAULT_SAMPLE_EPOCH: u64 = 900;
pub const DEFAULT_FIREWATCH_WALLET: &str = "5LikTUsx695BHRipWoRrn6YmTQEcPrvbR8YaHxdSRQo8";
pub const SAMPLE_EPOCHS: [u64; 11] = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadProfile {
    OrderedFullScan,
    Selective,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub source: Source,
    pub epoch: u64,
    pub target: Option<String>,
    pub output: PathBuf,
    pub max_blocks: Option<NonZeroU32>,
    pub threads: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    Network {
        origin: String,
        cache_root: PathBuf,
    },
    LocalSplit {
        ledger_root: PathBuf,
        retained_root: PathBuf,
        candidate_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadArguments {
    pub source: WorkloadSource,
    pub epoch: u64,
    pub target: Option<String>,
    pub output: PathBuf,
    pub threads: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CountArguments {
    pub source: WorkloadSource,
    pub epoch: u64,
    pub threads: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BeginnerArguments {
    source: WorkloadSource,
    epoch: u64,
    target: Option<String>,
    output: Option<PathBuf>,
    threads: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadSource {
    Network { origin: String, cache_root: PathBuf },
    LocalArchive { archive_root: PathBuf },
}

/// Parse the beginner flags. With no arguments this scans all of public epoch
/// 900 and writes `default_output` in the current directory.
pub fn workload_arguments(
    binary: &str,
    default_output: &str,
) -> Result<WorkloadArguments, Box<dyn Error>> {
    workload_arguments_from(
        binary,
        None,
        None,
        default_output,
        std::env::args_os().skip(1),
    )
}

pub fn workload_target_arguments(
    binary: &str,
    target_name: &str,
    default_target: &str,
    default_output: &str,
) -> Result<WorkloadArguments, Box<dyn Error>> {
    workload_arguments_from(
        binary,
        Some(target_name),
        Some(default_target),
        default_output,
        std::env::args_os().skip(1),
    )
}

pub fn workload_arguments_from(
    binary: &str,
    target_name: Option<&str>,
    default_target: Option<&str>,
    default_output: &str,
    values: impl IntoIterator<Item = OsString>,
) -> Result<WorkloadArguments, Box<dyn Error>> {
    let parsed = flag_arguments(
        binary,
        target_name,
        default_target,
        Some(default_output),
        values.into_iter().collect(),
    )?;
    Ok(WorkloadArguments {
        source: parsed.source,
        epoch: parsed.epoch,
        target: parsed.target,
        output: parsed.output.ok_or("the workload output path is missing")?,
        threads: parsed.threads,
    })
}

/// Parse the no-output full-epoch count example flags.
pub fn count_arguments(binary: &str) -> Result<CountArguments, Box<dyn Error>> {
    count_arguments_from(binary, std::env::args_os().skip(1))
}

pub fn count_arguments_from(
    binary: &str,
    values: impl IntoIterator<Item = OsString>,
) -> Result<CountArguments, Box<dyn Error>> {
    let parsed = flag_arguments(binary, None, None, None, values.into_iter().collect())?;
    Ok(CountArguments {
        source: parsed.source,
        epoch: parsed.epoch,
        threads: parsed.threads,
    })
}

fn flag_arguments(
    binary: &str,
    target_name: Option<&str>,
    default_target: Option<&str>,
    default_output: Option<&str>,
    values: Vec<OsString>,
) -> Result<BeginnerArguments, Box<dyn Error>> {
    let target_usage = target_name.map_or(String::new(), |name| format!(" [--{name} KEY]"));
    let output_usage = default_output.map_or("", |_| " [--output FILE]");
    let usage = format!(
        "usage: {binary} [--epoch N] [--archive-root DIR | --origin URL] [--cache-root DIR]{output_usage} [--threads N]{target_usage}\n\nSample epochs: 0, 100, 200, ..., 1000. The complete epoch is always scanned."
    );
    let mut epoch = DEFAULT_SAMPLE_EPOCH;
    let mut origin = DEFAULT_PUBLIC_ORIGIN.to_owned();
    let mut origin_was_set = false;
    let mut archive_root = None;
    let mut cache_root = PathBuf::from("archive-cache/indexer-v3");
    let mut cache_was_set = false;
    let mut target = default_target.map(str::to_owned);
    let mut output = default_output.map(PathBuf::from);
    let mut threads = default_worker_count();
    let target_flag = target_name.map(|name| format!("--{name}"));
    let mut values = values.into_iter();

    while let Some(value) = values.next() {
        let flag = text(Some(value), &usage)?;
        let next = |values: &mut std::vec::IntoIter<OsString>| text(values.next(), &usage);
        match flag.as_str() {
            "--epoch" => epoch = parse("epoch", &next(&mut values)?)?,
            "--origin" => {
                origin = next(&mut values)?;
                origin_was_set = true;
            }
            "--archive-root" => archive_root = Some(PathBuf::from(next(&mut values)?)),
            "--cache-root" => {
                cache_root = PathBuf::from(next(&mut values)?);
                cache_was_set = true;
            }
            "--output" if default_output.is_some() => {
                output = Some(PathBuf::from(next(&mut values)?));
            }
            "--threads" => {
                let count = parse("threads", &next(&mut values)?)?;
                threads = NonZeroUsize::new(count)
                    .ok_or_else(|| "threads must be greater than zero".to_owned())?;
                if threads.get() > MAX_INDEXER_V3_PARALLEL_WORKERS {
                    return Err(format!(
                        "threads must not exceed {MAX_INDEXER_V3_PARALLEL_WORKERS}"
                    )
                    .into());
                }
            }
            flag if target_flag.as_deref() == Some(flag) => target = Some(next(&mut values)?),
            "-h" | "--help" => return Err(usage.into()),
            _ => return Err(format!("unknown option {flag:?}\n\n{usage}").into()),
        }
    }

    if !SAMPLE_EPOCHS.contains(&epoch) {
        return Err(
            format!("epoch {epoch} is not in the public sample set: 0, 100, ..., 1000").into(),
        );
    }
    let source = match archive_root {
        Some(archive_root) => {
            if origin_was_set || cache_was_set {
                return Err(
                    "--archive-root cannot be combined with --origin or --cache-root".into(),
                );
            }
            WorkloadSource::LocalArchive { archive_root }
        }
        None => WorkloadSource::Network { origin, cache_root },
    };
    Ok(BeginnerArguments {
        source,
        epoch,
        target,
        output,
        threads,
    })
}

pub fn arguments(binary: &str) -> Result<Arguments, Box<dyn Error>> {
    arguments_from(binary, None, std::env::args_os().skip(1))
}

pub fn target_arguments(binary: &str, target_name: &str) -> Result<Arguments, Box<dyn Error>> {
    arguments_from(binary, Some(target_name), std::env::args_os().skip(1))
}

pub fn arguments_from(
    binary: &str,
    target_name: Option<&str>,
    values: impl IntoIterator<Item = OsString>,
) -> Result<Arguments, Box<dyn Error>> {
    let target_usage = target_name.map_or(String::new(), |name| format!(" <{name}>"));
    let usage = format!(
        "usage:\n  {binary} <worker-origin> <epoch> <cache-root>{target_usage} <output-file> [max-blocks] [--threads N]\n  {binary} local-split <ledger-root> <retained-root> <epoch> <candidate-id>{target_usage} <output-file> [max-blocks] [--threads N]"
    );
    let mut values = values.into_iter();
    let first = text(values.next(), &usage)?;
    let (source, epoch) = if first == "local-split" {
        let ledger_root = PathBuf::from(values.next().ok_or_else(|| usage.clone())?);
        let retained_root = PathBuf::from(values.next().ok_or_else(|| usage.clone())?);
        let epoch = parse("epoch", &text(values.next(), &usage)?)?;
        let candidate_id = text(values.next(), &usage)?;
        if candidate_id.is_empty() || candidate_id.chars().any(char::is_whitespace) {
            return Err("candidate-id must be one nonempty token".into());
        }
        (
            Source::LocalSplit {
                ledger_root,
                retained_root,
                candidate_id,
            },
            epoch,
        )
    } else {
        let epoch = parse("epoch", &text(values.next(), &usage)?)?;
        let cache_root = PathBuf::from(values.next().ok_or_else(|| usage.clone())?);
        (
            Source::Network {
                origin: first,
                cache_root,
            },
            epoch,
        )
    };
    let target = target_name
        .map(|_| text(values.next(), &usage))
        .transpose()?;
    let output = PathBuf::from(values.next().ok_or_else(|| usage.clone())?);
    let mut trailing = values
        .map(|value| text(Some(value), &usage))
        .collect::<Result<Vec<_>, _>>()?;
    let max_blocks = if trailing.first().is_some_and(|value| value != "--threads") {
        let value = trailing.remove(0);
        let parsed = parse("max-blocks", &value)?;
        Some(
            NonZeroU32::new(parsed)
                .ok_or_else(|| "max-blocks must be greater than zero".to_owned())?,
        )
    } else {
        None
    };
    let threads = match trailing.as_slice() {
        [] => default_worker_count(),
        [flag, value] if flag == "--threads" => {
            let parsed = parse("threads", value)?;
            let threads = NonZeroUsize::new(parsed)
                .ok_or_else(|| "threads must be greater than zero".to_owned())?;
            if threads.get() > MAX_INDEXER_V3_PARALLEL_WORKERS {
                return Err(
                    format!("threads must not exceed {MAX_INDEXER_V3_PARALLEL_WORKERS}").into(),
                );
            }
            threads
        }
        _ => return Err(usage.into()),
    };
    Ok(Arguments {
        source,
        epoch,
        target,
        output,
        max_blocks,
        threads,
    })
}

pub fn open_archive(
    arguments: &Arguments,
    profile: ReadProfile,
) -> blockzilla_indexer_v3_read_sdk::Result<IndexerV3Archive> {
    match &arguments.source {
        Source::Network { origin, cache_root } => match profile {
            ReadProfile::OrderedFullScan => {
                IndexerV3Archive::open(origin, arguments.epoch, cache_root)
            }
            ReadProfile::Selective => {
                IndexerV3Archive::open_selective(origin, arguments.epoch, cache_root)
            }
        },
        Source::LocalSplit {
            ledger_root,
            retained_root,
            candidate_id,
        } => IndexerV3Archive::open_local_split(
            ledger_root,
            retained_root,
            arguments.epoch,
            candidate_id,
        ),
    }
}

pub fn scan_request(
    archive: &IndexerV3Archive,
    max_blocks: Option<NonZeroU32>,
) -> blockzilla_indexer_v3_read_sdk::Result<ScanRequest> {
    max_blocks.map_or_else(
        || Ok(ScanRequest::all()),
        |count| Ok(ScanRequest::bounded(archive.bounded_range(0, count)?)),
    )
}

/// Timing and I/O state that is not part of the tutorial scan flow.
pub struct RunTiming {
    total_started: Instant,
    setup_seconds: f64,
    bound_source_size_bytes: u64,
    setup_io: IndexerV3TransportReceipt,
}

impl RunTiming {
    pub fn after_open(total_started: Instant, archive: &IndexerV3Archive) -> Self {
        Self {
            total_started,
            setup_seconds: total_started.elapsed().as_secs_f64(),
            bound_source_size_bytes: archive.bound_source_size_bytes(),
            setup_io: archive.transport_snapshot(),
        }
    }
}

/// Finish a full-epoch count and print the same timing and I/O units as the
/// workload examples.
pub fn finish_count(
    arguments: &CountArguments,
    archive: IndexerV3Archive,
    timing: RunTiming,
    parallel: IndexerV3ParallelScanReceipt,
    scan_seconds: f64,
    recorded_inner_instructions: u64,
) -> Result<(), Box<dyn Error>> {
    let receipt = parallel.scan;
    let total_io = finish_archive(archive)?;
    let total_seconds = timing.total_started.elapsed().as_secs_f64();
    let scan_seconds_nonzero = scan_seconds.max(f64::MIN_POSITIVE);
    let total_seconds_nonzero = total_seconds.max(f64::MIN_POSITIVE);
    let scan_io = subtract_transport(total_io, timing.setup_io);
    let setup_http = timing.setup_io.http_and_cache;
    let scan_http = scan_io.http_and_cache;
    let total_http = total_io.http_and_cache;
    let logical_bytes = receipt.io.source_read_bytes.unwrap_or(0);

    println!(
        "format=indexer-v3 workload=slot-hours scan_kind=ordered-full-scan epoch={} source={} threads={} requested_workers={} effective_workers={} max_active_workers={} parallel_jobs={} projected_blocks={} registry_mode={} registry_prefetch_read_calls={} registry_prefetch_read_bytes={} blocks={} transactions={} instructions={} recorded_inner_instructions={} transactions_with_incomplete_instructions={} transactions_with_incomplete_cpi={} setup_s={:.6} scan_s={:.6} total_s={:.6} scan_tps={:.3} total_tps={:.3} bound_source_size_bytes={} scan_logical_read_calls={} scan_logical_read_bytes={} scan_logical_read_mb_s={:.6} setup_network_bytes={} scan_network_bytes={} total_network_bytes={} scan_network_mb_s={:.6} total_network_mb_s={:.6} setup_cache_read_bytes={} scan_cache_read_bytes={} total_cache_read_bytes={} setup_local_read_calls={} scan_local_read_calls={} total_local_read_calls={} setup_local_read_bytes={} scan_local_read_bytes={} total_local_read_bytes={} scan_local_read_mb_s={:.6} total_local_read_mb_s={:.6}",
        arguments.epoch,
        workload_source_name(&arguments.source),
        arguments.threads,
        parallel.parallel.requested_workers,
        parallel.parallel.effective_workers,
        parallel.parallel.max_active_workers,
        parallel.parallel.jobs,
        parallel.parallel.projected_blocks,
        registry_mode_name(parallel.registry.mode),
        parallel.registry.prefetch_read_calls,
        parallel.registry.prefetch_read_bytes,
        receipt.blocks,
        receipt.transactions,
        receipt.instructions,
        recorded_inner_instructions,
        receipt.transactions_with_incomplete_instructions,
        receipt.transactions_with_incomplete_cpi,
        timing.setup_seconds,
        scan_seconds,
        total_seconds,
        receipt.transactions as f64 / scan_seconds_nonzero,
        receipt.transactions as f64 / total_seconds_nonzero,
        timing.bound_source_size_bytes,
        receipt.io.source_read_calls.unwrap_or(0),
        logical_bytes,
        decimal_mb_s(logical_bytes, scan_seconds_nonzero),
        setup_http.network_body_bytes,
        scan_http.network_body_bytes,
        total_http.network_body_bytes,
        decimal_mb_s(scan_http.network_body_bytes, scan_seconds_nonzero),
        decimal_mb_s(total_http.network_body_bytes, total_seconds_nonzero),
        setup_http.cache_read_bytes,
        scan_http.cache_read_bytes,
        total_http.cache_read_bytes,
        timing.setup_io.local_read_calls,
        scan_io.local_read_calls,
        total_io.local_read_calls,
        timing.setup_io.local_read_bytes,
        scan_io.local_read_bytes,
        total_io.local_read_bytes,
        decimal_mb_s(scan_io.local_read_bytes, scan_seconds_nonzero),
        decimal_mb_s(total_io.local_read_bytes, total_seconds_nonzero),
    );
    Ok(())
}

/// Counters common to the three small real-world workload reports.
pub trait ExampleReport {
    fn workload(&self) -> &'static str;
    fn common(&self) -> (u64, u64, OutputReport, CoverageReport, bool);
    fn print_details(&self);
}

impl ExampleReport for UsdcReport {
    fn workload(&self) -> &'static str {
        "usdc-recorded-balances"
    }

    fn common(&self) -> (u64, u64, OutputReport, CoverageReport, bool) {
        (
            self.blocks_seen,
            self.transactions_seen,
            self.output,
            self.coverage,
            self.output_complete,
        )
    }

    fn print_details(&self) {
        println!(
            "workload={} matching_transactions={} pre_rows={} post_rows={} token_balances_unavailable_transactions={} token_mint_unavailable_transactions={}",
            self.workload(),
            self.matching_transactions,
            self.pre_rows,
            self.post_rows,
            self.token_balances_unavailable_transactions,
            self.token_mint_unavailable_transactions,
        );
    }
}

impl ExampleReport for PumpReport {
    fn workload(&self) -> &'static str {
        "pumpfun-transactions"
    }

    fn common(&self) -> (u64, u64, OutputReport, CoverageReport, bool) {
        (
            self.blocks_seen,
            self.transactions_seen,
            self.output,
            self.coverage,
            self.output_complete,
        )
    }

    fn print_details(&self) {
        println!(
            "workload={} matching_transactions={} written_transactions={} direct_invocations={} cpi_invocations={} incomplete_instruction_transactions={} incomplete_cpi_transactions={} matches_without_primary_signature={}",
            self.workload(),
            self.matching_transactions,
            self.written_transactions,
            self.direct_invocations,
            self.cpi_invocations,
            self.incomplete_instruction_transactions,
            self.incomplete_cpi_transactions,
            self.matches_without_primary_signature,
        );
    }
}

impl ExampleReport for FirewatchReport {
    fn workload(&self) -> &'static str {
        "firewatch-wallet-programs"
    }

    fn common(&self) -> (u64, u64, OutputReport, CoverageReport, bool) {
        (
            self.blocks_seen,
            self.transactions_seen,
            self.output,
            self.coverage,
            self.output_complete,
        )
    }

    fn print_details(&self) {
        println!(
            "workload={} signer_transactions={} successful_signer_transactions={} failed_signer_transactions={} unknown_execution_signer_transactions={} reached_instructions={} distinct_programs={} incomplete_instruction_transactions={} incomplete_cpi_transactions={}",
            self.workload(),
            self.signer_transactions,
            self.successful_signer_transactions,
            self.failed_signer_transactions,
            self.unknown_execution_signer_transactions,
            self.reached_instructions,
            self.distinct_programs,
            self.incomplete_instruction_transactions,
            self.incomplete_cpi_transactions,
        );
    }
}

pub fn finish_ordered_workload<W: Write, R: ExampleReport>(
    arguments: &WorkloadArguments,
    archive: IndexerV3Archive,
    timing: RunTiming,
    parallel: IndexerV3ParallelScanReceipt,
    scan_seconds: f64,
    finished: FinishedOutput<W, R>,
) -> Result<(), Box<dyn Error>> {
    let receipt = parallel.scan;
    finish_workload(
        arguments,
        archive,
        timing,
        WorkloadScan {
            counts: WorkCounts {
                requested_blocks: receipt.blocks,
                requested_transactions: receipt.transactions,
                candidate_blocks: receipt.blocks,
                candidate_transactions: receipt.transactions,
                skipped_blocks: 0,
                skipped_transactions: 0,
                decoded_blocks: receipt.blocks,
                decoded_transactions: receipt.transactions,
            },
            source_io: receipt.io,
            parallel: parallel.parallel,
            registry: parallel.registry,
        },
        "ordered-full-scan",
        scan_seconds,
        finished,
    )
}

pub fn finish_targeted_workload<W: Write, R: ExampleReport>(
    arguments: &WorkloadArguments,
    archive: IndexerV3Archive,
    timing: RunTiming,
    targeted: IndexerV3TargetedScanReceipt,
    scan_seconds: f64,
    finished: FinishedOutput<W, R>,
) -> Result<(), Box<dyn Error>> {
    let scan = targeted.scan;
    let parallel = scan
        .parallel
        .ok_or("the targeted parallel scan did not return parallel statistics")?;
    finish_workload(
        arguments,
        archive,
        timing,
        WorkloadScan {
            counts: WorkCounts {
                requested_blocks: scan.requested_blocks,
                requested_transactions: scan.requested_transactions,
                candidate_blocks: scan.candidate_blocks,
                candidate_transactions: scan.candidate_transactions,
                skipped_blocks: scan.skipped_blocks,
                skipped_transactions: scan.skipped_transactions,
                decoded_blocks: scan.scan_receipt.blocks,
                decoded_transactions: scan.scan_receipt.transactions,
            },
            source_io: scan.source_io,
            parallel,
            registry: scan.registry,
        },
        "reverse-index-candidates",
        scan_seconds,
        finished,
    )
}

struct WorkloadScan {
    counts: WorkCounts,
    source_io: ScanIoReceipt,
    parallel: IndexerV3ParallelScanStats,
    registry: IndexerV3RegistryReadReceipt,
}

fn finish_workload<W: Write, R: ExampleReport>(
    arguments: &WorkloadArguments,
    archive: IndexerV3Archive,
    timing: RunTiming,
    scan: WorkloadScan,
    scan_kind: &'static str,
    scan_seconds: f64,
    finished: FinishedOutput<W, R>,
) -> Result<(), Box<dyn Error>> {
    let (mut writer, report) = finished.into_parts();
    writer.flush()?;
    let (blocks_seen, transactions_seen, output, coverage, output_complete) = report.common();
    validate_workload_counts(scan.counts, blocks_seen, transactions_seen)?;
    let total_io = finish_archive(archive)?;
    let total_seconds = timing.total_started.elapsed().as_secs_f64();
    print_run(RunReport {
        workload: report.workload(),
        scan_kind,
        arguments,
        counts: scan.counts,
        source_io: scan.source_io,
        setup_seconds: timing.setup_seconds,
        scan_seconds,
        total_seconds,
        bound_source_size_bytes: timing.bound_source_size_bytes,
        setup_io: timing.setup_io,
        total_io,
        output,
        coverage,
        output_complete,
        parallel: scan.parallel,
        registry: scan.registry,
    });
    report.print_details();
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkCounts {
    pub requested_blocks: u64,
    pub requested_transactions: u64,
    pub candidate_blocks: u64,
    pub candidate_transactions: u64,
    pub skipped_blocks: u64,
    pub skipped_transactions: u64,
    pub decoded_blocks: u64,
    pub decoded_transactions: u64,
}

pub struct RunReport<'a> {
    pub workload: &'static str,
    pub scan_kind: &'static str,
    pub arguments: &'a WorkloadArguments,
    pub counts: WorkCounts,
    pub source_io: ScanIoReceipt,
    pub setup_seconds: f64,
    pub scan_seconds: f64,
    pub total_seconds: f64,
    pub bound_source_size_bytes: u64,
    pub setup_io: IndexerV3TransportReceipt,
    pub total_io: IndexerV3TransportReceipt,
    pub output: OutputReport,
    pub coverage: CoverageReport,
    pub output_complete: bool,
    pub parallel: IndexerV3ParallelScanStats,
    pub registry: IndexerV3RegistryReadReceipt,
}

pub fn print_run(report: RunReport<'_>) {
    let scan_seconds = report.scan_seconds.max(f64::MIN_POSITIVE);
    let total_seconds = report.total_seconds.max(f64::MIN_POSITIVE);
    let scan_io = subtract_transport(report.total_io, report.setup_io);
    let setup_http = report.setup_io.http_and_cache;
    let scan_http = scan_io.http_and_cache;
    let total_http = report.total_io.http_and_cache;
    let logical_bytes = report.source_io.source_read_bytes.unwrap_or(0);
    println!(
        "format=indexer-v3 workload={} scan_kind={} epoch={} source={} threads={} requested_workers={} effective_workers={} max_active_workers={} parallel_jobs={} projected_blocks={} blocks_per_job_limit={} job_window_limit={} max_in_flight_jobs={} max_coordinator_pending_results={} max_result_channel_backlog={} max_coordinator_pending_projected_blocks={} declared_decoded_byte_limit={} transaction_limit={} max_in_flight_declared_decoded_bytes={} max_in_flight_transactions={} max_owned_payload_block_bytes={} max_in_flight_owned_payload_bytes={} global_projected_block_bound={} registry_mode={} registry_prefetch_read_calls={} registry_prefetch_read_bytes={} registry_resolutions={} registry_resident_payload_bytes={} requested_blocks={} requested_transactions={} candidate_blocks={} candidate_transactions={} skipped_blocks={} skipped_transactions={} decoded_blocks={} decoded_transactions={} setup_s={:.6} scan_s={:.6} total_s={:.6} scan_tps={:.3} total_tps={:.3} decoded_scan_tps={:.3} decoded_total_tps={:.3} bound_source_size_bytes={} scan_logical_read_calls={} scan_logical_read_bytes={} scan_logical_read_mb_s={:.6} setup_network_bytes={} scan_network_bytes={} total_network_bytes={} scan_network_mb_s={:.6} total_network_mb_s={:.6} setup_cache_read_bytes={} scan_cache_read_bytes={} total_cache_read_bytes={} setup_local_read_calls={} scan_local_read_calls={} total_local_read_calls={} setup_local_read_bytes={} scan_local_read_bytes={} total_local_read_bytes={} scan_local_read_mb_s={:.6} total_local_read_mb_s={:.6} output_path={} output_schema={} output_rows={} output_bytes={} output_complete={} indeterminate_transactions={} coverage_sha256={}",
        report.workload,
        report.scan_kind,
        report.arguments.epoch,
        workload_source_name(&report.arguments.source),
        report.arguments.threads,
        report.parallel.requested_workers,
        report.parallel.effective_workers,
        report.parallel.max_active_workers,
        report.parallel.jobs,
        report.parallel.projected_blocks,
        report.parallel.blocks_per_job_limit,
        report.parallel.job_window_limit,
        report.parallel.max_in_flight_jobs,
        report.parallel.max_coordinator_pending_results,
        report.parallel.max_result_channel_backlog,
        report.parallel.max_coordinator_pending_projected_blocks,
        report.parallel.declared_decoded_byte_limit,
        report.parallel.transaction_limit,
        report.parallel.max_in_flight_declared_decoded_bytes,
        report.parallel.max_in_flight_transactions,
        report.parallel.max_owned_payload_block_bytes,
        report.parallel.max_in_flight_owned_payload_bytes,
        report.parallel.global_projected_block_bound,
        registry_mode_name(report.registry.mode),
        report.registry.prefetch_read_calls,
        report.registry.prefetch_read_bytes,
        report.registry.resolutions,
        report.registry.resident_payload_bytes,
        report.counts.requested_blocks,
        report.counts.requested_transactions,
        report.counts.candidate_blocks,
        report.counts.candidate_transactions,
        report.counts.skipped_blocks,
        report.counts.skipped_transactions,
        report.counts.decoded_blocks,
        report.counts.decoded_transactions,
        report.setup_seconds,
        report.scan_seconds,
        report.total_seconds,
        report.counts.requested_transactions as f64 / scan_seconds,
        report.counts.requested_transactions as f64 / total_seconds,
        report.counts.decoded_transactions as f64 / scan_seconds,
        report.counts.decoded_transactions as f64 / total_seconds,
        report.bound_source_size_bytes,
        report.source_io.source_read_calls.unwrap_or(0),
        logical_bytes,
        decimal_mb_s(logical_bytes, scan_seconds),
        setup_http.network_body_bytes,
        scan_http.network_body_bytes,
        total_http.network_body_bytes,
        decimal_mb_s(scan_http.network_body_bytes, scan_seconds),
        decimal_mb_s(total_http.network_body_bytes, total_seconds),
        setup_http.cache_read_bytes,
        scan_http.cache_read_bytes,
        total_http.cache_read_bytes,
        report.setup_io.local_read_calls,
        scan_io.local_read_calls,
        report.total_io.local_read_calls,
        report.setup_io.local_read_bytes,
        scan_io.local_read_bytes,
        report.total_io.local_read_bytes,
        decimal_mb_s(scan_io.local_read_bytes, scan_seconds),
        decimal_mb_s(report.total_io.local_read_bytes, total_seconds),
        report.arguments.output.display(),
        report.output.schema,
        report.output.row_count,
        report.output.output_bytes,
        report.output_complete,
        report.coverage.indeterminate_transactions,
        report.coverage.sha256_hex(),
    );
}

pub fn finish_archive(
    archive: IndexerV3Archive,
) -> blockzilla_indexer_v3_read_sdk::Result<IndexerV3TransportReceipt> {
    archive.verify_local_unchanged()?;
    Ok(archive.finish_transport_io())
}

pub fn validate_workload_counts(
    counts: WorkCounts,
    blocks_seen: u64,
    transactions_seen: u64,
) -> Result<(), Box<dyn Error>> {
    if (counts.decoded_blocks, counts.decoded_transactions) != (blocks_seen, transactions_seen) {
        return Err(format!(
            "workload saw {blocks_seen} blocks and {transactions_seen} transactions, but the SDK decoded {} blocks and {} transactions",
            counts.decoded_blocks, counts.decoded_transactions
        )
        .into());
    }
    Ok(())
}

pub fn output_file(path: &Path) -> Result<std::io::BufWriter<std::fs::File>, Box<dyn Error>> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    Ok(std::io::BufWriter::with_capacity(1 << 20, file))
}

pub fn parse_pubkey(name: &str, value: &str) -> Result<[u8; 32], Box<dyn Error>> {
    let bytes = bs58::decode(value).into_vec()?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        format!("{name} decodes to {} bytes, expected 32", bytes.len()).into()
    })
}

fn subtract_transport(
    later: IndexerV3TransportReceipt,
    earlier: IndexerV3TransportReceipt,
) -> IndexerV3TransportReceipt {
    debug_assert_eq!(later.kind, earlier.kind);
    IndexerV3TransportReceipt {
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

fn workload_source_name(source: &WorkloadSource) -> &'static str {
    match source {
        WorkloadSource::Network { .. } => "network",
        WorkloadSource::LocalArchive { .. } => "local",
    }
}

fn registry_mode_name(mode: IndexerV3RegistryReadMode) -> &'static str {
    match mode {
        IndexerV3RegistryReadMode::Unused => "unused",
        IndexerV3RegistryReadMode::SparseChunkCache => "sparse-chunk-cache",
        IndexerV3RegistryReadMode::FullRegistry => "full-registry",
    }
}

fn decimal_mb_s(bytes: u64, seconds: f64) -> f64 {
    bytes as f64 / 1_000_000.0 / seconds.max(f64::MIN_POSITIVE)
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

fn parse<T>(name: &str, value: &str) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Display + Send + Sync + 'static,
{
    value
        .parse()
        .map_err(|error| format!("invalid {name} {value:?}: {error}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn beginner_defaults_to_the_complete_public_epoch_900() {
        let parsed = workload_arguments_from(
            "read-indexer-v3-usdc",
            None,
            None,
            "indexer-v3-usdc.bin",
            [],
        )
        .unwrap();

        assert_eq!(parsed.epoch, 900);
        assert_eq!(parsed.output, PathBuf::from("indexer-v3-usdc.bin"));
        assert!(matches!(
            parsed.source,
            WorkloadSource::Network { ref origin, .. } if origin == DEFAULT_PUBLIC_ORIGIN
        ));
    }

    #[test]
    fn beginner_accepts_the_public_tree_layout_and_no_block_limit() {
        let parsed = workload_arguments_from(
            "read-indexer-v3-usdc",
            None,
            None,
            "indexer-v3-usdc.bin",
            ["--epoch", "1000", "--archive-root", "archive"]
                .into_iter()
                .map(OsString::from),
        )
        .unwrap();
        assert!(matches!(
            parsed.source,
            WorkloadSource::LocalArchive { ref archive_root }
                if archive_root == Path::new("archive")
        ));

        for forbidden in ["--blocks", "--all"] {
            assert!(
                workload_arguments_from(
                    "read-indexer-v3-usdc",
                    None,
                    None,
                    "indexer-v3-usdc.bin",
                    [OsString::from(forbidden)],
                )
                .is_err()
            );
        }
    }

    #[test]
    fn slot_hour_reader_has_no_output_or_block_option() {
        let parsed = count_arguments_from(
            "read-indexer-v3-slot-hours",
            ["--archive-root", "archive", "--epoch", "100"]
                .into_iter()
                .map(OsString::from),
        )
        .unwrap();
        assert_eq!(parsed.epoch, 100);
        assert!(matches!(
            parsed.source,
            WorkloadSource::LocalArchive { ref archive_root }
                if archive_root == Path::new("archive")
        ));

        for forbidden in ["--blocks", "--all", "--output"] {
            assert!(
                count_arguments_from(
                    "read-indexer-v3-slot-hours",
                    [OsString::from(forbidden), OsString::from("value")],
                )
                .is_err()
            );
        }

        let help = count_arguments_from("read-indexer-v3-slot-hours", [OsString::from("--help")])
            .unwrap_err()
            .to_string();
        assert!(!help.contains("--output"));
    }

    #[test]
    fn parses_targeted_local_split_run() {
        let parsed = arguments_from(
            "read-indexer-v3-firewatch",
            Some("wallet"),
            [
                "local-split",
                "/v3",
                "/retained",
                "900",
                "candidate-1",
                "11111111111111111111111111111111",
                "/tmp/out.bin",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(parsed.epoch, 900);
        assert_eq!(parsed.max_blocks, None);
        assert!(parsed.target.is_some());
        assert!(matches!(parsed.source, Source::LocalSplit { .. }));
    }

    #[test]
    fn parses_positional_max_blocks_before_explicit_threads() {
        let parsed = arguments_from(
            "read-indexer-v3-usdc",
            None,
            [
                "https://archive.example",
                "900",
                "/tmp/cache",
                "/tmp/out.bin",
                "10000",
                "--threads",
                "12",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();

        assert_eq!(parsed.max_blocks, NonZeroU32::new(10_000));
        assert_eq!(parsed.threads, NonZeroUsize::new(12).unwrap());
    }

    #[test]
    fn rejects_more_than_the_sdk_worker_limit() {
        let error = arguments_from(
            "read-indexer-v3-usdc",
            None,
            [
                "https://archive.example",
                "900",
                "/tmp/cache",
                "/tmp/out.bin",
                "--threads",
                "65",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap_err();

        assert!(error.to_string().contains("must not exceed 64"));
    }
}
