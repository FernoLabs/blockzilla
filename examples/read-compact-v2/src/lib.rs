//! Small shared command-line and measurement support for the Compact V2 examples.

use std::{
    error::Error,
    ffi::OsString,
    fmt::Display,
    io::Write,
    num::NonZeroU32,
    path::{Path, PathBuf},
    time::Instant,
};

use blockzilla_compact_v2_read_sdk::{
    CompactV2Archive, CompactV2LocalDescriptor, CompactV2ParallelScanConfig,
    CompactV2ParallelScanReceipt, CompactV2TransportReceipt, MAX_COMPACT_V2_PARALLEL_WORKERS,
    ScanReceipt, ScanRequest,
};
use blockzilla_example_workloads::{
    CoverageReport, FinishedOutput, FirewatchReport, OutputReport, PumpReport, UsdcReport,
};

pub const DEFAULT_ORIGIN: &str =
    "https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev";
pub const DEFAULT_EPOCH: u64 = 900;
pub const DEFAULT_FIREWATCH_WALLET: &str = "5LikTUsx695BHRipWoRrn6YmTQEcPrvbR8YaHxdSRQo8";
pub const SAMPLE_EPOCHS: [u64; 11] = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1_000];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub source: Source,
    pub epoch: u64,
    pub target: Option<String>,
    pub output: PathBuf,
    pub max_blocks: Option<NonZeroU32>,
    pub threads: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CountArguments {
    pub source: Source,
    pub epoch: u64,
    pub threads: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    Network {
        origin: String,
        cache_root: PathBuf,
    },
    Local {
        epoch_root: PathBuf,
        candidate_id: String,
    },
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
    flag_arguments(binary, target_name, true, values.into_iter().collect())
}

pub fn count_arguments(binary: &str) -> Result<CountArguments, Box<dyn Error>> {
    count_arguments_from(binary, std::env::args_os().skip(1))
}

pub fn count_arguments_from(
    binary: &str,
    values: impl IntoIterator<Item = OsString>,
) -> Result<CountArguments, Box<dyn Error>> {
    let arguments = flag_arguments(binary, None, false, values.into_iter().collect())?;
    Ok(CountArguments {
        source: arguments.source,
        epoch: arguments.epoch,
        threads: arguments.threads,
    })
}

/// Positional form retained only for the transaction exporter and matrix tools.
pub fn positional_arguments(binary: &str) -> Result<Arguments, Box<dyn Error>> {
    positional_arguments_from(binary, None, std::env::args_os().skip(1))
}

pub fn positional_arguments_from(
    binary: &str,
    target_name: Option<&str>,
    values: impl IntoIterator<Item = OsString>,
) -> Result<Arguments, Box<dyn Error>> {
    parse_positional_arguments(binary, target_name, values.into_iter().collect())
}

fn flag_arguments(
    binary: &str,
    target_name: Option<&str>,
    allow_output: bool,
    values: Vec<OsString>,
) -> Result<Arguments, Box<dyn Error>> {
    let usage = flag_usage(binary, target_name, allow_output);
    let mut epoch = DEFAULT_EPOCH;
    let mut origin = DEFAULT_ORIGIN.to_owned();
    let mut origin_was_set = false;
    let mut archive_root = None;
    let mut cache_root = None;
    let mut output = None;
    let mut threads = CompactV2ParallelScanConfig::default().workers;
    let mut target = target_name.map(|_| DEFAULT_FIREWATCH_WALLET.to_owned());
    let mut values = values.into_iter();

    while let Some(flag) = values.next() {
        let flag = text(Some(flag), &usage)?;
        match flag.as_str() {
            "--epoch" => epoch = parse("epoch", &text(values.next(), &usage)?)?,
            "--origin" => {
                if origin_was_set {
                    return Err("--origin was provided more than once".into());
                }
                origin_was_set = true;
                origin = text(values.next(), &usage)?;
            }
            "--archive-root" => {
                if archive_root.is_some() {
                    return Err("--archive-root was provided more than once".into());
                }
                archive_root = Some(PathBuf::from(values.next().ok_or_else(|| usage.clone())?));
            }
            "--cache-root" => {
                if cache_root.is_some() {
                    return Err("--cache-root was provided more than once".into());
                }
                cache_root = Some(PathBuf::from(values.next().ok_or_else(|| usage.clone())?));
            }
            "--output" if allow_output => {
                if output.is_some() {
                    return Err("--output was provided more than once".into());
                }
                output = Some(PathBuf::from(values.next().ok_or_else(|| usage.clone())?));
            }
            "--threads" => {
                threads = parse("threads", &text(values.next(), &usage)?)?;
                validate_threads(threads)?;
            }
            "--wallet" if target_name == Some("wallet") => {
                target = Some(text(values.next(), &usage)?);
            }
            "--help" | "-h" => return Err(usage.into()),
            _ => return Err(format!("unknown option {flag:?}\n{usage}").into()),
        }
    }

    validate_sample_epoch(epoch)?;
    if archive_root.is_some() && origin_was_set {
        return Err("use either --origin or --archive-root, not both".into());
    }
    if archive_root.is_some() && cache_root.is_some() {
        return Err("--cache-root is only used with a network source".into());
    }
    let output = output.unwrap_or_else(|| default_output(binary, epoch));
    let source = if let Some(root) = archive_root {
        let epoch_root = absolute_path(root)?
            .join("compact-v2")
            .join(epoch.to_string());
        Source::Local {
            epoch_root,
            candidate_id: format!("sample-layout-epoch-{epoch}"),
        }
    } else {
        Source::Network {
            origin,
            cache_root: absolute_path(
                cache_root.unwrap_or_else(|| PathBuf::from(".blockzilla-cache")),
            )?,
        }
    };
    Ok(Arguments {
        source,
        epoch,
        target,
        output,
        max_blocks: None,
        threads,
    })
}

fn parse_positional_arguments(
    binary: &str,
    target_name: Option<&str>,
    mut values: Vec<OsString>,
) -> Result<Arguments, Box<dyn Error>> {
    let target_usage = target_name.map_or(String::new(), |name| format!(" <{name}>"));
    let usage = format!(
        "usage:\n  {binary} <origin> <epoch> <cache-root>{target_usage} <output> [max-blocks] [--threads N]\n  {binary} local <files-root> <epoch> <candidate-id>{target_usage} <output> [max-blocks] [--threads N]"
    );
    let mut threads = CompactV2ParallelScanConfig::default().workers;
    if values.len() >= 2 && values[values.len() - 2] == "--threads" {
        threads = parse("threads", &text(values.pop(), &usage)?)?;
        values.pop();
        validate_threads(threads)?;
    }
    let mut values = values.into_iter();
    let first = text(values.next(), &usage)?;
    let (source, epoch) = if first == "local" {
        let epoch_root = absolute_path(PathBuf::from(values.next().ok_or_else(|| usage.clone())?))?;
        let epoch = parse("epoch", &text(values.next(), &usage)?)?;
        let candidate_id = text(values.next(), &usage)?;
        validate_candidate_id(&candidate_id)?;
        (
            Source::Local {
                epoch_root,
                candidate_id,
            },
            epoch,
        )
    } else {
        let epoch = parse("epoch", &text(values.next(), &usage)?)?;
        let cache_root = absolute_path(PathBuf::from(values.next().ok_or_else(|| usage.clone())?))?;
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
    let max_blocks = values
        .next()
        .map(|value| positive_blocks(&text(Some(value), &usage)?))
        .transpose()?;
    if values.next().is_some() {
        return Err(usage.into());
    }
    Ok(Arguments {
        source,
        epoch,
        target,
        output,
        max_blocks,
        threads,
    })
}

fn flag_usage(binary: &str, target_name: Option<&str>, allow_output: bool) -> String {
    let wallet = target_name.map_or("", |_| " --wallet PUBKEY");
    let output = if allow_output { " [--output FILE]" } else { "" };
    format!(
        "usage: {binary} [--epoch EPOCH] [--origin URL | --archive-root DIR] [--cache-root DIR]{output} [--threads N]{wallet}"
    )
}

fn default_output(binary: &str, epoch: u64) -> PathBuf {
    PathBuf::from(format!("{binary}-epoch-{epoch}.bin"))
}

fn absolute_path(path: PathBuf) -> Result<PathBuf, Box<dyn Error>> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn validate_sample_epoch(epoch: u64) -> Result<(), Box<dyn Error>> {
    if SAMPLE_EPOCHS.contains(&epoch) {
        Ok(())
    } else {
        Err(format!("epoch {epoch} is not in the public sample set: 0, 100, 200, ..., 1000").into())
    }
}

fn validate_threads(threads: usize) -> Result<(), Box<dyn Error>> {
    if (1..=MAX_COMPACT_V2_PARALLEL_WORKERS).contains(&threads) {
        Ok(())
    } else {
        Err(format!("threads must be in 1..={MAX_COMPACT_V2_PARALLEL_WORKERS}").into())
    }
}

fn validate_candidate_id(candidate_id: &str) -> Result<(), Box<dyn Error>> {
    if candidate_id.is_empty() || candidate_id.chars().any(char::is_whitespace) {
        Err("candidate-id must be one nonempty token".into())
    } else {
        Ok(())
    }
}

fn positive_blocks(value: &str) -> Result<NonZeroU32, Box<dyn Error>> {
    let parsed = parse("max-blocks", value)?;
    NonZeroU32::new(parsed).ok_or_else(|| "max-blocks must be greater than zero".into())
}

pub fn open_archive(
    arguments: &Arguments,
) -> blockzilla_compact_v2_read_sdk::Result<CompactV2Archive> {
    match &arguments.source {
        Source::Network { origin, cache_root } => {
            CompactV2Archive::open(origin, arguments.epoch, cache_root)
        }
        Source::Local {
            epoch_root,
            candidate_id,
        } => {
            let descriptor = CompactV2LocalDescriptor::mainnet(arguments.epoch, candidate_id)?;
            CompactV2Archive::open_local(epoch_root, descriptor)
        }
    }
}

pub fn scan_request(
    archive: &CompactV2Archive,
    max_blocks: Option<NonZeroU32>,
) -> blockzilla_compact_v2_read_sdk::Result<ScanRequest> {
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
    setup_io: CompactV2TransportReceipt,
}

impl RunTiming {
    pub fn after_open(total_started: Instant, archive: &CompactV2Archive) -> Self {
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
    archive: CompactV2Archive,
    timing: RunTiming,
    parallel: CompactV2ParallelScanReceipt,
    scan_seconds: f64,
    recorded_inner_instructions: u64,
) -> Result<(), Box<dyn Error>> {
    let receipt = parallel.scan;
    let total_io = finish_archive(archive)?;
    let total_seconds = timing.total_started.elapsed().as_secs_f64();
    let scan_seconds_nonzero = scan_seconds.max(f64::MIN_POSITIVE);
    let total_seconds_nonzero = total_seconds.max(f64::MIN_POSITIVE);
    let scan_io = total_io.saturating_sub(timing.setup_io);
    let setup_http = timing.setup_io.http_and_cache;
    let scan_http = scan_io.http_and_cache;
    let total_http = total_io.http_and_cache;
    let logical_bytes = receipt.io.source_read_bytes.unwrap_or(0);

    println!(
        "format=compact-v2 workload=slot-hours epoch={} source={} threads={} requested_workers={} effective_workers={} max_active_workers={} blocks={} transactions={} instructions={} recorded_inner_instructions={} transactions_with_incomplete_instructions={} transactions_with_incomplete_cpi={} setup_s={:.6} scan_s={:.6} total_s={:.6} scan_tps={:.3} total_tps={:.3} bound_source_size_bytes={} scan_logical_read_calls={} scan_logical_read_bytes={} scan_logical_read_mb_s={:.6} setup_network_bytes={} scan_network_bytes={} total_network_bytes={} scan_network_mb_s={:.6} total_network_mb_s={:.6} setup_cache_read_bytes={} scan_cache_read_bytes={} total_cache_read_bytes={} setup_local_read_calls={} scan_local_read_calls={} total_local_read_calls={} setup_local_read_bytes={} scan_local_read_bytes={} total_local_read_bytes={} scan_local_read_mb_s={:.6} total_local_read_mb_s={:.6}",
        arguments.epoch,
        source_name(&arguments.source),
        arguments.threads,
        parallel.requested_workers,
        parallel.effective_workers,
        parallel.max_active_workers,
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

/// Print the detailed counters outside the small format-specific examples.
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

pub fn finish_workload<W: Write, R: ExampleReport>(
    arguments: &Arguments,
    archive: CompactV2Archive,
    timing: RunTiming,
    parallel: CompactV2ParallelScanReceipt,
    scan_seconds: f64,
    finished: FinishedOutput<W, R>,
) -> Result<(), Box<dyn Error>> {
    let (mut writer, report) = finished.into_parts();
    writer.flush()?;
    let receipt = parallel.scan;
    let (blocks_seen, transactions_seen, output, coverage, output_complete) = report.common();
    validate_workload_counts(&receipt, blocks_seen, transactions_seen)?;
    let total_io = finish_archive(archive)?;
    let total_seconds = timing.total_started.elapsed().as_secs_f64();
    print_run(RunReport {
        workload: report.workload(),
        arguments,
        receipt,
        parallel,
        setup_seconds: timing.setup_seconds,
        scan_seconds,
        total_seconds,
        bound_source_size_bytes: timing.bound_source_size_bytes,
        setup_io: timing.setup_io,
        total_io,
        output,
        coverage,
        output_complete,
    });
    report.print_details();
    Ok(())
}

pub struct RunReport<'a> {
    pub workload: &'static str,
    pub arguments: &'a Arguments,
    pub receipt: ScanReceipt,
    pub parallel: CompactV2ParallelScanReceipt,
    pub setup_seconds: f64,
    pub scan_seconds: f64,
    pub total_seconds: f64,
    pub bound_source_size_bytes: u64,
    pub setup_io: CompactV2TransportReceipt,
    pub total_io: CompactV2TransportReceipt,
    pub output: OutputReport,
    pub coverage: CoverageReport,
    pub output_complete: bool,
}

pub fn print_run(report: RunReport<'_>) {
    debug_assert_eq!(report.arguments.threads, report.parallel.requested_workers);
    let scan_seconds = report.scan_seconds.max(f64::MIN_POSITIVE);
    let total_seconds = report.total_seconds.max(f64::MIN_POSITIVE);
    let scan_io = report.total_io.saturating_sub(report.setup_io);
    let setup_http = report.setup_io.http_and_cache;
    let scan_http = scan_io.http_and_cache;
    let total_http = report.total_io.http_and_cache;
    let logical_bytes = report.receipt.io.source_read_bytes.unwrap_or(0);
    println!(
        "format=compact-v2 workload={} epoch={} source={} threads={} requested_workers={} effective_workers={} max_active_workers={} max_batch_blocks={} max_batch_transactions={} max_projected_block_bytes={} max_projected_batch_bytes={} registry_mode={} registry_prefetch_read_calls={} registry_prefetch_read_bytes={} registry_resident_bound_bytes={} blocks={} transactions={} setup_s={:.6} scan_s={:.6} total_s={:.6} scan_tps={:.3} total_tps={:.3} bound_source_size_bytes={} scan_logical_read_calls={} scan_logical_read_bytes={} scan_logical_read_mb_s={:.6} setup_network_bytes={} scan_network_bytes={} total_network_bytes={} scan_network_mb_s={:.6} total_network_mb_s={:.6} setup_cache_read_bytes={} scan_cache_read_bytes={} total_cache_read_bytes={} setup_local_read_calls={} scan_local_read_calls={} total_local_read_calls={} setup_local_read_bytes={} scan_local_read_bytes={} total_local_read_bytes={} scan_local_read_mb_s={:.6} total_local_read_mb_s={:.6} output_path={} output_schema={} output_rows={} output_bytes={} output_complete={} indeterminate_transactions={} coverage_sha256={}",
        report.workload,
        report.arguments.epoch,
        source_name(&report.arguments.source),
        report.arguments.threads,
        report.parallel.requested_workers,
        report.parallel.effective_workers,
        report.parallel.max_active_workers,
        report.parallel.pipeline.max_blocks_per_batch,
        report.parallel.pipeline.max_transactions_per_batch,
        report.parallel.max_projected_block_bytes,
        report.parallel.max_projected_batch_bytes,
        report.parallel.registry.mode,
        report.parallel.registry.prefetch_read_calls,
        report.parallel.registry.prefetch_read_bytes,
        report.parallel.registry.resident_bound_bytes,
        report.receipt.blocks,
        report.receipt.transactions,
        report.setup_seconds,
        report.scan_seconds,
        report.total_seconds,
        report.receipt.transactions as f64 / scan_seconds,
        report.receipt.transactions as f64 / total_seconds,
        report.bound_source_size_bytes,
        report.receipt.io.source_read_calls.unwrap_or(0),
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
    archive: CompactV2Archive,
) -> blockzilla_compact_v2_read_sdk::Result<CompactV2TransportReceipt> {
    archive.verify_local_unchanged()?;
    Ok(archive.finish_transport_io())
}

pub fn validate_workload_counts(
    receipt: &ScanReceipt,
    blocks_seen: u64,
    transactions_seen: u64,
) -> Result<(), Box<dyn Error>> {
    if (receipt.blocks, receipt.transactions) != (blocks_seen, transactions_seen) {
        return Err(format!(
            "workload saw {blocks_seen} blocks and {transactions_seen} transactions, but the SDK receipt has {} blocks and {} transactions",
            receipt.blocks, receipt.transactions
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
    Ok(std::io::BufWriter::new(file))
}

fn source_name(source: &Source) -> &'static str {
    match source {
        Source::Network { .. } => "network",
        Source::Local { .. } => "local",
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

pub fn parse_pubkey(name: &str, value: &str) -> Result<[u8; 32], Box<dyn Error>> {
    let bytes = bs58::decode(value).into_vec()?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        format!("{name} decodes to {} bytes, expected 32", bytes.len()).into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn beginner_defaults_select_the_full_public_epoch() {
        let parsed = arguments_from("read-compact-v2-usdc", None, std::iter::empty()).unwrap();
        assert_eq!(parsed.epoch, 900);
        assert!(parsed.max_blocks.is_none());
        assert_eq!(
            parsed.output,
            PathBuf::from("read-compact-v2-usdc-epoch-900.bin")
        );
        let Source::Network { origin, .. } = parsed.source else {
            panic!("expected network source");
        };
        assert_eq!(origin, DEFAULT_ORIGIN);

        let firewatch = arguments_from(
            "read-compact-v2-firewatch",
            Some("wallet"),
            std::iter::empty(),
        )
        .unwrap();
        assert_eq!(firewatch.target.as_deref(), Some(DEFAULT_FIREWATCH_WALLET));
    }

    #[test]
    fn local_mode_follows_the_public_key_layout() {
        let parsed = arguments_from(
            "read-compact-v2-firewatch",
            Some("wallet"),
            [
                "--archive-root",
                "/archive",
                "--epoch",
                "900",
                "--wallet",
                "11111111111111111111111111111111",
                "--threads",
                "12",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(parsed.threads, 12);
        assert!(parsed.max_blocks.is_none());
        let Source::Local { epoch_root, .. } = parsed.source else {
            panic!("expected local source");
        };
        assert_eq!(epoch_root, PathBuf::from("/archive/compact-v2/900"));
    }

    #[test]
    fn every_published_sample_epoch_is_accepted() {
        for epoch in SAMPLE_EPOCHS {
            let parsed = arguments_from(
                "read-compact-v2-usdc",
                None,
                ["--epoch", &epoch.to_string()]
                    .into_iter()
                    .map(OsString::from),
            )
            .unwrap();
            assert_eq!(parsed.epoch, epoch);
        }
    }

    #[test]
    fn beginner_cli_has_no_block_limit_or_positional_form() {
        for values in [
            vec!["--blocks", "1"],
            vec!["--all"],
            vec!["https://archive.test", "900", "/tmp/cache", "/tmp/out"],
        ] {
            assert!(
                arguments_from(
                    "read-compact-v2-usdc",
                    None,
                    values.into_iter().map(OsString::from)
                )
                .is_err()
            );
        }
    }

    #[test]
    fn rejects_invalid_source_and_thread_combinations() {
        for values in [
            vec![
                "--origin",
                "https://archive.test",
                "--archive-root",
                "/archive",
            ],
            vec!["--archive-root", "/archive", "--cache-root", "/tmp/cache"],
            vec!["--threads", "0"],
            vec!["--threads", "65"],
            vec!["--epoch", "42"],
        ] {
            assert!(
                arguments_from(
                    "read-compact-v2-usdc",
                    None,
                    values.into_iter().map(OsString::from)
                )
                .is_err()
            );
        }
    }

    #[test]
    fn slot_hour_reader_has_no_output_or_block_option() {
        let parsed = count_arguments_from(
            "read-compact-v2-slot-hours",
            [
                "--archive-root",
                "/archive",
                "--epoch",
                "100",
                "--threads",
                "12",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(parsed.epoch, 100);
        assert_eq!(parsed.threads, 12);
        let Source::Local { epoch_root, .. } = parsed.source else {
            panic!("expected local source");
        };
        assert_eq!(epoch_root, PathBuf::from("/archive/compact-v2/100"));

        for values in [
            vec!["--output", "/tmp/counts.bin"],
            vec!["--blocks", "1"],
            vec!["--all"],
        ] {
            assert!(
                count_arguments_from(
                    "read-compact-v2-slot-hours",
                    values.into_iter().map(OsString::from)
                )
                .is_err()
            );
        }

        let help = count_arguments_from("read-compact-v2-slot-hours", [OsString::from("--help")])
            .unwrap_err()
            .to_string();
        assert!(!help.contains("--output"));
        assert!(help.contains("--threads"));
    }

    #[test]
    fn transaction_exporter_positional_form_remains_separate() {
        let parsed = positional_arguments_from(
            "read-compact-v2-transactions",
            None,
            [
                "local",
                "/archive/files",
                "900",
                "candidate-1",
                "/tmp/out.bin",
                "10000",
                "--threads",
                "12",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(parsed.max_blocks.map(NonZeroU32::get), Some(10_000));
        assert_eq!(parsed.threads, 12);
    }
}
