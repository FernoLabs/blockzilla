//! Small shared command-line and report helpers for the CAR examples.
//!
//! Archive opening and application scans stay in each binary. This module only
//! keeps command-line parsing and the common measurement fields in one place.

use std::{
    error::Error,
    ffi::OsString,
    fmt::{self, Display, Formatter},
    fs::{File, OpenOptions},
    io::{self, BufWriter},
    num::NonZeroU32,
    path::PathBuf,
    str::FromStr,
};

use blockzilla_car_read_sdk::{
    ArchiveIoSnapshot, CarArchiveHttpProfile, ScanIoReceipt, ScanReceipt, SourceVerification,
};
use blockzilla_example_workloads::{CoverageReport, OutputReport};

pub const DEFAULT_PUBLIC_ORIGIN: &str =
    "https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev";
pub const DEFAULT_SAMPLE_EPOCH: u64 = 900;
pub const DEFAULT_FIREWATCH_WALLET: &str = "5LikTUsx695BHRipWoRrn6YmTQEcPrvbR8YaHxdSRQo8";
pub const SAMPLE_EPOCH_BLOCK_COUNTS: [(u64, u32); 11] = [
    (0, 431_548),
    (100, 402_076),
    (200, 318_235),
    (300, 408_989),
    (400, 392_412),
    (500, 421_310),
    (600, 371_273),
    (700, 420_895),
    (800, 430_282),
    (900, 431_858),
    (1_000, 431_781),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Worker,
    OldFaithful,
    OldFaithfulOperatorTrusted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskArguments {
    pub route: Route,
    pub origin: String,
    pub epoch: u64,
    pub expected_blocks: NonZeroU32,
    pub output: PathBuf,
    pub max_blocks: Option<NonZeroU32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetTaskArguments {
    pub source: TaskArguments,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadSource {
    Network { route: Route, origin: String },
    LocalArchive { archive_root: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadArguments {
    pub source: WorkloadSource,
    pub epoch: u64,
    pub expected_blocks: NonZeroU32,
    pub target: Option<String>,
    pub output: PathBuf,
    /// Present only when the hidden legacy positional form requests a smoke
    /// prefix. The normal flag interface always scans the complete epoch.
    pub max_blocks: Option<NonZeroU32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliError(String);

impl CliError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl Display for CliError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for CliError {}

/// Parse the simple full-epoch workload flags.
///
/// With no arguments, this selects the clean public Worker layout, epoch 900,
/// and `default_output`. The old positional form remains accepted for existing
/// benchmark runners, but it is not part of the beginner interface.
pub fn workload_arguments(
    binary: &str,
    default_output: &str,
) -> Result<WorkloadArguments, CliError> {
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
) -> Result<WorkloadArguments, CliError> {
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
) -> Result<WorkloadArguments, CliError> {
    let values = values.into_iter().collect::<Vec<_>>();
    if values
        .first()
        .and_then(|value| value.to_str())
        .is_some_and(|value| {
            matches!(
                value,
                "worker" | "old-faithful" | "old-faithful-operator-trusted"
            )
        })
    {
        return legacy_workload_arguments(target_name, values);
    }
    flag_workload_arguments(binary, target_name, default_target, default_output, values)
}

fn flag_workload_arguments(
    binary: &str,
    target_name: Option<&str>,
    default_target: Option<&str>,
    default_output: &str,
    values: Vec<OsString>,
) -> Result<WorkloadArguments, CliError> {
    let target_usage = target_name.map_or(String::new(), |name| format!(" [--{name} KEY]"));
    let usage = format!(
        "usage: {binary} [--epoch N] [--origin URL | --archive-root DIR] [--output FILE]{target_usage}\n\nSample epochs: 0, 100, 200, ..., 1000. The complete epoch is always scanned."
    );
    let mut epoch = DEFAULT_SAMPLE_EPOCH;
    let mut epoch_was_set = false;
    let mut origin = DEFAULT_PUBLIC_ORIGIN.to_owned();
    let mut origin_was_set = false;
    let mut archive_root = None;
    let mut output = PathBuf::from(default_output);
    let mut output_was_set = false;
    let mut target = default_target.map(str::to_owned);
    let mut target_was_set = false;
    let target_flag = target_name.map(|name| format!("--{name}"));
    let mut values = values.into_iter();

    while let Some(value) = values.next() {
        let flag = os_text(value, &usage)?;
        let next_text = |values: &mut std::vec::IntoIter<OsString>| {
            values
                .next()
                .ok_or_else(|| CliError::new(usage.clone()))
                .and_then(|value| os_text(value, &usage))
        };
        match flag.as_str() {
            "--epoch" => {
                if epoch_was_set {
                    return Err(CliError::new("--epoch was provided more than once"));
                }
                epoch_was_set = true;
                epoch = parse_number("epoch", &next_text(&mut values)?)?;
            }
            "--origin" => {
                if origin_was_set {
                    return Err(CliError::new("--origin was provided more than once"));
                }
                origin_was_set = true;
                origin = next_text(&mut values)?;
            }
            "--archive-root" => {
                if archive_root.is_some() {
                    return Err(CliError::new("--archive-root was provided more than once"));
                }
                archive_root = Some(PathBuf::from(
                    values.next().ok_or_else(|| CliError::new(usage.clone()))?,
                ));
            }
            "--output" => {
                if output_was_set {
                    return Err(CliError::new("--output was provided more than once"));
                }
                output_was_set = true;
                output = PathBuf::from(values.next().ok_or_else(|| CliError::new(usage.clone()))?);
            }
            flag if target_flag.as_deref() == Some(flag) => {
                if target_was_set {
                    return Err(CliError::new(format!(
                        "--{} was provided more than once",
                        target_name.expect("a target flag exists")
                    )));
                }
                target_was_set = true;
                target = Some(next_text(&mut values)?);
            }
            "--help" | "-h" => return Err(CliError::new(usage)),
            _ => {
                return Err(CliError::new(format!("unknown option {flag:?}\n\n{usage}")));
            }
        }
    }

    if archive_root.is_some() && origin_was_set {
        return Err(CliError::new(
            "use either --origin or --archive-root, not both",
        ));
    }
    let expected_blocks = sample_block_count(epoch)?;
    let source = archive_root.map_or_else(
        || WorkloadSource::Network {
            route: Route::Worker,
            origin,
        },
        |archive_root| WorkloadSource::LocalArchive { archive_root },
    );
    Ok(WorkloadArguments {
        source,
        epoch,
        expected_blocks,
        target,
        output,
        max_blocks: None,
    })
}

fn legacy_workload_arguments(
    target_name: Option<&str>,
    values: Vec<OsString>,
) -> Result<WorkloadArguments, CliError> {
    let values = values
        .into_iter()
        .map(|value| {
            value
                .into_string()
                .map_err(|_| CliError::new("legacy positional arguments must be UTF-8"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let usage = "legacy CAR workload arguments are invalid";
    if target_name.is_some() {
        let parsed = target_task_arguments(values, usage)?;
        Ok(WorkloadArguments {
            source: WorkloadSource::Network {
                route: parsed.source.route,
                origin: parsed.source.origin,
            },
            epoch: parsed.source.epoch,
            expected_blocks: parsed.source.expected_blocks,
            target: Some(parsed.target),
            output: parsed.source.output,
            max_blocks: parsed.source.max_blocks,
        })
    } else {
        let parsed = task_arguments(values, usage)?;
        Ok(WorkloadArguments {
            source: WorkloadSource::Network {
                route: parsed.route,
                origin: parsed.origin,
            },
            epoch: parsed.epoch,
            expected_blocks: parsed.expected_blocks,
            target: None,
            output: parsed.output,
            max_blocks: parsed.max_blocks,
        })
    }
}

fn sample_block_count(epoch: u64) -> Result<NonZeroU32, CliError> {
    SAMPLE_EPOCH_BLOCK_COUNTS
        .iter()
        .find_map(|&(candidate, blocks)| (candidate == epoch).then_some(blocks))
        .and_then(NonZeroU32::new)
        .ok_or_else(|| {
            CliError::new(format!(
                "epoch {epoch} is not in the public sample set: 0, 100, ..., 1000"
            ))
        })
}

fn os_text(value: OsString, usage: &str) -> Result<String, CliError> {
    value
        .into_string()
        .map_err(|_| CliError::new(format!("an option must be UTF-8\n\n{usage}")))
}

pub fn task_arguments(
    values: impl IntoIterator<Item = String>,
    usage: &'static str,
) -> Result<TaskArguments, CliError> {
    let mut values = values.into_iter();
    let route = parse_route(values.next(), usage)?;
    let origin = required(values.next(), usage)?;
    let epoch = parse_number("epoch", &required(values.next(), usage)?)?;
    let expected_blocks = parse_nonzero("canonical-block-count", &required(values.next(), usage)?)?;
    let output = PathBuf::from(required(values.next(), usage)?);
    let max_blocks = values
        .next()
        .map(|value| parse_nonzero("max-blocks", &value))
        .transpose()?;
    if values.next().is_some() {
        return Err(CliError::new(usage));
    }
    Ok(TaskArguments {
        route,
        origin,
        epoch,
        expected_blocks,
        output,
        max_blocks,
    })
}

pub fn target_task_arguments(
    values: impl IntoIterator<Item = String>,
    usage: &'static str,
) -> Result<TargetTaskArguments, CliError> {
    let mut values = values.into_iter();
    let route = parse_route(values.next(), usage)?;
    let origin = required(values.next(), usage)?;
    let epoch = parse_number("epoch", &required(values.next(), usage)?)?;
    let expected_blocks = parse_nonzero("canonical-block-count", &required(values.next(), usage)?)?;
    let target = required(values.next(), usage)?;
    let output = PathBuf::from(required(values.next(), usage)?);
    let max_blocks = values
        .next()
        .map(|value| parse_nonzero("max-blocks", &value))
        .transpose()?;
    if values.next().is_some() {
        return Err(CliError::new(usage));
    }
    Ok(TargetTaskArguments {
        source: TaskArguments {
            route,
            origin,
            epoch,
            expected_blocks,
            output,
            max_blocks,
        },
        target,
    })
}

fn parse_route(value: Option<String>, usage: &'static str) -> Result<Route, CliError> {
    match value.as_deref() {
        Some("worker") => Ok(Route::Worker),
        Some("old-faithful") => Ok(Route::OldFaithful),
        Some("old-faithful-operator-trusted") => Ok(Route::OldFaithfulOperatorTrusted),
        _ => Err(CliError::new(usage)),
    }
}

fn required(value: Option<String>, usage: &'static str) -> Result<String, CliError> {
    value.ok_or_else(|| CliError::new(usage))
}

fn parse_number<T>(name: &str, value: &str) -> Result<T, CliError>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .parse()
        .map_err(|error| CliError::new(format!("invalid {name}: {error}")))
}

fn parse_nonzero(name: &str, value: &str) -> Result<NonZeroU32, CliError> {
    NonZeroU32::new(parse_number(name, value)?)
        .ok_or_else(|| CliError::new(format!("{name} must be greater than zero")))
}

pub fn parse_pubkey(value: &str) -> Result<[u8; 32], CliError> {
    let bytes = bs58::decode(value)
        .into_vec()
        .map_err(|error| CliError::new(format!("invalid wallet public key: {error}")))?;
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        CliError::new(format!(
            "wallet public key has {} bytes; expected 32",
            bytes.len()
        ))
    })
}

/// Create a new output file. An existing file is never replaced by accident.
pub fn create_output(path: &PathBuf) -> io::Result<BufWriter<File>> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    Ok(BufWriter::new(file))
}

pub fn validate_workload_counts(
    receipt: &ScanReceipt,
    blocks_seen: u64,
    transactions_seen: u64,
) -> Result<(), CliError> {
    if (receipt.blocks, receipt.transactions) != (blocks_seen, transactions_seen) {
        return Err(CliError::new(format!(
            "workload saw {blocks_seen} blocks and {transactions_seen} transactions, but the SDK receipt has {} blocks and {} transactions",
            receipt.blocks, receipt.transactions
        )));
    }
    Ok(())
}

pub struct RunFacts {
    pub epoch: u64,
    pub verification: SourceVerification,
    pub requested_blocks: u32,
    pub bound_source_size_bytes: u64,
    pub http_profile: Option<CarArchiveHttpProfile>,
    pub receipt: ScanReceipt,
    pub setup_seconds: f64,
    pub scan_seconds: f64,
    pub total_seconds: f64,
    pub setup_io: ArchiveIoSnapshot,
    pub scan_io: ArchiveIoSnapshot,
    pub total_io: ArchiveIoSnapshot,
}

impl Display for RunFacts {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let scan_seconds = self.scan_seconds.max(f64::MIN_POSITIVE);
        let total_seconds = self.total_seconds.max(f64::MIN_POSITIVE);
        let (
            http_verification,
            http_object_binding,
            http_workers,
            http_window_chunks,
            http_chunk_bytes,
            http_body_window_bytes,
        ) = self.http_profile.map_or(
            ("not-applicable", "not-applicable", 0, 0, 0, 0),
            |profile| {
                (
                    match profile.verification {
                        blockzilla_car_read_sdk::CarArchiveHttpVerification::StrongEtag => {
                            "strong-etag"
                        }
                        blockzilla_car_read_sdk::CarArchiveHttpVerification::OperatorTrusted => {
                            "operator-trusted"
                        }
                    },
                    profile.verification.object_binding_kind(),
                    profile.workers,
                    profile.window_chunks,
                    profile.chunk_bytes,
                    profile.body_window_bytes,
                )
            },
        );
        write!(
            formatter,
            "format=car epoch={} verification={} requested_blocks={} bound_source_size_bytes={} http_verification={} http_object_binding={} http_content_hash=none http_workers={} http_window_chunks={} http_chunk_bytes={} http_body_window_bytes={} blocks={} transactions={} instructions={} setup_s={:.6} scan_s={:.6} total_s={:.6} scan_tps={:.3} total_tps={:.3}",
            self.epoch,
            self.verification,
            self.requested_blocks,
            self.bound_source_size_bytes,
            http_verification,
            http_object_binding,
            http_workers,
            http_window_chunks,
            http_chunk_bytes,
            http_body_window_bytes,
            self.receipt.blocks,
            self.receipt.transactions,
            self.receipt.instructions,
            self.setup_seconds,
            self.scan_seconds,
            self.total_seconds,
            self.receipt.transactions as f64 / scan_seconds,
            self.receipt.transactions as f64 / total_seconds,
        )?;
        write_scan_io(formatter, self.receipt.io, scan_seconds)?;
        write!(
            formatter,
            " setup_head_requests={} scan_head_requests={} total_head_requests={} setup_get_requests={} scan_get_requests={} total_get_requests={} setup_network_bytes={} scan_network_bytes={} total_network_bytes={} scan_network_mb_s={:.6} total_network_mb_s={:.6} setup_cache_bytes={} scan_cache_bytes={} total_cache_bytes={}",
            self.setup_io.head_requests,
            self.scan_io.head_requests,
            self.total_io.head_requests,
            self.setup_io.get_requests,
            self.scan_io.get_requests,
            self.total_io.get_requests,
            self.setup_io.network_body_bytes,
            self.scan_io.network_body_bytes,
            self.total_io.network_body_bytes,
            decimal_mb_s(self.scan_io.network_body_bytes, scan_seconds),
            decimal_mb_s(self.total_io.network_body_bytes, total_seconds),
            self.setup_io.cache_read_bytes,
            self.scan_io.cache_read_bytes,
            self.total_io.cache_read_bytes,
        )
    }
}

fn write_scan_io(
    formatter: &mut Formatter<'_>,
    io: ScanIoReceipt,
    scan_seconds: f64,
) -> fmt::Result {
    write_optional(formatter, "source_read_calls", io.source_read_calls)?;
    write_optional(formatter, "source_read_bytes", io.source_read_bytes)?;
    if let Some(bytes) = io.source_read_bytes {
        write!(
            formatter,
            " scan_source_mb_s={:.6}",
            decimal_mb_s(bytes, scan_seconds)
        )?;
    } else {
        formatter.write_str(" scan_source_mb_s=unavailable")?;
    }
    write_optional(formatter, "decoded_bytes", io.decoded_bytes)
}

fn write_optional(formatter: &mut Formatter<'_>, label: &str, value: Option<u64>) -> fmt::Result {
    match value {
        Some(value) => write!(formatter, " {label}={value}"),
        None => write!(formatter, " {label}=unavailable"),
    }
}

fn decimal_mb_s(bytes: u64, seconds: f64) -> f64 {
    bytes as f64 / 1_000_000.0 / seconds.max(f64::MIN_POSITIVE)
}

pub struct OutputFacts<'a> {
    pub workload: &'static str,
    pub output_complete: bool,
    pub output: &'a OutputReport,
    pub coverage: &'a CoverageReport,
}

impl Display for OutputFacts<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "workload={} output_schema={} output_rows={} output_bytes={} output_sha256={} output_complete={} coverage_indeterminate_transactions={} coverage_sha256={}",
            self.workload,
            self.output.schema,
            self.output.row_count,
            self.output.output_bytes,
            self.output.sha256_hex(),
            self.output_complete,
            self.coverage.indeterminate_transactions,
            self.coverage.sha256_hex(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const USAGE: &str = "usage: example ...";

    #[test]
    fn workload_defaults_to_public_epoch_900() {
        let arguments = workload_arguments_from(
            "read-car-usdc",
            None,
            None,
            "car-usdc.bin",
            std::iter::empty(),
        )
        .unwrap();
        assert_eq!(arguments.epoch, 900);
        assert_eq!(arguments.expected_blocks.get(), 431_858);
        assert_eq!(arguments.output, PathBuf::from("car-usdc.bin"));
        assert_eq!(arguments.max_blocks, None);
        assert_eq!(
            arguments.source,
            WorkloadSource::Network {
                route: Route::Worker,
                origin: DEFAULT_PUBLIC_ORIGIN.to_owned(),
            }
        );
    }

    #[test]
    fn workload_resolves_the_local_archive_root() {
        let arguments = workload_arguments_from(
            "read-car-pumpfun",
            None,
            None,
            "car-pumpfun.bin",
            [
                "--archive-root",
                "archive",
                "--epoch",
                "100",
                "--output",
                "pump.bin",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(arguments.epoch, 100);
        assert_eq!(arguments.expected_blocks.get(), 402_076);
        assert_eq!(arguments.output, PathBuf::from("pump.bin"));
        assert_eq!(
            arguments.source,
            WorkloadSource::LocalArchive {
                archive_root: PathBuf::from("archive"),
            }
        );
    }

    #[test]
    fn firewatch_has_a_default_wallet() {
        let arguments = workload_arguments_from(
            "read-car-firewatch",
            Some("wallet"),
            Some(DEFAULT_FIREWATCH_WALLET),
            "car-firewatch.bin",
            std::iter::empty(),
        )
        .unwrap();
        assert_eq!(arguments.target.as_deref(), Some(DEFAULT_FIREWATCH_WALLET));
    }

    #[test]
    fn workload_rejects_mixed_local_and_network_sources() {
        let error = workload_arguments_from(
            "read-car-usdc",
            None,
            None,
            "car-usdc.bin",
            [
                "--archive-root",
                "archive",
                "--origin",
                "https://archive.example",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("either --origin or --archive-root")
        );
    }

    #[test]
    fn workload_keeps_the_legacy_smoke_prefix() {
        let arguments = workload_arguments_from(
            "read-car-usdc",
            None,
            None,
            "car-usdc.bin",
            [
                "old-faithful-operator-trusted",
                "https://files.old-faithful.net",
                "900",
                "431858",
                "legacy.bin",
                "1024",
            ]
            .into_iter()
            .map(OsString::from),
        )
        .unwrap();
        assert_eq!(arguments.max_blocks.unwrap().get(), 1_024);
        assert_eq!(arguments.output, PathBuf::from("legacy.bin"));
        assert!(matches!(
            arguments.source,
            WorkloadSource::Network {
                route: Route::OldFaithfulOperatorTrusted,
                ..
            }
        ));
    }

    #[test]
    fn parses_a_full_epoch_task() {
        let arguments = task_arguments(
            [
                "old-faithful",
                "https://files.old-faithful.net",
                "900",
                "431858",
                "usdc.bin",
            ]
            .into_iter()
            .map(str::to_owned),
            USAGE,
        )
        .unwrap();
        assert_eq!(arguments.route, Route::OldFaithful);
        assert_eq!(arguments.epoch, 900);
        assert_eq!(arguments.expected_blocks.get(), 431_858);
        assert_eq!(arguments.output, PathBuf::from("usdc.bin"));
        assert_eq!(arguments.max_blocks, None);
    }

    #[test]
    fn parses_the_explicit_operator_trusted_route() {
        let arguments = task_arguments(
            [
                "old-faithful-operator-trusted",
                "https://files.old-faithful.net",
                "100",
                "430000",
                "usdc.bin",
            ]
            .into_iter()
            .map(str::to_owned),
            USAGE,
        )
        .unwrap();
        assert_eq!(arguments.route, Route::OldFaithfulOperatorTrusted);
    }

    #[test]
    fn parses_a_target_and_smoke_limit() {
        let arguments = target_task_arguments(
            [
                "worker",
                "https://archive.example",
                "0",
                "431548",
                "11111111111111111111111111111111",
                "firewatch.bin",
                "1024",
            ]
            .into_iter()
            .map(str::to_owned),
            USAGE,
        )
        .unwrap();
        assert_eq!(arguments.target, "11111111111111111111111111111111");
        assert_eq!(arguments.source.max_blocks.unwrap().get(), 1_024);
    }

    #[test]
    fn parses_a_32_byte_public_key() {
        assert_eq!(
            parse_pubkey("11111111111111111111111111111111").unwrap(),
            [0_u8; 32]
        );
        assert!(parse_pubkey("1111").is_err());
    }
}
