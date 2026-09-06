use std::{
    env,
    error::Error,
    fmt::{self, Display, Formatter},
    num::NonZeroU32,
    path::PathBuf,
    time::Instant,
};

use blockzilla_archive_v3_reader::{
    ArchiveInstructionSource, ArchiveInstructionSourceExt, ArchiveIoSnapshot, IndexerV3Archive,
    IndexerV3SourceScope, IndexerV3TransportKind, ScanRequest,
};

const DEFAULT_MAX_BLOCKS: u32 = 1_024;
const USAGE: &str = "usage:\n  read-archive-v3 <worker-origin> <epoch> <absolute-cache-root> [max-blocks]\n  read-archive-v3 local-split <ledger-root> <retained-sidecar-root> <epoch> <candidate-id> [max-blocks]";

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = arguments_from(env::args().skip(1))?;
    let total_started = Instant::now();

    let mut archive = open_archive(&arguments)?;
    let setup_seconds = elapsed_seconds(total_started);
    let bound_source_size_bytes = archive.bound_source_size_bytes();
    let cached_source_size_bytes = archive.cached_source_size_bytes();
    let source_scope = source_scope_name(archive.source_scope());
    let verification = archive.identity().verification;
    let range = archive.bounded_range(0, arguments.max_blocks)?;
    let request = benchmark_request(range);

    let setup_transport = archive.transport_snapshot();
    let setup_io = setup_transport.http_and_cache;
    let mut first_slot = None;
    let mut last_slot = None;
    let scan_started = Instant::now();
    let receipt = archive.for_each_block(&request, |block| {
        first_slot.get_or_insert(block.header.slot);
        last_slot = Some(block.header.slot);
        Ok(())
    })?;
    let scan_seconds = elapsed_seconds(scan_started);
    let transport_before_identity_check = archive.transport_snapshot();
    archive.verify_local_unchanged()?;
    let total_transport = archive.finish_transport_io();
    if total_transport != transport_before_identity_check {
        return Err(
            CliError("V3 transport counters changed during the identity check".into()).into(),
        );
    }
    let total_io = total_transport.http_and_cache;
    let scan_io = total_io.saturating_sub(setup_io);
    let scan_local_read_calls = total_transport
        .local_read_calls
        .saturating_sub(setup_transport.local_read_calls);
    let scan_local_read_bytes = total_transport
        .local_read_bytes
        .saturating_sub(setup_transport.local_read_bytes);
    let total_seconds = elapsed_seconds(total_started);
    let first_slot = first_slot.ok_or("scan returned no blocks")?;
    let last_slot = last_slot.ok_or("scan returned no blocks")?;
    print_result(ResultView {
        arguments: &arguments,
        transport_kind: total_transport.kind,
        verification,
        source_scope,
        selected_blocks: range.block_count.get(),
        bound_source_size_bytes,
        cached_source_size_bytes,
        blocks: receipt.blocks,
        transactions: receipt.transactions,
        first_slot,
        last_slot,
        setup_seconds,
        scan_seconds,
        total_seconds,
        setup_io,
        scan_io,
        total_io,
        setup_local_read_calls: setup_transport.local_read_calls,
        scan_local_read_calls,
        total_local_read_calls: total_transport.local_read_calls,
        setup_local_read_bytes: setup_transport.local_read_bytes,
        scan_local_read_bytes,
        total_local_read_bytes: total_transport.local_read_bytes,
    });
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Arguments {
    source: ArchiveLocation,
    epoch: u64,
    max_blocks: NonZeroU32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ArchiveLocation {
    Network {
        origin: String,
        cache_root: PathBuf,
    },
    LocalSplit {
        ledger_root: PathBuf,
        retained_sidecar_root: PathBuf,
        candidate_id: String,
    },
}

impl ArchiveLocation {
    fn candidate_id(&self) -> &str {
        match self {
            Self::Network { .. } => "none",
            Self::LocalSplit { candidate_id, .. } => candidate_id,
        }
    }
}

fn arguments_from<I, S>(values: I) -> Result<Arguments, CliError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut values = values.into_iter().map(Into::into);
    let first = values.next().ok_or_else(usage_error)?;
    if first == "local-split" {
        return local_split_arguments(values);
    }
    network_arguments(first, values)
}

fn network_arguments(
    origin: String,
    mut values: impl Iterator<Item = String>,
) -> Result<Arguments, CliError> {
    let epoch = parse_number("epoch", &values.next().ok_or_else(usage_error)?)?;
    let cache_root = PathBuf::from(values.next().ok_or_else(usage_error)?);
    let max_blocks = parse_max_blocks(values.next())?;
    if values.next().is_some() {
        return Err(usage_error());
    }
    Ok(Arguments {
        source: ArchiveLocation::Network { origin, cache_root },
        epoch,
        max_blocks,
    })
}

fn local_split_arguments(mut values: impl Iterator<Item = String>) -> Result<Arguments, CliError> {
    let ledger_root = PathBuf::from(values.next().ok_or_else(usage_error)?);
    let retained_sidecar_root = PathBuf::from(values.next().ok_or_else(usage_error)?);
    let epoch = parse_number("epoch", &values.next().ok_or_else(usage_error)?)?;
    let candidate_id = values.next().ok_or_else(usage_error)?;
    validate_candidate_id(&candidate_id)?;
    let max_blocks = parse_max_blocks(values.next())?;
    if values.next().is_some() {
        return Err(usage_error());
    }
    Ok(Arguments {
        source: ArchiveLocation::LocalSplit {
            ledger_root,
            retained_sidecar_root,
            candidate_id,
        },
        epoch,
        max_blocks,
    })
}

fn parse_number<T>(name: &str, value: &str) -> Result<T, CliError>
where
    T: std::str::FromStr,
    T::Err: Display,
{
    value
        .parse()
        .map_err(|error| CliError(format!("invalid {name} {value:?}: {error}")))
}

fn parse_max_blocks(value: Option<String>) -> Result<NonZeroU32, CliError> {
    let max_blocks = value.map_or(Ok(DEFAULT_MAX_BLOCKS), |value| {
        parse_number("max-blocks", &value)
    })?;
    NonZeroU32::new(max_blocks)
        .ok_or_else(|| CliError("max-blocks must be greater than zero".into()))
}

fn validate_candidate_id(candidate_id: &str) -> Result<(), CliError> {
    if candidate_id.is_empty() {
        return Err(CliError("candidate-id must not be empty".into()));
    }
    if candidate_id.chars().any(char::is_whitespace) {
        return Err(CliError(
            "candidate-id must be one token without whitespace".into(),
        ));
    }
    Ok(())
}

fn open_archive(arguments: &Arguments) -> blockzilla_archive_v3_reader::Result<IndexerV3Archive> {
    match &arguments.source {
        ArchiveLocation::Network { origin, cache_root } => {
            IndexerV3Archive::open(origin, arguments.epoch, cache_root)
        }
        ArchiveLocation::LocalSplit {
            ledger_root,
            retained_sidecar_root,
            candidate_id,
        } => IndexerV3Archive::open_local_split(
            ledger_root,
            retained_sidecar_root,
            arguments.epoch,
            candidate_id,
        ),
    }
}

fn benchmark_request(range: blockzilla_archive_v3_reader::ScanRange) -> ScanRequest {
    ScanRequest::bounded(range)
        .allow_incomplete_instructions()
        .allow_incomplete_cpi()
        .allow_unknown_execution()
        .without_instruction_data()
}

struct ResultView<'a> {
    arguments: &'a Arguments,
    transport_kind: IndexerV3TransportKind,
    verification: blockzilla_archive_v3_reader::SourceVerification,
    source_scope: &'static str,
    selected_blocks: u32,
    bound_source_size_bytes: u64,
    cached_source_size_bytes: u64,
    blocks: u64,
    transactions: u64,
    first_slot: u64,
    last_slot: u64,
    setup_seconds: f64,
    scan_seconds: f64,
    total_seconds: f64,
    setup_io: ArchiveIoSnapshot,
    scan_io: ArchiveIoSnapshot,
    total_io: ArchiveIoSnapshot,
    setup_local_read_calls: u64,
    scan_local_read_calls: u64,
    total_local_read_calls: u64,
    setup_local_read_bytes: u64,
    scan_local_read_bytes: u64,
    total_local_read_bytes: u64,
}

fn print_result(result: ResultView<'_>) {
    let scan_tps = result.transactions as f64 / result.scan_seconds;
    let total_tps = result.transactions as f64 / result.total_seconds;
    let scan_aggregate_io_bytes = result
        .scan_io
        .network_body_bytes
        .saturating_add(result.scan_io.cache_read_bytes);
    let total_aggregate_io_bytes = result
        .total_io
        .network_body_bytes
        .saturating_add(result.total_io.cache_read_bytes);
    println!(
        "format=indexer-v3 transport_kind={} candidate_id={} epoch={} verification={} source_scope={} requested_max_blocks={} selected_blocks={} bound_source_size_bytes={} cached_source_size_bytes={} blocks={} transactions={} first_slot={} last_slot={} setup_s={:.6} scan_s={:.6} total_s={:.6} scan_tps={:.3} total_tps={:.3} setup_head_requests={} scan_head_requests={} total_head_requests={} setup_get_requests={} scan_get_requests={} total_get_requests={} setup_network_bytes={} scan_network_bytes={} total_network_bytes={} setup_cache_hits={} scan_cache_hits={} total_cache_hits={} setup_cache_downloads={} scan_cache_downloads={} total_cache_downloads={} setup_cache_read_calls={} scan_cache_read_calls={} total_cache_read_calls={} setup_cache_bytes={} scan_cache_bytes={} total_cache_bytes={} scan_network_mb_s={:.6} scan_aggregate_io_mb_s={:.6} total_network_mb_s={:.6} total_aggregate_io_mb_s={:.6} setup_local_read_calls={} scan_local_read_calls={} total_local_read_calls={} setup_local_read_bytes={} scan_local_read_bytes={} total_local_read_bytes={} scan_local_mb_s={:.6} total_local_mb_s={:.6}",
        transport_kind_name(result.transport_kind),
        result.arguments.source.candidate_id(),
        result.arguments.epoch,
        result.verification,
        result.source_scope,
        result.arguments.max_blocks,
        result.selected_blocks,
        result.bound_source_size_bytes,
        result.cached_source_size_bytes,
        result.blocks,
        result.transactions,
        result.first_slot,
        result.last_slot,
        result.setup_seconds,
        result.scan_seconds,
        result.total_seconds,
        scan_tps,
        total_tps,
        result.setup_io.head_requests,
        result.scan_io.head_requests,
        result.total_io.head_requests,
        result.setup_io.get_requests,
        result.scan_io.get_requests,
        result.total_io.get_requests,
        result.setup_io.network_body_bytes,
        result.scan_io.network_body_bytes,
        result.total_io.network_body_bytes,
        result.setup_io.cache_hits,
        result.scan_io.cache_hits,
        result.total_io.cache_hits,
        result.setup_io.cache_downloads,
        result.scan_io.cache_downloads,
        result.total_io.cache_downloads,
        result.setup_io.cache_read_calls,
        result.scan_io.cache_read_calls,
        result.total_io.cache_read_calls,
        result.setup_io.cache_read_bytes,
        result.scan_io.cache_read_bytes,
        result.total_io.cache_read_bytes,
        decimal_mb_s(result.scan_io.network_body_bytes, result.scan_seconds),
        decimal_mb_s(scan_aggregate_io_bytes, result.scan_seconds),
        decimal_mb_s(result.total_io.network_body_bytes, result.total_seconds),
        decimal_mb_s(total_aggregate_io_bytes, result.total_seconds),
        result.setup_local_read_calls,
        result.scan_local_read_calls,
        result.total_local_read_calls,
        result.setup_local_read_bytes,
        result.scan_local_read_bytes,
        result.total_local_read_bytes,
        decimal_mb_s(result.scan_local_read_bytes, result.scan_seconds),
        decimal_mb_s(result.total_local_read_bytes, result.total_seconds),
    );
}

fn transport_kind_name(kind: IndexerV3TransportKind) -> &'static str {
    match kind {
        IndexerV3TransportKind::HttpCached => "http-cached",
        IndexerV3TransportKind::LocalSplit => "local-split",
    }
}

fn source_scope_name(scope: IndexerV3SourceScope) -> &'static str {
    match scope {
        IndexerV3SourceScope::SelectedPrefix => "selected-prefix",
        IndexerV3SourceScope::FullSelection => "full-selection",
    }
}

fn elapsed_seconds(started: Instant) -> f64 {
    started.elapsed().as_secs_f64().max(f64::MIN_POSITIVE)
}

fn decimal_mb_s(bytes: u64, seconds: f64) -> f64 {
    bytes as f64 / 1_000_000.0 / seconds
}

fn usage_error() -> CliError {
    CliError(USAGE.into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliError(String);

impl Display for CliError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for CliError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_positional_network_arguments_with_default_limit() {
        let arguments =
            arguments_from(["https://example.test", "900", "/absolute/cache/directory"]).unwrap();
        assert_eq!(arguments.epoch, 900);
        assert_eq!(arguments.max_blocks, NonZeroU32::new(1_024).unwrap());
        assert_eq!(
            arguments.source,
            ArchiveLocation::Network {
                origin: "https://example.test".into(),
                cache_root: PathBuf::from("/absolute/cache/directory"),
            }
        );
    }

    #[test]
    fn parses_positional_network_arguments_with_explicit_limit() {
        let arguments = arguments_from([
            "https://example.test",
            "800",
            "/absolute/cache/directory",
            "432000",
        ])
        .unwrap();
        assert_eq!(arguments.epoch, 800);
        assert_eq!(arguments.max_blocks, NonZeroU32::new(432_000).unwrap());
    }

    #[test]
    fn parses_local_split_arguments() {
        let arguments = arguments_from([
            "local-split",
            "/nas/indexer-v3/epoch-900",
            "/nas/retained/epoch-900",
            "900",
            "epoch-900-v3-candidate",
            "432000",
        ])
        .unwrap();
        assert_eq!(arguments.epoch, 900);
        assert_eq!(arguments.max_blocks, NonZeroU32::new(432_000).unwrap());
        assert_eq!(
            arguments.source,
            ArchiveLocation::LocalSplit {
                ledger_root: PathBuf::from("/nas/indexer-v3/epoch-900"),
                retained_sidecar_root: PathBuf::from("/nas/retained/epoch-900"),
                candidate_id: "epoch-900-v3-candidate".into(),
            }
        );
    }

    #[test]
    fn local_split_uses_the_default_limit_when_it_is_omitted() {
        let arguments = arguments_from([
            "local-split",
            "/nas/indexer-v3/epoch-900",
            "/nas/retained/epoch-900",
            "900",
            "epoch-900-v3-candidate",
        ])
        .unwrap();
        assert_eq!(arguments.max_blocks, NonZeroU32::new(1_024).unwrap());
    }

    #[test]
    fn rejects_zero_limit_empty_candidate_and_extra_arguments() {
        assert!(arguments_from(["https://example.test", "900", "/absolute/cache", "0"]).is_err());
        assert!(arguments_from(["local-split", "/nas/v3", "/nas/retained", "900", "",]).is_err());
        assert!(
            arguments_from([
                "local-split",
                "/nas/v3",
                "/nas/retained",
                "900",
                "candidate with spaces",
            ])
            .is_err()
        );
        assert!(
            arguments_from([
                "local-split",
                "/nas/v3",
                "/nas/retained",
                "900",
                "candidate",
                "1024",
                "extra",
            ])
            .is_err()
        );
    }
}
