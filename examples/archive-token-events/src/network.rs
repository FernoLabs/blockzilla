use std::{
    num::NonZeroU32,
    path::{Component, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, ensure};
use blockzilla_archive_sdk::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveIoSnapshot, ArchiveOpenReceipt, ArchiveSource,
    NetworkEpoch, ScanRange, ScanReceipt, SourceIdentity, SourceVerification, WORKER_FORMATS,
};
use blockzilla_dump::{
    TokenEventDatabase, TokenEventRunSpec, TokenEventScanOptions, scan_remaining_token_events,
};
use blockzilla_model::token::TargetMintTracker;
use serde::Serialize;

use crate::{
    layout::{FormatLayout, OutputLayout},
    parity::{ComparisonReport, DatabaseSummary, compare_output_databases, database_summary},
    report::{duration_ns, write_json_atomic, write_status},
};

pub const DEFAULT_USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const SUPPORTED_SAMPLE_EPOCHS: [u64; 11] =
    [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HistoryStart {
    Sparse,
    /// The operator asserts that no target account exists before the range.
    TrustedCompleteEmpty,
}

impl HistoryStart {
    fn opening_tracker(self, mint: [u8; 32]) -> TargetMintTracker {
        match self {
            Self::Sparse => TargetMintTracker::from_sparse_start(mint),
            Self::TrustedCompleteEmpty => TargetMintTracker::from_complete_start(mint),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub origin: String,
    pub epoch: u64,
    pub first_block: u32,
    pub max_blocks: NonZeroU32,
    pub output_root: PathBuf,
    pub mint: [u8; 32],
    pub history_start: HistoryStart,
    pub mismatch_limit: usize,
}

impl NetworkConfig {
    fn validate(&self) -> Result<()> {
        ensure!(
            SUPPORTED_SAMPLE_EPOCHS.contains(&self.epoch),
            "epoch {} is not a supported sample; use one of 0, 100, 200, 300, 400, 500, 600, 700, 800, 900, or 1000",
            self.epoch
        );
        ensure!(
            self.mismatch_limit <= 20,
            "mismatch limit must be in 0..=20"
        );
        ensure!(
            self.max_blocks.get() <= 1024,
            "max blocks must be in 1..=1024 for this bounded network demo"
        );
        self.first_block
            .checked_add(self.max_blocks.get())
            .context("first block plus max blocks exceeds u32")?;
        ensure!(
            !self.output_root.as_os_str().is_empty(),
            "output root is empty"
        );
        ensure!(
            self.output_root.is_absolute(),
            "output root must be an absolute path"
        );
        ensure!(
            !self
                .output_root
                .components()
                .any(|component| matches!(component, Component::ParentDir)),
            "output root must not contain '..'"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NetworkOutcome {
    pub compact_v2_report: PathBuf,
    pub car_report: PathBuf,
    pub indexer_v3_report: PathBuf,
    pub comparison_report: PathBuf,
    pub exact_token_event_parity: bool,
    pub exact_coverage_parity: bool,
    pub canonical_source_digest_parity: bool,
}

#[derive(Debug, Serialize)]
struct FormatReport {
    schema: &'static str,
    status: &'static str,
    format: &'static str,
    source: SourceIdentity,
    range: ScanRange,
    target_mint: String,
    history_start: HistoryStart,
    setup_wall_ns: u64,
    scan_wall_ns: u64,
    finalize_and_audit_wall_ns: u64,
    total_wall_ns: u64,
    already_complete: bool,
    scan_receipt: ScanReceipt,
    archive: FormatArchiveReport,
    sqlite: DatabaseSummary,
}

/// Format-neutral SDK facts. The label and exact trust level stay explicit.
#[derive(Debug, Serialize)]
struct FormatArchiveReport {
    format: &'static str,
    verification: SourceVerification,
    source_binding: String,
    open: ArchiveOpenReceipt,
    before_scan_snapshot: ArchiveIoSnapshot,
    scan_interval_delta: ArchiveIoSnapshot,
    total: ArchiveIoSnapshot,
}

pub fn run_network(config: &NetworkConfig) -> Result<NetworkOutcome> {
    config.validate()?;
    let layout = OutputLayout::new(&config.output_root, config.epoch);
    layout.prepare()?;
    for format in WORKER_FORMATS {
        write_status(
            &layout_for(&layout, format).report,
            config.epoch,
            Some(format_label(format)),
            "running",
            None,
        )?;
    }
    write_status(
        &layout.comparison_report,
        config.epoch,
        None,
        "running",
        None,
    )?;

    let mut epoch = match NetworkEpoch::open(&config.origin, config.epoch, &layout.archive_cache)
        .context("open the published archive epoch through blockzilla-archive-sdk")
    {
        Ok(epoch) => epoch,
        Err(error) => {
            mark_failure(config, &layout, 0, &error);
            return Err(error);
        }
    };
    let range = match epoch
        .bounded_range(config.first_block, config.max_blocks)
        .context("select the bounded SDK block range")
    {
        Ok(range) => range,
        Err(error) => {
            mark_failure(config, &layout, 0, &error);
            return Err(error);
        }
    };

    for (index, format) in WORKER_FORMATS.into_iter().enumerate() {
        if let Err(error) = run_format(
            config,
            &mut epoch,
            format,
            range,
            layout_for(&layout, format),
        ) {
            mark_failure(config, &layout, index, &error);
            return Err(error);
        }
    }

    let comparison = match (|| -> Result<ComparisonReport> {
        let comparison = compare_output_databases(
            &layout.car.database,
            &layout.compact_v2.database,
            &layout.indexer_v3.database,
            config.mismatch_limit,
        )
        .context("compare the three token-event databases")?;
        write_json_atomic(&layout.comparison_report, &comparison)?;
        Ok(comparison)
    })() {
        Ok(comparison) => comparison,
        Err(error) => {
            let message = format!("{error:#}");
            let _ = write_status(
                &layout.comparison_report,
                config.epoch,
                None,
                "failed",
                Some(&message),
            );
            return Err(error);
        }
    };
    Ok(outcome(&layout, &comparison))
}

fn run_format(
    config: &NetworkConfig,
    epoch: &mut NetworkEpoch,
    format: ArchiveFormat,
    range: ScanRange,
    layout: &FormatLayout,
) -> Result<()> {
    let label = format_label(format);
    let source = epoch
        .open_source_for(format, range)
        .with_context(|| format!("open {label} through blockzilla-archive-sdk"))?;
    run_source(config, format, range, layout, source)
}

fn run_source(
    config: &NetworkConfig,
    format: ArchiveFormat,
    range: ScanRange,
    layout: &FormatLayout,
    mut source: ArchiveSource,
) -> Result<()> {
    let label = format_label(format);
    let source_identity = source.identity().clone();
    let source_binding = source_identity
        .binding
        .clone()
        .context("the SDK source has no stable binding")?;
    let sdk_open = source.open_receipt().clone();

    let application_setup_start = Instant::now();
    let mut database = open_database(config, layout, &source_identity, range)?;
    let application_setup_wall_ns = duration_ns(application_setup_start.elapsed());
    let setup_wall_ns = sdk_open
        .setup_wall_ns
        .saturating_add(application_setup_wall_ns);
    let before_scan_snapshot = source.io_snapshot();

    let scan_start = Instant::now();
    let result = scan_remaining_token_events(
        &mut source,
        &mut database,
        TokenEventScanOptions {
            allow_weaker_source: matches!(
                source_identity.verification,
                SourceVerification::InternalBindingOnly | SourceVerification::Unverified
            ),
        },
    )
    .with_context(|| format!("scan {label} token events"))?;
    database
        .checkpoint_wal()
        .with_context(|| format!("checkpoint {label} database"))?;
    let scan_wall_ns = duration_ns(scan_start.elapsed());
    let after_scan_snapshot = source.io_snapshot();

    let finalize_start = Instant::now();
    drop(database);
    let final_archive_snapshot = source.finish_io();
    let sqlite = database_summary(&layout.database)?;
    let finalize_and_audit_wall_ns = duration_ns(finalize_start.elapsed());
    let total_wall_ns = setup_wall_ns
        .saturating_add(scan_wall_ns)
        .saturating_add(finalize_and_audit_wall_ns);
    let report = FormatReport {
        schema: "blockzilla-archive-token-events/format-report-v1",
        status: "complete",
        format: label,
        source: source_identity.clone(),
        range,
        target_mint: bs58::encode(config.mint).into_string(),
        history_start: config.history_start,
        setup_wall_ns,
        scan_wall_ns,
        finalize_and_audit_wall_ns,
        total_wall_ns,
        already_complete: result.already_complete,
        scan_receipt: result.receipt,
        archive: FormatArchiveReport {
            format: label,
            verification: source_identity.verification,
            source_binding,
            open: sdk_open,
            before_scan_snapshot,
            scan_interval_delta: after_scan_snapshot.saturating_sub(before_scan_snapshot),
            total: final_archive_snapshot,
        },
        sqlite,
    };
    write_json_atomic(&layout.report, &report)
}

fn open_database(
    config: &NetworkConfig,
    layout: &FormatLayout,
    source: &SourceIdentity,
    range: ScanRange,
) -> Result<TokenEventDatabase> {
    let opening = config.history_start.opening_tracker(config.mint).snapshot();
    let spec = TokenEventRunSpec::classic(source.clone(), config.mint, range, opening);
    TokenEventDatabase::create_or_open(&layout.database, spec)
        .with_context(|| format!("open token-event database {}", layout.database.display()))
}

const fn layout_for(layout: &OutputLayout, format: ArchiveFormat) -> &FormatLayout {
    match format {
        ArchiveFormat::Car => &layout.car,
        ArchiveFormat::CompactV2 => &layout.compact_v2,
        ArchiveFormat::IndexerV3 => &layout.indexer_v3,
    }
}

const fn format_label(format: ArchiveFormat) -> &'static str {
    match format {
        ArchiveFormat::Car => "car",
        ArchiveFormat::CompactV2 => "compact-v2",
        ArchiveFormat::IndexerV3 => "indexer-v3",
    }
}

fn mark_failure(
    config: &NetworkConfig,
    layout: &OutputLayout,
    failed_index: usize,
    error: &anyhow::Error,
) {
    let message = format!("{error:#}");
    let active_format = WORKER_FORMATS[failed_index];
    let _ = write_status(
        &layout_for(layout, active_format).report,
        config.epoch,
        Some(format_label(active_format)),
        "failed",
        Some(&message),
    );
    for format in WORKER_FORMATS.iter().copied().skip(failed_index + 1) {
        let _ = write_status(
            &layout_for(layout, format).report,
            config.epoch,
            Some(format_label(format)),
            "not-run",
            Some(&message),
        );
    }
    let _ = write_status(
        &layout.comparison_report,
        config.epoch,
        None,
        "failed",
        Some(&message),
    );
}

fn outcome(layout: &OutputLayout, comparison: &ComparisonReport) -> NetworkOutcome {
    NetworkOutcome {
        compact_v2_report: layout.compact_v2.report.clone(),
        car_report: layout.car.report.clone(),
        indexer_v3_report: layout.indexer_v3.report.clone(),
        comparison_report: layout.comparison_report.clone(),
        exact_token_event_parity: comparison.token_event_parity.exact_equal,
        exact_coverage_parity: comparison.coverage_parity.exact_equal,
        canonical_source_digest_parity: comparison.canonical_source_digest_parity.exact_equal,
    }
}

pub fn parse_mint(value: &str) -> Result<[u8; 32]> {
    ensure!(!value.is_empty(), "mint is empty");
    let bytes = bs58::decode(value)
        .into_vec()
        .context("mint is not valid base58")?;
    let mint: [u8; 32] = bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| anyhow::anyhow!("mint has {} bytes, expected 32", bytes.len()))?;
    ensure!(
        bs58::encode(mint).into_string() == value,
        "mint is not canonical base58"
    );
    Ok(mint)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sdk_format_order_has_application_labels_and_layouts() {
        let layout = OutputLayout::new("/tmp/example", 600);
        let observed = WORKER_FORMATS.map(|format| {
            (
                format_label(format),
                layout_for(&layout, format).directory.clone(),
            )
        });
        assert_eq!(
            observed,
            [
                (
                    "compact-v2",
                    PathBuf::from("/tmp/example/compact-v2/epoch-600"),
                ),
                ("car", PathBuf::from("/tmp/example/car/epoch-600")),
                (
                    "indexer-v3",
                    PathBuf::from("/tmp/example/indexer-v3/epoch-600"),
                ),
            ]
        );
    }

    #[test]
    fn validates_mint_and_application_limits() {
        let mint = parse_mint(DEFAULT_USDC_MINT).unwrap();
        assert_eq!(mint.len(), 32);
        assert!(parse_mint("not base58!").is_err());
        assert!(parse_mint("1111111111111111111111111111111").is_err());

        let config = NetworkConfig {
            origin: "https://example.workers.dev".into(),
            epoch: 600,
            first_block: 4,
            max_blocks: NonZeroU32::new(100).unwrap(),
            output_root: PathBuf::from("/tmp/example"),
            mint,
            history_start: HistoryStart::Sparse,
            mismatch_limit: 20,
        };
        config.validate().unwrap();

        let mut unsafe_path = config;
        unsafe_path.output_root = PathBuf::from("/private/tmp/one/../two");
        assert!(unsafe_path.validate().is_err());
    }

    #[test]
    fn rejects_unknown_sample_epoch_before_work() {
        let config = NetworkConfig {
            origin: "https://example.workers.dev".into(),
            epoch: 42,
            first_block: 0,
            max_blocks: NonZeroU32::new(1).unwrap(),
            output_root: PathBuf::from("unused"),
            mint: parse_mint(DEFAULT_USDC_MINT).unwrap(),
            history_start: HistoryStart::Sparse,
            mismatch_limit: 20,
        };
        assert!(config.validate().is_err());
    }
}
