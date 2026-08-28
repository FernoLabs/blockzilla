//! Archive V2 verification orchestration for the public dump CLI.

use std::time::Instant;

use anyhow::{Context, Result, ensure};
use blockzilla_read_sdk::{
    ArchiveReader, ArchiveSignatureConfig, HashVerification, OpenOptions, PinnedLocalRangeSource,
    archive_integrity::{
        ArchiveContinuityConfig, ArchiveIntegrityConfig, PohProtocolBounds, PohSidecarSchema,
        verify_archive_v2_blockhash_continuity, verify_archive_v2_integrity,
    },
    verify_archive_v2_signatures,
};
use serde::Serialize;

use crate::scan::{PreparedEpoch, SourceOptions, prepare_epoch};

pub const DEFAULT_POH_MAX_HASH_ROUNDS_PER_BLOCK: u64 = 20_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CheckState {
    Passed,
    NotRequested,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckReport {
    pub state: CheckState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl CheckReport {
    fn passed() -> Self {
        Self {
            state: CheckState::Passed,
            detail: None,
        }
    }

    fn not_requested() -> Self {
        Self {
            state: CheckState::NotRequested,
            detail: None,
        }
    }

    fn failed(error: impl std::fmt::Display) -> Self {
        Self {
            state: CheckState::Failed,
            detail: Some(error.to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EpochVerifyReport {
    pub epoch: u64,
    pub source: String,
    pub predecessor_epoch: Option<u64>,
    pub predecessor_boundary_checked: bool,
    pub continuity: CheckReport,
    pub poh: CheckReport,
    pub signatures: CheckReport,
    pub chain_blocks_verified: u64,
    pub predecessor_tail_records_verified: u64,
    pub poh_entries_verified: u64,
    pub poh_max_hash_rounds_per_block: Option<u64>,
    pub poh_max_total_hash_rounds: Option<u64>,
    pub transactions_with_signatures_verified: u64,
    pub signatures_verified: u64,
    pub signature_max_bytes_per_block: Option<usize>,
    pub signature_max_total_worker_bytes: Option<usize>,
    pub elapsed_millis: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerifyReport {
    pub first_epoch: u64,
    pub last_epoch: u64,
    pub continuity: &'static str,
    pub poh_requested: bool,
    pub signatures_requested: bool,
    pub overall: CheckState,
    pub epochs: Vec<EpochVerifyReport>,
}

pub struct VerifyRunConfig {
    pub source: SourceOptions,
    pub start_epoch: u64,
    pub end_epoch: u64,
    pub threads: usize,
    pub poh_requested: bool,
    pub signatures_requested: bool,
    pub poh_bounds: Option<PohProtocolBounds>,
    pub poh_schema: PohSidecarSchema,
    pub poh_max_hash_rounds_per_block: u64,
}

pub struct VerifyRunResult {
    pub report: VerifyReport,
}

pub fn run_verify(config: VerifyRunConfig) -> Result<VerifyRunResult> {
    ensure!(
        config.start_epoch <= config.end_epoch,
        "epoch range start {} is greater than end {}",
        config.start_epoch,
        config.end_epoch
    );
    ensure!(
        config.threads > 0
            && config.threads <= blockzilla_read_sdk::MAX_ORDERED_PARALLEL_DECODE_WORKERS,
        "threads must be in 1..={}",
        blockzilla_read_sdk::MAX_ORDERED_PARALLEL_DECODE_WORKERS
    );
    if config.poh_requested {
        ensure!(
            config.poh_bounds.is_some(),
            "trusted PoH protocol bounds are required with --poh"
        );
        let bounds = config.poh_bounds.expect("presence checked above");
        ensure!(
            bounds.ticks_per_slot > 0 && bounds.hashes_per_tick > 0,
            "PoH ticks-per-slot and hashes-per-tick must be positive"
        );
        ensure!(
            bounds
                .ticks_per_slot
                .checked_mul(bounds.hashes_per_tick)
                .is_some(),
            "PoH hashes-per-slot overflow"
        );
        ensure!(
            config.poh_max_hash_rounds_per_block > 0,
            "--poh-max-hash-rounds-per-block must be positive"
        );
    }

    let local_mode = config.source.archive.is_some();
    let mut previous = if config.start_epoch == 0 {
        None
    } else {
        let epoch = config.start_epoch - 1;
        Some(
            prepare_epoch(&config.source, epoch)
                .with_context(|| format!("open required predecessor epoch {epoch}"))?,
        )
    };
    let mut report = VerifyReport {
        first_epoch: config.start_epoch,
        last_epoch: config.end_epoch,
        continuity: "adjacent-epochs-with-predecessor-boundary",
        poh_requested: config.poh_requested,
        signatures_requested: config.signatures_requested,
        overall: CheckState::Passed,
        epochs: Vec::new(),
    };

    for epoch in config.start_epoch..=config.end_epoch {
        let current = prepare_epoch(&config.source, epoch)
            .with_context(|| format!("open required adjacent epoch {epoch}"))?;
        let started = Instant::now();
        let predecessor_epoch = previous
            .as_ref()
            .map(|value| value.archive.manifest().epoch);
        let slots_per_epoch = current.archive.manifest().slots_per_epoch;
        let poh_max_total_hash_rounds = config
            .poh_bounds
            .map(|bounds| {
                bounds
                    .ticks_per_slot
                    .checked_mul(bounds.hashes_per_tick)
                    .and_then(|hashes_per_slot| hashes_per_slot.checked_mul(slots_per_epoch))
                    .context("PoH per-epoch hash-round cap overflow")
            })
            .transpose()?;
        let continuity_result = verify_archive_v2_blockhash_continuity(
            &current.archive,
            previous.as_ref().map(|value| &value.archive),
            ArchiveContinuityConfig {
                epoch,
                slots_per_epoch,
                selected_blocks: current.archive.index().rows.len(),
                workers: config.threads,
            },
        );

        let mut epoch_report = EpochVerifyReport {
            epoch,
            source: current.source_identity.clone(),
            predecessor_epoch,
            predecessor_boundary_checked: false,
            continuity: CheckReport::passed(),
            poh: if config.poh_requested {
                CheckReport::failed("not run because continuity did not complete")
            } else {
                CheckReport::not_requested()
            },
            signatures: if config.signatures_requested {
                CheckReport::failed("not run because continuity did not complete")
            } else {
                CheckReport::not_requested()
            },
            chain_blocks_verified: 0,
            predecessor_tail_records_verified: 0,
            poh_entries_verified: 0,
            poh_max_hash_rounds_per_block: config
                .poh_requested
                .then_some(config.poh_max_hash_rounds_per_block),
            poh_max_total_hash_rounds,
            transactions_with_signatures_verified: 0,
            signatures_verified: 0,
            signature_max_bytes_per_block: None,
            signature_max_total_worker_bytes: None,
            elapsed_millis: 0,
        };

        match continuity_result {
            Ok(continuity) => {
                epoch_report.predecessor_boundary_checked = continuity.predecessor_boundary_checked;
                epoch_report.chain_blocks_verified = continuity.chain_blocks_verified;
                epoch_report.predecessor_tail_records_verified =
                    continuity.predecessor_tail_records_verified;
            }
            Err(error) => {
                epoch_report.continuity = CheckReport::failed(error);
                epoch_report.elapsed_millis = elapsed_millis(started.elapsed());
                report.overall = CheckState::Failed;
                report.epochs.push(epoch_report);
                break;
            }
        }

        if config.poh_requested {
            if local_mode {
                match verify_local_poh(
                    &current,
                    previous.as_ref(),
                    config.threads,
                    config.poh_bounds.expect("checked above"),
                    config.poh_schema,
                    config.poh_max_hash_rounds_per_block,
                    poh_max_total_hash_rounds.expect("computed for requested PoH"),
                ) {
                    Ok(poh) => {
                        epoch_report.poh = CheckReport::passed();
                        epoch_report.poh_entries_verified = poh.poh_entries_verified;
                    }
                    Err(error) => {
                        epoch_report.poh = CheckReport::failed(format!("{error:#}"));
                    }
                }
            } else {
                epoch_report.poh = CheckReport::failed(
                    "PoH verification is not supported in gateway mode; use --archive with a local published generation",
                );
            }
        }

        if config.signatures_requested {
            match verify_archive_v2_signatures(
                &current.archive,
                ArchiveSignatureConfig {
                    workers: config.threads,
                },
            ) {
                Ok(signatures) => {
                    epoch_report.signatures = CheckReport::passed();
                    epoch_report.transactions_with_signatures_verified =
                        signatures.transactions_verified;
                    epoch_report.signatures_verified = signatures.signatures_verified;
                    epoch_report.signature_max_bytes_per_block =
                        Some(signatures.max_signature_bytes_per_block);
                    epoch_report.signature_max_total_worker_bytes =
                        Some(signatures.max_total_worker_signature_bytes);
                }
                Err(error) => {
                    epoch_report.signatures = CheckReport::failed(error);
                }
            }
        }

        if epoch_report.poh.state == CheckState::Failed
            || epoch_report.signatures.state == CheckState::Failed
        {
            report.overall = CheckState::Failed;
        }
        epoch_report.elapsed_millis = elapsed_millis(started.elapsed());
        report.epochs.push(epoch_report);
        previous = Some(current);
        if report.overall == CheckState::Failed {
            break;
        }
    }

    Ok(VerifyRunResult { report })
}

fn verify_local_poh(
    current: &PreparedEpoch,
    predecessor: Option<&PreparedEpoch>,
    workers: usize,
    poh: PohProtocolBounds,
    poh_schema: PohSidecarSchema,
    max_hash_rounds_per_block: u64,
    max_total_hash_rounds: u64,
) -> Result<blockzilla_read_sdk::archive_integrity::ArchiveIntegrityReport> {
    let options = OpenOptions {
        hash_verification: HashVerification::ControlFiles,
        ..OpenOptions::default()
    };
    let current_reader = ArchiveReader::open_with_options(
        PinnedLocalRangeSource::new(&current.source_root),
        options.clone(),
    )
    .context("open local epoch for PoH verification")?;
    let predecessor_reader = predecessor
        .map(|value| {
            ArchiveReader::open_with_options(
                PinnedLocalRangeSource::new(&value.source_root),
                options.clone(),
            )
            .context("open local predecessor for PoH verification")
        })
        .transpose()?;
    verify_archive_v2_integrity(
        &current_reader,
        predecessor_reader.as_ref(),
        ArchiveIntegrityConfig {
            epoch: current_reader.manifest().epoch,
            slots_per_epoch: current_reader.manifest().slots_per_epoch,
            selected_blocks: current_reader.index().rows.len(),
            workers,
            poh,
            poh_schema,
            max_hash_rounds_per_block,
            max_total_hash_rounds,
        },
    )
    .map_err(anyhow::Error::from)
}

pub fn print_human_verify_report(report: &VerifyReport) {
    println!("epochs: {}..={}", report.first_epoch, report.last_epoch);
    println!("overall: {}", check_state_name(report.overall));
    for epoch in &report.epochs {
        println!("epoch {}: {}", epoch.epoch, epoch.source);
        println!(
            "  continuity: {} ({} blocks, boundary: {})",
            check_state_name(epoch.continuity.state),
            epoch.chain_blocks_verified,
            if epoch.predecessor_boundary_checked {
                "checked"
            } else {
                "not-applicable"
            }
        );
        print_check_detail(&epoch.continuity);
        println!("  PoH: {}", check_state_name(epoch.poh.state));
        if let (Some(per_block), Some(total)) = (
            epoch.poh_max_hash_rounds_per_block,
            epoch.poh_max_total_hash_rounds,
        ) {
            println!("    hash-round caps: {per_block} per block, {total} per epoch");
        }
        print_check_detail(&epoch.poh);
        println!(
            "  signatures: {} ({} transactions, {} signatures)",
            check_state_name(epoch.signatures.state),
            epoch.transactions_with_signatures_verified,
            epoch.signatures_verified
        );
        if let (Some(per_block), Some(total)) = (
            epoch.signature_max_bytes_per_block,
            epoch.signature_max_total_worker_bytes,
        ) {
            println!(
                "    signature-byte caps: {per_block} per in-flight block, {total} across workers"
            );
        }
        print_check_detail(&epoch.signatures);
    }
}

fn print_check_detail(report: &CheckReport) {
    if let Some(detail) = &report.detail {
        println!("    {detail}");
    }
}

fn check_state_name(state: CheckState) -> &'static str {
    match state {
        CheckState::Passed => "passed",
        CheckState::NotRequested => "not-requested",
        CheckState::Failed => "failed",
    }
}

fn elapsed_millis(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
