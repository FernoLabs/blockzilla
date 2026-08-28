//! Source-neutral driver for the instruction-only classic Token ledger.
//!
//! Format-specific code ends when it constructs an
//! [`ArchiveInstructionSource`]. This module owns the one common scan request
//! and resumes the bound SQLite sink from its durable block checkpoint.

use std::num::NonZeroU32;

use blockzilla_query_sdk::{
    ArchiveInstructionSource, ScanRange, ScanReceipt, ScanRequest, SourceIdentity,
};
use thiserror::Error;

use crate::{TokenEventDatabase, TokenEventRunSpec};

/// Policy for source verification during one token-event scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenEventScanOptions {
    /// Accept `InternalBindingOnly` or `Unverified` input.
    ///
    /// The database still requires a nonempty immutable binding and records
    /// the exact verification level. This option does not make the input a
    /// publication-verified source.
    pub allow_weaker_source: bool,
}

/// Result of one scan or resume operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenEventScanResult {
    /// True when the durable checkpoint already covered the bound range.
    pub already_complete: bool,
    /// Work done by this call. An already-complete call returns a zero receipt.
    pub receipt: ScanReceipt,
}

#[derive(Debug, Error)]
pub enum TokenEventScanError {
    #[error("archive source identity differs from the token event database")]
    SourceIdentityMismatch {
        database: Box<SourceIdentity>,
        actual_source: Box<SourceIdentity>,
    },
    #[error("invalid durable token event checkpoint: {0}")]
    InvalidCheckpoint(String),
    #[error(transparent)]
    Scan(#[from] blockzilla_query_sdk::Error),
}

pub type Result<T> = std::result::Result<T, TokenEventScanError>;

/// Resume the exact database range through any archive instruction source.
///
/// The request accepts incomplete instruction, CPI, and execution coverage so
/// the token tracker can save explicit coverage issues. Exact instruction
/// bytes are selected for the classic Token program. If those bytes are not
/// exact, the tracker saves a coverage issue instead of treating it as a
/// no-match result.
pub fn scan_remaining_token_events(
    source: &mut dyn ArchiveInstructionSource,
    database: &mut TokenEventDatabase,
    options: TokenEventScanOptions,
) -> Result<TokenEventScanResult> {
    let spec = database.run_spec();
    require_same_source(&spec.source, source.identity())?;

    let end = range_end(spec)?;
    let next = database.next_block_ordinal();
    if next < spec.range.first_block || next > end {
        return Err(TokenEventScanError::InvalidCheckpoint(format!(
            "next block {next} is outside bound range {}..={end}",
            spec.range.first_block
        )));
    }
    if next == end {
        return Ok(TokenEventScanResult {
            already_complete: true,
            receipt: ScanReceipt::default(),
        });
    }

    let remaining = end.checked_sub(next).ok_or_else(|| {
        TokenEventScanError::InvalidCheckpoint("remaining block count underflow".into())
    })?;
    let block_count = NonZeroU32::new(remaining).ok_or_else(|| {
        TokenEventScanError::InvalidCheckpoint("remaining block count is zero".into())
    })?;
    let mut request = ScanRequest::bounded(ScanRange {
        first_block: next,
        block_count,
    })
    .allow_incomplete_instructions()
    .allow_incomplete_cpi()
    .allow_unknown_execution()
    .with_instruction_data_for([spec.token_program])
    .allow_incomplete_instruction_data();
    if options.allow_weaker_source {
        request = request.allow_unverified_source();
    }

    let receipt = source.scan_ordered(&request, database)?;
    if database.next_block_ordinal() != end {
        return Err(TokenEventScanError::InvalidCheckpoint(format!(
            "source returned success at block {}, expected {end}",
            database.next_block_ordinal()
        )));
    }
    Ok(TokenEventScanResult {
        already_complete: false,
        receipt,
    })
}

fn require_same_source(database: &SourceIdentity, source: &SourceIdentity) -> Result<()> {
    if database == source {
        return Ok(());
    }
    Err(TokenEventScanError::SourceIdentityMismatch {
        database: Box::new(database.clone()),
        actual_source: Box::new(source.clone()),
    })
}

fn range_end(spec: &TokenEventRunSpec) -> Result<u32> {
    spec.range
        .first_block
        .checked_add(spec.range.block_count.get())
        .ok_or_else(|| TokenEventScanError::InvalidCheckpoint("scan range end exceeds u32".into()))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use blockzilla_query_sdk::{
        ArchiveFormat, BlockHeader, CanonicalBlock, CanonicalTransaction, CoverageReason,
        CpiCoverage, ExecutionStatus, InstructionCoordinate, InstructionCoverage,
        InstructionDataCoverage, OrderedBlockPublisher, ResolvedInstruction, SourceVerification,
        TransactionHeader,
        token::{CLASSIC_SPL_TOKEN_PROGRAM_ID, HistoryCoverage, TargetMintTracker},
    };

    use super::*;

    const TARGET: [u8; 32] = [7; 32];
    const TOKEN_ACCOUNT: [u8; 32] = [8; 32];

    struct FixtureSource {
        identity: SourceIdentity,
        blocks: Vec<CanonicalBlock>,
        scans: usize,
    }

    impl ArchiveInstructionSource for FixtureSource {
        fn identity(&self) -> &SourceIdentity {
            &self.identity
        }

        fn scan_ordered(
            &mut self,
            request: &ScanRequest,
            sink: &mut dyn blockzilla_query_sdk::BlockSink,
        ) -> blockzilla_query_sdk::Result<ScanReceipt> {
            self.scans += 1;
            let mut publisher = OrderedBlockPublisher::new(&self.identity, request, sink)?;
            let (first, end) = request
                .range
                .map_or((0, self.identity.block_count), |range| {
                    (
                        range.first_block,
                        range.first_block + range.block_count.get(),
                    )
                });
            for block in &self.blocks[first as usize..end as usize] {
                publisher.publish(block)?;
            }
            publisher.finish()
        }
    }

    struct PrivateTempDir(tempfile::TempDir);

    impl PrivateTempDir {
        fn new() -> Self {
            let root = std::fs::canonicalize(std::env::temp_dir()).unwrap();
            let value = tempfile::TempDir::new_in(root).unwrap();
            #[cfg(unix)]
            std::fs::set_permissions(value.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            Self(value)
        }

        fn path(&self) -> &Path {
            self.0.path()
        }
    }

    fn identity(verification: SourceVerification) -> SourceIdentity {
        SourceIdentity {
            format: ArchiveFormat::CompactV2,
            label: "fixture".into(),
            cluster_id: Some("test".into()),
            epoch: 0,
            first_slot: 0,
            slots_per_epoch: 32,
            block_count: 2,
            verification,
            binding: Some("fixture-binding".into()),
        }
    }

    fn fixture(verification: SourceVerification) -> FixtureSource {
        FixtureSource {
            identity: identity(verification),
            blocks: vec![
                CanonicalBlock {
                    header: BlockHeader {
                        epoch: 0,
                        block_ordinal: 0,
                        slot: 0,
                    },
                    transactions: Vec::new(),
                },
                CanonicalBlock {
                    header: BlockHeader {
                        epoch: 0,
                        block_ordinal: 1,
                        slot: 1,
                    },
                    transactions: vec![CanonicalTransaction {
                        header: TransactionHeader {
                            tx_index: 0,
                            status: ExecutionStatus::Succeeded,
                            failed_outer_instruction_index: None,
                            instruction_coverage: InstructionCoverage::Complete,
                            cpi_coverage: CpiCoverage::Unknown(CoverageReason::MetadataAbsent),
                        },
                        primary_signature: Some([9; 64]),
                        required_signers: Vec::new(),
                        instructions: vec![ResolvedInstruction {
                            coordinate: InstructionCoordinate {
                                order: 0,
                                outer_index: 0,
                                inner_index: None,
                                stack_height: None,
                            },
                            program_id: CLASSIC_SPL_TOKEN_PROGRAM_ID,
                            accounts: vec![TOKEN_ACCOUNT],
                            data_coverage: InstructionDataCoverage::Unknown(
                                CoverageReason::AmbiguousInstructionData,
                            ),
                            data: Vec::new(),
                        }],
                    }],
                },
            ],
            scans: 0,
        }
    }

    fn database(directory: &PrivateTempDir, source: SourceIdentity) -> TokenEventDatabase {
        let opening = TargetMintTracker::from_active_account_seed(TARGET, [TOKEN_ACCOUNT]);
        let spec = TokenEventRunSpec::classic(
            source,
            TARGET,
            ScanRange {
                first_block: 0,
                block_count: NonZeroU32::new(2).unwrap(),
            },
            opening.snapshot(),
        );
        TokenEventDatabase::create(directory.path().join("events.sqlite"), spec).unwrap()
    }

    #[test]
    fn one_common_request_records_gaps_and_resumes_without_rescanning() {
        let directory = PrivateTempDir::new();
        let mut source = fixture(SourceVerification::PublishedManifest);
        let mut event_database = database(&directory, source.identity.clone());

        let first = scan_remaining_token_events(
            &mut source,
            &mut event_database,
            TokenEventScanOptions::default(),
        )
        .unwrap();
        assert!(!first.already_complete);
        assert_eq!(first.receipt.blocks, 2);
        assert_eq!(first.receipt.transactions_with_incomplete_cpi, 1);
        assert_eq!(first.receipt.instructions_with_unknown_data, 1);
        assert_eq!(source.scans, 1);
        assert_eq!(event_database.next_block_ordinal(), 2);
        assert_eq!(
            event_database.tracker().history_coverage(),
            HistoryCoverage::Partial
        );

        let second = scan_remaining_token_events(
            &mut source,
            &mut event_database,
            TokenEventScanOptions::default(),
        )
        .unwrap();
        assert!(second.already_complete);
        assert_eq!(second.receipt, ScanReceipt::default());
        assert_eq!(source.scans, 1);
    }

    #[test]
    fn source_identity_and_verification_policy_fail_closed() {
        let directory = PrivateTempDir::new();
        let mut source = fixture(SourceVerification::InternalBindingOnly);
        let mut event_database = database(&directory, source.identity.clone());

        assert!(matches!(
            scan_remaining_token_events(
                &mut source,
                &mut event_database,
                TokenEventScanOptions::default(),
            ),
            Err(TokenEventScanError::Scan(
                blockzilla_query_sdk::Error::InvalidRequest(_)
            ))
        ));
        let result = scan_remaining_token_events(
            &mut source,
            &mut event_database,
            TokenEventScanOptions {
                allow_weaker_source: true,
            },
        )
        .unwrap();
        assert_eq!(result.receipt.blocks, 2);

        let other_directory = PrivateTempDir::new();
        let mut other_database = database(
            &other_directory,
            identity(SourceVerification::PublishedManifest),
        );
        assert!(matches!(
            scan_remaining_token_events(
                &mut source,
                &mut other_database,
                TokenEventScanOptions {
                    allow_weaker_source: true,
                },
            ),
            Err(TokenEventScanError::SourceIdentityMismatch { .. })
        ));
    }
}
