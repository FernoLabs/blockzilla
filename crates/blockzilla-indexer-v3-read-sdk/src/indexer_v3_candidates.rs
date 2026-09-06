//! Sound block candidates from the adaptive Indexer V3 account postings.
//!
//! A positive posting is sufficient to retain its block. An incomplete source
//! transaction can hide an account or CPI-program role, so each policy also
//! retains the coverage blocks that can contain a false negative.

use anyhow::{Context, Result, ensure};
use blockzilla_index_archive_format::indexes::accounts::{
    ROLE_CPI_PROGRAM, ROLE_MASK, ROLE_SIGNER, ROLE_TOP_LEVEL_PROGRAM,
};
use serde::Serialize;

#[cfg(test)]
use crate::AdaptiveV3ResolvedPosting;
use crate::{
    AdaptiveV3Reader, AdaptiveV3ResolvedCoverage, AdaptiveV3RoleBlockVisitSummary,
    AdaptiveV3RoleMatchedBlock,
};

const ACCOUNT_COVERAGE_COMPLETE: u8 = 0;
const ACCOUNT_COVERAGE_MAX: u8 = 3;
const CPI_COVERAGE_COMPLETE: u8 = 0;
const CPI_COVERAGE_MAX: u8 = 4;

/// Registry resolution for the queried public key.
///
/// An absent key can still have sound fallback blocks when source account or
/// CPI coverage is incomplete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexerV3CandidateKey {
    RegistryId(u32),
    RegistryAbsent,
}

/// The exact query meaning used to select positive postings and sound
/// incomplete-coverage fallbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexerV3CandidatePolicy {
    /// Retain signer postings and every block with incomplete account coverage.
    SignerWallet,
    /// Retain top-level or CPI-program postings and every block with incomplete
    /// account or CPI coverage.
    ReachedProgram,
}

impl IndexerV3CandidatePolicy {
    /// Role bits that make one posting a positive match for this policy.
    pub const fn positive_role_mask(self) -> u8 {
        match self {
            Self::SignerWallet => ROLE_SIGNER,
            Self::ReachedProgram => ROLE_TOP_LEVEL_PROGRAM | ROLE_CPI_PROGRAM,
        }
    }

    pub const fn falls_back_for_account_incomplete(self) -> bool {
        true
    }

    pub const fn falls_back_for_cpi_incomplete(self) -> bool {
        matches!(self, Self::ReachedProgram)
    }

    const fn selects_coverage(self, account_coverage: u8, cpi_coverage: u8) -> bool {
        account_coverage != ACCOUNT_COVERAGE_COMPLETE
            || (self.falls_back_for_cpi_incomplete() && cpi_coverage != CPI_COVERAGE_COMPLETE)
    }
}

/// Header geometry that binds the candidate list to one adaptive V3 source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct IndexerV3CandidateGeometry {
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub epoch_first_slot: u64,
    pub epoch_end_slot_exclusive: u64,
    pub registry_entries: u32,
    pub selected_blocks: u64,
    pub selected_transactions: u64,
}

impl IndexerV3CandidateGeometry {
    fn from_reader(reader: &AdaptiveV3Reader) -> Result<Self> {
        Self::from_parts(
            reader.epoch(),
            reader.slots_per_epoch(),
            reader.registry_entries(),
            reader.standalone_selected_blocks(),
            reader.standalone_selected_transactions(),
        )
    }

    fn from_parts(
        epoch: u64,
        slots_per_epoch: u64,
        registry_entries: u32,
        selected_blocks: u64,
        selected_transactions: u64,
    ) -> Result<Self> {
        let epoch_first_slot = epoch
            .checked_mul(slots_per_epoch)
            .context("adaptive V3 epoch first-slot overflow")?;
        let epoch_end_slot_exclusive = epoch_first_slot
            .checked_add(slots_per_epoch)
            .context("adaptive V3 epoch end-slot overflow")?;
        Ok(Self {
            epoch,
            slots_per_epoch,
            epoch_first_slot,
            epoch_end_slot_exclusive,
            registry_entries,
            selected_blocks,
            selected_transactions,
        })
    }
}

/// Exact incomplete-coverage facts checked while candidates were built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct IndexerV3CandidateCoverage {
    /// Number of sparse coverage records. Each record is one transaction.
    pub sparse_transactions: u64,
    pub account_incomplete_transactions: u64,
    pub cpi_incomplete_transactions: u64,
    pub account_only_incomplete_transactions: u64,
    pub cpi_only_incomplete_transactions: u64,
    pub account_and_cpi_incomplete_transactions: u64,
    /// Sparse transactions retained by the selected fallback policy.
    pub policy_fallback_transactions: u64,
    /// Unique blocks retained by the selected fallback policy.
    pub policy_fallback_blocks: u64,
    pub fallback_includes_account_incomplete: bool,
    pub fallback_includes_cpi_incomplete: bool,
    /// True only when the coverage lane proves that account absence is complete.
    pub absence_is_complete: bool,
    /// True only when the coverage lane proves that CPI role bits are complete.
    pub cpi_role_bits_are_complete: bool,
}

/// Exact posting and block counts for one candidate build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct IndexerV3CandidateCounts {
    pub postings_visited: u64,
    pub posting_blocks_visited: u64,
    pub positive_postings: u64,
    pub positive_blocks: u64,
    pub positive_fallback_overlap_blocks: u64,
    pub candidate_blocks: u64,
}

/// Exact account-posting page I/O for one candidate build.
///
/// The adaptive reader caches the sparse coverage lane during open, so the
/// coverage pass does not add range reads to this receipt. Open I/O is not
/// included.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct IndexerV3CandidateReadStats {
    pub pages_read: u64,
    pub read_calls: u64,
    pub stored_bytes: u64,
    pub decoded_bytes: u64,
}

/// Sorted, unique block candidates and the exact evidence used to build them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexerV3CandidateBlocks {
    pub key: IndexerV3CandidateKey,
    pub policy: IndexerV3CandidatePolicy,
    /// All role bits accepted from the account-posting wire format.
    pub validated_role_mask: u8,
    /// Role bits that produced positive candidates for this policy.
    pub positive_role_mask: u8,
    pub geometry: IndexerV3CandidateGeometry,
    pub coverage: IndexerV3CandidateCoverage,
    pub counts: IndexerV3CandidateCounts,
    pub read: IndexerV3CandidateReadStats,
    pub block_ids: Vec<u32>,
}

/// Stream one account posting list and build a sound, sorted, unique set of
/// block IDs for early block filtering.
///
/// `SignerWallet` unions signer-positive blocks with all account-incomplete
/// coverage blocks. `ReachedProgram` unions top-level or CPI-program-positive
/// blocks with all account-incomplete and CPI-incomplete coverage blocks.
pub fn build_indexer_v3_candidate_blocks(
    reader: &AdaptiveV3Reader,
    account_id: u32,
    policy: IndexerV3CandidatePolicy,
) -> Result<IndexerV3CandidateBlocks> {
    build_indexer_v3_candidate_blocks_for_key(
        reader,
        IndexerV3CandidateKey::RegistryId(account_id),
        policy,
    )
}

/// Build candidates after an exact registry lookup, including the
/// coverage-only case where the queried public key is absent from the
/// registry.
pub fn build_indexer_v3_candidate_blocks_for_key(
    reader: &AdaptiveV3Reader,
    key: IndexerV3CandidateKey,
    policy: IndexerV3CandidatePolicy,
) -> Result<IndexerV3CandidateBlocks> {
    let geometry = IndexerV3CandidateGeometry::from_reader(reader)?;
    let mut accumulator = CandidateAccumulator::new(geometry, policy);
    let summary = match key {
        IndexerV3CandidateKey::RegistryId(account_id) => {
            ensure!(
                account_id != 0,
                "adaptive V3 candidate account source ID zero is reserved"
            );
            ensure!(
                account_id <= geometry.registry_entries,
                "adaptive V3 candidate account source ID exceeds registry size"
            );
            let summary = reader
                .visit_account_role_blocks(account_id, policy.positive_role_mask(), |matched| {
                    accumulator.accept_matched_block(matched)
                })
                .context("stream adaptive V3 role blocks for block candidates")?;
            accumulator.accept_role_summary(summary)?;
            Some(CandidatePostingSummary::from(summary))
        }
        IndexerV3CandidateKey::RegistryAbsent => None,
    };
    reader
        .visit_incomplete_transactions(|coverage| accumulator.accept_coverage(coverage))
        .context("stream adaptive V3 incomplete coverage for block candidates")?;
    accumulator.finish(key, summary)
}

struct CandidateAccumulator {
    geometry: IndexerV3CandidateGeometry,
    policy: IndexerV3CandidatePolicy,
    #[cfg(test)]
    previous_posting: Option<(u32, u32)>,
    previous_coverage: Option<(u32, u32)>,
    posting_blocks_visited: u64,
    postings_visited: u64,
    positive_postings: u64,
    positive_blocks: Vec<u32>,
    sparse_transactions: u64,
    account_incomplete_transactions: u64,
    cpi_incomplete_transactions: u64,
    both_incomplete_transactions: u64,
    fallback_transactions: u64,
    fallback_blocks: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
struct CandidatePostingSummary {
    postings: u64,
    pages_read: u64,
    read_calls: u64,
    stored_bytes: u64,
    decoded_bytes: u64,
    incomplete_account_transactions: u64,
    incomplete_cpi_transactions: u64,
    absence_is_complete: bool,
    cpi_role_bits_are_complete: bool,
}

impl From<AdaptiveV3RoleBlockVisitSummary> for CandidatePostingSummary {
    fn from(summary: AdaptiveV3RoleBlockVisitSummary) -> Self {
        Self {
            postings: summary.postings,
            pages_read: summary.pages_read,
            read_calls: summary.read_calls,
            stored_bytes: summary.stored_bytes,
            decoded_bytes: summary.decoded_bytes,
            incomplete_account_transactions: summary.incomplete_account_transactions,
            incomplete_cpi_transactions: summary.incomplete_cpi_transactions,
            absence_is_complete: summary.absence_is_complete,
            cpi_role_bits_are_complete: summary.cpi_role_bits_are_complete,
        }
    }
}

impl CandidateAccumulator {
    fn new(geometry: IndexerV3CandidateGeometry, policy: IndexerV3CandidatePolicy) -> Self {
        Self {
            geometry,
            policy,
            #[cfg(test)]
            previous_posting: None,
            previous_coverage: None,
            posting_blocks_visited: 0,
            postings_visited: 0,
            positive_postings: 0,
            positive_blocks: Vec::new(),
            sparse_transactions: 0,
            account_incomplete_transactions: 0,
            cpi_incomplete_transactions: 0,
            both_incomplete_transactions: 0,
            fallback_transactions: 0,
            fallback_blocks: Vec::new(),
        }
    }

    #[cfg(test)]
    fn accept_posting(&mut self, posting: AdaptiveV3ResolvedPosting) -> Result<()> {
        validate_position(
            posting.block_id,
            posting.tx_index,
            self.geometry.selected_blocks,
            self.previous_posting,
            "adaptive V3 account postings",
        )?;
        ensure!(
            posting.roles & !ROLE_MASK == 0,
            "adaptive V3 posting has unknown role bits {:#x}",
            posting.roles
        );
        validate_coverage_pair(posting.account_coverage, posting.cpi_coverage, false)?;

        if self
            .previous_posting
            .is_none_or(|(block_id, _)| block_id != posting.block_id)
        {
            checked_increment(
                &mut self.posting_blocks_visited,
                "adaptive V3 visited posting-block count",
            )?;
        }
        self.previous_posting = Some((posting.block_id, posting.tx_index));
        checked_increment(
            &mut self.postings_visited,
            "adaptive V3 visited posting count",
        )?;

        if posting.roles & self.policy.positive_role_mask() != 0 {
            checked_increment(
                &mut self.positive_postings,
                "adaptive V3 positive posting count",
            )?;
            push_sorted_unique(
                &mut self.positive_blocks,
                posting.block_id,
                "adaptive V3 positive block list",
            )?;
        }
        Ok(())
    }

    fn accept_matched_block(&mut self, matched: AdaptiveV3RoleMatchedBlock) -> Result<()> {
        ensure!(
            u64::from(matched.block_id) < self.geometry.selected_blocks,
            "adaptive V3 matched block ID {} exceeds selected block geometry {}",
            matched.block_id,
            self.geometry.selected_blocks
        );
        ensure!(
            matched.matching_postings != 0,
            "adaptive V3 matched block has no matching postings"
        );
        if let Some(previous) = self.positive_blocks.last().copied() {
            ensure!(
                matched.block_id > previous,
                "adaptive V3 matched blocks are not strictly increasing"
            );
        }
        self.positive_blocks
            .try_reserve(1)
            .context("reserve adaptive V3 positive block list")?;
        self.positive_blocks.push(matched.block_id);
        self.positive_postings = self
            .positive_postings
            .checked_add(matched.matching_postings)
            .context("adaptive V3 positive posting count overflow")?;
        Ok(())
    }

    fn accept_role_summary(&mut self, summary: AdaptiveV3RoleBlockVisitSummary) -> Result<()> {
        ensure!(
            self.postings_visited == 0 && self.posting_blocks_visited == 0,
            "adaptive V3 role summary was applied after posting counts"
        );
        ensure!(
            self.positive_postings == summary.matching_postings,
            "adaptive V3 matching-posting count differs from its block visitor"
        );
        ensure!(
            usize_to_u64(
                self.positive_blocks.len(),
                "adaptive V3 streamed positive block count"
            )? == summary.matching_blocks,
            "adaptive V3 matching-block count differs from its block visitor"
        );
        self.postings_visited = summary.postings;
        self.posting_blocks_visited = summary.posting_blocks;
        Ok(())
    }

    fn accept_coverage(&mut self, coverage: AdaptiveV3ResolvedCoverage) -> Result<()> {
        validate_position(
            coverage.block_id,
            coverage.tx_index,
            self.geometry.selected_blocks,
            self.previous_coverage,
            "adaptive V3 sparse coverage records",
        )?;
        validate_coverage_pair(coverage.account_coverage, coverage.cpi_coverage, true)?;
        self.previous_coverage = Some((coverage.block_id, coverage.tx_index));
        checked_increment(
            &mut self.sparse_transactions,
            "adaptive V3 sparse coverage count",
        )?;

        let account_incomplete = coverage.account_coverage != ACCOUNT_COVERAGE_COMPLETE;
        let cpi_incomplete = coverage.cpi_coverage != CPI_COVERAGE_COMPLETE;
        if account_incomplete {
            checked_increment(
                &mut self.account_incomplete_transactions,
                "adaptive V3 account-incomplete count",
            )?;
        }
        if cpi_incomplete {
            checked_increment(
                &mut self.cpi_incomplete_transactions,
                "adaptive V3 CPI-incomplete count",
            )?;
        }
        if account_incomplete && cpi_incomplete {
            checked_increment(
                &mut self.both_incomplete_transactions,
                "adaptive V3 jointly incomplete count",
            )?;
        }

        if self
            .policy
            .selects_coverage(coverage.account_coverage, coverage.cpi_coverage)
        {
            checked_increment(
                &mut self.fallback_transactions,
                "adaptive V3 fallback transaction count",
            )?;
            push_sorted_unique(
                &mut self.fallback_blocks,
                coverage.block_id,
                "adaptive V3 fallback block list",
            )?;
        }
        Ok(())
    }

    fn finish(
        self,
        key: IndexerV3CandidateKey,
        summary: Option<CandidatePostingSummary>,
    ) -> Result<IndexerV3CandidateBlocks> {
        ensure!(
            matches!(
                (key, summary),
                (IndexerV3CandidateKey::RegistryId(_), Some(_))
                    | (IndexerV3CandidateKey::RegistryAbsent, None)
            ),
            "adaptive V3 candidate key differs from its posting receipt"
        );
        let (absence_is_complete, cpi_role_bits_are_complete, read) = match summary {
            Some(summary) => {
                ensure!(
                    self.postings_visited == summary.postings,
                    "adaptive V3 posting visitor count differs from its receipt"
                );
                ensure!(
                    self.account_incomplete_transactions == summary.incomplete_account_transactions,
                    "adaptive V3 account-incomplete coverage count differs from its control receipt"
                );
                ensure!(
                    self.cpi_incomplete_transactions == summary.incomplete_cpi_transactions,
                    "adaptive V3 CPI-incomplete coverage count differs from its control receipt"
                );
                ensure!(
                    summary.absence_is_complete == (self.account_incomplete_transactions == 0),
                    "adaptive V3 account completeness flag differs from its coverage records"
                );
                ensure!(
                    summary.cpi_role_bits_are_complete == (self.cpi_incomplete_transactions == 0),
                    "adaptive V3 CPI completeness flag differs from its coverage records"
                );
                (
                    summary.absence_is_complete,
                    summary.cpi_role_bits_are_complete,
                    IndexerV3CandidateReadStats {
                        pages_read: summary.pages_read,
                        read_calls: summary.read_calls,
                        stored_bytes: summary.stored_bytes,
                        decoded_bytes: summary.decoded_bytes,
                    },
                )
            }
            None => {
                ensure!(
                    self.postings_visited == 0
                        && self.posting_blocks_visited == 0
                        && self.positive_postings == 0
                        && self.positive_blocks.is_empty(),
                    "registry-absent adaptive V3 candidate has positive postings"
                );
                (
                    self.account_incomplete_transactions == 0,
                    self.cpi_incomplete_transactions == 0,
                    IndexerV3CandidateReadStats {
                        pages_read: 0,
                        read_calls: 0,
                        stored_bytes: 0,
                        decoded_bytes: 0,
                    },
                )
            }
        };

        let account_only_incomplete_transactions = self
            .account_incomplete_transactions
            .checked_sub(self.both_incomplete_transactions)
            .context("adaptive V3 account-only coverage count underflow")?;
        let cpi_only_incomplete_transactions = self
            .cpi_incomplete_transactions
            .checked_sub(self.both_incomplete_transactions)
            .context("adaptive V3 CPI-only coverage count underflow")?;
        let positive_blocks = usize_to_u64(
            self.positive_blocks.len(),
            "adaptive V3 positive block count",
        )?;
        let policy_fallback_blocks = usize_to_u64(
            self.fallback_blocks.len(),
            "adaptive V3 fallback block count",
        )?;
        let (block_ids, overlap_blocks) =
            merge_sorted_unique(&self.positive_blocks, &self.fallback_blocks)?;
        let candidate_blocks = usize_to_u64(block_ids.len(), "adaptive V3 candidate block count")?;

        Ok(IndexerV3CandidateBlocks {
            key,
            policy: self.policy,
            validated_role_mask: ROLE_MASK,
            positive_role_mask: self.policy.positive_role_mask(),
            geometry: self.geometry,
            coverage: IndexerV3CandidateCoverage {
                sparse_transactions: self.sparse_transactions,
                account_incomplete_transactions: self.account_incomplete_transactions,
                cpi_incomplete_transactions: self.cpi_incomplete_transactions,
                account_only_incomplete_transactions,
                cpi_only_incomplete_transactions,
                account_and_cpi_incomplete_transactions: self.both_incomplete_transactions,
                policy_fallback_transactions: self.fallback_transactions,
                policy_fallback_blocks,
                fallback_includes_account_incomplete: self
                    .policy
                    .falls_back_for_account_incomplete(),
                fallback_includes_cpi_incomplete: self.policy.falls_back_for_cpi_incomplete(),
                absence_is_complete,
                cpi_role_bits_are_complete,
            },
            counts: IndexerV3CandidateCounts {
                postings_visited: self.postings_visited,
                posting_blocks_visited: self.posting_blocks_visited,
                positive_postings: self.positive_postings,
                positive_blocks,
                positive_fallback_overlap_blocks: overlap_blocks,
                candidate_blocks,
            },
            read,
            block_ids,
        })
    }
}

fn validate_position(
    block_id: u32,
    tx_index: u32,
    selected_blocks: u64,
    previous: Option<(u32, u32)>,
    context: &'static str,
) -> Result<()> {
    ensure!(
        u64::from(block_id) < selected_blocks,
        "{context} block ID {block_id} exceeds selected block geometry {selected_blocks}"
    );
    if let Some(previous) = previous {
        ensure!(
            (block_id, tx_index) > previous,
            "{context} are not in strict transaction order"
        );
    }
    Ok(())
}

fn validate_coverage_pair(
    account_coverage: u8,
    cpi_coverage: u8,
    require_incomplete: bool,
) -> Result<()> {
    ensure!(
        account_coverage <= ACCOUNT_COVERAGE_MAX,
        "adaptive V3 record has unknown account coverage state {account_coverage}"
    );
    ensure!(
        cpi_coverage <= CPI_COVERAGE_MAX,
        "adaptive V3 record has unknown CPI coverage state {cpi_coverage}"
    );
    ensure!(
        !require_incomplete
            || account_coverage != ACCOUNT_COVERAGE_COMPLETE
            || cpi_coverage != CPI_COVERAGE_COMPLETE,
        "adaptive V3 sparse coverage lane contains a complete transaction"
    );
    Ok(())
}

fn checked_increment(value: &mut u64, context: &'static str) -> Result<()> {
    *value = value
        .checked_add(1)
        .with_context(|| format!("{context} overflow"))?;
    Ok(())
}

fn push_sorted_unique(output: &mut Vec<u32>, block_id: u32, context: &'static str) -> Result<()> {
    if let Some(previous) = output.last().copied() {
        ensure!(block_id >= previous, "{context} is not sorted");
        if block_id == previous {
            return Ok(());
        }
    }
    output
        .try_reserve(1)
        .with_context(|| format!("reserve {context}"))?;
    output.push(block_id);
    Ok(())
}

fn merge_sorted_unique(left: &[u32], right: &[u32]) -> Result<(Vec<u32>, u64)> {
    validate_sorted_unique(left, "adaptive V3 positive block list")?;
    validate_sorted_unique(right, "adaptive V3 fallback block list")?;
    let capacity = left
        .len()
        .checked_add(right.len())
        .context("adaptive V3 merged block capacity overflow")?;
    let mut merged = Vec::new();
    merged
        .try_reserve(capacity)
        .context("reserve adaptive V3 merged block list")?;
    let (mut left_index, mut right_index, mut overlap) = (0, 0, 0_u64);
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => {
                merged.push(left[left_index]);
                left_index += 1;
            }
            std::cmp::Ordering::Greater => {
                merged.push(right[right_index]);
                right_index += 1;
            }
            std::cmp::Ordering::Equal => {
                merged.push(left[left_index]);
                left_index += 1;
                right_index += 1;
                checked_increment(&mut overlap, "adaptive V3 candidate overlap count")?;
            }
        }
    }
    merged.extend_from_slice(&left[left_index..]);
    merged.extend_from_slice(&right[right_index..]);
    Ok((merged, overlap))
}

fn validate_sorted_unique(values: &[u32], context: &'static str) -> Result<()> {
    ensure!(
        values.windows(2).all(|pair| pair[0] < pair[1]),
        "{context} is not sorted and unique"
    );
    Ok(())
}

fn usize_to_u64(value: usize, context: &'static str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{context} exceeds u64"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn geometry() -> IndexerV3CandidateGeometry {
        IndexerV3CandidateGeometry::from_parts(900, 432_000, 20, 8, 40).unwrap()
    }

    fn summary(
        postings: u64,
        incomplete_accounts: u64,
        incomplete_cpi: u64,
    ) -> CandidatePostingSummary {
        CandidatePostingSummary {
            postings,
            pages_read: 2,
            read_calls: 2,
            stored_bytes: 120,
            decoded_bytes: 240,
            incomplete_account_transactions: incomplete_accounts,
            incomplete_cpi_transactions: incomplete_cpi,
            absence_is_complete: incomplete_accounts == 0,
            cpi_role_bits_are_complete: incomplete_cpi == 0,
        }
    }

    fn posting(block_id: u32, tx_index: u32, roles: u8) -> AdaptiveV3ResolvedPosting {
        AdaptiveV3ResolvedPosting {
            block_id,
            tx_index,
            roles,
            account_coverage: 0,
            cpi_coverage: 0,
        }
    }

    fn coverage(
        block_id: u32,
        tx_index: u32,
        account_coverage: u8,
        cpi_coverage: u8,
    ) -> AdaptiveV3ResolvedCoverage {
        AdaptiveV3ResolvedCoverage {
            block_id,
            tx_index,
            account_coverage,
            cpi_coverage,
        }
    }

    fn absent_result(
        policy: IndexerV3CandidatePolicy,
        coverage_records: &[AdaptiveV3ResolvedCoverage],
    ) -> IndexerV3CandidateBlocks {
        let mut accumulator = CandidateAccumulator::new(geometry(), policy);
        for record in coverage_records {
            accumulator.accept_coverage(*record).unwrap();
        }
        accumulator
            .finish(IndexerV3CandidateKey::RegistryAbsent, None)
            .unwrap()
    }

    #[test]
    fn absent_registry_key_with_complete_coverage_has_no_candidates() {
        for policy in [
            IndexerV3CandidatePolicy::SignerWallet,
            IndexerV3CandidatePolicy::ReachedProgram,
        ] {
            let result = absent_result(policy, &[]);
            assert_eq!(result.key, IndexerV3CandidateKey::RegistryAbsent);
            assert!(result.block_ids.is_empty());
            assert_eq!(result.counts.candidate_blocks, 0);
            assert!(result.coverage.absence_is_complete);
            assert!(result.coverage.cpi_role_bits_are_complete);
            assert_eq!(result.read.read_calls, 0);
        }
    }

    #[test]
    fn absent_signer_and_program_keys_keep_account_incomplete_blocks() {
        let incomplete = [coverage(2, 0, 1, 0), coverage(4, 1, 3, 0)];
        for policy in [
            IndexerV3CandidatePolicy::SignerWallet,
            IndexerV3CandidatePolicy::ReachedProgram,
        ] {
            let result = absent_result(policy, &incomplete);
            assert_eq!(result.block_ids, [2, 4]);
            assert_eq!(result.counts.positive_postings, 0);
            assert_eq!(result.coverage.policy_fallback_transactions, 2);
            assert_eq!(result.coverage.policy_fallback_blocks, 2);
            assert!(!result.coverage.absence_is_complete);
        }
    }

    #[test]
    fn absent_reached_program_key_keeps_cpi_only_incomplete_block() {
        let incomplete = [coverage(3, 0, 0, 2)];
        let result = absent_result(IndexerV3CandidatePolicy::ReachedProgram, &incomplete);
        assert_eq!(result.block_ids, [3]);
        assert_eq!(result.coverage.policy_fallback_transactions, 1);
        assert!(result.coverage.absence_is_complete);
        assert!(!result.coverage.cpi_role_bits_are_complete);

        let signer = absent_result(IndexerV3CandidatePolicy::SignerWallet, &incomplete);
        assert!(signer.block_ids.is_empty());
    }

    #[test]
    fn lean_role_block_summary_preserves_exact_candidate_counts() {
        let mut accumulator =
            CandidateAccumulator::new(geometry(), IndexerV3CandidatePolicy::ReachedProgram);
        accumulator
            .accept_matched_block(AdaptiveV3RoleMatchedBlock {
                block_id: 1,
                matching_postings: 2,
            })
            .unwrap();
        accumulator
            .accept_matched_block(AdaptiveV3RoleMatchedBlock {
                block_id: 3,
                matching_postings: 1,
            })
            .unwrap();
        let summary = AdaptiveV3RoleBlockVisitSummary {
            postings: 9,
            posting_blocks: 4,
            matching_postings: 3,
            matching_blocks: 2,
            pages_read: 2,
            read_calls: 2,
            stored_bytes: 120,
            decoded_bytes: 240,
            incomplete_account_transactions: 0,
            incomplete_cpi_transactions: 0,
            absence_is_complete: true,
            cpi_role_bits_are_complete: true,
        };
        accumulator.accept_role_summary(summary).unwrap();

        let result = accumulator
            .finish(
                IndexerV3CandidateKey::RegistryId(8),
                Some(CandidatePostingSummary::from(summary)),
            )
            .unwrap();
        assert_eq!(result.block_ids, [1, 3]);
        assert_eq!(result.counts.postings_visited, 9);
        assert_eq!(result.counts.posting_blocks_visited, 4);
        assert_eq!(result.counts.positive_postings, 3);
        assert_eq!(result.counts.positive_blocks, 2);
        assert_eq!(result.read.read_calls, 2);
    }

    #[test]
    fn signer_policy_adds_only_account_incomplete_fallback_blocks() {
        let mut accumulator =
            CandidateAccumulator::new(geometry(), IndexerV3CandidatePolicy::SignerWallet);
        accumulator
            .accept_posting(posting(1, 0, ROLE_SIGNER))
            .unwrap();
        accumulator
            .accept_posting(posting(1, 1, ROLE_TOP_LEVEL_PROGRAM))
            .unwrap();
        accumulator
            .accept_posting(posting(3, 0, ROLE_SIGNER))
            .unwrap();
        accumulator.accept_coverage(coverage(2, 0, 1, 0)).unwrap();
        accumulator.accept_coverage(coverage(3, 1, 0, 2)).unwrap();
        accumulator.accept_coverage(coverage(4, 0, 3, 4)).unwrap();

        let result = accumulator
            .finish(IndexerV3CandidateKey::RegistryId(7), Some(summary(3, 2, 2)))
            .unwrap();
        assert_eq!(result.block_ids, [1, 2, 3, 4]);
        assert_eq!(result.counts.posting_blocks_visited, 2);
        assert_eq!(result.counts.positive_postings, 2);
        assert_eq!(result.counts.positive_blocks, 2);
        assert_eq!(result.counts.positive_fallback_overlap_blocks, 0);
        assert_eq!(result.counts.candidate_blocks, 4);
        assert_eq!(result.coverage.sparse_transactions, 3);
        assert_eq!(result.coverage.policy_fallback_transactions, 2);
        assert_eq!(result.coverage.policy_fallback_blocks, 2);
        assert_eq!(result.coverage.account_only_incomplete_transactions, 1);
        assert_eq!(result.coverage.cpi_only_incomplete_transactions, 1);
        assert_eq!(result.coverage.account_and_cpi_incomplete_transactions, 1);
        assert!(!result.coverage.fallback_includes_cpi_incomplete);
    }

    #[test]
    fn program_policy_adds_account_and_cpi_incomplete_fallback_blocks() {
        let mut accumulator =
            CandidateAccumulator::new(geometry(), IndexerV3CandidatePolicy::ReachedProgram);
        accumulator
            .accept_posting(posting(1, 0, ROLE_TOP_LEVEL_PROGRAM))
            .unwrap();
        accumulator
            .accept_posting(posting(3, 0, ROLE_CPI_PROGRAM))
            .unwrap();
        accumulator.accept_coverage(coverage(2, 0, 1, 0)).unwrap();
        accumulator.accept_coverage(coverage(3, 1, 0, 2)).unwrap();
        accumulator.accept_coverage(coverage(4, 0, 3, 4)).unwrap();

        let result = accumulator
            .finish(IndexerV3CandidateKey::RegistryId(8), Some(summary(2, 2, 2)))
            .unwrap();
        assert_eq!(result.block_ids, [1, 2, 3, 4]);
        assert_eq!(result.coverage.policy_fallback_transactions, 3);
        assert_eq!(result.coverage.policy_fallback_blocks, 3);
        assert_eq!(result.counts.positive_fallback_overlap_blocks, 1);
        assert!(result.coverage.fallback_includes_cpi_incomplete);
    }

    #[test]
    fn rejects_unknown_role_bits_and_invalid_coverage_states() {
        let mut accumulator =
            CandidateAccumulator::new(geometry(), IndexerV3CandidatePolicy::SignerWallet);
        assert!(
            accumulator
                .accept_posting(posting(1, 0, ROLE_MASK | 0x80))
                .is_err()
        );

        let mut accumulator =
            CandidateAccumulator::new(geometry(), IndexerV3CandidatePolicy::ReachedProgram);
        assert!(accumulator.accept_coverage(coverage(1, 0, 4, 0)).is_err());
        assert!(accumulator.accept_coverage(coverage(1, 0, 0, 0)).is_err());
    }

    #[test]
    fn rejects_out_of_order_or_out_of_geometry_records() {
        let mut accumulator =
            CandidateAccumulator::new(geometry(), IndexerV3CandidatePolicy::SignerWallet);
        accumulator
            .accept_posting(posting(2, 1, ROLE_SIGNER))
            .unwrap();
        assert!(
            accumulator
                .accept_posting(posting(2, 0, ROLE_SIGNER))
                .is_err()
        );

        let mut accumulator =
            CandidateAccumulator::new(geometry(), IndexerV3CandidatePolicy::SignerWallet);
        assert!(
            accumulator
                .accept_posting(posting(8, 0, ROLE_SIGNER))
                .is_err()
        );
    }

    #[test]
    fn rejects_receipts_that_differ_from_streamed_coverage() {
        let accumulator =
            CandidateAccumulator::new(geometry(), IndexerV3CandidatePolicy::SignerWallet);
        assert!(
            accumulator
                .finish(IndexerV3CandidateKey::RegistryId(1), Some(summary(1, 0, 0)),)
                .is_err()
        );

        let mut accumulator =
            CandidateAccumulator::new(geometry(), IndexerV3CandidatePolicy::SignerWallet);
        accumulator.accept_coverage(coverage(1, 0, 1, 0)).unwrap();
        assert!(
            accumulator
                .finish(IndexerV3CandidateKey::RegistryId(1), Some(summary(0, 0, 0)),)
                .is_err()
        );
    }

    #[test]
    fn merge_reports_overlap_and_rejects_nonunique_input() {
        let (merged, overlap) = merge_sorted_unique(&[1, 3, 8], &[2, 3, 5, 8]).unwrap();
        assert_eq!(merged, [1, 2, 3, 5, 8]);
        assert_eq!(overlap, 2);
        assert!(merge_sorted_unique(&[1, 1], &[]).is_err());
        assert!(merge_sorted_unique(&[2, 1], &[]).is_err());
    }

    #[test]
    fn geometry_and_count_overflows_fail_closed() {
        assert!(IndexerV3CandidateGeometry::from_parts(u64::MAX, 2, 1, 1, 1).is_err());
        let mut count = u64::MAX;
        assert!(checked_increment(&mut count, "test count").is_err());
    }
}
