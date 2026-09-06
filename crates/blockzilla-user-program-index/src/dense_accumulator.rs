//! Signer-dense accumulation for the user-program index build pipeline.
//!
//! The accumulator is deliberately single-owner: decoder workers should send
//! bounded relation batches to one accumulator thread.  An upstream channel
//! must also be bounded; with capacity `C` and at most `B` pairs per batch,
//! queued relation payload is approximately `C * B * 8` bytes, excluding the
//! channel and allocation headers.  [`DenseAccumulator::max_batch_pairs`]
//! exposes `B` and every batch entry point rejects a larger message.
//!
//! Empty signer slots cost one `u32` head (four bytes), rather than an empty
//! `SmallVec` per signer.  Each distinct `(signer, program)` relation is one
//! eight-byte linked node.  Lists are kept sorted and unique *while inserting*;
//! repeated vote/bot traffic therefore never creates duplicate nodes.  A
//! registry-sized bitset tracks distinct programs.  The reusable batch scratch
//! is bounded by `B` pairs.  `Vec` capacity slack and allocator bookkeeping are
//! not included in those figures.
//!
//! Dense ranks are zero-based.  Rank order must match ascending signer registry
//! id order (as produced by `SignerRank::iter_ids`).  The accumulator does not
//! duplicate that rank-to-id mapping: [`DenseAccumulator::ranked_wallets`] can
//! be joined with it directly, while [`DenseAccumulator::wallets`] provides a
//! checked streaming adapter.  Both traverse the linked lists in place and do
//! not materialize a second full relation array.

use std::iter::FusedIterator;
use std::num::NonZeroUsize;

use thiserror::Error;

const NO_NODE: u32 = u32::MAX;

/// Bytes in the accumulator's fixed table for each discovered signer.
pub const EMPTY_SIGNER_SLOT_BYTES: usize = size_of::<u32>();

/// Bytes in the relation arena for each distinct signer/program edge.
pub const DISTINCT_RELATION_BYTES: usize = size_of::<RelationNode>();

/// A newly recorded relation or one already present in the signer list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenseRecordOutcome {
    Inserted,
    Duplicate,
}

/// Aggregate result from one bounded batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchStats {
    /// Number of input pairs, including repetitions within the batch.
    pub input_relations: usize,
    /// Number of unique `(dense_rank, program_id)` pairs within this batch.
    pub batch_distinct_relations: usize,
    /// Relations that were not already in the accumulator.
    pub inserted_relations: usize,
    /// Input repetitions plus relations already recorded by earlier batches.
    pub duplicate_relations: usize,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DenseAccumulatorError {
    #[error("dense signer rank {rank} is outside 0..{signer_count}")]
    InvalidRank { rank: u32, signer_count: u32 },
    #[error("program registry id {program_id} is outside 1..={max_program_id}")]
    InvalidProgram {
        program_id: u32,
        max_program_id: u32,
    },
    #[error("signer registry id {signer_id} is absent from the persisted signer set")]
    UnknownSigner { signer_id: u32 },
    #[error("relation batch contains {actual} pairs; configured maximum is {maximum}")]
    BatchTooLarge { actual: usize, maximum: usize },
    #[error("the u32-linked relation arena is full")]
    RelationCapacityExceeded,
}

/// A malformed dense-rank to signer-registry-id stream.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WalletMappingError {
    #[error("wallet mapping yielded dense rank {actual}; expected {expected}")]
    UnexpectedRank { expected: u32, actual: u32 },
    #[error("wallet mapping ended after {actual} ranks; expected {expected}")]
    Incomplete { expected: u32, actual: u32 },
    #[error(
        "wallet registry ids must be nonzero and strictly increasing; got {wallet_id} after {previous_wallet_id}"
    )]
    NonIncreasingWalletId {
        previous_wallet_id: u32,
        wallet_id: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Relation {
    dense_rank: u32,
    program_id: u32,
}

/// Eight bytes per *distinct* relation. `NO_NODE` terminates a signer list.
#[derive(Debug, Clone, Copy)]
struct RelationNode {
    program_id: u32,
    next: u32,
}

/// Single-owner, signer-dense relation accumulator.
#[derive(Debug)]
pub struct DenseAccumulator {
    heads: Vec<u32>,
    nodes: Vec<RelationNode>,
    program_words: Vec<u64>,
    max_program_id: u32,
    max_batch_pairs: NonZeroUsize,
    batch_scratch: Vec<Relation>,
    wallet_count: u32,
    distinct_program_count: u32,
    last_relation: Option<Relation>,
}

impl DenseAccumulator {
    /// Allocate the four-byte head table and the program-presence bitset.
    ///
    /// `max_batch_pairs` is the hard per-message backpressure boundary.  This
    /// constructor does not preallocate relation nodes or batch scratch, so a
    /// 10.6M-signer epoch starts near the documented fixed-size floor rather
    /// than immediately reserving the projected relation peak.
    pub fn new(signer_count: u32, max_program_id: u32, max_batch_pairs: NonZeroUsize) -> Self {
        let program_words = if max_program_id == 0 {
            Vec::new()
        } else {
            vec![0; (max_program_id as usize).div_ceil(64)]
        };
        Self {
            heads: vec![NO_NODE; signer_count as usize],
            nodes: Vec::new(),
            program_words,
            max_program_id,
            max_batch_pairs,
            batch_scratch: Vec::new(),
            wallet_count: 0,
            distinct_program_count: 0,
            last_relation: None,
        }
    }

    /// Optionally reserve a measured relation estimate without changing the
    /// logical bounds.  The estimate cannot exceed the u32 node-index space.
    pub fn reserve_relations(&mut self, additional: usize) -> Result<(), DenseAccumulatorError> {
        if self.nodes.len().saturating_add(additional) > NO_NODE as usize {
            return Err(DenseAccumulatorError::RelationCapacityExceeded);
        }
        self.nodes.reserve(additional);
        Ok(())
    }

    pub fn signer_count(&self) -> u32 {
        self.heads.len() as u32
    }

    pub fn max_program_id(&self) -> u32 {
        self.max_program_id
    }

    pub fn max_batch_pairs(&self) -> NonZeroUsize {
        self.max_batch_pairs
    }

    /// Number of signers with at least one relation.  Discovered signers with
    /// no successful reached program are deliberately absent from output.
    pub fn wallet_count(&self) -> u32 {
        self.wallet_count
    }

    /// Number of distinct signer/program edges (and therefore arena nodes).
    pub fn relation_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn distinct_program_count(&self) -> u32 {
        self.distinct_program_count
    }

    /// Record one relation, preserving sorted uniqueness at insertion time.
    pub fn record(
        &mut self,
        dense_rank: u32,
        program_id: u32,
    ) -> Result<DenseRecordOutcome, DenseAccumulatorError> {
        self.validate_relation(dense_rank, program_id)?;
        let relation = Relation {
            dense_rank,
            program_id,
        };
        if self.last_relation == Some(relation) {
            return Ok(DenseRecordOutcome::Duplicate);
        }
        if self.nodes.len() == NO_NODE as usize {
            return Err(DenseAccumulatorError::RelationCapacityExceeded);
        }
        Ok(self.record_validated(relation))
    }

    /// Record a bounded batch whose first tuple item is a zero-based dense
    /// signer rank.  The input remains untouched.  A reusable internal scratch
    /// sorts and deduplicates the batch before touching signer lists.
    pub fn record_rank_batch(
        &mut self,
        relations: &[(u32, u32)],
    ) -> Result<BatchStats, DenseAccumulatorError> {
        self.prepare_batch(relations, Some, false)
    }

    /// Record a bounded batch whose first tuple item is a signer registry id.
    ///
    /// `rank` should normally be `|id| signer_rank.rank(id)`.  Every input is
    /// resolved and validated before the accumulator is changed, so malformed
    /// batches do not partially publish relations.
    pub fn record_signer_batch(
        &mut self,
        relations: &[(u32, u32)],
        rank: impl FnMut(u32) -> Option<u32>,
    ) -> Result<BatchStats, DenseAccumulatorError> {
        self.prepare_batch(relations, rank, true)
    }

    /// Nonempty wallets in ascending dense-rank order.  Each program iterator
    /// is already sorted and unique and walks the existing nodes directly.
    pub fn ranked_wallets(&self) -> RankedWallets<'_> {
        RankedWallets {
            accumulator: self,
            next_rank: 0,
        }
    }

    /// Attach ascending registry wallet ids to ranked output without storing a
    /// second rank-to-id vector.  `ranked_ids` must cover every dense rank in
    /// order, including signers with no relations; `SignerRank::iter_ids()` is
    /// the intended source.
    pub fn wallets<I>(&self, ranked_ids: I) -> Wallets<'_, I::IntoIter>
    where
        I: IntoIterator<Item = (u32, u32)>,
    {
        Wallets {
            accumulator: self,
            ranked_ids: ranked_ids.into_iter(),
            expected_rank: 0,
            previous_wallet_id: 0,
            finished: false,
        }
    }

    /// Distinct program registry ids in ascending order.
    pub fn program_ids(&self) -> impl Iterator<Item = u32> + '_ {
        (1..=self.max_program_id).filter(|id| {
            let index = (*id - 1) as usize;
            self.program_words[index / 64] & (1u64 << (index % 64)) != 0
        })
    }

    fn prepare_batch(
        &mut self,
        relations: &[(u32, u32)],
        mut rank: impl FnMut(u32) -> Option<u32>,
        signer_ids: bool,
    ) -> Result<BatchStats, DenseAccumulatorError> {
        let maximum = self.max_batch_pairs.get();
        if relations.len() > maximum {
            return Err(DenseAccumulatorError::BatchTooLarge {
                actual: relations.len(),
                maximum,
            });
        }

        self.batch_scratch.clear();
        self.batch_scratch.reserve(relations.len());
        for &(source_id, program_id) in relations {
            let dense_rank = rank(source_id).ok_or_else(|| {
                if signer_ids {
                    DenseAccumulatorError::UnknownSigner {
                        signer_id: source_id,
                    }
                } else {
                    DenseAccumulatorError::InvalidRank {
                        rank: source_id,
                        signer_count: self.signer_count(),
                    }
                }
            })?;
            self.validate_relation(dense_rank, program_id)?;
            self.batch_scratch.push(Relation {
                dense_rank,
                program_id,
            });
        }

        self.batch_scratch.sort_unstable();
        self.batch_scratch.dedup();
        let batch_distinct_relations = self.batch_scratch.len();
        if self.nodes.len().saturating_add(batch_distinct_relations) > NO_NODE as usize {
            return Err(DenseAccumulatorError::RelationCapacityExceeded);
        }

        // Taking the reusable scratch avoids aliasing it while mutating the
        // relation arena.  Capacity is restored before returning.
        let mut scratch = std::mem::take(&mut self.batch_scratch);
        let mut inserted_relations = 0usize;
        for relation in scratch.iter().copied() {
            if self.record_validated(relation) == DenseRecordOutcome::Inserted {
                inserted_relations += 1;
            }
        }
        scratch.clear();
        self.batch_scratch = scratch;

        Ok(BatchStats {
            input_relations: relations.len(),
            batch_distinct_relations,
            inserted_relations,
            duplicate_relations: relations.len() - inserted_relations,
        })
    }

    #[inline]
    fn validate_relation(
        &self,
        dense_rank: u32,
        program_id: u32,
    ) -> Result<(), DenseAccumulatorError> {
        if dense_rank >= self.signer_count() {
            return Err(DenseAccumulatorError::InvalidRank {
                rank: dense_rank,
                signer_count: self.signer_count(),
            });
        }
        if program_id == 0 || program_id > self.max_program_id {
            return Err(DenseAccumulatorError::InvalidProgram {
                program_id,
                max_program_id: self.max_program_id,
            });
        }
        Ok(())
    }

    fn record_validated(&mut self, relation: Relation) -> DenseRecordOutcome {
        if self.last_relation == Some(relation) {
            return DenseRecordOutcome::Duplicate;
        }
        self.last_relation = Some(relation);

        let rank = relation.dense_rank as usize;
        let mut previous = NO_NODE;
        let mut current = self.heads[rank];
        while current != NO_NODE {
            let node = self.nodes[current as usize];
            match node.program_id.cmp(&relation.program_id) {
                std::cmp::Ordering::Equal => return DenseRecordOutcome::Duplicate,
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Less => {
                    previous = current;
                    current = node.next;
                }
            }
        }

        debug_assert!(self.nodes.len() < NO_NODE as usize);
        let node_index = self.nodes.len() as u32;
        self.nodes.push(RelationNode {
            program_id: relation.program_id,
            next: current,
        });
        if previous == NO_NODE {
            if self.heads[rank] == NO_NODE {
                self.wallet_count += 1;
            }
            self.heads[rank] = node_index;
        } else {
            self.nodes[previous as usize].next = node_index;
        }
        self.mark_program(relation.program_id);
        DenseRecordOutcome::Inserted
    }

    #[inline]
    fn mark_program(&mut self, program_id: u32) {
        let index = (program_id - 1) as usize;
        let mask = 1u64 << (index % 64);
        let word = &mut self.program_words[index / 64];
        if *word & mask == 0 {
            *word |= mask;
            self.distinct_program_count += 1;
        }
    }

    fn programs_from(&self, head: u32) -> ProgramIds<'_> {
        ProgramIds {
            nodes: &self.nodes,
            next: head,
        }
    }
}

/// One nonempty dense-rank wallet.
#[derive(Clone, Copy)]
pub struct RankedWallet<'a> {
    dense_rank: u32,
    accumulator: &'a DenseAccumulator,
    head: u32,
}

impl std::fmt::Debug for RankedWallet<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RankedWallet")
            .field("dense_rank", &self.dense_rank)
            .finish_non_exhaustive()
    }
}

impl RankedWallet<'_> {
    pub fn dense_rank(&self) -> u32 {
        self.dense_rank
    }

    pub fn programs(&self) -> ProgramIds<'_> {
        self.accumulator.programs_from(self.head)
    }
}

pub struct RankedWallets<'a> {
    accumulator: &'a DenseAccumulator,
    next_rank: u32,
}

impl<'a> Iterator for RankedWallets<'a> {
    type Item = RankedWallet<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_rank < self.accumulator.signer_count() {
            let dense_rank = self.next_rank;
            self.next_rank += 1;
            let head = self.accumulator.heads[dense_rank as usize];
            if head != NO_NODE {
                return Some(RankedWallet {
                    dense_rank,
                    accumulator: self.accumulator,
                    head,
                });
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            0,
            Some((self.accumulator.signer_count() - self.next_rank) as usize),
        )
    }
}

impl FusedIterator for RankedWallets<'_> {}

/// One nonempty wallet with its real registry id attached.
#[derive(Clone, Copy)]
pub struct WalletPrograms<'a> {
    wallet_id: u32,
    dense_rank: u32,
    accumulator: &'a DenseAccumulator,
    head: u32,
}

impl std::fmt::Debug for WalletPrograms<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WalletPrograms")
            .field("wallet_id", &self.wallet_id)
            .field("dense_rank", &self.dense_rank)
            .finish_non_exhaustive()
    }
}

impl WalletPrograms<'_> {
    pub fn wallet_id(&self) -> u32 {
        self.wallet_id
    }

    pub fn dense_rank(&self) -> u32 {
        self.dense_rank
    }

    pub fn programs(&self) -> ProgramIds<'_> {
        self.accumulator.programs_from(self.head)
    }
}

/// Checked adapter from a full `(dense_rank, registry_id)` stream to nonempty
/// wallet output.  Mapping errors are terminal and the iterator is fused.
pub struct Wallets<'a, I> {
    accumulator: &'a DenseAccumulator,
    ranked_ids: I,
    expected_rank: u32,
    previous_wallet_id: u32,
    finished: bool,
}

impl<'a, I> Iterator for Wallets<'a, I>
where
    I: Iterator<Item = (u32, u32)>,
{
    type Item = Result<WalletPrograms<'a>, WalletMappingError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            let Some((rank, wallet_id)) = self.ranked_ids.next() else {
                self.finished = true;
                if self.expected_rank != self.accumulator.signer_count() {
                    return Some(Err(WalletMappingError::Incomplete {
                        expected: self.accumulator.signer_count(),
                        actual: self.expected_rank,
                    }));
                }
                return None;
            };
            if self.expected_rank >= self.accumulator.signer_count() {
                self.finished = true;
                return Some(Err(WalletMappingError::UnexpectedRank {
                    expected: self.accumulator.signer_count(),
                    actual: rank,
                }));
            }
            if rank != self.expected_rank {
                self.finished = true;
                return Some(Err(WalletMappingError::UnexpectedRank {
                    expected: self.expected_rank,
                    actual: rank,
                }));
            }
            if wallet_id == 0 || wallet_id <= self.previous_wallet_id {
                self.finished = true;
                return Some(Err(WalletMappingError::NonIncreasingWalletId {
                    previous_wallet_id: self.previous_wallet_id,
                    wallet_id,
                }));
            }

            self.expected_rank += 1;
            self.previous_wallet_id = wallet_id;
            let head = self.accumulator.heads[rank as usize];
            if head != NO_NODE {
                return Some(Ok(WalletPrograms {
                    wallet_id,
                    dense_rank: rank,
                    accumulator: self.accumulator,
                    head,
                }));
            }
        }
    }
}

impl<I> FusedIterator for Wallets<'_, I> where I: Iterator<Item = (u32, u32)> {}

/// Sorted, unique program ids for one wallet.  This directly follows arena
/// links and allocates no per-wallet scratch.
#[derive(Clone)]
pub struct ProgramIds<'a> {
    nodes: &'a [RelationNode],
    next: u32,
}

impl Iterator for ProgramIds<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == NO_NODE {
            return None;
        }
        let node = self.nodes[self.next as usize];
        self.next = node.next;
        Some(node.program_id)
    }
}

impl FusedIterator for ProgramIds<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn accumulator(signers: u32, programs: u32) -> DenseAccumulator {
        DenseAccumulator::new(signers, programs, NonZeroUsize::new(16).unwrap())
    }

    #[test]
    fn compact_layout_has_four_byte_empty_slots_and_eight_byte_edges() {
        assert_eq!(EMPTY_SIGNER_SLOT_BYTES, 4);
        assert_eq!(DISTINCT_RELATION_BYTES, 8);
    }

    #[test]
    fn insertion_keeps_each_wallet_sorted_and_unique() {
        let mut accumulator = accumulator(4, 100);
        assert_eq!(
            accumulator.record(2, 50).unwrap(),
            DenseRecordOutcome::Inserted
        );
        accumulator.record(2, 10).unwrap();
        accumulator.record(2, 90).unwrap();
        accumulator.record(2, 30).unwrap();
        assert_eq!(
            accumulator.record(2, 50).unwrap(),
            DenseRecordOutcome::Duplicate
        );

        let wallet = accumulator.ranked_wallets().next().unwrap();
        assert_eq!(wallet.dense_rank(), 2);
        assert_eq!(wallet.programs().collect::<Vec<_>>(), [10, 30, 50, 90]);
        assert_eq!(accumulator.wallet_count(), 1);
        assert_eq!(accumulator.relation_count(), 4);
    }

    #[test]
    fn high_repeat_traffic_never_appends_duplicate_nodes() {
        let mut accumulator = accumulator(1, 10);
        for _ in 0..10_000 {
            accumulator.record(0, 7).unwrap();
        }
        let repeated_batch = [(0, 7); 16];
        for _ in 0..10_000 {
            let stats = accumulator.record_rank_batch(&repeated_batch).unwrap();
            assert_eq!(stats.inserted_relations, 0);
        }
        assert_eq!(accumulator.relation_count(), 1);
        assert_eq!(accumulator.wallet_count(), 1);
        assert_eq!(accumulator.distinct_program_count(), 1);
    }

    #[test]
    fn batch_is_bounded_deduplicated_and_reports_work() {
        let mut accumulator = accumulator(3, 20);
        let batch = [(2, 9), (0, 3), (2, 9), (0, 1), (0, 3)];
        let stats = accumulator.record_rank_batch(&batch).unwrap();
        assert_eq!(
            stats,
            BatchStats {
                input_relations: 5,
                batch_distinct_relations: 3,
                inserted_relations: 3,
                duplicate_relations: 2,
            }
        );
        assert_eq!(accumulator.relation_count(), 3);

        let oversized = vec![(0, 1); 17];
        assert_eq!(
            accumulator.record_rank_batch(&oversized),
            Err(DenseAccumulatorError::BatchTooLarge {
                actual: 17,
                maximum: 16,
            })
        );
        assert_eq!(accumulator.relation_count(), 3);
    }

    #[test]
    fn signer_id_batches_resolve_before_mutating() {
        let mut accumulator = accumulator(2, 10);
        let ranks = |id| match id {
            11 => Some(0),
            42 => Some(1),
            _ => None,
        };
        accumulator
            .record_signer_batch(&[(42, 8), (11, 3)], ranks)
            .unwrap();
        assert_eq!(accumulator.relation_count(), 2);

        let error = accumulator
            .record_signer_batch(&[(11, 4), (99, 5)], ranks)
            .unwrap_err();
        assert_eq!(
            error,
            DenseAccumulatorError::UnknownSigner { signer_id: 99 }
        );
        assert_eq!(accumulator.relation_count(), 2);
    }

    #[test]
    fn invalid_rank_and_programs_are_rejected() {
        let mut accumulator = accumulator(2, 10);
        assert_eq!(
            accumulator.record(2, 1),
            Err(DenseAccumulatorError::InvalidRank {
                rank: 2,
                signer_count: 2,
            })
        );
        for program_id in [0, 11] {
            assert_eq!(
                accumulator.record(0, program_id),
                Err(DenseAccumulatorError::InvalidProgram {
                    program_id,
                    max_program_id: 10,
                })
            );
        }
        assert_eq!(accumulator.relation_count(), 0);

        assert_eq!(
            accumulator.record_rank_batch(&[(0, 2), (2, 3)]),
            Err(DenseAccumulatorError::InvalidRank {
                rank: 2,
                signer_count: 2,
            })
        );
        assert_eq!(accumulator.relation_count(), 0);
    }

    #[test]
    fn wallet_iteration_omits_empty_signers_and_attaches_registry_ids() {
        let mut accumulator = accumulator(4, 20);
        accumulator.record(1, 9).unwrap();
        accumulator.record(3, 7).unwrap();
        accumulator.record(3, 2).unwrap();

        let output = accumulator
            .wallets([(0, 10), (1, 20), (2, 25), (3, 90)])
            .map(|wallet| {
                let wallet = wallet.unwrap();
                (
                    wallet.wallet_id(),
                    wallet.dense_rank(),
                    wallet.programs().collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(output, [(20, 1, vec![9]), (90, 3, vec![2, 7])]);
    }

    #[test]
    fn wallet_mapping_is_checked_even_across_empty_signers() {
        let dense = accumulator(3, 10);
        let mut wallets = dense.wallets([(0, 2), (2, 8)]);
        assert_eq!(
            wallets.next().unwrap().unwrap_err(),
            WalletMappingError::UnexpectedRank {
                expected: 1,
                actual: 2,
            }
        );
        assert!(wallets.next().is_none());

        let mut wallets = dense.wallets([(0, 2), (1, 8)]);
        assert_eq!(
            wallets.next().unwrap().unwrap_err(),
            WalletMappingError::Incomplete {
                expected: 3,
                actual: 2,
            }
        );

        let empty = accumulator(0, 10);
        let mut wallets = empty.wallets([(0, 2)]);
        assert_eq!(
            wallets.next().unwrap().unwrap_err(),
            WalletMappingError::UnexpectedRank {
                expected: 0,
                actual: 0,
            }
        );
        assert!(wallets.next().is_none());
    }

    #[test]
    fn distinct_program_iteration_is_sorted() {
        let mut accumulator = accumulator(3, 100);
        accumulator.record(0, 99).unwrap();
        accumulator.record(1, 4).unwrap();
        accumulator.record(2, 99).unwrap();
        accumulator.record(2, 30).unwrap();
        assert_eq!(accumulator.distinct_program_count(), 3);
        assert_eq!(accumulator.program_ids().collect::<Vec<_>>(), [4, 30, 99]);
    }

    #[test]
    fn signer_rank_contract_round_trips_without_a_reverse_id_vector() {
        use crate::signer_rank::{SignerSetBinding, SignerSetBuilder};

        let mut signers = SignerSetBuilder::new(20).unwrap();
        signers.insert(3).unwrap();
        signers.insert(17).unwrap();
        let ranks = signers
            .finish(SignerSetBinding {
                registry_entries: 20,
                generation_digest: [1; 32],
                registry_size: 20 * 32,
                registry_sha256: [2; 32],
            })
            .unwrap();

        let mut accumulator = accumulator(ranks.signer_count(), 20);
        accumulator
            .record_signer_batch(&[(17, 10), (3, 4)], |id| ranks.rank(id))
            .unwrap();
        let output = accumulator
            .wallets(ranks.iter_ids())
            .map(|wallet| {
                let wallet = wallet.unwrap();
                (wallet.wallet_id(), wallet.programs().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        assert_eq!(output, [(3, vec![4]), (17, vec![10])]);
    }

    #[test]
    fn batched_accumulation_matches_a_sorted_set_oracle() {
        use std::collections::BTreeSet;

        const SIGNERS: u32 = 64;
        const PROGRAMS: u32 = 127;
        let mut accumulator =
            DenseAccumulator::new(SIGNERS, PROGRAMS, NonZeroUsize::new(32).unwrap());
        let mut oracle = vec![BTreeSet::new(); SIGNERS as usize];
        let mut seed = 0x8d26_5f17_4a93_cbe1u64;
        for _ in 0..1_000 {
            let mut batch = [(0, 0); 32];
            for relation in &mut batch {
                // Fixed-seed xorshift64 keeps the test deterministic and does
                // not add a random-number dependency to the production crate.
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let rank = (seed % u64::from(SIGNERS)) as u32;
                let program = ((seed >> 32) % u64::from(PROGRAMS) + 1) as u32;
                *relation = (rank, program);
                oracle[rank as usize].insert(program);
            }
            accumulator.record_rank_batch(&batch).unwrap();
        }

        let actual = accumulator
            .ranked_wallets()
            .map(|wallet| (wallet.dense_rank(), wallet.programs().collect::<Vec<_>>()))
            .collect::<Vec<_>>();
        let expected = oracle
            .iter()
            .enumerate()
            .filter(|(_, programs)| !programs.is_empty())
            .map(|(rank, programs)| (rank as u32, programs.iter().copied().collect::<Vec<_>>()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(
            accumulator.relation_count(),
            oracle.iter().map(BTreeSet::len).sum::<usize>()
        );
    }
}
