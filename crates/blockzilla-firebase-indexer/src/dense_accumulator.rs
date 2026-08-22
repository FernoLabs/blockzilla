//! Signer-dense accumulation for the Stage 3 Firewatch build pipeline.
//!
//! The accumulator is deliberately single-owner: decoder workers should send
//! bounded relation batches to one accumulator thread.  An upstream channel
//! must also be bounded; with capacity `C` and at most `B` pairs per batch,
//! queued relation payload is approximately
//! `C * B * BATCH_RELATION_BYTES`, excluding the channel and allocation
//! headers.  [`DenseAccumulator::max_batch_pairs`] exposes `B` and every batch
//! entry point rejects a larger message.
//!
//! Empty signer slots cost one `u32` head (four bytes), rather than an empty
//! `SmallVec` per signer.  Each distinct `(signer, program)` relation is one
//! 56-byte arena entry: an eight-byte linked node followed by a 48-byte usage
//! payload that omits the duplicate program id.  Lists are kept sorted and
//! unique *while inserting*; repeated traffic merges into the existing
//! aggregate and never creates duplicate nodes.  A registry-sized bitset tracks
//! distinct programs.  The reusable batch scratch is bounded by `B` aggregate
//! pairs.  `Vec` capacity slack and allocator bookkeeping are not included in
//! those figures.
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

use crate::format::{ProgramUsage, ProgramUsageError};

const NO_NODE: u32 = u32::MAX;

/// Bytes in the accumulator's fixed table for each discovered signer.
pub const EMPTY_SIGNER_SLOT_BYTES: usize = size_of::<u32>();

/// Bytes in the linked-node arena for each distinct signer/program edge.
pub const RELATION_NODE_BYTES: usize = size_of::<RelationNode>();

/// Bytes in the program-id-free usage payload for each distinct edge.
pub const RELATION_USAGE_BYTES: usize = size_of::<ProgramUsagePayload>();

/// Exact arena bytes for each distinct signer/program edge.
pub const DISTINCT_RELATION_BYTES: usize = size_of::<RelationEntry>();

/// Bytes in one decoder-to-accumulator batch entry.
pub const BATCH_RELATION_BYTES: usize = size_of::<(u32, ProgramUsage)>();

/// A newly recorded relation or one merged into an existing aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenseRecordOutcome {
    Inserted,
    /// The relation already existed and its usage aggregate was updated.
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
    /// Their usage aggregates were merged; they were not discarded.
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
    #[error("could not reserve memory for the {region}")]
    AllocationFailed { region: &'static str },
    #[error("invalid or overflowing program usage: {source}")]
    InvalidProgramUsage {
        #[from]
        source: ProgramUsageError,
    },
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
#[repr(C)]
struct RelationNode {
    program_id: u32,
    next: u32,
}

/// The aggregate fields whose program id already lives in [`RelationNode`].
#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct ProgramUsagePayload {
    direct_instruction_count: u32,
    inner_instruction_count: u32,
    transaction_count: u32,
    timed_transaction_count: u32,
    first_seen_slot: u64,
    last_seen_slot: u64,
    min_block_time: i64,
    max_block_time: i64,
}

impl ProgramUsagePayload {
    fn from_usage(usage: ProgramUsage) -> Self {
        Self {
            direct_instruction_count: usage.direct_instruction_count,
            inner_instruction_count: usage.inner_instruction_count,
            transaction_count: usage.transaction_count,
            timed_transaction_count: usage.timed_transaction_count,
            first_seen_slot: usage.first_seen_slot,
            last_seen_slot: usage.last_seen_slot,
            min_block_time: usage.min_block_time,
            max_block_time: usage.max_block_time,
        }
    }

    fn with_program_id(self, program_id: u32) -> ProgramUsage {
        ProgramUsage {
            program_id,
            direct_instruction_count: self.direct_instruction_count,
            inner_instruction_count: self.inner_instruction_count,
            transaction_count: self.transaction_count,
            first_seen_slot: self.first_seen_slot,
            last_seen_slot: self.last_seen_slot,
            min_block_time: self.min_block_time,
            max_block_time: self.max_block_time,
            timed_transaction_count: self.timed_transaction_count,
        }
    }
}

/// One compact arena item. Node indices remain zero-based `u32` values.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct RelationEntry {
    node: RelationNode,
    usage: ProgramUsagePayload,
}

impl RelationEntry {
    fn program_usage(self) -> ProgramUsage {
        self.usage.with_program_id(self.node.program_id)
    }
}

const _: () = assert!(size_of::<RelationNode>() == 8);
const _: () = assert!(size_of::<ProgramUsagePayload>() == 48);
const _: () = assert!(size_of::<RelationEntry>() == 56);

#[derive(Debug, Clone, Copy)]
struct BatchRelation {
    relation: Relation,
    usage: ProgramUsage,
}

/// Single-owner, signer-dense relation accumulator.
#[derive(Debug)]
pub struct DenseAccumulator {
    heads: Vec<u32>,
    entries: Vec<RelationEntry>,
    program_words: Vec<u64>,
    max_program_id: u32,
    max_batch_pairs: NonZeroUsize,
    batch_scratch: Vec<BatchRelation>,
    wallet_count: u32,
    distinct_program_count: u32,
    last_relation: Option<(Relation, u32)>,
}

impl DenseAccumulator {
    /// Allocate the four-byte head table and the program-presence bitset.
    ///
    /// `max_batch_pairs` is the hard per-message backpressure boundary.  This
    /// constructor does not preallocate relation entries or batch scratch, so a
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
            entries: Vec::new(),
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
        self.ensure_relation_capacity(additional)
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
        self.entries.len()
    }

    pub fn distinct_program_count(&self) -> u32 {
        self.distinct_program_count
    }

    /// Record one usage aggregate, preserving sorted uniqueness at insertion
    /// time and checked-merging it when the relation already exists.
    pub fn record(
        &mut self,
        dense_rank: u32,
        usage: ProgramUsage,
    ) -> Result<DenseRecordOutcome, DenseAccumulatorError> {
        self.validate_relation(dense_rank, usage.program_id())?;
        usage.validate()?;
        let relation = Relation {
            dense_rank,
            program_id: usage.program_id(),
        };
        self.record_validated(relation, usage)
    }

    /// Record a bounded batch whose first tuple item is a zero-based dense
    /// signer rank.  The input remains untouched.  A reusable internal scratch
    /// sorts and checked-merges equal keys before touching signer lists.
    pub fn record_rank_batch(
        &mut self,
        relations: &[(u32, ProgramUsage)],
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
        relations: &[(u32, ProgramUsage)],
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
        relations: &[(u32, ProgramUsage)],
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

        let mut scratch = std::mem::take(&mut self.batch_scratch);
        scratch.clear();
        let result = (|| {
            scratch.try_reserve(relations.len()).map_err(|_| {
                DenseAccumulatorError::AllocationFailed {
                    region: "batch scratch",
                }
            })?;
            for &(source_id, usage) in relations {
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
                self.validate_relation(dense_rank, usage.program_id())?;
                usage.validate()?;
                scratch.push(BatchRelation {
                    relation: Relation {
                        dense_rank,
                        program_id: usage.program_id(),
                    },
                    usage,
                });
            }

            scratch.sort_unstable_by_key(|entry| entry.relation);

            // Compact equal keys in place. Every duplicate contributes its
            // complete aggregate; no count or timing extremum is discarded.
            let mut distinct_len = 0usize;
            for read_index in 0..scratch.len() {
                let entry = scratch[read_index];
                if distinct_len != 0 && scratch[distinct_len - 1].relation == entry.relation {
                    scratch[distinct_len - 1].usage =
                        scratch[distinct_len - 1].usage.checked_merge(entry.usage)?;
                } else {
                    scratch[distinct_len] = entry;
                    distinct_len += 1;
                }
            }
            scratch.truncate(distinct_len);
            let batch_distinct_relations = scratch.len();

            // Preflight every merge before changing the accumulator. Replace
            // batch deltas for existing relations with their final aggregate,
            // so the apply phase cannot encounter a count overflow halfway
            // through a batch.
            let mut inserted_relations = 0usize;
            for entry in &mut scratch {
                if let Some(node_index) = self.find_node(entry.relation) {
                    entry.usage = self.entries[node_index as usize]
                        .program_usage()
                        .checked_merge(entry.usage)?;
                } else {
                    inserted_relations += 1;
                }
            }
            self.ensure_relation_capacity(inserted_relations)?;

            for entry in scratch.iter().copied() {
                self.apply_premerged(entry.relation, entry.usage);
            }

            Ok(BatchStats {
                input_relations: relations.len(),
                batch_distinct_relations,
                inserted_relations,
                duplicate_relations: relations.len() - inserted_relations,
            })
        })();
        scratch.clear();
        self.batch_scratch = scratch;
        result
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

    fn record_validated(
        &mut self,
        relation: Relation,
        usage: ProgramUsage,
    ) -> Result<DenseRecordOutcome, DenseAccumulatorError> {
        if let Some(node_index) = self.find_node(relation) {
            let merged = self.entries[node_index as usize]
                .program_usage()
                .checked_merge(usage)?;
            self.entries[node_index as usize].usage = ProgramUsagePayload::from_usage(merged);
            self.last_relation = Some((relation, node_index));
            return Ok(DenseRecordOutcome::Duplicate);
        }

        self.ensure_relation_capacity(1)?;
        self.insert_new_relation(relation, usage);
        Ok(DenseRecordOutcome::Inserted)
    }

    fn apply_premerged(&mut self, relation: Relation, usage: ProgramUsage) {
        if let Some(node_index) = self.find_node(relation) {
            self.entries[node_index as usize].usage = ProgramUsagePayload::from_usage(usage);
            self.last_relation = Some((relation, node_index));
        } else {
            // The entire batch was validated and its exact insertion capacity
            // was reserved before this apply phase began.
            debug_assert!(self.entries.len() < NO_NODE as usize);
            debug_assert!(self.entries.len() < self.entries.capacity());
            self.insert_new_relation(relation, usage);
        }
    }

    fn find_node(&self, relation: Relation) -> Option<u32> {
        if let Some((last_relation, node_index)) = self.last_relation
            && last_relation == relation
        {
            return Some(node_index);
        }

        let mut current = self.heads[relation.dense_rank as usize];
        while current != NO_NODE {
            let node = self.entries[current as usize].node;
            match node.program_id.cmp(&relation.program_id) {
                std::cmp::Ordering::Equal => return Some(current),
                std::cmp::Ordering::Greater => return None,
                std::cmp::Ordering::Less => current = node.next,
            }
        }
        None
    }

    fn insert_new_relation(&mut self, relation: Relation, usage: ProgramUsage) {
        debug_assert_eq!(usage.program_id(), relation.program_id);
        debug_assert!(self.entries.len() < NO_NODE as usize);

        let rank = relation.dense_rank as usize;
        let mut previous = NO_NODE;
        let mut current = self.heads[rank];
        while current != NO_NODE {
            let node = self.entries[current as usize].node;
            debug_assert_ne!(node.program_id, relation.program_id);
            if node.program_id > relation.program_id {
                break;
            }
            previous = current;
            current = node.next;
        }

        let node_index = self.entries.len() as u32;
        self.entries.push(RelationEntry {
            node: RelationNode {
                program_id: relation.program_id,
                next: current,
            },
            usage: ProgramUsagePayload::from_usage(usage),
        });
        if previous == NO_NODE {
            if self.heads[rank] == NO_NODE {
                self.wallet_count += 1;
            }
            self.heads[rank] = node_index;
        } else {
            self.entries[previous as usize].node.next = node_index;
        }
        self.last_relation = Some((relation, node_index));
        self.mark_program(relation.program_id);
    }

    fn ensure_relation_capacity(&mut self, additional: usize) -> Result<(), DenseAccumulatorError> {
        if self.entries.len().saturating_add(additional) > NO_NODE as usize {
            return Err(DenseAccumulatorError::RelationCapacityExceeded);
        }
        self.entries.try_reserve(additional).map_err(|_| {
            DenseAccumulatorError::AllocationFailed {
                region: "relation-entry arena",
            }
        })?;
        Ok(())
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

    fn programs_from(&self, head: u32) -> ProgramUsages<'_> {
        ProgramUsages {
            entries: &self.entries,
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

    pub fn programs(&self) -> ProgramUsages<'_> {
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

    pub fn programs(&self) -> ProgramUsages<'_> {
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

/// Sorted, unique program usage aggregates for one wallet.  This directly
/// follows arena links and allocates no per-wallet scratch.
#[derive(Clone)]
pub struct ProgramUsages<'a> {
    entries: &'a [RelationEntry],
    next: u32,
}

impl Iterator for ProgramUsages<'_> {
    type Item = ProgramUsage;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == NO_NODE {
            return None;
        }
        let index = self.next as usize;
        let entry = self.entries[index];
        self.next = entry.node.next;
        Some(entry.program_usage())
    }
}

impl FusedIterator for ProgramUsages<'_> {}

/// Compatibility name for code that used the previous id-only iterator.
/// The iterator item is now a complete [`ProgramUsage`] aggregate.
pub type ProgramIds<'a> = ProgramUsages<'a>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::PROGRAM_USAGE_MISSING_BLOCK_TIME;

    fn accumulator(signers: u32, programs: u32) -> DenseAccumulator {
        DenseAccumulator::new(signers, programs, NonZeroUsize::new(16).unwrap())
    }

    #[allow(clippy::too_many_arguments)]
    fn usage(
        program_id: u32,
        direct_instruction_count: u32,
        inner_instruction_count: u32,
        transaction_count: u32,
        first_seen_slot: u64,
        last_seen_slot: u64,
        min_block_time: i64,
        max_block_time: i64,
        timed_transaction_count: u32,
    ) -> ProgramUsage {
        ProgramUsage {
            program_id,
            direct_instruction_count,
            inner_instruction_count,
            transaction_count,
            first_seen_slot,
            last_seen_slot,
            min_block_time,
            max_block_time,
            timed_transaction_count,
        }
    }

    fn single_usage(program_id: u32, slot: u64) -> ProgramUsage {
        usage(program_id, 1, 0, 1, slot, slot, slot as i64, slot as i64, 1)
    }

    fn untimed_usage(program_id: u32, direct: u32, transactions: u32) -> ProgramUsage {
        usage(
            program_id,
            direct,
            0,
            transactions,
            1,
            1,
            PROGRAM_USAGE_MISSING_BLOCK_TIME,
            PROGRAM_USAGE_MISSING_BLOCK_TIME,
            0,
        )
    }

    fn program_ids(programs: ProgramUsages<'_>) -> Vec<u32> {
        programs.map(|usage| usage.program_id()).collect()
    }

    #[test]
    fn compact_layout_is_exactly_fifty_six_bytes_per_relation() {
        assert_eq!(EMPTY_SIGNER_SLOT_BYTES, 4);
        assert_eq!(RELATION_NODE_BYTES, 8);
        assert_eq!(RELATION_USAGE_BYTES, 48);
        assert_eq!(DISTINCT_RELATION_BYTES, 56);
        assert_eq!(size_of::<RelationEntry>(), 56);
        assert_eq!(
            DISTINCT_RELATION_BYTES,
            RELATION_NODE_BYTES + RELATION_USAGE_BYTES
        );
        assert_eq!(BATCH_RELATION_BYTES, size_of::<(u32, ProgramUsage)>());
    }

    #[test]
    fn insertion_keeps_each_wallet_sorted_and_merges_duplicate_usage() {
        let mut accumulator = accumulator(4, 100);
        assert_eq!(
            accumulator.record(2, single_usage(50, 50)).unwrap(),
            DenseRecordOutcome::Inserted
        );
        accumulator.record(2, single_usage(10, 10)).unwrap();
        accumulator.record(2, single_usage(90, 90)).unwrap();
        accumulator.record(2, single_usage(30, 30)).unwrap();
        assert_eq!(
            accumulator
                .record(2, usage(50, 2, 1, 1, 5, 100, 40, 700, 1))
                .unwrap(),
            DenseRecordOutcome::Duplicate
        );

        let wallet = accumulator.ranked_wallets().next().unwrap();
        assert_eq!(wallet.dense_rank(), 2);
        let programs = wallet.programs().collect::<Vec<_>>();
        assert_eq!(
            programs
                .iter()
                .map(ProgramUsage::program_id)
                .collect::<Vec<_>>(),
            [10, 30, 50, 90]
        );
        assert_eq!(programs[2], usage(50, 3, 1, 2, 5, 100, 40, 700, 2));
        assert_eq!(accumulator.wallet_count(), 1);
        assert_eq!(accumulator.relation_count(), 4);
    }

    #[test]
    fn high_repeat_traffic_updates_usage_without_appending_duplicate_nodes() {
        let mut accumulator = accumulator(1, 10);
        for _ in 0..10_000 {
            accumulator.record(0, single_usage(7, 1)).unwrap();
        }
        let repeated_batch = [(0, single_usage(7, 2)); 16];
        for _ in 0..10_000 {
            let stats = accumulator.record_rank_batch(&repeated_batch).unwrap();
            assert_eq!(stats.inserted_relations, 0);
        }
        assert_eq!(accumulator.relation_count(), 1);
        assert_eq!(accumulator.wallet_count(), 1);
        assert_eq!(accumulator.distinct_program_count(), 1);
        assert_eq!(
            accumulator
                .ranked_wallets()
                .next()
                .unwrap()
                .programs()
                .next()
                .unwrap(),
            usage(7, 170_000, 0, 170_000, 1, 2, 1, 2, 170_000)
        );
    }

    #[test]
    fn batch_is_bounded_grouped_and_merges_all_duplicate_statistics() {
        let mut accumulator = accumulator(3, 20);
        let batch = [
            (2, single_usage(9, 10)),
            (0, single_usage(3, 20)),
            (2, usage(9, 0, 2, 1, 5, 30, 50, 300, 1)),
            (0, single_usage(1, 40)),
            (0, usage(3, 0, 1, 1, 10, 50, 100, 500, 1)),
        ];
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
        let rank_two = accumulator
            .ranked_wallets()
            .find(|wallet| wallet.dense_rank() == 2)
            .unwrap();
        assert_eq!(
            rank_two.programs().next().unwrap(),
            usage(9, 1, 2, 2, 5, 30, 10, 300, 2)
        );

        let oversized = vec![(0, single_usage(1, 1)); 17];
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
            .record_signer_batch(&[(42, single_usage(8, 1)), (11, single_usage(3, 2))], ranks)
            .unwrap();
        assert_eq!(accumulator.relation_count(), 2);

        let error = accumulator
            .record_signer_batch(&[(11, single_usage(4, 3)), (99, single_usage(5, 4))], ranks)
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
            accumulator.record(2, single_usage(1, 1)),
            Err(DenseAccumulatorError::InvalidRank {
                rank: 2,
                signer_count: 2,
            })
        );
        for program_id in [0, 11] {
            let mut invalid = single_usage(1, 1);
            invalid.program_id = program_id;
            assert_eq!(
                accumulator.record(0, invalid),
                Err(DenseAccumulatorError::InvalidProgram {
                    program_id,
                    max_program_id: 10,
                })
            );
        }
        assert_eq!(accumulator.relation_count(), 0);

        assert_eq!(
            accumulator.record_rank_batch(&[(0, single_usage(2, 1)), (2, single_usage(3, 2)),]),
            Err(DenseAccumulatorError::InvalidRank {
                rank: 2,
                signer_count: 2,
            })
        );
        assert_eq!(accumulator.relation_count(), 0);
    }

    #[test]
    fn invalid_usage_and_overflowing_batch_leave_the_accumulator_unchanged() {
        let mut accumulator = accumulator(1, 10);
        let invalid = usage(
            1,
            1,
            0,
            0,
            1,
            1,
            PROGRAM_USAGE_MISSING_BLOCK_TIME,
            PROGRAM_USAGE_MISSING_BLOCK_TIME,
            0,
        );
        assert!(matches!(
            accumulator.record(0, invalid),
            Err(DenseAccumulatorError::InvalidProgramUsage {
                source: ProgramUsageError::EmptyTransactionCount
            })
        ));

        let maximum = untimed_usage(2, u32::MAX, u32::MAX);
        accumulator.record(0, maximum).unwrap();
        let batch = [(0, single_usage(1, 1)), (0, untimed_usage(2, 1, 1))];
        assert!(matches!(
            accumulator.record_rank_batch(&batch),
            Err(DenseAccumulatorError::InvalidProgramUsage {
                source: ProgramUsageError::CountOverflow { .. }
            })
        ));
        assert_eq!(accumulator.relation_count(), 1);
        let only = accumulator.ranked_wallets().next().unwrap();
        assert_eq!(only.programs().collect::<Vec<_>>(), [maximum]);
    }

    #[test]
    fn wallet_iteration_omits_empty_signers_and_attaches_registry_ids() {
        let mut accumulator = accumulator(4, 20);
        accumulator.record(1, single_usage(9, 1)).unwrap();
        accumulator.record(3, single_usage(7, 2)).unwrap();
        accumulator.record(3, single_usage(2, 3)).unwrap();

        let output = accumulator
            .wallets([(0, 10), (1, 20), (2, 25), (3, 90)])
            .map(|wallet| {
                let wallet = wallet.unwrap();
                (
                    wallet.wallet_id(),
                    wallet.dense_rank(),
                    program_ids(wallet.programs()),
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
        accumulator.record(0, single_usage(99, 1)).unwrap();
        accumulator.record(1, single_usage(4, 2)).unwrap();
        accumulator.record(2, single_usage(99, 3)).unwrap();
        accumulator.record(2, single_usage(30, 4)).unwrap();
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
            .record_signer_batch(
                &[(17, single_usage(10, 1)), (3, single_usage(4, 2))],
                |id| ranks.rank(id),
            )
            .unwrap();
        let output = accumulator
            .wallets(ranks.iter_ids())
            .map(|wallet| {
                let wallet = wallet.unwrap();
                (wallet.wallet_id(), program_ids(wallet.programs()))
            })
            .collect::<Vec<_>>();
        assert_eq!(output, [(3, vec![4]), (17, vec![10])]);
    }

    #[test]
    fn batched_accumulation_matches_a_checked_usage_oracle() {
        use std::collections::BTreeMap;

        const SIGNERS: u32 = 64;
        const PROGRAMS: u32 = 127;
        let mut accumulator =
            DenseAccumulator::new(SIGNERS, PROGRAMS, NonZeroUsize::new(32).unwrap());
        let mut oracle = vec![BTreeMap::<u32, ProgramUsage>::new(); SIGNERS as usize];
        let mut seed = 0x8d26_5f17_4a93_cbe1u64;
        for batch_index in 0..1_000u64 {
            let mut batch = [(0, single_usage(1, 1)); 32];
            for (entry_index, relation) in batch.iter_mut().enumerate() {
                // Fixed-seed xorshift64 keeps the test deterministic and does
                // not add a random-number dependency to the production crate.
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let rank = (seed % u64::from(SIGNERS)) as u32;
                let program = ((seed >> 32) % u64::from(PROGRAMS) + 1) as u32;
                let slot = batch_index * 32 + entry_index as u64 + 1;
                let next = if seed & 1 == 0 {
                    single_usage(program, slot)
                } else {
                    usage(program, 0, 1, 1, slot, slot, slot as i64, slot as i64, 1)
                };
                *relation = (rank, next);
                oracle[rank as usize]
                    .entry(program)
                    .and_modify(|current| *current = current.checked_merge(next).unwrap())
                    .or_insert(next);
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
            .map(|(rank, programs)| (rank as u32, programs.values().copied().collect::<Vec<_>>()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(
            accumulator.relation_count(),
            oracle.iter().map(BTreeMap::len).sum::<usize>()
        );
    }
}
