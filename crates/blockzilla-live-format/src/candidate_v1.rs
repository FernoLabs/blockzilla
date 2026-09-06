//! Source-neutral, internal block-candidate semantics.
//!
//! This module intentionally defines no candidate wire encoding. A candidate is
//! a structural value produced from immutable evidence; it is not source
//! promotion, finality, or publication authority. Runtime observations are kept
//! in a separate attachment value and never merge into the signed ledger.

use std::{error::Error, fmt};

use blockzilla_compact::CompactPohEntry;

pub type PohEntryV1 = CompactPohEntry;

/// The minimal signed envelope retained until source promotion has verified it.
///
/// `signed_message_bytes` is the Solana message serialization covered by every
/// signature, not the surrounding transaction short-vector encoding. The
/// fixed-size signature representation prevents an invalid signature length
/// from crossing the structural candidate boundary. Era-specific parsing,
/// signature cardinality, sanitation, and cryptographic verification belong to
/// source promotion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedTransactionEnvelopeV1 {
    pub signatures: Vec<[u8; 64]>,
    pub signed_message_bytes: Vec<u8>,
}

/// A complete signed-transaction section in ledger order.
///
/// `None` at the candidate field means missing. `Some` with an empty `entries`
/// vector means the source verified that the block has no transactions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionsV1 {
    pub entries: Vec<SignedTransactionEnvelopeV1>,
}

/// Ordered source component layout without storing PoH entries twice.
///
/// Every component carries the end-exclusive cumulative data-shred count for
/// the exact `data_complete` range that encoded it. Entry batches additionally
/// advance the cumulative PoH-entry count; markers legitimately advance only
/// the data-shred count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockComponentLayoutV1 {
    EntryBatch {
        entries_through: u32,
        data_shreds_through: u32,
    },
    BlockMarker {
        bytes: Vec<u8>,
        data_shreds_through: u32,
    },
}

/// Internal, source-neutral structural candidate.
///
/// This type deliberately carries neither evidence claims nor a capability
/// bitset. A source-specific promotion stage must bind an immutable evidence set
/// and pinned verifier/trust policy before a candidate can enter finality or a
/// product builder.
#[derive(Debug)]
pub struct BlockCandidateV1 {
    slot: u64,
    parent_slot: u64,
    final_poh_hash: [u8; 32],
    consensus_block_id: Option<[u8; 32]>,

    parent_final_poh_hash: Option<[u8; 32]>,
    parent_consensus_block_id: Option<[u8; 32]>,
    transactions: Option<TransactionsV1>,
    poh_entries: Option<Vec<PohEntryV1>>,
    block_components: Option<Vec<BlockComponentLayoutV1>>,
}

/// Construction input for [`BlockCandidateV1`].
#[derive(Debug)]
pub struct BlockCandidatePartsV1 {
    pub slot: u64,
    pub parent_slot: u64,
    pub final_poh_hash: [u8; 32],
    pub consensus_block_id: Option<[u8; 32]>,

    pub parent_final_poh_hash: Option<[u8; 32]>,
    pub parent_consensus_block_id: Option<[u8; 32]>,
    pub transactions: Option<TransactionsV1>,
    pub poh_entries: Option<Vec<PohEntryV1>>,
    pub block_components: Option<Vec<BlockComponentLayoutV1>>,
}

impl BlockCandidateV1 {
    pub fn new(parts: BlockCandidatePartsV1) -> Result<Self, CandidateValidationError> {
        let candidate = Self {
            slot: parts.slot,
            parent_slot: parts.parent_slot,
            final_poh_hash: parts.final_poh_hash,
            consensus_block_id: parts.consensus_block_id,
            parent_final_poh_hash: parts.parent_final_poh_hash,
            parent_consensus_block_id: parts.parent_consensus_block_id,
            transactions: parts.transactions,
            poh_entries: parts.poh_entries,
            block_components: parts.block_components,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    #[must_use]
    pub const fn slot(&self) -> u64 {
        self.slot
    }

    #[must_use]
    pub const fn parent_slot(&self) -> u64 {
        self.parent_slot
    }

    #[must_use]
    pub const fn final_poh_hash(&self) -> &[u8; 32] {
        &self.final_poh_hash
    }

    #[must_use]
    pub const fn consensus_block_id(&self) -> Option<&[u8; 32]> {
        self.consensus_block_id.as_ref()
    }

    #[must_use]
    pub const fn parent_final_poh_hash(&self) -> Option<&[u8; 32]> {
        self.parent_final_poh_hash.as_ref()
    }

    #[must_use]
    pub const fn parent_consensus_block_id(&self) -> Option<&[u8; 32]> {
        self.parent_consensus_block_id.as_ref()
    }

    #[must_use]
    pub const fn transactions(&self) -> Option<&TransactionsV1> {
        self.transactions.as_ref()
    }

    #[must_use]
    pub fn poh_entries(&self) -> Option<&[PohEntryV1]> {
        self.poh_entries.as_deref()
    }

    #[must_use]
    pub fn block_components(&self) -> Option<&[BlockComponentLayoutV1]> {
        self.block_components.as_deref()
    }

    /// Validate only source-neutral structural invariants.
    ///
    /// Required sections, protocol-era identity rules, exact message parsing,
    /// transaction signatures, marker semantics, source trust, and evidence
    /// completeness remain source-promotion responsibilities.
    pub fn validate(&self) -> Result<(), CandidateValidationError> {
        self.validate_parent()?;
        self.validate_transactions()?;
        self.validate_poh()?;
        self.validate_components()?;
        Ok(())
    }

    fn validate_parent(&self) -> Result<(), CandidateValidationError> {
        if self.slot == 0 {
            if self.parent_slot != 0 {
                return Err(CandidateValidationError::GenesisParentSlotNotZero {
                    parent_slot: self.parent_slot,
                });
            }
            if self.parent_final_poh_hash.is_some() || self.parent_consensus_block_id.is_some() {
                return Err(CandidateValidationError::GenesisParentIdentityPresent);
            }
        } else if self.parent_slot >= self.slot {
            return Err(CandidateValidationError::ParentNotEarlier {
                slot: self.slot,
                parent_slot: self.parent_slot,
            });
        }
        Ok(())
    }

    fn validate_transactions(&self) -> Result<(), CandidateValidationError> {
        if let Some(transactions) = &self.transactions {
            for (transaction_index, transaction) in transactions.entries.iter().enumerate() {
                if transaction.signed_message_bytes.is_empty() {
                    return Err(CandidateValidationError::EmptySignedMessage { transaction_index });
                }
            }
        }
        Ok(())
    }

    fn validate_poh(&self) -> Result<(), CandidateValidationError> {
        let Some(entries) = &self.poh_entries else {
            return Ok(());
        };
        if entries.is_empty() {
            return Err(CandidateValidationError::EmptyPohEntries);
        }

        let final_hash = entries.last().expect("non-empty checked").hash;
        if final_hash != self.final_poh_hash {
            return Err(CandidateValidationError::FinalPohHashMismatch {
                final_poh_hash: self.final_poh_hash,
                final_entry_hash: final_hash,
            });
        }

        if let Some(transactions) = &self.transactions {
            let poh_transactions = entries.iter().try_fold(0u64, |count, entry| {
                count
                    .checked_add(u64::from(entry.tx_count))
                    .ok_or(CandidateValidationError::PohTransactionCountOverflow)
            })?;
            let transactions = u64::try_from(transactions.entries.len()).map_err(|_| {
                CandidateValidationError::TransactionCountExceedsU64 {
                    transactions: transactions.entries.len(),
                }
            })?;
            if poh_transactions != transactions {
                return Err(CandidateValidationError::PohTransactionCountMismatch {
                    poh_transactions,
                    transactions,
                });
            }
        }
        Ok(())
    }

    fn validate_components(&self) -> Result<(), CandidateValidationError> {
        let Some(components) = &self.block_components else {
            return Ok(());
        };
        if components.is_empty() {
            return Err(CandidateValidationError::EmptyBlockComponents);
        }
        let poh_entries = self
            .poh_entries
            .as_ref()
            .ok_or(CandidateValidationError::BlockComponentsWithoutPoh)?;
        let poh_entry_count = u32::try_from(poh_entries.len()).map_err(|_| {
            CandidateValidationError::PohEntryCountExceedsU32 {
                poh_entries: poh_entries.len(),
            }
        })?;

        let mut previous_entries_through = 0u32;
        let mut previous_data_shreds_through = 0u32;
        let mut entry_batch_count = 0usize;
        for (component_index, component) in components.iter().enumerate() {
            let data_shreds_through = match component {
                BlockComponentLayoutV1::EntryBatch {
                    entries_through,
                    data_shreds_through,
                } => {
                    if *entries_through == 0 {
                        return Err(CandidateValidationError::ZeroEntryBatchBoundary {
                            component_index,
                        });
                    }
                    if *entries_through <= previous_entries_through {
                        return Err(
                            CandidateValidationError::EntryBatchBoundariesNotIncreasing {
                                component_index,
                                previous_entries_through,
                                entries_through: *entries_through,
                            },
                        );
                    }
                    if *entries_through > poh_entry_count {
                        return Err(CandidateValidationError::EntryBatchBoundaryOutOfRange {
                            component_index,
                            entries_through: *entries_through,
                            poh_entries: poh_entry_count,
                        });
                    }
                    previous_entries_through = *entries_through;
                    entry_batch_count += 1;
                    *data_shreds_through
                }
                BlockComponentLayoutV1::BlockMarker {
                    bytes,
                    data_shreds_through,
                } => {
                    if bytes.is_empty() {
                        return Err(CandidateValidationError::EmptyBlockMarker { component_index });
                    }
                    *data_shreds_through
                }
            };
            if data_shreds_through == 0 || data_shreds_through <= previous_data_shreds_through {
                return Err(CandidateValidationError::ComponentDataShredsNotIncreasing {
                    component_index,
                    previous_data_shreds_through,
                    data_shreds_through,
                });
            }
            previous_data_shreds_through = data_shreds_through;
        }

        if entry_batch_count == 0 {
            return Err(CandidateValidationError::MissingEntryBatch);
        }
        if previous_entries_through != poh_entry_count {
            return Err(CandidateValidationError::FinalEntryBatchCountMismatch {
                entries_through: previous_entries_through,
                poh_entries: poh_entry_count,
            });
        }
        Ok(())
    }

    /// Cross-check the component layout against the adapter's exact recovered
    /// data-shred count.
    pub fn validate_data_shred_count(
        &self,
        data_shred_count: u32,
    ) -> Result<(), CandidateValidationError> {
        self.validate()?;
        let components = self
            .block_components
            .as_ref()
            .ok_or(CandidateValidationError::MissingBlockComponents)?;
        let component_count = match components
            .last()
            .expect("validate rejects empty components")
        {
            BlockComponentLayoutV1::EntryBatch {
                data_shreds_through,
                ..
            }
            | BlockComponentLayoutV1::BlockMarker {
                data_shreds_through,
                ..
            } => *data_shreds_through,
        };
        if component_count != data_shred_count {
            return Err(CandidateValidationError::FinalComponentDataCountMismatch {
                data_shreds_through: component_count,
                data_shreds: data_shred_count,
            });
        }
        Ok(())
    }

    /// Compare every structural field, including presence.
    ///
    /// Exact semantic equality is necessary but not sufficient for source
    /// deduplication: the caller must also require the same immutable evidence
    /// identity. This method never combines candidates.
    #[must_use]
    pub fn structurally_identical(&self, other: &Self) -> bool {
        self.slot == other.slot
            && self.parent_slot == other.parent_slot
            && self.final_poh_hash == other.final_poh_hash
            && self.consensus_block_id == other.consensus_block_id
            && self.parent_final_poh_hash == other.parent_final_poh_hash
            && self.parent_consensus_block_id == other.parent_consensus_block_id
            && self.transactions == other.transactions
            && optional_poh_equal(&self.poh_entries, &other.poh_entries)
            && self.block_components == other.block_components
    }

    /// Check whether two observations can refer to the same selected ledger.
    ///
    /// Missing optional source extensions are compatible with a present value;
    /// two different present values conflict. This relation is deliberately not
    /// transitive and must only create a pairwise join edge. It is never a merge
    /// or deduplication rule.
    pub fn pairwise_compatibility(
        &self,
        other: &Self,
    ) -> Result<(), CandidateCompatibilityConflictV1> {
        require_equal(
            self.slot,
            other.slot,
            CandidateCompatibilityConflictV1::Slot,
        )?;
        require_equal(
            self.parent_slot,
            other.parent_slot,
            CandidateCompatibilityConflictV1::ParentSlot,
        )?;
        require_equal(
            self.final_poh_hash,
            other.final_poh_hash,
            CandidateCompatibilityConflictV1::FinalPohHash,
        )?;
        require_optional_equal(
            self.consensus_block_id,
            other.consensus_block_id,
            CandidateCompatibilityConflictV1::ConsensusBlockId,
        )?;
        require_optional_equal(
            self.parent_final_poh_hash,
            other.parent_final_poh_hash,
            CandidateCompatibilityConflictV1::ParentFinalPohHash,
        )?;
        require_optional_equal(
            self.parent_consensus_block_id,
            other.parent_consensus_block_id,
            CandidateCompatibilityConflictV1::ParentConsensusBlockId,
        )?;
        require_optional_equal_by(
            self.transactions.as_ref(),
            other.transactions.as_ref(),
            CandidateCompatibilityConflictV1::Transactions,
            |left, right| left == right,
        )?;
        require_optional_equal_by(
            self.poh_entries.as_ref(),
            other.poh_entries.as_ref(),
            CandidateCompatibilityConflictV1::PohEntries,
            |left, right| poh_equal(left, right),
        )?;
        require_optional_equal_by(
            self.block_components.as_ref(),
            other.block_components.as_ref(),
            CandidateCompatibilityConflictV1::BlockComponents,
            |left, right| left == right,
        )?;
        Ok(())
    }
}

fn optional_poh_equal(left: &Option<Vec<PohEntryV1>>, right: &Option<Vec<PohEntryV1>>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => poh_equal(left, right),
        (None, Some(_)) | (Some(_), None) => false,
    }
}

fn poh_equal(left: &[PohEntryV1], right: &[PohEntryV1]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.num_hashes == right.num_hashes
                && left.hash == right.hash
                && left.tx_count == right.tx_count
        })
}

fn require_equal<T: Eq>(
    left: T,
    right: T,
    conflict: CandidateCompatibilityConflictV1,
) -> Result<(), CandidateCompatibilityConflictV1> {
    if left == right { Ok(()) } else { Err(conflict) }
}

fn require_optional_equal<T: Eq>(
    left: Option<T>,
    right: Option<T>,
    conflict: CandidateCompatibilityConflictV1,
) -> Result<(), CandidateCompatibilityConflictV1> {
    require_optional_equal_by(left.as_ref(), right.as_ref(), conflict, |left, right| {
        left == right
    })
}

fn require_optional_equal_by<T: ?Sized, F>(
    left: Option<&T>,
    right: Option<&T>,
    conflict: CandidateCompatibilityConflictV1,
    equal: F,
) -> Result<(), CandidateCompatibilityConflictV1>
where
    F: FnOnce(&T, &T) -> bool,
{
    match (left, right) {
        (Some(left), Some(right)) if !equal(left, right) => Err(conflict),
        _ => Ok(()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateCompatibilityConflictV1 {
    Slot,
    ParentSlot,
    FinalPohHash,
    ConsensusBlockId,
    ParentFinalPohHash,
    ParentConsensusBlockId,
    Transactions,
    PohEntries,
    BlockComponents,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateValidationError {
    GenesisParentSlotNotZero {
        parent_slot: u64,
    },
    GenesisParentIdentityPresent,
    ParentNotEarlier {
        slot: u64,
        parent_slot: u64,
    },
    EmptySignedMessage {
        transaction_index: usize,
    },
    EmptyPohEntries,
    PohTransactionCountOverflow,
    TransactionCountExceedsU64 {
        transactions: usize,
    },
    PohTransactionCountMismatch {
        poh_transactions: u64,
        transactions: u64,
    },
    FinalPohHashMismatch {
        final_poh_hash: [u8; 32],
        final_entry_hash: [u8; 32],
    },
    EmptyBlockComponents,
    BlockComponentsWithoutPoh,
    ZeroEntryBatchBoundary {
        component_index: usize,
    },
    EntryBatchBoundariesNotIncreasing {
        component_index: usize,
        previous_entries_through: u32,
        entries_through: u32,
    },
    EntryBatchBoundaryOutOfRange {
        component_index: usize,
        entries_through: u32,
        poh_entries: u32,
    },
    EmptyBlockMarker {
        component_index: usize,
    },
    ComponentDataShredsNotIncreasing {
        component_index: usize,
        previous_data_shreds_through: u32,
        data_shreds_through: u32,
    },
    MissingEntryBatch,
    FinalEntryBatchCountMismatch {
        entries_through: u32,
        poh_entries: u32,
    },
    MissingBlockComponents,
    PohEntryCountExceedsU32 {
        poh_entries: usize,
    },
    FinalComponentDataCountMismatch {
        data_shreds_through: u32,
        data_shreds: u32,
    },
}

impl fmt::Display for CandidateValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenesisParentSlotNotZero { parent_slot } => {
                write!(
                    formatter,
                    "genesis parent slot is {parent_slot}, expected zero"
                )
            }
            Self::GenesisParentIdentityPresent => {
                formatter.write_str("genesis candidate carries a parent identity")
            }
            Self::ParentNotEarlier { slot, parent_slot } => write!(
                formatter,
                "non-genesis parent slot {parent_slot} is not earlier than slot {slot}"
            ),
            Self::EmptySignedMessage { transaction_index } => write!(
                formatter,
                "transaction {transaction_index} has empty signed-message bytes"
            ),
            Self::EmptyPohEntries => formatter.write_str("present PoH entries are empty"),
            Self::PohTransactionCountOverflow => {
                formatter.write_str("PoH transaction count overflows u64")
            }
            Self::TransactionCountExceedsU64 { transactions } => write!(
                formatter,
                "candidate transaction count {transactions} exceeds u64"
            ),
            Self::PohTransactionCountMismatch {
                poh_transactions,
                transactions,
            } => write!(
                formatter,
                "PoH declares {poh_transactions} transactions but candidate has {transactions}"
            ),
            Self::FinalPohHashMismatch { .. } => {
                formatter.write_str("final PoH entry hash does not equal final_poh_hash")
            }
            Self::EmptyBlockComponents => {
                formatter.write_str("present block-component layout is empty")
            }
            Self::BlockComponentsWithoutPoh => {
                formatter.write_str("block-component layout requires present PoH entries")
            }
            Self::ZeroEntryBatchBoundary { component_index } => write!(
                formatter,
                "entry-batch component {component_index} has a zero boundary"
            ),
            Self::EntryBatchBoundariesNotIncreasing {
                component_index, ..
            } => write!(
                formatter,
                "entry-batch component {component_index} is not strictly cumulative"
            ),
            Self::EntryBatchBoundaryOutOfRange {
                component_index,
                entries_through,
                poh_entries,
            } => write!(
                formatter,
                "entry-batch component {component_index} covers {entries_through} entries, beyond {poh_entries}"
            ),
            Self::EmptyBlockMarker { component_index } => {
                write!(
                    formatter,
                    "block-marker component {component_index} is empty"
                )
            }
            Self::ComponentDataShredsNotIncreasing {
                component_index,
                previous_data_shreds_through,
                data_shreds_through,
            } => write!(
                formatter,
                "component {component_index} ends through data shred {data_shreds_through}, not after {previous_data_shreds_through}"
            ),
            Self::MissingEntryBatch => {
                formatter.write_str("block-component layout has no entry batch")
            }
            Self::FinalEntryBatchCountMismatch {
                entries_through,
                poh_entries,
            } => write!(
                formatter,
                "final entry-batch boundary covers {entries_through} entries, expected {poh_entries}"
            ),
            Self::MissingBlockComponents => {
                formatter.write_str("block-component layout is missing")
            }
            Self::PohEntryCountExceedsU32 { poh_entries } => write!(
                formatter,
                "PoH entry count {poh_entries} exceeds boundary u32 range"
            ),
            Self::FinalComponentDataCountMismatch {
                data_shreds_through,
                data_shreds,
            } => write!(
                formatter,
                "final component covers {data_shreds_through} data shreds, expected {data_shreds}"
            ),
        }
    }
}

impl Error for CandidateValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn transaction(message_byte: u8) -> SignedTransactionEnvelopeV1 {
        SignedTransactionEnvelopeV1 {
            signatures: vec![[7; 64]],
            signed_message_bytes: vec![message_byte, 1, 2, 3],
        }
    }

    fn candidate() -> BlockCandidateV1 {
        BlockCandidateV1::new(BlockCandidatePartsV1 {
            slot: 42,
            parent_slot: 41,
            final_poh_hash: [9; 32],
            consensus_block_id: Some([10; 32]),
            parent_final_poh_hash: Some([8; 32]),
            parent_consensus_block_id: Some([7; 32]),
            transactions: Some(TransactionsV1 {
                entries: vec![transaction(4)],
            }),
            poh_entries: Some(vec![PohEntryV1 {
                num_hashes: 12,
                hash: [9; 32],
                tx_count: 1,
                signature_count: 0,
            }]),
            block_components: Some(vec![
                BlockComponentLayoutV1::EntryBatch {
                    entries_through: 1,
                    data_shreds_through: 1,
                },
                BlockComponentLayoutV1::BlockMarker {
                    bytes: vec![1, 2],
                    data_shreds_through: 2,
                },
            ]),
        })
        .unwrap()
    }

    #[test]
    fn constructor_builds_ledger_only_candidate_with_distinct_identities() {
        let candidate = candidate();
        assert_eq!(candidate.slot(), 42);
        assert_eq!(candidate.parent_slot(), 41);
        assert_eq!(candidate.final_poh_hash(), &[9; 32]);
        assert_eq!(candidate.consensus_block_id(), Some(&[10; 32]));
        assert_eq!(candidate.parent_final_poh_hash(), Some(&[8; 32]));
        assert_eq!(candidate.parent_consensus_block_id(), Some(&[7; 32]));
        assert_eq!(candidate.transactions().unwrap().entries.len(), 1);
        assert_eq!(candidate.block_components().unwrap().len(), 2);
    }

    #[test]
    fn validates_genesis_parent_rules() {
        let invalid_slot = BlockCandidateV1::new(BlockCandidatePartsV1 {
            slot: 0,
            parent_slot: 1,
            final_poh_hash: [9; 32],
            consensus_block_id: None,
            parent_final_poh_hash: None,
            parent_consensus_block_id: None,
            transactions: None,
            poh_entries: None,
            block_components: None,
        });
        assert_eq!(
            invalid_slot.unwrap_err(),
            CandidateValidationError::GenesisParentSlotNotZero { parent_slot: 1 }
        );

        let invalid_identity = BlockCandidateV1::new(BlockCandidatePartsV1 {
            slot: 0,
            parent_slot: 0,
            final_poh_hash: [9; 32],
            consensus_block_id: None,
            parent_final_poh_hash: Some([8; 32]),
            parent_consensus_block_id: None,
            transactions: None,
            poh_entries: None,
            block_components: None,
        });
        assert_eq!(
            invalid_identity.unwrap_err(),
            CandidateValidationError::GenesisParentIdentityPresent
        );
    }

    #[test]
    fn validates_parent_message_and_final_poh() {
        let mut candidate = candidate();
        candidate.parent_slot = candidate.slot;
        assert!(matches!(
            candidate.validate(),
            Err(CandidateValidationError::ParentNotEarlier { .. })
        ));

        candidate.parent_slot = candidate.slot - 1;
        candidate.transactions.as_mut().unwrap().entries[0]
            .signed_message_bytes
            .clear();
        assert_eq!(
            candidate.validate(),
            Err(CandidateValidationError::EmptySignedMessage {
                transaction_index: 0
            })
        );

        candidate.transactions.as_mut().unwrap().entries[0] = transaction(4);
        candidate.poh_entries.as_mut().unwrap()[0].hash = [1; 32];
        assert!(matches!(
            candidate.validate(),
            Err(CandidateValidationError::FinalPohHashMismatch { .. })
        ));
    }

    #[test]
    fn distinguishes_missing_from_verified_empty_and_checks_poh_count() {
        let mut candidate = candidate();
        candidate.transactions = Some(TransactionsV1 { entries: vec![] });
        candidate.poh_entries.as_mut().unwrap()[0].tx_count = 0;
        candidate.validate().unwrap();

        candidate.poh_entries.as_mut().unwrap()[0].tx_count = 1;
        assert_eq!(
            candidate.validate(),
            Err(CandidateValidationError::PohTransactionCountMismatch {
                poh_transactions: 1,
                transactions: 0,
            })
        );

        candidate.transactions = None;
        candidate.validate().unwrap();
        candidate.poh_entries = Some(vec![]);
        assert_eq!(
            candidate.validate(),
            Err(CandidateValidationError::EmptyPohEntries)
        );
    }

    #[test]
    fn validates_component_layout_without_synthesizing_markers() {
        let mut candidate = candidate();
        candidate.poh_entries = Some(vec![
            PohEntryV1 {
                num_hashes: 1,
                hash: [3; 32],
                tx_count: 0,
                signature_count: 0,
            },
            PohEntryV1 {
                num_hashes: 1,
                hash: [9; 32],
                tx_count: 1,
                signature_count: 0,
            },
        ]);
        candidate.block_components = Some(vec![
            BlockComponentLayoutV1::EntryBatch {
                entries_through: 1,
                data_shreds_through: 2,
            },
            BlockComponentLayoutV1::BlockMarker {
                bytes: vec![8],
                data_shreds_through: 3,
            },
            BlockComponentLayoutV1::EntryBatch {
                entries_through: 2,
                data_shreds_through: 5,
            },
        ]);
        candidate.validate().unwrap();

        candidate.block_components.as_mut().unwrap()[2] = BlockComponentLayoutV1::EntryBatch {
            entries_through: 1,
            data_shreds_through: 5,
        };
        assert!(matches!(
            candidate.validate(),
            Err(CandidateValidationError::EntryBatchBoundariesNotIncreasing { .. })
        ));

        candidate.block_components = Some(vec![BlockComponentLayoutV1::BlockMarker {
            bytes: vec![8],
            data_shreds_through: 1,
        }]);
        assert_eq!(
            candidate.validate(),
            Err(CandidateValidationError::MissingEntryBatch)
        );
    }

    #[test]
    fn validates_component_aligned_data_shreds_and_exact_count() {
        let mut candidate = candidate();
        candidate.poh_entries = Some(vec![
            PohEntryV1 {
                num_hashes: 1,
                hash: [3; 32],
                tx_count: 0,
                signature_count: 0,
            },
            PohEntryV1 {
                num_hashes: 1,
                hash: [9; 32],
                tx_count: 1,
                signature_count: 0,
            },
        ]);
        candidate.block_components = Some(vec![
            BlockComponentLayoutV1::EntryBatch {
                entries_through: 1,
                data_shreds_through: 2,
            },
            BlockComponentLayoutV1::BlockMarker {
                bytes: vec![4],
                data_shreds_through: 3,
            },
            BlockComponentLayoutV1::EntryBatch {
                entries_through: 2,
                data_shreds_through: 5,
            },
        ]);
        candidate.validate_data_shred_count(5).unwrap();
        assert_eq!(
            candidate.validate_data_shred_count(6),
            Err(CandidateValidationError::FinalComponentDataCountMismatch {
                data_shreds_through: 5,
                data_shreds: 6,
            })
        );
    }

    #[test]
    fn separates_exact_identity_from_nontransitive_pairwise_compatibility() {
        let shred_a = candidate();

        let mut grpc_b = candidate();
        grpc_b.consensus_block_id = None;
        grpc_b.parent_consensus_block_id = None;
        grpc_b.block_components = None;

        let mut shred_c = candidate();
        let BlockComponentLayoutV1::BlockMarker { bytes, .. } =
            &mut shred_c.block_components.as_mut().unwrap()[1]
        else {
            panic!("fixture component 1 must be a marker")
        };
        *bytes = vec![9];

        assert!(!shred_a.structurally_identical(&grpc_b));
        assert!(candidate().structurally_identical(&candidate()));
        assert_eq!(shred_a.pairwise_compatibility(&grpc_b), Ok(()));
        assert_eq!(grpc_b.pairwise_compatibility(&shred_c), Ok(()));
        assert_eq!(
            shred_a.pairwise_compatibility(&shred_c),
            Err(CandidateCompatibilityConflictV1::BlockComponents)
        );
    }

    #[test]
    fn different_present_consensus_ids_conflict_without_changing_final_poh() {
        let left = candidate();
        let mut right = candidate();
        right.consensus_block_id = Some([0x44; 32]);

        assert_eq!(left.final_poh_hash, right.final_poh_hash);
        assert_eq!(
            left.pairwise_compatibility(&right),
            Err(CandidateCompatibilityConflictV1::ConsensusBlockId)
        );
    }
}
