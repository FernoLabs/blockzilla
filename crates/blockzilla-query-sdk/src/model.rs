use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Maximum item count encoded by one canonical Solana short vector.
pub const MAX_CANONICAL_SHORT_VEC_ITEMS: usize = u16::MAX as usize;
/// Maximum required-signer count encoded by the message header.
pub const MAX_CANONICAL_REQUIRED_SIGNERS: usize = u8::MAX as usize;

/// A reason why one part of a source transaction is not exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CoverageReason {
    MetadataAbsent,
    RawTransaction,
    RawMetadata,
    InvalidReference,
    AmbiguousInstructionData,
    InstructionDataUnavailable,
    UnsupportedInstruction,
    SourceUnverified,
    NonContiguousHistory,
    Other,
}

/// Transaction execution state recorded by the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "state", content = "reason")]
pub enum ExecutionStatus {
    Succeeded,
    Failed,
    Unknown(CoverageReason),
}

/// Coverage of recorded inner instructions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "state", content = "reason")]
pub enum CpiCoverage {
    Complete,
    NotRecorded,
    Unknown(CoverageReason),
}

/// Coverage of the outer message instructions and resolved account keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "state", content = "reason")]
pub enum InstructionCoverage {
    Complete,
    Unknown(CoverageReason),
}

/// Coverage of the exact source bytes for one instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "state", content = "reason")]
pub enum InstructionDataCoverage {
    Exact,
    NotRequested,
    Unknown(CoverageReason),
}

/// Stable source coordinates for one outer or inner instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstructionCoordinate {
    /// Zero-based position in the canonical instruction stream.
    pub order: u32,
    /// Zero-based top-level instruction index.
    pub outer_index: u32,
    /// Zero-based position in the recorded inner list for `outer_index`.
    /// `None` identifies the outer instruction itself.
    pub inner_index: Option<u32>,
    /// Runtime stack height when the source records it.
    pub stack_height: Option<u32>,
}

/// One validated instruction with all account indexes resolved to public keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedInstruction {
    pub coordinate: InstructionCoordinate,
    pub program_id: [u8; 32],
    /// Account public keys in the instruction account-index order.
    pub accounts: Vec<[u8; 32]>,
    pub data_coverage: InstructionDataCoverage,
    pub data: Vec<u8>,
}

/// Canonical identity for one block row in an archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockHeader {
    pub epoch: u64,
    pub block_ordinal: u32,
    pub slot: u64,
}

/// Canonical identity and coverage state for one transaction in a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionHeader {
    pub tx_index: u32,
    pub status: ExecutionStatus,
    /// Zero-based outer instruction that returned `InstructionError`.
    ///
    /// This is present only when the source records an exact failed outer
    /// instruction index. Other transaction failures keep this value absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_outer_instruction_index: Option<u32>,
    pub instruction_coverage: InstructionCoverage,
    pub cpi_coverage: CpiCoverage,
}

/// Owned transaction projection produced by a format adapter.
///
/// This type does not implement Serde. The fixed 64-byte signature is a wire
/// identity, and applications must select their own text or binary encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalTransaction {
    pub header: TransactionHeader,
    pub primary_signature: Option<[u8; 64]>,
    /// Required signer public keys in message order.
    pub required_signers: Vec<[u8; 32]>,
    pub instructions: Vec<ResolvedInstruction>,
}

impl CanonicalTransaction {
    fn validate(&self, expected_tx_index: u32) -> Result<()> {
        if self.header.tx_index != expected_tx_index {
            return Err(Error::InvalidTransaction(format!(
                "transaction index is {}, expected {expected_tx_index}",
                self.header.tx_index
            )));
        }
        if !matches!(self.header.status, ExecutionStatus::Failed)
            && self.header.failed_outer_instruction_index.is_some()
        {
            return Err(Error::InvalidTransaction(
                "a non-failed transaction has a failed outer instruction index".into(),
            ));
        }
        if self.required_signers.len() > MAX_CANONICAL_REQUIRED_SIGNERS {
            return Err(Error::InvalidTransaction(format!(
                "required signer count {} exceeds the canonical u8 limit",
                self.required_signers.len()
            )));
        }
        if self.instructions.len() > MAX_CANONICAL_SHORT_VEC_ITEMS {
            return Err(Error::InvalidTransaction(format!(
                "instruction count {} exceeds the canonical short-vector limit",
                self.instructions.len()
            )));
        }

        let mut expected_outer = 0u32;
        let mut next_inner = None;

        for (position, instruction) in self.instructions.iter().enumerate() {
            let coordinate = instruction.coordinate;
            let expected_order = u32::try_from(position)
                .map_err(|_| Error::InvalidTransaction("instruction count exceeds u32".into()))?;
            if coordinate.order != expected_order {
                return Err(Error::InvalidTransaction(format!(
                    "instruction order is {}, expected {expected_order}",
                    coordinate.order
                )));
            }
            if !matches!(instruction.data_coverage, InstructionDataCoverage::Exact)
                && !instruction.data.is_empty()
            {
                return Err(Error::InvalidTransaction(
                    "instruction has bytes without exact data coverage".into(),
                ));
            }
            if instruction.accounts.len() > MAX_CANONICAL_SHORT_VEC_ITEMS {
                return Err(Error::InvalidTransaction(format!(
                    "instruction {} account count {} exceeds the canonical short-vector limit",
                    coordinate.order,
                    instruction.accounts.len()
                )));
            }
            if instruction.data.len() > MAX_CANONICAL_SHORT_VEC_ITEMS {
                return Err(Error::InvalidTransaction(format!(
                    "instruction {} data length {} exceeds the canonical short-vector limit",
                    coordinate.order,
                    instruction.data.len()
                )));
            }

            match coordinate.inner_index {
                None => {
                    if coordinate.outer_index != expected_outer {
                        return Err(Error::InvalidTransaction(format!(
                            "outer instruction index is {}, expected {expected_outer}",
                            coordinate.outer_index
                        )));
                    }
                    if coordinate.stack_height.is_some() {
                        return Err(Error::InvalidTransaction(
                            "outer instruction has an inner stack height".into(),
                        ));
                    }
                    expected_outer = expected_outer.checked_add(1).ok_or_else(|| {
                        Error::InvalidTransaction("outer instruction index overflow".into())
                    })?;
                    next_inner = Some((coordinate.outer_index, 0u32));
                }
                Some(inner_index) => {
                    let Some((inner_outer, expected_inner)) = next_inner else {
                        return Err(Error::InvalidTransaction(
                            "inner instruction does not follow an outer instruction".into(),
                        ));
                    };
                    if coordinate.outer_index != inner_outer || inner_index != expected_inner {
                        return Err(Error::InvalidTransaction(format!(
                            "inner coordinate is ({}, {inner_index}), expected ({inner_outer}, {expected_inner})",
                            coordinate.outer_index
                        )));
                    }
                    if coordinate.stack_height == Some(0) {
                        return Err(Error::InvalidTransaction(
                            "inner instruction stack height is zero".into(),
                        ));
                    }
                    if let Some(failed) = self.header.failed_outer_instruction_index
                        && coordinate.outer_index > failed
                    {
                        return Err(Error::InvalidTransaction(format!(
                            "inner instruction belongs to outer index {}, after failed outer index {failed}",
                            coordinate.outer_index
                        )));
                    }
                    next_inner = Some((
                        inner_outer,
                        expected_inner.checked_add(1).ok_or_else(|| {
                            Error::InvalidTransaction("inner instruction index overflow".into())
                        })?,
                    ));
                }
            }
        }
        if matches!(
            self.header.instruction_coverage,
            InstructionCoverage::Complete
        ) && let Some(failed) = self.header.failed_outer_instruction_index
            && failed >= expected_outer
        {
            return Err(Error::InvalidTransaction(format!(
                "failed outer instruction index {failed} is outside {expected_outer} complete outer instructions"
            )));
        }
        Ok(())
    }

    fn as_view(&self, block: BlockHeader) -> TransactionView<'_> {
        TransactionView {
            block,
            header: self.header,
            primary_signature: self.primary_signature.as_ref(),
            required_signers: &self.required_signers,
            instructions: &self.instructions,
        }
    }
}

/// One owned block projection. Empty blocks are valid and are published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalBlock {
    pub header: BlockHeader,
    pub transactions: Vec<CanonicalTransaction>,
}

impl CanonicalBlock {
    /// Validate dense transaction indexes and canonical instruction order.
    pub fn validate(&self) -> Result<()> {
        for (position, transaction) in self.transactions.iter().enumerate() {
            let expected_tx_index = u32::try_from(position)
                .map_err(|_| Error::InvalidTransaction("transaction count exceeds u32".into()))?;
            transaction.validate(expected_tx_index)?;
        }
        Ok(())
    }

    pub fn as_view(&self) -> BlockView<'_> {
        BlockView {
            header: self.header,
            transactions: &self.transactions,
        }
    }
}

/// Borrowed block projection passed to an application sink.
#[derive(Debug, Clone, Copy)]
pub struct BlockView<'a> {
    pub header: BlockHeader,
    pub transactions: &'a [CanonicalTransaction],
}

impl<'a> BlockView<'a> {
    /// Iterate over transactions with the containing block identity attached.
    pub fn transaction_views(
        &self,
    ) -> impl ExactSizeIterator<Item = TransactionView<'a>> + DoubleEndedIterator + 'a {
        let header = self.header;
        self.transactions
            .iter()
            .map(move |transaction| transaction.as_view(header))
    }
}

/// Borrowed transaction projection used by the short transaction helper.
#[derive(Debug, Clone, Copy)]
pub struct TransactionView<'a> {
    pub block: BlockHeader,
    pub header: TransactionHeader,
    pub primary_signature: Option<&'a [u8; 64]>,
    pub required_signers: &'a [[u8; 32]],
    pub instructions: &'a [ResolvedInstruction],
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instruction(order: u32, outer: u32, inner: Option<u32>) -> ResolvedInstruction {
        ResolvedInstruction {
            coordinate: InstructionCoordinate {
                order,
                outer_index: outer,
                inner_index: inner,
                stack_height: inner.map(|_| 2),
            },
            program_id: [7; 32],
            accounts: vec![[8; 32]],
            data_coverage: InstructionDataCoverage::Exact,
            data: vec![3],
        }
    }

    fn block(instructions: Vec<ResolvedInstruction>) -> CanonicalBlock {
        CanonicalBlock {
            header: BlockHeader {
                epoch: 1,
                block_ordinal: 2,
                slot: 3,
            },
            transactions: vec![CanonicalTransaction {
                header: TransactionHeader {
                    tx_index: 0,
                    status: ExecutionStatus::Succeeded,
                    failed_outer_instruction_index: None,
                    instruction_coverage: InstructionCoverage::Complete,
                    cpi_coverage: CpiCoverage::Complete,
                },
                primary_signature: Some([5; 64]),
                required_signers: vec![[6; 32]],
                instructions,
            }],
        }
    }

    #[test]
    fn accepts_outer_then_inner_canonical_order() {
        block(vec![
            instruction(0, 0, None),
            instruction(1, 0, Some(0)),
            instruction(2, 0, Some(1)),
            instruction(3, 1, None),
        ])
        .validate()
        .unwrap();
    }

    #[test]
    fn rejects_storage_order_or_duplicate_inner_coordinates() {
        let wrong_outer = block(vec![instruction(0, 1, None)]);
        assert!(wrong_outer.validate().is_err());

        let duplicate_inner = block(vec![
            instruction(0, 0, None),
            instruction(1, 0, Some(0)),
            instruction(2, 0, Some(0)),
        ]);
        assert!(duplicate_inner.validate().is_err());
    }

    #[test]
    fn rejects_stack_height_on_outer_and_zero_on_inner() {
        let mut outer = instruction(0, 0, None);
        outer.coordinate.stack_height = Some(1);
        assert!(block(vec![outer]).validate().is_err());

        let mut inner = instruction(1, 0, Some(0));
        inner.coordinate.stack_height = Some(0);
        assert!(
            block(vec![instruction(0, 0, None), inner])
                .validate()
                .is_err()
        );
    }

    #[test]
    fn rejects_non_dense_transaction_indexes() {
        let mut candidate = block(vec![instruction(0, 0, None)]);
        candidate.transactions[0].header.tx_index = 1;
        assert!(candidate.validate().is_err());
    }

    #[test]
    fn validates_failed_outer_instruction_boundary() {
        let mut valid = block(vec![
            instruction(0, 0, None),
            instruction(1, 0, Some(0)),
            instruction(2, 1, None),
            instruction(3, 1, Some(0)),
            instruction(4, 2, None),
        ]);
        valid.transactions[0].header.status = ExecutionStatus::Failed;
        valid.transactions[0].header.failed_outer_instruction_index = Some(1);
        valid.validate().unwrap();

        let mut after_failure = valid.clone();
        after_failure.transactions[0]
            .instructions
            .push(instruction(5, 2, Some(0)));
        assert!(after_failure.validate().is_err());

        let mut outside_message = valid.clone();
        outside_message.transactions[0]
            .header
            .failed_outer_instruction_index = Some(3);
        assert!(outside_message.validate().is_err());

        let mut succeeded = valid;
        succeeded.transactions[0].header.status = ExecutionStatus::Succeeded;
        assert!(succeeded.validate().is_err());
    }

    #[test]
    fn rejects_values_above_canonical_message_geometry() {
        let mut too_many_signers = block(vec![instruction(0, 0, None)]);
        too_many_signers.transactions[0].required_signers =
            vec![[0; 32]; MAX_CANONICAL_REQUIRED_SIGNERS + 1];
        assert!(too_many_signers.validate().is_err());

        let mut too_much_data = block(vec![instruction(0, 0, None)]);
        too_much_data.transactions[0].instructions[0].data =
            vec![0; MAX_CANONICAL_SHORT_VEC_ITEMS + 1];
        assert!(too_much_data.validate().is_err());

        let mut too_many_accounts = block(vec![instruction(0, 0, None)]);
        too_many_accounts.transactions[0].instructions[0].accounts =
            vec![[0; 32]; MAX_CANONICAL_SHORT_VEC_ITEMS + 1];
        assert!(too_many_accounts.validate().is_err());
    }

    #[test]
    fn publishes_empty_blocks() {
        let candidate = CanonicalBlock {
            header: BlockHeader {
                epoch: 1,
                block_ordinal: 0,
                slot: 0,
            },
            transactions: Vec::new(),
        };
        candidate.validate().unwrap();
        assert_eq!(candidate.as_view().transaction_views().len(), 0);
    }
}
