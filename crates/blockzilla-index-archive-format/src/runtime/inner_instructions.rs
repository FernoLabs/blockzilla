//! `runtime/inner_instructions.wincode`: recorded CPI structure and data.
//!
//! A dense record exists only for `SourcePresent` or `BackfillPresent` in the
//! transaction row's [`EffectState`](crate::ledger::transactions::EffectState).
//! The state byte is the sole owner of absent, not-recorded, and recorded-empty
//! CPI states. Thus this file never stores an empty record. Each instruction
//! owns its payload bytes here; there is no second instruction-data object.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    ledger::transactions::{Instruction, MAX_ACCOUNTS_PER_INSTRUCTION, MAX_INSTRUCTION_DATA_LEN},
    wincode::{self as wire, ArchiveWincodeConfig},
};

pub const PATH: &str = "runtime/inner_instructions.wincode";
pub const SCHEMA: u16 = 1;
pub const MAX_GROUPS_PER_TRANSACTION: usize = 1 << 16;
pub const MAX_INNER_INSTRUCTIONS_PER_TRANSACTION: usize = 1 << 20;
pub const MAX_STACK_HEIGHT: u32 = 64;

/// All recorded CPI for one transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct TransactionInner {
    pub groups: Vec<InnerGroup>,
}

/// CPI invocations made by one top-level instruction.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct InnerGroup {
    pub parent_index: u32,
    pub instructions: Vec<InnerInstruction>,
}

/// One inner instruction. The payload is stored here exactly once.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct InnerInstruction {
    /// `None` means the source did not record stack height.
    pub stack_height: Option<u32>,
    pub instruction: Instruction,
}

impl TransactionInner {
    /// Validate one dense record against its owning transaction.
    pub fn validate(
        &self,
        top_level_instruction_count: usize,
        resolved_account_count: usize,
    ) -> Result<(), InnerInstructionError> {
        if self.groups.is_empty() {
            return Err(InnerInstructionError::EmptyDenseRecord);
        }
        if self.groups.len() > MAX_GROUPS_PER_TRANSACTION {
            return Err(InnerInstructionError::TooManyGroups(self.groups.len()));
        }
        let mut previous_parent = None;
        let mut instruction_count = 0_usize;
        for group in &self.groups {
            if group.parent_index as usize >= top_level_instruction_count {
                return Err(InnerInstructionError::ParentOutsideTransaction(
                    group.parent_index,
                ));
            }
            if previous_parent.is_some_and(|previous| group.parent_index <= previous) {
                return Err(InnerInstructionError::ParentsNotAscending);
            }
            previous_parent = Some(group.parent_index);
            instruction_count = instruction_count
                .checked_add(group.instructions.len())
                .ok_or(InnerInstructionError::TooManyInstructions(usize::MAX))?;
            if instruction_count > MAX_INNER_INSTRUCTIONS_PER_TRANSACTION {
                return Err(InnerInstructionError::TooManyInstructions(
                    instruction_count,
                ));
            }
            for inner in &group.instructions {
                if inner
                    .stack_height
                    .is_some_and(|height| height > MAX_STACK_HEIGHT)
                {
                    return Err(InnerInstructionError::StackTooDeep(
                        inner.stack_height.unwrap_or_default(),
                    ));
                }
                validate_instruction(&inner.instruction, resolved_account_count)?;
            }
        }
        Ok(())
    }
}

fn validate_instruction(
    instruction: &Instruction,
    resolved_account_count: usize,
) -> Result<(), InnerInstructionError> {
    if instruction.account_positions.len() > MAX_ACCOUNTS_PER_INSTRUCTION {
        return Err(InnerInstructionError::TooManyAccounts(
            instruction.account_positions.len(),
        ));
    }
    if instruction.data.len() > MAX_INSTRUCTION_DATA_LEN {
        return Err(InnerInstructionError::DataTooLong(instruction.data.len()));
    }
    if instruction.program_position as usize >= resolved_account_count
        || instruction
            .account_positions
            .iter()
            .any(|position| *position as usize >= resolved_account_count)
    {
        return Err(InnerInstructionError::AccountOutsideTransaction);
    }
    Ok(())
}

/// Append one dense record to an uncompressed effect chunk.
pub fn append_record(
    chunk: &mut Vec<u8>,
    record: &TransactionInner,
    top_level_instruction_count: usize,
    resolved_account_count: usize,
) -> Result<(), InnerInstructionError> {
    record.validate(top_level_instruction_count, resolved_account_count)?;
    wincode::config::serialize_into(chunk, record, wire::archive_wincode_config())?;
    Ok(())
}

/// Encode one dense record.
pub fn encode_record(
    record: &TransactionInner,
    top_level_instruction_count: usize,
    resolved_account_count: usize,
) -> Result<Vec<u8>, InnerInstructionError> {
    let mut bytes = Vec::new();
    append_record(
        &mut bytes,
        record,
        top_level_instruction_count,
        resolved_account_count,
    )?;
    Ok(bytes)
}

/// Decode a chunk with the exact dense record count from `EffectState` rank.
pub fn decode_chunk(
    bytes: &[u8],
    record_count: u32,
) -> Result<Vec<TransactionInner>, InnerInstructionError> {
    let mut remaining = bytes;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let record =
            <TransactionInner as SchemaRead<'_, ArchiveWincodeConfig>>::get(&mut remaining)?;
        if record.groups.is_empty() {
            return Err(InnerInstructionError::EmptyDenseRecord);
        }
        records.push(record);
    }
    if !remaining.is_empty() {
        return Err(InnerInstructionError::TrailingBytes(remaining.len()));
    }
    Ok(records)
}

#[derive(Debug, Error)]
pub enum InnerInstructionError {
    #[error("inner-instruction Wincode: {0}")]
    WincodeRead(#[from] wincode::ReadError),
    #[error("inner-instruction Wincode: {0}")]
    WincodeWrite(#[from] wincode::WriteError),
    #[error("recorded-empty CPI is owned by EffectState and must not have a dense record")]
    EmptyDenseRecord,
    #[error("transaction has {0} CPI groups, above the decode guard")]
    TooManyGroups(usize),
    #[error("transaction has {0} inner instructions, above the decode guard")]
    TooManyInstructions(usize),
    #[error("CPI parent {0} is outside the top-level instruction list")]
    ParentOutsideTransaction(u32),
    #[error("CPI parent indexes are not strictly ascending")]
    ParentsNotAscending,
    #[error("recorded stack height {0} is above the decode guard")]
    StackTooDeep(u32),
    #[error("inner instruction has {0} accounts, above the decode guard")]
    TooManyAccounts(usize),
    #[error("inner instruction data has {0} bytes, above the decode guard")]
    DataTooLong(usize),
    #[error("inner instruction account position is outside the resolved transaction accounts")]
    AccountOutsideTransaction,
    #[error("inner-instruction chunk has {0} trailing bytes")]
    TrailingBytes(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(data: &[u8]) -> TransactionInner {
        TransactionInner {
            groups: vec![InnerGroup {
                parent_index: 0,
                instructions: vec![InnerInstruction {
                    stack_height: Some(2),
                    instruction: Instruction {
                        program_position: 1,
                        account_positions: vec![0, 2],
                        data: data.to_vec(),
                    },
                }],
            }],
        }
    }

    #[test]
    fn structure_and_data_have_one_round_trip() {
        let one = record(&[0xaa, 0xbb]);
        let two = record(&[]);
        let mut chunk = Vec::new();
        append_record(&mut chunk, &one, 1, 3).unwrap();
        append_record(&mut chunk, &two, 1, 3).unwrap();
        assert_eq!(decode_chunk(&chunk, 2).unwrap(), [one, two]);
    }

    #[test]
    fn empty_is_only_in_effect_state() {
        let error = encode_record(&TransactionInner::default(), 1, 1).unwrap_err();
        assert!(matches!(error, InnerInstructionError::EmptyDenseRecord));
    }

    #[test]
    fn golden_bytes_freeze_merged_layout() {
        let bytes = encode_record(&record(&[0xaa, 0xbb]), 1, 3).unwrap();
        assert_eq!(bytes, [1, 0, 1, 1, 2, 1, 2, 0, 2, 2, 0xaa, 0xbb]);
    }
}
