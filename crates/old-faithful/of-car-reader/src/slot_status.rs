//! One `jetstreamer_slot_status` row for a decoded Old Faithful block.
//!
//! The counters follow Jetstreamer's `--no-plugins` writer. `transaction_count`
//! is the number of transactions in the block. A transaction is a vote when it
//! is a legacy message, carries one or two signatures, and its single
//! instruction calls the vote program. `block_time` is the block timestamp
//! clamped to a ClickHouse `DateTime`. A missing timestamp becomes Unix epoch.

use crate::car_block_group::CarBlockGroup;
use crate::error::GroupError;
use crate::versioned_transaction::{VersionedMessage, VersionedTransaction};

/// Vote111111111111111111111111111111111111111
const VOTE_PROGRAM_ID: [u8; 32] = [
    7, 97, 72, 29, 53, 116, 116, 187, 124, 77, 118, 36, 235, 211, 189, 179, 216, 53, 94, 115, 209,
    16, 67, 252, 13, 163, 83, 128, 0, 0, 0, 0,
];

/// Fields written to `jetstreamer_slot_status`, except `indexed_at`.
///
/// ClickHouse fills `indexed_at` at insert time. That column is the
/// `ReplacingMergeTree` version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotStatus {
    pub slot: u64,
    pub transaction_count: u32,
    pub vote_transaction_count: u32,
    pub non_vote_transaction_count: u32,
    pub thread_id: u8,
    pub block_time: u32,
}

/// Build the status row for one block.
///
/// `thread_id` is stored as Jetstreamer stores the firehose thread index. A
/// single reader uses `0`.
pub fn slot_status_from_group(
    group: &mut CarBlockGroup,
    thread_id: u8,
) -> Result<SlotStatus, GroupError> {
    let slot = group
        .slot
        .ok_or_else(|| GroupError::Other("block has no slot".to_string()))?;
    let block_time = clamp_block_time(group.block_time);
    let mut votes = 0u64;
    let mut non_votes = 0u64;
    let mut transactions = group.transactions_no_meta();
    while let Some(transaction) = transactions.next_tx()? {
        if is_simple_vote_transaction(transaction) {
            votes += 1;
        } else {
            non_votes += 1;
        }
    }
    Ok(SlotStatus {
        slot,
        transaction_count: cap_u32(votes + non_votes),
        vote_transaction_count: cap_u32(votes),
        non_vote_transaction_count: cap_u32(non_votes),
        thread_id,
        block_time,
    })
}

/// Jetstreamer's `is_simple_vote_transaction` rule.
pub fn is_simple_vote_transaction(transaction: &VersionedTransaction<'_>) -> bool {
    if !(1..=2).contains(&transaction.signatures.len()) {
        return false;
    }
    let VersionedMessage::Legacy(message) = &transaction.message else {
        return false;
    };
    if message.instructions.len() != 1 {
        return false;
    }
    let program_index = usize::from(message.instructions[0].program_id_index);
    message
        .account_keys
        .get(program_index)
        .is_some_and(|program_id| **program_id == VOTE_PROGRAM_ID)
}

fn clamp_block_time(block_time: Option<i64>) -> u32 {
    match block_time {
        Some(timestamp) if timestamp > 0 && timestamp <= i64::from(u32::MAX) => timestamp as u32,
        Some(timestamp) if timestamp > i64::from(u32::MAX) => u32::MAX,
        _ => 0,
    }
}

fn cap_u32(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versioned_transaction::{
        CompiledInstruction, LegacyMessage, MessageHeader, V0Message,
    };

    const SIGNATURE: [u8; 64] = [1; 64];
    const BLOCKHASH: [u8; 32] = [2; 32];
    const OTHER_PROGRAM: [u8; 32] = [9; 32];

    fn legacy(
        signature_count: usize,
        instruction_count: usize,
        program: &'static [u8; 32],
    ) -> VersionedTransaction<'static> {
        VersionedTransaction {
            signatures: vec![&SIGNATURE; signature_count],
            message: VersionedMessage::Legacy(LegacyMessage {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![program],
                recent_blockhash: &BLOCKHASH,
                instructions: vec![
                    CompiledInstruction {
                        program_id_index: 0,
                        accounts: Vec::new(),
                        data: Vec::new(),
                    };
                    instruction_count
                ],
            }),
        }
    }

    #[test]
    fn simple_vote_matches_jetstreamer() {
        assert!(is_simple_vote_transaction(&legacy(1, 1, &VOTE_PROGRAM_ID)));
        assert!(is_simple_vote_transaction(&legacy(2, 1, &VOTE_PROGRAM_ID)));
        assert!(!is_simple_vote_transaction(&legacy(3, 1, &VOTE_PROGRAM_ID)));
        assert!(!is_simple_vote_transaction(&legacy(0, 1, &VOTE_PROGRAM_ID)));
        assert!(!is_simple_vote_transaction(&legacy(1, 2, &VOTE_PROGRAM_ID)));
        assert!(!is_simple_vote_transaction(&legacy(1, 1, &OTHER_PROGRAM)));
    }

    #[test]
    fn versioned_message_is_not_a_simple_vote() {
        let transaction = VersionedTransaction {
            signatures: vec![&SIGNATURE],
            message: VersionedMessage::V0(V0Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![&VOTE_PROGRAM_ID],
                recent_blockhash: &BLOCKHASH,
                instructions: vec![CompiledInstruction {
                    program_id_index: 0,
                    accounts: Vec::new(),
                    data: Vec::new(),
                }],
                address_table_lookups: Vec::new(),
            }),
        };
        assert!(!is_simple_vote_transaction(&transaction));
    }

    #[test]
    fn block_time_clamps_to_clickhouse_datetime() {
        assert_eq!(clamp_block_time(None), 0);
        assert_eq!(clamp_block_time(Some(0)), 0);
        assert_eq!(clamp_block_time(Some(-1)), 0);
        assert_eq!(clamp_block_time(Some(1_700_000_000)), 1_700_000_000);
        assert_eq!(clamp_block_time(Some(i64::from(u32::MAX) + 1)), u32::MAX);
    }
}
