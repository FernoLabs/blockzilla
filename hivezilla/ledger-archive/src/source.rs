use std::collections::BTreeMap;

use anyhow::{Context, Result};
use blockzilla_compact::CompactTransactionError;

/// Agave `NUM_SLOTS_FOR_REWARD`. A skip or notarization reward for slot N is
/// carried by the footer of slot N+8.
pub(crate) const REWARD_SLOT_DISTANCE: u64 = 8;

/// One inclusive ledger window and every slot the checker may read.
///
/// A missing key means the slot has no `SlotMeta`. Slots past `end_slot` are
/// certificate carriers for holes near the end of the window.
#[derive(Debug, Clone)]
pub(crate) struct LedgerView {
    pub start_slot: u64,
    pub end_slot: u64,
    pub slots: BTreeMap<u64, SlotData>,
}

#[derive(Debug, Clone)]
pub(crate) struct SlotData {
    pub slot: u64,
    pub is_root: bool,
    pub is_full: bool,
    pub parent_slot: Option<u64>,
    pub previous_blockhash: Option<[u8; 32]>,
    pub blockhash: [u8; 32],
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub num_partitions: Option<u64>,
    pub rewards: Vec<SourceReward>,
    pub consensus_block_id: Option<[u8; 32]>,
    pub markers: Vec<Vec<u8>>,
    pub entries: Vec<SourceEntry>,
    pub shredding: Vec<SourceShredGroup>,
    pub transactions: Vec<SourceTx>,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceEntry {
    pub num_hashes: u64,
    pub hash: [u8; 32],
    pub tx_count: u32,
    pub signature_count: u32,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceShredGroup {
    pub entry_end_idx: i64,
    pub shred_end_idx: i64,
}

/// One completed shred range before the cumulative entry index is applied.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RawComponent {
    pub entry_count: u64,
    /// Exclusive end of the Agave shred range.
    pub shred_range_end: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceTx {
    pub signatures: Vec<[u8; 64]>,
    pub message: SourceMessage,
    pub status: Option<SourceStatus>,
}

#[derive(Debug, Clone)]
pub(crate) enum SourceMessage {
    Legacy(SourceMessageBody),
    V0(SourceMessageBody),
    V1(SourceV1Message),
}

#[derive(Debug, Clone)]
pub(crate) struct SourceMessageBody {
    pub header: SourceHeader,
    pub account_keys: Vec<[u8; 32]>,
    pub recent_blockhash: [u8; 32],
    pub instructions: Vec<SourceInstruction>,
    pub lookups: Vec<SourceLookup>,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceV1Message {
    pub header: SourceHeader,
    pub config: SourceTxConfig,
    pub account_keys: Vec<[u8; 32]>,
    pub recent_blockhash: [u8; 32],
    pub instructions: Vec<SourceInstruction>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceTxConfig {
    pub priority_fee: Option<u64>,
    pub compute_unit_limit: Option<u32>,
    pub loaded_accounts_data_size_limit: Option<u32>,
    pub heap_size: Option<u32>,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceLookup {
    pub account_key: [u8; 32],
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceStatus {
    pub err: Option<CompactTransactionError>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<SourceInnerGroup>>,
    pub logs: Option<Vec<String>>,
    pub pre_token_balances: Vec<SourceTokenBalance>,
    pub post_token_balances: Vec<SourceTokenBalance>,
    pub rewards: Vec<SourceReward>,
    pub loaded_writable: Vec<[u8; 32]>,
    pub loaded_readonly: Vec<[u8; 32]>,
    pub return_data: Option<SourceReturnData>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
}

impl SourceStatus {
    pub(crate) fn success() -> Self {
        Self {
            err: None,
            fee: 0,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable: Vec::new(),
            loaded_readonly: Vec::new(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SourceInnerGroup {
    pub index: u32,
    pub instructions: Vec<SourceInnerInstruction>,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceInnerInstruction {
    pub program_id_index: u32,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceTokenBalance {
    pub account_index: u32,
    pub mint: [u8; 32],
    pub owner: Option<[u8; 32]>,
    pub program_id: Option<[u8; 32]>,
    pub amount: u64,
    pub decimals: u8,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceReturnData {
    pub program_id: [u8; 32],
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct SourceReward {
    pub pubkey: [u8; 32],
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: i32,
    pub commission: Option<u8>,
}

/// `entry_end_idx` is the cumulative entry count after the component.
/// `shred_end_idx` is the last shred in the range, so it is `range.end - 1`.
pub(crate) fn shred_groups(components: &[RawComponent]) -> Result<Vec<SourceShredGroup>> {
    let mut entry_end = 0u64;
    let mut groups = Vec::with_capacity(components.len());
    for component in components {
        entry_end = entry_end
            .checked_add(component.entry_count)
            .context("entry index overflow")?;
        let shred_end = component
            .shred_range_end
            .checked_sub(1)
            .context("shred range is empty")?;
        groups.push(SourceShredGroup {
            entry_end_idx: i64::try_from(entry_end).context("entry index exceeds i64")?,
            shred_end_idx: i64::from(shred_end),
        });
    }
    Ok(groups)
}

#[cfg(test)]
pub(crate) fn test_block(
    slot: u64,
    parent_slot: u64,
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
) -> SlotData {
    SlotData {
        slot,
        is_root: true,
        is_full: true,
        parent_slot: Some(parent_slot),
        previous_blockhash: Some(previous_blockhash),
        blockhash,
        block_time: None,
        block_height: None,
        num_partitions: None,
        rewards: Vec::new(),
        consensus_block_id: None,
        markers: Vec::new(),
        entries: vec![SourceEntry {
            num_hashes: 1,
            hash: blockhash,
            tx_count: 0,
            signature_count: 0,
        }],
        shredding: vec![SourceShredGroup {
            entry_end_idx: 1,
            shred_end_idx: 31,
        }],
        transactions: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_entry_batches_produce_two_shredding_rows() {
        let groups = shred_groups(&[
            RawComponent {
                entry_count: 4,
                shred_range_end: 32,
            },
            RawComponent {
                entry_count: 7,
                shred_range_end: 96,
            },
        ])
        .unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].entry_end_idx, 4);
        assert_eq!(groups[0].shred_end_idx, 31);
        assert_eq!(groups[1].entry_end_idx, 11);
        assert_eq!(groups[1].shred_end_idx, 95);
    }

    #[test]
    fn a_marker_keeps_the_entry_cursor_and_still_adds_a_row() {
        let groups = shred_groups(&[
            RawComponent {
                entry_count: 4,
                shred_range_end: 32,
            },
            RawComponent {
                entry_count: 0,
                shred_range_end: 64,
            },
            RawComponent {
                entry_count: 7,
                shred_range_end: 96,
            },
        ])
        .unwrap();

        assert_eq!(groups.len(), 3);
        assert_eq!(groups[1].entry_end_idx, 4);
        assert_eq!(groups[1].shred_end_idx, 63);
        assert_eq!(groups[2].entry_end_idx, 11);
    }
}
