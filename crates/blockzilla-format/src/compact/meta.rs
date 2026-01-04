use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::str::FromStr;

use crate::{CompactLogStream, KeyIndex};

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactMetaV1 {
    pub err: Option<Vec<u8>>,

    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,

    pub inner_instructions: Option<Vec<CompactInnerInstructions>>,
    pub logs: Option<CompactLogStream>,

    pub pre_token_balances: Vec<CompactTokenBalance>,
    pub post_token_balances: Vec<CompactTokenBalance>,

    pub rewards: Vec<CompactReward>,

    pub loaded_writable_indices: Vec<u32>,
    pub loaded_readonly_indices: Vec<u32>,

    pub return_data: Option<CompactReturnData>,

    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactInnerInstructions {
    pub index: u32,
    pub instructions: Vec<CompactInnerInstruction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactInnerInstruction {
    pub program_id_index: u32, // message index
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactReturnData {
    pub program_id_index: u32, // registry index
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactTokenBalance {
    pub account_index: u32,

    // registry indices or 0 if missing or not found
    pub mint_index: u32,
    pub owner_index: u32,
    pub program_id_index: u32,

    pub amount: u64,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactReward {
    pub pubkey_index: u32,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: i32,
    pub commission: Option<u8>,
}

pub fn compact_meta_from_proto(
    meta: &car_reader::confirmed_block::TransactionStatusMeta,
    index: &KeyIndex,
) -> Result<CompactMetaV1> {
    let err = meta.err.as_ref().map(|e| e.err.clone());

    let loaded_writable_indices = meta
        .loaded_writable_addresses
        .iter()
        .map(|a| index.lookup_unchecked(a.as_slice().try_into().unwrap()))
        .collect();
    let loaded_readonly_indices = meta
        .loaded_readonly_addresses
        .iter()
        .map(|a| index.lookup_unchecked(a.as_slice().try_into().unwrap()))
        .collect();

    let inner_instructions = if meta.inner_instructions_none {
        None
    } else {
        Some(
            meta.inner_instructions
                .iter()
                .map(|ii| CompactInnerInstructions {
                    index: ii.index,
                    instructions: ii
                        .instructions
                        .iter()
                        .map(|ix| CompactInnerInstruction {
                            program_id_index: ix.program_id_index,
                            accounts: ix.accounts.to_vec(),
                            data: ix.data.to_vec(),
                            stack_height: ix.stack_height,
                        })
                        .collect(),
                })
                .collect(),
        )
    };

    let logs = if meta.log_messages_none {
        None
    } else {
        Some(crate::log::parse_logs(&meta.log_messages, index))
    };

    //TODO: fix error amangement it is easier to just unwrap to avoid typing issue for now
    let pre_token_balances = meta
        .pre_token_balances
        .iter()
        .map(|tb| compact_token_balance(tb, index).unwrap())
        .collect();

    let post_token_balances = meta
        .post_token_balances
        .iter()
        .map(|tb| compact_token_balance(tb, index).unwrap())
        .collect();

    let rewards = meta
        .rewards
        .iter()
        .map(|rw| compact_reward(rw, index).unwrap())
        .collect();

    let return_data = if meta.return_data_none {
        None
    } else {
        meta.return_data
            .as_ref()
            .map(|rd| -> Result<CompactReturnData> {
                let ix = index.lookup_unchecked(rd.program_id.as_slice().try_into().unwrap());
                Ok(CompactReturnData {
                    program_id_index: ix,
                    data: rd.data.clone(),
                })
            })
            .transpose()?
    };

    Ok(CompactMetaV1 {
        err,

        fee: meta.fee,
        pre_balances: meta.pre_balances.to_vec(),
        post_balances: meta.post_balances.to_vec(),

        inner_instructions,
        logs,

        pre_token_balances,
        post_token_balances,

        rewards,

        loaded_writable_indices,
        loaded_readonly_indices,

        return_data,

        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
}

#[inline]
fn lookup_pubkey_index_optional(index: &KeyIndex, s: &str) -> u32 {
    if s.is_empty() {
        return 0;
    }
    index.lookup_str(s).expect("invalid pubkey")
}

fn compact_token_balance(
    tb: &car_reader::confirmed_block::TokenBalance,
    index: &KeyIndex,
) -> Result<CompactTokenBalance> {
    let mint_index = lookup_pubkey_index_optional(index, &tb.mint);
    let owner_index = lookup_pubkey_index_optional(index, &tb.owner);
    let program_id_index = lookup_pubkey_index_optional(index, &tb.program_id);

    let (amount, decimals) = match &tb.ui_token_amount {
        None => (0u64, 0u8),
        Some(uta) => {
            let amount = uta
                .amount
                .parse::<u64>()
                .context("parse token amount u64")?;
            (amount, uta.decimals as u8)
        }
    };

    Ok(CompactTokenBalance {
        account_index: tb.account_index,
        mint_index,
        owner_index,
        program_id_index,
        amount,
        decimals,
    })
}

fn compact_reward(
    rw: &car_reader::confirmed_block::Reward,
    index: &KeyIndex,
) -> Result<CompactReward> {
    let pk = Pubkey::from_str(&rw.pubkey)
        .context("reward pubkey parse")?
        .to_bytes();
    let pubkey_index = index.lookup_unchecked(&pk);

    let commission = rw.commission.parse::<u8>().ok();

    Ok(CompactReward {
        pubkey_index,
        lamports: rw.lamports,
        post_balance: rw.post_balance,
        reward_type: rw.reward_type,
        commission,
    })
}
