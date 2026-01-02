use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::str::FromStr;
use wincode::{SchemaRead, SchemaWrite};

use crate::{CompactLogStream, Registry};

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
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

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactInnerInstructions {
    pub index: u32,
    pub instructions: Vec<CompactInnerInstruction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactInnerInstruction {
    pub program_id_index: u32, // message index
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactReturnData {
    pub program_id_index: u32, // registry index
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactTokenBalance {
    pub account_index: u32,

    // registry indices
    pub mint_index: u32,
    pub owner_index: u32,
    pub program_id_index: u32,

    pub amount: u64,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactReward {
    pub pubkey_index: u32,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: i32,
    pub commission: Option<u8>,
}

pub fn compact_meta_from_proto(
    meta: &car_reader::confirmed_block::TransactionStatusMeta,
    registry: &Registry,
) -> Result<CompactMetaV1> {
    let err = meta.err.as_ref().map(|e| e.err.clone());

    let loaded_writable_indices = map_loaded_addrs(&meta.loaded_writable_addresses, registry)?;
    let loaded_readonly_indices = map_loaded_addrs(&meta.loaded_readonly_addresses, registry)?;

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
                            accounts: ix.accounts.clone(),
                            data: ix.data.clone(),
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
        Some(crate::log::parse_logs(&meta.log_messages, registry))
    };

    let pre_token_balances = meta
        .pre_token_balances
        .iter()
        .map(|tb| compact_token_balance(tb, registry))
        .collect::<Result<Vec<_>>>()?;

    let post_token_balances = meta
        .post_token_balances
        .iter()
        .map(|tb| compact_token_balance(tb, registry))
        .collect::<Result<Vec<_>>>()?;

    let rewards = meta
        .rewards
        .iter()
        .map(|rw| compact_reward(rw, registry))
        .collect::<Result<Vec<_>>>()?;

    let return_data = if meta.return_data_none {
        None
    } else {
        meta.return_data
            .as_ref()
            .map(|rd| -> Result<CompactReturnData> {
                anyhow::ensure!(
                    rd.program_id.len() == 32,
                    "return_data program_id invalid len"
                );
                let mut a = [0u8; 32];
                a.copy_from_slice(&rd.program_id);
                let ix = registry
                    .lookup(&a)
                    .context("return_data program_id not in registry")?;
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
        pre_balances: meta.pre_balances.clone(),
        post_balances: meta.post_balances.clone(),

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

fn map_loaded_addrs(addrs: &Vec<Vec<u8>>, registry: &Registry) -> Result<Vec<u32>> {
    let mut out = Vec::with_capacity(addrs.len());
    for pk in addrs {
        if pk.len() != 32 {
            continue;
        }
        let mut a = [0u8; 32];
        a.copy_from_slice(pk);
        out.push(registry.lookup(&a).context("loaded addr not in registry")?);
    }
    Ok(out)
}

#[inline]
fn lookup_pubkey_index_optional(registry: &Registry, s: &str) -> u32 {
    if s.is_empty() {
        return 0;
    }

    match Pubkey::from_str(s) {
        Ok(pk) => registry.lookup(&pk.to_bytes()).unwrap_or(0),
        Err(_) => 0,
    }
}

fn compact_token_balance(
    tb: &car_reader::confirmed_block::TokenBalance,
    registry: &Registry,
) -> Result<CompactTokenBalance> {
    let mint = Pubkey::from_str(&tb.mint)
        .context("token mint parse")?
        .to_bytes();

    let mint_index = registry.lookup(&mint).context("mint not in registry")?;

    // OPTIONAL owner + program_id
    let owner_index = lookup_pubkey_index_optional(registry, &tb.owner);
    let program_id_index = lookup_pubkey_index_optional(registry, &tb.program_id);

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
        owner_index,      // 0 == unknown
        program_id_index, // 0 == unknown
        amount,
        decimals,
    })
}

fn compact_reward(
    rw: &car_reader::confirmed_block::Reward,
    registry: &Registry,
) -> Result<CompactReward> {
    let pk = Pubkey::from_str(&rw.pubkey)
        .context("reward pubkey parse")?
        .to_bytes();
    let pubkey_index = registry
        .lookup(&pk)
        .context("reward pubkey not in registry")?;

    let commission = rw.commission.parse::<u8>().ok();

    Ok(CompactReward {
        pubkey_index,
        lamports: rw.lamports,
        post_balance: rw.post_balance,
        reward_type: rw.reward_type,
        commission,
    })
}
