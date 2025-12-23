use anyhow::{Context, Result, bail};
use bincode::Options;
use prost::Message;
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use solana_transaction_error::TransactionResult;
use solana_transaction_status_client_types::{
    InnerInstruction as LegacyInnerInstruction, InnerInstructions as LegacyInnerInstructions,
    Reward as LegacyReward,
};
use std::io::Cursor;
use wincode::deserialize as wincode_deserialize;

use crate::confirmed_block::{self, TransactionStatusMeta};

#[derive(Debug, Serialize, Deserialize)]
struct LegacyTransactionStatusMeta {
    status: TransactionResult<()>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<LegacyInnerInstructions>>,
    log_messages: Option<Vec<String>>,
    pre_token_balances: Option<Vec<LegacyTransactionTokenBalance>>,
    post_token_balances: Option<Vec<LegacyTransactionTokenBalance>>,
    rewards: Option<Vec<LegacyReward>>,
    loaded_addresses: LegacyLoadedAddresses,
    return_data: Option<LegacyReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LegacyLoadedAddresses {
    writable: Vec<Pubkey>,
    readonly: Vec<Pubkey>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LegacyReturnData {
    program_id: Pubkey,
    data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LegacyTransactionTokenBalance {
    account_index: u8,
    mint: String,
    ui_token_amount: LegacyUiTokenAmount,
    owner: String,
    program_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct LegacyUiTokenAmount {
    ui_amount: Option<f64>,
    decimals: u8,
    amount: String,
    ui_amount_string: String,
}

#[inline]
pub fn is_zstd(buf: &[u8]) -> bool {
    buf.get(0..4)
        .map(|m| m == [0x28, 0xB5, 0x2F, 0xFD])
        .unwrap_or(false)
}

#[inline]
fn decode_proto(bytes: &[u8]) -> Result<TransactionStatusMeta> {
    TransactionStatusMeta::decode(&mut Cursor::new(bytes)).context("prost decode failed")
}

fn decode_proto_with_prefix(bytes: &[u8]) -> Result<TransactionStatusMeta> {
    match decode_proto(bytes) {
        Ok(meta) => Ok(meta),
        Err(first_err) => {
            if bytes.first().map(|b| *b <= 1).unwrap_or(false) && bytes.len() > 1 {
                decode_proto(&bytes[1..]).map_err(|second_err| {
                    first_err.context(format!(
                        "prost decode failed; retry after skipping prefix byte {}: {}",
                        bytes[0], second_err
                    ))
                })
            } else {
                Err(first_err)
            }
        }
    }
}

fn decode_proto_or_legacy(bytes: &[u8]) -> Result<TransactionStatusMeta> {
    match decode_proto_with_prefix(bytes) {
        Ok(meta) => Ok(meta),
        Err(proto_err) => decode_legacy(bytes).map_err(|legacy_err| {
            proto_err.context(format!("legacy bincode decode also failed: {legacy_err}"))
        }),
    }
}

fn decode_legacy(meta_bytes: &[u8]) -> Result<TransactionStatusMeta> {
    let legacy: LegacyTransactionStatusMeta = bincode_options()
        .deserialize(meta_bytes)
        .context("decode legacy bincode meta")?;

    convert_legacy_meta(legacy)
}

pub fn decode_transaction_status_meta_bytes(
    meta_zstd_or_raw: &[u8],
) -> Result<TransactionStatusMeta> {
    if meta_zstd_or_raw.is_empty() {
        bail!("empty metadata buffer (no meta bytes provided)");
    }

    let raw = if is_zstd(meta_zstd_or_raw) {
        zstd::decode_all(meta_zstd_or_raw).context("zstd decompress meta")?
    } else {
        meta_zstd_or_raw.to_vec()
    };

    if let Ok(wrapped) = wincode_deserialize::<Option<Vec<u8>>>(&raw) {
        return match wrapped {
            Some(inner) if inner.is_empty() => {
                bail!("metadata Option wrapper contained empty payload")
            }
            Some(inner) => decode_proto_or_legacy(&inner)
                .context("decode wincode Option-wrapped TransactionStatusMeta"),
            None => bail!("metadata missing (wincode Option::None wrapper)"),
        };
    }

    decode_proto_or_legacy(&raw)
}

fn convert_legacy_meta(meta: LegacyTransactionStatusMeta) -> Result<TransactionStatusMeta> {
    let mut pb = TransactionStatusMeta::default();

    pb.err = match meta.status {
        Ok(()) => None,
        Err(e) => Some(confirmed_block::TransactionError {
            err: bincode_options()
                .serialize(&e)
                .context("encode legacy TransactionError")?,
        }),
    };

    pb.fee = meta.fee;
    pb.pre_balances = meta.pre_balances;
    pb.post_balances = meta.post_balances;

    pb.inner_instructions = meta
        .inner_instructions
        .as_ref()
        .map(|list| list.iter().map(convert_inner_instructions).collect())
        .unwrap_or_default();
    pb.inner_instructions_none = meta.inner_instructions.is_none();

    pb.log_messages = meta.log_messages.clone().unwrap_or_default();
    pb.log_messages_none = meta.log_messages.is_none();

    pb.pre_token_balances = meta
        .pre_token_balances
        .unwrap_or_default()
        .into_iter()
        .map(convert_token_balance)
        .collect();
    pb.post_token_balances = meta
        .post_token_balances
        .unwrap_or_default()
        .into_iter()
        .map(convert_token_balance)
        .collect();

    pb.rewards = meta
        .rewards
        .unwrap_or_default()
        .into_iter()
        .map(convert_reward)
        .collect();

    pb.loaded_writable_addresses = meta
        .loaded_addresses
        .writable
        .iter()
        .map(|pk| pk.to_bytes().to_vec())
        .collect();
    pb.loaded_readonly_addresses = meta
        .loaded_addresses
        .readonly
        .iter()
        .map(|pk| pk.to_bytes().to_vec())
        .collect();

    pb.return_data = meta.return_data.as_ref().map(convert_return_data);
    pb.return_data_none = meta.return_data.is_none();

    pb.compute_units_consumed = meta.compute_units_consumed;
    pb.cost_units = meta.cost_units;

    Ok(pb)
}

#[inline]
fn bincode_options() -> impl bincode::Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

fn convert_inner_instruction(ix: &LegacyInnerInstruction) -> confirmed_block::InnerInstruction {
    confirmed_block::InnerInstruction {
        program_id_index: ix.instruction.program_id_index as u32,
        accounts: ix.instruction.accounts.clone(),
        data: ix.instruction.data.clone(),
        stack_height: ix.stack_height,
    }
}

fn convert_inner_instructions(
    list: &LegacyInnerInstructions,
) -> confirmed_block::InnerInstructions {
    confirmed_block::InnerInstructions {
        index: list.index as u32,
        instructions: list
            .instructions
            .iter()
            .map(convert_inner_instruction)
            .collect(),
    }
}

fn convert_token_balance(tb: LegacyTransactionTokenBalance) -> confirmed_block::TokenBalance {
    confirmed_block::TokenBalance {
        account_index: tb.account_index as u32,
        mint: tb.mint,
        ui_token_amount: Some(confirmed_block::UiTokenAmount {
            ui_amount: tb.ui_token_amount.ui_amount.unwrap_or_default(),
            decimals: tb.ui_token_amount.decimals as u32,
            amount: tb.ui_token_amount.amount,
            ui_amount_string: tb.ui_token_amount.ui_amount_string,
        }),
        owner: tb.owner,
        program_id: tb.program_id,
    }
}

fn convert_reward(r: LegacyReward) -> confirmed_block::Reward {
    confirmed_block::Reward {
        pubkey: r.pubkey,
        lamports: r.lamports,
        post_balance: r.post_balance,
        reward_type: r.reward_type.map(|rt| rt as i32).unwrap_or_default(),
        commission: r.commission.map(|c| c.to_string()).unwrap_or_default(),
    }
}

fn convert_return_data(rd: &LegacyReturnData) -> confirmed_block::ReturnData {
    confirmed_block::ReturnData {
        program_id: rd.program_id.to_bytes().to_vec(),
        data: rd.data.clone(),
    }
}
