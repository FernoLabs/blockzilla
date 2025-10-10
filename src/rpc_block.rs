use anyhow::{Context, Result, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use bincode::serde::Compat;
use prost::Message;
use solana_reward_info::RewardType;
use solana_sdk::transaction::{TransactionVersion, VersionedTransaction};
use solana_storage_proto::convert::generated;
use solana_transaction_error::TransactionError;
use solana_transaction_status_client_types::{
    EncodedConfirmedBlock, EncodedTransaction, EncodedTransactionWithStatusMeta, Reward,
    TransactionBinaryEncoding, UiCompiledInstruction, UiInnerInstructions, UiInstruction,
    UiLoadedAddresses, UiReturnDataEncoding, UiTransactionError, UiTransactionReturnData,
    UiTransactionStatusMeta, UiTransactionTokenBalance, option_serializer::OptionSerializer,
};
use std::cell::RefCell;
use std::io::{Cursor, Read};
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::{block_stream::CarBlock, node::Node};

thread_local! {
    static TL_META_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(64 * 1024));
    static TL_BINCODE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(16 * 1024));
    static TL_BASE64_BUF: RefCell<String> = RefCell::new(String::with_capacity(24 * 1024));
}

impl TryInto<EncodedConfirmedBlock> for CarBlock {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<EncodedConfirmedBlock, Self::Error> {
        // Constant strings to avoid allocation
        const MISSING_HASH: &str = "missing";

        let parent_slot = self
            .block
            .meta
            .parent_slot
            .ok_or_else(|| anyhow!("missing parent block"))?;

        // Pre-allocate with estimated capacity
        let estimated_tx_count = self.block.entries.len() * 4; // Rough estimate
        let mut transactions: Vec<EncodedTransactionWithStatusMeta> =
            Vec::with_capacity(estimated_tx_count);

        for e_cid in &self.block.entries {
            let Some(Node::Entry(entry)) = self.entries.get(e_cid) else {
                continue;
            };

            for tx_cid in &entry.transactions {
                match self.entries.get(tx_cid) {
                    Some(Node::Transaction(tx)) => {
                        // Merge dataframes
                        let meta_bytes = self
                            .merge_dataframe(&tx.metadata)
                            .context("merge_dataframe(meta) failed")?;
                        let tx_bytes = self
                            .merge_dataframe(&tx.data)
                            .context("merge_dataframe(tx) failed")?;

                        // Decode VersionedTransaction (bincode)
                        let (bincode::serde::Compat(vt), _len): (
                            bincode::serde::Compat<VersionedTransaction>,
                            usize,
                        ) = bincode::decode_from_slice(&tx_bytes, bincode::config::legacy())
                            .map_err(|err| anyhow!("cant decode {err}"))?;

                        // Decode metadata (zstd + prost) with buffer reuse
                        let meta = decode_meta(&meta_bytes);

                        // Re-encode transaction with buffer reuse
                        let encoded_tx = encode_transaction_base64(&vt)?;

                        transactions.push(EncodedTransactionWithStatusMeta {
                            transaction: EncodedTransaction::Binary(
                                encoded_tx,
                                TransactionBinaryEncoding::Base64,
                            ),
                            meta,
                            version: Some(TransactionVersion::Number(0)),
                        });
                    }
                    Some(other) => {
                        return Err(anyhow!("block entry not a transaction ({other:?})"));
                    }
                    None => return Err(anyhow!("block entry not found for tx cid")),
                }
            }
        }

        // Decode rewards efficiently
        let rewards = match self.block.rewards.and_then(|cid| self.entries.get(&cid)) {
            Some(Node::DataFrame(df)) => {
                let bytes = self.merge_dataframe(df)?;
                let res: Vec<Compat<Reward>> =
                    bincode::decode_from_slice(&bytes, bincode::config::legacy())?.0;
                res.into_iter().map(|a| a.0).collect()
            }
            _ => Vec::new(),
        };

        Ok(EncodedConfirmedBlock {
            previous_blockhash: MISSING_HASH.to_string(),
            blockhash: MISSING_HASH.to_string(),
            parent_slot,
            transactions,
            rewards,
            num_partitions: None,
            block_time: self.block.meta.blocktime,
            block_height: self.block.meta.block_height,
        })
    }
}

/// Encode VersionedTransaction to base64 using thread-local buffer reuse
#[inline]
fn encode_transaction_base64(vt: &VersionedTransaction) -> Result<String> {
    TL_BINCODE_BUF.with(|bincode_cell| {
        TL_BASE64_BUF.with(|base64_cell| {
            let mut bincode_buf = bincode_cell.borrow_mut();
            let mut base64_buf = base64_cell.borrow_mut();

            bincode_buf.clear();
            bincode::encode_into_slice(Compat(vt), &mut *bincode_buf, bincode::config::legacy())
                .context("re-serialize VersionedTransaction")?;

            base64_buf.clear();
            BASE64.encode_string(&*bincode_buf, &mut base64_buf);

            Ok(base64_buf.clone())
        })
    })
}

/// Decode and convert protobuf bytes into UiTransactionStatusMeta
#[inline]
pub fn decode_meta(meta_bytes: &[u8]) -> Option<UiTransactionStatusMeta> {
    decode_protobuf_meta(meta_bytes)
        .and_then(convert_generated_meta)
        .ok()
}

/// Decode zstd-compressed protobuf using thread-local reusable buffer
fn decode_protobuf_meta(bytes: &[u8]) -> Result<generated::TransactionStatusMeta> {
    TL_META_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();

        // Streaming zstd decode
        let mut decoder =
            ZstdDecoder::new(Cursor::new(bytes)).context("zstd stream open failed")?;
        decoder
            .read_to_end(&mut buf)
            .context("zstd decode failed")?;

        // Prost decode from reused buffer
        generated::TransactionStatusMeta::decode(&mut Cursor::new(&*buf))
            .context("prost decode failed")
    })
}

#[inline]
fn convert_generated_meta(
    meta: generated::TransactionStatusMeta,
) -> Result<UiTransactionStatusMeta> {
    let err = decode_transaction_error(&meta)?;
    let ui_err = err.as_ref().map(|e| UiTransactionError::from(e.clone()));
    let ui_status = err
        .map(|e| Err(UiTransactionError::from(e)))
        .unwrap_or(Ok(()));

    Ok(UiTransactionStatusMeta {
        err: ui_err,
        status: ui_status,
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions: convert_inner_instructions(
            meta.inner_instructions,
            meta.inner_instructions_none,
        ),
        log_messages: convert_log_messages(meta.log_messages, meta.log_messages_none),
        pre_token_balances: convert_token_balances(meta.pre_token_balances),
        post_token_balances: convert_token_balances(meta.post_token_balances),
        rewards: convert_rewards(meta.rewards),
        loaded_addresses: convert_loaded_addresses(
            meta.loaded_writable_addresses,
            meta.loaded_readonly_addresses,
        ),
        return_data: convert_return_data(meta.return_data),
        compute_units_consumed: convert_option_u64(meta.compute_units_consumed),
        cost_units: convert_option_u64(meta.cost_units),
    })
}

#[inline]
fn decode_transaction_error(
    meta: &generated::TransactionStatusMeta,
) -> Result<Option<TransactionError>> {
    meta.err
        .as_ref()
        .map(|e| {
            bincode::decode_from_slice::<bincode::serde::Compat<TransactionError>, _>(
                &e.err,
                bincode::config::legacy(),
            )
            .map(|(v, _)| v.0)
        })
        .transpose()
        .context("failed to deserialize TransactionError")
}

#[inline]
fn convert_inner_instructions(
    list: Vec<generated::InnerInstructions>,
    is_none: bool,
) -> OptionSerializer<Vec<UiInnerInstructions>> {
    if is_none {
        return OptionSerializer::None;
    }

    let converted: Vec<UiInnerInstructions> = list
        .into_iter()
        .map(|ix| {
            let instructions: Vec<UiInstruction> = ix
                .instructions
                .into_iter()
                .map(|i| {
                    UiInstruction::Compiled(UiCompiledInstruction {
                        program_id_index: i.program_id_index as u8,
                        accounts: i.accounts,
                        data: solana_sdk::bs58::encode(i.data).into_string(),
                        stack_height: i.stack_height,
                    })
                })
                .collect();

            UiInnerInstructions {
                index: ix.index as u8,
                instructions,
            }
        })
        .collect();

    OptionSerializer::Some(converted)
}

#[inline]
fn convert_log_messages(msgs: Vec<String>, is_none: bool) -> OptionSerializer<Vec<String>> {
    if is_none {
        OptionSerializer::None
    } else {
        OptionSerializer::Some(msgs)
    }
}

#[inline]
fn convert_token_balances(
    balances: Vec<generated::TokenBalance>,
) -> OptionSerializer<Vec<UiTransactionTokenBalance>> {
    if balances.is_empty() {
        return OptionSerializer::None;
    }

    let converted: Vec<UiTransactionTokenBalance> = balances
        .into_iter()
        .map(|t| {
            let ui_token_amount = t
                .ui_token_amount
                .map(
                    |amount| solana_account_decoder_client_types::token::UiTokenAmount {
                        ui_amount: Some(amount.ui_amount),
                        decimals: amount.decimals as u8,
                        amount: amount.amount,
                        ui_amount_string: amount.ui_amount_string,
                    },
                )
                .unwrap();

            UiTransactionTokenBalance {
                account_index: t.account_index as u8,
                mint: t.mint,
                ui_token_amount,
                owner: if t.owner.is_empty() {
                    OptionSerializer::Skip
                } else {
                    OptionSerializer::Some(t.owner)
                },
                program_id: if t.program_id.is_empty() {
                    OptionSerializer::Skip
                } else {
                    OptionSerializer::Some(t.program_id)
                },
            }
        })
        .collect();

    OptionSerializer::Some(converted)
}

#[inline]
fn convert_rewards(list: Vec<generated::Reward>) -> OptionSerializer<Vec<Reward>> {
    if list.is_empty() {
        return OptionSerializer::None;
    }

    let converted: Vec<Reward> = list
        .into_iter()
        .map(|r| Reward {
            pubkey: r.pubkey,
            lamports: r.lamports,
            post_balance: r.post_balance,
            reward_type: match r.reward_type {
                1 => Some(RewardType::Fee),
                2 => Some(RewardType::Rent),
                3 => Some(RewardType::Staking),
                4 => Some(RewardType::Voting),
                _ => None,
            },
            commission: r.commission.parse::<u8>().ok(),
        })
        .collect();

    OptionSerializer::Some(converted)
}

#[inline]
fn convert_loaded_addresses(
    writable: Vec<Vec<u8>>,
    readonly: Vec<Vec<u8>>,
) -> OptionSerializer<UiLoadedAddresses> {
    let writable_strings: Vec<String> = writable
        .into_iter()
        .map(|v| solana_sdk::bs58::encode(v).into_string())
        .collect();

    let readonly_strings: Vec<String> = readonly
        .into_iter()
        .map(|v| solana_sdk::bs58::encode(v).into_string())
        .collect();

    OptionSerializer::Some(UiLoadedAddresses {
        writable: writable_strings,
        readonly: readonly_strings,
    })
}

#[inline]
fn convert_return_data(
    data: Option<generated::ReturnData>,
) -> OptionSerializer<UiTransactionReturnData> {
    match data {
        Some(rd) => {
            let program_id = solana_sdk::bs58::encode(rd.program_id).into_string();
            let encoded_data = BASE64.encode(rd.data);

            OptionSerializer::Some(UiTransactionReturnData {
                program_id,
                data: (encoded_data, UiReturnDataEncoding::Base64),
            })
        }
        None => OptionSerializer::None,
    }
}

#[inline]
fn convert_option_u64(value: Option<u64>) -> OptionSerializer<u64> {
    value
        .map(OptionSerializer::Some)
        .unwrap_or(OptionSerializer::Skip)
}
