use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use cid::Cid;
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
use std::time::Instant;
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::{block_stream::CarBlock, Node};

/// Reusable decode buffer to avoid per-call allocations in decode_meta
thread_local! {
    static TL_META_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(32 * 1024));
}

impl TryInto<EncodedConfirmedBlock> for CarBlock {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<EncodedConfirmedBlock, Self::Error> {
        // TODO: compute real hashes if/when available
        let previous_blockhash = "missing".to_string();
        let blockhash = "missing".to_string();

        let parent_slot = self
            .block
            .meta
            .parent_slot
            .ok_or_else(|| anyhow!("missing parent block"))?;

        // --- Stream transactions directly from entries, no intermediate Vec<Cid> ---
        // We may not know exact capacity. If you have a rough upper bound, reserve here.
        let mut transactions: Vec<EncodedTransactionWithStatusMeta> = Vec::new();

        for e_cid in &self.block.entries {
            let Some(Node::Entry(entry)) = self.entries.get(e_cid) else {
                continue; // skip non-entry
            };

            // If you have an average tx per entry, you can reserve here:
            // transactions.reserve(entry.transactions.len());

            for tx_cid in &entry.transactions {
                match self.entries.get(tx_cid) {
                    Some(Node::Transaction(tx)) => {
                        // Merge metadata/data frames once
                        let t_merge0 = Instant::now();
                        let meta_bytes = self.merge_dataframe(&tx.metadata)
                            .context("merge_dataframe(meta) failed")?;
                        let tx_bytes = self.merge_dataframe(&tx.data)
                            .context("merge_dataframe(tx) failed")?;
                        let _t_merge = t_merge0.elapsed();

                        // Decode VersionedTransaction (bincode)
                        let t_tx0 = Instant::now();
                        let vt: VersionedTransaction =
                            bincode::deserialize(&tx_bytes).context("decode VersionedTransaction")?;
                        let _t_tx = t_tx0.elapsed();

                        // Decode UiTransactionStatusMeta (zstd + prost) with buffer reuse
                        let t_meta0 = Instant::now();
                        let meta = decode_meta(&meta_bytes);
                        let _t_meta = t_meta0.elapsed();

                        // Re-encode tx to bincode -> base64 (UI format)
                        let t_ser0 = Instant::now();
                        let tx_bincode =
                            bincode::serialize(&vt).context("re-serialize VersionedTransaction")?;
                        let encoded_tx = BASE64.encode(tx_bincode);
                        let _t_ser = t_ser0.elapsed();

                        transactions.push(EncodedTransactionWithStatusMeta {
                            transaction: EncodedTransaction::Binary(
                                encoded_tx,
                                TransactionBinaryEncoding::Base64,
                            ),
                            meta,
                            // If you have the actual version, set it; 0 keeps your previous behavior.
                            version: Some(TransactionVersion::Number(0)),
                        });
                    }
                    Some(other) => {
                        // Keep the error behavior the same as before, but without noisy prints:
                        return Err(anyhow!("block entry not a transaction ({other:?})"));
                    }
                    None => return Err(anyhow!("block entry not found for tx cid")),
                }
            }
        }

        // Rewards (same logic as before)
        let rewards = match self.block.rewards.and_then(|cid| self.entries.get(&cid)) {
            Some(Node::DataFrame(df)) => {
                let t0 = Instant::now();
                let bytes = self.merge_dataframe(df)?;
                let decoded: Vec<Reward> = bincode::deserialize(&bytes)?;
                let _t = t0.elapsed();
                decoded
            }
            _ => vec![],
        };

        Ok(EncodedConfirmedBlock {
            previous_blockhash,
            blockhash,
            parent_slot,
            transactions,
            rewards,
            num_partitions: None,
            block_time: self.block.meta.blocktime,
            block_height: self.block.meta.block_height,
        })
    }
}

/// Public entry: decode and convert protobuf bytes into UiTransactionStatusMeta
/// Fast path: zero-alloc (per call) via thread-local reused buffer.
pub fn decode_meta(meta_bytes: &[u8]) -> Option<UiTransactionStatusMeta> {
    (|| -> Result<UiTransactionStatusMeta> {
        let meta = decode_protobuf_meta(meta_bytes)?;
        convert_generated_meta(meta)
    })()
    .ok()
}

/// Decode zstd-compressed protobuf into generated::TransactionStatusMeta
/// Uses a thread-local reusable buffer to avoid per-call allocations.
fn decode_protobuf_meta(bytes: &[u8]) -> Result<generated::TransactionStatusMeta> {
    TL_META_BUF.try_with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();

        // Streaming zstd -> Vec (reused)
        let mut decoder = ZstdDecoder::new(Cursor::new(bytes)).context("zstd stream open failed")?;
        decoder.read_to_end(&mut *buf).context("zstd decode failed")?;

        // Prost decode directly from the in-place buffer
        let meta = generated::TransactionStatusMeta::decode(&mut Cursor::new(&*buf))
            .context("prost decode failed")?;
        Ok(meta)
    }).map_err(|_| anyhow!("thread-local access failed"))?
}

fn convert_generated_meta(
    meta: generated::TransactionStatusMeta,
) -> Result<UiTransactionStatusMeta> {
    let err = decode_transaction_error(&meta)?;
    let ui_err = err.clone().map(UiTransactionError::from);
    let ui_status = err.clone().map(UiTransactionError::from).map_or(Ok(()), Err);

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

fn decode_transaction_error(
    meta: &generated::TransactionStatusMeta,
) -> Result<Option<TransactionError>> {
    meta.err
        .as_ref()
        .map(|e| bincode::deserialize::<TransactionError>(&e.err))
        .transpose()
        .context("failed to deserialize TransactionError")
}

fn convert_inner_instructions(
    list: Vec<generated::InnerInstructions>,
    is_none: bool,
) -> OptionSerializer<Vec<UiInnerInstructions>> {
    if is_none {
        OptionSerializer::None
    } else {
        OptionSerializer::Some(
            list.into_iter()
                .map(|ix| UiInnerInstructions {
                    index: ix.index as u8,
                    instructions: ix
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
                        .collect(),
                })
                .collect(),
        )
    }
}

fn convert_log_messages(msgs: Vec<String>, is_none: bool) -> OptionSerializer<Vec<String>> {
    if is_none {
        OptionSerializer::None
    } else {
        OptionSerializer::Some(msgs)
    }
}

fn convert_token_balances(
    balances: Vec<generated::TokenBalance>,
) -> OptionSerializer<Vec<UiTransactionTokenBalance>> {
    if balances.is_empty() {
        OptionSerializer::None
    } else {
        OptionSerializer::Some(
            balances
                .into_iter()
                .map(|t| UiTransactionTokenBalance {
                    account_index: t.account_index as u8,
                    mint: t.mint,
                    ui_token_amount: t
                        .ui_token_amount
                        .map(
                            |amount| solana_account_decoder_client_types::token::UiTokenAmount {
                                ui_amount: Some(amount.ui_amount),
                                decimals: amount.decimals as u8,
                                amount: amount.amount,
                                ui_amount_string: amount.ui_amount_string,
                            },
                        )
                        .unwrap(),
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
                })
                .collect(),
        )
    }
}

fn convert_rewards(list: Vec<generated::Reward>) -> OptionSerializer<Vec<Reward>> {
    if list.is_empty() {
        OptionSerializer::None
    } else {
        OptionSerializer::Some(
            list.into_iter()
                .map(|r| Reward {
                    pubkey: r.pubkey,
                    lamports: r.lamports,
                    post_balance: r.post_balance,
                    reward_type: match r.reward_type {
                        1 => Some(RewardType::Fee),
                        2 => Some(RewardType::Rent),
                        3 => Some(RewardType::Staking),
                        4 => Some(RewardType::Voting),
                        0 => None,
                        _ => None,
                    },
                    commission: r.commission.parse::<u8>().ok(),
                })
                .collect(),
        )
    }
}

fn convert_loaded_addresses(
    writable: Vec<Vec<u8>>,
    readonly: Vec<Vec<u8>>,
) -> OptionSerializer<UiLoadedAddresses> {
    OptionSerializer::Some(UiLoadedAddresses {
        writable: writable
            .into_iter()
            .map(|v| solana_sdk::bs58::encode(v).into_string())
            .collect(),
        readonly: readonly
            .into_iter()
            .map(|v| solana_sdk::bs58::encode(v).into_string())
            .collect(),
    })
}

fn convert_return_data(
    data: Option<generated::ReturnData>,
) -> OptionSerializer<UiTransactionReturnData> {
    match data {
        Some(rd) => OptionSerializer::Some(UiTransactionReturnData {
            program_id: solana_sdk::bs58::encode(rd.program_id).into_string(),
            data: (BASE64.encode(rd.data), UiReturnDataEncoding::Base64),
        }),
        None => OptionSerializer::None,
    }
}

fn convert_option_u64(value: Option<u64>) -> OptionSerializer<u64> {
    value
        .map(OptionSerializer::Some)
        .unwrap_or(OptionSerializer::Skip)
}
