use anyhow::{Context, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use cid::Cid;
use prost::Message;
use solana_reward_info::RewardType;
use solana_sdk::{
    bs58,
    transaction::{TransactionVersion, VersionedTransaction},
};
use solana_storage_proto::convert::generated;
use solana_transaction_error::TransactionError;
use solana_transaction_status_client_types::{
    EncodedConfirmedBlock, EncodedTransaction, EncodedTransactionWithStatusMeta, Reward,
    TransactionBinaryEncoding, UiCompiledInstruction, UiInnerInstructions, UiInstruction,
    UiLoadedAddresses, UiReturnDataEncoding, UiTransactionError, UiTransactionReturnData,
    UiTransactionStatusMeta, UiTransactionTokenBalance, option_serializer::OptionSerializer,
};
use zstd;

use crate::{Node, block_stream::CarBlock};

impl TryInto<EncodedConfirmedBlock> for CarBlock {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<EncodedConfirmedBlock, Self::Error> {
        // todo compute block hash
        let previous_blockhash = "missing".to_string();
        let blockhash = "missing".to_string();

        let parent_slot = self
            .block
            .meta
            .parent_slot
            .ok_or(anyhow!("missing parent block"))?;

        let transactions_cid: Vec<Cid> = self
            .block
            .entries
            .iter()
            .flat_map(|e| {
                if let Some(Node::Entry(entry)) = self.entries.get(e) {
                    entry.transactions.clone()
                } else {
                    println!("Not an entry");
                    vec![]
                }
            })
            .collect();

        let transactions: Result<Vec<EncodedTransactionWithStatusMeta>, anyhow::Error> =
            transactions_cid
                .iter()
                .map(|cid| match self.entries.get(cid) {
                    Some(crate::Node::Transaction(tx)) => {
                        let meta_byte = self.merge_dataframe(&tx.metadata)?;
                        let tx_bytes = self.merge_dataframe(&tx.data)?;

                        let vt: VersionedTransaction = bincode::deserialize(&tx_bytes)
                            .context("Failed to decode VersionedTransaction")?;

                        let meta = decode_meta(&meta_byte);
                        let tx_bincode = bincode::serialize(&vt)?;
                        let encoded_tx = base64::encode(tx_bincode);

                        Ok(EncodedTransactionWithStatusMeta {
                            transaction: EncodedTransaction::Binary(
                                encoded_tx,
                                TransactionBinaryEncoding::Base64,
                            ),
                            meta,
                            version: Some(TransactionVersion::Number(0)),
                        })
                    }
                    Some(b) => Err(anyhow!("block entry not a transaction ({b:?})")),
                    None => Err(anyhow!("block entry not found")),
                })
                .collect();

        let rewards = match self.block.rewards.and_then(|cid| self.entries.get(&cid)) {
            Some(crate::Node::DataFrame(df)) => {
                let bytes = self.merge_dataframe(df)?;
                bincode::deserialize(&bytes)?
            }
            _ => vec![],
        };

        Ok(EncodedConfirmedBlock {
            previous_blockhash,
            blockhash,
            parent_slot,
            transactions: transactions?,
            rewards,
            num_partitions: None, // todo what is this ?
            block_time: self.block.meta.blocktime,
            block_height: self.block.meta.block_height,
        })
    }
}

/// Main entry: decode and convert protobuf bytes into UiTransactionStatusMeta
pub fn decode_meta(meta_bytes: &[u8]) -> Option<UiTransactionStatusMeta> {
    (|| -> Result<UiTransactionStatusMeta, anyhow::Error> {
        let meta = decode_protobuf_meta(meta_bytes)?;
        convert_generated_meta(meta)
    })()
    .ok()
}

fn decode_protobuf_meta(bytes: &[u8]) -> Result<generated::TransactionStatusMeta, anyhow::Error> {
    let buffer = zstd::decode_all(bytes).context("zstd decode failed")?;
    let meta = generated::TransactionStatusMeta::decode(buffer.as_slice())
        .context("prost decode failed")?;
    Ok(meta)
}

fn convert_generated_meta(
    meta: generated::TransactionStatusMeta,
) -> Result<UiTransactionStatusMeta, anyhow::Error> {
    let err = decode_transaction_error(&meta)?;
    let ui_err = err.clone().map(UiTransactionError::from);
    let ui_status = err
        .clone()
        .map(UiTransactionError::from)
        .map_or(Ok(()), Err);

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
) -> Result<Option<TransactionError>, anyhow::Error> {
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
                                data: bs58::encode(i.data).into_string(),
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
            .map(|v| bs58::encode(v).into_string())
            .collect(),
        readonly: readonly
            .into_iter()
            .map(|v| bs58::encode(v).into_string())
            .collect(),
    })
}

fn convert_return_data(
    data: Option<generated::ReturnData>,
) -> OptionSerializer<UiTransactionReturnData> {
    match data {
        Some(rd) => OptionSerializer::Some(UiTransactionReturnData {
            program_id: bs58::encode(rd.program_id).into_string(),
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
