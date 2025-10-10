use anyhow::{Context, Result, anyhow};
use bincode::serde::Compat;
use blockzilla::{
    block_stream::CarBlock,
    confirmed_block,
    node::{CborCid, Node},
};
use prost::Message;
use rayon::prelude::*;
use solana_sdk::{
    message::{MessageHeader, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use solana_transaction_error::TransactionError;
use std::{cell::RefCell, io::Read};
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::optimizer::{
    BlockWithIds, CompactAddressTableLookup, CompactInnerInstructions, CompactInstruction,
    CompactLoadedAddresses, CompactReward, CompactTokenBalance, CompactTransaction,
    CompactTransactionMeta, KeyRegistry,
};

thread_local! {
    static TL_META_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(128 * 1024));
}

/// Convert directly to BlockWithIds with parallel transaction processing
pub fn cb_to_compact_block(cb: CarBlock, reg: &mut KeyRegistry) -> Result<BlockWithIds> {
    let slot = cb
        .block
        .meta
        .parent_slot
        .ok_or_else(|| anyhow!("missing parent slot"))?
        + 1;
    let parent_slot = cb.block.meta.parent_slot.unwrap();
    let block_time = cb.block.meta.blocktime;
    let block_height = cb.block.meta.block_height;

    let blockhash = "missing".to_string();
    let previous_blockhash = "missing".to_string();

    // Process rewards (small serial step)
    let rewards = extract_rewards(&cb, reg)?;

    // Collect all transaction data first (serial but I/O bound)
    let mut tx_data = Vec::new();
    tx_data.reserve(
        cb.block
            .entries
            .iter()
            .filter_map(|cid| match cb.entries.get(&cid.0) {
                Some(Node::Entry(e)) => Some(e.transactions.len()),
                _ => None,
            })
            .sum(),
    );

    for e_cid in &cb.entries {
        let Some(Node::Entry(entry)) = cb.entries.get(&e_cid.0) else {
            continue;
        };
        for tx_cid in &entry.transactions {
            match cb.entries.get(&tx_cid.0) {
                Some(Node::Transaction(tx)) => {
                    let meta_bytes = cb
                        .merge_dataframe(&tx.metadata)
                        .context("merge_dataframe(meta) failed")?;
                    let tx_bytes = cb
                        .merge_dataframe(&tx.data)
                        .context("merge_dataframe(tx) failed")?;
                    tx_data.push((meta_bytes, tx_bytes));
                }
                Some(other) => return Err(anyhow!("block entry not a transaction ({other:?})")),
                None => return Err(anyhow!("block entry not found for tx cid")),
            }
        }
    }

    // Parallel decode + compact conversion
    let partial_transactions: Result<Vec<_>> = tx_data
        .into_par_iter()
        .map(|(meta_bytes, tx_bytes)| {
            let (Compat(vt), _): (Compat<VersionedTransaction>, _) =
                bincode::decode_from_slice(&tx_bytes, bincode::config::legacy())
                    .context("decode VersionedTransaction")?;
            let meta_proto = decode_protobuf_meta(&meta_bytes)?;
            convert_to_partial_transaction(vt, meta_proto)
        })
        .collect();

    let partial_transactions = partial_transactions?;

    // Final serial key registration
    let transactions = partial_transactions
        .into_iter()
        .map(|partial| finalize_transaction(partial, reg))
        .collect::<Result<Vec<_>>>()?;

    Ok(BlockWithIds {
        slot,
        blockhash,
        previous_blockhash,
        parent_slot,
        block_time,
        block_height,
        rewards,
        num_transactions: transactions.len() as u64,
        transactions,
    })
}

fn extract_rewards(cb: &CarBlock, reg: &mut KeyRegistry) -> Result<Vec<CompactReward>> {
    let rewards_data = match cb
        .block
        .rewards
        .clone()
        .and_then(|cid| cb.entries.get(&cid.0))
    {
        Some(Node::DataFrame(df)) => {
            let bytes = cb.merge_dataframe(df)?;

            bincode::decode_from_slice::<
                Vec<Compat<solana_transaction_status_client_types::Reward>>,
                _,
            >(&bytes, bincode::config::legacy())?
            .0
        }
        _ => return Ok(Vec::new()),
    };

    let mut rewards = Vec::with_capacity(rewards_data.len());
    for Compat(rw) in rewards_data {
        if let Ok(pk) = rw.pubkey.parse::<Pubkey>() {
            rewards.push(CompactReward {
                pubkey: reg.get_or_insert(&pk),
                lamports: rw.lamports,
                post_balance: rw.post_balance,
                reward_type: rw.reward_type.as_ref().map(|rt| format!("{:?}", rt)),
                commission: rw.commission,
            });
        }
    }
    Ok(rewards)
}

fn decode_protobuf_meta(bytes: &[u8]) -> Result<confirmed_block::TransactionStatusMeta> {
    TL_META_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();

        // 1st try: stream decode into reusable buffer
        if let Ok(mut dec) = ZstdDecoder::new(bytes) {
            if dec.read_to_end(&mut *buf).is_ok() {
                return confirmed_block::TransactionStatusMeta::decode(&buf[..])
                    .context("prost decode failed after streaming zstd");
            }
        }

        // 2nd: bulk decompress
        match zstd::bulk::decompress(bytes, 512 * 1024) {
            Ok(decompressed) => confirmed_block::TransactionStatusMeta::decode(&decompressed[..])
                .context("prost decode failed after bulk zstd"),
            Err(_) => confirmed_block::TransactionStatusMeta::decode(bytes)
                .context("all decode methods failed"),
        }
    })
}

// Intermediate “partial” structs
struct PartialTransaction {
    signatures: Vec<Signature>,
    message_header: MessageHeader,
    account_keys: Vec<Pubkey>,
    recent_blockhash: String,
    instructions: Vec<PartialInstruction>,
    address_table_lookups: Option<Vec<PartialAddressTableLookup>>,
    meta: Option<PartialTransactionMeta>,
    version: u8,
}
struct PartialInstruction {
    program_id: Pubkey,
    accounts: Vec<u8>,
    data: Vec<u8>,
}
struct PartialAddressTableLookup {
    account_key: Pubkey,
    writable_indexes: Vec<u8>,
    readonly_indexes: Vec<u8>,
}
struct PartialTransactionMeta {
    err: Option<String>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<PartialInnerInstructions>>,
    log_messages: Option<Vec<String>>,
    pre_token_balances: Option<Vec<PartialTokenBalance>>,
    post_token_balances: Option<Vec<PartialTokenBalance>>,
    loaded_addresses: Option<PartialLoadedAddresses>,
    return_data: Option<(Pubkey, Vec<u8>)>,
    compute_units_consumed: Option<u64>,
}
struct PartialInnerInstructions {
    index: u8,
    instructions: Vec<PartialInstruction>,
}
struct PartialTokenBalance {
    account_index: u8,
    mint: Pubkey,
    ui_token_amount: String,
    owner: Pubkey,
    program_id: Pubkey,
}
struct PartialLoadedAddresses {
    writable: Vec<Pubkey>,
    readonly: Vec<Pubkey>,
}

fn convert_to_partial_transaction(
    vt: VersionedTransaction,
    meta_proto: confirmed_block::TransactionStatusMeta,
) -> Result<PartialTransaction> {
    let msg = vt.message;
    let signatures = vt.signatures;
    let static_keys = msg.static_account_keys();
    let account_keys = static_keys.to_vec();
    let message_header = *msg.header();
    let recent_blockhash = msg.recent_blockhash().to_string();

    let mut instructions = Vec::with_capacity(msg.instructions().len());
    for ix in msg.instructions() {
        instructions.push(PartialInstruction {
            program_id: static_keys[ix.program_id_index as usize],
            accounts: ix.accounts.clone(),
            data: ix.data.clone(),
        });
    }

    let address_table_lookups = msg.address_table_lookups().map(|lookups| {
        let mut out = Vec::with_capacity(lookups.len());
        for l in lookups {
            out.push(PartialAddressTableLookup {
                account_key: l.account_key,
                writable_indexes: l.writable_indexes.clone(),
                readonly_indexes: l.readonly_indexes.clone(),
            });
        }
        out
    });

    let version = match msg {
        VersionedMessage::Legacy(_) => 0,
        VersionedMessage::V0(_) => 1,
    };

    let mut all_keys = static_keys.to_vec();
    all_keys.reserve(
        meta_proto.loaded_writable_addresses.len() + meta_proto.loaded_readonly_addresses.len(),
    );

    for addr_bytes in meta_proto
        .loaded_writable_addresses
        .iter()
        .chain(meta_proto.loaded_readonly_addresses.iter())
    {
        if addr_bytes.len() == 32 {
            if let Ok(arr) = <[u8; 32]>::try_from(addr_bytes.as_slice()) {
                all_keys.push(Pubkey::new_from_array(arr));
            }
        }
    }

    let meta = convert_to_partial_meta(meta_proto, &all_keys)?;

    Ok(PartialTransaction {
        signatures,
        message_header,
        account_keys,
        recent_blockhash,
        instructions,
        address_table_lookups,
        meta: Some(meta),
        version,
    })
}

fn convert_to_partial_meta(
    meta: confirmed_block::TransactionStatusMeta,
    all_keys: &[Pubkey],
) -> Result<PartialTransactionMeta> {
    let err = meta
        .err
        .as_ref()
        .map(|e| {
            bincode::decode_from_slice::<Compat<TransactionError>, _>(
                &e.err,
                bincode::config::legacy(),
            )
            .map(|a| a.0.0)
        })
        .transpose()
        .context("failed to deserialize TransactionError")?
        .map(|e| format!("{:?}", e));

    let inner_instructions = if meta.inner_instructions_none || meta.inner_instructions.is_empty() {
        None
    } else {
        let mut result = Vec::with_capacity(meta.inner_instructions.len());
        for ui_inner in meta.inner_instructions {
            if ui_inner.instructions.is_empty() {
                continue;
            }
            let mut inst = Vec::with_capacity(ui_inner.instructions.len());
            for i in ui_inner.instructions {
                if let Some(&pid) = all_keys.get(i.program_id_index as usize) {
                    inst.push(PartialInstruction {
                        program_id: pid,
                        accounts: i.accounts,
                        data: i.data,
                    });
                }
            }
            if !inst.is_empty() {
                result.push(PartialInnerInstructions {
                    index: ui_inner.index as u8,
                    instructions: inst,
                });
            }
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    };

    let log_messages = if meta.log_messages_none || meta.log_messages.is_empty() {
        None
    } else {
        Some(meta.log_messages)
    };

    let pre_token_balances = if meta.pre_token_balances.is_empty() {
        None
    } else {
        let mut balances = Vec::with_capacity(meta.pre_token_balances.len());
        for tb in meta.pre_token_balances {
            balances.push(convert_to_partial_token_balance(tb)?);
        }
        Some(balances)
    };

    let post_token_balances = if meta.post_token_balances.is_empty() {
        None
    } else {
        let mut balances = Vec::with_capacity(meta.post_token_balances.len());
        for tb in meta.post_token_balances {
            balances.push(convert_to_partial_token_balance(tb)?);
        }
        Some(balances)
    };

    let loaded_addresses = {
        let w_len = meta.loaded_writable_addresses.len();
        let r_len = meta.loaded_readonly_addresses.len();
        if w_len == 0 && r_len == 0 {
            None
        } else {
            let mut writable = Vec::with_capacity(w_len);
            for bytes in meta.loaded_writable_addresses {
                if bytes.len() == 32 {
                    if let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice()) {
                        writable.push(Pubkey::new_from_array(arr));
                    }
                }
            }
            let mut readonly = Vec::with_capacity(r_len);
            for bytes in meta.loaded_readonly_addresses {
                if bytes.len() == 32 {
                    if let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice()) {
                        readonly.push(Pubkey::new_from_array(arr));
                    }
                }
            }
            Some(PartialLoadedAddresses { writable, readonly })
        }
    };

    let return_data = meta.return_data.and_then(|rd| {
        if rd.program_id.len() == 32 {
            <[u8; 32]>::try_from(rd.program_id.as_slice())
                .ok()
                .map(|arr| (Pubkey::new_from_array(arr), rd.data))
        } else {
            None
        }
    });

    Ok(PartialTransactionMeta {
        err,
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions,
        log_messages,
        pre_token_balances,
        post_token_balances,
        loaded_addresses,
        return_data,
        compute_units_consumed: meta.compute_units_consumed,
    })
}

fn convert_to_partial_token_balance(
    tb: confirmed_block::TokenBalance,
) -> Result<PartialTokenBalance> {
    let mint = tb.mint.parse::<Pubkey>().context("Failed to parse mint")?;
    let owner = if tb.owner.is_empty() {
        Pubkey::default()
    } else {
        tb.owner.parse::<Pubkey>().unwrap_or_default()
    };
    let program_id = if tb.program_id.is_empty() {
        Pubkey::default()
    } else {
        tb.program_id.parse::<Pubkey>().unwrap_or_default()
    };

    let ui_token_amount = tb
        .ui_token_amount
        .map(|a| {
            serde_json::json!({
                "amount": a.amount,
                "decimals": a.decimals,
                "uiAmount": a.ui_amount,
                "uiAmountString": a.ui_amount_string,
            })
            .to_string()
        })
        .unwrap_or_default();

    Ok(PartialTokenBalance {
        account_index: tb.account_index as u8,
        mint,
        ui_token_amount,
        owner,
        program_id,
    })
}

fn finalize_transaction(
    partial: PartialTransaction,
    reg: &mut KeyRegistry,
) -> Result<CompactTransaction> {
    let account_keys: Vec<u32> = partial
        .account_keys
        .iter()
        .map(|pk| reg.get_or_insert(pk))
        .collect();

    let instructions: Vec<CompactInstruction> = partial
        .instructions
        .into_iter()
        .map(|ix| CompactInstruction {
            program_id: reg.get_or_insert(&ix.program_id),
            accounts: ix.accounts,
            data: ix.data,
            stack_height: None,
        })
        .collect();

    let address_table_lookups = partial.address_table_lookups.map(|lookups| {
        lookups
            .into_iter()
            .map(|lookup| CompactAddressTableLookup {
                account_key: reg.get_or_insert(&lookup.account_key),
                writable_indexes: lookup.writable_indexes,
                readonly_indexes: lookup.readonly_indexes,
            })
            .collect()
    });

    let meta = partial.meta.map(|m| finalize_meta(m, reg)).transpose()?;

    Ok(CompactTransaction {
        signatures: partial.signatures,
        message_header: partial.message_header,
        account_keys,
        recent_blockhash: partial.recent_blockhash,
        instructions,
        address_table_lookups,
        meta,
        version: partial.version,
    })
}

fn finalize_meta(
    partial: PartialTransactionMeta,
    reg: &mut KeyRegistry,
) -> Result<CompactTransactionMeta> {
    let inner_instructions = partial.inner_instructions.map(|inners| {
        inners
            .into_iter()
            .map(|inner| CompactInnerInstructions {
                index: inner.index,
                instructions: inner
                    .instructions
                    .into_iter()
                    .map(|ix| CompactInstruction {
                        program_id: reg.get_or_insert(&ix.program_id),
                        accounts: ix.accounts,
                        data: ix.data,
                        stack_height: None,
                    })
                    .collect(),
            })
            .collect()
    });

    let pre_token_balances = partial.pre_token_balances.map(|balances| {
        balances
            .into_iter()
            .map(|tb| CompactTokenBalance {
                account_index: tb.account_index,
                mint: reg.get_or_insert(&tb.mint),
                ui_token_amount: tb.ui_token_amount,
                owner: if tb.owner == Pubkey::default() {
                    0
                } else {
                    reg.get_or_insert(&tb.owner)
                },
                program_id: if tb.program_id == Pubkey::default() {
                    0
                } else {
                    reg.get_or_insert(&tb.program_id)
                },
            })
            .collect()
    });

    let post_token_balances = partial.post_token_balances.map(|balances| {
        balances
            .into_iter()
            .map(|tb| CompactTokenBalance {
                account_index: tb.account_index,
                mint: reg.get_or_insert(&tb.mint),
                ui_token_amount: tb.ui_token_amount,
                owner: if tb.owner == Pubkey::default() {
                    0
                } else {
                    reg.get_or_insert(&tb.owner)
                },
                program_id: if tb.program_id == Pubkey::default() {
                    0
                } else {
                    reg.get_or_insert(&tb.program_id)
                },
            })
            .collect()
    });

    let loaded_addresses = partial
        .loaded_addresses
        .map(|addrs| CompactLoadedAddresses {
            writable: addrs
                .writable
                .iter()
                .map(|pk| reg.get_or_insert(pk))
                .collect(),
            readonly: addrs
                .readonly
                .iter()
                .map(|pk| reg.get_or_insert(pk))
                .collect(),
        });

    let return_data = partial
        .return_data
        .map(|(pk, data)| (reg.get_or_insert(&pk), data));

    Ok(CompactTransactionMeta {
        err: partial.err,
        fee: partial.fee,
        pre_balances: partial.pre_balances,
        post_balances: partial.post_balances,
        inner_instructions,
        log_messages: partial.log_messages,
        pre_token_balances,
        post_token_balances,
        rewards: None,
        loaded_addresses,
        return_data,
        compute_units_consumed: partial.compute_units_consumed,
    })
}
