use anyhow::{Context, Result, anyhow};
use blockzilla::Node;
use blockzilla::block_stream::CarBlock;
use prost::Message;
use solana_sdk::{
    message::VersionedMessage, pubkey::Pubkey,
    transaction::VersionedTransaction,
};
use solana_storage_proto::convert::generated;
use solana_transaction_error::TransactionError;
use std::cell::RefCell;
use std::io::Read;
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::optimizer::{
    BlockWithIds, CompactAddressTableLookup, CompactInnerInstructions, CompactInstruction,
    CompactLoadedAddresses, CompactReward, CompactTokenBalance, CompactTransaction,
    CompactTransactionMeta, KeyRegistry,
};

thread_local! {
    static TL_META_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(128 * 1024));
}

/// Convert directly to BlockWithIds, bypassing EncodedConfirmedBlock
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

    // Use placeholder hashes
    let blockhash = "missing".to_string();
    let previous_blockhash = "missing".to_string();

    // Pre-allocate vectors
    let estimated_tx_count = cb.block.entries.len() * 4;
    let mut transactions = Vec::with_capacity(estimated_tx_count);

    // Process block rewards first
    let rewards = extract_rewards(&cb, reg)?;

    // Process transactions directly
    for e_cid in &cb.block.entries {
        let Some(Node::Entry(entry)) = cb.entries.get(e_cid) else {
            continue;
        };

        for tx_cid in &entry.transactions {
            match cb.entries.get(tx_cid) {
                Some(Node::Transaction(tx)) => {
                    // Merge dataframes
                    let meta_bytes = cb
                        .merge_dataframe(&tx.metadata)
                        .context("merge_dataframe(meta) failed")?;
                    let tx_bytes = cb
                        .merge_dataframe(&tx.data)
                        .context("merge_dataframe(tx) failed")?;

                    // Decode VersionedTransaction
                    let vt: VersionedTransaction =
                        bincode::deserialize(&tx_bytes).context("decode VersionedTransaction")?;

                    // Decode metadata
                    let meta_proto = decode_protobuf_meta(&meta_bytes)?;

                    // Convert directly to CompactTransaction
                    let compact_tx = convert_to_compact_transaction(vt, meta_proto, reg)?;
                    transactions.push(compact_tx);
                }
                Some(other) => {
                    return Err(anyhow!("block entry not a transaction ({other:?})"));
                }
                None => return Err(anyhow!("block entry not found for tx cid")),
            }
        }
    }

    let num_transactions = transactions.len() as u64;

    Ok(BlockWithIds {
        slot,
        blockhash,
        previous_blockhash,
        parent_slot,
        block_time,
        block_height,
        rewards,
        transactions,
        num_transactions,
    })
}

fn extract_rewards(cb: &CarBlock, reg: &mut KeyRegistry) -> Result<Vec<CompactReward>> {
    let rewards_data = match cb.block.rewards.and_then(|cid| cb.entries.get(&cid)) {
        Some(Node::DataFrame(df)) => {
            let bytes = cb.merge_dataframe(df)?;
            bincode::deserialize::<Vec<solana_transaction_status_client_types::Reward>>(&bytes)?
        }
        _ => return Ok(Vec::new()),
    };

    let mut rewards = Vec::with_capacity(rewards_data.len());
    for rw in rewards_data {
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

/// Decode zstd-compressed protobuf metadata with robust error handling
fn decode_protobuf_meta(bytes: &[u8]) -> Result<generated::TransactionStatusMeta> {
    // Fast path: try bulk decompression (most efficient)
    if let Ok(decompressed) = zstd::bulk::decompress(bytes, 512 * 1024) {
        if let Ok(meta) = generated::TransactionStatusMeta::decode(&decompressed[..]) {
            return Ok(meta);
        }
    }

    // Fallback: streaming decompression with thread-local buffer
    TL_META_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();

        // Try streaming zstd decode
        match ZstdDecoder::new(bytes) {
            Ok(mut decoder) => {
                match decoder.read_to_end(&mut *buf) {
                    Ok(_) => {
                        // Successfully decompressed, now decode protobuf
                        generated::TransactionStatusMeta::decode(&buf[..])
                            .context("prost decode failed after streaming zstd")
                    }
                    Err(e) => {
                        // Zstd decode failed - maybe it's uncompressed?
                        if bytes.len() < 1024 * 1024 {
                            generated::TransactionStatusMeta::decode(bytes)
                                .context("prost decode failed (tried as uncompressed)")
                        } else {
                            Err(anyhow!("zstd streaming decode failed: {}", e))
                        }
                    }
                }
            }
            Err(e) => {
                // Can't create decoder - try raw protobuf
                generated::TransactionStatusMeta::decode(bytes)
                    .with_context(|| format!("zstd decoder creation failed: {}, tried raw decode", e))
            }
        }
    })
}

/// Convert VersionedTransaction + metadata directly to CompactTransaction
fn convert_to_compact_transaction(
    vt: VersionedTransaction,
    meta_proto: generated::TransactionStatusMeta,
    reg: &mut KeyRegistry,
) -> Result<CompactTransaction> {
    let msg = vt.message;
    let signatures = vt.signatures;

    // Map static account keys
    let static_keys = msg.static_account_keys();
    let mut account_keys = Vec::with_capacity(static_keys.len());
    for key in static_keys.iter() {
        account_keys.push(reg.get_or_insert(key));
    }

    let message_header = *msg.header();
    let recent_blockhash = msg.recent_blockhash().to_string();

    // Convert instructions
    let msg_instructions = msg.instructions();
    let mut instructions = Vec::with_capacity(msg_instructions.len());
    for ix in msg_instructions {
        let program_id = reg.get_or_insert(&static_keys[ix.program_id_index as usize]);
        instructions.push(CompactInstruction {
            program_id,
            accounts: ix.accounts.clone(),
            data: ix.data.clone(),
            stack_height: None,
        });
    }

    // Convert address table lookups
    let address_table_lookups = msg.address_table_lookups().map(|lookups| {
        let mut compact_lookups = Vec::with_capacity(lookups.len());
        for lookup in lookups {
            compact_lookups.push(CompactAddressTableLookup {
                account_key: reg.get_or_insert(&lookup.account_key),
                writable_indexes: lookup.writable_indexes.clone(),
                readonly_indexes: lookup.readonly_indexes.clone(),
            });
        }
        compact_lookups
    });

    // Determine version
    let version = match msg {
        VersionedMessage::Legacy(_) => 0,
        VersionedMessage::V0(_) => 0,
    };

    // Build complete key list for inner instructions
    let mut all_keys = static_keys.to_vec();

    // Add loaded addresses to all_keys
    let writable_loaded = &meta_proto.loaded_writable_addresses;
    let readonly_loaded = &meta_proto.loaded_readonly_addresses;

    all_keys.reserve(writable_loaded.len() + readonly_loaded.len());
    for addr_bytes in writable_loaded.iter().chain(readonly_loaded.iter()) {
        if addr_bytes.len() == 32 {
            if let Ok(pk) = Pubkey::try_from(addr_bytes.as_slice()) {
                all_keys.push(pk);
            }
        }
    }

    // Convert metadata
    let meta = convert_compact_meta(meta_proto, &all_keys, reg)?;

    Ok(CompactTransaction {
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

/// Convert protobuf metadata to CompactTransactionMeta
fn convert_compact_meta(
    meta: generated::TransactionStatusMeta,
    all_keys: &[Pubkey],
    reg: &mut KeyRegistry,
) -> Result<CompactTransactionMeta> {
    // Decode error
    let err = meta
        .err
        .as_ref()
        .map(|e| bincode::deserialize::<TransactionError>(&e.err))
        .transpose()
        .context("failed to deserialize TransactionError")?
        .map(|e| format!("{:?}", e));

    // Convert inner instructions
    let inner_instructions = if meta.inner_instructions_none {
        None
    } else if meta.inner_instructions.is_empty() {
        None
    } else {
        Some(
            meta.inner_instructions
                .into_iter()
                .map(|ui_inner| {
                    let instructions = ui_inner
                        .instructions
                        .into_iter()
                        .filter_map(|i| {
                            let idx = i.program_id_index as usize;
                            if idx < all_keys.len() {
                                Some(CompactInstruction {
                                    program_id: reg.get_or_insert(&all_keys[idx]),
                                    accounts: i.accounts,
                                    data: i.data,
                                    stack_height: i.stack_height,
                                })
                            } else {
                                None
                            }
                        })
                        .collect();

                    CompactInnerInstructions {
                        index: ui_inner.index as u8,
                        instructions,
                    }
                })
                .collect(),
        )
    };

    // Convert log messages
    let log_messages = if meta.log_messages_none {
        None
    } else if meta.log_messages.is_empty() {
        None
    } else {
        Some(meta.log_messages)
    };

    // Convert token balances
    let pre_token_balances = if meta.pre_token_balances.is_empty() {
        None
    } else {
        Some(
            meta.pre_token_balances
                .into_iter()
                .map(|tb| convert_compact_token_balance(tb, reg))
                .collect(),
        )
    };

    let post_token_balances = if meta.post_token_balances.is_empty() {
        None
    } else {
        Some(
            meta.post_token_balances
                .into_iter()
                .map(|tb| convert_compact_token_balance(tb, reg))
                .collect(),
        )
    };

    // Convert loaded addresses
    let loaded_addresses =
        if meta.loaded_writable_addresses.is_empty() && meta.loaded_readonly_addresses.is_empty() {
            None
        } else {
            let writable = meta
                .loaded_writable_addresses
                .into_iter()
                .filter_map(|bytes| {
                    if bytes.len() == 32 {
                        Pubkey::try_from(bytes.as_slice())
                            .ok()
                            .map(|pk| reg.get_or_insert(&pk))
                    } else {
                        None
                    }
                })
                .collect();

            let readonly = meta
                .loaded_readonly_addresses
                .into_iter()
                .filter_map(|bytes| {
                    if bytes.len() == 32 {
                        Pubkey::try_from(bytes.as_slice())
                            .ok()
                            .map(|pk| reg.get_or_insert(&pk))
                    } else {
                        None
                    }
                })
                .collect();

            Some(CompactLoadedAddresses { writable, readonly })
        };

    // Convert return data
    let return_data = meta.return_data.and_then(|rd| {
        if rd.program_id.len() == 32 {
            Pubkey::try_from(rd.program_id.as_slice())
                .ok()
                .map(|pk| (reg.get_or_insert(&pk), rd.data))
        } else {
            None
        }
    });

    // Convert compute units
    let compute_units_consumed = meta.compute_units_consumed;

    Ok(CompactTransactionMeta {
        err,
        fee: meta.fee,
        pre_balances: meta.pre_balances,
        post_balances: meta.post_balances,
        inner_instructions,
        log_messages,
        pre_token_balances,
        post_token_balances,
        rewards: None,
        loaded_addresses,
        return_data,
        compute_units_consumed,
    })
}

/// Convert protobuf TokenBalance to CompactTokenBalance
fn convert_compact_token_balance(
    tb: generated::TokenBalance,
    reg: &mut KeyRegistry,
) -> CompactTokenBalance {
    // Parse mint
    let mint = tb
        .mint
        .parse::<Pubkey>()
        .ok()
        .map(|pk| reg.get_or_insert(&pk))
        .unwrap_or(0);

    // Parse owner
    let owner = if tb.owner.is_empty() {
        0
    } else {
        tb.owner
            .parse::<Pubkey>()
            .ok()
            .map(|pk| reg.get_or_insert(&pk))
            .unwrap_or(0)
    };

    // Parse program_id
    let program_id = if tb.program_id.is_empty() {
        0
    } else {
        tb.program_id
            .parse::<Pubkey>()
            .ok()
            .map(|pk| reg.get_or_insert(&pk))
            .unwrap_or(0)
    };

    // Serialize ui_token_amount
    let ui_token_amount = tb
        .ui_token_amount
        .map(|amount| {
            serde_json::json!({
                "amount": amount.amount,
                "decimals": amount.decimals,
                "uiAmount": amount.ui_amount,
                "uiAmountString": amount.ui_amount_string,
            })
            .to_string()
        })
        .unwrap_or_default();

    CompactTokenBalance {
        account_index: tb.account_index as u8,
        mint,
        ui_token_amount,
        owner,
        program_id,
    }
}