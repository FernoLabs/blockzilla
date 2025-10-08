use crate::optimizer::{BlockWithIds, KeyRegistry};
use anyhow::{Context, Result};
use rusqlite::Connection;
use solana_reward_info::RewardType;
use solana_sdk::{bs58, pubkey::Pubkey};
use solana_transaction_status_client_types::EncodedConfirmedBlock;
use solana_transaction_status_client_types::{
    EncodedTransactionWithStatusMeta, Reward, UiCompiledInstruction, UiMessage, UiTransaction,
    UiTransactionStatusMeta, option_serializer::OptionSerializer,
};
use std::io::Read;
use std::{fs::File, io::SeekFrom};

/// List all available slots in the index file
pub fn list_slots(idx_path: &str, limit: Option<usize>) -> Result<()> {
    let idx_file =
        File::open(idx_path).context(format!("Failed to open index file: {}", idx_path))?;
    let idx_size = idx_file.metadata()?.len();
    let num_entries = idx_size / 16;

    println!("Index file: {}", idx_path);
    println!("Total entries in index: {}", num_entries);
    println!("Index file size: {} bytes", idx_size);

    let mut idx = File::open(idx_path)?;
    let mut slots = Vec::new();

    let max_to_read = limit.unwrap_or(num_entries as usize);

    for i in 0..num_entries.min(max_to_read as u64) {
        let mut slot_buf = [0u8; 8];
        let mut offset_buf = [0u8; 8];

        if idx.read_exact(&mut slot_buf).is_err() || idx.read_exact(&mut offset_buf).is_err() {
            eprintln!(
                "Warning: Failed to read entry {} - file might be truncated",
                i
            );
            break;
        }

        let slot = u64::from_le_bytes(slot_buf);
        let offset = u64::from_le_bytes(offset_buf);

        slots.push((slot, offset));
    }

    if slots.is_empty() {
        println!("\nNo slots found in index!");
        return Ok(());
    }

    println!("\n=== First {} slots ===", slots.len().min(20));
    for (i, (slot, offset)) in slots.iter().take(20).enumerate() {
        println!("  [{:4}] Slot: {:12}, Offset: {:12}", i, slot, offset);
    }

    if slots.len() > 20 {
        println!("\n... {} more entries ...", slots.len() - 20);

        println!("\n=== Last 10 slots ===");
        for (slot, offset) in slots.iter().rev().take(10).rev() {
            println!("  Slot: {:12}, Offset: {:12}", slot, offset);
        }
    }

    // Show some statistics
    let min_slot = slots.iter().map(|(s, _)| s).min().unwrap();
    let max_slot = slots.iter().map(|(s, _)| s).max().unwrap();
    println!("\n=== Statistics ===");
    println!("Slot range: {} to {}", min_slot, max_slot);
    println!("Total slots: {}", slots.len());

    Ok(())
}

/// Read and print a block as JSON (simple version - just the compact format)
pub fn read_and_print_block_compact(
    bin_path: &str,
    idx_path: &str,
    registry_path: &str,
    slot: u64,
) -> Result<()> {
    println!("=== Loading Registry ===");

    let conn = Connection::open(registry_path)?;
    let mut stmt = conn.prepare("SELECT id, pubkey FROM keymap ORDER BY id")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?))
    })?;

    let mut registry = KeyRegistry::new();
    for row in rows {
        let (id, pubkey_str) = row?;
        let pubkey = pubkey_str.parse::<Pubkey>()?;
        registry.by_id.push(pubkey);
        registry.by_pubkey.insert(pubkey, id);
        if id >= registry.next_id {
            registry.next_id = id + 1;
        }
    }

    println!("Loaded {} pubkeys\n", registry.len());

    // Find offset
    let offset = find_slot_offset(idx_path, slot)?;

    // Read compact block
    let compact_block = read_block_at_offset(bin_path, offset)?;

    // Reconstruct EncodedConfirmedBlock
    let encoded_block = reconstruct_encoded_block(&compact_block, &registry)?;

    // Print as JSON using the built-in Serde serialization
    println!("{}", serde_json::to_string_pretty(&encoded_block)?);

    Ok(())
}

/// Reconstruct EncodedConfirmedBlock from compact format
fn reconstruct_encoded_block(
    compact: &BlockWithIds,
    registry: &KeyRegistry,
) -> Result<EncodedConfirmedBlock> {
    // Reconstruct rewards
    let rewards: Vec<Reward> = compact
        .rewards
        .iter()
        .map(|r| Reward {
            pubkey: registry
                .get(r.pubkey)
                .map(|pk| pk.to_string())
                .unwrap_or_default(),
            lamports: r.lamports,
            post_balance: r.post_balance,
            reward_type: if let Some(ref str) = r.reward_type {
                match str.as_str() {
                    "Fee" => Some(RewardType::Fee),
                    "Rent" => Some(RewardType::Rent),
                    "Stacking" => Some(RewardType::Staking),
                    "Voting" => Some(RewardType::Voting),
                    _ => None,
                }
            } else {
                None
            },
            commission: r.commission,
        })
        .collect();

    // Reconstruct transactions
    let transactions: Vec<EncodedTransactionWithStatusMeta> = compact
        .transactions
        .iter()
        .map(|tx| {
            // Reconstruct message
            let account_keys: Vec<String> = tx
                .account_keys
                .iter()
                .map(|id| {
                    registry
                        .get(*id)
                        .map(|pk| pk.to_string())
                        .unwrap_or_default()
                })
                .collect();

            let instructions: Vec<UiCompiledInstruction> = tx
                .instructions
                .iter()
                .map(|ix| UiCompiledInstruction {
                    program_id_index: tx
                        .account_keys
                        .iter()
                        .position(|k| k == &ix.program_id)
                        .unwrap_or(0) as u8,
                    accounts: ix.accounts.clone(),
                    data: bs58::encode(&ix.data).into_string(),
                    stack_height: ix.stack_height,
                })
                .collect();

            let message = UiMessage::Raw(solana_transaction_status_client_types::UiRawMessage {
                header: tx.message_header,
                account_keys,
                recent_blockhash: tx.recent_blockhash.clone(),
                instructions,
                address_table_lookups: tx.address_table_lookups.as_ref().map(|lookups| {
                    lookups
                        .iter()
                        .map(|lookup| {
                            solana_transaction_status_client_types::UiAddressTableLookup {
                                account_key: registry
                                    .get(lookup.account_key)
                                    .map(|pk| pk.to_string())
                                    .unwrap_or_default(),
                                writable_indexes: lookup.writable_indexes.clone(),
                                readonly_indexes: lookup.readonly_indexes.clone(),
                            }
                        })
                        .collect()
                }),
            });

            let ui_transaction = UiTransaction {
                signatures: tx.signatures.iter().map(|s| s.to_string()).collect(),
                message,
            };

            // Reconstruct meta if present
            let meta = tx.meta.as_ref().map(|m| {
                UiTransactionStatusMeta {
                    err: None,      // fixme
                    status: Ok(()), // Simplified
                    fee: m.fee,
                    pre_balances: m.pre_balances.clone(),
                    post_balances: m.post_balances.clone(),
                    inner_instructions: match &m.inner_instructions {
                        Some(_) => OptionSerializer::Some(vec![]), // Would need full reconstruction
                        None => OptionSerializer::Skip,
                    },
                    log_messages: match &m.log_messages {
                        Some(logs) => OptionSerializer::Some(logs.clone()),
                        None => OptionSerializer::Skip,
                    },
                    pre_token_balances: OptionSerializer::Skip, // Would need full reconstruction
                    post_token_balances: OptionSerializer::Skip,
                    rewards: OptionSerializer::Skip,
                    loaded_addresses: OptionSerializer::Skip,
                    return_data: OptionSerializer::Skip,
                    compute_units_consumed: match m.compute_units_consumed {
                        Some(units) => OptionSerializer::Some(units),
                        None => OptionSerializer::Skip,
                    },
                    cost_units: OptionSerializer::Skip,
                }
            });

            EncodedTransactionWithStatusMeta {
                transaction: solana_transaction_status_client_types::EncodedTransaction::Json(
                    ui_transaction,
                ),
                meta,
                version: if tx.version == 0 {
                    None
                } else {
                    Some(solana_sdk::transaction::TransactionVersion::Number(
                        tx.version,
                    ))
                },
            }
        })
        .collect();

    Ok(EncodedConfirmedBlock {
        previous_blockhash: compact.previous_blockhash.clone(),
        blockhash: compact.blockhash.clone(),
        parent_slot: compact.parent_slot,
        transactions,
        rewards,
        block_time: compact.block_time,
        block_height: compact.block_height,
        num_partitions: None,
    })
}

/// Find the offset for a given slot in the index
fn find_slot_offset(idx_path: &str, slot: u64) -> Result<u64> {
    println!("=== Searching for Slot {} ===", slot);

    let idx_file = File::open(idx_path)?;
    let idx_size = idx_file.metadata()?.len();
    let num_entries = idx_size / 16;

    println!("Index entries: {}", num_entries);

    let mut idx = File::open(idx_path)?;
    let mut first_slot: Option<u64> = None;
    let mut last_slot: Option<u64> = None;

    for i in 0..num_entries {
        let mut slot_buf = [0u8; 8];
        let mut offset_buf = [0u8; 8];
        idx.read_exact(&mut slot_buf)?;
        idx.read_exact(&mut offset_buf)?;

        let idx_slot = u64::from_le_bytes(slot_buf);
        let offset = u64::from_le_bytes(offset_buf);

        if first_slot.is_none() {
            first_slot = Some(idx_slot);
        }
        last_slot = Some(idx_slot);

        if i < 5 {
            println!("  Entry {}: slot={}, offset={}", i, idx_slot, offset);
        }

        if idx_slot == slot {
            println!("\n✓ Found slot {} at offset {}\n", slot, offset);
            return Ok(offset);
        }
    }

    anyhow::bail!(
        "Slot {} not found.\nFirst slot: {:?}\nLast slot: {:?}\nTry: list-slots",
        slot,
        first_slot,
        last_slot
    )
}

/// Read a block from the binary file at a specific offset
fn read_block_at_offset(path: &str, offset: u64) -> Result<BlockWithIds> {
    use std::io::Seek;

    println!("=== Reading Block ===");
    println!("Offset: {}", offset);

    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(offset))?;

    let mut len_buf = [0u8; 4];
    f.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    println!("Compressed: {} bytes", len);

    if len == 0 || len > 100_000_000 {
        anyhow::bail!("Invalid size: {} bytes", len);
    }

    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf)?;

    let decompressed = zstd::bulk::decompress(&buf, 10_000_000)?;
    println!("Decompressed: {} bytes", decompressed.len());

    postcard::from_bytes(&decompressed).context("Failed to deserialize")
}
