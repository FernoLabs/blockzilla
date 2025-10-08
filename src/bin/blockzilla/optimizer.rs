use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use blockzilla::block_stream::SolanaBlockStream;
use futures::io::AllowStdIo;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use solana_sdk::{bs58, message::MessageHeader, pubkey::Pubkey, signature::Signature, transaction::TransactionVersion};
use solana_transaction_status_client_types::{
    EncodedConfirmedBlock, EncodedTransactionWithStatusMeta, option_serializer::OptionSerializer,
    UiTransactionTokenBalance,
};

// -----------------------------------------------------------------------------
// 1. KeyRegistry (Pubkey ↔ u32) with SQLite persistence
// -----------------------------------------------------------------------------
#[derive(Default, Debug)]
pub struct KeyRegistry {
    pub next_id: u32,
    pub by_pubkey: HashMap<Pubkey, u32>,
    pub by_id: Vec<Pubkey>,
}

impl KeyRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_insert(&mut self, key: &Pubkey) -> u32 {
        if let Some(id) = self.by_pubkey.get(key) {
            *id
        } else {
            let id = self.next_id;
            self.next_id += 1;
            self.by_pubkey.insert(*key, id);
            self.by_id.push(*key);
            id
        }
    }

    pub fn get(&self, id: u32) -> Option<&Pubkey> {
        self.by_id.get(id as usize)
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn save_to_sqlite(&self, path: &str) -> Result<()> {
        let mut conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS keymap (id INTEGER PRIMARY KEY, pubkey TEXT UNIQUE)",
            [],
        )?;
        let tx = conn.transaction()?;
        {
            let mut stmt =
                tx.prepare("INSERT OR IGNORE INTO keymap (id, pubkey) VALUES (?1, ?2)")?;
            for (id, pk) in self.by_id.iter().enumerate() {
                stmt.execute(params![id as u32, pk.to_string()])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// 2. Compact structs with COMPLETE metadata for full reconstruction
// -----------------------------------------------------------------------------
#[derive(Debug, Serialize, Deserialize)]
pub struct CompactInstruction {
    pub program_id: u32,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>, // For inner instructions
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactInnerInstructions {
    pub index: u8, // Index of the outer instruction
    pub instructions: Vec<CompactInstruction>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTokenBalance {
    pub account_index: u8,
    pub mint: u32,
    pub ui_token_amount: String, // JSON serialized
    pub owner: u32,
    pub program_id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactAddressTableLookup {
    pub account_key: u32,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactLoadedAddresses {
    pub writable: Vec<u32>,
    pub readonly: Vec<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTransactionMeta {
    pub err: Option<String>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<CompactInnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<CompactTokenBalance>>,
    pub post_token_balances: Option<Vec<CompactTokenBalance>>,
    pub rewards: Option<Vec<CompactReward>>, // Per-tx rewards if any
    pub loaded_addresses: Option<CompactLoadedAddresses>,
    pub return_data: Option<(u32, Vec<u8>)>, // (program_id, data)
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTransaction {
    pub signatures: Vec<Signature>,
    pub message_header: MessageHeader, // num_required_signatures, etc.
    pub account_keys: Vec<u32>, // Static account keys
    pub recent_blockhash: String,
    pub instructions: Vec<CompactInstruction>,
    pub address_table_lookups: Option<Vec<CompactAddressTableLookup>>,
    pub meta: Option<CompactTransactionMeta>,
    pub version: u8, // 0 = legacy, 1 = v0
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompactReward {
    pub pubkey: u32,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: Option<String>,
    pub commission: Option<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockWithIds {
    pub slot: u64,
    pub blockhash: String,
    pub previous_blockhash: String,
    pub parent_slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub rewards: Vec<CompactReward>,
    pub transactions: Vec<CompactTransaction>,
    pub num_transactions: u64, // Total count including votes
}

// -----------------------------------------------------------------------------
// 3. Convert EncodedConfirmedBlock → BlockWithIds (COMPLETE)
// -----------------------------------------------------------------------------
fn to_compact_block(block: &EncodedConfirmedBlock, reg: &mut KeyRegistry) -> Result<BlockWithIds> {
    let slot = block.parent_slot + 1;
    let blockhash = block.blockhash.clone();
    let previous_blockhash = block.previous_blockhash.clone();
    let parent_slot = block.parent_slot;
    
    // Handle Option types directly (not OptionSerializer)
    let block_time = block.block_time;
    let block_height = block.block_height;

    // Collect rewards - block.rewards is a Vec, not OptionSerializer
    let mut rewards = Vec::new();
    for rw in &block.rewards {
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

    let num_transactions = block.transactions.len() as u64;
    let mut transactions = Vec::new();

    for EncodedTransactionWithStatusMeta { transaction, meta, version } in &block.transactions {
        let Some(decoded_tx) = transaction.decode() else {
            continue;
        };
        let msg = decoded_tx.message;

        // Map static account keys
        let account_keys = msg
            .static_account_keys()
            .iter()
            .map(|k| reg.get_or_insert(k))
            .collect::<Vec<_>>();

        // Get message header
        let message_header = *msg.header();

        // Get recent blockhash
        let recent_blockhash = msg.recent_blockhash().to_string();

        // Extract instructions
        let mut instructions = Vec::new();
        for ix in msg.instructions() {
            let all_keys = msg.static_account_keys();
            let program_id = reg.get_or_insert(&all_keys[ix.program_id_index as usize]);
            instructions.push(CompactInstruction {
                program_id,
                accounts: ix.accounts.clone(),
                data: ix.data.clone(),
                stack_height: None,
            });
        }

        // Extract address table lookups
        let address_table_lookups = if let Some(lookups) = msg.address_table_lookups() {
            let mut compact_lookups = Vec::new();
            for lookup in lookups {
                compact_lookups.push(CompactAddressTableLookup {
                    account_key: reg.get_or_insert(&lookup.account_key),
                    writable_indexes: lookup.writable_indexes.clone(),
                    readonly_indexes: lookup.readonly_indexes.clone(),
                });
            }
            Some(compact_lookups)
        } else {
            None
        };

        let signatures = decoded_tx.signatures;

        // Determine version - TransactionVersion is an enum with Number(u8) variant
        let tx_version = match version {
            Some(v) => match v {
                TransactionVersion::Number(n) => *n,
                TransactionVersion::Legacy(_) => 0,
            },
            None => 0, // Legacy transactions
        };

        // Keep reference to account keys for inner instructions
        let static_keys = msg.static_account_keys().to_vec();

        // Extract COMPLETE transaction metadata
        let compact_meta = meta.as_ref().map(|m| {
            // Inner instructions
            let inner_instructions = match &m.inner_instructions {
                OptionSerializer::Some(inners) => {
                    Some(inners.iter().map(|ui_inner| {
                        let instructions = ui_inner.instructions.iter().filter_map(|ui_ix| {
                            // UiInstruction can be Compiled or Parsed - we need Compiled
                            match ui_ix {
                                solana_transaction_status_client_types::UiInstruction::Compiled(compiled) => {
                                    // compiled.data is a base58 String, need to decode it
                                    let data_bytes = bs58::decode(&compiled.data).into_vec().unwrap_or_default();
                                    Some(CompactInstruction {
                                        program_id: reg.get_or_insert(&static_keys[compiled.program_id_index as usize]),
                                        accounts: compiled.accounts.clone(),
                                        data: data_bytes,
                                        stack_height: compiled.stack_height,
                                    })
                                },
                                _ => None, // Skip parsed instructions
                            }
                        }).collect();
                        
                        CompactInnerInstructions {
                            index: ui_inner.index,
                            instructions,
                        }
                    }).collect())
                },
                _ => None,
            };

            // Token balances
            let pre_token_balances = match &m.pre_token_balances {
                OptionSerializer::Some(balances) => Some(balances.iter().map(|x|convert_token_balance(x, reg)).collect()),
                _ => None,
            };

            let post_token_balances = match &m.post_token_balances {
                OptionSerializer::Some(balances) => Some(balances.iter().map(|x|convert_token_balance(x, reg)).collect()),
                _ => None,
            };

            // Loaded addresses
            let loaded_addresses = m.loaded_addresses.as_ref().map(|addrs| {
                CompactLoadedAddresses {
                    writable: addrs.writable.iter().filter_map(|s| s.parse::<Pubkey>().ok()).map(|pk| reg.get_or_insert(&pk)).collect(),
                    readonly: addrs.readonly.iter().filter_map(|s| s.parse::<Pubkey>().ok()).map(|pk| reg.get_or_insert(&pk)).collect(),
                }
            });

            // Return data
            let return_data = match &m.return_data {
                OptionSerializer::Some(rd) => {
                    rd.program_id.parse::<Pubkey>().ok().map(|pk| {
                        // rd.data is (String, UiReturnDataEncoding) - we need the decoded bytes
                        let data_bytes = match bs58::decode(&rd.data.0).into_vec() {
                            Ok(bytes) => bytes,
                            Err(_) => rd.data.0.as_bytes().to_vec(), // Fallback
                        };
                        (reg.get_or_insert(&pk), data_bytes)
                    })
                },
                _ => None,
            };

            CompactTransactionMeta {
                err: m.err.as_ref().map(|e| format!("{:?}", e)),
                fee: m.fee,
                pre_balances: m.pre_balances.clone(),
                post_balances: m.post_balances.clone(),
                inner_instructions,
                log_messages: match &m.log_messages {
                    OptionSerializer::Some(logs) => Some(logs.clone()),
                    _ => None,
                },
                pre_token_balances,
                post_token_balances,
                rewards: None,
                loaded_addresses,
                return_data,
                compute_units_consumed: match &m.compute_units_consumed {
                    OptionSerializer::Some(units) => Some(*units),
                    _ => None,
                },
            }
        });

        transactions.push(CompactTransaction {
            signatures,
            message_header,
            account_keys,
            recent_blockhash,
            instructions,
            address_table_lookups,
            meta: compact_meta,
            version: tx_version,
        });
    }

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


fn convert_token_balance(tb: &UiTransactionTokenBalance, reg:&mut KeyRegistry) -> CompactTokenBalance {
    let mint = tb.mint.parse::<Pubkey>().ok().map(|pk| reg.get_or_insert(&pk)).unwrap_or(0);
    
    let owner = match &tb.owner {
        OptionSerializer::Some(o) => o.parse::<Pubkey>().ok().map(|pk| reg.get_or_insert(&pk)).unwrap_or(0),
        _ => 0,
    };
    
    let program_id = match &tb.program_id {
        OptionSerializer::Some(p) => p.parse::<Pubkey>().ok().map(|pk| reg.get_or_insert(&pk)).unwrap_or(0),
        _ => 0,
    };
    
    CompactTokenBalance {
        account_index: tb.account_index,
        mint,
        ui_token_amount: serde_json::to_string(&tb.ui_token_amount).unwrap_or_default(),
        owner,
        program_id,
    }
}

// -----------------------------------------------------------------------------
// 4. Main optimizer — postcard + zstd + BufWriter
// -----------------------------------------------------------------------------

fn extract_epoch_from_path(path: &str) -> Option<u64> {
    Path::new(path)
        .file_stem()?
        .to_string_lossy()
        .split('-')
        .nth(1)?
        .parse::<u64>()
        .ok()
}

pub async fn run_car_optimizer(path: &str, output_dir: Option<String>) -> Result<()> {
    let epoch = extract_epoch_from_path(path)
        .context("Could not parse epoch number from input filename")?;

    let file = File::open(path)?;
    let reader = AllowStdIo::new(file);
    let mut stream = SolanaBlockStream::new(reader).await?;

    let out_dir = PathBuf::from(output_dir.unwrap_or_else(|| "optimized".into()));
    std::fs::create_dir_all(&out_dir)?;

    let bin_path = out_dir.join(format!("epoch-{epoch}.bin"));
    let idx_path = out_dir.join(format!("epoch-{epoch}.idx"));
    let reg_path = out_dir.join("registry.sqlite");

    let mut reg = KeyRegistry::new();

    let bin_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&bin_path)
        .context("Failed to open .bin file")?;
    let mut bin = BufWriter::with_capacity(8 * 1024 * 1024, bin_file);

    let idx_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&idx_path)
        .context("Failed to open .idx file")?;
    let mut idx = BufWriter::with_capacity(1024 * 1024, idx_file);

    let mut count = 0u64;
    let mut current_offset = 0u64; // Track offset manually instead of using file metadata

    while let Some(block) = stream.next_solana_block().await? {
        let rpc_block: EncodedConfirmedBlock = block.try_into()?;
        let compact = to_compact_block(&rpc_block, &mut reg)?;

        // Serialize + compress
        let raw = postcard::to_allocvec(&compact)?;
        let compressed = zstd::bulk::compress(&raw, 5)?;

        let len = compressed.len() as u32;

        // Write length prefix and compressed data to binary file
        bin.write_all(&len.to_le_bytes())?;
        bin.write_all(&compressed)?;

        // Write index entry: slot and offset
        idx.write_all(&compact.slot.to_le_bytes())?;
        idx.write_all(&current_offset.to_le_bytes())?;

        // Update offset for next block (4 bytes for length + compressed data size)
        current_offset += 4 + compressed.len() as u64;

        count += 1;

        if count.is_multiple_of(10_000) {
            bin.flush()?;
            idx.flush()?;
            println!(
                "Processed {} blocks, {} unique keys, file ≈ {:.2} MB",
                count,
                reg.len(),
                current_offset as f64 / 1_000_000.0
            );
            reg.save_to_sqlite(reg_path.to_str().unwrap())?;
        }
    }

    bin.flush()?;
    idx.flush()?;
    reg.save_to_sqlite(reg_path.to_str().unwrap())?;

    println!(
        "✅ Done: {} blocks → {}, {} unique keys",
        count,
        bin_path.display(),
        reg.len()
    );
    Ok(())
}