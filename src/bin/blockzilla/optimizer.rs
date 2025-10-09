use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use blockzilla::block_stream::SolanaBlockStream;
use futures::io::AllowStdIo;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use solana_sdk::{
    bs58, message::MessageHeader, pubkey::Pubkey, signature::Signature,
    transaction::TransactionVersion,
};
use solana_transaction_status_client_types::{
    EncodedConfirmedBlock, EncodedTransactionWithStatusMeta, UiTransactionTokenBalance,
    option_serializer::OptionSerializer,
};

use crate::compat_block::cb_to_compact_block;

#[derive(Default, Debug, Clone)]
pub struct KeyRegistry {
    pub next_id: u32,
    pub by_pubkey: HashMap<Pubkey, u32>,
    pub by_id: Vec<Pubkey>,
}

impl KeyRegistry {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn get_or_insert(&mut self, key: &Pubkey) -> u32 {
        *self.by_pubkey.entry(*key).or_insert_with(|| {
            let id = self.next_id;
            self.next_id += 1;
            self.by_id.push(*key);
            id
        })
    }

    #[inline]
    pub fn get(&self, id: u32) -> Option<&Pubkey> {
        self.by_id.get(id as usize)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn save_to_sqlite(&self, path: &str) -> Result<()> {
        let mut conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS keymap (id INTEGER PRIMARY KEY, pubkey TEXT UNIQUE)",
            [],
        )?;
        // Use transaction for batch inserts
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

#[derive(Default, Debug)]
struct StageTimings {
    decode_total: f64,
    compact_total: f64,
    serialize_total: f64,
    compress_total: f64,
    write_total: f64,
    blocks: u64,
}

impl StageTimings {
    #[inline]
    fn record(&mut self, d: f64, c: f64, s: f64, z: f64, w: f64) {
        self.decode_total += d;
        self.compact_total += c;
        self.serialize_total += s;
        self.compress_total += z;
        self.write_total += w;
        self.blocks += 1;
    }

    fn print_periodic(&self, count: u64, bytes: u64, start: Instant) {
        if self.blocks == 0 {
            println!("🧮 {:>7} blk | collecting…", count);
            return;
        }
        let total = self.decode_total
            + self.compact_total
            + self.serialize_total
            + self.compress_total
            + self.write_total;
        let pct = |x: f64| if total > 0.0 { x / total * 100.0 } else { 0.0 };
        let avg_ms = total / self.blocks as f64;
        let throughput = count as f64 / start.elapsed().as_secs_f64().max(0.001);
        println!(
            "🧮 {:>7} blk | decode {:>6.2}% | compact {:>6.2}% | ser {:>6.2}% | comp {:>6.2}% | write {:>6.2}% | avg {:>6.2} ms/blk | {:.1} blk/s | {:.2} MB",
            count,
            pct(self.decode_total),
            pct(self.compact_total),
            pct(self.serialize_total),
            pct(self.compress_total),
            pct(self.write_total),
            avg_ms,
            throughput,
            bytes as f64 / 1_000_000.0
        );
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactInstruction {
    pub program_id: u32,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactInnerInstructions {
    pub index: u8,
    pub instructions: Vec<CompactInstruction>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTokenBalance {
    pub account_index: u8,
    pub mint: u32,
    pub ui_token_amount: String,
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
    pub rewards: Option<Vec<CompactReward>>,
    pub loaded_addresses: Option<CompactLoadedAddresses>,
    pub return_data: Option<(u32, Vec<u8>)>,
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTransaction {
    pub signatures: Vec<Signature>,
    pub message_header: MessageHeader,
    pub account_keys: Vec<u32>,
    pub recent_blockhash: String,
    pub instructions: Vec<CompactInstruction>,
    pub address_table_lookups: Option<Vec<CompactAddressTableLookup>>,
    pub meta: Option<CompactTransactionMeta>,
    pub version: u8,
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
    pub num_transactions: u64,
}

pub fn to_compact_block(
    block: &EncodedConfirmedBlock,
    reg: &mut KeyRegistry,
) -> Result<BlockWithIds> {
    let slot = block.parent_slot + 1;
    let blockhash = block.blockhash.clone();
    let previous_blockhash = block.previous_blockhash.clone();
    let parent_slot = block.parent_slot;
    let block_time = block.block_time;
    let block_height = block.block_height;

    // Pre-allocate with expected capacity
    let mut rewards = Vec::with_capacity(block.rewards.len());
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
    let mut transactions = Vec::with_capacity(block.transactions.len());

    for EncodedTransactionWithStatusMeta {
        transaction,
        meta,
        version,
    } in &block.transactions
    {
        let Some(decoded_tx) = transaction.decode() else {
            continue;
        };
        let msg = decoded_tx.message;

        // Map static account keys - pre-allocate
        let static_keys = msg.static_account_keys();
        let mut account_keys = Vec::with_capacity(static_keys.len());
        for k in static_keys.iter() {
            account_keys.push(reg.get_or_insert(k));
        }

        let message_header = *msg.header();
        let recent_blockhash = msg.recent_blockhash().to_string();

        // Extract instructions - pre-allocate
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

        // Extract address table lookups
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

        let signatures = decoded_tx.signatures;

        // Determine version
        let tx_version = match version {
            Some(TransactionVersion::Number(n)) => *n,
            Some(TransactionVersion::Legacy(_)) | None => 0,
        };

        // Build complete key list for inner instructions
        let mut all_keys = static_keys.to_vec();

        if let Some(meta) = meta
            && let OptionSerializer::Some(loaded) = &meta.loaded_addresses
        {
            // Estimate capacity
            let extra_capacity = loaded.writable.len() + loaded.readonly.len();
            all_keys.reserve(extra_capacity);

            for p_str in loaded.writable.iter().chain(loaded.readonly.iter()) {
                all_keys.push(Pubkey::from_str_const(p_str));
            }
        }

        // Extract transaction metadata
        let compact_meta = meta.as_ref().map(|m| {
            // Inner instructions
            let inner_instructions = match &m.inner_instructions {
                OptionSerializer::Some(inners) => {
                    let mut compact_inners = Vec::with_capacity(inners.len());
                    for ui_inner in inners {
                        let instructions = ui_inner.instructions.iter().filter_map(|ui_ix| {
                            match ui_ix {
                                solana_transaction_status_client_types::UiInstruction::Compiled(compiled) => {
                                    // Decode base58 data
                                    let data_bytes = bs58::decode(&compiled.data)
                                        .into_vec()
                                        .unwrap_or_default();
                                    Some(CompactInstruction {
                                        program_id: reg.get_or_insert(&all_keys[compiled.program_id_index as usize]),
                                        accounts: compiled.accounts.clone(),
                                        data: data_bytes,
                                        stack_height: compiled.stack_height,
                                    })
                                },
                                _ => None,
                            }
                        }).collect();
                        compact_inners.push(CompactInnerInstructions {
                            index: ui_inner.index,
                            instructions,
                        });
                    }
                    Some(compact_inners)
                },
                _ => None,
            };

            // Token balances - use helper function
            let pre_token_balances = match &m.pre_token_balances {
                OptionSerializer::Some(balances) => {
                    Some(balances.iter().map(|x| convert_token_balance(x, reg)).collect())
                },
                _ => None,
            };

            let post_token_balances = match &m.post_token_balances {
                OptionSerializer::Some(balances) => {
                    Some(balances.iter().map(|x| convert_token_balance(x, reg)).collect())
                },
                _ => None,
            };

            // Loaded addresses
            let loaded_addresses = m.loaded_addresses.as_ref().map(|addrs| {
                let writable: Vec<u32> = addrs.writable.iter()
                    .filter_map(|s| s.parse::<Pubkey>().ok())
                    .map(|pk| reg.get_or_insert(&pk))
                    .collect();
                let readonly: Vec<u32> = addrs.readonly.iter()
                    .filter_map(|s| s.parse::<Pubkey>().ok())
                    .map(|pk| reg.get_or_insert(&pk))
                    .collect();
                CompactLoadedAddresses { writable, readonly }
            });

            // Return data
            let return_data = match &m.return_data {
                OptionSerializer::Some(rd) => {
                    rd.program_id.parse::<Pubkey>().ok().map(|pk| {
                        let data_bytes = bs58::decode(&rd.data.0)
                            .into_vec()
                            .unwrap_or_else(|_| rd.data.0.as_bytes().to_vec());
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

#[inline]
fn convert_token_balance(
    tb: &UiTransactionTokenBalance,
    reg: &mut KeyRegistry,
) -> CompactTokenBalance {
    let mint = tb
        .mint
        .parse::<Pubkey>()
        .ok()
        .map(|pk| reg.get_or_insert(&pk))
        .unwrap_or(0);

    let owner = match &tb.owner {
        OptionSerializer::Some(o) => o
            .parse::<Pubkey>()
            .ok()
            .map(|pk| reg.get_or_insert(&pk))
            .unwrap_or(0),
        _ => 0,
    };

    let program_id = match &tb.program_id {
        OptionSerializer::Some(p) => p
            .parse::<Pubkey>()
            .ok()
            .map(|pk| reg.get_or_insert(&pk))
            .unwrap_or(0),
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

#[inline]
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
    let mut timings = StageTimings::default();
    let start = Instant::now();

    // Larger buffer sizes for better I/O performance
    let bin_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&bin_path)?;
    let mut bin = BufWriter::with_capacity(16 * 1024 * 1024, bin_file);

    let idx_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&idx_path)?;
    let mut idx = BufWriter::with_capacity(2 * 1024 * 1024, idx_file);

    let mut count = 0u64;
    let mut current_offset = 0u64;
    let mut total_bytes = 0u64;
    let mut last_log = Instant::now();

    while let Some(block) = stream.next_solana_block().await? {
        let t0 = Instant::now();
        //let rpc_block: EncodedConfirmedBlock = block.try_into()?;
        let t1 = Instant::now();
        //let compact = to_compact_block(&rpc_block, &mut reg)?;
        let compact = cb_to_compact_block(block, &mut reg)?;
        let t2 = Instant::now();

        let raw = postcard::to_allocvec(&compact)?;
        let t3 = Instant::now();

        // Use faster zstd compression level (3 instead of 5)
        let compressed = zstd::bulk::compress(&raw, 3)?;
        let t4 = Instant::now();

        let len = compressed.len() as u32;
        bin.write_all(&len.to_le_bytes())?;
        bin.write_all(&compressed)?;
        idx.write_all(&compact.slot.to_le_bytes())?;
        idx.write_all(&current_offset.to_le_bytes())?;
        let t5 = Instant::now();

        current_offset += 4 + compressed.len() as u64;
        total_bytes += compressed.len() as u64;
        count += 1;

        timings.record(
            (t1 - t0).as_secs_f64() * 1000.0,
            (t2 - t1).as_secs_f64() * 1000.0,
            (t3 - t2).as_secs_f64() * 1000.0,
            (t4 - t3).as_secs_f64() * 1000.0,
            (t5 - t4).as_secs_f64() * 1000.0,
        );

        // Less frequent logging and flushing
        if last_log.elapsed() > Duration::from_secs(15) {
            bin.flush()?;
            idx.flush()?;
            reg.save_to_sqlite(reg_path.to_str().unwrap())?;
            timings.print_periodic(count, total_bytes, start);
            last_log = Instant::now();
        }
    }

    bin.flush()?;
    idx.flush()?;
    reg.save_to_sqlite(reg_path.to_str().unwrap())?;

    timings.print_periodic(count, total_bytes, start);
    println!(
        "✅ Done: {count} blocks → {}, {} unique keys ({:.2} MB total)",
        bin_path.display(),
        reg.len(),
        total_bytes as f64 / 1_000_000.0
    );

    Ok(())
}
