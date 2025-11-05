use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use indicatif::ProgressBar;
use serde::Serialize;
use std::{
    convert::TryInto,
    io::SeekFrom,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
    sync::{Mutex, mpsc},
    task,
};

use crate::optimizer::{
    BlockWithIds, CompactInstruction, CompactLoadedAddresses, CompactTokenBalance,
    CompactTransaction, CompactTransactionMeta, OrderedPubkeyRegistry,
};

pub const LOG_INTERVAL_SECS: u64 = 2;

// ============================================================================
// Sequential block reading with async I/O
// ============================================================================
pub async fn read_compressed_blocks(epoch: u64, input_dir: &str) -> Result<()> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));

    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }

    let file = File::open(&bin_path).await?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);

    let start = Instant::now();
    let mut last_log = start;
    let pb = ProgressBar::new_spinner();

    let mut blocks_count = 0u64;
    let mut tx_count = 0u64;
    let mut decompress_buf = Vec::with_capacity(2 * 1024 * 1024);

    loop {
        // Read length prefix
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        // Read compressed block
        let mut compressed = vec![0u8; len];
        reader.read_exact(&mut compressed).await?;

        // Decompress
        decompress_buf.clear();
        zstd::stream::copy_decode(&compressed[..], &mut decompress_buf)
            .context("Failed to decompress block")?;

        // Deserialize
        let block: BlockWithIds =
            postcard::from_bytes(&decompress_buf).context("Failed to deserialize BlockWithIds")?;

        tx_count += block.num_transactions;
        blocks_count += 1;

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS",
                blocks_count,
                blocks_count as f64 / elapsed.as_secs_f64(),
                tx_count,
                if elapsed.as_secs() > 0 {
                    tx_count / elapsed.as_secs()
                } else {
                    0
                },
            ));
        }
    }

    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS | {:.1}s",
        blocks_count,
        blocks_count as f64 / start.elapsed().as_secs_f64(),
        tx_count,
        if start.elapsed().as_secs() > 0 {
            tx_count / start.elapsed().as_secs()
        } else {
            0
        },
        start.elapsed().as_secs_f64()
    ));

    Ok(())
}

// ============================================================================
// Parallel block reading with async I/O
// ============================================================================
pub async fn read_compressed_blocks_par(epoch: u64, input_dir: &str, jobs: usize) -> Result<()> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));

    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }

    let blocks_count = Arc::new(AtomicU64::new(0));
    let tx_count = Arc::new(AtomicU64::new(0));

    // Channel for compressed blocks
    let (tx, rx) = mpsc::channel::<Vec<u8>>(jobs * 4);
    let rx = Arc::new(Mutex::new(rx));

    // Spawn worker tasks for decompression and deserialization
    for _ in 0..jobs {
        let rx = Arc::clone(&rx);
        let blocks_count = Arc::clone(&blocks_count);
        let tx_count = Arc::clone(&tx_count);

        tokio::task::spawn_blocking(move || {
            let mut decompress_buf = Vec::with_capacity(2 * 1024 * 1024);

            loop {
                let Some(compressed) = ({
                    let mut guard = rx.blocking_lock();
                    guard.blocking_recv()
                }) else {
                    break;
                };

                // Decompress
                decompress_buf.clear();
                if let Err(e) = zstd::stream::copy_decode(&compressed[..], &mut decompress_buf) {
                    eprintln!("Decompression error: {}", e);
                    continue;
                }

                // Deserialize
                match postcard::from_bytes::<BlockWithIds>(&decompress_buf) {
                    Ok(block) => {
                        tx_count.fetch_add(block.num_transactions, Ordering::Relaxed);
                        blocks_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Deserialization error: {}", e);
                    }
                }
            }
        });
    }

    // Main task: read blocks and feed to workers
    let file = File::open(&bin_path).await?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);

    let pb = ProgressBar::new_spinner();
    let start = Instant::now();
    let mut last_log = start;

    loop {
        // Read length prefix
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        // Read compressed block
        let mut compressed = vec![0u8; len];
        reader.read_exact(&mut compressed).await?;

        // Send to workers
        if tx.send(compressed).await.is_err() {
            break;
        }

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);

            let b = blocks_count.load(Ordering::Relaxed);
            let t = tx_count.load(Ordering::Relaxed);

            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS",
                b,
                b as f64 / elapsed.as_secs_f64(),
                t,
                if elapsed.as_secs() > 0 {
                    t / elapsed.as_secs()
                } else {
                    0
                },
            ));
        }
    }

    // Close channel and wait for workers
    drop(tx);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let elapsed = start.elapsed();
    let b = blocks_count.load(Ordering::Relaxed);
    let t = tx_count.load(Ordering::Relaxed);

    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS | {:.1}s",
        b,
        b as f64 / elapsed.as_secs_f64(),
        t,
        if elapsed.as_secs() > 0 {
            t / elapsed.as_secs()
        } else {
            0
        },
        elapsed.as_secs_f64()
    ));

    Ok(())
}

// ============================================================================
// Analyze block contents
// ============================================================================
pub async fn analyze_compressed_blocks(epoch: u64, input_dir: &str) -> Result<()> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));

    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }

    let file = File::open(&bin_path).await?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);

    let mut blocks_count = 0u64;
    let mut total_txs = 0u64;
    let mut total_instructions = 0u64;
    let mut total_inner_instructions = 0u64;
    let mut blocks_with_rewards = 0u64;
    let mut decompress_buf = Vec::with_capacity(2 * 1024 * 1024);

    let pb = ProgressBar::new_spinner();

    loop {
        // Read length prefix
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        // Read compressed block
        let mut compressed = vec![0u8; len];
        reader.read_exact(&mut compressed).await?;

        // Decompress
        decompress_buf.clear();
        zstd::stream::copy_decode(&compressed[..], &mut decompress_buf)?;

        // Deserialize
        let block: BlockWithIds = postcard::from_bytes(&decompress_buf)?;

        blocks_count += 1;
        total_txs += block.num_transactions;
        if !block.rewards.is_empty() {
            blocks_with_rewards += 1;
        }

        for tx in &block.transactions {
            total_instructions += tx.instructions.len() as u64;
            if let Some(inner) = tx.meta.as_ref().and_then(|m| m.inner_instructions.as_ref()) {
                for ii in inner {
                    total_inner_instructions += ii.instructions.len() as u64;
                }
            }
        }

        if blocks_count % 1000 == 0 {
            pb.set_message(format!("Analyzed {} blocks...", blocks_count));
        }
    }

    pb.finish();

    println!("\n📊 Analysis Results:");
    println!("  Epoch:              {}", epoch);
    println!("  Total Blocks:       {}", blocks_count);
    println!("  Total Transactions: {}", total_txs);
    println!(
        "  Avg TXs/Block:      {:.2}",
        total_txs as f64 / blocks_count as f64
    );
    println!("  Total Instructions: {}", total_instructions);
    println!("  Total Inner Ixs:    {}", total_inner_instructions);
    println!("  Blocks w/ Rewards:  {}", blocks_with_rewards);

    Ok(())
}

// ============================================================================
// Read a specific block with registry expansion
// ============================================================================
pub async fn read_compressed_block_with_registry(
    epoch: u64,
    slot: u64,
    input_dir: &str,
    registry_dir: &str,
) -> Result<()> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));
    let idx_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.idx"));

    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }
    if !idx_path.exists() {
        anyhow::bail!("Index file not found: {}", idx_path.display());
    }

    let idx_bytes = tokio::fs::read(&idx_path)
        .await
        .with_context(|| format!("failed to read {}", idx_path.display()))?;

    if idx_bytes.len() % 16 != 0 {
        anyhow::bail!(
            "index file {} has invalid length {} (expected multiple of 16)",
            idx_path.display(),
            idx_bytes.len()
        );
    }

    let mut target_offset: Option<u64> = None;
    for chunk in idx_bytes.chunks_exact(16) {
        let chunk_slot = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
        if chunk_slot == slot {
            let offset = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
            target_offset = Some(offset);
            break;
        }
    }

    let Some(offset) = target_offset else {
        anyhow::bail!("slot {slot} not found in index {}", idx_path.display());
    };

    let registry_dir = registry_dir.to_string();
    let registry = task::spawn_blocking(move || OrderedPubkeyRegistry::load(registry_dir, epoch))
        .await
        .context("failed to load registry")??;

    let file = File::open(&bin_path)
        .await
        .with_context(|| format!("failed to open {}", bin_path.display()))?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);
    reader
        .seek(SeekFrom::Start(offset))
        .await
        .with_context(|| format!("failed to seek to offset {}", offset))?;

    let mut len_bytes = [0u8; 4];
    reader
        .read_exact(&mut len_bytes)
        .await
        .context("failed to read frame length")?;
    let len = u32::from_le_bytes(len_bytes) as usize;

    let mut compressed = vec![0u8; len];
    reader
        .read_exact(&mut compressed)
        .await
        .context("failed to read compressed block")?;

    let mut decompress_buf = Vec::with_capacity(len * 4);
    zstd::stream::copy_decode(&compressed[..], &mut decompress_buf)
        .context("failed to decompress block")?;

    let block: BlockWithIds =
        postcard::from_bytes(&decompress_buf).context("failed to deserialize BlockWithIds")?;

    let resolved = resolve_block(&block, &registry);
    let json = serde_json::to_string_pretty(&resolved).context("failed to serialize block")?;
    println!("{}", json);

    Ok(())
}

#[derive(Serialize)]
struct ResolvedBlock {
    slot: u64,
    blockhash: String,
    previous_blockhash: String,
    parent_slot: u64,
    block_time: Option<i64>,
    block_height: Option<u64>,
    rewards: Vec<ResolvedReward>,
    transactions: Vec<ResolvedTransaction>,
    num_transactions: u64,
}

#[derive(Serialize)]
struct ResolvedReward {
    pubkey: Option<String>,
    lamports: i64,
    post_balance: u64,
    reward_type: Option<String>,
    commission: Option<u8>,
}

#[derive(Serialize)]
struct ResolvedTransaction {
    signatures: Vec<String>,
    message_header: ResolvedMessageHeader,
    account_keys: Vec<Option<String>>,
    recent_blockhash: String,
    instructions: Vec<ResolvedInstruction>,
    address_table_lookups: Option<Vec<ResolvedAddressTableLookup>>,
    meta: Option<ResolvedTransactionMeta>,
    version: u8,
}

#[derive(Serialize)]
struct ResolvedMessageHeader {
    num_required_signatures: u8,
    num_readonly_signed_accounts: u8,
    num_readonly_unsigned_accounts: u8,
}

#[derive(Serialize)]
struct ResolvedInstruction {
    program_id: Option<String>,
    accounts: Vec<Option<String>>,
    data: String,
    stack_height: Option<u32>,
}

#[derive(Serialize)]
struct ResolvedAddressTableLookup {
    account_key: Option<String>,
    writable_indexes: Vec<u8>,
    readonly_indexes: Vec<u8>,
}

#[derive(Serialize)]
struct ResolvedTransactionMeta {
    err: Option<String>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<ResolvedInnerInstructions>>,
    log_messages: Option<Vec<String>>,
    pre_token_balances: Option<Vec<ResolvedTokenBalance>>,
    post_token_balances: Option<Vec<ResolvedTokenBalance>>,
    rewards: Option<Vec<ResolvedReward>>,
    loaded_addresses: Option<ResolvedLoadedAddresses>,
    return_data: Option<ResolvedReturnData>,
    compute_units_consumed: Option<u64>,
}

#[derive(Serialize)]
struct ResolvedInnerInstructions {
    index: u8,
    instructions: Vec<ResolvedInstruction>,
}

#[derive(Serialize)]
struct ResolvedTokenBalance {
    account_index: u8,
    mint: Option<String>,
    ui_token_amount: String,
    owner: Option<String>,
    program_id: Option<String>,
}

#[derive(Serialize)]
struct ResolvedLoadedAddresses {
    writable: Vec<Option<String>>,
    readonly: Vec<Option<String>>,
}

#[derive(Serialize)]
struct ResolvedReturnData {
    program_id: Option<String>,
    data: String,
}

fn resolve_block(block: &BlockWithIds, registry: &OrderedPubkeyRegistry) -> ResolvedBlock {
    let rewards = block
        .rewards
        .iter()
        .map(|r| ResolvedReward {
            pubkey: registry.get_pubkey(r.pubkey).map(|pk| pk.to_string()),
            lamports: r.lamports,
            post_balance: r.post_balance,
            reward_type: r.reward_type.clone(),
            commission: r.commission,
        })
        .collect();

    let transactions = block
        .transactions
        .iter()
        .map(|tx| resolve_transaction(tx, registry))
        .collect();

    ResolvedBlock {
        slot: block.slot,
        blockhash: block.blockhash.clone(),
        previous_blockhash: block.previous_blockhash.clone(),
        parent_slot: block.parent_slot,
        block_time: block.block_time,
        block_height: block.block_height,
        rewards,
        transactions,
        num_transactions: block.num_transactions,
    }
}

fn resolve_transaction(
    tx: &CompactTransaction,
    registry: &OrderedPubkeyRegistry,
) -> ResolvedTransaction {
    let account_keys: Vec<Option<String>> = tx
        .account_keys
        .iter()
        .map(|id| registry.get_pubkey(*id).map(|pk| pk.to_string()))
        .collect();

    let loaded_addresses = tx.meta.as_ref().and_then(|m| m.loaded_addresses.as_ref());

    let instructions = tx
        .instructions
        .iter()
        .map(|ix| resolve_instruction(ix, &account_keys, loaded_addresses, registry))
        .collect();

    let address_table_lookups = tx.address_table_lookups.as_ref().map(|lookups| {
        lookups
            .iter()
            .map(|l| ResolvedAddressTableLookup {
                account_key: registry.get_pubkey(l.account_key).map(|pk| pk.to_string()),
                writable_indexes: l.writable_indexes.clone(),
                readonly_indexes: l.readonly_indexes.clone(),
            })
            .collect()
    });

    let message_header = ResolvedMessageHeader {
        num_required_signatures: tx.message_header.num_required_signatures,
        num_readonly_signed_accounts: tx.message_header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: tx.message_header.num_readonly_unsigned_accounts,
    };

    let meta = tx
        .meta
        .as_ref()
        .map(|m| resolve_transaction_meta(m, &account_keys, registry));

    ResolvedTransaction {
        signatures: tx.signatures.iter().map(|s| s.to_string()).collect(),
        message_header,
        account_keys,
        recent_blockhash: tx.recent_blockhash.clone(),
        instructions,
        address_table_lookups,
        meta,
        version: tx.version,
    }
}

fn resolve_transaction_meta(
    meta: &CompactTransactionMeta,
    account_keys: &[Option<String>],
    registry: &OrderedPubkeyRegistry,
) -> ResolvedTransactionMeta {
    let loaded_addresses = meta
        .loaded_addresses
        .as_ref()
        .map(|l| resolve_loaded_addresses(l, registry));
    let loaded_addresses_ids = meta.loaded_addresses.as_ref();

    let inner_instructions = meta.inner_instructions.as_ref().map(|inner| {
        inner
            .iter()
            .map(|ii| ResolvedInnerInstructions {
                index: ii.index,
                instructions: ii
                    .instructions
                    .iter()
                    .map(|ix| resolve_instruction(ix, account_keys, loaded_addresses_ids, registry))
                    .collect(),
            })
            .collect()
    });

    let pre_token_balances = meta.pre_token_balances.as_ref().map(|balances| {
        balances
            .iter()
            .map(|tb| resolve_token_balance(tb, registry))
            .collect()
    });

    let post_token_balances = meta.post_token_balances.as_ref().map(|balances| {
        balances
            .iter()
            .map(|tb| resolve_token_balance(tb, registry))
            .collect()
    });

    let rewards = meta.rewards.as_ref().map(|rewards| {
        rewards
            .iter()
            .map(|r| ResolvedReward {
                pubkey: registry.get_pubkey(r.pubkey).map(|pk| pk.to_string()),
                lamports: r.lamports,
                post_balance: r.post_balance,
                reward_type: r.reward_type.clone(),
                commission: r.commission,
            })
            .collect()
    });

    let return_data = meta
        .return_data
        .as_ref()
        .map(|(program_id, data)| ResolvedReturnData {
            program_id: registry.get_pubkey(*program_id).map(|pk| pk.to_string()),
            data: STANDARD.encode(data),
        });

    ResolvedTransactionMeta {
        err: meta.err.clone(),
        fee: meta.fee,
        pre_balances: meta.pre_balances.clone(),
        post_balances: meta.post_balances.clone(),
        inner_instructions,
        log_messages: meta.log_messages.clone(),
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_addresses,
        return_data,
        compute_units_consumed: meta.compute_units_consumed,
    }
}

fn resolve_instruction(
    ix: &CompactInstruction,
    account_keys: &[Option<String>],
    loaded_addresses: Option<&CompactLoadedAddresses>,
    registry: &OrderedPubkeyRegistry,
) -> ResolvedInstruction {
    let accounts = ix
        .accounts
        .iter()
        .map(|idx| resolve_account_index(*idx, account_keys, loaded_addresses, registry))
        .collect();

    ResolvedInstruction {
        program_id: registry.get_pubkey(ix.program_id).map(|pk| pk.to_string()),
        accounts,
        data: STANDARD.encode(&ix.data),
        stack_height: ix.stack_height,
    }
}

fn resolve_account_index(
    idx: u8,
    account_keys: &[Option<String>],
    loaded_addresses: Option<&CompactLoadedAddresses>,
    registry: &OrderedPubkeyRegistry,
) -> Option<String> {
    let idx = idx as usize;
    if idx < account_keys.len() {
        return account_keys[idx].clone();
    }

    let Some(loaded) = loaded_addresses else {
        return None;
    };

    let mut offset = idx.saturating_sub(account_keys.len());
    if offset < loaded.writable.len() {
        return registry
            .get_pubkey(loaded.writable[offset])
            .map(|pk| pk.to_string());
    }

    offset -= loaded.writable.len();
    if offset < loaded.readonly.len() {
        return registry
            .get_pubkey(loaded.readonly[offset])
            .map(|pk| pk.to_string());
    }

    None
}

fn resolve_loaded_addresses(
    loaded: &CompactLoadedAddresses,
    registry: &OrderedPubkeyRegistry,
) -> ResolvedLoadedAddresses {
    ResolvedLoadedAddresses {
        writable: loaded
            .writable
            .iter()
            .map(|id| registry.get_pubkey(*id).map(|pk| pk.to_string()))
            .collect(),
        readonly: loaded
            .readonly
            .iter()
            .map(|id| registry.get_pubkey(*id).map(|pk| pk.to_string()))
            .collect(),
    }
}

fn resolve_token_balance(
    tb: &CompactTokenBalance,
    registry: &OrderedPubkeyRegistry,
) -> ResolvedTokenBalance {
    ResolvedTokenBalance {
        account_index: tb.account_index,
        mint: registry.get_pubkey(tb.mint).map(|pk| pk.to_string()),
        ui_token_amount: tb.ui_token_amount.clone(),
        owner: registry.get_pubkey(tb.owner).map(|pk| pk.to_string()),
        program_id: registry.get_pubkey(tb.program_id).map(|pk| pk.to_string()),
    }
}
