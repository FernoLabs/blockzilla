use ahash::AHashSet;
use anyhow::{Context, Result};
use blockzilla::{
    block_stream::{CarBlock, SolanaBlockStream},
    confirmed_block,
    node::{CborCid, Node},
};
use prost::Message;
use rusqlite::{Connection, params};
use solana_sdk::pubkey::Pubkey;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::{fs::File, time::Instant};
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::partial_tx_parser;

const WORKER_THREADS: usize = 4;
const CHANNEL_BUFFER: usize = 1000;
const MERGE_INTERVAL_BLOCKS: usize = 1000;

pub async fn extract_unique_pubkeys(path: &str, output_dir: Option<String>) -> Result<()> {
    let epoch =
        extract_epoch_from_path(path).context("Could not parse epoch number from filename")?;
    let out_dir = PathBuf::from(output_dir.unwrap_or_else(|| "optimized".into()));
    std::fs::create_dir_all(&out_dir)?;
    let db_path = out_dir.join(format!("pubkeys-{epoch:04}.sqlite"));

    println!(
        "🧮 Extracting pubkeys for epoch {epoch} → {} (using {} threads)",
        db_path.display(),
        WORKER_THREADS
    );

    // Shared global set (protected by mutex)
    let global_set: Arc<Mutex<AHashSet<Pubkey>>> =
        Arc::new(Mutex::new(AHashSet::with_capacity(10_000_000)));

    // Metrics
    let blocks_processed = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // Create channel for sending blocks to workers
    let (tx, rx) = crossbeam_channel::bounded::<CarBlock>(CHANNEL_BUFFER);
    let rx = Arc::new(rx);

    // Spawn worker threads
    let mut handles = vec![];
    for worker_id in 0..WORKER_THREADS {
        let rx = Arc::clone(&rx);
        let global_set = Arc::clone(&global_set);
        let blocks_processed = Arc::clone(&blocks_processed);

        let handle =
            thread::spawn(move || worker_thread(worker_id, rx, global_set, blocks_processed));
        handles.push(handle);
    }

    // Progress logging thread
    let blocks_processed_log = Arc::clone(&blocks_processed);
    let global_set_log = Arc::clone(&global_set);
    let log_handle = thread::spawn(move || {
        let mut last_count = 0u64;
        loop {
            thread::sleep(Duration::from_secs(10));
            let count = blocks_processed_log.load(Ordering::Relaxed);
            if count == last_count {
                break; // No progress, likely done
            }
            last_count = count;
            let elapsed = start.elapsed().as_secs_f64();
            let blk_per_s = (count as f64) / elapsed;
            let unique = global_set_log.lock().unwrap().len();
            println!(
                "🧮 {:>7} blk | {:>6.1} blk/s | {:>9} unique pubkeys",
                count, blk_per_s, unique,
            );
        }
    });

    // Stream CAR file and send blocks to workers
    let file = File::open(path).await?;
    let mut stream = SolanaBlockStream::new(file).await?;

    while let Some(block) = stream.next_solana_block().await? {
        // Send block to workers (blocks if channel is full)
        tx.send(block).context("Failed to send block to workers")?;
    }

    // Close channel to signal workers to finish
    drop(tx);

    // Wait for all workers to complete
    for handle in handles {
        handle.join().expect("Worker thread panicked");
    }

    // Stop logging thread
    log_handle.join().ok();

    let final_set = Arc::try_unwrap(global_set)
        .unwrap_or_else(|arc| (*arc.lock().unwrap()).clone().into())
        .into_inner()
        .unwrap();

    let total_blocks = blocks_processed.load(Ordering::Relaxed);
    println!(
        "✅ Processed {} blocks, found {} unique pubkeys",
        total_blocks,
        final_set.len()
    );

    println!("💾 Writing {} pubkeys to SQLite...", final_set.len());
    dump_pubkeys_to_sqlite(&db_path, &final_set)?;
    println!("✅ Done.");
    Ok(())
}

fn worker_thread(
    worker_id: usize,
    rx: Arc<crossbeam_channel::Receiver<CarBlock>>,
    global_set: Arc<Mutex<AHashSet<Pubkey>>>,
    blocks_processed: Arc<AtomicU64>,
) -> Result<()> {
    let mut local_set = AHashSet::with_capacity(100_000);
    let mut blocks_since_merge = 0;

    loop {
        match rx.recv() {
            Ok(block) => {
                // Extract pubkeys into local set
                extract_transactions(&block, &mut local_set)?;
                blocks_processed.fetch_add(1, Ordering::Relaxed);
                blocks_since_merge += 1;

                // Periodically merge local set into global set
                if blocks_since_merge >= MERGE_INTERVAL_BLOCKS {
                    merge_into_global(&mut local_set, &global_set);
                    blocks_since_merge = 0;
                }
            }
            Err(_) => {
                // Channel closed, do final merge and exit
                if !local_set.is_empty() {
                    merge_into_global(&mut local_set, &global_set);
                }
                break;
            }
        }
    }

    Ok(())
}

fn merge_into_global(local_set: &mut AHashSet<Pubkey>, global_set: &Arc<Mutex<AHashSet<Pubkey>>>) {
    if local_set.is_empty() {
        return;
    }

    let mut global = global_set.lock().unwrap();
    global.extend(local_set.drain());
}

fn extract_rewards(cb: &CarBlock, out: &mut AHashSet<Pubkey>) -> Result<()> {
    if let Some(ref reward_cid) = cb.block.rewards {
        if let Some(Node::DataFrame(df)) = cb.entries.get(&reward_cid.0) {
            let bytes = cb.merge_dataframe(df)?;
            if let Ok(rewards) = bincode::decode_from_slice::<
                Vec<bincode::serde::Compat<solana_transaction_status_client_types::Reward>>,
                _,
            >(&bytes, bincode::config::legacy())
            {
                for bincode::serde::Compat(rw) in rewards.0 {
                    out.insert(Pubkey::from_str_const(&rw.pubkey));
                }
            }
        }
    }
    Ok(())
}

fn extract_transactions(cb: &CarBlock, out: &mut AHashSet<Pubkey>) -> Result<()> {
    for e_cid in &cb.entries {
        let Some(Node::Entry(entry)) = cb.entries.get(&e_cid.0) else {
            continue;
        };
        for tx_cid in &entry.transactions {
            let Some(Node::Transaction(tx)) = cb.entries.get(&tx_cid.0) else {
                continue;
            };

            // Transaction data
            let tx_bytes = if tx.data.next.is_none() {
                &tx.data.data
            } else {
                &cb.merge_dataframe(&tx.data)?
            };

            partial_tx_parser::parse_bincode_tx_static_accounts(tx_bytes, out)?;

            // Metadata (commented out in original)
            let meta_bytes = cb.merge_dataframe(&tx.metadata)?;
            if let Ok(meta) = decode_protobuf_meta(&meta_bytes) {
                extract_pubkeys_from_meta(&meta, out);
            }
        }
    }
    Ok(())
}

pub fn same_accounts(vec_accounts: &[Pubkey], set_accounts: &AHashSet<Pubkey>) -> bool {
    if vec_accounts.len() != set_accounts.len() {
        eprintln!(
            "❌ Different size: vec={} set={}",
            vec_accounts.len(),
            set_accounts.len()
        );
    }

    let vec_set: AHashSet<_> = vec_accounts.iter().copied().collect();

    if vec_set == *set_accounts {
        return true;
    }

    // Compute diffs
    for pk in vec_set.difference(set_accounts) {
        eprintln!("⚠️ Missing in set: {}", pk);
    }
    for pk in set_accounts.difference(&vec_set) {
        eprintln!("⚠️ Extra in set: {}", pk);
    }

    false
}

fn decode_protobuf_meta(bytes: &[u8]) -> Result<confirmed_block::TransactionStatusMeta> {
    let decompressed = match zstd::bulk::decompress(bytes, 512 * 1024) {
        Ok(buf) => buf,
        Err(_) => {
            let mut tmp = Vec::new();
            if let Ok(mut dec) = ZstdDecoder::new(bytes) {
                std::io::copy(&mut dec, &mut tmp).ok();
            } else {
                tmp.extend_from_slice(bytes);
            }
            tmp
        }
    };
    Ok(confirmed_block::TransactionStatusMeta::decode(
        &decompressed[..],
    )?)
}

fn extract_pubkeys_from_meta(
    meta: &confirmed_block::TransactionStatusMeta,
    out: &mut AHashSet<Pubkey>,
) {
    // Pre/post token balances
    for tb in &meta.pre_token_balances {
        out.insert(Pubkey::from_str_const(&tb.mint));
        out.insert(Pubkey::from_str_const(&tb.owner));
        out.insert(Pubkey::from_str_const(&tb.program_id));
    }

    for tb in &meta.post_token_balances {
        out.insert(Pubkey::from_str_const(&tb.mint));
        out.insert(Pubkey::from_str_const(&tb.owner));
        out.insert(Pubkey::from_str_const(&tb.program_id));
    }

    // Loaded addresses
    for addr_bytes in meta
        .loaded_writable_addresses
        .iter()
        .chain(meta.loaded_readonly_addresses.iter())
    {
        if let Ok(pk) = Pubkey::try_from(addr_bytes.as_slice()) {
            out.insert(pk);
        }
    }

    // Return data program id
    if let Some(rd) = &meta.return_data {
        if let Ok(pk) = Pubkey::try_from(rd.program_id.as_slice()) {
            out.insert(pk);
        }
    }
}

fn dump_pubkeys_to_sqlite(db_path: &PathBuf, set: &AHashSet<Pubkey>) -> Result<()> {
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "PRAGMA synchronous = OFF;
         PRAGMA journal_mode = WAL;
         CREATE TABLE IF NOT EXISTS pubkeys (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            pubkey BLOB UNIQUE
         );",
    )?;

    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare("INSERT OR IGNORE INTO pubkeys (pubkey) VALUES (?1)")?;
        for pk in set {
            let bytes = pk.to_bytes();
            stmt.execute(params![&bytes[..]])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn extract_epoch_from_path(path: &str) -> Option<u64> {
    Path::new(path)
        .file_stem()?
        .to_string_lossy()
        .split('-')
        .find(|s| s.chars().all(|c| c.is_ascii_digit()))
        .and_then(|num| num.parse::<u64>().ok())
}
