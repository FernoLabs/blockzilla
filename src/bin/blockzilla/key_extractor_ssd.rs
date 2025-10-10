use anyhow::{Context, Result};
use blockzilla::{
    block_stream::{CarBlock, SolanaBlockStream},
    node::Node,
};
use prost::Message;
use rusqlite::{params, Connection};
use solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction};
use solana_storage_proto::convert::generated;
use std::{
    cell::RefCell,
    collections::HashSet,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use zstd::stream::read::Decoder as ZstdDecoder;
use tokio::fs::File;

thread_local! {
    static TL_META_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(128 * 1024));
}

const LOG_INTERVAL: Duration = Duration::from_secs(10);

pub async fn extract_unique_pubkeys_profiled(path: &str, output_dir: Option<String>, bench: bool) -> Result<()> {
    let epoch = extract_epoch_from_path(path)
        .context("Could not parse epoch number from input filename")?;
    let out_dir = PathBuf::from(output_dir.unwrap_or_else(|| "optimized".into()));
    std::fs::create_dir_all(&out_dir)?;
    let db_path = out_dir.join(format!("pubkeys-{epoch:04}.sqlite"));

    println!("🧮 Deep-profiled pubkey extraction for epoch {epoch} → {}", db_path.display());

    // SQLite
    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "PRAGMA synchronous = OFF;
         PRAGMA journal_mode = WAL;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = 800000;
         CREATE TABLE IF NOT EXISTS pubkeys (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            pubkey BLOB UNIQUE
         );",
    )?;

    // Stream init
    let file = File::open(path).await?;
    let mut stream = SolanaBlockStream::new(file).await?;

    let mut set: HashSet<Pubkey> = HashSet::with_capacity(10_000_000);

    // Metrics
    let mut blocks_processed = 0u64;
    let mut last_log = Instant::now();
    let mut last_block = Instant::now();
    let start = Instant::now();

    let mut t_zstd = 0.0;
    let mut t_prost = 0.0;
    let mut t_rewards = 0.0;
    let mut t_tx = 0.0;
    let mut t_meta = 0.0;
    let mut t_gap = 0.0;
    let mut count_rewards = 0u64;
    let mut count_tx = 0u64;
    let mut count_keys = 0u64;

    while let Some(block) = stream.next_solana_block().await? {
        let now = Instant::now();
        t_gap += now.duration_since(last_block).as_secs_f64();
        last_block = now;
        let t_block = Instant::now();

        // ---- rewards ----
        let t0 = Instant::now();
        count_rewards += extract_rewards(&block, &mut set)?;
        t_rewards += t0.elapsed().as_secs_f64();

        // ---- tx & meta ----
        let t1 = Instant::now();
        let (tx_time, meta_time, tx_count, key_count, zstd_t, prost_t) =
            extract_transactions_profiled(&block, &mut set)?;
        t_tx += tx_time;
        t_meta += meta_time;
        t_zstd += zstd_t;
        t_prost += prost_t;
        count_tx += tx_count as u64;
        count_keys += key_count as u64;

        let total_block = t_block.elapsed().as_secs_f64();
        blocks_processed += 1;

        // ---- periodic logging ----
        if last_log.elapsed() >= LOG_INTERVAL {
            let elapsed = start.elapsed().as_secs_f64();
            let blk_per_s = (blocks_processed as f64) / elapsed;
            let key_count = set.len();

            println!(
                "🧮 {:>7} blk | {:>6.1} blk/s | {:>9} unique | avg {:.3}s (gap {:.3}s) \
                 [tx {:.3}s | meta {:.3}s | rewards {:.3}s | zstd {:.3}s | prost {:.3}s] \
                 keys+{} tx+{} rw+{}",
                blocks_processed,
                blk_per_s,
                key_count,
                total_block / blocks_processed as f64,
                t_gap / blocks_processed as f64,
                t_tx / blocks_processed as f64,
                t_meta / blocks_processed as f64,
                t_rewards / blocks_processed as f64,
                t_zstd / blocks_processed as f64,
                t_prost / blocks_processed as f64,
                count_keys,
                count_tx,
                count_rewards
            );

            last_log = Instant::now();
        }
    }

    println!("💾 Dumping {} pubkeys to SQLite...", set.len());
    dump_pubkeys_to_sqlite(&conn, &set)?;

    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "✅ Done: {blocks_processed} blocks | {:.1} blk/s | {:.1}s total",
        (blocks_processed as f64) / elapsed,
        elapsed
    );
    Ok(())
}

// ---- Rewards ---------------------------------------------------------------
fn extract_rewards(cb: &CarBlock, out: &mut HashSet<Pubkey>) -> Result<u64> {
    let mut count = 0u64;
    if let Some(reward_cid) = cb.block.rewards {
        if let Some(Node::DataFrame(df)) = cb.entries.get(&reward_cid) {
            let bytes = cb.merge_dataframe(df)?;
            if let Ok(rewards) =
                bincode::deserialize::<Vec<solana_transaction_status_client_types::Reward>>(&bytes)
            {
                for rw in rewards {
                    if let Ok(pk) = rw.pubkey.parse::<Pubkey>() {
                        out.insert(pk);
                        count += 1;
                    }
                }
            }
        }
    }
    Ok(count)
}

// ---- Transactions + metadata ----------------------------------------------
fn extract_transactions_profiled(
    cb: &CarBlock,
    out: &mut HashSet<Pubkey>,
) -> Result<(f64, f64, usize, usize, f64, f64)> {
    let mut t_tx = 0.0;
    let mut t_meta = 0.0;
    let mut t_zstd = 0.0;
    let mut t_prost = 0.0;
    let mut tx_count = 0usize;
    let mut keys_added = 0usize;

    for e_cid in &cb.block.entries {
        let Some(Node::Entry(entry)) = cb.entries.get(e_cid) else { continue; };
        for tx_cid in &entry.transactions {
            tx_count += 1;
            let Some(Node::Transaction(tx)) = cb.entries.get(tx_cid) else { continue; };

            // ---- tx data ----
            let t0 = Instant::now();
            let tx_bytes = cb.merge_dataframe(&tx.data)?;
            if let Ok(vt) = bincode::deserialize::<VersionedTransaction>(&tx_bytes) {
                let msg = vt.message;
                for pk in msg.static_account_keys() {
                    if out.insert(*pk) {
                        keys_added += 1;
                    }
                }
                if let Some(lookups) = msg.address_table_lookups() {
                    for lookup in lookups {
                        if out.insert(lookup.account_key) {
                            keys_added += 1;
                        }
                    }
                }
            }
            t_tx += t0.elapsed().as_secs_f64();

            // ---- meta ----
            let t1 = Instant::now();
            let meta_bytes = cb.merge_dataframe(&tx.metadata)?;
            let (meta_proto, zstd_t, prost_t) = decode_protobuf_meta_timed(&meta_bytes)?;
            t_zstd += zstd_t;
            t_prost += prost_t;
            extract_pubkeys_from_meta(&meta_proto, out, &mut keys_added);
            t_meta += t1.elapsed().as_secs_f64();
        }
    }

    Ok((t_tx, t_meta, tx_count, keys_added, t_zstd, t_prost))
}

// ---- Decode protobuf -------------------------------------------------------
fn decode_protobuf_meta_timed(
    bytes: &[u8],
) -> Result<(generated::TransactionStatusMeta, f64, f64)> {
    let mut zstd_time = 0.0;
    let mut prost_time = 0.0;

    let t0 = Instant::now();
    let decompressed = match zstd::bulk::decompress(bytes, 512 * 1024) {
        Ok(buf) => buf,
        Err(_) => {
            let mut out = Vec::new();
            TL_META_BUF.with(|cell| {
                let mut tmp = cell.borrow_mut();
                tmp.clear();
                if let Ok(mut dec) = ZstdDecoder::new(bytes) {
                    std::io::copy(&mut dec, &mut *tmp).ok();
                } else {
                    tmp.extend_from_slice(bytes);
                }
                out = tmp.clone();
            });
            out
        }
    };
    zstd_time += t0.elapsed().as_secs_f64();

    let t1 = Instant::now();
    let meta = generated::TransactionStatusMeta::decode(&decompressed[..])
        .context("prost decode failed")?;
    prost_time += t1.elapsed().as_secs_f64();

    Ok((meta, zstd_time, prost_time))
}

// ---- Extract pubkeys from metadata -----------------------------------------
fn extract_pubkeys_from_meta(meta: &generated::TransactionStatusMeta, out: &mut HashSet<Pubkey>, keys: &mut usize) {
    let mut add = |s: &str| {
        if let Ok(pk) = s.parse::<Pubkey>() {
            if out.insert(pk) {
                *keys += 1;
            }
        }
    };

    for tb in &meta.pre_token_balances {
        add(&tb.mint);
        add(&tb.owner);
        add(&tb.program_id);
    }
    for tb in &meta.post_token_balances {
        add(&tb.mint);
        add(&tb.owner);
        add(&tb.program_id);
    }

    for addr_bytes in meta
        .loaded_writable_addresses
        .iter()
        .chain(meta.loaded_readonly_addresses.iter())
    {
        if addr_bytes.len() == 32 {
            if let Ok(pk) = Pubkey::try_from(addr_bytes.as_slice()) {
                if out.insert(pk) {
                    *keys += 1;
                }
            }
        }
    }

    if let Some(rd) = &meta.return_data {
        if rd.program_id.len() == 32 {
            if let Ok(pk) = Pubkey::try_from(rd.program_id.as_slice()) {
                if out.insert(pk) {
                    *keys += 1;
                }
            }
        }
    }
}

// ---- SQLite dump -----------------------------------------------------------
fn dump_pubkeys_to_sqlite(conn: &Connection, set: &HashSet<Pubkey>) -> Result<()> {
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

// ---- Utility ---------------------------------------------------------------
fn extract_epoch_from_path(path: &str) -> Option<u64> {
    Path::new(path)
        .file_stem()?
        .to_string_lossy()
        .split('-')
        .find(|s| s.chars().all(|c| c.is_ascii_digit()))
        .and_then(|num| num.parse::<u64>().ok())
}
