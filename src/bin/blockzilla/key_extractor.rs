use bincode::serde::Compat;
use rusqlite::{Connection, params};
use std::{
    cell::RefCell,
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};
use tokio::fs::File;

use anyhow::{Context, Result};
use blockzilla::{block_stream::SolanaBlockStream, confirmed_block};
use blockzilla::node::Node;
use crossbeam_channel::{Receiver, Sender, bounded};
use prost::Message;
use solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction};
use zstd::stream::read::Decoder as ZstdDecoder;

const NUM_WORKERS: usize = 8;
const CHANNEL_SIZE_BLOCKS: usize = 16;
const FLUSH_THRESHOLD: usize = 2_000_000;
const LOG_INTERVAL: Duration = Duration::from_secs(10);

thread_local! {
    static TL_META_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(128 * 1024));
}

struct KeyBatch {
    keys: Vec<Pubkey>,
    counts: Vec<u32>,
    slot: u64,
}

/// HDD-safe parallel registry builder
///  - Reads one epoch’s CAR sequentially
///  - Extracts pubkeys in parallel
///  - Writes per-epoch registry like `registry-0839.sqlite`
pub async fn build_registry_hdd_parallel(path: &str, output_dir: Option<String>) -> Result<()> {
    let epoch = extract_epoch_from_path(path)
        .context("Could not parse epoch number from input filename")?;

    let out_dir = PathBuf::from(output_dir.unwrap_or_else(|| "optimized".into()));
    std::fs::create_dir_all(&out_dir)?;

    // 🆕 Derive registry name from epoch number
    let reg_path = out_dir.join(format!("registry-{epoch:04}.sqlite"));
    println!(
        "🔑 Building registry for epoch {epoch} → {}",
        reg_path.display()
    );

    // SQLite init
    let conn = Connection::open(&reg_path)?;
    conn.execute_batch(
        "PRAGMA page_size = 32768;
         PRAGMA synchronous = OFF;
         PRAGMA journal_mode = WAL;
         PRAGMA wal_autocheckpoint = 0;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = 200000;
         PRAGMA locking_mode = EXCLUSIVE;

         CREATE TABLE IF NOT EXISTS keymap (
            pubkey   BLOB PRIMARY KEY,
            views    INTEGER NOT NULL DEFAULT 0,
            min_slot INTEGER,
            max_slot INTEGER
         ) WITHOUT ROWID;",
    )?;

    // Channels
    let (block_tx, block_rx) = bounded::<blockzilla::block_stream::CarBlock>(CHANNEL_SIZE_BLOCKS);
    let (keys_tx, keys_rx) = bounded::<KeyBatch>(CHANNEL_SIZE_BLOCKS * 2);

    // Reader thread
    let path_owned = path.to_string();
    let reader_handle = thread::spawn(move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move { reader_thread(path_owned, block_tx).await })
    });

    // Worker threads
    let mut worker_handles = Vec::with_capacity(NUM_WORKERS);
    for worker_id in 0..NUM_WORKERS {
        let rx = block_rx.clone();
        let tx = keys_tx.clone();
        worker_handles.push(thread::spawn(move || worker_thread(worker_id, rx, tx)));
    }
    drop(block_rx);
    drop(keys_tx);

    // Writer loop
    let start = Instant::now();
    let mut counts: HashMap<Pubkey, (u64, u64, u64)> = HashMap::with_capacity(FLUSH_THRESHOLD);
    // key → (views, min_slot, max_slot)
    let mut blocks_processed = 0u64;
    let mut flush_count = 0u64;
    let mut last_log = Instant::now();

    while let Ok(batch) = keys_rx.recv() {
        merge_batch(&mut counts, &batch);
        blocks_processed += 1;

        if counts.len() >= FLUSH_THRESHOLD {
            while let Ok(b) = keys_rx.try_recv() {
                merge_batch(&mut counts, &b);
                blocks_processed += 1;
            }
            flush_count += 1;
            let before = Instant::now();
            let batch_keys = counts.len();
            println!(
                "\n💾 Flush #{flush_count} with {batch_keys} keys (blk={blocks_processed})..."
            );
            flush_to_sqlite(&conn, &mut counts)?;
            let dur = before.elapsed().as_secs_f64();
            println!(
                "✅ Flush #{flush_count} done in {:.2}s → {:.1} kkeys/s",
                dur,
                (batch_keys as f64 / 1000.0) / dur
            );
        }

        if last_log.elapsed() >= LOG_INTERVAL {
            let elapsed = start.elapsed().as_secs_f64();
            let blk_per_s = blocks_processed as f64 / elapsed;
            println!(
                "🧮 {:>9} blk | {:>7.1} blk/s | {:>9} keys in RAM | {:>3} flushes",
                blocks_processed,
                blk_per_s,
                counts.len(),
                flush_count
            );
            last_log = Instant::now();
        }
    }

    // Final drain
    while let Ok(b) = keys_rx.try_recv() {
        merge_batch(&mut counts, &b);
        blocks_processed += 1;
    }

    if !counts.is_empty() {
        flush_count += 1;
        let before = Instant::now();
        let batch_keys = counts.len();
        println!(
            "\n💾 Final flush #{flush_count} with {batch_keys} keys (blk={blocks_processed})..."
        );
        flush_to_sqlite(&conn, &mut counts)?;
        let dur = before.elapsed().as_secs_f64();
        println!(
            "✅ Final flush done in {:.2}s → {:.1} kkeys/s",
            dur,
            (batch_keys as f64 / 1000.0) / dur
        );
    }

    reader_handle.join().unwrap()?;
    for h in worker_handles {
        h.join().unwrap()?;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let blk_per_s = blocks_processed as f64 / elapsed;
    println!(
        "\n✅ Done: {blocks_processed} blocks | {flush_count} flushes | {:.1} blk/s | {:.1}s total",
        blk_per_s, elapsed
    );
    println!("📘 Registry saved → {}", reg_path.display());
    Ok(())
}

async fn reader_thread(path: String, tx: Sender<blockzilla::block_stream::CarBlock>) -> Result<()> {
    let file = File::open(path).await?;
    let mut stream = SolanaBlockStream::new(file).await?;
    while let Some(block) = stream.next_solana_block().await? {
        tx.send(block)?;
    }
    Ok(())
}

fn worker_thread(
    worker_id: usize,
    rx: Receiver<blockzilla::block_stream::CarBlock>,
    tx: Sender<KeyBatch>,
) -> Result<()> {
    use std::collections::HashMap;
    let mut keys_buf = Vec::with_capacity(8_000);
    let mut per_block: HashMap<Pubkey, u32> = HashMap::with_capacity(4_000);

    while let Ok(block) = rx.recv() {
        keys_buf.clear();
        extract_pubkeys_from_block(&block, &mut keys_buf)?;
        per_block.clear();
        for k in &keys_buf {
            *per_block.entry(*k).or_insert(0) += 1;
        }
        if !per_block.is_empty() {
            let mut keys = Vec::with_capacity(per_block.len());
            let mut counts = Vec::with_capacity(per_block.len());
            for (k, c) in per_block.iter() {
                keys.push(*k);
                counts.push(*c);
            }
            let slot = block.block.slot;
            tx.send(KeyBatch { keys, counts, slot })?;
        }
    }

    let _ = worker_id;
    Ok(())
}

#[inline]
fn merge_batch(acc: &mut HashMap<Pubkey, (u64, u64, u64)>, batch: &KeyBatch) {
    for (k, c) in batch.keys.iter().zip(batch.counts.iter()) {
        let entry = acc.entry(*k).or_insert((*c as u64, batch.slot, batch.slot));
        entry.0 += *c as u64;
        entry.2 = batch.slot; // update max_slot
        if batch.slot < entry.1 {
            entry.1 = batch.slot;
        }
    }
}

fn flush_to_sqlite(conn: &Connection, counts: &mut HashMap<Pubkey, (u64, u64, u64)>) -> Result<()> {
    if counts.is_empty() {
        return Ok(());
    }

    let tx = conn.unchecked_transaction()?;

    tx.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS staging_keys (
            pubkey   BLOB NOT NULL,
            views    INTEGER NOT NULL,
            min_slot INTEGER NOT NULL,
            max_slot INTEGER NOT NULL
         );",
    )?;
    tx.execute("DELETE FROM staging_keys;", [])?;

    {
        let mut ins = tx.prepare(
            "INSERT INTO staging_keys (pubkey, views, min_slot, max_slot) VALUES (?1, ?2, ?3, ?4)",
        )?;
        for (pk, (views, min_slot, max_slot)) in counts.drain() {
            let bytes = pk.to_bytes();
            ins.execute(params![
                &bytes[..],
                views as i64,
                min_slot as i64,
                max_slot as i64
            ])?;
        }
    }

    tx.execute_batch(
        "INSERT INTO keymap (pubkey, views, min_slot, max_slot)
         SELECT pubkey, SUM(views), MIN(min_slot), MAX(max_slot)
         FROM staging_keys
         GROUP BY pubkey
         ON CONFLICT(pubkey) DO UPDATE SET
            views    = keymap.views + excluded.views,
            max_slot = excluded.max_slot;",
    )?;

    tx.execute("DELETE FROM staging_keys;", [])?;
    tx.commit()?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    Ok(())
}

fn extract_pubkeys_from_block(
    cb: &blockzilla::block_stream::CarBlock,
    keys: &mut Vec<Pubkey>,
) -> Result<()> {
    if let Some(reward_cid) = cb.block.rewards
        && let Some(Node::DataFrame(df)) = cb.entries.get(&reward_cid)
    {
        let bytes = cb.merge_dataframe(df)?;
        if let Ok((rewards, _)) = bincode::decode_from_slice::<
            Vec<bincode::serde::Compat<solana_transaction_status_client_types::Reward>>,
            _,
        >(&bytes, bincode::config::legacy())
        {
            for bincode::serde::Compat(rw) in rewards {
                if let Ok(pk) = rw.pubkey.parse::<Pubkey>() {
                    keys.push(pk);
                }
            }
        }
    }

    for e_cid in &cb.block.entries {
        let Some(Node::Entry(entry)) = cb.entries.get(e_cid) else {
            continue;
        };
        for tx_cid in &entry.transactions {
            if let Some(Node::Transaction(tx)) = cb.entries.get(tx_cid) {
                let tx_bytes = cb.merge_dataframe(&tx.data)?;
                if let Ok((Compat(vt), _)) = bincode::decode_from_slice::<
                    Compat<VersionedTransaction>,
                    _,
                >(&tx_bytes, bincode::config::legacy())
                {
                    let msg = vt.message;
                    keys.extend_from_slice(msg.static_account_keys());
                    if let Some(lookups) = msg.address_table_lookups() {
                        for lookup in lookups {
                            keys.push(lookup.account_key);
                        }
                    }
                }

                //let meta_bytes = cb.merge_dataframe(&tx.metadata)?;
                //if let Ok(meta_proto) = decode_protobuf_meta(&meta_bytes) {
                //    extract_pubkeys_from_meta(&meta_proto, keys);
                //}
            }
        }
    }
    Ok(())
}

fn decode_protobuf_meta(bytes: &[u8]) -> Result<confirmed_block::TransactionStatusMeta> {
    match zstd::bulk::decompress(bytes, 512 * 1024) {
        Ok(decompressed) => confirmed_block::TransactionStatusMeta::decode(&decompressed[..])
            .context("prost decode failed"),
        Err(_) => TL_META_BUF.with(|cell| {
            let mut buf = cell.borrow_mut();
            buf.clear();
            match ZstdDecoder::new(bytes) {
                Ok(mut decoder) => {
                    decoder.read_to_end(&mut buf)?;
                    confirmed_block::TransactionStatusMeta::decode(&buf[..])
                        .context("prost decode failed")
                }
                Err(_) => {
                    confirmed_block::TransactionStatusMeta::decode(bytes).context("raw decode failed")
                }
            }
        }),
    }
}

fn extract_pubkeys_from_meta(meta: &confirmed_block::TransactionStatusMeta, keys: &mut Vec<Pubkey>) {
    for tb in &meta.pre_token_balances {
        if let Ok(pk) = tb.mint.parse::<Pubkey>() {
            keys.push(pk);
        }
        if let Ok(pk) = tb.owner.parse::<Pubkey>() {
            keys.push(pk);
        }
        if let Ok(pk) = tb.program_id.parse::<Pubkey>() {
            keys.push(pk);
        }
    }

    for tb in &meta.post_token_balances {
        if let Ok(pk) = tb.mint.parse::<Pubkey>() {
            keys.push(pk);
        }
        if let Ok(pk) = tb.owner.parse::<Pubkey>() {
            keys.push(pk);
        }
        if let Ok(pk) = tb.program_id.parse::<Pubkey>() {
            keys.push(pk);
        }
    }

    for addr_bytes in meta
        .loaded_writable_addresses
        .iter()
        .chain(meta.loaded_readonly_addresses.iter())
    {
        if addr_bytes.len() == 32
            && let Ok(pk) = Pubkey::try_from(addr_bytes.as_slice())
        {
            keys.push(pk);
        }
    }

    if let Some(rd) = &meta.return_data
        && rd.program_id.len() == 32
        && let Ok(pk) = Pubkey::try_from(rd.program_id.as_slice())
    {
        keys.push(pk);
    }
}

fn extract_epoch_from_path(path: &str) -> Option<u64> {
    Path::new(path)
        .file_stem()?
        .to_string_lossy()
        .split('-')
        .find(|s| s.chars().all(|c| c.is_ascii_digit()))
        .and_then(|num| num.parse::<u64>().ok())
}
