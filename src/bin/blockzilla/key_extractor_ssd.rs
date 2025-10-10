use ahash::AHashSet;
use anyhow::{Context, Result};
use blockzilla::{
    block_stream::{CarBlock, SolanaBlockStream}, confirmed_block, node::Node
};
use prost::Message;
use rusqlite::{Connection, params};
use solana_sdk::pubkey::Pubkey;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{fs::File, time::Instant};
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::partial_tx_parser;

pub async fn extract_unique_pubkeys(path: &str, output_dir: Option<String>) -> Result<()> {
    let epoch =
        extract_epoch_from_path(path).context("Could not parse epoch number from filename")?;
    let out_dir = PathBuf::from(output_dir.unwrap_or_else(|| "optimized".into()));
    std::fs::create_dir_all(&out_dir)?;
    let db_path = out_dir.join(format!("pubkeys-{epoch:04}.sqlite"));

    println!(
        "🧮 Extracting pubkeys for epoch {epoch} → {}",
        db_path.display()
    );

    // Stream CAR file
    let file = File::open(path).await?;
    let mut stream = SolanaBlockStream::new(file).await?;
    let mut set = AHashSet::with_capacity(10_000_000);

    // Metrics
    let start = Instant::now();
    let mut last_log = Instant::now();
    let mut blocks_processed = 0u64;

    while let Some(block) = stream.next_solana_block().await? {
        //extract_rewards(&block, &mut set)?;
        extract_transactions(&block, &mut set)?;
        blocks_processed += 1;

        if last_log.elapsed() >= Duration::from_secs(10) {
            let elapsed = start.elapsed().as_secs_f64();
            let blk_per_s = (blocks_processed as f64) / elapsed;
            println!(
                "🧮 {:>7} blk | {:>6.1} blk/s | {:>9} unique pubkeys",
                blocks_processed,
                blk_per_s,
                set.len(),
            );
            last_log = Instant::now();
        }
    }

    println!("💾 Writing {} pubkeys to SQLite...", set.len());

    dump_pubkeys_to_sqlite(&db_path, &set)?;
    println!("✅ Done.");
    Ok(())
}

fn extract_rewards(cb: &CarBlock, out: &mut AHashSet<Pubkey>) -> Result<()> {
    if let Some(reward_cid) = cb.block.rewards {
        if let Some(Node::DataFrame(df)) = cb.entries.get(&reward_cid) {
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
    for e_cid in &cb.block.entries {
        let Some(Node::Entry(entry)) = cb.entries.get(e_cid) else {
            continue;
        };
        for tx_cid in &entry.transactions {
            let Some(Node::Transaction(tx)) = cb.entries.get(tx_cid) else {
                continue;
            };

            // Transaction data
            let tx_bytes = if tx.data.next.is_none() {
                &tx.data.data
            } else {
                &cb.merge_dataframe(&tx.data)?
            };

            partial_tx_parser::parse_bincode_tx_static_accounts(tx_bytes, out)?;

            // Metadata
            //let meta_bytes = cb.merge_dataframe(&tx.metadata)?;
            //if let Ok(meta) = decode_protobuf_meta(&meta_bytes) {
            //    extract_pubkeys_from_meta(&meta, out);
            //}
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

    // Compute diffs (cheap because sets are small)
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
    Ok(confirmed_block::TransactionStatusMeta::decode(&decompressed[..])?)
}

fn extract_pubkeys_from_meta(meta: &confirmed_block::TransactionStatusMeta, out: &mut AHashSet<Pubkey>) {
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
