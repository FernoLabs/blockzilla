use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result, anyhow};
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
};
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use sled::Db;
use solana_pubkey::Pubkey;
use std::{
    io::Read, path::{Path, PathBuf}, str::FromStr, time::{Duration, Instant}
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
};

use crate::{
    LOG_INTERVAL_SECS,
    file_downloader::download_epoch,
    transaction_parser::parse_account_keys_only_fast,
};

const MAX_RETRIES: usize = 3;

// ============================================================================
// KeyStats
// ============================================================================
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[repr(C)]
pub struct KeyStats {
    pub id: u32,
    pub count: u64,
    pub first_fee_payer: u32,
    pub first_slot: u64,
    pub epoch: u64,
}

// ============================================================================
// Helpers
// ============================================================================
fn has_local_epoch(cache_dir: &str, epoch: u64) -> bool {
    Path::new(&format!("{cache_dir}/epoch-{epoch}.car")).exists()
}

async fn detect_last_saved_epoch(results_dir: &Path) -> Result<Option<(u64, PathBuf)>> {
    let mut last: Option<(u64, PathBuf)> = None;
    let mut entries = match fs::read_dir(results_dir).await {
        Ok(e) => e,
        Err(_) => return Ok(None),
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(num_str) = name
            .strip_prefix("registry-")
            .and_then(|s| s.strip_suffix(".bin"))
        {
            if let Ok(num) = num_str.parse::<u64>() {
                if last.as_ref().map(|(n, _)| num > *n).unwrap_or(true) {
                    last = Some((num, entry.path()));
                }
            }
        }
    }
    Ok(last)
}

pub async fn write_registry_to_disk<P: AsRef<Path>>(
    path: P,
    registry: &AHashMap<Pubkey, KeyStats>,
) -> Result<()> {
    let path = path.as_ref();
    let tmp_path = path.with_extension("tmp");
    let data = postcard::to_allocvec(registry)?;
    let file = fs::File::create(&tmp_path).await?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&data).await?;
    writer.flush().await?;
    fs::rename(&tmp_path, path).await?;
    tracing::info!(
        "💾 wrote registry: {} entries → {}",
        registry.len(),
        path.display()
    );
    Ok(())
}

pub async fn read_registry_from_disk<P: AsRef<Path>>(
    path: P,
) -> Result<AHashMap<Pubkey, KeyStats>> {
    let file = fs::File::open(path.as_ref()).await?;
    let mut reader = BufReader::new(file);
    let mut data = Vec::new();
    reader.read_to_end(&mut data).await?;
    let registry: AHashMap<Pubkey, KeyStats> = postcard::from_bytes(&data)?;
    tracing::info!("📖 loaded registry: {} entries", registry.len());
    Ok(registry)
}

// ============================================================================
// Download helper
// ============================================================================
async fn download_epoch_to_disk(cache_dir: &str, epoch: u64) -> Result<PathBuf> {
    tokio::fs::create_dir_all(cache_dir).await?;
    let out_path = PathBuf::from(format!("{cache_dir}/epoch-{epoch}.car"));
    if out_path.exists() {
        tracing::info!("✅ epoch-{epoch}.car already exists");
        return Ok(out_path);
    }

    download_epoch(epoch, &cache_dir, MAX_RETRIES).await?;
    Ok(out_path)
}

// ============================================================================
// Sled integration
// ============================================================================
pub fn open_registry_db<P: AsRef<Path>>(path: P) -> Result<Db> {
    Ok(sled::open(path).context("Failed to open sled registry DB")?)
}

pub fn merge_epoch_into_sled_postcard(
    epoch_registry: &AHashMap<Pubkey, KeyStats>,
    db: &Db,
) -> Result<()> {
    let mut updated = 0usize;
    let mut inserted = 0usize;

    for (pubkey, stats) in epoch_registry.iter() {
        let key_bytes = pubkey.to_bytes();

        if let Some(existing) = db.get(&key_bytes)? {
            let mut old: KeyStats = postcard::from_bytes(&existing)
                .map_err(|_| anyhow!("Failed to deserialize KeyStats"))?;

            old.count += stats.count;
            if stats.first_slot < old.first_slot {
                old.first_slot = stats.first_slot;
                old.first_fee_payer = stats.first_fee_payer;
            }
            if stats.epoch > old.epoch {
                old.epoch = stats.epoch;
            }

            let bytes = postcard::to_allocvec(&old)?;
            db.insert(key_bytes, bytes)?;
            updated += 1;
        } else {
            let bytes = postcard::to_allocvec(stats)?;
            db.insert(key_bytes, bytes)?;
            inserted += 1;
        }
    }

    db.flush()?;
    tracing::info!(
        "🧩 Merged epoch into sled: {} updated, {} added (total ~{})",
        updated,
        inserted,
        db.len()
    );
    Ok(())
}

// ============================================================================
// Unified auto mode: offline + auto-download + sled merge
// ============================================================================
pub async fn build_registry_auto(cache_dir: &str, results_dir: &str, max_epoch: u64) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();
    let results_dir_path = Path::new(results_dir);

    // Open sled global DB
    let global_db_path = results_dir_path.join("registry-global.db");
    let global_db = open_registry_db(&global_db_path)?;

    // Resume if a previous registry exists
    let last_saved = detect_last_saved_epoch(results_dir_path).await?;
    let (mut registry, start_epoch) = if let Some((epoch, path)) = last_saved {
        tracing::info!("🔁 Resuming from epoch {epoch:04} ({})", path.display());
        (read_registry_from_disk(path).await?, epoch + 1)
    } else {
        tracing::info!("🆕 Starting fresh from epoch 0000");
        (AHashMap::new(), 0)
    };

    let mut unique = AHashSet::with_capacity(256);
    let pb = ProgressBar::new_spinner();
    let start_total = Instant::now();
    let mut total_blocks = 0u64;

    'outer: for epoch in start_epoch..=max_epoch {
        tracing::info!("🧩 Starting epoch {epoch:04}");

        // ensure file exists or download it
        let local_path = if has_local_epoch(cache_dir, epoch) {
            PathBuf::from(format!("{cache_dir}/epoch-{epoch}.car"))
        } else {
            download_epoch_to_disk(cache_dir, epoch).await?
        };

        // open offline mode
        let reader = match open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("❌ Failed to open epoch {epoch}: {e:#}");
                continue 'outer;
            }
        };

        let mut car = CarBlockReader::new(reader);
        car.read_header().await?;
        let start = Instant::now();
        let mut last_log = start;
        let mut blocks_count = 0;

        while let Some(block) = car.next_block().await? {
            let block_info = block.block()?;
            for entry_cid in block_info.entries.iter() {
                let Node::Entry(entry) = block.decode(entry_cid?.hash_bytes())? else {
                    continue;
                };
                for tx_cid in entry.transactions.iter() {
                    let tx_cid = tx_cid?;
                    let Node::Transaction(tx) = block.decode(tx_cid.hash_bytes())? else {
                        continue;
                    };

                    let tx_bytes: Vec<u8> = match tx.data.next {
                        None => tx.data.data.to_vec(),
                        Some(df_cid) => {
                            let df_cid = df_cid.to_cid()?;
                            let mut reader = block.dataframe_reader(&df_cid);
                            let mut tx_buff = Vec::new();
                            reader.read_to_end(&mut tx_buff)?;
                            tx_buff
                        }
                    };

                    unique.clear();
                    if let Some(fee_payer) = parse_account_keys_only_fast(&tx_bytes, &mut unique)? {
                        let fee_payer_id = registry.get(&fee_payer).map(|fp| fp.id).unwrap_or(0);
                        let mut next_id = registry.len() as u32;
                        for key in &unique {
                            let entry = registry.entry(*key).or_insert_with(|| {
                                let id = next_id;
                                next_id += 1;
                                KeyStats {
                                    id,
                                    count: 0,
                                    first_fee_payer: fee_payer_id,
                                    first_slot: block_info.slot,
                                    epoch,
                                }
                            });
                            entry.count += 1;
                            entry.epoch = epoch;
                        }
                    }
                }
            }

            blocks_count += 1;
            total_blocks += 1;
            let now = Instant::now();
            if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
                last_log = now;
                pb.set_message(format!(
                    "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} keys",
                    blocks_count,
                    blocks_count as f64 / now.duration_since(start).as_secs_f64(),
                    registry.len()
                ));
            }
        }

        // checkpoint per-epoch registry snapshot
        let out_path = results_dir_path.join(format!("registry-{epoch:04}.bin"));
        write_registry_to_disk(&out_path, &registry).await?;

        // merge to sled global DB
        merge_epoch_into_sled_postcard(&registry, &global_db)?;

        // clear memory before next epoch
        registry.clear();

        // delete .car to save disk
        if let Err(e) = fs::remove_file(&local_path).await {
            tracing::warn!("⚠️ Failed to delete {}: {e}", local_path.display());
        } else {
            tracing::info!("🧹 Deleted {}", local_path.display());
        }
    }

    tracing::info!(
        "🏁 Finished all epochs | {} total blocks | {:.1}s",
        total_blocks,
        start_total.elapsed().as_secs_f64()
    );
    Ok(())
}


pub fn inspect_registry_cli(
    db_path: &str,
    json_output: bool,
    single_pubkey: Option<String>,
) -> Result<()> {
    let db = open_registry_db(db_path)?;
    let mut count = 0usize;

    if let Some(pubkey_str) = single_pubkey {
        // --- Single key mode ---
        let pubkey =
            Pubkey::from_str(&pubkey_str).map_err(|_| anyhow!("Invalid Pubkey: {}", pubkey_str))?;
        let key = pubkey.to_bytes();

        if let Some(value_bytes) = db.get(&key)? {
            let stats: KeyStats = postcard::from_bytes(&value_bytes)?;
            if json_output {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "pubkey": pubkey_str,
                        "stats": stats
                    }))?
                );
            } else {
                println!("{pubkey_str}: {:?}", stats);
            }
        } else {
            println!("⚠️ No entry found for {pubkey_str}");
        }
        return Ok(());
    }

    // --- Full listing mode ---
    if json_output {
        let mut all = Vec::new();
        for item in db.iter() {
            let (k_bytes, v_bytes) = item?;
            let pubkey = Pubkey::try_from(k_bytes.as_ref())?;
            let stats: KeyStats = postcard::from_bytes(&v_bytes)?;
            all.push(serde_json::json!({
                "pubkey": pubkey.to_string(),
                "id": stats.id,
                "count": stats.count,
                "epoch": stats.epoch,
                "first_slot": stats.first_slot,
                "first_fee_payer": stats.first_fee_payer,
            }));
            count += 1;
        }
        println!("{}", serde_json::to_string_pretty(&all)?);
    } else {
        println!("📂 Listing all registry entries in {}", db_path);
        for item in db.iter() {
            let (k_bytes, v_bytes) = item?;
            let pubkey = Pubkey::try_from(k_bytes.as_ref())?;
            let stats: KeyStats = postcard::from_bytes(&v_bytes)?;
            println!(
                "{:<44} | id {:<6} | count {:<8} | epoch {:<5} | slot {:<8}",
                pubkey, stats.id, stats.count, stats.epoch, stats.first_slot
            );
            count += 1;
        }
        println!("\n📊 Total entries: {}", count);
    }

    Ok(())
}