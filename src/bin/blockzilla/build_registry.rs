use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result, anyhow};
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
};
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::{
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
};

use crate::{
    LOG_INTERVAL_SECS, file_downloader::download_epoch,
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

    download_epoch(epoch, &cache_dir, 3).await?;
    Ok(out_path)
}
// ============================================================================
// Unified auto mode: offline + auto-download + delete
// ============================================================================
pub async fn build_registry_auto(cache_dir: &str, results_dir: &str, max_epoch: u64) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();
    let results_dir_path = Path::new(results_dir);
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

        // checkpoint
        let out_path = results_dir_path.join(format!("registry-{epoch:04}.bin"));
        write_registry_to_disk(&out_path, &registry).await?;

        // delete .car to save disk
        if let Err(e) = fs::remove_file(&local_path).await {
            tracing::warn!("⚠️ Failed to delete {}: {e}", local_path.display());
        } else {
            tracing::info!("🧹 Deleted {}", local_path.display());
        }
    }

    tracing::info!(
        "🏁 Finished all epochs | {} total blocks | {} total keys | {:.1}s",
        total_blocks,
        registry.len(),
        start_total.elapsed().as_secs_f64()
    );
    Ok(())
}

pub async fn build_registry_single(cache_dir: &str, results_dir: &str, epoch: u64) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();

    tracing::info!("🧩 Building registry for epoch {epoch:04}");
    let results_dir_path = Path::new(results_dir);
    let mut registry = AHashMap::<Pubkey, KeyStats>::new();
    let mut unique = AHashSet::with_capacity(256);

    // ensure file exists or download it
    let local_path = if has_local_epoch(cache_dir, epoch) {
        PathBuf::from(format!("{cache_dir}/epoch-{epoch}.car"))
    } else {
        download_epoch_to_disk(cache_dir, epoch).await?
    };

    // open offline mode
    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline)
        .await
        .context("failed to open epoch")?;

    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;
    let pb = ProgressBar::new_spinner();
    let start = Instant::now();
    let mut last_log = start;
    let mut blocks_count = 0u64;
    let mut total_blocks = 0u64;

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

    // write output
    let out_path = results_dir_path.join(format!("registry-{epoch:04}.bin"));
    write_registry_to_disk(&out_path, &registry).await?;

    // optional cleanup
    if let Err(e) = fs::remove_file(&local_path).await {
        tracing::warn!("⚠️ Failed to delete {}: {e}", local_path.display());
    } else {
        tracing::info!("🧹 Deleted {}", local_path.display());
    }

    tracing::info!(
        "✅ Finished epoch {epoch:04} | {} blocks | {} keys | {:.1}s",
        total_blocks,
        registry.len(),
        start.elapsed().as_secs_f64()
    );
    Ok(())
}
