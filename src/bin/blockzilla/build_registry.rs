use ahash::AHashMap;
use anyhow::Result;
use blockzilla::transaction_parser::parse_account_keys_only;
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
};
use indicatif::{ProgressBar, ProgressStyle};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::collections::hash_map::Entry;
use std::{
    collections::HashSet,
    error::Error as StdError,
    io::{self, ErrorKind, Read},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, File},
    io::{AsyncWriteExt, BufWriter},
};

use crate::LOG_INTERVAL_SECS;

// ===============================
// Config
// ===============================
const BUF_WRITER_BYTES: usize = 16 * 1024 * 1024;
// Heuristic: reserve big to avoid mid-epoch rehash storms.
// Tune this to your machine/epoch scale.
const EXPECTED_UNIQUE_FP: usize = 12_000_000;

// ===============================
// Paths
// ===============================
pub fn epoch_dir(base: &Path, epoch: u64) -> PathBuf {
    base.join(format!("epoch-{epoch:04}"))
}
pub fn keys_bin(dir: &Path) -> PathBuf {
    dir.join("keys.bin")
}
pub fn fp2key_bin(dir: &Path) -> PathBuf {
    dir.join("fp2key.bin")
}
fn lock_path(dir: &Path) -> PathBuf {
    dir.join("epoch.lock")
}
fn unique_tmp_file(dir: &Path, stem: &str) -> PathBuf {
    let pid = std::process::id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    dir.join(format!("{stem}.tmp.{pid}.{ts}"))
}

async fn try_epoch_lock(dir: &Path) -> Result<Option<File>> {
    match fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(lock_path(dir))
        .await
    {
        Ok(f) => Ok(Some(f)),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(None),
        Err(e) => Err(e.into()),
    }
}
async fn release_epoch_lock(dir: &Path) {
    let _ = fs::remove_file(lock_path(dir)).await;
}

// ===============================
// Fingerprint (u128 from 32B pubkey)
// ===============================
#[inline(always)]
pub fn fp128_from_bytes(b: &[u8; 32]) -> u128 {
    let a = u128::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15],
    ]);
    let c = u128::from_le_bytes([
        b[16], b[17], b[18], b[19], b[20], b[21], b[22], b[23], b[24], b[25], b[26], b[27], b[28],
        b[29], b[30], b[31],
    ]);
    a ^ c.rotate_left(64)
}

// ===============================
// Soft EOF detection
// ===============================
fn is_soft_eof(e: &anyhow::Error) -> bool {
    let mut cur: Option<&(dyn StdError + 'static)> = Some(e.as_ref());
    while let Some(err) = cur {
        if let Some(ioe) = err.downcast_ref::<io::Error>()
            && ioe.kind() == io::ErrorKind::UnexpectedEof
        {
            return true;
        }
        let msg = err.to_string();
        if msg.contains("unexpected EOF") || msg.contains("without BlockNode") {
            return true;
        }
        cur = err.source();
    }
    false
}

// ===============================
// DROP-IN: single-thread epoch builder
// - ONE map: fp -> u16 (saturating)
// - On first insert: stream [fp|key] + append key
// ===============================
pub async fn process_epoch_one_pass(
    cache_dir: &str,
    results_dir: &str,
    epoch: u64,
    pb: &ProgressBar,
) -> Result<usize> {
    let base = Path::new(results_dir);
    let edir = epoch_dir(base, epoch);
    fs::create_dir_all(&edir).await?;

    let final_keys = keys_bin(&edir);
    let final_fp = fp2key_bin(&edir);
    if fs::metadata(&final_keys).await.is_ok() && fs::metadata(&final_fp).await.is_ok() {
        pb.set_message(format!("epoch {epoch:04} already complete"));
        return Ok(0);
    }

    // Lock
    let _lock = match try_epoch_lock(&edir).await? {
        Some(f) => Some(f),
        None => {
            pb.set_message(format!("epoch {epoch:04} skipped (locked)"));
            return Ok(0);
        }
    };

    // Temp writers
    let tmp_fp_path = unique_tmp_file(&edir, "fp2key");
    let tmp_keys_path = unique_tmp_file(&edir, "keys");
    let mut fp_writer =
        BufWriter::with_capacity(BUF_WRITER_BYTES, File::create(&tmp_fp_path).await?);
    let mut keys_writer =
        BufWriter::with_capacity(BUF_WRITER_BYTES, File::create(&tmp_keys_path).await?);

    // Global counts map (single structure, no extra HashSet)
    let mut counts: AHashMap<u128, u16> = AHashMap::with_capacity(EXPECTED_UNIQUE_FP);

    // Reader
    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let mut blocks_done: u64 = 0;
    let start = Instant::now();
    let mut last_log = start;

    // Reusable buffers
    let mut tx_buf = Vec::<u8>::with_capacity(128 * 1024);
    let mut keys_vec: SmallVec<[Pubkey; 256]> = SmallVec::new();

    'epoch: while let Some(block) = car.next_block().await? {
        // Decode BlockNode with soft-EOF handling
        let info = match block.block() {
            Ok(b) => b,
            Err(e) => {
                if is_soft_eof(&e) {
                    break 'epoch;
                }
                continue;
            }
        };

        for entry_cid in info.entries.iter() {
            // Decode Entry
            let entry = match (|| -> anyhow::Result<_> {
                let Node::Entry(entry) = block.decode(entry_cid?.hash_bytes())? else {
                    anyhow::bail!("not Entry");
                };
                Ok(entry)
            })() {
                Ok(e) => e,
                Err(e) => {
                    if is_soft_eof(&e) {
                        break 'epoch;
                    }
                    continue;
                }
            };

            for tx_cid in entry.transactions.iter() {
                // Decode Transaction
                let tx_node = match (|| -> anyhow::Result<_> {
                    let tcid = tx_cid?;
                    let Node::Transaction(tx_node) = block.decode(tcid.hash_bytes())? else {
                        anyhow::bail!("not Transaction");
                    };
                    Ok(tx_node)
                })() {
                    Ok(t) => t,
                    Err(e) => {
                        if is_soft_eof(&e) {
                            break 'epoch;
                        }
                        continue;
                    }
                };

                // Pull tx bytes (inline or from dataframe)
                let tx_bytes: &[u8] = match tx_node.data.next {
                    None => tx_node.data.data,
                    Some(df_cid) => {
                        tx_buf.clear();
                        let df = df_cid.to_cid()?;
                        let mut rdr = block.dataframe_reader(&df);
                        rdr.read_to_end(&mut tx_buf)?;
                        &tx_buf
                    }
                };

                // Parse all keys (no per-tx dedup)
                keys_vec.clear();
                if parse_account_keys_only(tx_bytes, &mut keys_vec)
                    .ok()
                    .flatten()
                    .is_none()
                {
                    continue;
                }

                // Single-structure fast path:
                // - Entry::Vacant => first global sighting: stream [fp|key] + append key, insert 1
                // - Entry::Occupied => saturating increment
                for &pk in keys_vec.iter() {
                    let kb: [u8; 32] = pk.to_bytes();
                    let fp = fp128_from_bytes(&kb);

                    match counts.entry(fp) {
                        Entry::Vacant(v) => {
                            // first sighting => stream
                            fp_writer.write_all(&fp.to_le_bytes()).await?;
                            fp_writer.write_all(&kb).await?;
                            keys_writer.write_all(&kb).await?;
                            v.insert(1u16);
                        }
                        Entry::Occupied(mut o) => {
                            let r = (*o.get() as u32) + 1;
                            *o.get_mut() = r.min(u16::MAX as u32) as u16;
                        }
                    }
                }
            }
        }

        blocks_done += 1;

        // Progress every LOG_INTERVAL_SECS
        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);
            let blkps = blocks_done as f64 / elapsed;
            let discovered = counts.len();
            pb.set_message(format!(
                "epoch {epoch:04} | {:>8} blk | {:>8.1} blk/s | {:>9} keys",
                blocks_done, blkps, discovered
            ));
            last_log = now;
        }
    }

    // Final flush
    fp_writer.flush().await?;
    keys_writer.flush().await?;

    // Atomically move files into place
    if fs::metadata(&final_fp).await.is_ok() {
        let _ = fs::remove_file(&final_fp).await;
    }
    if fs::metadata(&final_keys).await.is_ok() {
        let _ = fs::remove_file(&final_keys).await;
    }
    fs::rename(&tmp_fp_path, &final_fp).await?;
    fs::rename(&tmp_keys_path, &final_keys).await?;

    // Final progress line
    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    let blkps = blocks_done as f64 / elapsed;
    pb.set_message(format!(
        "epoch {epoch:04} | {:>8} blk | {:>8.1} blk/s | {:>9} keys (done)",
        blocks_done,
        blkps,
        counts.len()
    ));

    release_epoch_lock(&edir).await;
    Ok(counts.len())
}

// ===============================
// Orchestration (signatures unchanged)
// ===============================
pub async fn build_registry_auto(
    cache_dir: &str,
    results_dir: &str,
    max_epoch: u64,
    _workers: usize, // unused
) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();

    // epoch is done if both files exist
    let mut completed = HashSet::new();
    if let Ok(mut rd) = fs::read_dir(results_dir).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            let p = ent.path();
            if !p.is_dir() {
                continue;
            }
            if let Some(name) = p.file_name().and_then(|s| s.to_str())
                && let Some(num) = name
                    .strip_prefix("epoch-")
                    .and_then(|s| s.parse::<u64>().ok())
                && fs::metadata(keys_bin(&p)).await.is_ok()
                && fs::metadata(fp2key_bin(&p)).await.is_ok()
            {
                completed.insert(num);
            }
        }
    }

    let mut queue: Vec<u64> = (0..=max_epoch).filter(|e| !completed.contains(e)).collect();
    if queue.is_empty() {
        tracing::info!("All epochs already completed");
        return Ok(());
    }
    queue.sort_unstable();

    let pb = ProgressBar::new_spinner();
    pb.set_prefix("AUTO");
    pb.set_style(ProgressStyle::with_template("[{elapsed_precise}] {prefix:>4} | {msg}").unwrap());
    pb.enable_steady_tick(Duration::from_millis(100));

    let mut done = 0usize;
    let mut failed = 0usize;

    for epoch in queue {
        pb.set_message(format!("starting epoch {epoch:04}..."));
        match process_epoch_one_pass(cache_dir, results_dir, epoch, &pb).await {
            Ok(total) => {
                done += 1;
                pb.set_message(format!(
                    "ok epoch {epoch:04} | {:>8} keys | {done} ok, {failed} fail",
                    total
                ));
            }
            Err(e) => {
                failed += 1;
                pb.set_message(format!(
                    "fail epoch {epoch:04}: {e} | {done} ok, {failed} fail"
                ));
            }
        }
    }

    pb.finish_with_message(format!("Finished: {done} ok, {failed} fail"));
    Ok(())
}

pub async fn build_registry_single(cache_dir: &str, results_dir: &str, epoch: u64) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();
    let pb = ProgressBar::new_spinner();
    pb.set_prefix("SINGLE");
    pb.enable_steady_tick(Duration::from_millis(200));
    tracing::info!("Building fp2key and keys for epoch {epoch:04}");

    let start = Instant::now();
    let total = process_epoch_one_pass(cache_dir, results_dir, epoch, &pb).await?;
    tracing::info!(
        "Finished epoch {epoch:04} | {} keys | {:.1}s",
        total,
        start.elapsed().as_secs_f64()
    );
    pb.finish_with_message(format!("epoch {epoch:04} complete | {} keys", total));
    Ok(())
}
