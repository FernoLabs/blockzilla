use ahash::{AHashMap, AHashSet};
use anyhow::{anyhow, Context, Result};
use blockzilla::transaction_parser::parse_account_keys_only;
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
};
use indicatif::{ProgressBar, ProgressStyle};
use prost::encoding::decode_varint;
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::collections::hash_map::Entry;
use std::{
    collections::HashSet,
    error::Error as StdError,
    io::{self, ErrorKind, Read},
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
};

use crate::LOG_INTERVAL_SECS;

// ===============================
// Config
// ===============================
const BUF_WRITER_BYTES: usize = 16 * 1024 * 1024;
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
// Protobuf wire walker helpers
// ===============================
#[inline]
fn skip_value(cur: &mut std::io::Cursor<&[u8]>, wire: u64) -> anyhow::Result<()> {
    match wire {
        0 => {
            let _ = decode_varint(cur)?;
        }
        1 => {
            cur.set_position(cur.position() + 8);
        }
        2 => {
            let len = decode_varint(cur)? as u64;
            cur.set_position(cur.position().saturating_add(len));
        }
        5 => {
            cur.set_position(cur.position() + 4);
        }
        _ => return Err(anyhow!("unsupported wire type {wire}")),
    }
    Ok(())
}

#[inline]
fn read_len_slice<'a>(
    cur: &mut std::io::Cursor<&'a [u8]>,
    whole: &'a [u8],
) -> anyhow::Result<&'a [u8]> {
    let len = decode_varint(cur)? as usize;
    let start = cur.position() as usize;
    let end = start
        .checked_add(len)
        .ok_or_else(|| anyhow!("overflow"))?
        .min(whole.len());
    let s = &whole[start..end];
    cur.set_position((start + len) as u64);
    Ok(s)
}

/// reward message: only take field 1 (string pubkey)
fn reward_take_pubkey_bytes<'a>(reward: &'a [u8]) -> Option<&'a [u8]> {
    let mut c = std::io::Cursor::new(reward);
    while (c.position() as usize) < reward.len() {
        let tag = decode_varint(&mut c).ok()?;
        let f = tag >> 3;
        let w = tag & 0x7;
        if f == 1 && w == 2 {
            return read_len_slice(&mut c, reward).ok();
        }
        if skip_value(&mut c, w).is_err() {
            break;
        }
    }
    None
}

/// Walk TransactionStatusMeta and push pubkey strings (as bytes) into callback.
fn meta_walk_pubkey_strs<'a>(
    meta: &'a [u8],
    mut on_pubkey_str_bytes: impl FnMut(&'a [u8]),
) -> anyhow::Result<()> {
    let mut cur = std::io::Cursor::new(meta);
    while (cur.position() as usize) < meta.len() {
        let start = cur.position();
        let tag = match decode_varint(&mut cur) {
            Ok(t) => t,
            Err(_) => break,
        };
        let field = tag >> 3;
        let wire = tag & 0x7;

        match (field, wire) {
            // rewards (repeated message)
            (9, 2) => {
                let msg = read_len_slice(&mut cur, meta)?;
                if let Some(pk) = reward_take_pubkey_bytes(msg) {
                    on_pubkey_str_bytes(pk);
                }
            }
            // loaded_{writable,readonly}_addresses (strings)
            (10, 2) | (11, 2) => {
                let s = read_len_slice(&mut cur, meta)?;
                on_pubkey_str_bytes(s);
            }
            _ => {
                skip_value(&mut cur, wire)?;
            }
        }

        if cur.position() == start {
            break;
        }
    }
    Ok(())
}

// ===============================
// Metadata decoder (full zstd decode)
// ===============================
fn extract_metadata_pubkeys(meta_zstd: &[u8], out: &mut SmallVec<[Pubkey; 256]>) -> Result<()> {
    if meta_zstd.is_empty() {
        return Ok(());
    }

    let decompressed = zstd::decode_all(meta_zstd)
        .context("failed to decompress metadata with zstd")?;

    meta_walk_pubkey_strs(&decompressed, |s_bytes| {
        if let Ok(st) = core::str::from_utf8(s_bytes) {
            if let Ok(pk) = Pubkey::from_str(st) {
                out.push(pk);
            }
        }
    })?;

    Ok(())
}

// ===============================
// Single-thread epoch builder
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

    // Counts
    let mut counts: AHashMap<u128, u16> = AHashMap::with_capacity(EXPECTED_UNIQUE_FP);

    // Reader
    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let mut blocks_done = 0u64;
    let mut txs_with_meta = 0u64;
    let mut total_txs = 0u64;

    let start = Instant::now();
    let mut last_log = start;

    let mut tx_buf = Vec::with_capacity(128 * 1024);
    let mut buf_meta = Vec::with_capacity(128 * 1024);
    let mut keys_vec: SmallVec<[Pubkey; 256]> = SmallVec::new();

    'epoch: while let Some(block) = car.next_block().await? {
        let info = match block.block() {
            Ok(b) => b,
            Err(e) if is_soft_eof(&e) => break 'epoch,
            Err(_) => continue,
        };

        for entry_cid in info.entries.iter() {
            // Decode Entry
            let entry = match (|| -> anyhow::Result<_> {
                let block_node = block.decode(entry_cid?.hash_bytes())?;
                let Node::Entry(entry) = block_node else {
                    anyhow::bail!("not Entry");
                };
                Ok(entry)
            })() {
                Ok(e) => e,
                Err(e) if is_soft_eof(&e) => break 'epoch,
                Err(_) => continue,
            };

            for tx_cid in entry.transactions.iter() {
                // Decode Transaction
                let tx_node = match (|| -> anyhow::Result<_> {
                    let tcid = tx_cid?;
                    let block_node = block.decode(tcid.hash_bytes())?;
                    let Node::Transaction(tx_node) = block_node else {
                        anyhow::bail!("not Transaction");
                    };
                    Ok(tx_node)
                })() {
                    Ok(t) => t,
                    Err(e) if is_soft_eof(&e) => break 'epoch,
                    Err(_) => continue,
                };

                total_txs += 1;

                // Transaction bytes
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

                // parse account keys from transaction
                keys_vec.clear();
                if parse_account_keys_only(tx_bytes, &mut keys_vec)
                    .ok()
                    .flatten()
                    .is_none()
                {
                    continue;
                }

                // Decode metadata fully and walk
                let meta_res = (|| -> anyhow::Result<()> {
                    let meta_bytes: &[u8] = match tx_node.metadata.next {
                        None => tx_node.metadata.data,
                        Some(df_cid) => {
                            buf_meta.clear();
                            let df = df_cid.to_cid()?;
                            let mut rdr = block.dataframe_reader(&df);
                            rdr.read_to_end(&mut buf_meta)?;
                            &buf_meta
                        }
                    };

                    if !meta_bytes.is_empty() {
                        extract_metadata_pubkeys(meta_bytes, &mut keys_vec)?;
                        txs_with_meta += 1;
                    }
                    Ok(())
                })();

                if let Err(e) = meta_res {
                    if blocks_done == 0 {
                        tracing::debug!("metadata parse failed: {e}");
                    }
                }

                // Process all collected keys
                for &pk in keys_vec.iter() {
                    let kb = pk.to_bytes();
                    let fp = fp128_from_bytes(&kb);
                    match counts.entry(fp) {
                        Entry::Vacant(v) => {
                            fp_writer.write_all(&fp.to_le_bytes()).await?;
                            fp_writer.write_all(&kb).await?;
                            keys_writer.write_all(&kb).await?;
                            v.insert(1);
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
        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);
            let blkps = blocks_done as f64 / elapsed;
            let tps = total_txs as f64 / elapsed;
            pb.set_message(format!(
                "epoch {epoch:04} | {:>8} blk | {:>8.1} blk/s | {:>10} tx | {:>8.1} tps | {:>9} keys | {:>8} tx/meta",
                blocks_done, blkps, total_txs, tps, counts.len(), txs_with_meta
            ));
            last_log = now;
        }
    }

    // Final flush and move
    fp_writer.flush().await?;
    keys_writer.flush().await?;
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
    let tps = total_txs as f64 / elapsed;
    pb.set_message(format!(
        "epoch {epoch:04} | {:>8} blk | {:>8.1} blk/s | {:>10} tx | {:>8.1} tps | {:>9} keys | {:>8} tx/meta (done)",
        blocks_done, blkps, total_txs, tps, counts.len(), txs_with_meta
    ));

    release_epoch_lock(&edir).await;
    Ok(counts.len())
}

// ===============================
// Orchestration (public API)
// ===============================
pub async fn build_registry_auto(
    cache_dir: &str,
    results_dir: &str,
    max_epoch: u64,
    _workers: usize,
) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();

    // detect already-completed epochs (skip those)
    let mut completed = HashSet::new();
    if let Ok(mut rd) = fs::read_dir(results_dir).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            let p = ent.path();
            if !p.is_dir() {
                continue;
            }
            if let Some(name) = p.file_name().and_then(|s| s.to_str())
                && name.starts_with("epoch-")
                && fs::metadata(keys_bin(&p)).await.is_ok()
                && fs::metadata(fp2key_bin(&p)).await.is_ok()
            {
                if let Some(num) = name.trim_start_matches("epoch-").parse::<u64>().ok() {
                    completed.insert(num);
                }
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

pub async fn build_registry_single(
    cache_dir: &str,
    results_dir: &str,
    epoch: u64,
) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();
    let pb = ProgressBar::new_spinner();
    pb.set_prefix("SINGLE");
    pb.set_style(ProgressStyle::with_template("[{elapsed_precise}] {prefix:>6} | {msg}").unwrap());
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

pub async fn merge_registries(registry_dir: &str, output_dir: &str) -> Result<usize> {
    // collect per-epoch dirs that have both files
    let mut epochs: Vec<(u64, PathBuf)> = Vec::new();
    let mut rd = fs::read_dir(registry_dir)
        .await
        .with_context(|| format!("read registry dir {registry_dir}"))?;
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        let Some(epoch) = name
            .strip_prefix("epoch-")
            .and_then(|s| s.parse::<u64>().ok())
        else {
            continue;
        };
        if fs::metadata(keys_bin(&path)).await.is_err() {
            continue;
        }
        if fs::metadata(fp2key_bin(&path)).await.is_err() {
            continue;
        }
        epochs.push((epoch, path));
    }

    epochs.sort_by_key(|(epoch, _)| *epoch);

    // merge preserving first-seen order across epochs
    let mut seen = AHashSet::new();
    let mut ordered: Vec<(u128, [u8; 32])> = Vec::new();

    for (_, dir) in epochs.into_iter() {
        let fp_path = fp2key_bin(&dir);
        let meta = match fs::metadata(&fp_path).await {
            Ok(m) => m,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        let len = meta.len();
        if len == 0 {
            continue;
        }
        if len % 48 != 0 {
            return Err(anyhow!(
                "fp2key file {} has length {} which is not a multiple of 48",
                fp_path.display(),
                len
            ));
        }
        let mut reader = BufReader::new(File::open(&fp_path).await?);
        let entries = len / 48;
        for _ in 0..entries {
            let mut chunk = [0u8; 48];
            reader.read_exact(&mut chunk).await?;
            let mut fp_bytes = [0u8; 16];
            fp_bytes.copy_from_slice(&chunk[..16]);
            let fp = u128::from_le_bytes(fp_bytes);
            if seen.insert(fp) {
                let mut key = [0u8; 32];
                key.copy_from_slice(&chunk[16..]);
                ordered.push((fp, key));
            }
        }
    }

    // write merged outputs atomically
    let out_dir = Path::new(output_dir);
    fs::create_dir_all(out_dir).await?;

    let tmp_fp = unique_tmp_file(out_dir, "fp2key-merged");
    let tmp_keys = unique_tmp_file(out_dir, "keys-merged");
    let mut fp_writer = BufWriter::with_capacity(BUF_WRITER_BYTES, File::create(&tmp_fp).await?);
    let mut keys_writer =
        BufWriter::with_capacity(BUF_WRITER_BYTES, File::create(&tmp_keys).await?);

    for (fp, key) in &ordered {
        fp_writer.write_all(&fp.to_le_bytes()).await?;
        fp_writer.write_all(key).await?;
        keys_writer.write_all(key).await?;
    }

    fp_writer.flush().await?;
    keys_writer.flush().await?;

    let final_fp = fp2key_bin(out_dir);
    let final_keys = keys_bin(out_dir);
    if fs::metadata(&final_fp).await.is_ok() {
        let _ = fs::remove_file(&final_fp).await;
    }
    if fs::metadata(&final_keys).await.is_ok() {
        let _ = fs::remove_file(&final_keys).await;
    }
    fs::rename(&tmp_fp, &final_fp).await?;
    fs::rename(&tmp_keys, &final_keys).await?;

    Ok(ordered.len())
}
