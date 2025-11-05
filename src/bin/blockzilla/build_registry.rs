use ahash::AHashMap;
use anyhow::{anyhow, Context, Result};
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::{
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
    task,
};

use memmap2::Mmap;

use crate::{LOG_INTERVAL_SECS, transaction_parser::parse_account_keys_only};

/// 8-byte fingerprint from the first bytes of the pubkey
#[inline(always)]
fn pubkey_fp(k: &Pubkey) -> u64 {
    let b = k.as_ref();
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

/// Map entry that guarantees collision-safety by verifying full 32B keys
enum IndexEntry {
    /// Fast path: only one key currently mapped for this fp
    Single { id: u32, key: [u8; 32] },
    /// Slow path: multiple distinct keys sharing the same fp
    Multi(Vec<([u8; 32], u32)>),
}

impl IndexEntry {
    #[inline(always)]
    fn get_or_insert_id(&mut self, key: &[u8; 32], next_id: &mut u32) -> u32 {
        match self {
            IndexEntry::Single { id, key: stored } => {
                if stored == key {
                    *id
                } else {
                    // Collision: upgrade to Multi
                    let old = (*stored, *id);
                    let new_id = *next_id;
                    *next_id = next_id.saturating_add(1);
                    *self = IndexEntry::Multi(vec![old, (*key, new_id)]);
                    new_id
                }
            }
            IndexEntry::Multi(list) => {
                if let Some((_, id)) = list.iter().find(|(k, _)| k == key) {
                    *id
                } else {
                    let new_id = *next_id;
                    *next_id = next_id.saturating_add(1);
                    list.push((*key, new_id));
                    new_id
                }
            }
        }
    }
}

async fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path).await {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Clean up temporary keys file for a specific epoch
async fn cleanup_keys_tmp_for_epoch(results_dir: &str, epoch: u64) -> Result<()> {
    let tmp_path = Path::new(results_dir).join(format!("keys-{epoch:04}.tmp"));
    remove_if_exists(&tmp_path).await
}

/// Clean up all temporary keys files in the results directory
pub async fn cleanup_all_keys_tmp(results_dir: &str) -> Result<usize> {
    let mut removed = 0usize;
    let mut read_dir = match fs::read_dir(results_dir).await {
        Ok(rd) => rd,
        Err(_) => return Ok(0),
    };

    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with("keys-")
            && name_str.ends_with(".tmp")
            && fs::remove_file(entry.path()).await.is_ok()
        {
            removed += 1;
            tracing::debug!("Removed temporary file: {}", name_str);
        }
    }

    if removed > 0 {
        tracing::info!("Cleaned up {} temporary keys files", removed);
    }
    Ok(removed)
}

/// Simple grow-on-demand counters
struct CompactKeyData {
    counts: Vec<u32>,
}
impl CompactKeyData {
    fn new() -> Self {
        Self { counts: Vec::new() }
    }
    #[inline(always)]
    fn ensure_len(&mut self, idx: usize) {
        if idx >= self.counts.len() {
            self.counts.resize(idx + 1, 0);
        }
    }
    #[inline(always)]
    fn inc(&mut self, idx: usize) {
        self.ensure_len(idx);
        unsafe {
            let c = self.counts.get_unchecked_mut(idx);
            *c = c.saturating_add(1);
        }
    }
}

/// Write ordered pubkeys using only counts + keys file (no rayon)
fn write_ordered_pubkeys_from_counts(
    keys_file: &Path,
    out_file: &Path,
    counts: &[u32],
    total_keys: usize,
) -> Result<()> {
    // Build index vector 0..N-1 and sort by count desc
    let mut idxs: Vec<u32> = (0..total_keys as u32).collect();
    idxs.sort_unstable_by(|&a, &b| counts[b as usize].cmp(&counts[a as usize]));

    // mmap input for fast random reads; write output sequentially
    let in_f = std::fs::OpenOptions::new()
        .read(true)
        .open(keys_file)
        .with_context(|| format!("open {}", keys_file.display()))?;
    let mmap = unsafe { Mmap::map(&in_f).with_context(|| "mmap keys file")? };
    if mmap.len() != total_keys * 32 {
        return Err(anyhow!(
            "keys file size mismatch: {} vs expected {}",
            mmap.len(),
            total_keys * 32
        ));
    }

    let out_f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(out_file)
        .with_context(|| format!("create {}", out_file.display()))?;
    let mut w = std::io::BufWriter::with_capacity(64 * 1024 * 1024, out_f);

    for idx in idxs {
        let off = (idx as usize) * 32;
        w.write_all(&mmap[off..off + 32])?;
    }
    w.flush()?;
    Ok(())
}

/// One pass over an epoch: count unique pubkeys and persist canonical key list
/// Returns (counts, total_unique_keys)
async fn process_epoch_one_pass(
    cache_dir: &str,
    results_dir: &str,
    epoch: u64,
    pb: &ProgressBar,
) -> Result<(Vec<u32>, usize)> {
    let keys_path = Path::new(results_dir).join(format!("keys-{epoch:04}.bin"));
    let keys_tmp = keys_path.with_extension("tmp");
    remove_if_exists(&keys_tmp).await?;

    let f = File::create(&keys_tmp).await?;
    let mut keys_writer = BufWriter::with_capacity(32 * 1024 * 1024, f);

    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    // fp -> IndexEntry (conflict-safe via full 32B check)
    let mut fp_index: AHashMap<u64, IndexEntry> = AHashMap::with_capacity(1_000_000);
    let mut next_id: u32 = 0;

    let mut data = CompactKeyData::new();

    let start = Instant::now();
    let mut last_log = start;
    let mut blocks = 0u64;

    let mut tmp_keys: SmallVec<[Pubkey; 256]> = SmallVec::new();
    let mut tx_buf = Vec::<u8>::with_capacity(64 * 1024);

    // Batch write 32B keys to reduce syscalls
    const KEY_BATCH_SIZE: usize = 8192;
    let mut key_batch = Vec::with_capacity(KEY_BATCH_SIZE * 32);

    let mut msg = String::with_capacity(96);

    while let Some(block) = car.next_block().await? {
        let block_info = block.block()?;

        for entry_cid in block_info.entries.iter() {
            let Node::Entry(entry) = block.decode(entry_cid?.hash_bytes())? else {
                continue;
            };
            for tx_cid in entry.transactions.iter() {
                let tx_cid = tx_cid?;
                let Node::Transaction(tx_node) = block.decode(tx_cid.hash_bytes())? else {
                    continue;
                };

                // Load tx bytes
                let tx_bytes: &[u8] = match tx_node.data.next {
                    None => tx_node.data.data,
                    Some(df_cid) => {
                        tx_buf.clear();
                        let df_cid = df_cid.to_cid()?;
                        let mut rdr = block.dataframe_reader(&df_cid);
                        rdr.read_to_end(&mut tx_buf)?;
                        &tx_buf
                    }
                };

                // Extract static account keys, dedup inside tx
                tmp_keys.clear();
                if parse_account_keys_only(tx_bytes, &mut tmp_keys)?.is_some() {
                    tmp_keys.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
                    tmp_keys.dedup_by_key(|k| *k);

                    for &k in tmp_keys.iter() {
                        let fp = pubkey_fp(&k);
                        let key_arr: [u8; 32] = k.as_ref().try_into().unwrap();

                        let id = match fp_index.get_mut(&fp) {
                            Some(entry) => entry.get_or_insert_id(&key_arr, &mut next_id),
                            None => {
                                let id = next_id;
                                next_id = next_id.saturating_add(1);
                                fp_index.insert(fp, IndexEntry::Single { id, key: key_arr });

                                // Append 32-byte key to file in batches
                                key_batch.extend_from_slice(&key_arr);
                                if key_batch.len() >= KEY_BATCH_SIZE * 32 {
                                    keys_writer.write_all(&key_batch).await?;
                                    key_batch.clear();
                                }
                                id
                            }
                        };

                        data.inc(id as usize);
                    }
                }
            }
        }

        blocks += 1;
        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            msg.clear();
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);
            let blkps = blocks as f64 / elapsed;
            use std::fmt::Write;
            let _ = write!(
                &mut msg,
                "epoch {epoch:04} | {blocks:>7} blk | {blkps:>7.1} blk/s | {keys} keys",
                keys = next_id
            );
            pb.set_message(msg.clone());
            last_log = now;
        }
    }

    // Flush remaining keys
    if !key_batch.is_empty() {
        keys_writer.write_all(&key_batch).await?;
    }
    keys_writer.flush().await?;

    // Atomic rename tmp to final file
    if let Err(err) = fs::rename(&keys_tmp, &keys_path).await {
        let _ = remove_if_exists(&keys_tmp).await;
        return Err(err.into());
    }

    Ok((data.counts, next_id as usize))
}

pub async fn build_registry_auto(
    cache_dir: &str,
    results_dir: &str,
    max_epoch: u64,
    workers: usize,
) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();

    // Clean stale tmp files
    let cleaned = cleanup_all_keys_tmp(results_dir).await?;
    if cleaned > 0 {
        tracing::info!("Cleaned up {} stale temporary files", cleaned);
    }

    let start_total = Instant::now();

    // Detect completed epochs by presence of registry-pubkeys
    let mut completed_epochs = std::collections::HashSet::new();
    if let Ok(mut entries) = fs::read_dir(results_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(num_str) = name
                .strip_prefix("registry-pubkeys-")
                .and_then(|s| s.strip_suffix(".bin"))
            {
                if let Ok(num) = num_str.parse::<u64>() {
                    completed_epochs.insert(num);
                }
            }
        }
    }

    let work_queue: Vec<u64> = (0..=max_epoch)
        .filter(|e| !completed_epochs.contains(e))
        .collect();

    if work_queue.is_empty() {
        tracing::info!("All epochs already completed");
        return Ok(());
    }

    tracing::info!(
        "Processing {} epochs with {} workers (skipping {})",
        work_queue.len(),
        workers,
        completed_epochs.len()
    );

    let work_queue = Arc::new(tokio::sync::Mutex::new(work_queue.into_iter()));
    let multi = Arc::new(MultiProgress::new());
    let style = ProgressStyle::with_template("[{elapsed_precise}] {prefix:>4} | {msg}").unwrap();

    let mut handles = Vec::new();
    for w in 0..workers {
        let cache = cache_dir.to_string();
        let out = results_dir.to_string();
        let mp = multi.clone();
        let val = style.clone();
        let queue = work_queue.clone();

        handles.push(task::spawn(async move {
            let pb = mp.add(ProgressBar::new_spinner());
            pb.set_prefix(format!("W{w}"));
            pb.set_style(val.clone());
            pb.enable_steady_tick(Duration::from_millis(100));

            let mut processed = 0usize;
            let mut failed = 0usize;

            loop {
                let epoch = {
                    let mut q = queue.lock().await;
                    q.next()
                };
                let Some(epoch) = epoch else { break };

                pb.set_message(format!("starting epoch {epoch:04}..."));

                match process_epoch_one_pass(&cache, &out, epoch, &pb).await {
                    Ok((counts, total)) => {
                        pb.set_message(format!("epoch {epoch:04} sorting/writing pubkeys..."));

                        let keys_path = Path::new(&out).join(format!("keys-{epoch:04}.bin"));
                        let pubkeys_path =
                            Path::new(&out).join(format!("registry-pubkeys-{epoch:04}.bin"));

                        let write_res = (|| -> Result<()> {
                            write_ordered_pubkeys_from_counts(
                                &keys_path,
                                &pubkeys_path,
                                &counts,
                                total,
                            )?;
                            Ok(())
                        })();

                        match write_res {
                            Ok(_) => {
                                // Optional: remove the raw keys file to save disk
                                // std::fs::remove_file(&keys_path).ok();

                                // Clean tmp for this epoch
                                let _ = cleanup_keys_tmp_for_epoch(&out, epoch).await;

                                processed += 1;
                                pb.set_message(format!(
                                    "done epoch {epoch:04} | {} keys | {processed} ok, {failed} fail",
                                    total
                                ));
                            }
                            Err(e) => {
                                failed += 1;
                                pb.set_message(format!(
                                    "write failed epoch {epoch:04}: {e} | {processed} ok, {failed} fail"
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        failed += 1;
                        pb.set_message(format!(
                            "failed epoch {epoch:04}: {e} | {processed} ok, {failed} fail"
                        ));
                    }
                }
            }

            pb.finish_with_message(format!("W{w} finished: {processed} ok, {failed} fail"));
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    // Final cleanup
    let final_cleaned = cleanup_all_keys_tmp(results_dir).await?;
    if final_cleaned > 0 {
        tracing::info!("Final cleanup removed {} temporary files", final_cleaned);
    }

    tracing::info!(
        "Finished all epochs ({} workers) in {:.1}s",
        workers,
        start_total.elapsed().as_secs_f64()
    );
    Ok(())
}

pub async fn build_registry_single(cache_dir: &str, results_dir: &str, epoch: u64) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();
    let dir = Path::new(results_dir);

    let pb = ProgressBar::new_spinner();
    pb.set_prefix("SINGLE");
    pb.enable_steady_tick(Duration::from_millis(200));

    tracing::info!("Building ordered pubkeys for epoch {epoch:04}");

    let start = Instant::now();
    let (counts, total_keys) =
        process_epoch_one_pass(cache_dir, results_dir, epoch, &pb).await?;

    let keys_path = dir.join(format!("keys-{epoch:04}.bin"));
    let pubkeys_out = dir.join(format!("registry-pubkeys-{epoch:04}.bin"));

    write_ordered_pubkeys_from_counts(&keys_path, &pubkeys_out, &counts, total_keys)?;

    // Optional: remove keys file after success
    // fs::remove_file(&keys_path).await.ok();

    // Clean up tmp file
    let _ = cleanup_keys_tmp_for_epoch(results_dir, epoch).await;

    tracing::info!(
        "Finished epoch {epoch:04} | {} keys | {:.1}s",
        total_keys,
        start.elapsed().as_secs_f64()
    );

    pb.finish_with_message(format!("epoch {epoch:04} complete | {} keys", total_keys));
    Ok(())
}

/// Simple inspection that reads first few ordered pubkeys for sanity
pub async fn inspect_pubkeys(registry_dir: &str, epoch: u64) -> Result<()> {
    let dir = Path::new(registry_dir);
    let pubkeys_path = dir.join(format!("registry-pubkeys-{epoch:04}.bin"));

    let meta = fs::metadata(&pubkeys_path)
        .await
        .with_context(|| format!("missing {}", pubkeys_path.display()))?;
    if meta.len() % 32 != 0 {
        tracing::warn!(
            "File size {} is not a multiple of 32 for {}",
            meta.len(),
            pubkeys_path.display()
        );
    }

    let mut preview = Vec::new();
    let mut reader = File::open(&pubkeys_path)
        .await
        .with_context(|| format!("open {}", pubkeys_path.display()))?;
    for order in 0..5u32 {
        let mut buf = [0u8; 32];
        match reader.read_exact(&mut buf).await {
            Ok(_) => {
                let pk = Pubkey::new_from_array(buf);
                preview.push((order, pk));
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
    }

    tracing::info!(
        "pubkeys-{:04}: {} entries | showing first {}",
        epoch,
        meta.len() / 32,
        preview.len()
    );
    for (i, pk) in preview {
        tracing::info!("order {:04} => {}", i, pk);
    }
    Ok(())
}

pub async fn inspect_registry(registry_dir: &str, epoch: u64) -> Result<()> {
    let dir = Path::new(registry_dir);
    let pubkeys_path = dir.join(format!("registry-pubkeys-{epoch:04}.bin"));

    // Check file existence
    let meta = fs::metadata(&pubkeys_path)
        .await
        .with_context(|| format!("missing {}", pubkeys_path.display()))?;
    if meta.len() % 32 != 0 {
        tracing::warn!(
            "⚠️ registry-pubkeys-{:04}.bin has unexpected size {} (not multiple of 32)",
            epoch,
            meta.len()
        );
    }

    let total_keys = (meta.len() / 32) as usize;
    if total_keys == 0 {
        tracing::info!("ℹ️ registry-pubkeys-{epoch:04}.bin is empty");
        return Ok(());
    }

    tracing::info!(
        "📘 registry-pubkeys-{epoch:04}.bin → {} keys ({} bytes)",
        total_keys,
        meta.len()
    );

    // Read a few from start and end
    let mut reader = File::open(&pubkeys_path)
        .await
        .with_context(|| format!("failed to open {}", pubkeys_path.display()))?;

    let mut preview_first = Vec::new();
    let mut buf = [0u8; 32];

    for order in 0..5u32.min(total_keys as u32) {
        if reader.read_exact(&mut buf).await.is_err() {
            break;
        }
        let pk = Pubkey::new_from_array(buf);
        preview_first.push((order, pk));
    }

    // Preview last few pubkeys
    let mut preview_last = Vec::new();
    if total_keys > 10 {
        let mut reader_tail = File::open(&pubkeys_path).await?;
        let start = (total_keys - 5) as u64 * 32;
        reader_tail.seek(SeekFrom::Start(start)).await?;
        for i in (total_keys - 5)..total_keys {
            if reader_tail.read_exact(&mut buf).await.is_err() {
                break;
            }
            let pk = Pubkey::new_from_array(buf);
            preview_last.push((i as u32, pk));
        }
    }

    // Log preview
    tracing::info!("🔹 First {} entries:", preview_first.len());
    for (order, pk) in &preview_first {
        tracing::info!("   #{:04} {}", order, pk);
    }

    if !preview_last.is_empty() {
        tracing::info!("🔹 Last {} entries:", preview_last.len());
        for (order, pk) in &preview_last {
            tracing::info!("   #{:04} {}", order, pk);
        }
    }

    tracing::info!(
        "✅ registry-pubkeys-{epoch:04}.bin looks consistent: {} keys",
        total_keys
    );
    Ok(())
}
