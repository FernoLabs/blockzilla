use ahash::AHashMap;
use anyhow::{anyhow, Context, Result};
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use memmap2::Mmap;
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::{
    collections::HashSet,
    io::{ErrorKind, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
    task,
};
use std::io::Read;

use crate::{LOG_INTERVAL_SECS, transaction_parser::parse_account_keys_only};

/// -------- Path helpers (per-epoch directories) --------

fn epoch_dir(base: &Path, epoch: u64) -> PathBuf {
    base.join(format!("epoch-{epoch:04}"))
}
fn keys_bin(dir: &Path) -> PathBuf {
    dir.join("keys.bin")
}
fn pubkeys_bin(dir: &Path) -> PathBuf {
    dir.join("registry-pubkeys.bin")
}
fn lock_path(dir: &Path) -> PathBuf {
    dir.join("epoch.lock")
}
fn unique_tmp_keys(dir: &Path) -> PathBuf {
    let pid = std::process::id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    dir.join(format!("keys.tmp.{pid}.{ts}"))
}

/// Optional per-epoch lock for multi-process safety.
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

/// -------- Fingerprint + index structures --------

#[inline(always)]
fn fp128_from_bytes(b: &[u8; 32]) -> u128 {
    u128::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
    ])
}

/// Collision-safe entry: stores full 32B key for verification.
enum IndexEntry {
    /// Fast path: only one key for this fp
    Single { id: u32, key: [u8; 32] },
    /// Slow path: multiple distinct keys sharing the same fp (rare). Inline cap 4.
    Multi(SmallVec<[([u8; 32], u32); 4]>),
}
impl IndexEntry {
    #[inline(always)]
    fn get_or_insert(&mut self, key_bytes: &[u8; 32], next_id: &mut u32) -> (u32, bool) {
        // returns (id, is_new_id)
        match self {
            IndexEntry::Single { id, key } => {
                if key == key_bytes {
                    (*id, false)
                } else {
                    let old = (*key, *id);
                    let new_id = *next_id;
                    *next_id += 1;
                    *self = IndexEntry::Multi(smallvec::smallvec![old, (*key_bytes, new_id)]);
                    (new_id, true)
                }
            }
            IndexEntry::Multi(list) => {
                if let Some((_, id)) = list.iter().find(|(k, _)| k == key_bytes) {
                    (*id, false)
                } else {
                    let new_id = *next_id;
                    *next_id += 1;
                    list.push((*key_bytes, new_id));
                    (new_id, true)
                }
            }
        }
    }
}

/// -------- Compact counters --------

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
            // grow in chunks to reduce reallocs
            let new_len = (idx + 1).max(self.counts.len().saturating_add(100_000));
            self.counts.resize(new_len, 0);
        }
    }
    #[inline(always)]
    fn inc(&mut self, idx: usize) {
        self.ensure_len(idx);
        // SAFETY: ensured above
        unsafe {
            *self.counts.get_unchecked_mut(idx) += 1;
        }
    }
    #[inline(always)]
    fn into_counts_truncated(mut self, new_len: usize) -> Vec<u32> {
        if self.counts.len() > new_len {
            self.counts.truncate(new_len);
        }
        self.counts
    }
}

/// -------- Final writer: order by counts, stream keys via mmap --------

fn write_ordered_pubkeys_from_counts(
    keys_file: &Path,
    out_file: &Path,
    counts: &[u32],
    total_keys: usize,
) -> Result<()> {
    // Build indices 0..N-1 and sort by count desc (counts.len() == total_keys).
    let mut idxs: Vec<u32> = Vec::with_capacity(total_keys);
    idxs.extend(0..total_keys as u32);
    let c = &counts;
    idxs.sort_unstable_by(|&a, &b| c[b as usize].cmp(&c[a as usize]));

    // mmap input for fast reads; write output sequentially
    let in_f = std::fs::OpenOptions::new()
        .read(true)
        .open(keys_file)
        .with_context(|| format!("open {}", keys_file.display()))?;
    let mmap = unsafe { Mmap::map(&in_f).with_context(|| "mmap keys file")? };
    let expected_size = total_keys * 32;
    if mmap.len() != expected_size {
        return Err(anyhow!(
            "keys file size mismatch: {} vs expected {}",
            mmap.len(),
            expected_size
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

/// -------- One-pass scanner per epoch --------
/// Returns (counts, total_unique_keys).
async fn process_epoch_one_pass(
    cache_dir: &str,
    results_dir: &str,
    epoch: u64,
    pb: &ProgressBar,
) -> Result<(Vec<u32>, usize)> {
    // Per-epoch dir + files
    let base = Path::new(results_dir);
    let edir = epoch_dir(base, epoch);
    fs::create_dir_all(&edir).await?;

    let keys_final = keys_bin(&edir);
    let keys_tmp = unique_tmp_keys(&edir);

    // Optional lock (safe multi-process)
    let _lock = match try_epoch_lock(&edir).await? {
        Some(f) => Some(f),
        None => {
            pb.set_message(format!(
                "epoch {epoch:04} skipped (locked by another process)"
            ));
            return Ok((Vec::new(), 0));
        }
    };

    // If keys.bin already exists, skip scanning
    if fs::metadata(&keys_final).await.is_ok() {
        release_epoch_lock(&edir).await;
        return Ok((Vec::new(), 0));
    }

    // Writer for canonical 32B keys
    let f = File::create(&keys_tmp).await?;
    let mut keys_writer = BufWriter::new(f);
    // Batched append of 32B keys (reduce syscalls)
    const KEY_BATCH_SIZE: usize = 16_384; // 512 KiB per batch
    let mut key_batch = Vec::with_capacity(KEY_BATCH_SIZE * 32);

    // Read CAR/epoch
    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    // fp -> entry (u128) — hardcoded 50M capacity as requested
    let mut fp_index: AHashMap<u128, IndexEntry> = AHashMap::with_capacity(50_000_000);
    let mut next_id: u32 = 0;

    let mut data = CompactKeyData::new();

    let start = Instant::now();
    let mut last_log = start;
    let mut blocks = 0u64;

    let mut tmp_keys: SmallVec<[Pubkey; 256]> = SmallVec::new();
    let mut tx_buf = Vec::<u8>::with_capacity(128 * 1024);

    let mut msg = String::with_capacity(128);
    #[cfg(debug_assertions)]
    let mut collisions = 0u64;

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

                // Pull tx bytes
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

                // Extract & dedup static account keys by full bytes
                tmp_keys.clear();
                if parse_account_keys_only(tx_bytes, &mut tmp_keys)?.is_some() {
                    // Dedup by full key – sorting by slice avoids extra array copies in comparator
                    tmp_keys.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
                    tmp_keys.dedup_by_key(|k| *k);

                    for &k in tmp_keys.iter() {
                        // One 32B array: reuse for fp + file write
                        let key_bytes = k.to_bytes();
                        let fp = fp128_from_bytes(&key_bytes);

                        match fp_index.get_mut(&fp) {
                            Some(entry) => {
                                // Existing fingerprint: might be same key, or a true collision bucket
                                let before = next_id;
                                let (id, is_new) = entry.get_or_insert(&key_bytes, &mut next_id);
                                if is_new {
                                    // ✅ BUGFIX: append bytes for new ID even in collision bucket
                                    key_batch.extend_from_slice(&key_bytes);
                                    if key_batch.len() >= KEY_BATCH_SIZE * 32 {
                                        keys_writer.write_all(&key_batch).await?;
                                        key_batch.clear();
                                    }
                                    #[cfg(debug_assertions)]
                                    { collisions += 1; }
                                }
                                data.inc(id as usize);
                            }
                            None => {
                                // New fingerprint: definitely a brand-new key/id
                                let id = next_id;
                                next_id += 1;
                                fp_index.insert(fp, IndexEntry::Single { id, key: key_bytes });

                                key_batch.extend_from_slice(&key_bytes);
                                if key_batch.len() >= KEY_BATCH_SIZE * 32 {
                                    keys_writer.write_all(&key_batch).await?;
                                    key_batch.clear();
                                }
                                data.inc(id as usize);
                            }
                        }
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
            use std::fmt::Write as _;
            #[cfg(debug_assertions)]
            {
                let _ = write!(
                    &mut msg,
                    "epoch {epoch:04} | {blocks:>7} blk | {blkps:>7.1} blk/s | {keys:>8} keys | {collisions} collisions",
                    keys = next_id
                );
            }
            #[cfg(not(debug_assertions))]
            {
                let _ = write!(
                    &mut msg,
                    "epoch {epoch:04} | {blocks:>7} blk | {blkps:>7.1} blk/s | {keys:>8} keys",
                    keys = next_id
                );
            }
            pb.set_message(std::mem::take(&mut msg));
            last_log = now;
        }
    }

    // Flush remaining batch
    if !key_batch.is_empty() {
        keys_writer.write_all(&key_batch).await?;
    }
    keys_writer.flush().await?;

    // Atomic finalize: tmp -> keys.bin (unless another process already wrote it)
    if fs::metadata(&keys_final).await.is_ok() {
        let _ = fs::remove_file(&keys_tmp).await;
        release_epoch_lock(&edir).await;
        let total = next_id as usize;
        // Trim counts to exact size for later sort
        let counts = data.into_counts_truncated(total);
        return Ok((counts, total));
    }
    if let Err(err) = fs::rename(&keys_tmp, &keys_final).await {
        let _ = fs::remove_file(&keys_tmp).await;
        release_epoch_lock(&edir).await;
        return Err(err.into());
    }

    release_epoch_lock(&edir).await;

    // Trim counts to exact size for later sort
    let total = next_id as usize;
    let counts = data.into_counts_truncated(total);
    Ok((counts, total))
}

/// -------- Multi-epoch orchestration --------

pub async fn build_registry_auto(
    cache_dir: &str,
    results_dir: &str,
    max_epoch: u64,
    workers: usize,
) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();

    // Discover completed epochs (presence of registry-pubkeys.bin)
    let mut completed = HashSet::new();
    if let Ok(mut rd) = fs::read_dir(results_dir).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            let p = ent.path();
            if !p.is_dir() {
                continue;
            }
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if let Some(num) = name
                    .strip_prefix("epoch-")
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    if fs::metadata(pubkeys_bin(&p)).await.is_ok() {
                        completed.insert(num);
                    }
                }
            }
        }
    }

    let work_queue: Vec<u64> = (0..=max_epoch).filter(|e| !completed.contains(e)).collect();
    if work_queue.is_empty() {
        tracing::info!("✅ All epochs already completed");
        return Ok(());
    }

    tracing::info!(
        "📋 Processing {} epochs with {} workers (skipping {})",
        work_queue.len(),
        workers,
        completed.len()
    );

    let start_total = Instant::now();

    let queue = Arc::new(tokio::sync::Mutex::new(work_queue.into_iter()));
    let multi = Arc::new(MultiProgress::new());
    let style = ProgressStyle::with_template("[{elapsed_precise}] {prefix:>4} | {msg}").unwrap();

    let mut handles = Vec::new();
    for w in 0..workers {
        let cache = cache_dir.to_string();
        let out = results_dir.to_string();
        let mp = multi.clone();
        let sty = style.clone();
        let q = queue.clone();

        handles.push(task::spawn(async move {
            let pb = mp.add(ProgressBar::new_spinner());
            pb.set_prefix(format!("W{w}"));
            pb.set_style(sty);
            pb.enable_steady_tick(Duration::from_millis(100));

            let mut done = 0usize;
            let mut failed = 0usize;

            loop {
                let epoch = {
                    let mut guard = q.lock().await;
                    guard.next()
                };
                let Some(epoch) = epoch else { break };

                pb.set_message(format!("starting epoch {epoch:04}..."));

                match process_epoch_one_pass(&cache, &out, epoch, &pb).await {
                    Ok((counts, total)) => {
                        // If total==0 it means we skipped due to existing keys.bin or lock.
                        let edir = epoch_dir(Path::new(&out), epoch);
                        let keys_final = keys_bin(&edir);
                        let pubkeys_final = pubkeys_bin(&edir);

                        let sort_res = (|| -> Result<()> {
                            if total > 0 && !counts.is_empty() {
                                // 1) Build the sorted pubkeys
                                write_ordered_pubkeys_from_counts(
                                    &keys_final,
                                    &pubkeys_final,
                                    &counts,
                                    total,
                                )?;
                                // 2) Delete keys.bin to save space (per your request)
                                std::fs::remove_file(&keys_final).ok();
                            }
                            Ok(())
                        })();

                        match sort_res {
                            Ok(_) => {
                                done += 1;
                                pb.set_message(format!(
                                    "✅ epoch {epoch:04} | {:>8} keys | {done} ok, {failed} fail",
                                    total
                                ));
                            }
                            Err(e) => {
                                failed += 1;
                                pb.set_message(format!(
                                    "❌ epoch {epoch:04} sort/write failed: {e} | {done} ok, {failed} fail"
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        failed += 1;
                        pb.set_message(format!(
                            "❌ epoch {epoch:04} failed: {e} | {done} ok, {failed} fail"
                        ));
                    }
                }
            }

            pb.finish_with_message(format!("✅ W{w} finished: {done} ok, {failed} fail"));
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    tracing::info!(
        "🏁 Finished all epochs ({} workers) in {:.1}s",
        workers,
        start_total.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Single-epoch helper (handy for retries / debugging).
pub async fn build_registry_single(cache_dir: &str, results_dir: &str, epoch: u64) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();

    let pb = ProgressBar::new_spinner();
    pb.set_prefix("SINGLE");
    pb.enable_steady_tick(Duration::from_millis(200));

    tracing::info!("🧩 Building ordered pubkeys for epoch {epoch:04}");

    let start = Instant::now();
    let (counts, total) = process_epoch_one_pass(cache_dir, results_dir, epoch, &pb).await?;

    let edir = epoch_dir(Path::new(results_dir), epoch);
    let keys_final = keys_bin(&edir);
    let pubkeys_final = pubkeys_bin(&edir);

    if total > 0 && !counts.is_empty() {
        write_ordered_pubkeys_from_counts(&keys_final, &pubkeys_final, &counts, total)?;
        // Delete keys.bin after success
        std::fs::remove_file(&keys_final).ok();
    }

    tracing::info!(
        "✅ Finished epoch {epoch:04} | {} keys | {:.1}s",
        total,
        start.elapsed().as_secs_f64()
    );
    pb.finish_with_message(format!("✅ epoch {epoch:04} complete | {} keys", total));
    Ok(())
}

/// Inspect the pubkeys file with head/tail preview and basic checks.
pub async fn inspect_registry(registry_dir: &str, epoch: u64) -> Result<()> {
    let edir = epoch_dir(Path::new(registry_dir), epoch);
    let path = pubkeys_bin(&edir);

    // Check file existence and shape
    let meta = fs::metadata(&path)
        .await
        .with_context(|| format!("missing {}", path.display()))?;
    if meta.len() % 32 != 0 {
        tracing::warn!(
            "⚠️ registry-pubkeys.bin has unexpected size {} (not multiple of 32)",
            meta.len()
        );
    }

    let total_keys = (meta.len() / 32) as usize;
    if total_keys == 0 {
        tracing::info!("ℹ️ registry-pubkeys.bin is empty for epoch {epoch:04}");
        return Ok(());
    }

    tracing::info!(
        "📘 registry-pubkeys.bin (epoch {epoch:04}) → {} keys ({} bytes)",
        total_keys,
        meta.len()
    );

    // Head
    let mut head_reader = File::open(&path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;
    let mut buf = [0u8; 32];
    let mut preview_first = Vec::new();
    for order in 0..5u32.min(total_keys as u32) {
        if head_reader.read_exact(&mut buf).await.is_err() {
            break;
        }
        let pk = Pubkey::new_from_array(buf);
        preview_first.push((order, pk));
    }

    // Tail
    let mut preview_last = Vec::new();
    if total_keys > 10 {
        let mut tail_reader = File::open(&path).await?;
        let start = (total_keys - 5) as u64 * 32;
        tail_reader.seek(SeekFrom::Start(start)).await?;
        for i in (total_keys - 5)..total_keys {
            if tail_reader.read_exact(&mut buf).await.is_err() {
                break;
            }
            let pk = Pubkey::new_from_array(buf);
            preview_last.push((i as u32, pk));
        }
    }

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
        "✅ registry-pubkeys.bin looks consistent for epoch {epoch:04}: {} keys",
        total_keys
    );
    Ok(())
}
