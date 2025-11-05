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
    cmp::max,
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

// ============================================================================
// Config
// ============================================================================

/// Expected unique keys per epoch. You said at least 30M.
const EXPECTED_UNIQUE_KEYS: usize = 30_000_000;

/// Target max load factor for the open addressing table.
const TARGET_LOAD_FACTOR: f64 = 0.47;

/// Batch size for writing pubkeys to disk.
const KEY_BATCH_SIZE: usize = 32_768;

// ============================================================================
// Paths
// ============================================================================

fn epoch_dir(base: &Path, epoch: u64) -> PathBuf {
    base.join(format!("epoch-{epoch:04}"))
}
fn keys_bin(dir: &Path) -> PathBuf {
    dir.join("keys.bin") // arrival order, raw 32B pubkeys
}
fn usage_map_bin(dir: &Path) -> PathBuf {
    dir.join("order_ids.map") // u32 array: usage_rank -> arrival_id
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

// ============================================================================
// Fingerprint
// ============================================================================

#[inline(always)]
fn fp128_from_bytes(b: &[u8; 32]) -> u128 {
    u128::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
    ])
}

// ============================================================================
// Compact open addressing table for u128 -> u32 (arrival id)
// Layout: parallel arrays + occupancy bitset, power-of-two capacity, linear probing
// ============================================================================

struct FlatTable {
    mask: usize,            // capacity - 1
    len: usize,             // number of elements
    keys: Vec<u128>,        // size = capacity, unused slots hold EMPTY_KEY
    ids: Vec<u32>,          // size = capacity, valid only when slot occupied
    occ: Vec<u64>,          // bitset, 1 means occupied
}

const EMPTY_KEY: u128 = 0; // sentinel for unused slot, safe because we also guard with occ bit

impl FlatTable {
    fn with_capacity_for(expected_items: usize, target_load: f64) -> Self {
        let min_cap = ((expected_items as f64) / target_load).ceil() as usize;
        let cap = next_pow2(max(1024, min_cap));
        let words = (cap + 63) / 64;
        Self {
            mask: cap - 1,
            len: 0,
            keys: vec![EMPTY_KEY; cap],
            ids: vec![0u32; cap],
            occ: vec![0u64; words],
        }
    }

    #[inline(always)]
    fn capacity(&self) -> usize {
        self.mask + 1
    }

    #[inline(always)]
    fn is_occupied(&self, idx: usize) -> bool {
        let w = idx >> 6;
        let b = idx & 63;
        (self.occ[w] >> b) & 1 == 1
    }

    #[inline(always)]
    fn set_occupied(&mut self, idx: usize) {
        let w = idx >> 6;
        let b = idx & 63;
        self.occ[w] |= 1u64 << b;
    }

    /// Very cheap 128->64 mix, then Fibonacci hashing for power-of-two table.
    #[inline(always)]
    fn hash_idx(&self, k: u128) -> usize {
        let lo = k as u64;
        let hi = (k >> 64) as u64;
        let x = lo ^ hi.rotate_left(23) ^ hi.wrapping_mul(0x9E3779B97F4A7C15);
        // Fibonacci hashing
        (((x as u128).wrapping_mul(0x9E3779B97F4A7C15u128) >> 64) as usize) & self.mask
    }

    /// Insert or get the id for key `k`. Returns (id, inserted).
    #[inline(always)]
    fn insert_or_get(&mut self, k: u128, next_id: &mut u32) -> (u32, bool) {
        let mut idx = self.hash_idx(k);
        // linear probing
        loop {
            if !self.is_occupied(idx) {
                // empty slot
                self.keys[idx] = k;
                let id = *next_id;
                *next_id = id + 1;
                self.ids[idx] = id;
                self.set_occupied(idx);
                self.len += 1;
                return (id, true);
            } else {
                // occupied, check match
                if self.keys[idx] == k {
                    return (self.ids[idx], false);
                }
                idx = (idx + 1) & self.mask;
            }
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }
}

#[inline(always)]
fn next_pow2(x: usize) -> usize {
    x.next_power_of_two()
}

// ============================================================================
// Registry build: one pass with FlatTable + counts in Vec<u32>
// ============================================================================

pub async fn process_epoch_one_pass(
    cache_dir: &str,
    results_dir: &str,
    epoch: u64,
    pb: &ProgressBar,
) -> Result<(usize /*total_keys*/,)> {
    let base = Path::new(results_dir);
    let edir = epoch_dir(base, epoch);
    fs::create_dir_all(&edir).await?;

    let keys_final = keys_bin(&edir);
    let usage_map_final = usage_map_bin(&edir);

    // If mapping already exists, epoch is done
    if fs::metadata(&usage_map_final).await.is_ok() && fs::metadata(&keys_final).await.is_ok() {
        pb.set_message(format!("epoch {epoch:04} already complete"));
        return Ok((0,));
    }

    let _lock = match try_epoch_lock(&edir).await? {
        Some(f) => Some(f),
        None => {
            pb.set_message(format!("epoch {epoch:04} skipped (locked by another process)"));
            return Ok((0,));
        }
    };

    // Prepare keys writer if keys.bin does not exist yet
    let have_keys = fs::metadata(&keys_final).await.is_ok();
    let keys_tmp = if have_keys { None } else { Some(unique_tmp_file(&edir, "keys")) };
    let mut keys_writer = if let Some(ref p) = keys_tmp {
        let f = File::create(p).await?;
        Some(BufWriter::with_capacity(128 * 1024 * 1024, f))
    } else {
        None
    };

    // Open CAR for counting pass
    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    // FlatTable sized for at least 30M unique keys
    let mut table = FlatTable::with_capacity_for(EXPECTED_UNIQUE_KEYS, TARGET_LOAD_FACTOR);
    // counts are per arrival id, so we will grow it as we insert new keys
    let mut counts: Vec<u32> = Vec::with_capacity(EXPECTED_UNIQUE_KEYS);

    let mut next_id: u32 = 0;

    let mut blocks = 0u64;
    let start = Instant::now();
    let mut last_log = start;
    let mut msg = String::with_capacity(128);

    // Working buffers
    let mut tmp_keys: SmallVec<[Pubkey; 256]> = SmallVec::new();
    let mut tx_buf = Vec::<u8>::with_capacity(128 * 1024);
    let mut key_batch = Vec::with_capacity(KEY_BATCH_SIZE * 32);

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

                tmp_keys.clear();
                if parse_account_keys_only(tx_bytes, &mut tmp_keys)?.is_some() {
                    tmp_keys.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
                    tmp_keys.dedup_by_key(|k| *k);

                    for &k in tmp_keys.iter() {
                        let key_bytes = k.to_bytes();
                        let fp = fp128_from_bytes(&key_bytes);

                        let (id, inserted) = table.insert_or_get(fp, &mut next_id);
                        if inserted {
                            // first time: ensure counts slot and set to 1
                            counts.push(1);
                            if let Some(w) = keys_writer.as_mut() {
                                key_batch.extend_from_slice(&key_bytes);
                                if key_batch.len() >= KEY_BATCH_SIZE * 32 {
                                    w.write_all(&key_batch).await?;
                                    key_batch.clear();
                                }
                            }
                        } else {
                            // bump count
                            // SAFETY: id is arrival index, counts was pushed on first insert
                            unsafe {
                                let slot = counts.get_unchecked_mut(id as usize);
                                *slot = slot.saturating_add(1);
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
            let _ = write!(
                &mut msg,
                "epoch {epoch:04} | {blocks:>7} blk | {blkps:>7.1} blk/s | {keys:>8} keys",
                keys = table.len()
            );
            pb.set_message(std::mem::take(&mut msg));
            last_log = now;
        }
    }

    // Finish keys writer if we created it
    if let Some(w) = keys_writer.as_mut() {
        if !key_batch.is_empty() {
            w.write_all(&key_batch).await?;
            key_batch.clear();
        }
        w.flush().await?;
    }
    if let Some(p) = keys_tmp.as_ref() {
        if fs::metadata(&keys_final).await.is_ok() {
            fs::remove_file(p).await.ok();
        } else {
            fs::rename(p, &keys_final).await?;
        }
    }

    let total = table.len();
    if total == 0 {
        release_epoch_lock(&edir).await;
        return Ok((0,));
    }

    // Build usage order by sorting indices by counts desc
    let mut order: Vec<u32> = (0..total as u32).collect();
    order.sort_unstable_by(|&a, &b| {
        let ca = unsafe { *counts.get_unchecked(a as usize) };
        let cb = unsafe { *counts.get_unchecked(b as usize) };
        cb.cmp(&ca).then_with(|| a.cmp(&b))
    });

    // Write order_ids.map
    let usage_map_tmp = unique_tmp_file(&edir, "order_ids");
    {
        let out = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&usage_map_tmp)
            .with_context(|| format!("create {}", usage_map_tmp.display()))?;
        let mut w = std::io::BufWriter::with_capacity(128 * 1024 * 1024, out);
        for &arrival in &order {
            w.write_all(&arrival.to_le_bytes())?;
        }
        w.flush()?;
    }
    if fs::metadata(&usage_map_final).await.is_ok() {
        fs::remove_file(&usage_map_tmp).await.ok();
    } else {
        fs::rename(&usage_map_tmp, &usage_map_final).await?;
    }

    release_epoch_lock(&edir).await;
    Ok((total,))
}

// ============================================================================
// Orchestration
// ============================================================================

pub async fn build_registry_auto(
    cache_dir: &str,
    results_dir: &str,
    max_epoch: u64,
    workers: usize,
) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();

    // Consider an epoch "done" if both keys.bin and order_ids.map exist
    let mut completed = HashSet::new();
    if let Ok(mut rd) = fs::read_dir(results_dir).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            let p = ent.path();
            if !p.is_dir() {
                continue;
            }
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if let Some(num) = name.strip_prefix("epoch-").and_then(|s| s.parse::<u64>().ok()) {
                    let keys_exists = fs::metadata(keys_bin(&p)).await.is_ok();
                    let map_exists = fs::metadata(usage_map_bin(&p)).await.is_ok();
                    if keys_exists && map_exists {
                        completed.insert(num);
                    }
                }
            }
        }
    }

    let work_queue: Vec<u64> = (0..=max_epoch).filter(|e| !completed.contains(e)).collect();
    if work_queue.is_empty() {
        tracing::info!("All epochs already completed");
        return Ok(());
    }

    tracing::info!(
        "Processing {} epochs with {} workers (skipping {})",
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
                    Ok((total,)) => {
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

            pb.finish_with_message(format!("W{w} finished: {done} ok, {failed} fail"));
        }));
    }

    for h in handles {
        let _ = h.await;
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

    let pb = ProgressBar::new_spinner();
    pb.set_prefix("SINGLE");
    pb.enable_steady_tick(Duration::from_millis(200));

    tracing::info!("Building order_ids.map for epoch {epoch:04}");

    let start = Instant::now();
    let (total,) = process_epoch_one_pass(cache_dir, results_dir, epoch, &pb).await?;

    tracing::info!(
        "Finished epoch {epoch:04} | {} keys | {:.1}s",
        total,
        start.elapsed().as_secs_f64()
    );
    pb.finish_with_message(format!("epoch {epoch:04} complete | {} keys", total));
    Ok(())
}

pub async fn inspect_registry(registry_dir: &str, epoch: u64) -> Result<()> {
    let edir = epoch_dir(Path::new(registry_dir), epoch);
    let keys_path = keys_bin(&edir);
    let map_path = usage_map_bin(&edir);

    let keys_meta = fs::metadata(&keys_path)
        .await
        .with_context(|| format!("missing {}", keys_path.display()))?;
    if keys_meta.len() % 32 != 0 {
        tracing::warn!(
            "⚠️ keys.bin has unexpected size {} (not multiple of 32)",
            keys_meta.len()
        );
    }
    let total_keys = (keys_meta.len() / 32) as usize;

    let map_meta = fs::metadata(&map_path)
        .await
        .with_context(|| format!("missing {}", map_path.display()))?;
    if map_meta.len() % 4 != 0 {
        tracing::warn!(
            "⚠️ order_ids.map has unexpected size {} (not multiple of 4)",
            map_meta.len()
        );
    }
    let total_map = (map_meta.len() / 4) as usize;

    if total_keys == 0 {
        tracing::info!("ℹ️ empty keys.bin for epoch {epoch:04}");
        return Ok(());
    }
    if total_keys != total_map {
        tracing::warn!(
            "⚠️ size mismatch: {} keys but {} usage entries",
            total_keys,
            total_map
        );
    }

    tracing::info!(
        "📘 epoch {epoch:04}: keys.bin → {} keys ({} bytes), order_ids.map → {} entries ({} bytes)",
        total_keys, keys_meta.len(), total_map, map_meta.len()
    );

    // Preview: first/last few arrival-ordered keys
    let mut head_reader = File::open(&keys_path)
        .await
        .with_context(|| format!("failed to open {}", keys_path.display()))?;
    let mut buf = [0u8; 32];
    let mut preview_first = Vec::new();
    for order in 0..5u32.min(total_keys as u32) {
        if head_reader.read_exact(&mut buf).await.is_err() {
            break;
        }
        let pk = Pubkey::new_from_array(buf);
        preview_first.push((order, pk));
    }

    let mut preview_last = Vec::new();
    if total_keys > 10 {
        let mut tail_reader = File::open(&keys_path).await?;
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

    tracing::info!("🔹 First {} arrival entries:", preview_first.len());
    for (order, pk) in &preview_first {
        tracing::info!("   #{:04} {}", order, pk);
    }
    if !preview_last.is_empty() {
        tracing::info!("🔹 Last {} arrival entries:", preview_last.len());
        for (order, pk) in &preview_last {
            tracing::info!("   #{:04} {}", order, pk);
        }
    }

    tracing::info!(
        "✅ registry artifacts look consistent for epoch {epoch:04}: {} unique keys",
        total_keys
    );
    Ok(())
}
