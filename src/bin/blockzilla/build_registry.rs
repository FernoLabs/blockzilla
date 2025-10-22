use ahash::AHashMap;
use anyhow::Result;
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hasher},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs,
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    task,
};

use crate::{
    LOG_INTERVAL_SECS, 
    file_downloader::download_epoch,
    transaction_parser::parse_account_keys_only_fast,
};

// ============================================================================
// Zero-cost hashers
// ============================================================================
#[derive(Default)]
struct PubkeyHasher(u64);

impl Hasher for PubkeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() >= 8 {
            self.0 = u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
        }
    }
}

#[derive(Default)]
struct NoOpHasher(u64);

impl Hasher for NoOpHasher {
    fn finish(&self) -> u64 { 
        self.0 
    }
    
    fn write_u64(&mut self, i: u64) { 
        self.0 = i; 
    }
    
    fn write(&mut self, _: &[u8]) { }
}

type FastPubkeyMap<V> = HashMap<Pubkey, V, BuildHasherDefault<PubkeyHasher>>;
type FastPubkeySet = HashSet<Pubkey, BuildHasherDefault<PubkeyHasher>>;
type U64Map<V> = HashMap<u64, V, BuildHasherDefault<NoOpHasher>>;

// ============================================================================
// Zero-cost pubkey fingerprint - just extract first 8 bytes
// ============================================================================
#[inline(always)]
fn pubkey_fp(k: &Pubkey) -> u64 {
    let bytes = k.as_ref();
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

// ============================================================================
// KeyStats
// ============================================================================
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[repr(C)]
pub struct KeyStats {
    pub id: u32,
    pub order_id: u32,
    pub count: u64,
    pub first_fee_payer: u32,
    pub first_slot: u64,
    pub epoch: u64,
}

// ============================================================================
// CompactKeyData with pre-allocation
// ============================================================================
struct CompactKeyData {
    counts: Vec<u32>,
    first_fee_payers: Vec<u32>,
    first_slots: Vec<u64>,
}

impl CompactKeyData {
    fn new(capacity: usize) -> Self {
        Self {
            counts: vec![0; capacity],
            first_fee_payers: vec![0; capacity],
            first_slots: vec![0; capacity],
        }
    }

    #[inline(always)]
    fn update(&mut self, idx: usize, fee_payer_id: u32, slot: u64) {
        // Safe because we pre-allocate based on key count
        if idx < self.counts.len() {
            unsafe {
                let c = self.counts.get_unchecked_mut(idx);
                *c = c.saturating_add(1);
                if *c == 1 {
                    *self.first_fee_payers.get_unchecked_mut(idx) = fee_payer_id;
                    *self.first_slots.get_unchecked_mut(idx) = slot;
                }
            }
        }
    }

    fn to_registry(self, keys_file: &Path, epoch: u64) -> Result<AHashMap<Pubkey, KeyStats>> {
        let mut reg = AHashMap::with_capacity(self.counts.len());
        let mut reader = std::fs::File::open(keys_file)?;
        let mut buf = [0u8; 32];

        for id in 0..self.counts.len() {
            if reader.read_exact(&mut buf).is_err() {
                break;
            }
            if self.counts[id] == 0 {
                continue;
            }
            let key = Pubkey::new_from_array(buf);
            reg.insert(
                key,
                KeyStats {
                    id: id as u32,
                    order_id: 0,
                    count: self.counts[id] as u64,
                    first_fee_payer: self.first_fee_payers[id],
                    first_slot: self.first_slots[id],
                    epoch,
                },
            );
        }
        Ok(reg)
    }
}

// ============================================================================
// Pass 3 – Optimized order ID assignment
// ============================================================================
fn assign_order_ids_from_arrays(
    registry: &mut AHashMap<Pubkey, KeyStats>,
    counts: &[u32],
    keys_total: usize,
) {
    // Build reverse lookup: id -> key
    let mut id_to_key: HashMap<u32, Pubkey> = HashMap::with_capacity(registry.len());
    for (key, stats) in registry.iter() {
        id_to_key.insert(stats.id, *key);
    }

    // Sort indices by count (descending)
    let mut idxs: Vec<u32> = (0..keys_total as u32).collect();
    idxs.sort_unstable_by(|&a, &b| counts[b as usize].cmp(&counts[a as usize]));

    // Assign order_ids using reverse lookup
    for (order_id, idx) in idxs.into_iter().enumerate() {
        if let Some(key) = id_to_key.get(&idx) {
            if let Some(stats) = registry.get_mut(key) {
                stats.order_id = order_id as u32;
            }
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================
async fn write_registry_to_disk<P: AsRef<Path>>(
    path: P,
    registry: &AHashMap<Pubkey, KeyStats>,
) -> Result<()> {
    let path = path.as_ref();
    let tmp = path.with_extension("tmp");
    let data = postcard::to_allocvec(registry)?;
    let f = fs::File::create(&tmp).await?;
    let mut w = BufWriter::new(f);
    w.write_all(&data).await?;
    w.flush().await?;
    fs::rename(&tmp, path).await?;
    Ok(())
}

fn has_local_epoch(cache_dir: &str, epoch: u64) -> bool {
    Path::new(&format!("{cache_dir}/epoch-{epoch}.car")).exists()
}

async fn download_epoch_to_disk(cache_dir: &str, epoch: u64) -> Result<PathBuf> {
    tokio::fs::create_dir_all(cache_dir).await.ok();
    let out = PathBuf::from(format!("{cache_dir}/epoch-{epoch}.car"));
    if !out.exists() {
        download_epoch(epoch, cache_dir, 3).await?;
    }
    Ok(out)
}

// ============================================================================
// Optimized one-pass processor
// ============================================================================
async fn process_epoch_one_pass(
    cache_dir: &str,
    results_dir: &str,
    epoch: u64,
    pb: &ProgressBar,
) -> Result<(AHashMap<Pubkey, KeyStats>, Vec<u32>, usize)> {
    let local_path = if has_local_epoch(cache_dir, epoch) {
        PathBuf::from(format!("{cache_dir}/epoch-{epoch}.car"))
    } else {
        download_epoch_to_disk(cache_dir, epoch).await?
    };

    let keys_path = Path::new(results_dir).join(format!("keys-{epoch:04}.bin"));
    let keys_tmp = keys_path.with_extension("tmp");
    let f = File::create(&keys_tmp).await?;
    // Larger buffer for better I/O performance
    let mut keys_writer = BufWriter::with_capacity(32 * 1024 * 1024, f);

    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    // Use u64 fingerprints with no-op hasher (they're already good hashes)
    let mut fp_to_id: U64Map<u32> = HashMap::with_capacity_and_hasher(
        50_000_000,
        BuildHasherDefault::default()
    );
    let mut next_id: u32 = 0;
    
    // Pre-allocate for expected key count
    let mut data = CompactKeyData::new(50_000_000);

    let start = Instant::now();
    let mut last_log = start;
    let mut blocks = 0u64;
    
    // Pre-allocate and reuse buffers
    let mut tmp_keys: SmallVec<[Pubkey; 256]> = SmallVec::new();
    let mut unique_keys = FastPubkeySet::with_capacity_and_hasher(256, BuildHasherDefault::default());
    let mut tx_buf = Vec::<u8>::with_capacity(64 * 1024); // Larger buffer
    
    // Pre-compute fee payer lookups
    let mut fee_payer_cache: U64Map<u32> = HashMap::with_capacity_and_hasher(10_000, BuildHasherDefault::default());

    // Batch key writes
    const KEY_BATCH_SIZE: usize = 2048; // Larger batches
    let mut key_batch = Vec::with_capacity(KEY_BATCH_SIZE * 32);

    while let Some(block) = car.next_block().await? {
        let block_info = block.block()?;
        for entry_cid in block_info.entries.iter() {
            let Node::Entry(entry) = block.decode(entry_cid?.hash_bytes())? else { continue };
            for tx_cid in entry.transactions.iter() {
                let tx_cid = tx_cid?;
                let Node::Transaction(tx_node) = block.decode(tx_cid.hash_bytes())? else { continue };

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
                if let Some(fee_payer) = parse_account_keys_only_fast(tx_bytes, &mut tmp_keys)? {
                    // Deduplicate using FastPubkeySet
                    unique_keys.clear();
                    unique_keys.extend(tmp_keys.iter().copied());
                    
                    let fee_fp = pubkey_fp(&fee_payer);
                    // Cache fee payer lookups
                    let fee_payer_id = *fee_payer_cache.entry(fee_fp).or_insert_with(|| {
                        *fp_to_id.get(&fee_fp).unwrap_or(&0)
                    });

                    for &k in unique_keys.iter() {
                        let fp = pubkey_fp(&k);
                        let idx = match fp_to_id.get(&fp) {
                            Some(&id) => id,
                            None => {
                                let id = next_id;
                                fp_to_id.insert(fp, id);
                                
                                // Batch key writes
                                key_batch.extend_from_slice(k.as_ref());
                                if key_batch.len() >= KEY_BATCH_SIZE * 32 {
                                    keys_writer.write_all(&key_batch).await?;
                                    key_batch.clear();
                                }
                                
                                next_id += 1;
                                id
                            }
                        };
                        data.update(idx as usize, fee_payer_id, block_info.slot);
                    }
                }
            }
        }

        blocks += 1;
        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);
            let blkps = blocks as f64 / elapsed;
            pb.set_message(format!(
                "epoch {epoch:04} | {blocks:>7} blk | {blkps:>7.1} blk/s | {keys} keys",
                keys = next_id
            ));
            last_log = now;
        }
    }

    // Flush remaining batched keys
    if !key_batch.is_empty() {
        keys_writer.write_all(&key_batch).await?;
    }
    
    keys_writer.flush().await?;
    fs::rename(&keys_tmp, &keys_path).await.ok();

    let counts = data.counts.clone();
    let registry = data.to_registry(&keys_path, epoch)?;
    
    if let Err(e) = fs::remove_file(&local_path).await {
        tracing::warn!("cleanup failed for {}: {e}", local_path.display());
    }
    
    Ok((registry, counts, next_id as usize))
}

// ============================================================================
// Parallel multi-epoch runner with better coordination
// ============================================================================
pub async fn build_registry_auto(
    cache_dir: &str,
    results_dir: &str,
    max_epoch: u64,
    workers: usize,
) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();
    let start_total = Instant::now();

    // Check which epochs are already completed
    let mut completed_epochs = HashSet::new();
    if let Ok(mut entries) = fs::read_dir(results_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(num_str) = name
                .strip_prefix("registry-")
                .and_then(|s| s.strip_suffix(".bin"))
            {
                if let Ok(num) = num_str.parse::<u64>() {
                    completed_epochs.insert(num);
                }
            }
        }
    }

    // Build work queue of remaining epochs
    let work_queue: Vec<u64> = (0..=max_epoch)
        .filter(|e| !completed_epochs.contains(e))
        .collect();
    
    if work_queue.is_empty() {
        tracing::info!("✅ All epochs already completed!");
        return Ok(());
    }

    tracing::info!(
        "📋 Processing {} epochs with {} workers (skipping {} completed)",
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

            let mut processed = 0;
            let mut failed = 0;

            loop {
                // Get next epoch from queue
                let epoch = {
                    let mut q = queue.lock().await;
                    q.next()
                };

                let Some(epoch) = epoch else {
                    break; // No more work
                };

                pb.set_message(format!("starting epoch {epoch:04}..."));

                match process_epoch_one_pass(&cache, &out, epoch, &pb).await {
                    Ok((mut reg, counts, total)) => {
                        pb.set_message(format!("epoch {epoch:04} sorting..."));
                        assign_order_ids_from_arrays(&mut reg, &counts, total);
                        
                        pb.set_message(format!("epoch {epoch:04} writing..."));
                        let path = Path::new(&out).join(format!("registry-{epoch:04}.bin"));
                        match write_registry_to_disk(&path, &reg).await {
                            Ok(_) => {
                                processed += 1;
                                pb.set_message(format!(
                                    "✅ epoch {epoch:04} | {:>8} keys | {processed} done, {failed} failed",
                                    reg.len()
                                ));
                            }
                            Err(e) => {
                                failed += 1;
                                pb.set_message(format!(
                                    "❌ epoch {epoch:04} write failed: {e} | {processed} done, {failed} failed"
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        failed += 1;
                        pb.set_message(format!(
                            "❌ epoch {epoch:04} failed: {e} | {processed} done, {failed} failed"
                        ));
                    }
                }
            }

            pb.finish_with_message(format!("✅ W{w} finished: {processed} processed, {failed} failed"));
        }));
    }

    // Wait for all workers
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

// ============================================================================
// Single-epoch entrypoint
// ============================================================================
pub async fn build_registry_single(cache_dir: &str, results_dir: &str, epoch: u64) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();
    let dir = Path::new(results_dir);

    let pb = ProgressBar::new_spinner();
    pb.set_prefix("SINGLE");
    pb.enable_steady_tick(Duration::from_millis(200));

    tracing::info!("🧩 Building registry for epoch {epoch:04}");

    let start = Instant::now();
    let (mut reg, counts, total_keys) =
        process_epoch_one_pass(cache_dir, results_dir, epoch, &pb).await?;
    assign_order_ids_from_arrays(&mut reg, &counts, total_keys);

    let out = dir.join(format!("registry-{epoch:04}.bin"));
    write_registry_to_disk(&out, &reg).await?;

    tracing::info!(
        "✅ Finished epoch {epoch:04} | {} keys | {:.1}s",
        reg.len(),
        start.elapsed().as_secs_f64()
    );

    pb.finish_with_message(format!("✅ epoch {epoch:04} complete | {} keys", reg.len()));
    Ok(())
}