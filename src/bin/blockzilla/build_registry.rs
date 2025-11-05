use ahash::AHashMap;
use anyhow::{Context, Result};
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
    io::{ErrorKind, Read},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs,
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    task,
};

use crate::{LOG_INTERVAL_SECS, transaction_parser::parse_account_keys_only};

#[derive(Default)]
struct PubkeyHasher(u64);

impl Hasher for PubkeyHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() >= 8 {
            self.0 = u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
        }
    }
}

#[derive(Default)]
struct NoOpHasher(u64);

impl Hasher for NoOpHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline(always)]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    #[inline(always)]
    fn write(&mut self, _: &[u8]) {}
}

type FastPubkeySet = HashSet<Pubkey, BuildHasherDefault<PubkeyHasher>>;
type U64Map<V> = HashMap<u64, V, BuildHasherDefault<NoOpHasher>>;

#[inline(always)]
fn pubkey_fp(k: &Pubkey) -> u64 {
    let bytes = k.as_ref();
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
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

/// Clean up ALL temporary keys files in the results directory
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
            tracing::debug!("🧹 Removed temporary file: {}", name_str);
        }
    }

    if removed > 0 {
        tracing::info!("🧹 Cleaned up {} temporary keys files", removed);
    }

    Ok(removed)
}

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

    fn into_registry(self, keys_file: &Path, epoch: u64) -> Result<AHashMap<Pubkey, KeyStats>> {
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

fn assign_order_ids_from_arrays(
    registry: &mut AHashMap<Pubkey, KeyStats>,
    counts: &[u32],
    keys_total: usize,
) -> Vec<Pubkey> {
    // Build id->key mapping once
    let mut id_to_key: HashMap<u32, Pubkey> = HashMap::with_capacity(registry.len());
    for (key, stats) in registry.iter() {
        id_to_key.insert(stats.id, *key);
    }

    // Sort indices by count (descending)
    let mut idxs: Vec<u32> = (0..keys_total as u32).collect();
    idxs.sort_unstable_by(|&a, &b| counts[b as usize].cmp(&counts[a as usize]));

    // Assign order_ids and build ordered list
    let mut ordered_keys = Vec::with_capacity(registry.len());
    for (order_id, idx) in idxs.into_iter().enumerate() {
        if let Some(key) = id_to_key.get(&idx)
            && let Some(stats) = registry.get_mut(key)
        {
            stats.order_id = order_id as u32;
            ordered_keys.push(*key);
        }
    }

    ordered_keys
}

async fn write_registry_to_disk<P: AsRef<Path>>(
    path: P,
    registry: &AHashMap<Pubkey, KeyStats>,
) -> Result<()> {
    let path = path.as_ref();
    let tmp = path.with_extension("tmp");
    remove_if_exists(&tmp).await?;

    let data = postcard::to_allocvec(registry)?;
    let f = fs::File::create(&tmp).await?;
    let mut w = BufWriter::new(f);
    w.write_all(&data).await?;
    w.flush().await?;

    if let Err(err) = fs::rename(&tmp, path).await {
        let _ = remove_if_exists(&tmp).await;
        return Err(err.into());
    }
    Ok(())
}

async fn write_ordered_pubkeys_to_disk<P: AsRef<Path>>(
    path: P,
    ordered_pubkeys: &[Pubkey],
) -> Result<()> {
    let path = path.as_ref();
    let tmp = path.with_extension("tmp");
    remove_if_exists(&tmp).await?;

    let f = fs::File::create(&tmp).await?;
    let mut w = BufWriter::new(f);
    for key in ordered_pubkeys {
        w.write_all(key.as_ref()).await?;
    }
    w.flush().await?;

    if let Err(err) = fs::rename(&tmp, path).await {
        let _ = remove_if_exists(&tmp).await;
        return Err(err.into());
    }
    Ok(())
}

async fn process_epoch_one_pass(
    cache_dir: &str,
    results_dir: &str,
    epoch: u64,
    pb: &ProgressBar,
) -> Result<(AHashMap<Pubkey, KeyStats>, Vec<u32>, usize)> {
    let keys_path = Path::new(results_dir).join(format!("keys-{epoch:04}.bin"));
    let keys_tmp = keys_path.with_extension("tmp");

    // Clean up any existing tmp file before starting
    remove_if_exists(&keys_tmp).await?;

    let f = File::create(&keys_tmp).await?;
    let mut keys_writer = BufWriter::with_capacity(32 * 1024 * 1024, f);

    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let mut fp_to_id: U64Map<u32> =
        HashMap::with_capacity_and_hasher(50_000_000, BuildHasherDefault::default());
    let mut next_id: u32 = 0;

    let mut data = CompactKeyData::new(50_000_000);

    let start = Instant::now();
    let mut last_log = start;
    let mut blocks = 0u64;

    let mut tmp_keys: SmallVec<[Pubkey; 256]> = SmallVec::new();
    let mut unique_keys =
        FastPubkeySet::with_capacity_and_hasher(256, BuildHasherDefault::default());
    let mut tx_buf = Vec::<u8>::with_capacity(64 * 1024);

    let mut fee_payer_cache: U64Map<u32> =
        HashMap::with_capacity_and_hasher(10_000, BuildHasherDefault::default());

    const KEY_BATCH_SIZE: usize = 2048;
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
                if let Some(fee_payer) = parse_account_keys_only(tx_bytes, &mut tmp_keys)? {
                    unique_keys.clear();
                    unique_keys.extend(tmp_keys.iter().copied());
                    let fee_fp = pubkey_fp(&fee_payer);
                    let fee_payer_id = *fee_payer_cache
                        .entry(fee_fp)
                        .or_insert_with(|| *fp_to_id.get(&fee_fp).unwrap_or(&0));

                    for &k in unique_keys.iter() {
                        let fp = pubkey_fp(&k);
                        let idx = match fp_to_id.get(&fp) {
                            Some(&id) => id,
                            None => {
                                let id = next_id;
                                fp_to_id.insert(fp, id);

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

    // Flush remaining keys
    if !key_batch.is_empty() {
        keys_writer.write_all(&key_batch).await?;
    }
    keys_writer.flush().await?;

    // Atomic rename from tmp to final file
    if let Err(err) = fs::rename(&keys_tmp, &keys_path).await {
        let _ = remove_if_exists(&keys_tmp).await;
        return Err(err.into());
    }

    let counts = data.counts.clone();
    let registry = data.into_registry(&keys_path, epoch)?;

    Ok((registry, counts, next_id as usize))
}

pub async fn build_registry_auto(
    cache_dir: &str,
    results_dir: &str,
    max_epoch: u64,
    workers: usize,
) -> Result<()> {
    fs::create_dir_all(results_dir).await.ok();

    // Clean up any leftover tmp files from previous runs
    let cleaned = cleanup_all_keys_tmp(results_dir).await?;
    if cleaned > 0 {
        tracing::info!("🧹 Cleaned up {} stale temporary files", cleaned);
    }

    let start_total = Instant::now();

    // Find completed epochs
    let mut completed_epochs = HashSet::new();
    if let Ok(mut entries) = fs::read_dir(results_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(num_str) = name
                .strip_prefix("registry-")
                .and_then(|s| s.strip_suffix(".bin"))
                && let Ok(num) = num_str.parse::<u64>()
            {
                completed_epochs.insert(num);
            }
        }
    }

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
                let epoch = {
                    let mut q = queue.lock().await;
                    q.next()
                };

                let Some(epoch) = epoch else {
                    break;
                };

                pb.set_message(format!("starting epoch {epoch:04}..."));

                match process_epoch_one_pass(&cache, &out, epoch, &pb).await {
                    Ok((mut reg, counts, total)) => {
                        pb.set_message(format!("epoch {epoch:04} sorting..."));
                        let ordered_pubkeys =
                            assign_order_ids_from_arrays(&mut reg, &counts, total);

                        pb.set_message(format!("epoch {epoch:04} writing..."));
                        let registry_path = Path::new(&out).join(format!("registry-{epoch:04}.bin"));
                        let pubkeys_path =
                            Path::new(&out).join(format!("registry-pubkeys-{epoch:04}.bin"));

                        let write_res = async {
                            write_registry_to_disk(&registry_path, &reg).await?;
                            write_ordered_pubkeys_to_disk(&pubkeys_path, &ordered_pubkeys).await?;
                            Ok::<(), anyhow::Error>(())
                        }
                        .await;

                        match write_res {
                            Ok(_) => {
                                // Clean up tmp file for this epoch
                                let _ = cleanup_keys_tmp_for_epoch(&out, epoch).await;

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

            pb.finish_with_message(format!(
                "✅ W{w} finished: {processed} processed, {failed} failed"
            ));
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    // Final cleanup of any remaining tmp files
    let final_cleaned = cleanup_all_keys_tmp(results_dir).await?;
    if final_cleaned > 0 {
        tracing::info!(
            "🧹 Final cleanup: removed {} temporary files",
            final_cleaned
        );
    }

    tracing::info!(
        "🏁 Finished all epochs ({} workers) in {:.1}s",
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

    tracing::info!("🧩 Building registry for epoch {epoch:04}");

    let start = Instant::now();
    let (mut reg, counts, total_keys) =
        process_epoch_one_pass(cache_dir, results_dir, epoch, &pb).await?;
    let ordered_pubkeys = assign_order_ids_from_arrays(&mut reg, &counts, total_keys);

    let registry_out = dir.join(format!("registry-{epoch:04}.bin"));
    let pubkeys_out = dir.join(format!("registry-pubkeys-{epoch:04}.bin"));
    write_registry_to_disk(&registry_out, &reg).await?;
    write_ordered_pubkeys_to_disk(&pubkeys_out, &ordered_pubkeys).await?;

    // Clean up tmp file
    let _ = cleanup_keys_tmp_for_epoch(results_dir, epoch).await;

    tracing::info!(
        "✅ Finished epoch {epoch:04} | {} keys | {:.1}s",
        reg.len(),
        start.elapsed().as_secs_f64()
    );

    pb.finish_with_message(format!("✅ epoch {epoch:04} complete | {} keys", reg.len()));
    Ok(())
}

pub async fn inspect_registry(registry_dir: &str, epoch: u64) -> Result<()> {
    let dir = Path::new(registry_dir);
    let registry_path = dir.join(format!("registry-{epoch:04}.bin"));
    let pubkeys_path = dir.join(format!("registry-pubkeys-{epoch:04}.bin"));

    let registry_bytes = fs::read(&registry_path)
        .await
        .with_context(|| format!("failed to read {}", registry_path.display()))?;
    let registry: AHashMap<Pubkey, KeyStats> = postcard::from_bytes(&registry_bytes)
        .with_context(|| format!("failed to decode registry {}", registry_path.display()))?;

    if registry.is_empty() {
        tracing::info!("ℹ️ registry {epoch:04} is empty");
        return Ok(());
    }

    let mut total_count = 0u64;
    let mut zero_count = 0usize;
    let mut max_order_id = 0u32;
    let mut top: Vec<(Pubkey, KeyStats)> = Vec::new();

    for (pk, stats) in &registry {
        let stats = *stats;
        total_count = total_count.saturating_add(stats.count);
        if stats.count == 0 {
            zero_count += 1;
        }
        if stats.order_id > max_order_id {
            max_order_id = stats.order_id;
        }

        if top.len() < 5 {
            top.push((*pk, stats));
        } else if let Some((idx, _)) = top
            .iter()
            .enumerate()
            .min_by_key(|(_, entry)| entry.1.count)
            && stats.count > top[idx].1.count
        {
            top[idx] = (*pk, stats);
        }
    }

    top.sort_by(|a, b| b.1.count.cmp(&a.1.count));

    let expected_bytes = (registry.len() as u64) * 32;
    let pubkeys_meta = fs::metadata(&pubkeys_path)
        .await
        .with_context(|| format!("missing registry pubkeys file {}", pubkeys_path.display()))?;
    if pubkeys_meta.len() != expected_bytes {
        tracing::warn!(
            "⚠️ registry-pubkeys-{:04}.bin size {} does not match expected {}",
            epoch,
            pubkeys_meta.len(),
            expected_bytes
        );
    }

    let mut preview = Vec::new();
    let mut reader = File::open(&pubkeys_path)
        .await
        .with_context(|| format!("failed to open {}", pubkeys_path.display()))?;
    for order in 0..5u32 {
        let mut buf = [0u8; 32];
        match reader.read_exact(&mut buf).await {
            Ok(_) => {
                let pk = Pubkey::new_from_array(buf);
                preview.push((order, pk));
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                break;
            }
            Err(err) => return Err(err.into()),
        }
    }

    tracing::info!(
        "ℹ️ registry {epoch:04}: {} keys | total uses {} | max order id {}",
        registry.len(),
        total_count,
        max_order_id
    );

    if zero_count > 0 {
        tracing::info!("   • {zero_count} keys have zero usage counts");
    }

    if max_order_id + 1 != registry.len() as u32 {
        tracing::warn!(
            "   • order ids are not contiguous: max {} vs {} keys",
            max_order_id,
            registry.len()
        );
    }

    if !top.is_empty() {
        tracing::info!("   • top keys by usage count:");
        for (pk, stats) in &top {
            tracing::info!(
                "     #{:04} {} | count {} | first slot {} | first fee payer {}",
                stats.order_id,
                pk,
                stats.count,
                stats.first_slot,
                stats.first_fee_payer
            );
        }
    }

    if !preview.is_empty() {
        tracing::info!("   • first {} entries in registry-pubkeys: ", preview.len());
        for (order, pk) in &preview {
            match registry.get(pk) {
                Some(stats) => {
                    tracing::info!(
                        "     order {:04} => {} (id {} count {})",
                        order,
                        pk,
                        stats.id,
                        stats.count
                    );
                }
                None => {
                    tracing::warn!(
                        "     order {:04} => {} (missing from registry map)",
                        order,
                        pk
                    );
                }
            }
        }
    }

    Ok(())
}
