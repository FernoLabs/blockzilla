use anyhow::{Context, Result, anyhow};
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::io::Read;
use std::{
    cmp::max,
    collections::HashSet,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    task,
};

use crate::{LOG_INTERVAL_SECS, transaction_parser::parse_account_keys_only};

/// ========================================================================
/// Public compact wire types (postcard-serialized, length-prefixed frames)
/// ========================================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactInstruction {
    /// usage-ordered account id from the registry snapshot
    pub account_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_len: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactReward {
    pub account_id: u32,
    pub lamports: i64,
    pub reward_type: u8,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactTokenBalance {
    pub account_id: u32,
    pub amount: i128,
    pub mint_id: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlockWithIds {
    pub slot: u64,
    pub instructions: Vec<CompactInstruction>,
    pub rewards: Vec<CompactReward>,
    pub token_balances: Vec<CompactTokenBalance>,
}

/// ========================================================================
/// Paths
/// ========================================================================
fn epoch_dir(base: &Path, epoch: u64) -> PathBuf {
    base.join(format!("epoch-{epoch:04}"))
}
fn keys_bin(dir: &Path) -> PathBuf {
    dir.join("keys.bin") // arrival order, raw 32B pubkeys
}
fn usage_map_bin(dir: &Path) -> PathBuf {
    dir.join("order_ids.map") // u32 array: usage_id -> arrival_id
}
fn optimized_dir(base: &Path, epoch: u64) -> PathBuf {
    base.join(format!("epoch-{epoch:04}/optimized"))
}
fn optimized_blocks_file(dir: &Path) -> PathBuf {
    dir.join("blocks.bin")
}
fn unique_tmp(dir: &Path, name: &str) -> PathBuf {
    let pid = std::process::id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    dir.join(format!("{name}.tmp.{pid}.{ts}"))
}

/// ========================================================================
/// Fingerprint + FlatTable (u128 -> u32)
/// ========================================================================
#[inline(always)]
fn fp128_from_bytes(b: &[u8; 32]) -> u128 {
    u128::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15],
    ])
}

struct FlatTable {
    mask: usize, // capacity - 1
    len: usize,
    keys: Vec<u128>,
    vals: Vec<u32>,
    occ: Vec<u64>, // bitset
}
const EMPTY_KEY: u128 = 0;

impl FlatTable {
    fn with_capacity_for(expected_items: usize, target_load: f64) -> Self {
        let min_cap = ((expected_items as f64) / target_load).ceil() as usize;
        let cap = next_pow2(max(1024, min_cap));
        let words = (cap + 63) / 64;
        FlatTable {
            mask: cap - 1,
            len: 0,
            keys: vec![EMPTY_KEY; cap],
            vals: vec![0u32; cap],
            occ: vec![0u64; words],
        }
    }
    #[inline]
    fn capacity(&self) -> usize {
        self.mask + 1
    }
    #[inline]
    fn len(&self) -> usize {
        self.len
    }
    #[inline]
    fn is_occupied(&self, idx: usize) -> bool {
        let w = idx >> 6;
        let b = idx & 63;
        (self.occ[w] >> b) & 1 == 1
    }
    #[inline]
    fn set_occupied(&mut self, idx: usize) {
        let w = idx >> 6;
        let b = idx & 63;
        self.occ[w] |= 1u64 << b;
    }
    #[inline]
    fn hash_idx(&self, k: u128) -> usize {
        let lo = k as u64;
        let hi = (k >> 64) as u64;
        let x = lo ^ hi.rotate_left(23) ^ hi.wrapping_mul(0x9E3779B97F4A7C15);
        (((x as u128).wrapping_mul(0x9E3779B97F4A7C15u128) >> 64) as usize) & self.mask
    }
    fn insert_exact(&mut self, fp: u128, usage_id: u32) {
        let mut idx = self.hash_idx(fp);
        loop {
            if !self.is_occupied(idx) {
                self.keys[idx] = fp;
                self.vals[idx] = usage_id;
                self.set_occupied(idx);
                self.len += 1;
                return;
            } else if self.keys[idx] == fp {
                debug_assert_eq!(
                    self.vals[idx], usage_id,
                    "fingerprint collision in registry map"
                );
                return;
            }
            idx = (idx + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, fp: u128) -> Option<u32> {
        let mut idx = self.hash_idx(fp);
        loop {
            if !self.is_occupied(idx) {
                return None;
            }
            if self.keys[idx] == fp {
                return Some(self.vals[idx]);
            }
            idx = (idx + 1) & self.mask;
        }
    }
}
#[inline(always)]
fn next_pow2(x: usize) -> usize {
    x.next_power_of_two()
}

/// ========================================================================
/// Mutable registry snapshot (baseline + appended unknown keys)
/// ========================================================================
struct MutableRegistry {
    // baseline
    base_keys_mmap: Mmap, // arrival-ordered baseline keys
    base_order: Vec<u8>,  // usage_id -> arrival_id (u32 LE)
    base_len: usize,      // number of existing usage ids / keys

    // lookup from fingerprint -> usage_id
    map: FlatTable,

    // newly discovered keys during optimize
    new_keys: Vec<[u8; 32]>,
}

impl MutableRegistry {
    fn open(registry_dir: &Path, epoch: u64) -> Result<Self> {
        let edir = epoch_dir(registry_dir, epoch);
        let p_keys = keys_bin(&edir);
        let p_map = usage_map_bin(&edir);

        // baseline keys
        let in_keys = std::fs::OpenOptions::new()
            .read(true)
            .open(&p_keys)
            .with_context(|| format!("open {}", p_keys.display()))?;
        let keys_mmap = unsafe { Mmap::map(&in_keys)? };
        if keys_mmap.len() % 32 != 0 {
            anyhow::bail!("keys.bin size not multiple of 32");
        }
        let n = keys_mmap.len() / 32;

        // baseline order
        let order_bytes =
            std::fs::read(&p_map).with_context(|| format!("read {}", p_map.display()))?;
        if order_bytes.len() != n * 4 {
            anyhow::bail!(
                "order_ids.map size mismatch: got {}, expected {}",
                order_bytes.len(),
                n * 4
            );
        }

        // build fp -> usage_id from baseline
        let mut table = FlatTable::with_capacity_for(n, 0.65);
        for usage_id in 0..n {
            let off = usage_id * 4;
            let arrival = u32::from_le_bytes([
                order_bytes[off],
                order_bytes[off + 1],
                order_bytes[off + 2],
                order_bytes[off + 3],
            ]) as usize;
            let mut key = [0u8; 32];
            key.copy_from_slice(&keys_mmap[arrival * 32..arrival * 32 + 32]);
            let fp = fp128_from_bytes(&key);
            table.insert_exact(fp, usage_id as u32);
        }

        Ok(Self {
            base_keys_mmap: keys_mmap,
            base_order: order_bytes,
            base_len: n,
            map: table,
            new_keys: Vec::with_capacity(1 << 20),
        })
    }

    #[inline(always)]
    fn get_or_insert_usage_id(&mut self, key32: &[u8; 32]) -> u32 {
        let fp = fp128_from_bytes(key32);
        if let Some(id) = self.map.get(fp) {
            return id;
        }
        // assign a new id (append to end)
        let id = (self.base_len + self.new_keys.len()) as u32;
        self.new_keys.push(*key32);
        self.map.insert_exact(fp, id);
        id
    }

    /// Commit updated snapshot into `optimized/` (keys.bin + order_ids.map).
    /// Does NOT modify the baseline registry dir.
    fn commit_snapshot(&self, optimized_epoch_dir: &Path) -> Result<()> {
        std::fs::create_dir_all(optimized_epoch_dir)
            .with_context(|| format!("create {}", optimized_epoch_dir.display()))?;

        // keys.bin' = baseline keys + new keys
        let out_keys = optimized_epoch_dir.join("keys.bin");
        {
            let mut w = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&out_keys)
                .with_context(|| format!("create {}", out_keys.display()))?;
            // baseline
            std::io::copy(&mut &*self.base_keys_mmap, &mut w)
                .with_context(|| "copy baseline keys")?;
            // appended
            for k in &self.new_keys {
                w.write_all(k)?;
            }
            w.flush()?;
        }

        // order_ids.map' = baseline order + identity for appended range
        let out_map = optimized_epoch_dir.join("order_ids.map");
        {
            let mut mw = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&out_map)
                .with_context(|| format!("create {}", out_map.display()))?;
            mw.write_all(&self.base_order)?;
            let start_arrival = self.base_len as u32;
            for i in 0..(self.new_keys.len() as u32) {
                let arrival = start_arrival + i;
                mw.write_all(&arrival.to_le_bytes())?;
            }
            mw.flush()?;
        }

        Ok(())
    }

    #[inline(always)]
    fn appended_count(&self) -> usize {
        self.new_keys.len()
    }
}

/// ========================================================================
/// Optimize an epoch: remap pubkeys to usage_id, auto-extend registry snapshot
/// ========================================================================
pub async fn optimize_epoch(
    cache_dir: &str,
    registry_dir: &str, // baseline registry lives here
    out_base: &str,     // optimized output (and snapshot) lives here
    epoch: u64,
) -> Result<()> {
    let outdir = optimized_dir(Path::new(out_base), epoch);
    fs::create_dir_all(&outdir).await?;

    let blocks_path = optimized_blocks_file(&outdir);
    // We always rewrite blocks.bin; if you want to skip when present, early-return here.

    // open mutable registry on baseline (will append unknown keys in-memory)
    let mut reg = MutableRegistry::open(Path::new(registry_dir), epoch)
        .with_context(|| "open baseline registry")?;
    tracing::info!(
        "optimizer registry baseline: {} ids; snapshot target: {}",
        reg.base_len,
        outdir.display()
    );

    // Stream CAR and write compact frames
    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let tmp_path = unique_tmp(&outdir, "blocks");
    let mut out = BufWriter::with_capacity(8 << 20, File::create(&tmp_path).await?);

    let mut tmp_keys: SmallVec<[Pubkey; 256]> = SmallVec::new();
    let mut tx_buf = Vec::<u8>::with_capacity(128 * 1024);

    let start = Instant::now();
    let mut last_log = start;
    let mut blocks = 0u64;
    let mut msg = String::with_capacity(128);

    while let Some(block) = car.next_block().await? {
        let bnode = block.block()?;
        let slot = bnode.slot;

        let mut compact_instructions: Vec<CompactInstruction> = Vec::new();
        let compact_rewards: Vec<CompactReward> = Vec::new();
        let compact_balances: Vec<CompactTokenBalance> = Vec::new();

        for entry_cid in bnode.entries.iter() {
            let Node::Entry(entry) = block.decode(entry_cid?.hash_bytes())? else {
                continue;
            };
            for tx_cid in entry.transactions.iter() {
                let tcid = tx_cid?;
                let Node::Transaction(tx_node) = block.decode(tcid.hash_bytes())? else {
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
                let parsed = parse_account_keys_only(tx_bytes, &mut tmp_keys)?;
                if parsed.is_none() {
                    continue;
                }

                for k in tmp_keys.iter() {
                    let key = k.to_bytes();
                    let uid = reg.get_or_insert_usage_id(&key);
                    compact_instructions.push(CompactInstruction {
                        account_id: uid,
                        ..Default::default()
                    });
                }
            }
        }

        let bw = BlockWithIds {
            slot,
            instructions: compact_instructions,
            rewards: compact_rewards,
            token_balances: compact_balances,
        };
        let bytes =
            postcard::to_allocvec(&bw).map_err(|e| anyhow!("serialize compact block: {e}"))?;
        out.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
        out.write_all(&bytes).await?;

        blocks += 1;
        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            msg.clear();
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);
            let blkps = blocks as f64 / elapsed;
            use std::fmt::Write as _;
            let _ = write!(
                &mut msg,
                "opt epoch {epoch:04} | {blocks:>7} blk | {blkps:>7.1} blk/s | new keys {}",
                reg.appended_count()
            );
            tracing::info!("{}", msg);
            last_log = now;
        }
    }

    out.flush().await?;
    if fs::metadata(&blocks_path).await.is_ok() {
        fs::remove_file(&tmp_path).await.ok();
    } else {
        fs::rename(&tmp_path, &blocks_path).await?;
    }

    // Commit registry snapshot into optimized/
    reg.commit_snapshot(&outdir)
        .with_context(|| "commit updated registry snapshot into optimized/")?;
    tracing::info!(
        "registry snapshot committed into {} (+{} new keys)",
        outdir.display(),
        reg.appended_count()
    );

    Ok(())
}

/// Optimize multiple epochs concurrently (writes blocks + snapshot under `out_base/epoch-####/optimized`)
pub async fn optimize_auto(
    cache_dir: &str,
    registry_dir: &str,
    out_base: &str,
    max_epoch: u64,
    workers: usize,
) -> Result<()> {
    fs::create_dir_all(out_base).await.ok();

    // detect already optimized (blocks.bin present)
    let mut completed = HashSet::new();
    if let Ok(mut rd) = fs::read_dir(out_base).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            let p = ent.path();
            if p.is_dir() {
                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if let Some(num) = name
                        .strip_prefix("epoch-")
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        if fs::metadata(optimized_blocks_file(&p.join("optimized")))
                            .await
                            .is_ok()
                        {
                            completed.insert(num);
                        }
                    }
                }
            }
        }
    }

    let work: Vec<u64> = (0..=max_epoch).filter(|e| !completed.contains(e)).collect();
    if work.is_empty() {
        tracing::info!("All epochs already optimized");
        return Ok(());
    }

    let mp = Arc::new(MultiProgress::new());
    let style = ProgressStyle::with_template("[{elapsed_precise}] {prefix:>4} | {msg}").unwrap();
    let queue = Arc::new(tokio::sync::Mutex::new(work.into_iter()));
    let mut handles = Vec::new();

    for w in 0..workers {
        let cache = cache_dir.to_string();
        let reg = registry_dir.to_string();
        let out = out_base.to_string();
        let q = queue.clone();
        let pb = mp.add(ProgressBar::new_spinner());
        pb.set_prefix(format!("O{w}"));
        pb.set_style(style.clone());
        pb.enable_steady_tick(Duration::from_millis(100));

        handles.push(task::spawn(async move {
            loop {
                let epoch = {
                    let mut g = q.lock().await;
                    g.next()
                };
                let Some(e) = epoch else { break };
                pb.set_message(format!("optimize epoch {e:04}..."));
                match optimize_epoch(&cache, &reg, &out, e).await {
                    Ok(_) => pb.set_message(format!("ok epoch {e:04}")),
                    Err(err) => pb.set_message(format!("fail epoch {e:04}: {err}")),
                }
            }
            pb.finish_with_message("done");
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    Ok(())
}

/// Optimize a single epoch with log wrapper.
pub async fn optimize_single(
    cache_dir: &str,
    registry_dir: &str,
    out_base: &str,
    epoch: u64,
) -> Result<()> {
    tracing::info!("Optimizing epoch {epoch:04}");
    let start = Instant::now();
    optimize_epoch(cache_dir, registry_dir, out_base, epoch).await?;
    tracing::info!(
        "Done epoch {epoch:04} in {:.1}s",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Back-compat wrapper expected by main.rs (`run_car_optimizer`)
pub async fn run_car_optimizer(
    cache_dir: &str,
    epoch: u64,
    optimized_dir: &str,
    registry_dir: Option<&str>,
) -> anyhow::Result<()> {
    let registry_root = registry_dir.unwrap_or(optimized_dir);
    optimize_epoch(cache_dir, registry_root, optimized_dir, epoch).await
}
