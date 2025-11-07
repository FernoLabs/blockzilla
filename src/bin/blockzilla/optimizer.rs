use ahash::AHashMap;
use anyhow::{Context, Result, anyhow};
use blockzilla::{
    car_block_reader::CarBlockReader,
    open_epoch::{self, FetchMode},
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use memmap2::Mmap;
use postcard::to_allocvec;
use solana_pubkey::Pubkey;
use std::{
    collections::HashSet,
    error::Error as StdError,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    io::BufWriter,
    task,
};

use crate::LOG_INTERVAL_SECS;
use crate::build_registry::{epoch_dir, fp128_from_bytes, keys_bin};
use blockzilla::carblock_to_compact::{
    CompactBlock, CompactMetadataPayload, MetadataMode, PubkeyIdProvider, StaticPubkeyIdProvider,
    carblock_to_compactblock_inplace,
};

use blockzilla::compact_log::{CompactLogStream, EncodeConfig, encode_logs};

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

async fn write_dynamic_keys(dir: &Path, provider: &DynamicPubkeyIdProvider) -> Result<()> {
    let keys_path = keys_bin(dir);
    let tmp_path = unique_tmp(dir, "keys");

    let mut writer = BufWriter::with_capacity(16 << 20, File::create(&tmp_path).await?);
    for key in provider.keys() {
        writer.write_all(key).await?;
    }
    writer.flush().await?;

    if fs::metadata(&keys_path).await.is_ok() {
        let _ = fs::remove_file(&keys_path).await;
    }
    fs::rename(&tmp_path, &keys_path).await?;
    Ok(())
}

fn is_soft_eof(e: &anyhow::Error) -> bool {
    let mut cur: Option<&(dyn StdError + 'static)> = Some(e.as_ref());
    while let Some(err) = cur {
        if let Some(ioe) = err.downcast_ref::<std::io::Error>()
            && ioe.kind() == std::io::ErrorKind::UnexpectedEof
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

struct RegistryIndex {
    _mmap: Mmap,
    map: AHashMap<u128, u32>,
    total: usize,
}

impl RegistryIndex {
    fn open(registry_dir: &Path, epoch: u64) -> Result<Self> {
        let edir = epoch_dir(registry_dir, epoch);
        let keys_path = keys_bin(&edir);
        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(&keys_path)
            .with_context(|| format!("open {}", keys_path.display()))?;
        let mmap = unsafe { Mmap::map(&f)? };
        if mmap.len() % 32 != 0 {
            return Err(anyhow!("keys.bin length is not multiple of 32"));
        }
        let n = mmap.len() / 32;

        let mut map: AHashMap<u128, u32> = AHashMap::with_capacity(n);
        let mut off = 0usize;
        for i in 0..n {
            // SAFETY: we validated the length above
            let key_bytes: &[u8; 32] = (&mmap[off..off + 32]).try_into().unwrap();
            off += 32;
            let fp = fp128_from_bytes(key_bytes);
            map.insert(fp, i as u32);
        }

        Ok(Self {
            _mmap: mmap,
            map,
            total: n,
        })
    }

    #[inline(always)]
    fn total(&self) -> usize {
        self.total
    }
}

#[inline]
fn push_block_into_batch(batch: &mut Vec<u8>, cb: &CompactBlock) -> anyhow::Result<()> {
    let bytes = to_allocvec(cb).map_err(|e| anyhow!("serialize compact block: {e}"))?;

    batch.reserve(4 + bytes.len());
    batch.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    batch.extend_from_slice(&bytes);
    Ok(())
}

fn compact_logs_inplace<P: PubkeyIdProvider>(
    cblk: &mut CompactBlock,
    resolver: &mut P,
    enable: bool,
) {
    if !enable {
        return;
    }

    let mut lookup_pid = |base58: &str| -> Option<u32> {
        Pubkey::try_from(base58)
            .ok()
            .and_then(|pk| resolver.resolve(&pk.to_bytes()))
    };

    for tx in &mut cblk.txs {
        let Some(CompactMetadataPayload::Compact(ref mut meta)) = tx.metadata else {
            continue;
        };
        let Some(ref logs) = meta.log_messages else {
            continue;
        };

        let cls: CompactLogStream = encode_logs(logs, &mut lookup_pid, EncodeConfig::default());

        const TAG: &[u8] = b"CLZ\0";
        if let Ok(mut blob) = postcard::to_allocvec(&cls) {
            let mut tagged = Vec::with_capacity(TAG.len() + blob.len());
            tagged.extend_from_slice(TAG);
            tagged.append(&mut blob);

            match meta.return_data {
                Some(ref mut rd) => {
                    rd.data = tagged;
                }
                None => {
                    meta.return_data = Some(blockzilla::carblock_to_compact::CompactReturnData {
                        program_id_id: 0,
                        data: tagged,
                    });
                }
            }

            meta.log_messages = None;
        } else {
            tracing::warn!("compact_logs: postcard encode failed; keeping original logs");
        }
    }
}

#[derive(Default)]
struct DynamicPubkeyIdProvider {
    fp_to_id: AHashMap<u128, u32>,
    collisions: Option<AHashMap<[u8; 32], u32>>,
    ids: Vec<[u8; 32]>,
}

impl DynamicPubkeyIdProvider {
    fn new() -> Self {
        Self {
            fp_to_id: AHashMap::with_capacity(1 << 17),
            collisions: None,
            ids: Vec::with_capacity(1 << 17),
        }
    }

    fn keys(&self) -> &[[u8; 32]] {
        self.ids.as_slice()
    }
}

impl PubkeyIdProvider for DynamicPubkeyIdProvider {
    fn resolve(&mut self, key: &[u8; 32]) -> Option<u32> {
        let fp = fp128_from_bytes(key);
        if let Some(&id) = self.fp_to_id.get(&fp) {
            if let Some(stored) = self.ids.get(id as usize)
                && stored == key
            {
                return Some(id);
            }
            if let Some(extra) = &self.collisions
                && let Some(&cid) = extra.get(key)
            {
                return Some(cid);
            }
        } else if let Some(extra) = &self.collisions
            && let Some(&cid) = extra.get(key)
        {
            return Some(cid);
        }

        if self.ids.len() >= u32::MAX as usize {
            return None;
        }

        let id = self.ids.len() as u32;
        self.ids.push(*key);
        if self.fp_to_id.contains_key(&fp) {
            self.collisions
                .get_or_insert_with(|| AHashMap::with_capacity(4))
                .insert(*key, id);
        } else {
            self.fp_to_id.insert(fp, id);
        }
        Some(id)
    }
}

pub async fn optimize_epoch_without_registry(
    cache_dir: &str,
    out_base: &str,
    epoch: u64,
    metadata_mode: MetadataMode,
) -> Result<()> {
    let outdir = optimized_dir(Path::new(out_base), epoch);
    fs::create_dir_all(&outdir).await?;

    let blocks_path = optimized_blocks_file(&outdir);

    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let tmp_path = unique_tmp(&outdir, "blocks");

    const OUT_BUF_CAP: usize = 128 << 20;
    const BATCH_TARGET: usize = 128 << 20;
    const BATCH_SOFT_MAX: usize = 160 << 20;

    let mut out = BufWriter::with_capacity(OUT_BUF_CAP, File::create(&tmp_path).await?);
    let mut frame_batch: Vec<u8> = Vec::with_capacity(BATCH_TARGET + (8 << 20));

    let mut cblk = CompactBlock {
        slot: 0,
        txs: Vec::with_capacity(512),
        rewards: Vec::with_capacity(64),
    };
    let mut buf_tx = Vec::<u8>::with_capacity(128 << 10);
    let mut buf_meta = Vec::<u8>::with_capacity(128 << 10);
    let mut provider = DynamicPubkeyIdProvider::new();

    let start = Instant::now();
    let mut last_log = start;
    let mut blocks = 0u64;
    let mut msg = String::with_capacity(128);

    let enable_log_compaction = matches!(metadata_mode, MetadataMode::Compact);

    'epoch: while let Some(block) = car.next_block().await? {
        if let Err(e) = block.block() {
            if is_soft_eof(&e) {
                break 'epoch;
            } else {
                return Err(e);
            }
        }

        if let Err(e) = carblock_to_compactblock_inplace(
            &block,
            &mut provider,
            metadata_mode,
            &mut buf_tx,
            &mut buf_meta,
            &mut cblk,
        ) {
            if is_soft_eof(&e) {
                break 'epoch;
            }
            return Err(e);
        }

        compact_logs_inplace(&mut cblk, &mut provider, enable_log_compaction);

        push_block_into_batch(&mut frame_batch, &cblk)?;

        if frame_batch.len() >= BATCH_TARGET || frame_batch.len() >= BATCH_SOFT_MAX {
            out.write_all(&frame_batch).await?;
            frame_batch.clear();
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
                "opt-nr epoch {epoch:04} | {blocks:>7} blk | {blkps:>7.1} blk/s",
            );
            tracing::info!("{}", msg);
            last_log = now;
        }
    }

    if !frame_batch.is_empty() {
        out.write_all(&frame_batch).await?;
        frame_batch.clear();
    }

    out.flush().await?;

    if fs::metadata(&blocks_path).await.is_ok() {
        let _ = fs::remove_file(&blocks_path).await;
    }
    fs::rename(&tmp_path, &blocks_path).await?;

    write_dynamic_keys(&outdir, &provider).await?;

    tracing::info!(
        "optimizer: wrote {} (postcard CompactBlock stream, assigned {} unique keys)",
        blocks_path.display(),
        provider.keys().len()
    );

    Ok(())
}

pub async fn optimize_epoch(
    cache_dir: &str,
    registry_dir: &str,
    out_base: &str,
    epoch: u64,
    metadata_mode: MetadataMode,
) -> Result<()> {
    let outdir = optimized_dir(Path::new(out_base), epoch);
    fs::create_dir_all(&outdir).await?;

    let blocks_path = optimized_blocks_file(&outdir);

    let reg = RegistryIndex::open(Path::new(registry_dir), epoch)
        .with_context(|| "open usage-ordered registry (keys.bin)")?;
    tracing::info!(
        "optimizer: registry epoch {epoch:04} loaded with {} usage ids",
        reg.total()
    );

    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let tmp_path = unique_tmp(&outdir, "blocks");

    const OUT_BUF_CAP: usize = 128 << 20;
    const BATCH_TARGET: usize = 128 << 20;
    const BATCH_SOFT_MAX: usize = 160 << 20;

    let mut out = BufWriter::with_capacity(OUT_BUF_CAP, File::create(&tmp_path).await?);
    let mut frame_batch: Vec<u8> = Vec::with_capacity(BATCH_TARGET + (8 << 20));

    let mut cblk = CompactBlock {
        slot: 0,
        txs: Vec::with_capacity(512),
        rewards: Vec::with_capacity(64),
    };
    let mut buf_tx = Vec::<u8>::with_capacity(128 << 10);
    let mut buf_meta = Vec::<u8>::with_capacity(128 << 10);

    let start = Instant::now();
    let mut last_log = start;
    let mut blocks = 0u64;
    let mut msg = String::with_capacity(128);

    let enable_log_compaction = matches!(metadata_mode, MetadataMode::Compact);
    let mut id_provider = StaticPubkeyIdProvider::new(&reg.map);

    'epoch: while let Some(block) = car.next_block().await? {
        if let Err(e) = block.block() {
            if is_soft_eof(&e) {
                break 'epoch;
            } else {
                return Err(e);
            }
        }

        if let Err(e) = carblock_to_compactblock_inplace(
            &block,
            &mut id_provider,
            metadata_mode,
            &mut buf_tx,
            &mut buf_meta,
            &mut cblk,
        ) {
            if is_soft_eof(&e) {
                break 'epoch;
            }
            return Err(e);
        }

        compact_logs_inplace(&mut cblk, &mut id_provider, enable_log_compaction);

        push_block_into_batch(&mut frame_batch, &cblk)?;

        if frame_batch.len() >= BATCH_TARGET || frame_batch.len() >= BATCH_SOFT_MAX {
            out.write_all(&frame_batch).await?;
            frame_batch.clear();
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
                "opt epoch {epoch:04} | {blocks:>7} blk | {blkps:>7.1} blk/s"
            );
            tracing::info!("{}", msg);
            last_log = now;
        }
    }

    if !frame_batch.is_empty() {
        out.write_all(&frame_batch).await?;
        frame_batch.clear();
    }

    out.flush().await?;

    if fs::metadata(&blocks_path).await.is_ok() {
        let _ = fs::remove_file(&blocks_path).await;
    }
    fs::rename(&tmp_path, &blocks_path).await?;

    tracing::info!(
        "optimizer: wrote {} (postcard CompactBlock stream, registry = keys.bin)",
        blocks_path.display()
    );
    Ok(())
}

#[allow(dead_code)]
pub async fn optimize_auto(
    cache_dir: &str,
    registry_dir: &str,
    out_base: &str,
    max_epoch: u64,
    workers: usize,
    metadata_mode: MetadataMode,
) -> Result<()> {
    fs::create_dir_all(out_base).await.ok();

    let mut completed = HashSet::new();
    if let Ok(mut rd) = fs::read_dir(out_base).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            let p = ent.path();
            if p.is_dir()
                && let Some(name) = p.file_name().and_then(|s| s.to_str())
                && let Some(num) = name
                    .strip_prefix("epoch-")
                    .and_then(|s| s.parse::<u64>().ok())
                && fs::metadata(optimized_blocks_file(&p.join("optimized")))
                    .await
                    .is_ok()
            {
                completed.insert(num);
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
                match optimize_epoch(&cache, &reg, &out, e, metadata_mode).await {
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

pub async fn run_car_optimizer(
    cache_dir: &str,
    epoch: u64,
    optimized_dir: &str,
    registry_dir: Option<&str>,
    metadata_mode: MetadataMode,
) -> anyhow::Result<()> {
    let registry_root = registry_dir.unwrap_or(optimized_dir);
    tracing::info!("Optimizing epoch {epoch:04}");
    let start = Instant::now();
    optimize_epoch(
        cache_dir,
        registry_root,
        optimized_dir,
        epoch,
        metadata_mode,
    )
    .await?;
    tracing::info!(
        "Done epoch {epoch:04} in {:.1}s",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}
