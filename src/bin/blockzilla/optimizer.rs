use ahash::AHashMap;
use anyhow::{Context, Result, anyhow};
use blockzilla::{
    car_block_reader::CarBlockReader,
    open_epoch::{self, FetchMode},
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use memmap2::Mmap;
use std::{
    collections::HashSet,
    error::Error as StdError,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, File},
    io::{AsyncWriteExt, BufWriter},
    task,
};

use crate::LOG_INTERVAL_SECS;
use crate::build_registry::{epoch_dir, fp128_from_bytes, keys_bin};
use blockzilla::carblock_to_compact::{CompactBlock, carblock_to_compactblock};

/// ========================================================================
/// Paths (optimizer-specific)
/// ========================================================================
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
/// Soft EOF detection (same behavior as registry)
/// ========================================================================
fn is_soft_eof(e: &anyhow::Error) -> bool {
    let mut cur: Option<&(dyn StdError + 'static)> = Some(e.as_ref());
    while let Some(err) = cur {
        if let Some(ioe) = err.downcast_ref::<std::io::Error>() {
            if ioe.kind() == std::io::ErrorKind::UnexpectedEof {
                return true;
            }
        }
        let msg = err.to_string();
        if msg.contains("unexpected EOF") || msg.contains("without BlockNode") {
            return true;
        }
        cur = err.source();
    }
    false
}

/// ========================================================================
/// Registry index: build fp128→usage_id from keys.bin (usage order)
/// ========================================================================
struct RegistryIndex {
    _mmap: Mmap, // keep mmap alive
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
        let mut key = [0u8; 32];
        let mut off = 0usize;
        for i in 0..n {
            key.copy_from_slice(&mmap[off..off + 32]);
            off += 32;
            let fp = fp128_from_bytes(&key);
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

/// ========================================================================
/// Optimize an epoch using usage-ordered registry (keys.bin only)
/// Writes a length-prefixed stream of `CompactBlock` (postcard)
/// ========================================================================
pub async fn optimize_epoch(
    cache_dir: &str,
    registry_dir: &str, // registry root containing epoch-####/keys.bin
    out_base: &str,     // outputs go to epoch-####/optimized
    epoch: u64,
    include_metadata: bool,
) -> Result<()> {
    let outdir = optimized_dir(Path::new(out_base), epoch);
    fs::create_dir_all(&outdir).await?;

    let blocks_path = optimized_blocks_file(&outdir);

    // Build fp128→usage_id map from keys.bin
    let reg = RegistryIndex::open(Path::new(registry_dir), epoch)
        .with_context(|| "open usage-ordered registry (keys.bin)")?;
    tracing::info!(
        "optimizer: registry epoch {epoch:04} loaded with {} usage ids",
        reg.total()
    );

    // Stream CAR and write compact frames
    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let tmp_path = unique_tmp(&outdir, "blocks");
    let mut out = BufWriter::with_capacity(8 << 20, File::create(&tmp_path).await?);

    let start = Instant::now();
    let mut last_log = start;
    let mut blocks = 0u64;
    let mut msg = String::with_capacity(128);

    'epoch: while let Some(block) = car.next_block().await? {
        // Ensure the block node exists, but keep soft-EOF tolerant behavior
        if let Err(e) = block.block() {
            if is_soft_eof(&e) {
                break 'epoch;
            } else {
                // treat as fatal like your previous strict path on registry misses, etc.
                return Err(e);
            }
        }

        // Convert whole block using the carblock_to_compact pipeline
        let compact: CompactBlock =
            match carblock_to_compactblock(&block, &reg.map, include_metadata) {
                Ok(cb) => cb,
                Err(e) => {
                    if is_soft_eof(&e) {
                        break 'epoch;
                    }
                    // Keep strict behavior on real errors (for example, registry miss)
                    return Err(e);
                }
            };

        // Serialize as length-prefixed postcard frame
        let bytes =
            postcard::to_allocvec(&compact).map_err(|e| anyhow!("serialize compact block: {e}"))?;
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
                "opt epoch {epoch:04} | {blocks:>7} blk | {blkps:>7.1} blk/s"
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

    tracing::info!(
        "optimizer: wrote {} (postcard CompactBlock stream, registry = keys.bin)",
        blocks_path.display()
    );
    Ok(())
}

/// Optimize multiple epochs concurrently
pub async fn optimize_auto(
    cache_dir: &str,
    registry_dir: &str,
    out_base: &str,
    max_epoch: u64,
    workers: usize,
    include_metadata: bool,
) -> Result<()> {
    fs::create_dir_all(out_base).await.ok();

    // already optimized if blocks.bin present
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
                match optimize_epoch(&cache, &reg, &out, e, include_metadata).await {
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

/// Back-compat wrapper expected by main.rs (`run_car_optimizer`)
pub async fn run_car_optimizer(
    cache_dir: &str,
    epoch: u64,
    optimized_dir: &str,
    registry_dir: Option<&str>,
    include_metadata: bool,
) -> anyhow::Result<()> {
    let registry_root = registry_dir.unwrap_or(optimized_dir);
    tracing::info!("Optimizing epoch {epoch:04}");
    let start = Instant::now();
    optimize_epoch(
        cache_dir,
        registry_root,
        optimized_dir,
        epoch,
        include_metadata,
    )
    .await?;
    tracing::info!(
        "Done epoch {epoch:04} in {:.1}s",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}
