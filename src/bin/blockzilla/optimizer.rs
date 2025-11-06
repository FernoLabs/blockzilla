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
    CompactBlock, CompactMetadataPayload, MetadataMode, carblock_to_compactblock_inplace,
};

// If your compact log module lives elsewhere, adjust this use accordingly.
use blockzilla::compact_log::{CompactLogStream, EncodeConfig, encode_logs};

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

/// ========================================================================
/// Postcard: write length-prefixed frames into a batch buffer (simple/robust)
/// ========================================================================
#[inline]
fn push_block_into_batch(batch: &mut Vec<u8>, cb: &CompactBlock) -> anyhow::Result<()> {
    // Serialize to a temporary Vec (keeps code straightforward; the big win
    // here is that `cb` and all scratch buffers are reused across iterations).
    let bytes = to_allocvec(cb).map_err(|e| anyhow!("serialize compact block: {e}"))?;

    // Length prefix + payload
    batch.reserve(4 + bytes.len());
    batch.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    batch.extend_from_slice(&bytes);
    Ok(())
}

/// ========================================================================
/// In-place log compaction shim (no schema change):
/// - Only runs when tx.metadata is Compact
/// - Encodes CompactLogStream via postcard
/// - Stores as tagged blob in metadata.return_data.data ("CLZ\0" + blob)
/// - Drops verbose log_messages to realize savings
/// ========================================================================
fn compact_logs_inplace(cblk: &mut CompactBlock, fp2id: &AHashMap<u128, u32>, enable: bool) {
    if !enable {
        return;
    }

    // Map base58 -> usage id (u32) using registry
    let mut lookup_pid = |base58: &str| -> Option<u32> {
        Pubkey::try_from(base58).ok().and_then(|pk| {
            let b = pk.to_bytes();
            let a = u128::from_le_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12],
                b[13], b[14], b[15],
            ]);
            let c = u128::from_le_bytes([
                b[16], b[17], b[18], b[19], b[20], b[21], b[22], b[23], b[24], b[25], b[26], b[27],
                b[28], b[29], b[30], b[31],
            ]);
            let fp = a ^ c.rotate_left(64);
            fp2id.get(&fp).copied()
        })
    };

    for tx in &mut cblk.txs {
        let Some(CompactMetadataPayload::Compact(ref mut meta)) = tx.metadata else {
            continue;
        };
        let Some(ref logs) = meta.log_messages else {
            continue;
        };

        let cls: CompactLogStream = encode_logs(logs, &mut lookup_pid, EncodeConfig::default());

        // tag + postcard-encode and stash into return_data
        const TAG: &[u8] = b"CLZ\0";
        if let Ok(mut blob) = postcard::to_allocvec(&cls) {
            let mut tagged = Vec::with_capacity(TAG.len() + blob.len());
            tagged.extend_from_slice(TAG);
            tagged.append(&mut blob);

            // place into return_data (create or overwrite)
            match meta.return_data {
                Some(ref mut rd) => {
                    rd.data = tagged;
                }
                None => {
                    meta.return_data = Some(blockzilla::carblock_to_compact::CompactReturnData {
                        program_id_id: 0, // reserved/ignored (shim)
                        data: tagged,
                    });
                }
            }

            // remove verbose logs — this realizes the size savings
            meta.log_messages = None;
        } else {
            // If we fail to encode for any reason, keep original logs to avoid data loss.
            tracing::warn!("compact_logs: postcard encode failed; keeping original logs");
        }
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
    metadata_mode: MetadataMode,
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

    // Bigger buffers and batched writes to saturate disk
    const OUT_BUF_CAP: usize = 128 << 20; // 128 MiB writer buffer
    const BATCH_TARGET: usize = 128 << 20; // flush when reaching this size
    const BATCH_SOFT_MAX: usize = 160 << 20; // guard flush

    let mut out = BufWriter::with_capacity(OUT_BUF_CAP, File::create(&tmp_path).await?);
    let mut frame_batch: Vec<u8> = Vec::with_capacity(BATCH_TARGET + (8 << 20));

    // Reusable CompactBlock and scratch buffers
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

    // Enable log compaction iff we actually parsed metadata to `Compact` structures.
    // If metadata_mode is Raw/None, there are no String logs to compact here.
    let enable_log_compaction = matches!(metadata_mode, MetadataMode::Compact);

    'epoch: while let Some(block) = car.next_block().await? {
        // Ensure the block node exists, but keep soft-EOF tolerant behavior
        if let Err(e) = block.block() {
            if is_soft_eof(&e) {
                break 'epoch;
            } else {
                return Err(e);
            }
        }

        // Convert whole block using the in-place builder (reuses `cblk`, `buf_tx`, `buf_meta`)
        if let Err(e) = carblock_to_compactblock_inplace(
            &block,
            &reg.map,
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

        // Post-process: compact logs (shim) only when metadata is parsed
        compact_logs_inplace(&mut cblk, &reg.map, enable_log_compaction);

        // Serialize directly into the batch
        push_block_into_batch(&mut frame_batch, &cblk)?;

        // Flush large chunks to reduce syscalls and keep NVMe busy
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

    // Tail flush
    if !frame_batch.is_empty() {
        out.write_all(&frame_batch).await?;
        frame_batch.clear();
    }

    out.flush().await?;

    // Atomic finalize: replace existing file if present
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

/// Optimize multiple epochs concurrently
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

    // already optimized if blocks.bin present
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

/// Back-compat wrapper expected by main.rs (`run_car_optimizer`)
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
