use anyhow::{anyhow, Context, Result};
use indicatif::ProgressBar;
use memmap2::Mmap;
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    sync::{mpsc, Mutex},
    task,
};

use crate::optimizer::BlockWithIds;

pub const LOG_INTERVAL_SECS: u64 = 2;

// ===============================
// Paths
// ===============================
fn epoch_dir(base: &Path, epoch: u64) -> PathBuf {
    base.join(format!("epoch-{epoch:04}"))
}
fn optimized_dir(base: &Path, epoch: u64) -> PathBuf {
    base.join(format!("epoch-{epoch:04}/optimized"))
}
fn optimized_blocks_file(dir: &Path) -> PathBuf {
    dir.join("blocks.bin")
}
fn keys_bin(dir: &Path) -> PathBuf {
    dir.join("keys.bin") // arrival order
}
fn usage_map_bin(dir: &Path) -> PathBuf {
    dir.join("order_ids.map") // usage_id -> arrival_id
}

/// Resolve where to read blocks from:
/// - `<input_dir>/epoch-####.bin` (legacy flat), or
/// - `<input_dir>/epoch-####/optimized/blocks.bin`
fn resolve_blocks_path(input_dir: &str, epoch: u64) -> PathBuf {
    let direct = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));
    if direct.exists() {
        return direct;
    }
    optimized_blocks_file(&optimized_dir(Path::new(input_dir), epoch))
}

// ===============================
// Registry access (only keys.bin + order_ids.map)
// ===============================

/// Random access resolver for `usage_id -> 32B pubkey`,
/// built **only** from the snapshot (optimized dir) or baseline registry dir.
pub struct RegistryAccess {
    keys: Mmap,   // arrival-ordered keys
    map: Vec<u8>, // usage_id -> arrival_id (u32 LE)
    total: usize,
}

impl RegistryAccess {
    /// `registry_dir` should be a folder containing `epoch-####/{keys.bin, order_ids.map}` —
    /// e.g., point it at your **optimized** folder to use the just-committed snapshot.
    pub fn open(registry_dir: &Path, epoch: u64) -> Result<Self> {
        let edir = epoch_dir(registry_dir, epoch);
        let p_keys = keys_bin(&edir);
        let p_map = usage_map_bin(&edir);

        // keys.bin
        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(&p_keys)
            .with_context(|| format!("open {}", p_keys.display()))?;
        let keys = unsafe { Mmap::map(&f)? };
        if keys.len() % 32 != 0 {
            return Err(anyhow!("keys.bin size not multiple of 32"));
        }
        let total = keys.len() / 32;

        // order_ids.map
        let map = std::fs::read(&p_map).with_context(|| format!("read {}", p_map.display()))?;
        if map.len() != total * 4 {
            return Err(anyhow!(
                "order_ids.map size mismatch: got {} bytes, expected {}",
                map.len(),
                total * 4
            ));
        }

        Ok(Self { keys, map, total })
    }

    #[inline(always)]
    pub fn total(&self) -> usize {
        self.total
    }

    /// Resolve usage_id -> 32B pubkey bytes
    #[inline(always)]
    pub fn pubkey_for(&self, usage_id: u32) -> Result<[u8; 32]> {
        let uid = usage_id as usize;
        if uid >= self.total {
            return Err(anyhow!("usage_id out of range"));
        }
        let off = uid * 4;
        let arrival = u32::from_le_bytes([
            self.map[off],
            self.map[off + 1],
            self.map[off + 2],
            self.map[off + 3],
        ]) as usize;

        let mut out = [0u8; 32];
        let src = &self.keys[arrival * 32..arrival * 32 + 32];
        out.copy_from_slice(src);
        Ok(out)
    }
}

// ===============================
// Length-prefixed optimized stream reader (no compression)
// ===============================
struct OptimizedStream {
    rdr: BufReader<File>,
}

impl OptimizedStream {
    async fn open(input_dir: &str, epoch: u64) -> Result<Self> {
        let path = resolve_blocks_path(input_dir, epoch);
        if !path.exists() {
            anyhow::bail!("Blocks file not found: {}", path.display());
        }
        let f = File::open(&path)
            .await
            .with_context(|| format!("open {}", path.display()))?;
        Ok(Self {
            rdr: BufReader::with_capacity(32 * 1024 * 1024, f),
        })
    }

    /// Read next block frame: [u32 length][payload bytes]
    async fn next_block(&mut self) -> Result<Option<BlockWithIds>> {
        let mut len_bytes = [0u8; 4];
        match self.rdr.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut buf = vec![0u8; len];
        self.rdr.read_exact(&mut buf).await?;

        let block: BlockWithIds = postcard::from_bytes(&buf)
            .map_err(|e| anyhow!("deserialize BlockWithIds: {e}"))?;
        Ok(Some(block))
    }
}

// ===============================
// Public API — sequential reader
// ===============================
pub async fn read_compressed_blocks(epoch: u64, input_dir: &str) -> Result<()> {
    let mut stream = OptimizedStream::open(input_dir, epoch).await?;

    let start = Instant::now();
    let mut last_log = start;
    let pb = ProgressBar::new_spinner();

    let mut blocks_count = 0u64;
    let mut instr_count = 0u64;

    while let Some(block) = stream.next_block().await? {
        instr_count += block.instructions.len() as u64;
        blocks_count += 1;

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} ixs | {:>6.1} ix/s",
                blocks_count,
                blocks_count as f64 / elapsed,
                instr_count,
                instr_count as f64 / elapsed,
            ));
        }
    }

    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} ixs | {:>6.1} ix/s | {:.1}s",
        blocks_count,
        blocks_count as f64 / elapsed,
        instr_count,
        instr_count as f64 / elapsed,
        elapsed,
    ));

    Ok(())
}

// ===============================
// Public API — parallel reader (Receiver is NOT Clone)
// ===============================
pub async fn read_compressed_blocks_par(epoch: u64, input_dir: &str, jobs: usize) -> Result<()> {
    let path = resolve_blocks_path(input_dir, epoch);
    if !path.exists() {
        anyhow::bail!("Blocks file not found: {}", path.display());
    }

    // counters
    let blocks_count = Arc::new(AtomicU64::new(0));
    let instr_count = Arc::new(AtomicU64::new(0));

    // channel of frames; single shared receiver behind a Mutex
    let (tx, rx) = mpsc::channel::<Vec<u8>>(jobs.max(1) * 8);
    let rx = Arc::new(Mutex::new(rx));

    // workers
    for _ in 0..jobs.max(1) {
        let rx = Arc::clone(&rx);
        let blocks_count = Arc::clone(&blocks_count);
        let instr_count = Arc::clone(&instr_count);

        task::spawn(async move {
            loop {
                let frame_opt = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                let Some(frame) = frame_opt else { break };

                match postcard::from_bytes::<BlockWithIds>(&frame) {
                    Ok(block) => {
                        instr_count.fetch_add(block.instructions.len() as u64, Ordering::Relaxed);
                        blocks_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => eprintln!("Deserialization error: {e}"),
                }
            }
        });
    }

    // producer
    let file = File::open(&path).await?;
    let mut rdr = BufReader::with_capacity(32 * 1024 * 1024, file);

    let pb = ProgressBar::new_spinner();
    let start = Instant::now();
    let mut last_log = start;

    loop {
        let mut len_bytes = [0u8; 4];
        match rdr.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut buf = vec![0u8; len];
        rdr.read_exact(&mut buf).await?;

        if tx.send(buf).await.is_err() {
            break;
        }

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);

            let b = blocks_count.load(Ordering::Relaxed);
            let i = instr_count.load(Ordering::Relaxed);

            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} ixs | {:>6.1} ix/s",
                b,
                b as f64 / elapsed,
                i,
                i as f64 / elapsed,
            ));
        }
    }

    drop(tx); // let workers drain and exit
    tokio::time::sleep(Duration::from_millis(100)).await;

    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    let b = blocks_count.load(Ordering::Relaxed);
    let i = instr_count.load(Ordering::Relaxed);

    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} ixs | {:>6.1} ix/s | {:.1}s",
        b,
        b as f64 / elapsed,
        i,
        i as f64 / elapsed,
        elapsed,
    ));

    Ok(())
}

// ===============================
// Public API — analyze contents
// ===============================
pub async fn analyze_compressed_blocks(epoch: u64, input_dir: &str) -> Result<()> {
    let mut stream = OptimizedStream::open(input_dir, epoch).await?;

    let mut blocks = 0u64;
    let mut total_instructions = 0u64;
    let mut total_rewards = 0u64;
    let mut total_balances = 0u64;

    let pb = ProgressBar::new_spinner();

    while let Some(block) = stream.next_block().await? {
        blocks += 1;
        total_instructions += block.instructions.len() as u64;
        total_rewards += block.rewards.len() as u64;
        total_balances += block.token_balances.len() as u64;

        if blocks % 1000 == 0 {
            pb.set_message(format!("Analyzed {} blocks...", blocks));
        }
    }

    pb.finish();

    println!("\n📊 Analysis Results:");
    println!("  Epoch:                 {}", epoch);
    println!("  Total Blocks:          {}", blocks);
    println!("  Total Instructions:    {}", total_instructions);
    println!("  Total Rewards:         {}", total_rewards);
    println!("  Total Token Balances:  {}", total_balances);
    if blocks > 0 {
        println!(
            "  Avg Ixs / Block:       {:.2}",
            total_instructions as f64 / blocks as f64
        );
    }

    Ok(())
}
