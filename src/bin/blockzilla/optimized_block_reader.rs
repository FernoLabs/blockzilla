use anyhow::{Context, Result, anyhow};
use indicatif::ProgressBar;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    sync::{Mutex, mpsc},
    task,
};

use blockzilla::carblock_to_compact::CompactBlock;

pub const LOG_INTERVAL_SECS: u64 = 2;

// ===============================
// Paths (optimized stream)
// ===============================
fn optimized_dir(base: &Path, epoch: u64) -> PathBuf {
    base.join(format!("epoch-{epoch:04}/optimized"))
}
fn optimized_blocks_file(dir: &Path) -> PathBuf {
    dir.join("blocks.bin")
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
// Length prefixed optimized stream reader (no compression)
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

    /// Read next block frame: [u32 length][postcard CompactBlock bytes]
    async fn next_block(&mut self) -> Result<Option<CompactBlock>> {
        let mut len_bytes = [0u8; 4];
        match self.rdr.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut buf = vec![0u8; len];
        self.rdr.read_exact(&mut buf).await?;

        let block: CompactBlock =
            postcard::from_bytes(&buf).map_err(|e| anyhow!("deserialize CompactBlock: {e}"))?;
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
    let mut tx_count = 0u64;
    let mut reward_count = 0u64;

    while let Some(block) = stream.next_block().await? {
        // Count per new CompactBlock shape
        tx_count += block.txs.len() as u64;
        reward_count += block.rewards.len() as u64;
        instr_count += block
            .txs
            .iter()
            .map(|t| t.instructions.len() as u64)
            .sum::<u64>();
        blocks_count += 1;

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} tx | {:>8} ixs | {:>6.1} ix/s | {:>7} rwd",
                blocks_count,
                blocks_count as f64 / elapsed,
                tx_count,
                instr_count,
                instr_count as f64 / elapsed,
                reward_count,
            ));
        }
    }

    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} tx | {:>8} ixs | {:>6.1} ix/s | {:>7} rwd | {:.1}s",
        blocks_count,
        blocks_count as f64 / elapsed,
        tx_count,
        instr_count,
        instr_count as f64 / elapsed,
        reward_count,
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
    let tx_count = Arc::new(AtomicU64::new(0));
    let reward_count = Arc::new(AtomicU64::new(0));

    // channel of frames; single shared receiver behind a Mutex
    let (tx, rx) = mpsc::channel::<Vec<u8>>(jobs.max(1) * 8);
    let rx = Arc::new(Mutex::new(rx));

    // workers
    for _ in 0..jobs.max(1) {
        let rx = Arc::clone(&rx);
        let blocks_count = Arc::clone(&blocks_count);
        let instr_count = Arc::clone(&instr_count);
        let tx_count = Arc::clone(&tx_count);
        let reward_count = Arc::clone(&reward_count);

        task::spawn(async move {
            loop {
                let frame_opt = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                let Some(frame) = frame_opt else { break };

                match postcard::from_bytes::<CompactBlock>(&frame) {
                    Ok(block) => {
                        let ixs_in_block: u64 =
                            block.txs.iter().map(|t| t.instructions.len() as u64).sum();
                        instr_count.fetch_add(ixs_in_block, Ordering::Relaxed);
                        tx_count.fetch_add(block.txs.len() as u64, Ordering::Relaxed);
                        reward_count.fetch_add(block.rewards.len() as u64, Ordering::Relaxed);
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
            let t = tx_count.load(Ordering::Relaxed);
            let r = reward_count.load(Ordering::Relaxed);

            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} tx | {:>8} ixs | {:>6.1} ix/s | {:>7} rwd",
                b,
                b as f64 / elapsed,
                t,
                i,
                i as f64 / elapsed,
                r,
            ));
        }
    }

    drop(tx); // let workers drain and exit
    tokio::time::sleep(Duration::from_millis(100)).await;

    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    let b = blocks_count.load(Ordering::Relaxed);
    let i = instr_count.load(Ordering::Relaxed);
    let t = tx_count.load(Ordering::Relaxed);
    let r = reward_count.load(Ordering::Relaxed);

    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} tx | {:>8} ixs | {:>6.1} ix/s | {:>7} rwd | {:.1}s",
        b,
        b as f64 / elapsed,
        t,
        i,
        i as f64 / elapsed,
        r,
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
    let mut total_txs = 0u64;
    let mut total_instructions = 0u64;
    let mut total_rewards = 0u64;

    let pb = ProgressBar::new_spinner();

    while let Some(block) = stream.next_block().await? {
        blocks += 1;
        total_txs += block.txs.len() as u64;
        total_rewards += block.rewards.len() as u64;
        total_instructions += block
            .txs
            .iter()
            .map(|t| t.instructions.len() as u64)
            .sum::<u64>();

        if blocks.is_multiple_of(1000) {
            pb.set_message(format!("Analyzed {} blocks...", blocks));
        }
    }

    pb.finish();

    println!("\n📊 Analysis Results:");
    println!("  Epoch:                 {}", epoch);
    println!("  Total Blocks:          {}", blocks);
    println!("  Total Transactions:    {}", total_txs);
    println!("  Total Instructions:    {}", total_instructions);
    println!("  Total Rewards:         {}", total_rewards);
    if blocks > 0 {
        println!(
            "  Avg Tx per Block:      {:.2}",
            total_txs as f64 / blocks as f64
        );
        println!(
            "  Avg Ixs per Block:     {:.2}",
            total_instructions as f64 / blocks as f64
        );
        println!(
            "  Avg Rewards per Block: {:.2}",
            total_rewards as f64 / blocks as f64
        );
    }

    Ok(())
}
