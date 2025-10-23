use anyhow::{Context, Result};
use indicatif::ProgressBar;
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    sync::{mpsc, Mutex},
};

use crate::optimizer::BlockWithIds;


pub const LOG_INTERVAL_SECS: u64 = 2;

// ============================================================================
// Sequential block reading with async I/O
// ============================================================================
pub async fn read_compressed_blocks(
    epoch: u64,
    input_dir: &str,
) -> Result<()> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));
    
    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }
    
    let file = File::open(&bin_path).await?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);
    
    let start = Instant::now();
    let mut last_log = start;
    let pb = ProgressBar::new_spinner();
    
    let mut blocks_count = 0u64;
    let mut tx_count = 0u64;
    let mut decompress_buf = Vec::with_capacity(2 * 1024 * 1024);
    
    loop {
        // Read length prefix
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes).await {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;
        
        // Read compressed block
        let mut compressed = vec![0u8; len];
        reader.read_exact(&mut compressed).await?;
        
        // Decompress
        decompress_buf.clear();
        zstd::stream::copy_decode(&compressed[..], &mut decompress_buf)
            .context("Failed to decompress block")?;
        
        // Deserialize
        let block: BlockWithIds = postcard::from_bytes(&decompress_buf)
            .context("Failed to deserialize BlockWithIds")?;
        
        tx_count += block.num_transactions;
        blocks_count += 1;
        
        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS",
                blocks_count,
                blocks_count as f64 / elapsed.as_secs_f64(),
                tx_count,
                if elapsed.as_secs() > 0 { tx_count / elapsed.as_secs() } else { 0 },
            ));
        }
    }
    
    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS | {:.1}s",
        blocks_count,
        blocks_count as f64 / start.elapsed().as_secs_f64(),
        tx_count,
        if start.elapsed().as_secs() > 0 { tx_count / start.elapsed().as_secs() } else { 0 },
        start.elapsed().as_secs_f64()
    ));
    
    Ok(())
}

// ============================================================================
// Parallel block reading with async I/O
// ============================================================================
pub async fn read_compressed_blocks_par(
    epoch: u64,
    input_dir: &str,
    jobs: usize,
) -> Result<()> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));
    
    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }
    
    let blocks_count = Arc::new(AtomicU64::new(0));
    let tx_count = Arc::new(AtomicU64::new(0));
    
    // Channel for compressed blocks
    let (tx, rx) = mpsc::channel::<Vec<u8>>(jobs * 4);
    let rx = Arc::new(Mutex::new(rx));
    
    // Spawn worker tasks for decompression and deserialization
    for _ in 0..jobs {
        let rx = Arc::clone(&rx);
        let blocks_count = Arc::clone(&blocks_count);
        let tx_count = Arc::clone(&tx_count);
        
        tokio::task::spawn_blocking(move || {
            let mut decompress_buf = Vec::with_capacity(2 * 1024 * 1024);
            
            loop {
                let Some(compressed) = ({
                    let mut guard = rx.blocking_lock();
                    guard.blocking_recv()
                }) else {
                    break;
                };
                
                // Decompress
                decompress_buf.clear();
                if let Err(e) = zstd::stream::copy_decode(&compressed[..], &mut decompress_buf) {
                    eprintln!("Decompression error: {}", e);
                    continue;
                }
                
                // Deserialize
                match postcard::from_bytes::<BlockWithIds>(&decompress_buf) {
                    Ok(block) => {
                        tx_count.fetch_add(block.num_transactions, Ordering::Relaxed);
                        blocks_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Deserialization error: {}", e);
                    }
                }
            }
        });
    }
    
    // Main task: read blocks and feed to workers
    let file = File::open(&bin_path).await?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);
    
    let pb = ProgressBar::new_spinner();
    let start = Instant::now();
    let mut last_log = start;
    
    loop {
        // Read length prefix
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes).await {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;
        
        // Read compressed block
        let mut compressed = vec![0u8; len];
        reader.read_exact(&mut compressed).await?;
        
        // Send to workers
        if tx.send(compressed).await.is_err() {
            break;
        }
        
        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);
            
            let b = blocks_count.load(Ordering::Relaxed);
            let t = tx_count.load(Ordering::Relaxed);
            
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS",
                b,
                b as f64 / elapsed.as_secs_f64(),
                t,
                if elapsed.as_secs() > 0 { t / elapsed.as_secs() } else { 0 },
            ));
        }
    }
    
    // Close channel and wait for workers
    drop(tx);
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let elapsed = start.elapsed();
    let b = blocks_count.load(Ordering::Relaxed);
    let t = tx_count.load(Ordering::Relaxed);
    
    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} txs | {:>6} TPS | {:.1}s",
        b,
        b as f64 / elapsed.as_secs_f64(),
        t,
        if elapsed.as_secs() > 0 { t / elapsed.as_secs() } else { 0 },
        elapsed.as_secs_f64()
    ));
    
    Ok(())
}

// ============================================================================
// Analyze block contents
// ============================================================================
pub async fn analyze_compressed_blocks(
    epoch: u64,
    input_dir: &str,
) -> Result<()> {
    let bin_path = Path::new(input_dir).join(format!("epoch-{epoch:04}.bin"));
    
    if !bin_path.exists() {
        anyhow::bail!("Binary file not found: {}", bin_path.display());
    }
    
    let file = File::open(&bin_path).await?;
    let mut reader = BufReader::with_capacity(32 * 1024 * 1024, file);
    
    let mut blocks_count = 0u64;
    let mut total_txs = 0u64;
    let mut total_instructions = 0u64;
    let mut total_inner_instructions = 0u64;
    let mut blocks_with_rewards = 0u64;
    let mut decompress_buf = Vec::with_capacity(2 * 1024 * 1024);
    
    let pb = ProgressBar::new_spinner();
    
    loop {
        // Read length prefix
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes).await {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;
        
        // Read compressed block
        let mut compressed = vec![0u8; len];
        reader.read_exact(&mut compressed).await?;
        
        // Decompress
        decompress_buf.clear();
        zstd::stream::copy_decode(&compressed[..], &mut decompress_buf)?;
        
        // Deserialize
        let block: BlockWithIds = postcard::from_bytes(&decompress_buf)?;
        
        blocks_count += 1;
        total_txs += block.num_transactions;
        if !block.rewards.is_empty() {
            blocks_with_rewards += 1;
        }
        
        for tx in &block.transactions {
            total_instructions += tx.instructions.len() as u64;
            if let Some(inner) = tx.meta.as_ref().and_then(|m| m.inner_instructions.as_ref()) {
                for ii in inner {
                    total_inner_instructions += ii.instructions.len() as u64;
                }
            }
        }
        
        if blocks_count % 1000 == 0 {
            pb.set_message(format!("Analyzed {} blocks...", blocks_count));
        }
    }
    
    pb.finish();
    
    println!("\n📊 Analysis Results:");
    println!("  Epoch:              {}", epoch);
    println!("  Total Blocks:       {}", blocks_count);
    println!("  Total Transactions: {}", total_txs);
    println!("  Avg TXs/Block:      {:.2}", total_txs as f64 / blocks_count as f64);
    println!("  Total Instructions: {}", total_instructions);
    println!("  Total Inner Ixs:    {}", total_inner_instructions);
    println!("  Blocks w/ Rewards:  {}", blocks_with_rewards);
    
    Ok(())
}