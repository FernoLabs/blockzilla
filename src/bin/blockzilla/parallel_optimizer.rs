use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use blockzilla::block_stream::SolanaBlockStream;
use crossbeam_channel::{bounded, Receiver, Sender};
use futures::io::AllowStdIo;
use rusqlite::{params, Connection};
use std::thread;

use crate::{compat_block::cb_to_compact_block, optimizer::{BlockWithIds, KeyRegistry}};

const CHANNEL_SIZE: usize = 32; // Buffer size for pipeline
const NUM_WORKERS: usize = 8;   // Parallel processing threads

#[derive(Default, Debug)]
struct StageTimings {
    decode_total: f64,
    compact_total: f64,
    serialize_total: f64,
    compress_total: f64,
    write_total: f64,
    blocks: u64,
}

impl StageTimings {
    fn record(&mut self, d: f64, c: f64, s: f64, z: f64, w: f64) {
        self.decode_total += d;
        self.compact_total += c;
        self.serialize_total += s;
        self.compress_total += z;
        self.write_total += w;
        self.blocks += 1;
    }

    fn print_periodic(&self, count: u64, bytes: u64, start: Instant) {
        if self.blocks == 0 {
            println!("🧮 {:>7} blk | collecting…", count);
            return;
        }
        let total = self.decode_total
            + self.compact_total
            + self.serialize_total
            + self.compress_total
            + self.write_total;
        let pct = |x: f64| if total > 0.0 { x / total * 100.0 } else { 0.0 };
        let avg_ms = total / self.blocks as f64;
        let throughput = count as f64 / start.elapsed().as_secs_f64().max(0.001);
        println!(
            "🧮 {:>7} blk | decode {:>6.2}% | compact {:>6.2}% | ser {:>6.2}% | comp {:>6.2}% | write {:>6.2}% | avg {:>6.2} ms/blk | {:.1} blk/s | {:.2} MB",
            count,
            pct(self.decode_total),
            pct(self.compact_total),
            pct(self.serialize_total),
            pct(self.compress_total),
            pct(self.write_total),
            avg_ms,
            throughput,
            bytes as f64 / 1_000_000.0
        );
    }
}

// Pipeline stages
enum BlockMessage {
    RawBlock(blockzilla::block_stream::CarBlock, u64), // block + slot number
    CompactBlock(BlockWithIds, Vec<(u64, Vec<u8>)>),    // block + pubkey mappings
    Compressed(u64, Vec<u8>),                            // slot + compressed data
    Done,
}

struct ProcessedBlock {
    slot: u64,
    compact: BlockWithIds,
    local_keys: Vec<(u64, Vec<u8>)>, // Local pubkey -> bytes mapping
}

pub async fn run_car_optimizer_parallel(path: &str, output_dir: Option<String>) -> Result<()> {
    let epoch = extract_epoch_from_path(path)
        .context("Could not parse epoch number from input filename")?;

    let out_dir = PathBuf::from(output_dir.unwrap_or_else(|| "optimized".into()));
    std::fs::create_dir_all(&out_dir)?;

    let bin_path = out_dir.join(format!("epoch-{epoch}.bin"));
    let idx_path = out_dir.join(format!("epoch-{epoch}.idx"));
    let reg_path = out_dir.join("registry.sqlite");

    // Shared registry protected by mutex
    let registry = Arc::new(Mutex::new(KeyRegistry::new()));
    
    // Pipeline channels
    let (raw_tx, raw_rx) = bounded::<BlockMessage>(CHANNEL_SIZE);
    let (compact_tx, compact_rx) = bounded::<BlockMessage>(CHANNEL_SIZE);
    let (compress_tx, compress_rx) = bounded::<BlockMessage>(CHANNEL_SIZE);

    let start = Instant::now();
    let timings = Arc::new(Mutex::new(StageTimings::default()));

    // Stage 1: Reader thread - read from CAR file
    let reader_handle = {
        let path = path.to_string();
        thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async {
                    reader_thread(path, raw_tx).await
                })
        })
    };

    // Stage 2: Parallel workers - decode and compact blocks
    let mut worker_handles = Vec::new();
    for _ in 0..NUM_WORKERS {
        let raw_rx = raw_rx.clone();
        let compact_tx = compact_tx.clone();
        let registry = Arc::clone(&registry);
        let timings = Arc::clone(&timings);
        
        let handle = thread::spawn(move || {
            worker_thread(raw_rx, compact_tx, registry, timings)
        });
        worker_handles.push(handle);
    }
    drop(raw_rx); // Close original receiver
    drop(compact_tx); // Workers will close their senders when done

    // Stage 3: Serializer/Compressor thread
    let compressor_handle = {
        let compress_tx = compress_tx.clone();
        thread::spawn(move || {
            compressor_thread(compact_rx, compress_tx)
        })
    };
    drop(compress_tx);

    // Stage 4: Writer thread - write to disk
    let writer_handle = {
        let bin_path = bin_path.clone();
        let idx_path = idx_path.clone();
        let registry = Arc::clone(&registry);
        let reg_path = reg_path.clone();
        let timings = Arc::clone(&timings);
        
        thread::spawn(move || {
            writer_thread(compress_rx, bin_path, idx_path, registry, reg_path, timings, start)
        })
    };

    // Wait for pipeline to complete
    reader_handle.join().unwrap()?;
    for handle in worker_handles {
        handle.join().unwrap()?;
    }
    compressor_handle.join().unwrap()?;
    let (count, total_bytes) = writer_handle.join().unwrap()?;

    // Final stats
    let timings = timings.lock().unwrap();
    timings.print_periodic(count, total_bytes, start);
    
    let registry = registry.lock().unwrap();
    println!(
        "✅ Done: {count} blocks → {}, {} unique keys ({:.2} MB total)",
        bin_path.display(),
        registry.len(),
        total_bytes as f64 / 1_000_000.0
    );

    Ok(())
}

// Stage 1: Read blocks from CAR file
async fn reader_thread(
    path: String,
    tx: Sender<BlockMessage>,
) -> Result<()> {
    let file = File::open(&path)?;
    let reader = AllowStdIo::new(file);
    let mut stream = SolanaBlockStream::new(reader).await?;

    while let Some(block) = stream.next_solana_block().await? {
        let slot = block.block.meta.parent_slot.unwrap_or(0) + 1;
        tx.send(BlockMessage::RawBlock(block, slot))?;
    }

    // Send done signals for all workers
    for _ in 0..NUM_WORKERS {
        tx.send(BlockMessage::Done)?;
    }

    Ok(())
}

// Stage 2: Worker threads - decode and compact
fn worker_thread(
    rx: Receiver<BlockMessage>,
    tx: Sender<BlockMessage>,
    registry: Arc<Mutex<KeyRegistry>>,
    timings: Arc<Mutex<StageTimings>>,
) -> Result<()> {
    // Each worker has its own local registry for thread-local key collection
    let mut local_registry = KeyRegistry::new();
    
    loop {
        match rx.recv()? {
            BlockMessage::RawBlock(car_block, slot) => {
                let t0 = Instant::now();
                
                // Convert to compact format with local registry
                let compact = cb_to_compact_block(car_block, &mut local_registry)?;
                
                let t1 = Instant::now();
                
                // Extract new keys added in this block (for merging later)
                let local_keys: Vec<(u64, Vec<u8>)> = local_registry
                    .by_id
                    .iter()
                    .enumerate()
                    .map(|(id, pk)| (id as u64, pk.to_bytes().to_vec()))
                    .collect();
                
                // Record timing
                let mut timings = timings.lock().unwrap();
                timings.record(
                    0.0, // decode done in compact phase
                    (t1 - t0).as_secs_f64() * 1000.0,
                    0.0,
                    0.0,
                    0.0,
                );
                drop(timings);
                
                tx.send(BlockMessage::CompactBlock(compact, local_keys))?;
            }
            BlockMessage::Done => {
                tx.send(BlockMessage::Done)?;
                break;
            }
            _ => unreachable!(),
        }
    }
    
    Ok(())
}

// Stage 3: Serialize and compress
fn compressor_thread(
    rx: Receiver<BlockMessage>,
    tx: Sender<BlockMessage>,
) -> Result<()> {
    let mut done_count = 0;
    
    loop {
        match rx.recv()? {
            BlockMessage::CompactBlock(compact, _local_keys) => {
                let slot = compact.slot;
                
                let t0 = Instant::now();
                let raw = postcard::to_allocvec(&compact)?;
                let t1 = Instant::now();
                
                let compressed = zstd::bulk::compress(&raw, 3)?;
                let t2 = Instant::now();
                
                tx.send(BlockMessage::Compressed(slot, compressed))?;
            }
            BlockMessage::Done => {
                done_count += 1;
                if done_count >= NUM_WORKERS {
                    tx.send(BlockMessage::Done)?;
                    break;
                }
            }
            _ => unreachable!(),
        }
    }
    
    Ok(())
}

// Stage 4: Write to disk
fn writer_thread(
    rx: Receiver<BlockMessage>,
    bin_path: PathBuf,
    idx_path: PathBuf,
    registry: Arc<Mutex<KeyRegistry>>,
    reg_path: PathBuf,
    timings: Arc<Mutex<StageTimings>>,
    start: Instant,
) -> Result<(u64, u64)> {
    let bin_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&bin_path)?;
    let mut bin = BufWriter::with_capacity(16 * 1024 * 1024, bin_file);
    
    let idx_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&idx_path)?;
    let mut idx = BufWriter::with_capacity(2 * 1024 * 1024, idx_file);

    let mut count = 0u64;
    let mut current_offset = 0u64;
    let mut total_bytes = 0u64;
    let mut last_log = Instant::now();

    loop {
        match rx.recv()? {
            BlockMessage::Compressed(slot, compressed) => {
                let t0 = Instant::now();
                
                let len = compressed.len() as u32;
                bin.write_all(&len.to_le_bytes())?;
                bin.write_all(&compressed)?;
                idx.write_all(&slot.to_le_bytes())?;
                idx.write_all(&current_offset.to_le_bytes())?;
                
                let t1 = Instant::now();

                current_offset += 4 + compressed.len() as u64;
                total_bytes += compressed.len() as u64;
                count += 1;

                // Update timings
                {
                    let mut timings = timings.lock().unwrap();
                    timings.write_total += (t1 - t0).as_secs_f64() * 1000.0;
                }

                // Periodic logging and flushing
                if last_log.elapsed() > Duration::from_secs(15) {
                    bin.flush()?;
                    idx.flush()?;
                    
                    let registry = registry.lock().unwrap();
                    registry.save_to_sqlite(reg_path.to_str().unwrap())?;
                    drop(registry);
                    
                    let timings = timings.lock().unwrap();
                    timings.print_periodic(count, total_bytes, start);
                    drop(timings);
                    
                    last_log = Instant::now();
                }
            }
            BlockMessage::Done => break,
            _ => unreachable!(),
        }
    }

    bin.flush()?;
    idx.flush()?;
    
    let registry = registry.lock().unwrap();
    registry.save_to_sqlite(reg_path.to_str().unwrap())?;

    Ok((count, total_bytes))
}

fn extract_epoch_from_path(path: &str) -> Option<u64> {
    Path::new(path)
        .file_stem()?
        .to_string_lossy()
        .split('-')
        .nth(1)?
        .parse::<u64>()
        .ok()
}