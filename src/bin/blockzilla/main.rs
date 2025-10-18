mod transaction_parser;

use ahash::AHashSet;
use anyhow::{Result, anyhow};
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
};
use clap::{Parser, Subcommand};
use indicatif::ProgressBar;
use std::sync::{
    Arc, atomic::{AtomicU64, Ordering}
};
use std::time::{Duration, Instant};
use std::{io::Read, mem::MaybeUninit};
use tokio::sync::{Mutex, mpsc};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use wincode::Deserialize;

use crate::transaction_parser::{VersionedTransaction, parse_account_keys_only_fast};

pub const LOG_INTERVAL_SECS: u64 = 2;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Block {
        #[arg(short, long)]
        epoch: u64,
        #[arg(short, long)]
        cache_dir: String,
        #[arg(short, long)]
        mode: FetchMode,
        #[arg(short = 'j', long, default_value_t = 1)]
        jobs: usize,
    },
    Registry {
        #[arg(short, long)]
        epoch: u64,
        #[arg(short, long)]
        cache_dir: String,
        #[arg(short, long)]
        mode: FetchMode,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::new(
            "debug,hyper=off,hyper_util=off,h2=off,reqwest=off,tokio_util=off",
        ))
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();

    let cli = Cli::parse();

    match cli.command {
        Commands::Block {
            epoch,
            cache_dir,
            mode,
            jobs,
        } => read_block_par(epoch, &cache_dir, mode, jobs).await?,
        Commands::Registry {
            epoch,
            cache_dir,
            mode,
        } => build_registry(epoch, &cache_dir, mode).await?,
    }

    Ok(())
}

async fn read_block(epoch: u64, cache_dir: &str, mode: FetchMode) -> anyhow::Result<()> {
    let reader = open_epoch::open_epoch(epoch, cache_dir, mode).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let start = Instant::now();
    let mut last_log = start;

    let pb = ProgressBar::new_spinner();
    let mut reusable_tx = MaybeUninit::uninit();
    let mut out = Vec::new();

    let mut blocks_count = 0;
    let mut tx_count = 0;
    let mut bytes_count = 0;
    let mut entry_count = 0;

    while let Some(block) = car.next_block().await? {
        entry_count += block.entries.len();

        for entry_cid in block.block()?.entries.iter() {
            let entry_cid = entry_cid?;
            let Node::Entry(entry) = block.decode(entry_cid.hash_bytes())? else {
                return Err(anyhow!("Entry not a Node::Entry"));
            };

            for tx_cid in entry.transactions.iter() {
                let tx_cid = tx_cid?;
                let Node::Transaction(tx) = block.decode(tx_cid.hash_bytes())? else {
                    return Err(anyhow!("Entry not a Node::Transaction"));
                };

                let tx_bytes = match tx.data.next {
                    None => tx.data.data,
                    Some(df_cid) => {
                        let df_cid = df_cid.to_cid()?;
                        let mut reader = block.dataframe_reader(&df_cid);
                        out.clear();
                        reader.read_to_end(&mut out)?;
                        drop(reader);
                        &out
                    }
                };
                //let rx = VersionedTransaction::deserialize(tx_bytes)?;
                VersionedTransaction::deserialize_into(tx_bytes, &mut reusable_tx)?;
                let tx = unsafe { reusable_tx.assume_init_ref() };

                unsafe {
                    std::ptr::drop_in_place(tx as *const _ as *mut VersionedTransaction);
                }
                tx_count += 1;
            }
        }

        blocks_count += 1;
        bytes_count += block.entries.iter().map(|(_id, a)| a.len()).sum::<usize>();
        drop(block);

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} avg blk size | {} TPS | {} avg entry",
                blocks_count,
                blocks_count as f64 / elapsed.as_secs_f64(),
                bytes_count as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
                bytes_count / blocks_count,
                tx_count / elapsed.as_secs(),
                entry_count / blocks_count
            ));
        }
    }
    let now = Instant::now();
    let elapsed = now.duration_since(start);
    tracing::info!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} avg blk size | {} TPS",
        blocks_count,
        blocks_count as f64 / elapsed.as_secs_f64(),
        bytes_count as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        bytes_count / blocks_count,
        tx_count / elapsed.as_secs()
    );

    Ok(())
}

async fn build_registry(epoch: u64, cache_dir: &str, mode: FetchMode) -> anyhow::Result<()> {
    let reader = open_epoch::open_epoch(epoch, cache_dir, mode).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let start = Instant::now();
    let mut last_log = start;
    let mut blocks_count = 0;

    let pb = ProgressBar::new_spinner();
    let mut out = AHashSet::new();
    let mut tx_byte_buff = Vec::new();

    while let Some(block) = car.next_block().await? {
        for entry_cid in block.block()?.entries.iter() {
            let Node::Entry(entry) = block.decode(entry_cid?.hash_bytes())? else {
                return Err(anyhow!("Entry not a Node::Entry"));
            };
            for tx_cid in entry.transactions.iter() {
                let tx_cid = tx_cid?;
                let Node::Transaction(tx) = block.decode(tx_cid.hash_bytes())? else {
                    return Err(anyhow!("Entry not a Node::Transaction"));
                };
                let tx_bytes = match tx.data.next {
                    None => tx.data.data,
                    Some(df_cid) => {
                        let df_cid = df_cid.to_cid()?;
                        let mut reader = block.dataframe_reader(&df_cid);
                        tx_byte_buff.clear();
                        reader.read_to_end(&mut tx_byte_buff)?;
                        &tx_byte_buff
                    }
                };
                parse_account_keys_only_fast(tx_bytes, &mut out)?;
            }
        }
        blocks_count += 1;
        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {} key",
                blocks_count,
                blocks_count as f64 / elapsed.as_secs_f64(),
                out.len()
            ));
        }
        drop(block);
    }
    let now = Instant::now();
    let elapsed = now.duration_since(start);
    tracing::info!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {} key",
        blocks_count,
        blocks_count as f64 / elapsed.as_secs_f64(),
        out.len()
    );

    Ok(())
}

async fn read_block_par(epoch: u64, cache_dir: &str, mode: FetchMode, jobs: usize) -> anyhow::Result<()> {
    use std::time::{Duration, Instant};
    use indicatif::ProgressBar;

    let reader = open_epoch::open_epoch(epoch, cache_dir, mode).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    // ---- metrics ----
    let blocks_count = Arc::new(AtomicU64::new(0));
    let tx_count = Arc::new(AtomicU64::new(0));
    let bytes_count = Arc::new(AtomicU64::new(0));
    let entry_count = Arc::new(AtomicU64::new(0));

    // ---- channel ----
    let (tx, rx) = mpsc::channel::<blockzilla::car_block_reader::CarBlock>(jobs * 2);
    let rx = Arc::new(Mutex::new(rx));

    // ---- worker threads ----
    for _ in 0..jobs {
        let rx = Arc::clone(&rx);
        let blocks_count = Arc::clone(&blocks_count);
        let tx_count = Arc::clone(&tx_count);
        let bytes_count = Arc::clone(&bytes_count);
        let entry_count = Arc::clone(&entry_count);

        tokio::spawn(async move {
            let mut reusable_tx = MaybeUninit::uninit();
            let mut tmp_buf = Vec::new();

            loop {
                let Some(block) = ({
                    let mut guard = rx.lock().await;
                    guard.recv().await
                }) else {
                    break;
                };

                entry_count.fetch_add(block.entries.len() as u64, Ordering::Relaxed);

                // decode entries
                let block_node = match block.block() {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                for entry_cid in block_node.entries.iter() {
                    let entry_cid = match entry_cid {
                        Ok(c) => c,
                        Err(_) => continue,
                    };
                    let Node::Entry(entry) = ((match block.decode(entry_cid.hash_bytes()) {
                        Ok(n) => n,
                        Err(_) => continue,
                    })) else {
                        continue;
                    };

                    for tx_cid in entry.transactions.iter() {
                        let tx_cid = match tx_cid {
                            Ok(c) => c,
                            Err(_) => continue,
                        };
                        let Node::Transaction(tx) = (match block.decode(tx_cid.hash_bytes()) {
                            Ok(n) => n,
                            Err(_) => continue,
                        }) else {
                            continue;
                        };

                        let tx_bytes = if let Some(df_cid) = tx.data.next {
                            let Ok(df_cid) = df_cid.to_cid() else {
                                continue;
                            };
                            let mut reader = block.dataframe_reader(&df_cid);
                            tmp_buf.clear();
                            if reader.read_to_end(&mut tmp_buf).is_err() {
                                continue;
                            }
                            &tmp_buf
                        } else {
                            tx.data.data
                        };

                        if VersionedTransaction::deserialize_into(tx_bytes, &mut reusable_tx).is_err()
                        {
                            continue;
                        }
                        let tx = unsafe { reusable_tx.assume_init_ref() };
                        unsafe {
                            std::ptr::drop_in_place(tx as *const _ as *mut VersionedTransaction);
                        }
                        tx_count.fetch_add(1, Ordering::Relaxed);
                    }
                }

                bytes_count.fetch_add(
                    block.entries.iter().map(|(_, a)| a.len()).sum::<usize>() as u64,
                    Ordering::Relaxed,
                );
                blocks_count.fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    // ---- feed blocks ----
    let pb = ProgressBar::new_spinner();
    let start = Instant::now();
    let mut last_log = start;

    while let Some(block) = car.next_block().await? {
        if tx.send(block).await.is_err() {
            break;
        }

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);

            let b = blocks_count.load(Ordering::Relaxed);
            let t = tx_count.load(Ordering::Relaxed);
            let by = bytes_count.load(Ordering::Relaxed);
            let e = entry_count.load(Ordering::Relaxed);

            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} avg blk size | {} TPS | {} avg entry",
                b,
                b as f64 / elapsed.as_secs_f64(),
                by as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
                if b > 0 { by / b } else { 0 },
                if elapsed.as_secs() > 0 { t / elapsed.as_secs() } else { 0 },
                if b > 0 { e / b } else { 0 },
            ));
        }
    }

    drop(tx); // close sender so workers exit
    tokio::time::sleep(Duration::from_millis(200)).await; // wait for cleanup

    let elapsed = start.elapsed();
    let b = blocks_count.load(Ordering::Relaxed);
    let t = tx_count.load(Ordering::Relaxed);
    let by = bytes_count.load(Ordering::Relaxed);
    tracing::info!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} avg blk size | {} TPS",
        b,
        b as f64 / elapsed.as_secs_f64(),
        by as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        if b > 0 { by / b } else { 0 },
        if elapsed.as_secs() > 0 { t / elapsed.as_secs() } else { 0 },
    );

    Ok(())
}
