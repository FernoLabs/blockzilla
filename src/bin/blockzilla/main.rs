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
use std::time::{Duration, Instant};
use std::{io::Read, mem::MaybeUninit};
use tracing_subscriber::FmtSubscriber;
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
    // Initialize global logger
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG) // or INFO
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set tracing subscriber");

    let cli = Cli::parse();

    match cli.command {
        Commands::Block {
            epoch,
            cache_dir,
            mode,
        } => read_block(epoch, &cache_dir, mode).await?,
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
                //tracing::error!("Entry not a Node::Entry {entry_cid}");
                //continue;
                return Err(anyhow!("Entry not a Node::Entry"));
            };
            for tx_cid in entry.transactions.iter() {
                let tx_cid = tx_cid?;
                let Node::Transaction(tx) = block.decode(tx_cid.hash_bytes())? else {
                    //tracing::error!("Entry not a Node::Transaction {tx_cid}");
                    //continue;
                    return Err(anyhow!("Entry not a Node::Transaction"));
                };
                let tx_bytes = match tx.data.next {
                    None => tx.data.data,
                    Some(df_cid) => {
                        let df_cid = df_cid.to_cid()?;
                        let mut reader = block.dataframe_reader(&df_cid);
                        out.clear();
                        reader.read_to_end(&mut out)?;
                        &out
                    }
                };
                VersionedTransaction::deserialize_into(tx_bytes, &mut reusable_tx)?;
                let _tx = unsafe { reusable_tx.assume_init_ref() };
                tx_count += 1;
            }
        }
        blocks_count += 1;
        bytes_count += block.entries.iter().map(|(_id, a)| a.len()).sum::<usize>();

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
        drop(block);
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
            let entry_cid = entry_cid?;
            let Node::Entry(entry) = block.decode(entry_cid.hash_bytes())? else {
                //tracing::error!("Entry not a Node::Entry {entry_cid}");
                //continue;
                return Err(anyhow!("Entry not a Node::Entry"));
            };
            for tx_cid in entry.transactions.iter() {
                let tx_cid = tx_cid?;
                let Node::Transaction(tx) = block.decode(tx_cid.hash_bytes())? else {
                    //tracing::error!("Entry not a Node::Transaction {tx_cid}");
                    //continue;
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
