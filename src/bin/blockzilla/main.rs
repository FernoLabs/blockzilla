mod block_mode;
//mod compat_block;
//mod dump_registry;
//mod optimizer;
//mod print_compressed_block;

mod config;
mod downloader;
mod key_extractor;
mod merge;
mod merge_registry;
mod node_mode;
mod reader;
mod transaction_parser;
mod types;

use anyhow::{Result, anyhow};
use blockzilla::{
    car_block_reader::CarBlockReader,
    node::Node,
    open_epoch::{self, FetchMode},
};
use clap::{Parser, Subcommand};
use futures::{StreamExt, stream::FuturesUnordered};
use indicatif::ProgressBar;
use solana_sdk::stable_layout::stable_ref_cell;
use std::{
    io::{Bytes, Read},
    mem::MaybeUninit,
    sync::Arc,
};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tracing_subscriber::FmtSubscriber;
use wincode::Deserialize;

use crate::{
    block_mode::run_block_mode,
    config::LOG_INTERVAL_SECS,
    node_mode::{run_car_mode, run_node_mode},
    transaction_parser::VersionedTransaction,
    types::DownloadMode,
};

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
    Car {
        #[arg(short, long)]
        file: String,
    },
    Node {
        #[arg(short, long)]
        file: String,
    },
    Block {
        #[arg(short, long)]
        file: String,
    },
    Block2 {
        #[arg(short, long)]
        epoch: u64,
    },
    Optimize {
        #[arg(short, long)]
        file: String,
        #[arg(long)]
        output_dir: Option<String>,
    },
    Read {
        #[arg(long)]
        epoch: String,
        #[arg(long)]
        idx: String,
        #[arg(long)]
        registry: String,
        slot: u64,
    },
    Registry {
        #[arg(long)]
        file: String,
        #[arg(long)]
        output_dir: Option<String>,
    },
    RegistryAll {
        #[arg(long)]
        base_dir: String,
        #[arg(long)]
        output_dir: Option<String>,
    },
    DumpRegistry {
        /// Path to the SQLite registry file
        #[arg(long)]
        registry: String,

        /// Output CSV path
        #[arg(long, default_value = "pubkey_map.csv")]
        output: String,
    },
    MergeRegistry {
        /// Input directory containing pubkeys-XXXX.bin files
        #[arg(long)]
        input: String,

        /// Output merged registry path
        #[arg(long, default_value = "merged_pubkeys.bin")]
        output: String,
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
        Commands::Car { file } => run_car_mode(&PathBuf::from(file), DownloadMode::Stream).await?,
        Commands::Node { file } => {
            run_node_mode(&PathBuf::from(file), DownloadMode::Stream).await?
        }
        Commands::Block { file } => {
            run_block_mode(&PathBuf::from(file), DownloadMode::Stream).await?
        }
        Commands::Optimize {
            file: _,
            output_dir: _,
        } => {
            //run_car_optimizer(&file, output_dir).await?
        }
        Commands::Read {
            epoch: _,
            idx: _,
            registry: _,
            slot: _,
        } => {
            //read_and_print_block_compact(&epoch, &idx, &registry, slot)?
        }
        Commands::DumpRegistry {
            registry: _,
            output: _,
        } => {
            //dump_registry::dump_registry_to_csv(&registry, &output)?
        }
        Commands::Registry { file, output_dir } => {
            /*key_extractor::process_single_epoch_file(
                &PathBuf::from(file),
                &PathBuf::from(&output_dir.unwrap_or_else(|| "optimized".to_string())),
                DownloadMode::Stream,
            )
            .await?;
        */
        }
        Commands::RegistryAll {
            base_dir,
            output_dir,
        } => {
            /*key_extractor::extract_all_pubkeys(
                &base_dir.into(),
                &output_dir.unwrap_or("optimized".into()).into(),
                830,
                8,
                DownloadMode::NoDownload,
            )
            .await
            */
        }
        Commands::MergeRegistry { input, output } => {
            let src = std::path::PathBuf::from(input);
            let dest = std::path::PathBuf::from(output);
            merge_registry::merge_bin_to_binary(&src, &dest)?
        }
        Commands::Block2 { epoch } => read_block2(epoch).await?,
    }

    Ok(())
}

/// Parse epoch from filename
pub fn extract_epoch_from_path(path: &str) -> Option<u64> {
    Path::new(path)
        .file_stem()?
        .to_string_lossy()
        .split('-')
        .find(|s| s.chars().all(|c| c.is_ascii_digit()))
        .and_then(|n| n.parse().ok())
}

async fn read_block2(epoch: u64) -> anyhow::Result<()> {
    let cache_dir = "./";
    let mode = FetchMode::Network;

    let reader = open_epoch::open_epoch(epoch, cache_dir, mode).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let start = Instant::now();
    let mut last_log = start;

    let pb = ProgressBar::new_spinner();
    let mut reusable_tx = MaybeUninit::uninit();
    let mut blocks_count = 0;
    let mut tx_count = 0;
    let mut bytes_count = 0;

    while let Some(block) = car.next_block().await? {
        for CborCid(entry_cid) in &block.block()?.entries {
            let Node::Entry(entry) = block.decode(entry_cid)? else {
                tracing::error!("Entry not a Node::Entry {entry_cid}");
                continue;
                //return Err(anyhow!("Entry not a Node::Entry"));
            };
            for CborCid(tx_cid) in entry.transactions {
                let Node::Transaction(tx) = block.decode(&tx_cid)? else {
                    tracing::error!("Entry not a Node::Transaction {tx_cid}");
                    continue;
                    //return Err(anyhow!("Entry not a Node::Transaction"));
                };
                let mut out = Vec::new();
                let tx_bytes = match tx.data.next {
                    None => tx.data.data,
                    Some(CborCid(df_cid)) => {
                        let mut reader = block.dataframe_reader(&df_cid);
                        reader.read_to_end(&mut out)?;
                        &out
                    }
                };
                VersionedTransaction::deserialize_into(tx_bytes, &mut reusable_tx)?;
                let tx = unsafe { reusable_tx.assume_init_ref() };
                tx_count += 1;
            }
        }
        blocks_count += 1;
        bytes_count += block.index.values().map(|a| a.len()).sum::<usize>();

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} avg blk size | {} TPS",
                blocks_count,
                blocks_count as f64 / elapsed.as_secs_f64(),
                bytes_count as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
                bytes_count / blocks_count,
                tx_count / elapsed.as_secs()
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

async fn read_block2_par(epoch: u64) -> anyhow::Result<()> {
    const LOG_INTERVAL_SECS: u64 = 5;

    let cache_dir = "./cache";
    let mode = FetchMode::Network;
    let reader = open_epoch::open_epoch(epoch, cache_dir, mode).await?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let start = Instant::now();
    let mut last_log = start;

    let pb = ProgressBar::new_spinner();
    let mut blocks_count = 0usize;
    let mut tx_count = 0usize;
    let mut bytes_count = 0usize;

    // To track worker tasks
    let mut workers = FuturesUnordered::new();

    while let Some(block) = car.next_block().await? {
        let block = Arc::new(block);
        blocks_count += 1;
        bytes_count += block.index.values().map(|a| a.len()).sum::<usize>();

        // Spawn background decoding
        let b = block.clone();
        workers.push(tokio::task::spawn_blocking(
            move || -> anyhow::Result<usize> {
                let mut reusable_tx = MaybeUninit::uninit();
                let mut local_tx_count = 0usize;
                for CborCid(entry_cid) in &b.block()?.entries {
                    let Node::Entry(entry) = b.decode(entry_cid)? else {
                        continue;
                    };
                    for CborCid(tx_cid) in entry.transactions {
                        let Node::Transaction(tx) = b.decode(&tx_cid)? else {
                            continue;
                        };
                        let mut out = Vec::new();
                        let tx_bytes = match tx.data.next {
                            None => tx.data.data,
                            Some(CborCid(df_cid)) => {
                                let mut reader = b.dataframe_reader(&df_cid);
                                reader.read_to_end(&mut out)?;
                                &out
                            }
                        };

                        VersionedTransaction::deserialize_into(tx_bytes, &mut reusable_tx)?;
                        let _ = unsafe { reusable_tx.assume_init_ref() };
                        local_tx_count += 1;
                    }
                }
                Ok(local_tx_count)
            },
        ));

        // Limit concurrent threads
        if workers.len() > 4 {
            if let Some(done) = workers.next().await {
                tx_count += done??;
            }
        }

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} avg blk size | {} TPS",
                blocks_count,
                blocks_count as f64 / elapsed.as_secs_f64(),
                bytes_count as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
                bytes_count / blocks_count.max(1),
                tx_count / elapsed.as_secs().max(1) as usize,
            ));
        }
    }

    // drain remaining workers
    while let Some(done) = workers.next().await {
        tx_count += done??;
    }

    let elapsed = start.elapsed();
    tracing::info!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} avg blk size | {} TPS",
        blocks_count,
        blocks_count as f64 / elapsed.as_secs_f64(),
        bytes_count as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        bytes_count / blocks_count.max(1),
        tx_count / elapsed.as_secs().max(1) as usize,
    );

    Ok(())
}
