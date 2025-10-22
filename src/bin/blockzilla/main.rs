mod block_reader;
mod build_registry;
mod build_registry_db;
mod file_downloader;
mod transaction_parser;

use anyhow::Result;
use blockzilla::open_epoch::FetchMode;
use clap::{Parser, Subcommand};

use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::{
    block_reader::{read_block, read_block_par},
    build_registry::{build_registry_auto, build_registry_single},
    build_registry_db::inspect_registry_cli,
};

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
    /// Reads and parses a single epoch (optionally multi-threaded)
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

    /// Builds the registry automatically, downloading and cleaning up each epoch
    Registry {
        #[arg(short, long)]
        cache_dir: String,
        #[arg(short, long)]
        results_dir: String,
        #[arg(long, default_value_t = 900)]
        max_epoch: u64,
    },
    RegistrySingle {
        #[arg(short, long)]
        cache_dir: String,
        #[arg(short, long)]
        results_dir: String,
        #[arg(short, long)]
        epoch: u64,
    },
    RegistryDb {
        #[arg(short, long)]
        cache_dir: String,
        #[arg(short, long)]
        results_dir: String,
        #[arg(long, default_value_t = 900)]
        max_epoch: u64,
    },
    /// Inspect global sled registry
    RegistryInspect {
        /// Path to sled DB (e.g. ./out/registry-global.db)
        #[arg(short, long)]
        db_path: String,

        /// Print as JSON instead of table
        #[arg(long, default_value_t = false)]
        json: bool,

        /// Optional Pubkey to query
        #[arg(short, long)]
        pubkey: Option<String>,
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
        } => {
            if jobs == 1 {
                read_block(epoch, &cache_dir, mode).await?
            } else {
                read_block_par(epoch, &cache_dir, mode, jobs).await?
            }
        }

        Commands::Registry {
            cache_dir,
            results_dir,
            max_epoch,
        } => build_registry_auto(&cache_dir, &results_dir, max_epoch).await?,
        Commands::RegistrySingle {
            cache_dir,
            results_dir,
            epoch,
        } => {
            build_registry_single(&cache_dir, &results_dir, epoch).await?;
        }

        Commands::RegistryDb {
            cache_dir,
            results_dir,
            max_epoch,
        } => build_registry_db::build_registry_auto(&cache_dir, &results_dir, max_epoch).await?,

        Commands::RegistryInspect {
            db_path,
            json,
            pubkey,
        } => {
            inspect_registry_cli(&db_path, json, pubkey)?;
        }
    }

    Ok(())
}
