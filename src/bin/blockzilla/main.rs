mod block_reader;
mod build_registry;
mod optimized_block_reader;
mod file_downloader;
mod optimizer;
mod transaction_parser;

use anyhow::Result;
use blockzilla::open_epoch::FetchMode;
use clap::{Parser, Subcommand};

use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::{
    block_reader::{read_block, read_block_par},
    build_registry::{build_registry_auto, build_registry_single},
    optimized_block_reader::{analyze_compressed_blocks, read_compressed_blocks, read_compressed_blocks_par},
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
    /// Reads and parses a single epoch from CAR format (optionally multi-threaded)
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

    /// Builds the registry for a single epoch
    RegistrySingle {
        #[arg(short, long)]
        cache_dir: String,
        #[arg(short, long)]
        results_dir: String,
        #[arg(short, long)]
        epoch: u64,
    },

    /// Optimizes a CAR epoch into compressed BlockWithIds format
    Optimize {
        #[arg(short, long)]
        cache_dir: String,
        #[arg(short, long, default_value = "optimized")]
        results_dir: String,
        #[arg(short, long)]
        epoch: u64,
        #[arg(short = 'z', long, default_value_t = 0)]
        zstd_level: i32,
    },

    /// Reads compressed BlockWithIds format (sequential)
    ReadCompressed {
        #[arg(short, long)]
        epoch: u64,
        #[arg(short, long, default_value = "optimized")]
        input_dir: String,
    },

    /// Reads compressed BlockWithIds format (parallel)
    ReadCompressedPar {
        #[arg(short, long)]
        epoch: u64,
        #[arg(short, long, default_value = "optimized")]
        input_dir: String,
        #[arg(short = 'j', long, default_value_t = 4)]
        jobs: usize,
    },

    /// Analyzes compressed BlockWithIds and shows statistics
    AnalyzeCompressed {
        #[arg(short, long)]
        epoch: u64,
        #[arg(short, long, default_value = "optimized")]
        input_dir: String,
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
        } => build_registry_auto(&cache_dir, &results_dir, max_epoch, 4).await?,

        Commands::RegistrySingle {
            cache_dir,
            results_dir,
            epoch,
        } => {
            build_registry_single(&cache_dir, &results_dir, epoch).await?;
        }

        Commands::Optimize {
            cache_dir,
            results_dir,
            epoch,
            zstd_level,
        } => optimizer::run_car_optimizer(&cache_dir, epoch, &results_dir, zstd_level).await?,

        Commands::ReadCompressed { epoch, input_dir } => {
            read_compressed_blocks(epoch, &input_dir).await?
        }

        Commands::ReadCompressedPar {
            epoch,
            input_dir,
            jobs,
        } => read_compressed_blocks_par(epoch, &input_dir, jobs).await?,

        Commands::AnalyzeCompressed { epoch, input_dir } => {
            analyze_compressed_blocks(epoch, &input_dir).await?
        }
    }

    Ok(())
}