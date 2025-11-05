mod block_reader;
mod build_registry;
mod file_downloader;
mod optimized_block_reader;
mod optimizer;
mod transaction_parser;

use anyhow::{Result, anyhow};
use blockzilla::open_epoch::FetchMode;
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::{
    block_reader::{read_block, read_block_par},
    build_registry::{build_registry_auto, build_registry_single, inspect_registry},
    optimized_block_reader::{
        analyze_compressed_blocks, read_compressed_block_with_registry, read_compressed_blocks,
        read_compressed_blocks_par,
    },
};

pub const LOG_INTERVAL_SECS: u64 = 2;
const DEFAULT_CACHE_DIR: &str = "cache";
const DEFAULT_REGISTRY_DIR: &str = "registry";
const DEFAULT_OPTIMIZED_DIR: &str = "optimized";

enum OptimizeOutcome {
    Completed,
    Skipped,
}

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
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long)]
        mode: FetchMode,
        #[arg(short = 'j', long, default_value_t = 1)]
        jobs: usize,
    },

    /// Registry utilities
    #[command(subcommand)]
    Registry(RegistryCommand),

    /// Optimize workflows for CAR epochs and compressed archives
    #[command(subcommand)]
    Optimize(OptimizeCommand),
}

#[derive(Subcommand, Debug)]
enum RegistryCommand {
    /// Builds the registry automatically, downloading and cleaning up each epoch
    Auto {
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long, default_value = DEFAULT_REGISTRY_DIR)]
        results_dir: String,
        #[arg(long, default_value_t = 900)]
        max_epoch: u64,
    },

    /// Builds the registry for a single epoch
    Build {
        /// Epoch number to process
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long, default_value = DEFAULT_REGISTRY_DIR)]
        results_dir: String,
    },

    /// Prints basic statistics about a registry
    Info {
        /// Epoch number to inspect
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(long, default_value = DEFAULT_REGISTRY_DIR)]
        registry_dir: String,
    },
}

#[derive(Subcommand, Debug)]
enum OptimizeCommand {
    /// Optimizes a CAR epoch into compressed BlockWithIds format
    Car {
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long, default_value = DEFAULT_OPTIMIZED_DIR)]
        results_dir: String,
        #[arg(long)]
        registry_dir: Option<String>,
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short = 'z', long, default_value_t = 0)]
        zstd_level: i32,
    },

    /// Runs download → registry → optimize for a single epoch
    Epoch {
        /// Epoch number to process
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(long, default_value = DEFAULT_REGISTRY_DIR)]
        registry_dir: String,
        #[arg(long, default_value = DEFAULT_OPTIMIZED_DIR)]
        optimized_dir: String,
        #[arg(short = 'z', long, default_value_t = 0)]
        zstd_level: i32,
        #[arg(long, default_value_t = false)]
        force: bool,
    },

    /// Runs download → registry → optimize for a range of epochs
    Range {
        /// First epoch in the range
        #[arg(value_name = "START")]
        start_epoch: u64,
        /// Last epoch in the range (inclusive)
        #[arg(value_name = "END")]
        end_epoch: u64,
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(long, default_value = DEFAULT_REGISTRY_DIR)]
        registry_dir: String,
        #[arg(long, default_value = DEFAULT_OPTIMIZED_DIR)]
        optimized_dir: String,
        #[arg(short = 'z', long, default_value_t = 0)]
        zstd_level: i32,
        #[arg(long, default_value_t = false)]
        force: bool,
    },

    /// Reads compressed BlockWithIds format
    Read {
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short, long, default_value = DEFAULT_OPTIMIZED_DIR)]
        input_dir: String,
        #[arg(short = 'j', long, default_value_t = 1)]
        jobs: usize,
    },

    /// Reads a single block from the compressed archive using the registry
    ReadBlock {
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short, long, value_name = "SLOT")]
        slot: u64,
        #[arg(short, long, default_value = DEFAULT_OPTIMIZED_DIR)]
        input_dir: String,
        #[arg(long)]
        registry_dir: Option<String>,
    },

    /// Analyzes compressed BlockWithIds and shows statistics
    Analyze {
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short, long, default_value = DEFAULT_OPTIMIZED_DIR)]
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

        Commands::Registry(cmd) => match cmd {
            RegistryCommand::Auto {
                cache_dir,
                results_dir,
                max_epoch,
            } => build_registry_auto(&cache_dir, &results_dir, max_epoch, 4).await?,

            RegistryCommand::Build {
                epoch,
                cache_dir,
                results_dir,
            } => {
                build_registry_single(&cache_dir, &results_dir, epoch).await?;
            }

            RegistryCommand::Info {
                epoch,
                registry_dir,
            } => {
                inspect_registry(&registry_dir, epoch).await?;
            }
        },

        Commands::Optimize(cmd) => match cmd {
            OptimizeCommand::Car {
                cache_dir,
                results_dir,
                registry_dir,
                epoch,
                zstd_level,
            } => {
                optimizer::run_car_optimizer(
                    &cache_dir,
                    epoch,
                    &results_dir,
                    registry_dir.as_deref(),
                    zstd_level,
                )
                .await?
            }

            OptimizeCommand::Epoch {
                epoch,
                cache_dir,
                registry_dir,
                optimized_dir,
                zstd_level,
                force,
            } => {
                match run_epoch_optimize(
                    &cache_dir,
                    &registry_dir,
                    &optimized_dir,
                    epoch,
                    zstd_level,
                    force,
                )
                .await?
                {
                    OptimizeOutcome::Completed => {
                        info!("✅ epoch {epoch:04} processed");
                    }
                    OptimizeOutcome::Skipped => {
                        info!("⏭️ epoch {epoch:04} already processed (use --force to rerun)");
                    }
                }
            }

            OptimizeCommand::Range {
                start_epoch,
                end_epoch,
                cache_dir,
                registry_dir,
                optimized_dir,
                zstd_level,
                force,
            } => {
                if start_epoch > end_epoch {
                    return Err(anyhow!(
                        "start_epoch ({start_epoch}) must be <= end_epoch ({end_epoch})"
                    ));
                }

                let mut completed = 0usize;
                let mut skipped = 0usize;
                for epoch in start_epoch..=end_epoch {
                    match run_epoch_optimize(
                        &cache_dir,
                        &registry_dir,
                        &optimized_dir,
                        epoch,
                        zstd_level,
                        force,
                    )
                    .await
                    {
                        Ok(OptimizeOutcome::Completed) => {
                            completed += 1;
                            info!("✅ epoch {epoch:04} processed");
                        }
                        Ok(OptimizeOutcome::Skipped) => {
                            skipped += 1;
                            info!("⏭️ epoch {epoch:04} already processed");
                        }
                        Err(e) => {
                            return Err(anyhow!("epoch {epoch:04} failed: {e}"));
                        }
                    }
                }

                info!(
                    "🏁 completed range {start_epoch:04}-{end_epoch:04} | {completed} processed | {skipped} skipped"
                );
            }

            OptimizeCommand::Read {
                epoch,
                input_dir,
                jobs,
            } => {
                if jobs <= 1 {
                    read_compressed_blocks(epoch, &input_dir).await?
                } else {
                    read_compressed_blocks_par(epoch, &input_dir, jobs).await?
                }
            }

            OptimizeCommand::ReadBlock {
                epoch,
                slot,
                input_dir,
                registry_dir,
            } => {
                let registry_dir = registry_dir.unwrap_or_else(|| input_dir.clone());
                read_compressed_block_with_registry(epoch, slot, &input_dir, &registry_dir).await?
            }

            OptimizeCommand::Analyze { epoch, input_dir } => {
                analyze_compressed_blocks(epoch, &input_dir).await?
            }
        },
    }

    Ok(())
}

fn registry_paths(registry_dir: &str, epoch: u64) -> (PathBuf, PathBuf) {
    let base = Path::new(registry_dir);
    (
        base.join(format!("registry-{epoch:04}.bin")),
        base.join(format!("registry-pubkeys-{epoch:04}.bin")),
    )
}

fn optimized_paths(optimized_dir: &str, epoch: u64) -> (PathBuf, PathBuf) {
    let base = Path::new(optimized_dir);
    (
        base.join(format!("epoch-{epoch:04}.bin")),
        base.join(format!("epoch-{epoch:04}.idx")),
    )
}

fn outputs_exist(registry_dir: &str, optimized_dir: &str, epoch: u64) -> bool {
    let (registry_bin, registry_keys) = registry_paths(registry_dir, epoch);
    let (optimized_bin, optimized_idx) = optimized_paths(optimized_dir, epoch);
    registry_bin.exists()
        && registry_keys.exists()
        && optimized_bin.exists()
        && optimized_idx.exists()
}

async fn ensure_epoch_car(cache_dir: &str, epoch: u64) -> Result<(PathBuf, bool)> {
    let path = Path::new(cache_dir).join(format!("epoch-{epoch}.car"));
    if path.exists() {
        Ok((path, false))
    } else {
        info!("📥 downloading epoch {epoch:04} to {}", path.display());
        let downloaded = crate::file_downloader::download_epoch(epoch, cache_dir, 3).await?;
        Ok((downloaded, true))
    }
}

async fn run_epoch_optimize(
    cache_dir: &str,
    registry_dir: &str,
    optimized_dir: &str,
    epoch: u64,
    zstd_level: i32,
    force: bool,
) -> Result<OptimizeOutcome> {
    if !force && outputs_exist(registry_dir, optimized_dir, epoch) {
        return Ok(OptimizeOutcome::Skipped);
    }

    let (car_path, downloaded) = ensure_epoch_car(cache_dir, epoch).await?;

    build_registry_single(cache_dir, registry_dir, epoch).await?;

    optimizer::run_car_optimizer(
        cache_dir,
        epoch,
        optimized_dir,
        Some(registry_dir),
        zstd_level,
    )
    .await?;

    if downloaded {
        if let Err(err) = tokio::fs::remove_file(&car_path).await {
            warn!(
                "⚠️ failed to delete downloaded CAR {}: {}",
                car_path.display(),
                err
            );
        } else {
            info!("🧹 removed downloaded CAR {}", car_path.display());
        }
    }

    Ok(OptimizeOutcome::Completed)
}
