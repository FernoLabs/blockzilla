mod block_reader;
mod build_registry;
mod file_downloader;
mod optimized_block_reader;
mod optimizer;

use anyhow::{Result, anyhow};
use blockzilla::{carblock_to_compact::MetadataMode, open_epoch::FetchMode};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use crate::{
    block_reader::{read_block, read_block_par},
    build_registry::{build_registry_auto, build_registry_single, merge_registries},
    optimized_block_reader::{
        analyze_compressed_blocks, dump_logs, read_compressed_blocks, read_compressed_blocks_par,
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

    #[command(subcommand)]
    Registry(RegistryCommand),

    #[command(subcommand)]
    Optimize(OptimizeCommand),
}

#[derive(Subcommand, Debug)]
enum RegistryCommand {
    Auto {
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long, default_value = DEFAULT_REGISTRY_DIR)]
        results_dir: String,
        #[arg(long, default_value_t = 900)]
        max_epoch: u64,
        #[arg(
            long,
            default_value_t = false,
            help = "Skip parsing transaction rewards and metadata when building the registry"
        )]
        skip_metadata: bool,
    },

    Build {
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long, default_value = DEFAULT_REGISTRY_DIR)]
        results_dir: String,
        #[arg(
            long,
            default_value_t = false,
            help = "Skip parsing transaction rewards and metadata when building the registry"
        )]
        skip_metadata: bool,
    },

    Info {
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(long, default_value = DEFAULT_REGISTRY_DIR)]
        registry_dir: String,
    },

    Merge {
        #[arg(long, default_value = DEFAULT_REGISTRY_DIR)]
        registry_dir: String,
        #[arg(
            long,
            help = "Directory to write the merged registry (defaults to <registry-dir>/merged)"
        )]
        output_dir: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum OptimizeCommand {
    Car {
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long, default_value = DEFAULT_OPTIMIZED_DIR)]
        results_dir: String,
        #[arg(short, long, default_value = DEFAULT_REGISTRY_DIR)]
        registry_dir: Option<String>,
        #[arg(
            long,
            help = "Parse and embed compact transaction metadata (omit for raw protobuf bytes)",
            conflicts_with = "drop_metadata"
        )]
        include_metadata: bool,
        #[arg(
            long,
            help = "Drop transaction metadata entirely (overrides raw protobuf storage)",
            conflicts_with = "include_metadata"
        )]
        drop_metadata: bool,
        #[arg(value_name = "EPOCH")]
        epoch: u64,
    },

    CarNoRegistry {
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long, default_value = DEFAULT_OPTIMIZED_DIR)]
        results_dir: String,
        #[arg(
            long,
            help = "Parse and embed compact transaction metadata (omit for raw protobuf bytes)",
            conflicts_with = "drop_metadata"
        )]
        include_metadata: bool,
        #[arg(
            long,
            help = "Drop transaction metadata entirely (overrides raw protobuf storage)",
            conflicts_with = "include_metadata"
        )]
        drop_metadata: bool,
        #[arg(value_name = "EPOCH")]
        epoch: u64,
    },

    Epoch {
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(long, default_value = DEFAULT_REGISTRY_DIR)]
        registry_dir: String,
        #[arg(long, default_value = DEFAULT_OPTIMIZED_DIR)]
        optimized_dir: String,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(
            long,
            help = "Parse and embed compact transaction metadata (omit for raw protobuf bytes)",
            conflicts_with = "drop_metadata"
        )]
        include_metadata: bool,
        #[arg(
            long,
            help = "Drop transaction metadata entirely (overrides raw protobuf storage)",
            conflicts_with = "include_metadata"
        )]
        drop_metadata: bool,
    },

    Range {
        #[arg(value_name = "START")]
        start_epoch: u64,
        #[arg(value_name = "END")]
        end_epoch: u64,
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(long, default_value = DEFAULT_REGISTRY_DIR)]
        registry_dir: String,
        #[arg(long, default_value = DEFAULT_OPTIMIZED_DIR)]
        optimized_dir: String,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(
            long,
            help = "Parse and embed compact transaction metadata (omit for raw protobuf bytes)",
            conflicts_with = "drop_metadata"
        )]
        include_metadata: bool,
        #[arg(
            long,
            help = "Drop transaction metadata entirely (overrides raw protobuf storage)",
            conflicts_with = "include_metadata"
        )]
        drop_metadata: bool,
    },

    Read {
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short, long, default_value = DEFAULT_OPTIMIZED_DIR)]
        input_dir: String,
        #[arg(short = 'j', long, default_value_t = 1)]
        jobs: usize,
    },

    Analyze {
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short, long, default_value = DEFAULT_OPTIMIZED_DIR)]
        input_dir: String,
    },

    Logs {
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(short, long, default_value = DEFAULT_OPTIMIZED_DIR)]
        input_dir: String,
        #[arg(short, long)]
        signature: Option<String>,
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
                skip_metadata,
            } => {
                build_registry_auto(&cache_dir, &results_dir, max_epoch, 4, !skip_metadata).await?
            }

            RegistryCommand::Build {
                epoch,
                cache_dir,
                results_dir,
                skip_metadata,
            } => {
                build_registry_single(&cache_dir, &results_dir, epoch, !skip_metadata).await?;
            }

            RegistryCommand::Info { .. } => {
                todo!()
            }

            RegistryCommand::Merge {
                registry_dir,
                output_dir,
            } => {
                let out_dir = output_dir.unwrap_or_else(|| {
                    Path::new(&registry_dir)
                        .join("merged")
                        .to_string_lossy()
                        .into_owned()
                });
                let total = merge_registries(&registry_dir, &out_dir).await?;
                info!(
                    "🧩 merged registries from {} into {} ({} unique keys)",
                    registry_dir, out_dir, total
                );
            }
        },

        Commands::Optimize(cmd) => match cmd {
            OptimizeCommand::Car {
                cache_dir,
                results_dir,
                registry_dir,
                include_metadata,
                drop_metadata,
                epoch,
            } => {
                let metadata_mode = metadata_mode_from_flags(include_metadata, drop_metadata);
                optimizer::run_car_optimizer(
                    &cache_dir,
                    epoch,
                    &results_dir,
                    registry_dir.as_deref(),
                    metadata_mode,
                )
                .await?
            }

            OptimizeCommand::CarNoRegistry {
                cache_dir,
                results_dir,
                include_metadata,
                drop_metadata,
                epoch,
            } => {
                let metadata_mode = metadata_mode_from_flags(include_metadata, drop_metadata);
                optimizer::optimize_epoch_without_registry(
                    &cache_dir,
                    &results_dir,
                    epoch,
                    metadata_mode,
                )
                .await?
            }

            OptimizeCommand::Epoch {
                epoch,
                cache_dir,
                registry_dir,
                optimized_dir,
                force,
                include_metadata,
                drop_metadata,
            } => {
                let metadata_mode = metadata_mode_from_flags(include_metadata, drop_metadata);
                match run_epoch_optimize(
                    &cache_dir,
                    &registry_dir,
                    &optimized_dir,
                    epoch,
                    metadata_mode,
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
                force,
                include_metadata,
                drop_metadata,
            } => {
                if start_epoch > end_epoch {
                    return Err(anyhow!(
                        "start_epoch ({start_epoch}) must be <= end_epoch ({end_epoch})"
                    ));
                }

                let metadata_mode = metadata_mode_from_flags(include_metadata, drop_metadata);
                let mut completed = 0usize;
                let mut skipped = 0usize;
                for epoch in start_epoch..=end_epoch {
                    match run_epoch_optimize(
                        &cache_dir,
                        &registry_dir,
                        &optimized_dir,
                        epoch,
                        metadata_mode,
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

            OptimizeCommand::Analyze { epoch, input_dir } => {
                analyze_compressed_blocks(epoch, &input_dir).await?
            }

            OptimizeCommand::Logs {
                epoch,
                input_dir,
                signature,
            } => dump_logs(epoch, &input_dir, signature.as_deref()).await?,
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

fn metadata_mode_from_flags(include_metadata: bool, drop_metadata: bool) -> MetadataMode {
    if include_metadata {
        MetadataMode::Compact
    } else if drop_metadata {
        MetadataMode::None
    } else {
        MetadataMode::Raw
    }
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
    metadata_mode: MetadataMode,
    force: bool,
) -> Result<OptimizeOutcome> {
    if !force && outputs_exist(registry_dir, optimized_dir, epoch) {
        return Ok(OptimizeOutcome::Skipped);
    }

    let (car_path, downloaded) = ensure_epoch_car(cache_dir, epoch).await?;

    build_registry_single(cache_dir, registry_dir, epoch, true).await?;

    optimizer::run_car_optimizer(
        cache_dir,
        epoch,
        optimized_dir,
        Some(registry_dir),
        metadata_mode,
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
