mod block_reader;
mod build_registry;
mod file_downloader;
mod optimized_block_reader;
mod optimizer;
mod program_stats;
mod token_dump;

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
    optimizer::OptimizedFormat,
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

    #[command(subcommand)]
    Token(TokenCommand),

    #[command(subcommand)]
    Stats(StatsCommand),
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
            help = "Store raw transaction metadata instead of compact encoding",
            conflicts_with = "drop_metadata"
        )]
        raw_metadata: bool,
        #[arg(
            long,
            help = "Drop transaction metadata entirely",
            conflicts_with = "raw_metadata"
        )]
        drop_metadata: bool,
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(long, value_enum, default_value_t = OptimizedFormat::Wincode)]
        format: OptimizedFormat,
    },

    CarNoRegistry {
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long, default_value = DEFAULT_OPTIMIZED_DIR)]
        results_dir: String,
        #[arg(
            long,
            help = "Store raw transaction metadata instead of compact encoding",
            conflicts_with = "drop_metadata"
        )]
        raw_metadata: bool,
        #[arg(
            long,
            help = "Drop transaction metadata entirely",
            conflicts_with = "raw_metadata"
        )]
        drop_metadata: bool,
        #[arg(value_name = "EPOCH")]
        epoch: u64,
        #[arg(long, value_enum, default_value_t = OptimizedFormat::Wincode)]
        format: OptimizedFormat,
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
            help = "Store raw transaction metadata instead of compact encoding",
            conflicts_with = "drop_metadata"
        )]
        raw_metadata: bool,
        #[arg(
            long,
            help = "Drop transaction metadata entirely",
            conflicts_with = "raw_metadata"
        )]
        drop_metadata: bool,
        #[arg(long, value_enum, default_value_t = OptimizedFormat::Wincode)]
        format: OptimizedFormat,
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
            help = "Store raw transaction metadata instead of compact encoding",
            conflicts_with = "drop_metadata"
        )]
        raw_metadata: bool,
        #[arg(
            long,
            help = "Drop transaction metadata entirely",
            conflicts_with = "raw_metadata"
        )]
        drop_metadata: bool,
        #[arg(long, value_enum, default_value_t = OptimizedFormat::Wincode)]
        format: OptimizedFormat,
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

#[derive(Subcommand, Debug)]
enum TokenCommand {
    Dump {
        #[arg(value_name = "START_EPOCH", help = "Starting epoch to scan, inclusive")]
        start_epoch: u64,
        #[arg(long, value_name = "SLOT")]
        start_slot: u64,
        #[arg(long, value_name = "MINT")]
        mint: String,
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long, default_value = "token_transaction_dump.bin")]
        output: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
enum StatsCommand {
    Program {
        #[arg(value_name = "START_EPOCH", help = "Starting epoch to scan, inclusive")]
        start_epoch: u64,
        #[arg(long, value_name = "SLOT", default_value_t = 0)]
        start_slot: u64,
        #[arg(short, long, default_value = DEFAULT_CACHE_DIR)]
        cache_dir: String,
        #[arg(short, long, default_value = "program_usage_stats.bin")]
        output: PathBuf,
        #[arg(
            long,
            value_name = "LIMIT",
            help = "Limit number of programs written to output"
        )]
        limit: Option<usize>,
        #[arg(
            long,
            help = "Only count top-level program instructions (skip metadata and inner instructions)"
        )]
        top_level_only: bool,
    },
    ProgramCsv {
        #[arg(
            short,
            long,
            value_name = "INPUT",
            default_value = "program_usage_stats.bin"
        )]
        input: PathBuf,
        #[arg(
            short,
            long,
            value_name = "OUTPUT",
            default_value = "program_usage_stats.csv"
        )]
        output: PathBuf,
        #[arg(
            long,
            value_name = "LIMIT",
            help = "Limit number of programs written to the CSV"
        )]
        limit: Option<usize>,
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
                raw_metadata,
                drop_metadata,
                epoch,
                format,
            } => {
                let metadata_mode = metadata_mode_from_flags(raw_metadata, drop_metadata);
                optimizer::run_car_optimizer(
                    &cache_dir,
                    epoch,
                    &results_dir,
                    registry_dir.as_deref(),
                    metadata_mode,
                    format,
                )
                .await?
            }

            OptimizeCommand::CarNoRegistry {
                cache_dir,
                results_dir,
                raw_metadata,
                drop_metadata,
                epoch,
                format,
            } => {
                let metadata_mode = metadata_mode_from_flags(raw_metadata, drop_metadata);
                optimizer::optimize_epoch_without_registry(
                    &cache_dir,
                    &results_dir,
                    epoch,
                    metadata_mode,
                    format,
                )
                .await?
            }

            OptimizeCommand::Epoch {
                epoch,
                cache_dir,
                registry_dir,
                optimized_dir,
                force,
                raw_metadata,
                drop_metadata,
                format,
            } => {
                let metadata_mode = metadata_mode_from_flags(raw_metadata, drop_metadata);
                match run_epoch_optimize(
                    &cache_dir,
                    &registry_dir,
                    &optimized_dir,
                    epoch,
                    metadata_mode,
                    force,
                    format,
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
                raw_metadata,
                drop_metadata,
                format,
            } => {
                if start_epoch > end_epoch {
                    return Err(anyhow!(
                        "start_epoch ({start_epoch}) must be <= end_epoch ({end_epoch})"
                    ));
                }

                let metadata_mode = metadata_mode_from_flags(raw_metadata, drop_metadata);
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
                        format,
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

        Commands::Token(cmd) => match cmd {
            TokenCommand::Dump {
                start_epoch,
                start_slot,
                mint,
                cache_dir,
                output,
            } => {
                token_dump::dump_token_transactions(
                    start_epoch,
                    start_slot,
                    &cache_dir,
                    &mint,
                    &output,
                )
                .await?;
            }
        },

        Commands::Stats(cmd) => match cmd {
            StatsCommand::Program {
                start_epoch,
                start_slot,
                cache_dir,
                output,
                limit,
                top_level_only,
            } => {
                program_stats::dump_program_stats(
                    start_epoch,
                    start_slot,
                    &cache_dir,
                    &output,
                    limit,
                    top_level_only,
                )
                .await?;
            }
            StatsCommand::ProgramCsv {
                input,
                output,
                limit,
            } => {
                program_stats::dump_program_stats_csv(&input, &output, limit).await?;
            }
        },
    }

    Ok(())
}

fn registry_outputs_exist(registry_dir: &str, epoch: u64) -> bool {
    let epoch_dir = Path::new(registry_dir).join(format!("epoch-{epoch:04}"));
    epoch_dir.join("keys.bin").exists() && epoch_dir.join("fp2key.bin").exists()
}

fn optimized_outputs_exist(optimized_dir: &str, epoch: u64, format: OptimizedFormat) -> bool {
    let base = Path::new(optimized_dir);

    let direct_blocks = match format {
        OptimizedFormat::Wincode => base.join(format!("epoch-{epoch:04}.bin")),
        OptimizedFormat::Postcard => base.join(format!("epoch-{epoch:04}.postcard")),
        OptimizedFormat::Cbor => base.join(format!("epoch-{epoch:04}.cbor")),
    };

    if direct_blocks.exists() {
        // Older layouts produced a sidecar index file alongside the flat blocks file.
        let direct_idx = base.join(format!("epoch-{epoch:04}.idx"));
        return direct_idx.exists();
    }

    let nested_dir = base.join(format!("epoch-{epoch:04}/optimized"));
    let nested_blocks = match format {
        OptimizedFormat::Wincode => nested_dir.join("blocks.bin"),
        OptimizedFormat::Postcard => nested_dir.join("blocks.postcard"),
        OptimizedFormat::Cbor => nested_dir.join("blocks.cbor"),
    };

    nested_blocks.exists()
}

fn outputs_exist(
    registry_dir: &str,
    optimized_dir: &str,
    epoch: u64,
    format: OptimizedFormat,
) -> bool {
    registry_outputs_exist(registry_dir, epoch)
        && optimized_outputs_exist(optimized_dir, epoch, format)
}

fn metadata_mode_from_flags(raw_metadata: bool, drop_metadata: bool) -> MetadataMode {
    if drop_metadata {
        MetadataMode::None
    } else if raw_metadata {
        MetadataMode::Raw
    } else {
        MetadataMode::Compact
    }
}

async fn ensure_epoch_car(cache_dir: &str, epoch: u64) -> Result<(PathBuf, bool)> {
    let path = Path::new(cache_dir).join(format!("epoch-{epoch}.car"));
    if path.exists() {
        Ok((path, false))
    } else {
        let compressed = Path::new(cache_dir).join(format!("epoch-{epoch}.car.zst"));
        if compressed.exists() {
            Ok((compressed, false))
        } else {
            info!("📥 downloading epoch {epoch:04} to {}", path.display());
            let downloaded = crate::file_downloader::download_epoch(epoch, cache_dir, 3).await?;
            Ok((downloaded, true))
        }
    }
}

async fn run_epoch_optimize(
    cache_dir: &str,
    registry_dir: &str,
    optimized_dir: &str,
    epoch: u64,
    metadata_mode: MetadataMode,
    force: bool,
    format: OptimizedFormat,
) -> Result<OptimizeOutcome> {
    if !force && outputs_exist(registry_dir, optimized_dir, epoch, format) {
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
        format,
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
