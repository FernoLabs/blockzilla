//mod block_mode;
//mod compat_block;
//mod dump_registry;
//mod network_mode;
//mod optimizer;
//mod print_compressed_block;
mod key_extractor;
mod merge_registry;
mod node_mode;
mod transaction_parser;

use std::path::Path;

use anyhow::Result;
use clap::{Parser, Subcommand};
use indicatif::ProgressBar;
use tracing_subscriber::FmtSubscriber;

use crate::node_mode::run_node_mode;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Node {
        #[arg(short, long)]
        file: String,
    },
    Block {
        #[arg(short, long)]
        file: String,
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
    /// Stream and process CAR files directly from network
    Network {
        /// URL or IP of the remote CAR file or stream
        #[arg(short, long)]
        source: String,

        /// Optional output dir for compressed data
        #[arg(long)]
        output_dir: Option<String>,
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
        Commands::Node { file } => run_node_mode(&file).await?,
        Commands::Block { file } => {
            //run_block_mode(&file).await?
        }
        Commands::Optimize { file, output_dir } => {
            //run_car_optimizer(&file, output_dir).await?
        }
        Commands::Read {
            epoch,
            idx,
            registry,
            slot,
        } => {
            //read_and_print_block_compact(&epoch, &idx, &registry, slot)?
        }
        Commands::Network { source, output_dir } => {
            //network_mode::run_network_optimizer(&source, output_dir).await?
        }
        Commands::DumpRegistry { registry, output } => {
            //dump_registry::dump_registry_to_csv(&registry, &output)?
        }
        Commands::Registry { file, output_dir } => {
            let epoch = extract_epoch_from_path(&file).expect("could not parse epoch");
            let path = output_dir.unwrap_or_else(|| "optimized".to_string());
            let out_dir = Path::new(&path);
            std::fs::create_dir_all(out_dir)?;

            let bin_path = out_dir.join(format!("pubkeys-{epoch:04}.bin"));

            if bin_path.is_dir() {
                return Err(anyhow::anyhow!(
                    "Output path '{}' is a directory, expected a file",
                    bin_path.display()
                ));
            }

            key_extractor::extract_unique_pubkeys_with_pb(
                &Path::new(&file),
                &bin_path,
                epoch,
                ProgressBar::new_spinner(),
            )
            .await?;
        }
        Commands::RegistryAll {
            base_dir,
            output_dir,
        } => key_extractor::extract_all_pubkeys(
            &base_dir.into(),
            &output_dir.unwrap_or("optimized".into()).into(),
            100,
            4,
        ),
        Commands::MergeRegistry { input, output } => {
            let src = std::path::PathBuf::from(input);
            let dest = std::path::PathBuf::from(output);
            merge_registry::merge_bin_to_binary(&src, &dest)?
        }
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
