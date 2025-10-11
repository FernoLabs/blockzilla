//mod block_mode;
//mod compat_block;
//mod dump_registry;
//mod key_extractor;
mod key_extractor_ssd;
//mod network_mode;
mod node_mode;
//mod optimizer;
mod transaction_parser;
//mod print_compressed_block;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use crate::{
    //    block_mode::run_block_mode, key_extractor::build_registry_hdd_parallel,
    node_mode::run_node_mode,
    //optimizer::run_car_optimizer,
    //  print_compressed_block::read_and_print_block_compact,
};

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
    },
    RegistrySsd {
        #[arg(long)]
        file: String,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize global logger
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG) // or INFO
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set tracing subscriber");

    tracing::info!("TEST info log");

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
        Commands::Registry { file } => {
            //build_registry_hdd_parallel(&file, Some("optimized".to_string())).await?
        }
        Commands::DumpRegistry { registry, output } => {
            //dump_registry::dump_registry_to_csv(&registry, &output)?
        }
        Commands::RegistrySsd { file, output_dir } => {
            key_extractor_ssd::extract_unique_pubkeys(&file, output_dir).await?
        }
    }

    Ok(())
}
