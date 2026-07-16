use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use std::{net::SocketAddr, path::PathBuf};

use blockzilla_token_api::indexer::IndexProfile;
use blockzilla_token_api::{
    api::{ServeConfig, serve},
    indexer::{IndexArchiveV2Config, index_archive_v2},
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliIndexProfile {
    Full,
    PriceApi,
}

impl From<CliIndexProfile> for IndexProfile {
    fn from(value: CliIndexProfile) -> Self {
        match value {
            CliIndexProfile::Full => Self::Full,
            CliIndexProfile::PriceApi => Self::PriceApi,
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "blockzilla-token-api")]
#[command(about = "Index Blockzilla Archive V2 token data and serve a Birdeye-shaped price API")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Parse compressed Archive V2 hot blocks and write compact token/swap indexes.
    IndexArchiveV2 {
        /// archive-v2-blocks.zstd or archive-v2-blocks.wincode.
        input: PathBuf,
        /// Output directory for compact token API files.
        output_dir: PathBuf,
        /// Archive V2 hot-block index path. Defaults beside input.
        #[arg(long)]
        index: Option<PathBuf>,
        /// Pubkey registry path. Defaults to registry.bin beside input.
        #[arg(long)]
        registry: Option<PathBuf>,
        /// Stop after N blocks. Useful for smoke tests.
        #[arg(long)]
        max_blocks: Option<u64>,
        /// Quote mint used for USD price derivation. May be repeated.
        #[arg(long = "quote-mint")]
        quote_mints: Vec<String>,
        /// Extra DEX/router program to tag swap candidates with. May be repeated.
        #[arg(long = "dex-program")]
        dex_programs: Vec<String>,
        /// Output profile. price-api skips the full balance/account dump and keeps token + swap data.
        #[arg(long, value_enum, default_value_t = CliIndexProfile::Full)]
        profile: CliIndexProfile,
    },

    /// Serve Birdeye-shaped token and price routes from a compact index directory.
    Serve {
        /// Directory produced by index-archive-v2.
        index_dir: PathBuf,
        /// Listen address.
        #[arg(
            long,
            default_value = "127.0.0.1:8080",
            env = "BLOCKZILLA_TOKEN_API_LISTEN"
        )]
        listen: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::IndexArchiveV2 {
            input,
            output_dir,
            index,
            registry,
            max_blocks,
            quote_mints,
            dex_programs,
            profile,
        } => {
            index_archive_v2(IndexArchiveV2Config {
                input,
                output_dir,
                index,
                registry,
                max_blocks,
                quote_mints,
                dex_programs,
                profile: profile.into(),
            })?;
        }
        Command::Serve { index_dir, listen } => {
            serve(ServeConfig { index_dir, listen }).await?;
        }
    }

    Ok(())
}
