use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use blockzilla_spyx_query::{
    BuildConfig, DEFAULT_SOLANA_RPC_URL, DEFAULT_USD_QUOTE_MINTS, MarketBuildConfig,
    MarketOpenOptions, MarketStore, MintMetadataBuildConfig, MintMetadataStore,
    OwnerPostingsBuildConfig, OwnerPostingsStore, PostingsBuildConfig, PostingsOpenOptions,
    PostingsStore, QueryOpenOptions, QueryStore, ServeConfig, build_index, build_market,
    build_mint_metadata, build_owner_postings, build_postings, serve_with_all_indexes,
    verify_index, verify_owner_postings_artifact, verify_postings_artifact,
};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "blockzilla-spyx-query")]
#[command(about = "Build and serve read-only SPYx transaction and market indexes")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Build an immutable query index.
    Build {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// New or empty index directory.
        #[arg(long)]
        index: PathBuf,
        /// Stop after this many transactions and mark the index as a canary.
        #[arg(long)]
        max_transactions: Option<u64>,
    },
    /// Verify all source and index hashes.
    Verify {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// Query index directory.
        #[arg(long)]
        index: PathBuf,
        /// Permit an incomplete canary index.
        #[arg(long)]
        allow_incomplete: bool,
    },
    /// Build immutable target-address and program posting lists.
    BuildPostings {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// New or empty postings directory.
        #[arg(long)]
        postings: PathBuf,
        /// Stop after this many transactions and mark the postings as a canary.
        #[arg(long)]
        max_transactions: Option<u64>,
    },
    /// Verify all source and postings hashes.
    VerifyPostings {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// Postings directory.
        #[arg(long)]
        postings: PathBuf,
        /// Permit incomplete canary postings.
        #[arg(long)]
        allow_incomplete: bool,
    },
    /// Build immutable owner-to-transaction posting lists from strict replay.
    BuildOwnerPostings {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// New or empty owner postings directory.
        #[arg(long)]
        owner_postings: PathBuf,
        /// Stop after this many transactions and mark the owner postings as a canary.
        #[arg(long)]
        max_transactions: Option<u64>,
    },
    /// Verify strict-replay owner postings and all source bindings.
    VerifyOwnerPostings {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// Owner postings directory.
        #[arg(long)]
        owner_postings: PathBuf,
        /// Permit incomplete canary owner postings.
        #[arg(long)]
        allow_incomplete: bool,
    },
    /// Build an immutable, execution-proven SPYx market database.
    BuildMarket {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// New or empty market database directory.
        #[arg(long)]
        market: PathBuf,
        /// Stop after this many transactions and mark the market database as a canary.
        #[arg(long)]
        max_transactions: Option<u64>,
        /// Stable quote mint. Repeat this option to add more than one mint.
        #[arg(long = "usd-quote-mint")]
        usd_quote_mints: Vec<String>,
    },
    /// Verify the source binding and all market database identities.
    VerifyMarket {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// Market database directory.
        #[arg(long)]
        market: PathBuf,
        /// Permit an incomplete canary market database.
        #[arg(long)]
        allow_incomplete: bool,
    },
    /// Fetch immutable on-chain metadata for every mint in proven swaps.
    BuildMintMetadata {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// Execution-proven market database directory.
        #[arg(long)]
        market: PathBuf,
        /// New or empty mint metadata directory.
        #[arg(long)]
        output: PathBuf,
        /// Solana mainnet JSON-RPC endpoint.
        #[arg(long, env = "SOLANA_RPC_URL", default_value = DEFAULT_SOLANA_RPC_URL)]
        rpc_url: String,
        /// Maximum accounts in one JSON-RPC request.
        #[arg(long, default_value_t = 100)]
        batch_size: usize,
    },
    /// Verify a mint metadata artifact against its exact market source.
    VerifyMintMetadata {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// Execution-proven market database directory.
        #[arg(long)]
        market: PathBuf,
        /// Mint metadata directory or manifest file.
        #[arg(long)]
        metadata: PathBuf,
    },
    /// Serve the read-only HTTP API.
    Serve {
        /// Completed schema-3 dump directory.
        #[arg(long)]
        dump: PathBuf,
        /// Query index directory.
        #[arg(long)]
        index: PathBuf,
        /// Optional target-address and program postings directory.
        #[arg(long)]
        postings: Option<PathBuf>,
        /// Optional strict-replay owner postings directory.
        #[arg(long)]
        owner_postings: Option<PathBuf>,
        /// Optional execution-proven market database directory.
        #[arg(long)]
        market: Option<PathBuf>,
        /// Optional market-bound mint metadata directory or manifest.
        #[arg(long)]
        mint_metadata: Option<PathBuf>,
        /// Local address and port for the HTTP service.
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: SocketAddr,
        /// Allowed browser origin. Use * to permit all origins.
        #[arg(long, default_value = "*")]
        cors_origin: String,
        /// Optional static explorer directory to serve at the site root.
        #[arg(long)]
        static_dir: Option<PathBuf>,
        /// Maximum number of source reads that can run at the same time.
        #[arg(long, default_value_t = 4)]
        max_blocking_reads: usize,
        /// Permit incomplete canary index, postings, and market artifacts.
        #[arg(long)]
        allow_incomplete: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Build {
            dump,
            index,
            max_transactions,
        } => {
            let summary = build_index(&BuildConfig {
                dump,
                output: index,
                max_transactions,
            })?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        Command::Verify {
            dump,
            index,
            allow_incomplete,
        } => {
            let summary = verify_index(&dump, &index, allow_incomplete)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        Command::BuildPostings {
            dump,
            postings,
            max_transactions,
        } => {
            let summary = build_postings(&PostingsBuildConfig {
                dump,
                output: postings,
                max_transactions,
            })?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        Command::VerifyPostings {
            dump,
            postings,
            allow_incomplete,
        } => {
            let summary = verify_postings_artifact(&dump, &postings, allow_incomplete)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        Command::BuildOwnerPostings {
            dump,
            owner_postings,
            max_transactions,
        } => {
            let summary = build_owner_postings(&OwnerPostingsBuildConfig {
                dump,
                output: owner_postings,
                max_transactions,
            })?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        Command::VerifyOwnerPostings {
            dump,
            owner_postings,
            allow_incomplete,
        } => {
            let summary = verify_owner_postings_artifact(&dump, &owner_postings, allow_incomplete)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        Command::BuildMarket {
            dump,
            market,
            max_transactions,
            usd_quote_mints,
        } => {
            let usd_quote_mints = if usd_quote_mints.is_empty() {
                DEFAULT_USD_QUOTE_MINTS
                    .iter()
                    .map(|mint| (*mint).to_owned())
                    .collect()
            } else {
                usd_quote_mints
            };
            let summary = build_market(&MarketBuildConfig {
                dump,
                output: market,
                max_transactions,
                usd_quote_mints,
            })?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        Command::VerifyMarket {
            dump,
            market,
            allow_incomplete,
        } => {
            let store = MarketStore::open_with_options(
                &dump,
                &market,
                MarketOpenOptions { allow_incomplete },
            )?;
            store.verify_identities()?;
            println!("{}", serde_json::to_string_pretty(&store.health())?);
        }
        Command::BuildMintMetadata {
            dump,
            market,
            output,
            rpc_url,
            batch_size,
        } => {
            let mut config = MintMetadataBuildConfig::mainnet(dump, market, output, rpc_url);
            config.batch_size = batch_size;
            let summary = build_mint_metadata(&config).await?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        Command::VerifyMintMetadata {
            dump,
            market,
            metadata,
        } => {
            let market = MarketStore::open(&dump, &market)?;
            let store = MintMetadataStore::open(&market, &metadata)?;
            store.verify_identity()?;
            market.verify_identities()?;
            println!("{}", serde_json::to_string_pretty(&store.health())?);
        }
        Command::Serve {
            dump,
            index,
            postings,
            owner_postings,
            market,
            mint_metadata,
            bind,
            cors_origin,
            static_dir,
            max_blocking_reads,
            allow_incomplete,
        } => {
            let store = Arc::new(QueryStore::open_with_options(
                &dump,
                &index,
                QueryOpenOptions { allow_incomplete },
            )?);
            let postings = postings
                .as_deref()
                .map(|path| {
                    PostingsStore::open_with_options(
                        &dump,
                        path,
                        PostingsOpenOptions { allow_incomplete },
                    )
                    .map(Arc::new)
                })
                .transpose()?;
            let owner_postings = owner_postings
                .as_deref()
                .map(|path| {
                    OwnerPostingsStore::open_with_options(
                        &dump,
                        path,
                        PostingsOpenOptions { allow_incomplete },
                    )
                    .map(Arc::new)
                })
                .transpose()?;
            let market = market
                .as_deref()
                .map(|path| {
                    MarketStore::open_with_options(
                        &dump,
                        path,
                        MarketOpenOptions { allow_incomplete },
                    )
                    .map(Arc::new)
                })
                .transpose()?;
            let mint_metadata = mint_metadata
                .as_deref()
                .map(|path| {
                    let market = market
                        .as_deref()
                        .context("--mint-metadata requires --market")?;
                    MintMetadataStore::open(market, path).map(Arc::new)
                })
                .transpose()?;
            serve_with_all_indexes(
                store,
                postings,
                owner_postings,
                market,
                mint_metadata,
                ServeConfig {
                    bind,
                    cors_origin,
                    max_blocking_reads,
                    static_dir,
                },
            )
            .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_build_postings_command() {
        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "build-postings",
            "--dump",
            "/dump",
            "--postings",
            "/postings",
            "--max-transactions",
            "250000",
        ])
        .expect("parse build-postings command");
        let Command::BuildPostings {
            dump,
            postings,
            max_transactions,
        } = cli.command
        else {
            panic!("expected build-postings command");
        };
        assert_eq!(dump, PathBuf::from("/dump"));
        assert_eq!(postings, PathBuf::from("/postings"));
        assert_eq!(max_transactions, Some(250_000));
    }

    #[test]
    fn parses_serve_with_optional_postings() {
        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "serve",
            "--dump",
            "/dump",
            "--index",
            "/index",
            "--postings",
            "/postings",
            "--allow-incomplete",
        ])
        .expect("parse serve command");
        let Command::Serve {
            dump,
            index,
            postings,
            allow_incomplete,
            ..
        } = cli.command
        else {
            panic!("expected serve command");
        };
        assert_eq!(dump, PathBuf::from("/dump"));
        assert_eq!(index, PathBuf::from("/index"));
        assert_eq!(postings, Some(PathBuf::from("/postings")));
        assert!(allow_incomplete);
    }

    #[test]
    fn parses_verify_postings_command() {
        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "verify-postings",
            "--dump",
            "/dump",
            "--postings",
            "/postings",
            "--allow-incomplete",
        ])
        .expect("parse verify-postings command");
        let Command::VerifyPostings {
            dump,
            postings,
            allow_incomplete,
        } = cli.command
        else {
            panic!("expected verify-postings command");
        };
        assert_eq!(dump, PathBuf::from("/dump"));
        assert_eq!(postings, PathBuf::from("/postings"));
        assert!(allow_incomplete);
    }

    #[test]
    fn parses_owner_postings_build_verify_and_serve_options() {
        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "build-owner-postings",
            "--dump",
            "/dump",
            "--owner-postings",
            "/owners",
            "--max-transactions",
            "1000",
        ])
        .expect("parse build-owner-postings command");
        let Command::BuildOwnerPostings {
            dump,
            owner_postings,
            max_transactions,
        } = cli.command
        else {
            panic!("expected build-owner-postings command");
        };
        assert_eq!(dump, PathBuf::from("/dump"));
        assert_eq!(owner_postings, PathBuf::from("/owners"));
        assert_eq!(max_transactions, Some(1_000));

        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "verify-owner-postings",
            "--dump",
            "/dump",
            "--owner-postings",
            "/owners",
            "--allow-incomplete",
        ])
        .expect("parse verify-owner-postings command");
        let Command::VerifyOwnerPostings {
            dump,
            owner_postings,
            allow_incomplete,
        } = cli.command
        else {
            panic!("expected verify-owner-postings command");
        };
        assert_eq!(dump, PathBuf::from("/dump"));
        assert_eq!(owner_postings, PathBuf::from("/owners"));
        assert!(allow_incomplete);

        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "serve",
            "--dump",
            "/dump",
            "--index",
            "/index",
            "--owner-postings",
            "/owners",
        ])
        .expect("parse serve owner-postings option");
        let Command::Serve { owner_postings, .. } = cli.command else {
            panic!("expected serve command");
        };
        assert_eq!(owner_postings, Some(PathBuf::from("/owners")));
    }

    #[test]
    fn parses_build_market_command_and_repeated_quote_mints() {
        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "build-market",
            "--dump",
            "/dump",
            "--market",
            "/market",
            "--max-transactions",
            "50000",
            "--usd-quote-mint",
            DEFAULT_USD_QUOTE_MINTS[0],
            "--usd-quote-mint",
            DEFAULT_USD_QUOTE_MINTS[1],
        ])
        .expect("parse build-market command");
        let Command::BuildMarket {
            dump,
            market,
            max_transactions,
            usd_quote_mints,
        } = cli.command
        else {
            panic!("expected build-market command");
        };
        assert_eq!(dump, PathBuf::from("/dump"));
        assert_eq!(market, PathBuf::from("/market"));
        assert_eq!(max_transactions, Some(50_000));
        assert_eq!(
            usd_quote_mints,
            DEFAULT_USD_QUOTE_MINTS
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn parses_verify_market_command() {
        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "verify-market",
            "--dump",
            "/dump",
            "--market",
            "/market",
            "--allow-incomplete",
        ])
        .expect("parse verify-market command");
        let Command::VerifyMarket {
            dump,
            market,
            allow_incomplete,
        } = cli.command
        else {
            panic!("expected verify-market command");
        };
        assert_eq!(dump, PathBuf::from("/dump"));
        assert_eq!(market, PathBuf::from("/market"));
        assert!(allow_incomplete);
    }

    #[test]
    fn parses_serve_with_market() {
        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "serve",
            "--dump",
            "/dump",
            "--index",
            "/index",
            "--market",
            "/market",
            "--static-dir",
            "/site",
        ])
        .expect("parse serve market option");
        let Command::Serve {
            market, static_dir, ..
        } = cli.command
        else {
            panic!("expected serve command");
        };
        assert_eq!(market, Some(PathBuf::from("/market")));
        assert_eq!(static_dir, Some(PathBuf::from("/site")));
    }

    #[test]
    fn parses_build_and_verify_mint_metadata_commands() {
        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "build-mint-metadata",
            "--dump",
            "/dump",
            "--market",
            "/market",
            "--output",
            "/metadata",
            "--rpc-url",
            "https://rpc.example.invalid",
            "--batch-size",
            "50",
        ])
        .expect("parse build-mint-metadata command");
        let Command::BuildMintMetadata {
            dump,
            market,
            output,
            rpc_url,
            batch_size,
        } = cli.command
        else {
            panic!("expected build-mint-metadata command");
        };
        assert_eq!(dump, PathBuf::from("/dump"));
        assert_eq!(market, PathBuf::from("/market"));
        assert_eq!(output, PathBuf::from("/metadata"));
        assert_eq!(rpc_url, "https://rpc.example.invalid");
        assert_eq!(batch_size, 50);

        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "verify-mint-metadata",
            "--dump",
            "/dump",
            "--market",
            "/market",
            "--metadata",
            "/metadata",
        ])
        .expect("parse verify-mint-metadata command");
        let Command::VerifyMintMetadata {
            dump,
            market,
            metadata,
        } = cli.command
        else {
            panic!("expected verify-mint-metadata command");
        };
        assert_eq!(dump, PathBuf::from("/dump"));
        assert_eq!(market, PathBuf::from("/market"));
        assert_eq!(metadata, PathBuf::from("/metadata"));
    }

    #[test]
    fn parses_serve_with_mint_metadata() {
        let cli = Cli::try_parse_from([
            "blockzilla-spyx-query",
            "serve",
            "--dump",
            "/dump",
            "--index",
            "/index",
            "--market",
            "/market",
            "--mint-metadata",
            "/metadata",
        ])
        .expect("parse serve mint metadata option");
        let Command::Serve {
            market,
            mint_metadata,
            ..
        } = cli.command
        else {
            panic!("expected serve command");
        };
        assert_eq!(market, Some(PathBuf::from("/market")));
        assert_eq!(mint_metadata, Some(PathBuf::from("/metadata")));
    }
}
