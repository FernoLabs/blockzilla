use std::{num::NonZeroU32, path::PathBuf};

use anyhow::{Context, Result, ensure};
use blockzilla_archive_token_events::{
    HistoryStart, NetworkConfig, compare_output_databases,
    layout::OutputLayout,
    network::{DEFAULT_USDC_MINT, parse_mint},
    report::{write_json_atomic, write_status},
    run_network,
};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "blockzilla-archive-token-events",
    about = "Read one instruction-only classic SPL Token ledger from CAR, Compact V2, and Indexer V3"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the three archive readers against one public Worker origin.
    Network(NetworkArgs),
    /// Compare three existing epoch databases with bounded mismatch samples.
    Compare(CompareArgs),
}

#[derive(Debug, clap::Args)]
struct NetworkArgs {
    /// Public HTTPS Worker origin. The command derives all three archive paths.
    #[arg(long)]
    origin: String,
    /// Supported sample epoch: 0,100,200,300,400,500,600,700,800,900,1000.
    #[arg(long)]
    epoch: u64,
    /// First canonical block-row ordinal in the selected prefix.
    #[arg(long, default_value_t = 0)]
    first_block: u32,
    /// Maximum canonical block rows to scan from --first-block.
    #[arg(long, default_value_t = NonZeroU32::new(1024).unwrap())]
    max_blocks: NonZeroU32,
    /// Absolute parent folder for isolated car, compact-v2, indexer-v3, and comparison folders.
    #[arg(long)]
    output_root: PathBuf,
    /// Classic SPL Token mint in canonical base58. The default is Solana USDC.
    #[arg(long, default_value = DEFAULT_USDC_MINT)]
    mint: String,
    /// Opening history trust. Use trusted-complete-empty only with external proof.
    #[arg(long, value_enum, default_value_t = HistoryArg::Sparse)]
    history_start: HistoryArg,
    /// Maximum mismatch samples in comparison.json.
    #[arg(long, default_value_t = 20)]
    mismatch_limit: usize,
}

#[derive(Debug, clap::Args)]
struct CompareArgs {
    /// Absolute root that contains each format's epoch folder.
    #[arg(long)]
    output_root: PathBuf,
    /// Epoch folder to compare.
    #[arg(long)]
    epoch: u64,
    /// Maximum mismatch samples in comparison.json.
    #[arg(long, default_value_t = 20)]
    mismatch_limit: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum HistoryArg {
    Sparse,
    /// Assert an exact empty opening account set.
    TrustedCompleteEmpty,
}

impl From<HistoryArg> for HistoryStart {
    fn from(value: HistoryArg) -> Self {
        match value {
            HistoryArg::Sparse => Self::Sparse,
            HistoryArg::TrustedCompleteEmpty => Self::TrustedCompleteEmpty,
        }
    }
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Network(args) => {
            let mint = parse_mint(&args.mint).context("validate --mint")?;
            let result = run_network(&NetworkConfig {
                origin: args.origin,
                epoch: args.epoch,
                first_block: args.first_block,
                max_blocks: args.max_blocks,
                output_root: args.output_root,
                mint,
                history_start: args.history_start.into(),
                mismatch_limit: args.mismatch_limit,
            })?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Command::Compare(args) => {
            ensure!(
                args.mismatch_limit <= 20,
                "mismatch limit must be in 0..=20"
            );
            ensure!(
                args.output_root.is_absolute(),
                "output root must be an absolute path"
            );
            let layout = OutputLayout::new(&args.output_root, args.epoch);
            layout.prepare()?;
            write_status(&layout.comparison_report, args.epoch, None, "running", None)?;
            let report = match (|| -> Result<_> {
                let report = compare_output_databases(
                    &layout.car.database,
                    &layout.compact_v2.database,
                    &layout.indexer_v3.database,
                    args.mismatch_limit,
                )?;
                write_json_atomic(&layout.comparison_report, &report)?;
                Ok(report)
            })() {
                Ok(report) => report,
                Err(error) => {
                    let message = format!("{error:#}");
                    let _ = write_status(
                        &layout.comparison_report,
                        args.epoch,
                        None,
                        "failed",
                        Some(&message),
                    );
                    return Err(error);
                }
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}
