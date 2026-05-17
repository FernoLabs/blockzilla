use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use of_signature_index::{build_index, build_indexes_in_dir, get_transaction_response};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "of-signature-index")]
#[command(about = "Build and query a transaction signature index for CAR files")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Build a signature index from a .car or .car.zst file.
    Build {
        #[arg(long)]
        car: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
    },

    /// Build one index per epoch from a folder containing epoch-*.car / epoch-*.car.zst files.
    BuildAll {
        #[arg(long)]
        car_dir: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
        /// Skip epochs that already have a complete index in out-dir.
        #[arg(long)]
        resume: bool,
    },

    /// Query a transaction by Solana base58 signature.
    GetTransaction {
        #[arg(long)]
        index_dir: PathBuf,
        #[arg(long)]
        signature: String,
        /// Optional CAR path override. If omitted, the path recorded in the index is used.
        #[arg(long)]
        car: Option<PathBuf>,
        /// Fetch the transaction bytes from old faithful with an HTTP range request.
        #[arg(long)]
        network: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Build { car, out_dir } => {
            let meta = build_index(&car, &out_dir)?;
            println!(
                "indexed {} transactions from {} (skipped {})",
                meta.indexed_txs,
                car.display(),
                meta.skipped_txs
            );
            println!("index written to {}", out_dir.display());
            Ok(())
        }
        Cmd::BuildAll {
            car_dir,
            out_dir,
            resume,
        } => {
            let results = build_indexes_in_dir(&car_dir, &out_dir, resume)?;
            let mut built = 0usize;
            let mut reused = 0usize;
            for result in &results {
                if result.reused_existing {
                    reused += 1;
                    println!(
                        "epoch {}: reused existing index for {} (skipped {}) -> {}",
                        result.epoch,
                        result.car_path.display(),
                        result.skipped_txs,
                        result.index_dir.display()
                    );
                } else {
                    built += 1;
                    println!(
                        "epoch {}: indexed {} transactions from {} (skipped {}) -> {}",
                        result.epoch,
                        result.indexed_txs,
                        result.car_path.display(),
                        result.skipped_txs,
                        result.index_dir.display()
                    );
                }
            }
            println!(
                "processed {} epoch indexes under {} (built {}, reused {})",
                results.len(),
                out_dir.display(),
                built,
                reused
            );
            Ok(())
        }
        Cmd::GetTransaction {
            index_dir,
            signature,
            car,
            network,
        } => {
            let response = get_transaction_response(&index_dir, &signature, car, network)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&response).context("render response json")?
            );
            Ok(())
        }
    }
}
