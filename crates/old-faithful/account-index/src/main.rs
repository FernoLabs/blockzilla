use anyhow::Result;
use clap::{Parser, Subcommand};
use of_account_index::{
    BatchConfig, DumpConfig, MergeConfig, ProfileConfig, ProfileMode, dump_accounts,
    dump_accounts_in_dir, merge_first_seen, profile_car,
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "of-account-index")]
#[command(about = "Dump Solana transaction accounts and first-seen funders")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Dump one epoch CAR into accounts.txt, first-seen.tsv, and meta.json.
    Dump {
        #[arg(long)]
        car: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
        /// Number of first-seen reduction buckets. Must be a power of two. 0 uses default.
        #[arg(long, default_value_t = 0)]
        buckets: usize,
        /// Max unique accounts held in RAM before spilling. 0 uses default.
        #[arg(long, default_value_t = 0)]
        flush_accounts: usize,
        /// Also write signatures.bin with tx signatures and CAR offsets.
        #[arg(long)]
        signatures: bool,
        /// Read compressed CAR bytes through a background prefetch queue.
        #[arg(long)]
        prefetch: bool,
    },

    /// Dump every epoch-N.car/.car.zst in a directory, optionally in parallel.
    DumpAll {
        #[arg(long)]
        car_dir: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
        #[arg(long)]
        resume: bool,
        /// Parallel epoch jobs. On the 8 GB box, start with 1 or 2.
        #[arg(long, default_value_t = 1)]
        jobs: usize,
        /// Number of first-seen reduction buckets per epoch. Must be a power of two. 0 uses default.
        #[arg(long, default_value_t = 0)]
        buckets: usize,
        /// Max unique accounts held in RAM per job before spilling. 0 uses default.
        #[arg(long, default_value_t = 0)]
        flush_accounts: usize,
        /// Also write signatures.bin per epoch with tx signatures and CAR offsets.
        #[arg(long)]
        signatures: bool,
        /// Read compressed CAR bytes through a background prefetch queue.
        #[arg(long)]
        prefetch: bool,
    },

    /// Merge per-epoch first-seen.tsv files into one global first-seen account table.
    MergeFirstSeen {
        #[arg(long)]
        dump_dir: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
        /// Number of global merge buckets. Must be a power of two. 0 uses default.
        #[arg(long, default_value_t = 0)]
        buckets: usize,
    },

    /// Profile one CAR through isolated reader/parser stages without producing final dumps.
    Profile {
        #[arg(long)]
        car: PathBuf,
        /// raw-read, decode-read, car-framing, node-decode, tx-parse, or account-spool.
        #[arg(long, default_value = "tx-parse")]
        mode: ProfileMode,
        /// Stop after this many CAR entries. 0 means no entry limit.
        #[arg(long, default_value_t = 0)]
        max_entries: usize,
        /// Stop after this many transaction nodes. 0 means no tx limit.
        #[arg(long, default_value_t = 0)]
        max_txs: usize,
        /// For raw-read/decode-read, stop after this many copied bytes. 0 means no byte limit.
        #[arg(long, default_value_t = 0)]
        max_bytes: u64,
        /// Temp bucket dir for account-spool mode. Removed after the run.
        #[arg(long)]
        temp_dir: Option<PathBuf>,
        /// Read compressed CAR bytes through a background prefetch queue.
        #[arg(long)]
        prefetch: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Dump {
            car,
            out_dir,
            buckets,
            flush_accounts,
            signatures,
            prefetch,
        } => {
            let summary = dump_accounts(DumpConfig {
                car_path: car.clone(),
                out_dir: out_dir.clone(),
                bucket_count: buckets,
                flush_account_threshold: flush_accounts,
                dump_signatures: signatures,
                prefetch,
            })?;
            println!(
                "scanned {} txs from {} and wrote {} unique accounts to {} in {:.1}s",
                summary.scanned_txs,
                car.display(),
                summary.unique_accounts,
                summary.accounts_path,
                summary.elapsed_secs
            );
            println!("first-seen funder candidates: {}", summary.first_seen_path);
            if let (Some(path), Some(record_bytes)) = (
                summary.signatures_path.as_ref(),
                summary.signature_record_bytes,
            ) {
                println!(
                    "signature offset records: {} ({} bytes/record, {})",
                    path, record_bytes, summary.offset_kind
                );
            }
            println!("metadata parsing: disabled");
            println!("inner instruction parsing: disabled");
        }
        Cmd::DumpAll {
            car_dir,
            out_dir,
            resume,
            jobs,
            buckets,
            flush_accounts,
            signatures,
            prefetch,
        } => {
            let results = dump_accounts_in_dir(BatchConfig {
                car_dir: car_dir.clone(),
                out_dir: out_dir.clone(),
                resume,
                jobs,
                bucket_count: buckets,
                flush_account_threshold: flush_accounts,
                dump_signatures: signatures,
                prefetch,
            })?;
            let mut built = 0usize;
            let mut reused = 0usize;
            for result in &results {
                if result.reused_existing {
                    reused += 1;
                    println!(
                        "epoch {}: reused {} -> {} ({} accounts)",
                        result.epoch,
                        result.car_path.display(),
                        result.out_dir.display(),
                        result.unique_accounts
                    );
                } else {
                    built += 1;
                    println!(
                        "epoch {}: dumped {} unique accounts from {} txs in {:.1}s for {} -> {}",
                        result.epoch,
                        result.unique_accounts,
                        result.scanned_txs,
                        result.elapsed_secs,
                        result.car_path.display(),
                        result.out_dir.display()
                    );
                }
            }
            println!(
                "processed {} epoch dumps under {} (built {}, reused {}, jobs {})",
                results.len(),
                out_dir.display(),
                built,
                reused,
                jobs.max(1)
            );
        }
        Cmd::MergeFirstSeen {
            dump_dir,
            out_dir,
            buckets,
        } => {
            let summary = merge_first_seen(MergeConfig {
                dump_dir: dump_dir.clone(),
                out_dir: out_dir.clone(),
                bucket_count: buckets,
            })?;
            println!(
                "merged {} rows from {} epoch dirs into {} unique accounts at {} in {:.1}s",
                summary.input_rows,
                summary.epoch_dirs,
                summary.unique_accounts,
                summary.global_first_seen_path,
                summary.elapsed_secs
            );
        }
        Cmd::Profile {
            car,
            mode,
            max_entries,
            max_txs,
            max_bytes,
            temp_dir,
            prefetch,
        } => {
            let summary = profile_car(ProfileConfig {
                car_path: car,
                mode,
                max_entries: (max_entries > 0).then_some(max_entries),
                max_txs: (max_txs > 0).then_some(max_txs),
                max_bytes: (max_bytes > 0).then_some(max_bytes),
                temp_dir,
                prefetch,
            })?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
    }

    Ok(())
}
