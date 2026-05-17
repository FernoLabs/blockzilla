use anyhow::Result;
use clap::{Parser, Subcommand};
use of_token_index::{
    PiggyAnalyzeConfig, PiggyDumpConfig, PiggyScanConfig, analyze_piggy_flows, build_index,
    build_indexes_in_dir, dump_piggy, scan_piggy,
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "of-token-index")]
#[command(about = "Build a token and token-2022 instruction index from CAR files")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Build a token instruction index from a .car or .car.zst file.
    Build {
        #[arg(long)]
        car: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
    },

    /// Build one token instruction index per epoch from a folder containing epoch CAR files.
    BuildAll {
        #[arg(long)]
        car_dir: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
        /// Skip epochs that already have a complete index in out-dir.
        #[arg(long)]
        resume: bool,
    },

    /// Scan CAR transactions for targeted Piggy/pb token activity and emit JSONL events.
    PiggyScan {
        #[arg(long)]
        car: PathBuf,
        #[arg(long)]
        out: PathBuf,
        /// pb token mint to include. Defaults to known pbUSDC and pbSPYx mints.
        #[arg(long = "mint")]
        mints: Vec<String>,
        /// Piggy program id to include. Defaults to known Piggy v1/v2 program ids.
        #[arg(long = "program")]
        programs: Vec<String>,
        #[arg(long)]
        start_slot: Option<u64>,
        #[arg(long)]
        end_slot: Option<u64>,
    },

    /// Dump Piggy/pb-token/discovered-user transactions to binary with lookup indexes.
    PiggyDump {
        #[arg(long)]
        car: PathBuf,
        #[arg(long)]
        out_dir: PathBuf,
        /// pb token mint to include. Defaults to verified pbUSDC, pbSPYx, and pbJitoSOL mints.
        #[arg(long = "mint")]
        mints: Vec<String>,
        /// Piggy program id to include. Defaults to verified Piggy v1/v2 program ids.
        #[arg(long = "program")]
        programs: Vec<String>,
        /// File with one tracked user pubkey per line to seed discovery.
        #[arg(long = "seed-user-file")]
        seed_user_files: Vec<PathBuf>,
        /// File with one tracked token account pubkey per line to seed discovery.
        #[arg(long = "seed-token-account-file")]
        seed_token_account_files: Vec<PathBuf>,
        /// First slot to scan. Defaults to the first known Piggy transaction slot.
        #[arg(long, default_value_t = 375_549_954)]
        start_slot: u64,
        #[arg(long)]
        end_slot: Option<u64>,
    },

    /// Analyze Piggy withdraw/user flows from piggy-scan events and piggy-dump indexes.
    PiggyAnalyze {
        /// Root directory containing epoch-N piggy-dump output directories.
        #[arg(long)]
        dump_dir: PathBuf,
        /// One or more piggy-scan JSONL event files.
        #[arg(long = "events", required = true, num_args = 1..)]
        event_paths: Vec<PathBuf>,
        /// Output directory for TSV summaries and summary.md.
        #[arg(long)]
        out_dir: PathBuf,
        /// Follow window such as 1h, 24h, 7d, or 30d. May be repeated.
        #[arg(long = "window")]
        windows: Vec<String>,
        /// Optional multi-epoch dump summary.tsv; restrict analysis to completed epochs in it.
        #[arg(long = "completed-summary")]
        completed_summary: Option<PathBuf>,
        /// Optional TSV: program<TAB>protocol_guess<TAB>classification.
        #[arg(long = "program-labels")]
        program_label_file: Option<PathBuf>,
        /// Number of rows to write for top-users/top-mints/top-programs outputs.
        #[arg(long, default_value_t = 50)]
        max_top: usize,
        /// Max post-withdraw candidate transactions kept per burn. Use 0 for unlimited.
        #[arg(long, default_value_t = of_token_index::DEFAULT_MAX_FLOWS_PER_BURN)]
        max_flows_per_burn: usize,
        /// Also aggregate top users across every user.index row. Expensive on large dumps.
        #[arg(long)]
        top_users: bool,
        /// Also aggregate holder mint/burn net from all scan events. Expensive on large scans.
        #[arg(long)]
        holder_stats: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Build { car, out_dir } => {
            let meta = build_index(&car, &out_dir)?;
            println!(
                "matched {} transactions and {} instructions from {} (skipped {})",
                meta.matched_txs,
                meta.matched_instructions,
                car.display(),
                meta.skipped_txs
            );
            println!("token instructions: {}", meta.token_instructions);
            println!("token-2022 instructions: {}", meta.token_2022_instructions);
            println!("index written to {}", out_dir.display());
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
                        "epoch {}: reused existing index for {} -> {}",
                        result.epoch,
                        result.car_path.display(),
                        result.index_dir.display()
                    );
                } else {
                    built += 1;
                    println!(
                        "epoch {}: indexed {} txs / {} instructions from {} -> {}",
                        result.epoch,
                        result.matched_txs,
                        result.matched_instructions,
                        result.car_path.display(),
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
        }
        Cmd::PiggyScan {
            car,
            out,
            mints,
            programs,
            start_slot,
            end_slot,
        } => {
            let summary = scan_piggy(PiggyScanConfig {
                car_path: car.clone(),
                out_path: out.clone(),
                mints,
                programs,
                start_slot,
                end_slot,
            })?;
            println!(
                "scanned {} txs from {} and wrote {} events to {} (skipped {}, decode failures {})",
                summary.scanned_txs,
                car.display(),
                summary.written_events,
                out.display(),
                summary.skipped_txs,
                summary.decode_failures
            );
            println!(
                "token instruction events: {}",
                summary.token_instruction_events
            );
            println!(
                "token balance change events: {}",
                summary.token_balance_events
            );
            println!(
                "piggy instruction events: {}",
                summary.piggy_instruction_events
            );
            println!("candidate transactions: {}", summary.candidate_transactions);
            println!("transaction marker events: {}", summary.transaction_events);
            println!("discovered users: {}", summary.discovered_users);
            println!(
                "discovered token accounts: {}",
                summary.discovered_token_accounts
            );
        }
        Cmd::PiggyDump {
            car,
            out_dir,
            mints,
            programs,
            seed_user_files,
            seed_token_account_files,
            start_slot,
            end_slot,
        } => {
            let summary = dump_piggy(PiggyDumpConfig {
                car_path: car.clone(),
                out_dir: out_dir.clone(),
                mints,
                programs,
                seed_user_files,
                seed_token_account_files,
                start_slot: Some(start_slot),
                end_slot,
            })?;
            println!(
                "scanned {} txs from {} and dumped {} candidate txs to {} (skipped {}, decode failures {})",
                summary.scanned_txs,
                car.display(),
                summary.dumped_transactions,
                out_dir.display(),
                summary.skipped_txs,
                summary.decode_failures
            );
            println!("bytes written: {}", summary.dump_bytes);
            println!("signature index rows: {}", summary.signature_index_rows);
            println!("user index rows: {}", summary.user_index_rows);
            println!("account index rows: {}", summary.account_index_rows);
            println!("mint index rows: {}", summary.mint_index_rows);
            println!("program index rows: {}", summary.program_index_rows);
            println!("discovered users: {}", summary.discovered_users);
            println!(
                "discovered token accounts: {}",
                summary.discovered_token_accounts
            );
        }
        Cmd::PiggyAnalyze {
            dump_dir,
            event_paths,
            out_dir,
            windows,
            completed_summary,
            program_label_file,
            max_top,
            max_flows_per_burn,
            top_users,
            holder_stats,
        } => {
            let summary = analyze_piggy_flows(PiggyAnalyzeConfig {
                dump_dir,
                event_paths,
                out_dir: out_dir.clone(),
                windows,
                completed_summary,
                program_label_file,
                max_top,
                max_flows_per_burn,
                collect_top_users: top_users,
                collect_holder_stats: holder_stats,
            })?;
            println!(
                "analyzed {} withdraw burns from {} users across {} epoch dump dirs",
                summary.withdraw_burns, summary.withdraw_users, summary.epoch_dirs
            );
            println!("dumped tx index rows: {}", summary.dumped_txs);
            println!("users in dump index: {}", summary.users_in_dump_index);
            println!("analysis written to {}", summary.out_dir.display());
        }
    }

    Ok(())
}
