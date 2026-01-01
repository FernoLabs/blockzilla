use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::Level;

mod commands;

use commands::{
    analyze::{analyze_epoch_file, print_epoch_report},
    dump_log_strings::dump_log_strings,
};

#[derive(Parser)]
#[command(name = "blockzilla")]
#[command(about = "Blockzilla compact analyzer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Print progress every N blocks (0 disables)
    #[arg(long, default_value_t = 10_000)]
    progress_every: u64,
}

#[derive(Subcommand)]
enum Commands {
    /// Analyze compact blocks and compute consumed bytes per field by stream parsing
    Analyze {
        /// Input file containing a stream of (varint_u32_len + postcard(CompactBlockRecord))
        #[arg(short, long)]
        input: PathBuf,
        /// If set, stop after N blocks
        #[arg(long)]
        limit_blocks: Option<u64>,
    },

    /// Dump all interned log strings found in CompactMetaV1.logs
    ///
    /// Important: With V1 layout (events serialized before strings) we must deserialize the whole
    /// CompactLogStream to reach strings (postcard cannot skip via deserialize_any).
    DumpLogStrings {
        /// Input file containing a stream of (varint_u32_len + postcard(CompactBlockRecord))
        #[arg(short, long)]
        input: PathBuf,

        /// Output path (defaults to stdout). Tip: /dev/null to benchmark parse-only.
        #[arg(long)]
        out: Option<PathBuf>,

        /// If set, stop after N blocks
        #[arg(long)]
        limit_blocks: Option<u64>,

        /// If > 0, stop after writing N lines
        #[arg(long, default_value_t = 0)]
        max_lines: u64,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Analyze {
            input,
            limit_blocks,
        } => analyze_epoch_file(&input, cli.progress_every, limit_blocks)
            .map(|r| print_epoch_report(&r)),
        Commands::DumpLogStrings {
            input,
            out,
            limit_blocks,
            max_lines,
        } => dump_log_strings(
            &input,
            out.as_deref(),
            limit_blocks,
            cli.progress_every,
            max_lines,
        ),
    }
}
