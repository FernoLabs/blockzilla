use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Build v2 controls and frames from already committed compact banks and native observations.
    BuildConfirmed {
        processed: PathBuf,
        output: PathBuf,
        #[arg(long)]
        follow: bool,
    },
    /// Compact complete processed banks once; journal controls and archive selections separately.
    BuildProcessed {
        session: PathBuf,
        output: PathBuf,
        #[arg(long)]
        blockzilla: PathBuf,
        /// Explicit source prefix boundary. Needed when attaching after source retirement.
        #[arg(long)]
        start_sequence: Option<u64>,
        #[arg(long)]
        follow: bool,
    },
    /// Build compact blocks from durable local Agave events. No gRPC connection is used.
    Build {
        session: PathBuf,
        output: PathBuf,
        #[arg(long)]
        blockzilla: PathBuf,
        #[arg(long, default_value_t = 32)]
        batch_blocks: usize,
        #[arg(long)]
        follow: bool,
    },
    /// Check capture state without relying on service process state.
    Status { session: PathBuf },
}
fn main() -> Result<()> {
    match Args::parse().command {
        Command::BuildConfirmed {
            processed,
            output,
            follow,
        } => hivezilla_direct_agave::confirmed::run(&processed, &output, follow),
        Command::BuildProcessed {
            session,
            output,
            blockzilla,
            start_sequence,
            follow,
        } => hivezilla_direct_agave::processed::run(
            &session,
            &output,
            &blockzilla,
            start_sequence,
            follow,
        ),
        Command::Build {
            session,
            output,
            blockzilla,
            batch_blocks,
            follow,
        } => hivezilla_direct_agave::worker::run(
            &session,
            &output,
            &blockzilla,
            batch_blocks,
            follow,
        ),
        Command::Status { session } => {
            let failure: serde_json::Value =
                serde_json::from_slice(&std::fs::read(session.join("FAILURE.json"))?)?;
            anyhow::ensure!(failure.get("state").is_none(), "capture failed: {failure}");
            let db = hivezilla_direct_agave::journal::open_reader(&session.join("events.sqlite"))?;
            let (state, seq): (String, u64) =
                db.query_row("SELECT state,durable_sequence FROM status", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?;
            anyhow::ensure!(
                state != "CAPTURING"
                    || hivezilla_direct_agave::journal::exclusive_lock(&session.join("LIVE.lock"))
                        .is_err(),
                "capture ended without a clean close; possible source gap"
            );
            println!(
                "{}",
                serde_json::json!({"state":state,"durable_sequence":seq,"clean_close":session.join("CLOSED.json").exists(),"canonical":false})
            );
            anyhow::ensure!(state != "FAILED", "capture failed");
            Ok(())
        }
    }
}
