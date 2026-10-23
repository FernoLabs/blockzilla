use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use std::{
    io::Read,
    path::{Path, PathBuf},
};
#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Export one captured native Footer as a separate immutable consensus fact.
    ExportConsensus {
        binding: PathBuf,
        event: PathBuf,
        execution_payload: PathBuf,
        output: PathBuf,
    },
    /// Export captured footers from one bank's retained source-events JSON or Zstd JSON.
    ExportConsensusSourceEvents {
        binding: PathBuf,
        source_events: PathBuf,
        execution_payload: PathBuf,
        output: PathBuf,
    },
    /// Validate an export receipt and all referenced immutable consensus facts.
    InspectConsensusExport { directory: PathBuf },
    /// Read a consensus fact with an independently supplied source/bank/payload binding.
    InspectConsensusFact { fact: PathBuf, binding: PathBuf },
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
        Command::ExportConsensus {
            binding,
            event,
            execution_payload,
            output,
        } => {
            let binding = serde_json::from_slice(&read_bounded(&binding, 4096)?)?;
            let receipt = hivezilla_direct_agave::consensus::export_event(
                &binding,
                &event,
                &execution_payload,
                &output,
            )?;
            println!("{}", serde_json::to_string(&receipt)?);
            Ok(())
        }
        Command::ExportConsensusSourceEvents {
            binding,
            source_events,
            execution_payload,
            output,
        } => {
            let binding = serde_json::from_slice(&read_bounded(&binding, 4096)?)?;
            let receipt = hivezilla_direct_agave::consensus::export_source_events(
                &binding,
                &source_events,
                &execution_payload,
                &output,
            )?;
            println!("{}", serde_json::to_string(&receipt)?);
            Ok(())
        }
        Command::InspectConsensusExport { directory } => {
            let report = hivezilla_direct_agave::consensus::inspect_export(&directory)?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
        Command::InspectConsensusFact { fact, binding } => {
            use blockzilla_bank_stream_v2::consensus as c;
            let binding =
                serde_json::from_slice(&read_bounded(&binding, c::MAX_BINDING_BYTES as u64)?)?;
            let bytes = read_bounded(&fact, c::MAX_FACT_BYTES as u64)?;
            let value = c::decode_bound(&bytes, &binding)?;
            println!(
                "{}",
                serde_json::json!({
                    "profile": c::PROFILE, "binding": value.binding,
                    "verification": c::VERIFICATION, "carrier_consensus_block_id": null,
                    "footer": value.footer()?,
                })
            );
            Ok(())
        }
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

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "input_file_too_large");
    Ok(bytes)
}
