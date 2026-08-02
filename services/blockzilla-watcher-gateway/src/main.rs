use anyhow::Result;
use blockzilla_watcher_gateway::{
    backfill_status, public_proxy, runtime_operations, scheduler_incidents,
};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "blockzilla-watcher-gateway")]
#[command(about = "Bounded, public-safe Blockzilla watcher gateway")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the public watcher without exposing private scheduler telemetry.
    Serve(public_proxy::ServeArgs),
    /// Publish bounded, secret-free telemetry for same-user NAS processes.
    PublishRuntimeOperations(runtime_operations::PublishArgs),
    /// Publish a bounded, redacted block-time-gap backfill status document.
    PublishBlockTimeGapBackfill(backfill_status::PublishArgs),
    /// Record bounded private scheduler incidents without controlling workers.
    RecordSchedulerIncidents(scheduler_incidents::RecordArgs),
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    match cli.command {
        Command::Serve(args) => runtime.block_on(public_proxy::serve(args)),
        Command::PublishRuntimeOperations(args) => {
            runtime.block_on(runtime_operations::publish(args))
        }
        Command::PublishBlockTimeGapBackfill(args) => {
            runtime.block_on(backfill_status::publish(args))
        }
        Command::RecordSchedulerIncidents(args) => {
            runtime.block_on(scheduler_incidents::record(args))
        }
    }
}
