use anyhow::Result;
use blockzilla_watcher_gateway::{public_proxy, runtime_operations};
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
    }
}
