use anyhow::Result;
use blockzilla_watcher_gateway::{public_proxy, runtime_operations};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "blockzilla-watcher-gateway")]
#[command(about = "Bounded, public-safe Blockzilla watcher gateway")]
struct Cli {
    #[command(flatten)]
    serve: public_proxy::ServeArgs,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Compatibility form: `serve` is still supported.
    ///
    /// This is equivalent to running `blockzilla-watcher-gateway` directly with
    /// the same serve arguments.
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
        Some(Command::Serve(args)) => runtime.block_on(public_proxy::serve(args)),
        Some(Command::PublishRuntimeOperations(args)) => {
            runtime.block_on(runtime_operations::publish(args))
        }
        None => runtime.block_on(public_proxy::serve(cli.serve)),
    }
}
