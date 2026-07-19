use anyhow::{Context, Result, bail};
use blockzilla_archive_gateway::{
    GatewayConfig, GenerateManifestOptions, build_router, generate_manifest, load_catalog,
};
use clap::{Args, Parser, Subcommand};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(about, version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve pre-generated, immutable Archive V2 generations.
    Serve(ServeArgs),
    /// Hash an offline generation and publish its manifest atomically.
    GenerateManifest(GenerateManifestArgs),
}

#[derive(Debug, Args)]
struct ServeArgs {
    /// Address on which the HTTP server listens.
    #[arg(
        long,
        env = "BLOCKZILLA_ARCHIVE_GATEWAY_LISTEN",
        default_value = "127.0.0.1:8787"
    )]
    listen: SocketAddr,

    /// Immutable generation directory. Repeat once per allowlisted epoch.
    #[arg(
        long = "archive-dir",
        env = "BLOCKZILLA_ARCHIVE_GATEWAY_ARCHIVE_DIRS",
        value_delimiter = ','
    )]
    archive_dirs: Vec<PathBuf>,

    /// Shared bearer token. When set, every /v1 route requires it.
    #[arg(long, env = "BLOCKZILLA_ARCHIVE_GATEWAY_TOKEN", hide_env_values = true)]
    bearer_token: Option<String>,

    /// Refuse to start unless a bearer token is configured.
    #[arg(
        long,
        env = "BLOCKZILLA_ARCHIVE_GATEWAY_REQUIRE_AUTH",
        default_value_t = false
    )]
    require_auth: bool,

    /// Largest accepted single byte range.
    #[arg(long, env = "BLOCKZILLA_ARCHIVE_GATEWAY_MAX_RANGE_BYTES", default_value_t = 64 * 1024 * 1024)]
    max_range_bytes: u64,

    /// Maximum number of file response streams active at once.
    #[arg(
        long,
        env = "BLOCKZILLA_ARCHIVE_GATEWAY_MAX_DOWNLOADS",
        default_value_t = 4
    )]
    max_concurrent_downloads: usize,

    /// Maximum request-body size. GET/HEAD clients normally send no body.
    #[arg(
        long,
        env = "BLOCKZILLA_ARCHIVE_GATEWAY_MAX_REQUEST_BODY_BYTES",
        default_value_t = 4096
    )]
    max_request_body_bytes: usize,
}

#[derive(Debug, Args)]
struct GenerateManifestArgs {
    /// Complete, offline Archive V2 generation directory.
    #[arg(long)]
    archive_dir: PathBuf,

    #[arg(long)]
    cluster_id: String,

    #[arg(long)]
    epoch: u64,

    /// Immutable generation identifier chosen by the operator.
    #[arg(long)]
    generation_id: String,

    #[arg(long, default_value_t = 432_000)]
    slots_per_epoch: u64,

    /// Additional safe basename to hash and allowlist. Core files are automatic.
    #[arg(long = "file")]
    additional_files: Vec<String>,

    /// Output path. Defaults to archive-v2-generation.json inside archive-dir.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    match Cli::parse().command {
        Command::Serve(args) => serve(args).await,
        Command::GenerateManifest(args) => {
            let output = generate_manifest(GenerateManifestOptions {
                archive_dir: args.archive_dir,
                cluster_id: args.cluster_id,
                epoch: args.epoch,
                generation_id: args.generation_id,
                slots_per_epoch: args.slots_per_epoch,
                additional_files: args.additional_files,
                output: args.output,
            })?;
            println!("{}", output.display());
            Ok(())
        }
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    if args.archive_dirs.is_empty() {
        bail!("at least one --archive-dir is required");
    }
    if args.require_auth && args.bearer_token.is_none() {
        bail!("--require-auth was set but no bearer token is configured");
    }
    if args.max_range_bytes == 0 {
        bail!("--max-range-bytes must be greater than zero");
    }
    if args.max_concurrent_downloads == 0 {
        bail!("--max-concurrent-downloads must be greater than zero");
    }

    let catalog = load_catalog(&args.archive_dirs).context("load archive catalog")?;
    let archive_count = catalog.len();
    let config = GatewayConfig {
        catalog,
        bearer_token: args.bearer_token,
        max_range_bytes: args.max_range_bytes,
        max_concurrent_downloads: args.max_concurrent_downloads,
        max_request_body_bytes: args.max_request_body_bytes,
    };
    let app = build_router(Arc::new(config))?;
    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    info!(listen = %args.listen, archive_count, "archive gateway ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve HTTP")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
