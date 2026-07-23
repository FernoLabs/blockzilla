mod config;
mod identity;
mod leader_schedule;
mod loss_telemetry;
mod metrics;
mod receiver;
pub mod repair_runtime;
mod repair_service;
pub mod repair_tracker;
mod repair_trust_store;
pub mod repair_wal;
pub mod repair_wire;

use anyhow::Result;
use config::Config;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("shred_reader=info,solana_gossip=warn,solana_streamer=warn")
        }))
        .init();

    let config = Config::from_env()?;
    info!(
        environment = %config.environment,
        build_revision = option_env!("SHRED_READER_BUILD_REVISION").unwrap_or("unknown"),
        forward_policy = "all_valid_turbine_datagrams",
        receive_backend = "tokio_udp",
        af_xdp = "disabled_pending_measured_kernel_loss",
        repair_enabled = config.repair_enabled,
        "starting shred-reader"
    );

    receiver::run(config).await
}
