//! Shared library entrypoint for the shred-reader runtime.
//!
//! Hivezilla now reuses these components through a stable API boundary instead of maintaining
//! an isolated binary crate at runtime.

pub mod config;
pub mod identity;
pub mod leader_schedule;
pub mod loss_telemetry;
pub mod metrics;
pub mod receiver;
pub mod repair_runtime;
pub mod repair_service;
pub mod repair_socket;
pub mod repair_tracker;
pub mod repair_trust_store;
pub mod repair_wal;
pub mod repair_wal_worker;
pub mod repair_wire;

use anyhow::Result;
use config::Config;

/// Run the full shred-reader stack from environment-configured settings.
pub async fn run() -> Result<()> {
    let config = Config::from_env()?;
    receiver::run(config).await
}

/// Convenience helper when callers need to validate environment configuration first.
pub fn config_from_env() -> Result<Config> {
    Config::from_env()
}
