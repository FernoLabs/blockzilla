use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use blockzilla_hivezilla::epoch_slot_audit::{
    EpochSlotAuditConfig, LocalEpochSource, run_epoch_slot_audit,
};
use clap::{ArgGroup, Parser};

#[derive(Debug, Parser)]
#[command(name = "hivezilla-epoch-slot-audit")]
#[command(about = "Compare one finalized Old Faithful epoch with one getBlocks response")]
#[command(version)]
#[command(group(
    ArgGroup::new("local_source")
        .args(["archive_dir", "repair_bundle"])
        .multiple(false)
))]
struct Args {
    #[arg(long)]
    epoch: u64,

    /// Environment variable containing the complete RPC URL. Its value is never persisted.
    #[arg(long, default_value = "HIVEZILLA_EPOCH_AUDIT_RPC_URL")]
    rpc_url_env: String,

    /// Optional environment variable containing an x-token header value.
    #[arg(long)]
    rpc_x_token_env: Option<String>,

    #[arg(long)]
    provider_label: String,

    #[arg(long)]
    cluster_label: String,

    /// Record that this configured endpoint guarantees complete archival history.
    #[arg(long)]
    provider_archival_guarantee: bool,

    /// Small JSON receipt proving finalized_through_slot covers this epoch's end.
    #[arg(long)]
    eligibility_receipt: PathBuf,

    #[arg(long, default_value = "epoch-slot-audits")]
    state_dir: PathBuf,

    /// Canonical Archive V2 epoch directory containing archive-v2-blocks.index/.zstd.
    #[arg(long)]
    archive_dir: Option<PathBuf>,

    /// Atomically published REPAIR-REQUIRED epoch union.
    #[arg(long)]
    repair_bundle: Option<PathBuf>,

    /// Replace the cached finalized RPC bitmap with one new getBlocks call.
    #[arg(long)]
    refresh_rpc_snapshot: bool,

    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,

    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    max_rpc_response_bytes: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let rpc_url = env::var(&args.rpc_url_env).with_context(|| {
        format!(
            "required RPC URL environment variable {} is unset",
            args.rpc_url_env
        )
    })?;
    let rpc_x_token = args
        .rpc_x_token_env
        .as_deref()
        .map(env::var)
        .transpose()
        .context("configured RPC x-token environment variable is unset")?;
    let local_source = match (args.archive_dir, args.repair_bundle) {
        (Some(path), None) => Some(LocalEpochSource::ArchiveDir(path)),
        (None, Some(path)) => Some(LocalEpochSource::RepairBundle(path)),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("clap local-source group is exclusive"),
    };
    let report = run_epoch_slot_audit(EpochSlotAuditConfig {
        epoch: args.epoch,
        rpc_url,
        rpc_x_token,
        provider_label: args.provider_label,
        cluster_label: args.cluster_label,
        provider_archival_guarantee: args.provider_archival_guarantee,
        eligibility_receipt: args.eligibility_receipt,
        state_dir: args.state_dir,
        local_source,
        refresh_rpc_snapshot: args.refresh_rpc_snapshot,
        timeout: Duration::from_secs(args.timeout_secs),
        max_rpc_response_bytes: args.max_rpc_response_bytes,
    })
    .await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
