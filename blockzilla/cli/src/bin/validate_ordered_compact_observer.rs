//! Require the scheduler observer to classify a registry-only cohort safely.
//!
//! A legacy compact/reuse cohort must show every epoch in the range as
//! `queued`. Only the first epoch may report the ready message; every later
//! epoch must still be waiting on its predecessor. No epoch may carry a
//! registry-order label before compaction has run.

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use serde::Serialize;
use serde_json::Value;
use std::{path::PathBuf, time::Duration};

const READY: &str = "legacy registry sidecars are ready for one-pass compact/reuse; \
                     separate CAR preflight is bypassed";
const WAITING: &str = "legacy compact/reuse is waiting for a usable previous blockhash tail or \
                       predecessor reader sidecars";

/// Bounded wait for the observer endpoint, matching the shell loop it replaces.
const POLL_ATTEMPTS: u32 = 30;
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const POLL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Parser)]
struct Args {
    /// Status document to validate. With `--poll-url` this is written first.
    status: PathBuf,
    first_epoch: u64,
    last_epoch: u64,
    /// Poll this observer endpoint until it answers, then publish `status`.
    #[arg(long)]
    poll_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct Report {
    classification: &'static str,
    first_epoch: u64,
    last_epoch: u64,
    ready: u64,
    waiting_for_predecessor: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.first_epoch <= args.last_epoch,
        "first_epoch must not exceed last_epoch"
    );

    if let Some(url) = &args.poll_url {
        fetch_observer_status(url, &args.status)?;
    }

    let raw = std::fs::read_to_string(&args.status)
        .with_context(|| format!("read {}", args.status.display()))?;
    let status: Value =
        serde_json::from_str(&raw).with_context(|| format!("parse {}", args.status.display()))?;

    let epochs = status
        .get("epochs")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();

    for epoch in args.first_epoch..=args.last_epoch {
        let row = epochs
            .iter()
            .find(|row| row.get("epoch").and_then(Value::as_u64) == Some(epoch))
            .with_context(|| format!("observer omitted epoch {epoch}"))?;

        let state = row.get("state").and_then(Value::as_str);
        ensure!(
            state == Some("queued"),
            "epoch {epoch} observer state is {state:?}, not 'queued'"
        );

        let expected = if epoch == args.first_epoch {
            READY
        } else {
            WAITING
        };
        let message = row.get("message").and_then(Value::as_str);
        ensure!(
            message == Some(expected),
            "epoch {epoch} did not enter the required compact/reuse path: {message:?}"
        );

        // Absent and "unknown" both mean the label has not been assigned yet.
        let registry_order = row.get("registry_order").and_then(Value::as_str);
        ensure!(
            matches!(registry_order, None | Some("unknown")),
            "epoch {epoch} has unexpected pre-compaction registry order label: {registry_order:?}"
        );
    }

    let report = Report {
        classification: "legacy_compact_reuse",
        first_epoch: args.first_epoch,
        last_epoch: args.last_epoch,
        ready: 1,
        waiting_for_predecessor: args.last_epoch - args.first_epoch,
    };
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

/// Wait for the observer to answer, then publish its body atomically.
///
/// The endpoint is loopback-only and returns the same document the scheduler
/// serves, so the body is written through a sibling temporary and renamed. A
/// partially written status file could otherwise be validated by a later run.
fn fetch_observer_status(url: &str, destination: &PathBuf) -> Result<()> {
    let client = reqwest::blocking::Client::builder()
        .timeout(POLL_TIMEOUT)
        .build()
        .context("build observer HTTP client")?;

    for attempt in 1..=POLL_ATTEMPTS {
        match client.get(url).send().and_then(|response| {
            let response = response.error_for_status()?;
            response.bytes()
        }) {
            Ok(body) => {
                let temporary = destination.with_extension("building");
                std::fs::write(&temporary, &body)
                    .with_context(|| format!("write {}", temporary.display()))?;
                std::fs::rename(&temporary, destination)
                    .with_context(|| format!("publish {}", destination.display()))?;
                return Ok(());
            }
            Err(_) if attempt < POLL_ATTEMPTS => std::thread::sleep(POLL_INTERVAL),
            Err(error) => {
                bail!("scheduler observer did not become ready: {error}");
            }
        }
    }
    bail!("scheduler observer did not become ready")
}
