//! Background task that ingests the real Blockzilla pipeline snapshot from
//! the scheduler's private read-only status listener and feeds it into
//! `state::set_snapshot`.
//!
//! The scheduler listener stays on loopback. The monitor validates the wire
//! schema, maps it into an explicitly curated view model, and applies public
//! redaction before anything is served to a browser.
//!
//! This keeps one `PipelineSnapshot` in memory per connection cycle and
//! applies incremental `snapshot_patch` events to it in place (see
//! `snapshot::PipelineSnapshot::apply_patch`), the same reconcile-by-key
//! algorithm `apps/blockzilla-watcher/src/lib/snapshot-patch.ts` uses. A
//! full `GET /api/v1/status` only happens on connect, on an explicit
//! `resync` event, or when the patch stream's sequence numbers show a gap
//! (`snapshot::sequence_action`) -- not on every patch, which is what this
//! client did before. On any error (upstream down, malformed payload,
//! stream closed) the whole connect-resync-stream cycle restarts after a
//! fixed backoff.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, ensure};
use futures_core::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::time::Instant;

use crate::snapshot::{self, PipelineSnapshot, PipelineSnapshotPatch, SequenceAction};
use crate::state;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_DELAY: Duration = Duration::from_secs(3);
/// The scheduler reconciles every 5s by default, publishes active progress
/// as often as every 1s, and Axum emits SSE keep-alives every 15s. Three
/// missed heartbeat windows is generous to healthy quiet connections while
/// still bounding an upstream socket that remains open without delivering
/// bytes or valid application state.
const SSE_IDLE_TIMEOUT: Duration = Duration::from_secs(45);
const APPLICATION_FRESHNESS_TIMEOUT: Duration = Duration::from_secs(45);
/// A full scheduler `snapshot` event's `data:` line can legitimately run several MiB
/// (the live snapshot is ~4.3 MiB today), but an upstream sending an
/// unbounded line with no `\n` should not be able to grow this buffer
/// without limit -- bail and let the reconnect loop in `start` retry
/// instead.
const MAX_SSE_LINE_BYTES: usize = 8 * 1024 * 1024;
const MAX_STATUS_BYTES: usize = MAX_SSE_LINE_BYTES;
const MAX_GAP_INDEX_BYTES: usize = 8 * 1024 * 1024;
const MAX_FIREWATCH_STATUS_BYTES: usize = 4 * 1024 * 1024;
const MAX_FIREWATCH_ROWS: usize = 4_096;
const MAX_FIREWATCH_TEXT_BYTES: usize = 16 * 1024;
const MAX_FIREWATCH_STATUS_SKEW_SECS: u64 = 30;

pub fn start(upstream: String, firewatch_status_file: Option<PathBuf>) {
    tokio::spawn(async move {
        loop {
            if let Err(err) = run(&upstream, firewatch_status_file.clone()).await {
                state::set_offline(err.to_string()).await;
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    });
}

/// Schema-1 status published atomically by the local Firewatch controller.
/// This is an overlay, not a replacement for the scheduler snapshot: only
/// the Firewatch summary fields and `firewatch_index` lanes are changed.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct FirewatchControllerStatus {
    schema_version: u64,
    updated_unix_secs: u64,
    capacity_configured: u32,
    running: u32,
    epochs_total: u32,
    epochs_accepted: u32,
    epochs_queued: u32,
    /// Complete archive epochs in the Firewatch project scope. Optional so
    /// a schema-1 controller from before all-archive coverage remains valid.
    #[serde(default)]
    archive_epochs_total: Option<u32>,
    /// Archive epochs whose exact input generation is ready to index now.
    #[serde(default)]
    epochs_eligible: Option<u32>,
    /// Archive epochs waiting for registry migration before they can enter
    /// the Firewatch queue.
    #[serde(default)]
    epochs_blocked_migration: Option<u32>,
    /// Archive epochs whose exact generation still needs a wire-profile
    /// attestation before it can enter the runnable queue.
    #[serde(default)]
    epochs_blocked_wire_profile: Option<u32>,
    /// ETA for active and queued runnable Firewatch work. Failed and
    /// profile-audit rows are excluded. Per-row ETA is the active phase ETA.
    #[serde(default)]
    queue_eta_secs: Option<f64>,
    admission_blocked_reason: Option<String>,
    rows: Vec<FirewatchControllerRow>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct FirewatchControllerRow {
    epoch: u32,
    state: String,
    phase: String,
    auto_paused: bool,
    auto_pause_reason: Option<String>,
    progress_pct: f32,
    blocks_done: u64,
    eta_secs: Option<f64>,
    rss_bytes: Option<u64>,
    read_mib_per_sec: Option<f64>,
    write_mib_per_sec: Option<f64>,
    wallet_count: Option<u64>,
    relation_count: Option<u64>,
    parity_status: Option<String>,
}

impl FirewatchControllerStatus {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported Firewatch controller schema"
        );
        ensure!(
            self.updated_unix_secs > 0,
            "Firewatch controller timestamp is missing"
        );
        ensure!(
            self.rows.len() <= MAX_FIREWATCH_ROWS,
            "Firewatch controller exceeds {MAX_FIREWATCH_ROWS} rows"
        );
        ensure!(
            self.running <= self.capacity_configured,
            "Firewatch running workers exceed configured capacity"
        );
        ensure!(
            self.epochs_accepted <= self.epochs_total,
            "Firewatch accepted epochs exceed total epochs"
        );
        ensure!(
            self.epochs_queued <= self.epochs_total,
            "Firewatch queued epochs exceed total epochs"
        );
        ensure!(
            self.rows.len() == self.epochs_total as usize,
            "Firewatch row count does not equal total epochs"
        );
        validate_firewatch_nonnegative("Firewatch queue ETA", self.queue_eta_secs)?;
        if let Some(eligible) = self.epochs_eligible {
            ensure!(
                self.epochs_accepted <= eligible,
                "Firewatch accepted epochs exceed eligible epochs"
            );
            ensure!(
                self.running <= eligible,
                "Firewatch running epochs exceed eligible epochs"
            );
            ensure!(
                self.epochs_queued <= eligible,
                "Firewatch queued epochs exceed eligible epochs"
            );
        }
        if self.epochs_blocked_wire_profile.is_some() {
            ensure!(
                self.archive_epochs_total.is_some()
                    && self.epochs_eligible.is_some()
                    && self.epochs_blocked_migration.is_some(),
                "Firewatch wire-profile coverage is incomplete"
            );
        }
        if let Some(archive_total) = self.archive_epochs_total {
            if let Some(eligible) = self.epochs_eligible {
                ensure!(
                    eligible <= archive_total,
                    "Firewatch eligible epochs exceed archive scope"
                );
            }
            if let Some(blocked) = self.epochs_blocked_migration {
                ensure!(
                    blocked <= archive_total,
                    "Firewatch migration-blocked epochs exceed archive scope"
                );
            }
            if let Some(blocked) = self.epochs_blocked_wire_profile {
                ensure!(
                    blocked <= archive_total,
                    "Firewatch wire-profile-blocked epochs exceed archive scope"
                );
            }
            if let (Some(eligible), Some(blocked_migration), Some(blocked_wire_profile)) = (
                self.epochs_eligible,
                self.epochs_blocked_migration,
                self.epochs_blocked_wire_profile,
            ) {
                ensure!(
                    u64::from(eligible)
                        + u64::from(blocked_migration)
                        + u64::from(blocked_wire_profile)
                        == u64::from(archive_total),
                    "Firewatch coverage classes do not equal archive scope"
                );
            } else if let (Some(eligible), Some(blocked)) =
                (self.epochs_eligible, self.epochs_blocked_migration)
            {
                // A controller from before wire-profile attestations can
                // leave part of the archive scope unclassified.
                ensure!(
                    u64::from(eligible) + u64::from(blocked) <= u64::from(archive_total),
                    "Firewatch eligible and migration-blocked epochs exceed archive scope"
                );
            }
        }
        validate_firewatch_text(
            "Firewatch admission blocked reason",
            self.admission_blocked_reason.as_deref(),
        )?;

        let mut epochs = BTreeSet::new();
        for row in &self.rows {
            ensure!(
                epochs.insert(row.epoch),
                "Firewatch controller contains duplicate epochs"
            );
            ensure!(
                matches!(
                    row.state.as_str(),
                    "queued"
                        | "running"
                        | "paused"
                        | "accepted"
                        | "failed"
                        | "blocked"
                        | "profile_audit_required"
                ),
                "Firewatch controller row has an unknown state"
            );
            ensure!(
                matches!(
                    row.phase.as_str(),
                    "target_build"
                        | "source_control_build"
                        | "canonical_build"
                        | "parity"
                        | "wire_profile_audit"
                ),
                "Firewatch controller row has an unknown phase"
            );
            ensure!(
                (row.state == "profile_audit_required") == (row.phase == "wire_profile_audit"),
                "Firewatch profile-audit row state and phase differ"
            );
            ensure!(
                row.progress_pct.is_finite() && (0.0..=100.0).contains(&row.progress_pct),
                "Firewatch controller row has invalid progress"
            );
            validate_firewatch_nonnegative("Firewatch row ETA", row.eta_secs)?;
            validate_firewatch_nonnegative("Firewatch row read rate", row.read_mib_per_sec)?;
            validate_firewatch_nonnegative("Firewatch row write rate", row.write_mib_per_sec)?;
            validate_firewatch_text(
                "Firewatch row pause reason",
                row.auto_pause_reason.as_deref(),
            )?;
            if let Some(parity) = row.parity_status.as_deref() {
                ensure!(
                    matches!(parity, "pending" | "running" | "equal" | "mismatch"),
                    "Firewatch controller row has an unknown parity status"
                );
            }
        }
        let accepted = self
            .rows
            .iter()
            .filter(|row| row.state == "accepted")
            .count();
        let queued = self.rows.iter().filter(|row| row.state == "queued").count();
        let active = self
            .rows
            .iter()
            .filter(|row| matches!(row.state.as_str(), "running" | "paused"))
            .count();
        ensure!(
            accepted == self.epochs_accepted as usize,
            "Firewatch accepted count does not match rows"
        );
        ensure!(
            queued == self.epochs_queued as usize,
            "Firewatch queued count does not match rows"
        );
        ensure!(
            active == self.running as usize,
            "Firewatch active count does not match rows"
        );
        Ok(())
    }
}

fn validate_firewatch_text(label: &str, value: Option<&str>) -> anyhow::Result<()> {
    if let Some(value) = value {
        ensure!(
            value.len() <= MAX_FIREWATCH_TEXT_BYTES,
            "{label} exceeds {MAX_FIREWATCH_TEXT_BYTES} bytes"
        );
    }
    Ok(())
}

fn validate_firewatch_nonnegative(label: &str, value: Option<f64>) -> anyhow::Result<()> {
    if let Some(value) = value {
        ensure!(value.is_finite() && value >= 0.0, "{label} is invalid");
    }
    Ok(())
}

/// SSE envelope shape: `{"type": "...", "sequence": N, "data": ...}`.
#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "type")]
    event_type: String,
    sequence: i64,
    data: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PatchOutcome {
    Applied,
    Ignored,
    Resync,
}

#[derive(Debug, PartialEq, Eq)]
enum WatchdogEvent<T> {
    Item(Option<T>),
    Idle,
    ApplicationStale,
}

async fn next_with_watchdog<S>(
    body: &mut S,
    application_deadline: Instant,
    idle_timeout: Duration,
) -> WatchdogEvent<S::Item>
where
    S: Stream + Unpin,
{
    tokio::select! {
        _ = tokio::time::sleep_until(application_deadline) => {
            WatchdogEvent::ApplicationStale
        }
        item = tokio::time::timeout(idle_timeout, body.next()) => {
            match item {
                Ok(item) => WatchdogEvent::Item(item),
                Err(_) => WatchdogEvent::Idle,
            }
        }
    }
}

fn observe_application_timestamp(timestamp: u64, latest: &mut u64) -> bool {
    if timestamp <= *latest {
        return false;
    }
    *latest = timestamp;
    true
}

struct Session {
    client: reqwest::Client,
    upstream: String,
    current: PipelineSnapshot,
    last_sequence: i64,
    firewatch_status_file: Option<PathBuf>,
}

impl Session {
    async fn bootstrap(
        client: reqwest::Client,
        upstream: &str,
        firewatch_status_file: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let current = fetch_status(&client, upstream).await?;
        let last_sequence = current.sequence as i64;
        let session = Session {
            client,
            upstream: upstream.to_string(),
            current,
            last_sequence,
            firewatch_status_file,
        };
        session.publish_current().await;
        Ok(session)
    }

    async fn resync(&mut self) -> anyhow::Result<()> {
        self.current = fetch_status(&self.client, &self.upstream).await?;
        self.last_sequence = self.current.sequence as i64;
        self.publish_current().await;
        Ok(())
    }

    async fn publish_current(&self) {
        let snapshot =
            snapshot_for_publication(&self.current, self.firewatch_status_file.as_deref()).await;
        state::set_snapshot(snapshot).await;
    }

    async fn accept_snapshot(
        &mut self,
        envelope_sequence: i64,
        snapshot: PipelineSnapshot,
    ) -> anyhow::Result<bool> {
        snapshot.validate()?;
        ensure!(
            snapshot.sequence as i64 == envelope_sequence,
            "snapshot envelope sequence does not match its payload"
        );
        // A restarted upstream resets its process-local sequence counter;
        // a newer `now_unix_secs` is the evidence that's what happened
        // rather than a stale/duplicate full-snapshot event arriving late.
        if (snapshot.sequence as i64) <= self.last_sequence
            && snapshot.now_unix_secs <= self.current.now_unix_secs
        {
            return Ok(false);
        }
        self.last_sequence = snapshot.sequence as i64;
        self.current = snapshot;
        self.publish_current().await;
        Ok(true)
    }

    async fn handle_patch(
        &mut self,
        envelope_sequence: i64,
        patch: PipelineSnapshotPatch,
    ) -> anyhow::Result<PatchOutcome> {
        if patch.validate_shape().is_err() || patch.sequence as i64 != envelope_sequence {
            return Ok(PatchOutcome::Resync);
        }
        match snapshot::sequence_action(self.last_sequence, envelope_sequence) {
            SequenceAction::Ignore => Ok(PatchOutcome::Ignored),
            SequenceAction::Resync => Ok(PatchOutcome::Resync),
            SequenceAction::Apply => {
                let mut candidate = self.current.clone();
                candidate.apply_patch(patch);
                if candidate.validate().is_err() {
                    return Ok(PatchOutcome::Resync);
                }
                self.current = candidate;
                self.last_sequence = envelope_sequence;
                self.publish_current().await;
                Ok(PatchOutcome::Applied)
            }
        }
    }
}

/// Builds the exact snapshot published to dashboard state. The unmodified
/// scheduler snapshot remains the session's patch base. This prevents a local
/// overlay from leaking back into later scheduler patch reconciliation.
async fn snapshot_for_publication(
    scheduler: &PipelineSnapshot,
    firewatch_status_file: Option<&Path>,
) -> PipelineSnapshot {
    let mut published = scheduler.clone();
    let Some(path) = firewatch_status_file else {
        return published;
    };

    match read_firewatch_status_file(path)
        .await
        .and_then(|status| validate_firewatch_status_freshness(status, scheduler.now_unix_secs))
    {
        Ok(status) => apply_firewatch_overlay(&mut published, status),
        Err(error) => apply_firewatch_overlay_error(&mut published, &error),
    }
    published
}

fn validate_firewatch_status_freshness(
    status: FirewatchControllerStatus,
    scheduler_now_unix_secs: u64,
) -> anyhow::Result<FirewatchControllerStatus> {
    let skew = status.updated_unix_secs.abs_diff(scheduler_now_unix_secs);
    ensure!(
        skew <= MAX_FIREWATCH_STATUS_SKEW_SECS,
        "Firewatch controller status timestamp differs from scheduler time by {skew} seconds"
    );
    Ok(status)
}

fn clear_firewatch_overlay(snapshot: &mut PipelineSnapshot) {
    snapshot.lanes.retain(|lane| {
        lane.kind.as_str() != "firewatch_index" && !lane.id.starts_with("firewatch_index:")
    });
    snapshot.summary.firewatch_index_capacity_configured = 0;
    snapshot.summary.firewatch_index_running = 0;
    snapshot.summary.firewatch_index_epochs_total = 0;
    snapshot.summary.firewatch_index_epochs_accepted = 0;
    snapshot.summary.firewatch_index_epochs_queued = 0;
    snapshot.summary.firewatch_index_archive_epochs_total = None;
    snapshot.summary.firewatch_index_epochs_eligible = None;
    snapshot.summary.firewatch_index_epochs_blocked_migration = None;
    snapshot.summary.firewatch_index_epochs_blocked_wire_profile = None;
    snapshot.summary.firewatch_index_queue_eta_secs = None;
    snapshot.summary.firewatch_index_admission_blocked_reason = None;
}

fn apply_firewatch_overlay(snapshot: &mut PipelineSnapshot, status: FirewatchControllerStatus) {
    clear_firewatch_overlay(snapshot);
    let updated_unix_secs = status.updated_unix_secs;
    snapshot.summary.firewatch_index_capacity_configured = status.capacity_configured;
    snapshot.summary.firewatch_index_running = status.running;
    snapshot.summary.firewatch_index_epochs_total = status.epochs_total;
    snapshot.summary.firewatch_index_epochs_accepted = status.epochs_accepted;
    snapshot.summary.firewatch_index_epochs_queued = status.epochs_queued;
    snapshot.summary.firewatch_index_archive_epochs_total = status.archive_epochs_total;
    snapshot.summary.firewatch_index_epochs_eligible = status.epochs_eligible;
    snapshot.summary.firewatch_index_epochs_blocked_migration = status.epochs_blocked_migration;
    snapshot.summary.firewatch_index_epochs_blocked_wire_profile =
        status.epochs_blocked_wire_profile;
    snapshot.summary.firewatch_index_queue_eta_secs = status.queue_eta_secs;
    snapshot.summary.firewatch_index_admission_blocked_reason = status.admission_blocked_reason;

    snapshot.lanes.extend(status.rows.into_iter().map(|row| {
        let rss_bytes = row.rss_bytes;
        snapshot::LaneStatus {
            id: format!("firewatch_index:{}", row.epoch),
            kind: "firewatch_index".to_string(),
            state: row.state,
            phase: row.phase,
            epoch: Some(row.epoch),
            auto_paused: row.auto_paused,
            auto_pause_reason: row.auto_pause_reason,
            progress: snapshot::ProgressSnapshot {
                blocks_done: row.blocks_done,
                progress_pct: Some(row.progress_pct),
                eta_secs: row.eta_secs,
                disk_read_mib_per_sec: row.read_mib_per_sec,
                disk_write_mib_per_sec: row.write_mib_per_sec,
                rss_bytes,
                updated_unix_secs: Some(updated_unix_secs),
                ..Default::default()
            },
            rss_bytes,
            wallet_count: row.wallet_count,
            relation_count: row.relation_count,
            parity_status: row.parity_status,
        }
    }));
}

fn apply_firewatch_overlay_error(snapshot: &mut PipelineSnapshot, error: &anyhow::Error) {
    clear_firewatch_overlay(snapshot);
    let detail: String = error.to_string().chars().take(2_048).collect();
    snapshot.summary.firewatch_index_admission_blocked_reason =
        Some(format!("Firewatch controller status unavailable: {detail}"));
}

async fn read_firewatch_status_file(path: &Path) -> anyhow::Result<FirewatchControllerStatus> {
    let path = path.to_path_buf();
    let bytes = tokio::task::spawn_blocking(move || {
        read_bounded_regular_file_bytes(
            &path,
            MAX_FIREWATCH_STATUS_BYTES,
            "Firewatch controller status",
        )
    })
    .await
    .context("join bounded Firewatch controller status read")??;
    let status: FirewatchControllerStatus =
        serde_json::from_slice(&bytes).context("decode Firewatch controller status")?;
    status.validate()?;
    Ok(status)
}

async fn run(upstream: &str, firewatch_status_file: Option<PathBuf>) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()?;
    let mut session = Session::bootstrap(client, upstream, firewatch_status_file).await?;

    let response = tokio::time::timeout(
        CONNECT_TIMEOUT,
        session
            .client
            .get(format!("{upstream}/api/v1/events"))
            .header("accept", "text/event-stream")
            .send(),
    )
    .await
    .context("timed out opening scheduler event stream")??
    .error_for_status()?;

    let mut body = response.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut event_name = String::new();
    let mut last_application_update = Instant::now();
    let mut latest_application_timestamp = session.current.now_unix_secs;

    loop {
        let chunk = match next_with_watchdog(
            &mut body,
            last_application_update + APPLICATION_FRESHNESS_TIMEOUT,
            SSE_IDLE_TIMEOUT,
        )
        .await
        {
            WatchdogEvent::Item(Some(chunk)) => chunk?,
            WatchdogEvent::Item(None) => anyhow::bail!("event stream ended"),
            WatchdogEvent::Idle => anyhow::bail!(
                "scheduler event stream delivered no bytes for {} seconds",
                SSE_IDLE_TIMEOUT.as_secs()
            ),
            WatchdogEvent::ApplicationStale => anyhow::bail!(
                "scheduler event stream delivered no valid state for {} seconds",
                APPLICATION_FRESHNESS_TIMEOUT.as_secs()
            ),
        };
        buf.extend_from_slice(&chunk);
        anyhow::ensure!(
            buf.len() <= MAX_SSE_LINE_BYTES,
            "SSE line exceeded {MAX_SSE_LINE_BYTES} bytes with no newline"
        );

        while let Some(pos) = buf.iter().position(|&byte| byte == b'\n') {
            let raw_line: Vec<u8> = buf.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&raw_line);
            let line = line.trim_end_matches(['\r', '\n']);

            if line.is_empty() {
                event_name.clear();
                continue;
            }
            if let Some(name) = line.strip_prefix("event:") {
                event_name = name.trim().to_string();
                continue;
            }
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let data = data.trim();

            match event_name.as_str() {
                "snapshot" => {
                    let accepted = match decode_envelope::<PipelineSnapshot>(data, "snapshot") {
                        Ok((sequence, snapshot)) => {
                            match session.accept_snapshot(sequence, snapshot).await {
                                Ok(accepted) => accepted,
                                Err(_) => {
                                    session.resync().await?;
                                    true
                                }
                            }
                        }
                        Err(_) => {
                            session.resync().await?;
                            true
                        }
                    };
                    if accepted
                        && observe_application_timestamp(
                            session.current.now_unix_secs,
                            &mut latest_application_timestamp,
                        )
                    {
                        last_application_update = Instant::now();
                    }
                }
                "snapshot_patch" => {
                    let outcome =
                        match decode_envelope::<PipelineSnapshotPatch>(data, "snapshot_patch") {
                            Ok((sequence, patch)) => session.handle_patch(sequence, patch).await?,
                            Err(_) => PatchOutcome::Resync,
                        };
                    match outcome {
                        PatchOutcome::Applied => {
                            if observe_application_timestamp(
                                session.current.now_unix_secs,
                                &mut latest_application_timestamp,
                            ) {
                                last_application_update = Instant::now();
                            }
                        }
                        PatchOutcome::Ignored => {}
                        PatchOutcome::Resync => {
                            session.resync().await?;
                            if observe_application_timestamp(
                                session.current.now_unix_secs,
                                &mut latest_application_timestamp,
                            ) {
                                last_application_update = Instant::now();
                            }
                        }
                    }
                }
                "resync" => {
                    session.resync().await?;
                    if observe_application_timestamp(
                        session.current.now_unix_secs,
                        &mut latest_application_timestamp,
                    ) {
                        last_application_update = Instant::now();
                    }
                }
                _ => {}
            }
        }
    }
}

async fn fetch_status(
    client: &reqwest::Client,
    upstream: &str,
) -> anyhow::Result<PipelineSnapshot> {
    let response = client
        .get(format!("{upstream}/api/v1/status"))
        .header("accept", "application/json")
        .timeout(CONNECT_TIMEOUT)
        .send()
        .await?
        .error_for_status()?;
    let bytes = read_limited(response, MAX_STATUS_BYTES).await?;
    let snapshot: PipelineSnapshot =
        serde_json::from_slice(&bytes).context("decode scheduler status schema")?;
    snapshot.validate()?;
    Ok(snapshot)
}

fn decode_envelope<T: DeserializeOwned>(data: &str, expected: &str) -> anyhow::Result<(i64, T)> {
    let envelope: Envelope = serde_json::from_str(data).context("decode scheduler SSE envelope")?;
    ensure!(
        envelope.event_type == expected,
        "scheduler SSE event type does not match event name"
    );
    let payload = serde_json::from_value(envelope.data).context("decode scheduler SSE payload")?;
    Ok((envelope.sequence, payload))
}

async fn read_limited(response: reqwest::Response, max: usize) -> anyhow::Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|content_length| content_length > max as u64)
    {
        anyhow::bail!("upstream response exceeds {max} bytes");
    }
    let mut body = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        anyhow::ensure!(
            bytes.len().saturating_add(chunk.len()) <= max,
            "upstream response exceeds {max} bytes"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// A slow-changing offline batch artifact (`blockzilla build-block-time-gaps`
/// and `build-block-time-gap-index`, run outside the scheduler entirely --
/// see docs/reference/block-time-gap-sidecar.md), not live telemetry, so
/// this polls on its own long-period loop rather than joining the SSE
/// stream above. As of this writing the scheduler has no HTTP handler for
/// it, so the request fails regardless of whether the
/// index has been generated. `start_gap_index_file_poller` below reads the
/// generated JSON directly from disk instead, for deployments where this
/// binary runs on the same host as the archive; this HTTP path stays
/// available for when the scheduler grows a real route for it. Either way,
/// `run` records a failure as `gap_index_error` rather than treating it
/// like the main snapshot connection being down, and the calendar page
/// says so plainly instead of just rendering an empty overlay.
const GAP_INDEX_POLL_INTERVAL: Duration = Duration::from_secs(600);

pub fn start_gap_index_poller(upstream: String) {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            match fetch_gap_index(&client, &upstream).await {
                Ok(index) => state::set_gap_index(index),
                Err(err) => state::set_gap_index_error(err.to_string()),
            }
            tokio::time::sleep(GAP_INDEX_POLL_INTERVAL).await;
        }
    });
}

async fn fetch_gap_index(
    client: &reqwest::Client,
    upstream: &str,
) -> anyhow::Result<snapshot::BlockTimeGapIndex> {
    let response = client
        .get(format!(
            "{upstream}/api/v1/sidecars/block-time-gaps/index.json"
        ))
        .header("accept", "application/json")
        .timeout(CONNECT_TIMEOUT)
        .send()
        .await?
        .error_for_status()?;
    let bytes = read_limited(response, MAX_GAP_INDEX_BYTES).await?;
    let index: snapshot::BlockTimeGapIndex =
        serde_json::from_slice(&bytes).context("decode block-time-gap index")?;
    index.validate()?;
    Ok(index)
}

/// Same idea as `start_gap_index_poller`, but reads `path` from local disk
/// on each tick instead of making an HTTP request -- see `--gap-index-file`
/// in `main.rs`. Re-reads on the same interval so a periodically
/// regenerated file (a cron re-running `build-block-time-gap-index`) picks
/// up without restarting this process.
pub fn start_gap_index_file_poller(path: std::path::PathBuf) {
    tokio::spawn(async move {
        loop {
            match read_gap_index_file(&path).await {
                Ok(index) => state::set_gap_index(index),
                Err(err) => state::set_gap_index_error(err.to_string()),
            }
            tokio::time::sleep(GAP_INDEX_POLL_INTERVAL).await;
        }
    });
}

async fn read_gap_index_file(
    path: &std::path::Path,
) -> anyhow::Result<snapshot::BlockTimeGapIndex> {
    let path = path.to_path_buf();
    let bytes = tokio::task::spawn_blocking(move || read_gap_index_file_bytes(&path))
        .await
        .context("join bounded gap-index file read")??;
    let index: snapshot::BlockTimeGapIndex =
        serde_json::from_slice(&bytes).context("decode block-time-gap index")?;
    index.validate()?;
    Ok(index)
}

/// Opens and validates one object, then reads at most `MAX + 1` bytes from
/// that retained descriptor. `O_NONBLOCK` makes opening a FIFO safe and
/// `O_NOFOLLOW` rejects a final-component symlink. Rechecking the descriptor
/// and path identity after the read catches in-place mutation and an atomic
/// path replacement rather than trusting metadata gathered before `open`.
fn read_gap_index_file_bytes(path: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    read_bounded_regular_file_bytes(path, MAX_GAP_INDEX_BYTES, "gap index")
}

fn read_bounded_regular_file_bytes(
    path: &Path,
    max_bytes: usize,
    label: &str,
) -> anyhow::Result<Vec<u8>> {
    use std::io::Read;

    use rustix::fs::{FileType, Mode, OFlags};

    let fd = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .with_context(|| format!("open {label} {}", path.display()))?;
    let before = rustix::fs::fstat(&fd).with_context(|| format!("inspect opened {label}"))?;
    ensure!(
        FileType::from_raw_mode(before.st_mode) == FileType::RegularFile,
        "{label} is not a regular file"
    );
    let advertised_size = usize::try_from(before.st_size)
        .with_context(|| format!("{label} has a negative or unrepresentable size"))?;
    ensure!(
        advertised_size <= max_bytes,
        "{label} exceeds {max_bytes} bytes"
    );

    let mut file = std::fs::File::from(fd);
    let mut bytes = Vec::with_capacity(advertised_size.min(max_bytes.saturating_add(1)));
    (&mut file)
        .take(max_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read bounded {label}"))?;
    ensure!(
        bytes.len() <= max_bytes,
        "{label} exceeds {max_bytes} bytes"
    );

    let after = rustix::fs::fstat(&file).with_context(|| format!("reinspect opened {label}"))?;
    let path_after = rustix::fs::lstat(path).with_context(|| format!("reinspect {label} path"))?;
    ensure!(
        same_file_identity(&before, &after)
            && same_file_version(&before, &after)
            && same_file_identity(&after, &path_after)
            && FileType::from_raw_mode(path_after.st_mode) == FileType::RegularFile
            && after.st_size == path_after.st_size
            && usize::try_from(after.st_size).ok() == Some(bytes.len()),
        "{label} changed while it was being read"
    );
    Ok(bytes)
}

fn same_file_identity(left: &rustix::fs::Stat, right: &rustix::fs::Stat) -> bool {
    left.st_dev == right.st_dev && left.st_ino == right.st_ino
}

fn same_file_version(left: &rustix::fs::Stat, right: &rustix::fs::Stat) -> bool {
    left.st_size == right.st_size
        && left.st_mtime == right.st_mtime
        && left.st_mtime_nsec == right.st_mtime_nsec
        && left.st_ctime == right.st_ctime
        && left.st_ctime_nsec == right.st_ctime_nsec
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRoot(std::path::PathBuf);

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn test_root(label: &str) -> TestRoot {
        let root = std::env::temp_dir().join(format!(
            "blockzilla-monitor-{label}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).unwrap();
        TestRoot(root)
    }

    fn valid_snapshot() -> PipelineSnapshot {
        PipelineSnapshot {
            schema_version: snapshot::STATUS_SCHEMA_VERSION,
            sequence: 1,
            now_unix_secs: 1,
            ..Default::default()
        }
    }

    fn firewatch_status() -> FirewatchControllerStatus {
        FirewatchControllerStatus {
            schema_version: 1,
            updated_unix_secs: 1,
            capacity_configured: 1,
            running: 1,
            epochs_total: 3,
            epochs_accepted: 1,
            epochs_queued: 1,
            archive_epochs_total: Some(10),
            epochs_eligible: Some(3),
            epochs_blocked_migration: Some(7),
            epochs_blocked_wire_profile: Some(0),
            queue_eta_secs: Some(86_400.0),
            admission_blocked_reason: None,
            rows: vec![
                FirewatchControllerRow {
                    epoch: 301,
                    state: "accepted".into(),
                    phase: "parity".into(),
                    auto_paused: false,
                    auto_pause_reason: None,
                    progress_pct: 100.0,
                    blocks_done: 398_090,
                    eta_secs: None,
                    rss_bytes: Some(900 * 1024 * 1024),
                    read_mib_per_sec: Some(82.5),
                    write_mib_per_sec: Some(3.25),
                    wallet_count: Some(2_045_290),
                    relation_count: Some(6_018_402),
                    parity_status: Some("equal".into()),
                },
                FirewatchControllerRow {
                    epoch: 302,
                    state: "running".into(),
                    phase: "canonical_build".into(),
                    auto_paused: false,
                    auto_pause_reason: None,
                    progress_pct: 44.0,
                    blocks_done: 175_000,
                    eta_secs: Some(120.0),
                    rss_bytes: Some(800 * 1024 * 1024),
                    read_mib_per_sec: Some(75.0),
                    write_mib_per_sec: Some(2.0),
                    wallet_count: None,
                    relation_count: None,
                    parity_status: Some("pending".into()),
                },
                FirewatchControllerRow {
                    epoch: 900,
                    state: "queued".into(),
                    phase: "target_build".into(),
                    auto_paused: false,
                    auto_pause_reason: None,
                    progress_pct: 0.0,
                    blocks_done: 0,
                    eta_secs: None,
                    rss_bytes: None,
                    read_mib_per_sec: None,
                    write_mib_per_sec: None,
                    wallet_count: None,
                    relation_count: None,
                    parity_status: Some("pending".into()),
                },
            ],
        }
    }

    fn scheduler_with_raw_firewatch() -> PipelineSnapshot {
        PipelineSnapshot {
            schema_version: snapshot::STATUS_SCHEMA_VERSION,
            sequence: 1,
            now_unix_secs: 1,
            summary: snapshot::PipelineSummary {
                firewatch_index_capacity_configured: 9,
                firewatch_index_running: 0,
                firewatch_index_epochs_total: 9,
                firewatch_index_epochs_accepted: 9,
                ..Default::default()
            },
            lanes: vec![
                snapshot::LaneStatus {
                    id: "firewatch_index:777".into(),
                    kind: "firewatch_index".into(),
                    state: "accepted".into(),
                    phase: "parity".into(),
                    epoch: Some(777),
                    parity_status: Some("equal".into()),
                    ..Default::default()
                },
                snapshot::LaneStatus {
                    id: "scan:1".into(),
                    kind: "historical_scan".into(),
                    state: "running".into(),
                    phase: "scan".into(),
                    epoch: Some(1),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    fn write_firewatch_status(path: &Path, status: &FirewatchControllerStatus) {
        std::fs::write(path, serde_json::to_vec(status).unwrap()).unwrap();
    }

    #[tokio::test]
    async fn watchdog_distinguishes_transport_idle_from_application_staleness() {
        let mut transport_idle = futures_util::stream::pending::<()>();
        let event = next_with_watchdog(
            &mut transport_idle,
            Instant::now() + Duration::from_millis(100),
            Duration::from_millis(5),
        )
        .await;
        assert_eq!(event, WatchdogEvent::Idle);

        let mut application_stale = futures_util::stream::pending::<()>();
        let event = next_with_watchdog(
            &mut application_stale,
            Instant::now() + Duration::from_millis(5),
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(event, WatchdogEvent::ApplicationStale);
    }

    #[test]
    fn application_freshness_requires_the_scheduler_timestamp_to_advance() {
        let mut latest = 100;
        assert!(!observe_application_timestamp(99, &mut latest));
        assert!(!observe_application_timestamp(100, &mut latest));
        assert!(observe_application_timestamp(101, &mut latest));
        assert_eq!(latest, 101);
    }

    #[tokio::test]
    async fn configured_controller_overlays_only_firewatch_status() {
        let root = test_root("firewatch-overlay");
        let path = root.0.join("status.json");
        write_firewatch_status(&path, &firewatch_status());

        let scheduler = scheduler_with_raw_firewatch();
        let published = snapshot_for_publication(&scheduler, Some(&path)).await;

        assert_eq!(published.summary.firewatch_index_capacity_configured, 1);
        assert_eq!(published.summary.firewatch_index_running, 1);
        assert_eq!(published.summary.firewatch_index_epochs_total, 3);
        assert_eq!(published.summary.firewatch_index_epochs_accepted, 1);
        assert_eq!(published.summary.firewatch_index_epochs_queued, 1);
        assert_eq!(
            published.summary.firewatch_index_archive_epochs_total,
            Some(10)
        );
        assert_eq!(published.summary.firewatch_index_epochs_eligible, Some(3));
        assert_eq!(
            published.summary.firewatch_index_epochs_blocked_migration,
            Some(7)
        );
        assert_eq!(
            published
                .summary
                .firewatch_index_epochs_blocked_wire_profile,
            Some(0)
        );
        assert_eq!(
            published.summary.firewatch_index_queue_eta_secs,
            Some(86_400.0)
        );
        assert!(published.lanes.iter().any(|lane| lane.id == "scan:1"));
        assert!(
            published
                .lanes
                .iter()
                .all(|lane| lane.id != "firewatch_index:777")
        );
        let accepted = published
            .lanes
            .iter()
            .find(|lane| lane.id == "firewatch_index:301")
            .unwrap();
        assert_eq!(accepted.wallet_count, Some(2_045_290));
        assert_eq!(accepted.relation_count, Some(6_018_402));
        assert_eq!(accepted.parity_status.as_deref(), Some("equal"));
        assert_eq!(accepted.progress.rss_bytes, Some(900 * 1024 * 1024));
        assert_eq!(accepted.progress.disk_read_mib_per_sec, Some(82.5));
        assert_eq!(accepted.progress.updated_unix_secs, Some(1));
        let canonical = published
            .lanes
            .iter()
            .find(|lane| lane.id == "firewatch_index:302")
            .unwrap();
        assert_eq!(canonical.phase, "canonical_build");
        published.validate().unwrap();

        // The overlay must not mutate the scheduler patch base.
        assert_eq!(scheduler.summary.firewatch_index_epochs_accepted, 9);
        assert!(
            scheduler
                .lanes
                .iter()
                .any(|lane| lane.id == "firewatch_index:777")
        );
    }

    #[tokio::test]
    async fn no_configured_file_preserves_raw_scheduler_firewatch_values() {
        let scheduler = scheduler_with_raw_firewatch();
        let published = snapshot_for_publication(&scheduler, None).await;

        assert_eq!(published.summary.firewatch_index_capacity_configured, 9);
        assert_eq!(published.summary.firewatch_index_epochs_accepted, 9);
        assert!(
            published
                .lanes
                .iter()
                .any(|lane| lane.id == "firewatch_index:777")
        );
    }

    #[tokio::test]
    async fn configured_bad_file_blocks_only_firewatch_and_clears_untrusted_rows() {
        let root = test_root("bad-firewatch-overlay");
        let path = root.0.join("status.json");
        std::fs::write(&path, b"{ malformed").unwrap();

        let published =
            snapshot_for_publication(&scheduler_with_raw_firewatch(), Some(&path)).await;

        assert_eq!(published.summary.firewatch_index_capacity_configured, 0);
        assert_eq!(published.summary.firewatch_index_epochs_accepted, 0);
        assert_eq!(published.summary.firewatch_index_epochs_total, 0);
        assert!(
            published
                .summary
                .firewatch_index_admission_blocked_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("controller status unavailable"))
        );
        assert!(
            published
                .lanes
                .iter()
                .all(|lane| lane.kind != "firewatch_index")
        );
        assert!(published.lanes.iter().any(|lane| lane.id == "scan:1"));
        published.validate().unwrap();
    }

    #[tokio::test]
    async fn configured_missing_file_blocks_only_firewatch() {
        let root = test_root("missing-firewatch-overlay");
        let path = root.0.join("missing-status.json");

        let published =
            snapshot_for_publication(&scheduler_with_raw_firewatch(), Some(&path)).await;

        assert_eq!(published.summary.firewatch_index_epochs_accepted, 0);
        assert!(
            published
                .summary
                .firewatch_index_admission_blocked_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("controller status unavailable"))
        );
        assert!(
            published
                .lanes
                .iter()
                .all(|lane| lane.kind != "firewatch_index")
        );
        assert!(published.lanes.iter().any(|lane| lane.id == "scan:1"));
    }

    #[tokio::test]
    async fn each_publication_rereads_the_controller_file() {
        let root = test_root("reread-firewatch-overlay");
        let path = root.0.join("status.json");
        let first = firewatch_status();
        write_firewatch_status(&path, &first);

        let scheduler = valid_snapshot();
        let first_published = snapshot_for_publication(&scheduler, Some(&path)).await;
        assert_eq!(first_published.summary.firewatch_index_epochs_accepted, 1);

        let mut second = first;
        second.running = 0;
        second.epochs_accepted = 2;
        second.epochs_queued = 0;
        second.rows[1].state = "accepted".into();
        second.rows[1].phase = "parity".into();
        second.rows[1].progress_pct = 100.0;
        second.rows[1].parity_status = Some("equal".into());
        second.rows[2].state = "blocked".into();
        write_firewatch_status(&path, &second);

        let second_published = snapshot_for_publication(&scheduler, Some(&path)).await;
        assert_eq!(second_published.summary.firewatch_index_epochs_accepted, 2);
        assert_eq!(second_published.summary.firewatch_index_epochs_queued, 0);
        assert_eq!(
            second_published
                .lanes
                .iter()
                .find(|lane| lane.epoch == Some(302))
                .unwrap()
                .state,
            "accepted"
        );
    }

    #[tokio::test]
    async fn controller_validation_rejects_unknown_schema_and_duplicate_epochs() {
        let root = test_root("invalid-firewatch-schema");
        let path = root.0.join("status.json");
        let mut status = firewatch_status();
        status.schema_version = 2;
        write_firewatch_status(&path, &status);
        assert!(read_firewatch_status_file(&path).await.is_err());

        status.schema_version = 1;
        status.rows[1].epoch = status.rows[0].epoch;
        write_firewatch_status(&path, &status);
        assert!(read_firewatch_status_file(&path).await.is_err());
    }

    #[test]
    fn controller_coverage_fields_are_additive_and_validated() {
        let mut legacy = serde_json::to_value(firewatch_status()).unwrap();
        let object = legacy.as_object_mut().unwrap();
        for key in [
            "archive_epochs_total",
            "epochs_eligible",
            "epochs_blocked_migration",
            "epochs_blocked_wire_profile",
            "queue_eta_secs",
        ] {
            object.remove(key);
        }
        let legacy: FirewatchControllerStatus = serde_json::from_value(legacy).unwrap();
        assert!(legacy.archive_epochs_total.is_none());
        assert!(legacy.epochs_eligible.is_none());
        assert!(legacy.epochs_blocked_migration.is_none());
        assert!(legacy.epochs_blocked_wire_profile.is_none());
        assert!(legacy.queue_eta_secs.is_none());
        legacy.validate().unwrap();

        let mut invalid = firewatch_status();
        invalid.epochs_eligible = Some(4);
        assert!(invalid.validate().is_err());

        let mut invalid_eta = firewatch_status();
        invalid_eta.queue_eta_secs = Some(-1.0);
        assert!(invalid_eta.validate().is_err());
    }

    #[test]
    fn controller_accepts_exact_canonical_build_phase_only() {
        let mut status = firewatch_status();
        assert_eq!(status.rows[1].phase, "canonical_build");
        status.validate().unwrap();

        status.rows[1].phase = "canonical-build".into();
        assert!(status.validate().is_err());
    }

    #[test]
    fn controller_accepts_profile_audit_rows_and_requires_complete_coverage() {
        let mut status = firewatch_status();
        status.rows[2].state = "profile_audit_required".into();
        status.rows[2].phase = "wire_profile_audit".into();
        status.epochs_queued = 0;
        status.epochs_eligible = Some(2);
        status.epochs_blocked_wire_profile = Some(1);
        status.validate().unwrap();

        status.rows[2].phase = "canonical_build".into();
        assert!(status.validate().is_err());
        status.rows[2].phase = "wire_profile_audit".into();
        status.epochs_blocked_wire_profile = Some(2);
        assert!(status.validate().is_err());
    }

    #[tokio::test]
    async fn stale_or_far_future_controller_status_is_blocked() {
        let root = test_root("stale-firewatch-status");
        let path = root.0.join("status.json");
        let mut scheduler = valid_snapshot();
        scheduler.now_unix_secs = 100;

        let mut status = firewatch_status();
        status.updated_unix_secs = 69;
        write_firewatch_status(&path, &status);
        let stale = snapshot_for_publication(&scheduler, Some(&path)).await;
        assert_eq!(stale.summary.firewatch_index_epochs_accepted, 0);
        assert!(
            stale
                .summary
                .firewatch_index_admission_blocked_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("differs from scheduler time"))
        );
        assert!(
            stale
                .lanes
                .iter()
                .all(|lane| lane.kind != "firewatch_index")
        );

        status.updated_unix_secs = 131;
        write_firewatch_status(&path, &status);
        let future = snapshot_for_publication(&scheduler, Some(&path)).await;
        assert_eq!(future.summary.firewatch_index_epochs_accepted, 0);
        assert!(
            future
                .lanes
                .iter()
                .all(|lane| lane.kind != "firewatch_index")
        );

        status.updated_unix_secs = 130;
        write_firewatch_status(&path, &status);
        let boundary = snapshot_for_publication(&scheduler, Some(&path)).await;
        assert_eq!(boundary.summary.firewatch_index_epochs_accepted, 1);
    }

    #[tokio::test]
    async fn local_firewatch_status_rejects_sparse_oversize_without_reading_it_all() {
        let root = test_root("sparse-firewatch-status");
        let path = root.0.join("status.json");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len((MAX_FIREWATCH_STATUS_BYTES + 1) as u64)
            .unwrap();

        let result =
            tokio::time::timeout(Duration::from_secs(1), read_firewatch_status_file(&path))
                .await
                .expect("sparse oversize rejection must be bounded");
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_firewatch_status_rejects_symlink() {
        let root = test_root("symlink-firewatch-status");
        let target = root.0.join("target.json");
        let link = root.0.join("status.json");
        write_firewatch_status(&target, &firewatch_status());
        std::os::unix::fs::symlink(&target, &link).unwrap();

        assert!(read_firewatch_status_file(&link).await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_firewatch_status_rejects_fifo_without_blocking() {
        use std::{ffi::CString, os::unix::ffi::OsStrExt};

        let root = test_root("fifo-firewatch-status");
        let path = root.0.join("status.json");
        let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `c_path` is a live, NUL-terminated path and `mkfifo` does
        // not retain the pointer after returning.
        let created = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(
            created,
            0,
            "create FIFO: {}",
            std::io::Error::last_os_error()
        );

        let result =
            tokio::time::timeout(Duration::from_secs(1), read_firewatch_status_file(&path))
                .await
                .expect("FIFO rejection must not wait for a writer");
        assert!(result.is_err());
    }

    #[test]
    fn malformed_full_snapshot_envelope_fails_closed() {
        let malformed = serde_json::json!({
            "type": "snapshot",
            "sequence": 1,
            "data": {"schema_version": snapshot::STATUS_SCHEMA_VERSION}
        })
        .to_string();
        assert!(decode_envelope::<PipelineSnapshot>(&malformed, "snapshot").is_err());

        let wrong_type = serde_json::json!({
            "type": "snapshot_patch",
            "sequence": 1,
            "data": valid_snapshot()
        })
        .to_string();
        assert!(decode_envelope::<PipelineSnapshot>(&wrong_type, "snapshot").is_err());
    }

    #[tokio::test]
    async fn invalid_full_snapshot_never_replaces_the_session_base() {
        let current = valid_snapshot();
        let mut session = Session {
            client: reqwest::Client::new(),
            upstream: "http://127.0.0.1:1".into(),
            last_sequence: 1,
            current,
            firewatch_status_file: None,
        };
        let mut invalid = valid_snapshot();
        invalid.schema_version = snapshot::STATUS_SCHEMA_VERSION - 1;
        invalid.sequence = 2;
        invalid.now_unix_secs = 2;

        assert!(session.accept_snapshot(2, invalid).await.is_err());
        assert_eq!(session.last_sequence, 1);
        assert_eq!(session.current.sequence, 1);
    }

    #[tokio::test]
    async fn malformed_patch_candidate_requests_resync_without_publication() {
        let current = valid_snapshot();
        let mut session = Session {
            client: reqwest::Client::new(),
            upstream: "http://127.0.0.1:1".into(),
            last_sequence: 1,
            current,
            firewatch_status_file: None,
        };
        let patch = PipelineSnapshotPatch {
            schema_version: snapshot::STATUS_SCHEMA_VERSION,
            sequence: 2,
            now_unix_secs: 2,
            summary: snapshot::PipelineSummary {
                epochs_total: 1,
                queued: 1,
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(
            session.handle_patch(2, patch).await.unwrap(),
            PatchOutcome::Resync
        );
        assert_eq!(session.last_sequence, 1);
        assert_eq!(session.current.sequence, 1);
    }

    #[tokio::test]
    async fn local_gap_index_rejects_sparse_oversize_without_reading_it_all() {
        let root = test_root("sparse-gap-index");
        let path = root.0.join("index.json");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len((MAX_GAP_INDEX_BYTES + 1) as u64).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), read_gap_index_file(&path))
            .await
            .expect("sparse oversize rejection must be bounded");
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_gap_index_rejects_fifo_without_blocking() {
        use std::{ffi::CString, os::unix::ffi::OsStrExt};

        let root = test_root("fifo-gap-index");
        let path = root.0.join("index.json");
        let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `c_path` is a live, NUL-terminated path and `mkfifo` does
        // not retain the pointer after returning.
        let created = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(
            created,
            0,
            "create FIFO: {}",
            std::io::Error::last_os_error()
        );

        let result = tokio::time::timeout(Duration::from_secs(1), read_gap_index_file(&path))
            .await
            .expect("FIFO rejection must not wait for a writer");
        assert!(result.is_err());
    }
}
