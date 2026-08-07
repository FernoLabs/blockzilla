//! Background task that ingests the real Blockzilla pipeline snapshot from
//! `blockzilla-watcher-gateway` (the redacted, public-safe proxy in front of
//! the scheduler -- see `services/blockzilla-watcher-gateway`) and feeds it
//! into `state::set_snapshot`.
//!
//! This intentionally talks to the *gateway*, not the scheduler directly:
//! the gateway is the component that strips secrets and absolute paths
//! (`public_json.rs`) before anything leaves the host. Pointing this
//! dashboard at the scheduler's raw status port would defeat that boundary.
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

use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;

use crate::snapshot::{self, PipelineSnapshot, PipelineSnapshotPatch, SequenceAction};
use crate::state;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_DELAY: Duration = Duration::from_secs(3);
/// Matches `blockzilla-watcher-gateway`'s own `sse::MAX_SSE_LINE_BYTES`: a
/// full `snapshot` event's `data:` line can legitimately run several MiB
/// (the live snapshot is ~4.3 MiB today), but an upstream sending an
/// unbounded line with no `\n` should not be able to grow this buffer
/// without limit -- bail and let the reconnect loop in `start` retry
/// instead.
const MAX_SSE_LINE_BYTES: usize = 8 * 1024 * 1024;

pub fn start(upstream: String) {
    tokio::spawn(async move {
        loop {
            if let Err(err) = run(&upstream).await {
                state::set_offline(err.to_string()).await;
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    });
}

/// SSE envelope shape: `{"type": "...", "sequence": N, "data": ...}`.
#[derive(Deserialize)]
struct Envelope {
    sequence: i64,
    data: serde_json::Value,
}

struct Session {
    client: reqwest::Client,
    upstream: String,
    current: PipelineSnapshot,
    last_sequence: i64,
}

impl Session {
    async fn bootstrap(client: reqwest::Client, upstream: &str) -> anyhow::Result<Self> {
        let current = fetch_status(&client, upstream).await?;
        let last_sequence = current.sequence as i64;
        state::set_snapshot(current.clone()).await;
        Ok(Session {
            client,
            upstream: upstream.to_string(),
            current,
            last_sequence,
        })
    }

    async fn resync(&mut self) -> anyhow::Result<()> {
        self.current = fetch_status(&self.client, &self.upstream).await?;
        self.last_sequence = self.current.sequence as i64;
        state::set_snapshot(self.current.clone()).await;
        Ok(())
    }

    async fn accept_snapshot(&mut self, snapshot: PipelineSnapshot) {
        // A restarted upstream resets its process-local sequence counter;
        // a newer `now_unix_secs` is the evidence that's what happened
        // rather than a stale/duplicate full-snapshot event arriving late.
        if (snapshot.sequence as i64) <= self.last_sequence
            && snapshot.now_unix_secs <= self.current.now_unix_secs
        {
            return;
        }
        self.last_sequence = snapshot.sequence as i64;
        self.current = snapshot;
        state::set_snapshot(self.current.clone()).await;
    }

    /// Returns `true` if the patch was applied (or ignored as stale) and no
    /// resync is needed; `false` means the caller should resync.
    async fn handle_patch(
        &mut self,
        envelope_sequence: i64,
        patch: PipelineSnapshotPatch,
    ) -> anyhow::Result<bool> {
        match snapshot::sequence_action(self.last_sequence, envelope_sequence) {
            SequenceAction::Ignore => Ok(true),
            SequenceAction::Resync => Ok(false),
            SequenceAction::Apply => {
                if patch.sequence as i64 != envelope_sequence {
                    return Ok(false);
                }
                self.current.apply_patch(patch);
                self.last_sequence = envelope_sequence;
                state::set_snapshot(self.current.clone()).await;
                Ok(true)
            }
        }
    }
}

async fn run(upstream: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let mut session = Session::bootstrap(client, upstream).await?;

    let response = session
        .client
        .get(format!("{upstream}/api/v1/events"))
        .header("accept", "text/event-stream")
        .send()
        .await?
        .error_for_status()?;

    let mut body = response.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut event_name = String::new();

    while let Some(chunk) = body.next().await {
        buf.extend_from_slice(&chunk?);
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
                    if let Some(envelope) = parse_envelope(data)
                        && let Ok(snapshot) =
                            serde_json::from_value::<PipelineSnapshot>(envelope.data)
                    {
                        session.accept_snapshot(snapshot).await;
                    }
                }
                "snapshot_patch" => {
                    let Some(envelope) = parse_envelope(data) else {
                        continue;
                    };
                    let needs_resync =
                        match serde_json::from_value::<PipelineSnapshotPatch>(envelope.data) {
                            Ok(patch) => !session.handle_patch(envelope.sequence, patch).await?,
                            Err(_) => true,
                        };
                    if needs_resync {
                        session.resync().await?;
                    }
                }
                "resync" => {
                    session.resync().await?;
                }
                _ => {}
            }
        }
    }

    anyhow::bail!("event stream ended")
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
    Ok(response.json::<PipelineSnapshot>().await?)
}

fn parse_envelope(data: &str) -> Option<Envelope> {
    serde_json::from_str(data).ok()
}

/// A slow-changing offline batch artifact (`blockzilla build-block-time-gaps`
/// and `build-block-time-gap-index`, run outside the scheduler entirely --
/// see docs/reference/block-time-gap-sidecar.md), not live telemetry, so
/// this polls on its own long-period loop rather than joining the SSE
/// stream above. `blockzilla-watcher-gateway` already allowlists this path
/// (`public_proxy.rs`), but as of this writing the scheduler has no HTTP
/// handler for it at all -- every request 502s regardless of whether the
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
    Ok(response.json::<snapshot::BlockTimeGapIndex>().await?)
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
    let bytes = tokio::fs::read(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
