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

use std::time::Duration;

use anyhow::{Context, ensure};
use futures_core::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, de::DeserializeOwned};
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
        state::set_snapshot(self.current.clone()).await;
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
                state::set_snapshot(self.current.clone()).await;
                Ok(PatchOutcome::Applied)
            }
        }
    }
}

async fn run(upstream: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()?;
    let mut session = Session::bootstrap(client, upstream).await?;

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
    use std::io::Read;

    use rustix::fs::{FileType, Mode, OFlags};

    let fd = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .with_context(|| format!("open gap index {}", path.display()))?;
    let before = rustix::fs::fstat(&fd).context("inspect opened gap index")?;
    ensure!(
        FileType::from_raw_mode(before.st_mode) == FileType::RegularFile,
        "gap index is not a regular file"
    );
    let advertised_size = usize::try_from(before.st_size)
        .context("gap index has a negative or unrepresentable size")?;
    ensure!(
        advertised_size <= MAX_GAP_INDEX_BYTES,
        "gap index exceeds {MAX_GAP_INDEX_BYTES} bytes"
    );

    let mut file = std::fs::File::from(fd);
    let mut bytes = Vec::with_capacity(advertised_size.min(MAX_GAP_INDEX_BYTES + 1));
    (&mut file)
        .take((MAX_GAP_INDEX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .context("read bounded gap index")?;
    ensure!(
        bytes.len() <= MAX_GAP_INDEX_BYTES,
        "gap index exceeds {MAX_GAP_INDEX_BYTES} bytes"
    );

    let after = rustix::fs::fstat(&file).context("reinspect opened gap index")?;
    let path_after = rustix::fs::lstat(path).context("reinspect gap-index path")?;
    ensure!(
        same_file_identity(&before, &after)
            && same_file_version(&before, &after)
            && same_file_identity(&after, &path_after)
            && FileType::from_raw_mode(path_after.st_mode) == FileType::RegularFile
            && after.st_size == path_after.st_size
            && usize::try_from(after.st_size).ok() == Some(bytes.len()),
        "gap index changed while it was being read"
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
