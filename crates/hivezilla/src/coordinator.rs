use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    process::Command,
    sync::{Mutex, broadcast},
};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tower_http::cors::CorsLayer;

const BYTES_PER_TB: f64 = 1_000_000_000_000.0;
const BYTES_PER_GB: f64 = 1_000_000_000.0;

#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    pub bind: SocketAddr,
    pub event_dir: PathBuf,
    pub artifact_dir: Option<PathBuf>,
    pub token: Option<String>,
    pub destroy_hetzner: bool,
    pub dry_run: bool,
    pub target_bytes: u64,
    pub default_job_input_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEvent {
    pub run_id: String,
    pub worker_id: String,
    pub job_id: String,
    pub status: String,
    pub epoch: u64,
    pub shard_index: u32,
    pub shard_count: u32,
    pub slot_start: u64,
    pub slot_end: u64,
    pub output_uri: String,
    pub provider: Option<String>,
    pub server_id: Option<String>,
    pub server_name: Option<String>,
    pub finished_unix_secs: u64,
    #[serde(default)]
    pub input_bytes: Option<u64>,
    #[serde(default)]
    pub output_bytes: Option<u64>,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HiveStateSnapshot {
    pub now_unix_secs: u64,
    pub target_bytes: u64,
    pub processed_bytes: u64,
    pub gbps: f64,
    pub eta_secs: Option<u64>,
    pub open_machines: usize,
    pub artifact_downloaded_count: usize,
    pub jobs_total: usize,
    pub jobs_running: usize,
    pub jobs_finished: usize,
    pub jobs_failed: usize,
    pub epochs: Vec<EpochSnapshot>,
    pub jobs: Vec<JobSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EpochSnapshot {
    pub epoch: u64,
    pub status: String,
    pub progress_pct: f64,
    pub jobs_total: usize,
    pub jobs_running: usize,
    pub jobs_finished: usize,
    pub artifacts_downloaded: usize,
    pub processed_bytes: u64,
    pub slot_start: u64,
    pub slot_end: u64,
    pub updated_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobSnapshot {
    pub run_id: String,
    pub worker_id: String,
    pub job_id: String,
    pub status: String,
    pub epoch: u64,
    pub shard_index: u32,
    pub shard_count: u32,
    pub slot_start: u64,
    pub slot_end: u64,
    pub output_uri: String,
    pub provider: Option<String>,
    pub server_id: Option<String>,
    pub server_name: Option<String>,
    pub artifact_downloaded: bool,
    pub server_destroyed: bool,
    pub input_bytes: u64,
    pub output_bytes: Option<u64>,
    pub started_unix_secs: Option<u64>,
    pub updated_unix_secs: u64,
}

#[derive(Debug)]
struct CoordinatorAppState {
    config: CoordinatorConfig,
    hive: Mutex<HiveState>,
    updates: broadcast::Sender<HiveStateSnapshot>,
}

#[derive(Debug)]
struct HiveState {
    target_bytes: u64,
    default_job_input_bytes: u64,
    first_activity_unix_secs: Option<u64>,
    jobs: BTreeMap<String, JobRecord>,
}

#[derive(Debug, Clone)]
struct JobRecord {
    event: JobEvent,
    artifact_downloaded: bool,
    server_destroyed: bool,
    input_bytes: u64,
    output_bytes: Option<u64>,
    started_unix_secs: Option<u64>,
    updated_unix_secs: u64,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct AcceptResponse {
    accepted: bool,
    event_path: String,
}

#[derive(Debug)]
enum CoordinatorError {
    Unauthorized,
    Internal(anyhow::Error),
}

impl IntoResponse for CoordinatorError {
    fn into_response(self) -> Response {
        match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
            Self::Internal(err) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:?}")).into_response()
            }
        }
    }
}

pub async fn run_coordinator(config: CoordinatorConfig) -> Result<()> {
    fs::create_dir_all(&config.event_dir)
        .await
        .with_context(|| format!("create event dir {}", config.event_dir.display()))?;
    if let Some(artifact_dir) = config.artifact_dir.as_deref() {
        fs::create_dir_all(artifact_dir)
            .await
            .with_context(|| format!("create artifact dir {}", artifact_dir.display()))?;
    }

    let bind = config.bind;
    let (updates, _) = broadcast::channel(256);
    let app_state = Arc::new(CoordinatorAppState {
        hive: Mutex::new(HiveState::new(
            config.target_bytes,
            config.default_job_input_bytes,
        )),
        config,
        updates,
    });
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/state", get(state_snapshot))
        .route("/events", get(state_events))
        .route("/job-event", post(job_event))
        .layer(CorsLayer::permissive())
        .with_state(app_state);
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind coordinator on {bind}"))?;

    axum::serve(listener, app)
        .await
        .context("run coordinator server")
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn state_snapshot(State(state): State<Arc<CoordinatorAppState>>) -> Json<HiveStateSnapshot> {
    Json(state.hive.lock().await.snapshot())
}

async fn state_events(
    State(state): State<Arc<CoordinatorAppState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let initial = state.hive.lock().await.snapshot();
    let initial_stream = tokio_stream::once(Ok(Event::default()
        .event("state")
        .data(serde_json::to_string(&initial).unwrap_or_else(|_| "{}".to_string()))));
    let update_stream = BroadcastStream::new(state.updates.subscribe()).filter_map(|message| {
        message.ok().map(|snapshot| {
            Ok(Event::default()
                .event("state")
                .data(serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".to_string())))
        })
    });

    Sse::new(initial_stream.chain(update_stream)).keep_alive(KeepAlive::default())
}

async fn job_event(
    State(state): State<Arc<CoordinatorAppState>>,
    headers: HeaderMap,
    Json(event): Json<JobEvent>,
) -> Result<Json<AcceptResponse>, CoordinatorError> {
    authorize(&state.config, &headers)?;
    let event_path = write_event(&state.config.event_dir, &event)
        .await
        .map_err(CoordinatorError::Internal)?;
    apply_job_event(&state, event.clone()).await;
    let state_for_task = Arc::clone(&state);
    let event_for_task = event.clone();

    tokio::spawn(async move {
        if let Err(err) = process_event(state_for_task, event_for_task).await {
            eprintln!("hivezilla coordinator event processing failed: {err:?}");
        }
    });

    Ok(Json(AcceptResponse {
        accepted: true,
        event_path: event_path.display().to_string(),
    }))
}

fn authorize(config: &CoordinatorConfig, headers: &HeaderMap) -> Result<(), CoordinatorError> {
    let Some(token) = config.token.as_deref() else {
        return Ok(());
    };
    let expected = format!("Bearer {token}");
    let actual = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    if actual == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(CoordinatorError::Unauthorized)
    }
}

async fn write_event(event_dir: &Path, event: &JobEvent) -> Result<PathBuf> {
    let run_dir = event_dir.join(safe_path_segment(&event.run_id));
    fs::create_dir_all(&run_dir)
        .await
        .with_context(|| format!("create run event dir {}", run_dir.display()))?;
    let event_path = run_dir.join(format!("{}.json", safe_path_segment(&event.job_id)));
    let json = serde_json::to_string_pretty(event).context("serialize job event")?;
    fs::write(&event_path, format!("{json}\n"))
        .await
        .with_context(|| format!("write event {}", event_path.display()))?;

    let json_line = serde_json::to_string(event).context("serialize job event line")?;
    let events_jsonl = event_dir.join("events.jsonl");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&events_jsonl)
        .await
        .with_context(|| format!("open {}", events_jsonl.display()))?;
    file.write_all(json_line.as_bytes())
        .await
        .with_context(|| format!("append {}", events_jsonl.display()))?;
    file.write_all(b"\n")
        .await
        .with_context(|| format!("append newline {}", events_jsonl.display()))?;

    Ok(event_path)
}

async fn process_event(state: Arc<CoordinatorAppState>, event: JobEvent) -> Result<()> {
    if event.status != "success" {
        write_processing_note(&state.config, &event, "skip non-success event").await?;
        return Ok(());
    }

    if let Some(artifact_root) = state.config.artifact_dir.as_deref() {
        let destination = artifact_root
            .join(safe_path_segment(&event.run_id))
            .join(safe_path_segment(&event.job_id));
        pull_artifact(&event.output_uri, &destination, state.config.dry_run).await?;
        write_processing_note(
            &state.config,
            &event,
            &format!("artifact pulled to {}", destination.display()),
        )
        .await?;
        mark_artifact_downloaded(&state, &event.job_id).await;
    }

    if state.config.destroy_hetzner {
        destroy_hetzner_server(&event, state.config.dry_run).await?;
        write_processing_note(&state.config, &event, "hetzner destroy complete").await?;
        mark_server_destroyed(&state, &event.job_id).await;
    }

    Ok(())
}

async fn pull_artifact(source_uri: &str, destination: &Path, dry_run: bool) -> Result<()> {
    fs::create_dir_all(destination)
        .await
        .with_context(|| format!("create artifact destination {}", destination.display()))?;

    if dry_run {
        eprintln!(
            "dry-run: would pull artifact {} -> {}",
            source_uri,
            destination.display()
        );
        return Ok(());
    }

    let status = if is_rclone_ref(source_uri) {
        Command::new("rclone")
            .arg("copy")
            .arg(format!("{}/", source_uri.trim_end_matches('/')))
            .arg(format!("{}/", destination.display()))
            .status()
            .await
            .with_context(|| format!("spawn rclone for {source_uri}"))?
    } else {
        Command::new("rsync")
            .arg("-a")
            .arg(format!("{}/", source_uri.trim_end_matches('/')))
            .arg(format!("{}/", destination.display()))
            .status()
            .await
            .with_context(|| format!("spawn rsync for {source_uri}"))?
    };

    anyhow::ensure!(
        status.success(),
        "artifact pull failed for {} -> {} with status {}",
        source_uri,
        destination.display(),
        status
    );
    Ok(())
}

async fn destroy_hetzner_server(event: &JobEvent, dry_run: bool) -> Result<()> {
    let server = event
        .server_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            event
                .server_name
                .as_deref()
                .filter(|value| !value.is_empty())
        })
        .with_context(|| {
            format!(
                "no server_id/server_name in event {}; cannot destroy Hetzner server",
                event.job_id
            )
        })?;

    if dry_run {
        eprintln!("dry-run: would run hcloud server delete {server}");
        return Ok(());
    }

    let status = Command::new("hcloud")
        .arg("server")
        .arg("delete")
        .arg(server)
        .status()
        .await
        .with_context(|| format!("spawn hcloud server delete {server}"))?;
    anyhow::ensure!(
        status.success(),
        "hcloud server delete {server} failed with status {status}"
    );
    Ok(())
}

async fn write_processing_note(
    config: &CoordinatorConfig,
    event: &JobEvent,
    note: &str,
) -> Result<()> {
    let run_dir = config.event_dir.join(safe_path_segment(&event.run_id));
    fs::create_dir_all(&run_dir)
        .await
        .with_context(|| format!("create run dir {}", run_dir.display()))?;
    let path = run_dir.join(format!(
        "{}.processing.log",
        safe_path_segment(&event.job_id)
    ));
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    file.write_all(note.as_bytes())
        .await
        .with_context(|| format!("write {}", path.display()))?;
    file.write_all(b"\n")
        .await
        .with_context(|| format!("write newline {}", path.display()))?;
    Ok(())
}

async fn apply_job_event(state: &Arc<CoordinatorAppState>, event: JobEvent) {
    let snapshot = {
        let mut hive = state.hive.lock().await;
        hive.apply_event(event);
        hive.snapshot()
    };
    let _ = state.updates.send(snapshot);
}

async fn mark_artifact_downloaded(state: &Arc<CoordinatorAppState>, job_id: &str) {
    let snapshot = {
        let mut hive = state.hive.lock().await;
        hive.mark_artifact_downloaded(job_id);
        hive.snapshot()
    };
    let _ = state.updates.send(snapshot);
}

async fn mark_server_destroyed(state: &Arc<CoordinatorAppState>, job_id: &str) {
    let snapshot = {
        let mut hive = state.hive.lock().await;
        hive.mark_server_destroyed(job_id);
        hive.snapshot()
    };
    let _ = state.updates.send(snapshot);
}

impl HiveState {
    fn new(target_bytes: u64, default_job_input_bytes: u64) -> Self {
        Self {
            target_bytes,
            default_job_input_bytes,
            first_activity_unix_secs: None,
            jobs: BTreeMap::new(),
        }
    }

    fn apply_event(&mut self, event: JobEvent) {
        let now = unix_now();
        let event_time = if event.finished_unix_secs > 0 {
            event.finished_unix_secs
        } else {
            now
        };
        match self.first_activity_unix_secs {
            Some(started) if event_time < started => {
                self.first_activity_unix_secs = Some(event_time)
            }
            None => self.first_activity_unix_secs = Some(event_time),
            _ => {}
        }
        let input_bytes = event.input_bytes.unwrap_or(self.default_job_input_bytes);
        let output_bytes = event.output_bytes;
        let started_unix_secs = if event.status == "running" {
            Some(event_time)
        } else {
            self.jobs
                .get(&event.job_id)
                .and_then(|existing| existing.started_unix_secs)
        };

        self.jobs
            .entry(event.job_id.clone())
            .and_modify(|record| {
                record.event = event.clone();
                record.input_bytes = input_bytes;
                record.output_bytes = output_bytes.or(record.output_bytes);
                record.started_unix_secs = started_unix_secs.or(record.started_unix_secs);
                record.updated_unix_secs = now;
            })
            .or_insert_with(|| JobRecord {
                event,
                artifact_downloaded: false,
                server_destroyed: false,
                input_bytes,
                output_bytes,
                started_unix_secs,
                updated_unix_secs: now,
            });
    }

    fn mark_artifact_downloaded(&mut self, job_id: &str) {
        if let Some(record) = self.jobs.get_mut(job_id) {
            record.artifact_downloaded = true;
            record.updated_unix_secs = unix_now();
        }
    }

    fn mark_server_destroyed(&mut self, job_id: &str) {
        if let Some(record) = self.jobs.get_mut(job_id) {
            record.server_destroyed = true;
            record.updated_unix_secs = unix_now();
        }
    }

    fn snapshot(&self) -> HiveStateSnapshot {
        let now = unix_now();
        let processed_bytes = self
            .jobs
            .values()
            .filter(|job| is_finished_status(&job.event.status))
            .map(|job| job.input_bytes)
            .sum::<u64>()
            .min(self.target_bytes);
        let elapsed_secs = self
            .first_activity_unix_secs
            .map(|started| now.saturating_sub(started))
            .unwrap_or(0)
            .max(1);
        let bytes_per_sec = processed_bytes as f64 / elapsed_secs as f64;
        let gbps = bytes_per_sec * 8.0 / 1_000_000_000.0;
        let remaining_bytes = self.target_bytes.saturating_sub(processed_bytes);
        let eta_secs = if bytes_per_sec > 0.0 && remaining_bytes > 0 {
            Some((remaining_bytes as f64 / bytes_per_sec).ceil() as u64)
        } else if remaining_bytes == 0 {
            Some(0)
        } else {
            None
        };

        let open_machines = self
            .jobs
            .values()
            .filter(|job| is_open_machine(job))
            .map(|job| job.event.worker_id.clone())
            .collect::<BTreeSet<_>>()
            .len();
        let artifact_downloaded_count = self
            .jobs
            .values()
            .filter(|job| job.artifact_downloaded)
            .count();
        let jobs_running = self
            .jobs
            .values()
            .filter(|job| is_running_status(&job.event.status))
            .count();
        let jobs_finished = self
            .jobs
            .values()
            .filter(|job| is_finished_status(&job.event.status))
            .count();
        let jobs_failed = self
            .jobs
            .values()
            .filter(|job| is_failed_status(&job.event.status))
            .count();

        HiveStateSnapshot {
            now_unix_secs: now,
            target_bytes: self.target_bytes,
            processed_bytes,
            gbps,
            eta_secs,
            open_machines,
            artifact_downloaded_count,
            jobs_total: self.jobs.len(),
            jobs_running,
            jobs_finished,
            jobs_failed,
            epochs: self.epoch_snapshots(),
            jobs: self.job_snapshots(),
        }
    }

    fn epoch_snapshots(&self) -> Vec<EpochSnapshot> {
        let mut epochs = BTreeMap::<u64, Vec<&JobRecord>>::new();
        for job in self.jobs.values() {
            epochs.entry(job.event.epoch).or_default().push(job);
        }

        epochs
            .into_iter()
            .map(|(epoch, jobs)| {
                let jobs_total = jobs.len();
                let jobs_finished = jobs
                    .iter()
                    .filter(|job| is_finished_status(&job.event.status))
                    .count();
                let jobs_running = jobs
                    .iter()
                    .filter(|job| is_running_status(&job.event.status))
                    .count();
                let artifacts_downloaded =
                    jobs.iter().filter(|job| job.artifact_downloaded).count();
                let processed_bytes = jobs
                    .iter()
                    .filter(|job| is_finished_status(&job.event.status))
                    .map(|job| job.input_bytes)
                    .sum::<u64>();
                let slot_start = jobs
                    .iter()
                    .map(|job| job.event.slot_start)
                    .min()
                    .unwrap_or_default();
                let slot_end = jobs
                    .iter()
                    .map(|job| job.event.slot_end)
                    .max()
                    .unwrap_or_default();
                let updated_unix_secs = jobs
                    .iter()
                    .map(|job| job.updated_unix_secs)
                    .max()
                    .unwrap_or_default();
                let progress_pct = if jobs_total == 0 {
                    0.0
                } else {
                    jobs_finished as f64 * 100.0 / jobs_total as f64
                };
                let status = epoch_status(jobs_total, jobs_running, jobs_finished);

                EpochSnapshot {
                    epoch,
                    status,
                    progress_pct,
                    jobs_total,
                    jobs_running,
                    jobs_finished,
                    artifacts_downloaded,
                    processed_bytes,
                    slot_start,
                    slot_end,
                    updated_unix_secs,
                }
            })
            .collect()
    }

    fn job_snapshots(&self) -> Vec<JobSnapshot> {
        self.jobs
            .values()
            .map(|job| JobSnapshot {
                run_id: job.event.run_id.clone(),
                worker_id: job.event.worker_id.clone(),
                job_id: job.event.job_id.clone(),
                status: job.event.status.clone(),
                epoch: job.event.epoch,
                shard_index: job.event.shard_index,
                shard_count: job.event.shard_count,
                slot_start: job.event.slot_start,
                slot_end: job.event.slot_end,
                output_uri: job.event.output_uri.clone(),
                provider: job.event.provider.clone(),
                server_id: job.event.server_id.clone(),
                server_name: job.event.server_name.clone(),
                artifact_downloaded: job.artifact_downloaded,
                server_destroyed: job.server_destroyed,
                input_bytes: job.input_bytes,
                output_bytes: job.output_bytes,
                started_unix_secs: job.started_unix_secs,
                updated_unix_secs: job.updated_unix_secs,
            })
            .collect()
    }
}

fn epoch_status(jobs_total: usize, jobs_running: usize, jobs_finished: usize) -> String {
    if jobs_total > 0 && jobs_finished == jobs_total {
        "complete".to_string()
    } else if jobs_running > 0 || jobs_finished > 0 {
        "running".to_string()
    } else {
        "queued".to_string()
    }
}

fn is_running_status(status: &str) -> bool {
    matches!(
        status,
        "running" | "materializing" | "building" | "publishing"
    )
}

fn is_finished_status(status: &str) -> bool {
    matches!(status, "success" | "artifact_downloaded" | "destroyed")
}

fn is_failed_status(status: &str) -> bool {
    matches!(status, "failed" | "error")
}

fn is_open_machine(job: &JobRecord) -> bool {
    !job.server_destroyed && !is_failed_status(&job.event.status)
}

pub fn tb_to_bytes(tb: f64) -> u64 {
    (tb * BYTES_PER_TB).round().max(0.0) as u64
}

pub fn gb_to_bytes(gb: f64) -> u64 {
    (gb * BYTES_PER_GB).round().max(0.0) as u64
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn is_rclone_ref(value: &str) -> bool {
    !(value.starts_with("http://") || value.starts_with("https://"))
        && (value.contains("://") || value.contains(':'))
}

fn safe_path_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}
