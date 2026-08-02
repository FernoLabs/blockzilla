//! Bounded projection of the private block-time-gap backfill status.
//!
//! The producer document may contain paths, commands, and diagnostics. This
//! module deserializes only the public contract, validates cross-field
//! invariants, and writes a fresh document through an atomic rename.

use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::{
    fd::AsRawFd,
    unix::fs::{OpenOptionsExt, PermissionsExt},
};

const MAX_SOURCE_BYTES: u64 = 64 * 1024;
const MAX_PUBLIC_BYTES: usize = 16 * 1024;
const MAX_PROGRESS_CHARS: usize = 64;

const STATES: &[&str] = &[
    "starting",
    "waiting_for_resources",
    "running",
    "paused_for_resources",
    "stopped",
    "failed",
    "complete",
];

const PROGRESS_STATES: &[&str] = &[
    "starting",
    "waiting",
    "queued",
    "running",
    "extracting",
    "scanning",
    "writing",
    "paused",
    "stopped",
    "failed",
    "complete",
    "done",
];

#[derive(Debug, Clone, Args)]
pub struct PublishArgs {
    /// Private scheduler status JSON. It is read only and never served directly.
    #[arg(long)]
    source: PathBuf,

    /// Atomically replaced public status JSON.
    #[arg(long)]
    output: PathBuf,

    /// Publication interval in seconds.
    #[arg(long, default_value_t = 15.0, value_parser = positive_interval)]
    interval_secs: f64,

    /// Publish one sample and exit.
    #[arg(long)]
    once: bool,
}

fn positive_interval(value: &str) -> std::result::Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| "interval must be a number".to_string())?;
    if !parsed.is_finite() || parsed <= 0.0 {
        return Err("interval must be positive and finite".to_string());
    }
    Ok(parsed)
}

#[derive(Debug, Deserialize)]
struct PrivateStatus {
    schema_version: u32,
    state: String,
    started_unix_seconds: u64,
    updated_unix_seconds: u64,
    backfill: PrivateBackfill,
    current: PrivateCurrent,
    resources: PrivateResources,
    last_error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PrivateBackfill {
    epochs_done: u64,
    epochs_total: u64,
    workers_configured: u64,
    active_workers: u64,
    overall_source_bytes_done: u64,
    source_bytes_total: u64,
    wall_throughput_bytes_per_second: f64,
    eta_seconds: Option<u64>,
    eta_reliable: bool,
}

#[derive(Debug, Deserialize)]
struct PrivateCurrent {
    epoch: Option<u64>,
    progress_state: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PrivateResources {
    paused_seconds: u64,
}

#[derive(Debug, PartialEq, Serialize)]
struct PublicStatus {
    schema_version: u32,
    state: String,
    started_unix_secs: u64,
    updated_unix_secs: u64,
    backfill: PublicBackfill,
    current: PublicCurrent,
    paused_secs: u64,
    last_error: Option<String>,
}

#[derive(Debug, PartialEq, Serialize)]
struct PublicBackfill {
    epochs_done: u64,
    epochs_total: u64,
    workers_configured: u64,
    active_workers: u64,
    source_bytes_done: u64,
    source_bytes_total: u64,
    throughput_bytes_per_sec: f64,
    eta_secs: Option<u64>,
    eta_reliable: bool,
}

#[derive(Debug, PartialEq, Serialize)]
struct PublicCurrent {
    epoch: Option<u64>,
    progress_state: Option<String>,
}

pub async fn publish(args: PublishArgs) -> Result<()> {
    ensure!(
        args.interval_secs.is_finite() && args.interval_secs > 0.0,
        "--interval-secs must be positive and finite"
    );
    ensure!(
        args.source != args.output,
        "--source and --output must differ"
    );
    let parent = parent_or_dot(&args.output);
    fs::create_dir_all(parent)
        .with_context(|| format!("create backfill status directory {}", parent.display()))?;
    let lock_path = parent.join(".block-time-gap-backfill-publisher.lock");
    let _lock = PublisherLock::acquire(&lock_path)?;
    let interval = interval_duration(args.interval_secs)?;

    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install backfill publisher SIGTERM handler")?;

    loop {
        match publish_once(&args.source, &args.output) {
            Ok(()) => {}
            Err(error) if !args.once => {
                eprintln!("block-time-gap status publish failed: {error:#}");
            }
            Err(error) => return Err(error),
        }
        if args.once {
            break;
        }

        #[cfg(unix)]
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            result = tokio::signal::ctrl_c() => {
                result.context("wait for backfill publisher SIGINT")?;
                break;
            }
            _ = terminate.recv() => break,
        }
        #[cfg(not(unix))]
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            result = tokio::signal::ctrl_c() => {
                result.context("wait for backfill publisher interrupt")?;
                break;
            }
        }
    }
    Ok(())
}

fn interval_duration(seconds: f64) -> Result<Duration> {
    let duration = Duration::try_from_secs_f64(seconds)
        .map_err(|_| anyhow::anyhow!("--interval-secs cannot be represented safely"))?;
    ensure!(
        !duration.is_zero(),
        "--interval-secs is too small for the system clock"
    );
    Ok(duration)
}

fn publish_once(source: &Path, output: &Path) -> Result<()> {
    let raw = read_bounded_regular(source, MAX_SOURCE_BYTES)
        .with_context(|| format!("read private backfill status {}", source.display()))?;
    let private: PrivateStatus = serde_json::from_slice(&raw)
        .with_context(|| format!("decode private backfill status {}", source.display()))?;
    let public = project(private)?;
    publish_atomic(output, &public)
}

fn project(private: PrivateStatus) -> Result<PublicStatus> {
    ensure!(
        private.schema_version == 1,
        "unsupported private status schema"
    );
    ensure!(
        STATES.contains(&private.state.as_str()),
        "unsupported backfill state"
    );
    ensure!(
        private.updated_unix_seconds >= private.started_unix_seconds,
        "updated timestamp precedes start timestamp"
    );
    ensure!(
        private.backfill.epochs_done <= private.backfill.epochs_total,
        "backfill epoch counter exceeds its total"
    );
    ensure!(
        private.backfill.overall_source_bytes_done <= private.backfill.source_bytes_total,
        "backfill byte counter exceeds its total"
    );
    ensure!(
        private.backfill.active_workers <= private.backfill.workers_configured,
        "active_workers exceeds workers_configured"
    );
    ensure!(
        private
            .backfill
            .wall_throughput_bytes_per_second
            .is_finite()
            && private.backfill.wall_throughput_bytes_per_second >= 0.0,
        "wall_throughput_bytes_per_second must be non-negative and finite"
    );

    Ok(PublicStatus {
        schema_version: 1,
        state: private.state,
        started_unix_secs: private.started_unix_seconds,
        updated_unix_secs: private.updated_unix_seconds,
        backfill: PublicBackfill {
            epochs_done: private.backfill.epochs_done,
            epochs_total: private.backfill.epochs_total,
            workers_configured: private.backfill.workers_configured,
            active_workers: private.backfill.active_workers,
            source_bytes_done: private.backfill.overall_source_bytes_done,
            source_bytes_total: private.backfill.source_bytes_total,
            throughput_bytes_per_sec: private.backfill.wall_throughput_bytes_per_second,
            eta_secs: private.backfill.eta_seconds,
            eta_reliable: private.backfill.eta_reliable,
        },
        current: PublicCurrent {
            epoch: private.current.epoch,
            progress_state: safe_progress_state(private.current.progress_state.as_deref()),
        },
        paused_secs: private.resources.paused_seconds,
        last_error: safe_error(private.last_error.as_deref()),
    })
}

fn safe_progress_state(value: Option<&str>) -> Option<String> {
    let value = value?;
    if value.chars().count() <= MAX_PROGRESS_CHARS && PROGRESS_STATES.contains(&value) {
        Some(value.to_string())
    } else {
        // A private producer may include a command, path, or diagnostic here.
        // Preserve the fact that progress exists without copying its contents.
        Some("unknown".to_string())
    }
}

fn safe_error(value: Option<&str>) -> Option<String> {
    let value = value?;
    static SAFE_ERROR: OnceLock<Regex> = OnceLock::new();
    let pattern = SAFE_ERROR.get_or_init(|| {
        Regex::new(r"\Aepoch [0-9]+ extractor exited [0-9]+\z").expect("valid safe error regex")
    });
    Some(if pattern.is_match(value) {
        value.to_string()
    } else {
        "Extractor failed".to_string()
    })
}

fn parent_or_dot(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn read_bounded_regular(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect {}", path.display()))?;
    ensure!(metadata.is_file(), "source is not a regular file");
    ensure!(
        metadata.len() > 0 && metadata.len() <= limit,
        "private status size is outside the allowed range"
    );
    let expected = metadata.len() as usize;
    let mut raw = Vec::with_capacity(expected);
    file.take(limit + 1)
        .read_to_end(&mut raw)
        .with_context(|| format!("read {}", path.display()))?;
    ensure!(
        !raw.is_empty() && raw.len() <= limit as usize,
        "private status size is outside the allowed range"
    );
    Ok(raw)
}

struct PublisherLock {
    #[allow(dead_code)]
    file: File,
}

impl PublisherLock {
    #[cfg(unix)]
    fn acquire(path: &Path) -> Result<Self> {
        let mut options = OpenOptions::new();
        options
            .create(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
        let file = options
            .open(path)
            .with_context(|| format!("open backfill publisher lock {}", path.display()))?;
        ensure!(
            file.metadata()?.is_file(),
            "publisher lock is not a regular file"
        );
        // SAFETY: `file` owns this descriptor until the lock object is dropped.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK)
            {
                bail!("block-time-gap status publisher is already running");
            }
            return Err(error).context("lock block-time-gap status publisher");
        }
        Ok(Self { file })
    }

    #[cfg(not(unix))]
    fn acquire(_path: &Path) -> Result<Self> {
        bail!("backfill publisher locking requires Unix")
    }
}

fn publish_atomic(path: &Path, value: &PublicStatus) -> Result<()> {
    let parent = parent_or_dot(path);
    fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("backfill output needs a UTF-8 file name")?;
    let mut encoded = serde_json::to_vec(value).context("encode public backfill status")?;
    encoded.push(b'\n');
    ensure!(
        encoded.len() <= MAX_PUBLIC_BYTES,
        "public backfill status exceeds its size limit"
    );
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!(".{name}.{}.{}.tmp", std::process::id(), nonce));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options
            .mode(0o644)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let mut file = options
            .open(&temporary)
            .with_context(|| format!("create {}", temporary.display()))?;
        file.write_all(&encoded)?;
        file.flush()?;
        file.sync_all()?;
        #[cfg(unix)]
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o644))?;
        fs::rename(&temporary, path)
            .with_context(|| format!("publish public backfill status {}", path.display()))?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};

    fn root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "blockzilla-backfill-status-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn private_status() -> Value {
        json!({
            "schema_version": 1,
            "state": "running",
            "started_unix_seconds": 1_000,
            "updated_unix_seconds": 1_100,
            "backfill": {
                "epochs_done": 265,
                "epochs_total": 526,
                "workers_configured": 2,
                "active_workers": 2,
                "overall_source_bytes_done": 2_500,
                "source_bytes_total": 10_000,
                "wall_throughput_bytes_per_second": 120.5,
                "eta_seconds": 63,
                "eta_reliable": true,
                "private_path": "/volume1/private"
            },
            "current": {"epoch": 289, "progress_state": "running"},
            "resources": {"paused_seconds": 182, "secret": "TOPSECRET"},
            "last_error": null,
            "command": "curl -H Authorization:Bearer SECRET"
        })
    }

    fn projection(value: Value) -> Result<PublicStatus> {
        project(serde_json::from_value(value)?)
    }

    #[test]
    fn projects_python_contract_without_private_fields() {
        let public = projection(private_status()).unwrap();
        assert_eq!(public.state, "running");
        assert_eq!(public.backfill.source_bytes_done, 2_500);
        assert_eq!(public.current.progress_state.as_deref(), Some("running"));
        let encoded = serde_json::to_string(&public).unwrap();
        assert!(!encoded.contains("TOPSECRET"));
        assert!(!encoded.contains("/volume1"));
        assert!(!encoded.contains("Authorization"));
    }

    #[test]
    fn preserves_all_supported_states_and_safe_errors() {
        for state in STATES {
            let mut value = private_status();
            value["state"] = json!(state);
            value["last_error"] = json!("epoch 761 extractor exited 17");
            let public = projection(value).unwrap();
            assert_eq!(public.state, *state);
            assert_eq!(
                public.last_error.as_deref(),
                Some("epoch 761 extractor exited 17")
            );
        }
    }

    #[test]
    fn redacts_free_form_error_and_progress_text() {
        let mut value = private_status();
        value["last_error"] = json!("token=SECRET at /volume1/private");
        value["current"]["progress_state"] = json!("reading /volume1/private?token=SECRET");
        let public = projection(value).unwrap();
        assert_eq!(public.last_error.as_deref(), Some("Extractor failed"));
        assert_eq!(public.current.progress_state.as_deref(), Some("unknown"));
    }

    #[test]
    fn rejects_cross_field_and_timestamp_contradictions() {
        for (pointer, replacement, message) in [
            ("/backfill/active_workers", json!(3), "active_workers"),
            ("/backfill/epochs_done", json!(527), "epoch counter"),
            (
                "/backfill/overall_source_bytes_done",
                json!(10_001),
                "byte counter",
            ),
            ("/updated_unix_seconds", json!(999), "timestamp"),
        ] {
            let mut value = private_status();
            *value.pointer_mut(pointer).unwrap() = replacement;
            assert!(projection(value).unwrap_err().to_string().contains(message));
        }
    }

    #[test]
    fn typed_decode_rejects_negative_float_boolean_and_duplicate_counters() {
        for raw in [
            br#"{"schema_version":1,"state":"running","started_unix_seconds":0,"updated_unix_seconds":1,"backfill":{"epochs_done":-1,"epochs_total":1,"workers_configured":1,"active_workers":1,"overall_source_bytes_done":0,"source_bytes_total":1,"wall_throughput_bytes_per_second":0,"eta_seconds":null,"eta_reliable":true},"current":{"epoch":null,"progress_state":null},"resources":{"paused_seconds":0},"last_error":null}"#.as_slice(),
            br#"{"schema_version":1,"state":"running","started_unix_seconds":false,"updated_unix_seconds":1,"backfill":{"epochs_done":0,"epochs_total":1,"workers_configured":1,"active_workers":1,"overall_source_bytes_done":0,"source_bytes_total":1,"wall_throughput_bytes_per_second":0,"eta_seconds":null,"eta_reliable":true},"current":{"epoch":null,"progress_state":null},"resources":{"paused_seconds":0},"last_error":null}"#.as_slice(),
            br#"{"schema_version":1,"state":"running","started_unix_seconds":0,"started_unix_seconds":1,"updated_unix_seconds":1,"backfill":{"epochs_done":0,"epochs_total":1,"workers_configured":1,"active_workers":1,"overall_source_bytes_done":0,"source_bytes_total":1,"wall_throughput_bytes_per_second":0,"eta_seconds":null,"eta_reliable":true},"current":{"epoch":null,"progress_state":null},"resources":{"paused_seconds":0},"last_error":null}"#.as_slice(),
        ] {
            assert!(serde_json::from_slice::<PrivateStatus>(raw).is_err());
        }
    }

    #[test]
    fn atomic_publication_is_newline_terminated_and_public_readable() {
        let root = root("atomic");
        let source = root.join("private.json");
        let output = root.join("public/status.json");
        fs::create_dir_all(output.parent().unwrap()).unwrap();
        fs::write(&source, serde_json::to_vec(&private_status()).unwrap()).unwrap();
        publish_once(&source, &output).unwrap();
        let bytes = fs::read(&output).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        let public: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(public["backfill"]["active_workers"], 2);
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o644
        );
        assert!(
            fs::read_dir(output.parent().unwrap())
                .unwrap()
                .all(|entry| {
                    !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".tmp")
                })
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_projection_does_not_replace_last_good_output() {
        let root = root("fail-closed");
        let source = root.join("private.json");
        let output = root.join("status.json");
        fs::write(&source, serde_json::to_vec(&private_status()).unwrap()).unwrap();
        publish_once(&source, &output).unwrap();
        let good = fs::read(&output).unwrap();
        fs::write(&source, br#"{"schema_version":2}"#).unwrap();
        assert!(publish_once(&source, &output).is_err());
        assert_eq!(fs::read(&output).unwrap(), good);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn source_size_is_strictly_bounded() {
        let root = root("bounds");
        let source = root.join("private.json");
        fs::write(&source, vec![b'x'; MAX_SOURCE_BYTES as usize + 1]).unwrap();
        assert!(read_bounded_regular(&source, MAX_SOURCE_BYTES).is_err());
        fs::write(&source, b"").unwrap();
        assert!(read_bounded_regular(&source, MAX_SOURCE_BYTES).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interval_conversion_never_panics_or_busy_spins() {
        assert_eq!(interval_duration(15.0).unwrap(), Duration::from_secs(15));
        assert!(interval_duration(f64::MAX).is_err());
        assert!(interval_duration(f64::MIN_POSITIVE).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn source_symlinks_and_non_regular_files_are_rejected_without_blocking() {
        let root = root("nofollow");
        let real = root.join("real.json");
        let link = root.join("link.json");
        fs::write(&real, b"{}").unwrap();
        symlink(&real, &link).unwrap();
        assert!(read_bounded_regular(&link, MAX_SOURCE_BYTES).is_err());
        assert!(read_bounded_regular(&root, MAX_SOURCE_BYTES).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn lock_is_exclusive_and_does_not_follow_symlinks() {
        let root = root("lock");
        let lock_path = root.join("publisher.lock");
        let held = PublisherLock::acquire(&lock_path).unwrap();
        assert!(PublisherLock::acquire(&lock_path).is_err());
        drop(held);
        fs::remove_file(&lock_path).unwrap();
        let target = root.join("target");
        fs::write(&target, b"untouched").unwrap();
        symlink(&target, &lock_path).unwrap();
        assert!(PublisherLock::acquire(&lock_path).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"untouched");
        fs::remove_dir_all(root).unwrap();
    }
}
