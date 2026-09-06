//! Portable child-process supervision for long-lived Hivezilla source instances.
//!
//! This deliberately does not pretend to preserve an outgoing network connection across an
//! executable replacement. Durable source WALs provide that continuity. The supervisor provides
//! restart policy, crash-loop fencing, readiness/heartbeat notifications, graceful termination,
//! and secret-free status snapshots without depending on systemd or a container runtime.

use std::{
    collections::VecDeque,
    env,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    process::{Child, Command},
    sync::oneshot,
    time::{sleep, timeout},
};

const STATUS_SCHEMA_VERSION: u32 = 1;
const NOTIFICATION_SCHEMA_VERSION: u32 = 1;
const STATUS_FILE: &str = "status.json";
const NOTIFICATION_FILE: &str = "notify.json";
const LOCK_FILE: &str = "supervisor.lock";
const NOTIFY_FILE_ENV: &str = "BLOCKZILLA_SUPERVISOR_NOTIFY_FILE";
const NOTIFY_TOKEN_ENV: &str = "BLOCKZILLA_SUPERVISOR_NOTIFY_TOKEN";
const NOTIFY_MAX_BYTES: u64 = 64 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    Never,
    OnFailure,
    Always,
}

impl RestartPolicy {
    fn should_restart(self, success: bool) -> bool {
        match self {
            Self::Never => false,
            Self::OnFailure => !success,
            Self::Always => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffPolicy {
    pub initial: Duration,
    pub maximum: Duration,
    /// Integer fixed point: 2000 means 2.0x.
    pub factor_milli: u32,
}

impl BackoffPolicy {
    fn validate(self) -> Result<Self> {
        ensure!(!self.initial.is_zero(), "initial backoff must be non-zero");
        ensure!(
            self.maximum >= self.initial,
            "maximum backoff must be at least the initial backoff"
        );
        ensure!(
            (1_000..=10_000).contains(&self.factor_milli),
            "backoff factor must be between 1000 and 10000"
        );
        Ok(self)
    }

    pub fn delay(self, consecutive_failure: u32) -> Duration {
        let mut millis = self.initial.as_millis();
        let maximum = self.maximum.as_millis();
        for _ in 0..consecutive_failure {
            millis = millis
                .saturating_mul(self.factor_milli as u128)
                .saturating_div(1_000)
                .min(maximum);
        }
        Duration::from_millis(millis.min(u64::MAX as u128) as u64)
    }
}

#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub name: String,
    pub state_dir: PathBuf,
    pub program: OsString,
    pub args: Vec<OsString>,
    pub working_dir: Option<PathBuf>,
    pub restart_policy: RestartPolicy,
    pub backoff: BackoffPolicy,
    pub restart_burst: usize,
    pub restart_window: Duration,
    pub healthy_after: Duration,
    /// Zero disables explicit readiness notification.
    pub readiness_timeout: Duration,
    /// Zero disables heartbeat enforcement. Requires readiness notification when non-zero.
    pub heartbeat_timeout: Duration,
    pub stop_timeout: Duration,
}

impl SupervisorConfig {
    fn validate(self) -> Result<Self> {
        validate_name(&self.name)?;
        ensure!(
            !self.program.is_empty(),
            "supervised program must not be empty"
        );
        self.backoff.validate()?;
        ensure!(self.restart_burst > 0, "restart burst must be non-zero");
        ensure!(
            !self.restart_window.is_zero(),
            "restart window must be non-zero"
        );
        ensure!(
            !self.healthy_after.is_zero(),
            "healthy-after must be non-zero"
        );
        ensure!(
            !self.stop_timeout.is_zero(),
            "stop timeout must be non-zero"
        );
        ensure!(
            self.heartbeat_timeout.is_zero() || !self.readiness_timeout.is_zero(),
            "heartbeat enforcement requires readiness notification"
        );
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorState {
    Starting,
    Running,
    Backoff,
    Stopping,
    Exited,
    CrashLoop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorStatus {
    pub schema_version: u32,
    pub name: String,
    pub state: SupervisorState,
    pub supervisor_pid: u32,
    pub child_pid: Option<u32>,
    pub attempt: u64,
    pub restarts: u64,
    pub consecutive_failures: u32,
    pub ready: bool,
    pub last_event: Option<String>,
    pub last_exit_code: Option<i32>,
    pub next_restart_unix_ms: Option<u64>,
    pub updated_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorReport {
    pub name: String,
    pub final_state: SupervisorState,
    pub attempts: u64,
    pub restarts: u64,
    pub last_exit_code: Option<i32>,
    pub successful: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SupervisorNotification {
    schema_version: u32,
    token: String,
    ready: bool,
    heartbeat_sequence: u64,
    updated_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorNotificationKind {
    Ready,
    Heartbeat,
}

#[derive(Debug)]
enum ChildOutcome {
    Exited(ExitStatus),
    ReadinessTimeout,
    HeartbeatTimeout,
    Shutdown(Option<i32>),
}

impl ChildOutcome {
    fn exit_code(&self) -> Option<i32> {
        match self {
            Self::Exited(status) => status.code(),
            Self::Shutdown(code) => *code,
            Self::ReadinessTimeout | Self::HeartbeatTimeout => None,
        }
    }

    fn event(&self) -> &'static str {
        match self {
            Self::Exited(status) if status.success() => "child_exited_successfully",
            Self::Exited(_) => "child_exited_with_failure",
            Self::ReadinessTimeout => "readiness_timeout",
            Self::HeartbeatTimeout => "heartbeat_timeout",
            Self::Shutdown(_) => "supervisor_shutdown",
        }
    }

    fn success(&self) -> bool {
        matches!(self, Self::Exited(status) if status.success())
    }
}

pub async fn run_supervisor(config: SupervisorConfig) -> Result<SupervisorReport> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let signal_task = tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        let _ = shutdown_tx.send(());
    });
    let report = run_supervisor_with_shutdown(config, shutdown_rx).await;
    signal_task.abort();
    report
}

async fn run_supervisor_with_shutdown(
    config: SupervisorConfig,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<SupervisorReport> {
    let config = config.validate()?;
    prepare_state_dir(&config.state_dir)?;
    let _lock = acquire_lock(&config.state_dir)?;
    let status_path = config.state_dir.join(STATUS_FILE);
    let notification_path = config.state_dir.join(NOTIFICATION_FILE);

    let mut attempt = 0u64;
    let mut restarts = 0u64;
    let mut consecutive_failures = 0u32;
    let mut restart_history = VecDeque::new();
    let mut last_exit_code = None;

    loop {
        attempt = attempt
            .checked_add(1)
            .context("supervisor attempt overflow")?;
        remove_notification(&notification_path)?;
        let token = notification_token(&config, attempt);
        let mut child = match spawn_child(&config, &notification_path, &token) {
            Ok(child) => child,
            Err(error) => {
                tracing::error!(
                    service = %config.name,
                    error = %error,
                    "failed to spawn supervised child"
                );
                if config.restart_policy == RestartPolicy::Never {
                    publish_status(
                        &status_path,
                        status_for(
                            &config,
                            SupervisorState::Exited,
                            None,
                            attempt,
                            restarts,
                            consecutive_failures,
                            false,
                            Some("spawn_failed"),
                            None,
                            None,
                        ),
                    )?;
                    return Ok(SupervisorReport {
                        name: config.name,
                        final_state: SupervisorState::Exited,
                        attempts: attempt,
                        restarts,
                        last_exit_code: None,
                        successful: false,
                    });
                }

                let now = Instant::now();
                while restart_history
                    .front()
                    .is_some_and(|instant| now.duration_since(*instant) >= config.restart_window)
                {
                    restart_history.pop_front();
                }
                if restart_history.len() >= config.restart_burst {
                    publish_status(
                        &status_path,
                        status_for(
                            &config,
                            SupervisorState::CrashLoop,
                            None,
                            attempt,
                            restarts,
                            consecutive_failures,
                            false,
                            Some("restart_burst_exhausted_after_spawn_failure"),
                            None,
                            None,
                        ),
                    )?;
                    return Ok(SupervisorReport {
                        name: config.name,
                        final_state: SupervisorState::CrashLoop,
                        attempts: attempt,
                        restarts,
                        last_exit_code: None,
                        successful: false,
                    });
                }
                restart_history.push_back(now);
                consecutive_failures = consecutive_failures.saturating_add(1);
                let delay = config.backoff.delay(consecutive_failures.saturating_sub(1));
                restarts = restarts
                    .checked_add(1)
                    .context("supervisor restart overflow")?;
                publish_status(
                    &status_path,
                    status_for(
                        &config,
                        SupervisorState::Backoff,
                        None,
                        attempt,
                        restarts,
                        consecutive_failures,
                        false,
                        Some("spawn_failed"),
                        None,
                        Some(unix_time_ms().saturating_add(duration_millis(delay))),
                    ),
                )?;
                tokio::select! {
                    _ = sleep(delay) => continue,
                    _ = &mut shutdown => {
                        publish_status(
                            &status_path,
                            status_for(
                                &config,
                                SupervisorState::Exited,
                                None,
                                attempt,
                                restarts,
                                consecutive_failures,
                                false,
                                Some("shutdown_during_spawn_backoff"),
                                None,
                                None,
                            ),
                        )?;
                        return Ok(SupervisorReport {
                            name: config.name,
                            final_state: SupervisorState::Exited,
                            attempts: attempt,
                            restarts,
                            last_exit_code: None,
                            successful: true,
                        });
                    }
                }
            }
        };
        let child_pid = child.id();
        let started_at = Instant::now();
        let mut ready = config.readiness_timeout.is_zero();
        let mut heartbeat_sequence = 0u64;
        let mut heartbeat_seen_at = Instant::now();

        publish_status(
            &status_path,
            status_for(
                &config,
                SupervisorState::Starting,
                child_pid,
                attempt,
                restarts,
                consecutive_failures,
                ready,
                Some("child_spawned"),
                last_exit_code,
                None,
            ),
        )?;

        if ready {
            publish_status(
                &status_path,
                status_for(
                    &config,
                    SupervisorState::Running,
                    child_pid,
                    attempt,
                    restarts,
                    consecutive_failures,
                    true,
                    Some("readiness_not_required"),
                    last_exit_code,
                    None,
                ),
            )?;
        }

        let outcome = loop {
            tokio::select! {
                status = child.wait() => {
                    break ChildOutcome::Exited(status.context("wait for supervised child")?);
                }
                _ = &mut shutdown => {
                    publish_status(
                        &status_path,
                        status_for(
                            &config,
                            SupervisorState::Stopping,
                            child.id(),
                            attempt,
                            restarts,
                            consecutive_failures,
                            ready,
                            Some("shutdown_requested"),
                            last_exit_code,
                            None,
                        ),
                    )?;
                    let status = terminate_child(&mut child, config.stop_timeout).await?;
                    break ChildOutcome::Shutdown(status.and_then(|status| status.code()));
                }
                _ = sleep(POLL_INTERVAL) => {
                    if !config.readiness_timeout.is_zero() {
                        if let Some(notification) = read_notification(&notification_path, &token)? {
                            if notification.ready && !ready {
                                ready = true;
                                heartbeat_sequence = notification.heartbeat_sequence;
                                heartbeat_seen_at = Instant::now();
                                publish_status(
                                    &status_path,
                                    status_for(
                                        &config,
                                        SupervisorState::Running,
                                        child.id(),
                                        attempt,
                                        restarts,
                                        consecutive_failures,
                                        true,
                                        Some("child_ready"),
                                        last_exit_code,
                                        None,
                                    ),
                                )?;
                            } else if ready && notification.heartbeat_sequence > heartbeat_sequence {
                                heartbeat_sequence = notification.heartbeat_sequence;
                                heartbeat_seen_at = Instant::now();
                            }
                        }
                        if !ready && started_at.elapsed() >= config.readiness_timeout {
                            let _ = terminate_child(&mut child, config.stop_timeout).await?;
                            break ChildOutcome::ReadinessTimeout;
                        }
                        if ready
                            && !config.heartbeat_timeout.is_zero()
                            && heartbeat_seen_at.elapsed() >= config.heartbeat_timeout
                        {
                            let _ = terminate_child(&mut child, config.stop_timeout).await?;
                            break ChildOutcome::HeartbeatTimeout;
                        }
                    }
                }
            }
        };

        last_exit_code = outcome.exit_code();
        if matches!(outcome, ChildOutcome::Shutdown(_)) {
            publish_status(
                &status_path,
                status_for(
                    &config,
                    SupervisorState::Exited,
                    None,
                    attempt,
                    restarts,
                    consecutive_failures,
                    false,
                    Some(outcome.event()),
                    last_exit_code,
                    None,
                ),
            )?;
            return Ok(SupervisorReport {
                name: config.name,
                final_state: SupervisorState::Exited,
                attempts: attempt,
                restarts,
                last_exit_code,
                successful: true,
            });
        }

        if started_at.elapsed() >= config.healthy_after {
            consecutive_failures = 0;
            restart_history.clear();
        }
        let child_success = outcome.success();
        if !config.restart_policy.should_restart(child_success) {
            publish_status(
                &status_path,
                status_for(
                    &config,
                    SupervisorState::Exited,
                    None,
                    attempt,
                    restarts,
                    consecutive_failures,
                    false,
                    Some(outcome.event()),
                    last_exit_code,
                    None,
                ),
            )?;
            return Ok(SupervisorReport {
                name: config.name,
                final_state: SupervisorState::Exited,
                attempts: attempt,
                restarts,
                last_exit_code,
                successful: child_success,
            });
        }

        let now = Instant::now();
        while restart_history
            .front()
            .is_some_and(|instant| now.duration_since(*instant) >= config.restart_window)
        {
            restart_history.pop_front();
        }
        if restart_history.len() >= config.restart_burst {
            publish_status(
                &status_path,
                status_for(
                    &config,
                    SupervisorState::CrashLoop,
                    None,
                    attempt,
                    restarts,
                    consecutive_failures,
                    false,
                    Some("restart_burst_exhausted"),
                    last_exit_code,
                    None,
                ),
            )?;
            return Ok(SupervisorReport {
                name: config.name,
                final_state: SupervisorState::CrashLoop,
                attempts: attempt,
                restarts,
                last_exit_code,
                successful: false,
            });
        }
        restart_history.push_back(now);
        consecutive_failures = consecutive_failures.saturating_add(1);
        let delay = config.backoff.delay(consecutive_failures.saturating_sub(1));
        restarts = restarts
            .checked_add(1)
            .context("supervisor restart overflow")?;
        let next_restart = unix_time_ms().saturating_add(duration_millis(delay));
        publish_status(
            &status_path,
            status_for(
                &config,
                SupervisorState::Backoff,
                None,
                attempt,
                restarts,
                consecutive_failures,
                false,
                Some(outcome.event()),
                last_exit_code,
                Some(next_restart),
            ),
        )?;

        tokio::select! {
            _ = sleep(delay) => {}
            _ = &mut shutdown => {
                publish_status(
                    &status_path,
                    status_for(
                        &config,
                        SupervisorState::Exited,
                        None,
                        attempt,
                        restarts,
                        consecutive_failures,
                        false,
                        Some("shutdown_during_backoff"),
                        last_exit_code,
                        None,
                    ),
                )?;
                return Ok(SupervisorReport {
                    name: config.name,
                    final_state: SupervisorState::Exited,
                    attempts: attempt,
                    restarts,
                    last_exit_code,
                    successful: true,
                });
            }
        }
    }
}

pub fn notify_supervisor(kind: SupervisorNotificationKind) -> Result<()> {
    let path = PathBuf::from(
        env::var_os(NOTIFY_FILE_ENV)
            .context("supervisor notification environment is not configured")?,
    );
    let token = env::var(NOTIFY_TOKEN_ENV)
        .context("supervisor notification token environment is not configured")?;
    let notification = match kind {
        SupervisorNotificationKind::Ready => match existing_notification(&path)? {
            Some(existing) => {
                ensure!(
                    existing.token == token,
                    "supervisor notification token changed"
                );
                SupervisorNotification {
                    ready: true,
                    heartbeat_sequence: existing
                        .heartbeat_sequence
                        .checked_add(1)
                        .context("supervisor heartbeat sequence overflow")?,
                    updated_unix_ms: unix_time_ms(),
                    ..existing
                }
            }
            None => SupervisorNotification {
                schema_version: NOTIFICATION_SCHEMA_VERSION,
                token,
                ready: true,
                heartbeat_sequence: 1,
                updated_unix_ms: unix_time_ms(),
            },
        },
        SupervisorNotificationKind::Heartbeat => {
            let existing = read_notification_exact(&path)?;
            ensure!(
                existing.token == token,
                "supervisor notification token changed"
            );
            ensure!(existing.ready, "cannot heartbeat before readiness");
            SupervisorNotification {
                heartbeat_sequence: existing
                    .heartbeat_sequence
                    .checked_add(1)
                    .context("supervisor heartbeat sequence overflow")?,
                updated_unix_ms: unix_time_ms(),
                ..existing
            }
        }
    };
    write_json_atomic(&path, &notification)
}

fn existing_notification(path: &Path) -> Result<Option<SupervisorNotification>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "supervisor notification path is not a regular file"
            );
            Ok(Some(read_notification_exact(path)?))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error)
            .with_context(|| format!("inspect supervisor notification {}", path.display())),
    }
}

fn spawn_child(config: &SupervisorConfig, notification_path: &Path, token: &str) -> Result<Child> {
    let mut command = Command::new(&config.program);
    command
        .args(&config.args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env(NOTIFY_FILE_ENV, notification_path)
        .env(NOTIFY_TOKEN_ENV, token);
    if let Some(working_dir) = config.working_dir.as_ref() {
        command.current_dir(working_dir);
    }
    #[cfg(unix)]
    command.process_group(0);
    command.spawn().context("spawn child process")
}

async fn terminate_child(child: &mut Child, grace: Duration) -> Result<Option<ExitStatus>> {
    if let Some(status) = child.try_wait().context("poll child before shutdown")? {
        return Ok(Some(status));
    }

    #[cfg(unix)]
    {
        if let Some(pid) = child.id() {
            let result = unsafe { libc::kill(-(pid as i32), libc::SIGTERM) };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error).context("send SIGTERM to supervised child");
                }
            }
        }
    }
    #[cfg(not(unix))]
    child.start_kill().context("terminate supervised child")?;

    match timeout(grace, child.wait()).await {
        Ok(status) => Ok(Some(status.context("wait for supervised child shutdown")?)),
        Err(_) => {
            #[cfg(unix)]
            if let Some(pid) = child.id() {
                let result = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
                if result != 0 {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(error).context("kill supervised process group after timeout");
                    }
                }
            }
            #[cfg(not(unix))]
            child
                .start_kill()
                .context("kill supervised child after timeout")?;
            Ok(Some(
                child.wait().await.context("reap killed supervised child")?,
            ))
        }
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn prepare_state_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("create supervisor state directory {}", path.display()))?;
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect supervisor state directory {}", path.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "supervisor state path is not a real directory"
    );
    Ok(())
}

fn acquire_lock(state_dir: &Path) -> Result<File> {
    let path = state_dir.join(LOCK_FILE);
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("open supervisor lock {}", path.display()))?;
    lock.try_lock()
        .with_context(|| format!("lock supervisor state {}", state_dir.display()))?;
    Ok(lock)
}

fn remove_notification(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "supervisor notification path is not a regular file"
            );
            fs::remove_file(path)
                .with_context(|| format!("remove stale supervisor notification {}", path.display()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("inspect supervisor notification {}", path.display())),
    }
}

fn read_notification(path: &Path, token: &str) -> Result<Option<SupervisorNotification>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "supervisor notification path is not a regular file"
            );
            ensure!(
                metadata.len() <= NOTIFY_MAX_BYTES,
                "supervisor notification exceeds maximum size"
            );
            let notification = read_notification_exact(path)?;
            if notification.token != token {
                return Ok(None);
            }
            Ok(Some(notification))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error)
            .with_context(|| format!("inspect supervisor notification {}", path.display())),
    }
}

fn read_notification_exact(path: &Path) -> Result<SupervisorNotification> {
    let bytes = fs::read(path)
        .with_context(|| format!("read supervisor notification {}", path.display()))?;
    ensure!(
        bytes.len() as u64 <= NOTIFY_MAX_BYTES,
        "supervisor notification exceeds maximum size"
    );
    let notification: SupervisorNotification = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode supervisor notification {}", path.display()))?;
    ensure!(
        notification.schema_version == NOTIFICATION_SCHEMA_VERSION,
        "unsupported supervisor notification schema"
    );
    Ok(notification)
}

fn publish_status(path: &Path, status: SupervisorStatus) -> Result<()> {
    write_json_atomic(path, &status)
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .context("supervisor state file has no parent")?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("supervisor state file has invalid name")?;
    let temporary = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .with_context(|| format!("create supervisor state temp {}", temporary.display()))?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "publish supervisor state {} from {}",
                path.display(),
                temporary.display()
            )
        })?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn status_for(
    config: &SupervisorConfig,
    state: SupervisorState,
    child_pid: Option<u32>,
    attempt: u64,
    restarts: u64,
    consecutive_failures: u32,
    ready: bool,
    event: Option<&str>,
    last_exit_code: Option<i32>,
    next_restart_unix_ms: Option<u64>,
) -> SupervisorStatus {
    SupervisorStatus {
        schema_version: STATUS_SCHEMA_VERSION,
        name: config.name.clone(),
        state,
        supervisor_pid: std::process::id(),
        child_pid,
        attempt,
        restarts,
        consecutive_failures,
        ready,
        last_event: event.map(str::to_owned),
        last_exit_code,
        next_restart_unix_ms,
        updated_unix_ms: unix_time_ms(),
    }
}

fn notification_token(config: &SupervisorConfig, attempt: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"BLOCKZILLA-SUPERVISOR-NOTIFY-v1");
    hasher.update(config.name.as_bytes());
    hasher.update(attempt.to_le_bytes());
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(unix_time_ms().to_le_bytes());
    let digest = hasher.finalize();
    digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn validate_name(name: &str) -> Result<()> {
    ensure!(!name.is_empty() && name.len() <= 64, "invalid service name");
    ensure!(
        name.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "service name contains unsupported characters"
    );
    Ok(())
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration_millis(duration))
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "hivezilla-supervisor-{name}-{}-{}",
            std::process::id(),
            unix_time_ms()
        ))
    }

    #[test]
    fn backoff_is_bounded_and_uses_integer_fixed_point() {
        let policy = BackoffPolicy {
            initial: Duration::from_millis(100),
            maximum: Duration::from_secs(1),
            factor_milli: 2_000,
        };
        assert_eq!(policy.delay(0), Duration::from_millis(100));
        assert_eq!(policy.delay(1), Duration::from_millis(200));
        assert_eq!(policy.delay(3), Duration::from_millis(800));
        assert_eq!(policy.delay(20), Duration::from_secs(1));
    }

    #[test]
    fn invalid_supervisor_limits_fail_before_state_creation() {
        let root = temp_dir("invalid");
        let config = SupervisorConfig {
            name: "bad/name".to_string(),
            state_dir: root.clone(),
            program: OsString::from("ignored"),
            args: Vec::new(),
            working_dir: None,
            restart_policy: RestartPolicy::OnFailure,
            backoff: BackoffPolicy {
                initial: Duration::from_millis(1),
                maximum: Duration::from_secs(1),
                factor_milli: 2_000,
            },
            restart_burst: 3,
            restart_window: Duration::from_secs(10),
            healthy_after: Duration::from_secs(5),
            readiness_timeout: Duration::ZERO,
            heartbeat_timeout: Duration::ZERO,
            stop_timeout: Duration::from_secs(1),
        };
        assert!(config.validate().is_err());
        assert!(!root.exists());
    }

    #[test]
    fn notification_rejects_wrong_attempt_token() {
        let root = temp_dir("wrong-token");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(NOTIFICATION_FILE);
        write_json_atomic(
            &path,
            &SupervisorNotification {
                schema_version: NOTIFICATION_SCHEMA_VERSION,
                token: "old".to_string(),
                ready: true,
                heartbeat_sequence: 1,
                updated_unix_ms: unix_time_ms(),
            },
        )
        .unwrap();
        assert!(read_notification(&path, "new").unwrap().is_none());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn status_snapshot_never_contains_the_child_command() {
        let root = temp_dir("status-redaction");
        fs::create_dir_all(&root).unwrap();
        let config = SupervisorConfig {
            name: "capture".to_string(),
            state_dir: root.clone(),
            program: OsString::from("secret-program"),
            args: vec![OsString::from("--token=secret")],
            working_dir: None,
            restart_policy: RestartPolicy::OnFailure,
            backoff: BackoffPolicy {
                initial: Duration::from_millis(1),
                maximum: Duration::from_secs(1),
                factor_milli: 2_000,
            },
            restart_burst: 3,
            restart_window: Duration::from_secs(10),
            healthy_after: Duration::from_secs(5),
            readiness_timeout: Duration::ZERO,
            heartbeat_timeout: Duration::ZERO,
            stop_timeout: Duration::from_secs(1),
        };
        let path = root.join(STATUS_FILE);
        publish_status(
            &path,
            status_for(
                &config,
                SupervisorState::Running,
                Some(42),
                1,
                0,
                0,
                true,
                Some("child_ready"),
                None,
                None,
            ),
        )
        .unwrap();
        let bytes = fs::read(&path).unwrap();
        assert!(
            !bytes
                .windows(b"secret".len())
                .any(|window| window == b"secret")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn state_lock_rejects_a_second_supervisor() {
        let root = temp_dir("lock");
        fs::create_dir_all(&root).unwrap();
        let first = acquire_lock(&root).unwrap();
        assert!(acquire_lock(&root).is_err());
        drop(first);
        drop(acquire_lock(&root).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn crashing_child_is_fenced_after_the_restart_burst() {
        let root = temp_dir("crash-loop");
        let config = SupervisorConfig {
            name: "crash-loop".to_string(),
            state_dir: root.clone(),
            program: OsString::from("/bin/sh"),
            args: vec![OsString::from("-c"), OsString::from("exit 7")],
            working_dir: None,
            restart_policy: RestartPolicy::OnFailure,
            backoff: BackoffPolicy {
                initial: Duration::from_millis(1),
                maximum: Duration::from_millis(2),
                factor_milli: 2_000,
            },
            restart_burst: 2,
            restart_window: Duration::from_secs(10),
            healthy_after: Duration::from_secs(60),
            readiness_timeout: Duration::ZERO,
            heartbeat_timeout: Duration::ZERO,
            stop_timeout: Duration::from_millis(100),
        };
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let report = run_supervisor_with_shutdown(config, shutdown_rx)
            .await
            .unwrap();
        assert_eq!(report.final_state, SupervisorState::CrashLoop);
        assert_eq!(report.attempts, 3);
        assert_eq!(report.restarts, 2);
        assert_eq!(report.last_exit_code, Some(7));
        assert!(!report.successful);

        let status: SupervisorStatus =
            serde_json::from_slice(&fs::read(root.join(STATUS_FILE)).unwrap()).unwrap();
        assert_eq!(status.state, SupervisorState::CrashLoop);
        assert_eq!(
            status.last_event.as_deref(),
            Some("restart_burst_exhausted")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn readiness_timeout_terminates_the_child_without_restart() {
        let root = temp_dir("readiness-timeout");
        let config = SupervisorConfig {
            name: "not-ready".to_string(),
            state_dir: root.clone(),
            program: OsString::from("/bin/sh"),
            args: vec![OsString::from("-c"), OsString::from("sleep 10")],
            working_dir: None,
            restart_policy: RestartPolicy::Never,
            backoff: BackoffPolicy {
                initial: Duration::from_millis(1),
                maximum: Duration::from_millis(2),
                factor_milli: 2_000,
            },
            restart_burst: 2,
            restart_window: Duration::from_secs(10),
            healthy_after: Duration::from_secs(60),
            readiness_timeout: Duration::from_millis(50),
            heartbeat_timeout: Duration::ZERO,
            stop_timeout: Duration::from_millis(100),
        };
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let report = run_supervisor_with_shutdown(config, shutdown_rx)
            .await
            .unwrap();
        assert_eq!(report.final_state, SupervisorState::Exited);
        assert_eq!(report.attempts, 1);
        assert!(!report.successful);

        let status: SupervisorStatus =
            serde_json::from_slice(&fs::read(root.join(STATUS_FILE)).unwrap()).unwrap();
        assert_eq!(status.last_event.as_deref(), Some("readiness_timeout"));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_is_forwarded_to_the_child_process_group() {
        let root = temp_dir("graceful-shutdown");
        let marker = root.join("term-received");
        let script = format!(
            "trap 'touch {}; exit 0' TERM; while :; do sleep 1; done",
            marker.display()
        );
        let config = SupervisorConfig {
            name: "graceful-shutdown".to_string(),
            state_dir: root.clone(),
            program: OsString::from("/bin/sh"),
            args: vec![OsString::from("-c"), OsString::from(script)],
            working_dir: None,
            restart_policy: RestartPolicy::Always,
            backoff: BackoffPolicy {
                initial: Duration::from_millis(1),
                maximum: Duration::from_millis(2),
                factor_milli: 2_000,
            },
            restart_burst: 2,
            restart_window: Duration::from_secs(10),
            healthy_after: Duration::from_secs(60),
            readiness_timeout: Duration::ZERO,
            heartbeat_timeout: Duration::ZERO,
            stop_timeout: Duration::from_secs(1),
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            let _ = shutdown_tx.send(());
        });

        let report = run_supervisor_with_shutdown(config, shutdown_rx)
            .await
            .unwrap();
        assert!(report.successful);
        assert_eq!(report.final_state, SupervisorState::Exited);
        assert_eq!(report.attempts, 1);
        assert!(marker.exists(), "child did not handle SIGTERM");

        let status: SupervisorStatus =
            serde_json::from_slice(&fs::read(root.join(STATUS_FILE)).unwrap()).unwrap();
        assert_eq!(status.last_event.as_deref(), Some("supervisor_shutdown"));
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn spawn_failure_retries_then_enters_crash_loop() {
        let root = temp_dir("spawn-failure");
        let config = SupervisorConfig {
            name: "missing-program".to_string(),
            state_dir: root.clone(),
            program: OsString::from(root.join("does-not-exist")),
            args: Vec::new(),
            working_dir: None,
            restart_policy: RestartPolicy::OnFailure,
            backoff: BackoffPolicy {
                initial: Duration::from_millis(1),
                maximum: Duration::from_millis(1),
                factor_milli: 1_000,
            },
            restart_burst: 1,
            restart_window: Duration::from_secs(10),
            healthy_after: Duration::from_secs(60),
            readiness_timeout: Duration::ZERO,
            heartbeat_timeout: Duration::ZERO,
            stop_timeout: Duration::from_millis(100),
        };
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let report = run_supervisor_with_shutdown(config, shutdown_rx)
            .await
            .unwrap();
        assert_eq!(report.final_state, SupervisorState::CrashLoop);
        assert_eq!(report.attempts, 2);
        assert_eq!(report.restarts, 1);
        assert!(!report.successful);
        fs::remove_dir_all(root).unwrap();
    }
}
