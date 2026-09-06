//! Durable, at-most-once Telegram alerting for stalled receiver acknowledgements.
//!
//! Delivery is intentionally modeled as delivered, definitely rejected, or ambiguous. Before a
//! network request, the monitor persists a transitional phase. An ambiguous result (including
//! cancellation during shutdown) leaves that phase in place; on restart it is interpreted as if
//! the notification was delivered. This favors a missed alert over duplicate incident spam.

use std::{
    env,
    ffi::{CString, OsStr, OsString},
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::future::Future;

use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use futures::StreamExt;
use reqwest::{Client, StatusCode, header::CONTENT_TYPE, redirect::Policy};
use serde::Deserialize;
use serde_json::Value;
use zeroize::Zeroizing;

pub const MAX_ACK_STATUS_BYTES: usize = 64 * 1024;
pub const MAX_STATE_BYTES: usize = 4 * 1024;
pub const MAX_TOKEN_BYTES: usize = 256;
pub const MAX_TELEGRAM_RESPONSE_BYTES: usize = 64 * 1024;

const DEFAULT_ACK_STATUS_FILE: &str = "/control/pull-ack-status.json";
const DEFAULT_STATE_FILE: &str = "/alert-state/pull-ack-alert.json";
const DEFAULT_TOKEN_FILE: &str = "/run/secrets/telegram_bot_token";
const DEFAULT_STALE_AFTER_SECS: u64 = 300;
const DEFAULT_STARTUP_GRACE_SECS: u64 = 300;
const DEFAULT_INTERVAL_SECS: u64 = 30;
const TELEGRAM_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const TELEGRAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_STALE_AFTER: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const MAX_STARTUP_GRACE: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const MAX_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

static TEMPORARY_ORDINAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Args)]
pub struct MonitorPullAckTelegramArgs {
    /// Atomically published durable receiver ACK status file.
    #[arg(long)]
    pub ack_status_file: Option<PathBuf>,

    /// Private durable alert-state file.
    #[arg(long)]
    pub state_file: Option<PathBuf>,

    /// File containing the Telegram bot token; the token is never logged.
    #[arg(long)]
    pub token_file: Option<PathBuf>,

    /// Numeric Telegram chat id or @channel name.
    #[arg(long, allow_hyphen_values = true)]
    pub chat_id: Option<String>,

    /// Optional positive Telegram forum-topic id.
    #[arg(long)]
    pub message_thread_id: Option<u64>,

    /// ACK age that opens an incident.
    #[arg(long)]
    pub stale_after_secs: Option<u64>,

    /// Delay before a newly started monitor may open an incident.
    #[arg(long)]
    pub startup_grace_secs: Option<u64>,

    /// Poll interval.
    #[arg(long)]
    pub interval_secs: Option<u64>,
}

#[derive(Debug, Clone)]
struct Config {
    ack_status_file: PathBuf,
    state_file: PathBuf,
    token_file: PathBuf,
    chat_id: String,
    message_thread_id: Option<u64>,
    stale_after: Duration,
    startup_grace: Duration,
    interval: Duration,
}

impl Config {
    fn from_args(args: MonitorPullAckTelegramArgs) -> Result<Self> {
        Self::from_args_with_env(args, &|name| env::var_os(name))
    }

    fn from_args_with_env(
        args: MonitorPullAckTelegramArgs,
        lookup: &dyn Fn(&str) -> Option<OsString>,
    ) -> Result<Self> {
        let config = Self {
            ack_status_file: path_arg_or_env(
                args.ack_status_file,
                "BLOCKZILLA_PULL_ACK_STATUS_FILE",
                DEFAULT_ACK_STATUS_FILE,
                lookup,
            )?,
            state_file: path_arg_or_env(
                args.state_file,
                "BLOCKZILLA_PULL_ACK_ALERT_STATE_FILE",
                DEFAULT_STATE_FILE,
                lookup,
            )?,
            token_file: path_arg_or_env(
                args.token_file,
                "BLOCKZILLA_TELEGRAM_BOT_TOKEN_FILE",
                DEFAULT_TOKEN_FILE,
                lookup,
            )?,
            chat_id: string_arg_or_env(args.chat_id, "BLOCKZILLA_TELEGRAM_CHAT_ID", "", lookup)?,
            message_thread_id: optional_positive_arg_or_env(
                args.message_thread_id,
                "BLOCKZILLA_TELEGRAM_MESSAGE_THREAD_ID",
                lookup,
            )?,
            stale_after: Duration::from_secs(
                positive_arg_or_env(
                    args.stale_after_secs,
                    "BLOCKZILLA_PULL_ACK_STALE_AFTER_SECS",
                    Some(DEFAULT_STALE_AFTER_SECS),
                    lookup,
                )?
                .expect("default is present"),
            ),
            startup_grace: Duration::from_secs(
                positive_arg_or_env(
                    args.startup_grace_secs,
                    "BLOCKZILLA_PULL_ACK_STARTUP_GRACE_SECS",
                    Some(DEFAULT_STARTUP_GRACE_SECS),
                    lookup,
                )?
                .expect("default is present"),
            ),
            interval: Duration::from_secs(
                positive_arg_or_env(
                    args.interval_secs,
                    "BLOCKZILLA_PULL_ACK_MONITOR_INTERVAL_SECS",
                    Some(DEFAULT_INTERVAL_SECS),
                    lookup,
                )?
                .expect("default is present"),
            ),
        };
        config.validate()?;
        // Validate the secret at startup while still reading it afresh for token rotation.
        let _ = read_token(&config.token_file).context("validate Telegram bot-token file")?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        validate_absolute_non_root(&self.ack_status_file, "ACK status")?;
        validate_absolute_non_root(&self.state_file, "alert state")?;
        validate_absolute_non_root(&self.token_file, "Telegram token")?;
        ensure!(
            valid_chat_id(&self.chat_id),
            "Telegram chat id has an invalid shape"
        );
        ensure!(
            self.message_thread_id.is_none_or(|value| value > 0),
            "Telegram message thread id must be positive"
        );
        ensure!(
            !self.stale_after.is_zero() && self.stale_after <= MAX_STALE_AFTER,
            "ACK stale threshold must be positive and at most one year"
        );
        ensure!(
            !self.startup_grace.is_zero() && self.startup_grace <= MAX_STARTUP_GRACE,
            "startup grace must be positive and at most one year"
        );
        ensure!(
            !self.interval.is_zero() && self.interval <= MAX_INTERVAL,
            "monitor interval must be positive and at most one day"
        );
        ensure!(
            self.state_file != self.ack_status_file && self.state_file != self.token_file,
            "alert state must not overwrite an input file"
        );
        Ok(())
    }
}

fn path_arg_or_env(
    value: Option<PathBuf>,
    name: &str,
    default: &str,
    lookup: &dyn Fn(&str) -> Option<OsString>,
) -> Result<PathBuf> {
    if let Some(value) = value {
        return Ok(value);
    }
    match lookup(name) {
        Some(value) => {
            ensure!(!value.is_empty(), "{name} must not be empty");
            Ok(PathBuf::from(value))
        }
        None => Ok(PathBuf::from(default)),
    }
}

fn string_arg_or_env(
    value: Option<String>,
    name: &str,
    default: &str,
    lookup: &dyn Fn(&str) -> Option<OsString>,
) -> Result<String> {
    if let Some(value) = value {
        return Ok(value);
    }
    match lookup(name) {
        Some(value) => value
            .into_string()
            .map_err(|_| anyhow::anyhow!("{name} must be valid UTF-8")),
        None => Ok(default.to_owned()),
    }
}

fn positive_arg_or_env(
    value: Option<u64>,
    name: &str,
    default: Option<u64>,
    lookup: &dyn Fn(&str) -> Option<OsString>,
) -> Result<Option<u64>> {
    if let Some(value) = value {
        ensure!(value > 0, "{name} must be a positive integer");
        return Ok(Some(value));
    }
    match lookup(name) {
        Some(value) => {
            let value = value
                .into_string()
                .map_err(|_| anyhow::anyhow!("{name} must be ASCII"))?;
            ensure!(
                !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
                "{name} must be a positive integer"
            );
            let value = value
                .parse::<u64>()
                .with_context(|| format!("{name} must be a positive integer"))?;
            ensure!(value > 0, "{name} must be a positive integer");
            Ok(Some(value))
        }
        None => Ok(default),
    }
}

fn optional_positive_arg_or_env(
    value: Option<u64>,
    name: &str,
    lookup: &dyn Fn(&str) -> Option<OsString>,
) -> Result<Option<u64>> {
    if value.is_some() {
        return positive_arg_or_env(value, name, None, lookup);
    }
    match lookup(name) {
        Some(value) if value.is_empty() => Ok(None),
        Some(value) => {
            let value = value
                .into_string()
                .map_err(|_| anyhow::anyhow!("{name} must be ASCII"))?;
            ensure!(
                value.bytes().all(|byte| byte.is_ascii_digit()),
                "{name} must be a positive integer"
            );
            let value = value
                .parse::<u64>()
                .with_context(|| format!("{name} must be a positive integer"))?;
            ensure!(value > 0, "{name} must be a positive integer");
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

fn validate_absolute_non_root(path: &Path, label: &str) -> Result<()> {
    ensure!(path.is_absolute(), "{label} path must be absolute");
    ensure!(path != Path::new("/"), "{label} path must be non-root");
    ensure!(
        path.components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_))),
        "{label} path contains a forbidden component"
    );
    ensure!(path.file_name().is_some(), "{label} path must name a file");
    Ok(())
}

fn valid_chat_id(value: &str) -> bool {
    if let Some(name) = value.strip_prefix('@') {
        return (5..=32).contains(&name.len())
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    }
    let digits = value.strip_prefix('-').unwrap_or(value);
    (1..=20).contains(&digits.len()) && digits.bytes().all(|byte| byte.is_ascii_digit())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AckSnapshot {
    _through_sequence: u64,
    updated_unix_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Inactive,
    Opening,
    Active,
    Recovery,
}

impl Phase {
    fn persisted(self) -> &'static str {
        match self {
            Self::Inactive => "inactive",
            Self::Opening => "opening",
            Self::Active => "active",
            Self::Recovery => "recovery",
        }
    }

    fn effective(self) -> Self {
        match self {
            Self::Opening => Self::Active,
            Self::Recovery => Self::Inactive,
            phase => phase,
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "inactive" => Ok(Self::Inactive),
            "opening" => Ok(Self::Opening),
            "active" => Ok(Self::Active),
            "recovery" => Ok(Self::Recovery),
            _ => bail!("invalid alert phase"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PersistedState {
    schema_version: u64,
    phase: String,
}

struct StateStore {
    parent: File,
    state_name: CString,
    lock_name: CString,
}

impl StateStore {
    fn open(path: &Path) -> Result<Self> {
        validate_absolute_non_root(path, "alert state")?;
        let parent_path = path.parent().context("alert state path lacks a parent")?;
        let parent = open_directory_chain(parent_path, true)
            .context("open private alert-state directory")?;
        validate_private_directory(&parent)?;
        let state_name = c_string(path.file_name().expect("validated file name"))?;
        let mut lock = state_name.as_bytes().to_vec();
        lock.extend_from_slice(b".lock");
        Ok(Self {
            parent,
            state_name,
            lock_name: CString::new(lock).context("alert lock name contains NUL")?,
        })
    }

    fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            parent: self
                .parent
                .try_clone()
                .context("duplicate alert-state directory handle")?,
            state_name: self.state_name.clone(),
            lock_name: self.lock_name.clone(),
        })
    }

    fn lock(&self) -> Result<StateLock> {
        let descriptor = unsafe {
            libc::openat(
                self.parent.as_raw_fd(),
                self.lock_name.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error()).context("open alert-state lock");
        }
        let file = unsafe { File::from_raw_fd(descriptor) };
        validate_private_regular(&file, "alert-state lock")?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::WouldBlock {
                bail!("another ACK monitor holds the alert-state lock");
            }
            return Err(error).context("lock alert state");
        }
        // Fence replacement between open and lock acquisition. The private parent mode prevents
        // later replacement by another uid while this process owns the lock.
        let current = openat_regular(&self.parent, &self.lock_name, libc::O_RDONLY)?;
        let opened = file.metadata().context("inspect opened alert-state lock")?;
        let linked = current
            .metadata()
            .context("inspect linked alert-state lock")?;
        ensure!(
            opened.dev() == linked.dev() && opened.ino() == linked.ino(),
            "alert-state lock changed during acquisition"
        );
        Ok(StateLock { _file: file })
    }

    fn load_persisted(&self) -> Result<(Phase, bool)> {
        let mut file = match openat_regular(&self.parent, &self.state_name, libc::O_RDONLY) {
            Ok(file) => file,
            Err(error) if is_not_found(&error) => return Ok((Phase::Inactive, false)),
            Err(error) => return Err(error).context("open alert state"),
        };
        validate_private_regular(&file, "alert state")?;
        let bytes = read_open_bounded(&mut file, MAX_STATE_BYTES, "alert state")?;
        let value: PersistedState = serde_json::from_slice(&bytes).context("parse alert state")?;
        ensure!(value.schema_version == 1, "unsupported alert state");
        Ok((Phase::parse(&value.phase)?, true))
    }

    fn load(&self) -> Result<(Phase, bool)> {
        let (phase, exists) = self.load_persisted()?;
        Ok((phase.effective(), exists))
    }

    fn save(&self, phase: Phase, now: u64) -> Result<()> {
        let payload = format!(
            "{{\"phase\":\"{}\",\"schema_version\":1,\"updated_unix_secs\":{now}}}\n",
            phase.persisted()
        );
        ensure!(
            payload.len() <= MAX_STATE_BYTES,
            "alert state exceeds its limit"
        );
        let ordinal = TEMPORARY_ORDINAL.fetch_add(1, Ordering::Relaxed);
        let temporary = CString::new(format!(
            ".{}.{}.{}.tmp",
            self.state_name.to_string_lossy(),
            std::process::id(),
            ordinal
        ))
        .context("temporary alert-state name contains NUL")?;
        let descriptor = unsafe {
            libc::openat(
                self.parent.as_raw_fd(),
                temporary.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error()).context("create temporary alert state");
        }
        let mut file = unsafe { File::from_raw_fd(descriptor) };
        let published = (|| -> Result<()> {
            file.write_all(payload.as_bytes())
                .context("write temporary alert state")?;
            file.sync_all().context("sync temporary alert state")?;
            let result = unsafe {
                libc::renameat(
                    self.parent.as_raw_fd(),
                    temporary.as_ptr(),
                    self.parent.as_raw_fd(),
                    self.state_name.as_ptr(),
                )
            };
            if result != 0 {
                return Err(io::Error::last_os_error()).context("publish alert state");
            }
            self.parent
                .sync_all()
                .context("sync alert-state directory")?;
            Ok(())
        })();
        if published.is_err() {
            unsafe {
                libc::unlinkat(self.parent.as_raw_fd(), temporary.as_ptr(), 0);
            }
        }
        published
    }
}

struct StateLock {
    _file: File,
}

fn c_string(value: &OsStr) -> Result<CString> {
    CString::new(value.as_bytes()).context("path component contains NUL")
}

fn open_directory_chain(path: &Path, create: bool) -> Result<File> {
    ensure!(path.is_absolute(), "directory path must be absolute");
    let root = CString::new("/").expect("root has no NUL");
    let descriptor = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error()).context("open filesystem root");
    }
    let mut directory = unsafe { File::from_raw_fd(descriptor) };
    for component in path.components() {
        let Component::Normal(component) = component else {
            if component == Component::RootDir {
                continue;
            }
            bail!("directory path contains a forbidden component");
        };
        let name = c_string(component)?;
        let mut next = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if next < 0 && create && io::Error::last_os_error().kind() == io::ErrorKind::NotFound {
            let made = unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o700) };
            if made != 0 && io::Error::last_os_error().kind() != io::ErrorKind::AlreadyExists {
                return Err(io::Error::last_os_error()).context("create alert-state directory");
            }
            // The transitional Opening/Recovery phase is only useful after a crash when its
            // entire directory chain is durable. Persist each new name in its containing
            // directory before descending into it. This is also required after an accepted
            // EEXIST: a concurrent creator might not have synced the containing directory yet.
            directory
                .sync_all()
                .context("sync newly created alert-state directory entry")?;
            next = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                )
            };
        }
        if next < 0 {
            return Err(io::Error::last_os_error()).context("open directory component");
        }
        directory = unsafe { File::from_raw_fd(next) };
    }
    Ok(directory)
}

fn validate_private_directory(directory: &File) -> Result<()> {
    let metadata = directory
        .metadata()
        .context("inspect alert-state directory")?;
    ensure!(metadata.is_dir(), "alert-state parent is not a directory");
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "alert-state parent must be owned by the monitor uid"
    );
    ensure!(
        metadata.mode() & 0o022 == 0,
        "alert-state parent must not be group/world writable"
    );
    Ok(())
}

fn validate_private_regular(file: &File, label: &str) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect {label}"))?;
    ensure!(metadata.is_file(), "{label} is not a regular file");
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "{label} must be owned by the monitor uid"
    );
    ensure!(
        metadata.mode() & 0o077 == 0,
        "{label} must not be accessible by group/other"
    );
    Ok(())
}

fn openat_regular(parent: &File, name: &CString, access: i32) -> Result<File> {
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            access | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error()).context("open bounded regular file");
    }
    let file = unsafe { File::from_raw_fd(descriptor) };
    ensure!(
        file.metadata().context("inspect bounded file")?.is_file(),
        "bounded input is not a regular file"
    );
    Ok(file)
}

fn is_not_found(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|cause| cause.downcast_ref::<io::Error>())
        .any(|error| error.kind() == io::ErrorKind::NotFound)
}

fn read_open_bounded(file: &mut File, maximum: usize, label: &str) -> Result<Vec<u8>> {
    let size = file
        .metadata()
        .with_context(|| format!("inspect {label}"))?
        .len();
    ensure!(
        size > 0 && size <= maximum as u64,
        "{label} has an invalid size"
    );
    let mut bytes = Vec::with_capacity(size as usize);
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {label}"))?;
    ensure!(
        !bytes.is_empty() && bytes.len() <= maximum,
        "{label} exceeds its size limit"
    );
    Ok(bytes)
}

fn read_bounded_regular(path: &Path, maximum: usize, label: &str) -> Result<Vec<u8>> {
    validate_absolute_non_root(path, label)?;
    let parent = open_directory_chain(path.parent().expect("validated parent"), false)?;
    let name = c_string(path.file_name().expect("validated file name"))?;
    let mut file = openat_regular(&parent, &name, libc::O_RDONLY)?;
    read_open_bounded(&mut file, maximum, label)
}

fn read_token(path: &Path) -> Result<Zeroizing<String>> {
    let raw = Zeroizing::new(read_bounded_regular(
        path,
        MAX_TOKEN_BYTES,
        "Telegram token",
    )?);
    ensure!(
        !raw.contains(&b'\r'),
        "Telegram token contains a carriage return"
    );
    let raw = raw.strip_suffix(b"\n").unwrap_or(&raw);
    ensure!(
        !raw.contains(&b'\n'),
        "Telegram token must contain one line"
    );
    ensure!(raw.is_ascii(), "Telegram token must be ASCII");
    let token = std::str::from_utf8(raw).expect("ASCII is UTF-8");
    let (bot_id, secret) = token
        .split_once(':')
        .context("Telegram token has an invalid shape")?;
    ensure!(
        (6..=15).contains(&bot_id.len()) && bot_id.bytes().all(|byte| byte.is_ascii_digit()),
        "Telegram token has an invalid bot id"
    );
    ensure!(
        secret.len() >= 30
            && secret
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
        "Telegram token has an invalid secret"
    );
    Ok(Zeroizing::new(token.to_owned()))
}

fn read_ack_snapshot(path: &Path, now: u64) -> Option<AckSnapshot> {
    let bytes = read_bounded_regular(path, MAX_ACK_STATUS_BYTES, "ACK status").ok()?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    let object = value.as_object()?;
    if object.get("schema_version")?.as_u64()? != 1 {
        return None;
    }
    let through_sequence = object.get("through_sequence")?.as_u64()?;
    let updated_unix_secs = object.get("updated_unix_secs")?.as_u64()?;
    if updated_unix_secs == 0 || updated_unix_secs > now.saturating_add(300) {
        return None;
    }
    Some(AckSnapshot {
        _through_sequence: through_sequence,
        updated_unix_secs,
    })
}

fn human_duration(seconds: u64) -> String {
    if seconds < 60 {
        return format!("{seconds} seconds");
    }
    let minutes = seconds / 60;
    if minutes < 60 {
        return format!("{minutes} minute{}", if minutes == 1 { "" } else { "s" });
    }
    let hours = minutes / 60;
    format!("{hours} hour{}", if hours == 1 { "" } else { "s" })
}

fn warning_message(age: Option<u64>) -> String {
    let last = age
        .map(|age| format!("{} ago", human_duration(age)))
        .unwrap_or_else(|| "unavailable".to_owned());
    [
        "Blockzilla backup - WARNING".to_owned(),
        "Durable receiver confirmations stopped".to_owned(),
        format!("Last confirmation: {last}."),
        "source host is keeping every unconfirmed gRPC block.".to_owned(),
        "Action: check live capture first, then the NAS receiver.".to_owned(),
    ]
    .join("\n")
}

fn recovery_message() -> String {
    [
        "Blockzilla backup - RECOVERED",
        "Durable receiver confirmations resumed",
        "source host received a new signed storage ACK.",
        "This confirms backup storage, not indexing.",
        "Action: none.",
    ]
    .join("\n")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delivery {
    Delivered,
    Rejected,
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingAlert {
    None,
    Warning(String),
    Recovery(String),
}

struct PreparedAlert {
    pending: PendingAlert,
    // Keep the advisory lock alive from the state transition until delivery is finalized. A
    // second monitor must never observe and advance a transitional phase while the HTTP request
    // that created it is still in flight.
    _lock: StateLock,
}

fn prepare_once(
    config: &Config,
    store: &StateStore,
    now: u64,
    startup_elapsed: Duration,
) -> Result<PreparedAlert> {
    let state_lock = store.lock()?;
    let (phase, state_existed) = store.load()?;
    let snapshot = read_ack_snapshot(&config.ack_status_file, now);
    let age = snapshot.map(|snapshot| now.saturating_sub(snapshot.updated_unix_secs));
    let stale = age.is_none_or(|age| age > config.stale_after.as_secs());

    if stale {
        if startup_elapsed < config.startup_grace || phase == Phase::Active {
            return Ok(PreparedAlert {
                pending: PendingAlert::None,
                _lock: state_lock,
            });
        }
        store.save(Phase::Opening, now)?;
        return Ok(PreparedAlert {
            pending: PendingAlert::Warning(warning_message(age)),
            _lock: state_lock,
        });
    }

    let pending = if phase == Phase::Active {
        store.save(Phase::Recovery, now)?;
        PendingAlert::Recovery(recovery_message())
    } else {
        if !state_existed {
            store.save(Phase::Inactive, now)?;
        }
        PendingAlert::None
    };
    Ok(PreparedAlert {
        pending,
        _lock: state_lock,
    })
}

fn finalize_once(
    store: &StateStore,
    prepared: PreparedAlert,
    delivery: Delivery,
    now: u64,
) -> Result<()> {
    let pending = &prepared.pending;
    if delivery == Delivery::Ambiguous || *pending == PendingAlert::None {
        return Ok(());
    }
    let (expected, next) = match (pending, delivery) {
        (PendingAlert::Warning(_), Delivery::Delivered) => (Phase::Opening, Phase::Active),
        (PendingAlert::Warning(_), Delivery::Rejected) => (Phase::Opening, Phase::Inactive),
        (PendingAlert::Recovery(_), Delivery::Delivered) => (Phase::Recovery, Phase::Inactive),
        (PendingAlert::Recovery(_), Delivery::Rejected) => (Phase::Recovery, Phase::Active),
        (_, Delivery::Ambiguous) | (PendingAlert::None, _) => return Ok(()),
    };
    // `prepared` still owns the lock acquired before the transitional state was persisted.
    let (current, exists) = store.load_persisted()?;
    ensure!(
        exists && current == expected,
        "alert state changed while delivery was in flight"
    );
    store.save(next, now)
}

#[cfg(test)]
async fn run_once<S, Fut>(
    config: &Config,
    store: &StateStore,
    sender: &mut S,
    now: u64,
    startup_elapsed: Duration,
) -> Result<()>
where
    S: FnMut(String) -> Fut,
    Fut: Future<Output = Delivery>,
{
    let prepared = prepare_once(config, store, now, startup_elapsed)?;
    let message = match &prepared.pending {
        PendingAlert::None => return Ok(()),
        PendingAlert::Warning(message) | PendingAlert::Recovery(message) => message.clone(),
    };
    let warning = matches!(prepared.pending, PendingAlert::Warning(_));
    let delivery = sender(message).await;
    finalize_once(store, prepared, delivery, now)?;
    if delivery == Delivery::Delivered {
        if warning {
            tracing::warn!("pull ACK warning delivered");
        } else {
            tracing::info!("pull ACK recovery delivered");
        }
    }
    Ok(())
}

async fn run_once_offloaded(
    config: &Config,
    store: &StateStore,
    sender: &TelegramSender,
    now: u64,
    startup_elapsed: Duration,
) -> Result<()> {
    // Load the rotatable secret before creating a transitional state. A failed/cancelled local
    // read is definitely unsent and therefore remains retryable on the next poll.
    let token_path = config.token_file.clone();
    let token = match tokio::task::spawn_blocking(move || read_token(&token_path))
        .await
        .context("join Telegram token loading")?
    {
        Ok(token) => token,
        Err(_) => {
            tracing::warn!("Telegram token could not be loaded");
            return Ok(());
        }
    };
    let blocking_config = config.clone();
    let blocking_store = store.try_clone()?;
    let prepared = tokio::task::spawn_blocking(move || {
        prepare_once(&blocking_config, &blocking_store, now, startup_elapsed)
    })
    .await
    .context("join ACK-monitor preparation")??;
    let message = match &prepared.pending {
        PendingAlert::None => return Ok(()),
        PendingAlert::Warning(message) | PendingAlert::Recovery(message) => message.clone(),
    };
    let warning = matches!(prepared.pending, PendingAlert::Warning(_));
    let delivery = sender.send(message, token).await;
    let blocking_store = store.try_clone()?;
    tokio::task::spawn_blocking(move || finalize_once(&blocking_store, prepared, delivery, now))
        .await
        .context("join ACK-monitor state finalization")??;
    if delivery == Delivery::Delivered {
        if warning {
            tracing::warn!("pull ACK warning delivered");
        } else {
            tracing::info!("pull ACK recovery delivered");
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct TelegramResponse {
    ok: bool,
}

struct TelegramSender {
    config: Config,
    client: Client,
}

impl TelegramSender {
    fn new(config: Config) -> Result<Self> {
        let client = Client::builder()
            .https_only(true)
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
            .timeout(TELEGRAM_REQUEST_TIMEOUT)
            .user_agent("hivezilla-pull-ack-monitor/1")
            .build()
            .context("build Telegram HTTP client")?;
        Ok(Self { config, client })
    }

    async fn send(&self, message: String, token: Zeroizing<String>) -> Delivery {
        match self.try_send(message, token).await {
            Ok(delivery) => delivery,
            Err(SendError::Rejected(status)) => {
                if let Some(status) = status {
                    tracing::warn!(http_status = status.as_u16(), "Telegram delivery rejected");
                } else {
                    tracing::warn!("Telegram delivery rejected");
                }
                Delivery::Rejected
            }
            Err(SendError::Ambiguous) => {
                // Error values and URLs can contain the bot token. Never log either.
                tracing::warn!("Telegram delivery outcome is ambiguous");
                Delivery::Ambiguous
            }
        }
    }

    async fn try_send(
        &self,
        message: String,
        token: Zeroizing<String>,
    ) -> std::result::Result<Delivery, SendError> {
        let url = Zeroizing::new(format!(
            "https://api.telegram.org/bot{}/sendMessage",
            token.as_str()
        ));
        self.try_send_to_url(message, url.as_str()).await
    }

    async fn try_send_to_url(
        &self,
        message: String,
        url: &str,
    ) -> std::result::Result<Delivery, SendError> {
        let mut fields = vec![
            ("chat_id", self.config.chat_id.clone()),
            ("text", message),
            ("disable_web_page_preview", "true".to_owned()),
        ];
        if let Some(thread_id) = self.config.message_thread_id {
            fields.push(("message_thread_id", thread_id.to_string()));
        }
        let response = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(form_encode(&fields))
            .send()
            .await
            .map_err(|_| SendError::Ambiguous)?;
        classify_http_status(response.status())?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_TELEGRAM_RESPONSE_BYTES as u64)
        {
            return Err(SendError::Ambiguous);
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| SendError::Ambiguous)?;
            if body.len().saturating_add(chunk.len()) > MAX_TELEGRAM_RESPONSE_BYTES {
                return Err(SendError::Ambiguous);
            }
            body.extend_from_slice(&chunk);
        }
        let response: TelegramResponse =
            serde_json::from_slice(&body).map_err(|_| SendError::Ambiguous)?;
        if response.ok {
            Ok(Delivery::Delivered)
        } else {
            Err(SendError::Rejected(None))
        }
    }
}

fn classify_http_status(status: StatusCode) -> std::result::Result<(), SendError> {
    if status.is_success() {
        return Ok(());
    }
    // A client-error response is an explicit refusal, except Request Timeout: an intermediary or
    // upstream may have timed out after Telegram committed the message. Redirects, 408, and 5xx
    // are therefore ambiguous under the monitor's strict at-most-once policy.
    if status.is_client_error() && status != StatusCode::REQUEST_TIMEOUT {
        Err(SendError::Rejected(Some(status)))
    } else {
        Err(SendError::Ambiguous)
    }
}

fn form_encode(fields: &[(&str, String)]) -> String {
    let mut encoded = String::new();
    for (index, (name, value)) in fields.iter().enumerate() {
        if index != 0 {
            encoded.push('&');
        }
        form_encode_component(name.as_bytes(), &mut encoded);
        encoded.push('=');
        form_encode_component(value.as_bytes(), &mut encoded);
    }
    encoded
}

fn form_encode_component(input: &[u8], output: &mut String) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &byte in input {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                output.push(char::from(byte));
            }
            b' ' => output.push('+'),
            _ => {
                output.push('%');
                output.push(char::from(HEX[usize::from(byte >> 4)]));
                output.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SendError {
    Rejected(Option<StatusCode>),
    Ambiguous,
}

pub async fn monitor_pull_ack_telegram(args: MonitorPullAckTelegramArgs) -> Result<()> {
    let config = Config::from_args(args)?;
    let store = StateStore::open(&config.state_file)?;
    let sender = TelegramSender::new(config.clone())?;
    let shutdown = shutdown_signal()?;
    run_monitor_until(config, store, sender, shutdown).await
}

async fn run_monitor_until<F>(
    config: Config,
    store: StateStore,
    sender: TelegramSender,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()>,
{
    tracing::info!(
        stale_after_secs = config.stale_after.as_secs(),
        interval_secs = config.interval.as_secs(),
        "pull ACK monitor started"
    );
    let started = Instant::now();
    let mut shutdown = Box::pin(shutdown);
    loop {
        let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(now) => now.as_secs(),
            Err(_) => {
                tracing::warn!("pull ACK monitor clock is before Unix epoch");
                tokio::select! {
                    biased;
                    _ = &mut shutdown => break,
                    _ = tokio::time::sleep(config.interval) => {}
                }
                continue;
            }
        };
        let check = run_once_offloaded(&config, &store, &sender, now, started.elapsed());
        tokio::pin!(check);
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            result = &mut check => {
                if result.is_err() {
                    // Paths and parser errors are private operational data.
                    tracing::warn!("pull ACK monitor check failed");
                }
            }
        }
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            _ = tokio::time::sleep(config.interval) => {}
        }
    }
    tracing::info!("pull ACK monitor stopped");
    Ok(())
}

#[cfg(unix)]
fn shutdown_signal() -> Result<impl Future<Output = ()>> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install SIGTERM handler")?;
    Ok(async move {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if result.is_err() {
                    tracing::warn!("failed to await interrupt signal");
                }
            }
            _ = terminate.recv() => {}
        }
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> Result<impl Future<Output = ()>> {
    Ok(async {
        if tokio::signal::ctrl_c().await.is_err() {
            tracing::warn!("failed to await interrupt signal");
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        fs,
        future::{pending, ready},
        net::TcpListener,
        os::unix::fs::{PermissionsExt, symlink},
        sync::{Arc, Mutex, atomic::AtomicBool},
        thread,
    };
    use tempfile::TempDir;

    const TOKEN: &str = "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi";

    struct Fixture {
        _temporary: TempDir,
        root: PathBuf,
        config: Config,
    }

    impl Fixture {
        fn new() -> Self {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let root = temporary
                .path()
                .canonicalize()
                .expect("canonical fixture root");
            let token = root.join("token");
            fs::write(&token, TOKEN).expect("write token");
            let config = Config {
                ack_status_file: root.join("pull-ack-status.json"),
                state_file: root.join("state/pull-ack-alert.json"),
                token_file: token,
                chat_id: "-100123456".to_owned(),
                message_thread_id: None,
                stale_after: Duration::from_secs(300),
                startup_grace: Duration::from_secs(300),
                interval: Duration::from_secs(30),
            };
            Self {
                _temporary: temporary,
                root,
                config,
            }
        }

        fn store(&self) -> StateStore {
            StateStore::open(&self.config.state_file).expect("state store")
        }

        fn write_status(&self, updated: u64, sequence: u64) {
            fs::write(
                &self.config.ack_status_file,
                serde_json::json!({
                    "schema_version": 1,
                    "through_sequence": sequence,
                    "updated_unix_secs": updated,
                })
                .to_string(),
            )
            .expect("write ACK status");
        }
    }

    fn empty_args() -> MonitorPullAckTelegramArgs {
        MonitorPullAckTelegramArgs {
            ack_status_file: None,
            state_file: None,
            token_file: None,
            chat_id: None,
            message_thread_id: None,
            stale_after_secs: None,
            startup_grace_secs: None,
            interval_secs: None,
        }
    }

    fn local_http_response(response: Vec<u8>, delay: Duration) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock Telegram server");
        let address = listener.local_addr().expect("mock server address");
        let worker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept mock request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("request read timeout");
            let mut request = [0_u8; 8 * 1024];
            let _ = stream.read(&mut request);
            if !delay.is_zero() {
                thread::sleep(delay);
            }
            let _ = stream.write_all(&response);
        });
        (format!("http://{address}/sendMessage"), worker)
    }

    fn test_telegram_sender(config: Config, timeout: Duration) -> TelegramSender {
        let client = Client::builder()
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .expect("test Telegram client");
        TelegramSender { config, client }
    }

    #[tokio::test]
    async fn fresh_status_is_silent_and_initializes_state() {
        let fixture = Fixture::new();
        fixture.write_status(990, 7);
        let store = fixture.store();
        let sent = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&sent);
        let mut sender = move |message| {
            captured.lock().expect("sent lock").push(message);
            ready(Delivery::Delivered)
        };
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_000,
            Duration::from_secs(301),
        )
        .await
        .expect("run check");
        assert!(sent.lock().expect("sent lock").is_empty());
        assert_eq!(store.load().expect("load state"), (Phase::Inactive, true));
    }

    #[tokio::test]
    async fn stale_status_warns_once_then_recovers_once() {
        let fixture = Fixture::new();
        fixture.write_status(600, 7);
        let store = fixture.store();
        let sent = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&sent);
        let mut sender = move |message| {
            captured.lock().expect("sent lock").push(message);
            ready(Delivery::Delivered)
        };
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_000,
            Duration::from_secs(299),
        )
        .await
        .expect("grace check");
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_000,
            Duration::from_secs(300),
        )
        .await
        .expect("warning check");
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_030,
            Duration::from_secs(330),
        )
        .await
        .expect("repeated warning check");
        fixture.write_status(1_035, 8);
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_040,
            Duration::from_secs(340),
        )
        .await
        .expect("recovery check");
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_070,
            Duration::from_secs(370),
        )
        .await
        .expect("repeated recovery check");
        let sent = sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 2);
        assert!(sent[0].contains("6 minutes ago"));
        assert!(sent[0].contains("every unconfirmed gRPC block"));
        assert!(sent[1].contains("RECOVERED"));
        assert!(sent[1].contains("not indexing"));
    }

    #[tokio::test]
    async fn missing_status_has_an_explicit_unknown_age() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let sent = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&sent);
        let mut sender = move |message| {
            captured.lock().expect("sent lock").push(message);
            ready(Delivery::Delivered)
        };
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_000,
            Duration::from_secs(300),
        )
        .await
        .expect("missing check");
        assert!(sent.lock().expect("sent lock")[0].contains("Last confirmation: unavailable."));
    }

    #[tokio::test]
    async fn definite_rejection_rolls_back_and_retries() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let mut rejected = |_| ready(Delivery::Rejected);
        run_once(
            &fixture.config,
            &store,
            &mut rejected,
            1_000,
            Duration::from_secs(300),
        )
        .await
        .expect("rejected warning");
        assert_eq!(store.load().expect("state").0, Phase::Inactive);
        let calls = Arc::new(Mutex::new(0));
        let captured = Arc::clone(&calls);
        let mut delivered = move |_| {
            *captured.lock().expect("calls lock") += 1;
            ready(Delivery::Delivered)
        };
        run_once(
            &fixture.config,
            &store,
            &mut delivered,
            1_030,
            Duration::from_secs(330),
        )
        .await
        .expect("retry warning");
        assert_eq!(*calls.lock().expect("calls lock"), 1);
    }

    #[tokio::test]
    async fn rotated_token_read_failure_stays_retryable_without_touching_state() {
        let fixture = Fixture::new();
        let store = fixture.store();
        fs::write(&fixture.config.token_file, "invalid").expect("write invalid rotated token");
        let sender = TelegramSender::new(fixture.config.clone()).expect("sender");
        run_once_offloaded(
            &fixture.config,
            &store,
            &sender,
            1_000,
            Duration::from_secs(300),
        )
        .await
        .expect("invalid token is a retryable local failure");
        assert_eq!(store.load().expect("state"), (Phase::Inactive, false));

        fs::write(&fixture.config.token_file, TOKEN).expect("restore token");
        let prepared = prepare_once(&fixture.config, &store, 1_030, Duration::from_secs(330))
            .expect("next poll prepares warning");
        assert!(matches!(prepared.pending, PendingAlert::Warning(_)));
    }

    #[tokio::test]
    async fn ambiguous_opening_is_assumed_delivered_and_not_retried() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let calls = Arc::new(Mutex::new(0));
        let captured = Arc::clone(&calls);
        let mut sender = move |_| {
            *captured.lock().expect("calls lock") += 1;
            ready(Delivery::Ambiguous)
        };
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_000,
            Duration::from_secs(300),
        )
        .await
        .expect("ambiguous warning");
        assert_eq!(store.load().expect("state").0, Phase::Active);
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_030,
            Duration::from_secs(330),
        )
        .await
        .expect("second stale check");
        assert_eq!(*calls.lock().expect("calls lock"), 1);
    }

    #[tokio::test]
    async fn ambiguous_recovery_is_assumed_delivered_and_not_retried() {
        let fixture = Fixture::new();
        fixture.write_status(995, 8);
        let store = fixture.store();
        store.save(Phase::Active, 900).expect("active state");
        let calls = Arc::new(Mutex::new(0));
        let captured = Arc::clone(&calls);
        let mut sender = move |_| {
            *captured.lock().expect("calls lock") += 1;
            ready(Delivery::Ambiguous)
        };
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_000,
            Duration::from_secs(300),
        )
        .await
        .expect("ambiguous recovery");
        assert_eq!(store.load().expect("state").0, Phase::Inactive);
        run_once(
            &fixture.config,
            &store,
            &mut sender,
            1_030,
            Duration::from_secs(330),
        )
        .await
        .expect("second fresh check");
        assert_eq!(*calls.lock().expect("calls lock"), 1);
    }

    #[test]
    fn crash_phases_map_to_the_at_most_once_effective_state() {
        let fixture = Fixture::new();
        let store = fixture.store();
        store.save(Phase::Opening, 1_000).expect("opening state");
        assert_eq!(store.load().expect("load opening").0, Phase::Active);
        store.save(Phase::Recovery, 1_001).expect("recovery state");
        assert_eq!(store.load().expect("load recovery").0, Phase::Inactive);
    }

    #[test]
    fn symlink_status_and_non_regular_status_are_not_followed() {
        let fixture = Fixture::new();
        let target = fixture.root.join("target");
        fs::write(&target, "{}").expect("write target");
        symlink(&target, &fixture.config.ack_status_file).expect("status symlink");
        assert_eq!(
            read_ack_snapshot(&fixture.config.ack_status_file, 1_000),
            None
        );
        fs::remove_file(&fixture.config.ack_status_file).expect("remove symlink");
        fs::create_dir(&fixture.config.ack_status_file).expect("status directory");
        assert_eq!(
            read_ack_snapshot(&fixture.config.ack_status_file, 1_000),
            None
        );
    }

    #[test]
    fn malformed_oversized_boolean_and_far_future_status_fail_closed() {
        let fixture = Fixture::new();
        for body in [
            "not-json".to_owned(),
            serde_json::json!({"schema_version": 1, "through_sequence": true, "updated_unix_secs": 900}).to_string(),
            serde_json::json!({"schema_version": 1, "through_sequence": 1, "updated_unix_secs": 1_301}).to_string(),
            "x".repeat(MAX_ACK_STATUS_BYTES + 1),
        ] {
            fs::write(&fixture.config.ack_status_file, body).expect("write invalid status");
            assert_eq!(read_ack_snapshot(&fixture.config.ack_status_file, 1_000), None);
        }
    }

    #[test]
    fn token_shape_is_strict_and_secret_is_zeroizing() {
        let fixture = Fixture::new();
        assert_eq!(
            read_token(&fixture.config.token_file)
                .expect("token")
                .as_str(),
            TOKEN
        );
        for token in [
            "12345:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi",
            "123456:short",
            "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefg!",
            "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi\r\n",
            "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi\nsecond",
        ] {
            fs::write(&fixture.config.token_file, token).expect("write invalid token");
            assert!(
                read_token(&fixture.config.token_file).is_err(),
                "accepted {token:?}"
            );
        }
    }

    #[test]
    fn token_symlink_and_oversize_are_rejected() {
        let fixture = Fixture::new();
        let target = fixture.root.join("real-token");
        fs::write(&target, TOKEN).expect("write token target");
        fs::remove_file(&fixture.config.token_file).expect("remove token");
        symlink(&target, &fixture.config.token_file).expect("token symlink");
        assert!(read_token(&fixture.config.token_file).is_err());
        fs::remove_file(&fixture.config.token_file).expect("remove token symlink");
        fs::write(&fixture.config.token_file, "x".repeat(MAX_TOKEN_BYTES + 1))
            .expect("oversize token");
        assert!(read_token(&fixture.config.token_file).is_err());
    }

    #[test]
    fn state_is_atomic_private_and_rejects_symlinks() {
        let fixture = Fixture::new();
        let store = fixture.store();
        store.save(Phase::Active, 1_000).expect("save state");
        let mode = fs::metadata(&fixture.config.state_file)
            .expect("state metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        fs::remove_file(&fixture.config.state_file).expect("remove state");
        let target = fixture.root.join("state-target");
        fs::write(&target, r#"{"schema_version":1,"phase":"active"}"#).expect("write state target");
        symlink(&target, &fixture.config.state_file).expect("state symlink");
        assert!(store.load().is_err());
    }

    #[test]
    fn invalid_state_schema_and_phase_fail_closed() {
        let fixture = Fixture::new();
        let store = fixture.store();
        for body in [
            r#"{"schema_version":2,"phase":"active"}"#,
            r#"{"schema_version":1,"phase":"unknown"}"#,
            r#"[]"#,
        ] {
            fs::write(&fixture.config.state_file, body).expect("write invalid state");
            assert!(store.load().is_err());
        }
    }

    #[test]
    fn lock_excludes_a_second_monitor_instance() {
        let fixture = Fixture::new();
        let first = fixture.store();
        let second = fixture.store();
        let _guard = first.lock().expect("first lock");
        assert!(second.lock().is_err());
    }

    #[tokio::test]
    async fn cancellation_after_opening_preserves_at_most_once_state() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let mut sender = |_| pending::<Delivery>();
        {
            let check = run_once(
                &fixture.config,
                &store,
                &mut sender,
                1_000,
                Duration::from_secs(300),
            );
            tokio::pin!(check);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                _ = &mut check => panic!("pending delivery unexpectedly completed"),
            }
            assert!(
                fixture.store().lock().is_err(),
                "state lock must span the in-flight delivery"
            );
        }
        assert_eq!(
            store.load().expect("state after cancellation").0,
            Phase::Active
        );
    }

    #[test]
    fn duration_rendering_matches_the_operational_messages() {
        assert_eq!(human_duration(0), "0 seconds");
        assert_eq!(human_duration(59), "59 seconds");
        assert_eq!(human_duration(60), "1 minute");
        assert_eq!(human_duration(119), "1 minute");
        assert_eq!(human_duration(120), "2 minutes");
        assert_eq!(human_duration(3_600), "1 hour");
        assert_eq!(human_duration(7_200), "2 hours");
    }

    #[test]
    fn chat_ids_and_paths_are_strictly_validated() {
        for valid in ["1", "-100123456", "@abcde", "@ABC_123"] {
            assert!(valid_chat_id(valid), "rejected {valid}");
        }
        for invalid in ["", "-", "+123", "@abcd", "@bad-name", "1 2"] {
            assert!(!valid_chat_id(invalid), "accepted {invalid}");
        }
        assert!(validate_absolute_non_root(Path::new("relative"), "test").is_err());
        assert!(validate_absolute_non_root(Path::new("/"), "test").is_err());
        assert!(validate_absolute_non_root(Path::new("/tmp/../token"), "test").is_err());
        assert!(validate_absolute_non_root(Path::new("/tmp/token"), "test").is_ok());
    }

    #[test]
    fn private_state_parent_rejects_group_writable_directories_and_symlink_ancestors() {
        let fixture = Fixture::new();
        let unsafe_parent = fixture.root.join("unsafe-state");
        fs::create_dir(&unsafe_parent).expect("unsafe parent");
        fs::set_permissions(&unsafe_parent, fs::Permissions::from_mode(0o770))
            .expect("unsafe mode");
        assert!(StateStore::open(&unsafe_parent.join("state.json")).is_err());

        let real = fixture.root.join("real-parent");
        fs::create_dir(&real).expect("real parent");
        let linked = fixture.root.join("linked-parent");
        symlink(&real, &linked).expect("parent symlink");
        assert!(StateStore::open(&linked.join("state.json")).is_err());
    }

    #[test]
    fn nested_state_directories_are_created_private_and_are_immediately_usable() {
        let mut fixture = Fixture::new();
        fixture.config.state_file = fixture.root.join("one/two/three/state.json");
        let store = fixture.store();
        for relative in ["one", "one/two", "one/two/three"] {
            let mode = fs::metadata(fixture.root.join(relative))
                .expect("created state directory")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o700);
        }
        store.save(Phase::Active, 1_000).expect("durable state");
        assert_eq!(store.load().expect("load state"), (Phase::Active, true));
    }

    #[test]
    fn cli_values_override_environment_and_environment_overrides_defaults() {
        let fixture = Fixture::new();
        let env_state = fixture.root.join("env-state/state.json");
        let explicit_state = fixture.root.join("explicit-state/state.json");
        let values = HashMap::from([
            (
                "BLOCKZILLA_PULL_ACK_STATUS_FILE",
                fixture.config.ack_status_file.as_os_str().to_owned(),
            ),
            (
                "BLOCKZILLA_PULL_ACK_ALERT_STATE_FILE",
                env_state.as_os_str().to_owned(),
            ),
            (
                "BLOCKZILLA_TELEGRAM_BOT_TOKEN_FILE",
                fixture.config.token_file.as_os_str().to_owned(),
            ),
            ("BLOCKZILLA_TELEGRAM_CHAT_ID", OsString::from("-100999")),
            (
                "BLOCKZILLA_TELEGRAM_MESSAGE_THREAD_ID",
                OsString::from("77"),
            ),
            (
                "BLOCKZILLA_PULL_ACK_STALE_AFTER_SECS",
                OsString::from("901"),
            ),
            (
                "BLOCKZILLA_PULL_ACK_STARTUP_GRACE_SECS",
                OsString::from("902"),
            ),
            (
                "BLOCKZILLA_PULL_ACK_MONITOR_INTERVAL_SECS",
                OsString::from("903"),
            ),
        ]);
        let lookup = |name: &str| values.get(name).cloned();
        let from_env = Config::from_args_with_env(empty_args(), &lookup).expect("environment");
        assert_eq!(from_env.state_file, env_state);
        assert_eq!(from_env.chat_id, "-100999");
        assert_eq!(from_env.message_thread_id, Some(77));
        assert_eq!(from_env.stale_after, Duration::from_secs(901));
        assert_eq!(from_env.startup_grace, Duration::from_secs(902));
        assert_eq!(from_env.interval, Duration::from_secs(903));

        let mut explicit = empty_args();
        explicit.ack_status_file = Some(fixture.config.ack_status_file.clone());
        explicit.state_file = Some(explicit_state.clone());
        explicit.token_file = Some(fixture.config.token_file.clone());
        explicit.chat_id = Some("-100123".to_owned());
        explicit.message_thread_id = Some(88);
        explicit.stale_after_secs = Some(101);
        explicit.startup_grace_secs = Some(102);
        explicit.interval_secs = Some(103);
        let from_cli = Config::from_args_with_env(explicit, &lookup).expect("CLI precedence");
        assert_eq!(from_cli.state_file, explicit_state);
        assert_eq!(from_cli.chat_id, "-100123");
        assert_eq!(from_cli.message_thread_id, Some(88));
        assert_eq!(from_cli.stale_after, Duration::from_secs(101));
        assert_eq!(from_cli.startup_grace, Duration::from_secs(102));
        assert_eq!(from_cli.interval, Duration::from_secs(103));
    }

    #[test]
    fn telegram_json_classification_is_exact_and_bounded() {
        let delivered: TelegramResponse =
            serde_json::from_slice(br#"{"ok":true}"#).expect("delivered response");
        let rejected: TelegramResponse =
            serde_json::from_slice(br#"{"ok":false}"#).expect("rejected response");
        assert!(delivered.ok);
        assert!(!rejected.ok);
        assert!(serde_json::from_slice::<TelegramResponse>(br#"{"ok":1}"#).is_err());
        assert!(serde_json::from_slice::<TelegramResponse>(br#"[]"#).is_err());
    }

    #[test]
    fn telegram_http_statuses_preserve_at_most_once_delivery() {
        assert_eq!(classify_http_status(StatusCode::OK), Ok(()));
        assert_eq!(
            classify_http_status(StatusCode::BAD_REQUEST),
            Err(SendError::Rejected(Some(StatusCode::BAD_REQUEST)))
        );
        assert_eq!(
            classify_http_status(StatusCode::TOO_MANY_REQUESTS),
            Err(SendError::Rejected(Some(StatusCode::TOO_MANY_REQUESTS)))
        );
        assert_eq!(
            classify_http_status(StatusCode::REQUEST_TIMEOUT),
            Err(SendError::Ambiguous)
        );
        assert_eq!(
            classify_http_status(StatusCode::FOUND),
            Err(SendError::Ambiguous)
        );
        assert_eq!(
            classify_http_status(StatusCode::INTERNAL_SERVER_ERROR),
            Err(SendError::Ambiguous)
        );
    }

    #[tokio::test]
    async fn telegram_transport_disables_redirects_and_bounds_status_body_and_time() {
        let fixture = Fixture::new();
        let sender = test_telegram_sender(fixture.config.clone(), Duration::from_millis(100));

        let redirect_target = TcpListener::bind("127.0.0.1:0").expect("bind redirect target");
        redirect_target
            .set_nonblocking(true)
            .expect("nonblocking redirect target");
        let redirect_target_address = redirect_target
            .local_addr()
            .expect("redirect target address");
        let followed = Arc::new(AtomicBool::new(false));
        let followed_worker = Arc::clone(&followed);
        let redirect_target_worker = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_millis(250);
            while Instant::now() < deadline {
                match redirect_target.accept() {
                    Ok((mut stream, _)) => {
                        followed_worker.store(true, Ordering::SeqCst);
                        let _ = stream.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
                        );
                        return;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("accept redirect target: {error}"),
                }
            }
        });
        let (url, worker) = local_http_response(
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{redirect_target_address}/other\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .into_bytes(),
            Duration::ZERO,
        );
        assert_eq!(
            sender.try_send_to_url("warning".to_owned(), &url).await,
            Err(SendError::Ambiguous)
        );
        worker.join().expect("redirect server");
        redirect_target_worker.join().expect("redirect target");
        assert!(!followed.load(Ordering::SeqCst));

        let (url, worker) = local_http_response(
            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            Duration::ZERO,
        );
        assert_eq!(
            sender.try_send_to_url("warning".to_owned(), &url).await,
            Err(SendError::Rejected(Some(StatusCode::BAD_REQUEST)))
        );
        worker.join().expect("rejection server");

        let (url, worker) = local_http_response(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_TELEGRAM_RESPONSE_BYTES + 1
            )
            .into_bytes(),
            Duration::ZERO,
        );
        assert_eq!(
            sender.try_send_to_url("warning".to_owned(), &url).await,
            Err(SendError::Ambiguous)
        );
        worker.join().expect("oversize server");

        let timeout_sender =
            test_telegram_sender(fixture.config.clone(), Duration::from_millis(25));
        let (url, worker) = local_http_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}"
                .to_vec(),
            Duration::from_millis(100),
        );
        assert_eq!(
            timeout_sender
                .try_send_to_url("warning".to_owned(), &url)
                .await,
            Err(SendError::Ambiguous)
        );
        worker.join().expect("timeout server");
    }

    #[test]
    fn telegram_form_encoding_is_utf8_safe_and_unambiguous() {
        assert_eq!(
            form_encode(&[("chat_id", "-100".to_owned()), ("text", "a & é".to_owned())]),
            "chat_id=-100&text=a+%26+%C3%A9"
        );
    }
}
