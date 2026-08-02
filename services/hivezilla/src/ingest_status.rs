//! Bounded, secret-free status for the durable live-ingest pipeline.
//!
//! This native sidecar deliberately parses only a small public projection from recorder state,
//! ACKs, gap evidence, and alert state. Unknown/private input fields never cross the boundary.

use std::{
    collections::BTreeMap,
    ffi::{CStr, CString, OsStr, OsString},
    fs::{self, File, OpenOptions},
    future::{Future, pending},
    io::{self, Read, Seek, SeekFrom},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd},
        unix::{
            ffi::{OsStrExt, OsStringExt},
            fs::{MetadataExt, OpenOptionsExt},
        },
    },
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant as MonotonicInstant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use serde::Serialize;
use serde_json::Value;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, oneshot},
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};

pub const STATUS_PATH: &str = "/api/v1/sidecars/ingest-pipeline/status.json";
pub const HEALTH_PATH: &str = "/healthz";
pub const MAX_JSON_BYTES: usize = 1024 * 1024;
pub const MAX_TAIL_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_GAPS: usize = 32;
pub const MAX_INCIDENTS: usize = 32;
pub const MAX_TREE_ENTRIES: usize = 4096;
pub const MAX_SAFE_INTEGER: u64 = (1_u64 << 53) - 1;
pub const MAX_REPLAY_RESUME_HEADROOM_SLOTS: u64 = 10_000;
pub const DEFAULT_DISK_CRITICAL_FREE_BYTES: u64 = 21_474_836_480;
pub const DEFAULT_DISK_WARNING_FREE_BYTES: u64 = 32_212_254_720;

const DEFAULT_LISTEN: &str = "127.0.0.1:8790";
const DEFAULT_INTERVAL_SECS: u64 = 10;
const DEFAULT_CAPTURE_STALE_AFTER_SECS: u64 = 60;
const DEFAULT_ACK_STALE_AFTER_SECS: u64 = 120;
const MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_FRESHNESS_THRESHOLD: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
const MAX_HTTP_REQUESTS: usize = 32;
const REQUEST_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const LISTEN_BACKLOG: i32 = 32;
const ROTATION_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const ROTATION_LOCK_RETRY: Duration = Duration::from_millis(10);
const REFRESH_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const REFRESH_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

static NEVER_CANCELLED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Args)]
pub struct ServeIngestStatusArgs {
    /// HTTP listen address. Only loopback and RFC1918 private addresses are accepted.
    #[arg(long, default_value = DEFAULT_LISTEN)]
    pub listen: String,

    /// Durable recorder cache root containing active/, sealed/, and .rotation.lock.
    #[arg(long)]
    pub cache_root: PathBuf,

    /// Atomically published pull-replication ACK status.
    #[arg(long)]
    pub ack_status_file: PathBuf,

    /// Optional immutable registry of known historical ingest gaps.
    #[arg(long)]
    pub known_gaps_file: Option<PathBuf>,

    /// Refresh interval.
    #[arg(long, default_value_t = DEFAULT_INTERVAL_SECS)]
    pub interval_secs: u64,

    /// Age after which the logical committed WAL tail is stalled.
    #[arg(long, default_value_t = DEFAULT_CAPTURE_STALE_AFTER_SECS)]
    pub capture_stale_after_secs: u64,

    /// Age after which replication ACK progress is stalled.
    #[arg(long, default_value_t = DEFAULT_ACK_STALE_AFTER_SECS)]
    pub ack_stale_after_secs: u64,

    #[arg(long, default_value_t = DEFAULT_DISK_CRITICAL_FREE_BYTES)]
    pub disk_critical_free_bytes: u64,

    #[arg(long, default_value_t = DEFAULT_DISK_WARNING_FREE_BYTES)]
    pub disk_warning_free_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct IngestStatusConfig {
    pub cache_root: PathBuf,
    pub ack_status_file: PathBuf,
    pub known_gaps_file: Option<PathBuf>,
    pub capture_stale_after: Duration,
    pub ack_stale_after: Duration,
    pub disk_critical_free_bytes: u64,
    pub disk_warning_free_bytes: u64,
    interval: Duration,
    max_http_requests: usize,
    request_header_timeout: Duration,
    rotation_lock_timeout: Duration,
    refresh_attempt_timeout: Duration,
    refresh_shutdown_grace: Duration,
}

impl IngestStatusConfig {
    fn from_args(args: ServeIngestStatusArgs) -> Result<(SocketAddr, Self)> {
        let listen = parse_listener(&args.listen)?;
        let config = Self {
            cache_root: args.cache_root,
            ack_status_file: args.ack_status_file,
            known_gaps_file: args.known_gaps_file,
            capture_stale_after: Duration::from_secs(args.capture_stale_after_secs),
            ack_stale_after: Duration::from_secs(args.ack_stale_after_secs),
            disk_critical_free_bytes: args.disk_critical_free_bytes,
            disk_warning_free_bytes: args.disk_warning_free_bytes,
            interval: Duration::from_secs(args.interval_secs),
            max_http_requests: MAX_HTTP_REQUESTS,
            request_header_timeout: REQUEST_HEADER_TIMEOUT,
            rotation_lock_timeout: ROTATION_LOCK_TIMEOUT,
            refresh_attempt_timeout: REFRESH_ATTEMPT_TIMEOUT,
            refresh_shutdown_grace: REFRESH_SHUTDOWN_GRACE,
        };
        config.validate()?;
        Ok((listen, config))
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.interval.is_zero() && self.interval <= MAX_REFRESH_INTERVAL,
            "refresh interval must be positive and at most one day"
        );
        ensure!(
            !self.capture_stale_after.is_zero()
                && self.capture_stale_after <= MAX_FRESHNESS_THRESHOLD,
            "capture stale threshold must be positive and at most one year"
        );
        ensure!(
            !self.ack_stale_after.is_zero() && self.ack_stale_after <= MAX_FRESHNESS_THRESHOLD,
            "ACK stale threshold must be positive and at most one year"
        );
        ensure!(
            self.disk_critical_free_bytes <= MAX_SAFE_INTEGER
                && self.disk_warning_free_bytes <= MAX_SAFE_INTEGER
                && self.disk_warning_free_bytes > self.disk_critical_free_bytes,
            "disk warning threshold must exceed the critical threshold and remain a safe integer"
        );
        ensure!(
            (1..=MAX_HTTP_REQUESTS).contains(&self.max_http_requests),
            "HTTP request limit is invalid"
        );
        ensure!(
            !self.request_header_timeout.is_zero()
                && self.request_header_timeout <= Duration::from_secs(60),
            "HTTP request-header timeout is invalid"
        );
        ensure!(
            !self.rotation_lock_timeout.is_zero()
                && self.rotation_lock_timeout <= Duration::from_secs(60),
            "rotation-lock timeout is invalid"
        );
        ensure!(
            !self.refresh_attempt_timeout.is_zero()
                && self.refresh_attempt_timeout <= MAX_REFRESH_INTERVAL,
            "refresh-attempt timeout is invalid"
        );
        ensure!(
            !self.refresh_shutdown_grace.is_zero()
                && self.refresh_shutdown_grace <= Duration::from_secs(60),
            "refresh shutdown grace is invalid"
        );
        Ok(())
    }

    fn maximum_healthy_age(&self) -> Duration {
        self.interval
            .saturating_add(self.refresh_attempt_timeout)
            .saturating_add(self.interval)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicIngestStatus {
    pub schema_version: u32,
    pub updated_unix_secs: u64,
    pub overall_state: String,
    pub upstream: UpstreamStatus,
    pub recorder: RecorderStatus,
    pub replication: ReplicationStatus,
    pub indexer: SlotConsumerStatus,
    pub object_store: ObjectStoreStatus,
    pub fallback: SlotConsumerStatus,
    pub gaps: Vec<Gap>,
    pub gaps_truncated: bool,
    pub incidents: Vec<Incident>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpstreamStatus {
    pub state: String,
    pub updated_unix_secs: u64,
    pub reconnects_1h: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecorderStatus {
    pub state: String,
    pub durable_slot: u64,
    pub updated_unix_secs: u64,
    pub active_bytes: u64,
    pub sealed_generations: usize,
    pub unacknowledged_bytes: u64,
    pub disk_free_bytes: u64,
    pub disk_total_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplicationStatus {
    pub state: String,
    pub ack_through_sequence: u64,
    pub ack_slot: Option<u64>,
    pub updated_unix_secs: u64,
    pub lag_records: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SlotConsumerStatus {
    pub state: String,
    pub last_slot: Option<u64>,
    pub updated_unix_secs: Option<u64>,
    pub lag_slots: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObjectStoreStatus {
    pub provider: String,
    pub state: String,
    pub committed_bytes: Option<u64>,
    pub pending_bytes: u64,
    pub updated_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Gap {
    pub from_slot: u64,
    pub to_slot: u64,
    pub produced_blocks: Option<u64>,
    pub coverage: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Incident {
    pub id: String,
    pub severity: String,
    pub started_unix_secs: u64,
    pub resolved_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamIdentity {
    cluster_id: String,
    origin_node_id: String,
    source_id: String,
    journal_id: String,
}

#[derive(Debug, Clone)]
struct Capture {
    state: &'static str,
    slot: u64,
    sequence: u64,
    updated: u64,
    active_bytes: u64,
    sealed_count: usize,
    sealed_bytes: u64,
    stream: StreamIdentity,
}

#[derive(Debug, Clone)]
struct Ack {
    state: &'static str,
    sequence: u64,
    updated: u64,
    lag: u64,
    stream: StreamIdentity,
}

#[derive(Debug, Default)]
pub struct CaptureProgressTracker {
    inner: Mutex<CaptureProgress>,
}

#[derive(Debug, Default)]
struct CaptureProgress {
    stream: Option<StreamIdentity>,
    sequence: Option<u64>,
    updated: Option<u64>,
}

impl CaptureProgressTracker {
    fn observe(
        &self,
        stream: &StreamIdentity,
        sequence: u64,
        evidence_updated: u64,
    ) -> Result<u64> {
        let mut progress = self.inner.lock().expect("capture progress mutex poisoned");
        if progress.stream.as_ref() == Some(stream) {
            if let Some(previous) = progress.sequence {
                ensure!(
                    sequence >= previous,
                    "durable journal logical sequence regressed"
                );
                if sequence == previous {
                    return progress
                        .updated
                        .context("capture progress timestamp is missing");
                }
            }
        }
        progress.stream = Some(stream.clone());
        progress.sequence = Some(sequence);
        progress.updated = Some(evidence_updated);
        Ok(evidence_updated)
    }
}

#[derive(Debug)]
struct Directory(File);

#[derive(Debug)]
struct NamedDirectory {
    name: OsString,
    directory: Directory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    Directory,
    Regular,
    Symlink,
    Special,
}

impl Directory {
    fn open(path: &Path, label: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .with_context(|| format!("open {label} {}", path.display()))?;
        ensure!(file.metadata()?.is_dir(), "{label} is not a directory");
        Ok(Self(file))
    }

    fn try_clone(&self) -> Result<Self> {
        let current = c".";
        let descriptor = unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                current.as_ptr(),
                libc::O_RDONLY
                    | libc::O_CLOEXEC
                    | libc::O_DIRECTORY
                    | libc::O_NOFOLLOW
                    | libc::O_NONBLOCK,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error()).context("reopen status directory descriptor");
        }
        Ok(Self(unsafe { File::from_raw_fd(descriptor) }))
    }

    fn metadata_at(&self, name: &OsStr) -> Result<Option<libc::stat>> {
        let name = safe_entry_name(name)?;
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        let result = unsafe {
            libc::fstatat(
                self.0.as_raw_fd(),
                name.as_ptr(),
                metadata.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if result == 0 {
            return Ok(Some(unsafe { metadata.assume_init() }));
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error).context("inspect status directory entry")
        }
    }

    fn open_directory(&self, name: &OsStr, label: &str) -> Result<Self> {
        let name = safe_entry_name(name)?;
        let descriptor = unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY
                    | libc::O_CLOEXEC
                    | libc::O_DIRECTORY
                    | libc::O_NOFOLLOW
                    | libc::O_NONBLOCK,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error()).with_context(|| format!("open {label}"));
        }
        let file = unsafe { File::from_raw_fd(descriptor) };
        ensure!(file.metadata()?.is_dir(), "{label} is not a directory");
        Ok(Self(file))
    }

    fn optional_directory(&self, name: &OsStr, label: &str) -> Result<Option<Self>> {
        let Some(metadata) = self.metadata_at(name)? else {
            return Ok(None);
        };
        ensure!(
            entry_kind(&metadata) == EntryKind::Directory,
            "{label} is not a real directory"
        );
        self.open_directory(name, label).map(Some)
    }

    fn optional_nested_directory(&self, names: &[&str], label: &str) -> Result<Option<Self>> {
        let mut current = self.try_clone()?;
        for name in names {
            let Some(next) = current.optional_directory(OsStr::new(name), label)? else {
                return Ok(None);
            };
            current = next;
        }
        Ok(Some(current))
    }

    fn open_regular(&self, name: &OsStr, label: &str) -> Result<(File, fs::Metadata)> {
        let name = safe_entry_name(name)?;
        let descriptor = unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error()).with_context(|| format!("open {label}"));
        }
        let file = unsafe { File::from_raw_fd(descriptor) };
        let metadata = file
            .metadata()
            .with_context(|| format!("inspect {label}"))?;
        ensure!(metadata.is_file(), "{label} is not a regular file");
        Ok((file, metadata))
    }

    fn entries(&self) -> Result<DirectoryStream> {
        let descriptor = self.try_clone()?.0.into_raw_fd();
        let stream = unsafe { libc::fdopendir(descriptor) };
        if stream.is_null() {
            let error = io::Error::last_os_error();
            unsafe {
                libc::close(descriptor);
            }
            return Err(error).context("open status directory stream");
        }
        Ok(DirectoryStream(stream))
    }
}

struct DirectoryStream(*mut libc::DIR);

impl DirectoryStream {
    fn next_name(&mut self) -> Result<Option<OsString>> {
        loop {
            set_errno(0);
            let entry = unsafe { libc::readdir(self.0) };
            if entry.is_null() {
                let error = errno();
                if error == 0 {
                    return Ok(None);
                }
                return Err(io::Error::from_raw_os_error(error))
                    .context("read status directory entry");
            }
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if matches!(name, b"." | b"..") {
                continue;
            }
            return Ok(Some(OsString::from_vec(name.to_vec())));
        }
    }
}

impl Drop for DirectoryStream {
    fn drop(&mut self) {
        unsafe {
            libc::closedir(self.0);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn set_errno(value: libc::c_int) {
    unsafe {
        *libc::__errno_location() = value;
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn errno() -> libc::c_int {
    unsafe { *libc::__errno_location() }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
fn set_errno(value: libc::c_int) {
    unsafe {
        *libc::__error() = value;
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
fn errno() -> libc::c_int {
    unsafe { *libc::__error() }
}

fn safe_entry_name(name: &OsStr) -> Result<CString> {
    let bytes = name.as_bytes();
    ensure!(
        !bytes.is_empty() && bytes != b"." && bytes != b".." && !bytes.contains(&b'/'),
        "unsafe status directory entry name"
    );
    CString::new(bytes).context("status directory entry contains NUL")
}

fn entry_kind(metadata: &libc::stat) -> EntryKind {
    match metadata.st_mode & libc::S_IFMT {
        libc::S_IFDIR => EntryKind::Directory,
        libc::S_IFREG => EntryKind::Regular,
        libc::S_IFLNK => EntryKind::Symlink,
        _ => EntryKind::Special,
    }
}

struct RotationLock {
    file: File,
    device: u64,
    inode: u64,
}

impl RotationLock {
    fn acquire(
        cache_root: &Directory,
        maximum_wait: Duration,
        cancelled: &AtomicBool,
    ) -> Result<Self> {
        let name = OsStr::new(".rotation.lock");
        let linked_before = cache_root
            .metadata_at(name)?
            .context("rotation lock is missing")?;
        ensure!(
            entry_kind(&linked_before) == EntryKind::Regular,
            "rotation lock is not a regular file"
        );
        let (file, opened) = cache_root.open_regular(name, "rotation lock")?;
        let linked_after = cache_root
            .metadata_at(name)?
            .context("rotation lock disappeared while it was opened")?;
        ensure!(
            entry_kind(&linked_after) == EntryKind::Regular
                && opened.dev() == linked_before.st_dev as u64
                && opened.ino() == linked_before.st_ino as u64
                && opened.dev() == linked_after.st_dev as u64
                && opened.ino() == linked_after.st_ino as u64,
            "rotation lock changed while it was opened"
        );
        let effective_uid = unsafe { libc::geteuid() };
        ensure!(
            opened.uid() == effective_uid || opened.uid() == 0,
            "rotation lock has an untrusted owner"
        );
        ensure!(opened.mode() & 0o077 == 0, "rotation lock must be private");

        let deadline = MonotonicInstant::now() + maximum_wait;
        loop {
            ensure!(
                !cancelled.load(Ordering::Acquire),
                "rotation-lock acquisition was cancelled"
            );
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) };
            if result == 0 {
                let linked_locked = cache_root
                    .metadata_at(name)?
                    .context("rotation lock disappeared while it was acquired")?;
                ensure!(
                    entry_kind(&linked_locked) == EntryKind::Regular
                        && opened.dev() == linked_locked.st_dev as u64
                        && opened.ino() == linked_locked.st_ino as u64,
                    "rotation lock changed while it was acquired"
                );
                return Ok(Self {
                    file,
                    device: opened.dev(),
                    inode: opened.ino(),
                });
            }
            let error = io::Error::last_os_error();
            if !matches!(error.kind(), io::ErrorKind::WouldBlock) {
                return Err(error).context("acquire rotation lock");
            }
            ensure!(
                MonotonicInstant::now() < deadline,
                "timed out acquiring rotation lock"
            );
            std::thread::sleep(
                ROTATION_LOCK_RETRY
                    .min(deadline.saturating_duration_since(MonotonicInstant::now())),
            );
        }
    }

    fn validate_identity(&self, cache_root: &Directory) -> Result<()> {
        let linked = cache_root
            .metadata_at(OsStr::new(".rotation.lock"))?
            .context("rotation lock disappeared while status was collected")?;
        ensure!(
            entry_kind(&linked) == EntryKind::Regular
                && self.device == linked.st_dev as u64
                && self.inode == linked.st_ino as u64,
            "rotation lock changed while status was collected"
        );
        Ok(())
    }
}

impl Drop for RotationLock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

fn safe_regular_bytes(path: &Path, maximum: usize) -> Result<(Vec<u8>, fs::Metadata)> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open status input {}", path.display()))?;
    let metadata = file.metadata().context("inspect status input")?;
    ensure!(metadata.is_file(), "status input is not a regular file");
    ensure!(
        metadata.len() <= maximum as u64,
        "status input exceeds its byte limit"
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    Read::by_ref(&mut file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .context("read status input")?;
    ensure!(
        bytes.len() <= maximum,
        "status input exceeds its byte limit"
    );
    Ok((bytes, metadata))
}

fn safe_regular_bytes_at(
    directory: &Directory,
    name: &OsStr,
    maximum: usize,
    label: &str,
) -> Result<(Vec<u8>, fs::Metadata)> {
    let (mut file, metadata) = directory.open_regular(name, label)?;
    ensure!(
        metadata.len() <= maximum as u64,
        "{label} exceeds its byte limit"
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    Read::by_ref(&mut file)
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {label}"))?;
    ensure!(bytes.len() <= maximum, "{label} exceeds its byte limit");
    Ok((bytes, metadata))
}

fn safe_json(path: &Path) -> Result<Value> {
    let (bytes, _) = safe_regular_bytes(path, MAX_JSON_BYTES)?;
    serde_json::from_slice(&bytes).context("decode bounded status JSON")
}

fn safe_json_at(directory: &Directory, name: &OsStr, label: &str) -> Result<Value> {
    let (bytes, _) = safe_regular_bytes_at(directory, name, MAX_JSON_BYTES, label)?;
    serde_json::from_slice(&bytes).context("decode bounded status JSON")
}

fn safe_tail_json_at(directory: &Directory, name: &OsStr) -> Result<(Value, fs::Metadata)> {
    let (mut file, metadata) = directory.open_regular(name, "durable journal")?;
    safe_tail_json_file(&mut file, metadata)
}

fn safe_tail_json_file(file: &mut File, metadata: fs::Metadata) -> Result<(Value, fs::Metadata)> {
    ensure!(
        metadata.is_file() && metadata.len() > 0,
        "durable journal is empty or not regular"
    );
    let start = metadata.len().saturating_sub(MAX_TAIL_BYTES as u64);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes =
        Vec::with_capacity((metadata.len() - start).min(MAX_TAIL_BYTES as u64) as usize);
    Read::by_ref(file)
        .take(MAX_TAIL_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_TAIL_BYTES,
        "durable journal tail exceeds its byte limit"
    );
    if start > 0 {
        let first_newline = bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .context("durable journal has no complete bounded JSON row")?;
        bytes.drain(..=first_newline);
    }
    let final_newline = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .context("durable journal has no newline-committed JSON row")?;
    let committed = &bytes[..final_newline];
    let row_start = committed
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |position| position + 1);
    let row = &committed[row_start..];
    ensure!(
        row.iter().any(|byte| !byte.is_ascii_whitespace()),
        "durable journal final committed row is empty"
    );
    let value: Value = serde_json::from_slice(row)
        .context("durable journal final committed row is invalid JSON")?;
    ensure!(
        value.is_object(),
        "durable journal final committed row is not an object"
    );
    Ok((value, metadata))
}

fn safe_u64(value: Option<&Value>) -> Option<u64> {
    value
        .and_then(Value::as_u64)
        .filter(|value| *value <= MAX_SAFE_INTEGER)
}

fn positive_u64(value: Option<&Value>) -> Option<u64> {
    safe_u64(value).filter(|value| *value > 0)
}

fn bounded_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.chars().count() <= 128)
        .map(ToOwned::to_owned)
}

fn journal_id_hex(value: Option<&Value>) -> Option<String> {
    let bytes = value?.as_array()?;
    if bytes.len() != 16 {
        return None;
    }
    let mut encoded = String::with_capacity(32);
    for value in bytes {
        let byte = value.as_u64().filter(|value| *value <= u8::MAX as u64)?;
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("write to String cannot fail");
    }
    Some(encoded)
}

fn metadata_unix_secs(metadata: &fs::Metadata) -> Result<u64> {
    metadata
        .modified()
        .context("read input modification time")?
        .duration_since(UNIX_EPOCH)
        .context("input timestamp predates Unix epoch")
        .map(|duration| duration.as_secs())
}

#[cfg(test)]
fn bounded_tree_bytes(root: &Path) -> Result<u64> {
    let root = Directory::open(root, "status tree root")?;
    let mut seen = 0;
    bounded_tree_bytes_at(root, &NEVER_CANCELLED, &mut seen)
}

fn bounded_tree_bytes_at(root: Directory, cancelled: &AtomicBool, seen: &mut usize) -> Result<u64> {
    let mut total = 0_u64;
    let mut pending = vec![root];
    while let Some(current) = pending.pop() {
        let mut entries = current.entries()?;
        while let Some(name) = entries.next_name()? {
            ensure!(
                !cancelled.load(Ordering::Acquire),
                "status-tree scan was cancelled"
            );
            *seen += 1;
            ensure!(
                *seen <= MAX_TREE_ENTRIES,
                "status tree exceeds its entry limit"
            );
            let metadata = current
                .metadata_at(&name)?
                .context("status tree entry disappeared while scanning")?;
            match entry_kind(&metadata) {
                EntryKind::Directory => {
                    pending.push(current.open_directory(&name, "status tree directory")?);
                }
                EntryKind::Regular => {
                    total = total
                        .checked_add(
                            metadata
                                .st_size
                                .try_into()
                                .context("status tree entry has a negative byte count")?,
                        )
                        .context("status tree byte count overflow")?;
                }
                EntryKind::Symlink => bail!("status tree contains a symlink"),
                EntryKind::Special => bail!("status tree contains a special file"),
            }
        }
    }
    Ok(total)
}

fn safe_directories_at(
    parent: &Directory,
    root_name: &OsStr,
    maximum: usize,
    cancelled: &AtomicBool,
) -> Result<Vec<NamedDirectory>> {
    let Some(root) = parent.optional_directory(root_name, "status directory")? else {
        return Ok(Vec::new());
    };
    let mut directories = Vec::new();
    let mut scanned = 0_usize;
    let mut entries = root.entries()?;
    while let Some(name) = entries.next_name()? {
        ensure!(
            !cancelled.load(Ordering::Acquire),
            "status-directory scan was cancelled"
        );
        scanned += 1;
        ensure!(
            scanned <= MAX_TREE_ENTRIES,
            "status directory exceeds its scan limit"
        );
        let metadata = root
            .metadata_at(&name)?
            .context("status directory entry disappeared while scanning")?;
        match entry_kind(&metadata) {
            EntryKind::Directory => {
                directories.push(NamedDirectory {
                    directory: root.open_directory(&name, "status child directory")?,
                    name,
                });
                ensure!(
                    directories.len() <= maximum,
                    "status directory exceeds its directory limit"
                );
            }
            EntryKind::Regular => {}
            EntryKind::Symlink => bail!("status directory contains a symlink"),
            EntryKind::Special => bail!("status directory contains a special file"),
        }
    }
    directories.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(directories)
}

#[cfg(test)]
fn read_capture(
    config: &IngestStatusConfig,
    now: u64,
    tracker: Option<&CaptureProgressTracker>,
) -> Result<Capture> {
    let cache_root = Directory::open(&config.cache_root, "cache root")?;
    read_capture_at(config, now, tracker, &cache_root, &NEVER_CANCELLED)
}

fn read_capture_at(
    config: &IngestStatusConfig,
    now: u64,
    tracker: Option<&CaptureProgressTracker>,
    cache_root: &Directory,
    cancelled: &AtomicBool,
) -> Result<Capture> {
    let rotation_lock = RotationLock::acquire(cache_root, config.rotation_lock_timeout, cancelled)?;
    let active = cache_root.open_directory(OsStr::new("active"), "active generation")?;
    let identity = safe_json_at(&active, OsStr::new("identity.json"), "capture identity")?;
    let (tail, tail_metadata) = safe_tail_json_at(&active, OsStr::new("raw-blocks.jsonl"))?;
    let identity = identity
        .as_object()
        .context("capture identity is not an object")?;
    let tail = tail
        .as_object()
        .context("durable journal tail is not an object")?;
    ensure!(
        safe_u64(identity.get("schema_version")) == Some(1),
        "capture identity has an unsupported schema"
    );
    ensure!(
        safe_u64(tail.get("schema_version")) == Some(1),
        "durable journal tail has an unsupported schema"
    );

    let frame =
        safe_u64(tail.get("frame_id")).context("durable journal tail has invalid counters")?;
    let slot = safe_u64(tail.get("slot")).context("durable journal tail has invalid counters")?;
    let cluster_id = bounded_string(identity.get("cluster_id"))
        .context("capture identity has invalid fields")?;
    let origin_node_id = bounded_string(identity.get("origin_node_id"))
        .context("capture identity has invalid fields")?;
    let source_id =
        bounded_string(identity.get("source_id")).context("capture identity has invalid fields")?;
    let physical_journal_id = journal_id_hex(identity.get("journal_id"))
        .context("capture identity has invalid fields")?;
    let replication_journal = identity
        .get("replication_journal_id")
        .filter(|value| !value.is_null());
    let sequence_base = identity
        .get("replication_sequence_base")
        .filter(|value| !value.is_null());
    ensure!(
        replication_journal.is_some() == sequence_base.is_some(),
        "capture logical replication identity is incomplete"
    );
    let logical_journal_id = match replication_journal {
        Some(value) => {
            journal_id_hex(Some(value)).context("capture identity has invalid fields")?
        }
        None => physical_journal_id,
    };
    let sequence_base = match sequence_base {
        Some(value) => safe_u64(Some(value)).context("capture identity has invalid fields")?,
        None => 0,
    };
    let sequence = sequence_base
        .checked_add(frame)
        .filter(|value| *value <= MAX_SAFE_INTEGER)
        .context("durable journal logical sequence exceeds the public integer limit")?;
    let mut scanned_tree_entries = 0_usize;
    let active_bytes =
        bounded_tree_bytes_at(active.try_clone()?, cancelled, &mut scanned_tree_entries)?;
    let sealed = safe_directories_at(cache_root, OsStr::new("sealed"), 64, cancelled)?;
    let sealed_bytes = sealed.iter().try_fold(0_u64, |total, generation| {
        total
            .checked_add(bounded_tree_bytes_at(
                generation.directory.try_clone()?,
                cancelled,
                &mut scanned_tree_entries,
            )?)
            .context("sealed generation byte count overflow")
    })?;
    let stream = StreamIdentity {
        cluster_id,
        origin_node_id,
        source_id,
        journal_id: logical_journal_id,
    };
    rotation_lock.validate_identity(cache_root)?;
    drop(rotation_lock);

    let evidence_updated = metadata_unix_secs(&tail_metadata)?;
    ensure!(
        evidence_updated <= now.saturating_add(5),
        "durable journal timestamp is in the future"
    );
    let updated = match tracker {
        Some(tracker) => tracker.observe(&stream, sequence, evidence_updated)?,
        None => evidence_updated,
    };
    let state = if now.saturating_sub(updated) <= config.capture_stale_after.as_secs() {
        "recording"
    } else {
        "stalled"
    };
    Ok(Capture {
        state,
        slot,
        sequence,
        updated,
        active_bytes,
        sealed_count: sealed.len(),
        sealed_bytes,
        stream,
    })
}

fn read_ack(path: &Path) -> Result<Ack> {
    let value = safe_json(path)?;
    let value = value.as_object().context("ACK status is not an object")?;
    ensure!(
        safe_u64(value.get("schema_version")) == Some(1),
        "ACK status has an unsupported schema"
    );
    let cluster_id = bounded_string(value.get("cluster_id"));
    let origin_node_id = bounded_string(value.get("origin_node_id"));
    let source_id = bounded_string(value.get("source_id"));
    let journal_id = value
        .get("journal_id")
        .and_then(Value::as_str)
        .filter(|value| {
            value.len() == 32
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .map(ToOwned::to_owned);
    let sequence = safe_u64(value.get("through_sequence"));
    let updated = positive_u64(value.get("updated_unix_secs"));
    let (cluster_id, origin_node_id, source_id, journal_id, sequence, updated) = match (
        cluster_id,
        origin_node_id,
        source_id,
        journal_id,
        sequence,
        updated,
    ) {
        (Some(a), Some(b), Some(c), Some(d), Some(e), Some(f)) => (a, b, c, d, e, f),
        _ => bail!("ACK status fields are invalid"),
    };
    Ok(Ack {
        state: "unclassified",
        sequence,
        updated,
        lag: 0,
        stream: StreamIdentity {
            cluster_id,
            origin_node_id,
            source_id,
            journal_id,
        },
    })
}

fn ack_for_capture(
    mut ack: Ack,
    capture: &Capture,
    now: u64,
    stale_after: Duration,
) -> Result<Ack> {
    ensure!(
        ack.stream == capture.stream,
        "ACK status belongs to a different replication stream"
    );
    ensure!(
        ack.sequence <= capture.sequence,
        "ACK status is ahead of the durable capture"
    );
    ensure!(
        ack.updated <= now.saturating_add(5),
        "ACK status timestamp is in the future"
    );
    ack.lag = capture.sequence - ack.sequence;
    ack.state = if now.saturating_sub(ack.updated) > stale_after.as_secs() {
        "stalled"
    } else if ack.lag == 0 {
        "caught_up"
    } else {
        "syncing"
    };
    Ok(ack)
}

fn gap_coverage_rank(coverage: &str) -> Option<u8> {
    match coverage {
        "unproven" => Some(0),
        "rpc_recoverable" => Some(1),
        "normalized" => Some(2),
        "raw" => Some(3),
        _ => None,
    }
}

fn gap_from_event(value: &Value) -> Result<Option<Gap>> {
    let value = value
        .as_object()
        .context("replay gap record is not an object")?;
    let schema = safe_u64(value.get("schema_version"))
        .context("replay gap record has an unsupported schema")?;
    let anchor = safe_u64(value.get("anchor_slot"));
    let requested = safe_u64(value.get("requested_slot"));
    let resume = match schema {
        1 => {
            let resume = safe_u64(value.get("available_slot"));
            match (anchor, requested, resume) {
                (Some(anchor), Some(requested), Some(resume))
                    if requested >= anchor && resume > requested =>
                {
                    resume
                }
                _ => bail!("replay gap schema 1 fields are invalid"),
            }
        }
        2 => {
            let provider = safe_u64(value.get("provider_available_slot"));
            let resume = safe_u64(value.get("selected_resume_slot"));
            match (anchor, requested, provider, resume) {
                (Some(anchor), Some(requested), Some(provider), Some(resume))
                    if requested >= anchor
                        && provider > requested
                        && resume >= provider
                        && resume - provider <= MAX_REPLAY_RESUME_HEADROOM_SLOTS =>
                {
                    resume
                }
                _ => bail!("replay gap schema 2 fields are invalid"),
            }
        }
        _ => bail!("replay gap record has an unsupported schema"),
    };
    let anchor = anchor.expect("validated replay gap anchor");
    if resume == anchor.saturating_add(1) {
        return Ok(None);
    }
    ensure!(
        resume > anchor,
        "replay gap resume slot does not follow its anchor"
    );
    Ok(Some(Gap {
        from_slot: anchor + 1,
        to_slot: resume - 1,
        produced_blocks: None,
        coverage: "unproven".into(),
    }))
}

fn merge_gap(gaps: &mut BTreeMap<(u64, u64), Gap>, gap: Gap) {
    let key = (gap.from_slot, gap.to_slot);
    match gaps.get_mut(&key) {
        None => {
            gaps.insert(key, gap);
        }
        Some(previous)
            if gap_coverage_rank(&gap.coverage) > gap_coverage_rank(&previous.coverage) =>
        {
            *previous = gap;
        }
        Some(previous) if previous.produced_blocks.is_none() && gap.produced_blocks.is_some() => {
            previous.produced_blocks = gap.produced_blocks;
        }
        Some(_) => {}
    }
}

#[derive(Debug)]
struct Gaps {
    visible: Vec<Gap>,
    truncated: bool,
    has_unproven: bool,
    has_partial: bool,
}

#[cfg(test)]
fn read_gaps(config: &IngestStatusConfig) -> Result<Gaps> {
    let cache_root = Directory::open(&config.cache_root, "cache root")?;
    read_gaps_at(config, &cache_root, &NEVER_CANCELLED)
}

fn read_gaps_at(
    config: &IngestStatusConfig,
    cache_root: &Directory,
    cancelled: &AtomicBool,
) -> Result<Gaps> {
    let mut gaps = BTreeMap::new();
    if let Some(path) = &config.known_gaps_file {
        let value =
            safe_json(path).context("configured known gap file is missing or not regular")?;
        let values = value
            .as_array()
            .filter(|values| values.len() <= MAX_TREE_ENTRIES)
            .context("known gap file is invalid or unbounded")?;
        for value in values {
            ensure!(
                !cancelled.load(Ordering::Acquire),
                "known-gap scan was cancelled"
            );
            let value = value.as_object().context("known gap entry is invalid")?;
            let start = safe_u64(value.get("from_slot"));
            let end = safe_u64(value.get("to_slot"));
            let blocks = match value.get("produced_blocks") {
                None | Some(Value::Null) => Some(None),
                value => safe_u64(value).map(Some),
            };
            let coverage = value.get("coverage").and_then(Value::as_str);
            let (start, end, blocks, coverage) = match (start, end, blocks, coverage) {
                (Some(start), Some(end), Some(blocks), Some(coverage))
                    if end >= start && gap_coverage_rank(coverage).is_some() =>
                {
                    (start, end, blocks, coverage)
                }
                _ => bail!("known gap entry has invalid fields"),
            };
            merge_gap(
                &mut gaps,
                Gap {
                    from_slot: start,
                    to_slot: end,
                    produced_blocks: blocks,
                    coverage: coverage.to_owned(),
                },
            );
        }
    }

    let active = cache_root.optional_directory(OsStr::new("active"), "active generation")?;
    let sealed = safe_directories_at(cache_root, OsStr::new("sealed"), 64, cancelled)?;
    let mut replay_directories = Vec::new();
    if let Some(directory) = cache_root
        .optional_nested_directory(&["monitoring", "replay-gaps"], "replay gap directory")?
    {
        replay_directories.push(directory);
    }
    for generation in active
        .into_iter()
        .chain(sealed.into_iter().map(|generation| generation.directory))
    {
        if let Some(directory) =
            generation.optional_directory(OsStr::new("replay-gaps"), "replay gap directory")?
        {
            replay_directories.push(directory);
        }
    }
    let mut scanned = 0_usize;
    for directory in replay_directories {
        let mut entries = directory.entries()?;
        while let Some(name) = entries.next_name()? {
            ensure!(
                !cancelled.load(Ordering::Acquire),
                "replay-gap scan was cancelled"
            );
            scanned += 1;
            ensure!(
                scanned <= MAX_TREE_ENTRIES,
                "replay gap tree exceeds its entry limit"
            );
            let metadata = directory
                .metadata_at(&name)?
                .context("replay gap entry disappeared while scanning")?;
            ensure!(
                entry_kind(&metadata) != EntryKind::Symlink,
                "replay gap directory contains a symlink"
            );
            if Path::new(&name)
                .extension()
                .and_then(|value| value.to_str())
                != Some("json")
            {
                continue;
            }
            ensure!(
                entry_kind(&metadata) == EntryKind::Regular,
                "replay gap entry is not a regular file"
            );
            if let Some(gap) =
                gap_from_event(&safe_json_at(&directory, &name, "replay gap entry")?)?
            {
                merge_gap(&mut gaps, gap);
            }
        }
    }
    let ordered: Vec<_> = gaps.into_values().collect();
    let has_unproven = ordered.iter().any(|gap| gap.coverage == "unproven");
    let has_partial = ordered
        .iter()
        .any(|gap| matches!(gap.coverage.as_str(), "normalized" | "rpc_recoverable"));
    Ok(Gaps {
        truncated: ordered.len() > MAX_GAPS,
        visible: ordered.into_iter().take(MAX_GAPS).collect(),
        has_unproven,
        has_partial,
    })
}

fn incident_public_id(raw: &str) -> Option<&'static str> {
    match raw {
        "recorder_restarting" | "grpc_stale" => Some("grpc_stale"),
        "upstream_access_blocked" => Some("upstream_access_blocked"),
        "provider_replay_gap" => Some("replay_gap"),
        "resume_coverage" => Some("resume_coverage"),
        "replay_recovery_failed" => Some("replay_recovery_failed"),
        "disk_space" | "disk_warning" | "disk_critical" => Some("disk_space"),
        "disk_check_failed" => Some("disk_check_failed"),
        "volume_invalid" => Some("volume_invalid"),
        "cache_rotation_failed" => Some("cache_rotation_failed"),
        "generation_rotation_failed" => Some("generation_rotation_failed"),
        "generation_backlog" => Some("generation_backlog"),
        "primary_sync_stale" => Some("receiver_ack_stale"),
        "generation_upload_failed"
        | "r2_usage_check_failed"
        | "r2_usage"
        | "b2_usage_check_failed"
        | "b2_usage"
        | "b2_usage_warning"
        | "b2_usage_critical" => Some("object_store"),
        _ => None,
    }
}

fn incident_severity_at(
    directory: &Directory,
    name: &OsStr,
) -> Result<(&'static str, fs::Metadata)> {
    let (bytes, metadata) = safe_regular_bytes_at(directory, name, 4096, "incident active state")?;
    let heading = bytes
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    let severity = match heading {
        b"Blockzilla backup - WARNING" => "warning",
        b"Blockzilla backup - ERROR" => "error",
        b"Blockzilla backup - CRITICAL" => "critical",
        _ => bail!("incident active state has an invalid heading"),
    };
    Ok((severity, metadata))
}

fn incident_started_at(directory: &Directory, raw_id: &str, fallback: u64) -> Result<u64> {
    let delivered = OsString::from(format!("{raw_id}.delivered"));
    let Some(metadata) = directory.metadata_at(&delivered)? else {
        return Ok(fallback.max(1));
    };
    ensure!(
        entry_kind(&metadata) == EntryKind::Regular,
        "incident delivery state is not a regular file"
    );
    let (bytes, _) = safe_regular_bytes_at(directory, &delivered, 64, "incident delivery state")?;
    let text = std::str::from_utf8(&bytes).context("incident delivery state is invalid")?;
    let mut parts = text.split_ascii_whitespace();
    let timestamp = parts.next();
    let severity = parts.next();
    ensure!(
        parts.next().is_none() && matches!(severity, Some("WARNING" | "ERROR" | "CRITICAL")),
        "incident delivery state is invalid"
    );
    let started = timestamp
        .context("incident delivery timestamp is invalid")?
        .parse::<u64>()
        .context("incident delivery timestamp is invalid")?;
    ensure!(started > 0, "incident delivery timestamp is invalid");
    Ok(started)
}

fn severity_rank(severity: &str) -> u8 {
    match severity {
        "warning" => 1,
        "error" => 2,
        "critical" => 3,
        _ => 0,
    }
}

fn read_incidents_at(
    cache_root: &Directory,
    now: u64,
    cancelled: &AtomicBool,
) -> Result<Vec<Incident>> {
    let Some(directory) = cache_root
        .optional_nested_directory(&["monitoring", "telegram-alerts"], "incident directory")?
    else {
        return Ok(Vec::new());
    };
    let mut incidents = BTreeMap::<String, Incident>::new();
    let mut scanned = 0_usize;
    let mut entries = directory.entries()?;
    while let Some(name) = entries.next_name()? {
        ensure!(
            !cancelled.load(Ordering::Acquire),
            "incident scan was cancelled"
        );
        scanned += 1;
        ensure!(
            scanned <= MAX_TREE_ENTRIES,
            "incident directory exceeds its entry limit"
        );
        let metadata = directory
            .metadata_at(&name)?
            .context("incident entry disappeared while scanning")?;
        ensure!(
            entry_kind(&metadata) != EntryKind::Symlink,
            "incident directory contains a symlink"
        );
        let Some(name) = name.to_str() else { continue };
        let Some(raw_id) = name.strip_suffix(".active") else {
            continue;
        };
        if entry_kind(&metadata) != EntryKind::Regular {
            continue;
        }
        let Some(public_id) = incident_public_id(raw_id) else {
            continue;
        };
        let (severity, active_metadata) = incident_severity_at(&directory, OsStr::new(name))?;
        let fallback = metadata_unix_secs(&active_metadata)?;
        let started = incident_started_at(&directory, raw_id, fallback)?;
        ensure!(
            (1..=MAX_SAFE_INTEGER).contains(&started) && started <= now.saturating_add(5),
            "incident start timestamp is invalid or in the future"
        );
        if let Some(previous) = incidents.get_mut(public_id) {
            previous.started_unix_secs = previous.started_unix_secs.min(started);
            if severity_rank(severity) > severity_rank(&previous.severity) {
                previous.severity = severity.into();
            }
        } else if incidents.len() < MAX_INCIDENTS {
            incidents.insert(
                public_id.into(),
                Incident {
                    id: public_id.into(),
                    severity: severity.into(),
                    started_unix_secs: started,
                    resolved_unix_secs: None,
                },
            );
        }
    }
    let mut incidents: Vec<_> = incidents.into_values().collect();
    incidents.sort_by(|left, right| {
        (left.started_unix_secs, &left.id).cmp(&(right.started_unix_secs, &right.id))
    });
    Ok(incidents)
}

fn disk_stats_at(path: &Directory) -> Result<(u64, u64)> {
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::fstatvfs(path.0.as_raw_fd(), stats.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error()).context("inspect cache filesystem capacity");
    }
    let stats = unsafe { stats.assume_init() };
    let block_size = stats.f_frsize as u64;
    let free = (stats.f_bavail as u64)
        .checked_mul(block_size)
        .context("filesystem free-byte count overflow")?;
    let total = (stats.f_blocks as u64)
        .checked_mul(block_size)
        .context("filesystem total-byte count overflow")?;
    Ok((free, total))
}

pub fn build_status(
    config: &IngestStatusConfig,
    now: u64,
    tracker: Option<&CaptureProgressTracker>,
) -> Result<PublicIngestStatus> {
    build_status_cancellable(config, now, tracker, &NEVER_CANCELLED)
}

fn build_status_cancellable(
    config: &IngestStatusConfig,
    now: u64,
    tracker: Option<&CaptureProgressTracker>,
    cancelled: &AtomicBool,
) -> Result<PublicIngestStatus> {
    config.validate()?;
    ensure!(
        config.cache_root != Path::new("/"),
        "cache root must be non-root"
    );
    let cache_root = Directory::open(&config.cache_root, "cache root")?;
    build_status_cancellable_at(config, now, tracker, cancelled, &cache_root)
}

fn build_status_cancellable_at(
    config: &IngestStatusConfig,
    now: u64,
    tracker: Option<&CaptureProgressTracker>,
    cancelled: &AtomicBool,
    cache_root: &Directory,
) -> Result<PublicIngestStatus> {
    build_status_with_root(
        config,
        now,
        || read_capture_at(config, now, tracker, cache_root, cancelled),
        || disk_stats_at(cache_root),
        cache_root,
        cancelled,
    )
}

#[cfg(test)]
fn build_status_with<C, D>(
    config: &IngestStatusConfig,
    now: u64,
    capture_reader: C,
    disk_reader: D,
) -> Result<PublicIngestStatus>
where
    C: FnMut() -> Result<Capture>,
    D: FnOnce() -> Result<(u64, u64)>,
{
    config.validate()?;
    ensure!(
        config.cache_root != Path::new("/"),
        "cache root must be non-root"
    );
    let cache_root = Directory::open(&config.cache_root, "cache root")?;
    build_status_with_root(
        config,
        now,
        capture_reader,
        disk_reader,
        &cache_root,
        &NEVER_CANCELLED,
    )
}

fn build_status_with_root<C, D>(
    config: &IngestStatusConfig,
    now: u64,
    mut capture_reader: C,
    disk_reader: D,
    cache_root: &Directory,
    cancelled: &AtomicBool,
) -> Result<PublicIngestStatus>
where
    C: FnMut() -> Result<Capture>,
    D: FnOnce() -> Result<(u64, u64)>,
{
    ensure!(
        !cancelled.load(Ordering::Acquire),
        "status refresh was cancelled"
    );
    let mut capture = capture_reader()?;
    let raw_ack = read_ack(&config.ack_status_file)?;
    if raw_ack.stream != capture.stream || raw_ack.sequence > capture.sequence {
        capture = capture_reader()?;
    }
    let ack = ack_for_capture(raw_ack, &capture, now, config.ack_stale_after)?;
    let (free_bytes, total_bytes) = disk_reader()?;
    let gaps = read_gaps_at(config, cache_root, cancelled)?;
    let incidents = read_incidents_at(cache_root, now, cancelled)?;
    ensure!(
        !cancelled.load(Ordering::Acquire),
        "status refresh was cancelled"
    );
    let incident_failure = incidents
        .iter()
        .any(|incident| matches!(incident.severity.as_str(), "error" | "critical"));
    let disk_critical = free_bytes < config.disk_critical_free_bytes;
    let disk_warning = free_bytes < config.disk_warning_free_bytes;
    let has_error = capture.state == "stalled"
        || ack.state == "stalled"
        || incident_failure
        || disk_critical
        || gaps.has_unproven;
    let has_warning =
        ack.state == "syncing" || !incidents.is_empty() || disk_warning || gaps.has_partial;
    let overall_state = if has_error {
        "failed"
    } else if has_warning {
        "degraded"
    } else {
        "healthy"
    };
    let unacknowledged_bytes = if ack.lag > 0 {
        capture
            .sealed_bytes
            .checked_add(capture.active_bytes)
            .context("unacknowledged byte count overflow")?
    } else {
        capture.sealed_bytes
    };
    let unavailable_consumer = || SlotConsumerStatus {
        state: "unavailable".into(),
        last_slot: None,
        updated_unix_secs: None,
        lag_slots: None,
    };
    Ok(PublicIngestStatus {
        schema_version: 1,
        updated_unix_secs: now,
        overall_state: overall_state.into(),
        upstream: UpstreamStatus {
            state: if capture.state == "recording" {
                "connected"
            } else {
                "stalled"
            }
            .into(),
            updated_unix_secs: capture.updated,
            reconnects_1h: None,
        },
        recorder: RecorderStatus {
            state: capture.state.into(),
            durable_slot: capture.slot,
            updated_unix_secs: capture.updated,
            active_bytes: capture.active_bytes,
            sealed_generations: capture.sealed_count,
            unacknowledged_bytes,
            disk_free_bytes: free_bytes,
            disk_total_bytes: total_bytes,
        },
        replication: ReplicationStatus {
            state: ack.state.into(),
            ack_through_sequence: ack.sequence,
            ack_slot: (ack.lag == 0).then_some(capture.slot),
            updated_unix_secs: ack.updated,
            lag_records: ack.lag,
        },
        indexer: unavailable_consumer(),
        object_store: ObjectStoreStatus {
            provider: "r2".into(),
            state: "unavailable".into(),
            committed_bytes: None,
            pending_bytes: 0,
            updated_unix_secs: None,
        },
        fallback: unavailable_consumer(),
        gaps: gaps.visible,
        gaps_truncated: gaps.truncated,
        incidents,
    })
}

#[derive(Debug, Default)]
struct CachedStatus {
    body: Option<Arc<[u8]>>,
    refresh_failed: bool,
    last_success: Option<MonotonicInstant>,
}

#[derive(Debug)]
struct StatusCache {
    inner: RwLock<CachedStatus>,
    maximum_healthy_age: Duration,
}

impl Default for StatusCache {
    fn default() -> Self {
        Self::with_maximum_healthy_age(
            Duration::from_secs(DEFAULT_INTERVAL_SECS)
                .saturating_add(REFRESH_ATTEMPT_TIMEOUT)
                .saturating_add(Duration::from_secs(DEFAULT_INTERVAL_SECS)),
        )
    }
}

impl StatusCache {
    fn with_maximum_healthy_age(maximum_healthy_age: Duration) -> Self {
        Self {
            inner: RwLock::new(CachedStatus::default()),
            maximum_healthy_age,
        }
    }

    fn commit(&self, body: Vec<u8>) {
        let mut cache = self.inner.write().expect("ingest status cache poisoned");
        cache.body = Some(Arc::from(body));
        cache.refresh_failed = false;
        cache.last_success = Some(MonotonicInstant::now());
    }

    fn mark_refresh_failed(&self) {
        self.inner
            .write()
            .expect("ingest status cache poisoned")
            .refresh_failed = true;
    }

    fn body(&self) -> Option<Arc<[u8]>> {
        self.inner
            .read()
            .expect("ingest status cache poisoned")
            .body
            .clone()
    }

    fn healthy(&self) -> bool {
        let cache = self.inner.read().expect("ingest status cache poisoned");
        cache.body.is_some()
            && !cache.refresh_failed
            && cache.last_success.is_some_and(|last_success| {
                MonotonicInstant::now().saturating_duration_since(last_success)
                    <= self.maximum_healthy_age
            })
    }
}

#[derive(Debug, Clone)]
struct IngestStatusService {
    config: Arc<IngestStatusConfig>,
    cache_root: Arc<Directory>,
    cache: Arc<StatusCache>,
    tracker: Arc<CaptureProgressTracker>,
    cancelled: Arc<AtomicBool>,
}

struct AbortTaskOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortTaskOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl IngestStatusService {
    fn new(config: IngestStatusConfig) -> Result<Self> {
        config.validate()?;
        ensure!(
            config.cache_root != Path::new("/"),
            "cache root must be non-root"
        );
        let cache_root = Arc::new(Directory::open(&config.cache_root, "cache root")?);
        let maximum_healthy_age = config.maximum_healthy_age();
        Ok(Self {
            config: Arc::new(config),
            cache_root,
            cache: Arc::new(StatusCache::with_maximum_healthy_age(maximum_healthy_age)),
            tracker: Arc::new(CaptureProgressTracker::default()),
            cancelled: Arc::new(AtomicBool::new(false)),
        })
    }

    fn refresh_once_blocking(&self, cancelled: &AtomicBool) -> Result<PublicIngestStatus> {
        let result = (|| {
            let status = build_status_cancellable_at(
                &self.config,
                unix_time_secs()?,
                Some(self.tracker.as_ref()),
                cancelled,
                &self.cache_root,
            )?;
            let encoded = serde_json::to_vec(&status).context("encode public ingest status")?;
            ensure!(
                !encoded.is_empty() && encoded.len() <= MAX_JSON_BYTES,
                "public ingest status exceeds its byte limit"
            );
            ensure!(
                !cancelled.load(Ordering::Acquire),
                "status refresh was cancelled before commit"
            );
            self.cache.commit(encoded);
            Ok(status)
        })();
        if result.is_err() {
            self.cache.mark_refresh_failed();
        }
        result
    }

    async fn refresh_once_async(&self, cancelled: Arc<AtomicBool>) -> Result<PublicIngestStatus> {
        let service = self.clone();
        let mut worker = AbortTaskOnDrop(tokio::task::spawn_blocking(move || {
            service.refresh_once_blocking(cancelled.as_ref())
        }));
        let result = (&mut worker.0).await;
        match result {
            Ok(result) => result,
            Err(error) => {
                self.cache.mark_refresh_failed();
                Err(error).context("join ingest-status refresh worker")
            }
        }
    }

    async fn run(self, listen: SocketAddr) -> Result<()> {
        let shutdown = shutdown_signal()?;
        self.run_until(listen, shutdown).await
    }

    async fn run_until<F>(self, listen: SocketAddr, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        let mut shutdown = Box::pin(shutdown);
        let initial_cancelled = Arc::clone(&self.cancelled);
        let initial = self.refresh_once_async(Arc::clone(&initial_cancelled));
        tokio::pin!(initial);
        tokio::select! {
            result = &mut initial => {
                result?;
            },
            _ = &mut shutdown => {
                initial_cancelled.store(true, Ordering::Release);
                let _ = timeout(self.config.refresh_shutdown_grace, &mut initial).await;
                return Ok(());
            },
            _ = tokio::time::sleep(self.config.refresh_attempt_timeout) => {
                initial_cancelled.store(true, Ordering::Release);
                self.cache.mark_refresh_failed();
                let _ = timeout(self.config.refresh_shutdown_grace, &mut initial).await;
                bail!("initial ingest-status refresh timed out");
            }
        }
        let listener = bind_listener(listen)?;
        let refresh_service = self.clone();
        let (stop_refresh, refresh_stopped) = oneshot::channel();
        let mut refresh_task = tokio::spawn(async move {
            refresh_loop(refresh_service, refresh_stopped).await;
        });
        let result = serve_http(
            listener,
            Arc::clone(&self.cache),
            self.config.max_http_requests,
            self.config.request_header_timeout,
            shutdown,
        )
        .await;
        self.cancelled.store(true, Ordering::Release);
        let _ = stop_refresh.send(());
        if timeout(self.config.refresh_shutdown_grace, &mut refresh_task)
            .await
            .is_err()
        {
            refresh_task.abort();
        }
        result
    }
}

pub async fn serve_ingest_status(args: ServeIngestStatusArgs) -> Result<()> {
    let (listen, config) = IngestStatusConfig::from_args(args)?;
    IngestStatusService::new(config)?.run(listen).await
}

async fn refresh_loop(service: IngestStatusService, mut stopped: oneshot::Receiver<()>) {
    let start = Instant::now() + service.config.interval;
    let mut ticks = interval_at(start, service.config.interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = &mut stopped => return,
            _ = ticks.tick() => {
                let cancelled = Arc::clone(&service.cancelled);
                let refresh = service.refresh_once_async(Arc::clone(&cancelled));
                tokio::pin!(refresh);
                tokio::select! {
                    _ = &mut stopped => {
                        cancelled.store(true, Ordering::Release);
                        let _ = timeout(service.config.refresh_shutdown_grace, &mut refresh).await;
                        return;
                    }
                    result = &mut refresh => {
                        if result.is_err() {
                            // Errors can contain private paths. The public log records only the event.
                            tracing::warn!("ingest-status refresh failed");
                        }
                    }
                    _ = tokio::time::sleep(service.config.refresh_attempt_timeout) => {
                        cancelled.store(true, Ordering::Release);
                        service.cache.mark_refresh_failed();
                        let _ = timeout(service.config.refresh_shutdown_grace, &mut refresh).await;
                        tracing::warn!("ingest-status refresh timed out; refresh loop stopped");
                        return;
                    }
                }
            }
        }
    }
}

fn unix_time_secs() -> Result<u64> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs();
    ensure!(
        (1..=MAX_SAFE_INTEGER).contains(&value),
        "system time exceeds the public integer range"
    );
    Ok(value)
}

#[cfg(unix)]
fn shutdown_signal() -> Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    Ok(async move {
        tokio::select! {
            interrupted = tokio::signal::ctrl_c() => {
                if interrupted.is_err() {
                    if terminate.recv().await.is_none() {
                        pending::<()>().await;
                    }
                }
            }
            terminated = terminate.recv() => {
                if terminated.is_none() && tokio::signal::ctrl_c().await.is_err() {
                    pending::<()>().await;
                }
            }
        }
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> Result<impl Future<Output = ()>> {
    Ok(async {
        if tokio::signal::ctrl_c().await.is_err() {
            pending::<()>().await;
        }
    })
}

pub fn parse_listener(value: &str) -> Result<SocketAddr> {
    ensure!(
        !value.contains(['\r', '\n']),
        "listen address contains a line break"
    );
    let address = if let Some(port) = value.strip_prefix("localhost:") {
        let port = port.parse::<u16>().context("listen port is invalid")?;
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    } else {
        value
            .parse::<SocketAddr>()
            .context("listen must use localhost or an explicit IP address and port")?
    };
    ensure!(
        !address.ip().is_unspecified(),
        "listen must not use a wildcard address"
    );
    let allowed = match address.ip() {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback(),
    };
    ensure!(allowed, "listen must use a loopback or private address");
    Ok(address)
}

fn bind_listener(address: SocketAddr) -> Result<TcpListener> {
    let domain = if address.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .context("create ingest-status listener")?;
    socket
        .set_reuse_address(true)
        .context("set ingest-status address reuse")?;
    socket
        .set_nonblocking(true)
        .context("set ingest-status listener nonblocking")?;
    socket
        .bind(&address.into())
        .context("bind ingest-status listener")?;
    socket
        .listen(LISTEN_BACKLOG)
        .context("listen for ingest-status requests")?;
    let listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(listener).context("register ingest-status listener")
}

async fn serve_http<F>(
    listener: TcpListener,
    cache: Arc<StatusCache>,
    max_requests: usize,
    request_header_timeout: Duration,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()>,
{
    let slots = Arc::new(Semaphore::new(max_requests));
    let mut tasks = JoinSet::new();
    let shutdown: Pin<Box<F>> = Box::pin(shutdown);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("accept ingest-status connection")?;
                let Ok(permit) = Arc::clone(&slots).try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let cache = Arc::clone(&cache);
                tasks.spawn(async move {
                    let _permit = permit;
                    let _ = stream.set_nodelay(true);
                    let _ = timeout(
                        request_header_timeout,
                        handle_connection(stream, cache),
                    )
                    .await;
                });
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(())
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    target: String,
}

#[derive(Debug)]
enum ReadRequestError {
    Io(io::Error),
    Malformed,
    TooLarge,
}

impl From<io::Error> for ReadRequestError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

async fn handle_connection(stream: TcpStream, cache: Arc<StatusCache>) -> Result<()> {
    let response = match read_http_request(&stream).await {
        Ok(request) => route_request(&request, &cache),
        Err(ReadRequestError::TooLarge) => HttpResponse::json(
            431,
            "Request Header Fields Too Large",
            b"{\"error\":\"request headers too large\"}",
            true,
        ),
        Err(ReadRequestError::Malformed) => HttpResponse::json(
            400,
            "Bad Request",
            b"{\"error\":\"malformed request\"}",
            true,
        ),
        Err(ReadRequestError::Io(error)) => return Err(error).context("read HTTP request"),
    };
    write_all_nonblocking(&stream, &response.encode()).await?;
    Ok(())
}

async fn read_http_request(
    stream: &TcpStream,
) -> std::result::Result<HttpRequest, ReadRequestError> {
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 4096];
    loop {
        stream.readable().await?;
        let count = match stream.try_read(&mut chunk) {
            Ok(0) => return Err(ReadRequestError::Malformed),
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error.into()),
        };
        if buffer.len().saturating_add(count) > MAX_HTTP_HEADER_BYTES {
            return Err(ReadRequestError::TooLarge);
        }
        buffer.extend_from_slice(&chunk[..count]);
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            if end + 4 != buffer.len() {
                return Err(ReadRequestError::Malformed);
            }
            return parse_http_request(&buffer);
        }
    }
}

fn parse_http_request(raw: &[u8]) -> std::result::Result<HttpRequest, ReadRequestError> {
    let Some(header_end) = raw.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Err(ReadRequestError::Malformed);
    };
    if header_end + 4 != raw.len() {
        return Err(ReadRequestError::Malformed);
    }

    let text = std::str::from_utf8(&raw[..header_end]).map_err(|_| ReadRequestError::Malformed)?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().ok_or(ReadRequestError::Malformed)?;
    let mut request_parts = request_line.split(' ');
    let method = request_parts.next().ok_or(ReadRequestError::Malformed)?;
    let target = request_parts.next().ok_or(ReadRequestError::Malformed)?;
    let version = request_parts.next().ok_or(ReadRequestError::Malformed)?;
    if request_parts.next().is_some()
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
        || method.is_empty()
        || !method.bytes().all(|byte| byte.is_ascii_uppercase())
        || target.is_empty()
        || !target.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(ReadRequestError::Malformed);
    }

    let mut host_seen = false;
    let mut content_length_seen = false;
    for line in lines {
        if line.is_empty() || line.starts_with([' ', '\t']) {
            return Err(ReadRequestError::Malformed);
        }
        let (name, value) = line.split_once(':').ok_or(ReadRequestError::Malformed)?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || value
                .bytes()
                .any(|byte| (byte.is_ascii_control() && byte != b'\t') || byte == 0x7f)
        {
            return Err(ReadRequestError::Malformed);
        }

        let value = value.trim_matches(|character| matches!(character, ' ' | '\t'));
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(ReadRequestError::Malformed);
        }
        if name.eq_ignore_ascii_case("content-length") {
            if content_length_seen
                || value.is_empty()
                || !value.bytes().all(|byte| byte.is_ascii_digit())
                || value
                    .parse::<u64>()
                    .map_err(|_| ReadRequestError::Malformed)?
                    != 0
            {
                return Err(ReadRequestError::Malformed);
            }
            content_length_seen = true;
        }
        if name.eq_ignore_ascii_case("host") {
            if host_seen
                || value.is_empty()
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_graphic() && byte != b',')
            {
                return Err(ReadRequestError::Malformed);
            }
            host_seen = true;
        }
    }
    if version == "HTTP/1.1" && !host_seen {
        return Err(ReadRequestError::Malformed);
    }
    Ok(HttpRequest {
        method: method.into(),
        target: target.into(),
    })
}

async fn write_all_nonblocking(stream: &TcpStream, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.writable().await?;
        match stream.try_write(bytes) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            Ok(count) => bytes = &bytes[count..],
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    reason: &'static str,
    body: Arc<[u8]>,
    send_body: bool,
}

impl HttpResponse {
    fn json(status: u16, reason: &'static str, body: &[u8], send_body: bool) -> Self {
        Self {
            status,
            reason,
            body: Arc::from(body),
            send_body,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n",
            self.status,
            self.reason,
            self.body.len()
        );
        response.push_str("Cache-Control: no-store\r\n");
        response
            .push_str("Content-Security-Policy: default-src 'none'; frame-ancestors 'none'\r\n");
        response.push_str("X-Content-Type-Options: nosniff\r\n");
        response.push_str("X-Frame-Options: DENY\r\n");
        response.push_str("Connection: close\r\n\r\n");
        let mut bytes = response.into_bytes();
        if self.send_body {
            bytes.extend_from_slice(&self.body);
        }
        bytes
    }
}

fn route_request(request: &HttpRequest, cache: &StatusCache) -> HttpResponse {
    let send_body = request.method != "HEAD";
    if !request_target_is_safe(&request.target) {
        return HttpResponse::json(
            400,
            "Bad Request",
            b"{\"error\":\"invalid request target\"}",
            send_body,
        );
    }
    if !matches!(request.method.as_str(), "GET" | "HEAD") {
        return HttpResponse::json(
            405,
            "Method Not Allowed",
            b"{\"error\":\"read-only status service\"}",
            true,
        );
    }
    match request.target.as_str() {
        HEALTH_PATH => {
            let healthy = cache.healthy();
            HttpResponse::json(
                if healthy { 200 } else { 503 },
                if healthy { "OK" } else { "Service Unavailable" },
                if healthy {
                    b"{\"ok\":true}"
                } else {
                    b"{\"ok\":false}"
                },
                send_body,
            )
        }
        STATUS_PATH => match cache.body() {
            Some(body) => HttpResponse {
                status: 200,
                reason: "OK",
                body,
                send_body,
            },
            None => HttpResponse::json(
                503,
                "Service Unavailable",
                b"{\"error\":\"status snapshot unavailable\"}",
                send_body,
            ),
        },
        _ => HttpResponse::json(
            404,
            "Not Found",
            b"{\"error\":\"unknown status endpoint\"}",
            send_body,
        ),
    }
}

fn request_target_is_safe(target: &str) -> bool {
    target.starts_with('/') && !target.starts_with("//") && !target.contains(['?', '#', '\r', '\n'])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        io::Write as _,
        os::unix::fs::{PermissionsExt, symlink},
    };
    use tempfile::TempDir;

    struct Fixture {
        _root: TempDir,
        cache: PathBuf,
        active: PathBuf,
        ack: PathBuf,
        gaps: PathBuf,
        journal: PathBuf,
        now: u64,
    }

    impl Fixture {
        fn new() -> Self {
            let root = TempDir::new().unwrap();
            let cache = root.path().join("cache");
            let active = cache.join("active");
            let ack = root.path().join("pull-ack-status.json");
            let gaps = root.path().join("known-gaps.json");
            let journal = active.join("raw-blocks.jsonl");
            let now = 2_000;
            fs::create_dir_all(active.join("wal")).unwrap();
            fs::create_dir(cache.join("sealed")).unwrap();
            fs::create_dir(cache.join("receipts")).unwrap();
            let rotation_lock = cache.join(".rotation.lock");
            fs::write(&rotation_lock, b"").unwrap();
            fs::set_permissions(&rotation_lock, fs::Permissions::from_mode(0o600)).unwrap();
            write_json(
                &active.join("identity.json"),
                &json!({
                    "schema_version": 1,
                    "endpoint": "https://must-not-leak.invalid",
                    "cluster_id": "solana-mainnet",
                    "origin_node_id": "hetzner-recorder",
                    "source_id": "triton-blocks",
                    "journal_id": vec![1; 16],
                    "replication_journal_id": vec![2; 16],
                    "replication_sequence_base": 100,
                }),
            );
            fs::write(
                &journal,
                format!(
                    "{}\n{}\n",
                    json!({
                        "schema_version": 1,
                        "frame_id": 4,
                        "slot": 433_000_004_u64,
                        "blockhash": "must-not-leak",
                    }),
                    json!({
                        "schema_version": 1,
                        "frame_id": 5,
                        "slot": 433_000_005_u64,
                        "blockhash": "must-not-leak",
                    })
                ),
            )
            .unwrap();
            fs::write(active.join("wal/segment-0"), b"durable-payload").unwrap();
            write_json(
                &ack,
                &json!({
                    "schema_version": 1,
                    "cluster_id": "solana-mainnet",
                    "origin_node_id": "hetzner-recorder",
                    "source_id": "triton-blocks",
                    "journal_id": "02".repeat(16),
                    "through_sequence": 105,
                    "updated_unix_secs": now,
                }),
            );
            write_json(
                &gaps,
                &json!([{
                    "from_slot": 432_900_000_u64,
                    "to_slot": 432_900_010_u64,
                    "produced_blocks": 10,
                    "coverage": "rpc_recoverable",
                    "private_note": "must-not-leak",
                }]),
            );
            let receipt = cache.join("receipts/slot-1.json");
            write_json(
                &receipt,
                &json!({
                    "total_bytes": 1234,
                    "remote_key": "must-not-leak",
                    "sha256": "must-not-leak",
                }),
            );
            let alerts = cache.join("monitoring/telegram-alerts");
            fs::create_dir_all(&alerts).unwrap();
            let active_alert = alerts.join("disk_space.active");
            fs::write(
                &active_alert,
                b"Blockzilla backup - WARNING\nprivate alert body\n",
            )
            .unwrap();
            fs::write(alerts.join("disk_space.delivered"), b"1900 WARNING\n").unwrap();
            fs::write(alerts.join("disk_space.level"), b"CRITICAL\n").unwrap();
            for path in [&journal, &ack, &receipt, &active_alert] {
                set_mtime(path, now);
            }
            Self {
                _root: root,
                cache,
                active,
                ack,
                gaps,
                journal,
                now,
            }
        }

        fn config(&self) -> IngestStatusConfig {
            IngestStatusConfig {
                cache_root: self.cache.clone(),
                ack_status_file: self.ack.clone(),
                known_gaps_file: Some(self.gaps.clone()),
                capture_stale_after: Duration::from_secs(60),
                ack_stale_after: Duration::from_secs(120),
                disk_critical_free_bytes: 1,
                disk_warning_free_bytes: 2,
                interval: Duration::from_secs(10),
                max_http_requests: MAX_HTTP_REQUESTS,
                request_header_timeout: REQUEST_HEADER_TIMEOUT,
                rotation_lock_timeout: ROTATION_LOCK_TIMEOUT,
                refresh_attempt_timeout: REFRESH_ATTEMPT_TIMEOUT,
                refresh_shutdown_grace: REFRESH_SHUTDOWN_GRACE,
            }
        }

        fn ack_value(&self) -> Value {
            safe_json(&self.ack).unwrap()
        }

        fn write_ack(&self, value: &Value) {
            write_json(&self.ack, value);
        }

        fn clear_incidents(&self) {
            let alerts = self.cache.join("monitoring/telegram-alerts");
            for entry in fs::read_dir(alerts).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().and_then(|value| value.to_str()) == Some("active") {
                    fs::remove_file(path).unwrap();
                }
            }
        }
    }

    fn write_json(path: &Path, value: &Value) {
        fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    }

    fn set_mtime(path: &Path, seconds: u64) {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let times = [
            libc::timespec {
                tv_sec: seconds as libc::time_t,
                tv_nsec: 0,
            },
            libc::timespec {
                tv_sec: seconds as libc::time_t,
                tv_nsec: 0,
            },
        ];
        let result = unsafe {
            libc::utimensat(
                libc::AT_FDCWD,
                path.as_ptr(),
                times.as_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        assert_eq!(
            result,
            0,
            "failed to set fixture mtime: {}",
            io::Error::last_os_error()
        );
    }

    fn append(path: &Path, bytes: &[u8]) {
        OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(bytes)
            .unwrap();
    }

    #[test]
    fn builds_the_bounded_secret_free_public_contract() {
        let fixture = Fixture::new();
        let status = build_status(&fixture.config(), fixture.now, None).unwrap();
        assert_eq!(status.schema_version, 1);
        assert_eq!(status.overall_state, "degraded");
        assert_eq!(status.recorder.durable_slot, 433_000_005);
        assert_eq!(status.replication.ack_through_sequence, 105);
        assert_eq!(status.replication.lag_records, 0);
        assert_eq!(status.recorder.unacknowledged_bytes, 0);
        assert_eq!(status.object_store.state, "unavailable");
        assert_eq!(status.indexer.state, "unavailable");
        assert_eq!(status.fallback.state, "unavailable");
        assert_eq!(
            status.gaps,
            vec![Gap {
                from_slot: 432_900_000,
                to_slot: 432_900_010,
                produced_blocks: Some(10),
                coverage: "rpc_recoverable".into(),
            }]
        );
        assert!(!status.gaps_truncated);
        assert_eq!(status.incidents[0].id, "disk_space");
        assert_eq!(status.incidents[0].severity, "warning");
        assert_eq!(status.incidents[0].started_unix_secs, 1900);
        let encoded = serde_json::to_string(&status).unwrap();
        for private in [
            "endpoint",
            "journal_id",
            "blockhash",
            "remote_key",
            "sha256",
            "private alert",
            "must-not-leak",
        ] {
            assert!(!encoded.contains(private));
        }
    }

    #[test]
    fn stale_and_future_capture_or_ack_fail_visibly() {
        let fixture = Fixture::new();
        set_mtime(&fixture.journal, 1_000);
        let mut ack = fixture.ack_value();
        ack["updated_unix_secs"] = json!(1_000);
        fixture.write_ack(&ack);
        let status = build_status(&fixture.config(), 2_000, None).unwrap();
        assert_eq!(status.overall_state, "failed");
        assert_eq!(status.upstream.state, "stalled");
        assert_eq!(status.recorder.state, "stalled");
        assert_eq!(status.replication.state, "stalled");

        set_mtime(&fixture.journal, 2_006);
        assert!(
            format!(
                "{:#}",
                build_status(&fixture.config(), 2_000, None).unwrap_err()
            )
            .contains("journal timestamp is in the future")
        );
        set_mtime(&fixture.journal, 2_000);
        ack["updated_unix_secs"] = json!(2_006);
        fixture.write_ack(&ack);
        assert!(
            format!(
                "{:#}",
                build_status(&fixture.config(), 2_000, None).unwrap_err()
            )
            .contains("ACK status timestamp is in the future")
        );
    }

    #[test]
    fn logical_progress_ignores_only_an_uncommitted_tail_and_rejects_regression() {
        let fixture = Fixture::new();
        let tracker = CaptureProgressTracker::default();
        let first = build_status(&fixture.config(), fixture.now, Some(&tracker)).unwrap();
        assert_eq!(first.recorder.updated_unix_secs, 2_000);
        append(&fixture.journal, br#"{"schema_version":1,"frame_id":6"#);
        set_mtime(&fixture.journal, 2_061);
        let status = build_status(&fixture.config(), 2_061, Some(&tracker)).unwrap();
        assert_eq!(status.recorder.durable_slot, 433_000_005);
        assert_eq!(status.recorder.updated_unix_secs, 2_000);
        assert_eq!(status.recorder.state, "stalled");

        fs::write(
            &fixture.journal,
            format!(
                "{}\n",
                json!({"schema_version": 1, "frame_id": 4, "slot": 433_000_004_u64})
            ),
        )
        .unwrap();
        set_mtime(&fixture.journal, 2_062);
        let error = build_status(&fixture.config(), 2_062, Some(&tracker)).unwrap_err();
        assert!(format!("{error:#}").contains("logical sequence regressed"));
    }

    #[test]
    fn malformed_committed_tail_and_invalid_counters_fail_closed() {
        let fixture = Fixture::new();
        append(&fixture.journal, b"not-json\n");
        let error = build_status(&fixture.config(), fixture.now, None).unwrap_err();
        assert!(format!("{error:#}").contains("final committed row is invalid JSON"));

        for value in [
            json!({"schema_version": 2, "frame_id": 6, "slot": 433_000_006_u64}),
            json!({"schema_version": 1, "frame_id": true, "slot": 433_000_006_u64}),
            json!({"schema_version": 1, "frame_id": 6, "slot": -1}),
        ] {
            fs::write(&fixture.journal, format!("{value}\n")).unwrap();
            set_mtime(&fixture.journal, fixture.now);
            assert!(build_status(&fixture.config(), fixture.now, None).is_err());
        }
    }

    #[test]
    fn ack_lag_stream_fencing_and_one_rotation_reread_match_the_oracle() {
        let fixture = Fixture::new();
        let mut ack = fixture.ack_value();
        ack["through_sequence"] = json!(103);
        fixture.write_ack(&ack);
        let status = build_status(&fixture.config(), fixture.now, None).unwrap();
        assert_eq!(status.replication.state, "syncing");
        assert_eq!(status.replication.lag_records, 2);
        assert_eq!(status.replication.ack_slot, None);
        assert!(status.recorder.unacknowledged_bytes > 0);

        ack["journal_id"] = json!("01".repeat(16));
        fixture.write_ack(&ack);
        let error = build_status(&fixture.config(), fixture.now, None).unwrap_err();
        assert!(format!("{error:#}").contains("different replication stream"));

        ack["journal_id"] = json!("02".repeat(16));
        ack["through_sequence"] = json!(106);
        fixture.write_ack(&ack);
        let base = read_capture(&fixture.config(), fixture.now, None).unwrap();
        let mut calls = 0;
        let status = build_status_with(
            &fixture.config(),
            fixture.now,
            || {
                calls += 1;
                let mut capture = base.clone();
                if calls == 2 {
                    capture.sequence = 106;
                    capture.slot = 433_000_006;
                }
                Ok(capture)
            },
            || Ok((100, 200)),
        )
        .unwrap();
        assert_eq!(calls, 2);
        assert_eq!(status.recorder.durable_slot, 433_000_006);
        assert_eq!(status.replication.lag_records, 0);
    }

    #[test]
    fn replay_gap_schemas_are_redacted_unproven_intervals() {
        let fixture = Fixture::new();
        let replay = fixture.active.join("replay-gaps");
        fs::create_dir(&replay).unwrap();
        write_json(
            &replay.join("schema-1.json"),
            &json!({
                "schema_version": 1,
                "anchor_slot": 100,
                "requested_slot": 100,
                "available_slot": 104,
                "cluster_id": "must-not-leak",
            }),
        );
        write_json(
            &replay.join("schema-2.json"),
            &json!({
                "schema_version": 2,
                "anchor_slot": 200,
                "requested_slot": 201,
                "provider_available_slot": 204,
                "selected_resume_slot": 206,
                "source_id": "must-not-leak",
            }),
        );
        let status = build_status(&fixture.config(), fixture.now, None).unwrap();
        assert!(status.gaps.contains(&Gap {
            from_slot: 101,
            to_slot: 103,
            produced_blocks: None,
            coverage: "unproven".into(),
        }));
        assert!(status.gaps.contains(&Gap {
            from_slot: 201,
            to_slot: 205,
            produced_blocks: None,
            coverage: "unproven".into(),
        }));
        assert_eq!(status.overall_state, "failed");
        assert!(
            !serde_json::to_string(&status)
                .unwrap()
                .contains("must-not-leak")
        );
    }

    #[test]
    fn malformed_replay_gap_records_fail_closed() {
        let invalid = [
            json!({}),
            json!({"schema_version": true, "anchor_slot": 100, "requested_slot": 100, "available_slot": 104}),
            json!({"schema_version": 1, "anchor_slot": 100, "requested_slot": 99, "available_slot": 104}),
            json!({"schema_version": 1, "anchor_slot": 100, "requested_slot": 100, "available_slot": 100}),
            json!({"schema_version": 2, "anchor_slot": 100, "requested_slot": 100, "provider_available_slot": 100, "selected_resume_slot": 104}),
            json!({"schema_version": 2, "anchor_slot": 100, "requested_slot": 100, "provider_available_slot": 104, "selected_resume_slot": 103}),
            json!({"schema_version": 2, "anchor_slot": 100, "requested_slot": 100, "provider_available_slot": 104, "selected_resume_slot": 104 + MAX_REPLAY_RESUME_HEADROOM_SLOTS + 1}),
        ];
        for value in invalid {
            assert!(gap_from_event(&value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn persistent_gap_registry_deduplicates_caps_and_preserves_hidden_failure() {
        let fixture = Fixture::new();
        let registry = fixture.cache.join("monitoring/replay-gaps");
        fs::create_dir(&registry).unwrap();
        write_json(
            &registry.join("retired.json"),
            &json!({
                "schema_version": 2,
                "anchor_slot": 300,
                "requested_slot": 300,
                "provider_available_slot": 304,
                "selected_resume_slot": 305,
            }),
        );
        write_json(&fixture.gaps, &json!([]));
        let gaps = read_gaps(&fixture.config()).unwrap();
        assert!(gaps.visible.contains(&Gap {
            from_slot: 301,
            to_slot: 304,
            produced_blocks: None,
            coverage: "unproven".into(),
        }));

        let mut known = (0..MAX_GAPS)
            .map(|index| {
                json!({
                    "from_slot": index * 2,
                    "to_slot": index * 2 + 1,
                    "produced_blocks": 1,
                    "coverage": "raw",
                })
            })
            .collect::<Vec<_>>();
        known.push(json!({
            "from_slot": 10_000,
            "to_slot": 10_001,
            "produced_blocks": null,
            "coverage": "unproven",
        }));
        write_json(&fixture.gaps, &Value::Array(known));
        fixture.clear_incidents();
        fs::remove_file(registry.join("retired.json")).unwrap();
        let status = build_status(&fixture.config(), fixture.now, None).unwrap();
        assert_eq!(status.gaps.len(), MAX_GAPS);
        assert!(status.gaps_truncated);
        assert_eq!(status.overall_state, "failed");

        write_json(
            &fixture.gaps,
            &json!([
                {"from_slot": 10, "to_slot": 11, "produced_blocks": null, "coverage": "unproven"},
                {"from_slot": 10, "to_slot": 11, "produced_blocks": 1, "coverage": "raw"}
            ]),
        );
        let gaps = read_gaps(&fixture.config()).unwrap();
        assert_eq!(
            gaps.visible[0],
            Gap {
                from_slot: 10,
                to_slot: 11,
                produced_blocks: Some(1),
                coverage: "raw".into(),
            }
        );
    }

    #[test]
    fn persistent_replay_registry_survives_generation_retirement() {
        let fixture = Fixture::new();
        let registry = fixture.cache.join("monitoring/replay-gaps");
        fs::create_dir(&registry).unwrap();
        write_json(
            &registry.join("retired.json"),
            &json!({
                "schema_version": 2,
                "anchor_slot": 300,
                "requested_slot": 300,
                "provider_available_slot": 304,
                "selected_resume_slot": 305,
            }),
        );
        write_json(&fixture.gaps, &json!([]));
        fs::remove_dir_all(&fixture.active).unwrap();
        fs::remove_dir_all(fixture.cache.join("sealed")).unwrap();

        let gaps = read_gaps(&fixture.config()).unwrap();
        assert_eq!(
            gaps.visible,
            vec![Gap {
                from_slot: 301,
                to_slot: 304,
                produced_blocks: None,
                coverage: "unproven".into(),
            }]
        );
        assert!(gaps.has_unproven);
    }

    #[test]
    fn every_gap_coverage_and_disk_boundary_changes_health_conservatively() {
        let fixture = Fixture::new();
        fixture.clear_incidents();
        for (coverage, expected) in [
            ("raw", "healthy"),
            ("normalized", "degraded"),
            ("rpc_recoverable", "degraded"),
            ("unproven", "failed"),
        ] {
            write_json(
                &fixture.gaps,
                &json!([{
                    "from_slot": 10,
                    "to_slot": 11,
                    "produced_blocks": 1,
                    "coverage": coverage,
                }]),
            );
            assert_eq!(
                build_status(&fixture.config(), fixture.now, None)
                    .unwrap()
                    .overall_state,
                expected
            );
        }

        write_json(&fixture.gaps, &json!([]));
        let mut config = fixture.config();
        config.disk_critical_free_bytes = 20;
        config.disk_warning_free_bytes = 30;
        let capture = read_capture(&config, fixture.now, None).unwrap();
        for (free, expected) in [
            (19, "failed"),
            (20, "degraded"),
            (29, "degraded"),
            (30, "healthy"),
        ] {
            let status = build_status_with(
                &config,
                fixture.now,
                || Ok(capture.clone()),
                || Ok((free, 100)),
            )
            .unwrap();
            assert_eq!(status.overall_state, expected);
        }
    }

    #[test]
    fn invalid_known_gap_configuration_fails_closed() {
        let fixture = Fixture::new();
        write_json(
            &fixture.gaps,
            &json!([{
                "from_slot": 10,
                "to_slot": 11,
                "produced_blocks": 1,
                "coverage": [],
            }]),
        );
        let error = build_status(&fixture.config(), fixture.now, None).unwrap_err();
        assert!(format!("{error:#}").contains("known gap entry has invalid fields"));
        fs::remove_file(&fixture.gaps).unwrap();
        let error = build_status(&fixture.config(), fixture.now, None).unwrap_err();
        assert!(format!("{error:#}").contains("known gap file is missing"));
    }

    #[test]
    fn incidents_merge_allowlisted_public_ids_and_hide_unknown_keys() {
        let fixture = Fixture::new();
        let alerts = fixture.cache.join("monitoring/telegram-alerts");
        fs::write(
            alerts.join("r2_usage.active"),
            b"Blockzilla backup - ERROR\nprivate\n",
        )
        .unwrap();
        fs::write(alerts.join("r2_usage.delivered"), b"1800 ERROR\n").unwrap();
        fs::write(
            alerts.join("b2_usage.active"),
            b"Blockzilla backup - CRITICAL\nprivate\n",
        )
        .unwrap();
        fs::write(alerts.join("b2_usage.delivered"), b"1700 CRITICAL\n").unwrap();
        fs::write(
            alerts.join("secret_token_value.active"),
            b"Blockzilla backup - CRITICAL\nprivate\n",
        )
        .unwrap();
        let status = build_status(&fixture.config(), fixture.now, None).unwrap();
        let object_store = status
            .incidents
            .iter()
            .find(|incident| incident.id == "object_store")
            .unwrap();
        assert_eq!(object_store.severity, "critical");
        assert_eq!(object_store.started_unix_secs, 1700);
        assert_eq!(status.overall_state, "failed");
        assert!(
            !serde_json::to_string(&status)
                .unwrap()
                .contains("secret_token_value")
        );
    }

    #[test]
    fn symlinked_inputs_and_tree_entries_are_never_followed() {
        let fixture = Fixture::new();
        let identity = fixture.active.join("identity.json");
        let private = fixture._root.path().join("private-identity.json");
        fs::rename(&identity, &private).unwrap();
        symlink(&private, &identity).unwrap();
        assert!(build_status(&fixture.config(), fixture.now, None).is_err());

        fs::remove_file(&identity).unwrap();
        fs::rename(&private, &identity).unwrap();
        symlink(&private, fixture.active.join("leak")).unwrap();
        let error = bounded_tree_bytes(&fixture.active).unwrap_err();
        assert!(format!("{error:#}").contains("symlink"));
    }

    #[test]
    fn cache_retains_last_snapshot_but_health_fails_after_refresh_error() {
        let cache = StatusCache::default();
        cache.commit(b"{\"schema_version\":1}".to_vec());
        let before = cache.body().unwrap();
        assert!(cache.healthy());
        cache.mark_refresh_failed();
        assert!(!cache.healthy());
        assert_eq!(&*cache.body().unwrap(), &*before);
        let health = route_request(
            &HttpRequest {
                method: "GET".into(),
                target: HEALTH_PATH.into(),
            },
            &cache,
        );
        assert_eq!(health.status, 503);
        assert_eq!(&*health.body, b"{\"ok\":false}");
        let status = route_request(
            &HttpRequest {
                method: "GET".into(),
                target: STATUS_PATH.into(),
            },
            &cache,
        );
        assert_eq!(status.status, 200);
        assert_eq!(&*status.body, &*before);
    }

    #[test]
    fn last_good_snapshot_ages_health_red_without_discarding_the_body() {
        let cache = StatusCache::with_maximum_healthy_age(Duration::from_millis(5));
        cache.commit(b"{\"schema_version\":1}".to_vec());
        assert!(cache.healthy());
        std::thread::sleep(Duration::from_millis(30));
        assert!(!cache.healthy());
        assert_eq!(&*cache.body().unwrap(), b"{\"schema_version\":1}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn periodic_refresh_recovers_after_a_transient_input_error() {
        let fixture = Fixture::new();
        let mut config = fixture.config();
        config.interval = Duration::from_millis(20);
        config.refresh_attempt_timeout = Duration::from_millis(250);
        config.refresh_shutdown_grace = Duration::from_millis(100);
        let service = IngestStatusService::new(config).unwrap();
        service
            .refresh_once_async(Arc::new(AtomicBool::new(false)))
            .await
            .unwrap();
        let last_good = service.cache.body().unwrap();

        write_json(&fixture.gaps, &json!({"invalid": true}));
        let (stop, stopped) = oneshot::channel();
        let refresh_service = service.clone();
        let task = tokio::spawn(async move { refresh_loop(refresh_service, stopped).await });
        timeout(Duration::from_secs(1), async {
            while service.cache.healthy() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(&*service.cache.body().unwrap(), &*last_good);

        write_json(&fixture.gaps, &json!([]));
        timeout(Duration::from_secs(1), async {
            while !service.cache.healthy() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("periodic refresh did not recover after the input was repaired");
        stop.send(()).unwrap();
        task.await.unwrap();
    }

    #[test]
    fn null_logical_replication_identity_uses_the_physical_journal() {
        let fixture = Fixture::new();
        let identity_path = fixture.active.join("identity.json");
        let mut identity = safe_json(&identity_path).unwrap();
        identity["replication_journal_id"] = Value::Null;
        identity["replication_sequence_base"] = Value::Null;
        write_json(&identity_path, &identity);

        let mut ack = fixture.ack_value();
        ack["journal_id"] = json!("01".repeat(16));
        ack["through_sequence"] = json!(5);
        fixture.write_ack(&ack);
        let status = build_status(&fixture.config(), fixture.now, None).unwrap();
        assert_eq!(status.replication.ack_through_sequence, 5);
        assert_eq!(status.replication.lag_records, 0);
    }

    #[test]
    fn checked_in_mainnet_gap_seed_is_accepted() {
        let fixture = Fixture::new();
        let seed =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("config/known-ingest-gaps.mainnet.json");
        assert_eq!(
            safe_json(&seed).unwrap(),
            json!([{
                "from_slot": 433_728_271,
                "to_slot": 433_731_796,
                "produced_blocks": 3_526,
                "coverage": "rpc_recoverable",
            }])
        );
        let mut config = fixture.config();
        config.known_gaps_file = Some(seed);
        let status = build_status(&config, fixture.now, None).unwrap();
        assert!(status.gaps.contains(&Gap {
            from_slot: 433_728_271,
            to_slot: 433_731_796,
            produced_blocks: Some(3_526),
            coverage: "rpc_recoverable".into(),
        }));
    }

    #[test]
    fn every_sealed_entry_counts_toward_the_scan_bound() {
        let fixture = Fixture::new();
        let sealed = fixture.cache.join("sealed");
        for index in 0..=MAX_TREE_ENTRIES {
            fs::write(sealed.join(format!("ordinary-{index}")), b"").unwrap();
        }
        let cache_root = Directory::open(&fixture.cache, "test cache root").unwrap();
        let error = safe_directories_at(&cache_root, OsStr::new("sealed"), 64, &NEVER_CANCELLED)
            .unwrap_err();
        assert!(format!("{error:#}").contains("scan limit"));
    }

    #[test]
    fn replay_gap_entries_share_one_global_scan_bound() {
        let fixture = Fixture::new();
        let registry = fixture.cache.join("monitoring/replay-gaps");
        fs::create_dir(&registry).unwrap();
        for index in 0..=MAX_TREE_ENTRIES {
            fs::write(registry.join(format!("ordinary-{index}")), b"").unwrap();
        }
        let error = read_gaps(&fixture.config()).unwrap_err();
        assert!(format!("{error:#}").contains("replay gap tree exceeds its entry limit"));
    }

    #[test]
    fn active_and_sealed_trees_share_one_global_scan_bound() {
        let fixture = Fixture::new();
        let sealed_generation = fixture.cache.join("sealed/slot-1");
        fs::create_dir(&sealed_generation).unwrap();
        let existing_active_entries = 4;
        for index in 0..=(MAX_TREE_ENTRIES - existing_active_entries) {
            fs::write(sealed_generation.join(format!("ordinary-{index}")), b"").unwrap();
        }
        let error = read_capture(&fixture.config(), fixture.now, None).unwrap_err();
        assert!(format!("{error:#}").contains("status tree exceeds its entry limit"));
    }

    #[test]
    fn parent_directory_symlinks_and_special_inputs_fail_closed() {
        let fixture = Fixture::new();
        let monitoring = fixture.cache.join("monitoring");
        let relocated = fixture._root.path().join("relocated-monitoring");
        fs::rename(&monitoring, &relocated).unwrap();
        symlink(&relocated, &monitoring).unwrap();
        assert!(build_status(&fixture.config(), fixture.now, None).is_err());

        fs::remove_file(&monitoring).unwrap();
        fs::rename(&relocated, &monitoring).unwrap();
        let identity = fixture.active.join("identity.json");
        fs::remove_file(&identity).unwrap();
        let identity = CString::new(identity.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(identity.as_ptr(), 0o600) }, 0);
        let started = MonotonicInstant::now();
        assert!(build_status(&fixture.config(), fixture.now, None).is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn incident_delivery_timestamps_must_be_public_safe_and_not_future() {
        let fixture = Fixture::new();
        let delivered = fixture
            .cache
            .join("monitoring/telegram-alerts/disk_space.delivered");
        fs::write(&delivered, format!("{} WARNING\n", fixture.now + 6)).unwrap();
        let error = build_status(&fixture.config(), fixture.now, None).unwrap_err();
        assert!(format!("{error:#}").contains("incident start timestamp"));

        fs::write(&delivered, format!("{} WARNING\n", MAX_SAFE_INTEGER + 1)).unwrap();
        let error = build_status(&fixture.config(), fixture.now, None).unwrap_err();
        assert!(format!("{error:#}").contains("incident start timestamp"));
    }

    #[test]
    fn rotation_lock_contention_is_bounded_and_cancellable() {
        let fixture = Fixture::new();
        let held = OpenOptions::new()
            .read(true)
            .write(true)
            .open(fixture.cache.join(".rotation.lock"))
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        let cache_root = Directory::open(&fixture.cache, "test cache root").unwrap();
        let started = MonotonicInstant::now();
        let error = RotationLock::acquire(&cache_root, Duration::from_millis(40), &NEVER_CANCELLED)
            .err()
            .expect("contended lock unexpectedly acquired");
        assert!(format!("{error:#}").contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(1));

        let cancelled = AtomicBool::new(true);
        let error = RotationLock::acquire(&cache_root, Duration::from_secs(1), &cancelled)
            .err()
            .expect("cancelled lock unexpectedly acquired");
        assert!(format!("{error:#}").contains("cancelled"));
        unsafe {
            libc::flock(held.as_raw_fd(), libc::LOCK_UN);
        }
    }

    #[test]
    fn rotation_lock_replacement_is_detected_before_status_commit() {
        let fixture = Fixture::new();
        let cache_root = Directory::open(&fixture.cache, "test cache root").unwrap();
        let lock = RotationLock::acquire(&cache_root, Duration::from_millis(100), &NEVER_CANCELLED)
            .unwrap();
        let lock_path = fixture.cache.join(".rotation.lock");
        let displaced = fixture.cache.join(".rotation.lock.displaced");
        fs::rename(&lock_path, &displaced).unwrap();
        fs::write(&lock_path, b"").unwrap();
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600)).unwrap();

        let error = lock.validate_identity(&cache_root).unwrap_err();
        assert!(format!("{error:#}").contains("changed while status was collected"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_cancels_a_refresh_waiting_for_the_rotation_lock() {
        let fixture = Fixture::new();
        let held = OpenOptions::new()
            .read(true)
            .write(true)
            .open(fixture.cache.join(".rotation.lock"))
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        let mut config = fixture.config();
        config.rotation_lock_timeout = Duration::from_secs(5);
        config.refresh_attempt_timeout = Duration::from_secs(5);
        config.refresh_shutdown_grace = Duration::from_millis(250);
        let service = IngestStatusService::new(config).unwrap();
        timeout(
            Duration::from_secs(1),
            service.run_until("127.0.0.1:0".parse().unwrap(), async {
                tokio::time::sleep(Duration::from_millis(30)).await;
            }),
        )
        .await
        .expect("shutdown waited for the full lock timeout")
        .unwrap();
        unsafe {
            libc::flock(held.as_raw_fd(), libc::LOCK_UN);
        }
    }

    #[test]
    fn http_parser_accepts_only_unambiguous_bodyless_requests() {
        let request = parse_http_request(
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nContent-Length:\t0\t\r\n\r\n",
        )
        .unwrap();
        assert_eq!(request.method, "GET");
        assert_eq!(request.target, HEALTH_PATH);
        assert!(parse_http_request(b"HEAD /healthz HTTP/1.0\r\n\r\n").is_ok());

        let invalid: &[&[u8]] = &[
            b"GET /healthz HTTP/1.1\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost:\t\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nHOST: localhost\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost: localhost, example.invalid\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0,0\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nContent-Length: +0\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nX-Test: bad\x01value\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nX-Test: bad\x7fvalue\r\n\r\n",
            b"GET\t/healthz HTTP/1.1\r\nHost: localhost\r\n\r\n",
            b"GET  /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n",
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\nbody",
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\nGET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n",
        ];
        for raw in invalid {
            assert!(
                matches!(parse_http_request(raw), Err(ReadRequestError::Malformed)),
                "accepted ambiguous request: {raw:?}"
            );
        }
    }

    async fn read_tcp_response(stream: &TcpStream) -> Vec<u8> {
        let mut response = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            stream.readable().await.unwrap();
            match stream.try_read(&mut chunk) {
                Ok(0) => return response,
                Ok(count) => response.extend_from_slice(&chunk[..count]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => panic!("read test response: {error}"),
            }
        }
    }

    #[tokio::test]
    async fn tcp_handler_rejects_ambiguous_framing_with_bad_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let client = client.unwrap();
        let (server, _) = accepted.unwrap();
        let cache = Arc::new(StatusCache::default());
        cache.commit(b"{\"schema_version\":1}".to_vec());
        let handler = tokio::spawn(handle_connection(server, cache));

        write_all_nonblocking(
            &client,
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
        .await
        .unwrap();
        let response = timeout(Duration::from_secs(1), read_tcp_response(&client))
            .await
            .unwrap();
        assert!(response.starts_with(b"HTTP/1.1 400 Bad Request\r\n"));
        handler.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn partial_http_headers_are_closed_at_the_configured_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cache = Arc::new(StatusCache::default());
        cache.commit(b"{\"schema_version\":1}".to_vec());
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(serve_http(
            listener,
            cache,
            1,
            Duration::from_millis(50),
            async move {
                let _ = stopped.await;
            },
        ));

        let client = TcpStream::connect(address).await.unwrap();
        write_all_nonblocking(&client, b"GET /healthz HTTP/1.1\r\nHost: local")
            .await
            .unwrap();
        let response = timeout(Duration::from_secs(1), read_tcp_response(&client))
            .await
            .expect("partial request was not closed at the header timeout");
        assert!(response.is_empty());

        let _ = stop.send(());
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn saturated_http_request_limit_drops_excess_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cache = Arc::new(StatusCache::default());
        cache.commit(b"{\"schema_version\":1}".to_vec());
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(serve_http(
            listener,
            cache,
            1,
            Duration::from_secs(5),
            async {
                let _ = stopped.await;
            },
        ));

        let first = TcpStream::connect(address).await.unwrap();
        write_all_nonblocking(&first, b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        let second = TcpStream::connect(address).await.unwrap();
        timeout(Duration::from_secs(1), async {
            let mut byte = [0_u8; 1];
            loop {
                second.readable().await.unwrap();
                match second.try_read(&mut byte) {
                    Ok(0) => break,
                    Ok(_) => panic!("saturated connection unexpectedly received a response"),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(error) => panic!("read saturated connection: {error}"),
                }
            }
        })
        .await
        .expect("saturated connection was not closed promptly");

        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[test]
    fn http_surface_is_read_only_exact_and_has_no_cors() {
        let cache = StatusCache::default();
        cache.commit(b"{\"schema_version\":1}".to_vec());
        let get = route_request(
            &HttpRequest {
                method: "GET".into(),
                target: STATUS_PATH.into(),
            },
            &cache,
        );
        assert_eq!(get.status, 200);
        let encoded = String::from_utf8(get.encode()).unwrap();
        assert!(encoded.contains("Cache-Control: no-store"));
        assert!(!encoded.contains("Access-Control-Allow-Origin"));

        let head = route_request(
            &HttpRequest {
                method: "HEAD".into(),
                target: STATUS_PATH.into(),
            },
            &cache,
        );
        assert_eq!(head.status, 200);
        assert!(!head.send_body);
        let post = route_request(
            &HttpRequest {
                method: "POST".into(),
                target: STATUS_PATH.into(),
            },
            &cache,
        );
        assert_eq!(post.status, 405);
        let query = route_request(
            &HttpRequest {
                method: "GET".into(),
                target: format!("{STATUS_PATH}?secret=x"),
            },
            &cache,
        );
        assert_eq!(query.status, 400);
        assert_eq!(REQUEST_HEADER_TIMEOUT, Duration::from_secs(5));
        assert_eq!(MAX_HTTP_REQUESTS, 32);
    }

    #[test]
    fn listener_and_cli_limits_reject_public_or_wildcard_bindings() {
        assert_eq!(
            parse_listener("127.0.0.1:8790").unwrap(),
            "127.0.0.1:8790".parse().unwrap()
        );
        assert_eq!(
            parse_listener("192.168.1.10:8790").unwrap(),
            "192.168.1.10:8790".parse().unwrap()
        );
        assert!(parse_listener("0.0.0.0:8790").is_err());
        assert!(parse_listener("203.0.113.10:8790").is_err());
        assert!(parse_listener("example.com:8790").is_err());
        let fixture = Fixture::new();
        let mut config = fixture.config();
        config.interval = Duration::ZERO;
        assert!(config.validate().is_err());
        config.interval = Duration::from_secs(10);
        config.disk_warning_free_bytes = config.disk_critical_free_bytes;
        assert!(config.validate().is_err());
    }
}
