//! Bounded, secret-free aggregation and serving for the public shred-ingest status.
//!
//! The two inputs are deliberately independent: a malformed or unavailable shred-reader
//! metrics response must not hide Hivezilla's durable recorder state, and a malformed recorder
//! snapshot must not hide receiver telemetry. Only the fixed fields modeled below cross this
//! boundary; unknown source fields are ignored rather than copied into the public document.

use std::{
    collections::HashSet,
    env,
    ffi::CString,
    fmt,
    fs::{self, File, OpenOptions},
    future::{Future, pending},
    io::{self, Read, Write},
    net::{IpAddr, SocketAddr},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{ffi::OsStrExt, fs::OpenOptionsExt, fs::PermissionsExt},
    },
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use clap::Args;
use futures::StreamExt;
use reqwest::{
    Client,
    header::{ACCEPT, ACCEPT_ENCODING, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE},
    redirect::Policy,
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};

pub const STATUS_PATH: &str = "/api/v1/sidecars/shred-ingest/status.json";
pub const HEALTH_PATH: &str = "/healthz";
pub const MAX_SOURCE_JSON_BYTES: usize = 64 * 1024;
pub const MAX_PUBLIC_JSON_BYTES: usize = 64 * 1024;
pub const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
pub const MAX_HTTP_REQUESTS: usize = 32;
pub const REQUEST_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

const MAX_SAFE_INTEGER: u64 = (1_u64 << 53) - 1;
const DEFAULT_LISTEN: &str = "127.0.0.1:18790";
const DEFAULT_RECEIVER_METRICS_URL: &str = "http://127.0.0.1:19090/metrics";
const DEFAULT_INTERVAL_SECS: u64 = 5;
const DEFAULT_RECEIVER_TIMEOUT_SECS: f64 = 2.0;
const DEFAULT_HIVEZILLA_STALE_AFTER_SECS: u64 = 20;
const DEFAULT_TVU_ACTIVE_AFTER_SECS: u64 = 30;
const DEFAULT_MAX_FUTURE_SKEW_SECS: u64 = 5;
const LISTEN_BACKLOG: i32 = 32;
const MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_FRESHNESS_THRESHOLD: Duration = Duration::from_secs(365 * 24 * 60 * 60);
const MAX_REQUEST_HEADER_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_FUTURE_SKEW: Duration = Duration::from_secs(60);

static TEMPORARY_ORDINAL: AtomicU64 = AtomicU64::new(0);

/// CLI surface kept in this module so integration requires only one command variant and match arm.
/// Explicit flags take precedence over the legacy `SHRED_STATUS_*` environment variables.
#[derive(Debug, Clone, Args)]
pub struct ServeShredStatusArgs {
    /// HTTP listen address. Only loopback/private IPv4 and 0.0.0.0 are accepted.
    #[arg(long)]
    pub listen: Option<String>,

    /// Hivezilla's private, atomically published shred-recorder status file.
    #[arg(long)]
    pub hivezilla_status_file: Option<PathBuf>,

    /// Exact loopback `http://IP:port/metrics` endpoint exposed by shred-reader.
    #[arg(long)]
    pub receiver_metrics_url: Option<String>,

    /// Optional atomically replaced public JSON snapshot.
    #[arg(long)]
    pub output_file: Option<PathBuf>,

    /// Optional exact browser origin allowed by CORS.
    #[arg(long)]
    pub cors_origin: Option<String>,

    /// Refresh interval.
    #[arg(long)]
    pub interval_secs: Option<u64>,

    /// Loopback receiver request timeout; positive and at most 60 seconds.
    #[arg(long)]
    pub receiver_timeout_secs: Option<f64>,

    /// Age after which the recorder status is explicitly marked stale.
    #[arg(long)]
    pub hivezilla_stale_after_secs: Option<u64>,

    /// Packet age still classified as actively receiving.
    #[arg(long)]
    pub tvu_active_after_secs: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiverEndpoint {
    address: SocketAddr,
}

impl ReceiverEndpoint {
    pub fn parse(value: &str) -> Result<Self> {
        ensure!(
            !value.contains(['\r', '\n']),
            "receiver metrics URL contains a line break"
        );
        let url = reqwest::Url::parse(value).context("parse receiver metrics URL")?;
        ensure!(url.scheme() == "http", "receiver metrics URL must use HTTP");
        ensure!(
            url.username().is_empty() && url.password().is_none(),
            "receiver metrics URL must not contain credentials"
        );
        ensure!(
            url.path() == "/metrics" && url.query().is_none() && url.fragment().is_none(),
            "receiver metrics URL must target exact /metrics path"
        );
        let host = url
            .host_str()
            .context("receiver metrics URL is missing an IP host")?;
        // `url` keeps brackets around an IPv6 literal in `host_str()`; `IpAddr` expects the
        // unbracketed representation.
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        let ip: IpAddr = host
            .parse()
            .context("receiver metrics host must be an IP literal")?;
        ensure!(ip.is_loopback(), "receiver metrics host must be loopback");
        let port = url
            .port()
            .context("receiver metrics URL must include an explicit port")?;
        ensure!(port != 0, "receiver metrics port must be non-zero");
        Ok(Self {
            address: SocketAddr::new(ip, port),
        })
    }

    pub fn address(self) -> SocketAddr {
        self.address
    }

    pub fn url(self) -> String {
        format!("http://{}/metrics", self.address)
    }
}

impl fmt::Display for ReceiverEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "http://{}/metrics", self.address)
    }
}

#[derive(Debug, Clone)]
pub struct ShredStatusConfig {
    hivezilla_status_file: PathBuf,
    receiver_endpoint: ReceiverEndpoint,
    output_file: Option<PathBuf>,
    cors_origin: Option<String>,
    interval: Duration,
    receiver_timeout: Duration,
    hivezilla_stale_after: Duration,
    tvu_active_after: Duration,
    max_future_skew: Duration,
    max_http_requests: usize,
    request_header_timeout: Duration,
}

impl ShredStatusConfig {
    pub fn validate(&self) -> Result<()> {
        validate_absolute_non_root(&self.hivezilla_status_file, "Hivezilla status file")?;
        if let Some(output) = &self.output_file {
            validate_absolute_non_root(output, "public output file")?;
            validate_distinct_files(&self.hivezilla_status_file, output)?;
        }
        if let Some(origin) = &self.cors_origin {
            validate_cors_origin(origin)?;
        }
        ensure!(
            !self.interval.is_zero() && self.interval <= MAX_REFRESH_INTERVAL,
            "refresh interval must be positive and at most one day"
        );
        ensure!(
            !self.receiver_timeout.is_zero() && self.receiver_timeout <= Duration::from_secs(60),
            "receiver timeout must be positive and at most 60 seconds"
        );
        ensure!(
            !self.hivezilla_stale_after.is_zero()
                && self.hivezilla_stale_after <= MAX_FRESHNESS_THRESHOLD,
            "Hivezilla stale threshold must be positive and at most one year"
        );
        ensure!(
            !self.tvu_active_after.is_zero() && self.tvu_active_after <= MAX_FRESHNESS_THRESHOLD,
            "TVU active threshold must be positive and at most one year"
        );
        ensure!(
            (1..=MAX_HTTP_REQUESTS).contains(&self.max_http_requests),
            "HTTP request limit must be between 1 and {MAX_HTTP_REQUESTS}"
        );
        ensure!(
            !self.request_header_timeout.is_zero()
                && self.request_header_timeout <= MAX_REQUEST_HEADER_TIMEOUT,
            "HTTP request-header timeout must be positive and at most 60 seconds"
        );
        ensure!(
            self.max_future_skew <= MAX_FUTURE_SKEW,
            "future timestamp skew must be at most 60 seconds"
        );
        ensure!(
            self.receiver_endpoint.address().ip().is_loopback(),
            "receiver endpoint must remain loopback"
        );
        Ok(())
    }

    fn from_args(args: ServeShredStatusArgs) -> Result<(SocketAddr, Self)> {
        let listen = optional_string(args.listen, "SHRED_STATUS_LISTEN")
            .unwrap_or_else(|| DEFAULT_LISTEN.to_owned());
        let listen = parse_listener(&listen)?;

        let hivezilla_status_file = args
            .hivezilla_status_file
            .or_else(|| nonempty_environment("SHRED_STATUS_HIVEZILLA_FILE").map(PathBuf::from))
            .context("--hivezilla-status-file or SHRED_STATUS_HIVEZILLA_FILE must be configured")?;
        let receiver_metrics_url = optional_string(
            args.receiver_metrics_url,
            "SHRED_STATUS_RECEIVER_METRICS_URL",
        )
        .unwrap_or_else(|| DEFAULT_RECEIVER_METRICS_URL.to_owned());
        let output_file = args
            .output_file
            .or_else(|| nonempty_environment("SHRED_STATUS_OUTPUT_FILE").map(PathBuf::from));
        let cors_origin = optional_string(args.cors_origin, "SHRED_STATUS_CORS_ORIGIN");
        let interval_secs = numeric_setting(
            args.interval_secs,
            "SHRED_STATUS_INTERVAL_SECS",
            DEFAULT_INTERVAL_SECS,
        )?;
        let receiver_timeout_secs = float_setting(
            args.receiver_timeout_secs,
            "SHRED_STATUS_RECEIVER_TIMEOUT_SECS",
            DEFAULT_RECEIVER_TIMEOUT_SECS,
        )?;
        let hivezilla_stale_after_secs = numeric_setting(
            args.hivezilla_stale_after_secs,
            "SHRED_STATUS_HIVEZILLA_STALE_AFTER_SECS",
            DEFAULT_HIVEZILLA_STALE_AFTER_SECS,
        )?;
        let tvu_active_after_secs = numeric_setting(
            args.tvu_active_after_secs,
            "SHRED_STATUS_TVU_ACTIVE_AFTER_SECS",
            DEFAULT_TVU_ACTIVE_AFTER_SECS,
        )?;
        ensure!(
            interval_secs > 0 && interval_secs <= MAX_REFRESH_INTERVAL.as_secs(),
            "refresh interval must be positive and at most one day"
        );
        ensure!(
            receiver_timeout_secs.is_finite()
                && receiver_timeout_secs > 0.0
                && receiver_timeout_secs <= 60.0,
            "receiver timeout must be positive and at most 60 seconds"
        );
        ensure!(
            hivezilla_stale_after_secs > 0
                && hivezilla_stale_after_secs <= MAX_FRESHNESS_THRESHOLD.as_secs(),
            "Hivezilla stale threshold must be positive and at most one year"
        );
        ensure!(
            tvu_active_after_secs > 0 && tvu_active_after_secs <= MAX_FRESHNESS_THRESHOLD.as_secs(),
            "TVU active threshold must be positive and at most one year"
        );

        let config = Self {
            hivezilla_status_file,
            receiver_endpoint: ReceiverEndpoint::parse(&receiver_metrics_url)?,
            output_file,
            cors_origin,
            interval: Duration::from_secs(interval_secs),
            receiver_timeout: Duration::from_secs_f64(receiver_timeout_secs),
            hivezilla_stale_after: Duration::from_secs(hivezilla_stale_after_secs),
            tvu_active_after: Duration::from_secs(tvu_active_after_secs),
            max_future_skew: Duration::from_secs(DEFAULT_MAX_FUTURE_SKEW_SECS),
            max_http_requests: MAX_HTTP_REQUESTS,
            request_header_timeout: REQUEST_HEADER_TIMEOUT,
        };
        config.validate()?;
        Ok((listen, config))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicShredStatus {
    pub schema_version: u32,
    pub updated_unix_secs: u64,
    pub gossip: GossipStatus,
    pub tvu: TvuStatus,
    pub forwarding: ForwardingStatus,
    pub hivezilla: HivezillaStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GossipStatus {
    pub state: String,
    pub recent_peer_count: Option<u64>,
    pub known_peer_count: Option<u64>,
    pub tvu_peer_count: Option<u64>,
    pub shred_version: Option<u64>,
    pub receiver_uptime_secs: Option<u64>,
    pub updated_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TvuStatus {
    pub state: String,
    pub packets_total: Option<u64>,
    pub bytes_total: Option<u64>,
    pub parsed_total: Option<u64>,
    pub invalid_total: Option<u64>,
    pub version_mismatch_total: Option<u64>,
    pub unique_total: Option<u64>,
    pub duplicates_total: Option<u64>,
    pub data_total: Option<u64>,
    pub code_total: Option<u64>,
    pub socket_rxq_overflow_supported: Option<bool>,
    pub socket_rxq_overflow_total: Option<u64>,
    pub latest_slot: Option<u64>,
    pub seconds_since_last_packet: Option<u64>,
    pub updated_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ForwardingStatus {
    pub state: String,
    pub target_count: Option<u64>,
    pub attempts_total: Option<u64>,
    pub successful_datagrams_total: Option<u64>,
    pub errors_total: Option<u64>,
    pub updated_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HivezillaStatus {
    pub availability: String,
    pub status_fresh: bool,
    pub state: String,
    pub updated_unix_secs: Option<u64>,
    pub started_unix_secs: Option<u64>,
    pub last_durable_unix_secs: Option<u64>,
    pub accepted_total: Option<u64>,
    pub invalid_total: Option<u64>,
    pub bytes_total: Option<u64>,
    pub durable_through_sequence: Option<u64>,
    pub latest_slot: Option<u64>,
    pub shred_version: Option<u64>,
    pub spool_bytes: Option<u64>,
    pub spool_max_bytes: Option<u64>,
    pub filesystem_free_bytes: Option<u64>,
    pub filesystem_total_bytes: Option<u64>,
    pub reserve_free_bytes: Option<u64>,
    pub udp_received_total: Option<u64>,
    pub udp_received_bytes_total: Option<u64>,
    pub ingest_queue_depth_events: Option<u64>,
    pub ingest_queue_depth_bytes: Option<u64>,
    pub ingest_queue_high_water_events: Option<u64>,
    pub ingest_queue_high_water_bytes: Option<u64>,
    pub ingest_queue_capacity_events: Option<u64>,
    pub ingest_queue_capacity_bytes: Option<u64>,
    pub ingest_queue_backpressure_events_total: Option<u64>,
    pub ingest_queue_backpressure_micros_total: Option<u64>,
    pub ingest_queue_backpressured: Option<bool>,
    pub socket_rxq_overflow_supported: Option<bool>,
    pub socket_rxq_overflow_total: Option<u64>,
}

#[derive(Debug, Clone)]
struct ReceiverStatus {
    gossip: GossipStatus,
    tvu: TvuStatus,
    forwarding: ForwardingStatus,
}

/// Build one public sample. `None` represents an unavailable source; malformed source values are
/// also projected independently to their complete `unavailable` shape.
pub fn build_status_from_values(
    config: &ShredStatusConfig,
    now_unix_secs: u64,
    receiver: Option<Value>,
    hivezilla: Option<Value>,
) -> Result<PublicShredStatus> {
    ensure!(
        (1..=MAX_SAFE_INTEGER).contains(&now_unix_secs),
        "sample timestamp is outside the public integer range"
    );
    let receiver = receiver
        .and_then(|value| parse_receiver_metrics(&value, now_unix_secs, config).ok())
        .unwrap_or_else(unavailable_receiver_status);
    let hivezilla = hivezilla
        .and_then(|value| parse_hivezilla_status(&value, now_unix_secs, config).ok())
        .unwrap_or_else(unavailable_hivezilla_status);
    Ok(PublicShredStatus {
        schema_version: 1,
        updated_unix_secs: now_unix_secs,
        gossip: receiver.gossip,
        tvu: receiver.tvu,
        forwarding: receiver.forwarding,
        hivezilla,
    })
}

pub fn encode_public_status(status: &PublicShredStatus) -> Result<Vec<u8>> {
    // serde_json's default map representation is ordered. Going through Value preserves the
    // predecessor's stable, sorted-key output without exposing source map entries.
    let selected = serde_json::to_value(status).context("select public shred status")?;
    let encoded = serde_json::to_vec(&selected).context("encode public shred status")?;
    ensure!(
        !encoded.is_empty() && encoded.len() <= MAX_PUBLIC_JSON_BYTES,
        "public shred status exceeds its byte limit"
    );
    Ok(encoded)
}

pub fn decode_bounded_json(raw: &[u8]) -> Result<Value> {
    ensure!(
        !raw.is_empty() && raw.len() <= MAX_SOURCE_JSON_BYTES,
        "source JSON has an invalid size"
    );
    let mut deserializer = serde_json::Deserializer::from_slice(raw);
    let value =
        StrictValue::deserialize(&mut deserializer).context("decode duplicate-safe source JSON")?;
    deserializer
        .end()
        .context("source JSON has trailing data")?;
    Ok(value.0)
}

pub fn read_bounded_regular(path: &Path) -> Result<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("open bounded status input {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect bounded status input {}", path.display()))?;
    ensure!(metadata.is_file(), "status input is not a regular file");
    ensure!(
        metadata.len() > 0 && metadata.len() <= MAX_SOURCE_JSON_BYTES as u64,
        "status input has an invalid size"
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    Read::by_ref(&mut file)
        .take(MAX_SOURCE_JSON_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read bounded status input {}", path.display()))?;
    ensure!(
        !bytes.is_empty() && bytes.len() <= MAX_SOURCE_JSON_BYTES,
        "status input has an invalid size"
    );
    Ok(bytes)
}

pub fn validate_distinct_files(source: &Path, output: &Path) -> Result<()> {
    ensure!(
        source != output,
        "public output file must differ from its private input"
    );
    let source_identity = canonical_parent_file_identity(source, "Hivezilla status input", true)?;
    let output_identity = canonical_parent_file_identity(output, "public status output", false)?
        .context("public status output parent must exist")?;
    if let Some(source_identity) = source_identity {
        ensure!(
            source_identity != output_identity,
            "public output file aliases its private input"
        );
    }
    let source_metadata = match fs::metadata(source) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).context("inspect Hivezilla status input"),
    };
    let output_metadata = match fs::metadata(output) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).context("inspect public status output"),
    };
    if let (Some(source), Some(output)) = (source_metadata, output_metadata) {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            (source.dev(), source.ino()) != (output.dev(), output.ino()),
            "public output file aliases its private input"
        );
    }
    Ok(())
}

fn canonical_parent_file_identity(
    path: &Path,
    label: &str,
    allow_missing_parent: bool,
) -> Result<Option<PathBuf>> {
    ensure!(path.is_absolute(), "{label} must be absolute");
    ensure!(
        !path
            .as_os_str()
            .as_bytes()
            .split(|byte| *byte == b'/')
            .any(|component| component == b"." || component == b".."),
        "{label} must not contain dot path components"
    );
    let parent = path
        .parent()
        .with_context(|| format!("{label} has no parent"))?;
    let file_name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .with_context(|| format!("{label} has no file name"))?;
    let canonical_parent = match fs::canonicalize(parent) {
        Ok(parent) => parent,
        Err(error) if allow_missing_parent && error.kind() == io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => {
            return Err(error).with_context(|| format!("canonicalize {label} parent directory"));
        }
    };
    Ok(Some(canonical_parent.join(file_name)))
}

pub fn write_atomic_public(path: &Path, payload: &[u8]) -> Result<()> {
    ensure!(
        !payload.is_empty() && payload.len() <= MAX_PUBLIC_JSON_BYTES,
        "public shred status has an invalid size"
    );
    let parent = path
        .parent()
        .context("public snapshot path has no parent")?;
    let file_name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .context("public snapshot path has no safe file name")?;
    ensure!(
        file_name != "." && file_name != "..",
        "unsafe public snapshot name"
    );

    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(parent)
        .context("open public snapshot directory")?;
    ensure!(
        directory.metadata()?.is_dir(),
        "public snapshot parent is not a directory"
    );
    let destination = CString::new(file_name.as_bytes())
        .map_err(|_| anyhow!("public snapshot name contains a NUL byte"))?;
    reject_nonregular_at(directory.as_raw_fd(), &destination)?;

    let ordinal = TEMPORARY_ORDINAL.fetch_add(1, Ordering::Relaxed);
    let temporary_name = format!(
        ".{}.{}-{ordinal}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    );
    let temporary = CString::new(temporary_name.as_bytes())
        .map_err(|_| anyhow!("temporary public snapshot name contains a NUL byte"))?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            temporary.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o644,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error()).context("create public snapshot temporary file");
    }
    let mut temporary_file = unsafe { File::from_raw_fd(descriptor) };
    let result = (|| -> Result<()> {
        temporary_file
            .set_permissions(fs::Permissions::from_mode(0o644))
            .context("set public snapshot mode")?;
        temporary_file
            .write_all(payload)
            .context("write public snapshot")?;
        temporary_file.sync_all().context("sync public snapshot")?;
        drop(temporary_file);
        let renamed = unsafe {
            libc::renameat(
                directory.as_raw_fd(),
                temporary.as_ptr(),
                directory.as_raw_fd(),
                destination.as_ptr(),
            )
        };
        if renamed != 0 {
            return Err(io::Error::last_os_error()).context("publish public snapshot");
        }
        directory
            .sync_all()
            .context("sync public snapshot directory")?;
        Ok(())
    })();
    if result.is_err() {
        unsafe {
            libc::unlinkat(directory.as_raw_fd(), temporary.as_ptr(), 0);
        }
    }
    result
}

fn reject_nonregular_at(directory: i32, name: &CString) -> Result<()> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            directory,
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        let metadata = unsafe { metadata.assume_init() };
        ensure!(
            metadata.st_mode & libc::S_IFMT == libc::S_IFREG,
            "public snapshot destination is not a regular file"
        );
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::NotFound {
        Ok(())
    } else {
        Err(error).context("inspect public snapshot destination")
    }
}

fn parse_hivezilla_status(
    value: &Value,
    now_unix_secs: u64,
    config: &ShredStatusConfig,
) -> Result<HivezillaStatus> {
    let source = object(value, "Hivezilla status")?;
    ensure!(
        bounded_u64(required(source, "schema_version")?, 1, false)? == 1,
        "unsupported Hivezilla status schema"
    );
    let state = required_string(source, "state")?;
    ensure!(
        matches!(state, "waiting" | "receiving" | "stalled" | "stopped"),
        "unsupported Hivezilla recorder state"
    );
    let updated = required_u64(source, "updated_unix_secs", MAX_SAFE_INTEGER, true)?;
    let started = required_u64(source, "started_unix_secs", MAX_SAFE_INTEGER, true)?;
    let last_durable = optional_u64(source, "last_durable_unix_secs", MAX_SAFE_INTEGER, true)?;
    let accepted = required_u64(source, "accepted_total", MAX_SAFE_INTEGER, false)?;
    let invalid = required_u64(source, "invalid_total", MAX_SAFE_INTEGER, false)?;
    let payload_bytes = required_u64(source, "bytes_total", MAX_SAFE_INTEGER, false)?;
    let durable_sequence =
        optional_u64(source, "durable_through_sequence", MAX_SAFE_INTEGER, false)?;
    let latest_slot = optional_u64(source, "latest_slot", MAX_SAFE_INTEGER, false)?;
    let shred_version = optional_u64(source, "shred_version", u16::MAX as u64, false)?;
    let spool_bytes = required_u64(source, "spool_bytes", MAX_SAFE_INTEGER, false)?;
    let spool_max_bytes = required_u64(source, "spool_max_bytes", MAX_SAFE_INTEGER, true)?;
    let filesystem_free = required_u64(source, "filesystem_free_bytes", MAX_SAFE_INTEGER, false)?;
    let filesystem_total = required_u64(source, "filesystem_total_bytes", MAX_SAFE_INTEGER, true)?;
    let reserve_free = required_u64(source, "reserve_free_bytes", MAX_SAFE_INTEGER, false)?;
    let udp_received = required_u64(source, "udp_received_total", MAX_SAFE_INTEGER, false)?;
    let udp_received_bytes =
        required_u64(source, "udp_received_bytes_total", MAX_SAFE_INTEGER, false)?;
    let queue_depth_events =
        required_u64(source, "ingest_queue_depth_events", MAX_SAFE_INTEGER, false)?;
    let queue_depth_bytes =
        required_u64(source, "ingest_queue_depth_bytes", MAX_SAFE_INTEGER, false)?;
    let queue_high_water_events = required_u64(
        source,
        "ingest_queue_high_water_events",
        MAX_SAFE_INTEGER,
        false,
    )?;
    let queue_high_water_bytes = required_u64(
        source,
        "ingest_queue_high_water_bytes",
        MAX_SAFE_INTEGER,
        false,
    )?;
    let queue_capacity_events = required_u64(
        source,
        "ingest_queue_capacity_events",
        MAX_SAFE_INTEGER,
        true,
    )?;
    let queue_capacity_bytes = required_u64(
        source,
        "ingest_queue_capacity_bytes",
        MAX_SAFE_INTEGER,
        true,
    )?;
    let queue_backpressure_events = required_u64(
        source,
        "ingest_queue_backpressure_events_total",
        MAX_SAFE_INTEGER,
        false,
    )?;
    let queue_backpressure_micros = required_u64(
        source,
        "ingest_queue_backpressure_micros_total",
        MAX_SAFE_INTEGER,
        false,
    )?;
    let queue_backpressured = required_bool(source, "ingest_queue_backpressured")?;
    let socket_overflow_supported = required_bool(source, "socket_rxq_overflow_supported")?;
    let socket_overflow =
        optional_u64(source, "socket_rxq_overflow_total", MAX_SAFE_INTEGER, false)?;

    ensure!(
        started <= updated,
        "Hivezilla start timestamp is after its update"
    );
    ensure!(
        last_durable.is_none_or(|value| value <= updated),
        "Hivezilla durable timestamp is after its update"
    );
    ensure!(
        updated <= now_unix_secs.saturating_add(config.max_future_skew.as_secs()),
        "Hivezilla update timestamp is in the future"
    );
    ensure!(
        spool_bytes <= spool_max_bytes,
        "Hivezilla spool usage exceeds its configured maximum"
    );
    ensure!(
        filesystem_free <= filesystem_total && reserve_free <= filesystem_total,
        "Hivezilla filesystem counters are contradictory"
    );
    ensure!(
        accepted
            .checked_add(invalid)
            .is_some_and(|total| total <= udp_received)
            && payload_bytes <= udp_received_bytes,
        "Hivezilla UDP counters are contradictory"
    );
    ensure!(
        queue_depth_events <= queue_capacity_events
            && queue_high_water_events <= queue_capacity_events
            && queue_depth_events <= queue_high_water_events
            && queue_depth_bytes <= queue_capacity_bytes
            && queue_high_water_bytes <= queue_capacity_bytes
            && queue_depth_bytes <= queue_high_water_bytes,
        "Hivezilla ingest queue counters are contradictory"
    );
    ensure!(
        socket_overflow_supported == socket_overflow.is_some(),
        "Hivezilla socket-overflow support and counter disagree"
    );
    let durable_children = [
        durable_sequence.is_some(),
        latest_slot.is_some(),
        shred_version.is_some(),
    ];
    let has_durable_tail = durable_children.iter().any(|value| *value);
    let complete_durable_tail = durable_children.iter().all(|value| *value);
    ensure!(
        !has_durable_tail || complete_durable_tail,
        "Hivezilla durable evidence is incomplete"
    );
    ensure!(
        last_durable.is_none() || complete_durable_tail,
        "Hivezilla durable timestamp lacks a durable tail"
    );
    ensure!(
        accepted == 0 || (complete_durable_tail && last_durable.is_some()),
        "Hivezilla accepted count lacks durable evidence"
    );
    ensure!(
        accepted != 0 || payload_bytes == 0,
        "Hivezilla payload bytes exist without accepted shreds"
    );
    let fresh = now_unix_secs <= updated.saturating_add(config.hivezilla_stale_after.as_secs());

    Ok(HivezillaStatus {
        availability: "available".into(),
        status_fresh: fresh,
        state: state.into(),
        updated_unix_secs: Some(updated),
        started_unix_secs: Some(started),
        last_durable_unix_secs: last_durable,
        accepted_total: Some(accepted),
        invalid_total: Some(invalid),
        bytes_total: Some(payload_bytes),
        durable_through_sequence: durable_sequence,
        latest_slot,
        shred_version,
        spool_bytes: Some(spool_bytes),
        spool_max_bytes: Some(spool_max_bytes),
        filesystem_free_bytes: Some(filesystem_free),
        filesystem_total_bytes: Some(filesystem_total),
        reserve_free_bytes: Some(reserve_free),
        udp_received_total: Some(udp_received),
        udp_received_bytes_total: Some(udp_received_bytes),
        ingest_queue_depth_events: Some(queue_depth_events),
        ingest_queue_depth_bytes: Some(queue_depth_bytes),
        ingest_queue_high_water_events: Some(queue_high_water_events),
        ingest_queue_high_water_bytes: Some(queue_high_water_bytes),
        ingest_queue_capacity_events: Some(queue_capacity_events),
        ingest_queue_capacity_bytes: Some(queue_capacity_bytes),
        ingest_queue_backpressure_events_total: Some(queue_backpressure_events),
        ingest_queue_backpressure_micros_total: Some(queue_backpressure_micros),
        ingest_queue_backpressured: Some(queue_backpressured),
        socket_rxq_overflow_supported: Some(socket_overflow_supported),
        socket_rxq_overflow_total: socket_overflow,
    })
}

fn unavailable_hivezilla_status() -> HivezillaStatus {
    HivezillaStatus {
        availability: "unavailable".into(),
        status_fresh: false,
        state: "unavailable".into(),
        updated_unix_secs: None,
        started_unix_secs: None,
        last_durable_unix_secs: None,
        accepted_total: None,
        invalid_total: None,
        bytes_total: None,
        durable_through_sequence: None,
        latest_slot: None,
        shred_version: None,
        spool_bytes: None,
        spool_max_bytes: None,
        filesystem_free_bytes: None,
        filesystem_total_bytes: None,
        reserve_free_bytes: None,
        udp_received_total: None,
        udp_received_bytes_total: None,
        ingest_queue_depth_events: None,
        ingest_queue_depth_bytes: None,
        ingest_queue_high_water_events: None,
        ingest_queue_high_water_bytes: None,
        ingest_queue_capacity_events: None,
        ingest_queue_capacity_bytes: None,
        ingest_queue_backpressure_events_total: None,
        ingest_queue_backpressure_micros_total: None,
        ingest_queue_backpressured: None,
        socket_rxq_overflow_supported: None,
        socket_rxq_overflow_total: None,
    }
}

fn parse_receiver_metrics(
    value: &Value,
    sampled_unix_secs: u64,
    config: &ShredStatusConfig,
) -> Result<ReceiverStatus> {
    let source = object(value, "shred-reader metrics")?;
    let uptime = required_u64(source, "uptime_seconds", MAX_SAFE_INTEGER, false)?;
    let known_peers = required_u64(source, "gossip_peers", MAX_SAFE_INTEGER, false)?;
    let recent_peers = required_u64(source, "recent_gossip_peers", MAX_SAFE_INTEGER, false)?;
    let tvu_peers = required_u64(source, "tvu_peers", MAX_SAFE_INTEGER, false)?;
    let shred_version = required_u64(source, "shred_version", u16::MAX as u64, false)?;
    let packets = required_u64(source, "packets_total", MAX_SAFE_INTEGER, false)?;
    let received_bytes = required_u64(source, "bytes_total", MAX_SAFE_INTEGER, false)?;
    let parsed = required_u64(source, "parsed_total", MAX_SAFE_INTEGER, false)?;
    let invalid = required_u64(source, "invalid_total", MAX_SAFE_INTEGER, false)?;
    let mismatched = required_u64(source, "version_mismatch_total", MAX_SAFE_INTEGER, false)?;
    let unique = required_u64(source, "unique_total", MAX_SAFE_INTEGER, false)?;
    let duplicates = required_u64(source, "duplicates_total", MAX_SAFE_INTEGER, false)?;
    let data = required_u64(source, "data_total", MAX_SAFE_INTEGER, false)?;
    let code = required_u64(source, "code_total", MAX_SAFE_INTEGER, false)?;
    let targets = required_u64(source, "forward_targets", 32, false)?;
    let forwarded = required_u64(source, "forwarded_datagrams_total", MAX_SAFE_INTEGER, false)?;
    let forward_errors = required_u64(source, "forward_errors_total", MAX_SAFE_INTEGER, false)?;
    let latest_slot = required_u64(source, "latest_slot", MAX_SAFE_INTEGER, false)?;
    let seconds_since_packet =
        optional_u64(source, "seconds_since_last_packet", MAX_SAFE_INTEGER, false)?;
    let socket_overflow_supported = required_bool(source, "tvu_socket_rxq_overflow_supported")?;
    let socket_overflow = optional_u64(
        source,
        "tvu_socket_rxq_overflow_total",
        MAX_SAFE_INTEGER,
        false,
    )?;

    ensure!(
        recent_peers <= known_peers,
        "recent gossip peer count exceeds known peer count"
    );
    ensure!(
        targets != 0 || (forwarded == 0 && forward_errors == 0),
        "forwarding counters exist without a forwarding target"
    );
    ensure!(
        socket_overflow_supported == socket_overflow.is_some(),
        "shred-reader socket-overflow support and counter disagree"
    );
    let attempts = forwarded
        .checked_add(forward_errors)
        .filter(|value| *value <= MAX_SAFE_INTEGER)
        .context("forwarding attempts exceed the public integer limit")?;
    let gossip_state = if recent_peers > 0 {
        "observed"
    } else {
        "waiting"
    };
    let tvu_state = if packets == 0 || seconds_since_packet.is_none() {
        "waiting"
    } else if seconds_since_packet.is_some_and(|age| age <= config.tvu_active_after.as_secs()) {
        "receiving"
    } else {
        "idle"
    };
    let forwarding_state = if targets == 0 {
        "disabled"
    } else if forwarded > 0 {
        "sending"
    } else if forward_errors > 0 {
        "errors"
    } else {
        "waiting"
    };

    Ok(ReceiverStatus {
        gossip: GossipStatus {
            state: gossip_state.into(),
            recent_peer_count: Some(recent_peers),
            known_peer_count: Some(known_peers),
            tvu_peer_count: Some(tvu_peers),
            shred_version: Some(shred_version),
            receiver_uptime_secs: Some(uptime),
            updated_unix_secs: Some(sampled_unix_secs),
        },
        tvu: TvuStatus {
            state: tvu_state.into(),
            packets_total: Some(packets),
            bytes_total: Some(received_bytes),
            parsed_total: Some(parsed),
            invalid_total: Some(invalid),
            version_mismatch_total: Some(mismatched),
            unique_total: Some(unique),
            duplicates_total: Some(duplicates),
            data_total: Some(data),
            code_total: Some(code),
            socket_rxq_overflow_supported: Some(socket_overflow_supported),
            socket_rxq_overflow_total: socket_overflow,
            latest_slot: (parsed > 0).then_some(latest_slot),
            seconds_since_last_packet: seconds_since_packet,
            updated_unix_secs: Some(sampled_unix_secs),
        },
        forwarding: ForwardingStatus {
            state: forwarding_state.into(),
            target_count: Some(targets),
            attempts_total: Some(attempts),
            successful_datagrams_total: Some(forwarded),
            errors_total: Some(forward_errors),
            updated_unix_secs: Some(sampled_unix_secs),
        },
    })
}

fn unavailable_receiver_status() -> ReceiverStatus {
    ReceiverStatus {
        gossip: GossipStatus {
            state: "unavailable".into(),
            recent_peer_count: None,
            known_peer_count: None,
            tvu_peer_count: None,
            shred_version: None,
            receiver_uptime_secs: None,
            updated_unix_secs: None,
        },
        tvu: TvuStatus {
            state: "unavailable".into(),
            packets_total: None,
            bytes_total: None,
            parsed_total: None,
            invalid_total: None,
            version_mismatch_total: None,
            unique_total: None,
            duplicates_total: None,
            data_total: None,
            code_total: None,
            socket_rxq_overflow_supported: None,
            socket_rxq_overflow_total: None,
            latest_slot: None,
            seconds_since_last_packet: None,
            updated_unix_secs: None,
        },
        forwarding: ForwardingStatus {
            state: "unavailable".into(),
            target_count: None,
            attempts_total: None,
            successful_datagrams_total: None,
            errors_total: None,
            updated_unix_secs: None,
        },
    }
}

fn object<'a>(value: &'a Value, name: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .with_context(|| format!("{name} must be an object"))
}

fn required<'a>(source: &'a Map<String, Value>, key: &str) -> Result<&'a Value> {
    source
        .get(key)
        .with_context(|| format!("required field {key} is missing"))
}

fn required_string<'a>(source: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    required(source, key)?
        .as_str()
        .with_context(|| format!("{key} must be a string"))
}

fn required_bool(source: &Map<String, Value>, key: &str) -> Result<bool> {
    required(source, key)?
        .as_bool()
        .with_context(|| format!("{key} must be a boolean"))
}

fn required_u64(
    source: &Map<String, Value>,
    key: &str,
    maximum: u64,
    positive: bool,
) -> Result<u64> {
    bounded_u64(required(source, key)?, maximum, positive)
        .with_context(|| format!("{key} has an invalid integer value"))
}

fn optional_u64(
    source: &Map<String, Value>,
    key: &str,
    maximum: u64,
    positive: bool,
) -> Result<Option<u64>> {
    let value = required(source, key)?;
    if value.is_null() {
        Ok(None)
    } else {
        bounded_u64(value, maximum, positive)
            .map(Some)
            .with_context(|| format!("{key} has an invalid integer value"))
    }
}

fn bounded_u64(value: &Value, maximum: u64, positive: bool) -> Result<u64> {
    let value = value
        .as_u64()
        .context("value must be an unsigned integer")?;
    ensure!(value <= maximum, "integer exceeds its bound");
    ensure!(!positive || value > 0, "integer must be positive");
    Ok(value)
}

struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

struct StrictValueVisitor;

impl<'de> Visitor<'de> for StrictValueVisitor {
    type Value = StrictValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::Number(Number::from(value))))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::Number(Number::from(value))))
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(|value| StrictValue(Value::Number(value)))
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictValue(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(1024));
        while let Some(value) = sequence.next_element::<StrictValue>()? {
            values.push(value.0);
        }
        Ok(StrictValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut seen = HashSet::with_capacity(object.size_hint().unwrap_or(0).min(1024));
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if !seen.insert(key.clone()) {
                return Err(de::Error::custom("JSON contains a duplicate object key"));
            }
            let value = object.next_value::<StrictValue>()?;
            values.insert(key, value.0);
        }
        Ok(StrictValue(Value::Object(values)))
    }
}

fn validate_absolute_non_root(path: &Path, label: &str) -> Result<()> {
    ensure!(
        path.is_absolute() && path != Path::new("/"),
        "{label} must be absolute and non-root"
    );
    Ok(())
}

pub fn parse_listener(value: &str) -> Result<SocketAddr> {
    let normalized = value
        .strip_prefix("localhost:")
        .map_or_else(|| value.to_owned(), |port| format!("127.0.0.1:{port}"));
    let address: SocketAddr = normalized
        .parse()
        .context("parse shred-status listen address")?;
    let IpAddr::V4(ip) = address.ip() else {
        bail!("shred-status listener must use IPv4");
    };
    ensure!(
        address.port() != 0,
        "shred-status listener port must be non-zero"
    );
    ensure!(
        ip.is_loopback() || ip.is_private() || ip.is_unspecified(),
        "shred-status listener must be loopback, private, or 0.0.0.0"
    );
    Ok(address)
}

pub fn validate_cors_origin(value: &str) -> Result<()> {
    ensure!(value != "*", "CORS origin cannot be a wildcard");
    ensure!(
        !value.contains(['\r', '\n']),
        "CORS origin contains a line break"
    );
    let (scheme, authority) = value
        .split_once("://")
        .context("CORS origin must be an exact HTTP(S) origin")?;
    ensure!(
        matches!(scheme, "http" | "https"),
        "CORS origin must use HTTP(S)"
    );
    ensure!(
        !authority.is_empty()
            && !authority.contains(['/', '?', '#', '@'])
            && !authority.chars().any(char::is_whitespace),
        "CORS origin must contain only an authority"
    );
    let parsed = reqwest::Url::parse(&format!("{value}/")).context("parse CORS origin")?;
    ensure!(parsed.host_str().is_some(), "CORS origin is missing a host");
    ensure!(
        parsed.username().is_empty() && parsed.password().is_none(),
        "CORS origin must not contain credentials"
    );
    ensure!(
        parsed.port().is_none_or(|port| port != 0),
        "CORS origin port must be non-zero"
    );
    Ok(())
}

fn optional_string(cli: Option<String>, environment: &str) -> Option<String> {
    match cli {
        // An explicit empty flag is invalid input, not permission to silently consult a fallback.
        Some(value) => Some(value),
        None => nonempty_environment(environment),
    }
}

fn nonempty_environment(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn numeric_setting(cli: Option<u64>, environment: &str, default: u64) -> Result<u64> {
    match (cli, nonempty_environment(environment)) {
        (Some(value), _) => Ok(value),
        (None, Some(value)) => value
            .parse::<u64>()
            .with_context(|| format!("{environment} must be a positive integer")),
        (None, None) => Ok(default),
    }
}

fn float_setting(cli: Option<f64>, environment: &str, default: f64) -> Result<f64> {
    match (cli, nonempty_environment(environment)) {
        (Some(value), _) => Ok(value),
        (None, Some(value)) => value
            .parse::<f64>()
            .with_context(|| format!("{environment} must be a positive number")),
        (None, None) => Ok(default),
    }
}

async fn fetch_receiver_metrics(config: &ShredStatusConfig, client: &Client) -> Result<Value> {
    ensure!(
        config.receiver_endpoint.address().ip().is_loopback(),
        "receiver metrics endpoint is not loopback"
    );
    let response = client
        .get(config.receiver_endpoint.url())
        .header(ACCEPT, "application/json")
        .header(ACCEPT_ENCODING, "identity")
        .header(CONNECTION, "close")
        .send()
        .await
        .context("fetch shred-reader metrics")?;
    ensure!(
        response.status() == reqwest::StatusCode::OK,
        "shred-reader metrics returned a non-success status"
    );
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .context("shred-reader metrics omitted Content-Type")?
        .to_str()
        .context("shred-reader metrics Content-Type is not ASCII")?;
    ensure!(
        content_type
            .split(';')
            .next()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json")),
        "shred-reader metrics returned a non-JSON response"
    );
    let declared_length = response
        .headers()
        .get(CONTENT_LENGTH)
        .map(|value| {
            value
                .to_str()
                .context("shred-reader metrics Content-Length is not ASCII")?
                .parse::<u64>()
                .context("shred-reader metrics has an invalid Content-Length")
        })
        .transpose()?;
    ensure!(
        declared_length.is_none_or(|length| length <= MAX_SOURCE_JSON_BYTES as u64),
        "shred-reader metrics exceeds its byte limit"
    );

    let mut body = Vec::with_capacity(
        declared_length
            .unwrap_or(0)
            .min(MAX_SOURCE_JSON_BYTES as u64) as usize,
    );
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("read shred-reader metrics response")?;
        ensure!(
            body.len().saturating_add(chunk.len()) <= MAX_SOURCE_JSON_BYTES,
            "shred-reader metrics exceeds its byte limit"
        );
        body.extend_from_slice(&chunk);
    }
    decode_bounded_json(&body)
}

fn read_hivezilla_status(config: &ShredStatusConfig) -> Result<Value> {
    decode_bounded_json(&read_bounded_regular(&config.hivezilla_status_file)?)
}

async fn collect_status(
    config: Arc<ShredStatusConfig>,
    client: &Client,
    now_unix_secs: u64,
    permit: OwnedSemaphorePermit,
) -> Result<(PublicShredStatus, OwnedSemaphorePermit)> {
    let source_config = Arc::clone(&config);
    // The permit travels through the blocking closure. If this async future is cancelled, a
    // running filesystem job retains the sole permit until it actually returns, so a subsequent
    // refresh waits instead of accumulating detached blocking work.
    let source_task = tokio::task::spawn_blocking(move || {
        let source = (|| -> Result<Option<Value>> {
            if let Some(output) = &source_config.output_file {
                validate_distinct_files(&source_config.hivezilla_status_file, output)?;
            }
            Ok(read_hivezilla_status(&source_config).ok())
        })();
        (source, permit)
    });
    let (receiver, source) = tokio::join!(fetch_receiver_metrics(&config, client), source_task);
    let (hivezilla, permit) = source.context("join bounded Hivezilla status read")?;
    let status = build_status_from_values(&config, now_unix_secs, receiver.ok(), hivezilla?)?;
    Ok((status, permit))
}

#[derive(Debug, Default)]
struct CacheState {
    status: Option<Arc<[u8]>>,
    health: Option<Arc<[u8]>>,
    refresh_failed: bool,
}

#[derive(Debug, Default)]
pub struct StatusCache {
    state: RwLock<CacheState>,
}

impl StatusCache {
    pub fn status(&self) -> Option<Arc<[u8]>> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .status
            .clone()
    }

    /// A source being unavailable is a valid fresh sample and therefore healthy. Health becomes
    /// false only when collection cannot publish a new bounded snapshot.
    pub fn health(&self) -> (bool, Arc<[u8]>) {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.status.is_some() && !state.refresh_failed {
            if let Some(body) = &state.health {
                return (true, Arc::clone(body));
            }
        }
        (false, Arc::from(&b"{\"ok\":false}"[..]))
    }

    fn commit(&self, status: Vec<u8>, health: Vec<u8>) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.status = Some(Arc::from(status));
        state.health = Some(Arc::from(health));
        state.refresh_failed = false;
    }

    fn mark_refresh_failed(&self) {
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .refresh_failed = true;
    }
}

#[derive(Clone)]
pub struct ShredStatusService {
    config: Arc<ShredStatusConfig>,
    client: Client,
    cache: Arc<StatusCache>,
    refresh_gate: Arc<Semaphore>,
}

impl ShredStatusService {
    pub fn new(config: ShredStatusConfig) -> Result<Self> {
        config.validate()?;
        let client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .connect_timeout(config.receiver_timeout)
            .timeout(config.receiver_timeout)
            .pool_max_idle_per_host(1)
            .http1_only()
            .build()
            .context("build bounded shred-reader metrics client")?;
        Ok(Self {
            config: Arc::new(config),
            client,
            cache: Arc::new(StatusCache::default()),
            refresh_gate: Arc::new(Semaphore::new(1)),
        })
    }

    pub fn cache(&self) -> Arc<StatusCache> {
        Arc::clone(&self.cache)
    }

    pub async fn refresh_once(&self) -> Result<PublicShredStatus> {
        let permit = Arc::clone(&self.refresh_gate)
            .acquire_owned()
            .await
            .context("acquire shred-status refresh gate")?;
        let result = self.refresh_once_inner(permit).await;
        if result.is_err() {
            self.cache.mark_refresh_failed();
        }
        result
    }

    async fn refresh_once_inner(&self, permit: OwnedSemaphorePermit) -> Result<PublicShredStatus> {
        let now = unix_time_secs()?;
        let (status, permit) =
            collect_status(Arc::clone(&self.config), &self.client, now, permit).await?;
        let encoded = encode_public_status(&status)?;
        let health = encode_health(&status)?;
        let encoded = if self.config.output_file.is_some() {
            let config = Arc::clone(&self.config);
            let write_task = tokio::task::spawn_blocking(move || {
                let result = (|| -> Result<()> {
                    let output = config
                        .output_file
                        .as_deref()
                        .context("public output disappeared from configuration")?;
                    // Recheck aliases immediately before replacement because mount points and
                    // symlinked parent directories can change after startup or source collection.
                    validate_distinct_files(&config.hivezilla_status_file, output)?;
                    write_atomic_public(output, &encoded)
                })();
                (result, permit, encoded)
            });
            let (write_result, permit, encoded) = write_task
                .await
                .context("join atomic public status publication")?;
            write_result?;
            drop(permit);
            encoded
        } else {
            drop(permit);
            encoded
        };
        self.cache.commit(encoded, health);
        Ok(status)
    }

    pub async fn run(self, listen: SocketAddr) -> Result<()> {
        let shutdown = shutdown_signal()?;
        self.run_until(listen, shutdown).await
    }

    pub async fn run_until<F>(self, listen: SocketAddr, shutdown: F) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        validate_listener_address(listen)?;
        let mut shutdown = Box::pin(shutdown);
        tokio::select! {
            initial = self.refresh_once() => {
                initial?;
            }
            _ = &mut shutdown => return Ok(()),
        }
        let listener = bind_listener(listen)?;
        let refresh_service = self.clone();
        let (stop_refresh, refresh_stopped) = oneshot::channel();
        let refresh_task = tokio::spawn(async move {
            refresh_loop(refresh_service, refresh_stopped).await;
        });
        let result = serve_http(
            listener,
            Arc::clone(&self.cache),
            self.config.cors_origin.clone(),
            self.config.max_http_requests,
            self.config.request_header_timeout,
            shutdown,
        )
        .await;
        let _ = stop_refresh.send(());
        let _ = refresh_task.await;
        result
    }
}

pub async fn serve_shred_status(args: ServeShredStatusArgs) -> Result<()> {
    let (listen, config) = ShredStatusConfig::from_args(args)?;
    ShredStatusService::new(config)?.run(listen).await
}

async fn refresh_loop(service: ShredStatusService, mut stopped: oneshot::Receiver<()>) {
    let start = Instant::now() + service.config.interval;
    let mut ticks = interval_at(start, service.config.interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = &mut stopped => return,
            _ = ticks.tick() => {
                tokio::select! {
                    _ = &mut stopped => return,
                    result = service.refresh_once() => {
                        if result.is_err() {
                            // Error values may contain private paths or URLs. Keep the log generic.
                            tracing::warn!("shred-status refresh failed");
                        }
                    }
                }
            }
        }
    }
}

fn encode_health(status: &PublicShredStatus) -> Result<Vec<u8>> {
    let receiver = if status.gossip.state == "unavailable" {
        "unavailable"
    } else {
        "available"
    };
    let value = serde_json::json!({
        "ok": true,
        "updated_unix_secs": status.updated_unix_secs,
        "receiver": receiver,
        "hivezilla": status.hivezilla.availability,
    });
    let encoded = serde_json::to_vec(&value).context("encode shred-status health")?;
    ensure!(
        encoded.len() <= MAX_PUBLIC_JSON_BYTES,
        "health response exceeds its limit"
    );
    Ok(encoded)
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
            interrupt = tokio::signal::ctrl_c() => {
                if interrupt.is_err() {
                    tracing::warn!("shred-status interrupt handler unavailable; retaining SIGTERM fallback");
                    if terminate.recv().await.is_none() {
                        pending::<()>().await;
                    }
                }
            }
            terminated = terminate.recv() => {
                if terminated.is_none() {
                    tracing::warn!("shred-status SIGTERM stream closed; retaining interrupt fallback");
                    if tokio::signal::ctrl_c().await.is_err() {
                        pending::<()>().await;
                    }
                }
            }
        }
    })
}

#[cfg(not(unix))]
fn shutdown_signal() -> Result<impl Future<Output = ()>> {
    Ok(async {
        if tokio::signal::ctrl_c().await.is_err() {
            tracing::warn!("shred-status interrupt handler unavailable");
            pending::<()>().await;
        }
    })
}

fn validate_listener_address(address: SocketAddr) -> Result<()> {
    let IpAddr::V4(ip) = address.ip() else {
        bail!("shred-status listener must use IPv4");
    };
    ensure!(
        address.port() != 0,
        "shred-status listener port must be non-zero"
    );
    ensure!(
        ip.is_loopback() || ip.is_private() || ip.is_unspecified(),
        "shred-status listener must be loopback, private, or 0.0.0.0"
    );
    Ok(())
}

fn bind_listener(address: SocketAddr) -> Result<TcpListener> {
    validate_listener_address(address)?;
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))
        .context("create shred-status listener")?;
    socket
        .set_reuse_address(true)
        .context("set shred-status address reuse")?;
    socket
        .set_nonblocking(true)
        .context("set shred-status listener nonblocking")?;
    socket
        .bind(&address.into())
        .context("bind shred-status listener")?;
    socket
        .listen(LISTEN_BACKLOG)
        .context("listen for shred-status requests")?;
    let listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(listener).context("register shred-status listener")
}

async fn serve_http<F>(
    listener: TcpListener,
    cache: Arc<StatusCache>,
    cors_origin: Option<String>,
    max_requests: usize,
    request_header_timeout: Duration,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()>,
{
    ensure!(max_requests > 0, "HTTP request limit must be non-zero");
    ensure!(
        !request_header_timeout.is_zero(),
        "HTTP header timeout must be non-zero"
    );
    let slots = Arc::new(Semaphore::new(max_requests));
    let mut tasks = JoinSet::new();
    let shutdown: Pin<Box<F>> = Box::pin(shutdown);
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("accept shred-status connection")?;
                let Ok(permit) = Arc::clone(&slots).try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let cache = Arc::clone(&cache);
                let cors_origin = cors_origin.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let _ = stream.set_nodelay(true);
                    let _ = timeout(
                        request_header_timeout,
                        handle_connection(stream, cache, cors_origin.as_deref()),
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
    headers: Vec<(String, String)>,
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

async fn handle_connection(
    stream: TcpStream,
    cache: Arc<StatusCache>,
    cors_origin: Option<&str>,
) -> Result<()> {
    let response = match read_http_request(&stream).await {
        Ok(request) => route_request(&request, &cache, cors_origin),
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
            return parse_http_request(&buffer[..end + 4]);
        }
    }
}

fn parse_http_request(raw: &[u8]) -> std::result::Result<HttpRequest, ReadRequestError> {
    let text = std::str::from_utf8(raw).map_err(|_| ReadRequestError::Malformed)?;
    let mut lines = text.split("\r\n");
    let mut request_line = lines
        .next()
        .ok_or(ReadRequestError::Malformed)?
        .split_ascii_whitespace();
    let method = request_line.next().ok_or(ReadRequestError::Malformed)?;
    let target = request_line.next().ok_or(ReadRequestError::Malformed)?;
    let version = request_line.next().ok_or(ReadRequestError::Malformed)?;
    if request_line.next().is_some()
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
        || method.is_empty()
        || !method.bytes().all(|byte| byte.is_ascii_uppercase())
    {
        return Err(ReadRequestError::Malformed);
    }
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if line.starts_with([' ', '\t']) {
            return Err(ReadRequestError::Malformed);
        }
        let (name, value) = line.split_once(':').ok_or(ReadRequestError::Malformed)?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || value
                .bytes()
                .any(|byte| byte == 0 || byte == b'\r' || byte == b'\n')
        {
            return Err(ReadRequestError::Malformed);
        }
        headers.push((name.to_ascii_lowercase(), value.trim().to_owned()));
    }
    Ok(HttpRequest {
        method: method.to_owned(),
        target: target.to_owned(),
        headers,
    })
}

async fn write_all_nonblocking(stream: &TcpStream, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.writable().await?;
        match stream.try_write(bytes) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "socket write returned zero",
                ));
            }
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
    content_type: bool,
    headers: Vec<(&'static str, String)>,
}

impl HttpResponse {
    fn json(status: u16, reason: &'static str, body: &[u8], send_body: bool) -> Self {
        Self {
            status,
            reason,
            body: Arc::from(body),
            send_body,
            content_type: true,
            headers: Vec::new(),
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\n",
            self.status,
            self.reason,
            self.body.len()
        );
        if self.content_type {
            response.push_str("Content-Type: application/json; charset=utf-8\r\n");
        }
        response.push_str("Cache-Control: no-store\r\n");
        response
            .push_str("Content-Security-Policy: default-src 'none'; frame-ancestors 'none'\r\n");
        response.push_str("Referrer-Policy: no-referrer\r\n");
        response.push_str("X-Content-Type-Options: nosniff\r\n");
        response.push_str("X-Frame-Options: DENY\r\n");
        response.push_str("Connection: close\r\n");
        for (name, value) in &self.headers {
            response.push_str(name);
            response.push_str(": ");
            response.push_str(value);
            response.push_str("\r\n");
        }
        response.push_str("\r\n");
        let mut bytes = response.into_bytes();
        if self.send_body {
            bytes.extend_from_slice(&self.body);
        }
        bytes
    }
}

fn route_request(
    request: &HttpRequest,
    cache: &StatusCache,
    cors_origin: Option<&str>,
) -> HttpResponse {
    let send_body = request.method != "HEAD";
    if !request_target_is_safe(&request.target) {
        return with_cors(
            HttpResponse::json(
                400,
                "Bad Request",
                b"{\"error\":\"invalid request target\"}",
                send_body,
            ),
            request,
            cors_origin,
            false,
        );
    }
    if !origin_is_allowed(request, cors_origin) {
        return HttpResponse::json(
            403,
            "Forbidden",
            b"{\"error\":\"origin not allowed\"}",
            send_body,
        );
    }

    if request.method == "OPTIONS" {
        if !matches!(request.target.as_str(), HEALTH_PATH | STATUS_PATH) {
            return with_cors(
                HttpResponse::json(404, "Not Found", b"{\"error\":\"not found\"}", true),
                request,
                cors_origin,
                false,
            );
        }
        let methods = header_values(request, "access-control-request-method");
        if methods.len() > 1
            || methods
                .first()
                .is_some_and(|method| !matches!(method.as_str(), "GET" | "HEAD"))
        {
            let mut response = HttpResponse::json(
                405,
                "Method Not Allowed",
                b"{\"error\":\"method not allowed\"}",
                true,
            );
            response
                .headers
                .push(("Allow", "GET, HEAD, OPTIONS".into()));
            return with_cors(response, request, cors_origin, false);
        }
        let requested_headers = header_values(request, "access-control-request-headers");
        if requested_headers.len() > 1
            || requested_headers.first().is_some_and(|value| {
                value
                    .split(',')
                    .map(|header| header.trim().to_ascii_lowercase())
                    .any(|header| !matches!(header.as_str(), "accept" | "cache-control" | ""))
            })
        {
            return with_cors(
                HttpResponse::json(
                    403,
                    "Forbidden",
                    b"{\"error\":\"request header not allowed\"}",
                    true,
                ),
                request,
                cors_origin,
                false,
            );
        }
        let response = HttpResponse {
            status: 204,
            reason: "No Content",
            body: Arc::from(&b""[..]),
            send_body: false,
            content_type: false,
            headers: vec![("Allow", "GET, HEAD, OPTIONS".into())],
        };
        return with_cors(response, request, cors_origin, true);
    }

    if !matches!(request.method.as_str(), "GET" | "HEAD") {
        let mut response = HttpResponse::json(
            405,
            "Method Not Allowed",
            b"{\"error\":\"read-only status service\"}",
            true,
        );
        response
            .headers
            .push(("Allow", "GET, HEAD, OPTIONS".into()));
        return with_cors(response, request, cors_origin, false);
    }

    let response = match request.target.as_str() {
        HEALTH_PATH => {
            let (healthy, body) = cache.health();
            HttpResponse {
                status: if healthy { 200 } else { 503 },
                reason: if healthy { "OK" } else { "Service Unavailable" },
                body,
                send_body,
                content_type: true,
                headers: Vec::new(),
            }
        }
        STATUS_PATH => match cache.status() {
            Some(body) => HttpResponse {
                status: 200,
                reason: "OK",
                body,
                send_body,
                content_type: true,
                headers: Vec::new(),
            },
            None => HttpResponse::json(
                503,
                "Service Unavailable",
                b"{\"error\":\"status unavailable\"}",
                send_body,
            ),
        },
        _ => HttpResponse::json(404, "Not Found", b"{\"error\":\"not found\"}", send_body),
    };
    with_cors(response, request, cors_origin, false)
}

fn request_target_is_safe(target: &str) -> bool {
    target.starts_with('/') && !target.starts_with("//") && !target.contains(['?', '#', '\r', '\n'])
}

fn header_values<'a>(request: &'a HttpRequest, name: &str) -> Vec<&'a String> {
    request
        .headers
        .iter()
        .filter_map(|(key, value)| (key == name).then_some(value))
        .collect()
}

fn origin_is_allowed(request: &HttpRequest, configured: Option<&str>) -> bool {
    let origins = header_values(request, "origin");
    origins.is_empty()
        || (origins.len() == 1 && configured.is_some_and(|value| value == origins[0]))
}

fn with_cors(
    mut response: HttpResponse,
    request: &HttpRequest,
    configured: Option<&str>,
    preflight: bool,
) -> HttpResponse {
    let origins = header_values(request, "origin");
    if origins.len() == 1 && configured.is_some_and(|origin| origin == origins[0]) {
        response
            .headers
            .push(("Access-Control-Allow-Origin", origins[0].clone()));
        response.headers.push(("Vary", "Origin".into()));
        if preflight {
            response
                .headers
                .push(("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS".into()));
            response.headers.push((
                "Access-Control-Allow-Headers",
                "Accept, Cache-Control".into(),
            ));
            response
                .headers
                .push(("Access-Control-Max-Age", "600".into()));
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{net::TcpListener as StdTcpListener, os::unix::fs::symlink, sync::mpsc, thread};
    use tempfile::TempDir;

    fn config(root: &Path) -> ShredStatusConfig {
        ShredStatusConfig {
            hivezilla_status_file: root.join("hivezilla.json"),
            receiver_endpoint: ReceiverEndpoint::parse("http://127.0.0.1:9/metrics").unwrap(),
            output_file: None,
            cors_origin: None,
            interval: Duration::from_secs(5),
            receiver_timeout: Duration::from_millis(100),
            hivezilla_stale_after: Duration::from_secs(20),
            tvu_active_after: Duration::from_secs(30),
            max_future_skew: Duration::from_secs(5),
            max_http_requests: 32,
            request_header_timeout: Duration::from_secs(5),
        }
    }

    fn hivezilla(now: u64) -> Value {
        json!({
            "schema_version": 1,
            "updated_unix_secs": now,
            "started_unix_secs": now - 100,
            "state": "receiving",
            "accepted_total": 500,
            "invalid_total": 2,
            "bytes_total": 614400,
            "durable_through_sequence": 7499,
            "latest_slot": 433735944,
            "shred_version": 50093,
            "last_durable_unix_secs": now,
            "spool_bytes": 1048576,
            "spool_max_bytes": 21474836480_u64,
            "filesystem_free_bytes": 42949672960_u64,
            "filesystem_total_bytes": 64424509440_u64,
            "reserve_free_bytes": 2147483648_u64,
            "udp_received_total": 503,
            "udp_received_bytes_total": 618096,
            "ingest_queue_depth_events": 3,
            "ingest_queue_depth_bytes": 3696,
            "ingest_queue_high_water_events": 64,
            "ingest_queue_high_water_bytes": 78848,
            "ingest_queue_capacity_events": 16384,
            "ingest_queue_capacity_bytes": 67108864,
            "ingest_queue_backpressure_events_total": 0,
            "ingest_queue_backpressure_micros_total": 0,
            "ingest_queue_backpressured": false,
            "socket_rxq_overflow_supported": true,
            "socket_rxq_overflow_total": 0,
            "secret": "must-not-leak"
        })
    }

    fn receiver() -> Value {
        json!({
            "identity": "secret-public-node-id",
            "advertised_ip": "203.0.113.25",
            "shred_version": 50093,
            "uptime_seconds": 900,
            "gossip_peers": 2000,
            "recent_gossip_peers": 37,
            "tvu_peers": 1500,
            "packets_total": 1000,
            "bytes_total": 1228800,
            "parsed_total": 990,
            "invalid_total": 3,
            "version_mismatch_total": 7,
            "unique_total": 800,
            "duplicates_total": 190,
            "data_total": 700,
            "code_total": 290,
            "forward_targets": 1,
            "forwarded_datagrams_total": 985,
            "forward_errors_total": 5,
            "tvu_socket_rxq_overflow_supported": true,
            "tvu_socket_rxq_overflow_total": 0,
            "latest_slot": 433735944,
            "seconds_since_last_packet": 2,
            "token": "must-not-leak"
        })
    }

    fn raw_metrics_server(response: Vec<u8>) -> (ReceiverEndpoint, thread::JoinHandle<()>) {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let task = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            let _ = stream.write_all(&response);
        });
        let endpoint = ReceiverEndpoint::parse(&format!("http://{address}/metrics")).unwrap();
        (endpoint, task)
    }

    #[test]
    fn selects_public_fields_and_redacts_unknown_source_data() {
        let root = TempDir::new().unwrap();
        let status = build_status_from_values(
            &config(root.path()),
            2_000,
            Some(receiver()),
            Some(hivezilla(2_000)),
        )
        .unwrap();
        assert_eq!(status.gossip.state, "observed");
        assert_eq!(status.tvu.state, "receiving");
        assert_eq!(status.forwarding.attempts_total, Some(990));
        assert_eq!(status.hivezilla.availability, "available");
        let encoded = String::from_utf8(encode_public_status(&status).unwrap()).unwrap();
        assert!(!encoded.contains("secret-public-node-id"));
        assert!(!encoded.contains("203.0.113.25"));
        assert!(!encoded.contains("must-not-leak"));
    }

    #[test]
    fn canonical_bytes_match_the_retired_migration_fixture() {
        let root = TempDir::new().unwrap();
        let status = build_status_from_values(
            &config(root.path()),
            2_000,
            Some(receiver()),
            Some(hivezilla(2_000)),
        )
        .unwrap();
        let encoded = String::from_utf8(encode_public_status(&status).unwrap()).unwrap();

        // Frozen exact bytes keep the retired implementation's public contract testable
        // without retaining a second runtime implementation.
        let expected = r#"{"forwarding":{"attempts_total":990,"errors_total":5,"state":"sending","successful_datagrams_total":985,"target_count":1,"updated_unix_secs":2000},"gossip":{"known_peer_count":2000,"receiver_uptime_secs":900,"recent_peer_count":37,"shred_version":50093,"state":"observed","tvu_peer_count":1500,"updated_unix_secs":2000},"hivezilla":{"accepted_total":500,"availability":"available","bytes_total":614400,"durable_through_sequence":7499,"filesystem_free_bytes":42949672960,"filesystem_total_bytes":64424509440,"ingest_queue_backpressure_events_total":0,"ingest_queue_backpressure_micros_total":0,"ingest_queue_backpressured":false,"ingest_queue_capacity_bytes":67108864,"ingest_queue_capacity_events":16384,"ingest_queue_depth_bytes":3696,"ingest_queue_depth_events":3,"ingest_queue_high_water_bytes":78848,"ingest_queue_high_water_events":64,"invalid_total":2,"last_durable_unix_secs":2000,"latest_slot":433735944,"reserve_free_bytes":2147483648,"shred_version":50093,"socket_rxq_overflow_supported":true,"socket_rxq_overflow_total":0,"spool_bytes":1048576,"spool_max_bytes":21474836480,"started_unix_secs":1900,"state":"receiving","status_fresh":true,"udp_received_bytes_total":618096,"udp_received_total":503,"updated_unix_secs":2000},"schema_version":1,"tvu":{"bytes_total":1228800,"code_total":290,"data_total":700,"duplicates_total":190,"invalid_total":3,"latest_slot":433735944,"packets_total":1000,"parsed_total":990,"seconds_since_last_packet":2,"socket_rxq_overflow_supported":true,"socket_rxq_overflow_total":0,"state":"receiving","unique_total":800,"updated_unix_secs":2000,"version_mismatch_total":7},"updated_unix_secs":2000}"#;
        assert_eq!(encoded, expected);
    }

    #[test]
    fn sources_fail_independently_and_unavailable_is_a_valid_sample() {
        let root = TempDir::new().unwrap();
        let receiver_missing =
            build_status_from_values(&config(root.path()), 2_000, None, Some(hivezilla(2_000)))
                .unwrap();
        assert_eq!(receiver_missing.gossip.state, "unavailable");
        assert_eq!(receiver_missing.hivezilla.availability, "available");

        let mut invalid_hivezilla = hivezilla(2_000);
        invalid_hivezilla["accepted_total"] = Value::Bool(true);
        let hivezilla_missing = build_status_from_values(
            &config(root.path()),
            2_000,
            Some(receiver()),
            Some(invalid_hivezilla),
        )
        .unwrap();
        assert_eq!(hivezilla_missing.gossip.state, "observed");
        assert_eq!(hivezilla_missing.hivezilla.availability, "unavailable");

        let both_missing =
            build_status_from_values(&config(root.path()), 2_000, None, None).unwrap();
        let cache = StatusCache::default();
        cache.commit(
            encode_public_status(&both_missing).unwrap(),
            encode_health(&both_missing).unwrap(),
        );
        let (healthy, health) = cache.health();
        assert!(healthy);
        let health: Value = serde_json::from_slice(&health).unwrap();
        assert_eq!(health["receiver"], "unavailable");
        assert_eq!(health["hivezilla"], "unavailable");
    }

    #[tokio::test]
    async fn receiver_fetch_refuses_redirects_and_bounds_close_delimited_bodies() {
        let root = TempDir::new().unwrap();
        let client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .build()
            .unwrap();

        let redirect = b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/metrics\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec();
        let (endpoint, task) = raw_metrics_server(redirect);
        let mut redirect_config = config(root.path());
        redirect_config.receiver_endpoint = endpoint;
        let error = fetch_receiver_metrics(&redirect_config, &client)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("non-success status"));
        task.join().unwrap();

        let mut oversized =
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                .to_vec();
        oversized.extend(std::iter::repeat_n(b' ', MAX_SOURCE_JSON_BYTES + 1));
        let (endpoint, task) = raw_metrics_server(oversized);
        let mut oversized_config = config(root.path());
        oversized_config.receiver_endpoint = endpoint;
        let error = fetch_receiver_metrics(&oversized_config, &client)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("exceeds its byte limit"));
        task.join().unwrap();
    }

    #[tokio::test]
    async fn startup_refresh_is_cancelled_promptly_by_shutdown() {
        let root = TempDir::new().unwrap();
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = ReceiverEndpoint::parse(&format!(
            "http://{}/metrics",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        listener.set_nonblocking(true).unwrap();
        let (release, released) = mpsc::channel();
        let server = thread::spawn(move || {
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(1)))
                            .unwrap();
                        let mut request = [0_u8; 4096];
                        let _ = stream.read(&mut request);
                        let _ = released.recv_timeout(Duration::from_secs(2));
                        return;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        if released.try_recv().is_ok() {
                            return;
                        }
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("test receiver accept failed: {error}"),
                }
            }
        });
        let mut service_config = config(root.path());
        service_config.receiver_endpoint = endpoint;
        service_config.receiver_timeout = Duration::from_secs(60);
        let service = ShredStatusService::new(service_config).unwrap();

        let started = Instant::now();
        service
            .run_until("127.0.0.1:65534".parse().unwrap(), async {
                tokio::time::sleep(Duration::from_millis(25)).await;
            })
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_millis(500));
        let _ = release.send(());
        server.join().unwrap();
    }

    #[test]
    fn duplicate_and_oversized_json_are_rejected() {
        assert!(decode_bounded_json(br#"{"schema_version":1,"schema_version":1}"#).is_err());
        assert!(decode_bounded_json(br#"{"nested":{"x":1,"x":2}}"#).is_err());
        assert!(decode_bounded_json(b"").is_err());
        assert!(decode_bounded_json(&vec![b'x'; MAX_SOURCE_JSON_BYTES + 1]).is_err());
    }

    #[test]
    fn stale_state_and_socket_overflow_contract_match_the_sources() {
        let root = TempDir::new().unwrap();
        let mut stale = hivezilla(1_979);
        stale["started_unix_secs"] = json!(1_900);
        stale["last_durable_unix_secs"] = json!(1_979);
        let status =
            build_status_from_values(&config(root.path()), 2_000, Some(receiver()), Some(stale))
                .unwrap();
        assert!(!status.hivezilla.status_fresh);

        let mut inconsistent = receiver();
        inconsistent["tvu_socket_rxq_overflow_supported"] = json!(false);
        let status = build_status_from_values(
            &config(root.path()),
            2_000,
            Some(inconsistent),
            Some(hivezilla(2_000)),
        )
        .unwrap();
        assert_eq!(status.tvu.state, "unavailable");
        assert_eq!(status.hivezilla.socket_rxq_overflow_total, Some(0));
    }

    #[test]
    fn atomic_output_is_mode_0644_and_never_follows_destination_symlink() {
        let root = TempDir::new().unwrap();
        let output = root.path().join("public.json");
        write_atomic_public(&output, b"{}").unwrap();
        assert_eq!(fs::read(&output).unwrap(), b"{}");
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o644
        );

        fs::remove_file(&output).unwrap();
        let private = root.path().join("private");
        fs::write(&private, b"do-not-change").unwrap();
        symlink(&private, &output).unwrap();
        assert!(write_atomic_public(&output, b"new").is_err());
        assert_eq!(fs::read(private).unwrap(), b"do-not-change");
    }

    #[test]
    fn distinct_file_gate_rejects_dot_components_and_symlink_parent_aliases() {
        let root = TempDir::new().unwrap();
        let real = root.path().join("real");
        fs::create_dir(&real).unwrap();
        let alias = root.path().join("alias");
        symlink(&real, &alias).unwrap();

        let source = real.join("not-created.json");
        assert!(validate_distinct_files(&source, &alias.join("not-created.json")).is_err());
        assert!(validate_distinct_files(&source, &real.join("child/../not-created.json")).is_err());
    }

    #[tokio::test]
    async fn missing_source_parent_is_allowed_but_output_parent_must_resolve() {
        let root = TempDir::new().unwrap();
        let mut service_config = config(root.path());
        service_config.hivezilla_status_file = root.path().join("not-mounted/private.json");
        let output = root.path().join("public.json");
        service_config.output_file = Some(output.clone());

        let service = ShredStatusService::new(service_config).unwrap();
        let status = service.refresh_once().await.unwrap();
        assert_eq!(status.hivezilla.availability, "unavailable");
        assert!(output.is_file());
        assert!(service.cache.health().0);

        let mut invalid = config(root.path());
        invalid.output_file = Some(root.path().join("not-mounted/public.json"));
        assert!(ShredStatusService::new(invalid).is_err());
    }

    #[test]
    fn cache_preserves_last_status_but_fails_health_after_publication_error() {
        let cache = StatusCache::default();
        cache.commit(
            b"{\"schema_version\":1}".to_vec(),
            b"{\"ok\":true}".to_vec(),
        );
        assert!(cache.health().0);
        let before = cache.status().unwrap();
        cache.mark_refresh_failed();
        assert!(!cache.health().0);
        assert_eq!(&*cache.status().unwrap(), &*before);
    }

    #[tokio::test]
    async fn real_publication_failure_preserves_old_status_and_fails_health() {
        let root = TempDir::new().unwrap();
        let target = root.path().join("private-target");
        fs::write(&target, b"do-not-change").unwrap();
        let output = root.path().join("public.json");
        symlink(&target, &output).unwrap();
        let mut service_config = config(root.path());
        service_config.output_file = Some(output);
        let service = ShredStatusService::new(service_config).unwrap();
        service.cache.commit(
            b"{\"schema_version\":1}".to_vec(),
            b"{\"ok\":true}".to_vec(),
        );
        let before = service.cache.status().unwrap();

        assert!(service.refresh_once().await.is_err());
        assert!(!service.cache.health().0);
        assert_eq!(&*service.cache.status().unwrap(), &*before);
        assert_eq!(fs::read(target).unwrap(), b"do-not-change");
    }

    fn request(method: &str, target: &str, headers: &[(&str, &str)]) -> HttpRequest {
        HttpRequest {
            method: method.into(),
            target: target.into(),
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_ascii_lowercase(), (*value).into()))
                .collect(),
        }
    }

    #[test]
    fn http_surface_is_read_only_bounded_and_exact_origin_cors() {
        let cache = StatusCache::default();
        cache.commit(
            b"{\"schema_version\":1}".to_vec(),
            b"{\"ok\":true}".to_vec(),
        );
        let get = route_request(
            &request("GET", STATUS_PATH, &[("Origin", "https://watch.example")]),
            &cache,
            Some("https://watch.example"),
        );
        assert_eq!(get.status, 200);
        assert!(get.headers.iter().any(|(name, value)| {
            *name == "Access-Control-Allow-Origin" && value == "https://watch.example"
        }));
        let head = route_request(&request("HEAD", STATUS_PATH, &[]), &cache, None);
        assert_eq!(head.status, 200);
        assert!(!head.send_body);
        let post = route_request(&request("POST", STATUS_PATH, &[]), &cache, None);
        assert_eq!(post.status, 405);
        let query = route_request(
            &request("GET", &format!("{STATUS_PATH}?secret=x"), &[]),
            &cache,
            None,
        );
        assert_eq!(query.status, 400);
        let evil = route_request(
            &request("GET", STATUS_PATH, &[("Origin", "https://evil.example")]),
            &cache,
            Some("https://watch.example"),
        );
        assert_eq!(evil.status, 403);
        let duplicate = route_request(
            &request(
                "GET",
                STATUS_PATH,
                &[
                    ("Origin", "https://watch.example"),
                    ("Origin", "https://watch.example"),
                ],
            ),
            &cache,
            Some("https://watch.example"),
        );
        assert_eq!(duplicate.status, 403);

        let no_origin_preflight = route_request(
            &request(
                "OPTIONS",
                STATUS_PATH,
                &[("Access-Control-Request-Method", "GET")],
            ),
            &cache,
            Some("https://watch.example"),
        );
        assert_eq!(no_origin_preflight.status, 204);
        assert!(
            no_origin_preflight
                .headers
                .iter()
                .all(|(name, _)| !name.starts_with("Access-Control-"))
        );
        let allowed_preflight = route_request(
            &request(
                "OPTIONS",
                STATUS_PATH,
                &[
                    ("Origin", "https://watch.example"),
                    ("Access-Control-Request-Method", "GET"),
                ],
            ),
            &cache,
            Some("https://watch.example"),
        );
        assert!(
            allowed_preflight
                .headers
                .iter()
                .any(|(name, _)| { *name == "Access-Control-Allow-Methods" })
        );
    }

    #[test]
    fn endpoint_listener_and_origin_validation_are_fail_closed() {
        assert!(ReceiverEndpoint::parse("http://127.0.0.1:19090/metrics").is_ok());
        assert!(ReceiverEndpoint::parse("http://[::1]:19090/metrics").is_ok());
        assert!(ReceiverEndpoint::parse("https://127.0.0.1:19090/metrics").is_err());
        assert!(ReceiverEndpoint::parse("http://10.0.0.1:19090/metrics").is_err());
        assert!(ReceiverEndpoint::parse("http://localhost:19090/metrics").is_err());
        assert!(parse_listener("0.0.0.0:8790").is_ok());
        assert!(parse_listener("localhost:8790").is_ok());
        assert!(parse_listener("8.8.8.8:8790").is_err());
        assert!(validate_cors_origin("https://watch.example:8443").is_ok());
        assert!(validate_cors_origin("*").is_err());
        assert!(validate_cors_origin("https://watch.example:0").is_err());
        assert!(validate_cors_origin("https://watch.example/").is_err());
        assert!(validate_cors_origin("https://watch.example?token=x").is_err());
        assert_eq!(
            optional_string(Some(String::new()), "SHRED_STATUS_TEST_UNUSED"),
            Some(String::new())
        );

        let root = TempDir::new().unwrap();
        let mut limits = config(root.path());
        limits.max_http_requests = MAX_HTTP_REQUESTS + 1;
        assert!(limits.validate().is_err());
        limits.max_http_requests = MAX_HTTP_REQUESTS;
        limits.request_header_timeout = MAX_REQUEST_HEADER_TIMEOUT + Duration::from_secs(1);
        assert!(limits.validate().is_err());
        limits.request_header_timeout = REQUEST_HEADER_TIMEOUT;
        limits.max_future_skew = MAX_FUTURE_SKEW + Duration::from_secs(1);
        assert!(limits.validate().is_err());
    }
}
