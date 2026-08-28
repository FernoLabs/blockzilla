//! Strict concurrent HTTP byte stream for the CAR query adapter.
//!
//! [`CarHttpStream`] pins one object with a `HEAD` request, then fetches fixed,
//! closed byte ranges concurrently. It implements [`std::io::Read`] and emits
//! bytes in exact object order, so it can be passed directly to
//! [`crate::query_sdk::CarInstructionSource`].
//!
//! HTTPS is required unless [`CarHttpOptions::allow_http`] is explicitly set.
//! Redirects and proxy discovery are disabled. The server must provide an exact
//! object length and one strong ASCII ETag. Each range response must preserve
//! that ETag and match the requested `Content-Range`, `Content-Length`, and body
//! length.
//!
//! The configured body window is `chunk_bytes * window_chunks`. The default is
//! 256 MiB (eight 32 MiB chunks). This bound covers range body vectors owned by
//! this module. It is not a total process-memory bound: TLS, HTTP, channel,
//! caller, and CAR decoder buffers can coexist with those vectors. Dropping a
//! stream cancels queued work and joins worker threads. An active request can
//! keep `Drop` blocked until the configured request timeout.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    io::{self, Read},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use reqwest::{
    StatusCode, Url,
    blocking::{Client, Response},
    header::{
        ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderMap,
        HeaderName, IF_MATCH, RANGE,
    },
    redirect::Policy,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Default number of range workers.
pub const DEFAULT_HTTP_WORKERS: usize = 4;
/// Default maximum number of scheduled, active, or buffered chunks.
pub const DEFAULT_HTTP_WINDOW_CHUNKS: usize = 8;
/// Default range size: 32 MiB.
pub const DEFAULT_HTTP_CHUNK_BYTES: usize = 32 << 20;
/// Maximum allowed number of range workers.
pub const MAX_HTTP_WORKERS: usize = 16;
/// Maximum allowed number of scheduled, active, or buffered chunks.
pub const MAX_HTTP_WINDOW_CHUNKS: usize = 16;
/// Maximum allowed range size: 32 MiB.
pub const MAX_HTTP_CHUNK_BYTES: usize = 32 << 20;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const OBJECT_BINDING_DOMAIN: &[u8] = b"blockzilla.car-http-object-binding.v1\0";

/// Configuration for [`CarHttpStream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CarHttpOptions {
    /// Concurrent closed-range workers.
    pub workers: usize,
    /// Maximum scheduled, active, or completed-but-not-delivered chunks.
    pub window_chunks: usize,
    /// Bytes in each range except the final range.
    pub chunk_bytes: usize,
    /// TCP and TLS connection timeout.
    pub connect_timeout: Duration,
    /// Timeout for each complete HEAD or GET request.
    pub request_timeout: Duration,
    /// Permit plain HTTP. Keep this false except for controlled local fixtures.
    pub allow_http: bool,
}

impl Default for CarHttpOptions {
    fn default() -> Self {
        Self {
            workers: DEFAULT_HTTP_WORKERS,
            window_chunks: DEFAULT_HTTP_WINDOW_CHUNKS,
            chunk_bytes: DEFAULT_HTTP_CHUNK_BYTES,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            allow_http: false,
        }
    }
}

impl CarHttpOptions {
    /// Return the maximum range-body bytes retained by this module.
    ///
    /// This value excludes TLS, HTTP, channel, caller, and CAR decoder buffers.
    pub fn body_window_bytes(self) -> Result<usize, CarHttpError> {
        self.validate()?;
        self.chunk_bytes
            .checked_mul(self.window_chunks)
            .ok_or(CarHttpError::ArithmeticOverflow("HTTP body window"))
    }

    fn validate(self) -> Result<(), CarHttpError> {
        validate_nonzero_cap("workers", self.workers, MAX_HTTP_WORKERS)?;
        validate_nonzero_cap("window_chunks", self.window_chunks, MAX_HTTP_WINDOW_CHUNKS)?;
        validate_nonzero_cap("chunk_bytes", self.chunk_bytes, MAX_HTTP_CHUNK_BYTES)?;
        if self.workers > self.window_chunks {
            return Err(CarHttpError::InvalidOptions(format!(
                "workers {} exceeds window_chunks {}",
                self.workers, self.window_chunks
            )));
        }
        if self.connect_timeout.is_zero() {
            return Err(CarHttpError::InvalidOptions(
                "connect_timeout must be nonzero".into(),
            ));
        }
        if self.request_timeout.is_zero() {
            return Err(CarHttpError::InvalidOptions(
                "request_timeout must be nonzero".into(),
            ));
        }
        let _ = self
            .chunk_bytes
            .checked_mul(self.window_chunks)
            .ok_or(CarHttpError::ArithmeticOverflow("HTTP body window"))?;
        Ok(())
    }
}

fn validate_nonzero_cap(label: &'static str, value: usize, cap: usize) -> Result<(), CarHttpError> {
    if value == 0 || value > cap {
        return Err(CarHttpError::InvalidOptions(format!(
            "{label} {value} is outside 1..={cap}"
        )));
    }
    Ok(())
}

/// Pinned identity of one HTTP object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CarHttpIdentity {
    /// Normalized URL used for HEAD and range requests.
    pub normalized_url: String,
    /// Exact object length from the pinned HEAD response.
    pub content_length: u64,
    /// Exact strong ASCII ETag from the pinned HEAD response.
    pub strong_etag: String,
    /// Domain-separated deterministic binding over URL, length, and ETag.
    pub object_binding: String,
}

/// A point-in-time copy of exact logical HTTP counters.
///
/// A snapshot taken while workers run is not an atomic multi-counter snapshot.
/// Each field is an exact cumulative counter at the time that field is loaded.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CarHttpStats {
    pub head_requests: u64,
    pub head_responses: u64,
    pub get_requests: u64,
    pub get_responses: u64,
    pub get_body_bytes_received: u64,
    pub chunks_scheduled: u64,
    pub chunks_fetched: u64,
    pub chunks_delivered: u64,
    pub bytes_delivered: u64,
    pub workers_started: u64,
    pub workers_finished: u64,
}

#[derive(Debug, Default)]
struct SharedStats {
    head_requests: AtomicU64,
    head_responses: AtomicU64,
    get_requests: AtomicU64,
    get_responses: AtomicU64,
    get_body_bytes_received: AtomicU64,
    chunks_scheduled: AtomicU64,
    chunks_fetched: AtomicU64,
    chunks_delivered: AtomicU64,
    bytes_delivered: AtomicU64,
    workers_started: AtomicU64,
    workers_finished: AtomicU64,
}

/// Cloneable access to stream counters, including after the stream is dropped.
#[derive(Debug, Clone, Default)]
pub struct CarHttpStatsHandle {
    inner: Arc<SharedStats>,
}

impl CarHttpStatsHandle {
    /// Load the current counter values.
    pub fn snapshot(&self) -> CarHttpStats {
        CarHttpStats {
            head_requests: self.inner.head_requests.load(Ordering::Relaxed),
            head_responses: self.inner.head_responses.load(Ordering::Relaxed),
            get_requests: self.inner.get_requests.load(Ordering::Relaxed),
            get_responses: self.inner.get_responses.load(Ordering::Relaxed),
            get_body_bytes_received: self.inner.get_body_bytes_received.load(Ordering::Relaxed),
            chunks_scheduled: self.inner.chunks_scheduled.load(Ordering::Relaxed),
            chunks_fetched: self.inner.chunks_fetched.load(Ordering::Relaxed),
            chunks_delivered: self.inner.chunks_delivered.load(Ordering::Relaxed),
            bytes_delivered: self.inner.bytes_delivered.load(Ordering::Relaxed),
            workers_started: self.inner.workers_started.load(Ordering::Relaxed),
            workers_finished: self.inner.workers_finished.load(Ordering::Relaxed),
        }
    }
}

/// Construction and strict HTTP protocol errors.
#[derive(Debug, Error)]
pub enum CarHttpError {
    #[error("invalid HTTP stream options: {0}")]
    InvalidOptions(String),
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
    #[error("URL contains credentials")]
    UrlCredentials,
    #[error("URL contains a fragment")]
    UrlFragment,
    #[error("URL scheme {0:?} is not allowed")]
    UnsupportedScheme(String),
    #[error("plain HTTP is disabled")]
    PlainHttpDisabled,
    #[error("failed to construct strict HTTP client: {0}")]
    ClientBuild(#[source] reqwest::Error),
    #[error("{method} request failed: {source}")]
    Request {
        method: &'static str,
        #[source]
        source: reqwest::Error,
    },
    #[error("{method} returned HTTP {actual}; expected {expected}")]
    Status {
        method: &'static str,
        expected: u16,
        actual: u16,
    },
    #[error("{method} response has no {header} header")]
    MissingHeader {
        method: &'static str,
        header: &'static str,
    },
    #[error("{method} response has more than one {header} header")]
    MultipleHeader {
        method: &'static str,
        header: &'static str,
    },
    #[error("{method} response has invalid {header}: {detail}")]
    InvalidHeader {
        method: &'static str,
        header: &'static str,
        detail: String,
    },
    #[error("{1} response uses unsupported content coding {0:?}")]
    ContentCoding(String, &'static str),
    #[error("HEAD response does not contain a strong ASCII ETag")]
    InvalidStrongEtag,
    #[error("range response ETag changed; expected {expected:?}, got {actual:?}")]
    ChangedEtag { expected: String, actual: String },
    #[error(
        "range response does not match the requested Content-Range; expected {expected:?}, got {actual:?}"
    )]
    ContentRangeMismatch { expected: String, actual: String },
    #[error("range response body is short; expected {expected} bytes, got {actual}")]
    ShortBody { expected: usize, actual: usize },
    #[error("range response body exceeds {expected} bytes")]
    LongBody { expected: usize },
    #[error("range response body read failed: {0}")]
    BodyRead(#[source] io::Error),
    #[error("failed to start HTTP worker: {0}")]
    ThreadSpawn(#[source] io::Error),
    #[error("HTTP worker channel closed before the object was delivered")]
    WorkerChannelClosed,
    #[error("HTTP worker protocol error: {0}")]
    WorkerProtocol(String),
    #[error("counter {0} overflowed")]
    CounterOverflow(&'static str),
    #[error("arithmetic overflow while computing {0}")]
    ArithmeticOverflow(&'static str),
}

#[derive(Debug, Clone)]
struct TerminalError {
    kind: io::ErrorKind,
    message: String,
}

impl TerminalError {
    fn from_http(error: CarHttpError) -> Self {
        let kind = match error {
            CarHttpError::ShortBody { .. } => io::ErrorKind::UnexpectedEof,
            CarHttpError::Request { .. } | CarHttpError::BodyRead(_) => io::ErrorKind::Other,
            _ => io::ErrorKind::InvalidData,
        };
        Self {
            kind,
            message: error.to_string(),
        }
    }

    fn io_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

#[derive(Debug, Clone, Copy)]
struct ChunkTask {
    index: u64,
    start: u64,
    end: u64,
}

impl ChunkTask {
    fn len(self) -> Result<usize, CarHttpError> {
        let len = self
            .end
            .checked_sub(self.start)
            .and_then(|value| value.checked_add(1))
            .ok_or(CarHttpError::ArithmeticOverflow("range length"))?;
        usize::try_from(len).map_err(|_| CarHttpError::ArithmeticOverflow("range length"))
    }
}

#[derive(Debug)]
struct ChunkResult {
    task: ChunkTask,
    result: Result<Vec<u8>, CarHttpError>,
}

struct WorkerContext {
    client: Client,
    url: Url,
    etag: String,
    total_length: u64,
    work_rx: Arc<Mutex<Receiver<ChunkTask>>>,
    result_tx: mpsc::Sender<ChunkResult>,
    cancel: Arc<AtomicBool>,
    stats: Arc<SharedStats>,
}

#[derive(Debug)]
struct CurrentChunk {
    index: u64,
    bytes: Vec<u8>,
    position: usize,
}

/// Concurrent strict range reader that exposes one ordered byte stream.
pub struct CarHttpStream {
    identity: CarHttpIdentity,
    options: CarHttpOptions,
    body_window_bytes: usize,
    total_chunks: u64,
    next_schedule_index: u64,
    next_deliver_index: u64,
    bytes_delivered: u64,
    in_flight: usize,
    current: Option<CurrentChunk>,
    pending: BTreeMap<u64, ChunkResult>,
    work_tx: Option<SyncSender<ChunkTask>>,
    result_rx: Receiver<ChunkResult>,
    cancel: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
    stats: CarHttpStatsHandle,
    terminal: Option<TerminalError>,
}

impl std::fmt::Debug for CarHttpStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CarHttpStream")
            .field("identity", &self.identity)
            .field("options", &self.options)
            .field("body_window_bytes", &self.body_window_bytes)
            .field("total_chunks", &self.total_chunks)
            .field("next_schedule_index", &self.next_schedule_index)
            .field("next_deliver_index", &self.next_deliver_index)
            .field("bytes_delivered", &self.bytes_delivered)
            .field("in_flight", &self.in_flight)
            .field("pending_chunks", &self.pending.len())
            .field("has_current_chunk", &self.current.is_some())
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

impl CarHttpStream {
    /// Pin `url` with HEAD and start bounded range-prefetch workers.
    pub fn open(url: &str, options: CarHttpOptions) -> Result<Self, CarHttpError> {
        options.validate()?;
        let body_window_bytes = options.body_window_bytes()?;
        let url = validate_url(url, options.allow_http)?;
        let normalized_url = url.as_str().to_owned();
        let stats = CarHttpStatsHandle::default();
        let client = build_client(options)?;
        let (content_length, strong_etag) = pin_object(&client, &url, &stats.inner)?;
        let identity = CarHttpIdentity {
            object_binding: object_binding(&normalized_url, content_length, &strong_etag),
            normalized_url,
            content_length,
            strong_etag,
        };

        let chunk_bytes = u64::try_from(options.chunk_bytes)
            .map_err(|_| CarHttpError::ArithmeticOverflow("chunk size"))?;
        let total_chunks = content_length
            .checked_add(chunk_bytes - 1)
            .ok_or(CarHttpError::ArithmeticOverflow("chunk count"))?
            / chunk_bytes;

        let (work_tx, work_rx) = mpsc::sync_channel(options.window_chunks);
        let (result_tx, result_rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let shared_work_rx = Arc::new(Mutex::new(work_rx));
        let mut workers = Vec::new();
        workers
            .try_reserve_exact(options.workers)
            .map_err(|_| CarHttpError::ArithmeticOverflow("worker handle allocation"))?;

        if total_chunks != 0 {
            for worker_index in 0..options.workers {
                let worker_client = client.clone();
                let worker_url = url.clone();
                let worker_etag = identity.strong_etag.clone();
                let worker_rx = Arc::clone(&shared_work_rx);
                let worker_results = result_tx.clone();
                let worker_cancel = Arc::clone(&cancel);
                let worker_stats = Arc::clone(&stats.inner);
                let total_length = identity.content_length;
                let spawn = thread::Builder::new()
                    .name(format!("car-http-{worker_index}"))
                    .spawn(move || {
                        worker_stats.workers_started.fetch_add(1, Ordering::Relaxed);
                        worker_loop(WorkerContext {
                            client: worker_client,
                            url: worker_url,
                            etag: worker_etag,
                            total_length,
                            work_rx: worker_rx,
                            result_tx: worker_results,
                            cancel: worker_cancel,
                            stats: worker_stats.clone(),
                        });
                        worker_stats
                            .workers_finished
                            .fetch_add(1, Ordering::Relaxed);
                    });
                match spawn {
                    Ok(worker) => workers.push(worker),
                    Err(error) => {
                        cancel.store(true, Ordering::Release);
                        drop(work_tx);
                        drop(result_tx);
                        for worker in workers {
                            let _ = worker.join();
                        }
                        return Err(CarHttpError::ThreadSpawn(error));
                    }
                }
            }
        }
        drop(result_tx);

        let mut stream = Self {
            identity,
            options,
            body_window_bytes,
            total_chunks,
            next_schedule_index: 0,
            next_deliver_index: 0,
            bytes_delivered: 0,
            in_flight: 0,
            current: None,
            pending: BTreeMap::new(),
            work_tx: Some(work_tx),
            result_rx,
            cancel,
            workers,
            stats,
            terminal: None,
        };
        let initial = options
            .window_chunks
            .min(usize::try_from(total_chunks).unwrap_or(options.window_chunks));
        for _ in 0..initial {
            if let Err(error) = stream.schedule_one() {
                stream.shutdown();
                return Err(error);
            }
        }
        Ok(stream)
    }

    /// Return the pinned object identity.
    pub fn identity(&self) -> &CarHttpIdentity {
        &self.identity
    }

    /// Return the validated options used by this stream.
    pub const fn options(&self) -> CarHttpOptions {
        self.options
    }

    /// Return the configured range-body window in bytes.
    ///
    /// This is not a total process-memory bound.
    pub const fn body_window_bytes(&self) -> usize {
        self.body_window_bytes
    }

    /// Return a cloneable stats handle that remains usable after stream drop.
    pub fn stats_handle(&self) -> CarHttpStatsHandle {
        self.stats.clone()
    }

    /// Return a point-in-time counter snapshot.
    pub fn stats(&self) -> CarHttpStats {
        self.stats.snapshot()
    }

    fn schedule_one(&mut self) -> Result<(), CarHttpError> {
        if self.next_schedule_index >= self.total_chunks {
            return Ok(());
        }
        if self.in_flight >= self.options.window_chunks {
            return Err(CarHttpError::WorkerProtocol(
                "range window invariant was exceeded".into(),
            ));
        }
        let chunk_bytes = u64::try_from(self.options.chunk_bytes)
            .map_err(|_| CarHttpError::ArithmeticOverflow("chunk size"))?;
        let start = self
            .next_schedule_index
            .checked_mul(chunk_bytes)
            .ok_or(CarHttpError::ArithmeticOverflow("range start"))?;
        let end_exclusive = start
            .checked_add(chunk_bytes)
            .ok_or(CarHttpError::ArithmeticOverflow("range end"))?
            .min(self.identity.content_length);
        let end = end_exclusive
            .checked_sub(1)
            .ok_or(CarHttpError::ArithmeticOverflow("closed range end"))?;
        let task = ChunkTask {
            index: self.next_schedule_index,
            start,
            end,
        };
        self.work_tx
            .as_ref()
            .ok_or(CarHttpError::WorkerChannelClosed)?
            .send(task)
            .map_err(|_| CarHttpError::WorkerChannelClosed)?;
        add_counter(&self.stats.inner.chunks_scheduled, 1, "chunks_scheduled")?;
        self.next_schedule_index = self
            .next_schedule_index
            .checked_add(1)
            .ok_or(CarHttpError::ArithmeticOverflow("scheduled chunk index"))?;
        self.in_flight = self
            .in_flight
            .checked_add(1)
            .ok_or(CarHttpError::ArithmeticOverflow("in-flight chunk count"))?;
        Ok(())
    }

    fn take_next_result(&mut self) -> Result<ChunkResult, TerminalError> {
        if let Some(result) = self.pending.remove(&self.next_deliver_index) {
            return Ok(result);
        }
        loop {
            let result = self
                .result_rx
                .recv()
                .map_err(|_| TerminalError::from_http(CarHttpError::WorkerChannelClosed))?;
            if result.task.index < self.next_deliver_index
                || result.task.index >= self.total_chunks
                || self.pending.contains_key(&result.task.index)
            {
                return Err(TerminalError::from_http(CarHttpError::WorkerProtocol(
                    format!(
                        "duplicate or out-of-range result for chunk {}",
                        result.task.index
                    ),
                )));
            }
            if result.task.index == self.next_deliver_index {
                return Ok(result);
            }
            self.pending.insert(result.task.index, result);
            if self.pending.len() > self.options.window_chunks {
                return Err(TerminalError::from_http(CarHttpError::WorkerProtocol(
                    "completed range window invariant was exceeded".into(),
                )));
            }
        }
    }

    fn install_next_chunk(&mut self) -> Result<(), TerminalError> {
        let result = self.take_next_result()?;
        let expected_task = self
            .task_for_index(self.next_deliver_index)
            .map_err(TerminalError::from_http)?;
        if result.task.start != expected_task.start || result.task.end != expected_task.end {
            return Err(TerminalError::from_http(CarHttpError::WorkerProtocol(
                format!(
                    "worker returned altered geometry for chunk {}",
                    result.task.index
                ),
            )));
        }
        let bytes = result.result.map_err(TerminalError::from_http)?;
        self.current = Some(CurrentChunk {
            index: result.task.index,
            bytes,
            position: 0,
        });
        Ok(())
    }

    fn task_for_index(&self, index: u64) -> Result<ChunkTask, CarHttpError> {
        if index >= self.total_chunks {
            return Err(CarHttpError::WorkerProtocol(format!(
                "chunk index {index} exceeds {}",
                self.total_chunks
            )));
        }
        let chunk_bytes = u64::try_from(self.options.chunk_bytes)
            .map_err(|_| CarHttpError::ArithmeticOverflow("chunk size"))?;
        let start = index
            .checked_mul(chunk_bytes)
            .ok_or(CarHttpError::ArithmeticOverflow("range start"))?;
        let end = start
            .checked_add(chunk_bytes)
            .ok_or(CarHttpError::ArithmeticOverflow("range end"))?
            .min(self.identity.content_length)
            .checked_sub(1)
            .ok_or(CarHttpError::ArithmeticOverflow("closed range end"))?;
        Ok(ChunkTask { index, start, end })
    }

    fn finish_current_chunk(&mut self) -> Result<(), TerminalError> {
        let current = self.current.take().ok_or_else(|| {
            TerminalError::from_http(CarHttpError::WorkerProtocol("missing current chunk".into()))
        })?;
        if current.index != self.next_deliver_index || current.position != current.bytes.len() {
            return Err(TerminalError::from_http(CarHttpError::WorkerProtocol(
                "current chunk completion invariant failed".into(),
            )));
        }
        self.next_deliver_index = self.next_deliver_index.checked_add(1).ok_or_else(|| {
            TerminalError::from_http(CarHttpError::ArithmeticOverflow("delivered chunk index"))
        })?;
        self.in_flight = self.in_flight.checked_sub(1).ok_or_else(|| {
            TerminalError::from_http(CarHttpError::WorkerProtocol(
                "in-flight chunk count underflow".into(),
            ))
        })?;
        add_counter(&self.stats.inner.chunks_delivered, 1, "chunks_delivered")
            .map_err(TerminalError::from_http)?;
        self.schedule_one().map_err(TerminalError::from_http)?;
        Ok(())
    }

    fn remember_terminal(&mut self, error: TerminalError) -> io::Error {
        let output = error.io_error();
        self.terminal = Some(error);
        output
    }

    fn shutdown(&mut self) {
        self.cancel.store(true, Ordering::Release);
        self.work_tx.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl Read for CarHttpStream {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if let Some(error) = &self.terminal {
            return Err(error.io_error());
        }
        if self.bytes_delivered == self.identity.content_length {
            return Ok(0);
        }

        let mut written = 0usize;
        while written < output.len() && self.bytes_delivered < self.identity.content_length {
            if self.current.is_none()
                && let Err(error) = self.install_next_chunk()
            {
                let io_error = self.remember_terminal(error);
                return if written == 0 {
                    Err(io_error)
                } else {
                    Ok(written)
                };
            }

            let current = self.current.as_mut().expect("installed above");
            let available = current.bytes.len().saturating_sub(current.position);
            if available == 0 {
                if let Err(error) = self.finish_current_chunk() {
                    let io_error = self.remember_terminal(error);
                    return if written == 0 {
                        Err(io_error)
                    } else {
                        Ok(written)
                    };
                }
                continue;
            }
            let count = available.min(output.len() - written);
            output[written..written + count]
                .copy_from_slice(&current.bytes[current.position..current.position + count]);
            current.position += count;
            written += count;
            let count_u64 = u64::try_from(count)
                .map_err(|_| io::Error::other("delivered byte count exceeds u64"))?;
            self.bytes_delivered = self
                .bytes_delivered
                .checked_add(count_u64)
                .ok_or_else(|| io::Error::other("delivered byte position overflowed"))?;
            if let Err(error) = add_counter(
                &self.stats.inner.bytes_delivered,
                count_u64,
                "bytes_delivered",
            ) {
                let io_error = self.remember_terminal(TerminalError::from_http(error));
                return if written == count {
                    Err(io_error)
                } else {
                    Ok(written - count)
                };
            }
            if current.position == current.bytes.len()
                && let Err(error) = self.finish_current_chunk()
            {
                let io_error = self.remember_terminal(error);
                return if written == 0 {
                    Err(io_error)
                } else {
                    Ok(written)
                };
            }
        }
        Ok(written)
    }
}

impl Drop for CarHttpStream {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn validate_url(input: &str, allow_http: bool) -> Result<Url, CarHttpError> {
    let url = Url::parse(input).map_err(|error| CarHttpError::InvalidUrl(error.to_string()))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(CarHttpError::UrlCredentials);
    }
    if url.fragment().is_some() {
        return Err(CarHttpError::UrlFragment);
    }
    match url.scheme() {
        "https" => {}
        "http" if allow_http => {}
        "http" => return Err(CarHttpError::PlainHttpDisabled),
        scheme => return Err(CarHttpError::UnsupportedScheme(scheme.to_owned())),
    }
    Ok(url)
}

fn build_client(options: CarHttpOptions) -> Result<Client, CarHttpError> {
    Client::builder()
        .https_only(!options.allow_http)
        .redirect(Policy::none())
        .no_proxy()
        .connect_timeout(options.connect_timeout)
        .timeout(options.request_timeout)
        .pool_max_idle_per_host(options.workers)
        .build()
        .map_err(CarHttpError::ClientBuild)
}

fn pin_object(
    client: &Client,
    url: &Url,
    stats: &SharedStats,
) -> Result<(u64, String), CarHttpError> {
    add_counter(&stats.head_requests, 1, "head_requests")?;
    let response = client
        .head(url.clone())
        .header(ACCEPT_ENCODING, "identity")
        .send()
        .map_err(|source| CarHttpError::Request {
            method: "HEAD",
            source,
        })?;
    add_counter(&stats.head_responses, 1, "head_responses")?;
    require_status("HEAD", &response, StatusCode::OK)?;
    require_identity_coding("HEAD", response.headers())?;
    let content_length = parse_u64_header("HEAD", response.headers(), &CONTENT_LENGTH)?;
    let etag = single_header_string("HEAD", response.headers(), &ETAG)?;
    if !is_strong_ascii_etag(&etag) {
        return Err(CarHttpError::InvalidStrongEtag);
    }
    Ok((content_length, etag))
}

fn worker_loop(context: WorkerContext) {
    loop {
        if context.cancel.load(Ordering::Acquire) {
            break;
        }
        let task = match context.work_rx.lock() {
            Ok(receiver) => receiver.recv(),
            Err(_) => break,
        };
        let task = match task {
            Ok(task) => task,
            Err(_) => break,
        };
        if context.cancel.load(Ordering::Acquire) {
            break;
        }
        let result = fetch_range(
            &context.client,
            &context.url,
            &context.etag,
            context.total_length,
            task,
            &context.stats,
        );
        if context
            .result_tx
            .send(ChunkResult { task, result })
            .is_err()
        {
            break;
        }
    }
}

fn fetch_range(
    client: &Client,
    url: &Url,
    etag: &str,
    total_length: u64,
    task: ChunkTask,
    stats: &SharedStats,
) -> Result<Vec<u8>, CarHttpError> {
    let range = format!("bytes={}-{}", task.start, task.end);
    add_counter(&stats.get_requests, 1, "get_requests")?;
    let mut response = client
        .get(url.clone())
        .header(ACCEPT_ENCODING, "identity")
        .header(RANGE, range)
        .header(IF_MATCH, etag)
        .send()
        .map_err(|source| CarHttpError::Request {
            method: "GET",
            source,
        })?;
    add_counter(&stats.get_responses, 1, "get_responses")?;
    require_status("GET", &response, StatusCode::PARTIAL_CONTENT)?;
    require_identity_coding("GET", response.headers())?;

    let actual_etag = single_header_string("GET", response.headers(), &ETAG)?;
    if actual_etag != etag {
        return Err(CarHttpError::ChangedEtag {
            expected: etag.to_owned(),
            actual: actual_etag,
        });
    }
    let expected_content_range = format!("bytes {}-{}/{}", task.start, task.end, total_length);
    let actual_content_range = single_header_string("GET", response.headers(), &CONTENT_RANGE)?;
    if actual_content_range != expected_content_range {
        return Err(CarHttpError::ContentRangeMismatch {
            expected: expected_content_range,
            actual: actual_content_range,
        });
    }
    let expected = task.len()?;
    let actual_content_length = parse_u64_header("GET", response.headers(), &CONTENT_LENGTH)?;
    let expected_u64 =
        u64::try_from(expected).map_err(|_| CarHttpError::ArithmeticOverflow("range length"))?;
    if actual_content_length != expected_u64 {
        return Err(CarHttpError::InvalidHeader {
            method: "GET",
            header: "content-length",
            detail: format!("expected {expected_u64}, got {actual_content_length}"),
        });
    }

    let mut body = Vec::new();
    body.try_reserve_exact(expected)
        .map_err(|_| CarHttpError::ArithmeticOverflow("range body allocation"))?;
    let mut scratch = [0u8; 64 << 10];
    while body.len() < expected {
        let remaining = expected - body.len();
        let read_len = remaining.min(scratch.len());
        let count = response
            .read(&mut scratch[..read_len])
            .map_err(CarHttpError::BodyRead)?;
        if count == 0 {
            break;
        }
        let count_u64 = u64::try_from(count)
            .map_err(|_| CarHttpError::ArithmeticOverflow("received byte count"))?;
        add_counter(
            &stats.get_body_bytes_received,
            count_u64,
            "get_body_bytes_received",
        )?;
        body.extend_from_slice(&scratch[..count]);
    }
    if body.len() < expected {
        return Err(CarHttpError::ShortBody {
            expected,
            actual: body.len(),
        });
    }
    let mut extra = [0u8; 1];
    let extra_count = response.read(&mut extra).map_err(CarHttpError::BodyRead)?;
    if extra_count != 0 {
        add_counter(&stats.get_body_bytes_received, 1, "get_body_bytes_received")?;
        return Err(CarHttpError::LongBody { expected });
    }
    add_counter(&stats.chunks_fetched, 1, "chunks_fetched")?;
    Ok(body)
}

fn require_status(
    method: &'static str,
    response: &Response,
    expected: StatusCode,
) -> Result<(), CarHttpError> {
    if response.status() != expected {
        return Err(CarHttpError::Status {
            method,
            expected: expected.as_u16(),
            actual: response.status().as_u16(),
        });
    }
    Ok(())
}

fn require_identity_coding(method: &'static str, headers: &HeaderMap) -> Result<(), CarHttpError> {
    let mut values = headers.get_all(&CONTENT_ENCODING).iter();
    let Some(first) = values.next() else {
        return Ok(());
    };
    if values.next().is_some() {
        return Err(CarHttpError::MultipleHeader {
            method,
            header: "content-encoding",
        });
    }
    let value = first.to_str().map_err(|_| CarHttpError::InvalidHeader {
        method,
        header: "content-encoding",
        detail: "value is not ASCII".into(),
    })?;
    if !value.eq_ignore_ascii_case("identity") {
        return Err(CarHttpError::ContentCoding(value.to_owned(), method));
    }
    Ok(())
}

fn parse_u64_header(
    method: &'static str,
    headers: &HeaderMap,
    name: &'static HeaderName,
) -> Result<u64, CarHttpError> {
    let value = single_header_string(method, headers, name)?;
    value
        .parse::<u64>()
        .map_err(|_| CarHttpError::InvalidHeader {
            method,
            header: name.as_str(),
            detail: format!("{value:?} is not an unsigned decimal integer"),
        })
}

fn single_header_string(
    method: &'static str,
    headers: &HeaderMap,
    name: &'static HeaderName,
) -> Result<String, CarHttpError> {
    let mut values = headers.get_all(name).iter();
    let first = values.next().ok_or(CarHttpError::MissingHeader {
        method,
        header: name.as_str(),
    })?;
    if values.next().is_some() {
        return Err(CarHttpError::MultipleHeader {
            method,
            header: name.as_str(),
        });
    }
    first
        .to_str()
        .map(str::to_owned)
        .map_err(|_| CarHttpError::InvalidHeader {
            method,
            header: name.as_str(),
            detail: "value is not ASCII".into(),
        })
}

fn is_strong_ascii_etag(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2
        && bytes[0] == b'"'
        && bytes[bytes.len() - 1] == b'"'
        && bytes[1..bytes.len() - 1]
            .iter()
            .all(|byte| matches!(byte, 0x21 | 0x23..=0x7e))
}

fn object_binding(normalized_url: &str, content_length: u64, etag: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(OBJECT_BINDING_DOMAIN);
    digest.update((normalized_url.len() as u64).to_be_bytes());
    digest.update(normalized_url.as_bytes());
    digest.update(content_length.to_be_bytes());
    digest.update((etag.len() as u64).to_be_bytes());
    digest.update(etag.as_bytes());
    let output = digest.finalize();
    let mut encoded = String::with_capacity("car-http-sha256=".len() + output.len() * 2);
    encoded.push_str("car-http-sha256=");
    for byte in output {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn add_counter(counter: &AtomicU64, delta: u64, label: &'static str) -> Result<(), CarHttpError> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(delta)
        })
        .map(|_| ())
        .map_err(|_| CarHttpError::CounterOverflow(label))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::{Read as _, Write as _},
        net::{SocketAddr, TcpListener, TcpStream},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use super::*;

    const FIXTURE_ETAG: &str = "\"fixture-v1\"";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ServerMode {
        Normal,
        OutOfOrder,
        Slow,
        MissingHeadEtag,
        WeakHeadEtag,
        ChangedGetEtag,
        MissingGetEtag,
        BadContentRange,
        ShortBody,
        RedirectHead,
    }

    #[derive(Debug, Default)]
    struct ServerState {
        requests: AtomicUsize,
        redirected_requests: AtomicUsize,
        active_gets: AtomicUsize,
        max_active_gets: AtomicUsize,
        completion_order: Mutex<Vec<u64>>,
    }

    struct TestServer {
        address: SocketAddr,
        url: String,
        state: Arc<ServerState>,
        shutdown: Arc<AtomicBool>,
        accept_thread: Option<thread::JoinHandle<()>>,
    }

    impl TestServer {
        fn start(data: Vec<u8>, mode: ServerMode) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
            listener
                .set_nonblocking(true)
                .expect("make fixture listener nonblocking");
            let address = listener.local_addr().expect("fixture address");
            let state = Arc::new(ServerState::default());
            let shutdown = Arc::new(AtomicBool::new(false));
            let accept_state = Arc::clone(&state);
            let accept_shutdown = Arc::clone(&shutdown);
            let data = Arc::new(data);
            let accept_thread = thread::spawn(move || {
                let mut connections = Vec::new();
                loop {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            if accept_shutdown.load(Ordering::Acquire) {
                                break;
                            }
                            let connection_data = Arc::clone(&data);
                            let connection_state = Arc::clone(&accept_state);
                            connections.push(thread::spawn(move || {
                                handle_connection(
                                    stream,
                                    &connection_data,
                                    mode,
                                    &connection_state,
                                );
                            }));
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            if accept_shutdown.load(Ordering::Acquire) {
                                break;
                            }
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("fixture accept failed: {error}"),
                    }
                }
                for connection in connections {
                    connection.join().expect("fixture connection thread");
                }
            });
            Self {
                address,
                url: format!("http://{address}/epoch.car"),
                state,
                shutdown,
                accept_thread: Some(accept_thread),
            }
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Release);
            let _ = TcpStream::connect(self.address);
            if let Some(thread) = self.accept_thread.take() {
                thread.join().expect("fixture accept thread");
            }
        }
    }

    struct ActiveGet<'a> {
        state: &'a ServerState,
    }

    impl<'a> ActiveGet<'a> {
        fn start(state: &'a ServerState) -> Self {
            let active = state.active_gets.fetch_add(1, Ordering::AcqRel) + 1;
            state.max_active_gets.fetch_max(active, Ordering::AcqRel);
            Self { state }
        }
    }

    impl Drop for ActiveGet<'_> {
        fn drop(&mut self) {
            self.state.active_gets.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn handle_connection(
        mut stream: TcpStream,
        data: &[u8],
        mode: ServerMode,
        state: &ServerState,
    ) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("fixture read timeout");
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .expect("fixture write timeout");
        let Some(request) = read_request(&mut stream) else {
            return;
        };
        state.requests.fetch_add(1, Ordering::Relaxed);
        if request.path == "/redirected" {
            state.redirected_requests.fetch_add(1, Ordering::Relaxed);
        }

        match request.method.as_str() {
            "HEAD" => serve_head(&mut stream, data.len(), mode),
            "GET" => serve_get(&mut stream, data, mode, state, &request.headers),
            _method => write_response(
                &mut stream,
                "405 Method Not Allowed",
                &[("Content-Length", "0".into())],
                &[],
            ),
        }
    }

    #[derive(Debug)]
    struct TestRequest {
        method: String,
        path: String,
        headers: HashMap<String, String>,
    }

    fn read_request(stream: &mut TcpStream) -> Option<TestRequest> {
        let mut bytes = Vec::new();
        let mut scratch = [0u8; 1024];
        let deadline = Instant::now() + Duration::from_secs(2);
        while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = match stream.read(&mut scratch) {
                Ok(0) => return None,
                Ok(count) => count,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    if Instant::now() >= deadline {
                        return None;
                    }
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(error) => panic!("read fixture request: {error}"),
            };
            bytes.extend_from_slice(&scratch[..count]);
            assert!(
                bytes.len() <= 64 << 10,
                "fixture request header is too large"
            );
        }
        let request = std::str::from_utf8(&bytes).expect("ASCII fixture request");
        let mut lines = request.split("\r\n");
        let mut request_line = lines.next().expect("request line").split_ascii_whitespace();
        let method = request_line.next().expect("request method").to_owned();
        let path = request_line.next().expect("request path").to_owned();
        let mut headers = HashMap::new();
        for line in lines.take_while(|line| !line.is_empty()) {
            let (name, value) = line.split_once(':').expect("fixture request header");
            headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
        }
        Some(TestRequest {
            method,
            path,
            headers,
        })
    }

    fn serve_head(stream: &mut TcpStream, length: usize, mode: ServerMode) {
        if mode == ServerMode::RedirectHead {
            write_response(
                stream,
                "302 Found",
                &[
                    ("Location", "/redirected".into()),
                    ("Content-Length", "0".into()),
                ],
                &[],
            );
            return;
        }
        let mut headers = vec![("Content-Length", length.to_string())];
        match mode {
            ServerMode::MissingHeadEtag => {}
            ServerMode::WeakHeadEtag => headers.push(("ETag", "W/\"fixture-v1\"".into())),
            _ => headers.push(("ETag", FIXTURE_ETAG.into())),
        }
        write_response(stream, "200 OK", &headers, &[]);
    }

    fn serve_get(
        stream: &mut TcpStream,
        data: &[u8],
        mode: ServerMode,
        state: &ServerState,
        headers: &HashMap<String, String>,
    ) {
        let _active = ActiveGet::start(state);
        assert_eq!(
            headers.get("if-match").map(String::as_str),
            Some(FIXTURE_ETAG)
        );
        assert_eq!(
            headers.get("accept-encoding").map(String::as_str),
            Some("identity")
        );
        let range = headers
            .get("range")
            .and_then(|value| value.strip_prefix("bytes="))
            .expect("closed range header");
        let (start, end) = range.split_once('-').expect("closed range geometry");
        let start = start.parse::<u64>().expect("range start");
        let end = end.parse::<u64>().expect("range end");
        assert!(start <= end);
        assert!(end < data.len() as u64);

        match mode {
            ServerMode::OutOfOrder if start == 0 => thread::sleep(Duration::from_millis(100)),
            ServerMode::OutOfOrder => thread::sleep(Duration::from_millis(2)),
            ServerMode::Slow => thread::sleep(Duration::from_millis(80)),
            _ => {}
        }

        let expected_body = &data[start as usize..=end as usize];
        let body = if mode == ServerMode::ShortBody {
            &expected_body[..expected_body.len().saturating_sub(1)]
        } else {
            expected_body
        };
        let content_range = if mode == ServerMode::BadContentRange {
            format!("bytes {}-{}/{}", start.saturating_add(1), end, data.len())
        } else {
            format!("bytes {start}-{end}/{}", data.len())
        };
        let mut response_headers = vec![
            ("Content-Range", content_range),
            ("Content-Length", expected_body.len().to_string()),
        ];
        match mode {
            ServerMode::ChangedGetEtag => {
                response_headers.push(("ETag", "\"fixture-v2\"".into()));
            }
            ServerMode::MissingGetEtag => {}
            _ => response_headers.push(("ETag", FIXTURE_ETAG.into())),
        }
        write_response(stream, "206 Partial Content", &response_headers, body);
        state
            .completion_order
            .lock()
            .expect("completion order")
            .push(start);
    }

    fn write_response(
        stream: &mut TcpStream,
        status: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) {
        write!(stream, "HTTP/1.1 {status}\r\n").expect("write fixture status");
        for (name, value) in headers {
            write!(stream, "{name}: {value}\r\n").expect("write fixture header");
        }
        write!(stream, "Connection: close\r\n\r\n").expect("finish fixture header");
        stream.write_all(body).expect("write fixture body");
        stream.flush().expect("flush fixture response");
    }

    fn fixture_options() -> CarHttpOptions {
        CarHttpOptions {
            workers: 4,
            window_chunks: 4,
            chunk_bytes: 1024,
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
            allow_http: true,
        }
    }

    fn fixture_data(length: usize) -> Vec<u8> {
        (0..length)
            .map(|index| ((index * 131 + 17) % 251) as u8)
            .collect()
    }

    #[test]
    fn defaults_match_bounded_network_profile() {
        let options = CarHttpOptions::default();
        assert_eq!(options.workers, 4);
        assert_eq!(options.window_chunks, 8);
        assert_eq!(options.chunk_bytes, 32 << 20);
        assert_eq!(options.body_window_bytes().unwrap(), 256 << 20);
        assert!(!options.allow_http);
    }

    #[test]
    fn out_of_order_fetch_is_delivered_in_order_with_exact_stats() {
        let expected = fixture_data(5 * 1024 + 17);
        let server = TestServer::start(expected.clone(), ServerMode::OutOfOrder);
        let mut stream = CarHttpStream::open(&server.url, fixture_options()).unwrap();
        assert_eq!(stream.identity().content_length, expected.len() as u64);
        assert_eq!(stream.identity().strong_etag, FIXTURE_ETAG);
        assert!(
            stream
                .identity()
                .object_binding
                .starts_with("car-http-sha256=")
        );
        assert_eq!(stream.body_window_bytes(), 4 * 1024);

        let stats = stream.stats_handle();
        let mut actual = Vec::new();
        stream.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, expected);
        drop(stream);

        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot,
            CarHttpStats {
                head_requests: 1,
                head_responses: 1,
                get_requests: 6,
                get_responses: 6,
                get_body_bytes_received: expected.len() as u64,
                chunks_scheduled: 6,
                chunks_fetched: 6,
                chunks_delivered: 6,
                bytes_delivered: expected.len() as u64,
                workers_started: 4,
                workers_finished: 4,
            }
        );
        assert!(server.state.max_active_gets.load(Ordering::Acquire) > 1);
        let completions = server
            .state
            .completion_order
            .lock()
            .expect("completion order");
        assert_ne!(completions.first(), Some(&0));
    }

    #[test]
    fn identity_binding_is_deterministic_and_binds_all_inputs() {
        let first = object_binding("https://example.test/a", 123, "\"one\"");
        assert_eq!(
            first,
            object_binding("https://example.test/a", 123, "\"one\"")
        );
        assert_ne!(
            first,
            object_binding("https://example.test/b", 123, "\"one\"")
        );
        assert_ne!(
            first,
            object_binding("https://example.test/a", 124, "\"one\"")
        );
        assert_ne!(
            first,
            object_binding("https://example.test/a", 123, "\"two\"")
        );
    }

    #[test]
    fn head_requires_one_strong_etag() {
        for (mode, expected) in [
            (ServerMode::MissingHeadEtag, "no etag"),
            (ServerMode::WeakHeadEtag, "strong ascii etag"),
        ] {
            let server = TestServer::start(fixture_data(16), mode);
            let error = CarHttpStream::open(&server.url, fixture_options()).unwrap_err();
            assert!(
                error.to_string().to_ascii_lowercase().contains(expected),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn range_requires_same_etag() {
        let server = TestServer::start(fixture_data(16), ServerMode::ChangedGetEtag);
        let mut stream = CarHttpStream::open(&server.url, fixture_options()).unwrap();
        let error = stream.read_to_end(&mut Vec::new()).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("ETag changed"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn range_requires_etag() {
        let server = TestServer::start(fixture_data(16), ServerMode::MissingGetEtag);
        let mut stream = CarHttpStream::open(&server.url, fixture_options()).unwrap();
        let error = stream.read_to_end(&mut Vec::new()).unwrap_err();
        assert!(error.to_string().contains("no etag"));
    }

    #[test]
    fn range_requires_exact_content_range() {
        let server = TestServer::start(fixture_data(16), ServerMode::BadContentRange);
        let mut stream = CarHttpStream::open(&server.url, fixture_options()).unwrap();
        let error = stream.read_to_end(&mut Vec::new()).unwrap_err();
        assert!(
            error
                .to_string()
                .to_ascii_lowercase()
                .contains("content-range")
        );
    }

    #[test]
    fn range_rejects_short_body() {
        let server = TestServer::start(fixture_data(16), ServerMode::ShortBody);
        let mut stream = CarHttpStream::open(&server.url, fixture_options()).unwrap();
        let error = stream.read_to_end(&mut Vec::new()).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("body") || message.contains("request"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn redirects_are_not_followed() {
        let server = TestServer::start(fixture_data(16), ServerMode::RedirectHead);
        let error = CarHttpStream::open(&server.url, fixture_options()).unwrap_err();
        assert!(error.to_string().contains("HTTP 302"));
        thread::sleep(Duration::from_millis(10));
        assert_eq!(server.state.redirected_requests.load(Ordering::Relaxed), 0);
        assert_eq!(server.state.requests.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn plain_http_is_rejected_before_a_request_by_default() {
        let server = TestServer::start(fixture_data(16), ServerMode::Normal);
        let error = CarHttpStream::open(&server.url, CarHttpOptions::default()).unwrap_err();
        assert!(matches!(error, CarHttpError::PlainHttpDisabled));
        thread::sleep(Duration::from_millis(10));
        assert_eq!(server.state.requests.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn options_enforce_thread_window_and_chunk_bounds() {
        let valid = fixture_options();
        assert!(valid.body_window_bytes().is_ok());
        for invalid in [
            CarHttpOptions {
                workers: 0,
                ..valid
            },
            CarHttpOptions {
                workers: MAX_HTTP_WORKERS + 1,
                window_chunks: MAX_HTTP_WORKERS + 1,
                ..valid
            },
            CarHttpOptions {
                workers: 4,
                window_chunks: 3,
                ..valid
            },
            CarHttpOptions {
                window_chunks: 0,
                ..valid
            },
            CarHttpOptions {
                chunk_bytes: 0,
                ..valid
            },
            CarHttpOptions {
                chunk_bytes: MAX_HTTP_CHUNK_BYTES + 1,
                ..valid
            },
            CarHttpOptions {
                connect_timeout: Duration::ZERO,
                ..valid
            },
            CarHttpOptions {
                request_timeout: Duration::ZERO,
                ..valid
            },
        ] {
            assert!(invalid.body_window_bytes().is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn early_drop_cancels_queued_work_and_joins_workers() {
        let server = TestServer::start(fixture_data(32 * 1024), ServerMode::Slow);
        let options = CarHttpOptions {
            workers: 2,
            window_chunks: 4,
            ..fixture_options()
        };
        let stream = CarHttpStream::open(&server.url, options).unwrap();
        let stats = stream.stats_handle();
        drop(stream);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.head_requests, 1);
        assert_eq!(snapshot.head_responses, 1);
        assert_eq!(snapshot.chunks_scheduled, 4);
        assert!(snapshot.get_requests <= 2);
        assert_eq!(snapshot.workers_started, 2);
        assert_eq!(snapshot.workers_finished, 2);
    }

    #[test]
    fn empty_object_is_a_valid_empty_ordered_stream() {
        let server = TestServer::start(Vec::new(), ServerMode::Normal);
        let mut stream = CarHttpStream::open(&server.url, fixture_options()).unwrap();
        let stats = stream.stats_handle();
        assert_eq!(stream.read(&mut [0u8; 1]).unwrap(), 0);
        drop(stream);
        assert_eq!(
            stats.snapshot(),
            CarHttpStats {
                head_requests: 1,
                head_responses: 1,
                ..CarHttpStats::default()
            }
        );
    }
}
