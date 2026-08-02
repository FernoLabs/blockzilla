//! Private, bounded scheduler incident recorder.
//!
//! This observer reads scheduler status/control files and procfs counters. It
//! never opens a management socket, sends a signal, or retains command lines.
//! Its private JSONL output is mode `0600`; a separate public projection must
//! be used before any incident is exposed by the watcher.

use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::OnceLock,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::{
    fd::AsRawFd,
    unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
};

const MIB: f64 = 1024.0 * 1024.0;
const MAX_EVENT_BYTES: usize = 256 * 1024;
const MAX_PRIVATE_LOG_BYTES: u64 = 10 * 1024 * 1024;
const MAX_PROC_FILE_BYTES: u64 = 64 * 1024;
const MAX_PROC_ENTRIES: usize = 131_072;
const MAX_CONTROL_BATCH_BYTES: usize = 4 * 1024 * 1024;
const MAX_CONTROL_LINES_PER_SAMPLE: usize = 512;
const MAX_PRIORITY_JOBS: usize = 1_024;
const MAX_CONTROL_TARGET_CHARS: usize = 1_024;
const MAX_PROCESS_LIMIT: usize = 128;
const MAX_RING_SAMPLES: usize = 4_096;
const MAX_SUBJECT_CHARS: usize = 128;
const MEMORY_PAUSE_KIB: u64 = 2_621_440;
const LOAD_PAUSE: f64 = 10.0;

const BACKFILL_STATES: &[&str] = &[
    "starting",
    "waiting_for_resources",
    "running",
    "paused_for_resources",
    "failed",
    "complete",
];

#[derive(Debug, Clone, Args)]
pub struct RecordArgs {
    /// Private block-time-gap backfill status JSON.
    #[arg(long)]
    backfill_status: PathBuf,

    /// Scheduler control-events JSONL.
    #[arg(long)]
    control_events: PathBuf,

    /// Optional scheduler priority-lease JSON.
    #[arg(long)]
    priority_lease: Option<PathBuf>,

    /// Private recorder checkpoint, atomically replaced with mode 0600.
    #[arg(long)]
    state_file: PathBuf,

    /// Private bounded incident JSONL, written with mode 0600.
    #[arg(long)]
    events_output: PathBuf,

    /// Sampling interval in seconds.
    #[arg(long, default_value_t = 5.0, value_parser = positive_number)]
    interval_secs: f64,

    /// Duration of pre-transition host summaries retained in memory.
    #[arg(long, default_value_t = 30.0, value_parser = nonnegative_number)]
    ring_secs: f64,

    /// Maximum active and blocked process rows retained per incident.
    #[arg(long, default_value_t = 20)]
    process_limit: usize,

    /// Procfs root. This override supports isolated fixtures.
    #[arg(long, default_value = "/proc", hide = true)]
    proc_root: PathBuf,

    /// Sample once and exit.
    #[arg(long)]
    once: bool,
}

fn positive_number(value: &str) -> std::result::Result<f64, String> {
    let parsed = nonnegative_number(value)?;
    if parsed == 0.0 {
        return Err("value must be positive".to_string());
    }
    Ok(parsed)
}

fn nonnegative_number(value: &str) -> std::result::Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| "value must be a number".to_string())?;
    if !parsed.is_finite() || parsed < 0.0 {
        return Err("value must be non-negative and finite".to_string());
    }
    Ok(parsed)
}

#[derive(Clone, Debug, Default, Deserialize)]
struct BackfillStatus {
    state: Option<String>,
    updated_unix_seconds: Option<u64>,
    current: Option<BackfillCurrent>,
    resources: Option<BackfillResources>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct BackfillCurrent {
    epoch: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct BackfillResources {
    mem_available_kib: Option<u64>,
    io_full_avg10: Option<f64>,
    io_pause_full_avg10: Option<f64>,
    load1: Option<f64>,
    finalizer_active: Option<bool>,
    live_capture_age_seconds: Option<u64>,
    live_capture_max_stale_seconds: Option<u64>,
    live_capture_healthy: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct PriorityLease {
    state: Option<String>,
    jobs: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Checkpoint {
    #[serde(default = "schema_one")]
    schema_version: u32,
    updated_unix_secs: Option<u64>,
    backfill_state: Option<String>,
    control_inode: Option<u64>,
    control_offset: Option<u64>,
    #[serde(default)]
    control_discarding_line: bool,
    process_accessible_count: Option<usize>,
    process_inaccessible_count: Option<usize>,
}

const fn schema_one() -> u32 {
    1
}

#[derive(Clone, Debug, Deserialize)]
struct RawControlEvent {
    at_unix_secs: u64,
    action: String,
    target: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct HostMetrics {
    memory_available_bytes: Option<u64>,
    io_full_avg10: Option<f64>,
    io_pause_threshold: Option<f64>,
    load1: Option<f64>,
    load_pause_threshold: f64,
    finalizer_active: Option<bool>,
    live_capture_age_secs: Option<u64>,
    live_capture_max_stale_secs: Option<u64>,
    live_capture_healthy: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
enum Reason {
    Code {
        code: &'static str,
    },
    Measured {
        code: &'static str,
        observed: Option<Value>,
        threshold: Option<Value>,
        unit: &'static str,
    },
}

impl Reason {
    fn code(code: &'static str) -> Self {
        Self::Code { code }
    }

    fn u64(
        code: &'static str,
        observed: Option<u64>,
        threshold: Option<u64>,
        unit: &'static str,
    ) -> Self {
        Self::Measured {
            code,
            observed: observed.map(Value::from),
            threshold: threshold.map(Value::from),
            unit,
        }
    }

    fn f64(code: &'static str, observed: f64, threshold: f64, unit: &'static str) -> Self {
        debug_assert!(observed.is_finite() && threshold.is_finite());
        Self::Measured {
            code,
            observed: Some(Value::from(observed)),
            threshold: Some(Value::from(threshold)),
            unit,
        }
    }

    #[cfg(test)]
    fn label(&self) -> &'static str {
        match self {
            Self::Code { code } | Self::Measured { code, .. } => code,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct ProcessContext {
    sample_window_secs: f64,
    active_count: usize,
    inaccessible_count: usize,
    truncated: bool,
    top_processes: Vec<ProcessRate>,
    blocked_processes: Vec<ProcessRate>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct ProcessRate {
    pid: u32,
    start_ticks: u64,
    name: String,
    uid: u32,
    state: String,
    read_mib_per_sec: Option<f64>,
    write_mib_per_sec: Option<f64>,
    cpu_percent: Option<f64>,
    rss_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct SampleSummary {
    at_unix_secs: u64,
    backfill_state: Option<String>,
    #[serde(flatten)]
    metrics: HostMetrics,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Incident {
    schema_version: u32,
    id: String,
    at_unix_secs: u64,
    component: &'static str,
    kind: &'static str,
    subject: String,
    epoch: Option<u64>,
    actor: &'static str,
    reasons: Vec<Reason>,
    metrics: HostMetrics,
    process_context: Option<ProcessContext>,
    prelude: Vec<SampleSummary>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ProcessIdentity {
    pid: u32,
    start_ticks: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProcessCounter {
    pid: u32,
    name: String,
    uid: u32,
    state: String,
    ppid: u32,
    cpu_ticks: u64,
    start_ticks: u64,
    rss_pages: u64,
    read_bytes: Option<u64>,
    write_bytes: Option<u64>,
}

#[derive(Debug)]
struct ProcessCollection {
    counters: BTreeMap<ProcessIdentity, ProcessCounter>,
    inaccessible: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct ProcStat {
    state: String,
    ppid: u32,
    cpu_ticks: u64,
    start_ticks: u64,
    rss_pages: u64,
}

#[derive(Debug)]
struct ControlBatch {
    values: Vec<RawControlEvent>,
    inode: u64,
    offset: u64,
    discarding_line: bool,
}

struct Recorder {
    previous_backfill_state: Option<String>,
    control_inode: u64,
    control_offset: u64,
    control_discarding_line: bool,
    prelude: VecDeque<SampleSummary>,
    previous_processes: BTreeMap<ProcessIdentity, ProcessCounter>,
    previous_monotonic: Instant,
    clock_ticks: u64,
    page_size: u64,
    process_limit: usize,
    ring_limit: usize,
}

pub async fn record(args: RecordArgs) -> Result<()> {
    validate_args(&args)?;
    let state_parent = parent_or_dot(&args.state_file);
    fs::create_dir_all(state_parent)
        .with_context(|| format!("create recorder state directory {}", state_parent.display()))?;
    #[cfg(unix)]
    {
        ensure!(
            fs::symlink_metadata(state_parent)?.file_type().is_dir(),
            "recorder state parent is not a directory"
        );
        fs::set_permissions(state_parent, fs::Permissions::from_mode(0o700))?;
    }
    fs::create_dir_all(parent_or_dot(&args.events_output)).with_context(|| {
        format!(
            "create scheduler incident directory {}",
            parent_or_dot(&args.events_output).display()
        )
    })?;
    let _lock = RecorderLock::acquire(&state_parent.join("recorder.lock"))?;
    let mut recorder = Recorder::initialize(&args)?;
    let interval = interval_duration(args.interval_secs)?;

    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install scheduler incident SIGTERM handler")?;

    loop {
        let sample_started = Instant::now();
        recorder.sample(&args)?;
        if args.once {
            break;
        }
        let remaining = interval.saturating_sub(sample_started.elapsed());
        #[cfg(unix)]
        tokio::select! {
            _ = tokio::time::sleep(remaining) => {}
            result = tokio::signal::ctrl_c() => {
                result.context("wait for scheduler incident SIGINT")?;
                break;
            }
            _ = terminate.recv() => break,
        }
        #[cfg(not(unix))]
        tokio::select! {
            _ = tokio::time::sleep(remaining) => {}
            result = tokio::signal::ctrl_c() => {
                result.context("wait for scheduler incident interrupt")?;
                break;
            }
        }
    }
    Ok(())
}

fn validate_args(args: &RecordArgs) -> Result<()> {
    ensure!(
        args.interval_secs.is_finite() && args.interval_secs > 0.0,
        "--interval-secs must be positive and finite"
    );
    ensure!(
        args.ring_secs.is_finite() && args.ring_secs >= args.interval_secs,
        "--ring-secs must be finite and cover one interval"
    );
    ensure!(
        (1..=MAX_PROCESS_LIMIT).contains(&args.process_limit),
        "--process-limit must be between 1 and {MAX_PROCESS_LIMIT}"
    );
    let ring_size = ring_size(args.ring_secs, args.interval_secs)?;
    ensure!(
        ring_size <= MAX_RING_SAMPLES,
        "sampling configuration exceeds {MAX_RING_SAMPLES} retained summaries"
    );
    ensure!(
        args.state_file != args.events_output,
        "--state-file and --events-output must differ"
    );
    for (output, output_label) in [
        (&args.state_file, "--state-file"),
        (&args.events_output, "--events-output"),
    ] {
        for (input, input_label) in [
            (&args.backfill_status, "--backfill-status"),
            (&args.control_events, "--control-events"),
        ] {
            ensure!(
                output != input,
                "{output_label} must differ from {input_label}"
            );
        }
        if let Some(priority_lease) = &args.priority_lease {
            ensure!(
                output != priority_lease,
                "{output_label} must differ from --priority-lease"
            );
        }
    }
    for (path, label) in [
        (&args.backfill_status, "--backfill-status"),
        (&args.control_events, "--control-events"),
        (&args.state_file, "--state-file"),
        (&args.events_output, "--events-output"),
        (&args.proc_root, "--proc-root"),
    ] {
        validate_absolute_non_root(path, label)?;
    }
    if let Some(path) = &args.priority_lease {
        validate_absolute_non_root(path, "--priority-lease")?;
    }
    interval_duration(args.interval_secs)?;
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

fn validate_absolute_non_root(path: &Path, label: &str) -> Result<()> {
    ensure!(path.is_absolute(), "{label} must be an absolute path");
    ensure!(
        path != Path::new("/"),
        "{label} must not be the filesystem root"
    );
    ensure!(
        path.components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_))),
        "{label} contains a forbidden path component"
    );
    Ok(())
}

fn ring_size(ring_secs: f64, interval_secs: f64) -> Result<usize> {
    let samples = (ring_secs / interval_secs).ceil();
    ensure!(
        samples.is_finite() && samples >= 0.0,
        "invalid sampling window"
    );
    Ok((samples as usize).saturating_add(1).max(2))
}

impl Recorder {
    fn initialize(args: &RecordArgs) -> Result<Self> {
        let checkpoint = read_optional_json::<Checkpoint>(&args.state_file, MAX_EVENT_BYTES as u64)
            .unwrap_or_default();
        let previous_backfill_state = checkpoint
            .backfill_state
            .filter(|state| BACKFILL_STATES.contains(&state.as_str()));
        let (control_inode, control_offset, control_discarding_line) =
            if let Some(inode) = checkpoint.control_inode {
                (
                    inode,
                    checkpoint.control_offset.unwrap_or(0),
                    checkpoint.control_discarding_line,
                )
            } else {
                match regular_file_identity(&args.control_events)? {
                    Some((inode, size)) => (inode, size, false),
                    None => (0, 0, false),
                }
            };
        let (clock_ticks, page_size) = system_parameters()?;
        let ring_limit = ring_size(args.ring_secs, args.interval_secs)?;
        Ok(Self {
            previous_backfill_state,
            control_inode,
            control_offset,
            control_discarding_line,
            prelude: VecDeque::with_capacity(ring_limit),
            previous_processes: BTreeMap::new(),
            previous_monotonic: Instant::now(),
            clock_ticks,
            page_size,
            process_limit: args.process_limit,
            ring_limit,
        })
    }

    fn sample(&mut self, args: &RecordArgs) -> Result<()> {
        let sample_started = Instant::now();
        let now = unix_now()?;
        let backfill =
            read_optional_json::<BackfillStatus>(&args.backfill_status, MAX_EVENT_BYTES as u64)
                .unwrap_or_default();
        let lease = args
            .priority_lease
            .as_deref()
            .and_then(|path| read_optional_json::<PriorityLease>(path, MAX_EVENT_BYTES as u64));
        let collection = collect_process_counters(&args.proc_root)?;
        let elapsed = sample_started
            .saturating_duration_since(self.previous_monotonic)
            .as_secs_f64()
            .max(0.001);
        let context = process_rates(
            &self.previous_processes,
            &collection.counters,
            elapsed,
            self.clock_ticks,
            self.page_size,
            collection.inaccessible,
            self.process_limit,
        );
        let metrics = host_metrics(&backfill, &args.proc_root);
        let backfill_state = backfill
            .state
            .as_deref()
            .filter(|state| BACKFILL_STATES.contains(state))
            .map(str::to_string);
        let epoch = backfill.current.as_ref().and_then(|current| current.epoch);
        self.prelude.push_back(SampleSummary {
            at_unix_secs: now,
            backfill_state: backfill_state.clone(),
            metrics: metrics.clone(),
        });
        while self.prelude.len() > self.ring_limit {
            self.prelude.pop_front();
        }

        if self.previous_backfill_state != backfill_state
            && let Some(state) = &backfill_state
        {
            let at = backfill.updated_unix_seconds.unwrap_or(now);
            let prelude = tail_prelude(&self.prelude);
            let incident = match state.as_str() {
                "paused_for_resources" => Some(make_event(
                    at,
                    "block_time_gap_backfill",
                    "paused",
                    "backfill",
                    epoch,
                    "automatic",
                    classify_backfill_pause(&metrics),
                    metrics.clone(),
                    Some(context.clone()),
                    prelude,
                )),
                "running"
                    if self.previous_backfill_state.as_deref() == Some("paused_for_resources") =>
                {
                    Some(make_event(
                        at,
                        "block_time_gap_backfill",
                        "resumed",
                        "backfill",
                        epoch,
                        "automatic",
                        vec![Reason::code("resources_recovered")],
                        metrics.clone(),
                        None,
                        Vec::new(),
                    ))
                }
                "failed" => Some(make_event(
                    at,
                    "block_time_gap_backfill",
                    "failed",
                    "backfill",
                    epoch,
                    "automatic",
                    vec![Reason::code("extractor_failed")],
                    metrics.clone(),
                    Some(context.clone()),
                    prelude,
                )),
                "complete" => Some(make_event(
                    at,
                    "block_time_gap_backfill",
                    "completed",
                    "backfill",
                    epoch,
                    "automatic",
                    vec![Reason::code("backfill_complete")],
                    metrics.clone(),
                    None,
                    Vec::new(),
                )),
                _ => None,
            };
            if let Some(incident) = incident {
                append_jsonl(&args.events_output, &incident)?;
            }
            self.previous_backfill_state = Some(state.clone());
        }

        let batch = read_new_control_jsonl(
            &args.control_events,
            self.control_inode,
            self.control_offset,
            self.control_discarding_line,
        )?;
        self.control_inode = batch.inode;
        self.control_offset = batch.offset;
        self.control_discarding_line = batch.discarding_line;
        let lease_jobs = active_lease_jobs(lease.as_ref());
        let prelude = tail_prelude(&self.prelude);
        for value in batch.values {
            if let Some(incident) = control_event(
                &value,
                lease.as_ref(),
                &lease_jobs,
                &metrics,
                &context,
                &prelude,
            ) {
                append_jsonl(&args.events_output, &incident)?;
            }
        }

        let checkpoint = Checkpoint {
            schema_version: 1,
            updated_unix_secs: Some(now),
            backfill_state: self.previous_backfill_state.clone(),
            control_inode: Some(self.control_inode),
            control_offset: Some(self.control_offset),
            control_discarding_line: self.control_discarding_line,
            process_accessible_count: Some(
                collection
                    .counters
                    .len()
                    .saturating_sub(collection.inaccessible),
            ),
            process_inaccessible_count: Some(collection.inaccessible),
        };
        atomic_json(&args.state_file, &checkpoint, 0o600)?;
        self.previous_processes = collection.counters;
        self.previous_monotonic = sample_started;
        Ok(())
    }
}

fn tail_prelude(prelude: &VecDeque<SampleSummary>) -> Vec<SampleSummary> {
    prelude
        .iter()
        .skip(prelude.len().saturating_sub(4))
        .cloned()
        .collect()
}

fn unix_now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs())
}

#[cfg(unix)]
fn system_parameters() -> Result<(u64, u64)> {
    // SAFETY: sysconf has no pointer arguments. Non-positive values are
    // rejected before use.
    let clock_ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    ensure!(
        clock_ticks > 0 && page_size > 0,
        "could not resolve procfs parameters"
    );
    Ok((clock_ticks as u64, page_size as u64))
}

#[cfg(not(unix))]
fn system_parameters() -> Result<(u64, u64)> {
    bail!("scheduler incident collection requires a Unix procfs host")
}

fn read_optional_json<T: for<'de> Deserialize<'de>>(path: &Path, limit: u64) -> Option<T> {
    let raw = read_bounded_regular(path, limit).ok()?;
    serde_json::from_slice(&raw).ok()
}

fn read_bounded_regular(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        path.display()
    );
    ensure!(
        metadata.len() > 0 && metadata.len() <= limit,
        "input size is outside its limit"
    );
    let mut raw = Vec::with_capacity(metadata.len() as usize);
    file.take(limit + 1).read_to_end(&mut raw)?;
    ensure!(
        !raw.is_empty() && raw.len() <= limit as usize,
        "input size is outside its limit"
    );
    Ok(raw)
}

fn read_bounded_proc(path: &Path) -> Result<String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path)?;
    ensure!(
        file.metadata()?.is_file(),
        "procfs input is not a regular file"
    );
    let mut raw = Vec::new();
    file.take(MAX_PROC_FILE_BYTES + 1).read_to_end(&mut raw)?;
    ensure!(
        raw.len() <= MAX_PROC_FILE_BYTES as usize,
        "procfs input exceeds its limit"
    );
    String::from_utf8(raw).context("procfs input is not UTF-8")
}

fn parse_proc_stat(value: &str) -> Result<ProcStat> {
    let opening = value.find('(').context("invalid proc stat")?;
    let closing = value.rfind(')').context("invalid proc stat")?;
    ensure!(opening > 0 && closing > opening, "invalid proc stat");
    let fields = value
        .get(closing + 1..)
        .context("invalid proc stat suffix")?
        .split_whitespace()
        .collect::<Vec<_>>();
    ensure!(fields.len() >= 22, "short proc stat");
    let user_ticks = fields[11].parse::<u64>().context("parse proc user ticks")?;
    let system_ticks = fields[12]
        .parse::<u64>()
        .context("parse proc system ticks")?;
    let rss = fields[21].parse::<i128>().context("parse proc RSS pages")?;
    Ok(ProcStat {
        state: fields[0].chars().take(1).collect(),
        ppid: fields[1].parse::<u32>().context("parse proc parent PID")?,
        cpu_ticks: user_ticks.saturating_add(system_ticks),
        start_ticks: fields[19]
            .parse::<u64>()
            .context("parse proc start ticks")?,
        rss_pages: u64::try_from(rss.max(0)).unwrap_or(u64::MAX),
    })
}

fn parse_proc_io(value: &str) -> Result<(u64, u64)> {
    let mut read_bytes = None;
    let mut write_bytes = None;
    for line in value.lines() {
        let Some((key, raw)) = line.split_once(':') else {
            continue;
        };
        if !matches!(key, "read_bytes" | "write_bytes") {
            continue;
        }
        let parsed = raw
            .trim()
            .parse::<i128>()
            .context("parse proc I/O counter")?;
        let value = u64::try_from(parsed.max(0)).unwrap_or(u64::MAX);
        if key == "read_bytes" {
            read_bytes = Some(value);
        } else {
            write_bytes = Some(value);
        }
    }
    Ok((
        read_bytes.context("incomplete proc io")?,
        write_bytes.context("incomplete proc io")?,
    ))
}

fn safe_process_name(value: &str) -> String {
    let clean = value
        .trim()
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || matches!(character, '.' | '_' | ':' | '+' | '-') {
                character
            } else {
                '_'
            }
        })
        .take(64)
        .collect::<String>();
    if clean.is_empty() {
        "process".to_string()
    } else {
        clean
    }
}

fn collect_process_counters(proc_root: &Path) -> Result<ProcessCollection> {
    let mut counters = BTreeMap::new();
    let mut inaccessible = 0usize;
    let mut entries = 0usize;
    for entry in fs::read_dir(proc_root)
        .with_context(|| format!("read procfs root {}", proc_root.display()))?
    {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.is_empty() || !name.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        count_proc_entry(&mut entries)?;
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        let process_root = entry.path();
        let (parsed, process_name, uid) = match (
            read_bounded_proc(&process_root.join("stat")).and_then(|value| parse_proc_stat(&value)),
            read_bounded_proc(&process_root.join("comm")),
            process_uid(&process_root),
        ) {
            (Ok(parsed), Ok(process_name), Ok(uid)) => (parsed, process_name, uid),
            _ => continue,
        };
        let io =
            read_bounded_proc(&process_root.join("io")).and_then(|value| parse_proc_io(&value));
        if io.is_err() {
            inaccessible = inaccessible.saturating_add(1);
        }
        let (read_bytes, write_bytes) = io
            .map(|(read, write)| (Some(read), Some(write)))
            .unwrap_or((None, None));
        let identity = ProcessIdentity {
            pid,
            start_ticks: parsed.start_ticks,
        };
        counters.insert(
            identity,
            ProcessCounter {
                pid,
                name: safe_process_name(&process_name),
                uid,
                state: parsed.state,
                ppid: parsed.ppid,
                cpu_ticks: parsed.cpu_ticks,
                start_ticks: parsed.start_ticks,
                rss_pages: parsed.rss_pages,
                read_bytes,
                write_bytes,
            },
        );
    }
    Ok(ProcessCollection {
        counters,
        inaccessible,
    })
}

fn count_proc_entry(entries: &mut usize) -> Result<()> {
    *entries = entries.saturating_add(1);
    ensure!(
        *entries <= MAX_PROC_ENTRIES,
        "procfs entry count exceeds its limit"
    );
    Ok(())
}

#[cfg(unix)]
fn process_uid(path: &Path) -> Result<u32> {
    Ok(fs::symlink_metadata(path)?.uid())
}

#[cfg(not(unix))]
fn process_uid(_path: &Path) -> Result<u32> {
    bail!("process ownership requires Unix")
}

fn rounded(value: f64, digits: usize) -> f64 {
    format!("{value:.digits$}")
        .parse()
        .expect("finite decimal value")
}

fn process_rates(
    previous: &BTreeMap<ProcessIdentity, ProcessCounter>,
    current: &BTreeMap<ProcessIdentity, ProcessCounter>,
    elapsed_secs: f64,
    clock_ticks: u64,
    page_size: u64,
    inaccessible_count: usize,
    limit: usize,
) -> ProcessContext {
    let elapsed = elapsed_secs.max(0.001);
    let mut active = Vec::new();
    let mut blocked = Vec::new();
    for (identity, value) in current {
        let prior = previous.get(identity);
        let read_rate = prior.and_then(|prior| {
            Some(value.read_bytes?.saturating_sub(prior.read_bytes?) as f64 / elapsed / MIB)
        });
        let write_rate = prior.and_then(|prior| {
            Some(value.write_bytes?.saturating_sub(prior.write_bytes?) as f64 / elapsed / MIB)
        });
        let cpu_percent = prior.map(|prior| {
            value.cpu_ticks.saturating_sub(prior.cpu_ticks) as f64 * 100.0
                / clock_ticks.max(1) as f64
                / elapsed
        });
        let item = ProcessRate {
            pid: value.pid,
            start_ticks: value.start_ticks,
            name: value.name.clone(),
            uid: value.uid,
            state: value.state.clone(),
            read_mib_per_sec: read_rate.map(|rate| rounded(rate, 3)),
            write_mib_per_sec: write_rate.map(|rate| rounded(rate, 3)),
            cpu_percent: cpu_percent.map(|percent| rounded(percent, 1)),
            rss_bytes: value.rss_pages.saturating_mul(page_size),
        };
        if value.state == "D" {
            blocked.push(item.clone());
        }
        if read_rate.unwrap_or(0.0) > 0.0 || write_rate.unwrap_or(0.0) > 0.0 {
            active.push(item);
        }
    }
    let compare = |left: &ProcessRate, right: &ProcessRate| {
        let left_rate =
            left.read_mib_per_sec.unwrap_or(0.0) + left.write_mib_per_sec.unwrap_or(0.0);
        let right_rate =
            right.read_mib_per_sec.unwrap_or(0.0) + right.write_mib_per_sec.unwrap_or(0.0);
        right_rate
            .total_cmp(&left_rate)
            .then_with(|| left.pid.cmp(&right.pid))
            .then_with(|| left.start_ticks.cmp(&right.start_ticks))
    };
    active.sort_by(compare);
    blocked.sort_by(compare);
    let active_count = active.len();
    let truncated = active.len() > limit || blocked.len() > limit;
    active.truncate(limit);
    blocked.truncate(limit);
    ProcessContext {
        sample_window_secs: rounded(elapsed, 3),
        active_count,
        inaccessible_count,
        truncated,
        top_processes: active,
        blocked_processes: blocked,
    }
}

fn host_metrics(backfill: &BackfillStatus, proc_root: &Path) -> HostMetrics {
    let resources = backfill.resources.clone().unwrap_or_default();
    let memory_kib = resources.mem_available_kib.or_else(|| {
        proc_value(&proc_root.join("meminfo"), "MemAvailable:")?
            .parse()
            .ok()
    });
    let io_full_avg10 = finite_nonnegative(resources.io_full_avg10).or_else(|| {
        let pressure = read_bounded_proc(&proc_root.join("pressure/io")).ok()?;
        pressure
            .lines()
            .find(|line| line.starts_with("full "))
            .and_then(|line| {
                line.split_whitespace()
                    .find_map(|field| field.strip_prefix("avg10="))
                    .and_then(|value| value.parse::<f64>().ok())
                    .and_then(|value| finite_nonnegative(Some(value)))
            })
    });
    let load1 = finite_nonnegative(resources.load1).or_else(|| {
        read_bounded_proc(&proc_root.join("loadavg"))
            .ok()?
            .split_whitespace()
            .next()?
            .parse::<f64>()
            .ok()
            .and_then(|value| finite_nonnegative(Some(value)))
    });
    HostMetrics {
        memory_available_bytes: memory_kib.map(|value| value.saturating_mul(1024)),
        io_full_avg10,
        io_pause_threshold: finite_nonnegative(resources.io_pause_full_avg10),
        load1,
        load_pause_threshold: LOAD_PAUSE,
        finalizer_active: resources.finalizer_active,
        live_capture_age_secs: resources.live_capture_age_seconds,
        live_capture_max_stale_secs: resources.live_capture_max_stale_seconds,
        live_capture_healthy: resources.live_capture_healthy,
    }
}

fn proc_value(path: &Path, prefix: &str) -> Option<String> {
    read_bounded_proc(path).ok()?.lines().find_map(|line| {
        line.strip_prefix(prefix)
            .and_then(|value| value.split_whitespace().next())
            .map(str::to_string)
    })
}

fn finite_nonnegative(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite() && *value >= 0.0)
}

fn classify_backfill_pause(metrics: &HostMetrics) -> Vec<Reason> {
    let mut reasons = Vec::new();
    let memory_threshold = MEMORY_PAUSE_KIB.saturating_mul(1024);
    if metrics
        .memory_available_bytes
        .is_some_and(|value| value < memory_threshold)
    {
        reasons.push(Reason::u64(
            "memory_pressure",
            metrics.memory_available_bytes,
            Some(memory_threshold),
            "bytes",
        ));
    }
    if let (Some(observed), Some(threshold)) = (metrics.io_full_avg10, metrics.io_pause_threshold)
        && observed >= threshold
    {
        reasons.push(Reason::f64("io_pressure", observed, threshold, "percent"));
    }
    if let Some(observed) = metrics.load1
        && observed >= LOAD_PAUSE
    {
        reasons.push(Reason::f64("load_pressure", observed, LOAD_PAUSE, "load"));
    }
    if metrics.finalizer_active == Some(true) {
        reasons.push(Reason::code("finalizer_active"));
    }
    if metrics.live_capture_healthy == Some(false) {
        reasons.push(Reason::u64(
            "live_capture_stale",
            metrics.live_capture_age_secs,
            metrics.live_capture_max_stale_secs,
            "seconds",
        ));
    }
    if reasons.is_empty() {
        reasons.push(Reason::code("transient_resource_guard"));
    }
    reasons
}

fn reason_from_control_target(target: &str) -> Vec<Reason> {
    static IO_REASON: OnceLock<Regex> = OnceLock::new();
    static LOAD_REASON: OnceLock<Regex> = OnceLock::new();
    let io = IO_REASON.get_or_init(|| {
        Regex::new(r"IO PSI full avg10 ([0-9]+(?:\.[0-9]+)?) reached pause threshold ([0-9]+(?:\.[0-9]+)?)")
            .expect("valid I/O reason regex")
    });
    if let Some(captures) = io.captures(target)
        && let (Ok(observed), Ok(threshold)) =
            (captures[1].parse::<f64>(), captures[2].parse::<f64>())
        && observed.is_finite()
        && threshold.is_finite()
    {
        return vec![Reason::f64("io_pressure", observed, threshold, "percent")];
    }
    let load = LOAD_REASON.get_or_init(|| {
        Regex::new(
            r"load average ([0-9]+(?:\.[0-9]+)?) reached CPU load ceiling ([0-9]+(?:\.[0-9]+)?)",
        )
        .expect("valid load reason regex")
    });
    if let Some(captures) = load.captures(target)
        && let (Ok(observed), Ok(threshold)) =
            (captures[1].parse::<f64>(), captures[2].parse::<f64>())
        && observed.is_finite()
        && threshold.is_finite()
    {
        return vec![Reason::f64("load_pressure", observed, threshold, "load")];
    }
    vec![Reason::code("resource_guard")]
}

fn is_job(value: &str) -> bool {
    static JOB: OnceLock<Regex> = OnceLock::new();
    value.chars().count() <= MAX_SUBJECT_CHARS
        && JOB
            .get_or_init(|| Regex::new(r"\A[a-z0-9_]+:[0-9]+\z").expect("valid job regex"))
            .is_match(value)
}

fn active_lease_jobs(lease: Option<&PriorityLease>) -> BTreeSet<String> {
    if lease.and_then(|lease| lease.state.as_deref()) != Some("active") {
        return BTreeSet::new();
    }
    lease
        .and_then(|lease| lease.jobs.as_ref())
        .into_iter()
        .flatten()
        .filter(|job| is_job(job))
        .take(MAX_PRIORITY_JOBS)
        .cloned()
        .collect()
}

fn control_event(
    value: &RawControlEvent,
    lease: Option<&PriorityLease>,
    lease_jobs: &BTreeSet<String>,
    metrics: &HostMetrics,
    context: &ProcessContext,
    prelude: &[SampleSummary],
) -> Option<Incident> {
    if value.action.chars().count() > 32 || value.target.chars().count() > MAX_CONTROL_TARGET_CHARS
    {
        return None;
    }
    let lease_active = lease.and_then(|lease| lease.state.as_deref()) == Some("active");
    if matches!(value.action.as_str(), "pause" | "resume")
        && (value.target == "scheduler" || is_job(&value.target))
    {
        let paused = value.action == "pause";
        let priority =
            lease_active && (value.target == "scheduler" || lease_jobs.contains(&value.target));
        return Some(make_event(
            value.at_unix_secs,
            "archive_scheduler",
            if paused { "paused" } else { "resumed" },
            &value.target,
            None,
            if priority {
                "priority_lease"
            } else {
                "operator"
            },
            vec![Reason::code(if priority {
                "priority_lease"
            } else {
                "operator_request"
            })],
            metrics.clone(),
            paused.then(|| context.clone()),
            if paused { prelude.to_vec() } else { Vec::new() },
        ));
    }
    if value.action != "legacy_adaptive" {
        return None;
    }
    static AUTO_PAUSE: OnceLock<Regex> = OnceLock::new();
    static AUTO_RESUME: OnceLock<Regex> = OnceLock::new();
    let pause = AUTO_PAUSE.get_or_init(|| {
        Regex::new(r"\Aauto-paused ([a-z0-9_]+:[0-9]+): (.+)\z").expect("valid auto-pause regex")
    });
    if let Some(captures) = pause.captures(&value.target) {
        let job = captures.get(1)?.as_str();
        if !is_job(job) {
            return None;
        }
        return Some(make_event(
            value.at_unix_secs,
            "archive_scheduler",
            "paused",
            job,
            None,
            "automatic",
            reason_from_control_target(captures.get(2)?.as_str()),
            metrics.clone(),
            Some(context.clone()),
            prelude.to_vec(),
        ));
    }
    let resume = AUTO_RESUME.get_or_init(|| {
        Regex::new(r"\Aauto-resumed ([a-z0-9_]+:[0-9]+): .+\z").expect("valid auto-resume regex")
    });
    let captures = resume.captures(&value.target)?;
    let job = captures.get(1)?.as_str();
    is_job(job).then(|| {
        make_event(
            value.at_unix_secs,
            "archive_scheduler",
            "resumed",
            job,
            None,
            "automatic",
            vec![Reason::code("resources_recovered")],
            metrics.clone(),
            None,
            Vec::new(),
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn make_event(
    at_unix_secs: u64,
    component: &'static str,
    kind: &'static str,
    subject: &str,
    epoch: Option<u64>,
    actor: &'static str,
    reasons: Vec<Reason>,
    metrics: HostMetrics,
    process_context: Option<ProcessContext>,
    prelude: Vec<SampleSummary>,
) -> Incident {
    Incident {
        schema_version: 1,
        id: event_id(at_unix_secs, component, kind, subject),
        at_unix_secs,
        component,
        kind,
        subject: subject.to_string(),
        epoch,
        actor,
        reasons,
        metrics,
        process_context,
        prelude,
    }
}

fn event_id(at_unix_secs: u64, component: &str, kind: &str, subject: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(at_unix_secs.to_string());
    digest.update([0]);
    digest.update(component.as_bytes());
    digest.update([0]);
    digest.update(kind.as_bytes());
    digest.update([0]);
    digest.update(subject.as_bytes());
    digest
        .finalize()
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn regular_file_identity(path: &Path) -> Result<Option<(u64, u64)>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("open {}", path.display())),
    };
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        path.display()
    );
    #[cfg(unix)]
    return Ok(Some((metadata.ino(), metadata.len())));
    #[cfg(not(unix))]
    Ok(Some((0, metadata.len())))
}

fn read_new_control_jsonl(
    path: &Path,
    previous_inode: u64,
    previous_offset: u64,
    previous_discarding: bool,
) -> Result<ControlBatch> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ControlBatch {
                values: Vec::new(),
                inode: previous_inode,
                offset: previous_offset,
                discarding_line: previous_discarding,
            });
        }
        Err(error) => return Err(error).with_context(|| format!("open {}", path.display())),
    };
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file(),
        "control event input is not a regular file"
    );
    #[cfg(unix)]
    let inode = metadata.ino();
    #[cfg(not(unix))]
    let inode = 0;
    let reset = previous_inode != inode || previous_offset > metadata.len();
    let start_offset = if reset { 0 } else { previous_offset };
    let mut discarding = if reset { false } else { previous_discarding };
    file.seek(SeekFrom::Start(start_offset))?;

    let mut values = Vec::new();
    let mut line = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut consumed = 0usize;
    let mut committed = start_offset;
    let mut position = start_offset;
    let mut complete_lines = 0usize;
    'outer: while consumed < MAX_CONTROL_BATCH_BYTES
        && complete_lines < MAX_CONTROL_LINES_PER_SAMPLE
    {
        let request = buffer.len().min(MAX_CONTROL_BATCH_BYTES - consumed);
        let read = file.read(&mut buffer[..request])?;
        if read == 0 {
            if discarding {
                // Permanently consume an overlong unterminated line so it
                // cannot make every subsequent sample rescan the same bytes.
                committed = position;
            }
            break;
        }
        for byte in &buffer[..read] {
            consumed += 1;
            position = position.saturating_add(1);
            if discarding {
                if *byte == b'\n' {
                    discarding = false;
                    committed = position;
                    complete_lines += 1;
                }
            } else if *byte == b'\n' {
                if let Ok(value) = serde_json::from_slice::<RawControlEvent>(&line) {
                    values.push(value);
                }
                line.clear();
                committed = position;
                complete_lines += 1;
            } else if line.len() < MAX_EVENT_BYTES {
                line.push(*byte);
            } else {
                line.clear();
                discarding = true;
            }
            if consumed == MAX_CONTROL_BATCH_BYTES || complete_lines == MAX_CONTROL_LINES_PER_SAMPLE
            {
                break 'outer;
            }
        }
    }
    if discarding {
        // A line may span multiple bounded batches. Persist progress through
        // discarded bytes so the next sample continues forward instead of
        // rescanning the same batch forever.
        committed = position;
    }
    Ok(ControlBatch {
        values,
        inode,
        offset: committed,
        discarding_line: discarding,
    })
}

fn parent_or_dot(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

struct RecorderLock {
    #[allow(dead_code)]
    file: File,
}

impl RecorderLock {
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
            .with_context(|| format!("open scheduler incident lock {}", path.display()))?;
        ensure!(
            file.metadata()?.is_file(),
            "recorder lock is not a regular file"
        );
        // SAFETY: the descriptor remains owned by this guard.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK)
            {
                bail!("scheduler incident recorder is already running");
            }
            return Err(error).context("lock scheduler incident recorder");
        }
        Ok(Self { file })
    }

    #[cfg(not(unix))]
    fn acquire(_path: &Path) -> Result<Self> {
        bail!("scheduler incident locking requires Unix")
    }
}

fn encoded_json_line(value: &impl Serialize, label: &str) -> Result<Vec<u8>> {
    let mut encoded = serde_json::to_vec(value).with_context(|| format!("encode {label}"))?;
    encoded.push(b'\n');
    ensure!(
        encoded.len() <= MAX_EVENT_BYTES,
        "{label} exceeds its size limit"
    );
    Ok(encoded)
}

fn atomic_json(path: &Path, value: &impl Serialize, mode: u32) -> Result<()> {
    let encoded = encoded_json_line(value, "scheduler incident state")?;
    let parent = parent_or_dot(path);
    fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("scheduler incident state needs a UTF-8 file name")?;
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
            .mode(mode)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let mut file = options.open(&temporary)?;
        file.write_all(&encoded)?;
        file.flush()?;
        file.sync_all()?;
        #[cfg(unix)]
        fs::set_permissions(&temporary, fs::Permissions::from_mode(mode))?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn append_jsonl(path: &Path, value: &Incident) -> Result<()> {
    let encoded = encoded_json_line(value, "scheduler incident")?;
    let parent = parent_or_dot(path);
    fs::create_dir_all(parent)?;
    if let Ok(metadata) = fs::symlink_metadata(path) {
        ensure!(
            metadata.file_type().is_file(),
            "incident log is not a regular file"
        );
        if metadata.len().saturating_add(encoded.len() as u64) > MAX_PRIVATE_LOG_BYTES {
            let rotated = path.with_file_name(format!(
                "{}.1",
                path.file_name()
                    .and_then(|name| name.to_str())
                    .context("incident log needs a UTF-8 file name")?
            ));
            match fs::remove_file(&rotated) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error).context("remove prior rotated incident log"),
            }
            fs::rename(path, &rotated)?;
            #[cfg(unix)]
            fs::set_permissions(&rotated, fs::Permissions::from_mode(0o600))?;
        }
    }
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let mut file = options.open(path)?;
    ensure!(
        file.metadata()?.is_file(),
        "incident log is not a regular file"
    );
    file.write_all(&encoded)?;
    file.flush()?;
    file.sync_all()?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};

    fn root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "blockzilla-scheduler-incidents-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn metrics() -> HostMetrics {
        HostMetrics {
            memory_available_bytes: Some(8 * 1024 * 1024),
            io_full_avg10: Some(1.0),
            io_pause_threshold: Some(20.0),
            load1: Some(2.0),
            load_pause_threshold: LOAD_PAUSE,
            finalizer_active: Some(false),
            live_capture_age_secs: Some(1),
            live_capture_max_stale_secs: Some(30),
            live_capture_healthy: Some(true),
        }
    }

    fn context() -> ProcessContext {
        ProcessContext {
            sample_window_secs: 5.0,
            active_count: 0,
            inaccessible_count: 0,
            truncated: false,
            top_processes: Vec::new(),
            blocked_processes: Vec::new(),
        }
    }

    fn counter(
        pid: u32,
        start: u64,
        state: &str,
        read: u64,
        write: u64,
    ) -> (ProcessIdentity, ProcessCounter) {
        (
            ProcessIdentity {
                pid,
                start_ticks: start,
            },
            ProcessCounter {
                pid,
                name: "worker".to_string(),
                uid: 501,
                state: state.to_string(),
                ppid: 1,
                cpu_ticks: 100,
                start_ticks: start,
                rss_pages: 2,
                read_bytes: Some(read),
                write_bytes: Some(write),
            },
        )
    }

    #[test]
    fn proc_stat_parser_handles_spaces_and_closing_parentheses() {
        let mut fields = vec!["0"; 22];
        fields[0] = "D";
        fields[1] = "7";
        fields[11] = "11";
        fields[12] = "13";
        fields[19] = "101";
        fields[21] = "9";
        let parsed = parse_proc_stat(&format!("42 (a worker) name) {}", fields.join(" "))).unwrap();
        assert_eq!(
            parsed,
            ProcStat {
                state: "D".to_string(),
                ppid: 7,
                cpu_ticks: 24,
                start_ticks: 101,
                rss_pages: 9,
            }
        );
    }

    #[test]
    fn proc_io_requires_both_counters_and_clamps_negative_values() {
        assert_eq!(
            parse_proc_io("read_bytes: -1\nwrite_bytes: 7\n").unwrap(),
            (0, 7)
        );
        assert!(parse_proc_io("read_bytes: 1\n").is_err());
    }

    #[test]
    fn process_names_are_bounded_and_cannot_carry_paths_or_controls() {
        assert_eq!(
            safe_process_name("  curl /tmp/private\n"),
            "curl__tmp_private"
        );
        assert_eq!(safe_process_name("\n"), "process");
        assert_eq!(safe_process_name(&"x".repeat(100)).chars().count(), 64);
    }

    #[test]
    fn process_rates_key_history_by_pid_and_start_time_and_bound_lists() {
        let (identity, old) = counter(7, 100, "S", 0, 0);
        let (_, current) = counter(7, 100, "D", 2 * 1024 * 1024, 1024 * 1024);
        let previous = BTreeMap::from([(identity.clone(), old)]);
        let current = BTreeMap::from([(identity, current)]);
        let result = process_rates(&previous, &current, 2.0, 100, 4096, 3, 1);
        assert_eq!(result.active_count, 1);
        assert_eq!(result.top_processes[0].read_mib_per_sec, Some(1.0));
        assert_eq!(result.top_processes[0].write_mib_per_sec, Some(0.5));
        assert_eq!(result.blocked_processes.len(), 1);
        assert_eq!(result.top_processes[0].rss_bytes, 8192);

        let (_, reused) = counter(7, 101, "S", 9 * 1024 * 1024, 0);
        let reused = BTreeMap::from([(
            ProcessIdentity {
                pid: 7,
                start_ticks: 101,
            },
            reused,
        )]);
        assert!(
            process_rates(&previous, &reused, 1.0, 100, 4096, 0, 1)
                .top_processes
                .is_empty()
        );
    }

    #[test]
    fn pause_classification_preserves_all_resource_guards() {
        let metrics = HostMetrics {
            memory_available_bytes: Some(1),
            io_full_avg10: Some(20.0),
            io_pause_threshold: Some(20.0),
            load1: Some(10.0),
            load_pause_threshold: LOAD_PAUSE,
            finalizer_active: Some(true),
            live_capture_age_secs: Some(31),
            live_capture_max_stale_secs: Some(30),
            live_capture_healthy: Some(false),
        };
        let reasons = classify_backfill_pause(&metrics);
        assert_eq!(
            reasons.iter().map(Reason::label).collect::<Vec<_>>(),
            [
                "memory_pressure",
                "io_pressure",
                "load_pressure",
                "finalizer_active",
                "live_capture_stale"
            ]
        );
    }

    #[test]
    fn control_events_preserve_operator_priority_and_automatic_semantics() {
        let lease = PriorityLease {
            state: Some("active".to_string()),
            jobs: Some(vec!["compact_reuse:761".to_string()]),
        };
        let jobs = active_lease_jobs(Some(&lease));
        let paused = control_event(
            &RawControlEvent {
                at_unix_secs: 100,
                action: "pause".to_string(),
                target: "compact_reuse:761".to_string(),
            },
            Some(&lease),
            &jobs,
            &metrics(),
            &context(),
            &[],
        )
        .unwrap();
        assert_eq!(paused.kind, "paused");
        assert_eq!(paused.actor, "priority_lease");
        assert!(paused.process_context.is_some());

        let resumed = control_event(
            &RawControlEvent {
                at_unix_secs: 101,
                action: "legacy_adaptive".to_string(),
                target: "auto-resumed compact_reuse:761: resources recovered".to_string(),
            },
            None,
            &BTreeSet::new(),
            &metrics(),
            &context(),
            &[],
        )
        .unwrap();
        assert_eq!(resumed.kind, "resumed");
        assert_eq!(resumed.reasons[0].label(), "resources_recovered");
        assert!(resumed.process_context.is_none());
    }

    #[test]
    fn automatic_reason_parsing_never_copies_free_form_diagnostics() {
        let incident = control_event(
            &RawControlEvent {
                at_unix_secs: 100,
                action: "legacy_adaptive".to_string(),
                target: "auto-paused compact_reuse:761: token=SECRET /volume1/private".to_string(),
            },
            None,
            &BTreeSet::new(),
            &metrics(),
            &context(),
            &[],
        )
        .unwrap();
        let encoded = serde_json::to_string(&incident).unwrap();
        assert_eq!(incident.reasons[0].label(), "resource_guard");
        assert!(!encoded.contains("SECRET"));
        assert!(!encoded.contains("/volume1"));
    }

    #[test]
    fn event_ids_match_the_python_contract_and_are_stable() {
        assert_eq!(
            event_id(100, "archive_scheduler", "paused", "scheduler"),
            "d02333bb6b273d550898b73a"
        );
        assert_ne!(
            event_id(100, "archive_scheduler", "paused", "scheduler"),
            event_id(100, "archive_scheduler", "resumed", "scheduler")
        );
    }

    #[test]
    fn control_reader_is_incremental_rotation_aware_and_keeps_partial_lines() {
        let root = root("control");
        let path = root.join("events.jsonl");
        fs::write(
            &path,
            b"{\"at_unix_secs\":1,\"action\":\"pause\",\"target\":\"scheduler\"}\npartial",
        )
        .unwrap();
        let first = read_new_control_jsonl(&path, 0, 0, false).unwrap();
        assert_eq!(first.values.len(), 1);
        assert!(first.offset < fs::metadata(&path).unwrap().len());
        fs::rename(&path, root.join("events.jsonl.1")).unwrap();
        fs::write(
            &path,
            b"{\"at_unix_secs\":2,\"action\":\"resume\",\"target\":\"scheduler\"}\n",
        )
        .unwrap();
        let second = read_new_control_jsonl(&path, first.inode, first.offset, false).unwrap();
        assert_eq!(second.values[0].at_unix_secs, 2);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn overlong_control_lines_are_discarded_without_unbounded_allocation_or_livelock() {
        let root = root("control-bound");
        let path = root.join("events.jsonl");
        let mut bytes = vec![b'x'; MAX_EVENT_BYTES + 10];
        bytes.extend_from_slice(
            b"\n{\"at_unix_secs\":2,\"action\":\"pause\",\"target\":\"scheduler\"}\n",
        );
        fs::write(&path, bytes).unwrap();
        let batch = read_new_control_jsonl(&path, 0, 0, false).unwrap();
        assert_eq!(batch.values.len(), 1);
        assert_eq!(batch.values[0].at_unix_secs, 2);
        assert_eq!(batch.offset, fs::metadata(&path).unwrap().len());
        assert!(!batch.discarding_line);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn overlong_control_line_advances_across_multiple_bounded_batches() {
        let root = root("control-multi-batch-bound");
        let path = root.join("events.jsonl");
        let mut bytes = vec![b'x'; MAX_CONTROL_BATCH_BYTES + MAX_EVENT_BYTES + 10];
        bytes.extend_from_slice(
            b"\n{\"at_unix_secs\":2,\"action\":\"pause\",\"target\":\"scheduler\"}\n",
        );
        fs::write(&path, bytes).unwrap();

        let first = read_new_control_jsonl(&path, 0, 0, false).unwrap();
        assert!(first.values.is_empty());
        assert!(first.discarding_line);
        assert_eq!(first.offset, MAX_CONTROL_BATCH_BYTES as u64);
        let second =
            read_new_control_jsonl(&path, first.inode, first.offset, first.discarding_line)
                .unwrap();
        assert_eq!(second.values.len(), 1);
        assert_eq!(second.values[0].at_unix_secs, 2);
        assert!(!second.discarding_line);
        assert_eq!(second.offset, fs::metadata(&path).unwrap().len());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn private_outputs_are_atomic_mode_0600_and_reject_symlinks() {
        let root = root("private-output");
        let state = root.join("state.json");
        atomic_json(&state, &Checkpoint::default(), 0o600).unwrap();
        assert_eq!(
            fs::metadata(&state).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let log = root.join("events.jsonl");
        let incident = make_event(
            1,
            "archive_scheduler",
            "paused",
            "scheduler",
            None,
            "operator",
            vec![Reason::code("operator_request")],
            metrics(),
            Some(context()),
            Vec::new(),
        );
        append_jsonl(&log, &incident).unwrap();
        assert_eq!(
            fs::metadata(&log).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::remove_file(&log).unwrap();
        let target = root.join("target");
        fs::write(&target, b"untouched").unwrap();
        symlink(&target, &log).unwrap();
        assert!(append_jsonl(&log, &incident).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"untouched");
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn recorder_lock_is_exclusive_and_nofollow() {
        let root = root("lock");
        let path = root.join("lock");
        let held = RecorderLock::acquire(&path).unwrap();
        assert!(RecorderLock::acquire(&path).is_err());
        drop(held);
        fs::remove_file(&path).unwrap();
        let target = root.join("target");
        fs::write(&target, b"safe").unwrap();
        symlink(&target, &path).unwrap();
        assert!(RecorderLock::acquire(&path).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"safe");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn host_metrics_use_status_first_and_bounded_proc_fallbacks() {
        let root = root("host");
        fs::write(root.join("meminfo"), "MemAvailable: 100 kB\n").unwrap();
        fs::write(root.join("loadavg"), "3.5 0 0 1/1 1\n").unwrap();
        fs::create_dir(root.join("pressure")).unwrap();
        fs::write(
            root.join("pressure/io"),
            "some avg10=0\nfull avg10=2.5 avg60=0\n",
        )
        .unwrap();
        let metrics = host_metrics(&BackfillStatus::default(), &root);
        assert_eq!(metrics.memory_available_bytes, Some(102_400));
        assert_eq!(metrics.io_full_avg10, Some(2.5));
        assert_eq!(metrics.load1, Some(3.5));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn collection_has_a_hard_proc_entry_limit() {
        let mut entries = MAX_PROC_ENTRIES;
        assert!(count_proc_entry(&mut entries).is_err());
        assert_eq!(entries, MAX_PROC_ENTRIES + 1);
    }

    #[test]
    fn malformed_or_oversized_json_is_ignored_by_optional_inputs() {
        let root = root("json-bounds");
        let path = root.join("status.json");
        fs::write(&path, b"not json").unwrap();
        assert!(read_optional_json::<BackfillStatus>(&path, 32).is_none());
        fs::write(&path, vec![b'x'; 33]).unwrap();
        assert!(read_optional_json::<BackfillStatus>(&path, 32).is_none());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sampling_configuration_is_bounded() {
        let base = RecordArgs {
            backfill_status: "/private/backfill".into(),
            control_events: "/private/control".into(),
            priority_lease: None,
            state_file: "/private/state".into(),
            events_output: "/private/events".into(),
            interval_secs: 5.0,
            ring_secs: 30.0,
            process_limit: 20,
            proc_root: "/proc".into(),
            once: true,
        };
        assert!(validate_args(&base).is_ok());
        let mut invalid = base.clone();
        invalid.ring_secs = 4.0;
        assert!(validate_args(&invalid).is_err());
        invalid = base.clone();
        invalid.process_limit = MAX_PROCESS_LIMIT + 1;
        assert!(validate_args(&invalid).is_err());
        invalid = base.clone();
        invalid.interval_secs = 0.001;
        invalid.ring_secs = 10_000.0;
        assert!(validate_args(&invalid).is_err());

        invalid = base.clone();
        invalid.interval_secs = f64::MAX;
        invalid.ring_secs = f64::MAX;
        assert!(validate_args(&invalid).is_err());

        invalid = base.clone();
        invalid.events_output = invalid.control_events.clone();
        assert!(validate_args(&invalid).is_err());
    }

    #[test]
    fn reason_json_is_typed_and_contains_no_private_free_form_field() {
        let reason = reason_from_control_target(
            "IO PSI full avg10 22.5 reached pause threshold 20.0 token=SECRET",
        );
        assert_eq!(reason[0].label(), "io_pressure");
        let value = serde_json::to_value(&reason).unwrap();
        assert_eq!(value[0]["observed"], json!(22.5));
        assert!(!value.to_string().contains("SECRET"));
    }
}
