//! Bounded, secret-free telemetry for processes owned by the monitor's UID.
//!
//! Process command lines are used only for local classification and are never
//! copied to the published document.

use anyhow::{Context, Result, bail};
use clap::Args;
use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::{
    fd::AsRawFd,
    unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
};

const MIB: f64 = 1024.0 * 1024.0;
const SLOTS_PER_EPOCH: u64 = 432_000;
const MAX_PROCESSES: usize = 20;
// The Python publisher bounded process rows but could still emit one job per
// matching process. Bound that second collection too so a same-UID process
// storm cannot create an arbitrarily large public document.
const MAX_JOBS: usize = 64;
const MAX_JOURNAL_TAIL_BYTES: u64 = 64 * 1024;
const MAX_CMDLINE_BYTES: u64 = 64 * 1024;
const MAX_CMDLINE_ARGS: usize = 256;
const MAX_PROCESS_NAME_CHARS: usize = 64;
const MAX_PROCESS_ANCESTORS: usize = 32;

#[derive(Debug, Clone, Args)]
pub struct PublishArgs {
    /// Atomically replaced public runtime-operations JSON document.
    #[arg(long)]
    output: PathBuf,

    /// Procfs root. The override exists for isolated fixtures and tests.
    #[arg(long, default_value = "/proc")]
    proc_root: PathBuf,

    /// Sampling interval. Counter rates need two samples before becoming available.
    #[arg(long, default_value_t = 5.0, value_parser = positive_interval)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessSample {
    pid: u32,
    ppid: u32,
    start_ticks: u64,
    start_unix_secs: u64,
    name: String,
    args: Vec<String>,
    rchar: u64,
    read_bytes: u64,
    write_bytes: u64,
    cpu_ticks: u64,
    rss_bytes: u64,
}

#[derive(Debug)]
pub struct ProcessCollection {
    pub samples: BTreeMap<u32, ProcessSample>,
    pub inaccessible: usize,
    pub clock_ticks: u64,
}

#[derive(Debug, Serialize)]
struct RuntimeOperationsStatus {
    schema_version: u32,
    updated_unix_secs: u64,
    live_capture: Option<RuntimeLiveCapture>,
    jobs: Vec<RuntimeJob>,
    process_io: ProcessIoStatus,
}

#[derive(Debug, Serialize)]
struct RuntimeLiveCapture {
    state: &'static str,
    mode: &'static str,
    source: &'static str,
    pid: u32,
    epoch: u64,
    last_slot: u64,
    blocks_written: u64,
    bytes_written: u64,
    write_mib_per_sec: Option<f64>,
    started_unix_secs: u64,
    updated_unix_secs: u64,
}

#[derive(Debug, Serialize)]
struct RuntimeJob {
    id: String,
    kind: &'static str,
    epoch: u64,
    phase: &'static str,
    state: &'static str,
    pid: u32,
    bytes_done: u64,
    bytes_total: Option<u64>,
    progress_pct: Option<f64>,
    read_mib_per_sec: Option<f64>,
    write_mib_per_sec: Option<f64>,
    eta_secs: Option<u64>,
    rss_bytes: Option<u64>,
    started_unix_secs: u64,
    updated_unix_secs: u64,
}

#[derive(Debug, Serialize)]
pub struct ProcessIoStatus {
    pub state: &'static str,
    pub sampled_unix_secs: u64,
    pub sample_window_secs: Option<f64>,
    pub active_count: usize,
    pub inaccessible_count: usize,
    pub truncated: bool,
    pub processes: Vec<ProcessIoEntry>,
    pub message: Option<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct ProcessIoEntry {
    pub id: String,
    pub pid: u32,
    pub name: String,
    pub read_mib_per_sec: f64,
    pub write_mib_per_sec: f64,
    pub cpu_percent: Option<f64>,
    pub rss_bytes: u64,
    pub blockzilla_owned: bool,
}

pub async fn publish(args: PublishArgs) -> Result<()> {
    if !args.interval_secs.is_finite() || args.interval_secs <= 0.0 {
        bail!("--interval-secs must be positive and finite");
    }
    let parent = args
        .output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create runtime operations directory {}", parent.display()))?;
    let lock_path = parent.join(".runtime-operations-publisher.lock");
    let _publisher_lock = PublisherLock::acquire(&lock_path)?;

    let mut previous = BTreeMap::new();
    let mut previous_at = None;
    let interval = Duration::from_secs_f64(args.interval_secs);

    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install runtime operations SIGTERM handler")?;

    loop {
        let now = unix_now()?;
        let collection = collect_processes(&args.proc_root)?;
        // Match the Python oracle's integer wall-clock sample window. This
        // deliberately yields no rate if two samples land in the same second.
        let elapsed = previous_at
            .map(|sampled_at: u64| now.saturating_sub(sampled_at) as f64)
            .unwrap_or(0.0);
        let status = build_status(
            &args.proc_root,
            &collection.samples,
            &previous,
            elapsed,
            collection.inaccessible,
            collection.clock_ticks,
            now,
        );
        publish_atomic(&args.output, &status)?;
        previous = collection.samples;
        previous_at = Some(now);

        if args.once {
            break;
        }

        #[cfg(unix)]
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            result = tokio::signal::ctrl_c() => {
                result.context("wait for runtime operations SIGINT")?;
                break;
            }
            _ = terminate.recv() => break,
        }
        #[cfg(not(unix))]
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            result = tokio::signal::ctrl_c() => {
                result.context("wait for runtime operations interrupt")?;
                break;
            }
        }
    }
    Ok(())
}

fn unix_now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs())
}

#[cfg(unix)]
fn system_parameters() -> Result<(u32, u64, u64)> {
    // SAFETY: these libc calls have no pointer arguments and no additional
    // preconditions. Negative sysconf results are rejected below.
    let uid = unsafe { libc::getuid() };
    let clock_ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if clock_ticks <= 0 || page_size <= 0 {
        bail!("could not resolve procfs clock tick or page size");
    }
    Ok((uid, clock_ticks as u64, page_size as u64))
}

#[cfg(not(unix))]
fn system_parameters() -> Result<(u32, u64, u64)> {
    bail!("runtime operations collection requires a Unix procfs host")
}

pub fn collect_processes(proc_root: &Path) -> Result<ProcessCollection> {
    let (uid, clock_ticks, page_size) = system_parameters()?;
    collect_processes_for_uid(proc_root, uid, clock_ticks, page_size)
}

fn collect_processes_for_uid(
    proc_root: &Path,
    uid: u32,
    clock_ticks: u64,
    page_size: u64,
) -> Result<ProcessCollection> {
    let boot_unix_secs = boot_time(proc_root)?;
    let mut samples = BTreeMap::new();
    let mut inaccessible = 0usize;
    for entry in fs::read_dir(proc_root)
        .with_context(|| format!("read procfs root {}", proc_root.display()))?
    {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.is_empty() || !name.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        #[cfg(unix)]
        match entry.metadata() {
            Ok(metadata) if metadata.uid() == uid => {}
            Ok(_) | Err(_) => continue,
        }
        #[cfg(not(unix))]
        continue;

        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        match process_sample(proc_root, pid, boot_unix_secs, clock_ticks, page_size) {
            Ok(sample) => {
                samples.insert(pid, sample);
            }
            Err(_) => inaccessible = inaccessible.saturating_add(1),
        }
    }
    Ok(ProcessCollection {
        samples,
        inaccessible,
        clock_ticks,
    })
}

fn boot_time(proc_root: &Path) -> Result<u64> {
    let contents = fs::read_to_string(proc_root.join("stat"))
        .with_context(|| format!("read {}", proc_root.join("stat").display()))?;
    contents
        .lines()
        .find_map(|line| line.strip_prefix("btime "))
        .and_then(|value| value.split_whitespace().next())
        .context("procfs stat has no boot time")?
        .parse::<u64>()
        .context("parse procfs boot time")
}

fn process_sample(
    proc_root: &Path,
    pid: u32,
    boot_unix_secs: u64,
    clock_ticks: u64,
    page_size: u64,
) -> Result<ProcessSample> {
    let root = proc_root.join(pid.to_string());
    let stat = parse_proc_stat(&fs::read_to_string(root.join("stat"))?)?;
    let counters = parse_proc_io(&fs::read_to_string(root.join("io"))?)?;
    let statm = fs::read_to_string(root.join("statm"))?;
    let resident_pages = statm
        .split_whitespace()
        .nth(1)
        .context("procfs statm has no resident-page count")?
        .parse::<u64>()
        .context("parse procfs resident-page count")?;
    let name = sanitize_name(&fs::read_to_string(root.join("comm"))?);
    let args = process_args(&root.join("cmdline"))?;
    Ok(ProcessSample {
        pid,
        ppid: stat.ppid,
        start_ticks: stat.start_ticks,
        start_unix_secs: boot_unix_secs.saturating_add(stat.start_ticks / clock_ticks),
        name,
        args,
        rchar: counters.get("rchar").copied().unwrap_or(0),
        read_bytes: counters.get("read_bytes").copied().unwrap_or(0),
        write_bytes: counters.get("write_bytes").copied().unwrap_or(0),
        cpu_ticks: stat.cpu_ticks,
        rss_bytes: resident_pages.saturating_mul(page_size),
    })
}

#[derive(Debug, PartialEq, Eq)]
struct ProcStat {
    ppid: u32,
    cpu_ticks: u64,
    start_ticks: u64,
}

fn parse_proc_stat(value: &str) -> Result<ProcStat> {
    let closing = value
        .rfind(')')
        .context("procfs stat has no closing command delimiter")?;
    let suffix = value
        .get(closing + 1..)
        .context("invalid procfs stat suffix")?
        .trim_start();
    let fields = suffix.split_whitespace().collect::<Vec<_>>();
    if fields.len() < 20 {
        bail!("procfs stat has fewer than 20 fields after comm");
    }
    let ppid = fields[1].parse::<u32>().context("parse procfs ppid")?;
    let user_ticks = fields[11]
        .parse::<u64>()
        .context("parse procfs user ticks")?;
    let system_ticks = fields[12]
        .parse::<u64>()
        .context("parse procfs system ticks")?;
    let start_ticks = fields[19]
        .parse::<u64>()
        .context("parse procfs start ticks")?;
    Ok(ProcStat {
        ppid,
        cpu_ticks: user_ticks.saturating_add(system_ticks),
        start_ticks,
    })
}

fn parse_proc_io(value: &str) -> Result<BTreeMap<String, u64>> {
    let mut counters = BTreeMap::new();
    for line in value.lines() {
        let Some((key, raw)) = line.split_once(':') else {
            continue;
        };
        let parsed = raw
            .trim()
            .parse::<i128>()
            .with_context(|| format!("parse procfs I/O counter {key}"))?;
        let value = if parsed <= 0 {
            0
        } else {
            u64::try_from(parsed).context("procfs I/O counter exceeds u64")?
        };
        counters.insert(key.to_string(), value);
    }
    Ok(counters)
}

fn process_args(path: &Path) -> Result<Vec<String>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut raw = Vec::with_capacity((MAX_CMDLINE_BYTES + 1) as usize);
    file.take(MAX_CMDLINE_BYTES + 1)
        .read_to_end(&mut raw)
        .with_context(|| format!("read bounded process command line {}", path.display()))?;
    if raw.len() > MAX_CMDLINE_BYTES as usize {
        bail!("process command line exceeds {} bytes", MAX_CMDLINE_BYTES);
    }

    let mut args = Vec::new();
    for item in raw.split(|byte| *byte == 0).filter(|item| !item.is_empty()) {
        if args.len() == MAX_CMDLINE_ARGS {
            bail!("process command line exceeds {MAX_CMDLINE_ARGS} arguments");
        }
        args.push(String::from_utf8_lossy(item).into_owned());
    }
    Ok(args)
}

fn sanitize_name(value: &str) -> String {
    let cleaned = value
        .chars()
        .filter(|character| !character.is_control())
        .collect::<String>();
    let cleaned = cleaned.trim();
    if cleaned.is_empty() {
        "unknown".to_string()
    } else {
        cleaned.chars().take(MAX_PROCESS_NAME_CHARS).collect()
    }
}

fn command_has(sample: &ProcessSample, executable: &str, subcommand: Option<&str>) -> bool {
    if process_executable_name(sample) != Some(executable) {
        return false;
    }
    subcommand.is_none_or(|subcommand| sample.args[1..].iter().any(|item| item == subcommand))
}

fn process_executable_name(sample: &ProcessSample) -> Option<&str> {
    Path::new(sample.args.first()?)
        .file_name()
        .and_then(|name| name.to_str())
}

fn is_exact_blockzilla_acquire_car(sample: &ProcessSample) -> bool {
    process_executable_name(sample) == Some("blockzilla")
        && sample.args.get(1).map(String::as_str) == Some("acquire-car")
}

fn is_exact_scheduler(sample: &ProcessSample) -> bool {
    matches!(
        (
            process_executable_name(sample),
            sample.args.get(1).map(String::as_str)
        ),
        (Some("blockzilla"), Some("scheduler")) | (Some("hivezilla"), Some("pipeline"))
    )
}

fn acquire_has_scheduler_ancestor(
    samples: &BTreeMap<u32, ProcessSample>,
    acquire: &ProcessSample,
) -> bool {
    let mut ancestor_pid = acquire.ppid;
    for _ in 0..MAX_PROCESS_ANCESTORS {
        let Some(ancestor) = samples.get(&ancestor_pid) else {
            return false;
        };
        if is_exact_scheduler(ancestor) {
            return true;
        }
        if ancestor.ppid == ancestor_pid {
            return false;
        }
        ancestor_pid = ancestor.ppid;
    }
    false
}

fn is_scheduler_managed_aria_child(
    samples: &BTreeMap<u32, ProcessSample>,
    aria: &ProcessSample,
) -> bool {
    let Some(parent) = samples.get(&aria.ppid) else {
        return false;
    };
    is_exact_blockzilla_acquire_car(parent) && acquire_has_scheduler_ancestor(samples, parent)
}

fn option_value<'a>(args: &'a [String], option: &str) -> Option<&'a str> {
    for (index, item) in args.iter().enumerate() {
        if item == option {
            return args.get(index + 1).map(String::as_str);
        }
        if let Some(value) = item
            .strip_prefix(option)
            .and_then(|item| item.strip_prefix('='))
        {
            return Some(value);
        }
    }
    None
}

fn previous_sample<'a>(
    previous: &'a BTreeMap<u32, ProcessSample>,
    sample: &ProcessSample,
) -> Option<&'a ProcessSample> {
    previous
        .get(&sample.pid)
        .filter(|candidate| candidate.start_ticks == sample.start_ticks)
}

fn nonnegative_rate(current: u64, previous: Option<u64>, elapsed: f64) -> Option<f64> {
    let previous = previous?;
    if !elapsed.is_finite() || elapsed <= 0.0 || current < previous {
        return None;
    }
    Some((current - previous) as f64 / elapsed)
}

fn rounded_nonnegative(value: Option<f64>) -> Option<f64> {
    let value = value?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    // Formatting uses correctly rounded decimal conversion and matches the
    // Python oracle's round(value, 3), including binary half-way edge cases.
    Some(format!("{value:.3}").parse().expect("finite decimal"))
}

fn read_bounded_tail(file: &mut File, end: u64) -> Result<Vec<u8>> {
    let start = end.saturating_sub(MAX_JOURNAL_TAIL_BYTES);
    file.seek(SeekFrom::Start(start))?;
    let tail_len = end - start;
    let mut payload = Vec::with_capacity(tail_len as usize);
    Read::take(file, tail_len).read_to_end(&mut payload)?;
    Ok(payload)
}

fn read_last_json_line(path: &Path) -> Result<(Value, u64)> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect journal {}", path.display()))?;
    if !metadata.is_file() {
        bail!("journal is not a regular file: {}", path.display());
    }
    let modified = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .context("file modification time is before the Unix epoch")?
        .as_secs();
    let end = file.seek(SeekFrom::End(0))?;
    if end == 0 {
        bail!("journal is empty");
    }
    let payload = read_bounded_tail(&mut file, end)?;
    let line = payload
        .split(|byte| *byte == b'\n' || *byte == b'\r')
        .rev()
        .find(|line| line.iter().any(|byte| !byte.is_ascii_whitespace()))
        .context("journal has no record")?;
    Ok((
        serde_json::from_slice(line).context("decode last journal record")?,
        modified,
    ))
}

fn json_nonnegative_integer(value: &Value) -> Option<u64> {
    if let Some(value) = value.as_u64() {
        return Some(value);
    }
    if let Some(value) = value.as_i64() {
        return u64::try_from(value).ok();
    }
    if let Some(value) = value.as_str() {
        return value.parse::<u64>().ok();
    }
    let value = value.as_f64()?;
    (value.is_finite() && value >= 0.0 && value.trunc() <= u64::MAX as f64)
        .then(|| value.trunc() as u64)
}

#[cfg(test)]
fn file_modified_unix_secs(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path)?
        .modified()?
        .duration_since(UNIX_EPOCH)
        .context("file modification time is before the Unix epoch")?
        .as_secs())
}

fn live_capture_status(
    samples: &BTreeMap<u32, ProcessSample>,
    previous: &BTreeMap<u32, ProcessSample>,
    elapsed: f64,
    now_unix_secs: u64,
) -> Option<RuntimeLiveCapture> {
    let mut candidates = samples
        .values()
        .filter(|sample| {
            command_has(sample, "hivezilla", Some("record-grpc-raw"))
                || command_has(sample, "blockzilla-live-producer", Some("record-grpc-raw"))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|sample| std::cmp::Reverse((sample.start_ticks, sample.pid)));
    for sample in candidates {
        let Some(output_dir) = option_value(&sample.args, "--output-dir") else {
            continue;
        };
        let journal = Path::new(output_dir).join("raw-blocks.jsonl");
        let (record, modified) = match read_last_json_line(&journal) {
            Ok((Value::Object(record), modified)) => (record, modified),
            _ => continue,
        };
        let Some(slot) = record.get("slot").and_then(json_nonnegative_integer) else {
            continue;
        };
        let epoch = record
            .get("epoch")
            .and_then(json_nonnegative_integer)
            .unwrap_or(slot / SLOTS_PER_EPOCH);
        let frame_id = record
            .get("frame_id")
            .and_then(json_nonnegative_integer)
            .unwrap_or(0);
        if epoch != slot / SLOTS_PER_EPOCH {
            continue;
        }
        if modified < sample.start_unix_secs || now_unix_secs < sample.start_unix_secs {
            continue;
        }
        let Some(blocks_written) = frame_id.checked_add(1) else {
            continue;
        };
        let old = previous_sample(previous, sample);
        let write_rate = nonnegative_rate(
            sample.write_bytes,
            old.map(|sample| sample.write_bytes),
            elapsed,
        );
        return Some(RuntimeLiveCapture {
            state: if now_unix_secs <= modified.saturating_add(30) {
                "capturing"
            } else {
                "stalled"
            },
            mode: "raw_wal",
            source: "grpc",
            pid: sample.pid,
            epoch,
            last_slot: slot,
            blocks_written,
            bytes_written: sample.write_bytes,
            write_mib_per_sec: rounded_nonnegative(write_rate.map(|rate| rate / MIB)),
            started_unix_secs: sample.start_unix_secs,
            updated_unix_secs: now_unix_secs.min(modified),
        });
    }
    None
}

fn epoch_car_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"^epoch-([0-9]+)[.]car(?:[.]zst)?(?:[.]part)?$").unwrap())
}

fn epoch_from_car_name(name: &str) -> Option<u64> {
    epoch_car_regex()
        .captures(name)?
        .get(1)?
        .as_str()
        .parse()
        .ok()
}

fn open_car_part(proc_root: &Path, pid: u32) -> Result<Option<(PathBuf, u64)>> {
    let fd_root = proc_root.join(pid.to_string()).join("fd");
    for descriptor in fs::read_dir(&fd_root)? {
        let Ok(descriptor) = descriptor else { continue };
        let Ok(target) = fs::read_link(descriptor.path()) else {
            continue;
        };
        let Some(name) = target.file_name().map(|name| name.to_string_lossy()) else {
            continue;
        };
        if !name.ends_with(".part") {
            continue;
        }
        if let Some(epoch) = epoch_from_car_name(&name) {
            return Ok(Some((target, epoch)));
        }
    }
    Ok(None)
}

fn eta_seconds(bytes_remaining: u64, bytes_per_sec: Option<f64>) -> Option<u64> {
    let rate = bytes_per_sec?;
    if !rate.is_finite() || rate <= 0.0 {
        return None;
    }
    let value = (bytes_remaining as f64 / rate).round_ties_even();
    (value.is_finite() && value >= 0.0 && value <= u64::MAX as f64).then_some(value as u64)
}

fn job_id(prefix: &str, epoch: u64, sample: &ProcessSample) -> String {
    format!(
        "{prefix}-epoch-{epoch}-pid-{}-start-{}",
        sample.pid, sample.start_ticks
    )
}

fn checksum_jobs(
    proc_root: &Path,
    samples: &BTreeMap<u32, ProcessSample>,
    previous: &BTreeMap<u32, ProcessSample>,
    elapsed: f64,
    now_unix_secs: u64,
) -> Vec<RuntimeJob> {
    let mut jobs = Vec::new();
    for sample in samples.values() {
        if !command_has(sample, "sha256sum", None)
            || !sample
                .args
                .iter()
                .skip(1)
                .any(|item| item == "-c" || item == "--check")
        {
            continue;
        }
        let Some((source, epoch)) = open_car_part(proc_root, sample.pid).ok().flatten() else {
            continue;
        };
        let Ok(total) = fs::metadata(&source).map(|metadata| metadata.len()) else {
            continue;
        };
        let done = sample.rchar.min(total);
        let old = previous_sample(previous, sample);
        let bytes_per_sec =
            nonnegative_rate(done, old.map(|sample| sample.rchar.min(total)), elapsed);
        jobs.push(RuntimeJob {
            id: job_id("car-verify", epoch, sample),
            kind: "car_verify",
            epoch,
            phase: "checksum",
            state: "running",
            pid: sample.pid,
            bytes_done: done,
            bytes_total: Some(total),
            progress_pct: (total > 0).then(|| {
                rounded_nonnegative(Some(done as f64 * 100.0 / total as f64))
                    .expect("bounded checksum progress")
            }),
            read_mib_per_sec: rounded_nonnegative(bytes_per_sec.map(|rate| rate / MIB)),
            write_mib_per_sec: Some(0.0),
            eta_secs: eta_seconds(total.saturating_sub(done), bytes_per_sec),
            rss_bytes: Some(sample.rss_bytes),
            started_unix_secs: sample.start_unix_secs,
            updated_unix_secs: now_unix_secs,
        });
    }
    jobs
}

fn expected_size_from_parent(
    samples: &BTreeMap<u32, ProcessSample>,
    sample: &ProcessSample,
    source: &Path,
) -> Option<u64> {
    let parent = samples.get(&sample.ppid)?;
    let source = source.to_string_lossy();
    if is_exact_blockzilla_acquire_car(parent)
        && option_value(&parent.args, "--part") == Some(source.as_ref())
    {
        return option_value(&parent.args, "--expected-bytes")?
            .parse::<u64>()
            .ok()
            .filter(|bytes| *bytes > 0);
    }

    // Compatibility with the retired shell publisher contract, which placed
    // the expected byte count immediately after the output path.
    parent.args.windows(2).find_map(|items| {
        (items[0] == source.as_ref())
            .then(|| items[1].parse::<u64>().ok())
            .flatten()
            .filter(|bytes| *bytes > 0)
    })
}

fn download_jobs(
    samples: &BTreeMap<u32, ProcessSample>,
    previous: &BTreeMap<u32, ProcessSample>,
    elapsed: f64,
    now_unix_secs: u64,
) -> Vec<RuntimeJob> {
    let mut jobs = Vec::new();
    for sample in samples.values() {
        if !command_has(sample, "aria2c", None) {
            continue;
        }
        if is_scheduler_managed_aria_child(samples, sample) {
            // The scheduler already publishes the acquire-car parent as its
            // logical download lane. Do not emit a second job for its aria2c
            // implementation child.
            continue;
        }
        let Some(output) = option_value(&sample.args, "--out") else {
            continue;
        };
        let source = Path::new(option_value(&sample.args, "--dir").unwrap_or(".")).join(output);
        let Some(name) = source.file_name().map(|name| name.to_string_lossy()) else {
            continue;
        };
        let Some(epoch) = epoch_from_car_name(&name) else {
            continue;
        };
        let mut done = fs::metadata(&source)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let total = expected_size_from_parent(samples, sample, &source);
        let old = previous_sample(previous, sample);
        let bytes_per_sec = nonnegative_rate(
            sample.write_bytes,
            old.map(|sample| sample.write_bytes),
            elapsed,
        );
        let (progress_pct, eta_secs) = match total.filter(|total| *total > 0) {
            Some(total) => {
                done = done.min(total);
                let progress = rounded_nonnegative(Some(done as f64 * 100.0 / total as f64))
                    .expect("bounded download progress");
                (
                    Some(progress),
                    eta_seconds(total.saturating_sub(done), bytes_per_sec),
                )
            }
            None => (None, None),
        };
        jobs.push(RuntimeJob {
            id: job_id("car-download", epoch, sample),
            kind: "car_download",
            epoch,
            phase: "download",
            state: "running",
            pid: sample.pid,
            bytes_done: done,
            bytes_total: total,
            progress_pct,
            read_mib_per_sec: Some(0.0),
            write_mib_per_sec: rounded_nonnegative(bytes_per_sec.map(|rate| rate / MIB)),
            eta_secs,
            rss_bytes: Some(sample.rss_bytes),
            started_unix_secs: sample.start_unix_secs,
            updated_unix_secs: now_unix_secs,
        });
    }
    jobs
}

fn has_blockzilla_command_marker(sample: &ProcessSample) -> bool {
    const RUST_EXECUTABLES: &[&str] = &[
        "blockzilla",
        "blockzilla-archive-gateway",
        "blockzilla-get-block",
        "blockzilla-live-producer",
        "blockzilla-monitor",
        "blockzilla-replay-poc",
        "blockzilla-user-program-index",
        "blockzilla-firebase-indexer",
        "firewatch-index-controller",
        "hivezilla",
        "index-parity",
    ];
    if process_executable_name(sample).is_some_and(|name| RUST_EXECUTABLES.contains(&name)) {
        return true;
    }
    if sample
        .args
        .iter()
        .any(|item| item == "hivezilla-car-download")
    {
        return true;
    }
    sample.args.iter().skip(1).any(|item| {
        Path::new(item).file_name().and_then(|name| name.to_str())
            == Some("publish-runtime-operations.py")
    })
}

fn is_blockzilla_owned(samples: &BTreeMap<u32, ProcessSample>, sample: &ProcessSample) -> bool {
    if has_blockzilla_command_marker(sample) {
        return true;
    }
    let mut parent_pid = sample.ppid;
    for _ in 0..MAX_PROCESS_ANCESTORS {
        let Some(parent) = samples.get(&parent_pid) else {
            return false;
        };
        if has_blockzilla_command_marker(parent) {
            return true;
        }
        if parent.ppid == parent_pid {
            return false;
        }
        parent_pid = parent.ppid;
    }
    false
}

pub fn process_io_status(
    samples: &BTreeMap<u32, ProcessSample>,
    previous: &BTreeMap<u32, ProcessSample>,
    elapsed: f64,
    inaccessible: usize,
    clock_ticks: u64,
    now_unix_secs: u64,
) -> ProcessIoStatus {
    let mut entries = Vec::new();
    for sample in samples.values() {
        if is_blockzilla_owned(samples, sample) {
            continue;
        }
        let old = previous_sample(previous, sample);
        let read_mib = rounded_nonnegative(
            nonnegative_rate(
                sample.read_bytes,
                old.map(|sample| sample.read_bytes),
                elapsed,
            )
            .map(|rate| rate / MIB),
        );
        let write_mib = rounded_nonnegative(
            nonnegative_rate(
                sample.write_bytes,
                old.map(|sample| sample.write_bytes),
                elapsed,
            )
            .map(|rate| rate / MIB),
        );
        let (Some(read_mib), Some(write_mib)) = (read_mib, write_mib) else {
            continue;
        };
        if read_mib + write_mib <= 0.0 {
            continue;
        }
        let cpu_percent = rounded_nonnegative(
            nonnegative_rate(
                sample.cpu_ticks,
                old.map(|sample| sample.cpu_ticks),
                elapsed,
            )
            .map(|rate| rate * 100.0 / clock_ticks as f64),
        );
        entries.push(ProcessIoEntry {
            id: format!("{}-{}", sample.pid, sample.start_ticks),
            pid: sample.pid,
            name: sample.name.clone(),
            read_mib_per_sec: read_mib,
            write_mib_per_sec: write_mib,
            cpu_percent,
            rss_bytes: sample.rss_bytes,
            blockzilla_owned: false,
        });
    }
    entries.sort_by(|left, right| {
        let left_rate = left.read_mib_per_sec + left.write_mib_per_sec;
        let right_rate = right.read_mib_per_sec + right.write_mib_per_sec;
        right_rate
            .total_cmp(&left_rate)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.pid.cmp(&right.pid))
    });
    let active_count = entries.len();
    let truncated = active_count > MAX_PROCESSES;
    entries.truncate(MAX_PROCESSES);
    let ready = !previous.is_empty();
    ProcessIoStatus {
        state: if ready { "ready" } else { "collecting" },
        sampled_unix_secs: now_unix_secs,
        sample_window_secs: (ready && elapsed > 0.0)
            .then(|| rounded_nonnegative(Some(elapsed)).unwrap()),
        active_count,
        inaccessible_count: inaccessible,
        truncated,
        processes: entries,
        message: (!ready).then_some("Collecting the first process-counter interval"),
    }
}

fn build_status(
    proc_root: &Path,
    samples: &BTreeMap<u32, ProcessSample>,
    previous: &BTreeMap<u32, ProcessSample>,
    elapsed: f64,
    inaccessible: usize,
    clock_ticks: u64,
    now_unix_secs: u64,
) -> RuntimeOperationsStatus {
    let mut jobs = checksum_jobs(proc_root, samples, previous, elapsed, now_unix_secs);
    jobs.extend(download_jobs(samples, previous, elapsed, now_unix_secs));
    jobs.sort_by(|left, right| {
        left.epoch
            .cmp(&right.epoch)
            .then_with(|| left.kind.cmp(right.kind))
            .then_with(|| left.id.cmp(&right.id))
    });
    jobs.truncate(MAX_JOBS);
    RuntimeOperationsStatus {
        schema_version: 1,
        updated_unix_secs: now_unix_secs,
        live_capture: live_capture_status(samples, previous, elapsed, now_unix_secs),
        jobs,
        process_io: process_io_status(
            samples,
            previous,
            elapsed,
            inaccessible,
            clock_ticks,
            now_unix_secs,
        ),
    }
}

struct PublisherLock {
    #[allow(dead_code)]
    file: File,
}

impl PublisherLock {
    #[cfg(unix)]
    fn acquire(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("open runtime operations lock {}", path.display()))?;
        // SAFETY: `file` owns a valid open descriptor for the duration of this
        // object. flock does not dereference pointers.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK)
            {
                bail!("runtime operations publisher is already running");
            }
            return Err(error)
                .with_context(|| format!("lock runtime operations publisher {}", path.display()));
        }
        Ok(Self { file })
    }

    #[cfg(not(unix))]
    fn acquire(_path: &Path) -> Result<Self> {
        bail!("runtime operations publisher locking requires Unix")
    }
}

fn publish_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("runtime operations output needs a UTF-8 file name")?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!(".{name}.{}.{}.tmp", std::process::id(), nonce));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o644);
        let mut file = options
            .open(&temporary)
            .with_context(|| format!("create {}", temporary.display()))?;
        serde_json::to_writer(&mut file, value).context("encode runtime operations status")?;
        file.write_all(b"\n")?;
        file.flush()?;
        file.sync_all()?;
        #[cfg(unix)]
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o644))?;
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "publish runtime operations {} to {}",
                temporary.display(),
                path.display()
            )
        })?;
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
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

    fn temporary_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "blockzilla-runtime-operations-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn sample(pid: u32, args: &[&str]) -> ProcessSample {
        ProcessSample {
            pid,
            ppid: 1,
            start_ticks: u64::from(pid) * 10,
            start_unix_secs: 1_000,
            name: Path::new(args[0])
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            args: args.iter().map(|value| (*value).to_string()).collect(),
            rchar: 0,
            read_bytes: 0,
            write_bytes: 0,
            cpu_ticks: 10,
            rss_bytes: 4_096,
        }
    }

    #[test]
    fn proc_stat_parser_handles_spaces_and_closing_parentheses_in_comm() {
        let mut fields = vec!["0"; 20];
        fields[0] = "S";
        fields[1] = "7";
        fields[11] = "11";
        fields[12] = "13";
        fields[19] = "101";
        let value = format!("42 (a worker) name) {}", fields.join(" "));
        assert_eq!(
            parse_proc_stat(&value).unwrap(),
            ProcStat {
                ppid: 7,
                cpu_ticks: 24,
                start_ticks: 101,
            }
        );
    }

    #[test]
    fn decimal_and_eta_rounding_match_python_oracle() {
        assert_eq!(rounded_nonnegative(Some(0.0025)), Some(0.003));
        assert_eq!(rounded_nonnegative(Some(1.2345)), Some(1.234));
        assert_eq!(eta_seconds(5, Some(2.0)), Some(2));
        assert_eq!(eta_seconds(7, Some(2.0)), Some(4));
    }

    #[test]
    fn car_name_contract_accepts_raw_and_zstd_producer_names_only() {
        for name in [
            "epoch-9.car",
            "epoch-9.car.part",
            "epoch-9.car.zst",
            "epoch-9.car.zst.part",
        ] {
            assert_eq!(epoch_from_car_name(name), Some(9), "{name}");
        }
        for name in [
            "prefix-epoch-9.car",
            "epoch-9.car.gz",
            "epoch-9.car.zst.tmp",
            "epoch-.car",
            "epoch-18446744073709551616.car",
        ] {
            assert_eq!(epoch_from_car_name(name), None, "{name}");
        }
    }

    #[test]
    fn command_line_reads_and_argument_retention_are_bounded() {
        let root = temporary_root("cmdline-bounds");
        let cmdline = root.join("cmdline");
        fs::write(&cmdline, vec![b'x'; MAX_CMDLINE_BYTES as usize + 1]).unwrap();
        assert!(
            process_args(&cmdline)
                .unwrap_err()
                .to_string()
                .contains("exceeds 65536 bytes")
        );

        let mut too_many_args = Vec::new();
        for _ in 0..=MAX_CMDLINE_ARGS {
            too_many_args.extend_from_slice(b"x\0");
        }
        fs::write(&cmdline, too_many_args).unwrap();
        assert!(
            process_args(&cmdline)
                .unwrap_err()
                .to_string()
                .contains("exceeds 256 arguments")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn wal_tail_is_strictly_bounded_and_rejects_non_regular_inputs() {
        let root = temporary_root("wal-tail");
        assert!(
            read_last_json_line(&root)
                .unwrap_err()
                .to_string()
                .contains("not a regular file")
        );

        let journal = root.join("raw-blocks.jsonl");
        let mut contents = vec![b'x'; MAX_JOURNAL_TAIL_BYTES as usize + 100];
        contents.extend_from_slice(
            b"\n{\"schema_version\":1,\"frame_id\":4,\"slot\":433719121,\"epoch\":1003}\n",
        );
        fs::write(&journal, contents).unwrap();
        let mut file = File::open(&journal).unwrap();
        let end = file.seek(SeekFrom::End(0)).unwrap();
        let tail = read_bounded_tail(&mut file, end).unwrap();
        assert_eq!(tail.len(), MAX_JOURNAL_TAIL_BYTES as usize);
        let (record, _) = read_last_json_line(&journal).unwrap();
        assert_eq!(record["slot"], 433_719_121u64);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn collection_filters_other_uids_and_counts_inaccessible_same_uid_processes() {
        let root = temporary_root("same-uid");
        fs::write(root.join("stat"), "cpu 0\nbtime 900\n").unwrap();
        let process = root.join("42");
        fs::create_dir(&process).unwrap();
        let actual_uid = fs::metadata(&process).unwrap().uid();

        let filtered =
            collect_processes_for_uid(&root, actual_uid.saturating_add(1), 100, 4096).unwrap();
        assert!(filtered.samples.is_empty());
        assert_eq!(filtered.inaccessible, 0);

        let included = collect_processes_for_uid(&root, actual_uid, 100, 4096).unwrap();
        assert!(included.samples.is_empty());
        assert_eq!(included.inaccessible, 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn compressed_checksum_fixture_matches_legacy_producer_contract() {
        let root = temporary_root("checksum");
        let part = root.join("epoch-1001.car.zst.part");
        fs::write(&part, vec![0; 1_000]).unwrap();
        let fd_root = root.join("proc/43/fd");
        fs::create_dir_all(&fd_root).unwrap();
        symlink(&part, fd_root.join("3")).unwrap();
        let mut current = sample(43, &["sha256sum", "-c", "-"]);
        current.rchar = 600;
        current.read_bytes = 600;
        let mut old = current.clone();
        old.rchar = 200;
        old.read_bytes = 200;
        let current = BTreeMap::from([(43, current)]);
        let previous = BTreeMap::from([(43, old)]);
        let jobs = checksum_jobs(&root.join("proc"), &current, &previous, 2.0, 2_000);
        assert_eq!(jobs.len(), 1);
        let value = serde_json::to_value(&jobs[0]).unwrap();
        assert_eq!(value["kind"], "car_verify");
        assert_eq!(value["epoch"], 1001);
        assert_eq!(value["bytes_done"], 600);
        assert_eq!(value["progress_pct"], 60.0);
        assert_eq!(value["eta_secs"], 2);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn capture_fixture_redacts_command_line_and_endpoint() {
        let root = temporary_root("capture");
        let output = root.join("raw");
        fs::create_dir(&output).unwrap();
        let journal = output.join("raw-blocks.jsonl");
        fs::write(
            &journal,
            b"{\"schema_version\":1,\"frame_id\":9,\"slot\":433719121,\"epoch\":1003}\n",
        )
        .unwrap();
        let now = file_modified_unix_secs(&journal).unwrap();
        let output_string = output.to_string_lossy().into_owned();
        let mut current = sample(
            44,
            &[
                "/bin/blockzilla-live-producer",
                "record-grpc-raw",
                "--endpoint",
                "https://secret.example",
                "--output-dir",
                &output_string,
            ],
        );
        current.write_bytes = 10 * 1024 * 1024;
        let mut old = current.clone();
        old.write_bytes = 2 * 1024 * 1024;
        let status = build_status(
            &root.join("proc"),
            &BTreeMap::from([(44, current)]),
            &BTreeMap::from([(44, old)]),
            2.0,
            0,
            100,
            now,
        );
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(value["live_capture"]["state"], "capturing");
        assert_eq!(value["live_capture"]["epoch"], 1003);
        assert_eq!(value["live_capture"]["last_slot"], 433_719_121u64);
        assert_eq!(value["live_capture"]["write_mib_per_sec"], 4.0);
        let serialized = serde_json::to_string(&value).unwrap();
        assert!(!serialized.contains("secret.example"));
        assert!(!serialized.contains(&output_string));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn canonical_hivezilla_capture_is_detected() {
        let root = temporary_root("hivezilla-capture");
        let output = root.join("raw");
        fs::create_dir(&output).unwrap();
        let journal = output.join("raw-blocks.jsonl");
        fs::write(
            &journal,
            b"{\"schema_version\":1,\"frame_id\":4,\"slot\":433719121,\"epoch\":1003}\n",
        )
        .unwrap();
        let output_string = output.to_string_lossy().into_owned();
        let current = sample(
            46,
            &[
                "/usr/local/bin/hivezilla",
                "record-grpc-raw",
                "--output-dir",
                &output_string,
            ],
        );
        let capture = live_capture_status(
            &BTreeMap::from([(46, current)]),
            &BTreeMap::new(),
            2.0,
            file_modified_unix_secs(&journal).unwrap(),
        )
        .unwrap();
        assert_eq!(capture.last_slot, 433_719_121);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stale_journal_from_before_process_start_is_not_attributed_to_capture() {
        let root = temporary_root("stale-capture");
        let output = root.join("raw");
        fs::create_dir(&output).unwrap();
        let journal = output.join("raw-blocks.jsonl");
        fs::write(
            &journal,
            b"{\"schema_version\":1,\"frame_id\":4,\"slot\":433719121,\"epoch\":1003}\n",
        )
        .unwrap();
        let modified = file_modified_unix_secs(&journal).unwrap();
        let output_string = output.to_string_lossy().into_owned();
        let mut current = sample(
            47,
            &[
                "/usr/local/bin/hivezilla",
                "record-grpc-raw",
                "--output-dir",
                &output_string,
            ],
        );
        current.start_unix_secs = modified + 1;
        assert!(
            live_capture_status(
                &BTreeMap::from([(47, current)]),
                &BTreeMap::new(),
                2.0,
                modified + 10,
            )
            .is_none()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pid_reuse_and_counter_regression_never_publish_rates() {
        let mut current = sample(45, &["sha256sum", "--check", "secret-path"]);
        current.read_bytes = 20 * 1024 * 1024;
        current.cpu_ticks = 30;

        let mut reused = current.clone();
        reused.start_ticks -= 1;
        reused.read_bytes = 10 * 1024 * 1024;
        let result = process_io_status(
            &BTreeMap::from([(45, current.clone())]),
            &BTreeMap::from([(45, reused)]),
            2.0,
            0,
            100,
            2_000,
        );
        assert!(result.processes.is_empty());

        let mut regressed = current.clone();
        regressed.read_bytes = current.read_bytes + 1;
        let result = process_io_status(
            &BTreeMap::from([(45, current)]),
            &BTreeMap::from([(45, regressed)]),
            2.0,
            0,
            100,
            2_000,
        );
        assert!(result.processes.is_empty());
    }

    #[test]
    fn process_output_is_bounded_sorted_and_redacted() {
        let mut current = BTreeMap::new();
        let mut previous = BTreeMap::new();
        for pid in 1..=25 {
            let mut sample = sample(pid, &["/usr/bin/worker", "secret-path"]);
            sample.read_bytes = u64::from(pid) * 1024 * 1024;
            let mut old = sample.clone();
            old.read_bytes = 0;
            old.cpu_ticks = 0;
            current.insert(pid, sample);
            previous.insert(pid, old);
        }
        let result = process_io_status(&current, &previous, 1.0, 3, 100, 2_000);
        assert_eq!(result.active_count, 25);
        assert_eq!(result.processes.len(), MAX_PROCESSES);
        assert!(result.truncated);
        assert_eq!(result.processes[0].pid, 25);
        let serialized = serde_json::to_string(&result).unwrap();
        assert!(!serialized.contains("secret-path"));
        assert!(!serialized.contains("args"));
    }

    #[test]
    fn monitor_process_is_not_reported_as_competing_host_io() {
        let monitor = sample(
            44,
            &[
                "/volume1/blockzilla/bin/blockzilla-monitor",
                "--upstream",
                "http://127.0.0.1:8786",
            ],
        );
        let samples = BTreeMap::from([(monitor.pid, monitor.clone())]);
        assert!(is_blockzilla_owned(&samples, &monitor));
    }

    #[test]
    fn firewatch_processes_are_not_reported_as_competing_host_io() {
        for executable in [
            "firewatch-index-controller",
            "blockzilla-user-program-index",
            "blockzilla-firebase-indexer",
            "index-parity",
        ] {
            let process = sample(44, &[&format!("/volume1/blockzilla/bin/{executable}")]);
            let samples = BTreeMap::from([(process.pid, process.clone())]);
            assert!(is_blockzilla_owned(&samples, &process), "{executable}");
        }
    }

    #[test]
    fn job_output_is_bounded_and_sorted() {
        let root = temporary_root("bounded-jobs");
        let mut current = BTreeMap::new();
        for epoch in (1..=MAX_JOBS as u64 + 6).rev() {
            let output = format!("epoch-{epoch}.car.part");
            let process = sample(
                u32::try_from(epoch).unwrap(),
                &[
                    "/usr/bin/aria2c",
                    "--out",
                    &output,
                    "--dir",
                    "/private/archive",
                ],
            );
            current.insert(process.pid, process);
        }
        let status = build_status(&root, &current, &BTreeMap::new(), 0.0, 0, 100, 2_000);
        assert_eq!(status.jobs.len(), MAX_JOBS);
        assert_eq!(status.jobs.first().unwrap().epoch, 1);
        assert_eq!(status.jobs.last().unwrap().epoch, MAX_JOBS as u64);
        let serialized = serde_json::to_string(&status).unwrap();
        assert!(!serialized.contains("/private/archive"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn duplicate_epoch_jobs_have_stable_unique_process_identity_ids() {
        let mut current = BTreeMap::new();
        for pid in [70, 71] {
            let process = sample(
                pid,
                &[
                    "/usr/bin/aria2c",
                    "--dir",
                    "/private/archive",
                    "--out",
                    "epoch-713.car.zst.part",
                ],
            );
            current.insert(pid, process);
        }
        let status = build_status(
            Path::new("/unused/proc"),
            &current,
            &BTreeMap::new(),
            0.0,
            0,
            100,
            2_000,
        );
        assert_eq!(status.jobs.len(), 2);
        assert_ne!(status.jobs[0].id, status.jobs[1].id);
        assert_eq!(status.jobs[0].id, "car-download-epoch-713-pid-70-start-700");
        assert_eq!(status.jobs[1].id, "car-download-epoch-713-pid-71-start-710");
    }

    #[test]
    fn rust_scheduler_aria_child_is_deduplicated_but_remains_managed() {
        let root = temporary_root("rust-aria-contract");
        let downloads = root.join("cars/.downloads");
        fs::create_dir_all(&downloads).unwrap();
        let part = downloads.join("epoch-713.car.zst.part");
        fs::write(&part, vec![0; 1_000]).unwrap();

        let part_string = part.to_string_lossy().into_owned();
        let downloads_string = downloads.to_string_lossy().into_owned();
        let canonical_string = root
            .join("cars/epoch-713.car.zst")
            .to_string_lossy()
            .into_owned();
        let alternate_string = root
            .join("cars/epoch-713.car")
            .to_string_lossy()
            .into_owned();
        let mut scheduler = sample(
            79,
            &[
                "/release/bin/blockzilla",
                "scheduler",
                "--car-root",
                "/private/archive",
            ],
        );
        scheduler.ppid = 1;
        let mut parent = sample(
            80,
            &[
                "/release/bin/blockzilla",
                "acquire-car",
                "--url",
                "https://files.example/epoch-713.car.zst",
                "--part",
                &part_string,
                "--canonical",
                &canonical_string,
                "--alternate",
                &alternate_string,
                "--epoch",
                "713",
                "--receipt",
                "/private/receipt.json",
                "--progress-json",
                "/private/progress.json",
                "--expected-bytes",
                "2000",
                "--max-attempts",
                "3",
                "--required-memory-mib",
                "4096",
                "--io-buffer-mib",
                "8",
            ],
        );
        parent.ppid = scheduler.pid;
        let mut child = sample(
            81,
            &[
                "/usr/bin/aria2c",
                "--continue=true",
                "--allow-overwrite=true",
                "--auto-file-renaming=false",
                "--file-allocation=none",
                "--max-connection-per-server=4",
                "--split=4",
                "--min-split-size=64M",
                "--dir",
                &downloads_string,
                "--out",
                "epoch-713.car.zst.part",
                "https://files.example/epoch-713.car.zst",
            ],
        );
        child.ppid = parent.pid;
        child.write_bytes = 8 * 1024 * 1024;
        child.read_bytes = 2 * 1024 * 1024;

        let mut old_parent = parent.clone();
        old_parent.cpu_ticks = 0;
        let mut old_scheduler = scheduler.clone();
        old_scheduler.cpu_ticks = 0;
        let mut old_child = child.clone();
        old_child.write_bytes = 2 * 1024 * 1024;
        old_child.read_bytes = 0;
        old_child.cpu_ticks = 0;
        let current = BTreeMap::from([
            (scheduler.pid, scheduler),
            (parent.pid, parent),
            (child.pid, child),
        ]);
        let previous = BTreeMap::from([
            (old_scheduler.pid, old_scheduler),
            (old_parent.pid, old_parent),
            (old_child.pid, old_child),
        ]);

        assert_eq!(
            expected_size_from_parent(&current, current.get(&81).unwrap(), &part),
            Some(2_000)
        );
        let jobs = download_jobs(&current, &previous, 2.0, 2_000);
        assert!(jobs.is_empty());

        let process_io = process_io_status(&current, &previous, 2.0, 0, 100, 2_000);
        assert!(process_io.processes.is_empty());
        let serialized = serde_json::to_string(&jobs).unwrap();
        assert!(!serialized.contains("files.example"));
        assert!(!serialized.contains(&root.to_string_lossy().into_owned()));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn standalone_typed_acquire_keeps_one_download_job_with_progress_and_eta() {
        let root = temporary_root("standalone-typed-acquire");
        let downloads = root.join("cars/.downloads");
        fs::create_dir_all(&downloads).unwrap();
        let part = downloads.join("epoch-716.car.zst.part");
        fs::write(&part, vec![0; 1024 * 1024]).unwrap();
        let part_string = part.to_string_lossy().into_owned();
        let downloads_string = downloads.to_string_lossy().into_owned();
        let mut parent = sample(
            84,
            &[
                "/release/bin/blockzilla",
                "acquire-car",
                "--part",
                &part_string,
                "--expected-bytes",
                "3145728",
            ],
        );
        parent.ppid = 1;
        let mut child = sample(
            85,
            &[
                "/usr/bin/aria2c",
                "--dir",
                &downloads_string,
                "--out",
                "epoch-716.car.zst.part",
                "https://files.example/epoch-716.car.zst",
            ],
        );
        child.ppid = parent.pid;
        child.write_bytes = 1024 * 1024;
        let mut old_parent = parent.clone();
        old_parent.cpu_ticks = 0;
        let mut old_child = child.clone();
        old_child.write_bytes = 0;
        old_child.cpu_ticks = 0;
        let current = BTreeMap::from([(parent.pid, parent), (child.pid, child)]);
        let previous = BTreeMap::from([(old_parent.pid, old_parent), (old_child.pid, old_child)]);

        let jobs = download_jobs(&current, &previous, 2.0, 2_000);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].epoch, 716);
        assert_eq!(jobs[0].bytes_done, 1024 * 1024);
        assert_eq!(jobs[0].bytes_total, Some(3 * 1024 * 1024));
        assert_eq!(jobs[0].progress_pct, Some(33.333));
        assert_eq!(jobs[0].write_mib_per_sec, Some(0.5));
        assert_eq!(jobs[0].eta_secs, Some(4));

        // It is a published logical job, but its process-tree I/O remains out
        // of the unrelated external-process list through exact ancestry.
        let process_io = process_io_status(&current, &previous, 2.0, 0, 100, 2_000);
        assert!(process_io.processes.is_empty());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn typed_acquire_expected_bytes_require_exact_parent_part_and_positive_value() {
        let source = Path::new("/cars/.downloads/epoch-713.car.zst.part");
        let mut parent = sample(
            90,
            &[
                "/release/bin/blockzilla",
                "acquire-car",
                "--part",
                source.to_str().unwrap(),
                "--expected-bytes=987654",
            ],
        );
        let mut child = sample(91, &["/usr/bin/aria2c"]);
        child.ppid = parent.pid;

        let samples = BTreeMap::from([(parent.pid, parent.clone()), (child.pid, child.clone())]);
        assert_eq!(
            expected_size_from_parent(&samples, &child, source),
            Some(987_654)
        );

        parent.args[4] = "--expected-bytes=0".to_string();
        let samples = BTreeMap::from([(parent.pid, parent.clone()), (child.pid, child.clone())]);
        assert_eq!(expected_size_from_parent(&samples, &child, source), None);

        parent.args[4] = "--expected-bytes=987654".to_string();
        parent.args[3] = "/cars/.downloads/epoch-714.car.zst.part".to_string();
        let samples = BTreeMap::from([(parent.pid, parent.clone()), (child.pid, child.clone())]);
        assert_eq!(expected_size_from_parent(&samples, &child, source), None);

        parent.args[0] = "/release/bin/blockzilla-helper".to_string();
        parent.args[3] = source.to_string_lossy().into_owned();
        let samples = BTreeMap::from([(parent.pid, parent), (child.pid, child.clone())]);
        assert_eq!(expected_size_from_parent(&samples, &child, source), None);
    }

    #[test]
    fn aria_dedup_requires_an_exact_scheduler_ancestor() {
        let mut scheduler = sample(94, &["/release/bin/blockzilla", "scheduler-extra"]);
        let mut acquire = sample(95, &["/release/bin/blockzilla", "acquire-car"]);
        acquire.ppid = scheduler.pid;
        let mut aria = sample(96, &["/usr/bin/aria2c"]);
        aria.ppid = acquire.pid;

        let samples = BTreeMap::from([
            (scheduler.pid, scheduler.clone()),
            (acquire.pid, acquire.clone()),
            (aria.pid, aria.clone()),
        ]);
        assert!(!is_scheduler_managed_aria_child(&samples, &aria));

        scheduler.args[1] = "scheduler".to_string();
        let samples = BTreeMap::from([
            (scheduler.pid, scheduler.clone()),
            (acquire.pid, acquire.clone()),
            (aria.pid, aria.clone()),
        ]);
        assert!(is_scheduler_managed_aria_child(&samples, &aria));

        scheduler.args = vec!["/release/bin/hivezilla".into(), "pipeline".into()];
        let samples = BTreeMap::from([
            (scheduler.pid, scheduler),
            (acquire.pid, acquire),
            (aria.pid, aria.clone()),
        ]);
        assert!(is_scheduler_managed_aria_child(&samples, &aria));
    }

    #[test]
    fn near_match_parent_is_neither_deduplicated_nor_marked_owned() {
        let root = temporary_root("near-match-parent");
        let downloads = root.join("contains-blockzilla/archive");
        fs::create_dir_all(&downloads).unwrap();
        let part = downloads.join("epoch-715.car.part");
        fs::write(&part, vec![0; 1_000]).unwrap();
        let part_string = part.to_string_lossy().into_owned();
        let downloads_string = downloads.to_string_lossy().into_owned();
        let mut parent = sample(
            92,
            &["/release/bin/blockzilla-helper", &part_string, "2000"],
        );
        let mut child = sample(
            93,
            &[
                "/usr/bin/aria2c",
                "--dir",
                &downloads_string,
                "--out",
                "epoch-715.car.part",
                "https://files.example/epoch-715.car",
            ],
        );
        child.ppid = parent.pid;
        child.write_bytes = 4 * 1024 * 1024;
        let mut old_child = child.clone();
        old_child.write_bytes = 0;
        let mut old_parent = parent.clone();
        old_parent.write_bytes = 0;
        parent.write_bytes = 0;
        let current = BTreeMap::from([(parent.pid, parent), (child.pid, child)]);
        let previous = BTreeMap::from([(old_parent.pid, old_parent), (old_child.pid, old_child)]);

        let jobs = download_jobs(&current, &previous, 2.0, 2_000);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].epoch, 715);
        assert_eq!(jobs[0].bytes_done, 1_000);
        assert_eq!(jobs[0].bytes_total, Some(2_000));
        assert_eq!(jobs[0].progress_pct, Some(50.0));
        assert_eq!(jobs[0].eta_secs, Some(0));
        let process_io = process_io_status(&current, &previous, 2.0, 0, 100, 2_000);
        assert!(process_io.processes.iter().any(|process| process.pid == 93));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_equals_form_aria_contract_remains_detected() {
        let root = temporary_root("legacy-aria-contract");
        let downloads = root.join("downloads");
        fs::create_dir(&downloads).unwrap();
        fs::write(downloads.join("epoch-714.car.part"), vec![0; 50]).unwrap();
        let directory_arg = format!("--dir={}", downloads.display());
        let process = sample(
            82,
            &[
                "/usr/bin/aria2c",
                &directory_arg,
                "--out=epoch-714.car.part",
                "https://files.example/epoch-714.car",
            ],
        );
        let jobs = download_jobs(
            &BTreeMap::from([(82, process)]),
            &BTreeMap::new(),
            0.0,
            2_000,
        );
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].epoch, 714);
        assert_eq!(jobs[0].bytes_done, 50);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn once_mode_collects_fake_procfs_and_publishes_redacted_json() {
        let root = temporary_root("once");
        let proc_root = root.join("proc");
        let process_root = proc_root.join("42");
        fs::create_dir_all(&process_root).unwrap();
        fs::write(proc_root.join("stat"), "cpu 0\nbtime 900\n").unwrap();

        let mut stat_fields = vec!["0"; 20];
        stat_fields[0] = "S";
        stat_fields[1] = "1";
        stat_fields[11] = "2";
        stat_fields[12] = "3";
        stat_fields[19] = "100";
        fs::write(
            process_root.join("stat"),
            format!("42 (worker) {}\n", stat_fields.join(" ")),
        )
        .unwrap();
        fs::write(
            process_root.join("io"),
            "rchar: 50\nread_bytes: 30\nwrite_bytes: 20\n",
        )
        .unwrap();
        fs::write(process_root.join("statm"), "2 1\n").unwrap();
        fs::write(process_root.join("comm"), "worker\n").unwrap();
        fs::write(
            process_root.join("cmdline"),
            b"/usr/bin/worker\0--token\0secret-value\0",
        )
        .unwrap();

        let output = root.join("runtime-operations.json");
        publish(PublishArgs {
            output: output.clone(),
            proc_root,
            interval_secs: 1.0,
            once: true,
        })
        .await
        .unwrap();

        let payload = fs::read_to_string(&output).unwrap();
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["process_io"]["state"], "collecting");
        assert_eq!(value["process_io"]["inaccessible_count"], 0);
        assert_eq!(value["process_io"]["active_count"], 0);
        assert!(!payload.contains("secret-value"));
        assert!(!payload.contains("--token"));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn atomic_publication_has_public_mode_and_no_temporary_residue() {
        let root = temporary_root("atomic");
        let output = root.join("status.json");
        let status = build_status(
            &root.join("proc"),
            &BTreeMap::new(),
            &BTreeMap::new(),
            0.0,
            0,
            100,
            2_000,
        );
        publish_atomic(&output, &status).unwrap();
        let bytes = fs::read(&output).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o644
        );
        assert_eq!(
            fs::read_dir(&root)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
                .count(),
            0
        );
        fs::remove_dir_all(root).unwrap();
    }
}
