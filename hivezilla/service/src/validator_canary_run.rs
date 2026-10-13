//! Finite local capture/normalization/compaction controller. No service or upload authority.
use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE, read_archive_v2_hot_block_index,
};
use blockzilla_live_format::execution_canary_v1::{
    EXECUTION_CANARY_FILE, EXECUTION_SOURCE_PROFILE, FinalizedRpcCoverageV1,
    NORMALIZED_EXECUTION_FILE,
};
use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::{
    fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    process::CommandExt,
};
use std::{
    collections::BTreeSet,
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_JSON: u64 = 16 << 20;
const MAX_FILE: u64 = 512 << 20;
const SEAL: &str = "CURRENT-GRPC-SEALED.v1.json";
const EPOCH_SLOTS: u64 = 432_000;

#[derive(Debug, Clone, clap::Args)]
pub struct RunLocalExecutionCanaryArgs {
    /// New absolute directory; the parent must exist. Failed attempts are retained.
    #[arg(long)]
    pub run_dir: PathBuf,
    /// Defaults to this Hivezilla executable.
    #[arg(long)]
    pub hivezilla: Option<PathBuf>,
    #[arg(long)]
    pub blockzilla: PathBuf,
    /// Existing validator PID. This command never signals that process.
    #[arg(long)]
    pub validator_pid: u32,
    #[arg(long)]
    pub expected_genesis_hash: String,
    #[arg(long)]
    pub expected_node_identity: String,
    #[arg(long, default_value = "http://127.0.0.1:10000")]
    pub grpc_endpoint: String,
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    pub rpc_endpoint: String,
    #[arg(long, default_value_t = 32)]
    pub blocks: usize,
    #[arg(long, default_value_t = 300)]
    pub capture_timeout_secs: u64,
    #[arg(long, default_value_t = 300)]
    pub stage_timeout_secs: u64,
    #[arg(long, default_value_t = 536_870_912_000)]
    pub min_free_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FileHash {
    bytes: u64,
    sha256: String,
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn digest(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}
fn now_ns() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
}
fn open_regular(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    ensure!(
        file.metadata()?.is_file(),
        "not a regular file: {}",
        path.display()
    );
    Ok(file)
}
fn file_hash(path: &Path) -> Result<FileHash> {
    let mut file = open_regular(path)?;
    let before = file.metadata()?;
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        bytes += n as u64;
    }
    let after = file.metadata()?;
    ensure!(
        bytes == before.len()
            && before.len() == after.len()
            && before.modified()? == after.modified()?,
        "file changed while hashing"
    );
    #[cfg(unix)]
    ensure!(
        (
            before.dev(),
            before.ino(),
            before.ctime(),
            before.ctime_nsec()
        ) == (after.dev(), after.ino(), after.ctime(), after.ctime_nsec()),
        "file identity changed"
    );
    Ok(FileHash {
        bytes,
        sha256: hex(&hash.finalize()),
    })
}
fn write_new(path: &Path, body: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let mut file = options.open(path)?;
    file.write_all(body)?;
    file.sync_all()?;
    Ok(())
}
fn json_bytes(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut body = serde_json::to_vec_pretty(value)?;
    body.push(b'\n');
    Ok(body)
}
fn save_report(root: &Path, report: &Value) -> Result<()> {
    let temporary = root.join(".run-report.tmp");
    write_new(&temporary, &json_bytes(report)?)?;
    fs::rename(&temporary, root.join("run-report.json"))?;
    File::open(root)?.sync_all()?;
    Ok(())
}
fn read_json(path: &Path) -> Result<Value> {
    let file = open_regular(path)?;
    ensure!(file.metadata()?.len() <= MAX_JSON, "JSON exceeds 16 MiB");
    let mut bytes = Vec::new();
    file.take(MAX_JSON + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= MAX_JSON, "JSON grew beyond bound");
    strict_json(&bytes)
}

// Preserve exact RPC bytes, but reject duplicate keys before they can admit a range.
struct StrictValue(Value);
impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = StrictValue;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("JSON without duplicate keys")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(
                    serde_json::Number::from_f64(v).ok_or_else(|| E::custom("nonfinite number"))?,
                )))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(v) = seq.next_element::<StrictValue>()? {
                    values.push(v.0);
                }
                Ok(StrictValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                    values.insert(key, map.next_value::<StrictValue>()?.0);
                }
                Ok(StrictValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(V)
    }
}
fn strict_json(body: &[u8]) -> Result<Value> {
    let mut decoder = serde_json::Deserializer::from_slice(body);
    let value = StrictValue::deserialize(&mut decoder)?.0;
    decoder.end()?;
    Ok(value)
}
fn integer(value: &Value, name: &str) -> Result<u64> {
    value.as_u64().with_context(|| format!("invalid {name}"))
}
fn loopback_url(value: &str) -> Result<String> {
    let rest = value
        .strip_prefix("http://127.0.0.1:")
        .context("endpoint must use the literal http://127.0.0.1:PORT form")?;
    let raw_port = rest
        .split('/')
        .next()
        .context("missing endpoint port")?
        .parse::<u16>()?;
    let url = reqwest::Url::parse(value)?;
    ensure!(
        url.scheme() == "http"
            && url.host_str() == Some("127.0.0.1")
            && raw_port > 0
            && url.port_or_known_default() == Some(raw_port)
            && url.username().is_empty()
            && url.password().is_none()
            && matches!(url.path(), "" | "/")
            && url.query().is_none()
            && url.fragment().is_none(),
        "only literal http://127.0.0.1:PORT endpoints are allowed"
    );
    Ok(value.trim_end_matches('/').to_string())
}

#[derive(Clone)]
struct Rpc {
    endpoint: String,
    directory: PathBuf,
    client: reqwest::Client,
    sequence: Arc<AtomicU64>,
}
impl Rpc {
    fn new(endpoint: &str, directory: PathBuf) -> Result<Self> {
        Ok(Self {
            endpoint: loopback_url(endpoint)?,
            directory,
            sequence: Arc::new(AtomicU64::new(0)),
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                // Keep exact response bytes and encoding headers even when another
                // workspace dependency enables reqwest compression features.
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .no_zstd()
                .timeout(Duration::from_secs(5))
                .build()?,
        })
    }
    async fn call(&self, method: &str, params: Value) -> Result<(Value, Vec<u8>, PathBuf)> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        let request = json!({"jsonrpc":"2.0", "id":sequence, "method":method, "params":params});
        let stem = format!("{sequence:06}-{method}");
        write_new(
            &self.directory.join(format!("{stem}.request.json")),
            &json_bytes(&request)?,
        )?;
        let mut response = self
            .client
            .post(&self.endpoint)
            .header("Accept-Encoding", "identity")
            .json(&request)
            .send()
            .await?;
        let status = response.status();
        let encoding = response
            .headers()
            .get("content-encoding")
            .map(|v| v.to_str().map(str::to_owned))
            .transpose()?;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                body.len().saturating_add(chunk.len()) <= MAX_JSON as usize,
                "RPC response exceeds bound"
            );
            body.extend_from_slice(&chunk);
        }
        let path = self.directory.join(format!("{stem}.response.json"));
        write_new(&path, &body)?;
        ensure!(
            status == reqwest::StatusCode::OK
                && encoding.as_deref().is_none_or(|v| v == "identity"),
            "RPC status/encoding differs: {status}"
        );
        let value = strict_json(&body)?;
        ensure!(
            value["jsonrpc"] == "2.0" && value["id"].as_u64() == Some(sequence),
            "RPC envelope mismatch"
        );
        if let Some(error) = value.get("error") {
            bail!("{method} RPC error: {error}");
        }
        Ok((
            value
                .get("result")
                .context("RPC reply lacks result")?
                .clone(),
            body,
            path,
        ))
    }
    async fn result(&self, method: &str, params: Value) -> Result<Value> {
        Ok(self.call(method, params).await?.0)
    }
    async fn identity(&self) -> Result<Value> {
        let genesis = self.result("getGenesisHash", json!([])).await?;
        let node = self.result("getIdentity", json!([])).await?;
        ensure!(
            genesis.is_string() && node["identity"].is_string(),
            "invalid validator identity reply"
        );
        Ok(json!({"genesis_hash":genesis, "node_identity":node["identity"]}))
    }
}

#[derive(Debug, Clone, Serialize)]
struct ProcessSample {
    pid: u32,
    start_ticks: u64,
    user_cpu_seconds: f64,
    system_cpu_seconds: f64,
    rss_bytes: u64,
    threads: u64,
}
fn process_sample(pid: u32) -> Result<ProcessSample> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let tail = stat.rsplit_once(") ").context("malformed process stat")?.1;
    let fields = tail.split_whitespace().collect::<Vec<_>>();
    ensure!(fields.len() >= 22, "short process stat");
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let threads = status
        .lines()
        .find_map(|line| line.strip_prefix("Threads:"))
        .context("missing process thread count")?
        .trim()
        .parse::<u64>()?;
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    ensure!(hz > 0 && page > 0, "invalid process accounting units");
    Ok(ProcessSample {
        pid,
        start_ticks: fields[19].parse()?,
        user_cpu_seconds: fields[11].parse::<u64>()? as f64 / hz as f64,
        system_cpu_seconds: fields[12].parse::<u64>()? as f64 / hz as f64,
        rss_bytes: fields[21]
            .parse::<u64>()?
            .checked_mul(page as u64)
            .context("RSS overflow")?,
        threads,
    })
}
fn process_delta(before: &ProcessSample, after: &ProcessSample) -> Result<Value> {
    ensure!(
        before.pid == after.pid && before.start_ticks == after.start_ticks,
        "validator PID changed or was reused"
    );
    ensure!(
        after.user_cpu_seconds >= before.user_cpu_seconds
            && after.system_cpu_seconds >= before.system_cpu_seconds,
        "validator CPU counters regressed"
    );
    Ok(
        json!({"user_cpu_seconds":after.user_cpu_seconds-before.user_cpu_seconds, "system_cpu_seconds":after.system_cpu_seconds-before.system_cpu_seconds,
        "rss_before_bytes":before.rss_bytes, "rss_after_bytes":after.rss_bytes, "threads_before":before.threads, "threads_after":after.threads, "memory_scope":"Validator process RSS; excludes filesystem page cache and cgroup MemoryCurrent."}),
    )
}
fn self_usage() -> Result<libc::rusage> {
    let mut usage = unsafe { std::mem::zeroed() };
    ensure!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } == 0,
        "getrusage failed"
    );
    Ok(usage)
}
fn cpu(time: libc::timeval) -> f64 {
    time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0
}
fn free_bytes(path: &Path) -> Result<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
        let mut stat = unsafe { std::mem::zeroed::<libc::statvfs>() };
        ensure!(
            unsafe { libc::statvfs(path.as_ptr(), &mut stat) } == 0,
            "statvfs failed"
        );
        return (stat.f_bavail as u64)
            .checked_mul(stat.f_frsize as u64)
            .context("free byte overflow");
    }
    #[cfg(not(unix))]
    bail!("Linux validator host required");
}
async fn health_sample(rpc: &Rpc, pid: u32) -> Value {
    let mut sample = json!({"unix_time_ns":now_ns().unwrap_or(0)});
    match process_sample(pid) {
        Ok(p) => sample["validator_process"] = json!(p),
        Err(e) => sample["process_error"] = json!(e.to_string()),
    }
    for (label, method, params) in [
        ("health", "getHealth", json!([])),
        (
            "processed_slot",
            "getSlot",
            json!([{"commitment":"processed"}]),
        ),
        (
            "finalized_slot",
            "getSlot",
            json!([{"commitment":"finalized"}]),
        ),
        ("max_shred_insert_slot", "getMaxShredInsertSlot", json!([])),
    ] {
        match rpc.result(method, params).await {
            Ok(value) => sample[label] = value,
            Err(e) => sample[format!("{label}_error")] = json!(e.to_string()),
        }
    }
    if let (Some(p), Some(f)) = (
        sample["processed_slot"].as_u64(),
        sample["finalized_slot"].as_u64(),
    ) {
        sample["processed_minus_finalized_slots"] = json!(p.saturating_sub(f));
    }
    if let (Some(p), Some(s)) = (
        sample["processed_slot"].as_u64(),
        sample["max_shred_insert_slot"].as_u64(),
    ) {
        sample["local_replay_backlog_slots"] = json!(s.saturating_sub(p));
    }
    sample
}
async fn health_monitor(
    rpc: Rpc,
    pid: u32,
    path: PathBuf,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<Value> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut samples = 0u64;
    let mut max_rss = 0;
    let mut max_backlog = 0;
    let mut max_threads = 0;
    loop {
        if *stop.borrow() {
            break;
        }
        let sample = health_sample(&rpc, pid).await;
        serde_json::to_writer(&mut file, &sample)?;
        file.write_all(b"\n")?;
        file.flush()?;
        samples += 1;
        max_rss = max_rss.max(
            sample["validator_process"]["rss_bytes"]
                .as_u64()
                .unwrap_or(0),
        );
        max_backlog = max_backlog.max(sample["local_replay_backlog_slots"].as_u64().unwrap_or(0));
        max_threads = max_threads.max(sample["validator_process"]["threads"].as_u64().unwrap_or(0));
        tokio::select! { _ = tokio::time::sleep(Duration::from_secs(5)) => {}, changed = stop.changed() => { if changed.is_err() { break; } } }
    }
    file.sync_all()?;
    Ok(
        json!({"samples":samples, "validator_sampled_peak_rss_bytes":max_rss, "maximum_local_replay_backlog_slots":max_backlog, "validator_sampled_peak_threads":max_threads,
        "lag_scope":"Local replay backlog and commitment distance are not cluster distance. Exact getHealth replies and any numSlotsBehind field are retained under rpc/."}),
    )
}

struct ChildGuard {
    child: Child,
    live: bool,
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.live {
            unsafe {
                libc::kill(-(self.child.id() as i32), libc::SIGKILL);
            }
            let mut status = 0;
            loop {
                let result = unsafe { libc::waitpid(self.child.id() as i32, &mut status, 0) };
                if result >= 0
                    || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR)
                {
                    break;
                }
            }
        }
    }
}
async fn run_stage(
    name: &str,
    argv: &[String],
    root: &Path,
    timeout: Duration,
    validator_pid: u32,
) -> Result<Value> {
    ensure!(!argv.is_empty(), "empty child command");
    let before = process_sample(validator_pid)?;
    let started = Instant::now();
    let stdout = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join(format!("{name}.stdout")))?;
    let stderr = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join(format!("{name}.stderr")))?;
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = ChildGuard {
        child: command.spawn()?,
        live: true,
    };
    let mut status = 0;
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
    let mut sampled_peak_threads = 0;
    let mut sampled_peak_rss = 0;
    loop {
        let result = unsafe {
            libc::wait4(
                child.child.id() as i32,
                &mut status,
                libc::WNOHANG,
                &mut usage,
            )
        };
        if result > 0 {
            child.live = false;
            break;
        }
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(error.into());
        }
        if let Ok(sample) = process_sample(child.child.id()) {
            sampled_peak_threads = sampled_peak_threads.max(sample.threads);
            sampled_peak_rss = sampled_peak_rss.max(sample.rss_bytes);
        }
        ensure!(
            started.elapsed() < timeout,
            "{name} exceeded {} seconds",
            timeout.as_secs_f64()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let exit_code = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else {
        -libc::WTERMSIG(status)
    };
    let elapsed = started.elapsed().as_secs_f64();
    let mut validator = process_delta(&before, &process_sample(validator_pid)?)?;
    validator["average_cpu_cores"] = json!(
        (validator["user_cpu_seconds"].as_f64().unwrap()
            + validator["system_cpu_seconds"].as_f64().unwrap())
            / elapsed
    );
    Ok(
        json!({"argv":argv, "exit_code":exit_code, "elapsed_seconds":elapsed,
        "process_average_cpu_cores":(cpu(usage.ru_utime)+cpu(usage.ru_stime))/elapsed,
        "process_sampled_peak_threads":sampled_peak_threads, "process_sampled_peak_rss_bytes":sampled_peak_rss,
        "process_user_cpu_seconds":cpu(usage.ru_utime), "process_system_cpu_seconds":cpu(usage.ru_stime),
        "process_peak_rss_bytes":(usage.ru_maxrss as u64).saturating_mul(1024), "validator":validator}),
    )
}
async fn stage(
    report: &mut Value,
    root: &Path,
    name: &str,
    argv: Vec<String>,
    timeout: u64,
    pid: u32,
) -> Result<()> {
    report["active_stage"] = json!(name);
    save_report(root, report)?;
    let result = run_stage(name, &argv, root, Duration::from_secs(timeout), pid).await?;
    let exit = result["exit_code"].as_i64();
    report["stages"][name] = result;
    save_report(root, report)?;
    ensure!(exit == Some(0), "{name} failed; see retained stdout/stderr");
    report.as_object_mut().unwrap().remove("active_stage");
    Ok(())
}

fn select_block_range(inventory: &Value, expected_count: usize) -> Result<(u64, u64, Vec<u64>)> {
    ensure!(
        inventory["schema"] == "hivezilla-current-block-inventory-v1",
        "wrong block inventory schema"
    );
    let blocks = inventory["blocks"]
        .as_array()
        .context("missing block inventory")?;
    ensure!(
        !blocks.is_empty() && blocks.len() == expected_count,
        "wrong captured block count"
    );
    let mut slots = Vec::new();
    let mut sequences = BTreeSet::new();
    for block in blocks {
        let slot = integer(&block["slot"], "slot")?;
        ensure!(slot > 0, "zero captured slot");
        slots.push(slot);
        integer(&block["bank_id"], "bank_id")?;
        integer(&block["executed_transaction_count"], "transaction count")?;
        ensure!(
            integer(&block["entries_count"], "entry count")? > 0,
            "no captured entries"
        );
        ensure!(
            sequences.insert(integer(&block["source_sequence"], "source sequence")?),
            "duplicate source sequence"
        );
        ensure!(
            valid_digest(block["source_raw_sha256"].as_str().unwrap_or("")),
            "invalid source digest"
        );
    }
    slots.sort_unstable();
    ensure!(
        slots.windows(2).all(|p| p[0] < p[1]),
        "duplicate captured slots"
    );
    let last = *slots.last().unwrap();
    ensure!(
        slots[0] / EPOCH_SLOTS == last / EPOCH_SLOTS,
        "capture crosses epoch; use a fresh run"
    );
    Ok((
        slots[0],
        last.checked_add(1).context("slot overflow")?,
        slots,
    ))
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn coverage_from_reply(
    body: &[u8],
    slots: &[u64],
    finalized: u64,
    node: &Value,
    source_hash: &str,
) -> Result<FinalizedRpcCoverageV1> {
    ensure!(
        !slots.is_empty() && slots[0] > 0 && slots.windows(2).all(|p| p[0] < p[1]),
        "invalid selected slot list"
    );
    let value = strict_json(body)?;
    ensure!(
        value["jsonrpc"] == "2.0" && value.get("error").is_none(),
        "invalid coverage RPC envelope"
    );
    let produced: Vec<u64> = serde_json::from_value(
        value
            .get("result")
            .context("missing coverage result")?
            .clone(),
    )?;
    ensure!(
        produced == slots && finalized >= *slots.last().unwrap(),
        "finalized RPC slots differ or end is not finalized"
    );
    ensure!(valid_digest(source_hash), "invalid seal digest");
    Ok(FinalizedRpcCoverageV1 {
        schema: "hivezilla-finalized-rpc-coverage-v1".into(),
        genesis_hash: node["genesis_hash"]
            .as_str()
            .context("missing genesis")?
            .into(),
        node_identity: node["node_identity"]
            .as_str()
            .context("missing identity")?
            .into(),
        commitment: "finalized".into(),
        start_slot: slots[0],
        end_slot_exclusive: slots
            .last()
            .unwrap()
            .checked_add(1)
            .context("slot overflow")?,
        finalized_slot: finalized,
        produced_slots: produced,
        rpc_response_sha256: digest(body),
        source_receipt_sha256: source_hash.into(),
    })
}
fn safe_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
fn verify_canary(root: &Path, slots: &[u64]) -> Result<Value> {
    let marker = read_json(&root.join(EXECUTION_CANARY_FILE))?;
    ensure!(
        marker["schema"] == "blockzilla-live-execution-canary-v1"
            && marker["source_profile"] == EXECUTION_SOURCE_PROFILE
            && marker["state"] == "COMPLETE"
            && marker["canonical"] == false
            && marker["publication_ready"] == false
            && marker["archive_verified"] == true
            && marker["finalized"] == true,
        "wrong canary authority or state"
    );
    let observed = marker["produced_blocks"]
        .as_array()
        .context("missing produced blocks")?
        .iter()
        .map(|r| integer(&r["slot"], "produced slot"))
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        observed == slots
            && marker["blocks"].as_u64() == Some(slots.len() as u64)
            && marker["start_slot"].as_u64() == slots.first().copied()
            && marker["end_slot_exclusive"].as_u64() == slots.last().and_then(|v| v.checked_add(1)),
        "canary range differs from capture"
    );
    let records = marker["files"].as_array().context("missing canary files")?;
    ensure!(
        !records.is_empty() && records.len() <= 64,
        "bad canary file count"
    );
    let mut names = BTreeSet::new();
    let mut bytes = 0u64;
    for record in records {
        let name = record["path"].as_str().context("missing filename")?;
        ensure!(
            safe_name(name) && name != EXECUTION_CANARY_FILE && names.insert(name.to_string()),
            "unsafe or duplicate canary filename"
        );
        let size = integer(&record["bytes"], "file bytes")?;
        ensure!(size <= MAX_FILE, "canary object exceeds 512 MiB");
        let actual = file_hash(&root.join(name))?;
        ensure!(
            actual.bytes == size && Some(actual.sha256.as_str()) == record["sha256"].as_str(),
            "canary file hash differs"
        );
        bytes += size;
    }
    names.insert(EXECUTION_CANARY_FILE.into());
    let actual = fs::read_dir(root)?
        .map(|r| r.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<BTreeSet<_>>>()?;
    ensure!(actual == names, "unlisted canary output");
    ensure!(
        marker["source_receipt_sha256"].as_str()
            == Some(
                file_hash(&root.join(NORMALIZED_EXECUTION_FILE))?
                    .sha256
                    .as_str()
            ),
        "normalization receipt hash differs"
    );
    Ok(
        json!({"files":records.len(), "payload_bytes_including_retained_evidence":bytes, "storage":storage_metrics(root,&marker,slots)?,
        "transactions":marker["transactions"], "signatures":marker["signatures"], "entries":marker["entries"], "marker":file_hash(&root.join(EXECUTION_CANARY_FILE))?}),
    )
}
fn storage_metrics(root: &Path, marker: &Value, slots: &[u64]) -> Result<Value> {
    let index = read_archive_v2_hot_block_index(&root.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    ensure!(
        index.flags == 0 && index.rows.len() == slots.len(),
        "unsupported hot index"
    );
    let mut compressed = 0u64;
    let mut uncompressed = 0u64;
    let mut transactions = 0u64;
    let mut signatures = 0u64;
    for (position, (row, slot)) in index.rows.iter().zip(slots).enumerate() {
        ensure!(
            row.block_id as usize == position
                && row.slot == *slot
                && row.compressed_offset == compressed
                && row.first_tx_ordinal == transactions
                && row.first_signature_ordinal == signatures,
            "hot index geometry differs"
        );
        compressed += u64::from(row.compressed_len);
        uncompressed += u64::from(row.uncompressed_len);
        transactions += u64::from(row.tx_count);
        signatures += u64::from(row.signature_count);
    }
    let mut sizes = std::collections::BTreeMap::new();
    for record in marker["files"].as_array().context("missing files")? {
        sizes.insert(
            record["path"].as_str().context("missing path")?,
            integer(&record["bytes"], "bytes")?,
        );
    }
    ensure!(
        compressed == index.blob_file_bytes
            && sizes.get(ARCHIVE_V2_BLOCKS_FILE) == Some(&compressed)
            && marker["transactions"].as_u64() == Some(transactions)
            && marker["signatures"].as_u64() == Some(signatures),
        "hot counts differ"
    );
    let mut evidence = BTreeSet::new();
    let mut retained = *sizes
        .get(NORMALIZED_EXECUTION_FILE)
        .context("missing normalized receipt")?;
    for value in marker["evidence_files"]
        .as_array()
        .context("missing evidence files")?
    {
        let name = value.as_str().context("invalid evidence filename")?;
        ensure!(
            evidence.insert(name)
                && ![
                    NORMALIZED_EXECUTION_FILE,
                    ARCHIVE_V2_BLOCKS_FILE,
                    ARCHIVE_V2_BLOCK_INDEX_FILE
                ]
                .contains(&name),
            "duplicate or invalid evidence filename"
        );
        retained += sizes.get(name).context("unlisted evidence file")?;
    }
    let total: u64 = sizes.values().sum();
    let compact = total
        .checked_sub(retained)
        .context("bad evidence byte total")?;
    Ok(
        json!({"hot_frames_compressed_bytes":compressed,"hot_frames_uncompressed_payload_bytes":uncompressed,
        "hot_frames_compressed_over_uncompressed_ratio":if uncompressed > 0 { Some(compressed as f64/uncompressed as f64) } else { None },
        "compact_sidecars_bytes":compact.checked_sub(compressed).context("bad compact byte total")?, "compact_format_total_bytes":compact,
        "retained_raw_and_evidence_bytes":retained,"authoritative_compressed_raw_wal_bytes":sizes.get("events.wal"),
        "scope":"Hot-frame ratio compares the same hot payload before and after zstd. Raw envelopes/evidence remain additional files; no total-payload/raw ratio is claimed."}),
    )
}

async fn execute(
    args: &RunLocalExecutionCanaryArgs,
    rpc: &Rpc,
    hivezilla: &Path,
    blockzilla: &Path,
    report: &mut Value,
) -> Result<()> {
    ensure!(
        rpc.result("getHealth", json!([])).await? == "ok",
        "validator is not healthy"
    );
    let node = rpc.identity().await?;
    ensure!(
        node == json!({"genesis_hash":args.expected_genesis_hash,"node_identity":args.expected_node_identity}),
        "validator identity differs from explicit expected identity"
    );
    report["validator_identity"] = node.clone();
    let root = &args.run_dir;
    let path = |p: &Path| p.to_string_lossy().into_owned();
    let cmd = vec![
        path(hivezilla),
        "record-grpc-current".into(),
        "--endpoint".into(),
        args.grpc_endpoint.clone(),
        "--validator-rpc-endpoint".into(),
        args.rpc_endpoint.clone(),
        "--output-dir".into(),
        path(&root.join("capture")),
        "--max-blocks".into(),
        args.blocks.to_string(),
        "--timeout-secs".into(),
        args.capture_timeout_secs.to_string(),
        "--idle-timeout-secs".into(),
        args.capture_timeout_secs.min(60).to_string(),
        "--commitment".into(),
        "finalized".into(),
        "--max-record-bytes".into(),
        (128u64 << 20).to_string(),
        "--max-capture-bytes".into(),
        MAX_FILE.to_string(),
        "--min-free-bytes".into(),
        args.min_free_bytes.to_string(),
        "--cluster-id".into(),
        args.expected_genesis_hash.clone(),
        "--origin-node-id".into(),
        args.expected_node_identity.clone(),
        "--source-id".into(),
        "self-operated-yellowstone16".into(),
    ];
    stage(
        report,
        root,
        "capture",
        cmd,
        args.capture_timeout_secs + 60,
        args.validator_pid,
    )
    .await?;
    stage(
        report,
        root,
        "inspect",
        vec![
            path(hivezilla),
            "inspect-grpc-current".into(),
            "--input".into(),
            path(&root.join("capture")),
            "--max-record-bytes".into(),
            (128u64 << 20).to_string(),
        ],
        args.stage_timeout_secs,
        args.validator_pid,
    )
    .await?;
    let inventory = read_json(&root.join("inspect.stdout"))?;
    let (start, end, slots) = select_block_range(&inventory, args.blocks)?;
    let seal = read_json(&root.join("capture").join(SEAL))?;
    ensure!(
        seal["state"] == "SEALED"
            && seal["source_profile"] == EXECUTION_SOURCE_PROFILE
            && seal["requested_commitment"] == "finalized"
            && seal["counts"]["blocks"].as_u64() == Some(args.blocks as u64),
        "capture seal differs"
    );
    ensure!(
        seal["generation_id"].is_string() && inventory["generation_id"] == seal["generation_id"],
        "inspected generation differs"
    );
    ensure!(
        seal["validator_identity"]["genesis_hash"] == node["genesis_hash"]
            && seal["validator_identity"]["node_identity"] == node["node_identity"]
            && rpc.identity().await? == node,
        "capture/current validator identity changed"
    );
    let finalized = integer(
        &rpc.result("getSlot", json!([{"commitment":"finalized"}]))
            .await?,
        "finalized slot",
    )?;
    let (_, body, reply_path) = rpc
        .call("getBlocks", json!([start,end-1,{"commitment":"finalized"}]))
        .await?;
    let coverage = coverage_from_reply(
        &body,
        &slots,
        finalized,
        &node,
        &file_hash(&root.join("capture").join(SEAL))?.sha256,
    )?;
    write_new(&root.join("coverage/rpc-get-blocks.json"), &body)?;
    write_new(
        &root.join("coverage/coverage.json"),
        &json_bytes(&coverage)?,
    )?;
    report["coverage"] = serde_json::to_value(&coverage)?;
    report["coverage"]["retained_rpc_reply"] = json!(reply_path);
    report["raw_capture"] = json!(file_hash(&root.join("capture/events.wal"))?);
    stage(
        report,
        root,
        "normalize",
        vec![
            path(hivezilla),
            "normalize-current-execution".into(),
            "--input".into(),
            path(&root.join("capture")),
            "--coverage".into(),
            path(&root.join("coverage/coverage.json")),
            "--output".into(),
            path(&root.join("normalized")),
            "--max-blocks".into(),
            args.blocks.to_string(),
            "--max-record-bytes".into(),
            (128u64 << 20).to_string(),
        ],
        args.stage_timeout_secs,
        args.validator_pid,
    )
    .await?;
    stage(
        report,
        root,
        "compact",
        vec![
            path(blockzilla),
            "build-live-execution-canary".into(),
            path(&root.join("normalized")),
            path(&root.join("compact")),
            "--level".into(),
            "1".into(),
        ],
        args.stage_timeout_secs,
        args.validator_pid,
    )
    .await?;
    let check = Instant::now();
    report["canary"] = verify_canary(&root.join("compact"), &slots)?;
    report["controller_output_hash_check_seconds"] = json!(check.elapsed().as_secs_f64());
    ensure!(
        rpc.identity().await? == node,
        "validator identity changed after compaction"
    );
    for (name, binary) in [("hivezilla", hivezilla), ("blockzilla", blockzilla)] {
        ensure!(
            json!(file_hash(binary)?) == report["binaries"][name]["identity"],
            "binary changed during run"
        );
    }
    Ok(())
}

pub async fn run(mut args: RunLocalExecutionCanaryArgs) -> Result<Value> {
    ensure!(
        cfg!(target_os = "linux"),
        "run this controller on the Linux validator host"
    );
    ensure!(
        (1..=128).contains(&args.blocks)
            && (1..=1800).contains(&args.capture_timeout_secs)
            && (1..=1800).contains(&args.stage_timeout_secs)
            && args.min_free_bytes >= 2u64 << 30,
        "invalid finite bounds"
    );
    args.rpc_endpoint = loopback_url(&args.rpc_endpoint)?;
    args.grpc_endpoint = loopback_url(&args.grpc_endpoint)?;
    let root = &args.run_dir;
    ensure!(
        root.is_absolute() && !root.try_exists()? && fs::symlink_metadata(root).is_err(),
        "run directory must be new and absolute"
    );
    let parent = root.parent().context("run directory has no parent")?;
    ensure!(
        parent.is_dir() && free_bytes(parent)? >= args.min_free_bytes,
        "missing parent or insufficient free disk space"
    );
    let hivezilla = fs::canonicalize(args.hivezilla.clone().unwrap_or(std::env::current_exe()?))?;
    let blockzilla = fs::canonicalize(&args.blockzilla)?;
    let before = process_sample(args.validator_pid)?;
    let controller_before = self_usage()?;
    let validator_exe = fs::read_link(format!("/proc/{}/exe", args.validator_pid))?;
    ensure!(
        validator_exe
            .file_name()
            .is_some_and(|name| name == "agave-validator"),
        "PID executable is not agave-validator"
    );
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(root)?;
    fs::create_dir(root.join("rpc"))?;
    fs::create_dir(root.join("coverage"))?;
    let rpc = Rpc::new(&args.rpc_endpoint, root.join("rpc"))?;
    let mut report = json!({"schema":"hivezilla-local-execution-canary-run-v1","state":"RUNNING","run_dir":root,"started_unix_ns":now_ns()?,"blocks_requested":args.blocks,"stages":{},
        "canonical":false,"upload_attempted":false,"validator_pid":args.validator_pid,"validator_executable":validator_exe,"rpc_endpoint":args.rpc_endpoint,"grpc_endpoint":args.grpc_endpoint,
        "binaries":{"hivezilla":{"path":hivezilla,"identity":file_hash(&hivezilla)?},"blockzilla":{"path":blockzilla,"identity":file_hash(&blockzilla)?}},
        "measurement_scope":"Stage command wall time includes capture waiting for live blocks. Child CPU/RSS and validator CPU are separate. No speed comparison is implied."});
    save_report(root, &report)?;
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut monitor = tokio::spawn(health_monitor(
        rpc.clone(),
        args.validator_pid,
        root.join("validator-health.jsonl"),
        stop_rx,
    ));
    let outcome: Result<()> = {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! {
                result = execute(&args,&rpc,&hivezilla,&blockzilla,&mut report) => result,
                result = tokio::signal::ctrl_c() => Err(anyhow::anyhow!("interrupted by SIGINT or signal setup error: {result:?}")),
                _ = terminate.recv() => Err(anyhow::anyhow!("interrupted by SIGTERM")),
            }
        }
        #[cfg(not(unix))]
        {
            execute(&args, &rpc, &hivezilla, &blockzilla, &mut report).await
        }
    };
    let mut error = outcome.err().map(|e| format!("{e:#}"));
    let _ = stop_tx.send(true);
    match tokio::time::timeout(Duration::from_secs(25), &mut monitor).await {
        Ok(Ok(Ok(value))) => report["health_monitor"] = value,
        other => {
            monitor.abort();
            report["resource_error"] = json!(format!("health monitor failed: {other:?}"));
            error.get_or_insert("health monitor did not complete".into());
        }
    }
    match process_sample(args.validator_pid).and_then(|after| process_delta(&before, &after)) {
        Ok(value) => report["validator_total"] = value,
        Err(e) => {
            report["resource_error"] = json!(e.to_string());
            error.get_or_insert(e.to_string());
        }
    }
    let controller_after = self_usage()?;
    report["controller_resources"] = json!({"user_cpu_seconds":cpu(controller_after.ru_utime)-cpu(controller_before.ru_utime),"system_cpu_seconds":cpu(controller_after.ru_stime)-cpu(controller_before.ru_stime),"peak_rss_bytes":(controller_after.ru_maxrss as u64).saturating_mul(1024),"scope":"Controller including RPC monitor and hashes; child CPU is separate."});
    report["ended_unix_ns"] = json!(now_ns()?);
    report["state"] = json!(if error.is_some() {
        "FAILED"
    } else {
        "COMPLETE_LOCAL_CANARY"
    });
    if let Some(message) = &error {
        report["error"] = json!(message);
    }
    save_report(root, &report)?;
    if let Some(message) = error {
        bail!(
            "{message}; report: {}",
            root.join("run-report.json").display()
        );
    }
    Ok(report)
}

#[cfg(test)]
#[path = "validator_canary_run/tests.rs"]
mod tests;
