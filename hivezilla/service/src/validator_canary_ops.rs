//! Finite validator operations and readiness. No upload or canonical authority.
use anyhow::{Context, Result, ensure};
use clap::{Args, Subcommand};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    ffi::CString,
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt},
            process::CommandExt,
        },
    },
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    process::Command,
    time::{sleep, timeout},
};

const UNIT: &str = "hivezilla-validator-canary.service";
const FREE: u64 = 500 << 30;
const JSON_LIMIT: u64 = 16 << 20;
const WAIT_LIMIT: Duration = Duration::from_secs(14400);
const UDP_MAX: &str = "134217728";
const KERNEL_KEYS: [&str; 2] = ["net.core.rmem_max", "net.core.wmem_max"];

#[derive(Debug, Args)]
pub struct ValidatorCanaryArgs {
    #[command(subcommand)]
    pub action: ValidatorCanaryAction,
}

#[derive(Debug, Subcommand)]
pub enum ValidatorCanaryAction {
    /// Check and start a stopped finite non-voting validator. Never restart.
    Start {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        check_only: bool,
    },
    /// Stop only the matching prepared validator. Keep all data and kernel limits.
    Stop {
        #[arg(long)]
        root: PathBuf,
    },
    /// Read public loopback RPC facts and selected service counters.
    Status {
        #[arg(long)]
        root: PathBuf,
    },
    /// Wait at most four hours, then execute one finite local 32-block canary.
    Execute(ExecuteArgs),
}

#[derive(Debug, Args)]
pub struct ExecuteArgs {
    #[arg(long)]
    pub root: PathBuf,
    #[arg(long)]
    pub hivezilla: PathBuf,
    #[arg(long)]
    pub blockzilla: PathBuf,
    #[arg(long)]
    pub validator_pid: u32,
    #[arg(long)]
    pub expected_node_identity: String,
    #[arg(long)]
    pub expected_genesis_hash: String,
    /// Same exact five test/build gates; this is not a bypass.
    #[arg(long)]
    pub gate_report: Option<PathBuf>,
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn token() -> Result<String> {
    let mut bytes = [0; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(hex(&bytes))
}
fn uid() -> u32 {
    unsafe { libc::geteuid() }
}
fn public_key(value: &str) -> bool {
    (32..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(&b))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Identity {
    dev: u64,
    ino: u64,
    bytes: u64,
    mtime: i64,
    mtime_ns: i64,
    ctime: i64,
    ctime_ns: i64,
}
fn file_identity(path: &Path) -> Result<Identity> {
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.is_file() && meta.uid() == uid() && meta.mode() & 0o022 == 0,
        "input must be an owned regular file without shared write access"
    );
    Ok(Identity {
        dev: meta.dev(),
        ino: meta.ino(),
        bytes: meta.len(),
        mtime: meta.mtime(),
        mtime_ns: meta.mtime_nsec(),
        ctime: meta.ctime(),
        ctime_ns: meta.ctime_nsec(),
    })
}
fn directory(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.is_dir()
            && !meta.file_type().is_symlink()
            && meta.uid() == uid()
            && meta.mode() & 0o022 == 0,
        "directory must be owned without shared write access"
    );
    Ok(())
}
fn root_dir(root: &Path) -> Result<()> {
    ensure!(
        cfg!(target_os = "linux"),
        "validator operations require Linux"
    );
    ensure!(
        root.is_absolute() && fs::canonicalize(root)? == root,
        "root must be an absolute resolved path"
    );
    directory(root)?;
    directory(&root.join("reports"))?;
    Ok(())
}
fn hash_file(path: &Path) -> Result<String> {
    let before = file_identity(path)?;
    let mut input = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 128 * 1024];
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    ensure!(
        file_identity(path)? == before,
        "input changed while hashing"
    );
    Ok(hex(&hash.finalize()))
}

struct Strict(Value);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Strict;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("JSON without duplicate keys")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Strict, E> {
                Ok(Strict(json!(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Strict, E> {
                Ok(Strict(json!(v)))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Strict, E> {
                Ok(Strict(json!(v)))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Strict, E> {
                if !v.is_finite() {
                    return Err(E::custom("non-finite number"));
                }
                Ok(Strict(json!(v)))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Strict, E> {
                Ok(Strict(json!(v)))
            }
            fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Strict, E> {
                Ok(Strict(json!(v)))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_none<E: de::Error>(self) -> std::result::Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Strict, A::Error> {
                let mut v = vec![];
                while let Some(Strict(x)) = a.next_element()? {
                    v.push(x);
                }
                Ok(Strict(Value::Array(v)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Strict, A::Error> {
                let mut v = serde_json::Map::new();
                while let Some(k) = a.next_key::<String>()? {
                    if v.contains_key(&k) {
                        return Err(de::Error::custom("duplicate key"));
                    }
                    let Strict(x) = a.next_value()?;
                    v.insert(k, x);
                }
                Ok(Strict(Value::Object(v)))
            }
        }
        d.deserialize_any(V)
    }
}
fn parse_json(bytes: &[u8]) -> Result<Value> {
    Ok(serde_json::from_slice::<Strict>(bytes)?.0)
}
fn read_json(path: &Path) -> Result<Value> {
    let before = file_identity(path)?;
    ensure!(before.bytes <= JSON_LIMIT, "JSON exceeds limit");
    let bytes = fs::read(path)?;
    ensure!(file_identity(path)? == before, "JSON changed while reading");
    parse_json(&bytes)
}
fn save(path: &Path, value: &Value, replace: bool) -> Result<()> {
    let parent = path.parent().context("record has no parent")?;
    directory(parent)?;
    if replace && path.exists() {
        file_identity(path)?;
    }
    let temp = parent.join(format!(".record-{}.tmp", token()?));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    if replace {
        fs::rename(&temp, path)?;
    } else {
        fs::hard_link(&temp, path)?;
        fs::remove_file(&temp)?;
    }
    File::open(parent)?.sync_all()?;
    Ok(())
}
fn lock(root: &Path, name: &str) -> Result<File> {
    let path = root.join("reports").join(name);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)?;
    file_identity(&path)?;
    ensure!(
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "operation already running"
    );
    Ok(file)
}

async fn command(program: &str, args: &[String], seconds: u64) -> Result<std::process::Output> {
    let mut cmd = Command::new(program);
    cmd.args(args).stdin(Stdio::null()).kill_on_drop(true);
    let output = timeout(Duration::from_secs(seconds), cmd.output())
        .await
        .context("command timed out")??;
    ensure!(
        output.stdout.len() <= JSON_LIMIT as usize && output.stderr.len() <= JSON_LIMIT as usize,
        "command output exceeds limit"
    );
    Ok(output)
}
async fn checked(program: &str, args: &[String], seconds: u64) -> Result<Vec<u8>> {
    let out = command(program, args, seconds).await?;
    ensure!(out.status.success(), "bounded system command failed");
    Ok(out.stdout)
}
fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|x| (*x).into()).collect()
}
async fn service() -> Result<Value> {
    let fields = [
        "LoadState",
        "ActiveState",
        "SubState",
        "MainPID",
        "ExecMainStatus",
        "Result",
        "InvocationID",
        "User",
        "WorkingDirectory",
        "MemoryCurrent",
        "MemoryPeak",
        "CPUUsageNSec",
    ];
    let mut args = strings(&["show", UNIT, "--no-pager"]);
    args.extend(fields.iter().map(|f| format!("--property={f}")));
    let out = command("systemctl", &args, 5).await?;
    let mut result = serde_json::Map::new();
    for line in std::str::from_utf8(&out.stdout)?.lines() {
        if let Some((k, v)) = line.split_once('=') {
            if fields.contains(&k) {
                result.insert(k.into(), json!(v));
            }
        }
    }
    ensure!(!result.is_empty(), "cannot read service state");
    Ok(Value::Object(result))
}
fn active(s: &Value) -> bool {
    matches!(
        s["ActiveState"].as_str(),
        Some("active" | "activating" | "deactivating" | "reloading")
    )
}
async fn process_identity(pid: u32) -> Result<Value> {
    let s = service().await?;
    ensure!(
        s["ActiveState"] == "active" && s["MainPID"] == pid.to_string(),
        "validator service is inactive or PID changed"
    );
    let invocation = s["InvocationID"].as_str().context("no invocation")?;
    ensure!(
        invocation.len() == 32 && invocation.bytes().all(|x| x.is_ascii_hexdigit()),
        "invalid service invocation"
    );
    let path = PathBuf::from(format!("/proc/{pid}"));
    let stat = fs::read_to_string(path.join("stat"))?;
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .context("invalid process stat")?
        .1
        .split_whitespace()
        .collect();
    let start = fields
        .get(19)
        .context("short process stat")?
        .parse::<u64>()?;
    let exe = fs::read_link(path.join("exe"))?;
    ensure!(
        exe.file_name().and_then(|s| s.to_str()) == Some("agave-validator"),
        "PID is not agave-validator"
    );
    Ok(
        json!({"pid":pid,"start_ticks":start,"unit_invocation_id":invocation,"executable":exe,
        "boot_id":fs::read_to_string("/proc/sys/kernel/random/boot_id")?.trim()}),
    )
}
fn free_bytes(path: &Path) -> Result<u64> {
    let c = CString::new(path.as_os_str().as_bytes())?;
    let mut data = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    ensure!(
        unsafe { libc::statvfs(c.as_ptr(), data.as_mut_ptr()) } == 0,
        "cannot inspect free disk"
    );
    let data = unsafe { data.assume_init() };
    (data.f_bavail as u64)
        .checked_mul(data.f_frsize as u64)
        .context("disk byte overflow")
}
fn memory() -> Result<Value> {
    let mut v = serde_json::Map::new();
    for line in fs::read_to_string("/proc/meminfo")?.lines() {
        if let Some((k, b)) = line.split_once(':') {
            if ["MemTotal", "MemAvailable"].contains(&k) {
                v.insert(
                    k.into(),
                    json!(
                        b.split_whitespace()
                            .next()
                            .context("memory value")?
                            .parse::<u64>()?
                            * 1024
                    ),
                );
            }
        }
    }
    Ok(Value::Object(v))
}
fn validator_exists() -> Result<bool> {
    for entry in fs::read_dir("/proc")? {
        let p = entry?.path();
        if p.file_name()
            .and_then(|x| x.to_str())
            .is_some_and(|x| x.bytes().all(|b| b.is_ascii_digit()))
        {
            if let Ok(comm) = fs::read_to_string(p.join("comm")) {
                if ["agave-validator", "solana-validato", "solana-validator"].contains(&comm.trim())
                {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn http() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(3))
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .build()?)
}
async fn http_bytes(mut response: reqwest::Response) -> Result<Vec<u8>> {
    ensure!(
        response.status() == reqwest::StatusCode::OK,
        "local HTTP status differs"
    );
    ensure!(
        response
            .headers()
            .get("content-encoding")
            .is_none_or(|v| v == "identity"),
        "local response encoding differs"
    );
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= 1 << 20,
            "local response exceeds limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
async fn rpc(client: &reqwest::Client, method: &str, params: Value) -> Result<Value> {
    let response = client
        .post("http://127.0.0.1:8899")
        .header("accept-encoding", "identity")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .await?;
    let value = parse_json(&http_bytes(response).await?)?;
    ensure!(
        value["jsonrpc"] == "2.0" && value["id"] == 1 && value.get("error").is_none(),
        "RPC unavailable or invalid"
    );
    value.get("result").cloned().context("RPC result absent")
}
async fn sample(client: &reqwest::Client) -> Value {
    let health = rpc(client, "getHealth", json!([]))
        .await
        .unwrap_or(Value::Null);
    let identity = rpc(client, "getIdentity", json!([]))
        .await
        .ok()
        .and_then(|v| {
            v["identity"]
                .as_str()
                .filter(|s| public_key(s))
                .map(str::to_owned)
        });
    let genesis = rpc(client, "getGenesisHash", json!([]))
        .await
        .ok()
        .and_then(|v| v.as_str().filter(|s| public_key(s)).map(str::to_owned));
    let slot = rpc(client, "getSlot", json!([{"commitment":"finalized"}]))
        .await
        .ok()
        .and_then(|v| v.as_u64());
    json!({"health":if health=="ok"{json!("ok")}else{Value::Null},"identity":identity,"genesis_hash":genesis,"finalized_slot":slot,"unix_ns":now_ns()})
}
#[derive(Default)]
struct Readiness {
    first: Option<(Duration, u64)>,
}
impl Readiness {
    fn observe(
        &mut self,
        s: &Value,
        elapsed: Duration,
        identity: &str,
        genesis: &str,
    ) -> Result<bool> {
        ensure!(
            s["identity"].is_null() || s["identity"] == identity,
            "validator identity differs"
        );
        ensure!(
            s["genesis_hash"].is_null() || s["genesis_hash"] == genesis,
            "validator genesis differs"
        );
        let Some(slot) = s["finalized_slot"].as_u64().filter(|_| {
            s["health"] == "ok" && s["identity"] == identity && s["genesis_hash"] == genesis
        }) else {
            self.first = None;
            return Ok(false);
        };
        if let Some((at, before)) = self.first {
            ensure!(slot >= before, "finalized slot regressed");
            Ok(elapsed.saturating_sub(at) >= Duration::from_secs(5) && slot > before)
        } else {
            self.first = Some((elapsed, slot));
            Ok(false)
        }
    }
}

fn expected_commands() -> Vec<Vec<String>> {
    vec![
        strings(&[
            "cargo",
            "test",
            "--locked",
            "-p",
            "hivezilla",
            "--lib",
            "--",
            "--test-threads=1",
        ]),
        strings(&[
            "cargo",
            "test",
            "--locked",
            "-p",
            "blockzilla",
            "--bin",
            "blockzilla",
            "execution_canary",
        ]),
        strings(&["cargo", "test", "--locked", "-p", "blockzilla-live-format"]),
        strings(&["cargo", "test", "--locked", "-p", "hivezilla-object-store"]),
        strings(&[
            "cargo",
            "build",
            "--locked",
            "--release",
            "-p",
            "hivezilla",
            "--bin",
            "hivezilla",
            "-p",
            "blockzilla",
            "--bin",
            "blockzilla",
            "-p",
            "hivezilla-object-store",
            "--bin",
            "blockzilla-s3-upload",
        ]),
    ]
}
fn validate_gate(gates: &Value) -> Result<()> {
    let rows = gates.as_array().context("gate must be an array")?;
    let expected = expected_commands();
    ensure!(
        rows.len() == expected.len(),
        "all five pipeline gates are required"
    );
    for (row, args) in rows.iter().zip(expected) {
        ensure!(
            row["command"] == json!(args) && row["exit_code"].as_i64() == Some(0),
            "pipeline gate command differs or failed"
        );
        ensure!(
            row["elapsed_seconds"]
                .as_f64()
                .is_some_and(|v| v.is_finite() && v >= 0.0),
            "pipeline gate is incomplete"
        );
        let log = row["log"].as_str().context("gate log missing")?;
        ensure!(
            log.strip_prefix("check-")
                .and_then(|s| s.strip_suffix(".log"))
                .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())),
            "invalid gate log reference"
        );
    }
    Ok(())
}
struct Pinned {
    path: PathBuf,
    identity: Identity,
    sha: String,
}
impl Pinned {
    fn new(path: PathBuf, executable: bool) -> Result<Self> {
        ensure!(path.is_absolute(), "input path must be absolute");
        let identity = file_identity(&path)?;
        if executable {
            ensure!(
                fs::metadata(&path)?.mode() & 0o111 != 0,
                "binary is not executable"
            );
        }
        let sha = hash_file(&path)?;
        Ok(Self {
            path,
            identity,
            sha,
        })
    }
    fn check(&self, full: bool) -> Result<()> {
        ensure!(
            file_identity(&self.path)? == self.identity,
            "pinned input changed"
        );
        if full {
            ensure!(
                hash_file(&self.path)? == self.sha,
                "pinned input digest changed"
            );
        }
        Ok(())
    }
    fn record(&self) -> Value {
        json!({"path":self.path,"bytes":self.identity.bytes,"sha256":self.sha})
    }
}
async fn environment(args: &ExecuteArgs, process: &Value, inputs: &[Pinned]) -> Result<()> {
    ensure!(
        process_identity(args.validator_pid).await? == *process,
        "validator process or service restarted"
    );
    ensure!(
        free_bytes(&args.root.join("captures"))? >= FREE,
        "less than 500 GiB remains"
    );
    for p in inputs {
        p.check(false)?;
    }
    Ok(())
}
async fn terminate_child(child: &mut tokio::process::Child) {
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(-(pid as i32), libc::SIGTERM);
        }
        if timeout(Duration::from_secs(15), child.wait())
            .await
            .is_err()
        {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
            let _ = timeout(Duration::from_secs(10), child.wait()).await;
        }
    }
}
async fn execute(args: ExecuteArgs) -> Result<Value> {
    root_dir(&args.root)?;
    directory(&args.root.join("captures"))?;
    ensure!(
        args.validator_pid > 0
            && public_key(&args.expected_node_identity)
            && public_key(&args.expected_genesis_hash),
        "invalid explicit validator identity"
    );
    let _lock = lock(&args.root, "canary-readiness.lock")?;
    let gate_path = args
        .gate_report
        .clone()
        .unwrap_or_else(|| args.root.join("reports/pipeline-check-results.json"));
    let inputs = vec![
        Pinned::new(gate_path.clone(), false)?,
        Pinned::new(args.hivezilla.clone(), true)?,
        Pinned::new(args.blockzilla.clone(), true)?,
    ];
    ensure!(
        fs::canonicalize(std::env::current_exe()?)? == fs::canonicalize(&args.hivezilla)?,
        "execute must use this exact hivezilla executable"
    );
    let gates = read_json(&gate_path)?;
    validate_gate(&gates)?;
    let id = token()?;
    let run_dir = args
        .root
        .join("captures")
        .join(format!("run-{}-{id}", now_ns()));
    ensure!(!run_dir.exists(), "canary directory already exists");
    let path = args
        .root
        .join("reports")
        .join(format!("canary-readiness-{id}.json"));
    let started = Instant::now();
    let process = process_identity(args.validator_pid).await?;
    let mut argv = strings(&["run-local-execution-canary", "--run-dir"]);
    argv.push(run_dir.display().to_string());
    for (k, v) in [
        ("--hivezilla", args.hivezilla.display().to_string()),
        ("--blockzilla", args.blockzilla.display().to_string()),
        ("--validator-pid", args.validator_pid.to_string()),
        (
            "--expected-node-identity",
            args.expected_node_identity.clone(),
        ),
        (
            "--expected-genesis-hash",
            args.expected_genesis_hash.clone(),
        ),
        ("--blocks", "32".into()),
        ("--min-free-bytes", FREE.to_string()),
        ("--capture-timeout-secs", "300".into()),
        ("--stage-timeout-secs", "300".into()),
        ("--rpc-endpoint", "http://127.0.0.1:8899".into()),
        ("--grpc-endpoint", "http://127.0.0.1:10000".into()),
    ] {
        argv.extend([k.into(), v]);
    }
    let mut report = json!({"schema":"hivezilla-validator-canary-readiness-v1","state":"WAITING","complete":false,"started_unix_ns":now_ns(),"status_path":path,"run_dir":run_dir,"validator":process,"passed_gates":gates,"inputs":{"gate":inputs[0].record(),"hivezilla":inputs[1].record(),"blockzilla":inputs[2].record()},"controller_argv":argv,"controller_launches":0,"upload_attempted":false,"validator_service_changed":false,"wait_limit_seconds":14400,"poll_seconds":30,"samples":[]});
    save(&path, &report, false)?;
    let mut child = None;
    let work: Result<()> = {
        let body = async {
            let client = http()?;
            let mut ready = Readiness::default();
            loop {
                ensure!(
                    started.elapsed() < WAIT_LIMIT,
                    "four-hour readiness deadline reached"
                );
                let cycle = Instant::now();
                environment(&args, &process, &inputs).await?;
                let observation = sample(&client).await;
                environment(&args, &process, &inputs).await?;
                ensure!(
                    started.elapsed() < WAIT_LIMIT,
                    "four-hour readiness deadline reached"
                );
                let samples = report["samples"].as_array_mut().context("sample list")?;
                samples.push(observation.clone());
                if samples.len() > 512 {
                    samples.remove(0);
                }
                report["updated_unix_ns"] = json!(now_ns());
                if ready.observe(
                    &observation,
                    started.elapsed(),
                    &args.expected_node_identity,
                    &args.expected_genesis_hash,
                )? {
                    report["readiness"] = json!({"first_finalized_slot":ready.first.map(|x|x.1),"second_finalized_slot":observation["finalized_slot"]});
                    break;
                }
                save(&path, &report, true)?;
                sleep(
                    Duration::from_secs(30)
                        .saturating_sub(cycle.elapsed())
                        .min(WAIT_LIMIT.saturating_sub(started.elapsed())),
                )
                .await;
            }
            for p in &inputs {
                p.check(true)?;
            }
            validate_gate(&read_json(&gate_path)?)?;
            environment(&args, &process, &inputs).await?;
            ensure!(
                started.elapsed() < WAIT_LIMIT,
                "deadline before capture launch"
            );
            report["state"] = json!("LAUNCHING");
            report["controller_launches"] = json!(1);
            save(&path, &report, true)?;
            let output = args
                .root
                .join("reports")
                .join(format!("canary-controller-{id}"));
            let stdout = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(output.with_extension("stdout"))?;
            let stderr = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(output.with_extension("stderr"))?;
            let mut cmd = Command::new(&args.hivezilla);
            cmd.args(&argv)
                .stdin(Stdio::null())
                .stdout(stdout)
                .stderr(stderr)
                .kill_on_drop(true);
            cmd.as_std_mut().process_group(0);
            child = Some(cmd.spawn()?);
            let c = child.as_mut().context("controller child missing")?;
            report["state"] = json!("RUNNING_CANARY");
            report["controller_pid"] = json!(c.id());
            save(&path, &report, true)?;
            let child_started = Instant::now();
            let status = loop {
                if let Some(status) = c.try_wait()? {
                    break status;
                }
                ensure!(
                    started.elapsed() < WAIT_LIMIT
                        && child_started.elapsed() < Duration::from_secs(1800),
                    "bounded controller deadline reached"
                );
                environment(&args, &process, &inputs).await?;
                report["updated_unix_ns"] = json!(now_ns());
                save(&path, &report, true)?;
                sleep(Duration::from_secs(5)).await;
            };
            report["controller_exit_code"] = json!(status.code());
            ensure!(
                status.success(),
                "local canary controller failed; inspect retained report"
            );
            environment(&args, &process, &inputs).await?;
            for p in &inputs {
                p.check(true)?;
            }
            let child_path = run_dir.join("run-report.json");
            let done = read_json(&child_path)?;
            ensure!(
                done["schema"] == "hivezilla-local-execution-canary-run-v1"
                    && done["state"] == "COMPLETE_LOCAL_CANARY"
                    && done["upload_attempted"] == false
                    && done["canonical"] == false
                    && done["blocks_requested"] == 32
                    && done["validator_pid"] == args.validator_pid,
                "canary did not produce required complete report"
            );
            for (index, name) in [(1, "hivezilla"), (2, "blockzilla")] {
                ensure!(
                    done["binaries"][name]["sha256"] == inputs[index].sha,
                    "canary used different binary"
                );
            }
            report["state"] = json!("COMPLETE_LOCAL_CANARY");
            report["complete"] = json!(true);
            report["canary_report"] = Pinned::new(child_path, false)?.record();
            Ok(())
        };
        tokio::pin!(body);
        let mut termination =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = &mut body => result,
            _ = tokio::signal::ctrl_c() => Err(anyhow::anyhow!("readiness runner interrupted")),
            _ = termination.recv() => Err(anyhow::anyhow!("readiness runner interrupted")),
        }
    };
    if let Err(error) = work {
        if let Some(c) = child.as_mut() {
            terminate_child(c).await;
        }
        report["state"] = json!("FAILED");
        report["complete"] = json!(false);
        report["error"] = json!(error.to_string());
    }
    report["finished_unix_ns"] = json!(now_ns());
    save(&path, &report, true)?;
    ensure!(
        report["state"] != "FAILED",
        "readiness failed; inspect {}",
        path.display()
    );
    Ok(report)
}

fn prepared(root: &Path) -> Result<(Value, Vec<String>, Value)> {
    root_dir(root)?;
    for name in ["config", "ledger", "accounts", "snapshots"] {
        directory(&root.join(name))?;
    }
    let install = read_json(&root.join("reports/install.json"))?;
    ensure!(
        install["schema"] == "hivezilla-validator-canary-install-v1"
            && install["root"] == root.display().to_string(),
        "install report differs from root"
    );
    let argv: Vec<String> =
        serde_json::from_value(read_json(&root.join("config/validator-argv.json"))?)?;
    ensure!(
        !argv.is_empty()
            && argv.len() <= 100
            && argv
                .iter()
                .all(|a| !a.is_empty() && !a.contains(['\0', '\n'])),
        "invalid validator argv"
    );
    let exe = Path::new(&argv[0]);
    ensure!(
        exe.is_absolute() && exe.starts_with(root) && install["validator_binary"] == argv[0],
        "argv binary differs from prepared source"
    );
    file_identity(exe)?;
    Ok((
        install,
        argv,
        json!({"install_sha256":hash_file(&root.join("reports/install.json"))?,"argv_sha256":hash_file(&root.join("config/validator-argv.json"))?}),
    ))
}
fn validate_argv(root: &Path, install: &Value, argv: &[String]) -> Result<()> {
    let bools = [
        "--no-voting",
        "--no-xdp",
        "--private-rpc",
        "--full-rpc-api",
        "--enable-rpc-transaction-history",
        "--enable-extended-tx-metadata-storage",
        "--only-known-rpc",
    ];
    let values = [
        "--identity",
        "--ledger",
        "--accounts",
        "--snapshots",
        "--rpc-bind-address",
        "--rpc-port",
        "--dynamic-port-range",
        "--expected-genesis-hash",
        "--limit-blockstore-size",
        "--geyser-plugin-config",
        "--log",
        "--entrypoint",
        "--known-validator",
    ];
    let mut seen: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut i = 1;
    while i < argv.len() {
        let flag = &argv[i];
        let v = if bools.contains(&flag.as_str()) {
            i += 1;
            "true".into()
        } else {
            ensure!(
                values.contains(&flag.as_str()) && i + 1 < argv.len(),
                "unsupported validator option"
            );
            i += 2;
            argv[i - 1].clone()
        };
        let list = seen.entry(flag.clone()).or_default();
        ensure!(
            list.is_empty() || ["--entrypoint", "--known-validator"].contains(&flag.as_str()),
            "duplicate validator option"
        );
        list.push(v);
    }
    for flag in bools {
        ensure!(
            seen.get(flag) == Some(&vec!["true".into()]),
            "required non-voting/private flag missing"
        );
    }
    let genesis = install["genesis_hash"].as_str().context("genesis absent")?;
    ensure!(public_key(genesis), "invalid genesis");
    for (flag, value) in [
        (
            "--identity",
            root.join("config/identity.json").display().to_string(),
        ),
        ("--ledger", root.join("ledger").display().to_string()),
        ("--accounts", root.join("accounts").display().to_string()),
        ("--snapshots", root.join("snapshots").display().to_string()),
        ("--rpc-bind-address", "127.0.0.1".into()),
        ("--rpc-port", "8899".into()),
        ("--dynamic-port-range", "8000-8030".into()),
        ("--expected-genesis-hash", genesis.into()),
        ("--limit-blockstore-size", "100000000".into()),
        (
            "--geyser-plugin-config",
            root.join("config/yellowstone.json").display().to_string(),
        ),
        ("--log", "-".into()),
    ] {
        ensure!(
            seen.get(flag) == Some(&vec![value]),
            "validator option differs from canary contract"
        );
    }
    ensure!(
        seen.get("--known-validator")
            .is_some_and(|v| !v.is_empty() && v.iter().all(|x| public_key(x)))
            && seen.get("--entrypoint").is_some_and(|v| !v.is_empty()),
        "known validators and entrypoints required"
    );
    Ok(())
}
async fn kernel() -> Result<Value> {
    let mut v = serde_json::Map::new();
    for key in KERNEL_KEYS {
        let out = checked("sysctl", &strings(&["-n", key]), 5).await?;
        v.insert(
            key.into(),
            json!(std::str::from_utf8(&out)?.trim().parse::<u64>()?),
        );
    }
    Ok(Value::Object(v))
}
fn preserve_kernel(path: &Path, value: &Value) -> Result<bool> {
    if path.exists() {
        read_json(path)?;
        return Ok(false);
    }
    save(path, value, false)?;
    Ok(true)
}
async fn start(root: &Path, install: &Value, argv: &[String], check_only: bool) -> Result<Value> {
    ensure!(
        !active(&service().await?) && !validator_exists()?,
        "validator already running; no restart is allowed"
    );
    validate_argv(root, install, argv)?;
    let identity = fs::symlink_metadata(root.join("config/identity.json"))?;
    ensure!(
        identity.is_file()
            && identity.uid() == uid()
            && identity.mode() & 0o077 == 0
            && (32..=4096).contains(&identity.len()),
        "identity file permissions or size invalid"
    );
    let config_path = root.join("config/yellowstone.json");
    let plugin = read_json(&config_path)?;
    ensure!(
        plugin["grpc"]["listen"] == json!([{"address":"127.0.0.1:10000"}])
            && plugin["prometheus"]["address"] == "127.0.0.1:8999",
        "plugin endpoints must be private loopback"
    );
    let library = PathBuf::from(
        plugin["libpath"]
            .as_str()
            .context("plugin library missing")?,
    );
    ensure!(
        library.is_absolute() && library.starts_with(root),
        "plugin outside prepared root"
    );
    let checker = library
        .parent()
        .context("plugin parent")?
        .join("config-check-linux-amd64");
    for p in [&library, &checker] {
        let name = p
            .file_name()
            .and_then(|s| s.to_str())
            .context("plugin filename")?;
        let asset = install["assets"]
            .as_array()
            .context("assets missing")?
            .iter()
            .find(|a| a["name"] == name)
            .context("asset absent")?;
        ensure!(
            asset["sha256"] == hash_file(p)?
                && asset["bytes"].as_u64() == Some(fs::metadata(p)?.len()),
            "plugin asset identity differs"
        );
    }
    checked(
        checker.to_str().context("checker path")?,
        &vec!["--config".into(), config_path.display().to_string()],
        30,
    )
    .await?;
    let version = checked(&argv[0], &strings(&["--version"]), 30).await?;
    let expected = install["agave_tag"]
        .as_str()
        .context("Agave tag absent")?
        .trim_start_matches('v');
    ensure!(
        !expected.is_empty()
            && std::str::from_utf8(&version)?
                .split_whitespace()
                .any(|x| x == expected),
        "validator version differs"
    );
    let ram = memory()?;
    ensure!(
        ram["MemTotal"].as_u64().unwrap_or(0) >= 900 << 30
            && ram["MemAvailable"].as_u64().unwrap_or(0) >= 850 << 30,
        "insufficient memory"
    );
    for name in ["ledger", "accounts", "snapshots"] {
        ensure!(
            free_bytes(&root.join(name))? >= 1024 << 30,
            "insufficient disk"
        );
    }
    for port in [8899, 8900, 10000, 8999] {
        std::net::TcpListener::bind(("127.0.0.1", port)).context("loopback port in use")?;
    }
    checked("sudo", &strings(&["-n", "true"]), 5).await?;
    let checks = json!({"memory_bytes":ram,"validator_sha256":hash_file(Path::new(&argv[0]))?,"plugin_config_sha256":hash_file(&config_path)?});
    if check_only {
        return Ok(json!({"outcome":"CHECKED","preflight":checks,"service_changed":false}));
    }
    let kernel_path = root.join("reports/kernel-before.json");
    let before = if kernel_path.exists() {
        read_json(&kernel_path)?
    } else {
        json!({"schema":"hivezilla-validator-kernel-before-v1","recorded_unix_ns":now_ns(),"values":kernel().await?})
    };
    let created = preserve_kernel(&kernel_path, &before)?;
    for key in KERNEL_KEYS {
        checked(
            "sudo",
            &vec![
                "-n".into(),
                "sysctl".into(),
                "-w".into(),
                format!("{key}={UDP_MAX}"),
            ],
            5,
        )
        .await?;
    }
    ensure!(
        !active(&service().await?) && !validator_exists()?,
        "validator started during preflight"
    );
    let mut launch = strings(&[
        "-n",
        "systemd-run",
        "--unit",
        UNIT,
        "--collect",
        "--service-type=exec",
        "--description=Hivezilla finite non-voting validator canary",
    ]);
    launch.extend([
        format!("--uid={}", uid()),
        format!("--gid={}", unsafe { libc::getegid() }),
        format!("--working-directory={}", root.display()),
    ]);
    launch.extend(strings(&[
        "--property=RuntimeMaxSec=21600",
        "--property=MemoryHigh=800G",
        "--property=MemoryMax=850G",
        "--property=LimitNOFILE=1000000",
        "--property=LimitMEMLOCK=2000000000",
        "--property=Restart=no",
        "--property=UMask=0077",
        "--property=KillMode=control-group",
        "--property=StandardOutput=journal",
        "--property=StandardError=journal",
        "--",
    ]));
    launch.extend_from_slice(argv);
    checked("sudo", &launch, 40).await?;
    let s = service().await?;
    ensure!(active(&s), "service not active after start");
    Ok(
        json!({"outcome":"STARTED","preflight":checks,"service":s,"kernel_before_created":created,"kernel_before_sha256":hash_file(&kernel_path)?}),
    )
}
async fn stop(root: &Path, argv: &[String]) -> Result<Value> {
    let before = service().await?;
    if !active(&before) {
        return Ok(json!({"outcome":"ALREADY_STOPPED","service":before}));
    }
    let pid = before["MainPID"]
        .as_str()
        .context("PID absent")?
        .parse::<u32>()?;
    ensure!(pid > 0, "PID absent");
    let process = PathBuf::from(format!("/proc/{pid}"));
    ensure!(
        fs::metadata(&process)?.uid() == uid(),
        "service runs as another user"
    );
    ensure!(
        fs::canonicalize(process.join("exe"))? == fs::canonicalize(&argv[0])?,
        "service executable differs"
    );
    ensure!(
        before["WorkingDirectory"] == root.display().to_string()
            || fs::canonicalize(process.join("cwd"))? == root,
        "service root differs"
    );
    checked("sudo", &strings(&["-n", "systemctl", "stop", UNIT]), 180).await?;
    let after = service().await?;
    ensure!(!active(&after), "service remains active");
    Ok(
        json!({"outcome":"STOPPED","service_before":before,"service":after,"kernel_changed":false,"source_data_removed":false}),
    )
}
async fn status(root: &Path, source: Value) -> Result<Value> {
    let client = http()?;
    let s = service().await?;
    let observation = sample(&client).await;
    let mut categories: BTreeMap<&str, u64> = BTreeMap::new();
    if let Some(inv) = s["InvocationID"]
        .as_str()
        .filter(|v| v.len() == 32 && v.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        if let Ok(output) = command(
            "sudo",
            &vec![
                "-n".into(),
                "journalctl".into(),
                format!("_SYSTEMD_INVOCATION_ID={inv}"),
                "--no-pager".into(),
                "-n".into(),
                "120".into(),
                "-o".into(),
                "json".into(),
                "--output-fields=MESSAGE,__REALTIME_TIMESTAMP".into(),
            ],
            10,
        )
        .await
        {
            if output.status.success() {
                for line in output.stdout.split(|b| *b == b'\n') {
                    if let Ok(value) = parse_json(line) {
                        if let Some(m) = value["MESSAGE"].as_str() {
                            let low = m.to_lowercase();
                            for key in [
                                "panic", "error", "warn", "snapshot", "replay", "gossip", "slot",
                            ] {
                                if low.contains(key) {
                                    *categories.entry(key).or_default() += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let mut metrics: BTreeMap<String, f64> = BTreeMap::new();
    if let Ok(response) = client.get("http://127.0.0.1:8999/metrics").send().await {
        if let Ok(body) = http_bytes(response).await {
            if let Ok(text) = std::str::from_utf8(&body) {
                for line in text.lines() {
                    let name = line.split(['{', ' ']).next().unwrap_or("");
                    if name.len() <= 120
                        && name
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':')
                        && [
                            "slot",
                            "queue",
                            "channel",
                            "block",
                            "transaction",
                            "error",
                            "message",
                            "connection",
                        ]
                        .iter()
                        .any(|x| name.contains(x))
                    {
                        let rest = if let Some((_, r)) = line.rsplit_once('}') {
                            r
                        } else {
                            line.strip_prefix(name).unwrap_or("")
                        };
                        if let Some(v) = rest
                            .split_whitespace()
                            .next()
                            .and_then(|v| v.parse::<f64>().ok())
                            .filter(|v| v.is_finite())
                        {
                            *metrics.entry(name.into()).or_default() += v;
                        }
                    }
                }
            }
        }
    }
    Ok(
        json!({"schema":"hivezilla-validator-canary-status-v1","root":root,"unix_ns":now_ns(),"source":source,"service":s,"rpc":observation,"log_categories":categories,"raw_messages_included":false,"metrics_sum_across_labels":metrics.into_iter().take(80).collect::<BTreeMap<_,_>>(),"metric_labels_included":false,"memory_bytes":memory()?,"kernel_values":kernel().await?,"service_changed":false}),
    )
}

pub async fn run(args: ValidatorCanaryArgs) -> Result<Value> {
    match args.action {
        ValidatorCanaryAction::Execute(args) => execute(args).await,
        ValidatorCanaryAction::Status { root } => {
            let (_, _, source) = prepared(&root)?;
            status(&root, source).await
        }
        action => {
            let (root, check, kind) = match action {
                ValidatorCanaryAction::Start { root, check_only } => (root, check_only, "start"),
                ValidatorCanaryAction::Stop { root } => (root, false, "stop"),
                _ => unreachable!(),
            };
            let (install, argv, source) = prepared(&root)?;
            let _lock = lock(&root, "validator-ops.lock")?;
            let mut event = json!({"schema":"hivezilla-validator-canary-lifecycle-v1","action":kind,"started_unix_ns":now_ns(),"source":source,"root":root,"operator_uid":uid()});
            let result = if kind == "start" {
                start(&root, &install, &argv, check).await
            } else {
                stop(&root, &argv).await
            };
            match result {
                Ok(v) => {
                    for (k, v) in v.as_object().context("operation result")? {
                        event[k] = v.clone();
                    }
                }
                Err(e) => {
                    event["outcome"] = json!("FAILED");
                    event["error"] = json!(e.to_string());
                }
            }
            event["finished_unix_ns"] = json!(now_ns());
            save(
                &root
                    .join("reports")
                    .join(format!("validator-ops-{}.json", token()?)),
                &event,
                false,
            )?;
            Ok(event)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn observation(slot: u64) -> Value {
        json!({"identity":"node","genesis_hash":"chain","health":"ok","finalized_slot":slot})
    }
    #[test]
    fn needs_health_identity_advance_and_five_seconds() {
        let mut r = Readiness::default();
        assert!(
            !r.observe(&observation(10), Duration::ZERO, "node", "chain")
                .unwrap()
        );
        assert!(
            !r.observe(&observation(11), Duration::from_secs(4), "node", "chain")
                .unwrap()
        );
        assert!(
            r.observe(&observation(11), Duration::from_secs(5), "node", "chain")
                .unwrap()
        );
    }
    #[test]
    fn unchanged_slot_never_passes() {
        let mut r = Readiness::default();
        r.observe(&observation(10), Duration::ZERO, "node", "chain")
            .unwrap();
        assert!(
            !r.observe(&observation(10), Duration::from_secs(30), "node", "chain")
                .unwrap()
        );
    }
    #[test]
    fn health_loss_resets_observations() {
        let mut r = Readiness::default();
        r.observe(&observation(10), Duration::ZERO, "node", "chain")
            .unwrap();
        let mut bad = observation(11);
        bad["health"] = Value::Null;
        assert!(
            !r.observe(&bad, Duration::from_secs(30), "node", "chain")
                .unwrap()
        );
        assert!(
            !r.observe(&observation(12), Duration::from_secs(60), "node", "chain")
                .unwrap()
        );
    }
    #[test]
    fn identity_genesis_and_counter_regression_fail() {
        for mut s in [observation(9), observation(11), observation(11)] {
            let mut r = Readiness::default();
            r.observe(&observation(10), Duration::ZERO, "node", "chain")
                .unwrap();
            if s["finalized_slot"] == 11 {
                s["identity"] = json!("other");
            }
            assert!(
                r.observe(&s, Duration::from_secs(30), "node", "chain")
                    .is_err()
            );
        }
        let mut r = Readiness::default();
        let mut s = observation(10);
        s["genesis_hash"] = json!("other");
        assert!(r.observe(&s, Duration::ZERO, "node", "chain").is_err());
    }
    fn gates() -> Value {
        json!(expected_commands().into_iter().enumerate().map(|(i,c)|json!({"command":c,"exit_code":0,"elapsed_seconds":1.0,"log":format!("check-{i}.log")})).collect::<Vec<_>>())
    }
    #[test]
    fn all_five_exact_gates_are_required() {
        let v = gates();
        validate_gate(&v).unwrap();
        let mut bad = v.clone();
        bad.as_array_mut().unwrap().pop();
        assert!(validate_gate(&bad).is_err());
        let mut bad = v.clone();
        bad[0]["exit_code"] = json!(1);
        assert!(validate_gate(&bad).is_err());
        let mut bad = v;
        bad[0]["command"] = json!(["true"]);
        assert!(validate_gate(&bad).is_err());
    }
    #[test]
    fn duplicate_json_and_nonfinite_values_are_rejected() {
        assert!(parse_json(br#"{"a":1,"a":2}"#).is_err());
        assert!(parse_json(b"NaN").is_err());
    }
    #[test]
    fn kernel_baseline_is_never_replaced() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("kernel-before.json");
        fs::write(&p, b"{\"legacy\":4194304,\"other\":true}\n").unwrap();
        let before = fs::read(&p).unwrap();
        assert!(!preserve_kernel(&p, &json!({"replacement":1})).unwrap());
        assert_eq!(fs::read(p).unwrap(), before);
    }
    #[test]
    fn first_kernel_snapshot_and_records_are_no_clobber() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("kernel-before.json");
        assert!(preserve_kernel(&p, &json!({"a":1})).unwrap());
        assert!(save(&p, &json!({"a":2}), false).is_err());
        assert_eq!(read_json(&p).unwrap(), json!({"a":1}));
    }
    #[test]
    fn input_replacement_is_detected() {
        let t = tempfile::tempdir().unwrap();
        let p = t.path().join("binary");
        fs::write(&p, b"one").unwrap();
        let pin = Pinned::new(p.clone(), false).unwrap();
        fs::write(&p, b"two").unwrap();
        assert!(pin.check(false).is_err());
    }
}
