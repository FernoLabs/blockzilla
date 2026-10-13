//! Local experimental epoch capture. Keeps raw evidence; grants no publication authority.
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    net::TcpStream,
    os::fd::AsRawFd,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    run_dir: PathBuf,
    progress_file: PathBuf,
    hivezilla: PathBuf,
    blockzilla: PathBuf,
    epoch: u64,
    start_slot: u64,
    end_slot_exclusive: u64,
    genesis_hash: String,
    node_identity: String,
    #[serde(default = "default_batch")]
    batch_blocks: u64,
    #[serde(default = "default_reserve")]
    reserve_free_bytes: u64,
}
fn default_batch() -> u64 {
    128
}
fn default_reserve() -> u64 {
    536_870_912_000
}
#[derive(Clone, Serialize, Deserialize)]
struct Chunk {
    capture: PathBuf,
    coverage: PathBuf,
    slots: Vec<u64>,
    covered_start: u64,
    covered_end: u64,
    raw_bytes: u64,
    compact: Option<PathBuf>,
    compact_bytes: u64,
}
#[derive(Default, Serialize, Deserialize)]
struct State {
    epoch: u64,
    next_slot: u64,
    chunks: Vec<Chunk>,
    collected: u64,
    compacted: u64,
    skipped_slots: u64,
    missing_blocks: u64,
    raw_bytes: u64,
    compact_bytes: u64,
    capture_done: bool,
    capture_state: String,
    compaction_state: String,
    capture_error: Option<String>,
    compaction_error: Option<String>,
    retries: u64,
}
type Shared = Arc<(Mutex<State>, Condvar)>;
#[derive(Debug)]
struct CoverageGap {
    missing: Vec<u64>,
    unexpected: Vec<u64>,
}
impl std::fmt::Display for CoverageGap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "missing produced blocks {:?}; unexpected blocks {:?}",
            self.missing, self.unexpected
        )
    }
}
impl std::error::Error for CoverageGap {}
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
fn nonce() -> String {
    format!("{}-{}", now_ms(), std::process::id())
}
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("missing parent")?;
    fs::create_dir_all(parent)?;
    let temp = path.with_extension(format!("tmp.{}", nonce()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temp, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
fn read_json(path: &Path) -> Result<Value> {
    Ok(serde_json::from_reader(File::open(path)?)?)
}
fn checkpoint(cfg: &Config, state: &State) -> Result<()> {
    atomic_json(&cfg.run_dir.join("checkpoint.json"), state)?;
    let total_slots = cfg.end_slot_exclusive - cfg.start_slot;
    let covered = state
        .next_slot
        .saturating_sub(cfg.start_slot)
        .min(total_slots);
    let phase = if state.capture_done && state.compacted == state.collected {
        "COMPLETE"
    } else {
        &state.capture_state
    };
    atomic_json(
        &cfg.progress_file,
        &json!({
            "schema":"hivezilla-epoch-progress-v1", "epoch":cfg.epoch, "state":phase,
            "updated_unix_ms":now_ms(), "start_slot":cfg.start_slot, "end_slot_exclusive":cfg.end_slot_exclusive,
            "slots_in_epoch":total_slots, "covered_slots":covered,
            "collected_blocks":state.collected, "compacted_blocks":state.compacted,
            "skipped_slots":state.skipped_slots, "missing_blocks":state.missing_blocks,
            "last_collected_slot":state.chunks.last().and_then(|c| c.slots.last()),
            "last_compacted_slot":state.chunks.iter().filter(|c|c.compact.is_some()).filter_map(|c|c.slots.last()).max(),
            "raw_bytes":state.raw_bytes, "compact_bytes":state.compact_bytes,
            "capture_state":state.capture_state, "compaction_state":state.compaction_state,
            "capture_error":state.capture_error, "compaction_error":state.compaction_error,
            "capture_retries":state.retries, "run_dir":cfg.run_dir,
            "genesis_hash":cfg.genesis_hash, "node_identity":cfg.node_identity,
            "archive_verification": "SKIPPED", "archive_publication_allowed":false,
            "r2_upload":"DISABLED", "counts_basis":"sealed unique finalized block records in the target epoch",
            "gap_policy":"stop capture rather than count a missing produced block as a skipped slot"
        }),
    )
}
fn chunked(mut input: &[u8]) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    loop {
        let end = input
            .windows(2)
            .position(|x| x == b"\r\n")
            .context("chunk size line missing")?;
        let line = std::str::from_utf8(&input[..end])?
            .split(';')
            .next()
            .unwrap();
        let size = usize::from_str_radix(line, 16)?;
        input = &input[end + 2..];
        if size == 0 {
            break;
        }
        ensure!(
            size <= 8 * 1024 * 1024 && input.len() >= size + 2,
            "invalid HTTP chunk size"
        );
        ensure!(&input[size..size + 2] == b"\r\n", "invalid HTTP chunk end");
        result.extend_from_slice(&input[..size]);
        input = &input[size + 2..];
        ensure!(result.len() <= 8 * 1024 * 1024, "RPC body too large");
    }
    Ok(result)
}
fn rpc_body(method: &str, params: Value) -> Result<Vec<u8>> {
    let body =
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))?;
    let mut stream =
        TcpStream::connect_timeout(&"127.0.0.1:8899".parse()?, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    write!(
        stream,
        "POST / HTTP/1.1\r\nHost: 127.0.0.1:8899\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    let mut response = Vec::new();
    stream
        .take(8 * 1024 * 1024 + 1)
        .read_to_end(&mut response)?;
    ensure!(response.len() <= 8 * 1024 * 1024, "RPC reply too large");
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .context("HTTP headers missing")?;
    let head = std::str::from_utf8(&response[..split])?;
    ensure!(
        head.lines()
            .next()
            .is_some_and(|s| s.starts_with("HTTP/1.1 200 ") || s.starts_with("HTTP/1.0 200 ")),
        "RPC HTTP status differs"
    );
    let bytes = &response[split + 4..];
    let bytes = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        chunked(bytes)?
    } else {
        bytes.to_vec()
    };
    let reply: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        reply["id"] == 1 && reply.get("error").is_none() && reply.get("result").is_some(),
        "RPC {method} failed: {reply}"
    );
    Ok(bytes)
}
fn rpc(method: &str, params: Value) -> Result<Value> {
    let response: Value = serde_json::from_slice(&rpc_body(method, params)?)?;
    Ok(response["result"].clone())
}
fn finalized() -> Result<u64> {
    rpc("getSlot", json!([{"commitment":"finalized"}]))?
        .as_u64()
        .context("slot is not an integer")
}
fn hash(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        digest.update(&buf[..n]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn free_bytes(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: valid NUL-terminated path and writable statvfs buffer.
    ensure!(
        unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } == 0,
        "statvfs failed"
    );
    // SAFETY: successful statvfs initialized the buffer.
    let stat = unsafe { stat.assume_init() };
    Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
}
fn run(binary: &Path, args: &[String], log: &Path) -> Result<()> {
    let out = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(log)?;
    let err = out.try_clone()?;
    let code = Command::new(binary)
        .args(args)
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .status()?;
    ensure!(
        code.success(),
        "{} failed ({code}); log {}",
        binary.display(),
        log.display()
    );
    Ok(())
}
fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|s| (*s).into()).collect()
}
fn string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
fn select_slots(inventory: &Value, start: u64, end: u64) -> Result<Vec<u64>> {
    let mut all = Vec::new();
    for row in inventory["blocks"]
        .as_array()
        .context("missing block inventory")?
    {
        all.push(row["slot"].as_u64().context("invalid block slot")?);
    }
    all.sort_unstable();
    ensure!(all.windows(2).all(|p| p[0] < p[1]), "duplicate block slots");
    Ok(all
        .into_iter()
        .filter(|s| *s >= start && *s < end)
        .collect())
}
fn coverage_matches(produced: &[u64], captured: &[u64]) -> Result<()> {
    ensure!(
        produced.windows(2).all(|p| p[0] < p[1]),
        "RPC slots are not unique and sorted"
    );
    let p: BTreeSet<_> = produced.iter().copied().collect();
    let c: BTreeSet<_> = captured.iter().copied().collect();
    if p != c {
        return Err(CoverageGap {
            missing: p.difference(&c).copied().collect(),
            unexpected: c.difference(&p).copied().collect(),
        }
        .into());
    }
    Ok(())
}
fn capture_chunk(cfg: &Config, next: u64, index: usize) -> Result<Chunk> {
    ensure!(
        free_bytes(&cfg.run_dir)? > cfg.reserve_free_bytes + 4 * (1u64 << 30),
        "free-space reserve reached"
    );
    ensure!(rpc("getHealth", json!([]))? == "ok", "node is not healthy");
    let dir = cfg
        .run_dir
        .join("chunks")
        .join(format!("{index:06}-{}", nonce()));
    fs::create_dir_all(&dir)?;
    let capture = dir.join("capture");
    let mut command = args(&[
        "record-grpc-current",
        "--endpoint",
        "http://127.0.0.1:10000",
        "--validator-rpc-endpoint",
        "http://127.0.0.1:8899",
        "--timeout-secs",
        "120",
        "--idle-timeout-secs",
        "40",
        "--max-capture-bytes",
        "1073741824",
    ]);
    command.extend([
        "--output-dir".into(),
        string(&capture),
        "--max-blocks".into(),
        cfg.batch_blocks.to_string(),
        "--from-slot".into(),
        next.to_string(),
        "--min-free-bytes".into(),
        cfg.reserve_free_bytes.to_string(),
        "--cluster-id".into(),
        cfg.genesis_hash.clone(),
        "--origin-node-id".into(),
        cfg.node_identity.clone(),
        "--source-id".into(),
        format!("self-operated-epoch-{}", cfg.epoch),
    ]);
    run(&cfg.hivezilla, &command, &dir.join("capture.log"))?;
    let inventory_path = dir.join("inventory.json");
    run(
        &cfg.hivezilla,
        &[
            "inspect-grpc-current".into(),
            "--input".into(),
            string(&capture),
        ],
        &inventory_path,
    )?;
    let inventory = read_json(&inventory_path)?;
    let all = select_slots(&inventory, 0, u64::MAX)?;
    ensure!(all.len() == cfg.batch_blocks as usize, "wrong block count");
    let covered_end = (*all.last().context("empty capture")? + 1).min(cfg.end_slot_exclusive);
    ensure!(covered_end > next, "capture did not advance");
    let slots = select_slots(&inventory, next, cfg.end_slot_exclusive)?;
    let raw = rpc_body(
        "getBlocks",
        json!([next,covered_end-1,{"commitment":"finalized"}]),
    )?;
    let reply: Value = serde_json::from_slice(&raw)?;
    let produced: Vec<u64> = serde_json::from_value(reply["result"].clone())?;
    coverage_matches(&produced, &slots)?;
    fs::write(dir.join("rpc-get-blocks.json"), &raw)?;
    let root = finalized()?;
    ensure!(root >= covered_end - 1, "range is not finalized");
    let coverage = dir.join("coverage.json");
    atomic_json(
        &coverage,
        &json!({"schema":"hivezilla-finalized-rpc-coverage-v1",
        "genesis_hash":cfg.genesis_hash,"node_identity":cfg.node_identity,"commitment":"finalized",
        "start_slot":next,"end_slot_exclusive":covered_end,"finalized_slot":root,
        "produced_slots":slots,"rpc_response_sha256":digest(&raw),
        "source_receipt_sha256":hash(&capture.join("CURRENT-GRPC-SEALED.v1.json"))?}),
    )?;
    let seal = read_json(&capture.join("CURRENT-GRPC-SEALED.v1.json"))?;
    ensure!(
        seal["validator_identity"]["node_identity"] == cfg.node_identity
            && seal["validator_identity"]["genesis_hash"] == cfg.genesis_hash,
        "capture identity differs"
    );
    Ok(Chunk {
        capture,
        coverage,
        slots,
        covered_start: next,
        covered_end,
        raw_bytes: seal["files"][0]["bytes"]
            .as_u64()
            .context("raw bytes missing")?,
        compact: None,
        compact_bytes: 0,
    })
}
fn dedup_raw(source: &Path, target: &Path) -> Result<()> {
    ensure!(hash(source)? == hash(target)?, "raw evidence copies differ");
    fs::set_permissions(source, fs::Permissions::from_mode(0o400))?;
    let temp = target.with_extension(format!("link.{}", nonce()));
    fs::hard_link(source, &temp)?;
    fs::rename(temp, target)?;
    File::open(target.parent().unwrap())?.sync_all()?;
    Ok(())
}
fn directory_bytes(path: &Path) -> Result<u64> {
    let mut count = 0;
    for e in fs::read_dir(path)? {
        let e = e?;
        let m = e.metadata()?;
        count += if m.is_dir() {
            directory_bytes(&e.path())?
        } else {
            m.len()
        };
    }
    Ok(count)
}
fn compact_chunk(cfg: &Config, chunk: &Chunk) -> Result<(PathBuf, u64)> {
    if chunk.slots.is_empty() {
        return Ok((chunk.coverage.parent().unwrap().to_path_buf(), 0));
    }
    ensure!(
        free_bytes(&cfg.run_dir)? > cfg.reserve_free_bytes + 4 * (1u64 << 30),
        "free-space reserve reached"
    );
    let work = chunk
        .capture
        .parent()
        .unwrap()
        .join(format!("work-{}", nonce()));
    fs::create_dir(&work)?;
    let normalized = work.join("normalized");
    let compact = work.join("compact");
    run(
        &cfg.hivezilla,
        &[
            "normalize-current-execution".into(),
            "--input".into(),
            string(&chunk.capture),
            "--coverage".into(),
            string(&chunk.coverage),
            "--output".into(),
            string(&normalized),
            "--max-blocks".into(),
            cfg.batch_blocks.to_string(),
        ],
        &work.join("normalize.log"),
    )?;
    let receipt = read_json(&normalized.join("FINALIZED-EXECUTION-NORMALIZED.v1.json"))?;
    let receipt_slots = select_slots(&receipt, 0, u64::MAX)?;
    ensure!(receipt_slots == chunk.slots, "normalization slots differ");
    dedup_raw(
        &chunk.capture.join("events.wal"),
        &normalized.join("evidence/events.wal"),
    )?;
    let compact_result = run(
        &cfg.blockzilla,
        &[
            "build-live-execution-canary".into(),
            string(&normalized),
            string(&compact),
            "--level".into(),
            "1".into(),
            "--skip-verification".into(),
        ],
        &work.join("compact.log"),
    );
    if let Err(error) = compact_result {
        // Preserve the receipt and logs, but not a new copy of derived data
        // on each retry. The sealed capture remains the source of truth.
        fs::copy(
            normalized.join("FINALIZED-EXECUTION-NORMALIZED.v1.json"),
            work.join("FINALIZED-EXECUTION-NORMALIZED.v1.json"),
        )?;
        fs::remove_dir_all(&normalized)?;
        File::open(&work)?.sync_all()?;
        return Err(error);
    }
    let marker = read_json(&compact.join("LIVE-EXECUTION-CANARY.json"))?;
    ensure!(
        marker["state"] == "COMPLETE"
            && marker["canonical"] == false
            && marker["publication_ready"] == false
            && marker["archive_verified"] == false,
        "compact marker state/authority differs"
    );
    let slots: Vec<u64> = marker["produced_blocks"]
        .as_array()
        .context("compact slots missing")?
        .iter()
        .map(|b| b["slot"].as_u64().context("compact slot invalid"))
        .collect::<Result<_>>()?;
    ensure!(
        slots == chunk.slots && marker["blocks"] == chunk.slots.len() as u64,
        "compact block counts differ"
    );
    dedup_raw(
        &chunk.capture.join("events.wal"),
        &compact.join("events.wal"),
    )?;
    let bytes = directory_bytes(&compact)?.saturating_sub(chunk.raw_bytes);
    // Only this run's disposable normalization is removed. Sealed raw capture,
    // compact evidence, and the copied normalization receipt remain durable.
    fs::remove_dir_all(&normalized)?;
    File::open(&work)?.sync_all()?;
    Ok((compact, bytes))
}
fn compactor(cfg: Config, shared: Shared) -> Result<()> {
    loop {
        let (index, chunk) = {
            let (lock, cv) = &*shared;
            let mut state = lock.lock().unwrap();
            loop {
                if let Some((index, chunk)) = state
                    .chunks
                    .iter()
                    .enumerate()
                    .find(|(_, c)| c.compact.is_none())
                {
                    let result = (index, chunk.clone());
                    state.compaction_state = "COMPACTING".into();
                    checkpoint(&cfg, &state)?;
                    break result;
                }
                if state.capture_done {
                    state.compaction_state = "COMPLETE".into();
                    checkpoint(&cfg, &state)?;
                    return Ok(());
                }
                state.compaction_state = "WAITING".into();
                checkpoint(&cfg, &state)?;
                state = cv.wait(state).unwrap();
            }
        };
        let result = compact_chunk(&cfg, &chunk);
        {
            let mut state = shared.0.lock().unwrap();
            match result {
                Ok((path, bytes)) => {
                    state.chunks[index].compact = Some(path);
                    state.chunks[index].compact_bytes = bytes;
                    state.compacted += chunk.slots.len() as u64;
                    state.compact_bytes += bytes;
                    state.compaction_error = None;
                }
                Err(e) => {
                    state.compaction_state = "RETRYING".into();
                    state.compaction_error = Some(format!("{e:#}"));
                }
            }
            checkpoint(&cfg, &state)?;
        }
        if shared.0.lock().unwrap().compaction_error.is_some() {
            thread::sleep(Duration::from_secs(15));
        }
    }
}
fn main() -> Result<()> {
    let argv: Vec<_> = std::env::args_os().collect();
    ensure!(
        argv.len() == 3 && argv[1] == "--config",
        "usage: hivezilla-epoch-runner --config CONFIG.json"
    );
    let cfg: Config = serde_json::from_reader(File::open(&argv[2])?)?;
    ensure!(
        cfg.start_slot == cfg.epoch.checked_mul(432_000).context("epoch overflow")?
            && cfg.end_slot_exclusive == cfg.start_slot + 432_000,
        "epoch range differs"
    );
    ensure!(
        (1..=256).contains(&cfg.batch_blocks),
        "batch limit must be 1..=256"
    );
    for p in [
        &cfg.run_dir,
        &cfg.progress_file,
        &cfg.hivezilla,
        &cfg.blockzilla,
    ] {
        ensure!(p.is_absolute(), "paths must be absolute");
    }
    fs::create_dir_all(cfg.run_dir.join("chunks"))?;
    fs::set_permissions(&cfg.run_dir, fs::Permissions::from_mode(0o700))?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(cfg.run_dir.join(".lock"))?;
    // SAFETY: valid open descriptor; lock prevents two owners of the same run.
    ensure!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "another epoch runner owns this run"
    );
    ensure!(
        rpc("getGenesisHash", json!([]))? == cfg.genesis_hash
            && rpc("getIdentity", json!([]))?["identity"] == cfg.node_identity,
        "validator source identity differs"
    );
    let path = cfg.run_dir.join("checkpoint.json");
    let mut initial: State = if path.exists() {
        serde_json::from_reader(File::open(path)?)?
    } else {
        State {
            epoch: cfg.epoch,
            next_slot: cfg.start_slot,
            capture_state: "ARMED".into(),
            ..State::default()
        }
    };
    ensure!(
        initial.epoch == cfg.epoch
            && (cfg.start_slot..=cfg.end_slot_exclusive).contains(&initial.next_slot),
        "checkpoint range differs"
    );
    initial.compaction_error = None;
    checkpoint(&cfg, &initial)?;
    let shared = Arc::new((Mutex::new(initial), Condvar::new()));
    let worker_shared = shared.clone();
    let worker_cfg = cfg.clone();
    let worker = thread::spawn(move || compactor(worker_cfg, worker_shared));
    while !shared.0.lock().unwrap().capture_done {
        let next = shared.0.lock().unwrap().next_slot;
        if finalized()? + 32 < next {
            {
                let mut s = shared.0.lock().unwrap();
                s.capture_state = "ARMED".into();
                checkpoint(&cfg, &s)?;
            }
            thread::sleep(Duration::from_secs(5));
            continue;
        }
        let index = shared.0.lock().unwrap().chunks.len();
        {
            let mut s = shared.0.lock().unwrap();
            s.capture_state = "CAPTURING".into();
            checkpoint(&cfg, &s)?;
        }
        match capture_chunk(&cfg, next, index) {
            Ok(chunk) => {
                let mut s = shared.0.lock().unwrap();
                s.skipped_slots +=
                    chunk.covered_end - chunk.covered_start - chunk.slots.len() as u64;
                s.collected += chunk.slots.len() as u64;
                s.raw_bytes += chunk.raw_bytes;
                s.next_slot = chunk.covered_end;
                s.capture_done = s.next_slot == cfg.end_slot_exclusive;
                s.capture_state = if s.capture_done {
                    "DRAINING"
                } else {
                    "CAPTURING"
                }
                .into();
                s.capture_error = None;
                s.chunks.push(chunk);
                checkpoint(&cfg, &s)?;
                shared.1.notify_all();
            }
            Err(e) => {
                let text = format!("{e:#}");
                let mut s = shared.0.lock().unwrap();
                s.retries += 1;
                s.capture_error = Some(text.clone());
                s.capture_state = "RETRYING".into();
                if let Some(gap) = e.downcast_ref::<CoverageGap>() {
                    s.capture_state = "GAP".into();
                    s.missing_blocks = gap.missing.len() as u64;
                    checkpoint(&cfg, &s)?;
                    bail!("capture gap; cursor preserved: {text}");
                }
                checkpoint(&cfg, &s)?;
                drop(s);
                thread::sleep(Duration::from_secs(3));
            }
        }
        if !shared.0.lock().unwrap().capture_done {
            ensure!(
                !worker.is_finished(),
                "compaction worker stopped unexpectedly"
            );
        }
    }
    shared.1.notify_all();
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("compaction worker panicked"))??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decodes_chunked_rpc() {
        assert_eq!(
            chunked(b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n").unwrap(),
            b"Wikipedia"
        );
    }
    #[test]
    fn rejects_truncated_chunk() {
        assert!(chunked(b"10\r\nx\r\n").is_err());
    }
    #[test]
    fn a_chain_skip_is_not_a_capture_gap() {
        coverage_matches(&[10, 12], &[10, 12]).unwrap();
        assert!(coverage_matches(&[10, 11, 12], &[10, 12]).is_err());
    }
    #[test]
    fn rejects_duplicate_source_slots() {
        assert!(select_slots(&json!({"blocks":[{"slot":10},{"slot":10}]}), 10, 20).is_err());
    }
    #[test]
    fn limits_epoch_boundary_without_losing_evidence() {
        assert_eq!(
            select_slots(
                &json!({"blocks":[{"slot":19},{"slot":20},{"slot":21}]}),
                10,
                20
            )
            .unwrap(),
            vec![19]
        );
    }
    #[test]
    fn checkpoint_preserves_independent_capture_and_compaction() {
        let s = State {
            epoch: 1047,
            next_slot: 452304128,
            collected: 128,
            compacted: 0,
            ..State::default()
        };
        let bytes = serde_json::to_vec(&s).unwrap();
        let restored: State = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            (restored.next_slot, restored.collected, restored.compacted),
            (452304128, 128, 0)
        );
    }
}
