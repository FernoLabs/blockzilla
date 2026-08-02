//! Native, bounded helpers used by `linux-raw-grpc-recorder.sh`.
//!
//! This command group is intentionally hidden from the ordinary operator CLI. It replaces small
//! embedded Python programs while keeping their narrow stdout contracts. Every filesystem input is
//! opened read-only with `O_NOFOLLOW`, bounded before allocation, and checked for mutation while it
//! is read. JSON parsing rejects duplicate keys at every nesting level.

use std::{
    collections::{HashMap, HashSet},
    ffi::{CStr, CString, OsString},
    fmt,
    fs::{self, File, Metadata},
    io::{Read, Seek, SeekFrom},
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::{
            ffi::{OsStrExt, OsStringExt},
            fs::MetadataExt,
        },
    },
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use clap::{Args, Subcommand};
use serde::{Deserialize, Deserializer, de};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};

use crate::ingest::{ReplicationStreamId, read_receiver_progress_audit_snapshot};

const MAX_SMALL_JSON_BYTES: usize = 64 * 1024;
const MAX_RESUME_EVENT_BYTES: usize = 4 * 1024;
const MAX_RECEIPT_BYTES: usize = 1024 * 1024;
const MAX_JOURNAL_TAIL_BYTES: u64 = 2 * 1024 * 1024;
const MAX_RETENTION_RESULT_BYTES: usize = 1024 * 1024;
// At the default configured generation size this still represents hundreds of terabytes of
// history. The ceiling makes adversarial directory scans fail before another name or map entry is
// allocated instead of letting an operational helper exhaust the host.
const MAX_GENERATION_DIRECTORY_ENTRIES: usize = 1_000_000;
const RESUME_EVENT_DOMAIN: &[u8] = b"blockzilla-grpc-resume-coverage-warning-v1";
const MAX_REPLAY_REPORT_INTEGER: u64 = 1_000_000_000_000_000_000;
const MAX_SIGNED_REPORT_INTEGER: u64 = i64::MAX as u64;

#[derive(Debug, Args)]
pub struct RawRecorderSupportArgs {
    #[command(subcommand)]
    command: RawRecorderSupportCommand,
}

#[derive(Debug, Subcommand)]
enum RawRecorderSupportCommand {
    /// Acquire an advisory lock through an inherited descriptor.
    FlockLockFd { fd: i32, wait_secs: String },
    /// Release an advisory lock through an inherited descriptor.
    FlockUnlockFd { fd: i32 },
    /// Validate that an inherited lock descriptor still names one private inode.
    ValidatePrivateLockFdPath { fd: i32, path: PathBuf },
    /// Print `pending` or `caught-up` after comparing a signed ACK with the journal tail.
    ReceiverAckState {
        ack_path: PathBuf,
        identity_path: PathBuf,
        journal_path: PathBuf,
    },
    /// Print the deterministic ID for one reconnect-coverage event.
    ResumeCoverageEventId {
        requested_slot: u64,
        first_slot: u64,
        observed_slot: u64,
    },
    /// Validate a reconnect-coverage event and print its four colon-separated fields.
    ParseResumeCoverageEvent { path: PathBuf },
    /// Validate a replay-unavailable report and print its six integer fields.
    StrictReplayReportFields { path: PathBuf },
    /// Validate a generation receipt against its local identity and predecessor.
    ValidateGenerationReceipt {
        path: PathBuf,
        generation_id: String,
        remote_prefix: String,
        expected_predecessor: String,
    },
    /// Validate a receipt once and print the manifest SHA-256 from that same file snapshot.
    ValidateGenerationReceiptHash {
        path: PathBuf,
        generation_id: String,
        remote_prefix: String,
        expected_predecessor: String,
    },
    /// Print the manifest SHA-256 from a bounded, strictly parsed receipt.
    ReceiptManifestHash { path: PathBuf },
    /// Print the predecessor manifest SHA-256 from a bounded, strictly parsed receipt.
    ReceiptPredecessorHash { path: PathBuf },
    /// Print the five-field local generation retention scan.
    ScanGenerationRetention {
        sealed_dir: PathBuf,
        receipt_dir: PathBuf,
        base_prefix: String,
        cluster_id: String,
        origin_node_id: String,
        target_id: String,
    },
    /// Print total stored bytes from a complete Backblaze account-usage report.
    B2UsageReportBytes { path: PathBuf },
    /// Print `empty` or `chained` for the local R2 receipt ledger.
    R2ReceiptDirectoryState { path: PathBuf },
    /// Validate one R2 retention result and print its nine shell-safe fields.
    ParseR2RetentionResult {
        path: PathBuf,
        expected_mode: String,
        expected_target: u64,
        expected_prefix: String,
        expected_maximum_slot: u64,
        expected_minimum_age: u64,
        expected_minimum_generations: u64,
    },
    /// Print a read-only, fixed-prefix snapshot of a receiver progress WAL.
    SnapshotReceiverProgress {
        path: PathBuf,
        cluster_id: String,
        origin_node_id: String,
        source_id: String,
        journal_id: String,
    },
}

pub fn run_raw_recorder_support(args: RawRecorderSupportArgs) -> Result<()> {
    match args.command {
        RawRecorderSupportCommand::FlockLockFd { fd, wait_secs } => flock_lock_fd(fd, &wait_secs),
        RawRecorderSupportCommand::FlockUnlockFd { fd } => flock_unlock_fd(fd),
        RawRecorderSupportCommand::ValidatePrivateLockFdPath { fd, path } => {
            validate_private_lock_fd_path(fd, &path)
        }
        RawRecorderSupportCommand::ReceiverAckState {
            ack_path,
            identity_path,
            journal_path,
        } => {
            let state = receiver_ack_state(&ack_path, &identity_path, &journal_path)?;
            println!("{}", if state { "pending" } else { "caught-up" });
            Ok(())
        }
        RawRecorderSupportCommand::ResumeCoverageEventId {
            requested_slot,
            first_slot,
            observed_slot,
        } => {
            println!(
                "{}",
                resume_coverage_event_id(requested_slot, first_slot, observed_slot)
            );
            Ok(())
        }
        RawRecorderSupportCommand::ParseResumeCoverageEvent { path } => {
            let event = parse_resume_coverage_event(&path)?;
            println!(
                "{}:{}:{}:{}",
                event.event_id, event.requested_slot, event.first_slot, event.observed_slot
            );
            Ok(())
        }
        RawRecorderSupportCommand::StrictReplayReportFields { path } => {
            let values = strict_replay_report_fields(&path)?;
            println!(
                "{} {} {} {} {} {}",
                values[0], values[1], values[2], values[3], values[4], values[5]
            );
            Ok(())
        }
        RawRecorderSupportCommand::ValidateGenerationReceipt {
            path,
            generation_id,
            remote_prefix,
            expected_predecessor,
        } => {
            validate_generation_receipt(
                &path,
                &generation_id,
                &remote_prefix,
                &expected_predecessor,
            )?;
            Ok(())
        }
        RawRecorderSupportCommand::ValidateGenerationReceiptHash {
            path,
            generation_id,
            remote_prefix,
            expected_predecessor,
        } => {
            let receipt = validate_generation_receipt(
                &path,
                &generation_id,
                &remote_prefix,
                &expected_predecessor,
            )?;
            println!("{}", receipt.manifest_hash);
            Ok(())
        }
        RawRecorderSupportCommand::ReceiptManifestHash { path } => {
            println!("{}", receipt_manifest_hash(&path)?);
            Ok(())
        }
        RawRecorderSupportCommand::ReceiptPredecessorHash { path } => {
            println!("{}", receipt_predecessor_hash(&path)?);
            Ok(())
        }
        RawRecorderSupportCommand::ScanGenerationRetention {
            sealed_dir,
            receipt_dir,
            base_prefix,
            cluster_id,
            origin_node_id,
            target_id,
        } => {
            let scan = scan_generation_retention(
                &sealed_dir,
                &receipt_dir,
                &base_prefix,
                &cluster_id,
                &origin_node_id,
                &target_id,
            )?;
            println!(
                "{} {} {} {} {}",
                scan.retained_count,
                scan.uncommitted_count,
                scan.first_uncommitted.as_deref().unwrap_or("-"),
                scan.first_committed.as_deref().unwrap_or("-"),
                u8::from(scan.target_committed)
            );
            Ok(())
        }
        RawRecorderSupportCommand::B2UsageReportBytes { path } => {
            println!("{}", b2_usage_report_bytes(&path)?);
            Ok(())
        }
        RawRecorderSupportCommand::R2ReceiptDirectoryState { path } => {
            println!("{}", r2_receipt_directory_state(&path)?);
            Ok(())
        }
        RawRecorderSupportCommand::ParseR2RetentionResult {
            path,
            expected_mode,
            expected_target,
            expected_prefix,
            expected_maximum_slot,
            expected_minimum_age,
            expected_minimum_generations,
        } => {
            let result = parse_r2_retention_result(
                &path,
                &expected_mode,
                expected_target,
                &expected_prefix,
                expected_maximum_slot,
                expected_minimum_age,
                expected_minimum_generations,
            )?;
            println!(
                "{} {} {} {} {} {} {} {} {}",
                result.bytes_before,
                result.bytes_after,
                result.generations_before,
                result.generations_after,
                result.selected_count,
                result.selected_bytes,
                u8::from(result.target_satisfied),
                result.first_selected.as_deref().unwrap_or("-"),
                result.last_selected.as_deref().unwrap_or("-")
            );
            Ok(())
        }
        RawRecorderSupportCommand::SnapshotReceiverProgress {
            path,
            cluster_id,
            origin_node_id,
            source_id,
            journal_id,
        } => {
            let stream = ReplicationStreamId {
                cluster_id,
                origin_node_id,
                source_id,
                journal_id: parse_journal_id(&journal_id)?,
            };
            let snapshot = read_receiver_progress_audit_snapshot(&path, &stream)?;
            println!(
                "{}",
                serde_json::to_string(&snapshot).context("encode receiver progress snapshot")?
            );
            Ok(())
        }
    }
}

fn parse_journal_id(value: &str) -> Result<[u8; 16]> {
    ensure!(
        is_hex_case_insensitive(value, 32),
        "journal id must contain exactly 32 hex digits"
    );
    let mut output = [0u8; 16];
    for (index, byte) in output.iter_mut().enumerate() {
        let start = index * 2;
        *byte =
            u8::from_str_radix(&value[start..start + 2], 16).context("decode journal id hex")?;
    }
    Ok(output)
}

fn flock_lock_fd(fd: i32, wait_secs: &str) -> Result<()> {
    ensure!(fd >= 0, "lock descriptor must be non-negative");
    let wait_secs = wait_secs
        .parse::<f64>()
        .context("lock wait must be a number")?;
    ensure!(
        wait_secs.is_finite() && wait_secs >= 0.0,
        "invalid lock wait"
    );
    let wait = Duration::try_from_secs_f64(wait_secs).context("lock wait is too large")?;
    let deadline = Instant::now()
        .checked_add(wait)
        .context("lock deadline overflow")?;
    loop {
        let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::WouldBlock {
            return Err(error).context("lock inherited descriptor");
        }
        let now = Instant::now();
        if now >= deadline {
            bail!("timed out waiting for inherited lock descriptor");
        }
        thread::sleep(Duration::from_millis(50).min(deadline.saturating_duration_since(now)));
    }
}

fn flock_unlock_fd(fd: i32) -> Result<()> {
    ensure!(fd >= 0, "lock descriptor must be non-negative");
    let result = unsafe { libc::flock(fd, libc::LOCK_UN) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("unlock inherited descriptor");
    }
    Ok(())
}

fn validate_private_lock_fd_path(fd: i32, path: &Path) -> Result<()> {
    ensure!(fd >= 0, "lock descriptor must be non-negative");
    let mut opened = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, opened.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("inspect inherited lock descriptor");
    }
    let opened = unsafe { opened.assume_init() };
    let linked = fs::symlink_metadata(path).context("inspect linked lock path")?;
    ensure!(
        linked.is_file(),
        "cache retention lock is not a regular file"
    );
    ensure!(
        same_stat_identity(&opened, &linked),
        "cache retention lock changed while it was opened"
    );
    let effective_uid = unsafe { libc::geteuid() };
    ensure!(
        opened.st_uid == 0 || opened.st_uid == effective_uid,
        "cache retention lock has an untrusted owner"
    );
    ensure!(
        opened.st_mode & 0o077 == 0,
        "cache retention lock must be private"
    );
    Ok(())
}

fn receiver_ack_state(ack_path: &Path, identity_path: &Path, journal_path: &Path) -> Result<bool> {
    let ack = strict_json_file(ack_path, MAX_SMALL_JSON_BYTES, "receiver ACK")?;
    let identity = strict_json_file(identity_path, MAX_SMALL_JSON_BYTES, "journal identity")?;
    let ack = object(&ack, "receiver ACK")?;
    let identity = object(&identity, "journal identity")?;
    ensure!(
        u64_field(ack, "schema_version")? == 1,
        "unsupported ACK schema"
    );
    for field in ["cluster_id", "origin_node_id", "source_id"] {
        ensure!(
            string_field(ack, field)? == string_field(identity, field)?,
            "receiver ACK identity mismatch"
        );
    }

    let (journal_id, replication_base) = match (
        identity.get("replication_journal_id"),
        identity.get("replication_sequence_base"),
    ) {
        (None | Some(Value::Null), None | Some(Value::Null)) => (
            identity
                .get("journal_id")
                .context("journal identity lacks journal_id")?,
            0,
        ),
        (Some(journal_id), Some(replication_base))
            if !journal_id.is_null() && !replication_base.is_null() =>
        {
            (
                journal_id,
                replication_base
                    .as_u64()
                    .context("invalid replication_sequence_base")?,
            )
        }
        _ => bail!("incomplete replication journal identity"),
    };
    let journal_hex = journal_id_hex(journal_id)?;
    ensure!(
        string_field(ack, "journal_id")? == journal_hex,
        "receiver ACK journal mismatch"
    );
    let through_sequence = u64_field(ack, "through_sequence")?;
    let frame_id = last_complete_journal_frame_id(journal_path)?;
    let local_sequence = replication_base
        .checked_add(frame_id)
        .context("local replication sequence overflow")?;
    Ok(through_sequence < local_sequence)
}

fn journal_id_hex(value: &Value) -> Result<String> {
    if let Some(value) = value.as_str() {
        ensure!(is_hex_case_insensitive(value, 32), "invalid journal id");
        return Ok(value.to_ascii_lowercase());
    }
    let values = value.as_array().context("invalid journal id")?;
    ensure!(values.len() == 16, "invalid journal id length");
    let mut output = String::with_capacity(32);
    for value in values {
        let byte = value.as_u64().context("invalid journal id byte")?;
        ensure!(byte <= 255, "invalid journal id byte");
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("write to String");
    }
    Ok(output)
}

fn last_complete_journal_frame_id(path: &Path) -> Result<u64> {
    let mut file = open_regular_nofollow(path, "raw journal")?;
    let size = file.metadata().context("inspect raw journal")?.len();
    ensure!(size > 0, "raw journal is empty");
    let window = size.min(MAX_JOURNAL_TAIL_BYTES);
    file.seek(SeekFrom::Start(size - window))
        .context("seek raw journal tail")?;
    let mut bytes = vec![0; window as usize];
    file.read_exact(&mut bytes)
        .context("read raw journal tail")?;
    if !bytes.ends_with(b"\n") {
        let marker = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .context("raw journal tail has no complete record")?;
        bytes.truncate(marker + 1);
    }
    let line = bytes
        .split(|byte| *byte == b'\n')
        .rev()
        .find(|line| line.iter().any(|byte| !byte.is_ascii_whitespace()))
        .context("raw journal tail has no record")?;
    let value = strict_json(line).context("parse raw journal tail")?;
    u64_field(object(&value, "raw journal row")?, "frame_id")
}

fn resume_coverage_event_id(requested: u64, first: u64, observed: u64) -> String {
    let mut digest = Sha256::new();
    digest.update(RESUME_EVENT_DOMAIN);
    digest.update(requested.to_le_bytes());
    digest.update(first.to_le_bytes());
    digest.update(observed.to_le_bytes());
    let digest = digest.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(encoded, "{byte:02x}").expect("write to String");
    }
    encoded
}

struct ResumeCoverageEvent {
    event_id: String,
    requested_slot: u64,
    first_slot: u64,
    observed_slot: u64,
}

fn parse_resume_coverage_event(path: &Path) -> Result<ResumeCoverageEvent> {
    let event = strict_json_file(path, MAX_RESUME_EVENT_BYTES, "resume coverage event")?;
    let event = object(&event, "resume coverage event")?;
    const FIELDS: [&str; 6] = [
        "event_id",
        "schema_version",
        "requested_overlap_slot",
        "first_delivered_slot",
        "observed_later_slot",
        "written_unix_secs",
    ];
    ensure!(
        event.len() == FIELDS.len() && FIELDS.iter().all(|field| event.contains_key(*field)),
        "unexpected resume coverage event fields"
    );
    ensure!(
        u64_field(event, "schema_version")? == 1,
        "invalid event schema"
    );
    let requested_slot = u64_field(event, "requested_overlap_slot")?;
    let first_slot = u64_field(event, "first_delivered_slot")?;
    let observed_slot = u64_field(event, "observed_later_slot")?;
    let _written = u64_field(event, "written_unix_secs")?;
    ensure!(requested_slot < observed_slot, "event does not advance");
    let event_id = string_field(event, "event_id")?.to_owned();
    ensure!(is_hex(&event_id, 64), "invalid event ID");
    ensure!(
        event_id == resume_coverage_event_id(requested_slot, first_slot, observed_slot),
        "event ID mismatch"
    );
    Ok(ResumeCoverageEvent {
        event_id,
        requested_slot,
        first_slot,
        observed_slot,
    })
}

fn strict_replay_report_fields(path: &Path) -> Result<[u64; 6]> {
    let report = strict_json_file(path, MAX_SMALL_JSON_BYTES, "replay report")?;
    let report = object(&report, "replay report")?;
    ensure!(
        report.get("replay_unavailable") == Some(&Value::Bool(true)),
        "not a replay-unavailable report"
    );
    let mut output = [0; 6];
    for (index, name) in [
        "resume_overlap_slot",
        "replay_unavailable_requested_slot",
        "replay_available_slot",
        "effective_from_slot",
        "frames_seen",
        "frames_written",
    ]
    .iter()
    .enumerate()
    {
        let value = u64_field(report, name)?;
        ensure!(
            value < MAX_REPLAY_REPORT_INTEGER,
            "invalid replay report integer"
        );
        output[index] = value;
    }
    Ok(output)
}

#[derive(Debug, Clone)]
struct ReceiptFields {
    manifest_hash: String,
    predecessor: Option<String>,
}

fn validate_generation_receipt(
    path: &Path,
    generation_id: &str,
    remote_prefix: &str,
    expected_predecessor: &str,
) -> Result<ReceiptFields> {
    ensure!(valid_generation_id(generation_id), "invalid generation id");
    ensure!(
        expected_predecessor == "-"
            || expected_predecessor == "*"
            || is_hex(expected_predecessor, 64),
        "invalid expected predecessor"
    );
    let file = open_regular_nofollow(path, "generation receipt")?;
    validate_generation_receipt_file(file, generation_id, remote_prefix, expected_predecessor)
}

fn validate_generation_receipt_file(
    file: File,
    generation_id: &str,
    remote_prefix: &str,
    expected_predecessor: &str,
) -> Result<ReceiptFields> {
    let bytes = read_bounded_open_file(file, MAX_RECEIPT_BYTES, "generation receipt")?;
    let receipt = strict_json(&bytes).context("parse generation receipt")?;
    validate_generation_receipt_value(&receipt, generation_id, remote_prefix, expected_predecessor)
}

fn validate_generation_receipt_value(
    receipt: &Value,
    generation_id: &str,
    remote_prefix: &str,
    expected_predecessor: &str,
) -> Result<ReceiptFields> {
    let receipt = object(receipt, "generation receipt")?;
    ensure!(
        u64_field(receipt, "schema_version")? == 1,
        "unsupported receipt schema"
    );
    ensure!(
        string_field(receipt, "generation_id")? == generation_id,
        "receipt ID mismatch"
    );
    ensure!(
        string_field(receipt, "remote_prefix")? == remote_prefix,
        "receipt prefix mismatch"
    );
    ensure!(
        string_field(receipt, "manifest_key")? == format!("{remote_prefix}/manifest.json"),
        "manifest key mismatch"
    );
    ensure!(
        string_field(receipt, "commit_key")? == format!("{remote_prefix}/_COMMITTED"),
        "commit key mismatch"
    );
    let manifest_hash = string_field(receipt, "manifest_sha256")?.to_owned();
    ensure!(is_hex(&manifest_hash, 64), "invalid manifest digest");
    ensure!(
        is_hex(string_field(receipt, "commit_sha256")?, 64),
        "invalid commit digest"
    );
    ensure!(
        valid_version_id(string_field(receipt, "manifest_version_id")?),
        "invalid manifest version"
    );
    ensure!(
        valid_version_id(string_field(receipt, "commit_version_id")?),
        "invalid commit version"
    );
    ensure!(u64_field(receipt, "file_count")? >= 1, "invalid file count");
    ensure!(
        u64_field(receipt, "total_bytes")? > 0,
        "invalid total bytes"
    );
    ensure!(
        u64_field(receipt, "verified_unix_secs")? > 0,
        "invalid verification time"
    );
    let predecessor = match receipt.get("predecessor_manifest_sha256") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if is_hex(value, 64) => Some(value.clone()),
        _ => bail!("invalid predecessor hash"),
    };
    match expected_predecessor {
        "-" => ensure!(predecessor.is_none(), "unexpected receipt predecessor"),
        "*" => {}
        expected => ensure!(
            predecessor.as_deref() == Some(expected),
            "receipt predecessor mismatch"
        ),
    }
    Ok(ReceiptFields {
        manifest_hash,
        predecessor,
    })
}

fn receipt_manifest_hash(path: &Path) -> Result<String> {
    let receipt = strict_json_file(path, MAX_RECEIPT_BYTES, "generation receipt")?;
    let hash = string_field(object(&receipt, "generation receipt")?, "manifest_sha256")?;
    ensure!(is_hex(hash, 64), "invalid manifest digest");
    Ok(hash.to_owned())
}

fn receipt_predecessor_hash(path: &Path) -> Result<String> {
    let receipt = strict_json_file(path, MAX_RECEIPT_BYTES, "generation receipt")?;
    let hash = string_field(
        object(&receipt, "generation receipt")?,
        "predecessor_manifest_sha256",
    )?;
    ensure!(is_hex(hash, 64), "invalid predecessor digest");
    Ok(hash.to_owned())
}

struct GenerationRetentionScan {
    retained_count: usize,
    uncommitted_count: usize,
    first_uncommitted: Option<String>,
    first_committed: Option<String>,
    target_committed: bool,
}

fn scan_generation_retention(
    sealed_dir: &Path,
    receipt_dir: &Path,
    base_prefix: &str,
    cluster_id: &str,
    origin_node_id: &str,
    target_id: &str,
) -> Result<GenerationRetentionScan> {
    let sealed_guard = open_directory_nofollow(sealed_dir, "sealed generation directory")?;
    let receipt_guard = open_directory_nofollow(receipt_dir, "generation receipt directory")?;
    ensure!(
        target_id == "-" || valid_generation_id(target_id),
        "invalid retention target"
    );

    let mut sealed_ids = Vec::new();
    for name in directory_entry_names(&sealed_guard, MAX_GENERATION_DIRECTORY_ENTRIES)? {
        let raw_name = name.as_bytes();
        if !raw_name.starts_with(b"slot-") {
            continue;
        }
        let name = name.to_str().context("invalid sealed generation name")?;
        ensure!(valid_generation_id(name), "invalid sealed generation name");
        ensure!(
            mode_is_directory(metadata_at(&sealed_guard, name)?.st_mode),
            "sealed generation is not a real directory"
        );
        sealed_ids.push(name.to_owned());
    }
    sealed_ids.sort_unstable();

    let mut receipts_by_hash: HashMap<String, (String, Option<String>)> = HashMap::new();
    for name in directory_entry_names(&receipt_guard, MAX_GENERATION_DIRECTORY_ENTRIES)? {
        let raw_name = name.as_bytes();
        if !(raw_name.starts_with(b"slot-") && raw_name.ends_with(b".json")) {
            continue;
        }
        let name = name.to_str().context("invalid generation receipt name")?;
        ensure!(valid_receipt_name(name), "invalid generation receipt name");
        let metadata = metadata_at(&receipt_guard, name)?;
        ensure!(
            mode_is_regular(metadata.st_mode),
            "generation receipt is not a real file"
        );
        if metadata.st_size <= 0 || metadata.st_size as u64 > MAX_RECEIPT_BYTES as u64 {
            continue;
        }
        let generation_id = &name[..name.len() - ".json".len()];
        let prefix = format!("{base_prefix}/{cluster_id}/{origin_node_id}/{generation_id}");
        let parsed = openat_regular_nofollow(&receipt_guard, name, "generation receipt")
            .and_then(|file| read_bounded_open_file(file, MAX_RECEIPT_BYTES, "generation receipt"))
            .and_then(|bytes| strict_json(&bytes))
            .and_then(|receipt| {
                validate_generation_receipt_value(&receipt, generation_id, &prefix, "*")
            });
        let Ok(parsed) = parsed else {
            continue;
        };
        ensure!(
            receipts_by_hash.len() < MAX_GENERATION_DIRECTORY_ENTRIES,
            "too many valid generation receipts"
        );
        ensure!(
            !receipts_by_hash.contains_key(&parsed.manifest_hash),
            "duplicate generation manifest hash"
        );
        receipts_by_hash.insert(
            parsed.manifest_hash,
            (generation_id.to_owned(), parsed.predecessor),
        );
    }

    let mut committed_ids = HashSet::new();
    if let Some(metadata) = metadata_at_optional(&receipt_guard, ".chain")? {
        ensure!(
            mode_is_regular(metadata.st_mode) && metadata.st_size > 0 && metadata.st_size <= 256,
            "invalid receipt chain file"
        );
        let file = openat_regular_nofollow(&receipt_guard, ".chain", "receipt chain")?;
        let chain = read_bounded_open_file(file, 256, "receipt chain")?;
        ensure!(chain.is_ascii(), "receipt chain is not ASCII");
        let chain = std::str::from_utf8(&chain).expect("ASCII is UTF-8");
        let parts = chain.split_ascii_whitespace().collect::<Vec<_>>();
        ensure!(parts.len() == 2, "invalid receipt chain record");
        let chain_generation_id = parts[0];
        ensure!(
            valid_generation_id(chain_generation_id) && is_hex(parts[1], 64),
            "invalid receipt chain head"
        );
        let mut current_hash = Some(parts[1].to_owned());
        let mut seen_hashes = HashSet::new();
        let mut first = true;
        while let Some(hash) = current_hash {
            ensure!(seen_hashes.insert(hash.clone()), "receipt chain is cyclic");
            let (generation_id, predecessor) = receipts_by_hash
                .get(&hash)
                .context("receipt chain is incomplete")?;
            if first {
                ensure!(
                    generation_id == chain_generation_id,
                    "receipt chain head ID mismatch"
                );
                first = false;
            }
            ensure!(
                committed_ids.insert(generation_id.clone()),
                "duplicate generation in receipt chain"
            );
            current_hash = predecessor.clone();
        }
    }

    let uncommitted = sealed_ids
        .iter()
        .filter(|id| !committed_ids.contains(*id))
        .cloned()
        .collect::<Vec<_>>();
    let committed = sealed_ids
        .iter()
        .filter(|id| committed_ids.contains(*id))
        .cloned()
        .collect::<Vec<_>>();
    Ok(GenerationRetentionScan {
        retained_count: sealed_ids.len(),
        uncommitted_count: uncommitted.len(),
        first_uncommitted: uncommitted.first().cloned(),
        first_committed: committed.first().cloned(),
        target_committed: target_id != "-" && committed_ids.contains(target_id),
    })
}

fn b2_usage_report_bytes(path: &Path) -> Result<u64> {
    let report = strict_json_file(path, MAX_SMALL_JSON_BYTES, "usage report")?;
    let report = object(&report, "usage report")?;
    ensure!(
        u64_field(report, "schema_version")? == 1,
        "unsupported usage schema"
    );
    ensure!(
        report.get("scope_complete") == Some(&Value::Bool(true)),
        "usage report is incomplete"
    );
    signed_u64_field(report, "total_stored_bytes")
}

fn r2_receipt_directory_state(path: &Path) -> Result<&'static str> {
    let guard = open_directory_nofollow(path, "receipt directory")?;
    let entries = directory_entry_names(&guard, MAX_GENERATION_DIRECTORY_ENTRIES)?;
    if entries.is_empty() {
        return Ok("empty");
    }
    entries
        .iter()
        .find(|entry| entry.as_bytes() == b".chain")
        .context("non-empty receipt directory has no chain head")?;
    ensure!(
        mode_is_regular(metadata_at(&guard, ".chain")?.st_mode),
        "receipt chain head is not a regular file"
    );
    Ok("chained")
}

struct R2RetentionResult {
    bytes_before: u64,
    bytes_after: u64,
    generations_before: u64,
    generations_after: u64,
    selected_count: usize,
    selected_bytes: u64,
    target_satisfied: bool,
    first_selected: Option<String>,
    last_selected: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn parse_r2_retention_result(
    path: &Path,
    expected_mode: &str,
    expected_target: u64,
    expected_prefix: &str,
    expected_maximum_slot: u64,
    expected_minimum_age: u64,
    expected_minimum_generations: u64,
) -> Result<R2RetentionResult> {
    for value in [
        expected_target,
        expected_maximum_slot,
        expected_minimum_age,
        expected_minimum_generations,
    ] {
        ensure!(
            value <= MAX_SIGNED_REPORT_INTEGER,
            "expected retention integer is too large"
        );
    }
    let result = strict_json_file(path, MAX_RETENTION_RESULT_BYTES, "retention result")?;
    let result = object(&result, "retention result")?;
    ensure!(
        u64_field(result, "schema_version")? == 1,
        "unsupported retention schema"
    );
    ensure!(
        string_field(result, "storage_provider")? == "r2",
        "retention provider mismatch"
    );
    ensure!(
        string_field(result, "mode")? == expected_mode,
        "retention mode mismatch"
    );
    ensure!(
        string_field(result, "remote_prefix")? == expected_prefix,
        "retention prefix mismatch"
    );
    ensure!(
        u64_field(result, "target_bytes")? == expected_target,
        "retention target mismatch"
    );
    ensure!(
        u64_field(result, "maximum_generation_slot")? == expected_maximum_slot,
        "retention maximum slot mismatch"
    );
    ensure!(
        u64_field(result, "minimum_age_secs")? == expected_minimum_age,
        "retention minimum age mismatch"
    );
    ensure!(
        u64_field(result, "minimum_retained_generations")? == expected_minimum_generations,
        "retention minimum count mismatch"
    );

    let bytes_before = signed_u64_field(result, "retained_payload_bytes_before")?;
    let bytes_after = signed_u64_field(result, "retained_payload_bytes_after")?;
    let generations_before = signed_u64_field(result, "retained_generation_count_before")?;
    let generations_after = signed_u64_field(result, "retained_generation_count_after")?;
    let selected_bytes = signed_u64_field(result, "selected_payload_bytes")?;
    let selected_values = result
        .get("selected_generation_ids")
        .and_then(Value::as_array)
        .context("invalid selected generation IDs")?;
    let mut selected = Vec::with_capacity(selected_values.len());
    let mut unique = HashSet::new();
    for value in selected_values {
        let value = value.as_str().context("invalid selected generation ID")?;
        ensure!(valid_generation_id(value), "invalid selected generation ID");
        ensure!(unique.insert(value), "duplicate selected generation ID");
        selected.push(value.to_owned());
    }
    let target_satisfied = result
        .get("target_satisfied")
        .and_then(Value::as_bool)
        .context("invalid target_satisfied")?;
    ensure!(bytes_after <= bytes_before, "retention bytes increased");
    ensure!(
        bytes_before - bytes_after == selected_bytes,
        "inconsistent selected bytes"
    );
    ensure!(
        generations_after <= generations_before,
        "retention generation count increased"
    );
    ensure!(
        generations_before - generations_after
            == u64::try_from(selected.len()).context("selected count overflow")?,
        "inconsistent selected generation count"
    );
    if expected_mode == "dry-run" && expected_target == MAX_SIGNED_REPORT_INTEGER {
        ensure!(
            selected.is_empty() && bytes_before == bytes_after && target_satisfied,
            "accounting dry-run unexpectedly selected data"
        );
    }
    Ok(R2RetentionResult {
        bytes_before,
        bytes_after,
        generations_before,
        generations_after,
        selected_count: selected.len(),
        selected_bytes,
        target_satisfied,
        first_selected: selected.first().cloned(),
        last_selected: selected.last().cloned(),
    })
}

fn signed_u64_field(object: &Map<String, Value>, name: &str) -> Result<u64> {
    let value = u64_field(object, name)?;
    ensure!(value <= MAX_SIGNED_REPORT_INTEGER, "invalid {name}");
    Ok(value)
}

fn valid_generation_id(value: &str) -> bool {
    value.len() == 25
        && value.starts_with("slot-")
        && value.as_bytes()[5..].iter().all(u8::is_ascii_digit)
}

fn valid_receipt_name(value: &str) -> bool {
    value.strip_suffix(".json").is_some_and(valid_generation_id)
}

fn valid_version_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1024
        && !value
            .chars()
            .any(|character| character <= '\u{1f}' || character == '\u{7f}')
}

fn is_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn is_hex_case_insensitive(value: &str, length: usize) -> bool {
    value.len() == length && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .with_context(|| format!("{label} must be an object"))
}

fn u64_field(object: &Map<String, Value>, name: &str) -> Result<u64> {
    object
        .get(name)
        .and_then(Value::as_u64)
        .with_context(|| format!("invalid {name}"))
}

fn string_field<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a str> {
    object
        .get(name)
        .and_then(Value::as_str)
        .with_context(|| format!("invalid {name}"))
}

fn open_regular_nofollow(path: &Path, label: &str) -> Result<File> {
    let path = CString::new(path.as_os_str().as_bytes()).context("path contains NUL")?;
    let descriptor = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("open {label}"));
    }
    let file = unsafe { File::from_raw_fd(descriptor) };
    ensure!(
        file.metadata()
            .with_context(|| format!("inspect {label}"))?
            .is_file(),
        "{label} is not a regular file"
    );
    Ok(file)
}

fn open_directory_nofollow(path: &Path, label: &str) -> Result<File> {
    let path = CString::new(path.as_os_str().as_bytes()).context("path contains NUL")?;
    let descriptor = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("open {label}"));
    }
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn openat_regular_nofollow(parent: &File, name: &str, label: &str) -> Result<File> {
    let name = CString::new(name).context("file name contains NUL")?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("open {label}"));
    }
    let file = unsafe { File::from_raw_fd(descriptor) };
    ensure!(
        file.metadata()
            .with_context(|| format!("inspect {label}"))?
            .is_file(),
        "{label} is not a regular file"
    );
    Ok(file)
}

fn metadata_at(parent: &File, name: &str) -> Result<libc::stat> {
    metadata_at_optional(parent, name)?.context("directory entry disappeared")
}

fn metadata_at_optional(parent: &File, name: &str) -> Result<Option<libc::stat>> {
    let name = CString::new(name).context("file name contains NUL")?;
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        return Ok(Some(unsafe { metadata.assume_init() }));
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::NotFound {
        Ok(None)
    } else {
        Err(error).context("inspect directory entry")
    }
}

fn mode_is_regular(mode: libc::mode_t) -> bool {
    mode & libc::S_IFMT == libc::S_IFREG
}

fn mode_is_directory(mode: libc::mode_t) -> bool {
    mode & libc::S_IFMT == libc::S_IFDIR
}

struct DirectoryStream(*mut libc::DIR);

impl Drop for DirectoryStream {
    fn drop(&mut self) {
        unsafe {
            libc::closedir(self.0);
        }
    }
}

fn directory_entry_names(directory: &File, maximum: usize) -> Result<Vec<OsString>> {
    ensure!(maximum > 0, "directory entry limit must be non-zero");
    let descriptor = duplicate_cloexec(directory.as_raw_fd())?;
    let stream = unsafe { libc::fdopendir(descriptor.as_raw_fd()) };
    if stream.is_null() {
        return Err(std::io::Error::last_os_error()).context("enumerate directory descriptor");
    }
    // SAFETY: `fdopendir` took ownership of this exact descriptor on success.
    let _ = descriptor.into_raw_fd();
    let stream = DirectoryStream(stream);
    let mut output = Vec::new();
    loop {
        clear_errno();
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error().unwrap_or(0) != 0 {
                return Err(error).context("read directory descriptor");
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        ensure!(
            output.len() < maximum,
            "directory contains too many entries"
        );
        output.push(OsString::from_vec(name.to_vec()));
    }
    Ok(output)
}

fn duplicate_cloexec(descriptor: RawFd) -> Result<OwnedFd> {
    let duplicate = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(std::io::Error::last_os_error()).context("duplicate directory descriptor");
    }
    // SAFETY: `fcntl(F_DUPFD_CLOEXEC)` returned a new descriptor owned by this function.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn clear_errno() {
    unsafe {
        *libc::__errno_location() = 0;
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
fn clear_errno() {
    unsafe {
        *libc::__error() = 0;
    }
}

fn read_bounded_regular(path: &Path, maximum: usize, label: &str) -> Result<Vec<u8>> {
    let file = open_regular_nofollow(path, label)?;
    read_bounded_open_file(file, maximum, label)
}

fn read_bounded_open_file(file: File, maximum: usize, label: &str) -> Result<Vec<u8>> {
    read_bounded_open_file_after_snapshot(file, maximum, label, |_| Ok(()))
}

fn read_bounded_open_file_after_snapshot<F>(
    mut file: File,
    maximum: usize,
    label: &str,
    after_snapshot: F,
) -> Result<Vec<u8>>
where
    F: FnOnce(&Metadata) -> Result<()>,
{
    let before = file
        .metadata()
        .with_context(|| format!("inspect {label}"))?;
    ensure!(
        before.len() > 0 && before.len() <= maximum as u64,
        "{label} has an invalid size"
    );
    after_snapshot(&before)?;
    let mut bytes = Vec::with_capacity(before.len() as usize);
    file.by_ref()
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {label}"))?;
    let after = file
        .metadata()
        .with_context(|| format!("reinspect {label}"))?;
    ensure!(
        bytes.len() == before.len() as usize && same_file_snapshot(&before, &after),
        "{label} changed while reading"
    );
    Ok(bytes)
}

fn same_file_snapshot(left: &Metadata, right: &Metadata) -> bool {
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

#[allow(clippy::unnecessary_cast)]
fn same_stat_identity(left: &libc::stat, right: &Metadata) -> bool {
    // `dev_t` and `ino_t` widths differ between supported Unix targets, while
    // `MetadataExt` deliberately normalizes them to u64.
    left.st_dev as u64 == right.dev() && left.st_ino as u64 == right.ino()
}

fn strict_json_file(path: &Path, maximum: usize, label: &str) -> Result<Value> {
    let bytes = read_bounded_regular(path, maximum, label)?;
    strict_json(&bytes).with_context(|| format!("parse {label}"))
}

fn strict_json(bytes: &[u8]) -> Result<Value> {
    serde_json::from_slice::<StrictValue>(bytes)
        .map(|value| value.0)
        .context("invalid strict JSON")
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

impl<'de> de::Visitor<'de> for StrictValueVisitor {
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
            .map(Value::Number)
            .map(StrictValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string(value.to_owned())
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
        A: de::SeqAccess<'de>,
    {
        let mut output = Vec::new();
        while let Some(value) = sequence.next_element::<StrictValue>()? {
            output.push(value.0);
        }
        Ok(StrictValue(Value::Array(output)))
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut output = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if output.contains_key(&key) {
                return Err(de::Error::custom(format_args!(
                    "duplicate JSON key {key:?}"
                )));
            }
            let value = map.next_value::<StrictValue>()?;
            output.insert(key, value.0);
        }
        Ok(StrictValue(Value::Object(output)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Write, os::unix::fs::symlink};

    const TEST_GENERATION_ID: &str = "slot-00000000000000000001";
    const TEST_REMOTE_PREFIX: &str =
        "grpc-raw/v1/solana-mainnet/source-node-test/slot-00000000000000000001";

    fn receipt_bytes(manifest_hash: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "generation_id": TEST_GENERATION_ID,
            "remote_prefix": TEST_REMOTE_PREFIX,
            "manifest_key": format!("{TEST_REMOTE_PREFIX}/manifest.json"),
            "commit_key": format!("{TEST_REMOTE_PREFIX}/_COMMITTED"),
            "manifest_sha256": manifest_hash,
            "commit_sha256": "c".repeat(64),
            "manifest_version_id": "manifest-version",
            "commit_version_id": "commit-version",
            "file_count": 1,
            "total_bytes": 1,
            "verified_unix_secs": 1,
            "predecessor_manifest_sha256": null
        }))
        .unwrap()
    }

    #[test]
    fn strict_json_rejects_duplicate_keys_at_every_depth() {
        assert!(strict_json(br#"{"a":1,"a":2}"#).is_err());
        assert!(strict_json(br#"{"a":{"b":1,"b":2}}"#).is_err());
        assert!(strict_json(br#"{"a":[{"b":1,"b":2}]}"#).is_err());
        assert!(strict_json(br#"{"a":1,"b":[true,null,"x"]}"#).is_ok());
    }

    #[test]
    fn control_integer_fields_reject_bools_floats_negatives_and_overflow() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("usage.json");
        for value in [
            "true",
            "1.0",
            "-1",
            "9223372036854775808",
            "18446744073709551616",
        ] {
            fs::write(
                &path,
                format!(
                    "{{\"schema_version\":1,\"scope_complete\":true,\"total_stored_bytes\":{value}}}\n"
                ),
            )
            .unwrap();
            assert!(
                b2_usage_report_bytes(&path).is_err(),
                "accepted invalid integer value {value}"
            );
        }
        fs::write(
            &path,
            b"{\"schema_version\":1,\"scope_complete\":true,\"total_stored_bytes\":0}\n",
        )
        .unwrap();
        assert_eq!(b2_usage_report_bytes(&path).unwrap(), 0);
        fs::write(
            &path,
            format!(
                "{{\"schema_version\":1,\"scope_complete\":true,\"total_stored_bytes\":{MAX_SIGNED_REPORT_INTEGER}}}\n"
            ),
        )
        .unwrap();
        assert_eq!(
            b2_usage_report_bytes(&path).unwrap(),
            MAX_SIGNED_REPORT_INTEGER
        );
    }

    #[test]
    fn journal_id_parser_accepts_canonical_length_and_both_hex_cases() {
        assert_eq!(
            parse_journal_id("000102030405060708090A0B0C0D0E0F").unwrap(),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        );
        assert!(parse_journal_id("00").is_err());
        assert!(parse_journal_id("000102030405060708090a0b0c0d0e0g").is_err());
    }

    #[test]
    fn resume_event_id_and_parser_match_the_legacy_contract() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("event.json");
        let event_id = resume_coverage_event_id(100, 104, 104);
        fs::write(
            &path,
            format!(
                "{{\"event_id\":\"{event_id}\",\"schema_version\":1,\"requested_overlap_slot\":100,\"first_delivered_slot\":104,\"observed_later_slot\":104,\"written_unix_secs\":123}}\n"
            ),
        )
        .unwrap();
        let parsed = parse_resume_coverage_event(&path).unwrap();
        assert_eq!(parsed.event_id, event_id);
        assert_eq!(parsed.requested_slot, 100);
        assert_eq!(parsed.first_slot, 104);
        assert_eq!(parsed.observed_slot, 104);
    }

    #[test]
    fn bounded_reader_rejects_symlinks_and_oversize() {
        let temporary = tempfile::tempdir().unwrap();
        let target = temporary.path().join("target");
        let linked = temporary.path().join("linked");
        fs::write(&target, b"{}").unwrap();
        symlink(&target, &linked).unwrap();
        assert!(read_bounded_regular(&linked, 10, "fixture").is_err());
        fs::write(&target, vec![b'x'; 11]).unwrap();
        assert!(read_bounded_regular(&target, 10, "fixture").is_err());
    }

    #[test]
    fn bounded_reader_detects_same_length_mutation_with_restored_mtime() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("fixture");
        fs::write(&path, b"original").unwrap();
        let file = open_regular_nofollow(&path, "fixture").unwrap();
        let result = read_bounded_open_file_after_snapshot(file, 64, "fixture", |before| {
            thread::sleep(Duration::from_millis(5));
            let mut writer = File::options().write(true).open(&path)?;
            writer.write_all(b"mutated!")?;
            writer.flush()?;
            let times = [
                libc::timespec {
                    tv_sec: before.atime(),
                    tv_nsec: before.atime_nsec() as _,
                },
                libc::timespec {
                    tv_sec: before.mtime(),
                    tv_nsec: before.mtime_nsec() as _,
                },
            ];
            let status = unsafe { libc::futimens(writer.as_raw_fd(), times.as_ptr()) };
            ensure!(status == 0, "restore fixture timestamps");
            Ok(())
        });
        assert!(
            result.is_err(),
            "same-size content mutation with restored mtime was accepted"
        );
    }

    #[test]
    fn receipt_validation_and_hash_stay_bound_to_one_open_inode() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("receipt.json");
        let moved = temporary.path().join("opened-receipt.json");
        let original_hash = "a".repeat(64);
        let replacement_hash = "b".repeat(64);
        fs::write(&path, receipt_bytes(&original_hash)).unwrap();

        let opened = open_regular_nofollow(&path, "generation receipt").unwrap();
        fs::rename(&path, &moved).unwrap();
        fs::write(&path, receipt_bytes(&replacement_hash)).unwrap();

        let validated =
            validate_generation_receipt_file(opened, TEST_GENERATION_ID, TEST_REMOTE_PREFIX, "-")
                .unwrap();
        assert_eq!(validated.manifest_hash, original_hash);
        assert_eq!(
            validate_generation_receipt(&path, TEST_GENERATION_ID, TEST_REMOTE_PREFIX, "-")
                .unwrap()
                .manifest_hash,
            replacement_hash
        );
    }

    #[test]
    fn directory_enumeration_is_bounded_before_the_next_name() {
        let temporary = tempfile::tempdir().unwrap();
        for name in ["one", "two", "three"] {
            fs::write(temporary.path().join(name), b"x").unwrap();
        }
        let directory = open_directory_nofollow(temporary.path(), "fixture directory").unwrap();
        assert!(directory_entry_names(&directory, 2).is_err());
        assert!(directory_entry_names(&directory, 0).is_err());
    }

    #[test]
    fn duplicated_directory_descriptor_is_close_on_exec() {
        let temporary = tempfile::tempdir().unwrap();
        let directory = open_directory_nofollow(temporary.path(), "fixture directory").unwrap();
        let duplicate = duplicate_cloexec(directory.as_raw_fd()).unwrap();
        let flags = unsafe { libc::fcntl(duplicate.as_raw_fd(), libc::F_GETFD) };
        assert!(flags >= 0);
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
    }

    #[test]
    fn inherited_flock_is_retained_by_the_original_descriptor() {
        use std::os::fd::AsRawFd;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lock");
        let temporary = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        flock_lock_fd(temporary.as_raw_fd(), "0").unwrap();
        let separately_opened = File::options().read(true).write(true).open(&path).unwrap();
        let result =
            unsafe { libc::flock(separately_opened.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_ne!(
            result, 0,
            "a separately opened description bypassed the inherited lock"
        );
        flock_unlock_fd(temporary.as_raw_fd()).unwrap();
        let result =
            unsafe { libc::flock(separately_opened.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(result, 0, "unlock was not visible to the other description");
    }

    #[test]
    fn directory_operations_stay_bound_to_the_opened_inode() {
        let temporary = tempfile::tempdir().unwrap();
        let original = temporary.path().join("receipts");
        let moved = temporary.path().join("receipts-opened");
        let replacement = temporary.path().join("replacement");
        fs::create_dir(&original).unwrap();
        fs::write(original.join(".chain"), b"opened\n").unwrap();
        fs::create_dir(&replacement).unwrap();
        fs::write(replacement.join(".chain"), b"replacement\n").unwrap();

        let directory = open_directory_nofollow(&original, "fixture directory").unwrap();
        fs::rename(&original, &moved).unwrap();
        symlink(&replacement, &original).unwrap();

        let names = directory_entry_names(&directory, 16).unwrap();
        assert!(names.iter().any(|name| name == ".chain"));
        let file = openat_regular_nofollow(&directory, ".chain", "fixture chain").unwrap();
        assert_eq!(
            read_bounded_open_file(file, 64, "fixture chain").unwrap(),
            b"opened\n"
        );
        assert!(open_directory_nofollow(&original, "swapped fixture directory").is_err());
    }
}
