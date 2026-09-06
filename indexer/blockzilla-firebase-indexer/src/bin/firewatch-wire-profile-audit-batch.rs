//! Durable, resource-aware supervisor for direct Archive V2 wire-profile audits.
//!
//! The immutable manifest is the only work authority. This process holds the
//! Firewatch controller lock for its full lifetime, gives the archive scheduler
//! and known manual tools priority, and runs at most two pinned audit process
//! groups. The first selected grammar can advance only after the auditor's
//! dedicated selected-profile-rejected exit code.

#[path = "../firewatch_controller_cgroup.rs"]
mod firewatch_controller_cgroup;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::{
            fs::{DirBuilderExt, FileExt, MetadataExt, OpenOptionsExt, PermissionsExt},
            process::{CommandExt, ExitStatusExt},
        },
    },
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SIGNATURES_FILE,
};
#[cfg(test)]
use blockzilla_firebase_indexer::firewatch_wire_profile_attestation::encode_receipt_source_recovery_evidence_v3;
use blockzilla_firebase_indexer::{
    firewatch_wire_profile_attestation::{
        DIRECT_ATTESTATION_GENERATION_KIND, RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND,
        WireProfileAttestation, is_sha256, validate_receipt_source_recovery_evidence,
        validate_wire_profile_attestation_structure,
    },
    format::RegistryFileIdentity,
};
use blockzilla_read_sdk_legacy::{
    ArchiveV2WireProfile, wire_profile_marker, wire_profile_marker_bytes,
};
use clap::Parser;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use firewatch_controller_cgroup::{
    CgroupMemoryEvents, CgroupMemorySnapshot, read_cgroup_memory, resolve_self_cgroup_v2,
};

const MANIFEST_SCHEMA_VERSION: u32 = 1;
const STATE_SCHEMA_VERSION: u32 = 1;
const EVENT_SCHEMA_VERSION: u32 = 1;
const ATTEMPT_RECEIPT_SCHEMA_VERSION: u32 = 1;
const TERMINAL_RECEIPT_SCHEMA_VERSION: u32 = 1;
const MAX_CONTROL_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ATTESTATION_BYTES: u64 = 64 * 1024;
const MAX_REGISTRY_RECEIPT_BYTES: u64 = 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;
const SELECTED_PROFILE_DECODE_REJECTED_EXIT_CODE: i32 = 20;
const TERMINAL_PROFILE_AUDIT_REJECTED_EXIT_CODE: i32 = 21;
const MAX_OPERATIONAL_RETRIES: u32 = 3;
const MIN_CGROUP_HEADROOM_PER_WORKER: u64 = 512 * MIB;
const DIRECT_GENERATION_DOMAIN: &[u8] = b"blockzilla.firewatch.direct-generation.v1\0";
const REGISTRY_GENERATION_DOMAIN: &[u8] = b"blockzilla.registry-reprocess.generation.v1";
const REGISTRY_RECEIPT_FILE: &str = "archive-v2-registry-reprocess.receipt.json";
const RECEIPT_SOURCE_PROFILE_AUTHORITY: &str = "registry_receipt_source_dual_audit";
const REQUIRED_YIELD_EXECUTABLE_PATHS: [&str; 3] = [
    "/volume1/blockzilla/bin/blockzilla-firebase-indexer",
    "/volume1/blockzilla/bin/index-parity",
    "/volume1/blockzilla/bin/blockzilla-index-archive-convert.new",
];
const REQUIRED_FORBIDDEN_EXECUTABLE_PATH: &str =
    "/volume1/blockzilla/bin/blockzilla-index-archive-convert";
const DIRECT_SEMANTIC_FILES: [&str; 6] = [
    ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SIGNATURES_FILE,
];
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Parser)]
#[command(
    name = "firewatch-wire-profile-audit-batch",
    about = "Run one immutable Firewatch wire-profile audit batch"
)]
struct Args {
    /// Immutable schema-1 batch manifest.
    #[arg(long)]
    manifest: PathBuf,
    /// Required lowercase SHA-256 of the exact manifest bytes.
    #[arg(long)]
    manifest_sha256: String,
    /// Private durable state root for this exact batch.
    #[arg(long)]
    state_root: PathBuf,
    /// Existing Firewatch controller root whose exclusive lock must remain held.
    #[arg(long)]
    controller_state_root: PathBuf,
    /// Read-only loopback scheduler status endpoint.
    #[arg(long, default_value = "http://127.0.0.1:8786/api/v1/status")]
    scheduler_status_url: String,
    /// Execute children and write durable batch state. Without this, validate only.
    #[arg(long)]
    execute: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BatchManifest {
    schema_version: u32,
    kind: String,
    cluster_id: String,
    batch_instance_id: String,
    source_status_sha256: String,
    audit_binary: ManifestExecutable,
    attestation_root: PathBuf,
    limits: BatchLimits,
    yield_executables: Vec<ManifestExecutable>,
    forbidden_executable_paths: Vec<PathBuf>,
    tasks: Vec<ManifestTask>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestExecutable {
    path: PathBuf,
    sha256: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BatchLimits {
    max_workers: usize,
    poll_secs: u64,
    resume_stable_secs: u64,
    disk_reserve_gib: u64,
    memory_resume_mib: u64,
    memory_hard_floor_mib: u64,
    cgroup_memory_high_mib: u64,
    cgroup_memory_max_mib: u64,
    memory_psi_resume_max_pct: f64,
    io_pause_full_pct: f64,
    io_pause_polls: u32,
    io_resume_full_pct: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestTask {
    task_id: String,
    epoch: u64,
    archive: PathBuf,
    registry_order: String,
    generation_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    registry_receipt: Option<PathBuf>,
    /// Immutable receipt and live source identities for a receipt-source task.
    /// The receipt content digest binds the source file hashes; the identities
    /// prevent a same-content inode replacement from changing the admitted task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    registry_receipt_source_binding: Option<ManifestReceiptSourceBinding>,
    profile_authority: String,
    content_generation_sha256: String,
    profile_attempts: [ArchiveV2WireProfile; 2],
    expected_attestation: PathBuf,
    archive_blocks_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestReceiptSourceBinding {
    receipt_sha256: String,
    receipt_identity: RegistryFileIdentity,
    source_files: BTreeMap<String, RegistryFileIdentity>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RegistryReceipt {
    version: u32,
    algorithm: String,
    epoch: u64,
    source_dir: String,
    target_dir: String,
    source_generation_sha256: String,
    target_generation_sha256: String,
    source_files: BTreeMap<String, RegistryFileBinding>,
    target_files: BTreeMap<String, RegistryFileBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    wire_profile: Option<ArchiveV2WireProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RegistryFileBinding {
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReceiptSourceCapture {
    receipt_sha256: String,
    receipt_identity: RegistryFileIdentity,
    source_files: BTreeMap<String, RegistryFileIdentity>,
    source_generation_sha256: String,
    archive_blocks_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TaskStatus {
    Pending,
    Starting,
    Running,
    Paused,
    RetryWait,
    Complete,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchState {
    schema_version: u32,
    kind: String,
    manifest_sha256: String,
    status: String,
    next_event_seq: u64,
    pinned_audit_binary: PinnedExecutable,
    public_audit_binary: PinnedExecutable,
    pinned_yield_executables: Vec<PinnedExecutable>,
    cgroup: CgroupBinding,
    cgroup_hard_event_baseline: CgroupEventRecord,
    updated_unix_secs: u64,
    tasks: BTreeMap<String, TaskState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskState {
    status: TaskStatus,
    profile_index: u8,
    operational_retries: u32,
    retry_not_before_unix_secs: Option<u64>,
    attempt: Option<AttemptState>,
    last_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttemptState {
    attempt_id: String,
    profile: ArchiveV2WireProfile,
    executable: PathBuf,
    executable_device: u64,
    executable_inode: u64,
    argv: Vec<String>,
    pid: Option<u32>,
    process_start_ticks: Option<u64>,
    pgid: Option<u32>,
    log: PathBuf,
    started_unix_secs: u64,
    paused: bool,
    pause_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PinnedExecutable {
    path: PathBuf,
    device: u64,
    inode: u64,
    size: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
    sha256: String,
}

#[derive(Debug)]
struct PinnedControlEvidence {
    path: PathBuf,
    file: File,
    identity: RegistryFileIdentity,
    sha256: String,
    max_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CgroupBinding {
    path: PathBuf,
    device: u64,
    inode: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CgroupEventRecord {
    high: u64,
    max: u64,
    oom: u64,
    oom_kill: u64,
}

impl From<CgroupMemoryEvents> for CgroupEventRecord {
    fn from(value: CgroupMemoryEvents) -> Self {
        Self {
            high: value.high,
            max: value.max,
            oom: value.oom,
            oom_kill: value.oom_kill,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableEvent {
    schema_version: u32,
    kind: String,
    manifest_sha256: String,
    sequence: u64,
    unix_secs: u64,
    task_id: Option<String>,
    action: String,
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttemptReceipt {
    schema_version: u32,
    kind: String,
    manifest_sha256: String,
    task_id: String,
    attempt_id: String,
    profile: ArchiveV2WireProfile,
    pid: Option<u32>,
    process_start_ticks: Option<u64>,
    pgid: Option<u32>,
    executable_device: u64,
    executable_inode: u64,
    executable_sha256: String,
    argv: Vec<String>,
    started_unix_secs: u64,
    finished_unix_secs: u64,
    outcome: String,
    exit_code: Option<i32>,
    signal: Option<i32>,
    log: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalReceipt {
    schema_version: u32,
    kind: String,
    manifest_sha256: String,
    task_id: String,
    epoch: u64,
    status: TaskStatus,
    profile: ArchiveV2WireProfile,
    expected_attestation: PathBuf,
    attestation_sha256: Option<String>,
    unix_secs: u64,
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerStatus {
    schema_version: u32,
    sequence: u64,
    now_unix_secs: u64,
    control_reconciled_unix_secs: u64,
    observer_mode: bool,
    scheduler: SchedulerControl,
    machine: SchedulerMachine,
    inventory: SchedulerInventory,
    summary: SchedulerSummary,
    scan_sweep: SchedulerScanSweep,
    finalizer_queue: Vec<SchedulerFinalizerQueueItem>,
    lanes: Vec<SchedulerLane>,
    live: Vec<SchedulerLiveItem>,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerControl {
    paused: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerMachine {
    memory_available_bytes: u64,
    disk_available_bytes: u64,
    io_pressure_full_avg10: Option<f64>,
    memory_pressure_some_avg10: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerInventory {
    complete: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerSummary {
    queued: usize,
    scanning: usize,
    finalizing: usize,
    legacy_compact_active: usize,
    scan_ready: usize,
    poh_migration_epochs_runnable: usize,
    poh_migration_running: usize,
    registry_reprocess_epochs_runnable: usize,
    registry_reprocess_audits_runnable: usize,
    registry_reprocess_running: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerScanSweep {
    complete: bool,
    pending: usize,
    active: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerLane {
    state: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerFinalizerQueueItem {
    state: String,
    #[allow(dead_code)]
    deferred_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerLiveItem {
    state: String,
}

#[derive(Debug, Deserialize)]
struct SourceControllerStatus {
    schema_version: u32,
    running: u32,
    rows: Vec<SourceStatusRow>,
}

#[derive(Debug, Deserialize)]
struct SourceStatusRow {
    epoch: u64,
    state: String,
    phase: String,
    #[serde(default)]
    wire_profile_audit_inputs: Vec<SourceAuditInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceAuditInput {
    archive: PathBuf,
    registry_order: String,
    generation_kind: String,
    content_generation_sha256: String,
}

struct ActiveAttempt {
    child: Option<Child>,
    pid: u32,
    start_ticks: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitDisposition {
    AdvanceProfile,
    TerminalBlocked,
    RetrySameProfile,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let (manifest, manifest_sha256) = load_and_validate_manifest(&args)?;
    if !args.execute {
        let guard = run_validation_probe(&args, &manifest, &manifest_sha256)?;
        println!("manifest_sha256={manifest_sha256}");
        println!("tasks={}", manifest.tasks.len());
        println!("guard={}", guard.as_deref().unwrap_or("safe"));
        println!("mode=validate_only");
        return Ok(());
    }
    run(args, manifest, manifest_sha256)
}

// The supervisor implementation follows below. Its helpers are deliberately
// pure where possible so the fail-closed state machine has focused tests.

fn run_validation_probe(
    args: &Args,
    manifest: &BatchManifest,
    manifest_sha256: &str,
) -> Result<Option<String>> {
    let _lock = acquire_existing_controller_lock(&args.controller_state_root)?;
    validate_source_status(args, manifest)?;
    let cgroup_path = resolve_self_cgroup_v2().context("resolve validation-probe cgroup v2")?;
    let cgroup_metadata = fs::symlink_metadata(&cgroup_path)?;
    let cgroup_binding = CgroupBinding {
        path: cgroup_path,
        device: cgroup_metadata.dev(),
        inode: cgroup_metadata.ino(),
    };
    let cgroup = read_bound_cgroup(&cgroup_binding)?;
    validate_cgroup_limits(&cgroup, &manifest.limits)?;
    ensure!(
        cgroup_has_headroom(&cgroup, 1),
        "batch cgroup has insufficient headroom for one audit worker"
    );
    let public_audit = capture_manifest_executable(&manifest.audit_binary)?;
    let yields = capture_pinned_executables(&manifest.yield_executables)?;
    let state = initialize_state(
        manifest,
        manifest_sha256,
        public_audit.clone(),
        public_audit,
        yields,
        cgroup_binding,
        cgroup.events.into(),
    );
    println!("cgroup_current_bytes={}", cgroup.current_bytes);
    println!("cgroup_high_bytes={}", cgroup.high_bytes.unwrap());
    println!("cgroup_max_bytes={}", cgroup.max_bytes.unwrap());
    println!(
        "cgroup_headroom_worker_1={}",
        cgroup_has_headroom(&cgroup, 1)
    );
    println!(
        "cgroup_headroom_worker_2={}",
        cgroup_has_headroom(&cgroup, 2)
    );
    let external = verify_external_work_guard(manifest, &state, &BTreeMap::new())?;
    ensure!(
        external.is_none(),
        "external protected work is active: {}",
        external.unwrap_or_default()
    );
    let client = Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let scheduler = fetch_scheduler_status(&client, &args.scheduler_status_url)?;
    ensure!(
        admission_safe(&manifest.limits, &scheduler, &cgroup, 1),
        "scheduler or resource state is not safe for the first audit worker"
    );
    Ok(None)
}

fn load_and_validate_manifest(args: &Args) -> Result<(BatchManifest, String)> {
    ensure!(args.manifest.is_absolute(), "--manifest must be absolute");
    ensure!(
        args.state_root.is_absolute(),
        "--state-root must be absolute"
    );
    ensure!(
        args.controller_state_root.is_absolute(),
        "--controller-state-root must be absolute"
    );
    ensure!(
        is_sha256(&args.manifest_sha256),
        "--manifest-sha256 must be a lowercase SHA-256"
    );
    validate_scheduler_url(&args.scheduler_status_url)?;
    ensure!(
        fs::canonicalize(&args.manifest)? == args.manifest,
        "--manifest must be canonical"
    );
    let manifest_metadata = fs::symlink_metadata(&args.manifest)?;
    ensure!(
        manifest_metadata.file_type().is_file()
            && !manifest_metadata.file_type().is_symlink()
            && manifest_metadata.uid() == unsafe { libc::geteuid() }
            && manifest_metadata.permissions().mode() & 0o022 == 0,
        "manifest must be an euid-owned real file that is not group/other writable"
    );
    let (bytes, manifest_sha256) = read_bounded_pinned_bytes(&args.manifest, MAX_CONTROL_BYTES)?;
    ensure!(
        manifest_sha256 == args.manifest_sha256,
        "manifest SHA-256 differs from --manifest-sha256"
    );
    let manifest: BatchManifest =
        serde_json::from_slice(&bytes).context("decode batch manifest")?;
    validate_manifest(&manifest)?;
    validate_runner_layout(args, &manifest, &manifest_sha256)?;
    Ok((manifest, manifest_sha256))
}

fn validate_manifest(manifest: &BatchManifest) -> Result<()> {
    ensure!(
        manifest.schema_version == MANIFEST_SCHEMA_VERSION
            && manifest.kind == "firewatch_wire_profile_audit_batch"
            && manifest.cluster_id == "mainnet-beta",
        "batch manifest identity is invalid"
    );
    ensure!(
        manifest.batch_instance_id.len() == 32
            && manifest
                .batch_instance_id
                .bytes()
                .all(|byte| { byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) }),
        "batch_instance_id must be 32 lowercase hexadecimal characters"
    );
    ensure!(
        is_sha256(&manifest.source_status_sha256),
        "batch source status SHA-256 is invalid"
    );
    ensure!(
        is_sha256(&manifest.audit_binary.sha256),
        "audit binary SHA-256 is invalid"
    );
    validate_real_canonical_file(&manifest.audit_binary.path, "audit binary")?;
    ensure!(
        fs::metadata(&manifest.audit_binary.path)?
            .permissions()
            .mode()
            & 0o111
            != 0,
        "audit binary is not executable"
    );
    ensure!(
        sha256_file_pinned(&manifest.audit_binary.path)? == manifest.audit_binary.sha256,
        "audit binary hash differs from the immutable manifest"
    );
    validate_real_canonical_directory(&manifest.attestation_root, "attestation root")?;
    validate_limits(&manifest.limits)?;
    ensure!(!manifest.tasks.is_empty(), "batch manifest has no tasks");
    ensure!(
        manifest.tasks.len() <= 4_096,
        "batch manifest has too many tasks"
    );
    ensure!(
        manifest.yield_executables.len() == REQUIRED_YIELD_EXECUTABLE_PATHS.len()
            && manifest
                .yield_executables
                .iter()
                .map(|item| item.path.as_path())
                .eq(REQUIRED_YIELD_EXECUTABLE_PATHS.iter().map(Path::new)),
        "initial batch release requires the exact ordered indexer, parity, and converter.new yield paths"
    );
    ensure!(
        manifest.forbidden_executable_paths == [PathBuf::from(REQUIRED_FORBIDDEN_EXECUTABLE_PATH)],
        "initial batch release requires the exact absent legacy converter path"
    );

    let mut yield_paths = BTreeSet::new();
    for executable in &manifest.yield_executables {
        let path = &executable.path;
        ensure!(
            is_sha256(&executable.sha256),
            "yield executable SHA-256 is invalid: {}",
            path.display()
        );
        validate_real_canonical_file(path, "yield executable")?;
        ensure!(
            fs::metadata(path)?.permissions().mode() & 0o111 != 0,
            "yield executable is not executable: {}",
            path.display()
        );
        ensure!(
            yield_paths.insert(path.clone()),
            "duplicate yield executable {}",
            path.display()
        );
        ensure!(
            sha256_file_pinned(path)? == executable.sha256,
            "yield executable hash differs from manifest: {}",
            path.display()
        );
    }
    let mut forbidden_paths = BTreeSet::new();
    for path in &manifest.forbidden_executable_paths {
        validate_absent_executable_path(path)?;
        ensure!(
            forbidden_paths.insert(path.clone()),
            "duplicate forbidden executable path {}",
            path.display()
        );
        ensure!(
            !yield_paths.contains(path) && path != &manifest.audit_binary.path,
            "forbidden executable path conflicts with a pinned executable"
        );
    }

    let mut task_ids = BTreeSet::new();
    let mut task_identities = BTreeSet::new();
    let mut epochs = BTreeSet::new();
    let mut expected_attestations = BTreeSet::new();
    for task in &manifest.tasks {
        validate_manifest_task(task, &manifest.attestation_root)?;
        ensure!(
            task_ids.insert(task.task_id.clone()),
            "duplicate task id {}",
            task.task_id
        );
        ensure!(
            task_identities.insert((
                task.epoch,
                task.archive.clone(),
                task.content_generation_sha256.clone(),
            )),
            "duplicate exact task identity for epoch {}",
            task.epoch
        );
        ensure!(
            epochs.insert(task.epoch),
            "duplicate task epoch {}",
            task.epoch
        );
        ensure!(
            expected_attestations.insert(task.expected_attestation.clone()),
            "duplicate expected attestation {}",
            task.expected_attestation.display()
        );
    }
    Ok(())
}

fn validate_absent_executable_path(path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute(),
        "forbidden executable path is not absolute"
    );
    let parent = path
        .parent()
        .context("forbidden executable has no parent")?;
    validate_real_canonical_directory(parent, "forbidden executable parent")?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("forbidden executable has an invalid basename")?;
    ensure!(
        !name.is_empty() && !matches!(name, "." | ".."),
        "forbidden executable basename is invalid"
    );
    ensure!(
        fs::symlink_metadata(path).is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
        "forbidden executable path exists and requires a revised manifest: {}",
        path.display()
    );
    Ok(())
}

fn validate_runner_layout(
    args: &Args,
    manifest: &BatchManifest,
    manifest_sha256: &str,
) -> Result<()> {
    validate_owned_not_writable_directory(&args.controller_state_root, "controller state root")?;
    ensure!(
        manifest.attestation_root == args.controller_state_root.join("wire-profile-attestations"),
        "manifest attestation root differs from the controller-owned attestation root"
    );
    validate_owned_not_writable_directory(&manifest.attestation_root, "attestation root")?;
    ensure!(
        args.state_root
            == args
                .controller_state_root
                .join("wire-profile-audit-batches")
                .join(manifest_sha256),
        "state root is not the dedicated manifest-SHA batch path"
    );
    ensure!(
        !args.state_root.starts_with(&manifest.attestation_root)
            && !manifest.attestation_root.starts_with(&args.state_root),
        "batch state and attestation roots overlap"
    );
    Ok(())
}

fn validate_owned_not_writable_directory(path: &Path, label: &str) -> Result<()> {
    validate_real_canonical_directory(path, label)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() } && metadata.permissions().mode() & 0o022 == 0,
        "{label} is not euid-owned or is group/other writable"
    );
    Ok(())
}

fn validate_owned_control_file(path: &Path, label: &str) -> Result<()> {
    validate_real_canonical_file(path, label)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() } && metadata.permissions().mode() & 0o022 == 0,
        "{label} is not euid-owned or is group/other writable"
    );
    Ok(())
}

fn validate_limits(limits: &BatchLimits) -> Result<()> {
    ensure!(
        limits.max_workers == 2
            && limits.poll_secs == 5
            && limits.resume_stable_secs == 60
            && limits.disk_reserve_gib == 512
            && limits.memory_resume_mib == 4_096
            && limits.memory_hard_floor_mib == 3_072
            && limits.cgroup_memory_high_mib == 3_072
            && limits.cgroup_memory_max_mib == 4_096
            && limits.memory_psi_resume_max_pct == 1.0
            && limits.io_pause_full_pct == 40.0
            && limits.io_pause_polls == 2
            && limits.io_resume_full_pct == 8.0,
        "batch limits differ from the exact initial-release safety envelope"
    );
    ensure!(
        (1..=2).contains(&limits.max_workers),
        "max_workers must be between 1 and 2"
    );
    ensure!(
        (1..=60).contains(&limits.poll_secs),
        "poll_secs must be between 1 and 60"
    );
    ensure!(
        (1..=3_600).contains(&limits.resume_stable_secs),
        "resume_stable_secs must be between 1 and 3600"
    );
    ensure!(
        limits.resume_stable_secs >= limits.poll_secs.saturating_mul(2),
        "resume_stable_secs must include at least two poll intervals"
    );
    ensure!(limits.disk_reserve_gib > 0, "disk reserve must be positive");
    ensure!(
        limits.disk_reserve_gib.checked_mul(GIB).is_some()
            && limits.memory_resume_mib.checked_mul(MIB).is_some()
            && limits.memory_hard_floor_mib.checked_mul(MIB).is_some()
            && limits.cgroup_memory_high_mib.checked_mul(MIB).is_some()
            && limits.cgroup_memory_max_mib.checked_mul(MIB).is_some(),
        "batch byte limits overflow u64"
    );
    ensure!(
        limits.memory_resume_mib > limits.memory_hard_floor_mib && limits.memory_hard_floor_mib > 0,
        "memory resume limit must exceed the positive hard floor"
    );
    ensure!(
        limits.cgroup_memory_high_mib == 3_072 && limits.cgroup_memory_max_mib == 4_096,
        "initial batch release requires cgroup memory.high=3072 MiB and memory.max=4096 MiB"
    );
    for (label, value) in [
        (
            "memory_psi_resume_max_pct",
            limits.memory_psi_resume_max_pct,
        ),
        ("io_pause_full_pct", limits.io_pause_full_pct),
        ("io_resume_full_pct", limits.io_resume_full_pct),
    ] {
        ensure!(
            value.is_finite() && (0.0..=100.0).contains(&value),
            "{label} is outside 0..=100"
        );
    }
    ensure!(
        limits.io_resume_full_pct < limits.io_pause_full_pct,
        "I/O resume threshold must be below pause threshold"
    );
    ensure!(
        (1..=60).contains(&limits.io_pause_polls),
        "io_pause_polls must be between 1 and 60"
    );
    Ok(())
}

fn validate_manifest_task(task: &ManifestTask, attestation_root: &Path) -> Result<()> {
    ensure!(
        !task.task_id.is_empty()
            && task.task_id.len() <= 128
            && task.task_id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            }),
        "task id is invalid: {}",
        task.task_id
    );
    ensure!(
        matches!(task.registry_order.as_str(), "first_seen" | "usage_sorted"),
        "task registry order is invalid"
    );
    ensure!(
        is_sha256(&task.content_generation_sha256),
        "task content generation is invalid"
    );
    match task.generation_kind.as_str() {
        DIRECT_ATTESTATION_GENERATION_KIND => {
            ensure!(
                task.registry_receipt.is_none() && task.registry_receipt_source_binding.is_none(),
                "direct task must not contain registry receipt authority"
            );
            ensure!(
                task.profile_authority == "marker_free_dual_audit",
                "direct task profile authority must be marker_free_dual_audit"
            );
            ensure_dual_profile_attempts(task, false)?;
            validate_real_canonical_directory(&task.archive, "task archive")?;
            ensure_marker_free_archive(&task.archive)?;
            let (files, blocks_bytes) = capture_direct_generation_files(&task.archive)?;
            ensure!(
                blocks_bytes == task.archive_blocks_bytes,
                "task archive block bytes changed for {}",
                task.task_id
            );
            ensure!(
                direct_generation_digest(task.epoch, &task.registry_order, &files)
                    == task.content_generation_sha256,
                "task direct generation identity changed for {}",
                task.task_id
            );
        }
        RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND => {
            ensure!(
                task.registry_order == "first_seen",
                "receipt-source task must use first_seen registry order"
            );
            ensure!(
                task.profile_authority == RECEIPT_SOURCE_PROFILE_AUTHORITY,
                "receipt-source task profile authority is invalid"
            );
            ensure_dual_profile_attempts(task, true)?;
            let capture = capture_receipt_source_task(task)?;
            validate_receipt_source_task_binding(task, &capture)?;
        }
        _ => bail!(
            "batch task generation kind is not admitted: {}",
            task.generation_kind
        ),
    }
    ensure!(
        task.expected_attestation
            == attestation_root.join(format!(
                "epoch-{}-{}.json",
                task.epoch, task.content_generation_sha256
            )),
        "task expected attestation path is not exact for {}",
        task.task_id
    );
    ensure!(
        task.expected_attestation.is_absolute()
            && task.expected_attestation.parent() == Some(attestation_root),
        "task expected attestation escapes its root"
    );
    Ok(())
}

fn validate_receipt_source_task_binding(
    task: &ManifestTask,
    capture: &ReceiptSourceCapture,
) -> Result<()> {
    let binding = task
        .registry_receipt_source_binding
        .as_ref()
        .context("receipt-source task has no immutable source binding")?;
    ensure!(
        is_sha256(&binding.receipt_sha256)
            && capture.receipt_sha256 == binding.receipt_sha256
            && capture.receipt_identity == binding.receipt_identity
            && capture.source_files == binding.source_files
            && capture.source_generation_sha256 == task.content_generation_sha256
            && capture.archive_blocks_bytes == task.archive_blocks_bytes,
        "receipt-source task immutable identity or content binding changed for {}",
        task.task_id
    );
    Ok(())
}

fn ensure_dual_profile_attempts(task: &ManifestTask, post_first: bool) -> Result<()> {
    let valid = if post_first {
        task.profile_attempts
            == [
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            ]
    } else {
        matches!(
            task.profile_attempts,
            [
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            ] | [
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            ]
        )
    };
    ensure!(
        valid,
        if post_first {
            "receipt-source task must attempt Post and then Pre exactly"
        } else {
            "task profile attempts must contain each supported profile exactly once"
        }
    );
    Ok(())
}

fn validate_scheduler_url(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value).context("parse scheduler status URL")?;
    ensure!(url.scheme() == "http", "scheduler status URL must use HTTP");
    ensure!(
        url.host_str() == Some("127.0.0.1")
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none(),
        "scheduler status URL must be plain IPv4 loopback without user information or fragment"
    );
    Ok(())
}

fn validate_real_canonical_file(path: &Path, label: &str) -> Result<()> {
    ensure!(path.is_absolute(), "{label} path is not absolute");
    ensure!(
        fs::canonicalize(path)? == path,
        "{label} path is not canonical"
    );
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "{label} is not a real regular file"
    );
    Ok(())
}

fn validate_real_canonical_directory(path: &Path, label: &str) -> Result<()> {
    ensure!(path.is_absolute(), "{label} path is not absolute");
    ensure!(
        fs::canonicalize(path)? == path,
        "{label} path is not canonical"
    );
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "{label} is not a real directory"
    );
    Ok(())
}

fn ensure_marker_free_archive(archive: &Path) -> Result<()> {
    for profile in [
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
    ] {
        let marker = wire_profile_marker(profile);
        ensure!(
            marker.size == wire_profile_marker_bytes(profile).len() as u64,
            "SDK wire-profile marker definition is inconsistent"
        );
        match fs::symlink_metadata(archive.join(&marker.name)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => bail!(
                "marker-free batch task contains Archive V2 marker {}",
                marker.name
            ),
        }
    }
    Ok(())
}

fn capture_direct_generation_files(
    archive: &Path,
) -> Result<(BTreeMap<String, RegistryFileIdentity>, u64)> {
    validate_real_canonical_directory(archive, "direct archive")?;
    let mut files = BTreeMap::new();
    for name in DIRECT_SEMANTIC_FILES {
        let identity = capture_file_identity(&archive.join(name))?;
        if name != ARCHIVE_V2_SIGNATURES_FILE {
            ensure!(identity.size > 0, "direct archive input {name} is empty");
        }
        files.insert(name.into(), identity);
    }
    let blocks_bytes = files
        .get(ARCHIVE_V2_BLOCKS_FILE)
        .context("direct archive has no block payload")?
        .size;
    Ok((files, blocks_bytes))
}

fn capture_receipt_source_task(task: &ManifestTask) -> Result<ReceiptSourceCapture> {
    let receipt_path = task
        .registry_receipt
        .as_deref()
        .context("receipt-source task has no registry receipt")?;
    ensure!(
        receipt_path.is_absolute() && fs::canonicalize(receipt_path)? == receipt_path,
        "receipt-source task receipt path is not absolute and canonical"
    );
    let (receipt_bytes, receipt_sha256, receipt_identity) =
        read_secure_control_bytes(receipt_path, MAX_REGISTRY_RECEIPT_BYTES, "registry receipt")?;
    let receipt: RegistryReceipt =
        serde_json::from_slice(&receipt_bytes).context("decode registry receipt")?;
    ensure!(
        receipt.epoch == task.epoch
            && matches!(
                (receipt.version, receipt.algorithm.as_str()),
                (
                    1,
                    "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v1"
                ) | (
                    2,
                    "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v2"
                ) | (
                    3,
                    "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3"
                )
            ),
        "receipt-source task receipt provenance is invalid"
    );
    validate_receipt_source_profile_shape(&receipt)?;
    validate_registry_file_bindings(&receipt.source_files, "source")?;
    validate_registry_file_bindings(&receipt.target_files, "target")?;
    ensure!(
        registry_generation_digest(&receipt.source_files) == receipt.source_generation_sha256
            && registry_generation_digest(&receipt.target_files)
                == receipt.target_generation_sha256,
        "receipt-source task receipt generation digest is invalid"
    );

    let source = PathBuf::from(&receipt.source_dir);
    let target = PathBuf::from(&receipt.target_dir);
    ensure!(
        source.is_absolute()
            && target.is_absolute()
            && fs::canonicalize(&source)? == source
            && fs::canonicalize(&target)? == target,
        "receipt-source task receipt archive paths are not canonical"
    );
    validate_real_canonical_directory(&source, "registry receipt source")?;
    validate_real_canonical_directory(&target, "registry receipt target")?;
    ensure!(
        source == task.archive,
        "receipt source path differs from the exact task archive"
    );
    ensure!(
        receipt_path == target.join(REGISTRY_RECEIPT_FILE),
        "registry receipt is not at the exact target generation path"
    );

    let source_files = capture_receipt_source_identities(&source, &receipt.source_files)?;
    let archive_blocks_bytes = source_files
        .get(ARCHIVE_V2_BLOCKS_FILE)
        .context("receipt source has no block payload")?
        .size;
    ensure!(
        receipt.source_files.contains_key(ARCHIVE_V2_POH_FILE),
        "receipt source has no exact PoH binding"
    );
    Ok(ReceiptSourceCapture {
        receipt_sha256,
        receipt_identity,
        source_files,
        source_generation_sha256: receipt.source_generation_sha256,
        archive_blocks_bytes,
    })
}

fn validate_registry_file_bindings(
    files: &BTreeMap<String, RegistryFileBinding>,
    side: &str,
) -> Result<()> {
    ensure!(
        !files.is_empty(),
        "registry receipt {side} file map is empty"
    );
    for (name, binding) in files {
        ensure!(
            !name.is_empty()
                && Path::new(name).components().count() == 1
                && name != "."
                && name != ".."
                && is_sha256(&binding.sha256),
            "registry receipt {side} has an invalid file binding"
        );
    }
    Ok(())
}

fn validate_receipt_source_profile_shape(receipt: &RegistryReceipt) -> Result<()> {
    let pre = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
    let post = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
    let pre_marker = wire_profile_marker(pre);
    let post_marker = wire_profile_marker(post);
    let validate_side = |files: &BTreeMap<String, RegistryFileBinding>, required: bool| {
        ensure!(
            !files.contains_key(&pre_marker.name),
            "receipt-source task binds the Pre wire-profile marker"
        );
        let selected = files.get(&post_marker.name);
        ensure!(
            !required || selected.is_some(),
            "profile-bound registry receipt target omits the Post marker"
        );
        if let Some(binding) = selected {
            ensure!(
                binding.bytes == post_marker.size && binding.sha256 == post_marker.sha256,
                "registry receipt has a malformed Post wire-profile marker binding"
            );
        }
        Ok::<(), anyhow::Error>(())
    };
    match (receipt.version, receipt.wire_profile) {
        (1 | 2, None) | (3, None) => {
            ensure!(
                !receipt.source_files.contains_key(&pre_marker.name)
                    && !receipt.source_files.contains_key(&post_marker.name)
                    && !receipt.target_files.contains_key(&pre_marker.name)
                    && !receipt.target_files.contains_key(&post_marker.name),
                "profile-neutral registry receipt binds a wire-profile marker"
            );
        }
        (3, Some(profile)) => {
            ensure!(
                profile == post,
                "receipt-source task profile-bound receipt is not Post"
            );
            validate_side(&receipt.source_files, false)?;
            validate_side(&receipt.target_files, true)?;
        }
        _ => bail!("registry receipt version/profile combination is invalid"),
    }
    Ok(())
}

fn capture_receipt_source_identities(
    source: &Path,
    bindings: &BTreeMap<String, RegistryFileBinding>,
) -> Result<BTreeMap<String, RegistryFileIdentity>> {
    let mut identities = BTreeMap::new();
    for (name, binding) in bindings {
        let path = source.join(name);
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(&path)
            .with_context(|| format!("open registry receipt source file {name}"))?;
        let opened = file.metadata()?;
        let path_after = fs::symlink_metadata(&path)?;
        ensure!(
            opened.file_type().is_file()
                && opened.nlink() == 1
                && opened.uid() == unsafe { libc::geteuid() }
                && opened.permissions().mode() & 0o022 == 0
                && path_after.file_type().is_file()
                && !path_after.file_type().is_symlink()
                && same_file(&opened, &path_after)
                && same_version(&opened, &path_after)
                && path_after.nlink() == 1
                && path_after.uid() == unsafe { libc::geteuid() }
                && path_after.permissions().mode() & 0o022 == 0,
            "registry receipt source file is not one exact euid-owned protected nlink-1 regular file: {name}"
        );
        let identity = file_identity(&opened);
        ensure!(
            identity.size == binding.bytes,
            "registry receipt source file size differs for {name}"
        );
        identities.insert(name.clone(), identity);
    }
    Ok(identities)
}

fn registry_generation_digest(files: &BTreeMap<String, RegistryFileBinding>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(REGISTRY_GENERATION_DOMAIN);
    hasher.update((files.len() as u64).to_le_bytes());
    for (name, binding) in files {
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(binding.bytes.to_le_bytes());
        hasher.update(binding.sha256.as_bytes());
    }
    hex_digest(hasher.finalize())
}

fn read_pinned_control_fd_bytes(file: &File, limit: u64, label: &str) -> Result<Vec<u8>> {
    let capacity = limit
        .checked_add(1)
        .context("control byte limit overflow")?;
    let mut bytes = Vec::new();
    let mut offset = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    while (bytes.len() as u64) < capacity {
        let remaining = capacity - bytes.len() as u64;
        let read_limit = usize::try_from(remaining.min(buffer.len() as u64))?;
        let read = loop {
            match file.read_at(&mut buffer[..read_limit], offset) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                result => break result,
            }
        }
        .with_context(|| format!("read pinned {label}"))?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
        offset = offset
            .checked_add(read as u64)
            .context("pinned control read offset overflow")?;
    }
    ensure!(bytes.len() as u64 <= limit, "{label} is too large");
    Ok(bytes)
}

fn validate_pinned_control_metadata(
    metadata: &fs::Metadata,
    require_protected: bool,
    label: &str,
) -> Result<()> {
    ensure!(
        metadata.file_type().is_file(),
        "{label} is not a regular file"
    );
    if require_protected {
        ensure!(
            metadata.nlink() == 1
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o022 == 0,
            "{label} is not an euid-owned protected nlink-1 regular file"
        );
    }
    Ok(())
}

fn read_pinned_control_evidence(
    path: &Path,
    limit: u64,
    label: &str,
    require_protected: bool,
) -> Result<(Vec<u8>, PinnedControlEvidence)> {
    ensure!(limit < u64::MAX, "bounded {label} limit is invalid");
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let opened = file.metadata()?;
    validate_pinned_control_metadata(&opened, require_protected, label)?;
    ensure!(opened.len() <= limit, "{label} is too large");
    let identity = file_identity(&opened);
    let bytes = read_pinned_control_fd_bytes(&file, limit, label)?;
    let after = file.metadata()?;
    let path_after = fs::symlink_metadata(path)?;
    validate_pinned_control_metadata(&after, require_protected, label)?;
    validate_pinned_control_metadata(&path_after, require_protected, label)?;
    ensure!(
        !path_after.file_type().is_symlink()
            && file_identity(&after) == identity
            && file_identity(&path_after) == identity
            && after.len() == bytes.len() as u64,
        "{label} path or descriptor changed while reading"
    );
    let sha256 = hex_digest(Sha256::digest(&bytes));
    Ok((
        bytes,
        PinnedControlEvidence {
            path: path.to_path_buf(),
            file,
            identity,
            sha256,
            max_bytes: limit,
        },
    ))
}

impl PinnedControlEvidence {
    fn recheck(&self, require_protected: bool, label: &str) -> Result<()> {
        let descriptor_before = self.file.metadata()?;
        validate_pinned_control_metadata(&descriptor_before, require_protected, label)?;
        ensure!(
            file_identity(&descriptor_before) == self.identity,
            "{label} descriptor identity changed"
        );
        let path_before = fs::symlink_metadata(&self.path)?;
        validate_pinned_control_metadata(&path_before, require_protected, label)?;
        ensure!(
            !path_before.file_type().is_symlink() && file_identity(&path_before) == self.identity,
            "{label} path no longer names its pinned descriptor"
        );
        let bytes = read_pinned_control_fd_bytes(&self.file, self.max_bytes, label)?;
        ensure!(
            bytes.len() as u64 == self.identity.size
                && hex_digest(Sha256::digest(&bytes)) == self.sha256,
            "{label} descriptor content changed"
        );
        let descriptor_after = self.file.metadata()?;
        let path_after = fs::symlink_metadata(&self.path)?;
        validate_pinned_control_metadata(&descriptor_after, require_protected, label)?;
        validate_pinned_control_metadata(&path_after, require_protected, label)?;
        ensure!(
            !path_after.file_type().is_symlink()
                && file_identity(&descriptor_after) == self.identity
                && file_identity(&path_after) == self.identity,
            "{label} path or descriptor changed during final recheck"
        );
        Ok(())
    }
}

fn read_secure_control_bytes(
    path: &Path,
    limit: u64,
    label: &str,
) -> Result<(Vec<u8>, String, RegistryFileIdentity)> {
    let (bytes, evidence) = read_pinned_control_evidence(path, limit, label, true)?;
    evidence.recheck(true, label)?;
    Ok((bytes, evidence.sha256.clone(), evidence.identity.clone()))
}

fn capture_file_identity(path: &Path) -> Result<RegistryFileIdentity> {
    let before = fs::symlink_metadata(path)
        .with_context(|| format!("inspect task input {}", path.display()))?;
    ensure!(
        before.file_type().is_file() && !before.file_type().is_symlink(),
        "task input is not a real regular file: {}",
        path.display()
    );
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let opened = file.metadata()?;
    let after = fs::symlink_metadata(path)?;
    ensure!(
        same_file(&before, &opened)
            && same_version(&before, &opened)
            && same_file(&opened, &after)
            && same_version(&opened, &after),
        "task input changed during identity capture: {}",
        path.display()
    );
    Ok(file_identity(&opened))
}

fn file_identity(metadata: &fs::Metadata) -> RegistryFileIdentity {
    RegistryFileIdentity {
        size: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    }
}

fn direct_generation_digest(
    epoch: u64,
    registry_order: &str,
    files: &BTreeMap<String, RegistryFileIdentity>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DIRECT_GENERATION_DOMAIN);
    hasher.update(epoch.to_le_bytes());
    hasher.update((registry_order.len() as u64).to_le_bytes());
    hasher.update(registry_order.as_bytes());
    hasher.update((files.len() as u64).to_le_bytes());
    for (name, identity) in files {
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(identity.size.to_le_bytes());
        hasher.update(identity.device.to_le_bytes());
        hasher.update(identity.inode.to_le_bytes());
        hasher.update(identity.modified_seconds.to_le_bytes());
        hasher.update(identity.modified_nanoseconds.to_le_bytes());
        hasher.update(identity.changed_seconds.to_le_bytes());
        hasher.update(identity.changed_nanoseconds.to_le_bytes());
    }
    hex_digest(hasher.finalize())
}

fn run(args: Args, manifest: BatchManifest, manifest_sha256: String) -> Result<()> {
    install_shutdown_handlers()?;
    let _controller_lock = acquire_controller_lock(&args.controller_state_root)?;
    validate_source_status(&args, &manifest)?;
    create_batch_layout(&args.state_root)?;
    let (pinned_audit, public_audit) = pin_audit_binary(&args.state_root, &manifest.audit_binary)?;
    let pinned_yields = capture_pinned_executables(&manifest.yield_executables)?;
    let cgroup_path = resolve_self_cgroup_v2().context("resolve batch cgroup v2")?;
    let cgroup_metadata = fs::symlink_metadata(&cgroup_path)?;
    let cgroup_binding = CgroupBinding {
        path: cgroup_path.clone(),
        device: cgroup_metadata.dev(),
        inode: cgroup_metadata.ino(),
    };
    let state_path = args.state_root.join("state.json");
    let existing_state = read_optional_json::<BatchState>(&state_path)?;
    let initial_cgroup = if existing_state.is_none() {
        let snapshot = read_cgroup_memory(&cgroup_path)?;
        validate_cgroup_limits(&snapshot, &manifest.limits)?;
        ensure!(
            cgroup_has_headroom(&snapshot, 1),
            "batch cgroup has insufficient headroom for the first audit worker"
        );
        Some(snapshot)
    } else {
        None
    };
    let mut state = match existing_state {
        Some(state) => {
            validate_existing_state(
                &args,
                &state,
                &manifest,
                &manifest_sha256,
                &pinned_audit,
                &public_audit,
                &pinned_yields,
                &cgroup_binding,
            )?;
            state
        }
        None => initialize_state(
            &manifest,
            &manifest_sha256,
            pinned_audit.clone(),
            public_audit.clone(),
            pinned_yields.clone(),
            cgroup_binding.clone(),
            initial_cgroup
                .expect("new state has initial cgroup")
                .events
                .into(),
        ),
    };
    reconcile_event_sequence(&args.state_root, &mut state)?;
    let task_map = task_map_from_manifest(&manifest);
    let discovered = discover_state_attempt_processes(&state, &pinned_audit)?;
    let discovered_task_ids = discovered.keys().cloned().collect::<Vec<_>>();
    recover_and_terminate_discovered_attempts(
        &args,
        &manifest_sha256,
        &task_map,
        &mut state,
        discovered,
    )?;
    let mut active = BTreeMap::new();
    replay_current_attempt_receipts(&args, &manifest_sha256, &task_map, &mut state)?;
    for task_id in discovered_task_ids {
        ensure!(
            state.tasks[&task_id].attempt.is_none(),
            "durable interruption intent did not replay for recovered audit"
        );
    }
    publish_json_atomic(&state_path, &state)?;
    let client = Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let current_cgroup = read_bound_cgroup(&state.cgroup)?;
    validate_cgroup_limits(&current_cgroup, &manifest.limits)?;
    if cgroup_hard_event_changed(
        state.cgroup_hard_event_baseline,
        current_cgroup.events.into(),
    ) {
        interrupt_all_active(
            &args,
            &manifest_sha256,
            &task_map,
            &mut state,
            &mut active,
            "batch cgroup hard-event counters changed while offline",
        )?;
        block_nonterminal_tasks(
            &args,
            &manifest_sha256,
            &task_map,
            &mut state,
            "batch cgroup hard-event counters changed while offline",
        )?;
        state.status = "blocked".into();
        publish_json_atomic(&state_path, &state)?;
        return Ok(());
    }
    let owned = active
        .iter()
        .map(|(task_id, attempt)| (task_id.clone(), (attempt.pid, attempt.start_ticks)))
        .collect();
    verify_external_work_guard(&manifest, &state, &owned)?;
    replay_terminal_receipts(&args, &manifest_sha256, &task_map, &mut state)?;
    reconcile_all_attestations(&args, &manifest_sha256, &task_map, &mut state, &mut active)?;
    replay_current_attempt_receipts(&args, &manifest_sha256, &task_map, &mut state)?;
    reconcile_absent_attempts(&args, &manifest_sha256, &task_map, &mut state, &active)?;
    validate_attempt_receipt_wal(&args, &manifest_sha256, &task_map, &state)?;
    validate_terminal_state_receipts(&args, &manifest_sha256, &task_map, &state)?;
    publish_json_atomic(&state_path, &state)?;

    let loop_result = (|| -> Result<()> {
        let mut admission_safe_since: Option<Instant> = None;
        let mut resume_safe_since: Option<Instant> = None;
        let mut io_high_polls = 0u32;
        loop {
            if all_tasks_terminal(&state) {
                state.status = if state
                    .tasks
                    .values()
                    .any(|task| task.status == TaskStatus::Blocked)
                {
                    "blocked".into()
                } else {
                    "complete".into()
                };
                state.updated_unix_secs = unix_now();
                publish_json_atomic(&state_path, &state)?;
                return Ok(());
            }

            // An exact attestation and an owned exit status are stronger evidence
            // than every admission guard. Harvest them before a broad /proc scan can
            // observe a just-exited child as missing.
            reconcile_all_attestations(
                &args,
                &manifest_sha256,
                &task_map,
                &mut state,
                &mut active,
            )?;
            reap_finished_attempts(
                &args,
                &manifest,
                &manifest_sha256,
                &task_map,
                &mut state,
                &mut active,
            )?;

            let current_cgroup = read_bound_cgroup(&state.cgroup)?;
            validate_cgroup_limits(&current_cgroup, &manifest.limits)?;
            if cgroup_hard_event_changed(
                state.cgroup_hard_event_baseline,
                current_cgroup.events.into(),
            ) {
                interrupt_all_active(
                    &args,
                    &manifest_sha256,
                    &task_map,
                    &mut state,
                    &mut active,
                    "batch cgroup reached a hard memory event",
                )?;
                block_nonterminal_tasks(
                    &args,
                    &manifest_sha256,
                    &task_map,
                    &mut state,
                    "batch cgroup hard-event counters changed",
                )?;
                state.status = "blocked".into();
                publish_json_atomic(&state_path, &state)?;
                return Ok(());
            }

            if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
                shutdown_all_active(
                    &args,
                    &manifest_sha256,
                    &task_map,
                    &mut state,
                    &mut active,
                    "batch supervisor shutdown",
                )?;
                state.status = "stopped".into();
                state.updated_unix_secs = unix_now();
                publish_json_atomic(&state_path, &state)?;
                return Ok(());
            }

            verify_pinned_files(&manifest, &state)?;
            let owned = active
                .iter()
                .map(|(task_id, attempt)| (task_id.clone(), (attempt.pid, attempt.start_ticks)))
                .collect::<BTreeMap<_, _>>();
            let external_reason = verify_external_work_guard(&manifest, &state, &owned)?;
            let scheduler = fetch_scheduler_status(&client, &args.scheduler_status_url);
            let running_count = active
                .keys()
                .filter(|task_id| {
                    state.tasks[*task_id]
                        .attempt
                        .as_ref()
                        .is_some_and(|attempt| !attempt.paused)
                })
                .count();
            let no_external_work = external_reason.is_none();
            let strict_resume_safe = no_external_work
                && scheduler
                    .as_ref()
                    .is_ok_and(|status| resume_safe(&manifest.limits, status));
            if strict_resume_safe {
                resume_safe_since.get_or_insert_with(Instant::now);
            } else {
                resume_safe_since = None;
            }
            let strict_admission_safe = active.len() < manifest.limits.max_workers
                && no_external_work
                && scheduler.as_ref().is_ok_and(|status| {
                    admission_safe(&manifest.limits, status, &current_cgroup, active.len() + 1)
                });
            if strict_admission_safe {
                admission_safe_since.get_or_insert_with(Instant::now);
            } else {
                admission_safe_since = None;
            }

            let gate = resource_gate(
                &manifest.limits,
                scheduler.as_ref().map_err(|error| format!("{error:#}")),
                external_reason,
                &current_cgroup,
                &mut io_high_polls,
                running_count > 0,
            );
            match gate {
                ResourceGate::Interrupt(ref reason) => {
                    interrupt_all_active(
                        &args,
                        &manifest_sha256,
                        &task_map,
                        &mut state,
                        &mut active,
                        reason,
                    )?;
                    admission_safe_since = None;
                    resume_safe_since = None;
                }
                ResourceGate::Pause(ref reason) => {
                    pause_all_active(&args, &mut state, &mut active, reason)?;
                    state.status = "paused".into();
                    admission_safe_since = None;
                    resume_safe_since = None;
                }
                ResourceGate::Safe => {
                    let strict_stable = resume_safe_since.is_some_and(|since| {
                        since.elapsed() >= Duration::from_secs(manifest.limits.resume_stable_secs)
                    });
                    if strict_stable {
                        resume_all_active(&args, &mut state, &mut active)?;
                        state.status = "running".into();
                    } else if running_count > 0 {
                        state.status = "running".into();
                    } else {
                        state.status = "waiting_for_stable_resources".into();
                    }
                }
            }

            if matches!(gate, ResourceGate::Safe)
                && admission_safe_since.is_some_and(|since| {
                    since.elapsed() >= Duration::from_secs(manifest.limits.resume_stable_secs)
                })
            {
                while active.len() < manifest.limits.max_workers {
                    verify_pinned_files(&manifest, &state)?;
                    let owned = active
                        .iter()
                        .map(|(task_id, attempt)| {
                            (task_id.clone(), (attempt.pid, attempt.start_ticks))
                        })
                        .collect::<BTreeMap<_, _>>();
                    if verify_external_work_guard(&manifest, &state, &owned)?.is_some() {
                        admission_safe_since = None;
                        break;
                    }
                    let fresh_scheduler =
                        fetch_scheduler_status(&client, &args.scheduler_status_url)?;
                    let fresh_cgroup = read_bound_cgroup(&state.cgroup)?;
                    if !admission_safe(
                        &manifest.limits,
                        &fresh_scheduler,
                        &fresh_cgroup,
                        active.len() + 1,
                    ) {
                        admission_safe_since = None;
                        break;
                    }
                    let Some(task_id) = next_runnable_task(&manifest, &state) else {
                        break;
                    };
                    let task = task_map
                        .get(&task_id)
                        .copied()
                        .context("state references an unknown task")?;
                    validate_manifest_task(task, &manifest.attestation_root)?;
                    if let Some((attestation_sha256, profile)) = validate_exact_attestation(task)? {
                        mark_complete(
                            &args,
                            &manifest_sha256,
                            task,
                            &mut state,
                            attestation_sha256,
                            profile,
                            "exact attestation already exists",
                        )?;
                        continue;
                    }
                    spawn_attempt(
                        &args,
                        &manifest,
                        &manifest_sha256,
                        task,
                        &mut state,
                        &client,
                        &mut active,
                    )?;
                    admission_safe_since = None;
                    break;
                }
            }

            state.updated_unix_secs = unix_now();
            publish_json_atomic(&state_path, &state)?;
            interruptible_sleep(Duration::from_secs(manifest.limits.poll_secs));
        }
    })();

    if let Err(error) = loop_result {
        let reason = bounded_message(&format!("batch supervisor loop failed: {error:#}"));
        if let Err(cleanup_error) = interrupt_all_active(
            &args,
            &manifest_sha256,
            &task_map,
            &mut state,
            &mut active,
            &reason,
        ) {
            ensure!(
                active.is_empty(),
                "active audits remained after fail-closed cleanup"
            );
            state.status = "stopped_after_error".into();
            state.updated_unix_secs = unix_now();
            let snapshot_error = publish_json_atomic(&state_path, &state).err();
            return Err(error.context(format!(
                "durable active-audit cleanup also failed: {cleanup_error:#}{}",
                snapshot_error
                    .map(|error| format!("; final nonterminal snapshot failed: {error:#}"))
                    .unwrap_or_default()
            )));
        }
        state.status = "stopped_after_error".into();
        state.updated_unix_secs = unix_now();
        if let Err(snapshot_error) = publish_json_atomic(&state_path, &state) {
            return Err(error.context(format!(
                "publish stopped-after-error state failed: {snapshot_error:#}"
            )));
        }
        return Err(error);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
enum ResourceGate {
    Safe,
    Pause(String),
    Interrupt(String),
}

fn resource_gate(
    limits: &BatchLimits,
    scheduler: Result<&SchedulerStatus, String>,
    external_reason: Option<String>,
    _cgroup: &CgroupMemorySnapshot,
    io_high_polls: &mut u32,
    active: bool,
) -> ResourceGate {
    let status = match scheduler {
        Ok(status) => status,
        Err(error) => return ResourceGate::Pause(format!("scheduler status unavailable: {error}")),
    };
    if status.machine.memory_available_bytes < limits.memory_hard_floor_mib * MIB {
        return ResourceGate::Interrupt("host memory is below the hard floor".into());
    }
    if status.machine.disk_available_bytes < limits.disk_reserve_gib * GIB {
        return ResourceGate::Pause("archive disk is below the reserved free space".into());
    }
    if let Some(reason) = external_reason {
        return ResourceGate::Pause(reason);
    }
    if scheduler_has_priority_work(status) {
        return ResourceGate::Pause("archive scheduler has priority work".into());
    }
    let io = status
        .machine
        .io_pressure_full_avg10
        .unwrap_or(f64::INFINITY);
    if io >= limits.io_pause_full_pct {
        *io_high_polls = io_high_polls.saturating_add(1);
    } else {
        *io_high_polls = 0;
    }
    if active && *io_high_polls >= limits.io_pause_polls {
        return ResourceGate::Pause(format!("host I/O full pressure is {io:.2}%"));
    }
    if !active
        && (status.machine.memory_available_bytes < limits.memory_resume_mib * MIB
            || status
                .machine
                .memory_pressure_some_avg10
                .unwrap_or(f64::INFINITY)
                > limits.memory_psi_resume_max_pct
            || io > limits.io_resume_full_pct)
    {
        return ResourceGate::Pause("resource headroom is below the resume threshold".into());
    }
    ResourceGate::Safe
}

fn admission_safe(
    limits: &BatchLimits,
    scheduler: &SchedulerStatus,
    cgroup: &CgroupMemorySnapshot,
    worker_count: usize,
) -> bool {
    !scheduler_has_priority_work(scheduler)
        && scheduler.machine.memory_available_bytes >= limits.memory_resume_mib * MIB
        && scheduler
            .machine
            .memory_pressure_some_avg10
            .is_some_and(|value| value <= limits.memory_psi_resume_max_pct)
        && scheduler
            .machine
            .io_pressure_full_avg10
            .is_some_and(|value| value <= limits.io_resume_full_pct)
        && scheduler.machine.disk_available_bytes >= limits.disk_reserve_gib * GIB
        && cgroup_has_headroom(cgroup, worker_count)
}

fn resume_safe(limits: &BatchLimits, scheduler: &SchedulerStatus) -> bool {
    !scheduler_has_priority_work(scheduler)
        && scheduler.machine.memory_available_bytes >= limits.memory_resume_mib * MIB
        && scheduler
            .machine
            .memory_pressure_some_avg10
            .is_some_and(|value| value <= limits.memory_psi_resume_max_pct)
        && scheduler
            .machine
            .io_pressure_full_avg10
            .is_some_and(|value| value <= limits.io_resume_full_pct)
        && scheduler.machine.disk_available_bytes >= limits.disk_reserve_gib * GIB
}

fn scheduler_has_priority_work(status: &SchedulerStatus) -> bool {
    status.scheduler.paused
        || status.summary.queued > 0
        || status.summary.scanning > 0
        || status.summary.finalizing > 0
        || status.summary.legacy_compact_active > 0
        || status.summary.scan_ready > 0
        || status.summary.poh_migration_epochs_runnable > 0
        || status.summary.poh_migration_running > 0
        || status.summary.registry_reprocess_epochs_runnable > 0
        || status.summary.registry_reprocess_audits_runnable > 0
        || status.summary.registry_reprocess_running > 0
        || !status.scan_sweep.complete
        || status.scan_sweep.pending > 0
        || status.scan_sweep.active > 0
        || status
            .finalizer_queue
            .iter()
            .any(|item| !is_terminal_scheduler_state(&item.state))
        || status
            .lanes
            .iter()
            .any(|lane| !is_terminal_scheduler_state(&lane.state))
        || status.live.iter().any(|item| {
            !matches!(
                item.state.as_str(),
                "packaged" | "complete" | "failed" | "blocked"
            )
        })
}

fn is_terminal_scheduler_state(state: &str) -> bool {
    matches!(
        state,
        "idle" | "done" | "complete" | "completed" | "failed" | "stopped" | "cancelled"
    )
}

fn cgroup_has_headroom(cgroup: &CgroupMemorySnapshot, worker_count: usize) -> bool {
    let Some(high) = cgroup.high_bytes else {
        return false;
    };
    let Some(max) = cgroup.max_bytes else {
        return false;
    };
    let required = (worker_count as u64).saturating_mul(MIN_CGROUP_HEADROOM_PER_WORKER);
    cgroup_working_usage_bytes(cgroup).saturating_add(required) <= high.min(max)
}

fn cgroup_working_usage_bytes(cgroup: &CgroupMemorySnapshot) -> u64 {
    cgroup
        .current_bytes
        .saturating_sub(cgroup.inactive_file_bytes)
        .saturating_add(cgroup.swap_current_bytes)
}

fn validate_cgroup_limits(cgroup: &CgroupMemorySnapshot, limits: &BatchLimits) -> Result<()> {
    ensure!(
        cgroup.high_bytes == Some(limits.cgroup_memory_high_mib * MIB)
            && cgroup.max_bytes == Some(limits.cgroup_memory_max_mib * MIB),
        "batch cgroup memory.high or memory.max differs from the immutable manifest"
    );
    Ok(())
}

fn cgroup_hard_event_changed(before: CgroupEventRecord, after: CgroupEventRecord) -> bool {
    after.max != before.max || after.oom != before.oom || after.oom_kill != before.oom_kill
}

fn create_batch_layout(state_root: &Path) -> Result<()> {
    let parent = state_root
        .parent()
        .context("batch state root has no parent")?;
    create_directory_chain_without_symlinks(parent)?;
    validate_private_directory(parent, "batch state parent")?;
    match fs::symlink_metadata(state_root) {
        Ok(_) => validate_private_directory(state_root, "batch state root")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_private_directory(state_root)?;
            validate_private_directory(state_root, "batch state root")?;
        }
        Err(error) => return Err(error.into()),
    }
    File::open(state_root.parent().unwrap())?.sync_all()?;
    for name in ["events", "attempts", "terminal", "logs", "pinned"] {
        let path = state_root.join(name);
        match fs::symlink_metadata(&path) {
            Ok(_) => validate_private_directory(&path, "batch state child")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_private_directory(&path)?;
                validate_private_directory(&path, "batch state child")?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    cleanup_internal_temps_in_directory(state_root)?;
    for name in ["events", "attempts", "terminal", "pinned"] {
        cleanup_internal_temps_in_directory(&state_root.join(name))?;
    }
    File::open(state_root)?.sync_all()?;
    Ok(())
}

fn create_directory_chain_without_symlinks(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "directory chain must be absolute");
    let mut current = PathBuf::from("/");
    for component in path.components().skip(1) {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) => ensure!(
                metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
                "directory ancestor is not a real directory: {}",
                current.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_private_directory(&current)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn create_private_directory(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700).create(path)?;
    Ok(())
}

fn validate_private_directory(path: &Path, label: &str) -> Result<()> {
    validate_real_canonical_directory(path, label)?;
    let metadata = fs::symlink_metadata(path)?;
    let effective_uid = unsafe { libc::geteuid() };
    ensure!(
        metadata.uid() == effective_uid,
        "{label} is not owned by the effective user"
    );
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "{label} permits group or other access"
    );
    Ok(())
}

fn acquire_controller_lock(root: &Path) -> Result<File> {
    validate_owned_not_writable_directory(root, "controller state root")?;
    let path = root.join("controller.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)?;
    let opened = file.metadata()?;
    let path_metadata = fs::symlink_metadata(&path)?;
    ensure!(
        opened.file_type().is_file()
            && opened.nlink() == 1
            && opened.uid() == unsafe { libc::geteuid() }
            && opened.permissions().mode() & 0o022 == 0
            && same_file(&opened, &path_metadata),
        "controller lock authority is invalid"
    );
    // SAFETY: this descriptor remains owned for the supervisor lifetime.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    ensure!(
        result == 0,
        "Firewatch controller is running or another batch holds its lock"
    );
    let after = fs::symlink_metadata(&path)?;
    ensure!(
        same_file(&opened, &after),
        "controller lock path changed after flock"
    );
    Ok(file)
}

fn acquire_existing_controller_lock(root: &Path) -> Result<File> {
    validate_owned_not_writable_directory(root, "controller state root")?;
    let path = root.join("controller.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
        .with_context(|| format!("open existing controller lock {}", path.display()))?;
    let opened = file.metadata()?;
    let path_metadata = fs::symlink_metadata(&path)?;
    ensure!(
        opened.file_type().is_file()
            && opened.nlink() == 1
            && opened.uid() == unsafe { libc::geteuid() }
            && opened.permissions().mode() & 0o022 == 0
            && same_file(&opened, &path_metadata),
        "controller lock authority is invalid"
    );
    // SAFETY: this descriptor remains owned for the validation probe lifetime.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    ensure!(
        result == 0,
        "Firewatch controller is running or another batch holds its lock"
    );
    ensure!(
        same_file(&opened, &fs::symlink_metadata(&path)?),
        "controller lock path changed after flock"
    );
    Ok(file)
}

fn pin_audit_binary(
    state_root: &Path,
    manifest: &ManifestExecutable,
) -> Result<(PinnedExecutable, PinnedExecutable)> {
    validate_real_canonical_file(&manifest.path, "audit binary")?;
    let path_before = fs::symlink_metadata(&manifest.path)?;
    let mut source = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&manifest.path)?;
    let source_before = source.metadata()?;
    ensure!(
        same_file(&path_before, &source_before) && same_version(&path_before, &source_before),
        "public audit source changed before stable open"
    );
    let pinned_path = state_root
        .join("pinned")
        .join(format!("firewatch-wire-profile-audit-{}", manifest.sha256));
    let private_exists = match fs::symlink_metadata(&pinned_path) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file()
                    && !metadata.file_type().is_symlink()
                    && metadata.uid() == unsafe { libc::geteuid() }
                    && metadata.permissions().mode() & 0o077 == 0,
                "existing private audit copy is not protected"
            );
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    let temp = (!private_exists)
        .then(|| unique_temp_path(&pinned_path))
        .transpose()?;
    let mut output = match &temp {
        Some(path) => Some(
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o500)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)?,
        ),
        None => None,
    };
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        if let Some(output) = output.as_mut() {
            output.write_all(&buffer[..read])?;
        }
    }
    let source_after = source.metadata()?;
    let path_after = fs::symlink_metadata(&manifest.path)?;
    ensure!(
        same_file(&source_before, &source_after)
            && same_version(&source_before, &source_after)
            && same_file(&source_after, &path_after)
            && same_version(&source_after, &path_after)
            && hex_digest(hasher.finalize()) == manifest.sha256,
        "public audit source identity or hash changed while pinning"
    );
    if let (Some(temp), Some(mut output)) = (temp, output) {
        output.flush()?;
        output.sync_all()?;
        fs::set_permissions(&temp, fs::Permissions::from_mode(0o500))?;
        output.sync_all()?;
        ensure!(
            sha256_file_pinned(&temp)? == manifest.sha256,
            "private audit copy has the wrong hash"
        );
        fs::hard_link(&temp, &pinned_path)?;
        File::open(pinned_path.parent().unwrap())?.sync_all()?;
        fs::remove_file(&temp)?;
        File::open(pinned_path.parent().unwrap())?.sync_all()?;
    }
    let pinned = fs::symlink_metadata(&pinned_path)?;
    ensure!(
        pinned.file_type().is_file()
            && !pinned.file_type().is_symlink()
            && pinned.permissions().mode() & 0o777 == 0o500,
        "audit binary changed while it was pinned"
    );
    ensure!(
        sha256_file_pinned(&pinned_path)? == manifest.sha256,
        "pinned audit binary hash is invalid"
    );
    let private = PinnedExecutable {
        path: pinned_path,
        device: pinned.dev(),
        inode: pinned.ino(),
        size: pinned.len(),
        modified_seconds: pinned.mtime(),
        modified_nanoseconds: pinned.mtime_nsec(),
        changed_seconds: pinned.ctime(),
        changed_nanoseconds: pinned.ctime_nsec(),
        sha256: manifest.sha256.clone(),
    };
    let public = PinnedExecutable {
        path: manifest.path.clone(),
        device: source_after.dev(),
        inode: source_after.ino(),
        size: source_after.len(),
        modified_seconds: source_after.mtime(),
        modified_nanoseconds: source_after.mtime_nsec(),
        changed_seconds: source_after.ctime(),
        changed_nanoseconds: source_after.ctime_nsec(),
        sha256: manifest.sha256.clone(),
    };
    Ok((private, public))
}

fn capture_pinned_executables(executables: &[ManifestExecutable]) -> Result<Vec<PinnedExecutable>> {
    executables
        .iter()
        .map(|executable| {
            let metadata = fs::symlink_metadata(&executable.path)?;
            ensure!(
                metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
                "yield executable is not a real file"
            );
            ensure!(
                sha256_file_pinned(&executable.path)? == executable.sha256,
                "yield executable hash changed: {}",
                executable.path.display()
            );
            Ok(PinnedExecutable {
                path: executable.path.clone(),
                device: metadata.dev(),
                inode: metadata.ino(),
                size: metadata.len(),
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
                changed_seconds: metadata.ctime(),
                changed_nanoseconds: metadata.ctime_nsec(),
                sha256: executable.sha256.clone(),
            })
        })
        .collect()
}

fn capture_manifest_executable(executable: &ManifestExecutable) -> Result<PinnedExecutable> {
    let mut captured = capture_pinned_executables(std::slice::from_ref(executable))?;
    Ok(captured.pop().expect("one executable was requested"))
}

fn initialize_state(
    manifest: &BatchManifest,
    manifest_sha256: &str,
    pinned_audit_binary: PinnedExecutable,
    public_audit_binary: PinnedExecutable,
    pinned_yield_executables: Vec<PinnedExecutable>,
    cgroup: CgroupBinding,
    cgroup_hard_event_baseline: CgroupEventRecord,
) -> BatchState {
    BatchState {
        schema_version: STATE_SCHEMA_VERSION,
        kind: "firewatch_wire_profile_audit_batch_state".into(),
        manifest_sha256: manifest_sha256.into(),
        status: "initialized".into(),
        next_event_seq: 1,
        pinned_audit_binary,
        public_audit_binary,
        pinned_yield_executables,
        cgroup,
        cgroup_hard_event_baseline,
        updated_unix_secs: unix_now(),
        tasks: manifest
            .tasks
            .iter()
            .map(|task| {
                (
                    task.task_id.clone(),
                    TaskState {
                        status: TaskStatus::Pending,
                        profile_index: 0,
                        operational_retries: 0,
                        retry_not_before_unix_secs: None,
                        attempt: None,
                        last_message: None,
                    },
                )
            })
            .collect(),
    }
}

fn validate_existing_state(
    args: &Args,
    state: &BatchState,
    manifest: &BatchManifest,
    manifest_sha256: &str,
    pinned_audit: &PinnedExecutable,
    public_audit: &PinnedExecutable,
    pinned_yields: &[PinnedExecutable],
    cgroup: &CgroupBinding,
) -> Result<()> {
    ensure!(
        state.schema_version == STATE_SCHEMA_VERSION
            && state.kind == "firewatch_wire_profile_audit_batch_state"
            && state.manifest_sha256 == manifest_sha256,
        "existing batch state identity is invalid"
    );
    ensure!(
        state.next_event_seq > 0 && state.updated_unix_secs > 0,
        "existing state sequence or time is invalid"
    );
    ensure!(
        valid_batch_state_status(&state.status),
        "existing batch status is invalid"
    );
    ensure!(
        pinned_executable_equal(&state.pinned_audit_binary, pinned_audit),
        "existing state binds another pinned audit executable"
    );
    ensure!(
        pinned_executable_equal(&state.public_audit_binary, public_audit),
        "existing state binds another public audit executable"
    );
    ensure!(
        state.pinned_yield_executables.len() == pinned_yields.len()
            && state
                .pinned_yield_executables
                .iter()
                .zip(pinned_yields)
                .all(|(left, right)| pinned_executable_equal(left, right)),
        "existing state binds another yield executable set"
    );
    ensure!(
        state.cgroup.path == cgroup.path
            && state.cgroup.device == cgroup.device
            && state.cgroup.inode == cgroup.inode,
        "batch cgroup identity changed; automatic restart/rebind is disabled"
    );
    let expected = manifest
        .tasks
        .iter()
        .map(|task| task.task_id.as_str())
        .collect::<BTreeSet<_>>();
    ensure!(
        state
            .tasks
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            == expected,
        "existing state task set differs from manifest"
    );
    for (task_id, task_state) in &state.tasks {
        let task = manifest
            .tasks
            .iter()
            .find(|task| &task.task_id == task_id)
            .expect("task set validated");
        ensure!(
            usize::from(task_state.profile_index) < task.profile_attempts.len(),
            "task state has an invalid profile index"
        );
        ensure!(
            task_state.operational_retries <= MAX_OPERATIONAL_RETRIES,
            "task state exceeds the retry limit"
        );
        ensure!(
            task_state.attempt.is_some()
                == matches!(
                    task_state.status,
                    TaskStatus::Starting | TaskStatus::Running | TaskStatus::Paused
                ),
            "task state and attempt presence differ"
        );
        if let Some(attempt) = &task_state.attempt {
            validate_attempt_state(
                args,
                attempt,
                task,
                task_state,
                pinned_audit,
                &manifest.attestation_root,
            )?;
        }
    }
    Ok(())
}

fn valid_batch_state_status(status: &str) -> bool {
    matches!(
        status,
        "initialized"
            | "running"
            | "paused"
            | "waiting_for_stable_resources"
            | "stopped"
            | "stopped_after_error"
            | "complete"
            | "blocked"
    )
}

fn pinned_executable_equal(left: &PinnedExecutable, right: &PinnedExecutable) -> bool {
    left.path == right.path
        && left.device == right.device
        && left.inode == right.inode
        && left.size == right.size
        && left.modified_seconds == right.modified_seconds
        && left.modified_nanoseconds == right.modified_nanoseconds
        && left.changed_seconds == right.changed_seconds
        && left.changed_nanoseconds == right.changed_nanoseconds
        && left.sha256 == right.sha256
}

fn validate_attempt_state(
    args: &Args,
    attempt: &AttemptState,
    task: &ManifestTask,
    task_state: &TaskState,
    pinned: &PinnedExecutable,
    attestation_root: &Path,
) -> Result<()> {
    ensure!(is_attempt_id(&attempt.attempt_id), "attempt id is invalid");
    ensure!(
        attempt.profile == task.profile_attempts[usize::from(task_state.profile_index)],
        "attempt profile differs from the durable profile index"
    );
    ensure!(
        attempt.executable == pinned.path
            && attempt.executable_device == pinned.device
            && attempt.executable_inode == pinned.inode,
        "attempt executable differs from pinned auditor"
    );
    ensure!(
        attempt.pid.is_some() == attempt.process_start_ticks.is_some()
            && attempt.pid.is_some() == attempt.pgid.is_some(),
        "attempt process identity is incomplete"
    );
    if let (Some(pid), Some(pgid)) = (attempt.pid, attempt.pgid) {
        ensure!(pid == pgid, "attempt process is not its group leader");
    }
    ensure!(
        matches!(task_state.status, TaskStatus::Starting)
            || (attempt.pid.is_some()
                && matches!(task_state.status, TaskStatus::Running | TaskStatus::Paused)),
        "running or paused attempt has no full process identity"
    );
    ensure!(
        (task_state.status == TaskStatus::Paused) == attempt.paused,
        "durable pause state is inconsistent"
    );
    ensure!(
        attempt.argv == audit_argv(task, attempt.profile, attestation_root),
        "attempt arguments differ from the exact batch task"
    );
    ensure!(
        attempt.log
            == args
                .state_root
                .join("logs")
                .join(format!("{}.log", attempt.attempt_id)),
        "attempt log path is not exact"
    );
    Ok(())
}

fn read_bound_cgroup(binding: &CgroupBinding) -> Result<CgroupMemorySnapshot> {
    let metadata = fs::symlink_metadata(&binding.path)?;
    ensure!(
        metadata.file_type().is_dir()
            && !metadata.file_type().is_symlink()
            && metadata.dev() == binding.device
            && metadata.ino() == binding.inode,
        "batch cgroup identity changed"
    );
    read_cgroup_memory(&binding.path)
}

fn fetch_scheduler_status(client: &Client, url: &str) -> Result<SchedulerStatus> {
    let mut response = client
        .get(url)
        .send()
        .context("fetch scheduler status")?
        .error_for_status()
        .context("scheduler status returned an error")?;
    if let Some(length) = response.content_length() {
        ensure!(
            length <= MAX_CONTROL_BYTES,
            "scheduler status exceeds size bound"
        );
    }
    let mut bytes = Vec::new();
    response
        .by_ref()
        .take(MAX_CONTROL_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_CONTROL_BYTES,
        "scheduler status exceeds size bound"
    );
    let status: SchedulerStatus = serde_json::from_slice(&bytes)?;
    ensure!(
        status.lanes.len() <= 4_096
            && status.finalizer_queue.len() <= 4_096
            && status.live.len() <= 4_096,
        "scheduler status contains too many lanes, finalizer items, or live captures"
    );
    let now = unix_now();
    ensure!(
        status.schema_version == 3
            && status.sequence > 0
            && status.now_unix_secs > 0
            && status.now_unix_secs.abs_diff(now) <= 15
            && status.control_reconciled_unix_secs > 0
            && status.control_reconciled_unix_secs.abs_diff(now) <= 15
            && !status.observer_mode
            && status.inventory.complete,
        "scheduler status is stale, incomplete, or not in execute mode"
    );
    Ok(status)
}

fn validate_source_status(args: &Args, manifest: &BatchManifest) -> Result<()> {
    let path = args.controller_state_root.join("status.json");
    validate_owned_control_file(&path, "controller source status")?;
    let (bytes, sha256) = read_bounded_pinned_bytes(&path, MAX_CONTROL_BYTES)?;
    ensure!(
        sha256 == manifest.source_status_sha256,
        "controller source status hash differs from the manifest"
    );
    let status: SourceControllerStatus =
        serde_json::from_slice(&bytes).context("decode controller source status")?;
    ensure!(
        status.schema_version == 1 && status.running == 0,
        "controller source status is not a stopped schema-1 snapshot"
    );
    ensure!(
        status.rows.len() <= 8_192,
        "controller source status has too many rows"
    );
    let mut found = BTreeSet::new();
    for row in status.rows {
        ensure!(
            row.wire_profile_audit_inputs.len() <= 16,
            "controller source row has too many audit inputs"
        );
        if row.state != "profile_audit_required" || row.phase != "wire_profile_audit" {
            continue;
        }
        for input in row.wire_profile_audit_inputs {
            let Some(task) = manifest.tasks.iter().find(|task| {
                task.epoch == row.epoch
                    && task.archive == input.archive
                    && task.registry_order == input.registry_order
                    && task.generation_kind == input.generation_kind
                    && task.content_generation_sha256 == input.content_generation_sha256
            }) else {
                continue;
            };
            ensure!(
                found.insert(task.task_id.clone()),
                "source status contains duplicate audit input for {}",
                task.task_id
            );
        }
    }
    ensure!(
        found.len() == manifest.tasks.len(),
        "source status does not contain every exact manifest task"
    );
    Ok(())
}

fn verify_pinned_files(manifest: &BatchManifest, state: &BatchState) -> Result<()> {
    verify_one_pinned_executable(&manifest.audit_binary, &state.pinned_audit_binary, true)?;
    verify_one_pinned_executable(&manifest.audit_binary, &state.public_audit_binary, false)?;
    ensure!(
        manifest.yield_executables.len() == state.pinned_yield_executables.len(),
        "pinned yield executable count changed"
    );
    for (manifest_executable, pinned) in manifest
        .yield_executables
        .iter()
        .zip(&state.pinned_yield_executables)
    {
        verify_one_pinned_executable(manifest_executable, pinned, false)?;
    }
    for path in &manifest.forbidden_executable_paths {
        ensure!(
            fs::symlink_metadata(path)
                .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
            "forbidden executable appeared and requires a revised manifest: {}",
            path.display()
        );
    }
    Ok(())
}

fn verify_pinned_hashes(manifest: &BatchManifest, state: &BatchState) -> Result<()> {
    ensure!(
        sha256_file_pinned(&state.pinned_audit_binary.path)? == manifest.audit_binary.sha256
            && sha256_file_pinned(&manifest.audit_binary.path)? == manifest.audit_binary.sha256,
        "public or private audit executable hash changed"
    );
    for (manifest_executable, pinned) in manifest
        .yield_executables
        .iter()
        .zip(&state.pinned_yield_executables)
    {
        ensure!(
            sha256_file_pinned(&pinned.path)? == manifest_executable.sha256,
            "yield executable hash changed before spawn: {}",
            pinned.path.display()
        );
    }
    Ok(())
}

fn verify_one_pinned_executable(
    manifest: &ManifestExecutable,
    pinned: &PinnedExecutable,
    audit_has_private_hardlink: bool,
) -> Result<()> {
    let source = fs::symlink_metadata(&manifest.path)?;
    ensure!(
        source.file_type().is_file()
            && !source.file_type().is_symlink()
            && manifest.sha256 == pinned.sha256,
        "manifest executable is not a real file: {}",
        manifest.path.display()
    );
    if audit_has_private_hardlink {
        let private = fs::symlink_metadata(&pinned.path)?;
        ensure!(
            private.file_type().is_file()
                && !private.file_type().is_symlink()
                && private.dev() == pinned.device
                && private.ino() == pinned.inode
                && private.len() == pinned.size
                && private.mtime() == pinned.modified_seconds
                && private.mtime_nsec() == pinned.modified_nanoseconds
                && private.ctime() == pinned.changed_seconds
                && private.ctime_nsec() == pinned.changed_nanoseconds,
            "private pinned audit executable changed"
        );
    } else {
        ensure!(
            pinned.path == manifest.path
                && source.dev() == pinned.device
                && source.ino() == pinned.inode
                && source.len() == pinned.size
                && source.mtime() == pinned.modified_seconds
                && source.mtime_nsec() == pinned.modified_nanoseconds
                && source.ctime() == pinned.changed_seconds
                && source.ctime_nsec() == pinned.changed_nanoseconds,
            "yield executable identity changed"
        );
    }
    Ok(())
}

fn verify_external_work_guard(
    manifest: &BatchManifest,
    state: &BatchState,
    owned: &BTreeMap<String, (u32, u64)>,
) -> Result<Option<String>> {
    verify_pinned_files(manifest, state)?;
    let mut protected = BTreeSet::new();
    protected.insert((
        state.pinned_audit_binary.device,
        state.pinned_audit_binary.inode,
    ));
    protected.insert((
        state.public_audit_binary.device,
        state.public_audit_binary.inode,
    ));
    protected.extend(
        state
            .pinned_yield_executables
            .iter()
            .map(|executable| (executable.device, executable.inode)),
    );
    protected.extend(scan_historical_protected_executables(manifest)?);
    let owned_by_pid = owned
        .iter()
        .map(|(task_id, (pid, ticks))| (*pid, (task_id, *ticks)))
        .collect::<BTreeMap<_, _>>();
    let mut observed_owned = BTreeSet::new();

    let entries = fs::read_dir("/proc").context("scan /proc for protected executables")?;
    for entry in entries {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == std::process::id() {
            continue;
        }
        let ticks = process_start_ticks(pid);
        let exe_path = entry.path().join("exe");
        let forbidden_match =
            process_mentions_forbidden_path(pid, &manifest.forbidden_executable_paths)?;
        if forbidden_match {
            return Ok(Some(format!(
                "process {pid} refers to a forbidden executable path"
            )));
        }
        let metadata = match fs::metadata(&exe_path) {
            Ok(metadata) => metadata,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                ) && ticks.is_none() =>
            {
                continue;
            }
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                return Err(error).context("inspect protected process executable");
            }
            Err(error) => {
                if owned_by_pid.contains_key(&pid) {
                    return Err(error).context("inspect owned audit process executable");
                }
                continue;
            }
        };
        let identity = (metadata.dev(), metadata.ino());
        if let Some((task_id, expected_ticks)) = owned_by_pid.get(&pid) {
            ensure!(
                identity
                    == (
                        state.pinned_audit_binary.device,
                        state.pinned_audit_binary.inode,
                    )
                    && ticks == Some(*expected_ticks),
                "owned audit executable or PID identity changed"
            );
            let attempt = state
                .tasks
                .get(*task_id)
                .and_then(|task| task.attempt.as_ref())
                .context("owned audit has no durable attempt")?;
            ensure!(
                process_matches_attempt(attempt, pid, &state.cgroup)?,
                "owned audit process identity is invalid"
            );
            observed_owned.insert(pid);
            continue;
        }
        if protected.contains(&identity) {
            return Ok(Some(format!(
                "external protected executable is active at PID {pid}"
            )));
        }
    }
    ensure!(
        observed_owned.len() == owned_by_pid.len(),
        "one or more owned audit processes were not observed exactly"
    );
    Ok(None)
}

fn scan_historical_protected_executables(manifest: &BatchManifest) -> Result<BTreeSet<(u64, u64)>> {
    let controller_root = manifest
        .attestation_root
        .parent()
        .context("attestation root has no controller parent")?;
    validate_owned_not_writable_directory(controller_root, "controller state root")?;
    let batches_root = controller_root.join("wire-profile-audit-batches");
    if fs::symlink_metadata(&batches_root)
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Ok(BTreeSet::new());
    }
    validate_owned_not_writable_directory(&batches_root, "audit batches root")?;
    let mut identities = BTreeSet::new();
    let entries = fs::read_dir(batches_root)?.collect::<std::io::Result<Vec<_>>>()?;
    ensure!(entries.len() <= 4_096, "too many historical audit batches");
    for entry in entries {
        let name = entry.file_name();
        let name = name.to_str().context("batch directory name is not UTF-8")?;
        ensure!(
            is_sha256(name),
            "unexpected entry in audit batches root: {name}"
        );
        validate_private_directory(&entry.path(), "historical batch root")?;
        let state_path = entry.path().join("state.json");
        if let Some(historical) = read_optional_json::<BatchState>(&state_path)? {
            let state_metadata = fs::symlink_metadata(&state_path)?;
            ensure!(
                state_metadata.file_type().is_file()
                    && !state_metadata.file_type().is_symlink()
                    && state_metadata.uid() == unsafe { libc::geteuid() }
                    && state_metadata.permissions().mode() & 0o077 == 0
                    && historical.schema_version == STATE_SCHEMA_VERSION
                    && historical.kind == "firewatch_wire_profile_audit_batch_state"
                    && historical.manifest_sha256 == name
                    && historical.pinned_yield_executables.len() <= 32,
                "historical batch state identity is invalid"
            );
            identities.insert((
                historical.pinned_audit_binary.device,
                historical.pinned_audit_binary.inode,
            ));
            identities.insert((
                historical.public_audit_binary.device,
                historical.public_audit_binary.inode,
            ));
            identities.extend(
                historical
                    .pinned_yield_executables
                    .iter()
                    .map(|item| (item.device, item.inode)),
            );
        }
        let pinned = entry.path().join("pinned");
        if fs::symlink_metadata(&pinned)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            continue;
        }
        validate_private_directory(&pinned, "historical pinned auditor root")?;
        let executables = fs::read_dir(&pinned)?.collect::<std::io::Result<Vec<_>>>()?;
        ensure!(
            executables.len() <= 8,
            "too many historical pinned executables"
        );
        for executable in executables {
            let filename = executable.file_name();
            let filename = filename
                .to_str()
                .context("private auditor filename is not UTF-8")?;
            let hash = filename
                .strip_prefix("firewatch-wire-profile-audit-")
                .context("unexpected file in private auditor root")?;
            ensure!(
                is_sha256(hash),
                "private auditor filename has no exact SHA-256"
            );
            let metadata = fs::symlink_metadata(executable.path())?;
            ensure!(
                metadata.file_type().is_file()
                    && !metadata.file_type().is_symlink()
                    && metadata.uid() == unsafe { libc::geteuid() }
                    && metadata.permissions().mode() & 0o777 == 0o500,
                "historical private auditor is not protected"
            );
            identities.insert((metadata.dev(), metadata.ino()));
        }
    }
    Ok(identities)
}

fn process_mentions_forbidden_path(pid: u32, forbidden: &[PathBuf]) -> Result<bool> {
    if forbidden.is_empty() {
        return Ok(false);
    }
    let exe_link = match fs::read_link(format!("/proc/{pid}/exe")) {
        Ok(path) => Some(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_error) if process_start_ticks(pid).is_none() => return Ok(false),
        Err(error) => return Err(error).context("read process executable link"),
    };
    if exe_link.as_ref().is_some_and(|path| {
        let text = path.to_string_lossy();
        forbidden.iter().any(|forbidden| {
            text == forbidden.to_string_lossy()
                || text.strip_suffix(" (deleted)") == Some(&*forbidden.to_string_lossy())
        })
    }) {
        return Ok(true);
    }
    let cmdline = match fs::read(format!("/proc/{pid}/cmdline")) {
        Ok(bytes) => bytes,
        Err(_error) if process_start_ticks(pid).is_none() => return Ok(false),
        Err(error) => return Err(error).context("read process command line for forbidden path"),
    };
    let executable = cmdline.split(|byte| *byte == 0).next().unwrap_or_default();
    Ok(forbidden
        .iter()
        .any(|path| executable == path.as_os_str().as_encoded_bytes()))
}

fn discover_state_attempt_processes(
    state: &BatchState,
    pinned: &PinnedExecutable,
) -> Result<BTreeMap<String, (u32, u64)>> {
    let mut found = BTreeMap::new();
    let mut claimed_pids = BTreeSet::new();
    for (task_id, task_state) in &state.tasks {
        let Some(attempt) = &task_state.attempt else {
            continue;
        };
        let matches = find_attempt_processes(attempt, pinned, &state.cgroup)?;
        ensure!(
            matches.len() <= 1,
            "multiple processes match durable attempt {}",
            attempt.attempt_id
        );
        if let Some((pid, ticks)) = matches.into_iter().next() {
            ensure!(
                claimed_pids.insert(pid),
                "one process matches multiple attempts"
            );
            found.insert(task_id.clone(), (pid, ticks));
        }
    }
    Ok(found)
}

fn find_attempt_processes(
    attempt: &AttemptState,
    pinned: &PinnedExecutable,
    cgroup: &CgroupBinding,
) -> Result<Vec<(u32, u64)>> {
    let mut matches = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let metadata = match fs::metadata(entry.path().join("exe")) {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if metadata.dev() != pinned.device || metadata.ino() != pinned.inode {
            continue;
        }
        if process_matches_attempt(attempt, pid, cgroup)? {
            let ticks = process_start_ticks(pid).context("matching process has no start ticks")?;
            matches.push((pid, ticks));
        }
    }
    Ok(matches)
}

fn process_matches_attempt(
    attempt: &AttemptState,
    pid: u32,
    cgroup: &CgroupBinding,
) -> Result<bool> {
    process_matches_attempt_identity(attempt, pid, Some(cgroup))
}

fn process_matches_attempt_identity(
    attempt: &AttemptState,
    pid: u32,
    cgroup: Option<&CgroupBinding>,
) -> Result<bool> {
    if process_group_id(pid) != Some(pid) {
        return Ok(false);
    }
    if let Some(cgroup) = cgroup
        && !process_in_bound_cgroup(pid, cgroup)?
    {
        return Ok(false);
    }
    let metadata = match fs::metadata(format!("/proc/{pid}/exe")) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(false),
    };
    if metadata.dev() != attempt.executable_device || metadata.ino() != attempt.executable_inode {
        return Ok(false);
    }
    let environment = match fs::read(format!("/proc/{pid}/environ")) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(false),
    };
    let expected_attempt = format!(
        "BLOCKZILLA_FIREWATCH_AUDIT_BATCH_ATTEMPT_ID={}",
        attempt.attempt_id
    );
    let environment = environment
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .collect::<Vec<_>>();
    if environment.len() != 1 || environment[0] != expected_attempt.as_bytes() {
        return Ok(false);
    }
    let cmdline = match fs::read(format!("/proc/{pid}/cmdline")) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(false),
    };
    let actual = cmdline
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .collect::<Vec<_>>();
    Ok(actual.len() == attempt.argv.len() + 1
        && actual[0] == attempt.executable.as_os_str().as_encoded_bytes()
        && actual[1..]
            .iter()
            .zip(&attempt.argv)
            .all(|(actual, expected)| *actual == expected.as_bytes()))
}

fn process_in_bound_cgroup(pid: u32, binding: &CgroupBinding) -> Result<bool> {
    let path = PathBuf::from(format!("/proc/{pid}/cgroup"));
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        metadata.file_type().is_file()
            && !metadata.file_type().is_symlink()
            && metadata.len() <= 64 * 1024,
        "process cgroup membership is not a bounded real file"
    );
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(&path)?;
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(64 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 64 * 1024, "process cgroup data is too large");
    let text = std::str::from_utf8(&bytes).context("process cgroup data is not UTF-8")?;
    let mut unified = None;
    for line in text.lines() {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next().unwrap_or_default();
        let controllers = fields.next().context("malformed process cgroup line")?;
        let relative = fields.next().context("malformed process cgroup path")?;
        if hierarchy == "0" && controllers.is_empty() {
            ensure!(unified.is_none(), "multiple unified process cgroups");
            let relative = Path::new(relative);
            ensure!(
                relative.is_absolute(),
                "process cgroup path is not absolute"
            );
            ensure!(
                relative.components().all(|component| matches!(
                    component,
                    std::path::Component::RootDir | std::path::Component::Normal(_)
                )),
                "process cgroup path contains traversal"
            );
            unified = Some(relative.to_path_buf());
        }
    }
    let relative = unified.context("process has no unified cgroup")?;
    let expected = binding
        .path
        .strip_prefix("/sys/fs/cgroup")
        .context("durable cgroup is outside /sys/fs/cgroup")?;
    Ok(relative.strip_prefix("/").ok() == Some(expected))
}

fn recover_and_terminate_discovered_attempts(
    args: &Args,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &mut BatchState,
    discovered: BTreeMap<String, (u32, u64)>,
) -> Result<()> {
    let mut recovered = BTreeMap::new();
    let identity_result = (|| -> Result<()> {
        let task_ids = state.tasks.keys().cloned().collect::<Vec<_>>();
        for task_id in task_ids {
            if let Some((pid, ticks)) = discovered.get(&task_id).copied() {
                let task_state = state.tasks.get_mut(&task_id).unwrap();
                let attempt = task_state.attempt.as_mut().unwrap();
                if let Some(expected_pid) = attempt.pid {
                    ensure!(expected_pid == pid, "durable attempt PID changed");
                }
                if let Some(expected_ticks) = attempt.process_start_ticks {
                    ensure!(expected_ticks == ticks, "durable attempt PID was reused");
                }
                attempt.pid = Some(pid);
                attempt.process_start_ticks = Some(ticks);
                attempt.pgid = Some(pid);
                task_state.status = if attempt.paused {
                    TaskStatus::Paused
                } else {
                    TaskStatus::Running
                };
                let process = ActiveAttempt {
                    child: None,
                    pid,
                    start_ticks: ticks,
                };
                ensure!(
                    verified_attempt_live(state, &task_id, &process)?,
                    "recovered audit process identity is invalid"
                );
                recovered.insert(task_id, process);
            }
        }
        if !recovered.is_empty() {
            append_event_and_snapshot(
                args,
                state,
                manifest_sha256,
                None,
                "recovered_attempt_identities_bound",
                "persisted exact recovered audit identities before interruption intents",
            )?;
        }
        Ok(())
    })();
    if let Err(error) = identity_result {
        for (pid, ticks) in discovered.values().copied() {
            wait_for_process_absence_without_signal(pid, ticks);
        }
        return Err(error).context("bind recovered audit identities");
    }
    let mut authority_errors = Vec::new();
    for (task_id, process) in &recovered {
        let authority_result = (|| -> Result<()> {
            ensure!(
                verified_attempt_live(state, task_id, process)?,
                "recovered audit changed before interruption intent"
            );
            let attempt = state.tasks[task_id].attempt.clone().unwrap();
            let path = args
                .state_root
                .join("attempts")
                .join(format!("{}.json", attempt.attempt_id));
            if let Some(receipt) = read_optional_json::<AttemptReceipt>(&path)? {
                validate_attempt_receipt(&receipt, manifest_sha256, task_id, &attempt)?;
                ensure!(
                    receipt.executable_sha256 == state.pinned_audit_binary.sha256,
                    "recovered attempt receipt binds another audit executable"
                );
                if receipt.outcome == "attested" {
                    ensure!(
                        receipt.exit_code.is_some() || receipt.signal.is_some(),
                        "attested receipt has no child outcome"
                    );
                    let task = task_map[task_id];
                    let Some((_sha256, profile)) = validate_exact_attestation(task)? else {
                        bail!("recovered attested receipt has no exact attestation");
                    };
                    ensure!(
                        profile == receipt.profile,
                        "recovered attested receipt profile changed"
                    );
                } else {
                    disposition_from_receipt(&receipt)?;
                }
            } else {
                publish_attempt_receipt(
                    args,
                    manifest_sha256,
                    task_id,
                    &attempt,
                    "supervisor_interruption_intent",
                    None,
                )?;
            }
            Ok(())
        })();
        if let Err(error) = authority_result {
            authority_errors.push(format!("{task_id}: {error:#}"));
        }
    }
    if !authority_errors.is_empty() {
        for process in recovered.values() {
            wait_for_process_absence_without_signal(process.pid, process.start_ticks);
        }
        bail!(
            "recovered interruption authority failed: {}",
            authority_errors.join("; ")
        );
    }
    let mut failures = Vec::new();
    for (task_id, mut process) in recovered {
        if let Err(error) = settle_active_attempt(state, &task_id, &mut process) {
            failures.push((task_id, process, error));
        }
    }
    // Settlement is never fail-fast. Later recovered workers are not children
    // of this supervisor, so each one must be settled or observed absent before
    // this process can return.
    for (_, process, _) in &failures {
        wait_for_process_absence_without_signal(process.pid, process.start_ticks);
    }
    ensure!(
        failures.is_empty(),
        "recovered audit settlement failures: {}",
        failures
            .iter()
            .map(|(task_id, _, error)| format!("{task_id}: {error:#}"))
            .collect::<Vec<_>>()
            .join("; ")
    );
    Ok(())
}

fn reconcile_absent_attempts(
    args: &Args,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &mut BatchState,
    active: &BTreeMap<String, ActiveAttempt>,
) -> Result<()> {
    let task_ids = state.tasks.keys().cloned().collect::<Vec<_>>();
    for task_id in task_ids {
        if active.contains_key(&task_id) || state.tasks[&task_id].attempt.is_none() {
            continue;
        }
        ensure!(
            !matches!(
                state.tasks[&task_id].status,
                TaskStatus::Complete | TaskStatus::Blocked
            ),
            "terminal task retains an attempt without a live process"
        );
        let attempt = state.tasks[&task_id].attempt.clone().unwrap();
        let receipt_path = args
            .state_root
            .join("attempts")
            .join(format!("{}.json", attempt.attempt_id));
        if read_optional_json::<AttemptReceipt>(&receipt_path)?.is_some() {
            continue;
        }
        publish_attempt_receipt(
            args,
            manifest_sha256,
            &task_id,
            &attempt,
            "lost_exit_after_restart",
            None,
        )?;
        let task = task_map[&task_id];
        let message = "audit process was absent after restart";
        if let Some((profile, terminal_message)) = terminal_block_plan(
            task,
            &state.tasks[&task_id],
            ExitDisposition::RetrySameProfile,
            message,
            true,
        ) {
            publish_terminal_receipt(
                args,
                manifest_sha256,
                task,
                TaskStatus::Blocked,
                profile,
                None,
                &terminal_message,
            )?;
        }
        schedule_same_profile_retry(task, state.tasks.get_mut(&task_id).unwrap(), message, true);
    }
    Ok(())
}

fn reconcile_all_attestations(
    args: &Args,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &mut BatchState,
    active: &mut BTreeMap<String, ActiveAttempt>,
) -> Result<()> {
    let task_ids = state.tasks.keys().cloned().collect::<Vec<_>>();
    for task_id in task_ids {
        let task = task_map[&task_id];
        if !matches!(
            state.tasks[&task_id].status,
            TaskStatus::Complete | TaskStatus::Blocked
        ) && let Some((attestation_sha256, profile)) = validate_exact_attestation(task)?
        {
            // The exact terminal receipt is durable authority to stop an audit
            // that raced with its own successful publication.
            publish_terminal_receipt(
                args,
                manifest_sha256,
                task,
                TaskStatus::Complete,
                profile,
                Some(attestation_sha256.clone()),
                "exact attestation reconciled",
            )?;
            if let Some(mut process) = active.remove(&task_id) {
                terminate_verified_attempt(state, &task_id, &mut process)?;
            }
            mark_complete(
                args,
                manifest_sha256,
                task,
                state,
                attestation_sha256,
                profile,
                "exact attestation reconciled",
            )?;
        }
    }
    Ok(())
}

fn validate_exact_attestation(
    task: &ManifestTask,
) -> Result<Option<(String, ArchiveV2WireProfile)>> {
    validate_exact_attestation_with_pre_final(task, || Ok(()))
}

fn validate_exact_attestation_with_pre_final(
    task: &ManifestTask,
    pre_final: impl FnOnce() -> Result<()>,
) -> Result<Option<(String, ArchiveV2WireProfile)>> {
    match fs::symlink_metadata(&task.expected_attestation) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let (bytes, evidence) = read_pinned_control_evidence(
        &task.expected_attestation,
        MAX_ATTESTATION_BYTES,
        "expected wire-profile attestation",
        true,
    )?;
    let sha256 = evidence.sha256.clone();
    let attestation: WireProfileAttestation = serde_json::from_slice(&bytes)?;
    validate_wire_profile_attestation_structure(&attestation)?;
    ensure!(
        attestation.epoch == task.epoch
            && attestation.archive == task.archive
            && attestation.registry_order == task.registry_order
            && attestation.generation_kind == task.generation_kind
            && attestation.content_generation_sha256 == task.content_generation_sha256
            && task.profile_attempts.contains(&attestation.wire_profile),
        "attestation identity differs from its batch task"
    );
    ensure!(
        fs::canonicalize(&attestation.archive)? == attestation.archive,
        "attestation archive is not canonical"
    );
    match task.generation_kind.as_str() {
        DIRECT_ATTESTATION_GENERATION_KIND => {
            ensure_marker_free_archive(&task.archive)?;
            let (files, blocks_bytes) = capture_direct_generation_files(&task.archive)?;
            ensure!(
                files == attestation.archive_files
                    && blocks_bytes == task.archive_blocks_bytes
                    && direct_generation_digest(task.epoch, &task.registry_order, &files)
                        == task.content_generation_sha256,
                "attestation does not bind the current exact direct archive generation"
            );
        }
        RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND => {
            ensure!(
                attestation.wire_profile == ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                "receipt-source recovery attestation is not for the Post profile"
            );
            validate_receipt_source_recovery_evidence(&attestation.evidence)?;
            let capture = capture_receipt_source_task(task)?;
            validate_receipt_source_task_binding(task, &capture)?;
            ensure!(
                capture.source_files == attestation.archive_files,
                "receipt-source attestation does not bind the admitted live source identities"
            );
        }
        _ => bail!("attestation task generation kind is not admitted"),
    }
    pre_final()?;
    evidence.recheck(true, "expected wire-profile attestation final use")?;
    Ok(Some((sha256, attestation.wire_profile)))
}

fn next_runnable_task(manifest: &BatchManifest, state: &BatchState) -> Option<String> {
    let now = unix_now();
    manifest.tasks.iter().find_map(|task| {
        let task_state = state.tasks.get(&task.task_id)?;
        let runnable = matches!(
            task_state.status,
            TaskStatus::Pending | TaskStatus::RetryWait
        ) && task_state
            .retry_not_before_unix_secs
            .is_none_or(|not_before| now >= not_before);
        runnable.then(|| task.task_id.clone())
    })
}

fn all_tasks_terminal(state: &BatchState) -> bool {
    state
        .tasks
        .values()
        .all(|task| matches!(task.status, TaskStatus::Complete | TaskStatus::Blocked))
}

fn task_map_from_manifest(manifest: &BatchManifest) -> BTreeMap<String, &ManifestTask> {
    manifest
        .tasks
        .iter()
        .map(|task| (task.task_id.clone(), task))
        .collect()
}

fn replay_terminal_receipts(
    args: &Args,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &mut BatchState,
) -> Result<()> {
    let mut receipt_tasks = BTreeSet::new();
    for entry in read_durable_json_entries(&args.state_root.join("terminal"))? {
        let receipt: TerminalReceipt = read_bounded_json(&entry)?;
        ensure!(
            receipt.schema_version == TERMINAL_RECEIPT_SCHEMA_VERSION
                && receipt.kind == "firewatch_wire_profile_audit_batch_terminal"
                && receipt.manifest_sha256 == manifest_sha256
                && receipt.unix_secs > 0,
            "terminal receipt identity is invalid"
        );
        let task = task_map
            .get(&receipt.task_id)
            .copied()
            .context("terminal receipt references an unknown task")?;
        ensure!(
            receipt_tasks.insert(receipt.task_id.clone()),
            "duplicate terminal receipt for one task"
        );
        ensure!(
            entry.file_name().and_then(|name| name.to_str())
                == Some(&format!("{}.json", receipt.task_id))
                && receipt.epoch == task.epoch
                && receipt.expected_attestation == task.expected_attestation
                && task.profile_attempts.contains(&receipt.profile),
            "terminal receipt differs from its manifest task"
        );
        let task_state = state.tasks.get_mut(&receipt.task_id).unwrap();
        match receipt.status {
            TaskStatus::Complete => {
                let Some((sha256, profile)) = validate_exact_attestation(task)? else {
                    bail!("completed terminal receipt has no exact attestation");
                };
                ensure!(
                    receipt.attestation_sha256.as_deref() == Some(&sha256)
                        && receipt.profile == profile,
                    "completed terminal receipt attestation changed"
                );
                task_state.status = TaskStatus::Complete;
                task_state.profile_index = task
                    .profile_attempts
                    .iter()
                    .position(|candidate| *candidate == profile)
                    .unwrap() as u8;
            }
            TaskStatus::Blocked => {
                ensure!(
                    receipt.attestation_sha256.is_none(),
                    "blocked terminal receipt binds an attestation"
                );
                task_state.status = TaskStatus::Blocked;
            }
            _ => bail!("terminal receipt has a nonterminal state"),
        }
        task_state.attempt = None;
        task_state.retry_not_before_unix_secs = None;
        task_state.last_message = Some(receipt.message);
    }
    Ok(())
}

fn validate_terminal_state_receipts(
    args: &Args,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &BatchState,
) -> Result<()> {
    let mut receipts = BTreeMap::new();
    for entry in read_durable_json_entries(&args.state_root.join("terminal"))? {
        let receipt: TerminalReceipt = read_bounded_json(&entry)?;
        ensure!(
            receipt.schema_version == TERMINAL_RECEIPT_SCHEMA_VERSION
                && receipt.kind == "firewatch_wire_profile_audit_batch_terminal"
                && receipt.manifest_sha256 == manifest_sha256,
            "terminal receipt identity is invalid"
        );
        ensure!(
            receipts.insert(receipt.task_id.clone(), receipt).is_none(),
            "duplicate terminal receipt identity"
        );
    }
    for (task_id, task_state) in &state.tasks {
        let receipt = receipts.get(task_id);
        let terminal = matches!(
            task_state.status,
            TaskStatus::Complete | TaskStatus::Blocked
        );
        ensure!(
            terminal == receipt.is_some(),
            "terminal state and immutable terminal receipt differ for {task_id}"
        );
        let Some(receipt) = receipt else {
            continue;
        };
        let task = task_map[task_id];
        ensure!(
            receipt.epoch == task.epoch
                && receipt.expected_attestation == task.expected_attestation
                && receipt.status == task_state.status
                && receipt.profile == task.profile_attempts[usize::from(task_state.profile_index)],
            "terminal receipt differs from final state for {task_id}"
        );
        if task_state.status == TaskStatus::Complete {
            let Some((sha256, profile)) = validate_exact_attestation(task)? else {
                bail!("completed task has no exact attestation");
            };
            ensure!(
                receipt.attestation_sha256.as_deref() == Some(&sha256)
                    && receipt.profile == profile,
                "completed receipt attestation changed"
            );
        } else {
            ensure!(
                receipt.attestation_sha256.is_none(),
                "blocked receipt binds an attestation"
            );
        }
    }
    ensure!(
        receipts
            .keys()
            .all(|task_id| state.tasks.contains_key(task_id)),
        "terminal receipt references an unknown task"
    );
    Ok(())
}

fn replay_current_attempt_receipts(
    args: &Args,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &mut BatchState,
) -> Result<()> {
    let task_ids = state.tasks.keys().cloned().collect::<Vec<_>>();
    for task_id in task_ids {
        let Some(attempt) = state.tasks[&task_id].attempt.clone() else {
            continue;
        };
        let path = args
            .state_root
            .join("attempts")
            .join(format!("{}.json", attempt.attempt_id));
        let Some(receipt) = read_optional_json::<AttemptReceipt>(&path)? else {
            continue;
        };
        validate_attempt_receipt(&receipt, manifest_sha256, &task_id, &attempt)?;
        ensure!(
            receipt.executable_sha256 == state.pinned_audit_binary.sha256,
            "attempt receipt binds another audit executable hash"
        );
        if receipt.outcome == "attested" {
            let task = task_map[&task_id];
            let Some((attestation_sha256, profile)) = validate_exact_attestation(task)? else {
                bail!("attested attempt receipt has no exact attestation");
            };
            publish_terminal_receipt(
                args,
                manifest_sha256,
                task,
                TaskStatus::Complete,
                profile,
                Some(attestation_sha256),
                "completed from durable attested attempt receipt",
            )?;
            let task_state = state.tasks.get_mut(&task_id).unwrap();
            task_state.status = TaskStatus::Complete;
            task_state.profile_index = task
                .profile_attempts
                .iter()
                .position(|candidate| *candidate == profile)
                .unwrap() as u8;
            task_state.attempt = None;
            task_state.retry_not_before_unix_secs = None;
            task_state.last_message = Some("completed from durable attempt receipt".into());
            continue;
        }
        let (disposition, consume_retry) = disposition_from_receipt(&receipt)?;
        let task = task_map[&task_id];
        let replay_message = format!("replayed durable attempt outcome {}", receipt.outcome);
        if let Some((profile, terminal_message)) = terminal_block_plan(
            task,
            &state.tasks[&task_id],
            disposition,
            &replay_message,
            consume_retry,
        ) {
            publish_terminal_receipt(
                args,
                manifest_sha256,
                task,
                TaskStatus::Blocked,
                profile,
                None,
                &terminal_message,
            )?;
        }
        apply_replayed_disposition(
            task,
            state.tasks.get_mut(&task_id).unwrap(),
            disposition,
            &replay_message,
            consume_retry,
        );
    }
    Ok(())
}

fn validate_attempt_receipt(
    receipt: &AttemptReceipt,
    manifest_sha256: &str,
    task_id: &str,
    attempt: &AttemptState,
) -> Result<()> {
    let exact_process_identity = receipt.pid == attempt.pid
        && receipt.process_start_ticks == attempt.process_start_ticks
        && receipt.pgid == attempt.pgid;
    let monotonic_bound_interruption = receipt.outcome == "supervisor_interruption_intent"
        && attempt.pid.is_none()
        && attempt.process_start_ticks.is_none()
        && attempt.pgid.is_none()
        && receipt.pid.is_some()
        && receipt.process_start_ticks.is_some()
        && receipt.pgid == receipt.pid;
    ensure!(
        receipt.schema_version == ATTEMPT_RECEIPT_SCHEMA_VERSION
            && receipt.kind == "firewatch_wire_profile_audit_batch_attempt"
            && receipt.manifest_sha256 == manifest_sha256
            && receipt.task_id == task_id
            && receipt.attempt_id == attempt.attempt_id
            && receipt.profile == attempt.profile
            && (exact_process_identity || monotonic_bound_interruption)
            && receipt.executable_device == attempt.executable_device
            && receipt.executable_inode == attempt.executable_inode
            && receipt.argv == attempt.argv
            && receipt.started_unix_secs == attempt.started_unix_secs
            && receipt.finished_unix_secs >= receipt.started_unix_secs
            && receipt.log == attempt.log
            && is_sha256(&receipt.executable_sha256),
        "attempt receipt differs from its durable attempt"
    );
    Ok(())
}

fn validate_attempt_receipt_wal(
    args: &Args,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &BatchState,
) -> Result<()> {
    for entry in read_durable_json_entries(&args.state_root.join("attempts"))? {
        let receipt: AttemptReceipt = read_bounded_json(&entry)?;
        let task = task_map
            .get(&receipt.task_id)
            .context("attempt receipt references an unknown task")?;
        ensure!(
            receipt.schema_version == ATTEMPT_RECEIPT_SCHEMA_VERSION
                && receipt.kind == "firewatch_wire_profile_audit_batch_attempt"
                && receipt.manifest_sha256 == manifest_sha256
                && is_attempt_id(&receipt.attempt_id)
                && entry.file_name().and_then(|name| name.to_str())
                    == Some(&format!("{}.json", receipt.attempt_id))
                && task.profile_attempts.contains(&receipt.profile)
                && receipt.pid.is_some() == receipt.process_start_ticks.is_some()
                && receipt.pid.is_some() == receipt.pgid.is_some()
                && receipt
                    .pid
                    .zip(receipt.pgid)
                    .is_none_or(|(pid, pgid)| pid == pgid)
                && receipt.executable_device == state.pinned_audit_binary.device
                && receipt.executable_inode == state.pinned_audit_binary.inode
                && receipt.executable_sha256 == state.pinned_audit_binary.sha256
                && receipt.argv
                    == audit_argv(
                        task,
                        receipt.profile,
                        &args.controller_state_root.join("wire-profile-attestations"),
                    )
                && receipt.started_unix_secs > 0
                && receipt.finished_unix_secs >= receipt.started_unix_secs
                && receipt.log
                    == args
                        .state_root
                        .join("logs")
                        .join(format!("{}.log", receipt.attempt_id)),
            "historical attempt receipt is invalid"
        );
        if receipt.outcome != "attested" {
            disposition_from_receipt(&receipt)?;
        }
    }
    Ok(())
}

fn disposition_from_receipt(receipt: &AttemptReceipt) -> Result<(ExitDisposition, bool)> {
    match receipt.outcome.as_str() {
        "selected_profile_decode_rejected" => {
            ensure!(
                receipt.exit_code == Some(SELECTED_PROFILE_DECODE_REJECTED_EXIT_CODE)
                    && receipt.signal.is_none(),
                "selected-profile receipt lacks exact exit 20"
            );
            Ok((ExitDisposition::AdvanceProfile, false))
        }
        "terminal_profile_audit_rejected" => {
            ensure!(
                receipt.exit_code == Some(TERMINAL_PROFILE_AUDIT_REJECTED_EXIT_CODE)
                    && receipt.signal.is_none(),
                "terminal-profile receipt lacks exact exit 21"
            );
            Ok((ExitDisposition::TerminalBlocked, false))
        }
        "operational_failure" | "lost_exit_after_restart" => {
            ensure!(
                receipt.exit_code != Some(SELECTED_PROFILE_DECODE_REJECTED_EXIT_CODE)
                    && receipt.exit_code != Some(TERMINAL_PROFILE_AUDIT_REJECTED_EXIT_CODE),
                "operational receipt cannot carry a profile-selection exit"
            );
            Ok((ExitDisposition::RetrySameProfile, true))
        }
        "supervisor_interruption_intent" => {
            ensure!(
                receipt.exit_code.is_none() && receipt.signal.is_none(),
                "supervisor interruption intent cannot bind an exit outcome"
            );
            Ok((ExitDisposition::RetrySameProfile, false))
        }
        "attested" => bail!("attested attempt is handled before disposition mapping"),
        value => bail!("unknown durable attempt outcome {value}"),
    }
}

fn apply_replayed_disposition(
    task: &ManifestTask,
    state: &mut TaskState,
    disposition: ExitDisposition,
    message: &str,
    consume_retry: bool,
) {
    state.attempt = None;
    match disposition {
        ExitDisposition::AdvanceProfile => {
            if usize::from(state.profile_index) + 1 < task.profile_attempts.len() {
                state.profile_index += 1;
                state.operational_retries = 0;
                state.status = TaskStatus::RetryWait;
                state.retry_not_before_unix_secs = Some(unix_now());
            } else {
                state.status = TaskStatus::Blocked;
                state.retry_not_before_unix_secs = None;
            }
        }
        ExitDisposition::TerminalBlocked => {
            state.status = TaskStatus::Blocked;
            state.retry_not_before_unix_secs = None;
        }
        ExitDisposition::RetrySameProfile => {
            schedule_same_profile_retry(task, state, message, consume_retry);
        }
    }
    state.last_message = Some(message.into());
}

fn read_durable_json_entries(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_str().context("durable filename is not UTF-8")?;
        if name.starts_with('.') && name.contains(".tmp-") {
            bail!("unexpected publisher temp remained after startup cleanup: {name}");
        }
        ensure!(name.ends_with(".json"), "unexpected durable file {name}");
        let metadata = fs::symlink_metadata(&path)?;
        ensure!(
            metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
            "durable JSON entry is not a real file"
        );
        entries.push(path);
    }
    entries.sort();
    Ok(entries)
}

fn cleanup_valid_internal_temp(path: &Path, name: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "internal temp path is not a real file"
    );
    let (_, owner) = name
        .rsplit_once(".tmp-")
        .context("internal temp filename has no owner identity")?;
    let mut fields = owner.split('-');
    let owner_pid = fields
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .context("internal temp filename has no owner PID")?;
    let owner_ticks = fields
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .context("internal temp filename has no owner start ticks")?;
    let nonce = fields
        .next()
        .context("internal temp filename has no nonce")?;
    ensure!(
        !nonce.is_empty()
            && nonce.bytes().all(|byte| byte.is_ascii_digit())
            && fields.next().is_none(),
        "internal temp filename is malformed"
    );
    ensure!(
        process_start_ticks(owner_pid) != Some(owner_ticks),
        "internal temp owner is still active"
    );
    fs::remove_file(path)?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

fn cleanup_internal_temps_in_directory(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().context("batch filename is not UTF-8")?;
        if name.starts_with('.') && name.contains(".tmp-") {
            let destination = name
                .strip_prefix('.')
                .and_then(|name| name.split_once(".tmp-").map(|(destination, _)| destination))
                .context("internal temp has no destination")?;
            let directory_name = directory.file_name().and_then(|name| name.to_str());
            let exact_internal = if directory_name == Some("events") {
                destination.len() == 25
                    && destination.ends_with(".json")
                    && destination[..20].bytes().all(|byte| byte.is_ascii_digit())
            } else if directory_name == Some("attempts") {
                destination.len() == 37
                    && destination.ends_with(".json")
                    && destination[..32]
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            } else if directory_name == Some("terminal") {
                destination.ends_with(".json")
                    && destination[..destination.len() - 5].bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'-' | b'_')
                    })
            } else if directory_name == Some("pinned") {
                destination
                    .strip_prefix("firewatch-wire-profile-audit-")
                    .is_some_and(is_sha256)
            } else {
                destination == "state.json"
            };
            ensure!(
                exact_internal,
                "dot file is not an exact batch publisher temp: {name}"
            );
            cleanup_valid_internal_temp(&entry.path(), name)?;
        } else if name.starts_with('.') {
            bail!("unexpected dot file in batch state: {name}");
        }
    }
    Ok(())
}

fn audit_argv(
    task: &ManifestTask,
    profile: ArchiveV2WireProfile,
    attestation_root: &Path,
) -> Vec<String> {
    let mut argv = vec![
        "--archive".into(),
        task.archive.to_string_lossy().into_owned(),
        "--epoch".into(),
        task.epoch.to_string(),
        "--registry-order".into(),
        task.registry_order.clone(),
        "--generation-kind".into(),
        task.generation_kind.clone(),
    ];
    if let Some(receipt) = &task.registry_receipt {
        argv.push("--registry-receipt".into());
        argv.push(receipt.to_string_lossy().into_owned());
    }
    argv.extend([
        "--content-generation-sha256".into(),
        task.content_generation_sha256.clone(),
        "--wire-profile".into(),
        profile.to_string(),
        "--attestation-root".into(),
        attestation_root.to_string_lossy().into_owned(),
    ]);
    argv
}

fn spawn_attempt(
    args: &Args,
    manifest: &BatchManifest,
    manifest_sha256: &str,
    task: &ManifestTask,
    state: &mut BatchState,
    client: &Client,
    active: &mut BTreeMap<String, ActiveAttempt>,
) -> Result<()> {
    validate_manifest_task(task, &manifest.attestation_root)?;
    verify_pinned_files(manifest, state)?;
    verify_pinned_hashes(manifest, state)?;
    ensure!(
        sha256_file_pinned(&state.pinned_audit_binary.path)? == state.pinned_audit_binary.sha256,
        "pinned audit binary hash changed before spawn"
    );
    let task_state = state
        .tasks
        .get(&task.task_id)
        .context("spawn task is absent from state")?;
    ensure!(
        matches!(
            task_state.status,
            TaskStatus::Pending | TaskStatus::RetryWait
        ) && task_state.attempt.is_none(),
        "task is not runnable"
    );
    let profile = task.profile_attempts[usize::from(task_state.profile_index)];
    let attempt_id = new_attempt_id()?;
    let log = args
        .state_root
        .join("logs")
        .join(format!("{attempt_id}.log"));
    let stdout = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&log)?;
    let stderr = stdout.try_clone()?;
    let argv = audit_argv(task, profile, &manifest.attestation_root);
    let attempt = AttemptState {
        attempt_id: attempt_id.clone(),
        profile,
        executable: state.pinned_audit_binary.path.clone(),
        executable_device: state.pinned_audit_binary.device,
        executable_inode: state.pinned_audit_binary.inode,
        argv: argv.clone(),
        pid: None,
        process_start_ticks: None,
        pgid: None,
        log,
        started_unix_secs: unix_now(),
        paused: false,
        pause_reason: None,
    };
    // Finish every fallible admission check before a child exists. A failed
    // preflight therefore leaves no durable phantom attempt.
    let descriptor = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&state.pinned_audit_binary.path)?;
    let descriptor_before = descriptor.metadata()?;
    ensure!(
        descriptor_before.dev() == state.pinned_audit_binary.device
            && descriptor_before.ino() == state.pinned_audit_binary.inode
            && descriptor_before.len() == state.pinned_audit_binary.size,
        "pinned audit descriptor identity changed before spawn"
    );
    let owned = owned_processes_from_state(state);
    ensure!(
        verify_external_work_guard(manifest, state, &owned)?.is_none(),
        "external protected work appeared before audit spawn"
    );
    let scheduler = fetch_scheduler_status(client, &args.scheduler_status_url)?;
    let cgroup = read_bound_cgroup(&state.cgroup)?;
    ensure!(
        !cgroup_hard_event_changed(state.cgroup_hard_event_baseline, cgroup.events.into(),),
        "cgroup hard-event counters changed before audit spawn"
    );
    let current_workers = state
        .tasks
        .values()
        .filter(|state| matches!(state.status, TaskStatus::Running | TaskStatus::Paused))
        .count();
    ensure!(
        admission_safe(&manifest.limits, &scheduler, &cgroup, current_workers + 1),
        "resource or scheduler state changed before audit spawn"
    );

    // A Starting record is the crash authority for the narrow fork/exec to PID
    // binding window. All admission checks above completed before this write.
    {
        let task_state = state.tasks.get_mut(&task.task_id).unwrap();
        task_state.status = TaskStatus::Starting;
        task_state.attempt = Some(attempt.clone());
        task_state.retry_not_before_unix_secs = None;
        task_state.last_message = Some(format!("starting profile {profile}"));
    }
    if let Err(error) = append_event_and_snapshot(
        args,
        state,
        manifest_sha256,
        Some(&task.task_id),
        "attempt_starting",
        &format!("starting profile {profile}"),
    ) {
        // The event may have committed while the state snapshot did not. Leave
        // a zero-consume receipt that is valid against the Starting record if it
        // became visible, then restore the in-memory task before returning.
        let receipt_result = publish_attempt_receipt(
            args,
            manifest_sha256,
            &task.task_id,
            &attempt,
            "supervisor_interruption_intent",
            None,
        );
        schedule_same_profile_retry(
            task,
            state.tasks.get_mut(&task.task_id).unwrap(),
            "starting state publication failed",
            false,
        );
        if let Err(receipt_error) = receipt_result {
            return Err(error).context(format!(
                "persist starting audit attempt; zero-consume cancellation receipt also failed: {receipt_error:#}"
            ));
        }
        return Err(error).context("persist starting audit attempt");
    }
    let mut command = Command::new(&state.pinned_audit_binary.path);
    command
        .args(&argv)
        .env_clear()
        .env("BLOCKZILLA_FIREWATCH_AUDIT_BATCH_ATTEMPT_ID", &attempt_id)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let supervisor_pid = unsafe { libc::getpid() };
    // SAFETY: this is the only pre-exec hook. It uses only async-signal-safe
    // syscalls and returns an OS error without allocation. The order is part of
    // the child-lifetime contract: arm parent death first, close the parent-death
    // race second, and create the private process group last.
    unsafe {
        command.pre_exec(move || configure_audit_child_lifetime(supervisor_pid));
    }
    let child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let cancel_result = cancel_starting_attempt(
                args,
                manifest_sha256,
                task,
                state,
                None,
                "audit spawn failed before a child existed",
            );
            if let Err(cancel_error) = cancel_result {
                // No child exists. Restore a nonterminal in-memory state so the
                // central error snapshot cannot publish a phantom Starting row.
                schedule_same_profile_retry(
                    task,
                    state.tasks.get_mut(&task.task_id).unwrap(),
                    "audit spawn failed before a child existed",
                    false,
                );
                return Err(error).context(format!(
                    "spawn audit task {}; zero-consume cancellation also failed: {cancel_error:#}",
                    task.task_id
                ));
            }
            return Err(error).with_context(|| format!("spawn audit task {}", task.task_id));
        }
    };
    let pid = child.id();
    let mut guard = ActiveAttempt {
        child: Some(child),
        pid,
        start_ticks: 0,
    };
    let start_ticks = match wait_for_spawned_identity(
        &attempt,
        guard.child.as_mut().unwrap(),
        pid,
        &state.cgroup,
    ) {
        Ok(ticks) => ticks,
        Err(error) => {
            let cancel_result = cancel_owned_starting_child(
                args,
                manifest_sha256,
                task,
                state,
                None,
                "audit child identity binding failed",
                guard
                    .child
                    .as_mut()
                    .context("spawned audit child handle is missing")?,
            );
            cancel_result.context("cancel child after identity binding failure")?;
            return Err(error).context("bind spawned audit process identity");
        }
    };
    guard.start_ticks = start_ticks;
    let post_bind_validation = (|| -> Result<()> {
        let descriptor_after = descriptor.metadata()?;
        let path_after = fs::symlink_metadata(&state.pinned_audit_binary.path)?;
        ensure!(
            same_file(&descriptor_before, &descriptor_after)
                && same_version(&descriptor_before, &descriptor_after)
                && descriptor_after.dev() == path_after.dev()
                && descriptor_after.ino() == path_after.ino()
                && process_start_ticks(pid) == Some(start_ticks)
                && process_group_id(pid) == Some(pid),
            "pinned audit identity changed during spawn"
        );
        Ok(())
    })();
    if let Err(error) = post_bind_validation {
        let mut bound_attempt = attempt.clone();
        bound_attempt.pid = Some(pid);
        bound_attempt.process_start_ticks = Some(start_ticks);
        bound_attempt.pgid = Some(pid);
        let cancel_result = cancel_owned_starting_child(
            args,
            manifest_sha256,
            task,
            state,
            Some(&bound_attempt),
            "new audit identity changed before durable binding",
            guard
                .child
                .as_mut()
                .context("spawned audit child handle is missing")?,
        );
        cancel_result.context("cancel child after post-bind validation failure")?;
        return Err(error).context("validate newly spawned audit identity");
    }

    let mut durable_attempt = attempt;
    durable_attempt.pid = Some(pid);
    durable_attempt.process_start_ticks = Some(start_ticks);
    durable_attempt.pgid = Some(pid);
    {
        let task_state = state.tasks.get_mut(&task.task_id).unwrap();
        task_state.attempt = Some(durable_attempt);
        task_state.status = TaskStatus::Running;
        task_state.retry_not_before_unix_secs = None;
        task_state.last_message = Some(format!("running profile {profile}"));
    }
    ensure!(
        active.insert(task.task_id.clone(), guard).is_none(),
        "spawn task already has an active audit"
    );
    // From this point the central active-loop error boundary owns cleanup. This
    // is important because an atomic state publication can return an error after
    // its rename became visible but before the parent-directory fsync returned.
    append_event_and_snapshot(
        args,
        state,
        manifest_sha256,
        Some(&task.task_id),
        "attempt_running",
        &format!("running profile {profile} as PID {pid}"),
    )?;
    let post_scheduler = fetch_scheduler_status(client, &args.scheduler_status_url)?;
    let post_cgroup = read_bound_cgroup(&state.cgroup)?;
    ensure!(
        !cgroup_hard_event_changed(state.cgroup_hard_event_baseline, post_cgroup.events.into(),)
            && admission_safe(
                &manifest.limits,
                &post_scheduler,
                &post_cgroup,
                current_workers + 1,
            ),
        "resource or scheduler state changed immediately after audit spawn"
    );
    Ok(())
}

fn cancel_starting_attempt(
    args: &Args,
    manifest_sha256: &str,
    task: &ManifestTask,
    state: &mut BatchState,
    bound_attempt: Option<&AttemptState>,
    reason: &str,
) -> Result<()> {
    let durable = state.tasks[&task.task_id]
        .attempt
        .clone()
        .context("starting cancellation has no durable attempt")?;
    let receipt_attempt = bound_attempt.unwrap_or(&durable);
    let receipt_result = publish_attempt_receipt(
        args,
        manifest_sha256,
        &task.task_id,
        receipt_attempt,
        "supervisor_interruption_intent",
        None,
    );
    schedule_same_profile_retry(
        task,
        state.tasks.get_mut(&task.task_id).unwrap(),
        reason,
        false,
    );
    let snapshot_result = append_event_and_snapshot(
        args,
        state,
        manifest_sha256,
        Some(&task.task_id),
        "attempt_start_cancelled",
        reason,
    );
    match (receipt_result, snapshot_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(receipt), Ok(())) => Err(receipt).context(
            "publish zero-consume starting cancellation receipt; nonterminal state was restored",
        ),
        (Ok(()), Err(snapshot)) => Err(snapshot)
            .context("publish nonterminal state after durable starting cancellation receipt"),
        (Err(receipt), Err(snapshot)) => Err(receipt).context(format!(
            "publish zero-consume starting cancellation receipt; nonterminal state publication also failed: {snapshot:#}"
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn cancel_owned_starting_child(
    args: &Args,
    manifest_sha256: &str,
    task: &ManifestTask,
    state: &mut BatchState,
    bound_attempt: Option<&AttemptState>,
    reason: &str,
    child: &mut Child,
) -> Result<()> {
    let durable = state.tasks[&task.task_id]
        .attempt
        .clone()
        .context("owned starting cancellation has no durable attempt")?;
    let receipt_attempt = bound_attempt.unwrap_or(&durable);
    let receipt_result = publish_attempt_receipt(
        args,
        manifest_sha256,
        &task.task_id,
        receipt_attempt,
        "supervisor_interruption_intent",
        None,
    );

    // A direct signal is permitted only after its immutable intent exists. If
    // publication failed, keep the supervisor alive and reap the child only
    // after it exits without a supervisor signal.
    let settle_result = if receipt_result.is_ok() {
        kill_and_reap_direct_child(child).map(|_| ())
    } else {
        wait_and_reap_direct_child_without_signal(child).map(|_| ())
    };

    schedule_same_profile_retry(
        task,
        state.tasks.get_mut(&task.task_id).unwrap(),
        reason,
        false,
    );
    let snapshot_result = append_event_and_snapshot(
        args,
        state,
        manifest_sha256,
        Some(&task.task_id),
        "attempt_start_cancelled",
        reason,
    );
    combine_cleanup_results(receipt_result, settle_result, snapshot_result)
}

fn combine_cleanup_results(
    receipt: Result<()>,
    settle: Result<()>,
    snapshot: Result<()>,
) -> Result<()> {
    let mut errors = Vec::new();
    if let Err(error) = receipt {
        errors.push(format!("interruption receipt: {error:#}"));
    }
    if let Err(error) = settle {
        errors.push(format!("child settlement: {error:#}"));
    }
    if let Err(error) = snapshot {
        errors.push(format!("nonterminal state publication: {error:#}"));
    }
    ensure!(errors.is_empty(), "{}", errors.join("; "));
    Ok(())
}

fn kill_and_reap_direct_child(child: &mut Child) -> Result<ExitStatus> {
    if let Some(status) = child.try_wait()? {
        return Ok(status);
    }
    let kill_error = child.kill().err();
    loop {
        match child.wait() {
            Ok(status) => return Ok(status),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                if let Some(status) = child.try_wait()? {
                    return Ok(status);
                }
                if let Some(kill_error) = kill_error.as_ref() {
                    bail!(
                        "kill directly owned audit child failed: {kill_error}; reap also failed: {error}"
                    );
                }
                return Err(error).context("reap directly owned audit child after SIGKILL");
            }
        }
    }
}

fn wait_and_reap_direct_child_without_signal(child: &mut Child) -> Result<ExitStatus> {
    loop {
        match child.wait() {
            Ok(status) => return Ok(status),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error).context("reap directly owned audit child"),
        }
    }
}

#[cfg(target_os = "linux")]
unsafe fn configure_audit_child_lifetime(supervisor_pid: libc::pid_t) -> std::io::Result<()> {
    // SAFETY: the caller is the single Command pre-exec closure. The syscalls
    // are ordered to close the fork/parent-death race before group creation.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::getppid() } != supervisor_pid {
        unsafe { libc::_exit(127) };
    }
    if unsafe { libc::setpgid(0, 0) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
unsafe fn configure_audit_child_lifetime(_supervisor_pid: libc::pid_t) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "audit batch execution requires Linux PR_SET_PDEATHSIG",
    ))
}

fn wait_for_spawned_identity(
    attempt: &AttemptState,
    child: &mut Child,
    pid: u32,
    cgroup: &CgroupBinding,
) -> Result<u64> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child.try_wait()? {
            bail!("audit child exited before identity binding: {status}");
        }
        if let Some(ticks) = process_start_ticks(pid)
            && process_matches_attempt(attempt, pid, cgroup)?
        {
            return Ok(ticks);
        }
        ensure!(
            Instant::now() < deadline,
            "audit child did not expose its exact identity within 3 seconds"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn reap_finished_attempts(
    args: &Args,
    _manifest: &BatchManifest,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &mut BatchState,
    active: &mut BTreeMap<String, ActiveAttempt>,
) -> Result<()> {
    let task_ids = active.keys().cloned().collect::<Vec<_>>();
    for task_id in task_ids {
        let outcome = {
            let process = active.get_mut(&task_id).unwrap();
            match process.child.as_mut() {
                Some(child) => child.try_wait()?.map(KnownOrLostExit::Known),
                None if process_start_ticks(process.pid) != Some(process.start_ticks) => {
                    Some(KnownOrLostExit::Lost)
                }
                None => None,
            }
        };
        let Some(outcome) = outcome else {
            continue;
        };
        active.remove(&task_id).unwrap();
        let task = task_map[&task_id];
        let attempt = state.tasks[&task_id]
            .attempt
            .clone()
            .context("finished process has no durable attempt")?;
        if let Some((attestation_sha256, profile)) = validate_exact_attestation(task)? {
            publish_attempt_receipt(
                args,
                manifest_sha256,
                &task_id,
                &attempt,
                "attested",
                outcome.status(),
            )?;
            mark_complete(
                args,
                manifest_sha256,
                task,
                state,
                attestation_sha256,
                profile,
                "exact attestation verified after audit exit",
            )?;
            continue;
        }

        let disposition = outcome.disposition();
        publish_attempt_receipt(
            args,
            manifest_sha256,
            &task_id,
            &attempt,
            disposition.stable_id(),
            outcome.status(),
        )?;
        apply_exit_disposition(
            args,
            manifest_sha256,
            task,
            state,
            disposition,
            outcome.describe(),
        )?;
    }
    Ok(())
}

enum KnownOrLostExit {
    Known(ExitStatus),
    Lost,
}

impl KnownOrLostExit {
    fn disposition(&self) -> ExitDisposition {
        match self {
            Self::Known(status) if status.code() == Some(0) => ExitDisposition::RetrySameProfile,
            Self::Known(status)
                if status.signal().is_none()
                    && status.code() == Some(SELECTED_PROFILE_DECODE_REJECTED_EXIT_CODE) =>
            {
                ExitDisposition::AdvanceProfile
            }
            Self::Known(status)
                if status.signal().is_none()
                    && status.code() == Some(TERMINAL_PROFILE_AUDIT_REJECTED_EXIT_CODE) =>
            {
                ExitDisposition::TerminalBlocked
            }
            Self::Known(_) | Self::Lost => ExitDisposition::RetrySameProfile,
        }
    }

    fn status(&self) -> Option<&ExitStatus> {
        match self {
            Self::Known(status) => Some(status),
            Self::Lost => None,
        }
    }

    fn describe(&self) -> String {
        match self {
            Self::Known(status) => format!("audit exited with {status}"),
            Self::Lost => "audit exit was lost after supervisor restart".into(),
        }
    }
}

impl ExitDisposition {
    fn stable_id(self) -> &'static str {
        match self {
            Self::AdvanceProfile => "selected_profile_decode_rejected",
            Self::TerminalBlocked => "terminal_profile_audit_rejected",
            Self::RetrySameProfile => "operational_failure",
        }
    }
}

fn apply_exit_disposition(
    args: &Args,
    manifest_sha256: &str,
    task: &ManifestTask,
    state: &mut BatchState,
    disposition: ExitDisposition,
    message: String,
) -> Result<()> {
    if let Some((profile, terminal_message)) = terminal_block_plan(
        task,
        &state.tasks[&task.task_id],
        disposition,
        &message,
        true,
    ) {
        // A terminal receipt is the authority for a terminal task state. It
        // must be durable before the in-memory state can become Blocked.
        publish_terminal_receipt(
            args,
            manifest_sha256,
            task,
            TaskStatus::Blocked,
            profile,
            None,
            &terminal_message,
        )?;
    }
    let task_state = state.tasks.get_mut(&task.task_id).unwrap();
    task_state.attempt = None;
    match disposition {
        ExitDisposition::AdvanceProfile => {
            if usize::from(task_state.profile_index) + 1 >= task.profile_attempts.len() {
                task_state.status = TaskStatus::Blocked;
                task_state.last_message = Some("no compatible wire profile".into());
                task_state.retry_not_before_unix_secs = None;
            } else {
                task_state.profile_index += 1;
                task_state.operational_retries = 0;
                task_state.status = TaskStatus::RetryWait;
                task_state.retry_not_before_unix_secs = Some(unix_now());
                task_state.last_message = Some(format!(
                    "first profile rejected; next profile is {}",
                    task.profile_attempts[usize::from(task_state.profile_index)]
                ));
            }
        }
        ExitDisposition::TerminalBlocked => {
            task_state.status = TaskStatus::Blocked;
            task_state.retry_not_before_unix_secs = None;
            task_state.last_message = Some(message.clone());
        }
        ExitDisposition::RetrySameProfile => {
            schedule_same_profile_retry(task, task_state, &message, true);
        }
    }
    append_event_and_snapshot(
        args,
        state,
        manifest_sha256,
        Some(&task.task_id),
        disposition.stable_id(),
        &message,
    )
}

fn terminal_block_plan(
    task: &ManifestTask,
    state: &TaskState,
    disposition: ExitDisposition,
    message: &str,
    consume_retry: bool,
) -> Option<(ArchiveV2WireProfile, String)> {
    let terminal_message = match disposition {
        ExitDisposition::AdvanceProfile
            if usize::from(state.profile_index) + 1 >= task.profile_attempts.len() =>
        {
            "both selected profile attempts rejected decoding".to_owned()
        }
        ExitDisposition::TerminalBlocked => message.to_owned(),
        ExitDisposition::RetrySameProfile
            if consume_retry && state.operational_retries >= MAX_OPERATIONAL_RETRIES =>
        {
            format!("operational retry limit reached after: {message}")
        }
        _ => return None,
    };
    Some((
        task.profile_attempts[usize::from(state.profile_index)],
        terminal_message,
    ))
}

fn schedule_same_profile_retry(
    _task: &ManifestTask,
    state: &mut TaskState,
    message: &str,
    consume_retry: bool,
) {
    state.attempt = None;
    if consume_retry {
        if state.operational_retries >= MAX_OPERATIONAL_RETRIES {
            state.status = TaskStatus::Blocked;
            state.retry_not_before_unix_secs = None;
            state.last_message = Some(format!("operational retry limit reached after: {message}"));
            return;
        }
        state.operational_retries += 1;
    }
    let delay = if consume_retry {
        15u64.saturating_mul(1u64 << state.operational_retries.saturating_sub(1).min(4))
    } else {
        0
    };
    state.status = TaskStatus::RetryWait;
    state.retry_not_before_unix_secs = Some(unix_now().saturating_add(delay));
    state.last_message = Some(message.into());
}

fn pause_all_active(
    args: &Args,
    state: &mut BatchState,
    active: &mut BTreeMap<String, ActiveAttempt>,
    reason: &str,
) -> Result<()> {
    let task_ids = active.keys().cloned().collect::<Vec<_>>();
    for task_id in task_ids {
        if state.tasks[&task_id]
            .attempt
            .as_ref()
            .is_some_and(|attempt| attempt.paused)
        {
            continue;
        }
        let process = active.get(&task_id).unwrap();
        ensure!(
            verified_attempt_live(state, &task_id, process)?,
            "cannot pause an unverified audit process"
        );
        signal_verified_attempt(state, &task_id, process, libc::SIGSTOP)?;
        let task_state = state.tasks.get_mut(&task_id).unwrap();
        let attempt = task_state.attempt.as_mut().unwrap();
        attempt.paused = true;
        attempt.pause_reason = Some(reason.into());
        task_state.status = TaskStatus::Paused;
        task_state.last_message = Some(reason.into());
    }
    if !active.is_empty() {
        append_event_and_snapshot(
            args,
            state,
            &state.manifest_sha256.clone(),
            None,
            "pool_paused",
            reason,
        )?;
    }
    Ok(())
}

fn resume_all_active(
    args: &Args,
    state: &mut BatchState,
    active: &mut BTreeMap<String, ActiveAttempt>,
) -> Result<()> {
    let mut resumed = false;
    let task_ids = active.keys().cloned().collect::<Vec<_>>();
    for task_id in task_ids {
        if !state.tasks[&task_id]
            .attempt
            .as_ref()
            .is_some_and(|attempt| attempt.paused)
        {
            continue;
        }
        let process = active.get(&task_id).unwrap();
        ensure!(
            verified_attempt_live(state, &task_id, process)?,
            "cannot resume an unverified audit process"
        );
        signal_verified_attempt(state, &task_id, process, libc::SIGCONT)?;
        let task_state = state.tasks.get_mut(&task_id).unwrap();
        let attempt = task_state.attempt.as_mut().unwrap();
        attempt.paused = false;
        attempt.pause_reason = None;
        task_state.status = TaskStatus::Running;
        task_state.last_message = Some("resources stable; audit resumed".into());
        resumed = true;
    }
    if resumed {
        append_event_and_snapshot(
            args,
            state,
            &state.manifest_sha256.clone(),
            None,
            "pool_resumed",
            "resources were stable for the full resume window",
        )?;
    }
    Ok(())
}

fn interrupt_all_active(
    args: &Args,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &mut BatchState,
    active: &mut BTreeMap<String, ActiveAttempt>,
    reason: &str,
) -> Result<()> {
    let task_ids = active.keys().cloned().collect::<Vec<_>>();
    let mut authority_errors = Vec::new();

    // Phase one publishes authority for the whole pool. Do not require the
    // process to remain live here: it can exit between the normal reap pass and
    // a later resource or supervisor error.
    for task_id in &task_ids {
        let mut attempt = state.tasks[task_id].attempt.clone().unwrap();
        let process = active.get(task_id).unwrap();
        if attempt.pid.is_none() {
            attempt.pid = Some(process.pid);
            attempt.process_start_ticks = Some(process.start_ticks);
            attempt.pgid = Some(process.pid);
        }
        if let Err(error) = publish_attempt_receipt(
            args,
            manifest_sha256,
            task_id,
            &attempt,
            "supervisor_interruption_intent",
            None,
        ) {
            authority_errors.push(format!("{task_id}: {error:#}"));
        }
    }

    if !authority_errors.is_empty() {
        // Never signal a partial pool. Keep this supervisor alive until every
        // worker that lacks whole-pool authority exits without a supervisor
        // signal, and reap every owned child before returning the error.
        for task_id in &task_ids {
            wait_active_attempt_without_signal(active.get_mut(task_id).unwrap());
        }
        for task_id in &task_ids {
            active.remove(task_id).unwrap();
            schedule_same_profile_retry(
                task_map[task_id],
                state.tasks.get_mut(task_id).unwrap(),
                reason,
                false,
            );
        }
        let snapshot_result = append_event_and_snapshot(
            args,
            state,
            manifest_sha256,
            None,
            "pool_interruption_authority_failed",
            reason,
        );
        if let Err(error) = snapshot_result {
            authority_errors.push(format!("nonterminal state publication: {error:#}"));
        }
        bail!(
            "whole-pool interruption authority failed: {}",
            authority_errors.join("; ")
        );
    }

    // Phase two settles every child under the already durable intent.
    let mut settlement_errors = Vec::new();
    for task_id in &task_ids {
        if let Err(error) = {
            let process = active.get_mut(task_id).unwrap();
            settle_active_attempt(state, task_id, process)
        } {
            settlement_errors.push(format!("{task_id}: {error:#}"));
        }
    }
    if !settlement_errors.is_empty() {
        for task_id in &task_ids {
            wait_active_attempt_without_signal(active.get_mut(task_id).unwrap());
        }
    }
    for task_id in &task_ids {
        active.remove(task_id).unwrap();
        schedule_same_profile_retry(
            task_map[task_id],
            state.tasks.get_mut(task_id).unwrap(),
            reason,
            false,
        );
    }
    if !task_ids.is_empty() {
        if let Err(error) = append_event_and_snapshot(
            args,
            state,
            manifest_sha256,
            None,
            "pool_interrupted",
            reason,
        ) {
            settlement_errors.push(format!("nonterminal state publication: {error:#}"));
        }
    }
    ensure!(
        settlement_errors.is_empty(),
        "whole-pool settlement failed: {}",
        settlement_errors.join("; ")
    );
    Ok(())
}

fn shutdown_all_active(
    args: &Args,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &mut BatchState,
    active: &mut BTreeMap<String, ActiveAttempt>,
    reason: &str,
) -> Result<()> {
    let task_ids = active.keys().cloned().collect::<Vec<_>>();
    let mut attested = BTreeMap::new();
    let mut interrupted_count = 0usize;

    // Phase one is entirely durable. Publish authority for every remaining
    // child before waiting for or signaling any one child.
    for task_id in &task_ids {
        let task = task_map[task_id];
        if let Some((attestation_sha256, profile)) = validate_exact_attestation(task)? {
            publish_terminal_receipt(
                args,
                manifest_sha256,
                task,
                TaskStatus::Complete,
                profile,
                Some(attestation_sha256.clone()),
                "exact attestation verified during supervisor shutdown",
            )?;
            attested.insert(task_id.clone(), (attestation_sha256, profile));
        } else {
            interrupted_count += 1;
            let attempt = state.tasks[task_id]
                .attempt
                .clone()
                .context("active shutdown task has no durable attempt")?;
            publish_attempt_receipt(
                args,
                manifest_sha256,
                task_id,
                &attempt,
                "supervisor_interruption_intent",
                None,
            )?;
        }
    }

    // Phase two can now settle every child. A child that exited in the race
    // after the normal reap pass still has the deliberate zero-consume receipt.
    for task_id in &task_ids {
        let task = task_map[task_id];
        settle_active_attempt(state, task_id, active.get_mut(task_id).unwrap())?;
        active.remove(task_id).unwrap();
        if let Some((attestation_sha256, profile)) = attested.remove(task_id) {
            mark_complete(
                args,
                manifest_sha256,
                task,
                state,
                attestation_sha256,
                profile,
                "exact attestation verified during supervisor shutdown",
            )?;
        } else {
            schedule_same_profile_retry(task, state.tasks.get_mut(task_id).unwrap(), reason, false);
        }
    }
    if interrupted_count > 0 {
        append_event_and_snapshot(
            args,
            state,
            manifest_sha256,
            None,
            "pool_interrupted",
            reason,
        )?;
    }
    Ok(())
}

fn settle_active_attempt(
    state: &BatchState,
    task_id: &str,
    process: &mut ActiveAttempt,
) -> Result<()> {
    if verified_attempt_live(state, task_id, process)? {
        terminate_verified_attempt(state, task_id, process)
    } else if let Some(child) = process.child.as_mut() {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        kill_and_reap_direct_child(child)
            .map(|_| ())
            .context("stop directly owned audit after exact identity changed")
    } else {
        wait_for_process_absence(process.pid, process.start_ticks)
    }
}

fn verified_attempt_live(
    state: &BatchState,
    task_id: &str,
    process: &ActiveAttempt,
) -> Result<bool> {
    let attempt = state
        .tasks
        .get(task_id)
        .and_then(|task| task.attempt.as_ref())
        .context("active process has no durable attempt")?;
    Ok(attempt.pid == Some(process.pid)
        && attempt.process_start_ticks == Some(process.start_ticks)
        && attempt.pgid == Some(process.pid)
        && process_start_ticks(process.pid) == Some(process.start_ticks)
        && process_group_id(process.pid) == Some(process.pid)
        && process_matches_attempt(attempt, process.pid, &state.cgroup)?)
}

fn signal_verified_attempt(
    state: &BatchState,
    task_id: &str,
    process: &ActiveAttempt,
    signal: libc::c_int,
) -> Result<()> {
    ensure!(
        verified_attempt_live(state, task_id, process)?,
        "audit process identity changed before signal"
    );
    // SAFETY: exact PID start ticks, PGID, executable, argv, environment, and cgroup
    // were revalidated immediately above.
    let result = unsafe { libc::kill(-(process.pid as libc::pid_t), signal) };
    if result == 0 {
        ensure!(
            process_start_ticks(process.pid) == Some(process.start_ticks),
            "audit process identity changed immediately after signal"
        );
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()).context("signal verified audit process group")
    }
}

fn terminate_verified_attempt(
    state: &BatchState,
    task_id: &str,
    process: &mut ActiveAttempt,
) -> Result<()> {
    if let Some(child) = process.child.as_mut() {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
    } else if process_start_ticks(process.pid) != Some(process.start_ticks) {
        return Ok(());
    }
    ensure!(
        verified_attempt_live(state, task_id, process)?,
        "audit process identity changed before termination"
    );
    // A stopped process must run to act on SIGTERM.
    // SAFETY: exact identity was validated immediately above.
    let continued = unsafe { libc::kill(-(process.pid as libc::pid_t), libc::SIGCONT) };
    ensure!(
        continued == 0,
        "continue exact audit process before termination failed: {}",
        std::io::Error::last_os_error()
    );
    ensure!(
        verified_attempt_live(state, task_id, process)?,
        "audit process identity changed before SIGTERM"
    );
    // SAFETY: exact identity was revalidated immediately above.
    let terminated = unsafe { libc::kill(-(process.pid as libc::pid_t), libc::SIGTERM) };
    ensure!(
        terminated == 0,
        "terminate exact audit process failed: {}",
        std::io::Error::last_os_error()
    );
    for _ in 0..100 {
        if let Some(child) = process.child.as_mut() {
            if child.try_wait()?.is_some() {
                return Ok(());
            }
        } else if process_start_ticks(process.pid) != Some(process.start_ticks) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    ensure!(
        verified_attempt_live(state, task_id, process)?,
        "audit process identity changed before SIGKILL"
    );
    // SAFETY: exact identity was revalidated immediately above.
    let killed = unsafe { libc::kill(-(process.pid as libc::pid_t), libc::SIGKILL) };
    ensure!(
        killed == 0,
        "kill exact audit process failed: {}",
        std::io::Error::last_os_error()
    );
    if let Some(child) = process.child.as_mut() {
        child.wait()?;
        Ok(())
    } else {
        wait_for_process_absence(process.pid, process.start_ticks)
    }
}

fn owned_processes_from_state(state: &BatchState) -> BTreeMap<String, (u32, u64)> {
    state
        .tasks
        .iter()
        .filter_map(|(task_id, task)| {
            let attempt = task.attempt.as_ref()?;
            Some((
                task_id.clone(),
                (attempt.pid?, attempt.process_start_ticks?),
            ))
        })
        .collect()
}

fn block_nonterminal_tasks(
    args: &Args,
    manifest_sha256: &str,
    task_map: &BTreeMap<String, &ManifestTask>,
    state: &mut BatchState,
    reason: &str,
) -> Result<()> {
    let task_ids = state.tasks.keys().cloned().collect::<Vec<_>>();
    for task_id in task_ids {
        if matches!(
            state.tasks[&task_id].status,
            TaskStatus::Complete | TaskStatus::Blocked
        ) {
            continue;
        }
        let task = task_map[&task_id];
        let profile = task.profile_attempts[usize::from(state.tasks[&task_id].profile_index)];
        publish_terminal_receipt(
            args,
            manifest_sha256,
            task,
            TaskStatus::Blocked,
            profile,
            None,
            reason,
        )?;
        let task_state = state.tasks.get_mut(&task_id).unwrap();
        task_state.attempt = None;
        task_state.status = TaskStatus::Blocked;
        task_state.retry_not_before_unix_secs = None;
        task_state.last_message = Some(reason.into());
    }
    append_event_and_snapshot(args, state, manifest_sha256, None, "batch_blocked", reason)
}

fn mark_complete(
    args: &Args,
    manifest_sha256: &str,
    task: &ManifestTask,
    state: &mut BatchState,
    attestation_sha256: String,
    profile: ArchiveV2WireProfile,
    message: &str,
) -> Result<()> {
    ensure!(
        state.tasks[&task.task_id].status != TaskStatus::Blocked,
        "a blocked immutable batch task requires a new manifest"
    );
    ensure!(
        task.profile_attempts.contains(&profile),
        "completed profile is outside the manifest"
    );
    publish_terminal_receipt(
        args,
        manifest_sha256,
        task,
        TaskStatus::Complete,
        profile,
        Some(attestation_sha256),
        message,
    )?;
    let task_state = state.tasks.get_mut(&task.task_id).unwrap();
    task_state.status = TaskStatus::Complete;
    task_state.profile_index = u8::try_from(
        task.profile_attempts
            .iter()
            .position(|candidate| *candidate == profile)
            .expect("profile membership checked"),
    )?;
    task_state.attempt = None;
    task_state.retry_not_before_unix_secs = None;
    task_state.last_message = Some(message.into());
    append_event_and_snapshot(
        args,
        state,
        manifest_sha256,
        Some(&task.task_id),
        "task_complete",
        message,
    )
}

#[allow(clippy::too_many_arguments)]
fn publish_terminal_receipt(
    args: &Args,
    manifest_sha256: &str,
    task: &ManifestTask,
    status: TaskStatus,
    profile: ArchiveV2WireProfile,
    attestation_sha256: Option<String>,
    message: &str,
) -> Result<()> {
    ensure!(
        matches!(status, TaskStatus::Complete | TaskStatus::Blocked),
        "terminal receipt has a nonterminal status"
    );
    ensure!(
        (status == TaskStatus::Complete) == attestation_sha256.is_some(),
        "only a completed task can bind an attestation"
    );
    if let Some(digest) = attestation_sha256.as_deref() {
        ensure!(is_sha256(digest), "terminal attestation hash is invalid");
    }
    let receipt = TerminalReceipt {
        schema_version: TERMINAL_RECEIPT_SCHEMA_VERSION,
        kind: "firewatch_wire_profile_audit_batch_terminal".into(),
        manifest_sha256: manifest_sha256.into(),
        task_id: task.task_id.clone(),
        epoch: task.epoch,
        status,
        profile,
        expected_attestation: task.expected_attestation.clone(),
        attestation_sha256,
        unix_secs: unix_now(),
        message: bounded_message(message),
    };
    let path = args
        .state_root
        .join("terminal")
        .join(format!("{}.json", task.task_id));
    publish_json_no_replace_or_exact(&path, &receipt, |existing: &TerminalReceipt| {
        existing.schema_version == receipt.schema_version
            && existing.kind == receipt.kind
            && existing.manifest_sha256 == receipt.manifest_sha256
            && existing.task_id == receipt.task_id
            && existing.epoch == receipt.epoch
            && existing.status == receipt.status
            && existing.profile == receipt.profile
            && existing.expected_attestation == receipt.expected_attestation
            && existing.attestation_sha256 == receipt.attestation_sha256
    })
}

fn publish_attempt_receipt(
    args: &Args,
    manifest_sha256: &str,
    task_id: &str,
    attempt: &AttemptState,
    outcome: &str,
    status: Option<&ExitStatus>,
) -> Result<()> {
    let receipt = AttemptReceipt {
        schema_version: ATTEMPT_RECEIPT_SCHEMA_VERSION,
        kind: "firewatch_wire_profile_audit_batch_attempt".into(),
        manifest_sha256: manifest_sha256.into(),
        task_id: task_id.into(),
        attempt_id: attempt.attempt_id.clone(),
        profile: attempt.profile,
        pid: attempt.pid,
        process_start_ticks: attempt.process_start_ticks,
        pgid: attempt.pgid,
        executable_device: attempt.executable_device,
        executable_inode: attempt.executable_inode,
        executable_sha256: state_independent_audit_sha(args, attempt)?,
        argv: attempt.argv.clone(),
        started_unix_secs: attempt.started_unix_secs,
        finished_unix_secs: unix_now(),
        outcome: outcome.into(),
        exit_code: status.and_then(ExitStatus::code),
        signal: status.and_then(ExitStatusExt::signal),
        log: attempt.log.clone(),
    };
    let path = args
        .state_root
        .join("attempts")
        .join(format!("{}.json", attempt.attempt_id));
    publish_json_no_replace_or_exact(&path, &receipt, |existing: &AttemptReceipt| {
        existing.schema_version == receipt.schema_version
            && existing.kind == receipt.kind
            && existing.manifest_sha256 == receipt.manifest_sha256
            && existing.task_id == receipt.task_id
            && existing.attempt_id == receipt.attempt_id
            && existing.profile == receipt.profile
            && existing.pid == receipt.pid
            && existing.process_start_ticks == receipt.process_start_ticks
            && existing.pgid == receipt.pgid
            && existing.executable_device == receipt.executable_device
            && existing.executable_inode == receipt.executable_inode
            && existing.executable_sha256 == receipt.executable_sha256
            && existing.argv == receipt.argv
            && existing.started_unix_secs == receipt.started_unix_secs
            && existing.outcome == receipt.outcome
            && existing.exit_code == receipt.exit_code
            && existing.signal == receipt.signal
            && existing.log == receipt.log
    })
}

fn state_independent_audit_sha(args: &Args, attempt: &AttemptState) -> Result<String> {
    let state: BatchState = read_bounded_json(&args.state_root.join("state.json"))?;
    ensure!(
        state.pinned_audit_binary.path == attempt.executable
            && state.pinned_audit_binary.device == attempt.executable_device
            && state.pinned_audit_binary.inode == attempt.executable_inode,
        "attempt receipt executable differs from durable pinned auditor"
    );
    Ok(state.pinned_audit_binary.sha256)
}

fn append_event_and_snapshot(
    args: &Args,
    state: &mut BatchState,
    manifest_sha256: &str,
    task_id: Option<&str>,
    action: &str,
    message: &str,
) -> Result<()> {
    ensure!(
        state.manifest_sha256 == manifest_sha256,
        "event manifest identity differs from state"
    );
    let sequence = state.next_event_seq;
    ensure!(sequence > 0, "event sequence is invalid");
    let event = DurableEvent {
        schema_version: EVENT_SCHEMA_VERSION,
        kind: "firewatch_wire_profile_audit_batch_event".into(),
        manifest_sha256: manifest_sha256.into(),
        sequence,
        unix_secs: unix_now(),
        task_id: task_id.map(str::to_owned),
        action: action.into(),
        message: bounded_message(message),
    };
    let path = args
        .state_root
        .join("events")
        .join(format!("{sequence:020}.json"));
    publish_json_no_replace(&path, &event)?;
    state.next_event_seq = sequence.checked_add(1).context("event sequence overflow")?;
    state.updated_unix_secs = unix_now();
    publish_json_atomic(&args.state_root.join("state.json"), state)
}

fn reconcile_event_sequence(state_root: &Path, state: &mut BatchState) -> Result<()> {
    let mut sequences = BTreeSet::new();
    for path in read_durable_json_entries(&state_root.join("events"))? {
        let name = path.file_name().context("event has no filename")?;
        let name = name.to_str().context("event filename is not UTF-8")?;
        let Some(number) = name.strip_suffix(".json") else {
            bail!("unexpected file in event directory: {name}");
        };
        ensure!(
            number.len() == 20 && number.bytes().all(|byte| byte.is_ascii_digit()),
            "event filename is malformed: {name}"
        );
        let sequence = number.parse::<u64>()?;
        let event: DurableEvent = read_bounded_json(&path)?;
        ensure!(
            event.schema_version == EVENT_SCHEMA_VERSION
                && event.kind == "firewatch_wire_profile_audit_batch_event"
                && event.manifest_sha256 == state.manifest_sha256
                && event.sequence == sequence
                && event.unix_secs > 0,
            "durable event identity is invalid"
        );
        ensure!(sequences.insert(sequence), "duplicate event sequence");
    }
    let max = sequences.iter().next_back().copied().unwrap_or(0);
    ensure!(
        sequences.iter().copied().eq(1..=max),
        "durable event sequence contains a gap"
    );
    let durable_next = max.checked_add(1).context("event sequence overflow")?;
    if state.next_event_seq < durable_next {
        state.next_event_seq = durable_next;
    } else {
        ensure!(
            state.next_event_seq == durable_next,
            "state event sequence is ahead of durable events"
        );
    }
    Ok(())
}

fn bounded_message(message: &str) -> String {
    message
        .chars()
        .filter(|character| !character.is_control())
        .take(1_024)
        .collect()
}

fn wait_for_process_absence(pid: u32, start_ticks: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while process_start_ticks(pid) == Some(start_ticks) {
        ensure!(
            Instant::now() < deadline,
            "terminated audit process did not exit within 10 seconds"
        );
        thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn wait_for_process_absence_without_signal(pid: u32, start_ticks: u64) {
    while process_start_ticks(pid) == Some(start_ticks) {
        thread::sleep(Duration::from_millis(100));
    }
}

fn wait_active_attempt_without_signal(process: &mut ActiveAttempt) {
    if let Some(child) = process.child.as_mut() {
        // A direct Child handle is the reap authority. On an unusual wait error,
        // keep the supervisor alive and retry; returning could activate
        // PDEATHSIG without a complete pool intent.
        loop {
            match child.wait() {
                Ok(_) => return,
                Err(_) if process_start_ticks(process.pid) != Some(process.start_ticks) => return,
                Err(_) => thread::sleep(Duration::from_millis(100)),
            }
        }
    } else {
        wait_for_process_absence_without_signal(process.pid, process.start_ticks);
    }
}

fn process_start_ticks(pid: u32) -> Option<u64> {
    process_stat_fields(pid)?.get(19)?.parse().ok()
}

fn process_group_id(pid: u32) -> Option<u32> {
    process_stat_fields(pid)?.get(2)?.parse().ok()
}

fn process_stat_fields(pid: u32) -> Option<Vec<String>> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.rfind(')')?;
    Some(
        stat.get(close + 2..)?
            .split_whitespace()
            .map(str::to_owned)
            .collect(),
    )
}

fn install_shutdown_handlers() -> Result<()> {
    extern "C" fn handle(_: libc::c_int) {
        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
    }
    // SAFETY: the handler only performs one async-signal-safe atomic store.
    unsafe {
        ensure!(
            libc::signal(libc::SIGTERM, handle as *const () as libc::sighandler_t,)
                != libc::SIG_ERR,
            "install SIGTERM handler failed"
        );
        ensure!(
            libc::signal(libc::SIGINT, handle as *const () as libc::sighandler_t,) != libc::SIG_ERR,
            "install SIGINT handler failed"
        );
    }
    Ok(())
}

fn interruptible_sleep(duration: Duration) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline && !SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
        thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(250)),
        );
    }
}

fn new_attempt_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(hex_digest(bytes))
}

fn is_attempt_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn read_bounded_pinned_bytes(path: &Path, limit: u64) -> Result<(Vec<u8>, String)> {
    ensure!(limit < u64::MAX, "bounded input limit is invalid");
    let before = fs::symlink_metadata(path)?;
    ensure!(
        before.file_type().is_file() && !before.file_type().is_symlink() && before.len() <= limit,
        "bounded input is not a safe regular file"
    );
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let opened = file.metadata()?;
    ensure!(
        same_file(&before, &opened) && same_version(&before, &opened),
        "bounded input changed before open"
    );
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "bounded input is too large");
    let after = file.metadata()?;
    let path_after = fs::symlink_metadata(path)?;
    ensure!(
        after.len() == bytes.len() as u64
            && same_file(&opened, &after)
            && same_version(&opened, &after)
            && same_file(&after, &path_after)
            && same_version(&after, &path_after),
        "bounded input changed while reading"
    );
    Ok((bytes.clone(), hex_digest(Sha256::digest(&bytes))))
}

fn read_bounded_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let (bytes, _) = read_bounded_pinned_bytes(path, MAX_CONTROL_BYTES)?;
    serde_json::from_slice(&bytes).with_context(|| format!("decode JSON {}", path.display()))
}

fn read_optional_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::symlink_metadata(path) {
        Ok(_) => read_bounded_json(path).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn publish_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("JSON output has no parent")?;
    validate_real_canonical_directory(parent, "JSON parent")?;
    let bytes = serde_json::to_vec_pretty(value)?;
    ensure!(
        bytes.len() as u64 <= MAX_CONTROL_BYTES,
        "JSON output is too large"
    );
    let temp = unique_temp_path(path)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn publish_json_no_replace(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("JSON output has no parent")?;
    validate_real_canonical_directory(parent, "JSON parent")?;
    ensure!(
        fs::symlink_metadata(path).is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
        "JSON output already exists: {}",
        path.display()
    );
    let bytes = serde_json::to_vec_pretty(value)?;
    ensure!(
        bytes.len() as u64 <= MAX_CONTROL_BYTES,
        "JSON output is too large"
    );
    let temp = unique_temp_path(path)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::hard_link(&temp, path)?;
    File::open(parent)?.sync_all()?;
    fs::remove_file(&temp)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn publish_json_no_replace_or_exact<T: DeserializeOwned + Serialize>(
    path: &Path,
    value: &T,
    equal: impl FnOnce(&T) -> bool,
) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
                "existing durable receipt is not a real file"
            );
            let existing: T = read_bounded_json(path)?;
            ensure!(equal(&existing), "existing durable receipt differs");
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            publish_json_no_replace(path, value)
        }
        Err(error) => Err(error.into()),
    }
}

fn unique_temp_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().context("output has no parent")?;
    let name = path
        .file_name()
        .context("output has no name")?
        .to_string_lossy();
    let pid = std::process::id();
    let start_ticks = process_start_ticks(pid).context("supervisor has no process start ticks")?;
    Ok(parent.join(format!(
        ".{name}.tmp-{pid}-{start_ticks}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )))
}

fn sha256_file_pinned(path: &Path) -> Result<String> {
    let before = fs::symlink_metadata(path)?;
    ensure!(
        before.file_type().is_file() && !before.file_type().is_symlink(),
        "hashed input is not a real file"
    );
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let opened = file.metadata()?;
    ensure!(
        same_file(&before, &opened) && same_version(&before, &opened),
        "hashed input changed before open"
    );
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata()?;
    let path_after = fs::symlink_metadata(path)?;
    ensure!(
        same_file(&opened, &after)
            && same_version(&opened, &after)
            && same_file(&after, &path_after)
            && same_version(&after, &path_after),
        "hashed input changed while reading"
    );
    Ok(hex_digest(hasher.finalize()))
}

fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.file_type().is_file()
        && right.file_type().is_file()
}

fn same_version(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes.as_ref() {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("write to String");
    }
    output
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_binding(bytes: &[u8]) -> RegistryFileBinding {
        RegistryFileBinding {
            bytes: bytes.len() as u64,
            sha256: hex_digest(Sha256::digest(bytes)),
        }
    }

    fn valid_receipt_source_evidence() -> String {
        encode_receipt_source_recovery_evidence_v3(1, 2, 0, 0, 2, 0).unwrap()
    }

    fn receipt_source_task(root: &Path, epoch: u64) -> ManifestTask {
        receipt_source_task_with_profile_bound_v3(root, epoch, false)
    }

    fn receipt_source_task_with_profile_bound_v3(
        root: &Path,
        epoch: u64,
        profile_bound_v3: bool,
    ) -> ManifestTask {
        let source = root.join(format!("source-{epoch}"));
        let target = root.join(format!("target-{epoch}"));
        let attestation_root = root.join("attest");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&target).unwrap();
        fs::create_dir_all(&attestation_root).unwrap();
        let source_objects = [
            (ARCHIVE_V2_BLOCKS_FILE, b"blocks".as_slice()),
            (ARCHIVE_V2_POH_FILE, b"poh".as_slice()),
            (ARCHIVE_V2_PUBKEY_REGISTRY_FILE, &[0x33; 32][..]),
            (
                ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
                b"registry-index".as_slice(),
            ),
        ];
        let mut source_bindings = BTreeMap::new();
        for (name, bytes) in source_objects {
            fs::write(source.join(name), bytes).unwrap();
            source_bindings.insert(name.to_owned(), file_binding(bytes));
        }
        let mut target_bindings =
            BTreeMap::from([(ARCHIVE_V2_BLOCKS_FILE.to_owned(), file_binding(b"target"))]);
        let post = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        if profile_bound_v3 {
            let marker = wire_profile_marker(post);
            fs::write(
                target.join(&marker.name),
                blockzilla_read_sdk_legacy::wire_profile_marker_bytes(post),
            )
            .unwrap();
            target_bindings.insert(
                marker.name,
                file_binding(blockzilla_read_sdk_legacy::wire_profile_marker_bytes(post)),
            );
        }
        let source_generation = registry_generation_digest(&source_bindings);
        let receipt = RegistryReceipt {
            version: if profile_bound_v3 { 3 } else { 2 },
            algorithm: if profile_bound_v3 {
                "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3".into()
            } else {
                "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v2".into()
            },
            epoch,
            source_dir: source.to_string_lossy().into_owned(),
            target_dir: target.to_string_lossy().into_owned(),
            source_generation_sha256: source_generation.clone(),
            target_generation_sha256: registry_generation_digest(&target_bindings),
            source_files: source_bindings,
            target_files: target_bindings,
            wire_profile: profile_bound_v3.then_some(post),
        };
        let receipt_path = target.join(REGISTRY_RECEIPT_FILE);
        fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();
        let mut task = ManifestTask {
            task_id: format!("receipt-source-{epoch}"),
            epoch,
            archive: source,
            registry_order: "first_seen".into(),
            generation_kind: RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND.into(),
            registry_receipt: Some(receipt_path),
            registry_receipt_source_binding: None,
            profile_authority: RECEIPT_SOURCE_PROFILE_AUTHORITY.into(),
            content_generation_sha256: source_generation.clone(),
            profile_attempts: [
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            ],
            expected_attestation: attestation_root
                .join(format!("epoch-{epoch}-{source_generation}.json")),
            archive_blocks_bytes: 6,
        };
        let capture = capture_receipt_source_task(&task).unwrap();
        task.registry_receipt_source_binding = Some(ManifestReceiptSourceBinding {
            receipt_sha256: capture.receipt_sha256,
            receipt_identity: capture.receipt_identity,
            source_files: capture.source_files,
        });
        task
    }

    fn publish_receipt_source_attestation(
        task: &ManifestTask,
        profile: ArchiveV2WireProfile,
        evidence: String,
    ) {
        let capture = capture_receipt_source_task(task).unwrap();
        let attestation = WireProfileAttestation {
            schema_version: 2,
            kind: "archive_v2_wire_profile_attestation".into(),
            audit_algorithm: "archive-v2-borrowed-dual-profile-full-generation-v2".into(),
            audited_profiles: [
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            ],
            cluster_id: "mainnet-beta".into(),
            epoch: task.epoch,
            archive: task.archive.clone(),
            registry_order: "first_seen".into(),
            generation_kind: RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND.into(),
            content_generation_sha256: task.content_generation_sha256.clone(),
            archive_files: capture.source_files,
            wire_profile: profile,
            evidence,
            attested_unix_secs: 1,
        };
        let mut output = OpenOptions::new();
        output.create_new(true).write(true).mode(0o600);
        output
            .open(&task.expected_attestation)
            .unwrap()
            .write_all(&serde_json::to_vec_pretty(&attestation).unwrap())
            .unwrap();
    }

    fn profile_task(id: &str, epoch: u64) -> ManifestTask {
        ManifestTask {
            task_id: id.into(),
            epoch,
            archive: PathBuf::from(format!("/archive/epoch-{epoch}")),
            registry_order: "usage_sorted".into(),
            generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
            registry_receipt: None,
            registry_receipt_source_binding: None,
            profile_authority: "marker_free_dual_audit".into(),
            content_generation_sha256: "a".repeat(64),
            profile_attempts: [
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            ],
            expected_attestation: PathBuf::from(format!(
                "/attest/epoch-{epoch}-{}.json",
                "a".repeat(64)
            )),
            archive_blocks_bytes: 1,
        }
    }

    #[test]
    fn receipt_source_task_is_post_first_and_pins_receipt_and_source_identities() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let task = receipt_source_task(&root, 700);
        validate_manifest_task(&task, &root.join("attest")).unwrap();
        let argv = audit_argv(&task, task.profile_attempts[0], &root.join("attest"));
        assert_eq!(
            task.profile_attempts[0],
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        );
        let receipt_index = argv
            .iter()
            .position(|item| item == "--registry-receipt")
            .unwrap();
        assert_eq!(
            argv[receipt_index + 1],
            task.registry_receipt.as_ref().unwrap().to_string_lossy()
        );

        let mut pre_first = task.clone();
        pre_first.profile_attempts.swap(0, 1);
        assert!(validate_manifest_task(&pre_first, &root.join("attest")).is_err());
    }

    #[test]
    fn receipt_source_task_rejects_receipt_path_security_and_source_path_mismatch() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let task = receipt_source_task(&root, 701);
        let receipt_path = task.registry_receipt.as_ref().unwrap();

        fs::set_permissions(receipt_path, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(validate_manifest_task(&task, &root.join("attest")).is_err());
        fs::set_permissions(receipt_path, fs::Permissions::from_mode(0o644)).unwrap();

        let hardlink = receipt_path.with_file_name("receipt-hardlink.json");
        fs::hard_link(receipt_path, &hardlink).unwrap();
        assert!(validate_manifest_task(&task, &root.join("attest")).is_err());
        fs::remove_file(hardlink).unwrap();

        let real = receipt_path.with_file_name("receipt-real.json");
        fs::rename(receipt_path, &real).unwrap();
        symlink(&real, receipt_path).unwrap();
        assert!(validate_manifest_task(&task, &root.join("attest")).is_err());
        fs::remove_file(receipt_path).unwrap();
        fs::rename(&real, receipt_path).unwrap();

        let same_content_source = root.join("same-content-source");
        fs::create_dir(&same_content_source).unwrap();
        let mut receipt: RegistryReceipt = read_bounded_json(receipt_path).unwrap();
        for name in receipt.source_files.keys() {
            fs::copy(task.archive.join(name), same_content_source.join(name)).unwrap();
        }
        receipt.source_dir = same_content_source.to_string_lossy().into_owned();
        fs::write(receipt_path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();
        assert!(validate_manifest_task(&task, &root.join("attest")).is_err());
    }

    #[test]
    fn receipt_source_task_rejects_unprotected_linked_and_symlinked_source_files() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();

        let writable = receipt_source_task(&root, 705);
        let writable_blocks = writable.archive.join(ARCHIVE_V2_BLOCKS_FILE);
        fs::set_permissions(&writable_blocks, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(validate_manifest_task(&writable, &root.join("attest")).is_err());

        let linked = receipt_source_task(&root, 706);
        let linked_blocks = linked.archive.join(ARCHIVE_V2_BLOCKS_FILE);
        fs::hard_link(&linked_blocks, linked.archive.join("blocks-hard-link")).unwrap();
        assert!(validate_manifest_task(&linked, &root.join("attest")).is_err());

        let symlinked = receipt_source_task(&root, 707);
        let symlinked_blocks = symlinked.archive.join(ARCHIVE_V2_BLOCKS_FILE);
        let real_blocks = symlinked.archive.join("blocks-real");
        fs::rename(&symlinked_blocks, &real_blocks).unwrap();
        symlink(&real_blocks, &symlinked_blocks).unwrap();
        assert!(validate_manifest_task(&symlinked, &root.join("attest")).is_err());
    }

    #[test]
    fn receipt_source_final_attestation_rejects_same_size_inode_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let task = receipt_source_task(&root, 702);
        validate_manifest_task(&task, &root.join("attest")).unwrap();
        publish_receipt_source_attestation(
            &task,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            valid_receipt_source_evidence(),
        );
        assert!(validate_exact_attestation(&task).unwrap().is_some());

        let blocks = task.archive.join(ARCHIVE_V2_BLOCKS_FILE);
        let replacement = task.archive.join("replacement-blocks");
        fs::write(&replacement, b"blocks").unwrap();
        fs::rename(&replacement, &blocks).unwrap();
        assert!(validate_exact_attestation(&task).is_err());
    }

    #[test]
    fn receipt_source_attestation_replacement_in_final_use_gap_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let task = receipt_source_task(&root, 708);
        validate_manifest_task(&task, &root.join("attest")).unwrap();
        publish_receipt_source_attestation(
            &task,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            valid_receipt_source_evidence(),
        );
        let exact_bytes = fs::read(&task.expected_attestation).unwrap();

        let result = validate_exact_attestation_with_pre_final(&task, || {
            let replacement = task.expected_attestation.with_extension("replacement");
            fs::write(&replacement, &exact_bytes)?;
            fs::rename(&replacement, &task.expected_attestation)?;
            Ok(())
        });

        assert!(result.is_err());
    }

    #[test]
    fn profile_bound_v3_post_producer_evidence_reaches_the_batch_consumer() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let task = receipt_source_task_with_profile_bound_v3(&root, 704, true);
        validate_manifest_task(&task, &root.join("attest")).unwrap();
        let evidence = valid_receipt_source_evidence();
        validate_receipt_source_recovery_evidence(&evidence).unwrap();
        publish_receipt_source_attestation(
            &task,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            evidence,
        );

        assert!(validate_exact_attestation(&task).unwrap().is_some());
    }

    #[test]
    fn receipt_source_recovery_evidence_rejects_pre_missing_and_ambiguous_results() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let task = receipt_source_task(&root, 703);
        assert!(validate_exact_attestation(&task).unwrap().is_none());

        publish_receipt_source_attestation(
            &task,
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            valid_receipt_source_evidence(),
        );
        assert!(validate_exact_attestation(&task).is_err());

        assert!(
            validate_receipt_source_recovery_evidence(&valid_receipt_source_evidence()).is_ok()
        );
        let ambiguous = valid_receipt_source_evidence()
            .replace(
                "both_semantically_equivalent=2",
                "both_semantically_equivalent=1",
            )
            .replace(
                "both_semantically_divergent=0",
                "both_semantically_divergent=1",
            );
        assert!(validate_receipt_source_recovery_evidence(&ambiguous).is_err());
        let both_pass_divergent = ambiguous.replace(
            "decision_basis=all_semantically_equivalent",
            "decision_basis=unique_full_generation_decode",
        );
        assert!(validate_receipt_source_recovery_evidence(&both_pass_divergent).is_err());
        let raw_fallback = valid_receipt_source_evidence()
            .replace("raw_transaction_fallbacks=0", "raw_transaction_fallbacks=1");
        assert!(validate_receipt_source_recovery_evidence(&raw_fallback).is_err());
    }

    fn limits() -> BatchLimits {
        BatchLimits {
            max_workers: 2,
            poll_secs: 5,
            resume_stable_secs: 60,
            disk_reserve_gib: 512,
            memory_resume_mib: 4096,
            memory_hard_floor_mib: 3072,
            cgroup_memory_high_mib: 3072,
            cgroup_memory_max_mib: 4096,
            memory_psi_resume_max_pct: 1.0,
            io_pause_full_pct: 40.0,
            io_pause_polls: 2,
            io_resume_full_pct: 8.0,
        }
    }

    fn scheduler(memory_mib: u64, io: f64, memory_psi: f64) -> SchedulerStatus {
        SchedulerStatus {
            schema_version: 3,
            sequence: 1,
            now_unix_secs: 1,
            control_reconciled_unix_secs: 1,
            observer_mode: false,
            scheduler: SchedulerControl { paused: false },
            machine: SchedulerMachine {
                memory_available_bytes: memory_mib * MIB,
                disk_available_bytes: 1_024 * GIB,
                io_pressure_full_avg10: Some(io),
                memory_pressure_some_avg10: Some(memory_psi),
            },
            inventory: SchedulerInventory { complete: true },
            summary: SchedulerSummary {
                queued: 0,
                scanning: 0,
                finalizing: 0,
                legacy_compact_active: 0,
                scan_ready: 0,
                poh_migration_epochs_runnable: 0,
                poh_migration_running: 0,
                registry_reprocess_epochs_runnable: 0,
                registry_reprocess_audits_runnable: 0,
                registry_reprocess_running: 0,
            },
            scan_sweep: SchedulerScanSweep {
                complete: true,
                pending: 0,
                active: 0,
            },
            finalizer_queue: Vec::new(),
            lanes: Vec::new(),
            live: Vec::new(),
        }
    }

    fn near_high_cgroup() -> CgroupMemorySnapshot {
        CgroupMemorySnapshot {
            current_bytes: 2_900 * MIB,
            high_bytes: Some(3 * GIB),
            max_bytes: Some(4 * GIB),
            ..cgroup()
        }
    }

    fn cgroup() -> CgroupMemorySnapshot {
        CgroupMemorySnapshot {
            current_bytes: 128 * MIB,
            high_bytes: Some(8 * GIB),
            max_bytes: Some(8 * GIB),
            anon_bytes: 64 * MIB,
            file_bytes: 64 * MIB,
            inactive_file_bytes: 0,
            pressure_some_avg10: Some(0.0),
            pressure_full_avg10: Some(0.0),
            swap_current_bytes: 0,
            events: CgroupMemoryEvents {
                high: 0,
                max: 0,
                oom: 0,
                oom_kill: 0,
            },
        }
    }

    fn state_for(tasks: &[ManifestTask]) -> BatchState {
        BatchState {
            schema_version: 1,
            kind: "firewatch_wire_profile_audit_batch_state".into(),
            manifest_sha256: "b".repeat(64),
            status: "initialized".into(),
            next_event_seq: 1,
            pinned_audit_binary: PinnedExecutable {
                path: "/audit".into(),
                device: 1,
                inode: 2,
                size: 3,
                modified_seconds: 4,
                modified_nanoseconds: 5,
                changed_seconds: 6,
                changed_nanoseconds: 7,
                sha256: "c".repeat(64),
            },
            public_audit_binary: PinnedExecutable {
                path: "/public-audit".into(),
                device: 1,
                inode: 20,
                size: 3,
                modified_seconds: 4,
                modified_nanoseconds: 5,
                changed_seconds: 6,
                changed_nanoseconds: 7,
                sha256: "c".repeat(64),
            },
            pinned_yield_executables: Vec::new(),
            cgroup: CgroupBinding {
                path: "/sys/fs/cgroup/test".into(),
                device: 1,
                inode: 2,
            },
            cgroup_hard_event_baseline: CgroupEventRecord {
                high: 0,
                max: 0,
                oom: 0,
                oom_kill: 0,
            },
            updated_unix_secs: 1,
            tasks: tasks
                .iter()
                .map(|task| {
                    (
                        task.task_id.clone(),
                        TaskState {
                            status: TaskStatus::Pending,
                            profile_index: 0,
                            operational_retries: 0,
                            retry_not_before_unix_secs: None,
                            attempt: None,
                            last_message: None,
                        },
                    )
                })
                .collect(),
        }
    }

    fn manifest(tasks: Vec<ManifestTask>) -> BatchManifest {
        BatchManifest {
            schema_version: 1,
            kind: "firewatch_wire_profile_audit_batch".into(),
            cluster_id: "mainnet-beta".into(),
            batch_instance_id: "e".repeat(32),
            source_status_sha256: "d".repeat(64),
            audit_binary: ManifestExecutable {
                path: "/audit".into(),
                sha256: "c".repeat(64),
            },
            attestation_root: "/attest".into(),
            limits: limits(),
            yield_executables: Vec::new(),
            forbidden_executable_paths: Vec::new(),
            tasks,
        }
    }

    #[test]
    fn manifest_order_beats_lexical_task_id_order() {
        let first = profile_task("z-first", 1);
        let second = profile_task("a-second", 2);
        let manifest = manifest(vec![first.clone(), second.clone()]);
        let state = state_for(&[first, second]);
        assert_eq!(
            next_runnable_task(&manifest, &state).as_deref(),
            Some("z-first")
        );
    }

    #[test]
    fn exact_profile_exit_contract_is_fail_closed() {
        let exit20 = AttemptReceipt {
            schema_version: 1,
            kind: "firewatch_wire_profile_audit_batch_attempt".into(),
            manifest_sha256: "a".repeat(64),
            task_id: "task".into(),
            attempt_id: "b".repeat(32),
            profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            pid: Some(1),
            process_start_ticks: Some(2),
            pgid: Some(1),
            executable_device: 1,
            executable_inode: 2,
            executable_sha256: "c".repeat(64),
            argv: Vec::new(),
            started_unix_secs: 1,
            finished_unix_secs: 2,
            outcome: "selected_profile_decode_rejected".into(),
            exit_code: Some(20),
            signal: None,
            log: "/log".into(),
        };
        assert_eq!(
            disposition_from_receipt(&exit20).unwrap(),
            (ExitDisposition::AdvanceProfile, false)
        );
        let mut wrong = exit20;
        wrong.exit_code = Some(1);
        assert!(disposition_from_receipt(&wrong).is_err());
        wrong.outcome = "operational_failure".into();
        wrong.exit_code = Some(20);
        assert!(disposition_from_receipt(&wrong).is_err());
        wrong.outcome = "terminal_profile_audit_rejected".into();
        wrong.exit_code = Some(21);
        assert_eq!(
            disposition_from_receipt(&wrong).unwrap(),
            (ExitDisposition::TerminalBlocked, false)
        );
    }

    #[test]
    fn generic_signal_and_lost_exits_never_advance() {
        let success = ExitStatus::from_raw(0);
        assert_eq!(
            KnownOrLostExit::Known(success).disposition(),
            ExitDisposition::RetrySameProfile
        );
        let signaled = ExitStatus::from_raw(9);
        assert_eq!(
            KnownOrLostExit::Known(signaled).disposition(),
            ExitDisposition::RetrySameProfile
        );
        assert_eq!(
            KnownOrLostExit::Lost.disposition(),
            ExitDisposition::RetrySameProfile
        );
    }

    #[test]
    fn intentional_interruption_does_not_consume_retry_and_exhaustion_stays_at_three() {
        let task = profile_task("task", 1);
        let mut state = state_for(std::slice::from_ref(&task))
            .tasks
            .remove("task")
            .unwrap();
        state.operational_retries = 2;
        schedule_same_profile_retry(&task, &mut state, "yield", false);
        assert_eq!(state.operational_retries, 2);
        schedule_same_profile_retry(&task, &mut state, "failure", true);
        assert_eq!(state.operational_retries, 3);
        schedule_same_profile_retry(&task, &mut state, "failure", true);
        assert_eq!(state.operational_retries, 3);
        assert_eq!(state.status, TaskStatus::Blocked);
    }

    #[test]
    fn active_hysteresis_allows_ordinary_audit_io_and_memory() {
        let limits = limits();
        let status = scheduler(3500, 19.0, 5.0);
        let mut polls = 0;
        assert_eq!(
            resource_gate(&limits, Ok(&status), None, &cgroup(), &mut polls, true),
            ResourceGate::Safe
        );
        assert!(matches!(
            resource_gate(&limits, Ok(&status), None, &cgroup(), &mut polls, false),
            ResourceGate::Pause(_)
        ));
    }

    #[test]
    fn active_audit_continues_near_memory_high_but_new_worker_is_rejected() {
        let limits = limits();
        let status = scheduler(5_000, 1.0, 0.1);
        let cgroup = near_high_cgroup();
        let mut polls = 0;
        assert_eq!(
            resource_gate(&limits, Ok(&status), None, &cgroup, &mut polls, true),
            ResourceGate::Safe
        );
        assert!(!admission_safe(&limits, &status, &cgroup, 1));
        assert!(resume_safe(&limits, &status));
    }

    #[test]
    fn inactive_file_cache_does_not_block_safe_cgroup_admission() {
        let mut cache_heavy = near_high_cgroup();
        cache_heavy.inactive_file_bytes = 2 * GIB;
        cache_heavy.swap_current_bytes = 16 * MIB;
        assert_eq!(cgroup_working_usage_bytes(&cache_heavy), 868 * MIB);
        assert!(cgroup_has_headroom(&cache_heavy, 2));

        let mut anon_heavy = cache_heavy;
        anon_heavy.inactive_file_bytes = 0;
        assert!(!cgroup_has_headroom(&anon_heavy, 1));
    }

    #[test]
    fn stopped_after_error_state_is_restartable() {
        assert!(valid_batch_state_status("stopped_after_error"));
        assert!(!valid_batch_state_status("unknown"));
    }

    #[test]
    fn directly_owned_child_is_killed_and_reaped_promptly() {
        let mut child = Command::new("/bin/sleep").arg("30").spawn().unwrap();
        let started = Instant::now();
        let status = kill_and_reap_direct_child(&mut child).unwrap();
        assert!(!status.success());
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(child.try_wait().unwrap().is_some());
    }

    #[test]
    fn interruption_receipt_can_monotonically_bind_a_starting_attempt() {
        let attempt = AttemptState {
            attempt_id: "a".repeat(32),
            profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            executable: "/audit".into(),
            executable_device: 1,
            executable_inode: 2,
            argv: vec!["--epoch".into(), "1".into()],
            pid: None,
            process_start_ticks: None,
            pgid: None,
            log: "/log".into(),
            started_unix_secs: 1,
            paused: false,
            pause_reason: None,
        };
        let receipt = AttemptReceipt {
            schema_version: ATTEMPT_RECEIPT_SCHEMA_VERSION,
            kind: "firewatch_wire_profile_audit_batch_attempt".into(),
            manifest_sha256: "b".repeat(64),
            task_id: "task".into(),
            attempt_id: attempt.attempt_id.clone(),
            profile: attempt.profile,
            pid: Some(42),
            process_start_ticks: Some(99),
            pgid: Some(42),
            executable_device: attempt.executable_device,
            executable_inode: attempt.executable_inode,
            executable_sha256: "c".repeat(64),
            argv: attempt.argv.clone(),
            started_unix_secs: attempt.started_unix_secs,
            finished_unix_secs: 2,
            outcome: "supervisor_interruption_intent".into(),
            exit_code: None,
            signal: None,
            log: attempt.log.clone(),
        };
        validate_attempt_receipt(&receipt, &"b".repeat(64), "task", &attempt).unwrap();
        let mut wrong = receipt;
        wrong.outcome = "operational_failure".into();
        assert!(validate_attempt_receipt(&wrong, &"b".repeat(64), "task", &attempt).is_err());
    }

    #[test]
    fn cgroup_limits_are_bound_to_the_manifest() {
        let limits = limits();
        let exact = CgroupMemorySnapshot {
            high_bytes: Some(3 * GIB),
            max_bytes: Some(4 * GIB),
            ..cgroup()
        };
        validate_cgroup_limits(&exact, &limits).unwrap();
        let too_large = CgroupMemorySnapshot {
            high_bytes: Some(30 * GIB),
            max_bytes: Some(40 * GIB),
            ..exact
        };
        assert!(validate_cgroup_limits(&too_large, &limits).is_err());
        let mut typo = limits.clone();
        typo.disk_reserve_gib = 511;
        assert!(validate_limits(&typo).is_err());
    }

    #[test]
    fn one_strict_sample_does_not_satisfy_the_admission_window() {
        let limits = limits();
        let mut since = None;
        let relaxed = scheduler(5_000, 19.0, 0.1);
        if admission_safe(&limits, &relaxed, &cgroup(), 1) {
            since.get_or_insert_with(Instant::now);
        } else {
            since = None;
        }
        assert!(since.is_none());
        let strict = scheduler(5_000, 8.0, 0.1);
        if admission_safe(&limits, &strict, &cgroup(), 1) {
            since.get_or_insert_with(Instant::now);
        }
        assert!(since.is_some_and(|start| {
            start.elapsed() < Duration::from_secs(limits.resume_stable_secs)
        }));
    }

    #[test]
    fn live_finalizer_queue_has_priority_over_batch_audits() {
        let mut status = scheduler(5_000, 1.0, 0.1);
        status.finalizer_queue.push(SchedulerFinalizerQueueItem {
            state: "ReadyToPackage".into(),
            deferred_reason: None,
        });
        assert!(scheduler_has_priority_work(&status));
        status.finalizer_queue[0].deferred_reason = Some("memory guard".into());
        assert!(scheduler_has_priority_work(&status));
    }

    #[test]
    fn schema_three_scheduler_requires_finalizer_live_and_complete_sweep_truth() {
        let value = serde_json::json!({
            "schema_version": 3,
            "sequence": 1,
            "now_unix_secs": 1,
            "control_reconciled_unix_secs": 1,
            "observer_mode": false,
            "scheduler": {"paused": false},
            "machine": {
                "memory_available_bytes": 5 * GIB,
                "disk_available_bytes": 10 * GIB,
                "io_pressure_full_avg10": 0.0,
                "memory_pressure_some_avg10": 0.0
            },
            "inventory": {"complete": true},
            "summary": {
                "queued": 0,
                "scanning": 0,
                "finalizing": 0,
                "legacy_compact_active": 0,
                "scan_ready": 0,
                "poh_migration_epochs_runnable": 0,
                "poh_migration_running": 0,
                "registry_reprocess_epochs_runnable": 0,
                "registry_reprocess_audits_runnable": 0,
                "registry_reprocess_running": 0
            },
            "scan_sweep": {"complete": true, "pending": 0, "active": 0},
            "finalizer_queue": [],
            "lanes": [],
            "live": []
        });
        let status: SchedulerStatus = serde_json::from_value(value.clone()).unwrap();
        assert!(!scheduler_has_priority_work(&status));
        let mut missing = value;
        missing.as_object_mut().unwrap().remove("finalizer_queue");
        assert!(serde_json::from_value::<SchedulerStatus>(missing).is_err());
        let mut status = status;
        status.live.push(SchedulerLiveItem {
            state: "capturing".into(),
        });
        assert!(scheduler_has_priority_work(&status));
    }

    #[test]
    fn replayed_exit_twenty_advances_once_and_interruption_keeps_retry_count() {
        let task = profile_task("task", 1);
        let mut task_state = state_for(std::slice::from_ref(&task))
            .tasks
            .remove("task")
            .unwrap();
        task_state.attempt = Some(AttemptState {
            attempt_id: "a".repeat(32),
            profile: task.profile_attempts[0],
            executable: "/audit".into(),
            executable_device: 1,
            executable_inode: 2,
            argv: Vec::new(),
            pid: None,
            process_start_ticks: None,
            pgid: None,
            log: "/log".into(),
            started_unix_secs: 1,
            paused: false,
            pause_reason: None,
        });
        apply_replayed_disposition(
            &task,
            &mut task_state,
            ExitDisposition::AdvanceProfile,
            "exit20",
            false,
        );
        assert_eq!(task_state.profile_index, 1);
        assert!(task_state.attempt.is_none());
        task_state.operational_retries = 2;
        apply_replayed_disposition(
            &task,
            &mut task_state,
            ExitDisposition::RetrySameProfile,
            "supervisor interruption",
            false,
        );
        assert_eq!(task_state.profile_index, 1);
        assert_eq!(task_state.operational_retries, 2);
    }

    #[test]
    fn two_interruption_intents_replay_without_consuming_either_retry() {
        let tasks = [profile_task("task-a", 1), profile_task("task-b", 2)];
        let mut state = state_for(&tasks);
        for (index, task) in tasks.iter().enumerate() {
            let task_state = state.tasks.get_mut(&task.task_id).unwrap();
            task_state.operational_retries = 2;
            task_state.attempt = Some(AttemptState {
                attempt_id: format!("{:032x}", index + 1),
                profile: task.profile_attempts[0],
                executable: "/audit".into(),
                executable_device: 1,
                executable_inode: 2,
                argv: Vec::new(),
                pid: Some(100 + index as u32),
                process_start_ticks: Some(200 + index as u64),
                pgid: Some(100 + index as u32),
                log: PathBuf::from(format!("/log-{index}")),
                started_unix_secs: 1,
                paused: false,
                pause_reason: None,
            });
            let receipt = AttemptReceipt {
                schema_version: 1,
                kind: "firewatch_wire_profile_audit_batch_attempt".into(),
                manifest_sha256: state.manifest_sha256.clone(),
                task_id: task.task_id.clone(),
                attempt_id: format!("{:032x}", index + 1),
                profile: task.profile_attempts[0],
                pid: Some(100 + index as u32),
                process_start_ticks: Some(200 + index as u64),
                pgid: Some(100 + index as u32),
                executable_device: 1,
                executable_inode: 2,
                executable_sha256: "c".repeat(64),
                argv: Vec::new(),
                started_unix_secs: 1,
                finished_unix_secs: 2,
                outcome: "supervisor_interruption_intent".into(),
                exit_code: None,
                signal: None,
                log: PathBuf::from(format!("/log-{index}")),
            };
            let (disposition, consume) = disposition_from_receipt(&receipt).unwrap();
            apply_replayed_disposition(task, task_state, disposition, "restart", consume);
        }
        assert!(state.tasks.values().all(|task| {
            task.profile_index == 0
                && task.operational_retries == 2
                && task.status == TaskStatus::RetryWait
                && task.attempt.is_none()
        }));
    }

    #[test]
    fn terminal_state_without_receipt_is_rejected() {
        let root = std::env::temp_dir().join(format!(
            "firewatch-batch-test-{}-{}",
            std::process::id(),
            unix_now()
        ));
        create_private_directory(&root).unwrap();
        create_private_directory(&root.join("terminal")).unwrap();
        let task = profile_task("task", 1);
        let mut state = state_for(std::slice::from_ref(&task));
        state.tasks.get_mut("task").unwrap().status = TaskStatus::Blocked;
        let args = Args {
            manifest: root.join("manifest.json"),
            manifest_sha256: state.manifest_sha256.clone(),
            state_root: root.clone(),
            controller_state_root: root.clone(),
            scheduler_status_url: "http://127.0.0.1:8786/api/v1/status".into(),
            execute: true,
        };
        let tasks = BTreeMap::from([(task.task_id.clone(), &task)]);
        assert!(
            validate_terminal_state_receipts(&args, &state.manifest_sha256, &tasks, &state)
                .is_err()
        );
        fs::remove_dir(root.join("terminal")).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn complete_receipt_without_current_attestation_is_rejected() {
        let root = std::env::temp_dir().join(format!(
            "firewatch-complete-test-{}-{}",
            std::process::id(),
            unix_now()
        ));
        create_private_directory(&root).unwrap();
        create_private_directory(&root.join("terminal")).unwrap();
        let task = profile_task("task", 1);
        let mut state = state_for(std::slice::from_ref(&task));
        state.tasks.get_mut("task").unwrap().status = TaskStatus::Complete;
        let receipt = TerminalReceipt {
            schema_version: TERMINAL_RECEIPT_SCHEMA_VERSION,
            kind: "firewatch_wire_profile_audit_batch_terminal".into(),
            manifest_sha256: state.manifest_sha256.clone(),
            task_id: task.task_id.clone(),
            epoch: task.epoch,
            status: TaskStatus::Complete,
            profile: task.profile_attempts[0],
            expected_attestation: task.expected_attestation.clone(),
            attestation_sha256: Some("d".repeat(64)),
            unix_secs: 1,
            message: "complete".into(),
        };
        fs::write(
            root.join("terminal/task.json"),
            serde_json::to_vec(&receipt).unwrap(),
        )
        .unwrap();
        let args = Args {
            manifest: root.join("manifest.json"),
            manifest_sha256: state.manifest_sha256.clone(),
            state_root: root.clone(),
            controller_state_root: root.clone(),
            scheduler_status_url: "http://127.0.0.1:8786/api/v1/status".into(),
            execute: true,
        };
        let tasks = BTreeMap::from([(task.task_id.clone(), &task)]);
        assert!(
            validate_terminal_state_receipts(&args, &state.manifest_sha256, &tasks, &state)
                .is_err()
        );
        fs::remove_file(root.join("terminal/task.json")).unwrap();
        fs::remove_dir(root.join("terminal")).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn private_directory_permissions_are_enforced() {
        let root = std::env::temp_dir().join(format!(
            "firewatch-permission-test-{}-{}",
            std::process::id(),
            unix_now()
        ));
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o755).create(&root).unwrap();
        let root = fs::canonicalize(root).unwrap();
        assert!(validate_private_directory(&root, "test root").is_err());
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        validate_private_directory(&root, "test root").unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn cleanup_rejects_unrelated_dot_temp_and_accepts_exact_internal_temp() {
        let root = std::env::temp_dir().join(format!(
            "firewatch-temp-test-{}-{}",
            std::process::id(),
            unix_now()
        ));
        create_private_directory(&root).unwrap();
        let bad = root.join(format!(".valuable.tmp-{}-0-1", u32::MAX));
        File::create(&bad).unwrap();
        assert!(cleanup_internal_temps_in_directory(&root).is_err());
        fs::remove_file(&bad).unwrap();
        let good = root.join(format!(".state.json.tmp-{}-0-1", u32::MAX));
        File::create(&good).unwrap();
        cleanup_internal_temps_in_directory(&root).unwrap();
        assert!(!good.exists());
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn hard_event_increment_and_reset_are_both_detected() {
        let before = CgroupEventRecord {
            high: 1,
            max: 2,
            oom: 3,
            oom_kill: 4,
        };
        assert!(!cgroup_hard_event_changed(
            before,
            CgroupEventRecord { high: 2, ..before }
        ));
        assert!(cgroup_hard_event_changed(
            before,
            CgroupEventRecord { max: 3, ..before }
        ));
        assert!(cgroup_hard_event_changed(
            before,
            CgroupEventRecord { max: 1, ..before }
        ));
        assert!(!cgroup_hard_event_changed(before, before));
    }

    #[test]
    fn child_command_clears_hostile_loader_environment() {
        let mut command = Command::new("/bin/echo");
        command
            .env_clear()
            .env("BLOCKZILLA_FIREWATCH_AUDIT_BATCH_ATTEMPT_ID", "a");
        let environment = command.get_envs().collect::<Vec<_>>();
        assert_eq!(environment.len(), 1);
        assert!(
            environment
                .iter()
                .all(|(key, _)| { *key != "LD_PRELOAD" && *key != "LD_LIBRARY_PATH" })
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn public_and_private_auditors_yield_but_exact_owned_private_child_is_allowed() {
        let root = PathBuf::from(format!(
            "/tmp/firewatch-guard-test-{}-{}",
            std::process::id(),
            unix_now()
        ));
        create_private_directory(&root).unwrap();
        let controller = root.join("controller");
        create_private_directory(&controller).unwrap();
        create_private_directory(&controller.join("wire-profile-attestations")).unwrap();
        let batches = controller.join("wire-profile-audit-batches");
        create_private_directory(&batches).unwrap();
        let state_root = batches.join("b".repeat(64));
        create_private_directory(&state_root).unwrap();
        create_private_directory(&state_root.join("pinned")).unwrap();
        let source = root.join("public-audit");
        fs::copy("/bin/sleep", &source).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o500)).unwrap();
        let executable = ManifestExecutable {
            path: source.clone(),
            sha256: sha256_file_pinned(&source).unwrap(),
        };
        let (private, public) = pin_audit_binary(&state_root, &executable).unwrap();
        assert_ne!(
            (private.device, private.inode),
            (public.device, public.inode)
        );
        let task = profile_task("task", 1);
        let manifest = BatchManifest {
            audit_binary: executable,
            attestation_root: controller.join("wire-profile-attestations"),
            tasks: vec![task.clone()],
            ..manifest(Vec::new())
        };
        let cgroup_path = resolve_self_cgroup_v2().unwrap();
        let cgroup_metadata = fs::symlink_metadata(&cgroup_path).unwrap();
        let mut state = initialize_state(
            &manifest,
            &"b".repeat(64),
            private.clone(),
            public,
            Vec::new(),
            CgroupBinding {
                path: cgroup_path,
                device: cgroup_metadata.dev(),
                inode: cgroup_metadata.ino(),
            },
            CgroupEventRecord {
                high: 0,
                max: 0,
                oom: 0,
                oom_kill: 0,
            },
        );

        let mut public_child = Command::new(&source).arg("5").spawn().unwrap();
        assert!(
            verify_external_work_guard(&manifest, &state, &BTreeMap::new())
                .unwrap()
                .is_some()
        );
        public_child.kill().unwrap();
        public_child.wait().unwrap();

        let attempt_id = "a".repeat(32);
        let mut private_child = Command::new(&private.path)
            .arg("5")
            .env_clear()
            .env("BLOCKZILLA_FIREWATCH_AUDIT_BATCH_ATTEMPT_ID", &attempt_id)
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = private_child.id();
        let deadline = Instant::now() + Duration::from_secs(2);
        let ticks = loop {
            if let Some(ticks) = process_start_ticks(pid) {
                break ticks;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(10));
        };
        assert!(
            verify_external_work_guard(&manifest, &state, &BTreeMap::new())
                .unwrap()
                .is_some()
        );
        state.tasks.get_mut("task").unwrap().status = TaskStatus::Running;
        state.tasks.get_mut("task").unwrap().attempt = Some(AttemptState {
            attempt_id,
            profile: task.profile_attempts[0],
            executable: private.path.clone(),
            executable_device: private.device,
            executable_inode: private.inode,
            argv: vec!["5".into()],
            pid: Some(pid),
            process_start_ticks: Some(ticks),
            pgid: Some(pid),
            log: root.join("owned.log"),
            started_unix_secs: unix_now(),
            paused: false,
            pause_reason: None,
        });
        assert!(
            verify_external_work_guard(
                &manifest,
                &state,
                &BTreeMap::from([("task".into(), (pid, ticks))]),
            )
            .unwrap()
            .is_none()
        );
        private_child.kill().unwrap();
        private_child.wait().unwrap();

        fs::remove_file(&private.path).unwrap();
        fs::remove_file(source).unwrap();
        fs::remove_dir(state_root.join("pinned")).unwrap();
        fs::remove_dir(state_root).unwrap();
        fs::remove_dir(batches).unwrap();
        fs::remove_dir(controller.join("wire-profile-attestations")).unwrap();
        fs::remove_dir(controller).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parent_death_signal_kills_the_audit_process_group_leader() {
        unsafe {
            let mut report = [0; 2];
            assert_eq!(libc::pipe(report.as_mut_ptr()), 0);
            let middle = libc::fork();
            assert!(middle >= 0);
            if middle == 0 {
                libc::close(report[0]);
                let mut ready = [0; 2];
                if libc::pipe(ready.as_mut_ptr()) != 0 {
                    libc::_exit(101);
                }
                let child = libc::fork();
                if child < 0 {
                    libc::_exit(102);
                }
                if child == 0 {
                    libc::close(ready[0]);
                    let parent = libc::getppid();
                    if configure_audit_child_lifetime(parent).is_err() {
                        libc::_exit(103);
                    }
                    let identity = [libc::getpid(), libc::getpgrp()];
                    if libc::write(
                        report[1],
                        identity.as_ptr().cast(),
                        std::mem::size_of_val(&identity),
                    ) != std::mem::size_of_val(&identity) as isize
                    {
                        libc::_exit(104);
                    }
                    let byte = [1u8];
                    if libc::write(ready[1], byte.as_ptr().cast(), 1) != 1 {
                        libc::_exit(105);
                    }
                    loop {
                        libc::pause();
                    }
                }
                libc::close(ready[1]);
                let mut byte = [0u8];
                if libc::read(ready[0], byte.as_mut_ptr().cast(), 1) != 1 {
                    libc::_exit(106);
                }
                libc::_exit(0);
            }
            libc::close(report[1]);
            let mut identity = [0 as libc::pid_t; 2];
            assert_eq!(
                libc::read(
                    report[0],
                    identity.as_mut_ptr().cast(),
                    std::mem::size_of_val(&identity),
                ),
                std::mem::size_of_val(&identity) as isize
            );
            let mut middle_status = 0;
            assert_eq!(libc::waitpid(middle, &mut middle_status, 0), middle);
            assert!(libc::WIFEXITED(middle_status));
            assert_eq!(libc::WEXITSTATUS(middle_status), 0);
            assert_eq!(identity[0], identity[1]);
            let deadline = Instant::now() + Duration::from_secs(5);
            while libc::kill(identity[0], 0) == 0 && Instant::now() < deadline {
                libc::usleep(10_000);
            }
            assert_eq!(libc::kill(identity[0], 0), -1);
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::ESRCH)
            );
        }
    }
}
