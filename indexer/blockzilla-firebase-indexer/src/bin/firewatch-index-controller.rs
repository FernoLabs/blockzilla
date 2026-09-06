//! Resource-aware controller for the per-epoch Firewatch index project.
//!
//! The archive scheduler remains the priority owner of the NAS. This controller runs at most
//! a bounded adaptive pool of Firewatch child process groups, yields the pool when scheduler work
//! or resource pressure appears, and grows it only after a sustained safe window. It does not change scheduler
//! state, registry markers, archive generations, or unknown index staging directories.

#[path = "../firewatch_controller_cgroup.rs"]
mod firewatch_controller_cgroup;
#[path = "../firewatch_controller_eta.rs"]
mod firewatch_controller_eta;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::{
        fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_firebase_indexer::firewatch_wire_profile_attestation::{
    DIRECT_ATTESTATION_GENERATION_KIND, RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND,
    RECEIPT_TARGET_ATTESTATION_GENERATION_KIND, WireProfileAttestation,
    validate_receipt_source_recovery_evidence, validate_wire_profile_attestation_structure,
};
#[cfg(test)]
use blockzilla_firebase_indexer::firewatch_wire_profile_attestation::{
    FullGenerationAuditDecisionV3, FullGenerationAuditEvidenceV3,
    WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION, WIRE_PROFILE_AUDIT_ALGORITHM,
    WIRE_PROFILE_AUDITED_PROFILES, encode_full_generation_audit_evidence_v3,
    encode_receipt_source_recovery_evidence_v3,
};
use blockzilla_firebase_indexer::format::{
    FORMAT_VERSION, GenerationBindingKind, IndexManifest, MANIFEST_SCHEMA_VERSION,
    RegistryFileIdentity, SEMANTICS_VERSION,
};
use blockzilla_archive_v2::{ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SIGNATURES_FILE};
use blockzilla_read_sdk_legacy::{ArchiveV2WireProfile, wire_profile_marker, wire_profile_marker_bytes};
use clap::Parser;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use firewatch_controller_cgroup::{
    CgroupMemoryEvents, CgroupMemorySnapshot, read_cgroup_memory, resolve_self_cgroup_v2,
};
use firewatch_controller_eta::{
    ActiveItemWork, ActivePhaseWork, CompletedPhaseSample, EtaEstimator, EtaHistory, EtaPhase,
    PhaseWork, QueueEtaInput, QueuedItemWork,
};

const STATUS_SCHEMA_VERSION: u32 = 1;
const WORKFLOW_SCHEMA_VERSION: u32 = 3;
const LEGACY_PROFILE_BOUND_WORKFLOW_SCHEMA_VERSION: u32 = 2;
const ACCEPTANCE_SCHEMA_VERSION: u32 = 3;
const MAX_CONTROL_JSON_BYTES: u64 = 16 * 1024 * 1024;
const MAX_STATUS_JSON_BYTES: u64 = 4 * 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;
const MAX_FIREWATCH_WORKERS: usize = 4;
const DIRECT_GENERATION_DOMAIN: &[u8] = b"blockzilla.firewatch.direct-generation.v1\0";
const EFFECTIVE_INPUT_DOMAIN: &[u8] = b"blockzilla.firewatch.archive-v2-effective-input.v3\0";
const DIRECT_ACCEPTANCE_SCHEMA_VERSION: u32 = 3;
const RETRY_READY_SCHEMA_VERSION: u32 = 1;
const CURRENT_REGISTRY_MARKER_SCHEMA_VERSION: u32 = 4;
const REGISTRY_RECEIPT_FILE: &str = "archive-v2-registry-reprocess.receipt.json";
const REGISTRY_ACCESS_TEMP_SUFFIX: &str = ".registry-access.tmp";
const REGISTRY_RECEIPT_V3_ALGORITHM: &str =
    "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3";
const WIRE_PROFILE_ATTESTATIONS_DIR: &str = "wire-profile-attestations";
const RETRY_READY_DIR: &str = "retry-ready";
const ETA_HISTORY_FILE: &str = "eta-history.json";
const MAX_DURABLE_ETA_SAMPLES: usize = 4_096;
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
    name = "firewatch-index-controller",
    about = "Build and verify Firewatch indexes while giving the archive scheduler priority"
)]
struct Args {
    /// Scheduler read-only status endpoint. Loopback HTTP only.
    #[arg(
        long,
        env = "BLOCKZILLA_FIREWATCH_SCHEDULER_STATUS_URL",
        default_value = "http://127.0.0.1:8786/api/v1/status"
    )]
    scheduler_status_url: String,

    /// Scheduler state root containing registry-reprocess completion markers.
    #[arg(long, env = "BLOCKZILLA_STATE_ROOT")]
    scheduler_state_root: PathBuf,

    /// Canonical first-seen archive root.
    #[arg(long, env = "BLOCKZILLA_ARCHIVE_ROOT")]
    archive_root: PathBuf,

    /// Immutable usage-sorted generation root.
    #[arg(long, env = "BLOCKZILLA_REGISTRY_REPROCESS_TARGET_ROOT")]
    usage_sorted_root: PathBuf,

    /// Root receiving immutable per-epoch Firewatch index generations.
    #[arg(long, env = "BLOCKZILLA_FIREWATCH_INDEX_ROOT")]
    output_root: PathBuf,

    /// Parent for private index-parity sort workspaces.
    #[arg(long, env = "BLOCKZILLA_FIREWATCH_PARITY_SCRATCH_ROOT")]
    parity_scratch_root: PathBuf,

    /// Deployed index-builder executable.
    #[arg(long, env = "BLOCKZILLA_FIREWATCH_INDEXER_BIN")]
    indexer_bin: PathBuf,

    /// Deployed registry-aware parity executable.
    #[arg(long, env = "BLOCKZILLA_FIREWATCH_PARITY_BIN")]
    parity_bin: PathBuf,

    /// Controller-owned durable markers and logs.
    #[arg(long, env = "BLOCKZILLA_FIREWATCH_STATE_ROOT")]
    controller_state_root: PathBuf,

    /// Atomic schema-1 JSON consumed by the monitor.
    #[arg(long, env = "BLOCKZILLA_FIREWATCH_STATUS_FILE")]
    status_file: PathBuf,

    /// Decoder threads passed to each dense index build.
    #[arg(long, env = "BLOCKZILLA_FIREWATCH_THREADS", default_value_t = 4)]
    threads: usize,

    /// Decoder threads used by each worker after the first one.
    #[arg(
        long,
        env = "BLOCKZILLA_FIREWATCH_ADDITIONAL_WORKER_THREADS",
        default_value_t = 2
    )]
    additional_worker_threads: usize,

    /// Maximum number of Firewatch phases that adaptive admission can run together.
    #[arg(long, env = "BLOCKZILLA_FIREWATCH_MAX_WORKERS", default_value_t = 1)]
    max_workers: usize,

    /// Memory headroom reserved for each additional Firewatch phase.
    #[arg(
        long,
        env = "BLOCKZILLA_FIREWATCH_WORKER_MEMORY_RESERVE_MIB",
        default_value_t = 768
    )]
    worker_memory_reserve_mib: u64,

    /// Host memory that Firewatch keeps available before it adds another worker.
    #[arg(
        long,
        env = "BLOCKZILLA_FIREWATCH_HOST_MEMORY_RESERVE_MIB",
        default_value_t = 3072
    )]
    host_memory_reserve_mib: u64,

    /// Maximum host I/O full pressure used only when Firewatch adds another worker.
    #[arg(
        long,
        env = "BLOCKZILLA_FIREWATCH_ADDITIONAL_IO_FULL_MAX",
        default_value_t = 10.0
    )]
    additional_io_full_max: f64,

    /// Maximum host CPU pressure used only when Firewatch adds another worker.
    #[arg(
        long,
        env = "BLOCKZILLA_FIREWATCH_ADDITIONAL_CPU_SOME_MAX",
        default_value_t = 20.0
    )]
    additional_cpu_some_max: f64,

    /// Registry-aware parity sort memory.
    #[arg(
        long,
        env = "BLOCKZILLA_FIREWATCH_PARITY_SORT_MEMORY_MIB",
        default_value_t = 256
    )]
    parity_sort_memory_mib: u64,

    /// Controller poll interval.
    #[arg(long, env = "BLOCKZILLA_FIREWATCH_POLL_SECS", default_value_t = 5)]
    poll_secs: u64,

    /// Sustained safe time required before a start or resume.
    #[arg(
        long,
        env = "BLOCKZILLA_FIREWATCH_RESUME_STABLE_SECS",
        default_value_t = 60
    )]
    resume_stable_secs: u64,

    /// Safe time used only to refill one slot after a verified successful child completion.
    #[arg(
        long,
        env = "BLOCKZILLA_FIREWATCH_CLEAN_TRANSITION_SECS",
        default_value_t = 10
    )]
    clean_transition_secs: u64,

    /// Keep this much shared archive-disk space free for scheduler work and Firewatch scratch.
    #[arg(
        long,
        env = "BLOCKZILLA_FIREWATCH_DISK_RESERVE_GIB",
        default_value_t = 512
    )]
    disk_reserve_gib: u64,

    /// Start child work. Without this flag, publish an observer-only project status.
    #[arg(long)]
    execute: bool,

    /// Publish one no-clobber retry authorization for an exact failed direct epoch, then exit.
    #[arg(long, conflicts_with = "execute", requires = "retry_reason")]
    authorize_direct_retry_epoch: Option<u64>,

    /// Short operator reason recorded in the durable retry authorization.
    #[arg(long, requires = "authorize_direct_retry_epoch")]
    retry_reason: Option<String>,
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
    epochs: Vec<SchedulerEpoch>,
    lanes: Vec<SchedulerLane>,
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
    cpu_pressure_some_avg10: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct SchedulerSummary {
    queued: usize,
    scan_ready: usize,
    poh_migration_epochs_runnable: usize,
    registry_reprocess_epochs_runnable: usize,
    registry_reprocess_audits_runnable: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerInventory {
    complete: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct SchedulerScanSweep {
    pending: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerLane {
    #[allow(dead_code)]
    id: String,
    state: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SchedulerEpoch {
    epoch: u64,
    state: String,
    registry_order: String,
    /// Scheduler schema v3 deliberately publishes only the canonical basename.
    output_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct RegistryMarker {
    schema_version: u32,
    kind: String,
    epoch: u64,
    state: String,
    phase: Option<String>,
    source: PathBuf,
    target: PathBuf,
    #[serde(default)]
    threads: Option<usize>,
    #[serde(default)]
    wire_profile: Option<ArchiveV2WireProfile>,
    #[serde(default)]
    attempt_id: Option<String>,
    #[serde(default)]
    staging_dir: Option<PathBuf>,
    #[serde(default)]
    handoff_sha256: Option<String>,
    #[serde(default)]
    expected_access_state: Option<String>,
    pid: Option<u32>,
    process_start_ticks: Option<u64>,
    #[serde(default)]
    audit_retry_is_safe: bool,
    #[serde(default)]
    audit_is_continuation: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RegistryReceipt {
    version: u32,
    algorithm: String,
    epoch: u64,
    #[serde(default)]
    threads: Option<usize>,
    source_dir: String,
    target_dir: String,
    source_generation_sha256: String,
    target_generation_sha256: String,
    source_files: BTreeMap<String, RegistryFileBinding>,
    target_files: BTreeMap<String, RegistryFileBinding>,
    #[serde(default)]
    attempt_id: Option<String>,
    #[serde(default)]
    handoff_sha256: Option<String>,
    #[serde(default)]
    wire_profile: Option<ArchiveV2WireProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct RegistryFileBinding {
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CandidateMode {
    MigrationPair,
    CanonicalDirect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompleteRegistryContract {
    Legacy,
    ProfileBoundV4,
}

#[derive(Debug, Clone)]
struct Candidate {
    mode: CandidateMode,
    epoch: u64,
    source: PathBuf,
    target: PathBuf,
    source_generation: String,
    target_generation: String,
    source_wire_profile: ArchiveV2WireProfile,
    target_wire_profile: ArchiveV2WireProfile,
    source_effective_input: String,
    target_effective_input: String,
    /// Digest of the exact inode identities and bytes that authorized both
    /// wire profiles. A same-value replacement must change the candidate set
    /// before a worker can start.
    authority_binding_sha256: String,
    /// Live descriptors for every file that formed the authority digest.
    /// Production candidates always contain this set; narrow unit fixtures
    /// that never cross a trust boundary can omit it.
    authority_proofs: Option<Arc<AuthorityProofSet>>,
    registry_order: String,
    direct_files: Option<BTreeMap<String, RegistryFileIdentity>>,
    input_bytes: u64,
    /// Exact failed workflow marker authorized by a one-shot retry-ready record.
    retry_of_failed_marker_sha256: Option<String>,
}

impl Candidate {
    fn epoch_root(&self, args: &Args) -> PathBuf {
        args.output_root.join(format!("epoch-{}", self.epoch))
    }

    fn source_index(&self, args: &Args) -> PathBuf {
        match self.mode {
            CandidateMode::MigrationPair => self
                .epoch_root(args)
                .join(format!("source-first-seen-{}", self.source_effective_input)),
            CandidateMode::CanonicalDirect => self.direct_index(args),
        }
    }

    fn target_index(&self, args: &Args) -> PathBuf {
        match self.mode {
            CandidateMode::MigrationPair => self.epoch_root(args).join(format!(
                "target-usage-sorted-{}",
                self.target_effective_input
            )),
            CandidateMode::CanonicalDirect => self.direct_index(args),
        }
    }

    fn direct_index(&self, args: &Args) -> PathBuf {
        self.epoch_root(args).join(format!(
            "canonical-{}-{}",
            self.registry_order, self.target_effective_input
        ))
    }

    fn acceptance_path(&self, args: &Args) -> PathBuf {
        match self.mode {
            // A pair receipt must be generation-profile scoped too. The old
            // fixed receipt remains immutable during a controlled rebuild.
            CandidateMode::MigrationPair => self.epoch_root(args).join(format!(
                "firewatch-index-{}-{}.accepted.json",
                self.source_effective_input, self.target_effective_input
            )),
            CandidateMode::CanonicalDirect => self.epoch_root(args).join(format!(
                "canonical-{}-{}.accepted.json",
                self.registry_order, self.target_effective_input
            )),
        }
    }

    fn workflow_path(&self, args: &Args) -> PathBuf {
        let kind = match self.mode {
            CandidateMode::MigrationPair => "epochs",
            CandidateMode::CanonicalDirect => "canonical-epochs",
        };
        args.controller_state_root
            .join(kind)
            .join(format!("epoch-{}.json", self.epoch))
    }

    fn direct_files(&self) -> Result<&BTreeMap<String, RegistryFileIdentity>> {
        ensure!(
            self.mode == CandidateMode::CanonicalDirect,
            "pair candidate has no direct archive binding"
        );
        self.direct_files
            .as_ref()
            .context("direct candidate has no archive file bindings")
    }

    fn input_bytes(&self) -> u64 {
        self.input_bytes
    }

    fn legacy_direct_index(&self, args: &Args) -> PathBuf {
        self.epoch_root(args).join(format!(
            "canonical-{}-{}",
            self.registry_order, self.target_generation
        ))
    }

    fn legacy_direct_acceptance_path(&self, args: &Args) -> PathBuf {
        self.epoch_root(args).join(format!(
            "canonical-{}-{}.accepted.json",
            self.registry_order, self.target_generation
        ))
    }

    fn legacy_source_index(&self, args: &Args) -> PathBuf {
        self.epoch_root(args)
            .join(format!("source-first-seen-{}", self.source_generation))
    }

    fn legacy_target_index(&self, args: &Args) -> PathBuf {
        self.epoch_root(args)
            .join(format!("target-usage-sorted-{}", self.target_generation))
    }

    fn legacy_pair_acceptance_path(&self, args: &Args) -> PathBuf {
        self.epoch_root(args).join("firewatch-index.accepted.json")
    }

    fn ensure_profile_bound_paths_do_not_alias_legacy(&self, args: &Args) -> Result<()> {
        // Legacy indexes were built before the decoder profile was part of
        // generation identity. They are never adopted. A rebuild is safe
        // only when every new final path is a distinct no-clobber target.
        match self.mode {
            CandidateMode::MigrationPair => {
                ensure!(
                    self.source_index(args) != self.legacy_source_index(args)
                        && self.target_index(args) != self.legacy_target_index(args)
                        && self.acceptance_path(args) != self.legacy_pair_acceptance_path(args),
                    "profile-bound pair output aliases a legacy output path"
                );
            }
            CandidateMode::CanonicalDirect => {
                ensure!(
                    self.direct_index(args) != self.legacy_direct_index(args)
                        && self.acceptance_path(args) != self.legacy_direct_acceptance_path(args),
                    "profile-bound direct output aliases a legacy output path"
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetryReady {
    schema_version: u32,
    kind: String,
    epoch: u64,
    mode: String,
    content_generation_sha256: String,
    effective_input_sha256: String,
    wire_profile: ArchiveV2WireProfile,
    failed_marker_sha256: String,
    authorized_unix_secs: u64,
    reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    TargetBuild,
    SourceControlBuild,
    Parity,
    CanonicalBuild,
}

impl Phase {
    fn as_status(self) -> &'static str {
        match self {
            Self::TargetBuild => "target_build",
            Self::SourceControlBuild => "source_control_build",
            Self::Parity => "parity",
            Self::CanonicalBuild => "canonical_build",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkflowMarker {
    schema_version: u32,
    epoch: u64,
    source_generation_sha256: String,
    target_generation_sha256: String,
    #[serde(default)]
    source_effective_input_sha256: Option<String>,
    #[serde(default)]
    target_effective_input_sha256: Option<String>,
    #[serde(default)]
    source_wire_profile: Option<ArchiveV2WireProfile>,
    #[serde(default)]
    target_wire_profile: Option<ArchiveV2WireProfile>,
    #[serde(default)]
    authority_binding_sha256: Option<String>,
    #[serde(default)]
    retry_of_failed_marker_sha256: Option<String>,
    state: String,
    phase: Phase,
    created_unix_secs: u64,
    updated_unix_secs: u64,
    attempt_id: Option<String>,
    pid: Option<u32>,
    process_start_ticks: Option<u64>,
    executable: Option<PathBuf>,
    executable_dev: Option<u64>,
    executable_ino: Option<u64>,
    #[serde(default)]
    argv: Vec<String>,
    log_path: Option<PathBuf>,
    auto_paused: bool,
    auto_pause_reason: Option<String>,
    owned_temp_path: Option<PathBuf>,
    #[serde(default)]
    cleanup_owner_absence_confirmed: bool,
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AcceptanceReceipt {
    schema_version: u32,
    epoch: u64,
    source_generation_sha256: String,
    target_generation_sha256: String,
    source_effective_input_sha256: String,
    target_effective_input_sha256: String,
    source_wire_profile: ArchiveV2WireProfile,
    target_wire_profile: ArchiveV2WireProfile,
    authority_binding_sha256: String,
    source_index: PathBuf,
    target_index: PathBuf,
    source_manifest_sha256: String,
    target_manifest_sha256: String,
    wallets: u64,
    relations: u64,
    canonical_sha256: String,
    accepted_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexGenerationIdentity {
    manifest_sha256: String,
    files: BTreeMap<String, RegistryFileIdentity>,
}

#[derive(Debug)]
struct VerifiedIndexGeneration {
    manifest: IndexManifest,
    identity: IndexGenerationIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PairAcceptanceVerification {
    source: IndexGenerationIdentity,
    target: IndexGenerationIdentity,
}

#[derive(Debug, Default)]
struct PairAcceptanceVerificationCache {
    entries: BTreeMap<PathBuf, PairAcceptanceVerification>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectAcceptanceReceipt {
    schema_version: u32,
    mode: String,
    epoch: u64,
    registry_order: String,
    content_generation_sha256: String,
    effective_input_sha256: String,
    wire_profile: ArchiveV2WireProfile,
    authority_binding_sha256: String,
    archive: PathBuf,
    archive_files: BTreeMap<String, RegistryFileIdentity>,
    input_bytes: u64,
    index: PathBuf,
    index_manifest_sha256: String,
    wallets: u64,
    programs: u64,
    transactions_scanned: u64,
    blocks_scanned: u64,
    failed_transactions_excluded: u64,
    accepted_unix_secs: u64,
}

#[derive(Debug, Clone)]
enum CandidateAcceptance {
    Pair(AcceptanceReceipt),
    Direct(DirectAcceptanceReceipt),
}

impl CandidateAcceptance {
    fn wallets(&self) -> u64 {
        match self {
            Self::Pair(receipt) => receipt.wallets,
            Self::Direct(receipt) => receipt.wallets,
        }
    }

    fn relations(&self) -> Option<u64> {
        match self {
            Self::Pair(receipt) => Some(receipt.relations),
            Self::Direct(_) => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ControllerStatus {
    schema_version: u32,
    updated_unix_secs: u64,
    capacity_configured: u32,
    running: u32,
    epochs_total: u32,
    epochs_accepted: u32,
    epochs_queued: u32,
    archive_epochs_total: Option<u32>,
    epochs_eligible: Option<u32>,
    epochs_blocked_migration: Option<u32>,
    epochs_blocked_wire_profile: Option<u32>,
    queue_eta_secs: Option<f64>,
    admission_blocked_reason: Option<String>,
    rows: Vec<StatusRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct WireProfileAuditInput {
    archive: PathBuf,
    registry_order: String,
    generation_kind: String,
    content_generation_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WireProfileBlockedCandidate {
    epoch: u64,
    mode: CandidateMode,
    inputs: Vec<WireProfileAuditInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectWireProfileMarker {
    profile: ArchiveV2WireProfile,
    name: String,
    identity: RegistryFileIdentity,
}

#[derive(Debug, Clone, Serialize)]
struct StatusRow {
    epoch: u32,
    state: String,
    phase: String,
    auto_paused: bool,
    auto_pause_reason: Option<String>,
    progress_pct: f32,
    blocks_done: u64,
    eta_secs: Option<f64>,
    rss_bytes: Option<u64>,
    read_mib_per_sec: Option<f64>,
    write_mib_per_sec: Option<f64>,
    wallet_count: Option<u64>,
    relation_count: Option<u64>,
    parity_status: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    wire_profile_audit_inputs: Vec<WireProfileAuditInput>,
}

#[derive(Debug, Clone)]
struct CandidateView {
    candidate: Candidate,
    phase: Phase,
    state: String,
    acceptance: Option<CandidateAcceptance>,
    target_manifest: Option<IndexManifest>,
    source_manifest: Option<IndexManifest>,
}

#[derive(Debug)]
struct ActiveChild {
    candidate: Candidate,
    phase: Phase,
    child: Child,
    pid: u32,
    start_ticks: u64,
    log_path: PathBuf,
    paused: bool,
    pause_reason: Option<String>,
    owned_temp_path: Option<PathBuf>,
    io_sample: Option<ProcessIoSample>,
    telemetry: ProcessTelemetry,
    worker_threads: u32,
    started_unix_secs: u64,
    started_at: Instant,
    paused_started_at: Option<Instant>,
    paused_secs: Duration,
}

impl Drop for ActiveChild {
    fn drop(&mut self) {
        if process_start_ticks(self.pid) == Some(self.start_ticks) {
            terminate_pid_group(self.pid, self.start_ticks);
            let _ = self.child.wait();
        }
        // Temporary workspace cleanup is durable and incremental. Do not recursively remove a
        // potentially large tree while the controller is stopping or while scheduler pressure is
        // changing.
    }
}

#[derive(Debug, Clone, Copy)]
struct ProcessIoSample {
    read_bytes: u64,
    write_bytes: u64,
    sampled_at: Instant,
}

#[derive(Debug, Default, Clone, Copy)]
struct ProcessTelemetry {
    rss_bytes: Option<u64>,
    read_mib_per_sec: Option<f64>,
    write_mib_per_sec: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionWindowKind {
    Cold,
    CleanTransition,
}

#[derive(Debug, Clone, Copy)]
struct AdmissionWindow {
    kind: AdmissionWindowKind,
    safe_since: Instant,
}

#[derive(Debug, Default)]
struct PoolState {
    children: BTreeMap<u64, ActiveChild>,
    admission_window: Option<AdmissionWindow>,
    resume_safe_since: Option<Instant>,
    previous_cgroup: Option<CgroupMemorySnapshot>,
}

impl PoolState {
    fn clear_admission_window(&mut self) {
        self.admission_window = None;
    }

    fn grant_clean_transition(&mut self, now: Instant) {
        self.admission_window = Some(AdmissionWindow {
            kind: AdmissionWindowKind::CleanTransition,
            safe_since: now,
        });
    }

    fn admission_window(&mut self, now: Instant) -> AdmissionWindow {
        *self.admission_window.get_or_insert(AdmissionWindow {
            kind: AdmissionWindowKind::Cold,
            safe_since: now,
        })
    }
}

fn admission_window_secs(args: &Args, kind: AdmissionWindowKind) -> u64 {
    match kind {
        AdmissionWindowKind::Cold => args.resume_stable_secs,
        AdmissionWindowKind::CleanTransition => args.clean_transition_secs,
    }
}

fn update_admission_window_after_completions(
    pool: &mut PoolState,
    clean_completion_context_safe: bool,
    clean_completion_seen: bool,
    completion_fault_seen: bool,
    now: Instant,
) {
    if completion_fault_seen {
        pool.clear_admission_window();
    } else if clean_completion_seen {
        if clean_completion_context_safe {
            pool.grant_clean_transition(now);
        } else {
            pool.clear_admission_window();
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum AdmissionDecision {
    Admit,
    Wait(String),
}

#[derive(Debug, Clone, PartialEq)]
enum PressureState {
    Cancel(String),
    Pause(String),
    Hold,
    Safe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PressureContext {
    /// No Firewatch worker is active. Any measured I/O pressure comes from other work.
    Admission,
    /// A Firewatch worker is running. Its own I/O must not make it stop itself.
    Active,
    /// A Firewatch worker is stopped. It adds no new I/O, so PSI is a conservative resume gate;
    /// the rolling average can still include its earlier I/O for a short time.
    Paused,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    validate_args(&args)?;
    install_shutdown_handlers()?;
    create_root_without_symlink_ancestors(&args.controller_state_root)?;
    create_root_without_symlink_ancestors(&args.output_root)?;
    create_root_without_symlink_ancestors(&args.parity_scratch_root)?;
    validate_created_path_layout(&args)?;
    create_child_directory(&args.controller_state_root, "epochs")?;
    create_child_directory(&args.controller_state_root, "canonical-epochs")?;
    create_child_directory(&args.controller_state_root, "logs")?;
    create_child_directory(&args.controller_state_root, WIRE_PROFILE_ATTESTATIONS_DIR)?;
    create_child_directory(&args.controller_state_root, RETRY_READY_DIR)?;
    create_child_directory(
        &args.controller_state_root.join(RETRY_READY_DIR),
        "canonical-epochs",
    )?;
    let _controller_lock = acquire_controller_lock(&args.controller_state_root)?;

    let client = Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    if let Some(epoch) = args.authorize_direct_retry_epoch {
        let path = authorize_direct_retry(
            &args,
            &client,
            epoch,
            args.retry_reason
                .as_deref()
                .context("retry authorization has no reason")?,
        )?;
        println!("retry_ready={}", path.display());
        return Ok(());
    }
    let cgroup_path = match resolve_self_cgroup_v2() {
        Ok(path) => Some(path),
        Err(error) => {
            tracing::warn!(%error, "adaptive Firewatch capacity is limited to one worker");
            None
        }
    };
    run_loop(&args, &client, cgroup_path.as_deref())
}

fn create_root_without_symlink_ancestors(path: &Path) -> Result<()> {
    let mut current = PathBuf::from("/");
    for component in path.components().skip(1) {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) => ensure!(
                metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
                "controller root ancestor is not a real directory: {}",
                current.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current).with_context(|| {
                    format!("create controller directory {}", current.display())
                })?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn create_child_directory(parent: &Path, name: &str) -> Result<()> {
    let path = parent.join(name);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
            "controller child is not a real directory: {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(&path)?,
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

extern "C" fn request_shutdown(_signal: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

fn install_shutdown_handlers() -> Result<()> {
    // SAFETY: the handler only stores to a lock-free atomic flag.
    let term = unsafe {
        libc::signal(
            libc::SIGTERM,
            request_shutdown as *const () as libc::sighandler_t,
        )
    };
    // SAFETY: the handler only stores to a lock-free atomic flag.
    let interrupt = unsafe {
        libc::signal(
            libc::SIGINT,
            request_shutdown as *const () as libc::sighandler_t,
        )
    };
    ensure!(
        term != libc::SIG_ERR && interrupt != libc::SIG_ERR,
        "install shutdown signal handlers"
    );
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    ensure!(args.threads > 0, "--threads must be positive");
    ensure!(
        args.additional_worker_threads > 0 && args.additional_worker_threads <= args.threads,
        "--additional-worker-threads must be positive and no larger than --threads"
    );
    ensure!(
        (1..=MAX_FIREWATCH_WORKERS).contains(&args.max_workers),
        "--max-workers must be between 1 and {MAX_FIREWATCH_WORKERS}"
    );
    ensure!(
        args.worker_memory_reserve_mib > 0
            && args.worker_memory_reserve_mib.checked_mul(MIB).is_some(),
        "--worker-memory-reserve-mib must be a positive bounded value"
    );
    ensure!(
        args.host_memory_reserve_mib > 0 && args.host_memory_reserve_mib.checked_mul(MIB).is_some(),
        "--host-memory-reserve-mib must be a positive bounded value"
    );
    ensure!(
        args.additional_io_full_max.is_finite() && args.additional_io_full_max > 0.0,
        "--additional-io-full-max must be positive and finite"
    );
    ensure!(
        args.additional_cpu_some_max.is_finite() && args.additional_cpu_some_max > 0.0,
        "--additional-cpu-some-max must be positive and finite"
    );
    ensure!(args.poll_secs > 0, "--poll-secs must be positive");
    validate_admission_timing(
        args.poll_secs,
        args.clean_transition_secs,
        args.resume_stable_secs,
    )?;
    ensure!(
        args.disk_reserve_gib > 0 && args.disk_reserve_gib.checked_mul(GIB).is_some(),
        "--disk-reserve-gib must be a positive bounded value"
    );
    ensure!(
        args.parity_sort_memory_mib > 0,
        "--parity-sort-memory-mib must be positive"
    );
    if let Some(reason) = args.retry_reason.as_deref() {
        ensure!(
            !reason.is_empty() && reason.len() <= 1_024 && !reason.chars().any(char::is_control),
            "--retry-reason must be 1 to 1024 printable bytes"
        );
    }
    validate_scheduler_url(&args.scheduler_status_url)?;
    validate_path_arguments(args)?;
    ensure!(
        args.indexer_bin.is_file(),
        "indexer binary is not a regular file"
    );
    ensure!(
        args.parity_bin.is_file(),
        "parity binary is not a regular file"
    );
    Ok(())
}

fn validate_admission_timing(
    poll_secs: u64,
    clean_transition_secs: u64,
    resume_stable_secs: u64,
) -> Result<()> {
    let minimum_clean_transition_secs = poll_secs
        .checked_mul(2)
        .context("--poll-secs is too large for clean-transition validation")?;
    ensure!(
        clean_transition_secs >= minimum_clean_transition_secs,
        "--clean-transition-secs must be at least two poll intervals ({minimum_clean_transition_secs} seconds)"
    );
    ensure!(
        clean_transition_secs <= resume_stable_secs,
        "--clean-transition-secs must be no larger than --resume-stable-secs"
    );
    Ok(())
}

fn validate_path_arguments(args: &Args) -> Result<()> {
    let paths = [
        &args.scheduler_state_root,
        &args.archive_root,
        &args.usage_sorted_root,
        &args.output_root,
        &args.parity_scratch_root,
        &args.controller_state_root,
        &args.status_file,
        &args.indexer_bin,
        &args.parity_bin,
    ];
    for path in paths {
        ensure!(path.is_absolute(), "all controller paths must be absolute");
        ensure!(
            path.components().all(|component| !matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )),
            "controller paths must be normalized"
        );
    }
    ensure!(
        args.controller_state_root == args.scheduler_state_root.join("firewatch-index"),
        "controller state root must be the scheduler firewatch-index child"
    );
    ensure!(
        args.status_file == args.controller_state_root.join("status.json"),
        "status file must be controller-state-root/status.json"
    );
    ensure!(
        args.usage_sorted_root.starts_with(&args.archive_root),
        "usage-sorted root must be inside archive root"
    );
    for writable in [
        &args.output_root,
        &args.parity_scratch_root,
        &args.controller_state_root,
    ] {
        ensure!(
            !writable.starts_with(&args.archive_root) && !args.archive_root.starts_with(writable),
            "writable Firewatch roots must not overlap the archive root"
        );
    }
    Ok(())
}

fn validate_created_path_layout(args: &Args) -> Result<()> {
    for path in [
        &args.scheduler_state_root,
        &args.archive_root,
        &args.usage_sorted_root,
        &args.output_root,
        &args.parity_scratch_root,
        &args.controller_state_root,
    ] {
        let metadata = fs::symlink_metadata(path)?;
        ensure!(
            metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
            "controller root is not a real directory: {}",
            path.display()
        );
        ensure!(
            fs::canonicalize(path)? == *path,
            "controller root contains a symlink or non-canonical component: {}",
            path.display()
        );
    }
    let writable = [
        fs::canonicalize(&args.output_root)?,
        fs::canonicalize(&args.parity_scratch_root)?,
        fs::canonicalize(&args.controller_state_root)?,
    ];
    for (index, left) in writable.iter().enumerate() {
        for right in &writable[index + 1..] {
            ensure!(
                !left.starts_with(right) && !right.starts_with(left),
                "writable Firewatch roots overlap"
            );
        }
    }
    Ok(())
}

fn validate_scheduler_url(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value).context("parse scheduler status URL")?;
    ensure!(url.scheme() == "http", "scheduler status URL must use HTTP");
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "scheduler status URL must not contain user information"
    );
    ensure!(
        url.host_str() == Some("127.0.0.1"),
        "scheduler status URL must use exact IPv4 loopback"
    );
    ensure!(
        url.fragment().is_none(),
        "scheduler status URL must not contain a fragment"
    );
    Ok(())
}

fn acquire_controller_lock(root: &Path) -> Result<File> {
    fs::create_dir_all(root)?;
    let path = root.join("controller.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
        .with_context(|| format!("open controller lock {}", path.display()))?;
    ensure!(
        file.metadata()?.file_type().is_file(),
        "controller lock is not a regular file"
    );
    // SAFETY: `file` owns a live descriptor for the controller lifetime.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    ensure!(result == 0, "another Firewatch controller holds the lock");
    Ok(file)
}

use std::os::fd::AsRawFd;

fn run_loop(args: &Args, client: &Client, cgroup_path: Option<&Path>) -> Result<()> {
    let mut pool = PoolState::default();
    let mut pair_acceptance_cache = PairAcceptanceVerificationCache::default();
    let mut last_candidate_identities: Option<Vec<CandidateIdentity>> = None;
    if let Err(error) = load_or_initialize_eta_history(args).and_then(|history| {
        EtaEstimator::from_history(&history).context("build initial Firewatch ETA calibration")
    }) {
        tracing::warn!(%error, "Firewatch ETA calibration is unavailable; indexing will continue");
    }

    loop {
        let mut runtime_reason = None;
        if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            if !pool.children.is_empty() {
                cancel_pool(args, &mut pool, "controller shutdown")?;
            }
            return Ok(());
        }

        // Priority control is always first. It is bounded by one scheduler request and never waits
        // for archive discovery, index validation, cleanup, or another Firewatch child.
        let scheduler = fetch_scheduler_status(client, &args.scheduler_status_url);
        let (mut cgroup, cgroup_error) = match cgroup_path.map(read_cgroup_memory) {
            Some(Ok(snapshot)) => (Some(snapshot), None),
            Some(Err(error)) => (
                None,
                Some(format!("cgroup telemetry unavailable: {error:#}")),
            ),
            None => (
                None,
                Some("cgroup telemetry is not configured for adaptive capacity".into()),
            ),
        };
        if !pool.children.is_empty() {
            if let Some(reason) = fail_children_with_invalid_authority(args, &mut pool)? {
                runtime_reason = Some(reason);
            }
            let context = pool_pressure_context(&pool);
            let mut pressure = match &scheduler {
                Ok(snapshot) => pressure_state(
                    snapshot,
                    args.disk_reserve_gib * GIB,
                    context,
                    args.host_memory_reserve_mib * MIB,
                ),
                Err(error) => {
                    PressureState::Cancel(format!("scheduler status unavailable: {error}"))
                }
            };
            if !matches!(pressure, PressureState::Cancel(_) | PressureState::Pause(_))
                && pool.children.len() > 1
            {
                if let Ok(snapshot) = &scheduler {
                    let memory_some = snapshot
                        .machine
                        .memory_pressure_some_avg10
                        .unwrap_or(f64::INFINITY);
                    let host_floor = args.host_memory_reserve_mib.saturating_mul(MIB);
                    let cgroup_memory_some = cgroup
                        .as_ref()
                        .and_then(|snapshot| snapshot.pressure_some_avg10)
                        .unwrap_or(f64::INFINITY);
                    let reduction_reason = if snapshot.machine.memory_available_bytes < host_floor {
                        Some(format!(
                            "host memory available fell below the adaptive reserve ({:.2} GiB)",
                            snapshot.machine.memory_available_bytes as f64 / GIB as f64
                        ))
                    } else if memory_some > 1.0 {
                        Some(format!(
                            "host memory pressure rose above 1% ({memory_some:.2}%)"
                        ))
                    } else if cgroup_memory_some > 1.0 {
                        Some(format!(
                            "Firewatch memory pressure rose above 1% ({cgroup_memory_some:.2}%)"
                        ))
                    } else {
                        None
                    };
                    if let Some(reason) = reduction_reason {
                        let epoch = shed_newest_child(args, &mut pool, &reason)?;
                        runtime_reason = Some(format!(
                            "adaptive worker for epoch {epoch} stopped: {reason}"
                        ));
                    }
                }
            }
            if !matches!(pressure, PressureState::Cancel(_)) {
                if let Some(reason) = cgroup_error.as_deref() {
                    if pool.children.len() > 1 {
                        let epoch = shed_newest_child(args, &mut pool, reason)?;
                        runtime_reason = Some(format!(
                            "adaptive worker for epoch {epoch} stopped: {reason}"
                        ));
                    }
                }
                if let Some(cgroup_pressure) =
                    cgroup_runtime_pressure(pool.previous_cgroup.as_ref(), cgroup.as_ref())
                {
                    let PressureState::Cancel(reason) = cgroup_pressure else {
                        unreachable!("cgroup runtime pressure is always a cancellation")
                    };
                    if pool.children.len() > 1 {
                        let epoch = shed_newest_child(args, &mut pool, &reason)?;
                        runtime_reason = Some(format!(
                            "adaptive worker for epoch {epoch} stopped: {reason}"
                        ));
                    } else {
                        pressure = PressureState::Cancel(reason);
                    }
                }
            }
            if let PressureState::Cancel(reason) = &pressure {
                cancel_pool(args, &mut pool, reason)?;
                runtime_reason = Some(reason.clone());
            } else {
                apply_pool_runtime_pressure(args, &mut pool, &pressure)?;
                let pressure_reason = match &pressure {
                    PressureState::Pause(reason) => Some(reason.clone()),
                    PressureState::Hold if pool.children.values().any(|child| child.paused) => {
                        Some("resource values are inside the pause/resume band".into())
                    }
                    PressureState::Safe | PressureState::Hold => None,
                    PressureState::Cancel(_) => unreachable!(),
                };
                if pressure_reason.is_some() {
                    runtime_reason = pressure_reason;
                }
            }

            // A clean transition is a one-use fast path. It is granted only when every child
            // completion observed in this poll is successful and the current runtime sample is
            // fully safe. Candidate discovery below can still revoke it before admission.
            let clean_completion_context_safe = matches!(pressure, PressureState::Safe)
                && cgroup_error.is_none()
                && runtime_reason.is_none();
            let mut clean_completion_seen = false;
            let mut completion_fault_seen = false;
            let active_epochs = pool.children.keys().copied().collect::<Vec<_>>();
            for epoch in active_epochs {
                let outcome = {
                    let child = pool.children.get_mut(&epoch).expect("pool child exists");
                    sample_process(child);
                    child.child.try_wait()
                };
                match outcome {
                    Ok(Some(status)) => {
                        let finished = pool.children.remove(&epoch).expect("pool child exists");
                        match finish_child(args, finished, status.success()) {
                            Ok(true) if status.success() => clean_completion_seen = true,
                            Ok(_) => completion_fault_seen = true,
                            Err(error) => {
                                completion_fault_seen = true;
                                runtime_reason = Some(format!(
                                    "Firewatch epoch {epoch} child failed: {error:#}"
                                ));
                            }
                        }
                        pool.resume_safe_since = None;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        let mut failed = pool.children.remove(&epoch).expect("pool child exists");
                        kill_child_group(&failed);
                        let _ = failed.child.wait();
                        defer_child_cleanup(args, &failed, true, format!("poll child: {error}"))?;
                        failed.owned_temp_path = None;
                        runtime_reason = Some(format!(
                            "Firewatch epoch {epoch} child poll failed: {error}"
                        ));
                        completion_fault_seen = true;
                        pool.resume_safe_since = None;
                    }
                }
            }
            update_admission_window_after_completions(
                &mut pool,
                clean_completion_context_safe,
                clean_completion_seen,
                completion_fault_seen,
                Instant::now(),
            );
        }
        let mut admission_reason = runtime_reason;

        // Recovery can inspect inactive markers while exact in-memory pool markers are skipped.
        // This lets one healthy epoch continue if a different epoch has a startup recovery claim.
        let recovery_block = match reconcile_all_workflow_markers(args, &pool.children) {
            Ok(reason) => reason,
            Err(error) => Some(format!("workflow recovery failed: {error:#}")),
        };
        let (candidates, blocked_wire_profiles, coverage, mut state_block, candidate_set_changed) =
            match &scheduler {
                Ok(snapshot) => match discover_candidates(args, snapshot) {
                    Ok(discovery) => {
                        let identities = candidate_identities(&discovery.candidates);
                        let changed = last_candidate_identities
                            .as_ref()
                            .is_some_and(|previous| *previous != identities);
                        if changed || last_candidate_identities.is_none() {
                            pool.clear_admission_window();
                        }
                        last_candidate_identities = Some(identities);
                        let coverage = (
                            discovery.archive_epochs_total,
                            discovery.epochs_eligible,
                            discovery.epochs_blocked_migration,
                            discovery.epochs_blocked_wire_profile,
                        );
                        (
                            discovery.candidates,
                            discovery.blocked_wire_profiles,
                            coverage,
                            None,
                            changed,
                        )
                    }
                    Err(error) => {
                        let reason = format!("candidate discovery failed: {error:#}");
                        (Vec::new(), Vec::new(), (0, 0, 0, 0), Some(reason), false)
                    }
                },
                Err(error) => (
                    Vec::new(),
                    Vec::new(),
                    (0, 0, 0, 0),
                    Some(format!(
                        "candidate discovery needs scheduler status: {error:#}"
                    )),
                    false,
                ),
            };
        let views = match build_views(
            args,
            &candidates,
            &pool.children,
            &mut pair_acceptance_cache,
        ) {
            Ok(views) => views,
            Err(error) => {
                let reason = format!("Firewatch state validation failed: {error:#}");
                state_block = Some(reason);
                Vec::new()
            }
        };

        let pressure_context = pool_pressure_context(&pool);
        let admission_pressure = match state_block.as_ref().or(recovery_block.as_ref()) {
            Some(reason) => PressureState::Cancel(reason.clone()),
            None if candidate_set_changed => PressureState::Cancel(
                "Firewatch candidate set changed; waiting for scheduler reconciliation".into(),
            ),
            None => match &scheduler {
                Ok(snapshot) => pressure_state(
                    snapshot,
                    args.disk_reserve_gib * GIB,
                    pressure_context,
                    args.host_memory_reserve_mib * MIB,
                ),
                Err(error) => {
                    PressureState::Cancel(format!("scheduler status unavailable: {error}"))
                }
            },
        };

        let cleanup_allowed = pool.children.is_empty()
            && args.execute
            && state_block.is_none()
            && recovery_block.is_none()
            && scheduler.as_ref().is_ok_and(|snapshot| {
                let no_active_scheduler_lane = !snapshot
                    .lanes
                    .iter()
                    .any(|lane| lane_is_active(&lane.state));
                let disk_low = snapshot.machine.disk_available_bytes
                    < args.disk_reserve_gib.saturating_mul(GIB);
                no_active_scheduler_lane
                    && (matches!(admission_pressure, PressureState::Safe)
                        || disk_low
                        || scheduler_work_is_waiting(snapshot))
            });
        let cleanup_reason = if cleanup_allowed {
            match drain_one_deferred_cleanup(args) {
                Ok(reason) => reason,
                Err(error) => {
                    let reason = format!("deferred Firewatch cleanup failed: {error:#}");
                    state_block = Some(reason.clone());
                    Some(reason)
                }
            }
        } else {
            None
        };

        let mut spawned: Option<(u64, bool, Option<CgroupMemorySnapshot>)> = None;
        if let Some(reason) = cleanup_reason {
            pool.clear_admission_window();
            admission_reason = Some(reason);
        } else if let Some(reason) = state_block.or(recovery_block) {
            pool.clear_admission_window();
            admission_reason = Some(reason);
        } else if !args.execute {
            pool.clear_admission_window();
            admission_reason = Some("observer mode; child execution is disabled".into());
        } else if pool.children.len() >= args.max_workers {
            pool.clear_admission_window();
        } else if let Some(view) = next_runnable(&views, &pool.children) {
            let decision = if pool.children.is_empty() {
                match &admission_pressure {
                    PressureState::Safe => AdmissionDecision::Admit,
                    PressureState::Cancel(reason) | PressureState::Pause(reason) => {
                        AdmissionDecision::Wait(reason.clone())
                    }
                    PressureState::Hold => AdmissionDecision::Wait(
                        "resource values are inside the admission band".into(),
                    ),
                }
            } else if let Ok(snapshot) = &scheduler {
                additional_worker_decision(
                    args,
                    view,
                    snapshot,
                    cgroup.as_ref(),
                    pool.previous_cgroup.as_ref(),
                    &pool,
                )
            } else {
                AdmissionDecision::Wait("scheduler status is unavailable".into())
            };
            match decision {
                AdmissionDecision::Wait(reason) => {
                    pool.clear_admission_window();
                    admission_reason = Some(reason);
                }
                AdmissionDecision::Admit => {
                    let now = Instant::now();
                    let window = pool.admission_window(now);
                    let required = Duration::from_secs(admission_window_secs(args, window.kind));
                    let elapsed = now.saturating_duration_since(window.safe_since);
                    let stable = elapsed >= required;
                    if !stable {
                        let window_name = match window.kind {
                            AdmissionWindowKind::Cold => "adaptive capacity",
                            AdmissionWindowKind::CleanTransition => "clean transition",
                        };
                        admission_reason = Some(format!(
                            "{window_name} is safe; waiting {} more seconds before admission",
                            required.saturating_sub(elapsed).as_secs()
                        ));
                    } else {
                        let candidate = view.candidate.clone();
                        let phase = view.phase;
                        let is_additional = !pool.children.is_empty();
                        let worker_threads = if !is_additional {
                            args.threads
                        } else {
                            args.additional_worker_threads
                        };
                        let mut pre_spawn_cgroup = cgroup;
                        let revalidated = revalidate_before_spawn(
                            args,
                            client,
                            &candidate,
                            last_candidate_identities.as_deref().unwrap_or(&[]),
                            if pool.children.is_empty() {
                                PressureContext::Admission
                            } else {
                                PressureContext::Active
                            },
                        )
                        .and_then(|()| {
                            if !is_additional {
                                return Ok(());
                            }
                            let fresh_scheduler =
                                fetch_scheduler_status(client, &args.scheduler_status_url)?;
                            let path = cgroup_path.context(
                                "cgroup telemetry is unavailable before extra admission",
                            )?;
                            let fresh_cgroup = read_cgroup_memory(path)?;
                            match additional_worker_decision(
                                args,
                                view,
                                &fresh_scheduler,
                                Some(&fresh_cgroup),
                                cgroup.as_ref(),
                                &pool,
                            ) {
                                AdmissionDecision::Admit => {
                                    pre_spawn_cgroup = Some(fresh_cgroup);
                                    Ok(())
                                }
                                AdmissionDecision::Wait(reason) => {
                                    bail!("adaptive headroom changed before spawn: {reason}")
                                }
                            }
                        });
                        match revalidated
                            .and_then(|()| spawn_phase(args, &candidate, phase, worker_threads))
                        {
                            Ok(child) => {
                                ensure!(
                                    pool.children.insert(candidate.epoch, child).is_none(),
                                    "Firewatch admitted the same epoch twice"
                                );
                                admission_reason = None;
                                spawned = Some((candidate.epoch, is_additional, pre_spawn_cgroup));
                            }
                            Err(error) => {
                                admission_reason = Some(format!(
                                    "Firewatch epoch {} could not start: {error:#}",
                                    candidate.epoch
                                ));
                            }
                        }
                        // The clean transition is one-shot. Any completed spawn attempt, including
                        // a failed fresh revalidation, returns to the full cold window.
                        pool.clear_admission_window();
                    }
                }
            }
        } else {
            pool.clear_admission_window();
        }

        // Spawning can take several seconds. Close that race before the first sleep and apply the
        // result to every pool member, not only to the new process.
        if let Some((spawned_epoch, was_additional, pre_spawn_cgroup)) = spawned {
            let post_spawn_scheduler = fetch_scheduler_status(client, &args.scheduler_status_url);
            let post_spawn_pressure = match &post_spawn_scheduler {
                Ok(snapshot) => pressure_state(
                    snapshot,
                    args.disk_reserve_gib * GIB,
                    PressureContext::Active,
                    args.host_memory_reserve_mib * MIB,
                ),
                Err(error) => PressureState::Cancel(format!(
                    "scheduler status unavailable after Firewatch spawn: {error}"
                )),
            };
            if let PressureState::Cancel(reason) = &post_spawn_pressure {
                cancel_pool(args, &mut pool, reason)?;
                admission_reason = Some(reason.clone());
            } else {
                apply_pool_runtime_pressure(args, &mut pool, &post_spawn_pressure)?;
                if let PressureState::Pause(reason) = post_spawn_pressure {
                    admission_reason = Some(reason);
                }
                if was_additional && pool.children.contains_key(&spawned_epoch) {
                    match cgroup_path.map(read_cgroup_memory) {
                        Some(Ok(post_spawn_cgroup)) => {
                            if let Some(PressureState::Cancel(reason)) = cgroup_runtime_pressure(
                                pre_spawn_cgroup.as_ref(),
                                Some(&post_spawn_cgroup),
                            ) {
                                let epoch = shed_newest_child(args, &mut pool, &reason)?;
                                ensure!(
                                    epoch == spawned_epoch,
                                    "adaptive memory response selected the wrong worker"
                                );
                                admission_reason = Some(format!(
                                    "adaptive worker for epoch {epoch} stopped: {reason}"
                                ));
                            }
                            cgroup = Some(post_spawn_cgroup);
                        }
                        Some(Err(error)) => {
                            let reason =
                                format!("cgroup telemetry failed after extra admission: {error:#}");
                            let epoch = shed_newest_child(args, &mut pool, &reason)?;
                            ensure!(
                                epoch == spawned_epoch,
                                "adaptive telemetry response selected the wrong worker"
                            );
                            admission_reason = Some(format!(
                                "adaptive worker for epoch {epoch} stopped: {reason}"
                            ));
                        }
                        None => unreachable!("an extra worker requires a cgroup path"),
                    }
                }
            }
        }

        let effective_capacity = if args.execute {
            if cgroup.is_some() {
                args.max_workers
            } else {
                1
            }
        } else {
            0
        };
        let eta_estimator = load_or_initialize_eta_history(args)
            .and_then(|history| {
                EtaEstimator::from_history(&history).context("build Firewatch ETA calibration")
            })
            .map_err(|error| {
                tracing::warn!(%error, "Firewatch ETA calibration is unavailable");
                error
            })
            .ok();
        let status = build_status(
            args,
            &views,
            &blocked_wire_profiles,
            &pool.children,
            effective_capacity,
            eta_estimator.as_ref(),
            coverage,
            admission_reason.clone(),
        );
        publish_json_atomic(&args.status_file, &status)?;
        if let Some(snapshot) = cgroup {
            pool.previous_cgroup = Some(snapshot);
        }
        interruptible_sleep(Duration::from_secs(args.poll_secs));
    }
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

fn fetch_scheduler_status(client: &Client, url: &str) -> Result<SchedulerStatus> {
    let mut response = client
        .get(url)
        .send()
        .context("fetch scheduler status")?
        .error_for_status()
        .context("scheduler status returned an error")?;
    if let Some(length) = response.content_length() {
        ensure!(
            length <= MAX_CONTROL_JSON_BYTES,
            "scheduler status exceeds size bound"
        );
    }
    let mut bytes = Vec::new();
    response
        .by_ref()
        .take(MAX_CONTROL_JSON_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_CONTROL_JSON_BYTES,
        "scheduler status exceeds size bound"
    );
    let status: SchedulerStatus = serde_json::from_slice(&bytes)?;
    ensure!(
        status.schema_version == 3,
        "scheduler status schema is not version 3"
    );
    ensure!(status.sequence > 0, "scheduler status is missing sequence");
    ensure!(status.now_unix_secs > 0, "scheduler status is missing time");
    ensure!(
        status.now_unix_secs.abs_diff(unix_now()) <= 15,
        "scheduler status is stale or too far in the future"
    );
    ensure!(
        status.control_reconciled_unix_secs > 0
            && status.control_reconciled_unix_secs.abs_diff(unix_now()) <= 15,
        "scheduler control state is stale or too far in the future"
    );
    ensure!(
        !status.observer_mode,
        "archive scheduler is not in execute mode"
    );
    ensure!(
        status.inventory.complete,
        "archive scheduler inventory is incomplete"
    );
    Ok(status)
}

fn pressure_state(
    status: &SchedulerStatus,
    disk_reserve_bytes: u64,
    context: PressureContext,
    host_memory_reserve_bytes: u64,
) -> PressureState {
    if scheduler_work_is_waiting(status) {
        return PressureState::Cancel("archive scheduler has work waiting for resources".into());
    }
    let memory_some = status
        .machine
        .memory_pressure_some_avg10
        .unwrap_or(f64::INFINITY);
    let io_full = status
        .machine
        .io_pressure_full_avg10
        .unwrap_or(f64::INFINITY);
    if status.machine.memory_available_bytes < 2 * GIB {
        return PressureState::Cancel(format!(
            "available memory is below 2 GiB ({:.2} GiB)",
            status.machine.memory_available_bytes as f64 / GIB as f64
        ));
    }
    // Host memory PSI is not attributable while Firewatch is active. The worker can create its
    // own reclaim pressure, just as it creates its own I/O pressure. Direct MemAvailable and
    // cgroup hard-limit events remain the active-worker safety boundary. Use PSI only to block a
    // new worker or a resume.
    if memory_some > 1.0 {
        match context {
            PressureContext::Admission => {
                return PressureState::Cancel(format!(
                    "memory pressure is above 1% ({memory_some:.2}%)"
                ));
            }
            PressureContext::Paused => return PressureState::Hold,
            PressureContext::Active => {}
        }
    }
    if status.machine.disk_available_bytes < disk_reserve_bytes {
        return PressureState::Cancel(format!(
            "archive disk free space is below reserve ({:.2} GiB available; {:.2} GiB required)",
            status.machine.disk_available_bytes as f64 / GIB as f64,
            disk_reserve_bytes as f64 / GIB as f64
        ));
    }
    if status.scheduler.paused {
        return PressureState::Pause("archive scheduler is paused".into());
    }
    if status.lanes.iter().any(|lane| lane_is_active(&lane.state)) {
        return PressureState::Pause("archive scheduler has an active worker".into());
    }
    // Total host PSI cannot distinguish Firewatch I/O from competing I/O. Use it as an
    // admission/resume signal only, when Firewatch is not generating I/O. Scheduler runnable
    // work and active lanes remain separate, immediate priority signals above.
    if context != PressureContext::Active && io_full > 15.0 {
        return PressureState::Pause(format!("I/O pressure is above 15% ({io_full:.2}%)"));
    }
    let io_is_safe = context == PressureContext::Active || io_full < 5.0;
    if status.machine.memory_available_bytes > host_memory_reserve_bytes
        && memory_some < 0.2
        && io_is_safe
    {
        PressureState::Safe
    } else {
        PressureState::Hold
    }
}

fn pool_pressure_context(pool: &PoolState) -> PressureContext {
    if pool.children.is_empty() {
        PressureContext::Admission
    } else if pool.children.values().all(|child| child.paused) {
        PressureContext::Paused
    } else {
        PressureContext::Active
    }
}

fn phase_memory_budget(args: &Args, phase: Phase) -> u64 {
    let configured = args.worker_memory_reserve_mib.saturating_mul(MIB);
    if phase == Phase::Parity {
        configured.max(
            args.parity_sort_memory_mib
                .saturating_add(512)
                .saturating_mul(MIB),
        )
    } else {
        configured
    }
}

fn hard_events_increased(previous: &CgroupMemoryEvents, current: &CgroupMemoryEvents) -> bool {
    current.max > previous.max || current.oom > previous.oom || current.oom_kill > previous.oom_kill
}

fn events_decreased(previous: &CgroupMemoryEvents, current: &CgroupMemoryEvents) -> bool {
    current.high < previous.high
        || current.max < previous.max
        || current.oom < previous.oom
        || current.oom_kill < previous.oom_kill
}

fn cgroup_working_usage_bytes(cgroup: &CgroupMemorySnapshot) -> u64 {
    cgroup
        .current_bytes
        .saturating_sub(cgroup.inactive_file_bytes)
        .saturating_add(cgroup.swap_current_bytes)
}

fn additional_worker_decision(
    args: &Args,
    candidate: &CandidateView,
    status: &SchedulerStatus,
    cgroup: Option<&CgroupMemorySnapshot>,
    previous_cgroup: Option<&CgroupMemorySnapshot>,
    pool: &PoolState,
) -> AdmissionDecision {
    if pool.children.len() >= args.max_workers {
        return AdmissionDecision::Wait("adaptive worker ceiling is full".into());
    }
    if pool.children.values().any(|child| child.paused) {
        return AdmissionDecision::Wait("an existing Firewatch worker is paused".into());
    }
    if candidate.phase == Phase::Parity
        || pool
            .children
            .values()
            .any(|child| child.phase == Phase::Parity)
    {
        return AdmissionDecision::Wait("parity remains single-worker work".into());
    }
    if scheduler_work_is_waiting(status)
        || status.scheduler.paused
        || status.lanes.iter().any(|lane| lane_is_active(&lane.state))
    {
        return AdmissionDecision::Wait("archive scheduler has priority work".into());
    }
    let Some(cgroup) = cgroup else {
        return AdmissionDecision::Wait("cgroup memory telemetry is unavailable".into());
    };
    let Some(previous) = previous_cgroup else {
        return AdmissionDecision::Wait("collecting a cgroup memory baseline".into());
    };
    if events_decreased(&previous.events, &cgroup.events) {
        return AdmissionDecision::Wait("cgroup memory counters were reset".into());
    }
    if hard_events_increased(&previous.events, &cgroup.events) {
        return AdmissionDecision::Wait("cgroup hard memory counters increased".into());
    }
    let budget = phase_memory_budget(args, candidate.phase);
    let host_required = args
        .host_memory_reserve_mib
        .saturating_mul(MIB)
        .saturating_add(budget);
    if status.machine.memory_available_bytes < host_required {
        return AdmissionDecision::Wait(format!(
            "worker headroom needs {:.2} GiB available; {:.2} GiB is available",
            host_required as f64 / GIB as f64,
            status.machine.memory_available_bytes as f64 / GIB as f64
        ));
    }
    if status
        .machine
        .memory_pressure_some_avg10
        .unwrap_or(f64::INFINITY)
        >= 0.2
        || cgroup.pressure_some_avg10.unwrap_or(f64::INFINITY) >= 0.2
    {
        return AdmissionDecision::Wait(
            "memory pressure is not low enough for another worker".into(),
        );
    }
    if status
        .machine
        .io_pressure_full_avg10
        .unwrap_or(f64::INFINITY)
        >= args.additional_io_full_max
    {
        return AdmissionDecision::Wait(
            "I/O headroom is not stable enough for another worker".into(),
        );
    }
    if status
        .machine
        .cpu_pressure_some_avg10
        .unwrap_or(f64::INFINITY)
        >= args.additional_cpu_some_max
    {
        return AdmissionDecision::Wait(
            "CPU headroom is not stable enough for another worker".into(),
        );
    }
    let working_usage = cgroup_working_usage_bytes(cgroup);
    let cgroup_required = working_usage
        .saturating_add(budget)
        .saturating_add(256 * MIB);
    if cgroup
        .high_bytes
        .is_some_and(|limit| cgroup_required > limit)
        || cgroup
            .max_bytes
            .is_some_and(|limit| cgroup_required > limit)
    {
        return AdmissionDecision::Wait(
            "cgroup memory headroom is too small for another worker".into(),
        );
    }
    AdmissionDecision::Admit
}

fn scheduler_work_is_waiting(status: &SchedulerStatus) -> bool {
    status.summary.queued > 0
        || status.summary.scan_ready > 0
        || status.summary.poh_migration_epochs_runnable > 0
        || status.summary.registry_reprocess_epochs_runnable > 0
        || status.summary.registry_reprocess_audits_runnable > 0
        || status.scan_sweep.pending > 0
}

fn lane_is_active(state: &str) -> bool {
    !matches!(
        state,
        "idle" | "done" | "complete" | "completed" | "failed" | "stopped" | "cancelled"
    )
}

fn cgroup_runtime_pressure(
    previous: Option<&CgroupMemorySnapshot>,
    current: Option<&CgroupMemorySnapshot>,
) -> Option<PressureState> {
    let (Some(previous), Some(current)) = (previous, current) else {
        return None;
    };
    if current.events.max > previous.events.max
        || current.events.oom > previous.events.oom
        || current.events.oom_kill > previous.events.oom_kill
    {
        return Some(PressureState::Cancel(
            "Firewatch cgroup reached a hard memory event".into(),
        ));
    }
    None
}

fn apply_pool_runtime_pressure(
    args: &Args,
    pool: &mut PoolState,
    pressure: &PressureState,
) -> Result<()> {
    match pressure {
        PressureState::Cancel(_) => {
            bail!("cancel pressure must be handled before pool pause logic")
        }
        PressureState::Pause(reason) => {
            pool.clear_admission_window();
            pool.resume_safe_since = None;
            let mut errors = Vec::new();
            let mut exited = Vec::new();
            let epochs = pool.children.keys().copied().collect::<Vec<_>>();

            // Send the scheduler-priority signal to every provably live child before marker
            // updates or cleanup. One child can exit between the poll and the signal without
            // preventing later children from stopping.
            for epoch in &epochs {
                let child = pool.children.get_mut(epoch).expect("pool child exists");
                match probe_child_exit(child) {
                    Ok(Some(status)) => exited.push(ExitedPoolChild::Reaped(*epoch, status)),
                    Ok(None) if child.paused => {}
                    Ok(None) => match signal_verified_child_group(child, libc::SIGSTOP) {
                        Ok(OwnedChildSignal::Signaled) => {
                            child.paused = true;
                            child.pause_reason = Some(reason.clone());
                            child.paused_started_at = Some(Instant::now());
                        }
                        Ok(OwnedChildSignal::Exited(status))
                        | Ok(OwnedChildSignal::SignaledAndExited(status)) => {
                            exited.push(exited_pool_child(*epoch, status));
                        }
                        Err(error) => errors.push(format!("epoch {epoch}: {error:#}")),
                    },
                    Err(error) => errors.push(format!("epoch {epoch}: poll before pause: {error}")),
                }
            }
            finish_natural_pool_exits(args, pool, exited, &mut errors);
            for epoch in epochs {
                let Some(child) = pool.children.get_mut(&epoch) else {
                    continue;
                };
                if child.paused {
                    child.pause_reason = Some(reason.clone());
                }
                if let Err(error) = update_running_marker(args, child) {
                    errors.push(format!("epoch {epoch}: publish paused marker: {error:#}"));
                }
            }
            pool_action_result("pause Firewatch pool", errors)?;
        }
        PressureState::Hold => {
            pool.clear_admission_window();
            pool.resume_safe_since = None;
        }
        PressureState::Safe => {
            if pool.children.values().any(|child| child.paused) {
                // A resume always proves the full stable window. A clean completion cannot carry
                // a short admission window across a prior scheduler or pressure pause.
                pool.clear_admission_window();
                let since = pool.resume_safe_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_secs(args.resume_stable_secs) {
                    let mut errors = Vec::new();
                    let mut exited = Vec::new();
                    let epochs = pool.children.keys().copied().collect::<Vec<_>>();
                    for epoch in &epochs {
                        let child = pool.children.get_mut(epoch).expect("pool child exists");
                        match probe_child_exit(child) {
                            Ok(Some(status)) => {
                                exited.push(ExitedPoolChild::Reaped(*epoch, status));
                            }
                            Ok(None) if !child.paused => {}
                            Ok(None) => match signal_verified_child_group(child, libc::SIGCONT) {
                                Ok(OwnedChildSignal::Signaled) => {
                                    if let Some(paused_at) = child.paused_started_at.take() {
                                        child.paused_secs =
                                            child.paused_secs.saturating_add(paused_at.elapsed());
                                    }
                                    child.paused = false;
                                    child.pause_reason = None;
                                }
                                Ok(OwnedChildSignal::Exited(status))
                                | Ok(OwnedChildSignal::SignaledAndExited(status)) => {
                                    exited.push(exited_pool_child(*epoch, status));
                                }
                                Err(error) => {
                                    errors.push(format!("epoch {epoch}: {error:#}"));
                                }
                            },
                            Err(error) => {
                                errors.push(format!("epoch {epoch}: poll before resume: {error}"));
                            }
                        }
                    }
                    finish_natural_pool_exits(args, pool, exited, &mut errors);
                    for epoch in epochs {
                        let Some(child) = pool.children.get_mut(&epoch) else {
                            continue;
                        };
                        if let Err(error) = update_running_marker(args, child) {
                            errors
                                .push(format!("epoch {epoch}: publish resumed marker: {error:#}"));
                        }
                    }
                    pool.resume_safe_since = None;
                    pool_action_result("resume Firewatch pool", errors)?;
                }
            } else {
                pool.resume_safe_since = None;
            }
        }
    }
    Ok(())
}

fn cancel_pool(args: &Args, pool: &mut PoolState, reason: &str) -> Result<()> {
    let mut errors = Vec::new();
    let mut exited = Vec::new();
    let mut cancelled = Vec::new();
    let epochs = pool.children.keys().copied().collect::<Vec<_>>();

    // This is deliberately a broadcast phase. Cleanup starts only after every child that still
    // has the exact controller-owned PID, start tick, and process group has received SIGKILL.
    for epoch in &epochs {
        let child = pool.children.get_mut(epoch).expect("pool child exists");
        match probe_child_exit(child) {
            Ok(Some(status)) => exited.push(ExitedPoolChild::Reaped(*epoch, status)),
            Ok(None) => match signal_verified_child_group(child, libc::SIGKILL) {
                Ok(OwnedChildSignal::Signaled) => cancelled.push((*epoch, None)),
                Ok(OwnedChildSignal::SignaledAndExited(status)) => {
                    cancelled.push((*epoch, status));
                }
                Ok(OwnedChildSignal::Exited(status)) => {
                    exited.push(exited_pool_child(*epoch, status));
                }
                Err(error) => errors.push(format!("epoch {epoch}: {error:#}")),
            },
            Err(error) => errors.push(format!("epoch {epoch}: poll before cancel: {error}")),
        }
    }

    finish_natural_pool_exits(args, pool, exited, &mut errors);
    for (epoch, known_status) in cancelled {
        let mut child = pool
            .children
            .remove(&epoch)
            .expect("cancelled pool child exists");
        let status = match known_status {
            Some(status) => Ok(status),
            None => wait_for_signaled_child(&mut child),
        };
        match status {
            Ok(_) => {
                if let Err(error) = defer_child_cleanup(
                    args,
                    &child,
                    false,
                    format!("cancelled to release resources: {reason}"),
                ) {
                    errors.push(format!("epoch {epoch}: defer cancelled cleanup: {error:#}"));
                } else {
                    child.owned_temp_path = None;
                }
            }
            Err(error) => {
                errors.push(format!("epoch {epoch}: reap cancelled child: {error:#}"));
                pool.children.insert(epoch, child);
            }
        }
    }
    pool.clear_admission_window();
    pool.resume_safe_since = None;
    pool_action_result("cancel Firewatch pool", errors)
}

fn fail_children_with_invalid_authority(
    args: &Args,
    pool: &mut PoolState,
) -> Result<Option<String>> {
    let invalid = pool
        .children
        .iter()
        .filter_map(|(epoch, child)| {
            child
                .candidate
                .recheck_authority_proofs("active child poll")
                .err()
                .map(|error| (*epoch, format!("{error:#}")))
        })
        .collect::<Vec<_>>();
    if invalid.is_empty() {
        return Ok(None);
    }

    let mut errors = Vec::new();
    let mut failed_epochs = Vec::new();
    for (epoch, authority_error) in invalid {
        let outcome = {
            let child = pool.children.get_mut(&epoch).expect("invalid child exists");
            match probe_child_exit(child) {
                Ok(Some(_)) => Ok(()),
                Ok(None) => match signal_verified_child_group(child, libc::SIGKILL) {
                    Ok(OwnedChildSignal::Signaled) => wait_for_signaled_child(child).map(|_| ()),
                    Ok(OwnedChildSignal::SignaledAndExited(Some(_)))
                    | Ok(OwnedChildSignal::Exited(Some(_))) => Ok(()),
                    Ok(OwnedChildSignal::SignaledAndExited(None))
                    | Ok(OwnedChildSignal::Exited(None)) => {
                        wait_for_signaled_child(child).map(|_| ())
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            }
        };
        if let Err(error) = outcome {
            errors.push(format!(
                "epoch {epoch}: stop child after authority failure: {error:#}"
            ));
            continue;
        }
        let mut child = pool.children.remove(&epoch).expect("invalid child exists");
        let message = format!(
            "wire-profile authority changed while the Firewatch child was active: {authority_error}"
        );
        if let Err(error) = defer_child_cleanup(args, &child, true, message) {
            errors.push(format!(
                "epoch {epoch}: publish authority-failure marker: {error:#}"
            ));
            pool.children.insert(epoch, child);
            continue;
        }
        child.owned_temp_path = None;
        failed_epochs.push(epoch);
    }
    pool.clear_admission_window();
    pool.resume_safe_since = None;
    pool_action_result("fail Firewatch children with changed authority", errors)?;
    Ok(Some(format!(
        "wire-profile authority changed for active Firewatch epochs {}",
        failed_epochs
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )))
}

fn shed_newest_child(args: &Args, pool: &mut PoolState, reason: &str) -> Result<u64> {
    let epoch = pool
        .children
        .iter()
        .max_by_key(|(_, child)| child.started_at)
        .map(|(epoch, _)| *epoch)
        .context("cannot shed a child from an empty Firewatch pool")?;
    let outcome = {
        let child = pool
            .children
            .get_mut(&epoch)
            .expect("newest pool child exists");
        match probe_child_exit(child)? {
            Some(status) => ExitedPoolChild::Reaped(epoch, status),
            None => match signal_verified_child_group(child, libc::SIGKILL)? {
                OwnedChildSignal::Signaled => {
                    let mut child = pool
                        .children
                        .remove(&epoch)
                        .expect("newest pool child exists");
                    wait_for_signaled_child(&mut child).context("reap newest Firewatch child")?;
                    defer_child_cleanup(
                        args,
                        &child,
                        false,
                        format!("adaptive pool reduced to release resources: {reason}"),
                    )?;
                    child.owned_temp_path = None;
                    pool.clear_admission_window();
                    pool.resume_safe_since = None;
                    return Ok(epoch);
                }
                OwnedChildSignal::SignaledAndExited(status) => {
                    let mut child = pool
                        .children
                        .remove(&epoch)
                        .expect("newest pool child exists");
                    if status.is_none() {
                        wait_for_signaled_child(&mut child)
                            .context("reap newest Firewatch child")?;
                    }
                    defer_child_cleanup(
                        args,
                        &child,
                        false,
                        format!("adaptive pool reduced to release resources: {reason}"),
                    )?;
                    child.owned_temp_path = None;
                    pool.clear_admission_window();
                    pool.resume_safe_since = None;
                    return Ok(epoch);
                }
                OwnedChildSignal::Exited(status) => exited_pool_child(epoch, status),
            },
        }
    };
    let mut errors = Vec::new();
    finish_natural_pool_exits(args, pool, vec![outcome], &mut errors);
    pool.clear_admission_window();
    pool.resume_safe_since = None;
    pool_action_result("shed newest Firewatch child", errors)?;
    Ok(epoch)
}

#[derive(Debug)]
enum OwnedChildSignal {
    Signaled,
    /// The exact child exited after the initial poll and before the signal reached its group.
    Exited(Option<ExitStatus>),
    /// The signal reached the exact group, but its leader was ready to reap immediately after it.
    SignaledAndExited(Option<ExitStatus>),
}

#[derive(Debug)]
enum ExitedPoolChild {
    Reaped(u64, ExitStatus),
    NeedsReap(u64),
}

fn probe_child_exit(child: &mut ActiveChild) -> Result<Option<ExitStatus>> {
    child.child.try_wait().context("poll Firewatch child")
}

fn exited_pool_child(epoch: u64, status: Option<ExitStatus>) -> ExitedPoolChild {
    match status {
        Some(status) => ExitedPoolChild::Reaped(epoch, status),
        None => ExitedPoolChild::NeedsReap(epoch),
    }
}

fn signal_verified_child_group(
    child: &mut ActiveChild,
    signal: libc::c_int,
) -> Result<OwnedChildSignal> {
    if let Some(status) = child.child.try_wait()? {
        // The caller polls first so it can preserve the exit status. This second poll only closes
        // the small race before identity validation.
        return Ok(OwnedChildSignal::Exited(Some(status)));
    }
    let observed_start_ticks = process_start_ticks(child.pid);
    if observed_start_ticks != Some(child.start_ticks) {
        if let Some(status) = child.child.try_wait()? {
            return Ok(OwnedChildSignal::Exited(Some(status)));
        }
        if observed_start_ticks.is_none() {
            return Ok(OwnedChildSignal::Exited(None));
        }
        bail!("Firewatch child process identity changed");
    }
    if process_group_id(child.pid) != Some(child.pid) {
        if let Some(status) = child.child.try_wait()? {
            return Ok(OwnedChildSignal::Exited(Some(status)));
        }
        if process_start_ticks(child.pid) != Some(child.start_ticks) {
            return Ok(OwnedChildSignal::Exited(None));
        }
        bail!("Firewatch child is not its process-group leader");
    }
    // SAFETY: every controller child is spawned as its own process-group leader.
    let result = unsafe { libc::kill(-(child.pid as libc::pid_t), signal) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH)
            && process_start_ticks(child.pid) != Some(child.start_ticks)
        {
            return Ok(OwnedChildSignal::Exited(child.child.try_wait()?));
        }
        return Err(error).context("signal child process group");
    }
    if let Some(status) = child.child.try_wait()? {
        return Ok(OwnedChildSignal::SignaledAndExited(Some(status)));
    }
    Ok(OwnedChildSignal::Signaled)
}

fn wait_for_signaled_child(child: &mut ActiveChild) -> Result<ExitStatus> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.child.try_wait()? {
            return Ok(status);
        }
        ensure!(
            Instant::now() < deadline,
            "signaled Firewatch child did not exit within 10 seconds"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

fn finish_natural_pool_exits(
    args: &Args,
    pool: &mut PoolState,
    exited: Vec<ExitedPoolChild>,
    errors: &mut Vec<String>,
) {
    for exited_child in exited {
        let (epoch, known_status) = match exited_child {
            ExitedPoolChild::Reaped(epoch, status) => (epoch, Some(status)),
            ExitedPoolChild::NeedsReap(epoch) => (epoch, None),
        };
        let Some(mut child) = pool.children.remove(&epoch) else {
            continue;
        };
        let status = match known_status {
            Some(status) => Ok(status),
            None => wait_for_signaled_child(&mut child),
        };
        match status {
            Ok(status) => {
                if let Err(error) = finish_child(args, child, status.success()) {
                    errors.push(format!("epoch {epoch}: finish exited child: {error:#}"));
                }
            }
            Err(error) => {
                errors.push(format!("epoch {epoch}: reap exited child: {error:#}"));
                pool.children.insert(epoch, child);
            }
        }
    }
    if !pool.children.is_empty() {
        pool.clear_admission_window();
    }
    pool.resume_safe_since = None;
}

fn pool_action_result(action: &str, errors: Vec<String>) -> Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        bail!("{action} was incomplete: {}", errors.join("; "))
    }
}

#[derive(Debug)]
struct CandidateDiscovery {
    candidates: Vec<Candidate>,
    blocked_wire_profiles: Vec<WireProfileBlockedCandidate>,
    archive_epochs_total: u32,
    epochs_eligible: u32,
    epochs_blocked_migration: u32,
    epochs_blocked_wire_profile: u32,
}

type CandidateIdentity = (u64, CandidateMode, String, String, String, String, String);
type WireProfileAttestationKey = (u64, PathBuf, String);

#[derive(Debug)]
struct PinnedFileEvidence {
    path: PathBuf,
    file: File,
    identity: RegistryFileIdentity,
    sha256: String,
    max_bytes: u64,
}

#[derive(Debug)]
struct PinnedJson<T> {
    value: T,
    evidence: Arc<PinnedFileEvidence>,
}

#[derive(Debug)]
struct LoadedWireProfileAttestation {
    value: WireProfileAttestation,
    evidence: Arc<PinnedFileEvidence>,
    require_protected: bool,
}

#[derive(Debug, Clone)]
struct AuthorityProof {
    label: &'static str,
    evidence: Arc<PinnedFileEvidence>,
    require_protected: bool,
}

#[derive(Debug)]
struct AuthorityProofSet {
    digest: String,
    proofs: Vec<AuthorityProof>,
}

fn effective_input_digest(
    content_generation_sha256: &str,
    wire_profile: ArchiveV2WireProfile,
) -> Result<String> {
    effective_input_digest_for_index_profile(
        content_generation_sha256,
        wire_profile,
        MANIFEST_SCHEMA_VERSION,
        FORMAT_VERSION,
        SEMANTICS_VERSION,
    )
}

fn effective_input_digest_for_index_profile(
    content_generation_sha256: &str,
    wire_profile: ArchiveV2WireProfile,
    manifest_schema_version: u32,
    format_version: u32,
    semantics_version: u32,
) -> Result<String> {
    ensure!(
        is_sha256(content_generation_sha256),
        "effective input has an invalid content generation"
    );
    let profile = wire_profile.to_string();
    let mut hasher = Sha256::new();
    hasher.update(EFFECTIVE_INPUT_DOMAIN);
    hasher.update(manifest_schema_version.to_le_bytes());
    hasher.update(format_version.to_le_bytes());
    hasher.update(semantics_version.to_le_bytes());
    hasher.update((content_generation_sha256.len() as u64).to_le_bytes());
    hasher.update(content_generation_sha256.as_bytes());
    hasher.update((profile.len() as u64).to_le_bytes());
    hasher.update(profile.as_bytes());
    Ok(hex_digest(hasher.finalize()))
}

#[cfg(test)]
fn legacy_profile_bound_effective_input_digest(
    content_generation_sha256: &str,
    wire_profile: ArchiveV2WireProfile,
) -> Result<String> {
    const LEGACY_DOMAIN: &[u8] = b"blockzilla.firewatch.archive-v2-effective-input.v2\0";
    ensure!(
        is_sha256(content_generation_sha256),
        "legacy effective input has an invalid content generation"
    );
    let profile = wire_profile.to_string();
    let mut hasher = Sha256::new();
    hasher.update(LEGACY_DOMAIN);
    hasher.update((content_generation_sha256.len() as u64).to_le_bytes());
    hasher.update(content_generation_sha256.as_bytes());
    hasher.update((profile.len() as u64).to_le_bytes());
    hasher.update(profile.as_bytes());
    Ok(hex_digest(hasher.finalize()))
}

fn load_wire_profile_attestations(
    args: &Args,
) -> Result<BTreeMap<WireProfileAttestationKey, LoadedWireProfileAttestation>> {
    let root = args
        .controller_state_root
        .join(WIRE_PROFILE_ATTESTATIONS_DIR);
    let mut attestations = BTreeMap::new();
    match fs::symlink_metadata(&root) {
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir()
                && !metadata.file_type().is_symlink()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o022 == 0,
            "wire-profile attestation root is not an euid-owned protected real directory"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(attestations),
        Err(error) => return Err(error.into()),
    }
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("read wire-profile attestation root {}", root.display()));
        }
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some((name_epoch, name_generation)) =
            parse_wire_profile_attestation_name(&name.to_string_lossy())?
        else {
            continue;
        };
        let path = entry.path();
        let pinned: PinnedJson<WireProfileAttestation> =
            read_pinned_json(&path, "wire-profile attestation")?;
        let attestation = pinned.value;
        validate_wire_profile_attestation_structure(&attestation)
            .with_context(|| format!("wire-profile attestation is invalid: {}", path.display()))?;
        let receipt_source =
            attestation.generation_kind == RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND;
        if receipt_source {
            // This is the controller trust boundary for recovery evidence. Do
            // not accept a printable label as proof of a Post full-generation
            // audit, including for legacy schema-2/3 registry pairs.
            validate_receipt_source_recovery_evidence(&attestation.evidence)?;
        }
        pinned.evidence.recheck(true, "wire-profile attestation")?;
        let marker_profile = if attestation.generation_kind == DIRECT_ATTESTATION_GENERATION_KIND {
            direct_marker_profile_from_files(&attestation.archive_files)?
        } else {
            None
        };
        if let Some(marker_profile) = marker_profile {
            ensure!(
                marker_profile == attestation.wire_profile,
                "wire-profile attestation profile differs from its producer marker binding"
            );
        }
        ensure!(
            attestation.epoch == name_epoch
                && attestation.content_generation_sha256 == name_generation,
            "wire-profile attestation filename differs from its exact identity"
        );
        ensure!(
            attestation.archive.is_absolute()
                && fs::canonicalize(&attestation.archive)? == attestation.archive,
            "wire-profile attestation archive path is not canonical"
        );
        let key = (
            attestation.epoch,
            attestation.archive.clone(),
            attestation.content_generation_sha256.clone(),
        );
        ensure!(
            attestations
                .insert(
                    key,
                    LoadedWireProfileAttestation {
                        value: attestation,
                        evidence: pinned.evidence,
                        require_protected: true,
                    },
                )
                .is_none(),
            "duplicate wire-profile attestation identity"
        );
    }
    Ok(attestations)
}

fn parse_wire_profile_attestation_name(name: &str) -> Result<Option<(u64, String)>> {
    if !name.starts_with("epoch-") || !name.ends_with(".json") {
        return Ok(None);
    }
    let identity = name
        .strip_prefix("epoch-")
        .and_then(|name| name.strip_suffix(".json"))
        .context("wire-profile attestation filename is malformed")?;
    let (epoch, generation) = identity
        .rsplit_once('-')
        .context("wire-profile attestation filename has no generation")?;
    let epoch = epoch
        .parse::<u64>()
        .context("wire-profile attestation filename has an invalid epoch")?;
    ensure!(
        is_sha256(generation),
        "wire-profile attestation filename has an invalid generation"
    );
    Ok(Some((epoch, generation.to_owned())))
}

fn attested_wire_profile(
    attestations: &BTreeMap<WireProfileAttestationKey, LoadedWireProfileAttestation>,
    epoch: u64,
    archive: &Path,
    registry_order: &str,
    generation_kind: &str,
    content_generation_sha256: &str,
    archive_files: &BTreeMap<String, RegistryFileIdentity>,
) -> Result<Option<ArchiveV2WireProfile>> {
    let key = (
        epoch,
        archive.to_path_buf(),
        content_generation_sha256.to_owned(),
    );
    let Some(loaded) = attestations.get(&key) else {
        return Ok(None);
    };
    loaded.evidence.recheck(
        loaded.require_protected,
        "wire-profile attestation eligibility proof",
    )?;
    let attestation = &loaded.value;
    ensure!(
        attestation.registry_order == registry_order
            && attestation.generation_kind == generation_kind
            && &attestation.archive_files == archive_files,
        "wire-profile attestation provenance differs for epoch {epoch}"
    );
    if generation_kind == DIRECT_ATTESTATION_GENERATION_KIND {
        if let Some(marker_profile) = direct_marker_profile_from_files(archive_files)? {
            ensure!(
                marker_profile == attestation.wire_profile,
                "wire-profile attestation profile differs from the exact producer marker for epoch {epoch}"
            );
        }
    }
    Ok(Some(attestation.wire_profile))
}

fn recheck_attested_wire_profile_final_use(
    attestations: &BTreeMap<WireProfileAttestationKey, LoadedWireProfileAttestation>,
    epoch: u64,
    archive: &Path,
    content_generation_sha256: &str,
    expected_profile: Option<ArchiveV2WireProfile>,
) -> Result<()> {
    let Some(expected_profile) = expected_profile else {
        return Ok(());
    };
    let key = (
        epoch,
        archive.to_path_buf(),
        content_generation_sha256.to_owned(),
    );
    let loaded = attestations
        .get(&key)
        .context("wire-profile attestation disappeared before final use")?;
    ensure!(
        loaded.value.wire_profile == expected_profile,
        "wire-profile attestation profile changed before final use"
    );
    loaded.evidence.recheck(
        loaded.require_protected,
        "wire-profile attestation final-use proof",
    )
}

fn exact_attestation_file_evidence(
    attestations: &BTreeMap<WireProfileAttestationKey, LoadedWireProfileAttestation>,
    epoch: u64,
    archive: &Path,
    content_generation_sha256: &str,
) -> Result<Arc<PinnedFileEvidence>> {
    attestations
        .get(&(
            epoch,
            archive.to_path_buf(),
            content_generation_sha256.to_owned(),
        ))
        .map(|loaded| Arc::clone(&loaded.evidence))
        .context("exact wire-profile attestation authority is absent")
}

fn authority_binding_digest(authorities: &[(&str, &PinnedFileEvidence)]) -> Result<String> {
    ensure!(
        !authorities.is_empty(),
        "candidate has no wire-profile authority binding"
    );
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla.firewatch.profile-authority-binding.v1\0");
    hasher.update((authorities.len() as u64).to_le_bytes());
    for (label, evidence) in authorities {
        ensure!(
            is_sha256(&evidence.sha256) && evidence.path.is_absolute(),
            "candidate wire-profile authority is malformed"
        );
        let path = evidence.path.as_os_str().as_encoded_bytes();
        hasher.update((label.len() as u64).to_le_bytes());
        hasher.update(label.as_bytes());
        hasher.update((path.len() as u64).to_le_bytes());
        hasher.update(path);
        hasher.update(evidence.identity.size.to_le_bytes());
        hasher.update(evidence.identity.device.to_le_bytes());
        hasher.update(evidence.identity.inode.to_le_bytes());
        hasher.update(evidence.identity.modified_seconds.to_le_bytes());
        hasher.update(evidence.identity.modified_nanoseconds.to_le_bytes());
        hasher.update(evidence.identity.changed_seconds.to_le_bytes());
        hasher.update(evidence.identity.changed_nanoseconds.to_le_bytes());
        hasher.update(evidence.sha256.as_bytes());
    }
    Ok(hex_digest(hasher.finalize()))
}

fn build_authority_proof_set(proofs: Vec<AuthorityProof>) -> Result<Arc<AuthorityProofSet>> {
    ensure!(!proofs.is_empty(), "candidate authority proof set is empty");
    for proof in &proofs {
        proof
            .evidence
            .recheck(proof.require_protected, proof.label)?;
    }
    let bindings = proofs
        .iter()
        .map(|proof| (proof.label, proof.evidence.as_ref()))
        .collect::<Vec<_>>();
    let digest = authority_binding_digest(&bindings)?;
    Ok(Arc::new(AuthorityProofSet { digest, proofs }))
}

impl AuthorityProofSet {
    fn recheck(&self) -> Result<()> {
        for proof in &self.proofs {
            proof
                .evidence
                .recheck(proof.require_protected, proof.label)?;
        }
        let bindings = self
            .proofs
            .iter()
            .map(|proof| (proof.label, proof.evidence.as_ref()))
            .collect::<Vec<_>>();
        ensure!(
            authority_binding_digest(&bindings)? == self.digest,
            "candidate authority proof digest changed"
        );
        Ok(())
    }
}

impl Candidate {
    fn recheck_authority_proofs(&self, use_point: &str) -> Result<()> {
        ensure!(
            is_sha256(&self.authority_binding_sha256),
            "candidate authority digest is invalid at {use_point}"
        );
        let proofs = self
            .authority_proofs
            .as_ref()
            .with_context(|| format!("candidate has no live authority proofs at {use_point}"))?;
        ensure!(
            proofs.digest == self.authority_binding_sha256,
            "candidate authority digest differs from its live proofs at {use_point}"
        );
        proofs
            .recheck()
            .with_context(|| format!("recheck candidate authority at {use_point}"))
    }
}

fn discover_candidates(args: &Args, status: &SchedulerStatus) -> Result<CandidateDiscovery> {
    ensure!(
        status.inventory.complete,
        "archive scheduler inventory is incomplete"
    );
    let mut scheduler_epochs = BTreeMap::new();
    for row in &status.epochs {
        ensure!(
            scheduler_epochs.insert(row.epoch, row).is_none(),
            "scheduler status contains duplicate epoch {}",
            row.epoch
        );
    }

    let attestations = load_wire_profile_attestations(args)?;
    let marker_root = args.scheduler_state_root.join("registry_reprocess");
    let mut candidates = Vec::new();
    let mut blocked_wire_profiles = Vec::new();
    let mut pair_epochs = BTreeSet::new();
    for entry in fs::read_dir(&marker_root)
        .with_context(|| format!("read registry marker root {}", marker_root.display()))?
    {
        let entry = entry?;
        let Some(epoch) = parse_epoch_json_name(&entry.file_name().to_string_lossy()) else {
            continue;
        };
        let marker_path = entry.path();
        let marker_capture: PinnedJson<RegistryMarker> =
            read_pinned_json(&marker_path, "registry completion marker")?;
        let marker = &marker_capture.value;
        if marker.state != "complete" {
            continue;
        }
        ensure!(
            marker.epoch == epoch
                && marker.kind == "archive_v2_registry_reprocess"
                && marker.pid.is_none()
                && marker.process_start_ticks.is_none(),
            "complete registry marker for epoch {epoch} is invalid"
        );
        let expected_source = args.archive_root.join(format!("epoch-{epoch}"));
        let expected_target = args.usage_sorted_root.join(format!("epoch-{epoch}"));
        ensure!(
            marker.source == expected_source && marker.target == expected_target,
            "complete registry marker paths differ for epoch {epoch}"
        );
        let marker_contract =
            validate_complete_registry_contract(epoch, &marker, &expected_target)?;
        let scheduler_epoch = scheduler_epochs
            .get(&epoch)
            .context("complete registry epoch is absent from scheduler inventory")?;
        ensure!(
            scheduler_epoch.state == "complete"
                && scheduler_epoch.registry_order == "first_seen"
                && scheduler_epoch.output_path == PathBuf::from(format!("epoch-{epoch}")),
            "complete registry pair source is not an exact complete first-seen scheduler epoch {epoch}"
        );
        let receipt_path = expected_target.join(REGISTRY_RECEIPT_FILE);
        let receipt_capture: PinnedJson<RegistryReceipt> =
            read_pinned_json(&receipt_path, "registry completion receipt").with_context(|| {
                format!(
                    "read complete registry receipt for epoch {epoch} at {}",
                    receipt_path.display()
                )
            })?;
        let receipt = &receipt_capture.value;
        ensure!(
            receipt.epoch == epoch,
            "complete registry receipt epoch differs from marker epoch {epoch}"
        );
        match marker_contract {
            CompleteRegistryContract::Legacy => ensure!(
                matches!(
                    (receipt.version, receipt.algorithm.as_str()),
                    (
                        1,
                        "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v1"
                    ) | (
                        2,
                        "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v2"
                    ) | (3, REGISTRY_RECEIPT_V3_ALGORITHM)
                ) && Path::new(&receipt.source_dir) == expected_source
                    && Path::new(&receipt.target_dir) == expected_target
                    && is_sha256(&receipt.source_generation_sha256)
                    && is_sha256(&receipt.target_generation_sha256),
                "complete legacy registry receipt for epoch {epoch} is invalid"
            ),
            CompleteRegistryContract::ProfileBoundV4 => {
                validate_profile_bound_v4_receipt(
                    epoch,
                    &marker,
                    &receipt,
                    &expected_source,
                    &expected_target,
                )?;
            }
        }
        let source_archive_files = validate_receipt_files(&expected_source, &receipt.source_files)?;
        let target_archive_files = validate_receipt_files(&expected_target, &receipt.target_files)?;
        ensure!(
            registry_generation_digest(&receipt.source_files) == receipt.source_generation_sha256
                && registry_generation_digest(&receipt.target_files)
                    == receipt.target_generation_sha256,
            "complete registry receipt generation digest differs for epoch {epoch}"
        );
        if marker_contract == CompleteRegistryContract::ProfileBoundV4 {
            marker_capture
                .evidence
                .recheck(true, "schema-4 registry completion marker")?;
            receipt_capture
                .evidence
                .recheck(true, "schema-4 registry completion receipt")?;
            ensure!(
                receipt.source_files.contains_key(ARCHIVE_V2_POH_FILE),
                "schema-4 complete registry receipt has no current source PoH binding for epoch {epoch}"
            );
            verify_receipt_file_identities(
                &expected_source,
                &source_archive_files,
                "schema-4 receipt source",
            )?;
            verify_receipt_file_identities(
                &expected_target,
                &target_archive_files,
                "schema-4 receipt target",
            )?;
            ensure_registry_completion_paths_are_clean(&marker, &expected_target)?;
        }
        let target_post_marker_evidence =
            if marker_contract == CompleteRegistryContract::ProfileBoundV4 {
                Some(capture_schema4_target_post_marker(
                    &expected_target,
                    &receipt.target_files,
                    &target_archive_files,
                )?)
            } else {
                None
            };
        let (source_wire_profile, target_wire_profile) = match marker_contract {
            CompleteRegistryContract::Legacy => (
                attested_wire_profile(
                    &attestations,
                    epoch,
                    &expected_source,
                    "first_seen",
                    RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND,
                    &receipt.source_generation_sha256,
                    &source_archive_files,
                )?,
                attested_wire_profile(
                    &attestations,
                    epoch,
                    &expected_target,
                    "usage_sorted",
                    RECEIPT_TARGET_ATTESTATION_GENERATION_KIND,
                    &receipt.target_generation_sha256,
                    &target_archive_files,
                )?,
            ),
            CompleteRegistryContract::ProfileBoundV4 => (
                attested_wire_profile(
                    &attestations,
                    epoch,
                    &expected_source,
                    "first_seen",
                    RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND,
                    &receipt.source_generation_sha256,
                    &source_archive_files,
                )?,
                Some(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1),
            ),
        };
        if marker_contract == CompleteRegistryContract::ProfileBoundV4 {
            ensure!(
                source_wire_profile.is_none()
                    || source_wire_profile
                        == Some(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1),
                "schema-4 registry source attestation is not the exact Post profile for epoch {epoch}"
            );
        }
        recheck_attested_wire_profile_final_use(
            &attestations,
            epoch,
            &expected_source,
            &receipt.source_generation_sha256,
            source_wire_profile,
        )?;
        if marker_contract == CompleteRegistryContract::Legacy {
            recheck_attested_wire_profile_final_use(
                &attestations,
                epoch,
                &expected_target,
                &receipt.target_generation_sha256,
                target_wire_profile,
            )?;
        } else {
            marker_capture
                .evidence
                .recheck(true, "schema-4 registry completion marker final use")?;
            receipt_capture
                .evidence
                .recheck(true, "schema-4 registry completion receipt final use")?;
            target_post_marker_evidence
                .as_ref()
                .context("schema-4 target Post marker evidence is absent")?
                .recheck(true, "schema-4 target Post marker final use")?;
        }
        let mut missing_inputs = Vec::new();
        if source_wire_profile.is_none() {
            missing_inputs.push(WireProfileAuditInput {
                archive: expected_source.clone(),
                registry_order: "first_seen".into(),
                generation_kind: RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND.into(),
                content_generation_sha256: receipt.source_generation_sha256.clone(),
            });
        }
        if target_wire_profile.is_none() {
            missing_inputs.push(WireProfileAuditInput {
                archive: expected_target.clone(),
                registry_order: "usage_sorted".into(),
                generation_kind: RECEIPT_TARGET_ATTESTATION_GENERATION_KIND.into(),
                content_generation_sha256: receipt.target_generation_sha256.clone(),
            });
        }
        if !missing_inputs.is_empty() {
            blocked_wire_profiles.push(WireProfileBlockedCandidate {
                epoch,
                mode: CandidateMode::MigrationPair,
                inputs: missing_inputs,
            });
            pair_epochs.insert(epoch);
            continue;
        }
        let source_wire_profile = source_wire_profile.expect("source profile was checked");
        let target_wire_profile = target_wire_profile.expect("target profile was checked");
        let source_attestation_evidence = exact_attestation_file_evidence(
            &attestations,
            epoch,
            &expected_source,
            &receipt.source_generation_sha256,
        )?;
        let authority_proofs = match marker_contract {
            CompleteRegistryContract::Legacy => {
                let target_attestation_evidence = exact_attestation_file_evidence(
                    &attestations,
                    epoch,
                    &expected_target,
                    &receipt.target_generation_sha256,
                )?;
                build_authority_proof_set(vec![
                    AuthorityProof {
                        label: "source_attestation",
                        evidence: source_attestation_evidence,
                        require_protected: true,
                    },
                    AuthorityProof {
                        label: "target_attestation",
                        evidence: target_attestation_evidence,
                        require_protected: true,
                    },
                ])?
            }
            CompleteRegistryContract::ProfileBoundV4 => build_authority_proof_set(vec![
                AuthorityProof {
                    label: "source_attestation",
                    evidence: source_attestation_evidence,
                    require_protected: true,
                },
                AuthorityProof {
                    label: "registry_marker",
                    evidence: Arc::clone(&marker_capture.evidence),
                    require_protected: true,
                },
                AuthorityProof {
                    label: "registry_receipt",
                    evidence: Arc::clone(&receipt_capture.evidence),
                    require_protected: true,
                },
                AuthorityProof {
                    label: "target_post_marker",
                    evidence: Arc::clone(
                        target_post_marker_evidence
                            .as_ref()
                            .context("schema-4 target Post marker evidence is absent")?,
                    ),
                    require_protected: true,
                },
            ])?,
        };
        let authority_binding_sha256 = authority_proofs.digest.clone();
        candidates.push(Candidate {
            mode: CandidateMode::MigrationPair,
            epoch,
            source: expected_source,
            target: expected_target,
            source_generation: receipt.source_generation_sha256.clone(),
            target_generation: receipt.target_generation_sha256.clone(),
            source_wire_profile,
            target_wire_profile,
            source_effective_input: effective_input_digest(
                &receipt.source_generation_sha256,
                source_wire_profile,
            )?,
            target_effective_input: effective_input_digest(
                &receipt.target_generation_sha256,
                target_wire_profile,
            )?,
            authority_binding_sha256,
            authority_proofs: Some(authority_proofs),
            registry_order: "usage_sorted".into(),
            direct_files: None,
            input_bytes: receipt
                .source_files
                .get(ARCHIVE_V2_BLOCKS_FILE)
                .map(|binding| binding.bytes)
                .unwrap_or(0),
            retry_of_failed_marker_sha256: None,
        });
        ensure!(
            pair_epochs.insert(epoch),
            "registry pair epoch {epoch} was discovered twice"
        );
    }

    let archive_epochs_total = status
        .epochs
        .iter()
        .filter(|row| row.state == "complete")
        .count();
    for row in &status.epochs {
        if row.state != "complete" || pair_epochs.contains(&row.epoch) {
            continue;
        }
        if !matches!(row.registry_order.as_str(), "usage_sorted" | "first_seen") {
            continue;
        }
        ensure!(
            row.output_path == PathBuf::from(format!("epoch-{}", row.epoch)),
            "scheduler epoch {} has a non-canonical archive output basename",
            row.epoch
        );
        let archive = args.archive_root.join(format!("epoch-{}", row.epoch));
        let (mut direct_files, input_bytes) =
            capture_direct_semantic_files(&archive, &row.registry_order)?;
        let legacy_generation =
            direct_generation_digest(row.epoch, &row.registry_order, &direct_files);
        match capture_direct_wire_profile_marker(&archive) {
            Ok(marker) => bind_direct_wire_profile_marker(&mut direct_files, marker)?,
            Err(error) => {
                tracing::warn!(
                    epoch = row.epoch,
                    archive = %archive.display(),
                    error = %format!("{error:#}"),
                    "direct archive has an invalid wire-profile marker"
                );
                blocked_wire_profiles.push(WireProfileBlockedCandidate {
                    epoch: row.epoch,
                    mode: CandidateMode::CanonicalDirect,
                    inputs: vec![WireProfileAuditInput {
                        archive,
                        registry_order: row.registry_order.clone(),
                        generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
                        content_generation_sha256: legacy_generation,
                    }],
                });
                continue;
            }
        }
        let generation = direct_generation_digest(row.epoch, &row.registry_order, &direct_files);
        verify_direct_semantic_files(&archive, &direct_files)?;
        if let Err(error) = verify_direct_wire_profile_marker(&archive, &direct_files) {
            tracing::warn!(
                epoch = row.epoch,
                archive = %archive.display(),
                error = %format!("{error:#}"),
                "direct archive wire-profile marker changed during capture"
            );
            blocked_wire_profiles.push(WireProfileBlockedCandidate {
                epoch: row.epoch,
                mode: CandidateMode::CanonicalDirect,
                inputs: vec![WireProfileAuditInput {
                    archive,
                    registry_order: row.registry_order.clone(),
                    generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
                    content_generation_sha256: generation,
                }],
            });
            continue;
        }
        let wire_profile = attested_wire_profile(
            &attestations,
            row.epoch,
            &archive,
            &row.registry_order,
            DIRECT_ATTESTATION_GENERATION_KIND,
            &generation,
            &direct_files,
        )?;
        let Some(wire_profile) = wire_profile else {
            blocked_wire_profiles.push(WireProfileBlockedCandidate {
                epoch: row.epoch,
                mode: CandidateMode::CanonicalDirect,
                inputs: vec![WireProfileAuditInput {
                    archive,
                    registry_order: row.registry_order.clone(),
                    generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
                    content_generation_sha256: generation,
                }],
            });
            continue;
        };
        let effective_input = effective_input_digest(&generation, wire_profile)?;
        recheck_attested_wire_profile_final_use(
            &attestations,
            row.epoch,
            &archive,
            &generation,
            Some(wire_profile),
        )?;
        let authority_proofs = build_authority_proof_set(vec![AuthorityProof {
            label: "direct_attestation",
            evidence: exact_attestation_file_evidence(
                &attestations,
                row.epoch,
                &archive,
                &generation,
            )?,
            require_protected: true,
        }])?;
        let authority_binding_sha256 = authority_proofs.digest.clone();
        candidates.push(Candidate {
            mode: CandidateMode::CanonicalDirect,
            epoch: row.epoch,
            source: archive.clone(),
            target: archive,
            source_generation: generation.clone(),
            target_generation: generation,
            source_wire_profile: wire_profile,
            target_wire_profile: wire_profile,
            source_effective_input: effective_input.clone(),
            target_effective_input: effective_input,
            authority_binding_sha256,
            authority_proofs: Some(authority_proofs),
            registry_order: row.registry_order.clone(),
            direct_files: Some(direct_files),
            input_bytes,
            retry_of_failed_marker_sha256: None,
        });
    }
    candidates.sort_by_key(|candidate| {
        (
            candidate.mode != CandidateMode::MigrationPair,
            std::cmp::Reverse(candidate.epoch),
        )
    });
    blocked_wire_profiles.sort_by_key(|candidate| {
        (
            candidate.mode != CandidateMode::MigrationPair,
            std::cmp::Reverse(candidate.epoch),
        )
    });
    let eligible = candidates.len();
    ensure!(
        candidates
            .iter()
            .map(|candidate| candidate.epoch)
            .collect::<BTreeSet<_>>()
            .len()
            == eligible,
        "Firewatch discovered more than one candidate mode for one epoch"
    );
    let blocked_epochs = blocked_wire_profiles
        .iter()
        .map(|candidate| candidate.epoch)
        .collect::<BTreeSet<_>>();
    ensure!(
        blocked_epochs.len() == blocked_wire_profiles.len(),
        "Firewatch discovered more than one wire-profile audit row for one epoch"
    );
    ensure!(
        candidates
            .iter()
            .all(|candidate| !blocked_epochs.contains(&candidate.epoch)),
        "Firewatch epoch is both eligible and blocked on a wire-profile audit"
    );
    Ok(CandidateDiscovery {
        candidates,
        blocked_wire_profiles,
        archive_epochs_total: u32::try_from(archive_epochs_total)
            .context("archive epoch count exceeds u32")?,
        epochs_eligible: u32::try_from(eligible).context("eligible epoch count exceeds u32")?,
        epochs_blocked_wire_profile: u32::try_from(blocked_epochs.len())
            .context("wire-profile-blocked epoch count exceeds u32")?,
        // A missing profile attestation is not a registry migration block.
        epochs_blocked_migration: 0,
    })
}

fn candidate_identities(candidates: &[Candidate]) -> Vec<CandidateIdentity> {
    candidates
        .iter()
        .map(|candidate| {
            (
                candidate.epoch,
                candidate.mode,
                candidate.source_generation.clone(),
                candidate.target_generation.clone(),
                candidate.source_effective_input.clone(),
                candidate.target_effective_input.clone(),
                candidate.authority_binding_sha256.clone(),
            )
        })
        .collect()
}

fn revalidate_before_spawn(
    args: &Args,
    client: &Client,
    candidate: &Candidate,
    expected_identities: &[CandidateIdentity],
    pressure_context: PressureContext,
) -> Result<()> {
    let status = fetch_scheduler_status(client, &args.scheduler_status_url)?;
    let discovery = discover_candidates(args, &status)?;
    let candidates = discovery.candidates;
    ensure!(
        candidate_identities(&candidates) == expected_identities,
        "Firewatch candidate set changed before spawn"
    );
    ensure!(
        candidates.iter().any(|current| {
            current.epoch == candidate.epoch
                && current.mode == candidate.mode
                && current.source_generation == candidate.source_generation
                && current.target_generation == candidate.target_generation
                && current.source_effective_input == candidate.source_effective_input
                && current.target_effective_input == candidate.target_effective_input
                && current.authority_binding_sha256 == candidate.authority_binding_sha256
        }),
        "selected Firewatch candidate changed before spawn"
    );
    ensure!(
        matches!(
            pressure_state(
                &status,
                args.disk_reserve_gib * GIB,
                pressure_context,
                args.host_memory_reserve_mib * MIB,
            ),
            PressureState::Safe
        ),
        "scheduler or resource state changed before Firewatch spawn"
    );
    Ok(())
}

fn parse_epoch_json_name(name: &str) -> Option<u64> {
    name.strip_prefix("epoch-")?
        .strip_suffix(".json")?
        .parse()
        .ok()
}

fn validate_complete_registry_contract(
    epoch: u64,
    marker: &RegistryMarker,
    expected_target: &Path,
) -> Result<CompleteRegistryContract> {
    if matches!(marker.schema_version, 2 | 3) {
        // Keep the deployed legacy contract unchanged. Schema 2 and 3 did not
        // give Firewatch current attempt/profile provenance.
        ensure!(
            matches!(marker.phase.as_deref(), None | Some("access")),
            "complete legacy registry marker phase is invalid for epoch {epoch}"
        );
        return Ok(CompleteRegistryContract::Legacy);
    }
    ensure!(
        marker.schema_version == CURRENT_REGISTRY_MARKER_SCHEMA_VERSION,
        "complete registry marker schema is unsupported for epoch {epoch}"
    );
    let attempt_id = marker
        .attempt_id
        .as_deref()
        .context("schema-4 complete registry marker has no attempt ID")?;
    let handoff_sha256 = marker
        .handoff_sha256
        .as_deref()
        .context("schema-4 complete registry marker has no handoff digest")?;
    let threads = marker
        .threads
        .context("schema-4 complete registry marker has no thread count")?;
    ensure!(
        marker.phase.as_deref() == Some("access")
            && marker.wire_profile == Some(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1)
            && (1..=256).contains(&threads)
            && is_registry_attempt_id(attempt_id)
            && is_sha256(handoff_sha256)
            && marker.expected_access_state.is_none()
            && !marker.audit_retry_is_safe
            && !marker.audit_is_continuation,
        "schema-4 complete registry marker provenance is invalid for epoch {epoch}"
    );
    let target_name = expected_target
        .file_name()
        .and_then(|name| name.to_str())
        .context("schema-4 registry target has no valid basename")?;
    let expected_staging = expected_target.with_file_name(format!(
        ".{target_name}.registry-reprocess.{attempt_id}.staging"
    ));
    ensure!(
        marker.staging_dir.as_deref() == Some(expected_staging.as_path()),
        "schema-4 complete registry marker staging path is invalid for epoch {epoch}"
    );
    Ok(CompleteRegistryContract::ProfileBoundV4)
}

fn is_registry_attempt_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_profile_bound_v4_receipt(
    epoch: u64,
    marker: &RegistryMarker,
    receipt: &RegistryReceipt,
    expected_source: &Path,
    expected_target: &Path,
) -> Result<()> {
    ensure!(
        receipt.version == 3
            && receipt.algorithm == REGISTRY_RECEIPT_V3_ALGORITHM
            && receipt.epoch == epoch
            && Path::new(&receipt.source_dir) == expected_source
            && Path::new(&receipt.target_dir) == expected_target
            && is_sha256(&receipt.source_generation_sha256)
            && is_sha256(&receipt.target_generation_sha256)
            && receipt.threads == marker.threads
            && receipt.attempt_id == marker.attempt_id
            && receipt.handoff_sha256 == marker.handoff_sha256
            && receipt.wire_profile
                == Some(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1),
        "schema-4 complete registry receipt binding is invalid for epoch {epoch}"
    );
    validate_post_receipt_marker_binding(&receipt.source_files, false, "source")?;
    validate_post_receipt_marker_binding(&receipt.target_files, true, "target")?;
    Ok(())
}

fn validate_post_receipt_marker_binding(
    files: &BTreeMap<String, RegistryFileBinding>,
    required: bool,
    side: &str,
) -> Result<()> {
    let pre = wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1);
    let post = wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
    ensure!(
        !files.contains_key(&pre.name),
        "profile-bound registry receipt {side} binds the Pre marker"
    );
    let selected = files.get(&post.name);
    ensure!(
        !required || selected.is_some(),
        "profile-bound registry receipt {side} omits the Post marker"
    );
    if let Some(binding) = selected {
        ensure!(
            binding.bytes == post.size && binding.sha256 == post.sha256,
            "profile-bound registry receipt {side} has a malformed Post marker"
        );
    }
    Ok(())
}

fn capture_schema4_target_post_marker(
    target: &Path,
    receipt_files: &BTreeMap<String, RegistryFileBinding>,
    live_identities: &BTreeMap<String, RegistryFileIdentity>,
) -> Result<Arc<PinnedFileEvidence>> {
    let post = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
    let marker = wire_profile_marker(post);
    let expected_bytes = wire_profile_marker_bytes(post);
    let binding = receipt_files
        .get(&marker.name)
        .context("schema-4 target receipt has no Post marker binding")?;
    let admitted_identity = live_identities
        .get(&marker.name)
        .context("schema-4 target has no admitted Post marker identity")?;
    ensure!(
        marker.size == expected_bytes.len() as u64
            && marker.sha256 == hex_digest(Sha256::digest(expected_bytes))
            && binding.bytes == marker.size
            && binding.sha256 == marker.sha256,
        "schema-4 target Post marker definition or receipt binding is invalid"
    );
    let path = target.join(&marker.name);
    let (bytes, evidence) =
        read_pinned_file_evidence(&path, marker.size, "schema-4 target Post marker")?;
    ensure!(
        bytes == expected_bytes
            && evidence.sha256 == marker.sha256
            && evidence.identity == *admitted_identity,
        "schema-4 target Post marker content or identity differs from its exact receipt"
    );
    evidence.recheck(true, "schema-4 target Post marker")?;
    Ok(Arc::new(evidence))
}

fn ensure_registry_completion_paths_are_clean(
    marker: &RegistryMarker,
    expected_target: &Path,
) -> Result<()> {
    let staging = marker
        .staging_dir
        .as_deref()
        .context("schema-4 complete registry marker has no staging path")?;
    ensure_path_absent(staging, "completed registry staging")?;
    let mut entries = 0usize;
    for entry in fs::read_dir(expected_target)? {
        let entry = entry?;
        entries = entries
            .checked_add(1)
            .context("registry target entry count overflow")?;
        ensure!(
            entries <= 4_096,
            "schema-4 registry target has too many entries"
        );
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("schema-4 registry target has a non-UTF-8 entry"))?;
        ensure!(
            !name.ends_with(REGISTRY_ACCESS_TEMP_SUFFIX),
            "schema-4 registry target still contains an access temp: {name}"
        );
    }
    Ok(())
}

fn ensure_path_absent(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect {label} {}", path.display())),
        Ok(_) => bail!("{label} path exists: {}", path.display()),
    }
}

fn verify_receipt_file_identities(
    directory: &Path,
    expected: &BTreeMap<String, RegistryFileIdentity>,
    label: &str,
) -> Result<()> {
    for (name, identity) in expected {
        ensure!(
            capture_direct_file_identity(&directory.join(name))? == *identity,
            "{label} identity changed for {name}"
        );
    }
    Ok(())
}

fn validate_receipt_files(
    directory: &Path,
    files: &BTreeMap<String, RegistryFileBinding>,
) -> Result<BTreeMap<String, RegistryFileIdentity>> {
    ensure!(!files.is_empty(), "registry receipt file map is empty");
    let mut identities = BTreeMap::new();
    for (name, binding) in files {
        ensure!(
            !name.is_empty()
                && Path::new(name).components().count() == 1
                && name != "."
                && name != ".."
                && is_sha256(&binding.sha256),
            "registry receipt has an invalid file binding"
        );
        let identity = capture_direct_file_identity(&directory.join(name))?;
        ensure!(
            identity.size == binding.bytes,
            "registry receipt file size differs for {name}"
        );
        identities.insert(name.clone(), identity);
    }
    Ok(identities)
}

fn registry_generation_digest(files: &BTreeMap<String, RegistryFileBinding>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla.registry-reprocess.generation.v1");
    hasher.update((files.len() as u64).to_le_bytes());
    for (name, binding) in files {
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(binding.bytes.to_le_bytes());
        hasher.update(binding.sha256.as_bytes());
    }
    hex_digest(hasher.finalize())
}

#[cfg(test)]
fn capture_direct_archive(
    archive: &Path,
    epoch: u64,
    registry_order: &str,
) -> Result<(BTreeMap<String, RegistryFileIdentity>, String, u64)> {
    let (mut files, input_bytes) = capture_direct_semantic_files(archive, registry_order)?;
    let marker = capture_direct_wire_profile_marker(archive)?;
    bind_direct_wire_profile_marker(&mut files, marker)?;
    verify_direct_archive_files(archive, &files)?;
    let generation = direct_generation_digest(epoch, registry_order, &files);
    Ok((files, generation, input_bytes))
}

fn capture_direct_semantic_files(
    archive: &Path,
    registry_order: &str,
) -> Result<(BTreeMap<String, RegistryFileIdentity>, u64)> {
    ensure!(
        matches!(registry_order, "usage_sorted" | "first_seen"),
        "direct archive registry order is not indexable"
    );
    let directory = fs::symlink_metadata(archive)
        .with_context(|| format!("inspect direct archive {}", archive.display()))?;
    ensure!(
        directory.file_type().is_dir() && !directory.file_type().is_symlink(),
        "direct archive is not a real directory"
    );
    ensure!(
        fs::canonicalize(archive)? == archive,
        "direct archive path is not canonical"
    );
    let mut files = BTreeMap::new();
    for name in DIRECT_SEMANTIC_FILES {
        let identity = capture_direct_file_identity(&archive.join(name))?;
        if name != ARCHIVE_V2_SIGNATURES_FILE {
            ensure!(identity.size > 0, "direct archive input {name} is empty");
        }
        files.insert(name.to_string(), identity);
    }
    let input_bytes = files
        .get(ARCHIVE_V2_BLOCKS_FILE)
        .context("direct archive binding has no block payload")?
        .size;
    Ok((files, input_bytes))
}

fn wire_profiles() -> [ArchiveV2WireProfile; 2] {
    [
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
    ]
}

fn capture_direct_wire_profile_marker(archive: &Path) -> Result<Option<DirectWireProfileMarker>> {
    let mut selected = None;
    for profile in wire_profiles() {
        let marker = wire_profile_marker(profile);
        let path = archive.join(&marker.name);
        match fs::symlink_metadata(&path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "inspect direct archive wire-profile marker {}",
                        path.display()
                    )
                });
            }
        }
        ensure!(
            selected.is_none(),
            "direct archive contains conflicting Archive V2 wire-profile markers"
        );
        let expected = wire_profile_marker_bytes(profile);
        ensure!(
            marker.size == expected.len() as u64
                && marker.sha256 == hex_digest(Sha256::digest(expected)),
            "SDK Archive V2 wire-profile marker definition is inconsistent"
        );
        let (bytes, identity) = read_bounded_direct_marker(&path, expected.len())
            .with_context(|| format!("read direct archive wire-profile marker {}", marker.name))?;
        ensure!(
            bytes == expected && identity.size == marker.size,
            "direct archive has malformed Archive V2 wire-profile marker bytes: {}",
            marker.name
        );
        selected = Some(DirectWireProfileMarker {
            profile,
            name: marker.name,
            identity,
        });
    }
    Ok(selected)
}

fn read_bounded_direct_marker(
    path: &Path,
    max_bytes: usize,
) -> Result<(Vec<u8>, RegistryFileIdentity)> {
    let before = fs::symlink_metadata(path)
        .with_context(|| format!("inspect direct archive marker {}", path.display()))?;
    ensure!(
        before.file_type().is_file()
            && !before.file_type().is_symlink()
            && before.len() <= max_bytes as u64,
        "direct archive marker is not a bounded real regular file: {}",
        path.display()
    );
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open direct archive marker {}", path.display()))?;
    let opened = file.metadata()?;
    ensure!(
        same_file(&before, &opened) && same_version(&before, &opened),
        "direct archive marker changed before open: {}",
        path.display()
    );
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= max_bytes,
        "direct archive marker is too large: {}",
        path.display()
    );
    let after = file.metadata()?;
    let path_after = fs::symlink_metadata(path)?;
    ensure!(
        bytes.len() as u64 == after.len()
            && same_file(&opened, &after)
            && same_version(&opened, &after)
            && same_file(&after, &path_after)
            && same_version(&after, &path_after),
        "direct archive marker changed while reading: {}",
        path.display()
    );
    Ok((
        bytes,
        RegistryFileIdentity {
            size: after.len(),
            device: after.dev(),
            inode: after.ino(),
            modified_seconds: after.mtime(),
            modified_nanoseconds: after.mtime_nsec(),
            changed_seconds: after.ctime(),
            changed_nanoseconds: after.ctime_nsec(),
        },
    ))
}

fn bind_direct_wire_profile_marker(
    files: &mut BTreeMap<String, RegistryFileIdentity>,
    marker: Option<DirectWireProfileMarker>,
) -> Result<()> {
    let Some(marker) = marker else {
        return Ok(());
    };
    ensure!(
        files.insert(marker.name, marker.identity).is_none(),
        "direct archive wire-profile marker aliases a semantic input"
    );
    Ok(())
}

fn direct_marker_profile_from_files(
    files: &BTreeMap<String, RegistryFileIdentity>,
) -> Result<Option<ArchiveV2WireProfile>> {
    let mut selected = None;
    for profile in wire_profiles() {
        let marker = wire_profile_marker(profile);
        let Some(identity) = files.get(&marker.name) else {
            continue;
        };
        ensure!(
            selected.is_none(),
            "direct archive binding contains conflicting Archive V2 wire-profile markers"
        );
        ensure!(
            identity.size == marker.size,
            "direct archive binding has a malformed Archive V2 wire-profile marker size"
        );
        selected = Some(profile);
    }
    Ok(selected)
}

fn capture_direct_file_identity(path: &Path) -> Result<RegistryFileIdentity> {
    let before = fs::symlink_metadata(path)
        .with_context(|| format!("inspect direct archive input {}", path.display()))?;
    ensure!(
        before.file_type().is_file() && !before.file_type().is_symlink(),
        "direct archive input is not a real regular file: {}",
        path.display()
    );
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open direct archive input {}", path.display()))?;
    let opened = file.metadata()?;
    let after = fs::symlink_metadata(path)?;
    ensure!(
        same_file(&before, &opened)
            && same_version(&before, &opened)
            && same_file(&opened, &after)
            && same_version(&opened, &after),
        "direct archive input changed while its identity was captured: {}",
        path.display()
    );
    Ok(RegistryFileIdentity {
        size: opened.len(),
        device: opened.dev(),
        inode: opened.ino(),
        modified_seconds: opened.mtime(),
        modified_nanoseconds: opened.mtime_nsec(),
        changed_seconds: opened.ctime(),
        changed_nanoseconds: opened.ctime_nsec(),
    })
}

fn verify_direct_archive_files(
    archive: &Path,
    expected: &BTreeMap<String, RegistryFileIdentity>,
) -> Result<()> {
    verify_direct_semantic_files(archive, expected)?;
    verify_direct_wire_profile_marker(archive, expected)
}

fn verify_direct_semantic_files(
    archive: &Path,
    expected: &BTreeMap<String, RegistryFileIdentity>,
) -> Result<()> {
    let marker_profile = direct_marker_profile_from_files(expected)?;
    ensure!(
        expected.len() == DIRECT_SEMANTIC_FILES.len() + usize::from(marker_profile.is_some()),
        "direct archive binding has the wrong file count"
    );
    for name in DIRECT_SEMANTIC_FILES {
        let binding = expected
            .get(name)
            .with_context(|| format!("direct archive binding is missing {name}"))?;
        validate_archive_file_identity(&archive.join(name), binding, binding.size)?;
    }
    Ok(())
}

fn verify_direct_wire_profile_marker(
    archive: &Path,
    expected: &BTreeMap<String, RegistryFileIdentity>,
) -> Result<()> {
    let marker_profile = direct_marker_profile_from_files(expected)?;
    let current_marker = capture_direct_wire_profile_marker(archive)?;
    match (marker_profile, current_marker) {
        (None, None) => {}
        (Some(expected_profile), Some(current)) => {
            let expected_marker = wire_profile_marker(expected_profile);
            let expected_identity = expected
                .get(&expected_marker.name)
                .context("direct archive binding is missing its selected wire-profile marker")?;
            ensure!(
                current.profile == expected_profile
                    && current.name == expected_marker.name
                    && &current.identity == expected_identity,
                "direct archive wire-profile marker differs from its exact binding"
            );
        }
        _ => bail!("direct archive wire-profile marker presence changed"),
    }
    Ok(())
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

fn build_views(
    args: &Args,
    candidates: &[Candidate],
    active: &BTreeMap<u64, ActiveChild>,
    pair_acceptance_cache: &mut PairAcceptanceVerificationCache,
) -> Result<Vec<CandidateView>> {
    let current_pair_receipts = candidates
        .iter()
        .filter(|candidate| candidate.mode == CandidateMode::MigrationPair)
        .map(|candidate| candidate.acceptance_path(args))
        .collect::<BTreeSet<_>>();
    pair_acceptance_cache
        .entries
        .retain(|path, _| current_pair_receipts.contains(path));
    let mut views = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        views.push(view_candidate(
            args,
            candidate,
            active,
            pair_acceptance_cache,
        )?);
    }
    // Finish already-built control pairs first, then retain newest-first order.
    views.sort_by_key(|view| {
        let parity_ready = view.candidate.mode == CandidateMode::MigrationPair
            && view.acceptance.is_none()
            && view.target_manifest.is_some()
            && view.source_manifest.is_some()
            && view.state != "failed";
        (
            !parity_ready,
            view.candidate.mode != CandidateMode::MigrationPair,
            std::cmp::Reverse(view.candidate.epoch),
        )
    });
    Ok(views)
}

fn view_candidate(
    args: &Args,
    candidate: &Candidate,
    active: &BTreeMap<u64, ActiveChild>,
    pair_acceptance_cache: &mut PairAcceptanceVerificationCache,
) -> Result<CandidateView> {
    // Candidate discovery already requires exact wire-profile attestations.
    // Any legacy output is therefore bypassed, not treated as an audit block.
    candidate.ensure_profile_bound_paths_do_not_alias_legacy(args)?;
    if candidate.mode == CandidateMode::CanonicalDirect {
        return view_direct_candidate(args, candidate, active);
    }
    let target_manifest = exact_index_manifest(
        &candidate.target_index(args),
        &candidate.target,
        candidate.epoch,
        &candidate.target_effective_input,
        candidate.target_wire_profile,
        false,
    )?;
    let source_manifest = exact_index_manifest(
        &candidate.source_index(args),
        &candidate.source,
        candidate.epoch,
        &candidate.source_effective_input,
        candidate.source_wire_profile,
        false,
    )?;
    let acceptance = valid_acceptance(
        args,
        candidate,
        target_manifest.as_ref(),
        source_manifest.as_ref(),
        pair_acceptance_cache,
    )?
    .map(CandidateAcceptance::Pair);
    let marker = read_optional_json::<WorkflowMarker>(&candidate.workflow_path(args))?
        .filter(|marker| marker_content_matches_candidate(marker, candidate));
    let failed = marker
        .as_ref()
        .is_some_and(|marker| matches!(marker.state.as_str(), "failed" | "failed_cleanup_pending"));
    let cleanup_pending = marker
        .as_ref()
        .is_some_and(|marker| marker.state == "cleanup_pending");
    let phase = if target_manifest.is_none() {
        Phase::TargetBuild
    } else if source_manifest.is_none() {
        Phase::SourceControlBuild
    } else {
        Phase::Parity
    };
    let state = if acceptance.is_some() {
        "accepted"
    } else if failed {
        "failed"
    } else if cleanup_pending {
        "paused"
    } else {
        "queued"
    }
    .to_string();
    Ok(CandidateView {
        candidate: candidate.clone(),
        phase,
        state,
        acceptance,
        target_manifest,
        source_manifest,
    })
}

fn view_direct_candidate(
    args: &Args,
    candidate: &Candidate,
    active: &BTreeMap<u64, ActiveChild>,
) -> Result<CandidateView> {
    ensure!(
        candidate.mode == CandidateMode::CanonicalDirect,
        "direct candidate view received a pair candidate"
    );
    let has_acceptance = candidate.acceptance_path(args).try_exists()?;
    let target_manifest = exact_direct_index_manifest(candidate, args, !has_acceptance)?;
    // Status inspection is read-only. The exact child-exit path is the only receipt publisher, so
    // it cannot race a worker that has renamed its index but has not yet been reaped.
    let exact_active = active.get(&candidate.epoch).is_some_and(|child| {
        child.candidate.mode == CandidateMode::CanonicalDirect
            && child.candidate.target_generation == candidate.target_generation
            && child.candidate.target_effective_input == candidate.target_effective_input
            && child.candidate.authority_binding_sha256 == candidate.authority_binding_sha256
    });
    let mut acceptance = valid_direct_acceptance(args, candidate, target_manifest.as_ref())?;
    // Recover the narrow crash window after immutable index publication and before receipt
    // publication. Never do this while the exact worker is active: it can still be inside its
    // final-rename/exit path.
    if args.execute && !exact_active && target_manifest.is_some() && acceptance.is_none() {
        publish_direct_acceptance(
            args,
            candidate,
            target_manifest.as_ref().expect("direct manifest exists"),
        )?;
        acceptance = valid_direct_acceptance(args, candidate, target_manifest.as_ref())?;
    }
    let marker = read_optional_json::<WorkflowMarker>(&candidate.workflow_path(args))?
        .filter(|marker| marker_content_matches_candidate(marker, candidate));
    let retry_of_failed_marker_sha256 = marker
        .as_ref()
        .map(|marker| retry_ready_for_direct(args, candidate, marker, active))
        .transpose()?
        .flatten();
    let failed = marker
        .as_ref()
        .is_some_and(|marker| matches!(marker.state.as_str(), "failed" | "failed_cleanup_pending"))
        && retry_of_failed_marker_sha256.is_none();
    let cleanup_pending = marker
        .as_ref()
        .is_some_and(|marker| marker.state == "cleanup_pending");
    let state = if acceptance.is_some() {
        "accepted"
    } else if failed {
        "failed"
    } else if cleanup_pending {
        "paused"
    } else {
        "queued"
    }
    .to_string();
    let mut candidate = candidate.clone();
    candidate.retry_of_failed_marker_sha256 = retry_of_failed_marker_sha256;
    Ok(CandidateView {
        candidate,
        phase: Phase::CanonicalBuild,
        state,
        acceptance: acceptance.map(CandidateAcceptance::Direct),
        target_manifest,
        source_manifest: None,
    })
}

fn marker_content_matches_candidate(marker: &WorkflowMarker, candidate: &Candidate) -> bool {
    if marker.schema_version == WORKFLOW_SCHEMA_VERSION {
        return marker_matches_candidate(marker, candidate);
    }
    marker.schema_version == 1
        && marker.epoch == candidate.epoch
        && marker.source_generation_sha256 == candidate.source_generation
        && marker.target_generation_sha256 == candidate.target_generation
        && marker.source_effective_input_sha256.is_none()
        && marker.target_effective_input_sha256.is_none()
        && marker.source_wire_profile.is_none()
        && marker.target_wire_profile.is_none()
        && marker.authority_binding_sha256.is_none()
        && match candidate.mode {
            CandidateMode::MigrationPair => marker.phase != Phase::CanonicalBuild,
            CandidateMode::CanonicalDirect => marker.phase == Phase::CanonicalBuild,
        }
}

fn legacy_retry_ready_path(args: &Args, epoch: u64) -> PathBuf {
    args.controller_state_root
        .join(RETRY_READY_DIR)
        .join("canonical-epochs")
        .join(format!("epoch-{epoch}.json"))
}

fn retry_ready_path(args: &Args, epoch: u64, failed_marker_sha256: &str) -> PathBuf {
    debug_assert!(is_sha256(failed_marker_sha256));
    args.controller_state_root
        .join(RETRY_READY_DIR)
        .join("canonical-epochs")
        .join(format!("epoch-{epoch}-{failed_marker_sha256}.json"))
}

fn retry_ready_for_direct(
    args: &Args,
    candidate: &Candidate,
    failed_marker: &WorkflowMarker,
    active: &BTreeMap<u64, ActiveChild>,
) -> Result<Option<String>> {
    if failed_marker.state != "failed" {
        return Ok(None);
    }
    let marker_sha256 = direct_retry_precondition_hash(args, candidate, failed_marker, active)?;
    let retry: RetryReady = if failed_marker.schema_version == 1 {
        ensure_exact_direct_candidate_attestation(args, candidate)?;
        let legacy_path = legacy_retry_ready_path(args, candidate.epoch);
        let Some(retry) = read_optional_json(&legacy_path)? else {
            return Ok(None);
        };
        retry
    } else if let Some(retry) =
        read_optional_json(&retry_ready_path(args, candidate.epoch, &marker_sha256))?
    {
        retry
    } else if failed_marker.retry_of_failed_marker_sha256.is_none() {
        // Version 1 used one fixed authorization path per epoch. Read it only for an initial
        // current-schema failure so a consumed legacy authorization can never admit a later
        // attempt.
        let legacy_path = legacy_retry_ready_path(args, candidate.epoch);
        let Some(retry) = read_optional_json(&legacy_path)? else {
            return Ok(None);
        };
        retry
    } else {
        return Ok(None);
    };
    ensure!(
        candidate.mode == CandidateMode::CanonicalDirect
            && marker_content_matches_candidate(failed_marker, candidate)
            && retry.schema_version == RETRY_READY_SCHEMA_VERSION
            && retry.kind == "firewatch_retry_ready"
            && retry.mode == "canonical_direct"
            && retry.epoch == candidate.epoch
            && retry.content_generation_sha256 == candidate.target_generation
            && retry.effective_input_sha256 == candidate.target_effective_input
            && retry.wire_profile == candidate.target_wire_profile
            && retry.authorized_unix_secs > 0
            && !retry.reason.is_empty()
            && retry.reason.len() <= 1_024
            && !retry.reason.chars().any(char::is_control)
            && is_sha256(&retry.failed_marker_sha256),
        "retry-ready record differs from its exact failed candidate"
    );
    ensure!(
        retry.failed_marker_sha256 == marker_sha256,
        "retry-ready record is stale for an unrelated failed marker"
    );
    Ok(Some(marker_sha256))
}

fn direct_retry_precondition_hash(
    args: &Args,
    candidate: &Candidate,
    failed_marker: &WorkflowMarker,
    active: &BTreeMap<u64, ActiveChild>,
) -> Result<String> {
    candidate.ensure_profile_bound_paths_do_not_alias_legacy(args)?;
    let marker_path = candidate.workflow_path(args);
    let (current_marker, marker_sha256): (WorkflowMarker, String) =
        read_bounded_json_with_sha256(&marker_path)?;
    ensure!(
        current_marker == *failed_marker,
        "retry-ready workflow marker changed after it was read"
    );
    let marker_binding_matches = if current_marker.schema_version == WORKFLOW_SCHEMA_VERSION {
        marker_matches_candidate(&current_marker, candidate)
    } else {
        legacy_profile_neutral_marker_matches_candidate(&current_marker, candidate)
    };
    ensure!(
        candidate.mode == CandidateMode::CanonicalDirect
            && current_marker.state == "failed"
            && marker_binding_matches
            && current_marker.attempt_id.is_none()
            && current_marker.pid.is_none()
            && current_marker.process_start_ticks.is_none()
            && current_marker.executable.is_none()
            && current_marker.executable_dev.is_none()
            && current_marker.executable_ino.is_none()
            && current_marker.argv.is_empty()
            && current_marker.log_path.is_none()
            && !current_marker.auto_paused
            && current_marker.auto_pause_reason.is_none()
            && current_marker.owned_temp_path.is_none()
            && !current_marker.cleanup_owner_absence_confirmed
            && current_marker
                .retry_of_failed_marker_sha256
                .as_deref()
                .is_none_or(is_sha256),
        "retry-ready failed marker is not inactive and cleanup-free"
    );
    ensure!(
        !active.contains_key(&candidate.epoch),
        "retry-ready candidate still has a live worker"
    );
    ensure!(
        !candidate.acceptance_path(args).exists() && !candidate.direct_index(args).exists(),
        "retry-ready candidate already has a profile-bound index or acceptance receipt"
    );
    ensure!(
        !direct_staging_exists(args, candidate)?,
        "retry-ready candidate still has a staging workspace"
    );
    if current_marker.schema_version == 1 {
        ensure_exact_direct_candidate_attestation(args, candidate)?;
    }
    Ok(marker_sha256)
}

fn legacy_profile_neutral_marker_matches_candidate(
    marker: &WorkflowMarker,
    candidate: &Candidate,
) -> bool {
    marker.schema_version == 1
        && marker.epoch == candidate.epoch
        && marker.source_generation_sha256 == candidate.source_generation
        && marker.target_generation_sha256 == candidate.target_generation
        && marker.source_effective_input_sha256.is_none()
        && marker.target_effective_input_sha256.is_none()
        && marker.source_wire_profile.is_none()
        && marker.target_wire_profile.is_none()
        && marker.authority_binding_sha256.is_none()
        && marker.retry_of_failed_marker_sha256.is_none()
        && marker.phase == Phase::CanonicalBuild
        && candidate.mode == CandidateMode::CanonicalDirect
}

fn ensure_exact_direct_candidate_attestation(args: &Args, candidate: &Candidate) -> Result<()> {
    ensure!(
        candidate.mode == CandidateMode::CanonicalDirect
            && candidate.source == candidate.target
            && candidate.source_generation == candidate.target_generation
            && candidate.source_wire_profile == candidate.target_wire_profile
            && candidate.source_effective_input == candidate.target_effective_input
            && candidate.target_effective_input
                == effective_input_digest(
                    &candidate.target_generation,
                    candidate.target_wire_profile,
                )?,
        "legacy retry candidate is not one exact direct generation"
    );
    verify_direct_archive_files(&candidate.source, candidate.direct_files()?)?;
    let attestations = load_wire_profile_attestations(args)?;
    let profile = attested_wire_profile(
        &attestations,
        candidate.epoch,
        &candidate.source,
        &candidate.registry_order,
        DIRECT_ATTESTATION_GENERATION_KIND,
        &candidate.target_generation,
        candidate.direct_files()?,
    )?;
    ensure!(
        profile == Some(candidate.target_wire_profile),
        "legacy retry candidate has no exact wire-profile attestation"
    );
    recheck_attested_wire_profile_final_use(
        &attestations,
        candidate.epoch,
        &candidate.source,
        &candidate.target_generation,
        profile,
    )?;
    let authority_proofs = build_authority_proof_set(vec![AuthorityProof {
        label: "direct_attestation",
        evidence: exact_attestation_file_evidence(
            &attestations,
            candidate.epoch,
            &candidate.source,
            &candidate.target_generation,
        )?,
        require_protected: true,
    }])?;
    ensure!(
        authority_proofs.digest == candidate.authority_binding_sha256,
        "legacy retry candidate wire-profile authority changed"
    );
    Ok(())
}

fn authorize_direct_retry(
    args: &Args,
    client: &Client,
    epoch: u64,
    reason: &str,
) -> Result<PathBuf> {
    let scheduler = fetch_scheduler_status(client, &args.scheduler_status_url)?;
    let discovery = discover_candidates(args, &scheduler)?;
    ensure!(
        !discovery
            .blocked_wire_profiles
            .iter()
            .any(|blocked| blocked.epoch == epoch),
        "epoch {epoch} still needs a wire-profile audit"
    );
    let candidate = discovery
        .candidates
        .iter()
        .find(|candidate| {
            candidate.epoch == epoch && candidate.mode == CandidateMode::CanonicalDirect
        })
        .with_context(|| format!("epoch {epoch} is not an exact direct candidate"))?;
    let marker: WorkflowMarker = read_optional_json(&candidate.workflow_path(args))?
        .with_context(|| format!("epoch {epoch} has no failed workflow marker"))?;
    publish_direct_retry_authorization(args, candidate, &marker, reason)
}

fn publish_direct_retry_authorization(
    args: &Args,
    candidate: &Candidate,
    marker: &WorkflowMarker,
    reason: &str,
) -> Result<PathBuf> {
    ensure!(
        !reason.is_empty() && reason.len() <= 1_024 && !reason.chars().any(char::is_control),
        "retry authorization reason is invalid"
    );
    let active = BTreeMap::new();
    let failed_marker_sha256 = direct_retry_precondition_hash(args, candidate, &marker, &active)?;
    if marker.schema_version == 1 {
        ensure_exact_direct_candidate_attestation(args, candidate)?;
    }
    let retry = RetryReady {
        schema_version: RETRY_READY_SCHEMA_VERSION,
        kind: "firewatch_retry_ready".into(),
        epoch: candidate.epoch,
        mode: "canonical_direct".into(),
        content_generation_sha256: candidate.target_generation.clone(),
        effective_input_sha256: candidate.target_effective_input.clone(),
        wire_profile: candidate.target_wire_profile,
        failed_marker_sha256: failed_marker_sha256.clone(),
        authorized_unix_secs: unix_now(),
        reason: reason.into(),
    };
    if marker.schema_version == 1 {
        ensure!(
            !legacy_retry_ready_path(args, candidate.epoch).exists(),
            "this exact failed marker already has a legacy retry authorization"
        );
        let path = legacy_retry_ready_path(args, candidate.epoch);
        publish_json_no_replace(&path, &retry)?;
        ensure!(
            retry_ready_for_direct(args, candidate, &marker, &active)?
                == Some(failed_marker_sha256),
            "published legacy retry authorization did not pass its own admission check"
        );
        return Ok(path);
    }
    if marker.retry_of_failed_marker_sha256.is_none() {
        ensure!(
            !legacy_retry_ready_path(args, candidate.epoch).exists(),
            "this exact failed marker already has a legacy retry authorization"
        );
    }
    let path = retry_ready_path(args, candidate.epoch, &failed_marker_sha256);
    publish_json_no_replace(&path, &retry)?;
    ensure!(
        retry_ready_for_direct(args, candidate, &marker, &active)? == Some(failed_marker_sha256),
        "published retry authorization did not pass its own admission check"
    );
    Ok(path)
}

fn direct_staging_exists(args: &Args, candidate: &Candidate) -> Result<bool> {
    let root = candidate.epoch_root(args);
    let prefixes = [
        format!(
            ".canonical-{}-{}.staging-",
            candidate.registry_order, candidate.target_generation
        ),
        format!(
            ".canonical-{}-{}.staging-",
            candidate.registry_order, candidate.target_effective_input
        ),
    ];
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if prefixes.iter().any(|prefix| name.starts_with(prefix)) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn next_runnable<'a>(
    views: &'a [CandidateView],
    active: &BTreeMap<u64, ActiveChild>,
) -> Option<&'a CandidateView> {
    views
        .iter()
        .find(|view| view.state == "queued" && !active.contains_key(&view.candidate.epoch))
}

fn reconcile_all_workflow_markers(
    args: &Args,
    active: &BTreeMap<u64, ActiveChild>,
) -> Result<Option<String>> {
    for root_name in ["epochs", "canonical-epochs"] {
        let root = args.controller_state_root.join(root_name);
        let root_mode = workflow_root_mode(root_name)?;
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            let Some(epoch) = parse_epoch_json_name(&entry.file_name().to_string_lossy()) else {
                continue;
            };
            let marker: WorkflowMarker = read_bounded_json(&entry.path())?;
            ensure!(
                matches!(
                    marker.schema_version,
                    1 | LEGACY_PROFILE_BOUND_WORKFLOW_SCHEMA_VERSION | WORKFLOW_SCHEMA_VERSION
                ) && marker.epoch == epoch
                    && is_sha256(&marker.source_generation_sha256)
                    && is_sha256(&marker.target_generation_sha256)
                    && match marker.schema_version {
                        1 =>
                            marker.source_effective_input_sha256.is_none()
                                && marker.target_effective_input_sha256.is_none()
                                && marker.source_wire_profile.is_none()
                                && marker.target_wire_profile.is_none()
                                && marker.authority_binding_sha256.is_none(),
                        LEGACY_PROFILE_BOUND_WORKFLOW_SCHEMA_VERSION =>
                            marker
                                .source_effective_input_sha256
                                .as_deref()
                                .is_some_and(is_sha256)
                                && marker
                                    .target_effective_input_sha256
                                    .as_deref()
                                    .is_some_and(is_sha256)
                                && marker.source_wire_profile.is_some()
                                && marker.target_wire_profile.is_some()
                                && marker.authority_binding_sha256.is_none(),
                        WORKFLOW_SCHEMA_VERSION =>
                            marker
                                .source_effective_input_sha256
                                .as_deref()
                                .is_some_and(is_sha256)
                                && marker
                                    .target_effective_input_sha256
                                    .as_deref()
                                    .is_some_and(is_sha256)
                                && marker.source_wire_profile.is_some()
                                && marker.target_wire_profile.is_some()
                                && marker
                                    .authority_binding_sha256
                                    .as_deref()
                                    .is_some_and(is_sha256),
                        _ => false,
                    },
                "invalid Firewatch workflow marker for epoch {epoch}"
            );
            if let Some(child) = active
                .get(&epoch)
                .filter(|child| child.candidate.mode == root_mode)
            {
                ensure!(
                    marker_matches_candidate(&marker, &child.candidate)
                        && marker.pid == Some(child.pid)
                        && marker.process_start_ticks == Some(child.start_ticks)
                        && marker.phase == child.phase
                        && matches!(marker.state.as_str(), "running" | "paused")
                        && process_matches_marker_pid(&marker, child.pid)?,
                    "in-memory Firewatch child differs from its durable marker for epoch {epoch}"
                );
                continue;
            }
            if active.get(&epoch).is_some_and(|child| {
                other_mode_live_conflict(root_mode, child.candidate.mode, &marker.state)
            }) {
                return Ok(Some(format!(
                    "Firewatch epoch {epoch} has live workflow claims in both candidate modes"
                )));
            }
            if !matches!(marker.state.as_str(), "running" | "paused") {
                if marker.state != "starting" {
                    continue;
                }
            }
            let fresh_pidless_start = marker.state == "starting"
                && marker.pid.is_none()
                && marker.process_start_ticks.is_none()
                && unix_now().saturating_sub(marker.updated_unix_secs) < 30;
            ensure!(
                marker.attempt_id.as_deref().is_some_and(is_attempt_id)
                    && marker.executable_dev.is_some()
                    && marker.executable_ino.is_some(),
                "active Firewatch workflow marker for epoch {epoch} has no exact attempt identity"
            );
            let allowed_executable = match marker.phase {
                Phase::TargetBuild | Phase::SourceControlBuild | Phase::CanonicalBuild => {
                    &args.indexer_bin
                }
                Phase::Parity => &args.parity_bin,
            };
            ensure!(
                marker.executable.as_ref() == Some(allowed_executable),
                "Firewatch workflow marker for epoch {epoch} has an unexpected executable"
            );
            let discovered_temp = find_attempt_temp(args, &marker)?;
            if let Some(declared) = marker.owned_temp_path.as_deref() {
                validate_attempt_temp_path(args, &marker, declared)?;
                if declared.exists() {
                    ensure!(
                        discovered_temp.as_deref() == Some(declared),
                        "Firewatch epoch {epoch} temporary workspace differs from its marker"
                    );
                }
            }
            let matches = find_marker_processes(&marker)?;
            if let (Some(pid), Some(start_ticks)) = (marker.pid, marker.process_start_ticks) {
                if process_start_ticks(pid) == Some(start_ticks)
                    && !process_matches_marker_pid(&marker, pid)?
                {
                    return Ok(Some(format!(
                        "Firewatch epoch {epoch} has a live process identity that does not match its workflow"
                    )));
                }
            } else if marker.pid.is_some() || marker.process_start_ticks.is_some() {
                return Ok(Some(format!(
                    "Firewatch epoch {epoch} has an incomplete process identity"
                )));
            }
            if fresh_pidless_start && matches.is_empty() && discovered_temp.is_none() {
                return Ok(Some(format!(
                    "Firewatch epoch {epoch} is inside its bounded spawn-recovery window"
                )));
            }
            for (pid, start_ticks) in &matches {
                terminate_pid_group(*pid, *start_ticks);
                for _ in 0..50 {
                    if process_start_ticks(*pid) != Some(*start_ticks) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
                if process_start_ticks(*pid) == Some(*start_ticks) {
                    return Ok(Some(format!(
                        "Firewatch epoch {epoch} orphan worker did not stop"
                    )));
                }
            }
            if let Some(path) = discovered_temp.as_deref()
                && let Some(pid) = owned_temp_pid(path)
                && marker.process_start_ticks.map_or_else(
                    || process_start_ticks(pid).is_some(),
                    |original_ticks| process_start_ticks(pid) == Some(original_ticks),
                )
            {
                return Ok(Some(format!(
                    "Firewatch epoch {epoch} has a live temporary-workspace owner that cannot be proven as its exact attempt process"
                )));
            }
            let cleanup_path = marker.owned_temp_path.clone().or(discovered_temp.clone());
            if let Some(path) = cleanup_path.as_deref() {
                let owner_pid = marker.pid.or_else(|| owned_temp_pid(path));
                let Some(owner_pid) = owner_pid else {
                    return Ok(Some(format!(
                        "Firewatch epoch {epoch} has a temporary workspace without a bound owner"
                    )));
                };
                let owner_start_ticks = marker.process_start_ticks.or_else(|| {
                    matches
                        .iter()
                        .find_map(|(pid, ticks)| (*pid == owner_pid).then_some(*ticks))
                });
                let owner_absence_confirmed =
                    owner_start_ticks.is_none() && process_start_ticks(owner_pid).is_none();
                if owner_start_ticks.is_none() && !owner_absence_confirmed {
                    return Ok(Some(format!(
                        "Firewatch epoch {epoch} has an unproven live PID for a pidless cleanup attempt"
                    )));
                }
                let message = if matches.is_empty() {
                    "deferred cleanup for a finished or not-yet-started controller worker"
                } else {
                    "deferred cleanup for an orphaned controller worker"
                };
                mark_cleanup_pending_path(
                    &entry.path(),
                    marker,
                    path,
                    owner_pid,
                    owner_start_ticks,
                    owner_absence_confirmed,
                    false,
                    message,
                )?;
                return Ok(Some(format!(
                    "Firewatch epoch {epoch} workflow recovery deferred cleanup; waiting for stable admission"
                )));
            }
            let message = if matches.is_empty() {
                "recovered a finished or not-yet-started controller worker"
            } else {
                "recovered an orphaned controller worker"
            };
            mark_interrupted_path(&entry.path(), marker, message)?;
            return Ok(Some(format!(
                "Firewatch epoch {epoch} workflow recovery completed; waiting for stable admission"
            )));
        }
    }
    Ok(None)
}

fn workflow_root_mode(root_name: &str) -> Result<CandidateMode> {
    match root_name {
        "epochs" => Ok(CandidateMode::MigrationPair),
        "canonical-epochs" => Ok(CandidateMode::CanonicalDirect),
        _ => bail!("unknown Firewatch workflow root"),
    }
}

fn other_mode_live_conflict(
    root_mode: CandidateMode,
    active_mode: CandidateMode,
    marker_state: &str,
) -> bool {
    root_mode != active_mode && matches!(marker_state, "running" | "paused" | "starting")
}

fn find_marker_processes(marker: &WorkflowMarker) -> Result<Vec<(u32, u64)>> {
    let mut matches = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse().ok())
        else {
            continue;
        };
        if pid == std::process::id() {
            continue;
        }
        let Some(start_ticks) = process_start_ticks(pid) else {
            continue;
        };
        if process_matches_marker_pid(marker, pid)? {
            matches.push((pid, start_ticks));
        }
    }
    Ok(matches)
}

fn process_matches_marker_pid(marker: &WorkflowMarker, pid: u32) -> Result<bool> {
    let (Some(attempt_id), Some(executable_dev), Some(executable_ino)) = (
        marker.attempt_id.as_deref(),
        marker.executable_dev,
        marker.executable_ino,
    ) else {
        return Ok(false);
    };
    if !is_attempt_id(attempt_id) || process_group_id(pid) != Some(pid) {
        return Ok(false);
    }
    let proc_executable = match fs::metadata(format!("/proc/{pid}/exe")) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(false),
    };
    if !proc_executable.file_type().is_file()
        || proc_executable.dev() != executable_dev
        || proc_executable.ino() != executable_ino
    {
        return Ok(false);
    }
    let environment = match fs::read(format!("/proc/{pid}/environ")) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(false),
    };
    let expected_attempt = format!("BLOCKZILLA_FIREWATCH_ATTEMPT_ID={attempt_id}");
    if !environment
        .split(|byte| *byte == 0)
        .any(|field| field == expected_attempt.as_bytes())
    {
        return Ok(false);
    }
    let bytes = match fs::read(format!("/proc/{pid}/cmdline")) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(false),
    };
    let actual = bytes
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .collect::<Vec<_>>();
    if actual.len() != marker.argv.len() + 1 {
        return Ok(false);
    }
    Ok(actual[1..]
        .iter()
        .zip(&marker.argv)
        .all(|(actual, expected)| *actual == expected.as_bytes()))
}

fn process_group_id(pid: u32) -> Option<u32> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.rfind(')')?;
    let fields = stat
        .get(close + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    fields.get(2)?.parse().ok()
}

fn is_attempt_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn owned_temp_pid(path: &Path) -> Option<u32> {
    let name = path.file_name()?.to_str()?;
    if let Some(rest) = name.strip_prefix(".index-parity-") {
        return rest.split('-').next()?.parse().ok();
    }
    let rest = name.split_once(".staging-")?.1;
    rest.split('-').next()?.parse().ok()
}

fn attempt_temp_parent_and_prefix(
    args: &Args,
    marker: &WorkflowMarker,
) -> Result<(PathBuf, String)> {
    match marker.phase {
        Phase::TargetBuild => Ok((
            args.output_root.join(format!("epoch-{}", marker.epoch)),
            format!(
                ".target-usage-sorted-{}.staging-",
                marker
                    .target_effective_input_sha256
                    .as_deref()
                    .unwrap_or(&marker.target_generation_sha256)
            ),
        )),
        Phase::SourceControlBuild => Ok((
            args.output_root.join(format!("epoch-{}", marker.epoch)),
            format!(
                ".source-first-seen-{}.staging-",
                marker
                    .source_effective_input_sha256
                    .as_deref()
                    .unwrap_or(&marker.source_generation_sha256)
            ),
        )),
        Phase::CanonicalBuild => Ok((
            args.output_root.join(format!("epoch-{}", marker.epoch)),
            format!(
                ".canonical-{}-{}.staging-",
                direct_marker_order(marker)?,
                marker
                    .target_effective_input_sha256
                    .as_deref()
                    .unwrap_or(&marker.target_generation_sha256)
            ),
        )),
        Phase::Parity => Ok((args.parity_scratch_root.clone(), ".index-parity-".into())),
    }
}

fn direct_marker_order(marker: &WorkflowMarker) -> Result<&'static str> {
    let argv = &marker.argv;
    let out = argv
        .windows(2)
        .find_map(|pair| (pair[0] == "--out").then_some(pair[1].as_str()))
        .context("canonical marker has no output argument")?;
    if out.contains("canonical-usage_sorted-") {
        Ok("usage_sorted")
    } else if out.contains("canonical-first_seen-") {
        Ok("first_seen")
    } else {
        bail!("canonical marker has an invalid output path")
    }
}

fn validate_attempt_temp_path(args: &Args, marker: &WorkflowMarker, path: &Path) -> Result<()> {
    let attempt_id = marker
        .attempt_id
        .as_deref()
        .context("active Firewatch marker has no attempt ID")?;
    let (parent, prefix) = attempt_temp_parent_and_prefix(args, marker)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("Firewatch temporary workspace has no UTF-8 name")?;
    ensure!(
        path.parent() == Some(parent.as_path())
            && name.starts_with(&prefix)
            && name.ends_with(&format!("-{attempt_id}"))
            && owned_temp_pid(path).is_some(),
        "Firewatch temporary workspace is outside its exact attempt binding"
    );
    Ok(())
}

fn find_attempt_temp(args: &Args, marker: &WorkflowMarker) -> Result<Option<PathBuf>> {
    let attempt_id = marker
        .attempt_id
        .as_deref()
        .context("active Firewatch marker has no attempt ID")?;
    let (parent, prefix) = attempt_temp_parent_and_prefix(args, marker)?;
    let mut matches = Vec::new();
    match fs::read_dir(&parent) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with(&prefix) && name.ends_with(&format!("-{attempt_id}")) {
                    let path = entry.path();
                    validate_attempt_temp_path(args, marker, &path)?;
                    let metadata = fs::symlink_metadata(&path)?;
                    ensure!(
                        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
                        "Firewatch attempt temporary path is not a real directory"
                    );
                    matches.push(path);
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    ensure!(
        matches.len() <= 1,
        "multiple Firewatch temporary workspaces use one attempt ID"
    );
    Ok(matches.pop())
}

fn mark_interrupted_path(path: &Path, mut marker: WorkflowMarker, message: &str) -> Result<()> {
    marker.state = "interrupted".into();
    clear_inactive_marker(&mut marker, message);
    publish_json_atomic(path, &marker)
}

fn mark_failed_path(path: &Path, mut marker: WorkflowMarker, message: &str) -> Result<()> {
    marker.state = "failed".into();
    clear_inactive_marker(&mut marker, message);
    publish_json_atomic(path, &marker)
}

fn clear_inactive_marker(marker: &mut WorkflowMarker, message: &str) {
    marker.updated_unix_secs = unix_now();
    marker.attempt_id = None;
    marker.pid = None;
    marker.process_start_ticks = None;
    marker.executable = None;
    marker.executable_dev = None;
    marker.executable_ino = None;
    marker.argv.clear();
    marker.log_path = None;
    marker.auto_paused = false;
    marker.auto_pause_reason = None;
    marker.owned_temp_path = None;
    marker.cleanup_owner_absence_confirmed = false;
    marker.message = Some(message.into());
}

fn mark_cleanup_pending_path(
    marker_path: &Path,
    mut marker: WorkflowMarker,
    temporary_path: &Path,
    owner_pid: u32,
    owner_start_ticks: Option<u64>,
    owner_absence_confirmed: bool,
    terminal_failure: bool,
    message: &str,
) -> Result<()> {
    match owner_start_ticks {
        Some(start_ticks) => ensure!(
            process_start_ticks(owner_pid) != Some(start_ticks),
            "cannot defer cleanup while its owner process is live"
        ),
        None => ensure!(
            owner_absence_confirmed && process_start_ticks(owner_pid).is_none(),
            "unknown Firewatch cleanup owner was not proven absent"
        ),
    }
    validate_attempt_temp_path_from_marker(&marker, temporary_path, owner_pid)?;
    marker.state = if terminal_failure {
        "failed_cleanup_pending"
    } else {
        "cleanup_pending"
    }
    .into();
    marker.updated_unix_secs = unix_now();
    marker.pid = Some(owner_pid);
    marker.process_start_ticks = owner_start_ticks;
    marker.auto_paused = false;
    marker.auto_pause_reason = None;
    marker.owned_temp_path = Some(temporary_path.to_path_buf());
    marker.cleanup_owner_absence_confirmed = owner_absence_confirmed;
    marker.message = Some(message.into());
    publish_json_atomic(marker_path, &marker)
}

fn validate_attempt_temp_path_from_marker(
    marker: &WorkflowMarker,
    path: &Path,
    owner_pid: u32,
) -> Result<()> {
    let attempt_id = marker
        .attempt_id
        .as_deref()
        .context("cleanup marker has no attempt ID")?;
    ensure!(is_attempt_id(attempt_id), "cleanup attempt ID is invalid");
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("cleanup path has no UTF-8 file name")?;
    ensure!(
        (name.starts_with(".index-parity-") || name.contains(".staging-"))
            && name.contains(&format!("-{owner_pid}-"))
            && name.ends_with(&format!("-{attempt_id}")),
        "cleanup path is outside its exact process and attempt binding"
    );
    Ok(())
}

fn exact_index_manifest(
    index: &Path,
    archive: &Path,
    epoch: u64,
    generation: &str,
    wire_profile: ArchiveV2WireProfile,
    verify_contents: bool,
) -> Result<Option<IndexManifest>> {
    match fs::symlink_metadata(index) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("inspect {}", index.display())),
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
            "index output is not a real directory"
        ),
    }
    let manifest = if verify_contents {
        IndexManifest::verify_generation(index)
            .with_context(|| format!("verify index generation at {}", index.display()))?
    } else {
        IndexManifest::read(index)
            .with_context(|| format!("read index manifest at {}", index.display()))?
    };
    let canonical_archive = fs::canonicalize(archive)?;
    ensure!(
        manifest.schema_version == MANIFEST_SCHEMA_VERSION,
        "index schema is not version {MANIFEST_SCHEMA_VERSION}"
    );
    ensure!(
        manifest.format_version == FORMAT_VERSION,
        "index format is not version {FORMAT_VERSION}"
    );
    ensure!(manifest.complete, "index manifest is not complete");
    ensure!(
        manifest.binding_kind == GenerationBindingKind::TrustedLocalAssertedImmutable,
        "index is not trusted-local bound"
    );
    ensure!(manifest.epoch == epoch, "index epoch differs");
    ensure!(
        manifest.cluster_id == "mainnet-beta",
        "index cluster differs"
    );
    ensure!(
        Path::new(&manifest.archive_root) == canonical_archive,
        "index archive path differs"
    );
    ensure!(
        manifest.generation_id == generation,
        "index generation differs"
    );
    ensure!(
        manifest.archive_wire_profile == wire_profile,
        "index archive wire profile differs"
    );
    ensure!(
        manifest.omissions.raw_transactions == 0
            && manifest.omissions.raw_metadata == 0
            && manifest.omissions.decode_errors == 0
            && manifest.omissions.unresolved_required_pubkeys == 0,
        "index reports omissions"
    );
    validate_archive_identity(&canonical_archive, &manifest)?;
    Ok(Some(manifest))
}

fn verify_pair_index_generation(
    index: &Path,
    archive: &Path,
    epoch: u64,
    generation: &str,
    wire_profile: ArchiveV2WireProfile,
) -> Result<VerifiedIndexGeneration> {
    let initial = exact_index_manifest(index, archive, epoch, generation, wire_profile, false)?
        .context("index disappeared before generation verification")?;
    let before = capture_index_generation_identity(index, &initial)?;
    let manifest = exact_index_manifest(index, archive, epoch, generation, wire_profile, true)?
        .context("index disappeared during generation verification")?;
    let after = capture_index_generation_identity(index, &manifest)?;
    ensure!(
        before == after,
        "index generation changed while its shards were verified: {}",
        index.display()
    );
    Ok(VerifiedIndexGeneration {
        manifest,
        identity: after,
    })
}

fn capture_index_generation_identity(
    index: &Path,
    manifest: &IndexManifest,
) -> Result<IndexGenerationIdentity> {
    let first = capture_index_generation_files(index, manifest)?;
    let manifest_sha256 = sha256_control_file(&index.join("manifest.json"))?;
    let second = capture_index_generation_files(index, manifest)?;
    ensure!(
        first == second,
        "index generation changed while its identity was captured: {}",
        index.display()
    );
    Ok(IndexGenerationIdentity {
        manifest_sha256,
        files: second,
    })
}

fn capture_index_generation_files(
    index: &Path,
    manifest: &IndexManifest,
) -> Result<BTreeMap<String, RegistryFileIdentity>> {
    let mut expected = Vec::<(String, u64)>::with_capacity(
        2usize.saturating_add(manifest.shards.len().saturating_mul(2)),
    );
    expected.push(("programs.map".into(), manifest.program_map.size));
    for binding in &manifest.shards {
        let shard = format!("shard-{}", binding.shard);
        let shard_path = index.join(&shard);
        let metadata = fs::symlink_metadata(&shard_path)
            .with_context(|| format!("inspect index shard directory {}", shard_path.display()))?;
        ensure!(
            metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
            "index shard path is not a real directory: {}",
            shard_path.display()
        );
        expected.push((format!("{shard}/wallets.idx"), binding.wallets.size));
        expected.push((format!("{shard}/programs.rel"), binding.relations.size));
    }
    let mut files = BTreeMap::new();
    files.insert(
        "manifest.json".into(),
        capture_regular_file_identity(&index.join("manifest.json"), None)?,
    );
    for (relative, expected_size) in expected {
        let identity = capture_regular_file_identity(&index.join(&relative), Some(expected_size))?;
        ensure!(
            files.insert(relative, identity).is_none(),
            "index manifest contains a duplicate shard file binding"
        );
    }
    Ok(files)
}

fn capture_regular_file_identity(
    path: &Path,
    expected_size: Option<u64>,
) -> Result<RegistryFileIdentity> {
    let before = fs::symlink_metadata(path)
        .with_context(|| format!("inspect bound index file {}", path.display()))?;
    ensure!(
        before.file_type().is_file() && !before.file_type().is_symlink(),
        "bound index path is not a regular file: {}",
        path.display()
    );
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open bound index file {}", path.display()))?;
    let opened = file.metadata()?;
    let after = fs::symlink_metadata(path)?;
    ensure!(
        same_file(&before, &opened)
            && same_version(&before, &opened)
            && same_file(&opened, &after)
            && same_version(&opened, &after),
        "bound index file changed while its identity was captured: {}",
        path.display()
    );
    if let Some(expected_size) = expected_size {
        ensure!(
            opened.len() == expected_size,
            "bound index file size differs from its manifest: {}",
            path.display()
        );
    }
    Ok(RegistryFileIdentity {
        size: opened.len(),
        device: opened.dev(),
        inode: opened.ino(),
        modified_seconds: opened.mtime(),
        modified_nanoseconds: opened.mtime_nsec(),
        changed_seconds: opened.ctime(),
        changed_nanoseconds: opened.ctime_nsec(),
    })
}

fn exact_direct_index_manifest(
    candidate: &Candidate,
    args: &Args,
    verify_contents: bool,
) -> Result<Option<IndexManifest>> {
    ensure!(
        candidate.mode == CandidateMode::CanonicalDirect,
        "direct index validation received a pair candidate"
    );
    if verify_contents {
        verify_direct_archive_files(&candidate.source, candidate.direct_files()?)?;
    }
    let index = candidate.direct_index(args);
    match fs::symlink_metadata(&index) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("inspect {}", index.display())),
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
            "direct index output is not a real directory"
        ),
    }
    let manifest = if verify_contents {
        IndexManifest::verify_generation(&index)
            .with_context(|| format!("verify direct index generation at {}", index.display()))?
    } else {
        IndexManifest::read(&index)
            .with_context(|| format!("read direct index generation at {}", index.display()))?
    };
    ensure!(
        manifest.schema_version == MANIFEST_SCHEMA_VERSION
            && manifest.format_version == FORMAT_VERSION
            && manifest.complete
            && manifest.binding_kind == GenerationBindingKind::TrustedLocalAssertedImmutable
            && manifest.cluster_id == "mainnet-beta"
            && manifest.epoch == candidate.epoch
            && Path::new(&manifest.archive_root) == fs::canonicalize(&candidate.source)?
            && manifest.generation_id == candidate.target_effective_input
            && manifest.archive_wire_profile == candidate.target_wire_profile
            && manifest.omissions.raw_transactions == 0
            && manifest.omissions.raw_metadata == 0
            && manifest.omissions.decode_errors == 0
            && manifest.omissions.unresolved_required_pubkeys == 0,
        "direct index manifest differs from its exact candidate binding"
    );
    ensure!(
        candidate
            .direct_files()?
            .get(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
            == Some(&manifest.registry_file_identity)
            && candidate
                .direct_files()?
                .get(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE)
                == Some(&manifest.registry_index_file_identity),
        "direct index registry identities differ from the current candidate"
    );
    if verify_contents {
        verify_direct_archive_files(&candidate.source, candidate.direct_files()?)?;
    }
    Ok(Some(manifest))
}

fn valid_direct_acceptance(
    args: &Args,
    candidate: &Candidate,
    manifest: Option<&IndexManifest>,
) -> Result<Option<DirectAcceptanceReceipt>> {
    let Some(manifest) = manifest else {
        return Ok(None);
    };
    candidate.recheck_authority_proofs("direct acceptance validation")?;
    let Some(receipt) =
        read_optional_json::<DirectAcceptanceReceipt>(&candidate.acceptance_path(args))?
    else {
        return Ok(None);
    };
    ensure!(
        receipt.schema_version == DIRECT_ACCEPTANCE_SCHEMA_VERSION
            && receipt.mode == "canonical_direct"
            && receipt.epoch == candidate.epoch
            && receipt.registry_order == candidate.registry_order
            && receipt.content_generation_sha256 == candidate.target_generation
            && receipt.effective_input_sha256 == candidate.target_effective_input
            && receipt.wire_profile == candidate.target_wire_profile
            && receipt.authority_binding_sha256 == candidate.authority_binding_sha256
            && receipt.archive == candidate.source
            && receipt.archive_files == *candidate.direct_files()?
            && receipt.input_bytes == candidate.input_bytes
            && receipt.index == candidate.direct_index(args)
            && receipt.index_manifest_sha256
                == sha256_file(&candidate.direct_index(args).join("manifest.json"))?
            && receipt.wallets == manifest.wallet_count
            && receipt.programs == manifest.program_count
            && receipt.transactions_scanned == manifest.transactions_scanned
            && receipt.blocks_scanned == manifest.blocks_scanned
            && receipt.failed_transactions_excluded == manifest.failed_transactions_excluded,
        "direct acceptance receipt differs from its exact candidate or index"
    );
    candidate.recheck_authority_proofs("direct acceptance validation final use")?;
    Ok(Some(receipt))
}

fn publish_direct_acceptance(
    args: &Args,
    candidate: &Candidate,
    manifest: &IndexManifest,
) -> Result<()> {
    publish_direct_acceptance_with_pre_publish(args, candidate, manifest, || Ok(()))
}

fn publish_direct_acceptance_with_pre_publish(
    args: &Args,
    candidate: &Candidate,
    manifest: &IndexManifest,
    pre_publish: impl FnOnce() -> Result<()>,
) -> Result<()> {
    candidate.recheck_authority_proofs("direct acceptance publication start")?;
    let verified = exact_direct_index_manifest(candidate, args, true)?
        .context("direct index disappeared before acceptance")?;
    ensure!(
        verified.generation_id == manifest.generation_id
            && verified.wallet_count == manifest.wallet_count
            && verified.program_count == manifest.program_count
            && verified.transactions_scanned == manifest.transactions_scanned
            && verified.blocks_scanned == manifest.blocks_scanned
            && verified.failed_transactions_excluded == manifest.failed_transactions_excluded,
        "direct index changed before acceptance"
    );
    let receipt = DirectAcceptanceReceipt {
        schema_version: DIRECT_ACCEPTANCE_SCHEMA_VERSION,
        mode: "canonical_direct".into(),
        epoch: candidate.epoch,
        registry_order: candidate.registry_order.clone(),
        content_generation_sha256: candidate.target_generation.clone(),
        effective_input_sha256: candidate.target_effective_input.clone(),
        wire_profile: candidate.target_wire_profile,
        authority_binding_sha256: candidate.authority_binding_sha256.clone(),
        archive: candidate.source.clone(),
        archive_files: candidate.direct_files()?.clone(),
        input_bytes: candidate.input_bytes,
        index: candidate.direct_index(args),
        index_manifest_sha256: sha256_file(&candidate.direct_index(args).join("manifest.json"))?,
        wallets: verified.wallet_count,
        programs: verified.program_count,
        transactions_scanned: verified.transactions_scanned,
        blocks_scanned: verified.blocks_scanned,
        failed_transactions_excluded: verified.failed_transactions_excluded,
        accepted_unix_secs: unix_now(),
    };
    pre_publish()?;
    let path = candidate.acceptance_path(args);
    candidate.recheck_authority_proofs("direct acceptance final publication")?;
    if path.exists() {
        let existing: DirectAcceptanceReceipt = read_bounded_json(&path)?;
        ensure!(
            direct_receipts_same_immutable_result(&existing, &receipt),
            "an existing direct acceptance receipt differs"
        );
        return Ok(());
    }
    publish_json_no_replace(&path, &receipt)
}

fn direct_receipts_same_immutable_result(
    left: &DirectAcceptanceReceipt,
    right: &DirectAcceptanceReceipt,
) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.accepted_unix_secs = 0;
    right.accepted_unix_secs = 0;
    left == right
}

fn validate_archive_identity(archive: &Path, manifest: &IndexManifest) -> Result<()> {
    validate_archive_file_identity(
        &archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
        &manifest.registry_file_identity,
        manifest.registry.size,
    )?;
    validate_archive_file_identity(
        &archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE),
        &manifest.registry_index_file_identity,
        manifest.registry_index.size,
    )
}

fn validate_archive_file_identity(
    path: &Path,
    expected: &RegistryFileIdentity,
    expected_size: u64,
) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open bound archive file {}", path.display()))?;
    let metadata = file.metadata()?;
    ensure!(metadata.is_file(), "archive binding is not a regular file");
    let actual = RegistryFileIdentity {
        size: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    };
    ensure!(
        actual == *expected && actual.size == expected_size,
        "archive file identity differs from the Firewatch index manifest: {}",
        path.display()
    );
    let path_metadata = fs::symlink_metadata(path)?;
    ensure!(
        same_file(&metadata, &path_metadata) && same_version(&metadata, &path_metadata),
        "archive binding path changed while it was checked: {}",
        path.display()
    );
    Ok(())
}

fn valid_acceptance(
    args: &Args,
    candidate: &Candidate,
    target: Option<&IndexManifest>,
    source: Option<&IndexManifest>,
    cache: &mut PairAcceptanceVerificationCache,
) -> Result<Option<AcceptanceReceipt>> {
    let (Some(target), Some(source)) = (target, source) else {
        return Ok(None);
    };
    candidate.recheck_authority_proofs("pair acceptance validation")?;
    let Some(receipt) = read_optional_json::<AcceptanceReceipt>(&candidate.acceptance_path(args))?
    else {
        return Ok(None);
    };
    ensure!(
        receipt.schema_version == ACCEPTANCE_SCHEMA_VERSION,
        "acceptance schema differs"
    );
    ensure!(receipt.epoch == candidate.epoch, "acceptance epoch differs");
    ensure!(
        receipt.source_generation_sha256 == candidate.source_generation
            && receipt.target_generation_sha256 == candidate.target_generation,
        "acceptance generation differs"
    );
    ensure!(
        receipt.source_effective_input_sha256 == candidate.source_effective_input
            && receipt.target_effective_input_sha256 == candidate.target_effective_input
            && receipt.source_wire_profile == candidate.source_wire_profile
            && receipt.target_wire_profile == candidate.target_wire_profile
            && receipt.authority_binding_sha256 == candidate.authority_binding_sha256,
        "acceptance effective input differs"
    );
    ensure!(
        receipt.source_index == candidate.source_index(args),
        "acceptance source path differs"
    );
    ensure!(
        receipt.target_index == candidate.target_index(args),
        "acceptance target path differs"
    );

    let cache_key = candidate.acceptance_path(args);
    let current_source = capture_index_generation_identity(&candidate.source_index(args), source)?;
    let current_target = capture_index_generation_identity(&candidate.target_index(args), target)?;
    let cached = cache
        .entries
        .get(&cache_key)
        .is_some_and(|cached| cached.source == current_source && cached.target == current_target);
    let (verified_source, verified_target, verified_identity) = if cached {
        (
            source.clone(),
            target.clone(),
            PairAcceptanceVerification {
                source: current_source,
                target: current_target,
            },
        )
    } else {
        let verified_source = verify_pair_index_generation(
            &candidate.source_index(args),
            &candidate.source,
            candidate.epoch,
            &candidate.source_effective_input,
            candidate.source_wire_profile,
        )?;
        let verified_target = verify_pair_index_generation(
            &candidate.target_index(args),
            &candidate.target,
            candidate.epoch,
            &candidate.target_effective_input,
            candidate.target_wire_profile,
        )?;
        let identity = PairAcceptanceVerification {
            source: verified_source.identity,
            target: verified_target.identity,
        };
        (verified_source.manifest, verified_target.manifest, identity)
    };
    ensure!(
        receipt.source_manifest_sha256 == verified_identity.source.manifest_sha256
            && receipt.target_manifest_sha256 == verified_identity.target.manifest_sha256,
        "accepted manifest binding differs"
    );
    ensure!(
        is_sha256(&receipt.canonical_sha256),
        "accepted canonical hash is invalid"
    );
    ensure!(
        verified_source.wallet_count == verified_target.wallet_count
            && verified_source.wallet_count == receipt.wallets,
        "accepted wallet counts differ"
    );
    ensure!(
        verified_source.program_count == verified_target.program_count,
        "accepted program counts differ"
    );
    ensure!(
        verified_source.transactions_scanned == verified_target.transactions_scanned
            && verified_source.blocks_scanned == verified_target.blocks_scanned
            && verified_source.failed_transactions_excluded
                == verified_target.failed_transactions_excluded,
        "accepted scan totals differ"
    );
    let final_source =
        capture_index_generation_identity(&candidate.source_index(args), &verified_source)?;
    let final_target =
        capture_index_generation_identity(&candidate.target_index(args), &verified_target)?;
    ensure!(
        final_source == verified_identity.source && final_target == verified_identity.target,
        "accepted index generation changed during validation"
    );
    candidate.recheck_authority_proofs("pair acceptance validation final use")?;
    cache.entries.insert(cache_key, verified_identity);
    Ok(Some(receipt))
}

fn marker_matches_candidate(marker: &WorkflowMarker, candidate: &Candidate) -> bool {
    marker.schema_version == WORKFLOW_SCHEMA_VERSION
        && marker.epoch == candidate.epoch
        && marker.source_generation_sha256 == candidate.source_generation
        && marker.target_generation_sha256 == candidate.target_generation
        && marker.source_effective_input_sha256.as_deref()
            == Some(candidate.source_effective_input.as_str())
        && marker.target_effective_input_sha256.as_deref()
            == Some(candidate.target_effective_input.as_str())
        && marker.source_wire_profile == Some(candidate.source_wire_profile)
        && marker.target_wire_profile == Some(candidate.target_wire_profile)
        && marker.authority_binding_sha256.as_deref()
            == Some(candidate.authority_binding_sha256.as_str())
        && match candidate.mode {
            CandidateMode::MigrationPair => marker.phase != Phase::CanonicalBuild,
            CandidateMode::CanonicalDirect => marker.phase == Phase::CanonicalBuild,
        }
}

fn spawn_phase(
    args: &Args,
    candidate: &Candidate,
    phase: Phase,
    worker_threads: usize,
) -> Result<ActiveChild> {
    spawn_phase_with_pre_spawn(args, candidate, phase, worker_threads, || Ok(()))
}

fn spawn_phase_with_pre_spawn(
    args: &Args,
    candidate: &Candidate,
    phase: Phase,
    worker_threads: usize,
    pre_spawn: impl FnOnce() -> Result<()>,
) -> Result<ActiveChild> {
    candidate.recheck_authority_proofs("spawn admission")?;
    candidate.ensure_profile_bound_paths_do_not_alias_legacy(args)?;
    let worker_threads =
        u32::try_from(worker_threads).context("Firewatch thread count is too large")?;
    let epoch_root = candidate.epoch_root(args);
    match fs::symlink_metadata(&epoch_root) {
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
            "Firewatch epoch output root is not a real directory"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(&epoch_root)?;
        }
        Err(error) => return Err(error.into()),
    }
    ensure!(
        fs::canonicalize(&epoch_root)?
            .parent()
            .is_some_and(|parent| parent == args.output_root),
        "Firewatch epoch output root escaped its configured parent"
    );
    let now = unix_now();
    let log_path = args.controller_state_root.join("logs").join(format!(
        "epoch-{}-{}-{now}.log",
        candidate.epoch,
        phase.as_status()
    ));
    let log = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&log_path)?;
    let stderr = log.try_clone()?;
    let (executable, argv) = phase_command(args, candidate, phase, worker_threads as usize);
    let executable_metadata = fs::metadata(&executable)
        .with_context(|| format!("inspect Firewatch executable {}", executable.display()))?;
    ensure!(
        executable_metadata.file_type().is_file(),
        "Firewatch executable is not a regular file"
    );
    let attempt_id = new_attempt_id()?;
    candidate.recheck_authority_proofs("workflow marker setup")?;
    let marker = WorkflowMarker {
        schema_version: WORKFLOW_SCHEMA_VERSION,
        epoch: candidate.epoch,
        source_generation_sha256: candidate.source_generation.clone(),
        target_generation_sha256: candidate.target_generation.clone(),
        source_effective_input_sha256: Some(candidate.source_effective_input.clone()),
        target_effective_input_sha256: Some(candidate.target_effective_input.clone()),
        source_wire_profile: Some(candidate.source_wire_profile),
        target_wire_profile: Some(candidate.target_wire_profile),
        authority_binding_sha256: Some(candidate.authority_binding_sha256.clone()),
        retry_of_failed_marker_sha256: candidate.retry_of_failed_marker_sha256.clone(),
        state: "starting".into(),
        phase,
        created_unix_secs: now,
        updated_unix_secs: now,
        attempt_id: Some(attempt_id.clone()),
        pid: None,
        process_start_ticks: None,
        executable: Some(executable.clone()),
        executable_dev: Some(executable_metadata.dev()),
        executable_ino: Some(executable_metadata.ino()),
        argv: argv.clone(),
        log_path: Some(log_path.clone()),
        auto_paused: false,
        auto_pause_reason: None,
        owned_temp_path: None,
        cleanup_owner_absence_confirmed: false,
        message: None,
    };
    publish_json_atomic(&candidate.workflow_path(args), &marker)?;
    pre_spawn()?;
    candidate.recheck_authority_proofs("immediately before child spawn")?;
    ensure!(
        find_marker_processes(&marker)?.is_empty(),
        "an exact Firewatch worker already exists for epoch {}",
        candidate.epoch
    );

    let mut command = Command::new(&executable);
    command
        .args(&argv)
        .env("BLOCKZILLA_FIREWATCH_ATTEMPT_ID", &attempt_id)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));
    command.process_group(0);
    let started_at = Instant::now();
    let mut child = command
        .spawn()
        .with_context(|| format!("spawn {} for epoch {}", phase.as_status(), candidate.epoch))?;
    let pid = child.id();
    let start_ticks = match wait_for_spawned_identity(&marker, &mut child, pid) {
        Ok(ticks) => ticks,
        Err(error) => {
            kill_pid_group(pid);
            let _ = child.wait();
            return Err(error).context("bind spawned Firewatch process identity");
        }
    };
    let starting_with_process = WorkflowMarker {
        pid: Some(pid),
        process_start_ticks: Some(start_ticks),
        updated_unix_secs: unix_now(),
        ..marker
    };
    if let Err(error) = candidate.recheck_authority_proofs("spawned child identity publication") {
        terminate_pid_group(pid, start_ticks);
        let _ = child.wait();
        return Err(error);
    }
    if !process_matches_marker_pid(&starting_with_process, pid)? {
        terminate_pid_group(pid, start_ticks);
        let _ = child.wait();
        bail!("spawned Firewatch process identity changed before publication")
    }
    if let Err(error) = publish_json_atomic(&candidate.workflow_path(args), &starting_with_process)
    {
        terminate_pid_group(pid, start_ticks);
        let _ = child.wait();
        return Err(error).context("publish starting Firewatch process identity");
    }
    let owned_temp_path =
        match wait_for_owned_temp(args, candidate, phase, pid, &attempt_id, &mut child) {
            Ok(path) => path,
            Err(error) => {
                terminate_pid_group(pid, start_ticks);
                let _ = child.wait();
                return Err(error).context("bind Firewatch temporary workspace");
            }
        };
    let running = WorkflowMarker {
        state: "running".into(),
        updated_unix_secs: unix_now(),
        owned_temp_path: owned_temp_path.clone(),
        ..starting_with_process
    };
    if let Err(error) = candidate.recheck_authority_proofs("running marker publication") {
        terminate_pid_group(pid, start_ticks);
        let _ = child.wait();
        return Err(error);
    }
    if let Err(error) = publish_json_atomic(&candidate.workflow_path(args), &running) {
        terminate_pid_group(pid, start_ticks);
        let _ = child.wait();
        if let Some(path) = owned_temp_path.as_deref() {
            let _ = mark_cleanup_pending_path(
                &candidate.workflow_path(args),
                running,
                path,
                pid,
                Some(start_ticks),
                false,
                false,
                "deferred cleanup after running-marker publication failed",
            );
        }
        return Err(error).context("publish spawned Firewatch process identity");
    }
    Ok(ActiveChild {
        candidate: candidate.clone(),
        phase,
        child,
        pid,
        start_ticks,
        log_path,
        paused: false,
        pause_reason: None,
        owned_temp_path,
        io_sample: None,
        telemetry: ProcessTelemetry::default(),
        worker_threads,
        started_unix_secs: now,
        started_at,
        paused_started_at: None,
        paused_secs: Duration::ZERO,
    })
}

fn wait_for_spawned_identity(marker: &WorkflowMarker, child: &mut Child, pid: u32) -> Result<u64> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut observed_start_ticks = None;
    loop {
        if let Some(status) = child.try_wait()? {
            bail!("spawned Firewatch process exited before identity binding: {status}");
        }
        if let Some(start_ticks) = process_start_ticks(pid) {
            if let Some(observed) = observed_start_ticks {
                ensure!(
                    observed == start_ticks,
                    "spawned Firewatch PID changed during identity binding"
                );
            } else {
                observed_start_ticks = Some(start_ticks);
            }
            if process_matches_marker_pid(marker, pid)? {
                return Ok(start_ticks);
            }
        }
        ensure!(
            Instant::now() < deadline,
            "spawned Firewatch process did not expose its exact identity within 3 seconds"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn phase_command(
    args: &Args,
    candidate: &Candidate,
    phase: Phase,
    worker_threads: usize,
) -> (PathBuf, Vec<String>) {
    match phase {
        Phase::TargetBuild | Phase::SourceControlBuild => {
            let (archive, out, generation, wire_profile) = if phase == Phase::TargetBuild {
                (
                    &candidate.target,
                    candidate.target_index(args),
                    &candidate.target_effective_input,
                    candidate.target_wire_profile,
                )
            } else {
                (
                    &candidate.source,
                    candidate.source_index(args),
                    &candidate.source_effective_input,
                    candidate.source_wire_profile,
                )
            };
            (
                args.indexer_bin.clone(),
                vec![
                    "build-dense".into(),
                    "--epoch".into(),
                    candidate.epoch.to_string(),
                    "--archive".into(),
                    archive.display().to_string(),
                    "--out".into(),
                    out.display().to_string(),
                    "--trust-local".into(),
                    "--cluster-id".into(),
                    "mainnet-beta".into(),
                    "--generation-id".into(),
                    generation.clone(),
                    "--wire-profile".into(),
                    wire_profile.to_string(),
                    "--threads".into(),
                    worker_threads.to_string(),
                ],
            )
        }
        Phase::Parity => (
            args.parity_bin.clone(),
            vec![
                "--left-registry".into(),
                candidate.source.join("registry.bin").display().to_string(),
                "--right-registry".into(),
                candidate.target.join("registry.bin").display().to_string(),
                "--sort-memory-mib".into(),
                args.parity_sort_memory_mib.to_string(),
                "--temp-dir".into(),
                args.parity_scratch_root.display().to_string(),
                candidate.source_index(args).display().to_string(),
                candidate.target_index(args).display().to_string(),
            ],
        ),
        Phase::CanonicalBuild => (
            args.indexer_bin.clone(),
            vec![
                "build-dense".into(),
                "--epoch".into(),
                candidate.epoch.to_string(),
                "--archive".into(),
                candidate.source.display().to_string(),
                "--out".into(),
                candidate.direct_index(args).display().to_string(),
                "--trust-local".into(),
                "--cluster-id".into(),
                "mainnet-beta".into(),
                "--generation-id".into(),
                candidate.target_effective_input.clone(),
                "--wire-profile".into(),
                candidate.target_wire_profile.to_string(),
                "--threads".into(),
                worker_threads.to_string(),
            ],
        ),
    }
}

fn new_attempt_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(hex_digest(bytes))
}

fn wait_for_owned_temp(
    args: &Args,
    candidate: &Candidate,
    phase: Phase,
    pid: u32,
    attempt_id: &str,
    child: &mut Child,
) -> Result<Option<PathBuf>> {
    let (parent, prefix) = match phase {
        Phase::TargetBuild | Phase::SourceControlBuild | Phase::CanonicalBuild => {
            let final_path = match phase {
                Phase::TargetBuild => candidate.target_index(args),
                Phase::SourceControlBuild => candidate.source_index(args),
                Phase::CanonicalBuild => candidate.direct_index(args),
                Phase::Parity => unreachable!(),
            };
            let name = final_path
                .file_name()
                .context("Firewatch output has no file name")?
                .to_string_lossy();
            (
                final_path
                    .parent()
                    .context("Firewatch output has no parent")?
                    .to_path_buf(),
                format!(".{name}.staging-{pid}-"),
            )
        }
        Phase::Parity => (
            args.parity_scratch_root.clone(),
            format!(".index-parity-{pid}-"),
        ),
    };
    for _ in 0..100 {
        let mut matches = Vec::new();
        for entry in fs::read_dir(&parent)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && name.ends_with(&format!("-{attempt_id}")) {
                let metadata = fs::symlink_metadata(entry.path())?;
                ensure!(
                    metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
                    "owned temporary path is not a real directory"
                );
                matches.push(entry.path());
            }
        }
        ensure!(
            matches.len() <= 1,
            "multiple temporary workspaces match one Firewatch attempt"
        );
        if let Some(path) = matches.pop() {
            return Ok(Some(path));
        }
        if child.try_wait()?.is_some() {
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(50));
    }
    bail!("Firewatch child did not create its temporary workspace")
}

fn cleanup_owned_temp_step(
    args: &Args,
    path: &Path,
    marker: &WorkflowMarker,
    pid: u32,
) -> Result<bool> {
    validate_attempt_temp_path(args, marker, path)?;
    validate_attempt_temp_path_from_marker(marker, path, pid)?;
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error.into()),
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
            "owned temporary path changed type"
        ),
    }
    remove_one_tree_entry(path, 0)?;
    let finished = !path.exists();
    if finished {
        File::open(
            path.parent()
                .context("owned temporary path has no parent")?,
        )?
        .sync_all()?;
    }
    Ok(finished)
}

fn remove_one_tree_entry(path: &Path, depth: usize) -> Result<()> {
    ensure!(depth <= 64, "temporary workspace nesting is too deep");
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        let mut entries = fs::read_dir(path)?;
        if let Some(entry) = entries.next() {
            let entry = entry?;
            drop(entries);
            remove_one_tree_entry(&entry.path(), depth + 1)?;
        } else {
            drop(entries);
            fs::remove_dir(path)?;
        }
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn drain_one_deferred_cleanup(args: &Args) -> Result<Option<String>> {
    for root_name in ["epochs", "canonical-epochs"] {
        let root = args.controller_state_root.join(root_name);
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            let Some(epoch) = parse_epoch_json_name(&entry.file_name().to_string_lossy()) else {
                continue;
            };
            let marker: WorkflowMarker = read_bounded_json(&entry.path())?;
            if !matches!(
                marker.state.as_str(),
                "cleanup_pending" | "failed_cleanup_pending"
            ) {
                continue;
            }
            let terminal_failure = marker.state == "failed_cleanup_pending";
            ensure!(
                matches!(
                    marker.schema_version,
                    1 | LEGACY_PROFILE_BOUND_WORKFLOW_SCHEMA_VERSION | WORKFLOW_SCHEMA_VERSION
                ) && marker.epoch == epoch,
                "invalid deferred cleanup marker for epoch {epoch}"
            );
            let path = marker
                .owned_temp_path
                .as_deref()
                .context("deferred cleanup marker has no temporary path")?;
            let pid = marker
                .pid
                .context("deferred cleanup marker has no owner PID")?;
            ensure!(
                owned_temp_pid(path) == Some(pid),
                "deferred cleanup path PID differs from its marker"
            );
            if let Some(start_ticks) = marker.process_start_ticks {
                ensure!(
                    process_start_ticks(pid) != Some(start_ticks),
                    "deferred cleanup owner process is live"
                );
            } else {
                ensure!(
                    marker.cleanup_owner_absence_confirmed,
                    "pidless deferred cleanup owner was not proven absent"
                );
                ensure!(
                    find_marker_processes(&marker)?.is_empty(),
                    "an exact pidless Firewatch attempt process appeared during cleanup"
                );
            }
            if cleanup_owned_temp_step(args, path, &marker, pid)? {
                if terminal_failure {
                    mark_failed_path(
                        &entry.path(),
                        marker,
                        "failed Firewatch workspace cleanup is complete",
                    )?;
                } else {
                    mark_interrupted_path(
                        &entry.path(),
                        marker,
                        "stopped Firewatch workspace cleanup is complete",
                    )?;
                }
            }
            return Ok(Some(format!(
                "cleaning a stopped Firewatch workspace for epoch {epoch} during a safe idle window"
            )));
        }
    }
    Ok(None)
}

/// Finish one child and report whether its verified phase also reached a cleanup-free terminal
/// marker. `false` is a successful phase with deferred cleanup and must use the cold window.
fn finish_child(args: &Args, mut child: ActiveChild, success: bool) -> Result<bool> {
    ensure!(
        process_start_ticks(child.pid) != Some(child.start_ticks),
        "child exited but its PID identity is still live"
    );
    let result = if !success {
        Err(anyhow::anyhow!(
            "{} child exited unsuccessfully; see {}",
            child.phase.as_status(),
            child.log_path.display()
        ))
    } else {
        finish_successful_child(args, &mut child)
    };
    let clean_completion = match result {
        Ok(clean_completion) => clean_completion,
        Err(error) => {
            let message = format!(
                "{} child completion failed: {error:#}",
                child.phase.as_status()
            );
            if let Err(cleanup_error) = defer_child_cleanup(args, &child, true, message) {
                return Err(error).context(format!(
                    "durable Firewatch cleanup deferral also failed: {cleanup_error:#}"
                ));
            }
            child.owned_temp_path = None;
            return Err(error);
        }
    };
    if let Err(error) = record_successful_eta_sample(args, &child) {
        tracing::warn!(
            epoch = child.candidate.epoch,
            phase = child.phase.as_status(),
            %error,
            "Firewatch phase completed, but its ETA sample could not be saved"
        );
    }
    Ok(clean_completion)
}

fn eta_history_path(args: &Args) -> PathBuf {
    args.controller_state_root.join(ETA_HISTORY_FILE)
}

fn eta_seed_sample(
    epoch: u64,
    phase: EtaPhase,
    input_bytes: u64,
    wall_secs: f64,
) -> CompletedPhaseSample {
    let started_unix_secs = 1_700_000_000 + epoch;
    CompletedPhaseSample {
        epoch,
        phase,
        worker_threads: 4,
        input_bytes,
        started_unix_secs,
        completed_unix_secs: started_unix_secs + wall_secs.ceil() as u64,
        wall_secs,
        paused_secs: 0.0,
    }
}

/// Conservative first-start calibration from completed production phases. The target and source
/// values use observed wall times and source payload sizes. Parity uses the observed upper runtime
/// so a new controller does not promise an unrealistically short queue before it learns locally.
fn builtin_eta_seeds() -> Vec<CompletedPhaseSample> {
    let mut seeds = vec![
        eta_seed_sample(1010, EtaPhase::TargetBuild, 92_709_712_208, 583.0),
        eta_seed_sample(1011, EtaPhase::TargetBuild, 107_877_204_541, 1_224.0),
        eta_seed_sample(1012, EtaPhase::TargetBuild, 111_203_836_128, 1_527.0),
        eta_seed_sample(997, EtaPhase::SourceControlBuild, 98_013_362_278, 827.0),
        eta_seed_sample(1000, EtaPhase::SourceControlBuild, 86_336_354_618, 770.0),
        eta_seed_sample(1012, EtaPhase::SourceControlBuild, 111_203_836_128, 836.0),
        eta_seed_sample(501, EtaPhase::Parity, 38_198_949_979, 135.0),
        eta_seed_sample(1000, EtaPhase::Parity, 86_336_354_618, 135.0),
        eta_seed_sample(1012, EtaPhase::Parity, 111_203_836_128, 135.0),
    ];
    let two_thread_seeds = seeds
        .iter()
        .cloned()
        .map(|mut sample| {
            sample.worker_threads = 2;
            if sample.phase != EtaPhase::Parity {
                sample.wall_secs *= 2.0;
            }
            sample.completed_unix_secs = sample.started_unix_secs + sample.wall_secs.ceil() as u64;
            sample
        })
        .collect::<Vec<_>>();
    seeds.extend(two_thread_seeds);
    seeds
}

fn ensure_eta_seed_groups(history: &mut EtaHistory) -> bool {
    let seeds = builtin_eta_seeds();
    let mut changed = false;
    for (phase, threads) in [
        (EtaPhase::TargetBuild, 4),
        (EtaPhase::SourceControlBuild, 4),
        (EtaPhase::Parity, 4),
        (EtaPhase::TargetBuild, 2),
        (EtaPhase::SourceControlBuild, 2),
        (EtaPhase::Parity, 2),
    ] {
        if !history
            .samples
            .iter()
            .any(|sample| sample.phase == phase && sample.worker_threads == threads)
        {
            history.samples.extend(
                seeds
                    .iter()
                    .filter(|sample| sample.phase == phase && sample.worker_threads == threads)
                    .cloned(),
            );
            changed = true;
        }
    }
    changed
}

fn load_or_initialize_eta_history(args: &Args) -> Result<EtaHistory> {
    let path = eta_history_path(args);
    let (mut history, mut changed) = match read_optional_json::<EtaHistory>(&path)? {
        Some(history) => (history, false),
        None => (EtaHistory::new(Vec::new()), true),
    };
    history
        .validate()
        .context("validate Firewatch ETA history")?;
    changed |= ensure_eta_seed_groups(&mut history);
    history
        .validate()
        .context("validate seeded Firewatch ETA history")?;
    if changed {
        publish_json_atomic(&path, &history)?;
    }
    Ok(history)
}

fn trim_eta_history(history: &mut EtaHistory) {
    history.samples.sort_by(|left, right| {
        right
            .completed_unix_secs
            .cmp(&left.completed_unix_secs)
            .then_with(|| right.epoch.cmp(&left.epoch))
    });
    let mut group_counts = BTreeMap::<(EtaPhase, u32), usize>::new();
    history.samples.retain(|sample| {
        let count = group_counts
            .entry((sample.phase, sample.worker_threads))
            .or_default();
        let keep = *count < firewatch_controller_eta::MAX_SAMPLES_PER_GROUP;
        *count += 1;
        keep
    });
    history.samples.truncate(MAX_DURABLE_ETA_SAMPLES);
}

fn eta_phase_for(candidate: &Candidate, phase: Phase) -> Option<EtaPhase> {
    match phase {
        Phase::TargetBuild => Some(EtaPhase::TargetBuild),
        Phase::SourceControlBuild => Some(EtaPhase::SourceControlBuild),
        Phase::Parity => Some(EtaPhase::Parity),
        Phase::CanonicalBuild if candidate.registry_order == "usage_sorted" => {
            Some(EtaPhase::TargetBuild)
        }
        Phase::CanonicalBuild if candidate.registry_order == "first_seen" => {
            Some(EtaPhase::SourceControlBuild)
        }
        Phase::CanonicalBuild => None,
    }
}

fn paused_wall_secs(child: &ActiveChild) -> f64 {
    let current_pause = child
        .paused_started_at
        .map_or(Duration::ZERO, |started| started.elapsed());
    child
        .paused_secs
        .saturating_add(current_pause)
        .as_secs_f64()
}

fn record_successful_eta_sample(args: &Args, child: &ActiveChild) -> Result<()> {
    let phase = eta_phase_for(&child.candidate, child.phase)
        .context("completed Firewatch phase has no ETA calibration mapping")?;
    let mut history = load_or_initialize_eta_history(args)?;
    history.samples.push(CompletedPhaseSample {
        epoch: child.candidate.epoch,
        phase,
        worker_threads: child.worker_threads,
        input_bytes: child.candidate.input_bytes(),
        started_unix_secs: child.started_unix_secs,
        completed_unix_secs: unix_now(),
        wall_secs: child.started_at.elapsed().as_secs_f64().max(0.001),
        paused_secs: paused_wall_secs(child),
    });
    trim_eta_history(&mut history);
    history
        .validate()
        .context("validate updated Firewatch ETA history")?;
    publish_json_atomic(&eta_history_path(args), &history)
}

fn finish_successful_child(args: &Args, child: &mut ActiveChild) -> Result<bool> {
    child
        .candidate
        .recheck_authority_proofs("child output publication")?;
    match child.phase {
        Phase::TargetBuild => {
            ensure!(
                exact_index_manifest(
                    &child.candidate.target_index(args),
                    &child.candidate.target,
                    child.candidate.epoch,
                    &child.candidate.target_effective_input,
                    child.candidate.target_wire_profile,
                    true,
                )?
                .is_some(),
                "target build exited without an exact manifest"
            );
            complete_or_defer_cleanup(args, child, "target index complete")
        }
        Phase::SourceControlBuild => {
            ensure!(
                exact_index_manifest(
                    &child.candidate.source_index(args),
                    &child.candidate.source,
                    child.candidate.epoch,
                    &child.candidate.source_effective_input,
                    child.candidate.source_wire_profile,
                    true,
                )?
                .is_some(),
                "source build exited without an exact manifest"
            );
            complete_or_defer_cleanup(args, child, "source control index complete")
        }
        Phase::Parity => {
            let parity = parse_parity_log(&child.log_path)?;
            publish_acceptance(args, &child.candidate, parity)?;
            complete_or_defer_cleanup(args, child, "canonical parity accepted")
        }
        Phase::CanonicalBuild => {
            verify_direct_archive_files(&child.candidate.source, child.candidate.direct_files()?)?;
            let manifest = exact_direct_index_manifest(&child.candidate, args, true)?
                .context("canonical build exited without an exact manifest")?;
            publish_direct_acceptance(args, &child.candidate, &manifest)?;
            complete_or_defer_cleanup(args, child, "canonical direct index accepted")
        }
    }
}

fn complete_or_defer_cleanup(args: &Args, child: &mut ActiveChild, message: &str) -> Result<bool> {
    let Some(path) = child.owned_temp_path.as_deref() else {
        mark_phase_complete(args, &child.candidate, child.phase, message)?;
        return Ok(true);
    };
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            child.owned_temp_path = None;
            mark_phase_complete(args, &child.candidate, child.phase, message)?;
            Ok(true)
        }
        Err(error) => Err(error.into()),
        Ok(_) => {
            let marker_path = child.candidate.workflow_path(args);
            let marker: WorkflowMarker = read_bounded_json(&marker_path)?;
            mark_cleanup_pending_path(
                &marker_path,
                marker,
                path,
                child.pid,
                Some(child.start_ticks),
                false,
                false,
                &format!("{message}; temporary workspace cleanup is pending"),
            )?;
            child.owned_temp_path = None;
            Ok(false)
        }
    }
}

fn mark_phase_complete(
    args: &Args,
    candidate: &Candidate,
    phase: Phase,
    message: &str,
) -> Result<()> {
    candidate.recheck_authority_proofs("phase completion marker publication")?;
    let marker = WorkflowMarker {
        schema_version: WORKFLOW_SCHEMA_VERSION,
        epoch: candidate.epoch,
        source_generation_sha256: candidate.source_generation.clone(),
        target_generation_sha256: candidate.target_generation.clone(),
        source_effective_input_sha256: Some(candidate.source_effective_input.clone()),
        target_effective_input_sha256: Some(candidate.target_effective_input.clone()),
        source_wire_profile: Some(candidate.source_wire_profile),
        target_wire_profile: Some(candidate.target_wire_profile),
        authority_binding_sha256: Some(candidate.authority_binding_sha256.clone()),
        retry_of_failed_marker_sha256: candidate.retry_of_failed_marker_sha256.clone(),
        state: if matches!(phase, Phase::Parity | Phase::CanonicalBuild) {
            "accepted"
        } else {
            "phase_complete"
        }
        .into(),
        phase,
        created_unix_secs: unix_now(),
        updated_unix_secs: unix_now(),
        attempt_id: None,
        pid: None,
        process_start_ticks: None,
        executable: None,
        executable_dev: None,
        executable_ino: None,
        argv: Vec::new(),
        log_path: None,
        auto_paused: false,
        auto_pause_reason: None,
        owned_temp_path: None,
        cleanup_owner_absence_confirmed: false,
        message: Some(message.into()),
    };
    publish_json_atomic(&candidate.workflow_path(args), &marker)
}

fn fail_workflow(args: &Args, candidate: &Candidate, phase: Phase, message: String) -> Result<()> {
    let marker = WorkflowMarker {
        schema_version: WORKFLOW_SCHEMA_VERSION,
        epoch: candidate.epoch,
        source_generation_sha256: candidate.source_generation.clone(),
        target_generation_sha256: candidate.target_generation.clone(),
        source_effective_input_sha256: Some(candidate.source_effective_input.clone()),
        target_effective_input_sha256: Some(candidate.target_effective_input.clone()),
        source_wire_profile: Some(candidate.source_wire_profile),
        target_wire_profile: Some(candidate.target_wire_profile),
        authority_binding_sha256: Some(candidate.authority_binding_sha256.clone()),
        retry_of_failed_marker_sha256: candidate.retry_of_failed_marker_sha256.clone(),
        state: "failed".into(),
        phase,
        created_unix_secs: unix_now(),
        updated_unix_secs: unix_now(),
        attempt_id: None,
        pid: None,
        process_start_ticks: None,
        executable: None,
        executable_dev: None,
        executable_ino: None,
        argv: Vec::new(),
        log_path: None,
        auto_paused: false,
        auto_pause_reason: None,
        owned_temp_path: None,
        cleanup_owner_absence_confirmed: false,
        message: Some(message),
    };
    publish_json_atomic(&candidate.workflow_path(args), &marker)
}

fn defer_child_cleanup(
    args: &Args,
    child: &ActiveChild,
    terminal_failure: bool,
    message: String,
) -> Result<()> {
    ensure!(
        process_start_ticks(child.pid) != Some(child.start_ticks),
        "cannot defer cleanup while the Firewatch child is live"
    );
    let marker_path = child.candidate.workflow_path(args);
    let marker: WorkflowMarker = read_bounded_json(&marker_path)?;
    ensure!(
        marker_matches_candidate(&marker, &child.candidate)
            && marker.pid == Some(child.pid)
            && marker.process_start_ticks == Some(child.start_ticks),
        "Firewatch cleanup marker process binding changed"
    );
    if let Some(path) = child.owned_temp_path.as_deref() {
        match fs::symlink_metadata(path) {
            Ok(_) => mark_cleanup_pending_path(
                &marker_path,
                marker,
                path,
                child.pid,
                Some(child.start_ticks),
                false,
                terminal_failure,
                &message,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if terminal_failure {
                    fail_workflow(args, &child.candidate, child.phase, message)
                } else {
                    mark_interrupted_path(&marker_path, marker, &message)
                }
            }
            Err(error) => Err(error.into()),
        }
    } else {
        if terminal_failure {
            fail_workflow(args, &child.candidate, child.phase, message)
        } else {
            mark_interrupted_path(&marker_path, marker, &message)
        }
    }
}

fn update_running_marker(args: &Args, child: &ActiveChild) -> Result<()> {
    child
        .candidate
        .recheck_authority_proofs("running marker update")?;
    ensure!(
        process_start_ticks(child.pid) == Some(child.start_ticks),
        "Firewatch child PID identity changed"
    );
    let mut marker: WorkflowMarker = read_bounded_json(&child.candidate.workflow_path(args))?;
    ensure!(
        marker_matches_candidate(&marker, &child.candidate),
        "workflow marker binding changed"
    );
    ensure!(marker.pid == Some(child.pid), "workflow marker PID changed");
    ensure!(
        marker.process_start_ticks == Some(child.start_ticks),
        "workflow start identity changed"
    );
    ensure!(
        marker.owned_temp_path == child.owned_temp_path,
        "workflow temporary workspace binding changed"
    );
    ensure!(
        process_matches_marker_pid(&marker, child.pid)?,
        "workflow process attempt identity changed"
    );
    marker.state = if child.paused { "paused" } else { "running" }.into();
    marker.auto_paused = child.paused;
    marker.auto_pause_reason = child.pause_reason.clone();
    marker.updated_unix_secs = unix_now();
    publish_json_atomic(&child.candidate.workflow_path(args), &marker)
}

#[derive(Debug)]
struct ParityResult {
    wallets: u64,
    relations: u64,
    canonical_sha256: String,
}

fn parse_parity_log(path: &Path) -> Result<ParityResult> {
    let text = fs::read_to_string(path)?;
    let mut values = BTreeMap::<&str, &str>::new();
    for line in text.lines() {
        let Some((key, value)) = line.trim().split_once('=') else {
            continue;
        };
        if matches!(
            key,
            "canonical_equal" | "wallets" | "relations" | "canonical_sha256"
        ) {
            ensure!(
                values.insert(key, value).is_none(),
                "duplicate parity key {key}"
            );
        }
    }
    ensure!(
        values.get("canonical_equal") == Some(&"true"),
        "canonical parity is not equal"
    );
    let wallets = values
        .get("wallets")
        .context("parity wallets are missing")?
        .parse()?;
    let relations = values
        .get("relations")
        .context("parity relations are missing")?
        .parse()?;
    let canonical_sha256 = values
        .get("canonical_sha256")
        .context("parity hash is missing")?
        .to_string();
    ensure!(is_sha256(&canonical_sha256), "parity hash is invalid");
    Ok(ParityResult {
        wallets,
        relations,
        canonical_sha256,
    })
}

fn publish_acceptance(args: &Args, candidate: &Candidate, parity: ParityResult) -> Result<()> {
    publish_acceptance_with_pre_publish(args, candidate, parity, || Ok(()))
}

fn publish_acceptance_with_pre_publish(
    args: &Args,
    candidate: &Candidate,
    parity: ParityResult,
    pre_publish: impl FnOnce() -> Result<()>,
) -> Result<()> {
    candidate.recheck_authority_proofs("pair acceptance publication start")?;
    let source = verify_pair_index_generation(
        &candidate.source_index(args),
        &candidate.source,
        candidate.epoch,
        &candidate.source_effective_input,
        candidate.source_wire_profile,
    )?;
    let target = verify_pair_index_generation(
        &candidate.target_index(args),
        &candidate.target,
        candidate.epoch,
        &candidate.target_effective_input,
        candidate.target_wire_profile,
    )?;
    ensure!(
        source.manifest.wallet_count == target.manifest.wallet_count,
        "source/target wallet totals differ"
    );
    ensure!(
        source.manifest.program_count == target.manifest.program_count,
        "source/target program totals differ"
    );
    ensure!(
        source.manifest.transactions_scanned == target.manifest.transactions_scanned
            && source.manifest.blocks_scanned == target.manifest.blocks_scanned
            && source.manifest.failed_transactions_excluded
                == target.manifest.failed_transactions_excluded,
        "source/target scan totals differ"
    );
    ensure!(
        parity.wallets == source.manifest.wallet_count,
        "parity wallet total differs"
    );
    let receipt = AcceptanceReceipt {
        schema_version: ACCEPTANCE_SCHEMA_VERSION,
        epoch: candidate.epoch,
        source_generation_sha256: candidate.source_generation.clone(),
        target_generation_sha256: candidate.target_generation.clone(),
        source_effective_input_sha256: candidate.source_effective_input.clone(),
        target_effective_input_sha256: candidate.target_effective_input.clone(),
        source_wire_profile: candidate.source_wire_profile,
        target_wire_profile: candidate.target_wire_profile,
        authority_binding_sha256: candidate.authority_binding_sha256.clone(),
        source_index: candidate.source_index(args),
        target_index: candidate.target_index(args),
        source_manifest_sha256: source.identity.manifest_sha256.clone(),
        target_manifest_sha256: target.identity.manifest_sha256.clone(),
        wallets: parity.wallets,
        relations: parity.relations,
        canonical_sha256: parity.canonical_sha256,
        accepted_unix_secs: unix_now(),
    };
    pre_publish()?;
    let source_now =
        capture_index_generation_identity(&candidate.source_index(args), &source.manifest)?;
    let target_now =
        capture_index_generation_identity(&candidate.target_index(args), &target.manifest)?;
    ensure!(
        source_now == source.identity && target_now == target.identity,
        "source or target index changed before acceptance publication"
    );
    candidate.recheck_authority_proofs("pair acceptance final publication")?;
    let path = candidate.acceptance_path(args);
    if path.exists() {
        let existing: AcceptanceReceipt = read_bounded_json(&path)?;
        ensure!(
            serde_json::to_value(&existing)? == serde_json::to_value(&receipt)?,
            "an existing acceptance receipt differs"
        );
        return Ok(());
    }
    publish_json_no_replace(&path, &receipt)
}

fn eta_work(candidate: &Candidate, phase: Phase, worker_threads: u32) -> Option<PhaseWork> {
    let input_bytes = candidate.input_bytes();
    if input_bytes == 0 || worker_threads == 0 {
        return None;
    }
    Some(PhaseWork {
        phase: eta_phase_for(candidate, phase)?,
        worker_threads,
        input_bytes,
    })
}

fn queued_eta_phases(view: &CandidateView, worker_threads: u32) -> Option<Vec<PhaseWork>> {
    let phases = match (view.candidate.mode, view.phase) {
        (CandidateMode::CanonicalDirect, Phase::CanonicalBuild) => vec![Phase::CanonicalBuild],
        (CandidateMode::MigrationPair, Phase::TargetBuild) => {
            let mut phases = vec![Phase::TargetBuild];
            if view.source_manifest.is_none() {
                phases.push(Phase::SourceControlBuild);
            }
            phases.push(Phase::Parity);
            phases
        }
        (CandidateMode::MigrationPair, Phase::SourceControlBuild) => {
            vec![Phase::SourceControlBuild, Phase::Parity]
        }
        (CandidateMode::MigrationPair, Phase::Parity) => vec![Phase::Parity],
        _ => return None,
    };
    phases
        .into_iter()
        .map(|phase| eta_work(&view.candidate, phase, worker_threads))
        .collect()
}

fn future_eta_phases(
    view: Option<&CandidateView>,
    child: &ActiveChild,
    worker_threads: u32,
) -> Option<Vec<PhaseWork>> {
    let Some(view) = view else {
        return Some(Vec::new());
    };
    let phases = match (child.candidate.mode, child.phase) {
        (CandidateMode::CanonicalDirect, Phase::CanonicalBuild) => Vec::new(),
        (CandidateMode::MigrationPair, Phase::TargetBuild) => {
            let mut phases = Vec::new();
            if view.source_manifest.is_none() {
                phases.push(Phase::SourceControlBuild);
            }
            phases.push(Phase::Parity);
            phases
        }
        (CandidateMode::MigrationPair, Phase::SourceControlBuild) => vec![Phase::Parity],
        (CandidateMode::MigrationPair, Phase::Parity) => Vec::new(),
        _ => return None,
    };
    phases
        .into_iter()
        .map(|phase| eta_work(&child.candidate, phase, worker_threads))
        .collect()
}

fn active_eta_secs(estimator: Option<&EtaEstimator>, child: &ActiveChild) -> Option<f64> {
    let estimator = estimator?;
    let work = eta_work(&child.candidate, child.phase, child.worker_threads)?;
    estimator
        .estimate_active_phase(&ActivePhaseWork {
            work,
            elapsed_secs: child.started_at.elapsed().as_secs_f64(),
        })
        .ok()
        .flatten()
        .map(|estimate| estimate.expected_secs)
}

fn full_queue_eta_secs(
    args: &Args,
    views: &[CandidateView],
    active: &BTreeMap<u64, ActiveChild>,
    estimator: Option<&EtaEstimator>,
) -> Option<f64> {
    let estimator = estimator?;
    let primary_threads = u32::try_from(args.threads).ok()?;
    let mut active_items = Vec::with_capacity(active.len());
    for (epoch, child) in active {
        let current = ActivePhaseWork {
            work: eta_work(&child.candidate, child.phase, child.worker_threads)?,
            elapsed_secs: child.started_at.elapsed().as_secs_f64(),
        };
        let view = views.iter().find(|view| view.candidate.epoch == *epoch);
        active_items.push(ActiveItemWork {
            current,
            future_phases: future_eta_phases(view, child, primary_threads)?,
        });
    }
    let queued_items = views
        .iter()
        .filter(|view| view.state == "queued" && !active.contains_key(&view.candidate.epoch))
        .map(|view| {
            queued_eta_phases(view, primary_threads).map(|phases| QueuedItemWork { phases })
        })
        .collect::<Option<Vec<_>>>()?;
    // Steady work uses the short clean-transition gap. When no work is active, add the remaining
    // difference once so a controller cold start still carries one full admission window instead
    // of charging that full window to every queued phase.
    let cold_start_extra_secs = if active.is_empty() && !queued_items.is_empty() {
        args.resume_stable_secs
            .saturating_sub(args.clean_transition_secs) as f64
    } else {
        0.0
    };
    estimator
        .estimate_queue(&QueueEtaInput {
            active_items,
            queued_items,
            stable_admission_gap_secs: args.clean_transition_secs as f64,
            // Current production cgroup pressure gives one reliable slot. This value is
            // deliberately conservative even when adaptive admission briefly adds a worker.
            effective_concurrency: 1.0,
        })
        .ok()
        .flatten()
        .map(|estimate| estimate.expected_secs + cold_start_extra_secs)
}

fn build_status(
    args: &Args,
    views: &[CandidateView],
    blocked_wire_profiles: &[WireProfileBlockedCandidate],
    active: &BTreeMap<u64, ActiveChild>,
    effective_capacity: usize,
    eta_estimator: Option<&EtaEstimator>,
    coverage: (u32, u32, u32, u32),
    admission_reason: Option<String>,
) -> ControllerStatus {
    let accepted = views
        .iter()
        .filter(|view| view.acceptance.is_some())
        .count();
    let running = active.len();
    let queued = views
        .iter()
        .filter(|view| view.state == "queued" && !active.contains_key(&view.candidate.epoch))
        .count();
    let queue_eta_secs = full_queue_eta_secs(args, views, active, eta_estimator);
    let mut rows = views
        .iter()
        .map(|view| {
            let active_child = active.get(&view.candidate.epoch);
            let is_active = active_child.is_some();
            let acceptance = view.acceptance.as_ref();
            let (state, phase, paused, pause_reason) = if let Some(child) = active_child {
                (
                    if child.paused { "paused" } else { "running" }.to_string(),
                    child.phase.as_status().to_string(),
                    child.paused,
                    child.pause_reason.clone(),
                )
            } else {
                (
                    view.state.clone(),
                    view.phase.as_status().to_string(),
                    false,
                    None,
                )
            };
            let manifest = view
                .source_manifest
                .as_ref()
                .or(view.target_manifest.as_ref());
            StatusRow {
                epoch: u32::try_from(view.candidate.epoch).unwrap_or(u32::MAX),
                state,
                phase,
                auto_paused: paused,
                auto_pause_reason: pause_reason,
                progress_pct: if acceptance.is_some() { 100.0 } else { 0.0 },
                blocks_done: acceptance
                    .and_then(|_| manifest.map(|manifest| manifest.blocks_scanned))
                    .unwrap_or(0),
                eta_secs: active_child.and_then(|child| active_eta_secs(eta_estimator, child)),
                rss_bytes: active_child.and_then(|child| child.telemetry.rss_bytes),
                read_mib_per_sec: active_child.and_then(|child| child.telemetry.read_mib_per_sec),
                write_mib_per_sec: active_child.and_then(|child| child.telemetry.write_mib_per_sec),
                wallet_count: acceptance.map(CandidateAcceptance::wallets),
                relation_count: acceptance.and_then(CandidateAcceptance::relations),
                parity_status: if view.candidate.mode == CandidateMode::CanonicalDirect {
                    None
                } else if is_active
                    && active_child.is_some_and(|child| child.phase == Phase::Parity)
                {
                    Some("running".into())
                } else if acceptance.is_some() {
                    Some("equal".into())
                } else {
                    Some("pending".into())
                },
                wire_profile_audit_inputs: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    for blocked in blocked_wire_profiles {
        if active.contains_key(&blocked.epoch)
            || views
                .iter()
                .any(|view| view.candidate.epoch == blocked.epoch)
        {
            continue;
        }
        rows.push(StatusRow {
            epoch: u32::try_from(blocked.epoch).unwrap_or(u32::MAX),
            state: "profile_audit_required".into(),
            phase: "wire_profile_audit".into(),
            auto_paused: false,
            auto_pause_reason: None,
            progress_pct: 0.0,
            blocks_done: 0,
            eta_secs: None,
            rss_bytes: None,
            read_mib_per_sec: None,
            write_mib_per_sec: None,
            wallet_count: None,
            relation_count: None,
            parity_status: if blocked.mode == CandidateMode::MigrationPair {
                Some("pending".into())
            } else {
                None
            },
            wire_profile_audit_inputs: blocked.inputs.clone(),
        });
    }
    for (epoch, child) in active {
        if views.iter().any(|view| view.candidate.epoch == *epoch) {
            continue;
        }
        rows.push(StatusRow {
            epoch: u32::try_from(*epoch).unwrap_or(u32::MAX),
            state: if child.paused { "paused" } else { "running" }.into(),
            phase: child.phase.as_status().into(),
            auto_paused: child.paused,
            auto_pause_reason: child.pause_reason.clone(),
            progress_pct: 0.0,
            blocks_done: 0,
            eta_secs: active_eta_secs(eta_estimator, child),
            rss_bytes: child.telemetry.rss_bytes,
            read_mib_per_sec: child.telemetry.read_mib_per_sec,
            write_mib_per_sec: child.telemetry.write_mib_per_sec,
            wallet_count: None,
            relation_count: None,
            parity_status: Some(
                if child.phase == Phase::Parity {
                    "running"
                } else {
                    "pending"
                }
                .into(),
            ),
            wire_profile_audit_inputs: Vec::new(),
        });
    }
    // Preserve `views` order. It is also the `next_runnable` order, and the monitor uses the first
    // queued row as its "Next" epoch. Active orphan rows remain appended after known candidates.
    let epochs_total = rows.len();
    ControllerStatus {
        schema_version: STATUS_SCHEMA_VERSION,
        updated_unix_secs: unix_now(),
        capacity_configured: if args.execute {
            effective_capacity as u32
        } else {
            0
        },
        running: running as u32,
        epochs_total: epochs_total as u32,
        epochs_accepted: accepted as u32,
        epochs_queued: queued as u32,
        archive_epochs_total: Some(coverage.0),
        epochs_eligible: Some(coverage.1),
        epochs_blocked_migration: Some(coverage.2),
        epochs_blocked_wire_profile: Some(coverage.3),
        queue_eta_secs,
        admission_blocked_reason: admission_reason,
        rows,
    }
}

fn sample_process(child: &mut ActiveChild) -> ProcessTelemetry {
    if process_start_ticks(child.pid) != Some(child.start_ticks) {
        child.telemetry = ProcessTelemetry::default();
        return child.telemetry;
    }
    let rss_bytes = read_rss_bytes(child.pid);
    let current = read_process_io(child.pid);
    let mut telemetry = ProcessTelemetry {
        rss_bytes,
        ..ProcessTelemetry::default()
    };
    if let (Some(previous), Some(current)) = (child.io_sample, current) {
        let elapsed = current
            .sampled_at
            .duration_since(previous.sampled_at)
            .as_secs_f64();
        if elapsed > 0.0
            && current.read_bytes >= previous.read_bytes
            && current.write_bytes >= previous.write_bytes
        {
            telemetry.read_mib_per_sec =
                Some((current.read_bytes - previous.read_bytes) as f64 / elapsed / 1024.0 / 1024.0);
            telemetry.write_mib_per_sec = Some(
                (current.write_bytes - previous.write_bytes) as f64 / elapsed / 1024.0 / 1024.0,
            );
        }
    }
    child.io_sample = current;
    child.telemetry = telemetry;
    telemetry
}

fn read_rss_bytes(pid: u32) -> Option<u64> {
    let text = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    text.lines().find_map(|line| {
        let kib = line
            .strip_prefix("VmRSS:")?
            .trim()
            .strip_suffix(" kB")?
            .trim();
        kib.parse::<u64>().ok()?.checked_mul(1024)
    })
}

fn read_process_io(pid: u32) -> Option<ProcessIoSample> {
    let text = fs::read_to_string(format!("/proc/{pid}/io")).ok()?;
    let mut read_bytes = None;
    let mut write_bytes = None;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("read_bytes: ") {
            read_bytes = value.parse().ok();
        } else if let Some(value) = line.strip_prefix("write_bytes: ") {
            write_bytes = value.parse().ok();
        }
    }
    Some(ProcessIoSample {
        read_bytes: read_bytes?,
        write_bytes: write_bytes?,
        sampled_at: Instant::now(),
    })
}

fn process_start_ticks(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.rfind(')')?;
    let fields = stat
        .get(close + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    fields.get(19)?.parse().ok()
}

fn kill_pid_group(pid: u32) {
    // SAFETY: only called for a child spawned as its own process-group leader.
    let _ = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
}

fn terminate_pid_group(pid: u32, start_ticks: u64) {
    // SAFETY: only called after exact executable/argv or owned child identity validation.
    if process_start_ticks(pid) != Some(start_ticks) {
        return;
    }
    // A stopped process must run before it can act on SIGTERM.
    let _ = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGCONT) };
    // SAFETY: the same exact controller-owned process group is targeted.
    let _ = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGTERM) };
    for _ in 0..100 {
        if process_start_ticks(pid) != Some(start_ticks) {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    if process_start_ticks(pid) == Some(start_ticks) {
        kill_pid_group(pid);
    }
}

fn kill_child_group(child: &ActiveChild) {
    if process_start_ticks(child.pid) == Some(child.start_ticks) {
        terminate_pid_group(child.pid, child.start_ticks);
    }
}

fn read_bounded_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = read_bounded_control_bytes(path)?;
    serde_json::from_slice(&bytes).with_context(|| format!("decode JSON {}", path.display()))
}

fn read_bounded_json_with_sha256<T: DeserializeOwned>(path: &Path) -> Result<(T, String)> {
    let bytes = read_bounded_control_bytes(path)?;
    let sha256 = hex_digest(Sha256::digest(&bytes));
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode JSON {}", path.display()))?;
    Ok((value, sha256))
}

fn registry_file_identity(metadata: &fs::Metadata) -> RegistryFileIdentity {
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

fn read_pinned_fd_bytes(file: &File, max_bytes: u64, label: &str) -> Result<Vec<u8>> {
    let capacity = max_bytes
        .checked_add(1)
        .context("pinned file byte limit overflow")?;
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
            .context("pinned file read offset overflow")?;
    }
    ensure!(
        bytes.len() as u64 <= max_bytes,
        "pinned {label} is too large"
    );
    Ok(bytes)
}

fn validate_protected_pinned_metadata(metadata: &fs::Metadata, label: &str) -> Result<()> {
    ensure!(
        metadata.file_type().is_file()
            && metadata.nlink() == 1
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.permissions().mode() & 0o022 == 0,
        "{label} is not an euid-owned protected nlink-1 regular file"
    );
    Ok(())
}

fn read_pinned_file_evidence(
    path: &Path,
    max_bytes: u64,
    label: &str,
) -> Result<(Vec<u8>, PinnedFileEvidence)> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .with_context(|| format!("open pinned {label} {}", path.display()))?;
    let opened = file.metadata()?;
    ensure!(
        opened.file_type().is_file() && opened.len() <= max_bytes,
        "pinned {label} is not a bounded regular file"
    );
    let identity = registry_file_identity(&opened);
    let bytes = read_pinned_fd_bytes(&file, max_bytes, label)?;
    let after = file.metadata()?;
    let path_after = fs::symlink_metadata(path)?;
    ensure!(
        registry_file_identity(&after) == identity
            && path_after.file_type().is_file()
            && !path_after.file_type().is_symlink()
            && registry_file_identity(&path_after) == identity
            && after.len() == bytes.len() as u64,
        "pinned {label} changed during its descriptor read"
    );
    let sha256 = hex_digest(Sha256::digest(&bytes));
    Ok((
        bytes,
        PinnedFileEvidence {
            path: path.to_path_buf(),
            file,
            identity,
            sha256,
            max_bytes,
        },
    ))
}

fn read_pinned_json<T: DeserializeOwned>(path: &Path, label: &str) -> Result<PinnedJson<T>> {
    let (bytes, evidence) = read_pinned_file_evidence(path, MAX_CONTROL_JSON_BYTES, label)?;
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode pinned {label} JSON {}", path.display()))?;
    Ok(PinnedJson {
        value,
        evidence: Arc::new(evidence),
    })
}

impl PinnedFileEvidence {
    fn recheck(&self, require_protected: bool, label: &str) -> Result<()> {
        let descriptor_before = self.file.metadata()?;
        ensure!(
            registry_file_identity(&descriptor_before) == self.identity,
            "{label} descriptor identity changed"
        );
        if require_protected {
            validate_protected_pinned_metadata(&descriptor_before, label)?;
        }
        let path_before = fs::symlink_metadata(&self.path)?;
        ensure!(
            path_before.file_type().is_file()
                && !path_before.file_type().is_symlink()
                && registry_file_identity(&path_before) == self.identity,
            "{label} path no longer names its admitted descriptor"
        );
        if require_protected {
            validate_protected_pinned_metadata(&path_before, label)?;
        }
        let bytes = read_pinned_fd_bytes(&self.file, self.max_bytes, label)?;
        ensure!(
            bytes.len() as u64 == self.identity.size
                && hex_digest(Sha256::digest(&bytes)) == self.sha256,
            "{label} descriptor content changed"
        );
        let descriptor_after = self.file.metadata()?;
        let path_after = fs::symlink_metadata(&self.path)?;
        ensure!(
            registry_file_identity(&descriptor_after) == self.identity
                && path_after.file_type().is_file()
                && !path_after.file_type().is_symlink()
                && registry_file_identity(&path_after) == self.identity,
            "{label} path or descriptor changed during final recheck"
        );
        if require_protected {
            validate_protected_pinned_metadata(&descriptor_after, label)?;
            validate_protected_pinned_metadata(&path_after, label)?;
        }
        Ok(())
    }
}

fn read_bounded_control_bytes(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect JSON file {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "JSON path is not a real regular file"
    );
    ensure!(
        metadata.len() <= MAX_CONTROL_JSON_BYTES,
        "JSON file is too large"
    );
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let opened = file.metadata()?;
    ensure!(
        same_file(&metadata, &opened),
        "JSON path changed before open"
    );
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(MAX_CONTROL_JSON_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_CONTROL_JSON_BYTES,
        "JSON file is too large"
    );
    let after = file.metadata()?;
    let path_after = fs::symlink_metadata(path)?;
    ensure!(
        same_file(&opened, &after)
            && same_version(&opened, &after)
            && same_file(&after, &path_after)
            && same_version(&after, &path_after)
            && after.len() == bytes.len() as u64,
        "JSON file changed while reading"
    );
    Ok(bytes)
}

fn read_optional_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match read_bounded_json(path) {
        Ok(value) => Ok(Some(value)),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(None)
        }
        Err(_error) if !path.exists() => Ok(None),
        Err(error) => Err(error),
    }
}

fn same_file(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    a.dev() == b.dev() && a.ino() == b.ino() && a.file_type().is_file() && b.file_type().is_file()
}

fn same_version(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    a.len() == b.len()
        && a.mtime() == b.mtime()
        && a.mtime_nsec() == b.mtime_nsec()
        && a.ctime() == b.ctime()
        && a.ctime_nsec() == b.ctime_nsec()
}

fn publish_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("JSON output has no parent")?;
    fs::create_dir_all(parent)?;
    let bytes = serde_json::to_vec_pretty(value)?;
    let max_bytes = if path.file_name().and_then(|name| name.to_str()) == Some("status.json") {
        MAX_STATUS_JSON_BYTES
    } else {
        MAX_CONTROL_JSON_BYTES
    };
    ensure!(bytes.len() as u64 <= max_bytes, "JSON output is too large");
    let name = path
        .file_name()
        .context("JSON output has no name")?
        .to_string_lossy();
    let temp = unique_temp_path(parent, &name);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn publish_json_no_replace(path: &Path, value: &impl Serialize) -> Result<()> {
    ensure!(!path.exists(), "JSON output already exists");
    let parent = path.parent().context("JSON output has no parent")?;
    fs::create_dir_all(parent)?;
    let bytes = serde_json::to_vec_pretty(value)?;
    let name = path
        .file_name()
        .context("JSON output has no name")?
        .to_string_lossy();
    let temp = unique_temp_path(parent, &name);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::hard_link(&temp, path)?;
    File::open(parent)?.sync_all()?;
    fs::remove_file(&temp)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn unique_temp_path(parent: &Path, name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    parent.join(format!(".{name}.{}-{nanos}.tmp", std::process::id()))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_digest(hasher.finalize()))
}

fn sha256_control_file(path: &Path) -> Result<String> {
    let before = fs::symlink_metadata(path)?;
    ensure!(
        before.file_type().is_file()
            && !before.file_type().is_symlink()
            && before.len() <= MAX_CONTROL_JSON_BYTES,
        "control file is not a bounded regular file"
    );
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let opened = file.metadata()?;
    ensure!(
        same_file(&before, &opened) && same_version(&before, &opened),
        "control file changed before hashing"
    );
    let mut hasher = Sha256::new();
    let mut read_total = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        read_total = read_total
            .checked_add(read as u64)
            .context("control file size overflow")?;
        ensure!(
            read_total <= MAX_CONTROL_JSON_BYTES,
            "control file is too large"
        );
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata()?;
    let path_after = fs::symlink_metadata(path)?;
    ensure!(
        read_total == after.len()
            && same_file(&opened, &after)
            && same_version(&opened, &after)
            && same_file(&after, &path_after)
            && same_version(&after, &path_after),
        "control file changed while hashing"
    );
    Ok(hex_digest(hasher.finalize()))
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let digest = bytes.as_ref();
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("write to String");
    }
    encoded
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
    use blockzilla_firebase_indexer::format::{
        IndexBuilder, IndexSemantics, OmissionCounts, ProgramUsage, bind_shard, write_program_map,
    };

    const TEST_HOST_MEMORY_RESERVE_BYTES: u64 = 3 * GIB;

    fn scheduler(memory: u64, memory_pressure: f64, io_pressure: f64) -> SchedulerStatus {
        SchedulerStatus {
            schema_version: 3,
            sequence: 1,
            now_unix_secs: 1,
            control_reconciled_unix_secs: 1,
            observer_mode: false,
            scheduler: SchedulerControl { paused: false },
            machine: SchedulerMachine {
                memory_available_bytes: memory,
                disk_available_bytes: 10 * 1024 * GIB,
                io_pressure_full_avg10: Some(io_pressure),
                memory_pressure_some_avg10: Some(memory_pressure),
                cpu_pressure_some_avg10: Some(0.0),
            },
            inventory: SchedulerInventory { complete: true },
            summary: SchedulerSummary::default(),
            scan_sweep: SchedulerScanSweep::default(),
            epochs: Vec::new(),
            lanes: Vec::new(),
        }
    }

    fn test_pressure(
        status: &SchedulerStatus,
        disk_reserve_bytes: u64,
        context: PressureContext,
    ) -> PressureState {
        pressure_state(
            status,
            disk_reserve_bytes,
            context,
            TEST_HOST_MEMORY_RESERVE_BYTES,
        )
    }

    fn adaptive_args() -> Args {
        Args {
            scheduler_status_url: "http://127.0.0.1:8786/api/v1/status".into(),
            scheduler_state_root: PathBuf::from("/state"),
            archive_root: PathBuf::from("/archive"),
            usage_sorted_root: PathBuf::from("/archive/usage-sorted"),
            output_root: PathBuf::from("/output"),
            parity_scratch_root: PathBuf::from("/scratch"),
            indexer_bin: PathBuf::from("/bin/true"),
            parity_bin: PathBuf::from("/bin/true"),
            controller_state_root: PathBuf::from("/state/firewatch-index"),
            status_file: PathBuf::from("/state/firewatch-index/status.json"),
            threads: 4,
            additional_worker_threads: 2,
            max_workers: 2,
            worker_memory_reserve_mib: 768,
            host_memory_reserve_mib: 3072,
            additional_io_full_max: 10.0,
            additional_cpu_some_max: 20.0,
            parity_sort_memory_mib: 256,
            poll_secs: 5,
            resume_stable_secs: 60,
            clean_transition_secs: 10,
            disk_reserve_gib: 512,
            execute: true,
            authorize_direct_retry_epoch: None,
            retry_reason: None,
        }
    }

    #[test]
    fn admission_timing_requires_two_clean_polls_within_the_cold_window() {
        assert!(validate_admission_timing(5, 10, 60).is_ok());
        assert!(validate_admission_timing(5, 9, 60).is_err());
        assert!(validate_admission_timing(5, 61, 60).is_err());
        assert!(validate_admission_timing(u64::MAX, u64::MAX, u64::MAX).is_err());
    }

    #[test]
    fn clean_completion_grants_one_short_admission_window() {
        let args = adaptive_args();
        let started = Instant::now();
        let mut pool = PoolState::default();

        update_admission_window_after_completions(&mut pool, true, true, false, started);
        let clean = pool.admission_window(started + Duration::from_secs(10));
        assert_eq!(clean.kind, AdmissionWindowKind::CleanTransition);
        assert_eq!(admission_window_secs(&args, clean.kind), 10);
        assert_eq!(
            (started + Duration::from_secs(10)).saturating_duration_since(clean.safe_since),
            Duration::from_secs(10)
        );

        // A completed admission attempt consumes the permit. The next safe sample is cold.
        pool.clear_admission_window();
        let cold_started = started + Duration::from_secs(11);
        let cold = pool.admission_window(cold_started);
        assert_eq!(cold.kind, AdmissionWindowKind::Cold);
        assert_eq!(admission_window_secs(&args, cold.kind), 60);
        assert_eq!(cold.safe_since, cold_started);
    }

    #[test]
    fn unsafe_or_failed_completion_cannot_grant_a_clean_transition() {
        let started = Instant::now();
        let mut pool = PoolState::default();
        pool.grant_clean_transition(started);

        update_admission_window_after_completions(
            &mut pool,
            false,
            true,
            false,
            started + Duration::from_secs(1),
        );
        assert!(pool.admission_window.is_none());

        pool.grant_clean_transition(started);
        update_admission_window_after_completions(
            &mut pool,
            true,
            true,
            true,
            started + Duration::from_secs(1),
        );
        assert!(pool.admission_window.is_none());
    }

    #[test]
    fn a_poll_without_a_completion_preserves_the_existing_window() {
        let started = Instant::now();
        let mut pool = PoolState::default();
        pool.grant_clean_transition(started);
        update_admission_window_after_completions(
            &mut pool,
            true,
            false,
            false,
            started + Duration::from_secs(5),
        );
        let window = pool.admission_window.expect("clean window remains present");
        assert_eq!(window.kind, AdmissionWindowKind::CleanTransition);
        assert_eq!(window.safe_since, started);
    }

    fn candidate_view(phase: Phase) -> CandidateView {
        CandidateView {
            candidate: Candidate {
                mode: CandidateMode::MigrationPair,
                epoch: 301,
                source: PathBuf::from("/archive/epoch-301"),
                target: PathBuf::from("/archive/usage-sorted/epoch-301"),
                source_generation: "a".repeat(64),
                target_generation: "b".repeat(64),
                source_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                target_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                source_effective_input: "c".repeat(64),
                target_effective_input: "d".repeat(64),
                authority_binding_sha256: "e".repeat(64),
                authority_proofs: None,
                registry_order: "usage_sorted".into(),
                direct_files: None,
                input_bytes: 0,
                retry_of_failed_marker_sha256: None,
            },
            phase,
            state: "queued".into(),
            acceptance: None,
            target_manifest: None,
            source_manifest: None,
        }
    }

    fn legacy_accepted_marker(candidate: &Candidate, phase: Phase) -> WorkflowMarker {
        WorkflowMarker {
            schema_version: 1,
            epoch: candidate.epoch,
            source_generation_sha256: candidate.source_generation.clone(),
            target_generation_sha256: candidate.target_generation.clone(),
            source_effective_input_sha256: None,
            target_effective_input_sha256: None,
            source_wire_profile: None,
            target_wire_profile: None,
            authority_binding_sha256: None,
            retry_of_failed_marker_sha256: None,
            state: "accepted".into(),
            phase,
            created_unix_secs: 1,
            updated_unix_secs: 1,
            attempt_id: None,
            pid: None,
            process_start_ticks: None,
            executable: None,
            executable_dev: None,
            executable_ino: None,
            argv: Vec::new(),
            log_path: None,
            auto_paused: false,
            auto_pause_reason: None,
            owned_temp_path: None,
            cleanup_owner_absence_confirmed: false,
            message: Some("legacy accepted index".into()),
        }
    }

    fn cgroup_snapshot() -> CgroupMemorySnapshot {
        CgroupMemorySnapshot {
            current_bytes: 512 * MIB,
            high_bytes: Some(2 * GIB),
            max_bytes: Some(3 * GIB),
            anon_bytes: 384 * MIB,
            file_bytes: 128 * MIB,
            inactive_file_bytes: 64 * MIB,
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

    fn direct_test_args(root: &Path) -> Args {
        let mut args = adaptive_args();
        args.scheduler_state_root = root.join("state");
        args.archive_root = root.join("archive");
        args.usage_sorted_root = args.archive_root.join("usage-sorted-generations");
        args.output_root = root.join("output");
        args.parity_scratch_root = root.join("scratch");
        args.controller_state_root = args.scheduler_state_root.join("firewatch-index");
        args.status_file = args.controller_state_root.join("status.json");
        for path in [
            args.scheduler_state_root.join("registry_reprocess"),
            args.archive_root.clone(),
            args.usage_sorted_root.clone(),
            args.output_root.clone(),
            args.parity_scratch_root.clone(),
            args.controller_state_root.join("epochs"),
            args.controller_state_root.join("canonical-epochs"),
            args.controller_state_root.join("logs"),
            args.controller_state_root
                .join(WIRE_PROFILE_ATTESTATIONS_DIR),
            args.controller_state_root
                .join(RETRY_READY_DIR)
                .join("canonical-epochs"),
        ] {
            fs::create_dir_all(path).unwrap();
        }
        args
    }

    fn bind_test_authority(candidate: &mut Candidate, root: &Path) -> PathBuf {
        let mode = match candidate.mode {
            CandidateMode::MigrationPair => "pair",
            CandidateMode::CanonicalDirect => "direct",
        };
        let path = root.join(format!("test-authority-{mode}-{}.json", candidate.epoch));
        fs::write(&path, b"exact test authority v1").unwrap();
        let (_, evidence) = read_pinned_file_evidence(&path, 1_024, "test authority").unwrap();
        let proofs = build_authority_proof_set(vec![AuthorityProof {
            label: "test_authority",
            evidence: Arc::new(evidence),
            require_protected: false,
        }])
        .unwrap();
        candidate.authority_binding_sha256 = proofs.digest.clone();
        candidate.authority_proofs = Some(proofs);
        path
    }

    fn write_direct_archive(root: &Path, epoch: u64) -> PathBuf {
        let archive = root.join(format!("epoch-{epoch}"));
        fs::create_dir(&archive).unwrap();
        for (name, bytes) in [
            (ARCHIVE_V2_BLOCKS_FILE, b"blocks".as_slice()),
            (ARCHIVE_V2_BLOCK_INDEX_FILE, b"index".as_slice()),
            (ARCHIVE_V2_META_FILE, b"meta".as_slice()),
            (ARCHIVE_V2_PUBKEY_REGISTRY_FILE, &[7u8; 32]),
            (ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, b"mphf".as_slice()),
            (ARCHIVE_V2_SIGNATURES_FILE, b"".as_slice()),
        ] {
            fs::write(archive.join(name), bytes).unwrap();
        }
        archive
    }

    fn registry_binding(bytes: &[u8]) -> RegistryFileBinding {
        RegistryFileBinding {
            bytes: bytes.len() as u64,
            sha256: hex_digest(Sha256::digest(bytes)),
        }
    }

    fn valid_receipt_source_evidence() -> String {
        encode_receipt_source_recovery_evidence_v3(1, 2, 0, 0, 2, 0).unwrap()
    }

    fn valid_receipt_target_evidence() -> String {
        encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
            generation_kind: RECEIPT_TARGET_ATTESTATION_GENERATION_KIND.into(),
            blocks: 1,
            messages: 2,
            raw_transaction_fallbacks: 0,
            alternate_profile_failures: 0,
            both_semantically_equivalent: 2,
            both_semantically_divergent: 0,
            decision: FullGenerationAuditDecisionV3::ProfileBoundReceipt,
        })
        .unwrap()
    }

    struct Schema4PairFixture {
        marker_path: PathBuf,
        receipt_path: PathBuf,
        staging_path: PathBuf,
        target: PathBuf,
        status: SchedulerStatus,
    }

    fn write_schema4_pair_fixture(args: &Args, epoch: u64) -> Schema4PairFixture {
        let source = args.archive_root.join(format!("epoch-{epoch}"));
        let target = args.usage_sorted_root.join(format!("epoch-{epoch}"));
        fs::create_dir(&source).unwrap();
        fs::create_dir(&target).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"src").unwrap();
        fs::write(source.join(ARCHIVE_V2_POH_FILE), b"poh-current").unwrap();
        fs::write(target.join(ARCHIVE_V2_BLOCKS_FILE), b"dst").unwrap();
        let post = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let post_marker = wire_profile_marker(post);
        fs::write(
            target.join(&post_marker.name),
            wire_profile_marker_bytes(post),
        )
        .unwrap();
        let source_files = BTreeMap::from([
            (ARCHIVE_V2_BLOCKS_FILE.to_owned(), registry_binding(b"src")),
            (
                ARCHIVE_V2_POH_FILE.to_owned(),
                registry_binding(b"poh-current"),
            ),
        ]);
        let target_files = BTreeMap::from([
            (ARCHIVE_V2_BLOCKS_FILE.to_owned(), registry_binding(b"dst")),
            (
                post_marker.name.clone(),
                registry_binding(wire_profile_marker_bytes(post)),
            ),
        ]);
        let attempt_id = "ab".repeat(16);
        let handoff_sha256 = "cd".repeat(32);
        let target_name = target.file_name().unwrap().to_string_lossy();
        let staging_path = target.with_file_name(format!(
            ".{target_name}.registry-reprocess.{attempt_id}.staging"
        ));
        let marker_path = args
            .scheduler_state_root
            .join(format!("registry_reprocess/epoch-{epoch}.json"));
        fs::write(
            &marker_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 4,
                "kind": "archive_v2_registry_reprocess",
                "epoch": epoch,
                "state": "complete",
                "phase": "access",
                "source": source,
                "target": target,
                "threads": 6,
                "wire_profile": "post-unknown-instruction-fallbacks-v1",
                "attempt_id": attempt_id,
                "staging_dir": staging_path,
                "handoff_sha256": handoff_sha256,
                "expected_access_state": null,
                "pid": null,
                "process_start_ticks": null,
                "audit_retry_is_safe": false,
                "audit_is_continuation": false
            }))
            .unwrap(),
        )
        .unwrap();
        let receipt_path = target.join(REGISTRY_RECEIPT_FILE);
        fs::write(
            &receipt_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "version": 3,
                "algorithm": REGISTRY_RECEIPT_V3_ALGORITHM,
                "epoch": epoch,
                "threads": 6,
                "source_dir": source,
                "target_dir": target,
                "source_generation_sha256": registry_generation_digest(&source_files),
                "target_generation_sha256": registry_generation_digest(&target_files),
                "source_files": source_files,
                "target_files": target_files,
                "attempt_id": attempt_id,
                "handoff_sha256": handoff_sha256,
                "wire_profile": "post-unknown-instruction-fallbacks-v1"
            }))
            .unwrap(),
        )
        .unwrap();
        let mut status = scheduler(8 * GIB, 0.0, 0.0);
        status.epochs = vec![scheduler_epoch(epoch, "first_seen")];
        Schema4PairFixture {
            marker_path,
            receipt_path,
            staging_path,
            target,
            status,
        }
    }

    fn attest_schema4_source(args: &Args, fixture: &Schema4PairFixture, epoch: u64) {
        let receipt: RegistryReceipt = read_bounded_json(&fixture.receipt_path).unwrap();
        let source = PathBuf::from(&receipt.source_dir);
        let archive_files = validate_receipt_files(&source, &receipt.source_files).unwrap();
        let generation = receipt.source_generation_sha256;
        let attestation = WireProfileAttestation {
            schema_version: WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION,
            kind: "archive_v2_wire_profile_attestation".into(),
            audit_algorithm: WIRE_PROFILE_AUDIT_ALGORITHM.into(),
            audited_profiles: WIRE_PROFILE_AUDITED_PROFILES,
            cluster_id: "mainnet-beta".into(),
            epoch,
            archive: source,
            registry_order: "first_seen".into(),
            generation_kind: RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND.into(),
            content_generation_sha256: generation.clone(),
            archive_files,
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            evidence: valid_receipt_source_evidence(),
            attested_unix_secs: 1,
        };
        publish_json_no_replace(
            &args
                .controller_state_root
                .join(WIRE_PROFILE_ATTESTATIONS_DIR)
                .join(format!("epoch-{epoch}-{generation}.json")),
            &attestation,
        )
        .unwrap();
    }

    fn deterministic_direct_files() -> BTreeMap<String, RegistryFileIdentity> {
        DIRECT_SEMANTIC_FILES
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                let index = index as u64;
                (
                    name,
                    RegistryFileIdentity {
                        size: 11 + index,
                        device: 21 + index,
                        inode: 31 + index,
                        modified_seconds: 41 + index as i64,
                        modified_nanoseconds: 51 + index as i64,
                        changed_seconds: 61 + index as i64,
                        changed_nanoseconds: 71 + index as i64,
                    },
                )
            })
            .collect()
    }

    fn write_pair_archive(root: &Path, name: &str) -> PathBuf {
        let archive = root.join(name);
        fs::create_dir(&archive).unwrap();
        fs::write(archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), [7u8; 64]).unwrap();
        fs::write(
            archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE),
            [8u8; 24],
        )
        .unwrap();
        fs::canonicalize(archive).unwrap()
    }

    fn write_pair_index(index: &Path, archive: &Path, generation: &str) -> IndexManifest {
        fs::create_dir_all(index).unwrap();
        let shard = index.join("shard-0");
        let mut builder = IndexBuilder::new(1, 2, 2);
        assert_eq!(
            builder.record(
                1,
                ProgramUsage::new_transaction(2, 1, 0, 42, Some(1_700_000_000)).unwrap()
            ),
            blockzilla_firebase_indexer::format::RecordOutcome::Recorded
        );
        assert_eq!(builder.write(&shard).unwrap(), 1);
        let shard_binding = bind_shard(0, &shard, 2, 2).unwrap();
        let program_map = write_program_map(index, &[(2, [9u8; 32])]).unwrap();
        let registry_file_identity =
            capture_direct_file_identity(&archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)).unwrap();
        let registry_index_file_identity =
            capture_direct_file_identity(&archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
                .unwrap();
        let manifest = IndexManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            format_version: FORMAT_VERSION,
            semantics: IndexSemantics::current(),
            complete: true,
            omissions: OmissionCounts::default(),
            binding_kind: GenerationBindingKind::TrustedLocalAssertedImmutable,
            cluster_id: "mainnet-beta".into(),
            epoch: 700,
            archive_root: archive.display().to_string(),
            generation_id: generation.into(),
            generation_digest: "e".repeat(64),
            archive_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            registry: blockzilla_firebase_indexer::format::IndexFileBinding {
                size: 64,
                sha256: "a".repeat(64),
            },
            registry_file_identity,
            registry_index: blockzilla_firebase_indexer::format::IndexFileBinding {
                size: 24,
                sha256: "b".repeat(64),
            },
            registry_index_file_identity,
            registry_entries: 2,
            chunk_width: 2,
            shard_count: 1,
            shards: vec![shard_binding],
            program_map,
            wallet_count: 1,
            program_count: 1,
            transactions_scanned: 3,
            blocks_scanned: 2,
            failed_transactions_excluded: 1,
            built_unix_time: 1,
            tool_version: "test".into(),
        };
        manifest.write(index).unwrap();
        IndexManifest::verify_generation(index).unwrap()
    }

    fn pair_acceptance_fixture(root: &Path) -> (Args, Candidate, ParityResult) {
        let args = direct_test_args(root);
        let source = write_pair_archive(&args.archive_root, "epoch-700");
        let target = write_pair_archive(&args.usage_sorted_root, "epoch-700");
        let mut candidate = Candidate {
            mode: CandidateMode::MigrationPair,
            epoch: 700,
            source,
            target,
            source_generation: "a".repeat(64),
            target_generation: "b".repeat(64),
            source_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            target_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            source_effective_input: "c".repeat(64),
            target_effective_input: "d".repeat(64),
            authority_binding_sha256: "e".repeat(64),
            authority_proofs: None,
            registry_order: "usage_sorted".into(),
            direct_files: None,
            input_bytes: 1,
            retry_of_failed_marker_sha256: None,
        };
        bind_test_authority(&mut candidate, root);
        fs::create_dir_all(candidate.epoch_root(&args)).unwrap();
        write_pair_index(
            &candidate.source_index(&args),
            &candidate.source,
            &candidate.source_effective_input,
        );
        write_pair_index(
            &candidate.target_index(&args),
            &candidate.target,
            &candidate.target_effective_input,
        );
        let parity = ParityResult {
            wallets: 1,
            relations: 1,
            canonical_sha256: "f".repeat(64),
        };
        (args, candidate, parity)
    }

    fn direct_acceptance_fixture(root: &Path) -> (Args, Candidate, IndexManifest) {
        let args = direct_test_args(root);
        let archive = write_direct_archive(&args.archive_root, 700);
        fs::write(archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), [7u8; 64]).unwrap();
        fs::write(
            archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE),
            [8u8; 24],
        )
        .unwrap();
        let (files, generation, input_bytes) =
            capture_direct_archive(&archive, 700, "usage_sorted").unwrap();
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let effective_input = effective_input_digest(&generation, profile).unwrap();
        let mut candidate = Candidate {
            mode: CandidateMode::CanonicalDirect,
            epoch: 700,
            source: archive.clone(),
            target: archive,
            source_generation: generation.clone(),
            target_generation: generation,
            source_wire_profile: profile,
            target_wire_profile: profile,
            source_effective_input: effective_input.clone(),
            target_effective_input: effective_input,
            authority_binding_sha256: "e".repeat(64),
            authority_proofs: None,
            registry_order: "usage_sorted".into(),
            direct_files: Some(files),
            input_bytes,
            retry_of_failed_marker_sha256: None,
        };
        bind_test_authority(&mut candidate, root);
        fs::create_dir_all(candidate.epoch_root(&args)).unwrap();
        let manifest = write_pair_index(
            &candidate.direct_index(&args),
            &candidate.source,
            &candidate.target_effective_input,
        );
        (args, candidate, manifest)
    }

    fn scheduler_epoch(epoch: u64, order: &str) -> SchedulerEpoch {
        SchedulerEpoch {
            epoch,
            state: "complete".into(),
            registry_order: order.into(),
            output_path: PathBuf::from(format!("epoch-{epoch}")),
        }
    }

    fn attest_direct_archive(args: &Args, epoch: u64, order: &str) {
        let archive = args.archive_root.join(format!("epoch-{epoch}"));
        let (archive_files, generation, _) =
            capture_direct_archive(&archive, epoch, order).unwrap();
        let attestation = WireProfileAttestation {
            schema_version: WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION,
            kind: "archive_v2_wire_profile_attestation".into(),
            audit_algorithm: WIRE_PROFILE_AUDIT_ALGORITHM.into(),
            audited_profiles: WIRE_PROFILE_AUDITED_PROFILES,
            cluster_id: "mainnet-beta".into(),
            epoch,
            archive,
            registry_order: order.into(),
            generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
            content_generation_sha256: generation.clone(),
            archive_files,
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            evidence: encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
                generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
                blocks: 1,
                messages: 1,
                raw_transaction_fallbacks: 0,
                alternate_profile_failures: 0,
                both_semantically_equivalent: 1,
                both_semantically_divergent: 0,
                decision: FullGenerationAuditDecisionV3::AllSemanticallyEquivalent,
            })
            .unwrap(),
            attested_unix_secs: 1,
        };
        publish_json_no_replace(
            &args
                .controller_state_root
                .join(WIRE_PROFILE_ATTESTATIONS_DIR)
                .join(format!("epoch-{epoch}-{generation}.json")),
            &attestation,
        )
        .unwrap();
    }

    fn rewrite_only_wire_profile_attestation(
        args: &Args,
        update: impl FnOnce(&mut serde_json::Value),
    ) {
        let root = args
            .controller_state_root
            .join(WIRE_PROFILE_ATTESTATIONS_DIR);
        let mut entries = fs::read_dir(root).unwrap();
        let path = entries.next().unwrap().unwrap().path();
        assert!(entries.next().is_none());
        let mut attestation: serde_json::Value = read_bounded_json(&path).unwrap();
        update(&mut attestation);
        publish_json_atomic(&path, &attestation).unwrap();
    }

    #[test]
    fn controller_accepts_the_exact_audit_tool_attestation_json() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let archive = write_direct_archive(&args.archive_root, 700);
        let (archive_files, generation, _) =
            capture_direct_archive(&archive, 700, "usage_sorted").unwrap();
        let path = args
            .controller_state_root
            .join(WIRE_PROFILE_ATTESTATIONS_DIR)
            .join(format!("epoch-700-{generation}.json"));
        let evidence = encode_full_generation_audit_evidence_v3(&FullGenerationAuditEvidenceV3 {
            generation_kind: DIRECT_ATTESTATION_GENERATION_KIND.into(),
            blocks: 1,
            messages: 1,
            raw_transaction_fallbacks: 0,
            alternate_profile_failures: 0,
            both_semantically_equivalent: 1,
            both_semantically_divergent: 0,
            decision: FullGenerationAuditDecisionV3::AllSemanticallyEquivalent,
        })
        .unwrap();
        let audit_tool_json = serde_json::json!({
            "schema_version": 2,
            "kind": "archive_v2_wire_profile_attestation",
            "audit_algorithm": "archive-v2-borrowed-dual-profile-full-generation-v2",
            "audited_profiles": [
                "pre-unknown-instruction-fallbacks-v1",
                "post-unknown-instruction-fallbacks-v1"
            ],
            "cluster_id": "mainnet-beta",
            "epoch": 700,
            "archive": archive,
            "registry_order": "usage_sorted",
            "generation_kind": "direct-file-identity-v1",
            "content_generation_sha256": generation,
            "archive_files": archive_files,
            "wire_profile": "post-unknown-instruction-fallbacks-v1",
            "evidence": evidence,
            "attested_unix_secs": 1
        });
        fs::write(&path, serde_json::to_vec_pretty(&audit_tool_json).unwrap()).unwrap();

        let loaded = load_wire_profile_attestations(&args).unwrap();
        let attestation = &loaded.values().next().unwrap().value;
        assert_eq!(loaded.len(), 1);
        assert_eq!(attestation.epoch, 700);
        assert_eq!(
            attestation.wire_profile,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        );
    }

    #[test]
    fn attestation_loader_rejects_schema_1() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        write_direct_archive(&args.archive_root, 700);
        attest_direct_archive(&args, 700, "usage_sorted");
        rewrite_only_wire_profile_attestation(&args, |attestation| {
            attestation["schema_version"] = serde_json::json!(1);
        });

        assert!(load_wire_profile_attestations(&args).is_err());
    }

    #[test]
    fn attestation_loader_rejects_wrong_audit_algorithm() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        write_direct_archive(&args.archive_root, 700);
        attest_direct_archive(&args, 700, "usage_sorted");
        rewrite_only_wire_profile_attestation(&args, |attestation| {
            attestation["audit_algorithm"] = serde_json::json!("untrusted-audit-v1");
        });

        assert!(load_wire_profile_attestations(&args).is_err());
    }

    #[test]
    fn attestation_loader_rejects_wrong_audited_profile_set() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        write_direct_archive(&args.archive_root, 700);
        attest_direct_archive(&args, 700, "usage_sorted");
        rewrite_only_wire_profile_attestation(&args, |attestation| {
            attestation["audited_profiles"] = serde_json::json!([
                "post-unknown-instruction-fallbacks-v1",
                "post-unknown-instruction-fallbacks-v1"
            ]);
        });

        assert!(load_wire_profile_attestations(&args).is_err());
    }

    #[test]
    fn attestation_loader_rejects_reordered_audited_profiles() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        write_direct_archive(&args.archive_root, 700);
        attest_direct_archive(&args, 700, "usage_sorted");
        rewrite_only_wire_profile_attestation(&args, |attestation| {
            attestation["audited_profiles"] = serde_json::json!([
                "post-unknown-instruction-fallbacks-v1",
                "pre-unknown-instruction-fallbacks-v1"
            ]);
        });

        assert!(load_wire_profile_attestations(&args).is_err());
    }

    #[test]
    fn attestation_loader_ignores_publisher_temp_files_but_rejects_bad_exact_names() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        write_direct_archive(&args.archive_root, 700);
        attest_direct_archive(&args, 700, "usage_sorted");
        let attestation_root = args
            .controller_state_root
            .join(WIRE_PROFILE_ATTESTATIONS_DIR);
        fs::write(
            attestation_root.join(format!(".epoch-700-{}.json.123-456.tmp", "a".repeat(64))),
            b"partial",
        )
        .unwrap();
        assert_eq!(load_wire_profile_attestations(&args).unwrap().len(), 1);

        fs::write(attestation_root.join("epoch-700-not-a-digest.json"), b"{}").unwrap();
        assert!(load_wire_profile_attestations(&args).is_err());
    }

    #[test]
    fn every_receipt_source_attestation_is_protected_and_pinned_to_one_inode() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let fixture = write_schema4_pair_fixture(&args, 709);
        attest_schema4_source(&args, &fixture, 709);
        let attestation_path = fs::read_dir(
            args.controller_state_root
                .join(WIRE_PROFILE_ATTESTATIONS_DIR),
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();

        let attestation_root = args
            .controller_state_root
            .join(WIRE_PROFILE_ATTESTATIONS_DIR);
        let mut root_permissions = fs::metadata(&attestation_root).unwrap().permissions();
        root_permissions.set_mode(0o777);
        fs::set_permissions(&attestation_root, root_permissions).unwrap();
        assert!(load_wire_profile_attestations(&args).is_err());
        let mut root_permissions = fs::metadata(&attestation_root).unwrap().permissions();
        root_permissions.set_mode(0o755);
        fs::set_permissions(&attestation_root, root_permissions).unwrap();

        let mut permissions = fs::metadata(&attestation_path).unwrap().permissions();
        permissions.set_mode(0o666);
        fs::set_permissions(&attestation_path, permissions).unwrap();
        assert!(load_wire_profile_attestations(&args).is_err());
        let mut permissions = fs::metadata(&attestation_path).unwrap().permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&attestation_path, permissions).unwrap();

        let hard_link = attestation_path.with_extension("hard-link");
        fs::hard_link(&attestation_path, &hard_link).unwrap();
        assert!(load_wire_profile_attestations(&args).is_err());
        fs::remove_file(hard_link).unwrap();

        let loaded = load_wire_profile_attestations(&args).unwrap();
        let pinned = loaded.values().next().unwrap();
        let exact_bytes = fs::read(&attestation_path).unwrap();
        let replacement = attestation_path.with_extension("replacement");
        fs::write(&replacement, exact_bytes).unwrap();
        fs::rename(&replacement, &attestation_path).unwrap();
        assert!(
            pinned
                .evidence
                .recheck(true, "receipt-source attestation replacement test")
                .is_err()
        );
    }

    #[test]
    fn direct_generation_binds_every_opened_semantic_file_identity() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let archive_root = root.join("archive");
        fs::create_dir(&archive_root).unwrap();
        let archive = write_direct_archive(&archive_root, 700);
        let (before, before_digest, input_bytes) =
            capture_direct_archive(&archive, 700, "usage_sorted").unwrap();
        assert_eq!(before.len(), DIRECT_SEMANTIC_FILES.len());
        assert_eq!(input_bytes, 6);
        assert!(is_sha256(&before_digest));

        let replacement = archive.join("blocks.replacement");
        fs::write(&replacement, b"blocks").unwrap();
        fs::rename(&replacement, archive.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap();
        let (after, after_digest, _) =
            capture_direct_archive(&archive, 700, "usage_sorted").unwrap();
        assert_ne!(
            before[ARCHIVE_V2_BLOCKS_FILE].inode,
            after[ARCHIVE_V2_BLOCKS_FILE].inode
        );
        assert_ne!(before_digest, after_digest);
    }

    #[test]
    fn legacy_direct_digest_is_unchanged_and_marker_digest_matches_audit_vector() {
        let mut files = deterministic_direct_files();
        assert_eq!(
            direct_generation_digest(700, "usage_sorted", &files),
            "2efae1e22d39e6ae9dfea3e77091047eafcb27ac2be46b6262dd04c18ba38390"
        );

        let marker = wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1);
        files.insert(
            marker.name,
            RegistryFileIdentity {
                size: marker.size,
                device: 121,
                inode: 131,
                modified_seconds: 141,
                modified_nanoseconds: 151,
                changed_seconds: 161,
                changed_nanoseconds: 171,
            },
        );
        // This vector uses the frozen audit-tool field order and direct-generation domain.
        assert_eq!(
            direct_generation_digest(700, "usage_sorted", &files),
            "6a3a98cbf7f28af46e4303bd4d35aec5357798d0e90e32848b6b08470b605ce0"
        );
    }

    #[test]
    fn replacing_an_exact_marker_invalidates_its_attestation() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let archive = write_direct_archive(&args.archive_root, 700);
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let marker = wire_profile_marker(profile);
        fs::write(
            archive.join(&marker.name),
            wire_profile_marker_bytes(profile),
        )
        .unwrap();
        attest_direct_archive(&args, 700, "usage_sorted");
        let mut status = scheduler(8 * GIB, 0.0, 0.0);
        status.epochs = vec![scheduler_epoch(700, "usage_sorted")];

        let before = discover_candidates(&args, &status).unwrap();
        assert_eq!(before.candidates.len(), 1);
        let old_generation = before.candidates[0].target_generation.clone();

        let replacement = archive.join("wire-profile-marker.replacement");
        fs::write(&replacement, wire_profile_marker_bytes(profile)).unwrap();
        fs::rename(&replacement, archive.join(&marker.name)).unwrap();

        let after = discover_candidates(&args, &status).unwrap();
        assert!(after.candidates.is_empty());
        assert_eq!(after.blocked_wire_profiles.len(), 1);
        assert_ne!(
            after.blocked_wire_profiles[0].inputs[0].content_generation_sha256,
            old_generation
        );
    }

    #[test]
    fn marker_bound_attestation_cannot_claim_the_other_profile() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let archive = write_direct_archive(&args.archive_root, 700);
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let marker = wire_profile_marker(profile);
        fs::write(
            archive.join(&marker.name),
            wire_profile_marker_bytes(profile),
        )
        .unwrap();
        attest_direct_archive(&args, 700, "usage_sorted");
        let attestation_path = fs::read_dir(
            args.controller_state_root
                .join(WIRE_PROFILE_ATTESTATIONS_DIR),
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
        let mut attestation: WireProfileAttestation = read_bounded_json(&attestation_path).unwrap();
        attestation.wire_profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        publish_json_atomic(&attestation_path, &attestation).unwrap();

        assert!(load_wire_profile_attestations(&args).is_err());
    }

    #[test]
    fn conflicting_and_malformed_markers_block_only_their_epoch() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        write_direct_archive(&args.archive_root, 700);
        attest_direct_archive(&args, 700, "usage_sorted");
        let bad_archive = write_direct_archive(&args.archive_root, 701);
        let pre = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        let post = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let pre_marker = wire_profile_marker(pre);
        let post_marker = wire_profile_marker(post);
        fs::write(
            bad_archive.join(&pre_marker.name),
            wire_profile_marker_bytes(pre),
        )
        .unwrap();
        fs::write(
            bad_archive.join(&post_marker.name),
            wire_profile_marker_bytes(post),
        )
        .unwrap();
        let mut status = scheduler(8 * GIB, 0.0, 0.0);
        status.epochs = vec![
            scheduler_epoch(700, "usage_sorted"),
            scheduler_epoch(701, "usage_sorted"),
        ];

        let conflict = discover_candidates(&args, &status).unwrap();
        assert_eq!(conflict.candidates.len(), 1);
        assert_eq!(conflict.candidates[0].epoch, 700);
        assert_eq!(conflict.blocked_wire_profiles.len(), 1);
        assert_eq!(conflict.blocked_wire_profiles[0].epoch, 701);

        fs::remove_file(bad_archive.join(&post_marker.name)).unwrap();
        fs::write(
            bad_archive.join(&pre_marker.name),
            vec![0; pre_marker.size as usize],
        )
        .unwrap();
        let malformed = discover_candidates(&args, &status).unwrap();
        assert_eq!(malformed.candidates.len(), 1);
        assert_eq!(malformed.candidates[0].epoch, 700);
        assert_eq!(malformed.blocked_wire_profiles.len(), 1);
        assert_eq!(malformed.blocked_wire_profiles[0].epoch, 701);
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_marker_is_profile_audit_blocked_without_hiding_a_healthy_epoch() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        write_direct_archive(&args.archive_root, 700);
        attest_direct_archive(&args, 700, "usage_sorted");
        let bad_archive = write_direct_archive(&args.archive_root, 701);
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let marker = wire_profile_marker(profile);
        fs::write(
            bad_archive.join("marker-target"),
            wire_profile_marker_bytes(profile),
        )
        .unwrap();
        symlink("marker-target", bad_archive.join(&marker.name)).unwrap();
        let mut status = scheduler(8 * GIB, 0.0, 0.0);
        status.epochs = vec![
            scheduler_epoch(700, "usage_sorted"),
            scheduler_epoch(701, "usage_sorted"),
        ];

        let discovery = discover_candidates(&args, &status).unwrap();
        assert_eq!(discovery.candidates.len(), 1);
        assert_eq!(discovery.candidates[0].epoch, 700);
        assert_eq!(discovery.blocked_wire_profiles.len(), 1);
        assert_eq!(discovery.blocked_wire_profiles[0].epoch, 701);
    }

    #[cfg(unix)]
    #[test]
    fn direct_generation_rejects_a_symlinked_semantic_input() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let archive_root = root.join("archive");
        fs::create_dir(&archive_root).unwrap();
        let archive = write_direct_archive(&archive_root, 700);
        fs::rename(
            archive.join(ARCHIVE_V2_META_FILE),
            archive.join("real-meta"),
        )
        .unwrap();
        symlink("real-meta", archive.join(ARCHIVE_V2_META_FILE)).unwrap();
        assert!(capture_direct_archive(&archive, 700, "usage_sorted").is_err());
    }

    #[test]
    fn scheduler_complete_usage_sorted_and_first_seen_epochs_are_direct_candidates() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        for epoch in [700, 701, 702] {
            write_direct_archive(&args.archive_root, epoch);
        }
        attest_direct_archive(&args, 700, "usage_sorted");
        attest_direct_archive(&args, 701, "first_seen");
        fs::write(
            args.scheduler_state_root
                .join("registry_reprocess/epoch-701.json"),
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 3,
                "kind": "archive_v2_registry_reprocess",
                "epoch": 701,
                "state": "failed",
                "phase": "core",
                "source": args.archive_root.join("epoch-701"),
                "target": args.usage_sorted_root.join("epoch-701"),
                "pid": null,
                "process_start_ticks": null
            }))
            .unwrap(),
        )
        .unwrap();
        let mut status = scheduler(8 * GIB, 0.0, 0.0);
        status.epochs = vec![
            scheduler_epoch(700, "usage_sorted"),
            scheduler_epoch(701, "first_seen"),
            scheduler_epoch(702, "unknown"),
        ];

        let discovery = discover_candidates(&args, &status).unwrap();
        assert_eq!(discovery.archive_epochs_total, 3);
        assert_eq!(discovery.epochs_eligible, 2);
        assert_eq!(discovery.epochs_blocked_migration, 0);
        assert_eq!(
            discovery
                .candidates
                .iter()
                .map(|candidate| (
                    candidate.epoch,
                    candidate.mode,
                    candidate.registry_order.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![
                (701, CandidateMode::CanonicalDirect, "first_seen"),
                (700, CandidateMode::CanonicalDirect, "usage_sorted"),
            ]
        );
    }

    #[test]
    fn eta_history_is_seeded_atomically_for_every_primary_phase() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);

        let history = load_or_initialize_eta_history(&args).unwrap();
        let estimator = EtaEstimator::from_history(&history).unwrap();
        for phase in [
            EtaPhase::TargetBuild,
            EtaPhase::SourceControlBuild,
            EtaPhase::Parity,
        ] {
            assert!(estimator.sample_count(phase, 4) > 0);
        }
        assert!(estimator.sample_count(EtaPhase::TargetBuild, 2) > 0);
        assert!(estimator.sample_count(EtaPhase::SourceControlBuild, 2) > 0);
        assert!(estimator.sample_count(EtaPhase::Parity, 2) > 0);
        assert!(eta_history_path(&args).is_file());
        assert_eq!(load_or_initialize_eta_history(&args).unwrap(), history);
        assert!(
            fs::read_dir(&args.controller_state_root)
                .unwrap()
                .all(|entry| !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp"))
        );
    }

    #[test]
    fn canonical_eta_mapping_uses_registry_order() {
        let mut view = candidate_view(Phase::TargetBuild);
        view.candidate.mode = CandidateMode::CanonicalDirect;
        view.phase = Phase::CanonicalBuild;
        view.candidate.input_bytes = 10;
        view.candidate.registry_order = "usage_sorted".into();
        assert_eq!(
            eta_work(&view.candidate, Phase::CanonicalBuild, 4)
                .unwrap()
                .phase,
            EtaPhase::TargetBuild
        );
        view.candidate.registry_order = "first_seen".into();
        assert_eq!(
            eta_work(&view.candidate, Phase::CanonicalBuild, 4)
                .unwrap()
                .phase,
            EtaPhase::SourceControlBuild
        );
        view.candidate.registry_order = "unknown".into();
        assert!(eta_work(&view.candidate, Phase::CanonicalBuild, 4).is_none());
    }

    #[test]
    fn seeded_calibration_produces_a_full_mixed_queue_eta() {
        let args = adaptive_args();
        let estimator = EtaEstimator::from_history(&EtaHistory::new(builtin_eta_seeds())).unwrap();
        let mut pair = candidate_view(Phase::TargetBuild);
        pair.candidate.input_bytes = 100_000_000_000;
        let mut usage_sorted = candidate_view(Phase::TargetBuild);
        usage_sorted.candidate.mode = CandidateMode::CanonicalDirect;
        usage_sorted.candidate.epoch = 700;
        usage_sorted.candidate.registry_order = "usage_sorted".into();
        usage_sorted.candidate.input_bytes = 100_000_000_000;
        usage_sorted.phase = Phase::CanonicalBuild;
        let mut first_seen = usage_sorted.clone();
        first_seen.candidate.epoch = 701;
        first_seen.candidate.registry_order = "first_seen".into();
        let views = vec![pair, usage_sorted, first_seen];

        let eta = full_queue_eta_secs(&args, &views, &BTreeMap::new(), Some(&estimator)).unwrap();
        assert!(eta > 3.0 * args.resume_stable_secs as f64);
        let mut full_gap_args = adaptive_args();
        full_gap_args.clean_transition_secs = full_gap_args.resume_stable_secs;
        let full_gap_eta =
            full_queue_eta_secs(&full_gap_args, &views, &BTreeMap::new(), Some(&estimator))
                .unwrap();
        // Five queued phases pay the steady gap. The short model keeps one cold 60-second
        // startup, so only the other four phase transitions become 50 seconds shorter.
        assert_eq!(full_gap_eta - eta, 200.0);
        let status = build_status(
            &args,
            &views,
            &[],
            &BTreeMap::new(),
            2,
            Some(&estimator),
            (3, 3, 0, 0),
            None,
        );
        assert_eq!(status.queue_eta_secs, Some(eta));
    }

    fn test_active_child(candidate: Candidate, phase: Phase, worker_threads: u32) -> ActiveChild {
        let child = Command::new("/usr/bin/true").spawn().unwrap();
        ActiveChild {
            candidate,
            phase,
            pid: child.id(),
            child,
            start_ticks: u64::MAX,
            log_path: PathBuf::from("/tmp/firewatch-eta-test.log"),
            paused: false,
            pause_reason: None,
            owned_temp_path: None,
            io_sample: None,
            telemetry: ProcessTelemetry::default(),
            worker_threads,
            started_unix_secs: unix_now(),
            started_at: Instant::now(),
            paused_started_at: None,
            paused_secs: Duration::ZERO,
        }
    }

    #[cfg(target_os = "linux")]
    fn live_authority_test_child(args: &Args, candidate: Candidate) -> ActiveChild {
        let mut command = Command::new("/bin/sleep");
        command.arg("30").process_group(0);
        let child = command.spawn().unwrap();
        let pid = child.id();
        let deadline = Instant::now() + Duration::from_secs(3);
        let start_ticks = loop {
            if let Some(start_ticks) = process_start_ticks(pid) {
                break start_ticks;
            }
            assert!(Instant::now() < deadline, "test child identity is absent");
            thread::sleep(Duration::from_millis(10));
        };
        let marker = WorkflowMarker {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            epoch: candidate.epoch,
            source_generation_sha256: candidate.source_generation.clone(),
            target_generation_sha256: candidate.target_generation.clone(),
            source_effective_input_sha256: Some(candidate.source_effective_input.clone()),
            target_effective_input_sha256: Some(candidate.target_effective_input.clone()),
            source_wire_profile: Some(candidate.source_wire_profile),
            target_wire_profile: Some(candidate.target_wire_profile),
            authority_binding_sha256: Some(candidate.authority_binding_sha256.clone()),
            retry_of_failed_marker_sha256: None,
            state: "running".into(),
            phase: Phase::TargetBuild,
            created_unix_secs: 1,
            updated_unix_secs: 1,
            attempt_id: Some("a".repeat(32)),
            pid: Some(pid),
            process_start_ticks: Some(start_ticks),
            executable: None,
            executable_dev: None,
            executable_ino: None,
            argv: Vec::new(),
            log_path: None,
            auto_paused: false,
            auto_pause_reason: None,
            owned_temp_path: None,
            cleanup_owner_absence_confirmed: false,
            message: None,
        };
        publish_json_atomic(&candidate.workflow_path(args), &marker).unwrap();
        ActiveChild {
            candidate,
            phase: Phase::TargetBuild,
            child,
            pid,
            start_ticks,
            log_path: args.controller_state_root.join("authority-test.log"),
            paused: false,
            pause_reason: None,
            owned_temp_path: None,
            io_sample: None,
            telemetry: ProcessTelemetry::default(),
            worker_threads: 1,
            started_unix_secs: unix_now(),
            started_at: Instant::now(),
            paused_started_at: None,
            paused_secs: Duration::ZERO,
        }
    }

    #[test]
    fn authority_replacement_in_marker_to_spawn_gap_prevents_child_start() {
        for same_value_new_inode in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let root = fs::canonicalize(directory.path()).unwrap();
            let mut args = direct_test_args(&root);
            args.indexer_bin = PathBuf::from("/usr/bin/true");
            let mut candidate = candidate_view(Phase::TargetBuild).candidate;
            let authority_path = bind_test_authority(&mut candidate, &root);
            let exact_bytes = fs::read(&authority_path).unwrap();

            let result =
                spawn_phase_with_pre_spawn(&args, &candidate, Phase::TargetBuild, 1, || {
                    if same_value_new_inode {
                        let replacement = authority_path.with_extension("replacement");
                        fs::write(&replacement, &exact_bytes)?;
                        fs::rename(&replacement, &authority_path)?;
                    } else {
                        fs::write(&authority_path, b"invalid test authority")?;
                    }
                    Ok(())
                });

            assert!(result.is_err(), "spawn gap unexpectedly succeeded");
            assert!(
                candidate.workflow_path(&args).exists(),
                "spawn failed before marker setup: {:#}",
                result.unwrap_err()
            );
            let marker: WorkflowMarker =
                read_bounded_json(&candidate.workflow_path(&args)).unwrap();
            assert_eq!(marker.state, "starting");
            assert_eq!(
                marker.authority_binding_sha256.as_deref(),
                Some(candidate.authority_binding_sha256.as_str())
            );
            assert!(marker.pid.is_none());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn in_flight_authority_replacement_stops_and_fails_the_exact_child() {
        for same_value_new_inode in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let root = fs::canonicalize(directory.path()).unwrap();
            let args = direct_test_args(&root);
            let mut candidate = candidate_view(Phase::TargetBuild).candidate;
            let authority_path = bind_test_authority(&mut candidate, &root);
            let exact_bytes = fs::read(&authority_path).unwrap();
            let child = live_authority_test_child(&args, candidate.clone());
            let pid = child.pid;
            let start_ticks = child.start_ticks;
            let mut pool = PoolState::default();
            pool.children.insert(candidate.epoch, child);

            if same_value_new_inode {
                let replacement = authority_path.with_extension("replacement");
                fs::write(&replacement, exact_bytes).unwrap();
                fs::rename(&replacement, &authority_path).unwrap();
            } else {
                fs::write(&authority_path, b"invalid test authority").unwrap();
            }

            let reason = fail_children_with_invalid_authority(&args, &mut pool)
                .unwrap()
                .unwrap();
            assert!(reason.contains(&candidate.epoch.to_string()));
            assert!(pool.children.is_empty());
            assert_ne!(process_start_ticks(pid), Some(start_ticks));
            let marker: WorkflowMarker =
                read_bounded_json(&candidate.workflow_path(&args)).unwrap();
            assert_eq!(marker.state, "failed");
            assert_eq!(
                marker.authority_binding_sha256.as_deref(),
                Some(candidate.authority_binding_sha256.as_str())
            );
            assert!(marker.pid.is_none());
        }
    }

    #[test]
    fn active_row_and_two_thread_queue_eta_are_available() {
        let args = adaptive_args();
        let estimator = EtaEstimator::from_history(&EtaHistory::new(builtin_eta_seeds())).unwrap();
        let mut view = candidate_view(Phase::TargetBuild);
        view.candidate.input_bytes = 100_000_000_000;
        let child = test_active_child(view.candidate.clone(), Phase::TargetBuild, 2);
        let mut active = BTreeMap::new();
        active.insert(view.candidate.epoch, child);

        let status = build_status(
            &args,
            &[view],
            &[],
            &active,
            2,
            Some(&estimator),
            (1, 1, 0, 0),
            None,
        );
        assert!(status.queue_eta_secs.is_some());
        assert!(status.rows[0].eta_secs.is_some());
    }

    #[test]
    fn successful_sample_append_preserves_seed_groups() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let mut view = candidate_view(Phase::TargetBuild);
        view.candidate.input_bytes = 100;
        let child = test_active_child(view.candidate, Phase::TargetBuild, 4);

        record_successful_eta_sample(&args, &child).unwrap();
        let history: EtaHistory = read_bounded_json(&eta_history_path(&args)).unwrap();
        assert!(history.samples.iter().any(|sample| {
            sample.epoch == 301
                && sample.phase == EtaPhase::TargetBuild
                && sample.input_bytes == 100
        }));
        let estimator = EtaEstimator::from_history(&history).unwrap();
        assert!(estimator.sample_count(EtaPhase::Parity, 4) > 0);
    }

    #[test]
    fn corrupt_eta_history_fails_open_at_the_estimator_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        fs::write(eta_history_path(&args), b"not-json").unwrap();
        let estimator = load_or_initialize_eta_history(&args)
            .and_then(|history| {
                EtaEstimator::from_history(&history).context("build test ETA calibration")
            })
            .ok();
        assert!(estimator.is_none());

        let mut view = candidate_view(Phase::TargetBuild);
        view.candidate.input_bytes = 100;
        let status = build_status(
            &args,
            &[view],
            &[],
            &BTreeMap::new(),
            1,
            estimator.as_ref(),
            (1, 1, 0, 0),
            Some("ETA history is unavailable".into()),
        );
        assert_eq!(status.epochs_queued, 1);
        assert_eq!(status.queue_eta_secs, None);
    }

    #[test]
    fn schema4_controller_consumes_producer_evidence_and_requires_exact_v3_post_binding() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let fixture = write_schema4_pair_fixture(&args, 710);
        let blocked = discover_candidates(&args, &fixture.status).unwrap();
        assert!(blocked.candidates.is_empty());
        assert_eq!(blocked.blocked_wire_profiles.len(), 1);
        assert_eq!(blocked.blocked_wire_profiles[0].inputs.len(), 1);
        assert_eq!(
            blocked.blocked_wire_profiles[0].inputs[0].generation_kind,
            RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND
        );
        attest_schema4_source(&args, &fixture, 710);
        let discovery = discover_candidates(&args, &fixture.status).unwrap();
        assert_eq!(discovery.candidates.len(), 1);
        assert!(discovery.blocked_wire_profiles.is_empty());
        assert_eq!(
            discovery.candidates[0].source_wire_profile,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        );
        assert_eq!(
            discovery.candidates[0].target_wire_profile,
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        );

        let attestation_path = fs::read_dir(
            args.controller_state_root
                .join(WIRE_PROFILE_ATTESTATIONS_DIR),
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
        let mut attestation: serde_json::Value = read_bounded_json(&attestation_path).unwrap();
        let exact_evidence = attestation["evidence"].clone();
        attestation["evidence"] = serde_json::json!("arbitrary printable evidence");
        fs::write(
            &attestation_path,
            serde_json::to_vec_pretty(&attestation).unwrap(),
        )
        .unwrap();
        assert!(discover_candidates(&args, &fixture.status).is_err());
        attestation["evidence"] = exact_evidence;
        fs::write(
            &attestation_path,
            serde_json::to_vec_pretty(&attestation).unwrap(),
        )
        .unwrap();

        attestation["wire_profile"] = serde_json::json!("pre-unknown-instruction-fallbacks-v1");
        fs::write(
            &attestation_path,
            serde_json::to_vec_pretty(&attestation).unwrap(),
        )
        .unwrap();
        assert!(discover_candidates(&args, &fixture.status).is_err());
        attestation["wire_profile"] = serde_json::json!("post-unknown-instruction-fallbacks-v1");
        fs::write(
            &attestation_path,
            serde_json::to_vec_pretty(&attestation).unwrap(),
        )
        .unwrap();

        for (field, bad) in [
            (
                "wire_profile",
                serde_json::json!("pre-unknown-instruction-fallbacks-v1"),
            ),
            ("phase", serde_json::json!("core")),
            ("attempt_id", serde_json::json!("short")),
            ("attempt_id", serde_json::Value::Null),
            ("staging_dir", serde_json::json!("/wrong/staging")),
            ("handoff_sha256", serde_json::json!("not-a-sha")),
            ("handoff_sha256", serde_json::Value::Null),
            ("expected_access_state", serde_json::json!("receipt_ready")),
            ("threads", serde_json::json!(0)),
            ("pid", serde_json::json!(42)),
            ("process_start_ticks", serde_json::json!(9)),
            ("audit_retry_is_safe", serde_json::json!(true)),
            ("audit_is_continuation", serde_json::json!(true)),
            ("source", serde_json::json!("/wrong/source")),
            ("target", serde_json::json!("/wrong/target")),
        ] {
            let mut marker: serde_json::Value = read_bounded_json(&fixture.marker_path).unwrap();
            let original = marker[field].clone();
            marker[field] = bad;
            fs::write(
                &fixture.marker_path,
                serde_json::to_vec_pretty(&marker).unwrap(),
            )
            .unwrap();
            assert!(
                discover_candidates(&args, &fixture.status).is_err(),
                "schema-4 marker field {field} must fail closed"
            );
            marker[field] = original;
            fs::write(
                &fixture.marker_path,
                serde_json::to_vec_pretty(&marker).unwrap(),
            )
            .unwrap();
        }

        for (field, bad) in [
            ("version", serde_json::json!(2)),
            ("algorithm", serde_json::json!("other")),
            (
                "wire_profile",
                serde_json::json!("pre-unknown-instruction-fallbacks-v1"),
            ),
            ("attempt_id", serde_json::json!("ef".repeat(16))),
            ("attempt_id", serde_json::Value::Null),
            ("handoff_sha256", serde_json::json!("ef".repeat(32))),
            ("handoff_sha256", serde_json::Value::Null),
            ("threads", serde_json::json!(7)),
            ("source_dir", serde_json::json!("/wrong/source")),
            ("target_dir", serde_json::json!("/wrong/target")),
            (
                "source_generation_sha256",
                serde_json::json!("ef".repeat(32)),
            ),
        ] {
            let mut receipt: serde_json::Value = read_bounded_json(&fixture.receipt_path).unwrap();
            let original = receipt[field].clone();
            receipt[field] = bad;
            fs::write(
                &fixture.receipt_path,
                serde_json::to_vec_pretty(&receipt).unwrap(),
            )
            .unwrap();
            assert!(
                discover_candidates(&args, &fixture.status).is_err(),
                "schema-4 receipt field {field} must fail closed"
            );
            receipt[field] = original;
            fs::write(
                &fixture.receipt_path,
                serde_json::to_vec_pretty(&receipt).unwrap(),
            )
            .unwrap();
        }
    }

    #[test]
    fn legacy_schema2_and_schema3_complete_marker_branch_is_unchanged() {
        let target = PathBuf::from("/target/epoch-700");
        for (schema_version, phase) in [(2, None), (3, Some("access"))] {
            let marker: RegistryMarker = serde_json::from_value(serde_json::json!({
                "schema_version": schema_version,
                "kind": "archive_v2_registry_reprocess",
                "epoch": 700,
                "state": "complete",
                "phase": phase,
                "source": "/source/epoch-700",
                "target": target,
                "pid": null,
                "process_start_ticks": null
            }))
            .unwrap();
            assert_eq!(
                validate_complete_registry_contract(700, &marker, &target).unwrap(),
                CompleteRegistryContract::Legacy
            );
        }
    }

    #[test]
    fn schema4_complete_pair_rejects_source_poh_staging_temp_and_inode_races() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let fixture = write_schema4_pair_fixture(&args, 711);

        let mut receipt_permissions = fs::metadata(&fixture.receipt_path).unwrap().permissions();
        receipt_permissions.set_mode(0o666);
        fs::set_permissions(&fixture.receipt_path, receipt_permissions).unwrap();
        assert!(discover_candidates(&args, &fixture.status).is_err());
        let mut receipt_permissions = fs::metadata(&fixture.receipt_path).unwrap().permissions();
        receipt_permissions.set_mode(0o644);
        fs::set_permissions(&fixture.receipt_path, receipt_permissions).unwrap();

        let marker_hard_link = fixture.marker_path.with_extension("hard-link");
        fs::hard_link(&fixture.marker_path, &marker_hard_link).unwrap();
        assert!(discover_candidates(&args, &fixture.status).is_err());
        fs::remove_file(marker_hard_link).unwrap();

        let mut receipt: serde_json::Value = read_bounded_json(&fixture.receipt_path).unwrap();
        let poh = receipt["source_files"][ARCHIVE_V2_POH_FILE].take();
        receipt["source_files"]
            .as_object_mut()
            .unwrap()
            .remove(ARCHIVE_V2_POH_FILE);
        receipt["source_generation_sha256"] = serde_json::json!(registry_generation_digest(
            &serde_json::from_value(receipt["source_files"].clone()).unwrap()
        ));
        fs::write(
            &fixture.receipt_path,
            serde_json::to_vec_pretty(&receipt).unwrap(),
        )
        .unwrap();
        assert!(discover_candidates(&args, &fixture.status).is_err());
        receipt["source_files"][ARCHIVE_V2_POH_FILE] = poh;
        let source_files: BTreeMap<String, RegistryFileBinding> =
            serde_json::from_value(receipt["source_files"].clone()).unwrap();
        receipt["source_generation_sha256"] =
            serde_json::json!(registry_generation_digest(&source_files));
        fs::write(
            &fixture.receipt_path,
            serde_json::to_vec_pretty(&receipt).unwrap(),
        )
        .unwrap();

        let post_marker =
            wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
        let post_binding = receipt["target_files"][&post_marker.name].take();
        receipt["target_files"]
            .as_object_mut()
            .unwrap()
            .remove(&post_marker.name);
        let target_files: BTreeMap<String, RegistryFileBinding> =
            serde_json::from_value(receipt["target_files"].clone()).unwrap();
        receipt["target_generation_sha256"] =
            serde_json::json!(registry_generation_digest(&target_files));
        fs::write(
            &fixture.receipt_path,
            serde_json::to_vec_pretty(&receipt).unwrap(),
        )
        .unwrap();
        assert!(discover_candidates(&args, &fixture.status).is_err());
        receipt["target_files"][&post_marker.name] = post_binding;
        let target_files: BTreeMap<String, RegistryFileBinding> =
            serde_json::from_value(receipt["target_files"].clone()).unwrap();
        receipt["target_generation_sha256"] =
            serde_json::json!(registry_generation_digest(&target_files));
        fs::write(
            &fixture.receipt_path,
            serde_json::to_vec_pretty(&receipt).unwrap(),
        )
        .unwrap();

        let post_marker_path = fixture.target.join(&post_marker.name);
        let exact_post_marker = fs::read(&post_marker_path).unwrap();
        let mut corrupt_post_marker = exact_post_marker.clone();
        corrupt_post_marker[0] ^= 0x01;
        fs::write(&post_marker_path, corrupt_post_marker).unwrap();
        assert!(discover_candidates(&args, &fixture.status).is_err());
        fs::write(&post_marker_path, exact_post_marker).unwrap();

        let exact_receipt: RegistryReceipt = read_bounded_json(&fixture.receipt_path).unwrap();
        let target_identities =
            validate_receipt_files(&fixture.target, &exact_receipt.target_files).unwrap();
        let post_marker_evidence = capture_schema4_target_post_marker(
            &fixture.target,
            &exact_receipt.target_files,
            &target_identities,
        )
        .unwrap();
        let replacement = post_marker_path.with_extension("replacement");
        fs::write(&replacement, fs::read(&post_marker_path).unwrap()).unwrap();
        fs::rename(&replacement, &post_marker_path).unwrap();
        assert!(
            post_marker_evidence
                .recheck(true, "schema-4 target Post marker replacement test")
                .is_err()
        );

        fs::create_dir(&fixture.staging_path).unwrap();
        assert!(discover_candidates(&args, &fixture.status).is_err());
        fs::remove_dir(&fixture.staging_path).unwrap();
        let temp = fixture.target.join(format!(
            ".{REGISTRY_RECEIPT_FILE}{REGISTRY_ACCESS_TEMP_SUFFIX}"
        ));
        fs::write(&temp, b"temp").unwrap();
        assert!(discover_candidates(&args, &fixture.status).is_err());
        fs::remove_file(temp).unwrap();

        let marker_capture: PinnedJson<RegistryMarker> =
            read_pinned_json(&fixture.marker_path, "test schema-4 marker").unwrap();
        let marker_bytes = fs::read(&fixture.marker_path).unwrap();
        let marker_replacement = fixture.marker_path.with_extension("replacement");
        fs::write(&marker_replacement, marker_bytes).unwrap();
        fs::rename(&marker_replacement, &fixture.marker_path).unwrap();
        assert!(
            marker_capture
                .evidence
                .recheck(true, "test schema-4 marker")
                .is_err()
        );

        let receipt_capture: PinnedJson<RegistryReceipt> =
            read_pinned_json(&fixture.receipt_path, "test schema-4 receipt").unwrap();
        let receipt_bytes = fs::read(&fixture.receipt_path).unwrap();
        let receipt_replacement = fixture.receipt_path.with_extension("replacement");
        fs::write(&receipt_replacement, receipt_bytes).unwrap();
        fs::rename(&receipt_replacement, &fixture.receipt_path).unwrap();
        assert!(
            receipt_capture
                .evidence
                .recheck(true, "test schema-4 receipt")
                .is_err()
        );
    }

    #[test]
    fn schema4_candidate_binding_changes_for_same_value_authority_replacements() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let fixture = write_schema4_pair_fixture(&args, 712);
        attest_schema4_source(&args, &fixture, 712);
        let attestation_path = fs::read_dir(
            args.controller_state_root
                .join(WIRE_PROFILE_ATTESTATIONS_DIR),
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
        let post_marker =
            wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
        let post_marker_path = fixture.target.join(post_marker.name);

        let mut prior = discover_candidates(&args, &fixture.status)
            .unwrap()
            .candidates[0]
            .authority_binding_sha256
            .clone();
        for path in [
            &attestation_path,
            &fixture.marker_path,
            &fixture.receipt_path,
            &post_marker_path,
        ] {
            let replacement = path.with_extension(format!(
                "{}.replacement",
                path.extension()
                    .and_then(|extension| extension.to_str())
                    .unwrap_or("file")
            ));
            fs::write(&replacement, fs::read(path).unwrap()).unwrap();
            fs::rename(&replacement, path).unwrap();
            let current = discover_candidates(&args, &fixture.status)
                .unwrap()
                .candidates[0]
                .authority_binding_sha256
                .clone();
            assert_ne!(current, prior);
            prior = current;
        }
    }

    #[test]
    fn pair_source_and_target_attestations_bind_their_receipt_generations() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let source = args.archive_root.join("epoch-700");
        let target = args.usage_sorted_root.join("epoch-700");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&target).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"src").unwrap();
        fs::write(target.join(ARCHIVE_V2_BLOCKS_FILE), b"dst").unwrap();
        let source_files = BTreeMap::from([(
            ARCHIVE_V2_BLOCKS_FILE.to_string(),
            RegistryFileBinding {
                bytes: 3,
                sha256: "a".repeat(64),
            },
        )]);
        let target_files = BTreeMap::from([(
            ARCHIVE_V2_BLOCKS_FILE.to_string(),
            RegistryFileBinding {
                bytes: 3,
                sha256: "b".repeat(64),
            },
        )]);
        fs::write(
            args.scheduler_state_root
                .join("registry_reprocess/epoch-700.json"),
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 3,
                "kind": "archive_v2_registry_reprocess",
                "epoch": 700,
                "state": "complete",
                "phase": "access",
                "source": source,
                "target": target,
                "pid": null,
                "process_start_ticks": null
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            target.join("archive-v2-registry-reprocess.receipt.json"),
            serde_json::to_vec(&serde_json::json!({
                "version": 3,
                "algorithm": "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3",
                "epoch": 700,
                "source_dir": source,
                "target_dir": target,
                "source_generation_sha256": registry_generation_digest(&source_files),
                "target_generation_sha256": registry_generation_digest(&target_files),
                "source_files": source_files,
                "target_files": target_files
            }))
            .unwrap(),
        )
        .unwrap();
        let mut status = scheduler(8 * GIB, 0.0, 0.0);
        status.epochs = vec![scheduler_epoch(700, "first_seen")];

        // Discovery stays fail-closed until both exact generations have profile attestations.
        let blocked = discover_candidates(&args, &status).unwrap();
        assert!(blocked.candidates.is_empty());
        assert_eq!(blocked.blocked_wire_profiles.len(), 1);
        assert_eq!(blocked.blocked_wire_profiles[0].inputs.len(), 2);
        assert_eq!(
            blocked.blocked_wire_profiles[0].inputs[0].generation_kind,
            RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND
        );
        assert_eq!(
            blocked.blocked_wire_profiles[0].inputs[1].generation_kind,
            RECEIPT_TARGET_ATTESTATION_GENERATION_KIND
        );

        for (archive, order, generation, generation_kind) in [
            (
                source.clone(),
                "first_seen",
                registry_generation_digest(&source_files),
                RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND,
            ),
            (
                target.clone(),
                "usage_sorted",
                registry_generation_digest(&target_files),
                RECEIPT_TARGET_ATTESTATION_GENERATION_KIND,
            ),
        ] {
            let receipt_files = if order == "first_seen" {
                &source_files
            } else {
                &target_files
            };
            let archive_files = validate_receipt_files(&archive, receipt_files).unwrap();
            let attestation = WireProfileAttestation {
                schema_version: WIRE_PROFILE_ATTESTATION_SCHEMA_VERSION,
                kind: "archive_v2_wire_profile_attestation".into(),
                audit_algorithm: WIRE_PROFILE_AUDIT_ALGORITHM.into(),
                audited_profiles: WIRE_PROFILE_AUDITED_PROFILES,
                cluster_id: "mainnet-beta".into(),
                epoch: 700,
                archive,
                registry_order: order.into(),
                generation_kind: generation_kind.into(),
                content_generation_sha256: generation.clone(),
                archive_files,
                wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                evidence: if generation_kind == RECEIPT_SOURCE_ATTESTATION_GENERATION_KIND {
                    valid_receipt_source_evidence()
                } else {
                    valid_receipt_target_evidence()
                },
                attested_unix_secs: 1,
            };
            publish_json_no_replace(
                &args
                    .controller_state_root
                    .join(WIRE_PROFILE_ATTESTATIONS_DIR)
                    .join(format!("epoch-700-{generation}.json")),
                &attestation,
            )
            .unwrap();
        }

        let discovery = discover_candidates(&args, &status).unwrap();
        assert_eq!(discovery.candidates.len(), 1);
        let candidate = &discovery.candidates[0];
        assert_eq!(candidate.mode, CandidateMode::MigrationPair);
        assert_eq!(candidate.input_bytes(), 3);
        candidate
            .ensure_profile_bound_paths_do_not_alias_legacy(&args)
            .unwrap();

        // A pre-profile pair acceptance is not adopted. It stays immutable,
        // while the attested candidate becomes runnable at new paths.
        fs::create_dir(candidate.epoch_root(&args)).unwrap();
        let legacy_source_index = candidate.legacy_source_index(&args);
        let legacy_target_index = candidate.legacy_target_index(&args);
        fs::create_dir(&legacy_source_index).unwrap();
        fs::create_dir(&legacy_target_index).unwrap();
        fs::write(legacy_source_index.join("manifest.json"), b"legacy-source").unwrap();
        fs::write(legacy_target_index.join("manifest.json"), b"legacy-target").unwrap();
        let legacy_acceptance = b"{\"schema_version\":1,\"epoch\":700}";
        fs::write(
            candidate.legacy_pair_acceptance_path(&args),
            legacy_acceptance,
        )
        .unwrap();
        publish_json_atomic(
            &candidate.workflow_path(&args),
            &legacy_accepted_marker(candidate, Phase::Parity),
        )
        .unwrap();

        let view = view_candidate(
            &args,
            candidate,
            &BTreeMap::new(),
            &mut PairAcceptanceVerificationCache::default(),
        )
        .unwrap();
        assert_eq!(view.state, "queued");
        assert_eq!(view.phase, Phase::TargetBuild);
        assert!(view.acceptance.is_none());
        assert!(view.target_manifest.is_none());
        assert!(view.source_manifest.is_none());
        assert_eq!(
            fs::read(legacy_source_index.join("manifest.json")).unwrap(),
            b"legacy-source"
        );
        assert_eq!(
            fs::read(legacy_target_index.join("manifest.json")).unwrap(),
            b"legacy-target"
        );
        assert_eq!(
            fs::read(candidate.legacy_pair_acceptance_path(&args)).unwrap(),
            legacy_acceptance
        );
        assert!(!candidate.acceptance_path(&args).exists());
        let (_, argv) = phase_command(&args, candidate, Phase::TargetBuild, 2);
        assert!(argv.windows(2).any(|pair| {
            pair[0] == "--out" && Path::new(&pair[1]) == candidate.target_index(&args)
        }));
    }

    #[test]
    fn pair_acceptance_rejects_a_shard_mutation_before_receipt_publication() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let (args, candidate, parity) = pair_acceptance_fixture(&root);
        let relations = candidate.target_index(&args).join("shard-0/programs.rel");

        let result = publish_acceptance_with_pre_publish(&args, &candidate, parity, || {
            let mut bytes = fs::read(&relations)?;
            let last = bytes.last_mut().context("test relation file is empty")?;
            *last ^= 1;
            fs::write(&relations, bytes)?;
            Ok(())
        });

        assert!(result.is_err());
        assert!(!candidate.acceptance_path(&args).exists());
    }

    #[test]
    fn pair_acceptance_rejects_same_value_authority_replacement_in_publication_gap() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let (args, candidate, parity) = pair_acceptance_fixture(&root);
        let authority_path = candidate.authority_proofs.as_ref().unwrap().proofs[0]
            .evidence
            .path
            .clone();
        let exact_bytes = fs::read(&authority_path).unwrap();

        let result = publish_acceptance_with_pre_publish(&args, &candidate, parity, || {
            let replacement = authority_path.with_extension("replacement");
            fs::write(&replacement, &exact_bytes)?;
            fs::rename(&replacement, &authority_path)?;
            Ok(())
        });

        assert!(result.is_err());
        assert!(!candidate.acceptance_path(&args).exists());
    }

    #[test]
    fn direct_acceptance_rejects_same_value_authority_replacement_in_publication_gap() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let (args, candidate, manifest) = direct_acceptance_fixture(&root);
        let authority_path = candidate.authority_proofs.as_ref().unwrap().proofs[0]
            .evidence
            .path
            .clone();
        let exact_bytes = fs::read(&authority_path).unwrap();

        let result =
            publish_direct_acceptance_with_pre_publish(&args, &candidate, &manifest, || {
                let replacement = authority_path.with_extension("replacement");
                fs::write(&replacement, &exact_bytes)?;
                fs::rename(&replacement, &authority_path)?;
                Ok(())
            });

        assert!(result.is_err());
        assert!(!candidate.acceptance_path(&args).exists());
    }

    #[test]
    fn pair_accepted_state_revalidates_changed_shards_and_keeps_steady_polls_light() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let (args, candidate, parity) = pair_acceptance_fixture(&root);
        publish_acceptance(&args, &candidate, parity).unwrap();
        let source = exact_index_manifest(
            &candidate.source_index(&args),
            &candidate.source,
            candidate.epoch,
            &candidate.source_effective_input,
            candidate.source_wire_profile,
            false,
        )
        .unwrap()
        .unwrap();
        let target = exact_index_manifest(
            &candidate.target_index(&args),
            &candidate.target,
            candidate.epoch,
            &candidate.target_effective_input,
            candidate.target_wire_profile,
            false,
        )
        .unwrap()
        .unwrap();
        let mut cache = PairAcceptanceVerificationCache::default();

        assert!(
            valid_acceptance(&args, &candidate, Some(&target), Some(&source), &mut cache)
                .unwrap()
                .is_some()
        );
        assert!(
            valid_acceptance(&args, &candidate, Some(&target), Some(&source), &mut cache)
                .unwrap()
                .is_some()
        );
        assert_eq!(cache.entries.len(), 1);

        let relations = candidate.source_index(&args).join("shard-0/programs.rel");
        let mut bytes = fs::read(&relations).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&relations, bytes).unwrap();
        assert!(
            valid_acceptance(&args, &candidate, Some(&target), Some(&source), &mut cache).is_err()
        );
    }

    #[test]
    fn canonical_command_and_output_are_mode_explicit_and_collision_free() {
        let args = adaptive_args();
        let candidate = Candidate {
            mode: CandidateMode::CanonicalDirect,
            epoch: 700,
            source: PathBuf::from("/archive/epoch-700"),
            target: PathBuf::from("/archive/epoch-700"),
            source_generation: "a".repeat(64),
            target_generation: "a".repeat(64),
            source_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            target_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            source_effective_input: "b".repeat(64),
            target_effective_input: "b".repeat(64),
            authority_binding_sha256: "e".repeat(64),
            authority_proofs: None,
            registry_order: "first_seen".into(),
            direct_files: Some(BTreeMap::new()),
            input_bytes: 123,
            retry_of_failed_marker_sha256: None,
        };
        let (_, argv) = phase_command(&args, &candidate, Phase::CanonicalBuild, 2);
        assert!(
            argv.windows(2)
                .any(|pair| { pair[0] == "--generation-id" && pair[1] == "b".repeat(64) })
        );
        assert!(argv.windows(2).any(|pair| {
            pair[0] == "--wire-profile" && pair[1] == ArchiveV2WireProfile::POST_UNKNOWN_NAME
        }));
        assert!(argv.iter().any(|arg| arg == "--trust-local"));
        assert!(
            candidate
                .direct_index(&args)
                .to_string_lossy()
                .contains("canonical-first_seen-")
        );
        assert_ne!(
            candidate.direct_index(&args),
            candidate.epoch_root(&args).join(format!(
                "target-usage-sorted-{}",
                candidate.target_generation
            ))
        );
        candidate
            .ensure_profile_bound_paths_do_not_alias_legacy(&args)
            .unwrap();
        let mut aliased = candidate.clone();
        aliased.source_effective_input = aliased.source_generation.clone();
        aliased.target_effective_input = aliased.target_generation.clone();
        assert!(
            aliased
                .ensure_profile_bound_paths_do_not_alias_legacy(&args)
                .is_err()
        );
    }

    #[test]
    fn index_profile_change_produces_distinct_immutable_output_paths() {
        let args = adaptive_args();
        let generation = "a".repeat(64);
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let current = effective_input_digest(&generation, profile).unwrap();
        let prior = legacy_profile_bound_effective_input_digest(&generation, profile).unwrap();
        assert_eq!(
            current,
            "818eb64ed295a18e16c91fa6eddd942c02cb92630cdb507765614a1ee908368c"
        );
        assert_ne!(current, prior);
        assert_ne!(
            current,
            effective_input_digest_for_index_profile(
                &generation,
                profile,
                MANIFEST_SCHEMA_VERSION,
                FORMAT_VERSION - 1,
                SEMANTICS_VERSION - 1,
            )
            .unwrap()
        );

        let candidate = Candidate {
            mode: CandidateMode::MigrationPair,
            epoch: 700,
            source: PathBuf::from("/archive/epoch-700"),
            target: PathBuf::from("/archive/usage-sorted/epoch-700"),
            source_generation: generation.clone(),
            target_generation: generation,
            source_wire_profile: profile,
            target_wire_profile: profile,
            source_effective_input: current.clone(),
            target_effective_input: current,
            authority_binding_sha256: "e".repeat(64),
            authority_proofs: None,
            registry_order: "usage_sorted".into(),
            direct_files: None,
            input_bytes: 123,
            retry_of_failed_marker_sha256: None,
        };
        let mut prior_candidate = candidate.clone();
        prior_candidate.source_effective_input = prior.clone();
        prior_candidate.target_effective_input = prior;

        assert_ne!(
            candidate.source_index(&args),
            prior_candidate.source_index(&args)
        );
        assert_ne!(
            candidate.target_index(&args),
            prior_candidate.target_index(&args)
        );
        assert_ne!(
            candidate.acceptance_path(&args),
            prior_candidate.acceptance_path(&args)
        );
        let mut prior_marker = legacy_accepted_marker(&candidate, Phase::TargetBuild);
        prior_marker.schema_version = WORKFLOW_SCHEMA_VERSION;
        prior_marker.state = "failed".into();
        prior_marker.source_effective_input_sha256 =
            Some(prior_candidate.source_effective_input.clone());
        prior_marker.target_effective_input_sha256 =
            Some(prior_candidate.target_effective_input.clone());
        prior_marker.source_wire_profile = Some(profile);
        prior_marker.target_wire_profile = Some(profile);
        assert!(!marker_content_matches_candidate(&prior_marker, &candidate));

        let mut direct = candidate;
        direct.mode = CandidateMode::CanonicalDirect;
        direct.direct_files = Some(BTreeMap::new());
        let mut prior_direct = prior_candidate;
        prior_direct.mode = CandidateMode::CanonicalDirect;
        prior_direct.direct_files = Some(BTreeMap::new());
        assert_ne!(direct.direct_index(&args), prior_direct.direct_index(&args));
        assert_ne!(
            direct.acceptance_path(&args),
            prior_direct.acceptance_path(&args)
        );
        prior_marker.phase = Phase::CanonicalBuild;
        assert!(!marker_content_matches_candidate(&prior_marker, &direct));
    }

    #[test]
    fn attested_legacy_direct_acceptance_queues_an_immutable_profile_bound_rebuild() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        write_direct_archive(&args.archive_root, 700);
        attest_direct_archive(&args, 700, "usage_sorted");
        let mut scheduler_status = scheduler(8 * GIB, 0.0, 0.0);
        scheduler_status.epochs = vec![scheduler_epoch(700, "usage_sorted")];
        let discovery = discover_candidates(&args, &scheduler_status).unwrap();
        let candidate = discovery.candidates.first().unwrap();
        assert_eq!(candidate.mode, CandidateMode::CanonicalDirect);
        candidate
            .ensure_profile_bound_paths_do_not_alias_legacy(&args)
            .unwrap();

        fs::create_dir(candidate.epoch_root(&args)).unwrap();
        let legacy_index = candidate.legacy_direct_index(&args);
        fs::create_dir(&legacy_index).unwrap();
        let legacy_manifest = br#"{"schema_version":3,"complete":true}"#;
        fs::write(legacy_index.join("manifest.json"), legacy_manifest).unwrap();
        let legacy_receipt = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "mode": "canonical_direct",
            "epoch": 700,
            "generation_sha256": candidate.target_generation,
            "index": legacy_index
        }))
        .unwrap();
        fs::write(
            candidate.legacy_direct_acceptance_path(&args),
            &legacy_receipt,
        )
        .unwrap();
        publish_json_atomic(
            &candidate.workflow_path(&args),
            &legacy_accepted_marker(candidate, Phase::CanonicalBuild),
        )
        .unwrap();

        let view = view_candidate(
            &args,
            candidate,
            &BTreeMap::new(),
            &mut PairAcceptanceVerificationCache::default(),
        )
        .unwrap();
        assert_eq!(view.state, "queued");
        assert_eq!(view.phase, Phase::CanonicalBuild);
        assert!(view.acceptance.is_none());
        assert!(view.target_manifest.is_none());
        assert!(next_runnable(&[view], &BTreeMap::new()).is_some());
        assert_eq!(
            fs::read(candidate.legacy_direct_index(&args).join("manifest.json")).unwrap(),
            legacy_manifest
        );
        assert_eq!(
            fs::read(candidate.legacy_direct_acceptance_path(&args)).unwrap(),
            legacy_receipt
        );
        assert!(!candidate.direct_index(&args).exists());
        assert!(!candidate.acceptance_path(&args).exists());
        let (_, argv) = phase_command(&args, candidate, Phase::CanonicalBuild, 2);
        assert!(argv.windows(2).any(|pair| {
            pair[0] == "--out" && Path::new(&pair[1]) == candidate.direct_index(&args)
        }));
    }

    #[test]
    fn all_archive_status_is_bounded_well_below_the_monitor_limit() {
        let args = adaptive_args();
        let views = (277..=1012)
            .map(|epoch| CandidateView {
                candidate: Candidate {
                    mode: CandidateMode::CanonicalDirect,
                    epoch,
                    source: PathBuf::from(format!("/archive/epoch-{epoch}")),
                    target: PathBuf::from(format!("/archive/epoch-{epoch}")),
                    source_generation: "a".repeat(64),
                    target_generation: "a".repeat(64),
                    source_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                    target_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                    source_effective_input: "b".repeat(64),
                    target_effective_input: "b".repeat(64),
                    authority_binding_sha256: "e".repeat(64),
                    authority_proofs: None,
                    registry_order: "usage_sorted".into(),
                    direct_files: Some(BTreeMap::new()),
                    input_bytes: 1,
                    retry_of_failed_marker_sha256: None,
                },
                phase: Phase::CanonicalBuild,
                state: "queued".into(),
                acceptance: None,
                target_manifest: None,
                source_manifest: None,
            })
            .collect::<Vec<_>>();
        let status = build_status(
            &args,
            &views,
            &[],
            &BTreeMap::new(),
            2,
            None,
            (736, 736, 0, 0),
            None,
        );
        let encoded = serde_json::to_vec(&status).unwrap();
        assert_eq!(status.epochs_total, 736);
        assert_eq!(status.epochs_eligible, Some(736));
        assert!(encoded.len() < 4 * MIB as usize, "{}", encoded.len());
    }

    #[test]
    fn all_736_unattested_archives_remain_visible_as_profile_audit_required() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let mut scheduler_status = scheduler(8 * GIB, 0.0, 0.0);
        for epoch in 277..=1012 {
            write_direct_archive(&args.archive_root, epoch);
            scheduler_status
                .epochs
                .push(scheduler_epoch(epoch, "usage_sorted"));
        }

        let discovery = discover_candidates(&args, &scheduler_status).unwrap();
        assert!(discovery.candidates.is_empty());
        assert_eq!(discovery.blocked_wire_profiles.len(), 736);
        assert_eq!(discovery.archive_epochs_total, 736);
        assert_eq!(discovery.epochs_eligible, 0);
        assert_eq!(discovery.epochs_blocked_migration, 0);
        assert_eq!(discovery.epochs_blocked_wire_profile, 736);

        let status = build_status(
            &args,
            &[],
            &discovery.blocked_wire_profiles,
            &BTreeMap::new(),
            2,
            None,
            (736, 0, 0, 736),
            None,
        );
        assert_eq!(status.epochs_total, 736);
        assert_eq!(status.epochs_queued, 0);
        assert_eq!(status.epochs_blocked_wire_profile, Some(736));
        assert!(status.rows.iter().all(|row| {
            row.state == "profile_audit_required"
                && row.phase == "wire_profile_audit"
                && row.wire_profile_audit_inputs.len() == 1
                && row.wire_profile_audit_inputs[0].generation_kind
                    == DIRECT_ATTESTATION_GENERATION_KIND
                && is_sha256(&row.wire_profile_audit_inputs[0].content_generation_sha256)
        }));
        let encoded = serde_json::to_vec(&status).unwrap();
        assert!(encoded.len() < MAX_STATUS_JSON_BYTES as usize);
    }

    #[test]
    fn full_native_discovery_and_status_pass_is_metadata_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let mut status = scheduler(8 * GIB, 0.0, 0.0);
        for epoch in 277..=1012 {
            write_direct_archive(&args.archive_root, epoch);
            attest_direct_archive(&args, epoch, "usage_sorted");
            status.epochs.push(scheduler_epoch(epoch, "usage_sorted"));
        }
        let started = Instant::now();
        let discovery = discover_candidates(&args, &status).unwrap();
        let views = discovery
            .candidates
            .iter()
            .map(|candidate| CandidateView {
                candidate: candidate.clone(),
                phase: Phase::CanonicalBuild,
                state: "queued".into(),
                acceptance: None,
                target_manifest: None,
                source_manifest: None,
            })
            .collect::<Vec<_>>();
        let controller = build_status(
            &args,
            &views,
            &discovery.blocked_wire_profiles,
            &BTreeMap::new(),
            2,
            None,
            (
                discovery.archive_epochs_total,
                discovery.epochs_eligible,
                discovery.epochs_blocked_migration,
                discovery.epochs_blocked_wire_profile,
            ),
            None,
        );
        let encoded = serde_json::to_vec_pretty(&controller).unwrap();
        assert_eq!(discovery.candidates.len(), 736);
        assert!(encoded.len() < MAX_STATUS_JSON_BYTES as usize);
        // This is a generous regression bound for metadata-only work, even on a slow CI disk.
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn direct_acceptance_rejects_a_changed_candidate_binding() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let archive = write_direct_archive(&args.archive_root, 700);
        let (files, generation, input_bytes) =
            capture_direct_archive(&archive, 700, "usage_sorted").unwrap();
        let candidate = Candidate {
            mode: CandidateMode::CanonicalDirect,
            epoch: 700,
            source: archive,
            target: args.archive_root.join("epoch-700"),
            source_generation: generation.clone(),
            target_generation: generation.clone(),
            source_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            target_wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            source_effective_input: effective_input_digest(
                &generation,
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            )
            .unwrap(),
            target_effective_input: effective_input_digest(
                &generation,
                ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            )
            .unwrap(),
            authority_binding_sha256: "e".repeat(64),
            authority_proofs: None,
            registry_order: "usage_sorted".into(),
            direct_files: Some(files.clone()),
            input_bytes,
            retry_of_failed_marker_sha256: None,
        };
        fs::create_dir_all(candidate.epoch_root(&args)).unwrap();
        let receipt = DirectAcceptanceReceipt {
            schema_version: DIRECT_ACCEPTANCE_SCHEMA_VERSION,
            mode: "canonical_direct".into(),
            epoch: 700,
            registry_order: "usage_sorted".into(),
            content_generation_sha256: generation,
            effective_input_sha256: candidate.target_effective_input.clone(),
            wire_profile: candidate.target_wire_profile,
            authority_binding_sha256: candidate.authority_binding_sha256.clone(),
            archive: candidate.source.clone(),
            archive_files: files,
            input_bytes,
            index: candidate.direct_index(&args),
            index_manifest_sha256: "a".repeat(64),
            wallets: 1,
            programs: 1,
            transactions_scanned: 1,
            blocks_scanned: 1,
            failed_transactions_excluded: 0,
            accepted_unix_secs: 1,
        };
        publish_json_no_replace(&candidate.acceptance_path(&args), &receipt).unwrap();
        let replacement = candidate.source.join("blocks.replacement");
        fs::write(&replacement, b"blocks").unwrap();
        fs::rename(&replacement, candidate.source.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap();
        let (changed_files, changed_generation, _) =
            capture_direct_archive(&candidate.source, 700, "usage_sorted").unwrap();
        assert_ne!(changed_generation, candidate.target_generation);
        assert_ne!(&changed_files, candidate.direct_files().unwrap());
        let stored: DirectAcceptanceReceipt =
            read_bounded_json(&candidate.acceptance_path(&args)).unwrap();
        assert_ne!(stored.archive_files, changed_files);
        assert_ne!(stored.content_generation_sha256, changed_generation);
    }

    #[test]
    fn direct_receipt_idempotence_ignores_only_acceptance_time() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("archive");
        let index = directory.path().join("index");
        let binding = RegistryFileIdentity {
            size: 1,
            device: 2,
            inode: 3,
            modified_seconds: 4,
            modified_nanoseconds: 5,
            changed_seconds: 6,
            changed_nanoseconds: 7,
        };
        let mut first = DirectAcceptanceReceipt {
            schema_version: DIRECT_ACCEPTANCE_SCHEMA_VERSION,
            mode: "canonical_direct".into(),
            epoch: 700,
            registry_order: "usage_sorted".into(),
            content_generation_sha256: "a".repeat(64),
            effective_input_sha256: "c".repeat(64),
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            authority_binding_sha256: "d".repeat(64),
            archive: path,
            archive_files: BTreeMap::from([("registry.bin".into(), binding)]),
            input_bytes: 1,
            index,
            index_manifest_sha256: "b".repeat(64),
            wallets: 1,
            programs: 2,
            transactions_scanned: 3,
            blocks_scanned: 4,
            failed_transactions_excluded: 5,
            accepted_unix_secs: 10,
        };
        let mut retry = first.clone();
        retry.accepted_unix_secs = 20;
        assert!(direct_receipts_same_immutable_result(&first, &retry));
        let mut changed_profile = retry.clone();
        changed_profile.wire_profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        changed_profile.effective_input_sha256 = "d".repeat(64);
        assert!(!direct_receipts_same_immutable_result(
            &first,
            &changed_profile
        ));
        retry.wallets += 1;
        assert!(!direct_receipts_same_immutable_result(&first, &retry));
        first.accepted_unix_secs = 20;
        assert_ne!(first, retry);
    }

    #[test]
    fn retry_ready_is_bound_to_the_failed_marker_and_exact_wire_profile() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let archive = write_direct_archive(&args.archive_root, 700);
        let (files, generation, input_bytes) =
            capture_direct_archive(&archive, 700, "usage_sorted").unwrap();
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let effective_input = effective_input_digest(&generation, profile).unwrap();
        let candidate = Candidate {
            mode: CandidateMode::CanonicalDirect,
            epoch: 700,
            source: archive.clone(),
            target: archive,
            source_generation: generation.clone(),
            target_generation: generation.clone(),
            source_wire_profile: profile,
            target_wire_profile: profile,
            source_effective_input: effective_input.clone(),
            target_effective_input: effective_input.clone(),
            authority_binding_sha256: "e".repeat(64),
            authority_proofs: None,
            registry_order: "usage_sorted".into(),
            direct_files: Some(files),
            input_bytes,
            retry_of_failed_marker_sha256: None,
        };
        fs::create_dir_all(candidate.epoch_root(&args)).unwrap();
        fs::create_dir(candidate.legacy_direct_index(&args)).unwrap();
        fs::write(
            candidate.legacy_direct_index(&args).join("manifest.json"),
            b"legacy-index-remains-immutable",
        )
        .unwrap();
        fs::write(
            candidate.legacy_direct_acceptance_path(&args),
            b"legacy-acceptance-remains-immutable",
        )
        .unwrap();
        let marker = WorkflowMarker {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            epoch: 700,
            source_generation_sha256: generation.clone(),
            target_generation_sha256: generation.clone(),
            source_effective_input_sha256: Some(effective_input.clone()),
            target_effective_input_sha256: Some(effective_input.clone()),
            source_wire_profile: Some(profile),
            target_wire_profile: Some(profile),
            authority_binding_sha256: Some("e".repeat(64)),
            retry_of_failed_marker_sha256: None,
            state: "failed".into(),
            phase: Phase::CanonicalBuild,
            created_unix_secs: 1,
            updated_unix_secs: 1,
            attempt_id: None,
            pid: None,
            process_start_ticks: None,
            executable: None,
            executable_dev: None,
            executable_ino: None,
            argv: Vec::new(),
            log_path: None,
            auto_paused: false,
            auto_pause_reason: None,
            owned_temp_path: None,
            cleanup_owner_absence_confirmed: false,
            message: Some("decoder failed before output publication".into()),
        };
        publish_json_no_replace(&candidate.workflow_path(&args), &marker).unwrap();
        let failed_marker_sha256 = sha256_control_file(&candidate.workflow_path(&args)).unwrap();
        let mut changed_current_marker = marker.clone();
        changed_current_marker.updated_unix_secs = 2;
        changed_current_marker.message = Some("current marker changed after stale read".into());
        publish_json_atomic(&candidate.workflow_path(&args), &changed_current_marker).unwrap();
        assert!(
            publish_direct_retry_authorization(
                &args,
                &candidate,
                &marker,
                "a stale marker object must not authorize current bytes",
            )
            .is_err()
        );
        publish_json_atomic(&candidate.workflow_path(&args), &marker).unwrap();
        assert_eq!(
            sha256_control_file(&candidate.workflow_path(&args)).unwrap(),
            failed_marker_sha256
        );
        // Version 1 put the first authorization at one fixed per-epoch path. Keep reading it for
        // an initial failure, but do not let it authorize any later failed attempt.
        let retry_path = legacy_retry_ready_path(&args, candidate.epoch);
        publish_json_no_replace(
            &retry_path,
            &RetryReady {
                schema_version: RETRY_READY_SCHEMA_VERSION,
                kind: "firewatch_retry_ready".into(),
                epoch: candidate.epoch,
                mode: "canonical_direct".into(),
                content_generation_sha256: candidate.target_generation.clone(),
                effective_input_sha256: candidate.target_effective_input.clone(),
                wire_profile: candidate.target_wire_profile,
                failed_marker_sha256: failed_marker_sha256.clone(),
                authorized_unix_secs: unix_now(),
                reason: "controlled retry after wire-profile audit".into(),
            },
        )
        .unwrap();
        let retry: RetryReady = read_bounded_json(&retry_path).unwrap();
        assert_eq!(retry.failed_marker_sha256, failed_marker_sha256);
        assert_eq!(
            fs::read(candidate.legacy_direct_index(&args).join("manifest.json")).unwrap(),
            b"legacy-index-remains-immutable"
        );
        assert_eq!(
            fs::read(candidate.legacy_direct_acceptance_path(&args)).unwrap(),
            b"legacy-acceptance-remains-immutable"
        );
        assert!(
            publish_direct_retry_authorization(
                &args,
                &candidate,
                &marker,
                "second authorization must not replace the first",
            )
            .is_err()
        );

        assert_eq!(
            retry_ready_for_direct(&args, &candidate, &marker, &BTreeMap::new()).unwrap(),
            Some(failed_marker_sha256.clone())
        );

        let mut changed_profile = candidate.clone();
        changed_profile.source_wire_profile =
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        changed_profile.target_wire_profile =
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        changed_profile.source_effective_input = effective_input_digest(
            &changed_profile.source_generation,
            changed_profile.source_wire_profile,
        )
        .unwrap();
        changed_profile.target_effective_input = changed_profile.source_effective_input.clone();
        assert!(
            retry_ready_for_direct(&args, &changed_profile, &marker, &BTreeMap::new()).is_err()
        );
        assert!(
            publish_direct_retry_authorization(
                &args,
                &changed_profile,
                &marker,
                "a new authorization cannot change the marker profile",
            )
            .is_err()
        );
        let mut changed_effective_input = candidate.clone();
        changed_effective_input.source_effective_input = "e".repeat(64);
        changed_effective_input.target_effective_input = "e".repeat(64);
        assert!(
            publish_direct_retry_authorization(
                &args,
                &changed_effective_input,
                &marker,
                "a new authorization cannot change the effective input",
            )
            .is_err()
        );

        let mut consumed_failure = marker.clone();
        consumed_failure.retry_of_failed_marker_sha256 = Some(failed_marker_sha256);
        consumed_failure.updated_unix_secs = 2;
        consumed_failure.message = Some("controlled retry also failed".into());
        publish_json_atomic(&candidate.workflow_path(&args), &consumed_failure).unwrap();
        assert_eq!(
            retry_ready_for_direct(&args, &candidate, &consumed_failure, &BTreeMap::new(),)
                .unwrap(),
            None
        );

        let consumed_failure_sha256 = sha256_control_file(&candidate.workflow_path(&args)).unwrap();
        let second_retry_path = publish_direct_retry_authorization(
            &args,
            &candidate,
            &consumed_failure,
            "operator authorized a second controlled retry",
        )
        .unwrap();
        assert_ne!(retry_path, second_retry_path);
        assert!(retry_path.is_file());
        let second_retry: RetryReady = read_bounded_json(&second_retry_path).unwrap();
        assert_eq!(second_retry.failed_marker_sha256, consumed_failure_sha256);
        assert!(
            publish_direct_retry_authorization(
                &args,
                &candidate,
                &consumed_failure,
                "the same failed marker cannot be authorized twice",
            )
            .is_err()
        );
        assert_eq!(
            retry_ready_for_direct(&args, &candidate, &consumed_failure, &BTreeMap::new(),)
                .unwrap(),
            Some(consumed_failure_sha256.clone())
        );

        fs::create_dir(candidate.epoch_root(&args).join(format!(
            ".canonical-{}-{}.staging-test",
            candidate.registry_order, candidate.target_effective_input
        )))
        .unwrap();
        assert!(
            retry_ready_for_direct(&args, &candidate, &consumed_failure, &BTreeMap::new(),)
                .is_err()
        );

        fs::remove_dir(candidate.epoch_root(&args).join(format!(
            ".canonical-{}-{}.staging-test",
            candidate.registry_order, candidate.target_effective_input
        )))
        .unwrap();
        let mut second_consumed_failure = consumed_failure.clone();
        second_consumed_failure.retry_of_failed_marker_sha256 = Some(consumed_failure_sha256);
        second_consumed_failure.updated_unix_secs = 3;
        second_consumed_failure.message = Some("second controlled retry also failed".into());
        publish_json_atomic(&candidate.workflow_path(&args), &second_consumed_failure).unwrap();
        assert_eq!(
            retry_ready_for_direct(
                &args,
                &candidate,
                &second_consumed_failure,
                &BTreeMap::new(),
            )
            .unwrap(),
            None
        );

        let cleanup_pid = 4_000_000_000u32;
        let cleanup_attempt = "a".repeat(32);
        let cleanup_path = candidate.epoch_root(&args).join(format!(
            ".canonical-{}-{}.staging-{cleanup_pid}-{cleanup_attempt}",
            candidate.registry_order, candidate.target_effective_input
        ));
        fs::create_dir(&cleanup_path).unwrap();
        let mut cleanup_pending = second_consumed_failure.clone();
        cleanup_pending.state = "failed_cleanup_pending".into();
        cleanup_pending.attempt_id = Some(cleanup_attempt);
        cleanup_pending.pid = Some(cleanup_pid);
        cleanup_pending.process_start_ticks = Some(1);
        cleanup_pending.argv = vec![
            "--out".into(),
            candidate.direct_index(&args).display().to_string(),
        ];
        cleanup_pending.owned_temp_path = Some(cleanup_path.clone());
        cleanup_pending.cleanup_owner_absence_confirmed = false;
        publish_json_atomic(&candidate.workflow_path(&args), &cleanup_pending).unwrap();
        assert!(
            publish_direct_retry_authorization(
                &args,
                &candidate,
                &cleanup_pending,
                "cleanup-pending retries are not safe",
            )
            .is_err()
        );
        assert_eq!(
            retry_ready_for_direct(&args, &candidate, &cleanup_pending, &BTreeMap::new(),).unwrap(),
            None
        );
        assert!(drain_one_deferred_cleanup(&args).unwrap().is_some());
        assert!(!cleanup_path.exists());
        let cleanup_free: WorkflowMarker =
            read_bounded_json(&candidate.workflow_path(&args)).unwrap();
        assert_eq!(cleanup_free.state, "failed");
        assert_eq!(
            cleanup_free.retry_of_failed_marker_sha256,
            second_consumed_failure.retry_of_failed_marker_sha256
        );
        assert_eq!(
            retry_ready_for_direct(&args, &candidate, &cleanup_free, &BTreeMap::new()).unwrap(),
            None
        );
        let cleanup_free_sha256 = sha256_control_file(&candidate.workflow_path(&args)).unwrap();
        let third_retry_path = publish_direct_retry_authorization(
            &args,
            &candidate,
            &cleanup_free,
            "operator authorized the cleanup-free failed marker",
        )
        .unwrap();
        assert!(
            third_retry_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(&cleanup_free_sha256)
        );
        assert_eq!(
            retry_ready_for_direct(&args, &candidate, &cleanup_free, &BTreeMap::new()).unwrap(),
            Some(cleanup_free_sha256)
        );
    }

    #[test]
    fn legacy_profile_neutral_failure_needs_an_exact_attestation_for_its_first_retry() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let archive = write_direct_archive(&args.archive_root, 701);
        let (files, generation, input_bytes) =
            capture_direct_archive(&archive, 701, "usage_sorted").unwrap();
        let profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let effective_input = effective_input_digest(&generation, profile).unwrap();
        let mut candidate = Candidate {
            mode: CandidateMode::CanonicalDirect,
            epoch: 701,
            source: archive.clone(),
            target: archive,
            source_generation: generation.clone(),
            target_generation: generation,
            source_wire_profile: profile,
            target_wire_profile: profile,
            source_effective_input: effective_input.clone(),
            target_effective_input: effective_input,
            authority_binding_sha256: "e".repeat(64),
            authority_proofs: None,
            registry_order: "usage_sorted".into(),
            direct_files: Some(files),
            input_bytes,
            retry_of_failed_marker_sha256: None,
        };
        fs::create_dir_all(candidate.epoch_root(&args)).unwrap();
        let legacy_marker = WorkflowMarker {
            schema_version: 1,
            epoch: candidate.epoch,
            source_generation_sha256: candidate.source_generation.clone(),
            target_generation_sha256: candidate.target_generation.clone(),
            source_effective_input_sha256: None,
            target_effective_input_sha256: None,
            source_wire_profile: None,
            target_wire_profile: None,
            authority_binding_sha256: None,
            retry_of_failed_marker_sha256: None,
            state: "failed".into(),
            phase: Phase::CanonicalBuild,
            created_unix_secs: 1,
            updated_unix_secs: 1,
            attempt_id: None,
            pid: None,
            process_start_ticks: None,
            executable: None,
            executable_dev: None,
            executable_ino: None,
            argv: Vec::new(),
            log_path: None,
            auto_paused: false,
            auto_pause_reason: None,
            owned_temp_path: None,
            cleanup_owner_absence_confirmed: false,
            message: Some("legacy decoder failure".into()),
        };
        publish_json_no_replace(&candidate.workflow_path(&args), &legacy_marker).unwrap();
        assert!(
            publish_direct_retry_authorization(
                &args,
                &candidate,
                &legacy_marker,
                "legacy retry without an attestation must fail",
            )
            .is_err()
        );

        attest_direct_archive(&args, candidate.epoch, &candidate.registry_order);
        let attestations = load_wire_profile_attestations(&args).unwrap();
        let authority_proofs = build_authority_proof_set(vec![AuthorityProof {
            label: "direct_attestation",
            evidence: exact_attestation_file_evidence(
                &attestations,
                candidate.epoch,
                &candidate.source,
                &candidate.target_generation,
            )
            .unwrap(),
            require_protected: false,
        }])
        .unwrap();
        candidate.authority_binding_sha256 = authority_proofs.digest.clone();
        candidate.authority_proofs = Some(authority_proofs);
        let path = publish_direct_retry_authorization(
            &args,
            &candidate,
            &legacy_marker,
            "first attested legacy retry",
        )
        .unwrap();
        assert_eq!(path, legacy_retry_ready_path(&args, candidate.epoch));
        assert_eq!(
            retry_ready_for_direct(&args, &candidate, &legacy_marker, &BTreeMap::new()).unwrap(),
            Some(sha256_control_file(&candidate.workflow_path(&args)).unwrap())
        );

        let mut injected_profile = legacy_marker.clone();
        injected_profile.source_effective_input_sha256 =
            Some(candidate.source_effective_input.clone());
        injected_profile.target_effective_input_sha256 =
            Some(candidate.target_effective_input.clone());
        injected_profile.source_wire_profile = Some(profile);
        injected_profile.target_wire_profile = Some(profile);
        publish_json_atomic(&candidate.workflow_path(&args), &injected_profile).unwrap();
        assert!(
            direct_retry_precondition_hash(&args, &candidate, &injected_profile, &BTreeMap::new(),)
                .is_err()
        );
    }

    #[test]
    fn status_first_queued_row_matches_next_runnable_order() {
        let args = adaptive_args();
        let views = vec![
            CandidateView {
                candidate: Candidate {
                    epoch: 301,
                    ..candidate_view(Phase::TargetBuild).candidate
                },
                phase: Phase::Parity,
                state: "queued".into(),
                acceptance: None,
                target_manifest: None,
                source_manifest: None,
            },
            CandidateView {
                candidate: Candidate {
                    epoch: 1012,
                    ..candidate_view(Phase::TargetBuild).candidate
                },
                phase: Phase::TargetBuild,
                state: "queued".into(),
                acceptance: None,
                target_manifest: None,
                source_manifest: None,
            },
        ];
        let active = BTreeMap::new();
        let next = next_runnable(&views, &active).unwrap().candidate.epoch;
        let status = build_status(&args, &views, &[], &active, 1, None, (2, 2, 0, 0), None);
        let first_queued = status
            .rows
            .iter()
            .find(|row| row.state == "queued")
            .unwrap()
            .epoch as u64;
        assert_eq!(first_queued, next);
        assert_eq!(next, 301);
    }

    #[test]
    fn pair_and_canonical_recovery_markers_use_separate_roots() {
        let args = adaptive_args();
        let pair = candidate_view(Phase::TargetBuild).candidate;
        let mut direct = pair.clone();
        direct.mode = CandidateMode::CanonicalDirect;
        direct.source = PathBuf::from("/archive/epoch-301");
        direct.target = direct.source.clone();
        direct.registry_order = "usage_sorted".into();
        direct.direct_files = Some(BTreeMap::new());
        assert_eq!(
            pair.workflow_path(&args),
            PathBuf::from("/state/firewatch-index/epochs/epoch-301.json")
        );
        assert_eq!(
            direct.workflow_path(&args),
            PathBuf::from("/state/firewatch-index/canonical-epochs/epoch-301.json")
        );
    }

    #[test]
    fn legacy_schema2_workflow_marker_is_reconciled_but_cannot_bind_a_new_candidate() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let candidate = candidate_view(Phase::TargetBuild).candidate;
        let mut marker = legacy_accepted_marker(&candidate, Phase::TargetBuild);
        marker.schema_version = LEGACY_PROFILE_BOUND_WORKFLOW_SCHEMA_VERSION;
        marker.source_effective_input_sha256 = Some(candidate.source_effective_input.clone());
        marker.target_effective_input_sha256 = Some(candidate.target_effective_input.clone());
        marker.source_wire_profile = Some(candidate.source_wire_profile);
        marker.target_wire_profile = Some(candidate.target_wire_profile);
        publish_json_atomic(&candidate.workflow_path(&args), &marker).unwrap();

        assert!(!marker_content_matches_candidate(&marker, &candidate));
        assert_eq!(
            reconcile_all_workflow_markers(&args, &BTreeMap::new()).unwrap(),
            None
        );

        marker.authority_binding_sha256 = Some("e".repeat(64));
        publish_json_atomic(&candidate.workflow_path(&args), &marker).unwrap();
        assert!(reconcile_all_workflow_markers(&args, &BTreeMap::new()).is_err());
    }

    #[test]
    fn terminal_marker_in_other_mode_does_not_conflict_with_active_epoch() {
        let directory = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(directory.path()).unwrap();
        let args = direct_test_args(&root);
        let marker = WorkflowMarker {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            epoch: 301,
            source_generation_sha256: "a".repeat(64),
            target_generation_sha256: "b".repeat(64),
            source_effective_input_sha256: Some("c".repeat(64)),
            target_effective_input_sha256: Some("d".repeat(64)),
            source_wire_profile: Some(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1),
            target_wire_profile: Some(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1),
            authority_binding_sha256: Some("e".repeat(64)),
            retry_of_failed_marker_sha256: None,
            state: "accepted".into(),
            phase: Phase::CanonicalBuild,
            created_unix_secs: 1,
            updated_unix_secs: 1,
            attempt_id: None,
            pid: None,
            process_start_ticks: None,
            executable: None,
            executable_dev: None,
            executable_ino: None,
            argv: Vec::new(),
            log_path: None,
            auto_paused: false,
            auto_pause_reason: None,
            owned_temp_path: None,
            cleanup_owner_absence_confirmed: false,
            message: None,
        };
        publish_json_atomic(
            &args
                .controller_state_root
                .join("canonical-epochs/epoch-301.json"),
            &marker,
        )
        .unwrap();

        // No process is needed to prove the root/mode routing: an unrelated terminal marker is
        // ignored even if the active map contains the same epoch in pair mode.
        let active = BTreeMap::new();
        assert_eq!(
            reconcile_all_workflow_markers(&args, &active).unwrap(),
            None
        );
        assert!(!other_mode_live_conflict(
            CandidateMode::CanonicalDirect,
            CandidateMode::MigrationPair,
            "accepted"
        ));
        assert!(other_mode_live_conflict(
            CandidateMode::CanonicalDirect,
            CandidateMode::MigrationPair,
            "running"
        ));
        assert!(!other_mode_live_conflict(
            CandidateMode::MigrationPair,
            CandidateMode::MigrationPair,
            "running"
        ));
    }

    #[test]
    fn first_slot_can_start_when_cpu_is_busy_but_an_additional_slot_waits() {
        let args = adaptive_args();
        let candidate = candidate_view(Phase::TargetBuild);
        let mut status = scheduler(8 * GIB, 0.0, 0.0);
        status.machine.cpu_pressure_some_avg10 = Some(25.0);
        let cgroup = cgroup_snapshot();
        let pool = PoolState::default();

        assert_eq!(
            test_pressure(&status, 512 * GIB, PressureContext::Admission),
            PressureState::Safe
        );
        assert_eq!(
            additional_worker_decision(
                &args,
                &candidate,
                &status,
                Some(&cgroup),
                Some(&cgroup),
                &pool,
            ),
            AdmissionDecision::Wait("CPU headroom is not stable enough for another worker".into())
        );
    }

    #[test]
    fn cache_heavy_low_psi_cgroup_can_admit_an_additional_slot() {
        let args = adaptive_args();
        let candidate = candidate_view(Phase::TargetBuild);
        let status = scheduler(8 * GIB, 0.0, 0.0);
        let baseline = cgroup_snapshot();
        let pool = PoolState::default();

        let mut cache_heavy = baseline;
        cache_heavy.current_bytes = 3 * GIB;
        cache_heavy.anon_bytes = GIB;
        cache_heavy.file_bytes = 2 * GIB;
        cache_heavy.inactive_file_bytes = 2 * GIB;
        cache_heavy.high_bytes = Some(2 * GIB);
        cache_heavy.max_bytes = Some(3 * GIB);
        cache_heavy.events.high = 1;

        assert_eq!(
            additional_worker_decision(
                &args,
                &candidate,
                &status,
                Some(&cache_heavy),
                Some(&baseline),
                &pool,
            ),
            AdmissionDecision::Admit
        );
    }

    #[test]
    fn cgroup_working_usage_saturates_cache_and_swap_arithmetic() {
        let mut cgroup = cgroup_snapshot();
        cgroup.current_bytes = 64 * MIB;
        cgroup.inactive_file_bytes = 128 * MIB;
        cgroup.swap_current_bytes = 32 * MIB;
        assert_eq!(cgroup_working_usage_bytes(&cgroup), 32 * MIB);

        cgroup.current_bytes = u64::MAX;
        cgroup.inactive_file_bytes = 0;
        cgroup.swap_current_bytes = 1;
        assert_eq!(cgroup_working_usage_bytes(&cgroup), u64::MAX);
    }

    #[test]
    fn nonreclaimable_usage_without_cgroup_headroom_blocks_an_additional_slot() {
        let args = adaptive_args();
        let candidate = candidate_view(Phase::TargetBuild);
        let status = scheduler(8 * GIB, 0.0, 0.0);
        let mut cgroup = cgroup_snapshot();
        cgroup.current_bytes = 1_536 * MIB;
        cgroup.anon_bytes = 1_536 * MIB;
        cgroup.file_bytes = 0;
        cgroup.inactive_file_bytes = 0;
        cgroup.high_bytes = Some(2 * GIB);
        let pool = PoolState::default();

        assert_eq!(
            additional_worker_decision(
                &args,
                &candidate,
                &status,
                Some(&cgroup),
                Some(&cgroup),
                &pool,
            ),
            AdmissionDecision::Wait(
                "cgroup memory headroom is too small for another worker".into()
            )
        );
    }

    #[test]
    fn hard_cgroup_memory_events_block_an_additional_slot() {
        let args = adaptive_args();
        let candidate = candidate_view(Phase::TargetBuild);
        let status = scheduler(8 * GIB, 0.0, 0.0);
        let baseline = cgroup_snapshot();
        let pool = PoolState::default();

        let mut max = baseline;
        max.events.max = 1;
        let mut oom = baseline;
        oom.events.oom = 1;
        let mut oom_kill = baseline;
        oom_kill.events.oom_kill = 1;

        for current in [max, oom, oom_kill] {
            assert_eq!(
                additional_worker_decision(
                    &args,
                    &candidate,
                    &status,
                    Some(&current),
                    Some(&baseline),
                    &pool,
                ),
                AdmissionDecision::Wait("cgroup hard memory counters increased".into())
            );
        }
    }

    #[test]
    fn cgroup_memory_psi_blocks_an_additional_slot() {
        let args = adaptive_args();
        let candidate = candidate_view(Phase::TargetBuild);
        let status = scheduler(8 * GIB, 0.0, 0.0);
        let mut cgroup = cgroup_snapshot();
        cgroup.pressure_some_avg10 = Some(0.2);
        let pool = PoolState::default();

        assert_eq!(
            additional_worker_decision(
                &args,
                &candidate,
                &status,
                Some(&cgroup),
                Some(&cgroup),
                &pool,
            ),
            AdmissionDecision::Wait("memory pressure is not low enough for another worker".into())
        );
    }

    #[test]
    fn pressure_policy_has_distinct_pause_hold_and_safe_bands() {
        assert_eq!(
            test_pressure(
                &scheduler(4 * GIB, 0.0, 0.0),
                512 * GIB,
                PressureContext::Admission,
            ),
            PressureState::Safe
        );
        assert_eq!(
            test_pressure(
                &scheduler(3 * GIB, 0.5, 6.0),
                512 * GIB,
                PressureContext::Admission,
            ),
            PressureState::Hold
        );
        assert!(matches!(
            test_pressure(
                &scheduler(GIB, 0.0, 0.0),
                512 * GIB,
                PressureContext::Admission,
            ),
            PressureState::Cancel(_)
        ));
        assert!(matches!(
            test_pressure(
                &scheduler(4 * GIB, 1.1, 0.0),
                512 * GIB,
                PressureContext::Admission,
            ),
            PressureState::Cancel(_)
        ));
        assert!(matches!(
            test_pressure(
                &scheduler(4 * GIB, 0.0, 15.1),
                512 * GIB,
                PressureContext::Admission,
            ),
            PressureState::Pause(_)
        ));
        let mut disk_low = scheduler(4 * GIB, 0.0, 0.0);
        disk_low.machine.disk_available_bytes = 511 * GIB;
        assert!(matches!(
            test_pressure(&disk_low, 512 * GIB, PressureContext::Admission),
            PressureState::Cancel(_)
        ));
    }

    #[test]
    fn running_firewatch_does_not_pause_for_unattributed_host_io() {
        assert_eq!(
            test_pressure(
                &scheduler(4 * GIB, 0.0, 99.0),
                512 * GIB,
                PressureContext::Active,
            ),
            PressureState::Safe
        );
        let mut missing_io = scheduler(4 * GIB, 0.0, 0.0);
        missing_io.machine.io_pressure_full_avg10 = None;
        assert_eq!(
            test_pressure(&missing_io, 512 * GIB, PressureContext::Active),
            PressureState::Safe
        );
        assert!(matches!(
            test_pressure(&missing_io, 512 * GIB, PressureContext::Admission),
            PressureState::Pause(_)
        ));
    }

    #[test]
    fn running_firewatch_does_not_cancel_for_its_own_host_memory_pressure() {
        assert_eq!(
            test_pressure(
                &scheduler(4 * GIB, 1.1, 0.0),
                512 * GIB,
                PressureContext::Active,
            ),
            PressureState::Hold
        );
        assert!(matches!(
            test_pressure(
                &scheduler(4 * GIB, 1.1, 0.0),
                512 * GIB,
                PressureContext::Admission,
            ),
            PressureState::Cancel(_)
        ));
        assert!(matches!(
            test_pressure(
                &scheduler(GIB, 0.0, 0.0),
                512 * GIB,
                PressureContext::Active,
            ),
            PressureState::Cancel(_)
        ));
        assert_eq!(
            test_pressure(
                &scheduler(4 * GIB, 1.1, 0.0),
                512 * GIB,
                PressureContext::Paused,
            ),
            PressureState::Hold
        );
    }

    #[test]
    fn reclaimable_cgroup_cache_near_max_is_not_a_hard_event() {
        let mut previous = cgroup_snapshot();
        previous.current_bytes = 2 * GIB - 256 * MIB;
        previous.max_bytes = Some(2 * GIB);
        let mut current = previous;
        current.current_bytes = 2 * GIB - 64 * MIB;

        assert_eq!(
            cgroup_runtime_pressure(Some(&previous), Some(&current)),
            None
        );

        current.events.max = 1;
        assert!(matches!(
            cgroup_runtime_pressure(Some(&previous), Some(&current)),
            Some(PressureState::Cancel(_))
        ));
    }

    #[test]
    fn paused_firewatch_waits_for_host_io_to_recover() {
        assert!(matches!(
            test_pressure(
                &scheduler(4 * GIB, 0.0, 99.0),
                512 * GIB,
                PressureContext::Paused,
            ),
            PressureState::Pause(_)
        ));
        assert_eq!(
            test_pressure(
                &scheduler(4 * GIB, 0.0, 4.9),
                512 * GIB,
                PressureContext::Paused,
            ),
            PressureState::Safe
        );
    }

    #[test]
    fn scheduler_pause_and_active_lane_pause_firewatch() {
        let mut status = scheduler(4 * GIB, 0.0, 0.0);
        status.scheduler.paused = true;
        assert!(matches!(
            test_pressure(&status, 512 * GIB, PressureContext::Active),
            PressureState::Pause(_)
        ));
        status.scheduler.paused = false;
        status.lanes.push(SchedulerLane {
            id: "scan:1".into(),
            state: "running".into(),
        });
        assert!(matches!(
            test_pressure(&status, 512 * GIB, PressureContext::Active),
            PressureState::Pause(_)
        ));
        status.machine.memory_available_bytes = GIB;
        assert!(matches!(
            test_pressure(&status, 512 * GIB, PressureContext::Active),
            PressureState::Cancel(_)
        ));
    }

    #[test]
    fn only_typed_runnable_scheduler_work_stops_firewatch() {
        let mut status = scheduler(4 * GIB, 0.0, 0.0);
        status.summary.queued = 1;
        assert!(matches!(
            test_pressure(&status, 512 * GIB, PressureContext::Active),
            PressureState::Cancel(_)
        ));
        status.summary.queued = 0;
        status.summary.scan_ready = 1;
        assert!(matches!(
            test_pressure(&status, 512 * GIB, PressureContext::Active),
            PressureState::Cancel(_)
        ));
        status.summary.scan_ready = 0;
        status.summary.poh_migration_epochs_runnable = 1;
        assert!(matches!(
            test_pressure(&status, 512 * GIB, PressureContext::Active),
            PressureState::Cancel(_)
        ));
        status.summary.poh_migration_epochs_runnable = 0;
        status.scan_sweep.pending = 1;
        assert!(matches!(
            test_pressure(&status, 512 * GIB, PressureContext::Active),
            PressureState::Cancel(_)
        ));
        status.scan_sweep.pending = 0;
        status.summary.registry_reprocess_epochs_runnable = 1;
        assert!(matches!(
            test_pressure(&status, 512 * GIB, PressureContext::Active),
            PressureState::Cancel(_)
        ));
        status.summary.registry_reprocess_epochs_runnable = 0;
        status.summary.registry_reprocess_audits_runnable = 1;
        assert!(matches!(
            test_pressure(&status, 512 * GIB, PressureContext::Active),
            PressureState::Cancel(_)
        ));
    }

    #[test]
    fn parity_output_is_exact_and_rejects_duplicates_or_mismatch() {
        let root =
            std::env::temp_dir().join(format!("firewatch-controller-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        let path = root.join("parity.log");
        fs::write(&path, "canonical_equal=true\nwallets=12\nrelations=34\ncanonical_sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n").unwrap();
        let parsed = parse_parity_log(&path).unwrap();
        assert_eq!(parsed.wallets, 12);
        assert_eq!(parsed.relations, 34);
        fs::write(&path, "canonical_equal=false\nwallets=12\nrelations=34\ncanonical_sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n").unwrap();
        assert!(parse_parity_log(&path).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn epoch_marker_names_and_hashes_are_strict() {
        assert_eq!(parse_epoch_json_name("epoch-301.json"), Some(301));
        assert_eq!(parse_epoch_json_name("epoch-301.json.old"), None);
        assert!(is_sha256(&"a".repeat(64)));
        assert!(!is_sha256(&"A".repeat(64)));
        assert!(!is_sha256(&"a".repeat(63)));
        assert!(is_attempt_id(&"a".repeat(32)));
        assert!(!is_attempt_id(&"A".repeat(32)));
        assert!(!is_attempt_id(&"a".repeat(31)));
    }

    #[test]
    fn scheduler_url_rejects_remote_hosts_userinfo_and_redirect_indirection() {
        assert!(validate_scheduler_url("http://127.0.0.1:8786/api/v1/status").is_ok());
        assert!(validate_scheduler_url("http://localhost:8786/api/v1/status").is_err());
        assert!(validate_scheduler_url("http://127.0.0.1:80@evil.example/api/v1/status").is_err());
        assert!(validate_scheduler_url("https://127.0.0.1:8786/api/v1/status").is_err());
    }

    #[test]
    fn immutable_json_publication_never_replaces_an_existing_receipt() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("receipt.json");
        publish_json_no_replace(&path, &serde_json::json!({ "value": 1 })).unwrap();
        assert!(publish_json_no_replace(&path, &serde_json::json!({ "value": 2 })).is_err());
        let value: serde_json::Value = read_bounded_json(&path).unwrap();
        assert_eq!(value, serde_json::json!({ "value": 1 }));
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }
}
