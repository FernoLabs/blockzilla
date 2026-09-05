//! Verify, publish, and atomically select one completed metadata-normalization candidate.
//!
//! Preparation and canonical cutover are separate operations. Preparation may
//! add immutable controls only to the private candidate. Cutover changes the
//! canonical epoch path with one whole-directory exchange. The historical
//! source is never edited or deleted.

mod archive_v2_source_authority_common;

use anyhow::{Context, Result, anyhow, bail, ensure};
use archive_v2_source_authority_common::{
    AuthorityDisposition, SourceAuthorityInventory, looks_like_archive_or_control,
};
use blockzilla_format::{
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
};
use blockzilla_read_sdk::{
    ARCHIVE_V2_PUBLICATION_LOCK_FILE, ArchiveReader, ArchiveV2MetadataSchemaCounts,
    ArchiveV2MetadataWireProfile, ArchiveV2WireProfile, CURRENT_TYPED_ERRORS_MARKER_BYTES,
    CURRENT_TYPED_ERRORS_MARKER_FILE, CURRENT_TYPED_ERRORS_MARKER_SHA256,
    CURRENT_TYPED_ERRORS_MARKER_SIZE, HashVerification, OpenOptions as ReaderOpenOptions,
    PinnedLocalEntryKind, PinnedLocalRangeSource, RangeSource, SourceError, SourceResult,
    UnprovenWireProfileDecision, audit_current_metadata_for_marker_publication,
    audit_full_generation_wire_profile,
    manifest::{
        BLOCK_INDEX_FILE, BLOCKS_FILE, GENERATION_MANIFEST_FILE,
        GENERATION_MANIFEST_SCHEMA_VERSION, GenerationFile, GenerationManifest, META_FILE,
        REGISTRY_FILE, REGISTRY_INDEX_FILE, REQUIRED_GENERATION_FILES, SIGNATURES_FILE,
        TrustedGenerationIdentity, compute_generation_digest,
    },
    validate_manifest_bound_pinned_local_registry_index, wire_profile_marker,
    wire_profile_marker_bytes,
};
use blockzilla_token_transaction_dump::{
    ACCOUNT_ID_LOG_FILE, ACCOUNTS_FILE, CREATIONS_FILE, DISCOVERY_SHARDS_DIR, DUMP_MANIFEST_FILE,
    DUMP_SCHEMA_VERSION, DumpArtifactKind, DumpManifest, DumpSourceBinding, DumpWireProfile,
    EPOCH_SHARDS_DIR, RESUME_CHECKPOINT_FILE, ResumeCheckpointPayload, ResumeExtractionMode,
    ResumeStage, SPYX_MINT, SPYX_MINT_SIGNATURE, SPYX_MINT_SLOT, TRANSACTIONS_FILE,
};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    ffi::{CString, OsStr, OsString},
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{ffi::OsStrExt, fs::MetadataExt},
    },
    path::{Component, Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("archive-v2-publish-normalized-metadata requires Linux or macOS");

const CANDIDATE_FILE: &str = "archive-v2-metadata-normalization.candidate.v1.json";
const RECEIPT_FILE: &str = "archive-v2-metadata-normalization.receipt.v1.json";
const PREPARE_FILE: &str = "archive-v2-metadata-normalization.publication-prepare.v1.json";
const RELOCATION_INTENT_FILE: &str = "archive-v2-metadata-normalization.relocation-intent.v1.json";
const SWITCH_INTENT_FILE: &str = "archive-v2-metadata-normalization.switch-intent.v1.json";
const SOURCE_FREEZE_FILE: &str = "archive-v2-metadata-normalization.source-freeze.v1.json";
const SWITCH_COMPLETE_FILE: &str = "archive-v2-metadata-normalization.switch-complete.v1.json";
const ROLLBACK_INTENT_FILE: &str = "archive-v2-metadata-normalization.rollback-intent.v1.json";
const ROLLBACK_COMPLETE_FILE: &str = "archive-v2-metadata-normalization.rollback-complete.v1.json";
const QUIESCENCE_FRESHNESS_SECONDS: u64 = 300;
const JOURNAL_LOCK_FILE: &str = ".archive-v2-metadata-normalization.publish.lock";
// This is the lock already used by the in-place Pre-to-Post migration. Reuse
// it so the two canonical-directory writers cannot run together.
const ARCHIVE_ROOT_SWITCH_LOCK_FILE: &str = ".archive-v2-pre-to-post.switch.lock";
const MAX_JSON_BYTES: usize = 16 << 20;
const HASH_BUFFER_BYTES: usize = 8 << 20;
const MAX_PROFILE_MESSAGE_BYTES: usize = 16 << 20;
const CHECKPOINT_HASH_DOMAIN: &[u8] = b"blockzilla-token-transaction-dump/resume/v3\0";
const SUPPORTED_EPOCH: u64 = 900;
const CANONICAL_EPOCH_900_PATH: &str = "/volume1/blockzilla/archive/epoch-900";
const SPYX_TRUSTED_LOCAL_GENERATION_ID: &str = "token-transaction-dump-trusted-local-sizes-v1";
const SPYX_TRUSTED_LOCAL_CLUSTER_ID: &str = "mainnet-beta";
const SPYX_TRUSTED_LOCAL_SLOTS_PER_EPOCH: u64 = 432_000;
const SPYX_TRUSTED_LOCAL_REQUIRED_ADDITIONAL_FILES: [&str; 2] =
    [SIGNATURES_FILE, REGISTRY_INDEX_FILE];
const SPYX_TRUSTED_LOCAL_OPTIONAL_ADDITIONAL_FILES: [&str; 2] = [
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
];
const SPYX_DEPLOYED_TRUSTED_BINDING_RECIPE: &str = "deployed-spyx-name-size-message-profile-v1";
const SPYX_DEPLOYED_TRUSTED_FILE_SIZE_BINDING_DOMAIN: &[u8] =
    b"blockzilla/archive-v2-trusted-local-file-size-binding\0";
const SPYX_FIRST_EPOCH: u64 = 801;
const SPYX_LAST_EPOCH: u64 = 1_018;
const SPYX_EPOCH_COUNT: usize = 218;
const OBSERVED_SPYX_EPOCH_900_SOURCE_DIGEST: &str =
    "2477cfc2e93bf8ee85c6ea9092de810e947e8fbe8cc092d4094b48da1a9a752e";
const OBSERVED_SPYX_PID: u32 = 252_572;
const OBSERVED_SPYX_START_TICKS: u64 = 143_723_175;
const OBSERVED_SPYX_BOOT_ID: &str = "4db48623-c977-45b2-b1cb-b189b175b80a";
const OBSERVED_SPYX_EXECUTABLE_SHA256: &str =
    "5c2f5c89da0a87a208543286018ea490515bd27d91ebf4d396a37b64f29fd94e";
const OBSERVED_SPYX_EXECUTABLE_PATH: &str = "/volume1/blockzilla/bin/blockzilla-token-transaction-dump-5c2f5c89da0a87a208543286018ea490515bd27d91ebf4d396a37b64f29fd94e";
const SPYX_PROCESS_AUTHORITY_PATH: &str = "/volume1/blockzilla/archive-metadata-normalization/authority/spyx-live-process-authority-v1-fa6b5abd5955e00fd8996ba91ec143d9bc7a41b2beb0b7b049974dae6ac5a725.json";
const SPYX_PROCESS_AUTHORITY_SHA256: &str =
    "fa6b5abd5955e00fd8996ba91ec143d9bc7a41b2beb0b7b049974dae6ac5a725";
const SPYX_PROCESS_AUTHORITY_BYTES: u64 = 3_564;
const SPYX_OUTPUT_ROOT: &str = "/volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-single-read-20260827T201409";
const SPYX_FINAL_ROOT_MANIFEST_SHA256: &str =
    "841a8511cf1ad80060641bf0b81fa7feafe35fa71bc619312e39d71cd1d36783";
const SPYX_FINAL_ROOT_MANIFEST_BYTES: u64 = 779;
const SPYX_FINAL_CHECKPOINT_SHA256: &str =
    "3b520de5e5df86d2e9ff1fcac65a98389e43dd8313c4280f90585637e7b0ab9c";
const SPYX_FINAL_CHECKPOINT_BYTES: u64 = 183_478;
const SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256: &str =
    "235707838ca21648b7996059790c48403c64712838a796b950886ba494a67bf9";
const SPYX_WORKERS: usize = 12;
const SPYX_DISCOVERY_QUARANTINES: [&str; 2] = [
    ".abandoned-epoch-857-partial-1787869104464-0",
    ".abandoned-epoch-900-partial-1787899183898-0",
];
const SPYX_RAW_QUARANTINES: [&str; 2] = [
    ".abandoned-epoch-857-partial-1787869104465-0",
    ".abandoned-epoch-900-partial-1787899183930-0",
];

#[derive(Debug, Parser)]
#[command(name = "archive-v2-publish-normalized-metadata", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Verify the source and candidate, publish fixed controls, and freeze the candidate.
    Prepare(PrepareArgs),
    /// Fully verify SPYX and durably arm the cutover without changing canonical paths.
    ArmCutover(ArmCutoverArgs),
    /// Use a fresh quiescence proof and atomically select the armed candidate.
    Cutover(CutoverArgs),
    /// Atomically restore the preserved historical source after an external reader drain.
    Rollback(RollbackArgs),
}

#[derive(Debug, Args)]
struct PrepareArgs {
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    candidate: PathBuf,
    /// Durable hidden sibling of archive/epoch-N used for selection and rollback.
    #[arg(long)]
    selector: PathBuf,
    #[arg(long)]
    source_authority: PathBuf,
    #[arg(long)]
    source_authority_sha256: String,
    #[arg(long)]
    candidate_sha256: String,
    #[arg(long)]
    receipt_sha256: String,
    /// Exact source digest observed in both live SPYX epoch-900 checkpoint rows.
    #[arg(long)]
    expected_spyx_epoch_900_source_digest: String,
    /// Durable external journal directory. It is created if absent.
    #[arg(long)]
    journal: PathBuf,
    #[arg(long)]
    epoch: u64,
}

#[derive(Debug, Args)]
struct ArmCutoverArgs {
    #[arg(long)]
    journal: PathBuf,
    /// Root manifest written last by the completed SPYX extraction.
    #[arg(long)]
    spyx_root_manifest: PathBuf,
    #[arg(long)]
    spyx_root_manifest_sha256: String,
    #[arg(long)]
    spyx_resume_checkpoint: PathBuf,
    #[arg(long)]
    spyx_resume_checkpoint_sha256: String,
    #[arg(long)]
    expected_spyx_first_epoch: u64,
    #[arg(long)]
    expected_spyx_last_epoch: u64,
    #[arg(long)]
    expected_spyx_epoch_900_source_digest: String,
    /// Immutable live-process observation that causally binds PID to executable and argv.
    #[arg(long)]
    spyx_process_authority: PathBuf,
    #[arg(long)]
    spyx_process_authority_sha256: String,
    /// Fresh service-stop receipt used only on the second, source-freeze arm step.
    #[arg(long, requires = "reader_quiescence_receipt_sha256")]
    reader_quiescence_receipt: Option<PathBuf>,
    #[arg(long, requires = "reader_quiescence_receipt")]
    reader_quiescence_receipt_sha256: Option<String>,
}

#[derive(Debug, Args)]
struct CutoverArgs {
    #[arg(long)]
    journal: PathBuf,
    /// External durable statement that the gateway, scheduler, and other readers are stopped.
    #[arg(long)]
    reader_quiescence_receipt: PathBuf,
    #[arg(long)]
    reader_quiescence_receipt_sha256: String,
}

#[derive(Debug, Args)]
struct RollbackArgs {
    #[arg(long)]
    journal: PathBuf,
    /// External operator receipt that states all readers and the gateway are stopped.
    #[arg(long)]
    reader_quiescence_receipt: PathBuf,
    #[arg(long)]
    reader_quiescence_receipt_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileBinding {
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedFileBinding {
    name: String,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileIdentity {
    bytes: u64,
    device: u64,
    inode: u64,
    mode: u32,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryIdentity {
    device: u64,
    inode: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceFileBinding {
    identity: FileIdentity,
    content: FileBinding,
    disposition: AuthorityDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataCounts {
    records: u64,
    successful_records: u64,
    current_error_records: u64,
    legacy_error_records: u64,
    owned_fallback_records: u64,
    ambiguous_owned_fallback_records: u64,
    target_current_only_records: u64,
    target_both_equal_records: u64,
    input_bytes: u64,
    output_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrameProcessingCounts {
    copied_blocks: u64,
    copied_bytes: u64,
    recompressed_blocks: u64,
    recompressed_source_bytes: u64,
    recompressed_target_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NormalizationCandidate {
    schema_version: u32,
    kind: String,
    state: String,
    metadata_schema: String,
    source: String,
    staging: String,
    epoch: u64,
    slots_per_epoch: u64,
    cluster_id: String,
    source_generation_id: String,
    source_generation_digest: String,
    source_message_wire_profile: String,
    source_metadata_wire_profile: String,
    candidate_id: String,
    target_candidate_digest: String,
    authorized_message_marker: NamedFileBinding,
    authorized_metadata_marker: NamedFileBinding,
    source_authority_kind: String,
    source_authority_id: String,
    source_authority_binding: FileBinding,
    source_directory_identity: FileIdentity,
    source_files: BTreeMap<String, SourceFileBinding>,
    files: BTreeMap<String, FileBinding>,
    frame_processing: FrameProcessingCounts,
    omitted_source_controls: Vec<String>,
    ignored_unrelated_source_entries: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NormalizationReceipt {
    schema_version: u32,
    kind: String,
    state: String,
    canonical_publication_performed: bool,
    source: String,
    staging: String,
    epoch: u64,
    slots_per_epoch: u64,
    completed_unix_seconds: u64,
    source_blocks: FileBinding,
    source_index: FileBinding,
    target_blocks: FileBinding,
    target_index: FileBinding,
    candidate_manifest: FileBinding,
    message_marker: NamedFileBinding,
    metadata_marker: NamedFileBinding,
    cluster_id: String,
    source_generation_id: String,
    source_generation_digest: String,
    target_candidate_id: String,
    target_candidate_digest: String,
    source_authority_kind: String,
    source_authority_id: String,
    source_authority_binding: FileBinding,
    message_wire_profile: String,
    source_metadata_profile: String,
    source_metadata_profile_counts: ArchiveV2MetadataSchemaCounts,
    blocks: u64,
    transactions: u64,
    message_bytes: u64,
    message_sha256: String,
    metadata: MetadataCounts,
    frame_processing: FrameProcessingCounts,
    target_metadata_profile: String,
    target_metadata_profile_counts: ArchiveV2MetadataSchemaCounts,
    copied_sidecars: u64,
    copied_sidecar_bytes: u64,
    get_block_rows_rebuilt: u64,
    target_zstd_level: i32,
    source_revalidated_at_completion: bool,
    source_directory_identity: FileIdentity,
    source_files: BTreeMap<String, SourceFileBinding>,
    ignored_unrelated_source_entries: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenFileProof {
    identity: FileIdentity,
    content: FileBinding,
}

/// Exact proof of the synthetic trusted-local identity used by the completed
/// SPYX process. This is intentionally separate from the content-bound source
/// authority digest used by the normalizer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpyxTrustedLocalSourceProof {
    binding_recipe: String,
    trusted_manifest: GenerationManifest,
    generation_binding_digest: String,
    registry_binding_sha256: String,
    current_reader_generation_digest: String,
    current_reader_registry_binding_sha256: String,
    wire_profile: ArchiveV2WireProfile,
    metadata_wire_profile: ArchiveV2MetadataWireProfile,
    required_additional_files: Vec<String>,
    optional_additional_files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareReceipt {
    schema_version: u32,
    kind: String,
    state: String,
    prepared_unix_seconds: u64,
    epoch: u64,
    active_source: String,
    original_candidate: String,
    candidate: String,
    source_authority_path: String,
    source_authority: FileBinding,
    normalization_candidate: FileBinding,
    normalization_receipt: FileBinding,
    source_directory: DirectoryIdentity,
    target_directory: DirectoryIdentity,
    target_frozen_directory_identity: FileIdentity,
    cluster_id: String,
    generation_id: String,
    target_candidate_digest: String,
    generation_digest: String,
    spyx_epoch_900_source_generation_digest: String,
    spyx_trusted_local_source: SpyxTrustedLocalSourceProof,
    generation_manifest: FileBinding,
    message_marker: NamedFileBinding,
    metadata_marker: NamedFileBinding,
    source_files: BTreeMap<String, SourceFileBinding>,
    ignored_unrelated_source_entries: Vec<String>,
    target_files: BTreeMap<String, FrozenFileProof>,
    publication_lock_identity: FileIdentity,
    target_frozen: bool,
    full_descriptor_publication_audit_completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelocationIntent {
    schema_version: u32,
    kind: String,
    state: String,
    epoch: u64,
    original_candidate: String,
    selector: String,
    target_directory: DirectoryIdentity,
    normalization_candidate: FileBinding,
    normalization_receipt: FileBinding,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletionGate {
    root_manifest_path: String,
    root_manifest: FileBinding,
    resume_checkpoint_path: String,
    resume_checkpoint: FileBinding,
    resume_checkpoint_payload_sha256: String,
    process_authority_path: String,
    process_authority: FileBinding,
    first_epoch: u64,
    last_epoch: u64,
    extraction_mode: String,
    source_generations: u64,
    extractor_pid: u32,
    extractor_start_ticks: u64,
    extractor_boot_id: String,
    extractor_executable_path: String,
    extractor_executable: FileBinding,
    output_snapshot: SpyxOutputSnapshot,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpyxProcessAuthority {
    schema_version: u32,
    kind: String,
    state: String,
    observed_unix_seconds: u64,
    observed_utc: String,
    host_boot_id: String,
    process: SpyxObservedProcess,
    executable: SpyxObservedExecutable,
    argv: Vec<String>,
    run: SpyxObservedRun,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpyxObservedProcess {
    pid: u32,
    start_ticks: u64,
    comm: String,
    state: String,
    parent_pid: u32,
    process_group_id: u32,
    session_id: u32,
    raw_proc_stat: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpyxObservedExecutable {
    proc_exe_target: String,
    path: String,
    proc_exe_sha256: String,
    path_sha256: String,
    proc_exe_identity: SpyxObservedExecutableIdentity,
    path_identity: SpyxObservedExecutableIdentity,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpyxObservedExecutableIdentity {
    device: u64,
    inode: u64,
    bytes: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    modified_unix_seconds: i64,
    changed_unix_seconds: i64,
    modified_timestamp: String,
    changed_timestamp: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpyxObservedRun {
    archive_root: String,
    output_root: String,
    mint: String,
    mint_slot: u64,
    mint_signature: String,
    workers: usize,
    last_epoch: u64,
    trusted_local: bool,
    cluster_id: String,
    slots_per_epoch: u64,
    wire_profile_cli: String,
    single_read_batches: bool,
    single_read_match_hints: bool,
    resume: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpyxOutputSnapshot {
    directories: BTreeMap<String, FileIdentity>,
    files: BTreeMap<String, FrozenFileProof>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResumeCheckpointEnvelope {
    payload: ResumeCheckpointPayload,
    payload_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReaderQuiescenceReceipt {
    schema_version: u32,
    kind: String,
    state: String,
    operation: String,
    issued_unix_seconds: u64,
    current_host_boot_id: String,
    epoch: u64,
    active_path: String,
    retained_path: String,
    active_directory: DirectoryIdentity,
    retained_directory: DirectoryIdentity,
    prepare_receipt_sha256: String,
    switch_intent_sha256: Option<String>,
    source_freeze_sha256: Option<String>,
    switch_complete_sha256: Option<String>,
    spyx_root_manifest_sha256: Option<String>,
    spyx_resume_checkpoint_sha256: Option<String>,
    spyx_epoch_900_source_digest: Option<String>,
    spyx_process_pid: Option<u32>,
    spyx_process_start_ticks: Option<u64>,
    spyx_process_boot_id: Option<String>,
    spyx_extractor_executable_path: Option<String>,
    spyx_extractor_executable_sha256: Option<String>,
    spyx_output_root: Option<String>,
    gateway_stopped: bool,
    scheduler_stopped: bool,
    archive_readers_stopped: bool,
    archive_writers_stopped: bool,
    extractor_stopped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SwitchIntent {
    schema_version: u32,
    kind: String,
    state: String,
    armed_unix_seconds: u64,
    epoch: u64,
    prepare_receipt: FileBinding,
    active_source: String,
    candidate: String,
    source_directory: DirectoryIdentity,
    target_directory: DirectoryIdentity,
    target_generation_digest: String,
    spyx_completion_gate: CompletionGate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceFreezeComplete {
    schema_version: u32,
    kind: String,
    state: String,
    completed_unix_seconds: u64,
    epoch: u64,
    switch_intent: FileBinding,
    quiescence_attempt_file: String,
    quiescence_attempt: FileBinding,
    source_path: String,
    source_directory: DirectoryIdentity,
    frozen_directory_identity: FileIdentity,
    files: BTreeMap<String, FrozenFileProof>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SwitchComplete {
    schema_version: u32,
    kind: String,
    state: String,
    completed_unix_seconds: u64,
    epoch: u64,
    intent: FileBinding,
    source_freeze: FileBinding,
    quiescence_attempt_file: String,
    quiescence_attempt: FileBinding,
    exchange_attempt_file: String,
    exchange_attempt: FileBinding,
    active_target: String,
    preserved_legacy_source: String,
    target_directory: DirectoryIdentity,
    source_directory: DirectoryIdentity,
    active_target_directory_identity: FileIdentity,
    preserved_source_directory_identity: FileIdentity,
    target_generation_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RollbackIntent {
    schema_version: u32,
    kind: String,
    state: String,
    epoch: u64,
    switch_complete: FileBinding,
    active_target: String,
    preserved_legacy_source: String,
    target_directory: DirectoryIdentity,
    source_directory: DirectoryIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RollbackComplete {
    schema_version: u32,
    kind: String,
    state: String,
    completed_unix_seconds: u64,
    epoch: u64,
    intent: FileBinding,
    quiescence_attempt_file: String,
    quiescence_attempt: FileBinding,
    exchange_attempt_file: String,
    exchange_attempt: FileBinding,
    restored_source: String,
    retained_target: String,
    source_directory: DirectoryIdentity,
    target_directory: DirectoryIdentity,
    restored_source_directory_identity: FileIdentity,
    retained_target_directory_identity: FileIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuiescenceAttempt {
    schema_version: u32,
    kind: String,
    state: String,
    operation: String,
    epoch: u64,
    parent_intent: FileBinding,
    reader_quiescence_path: String,
    reader_quiescence: FileBinding,
    current_host_boot_id: String,
    active_directory: DirectoryIdentity,
    retained_directory: DirectoryIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExchangeAttempt {
    schema_version: u32,
    kind: String,
    state: String,
    authorized_unix_seconds: u64,
    operation: String,
    epoch: u64,
    parent_intent: FileBinding,
    source_freeze: FileBinding,
    quiescence_attempt_file: String,
    quiescence_attempt: FileBinding,
    previous_exchange_attempt_file: Option<String>,
    previous_exchange_attempt: Option<FileBinding>,
    pre_active_directory: DirectoryIdentity,
    pre_retained_directory: DirectoryIdentity,
    desired_active_directory: DirectoryIdentity,
    desired_retained_directory: DirectoryIdentity,
    target_generation_digest: String,
}

struct Journal {
    directory: File,
    anchor: PathAnchor,
    mount_id: u64,
    _lock: FileLock,
}

struct FileLock {
    file: File,
    identity: FileIdentity,
}

struct ArchiveRootGuard {
    source: PinnedLocalRangeSource,
    directory: File,
    identity: DirectoryIdentity,
    mount_id: u64,
    lock: FileLock,
}

struct PathAnchor {
    path: PathBuf,
    parent_source: PinnedLocalRangeSource,
    parent: File,
    name: OsString,
    directory: File,
    identity: DirectoryIdentity,
}

#[derive(Clone)]
struct MarkerOverlay {
    source: PinnedLocalRangeSource,
    markers: Arc<BTreeMap<String, &'static [u8]>>,
}

impl RangeSource for MarkerOverlay {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        if let Some(bytes) = self.markers.get(object) {
            return Ok(Some(bytes.len() as u64));
        }
        self.source.size(object)
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        let Some(bytes) = self.markers.get(object) else {
            return self.source.read_range(object, offset, length);
        };
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| SourceError::OutOfBounds {
                object: object.to_owned(),
                offset,
                length,
                size: bytes.len() as u64,
            })?;
        if end > bytes.len() as u64 {
            return Err(SourceError::OutOfBounds {
                object: object.to_owned(),
                offset,
                length,
                size: bytes.len() as u64,
            });
        }
        Ok(bytes[offset as usize..end as usize].to_vec())
    }
}

fn main() -> Result<()> {
    let result = match Cli::parse().command {
        Command::Prepare(args) => prepare(args).map(serde_json::to_value),
        Command::ArmCutover(args) => arm_cutover(args).map(serde_json::to_value),
        Command::Cutover(args) => cutover(args).map(serde_json::to_value),
        Command::Rollback(args) => rollback(args).map(serde_json::to_value),
    }??;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

fn prepare(args: PrepareArgs) -> Result<PrepareReceipt> {
    validate_sha256(&args.source_authority_sha256)?;
    validate_sha256(&args.candidate_sha256)?;
    validate_sha256(&args.receipt_sha256)?;
    validate_sha256(&args.expected_spyx_epoch_900_source_digest)?;
    ensure!(
        args.expected_spyx_epoch_900_source_digest == OBSERVED_SPYX_EPOCH_900_SOURCE_DIGEST,
        "SPYX epoch-900 source digest differs from the pinned live-run observation"
    );
    validate_prepare_paths(&args)?;
    let journal = Journal::open_or_create(&args.journal)?;
    let archive_root_source = PinnedLocalRangeSource::open_directory(
        args.source
            .parent()
            .context("active source has no archive root")?,
    )
    .map_err(|error| anyhow!(error))?;
    journal.require_same_mount_as(&archive_root_source.directory_file()?)?;
    if let Some(existing) = journal.read_optional_json::<PrepareReceipt>(PREPARE_FILE)? {
        validate_prepare_receipt(&existing)?;
        ensure!(
            existing.epoch == args.epoch
                && existing.active_source == args.source.display().to_string()
                && existing.original_candidate == args.candidate.display().to_string()
                && existing.candidate == args.selector.display().to_string()
                && existing.source_authority.sha256 == args.source_authority_sha256
                && existing.normalization_candidate.sha256 == args.candidate_sha256
                && existing.normalization_receipt.sha256 == args.receipt_sha256
                && existing.spyx_epoch_900_source_generation_digest
                    == args.expected_spyx_epoch_900_source_digest,
            "existing prepare receipt differs from this command"
        );
        revalidate_existing_prepare(&args, &journal, &existing)?;
        return Ok(existing);
    }

    let source = PinnedLocalRangeSource::open_directory(&args.source)
        .map_err(|error| anyhow!(error))
        .context("open active historical source through a directory capability")?;
    let existing_relocation =
        journal.read_optional_json::<RelocationIntent>(RELOCATION_INTENT_FILE)?;
    let candidate_input_path = locate_relocation_input(&args, existing_relocation.as_ref())?;
    let candidate_source = PinnedLocalRangeSource::open_directory(&candidate_input_path)
        .map_err(|error| anyhow!(error))
        .context("open normalization candidate through a directory capability")?;
    let authority_bytes = read_absolute_regular_bounded(&args.source_authority, MAX_JSON_BYTES)?;
    let authority_binding = binding_for_bytes(&authority_bytes);
    ensure!(
        authority_binding.sha256 == args.source_authority_sha256,
        "source authority SHA-256 differs from the command"
    );
    let authority: SourceAuthorityInventory =
        parse_json(&authority_bytes, "source authority inventory")?;
    authority.validate()?;

    let candidate_bytes = candidate_source
        .read_all_bounded(CANDIDATE_FILE, MAX_JSON_BYTES)
        .map_err(|error| anyhow!(error))?;
    let candidate_binding = binding_for_bytes(&candidate_bytes);
    ensure!(
        candidate_binding.sha256 == args.candidate_sha256,
        "normalization candidate SHA-256 differs from the command"
    );
    let candidate: NormalizationCandidate =
        parse_json(&candidate_bytes, "normalization candidate")?;
    let receipt_bytes = candidate_source
        .read_all_bounded(RECEIPT_FILE, MAX_JSON_BYTES)
        .map_err(|error| anyhow!(error))?;
    let receipt_binding = binding_for_bytes(&receipt_bytes);
    ensure!(
        receipt_binding.sha256 == args.receipt_sha256,
        "normalization receipt SHA-256 differs from the command"
    );
    let receipt: NormalizationReceipt = parse_json(&receipt_bytes, "normalization receipt")?;
    validate_normalization_documents(
        &args,
        &candidate,
        &receipt,
        &candidate_binding,
        &authority,
        &authority_binding,
    )?;
    let message_profile = ArchiveV2WireProfile::from_str(&candidate.source_message_wire_profile)
        .map_err(|error| anyhow!(error))?;
    validate_candidate_inventory(&candidate_source, &candidate.files, message_profile, false)?;
    hash_candidate_payload(&candidate_source, &candidate.files)?;
    validate_source_authority(
        &source,
        &authority,
        &candidate.source_files,
        &candidate.ignored_unrelated_source_entries,
        &candidate.source_directory_identity,
    )?;
    let spyx_trusted_local_source = reconstruct_spyx_trusted_local_source_proof(
        &source,
        &candidate.source_files,
        &args.expected_spyx_epoch_900_source_digest,
    )?;
    let relocation = RelocationIntent {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-relocation-intent".to_owned(),
        state: "ready-for-no-replace-relocation".to_owned(),
        epoch: args.epoch,
        original_candidate: args.candidate.display().to_string(),
        selector: args.selector.display().to_string(),
        target_directory: directory_identity(&candidate_source.directory_file()?.metadata()?)?,
        normalization_candidate: candidate_binding.clone(),
        normalization_receipt: receipt_binding.clone(),
    };
    validate_relocation_intent(&relocation, &args)?;
    if let Some(existing) = existing_relocation.as_ref() {
        ensure!(
            existing == &relocation,
            "existing relocation intent differs from the verified candidate"
        );
    }
    journal.write_or_validate_json(RELOCATION_INTENT_FILE, &relocation)?;
    relocate_candidate_to_selector(&args.source, &relocation, &journal)?;

    let candidate_source = PinnedLocalRangeSource::open_directory(&args.selector)
        .map_err(|error| anyhow!(error))
        .context("open relocated candidate through its durable selector")?;
    ensure!(
        directory_identity(&candidate_source.directory_file()?.metadata()?)?
            == relocation.target_directory,
        "relocated selector has the wrong directory identity"
    );
    ensure!(
        binding_for_bytes(
            &candidate_source
                .read_all_bounded(CANDIDATE_FILE, MAX_JSON_BYTES)
                .map_err(|error| anyhow!(error))?,
        ) == candidate_binding
            && binding_for_bytes(
                &candidate_source
                    .read_all_bounded(RECEIPT_FILE, MAX_JSON_BYTES)
                    .map_err(|error| anyhow!(error))?,
            ) == receipt_binding,
        "relocated normalization controls differ"
    );
    validate_candidate_inventory(&candidate_source, &candidate.files, message_profile, false)?;
    hash_candidate_payload(&candidate_source, &candidate.files)?;

    let target_candidate_manifest = build_target_candidate_manifest(&candidate)?;
    ensure!(
        target_candidate_manifest.generation_digest == candidate.target_candidate_digest,
        "normalizer target candidate digest is not reproducible"
    );
    let expected_manifest =
        build_published_manifest(&candidate, &candidate_binding, &receipt_binding)?;
    publish_candidate_controls_descriptor_relative(
        &args.selector,
        &candidate_source,
        &expected_manifest,
        &candidate,
        &receipt,
    )?;

    let published =
        PinnedLocalRangeSource::open_directory(&args.selector).map_err(|error| anyhow!(error))?;
    validate_published_controls(&published, &expected_manifest)?;
    validate_candidate_inventory(&published, &candidate.files, message_profile, true)?;
    freeze_candidate(&published)?;
    let frozen =
        PinnedLocalRangeSource::open_directory(&args.selector).map_err(|error| anyhow!(error))?;
    validate_published_controls(&frozen, &expected_manifest)?;
    let (target_files, publication_lock_identity, target_frozen_directory_identity) =
        collect_frozen_target_proofs(
            &frozen,
            &candidate.files,
            &candidate_binding,
            &receipt_binding,
            &expected_manifest,
        )?;
    frozen.verify_unchanged().map_err(|error| anyhow!(error))?;
    verify_source_identities(
        &source.directory_file()?,
        &candidate.source_files,
        &candidate.ignored_unrelated_source_entries,
    )?;
    source.verify_unchanged().map_err(|error| anyhow!(error))?;

    let generation_manifest_bytes = frozen
        .read_all_bounded(GENERATION_MANIFEST_FILE, MAX_JSON_BYTES)
        .map_err(|error| anyhow!(error))?;
    let prepare = PrepareReceipt {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-publication-prepare".to_owned(),
        state: "audited-frozen-unselected-generation".to_owned(),
        prepared_unix_seconds: unix_seconds()?,
        epoch: args.epoch,
        active_source: args.source.display().to_string(),
        original_candidate: args.candidate.display().to_string(),
        candidate: args.selector.display().to_string(),
        source_authority_path: args.source_authority.display().to_string(),
        source_authority: authority_binding,
        normalization_candidate: candidate_binding,
        normalization_receipt: receipt_binding,
        source_directory: directory_identity(&source.directory_file()?.metadata()?)?,
        target_directory: directory_identity(&frozen.directory_file()?.metadata()?)?,
        target_frozen_directory_identity,
        cluster_id: candidate.cluster_id,
        generation_id: candidate.candidate_id,
        target_candidate_digest: candidate.target_candidate_digest,
        generation_digest: expected_manifest.generation_digest,
        spyx_epoch_900_source_generation_digest: spyx_trusted_local_source
            .generation_binding_digest
            .clone(),
        spyx_trusted_local_source,
        generation_manifest: binding_for_bytes(&generation_manifest_bytes),
        message_marker: candidate.authorized_message_marker,
        metadata_marker: candidate.authorized_metadata_marker,
        source_files: candidate.source_files,
        ignored_unrelated_source_entries: candidate.ignored_unrelated_source_entries,
        target_files,
        publication_lock_identity,
        target_frozen: true,
        full_descriptor_publication_audit_completed: true,
    };
    validate_prepare_receipt(&prepare)?;
    journal.write_json_no_replace(PREPARE_FILE, &prepare)?;
    Ok(prepare)
}

fn revalidate_existing_prepare(
    args: &PrepareArgs,
    journal: &Journal,
    existing: &PrepareReceipt,
) -> Result<()> {
    ensure!(
        journal
            .read_optional_json::<SwitchIntent>(SWITCH_INTENT_FILE)?
            .is_none()
            && journal
                .read_optional_json::<SourceFreezeComplete>(SOURCE_FREEZE_FILE)?
                .is_none()
            && journal
                .read_optional_json::<SwitchComplete>(SWITCH_COMPLETE_FILE)?
                .is_none()
            && journal
                .read_optional_json::<RollbackIntent>(ROLLBACK_INTENT_FILE)?
                .is_none()
            && journal
                .read_optional_json::<RollbackComplete>(ROLLBACK_COMPLETE_FILE)?
                .is_none(),
        "prepare cannot resume after the cutover state machine has started"
    );
    let (relocation, _) =
        journal.read_required_bound::<RelocationIntent>(RELOCATION_INTENT_FILE)?;
    validate_relocation_intent(&relocation, args)?;
    ensure!(
        relocation.target_directory == existing.target_directory
            && relocation.normalization_candidate == existing.normalization_candidate
            && relocation.normalization_receipt == existing.normalization_receipt,
        "existing relocation and prepare receipts differ"
    );
    ensure!(
        PathAnchor::open_optional(Path::new(&existing.original_candidate))?.is_none(),
        "original staging path reappeared after candidate relocation"
    );

    let source = PinnedLocalRangeSource::open_directory(&args.source)
        .map_err(|error| anyhow!(error))
        .context("reopen historical source for full prepare revalidation")?;
    let target = PinnedLocalRangeSource::open_directory(&args.selector)
        .map_err(|error| anyhow!(error))
        .context("reopen frozen target for full prepare revalidation")?;
    ensure!(
        directory_identity(&source.directory_file()?.metadata()?)? == existing.source_directory
            && directory_identity(&target.directory_file()?.metadata()?)?
                == existing.target_directory,
        "prepared directories are not in the exact pre-cutover topology"
    );

    let authority_bytes = read_absolute_regular_bounded(&args.source_authority, MAX_JSON_BYTES)?;
    let authority_binding = binding_for_bytes(&authority_bytes);
    ensure!(
        authority_binding == existing.source_authority
            && authority_binding.sha256 == args.source_authority_sha256,
        "source authority differs from the existing prepare receipt"
    );
    let authority: SourceAuthorityInventory =
        parse_json(&authority_bytes, "source authority inventory")?;
    authority.validate()?;

    let candidate_bytes = target.read_all_bounded(CANDIDATE_FILE, MAX_JSON_BYTES)?;
    let candidate_binding = binding_for_bytes(&candidate_bytes);
    ensure!(
        candidate_binding == existing.normalization_candidate
            && candidate_binding.sha256 == args.candidate_sha256,
        "normalization candidate differs from the existing prepare receipt"
    );
    let candidate: NormalizationCandidate =
        parse_json(&candidate_bytes, "normalization candidate")?;
    let receipt_bytes = target.read_all_bounded(RECEIPT_FILE, MAX_JSON_BYTES)?;
    let receipt_binding = binding_for_bytes(&receipt_bytes);
    ensure!(
        receipt_binding == existing.normalization_receipt
            && receipt_binding.sha256 == args.receipt_sha256,
        "normalization receipt differs from the existing prepare receipt"
    );
    let receipt: NormalizationReceipt = parse_json(&receipt_bytes, "normalization receipt")?;
    validate_normalization_documents(
        args,
        &candidate,
        &receipt,
        &candidate_binding,
        &authority,
        &authority_binding,
    )?;
    ensure!(
        existing.cluster_id == candidate.cluster_id
            && existing.generation_id == candidate.candidate_id
            && existing.target_candidate_digest == candidate.target_candidate_digest
            && existing.message_marker == candidate.authorized_message_marker
            && existing.metadata_marker == candidate.authorized_metadata_marker
            && existing.source_files == candidate.source_files
            && existing.ignored_unrelated_source_entries
                == candidate.ignored_unrelated_source_entries,
        "existing prepare receipt does not bind the normalization proof"
    );
    validate_source_authority(
        &source,
        &authority,
        &candidate.source_files,
        &candidate.ignored_unrelated_source_entries,
        &candidate.source_directory_identity,
    )?;
    let reconstructed_spyx_source = reconstruct_spyx_trusted_local_source_proof(
        &source,
        &candidate.source_files,
        &args.expected_spyx_epoch_900_source_digest,
    )?;
    ensure!(
        reconstructed_spyx_source == existing.spyx_trusted_local_source
            && reconstructed_spyx_source.generation_binding_digest
                == existing.spyx_epoch_900_source_generation_digest,
        "reconstructed SPYX trusted-local source proof differs from the prepare receipt"
    );

    let target_candidate_manifest = build_target_candidate_manifest(&candidate)?;
    ensure!(
        target_candidate_manifest.generation_digest == existing.target_candidate_digest,
        "existing target candidate digest is not reproducible"
    );
    let published_manifest =
        build_published_manifest(&candidate, &candidate_binding, &receipt_binding)?;
    ensure!(
        published_manifest.generation_digest == existing.generation_digest,
        "existing published generation digest is not reproducible"
    );
    let message_profile = ArchiveV2WireProfile::from_str(&candidate.source_message_wire_profile)
        .map_err(|error| anyhow!(error))?;
    validate_candidate_inventory(&target, &candidate.files, message_profile, true)?;
    validate_published_controls(&target, &published_manifest)?;
    hash_candidate_payload(&target, &candidate.files)?;
    let target_directory = target.directory_file()?;
    let publication_lock =
        FileLock::acquire_existing_at(&target_directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
    audit_candidate_semantics(&target, &published_manifest, &candidate, &receipt)?;
    publication_lock.recheck_at(&target_directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;

    let (target_files, publication_lock_identity, target_frozen_directory_identity) =
        collect_frozen_target_proofs(
            &target,
            &candidate.files,
            &candidate_binding,
            &receipt_binding,
            &published_manifest,
        )?;
    ensure!(
        target_files == existing.target_files
            && publication_lock_identity == existing.publication_lock_identity
            && target_frozen_directory_identity == existing.target_frozen_directory_identity,
        "frozen target proofs differ from the existing prepare receipt"
    );
    ensure!(
        binding_for_bytes(&target.read_all_bounded(GENERATION_MANIFEST_FILE, MAX_JSON_BYTES)?)
            == existing.generation_manifest,
        "published manifest bytes differ from the existing prepare receipt"
    );
    source.verify_unchanged().map_err(|error| anyhow!(error))?;
    target.verify_unchanged().map_err(|error| anyhow!(error))?;
    verify_prepared_topology(existing, true, None)?;
    journal.recheck_path()?;
    Ok(())
}

fn arm_cutover(args: ArmCutoverArgs) -> Result<SwitchIntent> {
    validate_sha256(&args.spyx_root_manifest_sha256)?;
    validate_sha256(&args.spyx_resume_checkpoint_sha256)?;
    validate_sha256(&args.expected_spyx_epoch_900_source_digest)?;
    validate_sha256(&args.spyx_process_authority_sha256)?;
    ensure!(
        args.spyx_process_authority == Path::new(SPYX_PROCESS_AUTHORITY_PATH)
            && args.spyx_process_authority_sha256 == SPYX_PROCESS_AUTHORITY_SHA256,
        "arm-cutover requires the fixed live-process authority path and SHA-256"
    );
    ensure!(
        args.spyx_root_manifest == Path::new(SPYX_OUTPUT_ROOT).join(DUMP_MANIFEST_FILE)
            && args.spyx_root_manifest_sha256 == SPYX_FINAL_ROOT_MANIFEST_SHA256
            && args.spyx_resume_checkpoint
                == Path::new(SPYX_OUTPUT_ROOT).join(RESUME_CHECKPOINT_FILE)
            && args.spyx_resume_checkpoint_sha256 == SPYX_FINAL_CHECKPOINT_SHA256,
        "arm-cutover requires the independently accepted final SPYX controls"
    );
    let journal = Journal::open_existing(&args.journal)?;
    let (prepare, prepare_binding) = journal.read_required_bound::<PrepareReceipt>(PREPARE_FILE)?;
    validate_prepare_receipt(&prepare)?;
    {
        let root_guard = ArchiveRootGuard::acquire(Path::new(&prepare.active_source))?;
        journal.require_same_mount_as(&root_guard.directory)?;
        root_guard.recheck_generation_paths(&prepare)?;
    }
    if journal
        .read_optional_json::<RollbackComplete>(ROLLBACK_COMPLETE_FILE)?
        .is_some()
    {
        bail!("this publication was rolled back; use a new journal for another cutover");
    }
    if journal
        .read_optional_json::<RollbackIntent>(ROLLBACK_INTENT_FILE)?
        .is_some()
    {
        bail!("rollback intent exists; cutover cannot reverse an interrupted rollback");
    }
    if let Some(existing) = journal.read_optional_json::<SwitchIntent>(SWITCH_INTENT_FILE)? {
        let binding = binding_for_bytes(&pretty_json_bytes(&existing)?);
        validate_switch_intent(&existing, &prepare, &prepare_binding)?;
        ensure!(
            existing.spyx_completion_gate.root_manifest_path
                == args.spyx_root_manifest.display().to_string()
                && existing.spyx_completion_gate.root_manifest.sha256
                    == args.spyx_root_manifest_sha256
                && existing.spyx_completion_gate.resume_checkpoint_path
                    == args.spyx_resume_checkpoint.display().to_string()
                && existing.spyx_completion_gate.resume_checkpoint.sha256
                    == args.spyx_resume_checkpoint_sha256
                && existing.spyx_completion_gate.process_authority_path
                    == args.spyx_process_authority.display().to_string()
                && existing.spyx_completion_gate.process_authority.sha256
                    == args.spyx_process_authority_sha256
                && args.expected_spyx_first_epoch == SPYX_FIRST_EPOCH
                && args.expected_spyx_last_epoch == SPYX_LAST_EPOCH
                && args.expected_spyx_epoch_900_source_digest
                    == prepare.spyx_epoch_900_source_generation_digest,
            "arm-cutover arguments differ from the durable switch intent"
        );
        if let Some(complete) =
            journal.read_optional_json::<SwitchComplete>(SWITCH_COMPLETE_FILE)?
        {
            let (source_freeze, source_freeze_binding) =
                read_and_validate_source_freeze(&journal, &prepare, &binding)?;
            let (exchange_attempt, exchange_attempt_binding) = read_and_validate_exchange_attempt(
                &journal,
                &complete.exchange_attempt_file,
                "cutover",
                &prepare,
                &binding,
                &source_freeze_binding,
            )?;
            validate_switch_complete(
                &complete,
                &prepare,
                &binding,
                &source_freeze_binding,
                &complete.exchange_attempt_file,
                &exchange_attempt,
                &exchange_attempt_binding,
            )?;
            let root_guard = ArchiveRootGuard::acquire(Path::new(&prepare.active_source))?;
            journal.require_same_mount_as(&root_guard.directory)?;
            root_guard.recheck_generation_paths(&prepare)?;
            let (post, _, _) = current_generation_topology(&prepare)?;
            ensure!(post, "completed cutover is not in selected-target topology");
            verify_prepared_topology(&prepare, false, Some(&source_freeze))?;
        } else {
            if journal
                .read_optional_json::<SourceFreezeComplete>(SOURCE_FREEZE_FILE)?
                .is_some()
            {
                let (source_freeze, _) =
                    read_and_validate_source_freeze(&journal, &prepare, &binding)?;
                verify_prepared_topology(&prepare, true, Some(&source_freeze))?;
            } else {
                revalidate_completion_gate(&existing.spyx_completion_gate)?;
                if args.reader_quiescence_receipt.is_some() {
                    freeze_armed_source_if_requested(
                        &args,
                        &journal,
                        &prepare,
                        &prepare_binding,
                        &existing,
                        &binding,
                    )?;
                } else {
                    verify_prepared_topology(&prepare, true, None)?;
                }
            }
        }
        return Ok(existing);
    }
    let gate = validate_spyx_completion(
        &args.spyx_root_manifest,
        &args.spyx_root_manifest_sha256,
        &args.spyx_resume_checkpoint,
        &args.spyx_resume_checkpoint_sha256,
        &prepare,
        args.expected_spyx_first_epoch,
        args.expected_spyx_last_epoch,
        &args.expected_spyx_epoch_900_source_digest,
        &args.spyx_process_authority,
        &args.spyx_process_authority_sha256,
    )?;
    let root_guard = ArchiveRootGuard::acquire(Path::new(&prepare.active_source))?;
    journal.require_same_mount_as(&root_guard.directory)?;
    root_guard.recheck_generation_paths(&prepare)?;
    verify_source_content_against_prepare(&prepare)?;
    let intent = SwitchIntent {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-switch-intent".to_owned(),
        state: "ready-for-atomic-directory-exchange".to_owned(),
        armed_unix_seconds: unix_seconds()?,
        epoch: prepare.epoch,
        prepare_receipt: prepare_binding.clone(),
        active_source: prepare.active_source.clone(),
        candidate: prepare.candidate.clone(),
        source_directory: prepare.source_directory,
        target_directory: prepare.target_directory,
        target_generation_digest: prepare.generation_digest.clone(),
        spyx_completion_gate: gate.clone(),
    };
    validate_switch_intent(&intent, &prepare, &prepare_binding)?;
    let intent_binding = journal.write_or_validate_json(SWITCH_INTENT_FILE, &intent)?;
    verify_prepared_topology(&prepare, true, None)?;
    root_guard.recheck_generation_paths(&prepare)?;
    drop(root_guard);
    freeze_armed_source_if_requested(
        &args,
        &journal,
        &prepare,
        &prepare_binding,
        &intent,
        &intent_binding,
    )?;
    journal.recheck_path()?;
    Ok(intent)
}

fn freeze_armed_source_if_requested(
    args: &ArmCutoverArgs,
    journal: &Journal,
    prepare: &PrepareReceipt,
    prepare_binding: &FileBinding,
    intent: &SwitchIntent,
    intent_binding: &FileBinding,
) -> Result<()> {
    let (receipt_path, receipt_sha256) = match (
        args.reader_quiescence_receipt.as_deref(),
        args.reader_quiescence_receipt_sha256.as_deref(),
    ) {
        (None, None) => return Ok(()),
        (Some(path), Some(sha256)) => (path, sha256),
        _ => bail!("source-freeze quiescence path and SHA-256 must be supplied together"),
    };
    validate_sha256(receipt_sha256)?;
    let gate = &intent.spyx_completion_gate;
    let spyx_output_root = Path::new(&gate.root_manifest_path)
        .parent()
        .context("SPYX manifest has no output root")?;
    let root_guard = ArchiveRootGuard::acquire(Path::new(&prepare.active_source))?;
    journal.require_same_mount_as(&root_guard.directory)?;
    journal.recheck_path()?;
    ensure_no_open_generation_fds(prepare, Some((&gate.output_snapshot, spyx_output_root)))?;
    root_guard.recheck_generation_paths(prepare)?;
    revalidate_completion_gate(gate)?;
    let (post, active_directory, retained_directory) = current_generation_topology(prepare)?;
    ensure!(!post, "source freeze requires the pre-cutover topology");
    let quiescence = validate_reader_quiescence(
        receipt_path,
        receipt_sha256,
        "freeze-source",
        prepare,
        prepare_binding,
        intent.armed_unix_seconds,
        active_directory,
        retained_directory,
        Some(intent_binding),
        None,
        None,
        Some(&gate.root_manifest.sha256),
        Some(&gate.resume_checkpoint.sha256),
        Some(&prepare.spyx_epoch_900_source_generation_digest),
        Some((
            gate.extractor_pid,
            gate.extractor_start_ticks,
            &gate.extractor_boot_id,
            Path::new(&gate.extractor_executable_path),
            &gate.extractor_executable.sha256,
            spyx_output_root,
        )),
    )?;
    // The last slow checks run before the freshness check. This prevents a
    // valid 300-second receipt from expiring while /proc or the SPYX tree is
    // scanned and then reaching the first fchmod.
    ensure_no_open_generation_fds(prepare, Some((&gate.output_snapshot, spyx_output_root)))?;
    revalidate_completion_gate(gate)?;
    root_guard.recheck_generation_paths(prepare)?;
    let target_directory = directory_with_identity(prepare, prepare.target_directory)?;
    verify_target_identities(
        &target_directory,
        &prepare.target_files,
        &prepare.publication_lock_identity,
        &prepare.target_frozen_directory_identity,
    )?;
    let (freeze, _) = ensure_source_frozen(journal, prepare, intent_binding, || {
        let (attempt_file, attempt_binding) = publish_quiescence_attempt(
            journal,
            "freeze-source",
            prepare.epoch,
            intent_binding,
            receipt_path,
            &quiescence,
            active_directory,
            retained_directory,
        )?;
        ensure_no_open_generation_fds(prepare, Some((&gate.output_snapshot, spyx_output_root)))?;
        revalidate_completion_gate(gate)?;
        verify_target_identities(
            &target_directory,
            &prepare.target_files,
            &prepare.publication_lock_identity,
            &prepare.target_frozen_directory_identity,
        )?;
        ensure!(
            validate_reader_quiescence(
                receipt_path,
                receipt_sha256,
                "freeze-source",
                prepare,
                prepare_binding,
                intent.armed_unix_seconds,
                active_directory,
                retained_directory,
                Some(intent_binding),
                None,
                None,
                Some(&gate.root_manifest.sha256),
                Some(&gate.resume_checkpoint.sha256),
                Some(&prepare.spyx_epoch_900_source_generation_digest),
                Some((
                    gate.extractor_pid,
                    gate.extractor_start_ticks,
                    &gate.extractor_boot_id,
                    Path::new(&gate.extractor_executable_path),
                    &gate.extractor_executable.sha256,
                    spyx_output_root,
                )),
            )? == quiescence,
            "source-freeze quiescence proof changed at the mutation boundary"
        );
        root_guard.recheck_generation_paths(prepare)?;
        Ok((attempt_file, attempt_binding))
    })?;
    root_guard.recheck_generation_paths(prepare)?;
    verify_prepared_topology(prepare, true, Some(&freeze))?;
    journal.recheck_path()?;
    Ok(())
}

fn publish_switch_completion(
    journal: &Journal,
    prepare: &PrepareReceipt,
    intent_binding: &FileBinding,
    source_freeze_binding: &FileBinding,
    exchange_attempt_file: String,
    exchange_attempt: &ExchangeAttempt,
    exchange_attempt_binding: FileBinding,
) -> Result<SwitchComplete> {
    let active_target = PathAnchor::open(Path::new(&prepare.active_source))?;
    let preserved_source = PathAnchor::open(Path::new(&prepare.candidate))?;
    ensure!(
        active_target.identity == prepare.target_directory
            && preserved_source.identity == prepare.source_directory,
        "cutover completion requires the exact selected-target topology"
    );
    let complete = SwitchComplete {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-switch-complete".to_owned(),
        state: "canonical-target-selected-source-preserved".to_owned(),
        completed_unix_seconds: unix_seconds()?,
        epoch: prepare.epoch,
        intent: intent_binding.clone(),
        source_freeze: source_freeze_binding.clone(),
        quiescence_attempt_file: exchange_attempt.quiescence_attempt_file.clone(),
        quiescence_attempt: exchange_attempt.quiescence_attempt.clone(),
        exchange_attempt_file,
        exchange_attempt: exchange_attempt_binding.clone(),
        active_target: prepare.active_source.clone(),
        preserved_legacy_source: prepare.candidate.clone(),
        target_directory: prepare.target_directory,
        source_directory: prepare.source_directory,
        active_target_directory_identity: file_identity(
            &active_target.directory.metadata()?,
            false,
        )?,
        preserved_source_directory_identity: file_identity(
            &preserved_source.directory.metadata()?,
            false,
        )?,
        target_generation_digest: prepare.generation_digest.clone(),
    };
    validate_switch_complete(
        &complete,
        prepare,
        intent_binding,
        source_freeze_binding,
        &complete.exchange_attempt_file,
        exchange_attempt,
        &exchange_attempt_binding,
    )?;
    journal.write_or_validate_json(SWITCH_COMPLETE_FILE, &complete)?;
    Ok(complete)
}

fn publish_rollback_completion(
    journal: &Journal,
    prepare: &PrepareReceipt,
    intent_binding: &FileBinding,
    exchange_attempt_file: String,
    exchange_attempt: &ExchangeAttempt,
    exchange_attempt_binding: FileBinding,
) -> Result<RollbackComplete> {
    let restored_source = PathAnchor::open(Path::new(&prepare.active_source))?;
    let retained_target = PathAnchor::open(Path::new(&prepare.candidate))?;
    ensure!(
        restored_source.identity == prepare.source_directory
            && retained_target.identity == prepare.target_directory,
        "rollback completion requires the exact restored-source topology"
    );
    let complete = RollbackComplete {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-rollback-complete".to_owned(),
        state: "historical-source-restored-target-retained".to_owned(),
        completed_unix_seconds: unix_seconds()?,
        epoch: prepare.epoch,
        intent: intent_binding.clone(),
        quiescence_attempt_file: exchange_attempt.quiescence_attempt_file.clone(),
        quiescence_attempt: exchange_attempt.quiescence_attempt.clone(),
        exchange_attempt_file,
        exchange_attempt: exchange_attempt_binding.clone(),
        restored_source: prepare.active_source.clone(),
        retained_target: prepare.candidate.clone(),
        source_directory: prepare.source_directory,
        target_directory: prepare.target_directory,
        restored_source_directory_identity: file_identity(
            &restored_source.directory.metadata()?,
            false,
        )?,
        retained_target_directory_identity: file_identity(
            &retained_target.directory.metadata()?,
            false,
        )?,
    };
    validate_rollback_complete(
        &complete,
        prepare,
        intent_binding,
        &complete.exchange_attempt_file,
        exchange_attempt,
        &exchange_attempt_binding,
    )?;
    journal.write_or_validate_json(ROLLBACK_COMPLETE_FILE, &complete)?;
    Ok(complete)
}

fn cutover(args: CutoverArgs) -> Result<SwitchComplete> {
    validate_sha256(&args.reader_quiescence_receipt_sha256)?;
    let journal = Journal::open_existing(&args.journal)?;
    let (prepare, prepare_binding) = journal.read_required_bound::<PrepareReceipt>(PREPARE_FILE)?;
    validate_prepare_receipt(&prepare)?;
    if journal
        .read_optional_json::<RollbackComplete>(ROLLBACK_COMPLETE_FILE)?
        .is_some()
    {
        bail!("this publication was rolled back; use a new journal for another cutover");
    }
    if journal
        .read_optional_json::<RollbackIntent>(ROLLBACK_INTENT_FILE)?
        .is_some()
    {
        bail!("rollback intent exists; cutover cannot reverse an interrupted rollback");
    }
    let (intent, intent_binding) =
        journal.read_required_bound::<SwitchIntent>(SWITCH_INTENT_FILE)?;
    validate_switch_intent(&intent, &prepare, &prepare_binding)?;
    let gate = &intent.spyx_completion_gate;
    let root_guard = ArchiveRootGuard::acquire(Path::new(&prepare.active_source))?;
    journal.require_same_mount_as(&root_guard.directory)?;
    journal.recheck_path()?;

    let (source_freeze, source_freeze_binding) =
        read_and_validate_source_freeze(&journal, &prepare, &intent_binding)
            .context("cutover is not armed with a completed source freeze")?;
    if let Some(existing) = journal.read_optional_json::<SwitchComplete>(SWITCH_COMPLETE_FILE)? {
        let (exchange_attempt, exchange_attempt_binding) = read_and_validate_exchange_attempt(
            &journal,
            &existing.exchange_attempt_file,
            "cutover",
            &prepare,
            &intent_binding,
            &source_freeze_binding,
        )?;
        validate_switch_complete(
            &existing,
            &prepare,
            &intent_binding,
            &source_freeze_binding,
            &existing.exchange_attempt_file,
            &exchange_attempt,
            &exchange_attempt_binding,
        )?;
        ensure!(
            existing.quiescence_attempt_file == exchange_attempt.quiescence_attempt_file
                && existing.exchange_attempt == exchange_attempt_binding,
            "cutover completion does not bind its exact exchange authorization"
        );
        let (post, _, _) = current_generation_topology(&prepare)?;
        ensure!(
            post,
            "cutover completion exists outside the selected target topology"
        );
        verify_prepared_topology(&prepare, false, Some(&source_freeze))?;
        let active_target = PathAnchor::open(Path::new(&prepare.active_source))?;
        let preserved_source = PathAnchor::open(Path::new(&prepare.candidate))?;
        ensure!(
            existing.active_target_directory_identity
                == file_identity(&active_target.directory.metadata()?, false)?
                && existing.preserved_source_directory_identity
                    == file_identity(&preserved_source.directory.metadata()?, false)?,
            "selected directory metadata changed after cutover completion"
        );
        return Ok(existing);
    }

    let target_directory = directory_with_identity(&prepare, prepare.target_directory)?;
    let target_publication_lock =
        FileLock::acquire_existing_at(&target_directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
    let (post, active_directory, retained_directory) = current_generation_topology(&prepare)?;
    if post {
        let (exchange_attempt_file, exchange_attempt, exchange_attempt_binding) =
            exchange_attempt_chain_tail(
                &journal,
                "cutover",
                &prepare,
                &intent_binding,
                &source_freeze_binding,
            )?
            .context("selected target has no durable pre-exchange authorization")?;
        ensure_selected_topology(&prepare, &source_freeze, true, &root_guard, || {
            bail!("post-exchange recovery tried to exchange a second time")
        })?;
        target_publication_lock.recheck_at(&target_directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
        journal.recheck_path()?;
        return publish_switch_completion(
            &journal,
            &prepare,
            &intent_binding,
            &source_freeze_binding,
            exchange_attempt_file,
            &exchange_attempt,
            exchange_attempt_binding,
        );
    }
    let spyx_output_root = Path::new(&gate.root_manifest_path)
        .parent()
        .context("SPYX manifest has no output root")?;
    // Fail before the bounded SPYX identity recheck if this process cannot
    // inspect all process descriptors and mappings. NAS cutover needs root.
    ensure_no_open_generation_fds(&prepare, Some((&gate.output_snapshot, spyx_output_root)))?;
    root_guard.recheck_generation_paths(&prepare)?;
    revalidate_completion_gate(gate)?;

    let quiescence = validate_reader_quiescence(
        &args.reader_quiescence_receipt,
        &args.reader_quiescence_receipt_sha256,
        "cutover",
        &prepare,
        &prepare_binding,
        source_freeze.completed_unix_seconds,
        active_directory,
        retained_directory,
        Some(&intent_binding),
        Some(&source_freeze_binding),
        None,
        Some(&gate.root_manifest.sha256),
        Some(&gate.resume_checkpoint.sha256),
        Some(&prepare.spyx_epoch_900_source_generation_digest),
        Some((
            gate.extractor_pid,
            gate.extractor_start_ticks,
            &gate.extractor_boot_id,
            Path::new(&gate.extractor_executable_path),
            &gate.extractor_executable.sha256,
            spyx_output_root,
        )),
    )?;
    ensure_no_open_generation_fds(&prepare, Some((&gate.output_snapshot, spyx_output_root)))?;
    journal.recheck_path()?;
    revalidate_completion_gate(gate)?;
    root_guard.recheck_generation_paths(&prepare)?;
    let exchange_authorization = RefCell::new(None);
    ensure_selected_topology(&prepare, &source_freeze, true, &root_guard, || {
        let (quiescence_attempt_file, quiescence_attempt) = publish_quiescence_attempt(
            &journal,
            "cutover",
            prepare.epoch,
            &intent_binding,
            &args.reader_quiescence_receipt,
            &quiescence,
            active_directory,
            retained_directory,
        )?;
        let (exchange_attempt_file, exchange_attempt) = publish_exchange_attempt(
            &journal,
            "cutover",
            &prepare,
            &intent_binding,
            &source_freeze_binding,
            &quiescence_attempt_file,
            &quiescence_attempt,
        )?;
        // Durable authorization comes first. Then repeat every slow proof so
        // neither the 300-second deadline nor a newly opened FD/map can be
        // hidden by journal fsync latency.
        ensure_no_open_generation_fds(&prepare, Some((&gate.output_snapshot, spyx_output_root)))?;
        revalidate_completion_gate(gate)?;
        verify_prepared_topology(&prepare, true, Some(&source_freeze))?;
        ensure!(
            validate_reader_quiescence(
                &args.reader_quiescence_receipt,
                &args.reader_quiescence_receipt_sha256,
                "cutover",
                &prepare,
                &prepare_binding,
                source_freeze.completed_unix_seconds,
                active_directory,
                retained_directory,
                Some(&intent_binding),
                Some(&source_freeze_binding),
                None,
                Some(&gate.root_manifest.sha256),
                Some(&gate.resume_checkpoint.sha256),
                Some(&prepare.spyx_epoch_900_source_generation_digest),
                Some((
                    gate.extractor_pid,
                    gate.extractor_start_ticks,
                    &gate.extractor_boot_id,
                    Path::new(&gate.extractor_executable_path),
                    &gate.extractor_executable.sha256,
                    spyx_output_root,
                )),
            )? == quiescence,
            "cutover quiescence proof changed at the final exchange boundary"
        );
        target_publication_lock.recheck_at(&target_directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
        *exchange_authorization.borrow_mut() = Some((
            exchange_attempt_file,
            exchange_attempt,
            quiescence_attempt_file,
            quiescence_attempt,
        ));
        Ok(())
    })?;
    target_publication_lock.recheck_at(&target_directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
    journal.recheck_path()?;
    let (exchange_attempt_file, exchange_attempt_binding, quiescence_attempt_file, quiescence) =
        exchange_authorization
            .into_inner()
            .context("cutover exchanged without a durable authorization")?;
    let (exchange_attempt, actual_exchange_binding) = read_and_validate_exchange_attempt(
        &journal,
        &exchange_attempt_file,
        "cutover",
        &prepare,
        &intent_binding,
        &source_freeze_binding,
    )?;
    ensure!(
        actual_exchange_binding == exchange_attempt_binding
            && exchange_attempt.quiescence_attempt_file == quiescence_attempt_file
            && exchange_attempt.quiescence_attempt == quiescence,
        "cutover exchange authorization changed after exchange"
    );
    publish_switch_completion(
        &journal,
        &prepare,
        &intent_binding,
        &source_freeze_binding,
        exchange_attempt_file,
        &exchange_attempt,
        exchange_attempt_binding,
    )
}

fn rollback(args: RollbackArgs) -> Result<RollbackComplete> {
    validate_sha256(&args.reader_quiescence_receipt_sha256)?;
    let journal = Journal::open_existing(&args.journal)?;
    let (prepare, prepare_binding) = journal.read_required_bound::<PrepareReceipt>(PREPARE_FILE)?;
    validate_prepare_receipt(&prepare)?;
    let (switch_complete, switch_binding) =
        journal.read_required_bound::<SwitchComplete>(SWITCH_COMPLETE_FILE)?;
    let (switch_intent, switch_intent_binding) =
        journal.read_required_bound::<SwitchIntent>(SWITCH_INTENT_FILE)?;
    validate_switch_intent(&switch_intent, &prepare, &prepare_binding)?;
    let (source_freeze, source_freeze_binding) =
        read_and_validate_source_freeze(&journal, &prepare, &switch_intent_binding)?;
    let (switch_exchange_attempt, switch_exchange_attempt_binding) =
        read_and_validate_exchange_attempt(
            &journal,
            &switch_complete.exchange_attempt_file,
            "cutover",
            &prepare,
            &switch_intent_binding,
            &source_freeze_binding,
        )?;
    validate_switch_complete(
        &switch_complete,
        &prepare,
        &switch_intent_binding,
        &source_freeze_binding,
        &switch_complete.exchange_attempt_file,
        &switch_exchange_attempt,
        &switch_exchange_attempt_binding,
    )?;
    let intent = RollbackIntent {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-rollback-intent".to_owned(),
        state: "ready-for-atomic-directory-exchange".to_owned(),
        epoch: prepare.epoch,
        switch_complete: switch_binding.clone(),
        active_target: prepare.active_source.clone(),
        preserved_legacy_source: prepare.candidate.clone(),
        target_directory: prepare.target_directory,
        source_directory: prepare.source_directory,
    };
    let root_guard = ArchiveRootGuard::acquire(Path::new(&prepare.active_source))?;
    journal.require_same_mount_as(&root_guard.directory)?;
    journal.recheck_path()?;
    if let Some(existing) =
        journal.read_optional_json::<RollbackComplete>(ROLLBACK_COMPLETE_FILE)?
    {
        let (existing_intent, intent_binding) =
            journal.read_required_bound::<RollbackIntent>(ROLLBACK_INTENT_FILE)?;
        validate_rollback_intent(&existing_intent, &intent, &switch_binding)?;
        let (exchange_attempt, exchange_attempt_binding) = read_and_validate_exchange_attempt(
            &journal,
            &existing.exchange_attempt_file,
            "rollback",
            &prepare,
            &intent_binding,
            &source_freeze_binding,
        )?;
        validate_rollback_complete(
            &existing,
            &prepare,
            &intent_binding,
            &existing.exchange_attempt_file,
            &exchange_attempt,
            &exchange_attempt_binding,
        )?;
        ensure!(
            existing.quiescence_attempt_file == exchange_attempt.quiescence_attempt_file
                && existing.exchange_attempt == exchange_attempt_binding,
            "rollback completion does not bind its exact exchange authorization"
        );
        let (post, _, _) = current_generation_topology(&prepare)?;
        ensure!(
            !post,
            "rollback completion exists outside the restored source topology"
        );
        verify_prepared_topology(&prepare, true, Some(&source_freeze))?;
        let restored_source = PathAnchor::open(Path::new(&prepare.active_source))?;
        let retained_target = PathAnchor::open(Path::new(&prepare.candidate))?;
        ensure!(
            existing.restored_source_directory_identity
                == file_identity(&restored_source.directory.metadata()?, false)?
                && existing.retained_target_directory_identity
                    == file_identity(&retained_target.directory.metadata()?, false)?,
            "directory metadata changed after rollback completion"
        );
        return Ok(existing);
    }

    let (post, active_directory, retained_directory) = current_generation_topology(&prepare)?;
    let had_rollback_intent = journal
        .read_optional_json::<RollbackIntent>(ROLLBACK_INTENT_FILE)?
        .is_some();
    ensure!(
        post || had_rollback_intent,
        "restored source topology exists without a prior durable rollback intent"
    );
    let intent_binding = journal.write_or_validate_json(ROLLBACK_INTENT_FILE, &intent)?;
    validate_rollback_intent(&intent, &intent, &switch_binding)?;
    if post {
        let active_target = PathAnchor::open(Path::new(&prepare.active_source))?;
        let preserved_source = PathAnchor::open(Path::new(&prepare.candidate))?;
        ensure!(
            file_identity(&active_target.directory.metadata()?, false)?
                == switch_complete.active_target_directory_identity
                && file_identity(&preserved_source.directory.metadata()?, false)?
                    == switch_complete.preserved_source_directory_identity,
            "selected directory metadata changed before rollback"
        );
    }
    let target_directory = directory_with_identity(&prepare, prepare.target_directory)?;
    let target_publication_lock =
        FileLock::acquire_existing_at(&target_directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
    if !post {
        let (exchange_attempt_file, exchange_attempt, exchange_attempt_binding) =
            exchange_attempt_chain_tail(
                &journal,
                "rollback",
                &prepare,
                &intent_binding,
                &source_freeze_binding,
            )?
            .context("restored source has no durable pre-exchange rollback authorization")?;
        ensure_selected_topology(&prepare, &source_freeze, false, &root_guard, || {
            bail!("post-exchange rollback recovery tried to exchange a second time")
        })?;
        target_publication_lock.recheck_at(&target_directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
        journal.recheck_path()?;
        return publish_rollback_completion(
            &journal,
            &prepare,
            &intent_binding,
            exchange_attempt_file,
            &exchange_attempt,
            exchange_attempt_binding,
        );
    }
    ensure_no_open_generation_fds(&prepare, None)?;
    root_guard.recheck_generation_paths(&prepare)?;
    let quiescence = validate_reader_quiescence(
        &args.reader_quiescence_receipt,
        &args.reader_quiescence_receipt_sha256,
        "rollback",
        &prepare,
        &prepare_binding,
        switch_complete.completed_unix_seconds,
        active_directory,
        retained_directory,
        Some(&switch_intent_binding),
        Some(&source_freeze_binding),
        Some(&switch_binding),
        None,
        None,
        None,
        None,
    )?;
    ensure_no_open_generation_fds(&prepare, None)?;
    root_guard.recheck_generation_paths(&prepare)?;
    let exchange_authorization = RefCell::new(None);
    ensure_selected_topology(&prepare, &source_freeze, false, &root_guard, || {
        let (quiescence_attempt_file, quiescence_attempt) = publish_quiescence_attempt(
            &journal,
            "rollback",
            prepare.epoch,
            &intent_binding,
            &args.reader_quiescence_receipt,
            &quiescence,
            active_directory,
            retained_directory,
        )?;
        let (exchange_attempt_file, exchange_attempt) = publish_exchange_attempt(
            &journal,
            "rollback",
            &prepare,
            &intent_binding,
            &source_freeze_binding,
            &quiescence_attempt_file,
            &quiescence_attempt,
        )?;
        ensure_no_open_generation_fds(&prepare, None)?;
        verify_prepared_topology(&prepare, false, Some(&source_freeze))?;
        ensure!(
            validate_reader_quiescence(
                &args.reader_quiescence_receipt,
                &args.reader_quiescence_receipt_sha256,
                "rollback",
                &prepare,
                &prepare_binding,
                switch_complete.completed_unix_seconds,
                active_directory,
                retained_directory,
                Some(&switch_intent_binding),
                Some(&source_freeze_binding),
                Some(&switch_binding),
                None,
                None,
                None,
                None,
            )? == quiescence,
            "rollback quiescence proof changed at the final exchange boundary"
        );
        target_publication_lock.recheck_at(&target_directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
        *exchange_authorization.borrow_mut() = Some((
            exchange_attempt_file,
            exchange_attempt,
            quiescence_attempt_file,
            quiescence_attempt,
        ));
        Ok(())
    })?;
    target_publication_lock.recheck_at(&target_directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
    journal.recheck_path()?;
    let (exchange_attempt_file, exchange_attempt_binding, quiescence_attempt_file, quiescence) =
        exchange_authorization
            .into_inner()
            .context("rollback exchanged without a durable authorization")?;
    let (exchange_attempt, actual_exchange_binding) = read_and_validate_exchange_attempt(
        &journal,
        &exchange_attempt_file,
        "rollback",
        &prepare,
        &intent_binding,
        &source_freeze_binding,
    )?;
    ensure!(
        actual_exchange_binding == exchange_attempt_binding
            && exchange_attempt.quiescence_attempt_file == quiescence_attempt_file
            && exchange_attempt.quiescence_attempt == quiescence,
        "rollback exchange authorization changed after exchange"
    );
    publish_rollback_completion(
        &journal,
        &prepare,
        &intent_binding,
        exchange_attempt_file,
        &exchange_attempt,
        exchange_attempt_binding,
    )
}

fn validate_normalization_documents(
    args: &PrepareArgs,
    candidate: &NormalizationCandidate,
    receipt: &NormalizationReceipt,
    candidate_binding: &FileBinding,
    authority: &SourceAuthorityInventory,
    authority_binding: &FileBinding,
) -> Result<()> {
    ensure!(
        candidate.schema_version == 1
            && candidate.kind == "archive-v2-metadata-normalization-candidate"
            && candidate.state == "audited-unpublished-candidate"
            && candidate.metadata_schema == ArchiveV2MetadataWireProfile::CURRENT_NAME,
        "normalization candidate is not the supported audited schema"
    );
    ensure!(
        receipt.schema_version == 1
            && receipt.kind == "archive-v2-metadata-normalization-receipt"
            && receipt.state == "complete-unpublished-staging-generation"
            && !receipt.canonical_publication_performed
            && receipt.source_revalidated_at_completion,
        "normalization receipt is not a complete unpublished receipt"
    );
    ensure!(
        receipt.completed_unix_seconds != 0,
        "normalization receipt has no completion time"
    );
    ensure!(
        candidate.epoch == args.epoch
            && receipt.epoch == args.epoch
            && candidate.source == args.source.display().to_string()
            && receipt.source == candidate.source
            && candidate.staging == args.candidate.display().to_string()
            && receipt.staging == candidate.staging
            && candidate.slots_per_epoch == receipt.slots_per_epoch
            && candidate.cluster_id == receipt.cluster_id,
        "normalization paths or epoch identity differ"
    );
    ensure!(
        candidate.source_generation_id == receipt.source_generation_id
            && candidate.source_generation_digest == receipt.source_generation_digest
            && candidate.source_message_wire_profile == receipt.message_wire_profile
            && candidate.source_metadata_wire_profile == receipt.source_metadata_profile
            && candidate.candidate_id == receipt.target_candidate_id
            && candidate.target_candidate_digest == receipt.target_candidate_digest,
        "candidate and receipt generation identities differ"
    );
    ensure!(
        candidate.authorized_message_marker == receipt.message_marker
            && candidate.authorized_metadata_marker == receipt.metadata_marker
            && candidate.source_authority_kind == receipt.source_authority_kind
            && candidate.source_authority_id == receipt.source_authority_id
            && candidate.source_authority_binding == receipt.source_authority_binding
            && candidate.source_directory_identity == receipt.source_directory_identity
            && candidate.source_files == receipt.source_files
            && candidate.ignored_unrelated_source_entries
                == receipt.ignored_unrelated_source_entries
            && candidate.frame_processing == receipt.frame_processing,
        "candidate and receipt proof fields differ"
    );
    ensure!(
        candidate.ignored_unrelated_source_entries.is_empty(),
        "publication requires one exact flat source inventory with no ignored entries"
    );
    ensure!(
        receipt.candidate_manifest == *candidate_binding,
        "receipt does not bind the supplied normalization candidate"
    );
    ensure!(
        receipt.target_blocks
            == *candidate
                .files
                .get(BLOCKS_FILE)
                .context("candidate blocks missing")?
            && receipt.target_index
                == *candidate
                    .files
                    .get(BLOCK_INDEX_FILE)
                    .context("candidate index missing")?
            && receipt.source_blocks
                == candidate
                    .source_files
                    .get(BLOCKS_FILE)
                    .context("source blocks missing")?
                    .content
            && receipt.source_index
                == candidate
                    .source_files
                    .get(BLOCK_INDEX_FILE)
                    .context("source index missing")?
                    .content,
        "receipt core bindings differ from candidate maps"
    );
    ensure!(
        candidate
            .source_files
            .get(ARCHIVE_V2_POH_FILE)
            .is_some_and(|source| {
                candidate
                    .files
                    .get(ARCHIVE_V2_POH_FILE)
                    .is_some_and(|target| *target == source.content)
            }),
        "normalization proof does not copy-bind the finalized PoH sidecar"
    );
    ensure!(
        receipt.target_metadata_profile == ArchiveV2MetadataWireProfile::CURRENT_NAME
            && receipt.target_metadata_profile_counts.legacy_only == 0
            && receipt.target_metadata_profile_counts.both_different == 0
            && receipt.target_metadata_profile_counts.invalid == 0
            && receipt.target_metadata_profile_counts.raw_fallback == 0,
        "receipt does not prove one canonical target metadata format"
    );
    ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1
        .admit_counts(receipt.target_metadata_profile_counts)
        .map_err(|error| anyhow!(error))?;
    ensure!(
        receipt.metadata.records
            == receipt
                .target_metadata_profile_counts
                .checked_total()
                .map_err(|error| anyhow!(error))?
            && receipt.blocks != 0
            && receipt.transactions != 0
            && receipt.message_bytes != 0
            && receipt.message_sha256.len() == 64,
        "normalization coverage counts are incomplete"
    );
    ensure!(
        candidate.source_authority_kind == "external-source-authority-inventory"
            && authority_binding == &candidate.source_authority_binding
            && authority.authority_id == candidate.source_authority_id
            && authority.authority_digest == candidate.source_generation_digest
            && authority.cluster_id == candidate.cluster_id
            && authority.epoch == candidate.epoch
            && authority.slots_per_epoch == candidate.slots_per_epoch
            && authority.message_wire_profile == candidate.source_message_wire_profile
            && authority.metadata_wire_profile == candidate.source_metadata_wire_profile,
        "external authority does not match the normalization proof"
    );
    validate_sha256(&candidate.target_candidate_digest)?;
    validate_sha256(&receipt.message_sha256)?;
    let _ = (
        &candidate.omitted_source_controls,
        receipt.source_metadata_profile_counts,
        receipt.copied_sidecars,
        receipt.copied_sidecar_bytes,
        receipt.get_block_rows_rebuilt,
        receipt.target_zstd_level,
        receipt.metadata.current_error_records,
    );
    Ok(())
}

fn validate_source_authority(
    source: &PinnedLocalRangeSource,
    authority: &SourceAuthorityInventory,
    expected: &BTreeMap<String, SourceFileBinding>,
    expected_ignored: &[String],
    expected_directory: &FileIdentity,
) -> Result<()> {
    let actual_directory = file_identity(&source.directory_file()?.metadata()?, false)?;
    ensure!(
        actual_directory == *expected_directory,
        "historical source directory identity changed after normalization"
    );
    let authority_names = authority
        .files
        .iter()
        .map(|file| file.name.as_str())
        .collect::<BTreeSet<_>>();
    ensure!(
        authority_names.len() == expected.len()
            && expected
                .keys()
                .all(|name| authority_names.contains(name.as_str())),
        "source file proof does not equal the authority inventory"
    );
    let mut ignored = Vec::new();
    let mut found = BTreeSet::new();
    for entry in source.inventory()? {
        let name = entry
            .name
            .into_string()
            .map_err(|_| anyhow!("source contains a non-UTF-8 entry"))?;
        if let Some(proof) = expected.get(&name) {
            ensure!(
                entry.kind == PinnedLocalEntryKind::RegularFile,
                "authority-bound source object {name} is not a regular file"
            );
            let authority_file = authority
                .files
                .iter()
                .find(|file| file.name == name)
                .context("authority file disappeared")?;
            ensure!(
                authority_file.bytes == proof.content.bytes
                    && authority_file.sha256 == proof.content.sha256
                    && authority_file.disposition == proof.disposition,
                "authority and normalization source binding differ for {name}"
            );
            let file = source.open_file(&name)?;
            ensure!(
                file_identity(&file.metadata()?, true)? == proof.identity
                    && file.metadata()?.nlink() == 1,
                "source identity changed for {name}"
            );
            ensure!(
                hash_file(&file)? == proof.content,
                "source content changed for {name}"
            );
            found.insert(name);
        } else {
            ensure!(
                !looks_like_archive_or_control(&name),
                "unbound archive/control entry exists in source: {name}"
            );
            ensure!(
                matches!(
                    entry.kind,
                    PinnedLocalEntryKind::RegularFile | PinnedLocalEntryKind::Directory
                ),
                "unbound source entry {name} is a symlink or special object"
            );
            ignored.push(name);
        }
    }
    ignored.sort();
    ensure!(
        found.len() == expected.len(),
        "source authority files are missing"
    );
    ensure!(
        ignored == expected_ignored,
        "ignored source inventory changed"
    );
    source.verify_unchanged().map_err(|error| anyhow!(error))?;
    Ok(())
}

fn reconstruct_spyx_trusted_local_source_proof(
    source: &PinnedLocalRangeSource,
    expected_source_files: &BTreeMap<String, SourceFileBinding>,
    expected_generation_digest: &str,
) -> Result<SpyxTrustedLocalSourceProof> {
    let reader = ArchiveReader::open_trusted_with_additional_files_and_metadata_profile(
        source.clone(),
        TrustedGenerationIdentity {
            cluster_id: SPYX_TRUSTED_LOCAL_CLUSTER_ID.to_owned(),
            epoch: SUPPORTED_EPOCH,
            generation_id: SPYX_TRUSTED_LOCAL_GENERATION_ID.to_owned(),
            slots_per_epoch: SPYX_TRUSTED_LOCAL_SLOTS_PER_EPOCH,
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        },
        &SPYX_TRUSTED_LOCAL_REQUIRED_ADDITIONAL_FILES,
        &SPYX_TRUSTED_LOCAL_OPTIONAL_ADDITIONAL_FILES,
        ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
        ReaderOpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..ReaderOpenOptions::default()
        },
    )
    .map_err(|error| anyhow!(error))
    .context("reconstruct the exact SPYX trusted-local epoch-900 source identity")?;
    let current_binding = reader.binding();
    let trusted_manifest = synthesize_deployed_spyx_trusted_manifest(reader.manifest())?;
    let proof = SpyxTrustedLocalSourceProof {
        binding_recipe: SPYX_DEPLOYED_TRUSTED_BINDING_RECIPE.to_owned(),
        generation_binding_digest: trusted_manifest.generation_digest.clone(),
        registry_binding_sha256: trusted_manifest
            .required_file(REGISTRY_FILE)
            .map_err(|error| anyhow!(error))?
            .sha256
            .clone(),
        trusted_manifest,
        current_reader_generation_digest: hex_lower(&current_binding.generation_digest),
        current_reader_registry_binding_sha256: hex_lower(&current_binding.registry_sha256),
        wire_profile: current_binding.wire_profile,
        metadata_wire_profile: reader.metadata_wire_profile(),
        required_additional_files: SPYX_TRUSTED_LOCAL_REQUIRED_ADDITIONAL_FILES
            .iter()
            .map(|name| (*name).to_owned())
            .collect(),
        optional_additional_files: SPYX_TRUSTED_LOCAL_OPTIONAL_ADDITIONAL_FILES
            .iter()
            .map(|name| (*name).to_owned())
            .collect(),
    };
    validate_spyx_trusted_local_source_proof(
        &proof,
        expected_source_files,
        expected_generation_digest,
    )?;
    source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("historical source changed during SPYX trusted-local reconstruction")?;
    Ok(proof)
}

fn synthesize_deployed_spyx_trusted_manifest(
    current_reader_manifest: &GenerationManifest,
) -> Result<GenerationManifest> {
    ensure!(
        current_reader_manifest.schema_version == GENERATION_MANIFEST_SCHEMA_VERSION
            && current_reader_manifest.cluster_id == SPYX_TRUSTED_LOCAL_CLUSTER_ID
            && current_reader_manifest.epoch == SUPPORTED_EPOCH
            && current_reader_manifest.generation_id == SPYX_TRUSTED_LOCAL_GENERATION_ID
            && current_reader_manifest.slots_per_epoch == SPYX_TRUSTED_LOCAL_SLOTS_PER_EPOCH
            && current_reader_manifest.complete,
        "current trusted reader used the wrong fixed SPYX identity"
    );
    let mut manifest = GenerationManifest {
        schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
        cluster_id: SPYX_TRUSTED_LOCAL_CLUSTER_ID.to_owned(),
        epoch: SUPPORTED_EPOCH,
        generation_id: SPYX_TRUSTED_LOCAL_GENERATION_ID.to_owned(),
        generation_digest: "0".repeat(64),
        slots_per_epoch: SPYX_TRUSTED_LOCAL_SLOTS_PER_EPOCH,
        complete: true,
        files: current_reader_manifest
            .files
            .iter()
            .map(|file| GenerationFile {
                name: file.name.clone(),
                size: file.size,
                sha256: deployed_spyx_trusted_file_size_binding(
                    ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                    &file.name,
                    file.size,
                ),
            })
            .collect(),
    };
    manifest.generation_digest = compute_generation_digest(&manifest)?;
    manifest.validate().map_err(|error| anyhow!(error))?;
    Ok(manifest)
}

fn deployed_spyx_trusted_file_size_binding(
    wire_profile: ArchiveV2WireProfile,
    name: &str,
    size: u64,
) -> String {
    let wire_profile = match wire_profile {
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::POST_UNKNOWN_NAME
        }
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PRE_UNKNOWN_NAME
        }
    };
    let mut hasher = Sha256::new();
    hasher.update(SPYX_DEPLOYED_TRUSTED_FILE_SIZE_BINDING_DOMAIN);
    hasher.update(wire_profile.as_bytes());
    hasher.update([0]);
    hasher.update((name.len() as u64).to_le_bytes());
    hasher.update(name.as_bytes());
    hasher.update(size.to_le_bytes());
    hex_lower(&hasher.finalize())
}

fn validate_spyx_trusted_local_source_proof(
    proof: &SpyxTrustedLocalSourceProof,
    expected_source_files: &BTreeMap<String, SourceFileBinding>,
    expected_generation_digest: &str,
) -> Result<()> {
    validate_sha256(expected_generation_digest)?;
    validate_sha256(&proof.generation_binding_digest)?;
    validate_sha256(&proof.registry_binding_sha256)?;
    validate_sha256(&proof.current_reader_generation_digest)?;
    validate_sha256(&proof.current_reader_registry_binding_sha256)?;
    proof
        .trusted_manifest
        .validate()
        .map_err(|error| anyhow!(error))?;
    ensure!(
        proof.binding_recipe == SPYX_DEPLOYED_TRUSTED_BINDING_RECIPE
            && proof.trusted_manifest.schema_version == GENERATION_MANIFEST_SCHEMA_VERSION
            && proof.trusted_manifest.cluster_id == SPYX_TRUSTED_LOCAL_CLUSTER_ID
            && proof.trusted_manifest.epoch == SUPPORTED_EPOCH
            && proof.trusted_manifest.generation_id == SPYX_TRUSTED_LOCAL_GENERATION_ID
            && proof.trusted_manifest.slots_per_epoch == SPYX_TRUSTED_LOCAL_SLOTS_PER_EPOCH
            && proof.trusted_manifest.complete
            && proof.wire_profile == ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
            && proof.metadata_wire_profile
                == ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
        "SPYX trusted-local source proof has the wrong fixed reader identity"
    );
    ensure!(
        proof.required_additional_files
            == SPYX_TRUSTED_LOCAL_REQUIRED_ADDITIONAL_FILES
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>()
            && proof.optional_additional_files
                == SPYX_TRUSTED_LOCAL_OPTIONAL_ADDITIONAL_FILES
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect::<Vec<_>>(),
        "SPYX trusted-local additional-file contract differs"
    );
    ensure!(
        proof.generation_binding_digest == proof.trusted_manifest.generation_digest
            && proof.generation_binding_digest == expected_generation_digest,
        "reconstructed SPYX trusted-local generation digest differs"
    );
    ensure!(
        proof.registry_binding_sha256
            == proof
                .trusted_manifest
                .required_file(REGISTRY_FILE)
                .map_err(|error| anyhow!(error))?
                .sha256,
        "SPYX trusted-local registry binding differs from its synthetic manifest"
    );
    for file in &proof.trusted_manifest.files {
        ensure!(
            file.sha256
                == deployed_spyx_trusted_file_size_binding(
                    proof.wire_profile,
                    &file.name,
                    file.size,
                ),
            "SPYX deployed trusted-local file binding recipe differs for {}",
            file.name
        );
    }

    let required_names = REQUIRED_GENERATION_FILES
        .into_iter()
        .chain(SPYX_TRUSTED_LOCAL_REQUIRED_ADDITIONAL_FILES)
        .collect::<BTreeSet<_>>();
    let allowed_names = required_names
        .iter()
        .copied()
        .chain(SPYX_TRUSTED_LOCAL_OPTIONAL_ADDITIONAL_FILES)
        .collect::<BTreeSet<_>>();
    let actual_names = proof
        .trusted_manifest
        .files
        .iter()
        .map(|file| file.name.as_str())
        .collect::<BTreeSet<_>>();
    ensure!(
        required_names.is_subset(&actual_names)
            && actual_names.is_subset(&allowed_names)
            && actual_names.len() == proof.trusted_manifest.files.len(),
        "SPYX trusted-local synthetic manifest has the wrong admitted file set"
    );
    for file in &proof.trusted_manifest.files {
        let source_file = expected_source_files.get(&file.name).with_context(|| {
            format!(
                "SPYX trusted-local file {} is not authority-bound",
                file.name
            )
        })?;
        ensure!(
            source_file.content.bytes == file.size,
            "SPYX trusted-local size binding differs from source authority for {}",
            file.name
        );
    }
    Ok(())
}

fn validate_candidate_inventory(
    source: &PinnedLocalRangeSource,
    payload: &BTreeMap<String, FileBinding>,
    message_profile: ArchiveV2WireProfile,
    require_complete: bool,
) -> Result<()> {
    let mut base = payload.keys().cloned().collect::<BTreeSet<_>>();
    base.insert(CANDIDATE_FILE.to_owned());
    base.insert(RECEIPT_FILE.to_owned());
    let message_marker = wire_profile_marker(message_profile).name;
    let controls = BTreeSet::from([
        GENERATION_MANIFEST_FILE.to_owned(),
        ARCHIVE_V2_PUBLICATION_LOCK_FILE.to_owned(),
        CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
        message_marker.clone(),
    ]);
    let mut actual = BTreeSet::new();
    for entry in source.inventory()? {
        let name = entry
            .name
            .into_string()
            .map_err(|_| anyhow!("candidate contains a non-UTF-8 entry"))?;
        ensure!(
            entry.kind == PinnedLocalEntryKind::RegularFile,
            "candidate entry {name} is not a regular file"
        );
        actual.insert(name);
    }
    ensure!(
        base.is_subset(&actual),
        "candidate payload or normalization proof is missing"
    );
    let allowed = base.union(&controls).cloned().collect::<BTreeSet<_>>();
    ensure!(
        actual.is_subset(&allowed),
        "candidate inventory has an unknown entry"
    );
    if actual.contains(GENERATION_MANIFEST_FILE) {
        ensure!(
            actual.contains(&message_marker) && actual.contains(CURRENT_TYPED_ERRORS_MARKER_FILE),
            "candidate manifest exists without both prior marker publications"
        );
    }
    if require_complete {
        ensure!(
            actual == allowed,
            "published candidate inventory is not exact"
        );
    }
    Ok(())
}

fn hash_candidate_payload(
    source: &PinnedLocalRangeSource,
    payload: &BTreeMap<String, FileBinding>,
) -> Result<()> {
    for (name, expected) in payload {
        let file = source.open_file(name)?;
        ensure!(
            file.metadata()?.nlink() == 1 && hash_file(&file)? == *expected,
            "candidate payload hash differs for {name}"
        );
    }
    source.verify_unchanged().map_err(|error| anyhow!(error))?;
    Ok(())
}

fn build_target_candidate_manifest(
    candidate: &NormalizationCandidate,
) -> Result<GenerationManifest> {
    build_manifest(candidate, None, None)
}

fn build_published_manifest(
    candidate: &NormalizationCandidate,
    candidate_binding: &FileBinding,
    receipt_binding: &FileBinding,
) -> Result<GenerationManifest> {
    build_manifest(candidate, Some(candidate_binding), Some(receipt_binding))
}

fn build_manifest(
    candidate: &NormalizationCandidate,
    candidate_binding: Option<&FileBinding>,
    receipt_binding: Option<&FileBinding>,
) -> Result<GenerationManifest> {
    ensure!(
        candidate_binding.is_some() == receipt_binding.is_some(),
        "published provenance bindings must be present together"
    );
    let profile = ArchiveV2WireProfile::from_str(&candidate.source_message_wire_profile)
        .map_err(|error| anyhow!(error))?;
    let message_marker = wire_profile_marker(profile);
    let metadata_marker = GenerationFile {
        name: CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
        size: CURRENT_TYPED_ERRORS_MARKER_SIZE,
        sha256: CURRENT_TYPED_ERRORS_MARKER_SHA256.to_owned(),
    };
    ensure!(
        candidate.authorized_message_marker
            == NamedFileBinding {
                name: message_marker.name.clone(),
                bytes: message_marker.size,
                sha256: message_marker.sha256.clone(),
            }
            && candidate.authorized_metadata_marker
                == NamedFileBinding {
                    name: metadata_marker.name.clone(),
                    bytes: metadata_marker.size,
                    sha256: metadata_marker.sha256.clone(),
                },
        "candidate marker authorization is not canonical"
    );
    let mut files = candidate
        .files
        .iter()
        .map(|(name, binding)| GenerationFile {
            name: name.clone(),
            size: binding.bytes,
            sha256: binding.sha256.clone(),
        })
        .collect::<Vec<_>>();
    if let (Some(candidate_binding), Some(receipt_binding)) = (candidate_binding, receipt_binding) {
        files.push(GenerationFile {
            name: CANDIDATE_FILE.to_owned(),
            size: candidate_binding.bytes,
            sha256: candidate_binding.sha256.clone(),
        });
        files.push(GenerationFile {
            name: RECEIPT_FILE.to_owned(),
            size: receipt_binding.bytes,
            sha256: receipt_binding.sha256.clone(),
        });
    }
    files.push(message_marker);
    files.push(metadata_marker);
    files.sort_by(|left, right| left.name.cmp(&right.name));
    let mut manifest = GenerationManifest {
        schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
        cluster_id: candidate.cluster_id.clone(),
        epoch: candidate.epoch,
        generation_id: candidate.candidate_id.clone(),
        generation_digest: "0".repeat(64),
        slots_per_epoch: candidate.slots_per_epoch,
        complete: true,
        files,
    };
    manifest.generation_digest =
        compute_generation_digest(&manifest).map_err(|error| anyhow!(error))?;
    manifest.validate().map_err(|error| anyhow!(error))?;
    Ok(manifest)
}

fn validate_published_controls(
    source: &PinnedLocalRangeSource,
    expected: &GenerationManifest,
) -> Result<()> {
    let bytes = source.read_all_bounded(GENERATION_MANIFEST_FILE, MAX_JSON_BYTES)?;
    let actual = GenerationManifest::parse(&bytes).map_err(|error| anyhow!(error))?;
    ensure!(
        manifest_file_map(&actual) == manifest_file_map(expected)
            && actual.schema_version == expected.schema_version
            && actual.cluster_id == expected.cluster_id
            && actual.epoch == expected.epoch
            && actual.generation_id == expected.generation_id
            && actual.generation_digest == expected.generation_digest
            && actual.slots_per_epoch == expected.slots_per_epoch
            && actual.complete,
        "published generation manifest differs from the audited target candidate"
    );
    for profile in [
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
    ] {
        let marker = wire_profile_marker(profile);
        if expected.file(&marker.name).is_some() {
            ensure!(
                source.read_all_bounded(&marker.name, marker.size as usize)?
                    == wire_profile_marker_bytes(profile),
                "message marker bytes differ"
            );
        } else {
            ensure!(
                source.size(&marker.name)?.is_none(),
                "conflicting message marker exists"
            );
        }
    }
    ensure!(
        source.read_all_bounded(
            CURRENT_TYPED_ERRORS_MARKER_FILE,
            CURRENT_TYPED_ERRORS_MARKER_SIZE as usize,
        )? == CURRENT_TYPED_ERRORS_MARKER_BYTES,
        "metadata marker bytes differ"
    );
    source.verify_unchanged().map_err(|error| anyhow!(error))?;
    Ok(())
}

fn audit_candidate_semantics(
    source: &PinnedLocalRangeSource,
    manifest: &GenerationManifest,
    candidate: &NormalizationCandidate,
    receipt: &NormalizationReceipt,
) -> Result<ArchiveV2WireProfile> {
    let profile = ArchiveV2WireProfile::from_str(&candidate.source_message_wire_profile)
        .map_err(|error| anyhow!(error))?;
    let message_marker = wire_profile_marker(profile);
    let mut markers = BTreeMap::new();
    markers.insert(
        message_marker.name.clone(),
        wire_profile_marker_bytes(profile),
    );
    markers.insert(
        CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
        CURRENT_TYPED_ERRORS_MARKER_BYTES,
    );
    let reader = ArchiveReader::open_candidate(
        MarkerOverlay {
            source: source.clone(),
            markers: Arc::new(markers),
        },
        manifest.clone(),
        ReaderOpenOptions {
            hash_verification: HashVerification::AllFiles,
            ..ReaderOpenOptions::default()
        },
    )
    .map_err(|error| anyhow!(error))
    .context("open complete target through its in-memory publication controls")?;
    validate_manifest_bound_pinned_local_registry_index(source, manifest)
        .map_err(|error| anyhow!(error))?;
    ensure!(!reader.index().rows.is_empty(), "target hot index is empty");
    let decision = audit_full_generation_wire_profile(&reader, MAX_PROFILE_MESSAGE_BYTES)
        .map_err(|error| anyhow!(error))?
        .require_unproven_authority()
        .map_err(|error| anyhow!(error))?;
    if decision == UnprovenWireProfileDecision::AllSemanticallyEquivalent {
        ensure!(
            profile == ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            "all-equivalent target must publish the canonical Post message profile"
        );
    }
    let metadata_proof =
        audit_current_metadata_for_marker_publication(&reader).map_err(|error| anyhow!(error))?;
    ensure!(
        metadata_proof.source_binding() == reader.profiled_binding()
            && metadata_proof.audit().blocks == receipt.blocks
            && metadata_proof.audit().counts == receipt.target_metadata_profile_counts,
        "fresh target metadata audit differs from the normalization receipt"
    );
    let proven_marker = metadata_proof.marker_manifest_entry();
    ensure!(
        proven_marker.name == CURRENT_TYPED_ERRORS_MARKER_FILE
            && proven_marker.size == CURRENT_TYPED_ERRORS_MARKER_SIZE
            && proven_marker.sha256 == CURRENT_TYPED_ERRORS_MARKER_SHA256
            && metadata_proof.marker_bytes() == CURRENT_TYPED_ERRORS_MARKER_BYTES,
        "fresh target audit authorized unexpected metadata marker bytes"
    );
    recheck_manifest_payload_paths(source, manifest)?;
    source.verify_unchanged().map_err(|error| anyhow!(error))?;
    Ok(profile)
}

fn publish_candidate_controls_descriptor_relative(
    candidate_path: &Path,
    source: &PinnedLocalRangeSource,
    manifest: &GenerationManifest,
    candidate: &NormalizationCandidate,
    receipt: &NormalizationReceipt,
) -> Result<()> {
    let anchor = PathAnchor::open(candidate_path)?;
    ensure!(
        anchor.identity == directory_identity(&source.directory_file()?.metadata()?)?,
        "candidate path and descriptor root differ before publication"
    );
    source.verify_unchanged().map_err(|error| anyhow!(error))?;
    let directory = source.directory_file()?;
    let publication_lock = FileLock::acquire_at(&directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
    let profile = audit_candidate_semantics(source, manifest, candidate, receipt)?;
    let message_marker = wire_profile_marker(profile);
    ensure_anchor_unchanged(&anchor)?;
    publication_lock.recheck_at(&directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;

    publish_immutable_at(
        &directory,
        &message_marker.name,
        wire_profile_marker_bytes(profile),
    )?;
    publication_lock.recheck_at(&directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
    publish_immutable_at(
        &directory,
        CURRENT_TYPED_ERRORS_MARKER_FILE,
        CURRENT_TYPED_ERRORS_MARKER_BYTES,
    )?;
    publication_lock.recheck_at(&directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
    let manifest_bytes = pretty_json_bytes(manifest)?;
    publish_immutable_at(&directory, GENERATION_MANIFEST_FILE, &manifest_bytes)?;
    directory.sync_all()?;
    anchor.parent.sync_all()?;
    publication_lock.recheck_at(&directory, ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
    recheck_manifest_payload_paths(source, manifest)?;
    ensure_anchor_unchanged(&anchor)?;
    Ok(())
}

fn recheck_manifest_payload_paths(
    source: &PinnedLocalRangeSource,
    manifest: &GenerationManifest,
) -> Result<()> {
    let directory = source.directory_file()?;
    for entry in &manifest.files {
        if entry.name == CURRENT_TYPED_ERRORS_MARKER_FILE
            || entry.name
                == wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1).name
            || entry.name
                == wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1).name
        {
            continue;
        }
        let pinned = source.open_file(&entry.name)?;
        let current = openat_regular_nofollow(&directory, OsStr::new(&entry.name), false)?;
        ensure!(
            pinned.metadata()?.dev() == current.metadata()?.dev()
                && pinned.metadata()?.ino() == current.metadata()?.ino()
                && pinned.metadata()?.len() == entry.size,
            "target payload path changed during publication: {}",
            entry.name
        );
    }
    Ok(())
}

fn ensure_anchor_unchanged(anchor: &PathAnchor) -> Result<()> {
    anchor
        .parent_source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))?;
    let current = openat_directory_nofollow(&anchor.parent, &anchor.name)?;
    ensure!(
        directory_identity(&current.metadata()?)? == anchor.identity
            && directory_identity(&anchor.directory.metadata()?)? == anchor.identity,
        "candidate directory anchor changed during publication"
    );
    Ok(())
}

fn publish_immutable_at(directory: &File, name: &str, bytes: &[u8]) -> Result<()> {
    if let Some(mut existing) = try_openat_regular_nofollow(directory, OsStr::new(name))? {
        let mut actual = Vec::new();
        existing.read_to_end(&mut actual)?;
        ensure!(
            actual == bytes,
            "existing immutable control differs: {name}"
        );
        directory.sync_all()?;
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    let mut file = {
        let dot = CString::new(".")?;
        // SAFETY: the directory and fixed component remain live. O_TMPFILE
        // creates an unnamed inode, so a crash cannot leave a partial name.
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                dot.as_ptr(),
                libc::O_WRONLY | libc::O_TMPFILE | libc::O_CLOEXEC,
                0o400 as libc::c_uint,
            )
        };
        ensure!(
            descriptor >= 0,
            "create unnamed immutable-control staging inode: {}",
            io::Error::last_os_error()
        );
        // SAFETY: openat returned one new owned descriptor.
        unsafe { File::from_raw_fd(descriptor) }
    };

    #[cfg(target_os = "macos")]
    let (mut file, temporary) = {
        let temporary = format!(
            ".{name}.tmp.{}.{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        (
            createat_regular_nofollow(directory, OsStr::new(&temporary), 0o400)?,
            temporary,
        )
    };

    file.write_all(bytes)?;
    file.sync_all()?;
    let name_c = cstring_component(OsStr::new(name))?;

    #[cfg(target_os = "linux")]
    let linked = {
        // SAFETY: the unnamed inode descriptor and final directory remain live.
        let direct = unsafe {
            libc::linkat(
                file.as_raw_fd(),
                c"".as_ptr(),
                directory.as_raw_fd(),
                name_c.as_ptr(),
                libc::AT_EMPTY_PATH,
            )
        };
        if direct == 0 {
            0
        } else {
            let direct_error = io::Error::last_os_error();
            if direct_error.kind() == io::ErrorKind::AlreadyExists {
                direct
            } else {
                // AT_EMPTY_PATH usually needs CAP_DAC_READ_SEARCH. Prepare is
                // intentionally usable by the unprivileged archive owner, so
                // use the linkat(2) documented O_TMPFILE fallback. The kernel
                // procfs link identifies this still-open unnamed inode; the
                // final destination remains descriptor-relative and no-replace.
                let proc_path = CString::new(format!("/proc/self/fd/{}", file.as_raw_fd()))?;
                // SAFETY: proc_path identifies the live file descriptor, and
                // directory/name_c remain live for this call.
                unsafe {
                    libc::linkat(
                        libc::AT_FDCWD,
                        proc_path.as_ptr(),
                        directory.as_raw_fd(),
                        name_c.as_ptr(),
                        libc::AT_SYMLINK_FOLLOW,
                    )
                }
            }
        }
    };

    #[cfg(target_os = "macos")]
    let linked = {
        let temporary_c = cstring_component(OsStr::new(&temporary))?;
        // SAFETY: both names and the directory descriptor remain live.
        unsafe {
            libc::linkat(
                directory.as_raw_fd(),
                temporary_c.as_ptr(),
                directory.as_raw_fd(),
                name_c.as_ptr(),
                0,
            )
        }
    };
    if linked != 0 {
        let error = io::Error::last_os_error();
        ensure!(
            error.kind() == io::ErrorKind::AlreadyExists,
            "publish immutable control {name}: {error}"
        );
        let mut existing = openat_regular_nofollow(directory, OsStr::new(name), false)?;
        let mut actual = Vec::new();
        existing.read_to_end(&mut actual)?;
        ensure!(
            actual == bytes,
            "publication race wrote different bytes: {name}"
        );
    }
    directory.sync_all()?;

    #[cfg(target_os = "macos")]
    {
        let temporary_c = cstring_component(OsStr::new(&temporary))?;
        // SAFETY: this removes only the private temporary name created above.
        let unlinked = unsafe { libc::unlinkat(directory.as_raw_fd(), temporary_c.as_ptr(), 0) };
        ensure!(
            unlinked == 0,
            "remove private control temporary: {}",
            io::Error::last_os_error()
        );
        directory.sync_all()?;
    }
    Ok(())
}

fn manifest_file_map(manifest: &GenerationManifest) -> BTreeMap<String, FileBinding> {
    manifest
        .files
        .iter()
        .map(|file| {
            (
                file.name.clone(),
                FileBinding {
                    bytes: file.size,
                    sha256: file.sha256.clone(),
                },
            )
        })
        .collect()
}

fn freeze_candidate(source: &PinnedLocalRangeSource) -> Result<()> {
    for entry in source.inventory()? {
        let name = entry
            .name
            .into_string()
            .map_err(|_| anyhow!("candidate contains a non-UTF-8 entry"))?;
        ensure!(entry.kind == PinnedLocalEntryKind::RegularFile);
        if name == ARCHIVE_V2_PUBLICATION_LOCK_FILE {
            continue;
        }
        let file = source.open_file(&name)?;
        // SAFETY: the descriptor is live and the mode is a fixed read-only mode.
        ensure!(
            unsafe { libc::fchmod(file.as_raw_fd(), 0o444) } == 0,
            "make candidate object read-only {name}: {}",
            io::Error::last_os_error()
        );
        file.sync_all()?;
    }
    let directory = source.directory_file()?;
    // SAFETY: the descriptor is live and the mode is a fixed read-only directory mode.
    ensure!(
        unsafe { libc::fchmod(directory.as_raw_fd(), 0o555) } == 0,
        "freeze candidate directory: {}",
        io::Error::last_os_error()
    );
    directory.sync_all()?;
    sync_parent_of(source.root())?;
    Ok(())
}

fn collect_frozen_target_proofs(
    source: &PinnedLocalRangeSource,
    payload: &BTreeMap<String, FileBinding>,
    candidate_binding: &FileBinding,
    receipt_binding: &FileBinding,
    manifest: &GenerationManifest,
) -> Result<(
    BTreeMap<String, FrozenFileProof>,
    FileIdentity,
    FileIdentity,
)> {
    let mut bindings = manifest_file_map(manifest);
    bindings.insert(CANDIDATE_FILE.to_owned(), candidate_binding.clone());
    bindings.insert(RECEIPT_FILE.to_owned(), receipt_binding.clone());
    bindings.insert(
        GENERATION_MANIFEST_FILE.to_owned(),
        binding_for_bytes(&source.read_all_bounded(GENERATION_MANIFEST_FILE, MAX_JSON_BYTES)?),
    );
    let lock = source.open_file(ARCHIVE_V2_PUBLICATION_LOCK_FILE)?;
    let publication_lock_identity = file_identity(&lock.metadata()?, true)?;
    ensure!(
        publication_lock_identity.bytes == 0
            && publication_lock_identity.mode & 0o777 == 0o600
            && lock.metadata()?.nlink() == 1,
        "publication lock is not one private unlisted control"
    );
    ensure!(
        payload.keys().all(|name| bindings.contains_key(name)),
        "frozen target proof misses a payload"
    );
    let mut proofs = BTreeMap::new();
    for (name, content) in bindings {
        let file = source.open_file(&name)?;
        let identity = file_identity(&file.metadata()?, true)?;
        ensure!(
            identity.mode & 0o777 == 0o444 && file.metadata()?.nlink() == 1,
            "target object {name} is not frozen"
        );
        ensure!(
            hash_file(&file)? == content,
            "frozen target content differs for {name}"
        );
        proofs.insert(name, FrozenFileProof { identity, content });
    }
    let directory = file_identity(&source.directory_file()?.metadata()?, false)?;
    ensure!(
        directory.mode & 0o777 == 0o555,
        "target directory is not frozen"
    );
    Ok((proofs, publication_lock_identity, directory))
}

fn validate_prepare_receipt(receipt: &PrepareReceipt) -> Result<()> {
    ensure!(
        receipt.schema_version == 1
            && receipt.kind == "archive-v2-metadata-normalization-publication-prepare"
            && receipt.state == "audited-frozen-unselected-generation"
            && receipt.target_frozen
            && receipt.full_descriptor_publication_audit_completed
            && receipt.prepared_unix_seconds != 0,
        "invalid publication prepare receipt"
    );
    ensure!(
        receipt.epoch == SUPPORTED_EPOCH,
        "unsupported prepared epoch"
    );
    validate_sha256(&receipt.target_candidate_digest)?;
    validate_sha256(&receipt.generation_digest)?;
    validate_sha256(&receipt.spyx_epoch_900_source_generation_digest)?;
    validate_sha256(&receipt.source_authority.sha256)?;
    validate_sha256(&receipt.normalization_candidate.sha256)?;
    validate_sha256(&receipt.normalization_receipt.sha256)?;
    validate_sha256(&receipt.generation_manifest.sha256)?;
    ensure!(
        receipt.spyx_epoch_900_source_generation_digest == OBSERVED_SPYX_EPOCH_900_SOURCE_DIGEST,
        "prepare receipt has the wrong pinned SPYX epoch-900 source digest"
    );
    ensure!(
        receipt.target_frozen_directory_identity.device == receipt.target_directory.device
            && receipt.target_frozen_directory_identity.inode == receipt.target_directory.inode
            && receipt.target_frozen_directory_identity.mode & 0o777 == 0o555,
        "prepare receipt does not bind one frozen target directory"
    );
    ensure_absolute_normalized(Path::new(&receipt.active_source))?;
    ensure_absolute_normalized(Path::new(&receipt.original_candidate))?;
    ensure_absolute_normalized(Path::new(&receipt.candidate))?;
    ensure_absolute_normalized(Path::new(&receipt.source_authority_path))?;
    validate_canonical_active_source(Path::new(&receipt.active_source))?;
    validate_spyx_trusted_local_source_proof(
        &receipt.spyx_trusted_local_source,
        &receipt.source_files,
        &receipt.spyx_epoch_900_source_generation_digest,
    )?;
    ensure!(
        receipt.spyx_trusted_local_source.generation_binding_digest
            == receipt.spyx_epoch_900_source_generation_digest,
        "prepare receipt separates its reconstructed and checkpoint SPYX source bindings"
    );
    ensure!(
        Path::new(&receipt.active_source).parent() == Path::new(&receipt.candidate).parent(),
        "prepared selector is not a direct sibling of the active epoch"
    );
    ensure!(
        Path::new(&receipt.candidate)
            .file_name()
            .and_then(OsStr::to_str)
            == Some(
                expected_selector_name(receipt.epoch, &receipt.normalization_candidate.sha256)
                    .as_str()
            ),
        "prepared selector name is not deterministic"
    );
    let expected_message_marker =
        wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
    ensure!(
        receipt.cluster_id == "mainnet-beta"
            && receipt.ignored_unrelated_source_entries.is_empty()
            && !receipt.source_files.is_empty()
            && !receipt.target_files.is_empty()
            && receipt.source_directory.device == receipt.target_directory.device
            && receipt.source_directory != receipt.target_directory
            && receipt.target_candidate_digest != receipt.generation_digest
            && receipt.message_marker
                == NamedFileBinding {
                    name: expected_message_marker.name.clone(),
                    bytes: expected_message_marker.size,
                    sha256: expected_message_marker.sha256.clone(),
                }
            && receipt.metadata_marker
                == NamedFileBinding {
                    name: CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
                    bytes: CURRENT_TYPED_ERRORS_MARKER_SIZE,
                    sha256: CURRENT_TYPED_ERRORS_MARKER_SHA256.to_owned(),
                }
            && receipt.publication_lock_identity.device == receipt.target_directory.device
            && receipt.publication_lock_identity.bytes == 0
            && receipt.publication_lock_identity.mode & 0o777 == 0o600,
        "prepare receipt has inconsistent fixed publication fields"
    );
    for proof in receipt.source_files.values() {
        validate_sha256(&proof.content.sha256)?;
        ensure!(
            proof.identity.device == receipt.source_directory.device
                && proof.identity.bytes == proof.content.bytes,
            "prepare source-file proof is internally inconsistent"
        );
    }
    for proof in receipt.target_files.values() {
        validate_sha256(&proof.content.sha256)?;
        ensure!(
            proof.identity.device == receipt.target_directory.device
                && proof.identity.bytes == proof.content.bytes
                && proof.identity.mode & 0o777 == 0o444,
            "prepare target-file proof is internally inconsistent"
        );
    }
    for (name, binding) in [
        (CANDIDATE_FILE, &receipt.normalization_candidate),
        (RECEIPT_FILE, &receipt.normalization_receipt),
        (GENERATION_MANIFEST_FILE, &receipt.generation_manifest),
    ] {
        ensure!(
            receipt
                .target_files
                .get(name)
                .is_some_and(|proof| &proof.content == binding),
            "prepare receipt misses the exact {name} binding"
        );
    }
    for marker in [&receipt.message_marker, &receipt.metadata_marker] {
        ensure!(
            receipt.target_files.get(&marker.name).is_some_and(|proof| {
                proof.content.bytes == marker.bytes && proof.content.sha256 == marker.sha256
            }),
            "prepare receipt misses the exact marker binding"
        );
    }
    ensure!(
        receipt
            .source_files
            .get(ARCHIVE_V2_POH_FILE)
            .is_some_and(|source| {
                receipt
                    .target_files
                    .get(ARCHIVE_V2_POH_FILE)
                    .is_some_and(|target| target.content == source.content)
            }),
        "prepare receipt does not bind the finalized PoH sidecar in both generations"
    );
    ensure!(
        !receipt
            .target_files
            .contains_key(ARCHIVE_V2_PUBLICATION_LOCK_FILE),
        "publication lock must remain outside the official target inventory"
    );
    Ok(())
}

fn expected_selector_name(epoch: u64, candidate_sha256: &str) -> String {
    format!(".epoch-{epoch}.metadata-normalized-{candidate_sha256}")
}

fn validate_canonical_active_source(source: &Path) -> Result<()> {
    ensure!(
        source == Path::new(CANONICAL_EPOCH_900_PATH),
        "active source must be the canonical epoch-900 directory {CANONICAL_EPOCH_900_PATH}"
    );
    Ok(())
}

fn validate_prepare_paths(args: &PrepareArgs) -> Result<()> {
    for path in [
        &args.source,
        &args.candidate,
        &args.selector,
        &args.source_authority,
        &args.journal,
    ] {
        ensure_absolute_normalized(path)?;
    }
    ensure!(
        args.epoch == SUPPORTED_EPOCH,
        "this one-shot publisher supports only epoch 900"
    );
    validate_canonical_active_source(&args.source)?;
    ensure!(
        args.source.parent() == args.selector.parent(),
        "selector must be a direct sibling of the active epoch"
    );
    ensure!(
        args.selector.file_name().and_then(OsStr::to_str)
            == Some(expected_selector_name(args.epoch, &args.candidate_sha256).as_str()),
        "selector must use the deterministic hidden candidate name"
    );
    ensure!(
        args.source != args.candidate
            && args.source != args.selector
            && args.candidate != args.selector,
        "source, staging candidate, and selector paths must differ"
    );
    let archive_root = args
        .source
        .parent()
        .context("active source has no archive root")?;
    ensure!(
        [archive_root, &args.source, &args.candidate, &args.selector]
            .iter()
            .all(|path| !paths_overlap(&args.journal, path)),
        "journal must be outside the archive root and both generation trees"
    );
    ensure!(
        !paths_overlap(&args.journal, &args.source_authority),
        "journal must not contain or replace the source-authority path"
    );
    validate_journal_mount_scope(args, archive_root)?;
    Ok(())
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn validate_journal_mount_scope(args: &PrepareArgs, archive_root: &Path) -> Result<()> {
    let archive =
        PinnedLocalRangeSource::open_directory(archive_root).map_err(|error| anyhow!(error))?;
    let archive_directory = archive.directory_file()?;
    let journal_parent_path = args.journal.parent().context("journal has no parent")?;
    let journal_parent = PinnedLocalRangeSource::open_directory(journal_parent_path)
        .map_err(|error| anyhow!(error))?;
    let journal_parent_directory = journal_parent.directory_file()?;
    let archive_mount = descriptor_mount_id(&archive_directory)?;
    ensure!(
        descriptor_mount_id(&journal_parent_directory)? == archive_mount,
        "journal parent must be a normal sibling on the archive mount, not a bind-mount alias"
    );
    if let Some(name) = args.journal.file_name()
        && let Some(existing) = try_openat_directory_nofollow(&journal_parent_directory, name)?
    {
        ensure!(
            descriptor_mount_id(&existing)? == archive_mount,
            "existing journal must be on the archive mount and not a bind-mount alias"
        );
    }
    archive.verify_unchanged().map_err(|error| anyhow!(error))?;
    journal_parent
        .verify_unchanged()
        .map_err(|error| anyhow!(error))?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn descriptor_mount_id(file: &File) -> Result<u64> {
    let fdinfo = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))
        .context("read descriptor mount identity from procfs")?;
    let value = fdinfo
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:"))
        .context("descriptor fdinfo has no mount ID")?
        .trim()
        .parse::<u64>()
        .context("parse descriptor mount ID")?;
    Ok(value)
}

#[cfg(target_os = "macos")]
fn descriptor_mount_id(file: &File) -> Result<u64> {
    // macOS has no Linux bind mounts or statx mount IDs. The device identity is
    // the strictest descriptor-level equivalent used only by local tests.
    Ok(file.metadata()?.dev())
}

fn validate_relocation_intent(intent: &RelocationIntent, args: &PrepareArgs) -> Result<()> {
    ensure!(
        intent.schema_version == 1
            && intent.kind == "archive-v2-metadata-normalization-relocation-intent"
            && intent.state == "ready-for-no-replace-relocation"
            && intent.epoch == args.epoch
            && intent.original_candidate == args.candidate.display().to_string()
            && intent.selector == args.selector.display().to_string()
            && intent.normalization_candidate.sha256 == args.candidate_sha256
            && intent.normalization_receipt.sha256 == args.receipt_sha256,
        "invalid candidate relocation intent"
    );
    validate_sha256(&intent.normalization_candidate.sha256)?;
    validate_sha256(&intent.normalization_receipt.sha256)?;
    Ok(())
}

fn locate_relocation_input(
    args: &PrepareArgs,
    intent: Option<&RelocationIntent>,
) -> Result<PathBuf> {
    let original = PathAnchor::open_optional(&args.candidate)?;
    let selector = PathAnchor::open_optional(&args.selector)?;
    match intent {
        None => {
            ensure!(
                selector.is_none(),
                "selector exists without a durable relocation intent"
            );
            ensure!(
                original.is_some(),
                "normalization staging candidate is absent"
            );
            Ok(args.candidate.clone())
        }
        Some(intent) => {
            validate_relocation_intent(intent, args)?;
            match (original, selector) {
                (Some(original), None) if original.identity == intent.target_directory => {
                    Ok(args.candidate.clone())
                }
                (None, Some(selector)) if selector.identity == intent.target_directory => {
                    Ok(args.selector.clone())
                }
                _ => bail!("candidate relocation topology is ambiguous"),
            }
        }
    }
}

fn relocate_candidate_to_selector(
    active_source: &Path,
    intent: &RelocationIntent,
    journal: &Journal,
) -> Result<()> {
    let root_guard = ArchiveRootGuard::acquire(active_source)?;
    journal.require_same_mount_as(&root_guard.directory)?;
    let original_path = Path::new(&intent.original_candidate);
    let selector_path = Path::new(&intent.selector);
    let original_parent_source = PinnedLocalRangeSource::open_directory(
        original_path
            .parent()
            .context("staging candidate has no parent")?,
    )
    .map_err(|error| anyhow!(error))?;
    let original_parent = original_parent_source.directory_file()?;
    let selector_parent_source = PinnedLocalRangeSource::open_directory(
        selector_path.parent().context("selector has no parent")?,
    )
    .map_err(|error| anyhow!(error))?;
    let selector_parent = selector_parent_source.directory_file()?;
    ensure!(
        directory_identity(&selector_parent.metadata()?)? == root_guard.identity
            && descriptor_mount_id(&selector_parent)? == root_guard.mount_id,
        "selector parent differs from the locked archive root"
    );
    let original = PathAnchor::open_optional(original_path)?;
    let selector = PathAnchor::open_optional(selector_path)?;
    match (original, selector) {
        (Some(original), None) => {
            ensure!(
                original.identity == intent.target_directory,
                "staging candidate identity differs from the relocation intent"
            );
            ensure!(
                directory_identity(&original.parent.metadata()?)?.device
                    == directory_identity(&selector_parent.metadata()?)?.device
                    && descriptor_mount_id(&original.parent)? == root_guard.mount_id
                    && descriptor_mount_id(&original.directory)? == root_guard.mount_id,
                "candidate relocation requires one filesystem"
            );
            ensure_anchor_unchanged(&original)?;
            root_guard.recheck()?;
            rename_anchor_no_replace(
                &original,
                &selector_parent,
                selector_path
                    .file_name()
                    .context("selector has no basename")?,
            )?;
        }
        (None, Some(selector)) => ensure!(
            selector.identity == intent.target_directory,
            "relocated selector identity differs from the relocation intent"
        ),
        _ => bail!("candidate relocation topology is ambiguous"),
    }
    // A retry can observe the post-rename topology after a crash before either
    // parent fsync. Make both the removal and insertion durable on every path.
    original_parent.sync_all()?;
    selector_parent.sync_all()?;
    original_parent_source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))?;
    selector_parent_source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))?;
    root_guard.recheck()?;
    let selected = PathAnchor::open(selector_path)?;
    ensure!(
        selected.identity == intent.target_directory,
        "candidate relocation post-check failed"
    );
    ensure_anchor_unchanged(&selected)?;
    Ok(())
}

fn ensure_source_frozen(
    journal: &Journal,
    prepare: &PrepareReceipt,
    switch_intent_binding: &FileBinding,
    before_mutation: impl FnOnce() -> Result<(String, FileBinding)>,
) -> Result<(SourceFreezeComplete, FileBinding)> {
    if let Some(existing) =
        journal.read_optional_json::<SourceFreezeComplete>(SOURCE_FREEZE_FILE)?
    {
        let (_, quiescence_attempt_binding) = read_and_validate_quiescence_attempt(
            journal,
            &existing.quiescence_attempt_file,
            "freeze-source",
            prepare.epoch,
            switch_intent_binding,
        )?;
        validate_source_freeze(
            &existing,
            prepare,
            switch_intent_binding,
            &quiescence_attempt_binding,
        )?;
        let source_directory = directory_with_identity(prepare, prepare.source_directory)?;
        verify_frozen_source_identities(&source_directory, &existing)?;
        let binding = binding_for_bytes(&pretty_json_bytes(&existing)?);
        return Ok((existing, binding));
    }
    ensure!(
        prepare.ignored_unrelated_source_entries.is_empty(),
        "source freeze does not permit ignored inventory entries"
    );
    let active = PathAnchor::open(Path::new(&prepare.active_source))?;
    let selector = PathAnchor::open(Path::new(&prepare.candidate))?;
    ensure!(
        active.identity == prepare.source_directory
            && selector.identity == prepare.target_directory,
        "source freeze requires the exact pre-cutover topology"
    );
    let inventory = PinnedLocalRangeSource::from_directory_file(
        PathBuf::from("/descriptor-only-source-freeze"),
        active.directory.try_clone()?,
    )?;
    let entries = inventory.inventory()?;
    let actual_names = entries
        .iter()
        .map(|entry| {
            ensure!(
                entry.kind == PinnedLocalEntryKind::RegularFile,
                "source freeze found a non-regular entry"
            );
            entry
                .name
                .clone()
                .into_string()
                .map_err(|_| anyhow!("source freeze found a non-UTF-8 entry"))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    ensure!(
        actual_names == prepare.source_files.keys().cloned().collect(),
        "source inventory changed before freeze"
    );

    let mut validated_files = Vec::with_capacity(prepare.source_files.len());
    for (name, proof) in &prepare.source_files {
        let file = openat_regular_nofollow(&active.directory, OsStr::new(name), false)?;
        validate_source_file_for_freeze(&file, name, proof)?;
        validated_files.push((name.clone(), file));
    }

    // All inventory, content, SPYX, and process checks are complete. Validate
    // the short-lived receipt only now, immediately before the first mode
    // mutation. The retained descriptors prevent a name swap from selecting a
    // different inode after this point.
    let (quiescence_attempt_file, quiescence_attempt_binding) = before_mutation()?;
    for (name, file) in &validated_files {
        let current = openat_regular_nofollow(&active.directory, OsStr::new(name), false)?;
        let current_metadata = current.metadata()?;
        let retained_metadata = file.metadata()?;
        ensure!(
            current_metadata.dev() == retained_metadata.dev()
                && current_metadata.ino() == retained_metadata.ino()
                && current_metadata.nlink() == 1,
            "historical source object path changed before freeze: {name}"
        );
        // SAFETY: the descriptor is the authority-bound source object and the
        // fixed mode removes write permission without changing payload bytes.
        ensure!(
            unsafe { libc::fchmod(file.as_raw_fd(), 0o444) } == 0,
            "freeze historical source object {name}: {}",
            io::Error::last_os_error()
        );
        file.sync_all()?;
    }
    // SAFETY: the descriptor is the exact historical source directory.
    ensure!(
        unsafe { libc::fchmod(active.directory.as_raw_fd(), 0o555) } == 0,
        "freeze historical source directory: {}",
        io::Error::last_os_error()
    );
    active.directory.sync_all()?;
    active.parent.sync_all()?;

    let mut files = BTreeMap::new();
    for (name, proof) in &prepare.source_files {
        let file = openat_regular_nofollow(&active.directory, OsStr::new(name), false)?;
        let identity = file_identity(&file.metadata()?, true)?;
        ensure!(
            identity.mode & 0o777 == 0o444,
            "historical source object {name} is not frozen"
        );
        files.insert(
            name.clone(),
            FrozenFileProof {
                identity,
                content: proof.content.clone(),
            },
        );
    }
    let complete = SourceFreezeComplete {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-source-freeze".to_owned(),
        state: "legacy-source-read-only-and-content-bound".to_owned(),
        completed_unix_seconds: unix_seconds()?,
        epoch: prepare.epoch,
        switch_intent: switch_intent_binding.clone(),
        quiescence_attempt_file,
        quiescence_attempt: quiescence_attempt_binding.clone(),
        source_path: prepare.active_source.clone(),
        source_directory: prepare.source_directory,
        frozen_directory_identity: file_identity(&active.directory.metadata()?, false)?,
        files,
    };
    validate_source_freeze(
        &complete,
        prepare,
        switch_intent_binding,
        &quiescence_attempt_binding,
    )?;
    let binding = journal.write_json_no_replace(SOURCE_FREEZE_FILE, &complete)?;
    verify_frozen_source_identities(&active.directory, &complete)?;
    Ok((complete, binding))
}

fn validate_source_file_for_freeze(
    file: &File,
    name: &str,
    expected: &SourceFileBinding,
) -> Result<()> {
    let metadata = file.metadata()?;
    let current = file_identity(&metadata, true)?;
    ensure!(
        metadata.nlink() == 1,
        "historical source object {name} has multiple links"
    );
    if current == expected.identity {
        return Ok(());
    }
    ensure!(
        current.bytes == expected.identity.bytes
            && current.device == expected.identity.device
            && current.inode == expected.identity.inode
            && current.modified_seconds == expected.identity.modified_seconds
            && current.modified_nanoseconds == expected.identity.modified_nanoseconds
            && current.mode & 0o777 == 0o444
            && metadata.nlink() == 1,
        "historical source object {name} changed outside a recoverable partial freeze"
    );
    ensure!(
        hash_file(file)? == expected.content,
        "historical source content changed during partial freeze recovery: {name}"
    );
    Ok(())
}

fn validate_source_freeze(
    freeze: &SourceFreezeComplete,
    prepare: &PrepareReceipt,
    switch_intent_binding: &FileBinding,
    quiescence_attempt_binding: &FileBinding,
) -> Result<()> {
    ensure!(
        freeze.schema_version == 1
            && freeze.kind == "archive-v2-metadata-normalization-source-freeze"
            && freeze.state == "legacy-source-read-only-and-content-bound"
            && freeze.completed_unix_seconds != 0
            && freeze.epoch == prepare.epoch
            && freeze.switch_intent == *switch_intent_binding
            && freeze.quiescence_attempt == *quiescence_attempt_binding
            && !freeze.quiescence_attempt_file.is_empty()
            && freeze.source_path == prepare.active_source
            && freeze.source_directory == prepare.source_directory
            && freeze.frozen_directory_identity.device == prepare.source_directory.device
            && freeze.frozen_directory_identity.inode == prepare.source_directory.inode
            && freeze.frozen_directory_identity.mode & 0o777 == 0o555
            && freeze.files.len() == prepare.source_files.len(),
        "invalid or mismatched legacy-source freeze receipt"
    );
    for (name, source) in &prepare.source_files {
        let frozen = freeze
            .files
            .get(name)
            .with_context(|| format!("source freeze misses {name}"))?;
        ensure!(
            frozen.content == source.content
                && frozen.identity.device == source.identity.device
                && frozen.identity.inode == source.identity.inode
                && frozen.identity.bytes == source.identity.bytes
                && frozen.identity.modified_seconds == source.identity.modified_seconds
                && frozen.identity.modified_nanoseconds == source.identity.modified_nanoseconds
                && frozen.identity.mode & 0o777 == 0o444,
            "source freeze proof differs for {name}"
        );
    }
    Ok(())
}

fn read_and_validate_source_freeze(
    journal: &Journal,
    prepare: &PrepareReceipt,
    switch_intent_binding: &FileBinding,
) -> Result<(SourceFreezeComplete, FileBinding)> {
    let (freeze, freeze_binding) =
        journal.read_required_bound::<SourceFreezeComplete>(SOURCE_FREEZE_FILE)?;
    let (_, attempt_binding) = read_and_validate_quiescence_attempt(
        journal,
        &freeze.quiescence_attempt_file,
        "freeze-source",
        prepare.epoch,
        switch_intent_binding,
    )?;
    validate_source_freeze(&freeze, prepare, switch_intent_binding, &attempt_binding)?;
    Ok((freeze, freeze_binding))
}

fn directory_with_identity(prepare: &PrepareReceipt, identity: DirectoryIdentity) -> Result<File> {
    for path in [&prepare.active_source, &prepare.candidate] {
        let anchor = PathAnchor::open(Path::new(path))?;
        if anchor.identity == identity {
            return Ok(anchor.directory);
        }
    }
    bail!("neither selected path has the expected directory identity")
}

fn verify_frozen_source_identities(directory: &File, freeze: &SourceFreezeComplete) -> Result<()> {
    let source = PinnedLocalRangeSource::from_directory_file(
        PathBuf::from("/descriptor-only-frozen-source"),
        directory.try_clone()?,
    )?;
    let actual = source
        .inventory()?
        .into_iter()
        .map(|entry| {
            ensure!(
                entry.kind == PinnedLocalEntryKind::RegularFile,
                "frozen historical source contains a non-regular entry"
            );
            entry
                .name
                .into_string()
                .map_err(|_| anyhow!("frozen historical source has a non-UTF-8 entry"))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    ensure!(
        actual == freeze.files.keys().cloned().collect(),
        "frozen historical source inventory changed"
    );
    let directory_metadata = directory.metadata()?;
    ensure!(
        directory_identity(&directory_metadata)? == freeze.source_directory
            && directory_metadata.mode() & 0o777 == 0o555,
        "frozen historical source directory identity or mode changed"
    );
    for (name, proof) in &freeze.files {
        let file = openat_regular_nofollow(directory, OsStr::new(name), false)?;
        ensure!(
            file_identity(&file.metadata()?, true)? == proof.identity
                && file.metadata()?.nlink() == 1,
            "frozen historical source identity changed for {name}"
        );
    }
    Ok(())
}

fn verify_prepared_topology(
    receipt: &PrepareReceipt,
    require_pre_switch: bool,
    source_freeze: Option<&SourceFreezeComplete>,
) -> Result<()> {
    let active = PathAnchor::open(Path::new(&receipt.active_source))?;
    let candidate = PathAnchor::open(Path::new(&receipt.candidate))?;
    let pre = active.identity == receipt.source_directory
        && candidate.identity == receipt.target_directory;
    let post = active.identity == receipt.target_directory
        && candidate.identity == receipt.source_directory;
    ensure!(
        pre || post,
        "paths match neither the prepared nor selected topology"
    );
    if require_pre_switch {
        ensure!(
            pre,
            "candidate is no longer in the prepared pre-switch topology"
        );
    }
    if pre {
        verify_source_state(&active.directory, receipt, source_freeze)?;
        verify_target_identities(
            &candidate.directory,
            &receipt.target_files,
            &receipt.publication_lock_identity,
            &receipt.target_frozen_directory_identity,
        )?;
    } else {
        verify_target_identities(
            &active.directory,
            &receipt.target_files,
            &receipt.publication_lock_identity,
            &receipt.target_frozen_directory_identity,
        )?;
        verify_source_state(&candidate.directory, receipt, source_freeze)?;
    }
    Ok(())
}

fn current_generation_topology(
    receipt: &PrepareReceipt,
) -> Result<(bool, DirectoryIdentity, DirectoryIdentity)> {
    let active = PathAnchor::open(Path::new(&receipt.active_source))?;
    let retained = PathAnchor::open(Path::new(&receipt.candidate))?;
    let pre = active.identity == receipt.source_directory
        && retained.identity == receipt.target_directory;
    let post = active.identity == receipt.target_directory
        && retained.identity == receipt.source_directory;
    ensure!(pre || post, "selected generation topology is ambiguous");
    Ok((post, active.identity, retained.identity))
}

fn quiescence_attempt_file_name(
    operation: &str,
    receipt: &FileBinding,
    active_directory: DirectoryIdentity,
) -> Result<String> {
    ensure!(matches!(
        operation,
        "freeze-source" | "cutover" | "rollback"
    ));
    validate_sha256(&receipt.sha256)?;
    Ok(format!(
        "archive-v2-metadata-normalization.{operation}-quiescence-attempt.{}.{}.v1.json",
        receipt.sha256, active_directory.inode
    ))
}

#[allow(clippy::too_many_arguments)] // One security record cross-binds each independent input.
fn publish_quiescence_attempt(
    journal: &Journal,
    operation: &str,
    epoch: u64,
    parent_intent: &FileBinding,
    receipt_path: &Path,
    receipt: &FileBinding,
    active_directory: DirectoryIdentity,
    retained_directory: DirectoryIdentity,
) -> Result<(String, FileBinding)> {
    let name = quiescence_attempt_file_name(operation, receipt, active_directory)?;
    let attempt = QuiescenceAttempt {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-quiescence-attempt".to_owned(),
        state: "fresh-quiescence-verified-under-switch-lock".to_owned(),
        operation: operation.to_owned(),
        epoch,
        parent_intent: parent_intent.clone(),
        reader_quiescence_path: receipt_path.display().to_string(),
        reader_quiescence: receipt.clone(),
        current_host_boot_id: current_host_boot_id()?,
        active_directory,
        retained_directory,
    };
    let binding = journal.write_or_validate_json(&name, &attempt)?;
    Ok((name, binding))
}

fn read_and_validate_quiescence_attempt(
    journal: &Journal,
    name: &str,
    operation: &str,
    epoch: u64,
    parent_intent: &FileBinding,
) -> Result<(QuiescenceAttempt, FileBinding)> {
    let (attempt, binding) = journal.read_required_bound::<QuiescenceAttempt>(name)?;
    ensure!(
        attempt.schema_version == 1
            && attempt.kind == "archive-v2-metadata-normalization-quiescence-attempt"
            && attempt.state == "fresh-quiescence-verified-under-switch-lock"
            && attempt.operation == operation
            && attempt.epoch == epoch
            && attempt.parent_intent == *parent_intent
            && !attempt.current_host_boot_id.is_empty()
            && name
                == quiescence_attempt_file_name(
                    operation,
                    &attempt.reader_quiescence,
                    attempt.active_directory,
                )?,
        "invalid or mismatched quiescence attempt"
    );
    Ok((attempt, binding))
}

fn exchange_attempt_file_name(
    operation: &str,
    quiescence_attempt: &FileBinding,
    active_directory: DirectoryIdentity,
    previous: Option<&FileBinding>,
) -> Result<String> {
    ensure!(matches!(operation, "cutover" | "rollback"));
    validate_sha256(&quiescence_attempt.sha256)?;
    let previous = previous
        .map(|binding| binding.sha256.as_str())
        .unwrap_or("root");
    Ok(format!(
        "archive-v2-metadata-normalization.{operation}-exchange-attempt.{}.{}.{}.v1.json",
        quiescence_attempt.sha256, active_directory.inode, previous
    ))
}

fn exchange_attempt_topologies(
    operation: &str,
    prepare: &PrepareReceipt,
) -> Result<(
    DirectoryIdentity,
    DirectoryIdentity,
    DirectoryIdentity,
    DirectoryIdentity,
)> {
    match operation {
        "cutover" => Ok((
            prepare.source_directory,
            prepare.target_directory,
            prepare.target_directory,
            prepare.source_directory,
        )),
        "rollback" => Ok((
            prepare.target_directory,
            prepare.source_directory,
            prepare.source_directory,
            prepare.target_directory,
        )),
        _ => bail!("unsupported exchange operation"),
    }
}

fn validate_exchange_attempt(
    attempt: &ExchangeAttempt,
    name: &str,
    operation: &str,
    prepare: &PrepareReceipt,
    parent_intent: &FileBinding,
    source_freeze: &FileBinding,
) -> Result<()> {
    let (pre_active, pre_retained, desired_active, desired_retained) =
        exchange_attempt_topologies(operation, prepare)?;
    ensure!(
        attempt.schema_version == 1
            && attempt.kind == "archive-v2-metadata-normalization-exchange-attempt"
            && attempt.state == "durable-intent-before-final-mutation-gates"
            && attempt.authorized_unix_seconds != 0
            && attempt.operation == operation
            && attempt.epoch == prepare.epoch
            && attempt.parent_intent == *parent_intent
            && attempt.source_freeze == *source_freeze
            && attempt.pre_active_directory == pre_active
            && attempt.pre_retained_directory == pre_retained
            && attempt.desired_active_directory == desired_active
            && attempt.desired_retained_directory == desired_retained
            && attempt.target_generation_digest == prepare.generation_digest
            && attempt.previous_exchange_attempt_file.is_some()
                == attempt.previous_exchange_attempt.is_some()
            && name
                == exchange_attempt_file_name(
                    operation,
                    &attempt.quiescence_attempt,
                    attempt.pre_active_directory,
                    attempt.previous_exchange_attempt.as_ref(),
                )?,
        "invalid or mismatched durable exchange attempt"
    );
    validate_sha256(&attempt.quiescence_attempt.sha256)?;
    if let Some(previous) = &attempt.previous_exchange_attempt {
        validate_sha256(&previous.sha256)?;
    }
    Ok(())
}

fn read_and_validate_exchange_attempt(
    journal: &Journal,
    name: &str,
    operation: &str,
    prepare: &PrepareReceipt,
    parent_intent: &FileBinding,
    source_freeze: &FileBinding,
) -> Result<(ExchangeAttempt, FileBinding)> {
    let (attempt, binding) = journal.read_required_bound::<ExchangeAttempt>(name)?;
    validate_exchange_attempt(
        &attempt,
        name,
        operation,
        prepare,
        parent_intent,
        source_freeze,
    )?;
    let (quiescence, quiescence_binding) = read_and_validate_quiescence_attempt(
        journal,
        &attempt.quiescence_attempt_file,
        operation,
        prepare.epoch,
        parent_intent,
    )?;
    let (pre_active, pre_retained, _, _) = exchange_attempt_topologies(operation, prepare)?;
    ensure!(
        quiescence_binding == attempt.quiescence_attempt
            && quiescence.active_directory == pre_active
            && quiescence.retained_directory == pre_retained,
        "exchange attempt does not bind its exact pre-topology quiescence attempt"
    );
    Ok((attempt, binding))
}

fn exchange_attempt_chain_tail(
    journal: &Journal,
    operation: &str,
    prepare: &PrepareReceipt,
    parent_intent: &FileBinding,
    source_freeze: &FileBinding,
) -> Result<Option<(String, ExchangeAttempt, FileBinding)>> {
    journal.recheck_path()?;
    let source = PinnedLocalRangeSource::from_directory_file(
        PathBuf::from("/descriptor-only-publication-journal"),
        journal.directory.try_clone()?,
    )?;
    let prefix = format!("archive-v2-metadata-normalization.{operation}-exchange-attempt.");
    let mut nodes = BTreeMap::new();
    for entry in source.inventory()? {
        let name = entry
            .name
            .into_string()
            .map_err(|_| anyhow!("journal contains a non-UTF-8 entry"))?;
        if !name.starts_with(&prefix) {
            continue;
        }
        ensure!(
            entry.kind == PinnedLocalEntryKind::RegularFile && name.ends_with(".v1.json"),
            "exchange-attempt journal entry is not one regular v1 file"
        );
        let (attempt, binding) = read_and_validate_exchange_attempt(
            journal,
            &name,
            operation,
            prepare,
            parent_intent,
            source_freeze,
        )?;
        ensure!(
            nodes.insert(name, (attempt, binding)).is_none(),
            "duplicate exchange-attempt journal name"
        );
    }
    if nodes.is_empty() {
        return Ok(None);
    }
    let mut roots = Vec::new();
    let mut child_by_parent = BTreeMap::new();
    for (name, (attempt, _)) in &nodes {
        match (
            attempt.previous_exchange_attempt_file.as_deref(),
            attempt.previous_exchange_attempt.as_ref(),
        ) {
            (None, None) => roots.push(name.clone()),
            (Some(previous_name), Some(previous_binding)) => {
                let (_, actual_binding) = nodes
                    .get(previous_name)
                    .context("exchange-attempt chain references an absent predecessor")?;
                ensure!(
                    actual_binding == previous_binding,
                    "exchange-attempt predecessor binding differs"
                );
                ensure!(
                    child_by_parent
                        .insert(previous_name.to_owned(), name.clone())
                        .is_none(),
                    "exchange-attempt chain branches"
                );
            }
            _ => bail!("exchange-attempt predecessor fields differ"),
        }
    }
    ensure!(
        roots.len() == 1,
        "exchange-attempt chain has no unique root"
    );
    let mut tail = roots.pop().expect("checked one root");
    let mut visited = 1usize;
    while let Some(child) = child_by_parent.get(&tail) {
        tail = child.clone();
        visited = visited.checked_add(1).context("attempt-chain overflow")?;
        ensure!(
            visited <= nodes.len(),
            "exchange-attempt chain contains a cycle"
        );
    }
    ensure!(
        visited == nodes.len(),
        "exchange-attempt journal contains a disconnected chain"
    );
    let (attempt, binding) = nodes.remove(&tail).expect("tail is in attempt map");
    journal.recheck_path()?;
    Ok(Some((tail, attempt, binding)))
}

fn publish_exchange_attempt(
    journal: &Journal,
    operation: &str,
    prepare: &PrepareReceipt,
    parent_intent: &FileBinding,
    source_freeze: &FileBinding,
    quiescence_attempt_file: &str,
    quiescence_attempt: &FileBinding,
) -> Result<(String, FileBinding)> {
    let previous =
        exchange_attempt_chain_tail(journal, operation, prepare, parent_intent, source_freeze)?;
    let (pre_active, pre_retained, desired_active, desired_retained) =
        exchange_attempt_topologies(operation, prepare)?;
    let attempt = ExchangeAttempt {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-exchange-attempt".to_owned(),
        state: "durable-intent-before-final-mutation-gates".to_owned(),
        authorized_unix_seconds: unix_seconds()?,
        operation: operation.to_owned(),
        epoch: prepare.epoch,
        parent_intent: parent_intent.clone(),
        source_freeze: source_freeze.clone(),
        quiescence_attempt_file: quiescence_attempt_file.to_owned(),
        quiescence_attempt: quiescence_attempt.clone(),
        previous_exchange_attempt_file: previous.as_ref().map(|(name, _, _)| name.clone()),
        previous_exchange_attempt: previous.as_ref().map(|(_, _, binding)| binding.clone()),
        pre_active_directory: pre_active,
        pre_retained_directory: pre_retained,
        desired_active_directory: desired_active,
        desired_retained_directory: desired_retained,
        target_generation_digest: prepare.generation_digest.clone(),
    };
    let name = exchange_attempt_file_name(
        operation,
        quiescence_attempt,
        pre_active,
        attempt.previous_exchange_attempt.as_ref(),
    )?;
    validate_exchange_attempt(
        &attempt,
        &name,
        operation,
        prepare,
        parent_intent,
        source_freeze,
    )?;
    let binding = journal.write_or_validate_json(&name, &attempt)?;
    let (tail_name, _, tail_binding) =
        exchange_attempt_chain_tail(journal, operation, prepare, parent_intent, source_freeze)?
            .context("published exchange-attempt chain is empty")?;
    ensure!(
        tail_name == name && tail_binding == binding,
        "published exchange attempt is not the unique chain tail"
    );
    Ok((name, binding))
}

/// Ensure post-switch topology when `select_target` is true, and pre-switch
/// topology when it is false. An interrupted exchange is recovered only from
/// the two exact directory identities in the durable prepare receipt.
fn ensure_selected_topology(
    receipt: &PrepareReceipt,
    source_freeze: &SourceFreezeComplete,
    select_target: bool,
    root_guard: &ArchiveRootGuard,
    before_exchange: impl FnOnce() -> Result<()>,
) -> Result<()> {
    root_guard.recheck_generation_paths(receipt)?;
    let left = PathAnchor::open(Path::new(&receipt.active_source))?;
    let right = PathAnchor::open(Path::new(&receipt.candidate))?;
    ensure!(
        directory_identity(&left.parent.metadata()?)? == root_guard.identity
            && directory_identity(&right.parent.metadata()?)? == root_guard.identity,
        "generation parents differ from the locked archive root"
    );
    ensure!(
        left.identity.device == right.identity.device,
        "atomic directory exchange requires one filesystem"
    );
    let pre =
        left.identity == receipt.source_directory && right.identity == receipt.target_directory;
    let post =
        left.identity == receipt.target_directory && right.identity == receipt.source_directory;
    ensure!(
        pre || post,
        "directory topology is ambiguous; refusing exchange"
    );
    if pre {
        verify_frozen_source_identities(&left.directory, source_freeze)?;
        verify_target_identities(
            &right.directory,
            &receipt.target_files,
            &receipt.publication_lock_identity,
            &receipt.target_frozen_directory_identity,
        )?;
    } else {
        verify_target_identities(
            &left.directory,
            &receipt.target_files,
            &receipt.publication_lock_identity,
            &receipt.target_frozen_directory_identity,
        )?;
        verify_frozen_source_identities(&right.directory, source_freeze)?;
    }
    ensure_anchor_unchanged(&left)?;
    ensure_anchor_unchanged(&right)?;
    root_guard.recheck_generation_paths(receipt)?;
    let exchange_required = if select_target { pre } else { post };
    if exchange_required {
        before_exchange()?;
        root_guard.recheck_generation_paths(receipt)?;
        ensure_anchor_unchanged(&left)?;
        ensure_anchor_unchanged(&right)?;
        exchange_anchors(&left, &right)?;
    }
    // A recovery attempt can observe the desired post-exchange topology after
    // a crash that happened before the original parent fsync. Always fsync
    // both retained parent descriptors before a durable completion receipt.
    left.parent.sync_all()?;
    right.parent.sync_all()?;
    root_guard.recheck()?;
    let active = PathAnchor::open(Path::new(&receipt.active_source))?;
    let retained = PathAnchor::open(Path::new(&receipt.candidate))?;
    let (expected_active, expected_retained) = if select_target {
        (receipt.target_directory, receipt.source_directory)
    } else {
        (receipt.source_directory, receipt.target_directory)
    };
    ensure!(
        active.identity == expected_active && retained.identity == expected_retained,
        "atomic directory exchange identity check failed"
    );
    if select_target {
        verify_target_identities(
            &active.directory,
            &receipt.target_files,
            &receipt.publication_lock_identity,
            &receipt.target_frozen_directory_identity,
        )?;
        verify_frozen_source_identities(&retained.directory, source_freeze)?;
    } else {
        verify_frozen_source_identities(&active.directory, source_freeze)?;
        verify_target_identities(
            &retained.directory,
            &receipt.target_files,
            &receipt.publication_lock_identity,
            &receipt.target_frozen_directory_identity,
        )?;
    }
    active.directory.sync_all()?;
    retained.directory.sync_all()?;
    Ok(())
}

fn verify_source_state(
    directory: &File,
    receipt: &PrepareReceipt,
    source_freeze: Option<&SourceFreezeComplete>,
) -> Result<()> {
    if let Some(source_freeze) = source_freeze {
        verify_frozen_source_identities(directory, source_freeze)
    } else {
        verify_source_identities(
            directory,
            &receipt.source_files,
            &receipt.ignored_unrelated_source_entries,
        )
    }
}

fn verify_source_content_against_prepare(prepare: &PrepareReceipt) -> Result<()> {
    let source = PinnedLocalRangeSource::open_directory(&prepare.active_source)
        .map_err(|error| anyhow!(error))?;
    ensure!(
        directory_identity(&source.directory_file()?.metadata()?)? == prepare.source_directory,
        "full source audit requires the pre-cutover source topology"
    );
    verify_source_identities(
        &source.directory_file()?,
        &prepare.source_files,
        &prepare.ignored_unrelated_source_entries,
    )?;
    for (name, proof) in &prepare.source_files {
        let file = source.open_file(name)?;
        ensure!(
            file.metadata()?.nlink() == 1 && hash_file(&file)? == proof.content,
            "armed historical source content differs for {name}"
        );
    }
    source.verify_unchanged().map_err(|error| anyhow!(error))?;
    Ok(())
}

fn verify_source_identities(
    directory: &File,
    expected: &BTreeMap<String, SourceFileBinding>,
    expected_ignored: &[String],
) -> Result<()> {
    let inventory = PinnedLocalRangeSource::from_directory_file(
        PathBuf::from("/descriptor-only-source"),
        directory.try_clone()?,
    )?;
    let mut actual_names = BTreeSet::new();
    let expected_ignored = expected_ignored.iter().cloned().collect::<BTreeSet<_>>();
    for entry in inventory.inventory()? {
        let name = entry
            .name
            .into_string()
            .map_err(|_| anyhow!("source contains a non-UTF-8 entry"))?;
        if expected.contains_key(&name) {
            ensure!(
                entry.kind == PinnedLocalEntryKind::RegularFile,
                "preserved source object {name} is not a regular file"
            );
        } else {
            ensure!(
                expected_ignored.contains(&name)
                    && !looks_like_archive_or_control(&name)
                    && matches!(
                        entry.kind,
                        PinnedLocalEntryKind::RegularFile | PinnedLocalEntryKind::Directory
                    ),
                "preserved source has an unknown or unsafe entry: {name}"
            );
        }
        actual_names.insert(name);
    }
    let expected_names = expected
        .keys()
        .cloned()
        .chain(expected_ignored.iter().cloned())
        .collect::<BTreeSet<_>>();
    ensure!(
        actual_names == expected_names,
        "preserved source inventory changed"
    );
    for (name, proof) in expected {
        let file = openat_regular_nofollow(directory, OsStr::new(name), false)?;
        ensure!(
            file_identity(&file.metadata()?, true)? == proof.identity
                && file.metadata()?.nlink() == 1,
            "preserved source identity changed for {name}"
        );
    }
    Ok(())
}

fn verify_target_identities(
    directory: &File,
    expected: &BTreeMap<String, FrozenFileProof>,
    expected_lock: &FileIdentity,
    expected_directory: &FileIdentity,
) -> Result<()> {
    let directory_metadata = directory.metadata()?;
    let directory_proof = file_identity(&directory_metadata, false)?;
    ensure!(
        directory_proof.device == expected_directory.device
            && directory_proof.inode == expected_directory.inode
            && directory_proof.bytes == expected_directory.bytes
            && directory_proof.mode == expected_directory.mode
            && directory_proof.modified_seconds == expected_directory.modified_seconds
            && directory_proof.modified_nanoseconds == expected_directory.modified_nanoseconds
            && directory_proof.mode & 0o777 == 0o555,
        "frozen target directory identity, mode, or modification time changed"
    );
    let inventory = PinnedLocalRangeSource::from_directory_file(
        PathBuf::from("/descriptor-only-target"),
        directory.try_clone()?,
    )?;
    let actual = inventory
        .inventory()?
        .into_iter()
        .map(|entry| {
            entry
                .name
                .into_string()
                .map_err(|_| anyhow!("target contains a non-UTF-8 entry"))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let mut expected_names = expected.keys().cloned().collect::<BTreeSet<_>>();
    expected_names.insert(ARCHIVE_V2_PUBLICATION_LOCK_FILE.to_owned());
    ensure!(actual == expected_names, "frozen target inventory changed");
    for (name, proof) in expected {
        let file = openat_regular_nofollow(directory, OsStr::new(name), false)?;
        ensure!(
            file_identity(&file.metadata()?, true)? == proof.identity
                && file.metadata()?.nlink() == 1,
            "frozen target identity changed for {name}"
        );
    }
    let lock = openat_regular_nofollow(
        directory,
        OsStr::new(ARCHIVE_V2_PUBLICATION_LOCK_FILE),
        false,
    )?;
    ensure!(
        file_identity(&lock.metadata()?, true)? == *expected_lock
            && lock.metadata()?.len() == 0
            && lock.metadata()?.nlink() == 1,
        "unlisted publication lock identity changed"
    );
    Ok(())
}

fn validate_spyx_process_authority(
    path: &Path,
    expected_sha256: &str,
) -> Result<(SpyxProcessAuthority, FileBinding, FileBinding)> {
    ensure!(
        path == Path::new(SPYX_PROCESS_AUTHORITY_PATH)
            && expected_sha256 == SPYX_PROCESS_AUTHORITY_SHA256,
        "SPYX process authority path or SHA-256 is not the fixed live observation"
    );
    validate_sha256(expected_sha256)?;
    let (bytes, identity, _authority_uid, _authority_gid) =
        read_absolute_regular_proven(path, MAX_JSON_BYTES)?;
    let binding = binding_for_bytes(&bytes);
    ensure!(
        binding.sha256 == expected_sha256
            && binding.bytes == SPYX_PROCESS_AUTHORITY_BYTES
            && identity.mode & 0o777 == 0o600,
        "SPYX process authority file identity or content differs"
    );
    let authority: SpyxProcessAuthority = parse_json(&bytes, "SPYX live-process authority")?;
    let expected_raw_proc_stat = "252572 (blockzilla-toke) S 252571 252572 252572 34816 252572 4194304 29650557 344 306128 0 27891644 584517 1 0 20 0 14 0 143723175 2182008832 120706 18446744073709551615 140118992093184 140118994859680 140731096136560 0 0 0 0 3149824 1088 0 0 0 17 0 0 0 0 0 0 140118994866144 140118994932768 93825658769408 140731096143817 140731096144426 140731096144426 140731096145789 0";
    let expected_argv = [
        OBSERVED_SPYX_EXECUTABLE_PATH,
        "extract",
        "/volume1/blockzilla/archive",
        SPYX_OUTPUT_ROOT,
        "--mint",
        SPYX_MINT,
        "--mint-slot",
        "346066298",
        "--mint-signature",
        SPYX_MINT_SIGNATURE,
        "--workers",
        "12",
        "--last-epoch",
        "1018",
        "--trusted-local",
        "--cluster-id",
        "mainnet-beta",
        "--slots-per-epoch",
        "432000",
        "--wire-profile",
        "post",
        "--single-read-batches",
        "--single-read-match-hints",
        "--resume",
    ];
    ensure!(
        authority.schema_version == 1
            && authority.kind == "blockzilla-token-transaction-dump-live-process-authority"
            && authority.state == "observed-live-and-executable-bound"
            && authority.observed_unix_seconds == 1_787_936_913
            && authority.observed_utc == "2026-08-28T17:08:33Z"
            && authority.host_boot_id == OBSERVED_SPYX_BOOT_ID
            && authority.process.pid == OBSERVED_SPYX_PID
            && authority.process.start_ticks == OBSERVED_SPYX_START_TICKS
            && authority.process.comm == "blockzilla-toke"
            && authority.process.state == "S"
            && authority.process.parent_pid == 252_571
            && authority.process.process_group_id == OBSERVED_SPYX_PID
            && authority.process.session_id == OBSERVED_SPYX_PID
            && authority.process.raw_proc_stat == expected_raw_proc_stat
            && authority.argv.len() == expected_argv.len()
            && authority.argv.iter().map(String::as_str).eq(expected_argv)
            && authority.executable.proc_exe_target == OBSERVED_SPYX_EXECUTABLE_PATH
            && authority.executable.path == OBSERVED_SPYX_EXECUTABLE_PATH
            && authority.executable.proc_exe_sha256 == OBSERVED_SPYX_EXECUTABLE_SHA256
            && authority.executable.path_sha256 == OBSERVED_SPYX_EXECUTABLE_SHA256
            && authority.executable.proc_exe_identity == authority.executable.path_identity
            && authority.run.archive_root == "/volume1/blockzilla/archive"
            && authority.run.output_root == SPYX_OUTPUT_ROOT
            && authority.run.mint == SPYX_MINT
            && authority.run.mint_slot == SPYX_MINT_SLOT
            && authority.run.mint_signature == SPYX_MINT_SIGNATURE
            && authority.run.workers == SPYX_WORKERS
            && authority.run.last_epoch == SPYX_LAST_EPOCH
            && authority.run.trusted_local
            && authority.run.cluster_id == "mainnet-beta"
            && authority.run.slots_per_epoch == 432_000
            && authority.run.wire_profile_cli == "post"
            && authority.run.single_read_batches
            && authority.run.single_read_match_hints
            && authority.run.resume,
        "SPYX process authority fields differ from the captured live process"
    );
    let observed = &authority.executable.path_identity;
    ensure!(
        observed.device == 64_770
            && observed.inode == 2_423_362_237
            && observed.bytes == 2_837_440
            && observed.mode == 33_133
            && observed.uid == 1_000
            && observed.gid == 10
            && observed.modified_unix_seconds == 1_787_899_027
            && observed.changed_unix_seconds == 1_787_899_130
            && observed.modified_timestamp == "2026-08-28 08:37:07.927826961 +0200"
            && observed.changed_timestamp == "2026-08-28 08:38:50.852129700 +0200",
        "SPYX process authority executable identity differs from the live capture"
    );
    let executable_path = Path::new(&authority.executable.path);
    let (executable_bytes, current_identity, current_uid, current_gid) =
        read_absolute_regular_proven(executable_path, 1 << 30)?;
    let executable_binding = binding_for_bytes(&executable_bytes);
    ensure!(
        executable_binding.sha256 == OBSERVED_SPYX_EXECUTABLE_SHA256
            && executable_binding.bytes == observed.bytes
            && current_identity.device == observed.device
            && current_identity.inode == observed.inode
            && current_identity.bytes == observed.bytes
            && current_identity.mode == observed.mode
            && current_identity.modified_seconds == observed.modified_unix_seconds
            && current_identity.changed_seconds == observed.changed_unix_seconds
            && current_uid == observed.uid
            && current_gid == observed.gid,
        "current SPYX executable differs from the live process authority"
    );
    Ok((authority, binding, executable_binding))
}

#[allow(clippy::too_many_arguments)] // Keep every operator-supplied trust input explicit.
fn validate_spyx_completion(
    root_manifest_path: &Path,
    expected_root_sha256: &str,
    checkpoint_path: &Path,
    expected_checkpoint_sha256: &str,
    prepare: &PrepareReceipt,
    expected_first_epoch: u64,
    expected_last_epoch: u64,
    expected_epoch_900_source_digest: &str,
    process_authority_path: &Path,
    expected_process_authority_sha256: &str,
) -> Result<CompletionGate> {
    ensure!(
        expected_first_epoch == SPYX_FIRST_EPOCH
            && expected_last_epoch == SPYX_LAST_EPOCH
            && expected_epoch_900_source_digest == OBSERVED_SPYX_EPOCH_900_SOURCE_DIGEST
            && root_manifest_path == Path::new(SPYX_OUTPUT_ROOT).join(DUMP_MANIFEST_FILE)
            && expected_root_sha256 == SPYX_FINAL_ROOT_MANIFEST_SHA256
            && checkpoint_path == Path::new(SPYX_OUTPUT_ROOT).join(RESUME_CHECKPOINT_FILE)
            && expected_checkpoint_sha256 == SPYX_FINAL_CHECKPOINT_SHA256
            && prepare.spyx_epoch_900_source_generation_digest == expected_epoch_900_source_digest
            && prepare.epoch == SUPPORTED_EPOCH
            && process_authority_path == Path::new(SPYX_PROCESS_AUTHORITY_PATH)
            && expected_process_authority_sha256 == SPYX_PROCESS_AUTHORITY_SHA256,
        "SPYX cutover gate must use the fixed 801..1018 epoch-900 run"
    );
    let (process_authority, process_authority_binding, extractor_executable) =
        validate_spyx_process_authority(process_authority_path, expected_process_authority_sha256)?;
    let extractor_pid = process_authority.process.pid;
    let extractor_start_ticks = process_authority.process.start_ticks;
    let extractor_boot_id = process_authority.host_boot_id.as_str();
    let extractor_executable_path = Path::new(&process_authority.executable.path);
    ensure!(
        root_manifest_path.parent() == checkpoint_path.parent()
            && root_manifest_path.file_name() == Some(OsStr::new("manifest.json"))
            && checkpoint_path.file_name() == Some(OsStr::new("resume-checkpoint.json")),
        "SPYX root manifest and checkpoint are not the two canonical files in one output root"
    );
    let output_root_path = root_manifest_path
        .parent()
        .context("SPYX manifest has no parent")?;
    ensure!(
        process_authority.run.output_root == output_root_path.display().to_string()
            && process_authority.run.archive_root
                == Path::new(&prepare.active_source)
                    .parent()
                    .context("prepared source has no archive root")?
                    .display()
                    .to_string(),
        "SPYX process authority run roots differ from the armed paths"
    );
    let output_root = PinnedLocalRangeSource::open_directory(output_root_path)
        .map_err(|error| anyhow!(error))
        .context("open one descriptor-pinned SPYX output root")?;
    let root_file = output_root.open_file("manifest.json")?;
    ensure!(
        root_file.metadata()?.nlink() == 1,
        "SPYX root manifest has multiple links"
    );
    let root_bytes = output_root.read_all_bounded("manifest.json", MAX_JSON_BYTES)?;
    let root_binding = binding_for_bytes(&root_bytes);
    ensure!(
        root_binding.sha256 == expected_root_sha256
            && root_binding.sha256 == SPYX_FINAL_ROOT_MANIFEST_SHA256
            && root_binding.bytes == SPYX_FINAL_ROOT_MANIFEST_BYTES,
        "SPYX root manifest differs from the independently accepted final file"
    );
    reject_unknown_dump_manifest_fields(&root_bytes)?;
    let root: DumpManifest = parse_json(&root_bytes, "SPYX root manifest")?;

    let checkpoint_file = output_root.open_file("resume-checkpoint.json")?;
    ensure!(
        checkpoint_file.metadata()?.nlink() == 1,
        "SPYX checkpoint has multiple links"
    );
    let checkpoint_bytes =
        output_root.read_all_bounded("resume-checkpoint.json", MAX_JSON_BYTES)?;
    let checkpoint_binding = binding_for_bytes(&checkpoint_bytes);
    ensure!(
        checkpoint_binding.sha256 == expected_checkpoint_sha256
            && checkpoint_binding.sha256 == SPYX_FINAL_CHECKPOINT_SHA256
            && checkpoint_binding.bytes == SPYX_FINAL_CHECKPOINT_BYTES,
        "SPYX resume checkpoint differs from the independently accepted final file"
    );
    let checkpoint: ResumeCheckpointEnvelope =
        parse_json(&checkpoint_bytes, "SPYX resume checkpoint")?;
    validate_sha256(&checkpoint.payload_sha256)?;
    let canonical_checkpoint_payload = serde_json::to_vec(&checkpoint.payload)?;
    let mut checkpoint_hasher = Sha256::new();
    checkpoint_hasher.update(CHECKPOINT_HASH_DOMAIN);
    checkpoint_hasher.update(&canonical_checkpoint_payload);
    ensure!(
        checkpoint.payload_sha256 == hex_lower(&checkpoint_hasher.finalize())
            && checkpoint.payload_sha256 == SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256,
        "SPYX checkpoint payload digest differs from the accepted final payload"
    );
    validate_fixed_spyx_controls(
        root_manifest_path,
        &root_binding,
        checkpoint_path,
        &checkpoint_binding,
        &checkpoint.payload_sha256,
    )?;
    checkpoint.payload.validate(None)?;
    let payload = &checkpoint.payload;
    ensure!(
        payload.stage == ResumeStage::Complete
            && payload.identity.extraction_mode == ResumeExtractionMode::SingleReadBatches
            && !payload.identity.single_read_match_hints
            && payload.identity.dump_schema_version == DUMP_SCHEMA_VERSION
            && payload.identity.mint == SPYX_MINT
            && payload.identity.mint_slot == SPYX_MINT_SLOT
            && payload.identity.mint_signature == SPYX_MINT_SIGNATURE
            && payload.identity.workers == SPYX_WORKERS
            && payload.identity.cluster_id == "mainnet-beta"
            && payload.identity.slots_per_epoch == 432_000
            && payload.identity.first_epoch == SPYX_FIRST_EPOCH
            && payload.identity.last_epoch == SPYX_LAST_EPOCH
            && payload.discovery_shards.len() == SPYX_EPOCH_COUNT
            && payload.raw_shards.len() == SPYX_EPOCH_COUNT
            && payload.frozen_accounts.is_some(),
        "SPYX checkpoint is not the complete 218-epoch single-read run"
    );
    for (offset, (discovery, raw)) in payload
        .discovery_shards
        .iter()
        .zip(&payload.raw_shards)
        .enumerate()
    {
        let epoch = SPYX_FIRST_EPOCH + offset as u64;
        ensure!(
            discovery.epoch == epoch
                && raw.epoch == epoch
                && discovery.source_generation_digest == raw.source_generation_digest,
            "SPYX discovery/raw source binding differs for epoch {epoch}"
        );
    }
    let epoch_offset = usize::try_from(prepare.epoch - SPYX_FIRST_EPOCH)?;
    let epoch_900_discovery = &payload.discovery_shards[epoch_offset];
    let epoch_900_raw = &payload.raw_shards[epoch_offset];
    ensure!(
        epoch_900_discovery.source_generation_digest == expected_epoch_900_source_digest
            && epoch_900_discovery.creation_log_sha256
                == "737dd09e8bb6ba963e23b4c6800ab9bacde2f85dc099ef4819c02407d6607a92"
            && epoch_900_discovery.creations == 120
            && epoch_900_raw.source_generation_digest == expected_epoch_900_source_digest
            && epoch_900_raw.transaction_stream_sha256
                == "11e917e832260c31fcf3caa99cb8b0156e4bf6585db41cdf82af5e8c67867c5b"
            && epoch_900_raw.account_id_log_sha256
                == "1ffd84d8464c7dba7a9778d0a3ff491db4dc3e113d09ff1f5ea3f83faad72c88"
            && epoch_900_raw.counters.transactions == 3_402
            && epoch_900_raw.counters.anchor_transactions == 0
            && epoch_900_raw.counters.blocks_scanned == 431_858
            && epoch_900_raw.counters.transactions_scanned == 476_026_811
            && epoch_900_raw.counters.owned_block_fallbacks == 0,
        "SPYX checkpoint does not bind the verified historical epoch-900 generation"
    );
    let anchor = payload
        .anchor_position
        .context("complete SPYX checkpoint has no anchor coordinate")?;
    ensure!(
        anchor.epoch == SPYX_FIRST_EPOCH
            && anchor.slot == SPYX_MINT_SLOT
            && anchor.source_block_id == 34_188
            && anchor.tx_index == 1_509
            && anchor.source_first_signature_ordinal == 57_250_747
            && anchor.signature_count == 2
            && payload.cumulative.anchor_transactions == 1
            && payload.cumulative.owned_block_fallbacks == 0
            && payload
                .raw_shards
                .iter()
                .all(|shard| shard.counters.owned_block_fallbacks == 0),
        "SPYX checkpoint anchor or fallback counters differ from the fixed run"
    );

    ensure!(
        root.schema_version == DUMP_SCHEMA_VERSION
            && root.artifact_kind == DumpArtifactKind::RawExtractionRoot
            && root.complete
            && root.mint == SPYX_MINT
            && root.mint_slot == SPYX_MINT_SLOT
            && root.mint_signature == SPYX_MINT_SIGNATURE
            && root.workers == SPYX_WORKERS
            && root.mint == payload.identity.mint
            && root.mint_slot == payload.identity.mint_slot
            && root.mint_signature == payload.identity.mint_signature
            && root.workers == payload.identity.workers
            && root.source_binding == payload.identity.source_binding
            && root.first_epoch == payload.identity.first_epoch
            && root.last_epoch == payload.identity.last_epoch
            && root.transactions == payload.cumulative.transactions,
        "SPYX root manifest and complete checkpoint run identities differ"
    );
    let frozen = payload
        .frozen_accounts
        .as_ref()
        .context("complete SPYX checkpoint has no frozen-account binding")?;
    ensure!(
        root.transaction_stream == EPOCH_SHARDS_DIR
            && root.transaction_stream_sha256.is_none()
            && root.account_id_log.is_none()
            && root.account_id_log_sha256.is_none()
            && root.discovered_accounts.as_deref() == Some(ACCOUNTS_FILE)
            && root.discovered_accounts_sha256.as_deref() == Some(frozen.accounts_sha256.as_str())
            && root.discovered_account_count == Some(frozen.account_count)
            && root.signatures.is_none()
            && root.pubkeys.is_none()
            && root.signature_stream.is_none()
            && root.signature_stream_sha256.is_none()
            && root.pubkey_registry.is_none()
            && root.pubkey_registry_sha256.is_none()
            && root.registry_maps.is_none(),
        "SPYX root manifest artifact bindings differ from the complete checkpoint"
    );
    validate_spyx_source_binding(prepare, &root.source_binding)?;
    let output_snapshot = validate_spyx_artifact_layout(&output_root, output_root_path, payload)?;
    output_root
        .verify_unchanged()
        .map_err(|error| anyhow!(error))?;
    ensure_extractor_process_stopped(extractor_pid, extractor_start_ticks, extractor_boot_id)?;
    Ok(CompletionGate {
        root_manifest_path: root_manifest_path.display().to_string(),
        root_manifest: root_binding,
        resume_checkpoint_path: checkpoint_path.display().to_string(),
        resume_checkpoint: checkpoint_binding,
        resume_checkpoint_payload_sha256: checkpoint.payload_sha256.clone(),
        process_authority_path: process_authority_path.display().to_string(),
        process_authority: process_authority_binding,
        first_epoch: SPYX_FIRST_EPOCH,
        last_epoch: SPYX_LAST_EPOCH,
        extraction_mode: "single_read_batches".to_owned(),
        source_generations: SPYX_EPOCH_COUNT as u64,
        extractor_pid,
        extractor_start_ticks,
        extractor_boot_id: extractor_boot_id.to_owned(),
        extractor_executable_path: extractor_executable_path.display().to_string(),
        extractor_executable,
        output_snapshot,
    })
}

fn validate_fixed_spyx_controls(
    root_manifest_path: &Path,
    root_manifest: &FileBinding,
    checkpoint_path: &Path,
    checkpoint: &FileBinding,
    checkpoint_payload_sha256: &str,
) -> Result<()> {
    ensure!(
        root_manifest_path == Path::new(SPYX_OUTPUT_ROOT).join(DUMP_MANIFEST_FILE)
            && root_manifest
                == &FileBinding {
                    bytes: SPYX_FINAL_ROOT_MANIFEST_BYTES,
                    sha256: SPYX_FINAL_ROOT_MANIFEST_SHA256.to_owned(),
                }
            && checkpoint_path == Path::new(SPYX_OUTPUT_ROOT).join(RESUME_CHECKPOINT_FILE)
            && checkpoint
                == &FileBinding {
                    bytes: SPYX_FINAL_CHECKPOINT_BYTES,
                    sha256: SPYX_FINAL_CHECKPOINT_SHA256.to_owned(),
                }
            && checkpoint_payload_sha256 == SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256,
        "SPYX controls differ from the independently accepted final run"
    );
    Ok(())
}

fn reject_unknown_dump_manifest_fields(bytes: &[u8]) -> Result<()> {
    let document: serde_json::Value = parse_json(bytes, "SPYX root manifest")?;
    let object = document
        .as_object()
        .context("SPYX root manifest is not an object")?;
    let allowed = BTreeSet::from([
        "schema_version",
        "artifact_kind",
        "complete",
        "mint",
        "mint_slot",
        "mint_signature",
        "workers",
        "source_binding",
        "first_epoch",
        "last_epoch",
        "transactions",
        "signatures",
        "pubkeys",
        "transaction_stream",
        "transaction_stream_sha256",
        "account_id_log",
        "account_id_log_sha256",
        "discovered_accounts",
        "discovered_accounts_sha256",
        "discovered_account_count",
        "signature_stream",
        "signature_stream_sha256",
        "pubkey_registry",
        "pubkey_registry_sha256",
        "registry_maps",
    ]);
    ensure!(
        object.keys().all(|key| allowed.contains(key.as_str())),
        "SPYX root manifest has an unknown field"
    );
    Ok(())
}

fn revalidate_completion_gate(gate: &CompletionGate) -> Result<()> {
    let manifest_path = Path::new(&gate.root_manifest_path);
    let checkpoint_path = Path::new(&gate.resume_checkpoint_path);
    validate_fixed_spyx_controls(
        manifest_path,
        &gate.root_manifest,
        checkpoint_path,
        &gate.resume_checkpoint,
        &gate.resume_checkpoint_payload_sha256,
    )?;
    ensure!(
        manifest_path == Path::new(SPYX_OUTPUT_ROOT).join(DUMP_MANIFEST_FILE)
            && checkpoint_path == Path::new(SPYX_OUTPUT_ROOT).join(RESUME_CHECKPOINT_FILE)
            && gate.root_manifest
                == FileBinding {
                    bytes: SPYX_FINAL_ROOT_MANIFEST_BYTES,
                    sha256: SPYX_FINAL_ROOT_MANIFEST_SHA256.to_owned(),
                }
            && gate.resume_checkpoint
                == FileBinding {
                    bytes: SPYX_FINAL_CHECKPOINT_BYTES,
                    sha256: SPYX_FINAL_CHECKPOINT_SHA256.to_owned(),
                }
            && gate.resume_checkpoint_payload_sha256 == SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256
            && manifest_path.parent() == checkpoint_path.parent()
            && manifest_path.file_name() == Some(OsStr::new("manifest.json"))
            && checkpoint_path.file_name() == Some(OsStr::new("resume-checkpoint.json")),
        "durable SPYX gate paths are no longer one canonical output root"
    );
    let output_root_path = manifest_path
        .parent()
        .context("SPYX gate has no output root")?;
    let output_root =
        PinnedLocalRangeSource::open_directory(output_root_path).map_err(|error| anyhow!(error))?;
    revalidate_spyx_output_snapshot(&output_root, &gate.output_snapshot)?;
    let root_bytes = output_root.read_all_bounded(DUMP_MANIFEST_FILE, MAX_JSON_BYTES)?;
    let checkpoint_bytes = output_root.read_all_bounded(RESUME_CHECKPOINT_FILE, MAX_JSON_BYTES)?;
    ensure!(
        binding_for_bytes(&root_bytes) == gate.root_manifest
            && binding_for_bytes(&checkpoint_bytes) == gate.resume_checkpoint,
        "SPYX completion controls changed after arming"
    );
    reject_unknown_dump_manifest_fields(&root_bytes)?;
    let root: DumpManifest = parse_json(&root_bytes, "revalidated SPYX root manifest")?;
    let checkpoint: ResumeCheckpointEnvelope =
        parse_json(&checkpoint_bytes, "revalidated SPYX resume checkpoint")?;
    validate_spyx_control_snapshot_contract(gate, &root, &checkpoint)?;
    ensure!(
        gate.output_snapshot
            .files
            .get(DUMP_MANIFEST_FILE)
            .is_some_and(|proof| proof.content == gate.root_manifest)
            && gate
                .output_snapshot
                .files
                .get(RESUME_CHECKPOINT_FILE)
                .is_some_and(|proof| proof.content == gate.resume_checkpoint),
        "SPYX output snapshot does not bind the completion controls"
    );
    let (_, process_authority, executable) = validate_spyx_process_authority(
        Path::new(&gate.process_authority_path),
        &gate.process_authority.sha256,
    )?;
    ensure!(
        process_authority == gate.process_authority && executable == gate.extractor_executable,
        "SPYX live-process authority or extractor changed before exchange"
    );
    ensure_extractor_process_stopped(
        gate.extractor_pid,
        gate.extractor_start_ticks,
        &gate.extractor_boot_id,
    )?;
    output_root
        .verify_unchanged()
        .map_err(|error| anyhow!(error))?;
    Ok(())
}

fn validate_spyx_control_snapshot_contract(
    gate: &CompletionGate,
    root: &DumpManifest,
    checkpoint: &ResumeCheckpointEnvelope,
) -> Result<()> {
    validate_sha256(&checkpoint.payload_sha256)?;
    let mut hasher = Sha256::new();
    hasher.update(CHECKPOINT_HASH_DOMAIN);
    hasher.update(serde_json::to_vec(&checkpoint.payload)?);
    ensure!(
        checkpoint.payload_sha256 == hex_lower(&hasher.finalize())
            && checkpoint.payload_sha256 == SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256
            && checkpoint.payload_sha256 == gate.resume_checkpoint_payload_sha256,
        "revalidated SPYX checkpoint payload digest differs from the accepted final payload"
    );
    checkpoint.payload.validate(None)?;
    let payload = &checkpoint.payload;
    let expected_source = DumpSourceBinding::TrustedLocalSizesOnly {
        cluster_id: "mainnet-beta".to_owned(),
        slots_per_epoch: 432_000,
        wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
    };
    ensure!(
        gate.first_epoch == SPYX_FIRST_EPOCH
            && gate.last_epoch == SPYX_LAST_EPOCH
            && gate.extraction_mode == "single_read_batches"
            && gate.source_generations == SPYX_EPOCH_COUNT as u64
            && payload.stage == ResumeStage::Complete
            && payload.identity.extraction_mode == ResumeExtractionMode::SingleReadBatches
            && !payload.identity.single_read_match_hints
            && payload.identity.dump_schema_version == DUMP_SCHEMA_VERSION
            && payload.identity.mint == SPYX_MINT
            && payload.identity.mint_slot == SPYX_MINT_SLOT
            && payload.identity.mint_signature == SPYX_MINT_SIGNATURE
            && payload.identity.workers == SPYX_WORKERS
            && payload.identity.cluster_id == "mainnet-beta"
            && payload.identity.slots_per_epoch == 432_000
            && payload.identity.first_epoch == SPYX_FIRST_EPOCH
            && payload.identity.last_epoch == SPYX_LAST_EPOCH
            && payload.identity.source_binding == expected_source
            && payload.discovery_shards.len() == SPYX_EPOCH_COUNT
            && payload.raw_shards.len() == SPYX_EPOCH_COUNT,
        "armed SPYX checkpoint identity differs from the fixed run"
    );
    let frozen = payload
        .frozen_accounts
        .as_ref()
        .context("armed SPYX checkpoint has no frozen accounts")?;
    ensure!(
        root.schema_version == DUMP_SCHEMA_VERSION
            && root.artifact_kind == DumpArtifactKind::RawExtractionRoot
            && root.complete
            && root.mint == payload.identity.mint
            && root.mint_slot == payload.identity.mint_slot
            && root.mint_signature == payload.identity.mint_signature
            && root.workers == payload.identity.workers
            && root.source_binding == payload.identity.source_binding
            && root.first_epoch == SPYX_FIRST_EPOCH
            && root.last_epoch == SPYX_LAST_EPOCH
            && root.transactions == payload.cumulative.transactions
            && root.transaction_stream == EPOCH_SHARDS_DIR
            && root.transaction_stream_sha256.is_none()
            && root.account_id_log.is_none()
            && root.account_id_log_sha256.is_none()
            && root.discovered_accounts.as_deref() == Some(ACCOUNTS_FILE)
            && root.discovered_accounts_sha256.as_deref() == Some(frozen.accounts_sha256.as_str())
            && root.discovered_account_count == Some(frozen.account_count)
            && root.signatures.is_none()
            && root.pubkeys.is_none()
            && root.signature_stream.is_none()
            && root.signature_stream_sha256.is_none()
            && root.pubkey_registry.is_none()
            && root.pubkey_registry_sha256.is_none()
            && root.registry_maps.is_none(),
        "armed SPYX root/checkpoint contract differs from the fixed run"
    );
    ensure!(
        gate.output_snapshot
            .files
            .get(ACCOUNTS_FILE)
            .is_some_and(|proof| proof.content.sha256 == frozen.accounts_sha256),
        "armed SPYX account artifact is not checkpoint-bound"
    );
    for (offset, (discovery, raw)) in payload
        .discovery_shards
        .iter()
        .zip(&payload.raw_shards)
        .enumerate()
    {
        let epoch = SPYX_FIRST_EPOCH + offset as u64;
        ensure!(
            discovery.epoch == epoch
                && raw.epoch == epoch
                && discovery.source_generation_digest == raw.source_generation_digest
                && gate
                    .output_snapshot
                    .files
                    .get(&format!(
                        "{DISCOVERY_SHARDS_DIR}/epoch-{epoch}/{CREATIONS_FILE}"
                    ))
                    .is_some_and(|proof| proof.content.sha256 == discovery.creation_log_sha256)
                && gate
                    .output_snapshot
                    .files
                    .get(&format!(
                        "{EPOCH_SHARDS_DIR}/epoch-{epoch}/{TRANSACTIONS_FILE}"
                    ))
                    .is_some_and(|proof| { proof.content.sha256 == raw.transaction_stream_sha256 })
                && gate
                    .output_snapshot
                    .files
                    .get(&format!(
                        "{EPOCH_SHARDS_DIR}/epoch-{epoch}/{ACCOUNT_ID_LOG_FILE}"
                    ))
                    .is_some_and(|proof| proof.content.sha256 == raw.account_id_log_sha256),
            "armed SPYX artifact snapshot differs from checkpoint epoch {epoch}"
        );
    }
    let epoch_900 = usize::try_from(SUPPORTED_EPOCH - SPYX_FIRST_EPOCH)?;
    let discovery = &payload.discovery_shards[epoch_900];
    let raw = &payload.raw_shards[epoch_900];
    let anchor = payload
        .anchor_position
        .context("armed SPYX checkpoint has no anchor")?;
    ensure!(
        discovery.source_generation_digest == OBSERVED_SPYX_EPOCH_900_SOURCE_DIGEST
            && discovery.creation_log_sha256
                == "737dd09e8bb6ba963e23b4c6800ab9bacde2f85dc099ef4819c02407d6607a92"
            && discovery.creations == 120
            && raw.source_generation_digest == OBSERVED_SPYX_EPOCH_900_SOURCE_DIGEST
            && raw.transaction_stream_sha256
                == "11e917e832260c31fcf3caa99cb8b0156e4bf6585db41cdf82af5e8c67867c5b"
            && raw.account_id_log_sha256
                == "1ffd84d8464c7dba7a9778d0a3ff491db4dc3e113d09ff1f5ea3f83faad72c88"
            && raw.counters.transactions == 3_402
            && raw.counters.anchor_transactions == 0
            && raw.counters.blocks_scanned == 431_858
            && raw.counters.transactions_scanned == 476_026_811
            && raw.counters.owned_block_fallbacks == 0
            && anchor.epoch == SPYX_FIRST_EPOCH
            && anchor.slot == SPYX_MINT_SLOT
            && anchor.source_block_id == 34_188
            && anchor.tx_index == 1_509
            && anchor.source_first_signature_ordinal == 57_250_747
            && anchor.signature_count == 2
            && payload.cumulative.anchor_transactions == 1
            && payload.cumulative.owned_block_fallbacks == 0
            && payload
                .raw_shards
                .iter()
                .all(|binding| binding.counters.owned_block_fallbacks == 0),
        "armed SPYX fixed epoch-900 or anchor proof differs"
    );
    validate_spyx_snapshot_path_contract(&gate.output_snapshot)?;
    Ok(())
}

fn validate_spyx_source_binding(
    prepare: &PrepareReceipt,
    binding: &DumpSourceBinding,
) -> Result<()> {
    ensure!(
        prepare.cluster_id == "mainnet-beta"
            && prepare.message_marker
                == NamedFileBinding {
                    name: wire_profile_marker(
                        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                    )
                    .name,
                    bytes: wire_profile_marker(
                        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                    )
                    .size,
                    sha256: wire_profile_marker(
                        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                    )
                    .sha256,
                },
        "prepared source does not match the fixed SPYX trusted-local identity"
    );
    match binding {
        DumpSourceBinding::TrustedLocalSizesOnly {
            cluster_id,
            slots_per_epoch,
            wire_profile,
        } => ensure!(
            cluster_id == "mainnet-beta"
                && *slots_per_epoch == 432_000
                && *wire_profile == DumpWireProfile::PostUnknownInstructionFallbacksV1,
            "SPYX trusted-local source binding differs from the fixed run"
        ),
        DumpSourceBinding::PublishedManifest { .. } => {
            bail!("the completed SPYX run unexpectedly used published-manifest input")
        }
    }
    Ok(())
}

fn validate_spyx_artifact_layout(
    output: &PinnedLocalRangeSource,
    output_root: &Path,
    payload: &ResumeCheckpointPayload,
) -> Result<SpyxOutputSnapshot> {
    let frozen = payload
        .frozen_accounts
        .as_ref()
        .context("complete SPYX checkpoint has no frozen-account binding")?;
    let mut snapshot = SpyxOutputSnapshot {
        directories: BTreeMap::new(),
        files: BTreeMap::new(),
    };
    snapshot.directories.insert(
        String::new(),
        file_identity(&output.directory_file()?.metadata()?, false)?,
    );
    let inventory = output.inventory()?;
    for forbidden in [
        "resume-checkpoint.pending.json",
        ".resume-checkpoint.pending.json.partial",
        "manifest.json.partial",
        "accounts.wincode.partial",
    ] {
        ensure!(
            !inventory
                .iter()
                .any(|entry| entry.name == OsStr::new(forbidden)),
            "SPYX output contains pending control {forbidden}"
        );
    }
    let root_entries = inventory
        .iter()
        .map(|entry| {
            Ok((
                entry
                    .name
                    .clone()
                    .into_string()
                    .map_err(|_| anyhow!("SPYX root name is not UTF-8"))?,
                entry.kind,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let expected_root_entries = BTreeMap::from([
        (ACCOUNTS_FILE.to_owned(), PinnedLocalEntryKind::RegularFile),
        (
            DISCOVERY_SHARDS_DIR.to_owned(),
            PinnedLocalEntryKind::Directory,
        ),
        (EPOCH_SHARDS_DIR.to_owned(), PinnedLocalEntryKind::Directory),
        (
            DUMP_MANIFEST_FILE.to_owned(),
            PinnedLocalEntryKind::RegularFile,
        ),
        (
            RESUME_CHECKPOINT_FILE.to_owned(),
            PinnedLocalEntryKind::RegularFile,
        ),
    ]);
    ensure!(
        root_entries == expected_root_entries,
        "SPYX output-root inventory is not exact"
    );
    let accounts = output.open_file(ACCOUNTS_FILE)?;
    let accounts_binding = hash_file(&accounts)?;
    ensure!(
        accounts.metadata()?.nlink() == 1 && accounts_binding.sha256 == frozen.accounts_sha256,
        "SPYX frozen-account artifact differs from the complete checkpoint"
    );
    snapshot.files.insert(
        ACCOUNTS_FILE.to_owned(),
        FrozenFileProof {
            identity: file_identity(&accounts.metadata()?, true)?,
            content: accounts_binding,
        },
    );
    for name in [DUMP_MANIFEST_FILE, RESUME_CHECKPOINT_FILE] {
        let file = output.open_file(name)?;
        ensure!(
            file.metadata()?.nlink() == 1,
            "SPYX root control has multiple links: {name}"
        );
        snapshot.files.insert(
            name.to_owned(),
            FrozenFileProof {
                identity: file_identity(&file.metadata()?, true)?,
                content: hash_file(&file)?,
            },
        );
    }
    let root_directory = output.directory_file()?;
    for shard_root_name in [DISCOVERY_SHARDS_DIR, EPOCH_SHARDS_DIR] {
        let shard_root = openat_directory_nofollow(&root_directory, OsStr::new(shard_root_name))?;
        let shard_source = PinnedLocalRangeSource::from_directory_file(
            output_root.join(shard_root_name),
            shard_root,
        )?;
        snapshot.directories.insert(
            shard_root_name.to_owned(),
            file_identity(&shard_source.directory_file()?.metadata()?, false)?,
        );
        let entries = shard_source.inventory()?;
        let mut actual = BTreeSet::new();
        for entry in entries {
            ensure!(
                entry.kind == PinnedLocalEntryKind::Directory,
                "SPYX {shard_root_name} shard layout is not exact"
            );
            actual.insert(
                entry
                    .name
                    .into_string()
                    .map_err(|_| anyhow!("SPYX shard name is not UTF-8"))?,
            );
        }
        let mut expected = (SPYX_FIRST_EPOCH..=SPYX_LAST_EPOCH)
            .map(|epoch| format!("epoch-{epoch}"))
            .collect::<BTreeSet<_>>();
        let allowed_quarantines = if shard_root_name == DISCOVERY_SHARDS_DIR {
            SPYX_DISCOVERY_QUARANTINES.as_slice()
        } else {
            SPYX_RAW_QUARANTINES.as_slice()
        };
        expected.extend(allowed_quarantines.iter().map(|name| (*name).to_owned()));
        ensure!(
            actual == expected,
            "SPYX {shard_root_name} final-shard and quarantine set is not exact"
        );
        shard_source
            .verify_unchanged()
            .map_err(|error| anyhow!(error))?;
    }

    ensure!(
        payload.discovery_shards.len() == SPYX_EPOCH_COUNT
            && payload.raw_shards.len() == SPYX_EPOCH_COUNT,
        "SPYX checkpoint shard bindings are incomplete"
    );
    let discovery_root =
        openat_directory_nofollow(&root_directory, OsStr::new(DISCOVERY_SHARDS_DIR))?;
    let raw_root = openat_directory_nofollow(&root_directory, OsStr::new(EPOCH_SHARDS_DIR))?;
    for (discovery, raw) in payload.discovery_shards.iter().zip(&payload.raw_shards) {
        ensure!(
            discovery.epoch == raw.epoch,
            "SPYX checkpoint discovery/raw epoch mismatch"
        );
        let epoch_name = format!("epoch-{}", discovery.epoch);
        let discovery_directory =
            openat_directory_nofollow(&discovery_root, OsStr::new(&epoch_name))?;
        let discovery_source = PinnedLocalRangeSource::from_directory_file(
            output_root.join(DISCOVERY_SHARDS_DIR).join(&epoch_name),
            discovery_directory,
        )?;
        snapshot.directories.insert(
            format!("{DISCOVERY_SHARDS_DIR}/{epoch_name}"),
            file_identity(&discovery_source.directory_file()?.metadata()?, false)?,
        );
        validate_exact_regular_inventory(
            &discovery_source,
            &[CREATIONS_FILE],
            "SPYX discovery shard",
        )?;
        let creations = discovery_source.open_file(CREATIONS_FILE)?;
        let creation_binding = hash_file(&creations)?;
        ensure!(
            creations.metadata()?.nlink() == 1
                && creation_binding.sha256 == discovery.creation_log_sha256,
            "SPYX epoch-{} creation log differs from the checkpoint",
            discovery.epoch
        );
        snapshot.files.insert(
            format!("{DISCOVERY_SHARDS_DIR}/{epoch_name}/{CREATIONS_FILE}"),
            FrozenFileProof {
                identity: file_identity(&creations.metadata()?, true)?,
                content: creation_binding,
            },
        );
        discovery_source
            .verify_unchanged()
            .map_err(|error| anyhow!(error))?;

        let raw_directory = openat_directory_nofollow(&raw_root, OsStr::new(&epoch_name))?;
        let raw_source = PinnedLocalRangeSource::from_directory_file(
            output_root.join(EPOCH_SHARDS_DIR).join(&epoch_name),
            raw_directory,
        )?;
        snapshot.directories.insert(
            format!("{EPOCH_SHARDS_DIR}/{epoch_name}"),
            file_identity(&raw_source.directory_file()?.metadata()?, false)?,
        );
        validate_exact_regular_inventory(
            &raw_source,
            &[DUMP_MANIFEST_FILE, TRANSACTIONS_FILE, ACCOUNT_ID_LOG_FILE],
            "SPYX raw shard",
        )?;
        let transactions = raw_source.open_file(TRANSACTIONS_FILE)?;
        let account_ids = raw_source.open_file(ACCOUNT_ID_LOG_FILE)?;
        let transaction_binding = hash_file(&transactions)?;
        let account_id_binding = hash_file(&account_ids)?;
        ensure!(
            transactions.metadata()?.nlink() == 1
                && account_ids.metadata()?.nlink() == 1
                && transaction_binding.sha256 == raw.transaction_stream_sha256
                && account_id_binding.sha256 == raw.account_id_log_sha256,
            "SPYX epoch-{} raw artifacts differ from the checkpoint",
            raw.epoch
        );
        snapshot.files.insert(
            format!("{EPOCH_SHARDS_DIR}/{epoch_name}/{TRANSACTIONS_FILE}"),
            FrozenFileProof {
                identity: file_identity(&transactions.metadata()?, true)?,
                content: transaction_binding,
            },
        );
        snapshot.files.insert(
            format!("{EPOCH_SHARDS_DIR}/{epoch_name}/{ACCOUNT_ID_LOG_FILE}"),
            FrozenFileProof {
                identity: file_identity(&account_ids.metadata()?, true)?,
                content: account_id_binding,
            },
        );
        let manifest_file = raw_source.open_file(DUMP_MANIFEST_FILE)?;
        ensure!(
            manifest_file.metadata()?.nlink() == 1,
            "SPYX epoch-{} manifest has multiple links",
            raw.epoch
        );
        let manifest_bytes = raw_source.read_all_bounded(DUMP_MANIFEST_FILE, MAX_JSON_BYTES)?;
        snapshot.files.insert(
            format!("{EPOCH_SHARDS_DIR}/{epoch_name}/{DUMP_MANIFEST_FILE}"),
            FrozenFileProof {
                identity: file_identity(&manifest_file.metadata()?, true)?,
                content: binding_for_bytes(&manifest_bytes),
            },
        );
        reject_unknown_dump_manifest_fields(&manifest_bytes)?;
        let manifest: DumpManifest = parse_json(&manifest_bytes, "SPYX raw-shard manifest")?;
        ensure!(
            manifest.schema_version == DUMP_SCHEMA_VERSION
                && manifest.artifact_kind == DumpArtifactKind::RawEpochShard
                && manifest.complete
                && manifest.mint == payload.identity.mint
                && manifest.mint_slot == payload.identity.mint_slot
                && manifest.mint_signature == payload.identity.mint_signature
                && manifest.workers == payload.identity.workers
                && manifest.source_binding == payload.identity.source_binding
                && manifest.first_epoch == raw.epoch
                && manifest.last_epoch == raw.epoch
                && manifest.transactions == raw.counters.transactions
                && manifest.transaction_stream == TRANSACTIONS_FILE
                && manifest.transaction_stream_sha256.as_deref()
                    == Some(raw.transaction_stream_sha256.as_str())
                && manifest.account_id_log.as_deref() == Some(ACCOUNT_ID_LOG_FILE)
                && manifest.account_id_log_sha256.as_deref()
                    == Some(raw.account_id_log_sha256.as_str())
                && manifest.signatures.is_none()
                && manifest.pubkeys.is_none()
                && manifest.discovered_accounts.is_none()
                && manifest.discovered_accounts_sha256.is_none()
                && manifest.discovered_account_count.is_none()
                && manifest.signature_stream.is_none()
                && manifest.signature_stream_sha256.is_none()
                && manifest.pubkey_registry.is_none()
                && manifest.pubkey_registry_sha256.is_none()
                && manifest.registry_maps.is_none(),
            "SPYX epoch-{} raw manifest differs from the checkpoint",
            raw.epoch
        );
        raw_source
            .verify_unchanged()
            .map_err(|error| anyhow!(error))?;
    }
    for (root_name, names) in [
        (DISCOVERY_SHARDS_DIR, SPYX_DISCOVERY_QUARANTINES.as_slice()),
        (EPOCH_SHARDS_DIR, SPYX_RAW_QUARANTINES.as_slice()),
    ] {
        let root = openat_directory_nofollow(&root_directory, OsStr::new(root_name))?;
        for name in names {
            let directory = openat_directory_nofollow(&root, OsStr::new(name))?;
            let source = PinnedLocalRangeSource::from_directory_file(
                output_root.join(root_name).join(name),
                directory,
            )?;
            snapshot.directories.insert(
                format!("{root_name}/{name}"),
                file_identity(&source.directory_file()?.metadata()?, false)?,
            );
            for entry in source.inventory()? {
                let file_name = entry
                    .name
                    .into_string()
                    .map_err(|_| anyhow!("SPYX quarantine contains a non-UTF-8 entry"))?;
                ensure!(
                    entry.kind == PinnedLocalEntryKind::RegularFile,
                    "SPYX quarantine {root_name}/{name} contains a non-regular entry"
                );
                let file = source.open_file(&file_name)?;
                ensure!(
                    file.metadata()?.nlink() == 1,
                    "SPYX quarantine object has multiple links: {root_name}/{name}/{file_name}"
                );
                snapshot.files.insert(
                    format!("{root_name}/{name}/{file_name}"),
                    FrozenFileProof {
                        identity: file_identity(&file.metadata()?, true)?,
                        content: hash_file(&file)?,
                    },
                );
            }
            source.verify_unchanged().map_err(|error| anyhow!(error))?;
        }
    }
    output.verify_unchanged().map_err(|error| anyhow!(error))?;
    Ok(snapshot)
}

fn validate_exact_regular_inventory(
    source: &PinnedLocalRangeSource,
    expected: &[&str],
    label: &str,
) -> Result<()> {
    let entries = source.inventory()?;
    let mut actual = BTreeSet::new();
    for entry in entries {
        ensure!(
            entry.kind == PinnedLocalEntryKind::RegularFile,
            "{label} contains a non-regular entry"
        );
        actual.insert(
            entry
                .name
                .into_string()
                .map_err(|_| anyhow!("{label} contains a non-UTF-8 entry"))?,
        );
    }
    ensure!(
        actual == expected.iter().map(|name| (*name).to_owned()).collect(),
        "{label} inventory is not exact"
    );
    Ok(())
}

fn revalidate_spyx_output_snapshot(
    output: &PinnedLocalRangeSource,
    snapshot: &SpyxOutputSnapshot,
) -> Result<()> {
    ensure!(
        snapshot.directories.contains_key("")
            && snapshot.files.contains_key(DUMP_MANIFEST_FILE)
            && snapshot.files.contains_key(RESUME_CHECKPOINT_FILE),
        "SPYX output snapshot is incomplete"
    );
    let root = output.directory_file()?;
    let mut expected_children: BTreeMap<String, BTreeMap<String, PinnedLocalEntryKind>> = snapshot
        .directories
        .keys()
        .map(|path| (path.clone(), BTreeMap::new()))
        .collect();
    for directory_path in snapshot.directories.keys().filter(|path| !path.is_empty()) {
        let path = Path::new(directory_path);
        let parent = path
            .parent()
            .and_then(Path::to_str)
            .unwrap_or_default()
            .to_owned();
        let name = path
            .file_name()
            .and_then(OsStr::to_str)
            .context("SPYX snapshot directory path is not canonical UTF-8")?;
        let previous = expected_children
            .get_mut(&parent)
            .context("SPYX snapshot misses a parent directory")?
            .insert(name.to_owned(), PinnedLocalEntryKind::Directory);
        ensure!(previous.is_none(), "duplicate SPYX snapshot directory");
    }
    for file_path in snapshot.files.keys() {
        let path = Path::new(file_path);
        let parent = path
            .parent()
            .and_then(Path::to_str)
            .unwrap_or_default()
            .to_owned();
        let name = path
            .file_name()
            .and_then(OsStr::to_str)
            .context("SPYX snapshot file path is not canonical UTF-8")?;
        let previous = expected_children
            .get_mut(&parent)
            .context("SPYX snapshot file has no bound parent directory")?
            .insert(name.to_owned(), PinnedLocalEntryKind::RegularFile);
        ensure!(previous.is_none(), "duplicate SPYX snapshot entry");
    }

    for (path, expected_identity) in &snapshot.directories {
        let directory = open_relative_directory_nofollow(&root, Path::new(path))?;
        ensure!(
            file_identity(&directory.metadata()?, false)? == *expected_identity,
            "SPYX directory identity changed: {path}"
        );
        let source = PinnedLocalRangeSource::from_directory_file(
            PathBuf::from("/descriptor-only-spyx-snapshot").join(path),
            directory,
        )?;
        let actual = source
            .inventory()?
            .into_iter()
            .map(|entry| {
                Ok((
                    entry
                        .name
                        .into_string()
                        .map_err(|_| anyhow!("SPYX snapshot contains a non-UTF-8 entry"))?,
                    entry.kind,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        ensure!(
            actual == expected_children[path],
            "SPYX directory inventory changed: {path}"
        );
    }
    for (path, proof) in &snapshot.files {
        let file = open_relative_regular_nofollow(&root, Path::new(path))?;
        ensure!(
            file.metadata()?.nlink() == 1
                && file_identity(&file.metadata()?, true)? == proof.identity,
            "SPYX file identity changed: {path}"
        );
    }
    // Reopen every recorded name after the first full pass. This detects a
    // descendant swap that happened after its retained descriptor was read.
    for (path, expected_identity) in &snapshot.directories {
        let directory = open_relative_directory_nofollow(&root, Path::new(path))?;
        ensure!(
            file_identity(&directory.metadata()?, false)? == *expected_identity,
            "SPYX directory name changed during snapshot revalidation: {path}"
        );
    }
    for (path, proof) in &snapshot.files {
        let file = open_relative_regular_nofollow(&root, Path::new(path))?;
        ensure!(
            file.metadata()?.nlink() == 1
                && file_identity(&file.metadata()?, true)? == proof.identity,
            "SPYX file name changed during snapshot revalidation: {path}"
        );
    }
    output.verify_unchanged().map_err(|error| anyhow!(error))?;
    Ok(())
}

fn validate_spyx_snapshot_path_contract(snapshot: &SpyxOutputSnapshot) -> Result<()> {
    let mut required_directories = BTreeSet::from([
        String::new(),
        DISCOVERY_SHARDS_DIR.to_owned(),
        EPOCH_SHARDS_DIR.to_owned(),
    ]);
    let mut required_files = BTreeSet::from([
        ACCOUNTS_FILE.to_owned(),
        DUMP_MANIFEST_FILE.to_owned(),
        RESUME_CHECKPOINT_FILE.to_owned(),
    ]);
    for epoch in SPYX_FIRST_EPOCH..=SPYX_LAST_EPOCH {
        required_directories.insert(format!("{DISCOVERY_SHARDS_DIR}/epoch-{epoch}"));
        required_directories.insert(format!("{EPOCH_SHARDS_DIR}/epoch-{epoch}"));
        required_files.insert(format!(
            "{DISCOVERY_SHARDS_DIR}/epoch-{epoch}/{CREATIONS_FILE}"
        ));
        required_files.insert(format!(
            "{EPOCH_SHARDS_DIR}/epoch-{epoch}/{DUMP_MANIFEST_FILE}"
        ));
        required_files.insert(format!(
            "{EPOCH_SHARDS_DIR}/epoch-{epoch}/{TRANSACTIONS_FILE}"
        ));
        required_files.insert(format!(
            "{EPOCH_SHARDS_DIR}/epoch-{epoch}/{ACCOUNT_ID_LOG_FILE}"
        ));
    }
    let quarantine_directories = SPYX_DISCOVERY_QUARANTINES
        .iter()
        .map(|name| format!("{DISCOVERY_SHARDS_DIR}/{name}"))
        .chain(
            SPYX_RAW_QUARANTINES
                .iter()
                .map(|name| format!("{EPOCH_SHARDS_DIR}/{name}")),
        )
        .collect::<BTreeSet<_>>();
    required_directories.extend(quarantine_directories.iter().cloned());
    ensure!(
        snapshot
            .directories
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            == required_directories,
        "SPYX snapshot directory path set is not the exact fixed run"
    );
    let actual_files = snapshot.files.keys().cloned().collect::<BTreeSet<_>>();
    ensure!(
        required_files.is_subset(&actual_files),
        "SPYX snapshot misses a fixed-run artifact"
    );
    for extra in actual_files.difference(&required_files) {
        let parent = Path::new(extra)
            .parent()
            .and_then(Path::to_str)
            .context("SPYX snapshot extra file has no UTF-8 parent")?;
        ensure!(
            quarantine_directories.contains(parent),
            "SPYX snapshot has an unexpected artifact: {extra}"
        );
    }
    Ok(())
}

fn open_relative_directory_nofollow(root: &File, path: &Path) -> Result<File> {
    ensure!(!path.is_absolute(), "relative SPYX path is absolute");
    let mut directory = root.try_clone()?;
    for component in path.components() {
        let Component::Normal(name) = component else {
            bail!("SPYX snapshot path is not normalized")
        };
        directory = openat_directory_nofollow(&directory, name)?;
    }
    Ok(directory)
}

fn open_relative_regular_nofollow(root: &File, path: &Path) -> Result<File> {
    ensure!(!path.is_absolute(), "relative SPYX file path is absolute");
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let name = path
        .file_name()
        .context("relative SPYX file path has no basename")?;
    let directory = open_relative_directory_nofollow(root, parent)?;
    openat_regular_nofollow(&directory, name, false)
}

#[allow(clippy::too_many_arguments)] // Optional bindings differ by operation and must stay explicit.
fn validate_reader_quiescence(
    path: &Path,
    expected_sha256: &str,
    operation: &str,
    prepare: &PrepareReceipt,
    prepare_binding: &FileBinding,
    minimum_issued_unix_seconds: u64,
    active_directory: DirectoryIdentity,
    retained_directory: DirectoryIdentity,
    switch_intent_binding: Option<&FileBinding>,
    source_freeze_binding: Option<&FileBinding>,
    switch_complete_binding: Option<&FileBinding>,
    spyx_root_sha256: Option<&str>,
    spyx_checkpoint_sha256: Option<&str>,
    spyx_epoch_900_source_digest: Option<&str>,
    spyx_process: Option<(u32, u64, &str, &Path, &str, &Path)>,
) -> Result<FileBinding> {
    let bytes = read_absolute_regular_bounded(path, MAX_JSON_BYTES)?;
    let binding = binding_for_bytes(&bytes);
    ensure!(
        binding.sha256 == expected_sha256,
        "reader-quiescence receipt differs from the command"
    );
    let receipt: ReaderQuiescenceReceipt = parse_json(&bytes, "reader-quiescence receipt")?;
    let now = unix_seconds()?;
    let current_boot_id = current_host_boot_id()?;
    ensure!(
        matches!(operation, "freeze-source" | "cutover" | "rollback")
            && receipt.schema_version == 1
            && receipt.kind == "archive-v2-metadata-normalization-reader-quiescence"
            && receipt.state == "all-readers-and-writers-stopped"
            && receipt.operation == operation
            && receipt.issued_unix_seconds >= prepare.prepared_unix_seconds
            && receipt.issued_unix_seconds >= minimum_issued_unix_seconds
            && receipt.issued_unix_seconds <= now
            && now - receipt.issued_unix_seconds <= QUIESCENCE_FRESHNESS_SECONDS
            && receipt.current_host_boot_id == current_boot_id
            && receipt.epoch == prepare.epoch
            && receipt.active_path == prepare.active_source
            && receipt.retained_path == prepare.candidate
            && receipt.active_directory == active_directory
            && receipt.retained_directory == retained_directory
            && receipt.prepare_receipt_sha256 == prepare_binding.sha256
            && receipt.switch_intent_sha256
                == switch_intent_binding.map(|binding| binding.sha256.clone())
            && receipt.source_freeze_sha256
                == source_freeze_binding.map(|binding| binding.sha256.clone())
            && receipt.switch_complete_sha256
                == switch_complete_binding.map(|binding| binding.sha256.clone())
            && receipt.spyx_root_manifest_sha256 == spyx_root_sha256.map(str::to_owned)
            && receipt.spyx_resume_checkpoint_sha256 == spyx_checkpoint_sha256.map(str::to_owned)
            && receipt.spyx_epoch_900_source_digest
                == spyx_epoch_900_source_digest.map(str::to_owned)
            && receipt.gateway_stopped
            && receipt.scheduler_stopped
            && receipt.archive_readers_stopped
            && receipt.archive_writers_stopped
            && receipt.extractor_stopped,
        "reader-quiescence receipt does not bind the exact operation and stopped services"
    );
    match spyx_process {
        Some((pid, start_ticks, boot_id, executable_path, executable_sha256, output_root)) => {
            ensure!(
                receipt.spyx_process_pid == Some(pid)
                    && receipt.spyx_process_start_ticks == Some(start_ticks)
                    && receipt.spyx_process_boot_id.as_deref() == Some(boot_id)
                    && receipt.spyx_extractor_executable_path.as_deref()
                        == Some(executable_path.to_string_lossy().as_ref())
                    && receipt.spyx_extractor_executable_sha256.as_deref()
                        == Some(executable_sha256)
                    && receipt.spyx_output_root.as_deref()
                        == Some(output_root.to_string_lossy().as_ref()),
                "reader-quiescence receipt does not bind the exact SPYX process"
            );
        }
        None => ensure!(
            receipt.spyx_process_pid.is_none()
                && receipt.spyx_process_start_ticks.is_none()
                && receipt.spyx_process_boot_id.is_none()
                && receipt.spyx_extractor_executable_path.is_none()
                && receipt.spyx_extractor_executable_sha256.is_none()
                && receipt.spyx_output_root.is_none(),
            "rollback quiescence receipt contains stale SPYX process fields"
        ),
    }
    validate_sha256(&receipt.prepare_receipt_sha256)?;
    Ok(binding)
}

fn read_absolute_regular_proven(
    path: &Path,
    limit: usize,
) -> Result<(Vec<u8>, FileIdentity, u32, u32)> {
    ensure_absolute_normalized(path)?;
    let parent_path = path.parent().context("file path has no parent")?;
    let parent =
        PinnedLocalRangeSource::open_directory(parent_path).map_err(|error| anyhow!(error))?;
    let parent_directory = parent.directory_file()?;
    let name = path.file_name().context("file path has no basename")?;
    let mut file = openat_regular_nofollow(&parent_directory, name, false)?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.nlink() == 1 && metadata.len() <= limit as u64,
        "bound file is linked more than once or exceeds its size limit: {}",
        path.display()
    );
    let identity = file_identity(&metadata, true)?;
    let uid = metadata.uid();
    let gid = metadata.gid();
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len())?);
    file.read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 == metadata.len() && file_identity(&file.metadata()?, true)? == identity,
        "bound file changed while it was read: {}",
        path.display()
    );
    let current = openat_regular_nofollow(&parent_directory, name, false)?;
    ensure!(
        file_identity(&current.metadata()?, true)? == identity && current.metadata()?.nlink() == 1,
        "bound file path changed while it was read: {}",
        path.display()
    );
    parent.verify_unchanged().map_err(|error| anyhow!(error))?;
    Ok((bytes, identity, uid, gid))
}

#[cfg(target_os = "linux")]
fn current_host_boot_id() -> Result<String> {
    let boot_id = String::from_utf8(read_absolute_regular_bounded(
        Path::new("/proc/sys/kernel/random/boot_id"),
        128,
    )?)?;
    let boot_id = boot_id.trim().to_owned();
    ensure!(!boot_id.is_empty(), "current Linux boot ID is empty");
    Ok(boot_id)
}

#[cfg(target_os = "macos")]
fn current_host_boot_id() -> Result<String> {
    Ok("macos-test-host-no-linux-boot-id".to_owned())
}

#[cfg(target_os = "linux")]
fn ensure_extractor_process_stopped(pid: u32, start_ticks: u64, boot_id: &str) -> Result<()> {
    if current_host_boot_id()? != boot_id {
        return Ok(());
    }
    let stat_path = PathBuf::from(format!("/proc/{pid}/stat"));
    let stat = match read_absolute_regular_bounded(&stat_path, 1 << 20) {
        Ok(bytes) => bytes,
        Err(error) if error_chain_has_not_found(&error) => return Ok(()),
        Err(error) => return Err(error).context("inspect observed SPYX process"),
    };
    let stat = String::from_utf8(stat)?;
    let after_name = stat
        .rsplit_once(')')
        .map(|(_, fields)| fields.trim())
        .context("Linux process stat has no command terminator")?;
    let observed_start = after_name
        .split_ascii_whitespace()
        .nth(19)
        .context("Linux process stat has no start-time field")?
        .parse::<u64>()?;
    ensure!(
        observed_start != start_ticks,
        "the exact observed SPYX extractor process is still live"
    );
    Ok(())
}

#[cfg(target_os = "macos")]
fn ensure_extractor_process_stopped(pid: u32, _start_ticks: u64, _boot_id: &str) -> Result<()> {
    // SAFETY: signal 0 does not send a signal; it only checks process existence.
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    ensure!(
        result != 0 && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH),
        "the observed extractor PID is still live or cannot be disproved"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn ensure_no_open_generation_fds(
    prepare: &PrepareReceipt,
    spyx: Option<(&SpyxOutputSnapshot, &Path)>,
) -> Result<()> {
    let mut roots = vec![
        PathBuf::from(&prepare.active_source),
        PathBuf::from(&prepare.candidate),
    ];
    let mut generation_identities = BTreeSet::from([
        (
            prepare.source_directory.device,
            prepare.source_directory.inode,
        ),
        (
            prepare.target_directory.device,
            prepare.target_directory.inode,
        ),
        (
            prepare.publication_lock_identity.device,
            prepare.publication_lock_identity.inode,
        ),
    ]);
    generation_identities.extend(
        prepare
            .source_files
            .values()
            .map(|proof| (proof.identity.device, proof.identity.inode)),
    );
    if let Some((snapshot, output_root)) = spyx {
        roots.push(output_root.to_path_buf());
        generation_identities.extend(
            snapshot
                .directories
                .values()
                .map(|identity| (identity.device, identity.inode)),
        );
        generation_identities.extend(
            snapshot
                .files
                .values()
                .map(|proof| (proof.identity.device, proof.identity.inode)),
        );
    }
    generation_identities.extend(
        prepare
            .target_files
            .values()
            .map(|proof| (proof.identity.device, proof.identity.inode)),
    );
    let own_pid = std::process::id();
    for process in std::fs::read_dir("/proc").context("enumerate Linux processes")? {
        let process = process?;
        let Some(pid) = process
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == own_pid {
            continue;
        }
        let process_root = process.path();
        let fd_root = process_root.join("fd");
        let descriptors = match std::fs::read_dir(&fd_root) {
            Ok(descriptors) => descriptors,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("cannot prove descriptor quiescence for process {pid}")
                });
            }
        };
        for descriptor in descriptors {
            let descriptor = match descriptor {
                Ok(descriptor) => descriptor,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let target = match std::fs::read_link(descriptor.path()) {
                Ok(target) => target,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let metadata = match std::fs::metadata(descriptor.path()) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("cannot identify descriptor for process {pid}"));
                }
            };
            ensure!(
                !roots
                    .iter()
                    .any(|root| proc_target_is_within(&target, root))
                    && !generation_identities.contains(&(metadata.dev(), metadata.ino())),
                "process {pid} still has an open descriptor in a selected generation: {}",
                target.display()
            );
        }
        let mappings_root = process_root.join("map_files");
        let mappings = match std::fs::read_dir(&mappings_root) {
            Ok(mappings) => mappings,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("cannot prove mmap quiescence for process {pid}"));
            }
        };
        for mapping in mappings {
            let mapping = match mapping {
                Ok(mapping) => mapping,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let target = match std::fs::read_link(mapping.path()) {
                Ok(target) => target,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let metadata = match std::fs::metadata(mapping.path()) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("cannot identify mmap for process {pid}"));
                }
            };
            ensure!(
                !roots
                    .iter()
                    .any(|root| proc_target_is_within(&target, root))
                    && !generation_identities.contains(&(metadata.dev(), metadata.ino())),
                "process {pid} still maps an object in a selected generation: {}",
                target.display()
            );
        }
        for process_link in ["cwd", "root"] {
            let link_path = process_root.join(process_link);
            let target = match std::fs::read_link(&link_path) {
                Ok(target) => target,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                    ) =>
                {
                    if error.kind() == io::ErrorKind::PermissionDenied {
                        bail!("cannot prove {process_link} quiescence for process {pid}");
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let metadata = match std::fs::metadata(&link_path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("cannot identify {process_link} for process {pid}")
                    });
                }
            };
            ensure!(
                !roots
                    .iter()
                    .any(|root| proc_target_is_within(&target, root))
                    && !generation_identities.contains(&(metadata.dev(), metadata.ino())),
                "process {pid} still has {process_link} in a selected generation: {}",
                target.display()
            );
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn proc_target_is_within(target: &Path, root: &Path) -> bool {
    let target = target.to_string_lossy();
    let target = target.strip_suffix(" (deleted)").unwrap_or(&target);
    let target = Path::new(target);
    target == root || target.starts_with(root)
}

#[cfg(target_os = "macos")]
fn ensure_no_open_generation_fds(
    _prepare: &PrepareReceipt,
    _spyx: Option<(&SpyxOutputSnapshot, &Path)>,
) -> Result<()> {
    bail!("cutover and rollback require the Linux /proc descriptor-quiescence gate")
}

fn validate_switch_complete(
    complete: &SwitchComplete,
    prepare: &PrepareReceipt,
    intent_binding: &FileBinding,
    source_freeze_binding: &FileBinding,
    exchange_attempt_file: &str,
    exchange_attempt: &ExchangeAttempt,
    exchange_attempt_binding: &FileBinding,
) -> Result<()> {
    ensure!(
        complete.schema_version == 1
            && complete.kind == "archive-v2-metadata-normalization-switch-complete"
            && complete.state == "canonical-target-selected-source-preserved"
            && complete.completed_unix_seconds != 0
            && complete.epoch == prepare.epoch
            && complete.intent == *intent_binding
            && complete.source_freeze == *source_freeze_binding
            && complete.quiescence_attempt == exchange_attempt.quiescence_attempt
            && complete.quiescence_attempt_file == exchange_attempt.quiescence_attempt_file
            && complete.exchange_attempt == *exchange_attempt_binding
            && complete.exchange_attempt_file == exchange_attempt_file
            && complete.active_target == prepare.active_source
            && complete.preserved_legacy_source == prepare.candidate
            && complete.target_directory == prepare.target_directory
            && complete.source_directory == prepare.source_directory
            && complete.active_target_directory_identity.device == prepare.target_directory.device
            && complete.active_target_directory_identity.inode == prepare.target_directory.inode
            && complete.active_target_directory_identity.mode & 0o777 == 0o555
            && complete.preserved_source_directory_identity.device
                == prepare.source_directory.device
            && complete.preserved_source_directory_identity.inode == prepare.source_directory.inode
            && complete.preserved_source_directory_identity.mode & 0o777 == 0o555
            && complete.target_generation_digest == prepare.generation_digest,
        "invalid or mismatched cutover completion receipt"
    );
    Ok(())
}

fn validate_switch_intent(
    intent: &SwitchIntent,
    prepare: &PrepareReceipt,
    prepare_binding: &FileBinding,
) -> Result<()> {
    let gate = &intent.spyx_completion_gate;
    validate_fixed_spyx_controls(
        Path::new(&gate.root_manifest_path),
        &gate.root_manifest,
        Path::new(&gate.resume_checkpoint_path),
        &gate.resume_checkpoint,
        &gate.resume_checkpoint_payload_sha256,
    )?;
    ensure!(
        intent.schema_version == 1
            && intent.kind == "archive-v2-metadata-normalization-switch-intent"
            && intent.state == "ready-for-atomic-directory-exchange"
            && intent.armed_unix_seconds >= prepare.prepared_unix_seconds
            && intent.epoch == prepare.epoch
            && intent.prepare_receipt == *prepare_binding
            && intent.active_source == prepare.active_source
            && intent.candidate == prepare.candidate
            && intent.source_directory == prepare.source_directory
            && intent.target_directory == prepare.target_directory
            && intent.target_generation_digest == prepare.generation_digest
            && gate.first_epoch == SPYX_FIRST_EPOCH
            && gate.last_epoch == SPYX_LAST_EPOCH
            && gate.extraction_mode == "single_read_batches"
            && gate.source_generations == SPYX_EPOCH_COUNT as u64
            && gate.root_manifest_path
                == Path::new(SPYX_OUTPUT_ROOT)
                    .join(DUMP_MANIFEST_FILE)
                    .display()
                    .to_string()
            && gate.root_manifest
                == FileBinding {
                    bytes: SPYX_FINAL_ROOT_MANIFEST_BYTES,
                    sha256: SPYX_FINAL_ROOT_MANIFEST_SHA256.to_owned(),
                }
            && gate.resume_checkpoint_path
                == Path::new(SPYX_OUTPUT_ROOT)
                    .join(RESUME_CHECKPOINT_FILE)
                    .display()
                    .to_string()
            && gate.resume_checkpoint
                == FileBinding {
                    bytes: SPYX_FINAL_CHECKPOINT_BYTES,
                    sha256: SPYX_FINAL_CHECKPOINT_SHA256.to_owned(),
                }
            && gate.resume_checkpoint_payload_sha256 == SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256
            && gate.process_authority_path == SPYX_PROCESS_AUTHORITY_PATH
            && gate.process_authority.sha256 == SPYX_PROCESS_AUTHORITY_SHA256
            && gate.process_authority.bytes == SPYX_PROCESS_AUTHORITY_BYTES
            && gate.extractor_pid == OBSERVED_SPYX_PID
            && gate.extractor_start_ticks == OBSERVED_SPYX_START_TICKS
            && gate.extractor_boot_id == OBSERVED_SPYX_BOOT_ID
            && gate.extractor_executable_path == OBSERVED_SPYX_EXECUTABLE_PATH
            && gate.extractor_executable.sha256 == OBSERVED_SPYX_EXECUTABLE_SHA256,
        "invalid or mismatched armed cutover intent"
    );
    validate_sha256(&gate.root_manifest.sha256)?;
    validate_sha256(&gate.resume_checkpoint.sha256)?;
    validate_sha256(&gate.process_authority.sha256)?;
    validate_sha256(&gate.extractor_executable.sha256)?;
    let manifest_path = Path::new(&gate.root_manifest_path);
    let checkpoint_path = Path::new(&gate.resume_checkpoint_path);
    ensure_absolute_normalized(manifest_path)?;
    ensure_absolute_normalized(checkpoint_path)?;
    ensure_absolute_normalized(Path::new(&gate.process_authority_path))?;
    ensure_absolute_normalized(Path::new(&gate.extractor_executable_path))?;
    ensure!(
        manifest_path.parent() == checkpoint_path.parent()
            && manifest_path.file_name() == Some(OsStr::new(DUMP_MANIFEST_FILE))
            && checkpoint_path.file_name() == Some(OsStr::new(RESUME_CHECKPOINT_FILE)),
        "armed SPYX controls are not in one canonical output root"
    );
    let root_identity = gate
        .output_snapshot
        .directories
        .get("")
        .context("armed SPYX snapshot has no root directory")?;
    validate_spyx_snapshot_path_contract(&gate.output_snapshot)?;
    ensure!(
        gate.output_snapshot
            .files
            .get(DUMP_MANIFEST_FILE)
            .is_some_and(|proof| proof.content == gate.root_manifest)
            && gate
                .output_snapshot
                .files
                .get(RESUME_CHECKPOINT_FILE)
                .is_some_and(|proof| proof.content == gate.resume_checkpoint),
        "armed SPYX snapshot does not bind both completion controls"
    );
    for (path, identity) in &gate.output_snapshot.directories {
        validate_relative_snapshot_path(path, true)?;
        ensure!(
            identity.device == root_identity.device,
            "SPYX snapshot directory crosses filesystems"
        );
    }
    for (path, proof) in &gate.output_snapshot.files {
        validate_relative_snapshot_path(path, false)?;
        validate_sha256(&proof.content.sha256)?;
        ensure!(
            proof.identity.device == root_identity.device
                && proof.identity.bytes == proof.content.bytes,
            "SPYX snapshot file proof is inconsistent"
        );
    }
    Ok(())
}

fn validate_relative_snapshot_path(path: &str, allow_empty: bool) -> Result<()> {
    ensure!(
        allow_empty || !path.is_empty(),
        "SPYX snapshot file path is empty"
    );
    let path = Path::new(path);
    ensure!(!path.is_absolute(), "SPYX snapshot path is absolute");
    ensure!(
        path.components()
            .all(|component| matches!(component, Component::Normal(_))),
        "SPYX snapshot path is not normalized"
    );
    Ok(())
}

fn validate_rollback_complete(
    complete: &RollbackComplete,
    prepare: &PrepareReceipt,
    intent_binding: &FileBinding,
    exchange_attempt_file: &str,
    exchange_attempt: &ExchangeAttempt,
    exchange_attempt_binding: &FileBinding,
) -> Result<()> {
    ensure!(
        complete.schema_version == 1
            && complete.kind == "archive-v2-metadata-normalization-rollback-complete"
            && complete.state == "historical-source-restored-target-retained"
            && complete.completed_unix_seconds != 0
            && complete.epoch == prepare.epoch
            && complete.intent == *intent_binding
            && complete.quiescence_attempt == exchange_attempt.quiescence_attempt
            && complete.quiescence_attempt_file == exchange_attempt.quiescence_attempt_file
            && complete.exchange_attempt == *exchange_attempt_binding
            && complete.exchange_attempt_file == exchange_attempt_file
            && complete.restored_source == prepare.active_source
            && complete.retained_target == prepare.candidate
            && complete.source_directory == prepare.source_directory
            && complete.target_directory == prepare.target_directory
            && complete.restored_source_directory_identity.device
                == prepare.source_directory.device
            && complete.restored_source_directory_identity.inode == prepare.source_directory.inode
            && complete.restored_source_directory_identity.mode & 0o777 == 0o555
            && complete.retained_target_directory_identity.device
                == prepare.target_directory.device
            && complete.retained_target_directory_identity.inode == prepare.target_directory.inode
            && complete.retained_target_directory_identity.mode & 0o777 == 0o555,
        "invalid or mismatched rollback completion receipt"
    );
    Ok(())
}

fn validate_rollback_intent(
    actual: &RollbackIntent,
    expected: &RollbackIntent,
    switch_complete_binding: &FileBinding,
) -> Result<()> {
    ensure!(
        actual.schema_version == 1
            && actual.kind == "archive-v2-metadata-normalization-rollback-intent"
            && actual.state == "ready-for-atomic-directory-exchange"
            && actual.switch_complete == *switch_complete_binding
            && actual.schema_version == expected.schema_version
            && actual.kind == expected.kind
            && actual.state == expected.state
            && actual.epoch == expected.epoch
            && actual.switch_complete == expected.switch_complete
            && actual.active_target == expected.active_target
            && actual.preserved_legacy_source == expected.preserved_legacy_source
            && actual.target_directory == expected.target_directory
            && actual.source_directory == expected.source_directory,
        "invalid or mismatched rollback intent"
    );
    Ok(())
}

impl Journal {
    fn open_or_create(path: &Path) -> Result<Self> {
        ensure_absolute_normalized(path)?;
        if path_entry_exists_nofollow(path)? {
            return Self::open_existing(path);
        }
        let parent_path = path.parent().context("journal path has no parent")?;
        let parent =
            PinnedLocalRangeSource::open_directory(parent_path).map_err(|error| anyhow!(error))?;
        let parent_file = parent.directory_file()?;
        let name = path.file_name().context("journal has no basename")?;
        mkdirat_private(&parent_file, name)?;
        parent_file.sync_all()?;
        Self::open_existing(path)
    }

    fn open_existing(path: &Path) -> Result<Self> {
        let source =
            PinnedLocalRangeSource::open_directory(path).map_err(|error| anyhow!(error))?;
        let directory = source.directory_file()?;
        let anchor = PathAnchor::open(path)?;
        ensure!(
            anchor.identity == directory_identity(&directory.metadata()?)?,
            "journal path changed while it was opened"
        );
        let mount_id = descriptor_mount_id(&directory)?;
        ensure!(
            descriptor_mount_id(&anchor.parent)? == mount_id,
            "journal directory is a mount-point or bind-mount alias"
        );
        let lock = FileLock::acquire_at(&directory, JOURNAL_LOCK_FILE)?;
        let journal = Self {
            directory,
            anchor,
            mount_id,
            _lock: lock,
        };
        journal.recheck_path()?;
        journal.directory.sync_all()?;
        journal.recheck_path()?;
        Ok(journal)
    }

    fn recheck_path(&self) -> Result<()> {
        ensure_anchor_unchanged(&self.anchor)?;
        ensure!(
            directory_identity(&self.directory.metadata()?)? == self.anchor.identity,
            "journal descriptor and path identities differ"
        );
        ensure!(
            descriptor_mount_id(&self.directory)? == self.mount_id
                && descriptor_mount_id(&self.anchor.parent)? == self.mount_id,
            "journal mount identity changed"
        );
        self._lock.recheck_at(&self.directory, JOURNAL_LOCK_FILE)?;
        Ok(())
    }

    fn require_same_mount_as(&self, directory: &File) -> Result<()> {
        self.recheck_path()?;
        ensure!(
            descriptor_mount_id(directory)? == self.mount_id,
            "journal and archive root do not use the same mount identity"
        );
        Ok(())
    }

    fn read_optional_json<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        self.recheck_path()?;
        let Some(bytes) = read_optional_regular_at(&self.directory, name, MAX_JSON_BYTES)? else {
            self.recheck_path()?;
            return Ok(None);
        };
        let value = parse_json(&bytes, name)?;
        self.recheck_path()?;
        Ok(Some(value))
    }

    fn read_required_bound<T: DeserializeOwned>(&self, name: &str) -> Result<(T, FileBinding)> {
        self.recheck_path()?;
        let bytes = read_optional_regular_at(&self.directory, name, MAX_JSON_BYTES)?
            .with_context(|| format!("required journal object is absent: {name}"))?;
        let value = parse_json(&bytes, name)?;
        let binding = binding_for_bytes(&bytes);
        self.recheck_path()?;
        Ok((value, binding))
    }

    fn write_json_no_replace<T: Serialize>(&self, name: &str, value: &T) -> Result<FileBinding> {
        self.recheck_path()?;
        ensure!(
            read_optional_regular_at(&self.directory, name, MAX_JSON_BYTES)?.is_none(),
            "journal object already exists: {name}"
        );
        let bytes = pretty_json_bytes(value)?;
        let binding = binding_for_bytes(&bytes);
        publish_immutable_at(&self.directory, name, &bytes)?;
        self.recheck_path()?;
        Ok(binding)
    }

    fn write_or_validate_json<T>(&self, name: &str, value: &T) -> Result<FileBinding>
    where
        T: Serialize + DeserializeOwned,
    {
        self.recheck_path()?;
        let expected = pretty_json_bytes(value)?;
        let binding = binding_for_bytes(&expected);
        if let Some(actual) = read_optional_regular_at(&self.directory, name, MAX_JSON_BYTES)? {
            ensure!(
                actual == expected,
                "existing journal object differs: {name}"
            );
            self.directory.sync_all()?;
            self.recheck_path()?;
            return Ok(binding);
        }
        self.write_json_no_replace(name, value)
    }
}

impl FileLock {
    fn acquire_at(directory: &File, name: &str) -> Result<Self> {
        let file = openat_regular_nofollow(directory, OsStr::new(name), true)?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.len() == 0 && metadata.nlink() == 1,
            "lock file {name} is not one empty private file"
        );
        // SAFETY: the live lock descriptor is owned by this process.
        ensure!(
            unsafe { libc::fchmod(file.as_raw_fd(), 0o600) } == 0,
            "set private lock mode: {}",
            io::Error::last_os_error()
        );
        // SAFETY: the descriptor remains live for the lifetime of this guard.
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0,
            "acquire file lock: {}",
            io::Error::last_os_error()
        );
        let identity = file_identity(&file.metadata()?, true)?;
        let lock = Self { file, identity };
        lock.recheck_at(directory, name)?;
        Ok(lock)
    }

    fn acquire_existing_at(directory: &File, name: &str) -> Result<Self> {
        let file = openat_regular_nofollow(directory, OsStr::new(name), false)?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.len() == 0 && metadata.nlink() == 1 && metadata.mode() & 0o777 == 0o600,
            "existing lock file {name} is not one empty private file"
        );
        // SAFETY: the retained descriptor is live for this guard.
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0,
            "acquire existing file lock: {}",
            io::Error::last_os_error()
        );
        let identity = file_identity(&file.metadata()?, true)?;
        let lock = Self { file, identity };
        lock.recheck_at(directory, name)?;
        Ok(lock)
    }

    fn recheck_at(&self, directory: &File, name: &str) -> Result<()> {
        let current = openat_regular_nofollow(directory, OsStr::new(name), false)?;
        let metadata = current.metadata()?;
        ensure!(
            metadata.dev() == self.identity.device
                && metadata.ino() == self.identity.inode
                && metadata.len() == 0
                && metadata.nlink() == 1
                && metadata.mode() & 0o777 == 0o600,
            "lock path identity changed: {name}"
        );
        Ok(())
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // SAFETY: the descriptor is live until this method returns.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

impl PathAnchor {
    fn open(path: &Path) -> Result<Self> {
        ensure_absolute_normalized(path)?;
        let parent_path = path.parent().context("path has no parent")?;
        let parent_source =
            PinnedLocalRangeSource::open_directory(parent_path).map_err(|error| anyhow!(error))?;
        let parent = parent_source.directory_file()?;
        let name = path.file_name().context("path has no basename")?.to_owned();
        let directory = openat_directory_nofollow(&parent, &name)?;
        let identity = directory_identity(&directory.metadata()?)?;
        Ok(Self {
            path: path.to_path_buf(),
            parent_source,
            parent,
            name,
            directory,
            identity,
        })
    }

    fn open_optional(path: &Path) -> Result<Option<Self>> {
        ensure_absolute_normalized(path)?;
        let parent_path = path.parent().context("path has no parent")?;
        let parent_source =
            PinnedLocalRangeSource::open_directory(parent_path).map_err(|error| anyhow!(error))?;
        let parent = parent_source.directory_file()?;
        let name = path.file_name().context("path has no basename")?.to_owned();
        let Some(directory) = try_openat_directory_nofollow(&parent, &name)? else {
            return Ok(None);
        };
        let identity = directory_identity(&directory.metadata()?)?;
        Ok(Some(Self {
            path: path.to_path_buf(),
            parent_source,
            parent,
            name,
            directory,
            identity,
        }))
    }
}

impl ArchiveRootGuard {
    fn acquire(active_source: &Path) -> Result<Self> {
        let archive_root = active_source
            .parent()
            .context("active epoch has no archive root")?;
        let source =
            PinnedLocalRangeSource::open_directory(archive_root).map_err(|error| anyhow!(error))?;
        let directory = source.directory_file()?;
        let identity = directory_identity(&directory.metadata()?)?;
        let mount_id = descriptor_mount_id(&directory)?;
        let lock = FileLock::acquire_at(&directory, ARCHIVE_ROOT_SWITCH_LOCK_FILE)?;
        let guard = Self {
            source,
            directory,
            identity,
            mount_id,
            lock,
        };
        guard.recheck()?;
        Ok(guard)
    }

    fn recheck(&self) -> Result<()> {
        self.source
            .verify_unchanged()
            .map_err(|error| anyhow!(error))?;
        ensure!(
            directory_identity(&self.directory.metadata()?)? == self.identity
                && descriptor_mount_id(&self.directory)? == self.mount_id,
            "locked archive-root descriptor identity changed"
        );
        self.lock
            .recheck_at(&self.directory, ARCHIVE_ROOT_SWITCH_LOCK_FILE)?;
        Ok(())
    }

    fn recheck_generation_paths(&self, prepare: &PrepareReceipt) -> Result<()> {
        self.recheck()?;
        for path in [&prepare.active_source, &prepare.candidate] {
            let anchor = PathAnchor::open(Path::new(path))?;
            ensure!(
                directory_identity(&anchor.parent.metadata()?)? == self.identity
                    && descriptor_mount_id(&anchor.parent)? == self.mount_id
                    && descriptor_mount_id(&anchor.directory)? == self.mount_id,
                "generation path parent differs from the locked archive root"
            );
            ensure_anchor_unchanged(&anchor)?;
        }
        self.recheck()?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn rename_anchor_no_replace(
    source: &PathAnchor,
    target_parent: &File,
    target_name: &OsStr,
) -> Result<()> {
    let source_name = cstring_component(&source.name)?;
    let target_name = cstring_component(target_name)?;
    // SAFETY: the descriptors and components remain live for this one atomic call.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            source.parent.as_raw_fd(),
            source_name.as_ptr(),
            target_parent.as_raw_fd(),
            target_name.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    ensure!(
        result == 0,
        "atomically relocate candidate without replacement: {}",
        io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(target_os = "macos")]
fn rename_anchor_no_replace(
    source: &PathAnchor,
    target_parent: &File,
    target_name: &OsStr,
) -> Result<()> {
    let source_name = cstring_component(&source.name)?;
    let target_name = cstring_component(target_name)?;
    // SAFETY: the descriptors and components remain live for this one atomic call.
    let result = unsafe {
        libc::renameatx_np(
            source.parent.as_raw_fd(),
            source_name.as_ptr(),
            target_parent.as_raw_fd(),
            target_name.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    ensure!(
        result == 0,
        "atomically relocate candidate without replacement: {}",
        io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn exchange_anchors(left: &PathAnchor, right: &PathAnchor) -> Result<()> {
    let left_name = cstring_component(&left.name)?;
    let right_name = cstring_component(&right.name)?;
    // SAFETY: both parent descriptors and names are live. RENAME_EXCHANGE is
    // one atomic namespace operation on one filesystem.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            left.parent.as_raw_fd(),
            left_name.as_ptr(),
            right.parent.as_raw_fd(),
            right_name.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    ensure!(
        result == 0,
        "atomically exchange {} and {}: {}",
        left.path.display(),
        right.path.display(),
        io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(target_os = "macos")]
fn exchange_anchors(left: &PathAnchor, right: &PathAnchor) -> Result<()> {
    let left_path = CString::new(left.path.as_os_str().as_bytes())?;
    let right_path = CString::new(right.path.as_os_str().as_bytes())?;
    // SAFETY: both absolute paths stay live for the atomic RENAME_SWAP call.
    let result =
        unsafe { libc::renamex_np(left_path.as_ptr(), right_path.as_ptr(), libc::RENAME_SWAP) };
    ensure!(
        result == 0,
        "atomic directory exchange failed: {}",
        io::Error::last_os_error()
    );
    Ok(())
}

fn read_absolute_regular_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    ensure_absolute_normalized(path)?;
    let parent = path.parent().context("file path has no parent")?;
    let source = PinnedLocalRangeSource::open_directory(parent).map_err(|error| anyhow!(error))?;
    let name = path
        .file_name()
        .and_then(OsStr::to_str)
        .context("file basename is not UTF-8")?;
    source
        .read_all_bounded(name, limit)
        .map_err(|error| anyhow!(error))
}

fn hash_file(file: &File) -> Result<FileBinding> {
    let bytes = file.metadata()?.len();
    let mut reader = BufReader::with_capacity(HASH_BUFFER_BYTES, file.try_clone()?);
    reader.seek(SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; HASH_BUFFER_BYTES];
    let mut observed = 0u64;
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        observed = observed
            .checked_add(count as u64)
            .context("file length overflow")?;
    }
    ensure!(observed == bytes, "file changed while it was hashed");
    Ok(FileBinding {
        bytes,
        sha256: hex_lower(&hasher.finalize()),
    })
}

fn binding_for_bytes(bytes: &[u8]) -> FileBinding {
    FileBinding {
        bytes: bytes.len() as u64,
        sha256: hex_lower(&Sha256::digest(bytes)),
    }
}

fn file_identity(metadata: &std::fs::Metadata, regular: bool) -> Result<FileIdentity> {
    ensure!(
        if regular {
            metadata.is_file()
        } else {
            metadata.is_dir()
        },
        "filesystem identity has the wrong object type"
    );
    Ok(FileIdentity {
        bytes: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        mode: metadata.mode(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

fn directory_identity(metadata: &std::fs::Metadata) -> Result<DirectoryIdentity> {
    ensure!(metadata.is_dir(), "expected a directory");
    Ok(DirectoryIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn parse_json<T: DeserializeOwned>(bytes: &[u8], label: &str) -> Result<T> {
    serde_json::from_slice(bytes).with_context(|| format!("parse {label}"))
}

fn pretty_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn validate_sha256(value: &str) -> Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "SHA-256 is not 64 lowercase hexadecimal characters"
    );
    Ok(())
}

fn unix_seconds() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn ensure_absolute_normalized(path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute(),
        "path must be absolute: {}",
        path.display()
    );
    for component in path.components() {
        ensure!(
            matches!(component, Component::RootDir | Component::Normal(_)),
            "path must be normalized: {}",
            path.display()
        );
    }
    Ok(())
}

fn path_entry_exists_nofollow(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

#[cfg(target_os = "linux")]
fn error_chain_has_not_found(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.kind() == io::ErrorKind::NotFound)
    })
}

fn cstring_component(name: &OsStr) -> Result<CString> {
    let bytes = name.as_bytes();
    ensure!(
        !bytes.is_empty()
            && bytes != b"."
            && bytes != b".."
            && !bytes.contains(&b'/')
            && !bytes.contains(&0),
        "invalid path component"
    );
    Ok(CString::new(bytes)?)
}

fn openat_directory_nofollow(parent: &File, name: &OsStr) -> Result<File> {
    openat(
        parent,
        name,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        0,
    )
}

fn try_openat_directory_nofollow(parent: &File, name: &OsStr) -> Result<Option<File>> {
    match openat_io(
        parent,
        name,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        0,
    ) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("open directory without following links"),
    }
}

fn openat_regular_nofollow(parent: &File, name: &OsStr, create_lock: bool) -> Result<File> {
    let flags = if create_lock {
        libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK
    } else {
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK
    };
    let file = openat(parent, name, flags, 0o600)?;
    ensure!(file.metadata()?.is_file(), "object is not a regular file");
    Ok(file)
}

fn try_openat_regular_nofollow(parent: &File, name: &OsStr) -> Result<Option<File>> {
    match openat_io(
        parent,
        name,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        0,
    ) {
        Ok(file) => {
            ensure!(file.metadata()?.is_file(), "object is not a regular file");
            Ok(Some(file))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("open regular file without following links"),
    }
}

fn read_optional_regular_at(directory: &File, name: &str, limit: usize) -> Result<Option<Vec<u8>>> {
    let Some(mut file) = try_openat_regular_nofollow(directory, OsStr::new(name))? else {
        return Ok(None);
    };
    let metadata = file.metadata()?;
    ensure!(
        metadata.nlink() == 1,
        "immutable object {name} has multiple links"
    );
    ensure!(
        metadata.len() <= limit as u64,
        "immutable object {name} is too large"
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 == metadata.len(),
        "immutable object {name} changed while read"
    );
    Ok(Some(bytes))
}

fn createat_regular_nofollow(parent: &File, name: &OsStr, mode: libc::mode_t) -> Result<File> {
    openat(
        parent,
        name,
        libc::O_WRONLY
            | libc::O_CREAT
            | libc::O_EXCL
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK,
        mode,
    )
}

fn openat(parent: &File, name: &OsStr, flags: i32, mode: libc::mode_t) -> Result<File> {
    openat_io(parent, name, flags, mode).map_err(Into::into)
}

fn openat_io(parent: &File, name: &OsStr, flags: i32, mode: libc::mode_t) -> io::Result<File> {
    let name = cstring_component(name)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    // SAFETY: parent and name remain live. A successful call returns one owned descriptor.
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags,
            mode as libc::c_uint,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: openat returned a new owned descriptor.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn mkdirat_private(parent: &File, name: &OsStr) -> Result<()> {
    let name = cstring_component(name)?;
    // SAFETY: parent and name remain live for this call.
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    ensure!(
        result == 0,
        "create journal: {}",
        io::Error::last_os_error()
    );
    Ok(())
}

fn sync_parent_of(path: &Path) -> Result<()> {
    let parent = path.parent().context("path has no parent")?;
    PinnedLocalRangeSource::open_directory(parent)
        .map_err(|error| anyhow!(error))?
        .directory_file()?
        .sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::Cell,
        fs::{self, Permissions},
        os::unix::fs::PermissionsExt,
    };

    struct GenerationFixture {
        _root: tempfile::TempDir,
        archive_root: PathBuf,
        active: PathBuf,
        candidate: PathBuf,
        journal: PathBuf,
        prepare: PrepareReceipt,
        freeze: Option<SourceFreezeComplete>,
    }

    impl Drop for GenerationFixture {
        fn drop(&mut self) {
            for path in [&self.active, &self.candidate] {
                if path.exists() {
                    let _ = fs::set_permissions(path, Permissions::from_mode(0o755));
                }
            }
        }
    }

    fn dummy_binding(byte: char) -> FileBinding {
        FileBinding {
            bytes: 1,
            sha256: byte.to_string().repeat(64),
        }
    }

    fn write_mode(path: &Path, bytes: &[u8], mode: u32) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, Permissions::from_mode(mode)).unwrap();
    }

    fn frozen_proof(path: &Path) -> FrozenFileProof {
        let file = File::open(path).unwrap();
        FrozenFileProof {
            identity: file_identity(&file.metadata().unwrap(), true).unwrap(),
            content: hash_file(&file).unwrap(),
        }
    }

    fn source_proof(path: &Path) -> SourceFileBinding {
        let file = File::open(path).unwrap();
        SourceFileBinding {
            identity: file_identity(&file.metadata().unwrap(), true).unwrap(),
            content: hash_file(&file).unwrap(),
            disposition: AuthorityDisposition::CopySidecar,
        }
    }

    fn test_spyx_trusted_local_proof(
        optional_files: &[&str],
        device: u64,
    ) -> (
        SpyxTrustedLocalSourceProof,
        BTreeMap<String, SourceFileBinding>,
    ) {
        let names = REQUIRED_GENERATION_FILES
            .into_iter()
            .chain(SPYX_TRUSTED_LOCAL_REQUIRED_ADDITIONAL_FILES)
            .chain(optional_files.iter().copied())
            .collect::<BTreeSet<_>>();
        let files = names
            .into_iter()
            .enumerate()
            .map(|(index, name)| GenerationFile {
                name: name.to_owned(),
                size: (index + 1) as u64,
                sha256: deployed_spyx_trusted_file_size_binding(
                    ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                    name,
                    (index + 1) as u64,
                ),
            })
            .collect::<Vec<_>>();
        let mut manifest = GenerationManifest {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            cluster_id: SPYX_TRUSTED_LOCAL_CLUSTER_ID.to_owned(),
            epoch: SUPPORTED_EPOCH,
            generation_id: SPYX_TRUSTED_LOCAL_GENERATION_ID.to_owned(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: SPYX_TRUSTED_LOCAL_SLOTS_PER_EPOCH,
            complete: true,
            files,
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        let source_files = manifest
            .files
            .iter()
            .enumerate()
            .map(|(index, file)| {
                (
                    file.name.clone(),
                    SourceFileBinding {
                        identity: FileIdentity {
                            bytes: file.size,
                            device,
                            inode: (index + 1) as u64,
                            mode: 0o100444,
                            modified_seconds: 0,
                            modified_nanoseconds: 0,
                            changed_seconds: 0,
                            changed_nanoseconds: 0,
                        },
                        content: FileBinding {
                            bytes: file.size,
                            sha256: format!("{:064x}", index + 32),
                        },
                        disposition: AuthorityDisposition::CopySidecar,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let proof = SpyxTrustedLocalSourceProof {
            binding_recipe: SPYX_DEPLOYED_TRUSTED_BINDING_RECIPE.to_owned(),
            generation_binding_digest: manifest.generation_digest.clone(),
            registry_binding_sha256: manifest
                .required_file(REGISTRY_FILE)
                .unwrap()
                .sha256
                .clone(),
            trusted_manifest: manifest,
            current_reader_generation_digest: "a".repeat(64),
            current_reader_registry_binding_sha256: "b".repeat(64),
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            metadata_wire_profile: ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
            required_additional_files: SPYX_TRUSTED_LOCAL_REQUIRED_ADDITIONAL_FILES
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
            optional_additional_files: SPYX_TRUSTED_LOCAL_OPTIONAL_ADDITIONAL_FILES
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
        };
        (proof, source_files)
    }

    fn build_generation_fixture(freeze_source: bool) -> GenerationFixture {
        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let archive_root = root_path.join("archive");
        let active = archive_root.join("epoch-900");
        let candidate = archive_root.join(".epoch-900.metadata-normalized-test");
        let journal = root_path.join("journal");
        fs::create_dir(&archive_root).unwrap();
        fs::create_dir(&active).unwrap();
        fs::create_dir(&candidate).unwrap();

        let mut source_files = BTreeMap::new();
        for (name, bytes) in [
            ("source-a.bin", b"source-a".as_slice()),
            ("source-b.bin", b"source-b".as_slice()),
        ] {
            let path = active.join(name);
            write_mode(&path, bytes, 0o644);
            source_files.insert(name.to_owned(), source_proof(&path));
        }

        let target_path = candidate.join("target.bin");
        write_mode(&target_path, b"target", 0o444);
        let target_files = BTreeMap::from([("target.bin".to_owned(), frozen_proof(&target_path))]);
        let publication_lock = candidate.join(ARCHIVE_V2_PUBLICATION_LOCK_FILE);
        write_mode(&publication_lock, b"", 0o600);
        let publication_lock_identity = file_identity(
            &File::open(&publication_lock).unwrap().metadata().unwrap(),
            true,
        )
        .unwrap();

        let source_directory =
            directory_identity(&File::open(&active).unwrap().metadata().unwrap()).unwrap();
        let target_directory =
            directory_identity(&File::open(&candidate).unwrap().metadata().unwrap()).unwrap();
        fs::set_permissions(&candidate, Permissions::from_mode(0o555)).unwrap();
        let target_frozen_directory_identity =
            file_identity(&File::open(&candidate).unwrap().metadata().unwrap(), false).unwrap();

        let prepare = PrepareReceipt {
            schema_version: 1,
            kind: "test-prepare".to_owned(),
            state: "test".to_owned(),
            prepared_unix_seconds: 1,
            epoch: SUPPORTED_EPOCH,
            active_source: active.display().to_string(),
            original_candidate: candidate.display().to_string(),
            candidate: candidate.display().to_string(),
            source_authority_path: root_path.join("authority.json").display().to_string(),
            source_authority: dummy_binding('1'),
            normalization_candidate: dummy_binding('2'),
            normalization_receipt: dummy_binding('3'),
            source_directory,
            target_directory,
            target_frozen_directory_identity,
            cluster_id: "mainnet-beta".to_owned(),
            generation_id: "test-generation".to_owned(),
            target_candidate_digest: "4".repeat(64),
            generation_digest: "5".repeat(64),
            spyx_epoch_900_source_generation_digest: OBSERVED_SPYX_EPOCH_900_SOURCE_DIGEST
                .to_owned(),
            spyx_trusted_local_source: test_spyx_trusted_local_proof(&[], source_directory.device)
                .0,
            generation_manifest: dummy_binding('6'),
            message_marker: NamedFileBinding {
                name: "message-marker".to_owned(),
                bytes: 1,
                sha256: "7".repeat(64),
            },
            metadata_marker: NamedFileBinding {
                name: "metadata-marker".to_owned(),
                bytes: 1,
                sha256: "8".repeat(64),
            },
            source_files,
            ignored_unrelated_source_entries: Vec::new(),
            target_files,
            publication_lock_identity,
            target_frozen: true,
            full_descriptor_publication_audit_completed: true,
        };

        let freeze = if freeze_source {
            for name in prepare.source_files.keys() {
                fs::set_permissions(active.join(name), Permissions::from_mode(0o444)).unwrap();
            }
            fs::set_permissions(&active, Permissions::from_mode(0o555)).unwrap();
            let files = prepare
                .source_files
                .iter()
                .map(|(name, proof)| {
                    (
                        name.clone(),
                        FrozenFileProof {
                            identity: frozen_proof(&active.join(name)).identity,
                            content: proof.content.clone(),
                        },
                    )
                })
                .collect();
            Some(SourceFreezeComplete {
                schema_version: 1,
                kind: "archive-v2-metadata-normalization-source-freeze".to_owned(),
                state: "legacy-source-read-only-and-content-bound".to_owned(),
                completed_unix_seconds: 2,
                epoch: SUPPORTED_EPOCH,
                switch_intent: dummy_binding('9'),
                quiescence_attempt_file: "freeze-attempt.json".to_owned(),
                quiescence_attempt: dummy_binding('a'),
                source_path: active.display().to_string(),
                source_directory,
                frozen_directory_identity: file_identity(
                    &File::open(&active).unwrap().metadata().unwrap(),
                    false,
                )
                .unwrap(),
                files,
            })
        } else {
            None
        };

        GenerationFixture {
            _root: root,
            archive_root,
            active,
            candidate,
            journal,
            prepare,
            freeze,
        }
    }

    fn publish_test_exchange_attempt(
        journal: &Journal,
        operation: &str,
        prepare: &PrepareReceipt,
        parent_intent: &FileBinding,
        source_freeze: &FileBinding,
        receipt_byte: char,
    ) -> (String, FileBinding) {
        let receipt = dummy_binding(receipt_byte);
        let (pre_active, pre_retained, _, _) =
            exchange_attempt_topologies(operation, prepare).unwrap();
        let (quiescence_file, quiescence) = publish_quiescence_attempt(
            journal,
            operation,
            prepare.epoch,
            parent_intent,
            Path::new("/external/test-quiescence.json"),
            &receipt,
            pre_active,
            pre_retained,
        )
        .unwrap();
        publish_exchange_attempt(
            journal,
            operation,
            prepare,
            parent_intent,
            source_freeze,
            &quiescence_file,
            &quiescence,
        )
        .unwrap()
    }

    #[test]
    fn whole_directory_exchange_is_atomic_and_recoverable_by_identity() {
        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let active = root_path.join("epoch-900");
        let candidate = root_path.join("candidate");
        fs::create_dir(&active).unwrap();
        fs::create_dir(&candidate).unwrap();
        fs::write(active.join("source"), b"source").unwrap();
        fs::write(candidate.join("target"), b"target").unwrap();
        let left = PathAnchor::open(&active).unwrap();
        let right = PathAnchor::open(&candidate).unwrap();
        let source = left.identity;
        let target = right.identity;
        exchange_anchors(&left, &right).unwrap();
        assert_eq!(PathAnchor::open(&active).unwrap().identity, target);
        assert_eq!(PathAnchor::open(&candidate).unwrap().identity, source);
        exchange_anchors(
            &PathAnchor::open(&active).unwrap(),
            &PathAnchor::open(&candidate).unwrap(),
        )
        .unwrap();
        assert_eq!(PathAnchor::open(&active).unwrap().identity, source);
        assert_eq!(PathAnchor::open(&candidate).unwrap().identity, target);
    }

    #[test]
    fn journal_write_is_no_replace_and_exact() {
        let root = tempfile::tempdir().unwrap();
        let journal_path = root.path().canonicalize().unwrap().join("journal");
        let journal = Journal::open_or_create(&journal_path).unwrap();
        let binding = journal
            .write_json_no_replace("one.json", &serde_json::json!({"value": 1}))
            .unwrap();
        assert_eq!(
            binding_for_bytes(&fs::read(journal_path.join("one.json")).unwrap()),
            binding
        );
        assert!(journal.write_json_no_replace("one.json", &0).is_err());
    }

    #[test]
    fn unsafe_or_noncanonical_hash_input_is_rejected() {
        assert!(validate_sha256(&"a".repeat(64)).is_ok());
        assert!(validate_sha256(&"A".repeat(64)).is_err());
        assert!(validate_sha256("abc").is_err());
    }

    #[test]
    fn fixed_spyx_controls_reject_caller_actual_and_persisted_drift() {
        let root_path = Path::new(SPYX_OUTPUT_ROOT).join(DUMP_MANIFEST_FILE);
        let checkpoint_path = Path::new(SPYX_OUTPUT_ROOT).join(RESUME_CHECKPOINT_FILE);
        let root = FileBinding {
            bytes: SPYX_FINAL_ROOT_MANIFEST_BYTES,
            sha256: SPYX_FINAL_ROOT_MANIFEST_SHA256.to_owned(),
        };
        let checkpoint = FileBinding {
            bytes: SPYX_FINAL_CHECKPOINT_BYTES,
            sha256: SPYX_FINAL_CHECKPOINT_SHA256.to_owned(),
        };
        validate_fixed_spyx_controls(
            &root_path,
            &root,
            &checkpoint_path,
            &checkpoint,
            SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256,
        )
        .unwrap();

        let mut wrong_root_sha = root.clone();
        wrong_root_sha.sha256 = "f".repeat(64);
        assert!(
            validate_fixed_spyx_controls(
                &root_path,
                &wrong_root_sha,
                &checkpoint_path,
                &checkpoint,
                SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256,
            )
            .is_err()
        );
        let mut wrong_root_size = root.clone();
        wrong_root_size.bytes += 1;
        assert!(
            validate_fixed_spyx_controls(
                &root_path,
                &wrong_root_size,
                &checkpoint_path,
                &checkpoint,
                SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256,
            )
            .is_err()
        );
        let mut wrong_checkpoint = checkpoint.clone();
        wrong_checkpoint.sha256 = "e".repeat(64);
        assert!(
            validate_fixed_spyx_controls(
                &root_path,
                &root,
                &checkpoint_path,
                &wrong_checkpoint,
                SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256,
            )
            .is_err()
        );
        assert!(
            validate_fixed_spyx_controls(
                &Path::new(SPYX_OUTPUT_ROOT).join("replacement-manifest.json"),
                &root,
                &checkpoint_path,
                &checkpoint,
                SPYX_FINAL_CHECKPOINT_PAYLOAD_SHA256,
            )
            .is_err()
        );
        assert!(
            validate_fixed_spyx_controls(
                &root_path,
                &root,
                &checkpoint_path,
                &checkpoint,
                &"d".repeat(64),
            )
            .is_err()
        );
    }

    #[test]
    fn failed_final_mutation_gate_never_exchanges_generations() {
        let fixture = build_generation_fixture(true);
        let freeze = fixture.freeze.as_ref().unwrap();
        let root_guard = ArchiveRootGuard::acquire(&fixture.active).unwrap();
        let before_active = PathAnchor::open(&fixture.active).unwrap().identity;
        let before_candidate = PathAnchor::open(&fixture.candidate).unwrap().identity;
        assert!(
            ensure_selected_topology(&fixture.prepare, freeze, true, &root_guard, || {
                bail!("injected expired receipt or newly opened descriptor")
            })
            .is_err()
        );
        assert_eq!(
            PathAnchor::open(&fixture.active).unwrap().identity,
            before_active
        );
        assert_eq!(
            PathAnchor::open(&fixture.candidate).unwrap().identity,
            before_candidate
        );
    }

    #[test]
    fn partial_source_freeze_is_recoverable_and_idempotent() {
        let mut fixture = build_generation_fixture(false);
        let first = fixture.prepare.source_files.keys().next().unwrap().clone();
        fs::set_permissions(fixture.active.join(first), Permissions::from_mode(0o444)).unwrap();
        let journal = Journal::open_or_create(&fixture.journal).unwrap();
        let intent = dummy_binding('b');
        let callback_count = Cell::new(0);
        let (freeze, binding) = ensure_source_frozen(&journal, &fixture.prepare, &intent, || {
            callback_count.set(callback_count.get() + 1);
            publish_quiescence_attempt(
                &journal,
                "freeze-source",
                SUPPORTED_EPOCH,
                &intent,
                Path::new("/external/freeze.json"),
                &dummy_binding('c'),
                fixture.prepare.source_directory,
                fixture.prepare.target_directory,
            )
        })
        .unwrap();
        assert_eq!(callback_count.get(), 1);
        assert_eq!(freeze.files.len(), 2);
        verify_frozen_source_identities(&File::open(&fixture.active).unwrap(), &freeze).unwrap();
        let (again, again_binding) =
            ensure_source_frozen(&journal, &fixture.prepare, &intent, || {
                panic!("completed freeze retry must not request a new receipt")
            })
            .unwrap();
        assert_eq!(again_binding, binding);
        assert_eq!(
            again.files.keys().collect::<Vec<_>>(),
            freeze.files.keys().collect::<Vec<_>>()
        );
        fixture.freeze = Some(freeze);
    }

    #[test]
    fn cutover_post_exchange_recovery_needs_no_external_gate() {
        let fixture = build_generation_fixture(true);
        let freeze = fixture.freeze.as_ref().unwrap();
        let freeze_binding = binding_for_bytes(&pretty_json_bytes(freeze).unwrap());
        let intent_binding = dummy_binding('d');
        let journal = Journal::open_or_create(&fixture.journal).unwrap();
        let (exchange_file, exchange_binding) = publish_test_exchange_attempt(
            &journal,
            "cutover",
            &fixture.prepare,
            &intent_binding,
            &freeze_binding,
            'e',
        );
        let root_guard = ArchiveRootGuard::acquire(&fixture.active).unwrap();
        journal
            .require_same_mount_as(&root_guard.directory)
            .unwrap();
        ensure_selected_topology(&fixture.prepare, freeze, true, &root_guard, || Ok(())).unwrap();
        let invoked = Cell::new(false);
        ensure_selected_topology(&fixture.prepare, freeze, true, &root_guard, || {
            invoked.set(true);
            bail!("recovery must not exchange")
        })
        .unwrap();
        assert!(!invoked.get());
        let (tail_name, tail, tail_binding) = exchange_attempt_chain_tail(
            &journal,
            "cutover",
            &fixture.prepare,
            &intent_binding,
            &freeze_binding,
        )
        .unwrap()
        .unwrap();
        assert_eq!(tail_name, exchange_file);
        assert_eq!(tail_binding, exchange_binding);
        let complete = publish_switch_completion(
            &journal,
            &fixture.prepare,
            &intent_binding,
            &freeze_binding,
            tail_name,
            &tail,
            tail_binding,
        )
        .unwrap();
        let existing: SwitchComplete = journal
            .read_optional_json(SWITCH_COMPLETE_FILE)
            .unwrap()
            .unwrap();
        assert_eq!(existing.exchange_attempt, complete.exchange_attempt);
    }

    #[test]
    fn rollback_post_exchange_recovery_is_symmetric_and_idempotent() {
        let fixture = build_generation_fixture(true);
        let freeze = fixture.freeze.as_ref().unwrap();
        let freeze_binding = binding_for_bytes(&pretty_json_bytes(freeze).unwrap());
        let cutover_intent = dummy_binding('f');
        let rollback_intent = dummy_binding('0');
        let journal = Journal::open_or_create(&fixture.journal).unwrap();
        publish_test_exchange_attempt(
            &journal,
            "cutover",
            &fixture.prepare,
            &cutover_intent,
            &freeze_binding,
            '1',
        );
        let root_guard = ArchiveRootGuard::acquire(&fixture.active).unwrap();
        ensure_selected_topology(&fixture.prepare, freeze, true, &root_guard, || Ok(())).unwrap();
        let (rollback_file, rollback_binding) = publish_test_exchange_attempt(
            &journal,
            "rollback",
            &fixture.prepare,
            &rollback_intent,
            &freeze_binding,
            '2',
        );
        ensure_selected_topology(&fixture.prepare, freeze, false, &root_guard, || Ok(())).unwrap();
        let invoked = Cell::new(false);
        ensure_selected_topology(&fixture.prepare, freeze, false, &root_guard, || {
            invoked.set(true);
            bail!("rollback recovery must not exchange")
        })
        .unwrap();
        assert!(!invoked.get());
        let (tail_name, tail, tail_binding) = exchange_attempt_chain_tail(
            &journal,
            "rollback",
            &fixture.prepare,
            &rollback_intent,
            &freeze_binding,
        )
        .unwrap()
        .unwrap();
        assert_eq!(tail_name, rollback_file);
        assert_eq!(tail_binding, rollback_binding);
        let complete = publish_rollback_completion(
            &journal,
            &fixture.prepare,
            &rollback_intent,
            tail_name,
            &tail,
            tail_binding,
        )
        .unwrap();
        assert_eq!(
            journal
                .write_or_validate_json(ROLLBACK_COMPLETE_FILE, &complete)
                .unwrap(),
            binding_for_bytes(&pretty_json_bytes(&complete).unwrap())
        );
        let mut wrong_quiescence_name = complete.clone();
        wrong_quiescence_name.quiescence_attempt_file = "false-quiescence.json".to_owned();
        assert!(
            validate_rollback_complete(
                &wrong_quiescence_name,
                &fixture.prepare,
                &rollback_intent,
                &rollback_file,
                &tail,
                &rollback_binding,
            )
            .is_err()
        );
        let mut wrong_exchange_name = complete;
        wrong_exchange_name.exchange_attempt_file = "false-exchange.json".to_owned();
        assert!(
            validate_rollback_complete(
                &wrong_exchange_name,
                &fixture.prepare,
                &rollback_intent,
                &rollback_file,
                &tail,
                &rollback_binding,
            )
            .is_err()
        );
    }

    #[test]
    fn completion_rejects_tampered_attempt_filenames() {
        let fixture = build_generation_fixture(true);
        let freeze = fixture.freeze.as_ref().unwrap();
        let freeze_binding = binding_for_bytes(&pretty_json_bytes(freeze).unwrap());
        let intent = dummy_binding('3');
        let journal = Journal::open_or_create(&fixture.journal).unwrap();
        let (exchange_file, _) = publish_test_exchange_attempt(
            &journal,
            "cutover",
            &fixture.prepare,
            &intent,
            &freeze_binding,
            '4',
        );
        let (exchange, exchange_binding) = read_and_validate_exchange_attempt(
            &journal,
            &exchange_file,
            "cutover",
            &fixture.prepare,
            &intent,
            &freeze_binding,
        )
        .unwrap();
        let root_guard = ArchiveRootGuard::acquire(&fixture.active).unwrap();
        ensure_selected_topology(&fixture.prepare, freeze, true, &root_guard, || Ok(())).unwrap();
        let mut complete = publish_switch_completion(
            &journal,
            &fixture.prepare,
            &intent,
            &freeze_binding,
            exchange_file.clone(),
            &exchange,
            exchange_binding.clone(),
        )
        .unwrap();
        let valid_complete = complete.clone();
        complete.quiescence_attempt_file = "false-name.json".to_owned();
        assert!(
            validate_switch_complete(
                &complete,
                &fixture.prepare,
                &intent,
                &freeze_binding,
                &exchange_file,
                &exchange,
                &exchange_binding,
            )
            .is_err()
        );
        let mut wrong_exchange_name = valid_complete;
        wrong_exchange_name.exchange_attempt_file = "false-exchange.json".to_owned();
        assert!(
            validate_switch_complete(
                &wrong_exchange_name,
                &fixture.prepare,
                &intent,
                &freeze_binding,
                &exchange_file,
                &exchange,
                &exchange_binding,
            )
            .is_err()
        );
    }

    #[test]
    fn exchange_attempts_form_one_retry_safe_chain() {
        let fixture = build_generation_fixture(true);
        let journal = Journal::open_or_create(&fixture.journal).unwrap();
        let intent = dummy_binding('5');
        let freeze = dummy_binding('6');
        let first = publish_test_exchange_attempt(
            &journal,
            "cutover",
            &fixture.prepare,
            &intent,
            &freeze,
            '7',
        );
        let second = publish_test_exchange_attempt(
            &journal,
            "cutover",
            &fixture.prepare,
            &intent,
            &freeze,
            '8',
        );
        let (tail_name, tail, tail_binding) =
            exchange_attempt_chain_tail(&journal, "cutover", &fixture.prepare, &intent, &freeze)
                .unwrap()
                .unwrap();
        assert_eq!(tail_name, second.0);
        assert_eq!(tail_binding, second.1);
        assert_eq!(tail.previous_exchange_attempt, Some(first.1));
    }

    #[test]
    fn candidate_relocation_recovers_after_the_rename() {
        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let archive = root_path.join("archive");
        let active = archive.join("epoch-900");
        let staging = root_path.join("staging");
        let original = staging.join("candidate");
        let selector = archive.join(".epoch-900.metadata-normalized-test");
        fs::create_dir(&archive).unwrap();
        fs::create_dir(&active).unwrap();
        fs::create_dir(&staging).unwrap();
        fs::create_dir(&original).unwrap();
        let journal = Journal::open_or_create(&root_path.join("journal")).unwrap();
        let intent = RelocationIntent {
            schema_version: 1,
            kind: "archive-v2-metadata-normalization-relocation-intent".to_owned(),
            state: "ready-for-no-replace-relocation".to_owned(),
            epoch: SUPPORTED_EPOCH,
            original_candidate: original.display().to_string(),
            selector: selector.display().to_string(),
            target_directory: directory_identity(
                &File::open(&original).unwrap().metadata().unwrap(),
            )
            .unwrap(),
            normalization_candidate: dummy_binding('9'),
            normalization_receipt: dummy_binding('a'),
        };
        relocate_candidate_to_selector(&active, &intent, &journal).unwrap();
        assert!(!original.exists());
        assert!(selector.exists());
        relocate_candidate_to_selector(&active, &intent, &journal).unwrap();
        assert!(selector.exists());
    }

    #[test]
    fn journal_overlap_is_rejected_before_creation() {
        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let source = PathBuf::from(CANONICAL_EPOCH_900_PATH);
        let archive = source.parent().unwrap().to_path_buf();
        let candidate_sha = "b".repeat(64);
        let journal = source.join("journal");
        let args = PrepareArgs {
            source,
            candidate: root_path.join("staging/candidate"),
            selector: archive.join(expected_selector_name(SUPPORTED_EPOCH, &candidate_sha)),
            source_authority: root_path.join("authority/source.json"),
            source_authority_sha256: "c".repeat(64),
            candidate_sha256: candidate_sha,
            receipt_sha256: "d".repeat(64),
            expected_spyx_epoch_900_source_digest: OBSERVED_SPYX_EPOCH_900_SOURCE_DIGEST.to_owned(),
            journal: journal.clone(),
            epoch: SUPPORTED_EPOCH,
        };
        assert!(validate_prepare_paths(&args).is_err());
        assert!(!journal.exists());
    }

    #[test]
    fn canonical_epoch_900_path_is_required_in_args_and_persisted_prepare() {
        validate_canonical_active_source(Path::new(CANONICAL_EPOCH_900_PATH)).unwrap();
        assert!(
            validate_canonical_active_source(Path::new(
                "/volume1/blockzilla/archive/epoch-900-copy"
            ))
            .is_err()
        );
        assert!(
            validate_canonical_active_source(Path::new(
                "/volume1/blockzilla/archive/copied-epoch-900"
            ))
            .is_err()
        );

        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let candidate_sha = "b".repeat(64);
        let args = PrepareArgs {
            source: PathBuf::from("/volume1/blockzilla/archive/epoch-900-copy"),
            candidate: root_path.join("staging/candidate"),
            selector: PathBuf::from("/volume1/blockzilla/archive")
                .join(expected_selector_name(SUPPORTED_EPOCH, &candidate_sha)),
            source_authority: root_path.join("authority/source.json"),
            source_authority_sha256: "c".repeat(64),
            candidate_sha256: candidate_sha,
            receipt_sha256: "d".repeat(64),
            expected_spyx_epoch_900_source_digest: OBSERVED_SPYX_EPOCH_900_SOURCE_DIGEST.to_owned(),
            journal: root_path.join("journal"),
            epoch: SUPPORTED_EPOCH,
        };
        let error = validate_prepare_paths(&args).unwrap_err();
        assert!(error.to_string().contains("canonical epoch-900"));

        let fixture = build_generation_fixture(false);
        let mut persisted = fixture.prepare.clone();
        persisted.kind = "archive-v2-metadata-normalization-publication-prepare".to_owned();
        persisted.state = "audited-frozen-unselected-generation".to_owned();
        persisted.active_source = "/volume1/blockzilla/archive/epoch-900-copy".to_owned();
        let error = validate_prepare_receipt(&persisted).unwrap_err();
        assert!(error.to_string().contains("canonical epoch-900"));
    }

    #[test]
    fn trusted_local_source_proof_rejects_every_identity_domain_change() {
        let (proof, source_files) =
            test_spyx_trusted_local_proof(&SPYX_TRUSTED_LOCAL_OPTIONAL_ADDITIONAL_FILES, 1);
        let expected_digest = proof.generation_binding_digest.clone();
        validate_spyx_trusted_local_source_proof(&proof, &source_files, &expected_digest).unwrap();

        let mut changed_size_bindings = source_files.clone();
        changed_size_bindings
            .get_mut(BLOCKS_FILE)
            .unwrap()
            .content
            .bytes += 1;
        assert!(
            validate_spyx_trusted_local_source_proof(
                &proof,
                &changed_size_bindings,
                &expected_digest,
            )
            .is_err()
        );

        let mut missing_required = proof.clone();
        missing_required
            .trusted_manifest
            .files
            .retain(|file| file.name != SIGNATURES_FILE);
        missing_required.trusted_manifest.generation_digest =
            compute_generation_digest(&missing_required.trusted_manifest).unwrap();
        missing_required.generation_binding_digest =
            missing_required.trusted_manifest.generation_digest.clone();
        assert!(
            validate_spyx_trusted_local_source_proof(
                &missing_required,
                &source_files,
                &missing_required.generation_binding_digest,
            )
            .is_err()
        );

        let mut changed_optional_presence = proof.clone();
        changed_optional_presence
            .trusted_manifest
            .files
            .retain(|file| file.name != ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
        changed_optional_presence.trusted_manifest.generation_digest =
            compute_generation_digest(&changed_optional_presence.trusted_manifest).unwrap();
        changed_optional_presence.generation_binding_digest = changed_optional_presence
            .trusted_manifest
            .generation_digest
            .clone();
        assert!(
            validate_spyx_trusted_local_source_proof(
                &changed_optional_presence,
                &source_files,
                &expected_digest,
            )
            .is_err()
        );

        let mut changed_cluster = proof.clone();
        changed_cluster.trusted_manifest.cluster_id = "another-cluster".to_owned();
        changed_cluster.trusted_manifest.generation_digest =
            compute_generation_digest(&changed_cluster.trusted_manifest).unwrap();
        changed_cluster.generation_binding_digest =
            changed_cluster.trusted_manifest.generation_digest.clone();
        assert!(
            validate_spyx_trusted_local_source_proof(
                &changed_cluster,
                &source_files,
                &changed_cluster.generation_binding_digest,
            )
            .is_err()
        );

        let mut changed_generation = proof.clone();
        changed_generation.trusted_manifest.generation_id = "another-generation".to_owned();
        changed_generation.trusted_manifest.generation_digest =
            compute_generation_digest(&changed_generation.trusted_manifest).unwrap();
        changed_generation.generation_binding_digest = changed_generation
            .trusted_manifest
            .generation_digest
            .clone();
        assert!(
            validate_spyx_trusted_local_source_proof(
                &changed_generation,
                &source_files,
                &changed_generation.generation_binding_digest,
            )
            .is_err()
        );

        let mut changed_profile = proof.clone();
        changed_profile.wire_profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        assert!(
            validate_spyx_trusted_local_source_proof(
                &changed_profile,
                &source_files,
                &expected_digest,
            )
            .is_err()
        );
        assert!(
            validate_spyx_trusted_local_source_proof(&proof, &source_files, &"f".repeat(64))
                .is_err()
        );
    }

    #[test]
    fn deployed_spyx_trusted_local_recipe_matches_the_exact_epoch_900_golden() {
        let sizes = [
            (BLOCK_INDEX_FILE, 22_456_652),
            (BLOCKS_FILE, 59_928_994_141),
            (META_FILE, 66),
            (REGISTRY_FILE, 889_551_808),
            (SIGNATURES_FILE, 32_380_385_536),
            (REGISTRY_INDEX_FILE, 341_082_690),
            (ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, 13_819_456),
            (ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, 28_070_770),
        ];
        let current_size_binding = |name: &str, size: u64| {
            let mut hasher = Sha256::new();
            hasher.update(SPYX_DEPLOYED_TRUSTED_FILE_SIZE_BINDING_DOMAIN);
            hasher.update(ArchiveV2WireProfile::POST_UNKNOWN_NAME.as_bytes());
            hasher.update([0]);
            hasher.update(ArchiveV2MetadataWireProfile::HISTORICAL_COMPATIBILITY_NAME.as_bytes());
            hasher.update([0]);
            hasher.update((name.len() as u64).to_le_bytes());
            hasher.update(name.as_bytes());
            hasher.update(size.to_le_bytes());
            hex_lower(&hasher.finalize())
        };
        let mut current_manifest = GenerationManifest {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            cluster_id: SPYX_TRUSTED_LOCAL_CLUSTER_ID.to_owned(),
            epoch: SUPPORTED_EPOCH,
            generation_id: SPYX_TRUSTED_LOCAL_GENERATION_ID.to_owned(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: SPYX_TRUSTED_LOCAL_SLOTS_PER_EPOCH,
            complete: true,
            files: sizes
                .into_iter()
                .map(|(name, size)| GenerationFile {
                    name: name.to_owned(),
                    size,
                    sha256: current_size_binding(name, size),
                })
                .collect(),
        };
        current_manifest.generation_digest = compute_generation_digest(&current_manifest).unwrap();
        assert_eq!(
            current_manifest.generation_digest,
            "af6a38b3f16a1ca16f1fc251f027d407f1b181b8c82f6ca20bc8928b5cde1c6d"
        );

        let deployed = synthesize_deployed_spyx_trusted_manifest(&current_manifest).unwrap();
        assert_eq!(
            deployed.generation_digest,
            OBSERVED_SPYX_EPOCH_900_SOURCE_DIGEST
        );
        assert_eq!(
            deployed.required_file(REGISTRY_FILE).unwrap().sha256,
            "be67692b687ceaea79cef730952b87964e4a99c9e6f6f9abcc365d55f6620c33"
        );
        assert_ne!(
            deployed.generation_digest,
            current_manifest.generation_digest
        );
    }

    #[test]
    fn journal_parent_replacement_is_detected() {
        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let parent = root_path.join("state");
        let old_parent = root_path.join("state-old");
        let journal_path = parent.join("journal");
        fs::create_dir(&parent).unwrap();
        let journal = Journal::open_or_create(&journal_path).unwrap();
        fs::rename(&parent, &old_parent).unwrap();
        fs::create_dir(&parent).unwrap();
        fs::create_dir(&journal_path).unwrap();
        assert!(journal.recheck_path().is_err());
    }

    #[test]
    fn immutable_publication_ignores_a_private_crash_residue() {
        let root = tempfile::tempdir().unwrap();
        let directory = File::open(root.path()).unwrap();
        fs::write(root.path().join(".control.json.tmp.crash"), b"partial").unwrap();
        publish_immutable_at(&directory, "control.json", b"complete").unwrap();
        publish_immutable_at(&directory, "control.json", b"complete").unwrap();
        assert_eq!(
            fs::read(root.path().join("control.json")).unwrap(),
            b"complete"
        );
        assert!(publish_immutable_at(&directory, "control.json", b"different").is_err());
    }

    #[test]
    fn frozen_generation_drift_is_rejected_before_exchange() {
        let fixture = build_generation_fixture(true);
        let freeze = fixture.freeze.as_ref().unwrap();
        verify_prepared_topology(&fixture.prepare, true, Some(freeze)).unwrap();
        fs::set_permissions(&fixture.candidate, Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(
            fixture.candidate.join("target.bin"),
            Permissions::from_mode(0o644),
        )
        .unwrap();
        fs::write(fixture.candidate.join("target.bin"), b"changed").unwrap();
        fs::set_permissions(
            fixture.candidate.join("target.bin"),
            Permissions::from_mode(0o444),
        )
        .unwrap();
        fs::set_permissions(&fixture.candidate, Permissions::from_mode(0o555)).unwrap();
        assert!(verify_prepared_topology(&fixture.prepare, true, Some(freeze)).is_err());
    }

    fn exact_spyx_snapshot() -> SpyxOutputSnapshot {
        let identity = FileIdentity {
            bytes: 0,
            device: 1,
            inode: 1,
            mode: 0o100444,
            modified_seconds: 0,
            modified_nanoseconds: 0,
            changed_seconds: 0,
            changed_nanoseconds: 0,
        };
        let proof = FrozenFileProof {
            identity,
            content: dummy_binding('e'),
        };
        let mut snapshot = SpyxOutputSnapshot {
            directories: BTreeMap::from([
                (String::new(), identity),
                (DISCOVERY_SHARDS_DIR.to_owned(), identity),
                (EPOCH_SHARDS_DIR.to_owned(), identity),
            ]),
            files: BTreeMap::from([
                (ACCOUNTS_FILE.to_owned(), proof.clone()),
                (DUMP_MANIFEST_FILE.to_owned(), proof.clone()),
                (RESUME_CHECKPOINT_FILE.to_owned(), proof.clone()),
            ]),
        };
        for epoch in SPYX_FIRST_EPOCH..=SPYX_LAST_EPOCH {
            snapshot
                .directories
                .insert(format!("{DISCOVERY_SHARDS_DIR}/epoch-{epoch}"), identity);
            snapshot
                .directories
                .insert(format!("{EPOCH_SHARDS_DIR}/epoch-{epoch}"), identity);
            snapshot.files.insert(
                format!("{DISCOVERY_SHARDS_DIR}/epoch-{epoch}/{CREATIONS_FILE}"),
                proof.clone(),
            );
            for name in [DUMP_MANIFEST_FILE, TRANSACTIONS_FILE, ACCOUNT_ID_LOG_FILE] {
                snapshot.files.insert(
                    format!("{EPOCH_SHARDS_DIR}/epoch-{epoch}/{name}"),
                    proof.clone(),
                );
            }
        }
        for name in SPYX_DISCOVERY_QUARANTINES {
            snapshot
                .directories
                .insert(format!("{DISCOVERY_SHARDS_DIR}/{name}"), identity);
        }
        for name in SPYX_RAW_QUARANTINES {
            snapshot
                .directories
                .insert(format!("{EPOCH_SHARDS_DIR}/{name}"), identity);
        }
        snapshot
    }

    #[test]
    fn spyx_snapshot_requires_numeric_801_to_1018_and_only_fixed_quarantines() {
        let snapshot = exact_spyx_snapshot();
        validate_spyx_snapshot_path_contract(&snapshot).unwrap();
        let mut missing = snapshot.clone();
        missing
            .directories
            .remove(&format!("{DISCOVERY_SHARDS_DIR}/epoch-1000"));
        assert!(validate_spyx_snapshot_path_contract(&missing).is_err());
        let mut pending = snapshot.clone();
        pending.files.insert(
            "resume-checkpoint.pending.json".to_owned(),
            pending.files[ACCOUNTS_FILE].clone(),
        );
        assert!(validate_spyx_snapshot_path_contract(&pending).is_err());
        let mut extra_quarantine = snapshot;
        extra_quarantine.directories.insert(
            format!("{DISCOVERY_SHARDS_DIR}/.abandoned-epoch-999-partial-0"),
            extra_quarantine.directories[""],
        );
        assert!(validate_spyx_snapshot_path_contract(&extra_quarantine).is_err());
    }

    #[test]
    fn stale_future_and_wrong_boot_quiescence_receipts_are_rejected() {
        let fixture = build_generation_fixture(true);
        let prepare_binding = dummy_binding('f');
        let switch_intent = dummy_binding('1');
        let source_freeze = dummy_binding('2');
        let switch_complete = dummy_binding('3');
        let now = unix_seconds().unwrap();
        let mut receipt = ReaderQuiescenceReceipt {
            schema_version: 1,
            kind: "archive-v2-metadata-normalization-reader-quiescence".to_owned(),
            state: "all-readers-and-writers-stopped".to_owned(),
            operation: "rollback".to_owned(),
            issued_unix_seconds: now,
            current_host_boot_id: current_host_boot_id().unwrap(),
            epoch: SUPPORTED_EPOCH,
            active_path: fixture.prepare.active_source.clone(),
            retained_path: fixture.prepare.candidate.clone(),
            active_directory: fixture.prepare.target_directory,
            retained_directory: fixture.prepare.source_directory,
            prepare_receipt_sha256: prepare_binding.sha256.clone(),
            switch_intent_sha256: Some(switch_intent.sha256.clone()),
            source_freeze_sha256: Some(source_freeze.sha256.clone()),
            switch_complete_sha256: Some(switch_complete.sha256.clone()),
            spyx_root_manifest_sha256: None,
            spyx_resume_checkpoint_sha256: None,
            spyx_epoch_900_source_digest: None,
            spyx_process_pid: None,
            spyx_process_start_ticks: None,
            spyx_process_boot_id: None,
            spyx_extractor_executable_path: None,
            spyx_extractor_executable_sha256: None,
            spyx_output_root: None,
            gateway_stopped: true,
            scheduler_stopped: true,
            archive_readers_stopped: true,
            archive_writers_stopped: true,
            extractor_stopped: true,
        };
        let receipt_path = fixture
            .archive_root
            .parent()
            .unwrap()
            .join("quiescence.json");
        let validate = |receipt: &ReaderQuiescenceReceipt| {
            let bytes = serde_json::to_vec(receipt).unwrap();
            fs::write(&receipt_path, &bytes).unwrap();
            validate_reader_quiescence(
                &receipt_path,
                &binding_for_bytes(&bytes).sha256,
                "rollback",
                &fixture.prepare,
                &prepare_binding,
                1,
                fixture.prepare.target_directory,
                fixture.prepare.source_directory,
                Some(&switch_intent),
                Some(&source_freeze),
                Some(&switch_complete),
                None,
                None,
                None,
                None,
            )
        };
        validate(&receipt).unwrap();
        receipt.issued_unix_seconds = now - QUIESCENCE_FRESHNESS_SECONDS - 1;
        assert!(validate(&receipt).is_err());
        receipt.issued_unix_seconds = now + 1;
        assert!(validate(&receipt).is_err());
        receipt.issued_unix_seconds = now;
        receipt.current_host_boot_id = "wrong-boot".to_owned();
        assert!(validate(&receipt).is_err());
    }

    #[test]
    fn captured_process_authority_file_is_exact_and_strict() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../docs/operations/spyx-live-process-authority-20260828T190833CEST.json");
        let bytes = fs::read(path).unwrap();
        assert_eq!(
            binding_for_bytes(&bytes).sha256,
            SPYX_PROCESS_AUTHORITY_SHA256
        );
        assert_eq!(bytes.len() as u64, SPYX_PROCESS_AUTHORITY_BYTES);
        let authority: SpyxProcessAuthority = parse_json(&bytes, "test authority").unwrap();
        assert!(authority.run.single_read_match_hints);
        assert_eq!(authority.process.pid, OBSERVED_SPYX_PID);
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_owned(), serde_json::json!(true));
        assert!(
            parse_json::<SpyxProcessAuthority>(
                &serde_json::to_vec(&value).unwrap(),
                "tampered authority",
            )
            .is_err()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn old_process_tuple_is_stopped_after_a_boot_change() {
        let current = current_host_boot_id().unwrap();
        let different = if current == "not-current" {
            "another-boot"
        } else {
            "not-current"
        };
        ensure_extractor_process_stopped(std::process::id(), 0, different).unwrap();
    }
}
