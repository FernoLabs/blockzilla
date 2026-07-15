//! Minimal, crash-recoverable Yellowstone block recorder.
//!
//! This path deliberately performs no archive conversion and writes no derived sidecars. Each
//! protobuf update is encoded once, compressed as an independent zstd frame, and crossed through
//! the durable ingress-spool boundary before its small handoff journal is advanced.

use std::{
    collections::{HashMap, VecDeque},
    ffi::OsString,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::{
    fd::AsRawFd,
    unix::fs::{MetadataExt, OpenOptionsExt},
};

use anyhow::{Context, Result, anyhow, ensure};
use futures::{SinkExt, StreamExt, channel::mpsc};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
    time::{Instant as TokioInstant, sleep_until},
};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeRequestPing,
    SubscribeUpdate, SubscribeUpdateBlock, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::{
    Code, Status, codec::CompressionEncoding, metadata::MetadataMap, transport::Server,
};

use crate::{
    epoch::{EpochSlot, OLD_FAITHFUL_SLOTS_PER_EPOCH},
    grpc::{
        GrpcRawArchiveWriteReport, GrpcRawArchiveWriter,
        connect_grpc_with_max_decoding_message_size, inspect_capture,
    },
    grpc_relay::{YellowstoneBlockRelay, YellowstoneBlockRelayLimits, YellowstoneRelayAuth},
    ingest::{
        CommitmentEvidence, ContentDigest, DurableCumulativeAck, DurableReplicationWitness,
        DurableSpoolRecord, IngressRecordMeta, LockedSpoolAudit, LogicalKey, ObservationId,
        REPLICATION_PROTOCOL_VERSION, RawReplicationRecord, ReplicationOffer, ReplicationStreamId,
        SpoolJournalIdentity, SpoolLocation, SpoolOptions, SpoolRecord, SpoolWriter,
        read_spool_record,
    },
    layout::ProducerLayout,
};

const IDENTITY_SCHEMA_VERSION: u32 = 1;
const JOURNAL_SCHEMA_VERSION: u32 = 1;
const RESUME_COVERAGE_WARNING_SCHEMA_VERSION: u32 = 1;
/// The ingress payload is one independently compressed zstd frame containing the full known-schema
/// `SubscribeUpdate` envelope delivered by tonic. This retains filters and created-at metadata in
/// addition to the block variant. Prost cannot retain protobuf fields unknown to this build.
const PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1: u16 = 2;
const IDENTITY_FILE: &str = "identity.json";
const HANDOFF_JOURNAL_FILE: &str = "raw-blocks.jsonl";
const WAL_ROOT_DIR: &str = "wal";
const MONITORING_DIR: &str = ".monitoring";
const RESUME_COVERAGE_WARNING_FILE: &str = "resume-coverage-warning.json";
const MATERIALIZATION_RECEIPT_SCHEMA_VERSION: u32 = 1;
const MATERIALIZATION_RECEIPT_FILE: &str = "RAW-MATERIALIZATION-COMPLETE.v1.json";
const MATERIALIZATION_PROGRESS_FILE: &str = "progress.json";
const SUBSCRIBE_REQUEST_CHANNEL_CAPACITY: usize = 8;
const SUBSCRIBE_PING_ID: i32 = 1;
/// Covers two frame metadata envelopes, two handoff rows, and segment headers when sizing a
/// generation for a max-size seed plus one max-size live append. Identity bytes are added exactly.
const GENERATION_ROLLOVER_SAFETY_BYTES: u64 = 1024 * 1024;
const RELAY_X_TOKEN_MAX_BYTES: u64 = 4096;
const RELAY_SHUTDOWN_GRACE_SECS: u64 = 5;
const MAX_HANDOFF_RECORD_BYTES: u64 = 64 * 1024;
const MAX_IDENTITY_FILE_BYTES: u64 = 64 * 1024;
const MAX_REPLICATION_GENERATION_SCAN: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchdogOutcome<T> {
    Completed(T),
    TotalTimeout,
    IdleTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskAdmittedOutcome<T> {
    Admitted(WatchdogOutcome<T>),
    LowDisk(u64),
}

async fn await_with_watchdogs<F>(
    future: F,
    total_deadline: TokioInstant,
    idle_deadline: Option<TokioInstant>,
) -> WatchdogOutcome<F::Output>
where
    F: Future,
{
    tokio::pin!(future);
    let idle_wait = async {
        match idle_deadline {
            Some(deadline) => sleep_until(deadline).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        output = &mut future => WatchdogOutcome::Completed(output),
        _ = sleep_until(total_deadline) => WatchdogOutcome::TotalTimeout,
        _ = idle_wait => WatchdogOutcome::IdleTimeout,
    }
}

async fn next_with_disk_admission<S, C>(
    stream: &mut S,
    check_disk: C,
    total_deadline: TokioInstant,
    idle_deadline: Option<TokioInstant>,
) -> Result<DiskAdmittedOutcome<Option<S::Item>>>
where
    S: futures::Stream + Unpin,
    C: FnOnce() -> Result<Option<u64>>,
{
    if let Some(available) = check_disk()? {
        return Ok(DiskAdmittedOutcome::LowDisk(available));
    }
    Ok(DiskAdmittedOutcome::Admitted(
        await_with_watchdogs(stream.next(), total_deadline, idle_deadline).await,
    ))
}

fn deadline_after(
    now: TokioInstant,
    seconds: u64,
    description: &'static str,
) -> Result<TokioInstant> {
    now.checked_add(Duration::from_secs(seconds))
        .with_context(|| format!("{description} is too large"))
}

fn idle_deadline_after(now: TokioInstant, seconds: u64) -> Result<Option<TokioInstant>> {
    (seconds > 0)
        .then(|| deadline_after(now, seconds, "raw gRPC idle timeout"))
        .transpose()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GrpcResponseEncoding {
    Identity,
    Gzip,
    Zstd,
    Other,
}

impl GrpcResponseEncoding {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
            Self::Other => "other",
        }
    }
}

fn grpc_response_encoding(metadata: &MetadataMap) -> GrpcResponseEncoding {
    let Some(value) = metadata.get("grpc-encoding") else {
        return GrpcResponseEncoding::Identity;
    };
    let Ok(value) = value.to_str() else {
        return GrpcResponseEncoding::Other;
    };
    if value.eq_ignore_ascii_case("gzip") {
        GrpcResponseEncoding::Gzip
    } else if value.eq_ignore_ascii_case("zstd") {
        GrpcResponseEncoding::Zstd
    } else if value.eq_ignore_ascii_case("identity") {
        GrpcResponseEncoding::Identity
    } else {
        // Do not copy an arbitrary server-provided metadata value into logs.
        GrpcResponseEncoding::Other
    }
}

fn subscribe_ping_request() -> SubscribeRequest {
    SubscribeRequest {
        ping: Some(SubscribeRequestPing {
            id: SUBSCRIBE_PING_ID,
        }),
        ..Default::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscribePingReplyOutcome {
    Sent,
    RequestSideClosed,
    AlreadyClosed,
    TotalTimeout,
    IdleTimeout,
}

async fn reply_to_subscription_ping(
    request_side_open: &mut bool,
    request_sink: &mut mpsc::Sender<SubscribeRequest>,
    total_deadline: TokioInstant,
    idle_deadline: Option<TokioInstant>,
) -> SubscribePingReplyOutcome {
    if !*request_side_open {
        return SubscribePingReplyOutcome::AlreadyClosed;
    }
    match await_with_watchdogs(
        request_sink.send(subscribe_ping_request()),
        total_deadline,
        idle_deadline,
    )
    .await
    {
        WatchdogOutcome::Completed(Ok(())) => SubscribePingReplyOutcome::Sent,
        // A futures bounded mpsc SendError has no recoverable variants: it means the receiver was
        // dropped. Yellowstone may still have a terminal Status queued on the response stream, so
        // make the closure sticky and let the caller continue draining that stream.
        WatchdogOutcome::Completed(Err(_)) => {
            *request_side_open = false;
            SubscribePingReplyOutcome::RequestSideClosed
        }
        WatchdogOutcome::TotalTimeout => SubscribePingReplyOutcome::TotalTimeout,
        WatchdogOutcome::IdleTimeout => SubscribePingReplyOutcome::IdleTimeout,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcRawRecordConfig {
    pub endpoint: String,
    pub output_dir: PathBuf,
    pub max_blocks: usize,
    pub timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub from_slot: Option<u64>,
    /// Never subscribe below this slot, even when the durable journal tail is older.
    #[serde(default)]
    pub min_resume_slot: Option<u64>,
    pub resume_coverage_warning_file: Option<PathBuf>,
    pub slots_per_epoch: u64,
    pub stop_at_epoch_boundary: bool,
    pub compression_level: i32,
    pub segment_target_bytes: u64,
    pub max_record_bytes: u64,
    /// Maximum logical bytes occupied by this self-contained generation. Zero disables the cap.
    #[serde(default)]
    pub max_generation_bytes: u64,
    /// Optional supervisor-owned cache root for in-process generation rollover. When set, the
    /// recorder keeps the Yellowstone subscription open while atomically replacing
    /// `<root>/active` and publishing the stopped generation below `<root>/sealed`.
    #[serde(default)]
    pub hot_generation_root: Option<PathBuf>,
    pub min_free_bytes: u64,
    pub require_complete_poh: bool,
    pub cluster_id: String,
    pub origin_node_id: String,
    pub source_id: String,
    /// Optional internal Yellowstone relay listener. This and `relay_x_token_file` must be set
    /// together; the upstream token is deliberately not a fallback.
    #[serde(default)]
    pub relay_bind: Option<SocketAddr>,
    /// File containing the dedicated downstream `x-token`.
    #[serde(default)]
    pub relay_x_token_file: Option<PathBuf>,
    #[serde(default = "default_relay_max_records")]
    pub relay_max_records: usize,
    #[serde(default = "default_relay_max_encoded_bytes")]
    pub relay_max_encoded_bytes: usize,
    #[serde(default = "default_relay_max_clients")]
    pub relay_max_clients: usize,
}

const fn default_relay_max_records() -> usize {
    128
}

const fn default_relay_max_encoded_bytes() -> usize {
    128 * 1024 * 1024
}

const fn default_relay_max_clients() -> usize {
    4
}

impl Default for GrpcRawRecordConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            output_dir: PathBuf::from("blockzilla-grpc-raw"),
            max_blocks: 1_000_000,
            timeout_secs: 86_400,
            idle_timeout_secs: 180,
            from_slot: None,
            min_resume_slot: None,
            resume_coverage_warning_file: None,
            slots_per_epoch: OLD_FAITHFUL_SLOTS_PER_EPOCH,
            stop_at_epoch_boundary: false,
            compression_level: 1,
            segment_target_bytes: 256 * 1024 * 1024,
            max_record_bytes: 128 * 1024 * 1024,
            max_generation_bytes: 0,
            hot_generation_root: None,
            min_free_bytes: 16 * 1024 * 1024 * 1024,
            require_complete_poh: false,
            cluster_id: "solana-mainnet".to_string(),
            origin_node_id: "mac-bridge".to_string(),
            source_id: "grpc-raw".to_string(),
            relay_bind: None,
            relay_x_token_file: None,
            relay_max_records: default_relay_max_records(),
            relay_max_encoded_bytes: default_relay_max_encoded_bytes(),
            relay_max_clients: default_relay_max_clients(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrpcRawRecordOutcome {
    EpochBoundary,
    #[default]
    Retryable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrpcRawRecordRetryReason {
    ConnectError,
    SubscribeError,
    TotalTimeout,
    IdleTimeout,
    StreamEof,
    StreamError,
    PermissionDenied,
    Unauthenticated,
    MaxBlocks,
    ReplayUnavailable,
    GenerationFull,
    LowDisk,
    ResumeCoverageWarningPublicationFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcRawRecordReport {
    pub endpoint: String,
    pub output_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub journal_path: PathBuf,
    pub existing_frames: u64,
    pub frames_seen: u64,
    pub frames_written: u64,
    pub frames_skipped_before_resume: u64,
    pub recovered_handoff_record: bool,
    pub requested_from_slot: Option<u64>,
    #[serde(default)]
    pub minimum_resume_slot: Option<u64>,
    pub effective_from_slot: Option<u64>,
    pub resume_overlap_slot: Option<u64>,
    pub resume_overlap_observed: Option<bool>,
    pub first_delivered_slot: Option<u64>,
    pub resume_coverage_warning: bool,
    #[serde(default)]
    pub resume_coverage_warning_publication_failed: bool,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub first_epoch: Option<u64>,
    pub last_epoch: Option<u64>,
    pub raw_bytes_written: u64,
    pub compressed_bytes_written: u64,
    pub compression_ratio: f64,
    pub complete_poh_required: bool,
    pub poh_blocks_verified: u64,
    pub poh_entries_verified: u64,
    pub poh_transactions_verified: u64,
    pub poh_num_hashes_verified: String,
    pub elapsed_ms: u128,
    pub timed_out: bool,
    pub idle_timed_out: bool,
    pub stream_ended: bool,
    pub stopped_at_epoch_boundary: bool,
    #[serde(default)]
    pub outcome: GrpcRawRecordOutcome,
    #[serde(default)]
    pub retry_reason: Option<GrpcRawRecordRetryReason>,
    #[serde(default)]
    pub action_required: bool,
    /// The provider no longer retains the exact requested slot. This is a clean supervisor
    /// handoff, not proof that source coverage is complete.
    #[serde(default)]
    pub replay_unavailable: bool,
    #[serde(default)]
    pub replay_unavailable_requested_slot: Option<u64>,
    #[serde(default)]
    pub replay_available_slot: Option<u64>,
    #[serde(default)]
    pub stopped_generation_full: bool,
    /// Generations published without dropping the current Yellowstone subscription.
    #[serde(default)]
    pub generations_rotated: u64,
    pub stopped_low_disk: bool,
    pub available_bytes_at_stop: Option<u64>,
    #[serde(default)]
    pub generation_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcRawSeedReport {
    pub source_dir: PathBuf,
    pub target_dir: PathBuf,
    pub source_records_verified: u64,
    pub seeded_slot: u64,
    pub seeded_blockhash: String,
    pub compressed_bytes_copied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrpcRawResumeCoverageWarning {
    event_id: String,
    schema_version: u32,
    requested_overlap_slot: u64,
    first_delivered_slot: u64,
    observed_later_slot: u64,
    written_unix_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResumeCoverageWarningPublishOutcome {
    Published,
    AlreadyPresent,
    DifferentPending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcRawInspectReport {
    pub output_dir: PathBuf,
    pub journal_path: PathBuf,
    pub wal_dir: PathBuf,
    pub records: u64,
    pub first_frame_id: Option<u64>,
    pub last_frame_id: Option<u64>,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
    pub compression_ratio: f64,
    pub payloads_verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcRawPohVerifyReport {
    pub output_dir: PathBuf,
    pub minimum_records: u64,
    pub records_verified: u64,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub poh_entries: u64,
    pub transaction_references: u64,
    pub num_hashes: String,
    pub wal_incomplete_tail_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcRawMaterializeConfig {
    pub input_dir: PathBuf,
    pub archive_dir: PathBuf,
    pub epoch: u64,
    pub max_record_bytes: u64,
    pub pubkey_hot_registry_path: Option<PathBuf>,
    pub pubkey_hot_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrpcRawMaterializedArtifact {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

/// Stable proof tying one published capture to an immutable raw-WAL snapshot.
///
/// There is deliberately no timestamp, process id, or destination path in this structure. Running
/// the materializer twice against the same stopped spool and epoch produces the same receipt even
/// when the destination paths differ.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrpcRawMaterializationReceipt {
    pub schema_version: u32,
    pub source_identity_sha256: String,
    pub source_handoff_journal_sha256: String,
    pub source_frames: u64,
    pub source_tail_frame_id: Option<u64>,
    pub source_tail_slot: Option<u64>,
    pub source_wal_incomplete_tail_bytes: u64,
    pub epoch: u64,
    pub first_source_frame_id: u64,
    pub last_source_frame_id: u64,
    pub first_slot: u64,
    pub last_slot: u64,
    pub blocks_written: u64,
    pub transactions_written: u64,
    pub entries_written: u64,
    pub normalized_block_bytes: u64,
    pub pubkey_run_files: usize,
    pub pubkey_run_records: u64,
    pub pubkey_hot_keys: usize,
    pub pubkey_hot_records: usize,
    pub artifacts: Vec<GrpcRawMaterializedArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcRawMaterializeReport {
    pub input_dir: PathBuf,
    pub archive_dir: PathBuf,
    pub receipt_path: PathBuf,
    pub receipt: GrpcRawMaterializationReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompletePohBlockStats {
    entries: u64,
    transaction_references: u64,
    num_hashes: u128,
}

fn effective_resume_slot(
    configured_from_slot: Option<u64>,
    last_durable_slot: Option<u64>,
    minimum_resume_slot: Option<u64>,
) -> Option<u64> {
    last_durable_slot
        .or(configured_from_slot)
        .into_iter()
        .chain(minimum_resume_slot)
        .max()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplayUnavailable {
    requested_slot: u64,
    available_slot: u64,
}

/// Parse only the provider's complete, known replay-window message. In particular, do not mine
/// arbitrary status text for numbers: a supervisor may use the result to skip an unavailable
/// source range deliberately.
fn parse_replay_unavailable_message(message: &str) -> Option<ReplayUnavailable> {
    const PREFIX: &str = "broadcast from ";
    const SEPARATOR: &str = " is not available, last available: ";

    let remainder = message.strip_prefix(PREFIX)?;
    let (requested, available) = remainder.split_once(SEPARATOR)?;
    if requested.is_empty()
        || available.is_empty()
        || !requested.bytes().all(|byte| byte.is_ascii_digit())
        || !available.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    Some(ReplayUnavailable {
        requested_slot: requested.parse().ok()?,
        available_slot: available.parse().ok()?,
    })
}

fn replay_unavailable_from_status(
    status: &Status,
    effective_from_slot: Option<u64>,
    frames_seen: u64,
    frames_written: u64,
) -> Option<ReplayUnavailable> {
    if status.code() != Code::OutOfRange || frames_seen != 0 || frames_written != 0 {
        return None;
    }
    let replay = parse_replay_unavailable_message(status.message())?;
    if Some(replay.requested_slot) != effective_from_slot
        || replay.available_slot <= replay.requested_slot
    {
        return None;
    }
    Some(replay)
}

fn mark_replay_unavailable(report: &mut GrpcRawRecordReport, status: &Status) -> bool {
    let Some(replay) = replay_unavailable_from_status(
        status,
        report.effective_from_slot,
        report.frames_seen,
        report.frames_written,
    ) else {
        return false;
    };
    report.replay_unavailable = true;
    report.replay_unavailable_requested_slot = Some(replay.requested_slot);
    report.replay_available_slot = Some(replay.available_slot);
    apply_raw_record_retry(
        report,
        RawRecordRetry::new(GrpcRawRecordRetryReason::ReplayUnavailable),
    );
    tracing::warn!(
        requested_slot = replay.requested_slot,
        available_slot = replay.available_slot,
        "provider replay window no longer includes requested raw gRPC slot"
    );
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawRecordRetry {
    reason: GrpcRawRecordRetryReason,
    action_required: bool,
}

impl RawRecordRetry {
    const fn new(reason: GrpcRawRecordRetryReason) -> Self {
        Self {
            reason,
            action_required: false,
        }
    }
}

fn classify_raw_record_status(
    status: &Status,
    fallback: GrpcRawRecordRetryReason,
) -> RawRecordRetry {
    match status.code() {
        Code::PermissionDenied => RawRecordRetry {
            reason: GrpcRawRecordRetryReason::PermissionDenied,
            action_required: true,
        },
        Code::Unauthenticated => RawRecordRetry {
            reason: GrpcRawRecordRetryReason::Unauthenticated,
            action_required: true,
        },
        _ => RawRecordRetry::new(fallback),
    }
}

fn classify_raw_record_error(
    error: &anyhow::Error,
    fallback: GrpcRawRecordRetryReason,
) -> RawRecordRetry {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<Status>())
        .map_or_else(
            || RawRecordRetry::new(fallback),
            |status| classify_raw_record_status(status, fallback),
        )
}

fn apply_raw_record_retry(report: &mut GrpcRawRecordReport, retry: RawRecordRetry) {
    report.outcome = GrpcRawRecordOutcome::Retryable;
    report.retry_reason = Some(retry.reason);
    report.action_required = retry.action_required;
}

fn validate_complete_poh_block(block: &SubscribeUpdateBlock) -> Result<CompletePohBlockStats> {
    ensure!(
        block.entries_count > 0,
        "slot {} contains no PoH entries",
        block.slot
    );
    let expected_entries = usize::try_from(block.entries_count)
        .context("gRPC PoH entry count exceeds addressable memory")?;
    ensure!(
        block.entries.len() == expected_entries,
        "slot {} declares {} PoH entries but contains {}",
        block.slot,
        block.entries_count,
        block.entries.len()
    );
    let transaction_count =
        u64::try_from(block.transactions.len()).context("gRPC transaction count exceeds u64")?;
    ensure!(
        transaction_count == block.executed_transaction_count,
        "slot {} declares {} executed transactions but contains {}",
        block.slot,
        block.executed_transaction_count,
        transaction_count
    );

    let mut entries = block.entries.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|entry| entry.index);
    let mut expected_transaction_index = 0u64;
    let mut num_hashes = 0u128;
    for (expected_index, entry) in entries.iter().enumerate() {
        let expected_index =
            u64::try_from(expected_index).context("PoH entry index exceeds u64")?;
        ensure!(
            entry.index == expected_index,
            "slot {} PoH entry index {}, expected {}",
            block.slot,
            entry.index,
            expected_index
        );
        ensure!(
            entry.slot == block.slot,
            "slot {} contains a PoH entry for slot {}",
            block.slot,
            entry.slot
        );
        ensure!(
            entry.hash.len() == 32,
            "slot {} PoH entry {} has hash length {}, expected 32",
            block.slot,
            entry.index,
            entry.hash.len()
        );
        ensure!(
            entry.executed_transaction_count <= u32::MAX as u64,
            "slot {} PoH entry {} transaction count exceeds u32::MAX",
            block.slot,
            entry.index
        );
        ensure!(
            entry.starting_transaction_index == expected_transaction_index,
            "slot {} PoH entry {} starts at transaction {}, expected {}",
            block.slot,
            entry.index,
            entry.starting_transaction_index,
            expected_transaction_index
        );
        expected_transaction_index = expected_transaction_index
            .checked_add(entry.executed_transaction_count)
            .context("PoH transaction index overflow")?;
        num_hashes = num_hashes
            .checked_add(entry.num_hashes as u128)
            .context("PoH hash count overflow")?;
    }
    ensure!(
        expected_transaction_index == block.executed_transaction_count,
        "slot {} PoH entries reference {} transactions, expected {}",
        block.slot,
        expected_transaction_index,
        block.executed_transaction_count
    );

    let blockhash = decode_blockhash(&block.blockhash)?;
    let final_entry = entries.last().context("complete PoH has no final entry")?;
    ensure!(
        final_entry.hash.as_slice() == blockhash,
        "slot {} final PoH entry hash differs from its blockhash",
        block.slot
    );

    Ok(CompletePohBlockStats {
        entries: block.entries_count,
        transaction_references: expected_transaction_index,
        num_hashes,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrpcRawHandoffRecord {
    pub schema_version: u32,
    pub frame_id: u64,
    pub slot: u64,
    pub parent_slot: u64,
    pub epoch: u64,
    pub epoch_slot_index: u64,
    pub segment_id: u64,
    pub frame_offset: u64,
    pub frame_len: u64,
    pub compressed_len: u64,
    pub uncompressed_len: u64,
    pub protobuf_sha256: String,
    pub blockhash: String,
}

impl GrpcRawHandoffRecord {
    fn location(&self) -> SpoolLocation {
        SpoolLocation {
            segment_id: self.segment_id,
            frame_offset: self.frame_offset,
            frame_len: self.frame_len,
        }
    }
}

/// Allocation limits for lock-free reads of committed raw gRPC records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcRawCommittedReadLimits {
    pub max_compressed_record_bytes: u64,
    pub max_uncompressed_record_bytes: u64,
}

impl GrpcRawCommittedReadLimits {
    fn validate(self) -> Result<Self> {
        ensure!(
            self.max_compressed_record_bytes > 0,
            "maximum compressed raw gRPC record bytes must be non-zero"
        );
        ensure!(
            self.max_uncompressed_record_bytes > 0,
            "maximum uncompressed raw gRPC record bytes must be non-zero"
        );
        Ok(self)
    }
}

/// One handoff-committed raw record, verified directly against its WAL frame.
///
/// `compressed_bytes` is the exact independent zstd frame stored in the WAL; it is never
/// recompressed by this reader. `update` is decoded only after all WAL, identity, length, and raw
/// protobuf digest checks succeed.
#[derive(Debug, Clone)]
pub struct GrpcRawCommittedRecord {
    pub cluster_id: String,
    pub source_id: String,
    pub observation: ObservationId,
    pub slot: u64,
    pub blockhash: String,
    pub content_digest: ContentDigest,
    pub payload_format_version: u16,
    pub raw_protobuf_sha256: String,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: Vec<u8>,
    pub update: SubscribeUpdate,
    raw_protobuf_digest: [u8; 32],
    durable_local_record: DurableSpoolRecord,
}

impl GrpcRawCommittedRecord {
    /// Convert the verified handoff/WAL record into the exact compressed replication payload.
    /// The decoded Yellowstone value is dropped instead of being retained in the outbound batch.
    pub fn into_durable_replication_record(self) -> Result<GrpcRawDurableReplicationRecord> {
        let stream = ReplicationStreamId {
            cluster_id: self.cluster_id.clone(),
            origin_node_id: self.observation.origin_node_id.clone(),
            source_id: self.source_id.clone(),
            journal_id: self.observation.journal_id,
        };
        let durable_replication_witness = DurableReplicationWitness::from_verified_mapping(
            &self.durable_local_record,
            stream.clone(),
            self.observation.sequence,
            self.content_digest,
        )?;
        let blockhash = decode_blockhash(&self.blockhash)?;
        let record = RawReplicationRecord {
            offer: ReplicationOffer {
                protocol_version: REPLICATION_PROTOCOL_VERSION,
                cluster_id: self.cluster_id,
                record: self.observation,
                source_id: self.source_id,
                logical_key: LogicalKey::Block {
                    slot: self.slot,
                    blockhash,
                },
                content_digest: self.content_digest,
                payload_len: self.compressed_bytes.len() as u64,
                payload_format_version: self.payload_format_version,
                commitment: CommitmentEvidence::Confirmed,
            },
            compressed_payload: self.compressed_bytes,
            raw_protobuf_sha256: self.raw_protobuf_digest,
            uncompressed_len: self.uncompressed_bytes,
        };
        Ok(GrpcRawDurableReplicationRecord {
            stream,
            record,
            durable_local_record: self.durable_local_record,
            durable_replication_witness,
        })
    }
}

/// Lean outbound representation of one locally durable raw gRPC record. It retains compressed
/// bytes and the private local durability witness, but not the decoded Yellowstone allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcRawDurableReplicationRecord {
    pub stream: ReplicationStreamId,
    pub record: RawReplicationRecord,
    durable_local_record: DurableSpoolRecord,
    durable_replication_witness: DurableReplicationWitness,
}

impl GrpcRawDurableReplicationRecord {
    pub fn durable_local_record(&self) -> &DurableSpoolRecord {
        &self.durable_local_record
    }
}

/// Result of a snapshot read of the handoff-committed prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcRawCommittedReadReport {
    pub records: u64,
    /// Bytes after the last newline in the handoff snapshot. These are never decoded or visited.
    pub incomplete_handoff_tail_bytes: u64,
}

/// Incremental, restartable reader for a live handoff journal. It advances only after one row has
/// been re-read from the checksummed WAL and converted to the lean replication representation.
/// A final partial JSON row is never consumed.
#[derive(Debug, Clone)]
pub struct GrpcRawCommittedCursor {
    identity: GrpcRawIdentityFile,
    generation_dir: PathBuf,
    hot_cache_root: Option<PathBuf>,
    limits: GrpcRawCommittedReadLimits,
    next_frame_id: u64,
    next_replication_sequence: u64,
    next_handoff_offset: u64,
}

#[derive(Debug, Clone)]
pub struct GrpcRawCommittedBatch {
    pub records: Vec<GrpcRawDurableReplicationRecord>,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
    pub incomplete_handoff_tail_bytes: u64,
    pub snapshot_handoff_bytes: u64,
    start_frame_id: u64,
    start_replication_sequence: u64,
    start_handoff_offset: u64,
    next_frame_id: u64,
    next_replication_sequence: u64,
    next_handoff_offset: u64,
}

/// Opaque, payload-free reservation cursor retained while the batch bytes are owned by the gRPC
/// transport. It contains the exact local tail witness needed by the ACK WAL, but it cannot advance
/// the source cursor without a matching [`DurableCumulativeAck`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcRawCommittedAdvance {
    stream: ReplicationStreamId,
    start_frame_id: u64,
    start_replication_sequence: u64,
    start_handoff_offset: u64,
    next_frame_id: u64,
    next_replication_sequence: u64,
    next_handoff_offset: u64,
    durable_replication_witness: DurableReplicationWitness,
}

/// One immutable sealed generation, or the current active tail, in replication order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcRawReplicationGeneration {
    pub path: PathBuf,
    pub sealed: bool,
}

/// One cursor opened atomically with its hot-queue discovery snapshot.
#[derive(Debug, Clone)]
pub struct GrpcRawPreparedReplicationGeneration {
    pub cursor: GrpcRawCommittedCursor,
    pub sealed: bool,
}

/// Discover the bounded hot-generation queue oldest-first, followed by the live active tail.
/// Discovery fails during an unfinished rotation and rejects aliases/duplicate physical WAL
/// identities. Adjacent generations may intentionally share one logical replication stream.
pub fn discover_grpc_raw_replication_generations(
    cache_root: impl AsRef<Path>,
) -> Result<Vec<GrpcRawReplicationGeneration>> {
    let cache_root = cache_root.as_ref();
    ensure_real_directory(cache_root, "raw gRPC hot-generation root")?;
    let _rotation_guard = HotRotationGuard::acquire(cache_root, HotRotationLockMode::Shared)?;
    ensure!(
        !path_entry_exists(&cache_root.join(HOT_ROTATION_MARKER))?,
        "raw gRPC generation rotation is in progress; retry after supervisor recovery"
    );
    let sealed_root = cache_root.join(HOT_SEALED_DIR);
    let active = cache_root.join(HOT_ACTIVE_DIR);
    ensure_real_directory(&sealed_root, "raw gRPC sealed generation root")?;
    ensure_real_directory(&active, "raw gRPC active generation")?;

    let mut sealed = Vec::new();
    for entry in fs::read_dir(&sealed_root)
        .with_context(|| format!("list raw gRPC generations {}", sealed_root.display()))?
    {
        let entry = entry.context("read raw gRPC sealed generation entry")?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("raw gRPC sealed generation name is not UTF-8"))?;
        ensure!(
            valid_hot_generation_id(&name),
            "invalid raw gRPC sealed generation name {name:?}"
        );
        ensure!(
            sealed.len() < MAX_REPLICATION_GENERATION_SCAN,
            "raw gRPC sealed generation queue exceeds {MAX_REPLICATION_GENERATION_SCAN} entries"
        );
        let file_type = entry
            .file_type()
            .context("inspect raw gRPC sealed generation entry")?;
        ensure!(
            file_type.is_dir() && !file_type.is_symlink(),
            "raw gRPC sealed generation is not a real directory"
        );
        sealed.push((name, entry.path()));
    }
    sealed.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut physical_streams = std::collections::HashSet::with_capacity(sealed.len() + 1);
    let mut generations = Vec::with_capacity(sealed.len() + 1);
    for (_, path) in sealed {
        let identity = read_identity(&path.join(IDENTITY_FILE))?;
        ensure!(
            physical_streams.insert(identity.physical_stream_id()),
            "duplicate raw gRPC physical WAL identity in hot-generation queue"
        );
        generations.push(GrpcRawReplicationGeneration { path, sealed: true });
    }
    let active_identity = read_identity(&active.join(IDENTITY_FILE))?;
    ensure!(
        physical_streams.insert(active_identity.physical_stream_id()),
        "active raw gRPC physical WAL identity duplicates a sealed generation"
    );
    generations.push(GrpcRawReplicationGeneration {
        path: active,
        sealed: false,
    });
    Ok(generations)
}

/// Discover the bounded queue and pre-open every cursor while holding one shared rotation lock.
/// This closes the otherwise unavoidable `/active` pathname race between queue discovery and
/// identity fencing: the returned active cursor always belongs to the stream that occupied the
/// discovered snapshot, and follows that identity into `sealed` after the lock is released.
pub fn open_grpc_raw_replication_generation_cursors(
    cache_root: impl AsRef<Path>,
    limits: GrpcRawCommittedReadLimits,
) -> Result<Vec<GrpcRawPreparedReplicationGeneration>> {
    let cache_root = cache_root.as_ref();
    ensure_real_directory(cache_root, "raw gRPC hot-generation root")?;
    let _rotation_guard = HotRotationGuard::acquire(cache_root, HotRotationLockMode::Shared)?;
    let generations = discover_grpc_raw_replication_generations(cache_root)?;
    validate_grpc_raw_replication_generation_chain(&generations, limits)?;
    generations
        .into_iter()
        .map(|generation| {
            let cursor = GrpcRawCommittedCursor::open(&generation.path, limits, 0)?;
            Ok(GrpcRawPreparedReplicationGeneration {
                cursor,
                sealed: generation.sealed,
            })
        })
        .collect()
}

/// Validate every same-stream physical generation boundary before any durable ACK can be used to
/// seek a cursor. This prevents a corrupt/lowered logical base from turning an ACK into a skip.
fn validate_grpc_raw_replication_generation_chain(
    generations: &[GrpcRawReplicationGeneration],
    limits: GrpcRawCommittedReadLimits,
) -> Result<()> {
    let limits = limits.validate()?;
    let mut seen_logical_streams = std::collections::HashSet::with_capacity(generations.len());
    let mut previous: Option<(&GrpcRawReplicationGeneration, GrpcRawIdentityFile)> = None;

    for generation in generations {
        if generation.sealed {
            let snapshot = complete_handoff_snapshot(&generation.path.join(HANDOFF_JOURNAL_FILE))?;
            ensure!(
                snapshot.complete_offset == snapshot.snapshot_len,
                "sealed raw gRPC generation has a partial handoff tail"
            );
        }
        let identity = read_identity(&generation.path.join(IDENTITY_FILE))?;
        let stream = identity.replication_stream_id();
        match previous.as_ref() {
            Some((predecessor, predecessor_identity))
                if predecessor_identity.replication_stream_id() == stream =>
            {
                ensure!(
                    predecessor.sealed,
                    "only a sealed raw gRPC generation may precede a logical successor"
                );
                validate_grpc_raw_replication_generation_boundary(
                    &predecessor.path,
                    predecessor_identity,
                    &generation.path,
                    &identity,
                    limits,
                )?;
            }
            Some(_) => {
                ensure_initial_logical_generation(&identity)?;
                ensure!(
                    seen_logical_streams.insert(stream),
                    "raw gRPC logical replication stream reappeared non-contiguously"
                );
            }
            None => {
                ensure_initial_logical_generation(&identity)?;
                seen_logical_streams.insert(stream);
            }
        }
        previous = Some((generation, identity));
    }
    Ok(())
}

fn ensure_initial_logical_generation(identity: &GrpcRawIdentityFile) -> Result<()> {
    ensure!(
        identity.replication_sequence_base() == 0
            && identity.replication_stream_id() == identity.physical_stream_id(),
        "first retained raw gRPC logical stream generation lacks its base-zero physical anchor"
    );
    Ok(())
}

fn validate_grpc_raw_replication_generation_boundary(
    predecessor_dir: &Path,
    predecessor_identity: &GrpcRawIdentityFile,
    successor_dir: &Path,
    successor_identity: &GrpcRawIdentityFile,
    limits: GrpcRawCommittedReadLimits,
) -> Result<()> {
    ensure!(
        successor_identity.replication_journal_id.is_some()
            && successor_identity.replication_sequence_base.is_some(),
        "logical successor is missing its persisted replication anchor"
    );
    let predecessor_journal = predecessor_dir.join(HANDOFF_JOURNAL_FILE);
    let predecessor_snapshot = complete_handoff_snapshot(&predecessor_journal)?;
    ensure!(
        predecessor_snapshot.complete_offset == predecessor_snapshot.snapshot_len,
        "sealed raw gRPC predecessor has a partial handoff tail"
    );
    let predecessor_tail = predecessor_snapshot
        .state
        .last
        .as_ref()
        .context("logical raw gRPC predecessor is empty")?;
    let expected_base = predecessor_identity.replication_sequence(predecessor_tail.frame_id)?;
    ensure!(
        successor_identity.replication_sequence_base() == expected_base,
        "logical raw gRPC successor sequence base does not equal predecessor tail"
    );

    let successor_snapshot = complete_handoff_snapshot(&successor_dir.join(HANDOFF_JOURNAL_FILE))?;
    let successor_seed = successor_snapshot
        .state
        .first
        .as_ref()
        .context("logical raw gRPC successor is empty")?;
    ensure!(
        successor_seed.frame_id == 0
            && successor_identity.replication_sequence(successor_seed.frame_id)? == expected_base,
        "logical raw gRPC successor does not begin at its overlap sequence"
    );

    let predecessor_record = read_grpc_raw_boundary_spool_record(
        predecessor_dir,
        predecessor_identity,
        predecessor_tail,
        limits,
    )?;
    let successor_record = read_grpc_raw_boundary_spool_record(
        successor_dir,
        successor_identity,
        successor_seed,
        limits,
    )?;
    ensure!(
        predecessor_record.metadata.logical_key == successor_record.metadata.logical_key
            && predecessor_record.metadata.content_digest
                == successor_record.metadata.content_digest
            && predecessor_record.metadata.payload_format_version
                == successor_record.metadata.payload_format_version
            && predecessor_record.payload == successor_record.payload
            && predecessor_tail.slot == successor_seed.slot
            && predecessor_tail.parent_slot == successor_seed.parent_slot
            && predecessor_tail.epoch == successor_seed.epoch
            && predecessor_tail.epoch_slot_index == successor_seed.epoch_slot_index
            && predecessor_tail.compressed_len == successor_seed.compressed_len
            && predecessor_tail.uncompressed_len == successor_seed.uncompressed_len
            && predecessor_tail
                .protobuf_sha256
                .eq_ignore_ascii_case(&successor_seed.protobuf_sha256)
            && predecessor_tail.blockhash == successor_seed.blockhash,
        "logical raw gRPC successor seed does not exactly reproduce predecessor tail"
    );
    Ok(())
}

fn read_grpc_raw_boundary_spool_record(
    generation_dir: &Path,
    identity: &GrpcRawIdentityFile,
    row: &GrpcRawHandoffRecord,
    limits: GrpcRawCommittedReadLimits,
) -> Result<SpoolRecord> {
    ensure!(
        row.compressed_len <= limits.max_compressed_record_bytes
            && row.uncompressed_len <= limits.max_uncompressed_record_bytes,
        "raw gRPC generation boundary exceeds replication read limits"
    );
    let stored = read_spool_record(
        spool_journal_dir(generation_dir, identity),
        row.location(),
        limits.max_compressed_record_bytes,
    )?;
    ensure!(
        stored.metadata.cluster_id == identity.cluster_id
            && stored.metadata.observation.origin_node_id == identity.origin_node_id
            && stored.metadata.observation.journal_id == identity.journal_id
            && stored.metadata.observation.sequence == row.frame_id
            && stored.metadata.source_id == identity.source_id
            && stored.metadata.payload_format_version == PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1
            && stored.payload.len() as u64 == row.compressed_len,
        "raw gRPC generation boundary WAL/handoff identity mismatch"
    );
    Ok(stored)
}

impl GrpcRawCommittedBatch {
    /// Move compressed payload ownership directly into the transport while retaining only the
    /// small cursor/durability token needed to commit the eventual ACK. This avoids a second batch-
    /// sized payload allocation.
    pub fn into_transport_parts(
        self,
    ) -> Result<(Vec<RawReplicationRecord>, GrpcRawCommittedAdvance)> {
        let tail = self
            .records
            .last()
            .context("cannot transport an empty raw gRPC committed batch")?;
        let advance = GrpcRawCommittedAdvance {
            stream: tail.stream.clone(),
            start_frame_id: self.start_frame_id,
            start_replication_sequence: self.start_replication_sequence,
            start_handoff_offset: self.start_handoff_offset,
            next_frame_id: self.next_frame_id,
            next_replication_sequence: self.next_replication_sequence,
            next_handoff_offset: self.next_handoff_offset,
            durable_replication_witness: tail.durable_replication_witness.clone(),
        };
        let records = self
            .records
            .into_iter()
            .map(|record| record.record)
            .collect();
        Ok((records, advance))
    }
}

impl GrpcRawCommittedAdvance {
    pub fn durable_replication_witness(&self) -> &DurableReplicationWitness {
        &self.durable_replication_witness
    }

    pub fn through_sequence(&self) -> u64 {
        self.next_replication_sequence - 1
    }
}

impl GrpcRawCommittedCursor {
    /// Open at the first local frame that has not already been durably acknowledged. Startup scans
    /// only the bounded JSON handoff rows needed to locate `next_frame_id`; payload/WAL reads then
    /// proceed incrementally from that byte offset.
    pub fn open(
        output_dir: impl AsRef<Path>,
        limits: GrpcRawCommittedReadLimits,
        next_frame_id: u64,
    ) -> Result<Self> {
        Self::open_inner(output_dir.as_ref(), limits, next_frame_id, None)
    }

    /// Open only if the directory still names the exact stream discovered by the caller. For an
    /// active hot generation the identity check and handoff seek are protected by the shared
    /// rotation lock, so a successor can never be attached to a predecessor's ACK state.
    pub fn open_fenced(
        output_dir: impl AsRef<Path>,
        limits: GrpcRawCommittedReadLimits,
        next_frame_id: u64,
        expected_stream: &ReplicationStreamId,
    ) -> Result<Self> {
        Self::open_inner(
            output_dir.as_ref(),
            limits,
            next_frame_id,
            Some(expected_stream),
        )
    }

    fn open_inner(
        output_dir: &Path,
        limits: GrpcRawCommittedReadLimits,
        next_frame_id: u64,
        expected_stream: Option<&ReplicationStreamId>,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        let hot_cache_root = infer_hot_cache_root(output_dir);
        let _rotation_guard = hot_cache_root
            .as_deref()
            .map(|root| HotRotationGuard::acquire(root, HotRotationLockMode::Shared))
            .transpose()?;
        let identity = read_identity(&output_dir.join(IDENTITY_FILE))?;
        if let Some(expected_stream) = expected_stream {
            ensure!(
                identity.replication_stream_id() == *expected_stream,
                "raw gRPC generation identity differs from the expected replication stream"
            );
        }
        let journal_path = output_dir.join(HANDOFF_JOURNAL_FILE);
        let next_handoff_offset = locate_handoff_sequence(&journal_path, next_frame_id)?;
        let next_replication_sequence = identity.replication_sequence(next_frame_id)?;
        Ok(Self {
            identity,
            generation_dir: output_dir.to_path_buf(),
            hot_cache_root,
            limits,
            next_frame_id,
            next_replication_sequence,
            next_handoff_offset,
        })
    }

    pub fn stream(&self) -> ReplicationStreamId {
        self.identity.replication_stream_id()
    }

    pub fn physical_stream(&self) -> ReplicationStreamId {
        self.identity.physical_stream_id()
    }

    pub fn next_frame_id(&self) -> u64 {
        self.next_frame_id
    }

    pub fn next_replication_sequence(&self) -> u64 {
        self.next_replication_sequence
    }

    pub fn next_handoff_offset(&self) -> u64 {
        self.next_handoff_offset
    }

    pub(crate) fn read_limits(&self) -> GrpcRawCommittedReadLimits {
        self.limits
    }

    /// Reposition a cloned, identity-fenced cursor to the first sequence not covered by the local
    /// durable ACK. Resolution and journal seeking share one rotation lock, so an active cursor
    /// follows its predecessor into `sealed` without ever opening the successor's handoff file.
    pub(crate) fn seek_to_replication_sequence(
        &mut self,
        next_replication_sequence: u64,
    ) -> Result<()> {
        let _rotation_guard = self.acquire_hot_rotation_guard()?;
        let generation_dir = self.resolve_bound_generation_dir_unlocked()?;
        let requested_frame_id = self
            .identity
            .frame_id_for_replication_sequence(next_replication_sequence)?;
        let journal_path = generation_dir.join(HANDOFF_JOURNAL_FILE);
        let (complete_records, complete_offset) = complete_handoff_prefix(&journal_path)?;
        let (next_frame_id, next_handoff_offset) = if requested_frame_id <= complete_records {
            (
                requested_frame_id,
                locate_handoff_sequence(&journal_path, requested_frame_id)?,
            )
        } else {
            ensure!(
                generation_dir
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == HOT_SEALED_DIR),
                "remote durable prefix is ahead of an active raw gRPC generation"
            );
            // A later physical generation in this same logical stream has already been ACKed.
            // Pin this older sealed generation at EOF while retaining the actual logical cursor
            // so startup reconciliation still names the latest durable prefix.
            (complete_records, complete_offset)
        };
        self.generation_dir = generation_dir;
        self.next_frame_id = next_frame_id;
        self.next_replication_sequence = next_replication_sequence;
        self.next_handoff_offset = next_handoff_offset;
        Ok(())
    }

    /// Read a snapshot-bounded batch without advancing the committed cursor. Empty output means
    /// the cursor is caught up (or is waiting for a partial writer tail); it never means the cursor
    /// silently skipped a row. The same batch remains readable until
    /// [`Self::advance_after_durable_ack`] receives the fsynced ACK-WAL token for its exact tail.
    pub fn read_batch(
        &self,
        max_records: usize,
        max_compressed_bytes: u64,
        max_uncompressed_bytes: u64,
    ) -> Result<GrpcRawCommittedBatch> {
        ensure!(
            max_records > 0,
            "raw replication batch record limit must be non-zero"
        );
        ensure!(
            max_compressed_bytes > 0,
            "raw replication batch byte limit must be non-zero"
        );
        ensure!(
            max_uncompressed_bytes > 0,
            "raw replication batch uncompressed-byte limit must be non-zero"
        );
        // Keep the predecessor visible from identity resolution through the final WAL read. A
        // concurrent hot rotation takes the exclusive side of this lock, so the cursor can see
        // either the complete pre-rotation layout or the complete post-rotation layout, never the
        // short publication interval between them.
        let _rotation_guard = self.acquire_hot_rotation_guard()?;
        let generation_dir = self.resolve_bound_generation_dir_unlocked()?;
        let journal_path = generation_dir.join(HANDOFF_JOURNAL_FILE);
        let wal_dir = spool_journal_dir(&generation_dir, &self.identity);
        let mut file = open_regular_readonly_nofollow(&journal_path, "raw gRPC handoff journal")?;
        let snapshot_len = file
            .metadata()
            .with_context(|| format!("inspect raw gRPC journal {}", journal_path.display()))?
            .len();
        ensure!(
            snapshot_len >= self.next_handoff_offset,
            "raw gRPC handoff journal shrank below the committed cursor"
        );
        file.seek(SeekFrom::Start(self.next_handoff_offset))
            .with_context(|| format!("seek raw gRPC journal {}", journal_path.display()))?;
        let remaining = snapshot_len - self.next_handoff_offset;
        let mut reader = BufReader::new(file.take(remaining));
        let mut line = Vec::new();
        let mut records = Vec::with_capacity(max_records.min(64));
        let mut compressed_bytes = 0u64;
        let mut uncompressed_bytes = 0u64;
        let mut incomplete_handoff_tail_bytes = 0u64;
        let mut next_frame_id = self.next_frame_id;
        let mut next_replication_sequence = self.next_replication_sequence;
        let mut next_handoff_offset = self.next_handoff_offset;

        while records.len() < max_records {
            line.clear();
            let bytes = {
                let mut bounded = (&mut reader).take(MAX_HANDOFF_RECORD_BYTES + 1);
                bounded
                    .read_until(b'\n', &mut line)
                    .with_context(|| format!("read raw gRPC journal {}", journal_path.display()))?
            };
            if bytes == 0 {
                break;
            }
            let bytes = u64::try_from(bytes).context("raw gRPC handoff row length exceeds u64")?;
            ensure!(
                bytes <= MAX_HANDOFF_RECORD_BYTES,
                "raw gRPC handoff row exceeds {MAX_HANDOFF_RECORD_BYTES} bytes"
            );
            if !line.ends_with(b"\n") {
                incomplete_handoff_tail_bytes = bytes;
                break;
            }
            let payload = &line[..line.len() - 1];
            if payload.iter().all(u8::is_ascii_whitespace) {
                next_handoff_offset = next_handoff_offset
                    .checked_add(bytes)
                    .context("raw gRPC handoff cursor overflow")?;
                continue;
            }
            let row: GrpcRawHandoffRecord = serde_json::from_slice(payload)
                .with_context(|| format!("decode raw gRPC journal frame {next_frame_id}"))?;
            validate_handoff_sequence(&row, next_frame_id)?;
            ensure!(
                row.compressed_len <= max_compressed_bytes,
                "raw gRPC frame {} compressed length {} exceeds replication batch maximum {}",
                row.frame_id,
                row.compressed_len,
                max_compressed_bytes
            );
            ensure!(
                row.uncompressed_len <= max_uncompressed_bytes,
                "raw gRPC frame {} uncompressed length {} exceeds replication batch maximum {}",
                row.frame_id,
                row.uncompressed_len,
                max_uncompressed_bytes
            );
            let projected = compressed_bytes
                .checked_add(row.compressed_len)
                .context("raw replication batch byte count overflow")?;
            if !records.is_empty() && projected > max_compressed_bytes {
                break;
            }
            let projected_uncompressed = uncompressed_bytes
                .checked_add(row.uncompressed_len)
                .context("raw replication batch uncompressed-byte count overflow")?;
            if !records.is_empty() && projected_uncompressed > max_uncompressed_bytes {
                break;
            }
            let record = read_verified_grpc_raw_replication_record(
                &wal_dir,
                &self.identity,
                &row,
                self.limits,
            )?;
            ensure!(
                record.stream == self.stream(),
                "raw gRPC replication record changed stream identity"
            );
            ensure!(
                record.record.offer.record.sequence == next_replication_sequence,
                "raw gRPC logical replication sequence is not contiguous"
            );
            records.push(record);
            compressed_bytes = projected;
            uncompressed_bytes = projected_uncompressed;
            next_handoff_offset = next_handoff_offset
                .checked_add(bytes)
                .context("raw gRPC handoff cursor overflow")?;
            next_frame_id = next_frame_id
                .checked_add(1)
                .context("raw gRPC frame sequence exhausted")?;
            next_replication_sequence = next_replication_sequence
                .checked_add(1)
                .context("raw gRPC logical replication sequence exhausted")?;
        }

        Ok(GrpcRawCommittedBatch {
            records,
            compressed_bytes,
            uncompressed_bytes,
            incomplete_handoff_tail_bytes,
            snapshot_handoff_bytes: snapshot_len,
            start_frame_id: self.next_frame_id,
            start_replication_sequence: self.next_replication_sequence,
            start_handoff_offset: self.next_handoff_offset,
            next_frame_id,
            next_replication_sequence,
            next_handoff_offset,
        })
    }

    /// Advance only after the remote cumulative ACK has been authenticated and the replica's ACK
    /// journal has fsynced it. The durable token must name this reservation's exact local tail.
    pub fn advance_after_durable_ack(
        &mut self,
        advance: &GrpcRawCommittedAdvance,
        durable_ack: &DurableCumulativeAck,
    ) -> Result<()> {
        ensure!(
            advance.start_frame_id == self.next_frame_id
                && advance.start_replication_sequence == self.next_replication_sequence
                && advance.start_handoff_offset == self.next_handoff_offset,
            "raw gRPC committed batch is stale or belongs to a different cursor position"
        );
        ensure!(
            advance.stream == self.stream() && durable_ack.ack().stream == advance.stream,
            "durable cumulative ACK belongs to a different raw gRPC stream"
        );
        ensure!(
            durable_ack.ack().through_sequence == advance.through_sequence(),
            "durable cumulative ACK does not cover the reserved batch tail"
        );
        ensure!(
            durable_ack.matches_replication_witness(&advance.durable_replication_witness),
            "durable cumulative ACK names a different physical local spool witness"
        );
        self.next_frame_id = advance.next_frame_id;
        self.next_replication_sequence = advance.next_replication_sequence;
        self.next_handoff_offset = advance.next_handoff_offset;
        Ok(())
    }

    /// Resolve the directory that currently owns this immutable generation identity. If an active
    /// hot generation rotated after this cursor opened, the predecessor is found under `sealed`
    /// and drained there; the new active journal can never be mistaken for the old stream.
    pub fn bound_generation_dir(&self) -> Result<PathBuf> {
        let _rotation_guard = self.acquire_hot_rotation_guard()?;
        self.resolve_bound_generation_dir_unlocked()
    }

    pub fn bound_generation_is_sealed(&self) -> Result<bool> {
        let path = self.bound_generation_dir()?;
        Ok(path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == HOT_SEALED_DIR))
    }

    /// Whether a locally durable logical ACK covers the complete committed prefix of this
    /// physical sealed generation. Callers use this to skip old retained generations before
    /// remote-ahead reconciliation, which must occur only in the generation containing the next
    /// locally unacknowledged sequence.
    pub fn sealed_generation_is_covered_by(&self, through_sequence: u64) -> Result<bool> {
        let _rotation_guard = self.acquire_hot_rotation_guard()?;
        let generation_dir = self.resolve_bound_generation_dir_unlocked()?;
        ensure!(
            generation_dir
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == HOT_SEALED_DIR),
            "raw gRPC ACK coverage check requires a sealed generation"
        );
        let snapshot = complete_handoff_snapshot(&generation_dir.join(HANDOFF_JOURNAL_FILE))?;
        ensure!(
            snapshot.complete_offset == snapshot.snapshot_len,
            "sealed raw gRPC generation has a partial handoff tail"
        );
        let Some(tail) = snapshot.state.last else {
            return Ok(true);
        };
        Ok(through_sequence >= self.identity.replication_sequence(tail.frame_id)?)
    }

    fn acquire_hot_rotation_guard(&self) -> Result<Option<HotRotationGuard>> {
        self.hot_cache_root
            .as_deref()
            .map(|root| HotRotationGuard::acquire(root, HotRotationLockMode::Shared))
            .transpose()
    }

    fn resolve_bound_generation_dir_unlocked(&self) -> Result<PathBuf> {
        match read_identity_if_present(&self.generation_dir.join(IDENTITY_FILE))? {
            Some(identity) if identity == self.identity => {
                return Ok(self.generation_dir.clone());
            }
            Some(_) | None => {}
        }
        let cache_root = self.hot_cache_root.as_deref().context(
            "raw gRPC generation identity changed outside a recognized hot-generation layout",
        )?;
        let sealed_root = cache_root.join(HOT_SEALED_DIR);
        let metadata = fs::symlink_metadata(&sealed_root).with_context(|| {
            format!(
                "inspect raw gRPC sealed generation root {}",
                sealed_root.display()
            )
        })?;
        ensure!(
            metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
            "raw gRPC sealed generation root is not a real directory"
        );
        let mut matches = Vec::new();
        let mut scanned = 0usize;
        for entry in fs::read_dir(&sealed_root)
            .with_context(|| format!("list raw gRPC generations {}", sealed_root.display()))?
        {
            let entry = entry.context("read raw gRPC sealed generation entry")?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if !valid_hot_generation_id(name) {
                continue;
            }
            scanned = scanned
                .checked_add(1)
                .context("raw gRPC sealed generation count overflow")?;
            ensure!(
                scanned <= MAX_REPLICATION_GENERATION_SCAN,
                "raw gRPC sealed generation scan exceeds {MAX_REPLICATION_GENERATION_SCAN} entries"
            );
            let file_type = entry
                .file_type()
                .context("inspect raw gRPC sealed generation entry")?;
            ensure!(
                file_type.is_dir() && !file_type.is_symlink(),
                "raw gRPC sealed generation entry is not a real directory"
            );
            let candidate = entry.path();
            if read_identity_if_present(&candidate.join(IDENTITY_FILE))?
                .is_some_and(|identity| identity == self.identity)
            {
                matches.push(candidate);
            }
        }
        ensure!(
            matches.len() == 1,
            "raw gRPC rotated generation identity has {} sealed matches; retry after rotation publication or repair the cache",
            matches.len()
        );
        Ok(matches.remove(0))
    }
}

fn infer_hot_cache_root(generation_dir: &Path) -> Option<PathBuf> {
    if generation_dir
        .file_name()
        .is_some_and(|name| name == HOT_ACTIVE_DIR)
    {
        generation_dir.parent().map(Path::to_path_buf)
    } else {
        None
    }
}

fn valid_hot_generation_id(name: &str) -> bool {
    hot_generation_number(name).is_some()
}

fn hot_generation_number(name: &str) -> Option<u64> {
    if name.len() != "slot-".len() + 20 || !name.starts_with("slot-") {
        return None;
    }
    let digits = &name["slot-".len()..];
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn read_identity_if_present(path: &Path) -> Result<Option<GrpcRawIdentityFile>> {
    match fs::symlink_metadata(path) {
        Ok(_) => read_identity(path).map(Some),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("inspect raw gRPC identity {}", path.display()))
        }
    }
}

fn open_regular_readonly_nofollow(path: &Path, description: &str) -> Result<File> {
    let linked = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {description} {}", path.display()))?;
    ensure!(
        linked.file_type().is_file() && !linked.file_type().is_symlink(),
        "{description} is not a regular non-symlink file: {}",
        path.display()
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(path)
        .with_context(|| format!("open {description} {}", path.display()))?;
    let opened = file
        .metadata()
        .with_context(|| format!("inspect opened {description} {}", path.display()))?;
    ensure!(
        opened.file_type().is_file(),
        "{description} is not a regular file: {}",
        path.display()
    );
    #[cfg(unix)]
    {
        ensure!(
            opened.dev() == linked.dev() && opened.ino() == linked.ino(),
            "{description} changed while it was opened: {}",
            path.display()
        );
        let effective_uid = unsafe { libc::geteuid() };
        ensure!(
            opened.uid() == effective_uid || opened.uid() == 0,
            "{description} has an untrusted owner: {}",
            path.display()
        );
        ensure!(
            opened.mode() & 0o022 == 0,
            "{description} must not be writable by group or other users: {}",
            path.display()
        );
    }
    Ok(file)
}

/// Count and locate only the newline-committed handoff prefix in one stable file snapshot.
/// A concurrent writer's partial final row is ignored without truncation.
fn complete_handoff_prefix(path: &Path) -> Result<(u64, u64)> {
    let snapshot = complete_handoff_snapshot(path)?;
    Ok((snapshot.state.records, snapshot.complete_offset))
}

#[derive(Debug)]
struct CompleteHandoffSnapshot {
    state: JournalState,
    complete_offset: u64,
    snapshot_len: u64,
}

fn complete_handoff_snapshot(path: &Path) -> Result<CompleteHandoffSnapshot> {
    let file = open_regular_readonly_nofollow(path, "raw gRPC handoff journal")?;
    let snapshot_len = file
        .metadata()
        .with_context(|| format!("inspect raw gRPC journal {}", path.display()))?
        .len();
    let mut reader = BufReader::new(file.take(snapshot_len));
    let mut line = Vec::new();
    let mut state = JournalState::default();
    let mut offset = 0u64;
    loop {
        line.clear();
        let bytes = {
            let mut bounded = (&mut reader).take(MAX_HANDOFF_RECORD_BYTES + 1);
            bounded
                .read_until(b'\n', &mut line)
                .with_context(|| format!("read raw gRPC journal {}", path.display()))?
        };
        if bytes == 0 {
            break;
        }
        let bytes = u64::try_from(bytes).context("raw gRPC handoff row length exceeds u64")?;
        ensure!(
            bytes <= MAX_HANDOFF_RECORD_BYTES,
            "raw gRPC handoff row exceeds {MAX_HANDOFF_RECORD_BYTES} bytes"
        );
        if !line.ends_with(b"\n") {
            break;
        }
        let payload = &line[..line.len() - 1];
        offset = offset
            .checked_add(bytes)
            .context("raw gRPC handoff cursor overflow")?;
        if payload.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let row: GrpcRawHandoffRecord = serde_json::from_slice(payload)
            .with_context(|| format!("decode raw gRPC journal frame {}", state.records))?;
        validate_handoff_sequence(&row, state.records)?;
        state.first.get_or_insert_with(|| row.clone());
        state.last = Some(row);
        state.records = state
            .records
            .checked_add(1)
            .context("raw gRPC handoff sequence exhausted")?;
    }
    Ok(CompleteHandoffSnapshot {
        state,
        complete_offset: offset,
        snapshot_len,
    })
}

fn locate_handoff_sequence(path: &Path, requested_sequence: u64) -> Result<u64> {
    let file = open_regular_readonly_nofollow(path, "raw gRPC handoff journal")?;
    let snapshot_len = file
        .metadata()
        .with_context(|| format!("inspect raw gRPC journal {}", path.display()))?
        .len();
    let mut reader = BufReader::new(file.take(snapshot_len));
    let mut line = Vec::new();
    let mut sequence = 0u64;
    let mut offset = 0u64;
    loop {
        line.clear();
        let bytes = {
            let mut bounded = (&mut reader).take(MAX_HANDOFF_RECORD_BYTES + 1);
            bounded
                .read_until(b'\n', &mut line)
                .with_context(|| format!("read raw gRPC journal {}", path.display()))?
        };
        if bytes == 0 {
            break;
        }
        let bytes = u64::try_from(bytes).context("raw gRPC handoff row length exceeds u64")?;
        ensure!(
            bytes <= MAX_HANDOFF_RECORD_BYTES,
            "raw gRPC handoff row exceeds {MAX_HANDOFF_RECORD_BYTES} bytes"
        );
        if !line.ends_with(b"\n") {
            break;
        }
        let payload = &line[..line.len() - 1];
        if payload.iter().all(u8::is_ascii_whitespace) {
            offset = offset
                .checked_add(bytes)
                .context("raw gRPC handoff cursor overflow")?;
            continue;
        }
        let row: GrpcRawHandoffRecord = serde_json::from_slice(payload)
            .with_context(|| format!("decode raw gRPC journal frame {sequence}"))?;
        validate_handoff_sequence(&row, sequence)?;
        if sequence == requested_sequence {
            return Ok(offset);
        }
        sequence = sequence
            .checked_add(1)
            .context("raw gRPC handoff sequence exhausted")?;
        offset = offset
            .checked_add(bytes)
            .context("raw gRPC handoff cursor overflow")?;
    }
    ensure!(
        sequence == requested_sequence,
        "raw gRPC cursor sequence {requested_sequence} is beyond durable handoff tail {sequence}"
    );
    Ok(offset)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GrpcRawIdentityFile {
    schema_version: u32,
    endpoint: String,
    cluster_id: String,
    origin_node_id: String,
    source_id: String,
    journal_id: [u8; 16],
    /// Stable logical replication journal shared by physical hot-storage generations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replication_journal_id: Option<[u8; 16]>,
    /// Logical sequence represented by physical handoff frame zero in this generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replication_sequence_base: Option<u64>,
    payload_format_version: u16,
}

impl GrpcRawIdentityFile {
    fn spool_identity(&self) -> SpoolJournalIdentity {
        SpoolJournalIdentity {
            cluster_id: self.cluster_id.clone(),
            origin_node_id: self.origin_node_id.clone(),
            source_id: self.source_id.clone(),
            journal_id: self.journal_id,
        }
    }

    fn replication_stream_id(&self) -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: self.cluster_id.clone(),
            origin_node_id: self.origin_node_id.clone(),
            source_id: self.source_id.clone(),
            journal_id: self.replication_journal_id.unwrap_or(self.journal_id),
        }
    }

    fn physical_stream_id(&self) -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: self.cluster_id.clone(),
            origin_node_id: self.origin_node_id.clone(),
            source_id: self.source_id.clone(),
            journal_id: self.journal_id,
        }
    }

    fn replication_sequence_base(&self) -> u64 {
        self.replication_sequence_base.unwrap_or(0)
    }

    fn replication_sequence(&self, frame_id: u64) -> Result<u64> {
        self.replication_sequence_base()
            .checked_add(frame_id)
            .context("raw gRPC logical replication sequence exhausted")
    }

    fn frame_id_for_replication_sequence(&self, sequence: u64) -> Result<u64> {
        sequence
            .checked_sub(self.replication_sequence_base())
            .with_context(|| {
                format!(
                    "logical replication sequence {sequence} predates generation base {}",
                    self.replication_sequence_base()
                )
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrpcRawReplicationGenerationAnchor {
    journal_id: [u8; 16],
    sequence_base: u64,
}

#[derive(Debug, Default)]
struct JournalState {
    records: u64,
    first: Option<GrpcRawHandoffRecord>,
    last: Option<GrpcRawHandoffRecord>,
}

#[derive(Debug)]
struct RawGrpcRelayPublisher {
    relay: YellowstoneBlockRelay,
    next_sequence: u64,
}

impl RawGrpcRelayPublisher {
    fn new(relay: YellowstoneBlockRelay) -> Self {
        Self {
            relay,
            next_sequence: 0,
        }
    }

    fn publish_after_durability(&mut self, update: &SubscribeUpdate) -> Result<()> {
        self.publish_owned_after_durability(update.clone())
    }

    fn publish_owned_after_durability(&mut self, update: SubscribeUpdate) -> Result<()> {
        let sequence = self.next_sequence;
        self.relay
            .publish_fsynced(sequence, update)
            .with_context(|| {
                format!("publish durable raw gRPC update to relay sequence {sequence}")
            })?;
        self.next_sequence = sequence
            .checked_add(1)
            .context("raw gRPC relay sequence overflow")?;
        Ok(())
    }
}

#[derive(Debug)]
struct RawGrpcRelayRuntime {
    publisher: RawGrpcRelayPublisher,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_task: Option<JoinHandle<Result<()>>>,
}

impl RawGrpcRelayRuntime {
    async fn start_if_enabled(
        config: &GrpcRawRecordConfig,
        identity: &GrpcRawIdentityFile,
        wal_dir: &Path,
        journal_path: &Path,
    ) -> Result<Option<Self>> {
        let (bind, token_file) = match (config.relay_bind, config.relay_x_token_file.as_deref()) {
            (None, None) => return Ok(None),
            (Some(bind), Some(token_file)) => (bind, token_file),
            (Some(_), None) => {
                return Err(anyhow!(
                    "--relay-bind requires a separate --relay-x-token-file"
                ));
            }
            (None, Some(_)) => {
                return Err(anyhow!("--relay-x-token-file requires --relay-bind"));
            }
        };

        let token = read_relay_x_token_file(token_file)?;
        let auth = YellowstoneRelayAuth::from_shared_x_token(token)
            .context("validate downstream relay x-token")?;
        let limits = YellowstoneBlockRelayLimits {
            max_records: config.relay_max_records,
            max_encoded_bytes: config.relay_max_encoded_bytes,
            max_clients: config.relay_max_clients,
        };
        let maximum_record_bytes = usize::try_from(config.max_record_bytes)
            .context("raw gRPC maximum record bytes exceeds relay address space")?;
        ensure!(
            limits.max_encoded_bytes >= maximum_record_bytes,
            "relay max encoded bytes {} must be at least raw gRPC max record bytes {} so every durably accepted block remains publishable",
            limits.max_encoded_bytes,
            config.max_record_bytes
        );
        let relay =
            YellowstoneBlockRelay::new(auth, limits).context("validate downstream relay limits")?;

        // Reserve the listener before any paid upstream transport is opened. A bad/occupied bind
        // therefore cannot create a second Yellowstone consumer as a side effect.
        let listener = TcpListener::bind(bind)
            .await
            .with_context(|| format!("bind downstream raw gRPC relay at {bind}"))?;
        let bound_address = listener
            .local_addr()
            .context("read downstream raw gRPC relay listener address")?;

        // Populate only the bounded tail of the active generation before the gRPC service starts
        // answering requests. Sequence numbers are process-local and intentionally unrelated to
        // generation frame ids, which reset when the active WAL rotates.
        let mut publisher = RawGrpcRelayPublisher::new(relay.clone());
        for row in read_relay_tail_rows(journal_path, limits)? {
            let update =
                read_verified_relay_update(wal_dir, identity, &row, config.max_record_bytes)?;
            publisher.publish_owned_after_durability(update)?;
        }
        let preloaded_records = publisher.next_sequence;

        let incoming = futures::stream::unfold(listener, |listener| async move {
            let item = listener.accept().await.map(|(socket, _peer)| socket);
            Some((item, listener))
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let service = relay
            .into_geyser_service()
            .send_compressed(CompressionEncoding::Zstd);
        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .context("serve downstream raw gRPC relay")
        });
        tracing::info!(
            bind = %bound_address,
            preloaded_records,
            max_records = limits.max_records,
            max_encoded_bytes = limits.max_encoded_bytes,
            max_clients = limits.max_clients,
            "downstream raw gRPC relay started from durable active-WAL tail"
        );
        Ok(Some(Self {
            publisher,
            shutdown_tx: Some(shutdown_tx),
            server_task: Some(server_task),
        }))
    }

    fn publish_after_durability(&mut self, update: &SubscribeUpdate) -> Result<()> {
        ensure!(
            !self
                .server_task
                .as_ref()
                .is_some_and(JoinHandle::is_finished),
            "downstream raw gRPC relay server stopped before durable publication"
        );
        self.publisher.publish_after_durability(update)
    }

    async fn shutdown(mut self) -> Result<()> {
        let close_result = self
            .publisher
            .relay
            .close()
            .context("close downstream raw gRPC relay subscriptions");
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(mut server_task) = self.server_task.take() {
            match tokio::time::timeout(
                Duration::from_secs(RELAY_SHUTDOWN_GRACE_SECS),
                &mut server_task,
            )
            .await
            {
                Ok(joined) => joined.context("join downstream raw gRPC relay server")??,
                Err(_) => {
                    server_task.abort();
                    let _ = server_task.await;
                    return Err(anyhow!(
                        "downstream raw gRPC relay did not stop within {RELAY_SHUTDOWN_GRACE_SECS} seconds"
                    ));
                }
            }
        }
        close_result?;
        Ok(())
    }
}

impl Drop for RawGrpcRelayRuntime {
    fn drop(&mut self) {
        let _ = self.publisher.relay.close();
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}

fn read_relay_x_token_file(path: &Path) -> Result<Vec<u8>> {
    let entry = fs::symlink_metadata(path)
        .with_context(|| format!("inspect downstream relay x-token file {}", path.display()))?;
    ensure!(
        !entry.file_type().is_symlink(),
        "downstream relay x-token file must not be a symlink"
    );
    ensure!(
        entry.file_type().is_file(),
        "downstream relay x-token path must be a regular file"
    );
    ensure!(
        entry.len() <= RELAY_X_TOKEN_MAX_BYTES,
        "downstream relay x-token file exceeds {RELAY_X_TOKEN_MAX_BYTES} bytes"
    );

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .with_context(|| format!("open downstream relay x-token file {}", path.display()))?;
    ensure!(
        file.metadata()?.file_type().is_file(),
        "downstream relay x-token path must remain a regular file"
    );
    let mut token = Vec::new();
    file.take(RELAY_X_TOKEN_MAX_BYTES + 1)
        .read_to_end(&mut token)
        .context("read downstream relay x-token file")?;
    ensure!(
        token.len() as u64 <= RELAY_X_TOKEN_MAX_BYTES,
        "downstream relay x-token file exceeds {RELAY_X_TOKEN_MAX_BYTES} bytes"
    );
    if token.last() == Some(&b'\n') {
        token.pop();
        if token.last() == Some(&b'\r') {
            token.pop();
        }
    }
    ensure!(
        !token.is_empty(),
        "downstream relay x-token must not be empty"
    );
    ensure!(
        token.iter().all(u8::is_ascii_graphic),
        "downstream relay x-token must contain visible ASCII without control characters"
    );
    Ok(token)
}

fn read_relay_tail_rows(
    journal_path: &Path,
    limits: YellowstoneBlockRelayLimits,
) -> Result<Vec<GrpcRawHandoffRecord>> {
    let file = File::open(journal_path)
        .with_context(|| format!("open raw gRPC journal {}", journal_path.display()))?;
    let mut rows = VecDeque::new();
    let mut retained_bytes = 0usize;
    let mut expected_frame_id = 0u64;
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| format!("read {}", journal_path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: GrpcRawHandoffRecord = serde_json::from_str(&line).with_context(|| {
            format!(
                "decode {} frame {expected_frame_id} for relay preload",
                journal_path.display()
            )
        })?;
        validate_handoff_sequence(&row, expected_frame_id)?;
        expected_frame_id = expected_frame_id
            .checked_add(1)
            .context("raw gRPC relay preload frame id overflow")?;
        let row_bytes = usize::try_from(row.uncompressed_len)
            .context("raw gRPC relay preload record length exceeds usize")?;
        retained_bytes = retained_bytes
            .checked_add(row_bytes)
            .context("raw gRPC relay preload byte accounting overflow")?;
        rows.push_back(row);
        while rows.len() > limits.max_records || retained_bytes > limits.max_encoded_bytes {
            let evicted = rows
                .pop_front()
                .context("raw gRPC relay preload queue unexpectedly empty")?;
            retained_bytes -= usize::try_from(evicted.uncompressed_len)
                .context("raw gRPC relay evicted record length exceeds usize")?;
        }
    }
    Ok(rows.into())
}

fn read_verified_relay_update(
    wal_dir: &Path,
    identity: &GrpcRawIdentityFile,
    row: &GrpcRawHandoffRecord,
    max_record_bytes: u64,
) -> Result<SubscribeUpdate> {
    ensure!(
        row.uncompressed_len <= max_record_bytes,
        "raw gRPC relay preload frame {} exceeds maximum record bytes",
        row.frame_id
    );
    let stored = read_spool_record(wal_dir, row.location(), max_record_bytes)?;
    ensure!(
        stored.metadata.observation.sequence == row.frame_id,
        "raw gRPC relay preload WAL/journal sequence mismatch at frame {}",
        row.frame_id
    );
    ensure!(
        stored.metadata.cluster_id == identity.cluster_id
            && stored.metadata.observation.origin_node_id == identity.origin_node_id
            && stored.metadata.observation.journal_id == identity.journal_id
            && stored.metadata.source_id == identity.source_id
            && stored.metadata.payload_format_version == PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
        "raw gRPC relay preload identity/format mismatch at frame {}",
        row.frame_id
    );
    ensure!(
        stored.payload.len() as u64 == row.compressed_len,
        "raw gRPC relay preload compressed length mismatch at frame {}",
        row.frame_id
    );
    let raw_capacity = usize::try_from(row.uncompressed_len)
        .context("raw gRPC relay preload length exceeds usize")?;
    let raw = zstd::bulk::decompress(&stored.payload, raw_capacity)
        .with_context(|| format!("decompress raw gRPC relay preload frame {}", row.frame_id))?;
    ensure!(
        raw.len() as u64 == row.uncompressed_len && sha256_hex(&raw) == row.protobuf_sha256,
        "raw gRPC relay preload payload verification failed at frame {}",
        row.frame_id
    );
    let update = SubscribeUpdate::decode(raw.as_slice())
        .with_context(|| format!("decode raw gRPC relay preload frame {}", row.frame_id))?;
    let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
        return Err(anyhow!(
            "raw gRPC relay preload frame {} is not a block update",
            row.frame_id
        ));
    };
    ensure!(
        block.slot == row.slot
            && block.parent_slot == row.parent_slot
            && block.blockhash == row.blockhash,
        "raw gRPC relay preload protobuf metadata mismatch at frame {}",
        row.frame_id
    );
    ensure!(
        stored.metadata.logical_key
            == (LogicalKey::Block {
                slot: block.slot,
                blockhash: decode_blockhash(&block.blockhash)?,
            }),
        "raw gRPC relay preload logical key mismatch at frame {}",
        row.frame_id
    );
    Ok(update)
}

fn cross_durable_relay_boundary<T, W, H, P>(
    append_wal_and_sync: W,
    append_handoff_and_sync: H,
    publish_relay: P,
) -> Result<T>
where
    W: FnOnce() -> Result<T>,
    H: FnOnce(&T) -> Result<()>,
    P: FnOnce() -> Result<()>,
{
    let durable = append_wal_and_sync()?;
    append_handoff_and_sync(&durable)?;
    publish_relay()?;
    Ok(durable)
}

pub async fn record_grpc_raw_blocks(config: GrpcRawRecordConfig) -> Result<GrpcRawRecordReport> {
    let mut relay_runtime = None;
    let capture_result = record_grpc_raw_blocks_inner(config, &mut relay_runtime).await;
    let shutdown_result = match relay_runtime.take() {
        Some(runtime) => runtime.shutdown().await,
        None => Ok(()),
    };
    match (capture_result, shutdown_result) {
        (Ok(report), Ok(())) => Ok(report),
        (Ok(_), Err(shutdown)) => Err(shutdown),
        (Err(capture), Ok(())) => Err(capture),
        (Err(capture), Err(shutdown)) => Err(capture.context(format!(
            "downstream relay shutdown also failed: {shutdown:#}"
        ))),
    }
}

async fn record_grpc_raw_blocks_inner(
    config: GrpcRawRecordConfig,
    relay_runtime: &mut Option<RawGrpcRelayRuntime>,
) -> Result<GrpcRawRecordReport> {
    ensure!(
        config.slots_per_epoch > 0,
        "slots per epoch must be non-zero"
    );
    ensure!(
        config.max_record_bytes > 0,
        "max record bytes must be non-zero"
    );
    ensure!(
        config.segment_target_bytes > 0,
        "segment target bytes must be non-zero"
    );
    validate_generation_limit(&config)?;
    fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("create raw gRPC output {}", config.output_dir.display()))?;
    validate_hot_generation_config(&config)?;
    prepare_resume_coverage_warning_path(&config)?;
    let mut identity = load_or_create_identity(&config)?;
    let spool_options = SpoolOptions {
        segment_target_bytes: config.segment_target_bytes,
        max_record_bytes: config.max_record_bytes,
    };
    let wal_root = config.output_dir.join(WAL_ROOT_DIR);
    let mut spool = SpoolWriter::open(&wal_root, identity.spool_identity(), spool_options)?;
    let journal_path = config.output_dir.join(HANDOFF_JOURNAL_FILE);
    let mut journal_state = recover_handoff_journal(&journal_path)?;
    let recovered_handoff_record = reconcile_handoff_tail(
        &mut spool,
        &journal_path,
        &mut journal_state,
        config.slots_per_epoch,
        config.max_record_bytes,
    )?;
    let generation_bytes = raw_generation_bytes(
        &config.output_dir.join(IDENTITY_FILE),
        &wal_root,
        &journal_path,
    )?;

    // Keep the durable tail as the audit anchor even when a recovery floor advances the actual
    // subscription. The first post-floor block must still publish the explicit coverage warning.
    let resume_anchor = journal_state.last.clone();
    let last_durable_slot = resume_anchor.as_ref().map(|row| row.slot);
    let effective_from_slot =
        effective_resume_slot(config.from_slot, last_durable_slot, config.min_resume_slot);
    // A continuous spool may already span many epochs. On restart, an optional boundary stop is
    // relative to the epoch containing the durable tail, not the first epoch ever recorded.
    let mut capture_epoch = journal_state.last.as_ref().map(|row| row.epoch);
    let mut next_frame_id = journal_state.records;
    let mut durable_tail_row = journal_state.last.clone();

    let started_at = Instant::now();
    let mut report = GrpcRawRecordReport {
        endpoint: config.endpoint.clone(),
        output_dir: config.output_dir.clone(),
        wal_dir: spool.journal_dir().to_path_buf(),
        journal_path: journal_path.clone(),
        existing_frames: journal_state.records,
        frames_seen: 0,
        frames_written: 0,
        frames_skipped_before_resume: 0,
        recovered_handoff_record,
        requested_from_slot: config.from_slot,
        minimum_resume_slot: config.min_resume_slot,
        effective_from_slot,
        resume_overlap_slot: last_durable_slot,
        resume_overlap_observed: resume_anchor.as_ref().map(|_| false),
        first_delivered_slot: None,
        resume_coverage_warning: false,
        resume_coverage_warning_publication_failed: false,
        first_slot: None,
        last_slot: None,
        first_epoch: None,
        last_epoch: None,
        raw_bytes_written: 0,
        compressed_bytes_written: 0,
        compression_ratio: 0.0,
        complete_poh_required: config.require_complete_poh,
        poh_blocks_verified: 0,
        poh_entries_verified: 0,
        poh_transactions_verified: 0,
        poh_num_hashes_verified: "0".to_string(),
        elapsed_ms: 0,
        timed_out: false,
        idle_timed_out: false,
        stream_ended: false,
        stopped_at_epoch_boundary: false,
        outcome: GrpcRawRecordOutcome::Retryable,
        retry_reason: None,
        action_required: false,
        replay_unavailable: false,
        replay_unavailable_requested_slot: None,
        replay_available_slot: None,
        stopped_generation_full: false,
        generations_rotated: 0,
        stopped_low_disk: false,
        available_bytes_at_stop: None,
        generation_bytes,
    };
    if config.max_generation_bytes > 0 && report.generation_bytes >= config.max_generation_bytes {
        if let (Some(cache_root), Some(source_tail)) = (
            config.hot_generation_root.as_deref(),
            durable_tail_row.as_ref(),
        ) {
            let rotated =
                hot_rotate_generation(&config, cache_root, &mut identity, &mut spool, source_tail)?;
            report.generation_bytes = rotated.generation_bytes;
            report.generations_rotated = 1;
            report.wal_dir = spool.journal_dir().to_path_buf();
            next_frame_id = rotated.next_frame_id;
            durable_tail_row = Some(rotated.seeded_tail);
        } else {
            report.stopped_generation_full = true;
            apply_raw_record_retry(
                &mut report,
                RawRecordRetry::new(GrpcRawRecordRetryReason::GenerationFull),
            );
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
    }
    if let Some(available) = low_disk_bytes(&config.output_dir, config.min_free_bytes)? {
        report.stopped_low_disk = true;
        report.available_bytes_at_stop = Some(available);
        apply_raw_record_retry(
            &mut report,
            RawRecordRetry::new(GrpcRawRecordRetryReason::LowDisk),
        );
        report.elapsed_ms = started_at.elapsed().as_millis();
        return Ok(report);
    }

    *relay_runtime = RawGrpcRelayRuntime::start_if_enabled(
        &config,
        &identity,
        spool.journal_dir(),
        &journal_path,
    )
    .await?;

    let watchdog_started_at = TokioInstant::now();
    let deadline = deadline_after(
        watchdog_started_at,
        config.timeout_secs,
        "raw gRPC total timeout",
    )?;
    let mut idle_deadline = idle_deadline_after(watchdog_started_at, config.idle_timeout_secs)?;
    let mut client = match await_with_watchdogs(
        connect_grpc_with_max_decoding_message_size(&config.endpoint, config.max_record_bytes),
        deadline,
        idle_deadline,
    )
    .await
    {
        WatchdogOutcome::Completed(Ok(client)) => client,
        WatchdogOutcome::Completed(Err(error)) => {
            let retry = classify_raw_record_error(&error, GrpcRawRecordRetryReason::ConnectError);
            tracing::warn!(
                retry_reason = ?retry.reason,
                action_required = retry.action_required,
                error = %error,
                "raw gRPC connection ended before subscription"
            );
            apply_raw_record_retry(&mut report, retry);
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
        WatchdogOutcome::TotalTimeout => {
            report.timed_out = true;
            apply_raw_record_retry(
                &mut report,
                RawRecordRetry::new(GrpcRawRecordRetryReason::TotalTimeout),
            );
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
        WatchdogOutcome::IdleTimeout => {
            report.idle_timed_out = true;
            apply_raw_record_retry(
                &mut report,
                RawRecordRetry::new(GrpcRawRecordRetryReason::IdleTimeout),
            );
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
    };
    let request = SubscribeRequest {
        blocks: HashMap::from([(
            "raw-blocks".to_string(),
            SubscribeRequestFilterBlocks {
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        from_slot: effective_from_slot,
        ..Default::default()
    };
    // Keep the request side of Yellowstone's bidirectional subscription alive. The server emits
    // application-level Ping updates for clients to answer; dropping this sink (as
    // `subscribe_once` does) prevents those replies even while the response stream is healthy.
    let (mut request_sink, request_stream) =
        mpsc::channel::<SubscribeRequest>(SUBSCRIBE_REQUEST_CHANNEL_CAPACITY);
    match await_with_watchdogs(request_sink.send(request), deadline, idle_deadline).await {
        WatchdogOutcome::Completed(result) => {
            if let Err(error) = result {
                tracing::warn!(
                    error = %error,
                    "raw gRPC request side closed before subscription"
                );
                apply_raw_record_retry(
                    &mut report,
                    RawRecordRetry::new(GrpcRawRecordRetryReason::SubscribeError),
                );
                report.elapsed_ms = started_at.elapsed().as_millis();
                return Ok(report);
            }
        }
        WatchdogOutcome::TotalTimeout => {
            report.timed_out = true;
            apply_raw_record_retry(
                &mut report,
                RawRecordRetry::new(GrpcRawRecordRetryReason::TotalTimeout),
            );
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
        WatchdogOutcome::IdleTimeout => {
            report.idle_timed_out = true;
            apply_raw_record_retry(
                &mut report,
                RawRecordRetry::new(GrpcRawRecordRetryReason::IdleTimeout),
            );
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
    }
    let response = match await_with_watchdogs(
        client.geyser.subscribe(request_stream),
        deadline,
        idle_deadline,
    )
    .await
    {
        WatchdogOutcome::Completed(Ok(response)) => response,
        WatchdogOutcome::Completed(Err(status)) => {
            if mark_replay_unavailable(&mut report, &status) {
                report.elapsed_ms = started_at.elapsed().as_millis();
                return Ok(report);
            }
            let retry =
                classify_raw_record_status(&status, GrpcRawRecordRetryReason::SubscribeError);
            tracing::warn!(
                grpc_code = ?status.code(),
                retry_reason = ?retry.reason,
                action_required = retry.action_required,
                "raw gRPC subscription was rejected"
            );
            apply_raw_record_retry(&mut report, retry);
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
        WatchdogOutcome::TotalTimeout => {
            report.timed_out = true;
            apply_raw_record_retry(
                &mut report,
                RawRecordRetry::new(GrpcRawRecordRetryReason::TotalTimeout),
            );
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
        WatchdogOutcome::IdleTimeout => {
            report.idle_timed_out = true;
            apply_raw_record_retry(
                &mut report,
                RawRecordRetry::new(GrpcRawRecordRetryReason::IdleTimeout),
            );
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
    };
    let response_encoding = grpc_response_encoding(response.metadata());
    tracing::info!(
        grpc_response_encoding = response_encoding.as_str(),
        "raw gRPC subscription opened"
    );
    let mut stream = response.into_inner();
    let max_blocks = config.max_blocks.max(1) as u64;
    let mut raw = Vec::with_capacity(2 * 1024 * 1024);
    let mut compressed = Vec::with_capacity(1024 * 1024);
    let mut compressor = zstd::bulk::Compressor::new(config.compression_level)
        .context("create raw gRPC zstd compressor")?;
    let mut poh_num_hashes_verified = 0u128;
    let mut request_side_open = true;

    'capture: while report.frames_written < max_blocks {
        let update = match next_with_disk_admission(
            &mut stream,
            || low_disk_bytes(&config.output_dir, config.min_free_bytes),
            deadline,
            idle_deadline,
        )
        .await?
        {
            DiskAdmittedOutcome::LowDisk(available) => {
                // This branch is reached before polling: once the stream delivers a block, this
                // process must either durably append it or retain it across a rollover.
                report.stopped_low_disk = true;
                report.available_bytes_at_stop = Some(available);
                apply_raw_record_retry(
                    &mut report,
                    RawRecordRetry::new(GrpcRawRecordRetryReason::LowDisk),
                );
                break;
            }
            DiskAdmittedOutcome::Admitted(WatchdogOutcome::Completed(update)) => update,
            DiskAdmittedOutcome::Admitted(WatchdogOutcome::TotalTimeout) => {
                report.timed_out = true;
                apply_raw_record_retry(
                    &mut report,
                    RawRecordRetry::new(GrpcRawRecordRetryReason::TotalTimeout),
                );
                break;
            }
            DiskAdmittedOutcome::Admitted(WatchdogOutcome::IdleTimeout) => {
                report.idle_timed_out = true;
                apply_raw_record_retry(
                    &mut report,
                    RawRecordRetry::new(GrpcRawRecordRetryReason::IdleTimeout),
                );
                break;
            }
        };
        let Some(update) = update else {
            report.stream_ended = true;
            apply_raw_record_retry(
                &mut report,
                RawRecordRetry::new(GrpcRawRecordRetryReason::StreamEof),
            );
            break;
        };
        let update = match update {
            Ok(update) => update,
            Err(status) => {
                if mark_replay_unavailable(&mut report, &status) {
                    break;
                }
                let retry =
                    classify_raw_record_status(&status, GrpcRawRecordRetryReason::StreamError);
                tracing::warn!(
                    grpc_code = ?status.code(),
                    retry_reason = ?retry.reason,
                    action_required = retry.action_required,
                    "raw gRPC subscription stream failed"
                );
                apply_raw_record_retry(&mut report, retry);
                break;
            }
        };
        if matches!(update.update_oneof.as_ref(), Some(UpdateOneof::Ping(_))) {
            match reply_to_subscription_ping(
                &mut request_side_open,
                &mut request_sink,
                deadline,
                idle_deadline,
            )
            .await
            {
                SubscribePingReplyOutcome::Sent | SubscribePingReplyOutcome::AlreadyClosed => {}
                SubscribePingReplyOutcome::RequestSideClosed => {
                    tracing::warn!(
                        "raw gRPC request side closed while response stream remains readable; draining response"
                    );
                }
                SubscribePingReplyOutcome::TotalTimeout => {
                    report.timed_out = true;
                    apply_raw_record_retry(
                        &mut report,
                        RawRecordRetry::new(GrpcRawRecordRetryReason::TotalTimeout),
                    );
                    break;
                }
                SubscribePingReplyOutcome::IdleTimeout => {
                    report.idle_timed_out = true;
                    apply_raw_record_retry(
                        &mut report,
                        RawRecordRetry::new(GrpcRawRecordRetryReason::IdleTimeout),
                    );
                    break;
                }
            }
            continue;
        }
        let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
            continue;
        };
        report.frames_seen += 1;
        report.first_delivered_slot.get_or_insert(block.slot);
        if effective_from_slot.is_some_and(|from_slot| block.slot < from_slot) {
            report.frames_skipped_before_resume += 1;
            continue;
        }
        if let Some(anchor) = resume_anchor.as_ref() {
            if block.slot == anchor.slot {
                report.resume_overlap_observed = Some(true);
                if block.blockhash == anchor.blockhash {
                    report.frames_skipped_before_resume += 1;
                    continue;
                }
            } else if block.slot > anchor.slot
                && report.resume_overlap_observed == Some(false)
                && !report.resume_coverage_warning
            {
                report.resume_coverage_warning = true;
                if let Some(path) = config.resume_coverage_warning_file.as_deref() {
                    let first_delivered_slot = report.first_delivered_slot.unwrap_or(block.slot);
                    let event = GrpcRawResumeCoverageWarning {
                        event_id: resume_coverage_warning_event_id(
                            anchor.slot,
                            first_delivered_slot,
                            block.slot,
                        ),
                        schema_version: RESUME_COVERAGE_WARNING_SCHEMA_VERSION,
                        requested_overlap_slot: anchor.slot,
                        first_delivered_slot,
                        observed_later_slot: block.slot,
                        written_unix_secs: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map_or(0, |duration| duration.as_secs()),
                    };
                    match publish_resume_coverage_warning(path, &event) {
                        Ok(ResumeCoverageWarningPublishOutcome::DifferentPending) => {
                            tracing::warn!(
                                warning_file = %path.display(),
                                "coalesced raw gRPC resume-coverage warning into the pending incident"
                            );
                        }
                        Ok(
                            ResumeCoverageWarningPublishOutcome::Published
                            | ResumeCoverageWarningPublishOutcome::AlreadyPresent,
                        ) => {}
                        Err(error) => {
                            report.resume_coverage_warning_publication_failed = true;
                            apply_raw_record_retry(
                                &mut report,
                                RawRecordRetry::new(
                                    GrpcRawRecordRetryReason::ResumeCoverageWarningPublicationFailed,
                                ),
                            );
                            tracing::error!(
                                warning_file = %path.display(),
                                error = %error,
                                "failed to publish raw gRPC resume-coverage warning event"
                            );
                            break;
                        }
                    }
                }
                tracing::warn!(
                    requested_overlap_slot = anchor.slot,
                    first_delivered_slot = report.first_delivered_slot.unwrap_or(block.slot),
                    delivered_slot = block.slot,
                    "provider did not deliver the inclusive resume slot; audit source coverage"
                );
            }
        }
        let epoch_slot = EpochSlot::from_slot(block.slot, config.slots_per_epoch);
        if config.stop_at_epoch_boundary
            && capture_epoch.is_some_and(|epoch| epoch != epoch_slot.epoch)
        {
            report.stopped_at_epoch_boundary = true;
            report.outcome = GrpcRawRecordOutcome::EpochBoundary;
            report.retry_reason = None;
            report.action_required = false;
            break;
        }
        capture_epoch.get_or_insert(epoch_slot.epoch);
        let poh_stats = config
            .require_complete_poh
            .then(|| {
                validate_complete_poh_block(block)
                    .with_context(|| format!("validate complete PoH for slot {}", block.slot))
            })
            .transpose()?;

        raw.clear();
        update
            .encode(&mut raw)
            .context("encode raw gRPC protobuf update")?;
        ensure!(
            raw.len() as u64 <= config.max_record_bytes,
            "raw gRPC block at slot {} is {} bytes, over configured maximum {}",
            block.slot,
            raw.len(),
            config.max_record_bytes
        );
        compressed.clear();
        compress_raw_block(
            &mut compressor,
            &raw,
            &mut compressed,
            config.max_record_bytes,
        )
        .with_context(|| format!("compress raw gRPC protobuf block at slot {}", block.slot))?;
        let blockhash_bytes = decode_blockhash(&block.blockhash)?;
        let protobuf_sha256 = sha256_hex(&raw);
        loop {
            let frame_id = next_frame_id;
            let metadata = IngressRecordMeta::from_payload(
                identity.cluster_id.clone(),
                ObservationId {
                    origin_node_id: identity.origin_node_id.clone(),
                    journal_id: identity.journal_id,
                    sequence: frame_id,
                },
                identity.source_id.clone(),
                LogicalKey::Block {
                    slot: block.slot,
                    blockhash: blockhash_bytes,
                },
                PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
                &compressed,
            );
            // `append_and_sync` is the durability boundary. No cursor, journal, or report state
            // may advance before it succeeds.
            let projection = spool.project_append(&metadata, &compressed)?;
            let row = handoff_record_from_block(
                block,
                frame_id,
                epoch_slot,
                projection.location,
                raw.len() as u64,
                compressed.len() as u64,
                protobuf_sha256.clone(),
            );
            let projected_generation_bytes = projected_raw_generation_bytes(
                report.generation_bytes,
                projection.additional_bytes,
                &row,
            )?;
            if generation_limit_exceeded(config.max_generation_bytes, projected_generation_bytes) {
                let (Some(cache_root), Some(source_tail)) = (
                    config.hot_generation_root.as_deref(),
                    durable_tail_row.as_ref(),
                ) else {
                    report.stopped_generation_full = true;
                    apply_raw_record_retry(
                        &mut report,
                        RawRecordRetry::new(GrpcRawRecordRetryReason::GenerationFull),
                    );
                    break 'capture;
                };
                let rotated = hot_rotate_generation(
                    &config,
                    cache_root,
                    &mut identity,
                    &mut spool,
                    source_tail,
                )?;
                report.generation_bytes = rotated.generation_bytes;
                report.generations_rotated = report
                    .generations_rotated
                    .checked_add(1)
                    .context("raw gRPC hot generation rotation count overflow")?;
                report.wal_dir = spool.journal_dir().to_path_buf();
                next_frame_id = rotated.next_frame_id;
                durable_tail_row = Some(rotated.seeded_tail);
                // Rebuild metadata with the successor journal identity and append this already
                // decoded block without reading another item from the Yellowstone stream.
                continue;
            }

            cross_durable_relay_boundary(
                || spool.append_and_sync(metadata, &compressed),
                |durable| {
                    ensure!(
                        durable.location() == projection.location,
                        "spool append location differed from its preflight projection"
                    );
                    append_handoff_record(&journal_path, &row)
                },
                || match relay_runtime.as_mut() {
                    Some(runtime) => runtime.publish_after_durability(&update),
                    None => Ok(()),
                },
            )?;
            report.generation_bytes = projected_generation_bytes;
            next_frame_id = next_frame_id
                .checked_add(1)
                .context("raw gRPC frame id overflow")?;
            durable_tail_row = Some(row);
            break;
        }
        idle_deadline = idle_deadline_after(TokioInstant::now(), config.idle_timeout_secs)?;

        if let Some(stats) = poh_stats {
            report.poh_blocks_verified = report
                .poh_blocks_verified
                .checked_add(1)
                .context("verified PoH block count overflow")?;
            report.poh_entries_verified = report
                .poh_entries_verified
                .checked_add(stats.entries)
                .context("verified PoH entry count overflow")?;
            report.poh_transactions_verified = report
                .poh_transactions_verified
                .checked_add(stats.transaction_references)
                .context("verified PoH transaction count overflow")?;
            poh_num_hashes_verified = poh_num_hashes_verified
                .checked_add(stats.num_hashes)
                .context("verified PoH hash count overflow")?;
        }

        report.first_slot.get_or_insert(block.slot);
        report.last_slot = Some(block.slot);
        report.first_epoch.get_or_insert(epoch_slot.epoch);
        report.last_epoch = Some(epoch_slot.epoch);
        report.frames_written += 1;
        report.raw_bytes_written = report
            .raw_bytes_written
            .checked_add(raw.len() as u64)
            .context("raw byte counter overflow")?;
        report.compressed_bytes_written = report
            .compressed_bytes_written
            .checked_add(compressed.len() as u64)
            .context("compressed byte counter overflow")?;
        if let Some(available) = low_disk_bytes(&config.output_dir, config.min_free_bytes)? {
            // Any cap-triggering block is now durable in the successor. Stop before polling the
            // next update so disk pressure cannot create an acknowledged coverage hole.
            report.stopped_low_disk = true;
            report.available_bytes_at_stop = Some(available);
            apply_raw_record_retry(
                &mut report,
                RawRecordRetry::new(GrpcRawRecordRetryReason::LowDisk),
            );
            break;
        }
    }

    if report.retry_reason.is_none()
        && report.outcome == GrpcRawRecordOutcome::Retryable
        && report.frames_written >= max_blocks
    {
        apply_raw_record_retry(
            &mut report,
            RawRecordRetry::new(GrpcRawRecordRetryReason::MaxBlocks),
        );
    }

    report.elapsed_ms = started_at.elapsed().as_millis();
    report.compression_ratio =
        compression_ratio(report.raw_bytes_written, report.compressed_bytes_written);
    report.poh_num_hashes_verified = poh_num_hashes_verified.to_string();
    Ok(report)
}

/// Stream and validate a raw spool one record at a time. The callback receives the exact decoded
/// protobuf update envelope needed by a future archive rebuild/import pass.
pub fn replay_grpc_raw_blocks<F>(
    output_dir: impl AsRef<Path>,
    max_record_bytes: u64,
    visit: F,
) -> Result<u64>
where
    F: FnMut(&GrpcRawHandoffRecord, SubscribeUpdate) -> Result<()>,
{
    Ok(replay_grpc_raw_blocks_audited(output_dir, max_record_bytes, visit)?.records)
}

/// Read the complete, newline-committed handoff prefix without taking the WAL writer lock.
///
/// The handoff file length is snapshotted once. A final partial row is reported and ignored, and a
/// WAL record that has not yet crossed the handoff boundary is invisible. Every visited row is
/// independently verified by [`read_spool_record`], bounded-decompressed, SHA-checked, and prost
/// decoded before it is exposed. This works for both active and stopped generation directories and
/// never opens or requires the spool's exclusive `writer.lock`.
pub fn read_grpc_raw_committed_records<F>(
    output_dir: impl AsRef<Path>,
    limits: GrpcRawCommittedReadLimits,
    mut visit: F,
) -> Result<GrpcRawCommittedReadReport>
where
    F: FnMut(GrpcRawCommittedRecord) -> Result<()>,
{
    let limits = limits.validate()?;
    let output_dir = output_dir.as_ref();
    let identity = read_identity(&output_dir.join(IDENTITY_FILE))?;
    let wal_dir = spool_journal_dir(output_dir, &identity);
    let journal_path = output_dir.join(HANDOFF_JOURNAL_FILE);
    let file = File::open(&journal_path)
        .with_context(|| format!("open raw gRPC journal {}", journal_path.display()))?;
    let snapshot_len = file
        .metadata()
        .with_context(|| format!("inspect raw gRPC journal {}", journal_path.display()))?
        .len();
    let mut reader = BufReader::new(file.take(snapshot_len));
    let mut line = Vec::new();
    let mut records = 0u64;
    let mut incomplete_handoff_tail_bytes = 0u64;

    loop {
        line.clear();
        let bytes = {
            let mut bounded = (&mut reader).take(MAX_HANDOFF_RECORD_BYTES + 1);
            bounded
                .read_until(b'\n', &mut line)
                .with_context(|| format!("read raw gRPC journal {}", journal_path.display()))?
        };
        if bytes == 0 {
            break;
        }
        let bytes = u64::try_from(bytes).context("raw gRPC handoff row length exceeds u64")?;
        ensure!(
            bytes <= MAX_HANDOFF_RECORD_BYTES,
            "raw gRPC handoff row exceeds {MAX_HANDOFF_RECORD_BYTES} bytes"
        );
        if !line.ends_with(b"\n") {
            incomplete_handoff_tail_bytes = bytes;
            break;
        }
        let payload = &line[..line.len() - 1];
        if payload.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let row: GrpcRawHandoffRecord = serde_json::from_slice(payload)
            .with_context(|| format!("decode raw gRPC journal frame {records}"))?;
        validate_handoff_sequence(&row, records)?;
        let record = read_verified_grpc_raw_committed_record(&wal_dir, &identity, &row, limits)?;
        visit(record).with_context(|| format!("visit committed raw gRPC frame {records}"))?;
        records = records
            .checked_add(1)
            .context("committed raw gRPC record count overflow")?;
    }

    Ok(GrpcRawCommittedReadReport {
        records,
        incomplete_handoff_tail_bytes,
    })
}

fn read_verified_grpc_raw_committed_record(
    wal_dir: &Path,
    identity: &GrpcRawIdentityFile,
    row: &GrpcRawHandoffRecord,
    limits: GrpcRawCommittedReadLimits,
) -> Result<GrpcRawCommittedRecord> {
    ensure!(
        row.compressed_len <= limits.max_compressed_record_bytes,
        "committed raw gRPC frame {} compressed length {} exceeds maximum {}",
        row.frame_id,
        row.compressed_len,
        limits.max_compressed_record_bytes
    );
    ensure!(
        row.uncompressed_len <= limits.max_uncompressed_record_bytes,
        "committed raw gRPC frame {} uncompressed length {} exceeds maximum {}",
        row.frame_id,
        row.uncompressed_len,
        limits.max_uncompressed_record_bytes
    );
    let stored = read_spool_record(wal_dir, row.location(), limits.max_compressed_record_bytes)?;
    ensure!(
        stored.metadata.observation.sequence == row.frame_id,
        "committed raw gRPC WAL/handoff sequence mismatch at frame {}",
        row.frame_id
    );
    ensure!(
        stored.metadata.cluster_id == identity.cluster_id
            && stored.metadata.observation.origin_node_id == identity.origin_node_id
            && stored.metadata.observation.journal_id == identity.journal_id
            && stored.metadata.source_id == identity.source_id
            && stored.metadata.payload_format_version == PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
        "committed raw gRPC WAL identity/format mismatch at frame {}",
        row.frame_id
    );
    ensure!(
        stored.payload.len() as u64 == row.compressed_len,
        "committed raw gRPC compressed length mismatch at frame {}",
        row.frame_id
    );
    let raw_capacity = usize::try_from(row.uncompressed_len)
        .context("committed raw gRPC uncompressed length exceeds usize")?;
    let raw = zstd::bulk::decompress(&stored.payload, raw_capacity)
        .with_context(|| format!("decompress committed raw gRPC frame {}", row.frame_id))?;
    ensure!(
        raw.len() as u64 == row.uncompressed_len,
        "committed raw gRPC uncompressed length mismatch at frame {}",
        row.frame_id
    );
    ensure!(
        sha256_hex(&raw) == row.protobuf_sha256,
        "committed raw gRPC protobuf checksum mismatch at frame {}",
        row.frame_id
    );
    let update = SubscribeUpdate::decode(raw.as_slice())
        .with_context(|| format!("decode committed raw gRPC protobuf frame {}", row.frame_id))?;
    let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
        return Err(anyhow!(
            "committed raw gRPC frame {} is not a block update",
            row.frame_id
        ));
    };
    ensure!(
        block.slot == row.slot
            && block.parent_slot == row.parent_slot
            && block.blockhash == row.blockhash,
        "committed raw gRPC protobuf metadata mismatch at frame {}",
        row.frame_id
    );
    ensure!(
        stored.metadata.logical_key
            == (LogicalKey::Block {
                slot: block.slot,
                blockhash: decode_blockhash(&block.blockhash)?,
            }),
        "committed raw gRPC logical key mismatch at frame {}",
        row.frame_id
    );

    let raw_protobuf_digest: [u8; 32] = Sha256::digest(&raw).into();
    let durable_local_record =
        DurableSpoolRecord::from_verified_committed_read(stored.location, stored.metadata.clone());
    Ok(GrpcRawCommittedRecord {
        cluster_id: stored.metadata.cluster_id.clone(),
        source_id: stored.metadata.source_id.clone(),
        observation: stored.metadata.observation.clone(),
        slot: block.slot,
        blockhash: block.blockhash.clone(),
        content_digest: stored.metadata.content_digest,
        payload_format_version: stored.metadata.payload_format_version,
        raw_protobuf_sha256: row.protobuf_sha256.clone(),
        uncompressed_bytes: row.uncompressed_len,
        compressed_bytes: stored.payload,
        update,
        raw_protobuf_digest,
        durable_local_record,
    })
}

/// Verify one committed WAL/handoff row for replication without building a Prost object graph.
/// The sender needs the exact compressed envelope plus its raw digest, not a decoded block. This
/// keeps peak memory bounded by the compressed batch and one `uncompressed_len` digest buffer even
/// for protobufs containing adversarially many tiny repeated messages.
fn read_verified_grpc_raw_replication_record(
    wal_dir: &Path,
    identity: &GrpcRawIdentityFile,
    row: &GrpcRawHandoffRecord,
    limits: GrpcRawCommittedReadLimits,
) -> Result<GrpcRawDurableReplicationRecord> {
    ensure!(
        row.compressed_len <= limits.max_compressed_record_bytes,
        "committed raw gRPC frame {} compressed length {} exceeds maximum {}",
        row.frame_id,
        row.compressed_len,
        limits.max_compressed_record_bytes
    );
    ensure!(
        row.uncompressed_len <= limits.max_uncompressed_record_bytes,
        "committed raw gRPC frame {} uncompressed length {} exceeds maximum {}",
        row.frame_id,
        row.uncompressed_len,
        limits.max_uncompressed_record_bytes
    );
    let stored = read_spool_record(wal_dir, row.location(), limits.max_compressed_record_bytes)?;
    ensure!(
        stored.metadata.observation.sequence == row.frame_id,
        "committed raw gRPC WAL/handoff sequence mismatch at frame {}",
        row.frame_id
    );
    ensure!(
        stored.metadata.cluster_id == identity.cluster_id
            && stored.metadata.observation.origin_node_id == identity.origin_node_id
            && stored.metadata.observation.journal_id == identity.journal_id
            && stored.metadata.source_id == identity.source_id
            && stored.metadata.payload_format_version == PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
        "committed raw gRPC WAL identity/format mismatch at frame {}",
        row.frame_id
    );
    ensure!(
        stored.payload.len() as u64 == row.compressed_len,
        "committed raw gRPC compressed length mismatch at frame {}",
        row.frame_id
    );
    let logical_key = LogicalKey::Block {
        slot: row.slot,
        blockhash: decode_blockhash(&row.blockhash)?,
    };
    ensure!(
        stored.metadata.logical_key == logical_key,
        "committed raw gRPC logical key mismatch at frame {}",
        row.frame_id
    );
    let raw_capacity = usize::try_from(row.uncompressed_len)
        .context("committed raw gRPC uncompressed length exceeds usize")?;
    let raw_protobuf_digest = {
        let raw = zstd::bulk::decompress(&stored.payload, raw_capacity)
            .with_context(|| format!("decompress committed raw gRPC frame {}", row.frame_id))?;
        ensure!(
            raw.len() as u64 == row.uncompressed_len,
            "committed raw gRPC uncompressed length mismatch at frame {}",
            row.frame_id
        );
        let digest: [u8; 32] = Sha256::digest(&raw).into();
        ensure!(
            hex_bytes(&digest) == row.protobuf_sha256.to_ascii_lowercase(),
            "committed raw gRPC protobuf checksum mismatch at frame {}",
            row.frame_id
        );
        digest
    };
    let stream = identity.replication_stream_id();
    let replication_sequence = identity.replication_sequence(row.frame_id)?;
    let durable_local_record =
        DurableSpoolRecord::from_verified_committed_read(stored.location, stored.metadata.clone());
    let durable_replication_witness = DurableReplicationWitness::from_verified_mapping(
        &durable_local_record,
        stream.clone(),
        replication_sequence,
        stored.metadata.content_digest,
    )?;
    let record = RawReplicationRecord {
        offer: ReplicationOffer {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            cluster_id: stream.cluster_id.clone(),
            record: ObservationId {
                origin_node_id: stream.origin_node_id.clone(),
                journal_id: stream.journal_id,
                sequence: replication_sequence,
            },
            source_id: stream.source_id.clone(),
            logical_key,
            content_digest: stored.metadata.content_digest,
            payload_len: stored.payload.len() as u64,
            payload_format_version: stored.metadata.payload_format_version,
            commitment: CommitmentEvidence::Confirmed,
        },
        compressed_payload: stored.payload,
        raw_protobuf_sha256: raw_protobuf_digest,
        uncompressed_len: row.uncompressed_len,
    };
    Ok(GrpcRawDurableReplicationRecord {
        stream,
        record,
        durable_local_record,
        durable_replication_witness,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GrpcRawReplayAudit {
    records: u64,
    wal_incomplete_tail_bytes: u64,
    identity_sha256: String,
    handoff_journal_sha256: String,
    tail: Option<GrpcRawHandoffRecord>,
}

fn replay_grpc_raw_blocks_audited<F>(
    output_dir: impl AsRef<Path>,
    max_record_bytes: u64,
    visit: F,
) -> Result<GrpcRawReplayAudit>
where
    F: FnMut(&GrpcRawHandoffRecord, SubscribeUpdate) -> Result<()>,
{
    let output_dir = output_dir.as_ref();
    let identity = read_identity(&output_dir.join(IDENTITY_FILE))?;
    let wal_dir = spool_journal_dir(output_dir, &identity);
    let spool_audit = LockedSpoolAudit::open(
        output_dir.join(WAL_ROOT_DIR),
        identity.spool_identity(),
        SpoolOptions {
            max_record_bytes,
            ..SpoolOptions::default()
        },
    )
    .context("lock and audit raw gRPC WAL")?;
    ensure!(
        spool_audit.journal_dir() == wal_dir,
        "raw gRPC WAL audit resolved a different journal directory"
    );
    replay_grpc_raw_blocks_with_audit(
        output_dir,
        max_record_bytes,
        &identity,
        &wal_dir,
        &spool_audit,
        visit,
    )
}

fn replay_grpc_raw_blocks_with_audit<F>(
    output_dir: &Path,
    max_record_bytes: u64,
    identity: &GrpcRawIdentityFile,
    wal_dir: &Path,
    spool_audit: &LockedSpoolAudit,
    mut visit: F,
) -> Result<GrpcRawReplayAudit>
where
    F: FnMut(&GrpcRawHandoffRecord, SubscribeUpdate) -> Result<()>,
{
    let wal_tail = spool_audit.last_record().cloned();
    let journal_path = output_dir.join(HANDOFF_JOURNAL_FILE);
    let identity_sha256 = sha256_file(&output_dir.join(IDENTITY_FILE))?;
    let handoff_journal_sha256 = sha256_file(&journal_path)?;
    let journal_state = read_handoff_journal(&journal_path, false)?;
    let file = File::open(&journal_path)
        .with_context(|| format!("open raw gRPC journal {}", journal_path.display()))?;
    let mut expected_frame_id = 0u64;
    let mut journal_tail = None;
    let mut decompressor =
        zstd::bulk::Decompressor::new().context("create raw gRPC zstd decompressor")?;
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| format!("read {}", journal_path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: GrpcRawHandoffRecord = serde_json::from_str(&line).with_context(|| {
            format!(
                "decode {} frame {expected_frame_id}",
                journal_path.display()
            )
        })?;
        validate_handoff_sequence(&row, expected_frame_id)?;
        let stored = read_spool_record(&wal_dir, row.location(), max_record_bytes)?;
        ensure!(
            stored.metadata.observation.sequence == row.frame_id,
            "spool/journal frame id mismatch at {}",
            row.frame_id
        );
        ensure!(
            stored.metadata.cluster_id == identity.cluster_id
                && stored.metadata.observation.origin_node_id == identity.origin_node_id
                && stored.metadata.observation.journal_id == identity.journal_id
                && stored.metadata.source_id == identity.source_id
                && stored.metadata.payload_format_version == PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
            "spool identity/format mismatch at frame {}",
            row.frame_id
        );
        ensure!(
            stored.payload.len() as u64 == row.compressed_len,
            "spool/journal compressed length mismatch at frame {}",
            row.frame_id
        );
        let raw_capacity = usize::try_from(row.uncompressed_len)
            .context("raw gRPC uncompressed length exceeds usize")?;
        ensure!(
            row.uncompressed_len <= max_record_bytes,
            "raw gRPC frame {} uncompressed length {} exceeds maximum {}",
            row.frame_id,
            row.uncompressed_len,
            max_record_bytes
        );
        let raw = decompressor
            .decompress(&stored.payload, raw_capacity)
            .with_context(|| format!("decompress raw gRPC frame {}", row.frame_id))?;
        ensure!(
            raw.len() as u64 == row.uncompressed_len,
            "raw gRPC uncompressed length mismatch at frame {}",
            row.frame_id
        );
        ensure!(
            sha256_hex(&raw) == row.protobuf_sha256,
            "raw gRPC protobuf checksum mismatch at frame {}",
            row.frame_id
        );
        let update = SubscribeUpdate::decode(raw.as_slice())
            .with_context(|| format!("decode raw gRPC protobuf frame {}", row.frame_id))?;
        let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
            return Err(anyhow!(
                "raw gRPC frame {} is not a block update",
                row.frame_id
            ));
        };
        ensure!(
            block.slot == row.slot
                && block.parent_slot == row.parent_slot
                && block.blockhash == row.blockhash,
            "raw gRPC protobuf metadata mismatch at frame {}",
            row.frame_id
        );
        ensure!(
            stored.metadata.logical_key
                == (LogicalKey::Block {
                    slot: block.slot,
                    blockhash: decode_blockhash(&block.blockhash)?,
                }),
            "raw gRPC logical key mismatch at frame {}",
            row.frame_id
        );
        visit(&row, update)?;
        journal_tail = Some(row);
        expected_frame_id = expected_frame_id
            .checked_add(1)
            .context("raw gRPC replay frame id overflow")?;
    }
    ensure!(
        expected_frame_id == journal_state.records && journal_tail == journal_state.last,
        "raw gRPC handoff journal changed during locked replay"
    );
    match (wal_tail.as_ref(), journal_tail.as_ref()) {
        (None, None) => {}
        (Some(wal), Some(journal)) => {
            ensure!(
                wal.metadata().observation.sequence == journal.frame_id,
                "raw gRPC WAL/journal tail frame id mismatch"
            );
            ensure!(
                wal.location() == journal.location(),
                "raw gRPC WAL/journal tail location mismatch"
            );
        }
        (Some(wal), None) => {
            return Err(anyhow!(
                "raw gRPC handoff journal is empty but WAL has durable frame {}",
                wal.metadata().observation.sequence
            ));
        }
        (None, Some(journal)) => {
            return Err(anyhow!(
                "raw gRPC handoff journal frame {} exists without a durable WAL frame",
                journal.frame_id
            ));
        }
    }
    Ok(GrpcRawReplayAudit {
        records: expected_frame_id,
        wal_incomplete_tail_bytes: spool_audit.incomplete_tail_bytes(),
        identity_sha256,
        handoff_journal_sha256,
        tail: journal_tail,
    })
}

/// Convert the currently committed slice of one epoch from a stopped raw recorder spool into a
/// fresh live-capture directory.
///
/// The replay audit holds the spool writer lock for the full conversion. Consequently this
/// function is intentionally incompatible with an active recorder: callers must rotate/stop the
/// recorder or provide a filesystem snapshot. Output is written to a deterministic sibling staging
/// directory and published with one rename only after every source frame and generated sidecar has
/// been validated and synced. Neither the source WAL nor its handoff journal is ever modified.
pub fn materialize_grpc_raw_blocks(
    config: GrpcRawMaterializeConfig,
) -> Result<GrpcRawMaterializeReport> {
    ensure!(
        config.max_record_bytes > 0,
        "max record bytes must be non-zero"
    );
    ensure!(
        config.input_dir != config.archive_dir,
        "raw input and materialized archive directories must differ"
    );
    ensure_path_absent(&config.archive_dir, "materialized archive")?;
    let staging_dir = materialization_staging_path(&config.archive_dir)?;
    ensure_path_absent(&staging_dir, "raw materialization staging directory")?;

    let mut writer = None;
    let mut first_source_frame_id = None;
    let mut last_source_frame_id = None;
    let mut previous_slot = None;
    let replay = replay_grpc_raw_blocks_audited(
        &config.input_dir,
        config.max_record_bytes,
        |row, update| {
            if row.epoch != config.epoch {
                return Ok(());
            }
            if let Some(previous_slot) = previous_slot {
                ensure!(
                    row.slot > previous_slot,
                    "raw epoch {} slots are not strictly increasing: {} followed {}",
                    config.epoch,
                    row.slot,
                    previous_slot
                );
            }
            let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
                return Err(anyhow!("raw gRPC frame {} is not a block", row.frame_id));
            };
            if writer.is_none() {
                fs::create_dir(&staging_dir).with_context(|| {
                    format!(
                        "create raw materialization staging directory {}",
                        staging_dir.display()
                    )
                })?;
                writer = Some(GrpcRawArchiveWriter::create(
                    &staging_dir,
                    config.epoch,
                    config.pubkey_hot_registry_path.as_deref(),
                    config.pubkey_hot_count,
                )?);
            }
            writer
                .as_mut()
                .context("raw materialization writer was not initialized")?
                .write_block(block, row.epoch, row.epoch_slot_index)?;
            first_source_frame_id.get_or_insert(row.frame_id);
            last_source_frame_id = Some(row.frame_id);
            previous_slot = Some(row.slot);
            Ok(())
        },
    )?;

    let first_source_frame_id = first_source_frame_id.with_context(|| {
        format!(
            "raw gRPC spool contains no committed blocks for epoch {}",
            config.epoch
        )
    })?;
    let last_source_frame_id = last_source_frame_id.context("missing selected source tail")?;
    let archive = writer
        .context("raw materialization writer was not created")?
        .finish()?;
    ensure!(
        archive.blocks_written > 0,
        "raw materializer wrote no blocks"
    );

    rewrite_published_layout(&staging_dir, &config.archive_dir)?;
    validate_materialized_capture(&staging_dir, &archive)?;
    let artifacts = collect_materialized_artifacts(&staging_dir)?;
    let receipt = GrpcRawMaterializationReceipt {
        schema_version: MATERIALIZATION_RECEIPT_SCHEMA_VERSION,
        source_identity_sha256: replay.identity_sha256,
        source_handoff_journal_sha256: replay.handoff_journal_sha256,
        source_frames: replay.records,
        source_tail_frame_id: replay.tail.as_ref().map(|row| row.frame_id),
        source_tail_slot: replay.tail.as_ref().map(|row| row.slot),
        source_wal_incomplete_tail_bytes: replay.wal_incomplete_tail_bytes,
        epoch: config.epoch,
        first_source_frame_id,
        last_source_frame_id,
        first_slot: archive
            .first_slot
            .context("materialized archive has no first slot")?,
        last_slot: archive
            .last_slot
            .context("materialized archive has no last slot")?,
        blocks_written: archive.blocks_written,
        transactions_written: archive.transactions_written,
        entries_written: archive.entries_written,
        normalized_block_bytes: archive.normalized_block_bytes,
        pubkey_run_files: archive.pubkey_run_files,
        pubkey_run_records: archive.pubkey_run_records,
        pubkey_hot_keys: archive.pubkey_hot_keys,
        pubkey_hot_records: archive.pubkey_hot_records,
        artifacts,
    };
    let staged_receipt_path = staging_dir
        .join("journal")
        .join(MATERIALIZATION_RECEIPT_FILE);
    write_json_file_synced(&staged_receipt_path, &receipt)?;
    write_materialization_progress(&staging_dir, &config.archive_dir, &receipt)?;
    sync_tree(&staging_dir)?;

    // Recheck immediately before rename. The deterministic staging name already serializes
    // cooperating materializers targeting this archive; this check prevents replacing a target
    // created after the initial preflight.
    ensure_path_absent(&config.archive_dir, "materialized archive")?;
    fs::rename(&staging_dir, &config.archive_dir).with_context(|| {
        format!(
            "publish raw materialization {} -> {}",
            staging_dir.display(),
            config.archive_dir.display()
        )
    })?;
    if let Some(parent) = effective_parent(&config.archive_dir) {
        sync_directory(parent)?;
    }

    Ok(GrpcRawMaterializeReport {
        input_dir: config.input_dir,
        receipt_path: config
            .archive_dir
            .join("journal")
            .join(MATERIALIZATION_RECEIPT_FILE),
        archive_dir: config.archive_dir,
        receipt,
    })
}

/// Seed a new, empty generation with the exact durable tail record of a stopped source.
///
/// The source remains exclusively locked while it is replay-verified and copied. The target uses
/// a fresh physical WAL journal id, but preserves the logical replication journal and maps its
/// frame zero to the source tail sequence. Publishing is atomic with respect to the target's
/// parent directory.
pub fn seed_grpc_raw_generation(
    source_dir: PathBuf,
    target_dir: PathBuf,
    max_record_bytes: u64,
) -> Result<GrpcRawSeedReport> {
    ensure!(max_record_bytes > 0, "max record bytes must be non-zero");
    ensure!(
        source_dir != target_dir,
        "source and target generation paths must differ"
    );
    ensure_empty_generation_target(&target_dir)?;
    let target_parent = target_dir
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let parent_metadata = fs::symlink_metadata(&target_parent).with_context(|| {
        format!(
            "inspect target generation parent {}",
            target_parent.display()
        )
    })?;
    ensure!(
        parent_metadata.file_type().is_dir() && !parent_metadata.file_type().is_symlink(),
        "target generation parent is not a real directory: {}",
        target_parent.display()
    );
    let target_name = target_dir
        .file_name()
        .context("target generation path has no file name")?
        .to_string_lossy();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let temporary_dir = target_parent.join(format!(
        ".{target_name}.seed-{}-{unique}.tmp",
        std::process::id()
    ));
    fs::create_dir(&temporary_dir).with_context(|| {
        format!(
            "create temporary target generation {}",
            temporary_dir.display()
        )
    })?;
    sync_directory(&target_parent)?;

    let seeded = (|| -> Result<GrpcRawSeedReport> {
        let source_identity = read_identity(&source_dir.join(IDENTITY_FILE))?;
        let source_wal_dir = spool_journal_dir(&source_dir, &source_identity);
        let source_audit = LockedSpoolAudit::open(
            source_dir.join(WAL_ROOT_DIR),
            source_identity.spool_identity(),
            SpoolOptions {
                max_record_bytes,
                ..SpoolOptions::default()
            },
        )
        .context("lock and audit source raw gRPC generation")?;
        ensure!(
            source_audit.journal_dir() == source_wal_dir,
            "source raw gRPC WAL audit resolved a different journal directory"
        );

        let mut source_tail = None;
        let source_replay = replay_grpc_raw_blocks_with_audit(
            &source_dir,
            max_record_bytes,
            &source_identity,
            &source_wal_dir,
            &source_audit,
            |row, update| {
                let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
                    return Err(anyhow!("raw gRPC frame {} is not a block", row.frame_id));
                };
                validate_complete_poh_block(block).with_context(|| {
                    format!("verify source raw gRPC PoH frame {}", row.frame_id)
                })?;
                source_tail = Some(row.clone());
                Ok(())
            },
        )?;
        let source_tail = source_tail.context("source raw gRPC generation is empty")?;
        ensure!(
            source_replay.records > 0,
            "source raw gRPC generation is empty"
        );
        let source_record =
            read_spool_record(&source_wal_dir, source_tail.location(), max_record_bytes)?;

        let target_config = GrpcRawRecordConfig {
            endpoint: source_identity.endpoint.clone(),
            output_dir: temporary_dir.clone(),
            max_record_bytes,
            cluster_id: source_identity.cluster_id.clone(),
            origin_node_id: source_identity.origin_node_id.clone(),
            source_id: source_identity.source_id.clone(),
            ..GrpcRawRecordConfig::default()
        };
        let replication_anchor = GrpcRawReplicationGenerationAnchor {
            journal_id: source_identity.replication_stream_id().journal_id,
            sequence_base: source_identity.replication_sequence(source_tail.frame_id)?,
        };
        let target_identity = load_or_create_identity_with_replication_anchor(
            &target_config,
            Some(replication_anchor),
        )?;
        ensure!(
            target_identity.endpoint == source_identity.endpoint
                && target_identity.cluster_id == source_identity.cluster_id
                && target_identity.origin_node_id == source_identity.origin_node_id
                && target_identity.source_id == source_identity.source_id,
            "seed target identity is incompatible with source generation"
        );
        ensure!(
            target_identity.journal_id != source_identity.journal_id,
            "seed target unexpectedly reused the source journal id"
        );
        ensure!(
            target_identity.replication_stream_id() == source_identity.replication_stream_id()
                && target_identity.replication_sequence(0)?
                    == source_identity.replication_sequence(source_tail.frame_id)?,
            "seed target did not continue the source logical replication stream"
        );
        let mut target_spool = SpoolWriter::open(
            temporary_dir.join(WAL_ROOT_DIR),
            target_identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: target_config.segment_target_bytes,
                max_record_bytes,
            },
        )?;
        let target_journal = temporary_dir.join(HANDOFF_JOURNAL_FILE);
        let target_state = recover_handoff_journal(&target_journal)?;
        ensure!(
            target_state.records == 0 && target_spool.last_record().is_none(),
            "temporary seed target is not empty"
        );
        let target_metadata = IngressRecordMeta::from_payload(
            target_identity.cluster_id.clone(),
            ObservationId {
                origin_node_id: target_identity.origin_node_id.clone(),
                journal_id: target_identity.journal_id,
                sequence: 0,
            },
            target_identity.source_id.clone(),
            source_record.metadata.logical_key.clone(),
            PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
            &source_record.payload,
        );
        let target_durable =
            target_spool.append_and_sync(target_metadata, &source_record.payload)?;
        let target_row = GrpcRawHandoffRecord {
            frame_id: 0,
            segment_id: target_durable.location().segment_id,
            frame_offset: target_durable.location().frame_offset,
            frame_len: target_durable.location().frame_len,
            ..source_tail.clone()
        };
        append_handoff_record(&target_journal, &target_row)?;
        drop(target_spool);

        let target_verification = verify_grpc_raw_poh(temporary_dir.clone(), max_record_bytes, 1)?;
        ensure!(
            target_verification.records_verified == 1
                && target_verification.first_slot == Some(source_tail.slot)
                && target_verification.last_slot == Some(source_tail.slot),
            "seed target verification did not reproduce exactly one source tail record"
        );

        fs::rename(&temporary_dir, &target_dir).with_context(|| {
            format!(
                "publish seeded raw gRPC generation {}",
                target_dir.display()
            )
        })?;
        sync_directory(&target_parent)?;

        Ok(GrpcRawSeedReport {
            source_dir,
            target_dir,
            source_records_verified: source_replay.records,
            seeded_slot: source_tail.slot,
            seeded_blockhash: source_tail.blockhash,
            compressed_bytes_copied: source_tail.compressed_len,
        })
    })();

    if seeded.is_err() {
        let _ = fs::remove_dir_all(&temporary_dir);
        let _ = sync_directory(&target_parent);
    }
    seeded
}

const HOT_ACTIVE_DIR: &str = "active";
const HOT_SEALED_DIR: &str = "sealed";
const HOT_RECEIPTS_DIR: &str = "receipts";
const HOT_ROTATION_MARKER: &str = ".rotation";
const HOT_ROTATION_LOCK: &str = ".rotation.lock";
const HOT_GENERATION_SEQUENCE: &str = ".generation-sequence";
const MAX_HOT_GENERATION_SEQUENCE_BYTES: u64 = 64;
const REPLAY_GAPS_DIR: &str = "replay-gaps";
const MAX_HOT_AUXILIARY_FILES: usize = 128;
const MAX_HOT_AUXILIARY_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy)]
enum HotRotationLockMode {
    Shared,
    Exclusive,
}

#[derive(Debug)]
struct HotRotationGuard {
    _file: File,
}

impl HotRotationGuard {
    fn acquire(cache_root: &Path, mode: HotRotationLockMode) -> Result<Self> {
        ensure_real_directory(cache_root, "hot generation root")?;
        let path = cache_root.join(HOT_ROTATION_LOCK);
        let linked_before = match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                ensure!(
                    metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
                    "hot generation rotation lock is not a regular file"
                );
                Some(metadata)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("inspect hot generation rotation lock {}", path.display())
                });
            }
        };
        let created = linked_before.is_none();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let file = options
            .open(&path)
            .with_context(|| format!("open hot generation rotation lock {}", path.display()))?;
        let opened = file
            .metadata()
            .with_context(|| format!("inspect hot generation rotation lock {}", path.display()))?;
        ensure!(
            opened.file_type().is_file(),
            "hot generation rotation lock is not a regular file"
        );
        let linked_after = fs::symlink_metadata(&path).with_context(|| {
            format!("reinspect hot generation rotation lock {}", path.display())
        })?;
        #[cfg(unix)]
        {
            ensure!(
                opened.dev() == linked_after.dev() && opened.ino() == linked_after.ino(),
                "hot generation rotation lock changed while it was opened"
            );
            if let Some(linked_before) = linked_before.as_ref() {
                ensure!(
                    opened.dev() == linked_before.dev() && opened.ino() == linked_before.ino(),
                    "hot generation rotation lock was replaced while it was opened"
                );
            }
            let effective_uid = unsafe { libc::geteuid() };
            ensure!(
                opened.uid() == effective_uid || opened.uid() == 0,
                "hot generation rotation lock has an untrusted owner"
            );
            ensure!(
                opened.mode() & 0o077 == 0,
                "hot generation rotation lock must be private"
            );
            let operation = match mode {
                HotRotationLockMode::Shared => libc::LOCK_SH,
                HotRotationLockMode::Exclusive => libc::LOCK_EX,
            };
            // SAFETY: `file` remains owned by the guard for the complete lock lifetime.
            let result = unsafe { libc::flock(file.as_raw_fd(), operation) };
            if result != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("acquire hot generation rotation lock");
            }
        }
        #[cfg(not(unix))]
        let _ = (linked_before, linked_after, mode);
        if created {
            sync_directory(cache_root)?;
        }
        Ok(Self { _file: file })
    }
}

#[derive(Debug)]
struct HotRotationResult {
    generation_bytes: u64,
    next_frame_id: u64,
    seeded_tail: GrpcRawHandoffRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HotRotationStep {
    MarkerPublished,
    OldGenerationHidden,
    SuccessorActive,
    SealedGenerationVisible,
}

#[derive(Debug)]
struct HotRotationLayout {
    root: PathBuf,
    active: PathBuf,
    next: PathBuf,
    hidden_old: PathBuf,
    sealed: PathBuf,
    marker: PathBuf,
    receipt: PathBuf,
}

fn validate_hot_generation_config(config: &GrpcRawRecordConfig) -> Result<()> {
    let Some(root) = config.hot_generation_root.as_deref() else {
        return Ok(());
    };
    ensure!(
        config.max_generation_bytes > 0,
        "hot generation rotation requires a non-zero generation byte limit"
    );
    ensure!(root.is_absolute(), "hot generation root must be absolute");
    ensure!(
        config.output_dir == root.join(HOT_ACTIVE_DIR),
        "hot generation output must be <root>/active"
    );
    ensure_real_directory(root, "hot generation root")?;
    ensure_real_directory(&config.output_dir, "hot active generation")?;
    ensure_real_directory(
        &root.join(HOT_SEALED_DIR),
        "hot sealed generation directory",
    )?;
    ensure_real_directory(
        &root.join(HOT_RECEIPTS_DIR),
        "hot generation receipt directory",
    )?;
    ensure!(
        !path_entry_exists(&root.join(HOT_ROTATION_MARKER))?,
        "hot generation rotation marker must be recovered by the supervisor before startup"
    );
    for entry in fs::read_dir(root)
        .with_context(|| format!("list hot generation root {}", root.display()))?
    {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        ensure!(
            !name.starts_with(".next-slot-") && !name.starts_with(".sealed-slot-"),
            "hot generation transaction must be recovered by the supervisor before startup"
        );
    }
    Ok(())
}

fn ensure_real_directory(path: &Path, description: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {description} {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "{description} is not a real directory: {}",
        path.display()
    );
    Ok(())
}

fn path_entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect path entry {}", path.display())),
    }
}

fn hot_rotation_layout(cache_root: &Path, slot: u64) -> HotRotationLayout {
    let generation_id = format!("slot-{slot:020}");
    HotRotationLayout {
        root: cache_root.to_path_buf(),
        active: cache_root.join(HOT_ACTIVE_DIR),
        next: cache_root.join(format!(".next-{generation_id}")),
        hidden_old: cache_root.join(format!(".sealed-{generation_id}")),
        sealed: cache_root.join(HOT_SEALED_DIR).join(&generation_id),
        marker: cache_root.join(HOT_ROTATION_MARKER),
        receipt: cache_root
            .join(HOT_RECEIPTS_DIR)
            .join(format!("{generation_id}.json")),
    }
}

/// Allocate and durably reserve a strictly increasing generation identifier while retaining the
/// historical `slot-<20 digits>` wire/storage grammar used by the uploader. The identifier is a
/// generation sequence seeded from a real tail slot; after a repeated/lower fork slot it no longer
/// promises to equal the predecessor's tail slot. Reserving before publication may leave a harmless
/// gap after a crash, but it can never reuse an object-store prefix whose local generation was
/// already acknowledged and removed.
fn allocate_hot_generation_number(cache_root: &Path, tail_slot: u64) -> Result<u64> {
    let persisted = read_hot_generation_sequence(cache_root)?;
    // Retained upload receipts can grow indefinitely. They are consulted exactly once to migrate
    // a legacy cache; after the synced sequence file exists it is the compact authoritative high
    // water and rotations remain O(1) regardless of retention length.
    let high_water = match persisted {
        Some(value) => Some(value),
        None => scan_hot_generation_high_water(cache_root)?,
    };
    let number = match high_water {
        Some(high_water) if high_water >= tail_slot => high_water
            .checked_add(1)
            .context("hot generation identifier space exhausted")?,
        _ => tail_slot,
    };
    persist_hot_generation_sequence(cache_root, number)?;
    Ok(number)
}

fn read_hot_generation_sequence(cache_root: &Path) -> Result<Option<u64>> {
    let path = cache_root.join(HOT_GENERATION_SEQUENCE);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
                "hot generation sequence is not a regular file"
            );
            ensure!(
                metadata.len() > 0 && metadata.len() <= MAX_HOT_GENERATION_SEQUENCE_BYTES,
                "hot generation sequence has an invalid size"
            );
        }
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect hot generation sequence {}", path.display()));
        }
    }
    let mut file = open_regular_readonly_nofollow(&path, "hot generation sequence")?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_HOT_GENERATION_SEQUENCE_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read hot generation sequence {}", path.display()))?;
    ensure!(
        !bytes.is_empty() && bytes.len() as u64 <= MAX_HOT_GENERATION_SEQUENCE_BYTES,
        "hot generation sequence has an invalid size"
    );
    let value = std::str::from_utf8(&bytes)
        .context("hot generation sequence is not UTF-8")?
        .trim();
    ensure!(
        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
        "hot generation sequence is not an unsigned decimal integer"
    );
    value
        .parse::<u64>()
        .map(Some)
        .context("hot generation sequence exceeds u64")
}

fn scan_hot_generation_high_water(cache_root: &Path) -> Result<Option<u64>> {
    let sealed_root = cache_root.join(HOT_SEALED_DIR);
    ensure_real_directory(&sealed_root, "hot sealed generation directory")?;
    let receipts_root = cache_root.join(HOT_RECEIPTS_DIR);
    ensure_real_directory(&receipts_root, "hot generation receipt directory")?;
    let mut newest = None::<u64>;
    let mut count = 0usize;
    for entry in fs::read_dir(&sealed_root)
        .with_context(|| format!("list hot sealed generations {}", sealed_root.display()))?
    {
        let entry = entry.context("read hot sealed generation entry")?;
        count = count
            .checked_add(1)
            .context("hot sealed generation count overflow")?;
        ensure!(
            count <= MAX_REPLICATION_GENERATION_SCAN,
            "hot sealed generation queue exceeds {MAX_REPLICATION_GENERATION_SCAN} entries"
        );
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("hot sealed generation name is not UTF-8"))?;
        let number = hot_generation_number(&name)
            .with_context(|| format!("invalid hot sealed generation name {name:?}"))?;
        let file_type = entry
            .file_type()
            .context("inspect hot sealed generation entry")?;
        ensure!(
            file_type.is_dir() && !file_type.is_symlink(),
            "hot sealed generation is not a real directory"
        );
        newest = Some(newest.map_or(number, |current| current.max(number)));
    }
    for entry in fs::read_dir(&receipts_root)
        .with_context(|| format!("list hot generation receipts {}", receipts_root.display()))?
    {
        let entry = entry.context("read hot generation receipt entry")?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("hot generation receipt name is not UTF-8"))?;
        if !name.starts_with("slot-") || !name.ends_with(".json") {
            continue;
        }
        count = count
            .checked_add(1)
            .context("hot generation evidence count overflow")?;
        ensure!(
            count <= MAX_REPLICATION_GENERATION_SCAN,
            "hot generation evidence exceeds {MAX_REPLICATION_GENERATION_SCAN} entries"
        );
        let generation_id = name
            .strip_suffix(".json")
            .context("hot generation receipt suffix disappeared")?;
        let number = hot_generation_number(generation_id)
            .with_context(|| format!("invalid hot generation receipt name {name:?}"))?;
        let file_type = entry
            .file_type()
            .context("inspect hot generation receipt entry")?;
        ensure!(
            file_type.is_file() && !file_type.is_symlink(),
            "hot generation receipt is not a regular file"
        );
        newest = Some(newest.map_or(number, |current| current.max(number)));
    }
    Ok(newest)
}

fn persist_hot_generation_sequence(cache_root: &Path, number: u64) -> Result<()> {
    let path = cache_root.join(HOT_GENERATION_SEQUENCE);
    if path_entry_exists(&path)? {
        // Refuse to replace a hostile or corrupted path. This also validates ownership/mode and
        // detects an inode swap before the atomic update below.
        let current = read_hot_generation_sequence(cache_root)?
            .context("hot generation sequence disappeared before update")?;
        ensure!(
            number > current,
            "hot generation sequence must advance monotonically"
        );
    }
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let temporary = cache_root.join(format!(
        ".generation-sequence.{}.{unique}.tmp",
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        let mut file = options.open(&temporary).with_context(|| {
            format!(
                "create hot generation sequence temporary file {}",
                temporary.display()
            )
        })?;
        writeln!(file, "{number:020}")?;
        file.sync_all()?;
        fs::rename(&temporary, &path)
            .with_context(|| format!("publish hot generation sequence {}", path.display()))?;
        sync_directory(cache_root)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn hot_rotate_generation(
    config: &GrpcRawRecordConfig,
    cache_root: &Path,
    identity: &mut GrpcRawIdentityFile,
    spool: &mut SpoolWriter,
    source_tail: &GrpcRawHandoffRecord,
) -> Result<HotRotationResult> {
    let _rotation_guard = HotRotationGuard::acquire(cache_root, HotRotationLockMode::Exclusive)?;
    ensure!(
        spool
            .last_record()
            .is_some_and(|record| record.location() == source_tail.location()),
        "hot generation journal tail differs from its durable WAL tail"
    );
    let source_record = spool
        .read_record(
            spool
                .last_record()
                .context("hot generation source has no durable WAL tail")?,
        )
        .context("read hot generation durable tail")?;
    ensure!(
        source_record.metadata.observation.sequence == source_tail.frame_id,
        "hot generation durable tail sequence differs from its handoff row"
    );

    let generation_number = allocate_hot_generation_number(cache_root, source_tail.slot)?;
    let layout = hot_rotation_layout(cache_root, generation_number);
    for path in [
        &layout.next,
        &layout.hidden_old,
        &layout.sealed,
        &layout.marker,
        &layout.receipt,
    ] {
        ensure!(
            !path_entry_exists(path)?,
            "hot generation rotation target already exists: {}",
            path.display()
        );
    }
    let (successor_identity, seeded_tail) =
        seed_hot_generation_from_tail(config, identity, source_tail, &source_record, &layout.next)?;

    publish_hot_rotation_layout(&layout, |_| Ok(()))?;
    let successor_spool = SpoolWriter::open(
        layout.active.join(WAL_ROOT_DIR),
        successor_identity.spool_identity(),
        SpoolOptions {
            segment_target_bytes: config.segment_target_bytes,
            max_record_bytes: config.max_record_bytes,
        },
    )
    .context("open hot generation successor after publication")?;
    let generation_bytes = raw_generation_bytes(
        &layout.active.join(IDENTITY_FILE),
        &layout.active.join(WAL_ROOT_DIR),
        &layout.active.join(HANDOFF_JOURNAL_FILE),
    )?;

    // Replacing the writer drops the predecessor lock before its full audit. The predecessor is
    // still hidden and the durable marker keeps upload discovery blocked throughout that audit.
    *spool = successor_spool;
    *identity = successor_identity;
    let predecessor_verification =
        verify_grpc_raw_poh(layout.hidden_old.clone(), config.max_record_bytes, 1)
            .context("fully audit hot-rotated predecessor before publishing it")?;
    ensure!(
        predecessor_verification.last_slot == Some(source_tail.slot),
        "hot-rotated predecessor audit returned the wrong durable tail"
    );
    publish_hot_sealed_generation(&layout, |_| Ok(()))?;
    finish_hot_rotation_layout(&layout)?;
    tracing::info!(
        generation_id = %layout.sealed.file_name().unwrap_or_default().to_string_lossy(),
        sealed_slot = source_tail.slot,
        "raw gRPC generation hot-rotated without reconnecting"
    );
    Ok(HotRotationResult {
        generation_bytes,
        next_frame_id: 1,
        seeded_tail,
    })
}

fn seed_hot_generation_from_tail(
    config: &GrpcRawRecordConfig,
    source_identity: &GrpcRawIdentityFile,
    source_tail: &GrpcRawHandoffRecord,
    source_record: &crate::ingest::SpoolRecord,
    target_dir: &Path,
) -> Result<(GrpcRawIdentityFile, GrpcRawHandoffRecord)> {
    ensure_empty_generation_target(target_dir)?;
    let target_parent = target_dir
        .parent()
        .context("hot generation successor has no parent")?;
    let target_name = target_dir
        .file_name()
        .context("hot generation successor has no file name")?
        .to_string_lossy();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let temporary_dir = target_parent.join(format!(
        ".{target_name}.seed-{}-{unique}.tmp",
        std::process::id()
    ));
    fs::create_dir(&temporary_dir).with_context(|| {
        format!(
            "create hot generation successor temporary directory {}",
            temporary_dir.display()
        )
    })?;
    sync_directory(target_parent)?;

    let result = (|| -> Result<(GrpcRawIdentityFile, GrpcRawHandoffRecord)> {
        let target_config = GrpcRawRecordConfig {
            endpoint: source_identity.endpoint.clone(),
            output_dir: temporary_dir.clone(),
            segment_target_bytes: config.segment_target_bytes,
            max_record_bytes: config.max_record_bytes,
            cluster_id: source_identity.cluster_id.clone(),
            origin_node_id: source_identity.origin_node_id.clone(),
            source_id: source_identity.source_id.clone(),
            ..GrpcRawRecordConfig::default()
        };
        let replication_anchor = GrpcRawReplicationGenerationAnchor {
            journal_id: source_identity.replication_stream_id().journal_id,
            sequence_base: source_identity.replication_sequence(source_tail.frame_id)?,
        };
        let target_identity = load_or_create_identity_with_replication_anchor(
            &target_config,
            Some(replication_anchor),
        )?;
        ensure!(
            target_identity.journal_id != source_identity.journal_id,
            "hot generation successor reused the source journal id"
        );
        ensure!(
            target_identity.replication_stream_id() == source_identity.replication_stream_id()
                && target_identity.replication_sequence(0)?
                    == source_identity.replication_sequence(source_tail.frame_id)?,
            "hot generation successor did not continue the source logical replication stream"
        );
        let mut target_spool = SpoolWriter::open(
            temporary_dir.join(WAL_ROOT_DIR),
            target_identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )?;
        let target_journal = temporary_dir.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&target_journal)?;
        let target_metadata = IngressRecordMeta::from_payload(
            target_identity.cluster_id.clone(),
            ObservationId {
                origin_node_id: target_identity.origin_node_id.clone(),
                journal_id: target_identity.journal_id,
                sequence: 0,
            },
            target_identity.source_id.clone(),
            source_record.metadata.logical_key.clone(),
            PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
            &source_record.payload,
        );
        let durable = target_spool.append_and_sync(target_metadata, &source_record.payload)?;
        let target_tail = GrpcRawHandoffRecord {
            frame_id: 0,
            segment_id: durable.location().segment_id,
            frame_offset: durable.location().frame_offset,
            frame_len: durable.location().frame_len,
            ..source_tail.clone()
        };
        append_handoff_record(&target_journal, &target_tail)?;
        drop(target_spool);
        let verification = verify_grpc_raw_poh(temporary_dir.clone(), config.max_record_bytes, 1)?;
        ensure!(
            verification.records_verified == 1
                && verification.first_slot == Some(source_tail.slot)
                && verification.last_slot == Some(source_tail.slot),
            "hot generation successor did not reproduce exactly its source tail"
        );
        copy_hot_generation_auxiliary_files(&config.output_dir, &temporary_dir)?;
        fs::rename(&temporary_dir, target_dir).with_context(|| {
            format!("publish hot generation successor {}", target_dir.display())
        })?;
        sync_directory(target_parent)?;
        Ok((target_identity, target_tail))
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&temporary_dir);
        let _ = sync_directory(target_parent);
    }
    result
}

fn copy_hot_generation_auxiliary_files(source: &Path, target: &Path) -> Result<()> {
    let source_dir = source.join(REPLAY_GAPS_DIR);
    let metadata = match fs::symlink_metadata(&source_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("inspect hot generation replay-gap directory"),
    };
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "hot generation replay-gap path is not a real directory"
    );
    let target_dir = target.join(REPLAY_GAPS_DIR);
    fs::create_dir(&target_dir)?;
    sync_directory(target)?;
    let mut count = 0usize;
    let mut total = 0u64;
    for entry in fs::read_dir(&source_dir)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        ensure!(
            metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
            "hot generation replay-gap directory contains a non-regular file"
        );
        count = count
            .checked_add(1)
            .context("replay-gap file count overflow")?;
        total = total
            .checked_add(metadata.len())
            .context("replay-gap byte count overflow")?;
        ensure!(
            count <= MAX_HOT_AUXILIARY_FILES && total <= MAX_HOT_AUXILIARY_BYTES,
            "hot generation replay-gap evidence exceeds its bounded copy limit"
        );
        let target_path = target_dir.join(entry.file_name());
        fs::copy(entry.path(), &target_path)?;
        File::open(&target_path)?.sync_all()?;
    }
    sync_directory(&target_dir)?;
    Ok(())
}

fn publish_hot_rotation_layout<F>(layout: &HotRotationLayout, mut after_step: F) -> Result<()>
where
    F: FnMut(HotRotationStep) -> Result<()>,
{
    let generation_id = layout
        .sealed
        .file_name()
        .context("sealed hot generation has no file name")?
        .to_string_lossy();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let marker_temp = layout
        .root
        .join(format!(".rotation.{}.{unique}.tmp", std::process::id()));
    let marker_result = (|| -> Result<()> {
        let mut marker = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker_temp)?;
        writeln!(marker, "{generation_id}")?;
        marker.sync_all()?;
        fs::hard_link(&marker_temp, &layout.marker)?;
        fs::remove_file(&marker_temp)?;
        sync_directory(&layout.root)?;
        Ok(())
    })();
    if marker_result.is_err() {
        let _ = fs::remove_file(&marker_temp);
        return marker_result;
    }
    after_step(HotRotationStep::MarkerPublished)?;

    fs::rename(&layout.active, &layout.hidden_old)?;
    sync_directory(&layout.root)?;
    after_step(HotRotationStep::OldGenerationHidden)?;
    fs::rename(&layout.next, &layout.active)?;
    sync_directory(&layout.root)?;
    after_step(HotRotationStep::SuccessorActive)?;
    Ok(())
}

fn publish_hot_sealed_generation<F>(layout: &HotRotationLayout, mut after_step: F) -> Result<()>
where
    F: FnMut(HotRotationStep) -> Result<()>,
{
    fs::rename(&layout.hidden_old, &layout.sealed)?;
    sync_directory(
        layout
            .sealed
            .parent()
            .context("sealed hot generation has no parent")?,
    )?;
    sync_directory(&layout.root)?;
    after_step(HotRotationStep::SealedGenerationVisible)?;
    Ok(())
}

fn finish_hot_rotation_layout(layout: &HotRotationLayout) -> Result<()> {
    fs::remove_file(&layout.marker)?;
    sync_directory(&layout.root)
}

fn ensure_empty_generation_target(target: &Path) -> Result<()> {
    match fs::symlink_metadata(target) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
                "seed target is not a real directory: {}",
                target.display()
            );
            ensure!(
                fs::read_dir(target)
                    .with_context(|| format!("list seed target {}", target.display()))?
                    .next()
                    .is_none(),
                "seed target directory is not empty: {}",
                target.display()
            );
            Ok(())
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("inspect seed target {}", target.display()))
        }
    }
}

/// Fully replay a raw spool and prove that every retained block contains the ordered entry data
/// required to reconstruct Blockzilla's PoH sidecar after canonical block IDs are assigned.
pub fn verify_grpc_raw_poh(
    output_dir: PathBuf,
    max_record_bytes: u64,
    minimum_records: u64,
) -> Result<GrpcRawPohVerifyReport> {
    let mut report = GrpcRawPohVerifyReport {
        output_dir: output_dir.clone(),
        minimum_records,
        records_verified: 0,
        first_slot: None,
        last_slot: None,
        poh_entries: 0,
        transaction_references: 0,
        num_hashes: "0".to_string(),
        wal_incomplete_tail_bytes: 0,
    };
    let mut num_hashes = 0u128;
    let replay = replay_grpc_raw_blocks_audited(&output_dir, max_record_bytes, |row, update| {
        let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
            return Err(anyhow!("raw gRPC frame {} is not a block", row.frame_id));
        };
        let stats = validate_complete_poh_block(block)
            .with_context(|| format!("verify raw gRPC PoH frame {}", row.frame_id))?;
        report.first_slot.get_or_insert(row.slot);
        report.last_slot = Some(row.slot);
        report.records_verified = report
            .records_verified
            .checked_add(1)
            .context("verified raw gRPC record count overflow")?;
        report.poh_entries = report
            .poh_entries
            .checked_add(stats.entries)
            .context("verified raw gRPC PoH entry count overflow")?;
        report.transaction_references = report
            .transaction_references
            .checked_add(stats.transaction_references)
            .context("verified raw gRPC PoH transaction count overflow")?;
        num_hashes = num_hashes
            .checked_add(stats.num_hashes)
            .context("verified raw gRPC PoH hash count overflow")?;
        Ok(())
    })?;
    ensure!(
        replay.records == report.records_verified,
        "raw gRPC PoH verification count differs from replay audit"
    );
    report.wal_incomplete_tail_bytes = replay.wal_incomplete_tail_bytes;
    report.num_hashes = num_hashes.to_string();
    ensure!(
        report.records_verified >= minimum_records,
        "raw gRPC PoH verification found {} records, below required minimum {}",
        report.records_verified,
        minimum_records
    );
    Ok(report)
}

pub fn inspect_grpc_raw_blocks(
    output_dir: PathBuf,
    max_record_bytes: u64,
    verify_payloads: bool,
) -> Result<GrpcRawInspectReport> {
    let identity = read_identity(&output_dir.join(IDENTITY_FILE))?;
    let wal_dir = spool_journal_dir(&output_dir, &identity);
    let journal_path = output_dir.join(HANDOFF_JOURNAL_FILE);
    let mut report = GrpcRawInspectReport {
        output_dir: output_dir.clone(),
        journal_path,
        wal_dir,
        records: 0,
        first_frame_id: None,
        last_frame_id: None,
        first_slot: None,
        last_slot: None,
        raw_bytes: 0,
        compressed_bytes: 0,
        compression_ratio: 0.0,
        payloads_verified: verify_payloads,
    };
    if verify_payloads {
        replay_grpc_raw_blocks(&output_dir, max_record_bytes, |row, _block| {
            update_inspect_report(&mut report, row)
        })?;
    } else {
        let state = read_handoff_journal(&report.journal_path, false)?;
        report.records = state.records;
        if let Some(first) = state.first.as_ref() {
            report.first_frame_id = Some(first.frame_id);
            report.first_slot = Some(first.slot);
        }
        if let Some(last) = state.last.as_ref() {
            report.last_frame_id = Some(last.frame_id);
            report.last_slot = Some(last.slot);
        }
        // The fast path intentionally avoids retaining every row; stream once to total lengths.
        let file = File::open(&report.journal_path)
            .with_context(|| format!("open {}", report.journal_path.display()))?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let row: GrpcRawHandoffRecord = serde_json::from_str(&line)?;
            report.raw_bytes = report.raw_bytes.saturating_add(row.uncompressed_len);
            report.compressed_bytes = report.compressed_bytes.saturating_add(row.compressed_len);
        }
    }
    report.compression_ratio = compression_ratio(report.raw_bytes, report.compressed_bytes);
    Ok(report)
}

fn update_inspect_report(
    report: &mut GrpcRawInspectReport,
    row: &GrpcRawHandoffRecord,
) -> Result<()> {
    report.first_frame_id.get_or_insert(row.frame_id);
    report.last_frame_id = Some(row.frame_id);
    report.first_slot.get_or_insert(row.slot);
    report.last_slot = Some(row.slot);
    report.records = report
        .records
        .checked_add(1)
        .context("record count overflow")?;
    report.raw_bytes = report
        .raw_bytes
        .checked_add(row.uncompressed_len)
        .context("raw byte count overflow")?;
    report.compressed_bytes = report
        .compressed_bytes
        .checked_add(row.compressed_len)
        .context("compressed byte count overflow")?;
    Ok(())
}

fn load_or_create_identity(config: &GrpcRawRecordConfig) -> Result<GrpcRawIdentityFile> {
    load_or_create_identity_with_replication_anchor(config, None)
}

fn load_or_create_identity_with_replication_anchor(
    config: &GrpcRawRecordConfig,
    replication_anchor: Option<GrpcRawReplicationGenerationAnchor>,
) -> Result<GrpcRawIdentityFile> {
    let path = config.output_dir.join(IDENTITY_FILE);
    if path.exists() {
        let identity = read_identity(&path)?;
        ensure!(
            identity.endpoint == config.endpoint,
            "raw gRPC endpoint changed for existing spool"
        );
        ensure!(
            identity.cluster_id == config.cluster_id,
            "raw gRPC cluster id changed for existing spool"
        );
        ensure!(
            identity.origin_node_id == config.origin_node_id,
            "raw gRPC origin node id changed for existing spool"
        );
        ensure!(
            identity.source_id == config.source_id,
            "raw gRPC source id changed for existing spool"
        );
        if let Some(anchor) = replication_anchor {
            ensure!(
                identity.replication_stream_id().journal_id == anchor.journal_id
                    && identity.replication_sequence_base() == anchor.sequence_base,
                "raw gRPC logical replication anchor changed for existing spool"
            );
        }
        return Ok(identity);
    }
    let journal_id = generate_journal_id(config);
    let replication_anchor = replication_anchor.unwrap_or(GrpcRawReplicationGenerationAnchor {
        journal_id,
        sequence_base: 0,
    });
    let identity = GrpcRawIdentityFile {
        schema_version: IDENTITY_SCHEMA_VERSION,
        endpoint: config.endpoint.clone(),
        cluster_id: config.cluster_id.clone(),
        origin_node_id: config.origin_node_id.clone(),
        source_id: config.source_id.clone(),
        journal_id,
        replication_journal_id: Some(replication_anchor.journal_id),
        replication_sequence_base: Some(replication_anchor.sequence_base),
        payload_format_version: PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
    };
    let temp_path = path.with_file_name(format!(".{IDENTITY_FILE}.{}.tmp", std::process::id()));
    let mut temp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .with_context(|| format!("create raw gRPC identity temp {}", temp_path.display()))?;
    serde_json::to_writer_pretty(&mut temp, &identity)?;
    temp.write_all(b"\n")?;
    temp.sync_all()?;
    drop(temp);
    match fs::hard_link(&temp_path, &path) {
        Ok(()) => {
            fs::remove_file(&temp_path)?;
            sync_directory(&config.output_dir)?;
            Ok(identity)
        }
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            fs::remove_file(&temp_path)?;
            read_identity(&path)
        }
        Err(err) => {
            let _ = fs::remove_file(&temp_path);
            Err(err).with_context(|| format!("publish raw gRPC identity {}", path.display()))
        }
    }
}

fn read_identity(path: &Path) -> Result<GrpcRawIdentityFile> {
    let mut file = open_regular_readonly_nofollow(path, "raw gRPC identity")?;
    let length = file
        .metadata()
        .with_context(|| format!("inspect raw gRPC identity {}", path.display()))?
        .len();
    ensure!(
        length > 0 && length <= MAX_IDENTITY_FILE_BYTES,
        "raw gRPC identity must be 1..={MAX_IDENTITY_FILE_BYTES} bytes: {}",
        path.display()
    );
    let mut encoded = Vec::with_capacity(length as usize);
    Read::by_ref(&mut file)
        .take(MAX_IDENTITY_FILE_BYTES + 1)
        .read_to_end(&mut encoded)
        .with_context(|| format!("read raw gRPC identity {}", path.display()))?;
    ensure!(
        encoded.len() as u64 == length,
        "raw gRPC identity changed while it was read: {}",
        path.display()
    );
    let identity: GrpcRawIdentityFile = serde_json::from_slice(&encoded)
        .with_context(|| format!("decode raw gRPC identity {}", path.display()))?;
    ensure!(
        identity.schema_version == IDENTITY_SCHEMA_VERSION,
        "unsupported raw gRPC identity schema"
    );
    ensure!(
        identity.payload_format_version == PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
        "unsupported raw gRPC payload format"
    );
    ensure!(
        identity.replication_journal_id.is_some() == identity.replication_sequence_base.is_some(),
        "raw gRPC identity has an incomplete logical replication anchor"
    );
    for (name, value) in [
        ("cluster_id", identity.cluster_id.as_str()),
        ("origin_node_id", identity.origin_node_id.as_str()),
        ("source_id", identity.source_id.as_str()),
    ] {
        ensure!(
            valid_replication_identity_component(value),
            "raw gRPC identity {name} is not a safe stable identifier"
        );
    }
    ensure!(
        !identity.endpoint.is_empty()
            && identity.endpoint.len() <= 4096
            && !identity.endpoint.chars().any(char::is_control),
        "raw gRPC identity endpoint is invalid"
    );
    Ok(identity)
}

fn valid_replication_identity_component(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && value.len() <= 64
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn generate_journal_id(config: &GrpcRawRecordConfig) -> [u8; 16] {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let mut hasher = Sha256::new();
    hasher.update(b"BLOCKZILLA-RAW-GRPC-JOURNAL-v1");
    hasher.update(now.to_le_bytes());
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(config.endpoint.as_bytes());
    hasher.update(config.output_dir.as_os_str().as_encoded_bytes());
    let digest = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&digest[..16]);
    id
}

fn recover_handoff_journal(path: &Path) -> Result<JournalState> {
    if !path.exists() {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .with_context(|| format!("create raw gRPC journal {}", path.display()))?;
        file.sync_all()?;
        if let Some(parent) = path.parent() {
            sync_directory(parent)?;
        }
        return Ok(JournalState::default());
    }
    let state = read_handoff_journal(path, true)?;
    Ok(state)
}

fn read_handoff_journal(path: &Path, truncate_partial_tail: bool) -> Result<JournalState> {
    let file = OpenOptions::new()
        .read(true)
        .write(truncate_partial_tail)
        .open(path)
        .with_context(|| format!("open raw gRPC journal {}", path.display()))?;
    let mut reader = BufReader::new(&file);
    let mut state = JournalState::default();
    let mut line = Vec::new();
    let mut valid_len = 0u64;
    loop {
        line.clear();
        let bytes = reader.read_until(b'\n', &mut line)?;
        if bytes == 0 {
            break;
        }
        if !line.ends_with(b"\n") {
            if truncate_partial_tail {
                drop(reader);
                file.set_len(valid_len)?;
                file.sync_data()?;
                return Ok(state);
            }
            return Err(anyhow!(
                "partial raw gRPC journal tail in {}",
                path.display()
            ));
        }
        valid_len = valid_len
            .checked_add(bytes as u64)
            .context("journal offset overflow")?;
        let payload = &line[..line.len() - 1];
        if payload.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let row: GrpcRawHandoffRecord = serde_json::from_slice(payload)
            .with_context(|| format!("decode raw gRPC journal frame {}", state.records))?;
        validate_handoff_sequence(&row, state.records)?;
        state.first.get_or_insert_with(|| row.clone());
        state.last = Some(row);
        state.records = state
            .records
            .checked_add(1)
            .context("journal record overflow")?;
    }
    Ok(state)
}

fn validate_handoff_sequence(row: &GrpcRawHandoffRecord, expected_frame_id: u64) -> Result<()> {
    ensure!(
        row.schema_version == JOURNAL_SCHEMA_VERSION,
        "unsupported raw gRPC journal schema"
    );
    ensure!(
        row.frame_id == expected_frame_id,
        "raw gRPC journal frame id {}, expected {}",
        row.frame_id,
        expected_frame_id
    );
    ensure!(
        row.frame_len > 0 && row.compressed_len > 0 && row.uncompressed_len > 0,
        "raw gRPC journal has an empty frame at {}",
        row.frame_id
    );
    ensure!(
        row.protobuf_sha256.len() == 64
            && row
                .protobuf_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "invalid raw gRPC checksum at frame {}",
        row.frame_id
    );
    Ok(())
}

fn reconcile_handoff_tail(
    spool: &mut SpoolWriter,
    journal_path: &Path,
    state: &mut JournalState,
    slots_per_epoch: u64,
    max_record_bytes: u64,
) -> Result<bool> {
    if let Some(journal_last) = state.last.as_ref() {
        let stored = read_spool_record(
            spool.journal_dir(),
            journal_last.location(),
            max_record_bytes,
        )?;
        ensure!(
            stored.metadata.observation.sequence == journal_last.frame_id,
            "raw gRPC handoff journal points to wrong WAL frame"
        );
    }
    let Some(spool_last) = spool.last_record().cloned() else {
        ensure!(
            state.records == 0,
            "raw gRPC handoff journal exists without WAL records"
        );
        return Ok(false);
    };
    let spool_sequence = spool_last.metadata().observation.sequence;
    match state.last.as_ref().map(|row| row.frame_id) {
        Some(journal_sequence) if journal_sequence == spool_sequence => {
            let journal_last = state
                .last
                .as_ref()
                .context("missing handoff journal tail")?;
            let authoritative =
                handoff_record_from_stored(spool, &spool_last, slots_per_epoch, max_record_bytes)?;
            ensure!(
                *journal_last == authoritative,
                "raw gRPC handoff journal tail differs from its authoritative WAL frame"
            );
            Ok(false)
        }
        Some(journal_sequence) if journal_sequence > spool_sequence => {
            Err(anyhow!("raw gRPC handoff journal is ahead of durable WAL"))
        }
        Some(journal_sequence) => {
            ensure!(
                spool_sequence == journal_sequence + 1,
                "raw gRPC WAL is more than one frame ahead of handoff journal"
            );
            let row =
                handoff_record_from_stored(spool, &spool_last, slots_per_epoch, max_record_bytes)?;
            append_handoff_record(journal_path, &row)?;
            state.last = Some(row);
            state.records += 1;
            Ok(true)
        }
        None => {
            ensure!(
                spool_sequence == 0,
                "raw gRPC WAL has multiple frames but no handoff journal"
            );
            let row =
                handoff_record_from_stored(spool, &spool_last, slots_per_epoch, max_record_bytes)?;
            append_handoff_record(journal_path, &row)?;
            state.first = Some(row.clone());
            state.last = Some(row);
            state.records = 1;
            Ok(true)
        }
    }
}

fn handoff_record_from_stored(
    spool: &SpoolWriter,
    durable: &crate::ingest::DurableSpoolRecord,
    slots_per_epoch: u64,
    max_record_bytes: u64,
) -> Result<GrpcRawHandoffRecord> {
    let stored = spool.read_record(durable)?;
    let raw_capacity =
        usize::try_from(max_record_bytes).context("max record bytes exceeds usize")?;
    let raw = zstd::bulk::decompress(&stored.payload, raw_capacity)
        .context("decompress orphan raw gRPC WAL frame")?;
    let update =
        SubscribeUpdate::decode(raw.as_slice()).context("decode orphan raw gRPC WAL frame")?;
    let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
        return Err(anyhow!("orphan raw gRPC WAL frame is not a block update"));
    };
    let epoch_slot = EpochSlot::from_slot(block.slot, slots_per_epoch);
    Ok(handoff_record_from_block(
        block,
        durable.metadata().observation.sequence,
        epoch_slot,
        durable.location(),
        raw.len() as u64,
        stored.payload.len() as u64,
        sha256_hex(&raw),
    ))
}

fn handoff_record_from_block(
    block: &SubscribeUpdateBlock,
    frame_id: u64,
    epoch_slot: EpochSlot,
    location: SpoolLocation,
    uncompressed_len: u64,
    compressed_len: u64,
    protobuf_sha256: String,
) -> GrpcRawHandoffRecord {
    GrpcRawHandoffRecord {
        schema_version: JOURNAL_SCHEMA_VERSION,
        frame_id,
        slot: block.slot,
        parent_slot: block.parent_slot,
        epoch: epoch_slot.epoch,
        epoch_slot_index: epoch_slot.slot_index,
        segment_id: location.segment_id,
        frame_offset: location.frame_offset,
        frame_len: location.frame_len,
        compressed_len,
        uncompressed_len,
        protobuf_sha256,
        blockhash: block.blockhash.clone(),
    }
}

fn append_handoff_record(path: &Path, row: &GrpcRawHandoffRecord) -> Result<()> {
    let file = OpenOptions::new()
        .append(true)
        .open(path)
        .with_context(|| format!("open raw gRPC journal {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(16 * 1024, file);
    serde_json::to_writer(&mut writer, row)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_data()?;
    Ok(())
}

fn encoded_handoff_record_len(row: &GrpcRawHandoffRecord) -> Result<u64> {
    let json_len = u64::try_from(serde_json::to_vec(row)?.len())
        .context("raw gRPC handoff record length exceeds u64")?;
    json_len
        .checked_add(1)
        .context("raw gRPC handoff record length overflow")
}

fn projected_raw_generation_bytes(
    current_bytes: u64,
    wal_append_bytes: u64,
    row: &GrpcRawHandoffRecord,
) -> Result<u64> {
    let journal_append_bytes = encoded_handoff_record_len(row)?;
    current_bytes
        .checked_add(wal_append_bytes)
        .and_then(|bytes| bytes.checked_add(journal_append_bytes))
        .context("raw gRPC generation length overflow")
}

fn generation_limit_exceeded(max_generation_bytes: u64, projected_bytes: u64) -> bool {
    max_generation_bytes > 0 && projected_bytes > max_generation_bytes
}

fn validate_generation_limit(config: &GrpcRawRecordConfig) -> Result<()> {
    if config.max_generation_bytes == 0 {
        return Ok(());
    }
    let minimum = minimum_generation_bytes(config)?;
    ensure!(
        config.max_generation_bytes >= minimum,
        "max generation bytes {} is too small for a max-size seed and one max-size append; require at least {}",
        config.max_generation_bytes,
        minimum
    );
    Ok(())
}

fn minimum_generation_bytes(config: &GrpcRawRecordConfig) -> Result<u64> {
    let identity_reserve = GrpcRawIdentityFile {
        schema_version: IDENTITY_SCHEMA_VERSION,
        endpoint: config.endpoint.clone(),
        cluster_id: config.cluster_id.clone(),
        origin_node_id: config.origin_node_id.clone(),
        source_id: config.source_id.clone(),
        journal_id: [u8::MAX; 16],
        replication_journal_id: Some([u8::MAX; 16]),
        replication_sequence_base: Some(u64::MAX),
        payload_format_version: PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
    };
    let identity_bytes = u64::try_from(serde_json::to_vec_pretty(&identity_reserve)?.len())
        .context("raw gRPC identity length exceeds u64")?
        .checked_add(1)
        .context("raw gRPC identity length overflow")?;
    config
        .max_record_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(GENERATION_ROLLOVER_SAFETY_BYTES))
        .and_then(|bytes| bytes.checked_add(identity_bytes))
        .context("minimum raw gRPC generation size overflow")
}

/// Logical bytes in the files required to move and replay one self-contained generation.
fn raw_generation_bytes(identity_path: &Path, wal_root: &Path, journal_path: &Path) -> Result<u64> {
    let identity_bytes = regular_file_bytes(identity_path)?;
    let journal_bytes = regular_file_bytes(journal_path)?;
    let wal_bytes = directory_file_bytes(wal_root)?;
    identity_bytes
        .checked_add(journal_bytes)
        .and_then(|bytes| bytes.checked_add(wal_bytes))
        .context("raw gRPC generation byte count overflow")
}

fn regular_file_bytes(path: &Path) -> Result<u64> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect raw gRPC generation file {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "raw gRPC generation path is not a regular file: {}",
        path.display()
    );
    Ok(metadata.len())
}

fn directory_file_bytes(root: &Path) -> Result<u64> {
    let metadata = fs::symlink_metadata(root)
        .with_context(|| format!("inspect raw gRPC generation directory {}", root.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "raw gRPC generation path is not a real directory: {}",
        root.display()
    );
    let mut total = 0u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).with_context(|| {
            format!("list raw gRPC generation directory {}", directory.display())
        })? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("inspect raw gRPC generation path {}", path.display()))?;
            let file_type = metadata.file_type();
            ensure!(
                !file_type.is_symlink(),
                "raw gRPC generation contains a symlink: {}",
                path.display()
            );
            if file_type.is_dir() {
                pending.push(path);
            } else {
                ensure!(
                    file_type.is_file(),
                    "raw gRPC generation contains a special file: {}",
                    path.display()
                );
                total = total
                    .checked_add(metadata.len())
                    .context("raw gRPC generation byte count overflow")?;
            }
        }
    }
    Ok(total)
}

fn prepare_resume_coverage_warning_path(config: &GrpcRawRecordConfig) -> Result<()> {
    let Some(path) = config.resume_coverage_warning_file.as_deref() else {
        return Ok(());
    };
    let output_expected = config
        .output_dir
        .join(MONITORING_DIR)
        .join(RESUME_COVERAGE_WARNING_FILE);
    // Rolling generations replace `output_dir` atomically. Allow the supervisor
    // to keep an undelivered coverage event in a fixed sibling directory so a
    // generation rotation cannot discard or hide the alert.
    let sibling_expected = config
        .output_dir
        .parent()
        .map(|parent| parent.join("monitoring").join(RESUME_COVERAGE_WARNING_FILE));
    ensure!(
        path == output_expected || sibling_expected.as_deref() == Some(path),
        "resume-coverage warning path must be {} or the fixed sibling monitoring path",
        output_expected.display()
    );
    let parent = path
        .parent()
        .context("resume-coverage warning path has no parent directory")?;
    match fs::symlink_metadata(parent) {
        Ok(metadata) => ensure!(
            metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
            "resume-coverage warning parent is not a real directory: {}",
            parent.display()
        ),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            fs::create_dir(parent).with_context(|| {
                format!(
                    "create resume-coverage warning directory {}",
                    parent.display()
                )
            })?;
            let parent_directory = parent
                .parent()
                .context("resume-coverage warning directory has no parent")?;
            sync_directory(parent_directory)?;
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "inspect resume-coverage warning directory {}",
                    parent.display()
                )
            });
        }
    }
    let _ = read_resume_coverage_warning(path)?;
    Ok(())
}

fn resume_coverage_warning_event_id(
    requested_overlap_slot: u64,
    first_delivered_slot: u64,
    observed_later_slot: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla-grpc-resume-coverage-warning-v1");
    hasher.update(requested_overlap_slot.to_le_bytes());
    hasher.update(first_delivered_slot.to_le_bytes());
    hasher.update(observed_later_slot.to_le_bytes());
    hex_bytes(&hasher.finalize())
}

fn read_resume_coverage_warning(path: &Path) -> Result<Option<GrpcRawResumeCoverageWarning>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect resume-coverage warning {}", path.display()));
        }
    };
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "resume-coverage warning is not a regular file: {}",
        path.display()
    );
    ensure!(
        metadata.len() > 0 && metadata.len() <= 4096,
        "resume-coverage warning has invalid size {}",
        metadata.len()
    );
    let event: GrpcRawResumeCoverageWarning = serde_json::from_slice(
        &fs::read(path)
            .with_context(|| format!("read resume-coverage warning {}", path.display()))?,
    )
    .with_context(|| format!("decode resume-coverage warning {}", path.display()))?;
    ensure!(
        event.schema_version == RESUME_COVERAGE_WARNING_SCHEMA_VERSION,
        "unsupported resume-coverage warning schema {}",
        event.schema_version
    );
    ensure!(
        event.requested_overlap_slot < event.observed_later_slot,
        "resume-coverage warning does not advance beyond its requested overlap"
    );
    ensure!(
        event.event_id
            == resume_coverage_warning_event_id(
                event.requested_overlap_slot,
                event.first_delivered_slot,
                event.observed_later_slot,
            ),
        "resume-coverage warning event ID is invalid"
    );
    Ok(Some(event))
}

fn publish_resume_coverage_warning(
    path: &Path,
    event: &GrpcRawResumeCoverageWarning,
) -> Result<ResumeCoverageWarningPublishOutcome> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .context("resume-coverage warning path has no parent directory")?;
    if let Some(existing) = read_resume_coverage_warning(path)? {
        sync_directory(parent)?;
        return Ok(if existing.event_id == event.event_id {
            ResumeCoverageWarningPublishOutcome::AlreadyPresent
        } else {
            ResumeCoverageWarningPublishOutcome::DifferentPending
        });
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("resume-coverage-warning.json");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let temp_path = path.with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), nonce));
    let result = (|| -> Result<ResumeCoverageWarningPublishOutcome> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .with_context(|| {
                format!(
                    "create resume-coverage warning temp {}",
                    temp_path.display()
                )
            })?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, event)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        let outcome = match fs::hard_link(&temp_path, path) {
            Ok(()) => ResumeCoverageWarningPublishOutcome::Published,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let existing = read_resume_coverage_warning(path)?
                    .context("resume-coverage warning disappeared during publication")?;
                if existing.event_id == event.event_id {
                    ResumeCoverageWarningPublishOutcome::AlreadyPresent
                } else {
                    ResumeCoverageWarningPublishOutcome::DifferentPending
                }
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "publish resume-coverage warning {} -> {}",
                        temp_path.display(),
                        path.display()
                    )
                });
            }
        };
        fs::remove_file(&temp_path).with_context(|| {
            format!(
                "remove resume-coverage warning temp {}",
                temp_path.display()
            )
        })?;
        sync_directory(parent)?;
        Ok(outcome)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn spool_journal_dir(output_dir: &Path, identity: &GrpcRawIdentityFile) -> PathBuf {
    output_dir
        .join(WAL_ROOT_DIR)
        .join(&identity.cluster_id)
        .join(&identity.origin_node_id)
        .join(&identity.source_id)
        .join(hex_bytes(&identity.journal_id))
}

fn ensure_path_absent(path: &Path, description: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(anyhow!("{description} already exists: {}", path.display())),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
    }
}

fn materialization_staging_path(archive_dir: &Path) -> Result<PathBuf> {
    let name = archive_dir
        .file_name()
        .filter(|name| !name.is_empty())
        .context("materialized archive path has no file name")?;
    let mut staging_name = OsString::from(".");
    staging_name.push(name);
    staging_name.push(".raw-materializing");
    Ok(effective_parent(archive_dir)
        .unwrap_or_else(|| Path::new("."))
        .join(staging_name))
}

fn effective_parent(path: &Path) -> Option<&Path> {
    path.parent().map(|parent| {
        if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        }
    })
}

fn rewrite_published_layout(staging_dir: &Path, archive_dir: &Path) -> Result<()> {
    write_json_file_synced(
        &staging_dir.join("producer-layout.json"),
        &ProducerLayout::new(archive_dir),
    )
}

fn validate_materialized_capture(
    staging_dir: &Path,
    archive: &GrpcRawArchiveWriteReport,
) -> Result<()> {
    let inspected = inspect_capture(staging_dir.to_path_buf())
        .context("inspect staged raw gRPC materialization")?;
    ensure!(
        inspected.block_frames as u64 == archive.blocks_written,
        "materialized block count mismatch: wrote {}, inspected {}",
        archive.blocks_written,
        inspected.block_frames
    );
    ensure!(
        inspected.block_index_rows as u64 == archive.blocks_written,
        "materialized block-index count mismatch: wrote {}, inspected {}",
        archive.blocks_written,
        inspected.block_index_rows
    );
    ensure!(
        inspected.poh_frames as u64 == archive.blocks_written,
        "materialized PoH count mismatch: wrote {}, inspected {}",
        archive.blocks_written,
        inspected.poh_frames
    );
    ensure!(
        inspected.transactions as u64 == archive.transactions_written,
        "materialized transaction count mismatch: wrote {}, inspected {}",
        archive.transactions_written,
        inspected.transactions
    );
    ensure!(
        inspected.poh_entries as u64 == archive.entries_written,
        "materialized PoH-entry count mismatch: wrote {}, inspected {}",
        archive.entries_written,
        inspected.poh_entries
    );
    ensure!(
        inspected.first_slot == archive.first_slot && inspected.last_slot == archive.last_slot,
        "materialized slot range differs from writer report"
    );
    ensure!(
        inspected.pubkey_run_files == archive.pubkey_run_files
            && inspected.pubkey_run_records == archive.pubkey_run_records,
        "materialized pubkey-run totals differ from writer report"
    );
    Ok(())
}

fn collect_materialized_artifacts(root: &Path) -> Result<Vec<GrpcRawMaterializedArtifact>> {
    let mut paths = Vec::new();
    collect_regular_files(root, root, &mut paths)?;
    paths.sort();
    paths
        .into_iter()
        .filter(|path| {
            !matches!(
                path.as_str(),
                "producer-layout.json"
                    | "journal/progress.json"
                    | "journal/RAW-MATERIALIZATION-COMPLETE.v1.json"
            )
        })
        .map(|relative| {
            let path = root.join(&relative);
            Ok(GrpcRawMaterializedArtifact {
                path: relative,
                bytes: fs::metadata(&path)
                    .with_context(|| format!("stat materialized artifact {}", path.display()))?
                    .len(),
                sha256: sha256_file(&path)?,
            })
        })
        .collect()
}

fn collect_regular_files(root: &Path, dir: &Path, output: &mut Vec<String>) -> Result<()> {
    let mut entries = fs::read_dir(dir)
        .with_context(|| format!("read materialized directory {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("inspect materialized path {}", path.display()))?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "materialized staging contains a symlink: {}",
            path.display()
        );
        if metadata.is_dir() {
            collect_regular_files(root, &path, output)?;
        } else {
            ensure!(
                metadata.is_file(),
                "materialized staging contains a non-regular file: {}",
                path.display()
            );
            let relative = path
                .strip_prefix(root)
                .context("materialized artifact escaped staging root")?;
            let relative = relative
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            output.push(relative);
        }
    }
    Ok(())
}

fn write_materialization_progress(
    staging_dir: &Path,
    archive_dir: &Path,
    receipt: &GrpcRawMaterializationReceipt,
) -> Result<()> {
    let progress = serde_json::json!({
        "schema_version": 2,
        "phase": "Raw gRPC materialization",
        "state": "materialized_snapshot",
        "capture_dir": archive_dir,
        "epoch": receipt.epoch,
        "first_slot": receipt.first_slot,
        "last_slot": receipt.last_slot,
        "blocks_done": receipt.blocks_written,
        "transactions_done": receipt.transactions_written,
        "entries_done": receipt.entries_written,
        "stopped_at_epoch_boundary": false,
        "updated_unix_secs": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs()),
    });
    write_json_file_synced(
        &staging_dir
            .join("journal")
            .join(MATERIALIZATION_PROGRESS_FILE),
        &progress,
    )
}

fn write_json_file_synced(path: &Path, value: &impl Serialize) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("open JSON output {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)
        .with_context(|| format!("serialize JSON output {}", path.display()))?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn sync_tree(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect path before sync {}", path.display()))?;
    ensure!(
        !metadata.file_type().is_symlink(),
        "refuse to sync symlink in materialized staging: {}",
        path.display()
    );
    if metadata.is_dir() {
        let mut entries = fs::read_dir(path)
            .with_context(|| format!("read directory before sync {}", path.display()))?
            .collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            sync_tree(&entry.path())?;
        }
        sync_directory(path)
    } else {
        ensure!(
            metadata.is_file(),
            "refuse to sync non-regular materialized path: {}",
            path.display()
        );
        File::open(path)
            .with_context(|| format!("open file before sync {}", path.display()))?
            .sync_all()
            .with_context(|| format!("sync materialized file {}", path.display()))
    }
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("open {} for SHA-256", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("read {} for SHA-256", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_bytes(&hasher.finalize()))
}

fn decode_blockhash(value: &str) -> Result<[u8; 32]> {
    let mut bytes = [0u8; 32];
    five8::decode_32(value, &mut bytes)
        .map_err(|err| anyhow!("decode gRPC blockhash {value}: {err:?}"))?;
    Ok(bytes)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn compression_ratio(raw_bytes: u64, compressed_bytes: u64) -> f64 {
    if compressed_bytes == 0 {
        0.0
    } else {
        raw_bytes as f64 / compressed_bytes as f64
    }
}

fn compress_raw_block(
    compressor: &mut zstd::bulk::Compressor<'_>,
    raw: &[u8],
    compressed: &mut Vec<u8>,
    max_record_bytes: u64,
) -> Result<()> {
    let bound = zstd::zstd_safe::compress_bound(raw.len());
    compressed.clear();
    if compressed.capacity() < bound {
        compressed
            .try_reserve_exact(bound)
            .context("reserve raw gRPC compression buffer")?;
    }
    ensure!(
        compressed.capacity() >= bound,
        "raw gRPC compression buffer did not reach required bound"
    );
    compressor
        .compress_to_buffer(raw, compressed)
        .context("compress raw gRPC protobuf block")?;
    ensure!(
        compressed.len() as u64 <= max_record_bytes,
        "compressed gRPC block is {} bytes, over configured maximum {}",
        compressed.len(),
        max_record_bytes
    );
    Ok(())
}

fn low_disk_bytes(path: &Path, minimum: u64) -> Result<Option<u64>> {
    if minimum == 0 {
        return Ok(None);
    }
    let available = filesystem_available_bytes(path)?;
    Ok(disk_floor_reached(available, minimum).then_some(available))
}

fn disk_floor_reached(available: u64, minimum: u64) -> bool {
    minimum > 0 && available < minimum
}

#[cfg(unix)]
fn filesystem_available_bytes(path: &Path) -> Result<u64> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let path = CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("filesystem path contains NUL: {}", path.display()))?;
    // SAFETY: `stat` is valid writable storage and `path` is a NUL-terminated C string retained
    // for the entire call.
    let mut stat = unsafe { std::mem::zeroed::<libc::statvfs>() };
    let result = unsafe { libc::statvfs(path.as_ptr(), &mut stat) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("read filesystem free space");
    }
    (stat.f_bavail as u64)
        .checked_mul(stat.f_frsize as u64)
        .context("filesystem available byte count overflow")
}

#[cfg(not(unix))]
fn filesystem_available_bytes(_path: &Path) -> Result<u64> {
    Ok(u64::MAX)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory for sync {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{
        LiveBlockMissingField, WincodeArchiveV2PohRecord, WincodeLeb128FramedReader,
        WincodeLeb128FramedWriter,
    };
    use std::io::Cursor;
    use yellowstone_grpc_proto::prelude::SubscribeUpdateEntry;

    struct AcceptCumulativeSignature;

    impl crate::ingest::CumulativeAckSignatureVerifier for AcceptCumulativeSignature {
        fn verify_cumulative_ack_signature(
            &self,
            _key_id: &str,
            _signing_bytes: &[u8],
            _signature: &[u8],
        ) -> bool {
            true
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "blockzilla-raw-grpc-{label}-{}-{unique}",
            std::process::id()
        ))
    }

    fn test_config(output_dir: PathBuf) -> GrpcRawRecordConfig {
        GrpcRawRecordConfig {
            endpoint: "https://example.invalid".to_string(),
            output_dir,
            max_record_bytes: 1024 * 1024,
            segment_target_bytes: 1024 * 1024,
            ..GrpcRawRecordConfig::default()
        }
    }

    fn block(slot: u64) -> SubscribeUpdateBlock {
        SubscribeUpdateBlock {
            slot,
            parent_slot: slot - 1,
            blockhash: "11111111111111111111111111111111".to_string(),
            ..Default::default()
        }
    }

    fn update(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["raw-blocks".to_string()],
            update_oneof: Some(UpdateOneof::Block(block(slot))),
            created_at: None,
        }
    }

    fn complete_poh_block(slot: u64) -> SubscribeUpdateBlock {
        SubscribeUpdateBlock {
            slot,
            parent_slot: slot - 1,
            blockhash: "11111111111111111111111111111111".to_string(),
            executed_transaction_count: 0,
            entries_count: 2,
            // Deliberately reverse source order. The entry index is the canonical order.
            entries: vec![
                SubscribeUpdateEntry {
                    slot,
                    index: 1,
                    num_hashes: 7,
                    hash: vec![0; 32],
                    executed_transaction_count: 0,
                    starting_transaction_index: 0,
                },
                SubscribeUpdateEntry {
                    slot,
                    index: 0,
                    num_hashes: 5,
                    hash: vec![0x11; 32],
                    executed_transaction_count: 0,
                    starting_transaction_index: 0,
                },
            ],
            ..Default::default()
        }
    }

    fn complete_poh_update(slot: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec!["raw-blocks".to_string()],
            update_oneof: Some(UpdateOneof::Block(complete_poh_block(slot))),
            created_at: None,
        }
    }

    fn append_fixture(
        spool: &mut SpoolWriter,
        identity: &GrpcRawIdentityFile,
        update: &SubscribeUpdate,
        frame_id: u64,
    ) -> GrpcRawHandoffRecord {
        let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
            panic!("fixture update is not a block")
        };
        let raw = update.encode_to_vec();
        let compressed = zstd::bulk::compress(&raw, 1).unwrap();
        let metadata = IngressRecordMeta::from_payload(
            identity.cluster_id.clone(),
            ObservationId {
                origin_node_id: identity.origin_node_id.clone(),
                journal_id: identity.journal_id,
                sequence: frame_id,
            },
            identity.source_id.clone(),
            LogicalKey::Block {
                slot: block.slot,
                blockhash: decode_blockhash(&block.blockhash).unwrap(),
            },
            PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
            &compressed,
        );
        let durable = spool.append_and_sync(metadata, &compressed).unwrap();
        handoff_record_from_block(
            block,
            frame_id,
            EpochSlot::from_slot(block.slot, OLD_FAITHFUL_SLOTS_PER_EPOCH),
            durable.location(),
            raw.len() as u64,
            compressed.len() as u64,
            sha256_hex(&raw),
        )
    }

    #[test]
    fn generation_cap_preflight_stops_before_mutation_and_matches_written_size() {
        let root = temp_dir("generation-cap-preflight");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let current_bytes = raw_generation_bytes(
            &root.join(IDENTITY_FILE),
            &root.join(WAL_ROOT_DIR),
            &journal,
        )
        .unwrap();

        let source = complete_poh_update(1_000);
        let Some(UpdateOneof::Block(block)) = source.update_oneof.as_ref() else {
            panic!("fixture update is not a block")
        };
        let raw = source.encode_to_vec();
        let compressed = zstd::bulk::compress(&raw, 1).unwrap();
        let metadata = IngressRecordMeta::from_payload(
            identity.cluster_id.clone(),
            ObservationId {
                origin_node_id: identity.origin_node_id.clone(),
                journal_id: identity.journal_id,
                sequence: 0,
            },
            identity.source_id.clone(),
            LogicalKey::Block {
                slot: block.slot,
                blockhash: decode_blockhash(&block.blockhash).unwrap(),
            },
            PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
            &compressed,
        );
        let projection = spool.project_append(&metadata, &compressed).unwrap();
        let row = handoff_record_from_block(
            block,
            0,
            EpochSlot::from_slot(block.slot, OLD_FAITHFUL_SLOTS_PER_EPOCH),
            projection.location,
            raw.len() as u64,
            compressed.len() as u64,
            sha256_hex(&raw),
        );
        let projected =
            projected_raw_generation_bytes(current_bytes, projection.additional_bytes, &row)
                .unwrap();

        assert!(!generation_limit_exceeded(0, projected));
        assert!(!generation_limit_exceeded(projected, projected));
        assert!(generation_limit_exceeded(projected - 1, projected));
        assert!(spool.last_record().is_none());
        assert_eq!(recover_handoff_journal(&journal).unwrap().records, 0);

        let durable = spool.append_and_sync(metadata, &compressed).unwrap();
        assert_eq!(durable.location(), projection.location);
        append_handoff_record(&journal, &row).unwrap();
        let written = raw_generation_bytes(
            &root.join(IDENTITY_FILE),
            &root.join(WAL_ROOT_DIR),
            &journal,
        )
        .unwrap();
        assert_eq!(written, projected);
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn generation_limit_requires_seed_plus_one_maximum_record() {
        let root = temp_dir("generation-limit-minimum");
        let mut config = test_config(root);
        config.max_generation_bytes = 0;
        validate_generation_limit(&config).unwrap();

        let minimum = minimum_generation_bytes(&config).unwrap();
        config.max_generation_bytes = minimum - 1;
        let error = validate_generation_limit(&config).unwrap_err();
        assert!(error.to_string().contains("max-size seed"));
        assert!(error.to_string().contains(&minimum.to_string()));

        config.max_generation_bytes = minimum;
        validate_generation_limit(&config).unwrap();
    }

    #[test]
    fn existing_generation_at_limit_returns_explicit_full_report_without_connecting() {
        let root = temp_dir("generation-already-full");
        fs::create_dir_all(&root).unwrap();
        let mut config = test_config(root.clone());
        config.max_record_bytes = 1024;
        config.segment_target_bytes = 1024 * 1024;
        config.max_generation_bytes = 2 * 1024 * 1024;
        config.min_free_bytes = 0;
        validate_generation_limit(&config).unwrap();
        let identity = load_or_create_identity(&config).unwrap();
        let spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        recover_handoff_journal(&root.join(HANDOFF_JOURNAL_FILE)).unwrap();
        fs::write(
            spool.journal_dir().join("retention-accounting-padding"),
            vec![0u8; config.max_generation_bytes as usize],
        )
        .unwrap();
        drop(spool);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = runtime.block_on(record_grpc_raw_blocks(config)).unwrap();
        assert!(report.stopped_generation_full);
        assert_eq!(report.frames_seen, 0);
        assert_eq!(report.frames_written, 0);
        assert!(report.generation_bytes >= 2 * 1024 * 1024);
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["stopped_generation_full"], true);
        assert!(json["generation_bytes"].as_u64().is_some());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn seeds_exact_verified_tail_into_empty_generation_as_frame_zero() {
        let parent = temp_dir("seed-generation");
        let source_dir = parent.join("source");
        let target_dir = parent.join("target");
        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir(&target_dir).unwrap();
        let config = test_config(source_dir.clone());
        let source_identity = load_or_create_identity(&config).unwrap();
        let mut source_spool = SpoolWriter::open(
            source_dir.join(WAL_ROOT_DIR),
            source_identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let source_journal = source_dir.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&source_journal).unwrap();
        let mut source_tail = None;
        for frame_id in 0..3 {
            let row = append_fixture(
                &mut source_spool,
                &source_identity,
                &complete_poh_update(2_000 + frame_id),
                frame_id,
            );
            append_handoff_record(&source_journal, &row).unwrap();
            source_tail = Some(row);
        }
        let source_tail = source_tail.unwrap();
        let source_payload = source_spool
            .read_record(source_spool.last_record().unwrap())
            .unwrap()
            .payload;
        drop(source_spool);

        let report = seed_grpc_raw_generation(
            source_dir.clone(),
            target_dir.clone(),
            config.max_record_bytes,
        )
        .unwrap();
        assert_eq!(report.source_records_verified, 3);
        assert_eq!(report.seeded_slot, source_tail.slot);
        assert_eq!(report.seeded_blockhash, source_tail.blockhash);
        assert_eq!(report.compressed_bytes_copied, source_payload.len() as u64);

        let target_identity = read_identity(&target_dir.join(IDENTITY_FILE)).unwrap();
        assert_eq!(target_identity.endpoint, source_identity.endpoint);
        assert_eq!(target_identity.cluster_id, source_identity.cluster_id);
        assert_eq!(
            target_identity.origin_node_id,
            source_identity.origin_node_id
        );
        assert_eq!(target_identity.source_id, source_identity.source_id);
        assert_ne!(target_identity.journal_id, source_identity.journal_id);
        assert_eq!(
            target_identity.replication_stream_id(),
            source_identity.replication_stream_id()
        );
        assert_eq!(
            target_identity.replication_sequence(0).unwrap(),
            source_identity
                .replication_sequence(source_tail.frame_id)
                .unwrap()
        );
        let target_state =
            read_handoff_journal(&target_dir.join(HANDOFF_JOURNAL_FILE), false).unwrap();
        assert_eq!(target_state.records, 1);
        let target_tail = target_state.last.unwrap();
        assert_eq!(target_tail.frame_id, 0);
        assert_eq!(target_tail.slot, source_tail.slot);
        assert_eq!(
            effective_resume_slot(None, Some(target_tail.slot), None),
            Some(source_tail.slot)
        );
        let target_payload = read_spool_record(
            spool_journal_dir(&target_dir, &target_identity),
            target_tail.location(),
            config.max_record_bytes,
        )
        .unwrap()
        .payload;
        assert_eq!(target_payload, source_payload);
        let verified = verify_grpc_raw_poh(target_dir.clone(), config.max_record_bytes, 1).unwrap();
        assert_eq!(verified.records_verified, 1);
        assert_eq!(verified.first_slot, Some(source_tail.slot));
        assert_eq!(verified.last_slot, Some(source_tail.slot));
        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn hot_rotation_durably_appends_trigger_before_honoring_the_new_disk_floor() {
        let cache_root = temp_dir("hot-generation");
        let active = cache_root.join(HOT_ACTIVE_DIR);
        fs::create_dir_all(&active).unwrap();
        fs::create_dir(cache_root.join(HOT_SEALED_DIR)).unwrap();
        fs::create_dir(cache_root.join(HOT_RECEIPTS_DIR)).unwrap();
        let mut config = test_config(active.clone());
        config.hot_generation_root = Some(cache_root.clone());
        config.max_generation_bytes = minimum_generation_bytes(&config).unwrap();
        validate_hot_generation_config(&config).unwrap();

        let mut identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            active.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = active.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let mut source_tail = None;
        for frame_id in 0..3 {
            let row = append_fixture(
                &mut spool,
                &identity,
                &complete_poh_update(7_000 + frame_id),
                frame_id,
            );
            append_handoff_record(&journal, &row).unwrap();
            source_tail = Some(row);
        }
        let source_tail = source_tail.unwrap();
        let gap_dir = active.join(REPLAY_GAPS_DIR);
        fs::create_dir(&gap_dir).unwrap();
        fs::write(gap_dir.join("fixture.json"), b"{}\n").unwrap();

        let source_journal_id = identity.journal_id;
        let rotated = hot_rotate_generation(
            &config,
            &cache_root,
            &mut identity,
            &mut spool,
            &source_tail,
        )
        .unwrap();
        assert_eq!(rotated.next_frame_id, 1);
        assert_eq!(rotated.seeded_tail.frame_id, 0);
        assert_eq!(rotated.seeded_tail.slot, source_tail.slot);
        assert_ne!(identity.journal_id, source_journal_id);
        assert_eq!(
            identity.replication_stream_id().journal_id,
            source_journal_id
        );
        assert_eq!(
            identity.replication_sequence(0).unwrap(),
            source_tail.frame_id
        );
        assert!(!cache_root.join(HOT_ROTATION_MARKER).exists());
        assert!(
            cache_root
                .join(HOT_SEALED_DIR)
                .join(format!("slot-{:020}", source_tail.slot))
                .is_dir()
        );
        assert_eq!(
            fs::read(active.join(REPLAY_GAPS_DIR).join("fixture.json")).unwrap(),
            b"{}\n"
        );

        // This is the already-decoded block that crossed the predecessor's cap. It becomes frame
        // one in the successor without another subscription or a resume overlap.
        let trigger = complete_poh_update(source_tail.slot + 1);
        let trigger_row = append_fixture(&mut spool, &identity, &trigger, rotated.next_frame_id);
        append_handoff_record(&journal, &trigger_row).unwrap();
        assert_eq!(read_handoff_journal(&journal, false).unwrap().records, 2);
        assert!(disk_floor_reached(9, 10));
        drop(spool);
        let active_verification =
            verify_grpc_raw_poh(active.clone(), config.max_record_bytes, 2).unwrap();
        assert_eq!(active_verification.records_verified, 2);
        assert_eq!(active_verification.first_slot, Some(source_tail.slot));
        assert_eq!(active_verification.last_slot, Some(source_tail.slot + 1));
        fs::remove_dir_all(cache_root).unwrap();
    }

    #[test]
    fn hot_generation_sequence_never_reuses_a_repeated_tail_slot() {
        let cache_root = temp_dir("hot-generation-sequence");
        fs::create_dir_all(cache_root.join(HOT_ACTIVE_DIR)).unwrap();
        fs::create_dir(cache_root.join(HOT_SEALED_DIR)).unwrap();
        fs::create_dir(cache_root.join(HOT_RECEIPTS_DIR)).unwrap();

        assert_eq!(allocate_hot_generation_number(&cache_root, 42).unwrap(), 42);
        assert_eq!(read_hot_generation_sequence(&cache_root).unwrap(), Some(42));
        assert_eq!(allocate_hot_generation_number(&cache_root, 42).unwrap(), 43);
        assert_eq!(read_hot_generation_sequence(&cache_root).unwrap(), Some(43));
        assert_eq!(allocate_hot_generation_number(&cache_root, 7).unwrap(), 44);

        fs::remove_dir_all(cache_root).unwrap();
    }

    #[test]
    fn hot_generation_sequence_bootstraps_from_old_upload_receipts() {
        let cache_root = temp_dir("hot-generation-receipt-sequence");
        fs::create_dir_all(cache_root.join(HOT_ACTIVE_DIR)).unwrap();
        fs::create_dir(cache_root.join(HOT_SEALED_DIR)).unwrap();
        let receipts = cache_root.join(HOT_RECEIPTS_DIR);
        fs::create_dir(&receipts).unwrap();
        fs::write(receipts.join("slot-00000000000000000123.json"), b"{}\n").unwrap();

        assert_eq!(
            allocate_hot_generation_number(&cache_root, 100).unwrap(),
            124
        );
        assert_eq!(
            read_hot_generation_sequence(&cache_root).unwrap(),
            Some(124)
        );

        fs::remove_dir_all(cache_root).unwrap();
    }

    #[test]
    fn hot_rotation_crash_states_remain_recoverable_by_the_supervisor_marker() {
        for fail_at in [
            HotRotationStep::MarkerPublished,
            HotRotationStep::OldGenerationHidden,
            HotRotationStep::SuccessorActive,
            HotRotationStep::SealedGenerationVisible,
        ] {
            let cache_root = temp_dir("hot-generation-crash");
            fs::create_dir_all(cache_root.join(HOT_ACTIVE_DIR)).unwrap();
            fs::create_dir(cache_root.join(HOT_SEALED_DIR)).unwrap();
            fs::create_dir(cache_root.join(HOT_RECEIPTS_DIR)).unwrap();
            let layout = hot_rotation_layout(&cache_root, 42);
            fs::create_dir(&layout.next).unwrap();
            let error = publish_hot_rotation_layout(&layout, |step| {
                if step == fail_at {
                    Err(anyhow!("fixture crash after {step:?}"))
                } else {
                    Ok(())
                }
            })
            .and_then(|()| {
                publish_hot_sealed_generation(&layout, |step| {
                    if step == fail_at {
                        Err(anyhow!("fixture crash after {step:?}"))
                    } else {
                        Ok(())
                    }
                })
            })
            .unwrap_err();
            assert!(error.to_string().contains("fixture crash"));
            assert!(layout.marker.is_file());
            match fail_at {
                HotRotationStep::MarkerPublished => {
                    assert!(layout.active.is_dir());
                    assert!(layout.next.is_dir());
                    assert!(!layout.hidden_old.exists());
                    assert!(!layout.sealed.exists());
                }
                HotRotationStep::OldGenerationHidden => {
                    assert!(!layout.active.exists());
                    assert!(layout.next.is_dir());
                    assert!(layout.hidden_old.is_dir());
                    assert!(!layout.sealed.exists());
                }
                HotRotationStep::SuccessorActive => {
                    assert!(layout.active.is_dir());
                    assert!(!layout.next.exists());
                    assert!(layout.hidden_old.is_dir());
                    assert!(!layout.sealed.exists());
                }
                HotRotationStep::SealedGenerationVisible => {
                    assert!(layout.active.is_dir());
                    assert!(!layout.next.exists());
                    assert!(!layout.hidden_old.exists());
                    assert!(layout.sealed.is_dir());
                }
            }
            fs::remove_dir_all(cache_root).unwrap();
        }
    }

    #[test]
    fn seed_refuses_an_active_source_and_cleans_temporary_target() {
        let parent = temp_dir("seed-active-source");
        let source_dir = parent.join("source");
        let target_dir = parent.join("target");
        fs::create_dir_all(&source_dir).unwrap();
        let config = test_config(source_dir.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            source_dir.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = source_dir.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let row = append_fixture(&mut spool, &identity, &complete_poh_update(3_000), 0);
        append_handoff_record(&journal, &row).unwrap();

        let error = seed_grpc_raw_generation(
            source_dir.clone(),
            target_dir.clone(),
            config.max_record_bytes,
        )
        .unwrap_err();
        assert!(error.to_string().contains("lock and audit source"));
        assert!(!target_dir.exists());
        assert_eq!(fs::read_dir(&parent).unwrap().count(), 1);
        drop(spool);
        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn builds_minimal_subscription_ping_reply() {
        assert_eq!(
            subscribe_ping_request(),
            SubscribeRequest {
                ping: Some(SubscribeRequestPing {
                    id: SUBSCRIBE_PING_ID,
                }),
                ..Default::default()
            }
        );
    }

    #[test]
    fn ping_reply_receiver_closure_is_sticky_and_response_drain_can_continue() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (mut disconnected_sink, disconnected_receiver) =
                mpsc::channel::<SubscribeRequest>(1);
            drop(disconnected_receiver);
            let mut request_side_open = true;
            let outcome = reply_to_subscription_ping(
                &mut request_side_open,
                &mut disconnected_sink,
                deadline_after(TokioInstant::now(), 1, "test total timeout").unwrap(),
                None,
            )
            .await;
            assert_eq!(outcome, SubscribePingReplyOutcome::RequestSideClosed);
            assert!(!request_side_open);

            // A later Ping must not attempt a send after the closure. Use a fresh, healthy channel
            // so an accidental retry would be observable rather than merely failing again.
            let (mut healthy_sink, mut healthy_receiver) = mpsc::channel::<SubscribeRequest>(1);
            let outcome = reply_to_subscription_ping(
                &mut request_side_open,
                &mut healthy_sink,
                deadline_after(TokioInstant::now(), 1, "test total timeout").unwrap(),
                None,
            )
            .await;
            assert_eq!(outcome, SubscribePingReplyOutcome::AlreadyClosed);
            assert!(healthy_receiver.try_recv().is_err());
        });
    }

    #[test]
    fn healthy_ping_reply_is_still_sent() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (mut request_sink, mut request_receiver) = mpsc::channel::<SubscribeRequest>(1);
            let mut request_side_open = true;
            let outcome = reply_to_subscription_ping(
                &mut request_side_open,
                &mut request_sink,
                deadline_after(TokioInstant::now(), 1, "test total timeout").unwrap(),
                None,
            )
            .await;
            assert_eq!(outcome, SubscribePingReplyOutcome::Sent);
            assert!(request_side_open);
            assert_eq!(
                request_receiver.try_recv().unwrap(),
                subscribe_ping_request()
            );
        });
    }

    #[test]
    fn auth_statuses_produce_action_required_raw_reports() {
        let output_dir = temp_dir("auth-report");
        let mut config = test_config(output_dir.clone());
        // Build a real report without contacting the network, then exercise the
        // same status-to-report path used by subscribe and stream failures.
        config.min_free_bytes = u64::MAX;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut report = runtime.block_on(record_grpc_raw_blocks(config)).unwrap();

        for (status, expected_reason) in [
            (
                Status::permission_denied("provider HTTP 403: credit exhausted"),
                GrpcRawRecordRetryReason::PermissionDenied,
            ),
            (
                Status::unauthenticated("token rejected"),
                GrpcRawRecordRetryReason::Unauthenticated,
            ),
        ] {
            let retry = classify_raw_record_status(&status, GrpcRawRecordRetryReason::StreamError);
            apply_raw_record_retry(&mut report, retry);
            assert_eq!(report.outcome, GrpcRawRecordOutcome::Retryable);
            assert_eq!(report.retry_reason, Some(expected_reason));
            assert!(report.action_required);
            let json = serde_json::to_value(&report).unwrap();
            assert_eq!(json["outcome"], "retryable");
            assert!(json["action_required"].as_bool().unwrap());
        }

        let replay = classify_raw_record_status(
            &Status::out_of_range("broadcast from 100 is not available, last available: 200"),
            GrpcRawRecordRetryReason::StreamError,
        );
        assert_eq!(replay.reason, GrpcRawRecordRetryReason::StreamError);
        assert!(!replay.action_required);
        fs::remove_dir_all(output_dir).unwrap();
    }

    #[test]
    fn normalizes_response_encoding_without_exposing_unknown_metadata() {
        let mut metadata = MetadataMap::new();
        assert_eq!(
            grpc_response_encoding(&metadata),
            GrpcResponseEncoding::Identity
        );

        metadata.insert("grpc-encoding", "zstd".parse().unwrap());
        assert_eq!(
            grpc_response_encoding(&metadata),
            GrpcResponseEncoding::Zstd
        );

        metadata.insert("grpc-encoding", "gzip".parse().unwrap());
        assert_eq!(
            grpc_response_encoding(&metadata),
            GrpcResponseEncoding::Gzip
        );

        metadata.insert("grpc-encoding", "private-looking-value".parse().unwrap());
        assert_eq!(
            grpc_response_encoding(&metadata),
            GrpcResponseEncoding::Other
        );
        assert_eq!(GrpcResponseEncoding::Other.as_str(), "other");
    }

    #[test]
    fn watchdog_deadlines_reject_overflow_and_allow_disabling_idle_timeout() {
        let now = TokioInstant::now();
        assert_eq!(idle_deadline_after(now, 0).unwrap(), None);
        assert!(deadline_after(now, 1, "test timeout").is_ok());
        assert!(deadline_after(now, u64::MAX, "test timeout").is_err());

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let outcome = runtime.block_on(await_with_watchdogs(
            std::future::pending::<()>(),
            deadline_after(now, 1, "test timeout").unwrap(),
            Some(now),
        ));
        assert_eq!(outcome, WatchdogOutcome::IdleTimeout);
    }

    #[test]
    fn low_disk_admission_does_not_poll_the_next_stream_update() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut stream = futures::stream::iter([7u64]);
            let now = TokioInstant::now();
            let outcome = next_with_disk_admission(
                &mut stream,
                || Ok(Some(123)),
                deadline_after(now, 1, "test timeout").unwrap(),
                None,
            )
            .await
            .unwrap();
            assert_eq!(outcome, DiskAdmittedOutcome::LowDisk(123));
            assert_eq!(stream.next().await, Some(7));
        });
    }

    #[test]
    fn minimum_resume_floor_can_advance_an_older_durable_tail() {
        assert_eq!(effective_resume_slot(Some(100), None, None), Some(100));
        assert_eq!(effective_resume_slot(None, None, None), None);
        assert_eq!(effective_resume_slot(Some(999), Some(123), None), Some(123));
        assert_eq!(
            effective_resume_slot(Some(1), Some(123), Some(456)),
            Some(456)
        );
        assert_eq!(effective_resume_slot(None, None, Some(456)), Some(456));
        assert_eq!(
            effective_resume_slot(Some(999), Some(123), Some(456)),
            Some(456)
        );
        assert_eq!(
            effective_resume_slot(None, Some(u64::MAX), Some(456)),
            Some(u64::MAX)
        );
    }

    #[test]
    fn strictly_parses_provider_replay_window_message() {
        assert_eq!(
            parse_replay_unavailable_message(
                "broadcast from 432750943 is not available, last available: 432802559"
            ),
            Some(ReplayUnavailable {
                requested_slot: 432750943,
                available_slot: 432802559,
            })
        );
        assert_eq!(
            parse_replay_unavailable_message(&format!(
                "broadcast from 0 is not available, last available: {}",
                u64::MAX
            )),
            Some(ReplayUnavailable {
                requested_slot: 0,
                available_slot: u64::MAX,
            })
        );

        for invalid in [
            " broadcast from 1 is not available, last available: 2",
            "broadcast from 1 is not available, last available: 2\n",
            "broadcast from +1 is not available, last available: 2",
            "broadcast from 1 is not available, last available: -2",
            "broadcast from 1 is unavailable, last available: 2",
            "broadcast from 1 is not available, last available: 2 extra",
            "broadcast from 1 is not available, last available: 18446744073709551616",
            "broadcast from 1 is not available, last available: ",
        ] {
            assert_eq!(parse_replay_unavailable_message(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn only_accepts_advancing_out_of_range_status_for_the_exact_request() {
        let message = "broadcast from 100 is not available, last available: 120";
        let expected = ReplayUnavailable {
            requested_slot: 100,
            available_slot: 120,
        };
        assert_eq!(
            replay_unavailable_from_status(&Status::out_of_range(message), Some(100), 0, 0),
            Some(expected)
        );
        assert_eq!(
            replay_unavailable_from_status(&Status::internal(message), Some(100), 0, 0),
            None
        );
        assert_eq!(
            replay_unavailable_from_status(&Status::out_of_range(message), Some(99), 0, 0),
            None
        );
        assert_eq!(
            replay_unavailable_from_status(&Status::out_of_range(message), None, 0, 0),
            None
        );
        assert_eq!(
            replay_unavailable_from_status(&Status::out_of_range(message), Some(100), 1, 0),
            None
        );
        assert_eq!(
            replay_unavailable_from_status(&Status::out_of_range(message), Some(100), 0, 1),
            None
        );
        assert_eq!(
            replay_unavailable_from_status(
                &Status::out_of_range("broadcast from 100 is not available, last available: 100"),
                Some(100),
                0,
                0,
            ),
            None
        );
    }

    #[test]
    fn validates_complete_reconstructable_poh() {
        let stats = validate_complete_poh_block(&complete_poh_block(500)).unwrap();
        assert_eq!(stats.entries, 2);
        assert_eq!(stats.transaction_references, 0);
        assert_eq!(stats.num_hashes, 12);

        let mut partitioned = complete_poh_block(500);
        partitioned.executed_transaction_count = 3;
        partitioned.transactions = (0..3).map(|_| Default::default()).collect();
        partitioned.entries[1].executed_transaction_count = 1;
        partitioned.entries[0].starting_transaction_index = 1;
        partitioned.entries[0].executed_transaction_count = 2;
        let partitioned_stats = validate_complete_poh_block(&partitioned).unwrap();
        assert_eq!(partitioned_stats.transaction_references, 3);

        let mut invalid_partition = partitioned;
        invalid_partition.entries[0].starting_transaction_index = 2;
        assert!(
            validate_complete_poh_block(&invalid_partition)
                .unwrap_err()
                .to_string()
                .contains("starts at transaction 2, expected 1")
        );

        let mut missing = complete_poh_block(500);
        missing.entries_count = 3;
        assert!(
            validate_complete_poh_block(&missing)
                .unwrap_err()
                .to_string()
                .contains("declares 3 PoH entries but contains 2")
        );

        let mut wrong_final_hash = complete_poh_block(500);
        wrong_final_hash.entries[0].hash = vec![0x22; 32];
        assert!(
            validate_complete_poh_block(&wrong_final_hash)
                .unwrap_err()
                .to_string()
                .contains("final PoH entry hash differs")
        );

        let mut duplicate_index = complete_poh_block(500);
        duplicate_index.entries[0].index = 0;
        assert!(
            validate_complete_poh_block(&duplicate_index)
                .unwrap_err()
                .to_string()
                .contains("PoH entry index 0, expected 1")
        );
    }

    #[test]
    fn raw_wal_replay_produces_archive_v2_poh_record() {
        let root = temp_dir("poh-sidecar-replay");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let source = complete_poh_update(800);
        let row = append_fixture(&mut spool, &identity, &source, 0);
        append_handoff_record(&journal, &row).unwrap();
        drop(spool);

        let verified = verify_grpc_raw_poh(root.clone(), config.max_record_bytes, 1).unwrap();
        assert_eq!(verified.records_verified, 1);
        assert_eq!(verified.poh_entries, 2);
        assert_eq!(verified.transaction_references, 0);
        assert_eq!(verified.num_hashes, "12");

        let mut encoded = Vec::new();
        let count = replay_grpc_raw_blocks(&root, config.max_record_bytes, |_row, decoded| {
            let Some(UpdateOneof::Block(block)) = decoded.update_oneof else {
                panic!("replayed update is not a block")
            };
            let converted = crate::grpc::convert_grpc_block(&block, 37)?;
            assert!(
                !converted
                    .missing
                    .contains(&LiveBlockMissingField::PohEntries)
            );
            let mut writer = WincodeLeb128FramedWriter::new(&mut encoded);
            writer.write(&WincodeArchiveV2PohRecord {
                block_id: 37,
                slot: block.slot,
                entries: converted.poh_entries,
            })?;
            writer.flush()?;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);

        let mut reader = WincodeLeb128FramedReader::new(Cursor::new(encoded));
        let (_len, record) = reader.read::<WincodeArchiveV2PohRecord>().unwrap().unwrap();
        assert_eq!(record.block_id, 37);
        assert_eq!(record.slot, 800);
        assert_eq!(record.entries.len(), 2);
        assert_eq!(record.entries[0].num_hashes, 5);
        assert_eq!(record.entries[0].hash, [0x11; 32]);
        assert_eq!(record.entries[1].num_hashes, 7);
        assert_eq!(record.entries[1].hash, [0; 32]);
        assert!(
            reader
                .read::<WincodeArchiveV2PohRecord>()
                .unwrap()
                .is_none()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn poh_verifier_requires_records_unless_explicitly_allowed_empty() {
        let root = temp_dir("empty-poh-verification");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        recover_handoff_journal(&root.join(HANDOFF_JOURNAL_FILE)).unwrap();
        drop(spool);

        let empty = verify_grpc_raw_poh(root.clone(), config.max_record_bytes, 0).unwrap();
        assert_eq!(empty.records_verified, 0);
        assert_eq!(empty.wal_incomplete_tail_bytes, 0);
        assert!(
            verify_grpc_raw_poh(root.clone(), config.max_record_bytes, 1)
                .unwrap_err()
                .to_string()
                .contains("below required minimum 1")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn poh_verifier_rejects_wal_ahead_of_handoff_journal() {
        let root = temp_dir("wal-ahead-poh-verification");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        recover_handoff_journal(&root.join(HANDOFF_JOURNAL_FILE)).unwrap();
        append_fixture(&mut spool, &identity, &complete_poh_update(900), 0);
        drop(spool);

        assert!(
            verify_grpc_raw_poh(root.clone(), config.max_record_bytes, 0)
                .unwrap_err()
                .to_string()
                .contains("journal is empty but WAL has durable frame 0")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn poh_verifier_rejects_truncated_handoff_journal() {
        let root = temp_dir("truncated-journal-poh-verification");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        for frame_id in 0..2 {
            let row = append_fixture(
                &mut spool,
                &identity,
                &complete_poh_update(910 + frame_id),
                frame_id,
            );
            append_handoff_record(&journal, &row).unwrap();
        }
        drop(spool);

        let contents = fs::read(&journal).unwrap();
        let first_line_len = contents.iter().position(|byte| *byte == b'\n').unwrap() + 1;
        OpenOptions::new()
            .write(true)
            .open(&journal)
            .unwrap()
            .set_len(first_line_len as u64)
            .unwrap();
        assert!(
            verify_grpc_raw_poh(root.clone(), config.max_record_bytes, 0)
                .unwrap_err()
                .to_string()
                .contains("WAL/journal tail frame id mismatch")
        );

        let contents = fs::read(&journal).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&journal)
            .unwrap()
            .set_len((contents.len() - 1) as u64)
            .unwrap();
        assert!(
            verify_grpc_raw_poh(root.clone(), config.max_record_bytes, 0)
                .unwrap_err()
                .to_string()
                .contains("partial raw gRPC journal tail")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn replays_independent_compressed_protobuf_frames() {
        let root = temp_dir("replay");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        for frame_id in 0..3 {
            let source = update(100 + frame_id);
            let row = append_fixture(&mut spool, &identity, &source, frame_id);
            append_handoff_record(&journal, &row).unwrap();
        }
        drop(spool);
        let mut slots = Vec::new();
        let count = replay_grpc_raw_blocks(&root, config.max_record_bytes, |row, decoded| {
            assert_eq!(decoded.filters, vec!["raw-blocks"]);
            let Some(UpdateOneof::Block(block)) = decoded.update_oneof else {
                panic!("replayed update is not a block")
            };
            assert_eq!(row.slot, block.slot);
            slots.push(block.slot);
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 3);
        assert_eq!(slots, vec![100, 101, 102]);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn committed_reader_reads_active_writer_and_only_the_handoff_prefix() {
        let root = temp_dir("committed-reader-active");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let first_update = update(1_000);
        let first_row = append_fixture(&mut spool, &identity, &first_update, 0);
        append_handoff_record(&journal, &first_row).unwrap();
        let second_update = update(1_001);
        let second_row = append_fixture(&mut spool, &identity, &second_update, 1);

        let expected_compressed = zstd::bulk::compress(&first_update.encode_to_vec(), 1).unwrap();
        let limits = GrpcRawCommittedReadLimits {
            max_compressed_record_bytes: config.max_record_bytes,
            max_uncompressed_record_bytes: config.max_record_bytes,
        };
        let mut committed = Vec::new();
        let report = read_grpc_raw_committed_records(&root, limits, |record| {
            committed.push(record);
            Ok(())
        })
        .unwrap();
        assert_eq!(report.records, 1);
        assert_eq!(report.incomplete_handoff_tail_bytes, 0);
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].cluster_id, identity.cluster_id);
        assert_eq!(committed[0].source_id, identity.source_id);
        assert_eq!(
            committed[0].observation.origin_node_id,
            identity.origin_node_id
        );
        assert_eq!(committed[0].observation.journal_id, identity.journal_id);
        assert_eq!(committed[0].observation.sequence, 0);
        assert_eq!(committed[0].slot, 1_000);
        assert_eq!(committed[0].blockhash, first_row.blockhash);
        assert_eq!(
            committed[0].content_digest,
            crate::ingest::compute_content_digest(
                &identity.cluster_id,
                &LogicalKey::Block {
                    slot: 1_000,
                    blockhash: decode_blockhash(&first_row.blockhash).unwrap(),
                },
                PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
                &expected_compressed,
            )
        );
        assert_eq!(
            committed[0].payload_format_version,
            PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1
        );
        assert_eq!(committed[0].raw_protobuf_sha256, first_row.protobuf_sha256);
        assert_eq!(
            committed[0].uncompressed_bytes,
            first_update.encoded_len() as u64
        );
        assert_eq!(committed[0].compressed_bytes, expected_compressed);
        assert_eq!(committed[0].update, first_update);

        // The writer still owns its exclusive lock. Publishing the second handoff row makes that
        // already-fsynced WAL record visible without closing or reopening the writer.
        append_handoff_record(&journal, &second_row).unwrap();
        let mut slots = Vec::new();
        let report = read_grpc_raw_committed_records(&root, limits, |record| {
            slots.push(record.slot);
            Ok(())
        })
        .unwrap();
        assert_eq!(report.records, 2);
        assert_eq!(slots, vec![1_000, 1_001]);

        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn committed_cursor_batches_incrementally_and_restarts_from_a_durable_ack() {
        let root = temp_dir("committed-cursor-restart");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let mut rows = Vec::new();
        for frame_id in 0..3 {
            let row = append_fixture(&mut spool, &identity, &update(1_050 + frame_id), frame_id);
            append_handoff_record(&journal, &row).unwrap();
            rows.push(row);
        }
        let limits = GrpcRawCommittedReadLimits {
            max_compressed_record_bytes: config.max_record_bytes,
            max_uncompressed_record_bytes: config.max_record_bytes,
        };

        let mut cursor = GrpcRawCommittedCursor::open(&root, limits, 0).unwrap();
        assert_eq!(cursor.next_frame_id(), 0);
        assert_eq!(cursor.stream().cluster_id, identity.cluster_id);
        let first = cursor
            .read_batch(2, config.max_record_bytes, config.max_record_bytes)
            .unwrap();
        assert_eq!(first.records.len(), 2);
        assert_eq!(cursor.next_frame_id(), 0);
        assert_eq!(first.records[0].record.offer.record.sequence, 0);
        assert_eq!(first.records[1].record.offer.record.sequence, 1);
        assert_eq!(
            first.records[1].durable_local_record().location(),
            rows[1].location()
        );
        assert_eq!(
            first.records[1]
                .durable_local_record()
                .metadata()
                .content_digest,
            first.records[1].record.offer.content_digest
        );
        let expected_raw = update(1_051).encode_to_vec();
        assert_eq!(
            first.records[1].record.raw_protobuf_sha256,
            <[u8; 32]>::from(Sha256::digest(expected_raw))
        );
        let stream = cursor.stream();
        let mut rolling_chain = crate::ingest::cumulative_chain_seed(&stream).unwrap();
        for record in &first.records {
            rolling_chain =
                crate::ingest::cumulative_chain_next(&stream, rolling_chain, &record.record.offer)
                    .unwrap();
        }
        let tail = first.records.last().unwrap();
        let ack = crate::ingest::CumulativePrimaryAck {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            stream: stream.clone(),
            primary_id: "primary-a".to_string(),
            primary_term: 1,
            through_sequence: tail.record.offer.record.sequence,
            through_content_digest: tail.record.offer.content_digest,
            rolling_chain_digest: rolling_chain,
            disposition: crate::ingest::ReceiptDisposition::DurablyStored,
            durable_lsn: 2,
            signing_key_id: "receipt-key".to_string(),
            signature: vec![7; 64],
        };
        let verified = crate::ingest::verify_cumulative_ack(
            ack,
            crate::ingest::ExpectedCumulativeAck {
                stream: &stream,
                primary_id: "primary-a",
                minimum_primary_term: 1,
                through_sequence: tail.record.offer.record.sequence,
                through_content_digest: tail.record.offer.content_digest,
                rolling_chain_digest: rolling_chain,
            },
            &AcceptCumulativeSignature,
        )
        .unwrap();
        let (_transport_records, advance) = first.into_transport_parts().unwrap();
        let mut ack_wal = crate::ingest::CumulativeAckWal::open(root.join("acks.wal")).unwrap();
        let durable_ack = ack_wal
            .commit_verified_replication(verified, advance.durable_replication_witness())
            .unwrap();
        cursor
            .advance_after_durable_ack(&advance, &durable_ack)
            .unwrap();
        assert_eq!(cursor.next_frame_id(), 2);
        assert_eq!(
            cursor
                .read_batch(2, config.max_record_bytes, config.max_record_bytes)
                .unwrap()
                .records[0]
                .record
                .offer
                .record
                .sequence,
            2
        );

        // A process restart resumes from the sequence after the locally durable cumulative ACK.
        let restarted = GrpcRawCommittedCursor::open(&root, limits, 2).unwrap();
        let tail = restarted
            .read_batch(2, config.max_record_bytes, config.max_record_bytes)
            .unwrap();
        assert_eq!(tail.records.len(), 1);
        assert_eq!(tail.records[0].record.offer.record.sequence, 2);
        assert_eq!(restarted.next_frame_id(), 2);
        assert_eq!(
            restarted
                .read_batch(2, config.max_record_bytes, config.max_record_bytes)
                .unwrap()
                .records,
            tail.records
        );
        assert!(
            GrpcRawCommittedCursor::open(&root, limits, 4)
                .unwrap_err()
                .to_string()
                .contains("beyond durable handoff tail")
        );

        drop(ack_wal);
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn committed_cursor_does_not_advance_when_a_later_reserved_row_is_invalid() {
        let root = temp_dir("committed-cursor-transactional-error");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let first = append_fixture(&mut spool, &identity, &update(1_055), 0);
        append_handoff_record(&journal, &first).unwrap();
        let mut corrupt = append_fixture(&mut spool, &identity, &update(1_056), 1);
        corrupt.protobuf_sha256 = "00".repeat(32);
        append_handoff_record(&journal, &corrupt).unwrap();
        let limits = GrpcRawCommittedReadLimits {
            max_compressed_record_bytes: config.max_record_bytes,
            max_uncompressed_record_bytes: config.max_record_bytes,
        };
        let cursor = GrpcRawCommittedCursor::open(&root, limits, 0).unwrap();
        let initial_offset = cursor.next_handoff_offset();
        let error = cursor
            .read_batch(2, config.max_record_bytes, config.max_record_bytes)
            .unwrap_err();
        assert!(error.to_string().contains("protobuf checksum mismatch"));
        assert_eq!(cursor.next_frame_id(), 0);
        assert_eq!(cursor.next_handoff_offset(), initial_offset);
        let retried = cursor
            .read_batch(1, config.max_record_bytes, config.max_record_bytes)
            .unwrap();
        assert_eq!(retried.records.len(), 1);
        assert_eq!(retried.records[0].record.offer.record.sequence, 0);

        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn committed_cursor_follows_its_unacked_generation_into_the_sealed_queue() {
        let cache_root = temp_dir("committed-cursor-hot-rotation");
        let active = cache_root.join(HOT_ACTIVE_DIR);
        fs::create_dir_all(&active).unwrap();
        fs::create_dir(cache_root.join(HOT_SEALED_DIR)).unwrap();
        fs::create_dir(cache_root.join(HOT_RECEIPTS_DIR)).unwrap();
        let mut config = test_config(active.clone());
        config.hot_generation_root = Some(cache_root.clone());
        config.max_generation_bytes = minimum_generation_bytes(&config).unwrap();
        validate_hot_generation_config(&config).unwrap();
        let mut identity = load_or_create_identity(&config).unwrap();
        let predecessor_journal_id = identity.journal_id;
        let mut spool = SpoolWriter::open(
            active.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = active.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let mut tail = None;
        for frame_id in 0..3 {
            let row = append_fixture(
                &mut spool,
                &identity,
                &complete_poh_update(1_070 + frame_id),
                frame_id,
            );
            append_handoff_record(&journal, &row).unwrap();
            tail = Some(row);
        }
        let limits = GrpcRawCommittedReadLimits {
            max_compressed_record_bytes: config.max_record_bytes,
            max_uncompressed_record_bytes: config.max_record_bytes,
        };
        let mut cursor = GrpcRawCommittedCursor::open(&active, limits, 0).unwrap();
        let rotated = hot_rotate_generation(
            &config,
            &cache_root,
            &mut identity,
            &mut spool,
            tail.as_ref().unwrap(),
        )
        .unwrap();
        assert_ne!(identity.journal_id, predecessor_journal_id);
        assert_eq!(rotated.next_frame_id, 1);

        let bound = cursor.bound_generation_dir().unwrap();
        assert_eq!(
            bound.parent(),
            Some(cache_root.join(HOT_SEALED_DIR).as_path())
        );
        assert!(cursor.bound_generation_is_sealed().unwrap());
        let predecessor = cursor
            .read_batch(4, config.max_record_bytes, config.max_record_bytes)
            .unwrap();
        assert_eq!(predecessor.records.len(), 3);
        assert_eq!(
            predecessor
                .records
                .iter()
                .map(|record| record.record.offer.record.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(
            predecessor
                .records
                .iter()
                .all(|record| record.stream.journal_id == predecessor_journal_id)
        );

        let mut successor = GrpcRawCommittedCursor::open(&active, limits, 0).unwrap();
        assert_eq!(successor.stream().journal_id, predecessor_journal_id);
        assert_ne!(
            successor.physical_stream().journal_id,
            predecessor_journal_id
        );
        assert_eq!(successor.next_replication_sequence(), 2);
        let seeded = successor
            .read_batch(1, config.max_record_bytes, config.max_record_bytes)
            .unwrap();
        assert_eq!(seeded.records.len(), 1);
        assert_eq!(seeded.records[0].record.offer.record.sequence, 2);

        let successor_trigger =
            append_fixture(&mut spool, &identity, &complete_poh_update(1_073), 1);
        append_handoff_record(&journal, &successor_trigger).unwrap();
        successor.seek_to_replication_sequence(3).unwrap();
        let after_seed = successor
            .read_batch(1, config.max_record_bytes, config.max_record_bytes)
            .unwrap();
        assert_eq!(after_seed.records[0].record.offer.record.sequence, 3);

        hot_rotate_generation(
            &config,
            &cache_root,
            &mut identity,
            &mut spool,
            &successor_trigger,
        )
        .unwrap();
        // After a restart, one ACK from a later generation can cover this older retained
        // physical generation. It is pinned at EOF rather than failing an out-of-range seek.
        cursor.seek_to_replication_sequence(4).unwrap();
        assert_eq!(cursor.next_replication_sequence(), 4);
        assert_eq!(cursor.next_frame_id(), 3);
        assert!(
            cursor
                .read_batch(1, config.max_record_bytes, config.max_record_bytes)
                .unwrap()
                .records
                .is_empty()
        );

        drop(spool);
        let active_identity_path = active.join(IDENTITY_FILE);
        let mut corrupted_identity = read_identity(&active_identity_path).unwrap();
        corrupted_identity.replication_sequence_base = Some(2);
        fs::write(
            &active_identity_path,
            serde_json::to_vec_pretty(&corrupted_identity).unwrap(),
        )
        .unwrap();
        let error = open_grpc_raw_replication_generation_cursors(&cache_root, limits).unwrap_err();
        assert!(error.to_string().contains("sequence base"));
        fs::remove_dir_all(cache_root).unwrap();
    }

    #[test]
    fn replication_generation_discovery_orders_sealed_before_active_and_fences_rotation() {
        let cache_root = temp_dir("replication-generation-discovery");
        let active = cache_root.join(HOT_ACTIVE_DIR);
        let sealed_root = cache_root.join(HOT_SEALED_DIR);
        fs::create_dir_all(&active).unwrap();
        fs::create_dir(&sealed_root).unwrap();
        load_or_create_identity(&test_config(active.clone())).unwrap();
        let newer = sealed_root.join("slot-00000000000000000200");
        let older = sealed_root.join("slot-00000000000000000100");
        fs::create_dir(&newer).unwrap();
        fs::create_dir(&older).unwrap();
        load_or_create_identity(&test_config(newer.clone())).unwrap();
        load_or_create_identity(&test_config(older.clone())).unwrap();
        for generation in [&active, &newer, &older] {
            recover_handoff_journal(&generation.join(HANDOFF_JOURNAL_FILE)).unwrap();
        }

        let generations = discover_grpc_raw_replication_generations(&cache_root).unwrap();
        assert_eq!(
            generations,
            vec![
                GrpcRawReplicationGeneration {
                    path: older.clone(),
                    sealed: true,
                },
                GrpcRawReplicationGeneration {
                    path: newer.clone(),
                    sealed: true,
                },
                GrpcRawReplicationGeneration {
                    path: active.clone(),
                    sealed: false,
                },
            ]
        );
        let prepared = open_grpc_raw_replication_generation_cursors(
            &cache_root,
            GrpcRawCommittedReadLimits {
                max_compressed_record_bytes: 1024,
                max_uncompressed_record_bytes: 1024,
            },
        )
        .unwrap();
        assert_eq!(
            prepared
                .iter()
                .map(|generation| generation.sealed)
                .collect::<Vec<_>>(),
            vec![true, true, false]
        );
        assert_eq!(prepared[0].cursor.bound_generation_dir().unwrap(), older);
        assert_eq!(prepared[1].cursor.bound_generation_dir().unwrap(), newer);
        assert_eq!(prepared[2].cursor.bound_generation_dir().unwrap(), active);

        // A sealed legacy generation may be followed by an unrelated logical stream. Its torn
        // final row must still fail discovery instead of looking like a caught-up generation.
        let newer_journal = newer.join(HANDOFF_JOURNAL_FILE);
        fs::write(&newer_journal, b"{\"partial\":").unwrap();
        let error = open_grpc_raw_replication_generation_cursors(
            &cache_root,
            GrpcRawCommittedReadLimits {
                max_compressed_record_bytes: 1024,
                max_uncompressed_record_bytes: 1024,
            },
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("sealed raw gRPC generation has a partial handoff tail")
        );
        fs::write(&newer_journal, b"").unwrap();

        let older_identity_path = older.join(IDENTITY_FILE);
        let mut corrupted_identity = read_identity(&older_identity_path).unwrap();
        corrupted_identity.replication_sequence_base = Some(1);
        fs::write(
            &older_identity_path,
            serde_json::to_vec_pretty(&corrupted_identity).unwrap(),
        )
        .unwrap();
        let error = open_grpc_raw_replication_generation_cursors(
            &cache_root,
            GrpcRawCommittedReadLimits {
                max_compressed_record_bytes: 1024,
                max_uncompressed_record_bytes: 1024,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("base-zero physical anchor"));

        let marker = cache_root.join(HOT_ROTATION_MARKER);
        File::create(&marker).unwrap().sync_all().unwrap();
        assert!(
            discover_grpc_raw_replication_generations(&cache_root)
                .unwrap_err()
                .to_string()
                .contains("rotation is in progress")
        );
        fs::remove_dir_all(cache_root).unwrap();
    }

    #[test]
    fn committed_cursor_preserves_byte_limited_and_partial_rows_for_the_next_read() {
        let root = temp_dir("committed-cursor-boundaries");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let first_row = append_fixture(&mut spool, &identity, &update(1_060), 0);
        let second_row = append_fixture(&mut spool, &identity, &update(1_061), 1);
        append_handoff_record(&journal, &first_row).unwrap();
        append_handoff_record(&journal, &second_row).unwrap();
        let limits = GrpcRawCommittedReadLimits {
            max_compressed_record_bytes: config.max_record_bytes,
            max_uncompressed_record_bytes: config.max_record_bytes,
        };
        let cursor = GrpcRawCommittedCursor::open(&root, limits, 0).unwrap();

        let one = cursor
            .read_batch(2, first_row.compressed_len, config.max_record_bytes)
            .unwrap();
        assert_eq!(one.records.len(), 1);
        assert_eq!(cursor.next_frame_id(), 0);
        let one_by_uncompressed = cursor
            .read_batch(2, config.max_record_bytes, first_row.uncompressed_len)
            .unwrap();
        assert_eq!(one_by_uncompressed.records.len(), 1);
        assert_eq!(
            one_by_uncompressed.uncompressed_bytes,
            first_row.uncompressed_len
        );
        let mut second_cursor = GrpcRawCommittedCursor::open(&root, limits, 1).unwrap();
        let two = second_cursor
            .read_batch(1, second_row.compressed_len, config.max_record_bytes)
            .unwrap();
        assert_eq!(two.records.len(), 1);
        assert_eq!(two.records[0].record.offer.record.sequence, 1);

        let pending_row = append_fixture(&mut spool, &identity, &update(1_062), 2);
        let pending_json = serde_json::to_vec(&pending_row).unwrap();
        let split = pending_json.len() / 2;
        let mut append = OpenOptions::new().append(true).open(&journal).unwrap();
        append.write_all(&pending_json[..split]).unwrap();
        append.sync_data().unwrap();
        drop(append);
        second_cursor = GrpcRawCommittedCursor::open(&root, limits, 2).unwrap();
        let before_offset = second_cursor.next_handoff_offset();
        let partial = second_cursor
            .read_batch(1, pending_row.compressed_len, config.max_record_bytes)
            .unwrap();
        assert!(partial.records.is_empty());
        assert_eq!(
            partial.incomplete_handoff_tail_bytes,
            u64::try_from(split).unwrap()
        );
        assert_eq!(second_cursor.next_frame_id(), 2);
        assert_eq!(second_cursor.next_handoff_offset(), before_offset);

        let mut append = OpenOptions::new().append(true).open(&journal).unwrap();
        append.write_all(&pending_json[split..]).unwrap();
        append.write_all(b"\n").unwrap();
        append.sync_data().unwrap();
        drop(append);
        let completed = second_cursor
            .read_batch(1, pending_row.compressed_len, config.max_record_bytes)
            .unwrap();
        assert_eq!(completed.records.len(), 1);
        assert_eq!(completed.records[0].record.offer.record.sequence, 2);
        assert_eq!(second_cursor.next_frame_id(), 2);

        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn committed_reader_ignores_and_reports_a_partial_handoff_tail() {
        let root = temp_dir("committed-reader-partial-handoff");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let committed_row = append_fixture(&mut spool, &identity, &update(1_100), 0);
        append_handoff_record(&journal, &committed_row).unwrap();
        let pending_row = append_fixture(&mut spool, &identity, &update(1_101), 1);
        let pending_json = serde_json::to_vec(&pending_row).unwrap();
        let partial = &pending_json[..pending_json.len() / 2];
        let mut journal_append = OpenOptions::new().append(true).open(&journal).unwrap();
        journal_append.write_all(partial).unwrap();
        journal_append.sync_data().unwrap();
        drop(journal_append);
        let before = fs::read(&journal).unwrap();

        let mut slots = Vec::new();
        let report = read_grpc_raw_committed_records(
            &root,
            GrpcRawCommittedReadLimits {
                max_compressed_record_bytes: config.max_record_bytes,
                max_uncompressed_record_bytes: config.max_record_bytes,
            },
            |record| {
                slots.push(record.slot);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(report.records, 1);
        assert_eq!(
            report.incomplete_handoff_tail_bytes,
            u64::try_from(partial.len()).unwrap()
        );
        assert_eq!(slots, vec![1_100]);
        assert_eq!(fs::read(&journal).unwrap(), before);

        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn committed_reader_rejects_wal_metadata_and_protobuf_digest_mismatches() {
        let metadata_root = temp_dir("committed-reader-metadata-mismatch");
        fs::create_dir_all(&metadata_root).unwrap();
        let metadata_config = test_config(metadata_root.clone());
        let metadata_identity = load_or_create_identity(&metadata_config).unwrap();
        let mut metadata_spool = SpoolWriter::open(
            metadata_root.join(WAL_ROOT_DIR),
            metadata_identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: metadata_config.segment_target_bytes,
                max_record_bytes: metadata_config.max_record_bytes,
            },
        )
        .unwrap();
        let metadata_journal = metadata_root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&metadata_journal).unwrap();
        let source = update(1_200);
        let Some(UpdateOneof::Block(block)) = source.update_oneof.as_ref() else {
            panic!("fixture update is not a block")
        };
        let raw = source.encode_to_vec();
        let compressed = zstd::bulk::compress(&raw, 1).unwrap();
        let wrong_key_metadata = IngressRecordMeta::from_payload(
            metadata_identity.cluster_id.clone(),
            ObservationId {
                origin_node_id: metadata_identity.origin_node_id.clone(),
                journal_id: metadata_identity.journal_id,
                sequence: 0,
            },
            metadata_identity.source_id.clone(),
            LogicalKey::Block {
                slot: block.slot + 1,
                blockhash: decode_blockhash(&block.blockhash).unwrap(),
            },
            PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
            &compressed,
        );
        let durable = metadata_spool
            .append_and_sync(wrong_key_metadata, &compressed)
            .unwrap();
        let metadata_row = handoff_record_from_block(
            block,
            0,
            EpochSlot::from_slot(block.slot, OLD_FAITHFUL_SLOTS_PER_EPOCH),
            durable.location(),
            raw.len() as u64,
            compressed.len() as u64,
            sha256_hex(&raw),
        );
        append_handoff_record(&metadata_journal, &metadata_row).unwrap();
        let limits = GrpcRawCommittedReadLimits {
            max_compressed_record_bytes: metadata_config.max_record_bytes,
            max_uncompressed_record_bytes: metadata_config.max_record_bytes,
        };
        let error =
            read_grpc_raw_committed_records(&metadata_root, limits, |_| Ok(())).unwrap_err();
        assert!(error.to_string().contains("logical key mismatch"));
        drop(metadata_spool);
        fs::remove_dir_all(metadata_root).unwrap();

        let digest_root = temp_dir("committed-reader-digest-mismatch");
        fs::create_dir_all(&digest_root).unwrap();
        let digest_config = test_config(digest_root.clone());
        let digest_identity = load_or_create_identity(&digest_config).unwrap();
        let mut digest_spool = SpoolWriter::open(
            digest_root.join(WAL_ROOT_DIR),
            digest_identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: digest_config.segment_target_bytes,
                max_record_bytes: digest_config.max_record_bytes,
            },
        )
        .unwrap();
        let digest_journal = digest_root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&digest_journal).unwrap();
        let mut digest_row = append_fixture(&mut digest_spool, &digest_identity, &update(1_201), 0);
        digest_row.protobuf_sha256 = "00".repeat(32);
        append_handoff_record(&digest_journal, &digest_row).unwrap();
        let error = read_grpc_raw_committed_records(
            &digest_root,
            GrpcRawCommittedReadLimits {
                max_compressed_record_bytes: digest_config.max_record_bytes,
                max_uncompressed_record_bytes: digest_config.max_record_bytes,
            },
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("protobuf checksum mismatch"));

        drop(digest_spool);
        fs::remove_dir_all(digest_root).unwrap();
    }

    #[test]
    fn committed_reader_enforces_compressed_and_uncompressed_byte_limits() {
        let root = temp_dir("committed-reader-limits");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let row = append_fixture(&mut spool, &identity, &update(1_300), 0);
        append_handoff_record(&journal, &row).unwrap();

        let compressed_error = read_grpc_raw_committed_records(
            &root,
            GrpcRawCommittedReadLimits {
                max_compressed_record_bytes: row.compressed_len - 1,
                max_uncompressed_record_bytes: row.uncompressed_len,
            },
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(compressed_error.to_string().contains("compressed length"));
        assert!(compressed_error.to_string().contains("exceeds maximum"));

        let uncompressed_error = read_grpc_raw_committed_records(
            &root,
            GrpcRawCommittedReadLimits {
                max_compressed_record_bytes: row.compressed_len,
                max_uncompressed_record_bytes: row.uncompressed_len - 1,
            },
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(
            uncompressed_error
                .to_string()
                .contains("uncompressed length")
        );
        assert!(uncompressed_error.to_string().contains("exceeds maximum"));

        let zero_limit_error = read_grpc_raw_committed_records(
            &root,
            GrpcRawCommittedReadLimits {
                max_compressed_record_bytes: 0,
                max_uncompressed_record_bytes: row.uncompressed_len,
            },
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(zero_limit_error.to_string().contains("must be non-zero"));

        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stopped_spool_materializes_atomically_with_deterministic_receipt_and_no_approval() {
        let root = temp_dir("materialize-source");
        let archive_one = temp_dir("materialize-output-one");
        let archive_two = temp_dir("materialize-output-two");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        for frame_id in 0..2 {
            let source = complete_poh_update(800 + frame_id);
            let row = append_fixture(&mut spool, &identity, &source, frame_id);
            append_handoff_record(&journal, &row).unwrap();
        }
        drop(spool);

        let materialize = |archive_dir: PathBuf| {
            materialize_grpc_raw_blocks(GrpcRawMaterializeConfig {
                input_dir: root.clone(),
                archive_dir,
                epoch: 0,
                max_record_bytes: config.max_record_bytes,
                pubkey_hot_registry_path: None,
                pubkey_hot_count: 0,
            })
            .unwrap()
        };
        let first = materialize(archive_one.clone());
        let second = materialize(archive_two.clone());

        assert_eq!(first.receipt, second.receipt);
        assert_eq!(first.receipt.blocks_written, 2);
        assert_eq!(first.receipt.first_source_frame_id, 0);
        assert_eq!(first.receipt.last_source_frame_id, 1);
        assert_eq!(first.receipt.first_slot, 800);
        assert_eq!(first.receipt.last_slot, 801);
        assert_eq!(
            first.receipt_path.file_name().unwrap(),
            MATERIALIZATION_RECEIPT_FILE
        );
        assert!(first.receipt_path.is_file());
        assert!(root.join(IDENTITY_FILE).is_file());
        assert!(root.join(HANDOFF_JOURNAL_FILE).is_file());
        assert!(!materialization_staging_path(&archive_one).unwrap().exists());

        // A stopped spool is only a stable source snapshot. It is not evidence that the epoch is
        // complete, so the materializer must never grant repair or packaging approval.
        for archive in [&archive_one, &archive_two] {
            assert!(!archive.join("READY-TO-PACKAGE").exists());
            assert!(!archive.join("FINALIZE-NEXT.md").exists());
            let progress: serde_json::Value = serde_json::from_reader(
                File::open(archive.join("journal").join(MATERIALIZATION_PROGRESS_FILE)).unwrap(),
            )
            .unwrap();
            assert_eq!(progress["state"], "materialized_snapshot");
            assert_eq!(progress["stopped_at_epoch_boundary"], false);
        }

        let inspected = inspect_capture(archive_one.clone()).unwrap();
        assert_eq!(inspected.block_frames, 2);
        assert_eq!(inspected.block_index_rows, 2);
        assert_eq!(inspected.poh_frames, 2);
        assert_eq!(inspected.poh_entries, 4);

        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(archive_one).unwrap();
        fs::remove_dir_all(archive_two).unwrap();
    }

    #[test]
    fn active_recorder_rejects_materialization_before_staging_is_created() {
        let root = temp_dir("materialize-active-source");
        let archive = temp_dir("materialize-active-output");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let source = complete_poh_update(900);
        let row = append_fixture(&mut spool, &identity, &source, 0);
        append_handoff_record(&journal, &row).unwrap();

        let error = materialize_grpc_raw_blocks(GrpcRawMaterializeConfig {
            input_dir: root.clone(),
            archive_dir: archive.clone(),
            epoch: 0,
            max_record_bytes: config.max_record_bytes,
            pubkey_hot_registry_path: None,
            pubkey_hot_count: 0,
        })
        .unwrap_err();
        assert!(error.to_string().contains("lock and audit raw gRPC WAL"));
        assert!(!archive.exists());
        assert!(!materialization_staging_path(&archive).unwrap().exists());
        assert!(root.join(HANDOFF_JOURNAL_FILE).is_file());

        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reconciles_wal_record_committed_before_handoff_journal() {
        let root = temp_dir("reconcile");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        let mut state = recover_handoff_journal(&journal).unwrap();
        append_fixture(&mut spool, &identity, &update(700), 0);
        assert!(
            reconcile_handoff_tail(
                &mut spool,
                &journal,
                &mut state,
                OLD_FAITHFUL_SLOTS_PER_EPOCH,
                config.max_record_bytes,
            )
            .unwrap()
        );
        assert_eq!(state.records, 1);
        assert_eq!(state.last.unwrap().slot, 700);
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_valid_json_tail_with_slot_different_from_wal() {
        let root = temp_dir("journal-slot-tamper");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let mut row = append_fixture(&mut spool, &identity, &update(900), 0);
        row.slot = 901;
        let mut encoded = serde_json::to_vec(&row).unwrap();
        encoded.push(b'\n');
        fs::write(&journal, encoded).unwrap();
        let mut state = recover_handoff_journal(&journal).unwrap();
        let error = reconcile_handoff_tail(
            &mut spool,
            &journal,
            &mut state,
            OLD_FAITHFUL_SLOTS_PER_EPOCH,
            config.max_record_bytes,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("differs from its authoritative WAL")
        );
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn compression_buffer_grows_for_blocks_larger_than_initial_capacity() {
        let mut raw = vec![0u8; 2 * 1024 * 1024 + 17];
        let mut state = 0x1234_5678u32;
        for byte in &mut raw {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            *byte = state as u8;
        }
        let mut compressed = Vec::with_capacity(1024);
        let mut compressor = zstd::bulk::Compressor::new(1).unwrap();
        compress_raw_block(&mut compressor, &raw, &mut compressed, 4 * 1024 * 1024).unwrap();
        assert!(compressed.len() > 1024 * 1024);
        assert_eq!(zstd::bulk::decompress(&compressed, raw.len()).unwrap(), raw);
    }

    #[test]
    fn atomically_publishes_secret_free_resume_coverage_warning() {
        let root = temp_dir("resume-coverage-warning");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("warning.json");
        let event = GrpcRawResumeCoverageWarning {
            event_id: resume_coverage_warning_event_id(100, 104, 104),
            schema_version: RESUME_COVERAGE_WARNING_SCHEMA_VERSION,
            requested_overlap_slot: 100,
            first_delivered_slot: 104,
            observed_later_slot: 104,
            written_unix_secs: 123,
        };
        assert_eq!(
            publish_resume_coverage_warning(&path, &event).unwrap(),
            ResumeCoverageWarningPublishOutcome::Published
        );
        assert_eq!(
            publish_resume_coverage_warning(&path, &event).unwrap(),
            ResumeCoverageWarningPublishOutcome::AlreadyPresent
        );
        let decoded: GrpcRawResumeCoverageWarning =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(decoded, event);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn coalesces_but_does_not_replace_a_different_pending_resume_warning() {
        let root = temp_dir("resume-coverage-warning-pending");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("warning.json");
        let first = GrpcRawResumeCoverageWarning {
            event_id: resume_coverage_warning_event_id(100, 104, 104),
            schema_version: RESUME_COVERAGE_WARNING_SCHEMA_VERSION,
            requested_overlap_slot: 100,
            first_delivered_slot: 104,
            observed_later_slot: 104,
            written_unix_secs: 123,
        };
        let second = GrpcRawResumeCoverageWarning {
            event_id: resume_coverage_warning_event_id(104, 108, 108),
            schema_version: RESUME_COVERAGE_WARNING_SCHEMA_VERSION,
            requested_overlap_slot: 104,
            first_delivered_slot: 108,
            observed_later_slot: 108,
            written_unix_secs: 124,
        };
        assert_eq!(
            publish_resume_coverage_warning(&path, &first).unwrap(),
            ResumeCoverageWarningPublishOutcome::Published
        );
        assert_eq!(
            publish_resume_coverage_warning(&path, &second).unwrap(),
            ResumeCoverageWarningPublishOutcome::DifferentPending
        );
        assert_eq!(read_resume_coverage_warning(&path).unwrap(), Some(first));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupt_existing_resume_warning_is_fail_closed() {
        let root = temp_dir("resume-coverage-warning-corrupt");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("warning.json");
        let event = GrpcRawResumeCoverageWarning {
            event_id: resume_coverage_warning_event_id(104, 108, 108),
            schema_version: RESUME_COVERAGE_WARNING_SCHEMA_VERSION,
            requested_overlap_slot: 104,
            first_delivered_slot: 108,
            observed_later_slot: 108,
            written_unix_secs: 124,
        };
        let mut bom_prefixed = b"\xef\xbb\xbf".to_vec();
        bom_prefixed.extend(serde_json::to_vec(&event).unwrap());
        let non_finite_extra = format!(
            "{{\"event_id\":\"{}\",\"schema_version\":1,\"requested_overlap_slot\":104,\"first_delivered_slot\":108,\"observed_later_slot\":108,\"written_unix_secs\":124,\"extra\":NaN}}",
            event.event_id
        )
        .into_bytes();
        let unknown_extra = format!(
            "{{\"event_id\":\"{}\",\"schema_version\":1,\"requested_overlap_slot\":104,\"first_delivered_slot\":108,\"observed_later_slot\":108,\"written_unix_secs\":124,\"extra\":0}}",
            event.event_id
        )
        .into_bytes();

        for corrupt in [
            b"{".to_vec(),
            serde_json::to_vec(&GrpcRawResumeCoverageWarning {
                event_id: "f".repeat(64),
                ..event.clone()
            })
            .unwrap(),
            bom_prefixed,
            non_finite_extra,
            unknown_extra,
        ] {
            fs::write(&path, &corrupt).unwrap();
            assert!(publish_resume_coverage_warning(&path, &event).is_err());
            assert_eq!(fs::read(&path).unwrap(), corrupt);
        }

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resume_warning_path_accepts_fixed_output_and_sibling_locations() {
        let root = temp_dir("resume-coverage-warning-path");
        fs::create_dir_all(&root).unwrap();
        let mut config = test_config(root.clone());
        config.resume_coverage_warning_file = Some(root.join("raw-blocks.jsonl"));
        assert!(
            prepare_resume_coverage_warning_path(&config)
                .unwrap_err()
                .to_string()
                .contains("warning path must be")
        );
        let output_expected = root.join(MONITORING_DIR).join(RESUME_COVERAGE_WARNING_FILE);
        config.resume_coverage_warning_file = Some(output_expected);
        prepare_resume_coverage_warning_path(&config).unwrap();
        assert!(root.join(MONITORING_DIR).is_dir());

        let active = root.join("active");
        fs::create_dir(&active).unwrap();
        config.output_dir = active;
        let sibling_expected = root.join("monitoring").join(RESUME_COVERAGE_WARNING_FILE);
        config.resume_coverage_warning_file = Some(sibling_expected);
        prepare_resume_coverage_warning_path(&config).unwrap();
        assert!(root.join("monitoring").is_dir());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn resume_warning_sibling_rejects_symlinked_monitoring_directory() {
        use std::os::unix::fs::symlink;

        let root = temp_dir("resume-coverage-warning-sibling-symlink");
        let active = root.join("active");
        let outside = temp_dir("resume-coverage-warning-sibling-outside");
        fs::create_dir_all(&active).unwrap();
        fs::create_dir_all(&outside).unwrap();
        symlink(&outside, root.join("monitoring")).unwrap();
        let mut config = test_config(active);
        config.resume_coverage_warning_file =
            Some(root.join("monitoring").join(RESUME_COVERAGE_WARNING_FILE));
        let error = prepare_resume_coverage_warning_path(&config).unwrap_err();
        assert!(error.to_string().contains("not a real directory"));
        fs::remove_file(root.join("monitoring")).unwrap();
        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(outside).unwrap();
    }

    #[test]
    fn low_disk_guard_can_be_disabled_or_forced() {
        let root = temp_dir("disk-guard");
        fs::create_dir_all(&root).unwrap();
        assert_eq!(low_disk_bytes(&root, 0).unwrap(), None);
        assert!(low_disk_bytes(&root, u64::MAX).unwrap().is_some());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn truncates_partial_json_tail_without_reusing_committed_frame_id() {
        let root = temp_dir("partial-journal");
        fs::create_dir_all(&root).unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        let row = GrpcRawHandoffRecord {
            schema_version: JOURNAL_SCHEMA_VERSION,
            frame_id: 0,
            slot: 42,
            parent_slot: 41,
            epoch: 0,
            epoch_slot_index: 42,
            segment_id: 0,
            frame_offset: 8,
            frame_len: 100,
            compressed_len: 20,
            uncompressed_len: 40,
            protobuf_sha256: "00".repeat(32),
            blockhash: "11111111111111111111111111111111".to_string(),
        };
        append_handoff_record(&journal, &row).unwrap_err();
        recover_handoff_journal(&journal).unwrap();
        append_handoff_record(&journal, &row).unwrap();
        let valid_len = fs::metadata(&journal).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&journal)
            .unwrap()
            .write_all(b"{\"schema_version\":1")
            .unwrap();
        let state = recover_handoff_journal(&journal).unwrap();
        assert_eq!(state.records, 1);
        assert_eq!(fs::metadata(&journal).unwrap().len(), valid_len);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn relay_publication_is_strictly_after_both_durability_steps() {
        use std::cell::RefCell;

        let events = RefCell::new(Vec::new());
        let result = cross_durable_relay_boundary(
            || -> Result<u64> {
                events.borrow_mut().push("wal");
                Err(anyhow!("wal fsync failed"))
            },
            |_| {
                events.borrow_mut().push("handoff");
                Ok(())
            },
            || {
                events.borrow_mut().push("relay");
                Ok(())
            },
        );
        assert!(result.is_err());
        assert_eq!(&*events.borrow(), &["wal"]);

        events.borrow_mut().clear();
        let result = cross_durable_relay_boundary(
            || {
                events.borrow_mut().push("wal");
                Ok(7u64)
            },
            |_| {
                events.borrow_mut().push("handoff");
                Err(anyhow!("handoff fsync failed"))
            },
            || {
                events.borrow_mut().push("relay");
                Ok(())
            },
        );
        assert!(result.is_err());
        assert_eq!(&*events.borrow(), &["wal", "handoff"]);

        events.borrow_mut().clear();
        let result = cross_durable_relay_boundary(
            || {
                events.borrow_mut().push("wal");
                Ok(8u64)
            },
            |_| {
                events.borrow_mut().push("handoff");
                Ok(())
            },
            || {
                events.borrow_mut().push("relay");
                Err(anyhow!("relay publication failed"))
            },
        );
        assert!(result.is_err());
        assert_eq!(&*events.borrow(), &["wal", "handoff", "relay"]);

        events.borrow_mut().clear();
        let durable = cross_durable_relay_boundary(
            || {
                events.borrow_mut().push("wal");
                Ok(9u64)
            },
            |_| {
                events.borrow_mut().push("handoff");
                Ok(())
            },
            || {
                events.borrow_mut().push("relay");
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(durable, 9);
        assert_eq!(&*events.borrow(), &["wal", "handoff", "relay"]);
    }

    #[test]
    fn process_local_relay_sequence_ignores_generation_frame_resets() {
        let auth = YellowstoneRelayAuth::from_shared_x_token(b"relay-only").unwrap();
        let relay = YellowstoneBlockRelay::new(
            auth,
            YellowstoneBlockRelayLimits {
                max_records: 8,
                max_encoded_bytes: 1024 * 1024,
                max_clients: 2,
            },
        )
        .unwrap();
        let mut publisher = RawGrpcRelayPublisher::new(relay.clone());

        // The first generation can end at an arbitrary WAL frame id and its successor restarts at
        // zero. Neither value enters the relay publication API.
        let generation_frame_ids = [73u64, 0u64];
        publisher.publish_after_durability(&update(100)).unwrap();
        publisher.publish_after_durability(&update(101)).unwrap();

        assert_eq!(generation_frame_ids, [73, 0]);
        assert_eq!(publisher.next_sequence, 2);
        let stats = relay.stats().unwrap();
        assert_eq!(stats.last_durable_sequence, Some(1));
        assert_eq!(stats.last_durable_slot, Some(101));
    }

    #[test]
    fn relay_preload_reads_only_the_verified_bounded_active_wal_tail() {
        let root = temp_dir("relay-preload-tail");
        fs::create_dir_all(&root).unwrap();
        let config = test_config(root.clone());
        let identity = load_or_create_identity(&config).unwrap();
        let mut spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        for (frame_id, slot) in [(0, 100), (1, 101), (2, 102)] {
            let row = append_fixture(&mut spool, &identity, &update(slot), frame_id);
            append_handoff_record(&journal, &row).unwrap();
        }

        let limits = YellowstoneBlockRelayLimits {
            max_records: 2,
            max_encoded_bytes: 1024 * 1024,
            max_clients: 1,
        };
        let rows = read_relay_tail_rows(&journal, limits).unwrap();
        assert_eq!(
            rows.iter().map(|row| row.frame_id).collect::<Vec<_>>(),
            [1, 2]
        );
        let relay = YellowstoneBlockRelay::new(
            YellowstoneRelayAuth::from_shared_x_token(b"relay-only").unwrap(),
            limits,
        )
        .unwrap();
        let mut publisher = RawGrpcRelayPublisher::new(relay.clone());
        for row in rows {
            let replayed = read_verified_relay_update(
                spool.journal_dir(),
                &identity,
                &row,
                config.max_record_bytes,
            )
            .unwrap();
            publisher.publish_after_durability(&replayed).unwrap();
        }
        let stats = relay.stats().unwrap();
        assert_eq!(stats.retained_records, 2);
        assert_eq!(stats.first_available_slot, Some(101));
        assert_eq!(stats.last_durable_slot, Some(102));
        assert_eq!(stats.last_durable_sequence, Some(1));

        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn downstream_token_file_is_separate_from_an_upstream_like_token() {
        use yellowstone_grpc_proto::{
            prelude::{PingRequest, geyser_server::Geyser},
            tonic::Request,
        };

        let root = temp_dir("relay-token-separation");
        fs::create_dir_all(&root).unwrap();
        let token_file = root.join("relay-token");
        fs::write(&token_file, b"downstream-secret\n").unwrap();
        let token = read_relay_x_token_file(&token_file).unwrap();
        assert_eq!(token, b"downstream-secret");
        let relay = YellowstoneBlockRelay::new(
            YellowstoneRelayAuth::from_shared_x_token(token).unwrap(),
            YellowstoneBlockRelayLimits::default(),
        )
        .unwrap();

        let mut wrong = Request::new(PingRequest { count: 1 });
        wrong
            .metadata_mut()
            .insert("x-token", "upstream-secret".parse().unwrap());
        assert_eq!(
            Geyser::ping(&relay, wrong).await.unwrap_err().code(),
            Code::Unauthenticated
        );
        let mut right = Request::new(PingRequest { count: 2 });
        right
            .metadata_mut()
            .insert("x-token", "downstream-secret".parse().unwrap());
        assert_eq!(
            Geyser::ping(&relay, right)
                .await
                .unwrap()
                .into_inner()
                .count,
            2
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn downstream_token_file_rejects_unsafe_entries_and_contents() {
        let root = temp_dir("relay-token-validation");
        fs::create_dir_all(&root).unwrap();

        let empty = root.join("empty");
        fs::write(&empty, b"\n").unwrap();
        assert!(read_relay_x_token_file(&empty).is_err());

        let control = root.join("control");
        fs::write(&control, b"secret\tvalue").unwrap();
        assert!(read_relay_x_token_file(&control).is_err());

        let oversized = root.join("oversized");
        fs::write(&oversized, vec![b'x'; RELAY_X_TOKEN_MAX_BYTES as usize + 1]).unwrap();
        assert!(read_relay_x_token_file(&oversized).is_err());
        assert!(read_relay_x_token_file(&root).is_err());

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let valid = root.join("valid");
            let link = root.join("link");
            fs::write(&valid, b"secret").unwrap();
            symlink(&valid, &link).unwrap();
            assert!(read_relay_x_token_file(&link).is_err());
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn relay_bind_failure_prevents_any_upstream_connection() {
        use tokio::time::{Duration, timeout};

        let root = temp_dir("relay-bind-before-upstream");
        fs::create_dir_all(&root).unwrap();
        let token_file = root.join("relay-token");
        fs::write(&token_file, b"downstream-secret\n").unwrap();
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = test_config(root.clone());
        config.endpoint = format!("http://{}", upstream.local_addr().unwrap());
        config.min_free_bytes = 0;
        config.relay_bind = Some(occupied.local_addr().unwrap());
        config.relay_x_token_file = Some(token_file);

        let error = record_grpc_raw_blocks(config).await.unwrap_err();
        assert!(error.to_string().contains("bind downstream raw gRPC relay"));
        assert!(
            timeout(Duration::from_millis(100), upstream.accept())
                .await
                .is_err(),
            "the recorder contacted upstream despite a relay bind failure"
        );

        drop(occupied);
        drop(upstream);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn relay_runtime_closes_an_authenticated_client_cleanly() {
        use yellowstone_grpc_proto::{
            prelude::{PingRequest, geyser_client::GeyserClient},
            tonic::Request,
        };

        let root = temp_dir("relay-clean-shutdown");
        fs::create_dir_all(&root).unwrap();
        let token_file = root.join("relay-token");
        fs::write(&token_file, b"downstream-secret\n").unwrap();
        let address_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let relay_address = address_probe.local_addr().unwrap();
        drop(address_probe);

        let mut config = test_config(root.clone());
        config.relay_bind = Some(relay_address);
        config.relay_x_token_file = Some(token_file);
        let identity = load_or_create_identity(&config).unwrap();
        let spool = SpoolWriter::open(
            root.join(WAL_ROOT_DIR),
            identity.spool_identity(),
            SpoolOptions {
                segment_target_bytes: config.segment_target_bytes,
                max_record_bytes: config.max_record_bytes,
            },
        )
        .unwrap();
        let journal = root.join(HANDOFF_JOURNAL_FILE);
        recover_handoff_journal(&journal).unwrap();
        let runtime = RawGrpcRelayRuntime::start_if_enabled(
            &config,
            &identity,
            spool.journal_dir(),
            &journal,
        )
        .await
        .unwrap()
        .unwrap();

        let mut client = GeyserClient::connect(format!("http://{relay_address}"))
            .await
            .unwrap();
        let mut ping = Request::new(PingRequest { count: 42 });
        ping.metadata_mut()
            .insert("x-token", "downstream-secret".parse().unwrap());
        assert_eq!(client.ping(ping).await.unwrap().into_inner().count, 42);
        runtime.shutdown().await.unwrap();

        drop(client);
        drop(spool);
        fs::remove_dir_all(root).unwrap();
    }
}
