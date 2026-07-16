//! Minimal, crash-recoverable Yellowstone block recorder.
//!
//! This path deliberately performs no archive conversion and writes no derived sidecars. Each
//! protobuf update is encoded once, compressed as an independent zstd frame, and crossed through
//! the durable ingress-spool boundary before its small handoff journal is advanced.

use std::{
    collections::HashMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, ensure};
use futures::{SinkExt, StreamExt, channel::mpsc};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::time::{Instant as TokioInstant, sleep_until};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeRequestPing,
    SubscribeUpdate, SubscribeUpdateBlock, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::metadata::MetadataMap;

use crate::{
    epoch::{EpochSlot, OLD_FAITHFUL_SLOTS_PER_EPOCH},
    grpc::{
        GrpcRawArchiveWriteReport, GrpcRawArchiveWriter,
        connect_grpc_with_max_decoding_message_size, inspect_capture,
    },
    ingest::{
        IngressRecordMeta, LockedSpoolAudit, LogicalKey, ObservationId, SpoolJournalIdentity,
        SpoolLocation, SpoolOptions, SpoolWriter, read_spool_record,
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
const MATERIALIZATION_RECEIPT_FILE: &str = "raw-materialization-receipt.json";
const MATERIALIZATION_PROGRESS_FILE: &str = "progress.json";
const SUBSCRIBE_REQUEST_CHANNEL_CAPACITY: usize = 8;
const SUBSCRIBE_PING_ID: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchdogOutcome<T> {
    Completed(T),
    TotalTimeout,
    IdleTimeout,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcRawRecordConfig {
    pub endpoint: String,
    pub output_dir: PathBuf,
    pub max_blocks: usize,
    pub timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub from_slot: Option<u64>,
    pub resume_coverage_warning_file: Option<PathBuf>,
    pub slots_per_epoch: u64,
    pub stop_at_epoch_boundary: bool,
    pub compression_level: i32,
    pub segment_target_bytes: u64,
    pub max_record_bytes: u64,
    pub min_free_bytes: u64,
    pub require_complete_poh: bool,
    pub cluster_id: String,
    pub origin_node_id: String,
    pub source_id: String,
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
            resume_coverage_warning_file: None,
            slots_per_epoch: OLD_FAITHFUL_SLOTS_PER_EPOCH,
            stop_at_epoch_boundary: false,
            compression_level: 1,
            segment_target_bytes: 256 * 1024 * 1024,
            max_record_bytes: 128 * 1024 * 1024,
            min_free_bytes: 16 * 1024 * 1024 * 1024,
            require_complete_poh: false,
            cluster_id: "solana-mainnet".to_string(),
            origin_node_id: "mac-bridge".to_string(),
            source_id: "grpc-raw".to_string(),
        }
    }
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
    pub effective_from_slot: Option<u64>,
    pub resume_overlap_slot: Option<u64>,
    pub resume_overlap_observed: Option<bool>,
    pub first_delivered_slot: Option<u64>,
    pub resume_coverage_warning: bool,
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
    pub stopped_low_disk: bool,
    pub available_bytes_at_stop: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GrpcRawResumeCoverageWarning {
    event_id: String,
    schema_version: u32,
    requested_overlap_slot: u64,
    first_delivered_slot: u64,
    observed_later_slot: u64,
    written_unix_secs: u64,
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
) -> Option<u64> {
    last_durable_slot.or(configured_from_slot)
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GrpcRawIdentityFile {
    schema_version: u32,
    endpoint: String,
    cluster_id: String,
    origin_node_id: String,
    source_id: String,
    journal_id: [u8; 16],
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
}

#[derive(Debug, Default)]
struct JournalState {
    records: u64,
    first: Option<GrpcRawHandoffRecord>,
    last: Option<GrpcRawHandoffRecord>,
}

pub async fn record_grpc_raw_blocks(config: GrpcRawRecordConfig) -> Result<GrpcRawRecordReport> {
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
    fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("create raw gRPC output {}", config.output_dir.display()))?;
    prepare_resume_coverage_warning_path(&config)?;
    let identity = load_or_create_identity(&config)?;
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

    let resume_anchor = journal_state.last.clone();
    let last_durable_slot = resume_anchor.as_ref().map(|row| row.slot);
    let effective_from_slot = effective_resume_slot(config.from_slot, last_durable_slot);
    // A continuous spool may already span many epochs. On restart, an optional boundary stop is
    // relative to the epoch containing the durable tail, not the first epoch ever recorded.
    let mut capture_epoch = journal_state.last.as_ref().map(|row| row.epoch);
    let next_frame_id = journal_state.records;

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
        effective_from_slot,
        resume_overlap_slot: last_durable_slot,
        resume_overlap_observed: resume_anchor.as_ref().map(|_| false),
        first_delivered_slot: None,
        resume_coverage_warning: false,
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
        stopped_low_disk: false,
        available_bytes_at_stop: None,
    };
    if let Some(available) = low_disk_bytes(&config.output_dir, config.min_free_bytes)? {
        report.stopped_low_disk = true;
        report.available_bytes_at_stop = Some(available);
        report.elapsed_ms = started_at.elapsed().as_millis();
        return Ok(report);
    }

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
        WatchdogOutcome::Completed(result) => result?,
        WatchdogOutcome::TotalTimeout => {
            report.timed_out = true;
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
        WatchdogOutcome::IdleTimeout => {
            report.idle_timed_out = true;
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
            result.context("queue initial raw gRPC subscription request")?
        }
        WatchdogOutcome::TotalTimeout => {
            report.timed_out = true;
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
        WatchdogOutcome::IdleTimeout => {
            report.idle_timed_out = true;
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
        WatchdogOutcome::Completed(result) => {
            result.context("open raw gRPC bidirectional subscription")?
        }
        WatchdogOutcome::TotalTimeout => {
            report.timed_out = true;
            report.elapsed_ms = started_at.elapsed().as_millis();
            return Ok(report);
        }
        WatchdogOutcome::IdleTimeout => {
            report.idle_timed_out = true;
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

    while report.frames_written < max_blocks {
        let update = match await_with_watchdogs(stream.next(), deadline, idle_deadline).await {
            WatchdogOutcome::Completed(update) => update,
            WatchdogOutcome::TotalTimeout => {
                report.timed_out = true;
                break;
            }
            WatchdogOutcome::IdleTimeout => {
                report.idle_timed_out = true;
                break;
            }
        };
        let Some(update) = update else {
            report.stream_ended = true;
            break;
        };
        let update = update?;
        if matches!(update.update_oneof.as_ref(), Some(UpdateOneof::Ping(_))) {
            match await_with_watchdogs(
                request_sink.send(subscribe_ping_request()),
                deadline,
                idle_deadline,
            )
            .await
            {
                WatchdogOutcome::Completed(result) => {
                    result.context("reply to raw gRPC subscription ping")?
                }
                WatchdogOutcome::TotalTimeout => {
                    report.timed_out = true;
                    break;
                }
                WatchdogOutcome::IdleTimeout => {
                    report.idle_timed_out = true;
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
                    if let Err(error) = publish_resume_coverage_warning(path, &event) {
                        tracing::error!(
                            warning_file = %path.display(),
                            error = %error,
                            "failed to publish raw gRPC resume-coverage warning event"
                        );
                        break;
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
            break;
        }
        capture_epoch.get_or_insert(epoch_slot.epoch);
        if let Some(available) = low_disk_bytes(&config.output_dir, config.min_free_bytes)? {
            // The durable cursor remains at the preceding journal row, so this slot is requested
            // again on restart rather than being acknowledged and lost.
            report.stopped_low_disk = true;
            report.available_bytes_at_stop = Some(available);
            break;
        }
        if config.require_complete_poh {
            let stats = validate_complete_poh_block(block)
                .with_context(|| format!("validate complete PoH for slot {}", block.slot))?;
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
        let frame_id = next_frame_id
            .checked_add(report.frames_written)
            .context("raw gRPC frame id overflow")?;
        let blockhash_bytes = decode_blockhash(&block.blockhash)?;
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
        // `append_and_sync` is the durability boundary. No cursor, journal, or report state may
        // advance before it succeeds.
        let durable = spool.append_and_sync(metadata, &compressed)?;
        let row = handoff_record_from_block(
            block,
            frame_id,
            epoch_slot,
            durable.location(),
            raw.len() as u64,
            compressed.len() as u64,
            sha256_hex(&raw),
        );
        append_handoff_record(&journal_path, &row)?;
        idle_deadline = idle_deadline_after(TokioInstant::now(), config.idle_timeout_secs)?;

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
    mut visit: F,
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
        return Ok(identity);
    }
    let identity = GrpcRawIdentityFile {
        schema_version: IDENTITY_SCHEMA_VERSION,
        endpoint: config.endpoint.clone(),
        cluster_id: config.cluster_id.clone(),
        origin_node_id: config.origin_node_id.clone(),
        source_id: config.source_id.clone(),
        journal_id: generate_journal_id(config),
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
    let file =
        File::open(path).with_context(|| format!("open raw gRPC identity {}", path.display()))?;
    let identity: GrpcRawIdentityFile = serde_json::from_reader(file)
        .with_context(|| format!("decode raw gRPC identity {}", path.display()))?;
    ensure!(
        identity.schema_version == IDENTITY_SCHEMA_VERSION,
        "unsupported raw gRPC identity schema"
    );
    ensure!(
        identity.payload_format_version == PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
        "unsupported raw gRPC payload format"
    );
    Ok(identity)
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

fn prepare_resume_coverage_warning_path(config: &GrpcRawRecordConfig) -> Result<()> {
    let Some(path) = config.resume_coverage_warning_file.as_deref() else {
        return Ok(());
    };
    let expected = config
        .output_dir
        .join(MONITORING_DIR)
        .join(RESUME_COVERAGE_WARNING_FILE);
    ensure!(
        path == expected,
        "resume-coverage warning path must be {}",
        expected.display()
    );
    let parent = expected
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
            sync_directory(&config.output_dir)?;
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
) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .context("resume-coverage warning path has no parent directory")?;
    if let Some(existing) = read_resume_coverage_warning(path)? {
        ensure!(
            existing.event_id == event.event_id,
            "a different undelivered resume-coverage warning already exists"
        );
        sync_directory(parent)?;
        return Ok(());
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("resume-coverage-warning.json");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let temp_path = path.with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), nonce));
    let result = (|| -> Result<()> {
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
        match fs::hard_link(&temp_path, path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let existing = read_resume_coverage_warning(path)?
                    .context("resume-coverage warning disappeared during publication")?;
                ensure!(
                    existing.event_id == event.event_id,
                    "a different undelivered resume-coverage warning won publication"
                );
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
        }
        fs::remove_file(&temp_path).with_context(|| {
            format!(
                "remove resume-coverage warning temp {}",
                temp_path.display()
            )
        })?;
        sync_directory(parent)?;
        Ok(())
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
                    | "journal/raw-materialization-receipt.json"
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
    Ok((available < minimum).then_some(available))
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
    fn durable_tail_makes_configured_from_slot_bootstrap_only_and_requests_overlap() {
        assert_eq!(effective_resume_slot(Some(100), None), Some(100));
        assert_eq!(effective_resume_slot(None, None), None);
        assert_eq!(effective_resume_slot(Some(999), Some(123)), Some(123));
        assert_eq!(effective_resume_slot(Some(1), Some(123)), Some(123));
        assert_eq!(effective_resume_slot(None, Some(u64::MAX)), Some(u64::MAX));
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
        publish_resume_coverage_warning(&path, &event).unwrap();
        let decoded: GrpcRawResumeCoverageWarning =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(decoded, event);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refuses_to_replace_a_different_pending_resume_warning() {
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
        publish_resume_coverage_warning(&path, &first).unwrap();
        let error = publish_resume_coverage_warning(&path, &second).unwrap_err();
        assert!(error.to_string().contains("different undelivered"));
        assert_eq!(read_resume_coverage_warning(&path).unwrap(), Some(first));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resume_warning_path_is_fixed_below_the_raw_output() {
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
        let expected = root.join(MONITORING_DIR).join(RESUME_COVERAGE_WARNING_FILE);
        config.resume_coverage_warning_file = Some(expected);
        prepare_resume_coverage_warning_path(&config).unwrap();
        assert!(root.join(MONITORING_DIR).is_dir());
        fs::remove_dir_all(root).unwrap();
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
}
