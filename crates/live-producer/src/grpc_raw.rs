//! Minimal, crash-recoverable Yellowstone block recorder.
//!
//! This path deliberately performs no archive conversion and writes no derived sidecars. Each
//! protobuf update is encoded once, compressed as an independent zstd frame, and crossed through
//! the durable ingress-spool boundary before its small handoff journal is advanced.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, ErrorKind, Write},
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
    grpc::connect_grpc,
    ingest::{
        IngressRecordMeta, LogicalKey, ObservationId, SpoolJournalIdentity, SpoolLocation,
        SpoolOptions, SpoolWriter, read_spool_record,
    },
};

const IDENTITY_SCHEMA_VERSION: u32 = 1;
const JOURNAL_SCHEMA_VERSION: u32 = 1;
/// The ingress payload is one independently compressed zstd frame containing the full known-schema
/// `SubscribeUpdate` envelope delivered by tonic. This retains filters and created-at metadata in
/// addition to the block variant. Prost cannot retain protobuf fields unknown to this build.
const PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1: u16 = 2;
const IDENTITY_FILE: &str = "identity.json";
const HANDOFF_JOURNAL_FILE: &str = "raw-blocks.jsonl";
const WAL_ROOT_DIR: &str = "wal";
const SUBSCRIBE_REQUEST_CHANNEL_CAPACITY: usize = 8;
const SUBSCRIBE_PING_ID: i32 = 1;

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
    pub from_slot: Option<u64>,
    pub slots_per_epoch: u64,
    pub stop_at_epoch_boundary: bool,
    pub compression_level: i32,
    pub segment_target_bytes: u64,
    pub max_record_bytes: u64,
    pub min_free_bytes: u64,
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
            from_slot: None,
            slots_per_epoch: OLD_FAITHFUL_SLOTS_PER_EPOCH,
            stop_at_epoch_boundary: false,
            compression_level: 1,
            segment_target_bytes: 256 * 1024 * 1024,
            max_record_bytes: 128 * 1024 * 1024,
            min_free_bytes: 16 * 1024 * 1024 * 1024,
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
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub first_epoch: Option<u64>,
    pub last_epoch: Option<u64>,
    pub raw_bytes_written: u64,
    pub compressed_bytes_written: u64,
    pub compression_ratio: f64,
    pub elapsed_ms: u128,
    pub timed_out: bool,
    pub stream_ended: bool,
    pub stopped_at_epoch_boundary: bool,
    pub stopped_low_disk: bool,
    pub available_bytes_at_stop: Option<u64>,
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

    let last_durable_slot = journal_state.last.as_ref().map(|row| row.slot);
    let resume_slot = last_durable_slot
        .map(|slot| slot.checked_add(1).context("raw gRPC resume slot overflow"))
        .transpose()?;
    let effective_from_slot = match (config.from_slot, resume_slot) {
        (Some(requested), Some(resume)) => Some(requested.max(resume)),
        (Some(requested), None) => Some(requested),
        (None, Some(resume)) => Some(resume),
        (None, None) => None,
    };
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
        first_slot: None,
        last_slot: None,
        first_epoch: None,
        last_epoch: None,
        raw_bytes_written: 0,
        compressed_bytes_written: 0,
        compression_ratio: 0.0,
        elapsed_ms: 0,
        timed_out: false,
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

    let mut client = connect_grpc(&config.endpoint).await?;
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
    request_sink
        .send(request)
        .await
        .context("queue initial raw gRPC subscription request")?;
    let response = client
        .geyser
        .subscribe(request_stream)
        .await
        .context("open raw gRPC bidirectional subscription")?;
    let response_encoding = grpc_response_encoding(response.metadata());
    tracing::info!(
        grpc_response_encoding = response_encoding.as_str(),
        "raw gRPC subscription opened"
    );
    let mut stream = response.into_inner();
    let deadline = TokioInstant::now() + Duration::from_secs(config.timeout_secs);
    let max_blocks = config.max_blocks.max(1) as u64;
    let mut raw = Vec::with_capacity(2 * 1024 * 1024);
    let mut compressed = Vec::with_capacity(1024 * 1024);
    let mut compressor = zstd::bulk::Compressor::new(config.compression_level)
        .context("create raw gRPC zstd compressor")?;

    while report.frames_written < max_blocks {
        let update = tokio::select! {
            _ = sleep_until(deadline) => {
                report.timed_out = true;
                break;
            }
            update = stream.next() => update,
        };
        let Some(update) = update else {
            report.stream_ended = true;
            break;
        };
        let update = update?;
        if matches!(update.update_oneof.as_ref(), Some(UpdateOneof::Ping(_))) {
            request_sink
                .send(subscribe_ping_request())
                .await
                .context("reply to raw gRPC subscription ping")?;
            continue;
        }
        let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
            continue;
        };
        report.frames_seen += 1;
        if effective_from_slot.is_some_and(|from_slot| block.slot < from_slot) {
            report.frames_skipped_before_resume += 1;
            continue;
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
    Ok(report)
}

/// Stream and validate a raw spool one record at a time. The callback receives the exact decoded
/// protobuf update envelope needed by a future archive rebuild/import pass.
pub fn replay_grpc_raw_blocks<F>(
    output_dir: impl AsRef<Path>,
    max_record_bytes: u64,
    mut visit: F,
) -> Result<u64>
where
    F: FnMut(&GrpcRawHandoffRecord, SubscribeUpdate) -> Result<()>,
{
    let output_dir = output_dir.as_ref();
    let identity = read_identity(&output_dir.join(IDENTITY_FILE))?;
    let wal_dir = spool_journal_dir(output_dir, &identity);
    let journal_path = output_dir.join(HANDOFF_JOURNAL_FILE);
    let file = File::open(&journal_path)
        .with_context(|| format!("open raw gRPC journal {}", journal_path.display()))?;
    let mut expected_frame_id = 0u64;
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
        expected_frame_id = expected_frame_id
            .checked_add(1)
            .context("raw gRPC replay frame id overflow")?;
    }
    Ok(expected_frame_id)
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

fn spool_journal_dir(output_dir: &Path, identity: &GrpcRawIdentityFile) -> PathBuf {
    output_dir
        .join(WAL_ROOT_DIR)
        .join(&identity.cluster_id)
        .join(&identity.origin_node_id)
        .join(&identity.source_id)
        .join(hex_bytes(&identity.journal_id))
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
    fn low_disk_guard_can_be_disabled_or_forced() {
        let root = temp_dir("disk-guard");
        fs::create_dir_all(&root).unwrap();
        assert_eq!(low_disk_bytes(&root, 0).unwrap(), None);
        let available = filesystem_available_bytes(&root).unwrap();
        if available < u64::MAX {
            assert_eq!(
                low_disk_bytes(&root, available.saturating_add(1)).unwrap(),
                Some(available)
            );
        }
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
