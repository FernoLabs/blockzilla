//! Restart-safe, read-only bridge from the Blockzilla receiver WAL into the established raw-gRPC
//! generation format.
//!
//! The receiver spool remains canonical and is never opened for write. The target is a separate
//! derived raw generation that existing PoH verification and epoch materialization commands can
//! consume. Receiver byte durability, bridge progress, archive indexing, and compaction remain
//! separate watermarks; none of this module's state authorizes receiver-spool cleanup.

use std::{
    fs::{self, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use prost::Message;
use serde::{Deserialize, Serialize};
use yellowstone_grpc_proto::prelude::{SubscribeUpdate, subscribe_update::UpdateOneof};

use super::*;
use crate::ingest::{
    ContentDigest, IngressRecordMeta, ObservationId, RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
    RECEIVER_PROGRESS_WAL_FILE, ReceiverDurableProgress, ReplicationStreamId, SpoolJournalIdentity,
    SpoolLocation, SpoolOptions, SpoolRecord, SpoolWriter, read_receiver_durable_progress,
    read_spool_committed_snapshot_after, read_spool_record, spool_journal_dir_path,
    with_receiver_capacity_lock,
};

const BRIDGE_CURSOR_SCHEMA_VERSION: u32 = 1;
const BRIDGE_CURSOR_FILE: &str = "RECEIVER-BRIDGE-CURSOR.v1.json";
const MAX_BRIDGE_CURSOR_BYTES: u64 = 64 * 1024;
const BRIDGE_RESERVE_SAFETY_BYTES: u64 = 1024 * 1024;

/// One bounded bridge pass. Re-running with the same configuration resumes from the target's
/// durable handoff journal and the separately synced source cursor.
#[derive(Debug, Clone)]
pub struct GrpcReceiverBridgeConfig {
    pub receiver_spool_root: PathBuf,
    pub stream: ReplicationStreamId,
    pub output_dir: PathBuf,
    pub endpoint_label: String,
    pub slots_per_epoch: u64,
    pub target_segment_bytes: u64,
    pub max_compressed_record_bytes: u64,
    pub max_uncompressed_record_bytes: u64,
    /// Hard logical-byte ceiling for the entire derived output tree.
    pub max_output_bytes: u64,
    /// Filesystem free-space floor preserved before every target append.
    pub min_free_bytes: u64,
    pub max_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrpcReceiverBridgeReport {
    pub receiver_spool_root: PathBuf,
    pub output_dir: PathBuf,
    pub stream: ReplicationStreamId,
    /// Local receiver byte-durability watermark. This is not a signed cleanup receipt.
    pub receiver_durable_through_sequence: u64,
    pub receiver_durable_lsn: u64,
    pub target_frames_before: u64,
    pub target_frames_after: u64,
    pub frames_written: u64,
    pub output_bytes_after: u64,
    pub max_output_bytes: u64,
    pub min_free_bytes: u64,
    pub first_slot_written: Option<u64>,
    pub last_slot_written: Option<u64>,
    pub reached_receiver_durable_tail: bool,
    pub bridge_cursor_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiverBridgeCursor {
    schema_version: u32,
    stream: ReplicationStreamId,
    through_sequence: u64,
    through_content_digest: ContentDigest,
    source_location: SpoolLocation,
    target_physical_journal_id: [u8; 16],
    target_frame_id: u64,
}

/// Copy at most `max_records` locally durable receiver-prefix records into a separate standard
/// raw-gRPC generation.
///
/// Source records are bounded by the locally fsynced receiver-progress WAL. Each target record is
/// independently appended+fsynced, followed by its target handoff row and then the bridge cursor.
/// A crash in either gap is reconciled by the target WAL/handoff machinery and an exact source ↔
/// target tail comparison. The source receiver files are never locked, modified, or deleted.
pub fn bridge_receiver_grpc_raw(
    config: GrpcReceiverBridgeConfig,
) -> Result<GrpcReceiverBridgeReport> {
    validate_bridge_config(&config)?;
    let source_identity = SpoolJournalIdentity {
        cluster_id: config.stream.cluster_id.clone(),
        origin_node_id: config.stream.origin_node_id.clone(),
        source_id: config.stream.source_id.clone(),
        journal_id: config.stream.journal_id,
    };
    let source_journal_dir = spool_journal_dir_path(&config.receiver_spool_root, &source_identity)?;
    let receiver_progress_path = source_journal_dir.join(RECEIVER_PROGRESS_WAL_FILE);
    let receiver_progress =
        read_receiver_durable_progress(&receiver_progress_path, &config.stream)?
            .context("receiver has no locally durable raw-byte progress yet")?;

    fs::create_dir_all(&config.output_dir).with_context(|| {
        format!(
            "create receiver bridge output {}",
            config.output_dir.display()
        )
    })?;
    ensure_bridge_filesystem_reserve(
        &config.output_dir,
        config.min_free_bytes,
        BRIDGE_RESERVE_SAFETY_BYTES,
    )?;
    let target_config = GrpcRawRecordConfig {
        endpoint: config.endpoint_label.clone(),
        output_dir: config.output_dir.clone(),
        slots_per_epoch: config.slots_per_epoch,
        segment_target_bytes: config.target_segment_bytes,
        max_record_bytes: config.max_compressed_record_bytes,
        require_complete_poh: true,
        cluster_id: config.stream.cluster_id.clone(),
        origin_node_id: config.stream.origin_node_id.clone(),
        source_id: config.stream.source_id.clone(),
        ..GrpcRawRecordConfig::default()
    };
    let target_identity = load_or_create_identity_with_replication_anchor(
        &target_config,
        Some(GrpcRawReplicationGenerationAnchor {
            journal_id: config.stream.journal_id,
            sequence_base: 0,
        }),
    )?;
    ensure!(
        target_identity.replication_stream_id() == config.stream,
        "receiver bridge target changed logical replication stream"
    );
    let target_spool_options = SpoolOptions {
        segment_target_bytes: config.target_segment_bytes,
        max_record_bytes: config.max_compressed_record_bytes,
    };
    let mut target_spool = SpoolWriter::open(
        config.output_dir.join(WAL_ROOT_DIR),
        target_identity.spool_identity(),
        target_spool_options,
    )
    .context("open receiver bridge target raw WAL")?;
    let target_journal_path = config.output_dir.join(HANDOFF_JOURNAL_FILE);
    let mut target_journal = recover_handoff_journal(&target_journal_path)?;
    reconcile_handoff_tail(
        &mut target_spool,
        &target_journal_path,
        &mut target_journal,
        config.slots_per_epoch,
        config.max_compressed_record_bytes,
    )
    .context("reconcile receiver bridge target WAL/handoff")?;
    let target_frames_before = target_journal.records;
    ensure!(
        target_frames_before
            <= receiver_progress
                .through_sequence
                .checked_add(1)
                .context("receiver durable sequence exhausted")?,
        "receiver bridge target is ahead of receiver durable progress"
    );

    let cursor_path = config.output_dir.join(BRIDGE_CURSOR_FILE);
    let mut cursor = read_bridge_cursor(&cursor_path)?;
    validate_or_rebuild_cursor(
        &config,
        &receiver_progress,
        &source_identity,
        &source_journal_dir,
        &target_identity,
        &target_spool,
        &target_journal,
        &cursor_path,
        &mut cursor,
    )?;

    let mut output_bytes = directory_file_bytes(&config.output_dir)?;
    ensure!(
        output_bytes <= config.max_output_bytes,
        "receiver bridge output is already {output_bytes} bytes, over its {} byte ceiling",
        config.max_output_bytes
    );
    let mut cursor_file_bytes = if cursor.is_some() {
        regular_file_bytes(&cursor_path)?
    } else {
        0
    };

    let mut next_frame_id = target_journal.records;
    let mut frames_written = 0u64;
    let mut first_slot_written = None;
    let mut last_slot_written = None;
    let source_after = cursor.as_ref().map(|cursor| cursor.source_location);
    let snapshot = read_spool_committed_snapshot_after(
        &config.receiver_spool_root,
        source_identity,
        config.max_compressed_record_bytes,
        source_after,
        receiver_progress.through_sequence,
        config.max_records,
        |source_record| {
            let source_sequence = source_record.metadata.observation.sequence;
            ensure!(
                source_sequence == next_frame_id,
                "receiver bridge source sequence {source_sequence} does not match target frame {next_frame_id}"
            );
            let (update, raw_len, raw_sha256) = decode_receiver_record(
                &source_record,
                config.max_compressed_record_bytes,
                config.max_uncompressed_record_bytes,
            )?;
            let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
                return Err(anyhow!(
                    "receiver bridge sequence {source_sequence} is not a block update"
                ));
            };
            validate_complete_poh_block(block).with_context(|| {
                format!("verify receiver bridge PoH at source sequence {source_sequence}")
            })?;
            let epoch_slot = EpochSlot::from_slot(block.slot, config.slots_per_epoch);
            let target_metadata = IngressRecordMeta::from_payload(
                target_identity.cluster_id.clone(),
                ObservationId {
                    origin_node_id: target_identity.origin_node_id.clone(),
                    journal_id: target_identity.journal_id,
                    sequence: next_frame_id,
                },
                target_identity.source_id.clone(),
                source_record.metadata.logical_key.clone(),
                PAYLOAD_FORMAT_ZSTD_PROTOBUF_UPDATE_V1,
                &source_record.payload,
            );
            ensure!(
                target_metadata.content_digest == source_record.metadata.content_digest,
                "receiver bridge content digest changed while copying exact compressed bytes"
            );
            with_receiver_capacity_lock(&config.receiver_spool_root, || {
                let projection = target_spool
                    .project_append(&target_metadata, &source_record.payload)
                    .context("project receiver bridge target WAL append")?;
                let row = handoff_record_from_block(
                    block,
                    next_frame_id,
                    epoch_slot,
                    projection.location,
                    raw_len,
                    source_record.payload.len() as u64,
                    raw_sha256,
                );
                let next_cursor = ReceiverBridgeCursor {
                    schema_version: BRIDGE_CURSOR_SCHEMA_VERSION,
                    stream: config.stream.clone(),
                    through_sequence: source_sequence,
                    through_content_digest: source_record.metadata.content_digest,
                    source_location: source_record.location,
                    target_physical_journal_id: target_identity.journal_id,
                    target_frame_id: next_frame_id,
                };
                let handoff_bytes = encoded_handoff_record_len(&row)?;
                let next_cursor_bytes = encoded_bridge_cursor_len(&next_cursor)?;
                let durable_growth = projection
                    .additional_bytes
                    .checked_add(handoff_bytes)
                    .context("receiver bridge append growth overflow")?;
                let projected_output_bytes = output_bytes
                    .checked_add(durable_growth)
                    .and_then(|bytes| bytes.checked_add(next_cursor_bytes))
                    .and_then(|bytes| bytes.checked_sub(cursor_file_bytes))
                    .context("receiver bridge output byte count overflow")?;
                ensure!(
                    projected_output_bytes <= config.max_output_bytes,
                    "receiver bridge output ceiling would be crossed: current {output_bytes}, projected {projected_output_bytes}, maximum {}",
                    config.max_output_bytes
                );
                let transient_growth = durable_growth
                    .checked_add(next_cursor_bytes)
                    .and_then(|bytes| bytes.checked_add(BRIDGE_RESERVE_SAFETY_BYTES))
                    .context("receiver bridge reserve projection overflow")?;
                ensure_bridge_filesystem_reserve(
                    &config.output_dir,
                    config.min_free_bytes,
                    transient_growth,
                )?;
                let durable = target_spool
                    .append_and_sync(target_metadata, &source_record.payload)
                    .context("append receiver bridge target WAL")?;
                ensure!(
                    durable.location() == projection.location,
                    "receiver bridge WAL append differed from its admission projection"
                );
                append_handoff_record(&target_journal_path, &row)
                    .context("append receiver bridge target handoff")?;
                write_bridge_cursor(&cursor_path, &next_cursor)?;
                cursor = Some(next_cursor);
                output_bytes = projected_output_bytes;
                cursor_file_bytes = next_cursor_bytes;
                Ok(())
            })?;
            first_slot_written.get_or_insert(block.slot);
            last_slot_written = Some(block.slot);
            frames_written = frames_written
                .checked_add(1)
                .context("receiver bridge frame count overflow")?;
            next_frame_id = next_frame_id
                .checked_add(1)
                .context("receiver bridge target frame sequence exhausted")?;
            Ok(())
        },
    )?;

    if snapshot.reached_durable_tail {
        let cursor = cursor
            .as_ref()
            .context("receiver bridge reached durable tail without a cursor")?;
        ensure!(
            cursor.through_sequence == receiver_progress.through_sequence
                && cursor.through_content_digest == receiver_progress.through_content_digest
                && cursor.source_location == receiver_progress.spool_location,
            "receiver bridge tail does not match the receiver's exact durable progress"
        );
    }

    Ok(GrpcReceiverBridgeReport {
        receiver_spool_root: config.receiver_spool_root,
        output_dir: config.output_dir,
        stream: config.stream,
        receiver_durable_through_sequence: receiver_progress.through_sequence,
        receiver_durable_lsn: receiver_progress.durable_lsn,
        target_frames_before,
        target_frames_after: next_frame_id,
        frames_written,
        output_bytes_after: output_bytes,
        max_output_bytes: config.max_output_bytes,
        min_free_bytes: config.min_free_bytes,
        first_slot_written,
        last_slot_written,
        reached_receiver_durable_tail: snapshot.reached_durable_tail,
        bridge_cursor_path: cursor_path,
    })
}

fn validate_bridge_config(config: &GrpcReceiverBridgeConfig) -> Result<()> {
    ensure!(
        config.receiver_spool_root.is_absolute() && config.receiver_spool_root != Path::new("/"),
        "receiver spool root must be an absolute non-root directory"
    );
    ensure!(
        config.output_dir.is_absolute() && config.output_dir != Path::new("/"),
        "receiver bridge output must be an absolute non-root directory"
    );
    ensure!(
        config.output_dir.components().all(|component| !matches!(
            component,
            std::path::Component::CurDir | std::path::Component::ParentDir
        )),
        "receiver bridge output must not contain . or .. components"
    );
    ensure!(
        config.output_dir != config.receiver_spool_root
            && !config.output_dir.starts_with(&config.receiver_spool_root),
        "receiver bridge output must be outside the canonical receiver spool"
    );
    let canonical_source = fs::canonicalize(&config.receiver_spool_root).with_context(|| {
        format!(
            "canonicalize receiver spool root {}",
            config.receiver_spool_root.display()
        )
    })?;
    let prospective_output = canonicalize_prospective_path(&config.output_dir)?;
    ensure!(
        prospective_output != canonical_source
            && !prospective_output.starts_with(&canonical_source)
            && !canonical_source.starts_with(&prospective_output),
        "receiver bridge output and canonical receiver spool must not contain one another"
    );
    ensure!(
        !config.endpoint_label.is_empty() && config.endpoint_label.len() <= 256,
        "receiver bridge endpoint label must be 1..=256 bytes"
    );
    ensure!(
        config.slots_per_epoch > 0,
        "receiver bridge slots per epoch must be non-zero"
    );
    ensure!(
        (1..=1_000_000).contains(&config.max_records),
        "receiver bridge batch must be 1..=1000000 records"
    );
    ensure!(
        config.target_segment_bytes > 8,
        "receiver bridge target segment size is too small"
    );
    ensure!(
        config.max_compressed_record_bytes > 0 && config.max_uncompressed_record_bytes > 0,
        "receiver bridge record limits must be non-zero"
    );
    let minimum_output_bytes = config
        .max_compressed_record_bytes
        .checked_add(BRIDGE_RESERVE_SAFETY_BYTES)
        .context("receiver bridge minimum output size overflow")?;
    ensure!(
        config.max_output_bytes >= minimum_output_bytes,
        "receiver bridge max output bytes must admit one maximum record plus safety overhead"
    );
    Ok(())
}

fn ensure_bridge_filesystem_reserve(
    path: &Path,
    minimum: u64,
    projected_growth: u64,
) -> Result<()> {
    if minimum == 0 {
        return Ok(());
    }
    let available = filesystem_available_bytes(path)?;
    let required = minimum
        .checked_add(projected_growth)
        .context("receiver bridge filesystem reserve overflow")?;
    ensure!(
        available >= required,
        "receiver bridge filesystem reserve would be crossed: available {available}, projected growth {projected_growth}, reserve {minimum}"
    );
    Ok(())
}

fn canonicalize_prospective_path(path: &Path) -> Result<PathBuf> {
    let mut cursor = path;
    let mut missing = Vec::new();
    loop {
        match fs::symlink_metadata(cursor) {
            Ok(_) => break,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let name = cursor
                    .file_name()
                    .context("prospective receiver bridge path has no existing ancestor")?;
                missing.push(name.to_os_string());
                cursor = cursor
                    .parent()
                    .context("prospective receiver bridge path has no parent")?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "inspect receiver bridge output ancestor {}",
                        cursor.display()
                    )
                });
            }
        }
    }
    let mut resolved = fs::canonicalize(cursor)
        .with_context(|| format!("canonicalize receiver bridge ancestor {}", cursor.display()))?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

#[allow(clippy::too_many_arguments)]
fn validate_or_rebuild_cursor(
    config: &GrpcReceiverBridgeConfig,
    receiver_progress: &ReceiverDurableProgress,
    source_identity: &SpoolJournalIdentity,
    source_journal_dir: &Path,
    target_identity: &GrpcRawIdentityFile,
    target_spool: &SpoolWriter,
    target_journal: &JournalState,
    cursor_path: &Path,
    cursor: &mut Option<ReceiverBridgeCursor>,
) -> Result<()> {
    if target_journal.records == 0 {
        ensure!(
            cursor.is_none(),
            "receiver bridge cursor exists for an empty target"
        );
        return Ok(());
    }
    let target_tail_frame = target_journal
        .records
        .checked_sub(1)
        .context("receiver bridge target record underflow")?;
    ensure!(
        target_tail_frame <= receiver_progress.through_sequence,
        "receiver bridge target tail is beyond receiver durability"
    );
    if let Some(existing) = cursor.as_ref() {
        validate_cursor_identity(existing, &config.stream, target_identity)?;
        ensure!(
            existing.target_frame_id == existing.through_sequence,
            "receiver bridge cursor lost one-to-one source/target mapping"
        );
        ensure!(
            existing.target_frame_id <= target_tail_frame,
            "receiver bridge cursor is ahead of its target handoff"
        );
    }

    let target_tail_row = target_journal
        .last
        .as_ref()
        .context("receiver bridge target has no tail row")?;
    let target_tail = target_spool.read_record(
        target_spool
            .last_record()
            .context("receiver bridge target has no WAL tail")?,
    )?;
    ensure!(
        target_tail.location == target_tail_row.location()
            && target_tail.metadata.observation.sequence == target_tail_frame,
        "receiver bridge target WAL/handoff tail changed during cursor recovery"
    );

    if cursor
        .as_ref()
        .is_some_and(|existing| existing.target_frame_id == target_tail_frame)
    {
        let existing = cursor.as_ref().expect("checked receiver bridge cursor");
        let source_tail = read_spool_record(
            source_journal_dir,
            existing.source_location,
            config.max_compressed_record_bytes,
        )?;
        validate_source_target_tail(existing, &source_tail, &target_tail)?;
        return Ok(());
    }

    let after = cursor.as_ref().map(|existing| existing.source_location);
    let next_sequence = match cursor.as_ref() {
        Some(existing) => existing
            .through_sequence
            .checked_add(1)
            .context("receiver bridge cursor sequence exhausted")?,
        None => 0,
    };
    let records_to_recover = target_tail_frame
        .checked_sub(next_sequence)
        .and_then(|difference| difference.checked_add(1))
        .context("receiver bridge cursor recovery range is invalid")?;
    let records_to_recover = usize::try_from(records_to_recover)
        .context("receiver bridge cursor recovery range exceeds usize")?;
    let mut expected_sequence = next_sequence;
    let mut recovered_cursor = None;
    let report = read_spool_committed_snapshot_after(
        &config.receiver_spool_root,
        source_identity.clone(),
        config.max_compressed_record_bytes,
        after,
        target_tail_frame,
        records_to_recover,
        |source_record| {
            let sequence = source_record.metadata.observation.sequence;
            ensure!(
                sequence == expected_sequence,
                "receiver bridge cursor recovery found sequence {sequence}, expected {expected_sequence}"
            );
            if sequence == target_tail_frame {
                let candidate = ReceiverBridgeCursor {
                    schema_version: BRIDGE_CURSOR_SCHEMA_VERSION,
                    stream: config.stream.clone(),
                    through_sequence: sequence,
                    through_content_digest: source_record.metadata.content_digest,
                    source_location: source_record.location,
                    target_physical_journal_id: target_identity.journal_id,
                    target_frame_id: target_tail_frame,
                };
                validate_source_target_tail(&candidate, &source_record, &target_tail)?;
                recovered_cursor = Some(candidate);
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .context("receiver bridge cursor recovery sequence exhausted")?;
            Ok(())
        },
    )?;
    ensure!(
        report.reached_durable_tail && report.last_sequence == Some(target_tail_frame),
        "receiver bridge could not rebuild its cursor through target frame {target_tail_frame}"
    );
    let recovered_cursor = recovered_cursor.context("receiver bridge cursor recovery lost tail")?;
    write_bridge_cursor(cursor_path, &recovered_cursor)?;
    *cursor = Some(recovered_cursor);
    Ok(())
}

fn validate_cursor_identity(
    cursor: &ReceiverBridgeCursor,
    stream: &ReplicationStreamId,
    target_identity: &GrpcRawIdentityFile,
) -> Result<()> {
    ensure!(
        cursor.schema_version == BRIDGE_CURSOR_SCHEMA_VERSION,
        "unsupported receiver bridge cursor schema"
    );
    ensure!(
        cursor.stream == *stream && cursor.target_physical_journal_id == target_identity.journal_id,
        "receiver bridge cursor belongs to a different source or target"
    );
    Ok(())
}

fn validate_source_target_tail(
    cursor: &ReceiverBridgeCursor,
    source: &SpoolRecord,
    target: &SpoolRecord,
) -> Result<()> {
    ensure!(
        source.location == cursor.source_location
            && source.metadata.observation.sequence == cursor.through_sequence
            && source.metadata.content_digest == cursor.through_content_digest
            && source.metadata.cluster_id == cursor.stream.cluster_id
            && source.metadata.observation.origin_node_id == cursor.stream.origin_node_id
            && source.metadata.observation.journal_id == cursor.stream.journal_id
            && source.metadata.source_id == cursor.stream.source_id
            && target.metadata.observation.sequence == cursor.target_frame_id
            && source.metadata.cluster_id == target.metadata.cluster_id
            && source.metadata.source_id == target.metadata.source_id
            && source.metadata.logical_key == target.metadata.logical_key
            && source.metadata.payload_format_version == target.metadata.payload_format_version
            && source.metadata.content_digest == target.metadata.content_digest
            && source.payload == target.payload,
        "receiver bridge source and target tails are not the same exact compressed block"
    );
    Ok(())
}

fn decode_receiver_record(
    record: &SpoolRecord,
    max_compressed_record_bytes: u64,
    max_uncompressed_record_bytes: u64,
) -> Result<(SubscribeUpdate, u64, String)> {
    ensure!(
        record.payload.len() as u64 <= max_compressed_record_bytes,
        "receiver bridge compressed record exceeds configured maximum"
    );
    ensure!(
        record.metadata.payload_format_version == RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
        "receiver bridge does not understand payload format {}",
        record.metadata.payload_format_version
    );
    let max_uncompressed = usize::try_from(max_uncompressed_record_bytes)
        .context("receiver bridge uncompressed limit exceeds usize")?;
    let raw = zstd::bulk::decompress(&record.payload, max_uncompressed)
        .context("decompress receiver bridge raw protobuf")?;
    ensure!(
        !raw.is_empty() && raw.len() as u64 <= max_uncompressed_record_bytes,
        "receiver bridge raw protobuf length is outside configured bounds"
    );
    let update = SubscribeUpdate::decode(raw.as_slice())
        .context("decode receiver bridge SubscribeUpdate")?;
    let Some(UpdateOneof::Block(block)) = update.update_oneof.as_ref() else {
        return Err(anyhow!("receiver bridge record is not a block update"));
    };
    ensure!(
        record.metadata.logical_key
            == (LogicalKey::Block {
                slot: block.slot,
                blockhash: decode_blockhash(&block.blockhash)?,
            }),
        "receiver bridge protobuf identity differs from spool metadata"
    );
    Ok((update, raw.len() as u64, sha256_hex(&raw)))
}

fn read_bridge_cursor(path: &Path) -> Result<Option<ReceiverBridgeCursor>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect receiver bridge cursor {}", path.display()));
        }
    };
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "receiver bridge cursor is not a regular file: {}",
        path.display()
    );
    ensure!(
        metadata.len() <= MAX_BRIDGE_CURSOR_BYTES,
        "receiver bridge cursor exceeds {MAX_BRIDGE_CURSOR_BYTES} bytes"
    );
    let file = open_regular_readonly_nofollow(path, "receiver bridge cursor")?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_BRIDGE_CURSOR_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_BRIDGE_CURSOR_BYTES,
        "receiver bridge cursor grew over its read bound"
    );
    serde_json::from_slice(&bytes)
        .with_context(|| format!("decode receiver bridge cursor {}", path.display()))
        .map(Some)
}

fn write_bridge_cursor(path: &Path, cursor: &ReceiverBridgeCursor) -> Result<()> {
    let parent = path
        .parent()
        .context("receiver bridge cursor has no parent directory")?;
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let temporary = parent.join(format!(
        ".{BRIDGE_CURSOR_FILE}.{}-{unique}.tmp",
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options.open(&temporary).with_context(|| {
            format!(
                "create receiver bridge cursor temporary {}",
                temporary.display()
            )
        })?;
        serde_json::to_writer_pretty(&mut file, cursor)?;
        file.write_all(b"\n")?;
        ensure!(
            file.metadata()?.len() <= MAX_BRIDGE_CURSOR_BYTES,
            "encoded receiver bridge cursor exceeds {MAX_BRIDGE_CURSOR_BYTES} bytes"
        );
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)
            .with_context(|| format!("publish receiver bridge cursor {}", path.display()))?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn encoded_bridge_cursor_len(cursor: &ReceiverBridgeCursor) -> Result<u64> {
    u64::try_from(serde_json::to_vec_pretty(cursor)?.len())
        .context("receiver bridge cursor length exceeds u64")?
        .checked_add(1)
        .context("receiver bridge cursor length overflow")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{
        BlockzillaRawReceiver, CommitmentEvidence, LogicalKey, ObservationId,
        REPLICATION_PROTOCOL_VERSION, RawReceiverConfig, RawReceiverLimits, RawReplicationRecord,
        ReplicationOffer, compute_content_digest,
    };
    use sha2::{Digest, Sha256};

    #[derive(Debug)]
    struct TestTree(PathBuf);

    impl TestTree {
        fn new(label: &str) -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "blockzilla-receiver-bridge-{label}-{}-{unique}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestTree {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn stream() -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: "solana-mainnet".to_string(),
            origin_node_id: "hetzner-raw".to_string(),
            source_id: "grpc-raw-hetzner-backup".to_string(),
            journal_id: [17; 16],
        }
    }

    fn receiver_config(root: &Path) -> RawReceiverConfig {
        RawReceiverConfig {
            spool_root: root.to_path_buf(),
            stream: stream(),
            primary_id: "blockzilla-nas".to_string(),
            primary_term: 1,
            spool_options: SpoolOptions {
                segment_target_bytes: 1024 * 1024,
                max_record_bytes: 1024 * 1024,
            },
            max_spool_bytes: 64 * 1024 * 1024,
            reserve_free_bytes: 0,
            limits: RawReceiverLimits {
                max_batch_records: 8,
                max_batch_compressed_bytes: 8 * 1024 * 1024,
                max_batch_uncompressed_bytes: 8 * 1024 * 1024,
                max_compressed_record_bytes: 1024 * 1024,
                max_uncompressed_record_bytes: 1024 * 1024,
            },
        }
    }

    fn bridge_config(tree: &TestTree, max_records: usize) -> GrpcReceiverBridgeConfig {
        GrpcReceiverBridgeConfig {
            receiver_spool_root: tree.0.join("receiver"),
            stream: stream(),
            output_dir: tree.0.join("derived-raw"),
            endpoint_label: "blockzilla-receiver-wal".to_string(),
            slots_per_epoch: 10,
            target_segment_bytes: 1024 * 1024,
            max_compressed_record_bytes: 1024 * 1024,
            max_uncompressed_record_bytes: 1024 * 1024,
            max_output_bytes: 64 * 1024 * 1024,
            min_free_bytes: 0,
            max_records,
        }
    }

    fn push_varint(output: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn push_varint_field(output: &mut Vec<u8>, field: u32, value: u64) {
        push_varint(output, u64::from(field) << 3);
        push_varint(output, value);
    }

    fn push_bytes_field(output: &mut Vec<u8>, field: u32, bytes: &[u8]) {
        push_varint(output, (u64::from(field) << 3) | 2);
        push_varint(output, bytes.len() as u64);
        output.extend_from_slice(bytes);
    }

    fn encode_hash(hash: &[u8; 32]) -> String {
        let mut output = [0u8; five8::BASE58_ENCODED_32_MAX_LEN];
        let length = five8::encode_32(hash, &mut output) as usize;
        String::from_utf8(output[..length].to_vec()).unwrap()
    }

    fn raw_block(slot: u64, blockhash: [u8; 32]) -> Vec<u8> {
        let parent_hash = [blockhash[0].wrapping_sub(1); 32];
        let mut entry = Vec::new();
        push_varint_field(&mut entry, 1, slot);
        push_varint_field(&mut entry, 2, 0);
        push_varint_field(&mut entry, 3, 1);
        push_bytes_field(&mut entry, 4, &blockhash);
        push_varint_field(&mut entry, 5, 0);
        push_varint_field(&mut entry, 6, 0);

        let mut block = Vec::new();
        push_varint_field(&mut block, 1, slot);
        push_bytes_field(&mut block, 2, encode_hash(&blockhash).as_bytes());
        push_varint_field(&mut block, 7, slot.saturating_sub(1));
        push_bytes_field(&mut block, 8, encode_hash(&parent_hash).as_bytes());
        push_varint_field(&mut block, 9, 0);
        push_varint_field(&mut block, 10, 0);
        push_varint_field(&mut block, 12, 1);
        push_bytes_field(&mut block, 13, &entry);

        let mut update = Vec::new();
        push_bytes_field(&mut update, 5, &block);
        update
    }

    fn record(sequence: u64, slot: u64, hash_byte: u8) -> RawReplicationRecord {
        let raw = raw_block(slot, [hash_byte; 32]);
        let compressed_payload = zstd::bulk::compress(&raw, 1).unwrap();
        let stream = stream();
        let logical_key = LogicalKey::Block {
            slot,
            blockhash: [hash_byte; 32],
        };
        let content_digest = compute_content_digest(
            &stream.cluster_id,
            &logical_key,
            RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
            &compressed_payload,
        );
        RawReplicationRecord {
            offer: ReplicationOffer {
                protocol_version: REPLICATION_PROTOCOL_VERSION,
                cluster_id: stream.cluster_id.clone(),
                record: ObservationId {
                    origin_node_id: stream.origin_node_id,
                    journal_id: stream.journal_id,
                    sequence,
                },
                source_id: stream.source_id,
                logical_key,
                content_digest,
                payload_len: compressed_payload.len() as u64,
                payload_format_version: RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
                commitment: CommitmentEvidence::Confirmed,
            },
            compressed_payload,
            raw_protobuf_sha256: Sha256::digest(&raw).into(),
            uncompressed_len: raw.len() as u64,
        }
    }

    fn source_segment_bytes(receiver: &BlockzillaRawReceiver) -> u64 {
        fs::read_dir(receiver.spool_journal_dir())
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("segment-") && name.ends_with(".wal"))
            })
            .map(|entry| entry.metadata().unwrap().len())
            .sum()
    }

    #[test]
    fn active_receiver_bridge_resumes_without_source_mutation_and_materializes() {
        let tree = TestTree::new("end-to-end");
        let receiver_root = tree.0.join("receiver");
        let mut receiver = BlockzillaRawReceiver::open(receiver_config(&receiver_root)).unwrap();
        receiver.push_batch(vec![record(0, 100, 10)]).unwrap();
        let source_bytes_after_first = source_segment_bytes(&receiver);

        let first = bridge_receiver_grpc_raw(bridge_config(&tree, 1)).unwrap();
        assert_eq!(first.target_frames_before, 0);
        assert_eq!(first.target_frames_after, 1);
        assert_eq!(first.frames_written, 1);
        assert!(first.reached_receiver_durable_tail);
        assert_eq!(source_segment_bytes(&receiver), source_bytes_after_first);

        receiver.push_batch(vec![record(1, 101, 11)]).unwrap();
        let source_bytes_after_second = source_segment_bytes(&receiver);
        let second = bridge_receiver_grpc_raw(bridge_config(&tree, 1)).unwrap();
        assert_eq!(second.target_frames_before, 1);
        assert_eq!(second.target_frames_after, 2);
        assert_eq!(second.frames_written, 1);
        assert!(second.reached_receiver_durable_tail);
        assert_eq!(source_segment_bytes(&receiver), source_bytes_after_second);

        // Simulate a crash after target handoff fsync but before (or while replacing) the bridge
        // cursor. Recovery scans the read-only source, verifies the exact target tail, and does not
        // append a duplicate.
        fs::remove_file(tree.0.join("derived-raw").join(BRIDGE_CURSOR_FILE)).unwrap();
        let recovered = bridge_receiver_grpc_raw(bridge_config(&tree, 1)).unwrap();
        assert_eq!(recovered.target_frames_before, 2);
        assert_eq!(recovered.target_frames_after, 2);
        assert_eq!(recovered.frames_written, 0);
        assert!(recovered.reached_receiver_durable_tail);
        assert_eq!(source_segment_bytes(&receiver), source_bytes_after_second);

        let raw_output = tree.0.join("derived-raw");
        let verified = verify_grpc_raw_poh(raw_output.clone(), 1024 * 1024, 2).unwrap();
        assert_eq!(verified.records_verified, 2);
        assert_eq!(verified.first_slot, Some(100));
        assert_eq!(verified.last_slot, Some(101));

        // Existing archive/index materialization consumes the derived standard raw generation.
        // Its receipt remains an index/materialization watermark, not receiver cleanup authority.
        let archive = tree.0.join("epoch-10-archive");
        let materialized = materialize_grpc_raw_blocks(GrpcRawMaterializeConfig {
            input_dir: raw_output,
            archive_dir: archive.clone(),
            epoch: 10,
            max_record_bytes: 1024 * 1024,
            pubkey_hot_registry_path: None,
            pubkey_hot_count: 0,
        })
        .unwrap();
        assert_eq!(materialized.receipt.blocks_written, 2);
        assert!(
            archive
                .join("journal")
                .join(MATERIALIZATION_RECEIPT_FILE)
                .is_file()
        );
    }

    #[test]
    fn bridge_reserve_and_output_ceiling_fail_before_copying_source_data() {
        let tree = TestTree::new("admission");
        let receiver_root = tree.0.join("receiver");
        let mut receiver = BlockzillaRawReceiver::open(receiver_config(&receiver_root)).unwrap();
        receiver.push_batch(vec![record(0, 100, 10)]).unwrap();
        let source_bytes = source_segment_bytes(&receiver);

        let mut reserve_blocked = bridge_config(&tree, 1);
        reserve_blocked.min_free_bytes = u64::MAX;
        let error = bridge_receiver_grpc_raw(reserve_blocked).unwrap_err();
        assert!(error.to_string().contains("filesystem reserve"));
        assert_eq!(source_segment_bytes(&receiver), source_bytes);

        let mut ceiling_blocked = bridge_config(&tree, 1);
        ceiling_blocked.max_output_bytes = 1;
        let error = bridge_receiver_grpc_raw(ceiling_blocked).unwrap_err();
        assert!(error.to_string().contains("max output bytes"));
        assert_eq!(source_segment_bytes(&receiver), source_bytes);
    }
}
