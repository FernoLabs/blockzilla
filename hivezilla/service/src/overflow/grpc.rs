//! Bounded conversion of the committed gRPC WAL into immutable V1 overflow segments.
//!
//! Logical source sequences map one-to-one to V1 sequences. The catalog is the restart cursor;
//! the scan below never creates a custody ACK or permits deletion of the source journal.

use std::{io::Read, path::PathBuf};

use anyhow::{Context, Result, ensure};
use hivezilla_object_store::ImmutableObjectStore;
use hivezilla_protocol::{
    FRAME_V1_FIXED_ENCODED_LEN, FrameV1, MAX_RECORD_PAYLOAD_BYTES,
    MIN_SEALED_SEGMENT_V1_ENCODED_LEN, SealedSegmentV1, SegmentHeaderV1, StreamManifestV1,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use yellowstone_grpc_proto::prelude::{SubscribeUpdate, subscribe_update::UpdateOneof};

use super::{OverflowCatalog, hex};
use crate::{
    grpc_raw::{
        GrpcRawCommittedCursor, GrpcRawCommittedReadLimits,
        open_grpc_raw_replication_generation_cursors, validate_raw_grpc_update_v1,
    },
    ingest::{
        LogicalKey, RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1, RawReplicationRecord, ReplicationStreamId,
        cumulative_chain_seed,
    },
};

const MAX_RECORDS_PER_SEGMENT: usize = 1024;
const MAX_SEGMENTS_PER_PASS: usize = 1024;
const MAX_DECODED_SEGMENT_BYTES: u64 = 512 * 1024 * 1024;
// protoc --include_imports, no source info, geyser.proto from yellowstone-grpc-proto 12.7.0.
const PROTO_DESCRIPTOR_SHA256: &str =
    "d1afe607fd957c02c6445eba9bd3d1265e5227d1c3e6e192f0076164c24bd1a7";

/// Canonical JSON bytes for the manifest's producer descriptor. Changing any field requires a
/// successor V1 stream. This describes the recorder's stored protobuf, not upstream wire bytes.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcRawOverflowDescriptor {
    adapter: String,
    source: ReplicationStreamId,
    protobuf_descriptor_sha256: String,
    protobuf_encoding: String,
    projection: String,
    compression: String,
    max_decoded_record_bytes: u64,
}

impl GrpcRawOverflowDescriptor {
    pub fn new(source: ReplicationStreamId, max_decoded_record_bytes: u64) -> Result<Self> {
        cumulative_chain_seed(&source).map_err(|error| anyhow::anyhow!("{error:?}"))?;
        ensure!(
            (1..=MAX_RECORD_PAYLOAD_BYTES).contains(&max_decoded_record_bytes),
            "decoded record limit must be 1..={MAX_RECORD_PAYLOAD_BYTES}"
        );
        Ok(Self {
            adapter: "hivezilla-grpc-raw-overflow-v1".into(),
            source,
            protobuf_descriptor_sha256: PROTO_DESCRIPTOR_SHA256.into(),
            protobuf_encoding: "prost-0.14/SubscribeUpdate; exact decode-encode equality".into(),
            projection: "all known fields; block updates only; transaction message config absent"
                .into(),
            compression:
                "one zstd frame preserved verbatim; original settings unrecorded; no recompression"
                    .into(),
            max_decoded_record_bytes,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    fn from_manifest(manifest: &StreamManifestV1) -> Result<Self> {
        ensure!(
            manifest.stream().payload_format() == 1
                && manifest.stream().payload_format_version() == 1,
            "gRPC overflow requires V1 payload format 1/version 1"
        );
        let descriptor: Self = serde_json::from_slice(manifest.producer_descriptor())
            .context("invalid gRPC overflow producer descriptor")?;
        let expected = Self::new(
            descriptor.source.clone(),
            descriptor.max_decoded_record_bytes,
        )?;
        ensure!(
            expected.encode()? == manifest.producer_descriptor(),
            "unsupported or noncanonical gRPC overflow producer descriptor"
        );
        Ok(expected)
    }
}

#[derive(Debug)]
pub enum GrpcRawOverflowSource {
    Generation(PathBuf),
    HotCache(PathBuf),
}

#[derive(Clone, Copy, Debug)]
pub struct GrpcRawOverflowLimits {
    pub max_records_per_segment: usize,
    pub max_segments_per_pass: usize,
}

impl Default for GrpcRawOverflowLimits {
    fn default() -> Self {
        Self {
            max_records_per_segment: 256,
            max_segments_per_pass: 4,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct GrpcRawOverflowReport {
    pub reader_id: String,
    pub stream_id: String,
    pub source: ReplicationStreamId,
    pub first_sequence: u64,
    pub next_sequence: u64,
    pub verified_segments: usize,
    pub verified_records: u64,
    pub verified_bytes: u64,
    pub incomplete_handoff_tail_bytes: u64,
    /// All selected generations in this discovery snapshot were exhausted. A live writer can
    /// append/rotate immediately afterward; this is not a claim about the upstream chain head.
    pub reached_snapshot_end: bool,
}

pub fn spill_grpc_raw(
    catalog: &mut OverflowCatalog,
    reader_id: &str,
    manifest: &StreamManifestV1,
    source: GrpcRawOverflowSource,
    limits: GrpcRawOverflowLimits,
    store: &dyn ImmutableObjectStore,
) -> Result<GrpcRawOverflowReport> {
    ensure!(
        (1..=MAX_RECORDS_PER_SEGMENT).contains(&limits.max_records_per_segment),
        "record count must be 1..={MAX_RECORDS_PER_SEGMENT}"
    );
    ensure!(
        (1..=MAX_SEGMENTS_PER_PASS).contains(&limits.max_segments_per_pass),
        "segment count must be 1..={MAX_SEGMENTS_PER_PASS}"
    );
    let descriptor = GrpcRawOverflowDescriptor::from_manifest(manifest)?;
    let read_limits = GrpcRawCommittedReadLimits {
        max_compressed_record_bytes: catalog
            .limits
            .max_segment_bytes
            .min(MAX_RECORD_PAYLOAD_BYTES),
        max_uncompressed_record_bytes: descriptor.max_decoded_record_bytes,
    };
    let mut cursors = match source {
        GrpcRawOverflowSource::Generation(path) => {
            vec![GrpcRawCommittedCursor::open_fenced(
                path,
                read_limits,
                0,
                &descriptor.source,
            )?]
        }
        GrpcRawOverflowSource::HotCache(path) => {
            open_grpc_raw_replication_generation_cursors(path, read_limits)?
                .into_iter()
                .map(|generation| generation.cursor)
                .filter(|cursor| cursor.stream() == descriptor.source)
                .collect()
        }
    };
    ensure!(
        !cursors.is_empty(),
        "manifest source is absent from journal snapshot"
    );
    catalog.register_manifest(reader_id, manifest, store)?;
    let stream = manifest.stream();
    let mut next = catalog.contiguous_verified_end(reader_id, stream.stream_id())?;
    let mut report = GrpcRawOverflowReport {
        reader_id: reader_id.into(),
        stream_id: hex(stream.stream_id().as_bytes()),
        source: descriptor.source.clone(),
        first_sequence: next.next_sequence(),
        next_sequence: next.next_sequence(),
        verified_segments: 0,
        verified_records: 0,
        verified_bytes: 0,
        incomplete_handoff_tail_bytes: 0,
        reached_snapshot_end: false,
    };
    let mut generation = 0;
    cursors[generation].seek_to_replication_sequence(next.next_sequence())?;
    while report.verified_segments < limits.max_segments_per_pass {
        let pending = catalog
            .ranges_from(reader_id, stream.stream_id(), next.next_sequence(), 1)?
            .into_iter()
            .next();
        if let Some(range) = &pending {
            ensure!(
                !range.verified && range.object.start() == next,
                "overflow catalog has a gap or conflicting prefix at the source cursor"
            );
        }
        // A failed upload pins its original boundary, even if the source grew or tuning changed.
        let records = match &pending {
            Some(range) => {
                usize::try_from(range.object.end().next_sequence() - next.next_sequence())?
            }
            None => limits.max_records_per_segment,
        };
        ensure!(
            (1..=MAX_RECORDS_PER_SEGMENT).contains(&records),
            "pending range exceeds adapter record limit"
        );
        let encoded_budget = pending
            .as_ref()
            .map_or(catalog.limits.max_segment_bytes, |range| {
                range.object.encoded_len()
            });
        ensure!(
            encoded_budget <= catalog.limits.max_segment_bytes,
            "pending range exceeds current segment byte limit"
        );
        let overhead = MIN_SEALED_SEGMENT_V1_ENCODED_LEN as u64
            + records as u64 * FRAME_V1_FIXED_ENCODED_LEN as u64;
        let payload_budget = encoded_budget
            .checked_sub(overhead)
            .filter(|bytes| *bytes > 0)
            .context("segment byte limit leaves no room for payloads")?;
        let cursor = &mut cursors[generation];
        let batch = cursor.read_batch(records, payload_budget, MAX_DECODED_SEGMENT_BYTES)?;
        report.incomplete_handoff_tail_bytes = batch.incomplete_handoff_tail_bytes;
        if batch.records.is_empty() {
            generation += 1;
            if generation == cursors.len() {
                ensure!(
                    pending.is_none(),
                    "pending range source records are unavailable"
                );
                report.reached_snapshot_end = report.incomplete_handoff_tail_bytes == 0;
                break;
            }
            ensure!(
                report.incomplete_handoff_tail_bytes == 0,
                "partial handoff before a later generation"
            );
            cursors[generation].seek_to_replication_sequence(next.next_sequence())?;
            continue;
        }
        if pending.is_some() {
            ensure!(
                batch.records.len() == records,
                "pending range source is incomplete"
            );
        }
        cursor.advance_overflow_scan(&batch)?;
        let start = next;
        let mut frames = Vec::with_capacity(batch.records.len());
        for durable in batch.records {
            ensure!(
                durable.stream == descriptor.source,
                "source stream changed during overflow scan"
            );
            let record = durable.record;
            ensure!(
                record.offer.record.sequence == next.next_sequence(),
                "source sequence gap during overflow conversion"
            );
            validate_payload(&record, descriptor.max_decoded_record_bytes)?;
            let frame = FrameV1::new(next, record.compressed_payload)?;
            next = frame.validate_after(next)?;
            frames.push(frame);
        }
        let segment = SealedSegmentV1::new(SegmentHeaderV1::new(stream, start)?, frames)?;
        if let Some(range) = pending {
            // Validate every byte before retrying the remote create. A source mutation cannot
            // replace or strand an existing intent under a different object key.
            range.object.validate_segment(&segment.encode())?;
        }
        let object = catalog.upload(reader_id, &segment, store)?;
        report.verified_segments += 1;
        report.verified_records += next.next_sequence() - start.next_sequence();
        report.verified_bytes += object.encoded_len();
        report.next_sequence = next.next_sequence();
    }
    Ok(report)
}

fn validate_payload(record: &RawReplicationRecord, max_decoded: u64) -> Result<()> {
    ensure!(
        record.offer.payload_format_version == RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
        "source record is not a legacy gRPC protobuf payload"
    );
    ensure!(
        zstd::zstd_safe::find_frame_compressed_size(&record.compressed_payload)
            .is_ok_and(|size| size == record.compressed_payload.len()),
        "gRPC overflow payload must contain exactly one zstd frame"
    );
    let decoder = zstd::stream::read::Decoder::new(record.compressed_payload.as_slice())?;
    let mut raw = Vec::new();
    decoder.take(max_decoded + 1).read_to_end(&mut raw)?;
    ensure!(
        raw.len() as u64 <= max_decoded && raw.len() as u64 == record.uncompressed_len,
        "gRPC overflow decoded length mismatch"
    );
    ensure!(
        <[u8; 32]>::from(Sha256::digest(&raw)) == record.raw_protobuf_sha256,
        "gRPC overflow protobuf checksum mismatch"
    );
    let update = SubscribeUpdate::decode(raw.as_slice())?;
    validate_raw_grpc_update_v1(&update)?;
    ensure!(
        update.encode_to_vec() == raw,
        "gRPC overflow requires canonical known-schema protobuf bytes"
    );
    let Some(UpdateOneof::Block(block)) = update.update_oneof else {
        anyhow::bail!("gRPC overflow source must contain block updates");
    };
    let mut blockhash = [0; 32];
    five8::decode_32(&block.blockhash, &mut blockhash)
        .map_err(|_| anyhow::anyhow!("invalid gRPC overflow blockhash"))?;
    ensure!(
        record.offer.logical_key
            == LogicalKey::Block {
                slot: block.slot,
                blockhash
            },
        "gRPC overflow block identity differs from journal metadata"
    );
    Ok(())
}
