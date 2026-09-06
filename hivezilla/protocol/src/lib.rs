//! Canonical, dependency-light persisted primitives for Hivezilla V1.
//!
//! This crate intentionally has no storage, network, cloud, scheduler, source
//! adapter, or compaction dependencies. Its normative contract is
//! `docs/design/hivezilla-record-and-sync-protocol.md` in the workspace.

#![forbid(unsafe_code)]

mod custody;
mod error;
mod gap;
mod identity;
mod journal;
mod manifest;
mod overflow;
mod record;
mod registry;
mod sync;
mod terminal;

pub use custody::{
    ACCEPTED_ACK_RECEIPT_V1_FIXED_ENCODED_LEN, ACK_V1_ENCODED_LEN, AcceptedAckReceiptV1, AckV1,
    MAX_ACCEPTED_ACK_RECEIPT_V1_ENCODED_LEN, MAX_AUTHENTICATED_PEER_ID_BYTES,
    SOURCE_RETIREMENT_CHECKPOINT_V1_ENCODED_LEN, SourceRetirementCheckpointV1,
    accepted_ack_receipt_sha256,
};
pub use error::{ProtocolError, Result};
pub use gap::{
    GAP_EVENT_PAYLOAD_V1_FIXED_ENCODED_LEN, GapEventPayloadV1,
    MAX_GAP_EVENT_PAYLOAD_V1_ENCODED_LEN, MAX_GAP_EVENT_POSITION_BYTES,
};
pub use identity::{
    AcceptedAckReceiptSha256, BlockzillaAuthorityId, ClusterGenesisHash,
    DeletionAuthorizingStoreId, DurabilityPolicyId, DurabilityTargetDescriptorSha256,
    DurabilityTargetId, FailureDomainId, OverflowNamespaceSha256, PrefixHash, ProducerConfigSha256,
    SessionId, StreamId, StreamManifestSha256, StreamRegistrySnapshotSha256,
    TerminalCatalogDescriptorSha256,
};
pub use journal::{
    FRAME_V1_FIXED_ENCODED_LEN, FRAME_V1_MAGIC, FrameV1, MIN_SEALED_SEGMENT_V1_ENCODED_LEN,
    SEGMENT_FOOTER_V1_ENCODED_LEN, SEGMENT_FOOTER_V1_MAGIC, SEGMENT_HEADER_V1_ENCODED_LEN,
    SEGMENT_HEADER_V1_MAGIC, SealedSegmentV1, SegmentFooterV1, SegmentHeaderV1,
};
pub use manifest::{
    CONTINUITY_CONTIGUOUS, CONTINUITY_GAP_CONFIRMED, CONTINUITY_GAP_POSSIBLE,
    DURABILITY_POLICY_MAX_TARGETS, DURABILITY_POLICY_MIN_TARGETS, DurabilityPolicyV1,
    DurabilityTargetV1, GAP_EVENT_MAX_PERMITTED_REASONS, GAP_EVENT_MIN_PERMITTED_REASONS,
    GAP_EVENT_PRODUCER_DESCRIPTOR_VERSION_V1, GapEventProducerDescriptorV1, GapEventReasonV1,
    LineageContinuityV1, LineageReasonV1, LineageV1, MANIFEST_VERSION_V1,
    MAX_PRODUCER_DESCRIPTOR_BYTES, MAX_SOURCE_POSITION_DESCRIPTOR_BYTES, StreamManifestV1,
    durability_target_descriptor_sha256, producer_config_sha256,
    terminal_catalog_descriptor_sha256,
};
pub use overflow::{
    MAX_OBJECT_VERSION_BYTES, MAX_OVERFLOW_OBJECT_V1_ENCODED_LEN, OVERFLOW_MANIFEST_KEY_V1_LEN,
    OVERFLOW_OBJECT_KEY_V1_LEN, OVERFLOW_OBJECT_V1_FIXED_ENCODED_LEN, OverflowObjectV1,
    overflow_manifest_key,
};
pub use record::{
    CURSOR_V1_ENCODED_LEN, CursorV1, MAX_RECORD_PAYLOAD_BYTES, MAX_RECORD_V1_ENCODED_LEN,
    MAX_REGISTERED_PAYLOAD_FORMAT_V1, MIN_REGISTERED_PAYLOAD_FORMAT_V1,
    RECORD_V1_FIXED_ENCODED_LEN, REGISTERED_PAYLOAD_FORMAT_VERSION_V1, RecordV1,
    STREAM_HEADER_V1_ENCODED_LEN, StreamHeaderV1, validate_record_payload_len,
};
pub use registry::{
    MAX_REGISTRY_ENTRIES_V1, MAX_REGISTRY_LOGICAL_NAME_BYTES,
    MAX_STREAM_REGISTRY_SNAPSHOT_V1_ENCODED_LEN, STREAM_REGISTRY_ENTRY_V1_FIXED_ENCODED_LEN,
    STREAM_REGISTRY_HEAD_V1_ENCODED_LEN, STREAM_REGISTRY_SNAPSHOT_V1_FIXED_ENCODED_LEN,
    StreamRegistryEntryV1, StreamRegistryHeadV1, StreamRegistrySnapshotV1, StreamRegistryStatusV1,
    stream_registry_snapshot_sha256, validate_stream_registry_transition,
};
pub use sync::{
    CHUNK_FOOTER_V1_ENCODED_LEN, CHUNK_FOOTER_V1_MAGIC, CHUNK_HEADER_V1_ENCODED_LEN,
    CHUNK_HEADER_V1_MAGIC, ChunkFooterV1, ChunkHeaderV1, ErrorV1, FetchRangeV1,
    HiveSyncErrorCodeV1, MAX_CHUNK_RECORDS_V1, MAX_PARALLEL_FETCHES_V1, MAX_SYNC_RECORD_BYTES_V1,
    MIN_SYNC_RECORD_BYTES_V1, MIN_TRANSFER_CHUNK_V1_ENCODED_LEN, OpenV1, ResumeV1,
    TransferChunkCommitV1, TransferChunkV1,
};
pub use terminal::{
    ExternallyVerifiedTerminalRangeV1, MAX_TERMINAL_COPY_RECEIPT_V1_ENCODED_LEN,
    MAX_TERMINAL_OBJECT_KEY_BYTES, MAX_TERMINAL_OBJECT_VERSION_BYTES,
    MAX_TERMINAL_RANGE_INDEX_COPIES_V1, MAX_TERMINAL_RANGE_INDEX_V1_ENCODED_LEN,
    MIN_TERMINAL_RAW_OBJECT_V1_ENCODED_LEN, TERMINAL_COPY_RECEIPT_V1_FIXED_ENCODED_LEN,
    TERMINAL_CURSOR_CHECKPOINT_V1_ENCODED_LEN, TERMINAL_RANGE_INDEX_V1_FIXED_ENCODED_LEN,
    TERMINAL_RAW_FOOTER_V1_ENCODED_LEN, TERMINAL_RAW_FOOTER_V1_MAGIC, TERMINAL_RAW_HEADER_V1_MAGIC,
    TERMINAL_RAW_OBJECT_KEY_V1_LEN, TerminalCopyReceiptV1, TerminalCursorCheckpointV1,
    TerminalRangeIndexInsertDispositionV1, TerminalRangeIndexV1, TerminalRawFooterV1,
    TerminalRawHeaderV1, TerminalRawObjectDecodeLimitsV1, TerminalRawObjectV1,
    TerminalVerificationV1, terminal_raw_object_key, validate_terminal_checkpoint_transition,
    validate_terminal_range_index_insert,
};
