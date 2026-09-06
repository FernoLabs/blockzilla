//! Generated Hivezilla V1 protobuf bindings and strict conversion boundaries.
//!
//! Protobuf is transport-only. Every persisted identity inside a `bytes` field
//! is decoded through `hivezilla-protocol`; no protobuf serialization is used
//! as a hash or storage identity.
//!
//! Raw decoders return explicitly named structural types. Discovery, Hello,
//! and replay responses are promoted only through context-bound APIs supplied
//! with the configured authority, exact registry predecessor, authenticated
//! cursor/range membership, and current missing-cursor decision. The runtime
//! must still enforce frame ordering, active authenticated sessions and cutover
//! tokens, supply each record's actual predecessor, and enforce Fetch response
//! ordering and aggregate stream budgets.
//!
//! The raw-byte preflight guarantees in this module apply only when `decode_*`
//! receives the complete decompressed frame before generated Prost decoding.
//! Tonic decodes generated request/response messages before invoking handlers
//! by default, so Gate 4 must install equivalent codec/body limits and raw-byte
//! preflight; validating an already decoded message cannot undo its allocation.

use std::{collections::BTreeMap, error::Error, fmt};

use hivezilla_protocol::{
    AckV1, BlockzillaAuthorityId, CursorV1, DeletionAuthorizingStoreId, DurabilityPolicyId,
    ErrorV1, FetchRangeV1, HiveSyncErrorCodeV1, MAX_REGISTRY_ENTRIES_V1, OpenV1, RecordV1,
    ResumeV1, SessionId, StreamHeaderV1, StreamId, StreamManifestSha256, StreamManifestV1,
    StreamRegistryHeadV1, StreamRegistrySnapshotV1, TransferChunkCommitV1,
    validate_stream_registry_transition,
};
use prost::Message;

pub mod sync {
    tonic::include_proto!("hivezilla.sync.v1");
}

pub mod public_exit {
    tonic::include_proto!("hivezilla.public.v1");
}

pub const SYNC_CONTROL_PROTOBUF_MAX_BYTES: usize = 65_536;
pub const SYNC_RECORD_WIRE_MAX_BYTES: usize = 134_217_781;
pub const SYNC_SERVER_RECORD_FRAME_MAX_BYTES: usize = 134_217_786;
pub const FETCH_CHUNK_BYTES_MAX_BYTES: usize = 4_194_304;
pub const FETCH_RANGE_PART_MAX_BYTES: usize = 4_194_309;

pub const PUBLIC_SUBSCRIBE_REQUEST_MAX_BYTES: usize = 4_096;
pub const PUBLIC_LIST_STREAMS_REQUEST_MAX_BYTES: usize = 4_096;
pub const PUBLIC_DISCOVERY_RESPONSE_MAX_BYTES: usize = 67_108_864;
pub const PUBLIC_CONTROL_FRAME_MAX_BYTES: usize = 1_048_576;
pub const PUBLIC_EVENT_WIRE_MAX_BYTES: usize = 134_217_781;
pub const PUBLIC_SERVER_EVENT_FRAME_MAX_BYTES: usize = 134_217_786;
pub const PUBLIC_AVAILABLE_RANGES_MAX: usize = 1_024;
pub const PUBLIC_PROTOCOL_VERSION_V1: u32 = 1;

#[derive(Debug)]
pub enum WireError {
    Protocol(hivezilla_protocol::ProtocolError),
    ProtobufDecode(prost::DecodeError),
    MalformedProtobuf {
        context: &'static str,
        reason: &'static str,
    },
    MessageTooLarge {
        context: &'static str,
        actual: usize,
        max: usize,
    },
    InvalidLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    MissingOneof {
        context: &'static str,
    },
    InvalidBoolean {
        field: &'static str,
    },
    UnknownEnum {
        field: &'static str,
        value: i32,
    },
    InvalidValue {
        field: &'static str,
        reason: &'static str,
    },
    TooManyItems {
        field: &'static str,
        actual: usize,
        max: usize,
    },
    NonCanonicalOrder {
        field: &'static str,
    },
    RegistryMismatch {
        reason: &'static str,
    },
    ReplayMatrix {
        reason: &'static str,
    },
}

impl fmt::Display for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => {
                write!(formatter, "invalid canonical Hivezilla value: {error}")
            }
            Self::ProtobufDecode(error) => write!(formatter, "invalid protobuf: {error}"),
            Self::MalformedProtobuf { context, reason } => {
                write!(formatter, "invalid {context} protobuf wire bytes: {reason}")
            }
            Self::MessageTooLarge {
                context,
                actual,
                max,
            } => write!(
                formatter,
                "{context} protobuf length {actual} exceeds V1 limit {max}"
            ),
            Self::InvalidLength {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "{field} must contain exactly {expected} bytes, got {actual}"
            ),
            Self::MissingOneof { context } => {
                write!(formatter, "{context} has no selected oneof value")
            }
            Self::InvalidBoolean { field } => write!(formatter, "{field} must be true"),
            Self::UnknownEnum { field, value } => {
                write!(formatter, "{field} has zero or unknown enum value {value}")
            }
            Self::InvalidValue { field, reason } => {
                write!(formatter, "invalid {field}: {reason}")
            }
            Self::TooManyItems { field, actual, max } => {
                write!(formatter, "{field} has {actual} items, limit is {max}")
            }
            Self::NonCanonicalOrder { field } => {
                write!(formatter, "{field} is not in canonical order")
            }
            Self::RegistryMismatch { reason } => write!(formatter, "registry mismatch: {reason}"),
            Self::ReplayMatrix { reason } => write!(formatter, "invalid replay response: {reason}"),
        }
    }
}

impl Error for WireError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::ProtobufDecode(error) => Some(error),
            _ => None,
        }
    }
}

impl From<hivezilla_protocol::ProtocolError> for WireError {
    fn from(value: hivezilla_protocol::ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

impl From<prost::DecodeError> for WireError {
    fn from(value: prost::DecodeError) -> Self {
        Self::ProtobufDecode(value)
    }
}

pub type WireResult<T> = std::result::Result<T, WireError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidatedSyncClientFrameV1 {
    Open(OpenV1),
    Ack(AckV1),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidatedSyncServerFrameV1 {
    Resume(Box<ResumeV1>),
    Record(RecordV1),
    Error(ErrorV1),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidatedFetchRangePartV1 {
    ChunkBytes(Vec<u8>),
    Commit(TransferChunkCommitV1),
    Error(ErrorV1),
}

pub fn decode_sync_client_frame(encoded: &[u8]) -> WireResult<ValidatedSyncClientFrameV1> {
    let wire = decode_bounded::<sync::SyncClientFrameV1>(
        encoded,
        SYNC_CONTROL_PROTOBUF_MAX_BYTES,
        "SyncClientFrameV1",
    )?;
    validate_sync_client_frame(wire, encoded.len())
}

pub(crate) fn validate_sync_client_frame(
    wire: sync::SyncClientFrameV1,
    protobuf_len: usize,
) -> WireResult<ValidatedSyncClientFrameV1> {
    ensure_reported_len("SyncClientFrameV1", protobuf_len, wire.encoded_len())?;
    ensure_message_len(
        "SyncClientFrameV1",
        protobuf_len,
        SYNC_CONTROL_PROTOBUF_MAX_BYTES,
    )?;
    match wire.frame.ok_or(WireError::MissingOneof {
        context: "SyncClientFrameV1.frame",
    })? {
        sync::sync_client_frame_v1::Frame::Open(open) => {
            validate_open(open).map(ValidatedSyncClientFrameV1::Open)
        }
        sync::sync_client_frame_v1::Frame::Ack(ack) => {
            validate_ack(ack).map(ValidatedSyncClientFrameV1::Ack)
        }
    }
}

pub fn validate_open(wire: sync::OpenWireV1) -> WireResult<OpenV1> {
    let protected_cursor = if wire.protected_cursor.is_empty() {
        None
    } else {
        Some(CursorV1::decode(&wire.protected_cursor)?)
    };
    Ok(OpenV1::new(
        fixed_id::<16, StreamId>(&wire.stream_id, "OpenWireV1.stream_id")?,
        fixed_id::<16, DeletionAuthorizingStoreId>(
            &wire.terminal_store_id,
            "OpenWireV1.terminal_store_id",
        )?,
        protected_cursor,
    ))
}

pub fn validate_ack(wire: sync::AckWireV1) -> WireResult<AckV1> {
    Ok(AckV1::new(
        fixed_id::<16, StreamId>(&wire.stream_id, "AckWireV1.stream_id")?,
        fixed_id::<16, DeletionAuthorizingStoreId>(
            &wire.terminal_store_id,
            "AckWireV1.terminal_store_id",
        )?,
        fixed_id::<32, StreamManifestSha256>(
            &wire.stream_manifest_sha256,
            "AckWireV1.stream_manifest_sha256",
        )?,
        fixed_id::<16, DurabilityPolicyId>(&wire.policy_id, "AckWireV1.policy_id")?,
        CursorV1::decode(&wire.protected_cursor)?,
    ))
}

pub fn decode_sync_server_frame(
    encoded: &[u8],
    record_previous: CursorV1,
) -> WireResult<ValidatedSyncServerFrameV1> {
    ensure_message_len(
        "SyncServerFrameV1",
        encoded.len(),
        SYNC_SERVER_RECORD_FRAME_MAX_BYTES,
    )?;
    preflight_sync_server_frame(encoded)?;
    let wire = decode_bounded::<sync::SyncServerFrameV1>(
        encoded,
        SYNC_SERVER_RECORD_FRAME_MAX_BYTES,
        "SyncServerFrameV1",
    )?;
    validate_sync_server_frame(wire, encoded.len(), record_previous)
}

pub(crate) fn validate_sync_server_frame(
    wire: sync::SyncServerFrameV1,
    protobuf_len: usize,
    record_previous: CursorV1,
) -> WireResult<ValidatedSyncServerFrameV1> {
    ensure_reported_len("SyncServerFrameV1", protobuf_len, wire.encoded_len())?;
    let frame = wire.frame.ok_or(WireError::MissingOneof {
        context: "SyncServerFrameV1.frame",
    })?;
    match frame {
        sync::sync_server_frame_v1::Frame::Resume(resume) => {
            ensure_message_len(
                "SyncServerFrameV1.control",
                protobuf_len,
                SYNC_CONTROL_PROTOBUF_MAX_BYTES,
            )?;
            validate_resume(resume)
                .map(Box::new)
                .map(ValidatedSyncServerFrameV1::Resume)
        }
        sync::sync_server_frame_v1::Frame::Record(record) => {
            ensure_message_len(
                "SyncServerFrameV1.record",
                protobuf_len,
                SYNC_SERVER_RECORD_FRAME_MAX_BYTES,
            )?;
            validate_sync_record(record, record_previous).map(ValidatedSyncServerFrameV1::Record)
        }
        sync::sync_server_frame_v1::Frame::Error(error) => {
            ensure_message_len(
                "SyncServerFrameV1.control",
                protobuf_len,
                SYNC_CONTROL_PROTOBUF_MAX_BYTES,
            )?;
            validate_sync_error(error).map(ValidatedSyncServerFrameV1::Error)
        }
    }
}

pub fn validate_resume(wire: sync::ResumeWireV1) -> WireResult<ResumeV1> {
    let max_parallel_fetches =
        u16::try_from(wire.max_parallel_fetches).map_err(|_| WireError::InvalidValue {
            field: "ResumeWireV1.max_parallel_fetches",
            reason: "must fit u16",
        })?;
    ResumeV1::new(
        StreamHeaderV1::decode(&wire.stream)?,
        fixed_id::<16, SessionId>(&wire.session_id, "ResumeWireV1.session_id")?,
        CursorV1::decode(&wire.first_available)?,
        CursorV1::decode(&wire.bulk_start)?,
        CursorV1::decode(&wire.cutover)?,
        wire.max_record_bytes,
        wire.max_chunk_records,
        max_parallel_fetches,
    )
    .map_err(Into::into)
}

pub fn validate_sync_record(wire: sync::RecordWireV1, previous: CursorV1) -> WireResult<RecordV1> {
    ensure_message_len(
        "RecordWireV1",
        wire.encoded_len(),
        SYNC_RECORD_WIRE_MAX_BYTES,
    )?;
    RecordV1::decode_after(&wire.record, previous).map_err(Into::into)
}

pub fn validate_sync_error(wire: sync::HiveSyncErrorWireV1) -> WireResult<ErrorV1> {
    HiveSyncErrorCodeV1::try_from(wire.code)
        .map(ErrorV1::new)
        .map_err(Into::into)
}

pub fn decode_fetch_range_request(encoded: &[u8]) -> WireResult<FetchRangeV1> {
    let wire = decode_bounded::<sync::FetchRangeRequestV1>(
        encoded,
        SYNC_CONTROL_PROTOBUF_MAX_BYTES,
        "FetchRangeRequestV1",
    )?;
    validate_fetch_range_request(wire, encoded.len())
}

pub(crate) fn validate_fetch_range_request(
    wire: sync::FetchRangeRequestV1,
    protobuf_len: usize,
) -> WireResult<FetchRangeV1> {
    ensure_reported_len("FetchRangeRequestV1", protobuf_len, wire.encoded_len())?;
    ensure_message_len(
        "FetchRangeRequestV1",
        protobuf_len,
        SYNC_CONTROL_PROTOBUF_MAX_BYTES,
    )?;
    FetchRangeV1::new(
        fixed_id::<16, SessionId>(&wire.session_id, "FetchRangeRequestV1.session_id")?,
        CursorV1::decode(&wire.cutover)?,
        wire.first_sequence,
        wire.next_sequence,
    )
    .map_err(Into::into)
}

pub fn validate_transfer_commit(
    wire: sync::TransferChunkCommitWireV1,
) -> WireResult<TransferChunkCommitV1> {
    TransferChunkCommitV1::new(
        CursorV1::decode(&wire.start)?,
        CursorV1::decode(&wire.end)?,
        wire.encoded_len,
        fixed_array::<32>(
            &wire.encoded_sha256,
            "TransferChunkCommitWireV1.encoded_sha256",
        )?,
    )
    .map_err(Into::into)
}

pub fn decode_fetch_range_part(encoded: &[u8]) -> WireResult<ValidatedFetchRangePartV1> {
    ensure_message_len(
        "FetchRangePartWireV1",
        encoded.len(),
        FETCH_RANGE_PART_MAX_BYTES,
    )?;
    preflight_fetch_range_part(encoded)?;
    let wire = decode_bounded::<sync::FetchRangePartWireV1>(
        encoded,
        FETCH_RANGE_PART_MAX_BYTES,
        "FetchRangePartWireV1",
    )?;
    validate_fetch_range_part(wire, encoded.len())
}

pub(crate) fn validate_fetch_range_part(
    wire: sync::FetchRangePartWireV1,
    protobuf_len: usize,
) -> WireResult<ValidatedFetchRangePartV1> {
    ensure_reported_len("FetchRangePartWireV1", protobuf_len, wire.encoded_len())?;
    let part = wire.part.ok_or(WireError::MissingOneof {
        context: "FetchRangePartWireV1.part",
    })?;
    match part {
        sync::fetch_range_part_wire_v1::Part::ChunkBytes(bytes) => {
            ensure_message_len(
                "FetchRangePartWireV1.chunk_bytes",
                protobuf_len,
                FETCH_RANGE_PART_MAX_BYTES,
            )?;
            if bytes.is_empty() {
                return Err(WireError::InvalidValue {
                    field: "FetchRangePartWireV1.chunk_bytes",
                    reason: "must be non-empty",
                });
            }
            if bytes.len() > FETCH_CHUNK_BYTES_MAX_BYTES {
                return Err(WireError::MessageTooLarge {
                    context: "FetchRangePartWireV1.chunk_bytes",
                    actual: bytes.len(),
                    max: FETCH_CHUNK_BYTES_MAX_BYTES,
                });
            }
            Ok(ValidatedFetchRangePartV1::ChunkBytes(bytes))
        }
        sync::fetch_range_part_wire_v1::Part::Commit(commit) => {
            ensure_message_len(
                "FetchRangePartWireV1.control",
                protobuf_len,
                SYNC_CONTROL_PROTOBUF_MAX_BYTES,
            )?;
            validate_transfer_commit(commit).map(ValidatedFetchRangePartV1::Commit)
        }
        sync::fetch_range_part_wire_v1::Part::Error(error) => {
            ensure_message_len(
                "FetchRangePartWireV1.control",
                protobuf_len,
                SYNC_CONTROL_PROTOBUF_MAX_BYTES,
            )?;
            validate_sync_error(error).map(ValidatedFetchRangePartV1::Error)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicFeedKindV1 {
    RawShred,
    ShredBlockObservation,
}

impl PublicFeedKindV1 {
    pub fn from_stream(stream: StreamHeaderV1) -> WireResult<Self> {
        match stream.payload_format() {
            2 | 3 => Ok(Self::RawShred),
            6 => Ok(Self::ShredBlockObservation),
            _ => Err(WireError::InvalidValue {
                field: "public stream payload_format",
                reason: "must be format 2, 3, or 6",
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructurallyValidatedPublicStreamListV1 {
    registry_head: StreamRegistryHeadV1,
    registry: StreamRegistrySnapshotV1,
    public_manifests: Vec<StreamManifestV1>,
}

pub fn decode_list_streams_request(encoded: &[u8]) -> WireResult<()> {
    let wire = decode_bounded::<public_exit::ListStreamsRequestV1>(
        encoded,
        PUBLIC_LIST_STREAMS_REQUEST_MAX_BYTES,
        "ListStreamsRequestV1",
    )?;
    validate_list_streams_request(wire, encoded.len())
}

pub(crate) fn validate_list_streams_request(
    _wire: public_exit::ListStreamsRequestV1,
    protobuf_len: usize,
) -> WireResult<()> {
    ensure_message_len(
        "ListStreamsRequestV1",
        protobuf_len,
        PUBLIC_LIST_STREAMS_REQUEST_MAX_BYTES,
    )
}

impl StructurallyValidatedPublicStreamListV1 {
    #[must_use]
    pub const fn registry_head(&self) -> StreamRegistryHeadV1 {
        self.registry_head
    }

    #[must_use]
    pub const fn registry(&self) -> &StreamRegistrySnapshotV1 {
        &self.registry
    }

    #[must_use]
    pub fn public_manifests(&self) -> &[StreamManifestV1] {
        &self.public_manifests
    }

    #[must_use]
    pub fn manifest(&self, stream_id: StreamId) -> Option<&StreamManifestV1> {
        self.public_manifests
            .binary_search_by_key(&stream_id, |manifest| manifest.stream().stream_id())
            .ok()
            .map(|index| &self.public_manifests[index])
    }

    /// Binds a structurally decoded discovery response to the configured
    /// Blockzilla authority and an exact registry baseline.
    pub fn validate_context<'discovery>(
        &'discovery self,
        context: DiscoveryValidationContextV1<'_>,
    ) -> WireResult<ContextValidatedPublicStreamListV1<'discovery>> {
        if self.registry.blockzilla_authority_id() != context.configured_authority {
            return Err(WireError::RegistryMismatch {
                reason: "registry authority differs from configured Blockzilla authority",
            });
        }
        if let Some(previous) = context.previous_snapshot
            && previous.blockzilla_authority_id() != context.configured_authority
        {
            return Err(WireError::RegistryMismatch {
                reason: "prior registry authority differs from configured Blockzilla authority",
            });
        }
        if context.previous_snapshot != Some(&self.registry) {
            validate_stream_registry_transition(context.previous_snapshot, &self.registry)?;
        }
        Ok(ContextValidatedPublicStreamListV1 { discovery: self })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DiscoveryValidationContextV1<'a> {
    configured_authority: BlockzillaAuthorityId,
    previous_snapshot: Option<&'a StreamRegistrySnapshotV1>,
}

impl<'a> DiscoveryValidationContextV1<'a> {
    #[must_use]
    pub const fn new(
        configured_authority: BlockzillaAuthorityId,
        previous_snapshot: Option<&'a StreamRegistrySnapshotV1>,
    ) -> Self {
        Self {
            configured_authority,
            previous_snapshot,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ContextValidatedPublicStreamListV1<'a> {
    discovery: &'a StructurallyValidatedPublicStreamListV1,
}

impl<'a> ContextValidatedPublicStreamListV1<'a> {
    #[must_use]
    pub const fn registry(&self) -> &'a StreamRegistrySnapshotV1 {
        &self.discovery.registry
    }

    #[must_use]
    pub fn public_manifests(&self) -> &'a [StreamManifestV1] {
        &self.discovery.public_manifests
    }

    #[must_use]
    pub fn manifest(&self, stream_id: StreamId) -> Option<&'a StreamManifestV1> {
        self.discovery.manifest(stream_id)
    }
}

pub fn decode_public_stream_list(
    encoded: &[u8],
) -> WireResult<StructurallyValidatedPublicStreamListV1> {
    ensure_message_len(
        "PublicStreamListWireV1",
        encoded.len(),
        PUBLIC_DISCOVERY_RESPONSE_MAX_BYTES,
    )?;
    enforce_repeated_field_limit(
        encoded,
        3,
        MAX_REGISTRY_ENTRIES_V1,
        "PublicStreamListWireV1.public_manifests",
    )?;
    let wire = decode_bounded::<public_exit::PublicStreamListWireV1>(
        encoded,
        PUBLIC_DISCOVERY_RESPONSE_MAX_BYTES,
        "PublicStreamListWireV1",
    )?;
    validate_public_stream_list(wire, encoded.len())
}

pub(crate) fn validate_public_stream_list(
    wire: public_exit::PublicStreamListWireV1,
    protobuf_len: usize,
) -> WireResult<StructurallyValidatedPublicStreamListV1> {
    ensure_reported_len("PublicStreamListWireV1", protobuf_len, wire.encoded_len())?;
    ensure_message_len(
        "PublicStreamListWireV1",
        protobuf_len,
        PUBLIC_DISCOVERY_RESPONSE_MAX_BYTES,
    )?;
    if wire.public_manifests.len() > MAX_REGISTRY_ENTRIES_V1 {
        return Err(WireError::TooManyItems {
            field: "PublicStreamListWireV1.public_manifests",
            actual: wire.public_manifests.len(),
            max: MAX_REGISTRY_ENTRIES_V1,
        });
    }
    let registry_head = StreamRegistryHeadV1::decode(&wire.registry_head)?;
    let registry = StreamRegistrySnapshotV1::decode(&wire.registry)?;
    registry_head.validate_snapshot(&registry)?;

    let mut public_manifests = Vec::with_capacity(wire.public_manifests.len());
    for encoded_manifest in wire.public_manifests {
        public_manifests.push(StreamManifestV1::decode(&encoded_manifest)?);
    }
    if public_manifests
        .windows(2)
        .any(|pair| pair[0].stream().stream_id() >= pair[1].stream().stream_id())
    {
        return Err(WireError::NonCanonicalOrder {
            field: "PublicStreamListWireV1.public_manifests",
        });
    }
    let registry_by_stream = registry
        .entries()
        .iter()
        .map(|entry| (entry.stream_id(), entry))
        .collect::<BTreeMap<_, _>>();
    for manifest in &public_manifests {
        PublicFeedKindV1::from_stream(manifest.stream())?;
        let entry = registry_by_stream
            .get(&manifest.stream().stream_id())
            .copied()
            .ok_or(WireError::RegistryMismatch {
                reason: "public manifest stream ID is absent from the registry",
            })?;
        if entry.stream_manifest_sha256() != manifest.stream().stream_manifest_sha256() {
            return Err(WireError::RegistryMismatch {
                reason: "public manifest digest differs from the registry entry",
            });
        }
    }
    Ok(StructurallyValidatedPublicStreamListV1 {
        registry_head,
        registry,
        public_manifests,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicStartV1 {
    Latest,
    Cursor(CursorV1),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscribeV1 {
    stream_id: StreamId,
    start: PublicStartV1,
}

impl SubscribeV1 {
    #[must_use]
    pub const fn stream_id(self) -> StreamId {
        self.stream_id
    }

    #[must_use]
    pub const fn start(self) -> PublicStartV1 {
        self.start
    }

    pub fn validate_discovery<'discovery>(
        self,
        discovery: ContextValidatedPublicStreamListV1<'discovery>,
    ) -> WireResult<ContextValidatedSubscribeV1<'discovery>> {
        let manifest = discovery
            .manifest(self.stream_id)
            .ok_or(WireError::RegistryMismatch {
                reason: "subscription stream is not in the exit's public manifest subset",
            })?;
        if let PublicStartV1::Cursor(cursor) = self.start {
            validate_cursor_for_stream_if_zero(
                cursor,
                manifest.stream(),
                "SubscribeRequestV1.cursor",
            )?;
        }
        Ok(ContextValidatedSubscribeV1 {
            request: self,
            discovery,
            manifest,
            feed: PublicFeedKindV1::from_stream(manifest.stream())?,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ContextValidatedSubscribeV1<'a> {
    request: SubscribeV1,
    discovery: ContextValidatedPublicStreamListV1<'a>,
    manifest: &'a StreamManifestV1,
    feed: PublicFeedKindV1,
}

impl<'a> ContextValidatedSubscribeV1<'a> {
    #[must_use]
    pub const fn request(self) -> SubscribeV1 {
        self.request
    }

    #[must_use]
    pub const fn manifest(self) -> &'a StreamManifestV1 {
        self.manifest
    }

    #[must_use]
    pub const fn stream(self) -> StreamHeaderV1 {
        self.manifest.stream()
    }

    #[must_use]
    pub const fn feed(self) -> PublicFeedKindV1 {
        self.feed
    }
}

pub fn decode_subscribe_request(encoded: &[u8]) -> WireResult<SubscribeV1> {
    let wire = decode_bounded::<public_exit::SubscribeRequestV1>(
        encoded,
        PUBLIC_SUBSCRIBE_REQUEST_MAX_BYTES,
        "SubscribeRequestV1",
    )?;
    validate_subscribe_request(wire, encoded.len())
}

pub(crate) fn validate_subscribe_request(
    wire: public_exit::SubscribeRequestV1,
    protobuf_len: usize,
) -> WireResult<SubscribeV1> {
    ensure_reported_len("SubscribeRequestV1", protobuf_len, wire.encoded_len())?;
    ensure_message_len(
        "SubscribeRequestV1",
        protobuf_len,
        PUBLIC_SUBSCRIBE_REQUEST_MAX_BYTES,
    )?;
    let stream_id = fixed_id::<16, StreamId>(&wire.stream_id, "SubscribeRequestV1.stream_id")?;
    let start = match wire.start.ok_or(WireError::MissingOneof {
        context: "SubscribeRequestV1.start",
    })? {
        public_exit::subscribe_request_v1::Start::Latest(true) => PublicStartV1::Latest,
        public_exit::subscribe_request_v1::Start::Latest(false) => {
            return Err(WireError::InvalidBoolean {
                field: "SubscribeRequestV1.latest",
            });
        }
        public_exit::subscribe_request_v1::Start::Cursor(cursor) => {
            PublicStartV1::Cursor(CursorV1::decode(&cursor)?)
        }
    };
    Ok(SubscribeV1 { stream_id, start })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CursorRangeV1 {
    start: CursorV1,
    end: CursorV1,
}

impl CursorRangeV1 {
    pub fn new(start: CursorV1, end: CursorV1) -> WireResult<Self> {
        if start.next_sequence() >= end.next_sequence() {
            return Err(WireError::InvalidValue {
                field: "CursorRangeWireV1",
                reason: "range must be non-empty and increasing",
            });
        }
        Ok(Self { start, end })
    }

    #[must_use]
    pub const fn start(self) -> CursorV1 {
        self.start
    }

    #[must_use]
    pub const fn end(self) -> CursorV1 {
        self.end
    }
}

pub fn validate_cursor_range(wire: public_exit::CursorRangeWireV1) -> WireResult<CursorRangeV1> {
    CursorRangeV1::new(CursorV1::decode(&wire.start)?, CursorV1::decode(&wire.end)?)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructurallyValidatedPublicHelloV1 {
    stream: StreamHeaderV1,
    available: Vec<CursorRangeV1>,
    live_tail: CursorV1,
}

impl StructurallyValidatedPublicHelloV1 {
    #[must_use]
    pub const fn stream(&self) -> StreamHeaderV1 {
        self.stream
    }

    #[must_use]
    pub fn available(&self) -> &[CursorRangeV1] {
        &self.available
    }

    #[must_use]
    pub const fn live_tail(&self) -> CursorV1 {
        self.live_tail
    }

    pub fn validate_for_request<'hello>(
        &'hello self,
        subscription: ContextValidatedSubscribeV1<'_>,
        context: &impl PublicHelloValidationContextV1,
    ) -> WireResult<ContextValidatedPublicHelloV1<'hello>> {
        if self.stream != subscription.stream() {
            return Err(WireError::InvalidValue {
                field: "PublicHelloWireV1.stream",
                reason: "stream header differs from the discovery-bound subscription manifest",
            });
        }
        let request = subscription.request();
        let requested = match request.start {
            PublicStartV1::Latest => self.live_tail,
            PublicStartV1::Cursor(cursor) => cursor,
        };
        validate_cursor_for_stream_if_zero(requested, self.stream, "SubscribeRequestV1.cursor")?;
        if !context.exact_cursor_is_member(self.stream, self.live_tail) {
            return Err(WireError::InvalidValue {
                field: "PublicHelloWireV1.live_tail",
                reason: "runtime cursor index does not verify live_tail",
            });
        }
        if !context.exact_cursor_is_member(self.stream, requested) {
            return Err(WireError::InvalidValue {
                field: "SubscribeRequestV1.cursor",
                reason: "runtime cursor index does not verify the exact requested prefix",
            });
        }
        for range in &self.available {
            if !context.exact_cursor_is_member(self.stream, range.start)
                || !context.exact_cursor_is_member(self.stream, range.end)
                || !context.exact_range_is_available(self.stream, *range)
            {
                return Err(WireError::InvalidValue {
                    field: "PublicHelloWireV1.available",
                    reason: "runtime replay snapshot does not verify an advertised range",
                });
            }
        }
        if requested.next_sequence() > self.live_tail.next_sequence() {
            return Err(WireError::InvalidValue {
                field: "SubscribeRequestV1.cursor",
                reason: "cursor is after live_tail",
            });
        }
        if requested.next_sequence() == self.live_tail.next_sequence() {
            if requested != self.live_tail {
                return Err(WireError::InvalidValue {
                    field: "SubscribeRequestV1.cursor",
                    reason: "cursor prefix differs from live_tail",
                });
            }
            return Ok(ContextValidatedPublicHelloV1 {
                hello: self,
                request,
            });
        }
        if !self.available.iter().any(|range| {
            range.start.next_sequence() <= requested.next_sequence()
                && range.end.next_sequence() >= self.live_tail.next_sequence()
                && (range.start.next_sequence() != requested.next_sequence()
                    || range.start == requested)
                && (range.end.next_sequence() != self.live_tail.next_sequence()
                    || range.end == self.live_tail)
        }) {
            return Err(WireError::InvalidValue {
                field: "PublicHelloWireV1.available",
                reason: "no continuous advertised range covers requested through live_tail",
            });
        }
        Ok(ContextValidatedPublicHelloV1 {
            hello: self,
            request,
        })
    }
}

/// Authenticated runtime evidence needed to promote a structural Hello.
pub trait PublicHelloValidationContextV1 {
    fn exact_cursor_is_member(&self, stream: StreamHeaderV1, cursor: CursorV1) -> bool;

    fn exact_range_is_available(&self, stream: StreamHeaderV1, range: CursorRangeV1) -> bool;
}

#[derive(Clone, Copy, Debug)]
pub struct ContextValidatedPublicHelloV1<'a> {
    hello: &'a StructurallyValidatedPublicHelloV1,
    request: SubscribeV1,
}

impl<'a> ContextValidatedPublicHelloV1<'a> {
    #[must_use]
    pub const fn hello(&self) -> &'a StructurallyValidatedPublicHelloV1 {
        self.hello
    }

    #[must_use]
    pub const fn request(&self) -> SubscribeV1 {
        self.request
    }
}

pub fn validate_public_hello(
    wire: public_exit::PublicHelloWireV1,
) -> WireResult<StructurallyValidatedPublicHelloV1> {
    if wire.protocol_version != PUBLIC_PROTOCOL_VERSION_V1 {
        return Err(WireError::InvalidValue {
            field: "PublicHelloWireV1.protocol_version",
            reason: "must equal 1",
        });
    }
    let stream = StreamHeaderV1::decode(&wire.stream)?;
    PublicFeedKindV1::from_stream(stream)?;
    let live_tail = CursorV1::decode(&wire.live_tail)?;
    validate_cursor_for_stream_if_zero(live_tail, stream, "PublicHelloWireV1.live_tail")?;
    let available = validate_ranges(wire.available, Some((stream, live_tail)))?;
    Ok(StructurallyValidatedPublicHelloV1 {
        stream,
        available,
        live_tail,
    })
}

pub fn validate_public_event(
    wire: public_exit::PublicEventWireV1,
    previous: CursorV1,
) -> WireResult<RecordV1> {
    ensure_message_len(
        "PublicEventWireV1",
        wire.encoded_len(),
        PUBLIC_EVENT_WIRE_MAX_BYTES,
    )?;
    RecordV1::decode_after(&wire.record, previous).map_err(Into::into)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayReasonV1 {
    CursorExpired,
    StreamReplaced,
    HistoryPending,
    HistoryLost,
}

impl TryFrom<i32> for ReplayReasonV1 {
    type Error = WireError;

    fn try_from(value: i32) -> WireResult<Self> {
        match value {
            1 => Ok(Self::CursorExpired),
            2 => Ok(Self::StreamReplaced),
            3 => Ok(Self::HistoryPending),
            4 => Ok(Self::HistoryLost),
            value => Err(WireError::UnknownEnum {
                field: "ReplayUnavailableWireV1.reason",
                value,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayRecoveryV1 {
    Retry,
    CanonicalLookupOnly,
    None,
}

impl TryFrom<i32> for ReplayRecoveryV1 {
    type Error = WireError;

    fn try_from(value: i32) -> WireResult<Self> {
        match value {
            1 => Ok(Self::Retry),
            2 => Ok(Self::CanonicalLookupOnly),
            3 => Ok(Self::None),
            value => Err(WireError::UnknownEnum {
                field: "ReplayUnavailableWireV1.recovery",
                value,
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StructurallyValidatedReplayUnavailableV1 {
    feed: PublicFeedKindV1,
    reason: ReplayReasonV1,
    requested: CursorV1,
    available: Vec<CursorRangeV1>,
    successor_stream_id: Option<StreamId>,
    recovery: ReplayRecoveryV1,
}

impl StructurallyValidatedReplayUnavailableV1 {
    #[must_use]
    pub const fn feed(&self) -> PublicFeedKindV1 {
        self.feed
    }

    #[must_use]
    pub const fn reason(&self) -> ReplayReasonV1 {
        self.reason
    }

    #[must_use]
    pub const fn requested(&self) -> CursorV1 {
        self.requested
    }

    #[must_use]
    pub fn available(&self) -> &[CursorRangeV1] {
        &self.available
    }

    #[must_use]
    pub const fn successor_stream_id(&self) -> Option<StreamId> {
        self.successor_stream_id
    }

    #[must_use]
    pub const fn recovery(&self) -> ReplayRecoveryV1 {
        self.recovery
    }

    pub fn validate_registry_successor(
        &self,
        requested_stream_id: StreamId,
        discovery: ContextValidatedPublicStreamListV1<'_>,
    ) -> WireResult<()> {
        if self.reason != ReplayReasonV1::StreamReplaced {
            return Ok(());
        }
        let entry = discovery
            .registry()
            .entries()
            .iter()
            .find(|entry| entry.stream_id() == requested_stream_id)
            .ok_or(WireError::RegistryMismatch {
                reason: "replaced stream is absent from the registry",
            })?;
        if entry.successor_stream_id() != self.successor_stream_id {
            return Err(WireError::RegistryMismatch {
                reason: "replay successor is not the registry-verified successor",
            });
        }
        Ok(())
    }

    pub fn validate_context<'replay>(
        &'replay self,
        subscription: ContextValidatedSubscribeV1<'_>,
        context: &impl PublicReplayValidationContextV1,
    ) -> WireResult<ContextValidatedReplayUnavailableV1<'replay>> {
        let requested_stream = subscription.stream();
        if subscription.feed() != self.feed {
            return Err(WireError::ReplayMatrix {
                reason: "replay feed differs from the requested stream format",
            });
        }
        let discovery_manifest = subscription
            .discovery
            .manifest(requested_stream.stream_id())
            .ok_or(WireError::RegistryMismatch {
                reason: "requested replay stream is absent from authoritative discovery",
            })?;
        if discovery_manifest.stream() != requested_stream
            || discovery_manifest != subscription.manifest()
        {
            return Err(WireError::RegistryMismatch {
                reason: "requested replay stream header differs from the exact discovery manifest",
            });
        }
        validate_cursor_for_stream_if_zero(
            self.requested,
            requested_stream,
            "ReplayUnavailableWireV1.requested",
        )?;
        if !context.exact_cursor_is_member(requested_stream, self.requested) {
            return Err(WireError::ReplayMatrix {
                reason: "runtime cursor index does not verify the requested replay prefix",
            });
        }
        for range in &self.available {
            if !context.exact_cursor_is_member(requested_stream, range.start)
                || !context.exact_cursor_is_member(requested_stream, range.end)
                || !context.exact_range_is_available(requested_stream, *range)
            {
                return Err(WireError::ReplayMatrix {
                    reason: "runtime replay snapshot does not verify an advertised range",
                });
            }
        }
        self.validate_registry_successor(requested_stream.stream_id(), subscription.discovery)?;
        if !context.replay_decision_is_current(requested_stream, self) {
            return Err(WireError::ReplayMatrix {
                reason: "runtime status does not verify this concrete replay decision",
            });
        }
        Ok(ContextValidatedReplayUnavailableV1 { replay: self })
    }
}

/// Authenticated status/index evidence needed to promote a structural replay
/// response. Implementations own missing-cursor precedence and freshness.
pub trait PublicReplayValidationContextV1 {
    fn exact_cursor_is_member(&self, stream: StreamHeaderV1, cursor: CursorV1) -> bool;

    fn exact_range_is_available(&self, stream: StreamHeaderV1, range: CursorRangeV1) -> bool;

    fn replay_decision_is_current(
        &self,
        stream: StreamHeaderV1,
        replay: &StructurallyValidatedReplayUnavailableV1,
    ) -> bool;
}

#[derive(Clone, Copy, Debug)]
pub struct ContextValidatedReplayUnavailableV1<'a> {
    replay: &'a StructurallyValidatedReplayUnavailableV1,
}

impl<'a> ContextValidatedReplayUnavailableV1<'a> {
    #[must_use]
    pub const fn replay(&self) -> &'a StructurallyValidatedReplayUnavailableV1 {
        self.replay
    }
}

pub fn validate_replay_unavailable(
    wire: public_exit::ReplayUnavailableWireV1,
    feed: PublicFeedKindV1,
) -> WireResult<StructurallyValidatedReplayUnavailableV1> {
    let reason = ReplayReasonV1::try_from(wire.reason)?;
    let recovery = ReplayRecoveryV1::try_from(wire.recovery)?;
    let successor_stream_id = if wire.successor_stream_id.is_empty() {
        None
    } else {
        Some(fixed_id::<16, StreamId>(
            &wire.successor_stream_id,
            "ReplayUnavailableWireV1.successor_stream_id",
        )?)
    };
    let successor_required = reason == ReplayReasonV1::StreamReplaced;
    if successor_stream_id.is_some() != successor_required {
        return Err(WireError::ReplayMatrix {
            reason: "successor is present if and only if reason is STREAM_REPLACED",
        });
    }
    let valid = matches!(
        (feed, reason, recovery),
        (
            PublicFeedKindV1::RawShred,
            ReplayReasonV1::HistoryPending,
            ReplayRecoveryV1::Retry
        ) | (
            PublicFeedKindV1::RawShred,
            ReplayReasonV1::HistoryLost,
            ReplayRecoveryV1::None
        ) | (
            PublicFeedKindV1::RawShred,
            ReplayReasonV1::StreamReplaced,
            ReplayRecoveryV1::None
        ) | (
            PublicFeedKindV1::ShredBlockObservation,
            ReplayReasonV1::CursorExpired,
            ReplayRecoveryV1::CanonicalLookupOnly
        ) | (
            PublicFeedKindV1::ShredBlockObservation,
            ReplayReasonV1::StreamReplaced,
            ReplayRecoveryV1::None
        )
    );
    if !valid {
        return Err(WireError::ReplayMatrix {
            reason: "reason/recovery combination is not valid for this feed",
        });
    }
    Ok(StructurallyValidatedReplayUnavailableV1 {
        feed,
        reason,
        requested: CursorV1::decode(&wire.requested)?,
        available: validate_ranges(wire.available, None)?,
        successor_stream_id,
        recovery,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicErrorCodeV1 {
    UnknownStream,
    CursorMismatch,
    SlowSubscriber,
    Limit,
    Unavailable,
}

impl TryFrom<i32> for PublicErrorCodeV1 {
    type Error = WireError;

    fn try_from(value: i32) -> WireResult<Self> {
        match value {
            1 => Ok(Self::UnknownStream),
            2 => Ok(Self::CursorMismatch),
            3 => Ok(Self::SlowSubscriber),
            4 => Ok(Self::Limit),
            5 => Ok(Self::Unavailable),
            value => Err(WireError::UnknownEnum {
                field: "PublicErrorWireV1.code",
                value,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicErrorV1 {
    code: PublicErrorCodeV1,
}

impl PublicErrorV1 {
    #[must_use]
    pub const fn code(self) -> PublicErrorCodeV1 {
        self.code
    }
}

pub fn validate_public_error(wire: public_exit::PublicErrorWireV1) -> WireResult<PublicErrorV1> {
    Ok(PublicErrorV1 {
        code: PublicErrorCodeV1::try_from(wire.code)?,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StructurallyValidatedPublicServerFrameV1 {
    Hello(StructurallyValidatedPublicHelloV1),
    Event(RecordV1),
    ReplayUnavailable(StructurallyValidatedReplayUnavailableV1),
    Error(PublicErrorV1),
}

pub fn decode_public_server_frame(
    encoded: &[u8],
    feed: PublicFeedKindV1,
    event_previous: CursorV1,
) -> WireResult<StructurallyValidatedPublicServerFrameV1> {
    ensure_message_len(
        "PublicServerFrameV1",
        encoded.len(),
        PUBLIC_SERVER_EVENT_FRAME_MAX_BYTES,
    )?;
    preflight_public_server_frame(encoded)?;
    let wire = decode_bounded::<public_exit::PublicServerFrameV1>(
        encoded,
        PUBLIC_SERVER_EVENT_FRAME_MAX_BYTES,
        "PublicServerFrameV1",
    )?;
    validate_public_server_frame(wire, encoded.len(), feed, event_previous)
}

pub(crate) fn validate_public_server_frame(
    wire: public_exit::PublicServerFrameV1,
    protobuf_len: usize,
    feed: PublicFeedKindV1,
    event_previous: CursorV1,
) -> WireResult<StructurallyValidatedPublicServerFrameV1> {
    ensure_reported_len("PublicServerFrameV1", protobuf_len, wire.encoded_len())?;
    let frame = wire.frame.ok_or(WireError::MissingOneof {
        context: "PublicServerFrameV1.frame",
    })?;
    match frame {
        public_exit::public_server_frame_v1::Frame::Hello(hello) => {
            ensure_message_len(
                "PublicServerFrameV1.control",
                protobuf_len,
                PUBLIC_CONTROL_FRAME_MAX_BYTES,
            )?;
            let hello = validate_public_hello(hello)?;
            if PublicFeedKindV1::from_stream(hello.stream)? != feed {
                return Err(WireError::InvalidValue {
                    field: "PublicHelloWireV1.stream",
                    reason: "stream feed kind differs from subscription context",
                });
            }
            Ok(StructurallyValidatedPublicServerFrameV1::Hello(hello))
        }
        public_exit::public_server_frame_v1::Frame::Event(event) => {
            ensure_message_len(
                "PublicServerFrameV1.event",
                protobuf_len,
                PUBLIC_SERVER_EVENT_FRAME_MAX_BYTES,
            )?;
            validate_public_event(event, event_previous)
                .map(StructurallyValidatedPublicServerFrameV1::Event)
        }
        public_exit::public_server_frame_v1::Frame::ReplayUnavailable(replay) => {
            ensure_message_len(
                "PublicServerFrameV1.control",
                protobuf_len,
                PUBLIC_CONTROL_FRAME_MAX_BYTES,
            )?;
            validate_replay_unavailable(replay, feed)
                .map(StructurallyValidatedPublicServerFrameV1::ReplayUnavailable)
        }
        public_exit::public_server_frame_v1::Frame::Error(error) => {
            ensure_message_len(
                "PublicServerFrameV1.control",
                protobuf_len,
                PUBLIC_CONTROL_FRAME_MAX_BYTES,
            )?;
            validate_public_error(error).map(StructurallyValidatedPublicServerFrameV1::Error)
        }
    }
}

fn validate_ranges(
    wires: Vec<public_exit::CursorRangeWireV1>,
    stream_and_tail: Option<(StreamHeaderV1, CursorV1)>,
) -> WireResult<Vec<CursorRangeV1>> {
    if wires.len() > PUBLIC_AVAILABLE_RANGES_MAX {
        return Err(WireError::TooManyItems {
            field: "available",
            actual: wires.len(),
            max: PUBLIC_AVAILABLE_RANGES_MAX,
        });
    }
    let mut ranges = Vec::with_capacity(wires.len());
    for wire in wires {
        let range = validate_cursor_range(wire)?;
        if let Some((stream, tail)) = stream_and_tail {
            validate_cursor_for_stream_if_zero(range.start, stream, "CursorRangeWireV1.start")?;
            validate_cursor_for_stream_if_zero(range.end, stream, "CursorRangeWireV1.end")?;
            if range.end.next_sequence() > tail.next_sequence() {
                return Err(WireError::InvalidValue {
                    field: "PublicHelloWireV1.available",
                    reason: "availability range ends after live_tail",
                });
            }
            if range.end.next_sequence() == tail.next_sequence() && range.end != tail {
                return Err(WireError::InvalidValue {
                    field: "PublicHelloWireV1.available",
                    reason: "range ending at live_tail has a different prefix",
                });
            }
        }
        ranges.push(range);
    }
    if ranges
        .windows(2)
        .any(|pair| pair[0].end.next_sequence() >= pair[1].start.next_sequence())
    {
        return Err(WireError::NonCanonicalOrder { field: "available" });
    }
    Ok(ranges)
}

fn validate_cursor_for_stream_if_zero(
    cursor: CursorV1,
    stream: StreamHeaderV1,
    field: &'static str,
) -> WireResult<()> {
    if cursor.next_sequence() == 0 && cursor != stream.initial_cursor() {
        return Err(WireError::InvalidValue {
            field,
            reason: "sequence-zero cursor is not this stream's P(0)",
        });
    }
    Ok(())
}

fn decode_bounded<M>(encoded: &[u8], max: usize, context: &'static str) -> WireResult<M>
where
    M: Message + Default,
{
    ensure_message_len(context, encoded.len(), max)?;
    M::decode(encoded).map_err(Into::into)
}

fn ensure_message_len(context: &'static str, actual: usize, max: usize) -> WireResult<()> {
    if actual > max {
        return Err(WireError::MessageTooLarge {
            context,
            actual,
            max,
        });
    }
    Ok(())
}

fn ensure_reported_len(
    context: &'static str,
    reported: usize,
    minimum_known: usize,
) -> WireResult<()> {
    if reported < minimum_known {
        return Err(WireError::InvalidValue {
            field: context,
            reason: "reported protobuf length is smaller than known encoded fields",
        });
    }
    Ok(())
}

fn fixed_array<const N: usize>(bytes: &[u8], field: &'static str) -> WireResult<[u8; N]> {
    bytes.try_into().map_err(|_| WireError::InvalidLength {
        field,
        expected: N,
        actual: bytes.len(),
    })
}

fn fixed_id<const N: usize, T>(bytes: &[u8], field: &'static str) -> WireResult<T>
where
    T: From<[u8; N]>,
{
    fixed_array(bytes, field).map(Into::into)
}

fn preflight_public_server_frame(encoded: &[u8]) -> WireResult<()> {
    let mut range_occurrences = 0usize;
    let mut control_bytes = 0usize;
    let mut final_known_field = None;
    visit_protobuf_fields(
        encoded,
        "PublicServerFrameV1",
        &mut |field, wire, bytes, occurrence_len| {
            if !(1..=4).contains(&field) {
                return Ok(());
            }
            if wire != 2 {
                return Err(WireError::MalformedProtobuf {
                    context: "PublicServerFrameV1",
                    reason: "message oneof field has the wrong wire type",
                });
            }
            let replaces_active_oneof = final_known_field != Some(field);
            if replaces_active_oneof {
                control_bytes = 0;
            }
            if field == 1 || field == 3 || field == 4 {
                control_bytes = control_bytes.checked_add(occurrence_len).ok_or(
                    WireError::MessageTooLarge {
                        context: "PublicServerFrameV1.control occurrences",
                        actual: usize::MAX,
                        max: PUBLIC_CONTROL_FRAME_MAX_BYTES,
                    },
                )?;
                ensure_message_len(
                    "PublicServerFrameV1.control occurrences",
                    control_bytes,
                    PUBLIC_CONTROL_FRAME_MAX_BYTES,
                )?;
            } else {
                control_bytes = 0;
            }
            if field == 1 || field == 3 {
                let nested = bytes.ok_or(WireError::MalformedProtobuf {
                    context: "PublicServerFrameV1",
                    reason: "message oneof field has no length-delimited body",
                })?;
                let nested_occurrences =
                    count_repeated_field_occurrences(nested, 3, "PublicServerFrameV1.available")?;
                range_occurrences = if replaces_active_oneof {
                    nested_occurrences
                } else {
                    range_occurrences.checked_add(nested_occurrences).ok_or(
                        WireError::TooManyItems {
                            field: "available",
                            actual: usize::MAX,
                            max: PUBLIC_AVAILABLE_RANGES_MAX,
                        },
                    )?
                };
                if range_occurrences > PUBLIC_AVAILABLE_RANGES_MAX {
                    return Err(WireError::TooManyItems {
                        field: "available",
                        actual: range_occurrences,
                        max: PUBLIC_AVAILABLE_RANGES_MAX,
                    });
                }
            } else {
                range_occurrences = 0;
            }
            final_known_field = Some(field);
            Ok(())
        },
    )?;
    if matches!(final_known_field, Some(1 | 3 | 4)) {
        ensure_message_len(
            "PublicServerFrameV1.control",
            encoded.len(),
            PUBLIC_CONTROL_FRAME_MAX_BYTES,
        )?;
    }
    Ok(())
}

fn preflight_sync_server_frame(encoded: &[u8]) -> WireResult<()> {
    let mut control_bytes = 0usize;
    let mut final_known_field = None;
    visit_protobuf_fields(
        encoded,
        "SyncServerFrameV1",
        &mut |field, wire, _bytes, occurrence_len| {
            if !(1..=3).contains(&field) {
                return Ok(());
            }
            if wire != 2 {
                return Err(WireError::MalformedProtobuf {
                    context: "SyncServerFrameV1",
                    reason: "message oneof field has the wrong wire type",
                });
            }
            let replaces_active_oneof = final_known_field != Some(field);
            if replaces_active_oneof {
                control_bytes = 0;
            }
            if field == 1 || field == 3 {
                control_bytes = control_bytes.checked_add(occurrence_len).ok_or(
                    WireError::MessageTooLarge {
                        context: "SyncServerFrameV1.control occurrences",
                        actual: usize::MAX,
                        max: SYNC_CONTROL_PROTOBUF_MAX_BYTES,
                    },
                )?;
                ensure_message_len(
                    "SyncServerFrameV1.control occurrences",
                    control_bytes,
                    SYNC_CONTROL_PROTOBUF_MAX_BYTES,
                )?;
            } else {
                control_bytes = 0;
            }
            final_known_field = Some(field);
            Ok(())
        },
    )?;
    if matches!(final_known_field, Some(1 | 3)) {
        ensure_message_len(
            "SyncServerFrameV1.control",
            encoded.len(),
            SYNC_CONTROL_PROTOBUF_MAX_BYTES,
        )?;
    }
    Ok(())
}

fn preflight_fetch_range_part(encoded: &[u8]) -> WireResult<()> {
    let mut control_bytes = 0usize;
    let mut final_known_field = None;
    visit_protobuf_fields(
        encoded,
        "FetchRangePartWireV1",
        &mut |field, wire, _bytes, occurrence_len| {
            if !(1..=3).contains(&field) {
                return Ok(());
            }
            if wire != 2 {
                return Err(WireError::MalformedProtobuf {
                    context: "FetchRangePartWireV1",
                    reason: "part oneof field has the wrong wire type",
                });
            }
            if final_known_field != Some(field) {
                control_bytes = 0;
            }
            if field == 2 || field == 3 {
                control_bytes = control_bytes.checked_add(occurrence_len).ok_or(
                    WireError::MessageTooLarge {
                        context: "FetchRangePartWireV1.control occurrences",
                        actual: usize::MAX,
                        max: SYNC_CONTROL_PROTOBUF_MAX_BYTES,
                    },
                )?;
                ensure_message_len(
                    "FetchRangePartWireV1.control occurrences",
                    control_bytes,
                    SYNC_CONTROL_PROTOBUF_MAX_BYTES,
                )?;
            } else {
                control_bytes = 0;
            }
            final_known_field = Some(field);
            Ok(())
        },
    )?;
    if matches!(final_known_field, Some(2 | 3)) {
        ensure_message_len(
            "FetchRangePartWireV1.control",
            encoded.len(),
            SYNC_CONTROL_PROTOBUF_MAX_BYTES,
        )?;
    }
    Ok(())
}

fn enforce_repeated_field_limit(
    encoded: &[u8],
    field_number: u32,
    max: usize,
    field: &'static str,
) -> WireResult<()> {
    let actual = count_repeated_field_occurrences(encoded, field_number, field)?;
    if actual > max {
        return Err(WireError::TooManyItems { field, actual, max });
    }
    Ok(())
}

fn count_repeated_field_occurrences(
    encoded: &[u8],
    field_number: u32,
    context: &'static str,
) -> WireResult<usize> {
    let mut count = 0usize;
    visit_protobuf_fields(encoded, context, &mut |field, wire, _, _occurrence_len| {
        if field == field_number {
            if wire != 2 {
                return Err(WireError::MalformedProtobuf {
                    context,
                    reason: "bounded repeated message/bytes field has the wrong wire type",
                });
            }
            count = count.checked_add(1).ok_or(WireError::TooManyItems {
                field: context,
                actual: usize::MAX,
                max: usize::MAX - 1,
            })?;
        }
        Ok(())
    })?;
    Ok(count)
}

fn visit_protobuf_fields<'a>(
    encoded: &'a [u8],
    context: &'static str,
    visitor: &mut impl FnMut(u32, u8, Option<&'a [u8]>, usize) -> WireResult<()>,
) -> WireResult<()> {
    let mut offset = 0usize;
    while offset < encoded.len() {
        let occurrence_start = offset;
        let key = protobuf_varint(encoded, &mut offset, context)?;
        let field = u32::try_from(key >> 3).map_err(|_| WireError::MalformedProtobuf {
            context,
            reason: "field number does not fit u32",
        })?;
        if field == 0 || field > 0x1fff_ffff {
            return Err(WireError::MalformedProtobuf {
                context,
                reason: "protobuf field number is outside 1..=2^29-1",
            });
        }
        let wire = (key & 7) as u8;
        let bytes = skip_protobuf_value(encoded, &mut offset, field, wire, context, 0)?;
        visitor(field, wire, bytes, offset - occurrence_start)?;
    }
    Ok(())
}

fn skip_protobuf_value<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    field: u32,
    wire: u8,
    context: &'static str,
    group_depth: u8,
) -> WireResult<Option<&'a [u8]>> {
    match wire {
        0 => {
            protobuf_varint(encoded, offset, context)?;
            Ok(None)
        }
        1 => {
            protobuf_take(encoded, offset, 8, context)?;
            Ok(None)
        }
        2 => {
            let length = protobuf_varint(encoded, offset, context)?;
            let length = usize::try_from(length).map_err(|_| WireError::MalformedProtobuf {
                context,
                reason: "length-delimited field length does not fit usize",
            })?;
            protobuf_take(encoded, offset, length, context).map(Some)
        }
        3 => {
            if group_depth >= 100 {
                return Err(WireError::MalformedProtobuf {
                    context,
                    reason: "protobuf group nesting exceeds preflight limit",
                });
            }
            skip_protobuf_group(encoded, offset, field, context, group_depth + 1)?;
            Ok(None)
        }
        4 => Err(WireError::MalformedProtobuf {
            context,
            reason: "unexpected end-group tag",
        }),
        5 => {
            protobuf_take(encoded, offset, 4, context)?;
            Ok(None)
        }
        _ => Err(WireError::MalformedProtobuf {
            context,
            reason: "unknown protobuf wire type",
        }),
    }
}

fn skip_protobuf_group(
    encoded: &[u8],
    offset: &mut usize,
    opening_field: u32,
    context: &'static str,
    group_depth: u8,
) -> WireResult<()> {
    while *offset < encoded.len() {
        let key = protobuf_varint(encoded, offset, context)?;
        let field = u32::try_from(key >> 3).map_err(|_| WireError::MalformedProtobuf {
            context,
            reason: "group field number does not fit u32",
        })?;
        if field == 0 || field > 0x1fff_ffff {
            return Err(WireError::MalformedProtobuf {
                context,
                reason: "protobuf field number is outside 1..=2^29-1",
            });
        }
        let wire = (key & 7) as u8;
        if wire == 4 {
            if field != opening_field {
                return Err(WireError::MalformedProtobuf {
                    context,
                    reason: "end-group tag does not match its opening field",
                });
            }
            return Ok(());
        }
        skip_protobuf_value(encoded, offset, field, wire, context, group_depth)?;
    }
    Err(WireError::MalformedProtobuf {
        context,
        reason: "unterminated protobuf group",
    })
}

fn protobuf_varint(encoded: &[u8], offset: &mut usize, context: &'static str) -> WireResult<u64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = *encoded.get(*offset).ok_or(WireError::MalformedProtobuf {
            context,
            reason: "truncated varint",
        })?;
        *offset += 1;
        if shift == 63 && byte > 1 {
            return Err(WireError::MalformedProtobuf {
                context,
                reason: "varint exceeds u64",
            });
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(WireError::MalformedProtobuf {
        context,
        reason: "varint exceeds ten bytes",
    })
}

fn protobuf_take<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    length: usize,
    context: &'static str,
) -> WireResult<&'a [u8]> {
    let end = offset
        .checked_add(length)
        .ok_or(WireError::MalformedProtobuf {
            context,
            reason: "length-delimited field overflows address space",
        })?;
    let bytes = encoded
        .get(*offset..end)
        .ok_or(WireError::MalformedProtobuf {
            context,
            reason: "truncated fixed or length-delimited field",
        })?;
    *offset = end;
    Ok(bytes)
}

#[cfg(test)]
#[path = "v1_wire_tests.rs"]
mod tests;
