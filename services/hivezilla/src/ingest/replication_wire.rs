//! Generated gRPC contract and strict conversions for raw stream replication.
//!
//! This module deliberately contains no sockets, TLS policy, authentication, retry loop, or
//! durable receiver. It only defines the wire boundary and rejects protobuf defaults or malformed
//! fixed-width fields before they can enter the replication state machine. Prost allocates a
//! stream message before these conversions run, so the eventual service must acquire admission
//! before polling each message and set Tonic's per-message decoding limit to a checked record
//! envelope bound before accepting traffic.

use std::{error::Error, fmt};

use super::{
    dedup::{ContentDigest, LogicalKey, ObservationId, ShredKind, compute_content_digest},
    receiver::{RawReceiverLimits, RawReplicationRecord},
    replication::{
        CommitmentEvidence, CumulativePrimaryAck, REPLICATION_PROTOCOL_VERSION, ReceiptDisposition,
        ReplicationOffer, ReplicationStreamId,
    },
};

/// Generated protobuf messages and gRPC client/server traits.
pub mod wire {
    tonic::include_proto!("blockzilla.ingest.replication.v1");
}

const JOURNAL_ID_BYTES: usize = 16;
const DIGEST_BYTES: usize = 32;
const ED25519_SIGNATURE_BYTES: usize = 64;
const MAX_ID_BYTES: usize = 64;

/// Validated offset-free request for the server-selected next durable replication batch.
///
/// This is intentionally a zero-sized capability. The protobuf request contains only the protocol
/// version, so callers cannot smuggle a sequence, slot, stream, or batch-size choice into the pull
/// state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ValidatedPullBatchRequest;

/// Validated correlation response after the server has accepted a submitted cumulative ACK.
///
/// This value is not cursor or deletion authority. Only the signed ACK after crossing the server's
/// local cumulative-ACK WAL fsync boundary can authorize retention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedCommitAckResponse {
    stream: ReplicationStreamId,
    through_sequence: u64,
}

impl ValidatedCommitAckResponse {
    pub fn new(
        stream: ReplicationStreamId,
        through_sequence: u64,
    ) -> Result<Self, ReplicationWireError> {
        validate_stream(&stream)?;
        Ok(Self {
            stream,
            through_sequence,
        })
    }

    pub fn stream(&self) -> &ReplicationStreamId {
        &self.stream
    }

    pub fn through_sequence(&self) -> u64 {
        self.through_sequence
    }
}

/// A non-empty batch belonging to exactly one replication stream.
///
/// Construct this only after the streaming handler has enforced its cumulative record and byte
/// limits incrementally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPushBatch {
    stream: ReplicationStreamId,
    records: Vec<RawReplicationRecord>,
}

impl ValidatedPushBatch {
    pub fn stream(&self) -> &ReplicationStreamId {
        &self.stream
    }

    pub fn records(&self) -> &[RawReplicationRecord] {
        &self.records
    }

    pub fn into_parts(self) -> (ReplicationStreamId, Vec<RawReplicationRecord>) {
        (self.stream, self.records)
    }
}

/// One client-streaming PushBatch message after its nested offer has been bound to the declared
/// request stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPushRecord {
    stream: ReplicationStreamId,
    record: RawReplicationRecord,
}

impl ValidatedPushRecord {
    pub fn new(
        stream: ReplicationStreamId,
        record: RawReplicationRecord,
    ) -> Result<Self, ReplicationWireError> {
        validate_stream(&stream)?;
        validate_raw_record(&record)?;
        Self::bind_validated(stream, record)
    }

    fn bind_validated(
        stream: ReplicationStreamId,
        record: RawReplicationRecord,
    ) -> Result<Self, ReplicationWireError> {
        if ReplicationStreamId::from_offer(&record.offer) != stream {
            return Err(ReplicationWireError::WrongRecordStream);
        }
        Ok(Self { stream, record })
    }

    pub fn stream(&self) -> &ReplicationStreamId {
        &self.stream
    }

    pub fn record(&self) -> &RawReplicationRecord {
        &self.record
    }

    pub fn into_record(self) -> RawReplicationRecord {
        self.record
    }
}

/// Incremental client-streaming batch admission.
///
/// The handler must still enforce its idle/total deadlines and acquire its global concurrency
/// permit before polling Tonic's stream. This builder guarantees that no record is retained until
/// its per-record and cumulative budgets have passed.
#[derive(Debug)]
pub struct BoundedPushBatchBuilder {
    limits: RawReceiverLimits,
    stream: Option<ReplicationStreamId>,
    records: Vec<RawReplicationRecord>,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
}

/// Result of polling a stream which may not have established a durable prefix yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetAckState {
    NoDurablePrefix,
    DurablePrefix(CumulativePrimaryAck),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationWireError {
    MissingField {
        field: &'static str,
    },
    InvalidLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    UnknownEnum {
        field: &'static str,
        value: i32,
    },
    IntegerOutOfRange {
        field: &'static str,
        value: u64,
    },
    UnsupportedProtocol {
        value: u32,
    },
    InvalidValue {
        field: &'static str,
    },
    EmptyBatch,
    WrongRecordStream,
    WrongBatchStream {
        record_index: usize,
    },
    PayloadLengthMismatch {
        declared: u64,
        actual: u64,
    },
    ContentDigestMismatch,
    LimitExceeded {
        limit: &'static str,
        maximum: u64,
        attempted: u64,
    },
}

impl fmt::Display for ReplicationWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { field } => write!(formatter, "missing required field {field}"),
            Self::InvalidLength {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "field {field} must be exactly {expected} bytes, found {actual}"
            ),
            Self::UnknownEnum { field, value } => {
                write!(formatter, "field {field} has unknown enum value {value}")
            }
            Self::IntegerOutOfRange { field, value } => {
                write!(formatter, "field {field} is out of range: {value}")
            }
            Self::UnsupportedProtocol { value } => {
                write!(
                    formatter,
                    "unsupported replication protocol version {value}"
                )
            }
            Self::InvalidValue { field } => write!(formatter, "field {field} is invalid"),
            Self::EmptyBatch => write!(formatter, "PushBatch requires at least one record"),
            Self::WrongRecordStream => write!(
                formatter,
                "PushBatch record does not belong to its declared request stream"
            ),
            Self::WrongBatchStream { record_index } => write!(
                formatter,
                "PushBatch record {record_index} declares a different stream from the first record"
            ),
            Self::PayloadLengthMismatch { declared, actual } => write!(
                formatter,
                "compressed payload length mismatch: offer declares {declared}, found {actual}"
            ),
            Self::ContentDigestMismatch => {
                write!(
                    formatter,
                    "compressed payload content digest does not match offer"
                )
            }
            Self::LimitExceeded {
                limit,
                maximum,
                attempted,
            } => write!(
                formatter,
                "PushBatch {limit} limit exceeded: attempted {attempted}, maximum {maximum}"
            ),
        }
    }
}

impl Error for ReplicationWireError {}

impl TryFrom<wire::PullBatchRequest> for ValidatedPullBatchRequest {
    type Error = ReplicationWireError;

    fn try_from(value: wire::PullBatchRequest) -> Result<Self, Self::Error> {
        protocol_version(value.protocol_version)?;
        Ok(Self)
    }
}

impl From<ValidatedPullBatchRequest> for wire::PullBatchRequest {
    fn from(_value: ValidatedPullBatchRequest) -> Self {
        Self {
            protocol_version: u32::from(REPLICATION_PROTOCOL_VERSION),
        }
    }
}

impl TryFrom<wire::CommitAckResponse> for ValidatedCommitAckResponse {
    type Error = ReplicationWireError;

    fn try_from(value: wire::CommitAckResponse) -> Result<Self, Self::Error> {
        protocol_version(value.protocol_version)?;
        Self::new(
            required(value.stream, "commit_ack.stream")?.try_into()?,
            value.through_sequence,
        )
    }
}

impl TryFrom<&ValidatedCommitAckResponse> for wire::CommitAckResponse {
    type Error = ReplicationWireError;

    fn try_from(value: &ValidatedCommitAckResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            protocol_version: u32::from(REPLICATION_PROTOCOL_VERSION),
            stream: Some(wire::StreamId::try_from(value.stream())?),
            through_sequence: value.through_sequence(),
        })
    }
}

impl TryFrom<wire::StreamId> for ReplicationStreamId {
    type Error = ReplicationWireError;

    fn try_from(value: wire::StreamId) -> Result<Self, Self::Error> {
        validate_id(&value.cluster_id, "stream.cluster_id")?;
        validate_id(&value.origin_node_id, "stream.origin_node_id")?;
        validate_id(&value.source_id, "stream.source_id")?;
        Ok(Self {
            cluster_id: value.cluster_id,
            origin_node_id: value.origin_node_id,
            source_id: value.source_id,
            journal_id: fixed_bytes::<JOURNAL_ID_BYTES>(value.journal_id, "stream.journal_id")?,
        })
    }
}

impl TryFrom<&ReplicationStreamId> for wire::StreamId {
    type Error = ReplicationWireError;

    fn try_from(value: &ReplicationStreamId) -> Result<Self, Self::Error> {
        validate_stream(value)?;
        Ok(Self {
            cluster_id: value.cluster_id.clone(),
            origin_node_id: value.origin_node_id.clone(),
            source_id: value.source_id.clone(),
            journal_id: value.journal_id.to_vec(),
        })
    }
}

impl TryFrom<wire::ReplicationOffer> for ReplicationOffer {
    type Error = ReplicationWireError;

    fn try_from(value: wire::ReplicationOffer) -> Result<Self, Self::Error> {
        let protocol_version = protocol_version(value.protocol_version)?;
        let stream: ReplicationStreamId = required(value.stream, "offer.stream")?.try_into()?;
        let logical_key = logical_key_from_wire(required(value.logical_key, "offer.logical_key")?)?;
        let payload_format_version = payload_format_version(value.payload_format_version)?;
        let commitment = commitment_from_wire(value.commitment)?;
        let offer = Self {
            protocol_version,
            cluster_id: stream.cluster_id,
            record: ObservationId {
                origin_node_id: stream.origin_node_id,
                journal_id: stream.journal_id,
                sequence: value.sequence,
            },
            source_id: stream.source_id,
            logical_key,
            content_digest: ContentDigest(fixed_bytes::<DIGEST_BYTES>(
                value.content_digest,
                "offer.content_digest",
            )?),
            payload_len: value.payload_len,
            payload_format_version,
            commitment,
        };
        validate_offer(&offer)?;
        Ok(offer)
    }
}

impl TryFrom<&ReplicationOffer> for wire::ReplicationOffer {
    type Error = ReplicationWireError;

    fn try_from(value: &ReplicationOffer) -> Result<Self, Self::Error> {
        validate_offer(value)?;
        Ok(Self {
            protocol_version: u32::from(value.protocol_version),
            stream: Some(wire::StreamId::try_from(&ReplicationStreamId::from_offer(
                value,
            ))?),
            sequence: value.record.sequence,
            logical_key: Some(logical_key_to_wire(&value.logical_key)),
            content_digest: value.content_digest.0.to_vec(),
            payload_len: value.payload_len,
            payload_format_version: u32::from(value.payload_format_version),
            commitment: commitment_to_wire(value.commitment) as i32,
        })
    }
}

impl TryFrom<wire::RawReplicationRecord> for RawReplicationRecord {
    type Error = ReplicationWireError;

    fn try_from(value: wire::RawReplicationRecord) -> Result<Self, Self::Error> {
        let record = Self {
            offer: required(value.offer, "record.offer")?.try_into()?,
            compressed_payload: value.compressed_payload,
            uncompressed_len: value.uncompressed_len,
            raw_protobuf_sha256: fixed_bytes::<DIGEST_BYTES>(
                value.raw_protobuf_sha256,
                "record.raw_protobuf_sha256",
            )?,
        };
        validate_raw_record(&record)?;
        Ok(record)
    }
}

impl TryFrom<RawReplicationRecord> for wire::RawReplicationRecord {
    type Error = ReplicationWireError;

    fn try_from(value: RawReplicationRecord) -> Result<Self, Self::Error> {
        validate_raw_record(&value)?;
        Ok(Self {
            offer: Some(wire::ReplicationOffer::try_from(&value.offer)?),
            compressed_payload: value.compressed_payload,
            uncompressed_len: value.uncompressed_len,
            raw_protobuf_sha256: value.raw_protobuf_sha256.to_vec(),
        })
    }
}

impl TryFrom<wire::PushBatchRequest> for ValidatedPushRecord {
    type Error = ReplicationWireError;

    fn try_from(value: wire::PushBatchRequest) -> Result<Self, Self::Error> {
        let stream: ReplicationStreamId =
            required(value.stream, "push_batch.stream")?.try_into()?;
        let record: RawReplicationRecord =
            required(value.record, "push_batch.record")?.try_into()?;
        Self::bind_validated(stream, record)
    }
}

impl TryFrom<ValidatedPushRecord> for wire::PushBatchRequest {
    type Error = ReplicationWireError;

    fn try_from(value: ValidatedPushRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            stream: Some(wire::StreamId::try_from(&value.stream)?),
            record: Some(wire::RawReplicationRecord::try_from(value.record)?),
        })
    }
}

impl BoundedPushBatchBuilder {
    pub fn new(limits: RawReceiverLimits) -> Result<Self, ReplicationWireError> {
        validate_builder_limits(limits)?;
        Ok(Self {
            limits,
            stream: None,
            records: Vec::new(),
            compressed_bytes: 0,
            uncompressed_bytes: 0,
        })
    }

    /// Admit one already-decoded stream message before retaining its payload.
    pub fn push(&mut self, item: ValidatedPushRecord) -> Result<(), ReplicationWireError> {
        let record_index = self.records.len();
        if self
            .stream
            .as_ref()
            .is_some_and(|stream| stream != item.stream())
        {
            return Err(ReplicationWireError::WrongBatchStream { record_index });
        }

        let attempted_records =
            record_index
                .checked_add(1)
                .ok_or(ReplicationWireError::IntegerOutOfRange {
                    field: "push_batch.records.len",
                    value: u64::MAX,
                })?;
        if attempted_records > self.limits.max_batch_records {
            let maximum = u64::try_from(self.limits.max_batch_records).unwrap_or(u64::MAX);
            let attempted = u64::try_from(attempted_records).map_err(|_| {
                ReplicationWireError::IntegerOutOfRange {
                    field: "push_batch.records.len",
                    value: u64::MAX,
                }
            })?;
            return Err(limit_exceeded("record count", maximum, attempted));
        }

        let compressed = item.record().offer.payload_len;
        if compressed > self.limits.max_compressed_record_bytes {
            return Err(limit_exceeded(
                "compressed bytes per record",
                self.limits.max_compressed_record_bytes,
                compressed,
            ));
        }
        let uncompressed = item.record().uncompressed_len;
        if uncompressed > self.limits.max_uncompressed_record_bytes {
            return Err(limit_exceeded(
                "uncompressed bytes per record",
                self.limits.max_uncompressed_record_bytes,
                uncompressed,
            ));
        }

        let compressed_total = self.compressed_bytes.checked_add(compressed).ok_or(
            ReplicationWireError::IntegerOutOfRange {
                field: "push_batch.compressed_bytes",
                value: u64::MAX,
            },
        )?;
        if compressed_total > self.limits.max_batch_compressed_bytes {
            return Err(limit_exceeded(
                "cumulative compressed bytes",
                self.limits.max_batch_compressed_bytes,
                compressed_total,
            ));
        }
        let uncompressed_total = self.uncompressed_bytes.checked_add(uncompressed).ok_or(
            ReplicationWireError::IntegerOutOfRange {
                field: "push_batch.uncompressed_bytes",
                value: u64::MAX,
            },
        )?;
        if uncompressed_total > self.limits.max_batch_uncompressed_bytes {
            return Err(limit_exceeded(
                "cumulative uncompressed bytes",
                self.limits.max_batch_uncompressed_bytes,
                uncompressed_total,
            ));
        }

        if self.stream.is_none() {
            self.stream = Some(item.stream().clone());
        }
        self.compressed_bytes = compressed_total;
        self.uncompressed_bytes = uncompressed_total;
        self.records.push(item.into_record());
        Ok(())
    }

    pub fn push_wire(
        &mut self,
        request: wire::PushBatchRequest,
    ) -> Result<(), ReplicationWireError> {
        self.push(request.try_into()?)
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn compressed_bytes(&self) -> u64 {
        self.compressed_bytes
    }

    pub fn uncompressed_bytes(&self) -> u64 {
        self.uncompressed_bytes
    }

    /// Complete the batch at clean client-stream EOF. An empty stream is invalid.
    pub fn finish(self) -> Result<ValidatedPushBatch, ReplicationWireError> {
        let stream = self.stream.ok_or(ReplicationWireError::EmptyBatch)?;
        Ok(ValidatedPushBatch {
            stream,
            records: self.records,
        })
    }
}

impl TryFrom<wire::GetAckRequest> for ReplicationStreamId {
    type Error = ReplicationWireError;

    fn try_from(value: wire::GetAckRequest) -> Result<Self, Self::Error> {
        required(value.stream, "get_ack.stream")?.try_into()
    }
}

impl TryFrom<&ReplicationStreamId> for wire::GetAckRequest {
    type Error = ReplicationWireError;

    fn try_from(value: &ReplicationStreamId) -> Result<Self, Self::Error> {
        Ok(Self {
            stream: Some(wire::StreamId::try_from(value)?),
        })
    }
}

impl TryFrom<wire::GetAckResponse> for GetAckState {
    type Error = ReplicationWireError;

    fn try_from(value: wire::GetAckResponse) -> Result<Self, Self::Error> {
        match value.ack {
            Some(ack) => Ok(Self::DurablePrefix(ack.try_into()?)),
            None => Ok(Self::NoDurablePrefix),
        }
    }
}

impl TryFrom<&GetAckState> for wire::GetAckResponse {
    type Error = ReplicationWireError;

    fn try_from(value: &GetAckState) -> Result<Self, Self::Error> {
        Ok(Self {
            ack: match value {
                GetAckState::NoDurablePrefix => None,
                GetAckState::DurablePrefix(ack) => Some(wire::CumulativePrimaryAck::try_from(ack)?),
            },
        })
    }
}

impl TryFrom<wire::CumulativePrimaryAck> for CumulativePrimaryAck {
    type Error = ReplicationWireError;

    fn try_from(value: wire::CumulativePrimaryAck) -> Result<Self, Self::Error> {
        let ack = Self {
            protocol_version: protocol_version(value.protocol_version)?,
            stream: required(value.stream, "ack.stream")?.try_into()?,
            primary_id: value.primary_id,
            primary_term: value.primary_term,
            through_sequence: value.through_sequence,
            through_content_digest: ContentDigest(fixed_bytes::<DIGEST_BYTES>(
                value.through_content_digest,
                "ack.through_content_digest",
            )?),
            rolling_chain_digest: ContentDigest(fixed_bytes::<DIGEST_BYTES>(
                value.rolling_chain_digest,
                "ack.rolling_chain_digest",
            )?),
            disposition: disposition_from_wire(value.disposition)?,
            durable_lsn: value.durable_lsn,
            signing_key_id: value.signing_key_id,
            signature: value.signature,
        };
        validate_ack(&ack)?;
        Ok(ack)
    }
}

impl TryFrom<&CumulativePrimaryAck> for wire::CumulativePrimaryAck {
    type Error = ReplicationWireError;

    fn try_from(value: &CumulativePrimaryAck) -> Result<Self, Self::Error> {
        validate_ack(value)?;
        Ok(Self {
            protocol_version: u32::from(value.protocol_version),
            stream: Some(wire::StreamId::try_from(&value.stream)?),
            primary_id: value.primary_id.clone(),
            primary_term: value.primary_term,
            through_sequence: value.through_sequence,
            through_content_digest: value.through_content_digest.0.to_vec(),
            rolling_chain_digest: value.rolling_chain_digest.0.to_vec(),
            disposition: disposition_to_wire(value.disposition) as i32,
            durable_lsn: value.durable_lsn,
            signing_key_id: value.signing_key_id.clone(),
            signature: value.signature.clone(),
        })
    }
}

fn required<T>(value: Option<T>, field: &'static str) -> Result<T, ReplicationWireError> {
    value.ok_or(ReplicationWireError::MissingField { field })
}

fn validate_builder_limits(limits: RawReceiverLimits) -> Result<(), ReplicationWireError> {
    if limits.max_batch_records == 0 {
        return Err(ReplicationWireError::InvalidValue {
            field: "limits.max_batch_records",
        });
    }
    for (value, field) in [
        (
            limits.max_batch_compressed_bytes,
            "limits.max_batch_compressed_bytes",
        ),
        (
            limits.max_batch_uncompressed_bytes,
            "limits.max_batch_uncompressed_bytes",
        ),
        (
            limits.max_compressed_record_bytes,
            "limits.max_compressed_record_bytes",
        ),
        (
            limits.max_uncompressed_record_bytes,
            "limits.max_uncompressed_record_bytes",
        ),
    ] {
        if value == 0 {
            return Err(ReplicationWireError::InvalidValue { field });
        }
    }
    if limits.max_compressed_record_bytes > limits.max_batch_compressed_bytes {
        return Err(ReplicationWireError::InvalidValue {
            field: "limits.max_compressed_record_bytes",
        });
    }
    if limits.max_uncompressed_record_bytes > limits.max_batch_uncompressed_bytes {
        return Err(ReplicationWireError::InvalidValue {
            field: "limits.max_uncompressed_record_bytes",
        });
    }
    Ok(())
}

fn limit_exceeded(limit: &'static str, maximum: u64, attempted: u64) -> ReplicationWireError {
    ReplicationWireError::LimitExceeded {
        limit,
        maximum,
        attempted,
    }
}

fn fixed_bytes<const N: usize>(
    value: Vec<u8>,
    field: &'static str,
) -> Result<[u8; N], ReplicationWireError> {
    let actual = value.len();
    value
        .try_into()
        .map_err(|_| ReplicationWireError::InvalidLength {
            field,
            expected: N,
            actual,
        })
}

fn validate_id(value: &str, field: &'static str) -> Result<(), ReplicationWireError> {
    let mut characters = value.chars();
    let valid_first = characters
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric());
    let valid_rest = characters
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'));
    if value.len() > MAX_ID_BYTES || !valid_first || !valid_rest {
        Err(ReplicationWireError::InvalidValue { field })
    } else {
        Ok(())
    }
}

fn validate_stream(stream: &ReplicationStreamId) -> Result<(), ReplicationWireError> {
    validate_id(&stream.cluster_id, "stream.cluster_id")?;
    validate_id(&stream.origin_node_id, "stream.origin_node_id")?;
    validate_id(&stream.source_id, "stream.source_id")?;
    Ok(())
}

fn protocol_version(value: u32) -> Result<u16, ReplicationWireError> {
    let narrowed = u16::try_from(value).map_err(|_| ReplicationWireError::IntegerOutOfRange {
        field: "protocol_version",
        value: u64::from(value),
    })?;
    if narrowed != REPLICATION_PROTOCOL_VERSION {
        return Err(ReplicationWireError::UnsupportedProtocol { value });
    }
    Ok(narrowed)
}

fn payload_format_version(value: u32) -> Result<u16, ReplicationWireError> {
    let narrowed = u16::try_from(value).map_err(|_| ReplicationWireError::IntegerOutOfRange {
        field: "offer.payload_format_version",
        value: u64::from(value),
    })?;
    if narrowed == 0 {
        Err(ReplicationWireError::InvalidValue {
            field: "offer.payload_format_version",
        })
    } else {
        Ok(narrowed)
    }
}

fn validate_offer(offer: &ReplicationOffer) -> Result<(), ReplicationWireError> {
    if offer.protocol_version != REPLICATION_PROTOCOL_VERSION {
        return Err(ReplicationWireError::UnsupportedProtocol {
            value: u32::from(offer.protocol_version),
        });
    }
    validate_stream(&ReplicationStreamId::from_offer(offer))?;
    if offer.payload_format_version == 0 {
        return Err(ReplicationWireError::InvalidValue {
            field: "offer.payload_format_version",
        });
    }
    if offer.payload_len == 0 {
        return Err(ReplicationWireError::InvalidValue {
            field: "offer.payload_len",
        });
    }
    Ok(())
}

fn validate_raw_record(record: &RawReplicationRecord) -> Result<(), ReplicationWireError> {
    validate_offer(&record.offer)?;
    if record.compressed_payload.is_empty() {
        return Err(ReplicationWireError::InvalidValue {
            field: "record.compressed_payload",
        });
    }
    if record.uncompressed_len == 0 {
        return Err(ReplicationWireError::InvalidValue {
            field: "record.uncompressed_len",
        });
    }
    let actual = u64::try_from(record.compressed_payload.len()).map_err(|_| {
        ReplicationWireError::IntegerOutOfRange {
            field: "record.compressed_payload.len",
            value: u64::MAX,
        }
    })?;
    if record.offer.payload_len != actual {
        return Err(ReplicationWireError::PayloadLengthMismatch {
            declared: record.offer.payload_len,
            actual,
        });
    }
    let digest = compute_content_digest(
        &record.offer.cluster_id,
        &record.offer.logical_key,
        record.offer.payload_format_version,
        &record.compressed_payload,
    );
    if digest != record.offer.content_digest {
        return Err(ReplicationWireError::ContentDigestMismatch);
    }
    Ok(())
}

fn validate_ack(ack: &CumulativePrimaryAck) -> Result<(), ReplicationWireError> {
    if ack.protocol_version != REPLICATION_PROTOCOL_VERSION {
        return Err(ReplicationWireError::UnsupportedProtocol {
            value: u32::from(ack.protocol_version),
        });
    }
    validate_stream(&ack.stream)?;
    validate_id(&ack.primary_id, "ack.primary_id")?;
    validate_id(&ack.signing_key_id, "ack.signing_key_id")?;
    if ack.primary_term == 0 {
        return Err(ReplicationWireError::InvalidValue {
            field: "ack.primary_term",
        });
    }
    if ack.durable_lsn == 0 {
        return Err(ReplicationWireError::InvalidValue {
            field: "ack.durable_lsn",
        });
    }
    if ack.signature.len() != ED25519_SIGNATURE_BYTES {
        return Err(ReplicationWireError::InvalidLength {
            field: "ack.signature",
            expected: ED25519_SIGNATURE_BYTES,
            actual: ack.signature.len(),
        });
    }
    Ok(())
}

fn logical_key_from_wire(value: wire::LogicalKey) -> Result<LogicalKey, ReplicationWireError> {
    match required(value.kind, "offer.logical_key.kind")? {
        wire::logical_key::Kind::Block(block) => Ok(LogicalKey::Block {
            slot: block.slot,
            blockhash: fixed_bytes::<DIGEST_BYTES>(
                block.blockhash,
                "offer.logical_key.block.blockhash",
            )?,
        }),
        wire::logical_key::Kind::Entry(entry) => Ok(LogicalKey::Entry {
            slot: entry.slot,
            entry_index: entry.entry_index,
            entry_hash: fixed_bytes::<DIGEST_BYTES>(
                entry.entry_hash,
                "offer.logical_key.entry.entry_hash",
            )?,
        }),
        wire::logical_key::Kind::Shred(shred) => Ok(LogicalKey::Shred {
            slot: shred.slot,
            kind: shred_kind_from_wire(shred.kind)?,
            shred_index: shred.shred_index,
            fec_set_index: shred.fec_set_index,
        }),
    }
}

fn logical_key_to_wire(value: &LogicalKey) -> wire::LogicalKey {
    let kind = match value {
        LogicalKey::Block { slot, blockhash } => wire::logical_key::Kind::Block(wire::BlockKey {
            slot: *slot,
            blockhash: blockhash.to_vec(),
        }),
        LogicalKey::Entry {
            slot,
            entry_index,
            entry_hash,
        } => wire::logical_key::Kind::Entry(wire::EntryKey {
            slot: *slot,
            entry_index: *entry_index,
            entry_hash: entry_hash.to_vec(),
        }),
        LogicalKey::Shred {
            slot,
            kind,
            shred_index,
            fec_set_index,
        } => wire::logical_key::Kind::Shred(wire::ShredKey {
            slot: *slot,
            kind: shred_kind_to_wire(*kind) as i32,
            shred_index: *shred_index,
            fec_set_index: *fec_set_index,
        }),
    };
    wire::LogicalKey { kind: Some(kind) }
}

fn shred_kind_from_wire(value: i32) -> Result<ShredKind, ReplicationWireError> {
    match wire::ShredKind::try_from(value) {
        Ok(wire::ShredKind::Data) => Ok(ShredKind::Data),
        Ok(wire::ShredKind::Coding) => Ok(ShredKind::Coding),
        Ok(wire::ShredKind::Unspecified) | Err(_) => Err(ReplicationWireError::UnknownEnum {
            field: "offer.logical_key.shred.kind",
            value,
        }),
    }
}

fn shred_kind_to_wire(value: ShredKind) -> wire::ShredKind {
    match value {
        ShredKind::Data => wire::ShredKind::Data,
        ShredKind::Coding => wire::ShredKind::Coding,
    }
}

fn commitment_from_wire(value: i32) -> Result<CommitmentEvidence, ReplicationWireError> {
    match wire::CommitmentEvidence::try_from(value) {
        Ok(wire::CommitmentEvidence::Unknown) => Ok(CommitmentEvidence::Unknown),
        Ok(wire::CommitmentEvidence::Processed) => Ok(CommitmentEvidence::Processed),
        Ok(wire::CommitmentEvidence::Confirmed) => Ok(CommitmentEvidence::Confirmed),
        Ok(wire::CommitmentEvidence::Finalized) => Ok(CommitmentEvidence::Finalized),
        Ok(wire::CommitmentEvidence::Unspecified) | Err(_) => {
            Err(ReplicationWireError::UnknownEnum {
                field: "offer.commitment",
                value,
            })
        }
    }
}

fn commitment_to_wire(value: CommitmentEvidence) -> wire::CommitmentEvidence {
    match value {
        CommitmentEvidence::Unknown => wire::CommitmentEvidence::Unknown,
        CommitmentEvidence::Processed => wire::CommitmentEvidence::Processed,
        CommitmentEvidence::Confirmed => wire::CommitmentEvidence::Confirmed,
        CommitmentEvidence::Finalized => wire::CommitmentEvidence::Finalized,
    }
}

fn disposition_from_wire(value: i32) -> Result<ReceiptDisposition, ReplicationWireError> {
    match wire::ReceiptDisposition::try_from(value) {
        Ok(wire::ReceiptDisposition::DurablyStored) => Ok(ReceiptDisposition::DurablyStored),
        Ok(wire::ReceiptDisposition::AlreadyCommitted) => Ok(ReceiptDisposition::AlreadyCommitted),
        Ok(wire::ReceiptDisposition::DurablyStoredConflict) => {
            Ok(ReceiptDisposition::DurablyStoredConflict)
        }
        Ok(wire::ReceiptDisposition::Rejected) => Ok(ReceiptDisposition::Rejected),
        Ok(wire::ReceiptDisposition::Unspecified) | Err(_) => {
            Err(ReplicationWireError::UnknownEnum {
                field: "ack.disposition",
                value,
            })
        }
    }
}

fn disposition_to_wire(value: ReceiptDisposition) -> wire::ReceiptDisposition {
    match value {
        ReceiptDisposition::DurablyStored => wire::ReceiptDisposition::DurablyStored,
        ReceiptDisposition::AlreadyCommitted => wire::ReceiptDisposition::AlreadyCommitted,
        ReceiptDisposition::DurablyStoredConflict => {
            wire::ReceiptDisposition::DurablyStoredConflict
        }
        ReceiptDisposition::Rejected => wire::ReceiptDisposition::Rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream() -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: "solana-mainnet".to_owned(),
            origin_node_id: "source-node-replica".to_owned(),
            source_id: "triton-grpc".to_owned(),
            journal_id: [7; JOURNAL_ID_BYTES],
        }
    }

    fn offer(
        sequence: u64,
        logical_key: LogicalKey,
        compressed_payload: &[u8],
    ) -> ReplicationOffer {
        ReplicationOffer {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            cluster_id: stream().cluster_id,
            record: ObservationId {
                origin_node_id: stream().origin_node_id,
                journal_id: stream().journal_id,
                sequence,
            },
            source_id: stream().source_id,
            content_digest: compute_content_digest(
                "solana-mainnet",
                &logical_key,
                1,
                compressed_payload,
            ),
            logical_key,
            payload_len: compressed_payload.len() as u64,
            payload_format_version: 1,
            commitment: CommitmentEvidence::Confirmed,
        }
    }

    fn raw_record(sequence: u64) -> RawReplicationRecord {
        let compressed_payload = format!("exact-zstd-frame-{sequence}").into_bytes();
        RawReplicationRecord {
            offer: offer(
                sequence,
                LogicalKey::Block {
                    slot: 1000 + sequence,
                    blockhash: [sequence as u8; DIGEST_BYTES],
                },
                &compressed_payload,
            ),
            compressed_payload,
            uncompressed_len: 4096 + sequence,
            raw_protobuf_sha256: [9; DIGEST_BYTES],
        }
    }

    fn ack() -> CumulativePrimaryAck {
        CumulativePrimaryAck {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            stream: stream(),
            primary_id: "blockzilla-primary".to_owned(),
            primary_term: 4,
            through_sequence: 42,
            through_content_digest: ContentDigest([1; DIGEST_BYTES]),
            rolling_chain_digest: ContentDigest([2; DIGEST_BYTES]),
            disposition: ReceiptDisposition::DurablyStored,
            durable_lsn: 1002,
            signing_key_id: "receipt-2026-07".to_owned(),
            signature: vec![3; ED25519_SIGNATURE_BYTES],
        }
    }

    fn receiver_limits() -> RawReceiverLimits {
        RawReceiverLimits {
            max_batch_records: 4,
            max_batch_compressed_bytes: 1024,
            max_batch_uncompressed_bytes: 32 * 1024,
            max_compressed_record_bytes: 512,
            max_uncompressed_record_bytes: 8 * 1024,
        }
    }

    #[test]
    fn stream_and_get_ack_request_round_trip() {
        let expected = stream();
        let encoded = wire::StreamId::try_from(&expected).expect("encode stream");
        assert_eq!(
            ReplicationStreamId::try_from(encoded).expect("decode stream"),
            expected
        );

        let request = wire::GetAckRequest::try_from(&expected).expect("encode request");
        assert_eq!(
            ReplicationStreamId::try_from(request).expect("decode request"),
            expected
        );
    }

    #[test]
    fn pull_control_messages_are_protocol_only_and_strictly_versioned() {
        let request = wire::PullBatchRequest::from(ValidatedPullBatchRequest);
        assert_eq!(
            request,
            wire::PullBatchRequest {
                protocol_version: u32::from(REPLICATION_PROTOCOL_VERSION),
            }
        );
        assert_eq!(
            ValidatedPullBatchRequest::try_from(request).expect("decode pull request"),
            ValidatedPullBatchRequest
        );

        let expected_response =
            ValidatedCommitAckResponse::new(stream(), 42).expect("valid commit response");
        let response =
            wire::CommitAckResponse::try_from(&expected_response).expect("encode commit response");
        assert_eq!(
            response,
            wire::CommitAckResponse {
                protocol_version: u32::from(REPLICATION_PROTOCOL_VERSION),
                stream: Some(wire::StreamId::try_from(&stream()).expect("encode stream")),
                through_sequence: 42,
            }
        );
        assert_eq!(
            ValidatedCommitAckResponse::try_from(response).expect("decode commit response"),
            expected_response
        );

        for unsupported in [0, u32::from(REPLICATION_PROTOCOL_VERSION) + 1] {
            assert!(matches!(
                ValidatedPullBatchRequest::try_from(wire::PullBatchRequest {
                    protocol_version: unsupported,
                }),
                Err(ReplicationWireError::UnsupportedProtocol { value }) if value == unsupported
            ));
            assert!(matches!(
                ValidatedCommitAckResponse::try_from(wire::CommitAckResponse {
                    protocol_version: unsupported,
                    stream: Some(wire::StreamId::try_from(&stream()).expect("encode stream")),
                    through_sequence: 42,
                }),
                Err(ReplicationWireError::UnsupportedProtocol { value }) if value == unsupported
            ));
        }

        let out_of_range = u32::from(u16::MAX) + 1;
        assert!(matches!(
            ValidatedPullBatchRequest::try_from(wire::PullBatchRequest {
                protocol_version: out_of_range,
            }),
            Err(ReplicationWireError::IntegerOutOfRange {
                field: "protocol_version",
                value,
            }) if value == u64::from(out_of_range)
        ));

        assert!(matches!(
            ValidatedCommitAckResponse::try_from(wire::CommitAckResponse {
                protocol_version: u32::from(REPLICATION_PROTOCOL_VERSION),
                stream: None,
                through_sequence: 42,
            }),
            Err(ReplicationWireError::MissingField {
                field: "commit_ack.stream"
            })
        ));

        let mut invalid_stream = wire::StreamId::try_from(&stream()).expect("encode stream");
        invalid_stream.journal_id.pop();
        assert!(matches!(
            ValidatedCommitAckResponse::try_from(wire::CommitAckResponse {
                protocol_version: u32::from(REPLICATION_PROTOCOL_VERSION),
                stream: Some(invalid_stream),
                through_sequence: 42,
            }),
            Err(ReplicationWireError::InvalidLength {
                field: "stream.journal_id",
                expected: JOURNAL_ID_BYTES,
                actual: 15,
            })
        ));
    }

    #[test]
    fn pull_stream_records_reuse_the_strict_push_record_envelope() {
        let expected_stream = stream();
        let expected_record = raw_record(17);
        let wire_record = wire::PushBatchRequest::try_from(
            ValidatedPushRecord::new(expected_stream.clone(), expected_record.clone())
                .expect("validate pulled record"),
        )
        .expect("encode pulled record");
        let decoded = ValidatedPushRecord::try_from(wire_record).expect("decode pulled record");
        assert_eq!(decoded.stream(), &expected_stream);
        assert_eq!(decoded.record(), &expected_record);

        let mut missing_stream = wire::PushBatchRequest::try_from(
            ValidatedPushRecord::new(expected_stream, expected_record)
                .expect("validate pulled record"),
        )
        .expect("encode pulled record");
        missing_stream.stream = None;
        assert!(matches!(
            ValidatedPushRecord::try_from(missing_stream),
            Err(ReplicationWireError::MissingField {
                field: "push_batch.stream"
            })
        ));
    }

    #[test]
    fn offers_round_trip_every_logical_key_shape() {
        let cases = [
            LogicalKey::Block {
                slot: 1,
                blockhash: [1; DIGEST_BYTES],
            },
            LogicalKey::Entry {
                slot: 2,
                entry_index: 3,
                entry_hash: [4; DIGEST_BYTES],
            },
            LogicalKey::Shred {
                slot: 5,
                kind: ShredKind::Data,
                shred_index: 6,
                fec_set_index: Some(7),
            },
            LogicalKey::Shred {
                slot: 8,
                kind: ShredKind::Coding,
                shred_index: 9,
                fec_set_index: None,
            },
        ];

        for (sequence, logical_key) in cases.into_iter().enumerate() {
            let payload = format!("payload-{sequence}");
            let expected = offer(sequence as u64, logical_key, payload.as_bytes());
            let encoded = wire::ReplicationOffer::try_from(&expected).expect("encode offer");
            assert_eq!(
                ReplicationOffer::try_from(encoded).expect("decode offer"),
                expected
            );
        }
    }

    #[test]
    fn raw_record_and_non_empty_batch_round_trip_without_changing_payload() {
        let expected_record = raw_record(11);
        let expected_payload = expected_record.compressed_payload.clone();
        let encoded =
            wire::RawReplicationRecord::try_from(expected_record.clone()).expect("encode record");
        assert_eq!(encoded.compressed_payload, expected_payload);
        assert_eq!(
            RawReplicationRecord::try_from(encoded).expect("decode record"),
            expected_record
        );

        let expected = ValidatedPushBatch {
            stream: stream(),
            records: vec![raw_record(11), raw_record(12)],
        };
        let mut builder =
            BoundedPushBatchBuilder::new(receiver_limits()).expect("valid receiver limits");
        for record in expected.records.clone() {
            let request = wire::PushBatchRequest::try_from(
                ValidatedPushRecord::new(expected.stream.clone(), record)
                    .expect("validate outbound streamed record"),
            )
            .expect("encode streamed record");
            builder.push_wire(request).expect("admit streamed record");
        }
        assert_eq!(builder.finish().expect("finish non-empty batch"), expected);
    }

    #[test]
    fn get_ack_response_distinguishes_missing_and_present_prefix() {
        let missing = GetAckState::try_from(wire::GetAckResponse { ack: None })
            .expect("decode empty stream response");
        assert_eq!(missing, GetAckState::NoDurablePrefix);
        assert_eq!(
            wire::GetAckResponse::try_from(&missing)
                .expect("encode empty stream response")
                .ack,
            None
        );

        let expected = GetAckState::DurablePrefix(ack());
        let encoded = wire::GetAckResponse::try_from(&expected).expect("encode present ack");
        assert!(encoded.ack.is_some());
        assert_eq!(
            GetAckState::try_from(encoded).expect("decode present ack"),
            expected
        );
    }

    #[test]
    fn cumulative_ack_round_trips() {
        let expected = ack();
        let encoded = wire::CumulativePrimaryAck::try_from(&expected).expect("encode ack");
        assert_eq!(
            CumulativePrimaryAck::try_from(encoded).expect("decode ack"),
            expected
        );
    }

    #[test]
    fn invalid_fixed_width_fields_are_rejected() {
        let mut stream = wire::StreamId::try_from(&stream()).expect("encode stream");
        stream.journal_id.pop();
        assert!(matches!(
            ReplicationStreamId::try_from(stream),
            Err(ReplicationWireError::InvalidLength {
                field: "stream.journal_id",
                expected: JOURNAL_ID_BYTES,
                actual: 15,
            })
        ));

        let record = raw_record(1);
        let mut offer = wire::ReplicationOffer::try_from(&record.offer).expect("encode offer");
        offer.content_digest.pop();
        assert!(matches!(
            ReplicationOffer::try_from(offer),
            Err(ReplicationWireError::InvalidLength {
                field: "offer.content_digest",
                expected: DIGEST_BYTES,
                actual: 31,
            })
        ));

        let mut raw =
            wire::RawReplicationRecord::try_from(record.clone()).expect("encode raw record");
        raw.raw_protobuf_sha256.pop();
        assert!(matches!(
            RawReplicationRecord::try_from(raw),
            Err(ReplicationWireError::InvalidLength {
                field: "record.raw_protobuf_sha256",
                expected: DIGEST_BYTES,
                actual: 31,
            })
        ));

        let mut ack = wire::CumulativePrimaryAck::try_from(&ack()).expect("encode ack");
        ack.signature.pop();
        assert!(matches!(
            CumulativePrimaryAck::try_from(ack),
            Err(ReplicationWireError::InvalidLength {
                field: "ack.signature",
                expected: ED25519_SIGNATURE_BYTES,
                actual: 63,
            })
        ));
    }

    #[test]
    fn unknown_and_unspecified_enums_are_rejected() {
        let record = raw_record(2);
        let mut encoded_offer =
            wire::ReplicationOffer::try_from(&record.offer).expect("encode offer");
        encoded_offer.commitment = 999;
        assert!(matches!(
            ReplicationOffer::try_from(encoded_offer),
            Err(ReplicationWireError::UnknownEnum {
                field: "offer.commitment",
                value: 999,
            })
        ));

        let mut encoded_offer =
            wire::ReplicationOffer::try_from(&record.offer).expect("encode offer");
        encoded_offer.commitment = wire::CommitmentEvidence::Unspecified as i32;
        assert!(matches!(
            ReplicationOffer::try_from(encoded_offer),
            Err(ReplicationWireError::UnknownEnum {
                field: "offer.commitment",
                value: 0,
            })
        ));

        let mut ack = wire::CumulativePrimaryAck::try_from(&ack()).expect("encode ack");
        ack.disposition = 998;
        assert!(matches!(
            CumulativePrimaryAck::try_from(ack),
            Err(ReplicationWireError::UnknownEnum {
                field: "ack.disposition",
                value: 998,
            })
        ));

        let shred = offer(
            3,
            LogicalKey::Shred {
                slot: 3,
                kind: ShredKind::Data,
                shred_index: 4,
                fec_set_index: None,
            },
            b"shred-payload",
        );
        let mut shred = wire::ReplicationOffer::try_from(&shred).expect("encode shred offer");
        let Some(wire::logical_key::Kind::Shred(shred_key)) =
            shred.logical_key.as_mut().and_then(|key| key.kind.as_mut())
        else {
            panic!("expected shred key");
        };
        shred_key.kind = 997;
        assert!(matches!(
            ReplicationOffer::try_from(shred),
            Err(ReplicationWireError::UnknownEnum {
                field: "offer.logical_key.shred.kind",
                value: 997,
            })
        ));
    }

    #[test]
    fn narrowing_canonical_ids_and_ack_lsn_are_strict() {
        let record = raw_record(4);
        let mut protocol = wire::ReplicationOffer::try_from(&record.offer).expect("encode offer");
        protocol.protocol_version = u32::from(u16::MAX) + 1;
        assert!(matches!(
            ReplicationOffer::try_from(protocol),
            Err(ReplicationWireError::IntegerOutOfRange {
                field: "protocol_version",
                ..
            })
        ));

        let mut format = wire::ReplicationOffer::try_from(&record.offer).expect("encode offer");
        format.payload_format_version = u32::from(u16::MAX) + 1;
        assert!(matches!(
            ReplicationOffer::try_from(format),
            Err(ReplicationWireError::IntegerOutOfRange {
                field: "offer.payload_format_version",
                ..
            })
        ));

        let mut invalid_stream = wire::StreamId::try_from(&stream()).expect("encode stream");
        invalid_stream.source_id = "../unsafe".to_owned();
        assert!(matches!(
            ReplicationStreamId::try_from(invalid_stream),
            Err(ReplicationWireError::InvalidValue {
                field: "stream.source_id",
            })
        ));

        let mut zero_lsn = wire::CumulativePrimaryAck::try_from(&ack()).expect("encode ack");
        zero_lsn.durable_lsn = 0;
        assert!(matches!(
            CumulativePrimaryAck::try_from(zero_lsn),
            Err(ReplicationWireError::InvalidValue {
                field: "ack.durable_lsn",
            })
        ));
    }

    #[test]
    fn push_batch_rejects_empty_or_mixed_streams() {
        let empty = BoundedPushBatchBuilder::new(receiver_limits()).expect("valid limits");
        assert_eq!(empty.finish(), Err(ReplicationWireError::EmptyBatch));

        let mut other = raw_record(2);
        other.offer.source_id = "different-source".to_owned();
        let mismatched_request = wire::PushBatchRequest {
            stream: Some(wire::StreamId::try_from(&stream()).expect("encode stream")),
            record: Some(
                wire::RawReplicationRecord::try_from(other.clone())
                    .expect("encode other stream record"),
            ),
        };
        assert_eq!(
            ValidatedPushRecord::try_from(mismatched_request),
            Err(ReplicationWireError::WrongRecordStream)
        );

        let mixed_stream = ReplicationStreamId::from_offer(&other.offer);
        let mut builder =
            BoundedPushBatchBuilder::new(receiver_limits()).expect("valid receiver limits");
        builder
            .push(
                ValidatedPushRecord::new(stream(), raw_record(1))
                    .expect("validate first stream record"),
            )
            .expect("admit first stream record");
        assert_eq!(
            builder.push(
                ValidatedPushRecord::new(mixed_stream, other)
                    .expect("validate second stream record")
            ),
            Err(ReplicationWireError::WrongBatchStream { record_index: 1 })
        );
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn streaming_builder_checks_limits_before_retaining_each_record() {
        let first = raw_record(1);
        let second = raw_record(2);
        let one_record_limits = RawReceiverLimits {
            max_batch_records: 1,
            ..receiver_limits()
        };
        let mut builder =
            BoundedPushBatchBuilder::new(one_record_limits).expect("valid count limits");
        builder
            .push(ValidatedPushRecord::new(stream(), first).expect("validate first record"))
            .expect("admit first record");
        let retained_compressed = builder.compressed_bytes();
        let retained_uncompressed = builder.uncompressed_bytes();
        assert!(matches!(
            builder
                .push(ValidatedPushRecord::new(stream(), second).expect("validate second record")),
            Err(ReplicationWireError::LimitExceeded {
                limit: "record count",
                maximum: 1,
                attempted: 2,
            })
        ));
        assert_eq!(builder.len(), 1);
        assert_eq!(builder.compressed_bytes(), retained_compressed);
        assert_eq!(builder.uncompressed_bytes(), retained_uncompressed);

        let first = raw_record(3);
        let second = raw_record(4);
        let compressed_limit = first.offer.payload_len;
        let mut builder = BoundedPushBatchBuilder::new(RawReceiverLimits {
            max_batch_compressed_bytes: compressed_limit,
            max_compressed_record_bytes: compressed_limit,
            ..receiver_limits()
        })
        .expect("valid byte limits");
        builder
            .push(ValidatedPushRecord::new(stream(), first).expect("validate first record"))
            .expect("admit first record");
        assert!(matches!(
            builder
                .push(ValidatedPushRecord::new(stream(), second).expect("validate second record")),
            Err(ReplicationWireError::LimitExceeded {
                limit: "cumulative compressed bytes",
                ..
            })
        ));
        assert_eq!(builder.len(), 1);

        let first = raw_record(5);
        let second = raw_record(6);
        let uncompressed_limit = first
            .uncompressed_len
            .checked_add(second.uncompressed_len)
            .and_then(|total| total.checked_sub(1))
            .expect("test uncompressed limit");
        let per_record_limit = first.uncompressed_len.max(second.uncompressed_len);
        let mut builder = BoundedPushBatchBuilder::new(RawReceiverLimits {
            max_batch_uncompressed_bytes: uncompressed_limit,
            max_uncompressed_record_bytes: per_record_limit,
            ..receiver_limits()
        })
        .expect("valid uncompressed limits");
        builder
            .push(ValidatedPushRecord::new(stream(), first).expect("validate first record"))
            .expect("admit first record");
        assert!(matches!(
            builder
                .push(ValidatedPushRecord::new(stream(), second).expect("validate second record")),
            Err(ReplicationWireError::LimitExceeded {
                limit: "cumulative uncompressed bytes",
                ..
            })
        ));
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn raw_record_rejects_payload_length_or_content_mismatch() {
        let record = raw_record(3);
        let mut encoded =
            wire::RawReplicationRecord::try_from(record.clone()).expect("encode record");
        encoded.compressed_payload.push(0);
        assert!(matches!(
            RawReplicationRecord::try_from(encoded),
            Err(ReplicationWireError::PayloadLengthMismatch { .. })
        ));

        let mut encoded = wire::RawReplicationRecord::try_from(record).expect("encode record");
        encoded.compressed_payload[0] ^= 0xff;
        assert_eq!(
            RawReplicationRecord::try_from(encoded),
            Err(ReplicationWireError::ContentDigestMismatch)
        );
    }
}
