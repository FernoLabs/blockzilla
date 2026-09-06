use thiserror::Error;

/// Failure to construct, decode, or validate a Hivezilla V1 primitive.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("{field} must contain exactly {expected} bytes, got {actual}")]
    InvalidLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },

    #[error("{context} is truncated: expected {expected} bytes, got {actual}")]
    Truncated {
        context: &'static str,
        expected: usize,
        actual: usize,
    },

    #[error("{context} has {count} trailing bytes")]
    TrailingBytes { context: &'static str, count: usize },

    #[error("payload format zero is not registered in Hivezilla V1")]
    InvalidPayloadFormat,

    #[error("payload format {value} is not registered in Hivezilla V1")]
    UnknownPayloadFormat { value: u32 },

    #[error("payload format {payload_format} has no registered V1 version {version}")]
    UnknownPayloadFormatVersion { payload_format: u32, version: u16 },

    #[error("record payload length {actual} exceeds the V1 limit of {max}")]
    PayloadTooLarge { actual: u64, max: u64 },

    #[error("cursor at u64::MAX cannot advance")]
    SequenceOverflow,

    #[error("record sequence mismatch: expected {expected}, got {actual}")]
    SequenceMismatch { expected: u64, actual: u64 },

    #[error("record prefix hash does not match its predecessor cursor and payload")]
    PrefixMismatch,

    #[error("{context} has invalid magic bytes")]
    InvalidMagic { context: &'static str },

    #[error("{context} cursor does not match the required prefix-chain boundary")]
    CursorMismatch { context: &'static str },

    #[error("{context} stream does not match the required stream")]
    StreamMismatch { context: &'static str },

    #[error("overflow range ends before it starts")]
    ReversedOverflowRange,

    #[error("an empty overflow range must use one identical boundary cursor")]
    InvalidEmptyOverflowRange,

    #[error("encoded length {actual} does not match declared length {expected}")]
    EncodedLengthMismatch { expected: u64, actual: u64 },

    #[error("encoded bytes SHA-256 does not match the declared digest")]
    EncodedSha256Mismatch,

    #[error("sealed segment length {actual} is below the minimum of {min}")]
    EncodedSegmentTooSmall { actual: u64, min: u64 },

    #[error("a present object version must not be empty")]
    EmptyObjectVersion,

    #[error("object version length {actual} exceeds the V1 limit of {max}")]
    ObjectVersionTooLarge { actual: u64, max: u64 },

    #[error("manifest version {value} is not supported")]
    UnknownManifestVersion { value: u16 },

    #[error("producer descriptor must not be empty")]
    EmptyProducerDescriptor,

    #[error("producer descriptor length {actual} exceeds the V1 limit of {max}")]
    ProducerDescriptorTooLarge { actual: u64, max: u64 },

    #[error("producer configuration digest does not match the exact descriptor")]
    ProducerConfigMismatch,

    #[error("stream manifest digest does not match its canonical body")]
    ManifestHashMismatch,

    #[error("invalid option tag {value} while decoding {field}")]
    InvalidOptionTag { field: &'static str, value: u8 },

    #[error("unknown lineage reason {value}")]
    UnknownLineageReason { value: u16 },

    #[error("unknown lineage continuity {value}")]
    UnknownLineageContinuity { value: u8 },

    #[error("gap-event producer descriptor version {value} is not supported")]
    UnknownGapEventProducerDescriptorVersion { value: u16 },

    #[error("gap-event reason {value} is not registered in Hivezilla V1")]
    UnknownGapEventReason { value: u16 },

    #[error("invalid gap-event permitted-reason set: {reason}")]
    InvalidGapEventReasonSet { reason: &'static str },

    #[error("gap-event source-position descriptor must not be empty")]
    EmptySourcePositionDescriptor,

    #[error("gap-event source-position descriptor length {actual} exceeds the V1 limit of {max}")]
    SourcePositionDescriptorTooLarge { actual: u64, max: u64 },

    #[error("gap-event {field} length {actual} exceeds the V1 limit of {max}")]
    GapEventPositionTooLarge {
        field: &'static str,
        actual: u64,
        max: u64,
    },

    #[error("gap event does not match its producer descriptor: {reason}")]
    GapEventDescriptorMismatch { reason: &'static str },

    #[error("invalid HiveSync limit {field}={actual}; expected {min}..={max}")]
    InvalidSyncLimit {
        field: &'static str,
        actual: u64,
        min: u64,
        max: u64,
    },

    #[error("invalid cursor order in {context}")]
    InvalidCursorOrder { context: &'static str },

    #[error("invalid fetch range: {reason}")]
    InvalidFetchRange { reason: &'static str },

    #[error("HiveSync session ID does not match the active resume")]
    SessionMismatch,

    #[error("invalid transfer chunk: {reason}")]
    InvalidTransferChunk { reason: &'static str },

    #[error("HiveSync error code {value} is not registered in V1")]
    UnknownHiveSyncErrorCode { value: i32 },

    #[error("ACK binding mismatch: {reason}")]
    AckBindingMismatch { reason: &'static str },

    #[error("authenticated peer ID length {actual} is outside {min}..={max}")]
    InvalidAuthenticatedPeerIdLength { actual: u64, min: u64, max: u64 },

    #[error("accepted-ACK receipt generation cannot advance past u64::MAX")]
    ReceiptGenerationOverflow,

    #[error("accepted-ACK receipt digest does not match its canonical body")]
    AcceptedAckReceiptHashMismatch,

    #[error("invalid accepted-ACK receipt chain: {reason}")]
    InvalidAcceptedAckReceiptChain { reason: &'static str },

    #[error("accepted ACK cursor does not strictly advance its predecessor")]
    NonMonotonicAck,

    #[error("invalid durability policy: {reason}")]
    InvalidDurabilityPolicy { reason: &'static str },

    #[error("manifest fields are incompatible with payload format {payload_format}: {reason}")]
    InvalidManifestShape {
        payload_format: u32,
        reason: &'static str,
    },

    #[error("integer conversion overflow while decoding {field}")]
    IntegerOverflow { field: &'static str },

    #[error("{context} is not in canonical order")]
    NonCanonicalOrder { context: &'static str },

    #[error("invalid stream-registry logical name: {reason}")]
    InvalidRegistryLogicalName { reason: &'static str },

    #[error("unknown stream-registry status {value}")]
    UnknownRegistryStatus { value: u8 },

    #[error("stream-registry entry count {actual} exceeds the V1 limit of {max}")]
    RegistryEntryLimitExceeded { actual: u64, max: u64 },

    #[error("stream-registry snapshot length {actual} exceeds the V1 limit of {max}")]
    RegistrySnapshotTooLarge { actual: u64, max: u64 },

    #[error("stream-registry snapshot digest does not match its canonical body")]
    RegistrySnapshotHashMismatch,

    #[error("invalid stream-registry snapshot: {reason}")]
    InvalidRegistrySnapshot { reason: &'static str },

    #[error("invalid stream-registry generation/predecessor shape: {reason}")]
    InvalidRegistryGeneration { reason: &'static str },

    #[error("invalid stream-registry transition: {reason}")]
    InvalidRegistryTransition { reason: &'static str },

    #[error("stream-registry snapshot was authored by a different Blockzilla authority")]
    RegistryAuthorityMismatch,

    #[error("stream-registry head does not exactly match the snapshot")]
    RegistryHeadMismatch,

    #[error("invalid terminal raw object: {reason}")]
    InvalidTerminalRawObject { reason: &'static str },

    #[error("terminal raw-object {field} limit {actual} is below the minimum of {minimum}")]
    InvalidTerminalRawObjectLimit {
        field: &'static str,
        actual: u64,
        minimum: u64,
    },

    #[error("terminal raw-object encoding length {actual} exceeds the configured limit of {max}")]
    TerminalRawObjectTooLarge { actual: u64, max: u64 },

    #[error("terminal raw-object record count {actual} exceeds the configured limit of {max}")]
    TerminalRawObjectRecordLimitExceeded { actual: u64, max: u64 },

    #[error("terminal {field} length {actual} is outside {min}..={max}")]
    InvalidTerminalLocatorLength {
        field: &'static str,
        actual: u64,
        min: u64,
        max: u64,
    },

    #[error("terminal object key does not match its deterministic object identity")]
    TerminalObjectKeyMismatch,

    #[error("terminal-copy verification method {value} is not registered in V1")]
    UnknownTerminalVerification { value: u8 },

    #[error("invalid terminal copy receipt: {reason}")]
    InvalidTerminalCopyReceipt { reason: &'static str },

    #[error("invalid terminal range index: {reason}")]
    InvalidTerminalRangeIndex { reason: &'static str },

    #[error("terminal range-index copy count {actual} is outside {min}..={max}")]
    InvalidTerminalCopyCount { actual: u64, min: u64, max: u64 },

    #[error("terminal range-index encoding length {actual} exceeds the V1 limit of {max}")]
    TerminalRangeIndexTooLarge { actual: u64, max: u64 },

    #[error("terminal durability has {actual} verified failure domains but requires {required}")]
    TerminalDurabilityDeficit { actual: u64, required: u64 },

    #[error("terminal range conflict: {reason}")]
    TerminalRangeConflict { reason: &'static str },

    #[error("invalid terminal cursor checkpoint: {reason}")]
    InvalidTerminalCheckpoint { reason: &'static str },
}

pub type Result<T> = std::result::Result<T, ProtocolError>;
