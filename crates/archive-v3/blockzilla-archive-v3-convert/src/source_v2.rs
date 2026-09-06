//! Checked, lossless helpers for reading Compact V2 transaction data.
//!
//! These helpers do not write Index Archive files. They form the source-side
//! boundary of the upgrader: malformed ranges, unresolved dictionary values,
//! and missing vote hashes are errors instead of invented facts.

use std::{collections::BTreeMap, error::Error, fmt, fs::File, os::unix::fs::FileExt};

use blockzilla_archive_v2::{
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ArchiveV2ComputeBudgetInstructionData, ArchiveV2HotBlockBlob, ArchiveV2HotInstructionData,
    ArchiveV2HotMessagePayload, ArchiveV2HotTxRow, ArchiveV2HotV0Message,
    ArchiveV2SystemInstructionData, ArchiveV2VoteHashRef, ArchiveV2VoteStateUpdate,
    ArchiveV2VoteTowerSync,
};
use blockzilla_compact::{CompactMetaV1, OwnedCompactAddressTableLookup};
pub use blockzilla_compact_v2_reader::{CompactV2MessageSchema, CompactV2MetadataSchema};
use blockzilla_compact_v2_reader::{
    PinnedLocalRangeSource, SourceError, SourceResult, decode_compact_v2_message,
    decode_compact_v2_metadata,
};
use blockzilla_primitives::{CompactPubkey, PubkeyResolver};

/// Width of a Compact V2 public-key dictionary record.
pub const PUBKEY_RECORD_LEN: usize = 32;
/// Width of a Compact V2 vote-hash dictionary record.
pub const VOTE_HASH_RECORD_LEN: usize = 65;
/// Absolute bound on whole-message candidates tested for one transaction.
///
/// This covers 13 independent two-form instructions (`2^13` candidates), the
/// largest ambiguous combination that still fits a Solana transaction packet.
pub const MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS: usize = 8_192;

/// Resolves Compact V2 pubkey ids with one positioned registry read per id.
///
/// The file handle comes from [`PinnedLocalRangeSource`]. It therefore stays
/// on the same immutable file generation for the full conversion, even if the
/// registry pathname is replaced.
#[derive(Debug)]
pub struct PinnedPubkeyResolver {
    file: File,
    records: u32,
}

impl PinnedPubkeyResolver {
    /// Open an optional raw Compact V2 pubkey registry from its pinned handle.
    pub fn open(source: &PinnedLocalRangeSource, object: &str) -> SourceResult<Option<Self>> {
        let Some(file) = source.pinned_file_clone(object)? else {
            return Ok(None);
        };
        let byte_len = file
            .metadata()
            .map_err(|source| SourceError::Io {
                object: object.to_owned(),
                source,
            })?
            .len();
        let record_len = PUBKEY_RECORD_LEN as u64;
        if !byte_len.is_multiple_of(record_len) {
            return Err(SourceError::Protocol(format!(
                "public-key registry {object} has {byte_len} bytes, which is not a multiple of {PUBKEY_RECORD_LEN}"
            )));
        }
        let records = u32::try_from(byte_len / record_len).map_err(|_| {
            SourceError::Protocol(format!(
                "public-key registry {object} has more than {} records",
                u32::MAX
            ))
        })?;
        Ok(Some(Self { file, records }))
    }

    #[inline]
    pub fn record_count(&self) -> u32 {
        self.records
    }
}

impl PubkeyResolver for PinnedPubkeyResolver {
    #[inline]
    fn resolve_pubkey(&self, id: u32) -> Option<[u8; PUBKEY_RECORD_LEN]> {
        let zero_based = id.checked_sub(1)?;
        if zero_based >= self.records {
            return None;
        }
        let offset = u64::from(zero_based) * PUBKEY_RECORD_LEN as u64;
        let mut key = [0_u8; PUBKEY_RECORD_LEN];
        self.file.read_exact_at(&mut key, offset).ok()?;
        Some(key)
    }
}

const VERSIONED_MESSAGE_PREFIX: u8 = 0x80;
const V1_MESSAGE_VERSION: u8 = 1;

/// SIMD-0385 caps a v1 message's own counts.
const V1_MAX_ADDRESSES: u8 = 64;
const V1_MAX_INSTRUCTIONS: u8 = 64;

/// Config mask bits, in the order their values are written. `PRIORITY_FEE`
/// takes two bits because the value array is counted in four-byte slots.
const V1_CONFIG_PRIORITY_FEE: u32 = 0b11;
const V1_CONFIG_COMPUTE_UNIT_LIMIT: u32 = 0b100;
const V1_CONFIG_LOADED_ACCOUNTS_DATA_SIZE: u32 = 0b1000;
const V1_CONFIG_HEAP_SIZE: u32 = 0b1_0000;
const V0_MESSAGE_VERSION: u8 = 0;

/// A source-side validation or reconstruction error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceV2Error {
    InvalidPubkeyRegistryLength {
        actual: usize,
    },
    DuplicateRegistryPubkey {
        first_id: u32,
        duplicate_id: u32,
    },
    InvalidRegistryId {
        id: u32,
        records: u32,
    },
    PubkeyIdExhausted,
    AllocationFailed {
        what: &'static str,
        message: String,
    },
    RegionEndOverflow {
        tx_index: u32,
        region: &'static str,
        offset: u64,
        len: u64,
    },
    RegionIndexTooLarge {
        tx_index: u32,
        region: &'static str,
        value: u64,
    },
    RegionOutOfBounds {
        tx_index: u32,
        region: &'static str,
        offset: u64,
        len: u64,
        available: usize,
    },
    EmptyRegion {
        tx_index: u32,
        region: &'static str,
    },
    RawTransactionFallback {
        tx_index: u32,
    },
    RawMetadataFallback {
        tx_index: u32,
    },
    MetadataFlagMismatch {
        tx_index: u32,
        has_metadata: bool,
        metadata_len: u32,
    },
    MessageVersionFlagMismatch {
        tx_index: u32,
        flag_is_v0: bool,
        payload_is_v0: bool,
    },
    Decode {
        tx_index: u32,
        region: &'static str,
        message: String,
    },
    LookupCountOverflow {
        kind: LoadedAddressKind,
    },
    LookupCountMismatch {
        kind: LoadedAddressKind,
        expected: u32,
        actual: u32,
    },
    LoadedAddressesUnavailable {
        expected_writable: u32,
        expected_readonly: u32,
    },
    InvalidVoteHashRegistryLength {
        actual: usize,
    },
    VoteHashRegistryTooLarge {
        records: usize,
    },
    InvalidVoteHashRegistryFlags {
        block_id: u32,
        flags: u8,
    },
    MissingVoteHashResolver {
        block_id: u32,
        kind: VoteHashKind,
    },
    MissingVoteHash {
        block_id: u32,
        kind: VoteHashKind,
    },
    AuxiliaryVoteHashBlockReference {
        block_id: u32,
    },
    AmbiguousInstructionEncoding {
        candidates: usize,
    },
    LegacyRequiredSignaturesSetVersionBit {
        required: u8,
    },
    RequiredSignaturesExceedStaticKeys {
        required: u8,
        static_keys: usize,
    },
    ReadonlySignedExceedRequired {
        readonly: u8,
        required: u8,
    },
    NoWritableFeePayer {
        readonly: u8,
        required: u8,
    },
    ReadonlyUnsignedExceedUnsignedStatic {
        readonly: u8,
        unsigned_static: usize,
    },
    MessageAccountCountOverflow,
    MessageAccountCountExceedsIndexRange {
        actual: usize,
    },
    EmptyAddressTableLookup {
        lookup: usize,
    },
    MessageAccountIndexOutOfBounds {
        instruction: usize,
        field: &'static str,
        index: u8,
        account_count: usize,
    },
    ProgramIdIsFeePayer {
        instruction: usize,
    },
    V0ProgramIdIsLoaded {
        instruction: usize,
        index: u8,
        static_account_count: usize,
    },
    ShortVecLengthTooLarge {
        field: &'static str,
        actual: usize,
    },
    EmptyInstructionCandidates {
        instruction: usize,
    },
    InvalidCandidateCombinationLimit {
        requested: usize,
        hard_maximum: usize,
    },
    CandidateCombinationLimitExceeded {
        maximum: usize,
    },
    NoVerifiedMessageCandidate,
    MultipleVerifiedMessageCandidates,
    VoteLockoutCountTooLarge {
        actual: usize,
    },
    VoteLockoutSlotOverflow {
        previous: u64,
        offset: u64,
    },
    SystemSeedTooLong {
        actual: usize,
    },
}

impl fmt::Display for SourceV2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPubkeyRegistryLength { actual } => write!(
                f,
                "public-key registry length {actual} is not a multiple of {PUBKEY_RECORD_LEN}"
            ),
            Self::DuplicateRegistryPubkey {
                first_id,
                duplicate_id,
            } => write!(
                f,
                "public-key registry id {duplicate_id} duplicates id {first_id}"
            ),
            Self::InvalidRegistryId { id, records } => write!(
                f,
                "public-key registry id {id} is outside the valid 1..={records} range"
            ),
            Self::PubkeyIdExhausted => f.write_str("public-key registry has no free u32 id"),
            Self::AllocationFailed { what, message } => {
                write!(f, "cannot allocate {what}: {message}")
            }
            Self::RegionEndOverflow {
                tx_index,
                region,
                offset,
                len,
            } => write!(
                f,
                "transaction {tx_index} {region} range {offset}+{len} overflows"
            ),
            Self::RegionIndexTooLarge {
                tx_index,
                region,
                value,
            } => write!(
                f,
                "transaction {tx_index} {region} index {value} does not fit this platform"
            ),
            Self::RegionOutOfBounds {
                tx_index,
                region,
                offset,
                len,
                available,
            } => write!(
                f,
                "transaction {tx_index} {region} range {offset}+{len} exceeds {available} bytes"
            ),
            Self::EmptyRegion { tx_index, region } => {
                write!(f, "transaction {tx_index} has an empty {region} region")
            }
            Self::RawTransactionFallback { tx_index } => write!(
                f,
                "transaction {tx_index} uses raw transaction fallback; structured conversion is not lossless"
            ),
            Self::RawMetadataFallback { tx_index } => write!(
                f,
                "transaction {tx_index} uses raw metadata fallback; runtime conversion is not lossless"
            ),
            Self::MetadataFlagMismatch {
                tx_index,
                has_metadata,
                metadata_len,
            } => write!(
                f,
                "transaction {tx_index} has HAS_METADATA={has_metadata} and metadata_len={metadata_len}"
            ),
            Self::MessageVersionFlagMismatch {
                tx_index,
                flag_is_v0,
                payload_is_v0,
            } => write!(
                f,
                "transaction {tx_index} has V0 flag {flag_is_v0}, but decoded payload V0 is {payload_is_v0}"
            ),
            Self::Decode {
                tx_index,
                region,
                message,
            } => write!(
                f,
                "cannot decode transaction {tx_index} {region}: {message}"
            ),
            Self::LookupCountOverflow { kind } => {
                write!(f, "{kind} lookup address count exceeds u32")
            }
            Self::LookupCountMismatch {
                kind,
                expected,
                actual,
            } => write!(
                f,
                "V0 lookup descriptors require {expected} {kind} addresses, but metadata has {actual}"
            ),
            Self::LoadedAddressesUnavailable {
                expected_writable,
                expected_readonly,
            } => write!(
                f,
                "V0 lookup descriptors require {expected_writable} writable and {expected_readonly} readonly addresses, but metadata is unavailable"
            ),
            Self::InvalidVoteHashRegistryLength { actual } => write!(
                f,
                "vote-hash registry length {actual} is not a multiple of {VOTE_HASH_RECORD_LEN}"
            ),
            Self::VoteHashRegistryTooLarge { records } => write!(
                f,
                "vote-hash registry has {records} records, which exceeds the u32 block-id range"
            ),
            Self::InvalidVoteHashRegistryFlags { block_id, flags } => write!(
                f,
                "vote-hash registry block {block_id} has unknown flags {flags:#04x}"
            ),
            Self::MissingVoteHashResolver { block_id, kind } => write!(
                f,
                "instruction needs {kind} vote hash for block {block_id}, but no registry was supplied"
            ),
            Self::MissingVoteHash { block_id, kind } => {
                write!(f, "vote-hash registry block {block_id} has no {kind} hash")
            }
            Self::AuxiliaryVoteHashBlockReference { block_id } => write!(
                f,
                "switch-proof hash unexpectedly refers to vote-hash registry block {block_id}"
            ),
            Self::AmbiguousInstructionEncoding { candidates } => write!(
                f,
                "Compact V2 does not identify which of {candidates} valid instruction encodings was signed"
            ),
            Self::LegacyRequiredSignaturesSetVersionBit { required } => write!(
                f,
                "legacy required-signature count {required} sets the version prefix bit"
            ),
            Self::RequiredSignaturesExceedStaticKeys {
                required,
                static_keys,
            } => write!(
                f,
                "required-signature count {required} exceeds {static_keys} static account keys"
            ),
            Self::ReadonlySignedExceedRequired { readonly, required } => write!(
                f,
                "readonly signed count {readonly} exceeds required-signature count {required}"
            ),
            Self::NoWritableFeePayer { readonly, required } => write!(
                f,
                "readonly signed count {readonly} leaves no writable fee payer among {required} required signatures"
            ),
            Self::ReadonlyUnsignedExceedUnsignedStatic {
                readonly,
                unsigned_static,
            } => write!(
                f,
                "readonly unsigned count {readonly} exceeds {unsigned_static} unsigned static account keys"
            ),
            Self::MessageAccountCountOverflow => {
                f.write_str("static and loaded message account counts overflow usize")
            }
            Self::MessageAccountCountExceedsIndexRange { actual } => write!(
                f,
                "message has {actual} static and loaded accounts, which exceeds the 256 accounts addressable by u8 indexes"
            ),
            Self::EmptyAddressTableLookup { lookup } => write!(
                f,
                "V0 address-table lookup {lookup} does not load a writable or readonly account"
            ),
            Self::MessageAccountIndexOutOfBounds {
                instruction,
                field,
                index,
                account_count,
            } => write!(
                f,
                "instruction {instruction} {field} index {index} is outside {account_count} message accounts"
            ),
            Self::ProgramIdIsFeePayer { instruction } => {
                write!(
                    f,
                    "instruction {instruction} uses the fee payer as its program"
                )
            }
            Self::V0ProgramIdIsLoaded {
                instruction,
                index,
                static_account_count,
            } => write!(
                f,
                "V0 instruction {instruction} program index {index} refers to a loaded account; only {static_account_count} static accounts can be programs"
            ),
            Self::ShortVecLengthTooLarge { field, actual } => {
                write!(f, "{field} length {actual} exceeds u16")
            }
            Self::EmptyInstructionCandidates { instruction } => {
                write!(f, "instruction {instruction} has no data candidate")
            }
            Self::InvalidCandidateCombinationLimit {
                requested,
                hard_maximum,
            } => write!(
                f,
                "candidate combination limit {requested} is outside 1..={hard_maximum}"
            ),
            Self::CandidateCombinationLimitExceeded { maximum } => write!(
                f,
                "instruction candidates require more than {maximum} message combinations"
            ),
            Self::NoVerifiedMessageCandidate => {
                f.write_str("no reconstructed signed-message candidate verified")
            }
            Self::MultipleVerifiedMessageCandidates => {
                f.write_str("more than one reconstructed signed-message candidate verified")
            }
            Self::VoteLockoutCountTooLarge { actual } => write!(
                f,
                "vote lockout count {actual} exceeds the canonical short-vector u16 limit"
            ),
            Self::VoteLockoutSlotOverflow { previous, offset } => {
                write!(f, "vote lockout slot {previous}+{offset} overflows u64")
            }
            Self::SystemSeedTooLong { actual } => {
                write!(f, "system instruction seed length {actual} exceeds u64")
            }
        }
    }
}

impl Error for SourceV2Error {}

/// Writable or read-only addresses loaded through V0 lookup tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadedAddressKind {
    Writable,
    Readonly,
}

impl fmt::Display for LoadedAddressKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Writable => f.write_str("writable"),
            Self::Readonly => f.write_str("readonly"),
        }
    }
}

/// Counts proved by V0 descriptors and checked against runtime metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoadedAddressCounts {
    pub writable: u32,
    pub readonly: u32,
}

/// A deterministic, one-based public-key dictionary.
///
/// An ordered commit coordinator can own this value while conversion workers
/// return raw public keys. Existing raw keys reuse their original id. New raw
/// keys get their first free id. Zero is never returned.
#[derive(Debug, Clone)]
pub struct PubkeyDictionary {
    bytes: Vec<u8>,
    by_key: BTreeMap<[u8; PUBKEY_RECORD_LEN], u32>,
    records: u32,
}

impl PubkeyDictionary {
    /// Load and validate an existing Compact V2 `registry.bin` image.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, SourceV2Error> {
        if !bytes.len().is_multiple_of(PUBKEY_RECORD_LEN) {
            return Err(SourceV2Error::InvalidPubkeyRegistryLength {
                actual: bytes.len(),
            });
        }
        let record_count = u32::try_from(bytes.len() / PUBKEY_RECORD_LEN)
            .map_err(|_| SourceV2Error::PubkeyIdExhausted)?;
        let mut by_key = BTreeMap::new();
        for (index, chunk) in bytes.chunks_exact(PUBKEY_RECORD_LEN).enumerate() {
            let key: [u8; PUBKEY_RECORD_LEN] = chunk
                .try_into()
                .expect("chunks_exact provides the public-key record width");
            let id = u32::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .ok_or(SourceV2Error::PubkeyIdExhausted)?;
            if let Some(first_id) = by_key.insert(key, id) {
                return Err(SourceV2Error::DuplicateRegistryPubkey {
                    first_id,
                    duplicate_id: id,
                });
            }
        }
        debug_assert_eq!(usize::try_from(record_count).ok(), Some(by_key.len()));
        Ok(Self {
            bytes,
            by_key,
            records: record_count,
        })
    }

    #[inline]
    pub fn record_count(&self) -> u32 {
        self.records
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Validate an existing id or intern a raw key.
    pub fn resolve_or_intern(&mut self, key: CompactPubkey) -> Result<u32, SourceV2Error> {
        match key {
            CompactPubkey::Id(id) => self.validate_id(id),
            CompactPubkey::Raw(raw) => {
                if let Some(id) = self.by_key.get(&raw) {
                    return Ok(*id);
                }
                let id = self
                    .record_count()
                    .checked_add(1)
                    .ok_or(SourceV2Error::PubkeyIdExhausted)?;
                self.bytes.try_reserve(PUBKEY_RECORD_LEN).map_err(|error| {
                    SourceV2Error::AllocationFailed {
                        what: "public-key dictionary record",
                        message: error.to_string(),
                    }
                })?;
                self.bytes.extend_from_slice(&raw);
                self.by_key.insert(raw, id);
                self.records = id;
                Ok(id)
            }
        }
    }

    /// Resolve an id or inline key to its exact 32-byte value.
    pub fn resolve_bytes(&self, key: CompactPubkey) -> Result<[u8; 32], SourceV2Error> {
        match key {
            CompactPubkey::Raw(raw) => Ok(raw),
            CompactPubkey::Id(id) => {
                self.validate_id(id)?;
                let zero_based = usize::try_from(id - 1).expect("validated u32 id fits usize");
                let start = zero_based
                    .checked_mul(PUBKEY_RECORD_LEN)
                    .expect("validated registry length fits usize");
                Ok(self.bytes[start..start + PUBKEY_RECORD_LEN]
                    .try_into()
                    .expect("validated public-key registry record"))
            }
        }
    }

    fn validate_id(&self, id: u32) -> Result<u32, SourceV2Error> {
        let records = self.record_count();
        if id == CompactPubkey::RAW_SENTINEL || id > records {
            return Err(SourceV2Error::InvalidRegistryId { id, records });
        }
        Ok(id)
    }
}

/// Return a checked region without lossy integer casts or panics.
pub fn checked_region<'a>(
    bytes: &'a [u8],
    offset: u64,
    len: u64,
    region: &'static str,
    tx_index: u32,
) -> Result<&'a [u8], SourceV2Error> {
    let end = offset
        .checked_add(len)
        .ok_or(SourceV2Error::RegionEndOverflow {
            tx_index,
            region,
            offset,
            len,
        })?;
    let start = usize::try_from(offset).map_err(|_| SourceV2Error::RegionIndexTooLarge {
        tx_index,
        region,
        value: offset,
    })?;
    let end = usize::try_from(end).map_err(|_| SourceV2Error::RegionIndexTooLarge {
        tx_index,
        region,
        value: end,
    })?;
    bytes
        .get(start..end)
        .ok_or(SourceV2Error::RegionOutOfBounds {
            tx_index,
            region,
            offset,
            len,
            available: bytes.len(),
        })
}

/// Decode one structured message from a checked block-local range.
pub fn decode_message(
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
) -> Result<ArchiveV2HotMessagePayload, SourceV2Error> {
    decode_message_with_schema(CompactV2MessageSchema::Current, block, row)
}

/// Decode one structured message with the schema selected for its generation.
pub fn decode_message_with_schema(
    schema: CompactV2MessageSchema,
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
) -> Result<ArchiveV2HotMessagePayload, SourceV2Error> {
    decode_message_lane_with_schema(schema, &block.message_bytes, row)
}

/// Decode one structured message directly from a borrowed block message lane.
///
/// This has the same checks and output as [`decode_message_with_schema`], but
/// it does not require an owned [`ArchiveV2HotBlockBlob`]. It is the source
/// boundary used by a recycled borrowed block worker.
pub fn decode_message_lane_with_schema(
    schema: CompactV2MessageSchema,
    message_bytes: &[u8],
    row: &ArchiveV2HotTxRow,
) -> Result<ArchiveV2HotMessagePayload, SourceV2Error> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return Err(SourceV2Error::RawTransactionFallback {
            tx_index: row.tx_index,
        });
    }
    if row.message_len == 0 {
        return Err(SourceV2Error::EmptyRegion {
            tx_index: row.tx_index,
            region: "message",
        });
    }
    let bytes = checked_region(
        message_bytes,
        u64::from(row.message_offset),
        u64::from(row.message_len),
        "message",
        row.tx_index,
    )?;
    let payload =
        decode_compact_v2_message(schema, bytes).map_err(|error| SourceV2Error::Decode {
            tx_index: row.tx_index,
            region: "message",
            message: error.to_string(),
        })?;
    let flag_is_v0 = row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0;
    let payload_is_v0 = matches!(payload, ArchiveV2HotMessagePayload::V0(_));
    if flag_is_v0 != payload_is_v0 {
        return Err(SourceV2Error::MessageVersionFlagMismatch {
            tx_index: row.tx_index,
            flag_is_v0,
            payload_is_v0,
        });
    }
    Ok(payload)
}

/// Decode one metadata record, and reject raw fallback instead of dropping it.
pub fn decode_metadata(
    schema: CompactV2MetadataSchema,
    block: &ArchiveV2HotBlockBlob,
    row: &ArchiveV2HotTxRow,
) -> Result<Option<CompactMetaV1>, SourceV2Error> {
    decode_metadata_lane(schema, &block.metadata_bytes, row)
}

/// Decode one metadata record directly from a borrowed block metadata lane.
///
/// Raw fallback, flags, ranges and exact decoding are identical to
/// [`decode_metadata`].
pub fn decode_metadata_lane(
    schema: CompactV2MetadataSchema,
    metadata_bytes: &[u8],
    row: &ArchiveV2HotTxRow,
) -> Result<Option<CompactMetaV1>, SourceV2Error> {
    let has_metadata = row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
    if !has_metadata {
        if row.metadata_len != 0 || row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
            return Err(SourceV2Error::MetadataFlagMismatch {
                tx_index: row.tx_index,
                has_metadata,
                metadata_len: row.metadata_len,
            });
        }
        return Ok(None);
    }
    if row.metadata_len == 0 {
        return Err(SourceV2Error::MetadataFlagMismatch {
            tx_index: row.tx_index,
            has_metadata,
            metadata_len: row.metadata_len,
        });
    }
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        return Err(SourceV2Error::RawMetadataFallback {
            tx_index: row.tx_index,
        });
    }
    let bytes = checked_region(
        metadata_bytes,
        u64::from(row.metadata_offset),
        u64::from(row.metadata_len),
        "metadata",
        row.tx_index,
    )?;
    decode_compact_v2_metadata(schema, bytes)
        .map(Some)
        .map_err(|error| SourceV2Error::Decode {
            tx_index: row.tx_index,
            region: "metadata",
            message: error.to_string(),
        })
}

/// Sum the addresses required by signed V0 lookup descriptors.
pub fn expected_loaded_address_counts(
    lookups: &[OwnedCompactAddressTableLookup],
) -> Result<LoadedAddressCounts, SourceV2Error> {
    let mut counts = LoadedAddressCounts::default();
    for (lookup_index, lookup) in lookups.iter().enumerate() {
        if lookup.writable_indexes.is_empty() && lookup.readonly_indexes.is_empty() {
            return Err(SourceV2Error::EmptyAddressTableLookup {
                lookup: lookup_index,
            });
        }
        let writable = u32::try_from(lookup.writable_indexes.len()).map_err(|_| {
            SourceV2Error::LookupCountOverflow {
                kind: LoadedAddressKind::Writable,
            }
        })?;
        counts.writable =
            counts
                .writable
                .checked_add(writable)
                .ok_or(SourceV2Error::LookupCountOverflow {
                    kind: LoadedAddressKind::Writable,
                })?;
        let readonly = u32::try_from(lookup.readonly_indexes.len()).map_err(|_| {
            SourceV2Error::LookupCountOverflow {
                kind: LoadedAddressKind::Readonly,
            }
        })?;
        counts.readonly =
            counts
                .readonly
                .checked_add(readonly)
                .ok_or(SourceV2Error::LookupCountOverflow {
                    kind: LoadedAddressKind::Readonly,
                })?;
    }
    Ok(counts)
}

/// Check descriptor counts against the loaded addresses in runtime metadata.
pub fn validate_v0_loaded_address_counts(
    message: &ArchiveV2HotV0Message,
    loaded: Option<(&[CompactPubkey], &[CompactPubkey])>,
) -> Result<LoadedAddressCounts, SourceV2Error> {
    let expected = expected_loaded_address_counts(&message.address_table_lookups)?;
    let Some((loaded_writable, loaded_readonly)) = loaded else {
        if expected == LoadedAddressCounts::default() {
            return Ok(expected);
        }
        return Err(SourceV2Error::LoadedAddressesUnavailable {
            expected_writable: expected.writable,
            expected_readonly: expected.readonly,
        });
    };
    let actual_writable =
        u32::try_from(loaded_writable.len()).map_err(|_| SourceV2Error::LookupCountOverflow {
            kind: LoadedAddressKind::Writable,
        })?;
    if actual_writable != expected.writable {
        return Err(SourceV2Error::LookupCountMismatch {
            kind: LoadedAddressKind::Writable,
            expected: expected.writable,
            actual: actual_writable,
        });
    }
    let actual_readonly =
        u32::try_from(loaded_readonly.len()).map_err(|_| SourceV2Error::LookupCountOverflow {
            kind: LoadedAddressKind::Readonly,
        })?;
    if actual_readonly != expected.readonly {
        return Err(SourceV2Error::LookupCountMismatch {
            kind: LoadedAddressKind::Readonly,
            expected: expected.readonly,
            actual: actual_readonly,
        });
    }
    Ok(expected)
}

/// Which column of the Compact V2 vote-hash dictionary a reference uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteHashKind {
    Bank,
    BlockId,
}

impl fmt::Display for VoteHashKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bank => f.write_str("bank"),
            Self::BlockId => f.write_str("block-id"),
        }
    }
}

/// Resolve block-local vote hash references used by typed vote instructions.
pub trait VoteHashResolver {
    fn resolve_vote_hash(
        &self,
        block_id: u32,
        kind: VoteHashKind,
    ) -> Result<[u8; 32], SourceV2Error>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct VoteHashRow {
    bank_hash: Option<[u8; 32]>,
    block_id_hash: Option<[u8; 32]>,
}

/// Checked view of `vote_hash_registry.bin`.
#[derive(Debug, Clone)]
pub struct VoteHashRegistry {
    rows: Vec<VoteHashRow>,
}

/// A possible exact wire form for a typed Compact V2 instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstructionDataEncoding {
    Raw,
    ComputeBudget,
    System,
    VoteCompact,
    VoteTowerCanonical,
    VoteTowerHistorical,
}

/// One exact instruction-data candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionDataCandidate {
    pub encoding: InstructionDataEncoding,
    pub bytes: Vec<u8>,
}

/// One resolved V0 lookup descriptor as it appears in signed message bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedAddressTableLookup<'a> {
    pub account_key: [u8; 32],
    pub writable_indexes: &'a [u8],
    pub readonly_indexes: &'a [u8],
}

/// The compute budget a v1 message carries in its header (SIMD-0385).
///
/// Presence is load-bearing: which fields are set is exactly what the config
/// mask encodes, and the values follow in bit order. `Some(0)` and `None` are
/// different messages and hash differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SignedTransactionConfig {
    pub priority_fee: Option<u64>,
    pub compute_unit_limit: Option<u32>,
    pub loaded_accounts_data_size_limit: Option<u32>,
    pub heap_size: Option<u32>,
}

impl SignedTransactionConfig {
    /// The wire config mask. `PRIORITY_FEE` occupies two bits because the value
    /// array is counted in four-byte slots and a priority fee is a `u64`.
    fn mask(&self) -> u32 {
        let mut mask = 0;
        if self.priority_fee.is_some() {
            mask |= V1_CONFIG_PRIORITY_FEE;
        }
        if self.compute_unit_limit.is_some() {
            mask |= V1_CONFIG_COMPUTE_UNIT_LIMIT;
        }
        if self.loaded_accounts_data_size_limit.is_some() {
            mask |= V1_CONFIG_LOADED_ACCOUNTS_DATA_SIZE;
        }
        if self.heap_size.is_some() {
            mask |= V1_CONFIG_HEAP_SIZE;
        }
        mask
    }
}

/// The signed-message envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignedMessageVersion<'a> {
    Legacy,
    V0 {
        address_table_lookups: &'a [ResolvedAddressTableLookup<'a>],
    },
    /// v1 carries no lookup tables; its compute budget rides in the header.
    V1 {
        config: SignedTransactionConfig,
    },
}

/// One instruction whose exact data bytes are known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignedInstruction<'a> {
    pub program_id_index: u8,
    pub accounts: &'a [u8],
    pub data: &'a [u8],
}

/// A resolved Legacy or V0 message ready for canonical serialization.
#[derive(Debug, Clone, Copy)]
pub struct SignedMessage<'a> {
    pub version: SignedMessageVersion<'a>,
    pub header: blockzilla_compact::CompactMessageHeader,
    pub static_account_keys: &'a [[u8; 32]],
    pub recent_blockhash: [u8; 32],
    pub instructions: &'a [SignedInstruction<'a>],
}

/// One instruction with all byte forms that Compact V2 could have erased.
#[derive(Debug, Clone, Copy)]
pub struct SignedInstructionCandidates<'a> {
    pub program_id_index: u8,
    pub accounts: &'a [u8],
    pub data_candidates: &'a [InstructionDataCandidate],
}

/// A resolved message whose instruction bytes still need signature proof.
#[derive(Debug, Clone, Copy)]
pub struct SignedMessageCandidates<'a> {
    pub version: SignedMessageVersion<'a>,
    pub header: blockzilla_compact::CompactMessageHeader,
    pub static_account_keys: &'a [[u8; 32]],
    pub recent_blockhash: [u8; 32],
    pub instructions: &'a [SignedInstructionCandidates<'a>],
}

/// The unique instruction bytes and whole message accepted by a verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedSignedMessage {
    pub instruction_data: Vec<Vec<u8>>,
    pub signed_message: Vec<u8>,
}

/// Serialize the canonical Solana bytes covered by transaction signatures.
///
/// All public keys and the recent blockhash must already be resolved to their
/// exact 32-byte values. This function does not read a dictionary and does not
/// use runtime-loaded addresses. V0 lookup descriptors are signed, while the
/// addresses that they load are not part of the signed message.
pub fn serialize_signed_message(message: &SignedMessage<'_>) -> Result<Vec<u8>, SourceV2Error> {
    validate_message_header(message)?;
    let account_count = message_account_count(message)?;

    // v1 is not legacy-with-extras: the config mask and its value slots sit
    // between the header and the addresses, the counts are plain bytes rather
    // than ShortU16, and the instruction headers are separated from their
    // payloads. It gets its own pass.
    if let SignedMessageVersion::V1 { config } = message.version {
        return serialize_signed_v1_message(message, &config, account_count);
    }

    let mut out = Vec::new();
    if matches!(message.version, SignedMessageVersion::V0 { .. }) {
        out.push(VERSIONED_MESSAGE_PREFIX | V0_MESSAGE_VERSION);
    }
    out.extend_from_slice(&[
        message.header.num_required_signatures,
        message.header.num_readonly_signed_accounts,
        message.header.num_readonly_unsigned_accounts,
    ]);
    push_message_short_vec_len(
        &mut out,
        message.static_account_keys.len(),
        "static account keys",
    )?;
    for key in message.static_account_keys {
        out.extend_from_slice(key);
    }
    out.extend_from_slice(&message.recent_blockhash);

    push_message_short_vec_len(&mut out, message.instructions.len(), "instructions")?;
    let is_v0 = matches!(message.version, SignedMessageVersion::V0 { .. });
    for (instruction_index, instruction) in message.instructions.iter().enumerate() {
        validate_instruction_indices(
            instruction_index,
            instruction,
            account_count,
            message.static_account_keys.len(),
            is_v0,
        )?;
        out.push(instruction.program_id_index);
        push_message_short_vec_len(&mut out, instruction.accounts.len(), "instruction accounts")?;
        out.extend_from_slice(instruction.accounts);
        push_message_short_vec_len(&mut out, instruction.data.len(), "instruction data")?;
        out.extend_from_slice(instruction.data);
    }

    if let SignedMessageVersion::V0 {
        address_table_lookups,
    } = message.version
    {
        push_message_short_vec_len(
            &mut out,
            address_table_lookups.len(),
            "address-table lookups",
        )?;
        for lookup in address_table_lookups {
            out.extend_from_slice(&lookup.account_key);
            push_message_short_vec_len(
                &mut out,
                lookup.writable_indexes.len(),
                "writable lookup indexes",
            )?;
            out.extend_from_slice(lookup.writable_indexes);
            push_message_short_vec_len(
                &mut out,
                lookup.readonly_indexes.len(),
                "readonly lookup indexes",
            )?;
            out.extend_from_slice(lookup.readonly_indexes);
        }
    }

    Ok(out)
}

/// Serialize a v1 message exactly as SIMD-0385 defines it.
///
/// Layout, after the `0x81` prefix: the three-byte legacy header, the config
/// mask, the lifetime specifier, the instruction and address counts, the
/// addresses, then the config values in bit order, then every instruction
/// header, then every instruction payload. Headers precede payloads, so the
/// two are written in separate passes.
fn serialize_signed_v1_message(
    message: &SignedMessage<'_>,
    config: &SignedTransactionConfig,
    account_count: usize,
) -> Result<Vec<u8>, SourceV2Error> {
    let instruction_count = u8::try_from(message.instructions.len()).map_err(|_| {
        SourceV2Error::ShortVecLengthTooLarge {
            field: "v1 instructions",
            actual: message.instructions.len(),
        }
    })?;
    let address_count = u8::try_from(message.static_account_keys.len()).map_err(|_| {
        SourceV2Error::ShortVecLengthTooLarge {
            field: "v1 addresses",
            actual: message.static_account_keys.len(),
        }
    })?;
    if instruction_count > V1_MAX_INSTRUCTIONS {
        return Err(SourceV2Error::ShortVecLengthTooLarge {
            field: "v1 instructions",
            actual: message.instructions.len(),
        });
    }
    if address_count > V1_MAX_ADDRESSES {
        return Err(SourceV2Error::ShortVecLengthTooLarge {
            field: "v1 addresses",
            actual: message.static_account_keys.len(),
        });
    }

    let mut out = Vec::new();
    out.push(VERSIONED_MESSAGE_PREFIX | V1_MESSAGE_VERSION);
    out.extend_from_slice(&[
        message.header.num_required_signatures,
        message.header.num_readonly_signed_accounts,
        message.header.num_readonly_unsigned_accounts,
    ]);
    out.extend_from_slice(&config.mask().to_le_bytes());
    out.extend_from_slice(&message.recent_blockhash);
    out.push(instruction_count);
    out.push(address_count);

    for key in message.static_account_keys {
        out.extend_from_slice(key);
    }

    // Bit order, so the mask alone tells a reader how many slots follow.
    if let Some(priority_fee) = config.priority_fee {
        out.extend_from_slice(&priority_fee.to_le_bytes());
    }
    if let Some(compute_unit_limit) = config.compute_unit_limit {
        out.extend_from_slice(&compute_unit_limit.to_le_bytes());
    }
    if let Some(limit) = config.loaded_accounts_data_size_limit {
        out.extend_from_slice(&limit.to_le_bytes());
    }
    if let Some(heap_size) = config.heap_size {
        out.extend_from_slice(&heap_size.to_le_bytes());
    }

    for (instruction_index, instruction) in message.instructions.iter().enumerate() {
        // v1 resolves no addresses from lookup tables, so every index must fall
        // inside the static set — the same rule legacy follows.
        validate_instruction_indices(
            instruction_index,
            instruction,
            account_count,
            message.static_account_keys.len(),
            false,
        )?;
        let num_accounts = u8::try_from(instruction.accounts.len()).map_err(|_| {
            SourceV2Error::ShortVecLengthTooLarge {
                field: "v1 instruction accounts",
                actual: instruction.accounts.len(),
            }
        })?;
        let data_len = u16::try_from(instruction.data.len()).map_err(|_| {
            SourceV2Error::ShortVecLengthTooLarge {
                field: "v1 instruction data",
                actual: instruction.data.len(),
            }
        })?;
        out.push(instruction.program_id_index);
        out.push(num_accounts);
        out.extend_from_slice(&data_len.to_le_bytes());
    }

    for instruction in message.instructions {
        out.extend_from_slice(instruction.accounts);
        out.extend_from_slice(instruction.data);
    }

    Ok(out)
}

/// Select the only whole-message candidate accepted by `verify`.
///
/// Candidate combinations are enumerated in instruction order and candidate
/// order. The function calculates the full combination count before it calls
/// `verify`. It does no work if that count exceeds `max_combinations`, and the
/// caller cannot raise the limit above
/// [`MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS`].
pub fn select_signed_message_candidate<F>(
    message: &SignedMessageCandidates<'_>,
    max_combinations: usize,
    mut verify: F,
) -> Result<SelectedSignedMessage, SourceV2Error>
where
    F: FnMut(&[u8]) -> bool,
{
    if max_combinations == 0 || max_combinations > MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS {
        return Err(SourceV2Error::InvalidCandidateCombinationLimit {
            requested: max_combinations,
            hard_maximum: MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS,
        });
    }

    let mut combination_count = 1_usize;
    for (instruction, candidates) in message.instructions.iter().enumerate() {
        if candidates.data_candidates.is_empty() {
            return Err(SourceV2Error::EmptyInstructionCandidates { instruction });
        }
        combination_count = combination_count
            .checked_mul(candidates.data_candidates.len())
            .filter(|count| *count <= max_combinations)
            .ok_or(SourceV2Error::CandidateCombinationLimitExceeded {
                maximum: max_combinations,
            })?;
    }

    let mut choice_indices = vec![0_usize; message.instructions.len()];
    let mut selected: Option<SelectedSignedMessage> = None;
    for ordinal in 0..combination_count {
        let instructions = message
            .instructions
            .iter()
            .zip(&choice_indices)
            .map(|(instruction, choice)| SignedInstruction {
                program_id_index: instruction.program_id_index,
                accounts: instruction.accounts,
                data: &instruction.data_candidates[*choice].bytes,
            })
            .collect::<Vec<_>>();
        let signed_message = serialize_signed_message(&SignedMessage {
            version: message.version,
            header: message.header,
            static_account_keys: message.static_account_keys,
            recent_blockhash: message.recent_blockhash,
            instructions: &instructions,
        })?;

        if verify(&signed_message) {
            let instruction_data = instructions
                .iter()
                .map(|instruction| instruction.data.to_vec())
                .collect::<Vec<_>>();
            let candidate = SelectedSignedMessage {
                instruction_data,
                signed_message,
            };
            if let Some(previous) = &selected {
                // Different encoding labels can still hold the same bytes. A
                // byte-identical whole message is one candidate, not two.
                if previous != &candidate {
                    return Err(SourceV2Error::MultipleVerifiedMessageCandidates);
                }
            } else {
                selected = Some(candidate);
            }
        }

        if ordinal + 1 < combination_count {
            increment_candidate_choices(&mut choice_indices, message.instructions);
        }
    }

    selected.ok_or(SourceV2Error::NoVerifiedMessageCandidate)
}

fn validate_message_header(message: &SignedMessage<'_>) -> Result<(), SourceV2Error> {
    let required = message.header.num_required_signatures;
    if matches!(message.version, SignedMessageVersion::Legacy)
        && required & VERSIONED_MESSAGE_PREFIX != 0
    {
        return Err(SourceV2Error::LegacyRequiredSignaturesSetVersionBit { required });
    }
    if usize::from(required) > message.static_account_keys.len() {
        return Err(SourceV2Error::RequiredSignaturesExceedStaticKeys {
            required,
            static_keys: message.static_account_keys.len(),
        });
    }
    let readonly_signed = message.header.num_readonly_signed_accounts;
    if readonly_signed > required {
        return Err(SourceV2Error::ReadonlySignedExceedRequired {
            readonly: readonly_signed,
            required,
        });
    }
    if readonly_signed == required {
        return Err(SourceV2Error::NoWritableFeePayer {
            readonly: readonly_signed,
            required,
        });
    }
    let unsigned_static = message
        .static_account_keys
        .len()
        .saturating_sub(usize::from(required));
    let readonly_unsigned = message.header.num_readonly_unsigned_accounts;
    if usize::from(readonly_unsigned) > unsigned_static {
        return Err(SourceV2Error::ReadonlyUnsignedExceedUnsignedStatic {
            readonly: readonly_unsigned,
            unsigned_static,
        });
    }
    Ok(())
}

fn message_account_count(message: &SignedMessage<'_>) -> Result<usize, SourceV2Error> {
    let loaded = match message.version {
        SignedMessageVersion::Legacy | SignedMessageVersion::V1 { .. } => 0,
        SignedMessageVersion::V0 {
            address_table_lookups,
        } => address_table_lookups.iter().enumerate().try_fold(
            0_usize,
            |count, (lookup_index, lookup)| {
                if lookup.writable_indexes.is_empty() && lookup.readonly_indexes.is_empty() {
                    return Err(SourceV2Error::EmptyAddressTableLookup {
                        lookup: lookup_index,
                    });
                }
                count
                    .checked_add(lookup.writable_indexes.len())
                    .and_then(|count| count.checked_add(lookup.readonly_indexes.len()))
                    .ok_or(SourceV2Error::MessageAccountCountOverflow)
            },
        )?,
    };
    let account_count = message
        .static_account_keys
        .len()
        .checked_add(loaded)
        .ok_or(SourceV2Error::MessageAccountCountOverflow)?;
    if account_count > usize::from(u8::MAX) + 1 {
        return Err(SourceV2Error::MessageAccountCountExceedsIndexRange {
            actual: account_count,
        });
    }
    Ok(account_count)
}

fn validate_instruction_indices(
    instruction_index: usize,
    instruction: &SignedInstruction<'_>,
    account_count: usize,
    static_account_count: usize,
    is_v0: bool,
) -> Result<(), SourceV2Error> {
    if usize::from(instruction.program_id_index) >= account_count {
        return Err(SourceV2Error::MessageAccountIndexOutOfBounds {
            instruction: instruction_index,
            field: "program",
            index: instruction.program_id_index,
            account_count,
        });
    }
    if instruction.program_id_index == 0 {
        return Err(SourceV2Error::ProgramIdIsFeePayer {
            instruction: instruction_index,
        });
    }
    if is_v0 && usize::from(instruction.program_id_index) >= static_account_count {
        return Err(SourceV2Error::V0ProgramIdIsLoaded {
            instruction: instruction_index,
            index: instruction.program_id_index,
            static_account_count,
        });
    }
    for &index in instruction.accounts {
        if usize::from(index) >= account_count {
            return Err(SourceV2Error::MessageAccountIndexOutOfBounds {
                instruction: instruction_index,
                field: "account",
                index,
                account_count,
            });
        }
    }
    Ok(())
}

fn push_message_short_vec_len(
    out: &mut Vec<u8>,
    len: usize,
    field: &'static str,
) -> Result<(), SourceV2Error> {
    let mut value = u16::try_from(len)
        .map_err(|_| SourceV2Error::ShortVecLengthTooLarge { field, actual: len })?;
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return Ok(());
        }
    }
}

fn increment_candidate_choices(
    choices: &mut [usize],
    instructions: &[SignedInstructionCandidates<'_>],
) {
    for (choice, instruction) in choices.iter_mut().zip(instructions).rev() {
        *choice += 1;
        if *choice < instruction.data_candidates.len() {
            return;
        }
        *choice = 0;
    }
    unreachable!("the caller increments only before the final combination");
}

impl VoteHashRegistry {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SourceV2Error> {
        if !bytes.len().is_multiple_of(VOTE_HASH_RECORD_LEN) {
            return Err(SourceV2Error::InvalidVoteHashRegistryLength {
                actual: bytes.len(),
            });
        }
        let mut rows = Vec::with_capacity(bytes.len() / VOTE_HASH_RECORD_LEN);
        for (block_id, chunk) in bytes.chunks_exact(VOTE_HASH_RECORD_LEN).enumerate() {
            let block_id =
                u32::try_from(block_id).map_err(|_| SourceV2Error::VoteHashRegistryTooLarge {
                    records: bytes.len() / VOTE_HASH_RECORD_LEN,
                })?;
            let flags = chunk[0];
            if flags & !0b11 != 0 {
                return Err(SourceV2Error::InvalidVoteHashRegistryFlags { block_id, flags });
            }
            let bank_hash = (flags & 1 != 0).then(|| {
                chunk[1..33]
                    .try_into()
                    .expect("checked vote bank-hash record")
            });
            let block_id_hash = (flags & 2 != 0).then(|| {
                chunk[33..65]
                    .try_into()
                    .expect("checked vote block-id-hash record")
            });
            rows.push(VoteHashRow {
                bank_hash,
                block_id_hash,
            });
        }
        Ok(Self { rows })
    }
}

impl VoteHashResolver for VoteHashRegistry {
    fn resolve_vote_hash(
        &self,
        block_id: u32,
        kind: VoteHashKind,
    ) -> Result<[u8; 32], SourceV2Error> {
        let row = usize::try_from(block_id)
            .ok()
            .and_then(|index| self.rows.get(index))
            .ok_or(SourceV2Error::MissingVoteHash { block_id, kind })?;
        let value = match kind {
            VoteHashKind::Bank => row.bank_hash,
            VoteHashKind::BlockId => row.block_id_hash,
        };
        value.ok_or(SourceV2Error::MissingVoteHash { block_id, kind })
    }
}

/// Reconstruct the exact on-chain bytes of an unambiguous Compact V2
/// top-level instruction.
///
/// The three raw variants pass through unchanged. Compute-budget and System
/// variants use their canonical wire forms. Typed vote variants resolve only
/// the hashes that Compact V2 deliberately moved to `vote_hash_registry.bin`.
///
/// Compact V2 maps both the canonical and historical TowerSync wire forms to
/// the same value. This function rejects those ambiguous values. Use
/// [`reconstruct_instruction_data_candidates`] and a transaction signature to
/// select the correct candidate.
pub fn reconstruct_instruction_data(
    data: &ArchiveV2HotInstructionData,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<u8>, SourceV2Error> {
    let mut candidates = reconstruct_instruction_data_candidates(data, vote_hashes)?;
    if candidates.len() != 1 {
        return Err(SourceV2Error::AmbiguousInstructionEncoding {
            candidates: candidates.len(),
        });
    }
    Ok(candidates.pop().expect("one checked candidate").bytes)
}

/// Return all possible exact on-chain byte forms retained by Compact V2.
///
/// TowerSync values have two candidates because the source parser accepted a
/// historical bincode form and then erased the form marker. The ordered
/// converter must rebuild the signed message with each candidate and use the
/// fee-payer signature as the oracle. It must not select a form by preference.
pub fn reconstruct_instruction_data_candidates(
    data: &ArchiveV2HotInstructionData,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<InstructionDataCandidate>, SourceV2Error> {
    match data {
        ArchiveV2HotInstructionData::Raw(bytes)
        | ArchiveV2HotInstructionData::UnknownSystem(bytes)
        | ArchiveV2HotInstructionData::UnknownVote(bytes) => Ok(vec![InstructionDataCandidate {
            encoding: InstructionDataEncoding::Raw,
            bytes: bytes.clone(),
        }]),
        ArchiveV2HotInstructionData::ComputeBudget(value) => {
            let mut out = Vec::with_capacity(9);
            match value {
                ArchiveV2ComputeBudgetInstructionData::Unused => out.push(0),
                ArchiveV2ComputeBudgetInstructionData::RequestHeapFrame(bytes) => {
                    out.push(1);
                    out.extend_from_slice(&bytes.to_le_bytes());
                }
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(units) => {
                    out.push(2);
                    out.extend_from_slice(&units.to_le_bytes());
                }
                ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(price) => {
                    out.push(3);
                    out.extend_from_slice(&price.to_le_bytes());
                }
                ArchiveV2ComputeBudgetInstructionData::SetLoadedAccountsDataSizeLimit(bytes) => {
                    out.push(4);
                    out.extend_from_slice(&bytes.to_le_bytes());
                }
            }
            Ok(vec![InstructionDataCandidate {
                encoding: InstructionDataEncoding::ComputeBudget,
                bytes: out,
            }])
        }
        ArchiveV2HotInstructionData::System(value) => Ok(vec![InstructionDataCandidate {
            encoding: InstructionDataEncoding::System,
            bytes: system_instruction_bytes(value)?,
        }]),
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(update) => {
            Ok(vec![InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteCompact,
                bytes: vote_update_instruction_bytes(12, update, vote_hashes)?,
            }])
        }
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
            update,
            switch_proof_hash,
        } => {
            let mut out = vote_update_instruction_bytes(13, update, vote_hashes)?;
            out.extend_from_slice(&resolve_aux_hash(*switch_proof_hash)?);
            Ok(vec![InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteCompact,
                bytes: out,
            }])
        }
        ArchiveV2HotInstructionData::VoteTowerSync(tower) => {
            tower_candidates(14, tower, None, vote_hashes)
        }
        ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
            tower,
            switch_proof_hash,
        } => tower_candidates(15, tower, Some(*switch_proof_hash), vote_hashes),
    }
}

fn tower_candidates(
    variant: u32,
    tower: &ArchiveV2VoteTowerSync,
    switch_proof_hash: Option<ArchiveV2VoteHashRef>,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<InstructionDataCandidate>, SourceV2Error> {
    let switch_proof_hash = switch_proof_hash.map(resolve_aux_hash).transpose()?;
    let mut canonical = vote_tower_sync_instruction_bytes(variant, tower, vote_hashes)?;
    if let Some(hash) = switch_proof_hash {
        canonical.extend_from_slice(&hash);
    }
    let mut historical = historical_tower_sync_instruction_bytes(variant, tower, vote_hashes)?;
    if let Some(hash) = switch_proof_hash {
        historical.extend_from_slice(&hash);
    }
    let mut candidates = vec![InstructionDataCandidate {
        encoding: InstructionDataEncoding::VoteTowerCanonical,
        bytes: canonical,
    }];
    if historical != candidates[0].bytes {
        candidates.push(InstructionDataCandidate {
            encoding: InstructionDataEncoding::VoteTowerHistorical,
            bytes: historical,
        });
    }
    Ok(candidates)
}

fn resolve_vote_hash_ref(
    value: ArchiveV2VoteHashRef,
    kind: VoteHashKind,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<[u8; 32], SourceV2Error> {
    match value {
        ArchiveV2VoteHashRef::Zero => Ok([0; 32]),
        ArchiveV2VoteHashRef::Raw(hash) => Ok(hash),
        ArchiveV2VoteHashRef::Block(block_id) => vote_hashes
            .ok_or(SourceV2Error::MissingVoteHashResolver { block_id, kind })?
            .resolve_vote_hash(block_id, kind),
    }
}

fn resolve_aux_hash(value: ArchiveV2VoteHashRef) -> Result<[u8; 32], SourceV2Error> {
    match value {
        ArchiveV2VoteHashRef::Zero => Ok([0; 32]),
        ArchiveV2VoteHashRef::Raw(hash) => Ok(hash),
        ArchiveV2VoteHashRef::Block(block_id) => {
            Err(SourceV2Error::AuxiliaryVoteHashBlockReference { block_id })
        }
    }
}

fn vote_update_instruction_bytes(
    variant: u32,
    update: &ArchiveV2VoteStateUpdate,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<u8>, SourceV2Error> {
    let mut out = Vec::with_capacity(128);
    push_u32_le(&mut out, variant);
    push_u64_le(&mut out, update.root.unwrap_or(u64::MAX));
    push_short_vec_len(&mut out, update.lockout_offsets.len())?;
    for lockout in &update.lockout_offsets {
        push_var_u64(&mut out, lockout.offset);
        out.push(lockout.confirmation_count);
    }
    out.extend_from_slice(&resolve_vote_hash_ref(
        update.hash,
        VoteHashKind::Bank,
        vote_hashes,
    )?);
    push_option_i64(&mut out, update.timestamp);
    Ok(out)
}

fn vote_tower_sync_instruction_bytes(
    variant: u32,
    tower: &ArchiveV2VoteTowerSync,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<u8>, SourceV2Error> {
    let mut out = vote_update_instruction_bytes(variant, &tower.update, vote_hashes)?;
    out.extend_from_slice(&resolve_vote_hash_ref(
        tower.block_id_hash,
        VoteHashKind::BlockId,
        vote_hashes,
    )?);
    Ok(out)
}

fn historical_tower_sync_instruction_bytes(
    variant: u32,
    tower: &ArchiveV2VoteTowerSync,
    vote_hashes: Option<&dyn VoteHashResolver>,
) -> Result<Vec<u8>, SourceV2Error> {
    let mut out = Vec::with_capacity(160);
    push_u32_le(&mut out, variant);
    let lockout_count = u64::try_from(tower.update.lockout_offsets.len()).map_err(|_| {
        SourceV2Error::VoteLockoutCountTooLarge {
            actual: tower.update.lockout_offsets.len(),
        }
    })?;
    push_u64_le(&mut out, lockout_count);
    let mut slot = tower.update.root.unwrap_or_default();
    for lockout in &tower.update.lockout_offsets {
        slot = slot
            .checked_add(lockout.offset)
            .ok_or(SourceV2Error::VoteLockoutSlotOverflow {
                previous: slot,
                offset: lockout.offset,
            })?;
        push_u64_le(&mut out, slot);
        push_u32_le(&mut out, u32::from(lockout.confirmation_count));
    }
    push_option_u64(&mut out, tower.update.root);
    out.extend_from_slice(&resolve_vote_hash_ref(
        tower.update.hash,
        VoteHashKind::Bank,
        vote_hashes,
    )?);
    push_option_i64(&mut out, tower.update.timestamp);
    out.extend_from_slice(&resolve_vote_hash_ref(
        tower.block_id_hash,
        VoteHashKind::BlockId,
        vote_hashes,
    )?);
    Ok(out)
}

fn push_short_vec_len(out: &mut Vec<u8>, mut len: usize) -> Result<(), SourceV2Error> {
    if len > usize::from(u16::MAX) {
        return Err(SourceV2Error::VoteLockoutCountTooLarge { actual: len });
    }
    loop {
        let mut byte = (len & 0x7f) as u8;
        len >>= 7;
        if len != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            return Ok(());
        }
    }
}

fn push_var_u64(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn push_option_i64(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn push_option_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn system_instruction_bytes(
    data: &ArchiveV2SystemInstructionData,
) -> Result<Vec<u8>, SourceV2Error> {
    use ArchiveV2SystemInstructionData as SystemIx;

    let mut out = Vec::with_capacity(96);
    match data {
        SystemIx::CreateAccount {
            lamports,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 0);
            push_u64_le(&mut out, *lamports);
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
        SystemIx::Assign { owner } => {
            push_u32_le(&mut out, 1);
            out.extend_from_slice(owner);
        }
        SystemIx::Transfer { lamports } => {
            push_u32_le(&mut out, 2);
            push_u64_le(&mut out, *lamports);
        }
        SystemIx::CreateAccountWithSeed {
            base,
            seed,
            lamports,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 3);
            out.extend_from_slice(base);
            push_system_seed(&mut out, seed)?;
            push_u64_le(&mut out, *lamports);
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
        SystemIx::AdvanceNonceAccount => push_u32_le(&mut out, 4),
        SystemIx::WithdrawNonceAccount { lamports } => {
            push_u32_le(&mut out, 5);
            push_u64_le(&mut out, *lamports);
        }
        SystemIx::InitializeNonceAccount { authority } => {
            push_u32_le(&mut out, 6);
            out.extend_from_slice(authority);
        }
        SystemIx::AuthorizeNonceAccount { authority } => {
            push_u32_le(&mut out, 7);
            out.extend_from_slice(authority);
        }
        SystemIx::Allocate { space } => {
            push_u32_le(&mut out, 8);
            push_u64_le(&mut out, *space);
        }
        SystemIx::AllocateWithSeed {
            base,
            seed,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 9);
            out.extend_from_slice(base);
            push_system_seed(&mut out, seed)?;
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
        SystemIx::AssignWithSeed { base, seed, owner } => {
            push_u32_le(&mut out, 10);
            out.extend_from_slice(base);
            push_system_seed(&mut out, seed)?;
            out.extend_from_slice(owner);
        }
        SystemIx::TransferWithSeed {
            lamports,
            from_seed,
            from_owner,
        } => {
            push_u32_le(&mut out, 11);
            push_u64_le(&mut out, *lamports);
            push_system_seed(&mut out, from_seed)?;
            out.extend_from_slice(from_owner);
        }
        SystemIx::UpgradeNonceAccount => push_u32_le(&mut out, 12),
        SystemIx::CreateAccountAllowPrefund {
            lamports,
            space,
            owner,
        } => {
            push_u32_le(&mut out, 13);
            push_u64_le(&mut out, *lamports);
            push_u64_le(&mut out, *space);
            out.extend_from_slice(owner);
        }
    }
    Ok(out)
}

#[inline]
fn push_u32_le(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[inline]
fn push_u64_le(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_system_seed(out: &mut Vec<u8>, seed: &str) -> Result<(), SourceV2Error> {
    let len = u64::try_from(seed.len())
        .map_err(|_| SourceV2Error::SystemSeedTooLong { actual: seed.len() })?;
    push_u64_le(out, len);
    out.extend_from_slice(seed.as_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_archive_v2::{
        ArchiveV2HotBlockHeader, ArchiveV2HotLegacyMessage, ArchiveV2VoteLockoutOffset,
    };
    use blockzilla_compact::{
        CompactLogStream, CompactMessageHeader, DataTable, LogEvent, OwnedCompactRecentBlockhash,
        render_logs,
    };
    use blockzilla_primitives::{StringTable, wincode_leb128_config};
    use blockzilla_program_logs::program_logs::ProgramLog;
    use blockzilla_registry::KeyStore;
    use std::io::Write;

    /// Freeze the v1 canonical message bytes against a hand-built vector.
    ///
    /// The SDK's only canonical v1 serializer lives behind its wincode feature,
    /// which pins wincode 0.5 and cannot coexist with this workspace's 0.6.1 —
    /// its serde path is explicitly *not* the wire format. So the layout is
    /// asserted directly from SIMD-0385 rather than against a library.
    #[test]
    fn v1_signed_message_bytes_match_simd_0385_layout() {
        let keys = [[1u8; 32], [2u8; 32]];
        let accounts = [0u8];
        let data = [0xAA, 0xBB, 0xCC];
        let instructions = [SignedInstruction {
            program_id_index: 1,
            accounts: &accounts,
            data: &data,
        }];
        let message = SignedMessage {
            version: SignedMessageVersion::V1 {
                config: SignedTransactionConfig {
                    priority_fee: Some(42),
                    compute_unit_limit: Some(1_000),
                    loaded_accounts_data_size_limit: None,
                    heap_size: None,
                },
            },
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            static_account_keys: &keys,
            recent_blockhash: [9u8; 32],
            instructions: &instructions,
        };

        let mut want = Vec::new();
        want.push(0x81); // versioned prefix | version 1
        want.extend_from_slice(&[1, 0, 1]); // legacy header
        // priority fee occupies two bits, compute unit limit the third.
        want.extend_from_slice(&0b111u32.to_le_bytes());
        want.extend_from_slice(&[9u8; 32]); // lifetime specifier
        want.push(1); // instruction count, a plain byte in v1
        want.push(2); // address count
        want.extend_from_slice(&[1u8; 32]);
        want.extend_from_slice(&[2u8; 32]);
        want.extend_from_slice(&42u64.to_le_bytes()); // config values, bit order
        want.extend_from_slice(&1_000u32.to_le_bytes());
        want.extend_from_slice(&[1, 1, 3, 0]); // program index, accounts, data len
        want.extend_from_slice(&accounts); // payloads follow every header
        want.extend_from_slice(&data);

        let got = serialize_signed_message(&message).expect("v1 message serializes");
        assert_eq!(got, want);

        // An unset field must not occupy a slot, or every later value shifts.
        let mut bare = message;
        bare.version = SignedMessageVersion::V1 {
            config: SignedTransactionConfig::default(),
        };
        let bare_bytes = serialize_signed_message(&bare).expect("v1 message serializes");
        assert_eq!(&bare_bytes[4..8], &0u32.to_le_bytes());
        assert_eq!(bare_bytes.len(), want.len() - 12);
    }

    fn empty_header() -> ArchiveV2HotBlockHeader {
        ArchiveV2HotBlockHeader {
            slot: 7,
            parent_slot: 6,
            blockhash_id: 0,
            previous_blockhash_id: 0,
            block_time: None,
            block_height: None,
            rewards: None,
        }
    }

    fn row(flags: u32, message_len: u32, metadata_len: u32) -> ArchiveV2HotTxRow {
        ArchiveV2HotTxRow {
            tx_index: 3,
            flags,
            message_offset: 0,
            message_len,
            metadata_offset: 0,
            metadata_len,
            signature_count: 1,
            reserved: [0; 3],
        }
    }

    fn legacy_message() -> ArchiveV2HotMessagePayload {
        ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Raw([1; 32])],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([2; 32]),
            instructions: Vec::new(),
        })
    }

    fn empty_block(message_bytes: Vec<u8>, metadata_bytes: Vec<u8>) -> ArchiveV2HotBlockBlob {
        ArchiveV2HotBlockBlob {
            header: empty_header(),
            tx_count: 1,
            tx_rows: Vec::new(),
            message_bytes,
            metadata_bytes,
        }
    }

    #[test]
    fn pubkey_dictionary_is_one_based_and_interns_raw_keys_once() {
        let mut dictionary = PubkeyDictionary::from_bytes(vec![1; 32]).unwrap();
        assert_eq!(dictionary.resolve_or_intern(CompactPubkey::Id(1)), Ok(1));
        assert_eq!(
            dictionary.resolve_or_intern(CompactPubkey::Raw([1; 32])),
            Ok(1)
        );
        assert_eq!(
            dictionary.resolve_or_intern(CompactPubkey::Raw([2; 32])),
            Ok(2)
        );
        assert_eq!(
            dictionary.resolve_or_intern(CompactPubkey::Raw([2; 32])),
            Ok(2)
        );
        assert_eq!(dictionary.record_count(), 2);
        assert_eq!(dictionary.bytes().len(), 64);
        assert_eq!(dictionary.resolve_bytes(CompactPubkey::Id(2)), Ok([2; 32]));
    }

    #[test]
    fn pubkey_dictionary_rejects_zero_out_of_range_and_duplicates() {
        let mut dictionary = PubkeyDictionary::from_bytes(vec![1; 32]).unwrap();
        assert_eq!(
            dictionary.resolve_or_intern(CompactPubkey::Id(0)),
            Err(SourceV2Error::InvalidRegistryId { id: 0, records: 1 })
        );
        assert_eq!(
            dictionary.resolve_or_intern(CompactPubkey::Id(2)),
            Err(SourceV2Error::InvalidRegistryId { id: 2, records: 1 })
        );
        assert!(matches!(
            PubkeyDictionary::from_bytes(vec![7; 64]),
            Err(SourceV2Error::DuplicateRegistryPubkey {
                first_id: 1,
                duplicate_id: 2
            })
        ));
        assert!(matches!(
            PubkeyDictionary::from_bytes(vec![0; 31]),
            Err(SourceV2Error::InvalidPubkeyRegistryLength { actual: 31 })
        ));
    }

    #[test]
    fn pinned_pubkey_resolver_renders_logs_like_key_store() {
        let keys = [[7_u8; 32], [9_u8; 32]];
        let directory = tempfile::tempdir().unwrap();
        let registry_path = directory.path().join("registry.bin");
        let mut registry = File::create(&registry_path).unwrap();
        for key in keys {
            registry.write_all(&key).unwrap();
        }
        registry.sync_all().unwrap();
        drop(registry);

        let source = PinnedLocalRangeSource::new(directory.path());
        let resolver = PinnedPubkeyResolver::open(&source, "registry.bin")
            .unwrap()
            .unwrap();
        assert_eq!(resolver.record_count(), 2);
        assert_eq!(resolver.resolve_pubkey(0), None);
        assert_eq!(resolver.resolve_pubkey(1), Some(keys[0]));
        assert_eq!(resolver.resolve_pubkey(2), Some(keys[1]));
        assert_eq!(resolver.resolve_pubkey(3), None);

        let mut strings = StringTable::default();
        let error = strings.push("test error");
        let logs = CompactLogStream {
            events: vec![
                LogEvent::Invoke {
                    program: CompactPubkey::Id(1),
                    depth: 1,
                },
                LogEvent::System(
                    blockzilla_program_logs::program_logs::system_program::SystemProgramLog::TransferFromMustSign {
                        from: CompactPubkey::Id(2),
                    },
                ),
                LogEvent::ProgramLog(ProgramLog::Token2022(
                    blockzilla_program_logs::program_logs::token_2022::Token2022Log::ErrorHarvestingFrom {
                        account_key: CompactPubkey::Id(2),
                        error,
                    },
                )),
                LogEvent::Success {
                    program: CompactPubkey::Raw([5_u8; 32]),
                },
            ],
            strings,
            data: DataTable::default(),
        };
        let store = KeyStore {
            keys: keys.to_vec(),
        };

        assert_eq!(render_logs(&logs, &resolver), render_logs(&logs, &store));
    }

    #[test]
    fn checked_region_rejects_overflow_and_out_of_bounds() {
        assert_eq!(
            checked_region(&[1, 2, 3], 1, 2, "message", 9),
            Ok(&[2, 3][..])
        );
        assert!(matches!(
            checked_region(&[1, 2, 3], 2, 2, "message", 9),
            Err(SourceV2Error::RegionOutOfBounds { .. })
        ));
        assert!(matches!(
            checked_region(&[], u64::MAX, 1, "message", 9),
            Err(SourceV2Error::RegionEndOverflow { .. })
        ));
    }

    #[test]
    fn message_decode_uses_checked_exact_range_and_version_flag() {
        let bytes = wincode::config::serialize(&legacy_message(), wincode_leb128_config()).unwrap();
        let block = empty_block(bytes.clone(), Vec::new());
        let good_row = row(0, bytes.len() as u32, 0);
        let owned = decode_message(&block, &good_row).unwrap();
        let borrowed_lane =
            decode_message_lane_with_schema(CompactV2MessageSchema::Current, &bytes, &good_row)
                .unwrap();
        assert!(matches!(owned, ArchiveV2HotMessagePayload::Legacy(_)));
        assert_eq!(
            wincode::config::serialize(&owned, wincode_leb128_config()).unwrap(),
            wincode::config::serialize(&borrowed_lane, wincode_leb128_config()).unwrap(),
        );

        let wrong_version = row(ARCHIVE_V2_TX_FLAG_MESSAGE_V0, bytes.len() as u32, 0);
        assert!(matches!(
            decode_message(&block, &wrong_version),
            Err(SourceV2Error::MessageVersionFlagMismatch { .. })
        ));

        let out_of_bounds = row(0, bytes.len() as u32 + 1, 0);
        assert!(matches!(
            decode_message(&block, &out_of_bounds),
            Err(SourceV2Error::RegionOutOfBounds { .. })
        ));
    }

    #[test]
    fn metadata_lane_decode_matches_owned_block_helper() {
        let metadata = CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: vec![10, 20],
            post_balances: vec![9, 21],
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: vec![CompactPubkey::Raw([7; 32])],
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: Some(42),
            cost_units: Some(84),
        };
        let bytes = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let block = empty_block(Vec::new(), bytes.clone());
        let metadata_row = row(ARCHIVE_V2_TX_FLAG_HAS_METADATA, 0, bytes.len() as u32);

        let owned = decode_metadata(
            CompactV2MetadataSchema::CurrentTypedError,
            &block,
            &metadata_row,
        )
        .unwrap()
        .unwrap();
        let borrowed_lane = decode_metadata_lane(
            CompactV2MetadataSchema::CurrentTypedError,
            &bytes,
            &metadata_row,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            wincode::config::serialize(&owned, wincode_leb128_config()).unwrap(),
            wincode::config::serialize(&borrowed_lane, wincode_leb128_config()).unwrap(),
        );
    }

    #[test]
    fn raw_fallback_is_not_treated_as_an_empty_transaction() {
        let block = empty_block(vec![1], Vec::new());
        assert!(matches!(
            decode_message(&block, &row(ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, 1, 0)),
            Err(SourceV2Error::RawTransactionFallback { tx_index: 3 })
        ));
        assert!(matches!(
            decode_metadata(
                CompactV2MetadataSchema::CurrentTypedError,
                &block,
                &row(
                    ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                    1,
                    1,
                )
            ),
            Err(SourceV2Error::RawMetadataFallback { tx_index: 3 })
        ));
    }

    fn v0_message(lookups: Vec<OwnedCompactAddressTableLookup>) -> ArchiveV2HotV0Message {
        ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Raw([1; 32])],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([2; 32]),
            instructions: Vec::new(),
            address_table_lookups: lookups,
        }
    }

    #[test]
    fn v0_loaded_counts_must_match_signed_descriptors() {
        let message = v0_message(vec![
            OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Raw([8; 32]),
                writable_indexes: vec![1, 2],
                readonly_indexes: vec![3],
            },
            OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Raw([9; 32]),
                writable_indexes: vec![],
                readonly_indexes: vec![4, 5],
            },
        ]);
        let writable = [CompactPubkey::Raw([10; 32]), CompactPubkey::Raw([11; 32])];
        let readonly = [
            CompactPubkey::Raw([12; 32]),
            CompactPubkey::Raw([13; 32]),
            CompactPubkey::Raw([14; 32]),
        ];
        assert_eq!(
            validate_v0_loaded_address_counts(&message, Some((&writable, &readonly))),
            Ok(LoadedAddressCounts {
                writable: 2,
                readonly: 3
            })
        );
        assert_eq!(
            validate_v0_loaded_address_counts(&message, Some((&writable[..1], &readonly))),
            Err(SourceV2Error::LookupCountMismatch {
                kind: LoadedAddressKind::Writable,
                expected: 2,
                actual: 1
            })
        );
        assert_eq!(
            validate_v0_loaded_address_counts(&message, None),
            Err(SourceV2Error::LoadedAddressesUnavailable {
                expected_writable: 2,
                expected_readonly: 3
            })
        );
    }

    #[test]
    fn v0_without_lookups_needs_no_runtime_loaded_address_record() {
        assert_eq!(
            validate_v0_loaded_address_counts(&v0_message(Vec::new()), None),
            Ok(LoadedAddressCounts::default())
        );
        assert_eq!(
            validate_v0_loaded_address_counts(
                &v0_message(vec![OwnedCompactAddressTableLookup {
                    account_key: CompactPubkey::Raw([8; 32]),
                    writable_indexes: Vec::new(),
                    readonly_indexes: Vec::new(),
                }]),
                None,
            ),
            Err(SourceV2Error::EmptyAddressTableLookup { lookup: 0 })
        );
    }

    #[test]
    fn raw_compute_budget_and_system_bytes_are_canonical() {
        let raw = ArchiveV2HotInstructionData::UnknownSystem(vec![9, 8, 7]);
        assert_eq!(reconstruct_instruction_data(&raw, None).unwrap(), [9, 8, 7]);

        let price = ArchiveV2HotInstructionData::ComputeBudget(
            ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(10_000),
        );
        assert_eq!(
            reconstruct_instruction_data(&price, None).unwrap(),
            [3, 0x10, 0x27, 0, 0, 0, 0, 0, 0]
        );

        let transfer =
            ArchiveV2HotInstructionData::System(ArchiveV2SystemInstructionData::Transfer {
                lamports: 0x0102,
            });
        let mut expected = 2u32.to_le_bytes().to_vec();
        expected.extend_from_slice(&0x0102u64.to_le_bytes());
        assert_eq!(
            reconstruct_instruction_data(&transfer, None).unwrap(),
            expected
        );
    }

    #[test]
    fn vote_hash_registry_and_historical_tower_bytes_are_exact() {
        let hash = [
            0xf2, 0xb4, 0x29, 0xeb, 0xfc, 0x9b, 0x4b, 0x2e, 0x1e, 0xe7, 0x05, 0xe8, 0xd0, 0x3d,
            0x1b, 0xad, 0x21, 0x98, 0xae, 0x2d, 0xc4, 0x1a, 0x8b, 0x8e, 0x27, 0xa7, 0xec, 0x0d,
            0xab, 0x03, 0x36, 0x21,
        ];
        let mut registry_bytes = vec![0; 17 * VOTE_HASH_RECORD_LEN];
        registry_bytes.push(3);
        registry_bytes.extend_from_slice(&hash);
        registry_bytes.extend_from_slice(&hash);
        let registry = VoteHashRegistry::from_bytes(&registry_bytes).unwrap();
        let tower = ArchiveV2HotInstructionData::VoteTowerSync(ArchiveV2VoteTowerSync {
            update: ArchiveV2VoteStateUpdate {
                root: None,
                lockout_offsets: vec![
                    ArchiveV2VoteLockoutOffset {
                        offset: 409_124_781,
                        confirmation_count: 1,
                    },
                    ArchiveV2VoteLockoutOffset {
                        offset: 15,
                        confirmation_count: 0,
                    },
                ],
                hash: ArchiveV2VoteHashRef::Block(17),
                timestamp: Some(1_774_582_576_796),
            },
            block_id_hash: ArchiveV2VoteHashRef::Block(17),
        });
        let expected = hex_bytes(
            "0e0000000200000000000000adbf62180000000001000000bcbf6218000000000000000000f2b429ebfc9b4b2e1ee705e8d03d1bad2198ae2dc41a8b8e27a7ec0dab033621019c365d2d9d010000f2b429ebfc9b4b2e1ee705e8d03d1bad2198ae2dc41a8b8e27a7ec0dab033621",
        );
        let candidates = reconstruct_instruction_data_candidates(&tower, Some(&registry)).unwrap();
        assert_eq!(candidates.len(), 2);
        assert_eq!(
            candidates[1],
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteTowerHistorical,
                bytes: expected,
            }
        );
        assert_eq!(
            reconstruct_instruction_data(&tower, Some(&registry)),
            Err(SourceV2Error::AmbiguousInstructionEncoding { candidates: 2 })
        );
    }

    #[test]
    fn missing_or_invalid_vote_hashes_fail_closed() {
        let update =
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(ArchiveV2VoteStateUpdate {
                root: Some(1),
                lockout_offsets: Vec::new(),
                hash: ArchiveV2VoteHashRef::Block(4),
                timestamp: None,
            });
        assert_eq!(
            reconstruct_instruction_data(&update, None),
            Err(SourceV2Error::MissingVoteHashResolver {
                block_id: 4,
                kind: VoteHashKind::Bank
            })
        );
        assert!(matches!(
            VoteHashRegistry::from_bytes(&[4; VOTE_HASH_RECORD_LEN]),
            Err(SourceV2Error::InvalidVoteHashRegistryFlags {
                block_id: 0,
                flags: 4
            })
        ));

        let switched = ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
            update: ArchiveV2VoteStateUpdate {
                root: None,
                lockout_offsets: Vec::new(),
                hash: ArchiveV2VoteHashRef::Zero,
                timestamp: None,
            },
            switch_proof_hash: ArchiveV2VoteHashRef::Block(3),
        };
        assert_eq!(
            reconstruct_instruction_data(&switched, None),
            Err(SourceV2Error::AuxiliaryVoteHashBlockReference { block_id: 3 })
        );
    }

    #[test]
    fn signed_message_serializer_emits_legacy_and_v0_canonical_bytes() {
        let keys = [[3; 32]];
        let legacy = serialize_signed_message(&SignedMessage {
            version: SignedMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            static_account_keys: &keys,
            recent_blockhash: [4; 32],
            instructions: &[],
        })
        .unwrap();
        let mut expected_legacy = vec![1, 0, 0, 1];
        expected_legacy.extend_from_slice(&[3; 32]);
        expected_legacy.extend_from_slice(&[4; 32]);
        expected_legacy.push(0);
        assert_eq!(legacy, expected_legacy);

        let lookups = [ResolvedAddressTableLookup {
            account_key: [8; 32],
            writable_indexes: &[2, 3],
            readonly_indexes: &[4],
        }];
        let v0 = serialize_signed_message(&SignedMessage {
            version: SignedMessageVersion::V0 {
                address_table_lookups: &lookups,
            },
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            static_account_keys: &keys,
            recent_blockhash: [4; 32],
            instructions: &[],
        })
        .unwrap();
        let mut expected_v0 = vec![0x80, 1, 0, 0, 1];
        expected_v0.extend_from_slice(&[3; 32]);
        expected_v0.extend_from_slice(&[4; 32]);
        expected_v0.extend_from_slice(&[0, 1]);
        expected_v0.extend_from_slice(&[8; 32]);
        expected_v0.extend_from_slice(&[2, 2, 3, 1, 4]);
        assert_eq!(v0, expected_v0);
    }

    #[test]
    fn signed_message_requires_a_writable_fee_payer() {
        let keys = [[1; 32], [2; 32]];
        for (required, readonly) in [(0, 0), (1, 1)] {
            assert_eq!(
                serialize_signed_message(&SignedMessage {
                    version: SignedMessageVersion::Legacy,
                    header: CompactMessageHeader {
                        num_required_signatures: required,
                        num_readonly_signed_accounts: readonly,
                        num_readonly_unsigned_accounts: 0,
                    },
                    static_account_keys: &keys,
                    recent_blockhash: [3; 32],
                    instructions: &[],
                }),
                Err(SourceV2Error::NoWritableFeePayer { readonly, required })
            );
        }
    }

    #[test]
    fn signed_message_rejects_fee_payer_and_loaded_program_ids() {
        let keys = [[1; 32], [2; 32]];
        let payer_program = [SignedInstruction {
            program_id_index: 0,
            accounts: &[],
            data: &[],
        }];
        assert_eq!(
            serialize_signed_message(&SignedMessage {
                version: SignedMessageVersion::Legacy,
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                static_account_keys: &keys,
                recent_blockhash: [3; 32],
                instructions: &payer_program,
            }),
            Err(SourceV2Error::ProgramIdIsFeePayer { instruction: 0 })
        );

        let lookups = [ResolvedAddressTableLookup {
            account_key: [4; 32],
            writable_indexes: &[0],
            readonly_indexes: &[],
        }];
        let loaded_program = [SignedInstruction {
            program_id_index: 2,
            accounts: &[],
            data: &[],
        }];
        assert_eq!(
            serialize_signed_message(&SignedMessage {
                version: SignedMessageVersion::V0 {
                    address_table_lookups: &lookups,
                },
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                static_account_keys: &keys,
                recent_blockhash: [3; 32],
                instructions: &loaded_program,
            }),
            Err(SourceV2Error::V0ProgramIdIsLoaded {
                instruction: 0,
                index: 2,
                static_account_count: 2,
            })
        );
    }

    #[test]
    fn v0_message_rejects_empty_lookups_and_more_than_256_accounts() {
        let keys = [[1; 32], [2; 32]];
        let empty_lookup = [ResolvedAddressTableLookup {
            account_key: [3; 32],
            writable_indexes: &[],
            readonly_indexes: &[],
        }];
        let make_message = |lookups| SignedMessage {
            version: SignedMessageVersion::V0 {
                address_table_lookups: lookups,
            },
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            static_account_keys: &keys,
            recent_blockhash: [4; 32],
            instructions: &[],
        };
        assert_eq!(
            serialize_signed_message(&make_message(&empty_lookup)),
            Err(SourceV2Error::EmptyAddressTableLookup { lookup: 0 })
        );

        let at_limit_indexes = vec![0; 254];
        let at_limit_lookup = [ResolvedAddressTableLookup {
            account_key: [3; 32],
            writable_indexes: &at_limit_indexes,
            readonly_indexes: &[],
        }];
        assert!(serialize_signed_message(&make_message(&at_limit_lookup)).is_ok());

        let over_limit_indexes = vec![0; 255];
        let over_limit_lookup = [ResolvedAddressTableLookup {
            account_key: [3; 32],
            writable_indexes: &over_limit_indexes,
            readonly_indexes: &[],
        }];
        assert_eq!(
            serialize_signed_message(&make_message(&over_limit_lookup)),
            Err(SourceV2Error::MessageAccountCountExceedsIndexRange { actual: 257 })
        );
    }

    #[test]
    fn signed_message_short_vector_lengths_match_canonical_boundaries() {
        for (length, expected) in [
            (0, vec![0]),
            (127, vec![0x7f]),
            (128, vec![0x80, 0x01]),
            (16_383, vec![0xff, 0x7f]),
            (16_384, vec![0x80, 0x80, 0x01]),
            (65_535, vec![0xff, 0xff, 0x03]),
        ] {
            let mut bytes = Vec::new();
            push_message_short_vec_len(&mut bytes, length, "fixture").unwrap();
            assert_eq!(bytes, expected, "length {length}");
        }

        assert_eq!(
            push_message_short_vec_len(&mut Vec::new(), 65_536, "fixture"),
            Err(SourceV2Error::ShortVecLengthTooLarge {
                field: "fixture",
                actual: 65_536
            })
        );
    }

    #[test]
    fn signature_verifier_selects_one_of_two_instruction_forms() {
        let keys = [[1; 32], [9; 32]];
        let choices = [
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteTowerCanonical,
                bytes: vec![0xaa],
            },
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteTowerHistorical,
                bytes: vec![0xbb],
            },
        ];
        let candidate_instructions = [SignedInstructionCandidates {
            program_id_index: 1,
            accounts: &[0],
            data_candidates: &choices,
        }];
        let message = SignedMessageCandidates {
            version: SignedMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            static_account_keys: &keys,
            recent_blockhash: [2; 32],
            instructions: &candidate_instructions,
        };
        let selected_instruction = [SignedInstruction {
            program_id_index: 1,
            accounts: &[0],
            data: &[0xbb],
        }];
        let expected_message = serialize_signed_message(&SignedMessage {
            version: SignedMessageVersion::Legacy,
            header: message.header,
            static_account_keys: &keys,
            recent_blockhash: [2; 32],
            instructions: &selected_instruction,
        })
        .unwrap();

        let selected = select_signed_message_candidate(&message, 2, |bytes| {
            bytes == expected_message.as_slice()
        })
        .unwrap();
        assert_eq!(selected.instruction_data, vec![vec![0xbb]]);
        assert_eq!(selected.signed_message, expected_message);
    }

    #[test]
    fn signature_candidate_selection_rejects_none_multiple_and_limit() {
        let keys = [[1; 32], [9; 32]];
        let choices = [
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::Raw,
                bytes: vec![1],
            },
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::Raw,
                bytes: vec![2],
            },
        ];
        let instructions = [SignedInstructionCandidates {
            program_id_index: 1,
            accounts: &[],
            data_candidates: &choices,
        }];
        let message = SignedMessageCandidates {
            version: SignedMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            static_account_keys: &keys,
            recent_blockhash: [2; 32],
            instructions: &instructions,
        };

        assert_eq!(
            select_signed_message_candidate(&message, 2, |_| false),
            Err(SourceV2Error::NoVerifiedMessageCandidate)
        );
        assert_eq!(
            select_signed_message_candidate(&message, 2, |_| true),
            Err(SourceV2Error::MultipleVerifiedMessageCandidates)
        );

        let mut verifier_calls = 0;
        assert_eq!(
            select_signed_message_candidate(&message, 1, |_| {
                verifier_calls += 1;
                false
            }),
            Err(SourceV2Error::CandidateCombinationLimitExceeded { maximum: 1 })
        );
        assert_eq!(verifier_calls, 0);
        assert_eq!(
            select_signed_message_candidate(
                &message,
                MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS + 1,
                |_| false,
            ),
            Err(SourceV2Error::InvalidCandidateCombinationLimit {
                requested: MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS + 1,
                hard_maximum: MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS,
            })
        );
    }

    #[test]
    fn signature_candidate_selection_covers_8192_packet_sized_combinations() {
        let keys = [[1; 32], [9; 32]];
        let choices = [
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteTowerCanonical,
                bytes: vec![0],
            },
            InstructionDataCandidate {
                encoding: InstructionDataEncoding::VoteTowerHistorical,
                bytes: vec![1],
            },
        ];
        let instructions = [SignedInstructionCandidates {
            program_id_index: 1,
            accounts: &[],
            data_candidates: &choices,
        }; 13];
        let message = SignedMessageCandidates {
            version: SignedMessageVersion::Legacy,
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            static_account_keys: &keys,
            recent_blockhash: [2; 32],
            instructions: &instructions,
        };
        let selected_instructions = [SignedInstruction {
            program_id_index: 1,
            accounts: &[],
            data: &[1],
        }; 13];
        let expected = serialize_signed_message(&SignedMessage {
            version: SignedMessageVersion::Legacy,
            header: message.header,
            static_account_keys: &keys,
            recent_blockhash: message.recent_blockhash,
            instructions: &selected_instructions,
        })
        .unwrap();

        let mut verifier_calls = 0;
        let selected = select_signed_message_candidate(
            &message,
            MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS,
            |bytes| {
                verifier_calls += 1;
                bytes == expected
            },
        )
        .unwrap();
        assert_eq!(verifier_calls, 8_192);
        assert_eq!(selected.signed_message, expected);
        assert_eq!(selected.instruction_data, vec![vec![1]; 13]);
    }

    fn hex_bytes(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16).unwrap();
                let low = (pair[1] as char).to_digit(16).unwrap();
                ((high << 4) | low) as u8
            })
            .collect()
    }
}
