//! **Removal candidate.** Archive V3's ledger plane models the same slot,
//! transaction, message and instruction shapes, and adds the execution-effect
//! plane this format lacks. Payload format 8 was never enabled, and only the
//! PoH entry-hash slice has any consumer. See REMOVAL-CANDIDATE.md before
//! building anything new on this.
//!
//! Minimal, canonical Replay Projection V1 payload types and codec.
//!
//! This module only implements the hot per-produced-slot payload. Finality,
//! registries, status evidence, checkpoints, and publication receipts are
//! generation-level contracts. Payload format 8 remains reserved and must not
//! be enabled merely because this codec exists.

use std::{error::Error, fmt};

use sha2::{Digest, Sha256};

pub const REPLAY_PROJECTION_PAYLOAD_FORMAT_V1: u32 = 8;
pub const REPLAY_PROJECTION_PAYLOAD_VERSION_V1: u16 = 1;

pub const MAX_REPLAY_SLOT_BYTES: usize = 67_108_864;
pub const MAX_REPLAY_COMPONENTS_PER_SLOT: usize = 1_048_576;
pub const MAX_REPLAY_ENTRIES_PER_BATCH: usize = 1_048_576;
pub const MAX_REPLAY_ENTRIES_PER_SLOT: usize = 1_048_576;
pub const MAX_REPLAY_TRANSACTIONS_PER_ENTRY: usize = 524_288;
pub const MAX_REPLAY_TRANSACTIONS_PER_SLOT: usize = 524_288;
pub const MAX_REPLAY_NUM_HASHES_PER_ENTRY: u64 = 16_777_216;
pub const MAX_REPLAY_NUM_HASHES_PER_SLOT: u64 = 67_108_864;
pub const MAX_REPLAY_STATIC_ACCOUNT_KEYS: usize = 256;
pub const MAX_REPLAY_INSTRUCTIONS_PER_MESSAGE: usize = 1_232;
pub const MAX_REPLAY_INSTRUCTION_ACCOUNT_INDEXES: usize = 1_232;
pub const MAX_REPLAY_INSTRUCTION_DATA_BYTES: usize = 1_232;
pub const MAX_REPLAY_ADDRESS_TABLE_LOOKUPS: usize = 1_232;
pub const MAX_REPLAY_LOOKUP_INDEXES: usize = 1_232;
pub const MAX_REPLAY_EXPANDED_MESSAGE_BYTES: usize = 1_232;
pub const MAX_REPLAY_RAW_MESSAGE_BYTES: usize = 4_096;
pub const MAX_REPLAY_BLOCK_MARKER_BYTES: usize = 65_540;
pub const MAX_REPLAY_BLOCK_MARKER_BYTES_PER_SLOT: usize = 67_108_864;
pub const MAX_REPLAY_ADDRESS_REGISTRY_ENTRIES: u32 = 134_217_728;
pub const MAX_REPLAY_PREVIOUS_BLOCKHASH_TAIL_ROWS: u32 = 65_536;

const COMPONENT_ENTRY_BATCH_TAG: u8 = 0;
const COMPONENT_BLOCK_MARKER_TAG: u8 = 1;
const MESSAGE_LEGACY_TAG: u8 = 0;
const MESSAGE_V0_TAG: u8 = 1;
const MESSAGE_RAW_TAG: u8 = 2;
const TRANSACTION_STATUS_REF_BIT: u8 = 1 << 2;
const TRANSACTION_RESERVED_BITS: u8 = 0b1111_1000;
const STATUS_PRIOR_TX_DISTANCE_TAG: u8 = 0;
const STATUS_PREVIOUS_CLASS_ID_TAG: u8 = 1;
const BLOCKHASH_PRIOR_PRODUCED_DISTANCE_TAG: u8 = 0;
const BLOCKHASH_PREVIOUS_TAIL_INDEX_TAG: u8 = 1;
const BLOCKHASH_RAW_TAG: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySlotV1<'a> {
    pub components: Vec<ReplayComponentV1<'a>>,
}

/// One structural event from a Replay V1 slot payload.
///
/// Events are emitted in wire order. Byte strings borrow directly from the
/// input payload; only the bounded metadata needed by one transaction is
/// materialized at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplaySlotEventV1<'a> {
    EntryBatch {
        entry_count: usize,
    },
    Entry {
        num_hashes: u64,
        signature_mixin: Option<[u8; 32]>,
        transaction_count: usize,
    },
    Transaction(ReplayTransactionV1<'a>),
    BlockMarker(&'a [u8]),
}

/// Bounded, single-pass decoder for one Replay V1 slot payload.
///
/// Consumers must drive the iterator to completion to validate the complete
/// payload, including its final shape and absence of trailing bytes. After an
/// error the iterator is fused and emits no further events.
pub struct ReplaySlotEventDecoderV1<'a> {
    decoder: Decoder<'a>,
    components_remaining: usize,
    entries_remaining: usize,
    transactions_remaining: usize,
    saw_entry_batch: bool,
    slot_entry_count: u64,
    slot_transaction_count: u64,
    slot_num_hashes: u64,
    slot_marker_bytes: u64,
    state: ReplaySlotDecoderState,
}

enum ReplaySlotDecoderState {
    Active,
    Complete,
    Failed(ReplayCodecError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayComponentV1<'a> {
    EntryBatch(Vec<ReplayEntryV1<'a>>),
    BlockMarker(&'a [u8]),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayEntryV1<'a> {
    pub num_hashes: u64,
    pub signature_mixin: Option<[u8; 32]>,
    pub transactions: Vec<ReplayTransactionV1<'a>>,
}

impl ReplayEntryV1<'_> {
    #[must_use]
    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayTransactionV1<'a> {
    pub historical_status_backref: Option<StatusKeyClassRefV1>,
    pub message: ReplayMessageV1<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusKeyClassRefV1 {
    PriorTxDistance(u64),
    PreviousClassId([u8; 24]),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayMessageV1<'a> {
    Legacy(ReplayLegacyMessageV1<'a>),
    V0(ReplayV0MessageV1<'a>),
    Raw(ReplayRawMessageV1<'a>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayLegacyMessageV1<'a> {
    pub header: [u8; 3],
    pub static_account_keys: Vec<ReplayAddressRefV1>,
    pub recent_blockhash: RecentBlockhashRefV1,
    pub instructions: Vec<ReplayInstructionV1<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayV0MessageV1<'a> {
    pub header: [u8; 3],
    pub static_account_keys: Vec<ReplayAddressRefV1>,
    pub recent_blockhash: RecentBlockhashRefV1,
    pub instructions: Vec<ReplayInstructionV1<'a>>,
    pub address_table_lookups: Vec<ReplayAddressTableLookupV1<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecentBlockhashRefV1 {
    PriorProducedDistance(u32),
    PreviousTailIndex(u32),
    Raw([u8; 32]),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayInstructionV1<'a> {
    pub program_id_index: u8,
    pub account_indexes: &'a [u8],
    pub data: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayAddressTableLookupV1<'a> {
    pub table_account: ReplayAddressRefV1,
    pub writable_indexes: &'a [u8],
    pub readonly_indexes: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayAddressRefV1 {
    RegistryId(u32),
    Raw([u8; 32]),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRawMessageV1<'a> {
    pub signed_message_bytes: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayCodecError {
    InputTooLarge {
        max: usize,
        actual: usize,
    },
    OutputTooLarge {
        max: usize,
    },
    Truncated {
        field: &'static str,
        needed: usize,
        remaining: usize,
    },
    TrailingBytes {
        count: usize,
    },
    NonMinimalLeb128 {
        field: &'static str,
    },
    Leb128Overflow {
        field: &'static str,
    },
    CountOutOfBounds {
        field: &'static str,
        min: usize,
        max: usize,
        actual: u64,
    },
    LengthOutOfBounds {
        field: &'static str,
        min: usize,
        max: usize,
        actual: usize,
    },
    UnknownTag {
        field: &'static str,
        value: u8,
    },
    ReservedBitsSet {
        field: &'static str,
        value: u8,
    },
    InvalidValue {
        field: &'static str,
        reason: &'static str,
    },
    AggregateOutOfBounds {
        field: &'static str,
        max: u64,
        actual: u64,
    },
    ArithmeticOverflow {
        field: &'static str,
    },
}

impl fmt::Display for ReplayCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InputTooLarge { max, actual } => {
                write!(
                    formatter,
                    "Replay V1 input is {actual} bytes; maximum is {max}"
                )
            }
            Self::OutputTooLarge { max } => {
                write!(formatter, "Replay V1 output exceeds {max} bytes")
            }
            Self::Truncated {
                field,
                needed,
                remaining,
            } => write!(
                formatter,
                "truncated {field}: need {needed} bytes, have {remaining}"
            ),
            Self::TrailingBytes { count } => {
                write!(formatter, "Replay V1 payload has {count} trailing bytes")
            }
            Self::NonMinimalLeb128 { field } => {
                write!(formatter, "{field} uses non-minimal unsigned LEB128")
            }
            Self::Leb128Overflow { field } => {
                write!(formatter, "{field} overflows unsigned LEB128")
            }
            Self::CountOutOfBounds {
                field,
                min,
                max,
                actual,
            } => write!(formatter, "{field} count {actual} is outside {min}..={max}"),
            Self::LengthOutOfBounds {
                field,
                min,
                max,
                actual,
            } => write!(
                formatter,
                "{field} length {actual} is outside {min}..={max}"
            ),
            Self::UnknownTag { field, value } => {
                write!(formatter, "unknown {field} tag {value}")
            }
            Self::ReservedBitsSet { field, value } => {
                write!(formatter, "reserved bits are set in {field}: {value:#04x}")
            }
            Self::InvalidValue { field, reason } => write!(formatter, "invalid {field}: {reason}"),
            Self::AggregateOutOfBounds { field, max, actual } => {
                write!(formatter, "{field} aggregate {actual} exceeds {max}")
            }
            Self::ArithmeticOverflow { field } => {
                write!(formatter, "arithmetic overflow while validating {field}")
            }
        }
    }
}

impl Error for ReplayCodecError {}

pub type ReplayCodecResult<T> = Result<T, ReplayCodecError>;

/// Streaming construction of the exact signature Merkle root mixed into one
/// transaction-bearing PoH entry.
///
/// Signatures are consumed in transaction order and signer order. They are not
/// retained by the builder or represented in Replay V1.
#[derive(Debug, Clone)]
pub struct ReplaySignatureMixinBuilder {
    frontier: [Option<[u8; 32]>; 64],
    signature_count: u64,
}

impl Default for ReplaySignatureMixinBuilder {
    fn default() -> Self {
        Self {
            frontier: [None; 64],
            signature_count: 0,
        }
    }
}

impl ReplaySignatureMixinBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_signature(&mut self, signature: &[u8; 64]) -> ReplayCodecResult<()> {
        self.signature_count =
            self.signature_count
                .checked_add(1)
                .ok_or(ReplayCodecError::ArithmeticOverflow {
                    field: "ReplaySignatureMixinBuilder.signature_count",
                })?;

        let mut node = signature_leaf_hash(signature);
        let mut level = 0usize;
        loop {
            let Some(frontier) = self.frontier.get_mut(level) else {
                return Err(ReplayCodecError::ArithmeticOverflow {
                    field: "ReplaySignatureMixinBuilder.frontier",
                });
            };
            if let Some(left) = frontier.take() {
                node = signature_node_hash(left, node);
                level += 1;
            } else {
                *frontier = Some(node);
                return Ok(());
            }
        }
    }

    #[must_use]
    pub const fn signature_count(&self) -> u64 {
        self.signature_count
    }

    /// Finish using the frozen odd-node rule: duplicate the last node at every
    /// incomplete Merkle level.
    #[must_use]
    pub fn finish(self) -> [u8; 32] {
        if self.signature_count == 0 {
            return [0; 32];
        }

        let mut right: Option<([u8; 32], usize)> = None;
        for (level, left) in self.frontier.into_iter().enumerate() {
            let Some(left) = left else {
                continue;
            };
            right = Some(match right {
                None => (left, level),
                Some((mut right, mut right_level)) => {
                    while right_level < level {
                        right = signature_node_hash(right, right);
                        right_level += 1;
                    }
                    (signature_node_hash(left, right), level + 1)
                }
            });
        }
        right.expect("a non-empty frontier has a root").0
    }
}

/// Compute the exact Replay/Agave signature mixin without retaining signature
/// bytes after the call.
pub fn replay_signature_mixin<'a>(
    signatures: impl IntoIterator<Item = &'a [u8; 64]>,
) -> ReplayCodecResult<[u8; 32]> {
    let mut builder = ReplaySignatureMixinBuilder::new();
    for signature in signatures {
        builder.push_signature(signature)?;
    }
    Ok(builder.finish())
}

/// Derive one entry hash using Replay V1's frozen Agave-compatible PoH formula.
pub fn derive_replay_entry_hash(
    previous_hash: [u8; 32],
    num_hashes: u64,
    transaction_count: u32,
    signature_mixin: Option<[u8; 32]>,
) -> ReplayCodecResult<[u8; 32]> {
    if num_hashes > MAX_REPLAY_NUM_HASHES_PER_ENTRY {
        return Err(ReplayCodecError::AggregateOutOfBounds {
            field: "ReplayEntryV1.num_hashes",
            max: MAX_REPLAY_NUM_HASHES_PER_ENTRY,
            actual: num_hashes,
        });
    }
    match (transaction_count == 0, signature_mixin) {
        (true, None) | (false, Some(_)) => {}
        (true, Some(_)) => {
            return Err(ReplayCodecError::InvalidValue {
                field: "ReplayEntryV1.signature_mixin",
                reason: "must be absent when transaction_count is zero",
            });
        }
        (false, None) => {
            return Err(ReplayCodecError::InvalidValue {
                field: "ReplayEntryV1.signature_mixin",
                reason: "must be present when transaction_count is non-zero",
            });
        }
    }

    if num_hashes == 0 && transaction_count == 0 {
        return Ok(previous_hash);
    }

    let mut hash = previous_hash;
    for _ in 0..num_hashes.saturating_sub(1) {
        hash = Sha256::digest(hash).into();
    }
    if transaction_count == 0 {
        Ok(Sha256::digest(hash).into())
    } else {
        let mut hasher = Sha256::new();
        hasher.update(hash);
        hasher.update(signature_mixin.expect("non-empty transaction count checked"));
        Ok(hasher.finalize().into())
    }
}

fn signature_leaf_hash(signature: &[u8; 64]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([0]);
    hasher.update(signature);
    hasher.finalize().into()
}

fn signature_node_hash(left: [u8; 32], right: [u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([1]);
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// Encode one hot Replay V1 slot payload.
pub fn encode_replay_slot_v1(slot: &ReplaySlotV1<'_>) -> ReplayCodecResult<Vec<u8>> {
    let mut output = Vec::new();
    encode_replay_slot_v1_into(slot, &mut output)?;
    Ok(output)
}

/// Append one hot Replay V1 slot payload to `output`.
///
/// On error, bytes appended by this call are removed.
pub fn encode_replay_slot_v1_into(
    slot: &ReplaySlotV1<'_>,
    output: &mut Vec<u8>,
) -> ReplayCodecResult<()> {
    validate_replay_slot_v1(slot)?;
    let start = output.len();
    let result = {
        let mut encoder = Encoder { output, start };
        encode_slot(&mut encoder, slot)
    };
    if result.is_err() {
        output.truncate(start);
    }
    result
}

/// Start a bounded, event-oriented decode of one hot Replay V1 slot payload.
///
/// Aggregate entry and transaction limits are checked at their containing
/// headers, before any nested message metadata is materialized. The iterator
/// must be consumed to completion for whole-payload validation.
pub fn decode_replay_slot_events_v1(
    input: &[u8],
) -> ReplayCodecResult<ReplaySlotEventDecoderV1<'_>> {
    if input.len() > MAX_REPLAY_SLOT_BYTES {
        return Err(ReplayCodecError::InputTooLarge {
            max: MAX_REPLAY_SLOT_BYTES,
            actual: input.len(),
        });
    }

    let mut decoder = Decoder { input, offset: 0 };
    let component_count =
        decoder.count(1, MAX_REPLAY_COMPONENTS_PER_SLOT, "ReplaySlotV1.components")?;

    Ok(ReplaySlotEventDecoderV1 {
        decoder,
        components_remaining: component_count,
        entries_remaining: 0,
        transactions_remaining: 0,
        saw_entry_batch: false,
        slot_entry_count: 0,
        slot_transaction_count: 0,
        slot_num_hashes: 0,
        slot_marker_bytes: 0,
        state: ReplaySlotDecoderState::Active,
    })
}

impl<'a> ReplaySlotEventDecoderV1<'a> {
    fn decode_next(&mut self) -> ReplayCodecResult<Option<ReplaySlotEventV1<'a>>> {
        if self.transactions_remaining != 0 {
            let transaction = decode_transaction(&mut self.decoder)?;
            self.transactions_remaining -= 1;
            return Ok(Some(ReplaySlotEventV1::Transaction(transaction)));
        }

        if self.entries_remaining != 0 {
            let remaining_slot_transactions = MAX_REPLAY_TRANSACTIONS_PER_SLOT
                - usize::try_from(self.slot_transaction_count).expect("bounded slot count");
            let remaining_slot_num_hashes = MAX_REPLAY_NUM_HASHES_PER_SLOT - self.slot_num_hashes;
            let header = decode_entry_header(
                &mut self.decoder,
                remaining_slot_transactions,
                remaining_slot_num_hashes,
            )?;

            self.entries_remaining -= 1;
            self.slot_transaction_count = checked_add_count(
                self.slot_transaction_count,
                header.transaction_count,
                MAX_REPLAY_TRANSACTIONS_PER_SLOT as u64,
                "ReplaySlotV1.transactions",
            )?;
            self.slot_num_hashes = self.slot_num_hashes.checked_add(header.num_hashes).ok_or(
                ReplayCodecError::ArithmeticOverflow {
                    field: "ReplaySlotV1.num_hashes",
                },
            )?;
            if self.slot_num_hashes > MAX_REPLAY_NUM_HASHES_PER_SLOT {
                return Err(ReplayCodecError::AggregateOutOfBounds {
                    field: "ReplaySlotV1.num_hashes",
                    max: MAX_REPLAY_NUM_HASHES_PER_SLOT,
                    actual: self.slot_num_hashes,
                });
            }
            self.transactions_remaining = header.transaction_count;
            return Ok(Some(ReplaySlotEventV1::Entry {
                num_hashes: header.num_hashes,
                signature_mixin: header.signature_mixin,
                transaction_count: header.transaction_count,
            }));
        }

        if self.components_remaining != 0 {
            let tag = self.decoder.u8("ReplayComponentV1.tag")?;
            self.components_remaining -= 1;
            return match tag {
                COMPONENT_ENTRY_BATCH_TAG => {
                    let entry_count = self.decoder.count(
                        1,
                        MAX_REPLAY_ENTRIES_PER_BATCH,
                        "ReplayComponentV1.EntryBatch.entries",
                    )?;
                    self.slot_entry_count = checked_add_count(
                        self.slot_entry_count,
                        entry_count,
                        MAX_REPLAY_ENTRIES_PER_SLOT as u64,
                        "ReplaySlotV1.entries",
                    )?;
                    self.saw_entry_batch = true;
                    self.entries_remaining = entry_count;
                    Ok(Some(ReplaySlotEventV1::EntryBatch { entry_count }))
                }
                COMPONENT_BLOCK_MARKER_TAG => {
                    let bytes = self.decoder.bytes(
                        1,
                        MAX_REPLAY_BLOCK_MARKER_BYTES,
                        "ReplayComponentV1.BlockMarker.bytes",
                    )?;
                    self.slot_marker_bytes = checked_add_count(
                        self.slot_marker_bytes,
                        bytes.len(),
                        MAX_REPLAY_BLOCK_MARKER_BYTES_PER_SLOT as u64,
                        "ReplaySlotV1.block_marker_bytes",
                    )?;
                    Ok(Some(ReplaySlotEventV1::BlockMarker(bytes)))
                }
                value => Err(ReplayCodecError::UnknownTag {
                    field: "ReplayComponentV1",
                    value,
                }),
            };
        }

        self.decoder.finish()?;
        if !self.saw_entry_batch {
            return Err(ReplayCodecError::InvalidValue {
                field: "ReplaySlotV1.components",
                reason: "must contain at least one EntryBatch",
            });
        }
        Ok(None)
    }

    /// Consume all remaining events and validate the complete payload.
    pub fn finish(mut self) -> ReplayCodecResult<()> {
        if let ReplaySlotDecoderState::Failed(error) = &self.state {
            return Err(error.clone());
        }
        for event in &mut self {
            event?;
        }
        match self.state {
            ReplaySlotDecoderState::Complete => Ok(()),
            ReplaySlotDecoderState::Failed(error) => Err(error),
            ReplaySlotDecoderState::Active => {
                unreachable!("exhausting an active Replay slot decoder is terminal")
            }
        }
    }
}

impl<'a> Iterator for ReplaySlotEventDecoderV1<'a> {
    type Item = ReplayCodecResult<ReplaySlotEventV1<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if !matches!(self.state, ReplaySlotDecoderState::Active) {
            return None;
        }

        match self.decode_next() {
            Ok(Some(event)) => Some(Ok(event)),
            Ok(None) => {
                self.state = ReplaySlotDecoderState::Complete;
                None
            }
            Err(error) => {
                self.state = ReplaySlotDecoderState::Failed(error.clone());
                Some(Err(error))
            }
        }
    }
}

impl std::iter::FusedIterator for ReplaySlotEventDecoderV1<'_> {}

/// Decode one complete hot Replay V1 slot payload into a convenient owned
/// component tree while borrowing byte strings from `input`.
///
/// The event decoder is authoritative; this helper only collects its validated
/// event sequence.
pub fn decode_replay_slot_v1(input: &[u8]) -> ReplayCodecResult<ReplaySlotV1<'_>> {
    let mut components = Vec::new();
    for event in decode_replay_slot_events_v1(input)? {
        match event? {
            ReplaySlotEventV1::EntryBatch { .. } => {
                components.push(ReplayComponentV1::EntryBatch(Vec::new()));
            }
            ReplaySlotEventV1::Entry {
                num_hashes,
                signature_mixin,
                ..
            } => {
                let Some(ReplayComponentV1::EntryBatch(entries)) = components.last_mut() else {
                    unreachable!("event decoder emits entries only inside an entry batch");
                };
                entries.push(ReplayEntryV1 {
                    num_hashes,
                    signature_mixin,
                    transactions: Vec::new(),
                });
            }
            ReplaySlotEventV1::Transaction(transaction) => {
                let Some(ReplayComponentV1::EntryBatch(entries)) = components.last_mut() else {
                    unreachable!("event decoder emits transactions only inside an entry batch");
                };
                let entry = entries
                    .last_mut()
                    .expect("event decoder emits a transaction only after its entry");
                entry.transactions.push(transaction);
            }
            ReplaySlotEventV1::BlockMarker(bytes) => {
                components.push(ReplayComponentV1::BlockMarker(bytes));
            }
        }
    }
    Ok(ReplaySlotV1 { components })
}

/// Validate format-local Replay V1 invariants.
///
/// Registry cardinality, prior-produced ordinals, prior transaction classes,
/// marker semantics, and runtime-era sanitation require generation context and
/// are intentionally validated by the publication/replay layer.
pub fn validate_replay_slot_v1(slot: &ReplaySlotV1<'_>) -> ReplayCodecResult<()> {
    check_count(
        slot.components.len(),
        1,
        MAX_REPLAY_COMPONENTS_PER_SLOT,
        "ReplaySlotV1.components",
    )?;

    let mut saw_entry_batch = false;
    let mut entry_count = 0u64;
    let mut transaction_count = 0u64;
    let mut num_hashes = 0u64;
    let mut marker_bytes = 0u64;

    for component in &slot.components {
        match component {
            ReplayComponentV1::EntryBatch(entries) => {
                saw_entry_batch = true;
                check_count(
                    entries.len(),
                    1,
                    MAX_REPLAY_ENTRIES_PER_BATCH,
                    "ReplayComponentV1.EntryBatch.entries",
                )?;
                entry_count = checked_add_count(
                    entry_count,
                    entries.len(),
                    MAX_REPLAY_ENTRIES_PER_SLOT as u64,
                    "ReplaySlotV1.entries",
                )?;

                for entry in entries {
                    if entry.num_hashes > MAX_REPLAY_NUM_HASHES_PER_ENTRY {
                        return Err(ReplayCodecError::AggregateOutOfBounds {
                            field: "ReplayEntryV1.num_hashes",
                            max: MAX_REPLAY_NUM_HASHES_PER_ENTRY,
                            actual: entry.num_hashes,
                        });
                    }
                    num_hashes = num_hashes.checked_add(entry.num_hashes).ok_or(
                        ReplayCodecError::ArithmeticOverflow {
                            field: "ReplaySlotV1.num_hashes",
                        },
                    )?;
                    if num_hashes > MAX_REPLAY_NUM_HASHES_PER_SLOT {
                        return Err(ReplayCodecError::AggregateOutOfBounds {
                            field: "ReplaySlotV1.num_hashes",
                            max: MAX_REPLAY_NUM_HASHES_PER_SLOT,
                            actual: num_hashes,
                        });
                    }

                    check_count(
                        entry.transactions.len(),
                        0,
                        MAX_REPLAY_TRANSACTIONS_PER_ENTRY,
                        "ReplayEntryV1.transactions",
                    )?;
                    match (
                        entry.transactions.is_empty(),
                        entry.signature_mixin.is_some(),
                    ) {
                        (true, false) | (false, true) => {}
                        (true, true) => {
                            return Err(ReplayCodecError::InvalidValue {
                                field: "ReplayEntryV1.signature_mixin",
                                reason: "must be absent when transaction_count is zero",
                            });
                        }
                        (false, false) => {
                            return Err(ReplayCodecError::InvalidValue {
                                field: "ReplayEntryV1.signature_mixin",
                                reason: "must be present when transaction_count is non-zero",
                            });
                        }
                    }
                    transaction_count = checked_add_count(
                        transaction_count,
                        entry.transactions.len(),
                        MAX_REPLAY_TRANSACTIONS_PER_SLOT as u64,
                        "ReplaySlotV1.transactions",
                    )?;
                    for transaction in &entry.transactions {
                        validate_transaction(transaction)?;
                    }
                }
            }
            ReplayComponentV1::BlockMarker(bytes) => {
                check_length(
                    bytes.len(),
                    1,
                    MAX_REPLAY_BLOCK_MARKER_BYTES,
                    "ReplayComponentV1.BlockMarker.bytes",
                )?;
                marker_bytes = checked_add_count(
                    marker_bytes,
                    bytes.len(),
                    MAX_REPLAY_BLOCK_MARKER_BYTES_PER_SLOT as u64,
                    "ReplaySlotV1.block_marker_bytes",
                )?;
            }
        }
    }

    if !saw_entry_batch {
        return Err(ReplayCodecError::InvalidValue {
            field: "ReplaySlotV1.components",
            reason: "must contain at least one EntryBatch",
        });
    }

    let _ = (entry_count, transaction_count, marker_bytes);
    Ok(())
}

fn validate_transaction(transaction: &ReplayTransactionV1<'_>) -> ReplayCodecResult<()> {
    if let Some(StatusKeyClassRefV1::PriorTxDistance(0)) = transaction.historical_status_backref {
        return Err(ReplayCodecError::InvalidValue {
            field: "StatusKeyClassRefV1.PriorTxDistance",
            reason: "distance must be non-zero",
        });
    }

    match &transaction.message {
        ReplayMessageV1::Legacy(message) => validate_legacy_message(message),
        ReplayMessageV1::V0(message) => validate_v0_message(message),
        ReplayMessageV1::Raw(message) => validate_raw_message(message),
    }
}

fn validate_legacy_message(message: &ReplayLegacyMessageV1<'_>) -> ReplayCodecResult<()> {
    validate_static_keys_and_header(&message.static_account_keys, message.header)?;
    validate_recent_blockhash(message.recent_blockhash)?;
    check_count(
        message.instructions.len(),
        0,
        MAX_REPLAY_INSTRUCTIONS_PER_MESSAGE,
        "ReplayLegacyMessageV1.instructions",
    )?;
    validate_instructions(&message.instructions, message.static_account_keys.len())?;
    validate_expanded_message_len(expanded_legacy_message_len(message)?)
}

fn validate_v0_message(message: &ReplayV0MessageV1<'_>) -> ReplayCodecResult<()> {
    validate_static_keys_and_header(&message.static_account_keys, message.header)?;
    validate_recent_blockhash(message.recent_blockhash)?;
    check_count(
        message.instructions.len(),
        0,
        MAX_REPLAY_INSTRUCTIONS_PER_MESSAGE,
        "ReplayV0MessageV1.instructions",
    )?;
    check_count(
        message.address_table_lookups.len(),
        0,
        MAX_REPLAY_ADDRESS_TABLE_LOOKUPS,
        "ReplayV0MessageV1.address_table_lookups",
    )?;

    let mut loaded_count = 0usize;
    for lookup in &message.address_table_lookups {
        validate_address_ref(lookup.table_account)?;
        check_length(
            lookup.writable_indexes.len(),
            0,
            MAX_REPLAY_LOOKUP_INDEXES,
            "ReplayAddressTableLookupV1.writable_indexes",
        )?;
        check_length(
            lookup.readonly_indexes.len(),
            0,
            MAX_REPLAY_LOOKUP_INDEXES,
            "ReplayAddressTableLookupV1.readonly_indexes",
        )?;
        loaded_count = loaded_count
            .checked_add(lookup.writable_indexes.len())
            .and_then(|value| value.checked_add(lookup.readonly_indexes.len()))
            .ok_or(ReplayCodecError::ArithmeticOverflow {
                field: "ReplayV0MessageV1.loaded_account_count",
            })?;
    }
    let total_account_count = message
        .static_account_keys
        .len()
        .checked_add(loaded_count)
        .ok_or(ReplayCodecError::ArithmeticOverflow {
            field: "ReplayV0MessageV1.account_count",
        })?;
    if total_account_count > 256 {
        return Err(ReplayCodecError::CountOutOfBounds {
            field: "ReplayV0MessageV1.account_count",
            min: 0,
            max: 256,
            actual: total_account_count as u64,
        });
    }
    validate_instructions(&message.instructions, total_account_count)?;
    validate_expanded_message_len(expanded_v0_message_len(message)?)
}

fn validate_raw_message(message: &ReplayRawMessageV1<'_>) -> ReplayCodecResult<()> {
    check_length(
        message.signed_message_bytes.len(),
        1,
        MAX_REPLAY_RAW_MESSAGE_BYTES,
        "ReplayRawMessageV1.signed_message_bytes",
    )?;
    let first = message.signed_message_bytes[0];
    if first & 0x80 == 0 {
        return Err(ReplayCodecError::InvalidValue {
            field: "ReplayRawMessageV1.signed_message_bytes",
            reason: "Legacy messages must use the compact Legacy variant",
        });
    }
    if first == 0x80 {
        return Err(ReplayCodecError::InvalidValue {
            field: "ReplayRawMessageV1.signed_message_bytes",
            reason: "V0 messages must use the compact V0 variant",
        });
    }
    Ok(())
}

fn validate_static_keys_and_header(
    static_account_keys: &[ReplayAddressRefV1],
    header: [u8; 3],
) -> ReplayCodecResult<()> {
    check_count(
        static_account_keys.len(),
        0,
        MAX_REPLAY_STATIC_ACCOUNT_KEYS,
        "ReplayMessageV1.static_account_keys",
    )?;
    for address in static_account_keys {
        validate_address_ref(*address)?;
    }

    validate_message_header(static_account_keys.len(), header)
}

fn validate_message_header(
    static_account_key_count: usize,
    header: [u8; 3],
) -> ReplayCodecResult<()> {
    let required = usize::from(header[0]);
    let readonly_signed = usize::from(header[1]);
    let readonly_unsigned = usize::from(header[2]);
    if required > static_account_key_count {
        return Err(ReplayCodecError::InvalidValue {
            field: "ReplayMessageV1.header.num_required_signatures",
            reason: "exceeds static account-key count",
        });
    }
    if readonly_signed > required {
        return Err(ReplayCodecError::InvalidValue {
            field: "ReplayMessageV1.header.num_readonly_signed_accounts",
            reason: "exceeds required-signature count",
        });
    }
    if readonly_unsigned > static_account_key_count - required {
        return Err(ReplayCodecError::InvalidValue {
            field: "ReplayMessageV1.header.num_readonly_unsigned_accounts",
            reason: "exceeds unsigned static account count",
        });
    }
    Ok(())
}

fn validate_address_ref(address: ReplayAddressRefV1) -> ReplayCodecResult<()> {
    if let ReplayAddressRefV1::RegistryId(id) = address
        && (id == 0 || id > MAX_REPLAY_ADDRESS_REGISTRY_ENTRIES)
    {
        return Err(ReplayCodecError::InvalidValue {
            field: "ReplayAddressRefV1.RegistryId",
            reason: "registry ID is outside the one-based format range",
        });
    }
    Ok(())
}

fn validate_recent_blockhash(blockhash: RecentBlockhashRefV1) -> ReplayCodecResult<()> {
    match blockhash {
        RecentBlockhashRefV1::PriorProducedDistance(0) => Err(ReplayCodecError::InvalidValue {
            field: "RecentBlockhashRefV1.PriorProducedDistance",
            reason: "distance must be non-zero",
        }),
        RecentBlockhashRefV1::PreviousTailIndex(index)
            if index >= MAX_REPLAY_PREVIOUS_BLOCKHASH_TAIL_ROWS =>
        {
            Err(ReplayCodecError::InvalidValue {
                field: "RecentBlockhashRefV1.PreviousTailIndex",
                reason: "index exceeds the format tail-row bound",
            })
        }
        _ => Ok(()),
    }
}

fn validate_instructions(
    instructions: &[ReplayInstructionV1<'_>],
    account_count: usize,
) -> ReplayCodecResult<()> {
    for instruction in instructions {
        check_length(
            instruction.account_indexes.len(),
            0,
            MAX_REPLAY_INSTRUCTION_ACCOUNT_INDEXES,
            "ReplayInstructionV1.account_indexes",
        )?;
        check_length(
            instruction.data.len(),
            0,
            MAX_REPLAY_INSTRUCTION_DATA_BYTES,
            "ReplayInstructionV1.data",
        )?;
        if usize::from(instruction.program_id_index) >= account_count {
            return Err(ReplayCodecError::InvalidValue {
                field: "ReplayInstructionV1.program_id_index",
                reason: "index is outside the resolved account list",
            });
        }
        if instruction
            .account_indexes
            .iter()
            .any(|index| usize::from(*index) >= account_count)
        {
            return Err(ReplayCodecError::InvalidValue {
                field: "ReplayInstructionV1.account_indexes",
                reason: "an index is outside the resolved account list",
            });
        }
    }
    Ok(())
}

fn validate_expanded_message_len(length: usize) -> ReplayCodecResult<()> {
    if length > MAX_REPLAY_EXPANDED_MESSAGE_BYTES {
        return Err(ReplayCodecError::LengthOutOfBounds {
            field: "ReplayMessageV1.expanded_signed_message",
            min: 0,
            max: MAX_REPLAY_EXPANDED_MESSAGE_BYTES,
            actual: length,
        });
    }
    Ok(())
}

fn expanded_legacy_message_len(message: &ReplayLegacyMessageV1<'_>) -> ReplayCodecResult<usize> {
    expanded_message_common_len(&message.static_account_keys, &message.instructions)
}

fn expanded_message_common_len(
    static_account_keys: &[ReplayAddressRefV1],
    instructions: &[ReplayInstructionV1<'_>],
) -> ReplayCodecResult<usize> {
    let mut length = 3usize;
    add_len(
        &mut length,
        short_vec_len(static_account_keys.len()),
        "compact message",
    )?;
    add_len(
        &mut length,
        static_account_keys
            .len()
            .checked_mul(32)
            .ok_or(ReplayCodecError::ArithmeticOverflow {
                field: "ReplayMessageV1.static_account_keys",
            })?,
        "compact message",
    )?;
    add_len(&mut length, 32, "compact message")?;
    add_instruction_section_len(&mut length, instructions, "compact message")?;
    Ok(length)
}

fn expanded_v0_message_len(message: &ReplayV0MessageV1<'_>) -> ReplayCodecResult<usize> {
    let mut length =
        expanded_message_common_len(&message.static_account_keys, &message.instructions)?;
    add_len(&mut length, 1, "V0 message")?;
    add_len(
        &mut length,
        short_vec_len(message.address_table_lookups.len()),
        "V0 message",
    )?;
    for lookup in &message.address_table_lookups {
        add_len(&mut length, 32, "V0 message")?;
        add_len(
            &mut length,
            short_vec_len(lookup.writable_indexes.len()),
            "V0 message",
        )?;
        add_len(&mut length, lookup.writable_indexes.len(), "V0 message")?;
        add_len(
            &mut length,
            short_vec_len(lookup.readonly_indexes.len()),
            "V0 message",
        )?;
        add_len(&mut length, lookup.readonly_indexes.len(), "V0 message")?;
    }
    Ok(length)
}

fn add_instruction_section_len(
    length: &mut usize,
    instructions: &[ReplayInstructionV1<'_>],
    field: &'static str,
) -> ReplayCodecResult<()> {
    add_len(length, short_vec_len(instructions.len()), field)?;
    for instruction in instructions {
        add_len(length, 1, field)?;
        add_len(
            length,
            short_vec_len(instruction.account_indexes.len()),
            field,
        )?;
        add_len(length, instruction.account_indexes.len(), field)?;
        add_len(length, short_vec_len(instruction.data.len()), field)?;
        add_len(length, instruction.data.len(), field)?;
    }
    Ok(())
}

fn short_vec_len(mut value: usize) -> usize {
    let mut length = 1usize;
    while value >= 0x80 {
        value >>= 7;
        length += 1;
    }
    length
}

fn add_len(total: &mut usize, value: usize, field: &'static str) -> ReplayCodecResult<()> {
    *total = total
        .checked_add(value)
        .ok_or(ReplayCodecError::ArithmeticOverflow { field })?;
    Ok(())
}

fn check_count(
    actual: usize,
    min: usize,
    max: usize,
    field: &'static str,
) -> ReplayCodecResult<()> {
    if actual < min || actual > max {
        return Err(ReplayCodecError::CountOutOfBounds {
            field,
            min,
            max,
            actual: actual as u64,
        });
    }
    Ok(())
}

fn check_length(
    actual: usize,
    min: usize,
    max: usize,
    field: &'static str,
) -> ReplayCodecResult<()> {
    if actual < min || actual > max {
        return Err(ReplayCodecError::LengthOutOfBounds {
            field,
            min,
            max,
            actual,
        });
    }
    Ok(())
}

fn checked_add_count(
    total: u64,
    added: usize,
    max: u64,
    field: &'static str,
) -> ReplayCodecResult<u64> {
    let added = u64::try_from(added).map_err(|_| ReplayCodecError::ArithmeticOverflow { field })?;
    let actual = total
        .checked_add(added)
        .ok_or(ReplayCodecError::ArithmeticOverflow { field })?;
    if actual > max {
        return Err(ReplayCodecError::AggregateOutOfBounds { field, max, actual });
    }
    Ok(actual)
}

fn encode_slot(encoder: &mut Encoder<'_>, slot: &ReplaySlotV1<'_>) -> ReplayCodecResult<()> {
    encoder.leb_u32(slot.components.len(), "ReplaySlotV1.components")?;
    for component in &slot.components {
        match component {
            ReplayComponentV1::EntryBatch(entries) => {
                encoder.u8(COMPONENT_ENTRY_BATCH_TAG)?;
                encoder.leb_u32(entries.len(), "ReplayComponentV1.EntryBatch.entries")?;
                for entry in entries {
                    encode_entry(encoder, entry)?;
                }
            }
            ReplayComponentV1::BlockMarker(bytes) => {
                encoder.u8(COMPONENT_BLOCK_MARKER_TAG)?;
                encoder.bytes(bytes, "ReplayComponentV1.BlockMarker.bytes")?;
            }
        }
    }
    Ok(())
}

fn encode_entry(encoder: &mut Encoder<'_>, entry: &ReplayEntryV1<'_>) -> ReplayCodecResult<()> {
    encoder.leb_u64(entry.num_hashes)?;
    encoder.leb_u32(entry.transactions.len(), "ReplayEntryV1.transactions")?;
    if let Some(signature_mixin) = entry.signature_mixin {
        encoder.raw(&signature_mixin)?;
    }
    for transaction in &entry.transactions {
        encode_transaction(encoder, transaction)?;
    }
    Ok(())
}

fn encode_transaction(
    encoder: &mut Encoder<'_>,
    transaction: &ReplayTransactionV1<'_>,
) -> ReplayCodecResult<()> {
    let message_tag = match transaction.message {
        ReplayMessageV1::Legacy(_) => MESSAGE_LEGACY_TAG,
        ReplayMessageV1::V0(_) => MESSAGE_V0_TAG,
        ReplayMessageV1::Raw(_) => MESSAGE_RAW_TAG,
    };
    let tag = if transaction.historical_status_backref.is_some() {
        message_tag | TRANSACTION_STATUS_REF_BIT
    } else {
        message_tag
    };
    encoder.u8(tag)?;
    if let Some(status_ref) = transaction.historical_status_backref {
        match status_ref {
            StatusKeyClassRefV1::PriorTxDistance(distance) => {
                encoder.u8(STATUS_PRIOR_TX_DISTANCE_TAG)?;
                encoder.leb_u64(distance)?;
            }
            StatusKeyClassRefV1::PreviousClassId(class_id) => {
                encoder.u8(STATUS_PREVIOUS_CLASS_ID_TAG)?;
                encoder.raw(&class_id)?;
            }
        }
    }
    match &transaction.message {
        ReplayMessageV1::Legacy(message) => encode_legacy_message(encoder, message),
        ReplayMessageV1::V0(message) => encode_v0_message(encoder, message),
        ReplayMessageV1::Raw(message) => encoder.bytes(
            message.signed_message_bytes,
            "ReplayRawMessageV1.signed_message_bytes",
        ),
    }
}

fn encode_legacy_message(
    encoder: &mut Encoder<'_>,
    message: &ReplayLegacyMessageV1<'_>,
) -> ReplayCodecResult<()> {
    encoder.raw(&message.header)?;
    encode_address_refs(encoder, &message.static_account_keys)?;
    encode_recent_blockhash(encoder, message.recent_blockhash)?;
    encode_instructions(encoder, &message.instructions)
}

fn encode_v0_message(
    encoder: &mut Encoder<'_>,
    message: &ReplayV0MessageV1<'_>,
) -> ReplayCodecResult<()> {
    encoder.raw(&message.header)?;
    encode_address_refs(encoder, &message.static_account_keys)?;
    encode_recent_blockhash(encoder, message.recent_blockhash)?;
    encode_instructions(encoder, &message.instructions)?;
    encoder.leb_u32(
        message.address_table_lookups.len(),
        "ReplayV0MessageV1.address_table_lookups",
    )?;
    for lookup in &message.address_table_lookups {
        encode_address_ref(encoder, lookup.table_account)?;
        encoder.bytes(
            lookup.writable_indexes,
            "ReplayAddressTableLookupV1.writable_indexes",
        )?;
        encoder.bytes(
            lookup.readonly_indexes,
            "ReplayAddressTableLookupV1.readonly_indexes",
        )?;
    }
    Ok(())
}

fn encode_address_refs(
    encoder: &mut Encoder<'_>,
    addresses: &[ReplayAddressRefV1],
) -> ReplayCodecResult<()> {
    encoder.leb_u32(addresses.len(), "ReplayMessageV1.static_account_keys")?;
    for address in addresses {
        encode_address_ref(encoder, *address)?;
    }
    Ok(())
}

fn encode_address_ref(
    encoder: &mut Encoder<'_>,
    address: ReplayAddressRefV1,
) -> ReplayCodecResult<()> {
    match address {
        ReplayAddressRefV1::RegistryId(id) => encoder.leb_u32_value(id),
        ReplayAddressRefV1::Raw(pubkey) => {
            encoder.leb_u32_value(0)?;
            encoder.raw(&pubkey)
        }
    }
}

fn encode_recent_blockhash(
    encoder: &mut Encoder<'_>,
    blockhash: RecentBlockhashRefV1,
) -> ReplayCodecResult<()> {
    match blockhash {
        RecentBlockhashRefV1::PriorProducedDistance(distance) => {
            encoder.u8(BLOCKHASH_PRIOR_PRODUCED_DISTANCE_TAG)?;
            encoder.leb_u32_value(distance)
        }
        RecentBlockhashRefV1::PreviousTailIndex(index) => {
            encoder.u8(BLOCKHASH_PREVIOUS_TAIL_INDEX_TAG)?;
            encoder.leb_u32_value(index)
        }
        RecentBlockhashRefV1::Raw(blockhash) => {
            encoder.u8(BLOCKHASH_RAW_TAG)?;
            encoder.raw(&blockhash)
        }
    }
}

fn encode_instructions(
    encoder: &mut Encoder<'_>,
    instructions: &[ReplayInstructionV1<'_>],
) -> ReplayCodecResult<()> {
    encoder.leb_u32(instructions.len(), "ReplayMessageV1.instructions")?;
    for instruction in instructions {
        encoder.u8(instruction.program_id_index)?;
        encoder.bytes(
            instruction.account_indexes,
            "ReplayInstructionV1.account_indexes",
        )?;
        encoder.bytes(instruction.data, "ReplayInstructionV1.data")?;
    }
    Ok(())
}

struct ReplayEntryHeaderV1 {
    num_hashes: u64,
    signature_mixin: Option<[u8; 32]>,
    transaction_count: usize,
}

fn decode_entry_header(
    decoder: &mut Decoder<'_>,
    remaining_slot_transactions: usize,
    remaining_slot_num_hashes: u64,
) -> ReplayCodecResult<ReplayEntryHeaderV1> {
    let num_hashes = decoder.leb_u64("ReplayEntryV1.num_hashes")?;
    if num_hashes > MAX_REPLAY_NUM_HASHES_PER_ENTRY {
        return Err(ReplayCodecError::AggregateOutOfBounds {
            field: "ReplayEntryV1.num_hashes",
            max: MAX_REPLAY_NUM_HASHES_PER_ENTRY,
            actual: num_hashes,
        });
    }
    if num_hashes > remaining_slot_num_hashes {
        return Err(ReplayCodecError::AggregateOutOfBounds {
            field: "ReplaySlotV1.num_hashes",
            max: MAX_REPLAY_NUM_HASHES_PER_SLOT,
            actual: MAX_REPLAY_NUM_HASHES_PER_SLOT
                .saturating_sub(remaining_slot_num_hashes)
                .saturating_add(num_hashes),
        });
    }
    let transaction_count = decoder.count(
        0,
        MAX_REPLAY_TRANSACTIONS_PER_ENTRY.min(remaining_slot_transactions),
        "ReplayEntryV1.transactions",
    )?;
    let signature_mixin = if transaction_count == 0 {
        None
    } else {
        Some(decoder.array("ReplayEntryV1.signature_mixin")?)
    };
    Ok(ReplayEntryHeaderV1 {
        num_hashes,
        signature_mixin,
        transaction_count,
    })
}

struct ExpandedMessageBudget {
    length: usize,
}

impl ExpandedMessageBudget {
    fn new(length: usize) -> ReplayCodecResult<Self> {
        validate_expanded_message_len(length)?;
        Ok(Self { length })
    }

    fn add(&mut self, added: usize) -> ReplayCodecResult<()> {
        let actual =
            self.length
                .checked_add(added)
                .ok_or(ReplayCodecError::ArithmeticOverflow {
                    field: "ReplayMessageV1.expanded_signed_message",
                })?;
        if actual > MAX_REPLAY_EXPANDED_MESSAGE_BYTES {
            return Err(ReplayCodecError::LengthOutOfBounds {
                field: "ReplayMessageV1.expanded_signed_message",
                min: 0,
                max: MAX_REPLAY_EXPANDED_MESSAGE_BYTES,
                actual,
            });
        }
        self.length = actual;
        Ok(())
    }

    fn add_product(
        &mut self,
        count: usize,
        width: usize,
        field: &'static str,
    ) -> ReplayCodecResult<()> {
        let added = count
            .checked_mul(width)
            .ok_or(ReplayCodecError::ArithmeticOverflow { field })?;
        self.add(added)
    }
}

#[derive(Debug, Clone, Copy)]
struct InstructionSectionScan {
    count: usize,
    max_program_id_index: Option<u8>,
    max_account_index: Option<u8>,
}

#[derive(Debug, Clone, Copy)]
struct AddressTableLookupSectionScan {
    count: usize,
    loaded_account_count: usize,
}

fn decode_transaction<'a>(decoder: &mut Decoder<'a>) -> ReplayCodecResult<ReplayTransactionV1<'a>> {
    let tag = decoder.u8("ReplayTransactionV1.tag")?;
    if tag & TRANSACTION_RESERVED_BITS != 0 {
        return Err(ReplayCodecError::ReservedBitsSet {
            field: "ReplayTransactionV1.tag",
            value: tag,
        });
    }
    let historical_status_backref = if tag & TRANSACTION_STATUS_REF_BIT == 0 {
        None
    } else {
        let status_tag = decoder.u8("StatusKeyClassRefV1.tag")?;
        Some(match status_tag {
            STATUS_PRIOR_TX_DISTANCE_TAG => {
                let distance = decoder.leb_u64("StatusKeyClassRefV1.PriorTxDistance")?;
                if distance == 0 {
                    return Err(ReplayCodecError::InvalidValue {
                        field: "StatusKeyClassRefV1.PriorTxDistance",
                        reason: "distance must be non-zero",
                    });
                }
                StatusKeyClassRefV1::PriorTxDistance(distance)
            }
            STATUS_PREVIOUS_CLASS_ID_TAG => StatusKeyClassRefV1::PreviousClassId(
                decoder.array("StatusKeyClassRefV1.PreviousClassId")?,
            ),
            value => {
                return Err(ReplayCodecError::UnknownTag {
                    field: "StatusKeyClassRefV1",
                    value,
                });
            }
        })
    };

    let message = match tag & 0b11 {
        MESSAGE_LEGACY_TAG => ReplayMessageV1::Legacy(decode_legacy_message(decoder)?),
        MESSAGE_V0_TAG => ReplayMessageV1::V0(decode_v0_message(decoder)?),
        MESSAGE_RAW_TAG => {
            let message = ReplayRawMessageV1 {
                signed_message_bytes: decoder.bytes(
                    1,
                    MAX_REPLAY_RAW_MESSAGE_BYTES,
                    "ReplayRawMessageV1.signed_message_bytes",
                )?,
            };
            validate_raw_message(&message)?;
            ReplayMessageV1::Raw(message)
        }
        value => {
            return Err(ReplayCodecError::UnknownTag {
                field: "ReplayMessageV1",
                value,
            });
        }
    };
    Ok(ReplayTransactionV1 {
        historical_status_backref,
        message,
    })
}

fn decode_legacy_message<'a>(
    decoder: &mut Decoder<'a>,
) -> ReplayCodecResult<ReplayLegacyMessageV1<'a>> {
    let mut scan = decoder.fork();
    let header = scan.array("ReplayLegacyMessageV1.header")?;
    let mut budget = ExpandedMessageBudget::new(3)?;
    let static_account_key_count = scan_address_refs(&mut scan, &mut budget, header)?;
    budget.add(32)?;
    let _ = decode_recent_blockhash(&mut scan)?;
    let instruction_scan = scan_instructions(&mut scan, &mut budget)?;
    validate_instruction_scan(instruction_scan, static_account_key_count)?;
    let expected_end = scan.offset;

    let materialized_header = decoder.array("ReplayLegacyMessageV1.header")?;
    let static_account_keys = materialize_address_refs(decoder, static_account_key_count, header)?;
    let recent_blockhash = decode_recent_blockhash(decoder)?;
    let instructions =
        materialize_instructions(decoder, instruction_scan.count, static_account_key_count)?;
    ensure_materialized_scan_end(decoder, expected_end)?;
    debug_assert_eq!(materialized_header, header);

    Ok(ReplayLegacyMessageV1 {
        header: materialized_header,
        static_account_keys,
        recent_blockhash,
        instructions,
    })
}

fn decode_v0_message<'a>(decoder: &mut Decoder<'a>) -> ReplayCodecResult<ReplayV0MessageV1<'a>> {
    let mut scan = decoder.fork();
    let header = scan.array("ReplayV0MessageV1.header")?;
    let mut budget = ExpandedMessageBudget::new(4)?;
    let static_account_key_count = scan_address_refs(&mut scan, &mut budget, header)?;
    budget.add(32)?;
    let _ = decode_recent_blockhash(&mut scan)?;
    let instruction_scan = scan_instructions(&mut scan, &mut budget)?;
    let lookup_scan = scan_address_table_lookups(&mut scan, &mut budget, static_account_key_count)?;
    let total_account_count = static_account_key_count
        .checked_add(lookup_scan.loaded_account_count)
        .ok_or(ReplayCodecError::ArithmeticOverflow {
            field: "ReplayV0MessageV1.account_count",
        })?;
    validate_instruction_scan(instruction_scan, total_account_count)?;
    let expected_end = scan.offset;

    let materialized_header = decoder.array("ReplayV0MessageV1.header")?;
    let static_account_keys = materialize_address_refs(decoder, static_account_key_count, header)?;
    let recent_blockhash = decode_recent_blockhash(decoder)?;
    let instructions =
        materialize_instructions(decoder, instruction_scan.count, total_account_count)?;
    let address_table_lookups =
        materialize_address_table_lookups(decoder, lookup_scan, static_account_key_count)?;
    ensure_materialized_scan_end(decoder, expected_end)?;
    debug_assert_eq!(materialized_header, header);

    Ok(ReplayV0MessageV1 {
        header: materialized_header,
        static_account_keys,
        recent_blockhash,
        instructions,
        address_table_lookups,
    })
}

fn scan_address_refs(
    decoder: &mut Decoder<'_>,
    budget: &mut ExpandedMessageBudget,
    header: [u8; 3],
) -> ReplayCodecResult<usize> {
    let count = decoder.count(
        0,
        MAX_REPLAY_STATIC_ACCOUNT_KEYS,
        "ReplayMessageV1.static_account_keys",
    )?;
    validate_message_header(count, header)?;
    decoder.ensure_minimum_remaining(count, "ReplayMessageV1.static_account_keys")?;
    budget.add(short_vec_len(count))?;
    budget.add_product(count, 32, "ReplayMessageV1.static_account_keys")?;
    for _ in 0..count {
        let _ = decode_address_ref(decoder)?;
    }
    Ok(count)
}

fn materialize_address_refs(
    decoder: &mut Decoder<'_>,
    expected_count: usize,
    header: [u8; 3],
) -> ReplayCodecResult<Vec<ReplayAddressRefV1>> {
    let count = decoder.count(
        0,
        MAX_REPLAY_STATIC_ACCOUNT_KEYS,
        "ReplayMessageV1.static_account_keys",
    )?;
    if count != expected_count {
        return Err(scan_materialization_mismatch());
    }
    validate_message_header(count, header)?;
    decoder.ensure_minimum_remaining(count, "ReplayMessageV1.static_account_keys")?;
    let mut addresses = Vec::with_capacity(count);
    for _ in 0..count {
        addresses.push(decode_address_ref(decoder)?);
    }
    Ok(addresses)
}

fn scan_instructions(
    decoder: &mut Decoder<'_>,
    budget: &mut ExpandedMessageBudget,
) -> ReplayCodecResult<InstructionSectionScan> {
    let count = decoder.count(
        0,
        MAX_REPLAY_INSTRUCTIONS_PER_MESSAGE,
        "ReplayMessageV1.instructions",
    )?;
    let minimum_wire_bytes = checked_minimum_wire_bytes(count, 3, "ReplayMessageV1.instructions")?;
    decoder.ensure_minimum_remaining(minimum_wire_bytes, "ReplayMessageV1.instructions")?;
    budget.add(short_vec_len(count))?;
    budget.add(minimum_wire_bytes)?;

    let mut max_program_id_index = None;
    let mut max_account_index = None;
    for _ in 0..count {
        let program_id_index = decoder.u8("ReplayInstructionV1.program_id_index")?;
        max_program_id_index = max_u8(max_program_id_index, program_id_index);

        let account_index_count = decoder.count(
            0,
            MAX_REPLAY_INSTRUCTION_ACCOUNT_INDEXES,
            "ReplayInstructionV1.account_indexes",
        )?;
        budget.add(short_vec_len(account_index_count) - 1)?;
        budget.add(account_index_count)?;
        let account_indexes =
            decoder.take(account_index_count, "ReplayInstructionV1.account_indexes")?;
        for index in account_indexes {
            max_account_index = max_u8(max_account_index, *index);
        }

        let data_length = decoder.count(
            0,
            MAX_REPLAY_INSTRUCTION_DATA_BYTES,
            "ReplayInstructionV1.data",
        )?;
        budget.add(short_vec_len(data_length) - 1)?;
        budget.add(data_length)?;
        let _ = decoder.take(data_length, "ReplayInstructionV1.data")?;
    }

    Ok(InstructionSectionScan {
        count,
        max_program_id_index,
        max_account_index,
    })
}

fn validate_instruction_scan(
    scan: InstructionSectionScan,
    account_count: usize,
) -> ReplayCodecResult<()> {
    if scan
        .max_program_id_index
        .is_some_and(|index| usize::from(index) >= account_count)
    {
        return Err(ReplayCodecError::InvalidValue {
            field: "ReplayInstructionV1.program_id_index",
            reason: "index is outside the resolved account list",
        });
    }
    if scan
        .max_account_index
        .is_some_and(|index| usize::from(index) >= account_count)
    {
        return Err(ReplayCodecError::InvalidValue {
            field: "ReplayInstructionV1.account_indexes",
            reason: "an index is outside the resolved account list",
        });
    }
    Ok(())
}

fn materialize_instructions<'a>(
    decoder: &mut Decoder<'a>,
    expected_count: usize,
    account_count: usize,
) -> ReplayCodecResult<Vec<ReplayInstructionV1<'a>>> {
    let count = decoder.count(
        0,
        MAX_REPLAY_INSTRUCTIONS_PER_MESSAGE,
        "ReplayMessageV1.instructions",
    )?;
    if count != expected_count {
        return Err(scan_materialization_mismatch());
    }
    let minimum_wire_bytes = checked_minimum_wire_bytes(count, 3, "ReplayMessageV1.instructions")?;
    decoder.ensure_minimum_remaining(minimum_wire_bytes, "ReplayMessageV1.instructions")?;

    let mut instructions = Vec::with_capacity(count);
    for _ in 0..count {
        let program_id_index = decoder.u8("ReplayInstructionV1.program_id_index")?;
        if usize::from(program_id_index) >= account_count {
            return Err(ReplayCodecError::InvalidValue {
                field: "ReplayInstructionV1.program_id_index",
                reason: "index is outside the resolved account list",
            });
        }
        let account_indexes = decoder.bytes(
            0,
            MAX_REPLAY_INSTRUCTION_ACCOUNT_INDEXES,
            "ReplayInstructionV1.account_indexes",
        )?;
        if account_indexes
            .iter()
            .any(|index| usize::from(*index) >= account_count)
        {
            return Err(ReplayCodecError::InvalidValue {
                field: "ReplayInstructionV1.account_indexes",
                reason: "an index is outside the resolved account list",
            });
        }
        let data = decoder.bytes(
            0,
            MAX_REPLAY_INSTRUCTION_DATA_BYTES,
            "ReplayInstructionV1.data",
        )?;
        instructions.push(ReplayInstructionV1 {
            program_id_index,
            account_indexes,
            data,
        });
    }
    Ok(instructions)
}

fn scan_address_table_lookups(
    decoder: &mut Decoder<'_>,
    budget: &mut ExpandedMessageBudget,
    static_account_key_count: usize,
) -> ReplayCodecResult<AddressTableLookupSectionScan> {
    let count = decoder.count(
        0,
        MAX_REPLAY_ADDRESS_TABLE_LOOKUPS,
        "ReplayV0MessageV1.address_table_lookups",
    )?;
    let minimum_wire_bytes =
        checked_minimum_wire_bytes(count, 3, "ReplayV0MessageV1.address_table_lookups")?;
    decoder.ensure_minimum_remaining(
        minimum_wire_bytes,
        "ReplayV0MessageV1.address_table_lookups",
    )?;
    budget.add(short_vec_len(count))?;
    budget.add_product(count, 34, "ReplayV0MessageV1.address_table_lookups")?;

    let mut loaded_account_count = 0usize;
    for _ in 0..count {
        let _ = decode_address_ref(decoder)?;
        let writable_count = decoder.count(
            0,
            MAX_REPLAY_LOOKUP_INDEXES,
            "ReplayAddressTableLookupV1.writable_indexes",
        )?;
        budget.add(short_vec_len(writable_count) - 1)?;
        budget.add(writable_count)?;
        loaded_account_count = add_loaded_account_count(
            static_account_key_count,
            loaded_account_count,
            writable_count,
        )?;
        let _ = decoder.take(
            writable_count,
            "ReplayAddressTableLookupV1.writable_indexes",
        )?;

        let readonly_count = decoder.count(
            0,
            MAX_REPLAY_LOOKUP_INDEXES,
            "ReplayAddressTableLookupV1.readonly_indexes",
        )?;
        budget.add(short_vec_len(readonly_count) - 1)?;
        budget.add(readonly_count)?;
        loaded_account_count = add_loaded_account_count(
            static_account_key_count,
            loaded_account_count,
            readonly_count,
        )?;
        let _ = decoder.take(
            readonly_count,
            "ReplayAddressTableLookupV1.readonly_indexes",
        )?;
    }

    Ok(AddressTableLookupSectionScan {
        count,
        loaded_account_count,
    })
}

fn materialize_address_table_lookups<'a>(
    decoder: &mut Decoder<'a>,
    expected: AddressTableLookupSectionScan,
    static_account_key_count: usize,
) -> ReplayCodecResult<Vec<ReplayAddressTableLookupV1<'a>>> {
    let count = decoder.count(
        0,
        MAX_REPLAY_ADDRESS_TABLE_LOOKUPS,
        "ReplayV0MessageV1.address_table_lookups",
    )?;
    if count != expected.count {
        return Err(scan_materialization_mismatch());
    }
    let minimum_wire_bytes =
        checked_minimum_wire_bytes(count, 3, "ReplayV0MessageV1.address_table_lookups")?;
    decoder.ensure_minimum_remaining(
        minimum_wire_bytes,
        "ReplayV0MessageV1.address_table_lookups",
    )?;

    let mut loaded_account_count = 0usize;
    let mut address_table_lookups = Vec::with_capacity(count);
    for _ in 0..count {
        let table_account = decode_address_ref(decoder)?;
        let writable_indexes = decoder.bytes(
            0,
            MAX_REPLAY_LOOKUP_INDEXES,
            "ReplayAddressTableLookupV1.writable_indexes",
        )?;
        loaded_account_count = add_loaded_account_count(
            static_account_key_count,
            loaded_account_count,
            writable_indexes.len(),
        )?;
        let readonly_indexes = decoder.bytes(
            0,
            MAX_REPLAY_LOOKUP_INDEXES,
            "ReplayAddressTableLookupV1.readonly_indexes",
        )?;
        loaded_account_count = add_loaded_account_count(
            static_account_key_count,
            loaded_account_count,
            readonly_indexes.len(),
        )?;
        address_table_lookups.push(ReplayAddressTableLookupV1 {
            table_account,
            writable_indexes,
            readonly_indexes,
        });
    }
    if loaded_account_count != expected.loaded_account_count {
        return Err(scan_materialization_mismatch());
    }
    Ok(address_table_lookups)
}

fn add_loaded_account_count(
    static_account_key_count: usize,
    loaded_account_count: usize,
    added: usize,
) -> ReplayCodecResult<usize> {
    let loaded_account_count =
        loaded_account_count
            .checked_add(added)
            .ok_or(ReplayCodecError::ArithmeticOverflow {
                field: "ReplayV0MessageV1.loaded_account_count",
            })?;
    let actual = static_account_key_count
        .checked_add(loaded_account_count)
        .ok_or(ReplayCodecError::ArithmeticOverflow {
            field: "ReplayV0MessageV1.account_count",
        })?;
    if actual > 256 {
        return Err(ReplayCodecError::CountOutOfBounds {
            field: "ReplayV0MessageV1.account_count",
            min: 0,
            max: 256,
            actual: actual as u64,
        });
    }
    Ok(loaded_account_count)
}

fn checked_minimum_wire_bytes(
    count: usize,
    bytes_per_item: usize,
    field: &'static str,
) -> ReplayCodecResult<usize> {
    count
        .checked_mul(bytes_per_item)
        .ok_or(ReplayCodecError::ArithmeticOverflow { field })
}

fn max_u8(current: Option<u8>, value: u8) -> Option<u8> {
    Some(current.map_or(value, |current| current.max(value)))
}

fn ensure_materialized_scan_end(
    decoder: &Decoder<'_>,
    expected_end: usize,
) -> ReplayCodecResult<()> {
    if decoder.offset == expected_end {
        Ok(())
    } else {
        Err(scan_materialization_mismatch())
    }
}

fn scan_materialization_mismatch() -> ReplayCodecError {
    ReplayCodecError::InvalidValue {
        field: "ReplayMessageV1.decoder",
        reason: "allocation-free scan and materialization disagree",
    }
}

fn decode_address_ref(decoder: &mut Decoder<'_>) -> ReplayCodecResult<ReplayAddressRefV1> {
    let id = decoder.leb_u32("ReplayAddressRefV1")?;
    if id == 0 {
        Ok(ReplayAddressRefV1::Raw(
            decoder.array("ReplayAddressRefV1.Raw")?,
        ))
    } else {
        let address = ReplayAddressRefV1::RegistryId(id);
        validate_address_ref(address)?;
        Ok(address)
    }
}

fn decode_recent_blockhash(decoder: &mut Decoder<'_>) -> ReplayCodecResult<RecentBlockhashRefV1> {
    let blockhash = match decoder.u8("RecentBlockhashRefV1.tag")? {
        BLOCKHASH_PRIOR_PRODUCED_DISTANCE_TAG => Ok(RecentBlockhashRefV1::PriorProducedDistance(
            decoder.leb_u32("RecentBlockhashRefV1.PriorProducedDistance")?,
        )),
        BLOCKHASH_PREVIOUS_TAIL_INDEX_TAG => Ok(RecentBlockhashRefV1::PreviousTailIndex(
            decoder.leb_u32("RecentBlockhashRefV1.PreviousTailIndex")?,
        )),
        BLOCKHASH_RAW_TAG => Ok(RecentBlockhashRefV1::Raw(
            decoder.array("RecentBlockhashRefV1.Raw")?,
        )),
        value => Err(ReplayCodecError::UnknownTag {
            field: "RecentBlockhashRefV1",
            value,
        }),
    }?;
    validate_recent_blockhash(blockhash)?;
    Ok(blockhash)
}

struct Encoder<'a> {
    output: &'a mut Vec<u8>,
    start: usize,
}

impl Encoder<'_> {
    fn raw(&mut self, bytes: &[u8]) -> ReplayCodecResult<()> {
        let encoded_len = self.output.len().saturating_sub(self.start);
        if bytes.len() > MAX_REPLAY_SLOT_BYTES.saturating_sub(encoded_len) {
            return Err(ReplayCodecError::OutputTooLarge {
                max: MAX_REPLAY_SLOT_BYTES,
            });
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }

    fn u8(&mut self, value: u8) -> ReplayCodecResult<()> {
        self.raw(&[value])
    }

    fn leb_u32(&mut self, value: usize, field: &'static str) -> ReplayCodecResult<()> {
        let value =
            u32::try_from(value).map_err(|_| ReplayCodecError::ArithmeticOverflow { field })?;
        self.leb_u32_value(value)
    }

    fn leb_u32_value(&mut self, value: u32) -> ReplayCodecResult<()> {
        self.leb_u64(u64::from(value))
    }

    fn leb_u64(&mut self, mut value: u64) -> ReplayCodecResult<()> {
        let mut bytes = [0u8; 10];
        let mut length = 0usize;
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            bytes[length] = byte;
            length += 1;
            if value == 0 {
                break;
            }
        }
        self.raw(&bytes[..length])
    }

    fn bytes(&mut self, value: &[u8], field: &'static str) -> ReplayCodecResult<()> {
        self.leb_u32(value.len(), field)?;
        self.raw(value)
    }
}

struct Decoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn fork(&self) -> Self {
        Self {
            input: self.input,
            offset: self.offset,
        }
    }

    fn ensure_minimum_remaining(
        &self,
        needed: usize,
        field: &'static str,
    ) -> ReplayCodecResult<()> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if needed > remaining {
            return Err(ReplayCodecError::Truncated {
                field,
                needed,
                remaining,
            });
        }
        Ok(())
    }

    fn take(&mut self, length: usize, field: &'static str) -> ReplayCodecResult<&'a [u8]> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if length > remaining {
            return Err(ReplayCodecError::Truncated {
                field,
                needed: length,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    fn array<const N: usize>(&mut self, field: &'static str) -> ReplayCodecResult<[u8; N]> {
        Ok(self
            .take(N, field)?
            .try_into()
            .expect("slice length was checked"))
    }

    fn u8(&mut self, field: &'static str) -> ReplayCodecResult<u8> {
        Ok(self.take(1, field)?[0])
    }

    fn leb_u32(&mut self, field: &'static str) -> ReplayCodecResult<u32> {
        let value = self.leb(32, field)?;
        Ok(value as u32)
    }

    fn leb_u64(&mut self, field: &'static str) -> ReplayCodecResult<u64> {
        self.leb(64, field)
    }

    fn leb(&mut self, bits: u32, field: &'static str) -> ReplayCodecResult<u64> {
        let max_bytes = bits.div_ceil(7) as usize;
        let mut value = 0u64;
        for index in 0..max_bytes {
            let byte = self.u8(field)?;
            let payload = u64::from(byte & 0x7f);
            let shift = (index * 7) as u32;
            if shift >= 64 || payload > (u64::MAX >> shift) {
                return Err(ReplayCodecError::Leb128Overflow { field });
            }
            value |= payload << shift;

            if byte & 0x80 == 0 {
                if index > 0 && payload == 0 {
                    return Err(ReplayCodecError::NonMinimalLeb128 { field });
                }
                let max = if bits == 64 {
                    u64::MAX
                } else {
                    (1u64 << bits) - 1
                };
                if value > max {
                    return Err(ReplayCodecError::Leb128Overflow { field });
                }
                return Ok(value);
            }
        }
        Err(ReplayCodecError::Leb128Overflow { field })
    }

    fn count(&mut self, min: usize, max: usize, field: &'static str) -> ReplayCodecResult<usize> {
        let actual = u64::from(self.leb_u32(field)?);
        if actual < min as u64 || actual > max as u64 {
            return Err(ReplayCodecError::CountOutOfBounds {
                field,
                min,
                max,
                actual,
            });
        }
        Ok(actual as usize)
    }

    fn bytes(
        &mut self,
        min: usize,
        max: usize,
        field: &'static str,
    ) -> ReplayCodecResult<&'a [u8]> {
        let length = self.count(min, max, field)?;
        self.take(length, field)
    }

    fn finish(&self) -> ReplayCodecResult<()> {
        let count = self.input.len().saturating_sub(self.offset);
        if count == 0 {
            Ok(())
        } else {
            Err(ReplayCodecError::TrailingBytes { count })
        }
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;

    fn legacy_slot<'a>(accounts: &'a [u8], data: &'a [u8]) -> ReplaySlotV1<'a> {
        ReplaySlotV1 {
            components: vec![ReplayComponentV1::EntryBatch(vec![ReplayEntryV1 {
                num_hashes: 1,
                signature_mixin: Some([0xaa; 32]),
                transactions: vec![ReplayTransactionV1 {
                    historical_status_backref: None,
                    message: ReplayMessageV1::Legacy(ReplayLegacyMessageV1 {
                        header: [1, 0, 0],
                        static_account_keys: vec![ReplayAddressRefV1::Raw([0x11; 32])],
                        recent_blockhash: RecentBlockhashRefV1::Raw([0x22; 32]),
                        instructions: vec![ReplayInstructionV1 {
                            program_id_index: 0,
                            account_indexes: accounts,
                            data,
                        }],
                    }),
                }],
            }])],
        }
    }

    fn naive_signature_mixin(signatures: &[[u8; 64]]) -> [u8; 32] {
        if signatures.is_empty() {
            return [0; 32];
        }
        let mut level: Vec<[u8; 32]> = signatures.iter().map(signature_leaf_hash).collect();
        while level.len() > 1 {
            if !level.len().is_multiple_of(2) {
                level.push(*level.last().unwrap());
            }
            level = level
                .chunks_exact(2)
                .map(|pair| signature_node_hash(pair[0], pair[1]))
                .collect();
        }
        level[0]
    }

    fn single_transaction_prefix(transaction_tag: u8) -> Vec<u8> {
        let mut encoded = vec![1, 0, 1, 1, 1];
        encoded.extend_from_slice(&[0xaa; 32]);
        encoded.push(transaction_tag);
        encoded
    }

    #[test]
    fn streaming_signature_mixin_matches_frozen_merkle_rule() {
        let signatures: Vec<[u8; 64]> = (0u8..20).map(|value| [value; 64]).collect();
        assert_eq!(replay_signature_mixin([].iter()).unwrap(), [0; 32]);
        for count in 1..=signatures.len() {
            assert_eq!(
                replay_signature_mixin(signatures[..count].iter()).unwrap(),
                naive_signature_mixin(&signatures[..count]),
                "signature count {count}"
            );
        }
    }

    #[test]
    fn entry_hash_formula_covers_tick_record_and_zero_hash_edges() {
        let previous = [7; 32];
        assert_eq!(
            derive_replay_entry_hash(previous, 0, 0, None).unwrap(),
            previous
        );
        assert_eq!(
            derive_replay_entry_hash(previous, 1, 0, None).unwrap(),
            Sha256::digest(previous).as_slice()
        );

        let mixin = [9; 32];
        let mut expected = Sha256::new();
        expected.update(previous);
        expected.update(mixin);
        assert_eq!(
            derive_replay_entry_hash(previous, 0, 1, Some(mixin)).unwrap(),
            expected.finalize().as_slice()
        );
        assert!(matches!(
            derive_replay_entry_hash(previous, 1, 0, Some(mixin)),
            Err(ReplayCodecError::InvalidValue {
                field: "ReplayEntryV1.signature_mixin",
                ..
            })
        ));
    }

    #[test]
    fn legacy_payload_has_exact_golden_bytes_and_hash() {
        let slot = legacy_slot(&[0], &[1, 2]);
        let encoded = encode_replay_slot_v1(&slot).unwrap();

        let mut expected = vec![1, 0, 1, 1, 1];
        expected.extend_from_slice(&[0xaa; 32]);
        expected.extend_from_slice(&[0, 1, 0, 0, 1, 0]);
        expected.extend_from_slice(&[0x11; 32]);
        expected.extend_from_slice(&[2]);
        expected.extend_from_slice(&[0x22; 32]);
        expected.extend_from_slice(&[1, 0, 1, 0, 2, 1, 2]);
        assert_eq!(encoded, expected);

        let digest = Sha256::digest(&encoded);
        assert_eq!(
            digest.as_slice(),
            &[
                0x09, 0x92, 0x78, 0x1e, 0xdd, 0x4c, 0xf2, 0x1f, 0xe3, 0xcc, 0x85, 0xd4, 0xd9, 0x73,
                0x89, 0x07, 0x73, 0x0d, 0xdf, 0xb2, 0x8e, 0xb1, 0xfd, 0xa3, 0x0d, 0x93, 0xb3, 0x67,
                0x8b, 0x9e, 0x93, 0x96,
            ]
        );
        assert_eq!(decode_replay_slot_v1(&encoded).unwrap(), slot);
    }

    #[test]
    fn event_decoder_emits_wire_order_and_borrowed_payloads() {
        let mut slot = legacy_slot(&[0], &[1, 2]);
        slot.components
            .insert(0, ReplayComponentV1::BlockMarker(&[7, 8, 9]));
        let encoded = encode_replay_slot_v1(&slot).unwrap();
        let transaction = match &slot.components[1] {
            ReplayComponentV1::EntryBatch(entries) => entries[0].transactions[0].clone(),
            ReplayComponentV1::BlockMarker(_) => unreachable!(),
        };

        let events = decode_replay_slot_events_v1(&encoded)
            .unwrap()
            .collect::<ReplayCodecResult<Vec<_>>>()
            .unwrap();
        assert_eq!(
            events,
            vec![
                ReplaySlotEventV1::BlockMarker(&encoded[3..6]),
                ReplaySlotEventV1::EntryBatch { entry_count: 1 },
                ReplaySlotEventV1::Entry {
                    num_hashes: 1,
                    signature_mixin: Some([0xaa; 32]),
                    transaction_count: 1,
                },
                ReplaySlotEventV1::Transaction(transaction),
            ]
        );
    }

    #[test]
    fn event_decoder_streams_many_batches_without_collecting_a_slot_tree() {
        const BATCH_COUNT: usize = 10_000;

        let mut encoded = Vec::new();
        {
            let mut encoder = Encoder {
                output: &mut encoded,
                start: 0,
            };
            encoder
                .leb_u32(BATCH_COUNT, "ReplaySlotV1.components")
                .unwrap();
            for _ in 0..BATCH_COUNT {
                encoder.u8(COMPONENT_ENTRY_BATCH_TAG).unwrap();
                encoder
                    .leb_u32(1, "ReplayComponentV1.EntryBatch.entries")
                    .unwrap();
                encoder.leb_u64(0).unwrap();
                encoder.leb_u32(0, "ReplayEntryV1.transactions").unwrap();
            }
        }

        let mut events = decode_replay_slot_events_v1(&encoded).unwrap();
        for _ in 0..BATCH_COUNT {
            assert_eq!(
                events.next().unwrap().unwrap(),
                ReplaySlotEventV1::EntryBatch { entry_count: 1 }
            );
            assert_eq!(
                events.next().unwrap().unwrap(),
                ReplaySlotEventV1::Entry {
                    num_hashes: 0,
                    signature_mixin: None,
                    transaction_count: 0,
                }
            );
        }
        assert!(events.next().is_none());
        assert!(events.next().is_none());
    }

    #[test]
    fn event_decoder_finish_retains_an_already_yielded_error() {
        let mut events = decode_replay_slot_events_v1(&[1, 2]).unwrap();
        let error = events.next().unwrap().unwrap_err();
        assert_eq!(
            error,
            ReplayCodecError::UnknownTag {
                field: "ReplayComponentV1",
                value: 2,
            }
        );
        assert!(events.next().is_none());
        assert_eq!(events.finish(), Err(error));
    }

    #[test]
    fn message_scan_rejects_impossible_vector_minimum_before_materialization() {
        let mut encoded = single_transaction_prefix(MESSAGE_LEGACY_TAG);
        encoded.extend_from_slice(&[1, 0, 0, 2, 1]);

        assert_eq!(
            decode_replay_slot_v1(&encoded),
            Err(ReplayCodecError::Truncated {
                field: "ReplayMessageV1.static_account_keys",
                needed: 2,
                remaining: 1,
            })
        );
    }

    #[test]
    fn message_scan_rejects_expanded_limit_before_instruction_data() {
        let mut encoded = single_transaction_prefix(MESSAGE_LEGACY_TAG);
        encoded.extend_from_slice(&[
            1, 0, 0, // header
            1, 1, // one registry-backed static key
            0, 1, // prior-produced recent-blockhash distance
            1, 0, 0, // one instruction, program index zero, no accounts
            0xb0, 0x09, // 1,200 bytes of data, deliberately omitted
        ]);

        assert_eq!(
            decode_replay_slot_v1(&encoded),
            Err(ReplayCodecError::LengthOutOfBounds {
                field: "ReplayMessageV1.expanded_signed_message",
                min: 0,
                max: MAX_REPLAY_EXPANDED_MESSAGE_BYTES,
                actual: 1_273,
            })
        );
    }

    #[test]
    fn v0_scan_resolves_loaded_account_count_before_materializing_instructions() {
        let mut encoded = single_transaction_prefix(MESSAGE_V0_TAG);
        encoded.extend_from_slice(&[
            1, 0, 0, // header
            1, 1, // one registry-backed static key
            0, 1, // prior-produced recent-blockhash distance
            1, 1, 0, 0, // one instruction with out-of-range program index one
            0, // no address-table lookups
        ]);

        assert!(matches!(
            decode_replay_slot_v1(&encoded),
            Err(ReplayCodecError::InvalidValue {
                field: "ReplayInstructionV1.program_id_index",
                ..
            })
        ));
    }

    #[test]
    fn v0_raw_status_and_marker_variants_round_trip() {
        let slot = ReplaySlotV1 {
            components: vec![
                ReplayComponentV1::BlockMarker(&[7, 8, 9]),
                ReplayComponentV1::EntryBatch(vec![ReplayEntryV1 {
                    num_hashes: 128,
                    signature_mixin: Some([3; 32]),
                    transactions: vec![
                        ReplayTransactionV1 {
                            historical_status_backref: Some(StatusKeyClassRefV1::PriorTxDistance(
                                1,
                            )),
                            message: ReplayMessageV1::V0(ReplayV0MessageV1 {
                                header: [1, 0, 0],
                                static_account_keys: vec![ReplayAddressRefV1::RegistryId(1)],
                                recent_blockhash: RecentBlockhashRefV1::PreviousTailIndex(0),
                                instructions: vec![ReplayInstructionV1 {
                                    program_id_index: 0,
                                    account_indexes: &[0],
                                    data: &[],
                                }],
                                address_table_lookups: vec![ReplayAddressTableLookupV1 {
                                    table_account: ReplayAddressRefV1::Raw([4; 32]),
                                    writable_indexes: &[],
                                    readonly_indexes: &[],
                                }],
                            }),
                        },
                        ReplayTransactionV1 {
                            historical_status_backref: Some(StatusKeyClassRefV1::PreviousClassId(
                                [5; 24],
                            )),
                            message: ReplayMessageV1::Raw(ReplayRawMessageV1 {
                                signed_message_bytes: &[0x81, 0, 1],
                            }),
                        },
                    ],
                }]),
            ],
        };
        let encoded = encode_replay_slot_v1(&slot).unwrap();
        assert_eq!(decode_replay_slot_v1(&encoded).unwrap(), slot);
        assert!(encoded.windows(2).any(|bytes| bytes == [0x80, 0x01]));
    }

    #[test]
    fn rejects_non_minimal_and_overflowing_leb128() {
        assert!(matches!(
            decode_replay_slot_v1(&[0x81, 0x00]),
            Err(ReplayCodecError::NonMinimalLeb128 {
                field: "ReplaySlotV1.components"
            })
        ));
        assert!(matches!(
            decode_replay_slot_v1(&[0x80, 0x80, 0x80, 0x80, 0x10]),
            Err(ReplayCodecError::Leb128Overflow {
                field: "ReplaySlotV1.components"
            })
        ));
    }

    #[test]
    fn rejects_conditional_mixin_and_zero_distances() {
        let mut empty_with_mixin = ReplaySlotV1 {
            components: vec![ReplayComponentV1::EntryBatch(vec![ReplayEntryV1 {
                num_hashes: 1,
                signature_mixin: Some([0; 32]),
                transactions: vec![],
            }])],
        };
        assert!(matches!(
            validate_replay_slot_v1(&empty_with_mixin),
            Err(ReplayCodecError::InvalidValue {
                field: "ReplayEntryV1.signature_mixin",
                ..
            })
        ));

        empty_with_mixin.components = legacy_slot(&[0], &[]).components;
        let ReplayComponentV1::EntryBatch(entries) = &mut empty_with_mixin.components[0] else {
            unreachable!()
        };
        entries[0].transactions[0].historical_status_backref =
            Some(StatusKeyClassRefV1::PriorTxDistance(0));
        assert!(matches!(
            validate_replay_slot_v1(&empty_with_mixin),
            Err(ReplayCodecError::InvalidValue {
                field: "StatusKeyClassRefV1.PriorTxDistance",
                ..
            })
        ));
    }

    #[test]
    fn rejects_raw_legacy_and_v0_messages() {
        for bytes in [&[1, 0][..], &[0x80, 0][..]] {
            let slot = ReplaySlotV1 {
                components: vec![ReplayComponentV1::EntryBatch(vec![ReplayEntryV1 {
                    num_hashes: 1,
                    signature_mixin: Some([0; 32]),
                    transactions: vec![ReplayTransactionV1 {
                        historical_status_backref: None,
                        message: ReplayMessageV1::Raw(ReplayRawMessageV1 {
                            signed_message_bytes: bytes,
                        }),
                    }],
                }])],
            };
            assert!(matches!(
                validate_replay_slot_v1(&slot),
                Err(ReplayCodecError::InvalidValue {
                    field: "ReplayRawMessageV1.signed_message_bytes",
                    ..
                })
            ));
        }
    }

    #[test]
    fn rejects_unknown_tags_reserved_bits_and_trailing_bytes() {
        assert!(matches!(
            decode_replay_slot_v1(&[1, 2]),
            Err(ReplayCodecError::UnknownTag {
                field: "ReplayComponentV1",
                value: 2
            })
        ));

        let mut encoded = encode_replay_slot_v1(&legacy_slot(&[0], &[])).unwrap();
        let transaction_tag_offset = 5 + 32;
        encoded[transaction_tag_offset] = 0x08;
        assert!(matches!(
            decode_replay_slot_v1(&encoded),
            Err(ReplayCodecError::ReservedBitsSet { .. })
        ));

        let mut encoded = encode_replay_slot_v1(&legacy_slot(&[0], &[])).unwrap();
        encoded.push(0);
        assert!(matches!(
            decode_replay_slot_v1(&encoded),
            Err(ReplayCodecError::TrailingBytes { count: 1 })
        ));
    }

    #[test]
    fn rejects_empty_shape_and_out_of_range_indexes() {
        assert!(matches!(
            validate_replay_slot_v1(&ReplaySlotV1 { components: vec![] }),
            Err(ReplayCodecError::CountOutOfBounds {
                field: "ReplaySlotV1.components",
                ..
            })
        ));
        assert!(matches!(
            validate_replay_slot_v1(&ReplaySlotV1 {
                components: vec![ReplayComponentV1::BlockMarker(&[1])]
            }),
            Err(ReplayCodecError::InvalidValue {
                field: "ReplaySlotV1.components",
                ..
            })
        ));

        let slot = legacy_slot(&[1], &[]);
        assert!(matches!(
            validate_replay_slot_v1(&slot),
            Err(ReplayCodecError::InvalidValue {
                field: "ReplayInstructionV1.account_indexes",
                ..
            })
        ));
    }
}
