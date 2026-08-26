//! Source-only helpers for Compact V2 PoH and blockhash sidecars.
//!
//! Retained PoH and shredding frames are validated and copied by the binary's
//! `retained_sidecars` module. This module does not define a target sidecar
//! representation.

use std::{error::Error, fmt, ops::Range};

pub const BLOCKHASH_RECORD_LEN: usize = 32;
pub const PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN: usize = 40;
pub const PREVIOUS_BLOCKHASH_LEGACY_RECORD_LEN: usize = 32;
pub const PREVIOUS_BLOCKHASH_TAIL_CAPACITY: usize = 300;

/// The source PoH entry schema proved by exact frame decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourcePohSchema {
    Current,
    LegacyNoSignatureCount,
    /// All source records had no entries, so both schemas had the same bytes.
    NoEntrySchemaEvidence,
}

/// Per-block knowledge about the source entry signature-count lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockSignatureCountCoverage {
    CurrentExact,
    Recovered,
    LegacyUnknown,
    /// Only the registry-backed final hash is available.
    FinalHashOnly,
    /// The source has no PoH entry sequence for this block.
    NoEntries,
}

/// Deterministic source-block to retained-entry mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PohBlockMapping {
    pub source_block_id: u32,
    pub block_ordinal: u64,
    pub first_entry_ordinal: u64,
    pub entry_count: u32,
    pub final_entry_ordinal: Option<u64>,
    pub signature_count_coverage: BlockSignatureCountCoverage,
    pub block_signature_count: u32,
}

impl PohBlockMapping {
    pub fn entry_range(&self) -> Option<Range<u64>> {
        self.first_entry_ordinal
            .checked_add(u64::from(self.entry_count))
            .map(|end| self.first_entry_ordinal..end)
    }
}

/// Schema of `prev_blockhash_tail.bin`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreviousBlockhashTailSchema {
    CurrentHashAndSlot,
    LegacyHashOnly,
}

/// One previous-epoch hash, oldest to newest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreviousBlockhash {
    pub hash: [u8; 32],
    pub slot: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviousBlockhashTail {
    pub schema: PreviousBlockhashTailSchema,
    pub entries: Vec<PreviousBlockhash>,
}

/// Exact signed-ID resolver for current and previous-epoch blockhashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockhashResolver {
    current: Vec<[u8; 32]>,
    previous: PreviousBlockhashTail,
}

impl BlockhashResolver {
    pub fn from_bytes(
        current_registry: &[u8],
        previous_tail: PreviousBlockhashTail,
    ) -> Result<Self, SidecarConversionError> {
        Ok(Self {
            current: parse_blockhash_registry(current_registry)?,
            previous: previous_tail,
        })
    }

    pub fn current(&self) -> &[[u8; 32]] {
        &self.current
    }

    pub fn previous(&self) -> &PreviousBlockhashTail {
        &self.previous
    }

    /// Resolve `>= 0` in the current registry and `< 0` from the previous
    /// tail, where `-1` is the newest previous hash.
    pub fn resolve(&self, id: i32) -> Result<[u8; 32], SidecarConversionError> {
        if id >= 0 {
            return self
                .current
                .get(usize::try_from(id).expect("non-negative i32 fits usize"))
                .copied()
                .ok_or(SidecarConversionError::CurrentBlockhashIdOutOfRange {
                    id,
                    records: self.current.len(),
                });
        }

        let index = i64::try_from(self.previous.entries.len())
            .ok()
            .and_then(|len| len.checked_add(i64::from(id)))
            .filter(|index| *index >= 0)
            .and_then(|index| usize::try_from(index).ok())
            .ok_or(SidecarConversionError::PreviousBlockhashIdOutOfRange {
                id,
                records: self.previous.entries.len(),
            })?;
        self.previous
            .entries
            .get(index)
            .map(|entry| entry.hash)
            .ok_or(SidecarConversionError::PreviousBlockhashIdOutOfRange {
                id,
                records: self.previous.entries.len(),
            })
    }

    /// Resolve the unsigned header representation of the previous blockhash.
    /// Compact V2 uses `(blockhash_id=0, previous_id=0)` for the first current
    /// block and obtains its predecessor from the newest previous-tail row.
    pub fn resolve_header_previous(
        &self,
        blockhash_id: u32,
        previous_id: u32,
    ) -> Result<[u8; 32], SidecarConversionError> {
        if blockhash_id == 0 && previous_id == 0 {
            return self
                .previous
                .entries
                .last()
                .map(|entry| entry.hash)
                .ok_or(SidecarConversionError::MissingFirstBlockPredecessor);
        }
        let id = i32::try_from(previous_id).map_err(|_| {
            SidecarConversionError::UnsignedBlockhashIdExceedsI32 { id: previous_id }
        })?;
        self.resolve(id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SidecarConversionError {
    BlockhashRegistryLength {
        actual: usize,
    },
    BlockhashRegistryTooLarge {
        records: usize,
    },
    PreviousTailLength {
        schema: PreviousBlockhashTailSchema,
        actual: usize,
        record_len: usize,
    },
    PreviousTailTooLarge {
        records: usize,
    },
    PreviousTailSlotsNotAscending {
        previous: u64,
        current: u64,
    },
    PreviousTailInvalidEpoch,
    PreviousTailInvalidSlotsPerEpoch,
    PreviousTailEpochRangeOverflow,
    PreviousTailAmbiguousSchema {
        actual: usize,
    },
    PreviousTailNoValidSchema {
        actual: usize,
        epoch: u64,
    },
    CurrentBlockhashIdOutOfRange {
        id: i32,
        records: usize,
    },
    PreviousBlockhashIdOutOfRange {
        id: i32,
        records: usize,
    },
    UnsignedBlockhashIdExceedsI32 {
        id: u32,
    },
    MissingFirstBlockPredecessor,
}

impl fmt::Display for SidecarConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockhashRegistryLength { actual } => write!(
                f,
                "blockhash registry length {actual} is not a multiple of {BLOCKHASH_RECORD_LEN}"
            ),
            Self::BlockhashRegistryTooLarge { records } => write!(
                f,
                "blockhash registry has {records} records, above the i32 ID range"
            ),
            Self::PreviousTailLength {
                schema,
                actual,
                record_len,
            } => write!(
                f,
                "previous blockhash tail has {actual} bytes for {schema:?} records of {record_len} bytes"
            ),
            Self::PreviousTailTooLarge { records } => write!(
                f,
                "previous blockhash tail has {records} records, above the {PREVIOUS_BLOCKHASH_TAIL_CAPACITY}-record limit"
            ),
            Self::PreviousTailSlotsNotAscending { previous, current } => write!(
                f,
                "previous blockhash tail slots are not ascending: {previous} then {current}"
            ),
            Self::PreviousTailInvalidEpoch => {
                f.write_str("genesis epoch cannot have a previous blockhash tail")
            }
            Self::PreviousTailInvalidSlotsPerEpoch => {
                f.write_str("slots per epoch must be non-zero")
            }
            Self::PreviousTailEpochRangeOverflow => {
                f.write_str("previous blockhash tail epoch slot range overflows u64")
            }
            Self::PreviousTailAmbiguousSchema { actual } => write!(
                f,
                "previous blockhash tail with {actual} bytes matches both current and legacy schemas"
            ),
            Self::PreviousTailNoValidSchema { actual, epoch } => write!(
                f,
                "previous blockhash tail with {actual} bytes has no valid bounded schema for epoch {epoch}"
            ),
            Self::CurrentBlockhashIdOutOfRange { id, records } => write!(
                f,
                "current blockhash ID {id} is outside {records} registry records"
            ),
            Self::PreviousBlockhashIdOutOfRange { id, records } => write!(
                f,
                "previous blockhash ID {id} is outside {records} tail records"
            ),
            Self::UnsignedBlockhashIdExceedsI32 { id } => {
                write!(f, "unsigned blockhash ID {id} exceeds i32")
            }
            Self::MissingFirstBlockPredecessor => {
                f.write_str("first current block has no previous-tail predecessor")
            }
        }
    }
}

impl Error for SidecarConversionError {}

/// Detect and parse the bounded previous-blockhash tail.
///
/// Current rows are valid only when slots are strictly ascending inside the
/// previous epoch. A legacy candidate has no slots to validate. If both
/// candidates remain valid, the result is ambiguous and conversion stops.
pub fn detect_previous_blockhash_tail(
    bytes: &[u8],
    epoch: u64,
    slots_per_epoch: u64,
) -> Result<PreviousBlockhashTail, SidecarConversionError> {
    if epoch == 0 {
        return Err(SidecarConversionError::PreviousTailInvalidEpoch);
    }
    if slots_per_epoch == 0 {
        return Err(SidecarConversionError::PreviousTailInvalidSlotsPerEpoch);
    }
    if bytes.is_empty() {
        return Err(SidecarConversionError::PreviousTailNoValidSchema { actual: 0, epoch });
    }
    let previous_epoch_start = epoch
        .checked_sub(1)
        .and_then(|previous_epoch| previous_epoch.checked_mul(slots_per_epoch))
        .ok_or(SidecarConversionError::PreviousTailEpochRangeOverflow)?;
    let epoch_start = epoch
        .checked_mul(slots_per_epoch)
        .ok_or(SidecarConversionError::PreviousTailEpochRangeOverflow)?;

    let current = if bytes
        .len()
        .is_multiple_of(PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN)
        && bytes.len() / PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN <= PREVIOUS_BLOCKHASH_TAIL_CAPACITY
    {
        parse_previous_blockhash_tail(bytes, PreviousBlockhashTailSchema::CurrentHashAndSlot)
            .ok()
            .filter(|tail| {
                tail.entries.iter().all(|entry| {
                    entry
                        .slot
                        .is_some_and(|slot| (previous_epoch_start..epoch_start).contains(&slot))
                })
            })
    } else {
        None
    };
    let legacy = if bytes
        .len()
        .is_multiple_of(PREVIOUS_BLOCKHASH_LEGACY_RECORD_LEN)
        && bytes.len() / PREVIOUS_BLOCKHASH_LEGACY_RECORD_LEN <= PREVIOUS_BLOCKHASH_TAIL_CAPACITY
    {
        parse_previous_blockhash_tail(bytes, PreviousBlockhashTailSchema::LegacyHashOnly).ok()
    } else {
        None
    };

    match (current, legacy) {
        (Some(tail), None) | (None, Some(tail)) => Ok(tail),
        (Some(_), Some(_)) => Err(SidecarConversionError::PreviousTailAmbiguousSchema {
            actual: bytes.len(),
        }),
        (None, None) => Err(SidecarConversionError::PreviousTailNoValidSchema {
            actual: bytes.len(),
            epoch,
        }),
    }
}

/// Parse a previous-blockhash tail using an explicit, trusted schema.
pub fn parse_previous_blockhash_tail(
    bytes: &[u8],
    schema: PreviousBlockhashTailSchema,
) -> Result<PreviousBlockhashTail, SidecarConversionError> {
    let record_len = match schema {
        PreviousBlockhashTailSchema::CurrentHashAndSlot => PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN,
        PreviousBlockhashTailSchema::LegacyHashOnly => PREVIOUS_BLOCKHASH_LEGACY_RECORD_LEN,
    };
    if !bytes.len().is_multiple_of(record_len) {
        return Err(SidecarConversionError::PreviousTailLength {
            schema,
            actual: bytes.len(),
            record_len,
        });
    }
    let records = bytes.len() / record_len;
    if records > PREVIOUS_BLOCKHASH_TAIL_CAPACITY {
        return Err(SidecarConversionError::PreviousTailTooLarge { records });
    }

    let mut entries = Vec::with_capacity(records);
    let mut previous_slot = None;
    for chunk in bytes.chunks_exact(record_len) {
        let hash = chunk[..32]
            .try_into()
            .expect("validated previous-blockhash record width");
        let slot = match schema {
            PreviousBlockhashTailSchema::CurrentHashAndSlot => {
                let slot = u64::from_le_bytes(
                    chunk[32..40]
                        .try_into()
                        .expect("validated current tail record width"),
                );
                if let Some(previous) = previous_slot
                    && slot <= previous
                {
                    return Err(SidecarConversionError::PreviousTailSlotsNotAscending {
                        previous,
                        current: slot,
                    });
                }
                previous_slot = Some(slot);
                Some(slot)
            }
            PreviousBlockhashTailSchema::LegacyHashOnly => None,
        };
        entries.push(PreviousBlockhash { hash, slot });
    }
    Ok(PreviousBlockhashTail { schema, entries })
}

pub fn parse_blockhash_registry(bytes: &[u8]) -> Result<Vec<[u8; 32]>, SidecarConversionError> {
    if !bytes.len().is_multiple_of(BLOCKHASH_RECORD_LEN) {
        return Err(SidecarConversionError::BlockhashRegistryLength {
            actual: bytes.len(),
        });
    }
    let records = bytes.len() / BLOCKHASH_RECORD_LEN;
    if records > i32::MAX as usize + 1 {
        return Err(SidecarConversionError::BlockhashRegistryTooLarge { records });
    }
    Ok(bytes
        .chunks_exact(BLOCKHASH_RECORD_LEN)
        .map(|chunk| {
            chunk
                .try_into()
                .expect("validated blockhash registry record width")
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolver_supports_current_and_previous_ids() {
        let previous = PreviousBlockhashTail {
            schema: PreviousBlockhashTailSchema::CurrentHashAndSlot,
            entries: vec![
                PreviousBlockhash {
                    hash: [1; 32],
                    slot: Some(10),
                },
                PreviousBlockhash {
                    hash: [2; 32],
                    slot: Some(11),
                },
            ],
        };
        let resolver = BlockhashResolver::from_bytes(&[[3; 32], [4; 32]].concat(), previous)
            .expect("resolver");
        assert_eq!(resolver.resolve(0).unwrap(), [3; 32]);
        assert_eq!(resolver.resolve(1).unwrap(), [4; 32]);
        assert_eq!(resolver.resolve(-1).unwrap(), [2; 32]);
        assert_eq!(resolver.resolve(-2).unwrap(), [1; 32]);
        assert_eq!(resolver.resolve_header_previous(0, 0).unwrap(), [2; 32]);
        assert!(matches!(
            resolver.resolve(-3),
            Err(SidecarConversionError::PreviousBlockhashIdOutOfRange { .. })
        ));
    }

    #[test]
    fn explicit_current_tail_checks_order() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[1; 32]);
        bytes.extend_from_slice(&11_u64.to_le_bytes());
        bytes.extend_from_slice(&[2; 32]);
        bytes.extend_from_slice(&10_u64.to_le_bytes());
        assert!(matches!(
            parse_previous_blockhash_tail(&bytes, PreviousBlockhashTailSchema::CurrentHashAndSlot),
            Err(SidecarConversionError::PreviousTailSlotsNotAscending {
                previous: 11,
                current: 10
            })
        ));
    }

    #[test]
    fn detection_uses_previous_epoch_slot_evidence() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[1; 32]);
        bytes.extend_from_slice(&5_u64.to_le_bytes());
        let tail = detect_previous_blockhash_tail(&bytes, 1, 10).unwrap();
        assert_eq!(tail.schema, PreviousBlockhashTailSchema::CurrentHashAndSlot);
        assert_eq!(tail.entries[0].slot, Some(5));
    }
}
