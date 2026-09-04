//! Exact Compact V2 blockhash-registry and legacy predecessor-tail resolution.

use thiserror::Error;

pub const BLOCKHASH_RECORD_LEN: usize = 32;
/// Independent practical cap for one current-epoch blockhash registry.
pub const MAX_BLOCKHASH_REGISTRY_BYTES: usize = 64 << 20;
pub const PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN: usize = 40;
pub const PREVIOUS_BLOCKHASH_LEGACY_RECORD_LEN: usize = 32;
pub const PREVIOUS_BLOCKHASH_TAIL_CAPACITY: usize = 300;

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

/// The bounded previous-epoch tail and its proved source schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviousBlockhashTail {
    pub schema: PreviousBlockhashTailSchema,
    pub entries: Vec<PreviousBlockhash>,
}

/// Exact signed-ID resolver for boundary-prefixed, current, and legacy
/// previous-epoch blockhashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockhashResolver {
    current: Vec<[u8; 32]>,
    previous: PreviousBlockhashTail,
}

impl BlockhashResolver {
    pub fn from_bytes(
        current_registry: &[u8],
        previous_tail: PreviousBlockhashTail,
    ) -> Result<Self, BlockhashResolverError> {
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

    /// Resolve `>= 0` in the registry and `< 0` from the legacy previous tail,
    /// where `-1` is the newest previous hash. In a boundary-prefixed
    /// registry, ID 0 resolves to the epoch boundary hash.
    pub fn resolve(&self, id: i32) -> Result<[u8; 32], BlockhashResolverError> {
        if id >= 0 {
            return self
                .current
                .get(usize::try_from(id).expect("non-negative i32 fits usize"))
                .copied()
                .ok_or(BlockhashResolverError::CurrentBlockhashIdOutOfRange {
                    id,
                    records: self.current.len(),
                });
        }

        let index = i64::try_from(self.previous.entries.len())
            .ok()
            .and_then(|len| len.checked_add(i64::from(id)))
            .filter(|index| *index >= 0)
            .and_then(|index| usize::try_from(index).ok())
            .ok_or(BlockhashResolverError::PreviousBlockhashIdOutOfRange {
                id,
                records: self.previous.entries.len(),
            })?;
        self.previous
            .entries
            .get(index)
            .map(|entry| entry.hash)
            .ok_or(BlockhashResolverError::PreviousBlockhashIdOutOfRange {
                id,
                records: self.previous.entries.len(),
            })
    }

    /// Resolve the unsigned header representation of the previous blockhash.
    /// Legacy Compact V2 uses `(blockhash_id=0, previous_id=0)` for the first
    /// current block and obtains its predecessor from the newest previous-tail
    /// row. The boundary-prefixed layout uses `(1, 0)`, which resolves ID 0
    /// directly from the registry.
    pub fn resolve_header_previous(
        &self,
        blockhash_id: u32,
        previous_id: u32,
    ) -> Result<[u8; 32], BlockhashResolverError> {
        if blockhash_id == 0 && previous_id == 0 {
            return self
                .previous
                .entries
                .last()
                .map(|entry| entry.hash)
                .ok_or(BlockhashResolverError::MissingFirstBlockPredecessor);
        }
        let id = i32::try_from(previous_id).map_err(|_| {
            BlockhashResolverError::UnsignedBlockhashIdExceedsI32 { id: previous_id }
        })?;
        self.resolve(id)
    }
}

/// An invalid blockhash registry, predecessor tail, or signed blockhash ID.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BlockhashResolverError {
    #[error("blockhash registry has {actual} bytes, above the {maximum}-byte practical limit")]
    BlockhashRegistryByteLimit { actual: usize, maximum: usize },
    #[error("blockhash registry length {actual} is not a multiple of {BLOCKHASH_RECORD_LEN}")]
    BlockhashRegistryLength { actual: usize },
    #[error("blockhash registry has {records} records, above the i32 ID range")]
    BlockhashRegistryTooLarge { records: usize },
    #[error(
        "blockhash registry has {records} records for {blocks} archive blocks; expected {blocks} or {blocks} + 1"
    )]
    BlockhashRegistryBlockCount { records: usize, blocks: usize },
    #[error("cannot reserve {records} blockhash registry records")]
    BlockhashRegistryAllocation { records: usize },
    #[error(
        "previous blockhash tail has {actual} bytes for {schema:?} records of {record_len} bytes"
    )]
    PreviousTailLength {
        schema: PreviousBlockhashTailSchema,
        actual: usize,
        record_len: usize,
    },
    #[error(
        "previous blockhash tail has {records} records, above the {PREVIOUS_BLOCKHASH_TAIL_CAPACITY}-record limit"
    )]
    PreviousTailTooLarge { records: usize },
    #[error("cannot reserve {records} previous-blockhash records")]
    PreviousTailAllocation { records: usize },
    #[error("previous blockhash tail slots are not ascending: {previous} then {current}")]
    PreviousTailSlotsNotAscending { previous: u64, current: u64 },
    #[error("genesis epoch cannot have a previous blockhash tail")]
    PreviousTailInvalidEpoch,
    #[error("slots per epoch must be non-zero")]
    PreviousTailInvalidSlotsPerEpoch,
    #[error("previous blockhash tail epoch slot range overflows u64")]
    PreviousTailEpochRangeOverflow,
    #[error("previous blockhash tail with {actual} bytes matches both current and legacy schemas")]
    PreviousTailAmbiguousSchema { actual: usize },
    #[error(
        "previous blockhash tail with {actual} bytes has no valid bounded schema for epoch {epoch}"
    )]
    PreviousTailNoValidSchema { actual: usize, epoch: u64 },
    #[error("current blockhash ID {id} is outside {records} registry records")]
    CurrentBlockhashIdOutOfRange { id: i32, records: usize },
    #[error("previous blockhash ID {id} is outside {records} tail records")]
    PreviousBlockhashIdOutOfRange { id: i32, records: usize },
    #[error("unsigned blockhash ID {id} exceeds i32")]
    UnsignedBlockhashIdExceedsI32 { id: u32 },
    #[error("first current block has no previous-tail predecessor")]
    MissingFirstBlockPredecessor,
}

/// Detect and parse the bounded previous-blockhash tail.
///
/// Current rows are valid only when slots are strictly ascending inside the
/// previous epoch. A legacy candidate has no slots to validate. If both
/// candidates remain valid, the result is ambiguous and parsing stops.
pub fn detect_previous_blockhash_tail(
    bytes: &[u8],
    epoch: u64,
    slots_per_epoch: u64,
) -> Result<PreviousBlockhashTail, BlockhashResolverError> {
    if epoch == 0 {
        return Err(BlockhashResolverError::PreviousTailInvalidEpoch);
    }
    if slots_per_epoch == 0 {
        return Err(BlockhashResolverError::PreviousTailInvalidSlotsPerEpoch);
    }
    if bytes.is_empty() {
        return Err(BlockhashResolverError::PreviousTailNoValidSchema { actual: 0, epoch });
    }
    let previous_epoch_start = epoch
        .checked_sub(1)
        .and_then(|previous_epoch| previous_epoch.checked_mul(slots_per_epoch))
        .ok_or(BlockhashResolverError::PreviousTailEpochRangeOverflow)?;
    let epoch_start = epoch
        .checked_mul(slots_per_epoch)
        .ok_or(BlockhashResolverError::PreviousTailEpochRangeOverflow)?;

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
        (Some(_), Some(_)) => Err(BlockhashResolverError::PreviousTailAmbiguousSchema {
            actual: bytes.len(),
        }),
        (None, None) => Err(BlockhashResolverError::PreviousTailNoValidSchema {
            actual: bytes.len(),
            epoch,
        }),
    }
}

/// Parse a previous-blockhash tail using an explicit, trusted schema.
pub fn parse_previous_blockhash_tail(
    bytes: &[u8],
    schema: PreviousBlockhashTailSchema,
) -> Result<PreviousBlockhashTail, BlockhashResolverError> {
    let record_len = match schema {
        PreviousBlockhashTailSchema::CurrentHashAndSlot => PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN,
        PreviousBlockhashTailSchema::LegacyHashOnly => PREVIOUS_BLOCKHASH_LEGACY_RECORD_LEN,
    };
    if !bytes.len().is_multiple_of(record_len) {
        return Err(BlockhashResolverError::PreviousTailLength {
            schema,
            actual: bytes.len(),
            record_len,
        });
    }
    let records = bytes.len() / record_len;
    if records > PREVIOUS_BLOCKHASH_TAIL_CAPACITY {
        return Err(BlockhashResolverError::PreviousTailTooLarge { records });
    }

    let mut entries = Vec::new();
    entries
        .try_reserve_exact(records)
        .map_err(|_| BlockhashResolverError::PreviousTailAllocation { records })?;
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
                    return Err(BlockhashResolverError::PreviousTailSlotsNotAscending {
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

/// Return the number of leading boundary records in a registry.
///
/// Legacy registries contain one record for each archive block and return 0.
/// Current registries contain boundary record 0 followed by one record for
/// each archive block and return 1.
pub(crate) fn blockhash_registry_offset(
    byte_len: usize,
    block_count: usize,
) -> Result<usize, BlockhashResolverError> {
    if byte_len > MAX_BLOCKHASH_REGISTRY_BYTES {
        return Err(BlockhashResolverError::BlockhashRegistryByteLimit {
            actual: byte_len,
            maximum: MAX_BLOCKHASH_REGISTRY_BYTES,
        });
    }
    if !byte_len.is_multiple_of(BLOCKHASH_RECORD_LEN) {
        return Err(BlockhashResolverError::BlockhashRegistryLength { actual: byte_len });
    }
    let records = byte_len / BLOCKHASH_RECORD_LEN;
    match records.checked_sub(block_count) {
        Some(offset @ 0..=1) => Ok(offset),
        _ => Err(BlockhashResolverError::BlockhashRegistryBlockCount {
            records,
            blocks: block_count,
        }),
    }
}

/// Parse the registry in ID order. In the current layout, record 0 is the
/// epoch boundary and produced blocks start at record 1.
pub fn parse_blockhash_registry(bytes: &[u8]) -> Result<Vec<[u8; 32]>, BlockhashResolverError> {
    if bytes.len() > MAX_BLOCKHASH_REGISTRY_BYTES {
        return Err(BlockhashResolverError::BlockhashRegistryByteLimit {
            actual: bytes.len(),
            maximum: MAX_BLOCKHASH_REGISTRY_BYTES,
        });
    }
    if !bytes.len().is_multiple_of(BLOCKHASH_RECORD_LEN) {
        return Err(BlockhashResolverError::BlockhashRegistryLength {
            actual: bytes.len(),
        });
    }
    let records = bytes.len() / BLOCKHASH_RECORD_LEN;
    if records > i32::MAX as usize + 1 {
        return Err(BlockhashResolverError::BlockhashRegistryTooLarge { records });
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(records)
        .map_err(|_| BlockhashResolverError::BlockhashRegistryAllocation { records })?;
    for chunk in bytes.chunks_exact(BLOCKHASH_RECORD_LEN) {
        entries.push(
            chunk
                .try_into()
                .expect("validated blockhash registry record width"),
        );
    }
    Ok(entries)
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
            Err(BlockhashResolverError::PreviousBlockhashIdOutOfRange { .. })
        ));
    }

    #[test]
    fn boundary_prefixed_registry_resolves_id_zero_without_a_tail() {
        let boundary = [7; 32];
        let first_block = [8; 32];
        let resolver = BlockhashResolver::from_bytes(
            &[boundary, first_block].concat(),
            PreviousBlockhashTail {
                schema: PreviousBlockhashTailSchema::CurrentHashAndSlot,
                entries: Vec::new(),
            },
        )
        .unwrap();

        assert_eq!(blockhash_registry_offset(64, 1).unwrap(), 1);
        assert_eq!(resolver.resolve(0).unwrap(), boundary);
        assert_eq!(resolver.resolve_header_previous(1, 0).unwrap(), boundary);
        assert_eq!(resolver.resolve(1).unwrap(), first_block);
    }

    #[test]
    fn registry_offset_accepts_only_legacy_or_one_boundary_record() {
        assert_eq!(blockhash_registry_offset(64, 2).unwrap(), 0);
        assert_eq!(blockhash_registry_offset(96, 2).unwrap(), 1);
        assert!(matches!(
            blockhash_registry_offset(128, 2),
            Err(BlockhashResolverError::BlockhashRegistryBlockCount {
                records: 4,
                blocks: 2
            })
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
            Err(BlockhashResolverError::PreviousTailSlotsNotAscending {
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
