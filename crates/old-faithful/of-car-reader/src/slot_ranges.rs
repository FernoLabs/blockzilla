use std::fmt;
use std::io::{self, Write};

pub const SLOTS_PER_EPOCH: u64 = 432_000;
pub const SLOT_RANGE_ENTRY_SIZE: usize = 12;
pub const SLOT_RANGE_ENTRY_SIZE_U64: u64 = SLOT_RANGE_ENTRY_SIZE as u64;
pub const SLOT_RANGE_V2_ENTRY_SIZE: usize = 44;
pub const SLOT_RANGE_V2_ENTRY_SIZE_U64: u64 = SLOT_RANGE_V2_ENTRY_SIZE as u64;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SlotRange {
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SlotRangeWithPreviousBlockhash {
    pub range: SlotRange,
    pub previous_blockhash: [u8; 32],
}

impl SlotRange {
    pub const EMPTY: Self = Self { offset: 0, len: 0 };

    #[inline]
    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn end_exclusive(self) -> Option<u64> {
        self.offset.checked_add(self.len as u64)
    }

    #[inline]
    pub fn encode(self) -> [u8; SLOT_RANGE_ENTRY_SIZE] {
        let mut out = [0u8; SLOT_RANGE_ENTRY_SIZE];
        out[..8].copy_from_slice(&self.offset.to_le_bytes());
        out[8..12].copy_from_slice(&self.len.to_le_bytes());
        out
    }
}

impl SlotRangeWithPreviousBlockhash {
    pub const EMPTY: Self = Self {
        range: SlotRange::EMPTY,
        previous_blockhash: [0; 32],
    };

    #[inline]
    pub fn encode(self) -> [u8; SLOT_RANGE_V2_ENTRY_SIZE] {
        let mut out = [0u8; SLOT_RANGE_V2_ENTRY_SIZE];
        out[..SLOT_RANGE_ENTRY_SIZE].copy_from_slice(&self.range.encode());
        out[SLOT_RANGE_ENTRY_SIZE..].copy_from_slice(&self.previous_blockhash);
        out
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SlotRangeError {
    InvalidEntrySize { expected: usize, actual: usize },
    SlotInEpochOutOfRange(u64),
    OffsetOverflow,
}

impl fmt::Display for SlotRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SlotRangeError::InvalidEntrySize { expected, actual } => {
                write!(
                    f,
                    "invalid slot range entry size: expected {expected}, got {actual}"
                )
            }
            SlotRangeError::SlotInEpochOutOfRange(slot) => {
                write!(f, "slot in epoch out of range: {slot}")
            }
            SlotRangeError::OffsetOverflow => write!(f, "slot range byte offset overflow"),
        }
    }
}

impl std::error::Error for SlotRangeError {}

#[inline]
pub fn epoch_for_slot(slot: u64) -> u64 {
    slot / SLOTS_PER_EPOCH
}

#[inline]
pub fn slot_in_epoch(slot: u64) -> u64 {
    slot % SLOTS_PER_EPOCH
}

#[inline]
pub fn slot_range_entry_offset(slot_in_epoch: u64) -> Result<u64, SlotRangeError> {
    if slot_in_epoch >= SLOTS_PER_EPOCH {
        return Err(SlotRangeError::SlotInEpochOutOfRange(slot_in_epoch));
    }

    slot_in_epoch
        .checked_mul(SLOT_RANGE_ENTRY_SIZE_U64)
        .ok_or(SlotRangeError::OffsetOverflow)
}

#[inline]
pub fn slot_range_v2_entry_offset(slot_in_epoch: u64) -> Result<u64, SlotRangeError> {
    if slot_in_epoch >= SLOTS_PER_EPOCH {
        return Err(SlotRangeError::SlotInEpochOutOfRange(slot_in_epoch));
    }

    slot_in_epoch
        .checked_mul(SLOT_RANGE_V2_ENTRY_SIZE_U64)
        .ok_or(SlotRangeError::OffsetOverflow)
}

pub fn decode_slot_range_entry(bytes: &[u8]) -> Result<SlotRange, SlotRangeError> {
    if bytes.len() != SLOT_RANGE_ENTRY_SIZE {
        return Err(SlotRangeError::InvalidEntrySize {
            expected: SLOT_RANGE_ENTRY_SIZE,
            actual: bytes.len(),
        });
    }

    let offset = u64::from_le_bytes(bytes[..8].try_into().expect("checked length"));
    let len = u32::from_le_bytes(bytes[8..12].try_into().expect("checked length"));
    Ok(SlotRange { offset, len })
}

pub fn decode_slot_range_v2_entry(
    bytes: &[u8],
) -> Result<SlotRangeWithPreviousBlockhash, SlotRangeError> {
    if bytes.len() != SLOT_RANGE_V2_ENTRY_SIZE {
        return Err(SlotRangeError::InvalidEntrySize {
            expected: SLOT_RANGE_V2_ENTRY_SIZE,
            actual: bytes.len(),
        });
    }

    let range = decode_slot_range_entry(&bytes[..SLOT_RANGE_ENTRY_SIZE])?;
    let mut previous_blockhash = [0u8; 32];
    previous_blockhash.copy_from_slice(&bytes[SLOT_RANGE_ENTRY_SIZE..]);
    Ok(SlotRangeWithPreviousBlockhash {
        range,
        previous_blockhash,
    })
}

pub fn write_slot_ranges_raw<W: Write>(writer: &mut W, ranges: &[SlotRange]) -> io::Result<()> {
    for range in ranges {
        writer.write_all(&range.encode())?;
    }
    Ok(())
}

pub fn write_slot_ranges_v2_raw<W: Write>(
    writer: &mut W,
    ranges: &[SlotRangeWithPreviousBlockhash],
) -> io::Result<()> {
    for range in ranges {
        writer.write_all(&range.encode())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_range_round_trips() {
        let range = SlotRange {
            offset: 123,
            len: 456,
        };
        assert_eq!(decode_slot_range_entry(&range.encode()).unwrap(), range);
    }

    #[test]
    fn slot_range_v2_round_trips() {
        let range = SlotRangeWithPreviousBlockhash {
            range: SlotRange {
                offset: 123,
                len: 456,
            },
            previous_blockhash: [7; 32],
        };
        assert_eq!(decode_slot_range_v2_entry(&range.encode()).unwrap(), range);
    }

    #[test]
    fn computes_worker_range_offsets() {
        assert_eq!(slot_range_entry_offset(0).unwrap(), 0);
        assert_eq!(slot_range_entry_offset(42).unwrap(), 42 * 12);
        assert_eq!(slot_range_v2_entry_offset(42).unwrap(), 42 * 44);
        assert!(slot_range_entry_offset(SLOTS_PER_EPOCH).is_err());
        assert!(slot_range_v2_entry_offset(SLOTS_PER_EPOCH).is_err());
    }
}
