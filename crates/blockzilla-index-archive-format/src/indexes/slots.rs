//! `indexes/slots.idx`: slot to block ordinal.
//!
//! Schema 1 is a raw, strictly ascending array of little-endian `u64` slots.
//! The array position is the block ordinal, so the index does not store the
//! ordinal a second time. This also permits a reader to binary-search the
//! object with fixed 8-byte range reads.

use thiserror::Error;

pub const PATH: &str = "indexes/slots.idx";
pub const SCHEMA: u16 = 1;
pub const RECORD_LEN: usize = 8;

/// One parsed schema-1 slot index.
///
/// Parsing validates the full byte extent and strict slot order without
/// allocating. Point lookup then uses a binary search over the borrowed bytes.
#[derive(Debug, Clone, Copy)]
pub struct SlotIndex<'a> {
    payload: &'a [u8],
    record_count: u64,
}

impl<'a> SlotIndex<'a> {
    pub fn parse(payload: &'a [u8], record_count: u64) -> Result<Self, SlotIndexError> {
        let expected_len = record_count
            .checked_mul(RECORD_LEN as u64)
            .ok_or(SlotIndexError::LengthOverflow)?;
        let actual_len =
            u64::try_from(payload.len()).map_err(|_| SlotIndexError::LengthOverflow)?;
        if actual_len != expected_len {
            return Err(SlotIndexError::WrongPayloadLength {
                actual: actual_len,
                expected: expected_len,
            });
        }

        let mut previous = None;
        for (ordinal, bytes) in payload.chunks_exact(RECORD_LEN).enumerate() {
            let slot = u64::from_le_bytes(bytes.try_into().expect("8-byte chunk"));
            if let Some(previous) = previous
                && slot <= previous
            {
                return Err(SlotIndexError::SlotsNotAscending {
                    ordinal: ordinal as u64,
                    previous,
                    current: slot,
                });
            }
            previous = Some(slot);
        }

        Ok(Self {
            payload,
            record_count,
        })
    }

    pub const fn record_count(self) -> u64 {
        self.record_count
    }

    pub fn slot_at(self, block_ordinal: u64) -> Result<u64, SlotIndexError> {
        if block_ordinal >= self.record_count {
            return Err(SlotIndexError::OrdinalOutOfBounds {
                ordinal: block_ordinal,
                record_count: self.record_count,
            });
        }
        let start = usize::try_from(block_ordinal)
            .ok()
            .and_then(|ordinal| ordinal.checked_mul(RECORD_LEN))
            .ok_or(SlotIndexError::LengthOverflow)?;
        Ok(u64::from_le_bytes(
            self.payload[start..start + RECORD_LEN]
                .try_into()
                .expect("validated payload extent"),
        ))
    }

    /// Return the block ordinal for `slot`.
    pub fn find(self, slot: u64) -> Option<u64> {
        let mut lo = 0_u64;
        let mut hi = self.record_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let candidate = self
                .slot_at(mid)
                .expect("midpoint is inside the validated index");
            match candidate.cmp(&slot) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }
}

/// Encode a schema-1 payload. Input slots must strictly ascend.
pub fn encode(slots: &[u64]) -> Result<Vec<u8>, SlotIndexError> {
    let capacity = slots
        .len()
        .checked_mul(RECORD_LEN)
        .ok_or(SlotIndexError::LengthOverflow)?;
    let mut payload = Vec::with_capacity(capacity);
    let mut previous = None;
    for (ordinal, slot) in slots.iter().copied().enumerate() {
        if let Some(previous) = previous
            && slot <= previous
        {
            return Err(SlotIndexError::SlotsNotAscending {
                ordinal: ordinal as u64,
                previous,
                current: slot,
            });
        }
        previous = Some(slot);
        payload.extend_from_slice(&slot.to_le_bytes());
    }
    Ok(payload)
}

/// Find a slot through positioned record reads.
///
/// The object must first pass its common-header and full index validation.
/// `fetch` then needs only one 8-byte record for each binary-search probe.
pub fn search_slot<E>(
    record_count: u64,
    slot: u64,
    mut fetch: impl FnMut(u64) -> Result<u64, E>,
) -> Result<Option<u64>, E> {
    let mut lo = 0_u64;
    let mut hi = record_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match fetch(mid)?.cmp(&slot) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(Some(mid)),
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SlotIndexError {
    #[error("slot-index byte length overflows")]
    LengthOverflow,
    #[error("slot-index payload has {actual} bytes, expected exactly {expected}")]
    WrongPayloadLength { actual: u64, expected: u64 },
    #[error(
        "slot-index slots must strictly ascend at block ordinal {ordinal}: {previous} then {current}"
    )]
    SlotsNotAscending {
        ordinal: u64,
        previous: u64,
        current: u64,
    },
    #[error("block ordinal {ordinal} is outside {record_count} slot-index records")]
    OrdinalOutOfBounds { ordinal: u64, record_count: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_one_round_trips_and_finds_points() {
        let payload = encode(&[100, 102, 109, 1_000]).unwrap();
        let index = SlotIndex::parse(&payload, 4).unwrap();
        assert_eq!(index.record_count(), 4);
        assert_eq!(index.find(100), Some(0));
        assert_eq!(index.find(109), Some(2));
        assert_eq!(index.find(108), None);
        assert_eq!(index.slot_at(3).unwrap(), 1_000);
        assert_eq!(
            search_slot(4, 102, |ordinal| Ok::<_, std::convert::Infallible>(
                index.slot_at(ordinal).unwrap()
            ))
            .unwrap(),
            Some(1)
        );
    }

    #[test]
    fn parsing_is_exact_and_rejects_unsorted_slots() {
        let mut trailing = encode(&[1, 2]).unwrap();
        trailing.push(0);
        assert!(matches!(
            SlotIndex::parse(&trailing, 2),
            Err(SlotIndexError::WrongPayloadLength { .. })
        ));
        assert!(matches!(
            encode(&[1, 1]),
            Err(SlotIndexError::SlotsNotAscending { .. })
        ));
    }
}
