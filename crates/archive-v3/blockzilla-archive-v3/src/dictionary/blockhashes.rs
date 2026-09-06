//! `dictionary/blockhashes.pages`: every hash a transaction's recent blockhash
//! can point at.
//!
//! `TxCoreRow::blockhash_ref` is an ordinal into this file, so without it the
//! reference is dangling and the recent blockhash is unrecoverable.
//!
//! Two kinds of hash live here, in one ordinal space:
//!
//! - the epoch's recent blockhashes, carried from the source registry;
//! - **durable-nonce hashes**, which are not blockhashes at all. A nonce
//!   transaction puts the nonce account's stored hash where a recent blockhash
//!   would go, so it appears in no blockhash registry. On epoch 822 that is
//!   2.40% of transactions — 16.4 million — and dropping them loses the only
//!   copy.
//!
//! Records are the raw 32 bytes, positionally addressed. Like the pubkey
//! dictionary these are incompressible, so pages are stored raw.

use thiserror::Error;

pub const PATH: &str = "dictionary/blockhashes.pages";
pub const SCHEMA: u16 = 1;

/// Bytes of one hash record.
pub const RECORD_LEN: usize = 32;

/// Look one hash up by ordinal.
pub fn hash_at(dictionary: &[u8], ordinal: u32) -> Result<[u8; RECORD_LEN], BlockhashError> {
    let start = (ordinal as usize)
        .checked_mul(RECORD_LEN)
        .ok_or(BlockhashError::OrdinalOverflow)?;
    dictionary
        .get(start..start + RECORD_LEN)
        .and_then(|slice| slice.try_into().ok())
        .ok_or(BlockhashError::OrdinalOutOfRange {
            ordinal,
            records: (dictionary.len() / RECORD_LEN) as u32,
        })
}

pub fn record_count(dictionary: &[u8]) -> Result<u32, BlockhashError> {
    if !dictionary.len().is_multiple_of(RECORD_LEN) {
        return Err(BlockhashError::NotRecordAligned(dictionary.len()));
    }
    u32::try_from(dictionary.len() / RECORD_LEN).map_err(|_| BlockhashError::OrdinalOverflow)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BlockhashError {
    #[error("blockhash dictionary is {0} bytes, not a multiple of the record width")]
    NotRecordAligned(usize),
    #[error("blockhash ordinal {ordinal} is outside {records} records")]
    OrdinalOutOfRange { ordinal: u32, records: u32 },
    #[error("blockhash ordinal overflows a byte offset")]
    OrdinalOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dictionary() -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&[1u8; 32]);
        out.extend_from_slice(&[2u8; 32]);
        out.extend_from_slice(&[3u8; 32]);
        out
    }

    #[test]
    fn hashes_are_addressed_by_ordinal() {
        let d = dictionary();
        assert_eq!(record_count(&d).unwrap(), 3);
        assert_eq!(hash_at(&d, 0).unwrap(), [1u8; 32]);
        assert_eq!(hash_at(&d, 2).unwrap(), [3u8; 32]);
    }

    #[test]
    fn an_ordinal_past_the_end_is_an_error_not_a_zero_hash() {
        // A dangling reference must be loud. Returning zeros would look like a
        // real hash and silently corrupt whatever consumed it.
        assert_eq!(
            hash_at(&dictionary(), 3),
            Err(BlockhashError::OrdinalOutOfRange {
                ordinal: 3,
                records: 3
            })
        );
    }

    #[test]
    fn a_misaligned_dictionary_is_rejected() {
        let mut d = dictionary();
        d.push(0);
        assert_eq!(record_count(&d), Err(BlockhashError::NotRecordAligned(97)));
    }
}
