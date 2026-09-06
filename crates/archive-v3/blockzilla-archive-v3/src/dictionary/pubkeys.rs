//! `dictionary/pubkeys.pages`: the 32-byte account keys every column refers to.
//!
//! Account references throughout the format are registry ordinals, not bytes:
//! `ledger/accounts` stores them, instruction rows index into that, and token
//! balances name mints and owners by the same ordinal. This file is the only
//! place the bytes exist, so without it every account reference is unresolvable
//! and the generation cannot stand alone.
//!
//! Ordinal order is generation-local and must not affect meaning. A native
//! writer should use **descending usage**, so frequent accounts get short
//! ULEB128 values. An Archive V2 upgrade can instead preserve authenticated
//! source IDs and append previously inline keys. This avoids rewriting the
//! dictionary through an epoch-sized in-memory map. Readers must never infer
//! meaning from dictionary order.
//!
//! Records are raw 32-byte keys. Pubkeys are uniformly distributed, so the
//! dictionary is incompressible — zstd made it *larger* in measurement — and
//! pages are stored raw. At 32.0 bytes per unique key it is also the single
//! largest structure in an epoch, which is why nothing else stores a pubkey.

use thiserror::Error;

pub const PATH: &str = "dictionary/pubkeys.pages";
pub const SCHEMA: u16 = 1;

/// Bytes of one key record.
pub const RECORD_LEN: usize = 32;

/// Resolve one account ordinal to its key.
///
/// Ordinals are 1-based: zero is the source format's sentinel for a key stored
/// inline rather than in the registry, so treating it as an index would return
/// the wrong account.
pub fn key_at(dictionary: &[u8], ordinal: u32) -> Result<[u8; RECORD_LEN], PubkeyError> {
    if ordinal == 0 {
        return Err(PubkeyError::ReservedOrdinal);
    }
    let start = ((ordinal - 1) as usize)
        .checked_mul(RECORD_LEN)
        .ok_or(PubkeyError::OrdinalOverflow)?;
    dictionary
        .get(start..start + RECORD_LEN)
        .and_then(|slice| slice.try_into().ok())
        .ok_or(PubkeyError::OrdinalOutOfRange {
            ordinal,
            records: (dictionary.len() / RECORD_LEN) as u32,
        })
}

pub fn record_count(dictionary: &[u8]) -> Result<u32, PubkeyError> {
    if !dictionary.len().is_multiple_of(RECORD_LEN) {
        return Err(PubkeyError::NotRecordAligned(dictionary.len()));
    }
    u32::try_from(dictionary.len() / RECORD_LEN).map_err(|_| PubkeyError::OrdinalOverflow)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PubkeyError {
    #[error("pubkey dictionary is {0} bytes, not a multiple of the record width")]
    NotRecordAligned(usize),
    #[error("account ordinal 0 is the inline-key sentinel, not an index")]
    ReservedOrdinal,
    #[error("account ordinal {ordinal} is outside {records} records")]
    OrdinalOutOfRange { ordinal: u32, records: u32 },
    #[error("account ordinal overflows a byte offset")]
    OrdinalOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dictionary() -> Vec<u8> {
        let mut out = Vec::new();
        for byte in 1..=3u8 {
            out.extend_from_slice(&[byte; RECORD_LEN]);
        }
        out
    }

    #[test]
    fn ordinals_are_one_based() {
        let d = dictionary();
        assert_eq!(record_count(&d).unwrap(), 3);
        assert_eq!(key_at(&d, 1).unwrap(), [1u8; 32]);
        assert_eq!(key_at(&d, 3).unwrap(), [3u8; 32]);
    }

    #[test]
    fn ordinal_zero_is_the_sentinel_not_the_first_key() {
        // Treating it as an index would silently return the wrong account.
        assert_eq!(key_at(&dictionary(), 0), Err(PubkeyError::ReservedOrdinal));
    }

    #[test]
    fn an_ordinal_past_the_end_is_an_error_not_a_zero_key() {
        assert_eq!(
            key_at(&dictionary(), 4),
            Err(PubkeyError::OrdinalOutOfRange {
                ordinal: 4,
                records: 3
            })
        );
    }

    #[test]
    fn a_misaligned_dictionary_is_rejected() {
        let mut d = dictionary();
        d.push(0);
        assert_eq!(record_count(&d), Err(PubkeyError::NotRecordAligned(97)));
    }
}
