//! Minimal Solana shred transport envelope helpers shared between ingestion and archival services.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Uncompressed, byte-for-byte Solana shred datagram.
pub const RAW_SOLANA_SHRED_V1: u16 = 3;
/// Independently zstd-compressed, byte-for-byte Solana shred datagram.
pub const ZSTD_SOLANA_SHRED_V1: u16 = 4;

/// Smallest raw shred header fragment required for slot/fec/shred kind inference.
pub const COMMON_SHRED_HEADER_BYTES: usize = 83;
const SHRED_VARIANT_OFFSET: usize = 64;
const SLOT_OFFSET: usize = 65;
const INDEX_OFFSET: usize = 73;
const VERSION_OFFSET: usize = 77;
const FEC_SET_INDEX_OFFSET: usize = 79;
/// Maximum UDP payload size used by the recorder decompress envelope.
pub const MAX_UDP_DATAGRAM_BYTES: usize = 65_535;

/// Stable classification of shred coordinates found in parsed raw payload header bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ShredKind {
    Data,
    Coding,
}

/// Parsed Solana shred header tuple used to build logical ingest keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedShredHeader {
    pub slot: u64,
    pub index: u32,
    pub version: u16,
    pub fec_set_index: u32,
    pub kind: ShredKind,
}

/// Decode one independently compressed raw-shred spool payload.
///
/// The returned bytes are still an untrusted Solana UDP datagram. Callers must parse and validate
/// the shred before use; this helper only applies the recorder's bounded zstd envelope.
pub fn decode_stored_shred(payload: &[u8]) -> Result<Vec<u8>> {
    zstd::bulk::decompress(payload, MAX_UDP_DATAGRAM_BYTES)
        .context("decompress stored shred datagram")
}

/// Parse a raw Solana shred header from an untrusted datagram payload.
pub fn parse_shred_header(payload: &[u8]) -> Option<ParsedShredHeader> {
    if payload.len() < COMMON_SHRED_HEADER_BYTES {
        return None;
    }
    let kind = match payload[SHRED_VARIANT_OFFSET] & 0xf0 {
        0x60 | 0x70 => ShredKind::Coding,
        0x90 | 0xb0 => ShredKind::Data,
        _ => return None,
    };
    Some(ParsedShredHeader {
        slot: u64::from_le_bytes(payload[SLOT_OFFSET..SLOT_OFFSET + 8].try_into().ok()?),
        index: u32::from_le_bytes(payload[INDEX_OFFSET..INDEX_OFFSET + 4].try_into().ok()?),
        version: u16::from_le_bytes(
            payload[VERSION_OFFSET..VERSION_OFFSET + 2]
                .try_into()
                .ok()?,
        ),
        fec_set_index: u32::from_le_bytes(
            payload[FEC_SET_INDEX_OFFSET..FEC_SET_INDEX_OFFSET + 4]
                .try_into()
                .ok()?,
        ),
        kind,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_data_and_coding_shred_coordinates() {
        for (variant, kind) in [(0x90, ShredKind::Data), (0x6f, ShredKind::Coding)] {
            let mut payload = [0u8; COMMON_SHRED_HEADER_BYTES];
            payload[SHRED_VARIANT_OFFSET] = variant;
            payload[SLOT_OFFSET..SLOT_OFFSET + 8].copy_from_slice(&42u64.to_le_bytes());
            payload[INDEX_OFFSET..INDEX_OFFSET + 4].copy_from_slice(&7u32.to_le_bytes());
            payload[VERSION_OFFSET..VERSION_OFFSET + 2].copy_from_slice(&50093u16.to_le_bytes());
            payload[FEC_SET_INDEX_OFFSET..FEC_SET_INDEX_OFFSET + 4]
                .copy_from_slice(&3u32.to_le_bytes());

            assert_eq!(
                parse_shred_header(&payload),
                Some(ParsedShredHeader {
                    slot: 42,
                    index: 7,
                    version: 50093,
                    fec_set_index: 3,
                    kind,
                })
            );
        }
    }

    #[test]
    fn rejects_short_or_unknown_shreds() {
        assert_eq!(parse_shred_header(&[0; 82]), None);
        assert_eq!(parse_shred_header(&[0; COMMON_SHRED_HEADER_BYTES]), None);
    }
}
