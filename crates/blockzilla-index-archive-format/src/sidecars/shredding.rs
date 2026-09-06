//! `sidecars/shredding.wincode`: retained block-framed shredding records.
//!
//! The common 64-byte `FileHeader` is followed by one 8-byte
//! [`ShreddingPreamble`]. Each catalog span starts after it and covers one full
//! canonical unsigned-LEB128 length prefix plus its exact Wincode 0.5.5
//! payload. A converter validates retained block identity before it copies the
//! frame. Recorded empty boundaries remain a present frame, not absence.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    sidecars::framing::{self, FrameError},
    wincode as wire,
};

pub const PATH: &str = "sidecars/shredding.wincode";
pub const SCHEMA: u16 = 1;
pub const PREAMBLE_MAGIC: [u8; 4] = *b"BZSH";
pub const PREAMBLE_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ShreddingWireProfile {
    ArchiveV2Wincode055 = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShreddingPreamble {
    pub profile: ShreddingWireProfile,
}

impl ShreddingPreamble {
    pub const fn encode(self) -> [u8; PREAMBLE_LEN] {
        [
            PREAMBLE_MAGIC[0],
            PREAMBLE_MAGIC[1],
            PREAMBLE_MAGIC[2],
            PREAMBLE_MAGIC[3],
            self.profile as u8,
            0,
            0,
            0,
        ]
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ShreddingError> {
        let bytes: &[u8; PREAMBLE_LEN] = bytes
            .try_into()
            .map_err(|_| ShreddingError::PreambleLength(bytes.len()))?;
        if bytes[..4] != PREAMBLE_MAGIC {
            return Err(ShreddingError::PreambleMagic);
        }
        if bytes[5..] != [0; 3] {
            return Err(ShreddingError::ReservedPreambleBytes);
        }
        let profile = match bytes[4] {
            1 => ShreddingWireProfile::ArchiveV2Wincode055,
            other => return Err(ShreddingError::UnknownProfile(other)),
        };
        Ok(Self { profile })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ShreddingRecord {
    pub block_id: u32,
    pub slot: u64,
    pub boundaries: Vec<ShreddingBoundary>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ShreddingBoundary {
    pub entry_end_index: i64,
    pub shred_end_index: i64,
}

pub fn decode_frame(
    profile: ShreddingWireProfile,
    frame: &[u8],
) -> Result<ShreddingRecord, ShreddingError> {
    let payload = framing::decode_frame(frame)?;
    decode_payload(profile, payload)
}

/// Decode one retained payload after its outer length prefix was validated.
pub fn decode_payload(
    profile: ShreddingWireProfile,
    payload: &[u8],
) -> Result<ShreddingRecord, ShreddingError> {
    match profile {
        ShreddingWireProfile::ArchiveV2Wincode055 => Ok(wire::decode_exact(payload)?),
    }
}

#[derive(Debug, Error)]
pub enum ShreddingError {
    #[error("shredding preamble has {0} bytes, expected {PREAMBLE_LEN}")]
    PreambleLength(usize),
    #[error("shredding preamble magic is invalid")]
    PreambleMagic,
    #[error("shredding preamble profile {0} is unknown")]
    UnknownProfile(u8),
    #[error("shredding preamble reserved bytes are not zero")]
    ReservedPreambleBytes,
    #[error("shredding frame: {0}")]
    Frame(#[from] FrameError),
    #[error("shredding Wincode: {0}")]
    Wincode(#[from] wincode::ReadError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preamble_has_frozen_bytes() {
        let preamble = ShreddingPreamble {
            profile: ShreddingWireProfile::ArchiveV2Wincode055,
        };
        assert_eq!(preamble.encode(), *b"BZSH\x01\0\0\0");
        assert_eq!(
            ShreddingPreamble::decode(&preamble.encode()).unwrap(),
            preamble
        );
    }

    #[test]
    fn retained_frame_preserves_recorded_empty() {
        let record = ShreddingRecord {
            block_id: 7,
            slot: 100,
            boundaries: Vec::new(),
        };
        let payload = wire::encode(&record).unwrap();
        let frame = framing::encode_frame(&payload).unwrap();
        assert_eq!(
            decode_frame(ShreddingWireProfile::ArchiveV2Wincode055, &frame).unwrap(),
            record
        );
    }
}
