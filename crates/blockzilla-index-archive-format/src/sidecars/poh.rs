//! `sidecars/poh.wincode`: retained block-framed PoH records.
//!
//! The common 64-byte `FileHeader` is followed by one 8-byte [`PohPreamble`].
//! It selects one grammar for every frame, so readers never use trial decode.
//! A catalog PoH span starts after the preamble and covers one full canonical
//! unsigned-LEB128 length prefix plus its exact Wincode 0.5.5 payload.
//! Retention keeps the compact source bytes unchanged. A converter must first
//! validate each retained `block_id` and `slot` against its catalog row.

use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    sidecars::framing::{self, FrameError},
    wincode as wire,
};

pub const PATH: &str = "sidecars/poh.wincode";
pub const SCHEMA: u16 = 1;
pub const PREAMBLE_MAGIC: [u8; 4] = *b"BZPH";
pub const PREAMBLE_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PohWireProfile {
    ArchiveV2CurrentWincode055 = 1,
    ArchiveV2LegacyNoSignatureCountWincode055 = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PohPreamble {
    pub profile: PohWireProfile,
}

impl PohPreamble {
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

    pub fn decode(bytes: &[u8]) -> Result<Self, PohError> {
        let bytes: &[u8; PREAMBLE_LEN] = bytes
            .try_into()
            .map_err(|_| PohError::PreambleLength(bytes.len()))?;
        if bytes[..4] != PREAMBLE_MAGIC {
            return Err(PohError::PreambleMagic);
        }
        if bytes[5..] != [0; 3] {
            return Err(PohError::ReservedPreambleBytes);
        }
        let profile = match bytes[4] {
            1 => PohWireProfile::ArchiveV2CurrentWincode055,
            2 => PohWireProfile::ArchiveV2LegacyNoSignatureCountWincode055,
            other => return Err(PohError::UnknownProfile(other)),
        };
        Ok(Self { profile })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct CurrentPohRecord {
    pub block_id: u32,
    pub slot: u64,
    pub entries: Vec<CurrentPohEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct CurrentPohEntry {
    pub num_hashes: u64,
    pub hash: [u8; 32],
    pub transaction_count: u32,
    pub signature_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct LegacyPohRecord {
    pub block_id: u32,
    pub slot: u64,
    pub entries: Vec<LegacyPohEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct LegacyPohEntry {
    pub num_hashes: u64,
    pub hash: [u8; 32],
    pub transaction_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedPohFrame {
    Current(CurrentPohRecord),
    LegacyNoSignatureCount(LegacyPohRecord),
}

impl DecodedPohFrame {
    pub const fn identity(&self) -> (u32, u64) {
        match self {
            Self::Current(record) => (record.block_id, record.slot),
            Self::LegacyNoSignatureCount(record) => (record.block_id, record.slot),
        }
    }

    pub fn entry_count(&self) -> usize {
        match self {
            Self::Current(record) => record.entries.len(),
            Self::LegacyNoSignatureCount(record) => record.entries.len(),
        }
    }

    /// Final entry hash used by `HashOwner::PohBlockFinal`, or `None` for a
    /// retained block with no PoH entries.
    pub fn final_hash(&self) -> Option<&[u8; 32]> {
        match self {
            Self::Current(record) => record.entries.last().map(|entry| &entry.hash),
            Self::LegacyNoSignatureCount(record) => record.entries.last().map(|entry| &entry.hash),
        }
    }
}

/// Decode one catalog-addressed frame using only the selected preamble profile.
pub fn decode_frame(profile: PohWireProfile, frame: &[u8]) -> Result<DecodedPohFrame, PohError> {
    let payload = framing::decode_frame(frame)?;
    decode_payload(profile, payload)
}

/// Decode one retained payload after its outer length prefix was validated.
pub fn decode_payload(
    profile: PohWireProfile,
    payload: &[u8],
) -> Result<DecodedPohFrame, PohError> {
    Ok(match profile {
        PohWireProfile::ArchiveV2CurrentWincode055 => {
            DecodedPohFrame::Current(wire::decode_exact(payload)?)
        }
        PohWireProfile::ArchiveV2LegacyNoSignatureCountWincode055 => {
            DecodedPohFrame::LegacyNoSignatureCount(wire::decode_exact(payload)?)
        }
    })
}

#[derive(Debug, Error)]
pub enum PohError {
    #[error("PoH preamble has {0} bytes, expected {PREAMBLE_LEN}")]
    PreambleLength(usize),
    #[error("PoH preamble magic is invalid")]
    PreambleMagic,
    #[error("PoH preamble profile {0} is unknown")]
    UnknownProfile(u8),
    #[error("PoH preamble reserved bytes are not zero")]
    ReservedPreambleBytes,
    #[error("PoH frame: {0}")]
    Frame(#[from] FrameError),
    #[error("PoH Wincode: {0}")]
    Wincode(#[from] wincode::ReadError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preamble_has_frozen_bytes_and_rejects_unknown_profiles() {
        let preamble = PohPreamble {
            profile: PohWireProfile::ArchiveV2CurrentWincode055,
        };
        assert_eq!(preamble.encode(), *b"BZPH\x01\0\0\0");
        assert_eq!(PohPreamble::decode(&preamble.encode()).unwrap(), preamble);
        assert!(matches!(
            PohPreamble::decode(b"BZPH\x03\0\0\0"),
            Err(PohError::UnknownProfile(3))
        ));
    }

    #[test]
    fn selected_profile_decodes_once() {
        let record = CurrentPohRecord {
            block_id: 7,
            slot: 100,
            entries: vec![CurrentPohEntry {
                num_hashes: 2,
                hash: [9; 32],
                transaction_count: 1,
                signature_count: 2,
            }],
        };
        let payload = wire::encode(&record).unwrap();
        let frame = framing::encode_frame(&payload).unwrap();
        let decoded = decode_frame(PohWireProfile::ArchiveV2CurrentWincode055, &frame).unwrap();
        assert_eq!(decoded.final_hash(), Some(&[9; 32]));
        assert_eq!(decoded, DecodedPohFrame::Current(record));
    }
}
