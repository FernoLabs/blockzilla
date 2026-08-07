//! Minimal surface for Solana shred parsing used by Blockzilla.
//!
//! This crate intentionally uses a single Blockzilla shred implementation as the only active parsing
//! implementation. The API intentionally mirrors a small shard of Agave’s `shred` surface we need for
//! replay.
#![forbid(unsafe_code)]

mod blockzilla_shred;

pub use blockzilla_shred::{Shred, ShredId, ShredType};

/// Active implementation identifier.
pub const BACKEND: &str = "blockzilla-shred";

pub const DATA_SHREDS_PER_FEC_BLOCK: usize = blockzilla_shred::DATA_SHREDS_PER_FEC_BLOCK;

pub const MAX_DATA_SHREDS_PER_SLOT: usize = blockzilla_shred::MAX_DATA_SHREDS_PER_SLOT;

pub const MAX_CODE_SHREDS_PER_SLOT: usize = blockzilla_shred::MAX_CODE_SHREDS_PER_SLOT;

/// Returns `Some(data_index)` for a serialized data-shred frame or `None` when the frame is
/// shorter than the common shred header.
pub fn get_data_index(payload: &[u8]) -> Option<u32> {
    blockzilla_shred::get_data_index(payload)
}
