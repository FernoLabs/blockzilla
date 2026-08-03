//! Minimal surface for Solana shred parsing used by Blockzilla.
//!
//! This crate intentionally owns the `own` shred implementation as the only active backend.
//! The API intentionally mirrors a small shard of Agave’s `shred` surface we need for replay.
#![forbid(unsafe_code)]

mod own_backend;

pub use own_backend::{Shred, ShredId, ShredType};

/// Active backend identifier.
pub const BACKEND: &str = "own";

pub const DATA_SHREDS_PER_FEC_BLOCK: usize = own_backend::DATA_SHREDS_PER_FEC_BLOCK;

pub const MAX_DATA_SHREDS_PER_SLOT: usize = own_backend::MAX_DATA_SHREDS_PER_SLOT;

pub const MAX_CODE_SHREDS_PER_SLOT: usize = own_backend::MAX_CODE_SHREDS_PER_SLOT;

/// Returns `Some(data_index)` for a serialized data-shred frame or `None` when the frame is
/// shorter than the common shred header.
pub fn get_data_index(payload: &[u8]) -> Option<u32> {
    own_backend::get_data_index(payload)
}
