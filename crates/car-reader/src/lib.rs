//! CAR (Content Addressable aRchive) reader implementation
//!
//! This crate provides zero-copy parsing and reading of CAR files.
//! Designed to be reusable, auditable, and verifiable against other implementations.

pub mod car_block_group;
mod cid;
mod convert_metadata;
pub mod error;
pub mod metadata_decoder;
pub mod node;
pub mod reader;
pub mod stored_transaction_status_meta;
pub mod versioned_transaction;

pub use reader::CarBlockReader;

pub mod confirmed_block {
    include!(concat!(
        env!("OUT_DIR"),
        "/solana.storage.confirmed_block.rs"
    ));
}
