//! CAR (Content Addressable aRchive) reader implementation
//!
//! This crate provides zero-copy parsing and reading of CAR files.
//! Designed to be reusable, auditable, and verifiable against other implementations.

mod cid;
mod node;
mod versioned_transaction;
mod metadata_decoder;
mod convert_metadata;
mod stored_transaction_status_meta;
pub mod car_block_group;
pub mod error;
pub mod reader;

pub use reader::CarBlockReader;

pub mod confirmed_block {
    include!(concat!(
        env!("OUT_DIR"),
        "/solana.storage.confirmed_block.rs"
    ));
}
