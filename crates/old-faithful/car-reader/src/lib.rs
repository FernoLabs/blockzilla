#![allow(clippy::items_after_test_module)]

//! CAR (Content Addressable aRchive) reader implementation
//!
//! This crate provides zero-copy parsing and reading of CAR files.
//! Designed to be reusable, auditable, and verifiable against other implementations.

pub mod car_block_group;
pub mod car_stream;
#[cfg(feature = "compact-index")]
pub mod compact_index;
mod convert_metadata;
pub mod error;
#[cfg(feature = "genesis")]
pub mod genesis;
pub mod metadata_decoder;
pub mod node;
pub mod reader;
pub mod reconstruct;
pub mod slot_ranges;
pub mod stored_transaction;
pub mod versioned_transaction;

pub use reader::CarBlockReader;

pub mod confirmed_block {
    include!(concat!(
        env!("OUT_DIR"),
        "/solana.storage.confirmed_block.rs"
    ));
}

#[doc(hidden)]
pub mod confirmed_block_borrowed {
    #![allow(non_snake_case, non_camel_case_types, non_upper_case_globals)]
    include!(concat!(env!("OUT_DIR"), "/quick-protobuf/mod.rs"));
}
