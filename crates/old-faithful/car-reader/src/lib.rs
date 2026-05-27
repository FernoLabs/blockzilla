#![allow(clippy::items_after_test_module)]

//! Readers and helpers for Old Faithful Solana CAR archives.
//!
//! `of-car-reader` is a streaming reader for Ferno/Old Faithful Solana CAR
//! archives. It can read raw CAR entries, group entries into Solana blocks,
//! decode transactions and transaction status metadata, expose rewards frames,
//! and work with the slot range/index formats used by Old Faithful tooling.
//!
//! # Install
//!
//! ```toml
//! [dependencies]
//! of-car-reader = "0.1.2"
//! ```
//!
//! Default features enable Solana genesis archive parsing and native zstd
//! decompression. Disable default features if you only need plain CAR reading:
//!
//! ```toml
//! [dependencies]
//! of-car-reader = { version = "0.1.2", default-features = false }
//! ```
//!
//! # Which API should I use?
//!
//! Use [`CarStream`] when you want a simple block-by-block iterator over a CAR
//! or `.car.zst` file. Use [`CarBlockReader`] when you need lower-level control,
//! such as scanning every CAR entry, keeping exact byte offsets, or choosing
//! which payloads to load. [`CarBlockGroup`] is the reusable block buffer filled
//! by either API.
//!
//! # Stream blocks from a compressed CAR file
//!
//! This is the usual path for application code. A [`CarStream`] owns one
//! reusable [`CarBlockGroup`], so values borrowed from `group` should be used
//! before calling [`CarStream::next_group`] again.
//!
//! ```rust,no_run
//! use of_car_reader::CarStream;
//! use std::path::Path;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut stream = CarStream::open_zstd(Path::new("epoch-800.car.zst"))?;
//!
//!     while let Some(group) = stream.next_group()? {
//!         let slot = group.slot.expect("block node includes slot");
//!         let (tx_count, _tx_bytes) = group.get_len();
//!         let rewards_bytes = group
//!             .rewards
//!             .as_ref()
//!             .map(|frame| frame.data.len())
//!             .unwrap_or(0);
//!
//!         println!(
//!             "slot={slot} entries={} txs={tx_count} rewards_bytes={rewards_bytes}",
//!             group.entry_count()
//!         );
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! Use [`CarStream::open`] for an uncompressed `.car` file, or
//! [`CarStream::from_reader`] when you already have a reader, for example a
//! custom decompressor or network stream.
//!
//! # Decode transactions from each block
//!
//! [`CarBlockGroup::transactions`] decodes transaction payloads and status
//! metadata in file order. The returned transaction borrows from the group, and
//! the metadata object is reused by the iterator, so consume each item before
//! asking for the next one.
//!
//! ```rust,no_run
//! use of_car_reader::CarStream;
//! use std::path::Path;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut stream = CarStream::open_zstd(Path::new("epoch-800.car.zst"))?;
//!
//!     while let Some(group) = stream.next_group()? {
//!         let slot = group.slot.unwrap_or_default();
//!         let mut txs = group.transactions();
//!
//!         while let Some((tx, metadata)) = txs.next_tx()? {
//!             println!(
//!                 "slot={slot} signatures={} has_metadata={}",
//!                 tx.signatures.len(),
//!                 metadata.is_some()
//!             );
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! If you only need signatures, avoid decoding full transaction metadata:
//!
//! ```rust,no_run
//! use of_car_reader::{CarBlockGroup, CarBlockReader};
//! use std::fs::File;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let file = File::open("epoch-800.car")?;
//!     let mut reader = CarBlockReader::with_capacity(file, 128 << 20);
//!     reader.skip_header()?;
//!
//!     let mut group = CarBlockGroup::with_transaction_signature_prefixes();
//!     while reader.read_until_block_into(&mut group)? {
//!         let mut signatures = group.first_signatures();
//!         while let Some(signature) = signatures.next_signature()? {
//!             println!("first signature starts with {:02x?}", &signature[..4]);
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Scan raw CAR entries
//!
//! Use the lower-level reader when you want CAR offsets, CIDs, or node-type
//! filtering. Call [`CarBlockReader::skip_header`] once, then reuse a scratch
//! buffer for each entry.
//!
//! ```rust,no_run
//! use of_car_reader::{node::peek_node_type, CarBlockReader};
//! use std::fs::File;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let file = File::open("epoch-800.car")?;
//!     let mut reader = CarBlockReader::with_capacity(file, 128 << 20);
//!     reader.skip_header()?;
//!
//!     let mut scratch = Vec::new();
//!     while let Some(entry) = reader.read_entry_payload_with_scratch(&mut scratch)? {
//!         let kind = peek_node_type(entry.payload)?;
//!         println!(
//!             "entry={} offset={} payload_len={} kind={kind}",
//!             entry.location.entry_index,
//!             entry.location.car_offset,
//!             entry.payload_len
//!         );
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Memory and lifetime notes
//!
//! `CarBlockGroup` is designed to be reused. Each call to
//! [`CarBlockReader::read_until_block_into`] or [`CarStream::next_group`] clears
//! and refills the same buffers. Iterators such as
//! [`CarBlockGroup::transactions`], [`CarBlockGroup::transactions_no_meta`], and
//! [`CarBlockGroup::first_signatures`] return borrowed views into those buffers.
//! Do not keep those references after advancing to the next block.
//!
//! # Other modules
//!
//! - [`node`] decodes Old Faithful CBOR nodes and peeks node kinds.
//! - [`slot_ranges`] reads and writes worker slot range files.
//! - [`genesis`] parses Solana genesis archives when the `genesis` feature is
//!   enabled.
//! - [`compact_index`] parses compact indexes when the `compact-index` feature
//!   is enabled.

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

pub use car_block_group::CarBlockGroup;
pub use car_stream::CarStream;
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
