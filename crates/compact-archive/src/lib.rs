//! Compact archive format for Blockzilla
//! 
//! Defines the compacted archive format and provides read/write APIs
//! for encoding, decoding, and I/O operations.

#![warn(missing_docs)]

pub mod error;
pub mod format;
pub mod reader;
pub mod writer;

pub use error::{ArchiveError, Result};
pub use format::{BlockData, EpochMetadata, Registry, RuntimeInfo, SlotIndex};
pub use reader::ArchiveReader;
pub use writer::ArchiveWriter;
