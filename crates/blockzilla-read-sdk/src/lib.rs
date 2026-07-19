//! Read-only SDK for immutable Blockzilla Archive V2 generations.
//!
//! The reader binds filters to both the generation digest and registry hash,
//! validates publication metadata before exposing blocks, reads one independent
//! zstd frame at a time, and fetches signatures only for selected transactions.

mod error;
#[cfg(feature = "http")]
mod http;
pub mod manifest;
mod reader;
mod source;

pub use error::{Error, Result, SourceError};
#[cfg(feature = "http")]
pub use http::{HttpRangeSource, HttpRangeSourceOptions};
pub use reader::{
    ArchiveReader, BlockIterator, CompiledPubkeyFilter, DecodedBlock, GenerationBinding,
    HashVerification, IndeterminateReason, MetadataState, OpenOptions, ScanIterator, ScannedBlock,
    ScannedTransaction, SignatureReference, TransactionMatch, ValidatedGeneration,
    validate_generation_structure,
};
pub use source::{LocalRangeSource, OverlayRangeSource, RangeSource, SourceResult};
