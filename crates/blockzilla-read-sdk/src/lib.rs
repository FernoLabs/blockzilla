//! Read-only SDK for immutable Blockzilla Archive V2 generations.
//!
//! The reader binds filters to both the generation digest and registry hash,
//! validates publication metadata before exposing blocks, reads one independent
//! zstd frame at a time, and fetches signatures only for selected transactions.

pub mod archive_integrity;
mod error;
#[cfg(feature = "http")]
mod http;
pub mod manifest;
mod message_schema;
mod metadata_schema;
mod reader;
mod source;

pub use error::{Error, Result, SourceError};
#[cfg(feature = "http")]
pub use http::{HttpRangeSource, HttpRangeSourceOptions};
pub use message_schema::{
    COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES, COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
    COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SHA256, COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SIZE,
    CompactV2MessageSchema, CompactV2MessageSchemaError, decode_compact_v2_message,
    select_compact_v2_message_schema, select_unpublished_compact_v2_message_schema,
};
pub use metadata_schema::{
    CompactV2MetadataSchema, CompactV2MetadataSchemaError, decode_compact_v2_metadata,
};
pub use reader::{
    ArchiveReader, BlockIterator, BorrowedBlockStream, BorrowedDecodedBlock,
    BorrowedDecodedTxRowIter, CompiledPubkeyFilter, DecodedBlock, GenerationBinding,
    HashVerification, IndeterminateReason, MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH,
    MAX_ORDERED_PARALLEL_COMPRESSED_BUFFERS, MAX_ORDERED_PARALLEL_DECODE_WORKERS,
    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES,
    MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES, MetadataState, OpenOptions,
    OrderedParallelBlockConfig, OrderedParallelBlockStats, RecycledBlockScratch,
    RecycledBlockStats, ScanIterator, ScannedBlock, ScannedTransaction, SignatureReference,
    TransactionMatch, ValidatedGeneration, validate_generation_structure,
};
pub use source::{
    LocalRangeSource, OverlayRangeSource, PinnedLocalRangeSource, RangeSource, SourceResult,
};
