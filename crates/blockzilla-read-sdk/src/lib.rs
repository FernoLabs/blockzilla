//! Read-only SDK for immutable Blockzilla Archive V2 generations.
//!
//! The reader binds filters to both the generation digest and registry hash,
//! validates publication metadata before exposing blocks, reads one independent
//! zstd frame at a time, and fetches signatures only for selected transactions.

pub mod archive_integrity;
pub mod archive_signatures;
pub mod blockhash;
mod compact_query;
pub mod descriptor;
mod error;
#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
mod http_cache;
pub mod manifest;
mod message_projection;
mod message_schema;
mod metadata_projection;
mod metadata_schema;
mod reader;
pub mod signed_message;
mod source;

pub use archive_signatures::{
    ArchiveSignatureConfig, ArchiveSignatureError, ArchiveSignatureReport, SignatureResult,
    verify_archive_v2_signatures,
};
pub use blockhash::{
    BLOCKHASH_RECORD_LEN, BlockhashResolver, BlockhashResolverError, MAX_BLOCKHASH_REGISTRY_BYTES,
    PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN, PREVIOUS_BLOCKHASH_LEGACY_RECORD_LEN,
    PREVIOUS_BLOCKHASH_TAIL_CAPACITY, PreviousBlockhash, PreviousBlockhashTail,
    PreviousBlockhashTailSchema, detect_previous_blockhash_tail, parse_blockhash_registry,
    parse_previous_blockhash_tail,
};
pub use blockzilla_format::CompactPubkey;
pub use compact_query::{
    COMPACT_V2_PARALLEL_COMPRESSED_BUFFERS, COMPACT_V2_PARALLEL_MAX_BLOCKS_PER_BATCH,
    COMPACT_V2_PARALLEL_UNCOMPRESSED_BATCH_BYTES,
    COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
    COMPACT_V2_PROJECTION_SCRATCH_RETAINED_BYTES, COMPACT_V2_QUERY_REGISTRY_RETAINED_KEY_BYTES,
    COMPACT_V2_REGISTRY_PREFETCH_READ_BYTES, CompactV2InstructionSource,
    CompactV2InstructionSourceError, CompactV2InstructionSourceResult,
    CompactV2ParallelRegistryMode, CompactV2ParallelRegistryReceipt, CompactV2ParallelScanConfig,
    CompactV2ParallelScanReceipt, CompactV2RegistryReadPolicy,
    DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES,
};
pub use descriptor::{
    ArchiveDescriptor, ArchiveIdentity, ArchiveObject, ArchiveSourceBinding,
    COMPACT_V2_OPTIONAL_OBJECTS, COMPACT_V2_REQUIRED_OBJECTS, mainnet_identity_for_slot,
};
pub use error::{Error, Result, SourceError};
#[cfg(feature = "http")]
pub use http::{
    HttpObjectIdentity, HttpObjectPathLayout, HttpRangeSource, HttpRangeSourceOptions,
    HttpRangeSourceStats,
};
#[cfg(feature = "http")]
pub use http_cache::{
    CachedHttpRangeSource, DEFAULT_HTTP_CACHE_MAX_OBJECT_BYTES, DEFAULT_HTTP_CACHE_MAX_TOTAL_BYTES,
    HttpRangeCacheOptions, HttpRangeCachePlan, HttpRangeCacheStats,
    MAX_HTTP_CACHE_DOWNLOAD_RANGE_BYTES, create_http_cache_directory,
};
pub use manifest::{
    OperatorTrustedLocalDescriptor, OperatorTrustedLocalFile, TrustedGenerationIdentity,
};
pub use message_projection::{
    CompactV2MessageProjectionError, CompactV2MessageProjectionResult, CompactV2MessageProjector,
    MAX_COMPACT_V2_MESSAGE_ACCOUNTS, ProjectedCompactV2AddressTableLookup,
    ProjectedCompactV2Instruction, ProjectedCompactV2Message, ProjectedCompactV2MessageVersion,
};
pub use message_schema::{
    COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_BYTES, COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
    COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SHA256, COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_SIZE,
    COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_BYTES, COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
    COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SHA256, COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_SIZE,
    CompactV2MessageSchema, CompactV2MessageSchemaError, decode_compact_v2_message,
    detect_compact_v2_message_schema, select_compact_v2_message_schema,
    select_unpublished_compact_v2_message_schema,
};
pub use metadata_projection::{
    CompactV2ExecutionStatus, CompactV2MetadataProjectionError, CompactV2MetadataProjectionLimits,
    CompactV2MetadataProjectionResult, CompactV2MetadataProjector, MAX_COMPACT_V2_CPI_INSTRUCTIONS,
    MAX_COMPACT_V2_METADATA_ACCOUNTS, MAX_COMPACT_V2_TOP_LEVEL_INSTRUCTIONS,
    ProjectedCompactV2InnerInstruction, ProjectedCompactV2InnerInstructionGroup,
    ProjectedCompactV2Metadata, ProjectedCompactV2TokenBalances,
};
pub use metadata_schema::{
    COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_BYTES, COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE,
    COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SHA256, COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_SIZE,
    CompactV2MetadataSchema, CompactV2MetadataSchemaError, decode_compact_v2_metadata,
    detect_compact_v2_metadata_schema, select_compact_v2_metadata_schema,
    select_unpublished_compact_v2_metadata_schema,
};
pub use reader::{
    ArchiveGenerationDescriptor, ArchiveReader, ArchiveReaderSourceKind, BlockIterator,
    BorrowedBlockStream, BorrowedBlockStreamIoStats, BorrowedDecodedBlock,
    BorrowedDecodedTxRowIter, CompiledPubkeyFilter, DecodedBlock, GenerationBinding,
    HashVerification, IndeterminateReason, MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH,
    MAX_ORDERED_PARALLEL_COMPRESSED_BUFFERS, MAX_ORDERED_PARALLEL_DECODE_WORKERS,
    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES, MAX_ORDERED_PARALLEL_TRANSACTIONS_PER_BATCH,
    MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES, MetadataState, OpenOptions,
    OrderedParallelBlockConfig, OrderedParallelBlockStats, ProgramInvocationMatch,
    PubkeyReferenceMatch, RecycledBlockScratch, RecycledBlockStats, ScanIterator, ScannedBlock,
    ScannedTransaction, SelectorIndeterminateReason, SelectorOutcome, SignatureReference,
    TokenBalanceMatch, TransactionMatch, ValidatedGeneration, compact_v2_first_slot,
    validate_generation_structure,
};
pub use signed_message::{
    InstructionDataCandidate, InstructionDataEncoding, MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS,
    MAX_VOTE_HASH_REGISTRY_BYTES, ResolvedAddressTableLookup, SelectedSignedMessage,
    SignedInstruction, SignedInstructionCandidates, SignedMessage, SignedMessageCandidates,
    SignedMessageError, SignedMessageVersion, SignedTransactionConfig, VOTE_HASH_RECORD_LEN,
    VoteHashKind, VoteHashRegistry, VoteHashResolver, reconstruct_instruction_data,
    reconstruct_instruction_data_candidates, select_signed_message_candidate,
    select_signed_message_candidate_ed25519, serialize_signed_message, verify_ed25519_signatures,
};
pub use source::{
    LocalRangeSource, OverlayRangeSource, PinnedLocalObjectIdentity, PinnedLocalRangeSource,
    PinnedLocalRangeSourceStats, RangeSource, SourceResult,
};
