//! Source-neutral ordered query types for Blockzilla archive readers.
//!
//! Format adapters validate CAR, Compact V2, or Indexer V3 data and publish
//! one canonical transaction stream. A sink can therefore implement one query
//! without depending on source-specific transaction types.

mod error;
mod model;
mod source;
pub mod token;

pub use error::{Error, Result};
pub use model::{
    BlockHeader, BlockView, CanonicalBlock, CanonicalTransaction, CoverageReason, CpiCoverage,
    ExecutionStatus, InstructionCoordinate, InstructionCoverage, InstructionDataCoverage,
    MAX_CANONICAL_REQUIRED_SIGNERS, MAX_CANONICAL_SHORT_VEC_ITEMS, ResolvedInstruction,
    TransactionHeader, TransactionView,
};
pub use source::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveInstructionSourceExt, BlockSink, FnBlockSink,
    InstructionDataRequirement, MAX_INSTRUCTION_DATA_PROGRAMS, OrderedBlockPublisher,
    ScanIoReceipt, ScanRange, ScanReceipt, ScanRequest, SourceIdentity, SourceVerification,
    validate_request,
};
