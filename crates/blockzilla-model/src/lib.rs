//! Source-neutral ordered query types for Blockzilla archive readers.
//!
//! Format adapters validate CAR, Compact V2, or standalone Indexer V3 data and publish
//! one canonical transaction stream. A sink can therefore implement one query
//! without depending on source-specific transaction types.

mod error;
mod fingerprint;
mod model;
pub mod projection_pool;
mod source;
pub mod token;

pub use error::{Error, Result};
pub use fingerprint::BlockUniverseFingerprint;
pub use model::{
    BlockCounts, BlockHeader, BlockView, CanonicalBlock, CanonicalTransaction, CoverageReason,
    CpiCoverage, ExecutionStatus, InstructionCoordinate, InstructionCoverage,
    InstructionDataCoverage, MAX_CANONICAL_REQUIRED_SIGNERS, MAX_CANONICAL_SHORT_VEC_ITEMS,
    RecordedTokenBalance, ResolvedInstruction, TokenBalanceCoverage, TokenBalanceSide,
    TransactionHeader, TransactionView,
};
pub use source::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveInstructionSourceExt, ArchiveIoSnapshot,
    BlockSink, FnBlockSink, InstructionDataRequirement, MAX_INSTRUCTION_DATA_PROGRAMS,
    MAX_TOKEN_BALANCE_MINTS, OrderedBlockPublisher, ScanIoReceipt, ScanRange, ScanReceipt,
    ScanRequest, SourceIdentity, SourceVerification, TokenBalanceRequirement, validate_request,
};
