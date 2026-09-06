//! Compatibility re-exports for the operational index command.
//!
//! Index applications should depend on `blockzilla-user-program-index`.
//! Archive V3 readers should depend on `blockzilla-archive-v3-reader`.

pub use blockzilla_user_program_index::{
    build, decode, dense_accumulator, format, query, signer_rank,
};

// Archive V3 now owns its reader; retain these names for existing consumers.
pub use blockzilla_archive_v3_reader::{
    IndexerV3CandidateBlocks, IndexerV3CandidateCounts, IndexerV3CandidateCoverage,
    IndexerV3CandidateGeometry, IndexerV3CandidateKey, IndexerV3CandidatePolicy,
    IndexerV3CandidateReadStats, build_indexer_v3_candidate_blocks,
    build_indexer_v3_candidate_blocks_for_key,
};

pub use blockzilla_archive_v3_reader::IndexerV3RegistryIndex;
pub use blockzilla_archive_v3_reader::{
    ADAPTIVE_V3_CONTROL_FILE, ADAPTIVE_V3_COVERAGE_FILE, ADAPTIVE_V3_PAGES_FILE,
    AdaptiveOpenReadStats, AdaptiveV3LimitedLookupResult, AdaptiveV3LookupResult,
    AdaptiveV3PostingVisitSummary, AdaptiveV3Reader, AdaptiveV3ResolvedCoverage,
    AdaptiveV3ResolvedPosting, AdaptiveV3RoleBlockVisitSummary, AdaptiveV3RoleMatchedBlock,
};
pub use blockzilla_archive_v3_reader::{
    INDEXER_V3_OPTIONAL_RETAINED_SIDECARS, INDEXER_V3_PARALLEL_BLOCKS_PER_JOB,
    INDEXER_V3_PARALLEL_BUFFERED_BLOCKS_PER_WORKER,
    INDEXER_V3_PARALLEL_DECLARED_DECODED_BYTE_LIMIT,
    INDEXER_V3_PARALLEL_RETAINED_PROJECTION_SCRATCH_LIMIT,
    INDEXER_V3_PARALLEL_RETAINED_TRANSACTION_BUFFER_LIMIT,
    INDEXER_V3_PARALLEL_RETAINED_WORKSPACE_LIMIT, INDEXER_V3_PARALLEL_TRANSACTION_LIMIT,
    INDEXER_V3_QUERY_REGISTRY_RETAINED_KEY_BYTES, INDEXER_V3_REQUIRED_RETAINED_SIDECARS,
    IndexerV3InstructionSource, IndexerV3InstructionSourceError, IndexerV3InstructionSourceResult,
    IndexerV3ParallelScanReceipt, IndexerV3ParallelScanStats, IndexerV3RegistryReadMode,
    IndexerV3RegistryReadPolicy, IndexerV3RegistryReadReceipt, IndexerV3SelectiveScanReceipt,
    IndexerV3SourceScope, MAX_INDEXER_V3_PARALLEL_WORKERS, indexer_v3_required_ledger_objects,
};

// Wire-profile migration tooling, retained against the legacy read SDK.
pub mod firewatch_controller_cgroup;
pub mod firewatch_controller_eta;
pub mod firewatch_wire_profile_attestation;
pub mod firewatch_wire_profile_transition;
