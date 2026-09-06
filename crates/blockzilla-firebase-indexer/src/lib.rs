//! Compatibility re-exports for the operational index command.
//!
//! New library users should depend on `blockzilla-user-program-index`.

pub use blockzilla_user_program_index::{
    build, decode, dense_accumulator, format, query, signer_rank,
};

#[path = "bin/archive-v2-account-projection/standalone_v2.rs"]
#[allow(clippy::duplicate_mod, dead_code)]
mod indexer_v3_wire;
// The recovered posting reader is also compiled by its converter binary,
// where its sibling has this original module name.
use indexer_v3_wire as standalone_v2;

#[path = "bin/archive-v2-account-projection/standalone_account_postings.rs"]
#[allow(clippy::duplicate_mod, dead_code)]
mod indexer_v3_postings;

mod indexer_v3_candidates;
mod indexer_v3_query;
mod indexer_v3_registry;

pub use indexer_v3_candidates::{
    IndexerV3CandidateBlocks, IndexerV3CandidateCounts, IndexerV3CandidateCoverage,
    IndexerV3CandidateGeometry, IndexerV3CandidateKey, IndexerV3CandidatePolicy,
    IndexerV3CandidateReadStats, build_indexer_v3_candidate_blocks,
    build_indexer_v3_candidate_blocks_for_key,
};

pub use indexer_v3_postings::{
    ADAPTIVE_V3_CONTROL_FILE, ADAPTIVE_V3_COVERAGE_FILE, ADAPTIVE_V3_PAGES_FILE,
    AdaptiveOpenReadStats, AdaptiveV3Reader, LimitedLookupResult as AdaptiveV3LimitedLookupResult,
    LookupResult as AdaptiveV3LookupResult, PostingVisitSummary as AdaptiveV3PostingVisitSummary,
    ResolvedCoverage as AdaptiveV3ResolvedCoverage, ResolvedPosting as AdaptiveV3ResolvedPosting,
    RoleBlockVisitSummary as AdaptiveV3RoleBlockVisitSummary,
    RoleMatchedBlock as AdaptiveV3RoleMatchedBlock,
};
pub use indexer_v3_query::{
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
pub use indexer_v3_registry::IndexerV3RegistryIndex;

// Wire-profile migration tooling, retained against the legacy read SDK.
pub mod firewatch_controller_cgroup;
pub mod firewatch_controller_eta;
pub mod firewatch_wire_profile_attestation;
pub mod firewatch_wire_profile_transition;
