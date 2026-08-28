//! Compatibility re-exports for the operational index command.
//!
//! New library users should depend on `blockzilla-user-program-index`.

pub use blockzilla_user_program_index::{
    build, decode, dense_accumulator, format, query, signer_rank,
};

#[path = "bin/archive-v2-account-projection/standalone_v2.rs"]
#[allow(clippy::duplicate_mod, dead_code)]
mod indexer_v3_wire;

mod indexer_v3_query;

pub use indexer_v3_query::{
    INDEXER_V3_OPTIONAL_RETAINED_SIDECARS, INDEXER_V3_QUERY_REGISTRY_RETAINED_KEY_BYTES,
    INDEXER_V3_REQUIRED_RETAINED_SIDECARS, IndexerV3InstructionSource,
    IndexerV3InstructionSourceError, IndexerV3InstructionSourceResult, IndexerV3SourceScope,
    indexer_v3_required_ledger_objects,
};
