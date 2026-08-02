//! Canonical, dependency-light primitives for Blockzilla V1 compaction.
//!
//! This crate deliberately contains no scheduler, lease transport, worker,
//! object-store SDK, or catalog backend. The normative contract is
//! `docs/design/blockzilla-compaction-job-v1.md` in the workspace.

#![forbid(unsafe_code)]

mod catalog;
mod codec;
mod error;
mod finality;
mod job;
mod recovery;
mod result;
mod types;
mod validation;

pub use catalog::{
    CATALOG_ENTRY_DOMAIN, CatalogEntryV1, CatalogHeadCasValueV1, CatalogHeadV1,
    CompletionManifestV1, validate_catalog_head_advance,
};
pub use error::{ProtocolError, Result};
pub use finality::{
    FINALITY_MANIFEST_VERSION_V2, FinalityManifestV1, FinalizedBlockIdentityV1,
    FinalizedDispositionV1, FinalizedParentAnchorV1, FinalizedSlotV1, MAX_FINALITY_ENTRIES,
};
pub use job::{
    CompactionJobSpecV1, CompactionJobV1, InputObjectV1, InputStreamRangeV1,
    JOB_SPEC_OBJECT_DOMAIN, LeaseFenceV1, OLD_FAITHFUL_CAR_LOGICAL_NAME_V1,
    OLD_FAITHFUL_CAR_ZST_LOGICAL_NAME_V1, SHRED_TRUST_CONTEXT_LOGICAL_NAME_V1,
};
pub use recovery::{
    ArchiveRecoveryCheckpointV1, ArchiveRecoveryCheckpointValueV1, ArchiveRecoveryObjectV1,
    ArchiveRecoveryReceiptV1, RecoveryVerificationV1, archive_recovery_checkpoint_key,
    archive_recovery_receipt_key, validate_recovery_checkpoint_advance,
};
pub use result::{CandidateManifestV1, CompactionOutcomeV1, CompactionResultV1};
pub use types::{
    DESCRIPTOR_HASH_DOMAIN, HashedDescriptorV1, MAX_DESCRIPTOR_BYTES, MAX_LOGICAL_NAME_BYTES,
    MAX_OBJECT_KEY_BYTES, MAX_OBJECT_VERSION_BYTES, MAX_OUTPUT_NAMESPACE_BYTES,
    MAX_PUBLICATION_OBJECTS, MAX_REQUIRED_INPUTS, NamedObjectRefV1, ObjectRefV1, SlotRangeV1,
};
pub use validation::{validate_complete_candidate_v1, validate_completion_manifest_v1};
