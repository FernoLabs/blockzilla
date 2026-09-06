//! Read-only SDK for immutable Blockzilla Archive V2 generations.
//!
//! The reader binds filters to both the generation digest and registry hash,
//! validates publication metadata before exposing blocks, reads one independent
//! zstd frame at a time, and fetches signatures only for selected transactions.

mod error;
#[cfg(feature = "http")]
mod http;
pub mod manifest;
mod message_projection;
mod metadata_wire_profile;
mod publication_lock;
mod reader;
mod registry_index_admission;
mod selective_metadata;
mod source;
mod wire_profile;
mod wire_profile_audit;

pub use error::{Error, Result, SourceError};
#[cfg(feature = "http")]
pub use http::{HttpRangeSource, HttpRangeSourceOptions};
pub use message_projection::{
    ArchiveV2InstructionProgramSemantics, ArchiveV2MessageProjector, BorrowedArchiveV2Instruction,
    MAX_MESSAGE_ACCOUNTS, MessageProjectionError, MessageProjectionResult,
    ProjectedArchiveV2Message, ProjectedArchiveV2MessageAccountSummary, SignerKeys,
    WireProfileAuditOutcome,
};
pub use metadata_wire_profile::{
    ArchiveV2MetadataProfileAdmission, ArchiveV2MetadataSchemaClassification,
    ArchiveV2MetadataSchemaClassifier, ArchiveV2MetadataSchemaCounts, ArchiveV2MetadataWireProfile,
    AuditedCurrentMetadataMarkerPublication, CURRENT_TYPED_ERRORS_MARKER_BYTES,
    CURRENT_TYPED_ERRORS_MARKER_FILE, CURRENT_TYPED_ERRORS_MARKER_SHA256,
    CURRENT_TYPED_ERRORS_MARKER_SIZE, FullGenerationMetadataWireProfileAudit,
    audit_and_admit_full_generation_metadata_wire_profile,
    audit_and_admit_selected_metadata_wire_profile, audit_current_metadata_for_marker_publication,
    audit_full_generation_metadata_wire_profile, classify_archive_v2_metadata_schema_exact,
};
pub use publication_lock::{
    ARCHIVE_V2_PUBLICATION_LOCK_FILE, ArchiveV2PublicationLock, acquire_archive_v2_publication_lock,
};
pub use reader::{
    ArchiveReader, BatchBarrierBlockStats, BlockIterator, BorrowedBlockStream,
    BorrowedDecodedBlock, BorrowedDecodedTxRowIter, CompiledPubkeyFilter, DecodedBlock,
    GenerationBinding, HashVerification, IndeterminateReason, LocatedStorageTransactionRowIter,
    LocatedTransactionRow, MAX_ORDERED_PARALLEL_BLOCKS_PER_BATCH,
    MAX_ORDERED_PARALLEL_COMPRESSED_BUFFERS, MAX_ORDERED_PARALLEL_DECODE_WORKERS,
    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES,
    MAX_ORDERED_PARALLEL_UNCOMPRESSED_BATCH_BYTES, MetadataState, OpenOptions,
    OrderedParallelBlockConfig, OrderedParallelBlockStats, ProfiledGenerationBinding, ScanIterator,
    ScannedBlock, ScannedTransaction, SignatureReference, TransactionMatch, TransactionRowOrder,
    ValidatedGeneration, validate_generation_structure,
    validate_generation_structure_with_metadata_admission,
};
pub use registry_index_admission::{
    LocalRegistryIndexValidation, validate_manifest_bound_pinned_local_registry_index,
    validate_pinned_local_registry_index_mapping,
};
pub use selective_metadata::{
    ArchiveV2LoadedAddressSide, ArchiveV2MetadataProjectionLimits,
    BorrowedArchiveV2InnerInstruction, BorrowedArchiveV2InnerTokenInstruction,
    BorrowedArchiveV2LogDataChunks, BorrowedArchiveV2LogEvent, BorrowedArchiveV2LogEventKind,
    BorrowedArchiveV2LogTables, BorrowedArchiveV2ProgramLog, BorrowedArchiveV2StructuredLog,
    BorrowedArchiveV2TokenBalance, LogPayloadValidation, ProjectedArchiveV2CompactLogsSummary,
    ProjectedArchiveV2MetadataPrefix, ProjectedArchiveV2TokenMetadata,
    ProjectedArchiveV2TokenMetadataSummary, TokenBalanceSide, project_archive_v2_metadata_error,
    project_archive_v2_metadata_outcome, project_archive_v2_metadata_prefix,
    project_archive_v2_token_metadata_exact, project_archive_v2_token_metadata_exact_ordered,
    validate_archive_v2_current_metadata_exact, validate_archive_v2_metadata_exact,
    visit_archive_v2_compact_logs_exact,
    visit_archive_v2_compact_logs_exact_with_selected_error_schema,
    visit_archive_v2_token_metadata_exact_ordered,
    visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema,
};
pub use source::{
    LocalRangeSource, OverlayRangeSource, PinnedLocalDirectoryIdentity, PinnedLocalEntryKind,
    PinnedLocalInventoryEntry, PinnedLocalRangeSource, RangeSource, SourceResult,
};
pub use wire_profile::{
    ArchiveV2WireProfile, POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
    POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256,
    POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE, PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
    PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SHA256, PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_SIZE,
    wire_profile_marker, wire_profile_marker_bytes,
};
pub use wire_profile_audit::{
    FullGenerationWireProfileAudit, UnprovenWireProfileDecision, audit_full_generation_wire_profile,
};

#[cfg(test)]
mod test_allocations {
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        cell::Cell,
    };

    pub struct CountingAllocator;

    thread_local! {
        static TRACKING: Cell<bool> = const { Cell::new(false) };
        static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    }

    fn record_allocation() {
        TRACKING.with(|tracking| {
            if tracking.get() {
                ALLOCATIONS.with(|count| count.set(count.get().saturating_add(1)));
            }
        });
    }

    // SAFETY: Every operation delegates to the system allocator with the same
    // pointer and layout contract. The thread-local counter is diagnostic only.
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record_allocation();
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record_allocation();
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) }
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record_allocation();
            unsafe { System.realloc(pointer, layout, new_size) }
        }
    }

    #[global_allocator]
    static TEST_ALLOCATOR: CountingAllocator = CountingAllocator;

    pub fn count_current_thread_allocations<T>(operation: impl FnOnce() -> T) -> (T, usize) {
        TRACKING.with(|tracking| assert!(!tracking.replace(true)));
        ALLOCATIONS.with(|count| count.set(0));
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                TRACKING.with(|tracking| tracking.set(false));
            }
        }
        let reset = Reset;
        let value = operation();
        let allocations = ALLOCATIONS.with(Cell::get);
        drop(reset);
        (value, allocations)
    }
}
