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
    ProjectedArchiveV2Message, SignerKeys, WireProfileAuditOutcome,
};
pub use publication_lock::{
    ARCHIVE_V2_PUBLICATION_LOCK_FILE, ArchiveV2PublicationLock, acquire_archive_v2_publication_lock,
};
pub use reader::{
    ArchiveReader, BlockIterator, BorrowedBlockStream, BorrowedDecodedBlock,
    BorrowedDecodedTxRowIter, CompiledPubkeyFilter, DecodedBlock, GenerationBinding,
    HashVerification, IndeterminateReason, LocatedTransactionRow, MetadataState, OpenOptions,
    ScanIterator, ScannedBlock, ScannedTransaction, SignatureReference, TransactionMatch,
    TransactionRowOrder, ValidatedGeneration, validate_generation_structure,
};
pub use registry_index_admission::{
    LocalRegistryIndexValidation, validate_manifest_bound_pinned_local_registry_index,
    validate_pinned_local_registry_index_mapping,
};
pub use selective_metadata::{
    ArchiveV2MetadataProjectionLimits, BorrowedArchiveV2InnerInstruction,
    ProjectedArchiveV2MetadataPrefix, project_archive_v2_metadata_error,
    project_archive_v2_metadata_outcome, project_archive_v2_metadata_prefix,
    validate_archive_v2_metadata_exact,
};
pub use source::{
    LocalRangeSource, OverlayRangeSource, PinnedLocalRangeSource, RangeSource, SourceResult,
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
