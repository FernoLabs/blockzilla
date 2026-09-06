//! Source-neutral instruction projection for one Indexer V3 candidate.
//!
//! The adapter uses the frozen standalone V3 reader and its contiguous
//! semantic scanner. The V3 ledger files bind to each other through their
//! internal headers. The retained Compact V2 sidecars do not have a digest
//! binding to that candidate, so this adapter never claims publication
//! verification.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    num::{NonZeroU32, NonZeroUsize},
    ops::Range,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

use blockzilla_archive_v2::{
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_ERROR,
    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
};
use blockzilla_compact::{CompactTokenBalance, OwnedCompactRecentBlockhash};
use blockzilla_compact_v2_reader::{
    BLOCKHASH_RECORD_LEN, BlockhashResolver, BlockhashResolverError, CompactV2ExecutionStatus,
    CompactV2MessageProjectionError, CompactV2MessageProjector, CompactV2MetadataProjectionError,
    CompactV2MetadataProjectionLimits, CompactV2MetadataProjector, MAX_BLOCKHASH_REGISTRY_BYTES,
    MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS, MAX_VOTE_HASH_REGISTRY_BYTES,
    PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN, PreviousBlockhashTail, PreviousBlockhashTailSchema,
    ProjectedCompactV2Message, ProjectedCompactV2MessageVersion, ResolvedAddressTableLookup,
    SignedInstructionCandidates, SignedMessageCandidates, SignedMessageError, SignedMessageVersion,
    VOTE_HASH_RECORD_LEN, VoteHashRegistry, VoteHashResolver, parse_previous_blockhash_tail,
    select_signed_message_candidate_ed25519,
};
use blockzilla_model::{
    ArchiveFormat, ArchiveInstructionSource, BlockHeader, BlockSink, CanonicalBlock,
    CanonicalTransaction, CoverageReason, CpiCoverage, Error as QueryError, ExecutionStatus,
    InstructionCoordinate, InstructionCoverage, InstructionDataCoverage,
    InstructionDataRequirement, OrderedBlockPublisher, RecordedTokenBalance, ResolvedInstruction,
    ScanIoReceipt, ScanRange, ScanReceipt, ScanRequest, SourceIdentity, SourceVerification,
    TokenBalanceCoverage, TokenBalanceRequirement, TokenBalanceSide, TransactionHeader,
    validate_request,
};
use blockzilla_primitives::CompactPubkey;
use blockzilla_source::RangeSource;
use blockzilla_source_local::PinnedLocalRangeSource;
use thiserror::Error;

use crate::indexer_v3_postings::AdaptiveV3Reader;
use crate::indexer_v3_wire::{
    BlockRow, INDEX_FILE, Object, Reader, ReusableSemanticScanWorkspace, SemanticPlaneSelection,
    SemanticTransaction, StandaloneFormat,
};

const REGISTRY_KEY_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 64;
const REGISTRY_KEYS_PER_CHUNK: usize = 2_048;
const REGISTRY_CACHE_CHUNKS: usize = 8;
const MAX_REGISTRY_PREFETCH_READ_BYTES: usize = 32 << 20;
const DENSE_REGISTRY_MIN_CANDIDATE_TRANSACTIONS: u64 = 1_000_000;
const PREVIOUS_BLOCKHASH_RECORDS: usize = 300;
const MAX_SIGNATURE_BYTES_PER_BLOCK: usize = 256 * 1024 * 1024;
/// Keep one sequential signature request within the public range gateway cap.
const MAX_SIGNATURE_BATCH_BYTES: usize = 32 << 20;
/// Maximum projected blocks retained by one parallel V3 job.
pub const INDEXER_V3_PARALLEL_BLOCKS_PER_JOB: usize = 4;
const INDEXER_V3_PARALLEL_JOB_WINDOW_PER_WORKER: usize = 2;
/// Global projected-block window factor. A scan retains at most the requested
/// worker count multiplied by this value.
pub const INDEXER_V3_PARALLEL_BUFFERED_BLOCKS_PER_WORKER: usize =
    INDEXER_V3_PARALLEL_BLOCKS_PER_JOB * INDEXER_V3_PARALLEL_JOB_WINDOW_PER_WORKER;
/// Maximum declared decoded payload bytes across all assigned, unconsumed jobs.
pub const INDEXER_V3_PARALLEL_DECLARED_DECODED_BYTE_LIMIT: u64 = 256 << 20;
/// Maximum declared transactions across all assigned, unconsumed jobs.
pub const INDEXER_V3_PARALLEL_TRANSACTION_LIMIT: u64 = 100_000;
/// Maximum semantic buffer capacity retained by one worker between jobs.
pub const INDEXER_V3_PARALLEL_RETAINED_WORKSPACE_LIMIT: usize = 64 << 20;
/// Maximum retained allocation for one recycled outer transaction vector.
pub const INDEXER_V3_PARALLEL_RETAINED_TRANSACTION_BUFFER_LIMIT: usize = 4 << 20;
/// Maximum projection-scratch capacity retained by one worker between jobs.
pub const INDEXER_V3_PARALLEL_RETAINED_PROJECTION_SCRATCH_LIMIT: usize = 8 << 20;
/// Hard process-local worker bound for public parallel V3 scan APIs.
pub const MAX_INDEXER_V3_PARALLEL_WORKERS: usize = 64;

/// Maximum retained public-key payload bytes in the V3 registry chunk cache.
///
/// This value excludes map, vector, allocator, and LRU overhead.
pub const INDEXER_V3_QUERY_REGISTRY_RETAINED_KEY_BYTES: usize =
    REGISTRY_KEYS_PER_CHUNK * REGISTRY_KEY_BYTES * REGISTRY_CACHE_CHUNKS;

/// Policy for automatic full-registry prefetch during a selective V3 scan.
///
/// A nonzero limit permits prefetch only when the selected blocks contain at
/// least one million transactions and at least half of the requested
/// transactions. The complete retained registry must also fit within the
/// supplied byte limit. A zero limit keeps the bounded eight-chunk LRU path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexerV3RegistryReadPolicy {
    max_full_registry_bytes: u64,
    min_candidate_transactions: u64,
}

impl IndexerV3RegistryReadPolicy {
    /// Disable full-registry prefetch.
    pub const fn sparse_only() -> Self {
        Self {
            max_full_registry_bytes: 0,
            min_candidate_transactions: DENSE_REGISTRY_MIN_CANDIDATE_TRANSACTIONS,
        }
    }

    /// Permit automatic prefetch up to the supplied complete-registry size.
    pub const fn with_full_registry_limit(max_full_registry_bytes: u64) -> Self {
        Self {
            max_full_registry_bytes,
            min_candidate_transactions: DENSE_REGISTRY_MIN_CANDIDATE_TRANSACTIONS,
        }
    }

    pub const fn max_full_registry_bytes(self) -> u64 {
        self.max_full_registry_bytes
    }

    #[cfg(test)]
    const fn for_test(max_full_registry_bytes: u64, min_candidate_transactions: u64) -> Self {
        Self {
            max_full_registry_bytes,
            min_candidate_transactions,
        }
    }
}

/// Registry storage used by one selective scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum IndexerV3RegistryReadMode {
    #[default]
    Unused,
    SparseChunkCache,
    FullRegistry,
}

/// Per-scan registry lookup and retained public-key payload counts.
///
/// `resident_payload_bytes` excludes allocator capacity, `HashMap` storage,
/// and the bounded LRU bookkeeping.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct IndexerV3RegistryReadReceipt {
    pub mode: IndexerV3RegistryReadMode,
    pub prefetch_read_calls: u64,
    pub prefetch_read_bytes: u64,
    pub resolutions: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub resident_payload_bytes: u64,
}

/// Return the required internally bound V3 ledger object names.
///
/// The order is stable: the block index is first, followed by the 11 plane
/// files in their wire object-ID order.
pub fn indexer_v3_required_ledger_objects() -> impl Iterator<Item = &'static str> + Clone {
    std::iter::once(INDEX_FILE).chain(Object::ALL.into_iter().map(Object::file_name))
}

/// Retained sidecars required for source-neutral V3 projection.
pub const INDEXER_V3_REQUIRED_RETAINED_SIDECARS: [&str; 1] = [ARCHIVE_V2_PUBKEY_REGISTRY_FILE];

/// Retained sidecars that are optional until exact message proof needs them.
pub const INDEXER_V3_OPTIONAL_RETAINED_SIDECARS: [&str; 4] = [
    ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
];

/// Scope declared by the frozen Indexer V3 header.
///
/// `FullSelection` means that the candidate writer did not use its benchmark
/// prefix option. It is not a publication or complete-epoch assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexerV3SourceScope {
    SelectedPrefix,
    FullSelection,
}

/// Exact universe, selection, projection, and source-I/O counts for one
/// sparse Indexer V3 block scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct IndexerV3SelectiveScanReceipt {
    pub requested_blocks: u64,
    pub requested_transactions: u64,
    pub candidate_blocks: u64,
    pub candidate_transactions: u64,
    pub skipped_blocks: u64,
    pub skipped_transactions: u64,
    pub scan_receipt: ScanReceipt,
    pub source_io: ScanIoReceipt,
    pub registry: IndexerV3RegistryReadReceipt,
    /// Present only for the explicit parallel scan API.
    pub parallel: Option<IndexerV3ParallelScanStats>,
}

/// Measured worker, job, and ordered-result-window data for one V3 scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct IndexerV3ParallelScanStats {
    pub requested_workers: usize,
    pub effective_workers: usize,
    /// Maximum number of worker jobs active at the same time, including I/O.
    pub max_active_workers: usize,
    pub jobs: u64,
    pub projected_blocks: u64,
    pub blocks_per_job_limit: usize,
    pub job_window_limit: usize,
    pub max_in_flight_jobs: usize,
    /// Results retained by the ordered coordinator, excluding the channel.
    pub max_coordinator_pending_results: usize,
    /// Result messages waiting in the worker-to-coordinator channel.
    pub max_result_channel_backlog: usize,
    /// Projected blocks retained by the coordinator, excluding the channel.
    pub max_coordinator_pending_projected_blocks: usize,
    pub declared_decoded_byte_limit: u64,
    pub transaction_limit: u64,
    pub max_in_flight_declared_decoded_bytes: u64,
    pub max_in_flight_transactions: u64,
    /// Exact owned canonical payload capacity for the largest block.
    pub max_owned_payload_block_bytes: u64,
    /// Exact owned canonical payload capacity across executing and queued jobs.
    pub max_in_flight_owned_payload_bytes: u64,
    pub global_projected_block_bound: usize,
}

/// Common ordered receipt plus V3-specific parallel execution evidence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct IndexerV3ParallelScanReceipt {
    pub scan: ScanReceipt,
    pub registry: IndexerV3RegistryReadReceipt,
    pub parallel: IndexerV3ParallelScanStats,
}

#[derive(Debug, Error)]
pub enum IndexerV3InstructionSourceError {
    #[error("Indexer V3 reader error: {0:#}")]
    Reader(#[source] anyhow::Error),

    #[error("Indexer V3 range source error: {0}")]
    RangeSource(#[from] blockzilla_source::SourceError),

    #[error("Indexer V3 message projection error: {0}")]
    Message(#[from] CompactV2MessageProjectionError),

    #[error("Indexer V3 metadata projection error: {0}")]
    Metadata(#[from] CompactV2MetadataProjectionError),

    #[error("Indexer V3 signed-message error: {0}")]
    SignedMessage(#[from] SignedMessageError),

    #[error("Indexer V3 blockhash error: {0}")]
    Blockhash(#[from] BlockhashResolverError),

    #[error("Indexer V3 sidecar {object} is required for {purpose}")]
    MissingSidecar {
        object: &'static str,
        purpose: &'static str,
    },

    #[error("cannot reserve memory for {context}: {source}")]
    Allocation {
        context: &'static str,
        #[source]
        source: std::collections::TryReserveError,
    },

    #[error("Indexer V3 exact sidecar {object} failed to load: {source}")]
    ExactSidecarLoad {
        object: &'static str,
        #[source]
        source: Arc<IndexerV3InstructionSourceError>,
    },

    #[error("invalid Indexer V3 instruction source: {0}")]
    Invalid(String),
}

pub type IndexerV3InstructionSourceResult<T> =
    std::result::Result<T, IndexerV3InstructionSourceError>;

/// A sequential instruction source over one internally bound Indexer V3 candidate.
///
/// `registry.bin` is required. `signatures.bin` is optional, but selected
/// ambiguous instruction data cannot be published without its signature proof.
/// Blockhash, previous-blockhash, and vote-hash sidecars are loaded only when
/// exact selected message reconstruction needs them. Retained sidecars have no
/// digest binding to the V3 ledger candidate.
pub struct IndexerV3InstructionSource {
    reader: Arc<Reader>,
    identity: SourceIdentity,
    scope: IndexerV3SourceScope,
    meter: Arc<CountingRangeSource>,
    context: ExactContext,
}

/// Reusable temporary storage for one ordered or selective projection scan.
///
/// Both vectors keep their capacity between transactions. Their contents are
/// temporary and are never exposed through the owned canonical result.
#[derive(Default)]
struct TransactionProjectionScratch {
    account_keys: Vec<[u8; 32]>,
    selected_references: Vec<CompactPubkey>,
    token_balances: blockzilla_compact_v2_reader::ProjectedCompactV2TokenBalances,
}

impl TransactionProjectionScratch {
    fn retained_capacity_bytes(&self) -> Option<usize> {
        self.account_keys
            .capacity()
            .checked_mul(std::mem::size_of::<[u8; 32]>())?
            .checked_add(
                self.selected_references
                    .capacity()
                    .checked_mul(std::mem::size_of::<CompactPubkey>())?,
            )?
            .checked_add(
                (self.token_balances.pre.capacity() + self.token_balances.post.capacity())
                    .checked_mul(std::mem::size_of::<CompactTokenBalance>())?,
            )
    }

    fn shed_buffers_above(&mut self, limit: usize) -> bool {
        if self
            .retained_capacity_bytes()
            .is_some_and(|bytes| bytes <= limit)
        {
            return false;
        }
        self.account_keys = Vec::new();
        self.selected_references = Vec::new();
        self.token_balances = Default::default();
        true
    }
}

#[derive(Debug)]
struct ParallelScanJob {
    id: usize,
    blocks: ParallelScanJobBlocks,
    resources: ParallelScanJobResources,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ParallelScanJobResources {
    blocks: usize,
    declared_decoded_bytes: u64,
    transactions: u64,
}

impl ParallelScanJobResources {
    fn checked_add(self, other: Self) -> blockzilla_model::Result<Self> {
        Ok(Self {
            blocks: self.blocks.checked_add(other.blocks).ok_or_else(|| {
                QueryError::InvalidStream("parallel V3 block resource count overflow".into())
            })?,
            declared_decoded_bytes: self
                .declared_decoded_bytes
                .checked_add(other.declared_decoded_bytes)
                .ok_or_else(|| {
                    QueryError::InvalidStream(
                        "parallel V3 declared decoded-byte resource overflow".into(),
                    )
                })?,
            transactions: self
                .transactions
                .checked_add(other.transactions)
                .ok_or_else(|| {
                    QueryError::InvalidStream(
                        "parallel V3 transaction resource count overflow".into(),
                    )
                })?,
        })
    }

    fn checked_sub(self, other: Self) -> blockzilla_model::Result<Self> {
        Ok(Self {
            blocks: self.blocks.checked_sub(other.blocks).ok_or_else(|| {
                QueryError::InvalidStream("parallel V3 block resource count underflow".into())
            })?,
            declared_decoded_bytes: self
                .declared_decoded_bytes
                .checked_sub(other.declared_decoded_bytes)
                .ok_or_else(|| {
                    QueryError::InvalidStream(
                        "parallel V3 declared decoded-byte resource underflow".into(),
                    )
                })?,
            transactions: self
                .transactions
                .checked_sub(other.transactions)
                .ok_or_else(|| {
                    QueryError::InvalidStream(
                        "parallel V3 transaction resource count underflow".into(),
                    )
                })?,
        })
    }

    const fn is_empty(self) -> bool {
        self.blocks == 0 && self.declared_decoded_bytes == 0 && self.transactions == 0
    }

    const fn fits_global_limits(self) -> bool {
        self.declared_decoded_bytes <= INDEXER_V3_PARALLEL_DECLARED_DECODED_BYTE_LIMIT
            && self.transactions <= INDEXER_V3_PARALLEL_TRANSACTION_LIMIT
    }
}

fn admitted_parallel_resources(
    current: ParallelScanJobResources,
    next: ParallelScanJobResources,
) -> blockzilla_model::Result<Option<ParallelScanJobResources>> {
    let combined = current.checked_add(next)?;
    Ok((current.is_empty() || combined.fits_global_limits()).then_some(combined))
}

fn recycle_parallel_transaction_buffer(
    pool: &mut Vec<Vec<CanonicalTransaction>>,
    mut buffer: Vec<CanonicalTransaction>,
) {
    // Canonical transactions and their inner allocations are created by one
    // decode worker. Clear them only after the ordered coordinator returns the
    // buffer to that worker. This keeps allocator ownership local instead of
    // releasing hundreds of millions of small allocations on the coordinator
    // thread during a full-epoch scan.
    buffer.clear();
    let retained_bytes = buffer
        .capacity()
        .checked_mul(std::mem::size_of::<CanonicalTransaction>());
    if retained_bytes
        .is_some_and(|bytes| bytes <= INDEXER_V3_PARALLEL_RETAINED_TRANSACTION_BUFFER_LIMIT)
        && pool.len() < INDEXER_V3_PARALLEL_BLOCKS_PER_JOB
    {
        pool.push(buffer);
    }
}

#[derive(Debug, Clone, Copy)]
struct ParallelAssignedDescriptor {
    worker: usize,
    resources: ParallelScanJobResources,
    block_ordinals: [u32; INDEXER_V3_PARALLEL_BLOCKS_PER_JOB],
    slots: [u64; INDEXER_V3_PARALLEL_BLOCKS_PER_JOB],
    transaction_counts: [u32; INDEXER_V3_PARALLEL_BLOCKS_PER_JOB],
}

#[derive(Debug)]
enum ParallelScanJobBlocks {
    Ordered(Range<usize>),
    Selected {
        all: Arc<[usize]>,
        indexes: Range<usize>,
    },
}

impl ParallelScanJobBlocks {
    fn len(&self) -> usize {
        match self {
            Self::Ordered(range) => range.len(),
            Self::Selected { indexes, .. } => indexes.len(),
        }
    }

    fn get(&self, index: usize) -> Option<usize> {
        if index >= self.len() {
            return None;
        }
        Some(match self {
            Self::Ordered(range) => range.start + index,
            Self::Selected { all, indexes } => all[indexes.start + index],
        })
    }

    fn iter(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        (0..self.len()).map(|index| self.get(index).expect("bounded selection index"))
    }
}

enum ParallelScanJobPlan {
    Ordered {
        reader: Arc<Reader>,
        selection: SemanticPlaneSelection,
        include_signatures: bool,
        next_block: usize,
        end_block: usize,
        next_id: usize,
    },
    Selected {
        reader: Arc<Reader>,
        selection: SemanticPlaneSelection,
        include_signatures: bool,
        blocks: Arc<[usize]>,
        next_index: usize,
        next_id: usize,
    },
}

struct ParallelWorkerJob {
    job: ParallelScanJob,
}

struct ParallelWorkerRecycle {
    blocks: Vec<CanonicalBlock>,
    unused_transaction_buffers: Vec<Vec<CanonicalTransaction>>,
    owned_payload: ParallelOwnedPayloadLease,
}

enum ParallelWorkerCommand {
    Decode(ParallelWorkerJob),
    Recycle(ParallelWorkerRecycle),
    Shutdown,
}

struct ParallelScanJobOutput {
    worker: usize,
    blocks: Vec<CanonicalBlock>,
    unused_transaction_buffers: Vec<Vec<CanonicalTransaction>>,
    decoded_bytes: u64,
    registry: IndexerV3RegistryReadReceipt,
    owned_payload: ParallelOwnedPayloadLease,
}

impl ParallelScanJobOutput {
    fn into_worker_recycle(self) -> (usize, ParallelWorkerRecycle) {
        let Self {
            worker,
            blocks,
            unused_transaction_buffers,
            owned_payload,
            ..
        } = self;
        (
            worker,
            ParallelWorkerRecycle {
                blocks,
                unused_transaction_buffers,
                owned_payload,
            },
        )
    }
}

struct PendingParallelResult {
    resources: ParallelScanJobResources,
    result: blockzilla_model::Result<ParallelScanJobOutput>,
}

#[derive(Debug, Default)]
struct ParallelScanTotals {
    decoded_bytes: u64,
    registry: Option<IndexerV3RegistryReadReceipt>,
    parallel: IndexerV3ParallelScanStats,
}

#[derive(Default)]
struct ParallelWorkerActivity {
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl ParallelWorkerActivity {
    fn enter(&self) -> ParallelWorkerActivityGuard<'_> {
        let current = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(current, Ordering::Relaxed);
        ParallelWorkerActivityGuard { activity: self }
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
}

struct ParallelWorkerActivityGuard<'a> {
    activity: &'a ParallelWorkerActivity,
}

#[derive(Default)]
struct ParallelResultChannelActivity {
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl ParallelResultChannelActivity {
    fn note_send(&self) {
        let current = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(current, Ordering::Relaxed);
    }

    fn cancel_send(&self) {
        self.current.fetch_sub(1, Ordering::Relaxed);
    }

    fn note_receive(&self) {
        self.current.fetch_sub(1, Ordering::Relaxed);
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
struct ParallelOwnedPayloadTracker {
    current: AtomicU64,
    peak: AtomicU64,
    max_block: AtomicU64,
}

impl ParallelOwnedPayloadTracker {
    fn add(&self, bytes: u64) -> blockzilla_model::Result<()> {
        let current = self
            .current
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(bytes)
            })
            .map_err(|_| {
                QueryError::InvalidStream("parallel V3 owned-payload byte count overflow".into())
            })?
            + bytes;
        self.peak.fetch_max(current, Ordering::Relaxed);
        Ok(())
    }

    fn release(&self, bytes: u64) {
        let previous = self.current.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes);
    }
}

struct ParallelOwnedPayloadGuard {
    tracker: Arc<ParallelOwnedPayloadTracker>,
    bytes: u64,
    active: bool,
}

impl ParallelOwnedPayloadGuard {
    fn new(tracker: Arc<ParallelOwnedPayloadTracker>) -> Self {
        Self {
            tracker,
            bytes: 0,
            active: true,
        }
    }

    fn add_block(&mut self, bytes: u64) -> blockzilla_model::Result<()> {
        let total = self.bytes.checked_add(bytes).ok_or_else(|| {
            QueryError::InvalidStream("parallel V3 job owned-payload bytes overflow".into())
        })?;
        self.tracker.add(bytes)?;
        self.tracker.max_block.fetch_max(bytes, Ordering::Relaxed);
        self.bytes = total;
        Ok(())
    }

    fn finish(mut self) -> ParallelOwnedPayloadLease {
        self.active = false;
        ParallelOwnedPayloadLease {
            tracker: Arc::clone(&self.tracker),
            bytes: self.bytes,
            owner_thread: thread::current().id(),
        }
    }
}

impl Drop for ParallelOwnedPayloadGuard {
    fn drop(&mut self) {
        if self.active {
            self.tracker.release(self.bytes);
        }
    }
}

struct ParallelOwnedPayloadLease {
    tracker: Arc<ParallelOwnedPayloadTracker>,
    bytes: u64,
    owner_thread: thread::ThreadId,
}

impl Drop for ParallelOwnedPayloadLease {
    fn drop(&mut self) {
        debug_assert_eq!(
            thread::current().id(),
            self.owner_thread,
            "parallel V3 owned payload was released outside its decode worker"
        );
        self.tracker.release(self.bytes);
    }
}

fn reclaim_parallel_worker_output(
    pool: &mut Vec<Vec<CanonicalTransaction>>,
    recycle: ParallelWorkerRecycle,
    context: &mut blockzilla_model::projection_pool::ProjectionPool,
) {
    let ParallelWorkerRecycle {
        blocks,
        unused_transaction_buffers,
        owned_payload,
    } = recycle;
    for buffer in unused_transaction_buffers {
        recycle_parallel_transaction_buffer(pool, buffer);
    }
    for mut block in blocks {
        context.recycle_block(&mut block);
        recycle_parallel_transaction_buffer(pool, block.transactions);
    }
    // Release the measured owned-payload lease only after every inner Vec was
    // cleared or shed on its owner worker.
    drop(owned_payload);
}

fn return_parallel_output_to_worker(
    job_senders: &[mpsc::SyncSender<ParallelWorkerCommand>],
    output: ParallelScanJobOutput,
) -> blockzilla_model::Result<()> {
    let (worker, recycle) = output.into_worker_recycle();
    let Some(sender) = job_senders.get(worker) else {
        // No live owner exists for this invalid worker identity. Do not free
        // its worker-owned payload on the coordinator thread.
        std::mem::forget(recycle);
        return Err(QueryError::InvalidStream(format!(
            "parallel V3 output names invalid recycle worker {worker}"
        )));
    };
    if let Err(error) = sender.send(ParallelWorkerCommand::Recycle(recycle)) {
        // A disconnected owner cannot reclaim these allocations safely. The
        // scan is already invalid; retain them until process exit instead of
        // moving their destruction to the coordinator thread.
        std::mem::forget(error.0);
        return Err(QueryError::InvalidStream(format!(
            "parallel V3 worker {worker} stopped before output reclamation"
        )));
    }
    Ok(())
}

fn return_successful_parallel_result_to_worker(
    job_senders: &[mpsc::SyncSender<ParallelWorkerCommand>],
    result: blockzilla_model::Result<ParallelScanJobOutput>,
) -> blockzilla_model::Result<()> {
    match result {
        Ok(output) => return_parallel_output_to_worker(job_senders, output),
        Err(_) => Ok(()),
    }
}

fn canonical_block_owned_payload_bytes(
    transactions: &[CanonicalTransaction],
    transaction_capacity: usize,
) -> blockzilla_model::Result<u64> {
    let mut bytes = parallel_capacity_bytes::<CanonicalTransaction>(transaction_capacity)?;
    for transaction in transactions {
        parallel_checked_add_payload(
            &mut bytes,
            parallel_capacity_bytes::<[u8; REGISTRY_KEY_BYTES]>(
                transaction.required_signers.capacity(),
            )?,
        )?;
        parallel_checked_add_payload(
            &mut bytes,
            parallel_capacity_bytes::<ResolvedInstruction>(transaction.instructions.capacity())?,
        )?;
        for instruction in &transaction.instructions {
            parallel_checked_add_payload(
                &mut bytes,
                parallel_capacity_bytes::<[u8; REGISTRY_KEY_BYTES]>(
                    instruction.accounts.capacity(),
                )?,
            )?;
            parallel_checked_add_payload(
                &mut bytes,
                parallel_capacity_bytes::<u8>(instruction.data.capacity())?,
            )?;
        }
        parallel_checked_add_payload(
            &mut bytes,
            parallel_capacity_bytes::<RecordedTokenBalance>(transaction.token_balances.capacity())?,
        )?;
    }
    Ok(bytes)
}

fn parallel_checked_add_payload(total: &mut u64, value: u64) -> blockzilla_model::Result<()> {
    *total = total.checked_add(value).ok_or_else(|| {
        QueryError::InvalidStream("parallel V3 owned-payload bytes overflow".into())
    })?;
    Ok(())
}

fn parallel_capacity_bytes<T>(capacity: usize) -> blockzilla_model::Result<u64> {
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| {
            QueryError::InvalidStream("parallel V3 owned-payload capacity overflow".into())
        })?;
    u64::try_from(bytes).map_err(|_| {
        QueryError::InvalidStream("parallel V3 owned-payload capacity exceeds u64".into())
    })
}

impl Drop for ParallelWorkerActivityGuard<'_> {
    fn drop(&mut self) {
        self.activity.current.fetch_sub(1, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for IndexerV3InstructionSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexerV3InstructionSource")
            .field("identity", &self.identity)
            .field("scope", &self.scope)
            .field("registry_entries", &self.context.registry_entries)
            .field(
                "signatures_available",
                &self.context.sidecars.signatures_size.is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl IndexerV3InstructionSource {
    /// Open a local candidate through one anchored, immutable file source.
    ///
    /// `candidate_binding` is an opaque operator label for this exact archive
    /// build. It is not a content digest. The adapter records it for durable
    /// resume checks, but does not verify it.
    pub fn open_local(
        root: impl AsRef<Path>,
        first_slot: u64,
        candidate_binding: impl Into<String>,
    ) -> IndexerV3InstructionSourceResult<Self> {
        let root = root.as_ref();
        let allowed = local_source_objects();
        let source = PinnedLocalRangeSource::new_anchored(root, &allowed)?;
        Self::open_with_source(
            Arc::new(source),
            root.display().to_string(),
            first_slot,
            candidate_binding,
        )
    }

    /// Open a strict shared range source, including HTTPS or a cached HTTPS source.
    ///
    /// The same source instance serves the V3 ledger and all retained sidecars.
    /// `candidate_binding` is an opaque archive-build label that must stay
    /// stable across restarts. It is recorded but not verified by this
    /// constructor. Constructor, header, and size work is outside later scan
    /// receipts.
    pub fn open_with_source(
        source: Arc<dyn RangeSource>,
        source_label: impl Into<String>,
        first_slot: u64,
        candidate_binding: impl Into<String>,
    ) -> IndexerV3InstructionSourceResult<Self> {
        Self::open_with_verification(
            source,
            source_label,
            first_slot,
            candidate_binding,
            SourceVerification::InternalBindingOnly,
        )
    }

    /// Open a source whose complete required object set was pinned by the
    /// caller with exact lengths and strong validators.
    ///
    /// This makes the candidate usable with the default verified scan policy.
    /// It does not claim that the epoch has a published manifest.
    pub fn open_object_set_bound_source(
        source: Arc<dyn RangeSource>,
        source_label: impl Into<String>,
        first_slot: u64,
        candidate_binding: impl Into<String>,
    ) -> IndexerV3InstructionSourceResult<Self> {
        Self::open_with_verification(
            source,
            source_label,
            first_slot,
            candidate_binding,
            SourceVerification::ObjectSetBound,
        )
    }

    /// Open an explicit local object source accepted by the operator.
    ///
    /// This constructor does not claim a manifest, object-set, seal, or
    /// publication binding. The caller must anchor and restrict every local
    /// object before it supplies the shared source.
    pub fn open_operator_trusted_source(
        source: Arc<dyn RangeSource>,
        source_label: impl Into<String>,
        first_slot: u64,
        candidate_binding: impl Into<String>,
    ) -> IndexerV3InstructionSourceResult<Self> {
        Self::open_with_verification(
            source,
            source_label,
            first_slot,
            candidate_binding,
            SourceVerification::OperatorTrusted,
        )
    }

    fn open_with_verification(
        source: Arc<dyn RangeSource>,
        source_label: impl Into<String>,
        first_slot: u64,
        candidate_binding: impl Into<String>,
        verification: SourceVerification,
    ) -> IndexerV3InstructionSourceResult<Self> {
        let label = source_label.into();
        if label.is_empty() {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "source label is empty".into(),
            ));
        }
        let candidate_binding = candidate_binding.into();
        if candidate_binding.is_empty() {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "stable candidate binding is empty".into(),
            ));
        }
        let meter = Arc::new(CountingRangeSource::new(source));
        let shared_source: Arc<dyn RangeSource> = meter.clone();
        let reader = Reader::open_with_source(shared_source.clone(), label.clone())
            .map_err(IndexerV3InstructionSourceError::Reader)?;
        if reader.header.format != StandaloneFormat::V3 {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "standalone candidate is not format V3".into(),
            ));
        }

        let block_count = u32::try_from(reader.header.selected_blocks).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 block count exceeds the source-neutral u32 limit".into(),
            )
        })?;
        if u64::from(block_count) > reader.header.slots_per_epoch {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 has {block_count} block rows, above {} slots per epoch",
                reader.header.slots_per_epoch
            )));
        }
        let last_slot = first_slot
            .checked_add(reader.header.slots_per_epoch - 1)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "explicit V3 epoch slot range overflows u64".into(),
                )
            })?;
        validate_rows(&reader, first_slot, last_slot, block_count)?;

        let scope = if reader.header.prefix {
            IndexerV3SourceScope::SelectedPrefix
        } else {
            IndexerV3SourceScope::FullSelection
        };
        let sidecars = SidecarGeometry::inspect(shared_source.as_ref(), &reader, scope)?;
        let registry_entries = registry_entries(sidecars.registry_size)?;
        let message_schema = reader.message_schema();
        let identity = SourceIdentity {
            format: ArchiveFormat::IndexerV3,
            label,
            cluster_id: None,
            epoch: reader.header.epoch,
            first_slot,
            slots_per_epoch: reader.header.slots_per_epoch,
            block_count,
            verification,
            // This is an explicit operator-supplied candidate identity. It is
            // durable resume state, not proof that the V3 headers bind the
            // retained registry or any other candidate content.
            binding: Some(candidate_binding),
        };

        Ok(Self {
            reader: Arc::new(reader),
            identity,
            scope,
            meter,
            context: ExactContext::new(shared_source, registry_entries, sidecars, message_schema),
        })
    }

    pub const fn scope(&self) -> IndexerV3SourceScope {
        self.scope
    }

    pub const fn registry_entries(&self) -> u32 {
        self.context.registry_entries
    }

    /// Reuse the same epoch-bound filter IDs for candidate lookup and scan.
    pub fn filter_key_id(
        &mut self,
        request: &ScanRequest,
        key: &[u8; 32],
    ) -> blockzilla_model::Result<Option<Option<u32>>> {
        self.context
            .prepare_query_keys(request)
            .map_err(source_error)?;
        Ok(self.context.query_keys.registry_id(key))
    }

    /// Release a complete registry image retained by an earlier dense scan.
    pub fn release_full_registry(&mut self) -> bool {
        self.context.full_registry.take().is_some()
    }

    /// Release a complete registry image when it exceeds `max_bytes`.
    ///
    /// This lets a higher-level SDK lower its memory limit immediately. A
    /// resident image that still fits the new limit remains available for a
    /// later dense scan.
    pub fn release_full_registry_above(&mut self, max_bytes: u64) -> bool {
        let must_release = self.context.full_registry.as_ref().is_some_and(|registry| {
            u64::try_from(registry.len()).map_or(true, |bytes| bytes > max_bytes)
        });
        must_release && self.release_full_registry()
    }

    /// Return the exact transaction count bound by the V3 ledger header.
    pub fn selected_transactions(&self) -> u64 {
        self.reader.header.selected_transactions
    }

    pub const fn signatures_available(&self) -> bool {
        self.context.sidecars.signatures_size.is_some()
    }

    /// Return the validated slot for one dense block ordinal.
    pub fn block_slot(&self, ordinal: u32) -> Option<u64> {
        self.reader.block(ordinal as usize).map(|row| row.slot)
    }

    /// Open the adaptive reverse index while reusing this source's validated
    /// standalone ledger and block table.
    ///
    /// Both readers keep the same pinned, metered range source. The adaptive
    /// open validates its own binding and does not read or retain a second full
    /// `BlockRow` table. `AdaptiveV3Reader` still retains its smaller validated
    /// `BlockSpan` layout for reverse-posting decoding.
    pub fn open_adaptive_reader(&self) -> anyhow::Result<AdaptiveV3Reader> {
        AdaptiveV3Reader::open_with_shared_standalone(
            self.context.source.clone(),
            self.identity.label.clone(),
            Arc::clone(&self.reader),
        )
    }

    fn fork_for_parallel_scan(&self) -> Self {
        Self {
            reader: Arc::clone(&self.reader),
            identity: self.identity.clone(),
            scope: self.scope,
            meter: Arc::clone(&self.meter),
            context: self.context.fork_for_parallel_scan(),
        }
    }

    /// Decode and publish only the strictly increasing block candidates that
    /// are inside the normal scan request.
    ///
    /// The returned universe counts cover the full requested range. Candidate
    /// and skipped counts partition that universe exactly. Sparse gaps do not
    /// fetch semantic planes or signature bytes for skipped blocks.
    pub fn scan_selected_blocks(
        &mut self,
        request: &ScanRequest,
        candidate_blocks: &[u32],
        sink: &mut dyn BlockSink,
    ) -> blockzilla_model::Result<IndexerV3SelectiveScanReceipt> {
        self.scan_selected_blocks_with_registry_policy(
            request,
            candidate_blocks,
            IndexerV3RegistryReadPolicy::sparse_only(),
            sink,
        )
    }

    /// Decode selected blocks on bounded worker partitions and publish them in
    /// the same strict order as the single-worker scan.
    ///
    /// The immutable V3 block table and one optional dense registry image are
    /// shared. Each worker owns its projection scratch and bounded sparse
    /// registry cache. The global ordered window retains at most the requested
    /// worker count multiplied by
    /// [`INDEXER_V3_PARALLEL_BUFFERED_BLOCKS_PER_WORKER`] projected blocks.
    /// Exact selected instruction data is supported. When signature proof is
    /// required, each worker reads only the signature windows for its blocks.
    pub fn scan_selected_blocks_parallel_with_registry_policy(
        &mut self,
        request: &ScanRequest,
        candidate_blocks: &[u32],
        registry_policy: IndexerV3RegistryReadPolicy,
        workers: NonZeroUsize,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_model::Result<IndexerV3SelectiveScanReceipt> {
        validate_parallel_worker_count(workers)?;
        let identity = self.identity.clone();
        let (requested_start, requested_end) = validated_requested_bounds(&identity, request)?;
        let selected_blocks = validate_selected_blocks(
            candidate_blocks,
            requested_start,
            requested_end,
            identity.block_count,
        )?;
        let requested_blocks = u64::from(
            requested_end
                .checked_sub(requested_start)
                .ok_or_else(|| QueryError::InvalidRequest("V3 request range decreases".into()))?,
        );
        let requested_transactions = transaction_count_for_blocks(
            &self.reader,
            requested_start as usize..requested_end as usize,
        )
        .map_err(source_error)?;
        let candidate_transactions =
            transaction_count_for_blocks(&self.reader, selected_blocks.iter().copied())
                .map_err(source_error)?;
        let candidate_block_count = u64::try_from(selected_blocks.len()).map_err(|_| {
            QueryError::InvalidRequest("V3 candidate block count exceeds u64".into())
        })?;
        let skipped_blocks = requested_blocks
            .checked_sub(candidate_block_count)
            .ok_or_else(|| {
                QueryError::InvalidRequest("V3 candidate block count exceeds request".into())
            })?;
        let skipped_transactions = requested_transactions
            .checked_sub(candidate_transactions)
            .ok_or_else(|| {
                QueryError::InvalidRequest("V3 candidate transactions exceed request".into())
            })?;

        let source_io_before = self.meter.stats().map_err(source_error)?;
        let registry_stats_before = self.context.registry_stats;
        self.context
            .prepare_registry_for_selective_scan(
                request,
                registry_policy,
                requested_transactions,
                candidate_transactions,
            )
            .map_err(source_error)?;
        let shared_full_registry = self.context.full_registry.is_some();
        let setup_registry = self
            .context
            .registry_receipt_since(registry_stats_before)
            .map_err(source_error)?;

        let jobs = ParallelScanJobPlan::selected(
            Arc::clone(&self.reader),
            selected_blocks,
            semantic_plane_selection(request),
            request_needs_signature_bytes(request),
        );
        let mut scan_receipt = ScanReceipt::default();
        let mut one_block_request = request.clone();
        let totals = self.run_parallel_jobs(request, registry_policy, workers, jobs, |output| {
            for block in &output.blocks {
                one_block_request.range = Some(ScanRange {
                    first_block: block.header.block_ordinal,
                    block_count: NonZeroU32::new(1).expect("one is nonzero"),
                });
                let mut publisher =
                    OrderedBlockPublisher::new(&identity, &one_block_request, sink)?;
                publisher.publish(block)?;
                accumulate_scan_receipt(&mut scan_receipt, publisher.finish()?)?;
            }
            Ok(())
        })?;

        let source_io = self
            .meter
            .stats()
            .map_err(source_error)?
            .difference(source_io_before)
            .map_err(source_error)?;
        let source_io = ScanIoReceipt {
            source_read_calls: Some(source_io.calls),
            source_read_bytes: Some(source_io.bytes),
            decoded_bytes: Some(totals.decoded_bytes),
            cache_read_calls: None,
            cache_read_bytes: None,
        };
        scan_receipt.io = source_io;
        if scan_receipt.blocks != candidate_block_count
            || scan_receipt.transactions != candidate_transactions
        {
            return Err(QueryError::InvalidStream(
                "parallel V3 selective scan receipt differs from selected row geometry".into(),
            ));
        }
        let registry =
            merge_registry_receipts(setup_registry, totals.registry, shared_full_registry)?;

        Ok(IndexerV3SelectiveScanReceipt {
            requested_blocks,
            requested_transactions,
            candidate_blocks: candidate_block_count,
            candidate_transactions,
            skipped_blocks,
            skipped_transactions,
            scan_receipt,
            source_io,
            registry,
            parallel: Some(totals.parallel),
        })
    }

    /// Decode selected blocks with an explicit automatic registry-read policy.
    ///
    /// The prefetch decision uses exact requested and candidate transaction
    /// counts. Any prefetch starts after the scan I/O snapshot, so all of its
    /// calls and bytes are part of both returned I/O receipts.
    pub fn scan_selected_blocks_with_registry_policy(
        &mut self,
        request: &ScanRequest,
        candidate_blocks: &[u32],
        registry_policy: IndexerV3RegistryReadPolicy,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_model::Result<IndexerV3SelectiveScanReceipt> {
        let identity = self.identity.clone();
        let (requested_start, requested_end) = validated_requested_bounds(&identity, request)?;
        let selected_blocks = validate_selected_blocks(
            candidate_blocks,
            requested_start,
            requested_end,
            identity.block_count,
        )?;

        let requested_blocks = u64::from(
            requested_end
                .checked_sub(requested_start)
                .ok_or_else(|| QueryError::InvalidRequest("V3 request range decreases".into()))?,
        );
        let requested_transactions = transaction_count_for_blocks(
            &self.reader,
            requested_start as usize..requested_end as usize,
        )
        .map_err(source_error)?;
        let candidate_transactions =
            transaction_count_for_blocks(&self.reader, selected_blocks.iter().copied())
                .map_err(source_error)?;
        let candidate_block_count = u64::try_from(selected_blocks.len()).map_err(|_| {
            QueryError::InvalidRequest("V3 candidate block count exceeds u64".into())
        })?;
        let skipped_blocks = requested_blocks
            .checked_sub(candidate_block_count)
            .ok_or_else(|| {
                QueryError::InvalidRequest("V3 candidate block count exceeds request".into())
            })?;
        let skipped_transactions = requested_transactions
            .checked_sub(candidate_transactions)
            .ok_or_else(|| {
                QueryError::InvalidRequest("V3 candidate transactions exceed request".into())
            })?;

        let source_io_before = self.meter.stats().map_err(source_error)?;
        let reader = &self.reader;
        let context = &mut self.context;
        let registry_stats_before = context.registry_stats;
        context
            .prepare_registry_for_selective_scan(
                request,
                registry_policy,
                requested_transactions,
                candidate_transactions,
            )
            .map_err(source_error)?;
        let message_schema = reader.message_schema();
        let metadata_schema = reader.metadata_schema();
        let mut decoded_bytes = 0_u64;
        let mut scan_receipt = ScanReceipt::default();
        // The sink consumes each block synchronously. Keep the outer transaction
        // allocation after publish instead of allocating one Vec for every
        // candidate block. The owned transactions inside it are still dropped
        // before the next block is projected.
        let mut transactions = Vec::new();
        let mut projection_scratch = TransactionProjectionScratch::default();
        // A program-filter request can own a nonempty Vec. Clone it once for the
        // complete sparse scan, then update only the one-block range.
        let mut one_block_request = request.clone();
        let mut signature_scan = SelectedBlockSignatureReader::for_selected_blocks(
            reader,
            context.source.clone(),
            context.sidecars.signatures_size.is_some() && request_needs_signature_bytes(request),
            selected_blocks,
        )
        .map_err(source_error)?;

        let selected_len = signature_scan.selected_blocks.len();
        let mut selected_index = 0_usize;
        while selected_index < selected_len {
            let run_start = signature_scan
                .selected_blocks
                .get(selected_index)
                .expect("bounded selection index");
            let mut run_index_end = selected_index
                .checked_add(1)
                .ok_or_else(|| QueryError::InvalidStream("V3 selected index overflow".into()))?;
            while run_index_end < selected_len {
                let previous = signature_scan
                    .selected_blocks
                    .get(run_index_end - 1)
                    .expect("bounded selection index");
                let next = signature_scan
                    .selected_blocks
                    .get(run_index_end)
                    .expect("bounded selection index");
                if previous.checked_add(1) != Some(next) {
                    break;
                }
                run_index_end = run_index_end.checked_add(1).ok_or_else(|| {
                    QueryError::InvalidStream("V3 selected run index overflow".into())
                })?;
            }
            let run_end = signature_scan
                .selected_blocks
                .get(run_index_end - 1)
                .expect("bounded selection index")
                .checked_add(1)
                .ok_or_else(|| QueryError::InvalidStream("V3 selected block overflow".into()))?;
            let mut semantic_scan = reader
                .begin_contiguous_semantic_scan_with_selection(
                    run_start..run_end,
                    semantic_plane_selection(request),
                )
                .map_err(|error| source_error(IndexerV3InstructionSourceError::Reader(error)))?;

            for index in selected_index..run_index_end {
                let ordinal = signature_scan
                    .selected_blocks
                    .get(index)
                    .expect("bounded selection index");
                let row = reader.block(ordinal).ok_or_else(|| {
                    source_error(IndexerV3InstructionSourceError::Invalid(format!(
                        "selected V3 block ordinal {ordinal} is missing after open validation"
                    )))
                })?;
                let block_signatures = signature_scan.read_block(ordinal).map_err(source_error)?;
                let transaction_capacity = usize::try_from(row.tx_count).map_err(|_| {
                    source_error(IndexerV3InstructionSourceError::Invalid(
                        "V3 transaction count exceeds address space".into(),
                    ))
                })?;
                transactions.clear();
                reserve_exact(
                    &mut transactions,
                    transaction_capacity,
                    "canonical sparse V3 block transactions",
                )
                .map_err(source_error)?;
                let mut signature_cursor = 0_u64;

                let stats = semantic_scan
                    .visit_semantic_transactions(ordinal, None, |transaction| {
                        let signatures = transaction_signatures(
                            row,
                            &transaction,
                            block_signatures,
                            &mut signature_cursor,
                        )?;
                        let projected = Self::project_transaction(
                            context,
                            request,
                            message_schema,
                            metadata_schema,
                            transaction,
                            signatures,
                            &mut projection_scratch,
                        )?;
                        transactions.push(projected);
                        Ok(())
                    })
                    .map_err(|error| {
                        source_error(IndexerV3InstructionSourceError::Reader(error))
                    })?;
                if signature_cursor != u64::from(row.signature_count) {
                    return Err(source_error(IndexerV3InstructionSourceError::Invalid(
                        format!(
                            "V3 block {} transactions consume {signature_cursor} of {} signatures",
                            row.block_id, row.signature_count
                        ),
                    )));
                }
                decoded_bytes = decoded_bytes
                    .checked_add(stats.total_decoded_bytes())
                    .ok_or_else(|| {
                        source_error(IndexerV3InstructionSourceError::Invalid(
                            "V3 selective decoded-byte count overflow".into(),
                        ))
                    })?;

                one_block_request.range = Some(ScanRange {
                    first_block: row.block_id,
                    block_count: NonZeroU32::new(1).expect("one is nonzero"),
                });
                let mut publisher =
                    OrderedBlockPublisher::new(&identity, &one_block_request, sink)?;
                let mut block = CanonicalBlock {
                    counts: None,
                    header: BlockHeader {
                        epoch: identity.epoch,
                        block_ordinal: row.block_id,
                        slot: row.slot,
                    },
                    transactions: std::mem::take(&mut transactions),
                };
                publisher.publish(&block)?;
                accumulate_scan_receipt(&mut scan_receipt, publisher.finish()?)?;
                context.output_pool.recycle_block(&mut block);
                transactions = std::mem::take(&mut block.transactions);
            }
            semantic_scan
                .finish()
                .map_err(|error| source_error(IndexerV3InstructionSourceError::Reader(error)))?;
            selected_index = run_index_end;
        }
        signature_scan.finish().map_err(source_error)?;

        let source_io = self
            .meter
            .stats()
            .map_err(source_error)?
            .difference(source_io_before)
            .map_err(source_error)?;
        let source_io = ScanIoReceipt {
            source_read_calls: Some(source_io.calls),
            source_read_bytes: Some(source_io.bytes),
            decoded_bytes: Some(decoded_bytes),
            cache_read_calls: None,
            cache_read_bytes: None,
        };
        let registry = context
            .registry_receipt_since(registry_stats_before)
            .map_err(source_error)?;
        scan_receipt.io = source_io;
        if scan_receipt.blocks != candidate_block_count
            || scan_receipt.transactions != candidate_transactions
        {
            return Err(QueryError::InvalidStream(
                "V3 selective scan receipt differs from selected row geometry".into(),
            ));
        }

        Ok(IndexerV3SelectiveScanReceipt {
            requested_blocks,
            requested_transactions,
            candidate_blocks: candidate_block_count,
            candidate_transactions,
            skipped_blocks,
            skipped_transactions,
            scan_receipt,
            source_io,
            registry,
            parallel: None,
        })
    }

    /// Decode an ordered range with an explicit automatic registry policy.
    ///
    /// A large ordered scan has the same registry access risk as a dense
    /// selective scan. The default trait entry point keeps the sparse policy
    /// for compatibility. High-level SDKs can use this method to enable the
    /// bounded dense policy. Exact selected instruction data is supported.
    /// When signature proof is required, each worker reads only the signature
    /// windows for its blocks.
    pub fn scan_ordered_parallel_with_registry_policy(
        &mut self,
        request: &ScanRequest,
        registry_policy: IndexerV3RegistryReadPolicy,
        workers: NonZeroUsize,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_model::Result<IndexerV3ParallelScanReceipt> {
        validate_parallel_worker_count(workers)?;

        let identity = self.identity.clone();
        let (start, end) = validated_requested_bounds(&identity, request)?;
        let requested_transactions =
            transaction_count_for_blocks(&self.reader, start as usize..end as usize)
                .map_err(source_error)?;
        let source_io_before = self.meter.stats().map_err(source_error)?;
        let registry_stats_before = self.context.registry_stats;
        self.context
            .prepare_registry_for_selective_scan(
                request,
                registry_policy,
                requested_transactions,
                requested_transactions,
            )
            .map_err(source_error)?;
        let shared_full_registry = self.context.full_registry.is_some();
        let setup_registry = self
            .context
            .registry_receipt_since(registry_stats_before)
            .map_err(source_error)?;

        let jobs = ParallelScanJobPlan::ordered(
            Arc::clone(&self.reader),
            start as usize,
            end as usize,
            semantic_plane_selection(request),
            request_needs_signature_bytes(request),
        );
        let mut publisher = OrderedBlockPublisher::new(&identity, request, sink)?;
        let totals = self.run_parallel_jobs(request, registry_policy, workers, jobs, |output| {
            for block in &output.blocks {
                publisher.publish(block)?;
            }
            Ok(())
        })?;
        let source_io = self
            .meter
            .stats()
            .map_err(source_error)?
            .difference(source_io_before)
            .map_err(source_error)?;
        publisher.set_io_receipt(ScanIoReceipt {
            source_read_calls: Some(source_io.calls),
            source_read_bytes: Some(source_io.bytes),
            decoded_bytes: Some(totals.decoded_bytes),
            cache_read_calls: None,
            cache_read_bytes: None,
        });
        let registry =
            merge_registry_receipts(setup_registry, totals.registry, shared_full_registry)?;
        Ok(IndexerV3ParallelScanReceipt {
            scan: publisher.finish()?,
            registry,
            parallel: totals.parallel,
        })
    }

    /// Decode an ordered range with an explicit automatic registry policy.
    ///
    /// A large ordered scan has the same registry access risk as a dense
    /// selective scan. The default trait entry point keeps the sparse policy
    /// for compatibility. High-level SDKs can use this method to enable the
    /// bounded dense policy.
    pub fn scan_ordered_with_registry_policy(
        &mut self,
        request: &ScanRequest,
        registry_policy: IndexerV3RegistryReadPolicy,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_model::Result<ScanReceipt> {
        if request.counts_only {
            return self
                .scan_ordered_parallel_with_registry_policy(
                    request,
                    registry_policy,
                    NonZeroUsize::new(1).unwrap(),
                    sink,
                )
                .map(|receipt| receipt.scan);
        }
        let identity = self.identity.clone();
        let mut publisher = OrderedBlockPublisher::new(&identity, request, sink)?;
        let start = request
            .range
            .map_or(0usize, |range| range.first_block as usize);
        let end = request
            .range
            .map_or(identity.block_count as usize, |range| {
                usize::try_from(
                    range
                        .first_block
                        .checked_add(range.block_count.get())
                        .expect("publisher validated the requested u32 range"),
                )
                .expect("u32 fits the supported address space")
            });

        let reader = &self.reader;
        let context = &mut self.context;
        let source_io_before = self.meter.stats().map_err(source_error)?;
        let requested_transactions =
            transaction_count_for_blocks(reader, start..end).map_err(source_error)?;
        context
            .prepare_registry_for_selective_scan(
                request,
                registry_policy,
                requested_transactions,
                requested_transactions,
            )
            .map_err(source_error)?;
        let message_schema = reader.message_schema();
        let metadata_schema = reader.metadata_schema();
        let mut decoded_bytes = 0u64;
        let mut signature_scan = SelectedBlockSignatureReader::for_contiguous_range(
            reader,
            context.source.clone(),
            context.sidecars.signatures_size.is_some() && request_needs_signature_bytes(request),
            start..end,
        )
        .map_err(source_error)?;
        let mut scan = reader
            .begin_contiguous_semantic_scan_with_selection(
                start..end,
                semantic_plane_selection(request),
            )
            .map_err(|error| source_error(IndexerV3InstructionSourceError::Reader(error)))?;
        // Publish is synchronous, so the outer block Vec can be recycled after
        // every callback. This does not retain any transaction-owned buffers.
        let mut transactions = Vec::new();
        let mut projection_scratch = TransactionProjectionScratch::default();

        for ordinal in start..end {
            let row = reader.block(ordinal).ok_or_else(|| {
                source_error(IndexerV3InstructionSourceError::Invalid(format!(
                    "V3 block ordinal {ordinal} is missing after open validation"
                )))
            })?;
            let block_signatures = signature_scan.read_block(ordinal).map_err(source_error)?;
            let transaction_capacity = usize::try_from(row.tx_count).map_err(|_| {
                source_error(IndexerV3InstructionSourceError::Invalid(
                    "V3 transaction count exceeds address space".into(),
                ))
            })?;
            transactions.clear();
            reserve_exact(
                &mut transactions,
                transaction_capacity,
                "canonical V3 block transactions",
            )
            .map_err(source_error)?;
            let mut signature_cursor = 0u64;

            let stats = scan
                .visit_semantic_transactions(ordinal, None, |transaction| {
                    let signatures = transaction_signatures(
                        row,
                        &transaction,
                        block_signatures,
                        &mut signature_cursor,
                    )?;
                    let projected = Self::project_transaction(
                        context,
                        request,
                        message_schema,
                        metadata_schema,
                        transaction,
                        signatures,
                        &mut projection_scratch,
                    )?;
                    transactions.push(projected);
                    Ok(())
                })
                .map_err(|error| source_error(IndexerV3InstructionSourceError::Reader(error)))?;
            if signature_cursor != u64::from(row.signature_count) {
                return Err(source_error(IndexerV3InstructionSourceError::Invalid(
                    format!(
                        "V3 block {} transactions consume {signature_cursor} of {} signatures",
                        row.block_id, row.signature_count
                    ),
                )));
            }
            decoded_bytes = decoded_bytes
                .checked_add(stats.total_decoded_bytes())
                .ok_or_else(|| {
                    source_error(IndexerV3InstructionSourceError::Invalid(
                        "V3 decoded-byte count overflow".into(),
                    ))
                })?;

            let mut block = CanonicalBlock {
                counts: None,
                header: BlockHeader {
                    epoch: identity.epoch,
                    block_ordinal: row.block_id,
                    slot: row.slot,
                },
                transactions: std::mem::take(&mut transactions),
            };
            publisher.publish(&block)?;
            context.output_pool.recycle_block(&mut block);
            transactions = std::mem::take(&mut block.transactions);
        }
        scan.finish()
            .map_err(|error| source_error(IndexerV3InstructionSourceError::Reader(error)))?;
        signature_scan.finish().map_err(source_error)?;

        let source_io = self
            .meter
            .stats()
            .map_err(source_error)?
            .difference(source_io_before)
            .map_err(source_error)?;
        publisher.set_io_receipt(ScanIoReceipt {
            source_read_calls: Some(source_io.calls),
            source_read_bytes: Some(source_io.bytes),
            decoded_bytes: Some(decoded_bytes),
            cache_read_calls: None,
            cache_read_bytes: None,
        });
        publisher.finish()
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_parallel_job(
        &mut self,
        request: &ScanRequest,
        job: ParallelScanJob,
        mut transaction_buffers: Vec<Vec<CanonicalTransaction>>,
        projection_scratch: &mut TransactionProjectionScratch,
        semantic_workspace: &mut ReusableSemanticScanWorkspace,
        cancelled: &AtomicBool,
        owned_payload_tracker: Arc<ParallelOwnedPayloadTracker>,
        worker: usize,
    ) -> blockzilla_model::Result<ParallelScanJobOutput> {
        #[cfg(test)]
        self.apply_parallel_test_hook(job.id)?;
        let mut owned_payload = ParallelOwnedPayloadGuard::new(owned_payload_tracker);
        let block_ordinals = job.blocks;
        let registry_stats_before = self.context.registry_stats;
        let reader = &self.reader;
        let context = &mut self.context;
        let message_schema = reader.message_schema();
        let metadata_schema = reader.metadata_schema();
        let mut decoded_bytes = 0_u64;
        let mut blocks = Vec::new();
        blocks
            .try_reserve_exact(block_ordinals.len())
            .map_err(|error| {
                source_error(IndexerV3InstructionSourceError::Allocation {
                    context: "parallel V3 result blocks",
                    source: error,
                })
            })?;
        let mut signature_scan = SelectedBlockSignatureReader::for_blocks(
            reader,
            context.source.clone(),
            context.sidecars.signatures_size.is_some() && request_needs_signature_bytes(request),
            block_ordinals,
        )
        .map_err(source_error)?;

        let selected_len = signature_scan.selected_blocks.len();
        let mut selected_index = 0_usize;
        while selected_index < selected_len {
            let run_start = signature_scan
                .selected_blocks
                .get(selected_index)
                .expect("bounded selection index");
            let mut run_index_end = selected_index.checked_add(1).ok_or_else(|| {
                QueryError::InvalidStream("parallel V3 selected index overflow".into())
            })?;
            while run_index_end < selected_len {
                let previous = signature_scan
                    .selected_blocks
                    .get(run_index_end - 1)
                    .expect("bounded selection index");
                let next = signature_scan
                    .selected_blocks
                    .get(run_index_end)
                    .expect("bounded selection index");
                if previous.checked_add(1) != Some(next) {
                    break;
                }
                run_index_end = run_index_end.checked_add(1).ok_or_else(|| {
                    QueryError::InvalidStream("parallel V3 selected run index overflow".into())
                })?;
            }
            let run_end = signature_scan
                .selected_blocks
                .get(run_index_end - 1)
                .expect("bounded selection index")
                .checked_add(1)
                .ok_or_else(|| {
                    QueryError::InvalidStream("parallel V3 selected block overflow".into())
                })?;
            let mut semantic_scan = reader
                .begin_reusable_contiguous_semantic_scan_with_selection(
                    run_start..run_end,
                    semantic_plane_selection(request),
                    semantic_workspace,
                )
                .map_err(|error| source_error(IndexerV3InstructionSourceError::Reader(error)))?;

            for index in selected_index..run_index_end {
                if cancelled.load(Ordering::Relaxed) {
                    return Err(QueryError::InvalidStream(
                        "parallel V3 scan was cancelled".into(),
                    ));
                }
                let ordinal = signature_scan
                    .selected_blocks
                    .get(index)
                    .expect("bounded selection index");
                let row = reader.block(ordinal).ok_or_else(|| {
                    source_error(IndexerV3InstructionSourceError::Invalid(format!(
                        "parallel V3 block ordinal {ordinal} is missing after open validation"
                    )))
                })?;
                let block_signatures = signature_scan.read_block(ordinal).map_err(source_error)?;
                let transaction_capacity = usize::try_from(row.tx_count).map_err(|_| {
                    source_error(IndexerV3InstructionSourceError::Invalid(
                        "parallel V3 transaction count exceeds address space".into(),
                    ))
                })?;
                let mut transactions = transaction_buffers.pop().unwrap_or_default();
                transactions.clear();
                let mut counts = blockzilla_model::BlockCounts::default();
                reserve_exact(
                    &mut transactions,
                    if request.counts_only {
                        0
                    } else {
                        transaction_capacity
                    },
                    "parallel canonical V3 block transactions",
                )
                .map_err(source_error)?;
                let mut signature_cursor = 0_u64;

                let stats = semantic_scan
                    .visit_semantic_transactions(ordinal, None, |transaction| {
                        let signatures = transaction_signatures(
                            row,
                            &transaction,
                            block_signatures,
                            &mut signature_cursor,
                        )?;
                        if request.counts_only {
                            use blockzilla_compact_v2_reader::count_projection::{
                                CountMetadata, count_transaction,
                            };
                            anyhow::ensure!(
                                transaction.tx_index as u64 == counts.transactions,
                                "transaction order differs from block"
                            );
                            let flags = u32::from(transaction.source_flags);
                            let metadata = if flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0
                                && flags
                                    & (ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK
                                        | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK)
                                    == 0
                            {
                                CountMetadata::Split {
                                    outcome: required_plane(transaction.outcome, "outcome")?,
                                    loaded: required_plane(
                                        transaction.loaded_addresses,
                                        "loaded-addresses",
                                    )?,
                                    inner: required_plane(
                                        transaction.inner_instructions,
                                        "inner-instructions",
                                    )?,
                                    effect_state: transaction.effect_state,
                                }
                            } else {
                                CountMetadata::Unavailable
                            };
                            count_transaction(
                                &mut counts,
                                flags,
                                usize::try_from(
                                    transaction.signature_ordinals.end
                                        - transaction.signature_ordinals.start,
                                )?,
                                transaction.message,
                                metadata,
                                message_schema,
                                metadata_schema,
                                context.registry_entries,
                            )?;
                            return Ok(());
                        }
                        let projected = Self::project_transaction(
                            context,
                            request,
                            message_schema,
                            metadata_schema,
                            transaction,
                            signatures,
                            projection_scratch,
                        )?;
                        transactions.push(projected);
                        Ok(())
                    })
                    .map_err(|error| {
                        source_error(IndexerV3InstructionSourceError::Reader(error))
                    })?;
                if signature_cursor != u64::from(row.signature_count) {
                    return Err(source_error(IndexerV3InstructionSourceError::Invalid(
                        format!(
                            "parallel V3 block {} transactions consume {signature_cursor} of {} signatures",
                            row.block_id, row.signature_count
                        ),
                    )));
                }
                decoded_bytes = decoded_bytes
                    .checked_add(stats.total_decoded_bytes())
                    .ok_or_else(|| {
                        source_error(IndexerV3InstructionSourceError::Invalid(
                            "parallel V3 decoded-byte count overflow".into(),
                        ))
                    })?;
                let block_owned_payload =
                    canonical_block_owned_payload_bytes(&transactions, transactions.capacity())?;
                owned_payload.add_block(block_owned_payload)?;
                blocks.push(CanonicalBlock {
                    counts: request.counts_only.then_some(counts),
                    header: BlockHeader {
                        epoch: self.identity.epoch,
                        block_ordinal: row.block_id,
                        slot: row.slot,
                    },
                    transactions,
                });
            }
            semantic_scan
                .finish()
                .map_err(|error| source_error(IndexerV3InstructionSourceError::Reader(error)))?;
            selected_index = run_index_end;
        }
        signature_scan.finish().map_err(source_error)?;
        let registry = context
            .registry_receipt_since(registry_stats_before)
            .map_err(source_error)?;
        Ok(ParallelScanJobOutput {
            worker,
            blocks,
            unused_transaction_buffers: transaction_buffers,
            decoded_bytes,
            registry,
            owned_payload: owned_payload.finish(),
        })
    }

    #[cfg(test)]
    fn apply_parallel_test_hook(&self, job_id: usize) -> blockzilla_model::Result<()> {
        match self.identity.binding.as_deref() {
            Some("test-parallel-delay-first") if job_id == 0 => {
                thread::sleep(std::time::Duration::from_millis(250));
            }
            Some("test-parallel-errors") if job_id == 0 => {
                thread::sleep(std::time::Duration::from_millis(50));
                return Err(QueryError::InvalidStream(
                    "forced parallel V3 error in job 0".into(),
                ));
            }
            Some("test-parallel-errors") if job_id == 1 => {
                return Err(QueryError::InvalidStream(
                    "forced parallel V3 error in job 1".into(),
                ));
            }
            _ => {}
        }
        Ok(())
    }

    fn run_parallel_jobs<F>(
        &self,
        request: &ScanRequest,
        _registry_policy: IndexerV3RegistryReadPolicy,
        workers: NonZeroUsize,
        mut jobs: ParallelScanJobPlan,
        mut consume: F,
    ) -> blockzilla_model::Result<ParallelScanTotals>
    where
        F: FnMut(&ParallelScanJobOutput) -> blockzilla_model::Result<()>,
    {
        validate_parallel_worker_count(workers)?;
        let block_count = jobs.block_count();
        let global_projected_block_bound = workers
            .get()
            .checked_mul(INDEXER_V3_PARALLEL_BUFFERED_BLOCKS_PER_WORKER)
            .ok_or_else(|| {
                QueryError::InvalidRequest("parallel V3 projected-block bound overflows".into())
            })?;
        if block_count == 0 {
            return Ok(ParallelScanTotals {
                parallel: IndexerV3ParallelScanStats {
                    requested_workers: workers.get(),
                    blocks_per_job_limit: INDEXER_V3_PARALLEL_BLOCKS_PER_JOB,
                    declared_decoded_byte_limit: INDEXER_V3_PARALLEL_DECLARED_DECODED_BYTE_LIMIT,
                    transaction_limit: INDEXER_V3_PARALLEL_TRANSACTION_LIMIT,
                    global_projected_block_bound,
                    ..IndexerV3ParallelScanStats::default()
                },
                ..ParallelScanTotals::default()
            });
        }
        let worker_count = workers.get().min(block_count);
        let mut worker_sources = (0..worker_count)
            .map(|_| self.fork_for_parallel_scan())
            .collect::<Vec<_>>();
        let request = Arc::new(request.clone());
        let worker_activity = Arc::new(ParallelWorkerActivity::default());
        let result_channel_activity = Arc::new(ParallelResultChannelActivity::default());
        let owned_payload_tracker = Arc::new(ParallelOwnedPayloadTracker::default());
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut next_job = jobs.next_job()?;
        #[cfg(test)]
        let initial_worker_barrier = self
            .identity
            .binding
            .as_deref()
            .is_some_and(|binding| binding == "test-parallel-delay-first")
            .then(|| Arc::new(std::sync::Barrier::new(worker_count)));

        thread::scope(|scope| {
            let (result_sender, result_receiver) = mpsc::channel::<(
                usize,
                usize,
                ParallelScanJobResources,
                blockzilla_model::Result<ParallelScanJobOutput>,
            )>();
            let mut job_senders = Vec::with_capacity(worker_count);
            let mut handles = Vec::with_capacity(worker_count);
            for worker in 0..worker_count {
                let (job_sender, job_receiver) = mpsc::sync_channel::<ParallelWorkerCommand>(1);
                job_senders.push(job_sender);
                let result_sender = result_sender.clone();
                let request = Arc::clone(&request);
                let worker_activity = Arc::clone(&worker_activity);
                let result_channel_activity = Arc::clone(&result_channel_activity);
                let owned_payload_tracker = Arc::clone(&owned_payload_tracker);
                let cancelled = Arc::clone(&cancelled);
                #[cfg(test)]
                let initial_worker_barrier = initial_worker_barrier.clone();
                let mut source = worker_sources.remove(0);
                handles.push(scope.spawn(move || {
                    let mut projection_scratch = TransactionProjectionScratch::default();
                    let mut semantic_workspace = ReusableSemanticScanWorkspace::default();
                    let mut transaction_buffers = Vec::new();
                    loop {
                        let Ok(command) = job_receiver.recv() else {
                            break;
                        };
                        let work = match command {
                            ParallelWorkerCommand::Recycle(recycle) => {
                                reclaim_parallel_worker_output(
                                    &mut transaction_buffers,
                                    recycle,
                                    &mut source.context.output_pool,
                                );
                                continue;
                            }
                            ParallelWorkerCommand::Shutdown => break,
                            ParallelWorkerCommand::Decode(work) => work,
                        };
                        let job_id = work.job.id;
                        let resources = work.job.resources;
                        let result = if cancelled.load(Ordering::Relaxed) {
                            Err(QueryError::InvalidStream(
                                "parallel V3 scan was cancelled".into(),
                            ))
                        } else {
                            catch_unwind(AssertUnwindSafe(|| {
                                let _activity = worker_activity.enter();
                                #[cfg(test)]
                                if job_id < worker_count
                                    && let Some(barrier) = initial_worker_barrier.as_ref()
                                {
                                    barrier.wait();
                                }
                                let result = source.decode_parallel_job(
                                    &request,
                                    work.job,
                                    std::mem::take(&mut transaction_buffers),
                                    &mut projection_scratch,
                                    &mut semantic_workspace,
                                    &cancelled,
                                    Arc::clone(&owned_payload_tracker),
                                    worker,
                                );
                                projection_scratch.shed_buffers_above(
                                    INDEXER_V3_PARALLEL_RETAINED_PROJECTION_SCRATCH_LIMIT,
                                );
                                let shed = semantic_workspace
                                    .shed_buffers_above(
                                        INDEXER_V3_PARALLEL_RETAINED_WORKSPACE_LIMIT,
                                    )
                                    .map_err(|error| {
                                        source_error(IndexerV3InstructionSourceError::Reader(error))
                                    });
                                match result {
                                    Err(error) => Err(error),
                                    Ok(output) => shed.map(|_| output),
                                }
                            }))
                            .unwrap_or_else(|_| {
                                Err(QueryError::InvalidStream(format!(
                                    "parallel V3 worker {worker} panicked in job {job_id}"
                                )))
                            })
                        };
                        #[cfg(test)]
                        let result = {
                            let mut result = result;
                            if source.identity.binding.as_deref()
                                == Some("test-parallel-invalid-transactions")
                                && job_id == 0
                                && let Ok(output) = result.as_mut()
                                && let Some(block) = output.blocks.first_mut()
                            {
                                block.transactions.clear();
                            }
                            result
                        };
                        #[cfg(test)]
                        let result_job_id = if source.identity.binding.as_deref()
                            == Some("test-parallel-invalid-result")
                            && job_id == 0
                        {
                            usize::MAX
                        } else {
                            job_id
                        };
                        #[cfg(not(test))]
                        let result_job_id = job_id;
                        result_channel_activity.note_send();
                        if result_sender
                            .send((result_job_id, worker, resources, result))
                            .is_err()
                        {
                            result_channel_activity.cancel_send();
                            break;
                        }
                    }
                }));
            }
            drop(result_sender);

            let mut pending = BTreeMap::<usize, PendingParallelResult>::new();
            let mut assigned_descriptors = BTreeMap::<usize, ParallelAssignedDescriptor>::new();
            let mut next_result = 0_usize;
            let mut assigned_jobs = 0_usize;
            let mut received_results = 0_usize;
            let job_window = worker_count
                .saturating_mul(INDEXER_V3_PARALLEL_JOB_WINDOW_PER_WORKER)
                .min(block_count);
            let mut idle_workers = (0..worker_count).collect::<VecDeque<_>>();
            let mut worker_alive = vec![true; worker_count];
            let mut worker_registry_resident = vec![0_u64; worker_count];
            let mut worker_seen = vec![false; worker_count];
            let mut projected_blocks = 0_u64;
            let mut coordinator_pending_projected_blocks = 0_usize;
            let mut max_coordinator_pending_projected_blocks = 0_usize;
            let mut max_coordinator_pending_results = 0_usize;
            let mut in_flight_resources = ParallelScanJobResources::default();
            let mut max_in_flight_resources = ParallelScanJobResources::default();
            let mut max_in_flight_jobs = 0_usize;
            let mut totals = ParallelScanTotals::default();
            let mut failure = None;
            loop {
                while failure.is_none()
                    && assigned_jobs.saturating_sub(next_result) < job_window
                    && next_job.is_some()
                {
                    let Some(worker) = idle_workers.pop_front() else {
                        break;
                    };
                    if !worker_alive[worker] {
                        continue;
                    }
                    let job = next_job.take().expect("next parallel V3 job is present");
                    let combined =
                        match admitted_parallel_resources(in_flight_resources, job.resources) {
                            Ok(Some(combined)) => combined,
                            Ok(None) => {
                                idle_workers.push_front(worker);
                                next_job = Some(job);
                                break;
                            }
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        };
                    if job.id != assigned_jobs {
                        failure = Some(QueryError::InvalidStream(format!(
                            "parallel V3 job {} follows assigned job {assigned_jobs}",
                            job.id
                        )));
                        break;
                    }
                    let descriptor = match job.descriptor(&self.reader, worker) {
                        Ok(descriptor) => descriptor,
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    };
                    let Some(next_assigned_jobs) = assigned_jobs.checked_add(1) else {
                        failure = Some(QueryError::InvalidStream(
                            "parallel V3 assigned-job count overflow".into(),
                        ));
                        break;
                    };
                    if job_senders[worker]
                        .send(ParallelWorkerCommand::Decode(ParallelWorkerJob { job }))
                        .is_err()
                    {
                        worker_alive[worker] = false;
                        failure = Some(QueryError::InvalidStream(format!(
                            "parallel V3 worker {worker} stopped before its next job"
                        )));
                        break;
                    }
                    assigned_descriptors.insert(assigned_jobs, descriptor);
                    in_flight_resources = combined;
                    assigned_jobs = next_assigned_jobs;
                    max_in_flight_jobs =
                        max_in_flight_jobs.max(assigned_jobs.saturating_sub(next_result));
                    max_in_flight_resources.blocks = max_in_flight_resources
                        .blocks
                        .max(in_flight_resources.blocks);
                    max_in_flight_resources.declared_decoded_bytes = max_in_flight_resources
                        .declared_decoded_bytes
                        .max(in_flight_resources.declared_decoded_bytes);
                    max_in_flight_resources.transactions = max_in_flight_resources
                        .transactions
                        .max(in_flight_resources.transactions);
                    next_job = match jobs.next_job() {
                        Ok(job) => job,
                        Err(error) => {
                            failure = Some(error);
                            None
                        }
                    };
                }

                if failure.is_some() {
                    break;
                }

                // Dispatch work to every idle worker before the ordered sink
                // callback below. The callback can be much slower than V3
                // projection (for example, one durable SQLite commit per
                // block). Keeping the next bounded job active lets one worker
                // overlap projection with that callback. The existing job
                // window and resource counters include both the retained
                // result and the new decode job.
                let next_result_before_consume = next_result;
                while let Some(pending_result) = pending.remove(&next_result) {
                    let output = match pending_result.result {
                        Ok(output) => output,
                        Err(error) => {
                            failure = Some(error);
                            cancelled.store(true, Ordering::Relaxed);
                            break;
                        }
                    };
                    let process_result = (|| -> blockzilla_model::Result<()> {
                        assigned_descriptors.remove(&next_result).ok_or_else(|| {
                            QueryError::InvalidStream(format!(
                                "parallel V3 job {next_result} lost its assignment"
                            ))
                        })?;
                        in_flight_resources =
                            in_flight_resources.checked_sub(pending_result.resources)?;
                        coordinator_pending_projected_blocks = coordinator_pending_projected_blocks
                            .checked_sub(output.blocks.len())
                            .ok_or_else(|| {
                                QueryError::InvalidStream(
                                    "parallel V3 pending projected-block count underflow".into(),
                                )
                            })?;
                        consume(&output)?;
                        totals.decoded_bytes = totals
                            .decoded_bytes
                            .checked_add(output.decoded_bytes)
                            .ok_or_else(|| {
                                QueryError::InvalidStream(
                                    "parallel V3 decoded-byte count overflow".into(),
                                )
                            })?;
                        let output_block_count =
                            u64::try_from(output.blocks.len()).map_err(|_| {
                                QueryError::InvalidStream(
                                    "parallel V3 projected-block count exceeds u64".into(),
                                )
                            })?;
                        projected_blocks = projected_blocks
                            .checked_add(output_block_count)
                            .ok_or_else(|| {
                                QueryError::InvalidStream(
                                    "parallel V3 projected-block count overflow".into(),
                                )
                            })?;
                        worker_registry_resident[output.worker] =
                            output.registry.resident_payload_bytes;
                        let mut counter_receipt = output.registry;
                        counter_receipt.resident_payload_bytes = 0;
                        totals.registry = Some(match totals.registry {
                            Some(current) => merge_registry_receipts(
                                current,
                                Some(counter_receipt),
                                self.context.full_registry.is_some(),
                            )?,
                            None => counter_receipt,
                        });
                        Ok(())
                    })();
                    let reclaim_result = return_parallel_output_to_worker(&job_senders, output);
                    if let Err(error) = process_result {
                        failure = Some(error);
                        cancelled.store(true, Ordering::Relaxed);
                        break;
                    }
                    if let Err(error) = reclaim_result {
                        failure = Some(error);
                        cancelled.store(true, Ordering::Relaxed);
                        break;
                    }
                    next_result = match next_result.checked_add(1) {
                        Some(value) => value,
                        None => {
                            failure = Some(QueryError::InvalidStream(
                                "parallel V3 result index overflow".into(),
                            ));
                            cancelled.store(true, Ordering::Relaxed);
                            break;
                        }
                    };
                }

                if failure.is_some() {
                    break;
                }
                if next_result != next_result_before_consume {
                    // Consuming results can reopen the job window or the
                    // global resource gate. Re-enter dispatch before waiting
                    // for another result; every worker can already be idle
                    // when an earlier delayed job releases a full window.
                    continue;
                }
                if next_job.is_none() && next_result == assigned_jobs {
                    break;
                }

                let (job_id, worker, reported_resources, mut result) = match result_receiver.recv()
                {
                    Ok(result) => result,
                    Err(_) => {
                        failure = Some(QueryError::InvalidStream(
                            "all parallel V3 workers stopped before the scan completed".into(),
                        ));
                        break;
                    }
                };
                result_channel_activity.note_receive();
                received_results = match received_results.checked_add(1) {
                    Some(value) => value,
                    None => {
                        let accounting_error = QueryError::InvalidStream(
                            "parallel V3 received-result count overflow".into(),
                        );
                        failure = Some(
                            return_successful_parallel_result_to_worker(&job_senders, result)
                                .err()
                                .unwrap_or(accounting_error),
                        );
                        break;
                    }
                };
                if worker >= worker_count {
                    let protocol_error = QueryError::InvalidStream(format!(
                        "parallel V3 result names invalid worker {worker}"
                    ));
                    failure = Some(
                        return_successful_parallel_result_to_worker(&job_senders, result)
                            .err()
                            .unwrap_or(protocol_error),
                    );
                    break;
                }
                let Some(descriptor) = assigned_descriptors.get(&job_id).copied() else {
                    let protocol_error = QueryError::InvalidStream(format!(
                        "parallel V3 result names unassigned job {job_id}"
                    ));
                    failure = Some(
                        return_successful_parallel_result_to_worker(&job_senders, result)
                            .err()
                            .unwrap_or(protocol_error),
                    );
                    break;
                };
                if descriptor.worker != worker || descriptor.resources != reported_resources {
                    let protocol_error = QueryError::InvalidStream(format!(
                        "parallel V3 job {job_id} result differs from its assignment"
                    ));
                    failure = Some(
                        return_successful_parallel_result_to_worker(&job_senders, result)
                            .err()
                            .unwrap_or(protocol_error),
                    );
                    break;
                }
                if let Ok(output) = result.as_ref()
                    && let Err(error) =
                        validate_parallel_job_output(&self.identity, descriptor, worker, output)
                {
                    let successful = std::mem::replace(&mut result, Err(error));
                    if let Err(error) =
                        return_successful_parallel_result_to_worker(&job_senders, successful)
                    {
                        failure = Some(error);
                        break;
                    }
                }
                worker_seen[worker] = true;
                let worker_stopped = result.is_err();
                let result_blocks = result.as_ref().map_or(0, |output| output.blocks.len());
                if pending.contains_key(&job_id) {
                    let protocol_error = QueryError::InvalidStream(format!(
                        "parallel V3 job {job_id} returned twice"
                    ));
                    failure = Some(
                        return_successful_parallel_result_to_worker(&job_senders, result)
                            .err()
                            .unwrap_or(protocol_error),
                    );
                    break;
                }
                let Some(next_pending_projected_blocks) =
                    coordinator_pending_projected_blocks.checked_add(result_blocks)
                else {
                    let accounting_error = QueryError::InvalidStream(
                        "parallel V3 pending projected-block count overflow".into(),
                    );
                    failure = Some(
                        return_successful_parallel_result_to_worker(&job_senders, result)
                            .err()
                            .unwrap_or(accounting_error),
                    );
                    break;
                };
                coordinator_pending_projected_blocks = next_pending_projected_blocks;
                pending.insert(
                    job_id,
                    PendingParallelResult {
                        resources: descriptor.resources,
                        result,
                    },
                );
                max_coordinator_pending_projected_blocks = max_coordinator_pending_projected_blocks
                    .max(coordinator_pending_projected_blocks);
                max_coordinator_pending_results =
                    max_coordinator_pending_results.max(pending.len());
                if worker_stopped {
                    worker_alive[worker] = false;
                } else {
                    idle_workers.push_back(worker);
                }
            }

            if failure.is_some() {
                cancelled.store(true, Ordering::Relaxed);
            }

            // Every successful result owns allocations created by its decode
            // worker. Return all results already held by the coordinator
            // before waiting for the remaining assigned jobs.
            for pending_result in std::mem::take(&mut pending).into_values() {
                if let Err(error) =
                    return_successful_parallel_result_to_worker(&job_senders, pending_result.result)
                    && failure.is_none()
                {
                    failure = Some(error);
                }
            }
            while received_results < assigned_jobs {
                let (_, _, _, result) = match result_receiver.recv() {
                    Ok(result) => result,
                    Err(_) => {
                        if failure.is_none() {
                            failure = Some(QueryError::InvalidStream(
                                "all parallel V3 workers stopped during result cleanup".into(),
                            ));
                        }
                        break;
                    }
                };
                result_channel_activity.note_receive();
                received_results = match received_results.checked_add(1) {
                    Some(value) => value,
                    None => {
                        let accounting_error = QueryError::InvalidStream(
                            "parallel V3 received-result count overflow during cleanup".into(),
                        );
                        if let Err(error) =
                            return_successful_parallel_result_to_worker(&job_senders, result)
                        {
                            if failure.is_none() {
                                failure = Some(error);
                            }
                        } else if failure.is_none() {
                            failure = Some(accounting_error);
                        }
                        break;
                    }
                };
                if let Err(error) =
                    return_successful_parallel_result_to_worker(&job_senders, result)
                    && failure.is_none()
                {
                    failure = Some(error);
                }
            }
            assigned_descriptors.clear();

            // A worker receives its recycle commands before Shutdown because
            // both use the same bounded FIFO channel.
            for (worker, sender) in job_senders.iter().enumerate() {
                if sender.send(ParallelWorkerCommand::Shutdown).is_err() && failure.is_none() {
                    failure = Some(QueryError::InvalidStream(format!(
                        "parallel V3 worker {worker} stopped before shutdown"
                    )));
                }
            }
            drop(job_senders);
            for (worker, handle) in handles.into_iter().enumerate() {
                if handle.join().is_err() && failure.is_none() {
                    failure = Some(QueryError::InvalidStream(format!(
                        "parallel V3 worker {worker} panicked"
                    )));
                }
            }
            let remaining_owned_payload = owned_payload_tracker.current.load(Ordering::Acquire);
            if remaining_owned_payload != 0 && failure.is_none() {
                failure = Some(QueryError::InvalidStream(format!(
                    "parallel V3 cleanup retained {remaining_owned_payload} owned payload bytes"
                )));
            }
            if let Some(registry) = totals.registry.as_mut() {
                registry.resident_payload_bytes = if self.context.full_registry.is_some() {
                    worker_registry_resident.iter().copied().max().unwrap_or(0)
                } else {
                    worker_registry_resident
                        .iter()
                        .try_fold(0_u64, |total, bytes| {
                            total.checked_add(*bytes).ok_or_else(|| {
                                QueryError::InvalidStream(
                                    "parallel V3 registry resident payload bytes overflow".into(),
                                )
                            })
                        })?
                };
            }
            totals.parallel = IndexerV3ParallelScanStats {
                requested_workers: workers.get(),
                effective_workers: worker_seen.into_iter().filter(|seen| *seen).count(),
                max_active_workers: worker_activity.peak(),
                jobs: u64::try_from(assigned_jobs).map_err(|_| {
                    QueryError::InvalidStream("parallel V3 job count exceeds u64".into())
                })?,
                projected_blocks,
                blocks_per_job_limit: INDEXER_V3_PARALLEL_BLOCKS_PER_JOB,
                job_window_limit: job_window,
                max_in_flight_jobs,
                max_coordinator_pending_results,
                max_result_channel_backlog: result_channel_activity.peak(),
                max_coordinator_pending_projected_blocks,
                declared_decoded_byte_limit: INDEXER_V3_PARALLEL_DECLARED_DECODED_BYTE_LIMIT,
                transaction_limit: INDEXER_V3_PARALLEL_TRANSACTION_LIMIT,
                max_in_flight_declared_decoded_bytes: max_in_flight_resources
                    .declared_decoded_bytes,
                max_in_flight_transactions: max_in_flight_resources.transactions,
                max_owned_payload_block_bytes: owned_payload_tracker
                    .max_block
                    .load(Ordering::Relaxed),
                max_in_flight_owned_payload_bytes: owned_payload_tracker
                    .peak
                    .load(Ordering::Relaxed),
                global_projected_block_bound,
            };
            match failure {
                Some(error) => Err(error),
                None if next_job.is_none()
                    && next_result == assigned_jobs
                    && received_results == assigned_jobs
                    && pending.is_empty()
                    && assigned_descriptors.is_empty() =>
                {
                    Ok(totals)
                }
                None => Err(QueryError::InvalidStream(format!(
                    "parallel V3 produced {next_result} of {assigned_jobs} assigned jobs"
                ))),
            }
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn project_transaction(
        context: &mut ExactContext,
        request: &ScanRequest,
        message_schema: blockzilla_compact_v2_reader::CompactV2MessageSchema,
        metadata_schema: blockzilla_compact_v2_reader::CompactV2MetadataSchema,
        transaction: SemanticTransaction<'_>,
        signatures: Option<&[[u8; 64]]>,
        scratch: &mut TransactionProjectionScratch,
    ) -> anyhow::Result<CanonicalTransaction> {
        Self::project_transaction_inner(
            context,
            request,
            message_schema,
            metadata_schema,
            transaction,
            signatures,
            scratch,
        )
        .map_err(anyhow::Error::new)
    }

    #[allow(clippy::too_many_arguments)]
    fn project_transaction_inner(
        context: &mut ExactContext,
        request: &ScanRequest,
        message_schema: blockzilla_compact_v2_reader::CompactV2MessageSchema,
        metadata_schema: blockzilla_compact_v2_reader::CompactV2MetadataSchema,
        transaction: SemanticTransaction<'_>,
        signatures: Option<&[[u8; 64]]>,
        scratch: &mut TransactionProjectionScratch,
    ) -> IndexerV3InstructionSourceResult<CanonicalTransaction> {
        let flags = u32::from(transaction.source_flags);
        let primary_signature = if request.include_primary_signatures {
            signatures.and_then(|values| values.first()).copied()
        } else {
            None
        };
        if flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0
            && flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0
        {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "slot {} transaction {} has METADATA_RAW_FALLBACK without HAS_METADATA",
                transaction.slot, transaction.tx_index
            )));
        }
        if flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            let token_balance_coverage = if request.token_balances.is_requested() {
                TokenBalanceCoverage::Unknown(CoverageReason::RawTransaction)
            } else {
                TokenBalanceCoverage::NotRequested
            };
            return Ok(CanonicalTransaction {
                header: TransactionHeader {
                    tx_index: transaction.tx_index,
                    status: ExecutionStatus::Unknown(CoverageReason::RawTransaction),
                    failed_outer_instruction_index: None,
                    instruction_coverage: InstructionCoverage::Unknown(
                        CoverageReason::RawTransaction,
                    ),
                    cpi_coverage: CpiCoverage::Unknown(CoverageReason::RawTransaction),
                },
                primary_signature,
                required_signers: Vec::new(),
                instructions: Vec::new(),
                token_balance_coverage,
                token_balances: Vec::new(),
            });
        }

        let projector = CompactV2MessageProjector::new(message_schema, context.registry_entries);
        let message = if !request.include_instructions
            && !request.include_execution_status
            && !request.include_required_signers
            && request.required_signer.is_none()
        {
            CompactV2MessageProjector::new(message_schema, context.registry_entries)
                .count_message(transaction.message)?
        } else {
            Self::project_requested_message(
                context,
                projector,
                transaction.message,
                &request.instruction_data,
                !request.require_complete_instruction_data,
                scratch,
                request.include_instructions && request.include_instruction_accounts,
            )?
        };
        let is_v0 = matches!(
            message.version(),
            ProjectedCompactV2MessageVersion::V0 { .. }
        );
        require_flag(
            &transaction,
            ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
            "MESSAGE_V0",
            is_v0,
        )?;
        let signature_count = usize::try_from(
            transaction
                .signature_ordinals
                .end
                .checked_sub(transaction.signature_ordinals.start)
                .ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 transaction signature range decreases".into(),
                    )
                })?,
        )
        .map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 transaction signature count exceeds address space".into(),
            )
        })?;
        if signature_count != usize::from(message.header().num_required_signatures) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "slot {} transaction {} has {signature_count} signature rows but requires {}",
                transaction.slot,
                transaction.tx_index,
                message.header().num_required_signatures
            )));
        }
        if let Some(signatures) = signatures
            && signatures.len() != signature_count
        {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "slot {} transaction {} signature window has the wrong length",
                transaction.slot, transaction.tx_index
            )));
        }

        let metadata = if flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            reject_flag(&transaction, ARCHIVE_V2_TX_FLAG_HAS_ERROR, "HAS_ERROR")?;
            reject_flag(
                &transaction,
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                "HAS_INNER_IX",
            )?;
            reject_flag(
                &transaction,
                ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                "HAS_LOADED_ADDRESSES",
            )?;
            if transaction.loaded_addresses.is_some()
                || transaction.inner_instructions.is_some()
                || transaction.token_balances.is_some()
                || transaction.outcome.is_some()
                || transaction.raw_metadata_present
                || transaction.raw_metadata.is_some()
            {
                return Err(IndexerV3InstructionSourceError::Invalid(
                    "metadata-absent V3 transaction exposes metadata planes".into(),
                ));
            }
            ProjectedMetadata::Absent
        } else if flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
            if !transaction.raw_metadata_present
                || transaction.loaded_addresses.is_some()
                || transaction.inner_instructions.is_some()
                || transaction.token_balances.is_some()
                || transaction.outcome.is_some()
            {
                return Err(IndexerV3InstructionSourceError::Invalid(
                    "raw-metadata V3 transaction has inconsistent semantic planes".into(),
                ));
            }
            ProjectedMetadata::Raw
        } else {
            if transaction.raw_metadata_present || transaction.raw_metadata.is_some() {
                return Err(IndexerV3InstructionSourceError::Invalid(
                    "decoded V3 metadata also contains raw fallback bytes".into(),
                ));
            }
            if !request.include_instructions && !request.include_execution_status {
                if transaction.loaded_addresses.is_some()
                    || transaction.inner_instructions.is_some()
                    || transaction.outcome.is_some()
                {
                    return Err(IndexerV3InstructionSourceError::Invalid(
                        "unrequested V3 instruction metadata planes are present".into(),
                    ));
                }
                ProjectedMetadata::ExactUnprojected
            } else {
                let loaded = required_plane(transaction.loaded_addresses, "loaded-addresses")?;
                let inner = required_plane(transaction.inner_instructions, "inner-instructions")?;
                let outcome = required_plane(transaction.outcome, "outcome")?;
                let limits = CompactV2MetadataProjectionLimits::for_message(&message);
                let metadata =
                    CompactV2MetadataProjector::new(metadata_schema, context.registry_entries)
                        .project_split_planes(outcome, loaded, inner, limits)?;
                require_flag(
                    &transaction,
                    ARCHIVE_V2_TX_FLAG_HAS_ERROR,
                    "HAS_ERROR",
                    !metadata.execution_status.is_success(),
                )?;
                require_flag(
                    &transaction,
                    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                    "HAS_INNER_IX",
                    metadata.inner_instructions.is_some(),
                )?;
                require_flag(
                    &transaction,
                    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                    "HAS_LOADED_ADDRESSES",
                    !metadata.loaded_writable_addresses.is_empty()
                        || !metadata.loaded_readonly_addresses.is_empty(),
                )?;
                let expected_cpi_state = match &metadata.inner_instructions {
                    None => 1,
                    Some(groups) if groups.is_empty() => 2,
                    Some(_) => 3,
                };
                if transaction.effect_state & 0b111 != expected_cpi_state {
                    return Err(IndexerV3InstructionSourceError::Invalid(format!(
                        "slot {} transaction {} CPI state differs from its inner plane",
                        transaction.slot, transaction.tx_index
                    )));
                }
                ProjectedMetadata::Exact(metadata)
            }
        };

        let (recorded_status, recorded_failed_outer, recorded_cpi) = match &metadata {
            ProjectedMetadata::Absent => (
                ExecutionStatus::Unknown(CoverageReason::MetadataAbsent),
                None,
                CpiCoverage::Unknown(CoverageReason::MetadataAbsent),
            ),
            ProjectedMetadata::Raw => (
                ExecutionStatus::Unknown(CoverageReason::RawMetadata),
                None,
                CpiCoverage::Unknown(CoverageReason::RawMetadata),
            ),
            ProjectedMetadata::Exact(metadata) => {
                let (status, failed) = match metadata.execution_status {
                    CompactV2ExecutionStatus::Succeeded => (ExecutionStatus::Succeeded, None),
                    CompactV2ExecutionStatus::Failed {
                        failed_outer_instruction_index,
                    } => (
                        ExecutionStatus::Failed,
                        failed_outer_instruction_index.map(u32::from),
                    ),
                };
                let cpi = if metadata.inner_instructions.is_some() {
                    CpiCoverage::Complete
                } else {
                    CpiCoverage::NotRecorded
                };
                (status, failed, cpi)
            }
            ProjectedMetadata::ExactUnprojected => (
                ExecutionStatus::Unknown(CoverageReason::ProjectionNotRequested),
                None,
                CpiCoverage::Unknown(CoverageReason::ProjectionNotRequested),
            ),
        };

        let (status, failed_outer_instruction_index) = if request.include_execution_status {
            (recorded_status, recorded_failed_outer)
        } else {
            (
                ExecutionStatus::Unknown(CoverageReason::ProjectionNotRequested),
                None,
            )
        };
        let cpi_coverage = if request.include_instructions {
            recorded_cpi
        } else {
            CpiCoverage::Unknown(CoverageReason::ProjectionNotRequested)
        };

        let signer_matches = request.required_signer.is_none_or(|key| {
            message
                .static_account_keys()
                .iter()
                .take(usize::from(message.header().num_required_signatures))
                .any(|reference| context.query_keys.matches(*reference, &key))
        });
        let include_programs = signer_matches
            && (request.required_signer.is_none()
                || matches!(recorded_status, ExecutionStatus::Succeeded));
        let required_signers =
            if request.include_required_signers && request.required_signer.is_some() {
                request
                    .required_signer
                    .filter(|_| signer_matches)
                    .into_iter()
                    .collect()
            } else if request.include_required_signers {
                let required = usize::from(message.header().num_required_signatures);
                let mut required_signers = Vec::new();
                reserve_exact(
                    &mut required_signers,
                    required,
                    "resolved V3 required signers",
                )?;
                if request.include_instruction_accounts {
                    let signer_keys = scratch.account_keys.get(..required).ok_or_else(|| {
                        IndexerV3InstructionSourceError::Invalid(
                            "required signer prefix exceeds projected static keys".into(),
                        )
                    })?;
                    required_signers.extend_from_slice(signer_keys);
                } else {
                    let references =
                        message
                            .static_account_keys()
                            .get(..required)
                            .ok_or_else(|| {
                                IndexerV3InstructionSourceError::Invalid(
                                    "required signer prefix exceeds projected static keys".into(),
                                )
                            })?;
                    for reference in references {
                        required_signers.push(context.resolve_pubkey(*reference)?);
                    }
                }
                required_signers
            } else {
                Vec::new()
            };

        let loaded_key_count = match &metadata {
            ProjectedMetadata::Exact(metadata) => {
                let total = metadata
                    .loaded_writable_addresses
                    .len()
                    .checked_add(metadata.loaded_readonly_addresses.len())
                    .ok_or_else(|| {
                        IndexerV3InstructionSourceError::Invalid(
                            "loaded-address count overflow".into(),
                        )
                    })?;
                Some(total)
            }
            ProjectedMetadata::Absent
            | ProjectedMetadata::Raw
            | ProjectedMetadata::ExactUnprojected
                if message.expected_loaded_addresses() == 0 =>
            {
                Some(0)
            }
            ProjectedMetadata::Absent
            | ProjectedMetadata::Raw
            | ProjectedMetadata::ExactUnprojected => None,
        };

        let (instruction_coverage, instructions) = if !request.include_instructions {
            (
                InstructionCoverage::Unknown(CoverageReason::ProjectionNotRequested),
                Vec::new(),
            )
        } else if let Some(loaded_key_count) = loaded_key_count {
            let account_key_count = message
                .static_account_keys()
                .len()
                .checked_add(loaded_key_count)
                .ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "combined V3 account-key count overflow".into(),
                    )
                })?;
            if request.include_instruction_accounts {
                reserve_exact(
                    &mut scratch.account_keys,
                    loaded_key_count,
                    "combined V3 message account keys",
                )?;
                if let ProjectedMetadata::Exact(metadata) = &metadata {
                    for reference in metadata
                        .loaded_writable_addresses
                        .iter()
                        .chain(&metadata.loaded_readonly_addresses)
                    {
                        scratch
                            .account_keys
                            .push(context.resolve_pubkey(*reference)?);
                    }
                }
            }
            let instructions = Self::project_instructions(
                context,
                request,
                transaction.message,
                &message,
                &metadata,
                &scratch.account_keys,
                account_key_count,
                signatures,
                include_programs,
            )?;
            (InstructionCoverage::Complete, instructions)
        } else {
            let reason = match metadata {
                ProjectedMetadata::Absent => CoverageReason::MetadataAbsent,
                ProjectedMetadata::Raw => CoverageReason::RawMetadata,
                ProjectedMetadata::Exact(_) => unreachable!("exact metadata supplied loaded keys"),
                ProjectedMetadata::ExactUnprojected => {
                    unreachable!("instruction projection requires exact metadata")
                }
            };
            (InstructionCoverage::Unknown(reason), Vec::new())
        };

        let (token_balance_coverage, token_balances) = match &request.token_balances {
            TokenBalanceRequirement::None => (TokenBalanceCoverage::NotRequested, Vec::new()),
            requirement => match &metadata {
                ProjectedMetadata::Exact(_) | ProjectedMetadata::ExactUnprojected => {
                    let plane = required_plane(transaction.token_balances, "token-balances")?;
                    CompactV2MetadataProjector::new(metadata_schema, context.registry_entries)
                        .project_split_token_balances_reusing(
                            plane,
                            CompactV2MetadataProjectionLimits::for_message(&message),
                            &mut scratch.token_balances,
                        )?;
                    let balances = Self::resolve_token_balances(
                        context,
                        requirement,
                        &scratch.token_balances.pre,
                        &scratch.token_balances.post,
                    )?;
                    (TokenBalanceCoverage::Complete, balances)
                }
                ProjectedMetadata::Absent => (
                    TokenBalanceCoverage::Unknown(CoverageReason::MetadataAbsent),
                    Vec::new(),
                ),
                ProjectedMetadata::Raw => (
                    TokenBalanceCoverage::Unknown(CoverageReason::RawMetadata),
                    Vec::new(),
                ),
            },
        };

        Ok(CanonicalTransaction {
            header: TransactionHeader {
                tx_index: transaction.tx_index,
                status,
                failed_outer_instruction_index,
                instruction_coverage,
                cpi_coverage,
            },
            primary_signature,
            required_signers,
            instructions,
            token_balance_coverage,
            token_balances,
        })
    }

    fn resolve_token_balances(
        context: &mut ExactContext,
        requirement: &TokenBalanceRequirement,
        pre: &[CompactTokenBalance],
        post: &[CompactTokenBalance],
    ) -> IndexerV3InstructionSourceResult<Vec<RecordedTokenBalance>> {
        let mut output = context.output_pool.balances();
        if matches!(requirement, TokenBalanceRequirement::All) {
            reserve_exact(
                &mut output,
                pre.len().saturating_add(post.len()),
                "projected V3 token balances",
            )?;
        }
        for (side, balances) in [(TokenBalanceSide::Pre, pre), (TokenBalanceSide::Post, post)] {
            for (balance_index, balance) in balances.iter().enumerate() {
                let mint = match (balance.mint, requirement) {
                    (Some(reference), TokenBalanceRequirement::Mints(keys)) => {
                        let Some(key) = context.query_keys.selected(reference, keys) else {
                            continue;
                        };
                        Some(key)
                    }
                    (reference, _) => reference
                        .map(|reference| context.resolve_pubkey(reference))
                        .transpose()?,
                };
                if !requirement.selects(mint.as_ref()) {
                    continue;
                }
                let owner = balance
                    .owner
                    .map(|reference| context.resolve_pubkey(reference))
                    .transpose()?;
                let token_program = balance
                    .program_id
                    .map(|reference| context.resolve_pubkey(reference))
                    .transpose()?;
                if output.len() == output.capacity() {
                    output.try_reserve(1).map_err(|source| {
                        IndexerV3InstructionSourceError::Allocation {
                            context: "selected V3 token balances",
                            source,
                        }
                    })?;
                }
                output.push(RecordedTokenBalance {
                    side,
                    balance_index: u32::try_from(balance_index).map_err(|_| {
                        IndexerV3InstructionSourceError::Invalid(
                            "token-balance index exceeds u32".into(),
                        )
                    })?,
                    account_index: balance.account_index,
                    mint,
                    owner,
                    token_program,
                    amount: balance.amount,
                    decimals: balance.decimals,
                });
            }
        }
        Ok(output)
    }

    fn project_requested_message<'a>(
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        requirement: &InstructionDataRequirement,
        relaxed: bool,
        scratch: &mut TransactionProjectionScratch,
        resolve_keys: bool,
    ) -> IndexerV3InstructionSourceResult<ProjectedCompactV2Message<'a>> {
        scratch.account_keys.clear();
        match requirement {
            InstructionDataRequirement::All => {
                let message =
                    Self::project_all_with_vote_retry(context, projector, bytes, relaxed)?;
                if resolve_keys {
                    Self::resolve_static_keys_into(context, &message, &mut scratch.account_keys)?;
                }
                Ok(message)
            }
            InstructionDataRequirement::None => {
                let message =
                    projector.project_with_instruction_data_for_programs(bytes, &[], None)?;
                if resolve_keys {
                    Self::resolve_static_keys_into(context, &message, &mut scratch.account_keys)?;
                }
                Ok(message)
            }
            InstructionDataRequirement::Programs(programs) => {
                let unselected =
                    projector.project_with_instruction_data_for_programs(bytes, &[], None)?;
                scratch.selected_references.clear();
                reserve_exact(
                    &mut scratch.selected_references,
                    unselected.instructions().len(),
                    "selected V3 program references",
                )?;
                for instruction in unselected.instructions() {
                    let index = usize::from(instruction.program_id_index());
                    let reference =
                        *unselected.static_account_keys().get(index).ok_or_else(|| {
                            IndexerV3InstructionSourceError::Invalid(
                                "projected V3 program index is outside static keys".into(),
                            )
                        })?;
                    if context.query_keys.selected(reference, programs).is_some()
                        && !scratch.selected_references.contains(&reference)
                    {
                        scratch.selected_references.push(reference);
                    }
                }
                let message = Self::project_selected_with_vote_retry(
                    context,
                    projector,
                    bytes,
                    &scratch.selected_references,
                    relaxed,
                )?;
                if resolve_keys {
                    Self::resolve_static_keys_into(context, &message, &mut scratch.account_keys)?;
                }
                Ok(message)
            }
        }
    }

    fn resolve_static_keys(
        context: &mut ExactContext,
        message: &ProjectedCompactV2Message<'_>,
    ) -> IndexerV3InstructionSourceResult<Vec<[u8; 32]>> {
        let mut keys = Vec::new();
        Self::resolve_static_keys_into(context, message, &mut keys)?;
        Ok(keys)
    }

    fn resolve_static_keys_into(
        context: &mut ExactContext,
        message: &ProjectedCompactV2Message<'_>,
        keys: &mut Vec<[u8; 32]>,
    ) -> IndexerV3InstructionSourceResult<()> {
        keys.clear();
        reserve_exact(
            keys,
            message.static_account_keys().len(),
            "resolved V3 static account keys",
        )?;
        for reference in message.static_account_keys() {
            keys.push(context.resolve_pubkey(*reference)?);
        }
        Ok(())
    }

    fn project_selected_with_vote_retry<'a>(
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        programs: &[CompactPubkey],
        relaxed: bool,
    ) -> IndexerV3InstructionSourceResult<ProjectedCompactV2Message<'a>> {
        let strict = projector.project_with_instruction_data_for_programs(
            bytes,
            programs,
            context.vote_hashes(),
        );
        match strict {
            Err(error) if needs_vote_hashes(&error) => {
                context.load_vote_hashes()?;
                let retried = projector.project_with_instruction_data_for_programs(
                    bytes,
                    programs,
                    context.vote_hashes(),
                );
                match retried {
                    Err(error) if relaxed && is_relaxable_projection_error(&error) => Ok(projector
                        .project_with_instruction_data_for_programs_relaxed(
                            bytes,
                            programs,
                            context.vote_hashes(),
                        )?),
                    result => Ok(result?),
                }
            }
            Err(error) if relaxed && is_relaxable_projection_error(&error) => Ok(projector
                .project_with_instruction_data_for_programs_relaxed(
                    bytes,
                    programs,
                    context.vote_hashes(),
                )?),
            result => Ok(result?),
        }
    }

    fn project_all_with_vote_retry<'a>(
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        relaxed: bool,
    ) -> IndexerV3InstructionSourceResult<ProjectedCompactV2Message<'a>> {
        let strict = projector.project(bytes, context.vote_hashes());
        match strict {
            Err(error) if needs_vote_hashes(&error) => {
                context.load_vote_hashes()?;
                let retried = projector.project(bytes, context.vote_hashes());
                match retried {
                    Err(error) if relaxed && is_relaxable_projection_error(&error) => {
                        Ok(projector.project_relaxed(bytes, context.vote_hashes())?)
                    }
                    result => Ok(result?),
                }
            }
            Err(error) if relaxed && is_relaxable_projection_error(&error) => {
                Ok(projector.project_relaxed(bytes, context.vote_hashes())?)
            }
            result => Ok(result?),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn project_instructions(
        context: &mut ExactContext,
        request: &ScanRequest,
        message_bytes: &[u8],
        message: &ProjectedCompactV2Message<'_>,
        metadata: &ProjectedMetadata<'_>,
        account_keys: &[[u8; 32]],
        account_key_count: usize,
        signatures: Option<&[[u8; 64]]>,
        include_programs: bool,
    ) -> IndexerV3InstructionSourceResult<Vec<ResolvedInstruction>> {
        let has_selected_ambiguity = message.instructions().iter().any(|instruction| {
            instruction
                .data_candidates()
                .is_some_and(|candidates| candidates.len() > 1)
        });
        let mut selected_outer_data = if has_selected_ambiguity {
            match signatures {
                None if !request.require_complete_instruction_data => Some(
                    SelectedOuterData::Unknown(CoverageReason::InstructionDataUnavailable),
                ),
                None => {
                    return Err(IndexerV3InstructionSourceError::MissingSidecar {
                        object: ARCHIVE_V2_SIGNATURES_FILE,
                        purpose: "ambiguous selected instruction-data proof",
                    });
                }
                Some(signatures) => {
                    match Self::select_exact_outer_data(context, message_bytes, signatures) {
                        Ok(data) => Some(SelectedOuterData::Exact(data)),
                        Err(error)
                            if !request.require_complete_instruction_data
                                && is_missing_instruction_proof(&error) =>
                        {
                            Some(SelectedOuterData::Unknown(
                                CoverageReason::InstructionDataUnavailable,
                            ))
                        }
                        Err(error)
                            if !request.require_complete_instruction_data
                                && is_unresolved_instruction_ambiguity(&error) =>
                        {
                            Some(SelectedOuterData::Unknown(
                                CoverageReason::AmbiguousInstructionData,
                            ))
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        } else {
            None
        };

        let inner_groups = match metadata {
            ProjectedMetadata::Exact(metadata) => metadata.inner_instructions.as_deref(),
            ProjectedMetadata::Absent
            | ProjectedMetadata::Raw
            | ProjectedMetadata::ExactUnprojected => None,
        };
        let inner_count = inner_groups
            .into_iter()
            .flatten()
            .try_fold(0usize, |total, group| {
                total.checked_add(group.instructions.len()).ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 canonical instruction count overflow".into(),
                    )
                })
            })?;
        let output_count = message
            .instructions()
            .len()
            .checked_add(inner_count)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 canonical instruction count overflow".into(),
                )
            })?;
        let mut output = context.output_pool.instructions();
        reserve_exact(&mut output, output_count, "canonical V3 instructions")?;
        let mut next_group = inner_groups.into_iter().flatten().peekable();

        for (outer_index, instruction) in message.instructions().iter().enumerate() {
            if usize::from(instruction.program_id_index()) >= account_key_count {
                return Err(IndexerV3InstructionSourceError::Invalid(
                    "program index exceeds account count".into(),
                ));
            }
            let program_id = if !include_programs {
                None
            } else if request.include_instruction_accounts {
                Some(resolve_index(account_keys, instruction.program_id_index())?)
            } else {
                let reference = projected_account_reference(
                    message,
                    metadata,
                    usize::from(instruction.program_id_index()),
                )?;
                context.project_program(reference, &request.instruction_programs)?
            };
            let accounts = project_instruction_accounts(
                &mut context.output_pool,
                request.include_instruction_accounts,
                account_keys,
                account_key_count,
                instruction.accounts(),
            )?;
            let (data_coverage, data) = match instruction.data_candidates() {
                None => (InstructionDataCoverage::NotRequested, Vec::new()),
                Some([]) => (
                    InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable),
                    Vec::new(),
                ),
                Some(candidates) if candidates.len() == 1 => (
                    InstructionDataCoverage::Exact,
                    context
                        .output_pool
                        .copy_data(&candidates[0].bytes)
                        .map_err(|source| IndexerV3InstructionSourceError::Allocation {
                            context: "exact V3 outer instruction data",
                            source,
                        })?,
                ),
                Some(_) => {
                    let selected = selected_outer_data.as_mut().ok_or_else(|| {
                        IndexerV3InstructionSourceError::Invalid(
                            "ambiguous selected V3 data was not signature-selected".into(),
                        )
                    })?;
                    match selected {
                        SelectedOuterData::Exact(selected) => {
                            let data = selected.get_mut(outer_index).ok_or_else(|| {
                                IndexerV3InstructionSourceError::Invalid(
                                    "selected signed V3 message has the wrong instruction count"
                                        .into(),
                                )
                            })?;
                            (InstructionDataCoverage::Exact, std::mem::take(data))
                        }
                        SelectedOuterData::Unknown(reason) => {
                            (InstructionDataCoverage::Unknown(*reason), Vec::new())
                        }
                    }
                }
            };
            push_instruction(
                &mut output,
                outer_index,
                None,
                None,
                program_id,
                accounts,
                data_coverage,
                data,
            )?;

            if next_group
                .peek()
                .is_some_and(|group| group.outer_instruction_index as usize == outer_index)
            {
                let group = next_group.next().expect("peek proved a V3 CPI group");
                for (inner_index, inner) in group.instructions.iter().enumerate() {
                    if u64::from(inner.program_id_index) >= account_key_count as u64 {
                        return Err(IndexerV3InstructionSourceError::Invalid(
                            "CPI program index exceeds account count".into(),
                        ));
                    }
                    let program_id = if !include_programs {
                        None
                    } else if request.include_instruction_accounts {
                        Some(resolve_index_u32(account_keys, inner.program_id_index)?)
                    } else {
                        let index = usize::try_from(inner.program_id_index).map_err(|_| {
                            IndexerV3InstructionSourceError::Invalid(
                                "V3 CPI account index exceeds address space".into(),
                            )
                        })?;
                        let reference = projected_account_reference(message, metadata, index)?;
                        context.project_program(reference, &request.instruction_programs)?
                    };
                    let accounts = project_instruction_accounts(
                        &mut context.output_pool,
                        request.include_instruction_accounts,
                        account_keys,
                        account_key_count,
                        inner.accounts,
                    )?;
                    let selected = program_id.as_ref().is_some_and(|key| {
                        instruction_data_required(&request.instruction_data, key)
                    });
                    let (data_coverage, data) = if selected {
                        let data = context
                            .output_pool
                            .copy_data(inner.data)
                            .map_err(|source| IndexerV3InstructionSourceError::Allocation {
                                context: "selected V3 CPI data",
                                source,
                            })?;
                        (InstructionDataCoverage::Exact, data)
                    } else {
                        (InstructionDataCoverage::NotRequested, Vec::new())
                    };
                    push_instruction(
                        &mut output,
                        outer_index,
                        Some(inner_index),
                        inner.stack_height,
                        program_id,
                        accounts,
                        data_coverage,
                        data,
                    )?;
                }
            }
        }
        if next_group.next().is_some() {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "V3 metadata CPI group has no matching outer instruction".into(),
            ));
        }
        Ok(output)
    }

    fn select_exact_outer_data(
        context: &mut ExactContext,
        message_bytes: &[u8],
        signatures: &[[u8; 64]],
    ) -> IndexerV3InstructionSourceResult<Vec<Vec<u8>>> {
        let projector =
            CompactV2MessageProjector::new(context.message_schema, context.registry_entries);
        let message = Self::project_all_with_vote_retry(context, projector, message_bytes, false)?;
        let static_keys = Self::resolve_static_keys(context, &message)?;
        let recent_blockhash = match message.recent_blockhash() {
            OwnedCompactRecentBlockhash::Nonce(hash) => *hash,
            OwnedCompactRecentBlockhash::Id(id)
                if *id < 0 && context.sidecars.previous_blockhash_size.is_none() =>
            {
                return Err(IndexerV3InstructionSourceError::MissingSidecar {
                    object: ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
                    purpose: "ambiguous signed-message previous blockhash",
                });
            }
            OwnedCompactRecentBlockhash::Id(id) => context.load_blockhashes()?.resolve(*id)?,
        };
        let resolved_lookups = match message.version() {
            ProjectedCompactV2MessageVersion::V0 {
                address_table_lookups,
            } => {
                let mut lookups = Vec::new();
                reserve_exact(
                    &mut lookups,
                    address_table_lookups.len(),
                    "resolved V3 address-table lookups",
                )?;
                for lookup in address_table_lookups {
                    lookups.push(ResolvedAddressTableLookup {
                        account_key: context.resolve_pubkey(lookup.account_key())?,
                        writable_indexes: lookup.writable_indexes(),
                        readonly_indexes: lookup.readonly_indexes(),
                    });
                }
                lookups
            }
            ProjectedCompactV2MessageVersion::Legacy
            | ProjectedCompactV2MessageVersion::V1 { .. } => Vec::new(),
        };
        let version = match message.version() {
            ProjectedCompactV2MessageVersion::Legacy => SignedMessageVersion::Legacy,
            ProjectedCompactV2MessageVersion::V0 { .. } => SignedMessageVersion::V0 {
                address_table_lookups: &resolved_lookups,
            },
            ProjectedCompactV2MessageVersion::V1 { config } => {
                SignedMessageVersion::V1 { config: *config }
            }
        };
        let mut candidates = Vec::new();
        reserve_exact(
            &mut candidates,
            message.instructions().len(),
            "signed V3 instruction candidates",
        )?;
        for (index, instruction) in message.instructions().iter().enumerate() {
            let data_candidates = instruction.data_candidates().ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(format!(
                    "full V3 signature projection omitted instruction {index} data"
                ))
            })?;
            candidates.push(SignedInstructionCandidates {
                program_id_index: instruction.program_id_index(),
                accounts: instruction.accounts(),
                data_candidates,
            });
        }
        let selected = select_signed_message_candidate_ed25519(
            &SignedMessageCandidates {
                version,
                header: message.header(),
                static_account_keys: &static_keys,
                recent_blockhash,
                instructions: &candidates,
            },
            MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS,
            signatures,
        )?;
        Ok(selected.instruction_data)
    }
}

impl ArchiveInstructionSource for IndexerV3InstructionSource {
    fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_model::Result<ScanReceipt> {
        self.scan_ordered_with_registry_policy(
            request,
            IndexerV3RegistryReadPolicy::sparse_only(),
            sink,
        )
    }
}

fn validate_parallel_worker_count(workers: NonZeroUsize) -> blockzilla_model::Result<()> {
    if workers.get() > MAX_INDEXER_V3_PARALLEL_WORKERS {
        return Err(QueryError::InvalidRequest(format!(
            "parallel V3 workers {} exceeds the {MAX_INDEXER_V3_PARALLEL_WORKERS}-worker limit",
            workers.get()
        )));
    }
    Ok(())
}

fn validate_parallel_job_output(
    identity: &SourceIdentity,
    descriptor: ParallelAssignedDescriptor,
    channel_worker: usize,
    output: &ParallelScanJobOutput,
) -> blockzilla_model::Result<()> {
    if output.worker != channel_worker || output.blocks.len() != descriptor.resources.blocks {
        return Err(QueryError::InvalidStream(
            "parallel V3 result worker or block count differs from its assignment".into(),
        ));
    }
    let mut transactions = 0_u64;
    for (index, block) in output.blocks.iter().enumerate() {
        if block.header.epoch != identity.epoch
            || block.header.block_ordinal != descriptor.block_ordinals[index]
            || block.header.slot != descriptor.slots[index]
        {
            return Err(QueryError::InvalidStream(format!(
                "parallel V3 result block {index} differs from its assignment"
            )));
        }
        if block.transaction_count() != u64::from(descriptor.transaction_counts[index]) {
            return Err(QueryError::InvalidStream(format!(
                "parallel V3 result block {index} transaction count differs from its assignment"
            )));
        }
        transactions = transactions
            .checked_add(block.transaction_count())
            .ok_or_else(|| {
                QueryError::InvalidStream("parallel V3 result transaction count overflow".into())
            })?;
    }
    if transactions != descriptor.resources.transactions {
        return Err(QueryError::InvalidStream(
            "parallel V3 result job transaction count differs from its assignment".into(),
        ));
    }
    Ok(())
}

impl ParallelScanJobPlan {
    fn ordered(
        reader: Arc<Reader>,
        start_block: usize,
        end_block: usize,
        selection: SemanticPlaneSelection,
        include_signatures: bool,
    ) -> Self {
        Self::Ordered {
            reader,
            selection,
            include_signatures,
            next_block: start_block,
            end_block,
            next_id: 0,
        }
    }

    fn selected(
        reader: Arc<Reader>,
        blocks: Vec<usize>,
        selection: SemanticPlaneSelection,
        include_signatures: bool,
    ) -> Self {
        Self::Selected {
            reader,
            selection,
            include_signatures,
            blocks: Arc::from(blocks.into_boxed_slice()),
            next_index: 0,
            next_id: 0,
        }
    }

    fn block_count(&self) -> usize {
        match self {
            Self::Ordered {
                next_block,
                end_block,
                ..
            } => end_block.saturating_sub(*next_block),
            Self::Selected {
                blocks, next_index, ..
            } => blocks.len().saturating_sub(*next_index),
        }
    }

    fn next_job(&mut self) -> blockzilla_model::Result<Option<ParallelScanJob>> {
        match self {
            Self::Ordered {
                reader,
                selection,
                include_signatures,
                next_block,
                end_block,
                next_id,
            } => {
                if *next_block == *end_block {
                    return Ok(None);
                }
                let start = *next_block;
                let mut end = start;
                let mut resources = ParallelScanJobResources::default();
                while end < *end_block && end - start < INDEXER_V3_PARALLEL_BLOCKS_PER_JOB {
                    let block =
                        parallel_block_resources(reader, end, *selection, *include_signatures)?;
                    let combined = resources.checked_add(block)?;
                    if end != start && !combined.fits_global_limits() {
                        break;
                    }
                    resources = combined;
                    end += 1;
                    if !resources.fits_global_limits() {
                        break;
                    }
                }
                let job = ParallelScanJob {
                    id: *next_id,
                    blocks: ParallelScanJobBlocks::Ordered(start..end),
                    resources,
                };
                *next_block = end;
                *next_id += 1;
                Ok(Some(job))
            }
            Self::Selected {
                reader,
                selection,
                include_signatures,
                blocks,
                next_index,
                next_id,
            } => {
                if *next_index == blocks.len() {
                    return Ok(None);
                }
                let start = *next_index;
                let mut end = start;
                let mut resources = ParallelScanJobResources::default();
                while end < blocks.len() && end - start < INDEXER_V3_PARALLEL_BLOCKS_PER_JOB {
                    let block = parallel_block_resources(
                        reader,
                        blocks[end],
                        *selection,
                        *include_signatures,
                    )?;
                    let combined = resources.checked_add(block)?;
                    if end != start && !combined.fits_global_limits() {
                        break;
                    }
                    resources = combined;
                    end += 1;
                    if !resources.fits_global_limits() {
                        break;
                    }
                }
                let job = ParallelScanJob {
                    id: *next_id,
                    blocks: ParallelScanJobBlocks::Selected {
                        all: Arc::clone(blocks),
                        indexes: start..end,
                    },
                    resources,
                };
                *next_index = end;
                *next_id += 1;
                Ok(Some(job))
            }
        }
    }
}

impl ParallelScanJob {
    fn descriptor(
        &self,
        reader: &Reader,
        worker: usize,
    ) -> blockzilla_model::Result<ParallelAssignedDescriptor> {
        let mut block_ordinals = [0_u32; INDEXER_V3_PARALLEL_BLOCKS_PER_JOB];
        let mut slots = [0_u64; INDEXER_V3_PARALLEL_BLOCKS_PER_JOB];
        let mut transaction_counts = [0_u32; INDEXER_V3_PARALLEL_BLOCKS_PER_JOB];
        let mut count = 0_usize;
        let mut record = |ordinal: usize| -> blockzilla_model::Result<()> {
            if count == INDEXER_V3_PARALLEL_BLOCKS_PER_JOB {
                return Err(QueryError::InvalidStream(
                    "parallel V3 job descriptor exceeds its block bound".into(),
                ));
            }
            let row = reader.block(ordinal).ok_or_else(|| {
                QueryError::InvalidStream(format!(
                    "parallel V3 assigned block ordinal {ordinal} is missing"
                ))
            })?;
            block_ordinals[count] = row.block_id;
            slots[count] = row.slot;
            transaction_counts[count] = row.tx_count;
            count += 1;
            Ok(())
        };
        match &self.blocks {
            ParallelScanJobBlocks::Ordered(range) => {
                for ordinal in range.clone() {
                    record(ordinal)?;
                }
            }
            ParallelScanJobBlocks::Selected { all, indexes } => {
                for &ordinal in &all[indexes.clone()] {
                    record(ordinal)?;
                }
            }
        }
        if count != self.resources.blocks {
            return Err(QueryError::InvalidStream(
                "parallel V3 job descriptor block count is invalid".into(),
            ));
        }
        Ok(ParallelAssignedDescriptor {
            worker,
            resources: self.resources,
            block_ordinals,
            slots,
            transaction_counts,
        })
    }
}

fn parallel_block_resources(
    reader: &Reader,
    block_ordinal: usize,
    selection: SemanticPlaneSelection,
    include_signatures: bool,
) -> blockzilla_model::Result<ParallelScanJobResources> {
    let row = reader.block(block_ordinal).ok_or_else(|| {
        QueryError::InvalidStream(format!(
            "parallel V3 resource block ordinal {block_ordinal} is missing"
        ))
    })?;
    let semantic_bytes = Object::ALL
        .into_iter()
        .filter(|object| selection.includes(*object))
        .try_fold(0_u64, |total, object| {
            total
                .checked_add(u64::from(row.locators[object.index()].decoded_len))
                .ok_or_else(|| {
                    QueryError::InvalidStream("parallel V3 declared semantic bytes overflow".into())
                })
        })?;
    let signature_bytes = if include_signatures {
        u64::from(row.signature_count)
            .checked_mul(SIGNATURE_BYTES as u64)
            .ok_or_else(|| {
                QueryError::InvalidStream("parallel V3 declared signature bytes overflow".into())
            })?
    } else {
        0
    };
    Ok(ParallelScanJobResources {
        blocks: 1,
        declared_decoded_bytes: semantic_bytes.checked_add(signature_bytes).ok_or_else(|| {
            QueryError::InvalidStream("parallel V3 declared decoded bytes overflow".into())
        })?,
        transactions: u64::from(row.tx_count),
    })
}

fn merge_registry_receipts(
    first: IndexerV3RegistryReadReceipt,
    second: Option<IndexerV3RegistryReadReceipt>,
    shared_full_registry: bool,
) -> blockzilla_model::Result<IndexerV3RegistryReadReceipt> {
    let Some(second) = second else {
        return Ok(first);
    };
    let add = |left: u64, right: u64, label: &str| {
        left.checked_add(right).ok_or_else(|| {
            QueryError::InvalidStream(format!("parallel V3 registry {label} count overflow"))
        })
    };
    let mode = match (first.mode, second.mode) {
        (IndexerV3RegistryReadMode::FullRegistry, _)
        | (_, IndexerV3RegistryReadMode::FullRegistry) => IndexerV3RegistryReadMode::FullRegistry,
        (IndexerV3RegistryReadMode::SparseChunkCache, _)
        | (_, IndexerV3RegistryReadMode::SparseChunkCache) => {
            IndexerV3RegistryReadMode::SparseChunkCache
        }
        _ => IndexerV3RegistryReadMode::Unused,
    };
    let resident_payload_bytes =
        if shared_full_registry || mode == IndexerV3RegistryReadMode::FullRegistry {
            first
                .resident_payload_bytes
                .max(second.resident_payload_bytes)
        } else {
            add(
                first.resident_payload_bytes,
                second.resident_payload_bytes,
                "resident payload byte",
            )?
        };
    Ok(IndexerV3RegistryReadReceipt {
        mode,
        prefetch_read_calls: add(
            first.prefetch_read_calls,
            second.prefetch_read_calls,
            "prefetch read-call",
        )?,
        prefetch_read_bytes: add(
            first.prefetch_read_bytes,
            second.prefetch_read_bytes,
            "prefetch read-byte",
        )?,
        resolutions: add(first.resolutions, second.resolutions, "resolution")?,
        hits: add(first.hits, second.hits, "hit")?,
        misses: add(first.misses, second.misses, "miss")?,
        evictions: add(first.evictions, second.evictions, "eviction")?,
        resident_payload_bytes,
    })
}

fn validated_requested_bounds(
    identity: &SourceIdentity,
    request: &ScanRequest,
) -> blockzilla_model::Result<(u32, u32)> {
    // This call is required even when the selected block list is empty.
    validate_request(identity, request)?;
    let Some(range) = request.range else {
        return Ok((0, identity.block_count));
    };
    let end = range
        .first_block
        .checked_add(range.block_count.get())
        .ok_or_else(|| QueryError::InvalidRequest("V3 block range overflows u32".into()))?;
    Ok((range.first_block, end))
}

fn validate_selected_blocks(
    candidates: &[u32],
    requested_start: u32,
    requested_end: u32,
    archive_blocks: u32,
) -> blockzilla_model::Result<Vec<usize>> {
    let mut selected = Vec::new();
    selected
        .try_reserve_exact(candidates.len())
        .map_err(|source| {
            source_error(IndexerV3InstructionSourceError::Allocation {
                context: "V3 selective block candidates",
                source,
            })
        })?;
    let mut previous = None;
    for &candidate in candidates {
        if previous.is_some_and(|prior| candidate <= prior) {
            return Err(QueryError::InvalidRequest(
                "V3 candidate blocks are not strictly increasing".into(),
            ));
        }
        if candidate >= archive_blocks {
            return Err(QueryError::InvalidRequest(format!(
                "V3 candidate block {candidate} is outside the {archive_blocks}-block archive"
            )));
        }
        if candidate < requested_start || candidate >= requested_end {
            return Err(QueryError::InvalidRequest(format!(
                "V3 candidate block {candidate} is outside requested blocks {requested_start}..{requested_end}"
            )));
        }
        selected.push(usize::try_from(candidate).map_err(|_| {
            QueryError::InvalidRequest(format!(
                "V3 candidate block {candidate} exceeds this address space"
            ))
        })?);
        previous = Some(candidate);
    }
    Ok(selected)
}

fn transaction_count_for_blocks(
    reader: &Reader,
    blocks: impl IntoIterator<Item = usize>,
) -> IndexerV3InstructionSourceResult<u64> {
    blocks.into_iter().try_fold(0_u64, |total, ordinal| {
        let row = reader.block(ordinal).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(format!(
                "V3 block ordinal {ordinal} is missing while counting the scan universe"
            ))
        })?;
        total.checked_add(u64::from(row.tx_count)).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 scan-universe transaction count overflow".into(),
            )
        })
    })
}

fn accumulate_scan_receipt(
    total: &mut ScanReceipt,
    next: ScanReceipt,
) -> blockzilla_model::Result<()> {
    if next.io != ScanIoReceipt::default() {
        return Err(QueryError::InvalidStream(
            "one-block V3 publisher returned unexpected I/O counters".into(),
        ));
    }
    add_scan_count(&mut total.blocks, next.blocks, "block")?;
    add_scan_count(&mut total.transactions, next.transactions, "transaction")?;
    add_scan_count(&mut total.instructions, next.instructions, "instruction")?;
    add_scan_count(
        &mut total.instructions_not_requested,
        next.instructions_not_requested,
        "not-requested instruction",
    )?;
    add_scan_count(
        &mut total.instructions_with_unknown_data,
        next.instructions_with_unknown_data,
        "unknown-data instruction",
    )?;
    add_scan_count(
        &mut total.transactions_with_incomplete_instructions,
        next.transactions_with_incomplete_instructions,
        "incomplete-instruction transaction",
    )?;
    add_scan_count(
        &mut total.transactions_with_incomplete_cpi,
        next.transactions_with_incomplete_cpi,
        "incomplete-CPI transaction",
    )?;
    add_scan_count(
        &mut total.transactions_with_unknown_execution,
        next.transactions_with_unknown_execution,
        "unknown-execution transaction",
    )?;
    add_scan_count(
        &mut total.transactions_with_incomplete_token_balances,
        next.transactions_with_incomplete_token_balances,
        "incomplete-token-balance transaction",
    )?;
    Ok(())
}

fn add_scan_count(
    total: &mut u64,
    value: u64,
    label: &'static str,
) -> blockzilla_model::Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| QueryError::InvalidStream(format!("V3 selective {label} count overflow")))?;
    Ok(())
}

fn validate_rows(
    reader: &Reader,
    first_slot: u64,
    last_slot: u64,
    block_count: u32,
) -> IndexerV3InstructionSourceResult<()> {
    let mut previous_slot = None;
    let mut expected_first_transaction = 0_u64;
    for ordinal in 0..block_count {
        let row = reader.block(ordinal as usize).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(format!(
                "V3 row {ordinal} is missing from the validated index"
            ))
        })?;
        if row.block_id != ordinal {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 row {ordinal} has block ID {}",
                row.block_id
            )));
        }
        if row.slot < first_slot || row.slot > last_slot {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 block {} slot {} is outside explicit epoch slots {first_slot}..={last_slot}",
                row.block_id, row.slot
            )));
        }
        if previous_slot.is_some_and(|prior| row.slot <= prior) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 block {} slot {} is not after the previous slot",
                row.block_id, row.slot
            )));
        }
        advance_transaction_geometry(row, &mut expected_first_transaction)?;
        previous_slot = Some(row.slot);
    }
    if reader.block(block_count as usize).is_some() {
        return Err(IndexerV3InstructionSourceError::Invalid(
            "V3 reader exposes rows beyond its header block count".into(),
        ));
    }
    validate_selected_transaction_total(
        expected_first_transaction,
        reader.header.selected_transactions,
    )?;
    Ok(())
}

fn advance_transaction_geometry(
    row: &BlockRow,
    expected_first_transaction: &mut u64,
) -> IndexerV3InstructionSourceResult<()> {
    if row.first_tx_ordinal != *expected_first_transaction {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "V3 block {} starts at transaction {}, expected {}",
            row.block_id, row.first_tx_ordinal, *expected_first_transaction
        )));
    }
    *expected_first_transaction = expected_first_transaction
        .checked_add(u64::from(row.tx_count))
        .ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 selected transaction geometry overflows u64".into(),
            )
        })?;
    Ok(())
}

fn validate_selected_transaction_total(
    row_transactions: u64,
    declared_transactions: u64,
) -> IndexerV3InstructionSourceResult<()> {
    if row_transactions != declared_transactions {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "V3 block rows contain {row_transactions} transactions, but the header declares {declared_transactions}"
        )));
    }
    Ok(())
}

fn local_source_objects() -> Vec<&'static str> {
    let mut objects = Vec::with_capacity(18);
    objects.extend(indexer_v3_required_ledger_objects());
    objects.extend(INDEXER_V3_REQUIRED_RETAINED_SIDECARS);
    objects.extend(INDEXER_V3_OPTIONAL_RETAINED_SIDECARS);
    objects.push(blockzilla_archive_v2::ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    objects
}

fn registry_entries(size: u64) -> IndexerV3InstructionSourceResult<u32> {
    if !size.is_multiple_of(REGISTRY_KEY_BYTES as u64) {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "{} has a partial public-key record",
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE
        )));
    }
    u32::try_from(size / REGISTRY_KEY_BYTES as u64).map_err(|_| {
        IndexerV3InstructionSourceError::Invalid(format!(
            "{} entry count exceeds u32",
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE
        ))
    })
}

#[derive(Debug, Clone, Copy)]
struct SidecarGeometry {
    registry_size: u64,
    signatures_size: Option<u64>,
    blockhash_size: Option<u64>,
    previous_blockhash_size: Option<u64>,
    vote_hash_size: Option<u64>,
}

impl SidecarGeometry {
    fn inspect(
        source: &dyn RangeSource,
        reader: &Reader,
        scope: IndexerV3SourceScope,
    ) -> IndexerV3InstructionSourceResult<Self> {
        let registry_size = source.size(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)?.ok_or(
            IndexerV3InstructionSourceError::MissingSidecar {
                object: ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
                purpose: "CompactPubkey resolution",
            },
        )?;
        registry_entries(registry_size)?;

        let total_signatures = reader
            .header
            .selected_blocks
            .checked_sub(1)
            .and_then(|ordinal| reader.block(ordinal as usize))
            .map_or(Ok(0u64), |row| {
                row.first_signature_ordinal
                    .checked_add(u64::from(row.signature_count))
                    .ok_or_else(|| {
                        IndexerV3InstructionSourceError::Invalid(
                            "V3 total signature count overflows u64".into(),
                        )
                    })
            })?;
        let expected_signature_bytes = total_signatures
            .checked_mul(SIGNATURE_BYTES as u64)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 total signature byte length overflows u64".into(),
                )
            })?;
        let signatures_size = source.size(ARCHIVE_V2_SIGNATURES_FILE)?;
        if signatures_size.is_some_and(|size| {
            !size.is_multiple_of(SIGNATURE_BYTES as u64)
                || match scope {
                    IndexerV3SourceScope::SelectedPrefix => size < expected_signature_bytes,
                    IndexerV3SourceScope::FullSelection => size != expected_signature_bytes,
                }
        }) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "{} length {:?} does not match the {:?} V3 signature geometry ending at {expected_signature_bytes}",
                ARCHIVE_V2_SIGNATURES_FILE, signatures_size, scope
            )));
        }

        let maximum_blockhash_bytes = reader
            .header
            .slots_per_epoch
            .checked_add(1)
            .and_then(|count| count.checked_mul(BLOCKHASH_RECORD_LEN as u64))
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 blockhash sidecar bound overflows u64".into(),
                )
            })?;
        let blockhash_size = source.size(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)?;
        if blockhash_size.is_some_and(|size| {
            size > maximum_blockhash_bytes
                || size > MAX_BLOCKHASH_REGISTRY_BYTES as u64
                || !size.is_multiple_of(BLOCKHASH_RECORD_LEN as u64)
        }) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "{} has invalid V3 geometry",
                ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE
            )));
        }

        let previous_blockhash_size = source.size(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)?;
        let expected_previous =
            (PREVIOUS_BLOCKHASH_RECORDS * PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN) as u64;
        if previous_blockhash_size.is_some_and(|size| size != expected_previous) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "{} is {previous_blockhash_size:?}, expected {expected_previous} bytes",
                ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE
            )));
        }

        let maximum_vote_bytes = reader
            .header
            .slots_per_epoch
            .checked_mul(VOTE_HASH_RECORD_LEN as u64)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 vote-hash sidecar bound overflows u64".into(),
                )
            })?;
        let vote_hash_size = source.size(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE)?;
        if vote_hash_size.is_some_and(|size| {
            size > maximum_vote_bytes
                || size > MAX_VOTE_HASH_REGISTRY_BYTES as u64
                || !size.is_multiple_of(VOTE_HASH_RECORD_LEN as u64)
        }) {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "{} has invalid V3 geometry",
                ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE
            )));
        }

        Ok(Self {
            registry_size,
            signatures_size,
            blockhash_size,
            previous_blockhash_size,
            vote_hash_size,
        })
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct CountingRangeSourceStats {
    calls: u64,
    bytes: u64,
}

impl CountingRangeSourceStats {
    fn difference(self, before: Self) -> IndexerV3InstructionSourceResult<Self> {
        Ok(Self {
            calls: self.calls.checked_sub(before.calls).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 source read count moved backwards".into(),
                )
            })?,
            bytes: self.bytes.checked_sub(before.bytes).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 source read bytes moved backwards".into(),
                )
            })?,
        })
    }
}

struct CountingRangeSource {
    inner: Arc<dyn RangeSource>,
    stats: Mutex<CountingRangeSourceStats>,
}

impl CountingRangeSource {
    fn new(inner: Arc<dyn RangeSource>) -> Self {
        Self {
            inner,
            stats: Mutex::new(CountingRangeSourceStats::default()),
        }
    }

    fn stats(&self) -> IndexerV3InstructionSourceResult<CountingRangeSourceStats> {
        self.stats.lock().map(|stats| *stats).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid("V3 source read counter is poisoned".into())
        })
    }

    fn record(&self, bytes: usize) -> blockzilla_source::SourceResult<()> {
        let bytes = u64::try_from(bytes).map_err(|_| {
            blockzilla_source::SourceError::Protocol("V3 returned-byte count exceeds u64".into())
        })?;
        let mut stats = self.stats.lock().map_err(|_| {
            blockzilla_source::SourceError::Protocol("V3 source read counter is poisoned".into())
        })?;
        stats.calls = stats.calls.checked_add(1).ok_or_else(|| {
            blockzilla_source::SourceError::Protocol("V3 source read-call count overflow".into())
        })?;
        stats.bytes = stats.bytes.checked_add(bytes).ok_or_else(|| {
            blockzilla_source::SourceError::Protocol("V3 source read-byte count overflow".into())
        })?;
        Ok(())
    }
}

impl RangeSource for CountingRangeSource {
    fn size(&self, object: &str) -> blockzilla_source::SourceResult<Option<u64>> {
        self.inner.size(object)
    }

    fn read_range(
        &self,
        object: &str,
        offset: u64,
        length: usize,
    ) -> blockzilla_source::SourceResult<Vec<u8>> {
        let bytes = self.inner.read_range(object, offset, length)?;
        self.record(bytes.len())?;
        Ok(bytes)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> blockzilla_source::SourceResult<()> {
        self.inner
            .read_range_into(object, offset, length, destination)?;
        self.record(destination.len())
    }

    fn read_range_into_slice(
        &self,
        object: &str,
        offset: u64,
        destination: &mut [u8],
    ) -> blockzilla_source::SourceResult<()> {
        self.inner
            .read_range_into_slice(object, offset, destination)?;
        self.record(destination.len())
    }
}

/// Bounded signature windows for an ordered set of selected V3 blocks.
///
/// Contiguous selected blocks share one range read until the 32 MiB request
/// cap. A gap ends the batch, so a later sparse scan can reuse this helper
/// without fetching signatures for skipped blocks.
pub(crate) struct SelectedBlockSignatureReader<'a> {
    reader: &'a Reader,
    source: Arc<dyn RangeSource>,
    signatures_available: bool,
    selected_blocks: ParallelScanJobBlocks,
    next_selected: usize,
    batch: Option<SignatureBatch>,
}

struct SignatureBatch {
    block_range: Range<usize>,
    first_signature_ordinal: u64,
    signatures: Vec<[u8; SIGNATURE_BYTES]>,
}

impl<'a> SelectedBlockSignatureReader<'a> {
    pub(crate) fn for_contiguous_range(
        reader: &'a Reader,
        source: Arc<dyn RangeSource>,
        signatures_available: bool,
        blocks: Range<usize>,
    ) -> IndexerV3InstructionSourceResult<Self> {
        if blocks.start > blocks.end || blocks.end > reader.header.selected_blocks as usize {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "V3 signature range is outside the archive".into(),
            ));
        }
        Self::for_blocks(
            reader,
            source,
            signatures_available,
            ParallelScanJobBlocks::Ordered(blocks),
        )
    }

    /// Prepare a strictly increasing sparse block selection.
    ///
    /// This constructor does no source read. It is ready for the adaptive
    /// reverse path to supply exact candidate block ordinals later.
    pub(crate) fn for_selected_blocks(
        reader: &'a Reader,
        source: Arc<dyn RangeSource>,
        signatures_available: bool,
        selected_blocks: Vec<usize>,
    ) -> IndexerV3InstructionSourceResult<Self> {
        let indexes = 0..selected_blocks.len();
        Self::for_blocks(
            reader,
            source,
            signatures_available,
            ParallelScanJobBlocks::Selected {
                all: selected_blocks.into(),
                indexes,
            },
        )
    }

    fn for_blocks(
        reader: &'a Reader,
        source: Arc<dyn RangeSource>,
        signatures_available: bool,
        selected_blocks: ParallelScanJobBlocks,
    ) -> IndexerV3InstructionSourceResult<Self> {
        let mut previous = None;
        for block in selected_blocks.iter() {
            if reader.block(block).is_none() {
                return Err(IndexerV3InstructionSourceError::Invalid(format!(
                    "selected V3 signature block {block} is outside the archive"
                )));
            }
            if previous.is_some_and(|prior| block <= prior) {
                return Err(IndexerV3InstructionSourceError::Invalid(
                    "selected V3 signature blocks are not strictly increasing".into(),
                ));
            }
            previous = Some(block);
        }
        Ok(Self {
            reader,
            source,
            signatures_available,
            selected_blocks,
            next_selected: 0,
            batch: None,
        })
    }

    pub(crate) fn read_block(
        &mut self,
        block_ordinal: usize,
    ) -> IndexerV3InstructionSourceResult<Option<&[[u8; SIGNATURE_BYTES]]>> {
        let expected = self
            .selected_blocks
            .get(self.next_selected)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 signature scan received too many selected blocks".into(),
                )
            })?;
        if block_ordinal != expected {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 signature scan received block {block_ordinal}, expected {expected}"
            )));
        }
        if !self.signatures_available {
            self.next_selected += 1;
            return Ok(None);
        }
        if self
            .batch
            .as_ref()
            .is_none_or(|batch| !batch.block_range.contains(&block_ordinal))
        {
            self.load_batch()?;
        }
        let row = self.reader.block(block_ordinal).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(format!(
                "V3 signature block {block_ordinal} disappeared"
            ))
        })?;
        let batch = self.batch.as_ref().ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid("V3 signature batch is missing".into())
        })?;
        let row_end = row
            .first_signature_ordinal
            .checked_add(u64::from(row.signature_count))
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 block signature ordinal end overflow".into(),
                )
            })?;
        let start = row
            .first_signature_ordinal
            .checked_sub(batch.first_signature_ordinal)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 block signature range starts before its batch".into(),
                )
            })?;
        let end = row_end
            .checked_sub(batch.first_signature_ordinal)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 block signature range ends before its batch".into(),
                )
            })?;
        let selected = batch.signatures.get(start..end).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 block signature range exceeds its loaded batch".into(),
            )
        })?;
        self.next_selected += 1;
        Ok(Some(selected))
    }

    pub(crate) fn finish(self) -> IndexerV3InstructionSourceResult<()> {
        if self.next_selected != self.selected_blocks.len() {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "V3 signature scan ended before all selected blocks".into(),
            ));
        }
        Ok(())
    }

    fn load_batch(&mut self) -> IndexerV3InstructionSourceResult<()> {
        let first_block = self
            .selected_blocks
            .get(self.next_selected)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 signature batch has no first block".into(),
                )
            })?;
        let first_row = self.reader.block(first_block).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 signature batch starts outside the archive".into(),
            )
        })?;
        let first_signature_ordinal = first_row.first_signature_ordinal;
        let mut expected_signature_ordinal = first_signature_ordinal;
        let mut expected_block = first_block;
        let mut selected_end = self.next_selected;
        let mut block_end = first_block;

        while let Some(block_ordinal) = self.selected_blocks.get(selected_end) {
            if block_ordinal != expected_block {
                break;
            }
            let row = self.reader.block(block_ordinal).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "selected V3 signature block is outside the archive".into(),
                )
            })?;
            if row.first_signature_ordinal != expected_signature_ordinal {
                return Err(IndexerV3InstructionSourceError::Invalid(format!(
                    "V3 block {} signature ordinals are not contiguous",
                    row.block_id
                )));
            }
            validate_signature_row(row)?;
            let next_signature_ordinal = expected_signature_ordinal
                .checked_add(u64::from(row.signature_count))
                .ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 signature batch ordinal end overflow".into(),
                    )
                })?;
            let candidate_bytes = next_signature_ordinal
                .checked_sub(first_signature_ordinal)
                .and_then(|count| count.checked_mul(SIGNATURE_BYTES as u64))
                .and_then(|bytes| usize::try_from(bytes).ok())
                .ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 signature batch byte length overflow".into(),
                    )
                })?;
            if selected_end > self.next_selected && candidate_bytes > MAX_SIGNATURE_BATCH_BYTES {
                break;
            }
            expected_signature_ordinal = next_signature_ordinal;
            expected_block = expected_block.checked_add(1).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 signature block ordinal overflow".into(),
                )
            })?;
            selected_end += 1;
            block_end = expected_block;
            if candidate_bytes > MAX_SIGNATURE_BATCH_BYTES {
                // One valid large block is an isolated batch.
                break;
            }
        }
        if selected_end == self.next_selected {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "V3 signature batch planner made no progress".into(),
            ));
        }

        let signature_count = expected_signature_ordinal
            .checked_sub(first_signature_ordinal)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 signature batch record count overflow".into(),
                )
            })?;
        let mut signatures = self
            .batch
            .take()
            .map_or_else(Vec::new, |batch| batch.signatures);
        let additional = signature_count.saturating_sub(signatures.len());
        reserve_exact(&mut signatures, additional, "V3 signature batch records")?;
        signatures.resize(signature_count, [0; SIGNATURE_BYTES]);
        let total_bytes = signature_count
            .checked_mul(SIGNATURE_BYTES)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 signature batch byte length overflow".into(),
                )
            })?;
        let mut read_bytes = 0usize;
        while read_bytes < total_bytes {
            let length = (total_bytes - read_bytes).min(MAX_SIGNATURE_BATCH_BYTES);
            let offset = first_signature_ordinal
                .checked_mul(SIGNATURE_BYTES as u64)
                .and_then(|offset| offset.checked_add(read_bytes as u64))
                .ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 signature batch read offset overflow".into(),
                    )
                })?;
            self.source.read_range_into_slice(
                ARCHIVE_V2_SIGNATURES_FILE,
                offset,
                &mut signatures.as_flattened_mut()[read_bytes..read_bytes + length],
            )?;
            read_bytes += length;
        }
        if signatures.len() != signature_count {
            return Err(IndexerV3InstructionSourceError::Invalid(
                "V3 signature batch record count differs from its block rows".into(),
            ));
        }
        self.batch = Some(SignatureBatch {
            block_range: first_block..block_end,
            first_signature_ordinal,
            signatures,
        });
        Ok(())
    }
}

fn validate_signature_row(row: &BlockRow) -> IndexerV3InstructionSourceResult<()> {
    let length = usize::try_from(row.signature_count)
        .ok()
        .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
        .ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 block signature byte length overflow".into(),
            )
        })?;
    let row_bound = usize::try_from(row.tx_count)
        .ok()
        .and_then(|count| count.checked_mul(usize::from(u8::MAX)))
        .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
        .unwrap_or(usize::MAX)
        .min(MAX_SIGNATURE_BYTES_PER_BLOCK);
    if length > row_bound {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "V3 block {} signature window is {length} bytes, above {row_bound}",
            row.block_id
        )));
    }
    Ok(())
}

fn should_prefetch_full_registry(
    policy: IndexerV3RegistryReadPolicy,
    requested_transactions: u64,
    candidate_transactions: u64,
    registry_size: u64,
) -> bool {
    let half_requested = requested_transactions / 2 + requested_transactions % 2;
    policy.max_full_registry_bytes != 0
        && candidate_transactions >= policy.min_candidate_transactions
        && candidate_transactions <= requested_transactions
        && candidate_transactions >= half_requested
        && registry_size <= policy.max_full_registry_bytes
}

fn registry_prefetch_read_length(remaining: usize) -> usize {
    remaining.min(MAX_REGISTRY_PREFETCH_READ_BYTES)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RegistryReadStats {
    prefetch_read_calls: u64,
    prefetch_read_bytes: u64,
    resolutions: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl RegistryReadStats {
    fn difference(self, before: Self) -> IndexerV3InstructionSourceResult<Self> {
        let difference = |after: u64, before: u64, name: &str| {
            after.checked_sub(before).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(format!(
                    "V3 registry {name} count moved backwards"
                ))
            })
        };
        Ok(Self {
            prefetch_read_calls: difference(
                self.prefetch_read_calls,
                before.prefetch_read_calls,
                "prefetch read-call",
            )?,
            prefetch_read_bytes: difference(
                self.prefetch_read_bytes,
                before.prefetch_read_bytes,
                "prefetch read-byte",
            )?,
            resolutions: difference(self.resolutions, before.resolutions, "resolution")?,
            hits: difference(self.hits, before.hits, "hit")?,
            misses: difference(self.misses, before.misses, "miss")?,
            evictions: difference(self.evictions, before.evictions, "eviction")?,
        })
    }
}

#[derive(Default)]
struct SharedExactSidecars {
    vote_hashes_loaded: bool,
    vote_hashes: Option<Arc<VoteHashRegistry>>,
    vote_hashes_failure: Option<Arc<IndexerV3InstructionSourceError>>,
    blockhashes: Option<Arc<BlockhashResolver>>,
    blockhashes_failure: Option<Arc<IndexerV3InstructionSourceError>>,
}

struct ExactContext {
    output_pool: blockzilla_model::projection_pool::ProjectionPool,
    query_keys: Arc<blockzilla_compact_v2_reader::query_keys::BoundQueryKeys>,
    source: Arc<dyn RangeSource>,
    registry_entries: u32,
    sidecars: SidecarGeometry,
    // One dense registry image is shared by all parallel projection contexts.
    // Sparse chunk caches remain worker-local and bounded.
    full_registry: Option<Arc<[u8]>>,
    // Keep registry chunks in their fixed-width wire form. A chunk can then be
    // filled directly by the range source without a second decoded-key Vec.
    registry_chunks: HashMap<u32, Vec<u8>>,
    registry_lru: VecDeque<u32>,
    registry_stats: RegistryReadStats,
    // Exact-data workers share each large proof sidecar after one lazy load.
    // Each context keeps a local Arc so projection can borrow the resolver
    // without holding this mutex on the transaction hot path.
    shared_sidecars: Arc<Mutex<SharedExactSidecars>>,
    vote_hashes_loaded: bool,
    vote_hashes: Option<Arc<VoteHashRegistry>>,
    blockhashes: Option<Arc<BlockhashResolver>>,
    message_schema: blockzilla_compact_v2_reader::CompactV2MessageSchema,
}

impl std::fmt::Debug for ExactContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExactContext")
            .field("registry_entries", &self.registry_entries)
            .field("sidecars", &self.sidecars)
            .field(
                "full_registry_bytes",
                &self.full_registry.as_ref().map_or(0, |bytes| bytes.len()),
            )
            .field("registry_chunks", &self.registry_chunks.len())
            .field("vote_hashes_loaded", &self.vote_hashes_loaded)
            .field("blockhashes_loaded", &self.blockhashes.is_some())
            .finish_non_exhaustive()
    }
}

impl ExactContext {
    fn new(
        source: Arc<dyn RangeSource>,
        registry_entries: u32,
        sidecars: SidecarGeometry,
        message_schema: blockzilla_compact_v2_reader::CompactV2MessageSchema,
    ) -> Self {
        Self {
            source,
            registry_entries,
            sidecars,
            query_keys: Arc::default(),
            output_pool: Default::default(),
            full_registry: None,
            registry_chunks: HashMap::new(),
            registry_lru: VecDeque::new(),
            registry_stats: RegistryReadStats::default(),
            shared_sidecars: Arc::new(Mutex::new(SharedExactSidecars::default())),
            vote_hashes_loaded: false,
            vote_hashes: None,
            blockhashes: None,
            message_schema,
        }
    }

    fn fork_for_parallel_scan(&self) -> Self {
        Self {
            source: Arc::clone(&self.source),
            query_keys: Arc::clone(&self.query_keys),
            output_pool: Default::default(),
            registry_entries: self.registry_entries,
            sidecars: self.sidecars,
            full_registry: self.full_registry.clone(),
            registry_chunks: HashMap::new(),
            registry_lru: VecDeque::new(),
            registry_stats: RegistryReadStats::default(),
            shared_sidecars: Arc::clone(&self.shared_sidecars),
            vote_hashes_loaded: false,
            vote_hashes: None,
            blockhashes: None,
            message_schema: self.message_schema,
        }
    }

    fn project_program(
        &mut self,
        reference: CompactPubkey,
        requirement: &InstructionDataRequirement,
    ) -> IndexerV3InstructionSourceResult<Option<[u8; 32]>> {
        match requirement {
            InstructionDataRequirement::None => Ok(None),
            InstructionDataRequirement::Programs(keys) => {
                Ok(self.query_keys.selected(reference, keys))
            }
            InstructionDataRequirement::All => self.resolve_pubkey(reference).map(Some),
        }
    }

    fn resolve_pubkey(
        &mut self,
        reference: CompactPubkey,
    ) -> IndexerV3InstructionSourceResult<[u8; 32]> {
        let CompactPubkey::Id(id) = reference else {
            let CompactPubkey::Raw(pubkey) = reference else {
                unreachable!("CompactPubkey has only raw and ID forms")
            };
            return Ok(pubkey);
        };
        self.registry_stats.resolutions = self
            .registry_stats
            .resolutions
            .checked_add(1)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 registry resolution count overflow".into(),
                )
            })?;
        if id == 0 || id > self.registry_entries {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 registry ID {id} is outside 1..={}",
                self.registry_entries
            )));
        }
        let zero_based = usize::try_from(id - 1).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid("V3 registry ID exceeds address space".into())
        })?;
        if let Some(registry) = self.full_registry.as_ref() {
            self.registry_stats.hits =
                self.registry_stats.hits.checked_add(1).ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 registry hit count overflow".into(),
                    )
                })?;
            let start = zero_based.checked_mul(REGISTRY_KEY_BYTES).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 full-registry byte offset overflow".into(),
                )
            })?;
            let end = start.checked_add(REGISTRY_KEY_BYTES).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 full-registry byte end overflow".into(),
                )
            })?;
            let bytes = registry.get(start..end).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(format!(
                    "V3 registry ID {id} is outside the prefetched registry"
                ))
            })?;
            let mut key = [0_u8; REGISTRY_KEY_BYTES];
            key.copy_from_slice(bytes);
            return Ok(key);
        }
        let chunk_id = u32::try_from(zero_based / REGISTRY_KEYS_PER_CHUNK).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid("V3 registry chunk ID exceeds u32".into())
        })?;
        if self.registry_chunks.contains_key(&chunk_id) {
            self.registry_stats.hits =
                self.registry_stats.hits.checked_add(1).ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 registry hit count overflow".into(),
                    )
                })?;
        } else {
            self.registry_stats.misses =
                self.registry_stats.misses.checked_add(1).ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 registry miss count overflow".into(),
                    )
                })?;
        }
        self.ensure_registry_chunk(chunk_id)?;
        self.touch_registry_chunk(chunk_id);
        let index = zero_based % REGISTRY_KEYS_PER_CHUNK;
        let start = index.checked_mul(REGISTRY_KEY_BYTES).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 registry chunk byte offset overflow".into(),
            )
        })?;
        let end = start.checked_add(REGISTRY_KEY_BYTES).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid("V3 registry chunk byte end overflow".into())
        })?;
        let bytes = self
            .registry_chunks
            .get(&chunk_id)
            .and_then(|chunk| chunk.get(start..end))
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(format!(
                    "V3 registry ID {id} is outside its loaded chunk"
                ))
            })?;
        let mut key = [0_u8; REGISTRY_KEY_BYTES];
        key.copy_from_slice(bytes);
        Ok(key)
    }

    fn ensure_registry_chunk(&mut self, chunk_id: u32) -> IndexerV3InstructionSourceResult<()> {
        if self.registry_chunks.contains_key(&chunk_id) {
            return Ok(());
        }
        let first_key = usize::try_from(chunk_id)
            .ok()
            .and_then(|chunk| chunk.checked_mul(REGISTRY_KEYS_PER_CHUNK))
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid("V3 registry chunk offset overflow".into())
            })?;
        let entries = usize::try_from(self.registry_entries).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 registry entry count exceeds address space".into(),
            )
        })?;
        let key_count = entries
            .saturating_sub(first_key)
            .min(REGISTRY_KEYS_PER_CHUNK);
        if key_count == 0 {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 registry chunk {chunk_id} is outside the registry"
            )));
        }
        let offset = u64::try_from(first_key)
            .ok()
            .and_then(|key| key.checked_mul(REGISTRY_KEY_BYTES as u64))
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid("V3 registry byte offset overflow".into())
            })?;
        let length = key_count.checked_mul(REGISTRY_KEY_BYTES).ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid("V3 registry chunk length overflow".into())
        })?;
        let mut bytes = if self.registry_chunks.len() == REGISTRY_CACHE_CHUNKS {
            let evicted = self.registry_lru.pop_front().ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 registry chunk cache has no eviction candidate".into(),
                )
            })?;
            let bytes = self.registry_chunks.remove(&evicted).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 registry chunk LRU differs from its cache".into(),
                )
            })?;
            self.registry_stats.evictions = self
                .registry_stats
                .evictions
                .checked_add(1)
                .ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 registry eviction count overflow".into(),
                    )
                })?;
            bytes
        } else {
            Vec::new()
        };
        let additional = length.saturating_sub(bytes.len());
        reserve_exact(&mut bytes, additional, "V3 registry chunk bytes")?;
        bytes.resize(length, 0);
        self.source
            .read_range_into_slice(ARCHIVE_V2_PUBKEY_REGISTRY_FILE, offset, &mut bytes)?;
        self.registry_chunks.try_reserve(1).map_err(|source| {
            IndexerV3InstructionSourceError::Allocation {
                context: "V3 registry chunk map",
                source,
            }
        })?;
        self.registry_lru.try_reserve(1).map_err(|source| {
            IndexerV3InstructionSourceError::Allocation {
                context: "V3 registry chunk LRU",
                source,
            }
        })?;
        self.registry_chunks.insert(chunk_id, bytes);
        self.registry_lru.push_back(chunk_id);
        Ok(())
    }

    fn prepare_query_keys(
        &mut self,
        request: &ScanRequest,
    ) -> IndexerV3InstructionSourceResult<()> {
        if !self.query_keys.covers(request) {
            self.query_keys = Arc::new(
                blockzilla_compact_v2_reader::query_keys::BoundQueryKeys::bind_with_registry(
                    self.source.as_ref(),
                    self.registry_entries,
                    request,
                    self.full_registry.as_deref(),
                )
                .map_err(|error| IndexerV3InstructionSourceError::Invalid(error.to_string()))?,
            );
        }
        Ok(())
    }

    fn prepare_registry_for_selective_scan(
        &mut self,
        request: &ScanRequest,
        policy: IndexerV3RegistryReadPolicy,
        requested_transactions: u64,
        candidate_transactions: u64,
    ) -> IndexerV3InstructionSourceResult<()> {
        let needs_full_registry = (request.include_instructions
            && request.include_instruction_accounts)
            || (request.include_instructions
                && matches!(
                    request.instruction_programs,
                    InstructionDataRequirement::All
                )
                && request.required_signer.is_none())
            || (request.include_required_signers && request.required_signer.is_none())
            || request.token_balances.is_requested();
        if !needs_full_registry {
            return self.prepare_query_keys(request);
        }
        if self.full_registry.is_some()
            || !should_prefetch_full_registry(
                policy,
                requested_transactions,
                candidate_transactions,
                self.sidecars.registry_size,
            )
        {
            return self.prepare_query_keys(request);
        }
        self.prefetch_full_registry()?;
        self.prepare_query_keys(request)
    }

    fn prefetch_full_registry(&mut self) -> IndexerV3InstructionSourceResult<()> {
        let registry_size = usize::try_from(self.sidecars.registry_size).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 registry prefetch size exceeds address space".into(),
            )
        })?;
        let mut registry = Vec::new();
        reserve_exact(
            &mut registry,
            registry_size,
            "V3 full-registry prefetch buffer",
        )?;
        registry.resize(registry_size, 0);

        let mut start = 0_usize;
        while start < registry_size {
            let length = registry_prefetch_read_length(registry_size - start);
            let offset = u64::try_from(start).map_err(|_| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 registry prefetch offset exceeds u64".into(),
                )
            })?;
            let end = start.checked_add(length).ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 registry prefetch end overflows".into(),
                )
            })?;
            self.source.read_range_into_slice(
                ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
                offset,
                &mut registry[start..end],
            )?;
            self.registry_stats.prefetch_read_calls = self
                .registry_stats
                .prefetch_read_calls
                .checked_add(1)
                .ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 registry prefetch read-call count overflow".into(),
                    )
                })?;
            self.registry_stats.prefetch_read_bytes = self
                .registry_stats
                .prefetch_read_bytes
                .checked_add(u64::try_from(length).map_err(|_| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 registry prefetch read length exceeds u64".into(),
                    )
                })?)
                .ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 registry prefetch read-byte count overflow".into(),
                    )
                })?;
            start = end;
        }

        let evicted = u64::try_from(self.registry_chunks.len()).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid("V3 registry cache size exceeds u64".into())
        })?;
        self.registry_stats.evictions = self
            .registry_stats
            .evictions
            .checked_add(evicted)
            .ok_or_else(|| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 registry eviction count overflow".into(),
                )
            })?;
        self.registry_chunks.clear();
        self.registry_lru.clear();
        self.full_registry = Some(Arc::from(registry.into_boxed_slice()));
        Ok(())
    }

    fn registry_receipt_since(
        &self,
        before: RegistryReadStats,
    ) -> IndexerV3InstructionSourceResult<IndexerV3RegistryReadReceipt> {
        let stats = self.registry_stats.difference(before)?;
        let used = stats.resolutions != 0
            || stats.prefetch_read_calls != 0
            || stats.prefetch_read_bytes != 0;
        let mode = if !used {
            IndexerV3RegistryReadMode::Unused
        } else if self.full_registry.is_some() {
            IndexerV3RegistryReadMode::FullRegistry
        } else {
            IndexerV3RegistryReadMode::SparseChunkCache
        };
        Ok(IndexerV3RegistryReadReceipt {
            mode,
            prefetch_read_calls: stats.prefetch_read_calls,
            prefetch_read_bytes: stats.prefetch_read_bytes,
            resolutions: stats.resolutions,
            hits: stats.hits,
            misses: stats.misses,
            evictions: stats.evictions,
            resident_payload_bytes: self.registry_resident_payload_bytes()?,
        })
    }

    fn registry_resident_payload_bytes(&self) -> IndexerV3InstructionSourceResult<u64> {
        if let Some(registry) = self.full_registry.as_ref() {
            return u64::try_from(registry.len()).map_err(|_| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 full-registry resident payload bytes exceed u64".into(),
                )
            });
        }
        self.registry_chunks
            .values()
            .try_fold(0_u64, |total, chunk| {
                let bytes = u64::try_from(chunk.len()).map_err(|_| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 registry chunk resident payload bytes overflow".into(),
                    )
                })?;
                total.checked_add(bytes).ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 registry resident payload-byte count overflow".into(),
                    )
                })
            })
    }

    fn touch_registry_chunk(&mut self, chunk_id: u32) {
        if let Some(position) = self.registry_lru.iter().position(|id| *id == chunk_id) {
            self.registry_lru.remove(position);
            self.registry_lru.push_back(chunk_id);
        }
    }

    fn vote_hashes(&self) -> Option<&dyn VoteHashResolver> {
        self.vote_hashes
            .as_deref()
            .map(|registry| registry as &dyn VoteHashResolver)
    }

    fn load_vote_hashes(&mut self) -> IndexerV3InstructionSourceResult<()> {
        if self.vote_hashes_loaded {
            return Ok(());
        }
        let shared_sidecars = Arc::clone(&self.shared_sidecars);
        let (loaded, vote_hashes) = {
            let mut shared = shared_sidecars.lock().map_err(|_| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 shared exact-sidecar cache lock is poisoned".into(),
                )
            })?;
            if let Some(error) = shared.vote_hashes_failure.as_ref() {
                return Err(cached_exact_sidecar_error(
                    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
                    error,
                ));
            }
            if !shared.vote_hashes_loaded {
                match self.load_vote_hashes_uncached() {
                    Ok(vote_hashes) => {
                        shared.vote_hashes = vote_hashes;
                        shared.vote_hashes_loaded = true;
                    }
                    Err(error) => {
                        let error = Arc::new(error);
                        shared.vote_hashes_failure = Some(Arc::clone(&error));
                        return Err(cached_exact_sidecar_error(
                            ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
                            &error,
                        ));
                    }
                }
            }
            (shared.vote_hashes_loaded, shared.vote_hashes.clone())
        };
        self.vote_hashes_loaded = loaded;
        self.vote_hashes = vote_hashes;
        Ok(())
    }

    fn load_vote_hashes_uncached(
        &self,
    ) -> IndexerV3InstructionSourceResult<Option<Arc<VoteHashRegistry>>> {
        let Some(size) = self.sidecars.vote_hash_size else {
            return Ok(None);
        };
        if size > MAX_VOTE_HASH_REGISTRY_BYTES as u64 {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 vote-hash sidecar is {size} bytes, above the {MAX_VOTE_HASH_REGISTRY_BYTES}-byte practical limit"
            )));
        }
        let size = usize::try_from(size).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 vote-hash sidecar exceeds address space".into(),
            )
        })?;
        let bytes = read_exact(
            self.source.as_ref(),
            ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
            0,
            size,
        )?;
        Ok(Some(Arc::new(VoteHashRegistry::from_bytes(&bytes)?)))
    }

    fn load_blockhashes(&mut self) -> IndexerV3InstructionSourceResult<&BlockhashResolver> {
        if self.blockhashes.is_none() {
            let current_size = self.sidecars.blockhash_size.ok_or(
                IndexerV3InstructionSourceError::MissingSidecar {
                    object: ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
                    purpose: "ambiguous signed-message recent blockhash",
                },
            )?;
            let shared_sidecars = Arc::clone(&self.shared_sidecars);
            let blockhashes = {
                let mut shared = shared_sidecars.lock().map_err(|_| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 shared exact-sidecar cache lock is poisoned".into(),
                    )
                })?;
                if let Some(error) = shared.blockhashes_failure.as_ref() {
                    return Err(cached_exact_sidecar_error(
                        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
                        error,
                    ));
                }
                if shared.blockhashes.is_none() {
                    match self.load_blockhashes_uncached(current_size) {
                        Ok(blockhashes) => shared.blockhashes = Some(blockhashes),
                        Err(error) => {
                            let error = Arc::new(error);
                            shared.blockhashes_failure = Some(Arc::clone(&error));
                            return Err(cached_exact_sidecar_error(
                                ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
                                &error,
                            ));
                        }
                    }
                }
                shared.blockhashes.as_ref().cloned().ok_or_else(|| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 shared blockhash resolver was not initialized".into(),
                    )
                })?
            };
            self.blockhashes = Some(blockhashes);
        }
        self.blockhashes.as_deref().ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 blockhash resolver was not initialized".into(),
            )
        })
    }

    fn load_blockhashes_uncached(
        &self,
        current_size: u64,
    ) -> IndexerV3InstructionSourceResult<Arc<BlockhashResolver>> {
        if current_size > MAX_BLOCKHASH_REGISTRY_BYTES as u64 {
            return Err(IndexerV3InstructionSourceError::Invalid(format!(
                "V3 blockhash sidecar is {current_size} bytes, above the {MAX_BLOCKHASH_REGISTRY_BYTES}-byte practical limit"
            )));
        }
        let current_size = usize::try_from(current_size).map_err(|_| {
            IndexerV3InstructionSourceError::Invalid(
                "V3 blockhash sidecar exceeds address space".into(),
            )
        })?;
        let current = read_exact(
            self.source.as_ref(),
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
            0,
            current_size,
        )?;

        let previous = match self.sidecars.previous_blockhash_size {
            None => PreviousBlockhashTail {
                schema: PreviousBlockhashTailSchema::CurrentHashAndSlot,
                entries: Vec::new(),
            },
            Some(size) => {
                let size = usize::try_from(size).map_err(|_| {
                    IndexerV3InstructionSourceError::Invalid(
                        "V3 previous-blockhash sidecar exceeds address space".into(),
                    )
                })?;
                let bytes = read_exact(
                    self.source.as_ref(),
                    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
                    0,
                    size,
                )?;
                parse_previous_blockhash_tail(
                    &bytes,
                    PreviousBlockhashTailSchema::CurrentHashAndSlot,
                )?
            }
        };
        Ok(Arc::new(BlockhashResolver::from_bytes(&current, previous)?))
    }
}

fn cached_exact_sidecar_error(
    object: &'static str,
    source: &Arc<IndexerV3InstructionSourceError>,
) -> IndexerV3InstructionSourceError {
    IndexerV3InstructionSourceError::ExactSidecarLoad {
        object,
        source: Arc::clone(source),
    }
}

fn transaction_signatures<'a>(
    row: &BlockRow,
    transaction: &SemanticTransaction<'_>,
    block_signatures: Option<&'a [[u8; 64]]>,
    signature_cursor: &mut u64,
) -> anyhow::Result<Option<&'a [[u8; 64]]>> {
    let count = transaction
        .signature_ordinals
        .end
        .checked_sub(transaction.signature_ordinals.start)
        .ok_or_else(|| anyhow::anyhow!("V3 transaction signature ordinal range decreases"))?;
    let expected_start = row
        .first_signature_ordinal
        .checked_add(*signature_cursor)
        .ok_or_else(|| anyhow::anyhow!("V3 transaction signature ordinal start overflows"))?;
    let expected_end = expected_start
        .checked_add(count)
        .ok_or_else(|| anyhow::anyhow!("V3 transaction signature ordinal end overflows"))?;
    if transaction.signature_ordinals != (expected_start..expected_end) {
        anyhow::bail!(
            "V3 transaction {} signature ordinals are not contiguous in block {}",
            transaction.tx_index,
            row.block_id
        );
    }
    let expected_byte_start = expected_start
        .checked_mul(SIGNATURE_BYTES as u64)
        .ok_or_else(|| anyhow::anyhow!("V3 transaction signature byte start overflows"))?;
    let expected_byte_end = expected_end
        .checked_mul(SIGNATURE_BYTES as u64)
        .ok_or_else(|| anyhow::anyhow!("V3 transaction signature byte end overflows"))?;
    if transaction.signature_bytes != (expected_byte_start..expected_byte_end) {
        anyhow::bail!(
            "V3 transaction {} signature byte range differs from its ordinals",
            transaction.tx_index
        );
    }
    let start = usize::try_from(*signature_cursor)
        .map_err(|_| anyhow::anyhow!("V3 signature cursor exceeds address space"))?;
    *signature_cursor = signature_cursor
        .checked_add(count)
        .ok_or_else(|| anyhow::anyhow!("V3 block signature cursor overflows"))?;
    let end = usize::try_from(*signature_cursor)
        .map_err(|_| anyhow::anyhow!("V3 signature end exceeds address space"))?;
    block_signatures
        .map(|signatures| {
            signatures.get(start..end).ok_or_else(|| {
                anyhow::anyhow!(
                    "V3 transaction {} signature range exceeds its block window",
                    transaction.tx_index
                )
            })
        })
        .transpose()
}

fn read_exact(
    source: &dyn RangeSource,
    object: &'static str,
    offset: u64,
    length: usize,
) -> IndexerV3InstructionSourceResult<Vec<u8>> {
    if length == 0 {
        return Ok(Vec::new());
    }
    let mut bytes = Vec::new();
    reserve_exact(&mut bytes, length, "V3 exact source read")?;
    source.read_range_into(object, offset, length, &mut bytes)?;
    if bytes.len() != length {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "short {object} read: got {}, expected {length}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn source_error(error: impl std::error::Error + Send + Sync + 'static) -> QueryError {
    QueryError::source(ArchiveFormat::IndexerV3, error)
}

fn reserve_exact<T>(
    values: &mut Vec<T>,
    additional: usize,
    context: &'static str,
) -> IndexerV3InstructionSourceResult<()> {
    values
        .try_reserve_exact(additional)
        .map_err(|source| IndexerV3InstructionSourceError::Allocation { context, source })
}

fn required_plane<'a>(
    plane: Option<&'a [u8]>,
    name: &'static str,
) -> IndexerV3InstructionSourceResult<&'a [u8]> {
    plane.ok_or_else(|| {
        IndexerV3InstructionSourceError::Invalid(format!("decoded V3 metadata has no {name} plane"))
    })
}

fn require_flag(
    transaction: &SemanticTransaction<'_>,
    flag: u32,
    name: &str,
    expected: bool,
) -> IndexerV3InstructionSourceResult<()> {
    let actual = u32::from(transaction.source_flags) & flag != 0;
    if actual != expected {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "slot {} transaction {} {name} flag is {actual}, expected {expected}",
            transaction.slot, transaction.tx_index
        )));
    }
    Ok(())
}

fn reject_flag(
    transaction: &SemanticTransaction<'_>,
    flag: u32,
    name: &str,
) -> IndexerV3InstructionSourceResult<()> {
    require_flag(transaction, flag, name, false)
}

fn needs_vote_hashes(error: &CompactV2MessageProjectionError) -> bool {
    matches!(
        error,
        CompactV2MessageProjectionError::ExactInstructionData(
            SignedMessageError::MissingVoteHashResolver { .. }
        )
    )
}

fn is_relaxable_projection_error(error: &CompactV2MessageProjectionError) -> bool {
    matches!(
        error,
        CompactV2MessageProjectionError::CandidateCombinationLimit
            | CompactV2MessageProjectionError::ExactInstructionData(
                SignedMessageError::MissingVoteHashResolver { .. }
            )
    )
}

fn is_missing_instruction_proof(error: &IndexerV3InstructionSourceError) -> bool {
    matches!(
        error,
        IndexerV3InstructionSourceError::MissingSidecar { .. }
            | IndexerV3InstructionSourceError::Message(
                CompactV2MessageProjectionError::ExactInstructionData(
                    SignedMessageError::MissingVoteHashResolver { .. }
                )
            )
    )
}

fn is_unresolved_instruction_ambiguity(error: &IndexerV3InstructionSourceError) -> bool {
    matches!(
        error,
        IndexerV3InstructionSourceError::Message(
            CompactV2MessageProjectionError::CandidateCombinationLimit
                | CompactV2MessageProjectionError::ExactInstructionData(
                    SignedMessageError::AmbiguousInstructionEncoding { .. }
                )
        ) | IndexerV3InstructionSourceError::SignedMessage(
            SignedMessageError::AmbiguousInstructionEncoding { .. }
                | SignedMessageError::CandidateCombinationLimitExceeded { .. }
                | SignedMessageError::MultipleVerifiedMessageCandidates
        )
    )
}

fn resolve_index(
    account_keys: &[[u8; 32]],
    index: u8,
) -> IndexerV3InstructionSourceResult<[u8; 32]> {
    account_keys
        .get(usize::from(index))
        .copied()
        .ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(format!(
                "V3 message account index {index} is outside resolved keys"
            ))
        })
}

fn resolve_index_u32(
    account_keys: &[[u8; 32]],
    index: u32,
) -> IndexerV3InstructionSourceResult<[u8; 32]> {
    let index = usize::try_from(index).map_err(|_| {
        IndexerV3InstructionSourceError::Invalid(
            "V3 CPI account index exceeds address space".into(),
        )
    })?;
    account_keys.get(index).copied().ok_or_else(|| {
        IndexerV3InstructionSourceError::Invalid(format!(
            "V3 CPI account index {index} is outside resolved keys"
        ))
    })
}

fn projected_account_reference(
    message: &ProjectedCompactV2Message<'_>,
    metadata: &ProjectedMetadata<'_>,
    index: usize,
) -> IndexerV3InstructionSourceResult<CompactPubkey> {
    if let Some(reference) = message.static_account_keys().get(index) {
        return Ok(*reference);
    }
    let loaded_index = index
        .checked_sub(message.static_account_keys().len())
        .ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(format!(
                "V3 message account index {index} is outside projected keys"
            ))
        })?;
    let ProjectedMetadata::Exact(metadata) = metadata else {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "V3 message account index {index} requires unavailable loaded keys"
        )));
    };
    metadata
        .loaded_writable_addresses
        .iter()
        .chain(&metadata.loaded_readonly_addresses)
        .nth(loaded_index)
        .copied()
        .ok_or_else(|| {
            IndexerV3InstructionSourceError::Invalid(format!(
                "V3 message account index {index} is outside projected keys"
            ))
        })
}

fn project_instruction_accounts(
    pool: &mut blockzilla_model::projection_pool::ProjectionPool,
    include_accounts: bool,
    account_keys: &[[u8; 32]],
    account_key_count: usize,
    indexes: &[u8],
) -> IndexerV3InstructionSourceResult<Vec<[u8; 32]>> {
    if include_accounts {
        let mut output = pool.accounts();
        reserve_exact(&mut output, indexes.len(), "V3 instruction accounts")?;
        for &index in indexes {
            output.push(resolve_index(account_keys, index)?);
        }
        return Ok(output);
    }
    if let Some(index) = indexes
        .iter()
        .copied()
        .find(|index| usize::from(*index) >= account_key_count)
    {
        return Err(IndexerV3InstructionSourceError::Invalid(format!(
            "V3 message account index {index} is outside projected keys"
        )));
    }
    Ok(Vec::new())
}

#[allow(clippy::too_many_arguments)]
fn push_instruction(
    output: &mut Vec<ResolvedInstruction>,
    outer_index: usize,
    inner_index: Option<usize>,
    stack_height: Option<u32>,
    program_id: Option<[u8; 32]>,
    accounts: Vec<[u8; 32]>,
    data_coverage: InstructionDataCoverage,
    data: Vec<u8>,
) -> IndexerV3InstructionSourceResult<()> {
    let order = u32::try_from(output.len()).map_err(|_| {
        IndexerV3InstructionSourceError::Invalid("V3 instruction order exceeds u32".into())
    })?;
    let outer_index = u32::try_from(outer_index).map_err(|_| {
        IndexerV3InstructionSourceError::Invalid("V3 outer instruction index exceeds u32".into())
    })?;
    let inner_index = inner_index
        .map(|index| {
            u32::try_from(index).map_err(|_| {
                IndexerV3InstructionSourceError::Invalid(
                    "V3 inner instruction index exceeds u32".into(),
                )
            })
        })
        .transpose()?;
    output.push(ResolvedInstruction {
        coordinate: InstructionCoordinate {
            order,
            outer_index,
            inner_index,
            stack_height,
        },
        program_id,
        accounts,
        data_coverage,
        data,
    });
    Ok(())
}

fn instruction_data_required(
    requirement: &InstructionDataRequirement,
    program_id: &[u8; 32],
) -> bool {
    match requirement {
        InstructionDataRequirement::All => true,
        InstructionDataRequirement::Programs(programs) => programs.contains(program_id),
        InstructionDataRequirement::None => false,
    }
}

fn request_needs_signature_bytes(request: &ScanRequest) -> bool {
    request.include_primary_signatures
        || !matches!(request.instruction_data, InstructionDataRequirement::None)
}

fn semantic_plane_selection(request: &ScanRequest) -> SemanticPlaneSelection {
    SemanticPlaneSelection {
        loaded_addresses: request.include_instructions,
        inner_instructions: request.include_instructions,
        token_balances: request.token_balances.is_requested(),
        outcomes: request.include_instructions || request.include_execution_status,
        raw_metadata_fallbacks: false,
    }
}

enum ProjectedMetadata<'a> {
    Absent,
    Raw,
    Exact(blockzilla_compact_v2_reader::ProjectedCompactV2Metadata<'a>),
    ExactUnprojected,
}

enum SelectedOuterData {
    Exact(Vec<Vec<u8>>),
    Unknown(CoverageReason),
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use blockzilla_archive_v2::{
        ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
        ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
        ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2HotBlockIndexRow, ArchiveV2HotInstruction,
        ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
        ArchiveV2HotV0Message, ArchiveV2VoteHashRef, ArchiveV2VoteStateUpdate,
        ArchiveV2VoteTowerSync,
    };
    use blockzilla_compact::{
        CompactInnerInstruction, CompactInnerInstructions, CompactInstructionError,
        CompactLogStream, CompactMessageHeader, CompactMetaV1, CompactReward, CompactTokenBalance,
        CompactTransactionError, OwnedCompactAddressTableLookup,
    };
    use blockzilla_compact_v2_reader::{
        SignedInstruction, SignedMessage, reconstruct_instruction_data_candidates,
        serialize_signed_message,
    };
    use blockzilla_model::{
        ArchiveInstructionSourceExt, BlockView, CpiCoverage, Error as QueryError, ExecutionStatus,
        InstructionCoverage, InstructionDataCoverage, ScanRange,
    };
    use blockzilla_primitives::{WincodeLeb128Config, wincode_leb128_config};
    use blockzilla_source_local::LocalRangeSource;
    use ed25519_dalek::{Signer, SigningKey};
    use tempfile::TempDir;
    use wincode::SchemaWrite;

    use super::*;
    use crate::indexer_v3_wire::{
        Binding, CompressionPlan, DecodedMetadataParts, SourceBlockCore, WorkerScratch, Writers,
        encode_block_v3,
    };

    const FIRST_SLOT: u64 = 700;
    const SIGNER: [u8; 32] = [1; 32];
    const PROGRAM: [u8; 32] = [2; 32];
    const LOOKUP_TABLE: [u8; 32] = [3; 32];
    const LOADED_WRITABLE: [u8; 32] = [4; 32];
    const LOADED_READONLY: [u8; 32] = [5; 32];
    const VOTE_PROGRAM: [u8; 32] = [6; 32];
    const TARGET_MINT: [u8; 32] = [7; 32];
    const OTHER_MINT: [u8; 32] = [8; 32];
    const TOKEN_OWNER: [u8; 32] = [9; 32];

    struct OwnedBlockSink<'a> {
        output: &'a mut Vec<(BlockHeader, Vec<CanonicalTransaction>)>,
    }

    impl BlockSink for OwnedBlockSink<'_> {
        fn visit_block(
            &mut self,
            block: blockzilla_model::BlockView<'_>,
        ) -> blockzilla_model::Result<()> {
            self.output
                .push((block.header, block.transactions.to_vec()));
            Ok(())
        }
    }

    struct NullBlockSink;

    impl BlockSink for NullBlockSink {
        fn visit_block(
            &mut self,
            _block: blockzilla_model::BlockView<'_>,
        ) -> blockzilla_model::Result<()> {
            Ok(())
        }
    }

    struct FailingBlockSink {
        visits: usize,
    }

    impl BlockSink for FailingBlockSink {
        fn visit_block(
            &mut self,
            _block: blockzilla_model::BlockView<'_>,
        ) -> blockzilla_model::Result<()> {
            self.visits += 1;
            Err(QueryError::InvalidTransaction(
                "forced parallel V3 sink failure".into(),
            ))
        }
    }

    struct SignatureTrackingSource {
        inner: LocalRangeSource,
        signature_reads: Arc<Mutex<Vec<(u64, usize)>>>,
    }

    impl RangeSource for SignatureTrackingSource {
        fn size(&self, object: &str) -> blockzilla_source::SourceResult<Option<u64>> {
            self.inner.size(object)
        }

        fn read_range(
            &self,
            object: &str,
            offset: u64,
            length: usize,
        ) -> blockzilla_source::SourceResult<Vec<u8>> {
            if object == ARCHIVE_V2_SIGNATURES_FILE {
                self.signature_reads
                    .lock()
                    .expect("signature-read mutex poisoned")
                    .push((offset, length));
            }
            self.inner.read_range(object, offset, length)
        }
    }

    struct Fixture {
        directory: TempDir,
    }

    enum FixtureMetadata {
        Absent,
        Raw(Vec<u8>),
        Exact(Box<CompactMetaV1>),
    }

    struct FixtureTransaction {
        flags: u32,
        message: Vec<u8>,
        metadata: FixtureMetadata,
    }

    impl FixtureTransaction {
        fn exact(message: ArchiveV2HotMessagePayload, metadata: CompactMetaV1, flags: u32) -> Self {
            Self {
                flags: flags | ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                message: encode(&message),
                metadata: FixtureMetadata::Exact(Box::new(metadata)),
            }
        }

        fn absent(message: ArchiveV2HotMessagePayload) -> Self {
            Self {
                flags: 0,
                message: encode(&message),
                metadata: FixtureMetadata::Absent,
            }
        }

        fn raw_metadata(message: ArchiveV2HotMessagePayload) -> Self {
            Self {
                flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                message: encode(&message),
                metadata: FixtureMetadata::Raw(vec![0xde, 0xad]),
            }
        }

        fn raw_transaction(bytes: Vec<u8>) -> Self {
            Self {
                flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                message: bytes,
                metadata: FixtureMetadata::Absent,
            }
        }
    }

    fn raw_parallel_blocks(count: usize) -> Vec<Vec<FixtureTransaction>> {
        (0..count)
            .map(|marker| {
                vec![FixtureTransaction::raw_transaction(vec![
                    u8::try_from(marker % 251).unwrap(),
                ])]
            })
            .collect()
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let plan = CompressionPlan::default_level_three();
            let binding = Binding {
                epoch: 7,
                slots_per_epoch: 100,
                selected_blocks: 2,
                selected_transactions: 1,
                message_schema: blockzilla_compact_v2_reader::CompactV2MessageSchema::Current,
                metadata_schema:
                    blockzilla_compact_v2_reader::CompactV2MetadataSchema::CurrentTypedError,
                prefix: false,
            };
            let mut writers = Writers::create_v3(directory.path(), binding, plan).unwrap();
            let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
            let mut scratch = WorkerScratch::default();

            scratch.begin_block_v3();
            scratch.record_block_rewards(&[0]).unwrap();
            scratch.finish_block(0).unwrap();
            let block = encode_block_v3(&mut scratch, &mut compressor, plan).unwrap();
            writers
                .append(
                    source_row(0, FIRST_SLOT, 0, 0, 0, 0),
                    source_core(FIRST_SLOT - 1),
                    block,
                )
                .unwrap();

            let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
                instructions: vec![ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: ArchiveV2HotInstructionData::Raw(vec![4, 5]),
                }],
            });
            let inner = Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: vec![6, 7],
                    stack_height: Some(2),
                }],
            }]);
            let metadata = CompactMetaV1 {
                err: None,
                fee: 5_000,
                pre_balances: vec![1, 2],
                post_balances: vec![1, 2],
                inner_instructions: inner,
                logs: None,
                pre_token_balances: Vec::new(),
                post_token_balances: Vec::new(),
                rewards: Vec::new(),
                loaded_writable_addresses: Vec::new(),
                loaded_readonly_addresses: Vec::new(),
                return_data: None,
                compute_units_consumed: Some(10),
                cost_units: None,
            };
            let parts = split_parts(&metadata);

            scratch.begin_block_v3();
            scratch
                .begin_transaction(
                    ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                    1,
                    &encode(&message),
                )
                .unwrap();
            scratch.record_decoded_metadata(parts.borrow()).unwrap();
            scratch.record_block_rewards(&[0]).unwrap();
            scratch.finish_block(1).unwrap();
            let block = encode_block_v3(&mut scratch, &mut compressor, plan).unwrap();
            writers
                .append(
                    source_row(1, FIRST_SLOT + 2, 1, 0, 1, 0),
                    source_core(FIRST_SLOT),
                    block,
                )
                .unwrap();
            writers.finish(2, 1).unwrap();

            let mut registry = Vec::new();
            registry.extend_from_slice(&SIGNER);
            registry.extend_from_slice(&PROGRAM);
            std::fs::write(
                directory.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
                registry,
            )
            .unwrap();
            std::fs::write(directory.path().join(ARCHIVE_V2_SIGNATURES_FILE), [9u8; 64]).unwrap();

            Self { directory }
        }

        fn build(
            registry: &[[u8; 32]],
            blocks: Vec<Vec<FixtureTransaction>>,
            signatures: Option<&[[u8; 64]]>,
            prefix: bool,
            slots_per_epoch: u64,
        ) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let plan = CompressionPlan::default_level_three();
            let selected_blocks = blocks.len() as u64;
            let selected_transactions = blocks.iter().map(Vec::len).sum::<usize>() as u64;
            let binding = Binding {
                epoch: 7,
                slots_per_epoch,
                selected_blocks,
                selected_transactions,
                message_schema: blockzilla_compact_v2_reader::CompactV2MessageSchema::Current,
                metadata_schema:
                    blockzilla_compact_v2_reader::CompactV2MetadataSchema::CurrentTypedError,
                prefix,
            };
            let mut writers = Writers::create_v3(directory.path(), binding, plan).unwrap();
            let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
            let mut scratch = WorkerScratch::default();
            let mut first_transaction = 0u64;
            let mut first_signature = 0u64;

            for (block_id, transactions) in blocks.into_iter().enumerate() {
                scratch.begin_block_v3();
                for transaction in &transactions {
                    scratch
                        .begin_transaction(transaction.flags, 1, &transaction.message)
                        .unwrap();
                    match &transaction.metadata {
                        FixtureMetadata::Absent => scratch.record_missing_metadata().unwrap(),
                        FixtureMetadata::Raw(bytes) => scratch.record_raw_metadata(bytes).unwrap(),
                        FixtureMetadata::Exact(metadata) => {
                            let parts = split_parts(metadata);
                            scratch.record_decoded_metadata(parts.borrow()).unwrap();
                        }
                    }
                }
                scratch.record_block_rewards(&[0]).unwrap();
                scratch.finish_block(transactions.len() as u32).unwrap();
                let block = encode_block_v3(&mut scratch, &mut compressor, plan).unwrap();
                let transaction_count = transactions.len() as u32;
                let slot = FIRST_SLOT + block_id as u64 * 2;
                writers
                    .append(
                        source_row(
                            block_id as u32,
                            slot,
                            transaction_count,
                            first_transaction,
                            transaction_count,
                            first_signature,
                        ),
                        source_core(slot.saturating_sub(1)),
                        block,
                    )
                    .unwrap();
                first_transaction += u64::from(transaction_count);
                first_signature += u64::from(transaction_count);
            }
            writers
                .finish(selected_blocks, selected_transactions)
                .unwrap();

            std::fs::write(
                directory.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
                registry.iter().flatten().copied().collect::<Vec<_>>(),
            )
            .unwrap();
            if let Some(signatures) = signatures {
                std::fs::write(
                    directory.path().join(ARCHIVE_V2_SIGNATURES_FILE),
                    signatures.iter().flatten().copied().collect::<Vec<_>>(),
                )
                .unwrap();
            }
            Self { directory }
        }

        fn open(&self, binding: &str) -> IndexerV3InstructionSource {
            IndexerV3InstructionSource::open_local(self.directory.path(), FIRST_SLOT, binding)
                .unwrap()
        }
    }

    fn source_row(
        block_id: u32,
        slot: u64,
        tx_count: u32,
        first_tx_ordinal: u64,
        signature_count: u32,
        first_signature_ordinal: u64,
    ) -> ArchiveV2HotBlockIndexRow {
        ArchiveV2HotBlockIndexRow {
            block_id,
            slot,
            compressed_offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            tx_count,
            first_tx_ordinal,
            first_signature_ordinal,
            signature_count,
        }
    }

    fn source_core(parent_slot: u64) -> SourceBlockCore {
        SourceBlockCore {
            parent_slot,
            blockhash_id: 1,
            previous_blockhash_id: 0,
            block_time: None,
            block_height: None,
        }
    }

    fn encode<T: SchemaWrite<WincodeLeb128Config, Src = T>>(value: &T) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn message_header(unsigned_accounts: u8) -> CompactMessageHeader {
        CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: unsigned_accounts,
        }
    }

    fn raw_instruction(
        program_id_index: u8,
        accounts: &[u8],
        data: &[u8],
    ) -> ArchiveV2HotInstruction {
        ArchiveV2HotInstruction {
            program_id_index,
            accounts: accounts.to_vec(),
            data: ArchiveV2HotInstructionData::Raw(data.to_vec()),
        }
    }

    fn legacy_message(
        account_keys: Vec<CompactPubkey>,
        instructions: Vec<ArchiveV2HotInstruction>,
    ) -> ArchiveV2HotMessagePayload {
        ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: message_header((account_keys.len() - 1) as u8),
            account_keys,
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([13; 32]),
            instructions,
        })
    }

    fn metadata(
        account_count: usize,
        error: Option<CompactTransactionError>,
        inner_instructions: Option<Vec<CompactInnerInstructions>>,
        loaded_writable_addresses: Vec<CompactPubkey>,
        loaded_readonly_addresses: Vec<CompactPubkey>,
    ) -> CompactMetaV1 {
        CompactMetaV1 {
            err: error,
            fee: 5_000,
            pre_balances: vec![0; account_count],
            post_balances: vec![0; account_count],
            inner_instructions,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses,
            loaded_readonly_addresses,
            return_data: None,
            compute_units_consumed: Some(10),
            cost_units: None,
        }
    }

    fn vote_tower_data(block_references: bool) -> ArchiveV2HotInstructionData {
        let hash = if block_references {
            ArchiveV2VoteHashRef::Block(0)
        } else {
            ArchiveV2VoteHashRef::Raw([31; 32])
        };
        ArchiveV2HotInstructionData::VoteTowerSync(ArchiveV2VoteTowerSync {
            update: ArchiveV2VoteStateUpdate {
                root: None,
                lockout_offsets: Vec::new(),
                hash,
                timestamp: None,
            },
            block_id_hash: hash,
        })
    }

    struct SplitParts {
        outcome_head: Vec<u8>,
        pre_balances: Vec<u8>,
        post_balances: Vec<u8>,
        inner_instructions: Vec<u8>,
        logs: Vec<u8>,
        pre_token_balances: Vec<u8>,
        post_token_balances: Vec<u8>,
        transaction_rewards: Vec<u8>,
        loaded_writable: Vec<u8>,
        loaded_readonly: Vec<u8>,
        outcome_tail: Vec<u8>,
        effect_state: u8,
    }

    impl SplitParts {
        fn borrow(&self) -> DecodedMetadataParts<'_> {
            DecodedMetadataParts {
                outcome_head: &self.outcome_head,
                pre_balances: &self.pre_balances,
                post_balances: &self.post_balances,
                inner_instructions: &self.inner_instructions,
                logs: &self.logs,
                pre_token_balances: &self.pre_token_balances,
                post_token_balances: &self.post_token_balances,
                transaction_rewards: &self.transaction_rewards,
                loaded_writable: &self.loaded_writable,
                loaded_readonly: &self.loaded_readonly,
                outcome_tail: &self.outcome_tail,
                effect_state: self.effect_state,
            }
        }
    }

    fn split_parts(metadata: &CompactMetaV1) -> SplitParts {
        let mut outcome_head = encode(&metadata.err);
        outcome_head.extend(encode(&metadata.fee));
        let mut outcome_tail = encode(&metadata.return_data);
        outcome_tail.extend(encode(&metadata.compute_units_consumed));
        outcome_tail.extend(encode(&metadata.cost_units));
        let cpi_state = match &metadata.inner_instructions {
            None => 1,
            Some(groups) if groups.is_empty() => 2,
            Some(_) => 3,
        };
        SplitParts {
            outcome_head,
            pre_balances: encode(&metadata.pre_balances),
            post_balances: encode(&metadata.post_balances),
            inner_instructions: encode(&metadata.inner_instructions),
            logs: encode::<Option<CompactLogStream>>(&metadata.logs),
            pre_token_balances: encode::<Vec<CompactTokenBalance>>(&metadata.pre_token_balances),
            post_token_balances: encode::<Vec<CompactTokenBalance>>(&metadata.post_token_balances),
            transaction_rewards: encode::<Vec<CompactReward>>(&metadata.rewards),
            loaded_writable: encode(&metadata.loaded_writable_addresses),
            loaded_readonly: encode(&metadata.loaded_readonly_addresses),
            outcome_tail,
            effect_state: cpi_state | (1 << 3) | (1 << 4),
        }
    }

    #[test]
    fn count_and_program_filters_do_not_expand_registry_keys() {
        let fixture = Fixture::new();
        for target in [None, Some(PROGRAM), Some([99; 32])] {
            let mut source = fixture.open("id-only-query");
            let base = ScanRequest::all()
                .allow_unverified_source()
                .without_primary_signatures()
                .without_required_signers()
                .without_execution_status()
                .without_instruction_programs();
            let request = target.map_or(base.clone(), |key| {
                base.with_instruction_programs_for([key])
            });
            if let Some(key) = target {
                source.filter_key_id(&request, &key).unwrap();
            }
            let bound = Arc::clone(&source.context.query_keys);
            let mut observed = Vec::new();
            let mut sink =
                blockzilla_model::FnBlockSink::new(|block: blockzilla_model::BlockView<'_>| {
                    for tx in block.transactions {
                        observed.extend(
                            tx.instructions
                                .iter()
                                .map(|instruction| instruction.program_id),
                        );
                    }
                    Ok(())
                });
            let result = source
                .scan_ordered_parallel_with_registry_policy(
                    &request,
                    IndexerV3RegistryReadPolicy::for_test(64, 1),
                    NonZeroUsize::new(12).unwrap(),
                    &mut sink,
                )
                .unwrap();
            assert_eq!(result.scan.transactions, 1);
            assert_eq!(result.scan.instructions, 2);
            assert_eq!(result.registry.prefetch_read_bytes, 0);
            assert_eq!(result.registry.resolutions, 0);
            assert!(Arc::ptr_eq(&bound, &source.context.query_keys));
            let expected = (target == Some(PROGRAM)).then_some(PROGRAM);
            assert_eq!(observed, vec![expected; 2]);
        }
    }

    #[test]
    fn local_v3_fixture_projects_empty_block_outer_cpi_and_exact_io() {
        let fixture = Fixture::new();
        let mut source = fixture.open("fixture-binding-a");
        assert_eq!(source.scope(), IndexerV3SourceScope::FullSelection);
        assert_eq!(
            source.identity().verification,
            SourceVerification::InternalBindingOnly
        );
        assert_eq!(
            source.identity().binding.as_deref(),
            Some("fixture-binding-a")
        );
        assert!(source.signatures_available());

        let request = ScanRequest::all()
            .allow_unverified_source()
            .with_instruction_data_for([PROGRAM]);
        let mut blocks = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                blocks.push((block.header, block.transactions.to_vec()));
                Ok(())
            })
            .unwrap();

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].0.block_ordinal, 0);
        assert_eq!(blocks[0].0.slot, FIRST_SLOT);
        assert!(blocks[0].1.is_empty());
        let transaction = &blocks[1].1[0];
        assert_eq!(transaction.primary_signature, Some([9; 64]));
        assert_eq!(transaction.required_signers, [SIGNER]);
        assert_eq!(transaction.instructions.len(), 2);
        assert_eq!(transaction.instructions[0].program_id, Some(PROGRAM));
        assert_eq!(transaction.instructions[0].data, [4, 5]);
        assert_eq!(
            transaction.instructions[0].data_coverage,
            InstructionDataCoverage::Exact
        );
        assert_eq!(transaction.instructions[1].coordinate.outer_index, 0);
        assert_eq!(transaction.instructions[1].coordinate.inner_index, Some(0));
        assert_eq!(transaction.instructions[1].coordinate.stack_height, Some(2));
        assert_eq!(transaction.instructions[1].data, [6, 7]);
        assert_eq!(receipt.blocks, 2);
        assert_eq!(receipt.transactions, 1);
        assert_eq!(receipt.instructions, 2);
        assert!(receipt.io.source_read_calls.is_some_and(|calls| calls > 0));
        assert!(receipt.io.source_read_bytes.is_some_and(|bytes| bytes > 0));
        assert!(receipt.io.decoded_bytes.is_some_and(|bytes| bytes > 0));
    }

    #[test]
    fn token_balance_only_scan_reads_the_split_plane_and_filters_mints() {
        let mut exact_metadata = metadata(2, None, None, vec![], vec![]);
        exact_metadata.pre_token_balances = vec![
            CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Id(5)),
                owner: Some(CompactPubkey::Id(4)),
                program_id: Some(CompactPubkey::Id(2)),
                amount: 11,
                decimals: 6,
            },
            CompactTokenBalance {
                account_index: 1,
                mint: Some(CompactPubkey::Id(3)),
                owner: Some(CompactPubkey::Id(4)),
                program_id: Some(CompactPubkey::Id(2)),
                amount: 22,
                decimals: 6,
            },
        ];
        exact_metadata.post_token_balances = vec![CompactTokenBalance {
            account_index: 1,
            mint: Some(CompactPubkey::Id(3)),
            owner: Some(CompactPubkey::Id(4)),
            program_id: Some(CompactPubkey::Id(2)),
            amount: 33,
            decimals: 6,
        }];
        let fixture = Fixture::build(
            &[SIGNER, PROGRAM, TARGET_MINT, TOKEN_OWNER, OTHER_MINT],
            vec![vec![FixtureTransaction::exact(
                legacy_message(vec![CompactPubkey::Id(1), CompactPubkey::Id(2)], Vec::new()),
                exact_metadata,
                0,
            )]],
            None,
            false,
            100,
        );
        let mut source = fixture.open("token-plane-fixture");
        let request = ScanRequest::all()
            .allow_unverified_source()
            .without_instructions()
            .without_required_signers()
            .without_execution_status()
            .without_primary_signatures()
            .with_token_balances_for([TARGET_MINT]);
        let mut observed = None;
        let receipt = source
            .for_each_block(&request, |block| {
                observed = Some(block.transactions[0].clone());
                Ok(())
            })
            .unwrap();

        let transaction = observed.unwrap();
        assert_eq!(transaction.primary_signature, None);
        assert!(transaction.required_signers.is_empty());
        assert_eq!(
            transaction.header.status,
            ExecutionStatus::Unknown(CoverageReason::ProjectionNotRequested)
        );
        assert_eq!(
            transaction.header.instruction_coverage,
            InstructionCoverage::Unknown(CoverageReason::ProjectionNotRequested)
        );
        assert_eq!(
            transaction.header.cpi_coverage,
            CpiCoverage::Unknown(CoverageReason::ProjectionNotRequested)
        );
        assert!(transaction.instructions.is_empty());
        assert_eq!(
            transaction.token_balance_coverage,
            TokenBalanceCoverage::Complete
        );
        assert_eq!(transaction.token_balances.len(), 2);
        assert_eq!(transaction.token_balances[0].side, TokenBalanceSide::Pre);
        assert_eq!(transaction.token_balances[0].balance_index, 1);
        assert_eq!(transaction.token_balances[0].mint, Some(TARGET_MINT));
        assert_eq!(transaction.token_balances[0].owner, Some(TOKEN_OWNER));
        assert_eq!(transaction.token_balances[0].token_program, Some(PROGRAM));
        assert_eq!(transaction.token_balances[0].amount, 22);
        assert_eq!(transaction.token_balances[1].side, TokenBalanceSide::Post);
        assert_eq!(transaction.token_balances[1].balance_index, 0);
        assert_eq!(transaction.token_balances[1].amount, 33);
        assert_eq!(receipt.instructions, 0);
        assert_eq!(receipt.transactions_with_incomplete_token_balances, 0);
        assert!(receipt.io.source_read_bytes.is_some_and(|bytes| bytes > 0));
    }

    #[test]
    fn selected_token_balances_allocate_for_matches_and_preserve_output() {
        const MESSAGE_ACCOUNT_COUNT: usize = 10;
        const UNMATCHED_ROWS_PER_SIDE: usize = 8;

        let unmatched = CompactTokenBalance {
            account_index: 0,
            mint: Some(CompactPubkey::Id(5)),
            owner: Some(CompactPubkey::Id(4)),
            program_id: Some(CompactPubkey::Id(2)),
            amount: 11,
            decimals: 6,
        };
        let matching_pre = CompactTokenBalance {
            account_index: 1,
            mint: Some(CompactPubkey::Id(3)),
            owner: Some(CompactPubkey::Id(4)),
            program_id: Some(CompactPubkey::Id(2)),
            amount: 22,
            decimals: 6,
        };
        let matching_post = CompactTokenBalance {
            amount: 33,
            ..matching_pre.clone()
        };

        let mut no_matches = metadata(MESSAGE_ACCOUNT_COUNT, None, None, vec![], vec![]);
        no_matches.pre_token_balances = vec![unmatched.clone(); UNMATCHED_ROWS_PER_SIDE];
        no_matches.post_token_balances = vec![unmatched.clone(); UNMATCHED_ROWS_PER_SIDE];

        let mut few_matches = metadata(MESSAGE_ACCOUNT_COUNT, None, None, vec![], vec![]);
        few_matches.pre_token_balances = vec![unmatched.clone(); UNMATCHED_ROWS_PER_SIDE];
        few_matches.post_token_balances = vec![unmatched; UNMATCHED_ROWS_PER_SIDE];
        few_matches
            .pre_token_balances
            .insert(UNMATCHED_ROWS_PER_SIDE / 2, matching_pre);
        few_matches
            .post_token_balances
            .insert(UNMATCHED_ROWS_PER_SIDE / 2, matching_post);

        let message = || {
            let mut account_keys = vec![CompactPubkey::Id(1)];
            account_keys.extend(vec![CompactPubkey::Id(2); MESSAGE_ACCOUNT_COUNT - 1]);
            legacy_message(account_keys, Vec::new())
        };
        let fixture = Fixture::build(
            &[SIGNER, PROGRAM, TARGET_MINT, TOKEN_OWNER, OTHER_MINT],
            vec![vec![
                FixtureTransaction::exact(message(), no_matches, 0),
                FixtureTransaction::exact(message(), few_matches, 0),
            ]],
            None,
            false,
            100,
        );
        let projection_base = ScanRequest::all()
            .allow_unverified_source()
            .without_instructions()
            .without_required_signers()
            .without_execution_status()
            .without_primary_signatures();

        let mut all_source = fixture.open("token-allocation-all");
        let mut all_balances = Vec::new();
        all_source
            .for_each_block(&projection_base.clone().with_token_balances(), |block| {
                all_balances.extend(
                    block
                        .transactions
                        .iter()
                        .map(|transaction| transaction.token_balances.clone()),
                );
                Ok(())
            })
            .unwrap();

        let mut selected_source = fixture.open("token-allocation-selected");
        let mut selected_balances = Vec::new();
        let mut selected_capacities = Vec::new();
        selected_source
            .for_each_block(
                &projection_base.with_token_balances_for([TARGET_MINT]),
                |block| {
                    for transaction in block.transactions {
                        selected_capacities.push(transaction.token_balances.capacity());
                        selected_balances.push(transaction.token_balances.clone());
                    }
                    Ok(())
                },
            )
            .unwrap();

        let expected_selected = all_balances
            .into_iter()
            .map(|balances| {
                balances
                    .into_iter()
                    .filter(|balance| balance.mint == Some(TARGET_MINT))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(selected_balances, expected_selected);
        assert!(selected_balances[0].is_empty());
        assert_eq!(selected_capacities[0], 0);
        assert_eq!(selected_balances[1].len(), 2);
        assert!(selected_capacities[1] <= 4);
    }

    #[test]
    fn explicit_shared_local_source_is_operator_trusted() {
        let fixture = Fixture::new();
        let allowed = local_source_objects();
        let pinned =
            PinnedLocalRangeSource::new_anchored(fixture.directory.path(), &allowed).unwrap();
        let mut source = IndexerV3InstructionSource::open_operator_trusted_source(
            Arc::new(pinned),
            "explicit-local-fixture",
            FIRST_SLOT,
            "operator-candidate-a",
        )
        .unwrap();

        assert_eq!(
            source.identity().verification,
            SourceVerification::OperatorTrusted
        );
        assert_eq!(
            source.identity().binding.as_deref(),
            Some("operator-candidate-a")
        );
        let receipt = source
            .for_each_block(&ScanRequest::all(), |_| Ok(()))
            .unwrap();
        assert_eq!(receipt.blocks, 2);
        assert_eq!(receipt.transactions, 1);
    }

    #[test]
    fn selected_transaction_geometry_requires_contiguous_rows_and_header_total() {
        let fixture = Fixture::new();
        let reader = Reader::open(fixture.directory.path()).unwrap();
        let first = reader.block(0).unwrap();
        let second = reader.block(1).unwrap();
        let mut expected = 0;
        advance_transaction_geometry(first, &mut expected).unwrap();
        advance_transaction_geometry(second, &mut expected).unwrap();
        validate_selected_transaction_total(expected, reader.header.selected_transactions).unwrap();

        let mut wrong = second.clone();
        wrong.first_tx_ordinal += 1;
        assert!(advance_transaction_geometry(&wrong, &mut 0).is_err());
        assert!(validate_selected_transaction_total(expected, expected + 1).is_err());
    }

    #[test]
    fn bounded_empty_row_reads_only_its_directory_and_binding_changes_identity() {
        let fixture = Fixture::new();
        let mut first = fixture.open("fixture-binding-a");
        let second = fixture.open("fixture-binding-b");
        assert_ne!(first.identity(), second.identity());
        assert!(
            IndexerV3InstructionSource::open_local(fixture.directory.path(), FIRST_SLOT, "")
                .is_err()
        );

        let request = ScanRequest::bounded(ScanRange {
            first_block: 0,
            block_count: NonZeroU32::new(1).unwrap(),
        })
        .allow_unverified_source();
        let receipt = first.for_each_block(&request, |_| Ok(())).unwrap();
        assert_eq!(receipt.blocks, 1);
        assert_eq!(receipt.transactions, 0);
        assert_eq!(receipt.io.source_read_calls, Some(1));
        assert!(receipt.io.source_read_bytes.is_some_and(|bytes| bytes > 0));
        assert!(receipt.io.decoded_bytes.is_some_and(|bytes| bytes > 0));
    }

    #[test]
    fn ordered_v3_scan_coalesces_contiguous_block_signature_windows() {
        const BLOCKS: usize = 4;
        let blocks = (0..BLOCKS)
            .map(|block| vec![FixtureTransaction::raw_transaction(vec![block as u8 + 1])])
            .collect::<Vec<_>>();
        let signatures = (0..BLOCKS)
            .map(|block| [block as u8 + 11; SIGNATURE_BYTES])
            .collect::<Vec<_>>();
        let fixture = Fixture::build(&[], blocks, Some(&signatures), false, 100);
        let mut source = fixture.open("coalesced-signatures");
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data();
        let mut observed = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                observed.push(block.transactions[0].primary_signature.unwrap());
                Ok(())
            })
            .unwrap();

        assert_eq!(observed, signatures);
        assert_eq!(receipt.blocks, BLOCKS as u64);
        assert_eq!(receipt.transactions, BLOCKS as u64);
        assert_eq!(receipt.io.source_read_calls, Some(3));
        assert!(
            receipt
                .io
                .source_read_bytes
                .is_some_and(|bytes| bytes >= (BLOCKS * SIGNATURE_BYTES) as u64)
        );
    }

    #[test]
    fn selected_signature_reader_does_not_fill_sparse_block_gaps() {
        let blocks = (0..3)
            .map(|block| vec![FixtureTransaction::raw_transaction(vec![block as u8])])
            .collect::<Vec<_>>();
        let signatures = [
            [21; SIGNATURE_BYTES],
            [22; SIGNATURE_BYTES],
            [23; SIGNATURE_BYTES],
        ];
        let fixture = Fixture::build(&[], blocks, Some(&signatures), false, 100);
        let source = fixture.open("sparse-signatures");
        let before = source.meter.stats().unwrap();
        let mut selected = SelectedBlockSignatureReader::for_selected_blocks(
            &source.reader,
            source.context.source.clone(),
            true,
            vec![0, 2],
        )
        .unwrap();

        assert_eq!(selected.read_block(0).unwrap().unwrap(), &signatures[0..1]);
        assert_eq!(selected.read_block(2).unwrap().unwrap(), &signatures[2..3]);
        selected.finish().unwrap();
        let io = source.meter.stats().unwrap().difference(before).unwrap();
        assert_eq!(io.calls, 2);
        assert_eq!(io.bytes, (2 * SIGNATURE_BYTES) as u64);
    }

    #[test]
    fn selective_scan_sparse_blocks_match_full_scan_without_gap_signatures() {
        let blocks = vec![
            vec![FixtureTransaction::raw_transaction(vec![1])],
            vec![
                FixtureTransaction::raw_transaction(vec![2]),
                FixtureTransaction::raw_transaction(vec![3]),
            ],
            vec![FixtureTransaction::raw_transaction(vec![4])],
        ];
        let signatures = [
            [31; SIGNATURE_BYTES],
            [32; SIGNATURE_BYTES],
            [33; SIGNATURE_BYTES],
            [34; SIGNATURE_BYTES],
        ];
        let fixture = Fixture::build(&[], blocks, Some(&signatures), false, 100);
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data();

        let mut full_source = fixture.open("selective-full-parity");
        let mut full_selected = Vec::new();
        full_source
            .for_each_block(&request, |block| {
                if matches!(block.header.block_ordinal, 0 | 2) {
                    full_selected.push((block.header, block.transactions.to_vec()));
                }
                Ok(())
            })
            .unwrap();

        let signature_reads = Arc::new(Mutex::new(Vec::new()));
        let tracking_source = SignatureTrackingSource {
            inner: LocalRangeSource::new(fixture.directory.path()),
            signature_reads: signature_reads.clone(),
        };
        let mut selective_source = IndexerV3InstructionSource::open_with_source(
            Arc::new(tracking_source),
            "selective-sparse-parity",
            FIRST_SLOT,
            "selective-sparse-parity-binding",
        )
        .unwrap();
        let mut selected = Vec::new();
        let receipt = {
            let mut sink = OwnedBlockSink {
                output: &mut selected,
            };
            selective_source
                .scan_selected_blocks(&request, &[0, 2], &mut sink)
                .unwrap()
        };

        assert_eq!(selected, full_selected);
        assert_eq!(receipt.requested_blocks, 3);
        assert_eq!(receipt.requested_transactions, 4);
        assert_eq!(receipt.candidate_blocks, 2);
        assert_eq!(receipt.candidate_transactions, 2);
        assert_eq!(receipt.skipped_blocks, 1);
        assert_eq!(receipt.skipped_transactions, 2);
        assert_eq!(receipt.scan_receipt.blocks, 2);
        assert_eq!(receipt.scan_receipt.transactions, 2);
        assert_eq!(receipt.scan_receipt.io, receipt.source_io);
        assert!(
            receipt
                .source_io
                .source_read_calls
                .is_some_and(|calls| calls > 0)
        );
        assert!(
            receipt
                .source_io
                .decoded_bytes
                .is_some_and(|bytes| bytes > 0)
        );
        assert_eq!(
            *signature_reads
                .lock()
                .expect("signature-read mutex poisoned"),
            vec![
                (0, SIGNATURE_BYTES),
                (3 * SIGNATURE_BYTES as u64, SIGNATURE_BYTES)
            ]
        );

        let omitted_signature_reads = Arc::new(Mutex::new(Vec::new()));
        let omitted_tracking_source = SignatureTrackingSource {
            inner: LocalRangeSource::new(fixture.directory.path()),
            signature_reads: omitted_signature_reads.clone(),
        };
        let mut omitted_source = IndexerV3InstructionSource::open_with_source(
            Arc::new(omitted_tracking_source),
            "selective-signatures-omitted",
            FIRST_SLOT,
            "selective-signatures-omitted-binding",
        )
        .unwrap();
        let omitted_request = request.clone().without_primary_signatures();
        let mut omitted_blocks = Vec::new();
        {
            let mut sink = OwnedBlockSink {
                output: &mut omitted_blocks,
            };
            omitted_source
                .scan_selected_blocks(&omitted_request, &[0, 2], &mut sink)
                .unwrap();
        }

        assert_eq!(omitted_blocks.len(), 2);
        assert!(omitted_blocks.iter().all(|(_, transactions)| {
            transactions
                .iter()
                .all(|transaction| transaction.primary_signature.is_none())
        }));
        assert!(
            omitted_signature_reads
                .lock()
                .expect("signature-read mutex poisoned")
                .is_empty()
        );
    }

    #[test]
    fn dense_registry_policy_requires_transaction_threshold_density_and_size_limit() {
        let policy = IndexerV3RegistryReadPolicy::with_full_registry_limit(1_024);
        assert!(!should_prefetch_full_registry(
            IndexerV3RegistryReadPolicy::sparse_only(),
            2_000_000,
            1_000_000,
            1_024,
        ));
        assert!(!should_prefetch_full_registry(
            policy, 2_000_000, 999_999, 1_024,
        ));
        assert!(should_prefetch_full_registry(
            policy, 2_000_000, 1_000_000, 1_024,
        ));
        assert!(!should_prefetch_full_registry(
            policy, 2_000_001, 1_000_000, 1_024,
        ));
        assert!(should_prefetch_full_registry(
            policy, 2_000_001, 1_000_001, 1_024,
        ));
        assert!(!should_prefetch_full_registry(
            policy, 1_000_000, 1_000_001, 1_024,
        ));
        assert!(!should_prefetch_full_registry(
            policy, 2_000_000, 1_000_000, 1_025,
        ));
        assert_eq!(policy.max_full_registry_bytes(), 1_024);
    }

    #[test]
    fn signature_bytes_are_omitted_only_when_no_requested_field_needs_them() {
        let no_signatures = ScanRequest::all()
            .without_instruction_data()
            .without_primary_signatures();
        assert!(!request_needs_signature_bytes(&no_signatures));
        assert!(request_needs_signature_bytes(&ScanRequest::all()));
        assert!(request_needs_signature_bytes(
            &ScanRequest::all()
                .with_instruction_data_for([PROGRAM])
                .without_primary_signatures()
        ));
    }

    #[test]
    fn registry_prefetch_read_windows_are_never_larger_than_32_mib() {
        let total = MAX_REGISTRY_PREFETCH_READ_BYTES * 2 + 17;
        let mut remaining = total;
        let mut lengths = Vec::new();
        while remaining != 0 {
            let length = registry_prefetch_read_length(remaining);
            assert!(length > 0);
            assert!(length <= MAX_REGISTRY_PREFETCH_READ_BYTES);
            lengths.push(length);
            remaining -= length;
        }
        assert_eq!(
            lengths,
            [
                MAX_REGISTRY_PREFETCH_READ_BYTES,
                MAX_REGISTRY_PREFETCH_READ_BYTES,
                17,
            ]
        );
        assert_eq!(lengths.into_iter().sum::<usize>(), total);
    }

    #[test]
    fn dense_registry_prefetch_preserves_output_accounts_io_and_second_scan_reuse() {
        let fixture = Fixture::new();
        let request = ScanRequest::all().allow_unverified_source();

        let mut full_source = fixture.open("dense-registry-full-parity");
        let mut full_blocks = Vec::new();
        full_source
            .for_each_block(&request, |block| {
                if block.header.block_ordinal == 1 {
                    full_blocks.push((block.header, block.transactions.to_vec()));
                }
                Ok(())
            })
            .unwrap();

        let policy = IndexerV3RegistryReadPolicy::for_test(64, 1);
        let mut selective_source = fixture.open("dense-registry-selective-parity");
        let mut first_blocks = Vec::new();
        let first = {
            let mut first_sink = OwnedBlockSink {
                output: &mut first_blocks,
            };
            selective_source
                .scan_selected_blocks_with_registry_policy(&request, &[1], policy, &mut first_sink)
                .unwrap()
        };

        assert_eq!(first_blocks, full_blocks);
        assert_eq!(first.registry.mode, IndexerV3RegistryReadMode::FullRegistry);
        assert_eq!(first.registry.prefetch_read_calls, 1);
        assert_eq!(first.registry.prefetch_read_bytes, 64);
        assert!(first.registry.resolutions > 0);
        assert_eq!(first.registry.hits, first.registry.resolutions);
        assert_eq!(first.registry.misses, 0);
        assert_eq!(first.registry.evictions, 0);
        assert_eq!(first.registry.resident_payload_bytes, 64);
        assert!(
            first
                .source_io
                .source_read_bytes
                .is_some_and(|bytes| bytes >= first.registry.prefetch_read_bytes)
        );

        let mut second_blocks = Vec::new();
        let second = {
            let mut second_sink = OwnedBlockSink {
                output: &mut second_blocks,
            };
            selective_source
                .scan_selected_blocks_with_registry_policy(&request, &[1], policy, &mut second_sink)
                .unwrap()
        };

        assert_eq!(second_blocks, first_blocks);
        assert_eq!(
            second.registry.mode,
            IndexerV3RegistryReadMode::FullRegistry
        );
        assert_eq!(second.registry.prefetch_read_calls, 0);
        assert_eq!(second.registry.prefetch_read_bytes, 0);
        assert_eq!(second.registry.resolutions, first.registry.resolutions);
        assert_eq!(second.registry.hits, second.registry.resolutions);
        assert_eq!(second.registry.misses, 0);
        assert_eq!(second.registry.resident_payload_bytes, 64);
    }

    #[test]
    fn dense_ordered_scan_uses_the_same_bounded_registry_policy() {
        let fixture = Fixture::new();
        let request = ScanRequest::all().allow_unverified_source();
        let policy = IndexerV3RegistryReadPolicy::for_test(64, 1);
        let mut source = fixture.open("dense-registry-ordered");
        let mut sink = NullBlockSink;

        let first = source
            .scan_ordered_with_registry_policy(&request, policy, &mut sink)
            .unwrap();
        assert_eq!(source.context.registry_stats.prefetch_read_calls, 1);
        assert_eq!(source.context.registry_stats.prefetch_read_bytes, 64);
        assert_eq!(
            source.context.full_registry.as_deref().map(<[u8]>::len),
            Some(64)
        );
        assert!(first.io.source_read_bytes.is_some_and(|bytes| bytes >= 64));

        source
            .scan_ordered_with_registry_policy(&request, policy, &mut sink)
            .unwrap();
        assert_eq!(source.context.registry_stats.prefetch_read_calls, 1);
        assert_eq!(source.context.registry_stats.prefetch_read_bytes, 64);
    }

    #[test]
    fn parallel_ordered_receipt_reports_shared_registry_state() {
        let fixture = Fixture::new();
        let request = ScanRequest::all()
            .allow_unverified_source()
            .without_instruction_data();
        let mut source = fixture.open("parallel-dense-registry-ordered");
        let mut sink = NullBlockSink;

        let receipt = source
            .scan_ordered_parallel_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::for_test(64, 1),
                NonZeroUsize::new(2).unwrap(),
                &mut sink,
            )
            .unwrap();

        assert_eq!(
            receipt.registry.mode,
            IndexerV3RegistryReadMode::FullRegistry
        );
        assert_eq!(receipt.registry.prefetch_read_calls, 1);
        assert_eq!(receipt.registry.prefetch_read_bytes, 64);
        assert_eq!(receipt.registry.resident_payload_bytes, 64);
    }

    #[test]
    fn parallel_ordered_scan_matches_one_worker_in_exact_order() {
        const BLOCKS: usize = 800;
        let fixture = Fixture::build(&[], raw_parallel_blocks(BLOCKS), None, false, 2_000);
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data()
            .without_primary_signatures();

        let mut sequential = fixture.open("parallel-ordered-sequential");
        let mut sequential_blocks = Vec::new();
        let mut sequential_sink = OwnedBlockSink {
            output: &mut sequential_blocks,
        };
        let mut sequential_receipt = sequential
            .scan_ordered_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::sparse_only(),
                &mut sequential_sink,
            )
            .unwrap();

        let mut parallel = fixture.open("test-parallel-delay-first");
        let mut parallel_blocks = Vec::new();
        let mut parallel_sink = OwnedBlockSink {
            output: &mut parallel_blocks,
        };
        let parallel_run = parallel
            .scan_ordered_parallel_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::sparse_only(),
                NonZeroUsize::new(12).unwrap(),
                &mut parallel_sink,
            )
            .unwrap();
        let stats = parallel_run.parallel;
        let mut parallel_receipt = parallel_run.scan;

        assert_eq!(parallel_blocks, sequential_blocks);
        assert_eq!(stats.requested_workers, 12);
        assert_eq!(stats.effective_workers, 12);
        assert_eq!(stats.max_active_workers, 12);
        assert_eq!(stats.jobs, 200);
        assert_eq!(stats.projected_blocks, BLOCKS as u64);
        assert_eq!(stats.blocks_per_job_limit, 4);
        assert_eq!(stats.job_window_limit, 24);
        assert!(stats.max_in_flight_jobs > 12);
        assert!(stats.max_coordinator_pending_results > 12);
        assert!(stats.max_result_channel_backlog > 0);
        assert!(stats.max_coordinator_pending_projected_blocks > 12 * 4);
        assert_eq!(stats.declared_decoded_byte_limit, 256 << 20);
        assert_eq!(stats.transaction_limit, 100_000);
        assert!(stats.max_in_flight_declared_decoded_bytes <= stats.declared_decoded_byte_limit);
        assert!(stats.max_in_flight_transactions <= stats.transaction_limit);
        assert!(stats.max_owned_payload_block_bytes > 0);
        assert!(stats.max_in_flight_owned_payload_bytes >= stats.max_owned_payload_block_bytes);
        assert_eq!(stats.global_projected_block_bound, 12 * 8);
        assert_eq!(
            parallel_run.registry.mode,
            IndexerV3RegistryReadMode::Unused
        );
        assert_eq!(
            parallel_receipt.io.decoded_bytes,
            sequential_receipt.io.decoded_bytes
        );
        sequential_receipt.io = ScanIoReceipt::default();
        parallel_receipt.io = ScanIoReceipt::default();
        assert_eq!(parallel_receipt, sequential_receipt);
    }

    #[test]
    fn one_parallel_worker_dispatches_next_job_before_ordered_consume() {
        const BLOCKS: usize = INDEXER_V3_PARALLEL_BLOCKS_PER_JOB * 2;
        let fixture = Fixture::build(&[], raw_parallel_blocks(BLOCKS), None, false, 100);
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data()
            .without_primary_signatures();
        let mut source = fixture.open("parallel-one-worker-dispatch-before-consume");
        let mut sink = NullBlockSink;

        let receipt = source
            .scan_ordered_parallel_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::sparse_only(),
                NonZeroUsize::MIN,
                &mut sink,
            )
            .unwrap();
        let stats = receipt.parallel;

        assert_eq!(receipt.scan.blocks, BLOCKS as u64);
        assert_eq!(stats.requested_workers, 1);
        assert_eq!(stats.effective_workers, 1);
        assert_eq!(stats.max_active_workers, 1);
        assert_eq!(stats.jobs, 2);
        assert_eq!(stats.job_window_limit, 2);
        assert_eq!(stats.max_in_flight_jobs, 2);
        assert_eq!(
            stats.max_in_flight_transactions,
            u64::try_from(BLOCKS).unwrap()
        );
        assert_eq!(stats.global_projected_block_bound, BLOCKS);
    }

    #[test]
    fn parallel_selective_scan_matches_one_worker_and_skips_gaps() {
        const BLOCKS: usize = 800;
        let fixture = Fixture::build(&[], raw_parallel_blocks(BLOCKS), None, false, 2_000);
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data()
            .without_primary_signatures();
        let candidates = (0..BLOCKS as u32)
            .filter(|block| block % 2 == 0)
            .collect::<Vec<_>>();

        let mut sequential = fixture.open("parallel-selective-sequential");
        let mut sequential_blocks = Vec::new();
        let mut sequential_sink = OwnedBlockSink {
            output: &mut sequential_blocks,
        };
        let mut sequential_receipt = sequential
            .scan_selected_blocks_with_registry_policy(
                &request,
                &candidates,
                IndexerV3RegistryReadPolicy::sparse_only(),
                &mut sequential_sink,
            )
            .unwrap();

        let mut parallel = fixture.open("parallel-selective-twelve");
        let mut parallel_blocks = Vec::new();
        let mut parallel_sink = OwnedBlockSink {
            output: &mut parallel_blocks,
        };
        let mut parallel_receipt = parallel
            .scan_selected_blocks_parallel_with_registry_policy(
                &request,
                &candidates,
                IndexerV3RegistryReadPolicy::sparse_only(),
                NonZeroUsize::new(12).unwrap(),
                &mut parallel_sink,
            )
            .unwrap();

        assert_eq!(parallel_blocks, sequential_blocks);
        assert_eq!(parallel_receipt.candidate_blocks, candidates.len() as u64);
        assert_eq!(parallel_receipt.skipped_blocks, 400);
        let stats = parallel_receipt.parallel.unwrap();
        assert_eq!(stats.requested_workers, 12);
        assert_eq!(stats.effective_workers, 12);
        assert_eq!(stats.jobs, 100);
        assert_eq!(stats.projected_blocks, candidates.len() as u64);
        assert_eq!(
            parallel_receipt.source_io.decoded_bytes,
            sequential_receipt.source_io.decoded_bytes
        );
        sequential_receipt.source_io = ScanIoReceipt::default();
        parallel_receipt.source_io = ScanIoReceipt::default();
        sequential_receipt.scan_receipt.io = ScanIoReceipt::default();
        parallel_receipt.scan_receipt.io = ScanIoReceipt::default();
        parallel_receipt.parallel = None;
        assert_eq!(parallel_receipt, sequential_receipt);
    }

    #[test]
    fn parallel_scan_rejects_too_many_workers_before_source_io() {
        let fixture = Fixture::new();
        let request = ScanRequest::all().allow_unverified_source();
        let mut source = fixture.open("parallel-worker-limit");
        let before = source.meter.stats().unwrap();
        let mut sink = NullBlockSink;
        let error = source
            .scan_ordered_parallel_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::sparse_only(),
                NonZeroUsize::new(MAX_INDEXER_V3_PARALLEL_WORKERS + 1).unwrap(),
                &mut sink,
            )
            .unwrap_err();
        assert!(matches!(error, QueryError::InvalidRequest(_)));
        assert_eq!(source.meter.stats().unwrap(), before);
    }

    #[test]
    fn parallel_scan_preserves_exact_selected_instruction_data() {
        let fixture = Fixture::new();
        let request = ScanRequest::all()
            .allow_unverified_source()
            .with_instruction_data_for([PROGRAM]);
        let mut sequential_source = fixture.open("parallel-instruction-data-sequential");
        let mut sequential = Vec::new();
        sequential_source
            .scan_ordered_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::sparse_only(),
                &mut OwnedBlockSink {
                    output: &mut sequential,
                },
            )
            .unwrap();

        let mut parallel_source = fixture.open("parallel-instruction-data-parallel");
        let mut parallel = Vec::new();
        let receipt = parallel_source
            .scan_ordered_parallel_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::sparse_only(),
                NonZeroUsize::new(2).unwrap(),
                &mut OwnedBlockSink {
                    output: &mut parallel,
                },
            )
            .unwrap();

        assert_eq!(parallel, sequential);
        assert_eq!(parallel[1].1[0].instructions[0].data, [4, 5]);
        assert_eq!(parallel[1].1[0].instructions[1].data, [6, 7]);
        assert_eq!(receipt.scan.instructions_with_unknown_data, 0);
        assert!(
            receipt
                .scan
                .io
                .source_read_bytes
                .is_some_and(|bytes| bytes > 0)
        );
    }

    #[test]
    fn parallel_scan_reports_the_first_job_error_in_order() {
        let fixture = Fixture::build(&[], raw_parallel_blocks(800), None, false, 2_000);
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data()
            .without_primary_signatures();
        let mut source = fixture.open("test-parallel-errors");
        let mut sink = NullBlockSink;

        let error = source
            .scan_ordered_parallel_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::sparse_only(),
                NonZeroUsize::new(12).unwrap(),
                &mut sink,
            )
            .unwrap_err();

        assert!(error.to_string().contains("job 0"));
        assert!(!error.to_string().contains("job 1"));
    }

    #[test]
    fn parallel_scan_cancels_after_an_ordered_sink_failure() {
        let fixture = Fixture::build(&[], raw_parallel_blocks(800), None, false, 2_000);
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data()
            .without_primary_signatures();
        let mut source = fixture.open("parallel-sink-cancellation");
        let mut sink = FailingBlockSink { visits: 0 };

        let error = source
            .scan_ordered_parallel_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::sparse_only(),
                NonZeroUsize::new(12).unwrap(),
                &mut sink,
            )
            .unwrap_err();

        assert_eq!(sink.visits, 1);
        assert!(
            error
                .to_string()
                .contains("forced parallel V3 sink failure")
        );
    }

    #[test]
    fn parallel_scan_rejects_an_unassigned_result_without_deadlock() {
        let fixture = Fixture::build(&[], raw_parallel_blocks(40), None, false, 100);
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data()
            .without_primary_signatures();
        let mut source = fixture.open("test-parallel-invalid-result");
        let mut sink = NullBlockSink;

        let error = source
            .scan_ordered_parallel_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::sparse_only(),
                NonZeroUsize::new(12).unwrap(),
                &mut sink,
            )
            .unwrap_err();

        assert!(error.to_string().contains("unassigned job"));
    }

    #[test]
    fn parallel_scan_rejects_a_wrong_block_transaction_count() {
        let fixture = Fixture::build(&[], raw_parallel_blocks(40), None, false, 100);
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data()
            .without_primary_signatures();
        let mut source = fixture.open("test-parallel-invalid-transactions");
        let mut sink = NullBlockSink;

        let error = source
            .scan_ordered_parallel_with_registry_policy(
                &request,
                IndexerV3RegistryReadPolicy::sparse_only(),
                NonZeroUsize::new(12).unwrap(),
                &mut sink,
            )
            .unwrap_err();

        assert!(error.to_string().contains("transaction count differs"));
    }

    #[test]
    fn parallel_resource_gate_runs_one_oversized_block_alone() {
        let oversized = ParallelScanJobResources {
            blocks: 1,
            declared_decoded_bytes: INDEXER_V3_PARALLEL_DECLARED_DECODED_BYTE_LIMIT + 1,
            transactions: 1,
        };
        assert_eq!(
            admitted_parallel_resources(ParallelScanJobResources::default(), oversized).unwrap(),
            Some(oversized)
        );
        assert_eq!(
            admitted_parallel_resources(
                ParallelScanJobResources {
                    blocks: 1,
                    declared_decoded_bytes: 1,
                    transactions: 1,
                },
                oversized,
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn parallel_recycle_drops_an_oversized_outer_transaction_buffer() {
        let item_bytes = std::mem::size_of::<CanonicalTransaction>();
        assert!(item_bytes > 0);
        let oversized_capacity =
            INDEXER_V3_PARALLEL_RETAINED_TRANSACTION_BUFFER_LIMIT / item_bytes + 1;
        let oversized = Vec::<CanonicalTransaction>::with_capacity(oversized_capacity);
        let mut pool = Vec::new();

        recycle_parallel_transaction_buffer(&mut pool, oversized);

        assert!(pool.is_empty());
        recycle_parallel_transaction_buffer(&mut pool, Vec::with_capacity(1));
        assert_eq!(pool.len(), 1);

        for _ in 0..INDEXER_V3_PARALLEL_BLOCKS_PER_JOB + 2 {
            recycle_parallel_transaction_buffer(&mut pool, Vec::with_capacity(1));
        }
        assert_eq!(pool.len(), INDEXER_V3_PARALLEL_BLOCKS_PER_JOB);
    }

    #[test]
    fn parallel_worker_reclaims_a_populated_output_on_its_owner_thread() {
        let tracker = Arc::new(ParallelOwnedPayloadTracker::default());
        let worker_tracker = Arc::clone(&tracker);
        let (command_sender, command_receiver) = mpsc::sync_channel(1);
        let (output_sender, output_receiver) = mpsc::channel();
        let (observation_sender, observation_receiver) = mpsc::channel();

        let worker = std::thread::spawn(move || {
            let transaction = CanonicalTransaction {
                header: TransactionHeader {
                    tx_index: 0,
                    status: ExecutionStatus::Unknown(CoverageReason::RawTransaction),
                    failed_outer_instruction_index: None,
                    instruction_coverage: InstructionCoverage::Unknown(
                        CoverageReason::RawTransaction,
                    ),
                    cpi_coverage: CpiCoverage::Unknown(CoverageReason::RawTransaction),
                },
                primary_signature: None,
                required_signers: vec![[1; 32]; 8],
                instructions: Vec::new(),
                token_balance_coverage: TokenBalanceCoverage::NotRequested,
                token_balances: Vec::new(),
            };
            let transactions = vec![transaction];
            let owned_bytes =
                canonical_block_owned_payload_bytes(&transactions, transactions.capacity())
                    .unwrap();
            let mut owned_payload = ParallelOwnedPayloadGuard::new(Arc::clone(&worker_tracker));
            owned_payload.add_block(owned_bytes).unwrap();
            output_sender
                .send(ParallelScanJobOutput {
                    worker: 0,
                    blocks: vec![CanonicalBlock {
                        counts: None,
                        header: BlockHeader {
                            epoch: 7,
                            block_ordinal: 0,
                            slot: FIRST_SLOT,
                        },
                        transactions,
                    }],
                    unused_transaction_buffers: Vec::new(),
                    decoded_bytes: 0,
                    registry: IndexerV3RegistryReadReceipt::default(),
                    owned_payload: owned_payload.finish(),
                })
                .unwrap();

            let mut pool = Vec::new();
            let ParallelWorkerCommand::Recycle(recycle) = command_receiver.recv().unwrap() else {
                panic!("worker expected its populated output for reclamation");
            };
            reclaim_parallel_worker_output(&mut pool, recycle, &mut Default::default());
            observation_sender
                .send((
                    pool.len(),
                    pool.first().is_some_and(Vec::is_empty),
                    worker_tracker.current.load(Ordering::Acquire),
                ))
                .unwrap();
            assert!(matches!(
                command_receiver.recv().unwrap(),
                ParallelWorkerCommand::Shutdown
            ));
        });

        let output = output_receiver.recv().unwrap();
        assert!(tracker.current.load(Ordering::Acquire) > 0);
        return_parallel_output_to_worker(std::slice::from_ref(&command_sender), output).unwrap();
        let (pool_len, buffer_is_empty, owned_bytes) = observation_receiver.recv().unwrap();
        assert_eq!(pool_len, 1);
        assert!(buffer_is_empty);
        assert_eq!(owned_bytes, 0);
        command_sender
            .send(ParallelWorkerCommand::Shutdown)
            .unwrap();
        worker.join().unwrap();
        assert_eq!(tracker.current.load(Ordering::Acquire), 0);
    }

    #[test]
    fn parallel_projection_scratch_drops_oversized_capacities() {
        let mut scratch = TransactionProjectionScratch::default();
        let oversized_keys = INDEXER_V3_PARALLEL_RETAINED_PROJECTION_SCRATCH_LIMIT
            / std::mem::size_of::<[u8; 32]>()
            + 1;
        scratch.account_keys.reserve_exact(oversized_keys);

        assert!(scratch.shed_buffers_above(INDEXER_V3_PARALLEL_RETAINED_PROJECTION_SCRATCH_LIMIT));
        assert_eq!(scratch.account_keys.capacity(), 0);
        assert_eq!(scratch.selected_references.capacity(), 0);
    }

    #[test]
    fn parallel_no_resolution_scan_does_not_load_a_shared_registry() {
        const BLOCKS: usize = 400;
        let fixture = Fixture::build(
            &[SIGNER, PROGRAM],
            raw_parallel_blocks(BLOCKS),
            None,
            false,
            1_000,
        );
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instructions()
            .without_required_signers()
            .without_execution_status()
            .without_primary_signatures();
        let candidates = (0..BLOCKS as u32).collect::<Vec<_>>();
        let mut source = fixture.open("parallel-full-registry-no-resolution");
        let mut sink = NullBlockSink;

        let receipt = source
            .scan_selected_blocks_parallel_with_registry_policy(
                &request,
                &candidates,
                IndexerV3RegistryReadPolicy::for_test(64, 1),
                NonZeroUsize::new(12).unwrap(),
                &mut sink,
            )
            .unwrap();

        assert_eq!(receipt.registry.mode, IndexerV3RegistryReadMode::Unused);
        assert_eq!(receipt.registry.prefetch_read_calls, 0);
        assert_eq!(receipt.registry.prefetch_read_bytes, 0);
        assert_eq!(receipt.registry.resolutions, 0);
        assert_eq!(receipt.registry.resident_payload_bytes, 0);
        assert_eq!(receipt.parallel.unwrap().jobs, 100);
        assert!(!source.release_full_registry_above(64));
        assert!(source.context.full_registry.is_none());
        assert!(!source.release_full_registry_above(63));
        assert!(source.context.full_registry.is_none());
    }

    #[test]
    fn registry_limit_below_complete_file_keeps_sparse_cache() {
        let fixture = Fixture::new();
        let request = ScanRequest::all().allow_unverified_source();
        let mut source = fixture.open("dense-registry-limit");
        let mut sink = NullBlockSink;
        let receipt = source
            .scan_selected_blocks_with_registry_policy(
                &request,
                &[1],
                IndexerV3RegistryReadPolicy::for_test(63, 1),
                &mut sink,
            )
            .unwrap();

        assert_eq!(
            receipt.registry.mode,
            IndexerV3RegistryReadMode::SparseChunkCache
        );
        assert_eq!(receipt.registry.prefetch_read_calls, 0);
        assert_eq!(receipt.registry.prefetch_read_bytes, 0);
        assert!(receipt.registry.resolutions > 0);
        assert_eq!(receipt.registry.misses, 1);
        assert_eq!(
            receipt.registry.hits + receipt.registry.misses,
            receipt.registry.resolutions
        );
        assert_eq!(receipt.registry.resident_payload_bytes, 64);
    }

    #[test]
    fn sparse_registry_metrics_report_lru_eviction_and_bounded_residency() {
        let directory = tempfile::tempdir().unwrap();
        let chunk_count = REGISTRY_CACHE_CHUNKS + 1;
        let registry_entries = chunk_count * REGISTRY_KEYS_PER_CHUNK;
        let mut registry = Vec::with_capacity(registry_entries * REGISTRY_KEY_BYTES);
        for chunk_id in 0..chunk_count {
            for _ in 0..REGISTRY_KEYS_PER_CHUNK {
                registry.extend_from_slice(&[chunk_id as u8; REGISTRY_KEY_BYTES]);
            }
        }
        std::fs::write(
            directory.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            &registry,
        )
        .unwrap();
        let source: Arc<dyn RangeSource> = Arc::new(LocalRangeSource::new(directory.path()));
        let mut context = ExactContext::new(
            source,
            registry_entries as u32,
            SidecarGeometry {
                registry_size: registry.len() as u64,
                signatures_size: None,
                blockhash_size: None,
                previous_blockhash_size: None,
                vote_hash_size: None,
            },
            blockzilla_compact_v2_reader::CompactV2MessageSchema::Current,
        );
        let before = context.registry_stats;
        let mut first_chunk_buffer = None;
        for chunk_id in 0..chunk_count {
            let id = (chunk_id * REGISTRY_KEYS_PER_CHUNK + 1) as u32;
            assert_eq!(
                context.resolve_pubkey(CompactPubkey::Id(id)).unwrap(),
                [chunk_id as u8; REGISTRY_KEY_BYTES]
            );
            if chunk_id == 0 {
                first_chunk_buffer = Some(context.registry_chunks[&0].as_ptr());
            }
        }
        let receipt = context.registry_receipt_since(before).unwrap();

        assert_eq!(receipt.mode, IndexerV3RegistryReadMode::SparseChunkCache);
        assert_eq!(receipt.resolutions, chunk_count as u64);
        assert_eq!(receipt.hits, 0);
        assert_eq!(receipt.misses, chunk_count as u64);
        assert_eq!(receipt.evictions, 1);
        assert_eq!(
            context.registry_chunks[&u32::try_from(chunk_count - 1).unwrap()].as_ptr(),
            first_chunk_buffer.unwrap()
        );
        assert_eq!(
            receipt.resident_payload_bytes,
            INDEXER_V3_QUERY_REGISTRY_RETAINED_KEY_BYTES as u64
        );
    }

    #[test]
    fn parallel_contexts_share_lazy_exact_sidecars() {
        let fixture = Fixture::build(&[], vec![Vec::new()], None, false, 100);
        std::fs::write(
            fixture
                .directory
                .path()
                .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
            [0_u8; VOTE_HASH_RECORD_LEN],
        )
        .unwrap();
        std::fs::write(
            fixture
                .directory
                .path()
                .join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            [7_u8; BLOCKHASH_RECORD_LEN],
        )
        .unwrap();

        let source = fixture.open("shared-exact-sidecars");
        let before = source.meter.stats().unwrap();
        let mut first = source.fork_for_parallel_scan();
        let mut second = source.fork_for_parallel_scan();

        first.context.load_vote_hashes().unwrap();
        second.context.load_vote_hashes().unwrap();
        first.context.load_blockhashes().unwrap();
        second.context.load_blockhashes().unwrap();

        let read = source.meter.stats().unwrap().difference(before).unwrap();
        assert_eq!(read.calls, 2);
        assert_eq!(
            read.bytes,
            u64::try_from(VOTE_HASH_RECORD_LEN + BLOCKHASH_RECORD_LEN).unwrap()
        );
        assert!(Arc::ptr_eq(
            first.context.vote_hashes.as_ref().unwrap(),
            second.context.vote_hashes.as_ref().unwrap(),
        ));
        assert!(Arc::ptr_eq(
            first.context.blockhashes.as_ref().unwrap(),
            second.context.blockhashes.as_ref().unwrap(),
        ));
    }

    #[test]
    fn parallel_contexts_cache_one_malformed_present_vote_sidecar_failure() {
        let fixture = Fixture::build(&[], vec![Vec::new()], None, false, 100);
        let mut invalid_vote_row = vec![0_u8; VOTE_HASH_RECORD_LEN];
        invalid_vote_row[0] = 4;
        std::fs::write(
            fixture
                .directory
                .path()
                .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
            invalid_vote_row,
        )
        .unwrap();

        let source = fixture.open("shared-malformed-vote-sidecar");
        let before = source.meter.stats().unwrap();
        let mut first = source.fork_for_parallel_scan();
        let mut second = source.fork_for_parallel_scan();

        let first_error = first.context.load_vote_hashes().unwrap_err();
        let second_error = second.context.load_vote_hashes().unwrap_err();
        let first_source = match first_error {
            IndexerV3InstructionSourceError::ExactSidecarLoad { object, source } => {
                assert_eq!(object, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE);
                source
            }
            other => panic!("unexpected first vote-sidecar error: {other:?}"),
        };
        let second_source = match second_error {
            IndexerV3InstructionSourceError::ExactSidecarLoad { object, source } => {
                assert_eq!(object, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE);
                source
            }
            other => panic!("unexpected second vote-sidecar error: {other:?}"),
        };

        assert!(Arc::ptr_eq(&first_source, &second_source));
        assert!(matches!(
            first_source.as_ref(),
            IndexerV3InstructionSourceError::SignedMessage(
                SignedMessageError::InvalidVoteHashRegistryFlags {
                    block_id: 0,
                    flags: 4,
                }
            )
        ));
        let read = source.meter.stats().unwrap().difference(before).unwrap();
        assert_eq!(read.calls, 1);
        assert_eq!(read.bytes, VOTE_HASH_RECORD_LEN as u64);
    }

    #[test]
    fn prefetched_registry_keeps_invalid_id_validation() {
        let fixture = Fixture::new();
        let mut sparse = fixture.open("invalid-registry-id-sparse");
        let mut full = fixture.open("invalid-registry-id-full");
        full.context.prefetch_full_registry().unwrap();

        for id in [0, 3] {
            let sparse_error = sparse
                .context
                .resolve_pubkey(CompactPubkey::Id(id))
                .unwrap_err();
            let full_error = full
                .context
                .resolve_pubkey(CompactPubkey::Id(id))
                .unwrap_err();
            assert_eq!(sparse_error.to_string(), full_error.to_string());
        }
    }

    #[test]
    fn selective_scan_empty_candidates_still_validates_request_and_reads_nothing() {
        let fixture = Fixture::build(
            &[],
            vec![
                vec![FixtureTransaction::raw_transaction(vec![1])],
                vec![FixtureTransaction::raw_transaction(vec![2])],
            ],
            None,
            false,
            100,
        );
        let mut source = fixture.open("selective-empty");
        let invalid_request = ScanRequest::all()
            .allow_unverified_source()
            .with_instruction_data_for([PROGRAM, PROGRAM]);
        let mut sink = NullBlockSink;
        assert!(matches!(
            source.scan_selected_blocks(&invalid_request, &[], &mut sink),
            Err(QueryError::InvalidRequest(_))
        ));

        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data();
        let before = source.meter.stats().unwrap();
        let receipt = source
            .scan_selected_blocks(&request, &[], &mut sink)
            .unwrap();
        let after = source.meter.stats().unwrap();

        assert_eq!(before, after);
        assert_eq!(receipt.requested_blocks, 2);
        assert_eq!(receipt.requested_transactions, 2);
        assert_eq!(receipt.candidate_blocks, 0);
        assert_eq!(receipt.candidate_transactions, 0);
        assert_eq!(receipt.skipped_blocks, 2);
        assert_eq!(receipt.skipped_transactions, 2);
        assert_eq!(
            receipt.scan_receipt,
            ScanReceipt {
                io: ScanIoReceipt {
                    source_read_calls: Some(0),
                    source_read_bytes: Some(0),
                    decoded_bytes: Some(0),
                    cache_read_calls: None,
                    cache_read_bytes: None,
                },
                ..ScanReceipt::default()
            }
        );
        assert_eq!(receipt.source_io, receipt.scan_receipt.io);
        assert_eq!(
            receipt.registry,
            IndexerV3RegistryReadReceipt {
                mode: IndexerV3RegistryReadMode::Unused,
                prefetch_read_calls: 0,
                prefetch_read_bytes: 0,
                resolutions: 0,
                hits: 0,
                misses: 0,
                evictions: 0,
                resident_payload_bytes: 0,
            }
        );
    }

    #[test]
    fn selective_scan_rejects_malformed_and_out_of_range_candidates() {
        let fixture = Fixture::build(
            &[],
            vec![Vec::new(), Vec::new(), Vec::new()],
            None,
            false,
            100,
        );
        let mut source = fixture.open("selective-invalid-candidates");
        let request = ScanRequest::all().allow_unverified_source();
        let mut sink = NullBlockSink;

        for candidates in [&[0, 0][..], &[2, 1], &[3]] {
            assert!(matches!(
                source.scan_selected_blocks(&request, candidates, &mut sink),
                Err(QueryError::InvalidRequest(_))
            ));
        }
        let bounded = ScanRequest::bounded(ScanRange {
            first_block: 1,
            block_count: NonZeroU32::new(1).unwrap(),
        })
        .allow_unverified_source();
        assert!(matches!(
            source.scan_selected_blocks(&bounded, &[0], &mut sink),
            Err(QueryError::InvalidRequest(_))
        ));
    }

    #[test]
    fn direct_v3_states_cover_raw_absent_raw_metadata_cpi_and_failure() {
        let base_message = || {
            legacy_message(
                vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                vec![raw_instruction(1, &[0], &[7])],
            )
        };
        let transactions = vec![
            FixtureTransaction::raw_transaction(vec![0xff]),
            FixtureTransaction::absent(base_message()),
            FixtureTransaction::raw_metadata(base_message()),
            FixtureTransaction::exact(base_message(), metadata(2, None, None, vec![], vec![]), 0),
            FixtureTransaction::exact(
                base_message(),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            ),
            FixtureTransaction::exact(
                base_message(),
                metadata(
                    2,
                    Some(CompactTransactionError::InstructionError(
                        0,
                        CompactInstructionError::Custom(42),
                    )),
                    Some(Vec::new()),
                    vec![],
                    vec![],
                ),
                ARCHIVE_V2_TX_FLAG_HAS_ERROR | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            ),
        ];
        let fixture = Fixture::build(&[SIGNER, PROGRAM], vec![transactions], None, false, 100);
        let mut source = fixture.open("direct-state-fixture");
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution();
        let mut observed = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                observed.extend(block.transactions.to_vec());
                Ok(())
            })
            .unwrap();

        assert_eq!(observed.len(), 6);
        assert_eq!(
            observed[0].header.instruction_coverage,
            InstructionCoverage::Unknown(CoverageReason::RawTransaction)
        );
        assert_eq!(
            observed[1].header.status,
            ExecutionStatus::Unknown(CoverageReason::MetadataAbsent)
        );
        assert_eq!(
            observed[1].header.cpi_coverage,
            CpiCoverage::Unknown(CoverageReason::MetadataAbsent)
        );
        assert_eq!(
            observed[2].header.status,
            ExecutionStatus::Unknown(CoverageReason::RawMetadata)
        );
        assert_eq!(observed[3].header.cpi_coverage, CpiCoverage::NotRecorded);
        assert_eq!(observed[4].header.cpi_coverage, CpiCoverage::Complete);
        assert_eq!(observed[5].header.status, ExecutionStatus::Failed);
        assert_eq!(observed[5].header.failed_outer_instruction_index, Some(0));
        assert_eq!(receipt.transactions, 6);
        assert_eq!(receipt.transactions_with_unknown_execution, 3);
        assert_eq!(receipt.transactions_with_incomplete_instructions, 1);
        assert!(receipt.io.source_read_calls.is_some_and(|calls| calls > 0));
    }

    #[test]
    fn direct_v3_v0_resolves_loaded_writable_and_readonly_accounts() {
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: message_header(1),
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([14; 32]),
            instructions: vec![raw_instruction(1, &[2, 3], &[9, 8])],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(3),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let transaction = FixtureTransaction::exact(
            message,
            metadata(
                4,
                None,
                Some(vec![CompactInnerInstructions {
                    index: 0,
                    instructions: vec![CompactInnerInstruction {
                        program_id_index: 3,
                        accounts: vec![2],
                        data: vec![7, 7],
                        stack_height: Some(2),
                    }],
                }]),
                vec![CompactPubkey::Id(4)],
                vec![CompactPubkey::Id(5)],
            ),
            ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        let fixture = Fixture::build(
            &[
                SIGNER,
                PROGRAM,
                LOOKUP_TABLE,
                LOADED_WRITABLE,
                LOADED_READONLY,
            ],
            vec![vec![transaction]],
            None,
            false,
            100,
        );
        let mut source = fixture.open("v0-loaded-fixture");
        let request = ScanRequest::all()
            .allow_unverified_source()
            .with_instruction_data_for([PROGRAM]);
        let mut observed = None;
        source
            .for_each_block(&request, |block| {
                observed = block.transactions.first().cloned();
                Ok(())
            })
            .unwrap();
        let transaction = observed.unwrap();
        assert_eq!(transaction.required_signers, [SIGNER]);
        assert_eq!(transaction.instructions.len(), 2);
        assert_eq!(transaction.instructions[0].program_id, Some(PROGRAM));
        assert_eq!(
            transaction.instructions[0].accounts,
            [LOADED_WRITABLE, LOADED_READONLY]
        );
        assert_eq!(transaction.instructions[0].data, [9, 8]);
        assert_eq!(
            transaction.instructions[0].data_coverage,
            InstructionDataCoverage::Exact
        );
        assert_eq!(
            transaction.instructions[1].program_id,
            Some(LOADED_READONLY)
        );
        assert_eq!(transaction.instructions[1].accounts, [LOADED_WRITABLE]);
        assert_eq!(transaction.instructions[1].coordinate.inner_index, Some(0));

        let mut source = fixture.open("v0-loaded-program-only-fixture");
        let mut program_only = None;
        source
            .for_each_block(&request.without_instruction_accounts(), |block| {
                program_only = block.transactions.first().cloned();
                Ok(())
            })
            .unwrap();
        let program_only = program_only.unwrap();
        let mut expected = transaction;
        for instruction in &mut expected.instructions {
            instruction.accounts.clear();
        }
        assert_eq!(program_only, expected);
        assert!(program_only.instructions.iter().all(|instruction| {
            instruction.accounts.is_empty() && instruction.accounts.capacity() == 0
        }));
    }

    #[test]
    fn program_only_projection_skips_v3_instruction_account_resolution_and_allocation() {
        let message = legacy_message(
            vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(2),
                CompactPubkey::Id(3),
                CompactPubkey::Id(4),
            ],
            vec![raw_instruction(1, &[2, 3], &[])],
        );
        let transaction = FixtureTransaction::exact(
            message,
            metadata(4, None, Some(Vec::new()), vec![], vec![]),
            ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        let fixture = Fixture::build(
            &[SIGNER, PROGRAM, [0xa1; 32], [0xa2; 32]],
            vec![vec![transaction]],
            None,
            false,
            100,
        );

        struct CaptureSink {
            transaction: Option<CanonicalTransaction>,
            account_capacity_is_zero: bool,
        }

        impl BlockSink for CaptureSink {
            fn visit_block(&mut self, block: BlockView<'_>) -> blockzilla_model::Result<()> {
                let transaction = &block.transactions[0];
                self.account_capacity_is_zero = transaction
                    .instructions
                    .iter()
                    .all(|instruction| instruction.accounts.capacity() == 0);
                self.transaction = Some(transaction.clone());
                Ok(())
            }
        }

        let run = |request: &ScanRequest| {
            let mut source = fixture.open("v3-program-only-fixture");
            let mut sink = CaptureSink {
                transaction: None,
                account_capacity_is_zero: false,
            };
            let receipt = source
                .scan_selected_blocks_with_registry_policy(
                    request,
                    &[0],
                    IndexerV3RegistryReadPolicy::sparse_only(),
                    &mut sink,
                )
                .unwrap();
            (
                sink.transaction.unwrap(),
                sink.account_capacity_is_zero,
                receipt.registry.resolutions,
            )
        };

        let full_request = ScanRequest::all()
            .allow_unverified_source()
            .without_primary_signatures()
            .without_instruction_data();
        let (full, _, full_resolutions) = run(&full_request);
        let (program_only, no_account_allocation, program_only_resolutions) =
            run(&full_request.clone().without_instruction_accounts());

        let mut expected = full;
        for instruction in &mut expected.instructions {
            instruction.accounts.clear();
        }
        assert_eq!(program_only, expected);
        assert!(no_account_allocation);
        assert_eq!(program_only_resolutions, 2);
        assert_eq!(full_resolutions, 4);
    }

    #[test]
    fn program_only_projection_rejects_an_invalid_v3_instruction_account_index() {
        let transaction = FixtureTransaction::exact(
            legacy_message(
                vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                vec![raw_instruction(1, &[2], &[])],
            ),
            metadata(2, None, Some(Vec::new()), vec![], vec![]),
            ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        let fixture = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![transaction]],
            None,
            false,
            100,
        );
        let mut source = fixture.open("v3-program-only-invalid-account-fixture");
        let request = ScanRequest::all()
            .allow_unverified_source()
            .without_primary_signatures()
            .without_instruction_accounts()
            .without_instruction_data();
        let mut published_blocks = 0;
        let error = source
            .for_each_block(&request, |_| {
                published_blocks += 1;
                Ok(())
            })
            .unwrap_err();

        assert_eq!(published_blocks, 0);
        let QueryError::Source { source, .. } = error else {
            panic!("unexpected query error: {error}");
        };
        assert!(
            source.to_string().contains("account index"),
            "unexpected source error: {source}"
        );
    }

    #[test]
    fn direct_v3_signature_ambiguity_is_exact_or_explicitly_unavailable() {
        let signing_key = SigningKey::from_bytes(&[44; 32]);
        let signer = signing_key.verifying_key().to_bytes();
        let compact_data = vote_tower_data(false);
        let candidates = reconstruct_instruction_data_candidates(&compact_data, None).unwrap();
        assert_eq!(candidates.len(), 2);
        let selected_data = candidates[1].bytes.to_vec();
        let signed = serialize_signed_message(&SignedMessage {
            version: SignedMessageVersion::Legacy,
            header: message_header(1),
            static_account_keys: &[signer, VOTE_PROGRAM],
            recent_blockhash: [13; 32],
            instructions: &[SignedInstruction {
                program_id_index: 1,
                accounts: &[],
                data: &selected_data,
            }],
        })
        .unwrap();
        let signature = signing_key.sign(&signed).to_bytes();
        let transaction = || {
            FixtureTransaction::exact(
                legacy_message(
                    vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                    vec![ArchiveV2HotInstruction {
                        program_id_index: 1,
                        accounts: Vec::new(),
                        data: vote_tower_data(false),
                    }],
                ),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )
        };

        let proved = Fixture::build(
            &[signer, VOTE_PROGRAM],
            vec![vec![transaction()]],
            Some(&[signature]),
            false,
            100,
        );
        let signature_reads = Arc::new(Mutex::new(Vec::new()));
        let tracking_source = SignatureTrackingSource {
            inner: LocalRangeSource::new(proved.directory.path()),
            signature_reads: signature_reads.clone(),
        };
        let mut source = IndexerV3InstructionSource::open_with_source(
            Arc::new(tracking_source),
            "proved-ambiguity",
            FIRST_SLOT,
            "proved-ambiguity-binding",
        )
        .unwrap();
        let mut exact = None;
        source
            .for_each_transaction(
                &ScanRequest::all()
                    .allow_unverified_source()
                    .without_primary_signatures(),
                |transaction| {
                    exact = Some((
                        transaction.instructions[0].data.clone(),
                        transaction.primary_signature.copied(),
                    ));
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(exact, Some((selected_data.clone(), None)));
        assert_eq!(
            *signature_reads
                .lock()
                .expect("signature-read mutex poisoned"),
            vec![(0, SIGNATURE_BYTES)]
        );

        let mut parallel_source = proved.open("proved-ambiguity-parallel");
        let mut parallel_blocks = Vec::new();
        let parallel_receipt = parallel_source
            .scan_ordered_parallel_with_registry_policy(
                &ScanRequest::all()
                    .allow_unverified_source()
                    .without_primary_signatures(),
                IndexerV3RegistryReadPolicy::sparse_only(),
                NonZeroUsize::new(1).unwrap(),
                &mut OwnedBlockSink {
                    output: &mut parallel_blocks,
                },
            )
            .unwrap();
        assert_eq!(parallel_blocks.len(), 1);
        assert_eq!(parallel_blocks[0].1[0].instructions[0].data, selected_data);
        assert_eq!(parallel_blocks[0].1[0].primary_signature, None);
        assert_eq!(parallel_receipt.scan.instructions_with_unknown_data, 0);

        let missing = Fixture::build(
            &[signer, VOTE_PROGRAM],
            vec![vec![transaction()]],
            None,
            false,
            100,
        );
        let mut strict = missing.open("missing-ambiguity-strict");
        assert!(
            strict
                .for_each_block(&ScanRequest::all().allow_unverified_source(), |_| Ok(()))
                .is_err()
        );
        let mut relaxed = missing.open("missing-ambiguity-relaxed");
        let mut coverage = None;
        let receipt = relaxed
            .for_each_transaction(
                &ScanRequest::all()
                    .allow_unverified_source()
                    .allow_incomplete_instruction_data(),
                |transaction| {
                    coverage = Some((
                        transaction.instructions[0].data_coverage,
                        transaction.instructions[0].data.clone(),
                    ));
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            coverage,
            Some((
                InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable),
                Vec::new()
            ))
        );
        assert_eq!(receipt.instructions_with_unknown_data, 1);

        let inconsistent = Fixture::build(
            &[signer, VOTE_PROGRAM],
            vec![vec![transaction()]],
            Some(&[[0; 64]]),
            false,
            100,
        );
        let mut inconsistent = inconsistent.open("bad-signature-ambiguity");
        assert!(
            inconsistent
                .for_each_block(
                    &ScanRequest::all()
                        .allow_unverified_source()
                        .allow_incomplete_instruction_data(),
                    |_| Ok(()),
                )
                .is_err()
        );
    }

    #[test]
    fn relaxed_v3_preserves_exact_data_across_missing_vote_and_blockhash_proof() {
        let vote_transaction = || {
            FixtureTransaction::exact(
                legacy_message(
                    vec![
                        CompactPubkey::Id(1),
                        CompactPubkey::Id(2),
                        CompactPubkey::Id(3),
                    ],
                    vec![
                        raw_instruction(1, &[0], &[3, 11]),
                        ArchiveV2HotInstruction {
                            program_id_index: 2,
                            accounts: Vec::new(),
                            data: vote_tower_data(true),
                        },
                    ],
                ),
                metadata(3, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )
        };
        let missing_vote = Fixture::build(
            &[SIGNER, PROGRAM, VOTE_PROGRAM],
            vec![vec![vote_transaction()]],
            None,
            false,
            100,
        );
        let mut source = missing_vote.open("missing-vote-proof");
        let mut coverage = Vec::new();
        let receipt =
            source
                .for_each_transaction(
                    &ScanRequest::all()
                        .allow_unverified_source()
                        .allow_incomplete_instruction_data(),
                    |transaction| {
                        coverage.extend(transaction.instructions.iter().map(|instruction| {
                            (instruction.data_coverage, instruction.data.clone())
                        }));
                        Ok(())
                    },
                )
                .unwrap();
        assert_eq!(coverage[0], (InstructionDataCoverage::Exact, vec![3, 11]));
        assert_eq!(
            coverage[1],
            (
                InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable),
                Vec::new()
            )
        );
        assert_eq!(receipt.instructions_with_unknown_data, 1);

        let malformed_vote = Fixture::build(
            &[SIGNER, PROGRAM, VOTE_PROGRAM],
            vec![vec![vote_transaction()]],
            None,
            false,
            100,
        );
        let mut invalid_vote_row = vec![0u8; VOTE_HASH_RECORD_LEN];
        invalid_vote_row[0] = 4;
        std::fs::write(
            malformed_vote
                .directory
                .path()
                .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
            invalid_vote_row,
        )
        .unwrap();
        let mut malformed_source = malformed_vote.open("malformed-vote-proof");
        assert!(
            malformed_source
                .for_each_block(
                    &ScanRequest::all()
                        .allow_unverified_source()
                        .allow_incomplete_instruction_data(),
                    |_| Ok(()),
                )
                .is_err()
        );

        let incomplete_vote = Fixture::build(
            &[SIGNER, PROGRAM, VOTE_PROGRAM],
            vec![vec![vote_transaction()]],
            None,
            false,
            100,
        );
        std::fs::write(
            incomplete_vote
                .directory
                .path()
                .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
            [],
        )
        .unwrap();
        let mut incomplete_source = incomplete_vote.open("incomplete-vote-proof");
        assert!(
            incomplete_source
                .for_each_block(
                    &ScanRequest::all()
                        .allow_unverified_source()
                        .allow_incomplete_instruction_data(),
                    |_| Ok(()),
                )
                .is_err(),
            "a present registry with no referenced row is a hard source error"
        );

        let blockhash_message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: message_header(2),
            account_keys: vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(2),
                CompactPubkey::Id(3),
            ],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![
                raw_instruction(1, &[0], &[5]),
                ArchiveV2HotInstruction {
                    program_id_index: 2,
                    accounts: Vec::new(),
                    data: vote_tower_data(false),
                },
            ],
        });
        let missing_blockhash = Fixture::build(
            &[SIGNER, PROGRAM, VOTE_PROGRAM],
            vec![vec![FixtureTransaction::exact(
                blockhash_message,
                metadata(3, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )]],
            Some(&[[0; 64]]),
            false,
            100,
        );
        let mut source = missing_blockhash.open("missing-blockhash-proof");
        let mut coverage = Vec::new();
        source
            .for_each_transaction(
                &ScanRequest::all()
                    .allow_unverified_source()
                    .allow_incomplete_instruction_data(),
                |transaction| {
                    coverage.extend(
                        transaction.instructions.iter().map(|instruction| {
                            (instruction.data_coverage, instruction.data.clone())
                        }),
                    );
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(coverage[0], (InstructionDataCoverage::Exact, vec![5]));
        assert_eq!(
            coverage[1].0,
            InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable)
        );
        assert!(coverage[1].1.is_empty());
    }

    #[test]
    fn direct_v3_prefix_and_full_signature_geometry_are_distinct() {
        let ledger_objects = indexer_v3_required_ledger_objects().collect::<Vec<_>>();
        assert_eq!(ledger_objects.len(), 12);
        assert_eq!(ledger_objects[0], INDEX_FILE);
        assert_eq!(INDEXER_V3_REQUIRED_RETAINED_SIDECARS, ["registry.bin"]);
        assert_eq!(INDEXER_V3_OPTIONAL_RETAINED_SIDECARS.len(), 4);

        let transaction = || {
            FixtureTransaction::exact(
                legacy_message(
                    vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                    vec![raw_instruction(1, &[0], &[1])],
                ),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )
        };
        let signatures = [[8; 64], [9; 64]];
        let prefix = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![transaction()]],
            Some(&signatures),
            true,
            100,
        );
        let prefix_source = prefix.open("prefix-extra-signatures");
        assert_eq!(prefix_source.scope(), IndexerV3SourceScope::SelectedPrefix);

        let full_extra = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![transaction()]],
            Some(&signatures),
            false,
            100,
        );
        assert!(
            IndexerV3InstructionSource::open_local(
                full_extra.directory.path(),
                FIRST_SLOT,
                "full-extra-signatures",
            )
            .is_err()
        );

        let full_exact = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![transaction()]],
            Some(&signatures[..1]),
            false,
            100,
        );
        let full_source = full_exact.open("full-exact-signatures");
        assert_eq!(full_source.scope(), IndexerV3SourceScope::FullSelection);
    }

    #[test]
    fn v3_sidecar_practical_caps_reject_before_body_reads() {
        struct SizeOverrideSource {
            inner: LocalRangeSource,
            object: &'static str,
            size: u64,
            body_reads: Arc<AtomicUsize>,
        }

        impl RangeSource for SizeOverrideSource {
            fn size(&self, object: &str) -> blockzilla_source::SourceResult<Option<u64>> {
                if object == self.object {
                    Ok(Some(self.size))
                } else {
                    self.inner.size(object)
                }
            }

            fn read_range(
                &self,
                object: &str,
                offset: u64,
                length: usize,
            ) -> blockzilla_source::SourceResult<Vec<u8>> {
                if object == self.object {
                    self.body_reads.fetch_add(1, Ordering::Relaxed);
                }
                self.inner.read_range(object, offset, length)
            }
        }

        let fixture = Fixture::build(&[SIGNER, PROGRAM], vec![Vec::new()], None, false, 3_000_000);
        for (object, cap, record_len) in [
            (
                ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
                MAX_BLOCKHASH_REGISTRY_BYTES,
                BLOCKHASH_RECORD_LEN,
            ),
            (
                ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
                MAX_VOTE_HASH_REGISTRY_BYTES,
                VOTE_HASH_RECORD_LEN,
            ),
        ] {
            let oversized = (cap / record_len + 1) * record_len;
            let body_reads = Arc::new(AtomicUsize::new(0));
            let source = SizeOverrideSource {
                inner: LocalRangeSource::new(fixture.directory.path()),
                object,
                size: oversized as u64,
                body_reads: body_reads.clone(),
            };
            let error = IndexerV3InstructionSource::open_with_source(
                Arc::new(source),
                format!("oversized-{object}"),
                FIRST_SLOT,
                format!("oversized-{object}-binding"),
            )
            .expect_err("oversized sidecar must fail at open");
            assert!(error.to_string().contains("invalid V3 geometry"), "{error}");
            assert_eq!(body_reads.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn direct_v3_rejects_invalid_registry_id_and_stops_on_sink_error() {
        let invalid = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![FixtureTransaction::exact(
                legacy_message(
                    vec![CompactPubkey::Id(1), CompactPubkey::Id(3)],
                    vec![raw_instruction(1, &[0], &[1])],
                ),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )]],
            None,
            false,
            100,
        );
        let mut invalid_source = invalid.open("invalid-registry-id");
        assert!(
            invalid_source
                .for_each_block(&ScanRequest::all().allow_unverified_source(), |_| Ok(()))
                .is_err()
        );

        let transaction = || {
            FixtureTransaction::exact(
                legacy_message(
                    vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                    vec![raw_instruction(1, &[0], &[1])],
                ),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )
        };
        let fixture = Fixture::build(
            &[SIGNER, PROGRAM],
            vec![vec![transaction()], vec![transaction()]],
            None,
            false,
            100,
        );
        let mut source = fixture.open("sink-stop");
        let mut visits = 0usize;
        let error = source
            .for_each_block(&ScanRequest::all().allow_unverified_source(), |_| {
                visits += 1;
                Err(QueryError::InvalidTransaction("test sink stop".into()))
            })
            .unwrap_err();
        assert_eq!(visits, 1);
        assert!(error.to_string().contains("test sink stop"));
    }
}
