//! Sequential source-neutral instruction projection for Compact V2 archives.
//!
//! This is the reference adapter. It uses the admitted `ArchiveReader`, keeps
//! bounded registry chunks or one policy-bounded complete registry across the
//! scan, reads each block signature window once, and publishes only through
//! `OrderedBlockPublisher`.

use std::{
    collections::{HashMap, VecDeque},
    mem::size_of,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crate::query_keys::BoundQueryKeys;
use blockzilla_format::{
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, ArchiveV2HotTxRow,
    CompactPubkey, CompactTokenBalance, OwnedCompactRecentBlockhash,
};
use blockzilla_query_sdk::{
    ArchiveFormat, ArchiveInstructionSource, BlockHeader, BlockSink, CanonicalBlock,
    CanonicalTransaction, CoverageReason, CpiCoverage, Error as QueryError, ExecutionStatus,
    InstructionCoordinate, InstructionCoverage, InstructionDataCoverage,
    InstructionDataRequirement, OrderedBlockPublisher, RecordedTokenBalance, ResolvedInstruction,
    ScanIoReceipt, ScanReceipt, ScanRequest, SourceIdentity, SourceVerification,
    TokenBalanceCoverage, TokenBalanceRequirement, TokenBalanceSide, TransactionHeader,
};
use thiserror::Error;

use crate::blockhash::blockhash_registry_offset;
use crate::{
    ArchiveReader, ArchiveReaderSourceKind, BlockhashResolver, BlockhashResolverError,
    CompactV2ExecutionStatus, CompactV2MessageProjectionError, CompactV2MessageProjector,
    CompactV2MetadataProjectionError, CompactV2MetadataProjectionLimits,
    CompactV2MetadataProjector, MAX_BLOCKHASH_REGISTRY_BYTES, MAX_ORDERED_PARALLEL_DECODE_WORKERS,
    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES, MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS,
    MAX_VOTE_HASH_REGISTRY_BYTES, OrderedParallelBlockConfig, OrderedParallelBlockStats,
    PreviousBlockhashTail, PreviousBlockhashTailSchema, ProjectedCompactV2Message,
    ProjectedCompactV2MessageVersion, RangeSource, SignedInstructionCandidates,
    SignedMessageCandidates, SignedMessageError, SignedMessageVersion, VoteHashRegistry,
    VoteHashResolver, parse_previous_blockhash_tail,
};

const REGISTRY_KEY_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 64;
const REGISTRY_KEYS_PER_CHUNK: usize = 2_048;
const REGISTRY_CACHE_CHUNKS: usize = 8;
const PREVIOUS_BLOCKHASH_RECORDS: usize = 300;
const MAX_SIGNATURE_BYTES_PER_BLOCK: usize = 256 * 1024 * 1024;
/// Keep one sequential signature request within the public range gateway cap.
const MAX_SIGNATURE_BATCH_BYTES: usize = 32 << 20;

/// Maximum retained public-key payload bytes in the registry chunk cache.
///
/// This value does not include `HashMap`, `Vec`, or allocator overhead.
pub const COMPACT_V2_QUERY_REGISTRY_RETAINED_KEY_BYTES: usize =
    REGISTRY_KEYS_PER_CHUNK * REGISTRY_KEY_BYTES * REGISTRY_CACHE_CHUNKS;

/// Maximum compressed allocation tokens retained by a parallel Compact V2 scan.
pub const COMPACT_V2_PARALLEL_COMPRESSED_BUFFERS: usize = 3;
/// Maximum block projections retained before the ordered consumer receives them.
pub const COMPACT_V2_PARALLEL_MAX_BLOCKS_PER_BATCH: usize = 64;
/// Maximum declared decompressed bytes retained in one parallel batch.
pub const COMPACT_V2_PARALLEL_UNCOMPRESSED_BATCH_BYTES: usize = 32 * 1024 * 1024;
/// Maximum payload retained by each of the two reusable projection buffers.
pub const COMPACT_V2_PROJECTION_SCRATCH_RETAINED_BYTES: usize = 1024 * 1024;
/// Default limit for one immutable registry used by one dense scan.
pub const DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES: u64 = 1 << 30;
/// Small partial scans keep the sparse cache below this transaction count.
pub const COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS: u64 = 1_000_000;
/// Largest source read used to prefetch the shared registry.
pub const COMPACT_V2_REGISTRY_PREFETCH_READ_BYTES: usize = 32 * 1024 * 1024;

/// Policy for automatic full-registry prefetch during a sequential scan.
///
/// A nonzero limit permits one complete immutable registry when the request is
/// a full archive scan or contains at least one million transactions. The
/// request must select fields that need public-key resolution, and the exact
/// registry payload must fit the supplied limit. A zero-byte limit keeps the
/// bounded eight-chunk LRU path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactV2RegistryReadPolicy {
    max_full_registry_bytes: u64,
    min_requested_transactions: u64,
}

impl CompactV2RegistryReadPolicy {
    /// Disable full-registry prefetch.
    pub const fn sparse_only() -> Self {
        Self {
            max_full_registry_bytes: 0,
            min_requested_transactions: COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
        }
    }

    /// Permit automatic prefetch up to the supplied complete-registry size.
    pub const fn with_full_registry_limit(max_full_registry_bytes: u64) -> Self {
        Self {
            max_full_registry_bytes,
            min_requested_transactions: COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
        }
    }

    pub const fn max_full_registry_bytes(self) -> u64 {
        self.max_full_registry_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactV2ParallelRegistryMode {
    /// One complete immutable registry is shared across all workers.
    SharedFull,
    /// Each used worker has its own bounded eight-chunk LRU cache.
    SparseWorkerCache,
}

impl std::fmt::Display for CompactV2ParallelRegistryMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SharedFull => formatter.write_str("shared-full"),
            Self::SparseWorkerCache => formatter.write_str("sparse-worker-cache"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactV2ParallelRegistryReceipt {
    pub mode: CompactV2ParallelRegistryMode,
    /// Bounded source calls used by the one-pass complete-registry prefetch.
    pub prefetch_read_calls: u64,
    /// Source bytes used by the one-pass complete-registry prefetch.
    pub prefetch_read_bytes: u64,
    /// Exact shared payload for `SharedFull`; a checked worst-case retained
    /// worker-cache payload for `SparseWorkerCache`.
    pub resident_bound_bytes: u64,
}

/// Worker selection for an ordered parallel Compact V2 scan.
///
/// The scan keeps one monotonic source reader and publishes results in exact
/// block-index order. Workers borrow current-schema transaction lanes from
/// recycled decompression storage while they build the owned canonical
/// projection required by the common query SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactV2ParallelScanConfig {
    /// Parallel zstd-decode and canonical-projection workers.
    pub workers: usize,
    /// Largest complete registry that can be shared by all workers. Zero
    /// keeps the sparse worker-local cache. A nonzero limit applies to full
    /// scans and partial scans with at least one million requested
    /// transactions.
    pub max_full_registry_bytes: u64,
}

impl CompactV2ParallelScanConfig {
    pub const fn new(workers: usize) -> Self {
        Self {
            workers,
            max_full_registry_bytes: DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES,
        }
    }

    pub const fn with_full_registry_limit(mut self, bytes: u64) -> Self {
        self.max_full_registry_bytes = bytes;
        self
    }
}

impl Default for CompactV2ParallelScanConfig {
    fn default() -> Self {
        let workers = thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .min(MAX_ORDERED_PARALLEL_DECODE_WORKERS);
        Self::new(workers)
    }
}

/// Common query receipt plus bounded parallel-pipeline measurements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactV2ParallelScanReceipt {
    pub scan: ScanReceipt,
    pub pipeline: OrderedParallelBlockStats,
    /// Worker count supplied in [`CompactV2ParallelScanConfig`].
    pub requested_workers: usize,
    /// Distinct private-pool workers that decoded at least one block.
    pub effective_workers: usize,
    /// Peak simultaneous decode-and-project callbacks.
    pub max_active_workers: usize,
    pub compressed_buffer_count: usize,
    /// Largest owned canonical payload for one projected block.
    pub max_projected_block_bytes: u64,
    /// Largest owned canonical payload waiting for ordered delivery.
    pub max_projected_batch_bytes: u64,
    pub registry: CompactV2ParallelRegistryReceipt,
    pub signature_read_wall_time: Duration,
    pub signature_assign_wall_time: Duration,
    /// Canonical validation and the application sink, excluding signature reads.
    pub publish_wall_time: Duration,
}

#[derive(Debug, Error)]
pub enum CompactV2InstructionSourceError {
    #[error("Compact V2 reader error: {0}")]
    Reader(#[from] crate::Error),

    #[error("Compact V2 range source error: {0}")]
    RangeSource(#[from] crate::SourceError),

    #[error("Compact V2 message projection error: {0}")]
    Message(#[from] CompactV2MessageProjectionError),

    #[error("Compact V2 metadata projection error: {0}")]
    Metadata(#[from] CompactV2MetadataProjectionError),

    #[error("Compact V2 signed-message error: {0}")]
    SignedMessage(#[from] SignedMessageError),

    #[error("Compact V2 blockhash error: {0}")]
    Blockhash(#[from] BlockhashResolverError),

    #[error("Compact V2 sidecar {object} is required for {purpose}")]
    MissingSidecar {
        object: &'static str,
        purpose: &'static str,
    },

    #[error("invalid Compact V2 instruction source: {0}")]
    Invalid(String),
}

#[derive(Debug, Error)]
enum CompactV2ParallelScanError {
    #[error("parallel Compact V2 reader failed")]
    Reader(#[from] crate::Error),
    #[error("parallel Compact V2 projection failed")]
    Projection(#[from] CompactV2InstructionSourceError),
    #[error(transparent)]
    Query(#[from] QueryError),
}

impl CompactV2ParallelScanError {
    fn into_query_error(self) -> QueryError {
        match self {
            Self::Query(error) => error,
            other => source_error(other),
        }
    }
}

pub type CompactV2InstructionSourceResult<T> =
    std::result::Result<T, CompactV2InstructionSourceError>;

/// A sequential `ArchiveInstructionSource` over one admitted Compact V2 reader.
///
/// `first_slot` is explicit. The Compact V2 generation manifest records
/// `epoch` and `slots_per_epoch`, but it does not record a warm-up-aware first
/// slot. The adapter never derives this value with `epoch * slots_per_epoch`.
/// When `signatures.bin` exists, the adapter reads one signature window for
/// each non-empty block when primary signatures or instruction data are
/// requested. A selected ambiguous instruction uses this read as its exact
/// signature proof.
#[derive(Debug)]
pub struct CompactV2InstructionSource<S> {
    reader: ArchiveReader<S>,
    identity: SourceIdentity,
    context: ExactContext,
    projection_scratch: TransactionProjectionScratch,
}

impl<S: RangeSource> CompactV2InstructionSource<S> {
    pub fn new(
        reader: ArchiveReader<S>,
        first_slot: u64,
    ) -> CompactV2InstructionSourceResult<Self> {
        let block_count = u32::try_from(reader.index().rows.len()).map_err(|_| {
            CompactV2InstructionSourceError::Invalid(
                "block row count exceeds the source-neutral u32 limit".into(),
            )
        })?;
        let last_slot = first_slot
            .checked_add(reader.slots_per_epoch().saturating_sub(1))
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "explicit epoch slot range overflows u64".into(),
                )
            })?;
        if reader.slots_per_epoch() == 0 {
            return Err(CompactV2InstructionSourceError::Invalid(
                "slots_per_epoch is zero".into(),
            ));
        }
        if let Some(row) = reader
            .index()
            .rows
            .iter()
            .find(|row| row.slot < first_slot || row.slot > last_slot)
        {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "block slot {} is outside explicit epoch slots {first_slot}..={last_slot}",
                row.slot
            )));
        }

        let (verification, binding) = match reader.source_kind() {
            ArchiveReaderSourceKind::PublishedManifest => {
                return Err(CompactV2InstructionSourceError::Invalid(
                    "publication-manifest readers are retired; reopen the archive as a pinned local or strong-ETag object set"
                        .into(),
                ));
            }
            ArchiveReaderSourceKind::OperatorTrusted => (
                SourceVerification::OperatorTrusted,
                Some(format!(
                    "operator-trusted-candidate-id={}",
                    reader.generation_label()
                )),
            ),
            ArchiveReaderSourceKind::ObjectSetBound => (
                SourceVerification::ObjectSetBound,
                reader
                    .archive_descriptor()
                    .and_then(|descriptor| descriptor.source_binding.object_set_id())
                    .map(str::to_owned),
            ),
        };
        let identity = SourceIdentity {
            format: ArchiveFormat::CompactV2,
            label: reader.generation_label().to_owned(),
            cluster_id: Some(reader.cluster_id().to_owned()),
            epoch: reader.epoch(),
            first_slot,
            slots_per_epoch: reader.slots_per_epoch(),
            block_count,
            verification,
            binding,
        };

        Ok(Self {
            reader,
            identity,
            context: ExactContext::default(),
            projection_scratch: TransactionProjectionScratch::default(),
        })
    }

    pub const fn reader(&self) -> &ArchiveReader<S> {
        &self.reader
    }

    pub fn into_reader(self) -> ArchiveReader<S> {
        self.reader
    }

    /// Release a complete registry image retained by an earlier dense scan.
    pub fn release_full_registry(&mut self) -> bool {
        self.context.shared_registry.take().is_some()
    }

    /// Release a complete registry image when it exceeds `max_bytes`.
    pub fn release_full_registry_above(&mut self, max_bytes: u64) -> bool {
        let must_release = self
            .context
            .shared_registry
            .as_ref()
            .is_some_and(|registry| {
                u64::try_from(registry.bytes.len()).map_or(true, |bytes| bytes > max_bytes)
            });
        must_release && self.release_full_registry()
    }

    /// Decode an ordered range with an explicit automatic registry policy.
    ///
    /// A large sequential exact-instruction scan has the same registry access
    /// risk as a parallel scan. The common trait entry point keeps the sparse
    /// policy for compatibility. Higher-level SDKs can use this method to
    /// enable the bounded dense policy.
    pub fn scan_ordered_with_registry_policy(
        &mut self,
        request: &ScanRequest,
        registry_policy: CompactV2RegistryReadPolicy,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_query_sdk::Result<ScanReceipt> {
        self.scan_inner(request, registry_policy, sink)
    }

    /// Decode and project blocks in parallel, then publish them in exact
    /// source order through the common query contract.
    ///
    /// This path supports requests that do not select instruction payload
    /// bytes. Exact instruction-data reconstruction can load large blockhash
    /// and vote-hash sidecars; the sequential scan keeps one bounded copy of
    /// that state. The USDC, Pump.fun, and FireWatch reference workloads all
    /// request `InstructionDataRequirement::None` and can use this path.
    pub fn scan_ordered_parallel(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
        config: CompactV2ParallelScanConfig,
    ) -> blockzilla_query_sdk::Result<CompactV2ParallelScanReceipt> {
        if !matches!(request.instruction_data, InstructionDataRequirement::None) {
            return Err(source_error(CompactV2InstructionSourceError::Invalid(
                "parallel Compact V2 scans require InstructionDataRequirement::None; use scan_ordered when exact instruction payload bytes are required".into(),
            )));
        }
        let parallel = compact_v2_parallel_reader_config(config).map_err(source_error)?;
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
                        .expect("OrderedBlockPublisher validated the requested u32 range"),
                )
                .expect("u32 fits the supported address space")
            });
        let decoded_bytes =
            self.reader.index().rows[start..end]
                .iter()
                .try_fold(0_u64, |decoded_bytes, row| {
                    decoded_bytes
                        .checked_add(u64::from(row.uncompressed_len))
                        .ok_or_else(|| {
                            source_error(CompactV2InstructionSourceError::Invalid(
                                "parallel decoded-byte count overflow".into(),
                            ))
                        })
                })?;
        let requested_transactions =
            requested_transaction_count(&self.reader, start..end).map_err(source_error)?;
        let query_keys = Arc::new(
            BoundQueryKeys::bind(
                self.reader.source(),
                self.reader.registry_entries(),
                request,
            )
            .map_err(|error| {
                source_error(CompactV2InstructionSourceError::Invalid(error.to_string()))
            })?,
        );
        let (shared_registry, mut registry_receipt) = prepare_parallel_registry(
            &self.reader,
            start,
            end,
            request,
            requested_transactions,
            config,
        )
        .map_err(source_error)?;
        let read_signatures = request.include_primary_signatures;
        let mut signature_scan =
            ContiguousSignatureScan::new(&self.reader, start..end, read_signatures);
        let mut context_io = ContextIo {
            calls: registry_receipt.prefetch_read_calls + query_keys.read_calls,
            bytes: registry_receipt.prefetch_read_bytes + query_keys.read_bytes,
        };
        let reader = &self.reader;
        let projected_bytes_current = AtomicU64::new(0);
        let max_projected_block_bytes = AtomicU64::new(0);
        let max_projected_batch_bytes = AtomicU64::new(0);
        let mut signature_read_wall_time = Duration::ZERO;
        let mut signature_assign_wall_time = Duration::ZERO;
        let mut publish_wall_time = Duration::ZERO;

        let pipeline = reader
            .process_borrowed_blocks_parallel_ordered(
                start..end,
                parallel,
                |_| {
                    let mut worker = ParallelProjectionWorker::new(shared_registry.clone());
                    worker.context.query_keys = Arc::clone(&query_keys);
                    Ok::<_, CompactV2ParallelScanError>(worker)
                },
                |worker, _row_number, block| {
                    if request.counts_only {
                        let source_row = block.index_row;
                        let mut counts = blockzilla_query_sdk::BlockCounts::default();
                        for (index, row) in block.tx_rows().enumerate() {
                            if row.tx_index as usize != index {
                                return Err(CompactV2InstructionSourceError::Invalid(
                                    "transaction order differs from block".into(),
                                )
                                .into());
                            }
                            crate::count_projection::count_transaction(
                                &mut counts,
                                row.flags,
                                usize::from(row.signature_count),
                                lane_region(
                                    block.message_bytes(),
                                    row.message_offset,
                                    row.message_len,
                                )?,
                                crate::count_projection::CountMetadata::Full(lane_region(
                                    block.metadata_bytes(),
                                    row.metadata_offset,
                                    row.metadata_len,
                                )?),
                                reader.message_schema(),
                                reader.metadata_schema(),
                                reader.registry_entries(),
                            )
                            .map_err(|error| {
                                CompactV2InstructionSourceError::Invalid(error.to_string())
                            })?;
                        }
                        return Ok(ParallelProjectedBlock {
                            canonical: CanonicalBlock {
                                counts: Some(counts),
                                header: BlockHeader {
                                    epoch: identity.epoch,
                                    block_ordinal: source_row.block_id,
                                    slot: source_row.slot,
                                },
                                transactions: Vec::new(),
                            },
                            signature_counts: None,
                            context_io: ContextIo::default(),
                            owned_payload_bytes: 0,
                            recycle: None,
                        });
                    }
                    let context_io_before = worker.context.io;
                    for mut block in worker
                        .recycle
                        .lock()
                        .map_err(|_| {
                            CompactV2InstructionSourceError::Invalid(
                                "projection recycle queue poisoned".into(),
                            )
                        })?
                        .drain(..)
                    {
                        worker.context.output_pool.recycle_block(&mut block);
                        if worker.transaction_buffers.len() < 8
                            && block.transactions.capacity() * size_of::<CanonicalTransaction>()
                                <= 8 << 20
                        {
                            worker.transaction_buffers.push(block.transactions);
                        }
                    }
                    let source_row = block.index_row;
                    let mut signature_counts =
                        read_signatures.then(|| Vec::with_capacity(block.tx_rows_len()));
                    let mut transactions = worker.transaction_buffers.pop().unwrap_or_default();
                    transactions.reserve(block.tx_rows_len());
                    for row in block.tx_rows() {
                        if let Some(counts) = &mut signature_counts {
                            counts.push(row.signature_count);
                        }
                        transactions.push(Self::project_transaction(
                            reader,
                            &mut worker.context,
                            &mut worker.scratch,
                            request,
                            source_row.slot,
                            row,
                            block.message_bytes(),
                            block.metadata_bytes(),
                            None,
                        )?);
                    }
                    let owned_payload_bytes = canonical_projection_owned_payload_bytes(
                        &transactions,
                        transactions.capacity(),
                        signature_counts.as_deref(),
                        signature_counts.as_ref().map_or(0, Vec::capacity),
                    )?;
                    max_projected_block_bytes.fetch_max(owned_payload_bytes, Ordering::Relaxed);
                    let current = atomic_checked_add(
                        &projected_bytes_current,
                        owned_payload_bytes,
                        "parallel projected output bytes",
                    )?;
                    max_projected_batch_bytes.fetch_max(current, Ordering::Relaxed);
                    let context_io = worker.context.io.difference(context_io_before)?;
                    Ok(ParallelProjectedBlock {
                        recycle: Some(Arc::clone(&worker.recycle)),
                        canonical: CanonicalBlock {
                            counts: None,
                            header: BlockHeader {
                                epoch: identity.epoch,
                                block_ordinal: source_row.block_id,
                                slot: source_row.slot,
                            },
                            transactions,
                        },
                        signature_counts,
                        context_io,
                        owned_payload_bytes,
                    })
                },
                |row_number, mut projected| {
                    let source_row = reader.index().rows.get(row_number).ok_or_else(|| {
                        CompactV2InstructionSourceError::Invalid(
                            "parallel ordered result is outside the archive index".into(),
                        )
                    })?;
                    let needs_signatures = projected
                        .canonical
                        .transactions
                        .iter()
                        .any(|transaction| request.needs_primary_signature(transaction));
                    let started = Instant::now();
                    let signatures =
                        signature_scan.read_block_selected(source_row, needs_signatures)?;
                    signature_read_wall_time += started.elapsed();
                    let started = Instant::now();
                    assign_primary_signatures(
                        source_row.slot,
                        &mut projected.canonical.transactions,
                        projected.signature_counts.as_deref(),
                        signatures,
                    )?;
                    signature_assign_wall_time += started.elapsed();
                    context_io.checked_add(projected.context_io)?;
                    let started = Instant::now();
                    publisher.publish(&projected.canonical)?;
                    publish_wall_time += started.elapsed();
                    let previous = projected_bytes_current
                        .fetch_sub(projected.owned_payload_bytes, Ordering::AcqRel);
                    if previous < projected.owned_payload_bytes {
                        return Err(CompactV2InstructionSourceError::Invalid(
                            "parallel projected output byte accounting underflow".into(),
                        )
                        .into());
                    }
                    if let Some(recycle) = projected.recycle {
                        recycle
                            .lock()
                            .map_err(|_| {
                                CompactV2InstructionSourceError::Invalid(
                                    "projection recycle queue poisoned".into(),
                                )
                            })?
                            .push(projected.canonical);
                    }
                    Ok(())
                },
            )
            .map_err(CompactV2ParallelScanError::into_query_error)?;

        if matches!(
            registry_receipt.mode,
            CompactV2ParallelRegistryMode::SparseWorkerCache
        ) {
            registry_receipt.resident_bound_bytes = u64::try_from(pipeline.effective_workers)
                .ok()
                .and_then(|workers| {
                    workers.checked_mul(COMPACT_V2_QUERY_REGISTRY_RETAINED_KEY_BYTES as u64)
                })
                .ok_or_else(|| {
                    source_error(CompactV2InstructionSourceError::Invalid(
                        "sparse registry resident bound overflow".into(),
                    ))
                })?;
        }

        let signature_io = signature_scan.finish().map_err(source_error)?;
        let source_read_calls = pipeline
            .read_call_count
            .checked_add(signature_io.calls)
            .and_then(|calls| calls.checked_add(context_io.calls))
            .ok_or_else(|| {
                source_error(CompactV2InstructionSourceError::Invalid(
                    "parallel scan source-read count overflow".into(),
                ))
            })?;
        let source_read_bytes = pipeline
            .compressed_bytes
            .checked_add(signature_io.bytes)
            .and_then(|bytes| bytes.checked_add(context_io.bytes))
            .ok_or_else(|| {
                source_error(CompactV2InstructionSourceError::Invalid(
                    "parallel scan source-read byte count overflow".into(),
                ))
            })?;
        publisher.set_io_receipt(ScanIoReceipt {
            source_read_calls: Some(source_read_calls),
            source_read_bytes: Some(source_read_bytes),
            decoded_bytes: Some(decoded_bytes),
            cache_read_calls: None,
            cache_read_bytes: None,
        });
        let scan = publisher.finish()?;
        Ok(CompactV2ParallelScanReceipt {
            scan,
            pipeline,
            signature_read_wall_time,
            signature_assign_wall_time,
            publish_wall_time,
            requested_workers: config.workers,
            effective_workers: pipeline.effective_workers,
            max_active_workers: pipeline.max_active_workers,
            compressed_buffer_count: parallel.compressed_buffer_count,
            max_projected_block_bytes: max_projected_block_bytes.load(Ordering::Relaxed),
            max_projected_batch_bytes: max_projected_batch_bytes.load(Ordering::Relaxed),
            registry: registry_receipt,
        })
    }

    fn scan_inner(
        &mut self,
        request: &ScanRequest,
        registry_policy: CompactV2RegistryReadPolicy,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_query_sdk::Result<ScanReceipt> {
        if request.counts_only {
            return self
                .scan_ordered_parallel(request, sink, CompactV2ParallelScanConfig::new(1))
                .map(|receipt| receipt.scan);
        }
        let identity = self.identity.clone();
        let mut publisher = OrderedBlockPublisher::new(&identity, request, sink)?;
        let start = request
            .range
            .map_or(0usize, |range| range.first_block as usize);
        let end = request
            .range
            .map_or(self.identity.block_count as usize, |range| {
                usize::try_from(
                    range
                        .first_block
                        .checked_add(range.block_count.get())
                        .expect("OrderedBlockPublisher validated the requested u32 range"),
                )
                .expect("u32 fits the supported address space")
            });
        let reader = &self.reader;
        let context = &mut self.context;
        let projection_scratch = &mut self.projection_scratch;
        let context_io_before = context.io;
        let requested_transactions =
            requested_transaction_count(reader, start..end).map_err(source_error)?;
        context
            .prepare_registry_for_scan(
                reader,
                request,
                start == 0 && end == reader.index().rows.len(),
                requested_transactions,
                registry_policy,
            )
            .map_err(source_error)?;
        context
            .prepare_query_keys(reader, request)
            .map_err(source_error)?;
        let read_signatures = request.include_primary_signatures
            || !matches!(request.instruction_data, InstructionDataRequirement::None);
        let mut signature_scan = ContiguousSignatureScan::new(reader, start..end, read_signatures);
        let mut blocks = reader
            .borrowed_blocks_without_rewards_range(Range { start, end })
            .map_err(source_error)?;

        while let Some(block) = blocks.next_block() {
            let block = block.map_err(source_error)?;
            let source_row = block.index_row;
            let signatures = signature_scan
                .read_block(&source_row)
                .map_err(source_error)?;
            let mut signature_cursor = 0usize;
            let mut transactions = Vec::with_capacity(block.tx_rows_len());

            for row in block.tx_rows() {
                let transaction_signatures = match signatures {
                    Some(signatures) => {
                        let end = signature_cursor
                            .checked_add(usize::from(row.signature_count))
                            .filter(|end| *end <= signatures.len())
                            .ok_or_else(|| {
                                source_error(CompactV2InstructionSourceError::Invalid(format!(
                                    "slot {} transaction {} signature range exceeds its block window",
                                    source_row.slot, row.tx_index
                                )))
                            })?;
                        let selected = &signatures[signature_cursor..end];
                        signature_cursor = end;
                        Some(selected)
                    }
                    None => None,
                };
                let transaction = Self::project_transaction(
                    reader,
                    context,
                    projection_scratch,
                    request,
                    source_row.slot,
                    row,
                    block.message_bytes(),
                    block.metadata_bytes(),
                    transaction_signatures,
                )
                .map_err(source_error)?;
                transactions.push(transaction);
            }
            if let Some(signatures) = &signatures
                && signature_cursor != signatures.len()
            {
                return Err(source_error(CompactV2InstructionSourceError::Invalid(
                    format!(
                        "slot {} rows consume {signature_cursor} of {} block signatures",
                        source_row.slot,
                        signatures.len()
                    ),
                )));
            }

            let mut canonical = CanonicalBlock {
                counts: None,
                header: BlockHeader {
                    epoch: identity.epoch,
                    block_ordinal: source_row.block_id,
                    slot: source_row.slot,
                },
                transactions,
            };
            publisher.publish(&canonical)?;
            context.output_pool.recycle_block(&mut canonical);
        }

        let block_io = blocks.io_stats();
        let signature_io = signature_scan.finish().map_err(source_error)?;
        let context_io = context
            .io
            .difference(context_io_before)
            .map_err(source_error)?;
        publisher.set_io_receipt(ScanIoReceipt {
            source_read_calls: Some(
                block_io
                    .source_read_calls
                    .checked_add(signature_io.calls)
                    .and_then(|calls| calls.checked_add(context_io.calls))
                    .ok_or_else(|| {
                        source_error(CompactV2InstructionSourceError::Invalid(
                            "scan source-read count overflow".into(),
                        ))
                    })?,
            ),
            source_read_bytes: Some(
                block_io
                    .source_read_bytes
                    .checked_add(signature_io.bytes)
                    .and_then(|bytes| bytes.checked_add(context_io.bytes))
                    .ok_or_else(|| {
                        source_error(CompactV2InstructionSourceError::Invalid(
                            "scan source-read byte count overflow".into(),
                        ))
                    })?,
            ),
            decoded_bytes: Some(block_io.decoded_bytes),
            cache_read_calls: None,
            cache_read_bytes: None,
        });
        publisher.finish()
    }

    #[allow(clippy::too_many_arguments)]
    fn project_transaction(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        scratch: &mut TransactionProjectionScratch,
        request: &ScanRequest,
        slot: u64,
        row: ArchiveV2HotTxRow,
        message_lane: &[u8],
        metadata_lane: &[u8],
        signatures: Option<&[[u8; 64]]>,
    ) -> CompactV2InstructionSourceResult<CanonicalTransaction> {
        let result = Self::project_transaction_inner(
            reader,
            context,
            scratch,
            request,
            slot,
            row,
            message_lane,
            metadata_lane,
            signatures,
        );
        scratch.finish_transaction();
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn project_transaction_inner(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        scratch: &mut TransactionProjectionScratch,
        request: &ScanRequest,
        slot: u64,
        row: ArchiveV2HotTxRow,
        message_lane: &[u8],
        metadata_lane: &[u8],
        signatures: Option<&[[u8; 64]]>,
    ) -> CompactV2InstructionSourceResult<CanonicalTransaction> {
        let primary_signature = if request.include_primary_signatures {
            signatures
                .and_then(|signatures| signatures.first())
                .copied()
        } else {
            None
        };
        if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0
            && row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0
        {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "slot {slot} transaction {} has METADATA_RAW_FALLBACK without HAS_METADATA",
                row.tx_index
            )));
        }
        if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            let token_balance_coverage = if request.token_balances.is_requested() {
                TokenBalanceCoverage::Unknown(CoverageReason::RawTransaction)
            } else {
                TokenBalanceCoverage::NotRequested
            };
            return Ok(CanonicalTransaction {
                header: TransactionHeader {
                    tx_index: row.tx_index,
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

        // Typed row flags provide status without projecting instructions or balances.
        // Raw/absent metadata remains unknown and follows the normal coverage path.
        if request.omit_failed_transaction_details
            && row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0
            && row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK == 0
            && row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0
        {
            return Ok(CanonicalTransaction {
                header: TransactionHeader {
                    tx_index: row.tx_index,
                    status: ExecutionStatus::Failed,
                    failed_outer_instruction_index: None,
                    instruction_coverage: InstructionCoverage::Unknown(
                        CoverageReason::ProjectionNotRequested,
                    ),
                    cpi_coverage: CpiCoverage::Unknown(CoverageReason::ProjectionNotRequested),
                },
                primary_signature: None,
                required_signers: Vec::new(),
                instructions: Vec::new(),
                token_balance_coverage: TokenBalanceCoverage::NotRequested,
                token_balances: Vec::new(),
            });
        }

        let message_bytes = lane_region(message_lane, row.message_offset, row.message_len)?;
        let projector =
            CompactV2MessageProjector::new(reader.message_schema(), reader.registry_entries());
        let message = if !request.include_instructions
            && (!request.include_execution_status || request.omit_failed_transaction_details)
            && !request.include_required_signers
            && request.required_signer.is_none()
        {
            projector.count_message(message_bytes)?
        } else {
            Self::project_requested_message(
                reader,
                context,
                scratch,
                projector,
                message_bytes,
                &request.instruction_data,
                !request.require_complete_instruction_data,
                request.include_instructions && request.include_instruction_accounts,
            )?
        };
        let message_is_v0 = matches!(
            message.version(),
            ProjectedCompactV2MessageVersion::V0 { .. }
        );
        if message_is_v0 != (row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0) {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "slot {slot} transaction {} message version differs from its row flags",
                row.tx_index
            )));
        }
        if row.signature_count != message.header().num_required_signatures {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "slot {slot} transaction {} has {} signature rows but requires {}",
                row.tx_index,
                row.signature_count,
                message.header().num_required_signatures
            )));
        }
        if let Some(signatures) = signatures
            && signatures.len() != usize::from(row.signature_count)
        {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "slot {slot} transaction {} signature window has the wrong length",
                row.tx_index
            )));
        }

        let mut exact_metadata_bytes = None;
        let metadata = if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            reject_set_flag(row, ARCHIVE_V2_TX_FLAG_HAS_ERROR, "HAS_ERROR")?;
            reject_set_flag(row, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX, "HAS_INNER_IX")?;
            reject_set_flag(
                row,
                ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                "HAS_LOADED_ADDRESSES",
            )?;
            ProjectedMetadata::Absent
        } else if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
            ProjectedMetadata::Raw
        } else {
            let bytes = lane_region(metadata_lane, row.metadata_offset, row.metadata_len)?;
            exact_metadata_bytes = Some(bytes);
            if !request.include_instructions
                && (!request.include_execution_status || request.omit_failed_transaction_details)
            {
                ProjectedMetadata::ExactUnprojected
            } else {
                let limits = CompactV2MetadataProjectionLimits::for_message(&message);
                let metadata = CompactV2MetadataProjector::new(
                    reader.metadata_schema(),
                    reader.registry_entries(),
                )
                .project(bytes, limits)?;
                require_flag_state(
                    row,
                    ARCHIVE_V2_TX_FLAG_HAS_ERROR,
                    "HAS_ERROR",
                    !metadata.execution_status.is_success(),
                )?;
                require_flag_state(
                    row,
                    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                    "HAS_INNER_IX",
                    metadata.inner_instructions.is_some(),
                )?;
                require_flag_state(
                    row,
                    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
                    "HAS_LOADED_ADDRESSES",
                    !metadata.loaded_writable_addresses.is_empty()
                        || !metadata.loaded_readonly_addresses.is_empty(),
                )?;
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
                if request.include_execution_status {
                    if row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0 {
                        ExecutionStatus::Failed
                    } else {
                        ExecutionStatus::Succeeded
                    }
                } else {
                    ExecutionStatus::Unknown(CoverageReason::ProjectionNotRequested)
                },
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

        let loaded_key_count = if !request.include_instructions {
            None
        } else {
            match &metadata {
                ProjectedMetadata::Exact(metadata) => Some(
                    metadata
                        .loaded_writable_addresses
                        .len()
                        .checked_add(metadata.loaded_readonly_addresses.len())
                        .ok_or_else(|| {
                            CompactV2InstructionSourceError::Invalid(
                                "loaded-address count overflow".into(),
                            )
                        })?,
                ),
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
            }
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
        // A signer-targeted query does not consume instructions from unrelated
        // or failed transactions. Do not project and validate those instruction
        // rows. Full instruction scans keep the strict validation path below.
        let project_instructions =
            request.include_instructions && (request.required_signer.is_none() || include_programs);
        let (instruction_coverage, instructions) = if !project_instructions {
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
                    CompactV2InstructionSourceError::Invalid(
                        "combined account-key count overflow".into(),
                    )
                })?;
            if request.include_instruction_accounts {
                scratch
                    .account_keys
                    .try_reserve(loaded_key_count)
                    .map_err(|_| {
                        CompactV2InstructionSourceError::Invalid(
                            "failed to reserve projected loaded account keys".into(),
                        )
                    })?;
                if let ProjectedMetadata::Exact(metadata) = &metadata {
                    for reference in metadata
                        .loaded_writable_addresses
                        .iter()
                        .chain(&metadata.loaded_readonly_addresses)
                    {
                        scratch
                            .account_keys
                            .push(context.resolve_pubkey(reader, *reference)?);
                    }
                }
            }
            let instructions = Self::project_instructions(
                reader,
                context,
                request,
                message_bytes,
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

        let required_signers =
            if request.include_required_signers && request.required_signer.is_some() {
                request
                    .required_signer
                    .filter(|_| signer_matches)
                    .into_iter()
                    .collect()
            } else if request.include_required_signers {
                let required = usize::from(message.header().num_required_signatures);
                let references = static_keys_prefix(&message, required)?;
                if request.include_instruction_accounts {
                    scratch
                        .account_keys
                        .get(..required)
                        .ok_or_else(|| {
                            CompactV2InstructionSourceError::Invalid(
                                "required signer range exceeds resolved static account keys".into(),
                            )
                        })?
                        .to_vec()
                } else {
                    references
                        .iter()
                        .map(|reference| context.resolve_pubkey(reader, *reference))
                        .collect::<CompactV2InstructionSourceResult<Vec<_>>>()?
                }
            } else {
                Vec::new()
            };

        let (token_balance_coverage, token_balances) = match &request.token_balances {
            TokenBalanceRequirement::None => (TokenBalanceCoverage::NotRequested, Vec::new()),
            requirement => match (&metadata, exact_metadata_bytes) {
                (
                    ProjectedMetadata::Exact(_) | ProjectedMetadata::ExactUnprojected,
                    Some(bytes),
                ) => {
                    CompactV2MetadataProjector::new(
                        reader.metadata_schema(),
                        reader.registry_entries(),
                    )
                    .project_token_balances_reusing(
                        bytes,
                        CompactV2MetadataProjectionLimits::for_message(&message),
                        &mut scratch.token_balances,
                    )?;
                    let balances = Self::resolve_token_balances(
                        reader,
                        context,
                        requirement,
                        &scratch.token_balances.pre,
                        &scratch.token_balances.post,
                    )?;
                    (TokenBalanceCoverage::Complete, balances)
                }
                (ProjectedMetadata::Absent, _) => (
                    TokenBalanceCoverage::Unknown(CoverageReason::MetadataAbsent),
                    Vec::new(),
                ),
                (ProjectedMetadata::Raw, _) => (
                    TokenBalanceCoverage::Unknown(CoverageReason::RawMetadata),
                    Vec::new(),
                ),
                (ProjectedMetadata::Exact(_) | ProjectedMetadata::ExactUnprojected, None) => {
                    unreachable!("exact metadata has bytes")
                }
            },
        };

        let transaction = CanonicalTransaction {
            header: TransactionHeader {
                tx_index: row.tx_index,
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
        };
        Ok(transaction)
    }

    fn resolve_token_balances(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        requirement: &TokenBalanceRequirement,
        pre: &[CompactTokenBalance],
        post: &[CompactTokenBalance],
    ) -> CompactV2InstructionSourceResult<Vec<RecordedTokenBalance>> {
        let mut output = context.output_pool.balances();
        if matches!(requirement, TokenBalanceRequirement::All) {
            output
                .try_reserve(pre.len().saturating_add(post.len()))
                .map_err(|_| {
                    CompactV2InstructionSourceError::Invalid(
                        "failed to reserve projected token balances".into(),
                    )
                })?;
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
                        .map(|reference| context.resolve_pubkey(reader, reference))
                        .transpose()?,
                };
                if !requirement.selects(mint.as_ref()) {
                    continue;
                }
                let owner = balance
                    .owner
                    .map(|reference| context.resolve_pubkey(reader, reference))
                    .transpose()?;
                let token_program = balance
                    .program_id
                    .map(|reference| context.resolve_pubkey(reader, reference))
                    .transpose()?;
                output.push(RecordedTokenBalance {
                    side,
                    balance_index: u32::try_from(balance_index).map_err(|_| {
                        CompactV2InstructionSourceError::Invalid(
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

    #[allow(clippy::too_many_arguments)]
    fn project_requested_message<'a>(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        scratch: &mut TransactionProjectionScratch,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        requirement: &InstructionDataRequirement,
        relaxed: bool,
        resolve_keys: bool,
    ) -> CompactV2InstructionSourceResult<ProjectedCompactV2Message<'a>> {
        scratch.account_keys.clear();
        scratch.selected_references.clear();
        match requirement {
            InstructionDataRequirement::All => {
                let message =
                    Self::project_all_with_vote_retry(reader, context, projector, bytes, relaxed)?;
                if resolve_keys {
                    Self::resolve_static_keys_into(
                        reader,
                        context,
                        &message,
                        &mut scratch.account_keys,
                    )?;
                }
                Ok(message)
            }
            InstructionDataRequirement::None => {
                let message =
                    projector.project_with_instruction_data_for_programs(bytes, &[], None)?;
                if resolve_keys {
                    Self::resolve_static_keys_into(
                        reader,
                        context,
                        &message,
                        &mut scratch.account_keys,
                    )?;
                }
                Ok(message)
            }
            InstructionDataRequirement::Programs(programs) => {
                let unselected =
                    projector.project_with_instruction_data_for_programs(bytes, &[], None)?;
                for instruction in unselected.instructions() {
                    let index = usize::from(instruction.program_id_index());
                    let reference =
                        *unselected.static_account_keys().get(index).ok_or_else(|| {
                            CompactV2InstructionSourceError::Invalid(
                                "projected program index is outside static keys".into(),
                            )
                        })?;
                    if context.query_keys.selected(reference, programs).is_some()
                        && !scratch.selected_references.contains(&reference)
                    {
                        scratch.selected_references.push(reference);
                    }
                }
                let message = Self::project_selected_with_vote_retry(
                    reader,
                    context,
                    projector,
                    bytes,
                    &scratch.selected_references,
                    relaxed,
                )?;
                if resolve_keys {
                    Self::resolve_static_keys_into(
                        reader,
                        context,
                        &message,
                        &mut scratch.account_keys,
                    )?;
                }
                Ok(message)
            }
        }
    }

    fn resolve_static_keys_into(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        message: &ProjectedCompactV2Message<'_>,
        output: &mut Vec<[u8; REGISTRY_KEY_BYTES]>,
    ) -> CompactV2InstructionSourceResult<()> {
        output.clear();
        output
            .try_reserve(message.static_account_keys().len())
            .map_err(|_| {
                CompactV2InstructionSourceError::Invalid(
                    "failed to reserve projected static account keys".into(),
                )
            })?;
        for reference in message.static_account_keys() {
            output.push(context.resolve_pubkey(reader, *reference)?);
        }
        Ok(())
    }

    fn resolve_static_keys(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        message: &ProjectedCompactV2Message<'_>,
    ) -> CompactV2InstructionSourceResult<Vec<[u8; 32]>> {
        message
            .static_account_keys()
            .iter()
            .map(|reference| context.resolve_pubkey(reader, *reference))
            .collect()
    }

    fn project_selected_with_vote_retry<'a>(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        programs: &[CompactPubkey],
        relaxed: bool,
    ) -> CompactV2InstructionSourceResult<ProjectedCompactV2Message<'a>> {
        let first = projector.project_with_instruction_data_for_programs(bytes, programs, None);
        match first {
            Err(error) if needs_vote_hashes(&error) => {
                context.load_vote_hashes(reader)?;
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
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        projector: CompactV2MessageProjector,
        bytes: &'a [u8],
        relaxed: bool,
    ) -> CompactV2InstructionSourceResult<ProjectedCompactV2Message<'a>> {
        match projector.project(bytes, context.vote_hashes()) {
            Err(error) if needs_vote_hashes(&error) => {
                context.load_vote_hashes(reader)?;
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
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        request: &ScanRequest,
        message_bytes: &[u8],
        message: &ProjectedCompactV2Message<'_>,
        metadata: &ProjectedMetadata<'_>,
        account_keys: &[[u8; 32]],
        account_key_count: usize,
        signatures: Option<&[[u8; 64]]>,
        include_programs: bool,
    ) -> CompactV2InstructionSourceResult<Vec<ResolvedInstruction>> {
        let has_selected_ambiguity = message.instructions().iter().any(|instruction| {
            instruction
                .data_candidates()
                .is_some_and(|candidates| candidates.len() > 1)
        });
        let selected_outer_data = if has_selected_ambiguity {
            match signatures {
                None if !request.require_complete_instruction_data => Some(
                    SelectedOuterData::Unknown(CoverageReason::InstructionDataUnavailable),
                ),
                None => {
                    return Err(CompactV2InstructionSourceError::MissingSidecar {
                        object: crate::manifest::SIGNATURES_FILE,
                        purpose: "selected ambiguous instruction signature sidecar proof",
                    });
                }
                Some(signatures) => {
                    match Self::select_exact_outer_data(reader, context, message_bytes, signatures)
                    {
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
        let mut next_group = inner_groups.into_iter().flatten().peekable();
        let mut output = context.output_pool.instructions();

        for (outer_index, instruction) in message.instructions().iter().enumerate() {
            if usize::from(instruction.program_id_index()) >= account_key_count {
                return Err(CompactV2InstructionSourceError::Invalid(
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
                context.project_program(reader, reference, &request.instruction_programs)?
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
                        .map_err(|_| {
                            CompactV2InstructionSourceError::Invalid(
                                "instruction allocation failed".into(),
                            )
                        })?,
                ),
                Some(_) => {
                    let selected = selected_outer_data.as_ref().ok_or_else(|| {
                        CompactV2InstructionSourceError::Invalid(
                            "ambiguous selected data was not signature-selected".into(),
                        )
                    })?;
                    match selected {
                        SelectedOuterData::Exact(selected) => {
                            let data = selected.get(outer_index).ok_or_else(|| {
                                CompactV2InstructionSourceError::Invalid(
                                    "selected signed message has the wrong instruction count"
                                        .into(),
                                )
                            })?;
                            (
                                InstructionDataCoverage::Exact,
                                context.output_pool.copy_data(data).map_err(|_| {
                                    CompactV2InstructionSourceError::Invalid(
                                        "instruction allocation failed".into(),
                                    )
                                })?,
                            )
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
                let group = next_group.next().expect("peek proved a CPI group");
                for (inner_index, inner) in group.instructions.iter().enumerate() {
                    if u64::from(inner.program_id_index) >= account_key_count as u64 {
                        return Err(CompactV2InstructionSourceError::Invalid(
                            "CPI program index exceeds account count".into(),
                        ));
                    }
                    let program_id = if !include_programs {
                        None
                    } else if request.include_instruction_accounts {
                        Some(resolve_index_u32(account_keys, inner.program_id_index)?)
                    } else {
                        let index = usize::try_from(inner.program_id_index).map_err(|_| {
                            CompactV2InstructionSourceError::Invalid(
                                "CPI account index exceeds address space".into(),
                            )
                        })?;
                        let reference = projected_account_reference(message, metadata, index)?;
                        context.project_program(reader, reference, &request.instruction_programs)?
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
                        (
                            InstructionDataCoverage::Exact,
                            context.output_pool.copy_data(inner.data).map_err(|_| {
                                CompactV2InstructionSourceError::Invalid(
                                    "instruction allocation failed".into(),
                                )
                            })?,
                        )
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
            return Err(CompactV2InstructionSourceError::Invalid(
                "metadata CPI group has no matching outer instruction".into(),
            ));
        }
        Ok(output)
    }

    fn select_exact_outer_data(
        reader: &ArchiveReader<S>,
        context: &mut ExactContext,
        message_bytes: &[u8],
        signatures: &[[u8; 64]],
    ) -> CompactV2InstructionSourceResult<Vec<Vec<u8>>> {
        let projector =
            CompactV2MessageProjector::new(reader.message_schema(), reader.registry_entries());
        let message =
            Self::project_all_with_vote_retry(reader, context, projector, message_bytes, false)?;
        let static_keys = Self::resolve_static_keys(reader, context, &message)?;
        let recent_blockhash = match message.recent_blockhash() {
            OwnedCompactRecentBlockhash::Nonce(hash) => *hash,
            OwnedCompactRecentBlockhash::Id(id)
                if *id < 0
                    && reader
                        .file_size(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
                        .is_none() =>
            {
                return Err(CompactV2InstructionSourceError::MissingSidecar {
                    object: ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
                    purpose: "ambiguous signed-message previous blockhash",
                });
            }
            OwnedCompactRecentBlockhash::Id(id) => {
                context.load_blockhashes(reader)?.resolve(*id)?
            }
        };
        let resolved_lookups = match message.version() {
            ProjectedCompactV2MessageVersion::V0 {
                address_table_lookups,
            } => address_table_lookups
                .iter()
                .map(|lookup| {
                    context
                        .resolve_pubkey(reader, lookup.account_key())
                        .map(|account_key| crate::ResolvedAddressTableLookup {
                            account_key,
                            writable_indexes: lookup.writable_indexes(),
                            readonly_indexes: lookup.readonly_indexes(),
                        })
                })
                .collect::<CompactV2InstructionSourceResult<Vec<_>>>()?,
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
        let candidates = message
            .instructions()
            .iter()
            .enumerate()
            .map(|(index, instruction)| {
                let data_candidates = instruction.data_candidates().ok_or_else(|| {
                    CompactV2InstructionSourceError::Invalid(format!(
                        "full signature projection omitted instruction {index} data"
                    ))
                })?;
                Ok(SignedInstructionCandidates {
                    program_id_index: instruction.program_id_index(),
                    accounts: instruction.accounts(),
                    data_candidates,
                })
            })
            .collect::<CompactV2InstructionSourceResult<Vec<_>>>()?;
        let selected = crate::select_signed_message_candidate_ed25519(
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

impl<S: RangeSource> ArchiveInstructionSource for CompactV2InstructionSource<S> {
    fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_query_sdk::Result<ScanReceipt> {
        self.scan_inner(request, CompactV2RegistryReadPolicy::sparse_only(), sink)
    }
}

struct ParallelProjectedBlock {
    recycle: Option<Arc<Mutex<Vec<CanonicalBlock>>>>,
    canonical: CanonicalBlock,
    signature_counts: Option<Vec<u8>>,
    context_io: ContextIo,
    owned_payload_bytes: u64,
}

struct ParallelProjectionWorker {
    recycle: Arc<Mutex<Vec<CanonicalBlock>>>,
    transaction_buffers: Vec<Vec<CanonicalTransaction>>,
    context: ExactContext,
    scratch: TransactionProjectionScratch,
}

impl ParallelProjectionWorker {
    fn new(shared_registry: Option<Arc<SharedRegistry>>) -> Self {
        Self {
            recycle: Arc::new(Mutex::new(Vec::new())),
            transaction_buffers: Vec::new(),
            context: ExactContext::with_shared_registry(shared_registry),
            scratch: TransactionProjectionScratch::default(),
        }
    }
}

#[derive(Debug, Default)]
struct TransactionProjectionScratch {
    account_keys: Vec<[u8; REGISTRY_KEY_BYTES]>,
    selected_references: Vec<CompactPubkey>,
    // Each list is bounded by the 256-account metadata limit.
    token_balances: crate::ProjectedCompactV2TokenBalances,
}

impl TransactionProjectionScratch {
    fn finish_transaction(&mut self) {
        retain_or_release_scratch(
            &mut self.account_keys,
            COMPACT_V2_PROJECTION_SCRATCH_RETAINED_BYTES,
        );
        retain_or_release_scratch(
            &mut self.selected_references,
            COMPACT_V2_PROJECTION_SCRATCH_RETAINED_BYTES,
        );
    }
}

fn retain_or_release_scratch<T>(values: &mut Vec<T>, byte_limit: usize) {
    let retained_bytes = values.capacity().saturating_mul(size_of::<T>());
    if retained_bytes > byte_limit {
        *values = Vec::new();
    } else {
        values.clear();
    }
}

fn canonical_projection_owned_payload_bytes(
    transactions: &[CanonicalTransaction],
    transaction_capacity: usize,
    signature_counts: Option<&[u8]>,
    signature_count_capacity: usize,
) -> CompactV2InstructionSourceResult<u64> {
    let mut bytes = capacity_bytes::<CanonicalTransaction>(transaction_capacity)?;
    if signature_counts.is_some() {
        checked_add_payload(&mut bytes, capacity_bytes::<u8>(signature_count_capacity)?)?;
    }
    for transaction in transactions {
        checked_add_payload(
            &mut bytes,
            capacity_bytes::<[u8; REGISTRY_KEY_BYTES]>(transaction.required_signers.capacity())?,
        )?;
        checked_add_payload(
            &mut bytes,
            capacity_bytes::<ResolvedInstruction>(transaction.instructions.capacity())?,
        )?;
        for instruction in &transaction.instructions {
            checked_add_payload(
                &mut bytes,
                capacity_bytes::<[u8; REGISTRY_KEY_BYTES]>(instruction.accounts.capacity())?,
            )?;
            checked_add_payload(
                &mut bytes,
                capacity_bytes::<u8>(instruction.data.capacity())?,
            )?;
        }
        checked_add_payload(
            &mut bytes,
            capacity_bytes::<RecordedTokenBalance>(transaction.token_balances.capacity())?,
        )?;
    }
    Ok(bytes)
}

fn capacity_bytes<T>(capacity: usize) -> CompactV2InstructionSourceResult<u64> {
    let bytes = capacity.checked_mul(size_of::<T>()).ok_or_else(|| {
        CompactV2InstructionSourceError::Invalid("projected output capacity overflow".into())
    })?;
    u64::try_from(bytes).map_err(|_| {
        CompactV2InstructionSourceError::Invalid("projected output capacity exceeds u64".into())
    })
}

fn checked_add_payload(total: &mut u64, value: u64) -> CompactV2InstructionSourceResult<()> {
    *total = total.checked_add(value).ok_or_else(|| {
        CompactV2InstructionSourceError::Invalid("projected output byte count overflow".into())
    })?;
    Ok(())
}

fn atomic_checked_add(
    value: &AtomicU64,
    amount: u64,
    label: &'static str,
) -> CompactV2InstructionSourceResult<u64> {
    value
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(amount)
        })
        .map(|previous| previous + amount)
        .map_err(|_| CompactV2InstructionSourceError::Invalid(format!("{label} overflow")))
}

#[derive(Debug)]
struct SharedRegistry {
    bytes: Vec<u8>,
}

impl SharedRegistry {
    fn resolve(&self, id: u32) -> Option<[u8; REGISTRY_KEY_BYTES]> {
        let start = usize::try_from(id.checked_sub(1)?)
            .ok()?
            .checked_mul(REGISTRY_KEY_BYTES)?;
        let end = start.checked_add(REGISTRY_KEY_BYTES)?;
        let bytes = self.bytes.get(start..end)?;
        let mut key = [0_u8; REGISTRY_KEY_BYTES];
        key.copy_from_slice(bytes);
        Some(key)
    }
}

fn requested_transaction_count<S: RangeSource>(
    reader: &ArchiveReader<S>,
    range: Range<usize>,
) -> CompactV2InstructionSourceResult<u64> {
    reader.index().rows[range]
        .iter()
        .try_fold(0_u64, |transactions, row| {
            transactions
                .checked_add(u64::from(row.tx_count))
                .ok_or_else(|| {
                    CompactV2InstructionSourceError::Invalid(
                        "requested transaction count overflow".into(),
                    )
                })
        })
}

fn prepare_parallel_registry<S: RangeSource>(
    reader: &ArchiveReader<S>,
    start: usize,
    end: usize,
    request: &ScanRequest,
    requested_transactions: u64,
    config: CompactV2ParallelScanConfig,
) -> CompactV2InstructionSourceResult<(
    Option<Arc<SharedRegistry>>,
    CompactV2ParallelRegistryReceipt,
)> {
    let sparse = || CompactV2ParallelRegistryReceipt {
        mode: CompactV2ParallelRegistryMode::SparseWorkerCache,
        prefetch_read_calls: 0,
        prefetch_read_bytes: 0,
        resident_bound_bytes: 0,
    };
    let full_scan = start == 0 && end == reader.index().rows.len();
    let registry_bytes = u64::from(reader.registry_entries())
        .checked_mul(REGISTRY_KEY_BYTES as u64)
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("registry byte size overflow".into())
        })?;
    if !should_prefetch_parallel_registry(
        config,
        full_scan,
        requested_transactions,
        request_needs_registry(request),
        registry_bytes,
    ) {
        return Ok((None, sparse()));
    }
    let Some((shared, io)) = prefetch_shared_registry(reader, registry_bytes)? else {
        return Ok((None, sparse()));
    };
    Ok((
        Some(shared),
        CompactV2ParallelRegistryReceipt {
            mode: CompactV2ParallelRegistryMode::SharedFull,
            prefetch_read_calls: io.calls,
            prefetch_read_bytes: io.bytes,
            resident_bound_bytes: registry_bytes,
        },
    ))
}

fn prefetch_shared_registry<S: RangeSource>(
    reader: &ArchiveReader<S>,
    registry_bytes: u64,
) -> CompactV2InstructionSourceResult<Option<(Arc<SharedRegistry>, ContextIo)>> {
    let Ok(registry_len) = usize::try_from(registry_bytes) else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    if bytes.try_reserve_exact(registry_len).is_err() {
        return Ok(None);
    }
    bytes.resize(registry_len, 0);
    let mut io = ContextIo::default();
    let mut offset = 0_u64;
    for chunk in bytes.chunks_mut(COMPACT_V2_REGISTRY_PREFETCH_READ_BYTES) {
        reader
            .source()
            .read_range_into_slice(crate::manifest::REGISTRY_FILE, offset, chunk)?;
        io.record(chunk.len())?;
        offset = offset
            .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                CompactV2InstructionSourceError::Invalid(
                    "registry prefetch chunk length exceeds u64".into(),
                )
            })?)
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("registry prefetch offset overflow".into())
            })?;
    }
    if offset != registry_bytes {
        return Err(CompactV2InstructionSourceError::Invalid(format!(
            "registry prefetch read {offset} bytes, expected {registry_bytes}"
        )));
    }
    Ok(Some((Arc::new(SharedRegistry { bytes }), io)))
}

fn request_needs_registry(request: &ScanRequest) -> bool {
    (request.include_instructions && request.include_instruction_accounts)
        || (request.include_instructions && matches!(request.instruction_programs, InstructionDataRequirement::All) && request.required_signer.is_none())
        || (request.include_required_signers && request.required_signer.is_none())
        // Balance output includes owner/program keys. Keep the shared dense
        // read policy for these output fields; mint filtering itself uses IDs.
        || request.token_balances.is_requested()
}

fn should_prefetch_parallel_registry(
    config: CompactV2ParallelScanConfig,
    full_scan: bool,
    requested_transactions: u64,
    request_needs_registry: bool,
    registry_bytes: u64,
) -> bool {
    should_prefetch_registry(
        config.max_full_registry_bytes,
        COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
        full_scan,
        requested_transactions,
        request_needs_registry,
        registry_bytes,
    )
}

fn should_prefetch_sequential_registry(
    policy: CompactV2RegistryReadPolicy,
    full_scan: bool,
    requested_transactions: u64,
    request_needs_registry: bool,
    registry_bytes: u64,
) -> bool {
    should_prefetch_registry(
        policy.max_full_registry_bytes,
        policy.min_requested_transactions,
        full_scan,
        requested_transactions,
        request_needs_registry,
        registry_bytes,
    )
}

fn should_prefetch_registry(
    max_full_registry_bytes: u64,
    min_requested_transactions: u64,
    full_scan: bool,
    requested_transactions: u64,
    request_needs_registry: bool,
    registry_bytes: u64,
) -> bool {
    request_needs_registry
        && max_full_registry_bytes != 0
        && registry_bytes <= max_full_registry_bytes
        && (full_scan || requested_transactions >= min_requested_transactions)
}

fn compact_v2_parallel_reader_config(
    config: CompactV2ParallelScanConfig,
) -> CompactV2InstructionSourceResult<OrderedParallelBlockConfig> {
    if config.workers == 0 || config.workers > MAX_ORDERED_PARALLEL_DECODE_WORKERS {
        return Err(CompactV2InstructionSourceError::Invalid(format!(
            "parallel Compact V2 workers must be in 1..={MAX_ORDERED_PARALLEL_DECODE_WORKERS}"
        )));
    }
    Ok(OrderedParallelBlockConfig {
        decode_workers: config.workers,
        compressed_buffer_count: config
            .workers
            .clamp(1, COMPACT_V2_PARALLEL_COMPRESSED_BUFFERS),
        max_blocks_per_batch: COMPACT_V2_PARALLEL_MAX_BLOCKS_PER_BATCH,
        uncompressed_batch_budget_bytes: COMPACT_V2_PARALLEL_UNCOMPRESSED_BATCH_BYTES,
        retained_decompressed_bytes_per_worker: (32 * 1024 * 1024)
            .min(MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES / config.workers),
        discard_rewards: true,
        ..OrderedParallelBlockConfig::default()
    })
}

fn assign_primary_signatures(
    slot: u64,
    transactions: &mut [CanonicalTransaction],
    signature_counts: Option<&[u8]>,
    signatures: Option<&[[u8; SIGNATURE_BYTES]]>,
) -> CompactV2InstructionSourceResult<()> {
    let Some(signatures) = signatures else {
        return Ok(());
    };
    let counts = signature_counts.ok_or_else(|| {
        CompactV2InstructionSourceError::Invalid(format!(
            "slot {slot} loaded signatures without projected signature counts"
        ))
    })?;
    if counts.len() != transactions.len() {
        return Err(CompactV2InstructionSourceError::Invalid(format!(
            "slot {slot} has {} projected transactions and {} signature counts",
            transactions.len(),
            counts.len()
        )));
    }
    let mut cursor = 0usize;
    for (transaction, count) in transactions.iter_mut().zip(counts) {
        let end = cursor
            .checked_add(usize::from(*count))
            .filter(|end| *end <= signatures.len())
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(format!(
                    "slot {slot} transaction {} signature range exceeds its block window",
                    transaction.header.tx_index
                ))
            })?;
        transaction.primary_signature = (*count != 0)
            .then(|| signatures.get(cursor).copied())
            .flatten();
        cursor = end;
    }
    if cursor != signatures.len() {
        return Err(CompactV2InstructionSourceError::Invalid(format!(
            "slot {slot} projected rows consume {cursor} of {} block signatures",
            signatures.len()
        )));
    }
    Ok(())
}

fn source_error(error: impl std::error::Error + Send + Sync + 'static) -> QueryError {
    QueryError::source(ArchiveFormat::CompactV2, error)
}

fn lane_region(bytes: &[u8], offset: u32, length: u32) -> CompactV2InstructionSourceResult<&[u8]> {
    let start = usize::try_from(offset).map_err(|_| {
        CompactV2InstructionSourceError::Invalid("lane offset exceeds address space".into())
    })?;
    let end = start
        .checked_add(usize::try_from(length).map_err(|_| {
            CompactV2InstructionSourceError::Invalid("lane length exceeds address space".into())
        })?)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("lane range is outside its block lane".into())
        })?;
    Ok(&bytes[start..end])
}

fn reject_set_flag(
    row: ArchiveV2HotTxRow,
    flag: u32,
    name: &str,
) -> CompactV2InstructionSourceResult<()> {
    require_flag_state(row, flag, name, false)
}

fn require_flag_state(
    row: ArchiveV2HotTxRow,
    flag: u32,
    name: &str,
    expected: bool,
) -> CompactV2InstructionSourceResult<()> {
    let actual = row.flags & flag != 0;
    if actual != expected {
        return Err(CompactV2InstructionSourceError::Invalid(format!(
            "transaction {} {name} flag is {actual}, expected {expected}",
            row.tx_index
        )));
    }
    Ok(())
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

fn is_missing_instruction_proof(error: &CompactV2InstructionSourceError) -> bool {
    matches!(
        error,
        CompactV2InstructionSourceError::MissingSidecar { .. }
            | CompactV2InstructionSourceError::Message(
                CompactV2MessageProjectionError::ExactInstructionData(
                    SignedMessageError::MissingVoteHashResolver { .. }
                )
            )
    )
}

fn is_unresolved_instruction_ambiguity(error: &CompactV2InstructionSourceError) -> bool {
    matches!(
        error,
        CompactV2InstructionSourceError::Message(
            CompactV2MessageProjectionError::CandidateCombinationLimit
                | CompactV2MessageProjectionError::ExactInstructionData(
                    SignedMessageError::AmbiguousInstructionEncoding { .. }
                )
        ) | CompactV2InstructionSourceError::SignedMessage(
            SignedMessageError::AmbiguousInstructionEncoding { .. }
                | SignedMessageError::CandidateCombinationLimitExceeded { .. }
                | SignedMessageError::MultipleVerifiedMessageCandidates
        )
    )
}

fn static_keys_prefix<'a>(
    message: &'a ProjectedCompactV2Message<'_>,
    required: usize,
) -> CompactV2InstructionSourceResult<&'a [CompactPubkey]> {
    message
        .static_account_keys()
        .get(..required)
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid(
                "required signer prefix exceeds projected static keys".into(),
            )
        })
}

fn resolve_index(
    account_keys: &[[u8; 32]],
    index: u8,
) -> CompactV2InstructionSourceResult<[u8; 32]> {
    account_keys
        .get(usize::from(index))
        .copied()
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid(format!(
                "message account index {index} is outside resolved keys"
            ))
        })
}

fn resolve_index_u32(
    account_keys: &[[u8; 32]],
    index: u32,
) -> CompactV2InstructionSourceResult<[u8; 32]> {
    let index = usize::try_from(index).map_err(|_| {
        CompactV2InstructionSourceError::Invalid("CPI account index exceeds address space".into())
    })?;
    account_keys.get(index).copied().ok_or_else(|| {
        CompactV2InstructionSourceError::Invalid(format!(
            "CPI account index {index} is outside resolved keys"
        ))
    })
}

fn projected_account_reference(
    message: &ProjectedCompactV2Message<'_>,
    metadata: &ProjectedMetadata<'_>,
    index: usize,
) -> CompactV2InstructionSourceResult<CompactPubkey> {
    if let Some(reference) = message.static_account_keys().get(index) {
        return Ok(*reference);
    }
    let loaded_index = index
        .checked_sub(message.static_account_keys().len())
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid(format!(
                "message account index {index} is outside projected keys"
            ))
        })?;
    let ProjectedMetadata::Exact(metadata) = metadata else {
        return Err(CompactV2InstructionSourceError::Invalid(format!(
            "message account index {index} requires unavailable loaded keys"
        )));
    };
    metadata
        .loaded_writable_addresses
        .iter()
        .chain(&metadata.loaded_readonly_addresses)
        .nth(loaded_index)
        .copied()
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid(format!(
                "message account index {index} is outside projected keys"
            ))
        })
}

fn project_instruction_accounts(
    pool: &mut blockzilla_query_sdk::projection_pool::ProjectionPool,
    include_accounts: bool,
    account_keys: &[[u8; 32]],
    account_key_count: usize,
    indexes: &[u8],
) -> CompactV2InstructionSourceResult<Vec<[u8; 32]>> {
    if include_accounts {
        let mut output = pool.accounts();
        output.try_reserve(indexes.len()).map_err(|_| {
            CompactV2InstructionSourceError::Invalid("account allocation failed".into())
        })?;
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
        return Err(CompactV2InstructionSourceError::Invalid(format!(
            "message account index {index} is outside projected keys"
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
) -> CompactV2InstructionSourceResult<()> {
    let order = u32::try_from(output.len()).map_err(|_| {
        CompactV2InstructionSourceError::Invalid("instruction order exceeds u32".into())
    })?;
    let outer_index = u32::try_from(outer_index).map_err(|_| {
        CompactV2InstructionSourceError::Invalid("outer instruction index exceeds u32".into())
    })?;
    let inner_index = inner_index
        .map(|index| {
            u32::try_from(index).map_err(|_| {
                CompactV2InstructionSourceError::Invalid(
                    "inner instruction index exceeds u32".into(),
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

enum ProjectedMetadata<'a> {
    Absent,
    Raw,
    Exact(crate::ProjectedCompactV2Metadata<'a>),
    ExactUnprojected,
}

enum SelectedOuterData {
    Exact(Vec<Vec<u8>>),
    Unknown(CoverageReason),
}

#[derive(Debug, Default, Clone, Copy)]
struct ContextIo {
    calls: u64,
    bytes: u64,
}

impl ContextIo {
    fn record(&mut self, bytes: usize) -> CompactV2InstructionSourceResult<()> {
        self.calls = self.calls.checked_add(1).ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("context read count overflow".into())
        })?;
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(bytes).map_err(|_| {
                CompactV2InstructionSourceError::Invalid(
                    "context read byte count exceeds u64".into(),
                )
            })?)
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("context read bytes overflow".into())
            })?;
        Ok(())
    }

    fn difference(self, before: Self) -> CompactV2InstructionSourceResult<Self> {
        Ok(Self {
            calls: self.calls.checked_sub(before.calls).ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "context read count moved backwards".into(),
                )
            })?,
            bytes: self.bytes.checked_sub(before.bytes).ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "context read bytes moved backwards".into(),
                )
            })?,
        })
    }

    fn checked_add(&mut self, other: Self) -> CompactV2InstructionSourceResult<()> {
        self.calls = self.calls.checked_add(other.calls).ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("context read count overflow".into())
        })?;
        self.bytes = self.bytes.checked_add(other.bytes).ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("context read bytes overflow".into())
        })?;
        Ok(())
    }
}

/// One bounded, zero-gap signature window for an ordered block scan.
///
/// The public signature plane is contiguous in block order. Reading one
/// window avoids one small HTTP request for every non-empty block while the
/// returned per-block slices remain exact.
struct ContiguousSignatureScan<'a, S> {
    reader: &'a ArchiveReader<S>,
    requested_range: Range<usize>,
    next_block: usize,
    read_signatures: bool,
    batch: Option<SignatureBatch>,
    io: ContextIo,
}

struct SignatureBatch {
    block_range: Range<usize>,
    first_signature_ordinal: u64,
    signatures: Vec<[u8; SIGNATURE_BYTES]>,
}

impl<S: RangeSource> ContiguousSignatureScan<'_, S> {
    fn new(
        reader: &ArchiveReader<S>,
        requested_range: Range<usize>,
        read_signatures: bool,
    ) -> ContiguousSignatureScan<'_, S> {
        let next_block = requested_range.start;
        ContiguousSignatureScan {
            reader,
            requested_range,
            next_block,
            read_signatures,
            batch: None,
            io: ContextIo::default(),
        }
    }

    fn read_block(
        &mut self,
        row: &blockzilla_format::ArchiveV2HotBlockIndexRow,
    ) -> CompactV2InstructionSourceResult<Option<&[[u8; SIGNATURE_BYTES]]>> {
        self.read_block_selected(row, true)
    }

    fn read_block_selected(
        &mut self,
        row: &blockzilla_format::ArchiveV2HotBlockIndexRow,
        selected: bool,
    ) -> CompactV2InstructionSourceResult<Option<&[[u8; SIGNATURE_BYTES]]>> {
        if self.next_block >= self.requested_range.end {
            return Err(CompactV2InstructionSourceError::Invalid(
                "signature scan received too many blocks".into(),
            ));
        }
        let expected = self
            .reader
            .index()
            .rows
            .get(self.next_block)
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "signature scan block is outside the archive index".into(),
                )
            })?;
        if expected.block_id != row.block_id
            || expected.tx_count != row.tx_count
            || expected.first_signature_ordinal != row.first_signature_ordinal
            || expected.signature_count != row.signature_count
        {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "signature scan block {} differs from index row {}",
                row.block_id, self.next_block
            )));
        }
        if !self.read_signatures || !selected {
            self.next_block += 1;
            return Ok(None);
        }
        if !self.reader.signatures_available() {
            self.next_block += 1;
            return Ok(None);
        }

        if self
            .batch
            .as_ref()
            .is_none_or(|batch| !batch.block_range.contains(&self.next_block))
        {
            self.batch = Some(load_signature_batch(
                self.reader,
                self.next_block,
                self.requested_range.end,
                &mut self.io,
                self.batch
                    .take()
                    .map_or_else(Vec::new, |batch| batch.signatures),
            )?);
        }
        let batch = self.batch.as_ref().ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("signature batch is missing".into())
        })?;
        let row_end = row
            .first_signature_ordinal
            .checked_add(u64::from(row.signature_count))
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "block signature ordinal end overflow".into(),
                )
            })?;
        let start = row
            .first_signature_ordinal
            .checked_sub(batch.first_signature_ordinal)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "block signature range starts before its batch".into(),
                )
            })?;
        let end = row_end
            .checked_sub(batch.first_signature_ordinal)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "block signature range ends before its batch".into(),
                )
            })?;
        let selected = batch.signatures.get(start..end).ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid(
                "block signature range exceeds its loaded batch".into(),
            )
        })?;
        self.next_block += 1;
        Ok(Some(selected))
    }

    fn finish(self) -> CompactV2InstructionSourceResult<ContextIo> {
        if self.next_block != self.requested_range.end {
            return Err(CompactV2InstructionSourceError::Invalid(
                "signature scan ended before all requested blocks".into(),
            ));
        }
        Ok(self.io)
    }
}

fn load_signature_batch<S: RangeSource>(
    reader: &ArchiveReader<S>,
    start: usize,
    requested_end: usize,
    io: &mut ContextIo,
    mut signatures: Vec<[u8; SIGNATURE_BYTES]>,
) -> CompactV2InstructionSourceResult<SignatureBatch> {
    let rows = &reader.index().rows;
    let first = rows.get(start).ok_or_else(|| {
        CompactV2InstructionSourceError::Invalid(
            "signature batch starts outside the archive index".into(),
        )
    })?;
    let first_signature_ordinal = first.first_signature_ordinal;
    let mut expected_ordinal = first_signature_ordinal;
    let mut block_end = start;
    while block_end < requested_end {
        let row = rows.get(block_end).ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid(
                "signature batch block is outside the archive index".into(),
            )
        })?;
        if row.first_signature_ordinal != expected_ordinal {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "block {} signature ordinals are not contiguous",
                row.block_id
            )));
        }
        validate_signature_row(row)?;
        let next_ordinal = expected_ordinal
            .checked_add(u64::from(row.signature_count))
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "signature batch ordinal end overflow".into(),
                )
            })?;
        let candidate_bytes = next_ordinal
            .checked_sub(first_signature_ordinal)
            .and_then(|count| count.checked_mul(SIGNATURE_BYTES as u64))
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "signature batch byte length overflow".into(),
                )
            })?;
        if block_end > start && candidate_bytes > MAX_SIGNATURE_BATCH_BYTES {
            break;
        }
        expected_ordinal = next_ordinal;
        block_end += 1;
        if candidate_bytes > MAX_SIGNATURE_BATCH_BYTES {
            // A valid large block is one isolated, bounded batch.
            break;
        }
    }
    if block_end == start {
        return Err(CompactV2InstructionSourceError::Invalid(
            "signature batch planner made no progress".into(),
        ));
    }

    let signature_count = expected_ordinal
        .checked_sub(first_signature_ordinal)
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("signature batch record count overflow".into())
        })?;
    signatures
        .try_reserve_exact(signature_count.saturating_sub(signatures.len()))
        .map_err(|_| {
            CompactV2InstructionSourceError::Invalid(
                "cannot reserve the bounded signature batch".into(),
            )
        })?;
    signatures.resize(signature_count, [0; SIGNATURE_BYTES]);
    let total_bytes = signature_count
        .checked_mul(SIGNATURE_BYTES)
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("signature batch byte length overflow".into())
        })?;
    let mut read_bytes = 0usize;
    while read_bytes < total_bytes {
        let length = (total_bytes - read_bytes).min(MAX_SIGNATURE_BATCH_BYTES);
        let offset = first_signature_ordinal
            .checked_mul(SIGNATURE_BYTES as u64)
            .and_then(|offset| offset.checked_add(read_bytes as u64))
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(
                    "signature batch read offset overflow".into(),
                )
            })?;
        reader.source().read_range_into_slice(
            crate::manifest::SIGNATURES_FILE,
            offset,
            &mut signatures.as_flattened_mut()[read_bytes..read_bytes + length],
        )?;
        io.record(length)?;
        read_bytes += length;
    }
    if signatures.len() != signature_count {
        return Err(CompactV2InstructionSourceError::Invalid(
            "signature batch record count differs from its block rows".into(),
        ));
    }
    Ok(SignatureBatch {
        block_range: start..block_end,
        first_signature_ordinal,
        signatures,
    })
}

fn validate_signature_row(
    row: &blockzilla_format::ArchiveV2HotBlockIndexRow,
) -> CompactV2InstructionSourceResult<()> {
    let length = usize::try_from(row.signature_count)
        .ok()
        .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
        .ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("block signature byte length overflow".into())
        })?;
    let row_bound = usize::try_from(row.tx_count)
        .ok()
        .and_then(|count| count.checked_mul(usize::from(u8::MAX)))
        .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
        .unwrap_or(usize::MAX)
        .min(MAX_SIGNATURE_BYTES_PER_BLOCK);
    if length > row_bound {
        return Err(CompactV2InstructionSourceError::Invalid(format!(
            "block {} signature window is {length} bytes, above {row_bound}",
            row.block_id
        )));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ExactContext {
    output_pool: blockzilla_query_sdk::projection_pool::ProjectionPool,
    query_keys: Arc<BoundQueryKeys>,
    shared_registry: Option<Arc<SharedRegistry>>,
    registry_chunks: HashMap<u32, Vec<[u8; 32]>>,
    registry_lru: VecDeque<u32>,
    vote_hashes_loaded: bool,
    vote_hashes: Option<VoteHashRegistry>,
    blockhashes: Option<BlockhashResolver>,
    io: ContextIo,
}

impl ExactContext {
    fn with_shared_registry(shared_registry: Option<Arc<SharedRegistry>>) -> Self {
        Self {
            shared_registry,
            ..Self::default()
        }
    }

    fn prepare_registry_for_scan<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
        request: &ScanRequest,
        full_scan: bool,
        requested_transactions: u64,
        policy: CompactV2RegistryReadPolicy,
    ) -> CompactV2InstructionSourceResult<()> {
        if self.shared_registry.is_some() {
            return Ok(());
        }
        let registry_bytes = u64::from(reader.registry_entries())
            .checked_mul(REGISTRY_KEY_BYTES as u64)
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("registry byte size overflow".into())
            })?;
        if !should_prefetch_sequential_registry(
            policy,
            full_scan,
            requested_transactions,
            request_needs_registry(request),
            registry_bytes,
        ) {
            return Ok(());
        }
        let Some((registry, prefetch_io)) = prefetch_shared_registry(reader, registry_bytes)?
        else {
            return Ok(());
        };
        self.io.checked_add(prefetch_io)?;
        self.registry_chunks.clear();
        self.registry_lru.clear();
        self.shared_registry = Some(registry);
        Ok(())
    }

    fn prepare_query_keys<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
        request: &ScanRequest,
    ) -> CompactV2InstructionSourceResult<()> {
        if !self.query_keys.covers(request) {
            let keys = BoundQueryKeys::bind_with_registry(
                reader.source(),
                reader.registry_entries(),
                request,
                self.shared_registry
                    .as_ref()
                    .map(|registry| registry.bytes.as_slice()),
            )
            .map_err(|error| CompactV2InstructionSourceError::Invalid(error.to_string()))?;
            self.io.checked_add(ContextIo {
                calls: keys.read_calls,
                bytes: keys.read_bytes,
            })?;
            self.query_keys = Arc::new(keys);
        }
        Ok(())
    }

    fn project_program<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
        reference: CompactPubkey,
        requirement: &InstructionDataRequirement,
    ) -> CompactV2InstructionSourceResult<Option<[u8; 32]>> {
        match requirement {
            InstructionDataRequirement::None => Ok(None),
            InstructionDataRequirement::Programs(keys) => {
                Ok(self.query_keys.selected(reference, keys))
            }
            InstructionDataRequirement::All => self.resolve_pubkey(reader, reference).map(Some),
        }
    }

    fn resolve_pubkey<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
        reference: CompactPubkey,
    ) -> CompactV2InstructionSourceResult<[u8; 32]> {
        let CompactPubkey::Id(id) = reference else {
            let CompactPubkey::Raw(pubkey) = reference else {
                unreachable!("CompactPubkey has only raw and ID forms")
            };
            return Ok(pubkey);
        };
        if id == 0 || id > reader.registry_entries() {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "registry ID {id} is outside 1..={}",
                reader.registry_entries()
            )));
        }
        if let Some(registry) = &self.shared_registry {
            return registry.resolve(id).ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(format!(
                    "registry ID {id} is outside the shared complete registry"
                ))
            });
        }
        let zero_based = usize::try_from(id - 1).map_err(|_| {
            CompactV2InstructionSourceError::Invalid("registry ID exceeds address space".into())
        })?;
        let chunk_id = u32::try_from(zero_based / REGISTRY_KEYS_PER_CHUNK).map_err(|_| {
            CompactV2InstructionSourceError::Invalid("registry chunk ID exceeds u32".into())
        })?;
        self.ensure_registry_chunk(reader, chunk_id)?;
        self.touch_registry_chunk(chunk_id);
        let index = zero_based % REGISTRY_KEYS_PER_CHUNK;
        self.registry_chunks
            .get(&chunk_id)
            .and_then(|chunk| chunk.get(index))
            .copied()
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid(format!(
                    "registry ID {id} is outside its loaded chunk"
                ))
            })
    }

    fn ensure_registry_chunk<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
        chunk_id: u32,
    ) -> CompactV2InstructionSourceResult<()> {
        if self.registry_chunks.contains_key(&chunk_id) {
            return Ok(());
        }
        let first_key = usize::try_from(chunk_id)
            .ok()
            .and_then(|chunk| chunk.checked_mul(REGISTRY_KEYS_PER_CHUNK))
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("registry chunk offset overflow".into())
            })?;
        let entries = usize::try_from(reader.registry_entries()).map_err(|_| {
            CompactV2InstructionSourceError::Invalid(
                "registry entry count exceeds address space".into(),
            )
        })?;
        let key_count = entries
            .saturating_sub(first_key)
            .min(REGISTRY_KEYS_PER_CHUNK);
        if key_count == 0 {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "registry chunk {chunk_id} is outside the registry"
            )));
        }
        let offset = u64::try_from(first_key)
            .ok()
            .and_then(|key| key.checked_mul(REGISTRY_KEY_BYTES as u64))
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("registry byte offset overflow".into())
            })?;
        let length = key_count.checked_mul(REGISTRY_KEY_BYTES).ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid("registry chunk length overflow".into())
        })?;
        let mut keys = if self.registry_chunks.len() == REGISTRY_CACHE_CHUNKS {
            let evicted = self.registry_lru.pop_front().ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("registry LRU is empty".into())
            })?;
            self.registry_chunks.remove(&evicted).ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("registry LRU differs from cache".into())
            })?
        } else {
            Vec::new()
        };
        keys.resize(key_count, [0; REGISTRY_KEY_BYTES]);
        reader.source().read_range_into_slice(
            crate::manifest::REGISTRY_FILE,
            offset,
            keys.as_flattened_mut(),
        )?;
        self.io.record(length)?;
        self.registry_chunks.insert(chunk_id, keys);
        self.registry_lru.push_back(chunk_id);
        Ok(())
    }

    fn touch_registry_chunk(&mut self, chunk_id: u32) {
        if let Some(position) = self.registry_lru.iter().position(|id| *id == chunk_id) {
            self.registry_lru.remove(position);
            self.registry_lru.push_back(chunk_id);
        }
    }

    fn vote_hashes(&self) -> Option<&dyn VoteHashResolver> {
        self.vote_hashes
            .as_ref()
            .map(|registry| registry as &dyn VoteHashResolver)
    }

    fn load_vote_hashes<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
    ) -> CompactV2InstructionSourceResult<()> {
        if self.vote_hashes_loaded {
            return Ok(());
        }
        let Some(binding_size) = reader.file_size(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE) else {
            self.vote_hashes_loaded = true;
            return Ok(());
        };
        let maximum = reader
            .index()
            .rows
            .len()
            .checked_mul(crate::VOTE_HASH_RECORD_LEN)
            .ok_or_else(|| {
                CompactV2InstructionSourceError::Invalid("vote-hash registry bound overflow".into())
            })?
            .min(MAX_VOTE_HASH_REGISTRY_BYTES);
        let size = usize::try_from(binding_size).map_err(|_| {
            CompactV2InstructionSourceError::Invalid(
                "vote-hash registry size exceeds address space".into(),
            )
        })?;
        if size > maximum {
            return Err(CompactV2InstructionSourceError::Invalid(format!(
                "vote-hash registry is {size} bytes, above the {maximum}-byte block bound"
            )));
        }
        let bytes = reader
            .source()
            .read_range(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, 0, size)?;
        self.io.record(bytes.len())?;
        self.vote_hashes = Some(VoteHashRegistry::from_bytes(&bytes)?);
        self.vote_hashes_loaded = true;
        Ok(())
    }

    fn load_blockhashes<'a, S: RangeSource>(
        &'a mut self,
        reader: &ArchiveReader<S>,
    ) -> CompactV2InstructionSourceResult<&'a BlockhashResolver> {
        if self.blockhashes.is_none() {
            let current_size = reader.file_size(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE).ok_or(
                CompactV2InstructionSourceError::MissingSidecar {
                    object: ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
                    purpose: "ambiguous signed-message recent blockhash",
                },
            )?;
            let maximum = reader
                .index()
                .rows
                .len()
                .checked_add(1)
                .and_then(|count| count.checked_mul(crate::BLOCKHASH_RECORD_LEN))
                .ok_or_else(|| {
                    CompactV2InstructionSourceError::Invalid(
                        "blockhash registry bound overflow".into(),
                    )
                })?
                .min(MAX_BLOCKHASH_REGISTRY_BYTES);
            let current_size = usize::try_from(current_size).map_err(|_| {
                CompactV2InstructionSourceError::Invalid(
                    "blockhash registry size exceeds address space".into(),
                )
            })?;
            let registry_offset =
                blockhash_registry_offset(current_size, reader.index().rows.len())?;
            if current_size > maximum {
                return Err(CompactV2InstructionSourceError::Invalid(format!(
                    "blockhash registry is {current_size} bytes, above the {maximum}-byte block bound"
                )));
            }
            let current =
                reader
                    .source()
                    .read_range(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, 0, current_size)?;
            self.io.record(current.len())?;

            let previous = if reader.epoch() == 0 || registry_offset == 1 {
                PreviousBlockhashTail {
                    schema: PreviousBlockhashTailSchema::CurrentHashAndSlot,
                    entries: Vec::new(),
                }
            } else {
                let binding_size = reader
                    .file_size(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
                    .ok_or(CompactV2InstructionSourceError::MissingSidecar {
                        object: ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
                        purpose: "ambiguous signed-message previous blockhash",
                    })?;
                let expected = PREVIOUS_BLOCKHASH_RECORDS
                    .checked_mul(crate::PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN)
                    .ok_or_else(|| {
                        CompactV2InstructionSourceError::Invalid(
                            "previous blockhash tail bound overflow".into(),
                        )
                    })?;
                let size = usize::try_from(binding_size).map_err(|_| {
                    CompactV2InstructionSourceError::Invalid(
                        "previous blockhash tail size exceeds address space".into(),
                    )
                })?;
                if size != expected {
                    return Err(CompactV2InstructionSourceError::Invalid(format!(
                        "previous blockhash tail is {size} bytes, expected {expected} current-schema bytes"
                    )));
                }
                let bytes =
                    reader
                        .source()
                        .read_range(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, 0, size)?;
                self.io.record(bytes.len())?;
                parse_previous_blockhash_tail(
                    &bytes,
                    PreviousBlockhashTailSchema::CurrentHashAndSlot,
                )?
            };
            self.blockhashes = Some(BlockhashResolver::from_bytes(&current, previous)?);
        }
        self.blockhashes.as_ref().ok_or_else(|| {
            CompactV2InstructionSourceError::Invalid(
                "blockhash resolver was not initialized".into(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        num::NonZeroU32,
        path::Path,
        sync::{Arc, Mutex},
    };

    use blockzilla_format::{
        ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader,
        ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage,
        ArchiveV2HotMetaRecord, ArchiveV2HotV0Message, ArchiveV2VoteHashRef,
        ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync, CompactInnerInstruction,
        CompactInnerInstructions, CompactInstructionError, CompactMessageHeader, CompactMetaV1,
        CompactTokenBalance, CompactTransactionError, OwnedCompactAddressTableLookup,
        WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
        WincodeArchiveV2Footer, WincodeArchiveV2Header, wincode_leb128_config,
        write_archive_v2_hot_block_index,
    };
    use blockzilla_query_sdk::{
        ArchiveInstructionSourceExt, BlockView, CanonicalBlock, CpiCoverage, ExecutionStatus,
        InstructionCoverage, InstructionDataCoverage, ScanRange,
    };
    use ed25519_dalek::{Signer, SigningKey};
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        HashVerification, LocalRangeSource, OpenOptions, SignedInstruction, SignedMessage,
        compact_query::ExactContext,
        manifest::{
            BLOCK_INDEX_FILE, BLOCKS_FILE, GENERATION_MANIFEST_FILE, GenerationFile,
            GenerationManifest, META_FILE, REGISTRY_FILE, SIGNATURES_FILE,
            TrustedGenerationIdentity, compute_generation_digest,
        },
        reconstruct_instruction_data_candidates, serialize_signed_message,
    };

    const EPOCH: u64 = 1;
    const FIRST_SLOT: u64 = 100;
    const SLOTS_PER_EPOCH: u64 = 100;
    const TOKEN_PROGRAM: [u8; 32] = [2; 32];
    const VOTE_PROGRAM: [u8; 32] = [3; 32];
    const CPI_PROGRAM: [u8; 32] = [4; 32];
    const LOOKUP_TABLE: [u8; 32] = [5; 32];
    const LOADED_ACCOUNT: [u8; 32] = [6; 32];
    const TARGET_MINT: [u8; 32] = [7; 32];
    const OTHER_MINT: [u8; 32] = [8; 32];
    const TOKEN_OWNER: [u8; 32] = [9; 32];

    struct Fixture {
        directory: TempDir,
        signer: [u8; 32],
        signatures: Vec<[u8; 64]>,
        decoded_bytes: u64,
        compressed_bytes: u64,
    }

    #[derive(Clone)]
    struct CountingSource {
        inner: LocalRangeSource,
        reads: Arc<Mutex<Vec<(String, u64, usize)>>>,
    }

    impl CountingSource {
        fn new(inner: LocalRangeSource) -> Self {
            Self {
                inner,
                reads: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn clear(&self) {
            self.reads.lock().unwrap().clear();
        }

        fn reads_for(&self, object: &str) -> Vec<(u64, usize)> {
            self.reads
                .lock()
                .unwrap()
                .iter()
                .filter(|(name, _, _)| name == object)
                .map(|(_, offset, length)| (*offset, *length))
                .collect()
        }

        fn record(&self, object: &str, offset: u64, length: usize) {
            self.reads
                .lock()
                .unwrap()
                .push((object.to_owned(), offset, length));
        }
    }

    impl RangeSource for CountingSource {
        fn size(&self, object: &str) -> crate::SourceResult<Option<u64>> {
            self.inner.size(object)
        }

        fn read_range(
            &self,
            object: &str,
            offset: u64,
            length: usize,
        ) -> crate::SourceResult<Vec<u8>> {
            self.record(object, offset, length);
            self.inner.read_range(object, offset, length)
        }

        fn read_range_into(
            &self,
            object: &str,
            offset: u64,
            length: usize,
            destination: &mut Vec<u8>,
        ) -> crate::SourceResult<()> {
            self.record(object, offset, length);
            self.inner
                .read_range_into(object, offset, length, destination)
        }

        fn read_range_into_slice(
            &self,
            object: &str,
            offset: u64,
            destination: &mut [u8],
        ) -> crate::SourceResult<()> {
            self.record(object, offset, destination.len());
            self.inner
                .read_range_into_slice(object, offset, destination)
        }
    }

    #[derive(Clone)]
    struct FailingRegistrySource {
        inner: LocalRangeSource,
        fail_registry_reads: Arc<Mutex<bool>>,
    }

    impl FailingRegistrySource {
        fn new(inner: LocalRangeSource) -> Self {
            Self {
                inner,
                fail_registry_reads: Arc::new(Mutex::new(false)),
            }
        }

        fn arm(&self) {
            *self.fail_registry_reads.lock().unwrap() = true;
        }

        fn reject_registry_read(&self, object: &str) -> crate::SourceResult<()> {
            if object == REGISTRY_FILE && *self.fail_registry_reads.lock().unwrap() {
                return Err(crate::SourceError::Protocol(
                    "injected registry prefetch failure".into(),
                ));
            }
            Ok(())
        }
    }

    impl RangeSource for FailingRegistrySource {
        fn size(&self, object: &str) -> crate::SourceResult<Option<u64>> {
            self.inner.size(object)
        }

        fn read_range(
            &self,
            object: &str,
            offset: u64,
            length: usize,
        ) -> crate::SourceResult<Vec<u8>> {
            self.reject_registry_read(object)?;
            self.inner.read_range(object, offset, length)
        }

        fn read_range_into(
            &self,
            object: &str,
            offset: u64,
            length: usize,
            destination: &mut Vec<u8>,
        ) -> crate::SourceResult<()> {
            self.reject_registry_read(object)?;
            self.inner
                .read_range_into(object, offset, length, destination)
        }

        fn read_range_into_slice(
            &self,
            object: &str,
            offset: u64,
            destination: &mut [u8],
        ) -> crate::SourceResult<()> {
            self.reject_registry_read(object)?;
            self.inner
                .read_range_into_slice(object, offset, destination)
        }
    }

    impl Fixture {
        fn main() -> Self {
            let signing_key = SigningKey::from_bytes(&[41; 32]);
            let signer = signing_key.verifying_key().to_bytes();
            let registry = vec![
                signer,
                TOKEN_PROGRAM,
                VOTE_PROGRAM,
                CPI_PROGRAM,
                LOOKUP_TABLE,
                LOADED_ACCOUNT,
            ];

            let token_and_unrelated_vote = legacy_message(vec![
                raw_instruction(1, &[0], &[3, 11]),
                ArchiveV2HotInstruction {
                    program_id_index: 2,
                    accounts: Vec::new(),
                    data: vote_tower_data(true),
                },
            ]);
            let token_and_unrelated_vote_meta = metadata(3, None, Some(Vec::new()), vec![], vec![]);

            let v0 = blockzilla_format::ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header: header(),
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                recent_blockhash: OwnedCompactRecentBlockhash::Nonce([12; 32]),
                instructions: vec![raw_instruction(1, &[2], &[9, 8])],
                address_table_lookups: vec![OwnedCompactAddressTableLookup {
                    account_key: CompactPubkey::Id(5),
                    writable_indexes: vec![0],
                    readonly_indexes: vec![1],
                }],
            });
            let v0_meta = metadata(
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
                vec![CompactPubkey::Id(6)],
                vec![CompactPubkey::Id(4)],
            );

            let failed = legacy_message(vec![
                raw_instruction(1, &[0], &[1]),
                raw_instruction(1, &[0], &[2]),
            ]);
            let failed_meta = metadata(
                2,
                Some(CompactTransactionError::InstructionError(
                    1,
                    CompactInstructionError::Custom(42),
                )),
                Some(vec![CompactInnerInstructions {
                    index: 1,
                    instructions: vec![CompactInnerInstruction {
                        program_id_index: 1,
                        accounts: vec![0],
                        data: vec![4],
                        stack_height: Some(2),
                    }],
                }]),
                vec![],
                vec![],
            );

            let missing_meta = legacy_message(vec![raw_instruction(1, &[0], &[5])]);
            let raw_meta = legacy_message(vec![raw_instruction(1, &[0], &[6])]);
            let decoded_without_cpi = legacy_message(vec![raw_instruction(1, &[0], &[7])]);

            let signatures = vec![
                [21; 64], [22; 64], [23; 64], [24; 64], [25; 64], [26; 64], [27; 64],
            ];
            let transactions = vec![
                TxFixture::exact(
                    token_and_unrelated_vote,
                    token_and_unrelated_vote_meta,
                    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                ),
                TxFixture::exact(
                    v0,
                    v0_meta,
                    ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                        | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES
                        | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                ),
                TxFixture::exact(
                    failed,
                    failed_meta,
                    ARCHIVE_V2_TX_FLAG_HAS_ERROR | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                ),
                TxFixture::without_metadata(missing_meta),
                TxFixture::raw_metadata(raw_meta),
                TxFixture::raw_transaction(vec![0xff]),
                TxFixture::exact(
                    decoded_without_cpi,
                    metadata(2, None, None, vec![], vec![]),
                    0,
                ),
            ];
            Self::build(registry, vec![Vec::new(), transactions], Some(signatures))
        }

        fn ambiguous(with_signatures: bool) -> (Self, Vec<u8>) {
            let signing_key = SigningKey::from_bytes(&[44; 32]);
            let signer = signing_key.verifying_key().to_bytes();
            let data = vote_tower_data(false);
            let candidates = reconstruct_instruction_data_candidates(&data, None).unwrap();
            assert_eq!(candidates.len(), 2);
            let selected_data = candidates[1].bytes.to_vec();
            let signed_message = serialize_signed_message(&SignedMessage {
                version: SignedMessageVersion::Legacy,
                header: header(),
                static_account_keys: &[signer, VOTE_PROGRAM],
                recent_blockhash: [13; 32],
                instructions: &[SignedInstruction {
                    program_id_index: 1,
                    accounts: &[],
                    data: &selected_data,
                }],
            })
            .unwrap();
            let signature = signing_key.sign(&signed_message).to_bytes();
            let message = legacy_message(vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: Vec::new(),
                data,
            }]);
            let transaction = TxFixture::exact(
                message,
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            );
            let signatures = with_signatures.then_some(vec![signature]);
            (
                Self::build(
                    vec![signer, VOTE_PROGRAM],
                    vec![vec![transaction]],
                    signatures,
                ),
                selected_data,
            )
        }

        fn ambiguous_with_trusted_vote_sidecar() -> (Self, Vec<u8>) {
            let signing_key = SigningKey::from_bytes(&[46; 32]);
            let signer = signing_key.verifying_key().to_bytes();
            let vote_hash_bytes = vote_hash_registry_bytes();
            let vote_hashes = VoteHashRegistry::from_bytes(&vote_hash_bytes).unwrap();
            let data = vote_tower_data(true);
            let candidates = reconstruct_instruction_data_candidates(
                &data,
                Some(&vote_hashes as &dyn VoteHashResolver),
            )
            .unwrap();
            assert_eq!(candidates.len(), 2);
            let selected_data = candidates[1].bytes.to_vec();
            let signed_message = serialize_signed_message(&SignedMessage {
                version: SignedMessageVersion::Legacy,
                header: header(),
                static_account_keys: &[signer, VOTE_PROGRAM],
                recent_blockhash: [13; 32],
                instructions: &[SignedInstruction {
                    program_id_index: 1,
                    accounts: &[],
                    data: &selected_data,
                }],
            })
            .unwrap();
            let signature = signing_key.sign(&signed_message).to_bytes();
            let transaction = TxFixture::exact(
                legacy_message(vec![ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: Vec::new(),
                    data,
                }]),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            );
            let fixture = Self::build(
                vec![signer, VOTE_PROGRAM],
                vec![vec![transaction]],
                Some(vec![signature]),
            );
            fs::write(
                fixture
                    .directory
                    .path()
                    .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
                vote_hash_bytes,
            )
            .unwrap();
            fs::write(
                fixture
                    .directory
                    .path()
                    .join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
                [51; 32],
            )
            .unwrap();
            fs::write(
                fixture
                    .directory
                    .path()
                    .join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE),
                vec![0; PREVIOUS_BLOCKHASH_RECORDS * crate::PREVIOUS_BLOCKHASH_CURRENT_RECORD_LEN],
            )
            .unwrap();
            (fixture, selected_data)
        }

        fn invalid_raw_metadata_flag() -> Self {
            let signing_key = SigningKey::from_bytes(&[47; 32]);
            let signer = signing_key.verifying_key().to_bytes();
            let transaction = TxFixture {
                flags: ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                message: encode(&legacy_message(vec![raw_instruction(1, &[0], &[1])])),
                metadata: None,
            };
            Self::build(vec![signer, TOKEN_PROGRAM], vec![vec![transaction]], None)
        }

        fn empty_with_large_registry() -> Self {
            let signing_key = SigningKey::from_bytes(&[49; 32]);
            let signer = signing_key.verifying_key().to_bytes();
            let count = REGISTRY_KEYS_PER_CHUNK * (REGISTRY_CACHE_CHUNKS + 1);
            let registry = (0..count)
                .map(|index| {
                    let mut key = [0u8; 32];
                    key[..8].copy_from_slice(&(index as u64).to_le_bytes());
                    key
                })
                .collect();
            Self::build(registry, Vec::new(), None).with_signer(signer)
        }

        fn with_signer(mut self, signer: [u8; 32]) -> Self {
            self.signer = signer;
            self
        }

        fn build(
            registry: Vec<[u8; 32]>,
            blocks: Vec<Vec<TxFixture>>,
            signatures: Option<Vec<[u8; 64]>>,
        ) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            fs::write(
                root.join(REGISTRY_FILE),
                registry.iter().flatten().copied().collect::<Vec<_>>(),
            )
            .unwrap();

            let mut encoded_blocks = Vec::new();
            let mut index_rows = Vec::new();
            let mut compressed_offset = 0u64;
            let mut transaction_ordinal = 0u64;
            let mut signature_ordinal = 0u64;
            let mut decoded_bytes = 0u64;
            for (block_id, transactions) in blocks.into_iter().enumerate() {
                let mut rows = Vec::new();
                let mut messages = Vec::new();
                let mut metadata = Vec::new();
                for (tx_index, transaction) in transactions.into_iter().enumerate() {
                    let message_offset = messages.len() as u32;
                    let metadata_offset = metadata.len() as u32;
                    messages.extend_from_slice(&transaction.message);
                    if let Some(bytes) = &transaction.metadata {
                        metadata.extend_from_slice(bytes);
                    }
                    rows.push(ArchiveV2HotTxRow {
                        tx_index: tx_index as u32,
                        flags: transaction.flags,
                        message_offset,
                        message_len: transaction.message.len() as u32,
                        metadata_offset,
                        metadata_len: transaction
                            .metadata
                            .as_ref()
                            .map_or(0, |bytes| bytes.len() as u32),
                        signature_count: 1,
                        reserved: [0; 3],
                    });
                }
                let slot = FIRST_SLOT + 1 + block_id as u64;
                let blob = ArchiveV2HotBlockBlob {
                    header: ArchiveV2HotBlockHeader {
                        slot,
                        parent_slot: slot - 1,
                        blockhash_id: block_id as u32 + 1,
                        previous_blockhash_id: block_id as u32,
                        block_time: None,
                        block_height: None,
                        rewards: None,
                    },
                    tx_count: rows.len() as u32,
                    tx_rows: rows,
                    message_bytes: messages,
                    metadata_bytes: metadata,
                };
                let uncompressed =
                    wincode::config::serialize(&blob, wincode_leb128_config()).unwrap();
                let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
                let signature_count = blob.tx_count;
                index_rows.push(blockzilla_format::ArchiveV2HotBlockIndexRow {
                    block_id: block_id as u32,
                    slot,
                    compressed_offset,
                    compressed_len: compressed.len() as u32,
                    uncompressed_len: uncompressed.len() as u32,
                    tx_count: blob.tx_count,
                    first_tx_ordinal: transaction_ordinal,
                    first_signature_ordinal: signature_ordinal,
                    signature_count,
                });
                compressed_offset += compressed.len() as u64;
                transaction_ordinal += u64::from(blob.tx_count);
                signature_ordinal += u64::from(signature_count);
                decoded_bytes += uncompressed.len() as u64;
                encoded_blocks.extend_from_slice(&compressed);
            }
            fs::write(root.join(BLOCKS_FILE), &encoded_blocks).unwrap();
            write_archive_v2_hot_block_index(
                &root.join(BLOCK_INDEX_FILE),
                encoded_blocks.len() as u64,
                1,
                0,
                &index_rows,
            )
            .unwrap();

            let transaction_count = index_rows.iter().map(|row| u64::from(row.tx_count)).sum();
            write_metadata_file(root, index_rows.len() as u64, transaction_count);
            if let Some(signatures) = &signatures {
                fs::write(
                    root.join(SIGNATURES_FILE),
                    signatures.iter().flatten().copied().collect::<Vec<_>>(),
                )
                .unwrap();
            }
            Self {
                directory,
                signer: registry.first().copied().unwrap_or([0; 32]),
                signatures: signatures.unwrap_or_default(),
                decoded_bytes,
                compressed_bytes: encoded_blocks.len() as u64,
            }
        }

        fn trusted_reader(&self) -> ArchiveReader<LocalRangeSource> {
            self.trusted_reader_with_candidate("compact-query-fixture")
        }

        fn trusted_reader_with_candidate(
            &self,
            candidate_id: &str,
        ) -> ArchiveReader<LocalRangeSource> {
            ArchiveReader::open_trusted(
                LocalRangeSource::new(self.directory.path()),
                TrustedGenerationIdentity {
                    cluster_id: "testnet".into(),
                    epoch: EPOCH,
                    generation_id: candidate_id.into(),
                    slots_per_epoch: SLOTS_PER_EPOCH,
                },
                OpenOptions {
                    hash_verification: HashVerification::SizesOnly,
                    ..OpenOptions::default()
                },
            )
            .unwrap()
        }

        fn published_reader(&self) -> ArchiveReader<LocalRangeSource> {
            write_manifest(self.directory.path());
            ArchiveReader::open(LocalRangeSource::new(self.directory.path())).unwrap()
        }
    }

    fn open_trusted_test_reader<S: RangeSource>(source: S) -> ArchiveReader<S> {
        ArchiveReader::open_trusted(
            source,
            TrustedGenerationIdentity {
                cluster_id: "testnet".into(),
                epoch: EPOCH,
                generation_id: "compact-query-policy-fixture".into(),
                slots_per_epoch: SLOTS_PER_EPOCH,
            },
            OpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap()
    }

    struct TxFixture {
        flags: u32,
        message: Vec<u8>,
        metadata: Option<Vec<u8>>,
    }

    impl TxFixture {
        fn exact(
            message: blockzilla_format::ArchiveV2HotMessagePayload,
            metadata: CompactMetaV1,
            flags: u32,
        ) -> Self {
            Self {
                flags: flags | ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                message: encode(&message),
                metadata: Some(encode(&metadata)),
            }
        }

        fn without_metadata(message: blockzilla_format::ArchiveV2HotMessagePayload) -> Self {
            Self {
                flags: 0,
                message: encode(&message),
                metadata: None,
            }
        }

        fn raw_metadata(message: blockzilla_format::ArchiveV2HotMessagePayload) -> Self {
            Self {
                flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                message: encode(&message),
                metadata: Some(vec![0xde, 0xad]),
            }
        }

        fn raw_transaction(message: Vec<u8>) -> Self {
            Self {
                flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                message,
                metadata: None,
            }
        }
    }

    fn header() -> CompactMessageHeader {
        CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        }
    }

    fn legacy_message(
        instructions: Vec<ArchiveV2HotInstruction>,
    ) -> blockzilla_format::ArchiveV2HotMessagePayload {
        let account_keys = if instructions
            .iter()
            .any(|instruction| instruction.program_id_index == 2)
        {
            vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(2),
                CompactPubkey::Id(3),
            ]
        } else {
            vec![CompactPubkey::Id(1), CompactPubkey::Id(2)]
        };
        blockzilla_format::ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: header(),
            account_keys,
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([13; 32]),
            instructions,
        })
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

    fn vote_hash_registry_bytes() -> Vec<u8> {
        let mut bytes = Vec::with_capacity(crate::VOTE_HASH_RECORD_LEN);
        bytes.push(0b11);
        bytes.extend_from_slice(&[32; 32]);
        bytes.extend_from_slice(&[33; 32]);
        bytes
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
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    fn encode<T: wincode::SchemaWrite<blockzilla_format::WincodeLeb128Config, Src = T>>(
        value: &T,
    ) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn write_metadata_file(root: &Path, blocks: u64, transactions: u64) {
        let records = [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks,
                transactions,
                ..WincodeArchiveV2Footer::default()
            }),
        ];
        let mut bytes = Vec::new();
        for record in records {
            let encoded = encode(&record);
            write_u32_varint(&mut bytes, encoded.len() as u32);
            bytes.extend_from_slice(&encoded);
        }
        fs::write(root.join(META_FILE), bytes).unwrap();
    }

    fn write_manifest(root: &Path) {
        let mut files = Vec::new();
        for name in [
            BLOCKS_FILE,
            BLOCK_INDEX_FILE,
            META_FILE,
            REGISTRY_FILE,
            SIGNATURES_FILE,
        ] {
            let Ok(bytes) = fs::read(root.join(name)) else {
                continue;
            };
            files.push(GenerationFile {
                name: name.into(),
                size: bytes.len() as u64,
                sha256: hex_lower(&Sha256::digest(&bytes)),
            });
        }
        let mut manifest = GenerationManifest {
            schema_version: 1,
            cluster_id: "testnet".into(),
            epoch: EPOCH,
            generation_id: "published-compact-query-fixture".into(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: SLOTS_PER_EPOCH,
            complete: true,
            files,
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        fs::write(
            root.join(GENERATION_MANIFEST_FILE),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
    }

    fn hex_lower(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
        output
    }

    fn write_u32_varint(output: &mut Vec<u8>, mut value: u32) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }

    #[test]
    fn count_projection_never_reads_registry_with_one_or_twelve_workers() {
        let fixture = Fixture::main();
        for workers in [1, 12] {
            let input = FailingRegistrySource::new(LocalRangeSource::new(fixture.directory.path()));
            let reader = open_trusted_test_reader(input.clone());
            input.arm();
            let mut source = CompactV2InstructionSource::new(reader, FIRST_SLOT).unwrap();
            let request = ScanRequest::all()
                .allow_incomplete_instructions()
                .allow_incomplete_cpi()
                .without_primary_signatures()
                .without_required_signers()
                .without_execution_status()
                .without_instruction_programs();
            let mut inner = 0;
            let mut sink = blockzilla_query_sdk::FnBlockSink::new(|block: BlockView<'_>| {
                for tx in block.transactions {
                    for instruction in &tx.instructions {
                        assert_eq!(instruction.program_id, None);
                        inner += u64::from(instruction.coordinate.inner_index.is_some());
                    }
                }
                Ok(())
            });
            let result = source
                .scan_ordered_parallel(
                    &request,
                    &mut sink,
                    CompactV2ParallelScanConfig::new(workers),
                )
                .unwrap();
            assert_eq!(
                (
                    result.scan.blocks,
                    result.scan.transactions,
                    result.scan.instructions
                ),
                (2, 7, 10)
            );
            assert_eq!(inner, 2);
            assert_eq!(result.registry.prefetch_read_bytes, 0);
            assert_eq!(
                result.scan.io.source_read_bytes,
                Some(fixture.compressed_bytes)
            );
        }
    }

    #[test]
    fn program_filter_binds_once_before_parallel_projection() {
        let fixture = Fixture::main();
        let input = CountingSource::new(LocalRangeSource::new(fixture.directory.path()));
        let mut source =
            CompactV2InstructionSource::new(open_trusted_test_reader(input.clone()), FIRST_SLOT)
                .unwrap();
        input.clear();
        let request = ScanRequest::all()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .without_primary_signatures()
            .without_required_signers()
            .without_execution_status()
            .with_instruction_programs_for([TOKEN_PROGRAM]);
        let mut matched = 0;
        let mut sink = blockzilla_query_sdk::FnBlockSink::new(|block: BlockView<'_>| {
            for tx in block.transactions {
                for instruction in &tx.instructions {
                    assert!(
                        instruction.program_id.is_none()
                            || instruction.program_id == Some(TOKEN_PROGRAM)
                    );
                    matched += usize::from(instruction.program_id.is_some());
                }
            }
            Ok(())
        });
        let result = source
            .scan_ordered_parallel(&request, &mut sink, CompactV2ParallelScanConfig::new(12))
            .unwrap();
        assert!(matched > 0);
        assert_eq!(result.scan.instructions, 10);
        assert_eq!(input.reads_for(REGISTRY_FILE).len(), 1);
        assert_eq!(result.registry.prefetch_read_bytes, 0);
    }

    #[test]
    fn publishes_exact_order_loaded_keys_cpi_failure_coverage_and_io() {
        let fixture = Fixture::main();
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        assert_eq!(
            source.identity().verification,
            SourceVerification::OperatorTrusted
        );
        assert_eq!(
            source.identity().binding.as_deref(),
            Some("operator-trusted-candidate-id=compact-query-fixture")
        );

        let request = ScanRequest::all()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .with_instruction_data_for([TOKEN_PROGRAM]);
        let mut blocks = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                blocks.push((block.header, block.transactions.to_vec()));
                Ok(())
            })
            .unwrap();

        assert_eq!(blocks.len(), 2);
        assert!(blocks[0].1.is_empty());
        let transactions = &blocks[1].1;
        assert_eq!(transactions.len(), 7);
        assert_eq!(
            transactions
                .iter()
                .map(|transaction| transaction.header.tx_index)
                .collect::<Vec<_>>(),
            [0, 1, 2, 3, 4, 5, 6]
        );
        assert_eq!(
            transactions[0].primary_signature,
            Some(fixture.signatures[0])
        );
        assert_eq!(transactions[0].required_signers, [fixture.signer]);
        assert_eq!(transactions[0].header.cpi_coverage, CpiCoverage::Complete);
        assert_eq!(transactions[0].instructions[0].data, [3, 11]);
        assert_eq!(
            transactions[0].instructions[0].data_coverage,
            InstructionDataCoverage::Exact
        );
        assert_eq!(
            transactions[0].instructions[1].data_coverage,
            InstructionDataCoverage::NotRequested
        );
        assert!(transactions[0].instructions[1].data.is_empty());

        let v0 = &transactions[1];
        assert_eq!(v0.instructions.len(), 2);
        assert_eq!(v0.instructions[0].accounts, [LOADED_ACCOUNT]);
        assert_eq!(v0.instructions[1].program_id, Some(CPI_PROGRAM));
        assert_eq!(v0.instructions[1].accounts, [LOADED_ACCOUNT]);
        assert_eq!(v0.instructions[1].coordinate.order, 1);
        assert_eq!(v0.instructions[1].coordinate.outer_index, 0);
        assert_eq!(v0.instructions[1].coordinate.inner_index, Some(0));
        assert_eq!(v0.instructions[1].coordinate.stack_height, Some(2));
        assert_eq!(
            v0.instructions[1].data_coverage,
            InstructionDataCoverage::NotRequested
        );

        assert_eq!(transactions[2].header.status, ExecutionStatus::Failed);
        assert_eq!(
            transactions[2].header.failed_outer_instruction_index,
            Some(1)
        );
        assert_eq!(transactions[2].instructions[2].coordinate.outer_index, 1);
        assert_eq!(
            transactions[2].instructions[2].coordinate.inner_index,
            Some(0)
        );

        assert_eq!(
            transactions[3].header.status,
            ExecutionStatus::Unknown(CoverageReason::MetadataAbsent)
        );
        assert_eq!(
            transactions[3].header.cpi_coverage,
            CpiCoverage::Unknown(CoverageReason::MetadataAbsent)
        );
        assert_eq!(
            transactions[3].header.instruction_coverage,
            InstructionCoverage::Complete
        );
        assert_eq!(
            transactions[4].header.status,
            ExecutionStatus::Unknown(CoverageReason::RawMetadata)
        );
        assert_eq!(
            transactions[4].header.cpi_coverage,
            CpiCoverage::Unknown(CoverageReason::RawMetadata)
        );
        assert_eq!(
            transactions[5].header.instruction_coverage,
            InstructionCoverage::Unknown(CoverageReason::RawTransaction)
        );
        assert!(transactions[5].instructions.is_empty());
        assert_eq!(
            transactions[6].header.cpi_coverage,
            CpiCoverage::NotRecorded
        );

        assert_eq!(receipt.blocks, 2);
        assert_eq!(receipt.transactions, 7);
        assert_eq!(receipt.instructions, 10);
        assert_eq!(receipt.instructions_not_requested, 2);
        assert_eq!(receipt.instructions_with_unknown_data, 0);
        assert_eq!(receipt.transactions_with_incomplete_instructions, 1);
        assert_eq!(receipt.transactions_with_incomplete_cpi, 4);
        assert_eq!(receipt.transactions_with_unknown_execution, 3);
        // This fixture has no MPHF: bind query IDs with one registry pass.
        assert_eq!(receipt.io.source_read_calls, Some(4));
        assert_eq!(
            receipt.io.source_read_bytes,
            Some(fixture.compressed_bytes + 2 * 6 * 32 + 7 * 64)
        );
        assert_eq!(receipt.io.decoded_bytes, Some(fixture.decoded_bytes));
    }

    #[test]
    fn failed_detail_filter_keeps_headers_counts_and_unknown_status() {
        for workers in [1, 3] {
            let fixture = Fixture::main();
            let mut source =
                CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
            let request = ScanRequest::all()
                .allow_incomplete_instructions()
                .allow_incomplete_cpi()
                .without_primary_signatures()
                .without_required_signers()
                .without_instruction_accounts()
                .without_instruction_data()
                .without_failed_transaction_details();
            let mut transactions = Vec::new();
            let mut sink = blockzilla_query_sdk::FnBlockSink::new(|block: BlockView<'_>| {
                transactions.extend_from_slice(block.transactions);
                Ok(())
            });
            let receipt = source
                .scan_ordered_parallel(
                    &request,
                    &mut sink,
                    CompactV2ParallelScanConfig::new(workers),
                )
                .unwrap();
            assert_eq!(receipt.scan.transactions, 7);
            assert_eq!(transactions[2].header.status, ExecutionStatus::Failed);
            assert!(transactions[2].instructions.is_empty());
            assert!(transactions[2].token_balances.is_empty());
            assert!(!request.needs_primary_signature(&transactions[2]));
            assert!(matches!(
                transactions[3].header.status,
                ExecutionStatus::Unknown(CoverageReason::MetadataAbsent)
            ));
            assert!(matches!(
                transactions[4].header.status,
                ExecutionStatus::Unknown(CoverageReason::RawMetadata)
            ));
            assert!(!transactions[0].instructions.is_empty());
        }
    }

    #[test]
    fn program_only_projection_skips_instruction_account_resolution_and_allocation() {
        let far_account_id = REGISTRY_KEYS_PER_CHUNK + 1;
        let farther_account_id = 2 * REGISTRY_KEYS_PER_CHUNK + 1;
        let mut registry = vec![[0u8; 32]; farther_account_id];
        registry[0] = [0x11; 32];
        registry[1] = TOKEN_PROGRAM;
        registry[far_account_id - 1] = [0xa1; 32];
        registry[farther_account_id - 1] = [0xa2; 32];
        let message =
            blockzilla_format::ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: header(),
                account_keys: vec![
                    CompactPubkey::Id(1),
                    CompactPubkey::Id(2),
                    CompactPubkey::Id(u32::try_from(far_account_id).unwrap()),
                    CompactPubkey::Id(u32::try_from(farther_account_id).unwrap()),
                ],
                recent_blockhash: OwnedCompactRecentBlockhash::Nonce([13; 32]),
                instructions: vec![raw_instruction(1, &[2, 3], &[])],
            });
        let fixture = Fixture::build(
            registry,
            vec![vec![TxFixture::exact(
                message,
                metadata(4, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )]],
            None,
        );

        let run = |request: &ScanRequest| {
            let range_source = CountingSource::new(LocalRangeSource::new(fixture.directory.path()));
            let observed_source = range_source.clone();
            let reader = ArchiveReader::open_trusted(
                range_source,
                TrustedGenerationIdentity {
                    cluster_id: "testnet".into(),
                    epoch: EPOCH,
                    generation_id: "compact-query-program-only-fixture".into(),
                    slots_per_epoch: SLOTS_PER_EPOCH,
                },
                OpenOptions {
                    hash_verification: HashVerification::SizesOnly,
                    ..OpenOptions::default()
                },
            )
            .unwrap();
            observed_source.clear();
            let mut source = CompactV2InstructionSource::new(reader, FIRST_SLOT).unwrap();
            let mut transaction = None;
            let mut account_capacity_is_zero = false;
            source
                .for_each_block(request, |block| {
                    let projected = &block.transactions[0];
                    account_capacity_is_zero = projected
                        .instructions
                        .iter()
                        .all(|instruction| instruction.accounts.capacity() == 0);
                    transaction = Some(projected.clone());
                    Ok(())
                })
                .unwrap();
            (
                transaction.unwrap(),
                account_capacity_is_zero,
                observed_source.reads_for(REGISTRY_FILE),
            )
        };

        let full_request = ScanRequest::all()
            .allow_unverified_source()
            .without_primary_signatures()
            .without_instruction_data();
        let (full, _, full_registry_reads) = run(&full_request);
        let (program_only, no_account_allocation, program_only_registry_reads) =
            run(&full_request.clone().without_instruction_accounts());

        let mut expected = full;
        for instruction in &mut expected.instructions {
            instruction.accounts.clear();
        }
        assert_eq!(program_only, expected);
        assert!(no_account_allocation);
        assert_eq!(program_only_registry_reads.len(), 1);
        assert_eq!(full_registry_reads.len(), 3);
    }

    #[test]
    fn program_only_projection_keeps_loaded_cpi_programs_and_signers() {
        let fixture = Fixture::main();
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let request = ScanRequest::all()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_primary_signatures()
            .without_instruction_accounts()
            .without_instruction_data();
        let mut observed = None;
        source
            .for_each_block(&request, |block| {
                if block.header.block_ordinal == 1 {
                    observed = block.transactions.get(1).cloned();
                }
                Ok(())
            })
            .unwrap();

        let transaction = observed.unwrap();
        assert_eq!(transaction.required_signers, [fixture.signer]);
        assert_eq!(transaction.instructions.len(), 2);
        assert_eq!(transaction.instructions[0].program_id, Some(TOKEN_PROGRAM));
        assert_eq!(transaction.instructions[1].program_id, Some(CPI_PROGRAM));
        assert_eq!(transaction.instructions[1].coordinate.inner_index, Some(0));
        assert!(transaction.instructions.iter().all(|instruction| {
            instruction.accounts.is_empty() && instruction.accounts.capacity() == 0
        }));
    }

    #[test]
    fn signer_query_does_not_project_instructions_from_failed_transactions() {
        let signer = [0x31; 32];
        let program = [0x32; 32];
        let message = legacy_message((0..5).map(|_| raw_instruction(1, &[0], &[])).collect());
        let metadata = metadata(
            2,
            Some(CompactTransactionError::InstructionError(
                3,
                CompactInstructionError::Custom(42),
            )),
            Some(vec![CompactInnerInstructions {
                index: 4,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: Vec::new(),
                    stack_height: Some(2),
                }],
            }]),
            Vec::new(),
            Vec::new(),
        );
        let fixture = Fixture::build(
            vec![signer, program],
            vec![vec![TxFixture::exact(
                message,
                metadata,
                ARCHIVE_V2_TX_FLAG_HAS_ERROR | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )]],
            None,
        );
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let request = ScanRequest::all()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .without_primary_signatures()
            .without_instruction_accounts()
            .without_instruction_data()
            .with_required_signer(signer);
        let mut observed = None;

        source
            .for_each_block(&request, |block| {
                observed = block.transactions.first().cloned();
                Ok(())
            })
            .unwrap();

        let transaction = observed.unwrap();
        assert_eq!(transaction.header.status, ExecutionStatus::Failed);
        assert_eq!(transaction.required_signers, [signer]);
        assert!(transaction.instructions.is_empty());
        assert_eq!(
            transaction.header.instruction_coverage,
            InstructionCoverage::Unknown(CoverageReason::ProjectionNotRequested)
        );

        // A complete instruction scan still exposes the contradictory source
        // metadata instead of silently accepting it.
        let mut strict =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let error = strict
            .for_each_block(
                &ScanRequest::all()
                    .allow_incomplete_instructions()
                    .allow_incomplete_cpi()
                    .without_primary_signatures()
                    .without_instruction_accounts()
                    .without_instruction_data(),
                |_| Ok(()),
            )
            .unwrap_err();
        assert!(error.to_string().contains("after failed outer index 3"));
    }

    #[test]
    fn program_only_projection_rejects_an_invalid_instruction_account_index() {
        let fixture = Fixture::build(
            vec![[0x11; 32], TOKEN_PROGRAM],
            vec![vec![TxFixture::exact(
                legacy_message(vec![raw_instruction(1, &[2], &[])]),
                metadata(2, None, Some(Vec::new()), vec![], vec![]),
                ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            )]],
            None,
        );
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let request = ScanRequest::all()
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
    fn ordered_scan_coalesces_contiguous_block_signature_windows() {
        const BLOCKS: usize = 4;
        let blocks = (0..BLOCKS)
            .map(|block| vec![TxFixture::raw_transaction(vec![block as u8])])
            .collect::<Vec<_>>();
        let signatures = (0..BLOCKS)
            .map(|block| [block as u8 + 1; SIGNATURE_BYTES])
            .collect::<Vec<_>>();
        let fixture = Fixture::build(Vec::new(), blocks, Some(signatures.clone()));
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
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
        assert_eq!(receipt.io.source_read_calls, Some(2));
        assert_eq!(
            receipt.io.source_read_bytes,
            Some(fixture.compressed_bytes + (BLOCKS * SIGNATURE_BYTES) as u64)
        );
    }

    #[test]
    fn sequential_dense_registry_policy_applies_the_full_scan_threshold_and_size_bounds() {
        let registry_bytes = 889_551_808;
        let policy = CompactV2RegistryReadPolicy::with_full_registry_limit(registry_bytes);

        assert_eq!(policy.max_full_registry_bytes(), registry_bytes);
        assert!(should_prefetch_sequential_registry(
            policy,
            false,
            COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
            true,
            registry_bytes,
        ));
        assert!(!should_prefetch_sequential_registry(
            policy,
            false,
            COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS - 1,
            true,
            registry_bytes,
        ));
        assert!(should_prefetch_sequential_registry(
            policy,
            true,
            1,
            true,
            registry_bytes,
        ));
        assert!(!should_prefetch_sequential_registry(
            policy,
            true,
            1,
            false,
            registry_bytes,
        ));
        assert!(!should_prefetch_sequential_registry(
            CompactV2RegistryReadPolicy::sparse_only(),
            true,
            COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
            true,
            registry_bytes,
        ));
        assert!(!should_prefetch_sequential_registry(
            CompactV2RegistryReadPolicy::with_full_registry_limit(registry_bytes - 1),
            true,
            COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
            true,
            registry_bytes,
        ));
    }

    #[test]
    fn sequential_dense_registry_prefetch_is_in_the_scan_io_receipt_once() {
        let fixture = Fixture::main();
        let counted = CountingSource::new(LocalRangeSource::new(fixture.directory.path()));
        let observed = counted.clone();
        let reader = open_trusted_test_reader(counted);
        observed.clear();
        let mut source = CompactV2InstructionSource::new(reader, FIRST_SLOT).unwrap();
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .with_instruction_data_for([TOKEN_PROGRAM]);
        struct NoopSink;
        impl BlockSink for NoopSink {
            fn visit_block(&mut self, _block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
                Ok(())
            }
        }
        let policy = CompactV2RegistryReadPolicy::with_full_registry_limit(
            DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES,
        );

        let first = source
            .scan_ordered_with_registry_policy(&request, policy, &mut NoopSink)
            .unwrap();
        assert!(source.context.shared_registry.is_some());
        assert_eq!(observed.reads_for(REGISTRY_FILE), vec![(0, 6 * 32)]);
        assert_eq!(first.io.source_read_calls, Some(3));
        assert_eq!(
            first.io.source_read_bytes,
            Some(fixture.compressed_bytes + 6 * 32 + 7 * SIGNATURE_BYTES as u64)
        );

        observed.clear();
        let second = source
            .scan_ordered_with_registry_policy(&request, policy, &mut NoopSink)
            .unwrap();
        assert!(observed.reads_for(REGISTRY_FILE).is_empty());
        assert_eq!(second.io.source_read_calls, Some(2));
        assert_eq!(
            second.io.source_read_bytes,
            Some(fixture.compressed_bytes + 7 * SIGNATURE_BYTES as u64)
        );
    }

    #[test]
    fn sequential_small_partial_scan_keeps_the_sparse_registry_cache() {
        let signing_key = SigningKey::from_bytes(&[63; 32]);
        let signer = signing_key.verifying_key().to_bytes();
        let registry_entries = REGISTRY_KEYS_PER_CHUNK * (REGISTRY_CACHE_CHUNKS + 1);
        let mut registry = vec![[0_u8; REGISTRY_KEY_BYTES]; registry_entries];
        registry[0] = signer;
        registry[1] = TOKEN_PROGRAM;
        let blocks = (0..2)
            .map(|value| {
                vec![TxFixture::exact(
                    legacy_message(vec![raw_instruction(1, &[0], &[value])]),
                    metadata(2, None, Some(Vec::new()), vec![], vec![]),
                    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                )]
            })
            .collect();
        let fixture = Fixture::build(registry, blocks, None);
        let counted = CountingSource::new(LocalRangeSource::new(fixture.directory.path()));
        let observed = counted.clone();
        let reader = open_trusted_test_reader(counted);
        let selected_compressed_bytes = u64::from(reader.index().rows[0].compressed_len);
        observed.clear();
        let mut source = CompactV2InstructionSource::new(reader, FIRST_SLOT).unwrap();
        let request = ScanRequest::bounded(ScanRange {
            first_block: 0,
            block_count: NonZeroU32::new(1).unwrap(),
        })
        .allow_unverified_source()
        .with_instruction_data_for([TOKEN_PROGRAM]);
        struct NoopSink;
        impl BlockSink for NoopSink {
            fn visit_block(&mut self, _block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
                Ok(())
            }
        }

        let receipt = source
            .scan_ordered_with_registry_policy(
                &request,
                CompactV2RegistryReadPolicy::with_full_registry_limit(
                    DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES,
                ),
                &mut NoopSink,
            )
            .unwrap();

        assert!(source.context.shared_registry.is_none());
        assert_eq!(source.context.registry_chunks.len(), 1);
        assert_eq!(
            observed.reads_for(REGISTRY_FILE),
            vec![
                (0, registry_entries * REGISTRY_KEY_BYTES),
                (0, REGISTRY_KEYS_PER_CHUNK * REGISTRY_KEY_BYTES)
            ]
        );
        assert_eq!(receipt.io.source_read_calls, Some(3));
        assert_eq!(
            receipt.io.source_read_bytes,
            Some(
                selected_compressed_bytes
                    + u64::try_from(registry_entries * REGISTRY_KEY_BYTES).unwrap()
                    + u64::try_from(REGISTRY_KEYS_PER_CHUNK * REGISTRY_KEY_BYTES).unwrap()
            )
        );
    }

    #[test]
    fn sequential_dense_registry_prefetch_propagates_source_errors_before_publication() {
        let fixture = Fixture::main();
        let failing = FailingRegistrySource::new(LocalRangeSource::new(fixture.directory.path()));
        let control = failing.clone();
        let reader = open_trusted_test_reader(failing);
        control.arm();
        let mut source = CompactV2InstructionSource::new(reader, FIRST_SLOT).unwrap();
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data();
        struct CountingSink(usize);
        impl BlockSink for CountingSink {
            fn visit_block(&mut self, _block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
                self.0 += 1;
                Ok(())
            }
        }
        let mut sink = CountingSink(0);

        let error = source
            .scan_ordered_with_registry_policy(
                &request,
                CompactV2RegistryReadPolicy::with_full_registry_limit(
                    DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES,
                ),
                &mut sink,
            )
            .unwrap_err();

        assert!(
            format!("{error:?}").contains("injected registry prefetch failure"),
            "{error:?}"
        );
        assert_eq!(sink.0, 0);
        assert!(source.context.shared_registry.is_none());
    }

    #[test]
    fn parallel_borrowed_scan_matches_sequential_order_output_and_io() {
        const BLOCKS: usize = 8;
        let blocks = (0..BLOCKS)
            .map(|block| vec![TxFixture::raw_transaction(vec![block as u8])])
            .collect::<Vec<_>>();
        let signatures = (0..BLOCKS)
            .map(|block| [block as u8 + 1; SIGNATURE_BYTES])
            .collect::<Vec<_>>();
        let fixture = Fixture::build(Vec::new(), blocks, Some(signatures));
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data();

        let mut sequential =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let mut expected = Vec::<CanonicalBlock>::new();
        let sequential_receipt = sequential
            .for_each_block(&request, |block| {
                expected.push(CanonicalBlock {
                    counts: None,
                    header: block.header,
                    transactions: block.transactions.to_vec(),
                });
                Ok(())
            })
            .unwrap();

        let mut parallel =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let mut actual = Vec::<CanonicalBlock>::new();
        struct CollectSink<'a>(&'a mut Vec<CanonicalBlock>);
        impl BlockSink for CollectSink<'_> {
            fn visit_block(&mut self, block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
                self.0.push(CanonicalBlock {
                    counts: None,
                    header: block.header,
                    transactions: block.transactions.to_vec(),
                });
                Ok(())
            }
        }
        let mut sink = CollectSink(&mut actual);
        let parallel_receipt = parallel
            .scan_ordered_parallel(&request, &mut sink, CompactV2ParallelScanConfig::new(2))
            .unwrap();

        assert_eq!(actual, expected);
        assert_eq!(parallel_receipt.scan, sequential_receipt);
        assert_eq!(parallel_receipt.requested_workers, 2);
        assert!((1..=2).contains(&parallel_receipt.effective_workers));
        assert!((1..=2).contains(&parallel_receipt.max_active_workers));
        assert_eq!(parallel_receipt.compressed_buffer_count, 2);
        assert!(parallel_receipt.max_projected_block_bytes > 0);
        assert!(
            parallel_receipt.max_projected_batch_bytes
                >= parallel_receipt.max_projected_block_bytes
        );
        assert!(
            parallel_receipt.pipeline.max_transactions_per_batch
                <= crate::MAX_ORDERED_PARALLEL_TRANSACTIONS_PER_BATCH
        );
        assert!(
            parallel_receipt.pipeline.max_blocks_per_batch
                <= COMPACT_V2_PARALLEL_MAX_BLOCKS_PER_BATCH
        );
        assert!(
            parallel_receipt
                .pipeline
                .max_declared_uncompressed_batch_bytes
                <= COMPACT_V2_PARALLEL_UNCOMPRESSED_BATCH_BYTES as u64
        );
    }

    #[test]
    fn one_and_twelve_worker_scans_match_and_share_one_registry_prefetch() {
        const BLOCKS: usize = 24;
        let signing_key = SigningKey::from_bytes(&[61; 32]);
        let signer = signing_key.verifying_key().to_bytes();
        let blocks = (0..BLOCKS)
            .map(|block| {
                vec![TxFixture::exact(
                    legacy_message(vec![raw_instruction(1, &[0], &[block as u8])]),
                    metadata(2, None, Some(Vec::new()), vec![], vec![]),
                    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                )]
            })
            .collect::<Vec<_>>();
        let signatures = (0..BLOCKS)
            .map(|block| [block as u8 + 1; SIGNATURE_BYTES])
            .collect::<Vec<_>>();
        let fixture = Fixture::build(vec![signer, TOKEN_PROGRAM], blocks, Some(signatures));
        let request = ScanRequest::all()
            .allow_unverified_source()
            .without_instruction_data();

        fn run(
            fixture: &Fixture,
            request: &ScanRequest,
            workers: usize,
        ) -> (Vec<CanonicalBlock>, CompactV2ParallelScanReceipt) {
            struct CollectSink<'a>(&'a mut Vec<CanonicalBlock>);
            impl BlockSink for CollectSink<'_> {
                fn visit_block(
                    &mut self,
                    block: BlockView<'_>,
                ) -> blockzilla_query_sdk::Result<()> {
                    self.0.push(CanonicalBlock {
                        counts: None,
                        header: block.header,
                        transactions: block.transactions.to_vec(),
                    });
                    Ok(())
                }
            }

            let mut source =
                CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
            let mut blocks = Vec::new();
            let mut sink = CollectSink(&mut blocks);
            let receipt = source
                .scan_ordered_parallel(
                    request,
                    &mut sink,
                    CompactV2ParallelScanConfig::new(workers),
                )
                .unwrap();
            (blocks, receipt)
        }

        let (single_blocks, single) = run(&fixture, &request, 1);
        let (parallel_blocks, parallel) = run(&fixture, &request, 12);
        assert_eq!(parallel_blocks, single_blocks);
        assert_eq!(parallel.scan, single.scan);
        assert_eq!(parallel.pipeline.block_count, BLOCKS as u64);
        assert_eq!(parallel.requested_workers, 12);
        assert!((1..=12).contains(&parallel.effective_workers));
        assert!((1..=parallel.effective_workers).contains(&parallel.max_active_workers));

        for receipt in [single, parallel] {
            assert_eq!(
                receipt.registry.mode,
                CompactV2ParallelRegistryMode::SharedFull
            );
            assert_eq!(receipt.registry.prefetch_read_calls, 1);
            assert_eq!(receipt.registry.prefetch_read_bytes, 2 * 32);
            assert_eq!(receipt.registry.resident_bound_bytes, 2 * 32);
        }
    }

    #[test]
    fn shared_registry_prefetch_is_not_repeated_by_workers() {
        const BLOCKS: usize = 24;
        let signing_key = SigningKey::from_bytes(&[62; 32]);
        let signer = signing_key.verifying_key().to_bytes();
        let blocks = (0..BLOCKS)
            .map(|block| {
                vec![TxFixture::exact(
                    legacy_message(vec![raw_instruction(1, &[0], &[block as u8])]),
                    metadata(2, None, Some(Vec::new()), vec![], vec![]),
                    ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
                )]
            })
            .collect::<Vec<_>>();
        let fixture = Fixture::build(vec![signer, TOKEN_PROGRAM], blocks, None);
        let source = CountingSource::new(LocalRangeSource::new(fixture.directory.path()));
        let observed = source.clone();
        let reader = ArchiveReader::open_trusted(
            source,
            TrustedGenerationIdentity {
                cluster_id: "testnet".into(),
                epoch: EPOCH,
                generation_id: "compact-query-counting-fixture".into(),
                slots_per_epoch: SLOTS_PER_EPOCH,
            },
            OpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap();
        observed.clear();

        struct NoopSink;
        impl BlockSink for NoopSink {
            fn visit_block(&mut self, _block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
                Ok(())
            }
        }

        let mut query = CompactV2InstructionSource::new(reader, FIRST_SLOT).unwrap();
        let receipt = query
            .scan_ordered_parallel(
                &ScanRequest::all()
                    .allow_unverified_source()
                    .without_primary_signatures()
                    .without_instruction_data(),
                &mut NoopSink,
                CompactV2ParallelScanConfig::new(12),
            )
            .unwrap();

        assert_eq!(
            receipt.registry.mode,
            CompactV2ParallelRegistryMode::SharedFull
        );
        assert_eq!(receipt.registry.prefetch_read_calls, 1);
        assert_eq!(observed.reads_for(REGISTRY_FILE), vec![(0, 64)]);
    }

    #[test]
    fn large_partial_registry_policy_uses_the_one_million_transaction_threshold() {
        let config = CompactV2ParallelScanConfig::new(12);
        let registry_bytes = 889_551_808;
        let fields_without_pubkeys = ScanRequest::all()
            .without_instructions()
            .without_required_signers();

        assert!(!request_needs_registry(&fields_without_pubkeys));
        assert!(request_needs_registry(
            &fields_without_pubkeys.with_token_balances()
        ));

        assert!(should_prefetch_parallel_registry(
            config,
            false,
            COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
            true,
            registry_bytes,
        ));
        assert!(!should_prefetch_parallel_registry(
            config,
            false,
            COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS - 1,
            true,
            registry_bytes,
        ));
        assert!(should_prefetch_parallel_registry(
            config,
            true,
            1,
            true,
            registry_bytes,
        ));
        assert!(!should_prefetch_parallel_registry(
            config,
            false,
            COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
            false,
            registry_bytes,
        ));
        assert!(!should_prefetch_parallel_registry(
            config.with_full_registry_limit(registry_bytes - 1),
            false,
            COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
            true,
            registry_bytes,
        ));

        let fixture = Fixture::main();
        let reader = fixture.trusted_reader();
        let request = ScanRequest::all()
            .allow_unverified_source()
            .without_instruction_data();
        let (shared, receipt) = prepare_parallel_registry(
            &reader,
            0,
            1,
            &request,
            COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
            config,
        )
        .unwrap();
        assert!(shared.is_some());
        assert_eq!(receipt.mode, CompactV2ParallelRegistryMode::SharedFull);

        let (shared, receipt) = prepare_parallel_registry(
            &reader,
            0,
            1,
            &request,
            COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS - 1,
            config,
        )
        .unwrap();
        assert!(shared.is_none());
        assert_eq!(
            receipt.mode,
            CompactV2ParallelRegistryMode::SparseWorkerCache
        );
    }

    #[test]
    fn zero_full_registry_limit_uses_the_sparse_fallback() {
        let fixture = Fixture::main();
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        struct NoopSink;
        impl BlockSink for NoopSink {
            fn visit_block(&mut self, _block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
                Ok(())
            }
        }
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data();
        let receipt = source
            .scan_ordered_parallel(
                &request,
                &mut NoopSink,
                CompactV2ParallelScanConfig::new(2).with_full_registry_limit(0),
            )
            .unwrap();
        assert_eq!(
            receipt.registry.mode,
            CompactV2ParallelRegistryMode::SparseWorkerCache
        );
        assert_eq!(receipt.registry.prefetch_read_calls, 0);
        assert_eq!(
            receipt.registry.resident_bound_bytes,
            u64::try_from(receipt.effective_workers).unwrap()
                * u64::try_from(COMPACT_V2_QUERY_REGISTRY_RETAINED_KEY_BYTES).unwrap()
        );
    }

    #[test]
    fn parallel_scan_rejects_instruction_payload_requests() {
        let fixture = Fixture::main();
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        struct NoopSink;
        impl BlockSink for NoopSink {
            fn visit_block(&mut self, _block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
                Ok(())
            }
        }
        let mut sink = NoopSink;
        let error = source
            .scan_ordered_parallel(
                &ScanRequest::all(),
                &mut sink,
                CompactV2ParallelScanConfig::new(2),
            )
            .unwrap_err();
        assert!(
            format!("{error:?}").contains("InstructionDataRequirement::None"),
            "{error:?}"
        );
    }

    #[test]
    fn parallel_config_has_explicit_memory_bounds() {
        let config =
            compact_v2_parallel_reader_config(CompactV2ParallelScanConfig::new(12)).unwrap();
        assert_eq!(config.decode_workers, 12);
        assert_eq!(config.compressed_buffer_count, 3);
        assert_eq!(config.max_blocks_per_batch, 64);
        assert_eq!(config.uncompressed_batch_budget_bytes, 32 * 1024 * 1024);
        assert!(
            config.retained_decompressed_bytes_per_worker * config.decode_workers
                <= MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES
        );
        assert!(CompactV2ParallelScanConfig::default().workers >= 1);
        assert!(
            CompactV2ParallelScanConfig::default().workers <= MAX_ORDERED_PARALLEL_DECODE_WORKERS
        );
    }

    #[test]
    fn projection_scratch_reuses_small_buffers_and_releases_large_buffers() {
        let mut scratch = TransactionProjectionScratch::default();
        scratch.account_keys.reserve(32);
        scratch.account_keys.push([1; REGISTRY_KEY_BYTES]);
        let small_capacity = scratch.account_keys.capacity();
        scratch.finish_transaction();
        assert!(scratch.account_keys.is_empty());
        assert_eq!(scratch.account_keys.capacity(), small_capacity);

        let oversized = COMPACT_V2_PROJECTION_SCRATCH_RETAINED_BYTES
            / size_of::<[u8; REGISTRY_KEY_BYTES]>()
            + 1;
        scratch.account_keys.reserve(oversized);
        scratch.account_keys.push([2; REGISTRY_KEY_BYTES]);
        scratch.finish_transaction();
        assert!(scratch.account_keys.is_empty());
        assert_eq!(scratch.account_keys.capacity(), 0);
    }

    #[test]
    fn omitted_primary_signatures_skip_the_signature_plane_when_data_is_not_requested() {
        const BLOCKS: usize = 4;
        let blocks = (0..BLOCKS)
            .map(|block| vec![TxFixture::raw_transaction(vec![block as u8])])
            .collect::<Vec<_>>();
        let signatures = (0..BLOCKS)
            .map(|block| [block as u8 + 1; SIGNATURE_BYTES])
            .collect::<Vec<_>>();
        let fixture = Fixture::build(Vec::new(), blocks, Some(signatures));
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let request = ScanRequest::all()
            .allow_unverified_source()
            .allow_incomplete_instructions()
            .allow_incomplete_cpi()
            .allow_unknown_execution()
            .without_instruction_data()
            .without_primary_signatures();
        let mut observed = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                observed.push(block.transactions[0].primary_signature);
                Ok(())
            })
            .unwrap();

        assert_eq!(observed, vec![None; BLOCKS]);
        assert_eq!(receipt.blocks, BLOCKS as u64);
        assert_eq!(receipt.transactions, BLOCKS as u64);
        assert_eq!(receipt.io.source_read_calls, Some(1));
        assert_eq!(receipt.io.source_read_bytes, Some(fixture.compressed_bytes));
    }

    #[test]
    fn token_balance_only_scan_filters_mints_and_omits_other_message_planes() {
        let signer = SigningKey::from_bytes(&[52; 32]).verifying_key().to_bytes();
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
            vec![signer, TOKEN_PROGRAM, TARGET_MINT, TOKEN_OWNER, OTHER_MINT],
            vec![vec![TxFixture::exact(
                legacy_message(Vec::new()),
                exact_metadata,
                0,
            )]],
            Some(vec![[52; 64]]),
        );
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let request = ScanRequest::all()
            .without_instructions()
            .without_required_signers()
            .without_execution_status()
            .without_primary_signatures()
            .with_token_balances_for([TARGET_MINT]);
        let mut observed = None;
        let receipt = source
            .for_each_block(&request, |block| {
                let transaction = &block.transactions[0];
                observed = Some((
                    transaction.primary_signature,
                    transaction.required_signers.clone(),
                    transaction.header,
                    transaction.instructions.clone(),
                    transaction.token_balance_coverage,
                    transaction.token_balances.clone(),
                ));
                Ok(())
            })
            .unwrap();

        let (signature, signers, header, instructions, coverage, balances) = observed.unwrap();
        assert_eq!(signature, None);
        assert!(signers.is_empty());
        assert_eq!(
            header.status,
            ExecutionStatus::Unknown(CoverageReason::ProjectionNotRequested)
        );
        assert_eq!(
            header.instruction_coverage,
            InstructionCoverage::Unknown(CoverageReason::ProjectionNotRequested)
        );
        assert_eq!(
            header.cpi_coverage,
            CpiCoverage::Unknown(CoverageReason::ProjectionNotRequested)
        );
        assert!(instructions.is_empty());
        assert_eq!(coverage, TokenBalanceCoverage::Complete);
        assert_eq!(balances.len(), 2);
        assert_eq!(balances[0].side, TokenBalanceSide::Pre);
        assert_eq!(balances[0].balance_index, 1);
        assert_eq!(balances[0].account_index, 1);
        assert_eq!(balances[0].mint, Some(TARGET_MINT));
        assert_eq!(balances[0].owner, Some(TOKEN_OWNER));
        assert_eq!(balances[0].token_program, Some(TOKEN_PROGRAM));
        assert_eq!((balances[0].amount, balances[0].decimals), (22, 6));
        assert_eq!(balances[1].side, TokenBalanceSide::Post);
        assert_eq!(balances[1].balance_index, 0);
        assert_eq!((balances[1].amount, balances[1].decimals), (33, 6));
        assert_eq!(receipt.instructions, 0);
        assert_eq!(receipt.transactions_with_incomplete_token_balances, 0);
        assert_eq!(receipt.io.source_read_calls, Some(3));
        assert_eq!(
            receipt.io.source_read_bytes,
            Some(fixture.compressed_bytes + 2 * 5 * 32)
        );
    }

    #[test]
    fn selected_ambiguity_requires_and_uses_exact_signature_proof() {
        let (missing, _) = Fixture::ambiguous(false);
        let mut source =
            CompactV2InstructionSource::new(missing.trusted_reader(), FIRST_SLOT).unwrap();
        let error = source
            .for_each_block(&ScanRequest::all(), |_| Ok(()))
            .unwrap_err();
        assert!(error.to_string().contains("source error"));
        let source_text = format!("{error:?}");
        assert!(source_text.contains("signature sidecar proof"));

        let (missing, _) = Fixture::ambiguous(false);
        let mut source =
            CompactV2InstructionSource::new(missing.trusted_reader(), FIRST_SLOT).unwrap();
        let mut relaxed = None;
        let receipt = source
            .for_each_block(
                &ScanRequest::all().allow_incomplete_instruction_data(),
                |block| {
                    let instruction = &block.transactions[0].instructions[0];
                    relaxed = Some((instruction.data_coverage, instruction.data.clone()));
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            relaxed,
            Some((
                InstructionDataCoverage::Unknown(CoverageReason::InstructionDataUnavailable),
                Vec::new()
            ))
        );
        assert_eq!(receipt.instructions_with_unknown_data, 1);

        let (proved, selected_data) = Fixture::ambiguous(true);
        let expected_signature = proved.signatures[0];
        let mut source =
            CompactV2InstructionSource::new(proved.trusted_reader(), FIRST_SLOT).unwrap();
        let mut observed = None;
        source
            .for_each_block(&ScanRequest::all(), |block| {
                observed = Some((
                    block.transactions[0].instructions[0].data.clone(),
                    block.transactions[0].primary_signature,
                ));
                Ok(())
            })
            .unwrap();
        assert_eq!(observed, Some((selected_data, Some(expected_signature))));

        let (proved, selected_data) = Fixture::ambiguous(true);
        let mut source =
            CompactV2InstructionSource::new(proved.trusted_reader(), FIRST_SLOT).unwrap();
        let mut observed = None;
        let receipt = source
            .for_each_block(&ScanRequest::all().without_primary_signatures(), |block| {
                observed = Some((
                    block.transactions[0].instructions[0].data.clone(),
                    block.transactions[0].primary_signature,
                ));
                Ok(())
            })
            .unwrap();
        assert_eq!(observed, Some((selected_data, None)));
        assert!(
            receipt
                .io
                .source_read_bytes
                .is_some_and(|bytes| bytes >= SIGNATURE_BYTES as u64),
            "selected instruction data must retain its signature proof read"
        );
    }

    #[test]
    fn relaxed_candidate_limit_reports_ambiguity_and_keeps_scanning() {
        let signer = [71; 32];
        let instructions = (0..14)
            .map(|_| ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: Vec::new(),
                data: vote_tower_data(true),
            })
            .collect();
        let transaction = TxFixture::exact(
            legacy_message(instructions),
            metadata(2, None, Some(Vec::new()), vec![], vec![]),
            ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        let fixture = Fixture::build(
            vec![signer, VOTE_PROGRAM],
            vec![vec![transaction]],
            Some(vec![[0; 64]]),
        );
        fs::write(
            fixture
                .directory
                .path()
                .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
            vote_hash_registry_bytes(),
        )
        .unwrap();

        let mut strict =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        assert!(
            strict
                .for_each_block(&ScanRequest::all(), |_| Ok(()))
                .is_err()
        );

        let mut relaxed =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let mut coverages = Vec::new();
        let receipt = relaxed
            .for_each_block(
                &ScanRequest::all().allow_incomplete_instruction_data(),
                |block| {
                    coverages.extend(
                        block.transactions[0]
                            .instructions
                            .iter()
                            .map(|instruction| instruction.data_coverage),
                    );
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(coverages.len(), 14);
        assert!(coverages.iter().all(|coverage| {
            *coverage == InstructionDataCoverage::Unknown(CoverageReason::AmbiguousInstructionData)
        }));
        assert_eq!(receipt.instructions_with_unknown_data, 14);
    }

    #[test]
    fn relaxed_mode_rejects_present_vote_registry_without_referenced_hash() {
        let transaction = TxFixture::exact(
            legacy_message(vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: Vec::new(),
                data: vote_tower_data(true),
            }]),
            metadata(2, None, Some(Vec::new()), vec![], vec![]),
            ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        let fixture = Fixture::build(vec![[72; 32], VOTE_PROGRAM], vec![vec![transaction]], None);
        fs::write(
            fixture
                .directory
                .path()
                .join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE),
            [],
        )
        .unwrap();

        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        assert!(
            source
                .for_each_block(
                    &ScanRequest::all().allow_incomplete_instruction_data(),
                    |_| Ok(()),
                )
                .is_err(),
            "a present registry with no referenced row is a hard source error"
        );
    }

    #[test]
    fn trusted_reader_admits_optional_sidecars_for_exact_vote_proof() {
        let (fixture, selected_data) = Fixture::ambiguous_with_trusted_vote_sidecar();
        let reader = fixture.trusted_reader();
        for name in [
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
            ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
            ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
        ] {
            assert!(reader.file_size(name).is_some(), "missing {name}");
        }
        let mut source = CompactV2InstructionSource::new(reader, FIRST_SLOT).unwrap();
        let mut observed = None;
        let receipt = source
            .for_each_block(&ScanRequest::all(), |block| {
                observed = Some(block.transactions[0].instructions[0].data.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(observed, Some(selected_data));
        assert_eq!(receipt.io.source_read_calls, Some(4));
        assert_eq!(
            receipt.io.source_read_bytes.map(|bytes| bytes > 0),
            Some(true)
        );
    }

    #[test]
    fn boundary_registry_resolves_id_zero_without_a_previous_tail() {
        let fixture = Fixture::build(vec![[72; 32]], vec![Vec::new()], None);
        let boundary = [81; 32];
        let first_block = [82; 32];
        fs::write(
            fixture
                .directory
                .path()
                .join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            [boundary, first_block].concat(),
        )
        .unwrap();
        assert!(
            !fixture
                .directory
                .path()
                .join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
                .exists()
        );

        let reader = fixture.trusted_reader();
        let mut context = ExactContext::default();
        let resolver = context.load_blockhashes(&reader).unwrap();
        assert_eq!(resolver.resolve(0).unwrap(), boundary);
        assert_eq!(resolver.resolve_header_previous(1, 0).unwrap(), boundary);
        assert_eq!(resolver.resolve(1).unwrap(), first_block);
    }

    #[test]
    fn rejects_raw_metadata_fallback_without_metadata_presence() {
        let fixture = Fixture::invalid_raw_metadata_flag();
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let error = source
            .for_each_block(
                &ScanRequest::all()
                    .allow_incomplete_instructions()
                    .allow_incomplete_cpi()
                    .allow_unknown_execution(),
                |_| Ok(()),
            )
            .unwrap_err();
        assert!(format!("{error:?}").contains("METADATA_RAW_FALLBACK without HAS_METADATA"));
    }

    #[test]
    fn bounded_range_and_sink_stop_keep_publication_order() {
        let fixture = Fixture::main();
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let request = ScanRequest::bounded(ScanRange {
            first_block: 1,
            block_count: NonZeroU32::new(1).unwrap(),
        })
        .allow_incomplete_instructions()
        .allow_incomplete_cpi()
        .allow_unknown_execution()
        .with_instruction_data_for([TOKEN_PROGRAM]);
        let mut ordinals = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                ordinals.push(block.header.block_ordinal);
                Ok(())
            })
            .unwrap();
        assert_eq!(ordinals, [1]);
        assert_eq!(receipt.blocks, 1);

        struct StopSink {
            visits: usize,
        }
        impl BlockSink for StopSink {
            fn visit_block(&mut self, _block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
                self.visits += 1;
                Err(QueryError::sink(std::io::Error::other("stop")))
            }
        }
        let mut source =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        let mut sink = StopSink { visits: 0 };
        assert!(source.scan_ordered(&request, &mut sink).is_err());
        assert_eq!(sink.visits, 1);
    }

    #[test]
    fn source_identity_preserves_operator_trust_and_rejects_published_manifest() {
        let fixture = Fixture::main();
        let trusted =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        assert_eq!(
            trusted.identity().verification,
            SourceVerification::OperatorTrusted
        );
        assert_eq!(
            trusted.identity().binding.as_deref(),
            Some("operator-trusted-candidate-id=compact-query-fixture")
        );
        let reopened =
            CompactV2InstructionSource::new(fixture.trusted_reader(), FIRST_SLOT).unwrap();
        assert_eq!(trusted.identity().binding, reopened.identity().binding);

        let replacement = CompactV2InstructionSource::new(
            fixture.trusted_reader_with_candidate("compact-query-fixture-r2"),
            FIRST_SLOT,
        )
        .unwrap();
        assert_ne!(trusted.identity().binding, replacement.identity().binding);

        let error =
            CompactV2InstructionSource::new(fixture.published_reader(), FIRST_SLOT).unwrap_err();
        assert!(format!("{error}").contains(
            "publication-manifest readers are retired; reopen the archive as a pinned local or strong-ETag object set"
        ));
    }

    #[test]
    fn registry_chunk_cache_caps_retained_key_payload_and_evicts() {
        assert_eq!(COMPACT_V2_QUERY_REGISTRY_RETAINED_KEY_BYTES, 512 * 1024);
        let fixture = Fixture::empty_with_large_registry();
        let reader = fixture.trusted_reader();
        let mut context = ExactContext::default();
        for chunk in 0..=REGISTRY_CACHE_CHUNKS {
            let id = u32::try_from(chunk * REGISTRY_KEYS_PER_CHUNK + 1).unwrap();
            context
                .resolve_pubkey(&reader, CompactPubkey::Id(id))
                .unwrap();
        }
        assert_eq!(context.registry_chunks.len(), REGISTRY_CACHE_CHUNKS);
        assert!(!context.registry_chunks.contains_key(&0));
        let retained = context
            .registry_chunks
            .values()
            .map(|chunk| chunk.len() * REGISTRY_KEY_BYTES)
            .sum::<usize>();
        assert!(retained <= COMPACT_V2_QUERY_REGISTRY_RETAINED_KEY_BYTES);
    }
}
