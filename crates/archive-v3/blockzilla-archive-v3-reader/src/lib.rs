//! The source-neutral engine and targeted reader for one Blockzilla Archive V3 epoch.
//!
//! [`IndexerV3Archive::open`] derives the V3 Worker route, pins the complete
//! reader object set with exact lengths and strong ETags, prepares a bounded
//! persistent cache, and opens the source-neutral instruction reader.
//! [`IndexerV3Archive::open_local`] opens the same flat object directory from
//! a local copy of the public archive tree. The advanced
//! [`IndexerV3Archive::open_local_split`] entry point keeps support for
//! operator layouts that store ledger files and retained sidecars separately.

use std::{
    collections::BTreeSet,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[path = "engine/standalone_v2.rs"]
#[allow(dead_code)]
mod indexer_v3_wire;
// The frozen posting codec refers to its sibling by the original wire module name.
use indexer_v3_wire as standalone_v2;

#[path = "engine/standalone_account_postings.rs"]
#[allow(dead_code)]
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

pub use blockzilla_model::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveInstructionSourceExt, ArchiveIoSnapshot,
    BlockSink, BlockView, Error as QueryError, FnBlockSink, InstructionDataRequirement,
    RecordedTokenBalance, Result as QueryResult, ScanIoReceipt, ScanRange, ScanReceipt,
    ScanRequest, SourceIdentity, SourceVerification, TokenBalanceCoverage, TokenBalanceRequirement,
    TokenBalanceSide, TransactionView,
};
use blockzilla_source::{RangeSource, SourceError, SourceResult};
use blockzilla_source_cache::{CachedHttpRangeSource, create_http_cache_directory};
use blockzilla_source_http::{
    HttpObjectIdentity, HttpObjectPathLayout, HttpRangeSource, HttpRangeSourceOptions,
};
use blockzilla_source_local::PinnedLocalRangeSource;
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

const OBJECT_SET_BINDING_DOMAIN: &[u8] = b"blockzilla.indexer-v3.object-set.v1\0";
const CACHE_NAMESPACE_DOMAIN: &[u8] = b"blockzilla.indexer-v3.cache-namespace.v1\0";
const MAINNET_ARCHIVE_SLOTS_PER_EPOCH: u64 = 432_000;
/// Default memory limit for automatic dense-query registry loading.
pub const DEFAULT_INDEXER_V3_FULL_REGISTRY_BYTES: u64 = 1 << 30;
const TRANSACTION_DIRECTORY_OBJECT: &str = "archive-v2-standalone-transaction-directory.wincode";
const REGISTRY_INDEX_OBJECT: &str = "registry.mphf";
const REVERSE_OPTIONAL_OBJECTS: [&str; 4] = [
    REGISTRY_INDEX_OBJECT,
    ADAPTIVE_V3_PAGES_FILE,
    ADAPTIVE_V3_CONTROL_FILE,
    ADAPTIVE_V3_COVERAGE_FILE,
];

const LOCAL_SPLIT_REQUIRED_ADAPTIVE_OBJECTS: [&str; 3] = [
    ADAPTIVE_V3_PAGES_FILE,
    ADAPTIVE_V3_CONTROL_FILE,
    ADAPTIVE_V3_COVERAGE_FILE,
];
const LOCAL_SPLIT_REQUIRED_RETAINED_OBJECTS: [&str; 2] = [
    INDEXER_V3_REQUIRED_RETAINED_SIDECARS[0],
    REGISTRY_INDEX_OBJECT,
];

/// Logical CPUs available to one V3 application process, or one worker when
/// the operating system does not report this value.
pub fn default_worker_count() -> NonZeroUsize {
    let detected = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
    NonZeroUsize::new(detected.get().min(MAX_INDEXER_V3_PARALLEL_WORKERS))
        .expect("the V3 parallel worker limit is nonzero")
}

/// Persistent-cache layout for the expected read pattern.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexerV3CacheProfile {
    /// Cache the block index and complete transaction directory for an ordered
    /// scan. Cold setup can be large; later full scans use local directory
    /// reads.
    #[default]
    Sequential,
    /// Cache only the block index and small reverse control objects. Candidate
    /// transaction-directory rows and semantic planes stay as bounded reads.
    Selective,
}

/// Network transport options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexerV3OpenOptions {
    /// Permit cleartext HTTP only for a controlled local test server.
    pub allow_insecure_http: bool,
    /// Choose the persistent objects that match the intended workload.
    pub cache_profile: IndexerV3CacheProfile,
}

/// Errors from Indexer V3 network setup and range selection.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid Worker origin: {0}")]
    InvalidOrigin(String),
    #[error("invalid cache root: {0}")]
    InvalidCacheRoot(String),
    #[error("invalid archive range: {0}")]
    InvalidRange(String),
    #[error("Indexer V3 source geometry is invalid: {0}")]
    Geometry(String),
    #[error("Indexer V3 range source failed")]
    RangeSource(#[source] SourceError),
    #[error("Indexer V3 reader failed")]
    IndexerV3(#[source] IndexerV3InstructionSourceError),
    #[error("Indexer V3 reverse index is unavailable: {0}")]
    ReverseUnavailable(String),
    #[error("Indexer V3 reverse lookup failed")]
    Reverse(#[source] anyhow::Error),
    #[error("Indexer V3 selective scan failed")]
    Query(#[source] QueryError),
    #[error("Indexer V3 size arithmetic overflow")]
    SizeOverflow,
}

pub type Result<T> = std::result::Result<T, Error>;

/// Evidence from one public-key lookup and its sparse candidate scan.
///
/// The callback receives candidate blocks, including sound fallback blocks.
/// The application must confirm the exact program or signer in each decoded
/// transaction before it emits a final match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexerV3TargetedScanReceipt {
    /// Full-epoch reverse-index evidence and sorted candidate block IDs.
    pub candidates: IndexerV3CandidateBlocks,
    /// Requested-universe, candidate, skipped, decode, and source-I/O counts.
    pub scan: IndexerV3SelectiveScanReceipt,
    /// Network and persistent-cache work from registry lookup through scan.
    pub transport_io: ArchiveIoSnapshot,
    /// V3 transport work with local reads kept separate from network bytes.
    pub transport: IndexerV3TransportReceipt,
}

/// Transport used by one ready V3 archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexerV3TransportKind {
    /// HTTPS range reads with the existing persistent HTTP cache.
    HttpCached,
    /// Two explicit, anchored local roots with no persistent cache.
    LocalSplit,
}

/// V3 transport counters that do not classify local file reads as network I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexerV3TransportReceipt {
    pub kind: IndexerV3TransportKind,
    /// Existing normalized HTTP and persistent-cache counters.
    pub http_and_cache: ArchiveIoSnapshot,
    /// Local source range reads. These fields are zero for HTTPS archives.
    pub local_read_calls: u64,
    pub local_read_bytes: u64,
}

impl IndexerV3TransportReceipt {
    fn saturating_sub(self, earlier: Self) -> Self {
        debug_assert_eq!(self.kind, earlier.kind);
        Self {
            kind: self.kind,
            http_and_cache: self.http_and_cache.saturating_sub(earlier.http_and_cache),
            local_read_calls: self
                .local_read_calls
                .saturating_sub(earlier.local_read_calls),
            local_read_bytes: self
                .local_read_bytes
                .saturating_sub(earlier.local_read_bytes),
        }
    }
}

/// One object in the exact network candidate set.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundObject {
    name: String,
    url: String,
    identity: Option<HttpObjectIdentity>,
}

/// A ready-to-scan Indexer V3 archive.
pub struct IndexerV3Archive {
    source: IndexerV3InstructionSource,
    cache: Option<CachedHttpRangeSource>,
    local_split: Option<Arc<LocalSplitRangeSource>>,
    shared_source: Arc<dyn RangeSource>,
    reverse_available: bool,
    registry_index: Option<IndexerV3RegistryIndex>,
    adaptive_reader: Option<AdaptiveV3Reader>,
    cache_profile: IndexerV3CacheProfile,
    cache_root: PathBuf,
    bound_source_size_bytes: u64,
    cached_source_size_bytes: u64,
    transport_kind: IndexerV3TransportKind,
    registry_read_policy: IndexerV3RegistryReadPolicy,
}

/// Source-neutral ordered reader backed by the V3 parallel projection path.
///
/// The adapter keeps [`ArchiveInstructionSource::identity`] and
/// [`ArchiveInstructionSource::scan_ordered`] bound to the same archive. This
/// lets common application code use V3 parallel projection without supplying
/// an identity separately from its scanner.
pub struct IndexerV3ParallelInstructionSource<'a> {
    archive: &'a mut IndexerV3Archive,
    workers: NonZeroUsize,
    last_receipt: Option<IndexerV3ParallelScanReceipt>,
}

impl IndexerV3ParallelInstructionSource<'_> {
    pub const fn workers(&self) -> NonZeroUsize {
        self.workers
    }

    /// Return the V3 details from the last successful ordered scan.
    pub const fn last_receipt(&self) -> Option<&IndexerV3ParallelScanReceipt> {
        self.last_receipt.as_ref()
    }

    /// Take the V3 details from the last successful ordered scan.
    pub fn take_last_receipt(&mut self) -> Option<IndexerV3ParallelScanReceipt> {
        self.last_receipt.take()
    }
}

impl ArchiveInstructionSource for IndexerV3ParallelInstructionSource<'_> {
    fn identity(&self) -> &SourceIdentity {
        self.archive.source.identity()
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> QueryResult<ScanReceipt> {
        self.last_receipt = None;
        let receipt = self
            .archive
            .scan_ordered_parallel(request, self.workers, sink)?;
        let scan = receipt.scan;
        self.last_receipt = Some(receipt);
        Ok(scan)
    }
}

impl std::fmt::Debug for IndexerV3Archive {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexerV3Archive")
            .field("identity", self.source.identity())
            .field("cache_root", &self.cache_root)
            .field("cache_profile", &self.cache_profile)
            .field("transport_kind", &self.transport_kind)
            .field("registry_read_policy", &self.registry_read_policy)
            .field("reverse_available", &self.reverse_available)
            .field("bound_source_size_bytes", &self.bound_source_size_bytes)
            .field("cached_source_size_bytes", &self.cached_source_size_bytes)
            .finish_non_exhaustive()
    }
}

impl IndexerV3Archive {
    /// Open one HTTPS mainnet-beta epoch from a Worker origin.
    ///
    /// The caller supplies only the Worker origin. This method derives the
    /// `/indexer-v3/` route and the fixed archive-window first slot.
    pub fn open(origin: &str, epoch: u64, cache_root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(origin, epoch, cache_root, IndexerV3OpenOptions::default())
    }

    /// Open one epoch for an adaptive reverse lookup and sparse block scan.
    ///
    /// This profile does not download the complete transaction directory.
    pub fn open_selective(origin: &str, epoch: u64, cache_root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(
            origin,
            epoch,
            cache_root,
            IndexerV3OpenOptions {
                cache_profile: IndexerV3CacheProfile::Selective,
                ..IndexerV3OpenOptions::default()
            },
        )
    }

    /// Open one epoch from a local copy of the public archive tree.
    ///
    /// `archive_root` contains `indexer-v3/<epoch>/`. All V3 ledger,
    /// reverse-index, and retained objects are flat in that epoch directory.
    /// The V3 object header selects the encoded message and metadata schema;
    /// the caller does not supply a schema option.
    pub fn open_local(archive_root: impl AsRef<Path>, epoch: u64) -> Result<Self> {
        let epoch_root = local_epoch_root(archive_root.as_ref(), epoch);
        Self::open_local_split(
            &epoch_root,
            &epoch_root,
            epoch,
            format!("local-flat-epoch-{epoch}"),
        )
    }

    /// Open one operator-trusted V3 candidate from two anchored local roots.
    ///
    /// The ledger root supplies the internally bound V3 ledger and adaptive
    /// posting objects. The retained root supplies the public-key registry,
    /// its MPHF, signatures, and the other exact retained sidecars. This path
    /// does not create a cache and does not claim a manifest, object-set,
    /// seal, or publication binding.
    pub fn open_local_split(
        ledger_root: impl AsRef<Path>,
        retained_sidecar_root: impl AsRef<Path>,
        epoch: u64,
        candidate_id: impl Into<String>,
    ) -> Result<Self> {
        let ledger_root = ledger_root.as_ref();
        let retained_sidecar_root = retained_sidecar_root.as_ref();
        let local_split = Arc::new(LocalSplitRangeSource::open(
            ledger_root,
            retained_sidecar_root,
        )?);
        let bound_source_size_bytes = local_split.bound_source_size_bytes();
        let first_slot = archive_first_slot(epoch)?;
        let candidate_id = candidate_id.into();
        let label = format!(
            "local-split:ledger={};retained={}",
            ledger_root.display(),
            retained_sidecar_root.display()
        );
        let shared_source: Arc<dyn RangeSource> = local_split.clone();
        let source = IndexerV3InstructionSource::open_operator_trusted_source(
            Arc::clone(&shared_source),
            label,
            first_slot,
            candidate_id,
        )
        .map_err(Error::IndexerV3)?;
        validate_identity(&source, epoch, SourceVerification::OperatorTrusted)?;

        Ok(Self {
            source,
            cache: None,
            local_split: Some(local_split),
            shared_source,
            reverse_available: true,
            registry_index: None,
            adaptive_reader: None,
            cache_profile: IndexerV3CacheProfile::Selective,
            cache_root: PathBuf::new(),
            bound_source_size_bytes,
            cached_source_size_bytes: 0,
            transport_kind: IndexerV3TransportKind::LocalSplit,
            registry_read_policy: IndexerV3RegistryReadPolicy::with_full_registry_limit(
                DEFAULT_INDEXER_V3_FULL_REGISTRY_BYTES,
            ),
        })
    }

    /// Open with an explicit local-test transport policy.
    pub fn open_with_options(
        origin: &str,
        epoch: u64,
        cache_root: impl AsRef<Path>,
        options: IndexerV3OpenOptions,
    ) -> Result<Self> {
        let endpoint = WorkerEndpoint::parse(origin, options.allow_insecure_http)?;
        let cache_root = cache_root.as_ref().to_path_buf();
        if cache_root.as_os_str().is_empty() {
            return Err(Error::InvalidCacheRoot("path is empty".into()));
        }
        create_http_cache_directory(&cache_root).map_err(Error::RangeSource)?;

        let http = HttpRangeSource::with_options(
            endpoint.v3_base.as_str(),
            epoch,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: options.allow_insecure_http,
                object_path_layout: HttpObjectPathLayout::FlatEpoch,
                ..HttpRangeSourceOptions::default()
            },
        )
        .map_err(Error::RangeSource)?;

        let required_ledger = indexer_v3_required_ledger_objects().collect::<Vec<_>>();
        let index_name = required_ledger.first().copied().ok_or_else(|| {
            Error::Geometry("the V3 reader did not declare its block index".into())
        })?;
        let transaction_directory_name = required_ledger.get(1).copied().ok_or_else(|| {
            Error::Geometry("the V3 reader did not declare its transaction directory".into())
        })?;
        if transaction_directory_name != TRANSACTION_DIRECTORY_OBJECT {
            return Err(Error::Geometry(format!(
                "the second V3 ledger object is {transaction_directory_name}, expected {TRANSACTION_DIRECTORY_OBJECT}"
            )));
        }
        let required_names = required_ledger
            .iter()
            .copied()
            .chain(INDEXER_V3_REQUIRED_RETAINED_SIDECARS)
            .collect::<BTreeSet<_>>();
        let optional_names = INDEXER_V3_OPTIONAL_RETAINED_SIDECARS
            .into_iter()
            .chain(REVERSE_OPTIONAL_OBJECTS)
            .filter(|name| !required_names.contains(name))
            .collect::<BTreeSet<_>>();

        let mut objects = Vec::with_capacity(required_names.len() + optional_names.len());
        for name in required_names {
            objects.push(bind_required_object(&http, epoch, name)?);
        }
        for name in optional_names {
            objects.push(bind_optional_object(&http, epoch, name)?);
        }
        let binding = object_set_binding(epoch, &objects);
        let bound_source_size_bytes = objects.iter().try_fold(0_u64, |total, object| {
            let length = object
                .identity
                .as_ref()
                .map_or(0, |identity| identity.length);
            total.checked_add(length).ok_or(Error::SizeOverflow)
        })?;
        let reverse_available = REVERSE_OPTIONAL_OBJECTS
            .iter()
            .all(|name| object_is_present(&objects, name));
        if options.cache_profile == IndexerV3CacheProfile::Selective && !reverse_available {
            return Err(Error::ReverseUnavailable(missing_objects(
                &objects,
                &REVERSE_OPTIONAL_OBJECTS,
            )));
        }
        let cached_objects = match options.cache_profile {
            IndexerV3CacheProfile::Sequential => {
                vec![index_name, transaction_directory_name]
            }
            IndexerV3CacheProfile::Selective => vec![
                index_name,
                REGISTRY_INDEX_OBJECT,
                ADAPTIVE_V3_CONTROL_FILE,
                ADAPTIVE_V3_COVERAGE_FILE,
            ],
        };
        let cached_source_size_bytes = selected_object_size(&objects, &cached_objects)?;

        let candidate_cache = cache_root
            .join(cache_namespace(&endpoint.normalized_origin))
            .join("indexer-v3")
            .join(format!("epoch-{epoch}"))
            .join(&binding);
        create_http_cache_directory(&candidate_cache).map_err(Error::RangeSource)?;
        let cache = CachedHttpRangeSource::open(http, &candidate_cache, &cached_objects)
            .map_err(Error::RangeSource)?;

        let first_slot = archive_first_slot(epoch)?;
        let shared: Arc<dyn RangeSource> = Arc::new(cache.clone());
        let source = IndexerV3InstructionSource::open_object_set_bound_source(
            Arc::clone(&shared),
            endpoint.v3_base.to_string(),
            first_slot,
            binding,
        )
        .map_err(Error::IndexerV3)?;
        validate_identity(&source, epoch, SourceVerification::ObjectSetBound)?;

        Ok(Self {
            source,
            cache: Some(cache),
            local_split: None,
            shared_source: shared,
            reverse_available,
            registry_index: None,
            adaptive_reader: None,
            cache_profile: options.cache_profile,
            cache_root: candidate_cache,
            bound_source_size_bytes,
            cached_source_size_bytes,
            transport_kind: IndexerV3TransportKind::HttpCached,
            registry_read_policy: IndexerV3RegistryReadPolicy::with_full_registry_limit(
                DEFAULT_INDEXER_V3_FULL_REGISTRY_BYTES,
            ),
        })
    }

    /// Select a nonempty range, capped by the available V3 block rows.
    pub fn bounded_range(&self, first_block: u32, max_blocks: NonZeroU32) -> Result<ScanRange> {
        bounded_range(self.source.identity().block_count, first_block, max_blocks)
    }

    /// Exact byte size of all present objects in the bound reader set.
    ///
    /// This is not the size of optional files outside this reader set.
    pub const fn bound_source_size_bytes(&self) -> u64 {
        self.bound_source_size_bytes
    }

    /// Exact payload bytes in the selected persistent cache profile.
    ///
    /// The cache validates the runtime object lengths against its 8 GiB
    /// per-object and 16 GiB aggregate disk limits before a body download.
    pub const fn cached_source_size_bytes(&self) -> u64 {
        self.cached_source_size_bytes
    }

    /// Cache layout selected when this archive was opened.
    pub const fn cache_profile(&self) -> IndexerV3CacheProfile {
        self.cache_profile
    }

    /// Transport selected when this archive was opened.
    pub const fn transport_kind(&self) -> IndexerV3TransportKind {
        self.transport_kind
    }

    /// Automatic registry policy used by targeted selective scans.
    ///
    /// The default permits one complete registry of at most 1 GiB to become
    /// resident only for a dense query. Sparse queries keep the bounded chunk
    /// cache. Set a zero-byte limit to disable complete-registry loading.
    pub const fn registry_read_policy(&self) -> IndexerV3RegistryReadPolicy {
        self.registry_read_policy
    }

    /// Set the complete-registry memory limit for later targeted scans.
    ///
    /// The SDK still applies its fixed density and transaction-count gates.
    /// The SDK immediately releases an earlier complete registry image when
    /// that image is larger than the new limit. A zero-byte limit therefore
    /// forces later scans to use the sparse chunk cache.
    pub fn set_full_registry_limit(&mut self, max_full_registry_bytes: u64) {
        self.registry_read_policy =
            IndexerV3RegistryReadPolicy::with_full_registry_limit(max_full_registry_bytes);
        self.source
            .release_full_registry_above(max_full_registry_bytes);
    }

    /// Release a complete registry image retained by an earlier dense scan.
    pub fn release_full_registry(&mut self) -> bool {
        self.source.release_full_registry()
    }

    /// Bind this archive to its ordered parallel projection path.
    pub fn parallel_instruction_source(
        &mut self,
        workers: NonZeroUsize,
    ) -> QueryResult<IndexerV3ParallelInstructionSource<'_>> {
        if workers.get() > MAX_INDEXER_V3_PARALLEL_WORKERS {
            return Err(QueryError::InvalidRequest(format!(
                "V3 parallel worker count {} exceeds {MAX_INDEXER_V3_PARALLEL_WORKERS}",
                workers.get()
            )));
        }
        Ok(IndexerV3ParallelInstructionSource {
            archive: self,
            workers,
            last_receipt: None,
        })
    }

    /// True when the bound object set has every object needed for adaptive
    /// account postings and bounded registry lookup.
    pub const fn reverse_index_available(&self) -> bool {
        self.reverse_available
    }

    /// Resolve one public key through the bounded MPHF and verify its exact
    /// `registry.bin` row.
    pub fn resolve_pubkey_id(&mut self, pubkey: &[u8; 32]) -> Result<Option<u32>> {
        self.registry_index()?
            .lookup(pubkey)
            .map_err(Error::Reverse)
    }

    /// Scan an ordered range with bounded parallel projection and stable
    /// ledger-order publication.
    ///
    /// One dense registry image can be shared by all workers. Each worker has
    /// its own projection scratch and sparse registry cache. The ordered merge
    /// has one global bound: requested workers multiplied by
    /// [`INDEXER_V3_PARALLEL_BUFFERED_BLOCKS_PER_WORKER`] projected blocks.
    /// Separate declared decoded-byte and transaction budgets include jobs
    /// that execute, wait in the result channel, or wait in the coordinator.
    /// These are structural admission bounds, not hard bounds on the expanded
    /// owned canonical projection. The receipt reports the exact observed
    /// owned-payload high-water marks. One valid oversized block runs alone.
    /// Exact selected instruction data is supported. Workers read bounded
    /// signature windows when exact candidate selection needs proof.
    pub fn scan_ordered_parallel(
        &mut self,
        request: &ScanRequest,
        workers: NonZeroUsize,
        sink: &mut dyn BlockSink,
    ) -> QueryResult<IndexerV3ParallelScanReceipt> {
        self.source.scan_ordered_parallel_with_registry_policy(
            request,
            self.registry_read_policy,
            workers,
            sink,
        )
    }

    /// Build sound candidates for one public key and decode only their blocks.
    ///
    /// A key that is absent from `registry.bin` can still select fallback
    /// blocks when the adaptive coverage lane is incomplete. This method keeps
    /// candidate construction and sparse decoding bound to this archive.
    pub fn scan_target_candidates(
        &mut self,
        pubkey: &[u8; 32],
        policy: IndexerV3CandidatePolicy,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> Result<IndexerV3TargetedScanReceipt> {
        let io_before = self.io_snapshot();
        let transport_before = self.transport_snapshot();
        let id = match self
            .source
            .filter_key_id(request, pubkey)
            .map_err(Error::Query)?
        {
            Some(id) => id,
            None => self.resolve_pubkey_id(pubkey)?,
        };
        let key = match id {
            Some(account_id) => IndexerV3CandidateKey::RegistryId(account_id),
            None => IndexerV3CandidateKey::RegistryAbsent,
        };
        let candidates =
            build_indexer_v3_candidate_blocks_for_key(self.adaptive_reader()?, key, policy)
                .map_err(Error::Reverse)?;
        self.validate_candidate_geometry(&candidates)?;
        let selected = candidates_in_request(&candidates.block_ids, request)?;
        let scan = self
            .source
            .scan_selected_blocks_with_registry_policy(
                request,
                selected,
                self.registry_read_policy,
                sink,
            )
            .map_err(Error::Query)?;
        let transport_io = self.io_snapshot().saturating_sub(io_before);
        let transport = self.transport_snapshot().saturating_sub(transport_before);
        Ok(IndexerV3TargetedScanReceipt {
            candidates,
            scan,
            transport_io,
            transport,
        })
    }

    /// Build sound candidates for one public key and project candidate blocks
    /// on bounded workers. Output remains strictly ordered and byte-stable.
    pub fn scan_target_candidates_parallel(
        &mut self,
        pubkey: &[u8; 32],
        policy: IndexerV3CandidatePolicy,
        request: &ScanRequest,
        workers: NonZeroUsize,
        sink: &mut dyn BlockSink,
    ) -> Result<IndexerV3TargetedScanReceipt> {
        validate_worker_count(workers)?;
        let io_before = self.io_snapshot();
        let transport_before = self.transport_snapshot();
        let id = match self
            .source
            .filter_key_id(request, pubkey)
            .map_err(Error::Query)?
        {
            Some(id) => id,
            None => self.resolve_pubkey_id(pubkey)?,
        };
        let key = match id {
            Some(account_id) => IndexerV3CandidateKey::RegistryId(account_id),
            None => IndexerV3CandidateKey::RegistryAbsent,
        };
        let candidates =
            build_indexer_v3_candidate_blocks_for_key(self.adaptive_reader()?, key, policy)
                .map_err(Error::Reverse)?;
        self.validate_candidate_geometry(&candidates)?;
        let selected = candidates_in_request(&candidates.block_ids, request)?;
        let scan = self
            .source
            .scan_selected_blocks_parallel_with_registry_policy(
                request,
                selected,
                self.registry_read_policy,
                workers,
                sink,
            )
            .map_err(Error::Query)?;
        let transport_io = self.io_snapshot().saturating_sub(io_before);
        let transport = self.transport_snapshot().saturating_sub(transport_before);
        Ok(IndexerV3TargetedScanReceipt {
            candidates,
            scan,
            transport_io,
            transport,
        })
    }

    /// Visit sound candidate blocks for one reached program.
    ///
    /// The visitor must confirm the target program in the canonical
    /// transactions before it emits a final match.
    pub fn for_each_reached_program_candidate_block<F>(
        &mut self,
        program_id: &[u8; 32],
        request: &ScanRequest,
        visitor: F,
    ) -> Result<IndexerV3TargetedScanReceipt>
    where
        F: for<'a> FnMut(BlockView<'a>) -> QueryResult<()>,
    {
        let mut sink = FnBlockSink::new(visitor);
        self.scan_target_candidates(
            program_id,
            IndexerV3CandidatePolicy::ReachedProgram,
            request,
            &mut sink,
        )
    }

    /// Parallel form of [`Self::for_each_reached_program_candidate_block`].
    pub fn for_each_reached_program_candidate_block_parallel<F>(
        &mut self,
        program_id: &[u8; 32],
        request: &ScanRequest,
        workers: NonZeroUsize,
        visitor: F,
    ) -> Result<IndexerV3TargetedScanReceipt>
    where
        F: for<'a> FnMut(BlockView<'a>) -> QueryResult<()>,
    {
        let mut sink = FnBlockSink::new(visitor);
        self.scan_target_candidates_parallel(
            program_id,
            IndexerV3CandidatePolicy::ReachedProgram,
            request,
            workers,
            &mut sink,
        )
    }

    /// Visit sound candidate blocks for one signer wallet.
    ///
    /// The visitor must confirm the target signer in the canonical
    /// transactions before it emits a final match.
    pub fn for_each_signer_wallet_candidate_block<F>(
        &mut self,
        wallet: &[u8; 32],
        request: &ScanRequest,
        visitor: F,
    ) -> Result<IndexerV3TargetedScanReceipt>
    where
        F: for<'a> FnMut(BlockView<'a>) -> QueryResult<()>,
    {
        let mut sink = FnBlockSink::new(visitor);
        self.scan_target_candidates(
            wallet,
            IndexerV3CandidatePolicy::SignerWallet,
            request,
            &mut sink,
        )
    }

    /// Parallel form of [`Self::for_each_signer_wallet_candidate_block`].
    pub fn for_each_signer_wallet_candidate_block_parallel<F>(
        &mut self,
        wallet: &[u8; 32],
        request: &ScanRequest,
        workers: NonZeroUsize,
        visitor: F,
    ) -> Result<IndexerV3TargetedScanReceipt>
    where
        F: for<'a> FnMut(BlockView<'a>) -> QueryResult<()>,
    {
        let mut sink = FnBlockSink::new(visitor);
        self.scan_target_candidates_parallel(
            wallet,
            IndexerV3CandidatePolicy::SignerWallet,
            request,
            workers,
            &mut sink,
        )
    }

    /// Scope declared by the V3 file header.
    pub const fn source_scope(&self) -> IndexerV3SourceScope {
        self.source.scope()
    }

    /// Candidate-specific persistent cache directory.
    ///
    /// Local split archives return an empty path because they do not create or
    /// use a persistent cache.
    pub fn cache_root(&self) -> &Path {
        &self.cache_root
    }

    /// Current normalized network and persistent-cache counters.
    pub fn io_snapshot(&self) -> ArchiveIoSnapshot {
        self.cache
            .as_ref()
            .map_or_else(ArchiveIoSnapshot::default, range_io_snapshot)
    }

    /// Current V3 transport counters, with local reads separate from HTTP.
    pub fn transport_snapshot(&self) -> IndexerV3TransportReceipt {
        let local = self
            .local_split
            .as_ref()
            .map_or(LocalSplitReadStats::default(), |source| source.stats());
        IndexerV3TransportReceipt {
            kind: self.transport_kind,
            http_and_cache: self.io_snapshot(),
            local_read_calls: local.calls,
            local_read_bytes: local.bytes,
        }
    }

    /// Verify the identities of all local objects opened by this archive.
    ///
    /// HTTPS archives have no local split roots and return success.
    pub fn verify_local_unchanged(&self) -> Result<()> {
        if let Some(source) = &self.local_split {
            source.verify_unchanged().map_err(Error::RangeSource)?;
        }
        Ok(())
    }

    /// Consume the reader and return its final I/O counters.
    pub fn finish_io(self) -> ArchiveIoSnapshot {
        self.finish_transport_io().http_and_cache
    }

    /// Consume the reader and return final V3 transport counters.
    pub fn finish_transport_io(self) -> IndexerV3TransportReceipt {
        let Self {
            source,
            cache,
            local_split,
            shared_source,
            reverse_available: _,
            registry_index,
            adaptive_reader,
            cache_profile: _,
            cache_root: _,
            bound_source_size_bytes: _,
            cached_source_size_bytes: _,
            transport_kind,
            registry_read_policy: _,
        } = self;
        let http_and_cache = cache
            .as_ref()
            .map_or_else(ArchiveIoSnapshot::default, range_io_snapshot);
        let local = local_split
            .as_ref()
            .map_or(LocalSplitReadStats::default(), |source| source.stats());
        drop(source);
        drop(registry_index);
        drop(adaptive_reader);
        drop(shared_source);
        drop(cache);
        drop(local_split);
        IndexerV3TransportReceipt {
            kind: transport_kind,
            http_and_cache,
            local_read_calls: local.calls,
            local_read_bytes: local.bytes,
        }
    }

    fn registry_index(&mut self) -> Result<&IndexerV3RegistryIndex> {
        self.require_reverse_objects()?;
        if self.registry_index.is_none() {
            let index = IndexerV3RegistryIndex::open(
                Arc::clone(&self.shared_source),
                self.source.registry_entries(),
            )
            .map_err(Error::Reverse)?;
            self.registry_index = Some(index);
        }
        self.registry_index
            .as_ref()
            .ok_or_else(|| Error::ReverseUnavailable("registry index did not open".into()))
    }

    fn adaptive_reader(&mut self) -> Result<&AdaptiveV3Reader> {
        self.require_reverse_objects()?;
        if self.adaptive_reader.is_none() {
            let (
                expected_epoch,
                expected_slots,
                expected_blocks,
                expected_transactions,
                expected_registry_entries,
            ) = {
                let identity = self.source.identity();
                (
                    identity.epoch,
                    identity.slots_per_epoch,
                    u64::from(identity.block_count),
                    self.source.selected_transactions(),
                    self.source.registry_entries(),
                )
            };
            let reader = self.source.open_adaptive_reader().map_err(Error::Reverse)?;
            if reader.epoch() != expected_epoch
                || reader.slots_per_epoch() != expected_slots
                || reader.standalone_selected_blocks() != expected_blocks
                || reader.standalone_selected_transactions() != expected_transactions
                || reader.registry_entries() != expected_registry_entries
            {
                return Err(Error::ReverseUnavailable(
                    "adaptive reader geometry differs from the bound instruction source".into(),
                ));
            }
            self.adaptive_reader = Some(reader);
        }
        self.adaptive_reader
            .as_ref()
            .ok_or_else(|| Error::ReverseUnavailable("adaptive reader did not open".into()))
    }

    fn validate_candidate_geometry(&self, candidates: &IndexerV3CandidateBlocks) -> Result<()> {
        let identity = self.source.identity();
        let geometry = candidates.geometry;
        if geometry.epoch != identity.epoch
            || geometry.slots_per_epoch != identity.slots_per_epoch
            || geometry.epoch_first_slot != identity.first_slot
            || geometry.epoch_end_slot_exclusive
                != identity
                    .first_slot
                    .checked_add(identity.slots_per_epoch)
                    .ok_or(Error::SizeOverflow)?
            || geometry.registry_entries != self.source.registry_entries()
            || geometry.selected_blocks != u64::from(identity.block_count)
            || geometry.selected_transactions != self.source.selected_transactions()
        {
            return Err(Error::ReverseUnavailable(
                "candidate geometry differs from the bound instruction source".into(),
            ));
        }
        Ok(())
    }

    fn require_reverse_objects(&self) -> Result<()> {
        if self.reverse_available {
            Ok(())
        } else {
            Err(Error::ReverseUnavailable(
                "the bound epoch does not contain the complete adaptive reverse object set".into(),
            ))
        }
    }
}

impl ArchiveInstructionSource for IndexerV3Archive {
    fn identity(&self) -> &SourceIdentity {
        self.source.identity()
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_model::Result<ScanReceipt> {
        self.source
            .scan_ordered_with_registry_policy(request, self.registry_read_policy, sink)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerEndpoint {
    normalized_origin: String,
    v3_base: Url,
}

impl WorkerEndpoint {
    fn parse(origin: &str, allow_insecure_http: bool) -> Result<Self> {
        let mut parsed = Url::parse(origin)
            .map_err(|error| Error::InvalidOrigin(format!("URL parse failed: {error}")))?;
        match parsed.scheme() {
            "https" => {}
            "http" if allow_insecure_http => {}
            "http" => return Err(Error::InvalidOrigin("cleartext HTTP is disabled".into())),
            scheme => {
                return Err(Error::InvalidOrigin(format!(
                    "unsupported URL scheme {scheme}"
                )));
            }
        }
        if parsed.host_str().is_none() {
            return Err(Error::InvalidOrigin("URL has no host".into()));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(Error::InvalidOrigin("URL contains credentials".into()));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(Error::InvalidOrigin(
                "URL contains a query or fragment".into(),
            ));
        }
        if !matches!(parsed.path(), "" | "/") {
            return Err(Error::InvalidOrigin("URL contains a path".into()));
        }

        parsed.set_path("");
        let normalized_origin = parsed.as_str().trim_end_matches('/').to_owned();
        parsed.set_path("/indexer-v3/");
        Ok(Self {
            normalized_origin,
            v3_base: parsed,
        })
    }
}

fn bind_required_object(http: &HttpRangeSource, epoch: u64, name: &str) -> Result<BoundObject> {
    let identity = http.strong_identity(name).map_err(Error::RangeSource)?;
    Ok(BoundObject {
        name: name.to_owned(),
        url: object_url(http.base_url(), epoch, name)?,
        identity: Some(identity),
    })
}

fn bind_optional_object(http: &HttpRangeSource, epoch: u64, name: &str) -> Result<BoundObject> {
    let identity = match http.strong_identity(name) {
        Ok(identity) => Some(identity),
        Err(SourceError::NotFound(_)) => None,
        Err(error) => return Err(Error::RangeSource(error)),
    };
    Ok(BoundObject {
        name: name.to_owned(),
        url: object_url(http.base_url(), epoch, name)?,
        identity,
    })
}

fn object_url(base: &Url, epoch: u64, name: &str) -> Result<String> {
    let mut url = base.clone();
    let mut path = url
        .path_segments_mut()
        .map_err(|_| Error::InvalidOrigin("Indexer V3 URL cannot accept route segments".into()))?;
    path.pop_if_empty();
    path.push(&epoch.to_string());
    path.push(name);
    drop(path);
    Ok(url.to_string())
}

fn local_epoch_root(archive_root: &Path, epoch: u64) -> PathBuf {
    archive_root.join("indexer-v3").join(epoch.to_string())
}

fn object_set_binding(epoch: u64, objects: &[BoundObject]) -> String {
    let mut ordered = objects.to_vec();
    ordered.sort_unstable_by(|left, right| left.url.as_bytes().cmp(right.url.as_bytes()));
    let mut digest = Sha256::new();
    digest.update(OBJECT_SET_BINDING_DOMAIN);
    digest.update(epoch.to_le_bytes());
    digest.update((ordered.len() as u64).to_le_bytes());
    for object in ordered {
        hash_bytes(&mut digest, object.url.as_bytes());
        match object.identity {
            Some(identity) => {
                digest.update([1]);
                digest.update(identity.length.to_le_bytes());
                hash_bytes(&mut digest, identity.strong_etag.as_bytes());
            }
            None => digest.update([0]),
        }
    }
    hex_lower(&digest.finalize())
}

fn selected_object_size(objects: &[BoundObject], names: &[&str]) -> Result<u64> {
    names.iter().try_fold(0u64, |total, name| {
        let object = objects
            .iter()
            .find(|object| object.name == *name)
            .ok_or_else(|| Error::Geometry(format!("V3 cache object {name} is not bound")))?;
        let length = object
            .identity
            .as_ref()
            .ok_or_else(|| Error::Geometry(format!("V3 cache object {name} is absent")))?
            .length;
        total.checked_add(length).ok_or(Error::SizeOverflow)
    })
}

fn object_is_present(objects: &[BoundObject], name: &str) -> bool {
    objects
        .iter()
        .any(|object| object.name == name && object.identity.is_some())
}

fn missing_objects(objects: &[BoundObject], names: &[&str]) -> String {
    names
        .iter()
        .copied()
        .filter(|name| !object_is_present(objects, name))
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LocalSplitReadStats {
    calls: u64,
    bytes: u64,
}

#[derive(Debug)]
struct LocalSplitRangeSource {
    ledger: PinnedLocalRangeSource,
    retained: PinnedLocalRangeSource,
    stats: Mutex<LocalSplitReadStats>,
    bound_source_size_bytes: u64,
}

impl LocalSplitRangeSource {
    fn open(ledger_root: &Path, retained_root: &Path) -> Result<Self> {
        let ledger_objects = local_split_ledger_objects();
        let retained_objects = local_split_retained_objects();
        let ledger = PinnedLocalRangeSource::new_anchored(ledger_root, &ledger_objects)
            .map_err(Error::RangeSource)?;
        let retained = PinnedLocalRangeSource::new_anchored(retained_root, &retained_objects)
            .map_err(Error::RangeSource)?;

        let mut bound_source_size_bytes = 0_u64;
        for object in &ledger_objects {
            let size = required_local_object_size(&ledger, object, "ledger")?;
            bound_source_size_bytes = bound_source_size_bytes
                .checked_add(size)
                .ok_or(Error::SizeOverflow)?;
        }
        for object in LOCAL_SPLIT_REQUIRED_RETAINED_OBJECTS {
            let size = required_local_object_size(&retained, object, "retained")?;
            bound_source_size_bytes = bound_source_size_bytes
                .checked_add(size)
                .ok_or(Error::SizeOverflow)?;
        }
        for object in INDEXER_V3_OPTIONAL_RETAINED_SIDECARS {
            if let Some(size) = retained.size(object).map_err(Error::RangeSource)? {
                bound_source_size_bytes = bound_source_size_bytes
                    .checked_add(size)
                    .ok_or(Error::SizeOverflow)?;
            }
        }

        Ok(Self {
            ledger,
            retained,
            stats: Mutex::new(LocalSplitReadStats::default()),
            bound_source_size_bytes,
        })
    }

    const fn bound_source_size_bytes(&self) -> u64 {
        self.bound_source_size_bytes
    }

    fn stats(&self) -> LocalSplitReadStats {
        *self
            .stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn record(&self, bytes: usize) -> SourceResult<()> {
        let bytes = u64::try_from(bytes)
            .map_err(|_| SourceError::Protocol("local V3 read size exceeds u64".into()))?;
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| SourceError::Protocol("local V3 read counter is poisoned".into()))?;
        stats.calls = stats
            .calls
            .checked_add(1)
            .ok_or_else(|| SourceError::Protocol("local V3 read-call count overflow".into()))?;
        stats.bytes = stats
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| SourceError::Protocol("local V3 read-byte count overflow".into()))?;
        Ok(())
    }

    fn source_for(&self, object: &str) -> SourceResult<&PinnedLocalRangeSource> {
        if is_local_split_ledger_object(object) {
            Ok(&self.ledger)
        } else if is_local_split_retained_object(object) {
            Ok(&self.retained)
        } else {
            Err(SourceError::Protocol(format!(
                "object {object} is outside the local V3 split-root allowlists"
            )))
        }
    }

    fn verify_unchanged(&self) -> SourceResult<()> {
        self.ledger.verify_unchanged()?;
        self.retained.verify_unchanged()
    }
}

impl RangeSource for LocalSplitRangeSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        self.source_for(object)?.size(object)
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        let bytes = self
            .source_for(object)?
            .read_range(object, offset, length)?;
        self.record(bytes.len())?;
        Ok(bytes)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        self.source_for(object)?
            .read_range_into(object, offset, length, destination)?;
        self.record(destination.len())
    }

    fn read_range_into_slice(
        &self,
        object: &str,
        offset: u64,
        destination: &mut [u8],
    ) -> SourceResult<()> {
        self.source_for(object)?
            .read_range_into_slice(object, offset, destination)?;
        self.record(destination.len())
    }
}

fn local_split_ledger_objects() -> Vec<&'static str> {
    indexer_v3_required_ledger_objects()
        .chain(LOCAL_SPLIT_REQUIRED_ADAPTIVE_OBJECTS)
        .collect()
}

fn local_split_retained_objects() -> Vec<&'static str> {
    LOCAL_SPLIT_REQUIRED_RETAINED_OBJECTS
        .into_iter()
        .chain(INDEXER_V3_OPTIONAL_RETAINED_SIDECARS)
        .collect()
}

fn is_local_split_ledger_object(object: &str) -> bool {
    indexer_v3_required_ledger_objects().any(|candidate| candidate == object)
        || LOCAL_SPLIT_REQUIRED_ADAPTIVE_OBJECTS.contains(&object)
}

fn is_local_split_retained_object(object: &str) -> bool {
    LOCAL_SPLIT_REQUIRED_RETAINED_OBJECTS.contains(&object)
        || INDEXER_V3_OPTIONAL_RETAINED_SIDECARS.contains(&object)
}

fn required_local_object_size(
    source: &PinnedLocalRangeSource,
    object: &str,
    root_kind: &str,
) -> Result<u64> {
    source
        .size(object)
        .map_err(Error::RangeSource)?
        .ok_or_else(|| {
            Error::Geometry(format!(
                "required local V3 {root_kind} object {object} is missing"
            ))
        })
}

fn cache_namespace(origin: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(CACHE_NAMESPACE_DOMAIN);
    hash_bytes(&mut digest, origin.as_bytes());
    let value = digest.finalize();
    format!("origin-{}", hex_lower(&value[..16]))
}

fn archive_first_slot(epoch: u64) -> Result<u64> {
    epoch
        .checked_mul(MAINNET_ARCHIVE_SLOTS_PER_EPOCH)
        .ok_or_else(|| Error::Geometry("archive epoch slot range overflows u64".into()))
}

fn validate_identity(
    source: &IndexerV3InstructionSource,
    requested_epoch: u64,
    expected_verification: SourceVerification,
) -> Result<()> {
    let identity = source.identity();
    if identity.format != ArchiveFormat::IndexerV3 {
        return Err(Error::Geometry(format!(
            "reader returned {} instead of Indexer V3",
            identity.format
        )));
    }
    if identity.epoch != requested_epoch {
        return Err(Error::Geometry(format!(
            "reader epoch {} differs from requested epoch {requested_epoch}",
            identity.epoch
        )));
    }
    if identity.first_slot != archive_first_slot(requested_epoch)? {
        return Err(Error::Geometry(format!(
            "reader first slot {} differs from the fixed archive window",
            identity.first_slot
        )));
    }
    if identity.slots_per_epoch != MAINNET_ARCHIVE_SLOTS_PER_EPOCH {
        return Err(Error::Geometry(format!(
            "reader has {} slots per epoch; the archive window has {}",
            identity.slots_per_epoch, MAINNET_ARCHIVE_SLOTS_PER_EPOCH
        )));
    }
    if identity.verification != expected_verification {
        return Err(Error::Geometry(format!(
            "reader verification is {:?}, expected {expected_verification:?}",
            identity.verification,
        )));
    }
    if identity.binding.as_deref().is_none_or(str::is_empty) {
        return Err(Error::Geometry(
            "reader has no stable candidate identity".into(),
        ));
    }
    Ok(())
}

fn validate_worker_count(workers: NonZeroUsize) -> Result<()> {
    if workers.get() > MAX_INDEXER_V3_PARALLEL_WORKERS {
        return Err(Error::Query(QueryError::InvalidRequest(format!(
            "parallel V3 workers {} exceeds the {MAX_INDEXER_V3_PARALLEL_WORKERS}-worker limit",
            workers.get()
        ))));
    }
    Ok(())
}

fn bounded_range(block_count: u32, first_block: u32, max_blocks: NonZeroU32) -> Result<ScanRange> {
    if first_block >= block_count {
        return Err(Error::InvalidRange(format!(
            "first block {first_block} is outside block rows 0..{block_count}"
        )));
    }
    let available = block_count - first_block;
    let block_count = NonZeroU32::new(available.min(max_blocks.get()))
        .ok_or_else(|| Error::InvalidRange("selected range is empty".into()))?;
    Ok(ScanRange {
        first_block,
        block_count,
    })
}

fn candidates_in_request<'a>(candidates: &'a [u32], request: &ScanRequest) -> Result<&'a [u32]> {
    let Some(range) = request.range else {
        return Ok(candidates);
    };
    let end = range
        .first_block
        .checked_add(range.block_count.get())
        .ok_or_else(|| {
            Error::Query(QueryError::InvalidRequest(
                "V3 block range overflows u32".into(),
            ))
        })?;
    let start_index = candidates.partition_point(|candidate| *candidate < range.first_block);
    let end_index = candidates.partition_point(|candidate| *candidate < end);
    Ok(&candidates[start_index..end_index])
}

fn range_io_snapshot(cache: &CachedHttpRangeSource) -> ArchiveIoSnapshot {
    let http = cache.http().stats();
    let cache = cache.stats();
    ArchiveIoSnapshot {
        head_requests: http.head_requests,
        get_requests: http.get_requests,
        incomplete_body_retries: http.incomplete_body_retries,
        network_body_bytes: http.returned_body_bytes,
        cache_hits: cache.cache_hits,
        cache_downloads: cache.cache_downloads,
        cache_read_calls: cache.local_read_calls,
        cache_read_bytes: cache.local_read_bytes,
    }
}

fn hash_bytes(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_le_bytes());
    digest.update(bytes);
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

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    fn present(url: &str, length: u64, etag: &str) -> BoundObject {
        BoundObject {
            name: url.rsplit('/').next().unwrap().into(),
            url: url.into(),
            identity: Some(HttpObjectIdentity {
                length,
                strong_etag: etag.into(),
            }),
        }
    }

    #[test]
    fn worker_origin_derives_only_the_v3_route() {
        let endpoint = WorkerEndpoint::parse("https://archive.example", false).unwrap();
        assert_eq!(endpoint.normalized_origin, "https://archive.example");
        assert_eq!(
            endpoint.v3_base.as_str(),
            "https://archive.example/indexer-v3/"
        );

        assert!(WorkerEndpoint::parse("http://archive.example", false).is_err());
        assert!(WorkerEndpoint::parse("https://archive.example/other", false).is_err());
        assert!(WorkerEndpoint::parse("https://archive.example?token=x", false).is_err());
    }

    #[test]
    fn public_and_local_paths_use_only_format_epoch_and_object() {
        let endpoint = WorkerEndpoint::parse("https://archive.example", false).unwrap();
        assert_eq!(
            object_url(&endpoint.v3_base, 900, "registry.bin").unwrap(),
            "https://archive.example/indexer-v3/900/registry.bin"
        );
        assert_eq!(
            local_epoch_root(Path::new("archive"), 900),
            Path::new("archive/indexer-v3/900")
        );
    }

    #[test]
    fn binding_covers_url_length_etag_presence_and_epoch() {
        let objects = vec![
            present("https://a/indexer-v3/0/index", 12, "\"a\""),
            BoundObject {
                name: "optional".into(),
                url: "https://a/indexer-v3/0/optional".into(),
                identity: None,
            },
        ];
        let original = object_set_binding(0, &objects);
        let mut changed = objects.clone();
        changed[0].url.push('x');
        assert_ne!(original, object_set_binding(0, &changed));
        let mut changed = objects.clone();
        changed[0].identity.as_mut().unwrap().length += 1;
        assert_ne!(original, object_set_binding(0, &changed));
        let mut changed = objects.clone();
        changed[0].identity.as_mut().unwrap().strong_etag.push('x');
        assert_ne!(original, object_set_binding(0, &changed));
        let mut changed = objects.clone();
        changed[1] = present(&changed[1].url, 1, "\"new\"");
        assert_ne!(original, object_set_binding(0, &changed));
        assert_ne!(original, object_set_binding(1, &objects));
    }

    #[test]
    fn binding_is_independent_of_input_order() {
        let mut objects = vec![
            present("https://a/second", 2, "\"b\""),
            present("https://a/first", 1, "\"a\""),
        ];
        let expected = object_set_binding(0, &objects);
        objects.reverse();
        assert_eq!(expected, object_set_binding(0, &objects));
    }

    #[test]
    fn cache_sets_match_the_read_pattern() {
        let ledger = indexer_v3_required_ledger_objects().collect::<Vec<_>>();
        assert_eq!(ledger[1], TRANSACTION_DIRECTORY_OBJECT);
        let objects = vec![
            present("https://a/archive-v2-standalone-blocks.index", 100, "\"i\""),
            present(
                "https://a/archive-v2-standalone-transaction-directory.wincode",
                200,
                "\"d\"",
            ),
            present("https://a/registry.bin", 900, "\"r\""),
            present("https://a/registry.mphf", 30, "\"m\""),
            present(
                &format!("https://a/{ADAPTIVE_V3_CONTROL_FILE}"),
                40,
                "\"c\"",
            ),
            present(
                &format!("https://a/{ADAPTIVE_V3_COVERAGE_FILE}"),
                50,
                "\"v\"",
            ),
        ];
        assert_eq!(
            selected_object_size(&objects, &[ledger[0], ledger[1]]).unwrap(),
            300
        );
        assert_eq!(
            selected_object_size(
                &objects,
                &[
                    ledger[0],
                    REGISTRY_INDEX_OBJECT,
                    ADAPTIVE_V3_CONTROL_FILE,
                    ADAPTIVE_V3_COVERAGE_FILE,
                ],
            )
            .unwrap(),
            220
        );
    }

    #[test]
    fn first_slot_uses_fixed_archive_windows() {
        assert_eq!(archive_first_slot(0).unwrap(), 0);
        assert_eq!(archive_first_slot(1).unwrap(), 432_000);
        assert_eq!(archive_first_slot(185).unwrap(), 79_920_000);
    }

    #[test]
    fn bounded_range_caps_at_available_rows() {
        let range = bounded_range(100, 90, NonZeroU32::new(20).unwrap()).unwrap();
        assert_eq!(range.first_block, 90);
        assert_eq!(range.block_count.get(), 10);
        assert!(bounded_range(100, 100, NonZeroU32::new(1).unwrap()).is_err());
    }

    #[test]
    fn candidate_filter_keeps_only_the_requested_sorted_slice() {
        let request = ScanRequest::bounded(ScanRange {
            first_block: 4,
            block_count: NonZeroU32::new(5).unwrap(),
        });
        assert_eq!(
            candidates_in_request(&[1, 4, 7, 8, 9, 12], &request).unwrap(),
            vec![4, 7, 8]
        );
        assert_eq!(
            candidates_in_request(&[1, 4, 7], &ScanRequest::all()).unwrap(),
            vec![1, 4, 7]
        );
    }

    #[test]
    fn candidate_filter_rejects_a_range_overflow() {
        let request = ScanRequest::bounded(ScanRange {
            first_block: u32::MAX,
            block_count: NonZeroU32::new(1).unwrap(),
        });
        assert!(matches!(
            candidates_in_request(&[], &request),
            Err(Error::Query(QueryError::InvalidRequest(_)))
        ));
    }

    fn split_fixture() -> (tempfile::TempDir, tempfile::TempDir) {
        let ledger = tempdir().unwrap();
        let retained = tempdir().unwrap();
        for (index, object) in local_split_ledger_objects().into_iter().enumerate() {
            fs::write(ledger.path().join(object), [index as u8 + 1, 0xa5]).unwrap();
        }
        for (index, object) in LOCAL_SPLIT_REQUIRED_RETAINED_OBJECTS
            .into_iter()
            .enumerate()
        {
            fs::write(
                retained.path().join(object),
                [index as u8 + 0x40, 0x5a, 0x7e],
            )
            .unwrap();
        }
        fs::write(
            retained
                .path()
                .join(INDEXER_V3_OPTIONAL_RETAINED_SIDECARS[0]),
            [0x91, 0x92, 0x93, 0x94],
        )
        .unwrap();
        (ledger, retained)
    }

    #[test]
    fn split_local_source_routes_exact_allowlisted_roots_and_counts_reads() {
        let (ledger, retained) = split_fixture();
        let source = LocalSplitRangeSource::open(ledger.path(), retained.path()).unwrap();
        let ledger_object = indexer_v3_required_ledger_objects().next().unwrap();
        assert_eq!(source.read_range(ledger_object, 0, 2).unwrap(), [1, 0xa5]);
        assert_eq!(
            source
                .read_range(LOCAL_SPLIT_REQUIRED_RETAINED_OBJECTS[0], 0, 3)
                .unwrap(),
            [0x40, 0x5a, 0x7e]
        );
        assert_eq!(source.stats(), LocalSplitReadStats { calls: 2, bytes: 5 });
        assert!(source.size("outside.bin").is_err());
    }

    #[test]
    fn split_local_source_fails_on_each_required_root_class() {
        let (ledger, retained) = split_fixture();
        fs::remove_file(ledger.path().join(ADAPTIVE_V3_CONTROL_FILE)).unwrap();
        let error = LocalSplitRangeSource::open(ledger.path(), retained.path()).unwrap_err();
        assert!(error.to_string().contains(ADAPTIVE_V3_CONTROL_FILE));

        let (ledger, retained) = split_fixture();
        fs::remove_file(retained.path().join(REGISTRY_INDEX_OBJECT)).unwrap();
        let error = LocalSplitRangeSource::open(ledger.path(), retained.path()).unwrap_err();
        assert!(error.to_string().contains(REGISTRY_INDEX_OBJECT));
    }

    #[test]
    fn split_local_source_pins_optional_presence_and_verifies_open_files() {
        let (ledger, retained) = split_fixture();
        let optional = INDEXER_V3_OPTIONAL_RETAINED_SIDECARS[1];
        let source = LocalSplitRangeSource::open(ledger.path(), retained.path()).unwrap();
        assert_eq!(source.size(optional).unwrap(), None);
        fs::write(retained.path().join(optional), [1, 2, 3]).unwrap();
        assert_eq!(source.size(optional).unwrap(), None);

        let ledger_object = indexer_v3_required_ledger_objects().next().unwrap();
        fs::write(ledger.path().join(ledger_object), [1, 2, 3, 4]).unwrap();
        assert!(source.verify_unchanged().is_err());
    }

    #[test]
    fn transport_receipt_keeps_local_bytes_out_of_network_fields() {
        let before = IndexerV3TransportReceipt {
            kind: IndexerV3TransportKind::LocalSplit,
            http_and_cache: ArchiveIoSnapshot::default(),
            local_read_calls: 4,
            local_read_bytes: 100,
        };
        let after = IndexerV3TransportReceipt {
            kind: IndexerV3TransportKind::LocalSplit,
            http_and_cache: ArchiveIoSnapshot::default(),
            local_read_calls: 7,
            local_read_bytes: 180,
        };
        assert_eq!(
            after.saturating_sub(before),
            IndexerV3TransportReceipt {
                kind: IndexerV3TransportKind::LocalSplit,
                http_and_cache: ArchiveIoSnapshot::default(),
                local_read_calls: 3,
                local_read_bytes: 80,
            }
        );
    }
}
