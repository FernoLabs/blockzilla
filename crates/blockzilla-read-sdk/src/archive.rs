//! Small facade for one network or operator-trusted local Compact V2 epoch.
//!
//! [`CompactV2Archive`] owns source admission, fixed archive-window geometry,
//! and the canonical query adapter. Network mode owns the Worker route, the
//! format-defined object inventory, and a strict persistent cache. Local mode
//! owns a pinned directory and a separate operator-trusted size descriptor.

use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroU32,
    path::{Path, PathBuf},
};

pub use blockzilla_query_sdk::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveInstructionSourceExt, ArchiveIoSnapshot,
    BlockSink, BlockView, Error as QueryError, FnBlockSink, InstructionDataRequirement,
    RecordedTokenBalance, Result as QueryResult, ScanIoReceipt, ScanRange, ScanReceipt,
    ScanRequest, SourceIdentity, SourceVerification, TokenBalanceCoverage, TokenBalanceRequirement,
    TokenBalanceSide, TransactionView,
};
use crate::{
    ArchiveIdentity, ArchiveReader, CachedHttpRangeSource, CompactV2InstructionSource,
    CompactV2InstructionSourceError, HashVerification, HttpObjectIdentity, HttpObjectPathLayout,
    HttpRangeSource, HttpRangeSourceOptions, OpenOptions, PinnedLocalRangeSource, RangeSource,
    SourceError, create_http_cache_directory,
};
pub use crate::{
    COMPACT_V2_PARTIAL_REGISTRY_PREFETCH_MIN_TRANSACTIONS,
    COMPACT_V2_PROJECTION_SCRATCH_RETAINED_BYTES, CompactV2ParallelRegistryMode,
    CompactV2ParallelRegistryReceipt, CompactV2ParallelScanConfig, CompactV2ParallelScanReceipt,
    CompactV2RegistryReadPolicy, DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES,
    MAX_ORDERED_PARALLEL_DECODE_WORKERS as MAX_COMPACT_V2_PARALLEL_WORKERS,
};
use crate::{CompactV2MessageSchema, CompactV2MetadataSchema};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

const MAINNET_ARCHIVE_SLOTS_PER_EPOCH: u64 = 432_000;
const CACHE_NAMESPACE_DOMAIN: &[u8] = b"blockzilla.archive-sdk.cache-namespace.v1\0";
const OBJECT_SET_ID_DOMAIN: &[u8] = b"blockzilla.compact-v2.http-object-set.v1\0";

const REQUIRED_COMPACT_V2_OBJECTS: &[&str] = &[
    blockzilla_format::ARCHIVE_V2_BLOCKS_FILE,
    blockzilla_format::ARCHIVE_V2_BLOCK_INDEX_FILE,
    blockzilla_format::ARCHIVE_V2_META_FILE,
    blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
];

const OPTIONAL_COMPACT_V2_OBJECTS: &[&str] = &[
    blockzilla_format::ARCHIVE_V2_SIGNATURES_FILE,
    blockzilla_format::ARCHIVE_V2_GENESIS_BIN_FILE,
    blockzilla_format::ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    blockzilla_format::ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    blockzilla_format::ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    blockzilla_format::ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    blockzilla_format::ARCHIVE_V2_POH_FILE,
    blockzilla_format::ARCHIVE_V2_SHREDDING_FILE,
    blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    blockzilla_format::ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    blockzilla_format::ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    blockzilla_format::ARCHIVE_V2_BLOCK_ACCESS_FILE,
    blockzilla_format::ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    blockzilla_format::ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
];

const LOCAL_COMPACT_V2_OBJECTS: &[&str] = &[
    blockzilla_format::ARCHIVE_V2_BLOCKS_FILE,
    blockzilla_format::ARCHIVE_V2_BLOCK_INDEX_FILE,
    blockzilla_format::ARCHIVE_V2_META_FILE,
    blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    blockzilla_format::ARCHIVE_V2_SIGNATURES_FILE,
    blockzilla_format::ARCHIVE_V2_GENESIS_BIN_FILE,
    blockzilla_format::ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    blockzilla_format::ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    blockzilla_format::ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    blockzilla_format::ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    blockzilla_format::ARCHIVE_V2_POH_FILE,
    blockzilla_format::ARCHIVE_V2_SHREDDING_FILE,
    blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    blockzilla_format::ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    blockzilla_format::ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    blockzilla_format::ARCHIVE_V2_BLOCK_ACCESS_FILE,
    blockzilla_format::ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    blockzilla_format::ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
];

/// Network options for [`CompactV2Archive::open_with_options`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactV2OpenOptions {
    /// Permit cleartext HTTP for a controlled local fixture.
    pub allow_insecure_http: bool,
}

/// Explicit identity for one local reader set.
///
/// This is an operator input, not a content-derived identity.
/// `candidate_id` must change when the operator replaces the reader set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactV2LocalDescriptor {
    pub cluster_id: String,
    pub epoch: u64,
    pub candidate_id: String,
    pub first_slot: u64,
    pub slots_per_epoch: u64,
}

impl CompactV2LocalDescriptor {
    /// Describe a mainnet reader set in the fixed-width epoch range used by
    /// the retained Compact V2 archives.
    pub fn mainnet(epoch: u64, candidate_id: impl Into<String>) -> Result<Self> {
        let first_slot = epoch
            .checked_mul(MAINNET_ARCHIVE_SLOTS_PER_EPOCH)
            .ok_or_else(|| Error::Geometry("mainnet epoch first slot overflows u64".into()))?;
        let descriptor = Self {
            cluster_id: "mainnet-beta".into(),
            epoch,
            candidate_id: candidate_id.into(),
            first_slot,
            slots_per_epoch: MAINNET_ARCHIVE_SLOTS_PER_EPOCH,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("cluster_id", self.cluster_id.as_str()),
            ("candidate_id", self.candidate_id.as_str()),
        ] {
            if value.is_empty()
                || value.len() > 4096
                || value
                    .bytes()
                    .any(|byte| byte == 0 || byte.is_ascii_control())
            {
                return Err(Error::InvalidLocalDescriptor(format!(
                    "{name} is empty, too long, or contains a control character"
                )));
            }
        }
        if self
            .candidate_id
            .bytes()
            .any(|byte| byte.is_ascii_whitespace())
        {
            return Err(Error::InvalidLocalDescriptor(
                "candidate_id must be one result-safe token without whitespace".into(),
            ));
        }
        if self.slots_per_epoch == 0 {
            return Err(Error::InvalidLocalDescriptor(
                "slots_per_epoch must be greater than zero".into(),
            ));
        }
        self.first_slot
            .checked_add(self.slots_per_epoch - 1)
            .ok_or_else(|| {
                Error::InvalidLocalDescriptor("epoch slot range overflows u64".into())
            })?;
        Ok(())
    }
}

/// Transport used by the admitted Compact V2 source.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompactV2TransportKind {
    #[default]
    Https,
    LocalDirectory,
}

impl fmt::Display for CompactV2TransportKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Https => formatter.write_str("https"),
            Self::LocalDirectory => formatter.write_str("local-directory"),
        }
    }
}

/// Transport counters with local reads separate from HTTP and cache work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactV2TransportReceipt {
    pub kind: CompactV2TransportKind,
    pub http_and_cache: ArchiveIoSnapshot,
    pub local_read_calls: u64,
    pub local_read_bytes: u64,
}

impl CompactV2TransportReceipt {
    pub const fn saturating_sub(self, earlier: Self) -> Self {
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

/// Errors from Compact V2 network setup and range selection.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid Worker origin: {0}")]
    InvalidOrigin(String),
    #[error("invalid archive range: {0}")]
    InvalidRange(String),
    #[error("invalid Compact V2 geometry: {0}")]
    Geometry(String),
    #[error("invalid operator-trusted local descriptor: {0}")]
    InvalidLocalDescriptor(String),
    #[error("archive range source failed")]
    RangeSource(#[source] SourceError),
    #[error("Compact V2 reader failed")]
    Reader(#[source] crate::Error),
    #[error("Compact V2 query adapter failed")]
    Query(#[source] CompactV2InstructionSourceError),
    #[error("archive size arithmetic overflow")]
    SizeOverflow,
}

pub type Result<T> = std::result::Result<T, Error>;

/// One admitted Compact V2 epoch from the public Worker or a local directory.
///
/// Network admission probes the fixed Compact V2 object names. Every present
/// object must have an exact length and strong ETag. The SDK never downloads
/// an archive inventory file and never hashes an archive payload. Large block
/// and signature planes remain bounded range reads over authenticated TLS.
pub struct CompactV2Archive {
    source: CompactV2ArchiveSource,
    identity: SourceIdentity,
    bound_source_size_bytes: u64,
    cache_root: PathBuf,
    transport_kind: CompactV2TransportKind,
    candidate_id: Option<String>,
    registry_read_policy: CompactV2RegistryReadPolicy,
}

enum CompactV2ArchiveSource {
    Network(Box<CompactV2InstructionSource<CompactV2HttpObjectSet>>),
    Local(Box<CompactV2InstructionSource<PinnedLocalRangeSource>>),
}

/// One immutable remote Compact V2 object set.
///
/// `inventory` supplies all size calls without another network probe. The
/// underlying HTTP source has already pinned the same strong ETags and checks
/// them again on each payload response.
struct CompactV2HttpObjectSet {
    cache: CachedHttpRangeSource,
    inventory: NetworkObjectInventory,
}

impl RangeSource for CompactV2HttpObjectSet {
    fn size(&self, object: &str) -> std::result::Result<Option<u64>, SourceError> {
        Ok(self
            .inventory
            .identity(object)
            .map(|identity| identity.length))
    }

    fn read_range(
        &self,
        object: &str,
        offset: u64,
        length: usize,
    ) -> std::result::Result<Vec<u8>, SourceError> {
        self.require_object(object)?;
        self.cache.read_range(object, offset, length)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> std::result::Result<(), SourceError> {
        self.require_object(object)?;
        self.cache
            .read_range_into(object, offset, length, destination)
    }

    fn read_range_into_slice(
        &self,
        object: &str,
        offset: u64,
        destination: &mut [u8],
    ) -> std::result::Result<(), SourceError> {
        self.require_object(object)?;
        self.cache
            .read_range_into_slice(object, offset, destination)
    }
}

impl CompactV2HttpObjectSet {
    fn require_object(&self, object: &str) -> std::result::Result<(), SourceError> {
        if self.inventory.identity(object).is_none() {
            return Err(SourceError::NotFound(object.to_owned()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct NetworkObjectInventory {
    objects: BTreeMap<String, HttpObjectIdentity>,
}

impl NetworkObjectInventory {
    fn discover(http: &HttpRangeSource, epoch: u64) -> Result<Self> {
        let mut objects = BTreeMap::new();
        for &name in REQUIRED_COMPACT_V2_OBJECTS {
            let identity = http.strong_identity(name).map_err(Error::RangeSource)?;
            objects.insert(name.to_owned(), identity);
        }
        for &name in OPTIONAL_COMPACT_V2_OBJECTS {
            if name == blockzilla_format::ARCHIVE_V2_GENESIS_BIN_FILE && epoch != 0 {
                continue;
            }
            match http.strong_identity(name) {
                Ok(identity) => {
                    objects.insert(name.to_owned(), identity);
                }
                Err(SourceError::NotFound(_)) => {}
                Err(error) => return Err(Error::RangeSource(error)),
            }
        }
        Ok(Self { objects })
    }

    fn identity(&self, name: &str) -> Option<&HttpObjectIdentity> {
        self.objects.get(name)
    }

    fn total_bytes(&self) -> Result<u64> {
        self.objects.values().try_fold(0_u64, |total, identity| {
            total
                .checked_add(identity.length)
                .ok_or(Error::SizeOverflow)
        })
    }

    fn object_set_id(&self, epoch: u64) -> String {
        let mut digest = Sha256::new();
        digest.update(OBJECT_SET_ID_DOMAIN);
        digest.update(epoch.to_le_bytes());
        digest.update((self.objects.len() as u64).to_le_bytes());
        for (name, identity) in &self.objects {
            digest.update((name.len() as u64).to_le_bytes());
            digest.update(name.as_bytes());
            digest.update(identity.length.to_le_bytes());
            digest.update((identity.strong_etag.len() as u64).to_le_bytes());
            digest.update(identity.strong_etag.as_bytes());
        }
        format!("etag-set-{}", hex_lower(&digest.finalize()))
    }
}

impl CompactV2Archive {
    /// Open one mainnet-beta epoch from
    /// `origin/compact-v2/<epoch>/<object-name>`.
    pub fn open(origin: &str, epoch: u64, cache_root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(origin, epoch, cache_root, CompactV2OpenOptions::default())
    }

    /// Open with an explicit local-fixture transport policy.
    pub fn open_with_options(
        origin: &str,
        epoch: u64,
        cache_root: impl AsRef<Path>,
        options: CompactV2OpenOptions,
    ) -> Result<Self> {
        let worker = WorkerEndpoint::parse(origin, options.allow_insecure_http)?;
        let cache_root = cache_root.as_ref();
        create_http_cache_directory(cache_root).map_err(Error::RangeSource)?;

        let http = HttpRangeSource::with_options(
            &worker.compact_v2_base,
            epoch,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: options.allow_insecure_http,
                object_path_layout: HttpObjectPathLayout::FlatEpoch,
                ..HttpRangeSourceOptions::default()
            },
        )
        .map_err(Error::RangeSource)?;
        let inventory = NetworkObjectInventory::discover(&http, epoch)?;
        let object_set_id = inventory.object_set_id(epoch);
        let bound_source_size_bytes = inventory.total_bytes()?;
        let first_slot = mainnet_first_slot(epoch)?;

        let generation_cache = cache_root
            .join(cache_namespace(&worker.normalized_origin))
            .join("compact-v2")
            .join(format!("epoch-{epoch}"))
            .join(&object_set_id);
        create_http_cache_directory(&generation_cache).map_err(Error::RangeSource)?;
        let cached_objects = cache_objects(&inventory, epoch)?;
        let cache = CachedHttpRangeSource::open(http, &generation_cache, &cached_objects)
            .map_err(Error::RangeSource)?;
        let source = CompactV2HttpObjectSet { cache, inventory };
        let reader = ArchiveReader::open_object_set_with_schemas(
            source,
            ArchiveIdentity {
                cluster_id: "mainnet-beta".into(),
                epoch,
                generation_id: object_set_id.clone(),
                first_slot,
                slots_per_epoch: MAINNET_ARCHIVE_SLOTS_PER_EPOCH,
            },
            &object_set_id,
            OpenOptions {
                hash_verification: HashVerification::SizesOnly,
                epoch_first_slot: Some(first_slot),
                ..OpenOptions::default()
            },
            CompactV2MessageSchema::Current,
            CompactV2MetadataSchema::CurrentTypedError,
        )
        .map_err(Error::Reader)?;
        let source = CompactV2InstructionSource::new(reader, first_slot).map_err(Error::Query)?;
        let identity = source.identity().clone();

        Ok(Self {
            source: CompactV2ArchiveSource::Network(Box::new(source)),
            identity,
            bound_source_size_bytes,
            cache_root: generation_cache,
            transport_kind: CompactV2TransportKind::Https,
            candidate_id: None,
            registry_read_policy: CompactV2RegistryReadPolicy::with_full_registry_limit(
                DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES,
            ),
        })
    }

    /// Open one Compact V2 reader set from a pinned local root.
    ///
    /// The explicit descriptor supplies operator-trusted identity and geometry.
    /// Admission retains all structural index, registry, metadata,
    /// signature-length, and epoch-geometry checks.
    pub fn open_local(
        root: impl AsRef<Path>,
        descriptor: CompactV2LocalDescriptor,
    ) -> Result<Self> {
        descriptor.validate()?;
        let root = root.as_ref();
        if !root.is_absolute() {
            return Err(Error::InvalidLocalDescriptor(
                "local root must be an absolute path".into(),
            ));
        }
        let pinned = PinnedLocalRangeSource::new_anchored(root, LOCAL_COMPACT_V2_OBJECTS)
            .map_err(Error::RangeSource)?;
        let reader = ArchiveReader::open_pinned_with_schemas(
            pinned,
            ArchiveIdentity {
                cluster_id: descriptor.cluster_id.clone(),
                epoch: descriptor.epoch,
                generation_id: descriptor.candidate_id.clone(),
                first_slot: descriptor.first_slot,
                slots_per_epoch: descriptor.slots_per_epoch,
            },
            OpenOptions {
                epoch_first_slot: Some(descriptor.first_slot),
                ..OpenOptions::default()
            },
            CompactV2MessageSchema::Current,
            CompactV2MetadataSchema::CurrentTypedError,
        )
        .map_err(Error::Reader)?;
        let bound_source_size_bytes = reader
            .archive_descriptor()
            .expect("pinned local reader has an archive descriptor")
            .objects
            .iter()
            .try_fold(0_u64, |total, object| {
                total.checked_add(object.size).ok_or(Error::SizeOverflow)
            })?;
        let source =
            CompactV2InstructionSource::new(reader, descriptor.first_slot).map_err(Error::Query)?;
        let identity = source.identity().clone();

        Ok(Self {
            source: CompactV2ArchiveSource::Local(Box::new(source)),
            identity,
            bound_source_size_bytes,
            cache_root: PathBuf::new(),
            transport_kind: CompactV2TransportKind::LocalDirectory,
            candidate_id: Some(descriptor.candidate_id),
            registry_read_policy: CompactV2RegistryReadPolicy::with_full_registry_limit(
                DEFAULT_COMPACT_V2_FULL_REGISTRY_BYTES,
            ),
        })
    }

    /// Select a nonempty block-row range, capped by the available rows.
    pub fn bounded_range(&self, first_block: u32, max_blocks: NonZeroU32) -> Result<ScanRange> {
        bounded_range(self.identity().block_count, first_block, max_blocks)
    }

    /// Return the canonical slot plan in Compact block-row order.
    ///
    /// Cross-format demonstrations use this small fixed-width plan to prove
    /// that CAR and Indexer V3 scan the same block universe.
    pub fn canonical_slots(&self) -> Vec<u64> {
        match &self.source {
            CompactV2ArchiveSource::Network(source) => source
                .reader()
                .index()
                .rows
                .iter()
                .map(|row| row.slot)
                .collect(),
            CompactV2ArchiveSource::Local(source) => source
                .reader()
                .index()
                .rows
                .iter()
                .map(|row| row.slot)
                .collect(),
        }
    }

    /// Total bytes in the admitted format-defined object set.
    ///
    /// This does not include unrelated files in the physical epoch folder.
    pub const fn bound_source_size_bytes(&self) -> u64 {
        self.bound_source_size_bytes
    }

    /// Private generation-bound cache directory used by this reader.
    pub fn cache_root(&self) -> &Path {
        &self.cache_root
    }

    /// Operator-supplied candidate identity for a local source.
    pub fn candidate_id(&self) -> Option<&str> {
        self.candidate_id.as_deref()
    }

    pub const fn transport_kind(&self) -> CompactV2TransportKind {
        self.transport_kind
    }

    /// Automatic registry policy used by sequential scans.
    ///
    /// The default permits one complete registry of at most 1 GiB for a full
    /// scan or a partial scan with at least one million requested
    /// transactions. Smaller scans keep the bounded chunk cache. Set a
    /// zero-byte limit to disable complete-registry loading.
    pub const fn registry_read_policy(&self) -> CompactV2RegistryReadPolicy {
        self.registry_read_policy
    }

    /// Set the complete-registry memory limit for later sequential scans.
    pub fn set_full_registry_limit(&mut self, max_full_registry_bytes: u64) {
        self.registry_read_policy =
            CompactV2RegistryReadPolicy::with_full_registry_limit(max_full_registry_bytes);
        match &mut self.source {
            CompactV2ArchiveSource::Network(source) => {
                source.release_full_registry_above(max_full_registry_bytes);
            }
            CompactV2ArchiveSource::Local(source) => {
                source.release_full_registry_above(max_full_registry_bytes);
            }
        }
    }

    /// Release a complete registry image retained by an earlier dense scan.
    pub fn release_full_registry(&mut self) -> bool {
        match &mut self.source {
            CompactV2ArchiveSource::Network(source) => source.release_full_registry(),
            CompactV2ArchiveSource::Local(source) => source.release_full_registry(),
        }
    }

    /// Run the SDK's bounded borrowed-block pipeline with parallel decode and
    /// projection, then deliver canonical blocks in exact source order.
    ///
    /// The default worker count is the host's available logical CPU count.
    /// Set it explicitly for reproducible benchmarks. The parallel path is for
    /// requests with no instruction payload bytes, including the USDC,
    /// Pump.fun, and FireWatch reference workloads. Use `scan_ordered` when a
    /// request needs exact instruction payload reconstruction.
    ///
    /// A full scan shares one complete registry across workers when it fits the
    /// caller's configured byte limit. A partial scan uses the same path at one
    /// million requested transactions. Smaller ranges use bounded worker-local
    /// sparse caches. Network setup has already persisted the complete
    /// ETag-bound registry before this policy reads it into shared memory.
    pub fn scan_ordered_parallel(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
        config: CompactV2ParallelScanConfig,
    ) -> QueryResult<CompactV2ParallelScanReceipt> {
        let config = parallel_config_for_admitted_transport(self.transport_kind, config);
        match &mut self.source {
            CompactV2ArchiveSource::Network(source) => {
                source.scan_ordered_parallel(request, sink, config)
            }
            CompactV2ArchiveSource::Local(source) => {
                source.scan_ordered_parallel(request, sink, config)
            }
        }
    }

    /// Current normalized network and persistent-cache counters.
    pub fn io_snapshot(&self) -> ArchiveIoSnapshot {
        self.transport_snapshot().http_and_cache
    }

    /// Current transport counters, including direct local reads.
    pub fn transport_snapshot(&self) -> CompactV2TransportReceipt {
        match &self.source {
            CompactV2ArchiveSource::Network(source) => CompactV2TransportReceipt {
                kind: self.transport_kind,
                http_and_cache: io_snapshot(&source.reader().source().cache),
                local_read_calls: 0,
                local_read_bytes: 0,
            },
            CompactV2ArchiveSource::Local(source) => {
                let local = source.reader().source().stats();
                CompactV2TransportReceipt {
                    kind: self.transport_kind,
                    http_and_cache: ArchiveIoSnapshot::default(),
                    local_read_calls: local.read_calls,
                    local_read_bytes: local.read_bytes,
                }
            }
        }
    }

    /// Check that all opened local files still have their pinned identities.
    pub fn verify_local_unchanged(&self) -> Result<()> {
        if let CompactV2ArchiveSource::Local(source) = &self.source {
            source
                .reader()
                .source()
                .verify_unchanged()
                .map_err(Error::RangeSource)?;
        }
        Ok(())
    }

    /// Consume the reader and return its final normalized I/O counters.
    pub fn finish_io(self) -> ArchiveIoSnapshot {
        self.finish_transport_io().http_and_cache
    }

    /// Consume the reader and return final HTTP, cache, and local counters.
    pub fn finish_transport_io(self) -> CompactV2TransportReceipt {
        let snapshot = self.transport_snapshot();
        drop(self.source);
        snapshot
    }
}

fn parallel_config_for_admitted_transport(
    _transport: CompactV2TransportKind,
    config: CompactV2ParallelScanConfig,
) -> CompactV2ParallelScanConfig {
    // Network setup persists the strong-ETag-bound registry before the scan.
    // The caller's dense-memory limit is valid for both admitted modes.
    config
}

impl ArchiveInstructionSource for CompactV2Archive {
    fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> QueryResult<ScanReceipt> {
        match &mut self.source {
            CompactV2ArchiveSource::Network(source) => {
                source.scan_ordered_with_registry_policy(request, self.registry_read_policy, sink)
            }
            CompactV2ArchiveSource::Local(source) => {
                source.scan_ordered_with_registry_policy(request, self.registry_read_policy, sink)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerEndpoint {
    normalized_origin: String,
    compact_v2_base: String,
}

impl WorkerEndpoint {
    fn parse(origin: &str, allow_insecure_http: bool) -> Result<Self> {
        let parsed = Url::parse(origin)
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

        let normalized_origin = parsed.as_str().trim_end_matches('/').to_owned();
        Ok(Self {
            compact_v2_base: format!("{normalized_origin}/compact-v2"),
            normalized_origin,
        })
    }
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

fn cache_objects(inventory: &NetworkObjectInventory, epoch: u64) -> Result<Vec<&'static str>> {
    let mut names = vec![
        blockzilla_format::ARCHIVE_V2_BLOCK_INDEX_FILE,
        blockzilla_format::ARCHIVE_V2_META_FILE,
        blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ];
    if epoch == 0
        && inventory
            .identity(blockzilla_format::ARCHIVE_V2_GENESIS_BIN_FILE)
            .is_some()
    {
        names.push(blockzilla_format::ARCHIVE_V2_GENESIS_BIN_FILE);
    }
    for &name in &names {
        if inventory.identity(name).is_none() {
            return Err(Error::Geometry(format!(
                "required Compact V2 object {name} is absent"
            )));
        }
    }
    Ok(names)
}

fn mainnet_first_slot(epoch: u64) -> Result<u64> {
    epoch
        .checked_mul(MAINNET_ARCHIVE_SLOTS_PER_EPOCH)
        .ok_or_else(|| Error::Geometry("mainnet epoch first slot overflows u64".into()))
}

fn cache_namespace(origin: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(CACHE_NAMESPACE_DOMAIN);
    digest.update((origin.len() as u64).to_le_bytes());
    digest.update(origin.as_bytes());
    let value = digest.finalize();
    format!("origin-{}", hex_lower(&value[..16]))
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

fn io_snapshot(cache: &CachedHttpRangeSource) -> ArchiveIoSnapshot {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(length: u64, etag: &str) -> HttpObjectIdentity {
        HttpObjectIdentity {
            length,
            strong_etag: etag.into(),
        }
    }

    fn inventory(entries: &[(&str, HttpObjectIdentity)]) -> NetworkObjectInventory {
        NetworkObjectInventory {
            objects: entries
                .iter()
                .map(|(name, identity)| ((*name).to_owned(), identity.clone()))
                .collect(),
        }
    }

    #[test]
    fn derives_only_the_public_compact_route() {
        let endpoint = WorkerEndpoint::parse("https://example.test/", false).unwrap();
        assert_eq!(endpoint.normalized_origin, "https://example.test");
        assert_eq!(endpoint.compact_v2_base, "https://example.test/compact-v2");

        for invalid in [
            "http://example.test",
            "https://user@example.test",
            "https://example.test/path",
            "https://example.test?query=1",
            "file:///tmp/archive",
        ] {
            assert!(WorkerEndpoint::parse(invalid, false).is_err(), "{invalid}");
        }
        assert!(WorkerEndpoint::parse("http://127.0.0.1:1234", true).is_ok());
    }

    #[test]
    fn network_options_do_not_select_a_wire_grammar() {
        let current = CompactV2OpenOptions::default();
        assert!(!current.allow_insecure_http);
    }

    #[test]
    fn range_is_bounded_by_available_rows() {
        let range = bounded_range(10, 8, NonZeroU32::new(1024).unwrap()).unwrap();
        assert_eq!(range.first_block, 8);
        assert_eq!(range.block_count.get(), 2);
        assert!(bounded_range(10, 10, NonZeroU32::new(1).unwrap()).is_err());
    }

    #[test]
    fn first_slot_uses_fixed_mainnet_archive_windows() {
        assert_eq!(mainnet_first_slot(0).unwrap(), 0);
        assert_eq!(mainnet_first_slot(185).unwrap(), 79_920_000);
        assert!(mainnet_first_slot(u64::MAX).is_err());
    }

    #[test]
    fn object_set_id_uses_names_lengths_and_strong_etags() {
        let first = inventory(&[
            ("a.bin", identity(10, "\"a\"")),
            ("b.bin", identity(20, "\"b\"")),
        ]);
        let reordered = inventory(&[
            ("b.bin", identity(20, "\"b\"")),
            ("a.bin", identity(10, "\"a\"")),
        ]);
        let replaced = inventory(&[
            ("a.bin", identity(10, "\"different\"")),
            ("b.bin", identity(20, "\"b\"")),
        ]);
        assert_eq!(first.object_set_id(900), reordered.object_set_id(900));
        assert_ne!(first.object_set_id(900), replaced.object_set_id(900));
        assert_ne!(first.object_set_id(900), first.object_set_id(901));
        assert!(first.object_set_id(900).starts_with("etag-set-"));
    }

    #[test]
    fn network_inventory_contains_only_format_defined_objects() {
        let mut names = REQUIRED_COMPACT_V2_OBJECTS
            .iter()
            .chain(OPTIONAL_COMPACT_V2_OBJECTS)
            .copied()
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            REQUIRED_COMPACT_V2_OBJECTS.len() + OPTIONAL_COMPACT_V2_OBJECTS.len()
        );
        assert!(!names.contains(&"archive-v2-generation.json"));
    }

    #[test]
    fn cache_namespace_binds_the_origin() {
        let first = cache_namespace("https://one.example");
        assert_eq!(first, cache_namespace("https://one.example"));
        assert_ne!(first, cache_namespace("https://two.example"));
        assert!(first.starts_with("origin-"));
    }

    #[test]
    fn cached_network_scan_preserves_the_callers_shared_registry_limit() {
        let config = CompactV2ParallelScanConfig::new(12).with_full_registry_limit(900_000_000);
        assert_eq!(
            parallel_config_for_admitted_transport(CompactV2TransportKind::Https, config),
            config
        );
        assert_eq!(
            parallel_config_for_admitted_transport(CompactV2TransportKind::LocalDirectory, config),
            config
        );
    }

    #[test]
    fn strict_cache_root_is_private_and_absolute() {
        let temporary = tempfile::tempdir().unwrap();
        let cache = temporary
            .path()
            .canonicalize()
            .unwrap()
            .join("compact/cache");
        create_http_cache_directory(&cache).unwrap();
        assert!(cache.is_dir());
    }

    #[test]
    fn local_descriptor_is_explicit_and_current_schema() {
        let descriptor = CompactV2LocalDescriptor::mainnet(900, "epoch-900-corrected-v2").unwrap();
        assert_eq!(descriptor.first_slot, 388_800_000);
        assert_eq!(descriptor.slots_per_epoch, 432_000);
        assert!(CompactV2LocalDescriptor::mainnet(900, "bad candidate").is_err());
    }

    #[test]
    fn local_open_uses_the_format_defined_object_set() {
        let temporary = tempfile::tempdir().unwrap();
        let descriptor = CompactV2LocalDescriptor::mainnet(900, "epoch-900-corrected-v2").unwrap();
        let error = match CompactV2Archive::open_local(temporary.path(), descriptor) {
            Ok(_) => panic!("empty local root was admitted"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            Error::Reader(crate::Error::MissingLocalFile(name))
                if name == blockzilla_format::ARCHIVE_V2_BLOCKS_FILE
        ));
    }
}
