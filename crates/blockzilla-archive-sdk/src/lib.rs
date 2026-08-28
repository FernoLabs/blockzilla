//! High-level constructors for the Blockzilla archive query adapters.
//!
//! [`NetworkEpoch`] owns transport, immutable-object binding, cache setup, and
//! the canonical block plan. Applications select an explicit format and then
//! use the source-neutral [`ArchiveInstructionSource`] interface.

use std::{
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use blockzilla_firebase_indexer::{
    INDEXER_V3_OPTIONAL_RETAINED_SIDECARS, INDEXER_V3_REQUIRED_RETAINED_SIDECARS,
    IndexerV3InstructionSource, IndexerV3InstructionSourceError,
    indexer_v3_required_ledger_objects,
};
pub use blockzilla_query_sdk::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveInstructionSourceExt, BlockSink, BlockView,
    ScanIoReceipt, ScanRange, ScanReceipt, ScanRequest, SourceIdentity, SourceVerification,
    TransactionView,
};
use blockzilla_read_sdk::{
    ArchiveReader, CachedHttpRangeSource, CompactV2InstructionSource,
    CompactV2InstructionSourceError, HashVerification, HttpRangeSource, HttpRangeSourceOptions,
    OpenOptions, RangeSource, SourceError, create_http_cache_directory,
    manifest::{
        BLOCK_INDEX_FILE, GENERATION_MANIFEST_FILE, GENESIS_BIN_FILE, GenerationManifest,
        META_FILE, REGISTRY_FILE,
    },
};
use of_car_reader::{
    query_sdk::{CanonicalBlockPlan, CarInstructionSource, CarQueryError, CarQueryLimits},
    query_sdk_http::{
        CarHttpError, CarHttpOptions, CarHttpStats, CarHttpStatsHandle, CarHttpStream,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solana_epoch_schedule::EpochSchedule;
use thiserror::Error;
use url::Url;

const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
const V3_BINDING_DOMAIN: &[u8] = b"blockzilla.indexer-v3-network-candidate.v1\0";
const CACHE_NAMESPACE_DOMAIN: &[u8] = b"blockzilla.archive-sdk.cache-namespace.v1\0";

/// Stable order used by the public three-format Worker demonstration.
pub const WORKER_FORMATS: [ArchiveFormat; 3] = [
    ArchiveFormat::CompactV2,
    ArchiveFormat::Car,
    ArchiveFormat::IndexerV3,
];

/// Options for the high-level network constructors.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NetworkEpochOptions {
    /// Permit cleartext HTTP for a controlled local fixture.
    pub allow_insecure_http: bool,
}

/// Normalized transport and persistent-cache counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveIoSnapshot {
    pub head_requests: u64,
    pub get_requests: u64,
    pub network_body_bytes: u64,
    pub cache_hits: u64,
    pub cache_downloads: u64,
    pub cache_read_calls: u64,
    pub cache_read_bytes: u64,
}

impl ArchiveIoSnapshot {
    /// Return a monotonic interval without allowing a counter underflow.
    pub const fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            head_requests: self.head_requests.saturating_sub(earlier.head_requests),
            get_requests: self.get_requests.saturating_sub(earlier.get_requests),
            network_body_bytes: self
                .network_body_bytes
                .saturating_sub(earlier.network_body_bytes),
            cache_hits: self.cache_hits.saturating_sub(earlier.cache_hits),
            cache_downloads: self.cache_downloads.saturating_sub(earlier.cache_downloads),
            cache_read_calls: self
                .cache_read_calls
                .saturating_sub(earlier.cache_read_calls),
            cache_read_bytes: self
                .cache_read_bytes
                .saturating_sub(earlier.cache_read_bytes),
        }
    }
}

/// Source setup work completed before an application starts its scan timer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveOpenReceipt {
    pub setup_wall_ns: u64,
    /// Total immutable source bytes when the format has a bounded object set.
    pub source_size_bytes: Option<u64>,
    pub cache_root: Option<PathBuf>,
    pub io: ArchiveIoSnapshot,
}

/// Errors from high-level archive setup and source selection.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid Worker origin: {0}")]
    InvalidOrigin(String),
    #[error("invalid cache root: {0}")]
    InvalidCacheRoot(String),
    #[error("invalid archive range: {0}")]
    InvalidRange(String),
    #[error("archive source geometry differs: {0}")]
    Geometry(String),
    #[error("archive source identity differs: {0}")]
    Identity(String),
    #[error("{0} was already opened from this network epoch")]
    AlreadyOpened(ArchiveFormat),
    #[error("archive range source failed")]
    RangeSource(#[source] SourceError),
    #[error("Compact V2 reader failed")]
    CompactReader(#[source] blockzilla_read_sdk::Error),
    #[error("Compact V2 query adapter failed")]
    CompactQuery(#[source] CompactV2InstructionSourceError),
    #[error("CAR HTTP source failed")]
    CarHttp(#[source] CarHttpError),
    #[error("CAR query adapter failed")]
    CarQuery(#[source] CarQueryError),
    #[error("Indexer V3 query adapter failed")]
    IndexerV3(#[source] IndexerV3InstructionSourceError),
    #[error("archive size arithmetic overflow")]
    SizeOverflow,
}

pub type Result<T> = std::result::Result<T, Error>;

/// Create or verify one private absolute directory without following a
/// symbolic link. The function rejects parent traversal before it writes.
pub fn create_private_directory(path: impl AsRef<Path>) -> Result<()> {
    create_http_cache_directory(path).map_err(Error::RangeSource)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerPaths {
    normalized_origin: String,
    car: String,
    compact_v2: String,
    indexer_v3: String,
}

impl WorkerPaths {
    fn parse(origin: &str, epoch: u64, allow_insecure_http: bool) -> Result<Self> {
        let parsed = Url::parse(origin)
            .map_err(|error| Error::InvalidOrigin(format!("URL parse failed: {error}")))?;
        match parsed.scheme() {
            "https" => {}
            "http" if allow_insecure_http => {}
            "http" => {
                return Err(Error::InvalidOrigin("cleartext HTTP is disabled".into()));
            }
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
            car: format!("{normalized_origin}/car/{epoch}/epoch-{epoch}.car"),
            compact_v2: format!("{normalized_origin}/compact-v2"),
            indexer_v3: format!("{normalized_origin}/indexer-v3/"),
            normalized_origin,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct OpenState {
    car: bool,
    compact_v2: bool,
    indexer_v3: bool,
}

impl OpenState {
    const fn is_open(self, format: ArchiveFormat) -> bool {
        match format {
            ArchiveFormat::Car => self.car,
            ArchiveFormat::CompactV2 => self.compact_v2,
            ArchiveFormat::IndexerV3 => self.indexer_v3,
        }
    }

    fn mark_open(&mut self, format: ArchiveFormat) {
        match format {
            ArchiveFormat::Car => self.car = true,
            ArchiveFormat::CompactV2 => self.compact_v2 = true,
            ArchiveFormat::IndexerV3 => self.indexer_v3 = true,
        }
    }
}

/// One admitted mainnet-beta network epoch and its published Compact V2 block
/// universe.
///
/// CAR and Indexer V3 stay unopened until [`Self::open_source`] selects them.
/// Each format can be opened once from this value.
pub struct NetworkEpoch {
    paths: WorkerPaths,
    epoch: u64,
    allow_insecure_http: bool,
    cache_root: PathBuf,
    compact_reader: Option<ArchiveReader<CachedHttpRangeSource>>,
    reference_identity: SourceIdentity,
    canonical_slots: Vec<u64>,
    compact_open_receipt: ArchiveOpenReceipt,
    open_state: OpenState,
}

impl NetworkEpoch {
    /// Open and verify the published mainnet-beta Compact V2 reference for one
    /// Worker epoch.
    pub fn open(origin: &str, epoch: u64, cache_root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(origin, epoch, cache_root, NetworkEpochOptions::default())
    }

    /// Open with an explicit local-fixture transport policy.
    pub fn open_with_options(
        origin: &str,
        epoch: u64,
        cache_root: impl AsRef<Path>,
        options: NetworkEpochOptions,
    ) -> Result<Self> {
        let paths = WorkerPaths::parse(origin, epoch, options.allow_insecure_http)?;
        let cache_root = cache_root.as_ref().to_path_buf();
        if cache_root.as_os_str().is_empty() {
            return Err(Error::InvalidCacheRoot("path is empty".into()));
        }
        create_private_directory(&cache_root)?;

        let started = Instant::now();
        let http_options = HttpRangeSourceOptions {
            allow_insecure_http: options.allow_insecure_http,
            ..HttpRangeSourceOptions::default()
        };
        let http = HttpRangeSource::with_options(&paths.compact_v2, epoch, None, http_options)
            .map_err(Error::RangeSource)?;
        let manifest_bytes = http
            .read_all_bounded(GENERATION_MANIFEST_FILE, MAX_MANIFEST_BYTES)
            .map_err(Error::RangeSource)?;
        let manifest = GenerationManifest::parse(&manifest_bytes).map_err(Error::CompactReader)?;
        if manifest.epoch != epoch {
            return Err(Error::Geometry(format!(
                "Compact V2 manifest epoch {} differs from requested epoch {epoch}",
                manifest.epoch
            )));
        }
        let first_slot = resolve_first_slot(&manifest)?;

        let generation_cache = cache_root
            .join(cache_namespace(&paths.normalized_origin))
            .join("compact-v2")
            .join(format!("epoch-{epoch}"))
            .join(&manifest.generation_digest);
        create_private_directory(&generation_cache)?;
        let cache_names = compact_cache_objects(&manifest)?;
        let cache = CachedHttpRangeSource::open(http, &generation_cache, &cache_names)
            .map_err(Error::RangeSource)?;
        let reader = ArchiveReader::open_with_options(
            cache.clone(),
            OpenOptions {
                hash_verification: HashVerification::ControlFiles,
                epoch_first_slot: Some(first_slot),
                ..OpenOptions::default()
            },
        )
        .map_err(Error::CompactReader)?;
        let compact_source =
            CompactV2InstructionSource::new(reader, first_slot).map_err(Error::CompactQuery)?;
        let reference_identity = compact_source.identity().clone();
        let reader = compact_source.into_reader();
        let canonical_slots = reader.index().rows.iter().map(|row| row.slot).collect();
        let source_size_bytes = compact_source_size(&manifest, manifest_bytes.len())?;
        let io = range_io_snapshot(reader.source());
        let compact_open_receipt = ArchiveOpenReceipt {
            setup_wall_ns: duration_ns(started.elapsed()),
            source_size_bytes: Some(source_size_bytes),
            cache_root: Some(generation_cache),
            io,
        };

        Ok(Self {
            paths,
            epoch,
            allow_insecure_http: options.allow_insecure_http,
            cache_root,
            compact_reader: Some(reader),
            reference_identity,
            canonical_slots,
            compact_open_receipt,
            open_state: OpenState::default(),
        })
    }

    /// Published Compact V2 identity used for epoch geometry and CAR rows.
    pub const fn reference_identity(&self) -> &SourceIdentity {
        &self.reference_identity
    }

    /// Select a nonempty bounded range from the published block universe.
    pub fn bounded_range(&self, first_block: u32, max_blocks: NonZeroU32) -> Result<ScanRange> {
        if first_block >= self.reference_identity.block_count {
            return Err(Error::InvalidRange(format!(
                "first block {first_block} is outside block rows 0..{}",
                self.reference_identity.block_count
            )));
        }
        let available = self.reference_identity.block_count - first_block;
        let block_count = NonZeroU32::new(available.min(max_blocks.get()))
            .ok_or_else(|| Error::InvalidRange("selected range is empty".into()))?;
        Ok(ScanRange {
            first_block,
            block_count,
        })
    }

    /// Open one explicit format. The returned source has full source geometry.
    pub fn open_source(&mut self, format: ArchiveFormat) -> Result<ArchiveSource> {
        self.open_source_inner(format, None)
    }

    /// Open one explicit format and require it to cover `required_range`.
    pub fn open_source_for(
        &mut self,
        format: ArchiveFormat,
        required_range: ScanRange,
    ) -> Result<ArchiveSource> {
        self.open_source_inner(format, Some(required_range))
    }

    fn open_source_inner(
        &mut self,
        format: ArchiveFormat,
        required_range: Option<ScanRange>,
    ) -> Result<ArchiveSource> {
        if self.open_state.is_open(format) {
            return Err(Error::AlreadyOpened(format));
        }
        if let Some(range) = required_range {
            validate_required_range(&self.reference_identity, range)?;
        }
        let source = match format {
            ArchiveFormat::CompactV2 => self.open_compact(required_range)?,
            ArchiveFormat::Car => self.open_car(required_range)?,
            ArchiveFormat::IndexerV3 => self.open_indexer_v3(required_range)?,
        };
        self.open_state.mark_open(format);
        Ok(source)
    }

    fn open_compact(&mut self, required_range: Option<ScanRange>) -> Result<ArchiveSource> {
        let reader = self
            .compact_reader
            .take()
            .ok_or(Error::AlreadyOpened(ArchiveFormat::CompactV2))?;
        let first_slot = self.reference_identity.first_slot;
        let source =
            CompactV2InstructionSource::new(reader, first_slot).map_err(Error::CompactQuery)?;
        validate_source(
            source.identity(),
            ArchiveFormat::CompactV2,
            &self.reference_identity,
            required_range,
        )?;
        Ok(ArchiveSource {
            inner: ArchiveSourceInner::CompactV2(Box::new(source)),
            open_receipt: self.compact_open_receipt.clone(),
        })
    }

    fn open_car(&self, required_range: Option<ScanRange>) -> Result<ArchiveSource> {
        let started = Instant::now();
        let options = CarHttpOptions {
            allow_http: self.allow_insecure_http,
            ..CarHttpOptions::default()
        };
        let stream = CarHttpStream::open(&self.paths.car, options).map_err(Error::CarHttp)?;
        let object = stream.identity().clone();
        let stats = stream.stats_handle();
        let identity = car_identity(
            &self.reference_identity,
            format!("epoch-{}.car", self.epoch),
            object.object_binding,
            &self.canonical_slots,
        )?;
        let source = CarInstructionSource::new(
            stream,
            identity,
            CanonicalBlockPlan::new(self.canonical_slots.clone()),
            CarQueryLimits::default(),
        )
        .map_err(Error::CarQuery)?;
        validate_source(
            source.identity(),
            ArchiveFormat::Car,
            &self.reference_identity,
            required_range,
        )?;
        let open_receipt = ArchiveOpenReceipt {
            setup_wall_ns: duration_ns(started.elapsed()),
            source_size_bytes: Some(object.content_length),
            cache_root: None,
            io: car_io_snapshot(stats.snapshot()),
        };
        Ok(ArchiveSource {
            inner: ArchiveSourceInner::Car {
                source: Box::new(source),
                stats,
            },
            open_receipt,
        })
    }

    fn open_indexer_v3(&self, required_range: Option<ScanRange>) -> Result<ArchiveSource> {
        let started = Instant::now();
        let http = HttpRangeSource::with_options(
            &self.paths.indexer_v3,
            self.epoch,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: self.allow_insecure_http,
                ..HttpRangeSourceOptions::default()
            },
        )
        .map_err(Error::RangeSource)?;
        let required_ledger = indexer_v3_required_ledger_objects().collect::<Vec<_>>();
        if required_ledger.len() < 2 {
            return Err(Error::Geometry(
                "Indexer V3 did not expose its index and transaction directory".into(),
            ));
        }

        let mut identity_names = required_ledger.clone();
        identity_names.extend(INDEXER_V3_REQUIRED_RETAINED_SIDECARS);
        let mut bound_objects =
            Vec::with_capacity(identity_names.len() + INDEXER_V3_OPTIONAL_RETAINED_SIDECARS.len());
        for name in identity_names {
            bound_objects.push(bind_http_object(&http, name)?);
        }
        for name in INDEXER_V3_OPTIONAL_RETAINED_SIDECARS {
            if http.size(name).map_err(Error::RangeSource)?.is_some() {
                bound_objects.push(bind_http_object(&http, name)?);
            }
        }
        let candidate = candidate_binding(http.base_url().as_str(), self.epoch, &bound_objects);
        let candidate_cache = self
            .cache_root
            .join(cache_namespace(&self.paths.normalized_origin))
            .join("indexer-v3")
            .join(format!("epoch-{}", self.epoch))
            .join(&candidate);
        create_private_directory(&candidate_cache)?;

        // Cache only the bounded block index and small required retained
        // sidecars. The per-transaction directory and signatures can exceed
        // the cache's per-object limit and remain pinned range reads.
        let mut cache_names = vec![required_ledger[0]];
        cache_names.extend(INDEXER_V3_REQUIRED_RETAINED_SIDECARS);
        cache_names.sort_unstable();
        cache_names.dedup();
        let cache = CachedHttpRangeSource::open(http, &candidate_cache, &cache_names)
            .map_err(Error::RangeSource)?;
        let shared: Arc<dyn RangeSource> = Arc::new(cache.clone());
        let source = IndexerV3InstructionSource::open_with_source(
            shared,
            self.paths.indexer_v3.clone(),
            self.reference_identity.first_slot,
            candidate,
        )
        .map_err(Error::IndexerV3)?;
        validate_slot_plan(&source, &self.canonical_slots)?;
        validate_source(
            source.identity(),
            ArchiveFormat::IndexerV3,
            &self.reference_identity,
            required_range,
        )?;
        let source_size_bytes = bound_objects.iter().try_fold(0_u64, |total, object| {
            total.checked_add(object.length).ok_or(Error::SizeOverflow)
        })?;
        let open_receipt = ArchiveOpenReceipt {
            setup_wall_ns: duration_ns(started.elapsed()),
            source_size_bytes: Some(source_size_bytes),
            cache_root: Some(candidate_cache),
            io: range_io_snapshot(&cache),
        };
        Ok(ArchiveSource {
            inner: ArchiveSourceInner::IndexerV3 {
                source: Box::new(source),
                cache: Box::new(cache),
            },
            open_receipt,
        })
    }
}

/// Runtime-selected source with no public format-specific variants.
pub struct ArchiveSource {
    inner: ArchiveSourceInner,
    open_receipt: ArchiveOpenReceipt,
}

enum ArchiveSourceInner {
    Car {
        source: Box<CarInstructionSource<CarHttpStream>>,
        stats: CarHttpStatsHandle,
    },
    CompactV2(Box<CompactV2InstructionSource<CachedHttpRangeSource>>),
    IndexerV3 {
        source: Box<IndexerV3InstructionSource>,
        cache: Box<CachedHttpRangeSource>,
    },
}

impl ArchiveSource {
    /// Setup receipt recorded by the high-level constructor.
    pub const fn open_receipt(&self) -> &ArchiveOpenReceipt {
        &self.open_receipt
    }

    /// Current normalized transport and cache counters.
    pub fn io_snapshot(&self) -> ArchiveIoSnapshot {
        match &self.inner {
            ArchiveSourceInner::Car { stats, .. } => car_io_snapshot(stats.snapshot()),
            ArchiveSourceInner::CompactV2(source) => range_io_snapshot(source.reader().source()),
            ArchiveSourceInner::IndexerV3 { cache, .. } => range_io_snapshot(cache),
        }
    }

    /// Consume the source, wait for background transport work to stop, and
    /// return the final counters. Benchmarks must use this value for totals.
    pub fn finish_io(self) -> ArchiveIoSnapshot {
        match self.inner {
            ArchiveSourceInner::Car { source, stats } => {
                drop(source);
                car_io_snapshot(stats.snapshot())
            }
            ArchiveSourceInner::CompactV2(source) => {
                let snapshot = range_io_snapshot(source.reader().source());
                drop(source);
                snapshot
            }
            ArchiveSourceInner::IndexerV3 { source, cache } => {
                drop(source);
                let snapshot = range_io_snapshot(&cache);
                drop(cache);
                snapshot
            }
        }
    }
}

impl ArchiveInstructionSource for ArchiveSource {
    fn identity(&self) -> &SourceIdentity {
        match &self.inner {
            ArchiveSourceInner::Car { source, .. } => source.identity(),
            ArchiveSourceInner::CompactV2(source) => source.identity(),
            ArchiveSourceInner::IndexerV3 { source, .. } => source.identity(),
        }
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_query_sdk::Result<ScanReceipt> {
        match &mut self.inner {
            ArchiveSourceInner::Car { source, .. } => source.scan_ordered(request, sink),
            ArchiveSourceInner::CompactV2(source) => source.scan_ordered(request, sink),
            ArchiveSourceInner::IndexerV3 { source, .. } => source.scan_ordered(request, sink),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundHttpObject {
    name: String,
    length: u64,
    strong_etag: String,
}

fn compact_cache_objects(manifest: &GenerationManifest) -> Result<Vec<&'static str>> {
    for name in [
        BLOCK_INDEX_FILE,
        META_FILE,
        REGISTRY_FILE,
        blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ] {
        manifest.required_file(name).map_err(Error::CompactReader)?;
    }
    let mut names = vec![
        GENERATION_MANIFEST_FILE,
        BLOCK_INDEX_FILE,
        META_FILE,
        REGISTRY_FILE,
        blockzilla_format::ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ];
    for name in [
        GENESIS_BIN_FILE,
        blockzilla_read_sdk::COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
        blockzilla_read_sdk::COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
        blockzilla_read_sdk::COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE,
    ] {
        if manifest.file(name).is_some() {
            names.push(name);
        }
    }
    names.sort_unstable();
    names.dedup();
    Ok(names)
}

fn compact_source_size(manifest: &GenerationManifest, manifest_bytes: usize) -> Result<u64> {
    manifest.files.iter().try_fold(
        u64::try_from(manifest_bytes).map_err(|_| Error::SizeOverflow)?,
        |total, file| total.checked_add(file.size).ok_or(Error::SizeOverflow),
    )
}

fn resolve_first_slot(manifest: &GenerationManifest) -> Result<u64> {
    if manifest.cluster_id != "mainnet-beta" {
        return Err(Error::Geometry(format!(
            "the high-level network facade supports mainnet-beta; cluster {} needs a direct adapter with an explicit first slot",
            manifest.cluster_id
        )));
    }
    let schedule = EpochSchedule::default();
    if manifest.slots_per_epoch != schedule.slots_per_epoch {
        return Err(Error::Geometry(format!(
            "mainnet-beta manifest has {} slots per epoch, expected {}",
            manifest.slots_per_epoch, schedule.slots_per_epoch
        )));
    }
    Ok(schedule.get_first_slot_in_epoch(manifest.epoch))
}

fn validate_slot_plan(source: &IndexerV3InstructionSource, canonical_slots: &[u64]) -> Result<()> {
    let block_count = source.identity().block_count;
    validate_slot_plan_with(block_count, canonical_slots, |ordinal| {
        source.block_slot(ordinal)
    })
}

fn validate_slot_plan_with(
    block_count: u32,
    canonical_slots: &[u64],
    mut slot_at: impl FnMut(u32) -> Option<u64>,
) -> Result<()> {
    let candidate_len = usize::try_from(block_count).map_err(|_| Error::SizeOverflow)?;
    if candidate_len > canonical_slots.len() {
        return Err(Error::Geometry(format!(
            "Indexer V3 has {candidate_len} block rows, above the Compact V2 plan length {}",
            canonical_slots.len()
        )));
    }
    for ordinal in 0..block_count {
        let candidate = slot_at(ordinal).ok_or_else(|| {
            Error::Geometry(format!("Indexer V3 block row {ordinal} is unavailable"))
        })?;
        let expected = canonical_slots[ordinal as usize];
        if candidate != expected {
            return Err(Error::Geometry(format!(
                "Indexer V3 block row {ordinal} has slot {candidate}, expected Compact V2 slot {expected}"
            )));
        }
    }
    Ok(())
}

fn car_identity(
    reference: &SourceIdentity,
    label: String,
    object_binding: String,
    canonical_slots: &[u64],
) -> Result<SourceIdentity> {
    let block_count = u32::try_from(canonical_slots.len()).map_err(|_| Error::SizeOverflow)?;
    Ok(SourceIdentity {
        format: ArchiveFormat::Car,
        label,
        cluster_id: reference.cluster_id.clone(),
        epoch: reference.epoch,
        first_slot: reference.first_slot,
        slots_per_epoch: reference.slots_per_epoch,
        block_count,
        verification: SourceVerification::OperatorTrusted,
        binding: Some(object_binding),
    })
}

fn validate_source(
    candidate: &SourceIdentity,
    expected_format: ArchiveFormat,
    reference: &SourceIdentity,
    required_range: Option<ScanRange>,
) -> Result<()> {
    if candidate.format != expected_format {
        return Err(Error::Identity(format!(
            "opened {} through the {expected_format} constructor",
            candidate.format
        )));
    }
    let expected_verification = match expected_format {
        ArchiveFormat::Car => SourceVerification::OperatorTrusted,
        ArchiveFormat::CompactV2 => SourceVerification::PublishedManifest,
        ArchiveFormat::IndexerV3 => SourceVerification::InternalBindingOnly,
    };
    if candidate.verification != expected_verification {
        return Err(Error::Identity(format!(
            "{expected_format} verification is {:?}, expected {expected_verification:?}",
            candidate.verification
        )));
    }
    if candidate.binding.as_deref().is_none_or(str::is_empty) {
        return Err(Error::Identity(format!(
            "{expected_format} has no stable source binding"
        )));
    }
    validate_geometry(candidate, reference, required_range)
}

fn validate_geometry(
    candidate: &SourceIdentity,
    reference: &SourceIdentity,
    required_range: Option<ScanRange>,
) -> Result<()> {
    if candidate.epoch != reference.epoch {
        return Err(Error::Geometry(format!(
            "{} epoch {} differs from reference epoch {}",
            candidate.format, candidate.epoch, reference.epoch
        )));
    }
    if candidate.first_slot != reference.first_slot {
        return Err(Error::Geometry(format!(
            "{} first slot {} differs from reference first slot {}",
            candidate.format, candidate.first_slot, reference.first_slot
        )));
    }
    if candidate.slots_per_epoch != reference.slots_per_epoch {
        return Err(Error::Geometry(format!(
            "{} slots per epoch {} differs from reference {}",
            candidate.format, candidate.slots_per_epoch, reference.slots_per_epoch
        )));
    }
    if let Some(range) = required_range {
        validate_required_range(candidate, range)?;
    }
    Ok(())
}

fn validate_required_range(identity: &SourceIdentity, range: ScanRange) -> Result<()> {
    let end = range
        .first_block
        .checked_add(range.block_count.get())
        .ok_or_else(|| Error::InvalidRange("block range overflows u32".into()))?;
    if end > identity.block_count {
        return Err(Error::InvalidRange(format!(
            "{} has {} block rows, below required end {end}",
            identity.format, identity.block_count
        )));
    }
    Ok(())
}

fn bind_http_object(http: &HttpRangeSource, name: &str) -> Result<BoundHttpObject> {
    let identity = http.strong_identity(name).map_err(Error::RangeSource)?;
    Ok(BoundHttpObject {
        name: name.to_owned(),
        length: identity.length,
        strong_etag: identity.strong_etag,
    })
}

fn candidate_binding(base_url: &str, epoch: u64, objects: &[BoundHttpObject]) -> String {
    let mut ordered = objects.to_vec();
    ordered.sort_unstable_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
    let mut digest = Sha256::new();
    digest.update(V3_BINDING_DOMAIN);
    hash_bytes(&mut digest, base_url.as_bytes());
    digest.update(epoch.to_le_bytes());
    digest.update((ordered.len() as u64).to_le_bytes());
    for object in ordered {
        hash_bytes(&mut digest, object.name.as_bytes());
        digest.update(object.length.to_le_bytes());
        hash_bytes(&mut digest, object.strong_etag.as_bytes());
    }
    hex_lower(&digest.finalize())
}

fn cache_namespace(origin: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(CACHE_NAMESPACE_DOMAIN);
    hash_bytes(&mut digest, origin.as_bytes());
    let value = digest.finalize();
    format!("origin-{}", hex_lower(&value[..16]))
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

fn range_io_snapshot(cache: &CachedHttpRangeSource) -> ArchiveIoSnapshot {
    let http = cache.http().stats();
    let cache = cache.stats();
    ArchiveIoSnapshot {
        head_requests: http.head_requests,
        get_requests: http.get_requests,
        network_body_bytes: http.returned_body_bytes,
        cache_hits: cache.cache_hits,
        cache_downloads: cache.cache_downloads,
        cache_read_calls: cache.local_read_calls,
        cache_read_bytes: cache.local_read_bytes,
    }
}

fn car_io_snapshot(stats: CarHttpStats) -> ArchiveIoSnapshot {
    ArchiveIoSnapshot {
        head_requests: stats.head_requests,
        get_requests: stats.get_requests,
        network_body_bytes: stats.get_body_bytes_received,
        ..ArchiveIoSnapshot::default()
    }
}

const fn duration_ns(duration: std::time::Duration) -> u64 {
    let nanos = duration.as_nanos();
    if nanos > u64::MAX as u128 {
        u64::MAX
    } else {
        nanos as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_read_sdk::{HttpRangeCacheStats, HttpRangeSourceStats};

    fn identity(format: ArchiveFormat, block_count: u32) -> SourceIdentity {
        SourceIdentity {
            format,
            label: "fixture".into(),
            cluster_id: Some("mainnet-beta".into()),
            epoch: 7,
            first_slot: 3_024_000,
            slots_per_epoch: 432_000,
            block_count,
            verification: match format {
                ArchiveFormat::Car => SourceVerification::OperatorTrusted,
                ArchiveFormat::CompactV2 => SourceVerification::PublishedManifest,
                ArchiveFormat::IndexerV3 => SourceVerification::InternalBindingOnly,
            },
            binding: Some("fixture-binding".into()),
        }
    }

    fn object(name: &str, length: u64, etag: &str) -> BoundHttpObject {
        BoundHttpObject {
            name: name.into(),
            length,
            strong_etag: etag.into(),
        }
    }

    fn manifest(cluster_id: &str, epoch: u64) -> GenerationManifest {
        GenerationManifest {
            schema_version: 1,
            cluster_id: cluster_id.into(),
            epoch,
            generation_id: "fixture".into(),
            generation_digest: "00".repeat(32),
            slots_per_epoch: 432_000,
            complete: true,
            files: Vec::new(),
        }
    }

    #[test]
    fn derives_strict_worker_paths() {
        let paths = WorkerPaths::parse("https://example.test/", 600, false).unwrap();
        assert_eq!(paths.normalized_origin, "https://example.test");
        assert_eq!(paths.car, "https://example.test/car/600/epoch-600.car");
        assert_eq!(paths.compact_v2, "https://example.test/compact-v2");
        assert_eq!(paths.indexer_v3, "https://example.test/indexer-v3/");

        for invalid in [
            "http://example.test",
            "https://user@example.test",
            "https://example.test/path",
            "https://example.test?query=1",
            "file:///tmp/archive",
        ] {
            assert!(
                WorkerPaths::parse(invalid, 600, false).is_err(),
                "{invalid}"
            );
        }
        assert!(WorkerPaths::parse("http://127.0.0.1:1234", 0, true).is_ok());
    }

    #[test]
    fn candidate_binding_is_stable_and_complete() {
        let first = vec![
            object("index", 12, "\"a\""),
            object("registry", 20, "\"b\""),
        ];
        let second = vec![first[1].clone(), first[0].clone()];
        let expected = candidate_binding("https://example.test/v3/", 100, &first);
        assert_eq!(
            expected,
            candidate_binding("https://example.test/v3/", 100, &second)
        );
        assert_ne!(
            expected,
            candidate_binding(
                "https://example.test/v3/",
                100,
                &[object("index", 13, "\"a\"")]
            )
        );
        assert_ne!(
            expected,
            candidate_binding("https://example.test/v3/", 101, &first)
        );
        assert_ne!(
            expected,
            candidate_binding("https://example.test/other/", 100, &first)
        );
    }

    #[test]
    fn full_car_identity_is_stable_across_requested_ranges() {
        let reference = identity(ArchiveFormat::CompactV2, 4);
        let slots = vec![3_024_001, 3_024_003, 3_024_007, 3_024_009];
        let first =
            car_identity(&reference, "epoch-7.car".into(), "object".into(), &slots).unwrap();
        let second =
            car_identity(&reference, "epoch-7.car".into(), "object".into(), &slots).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.block_count, 4);
        assert_eq!(first.verification, SourceVerification::OperatorTrusted);
    }

    #[test]
    fn first_slot_uses_the_warmup_aware_mainnet_schedule() {
        assert_eq!(resolve_first_slot(&manifest("mainnet-beta", 0)).unwrap(), 0);
        assert_eq!(
            resolve_first_slot(&manifest("mainnet-beta", 185)).unwrap(),
            74_396_256
        );
        assert!(resolve_first_slot(&manifest("custom", 185)).is_err());
    }

    #[test]
    fn v3_slot_plan_must_be_a_dense_compact_prefix() {
        let canonical = [10, 12, 18];
        validate_slot_plan_with(2, &canonical, |ordinal| Some(canonical[ordinal as usize]))
            .unwrap();
        assert!(validate_slot_plan_with(4, &canonical, |_| Some(10)).is_err());
        assert!(
            validate_slot_plan_with(2, &canonical, |ordinal| {
                Some(if ordinal == 0 { 10 } else { 13 })
            })
            .is_err()
        );
        assert!(validate_slot_plan_with(2, &canonical, |_| None).is_err());
    }

    #[test]
    fn geometry_and_range_checks_fail_closed() {
        let reference = identity(ArchiveFormat::CompactV2, 10);
        let range = ScanRange {
            first_block: 8,
            block_count: NonZeroU32::new(2).unwrap(),
        };
        let candidate = identity(ArchiveFormat::IndexerV3, 10);
        validate_geometry(&candidate, &reference, Some(range)).unwrap();

        let short = identity(ArchiveFormat::IndexerV3, 9);
        assert!(validate_geometry(&short, &reference, Some(range)).is_err());
        assert!(validate_required_range(&reference, range).is_ok());
        let mut wrong_epoch = candidate.clone();
        wrong_epoch.epoch += 1;
        assert!(validate_geometry(&wrong_epoch, &reference, None).is_err());
        let mut wrong_first_slot = candidate.clone();
        wrong_first_slot.first_slot += 1;
        assert!(validate_geometry(&wrong_first_slot, &reference, None).is_err());
        let mut wrong_schedule = candidate;
        wrong_schedule.slots_per_epoch -= 1;
        assert!(validate_geometry(&wrong_schedule, &reference, None).is_err());
    }

    #[test]
    fn open_state_is_lazy_and_one_open() {
        let mut state = OpenState::default();
        for format in WORKER_FORMATS {
            assert!(!state.is_open(format));
        }
        state.mark_open(ArchiveFormat::Car);
        assert!(state.is_open(ArchiveFormat::Car));
        assert!(!state.is_open(ArchiveFormat::CompactV2));
        assert!(!state.is_open(ArchiveFormat::IndexerV3));
    }

    #[test]
    fn normalized_counters_use_saturating_intervals() {
        let earlier = ArchiveIoSnapshot {
            head_requests: 4,
            get_requests: 5,
            network_body_bytes: 100,
            cache_hits: 2,
            cache_downloads: 3,
            cache_read_calls: 6,
            cache_read_bytes: 200,
        };
        let later = ArchiveIoSnapshot {
            head_requests: 7,
            get_requests: 4,
            network_body_bytes: 140,
            cache_hits: 5,
            cache_downloads: 5,
            cache_read_calls: 10,
            cache_read_bytes: 260,
        };
        assert_eq!(
            later.saturating_sub(earlier),
            ArchiveIoSnapshot {
                head_requests: 3,
                get_requests: 0,
                network_body_bytes: 40,
                cache_hits: 3,
                cache_downloads: 2,
                cache_read_calls: 4,
                cache_read_bytes: 60,
            }
        );
    }

    #[test]
    fn cache_namespace_binds_the_full_origin() {
        let first = cache_namespace("https://one.example");
        assert_eq!(first, cache_namespace("https://one.example"));
        assert_ne!(first, cache_namespace("https://two.example"));
        assert!(first.starts_with("origin-"));
    }

    #[test]
    fn cache_directory_is_created_for_strict_cache_source() {
        let temporary = tempfile::tempdir().unwrap();
        let cache = temporary
            .path()
            .canonicalize()
            .unwrap()
            .join("nested/cache");
        create_private_directory(&cache).unwrap();
        assert!(cache.is_dir());
    }

    #[test]
    fn expected_adapter_trust_levels_remain_distinct() {
        let reference = identity(ArchiveFormat::CompactV2, 1);
        for format in WORKER_FORMATS {
            let candidate = identity(format, 1);
            validate_source(&candidate, format, &reference, None).unwrap();
        }

        let mut wrong_trust = identity(ArchiveFormat::Car, 1);
        wrong_trust.verification = SourceVerification::PublishedManifest;
        assert!(matches!(
            validate_source(&wrong_trust, ArchiveFormat::Car, &reference, None),
            Err(Error::Identity(_))
        ));

        let wrong_format = identity(ArchiveFormat::Car, 1);
        assert!(matches!(
            validate_source(&wrong_format, ArchiveFormat::IndexerV3, &reference, None,),
            Err(Error::Identity(_))
        ));
    }

    #[test]
    fn open_receipt_and_counters_are_serializable() {
        let receipt = ArchiveOpenReceipt {
            setup_wall_ns: 9,
            source_size_bytes: Some(12),
            cache_root: Some(PathBuf::from("cache")),
            io: ArchiveIoSnapshot::default(),
        };
        let json = serde_json::to_string(&receipt).unwrap();
        assert!(json.contains("setup_wall_ns"));
    }

    #[test]
    fn range_http_snapshot_keeps_cache_and_transport_scopes_separate() {
        let http = HttpRangeSourceStats {
            head_requests: 2,
            get_requests: 3,
            returned_body_bytes: 40,
        };
        let cache = HttpRangeCacheStats {
            cache_hits: 4,
            cache_downloads: 5,
            local_read_calls: 6,
            local_read_bytes: 70,
            ..HttpRangeCacheStats::default()
        };
        let snapshot = ArchiveIoSnapshot {
            head_requests: http.head_requests,
            get_requests: http.get_requests,
            network_body_bytes: http.returned_body_bytes,
            cache_hits: cache.cache_hits,
            cache_downloads: cache.cache_downloads,
            cache_read_calls: cache.local_read_calls,
            cache_read_bytes: cache.local_read_bytes,
        };
        assert_eq!(snapshot.network_body_bytes, 40);
        assert_eq!(snapshot.cache_read_bytes, 70);
    }
}
