//! High-level reader for CAR epochs.
//!
//! Enable the `archive` feature to use this module. It includes the native zstd
//! decoder and the blocking HTTP client.
//!
//! [`CarArchive`] opens the fixed CAR and raw slot-index names from a public
//! Worker, Old Faithful, or the matching local `car/<epoch>/` layout. It
//! validates the complete fixed-size slot index and publishes the common
//! ordered query interface. A trusted canonical block count or exact canonical
//! slot plan is required because a raw range index does not identify canonical
//! blocks that occupy no reconstructed CAR bytes. Applications do not need a
//! CAR parser or HTTP range code.

use std::{
    fmt::{self, Display, Formatter},
    fs::File,
    io::Read,
    num::NonZeroU32,
    path::{Path, PathBuf},
};

use crate::{
    query_sdk::{CanonicalBlockPlan, CarInstructionSource, CarQueryError, CarQueryLimits},
    query_sdk_http::{
        CarHttpError, CarHttpIdentity, CarHttpOptions, CarHttpSession, CarHttpStats,
        CarHttpStatsHandle, CarHttpStream, OperatorTrustedCarHttpStream,
    },
    slot_ranges::{
        SLOT_RANGE_ENTRY_SIZE, SLOTS_PER_EPOCH, SlotRangeError, decode_slot_range_entry,
    },
};
pub use blockzilla_query_sdk::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveInstructionSourceExt, ArchiveIoSnapshot,
    BlockSink, BlockView, Error as QueryError, FnBlockSink, RecordedTokenBalance,
    Result as QueryResult, ScanIoReceipt, ScanRange, ScanReceipt, ScanRequest, SourceIdentity,
    SourceVerification, TokenBalanceCoverage, TokenBalanceRequirement, TokenBalanceSide,
    TransactionView,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

/// Exact size of one raw CAR slot index: 432,000 rows of 12 bytes.
pub const CAR_SLOT_INDEX_BYTES: u64 = SLOTS_PER_EPOCH * SLOT_RANGE_ENTRY_SIZE as u64;
const OBJECT_SET_BINDING_DOMAIN: &[u8] = b"blockzilla.car-network-object-set.v1\0";
const OPERATOR_TRUSTED_DESCRIPTOR_DOMAIN: &[u8] =
    b"blockzilla.car-operator-trusted-url-length-descriptor.v1\0";
const LOCAL_FILE_SET_BINDING_DOMAIN: &[u8] = b"blockzilla.car-local-open-file-set-descriptor.v1\0";

/// Network policy and bounded HTTP range controls for
/// [`CarArchive::open_with_options`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CarArchiveOptions {
    /// Permit cleartext HTTP for a controlled local test server.
    pub allow_insecure_http: bool,
    /// Concurrent closed-range workers.
    pub http_workers: usize,
    /// Maximum scheduled, active, or buffered range chunks.
    pub http_window_chunks: usize,
    /// Bytes in each HTTP range except the final range.
    pub http_chunk_bytes: usize,
}

impl Default for CarArchiveOptions {
    fn default() -> Self {
        let http = CarHttpOptions::default();
        Self {
            allow_insecure_http: false,
            http_workers: http.workers,
            http_window_chunks: http.window_chunks,
            http_chunk_bytes: http.chunk_bytes,
        }
    }
}

impl CarArchiveOptions {
    /// Validate the HTTP controls and return their maximum range-body window.
    ///
    /// The returned value does not include TLS, HTTP, channel, caller, or CAR
    /// decoder buffers.
    pub fn http_body_window_bytes(self) -> Result<usize> {
        Ok(self.validated_http_options()?.body_window_bytes()?)
    }

    fn validated_http_options(self) -> Result<CarHttpOptions> {
        let http = CarHttpOptions {
            workers: self.http_workers,
            window_chunks: self.http_window_chunks,
            chunk_bytes: self.http_chunk_bytes,
            allow_http: self.allow_insecure_http,
            ..CarHttpOptions::default()
        };
        let _ = http.body_window_bytes()?;
        Ok(http)
    }
}

/// HTTP object admission used for one CAR archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarArchiveHttpVerification {
    /// Exact URL, length, and strong ETag on HEAD and every range response.
    StrongEtag,
    /// Operator-accepted HTTPS object with exact length and response geometry.
    OperatorTrusted,
}

impl Display for CarArchiveHttpVerification {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::StrongEtag => formatter.write_str("strong-etag"),
            Self::OperatorTrusted => formatter.write_str("operator-trusted"),
        }
    }
}

impl CarArchiveHttpVerification {
    /// Return the object-binding kind that the HTTP admission supplies.
    ///
    /// Operator-trusted admission has no object binding. Neither HTTP
    /// admission computes an archive content hash.
    pub const fn object_binding_kind(self) -> &'static str {
        match self {
            Self::StrongEtag => "strong-etag",
            Self::OperatorTrusted => "none",
        }
    }
}

/// Effective HTTP profile used by one open archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CarArchiveHttpProfile {
    pub verification: CarArchiveHttpVerification,
    pub workers: usize,
    pub window_chunks: usize,
    pub chunk_bytes: usize,
    pub body_window_bytes: usize,
}

impl CarArchiveHttpProfile {
    fn new(verification: CarArchiveHttpVerification, options: CarArchiveOptions) -> Result<Self> {
        Ok(Self {
            verification,
            workers: options.http_workers,
            window_chunks: options.http_window_chunks,
            chunk_bytes: options.http_chunk_bytes,
            body_window_bytes: options.http_body_window_bytes()?,
        })
    }
}

/// High-level CAR reader errors.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid Worker origin: {0}")]
    InvalidOrigin(String),
    #[error("operator-trusted Old Faithful HTTP requires HTTPS")]
    OperatorTrustedRequiresHttps,
    #[error("epoch slot range overflows u64")]
    EpochOverflow,
    #[error("CAR slot index has {actual} bytes; expected exactly {expected}")]
    SlotIndexLength { expected: u64, actual: u64 },
    #[error("failed to read the complete CAR slot index")]
    SlotIndexRead(#[source] std::io::Error),
    #[error("CAR slot-index row {row} is invalid")]
    SlotIndexRow {
        row: u64,
        #[source]
        source: SlotRangeError,
    },
    #[error("empty CAR slot-index row {row} has nonzero offset {offset}")]
    EmptySlotOffset { row: u64, offset: u64 },
    #[error("CAR slot-index row {row} range end overflows u64")]
    SlotRangeOverflow { row: u64 },
    #[error(
        "CAR slot-index row {row} range {offset}..{end} is outside CAR object length {car_bytes}"
    )]
    SlotRangeOutsideCar {
        row: u64,
        offset: u64,
        end: u64,
        car_bytes: u64,
    },
    #[error(
        "CAR slot-index row {row} starts at {offset}, before the previous range end {previous_end}"
    )]
    SlotRangeOverlap {
        row: u64,
        offset: u64,
        previous_end: u64,
    },
    #[error("CAR slot index has no nonempty rows")]
    EmptySlotPlan,
    #[error(
        "CAR raw slot index has {nonempty_rows} nonempty rows; the trusted canonical block count is {expected_blocks}; supply the exact canonical slot plan when these counts differ"
    )]
    CanonicalBlockCountMismatch {
        expected_blocks: u32,
        nonempty_rows: u32,
    },
    #[error("the exact canonical slot plan does not include nonempty CAR slot {slot}")]
    CanonicalPlanMissingRangeSlot { slot: u64 },
    #[error(
        "the exact canonical slot plan is not strictly increasing at slots {previous} and {slot}"
    )]
    CanonicalPlanOrder { previous: u64, slot: u64 },
    #[error("canonical slot {slot} is outside epoch window {first_slot}..={last_slot}")]
    CanonicalPlanSlotOutsideEpoch {
        slot: u64,
        first_slot: u64,
        last_slot: u64,
    },
    #[error("cannot allocate the CAR canonical slot plan")]
    SlotPlanAllocation,
    #[error("CAR block count does not fit in u32")]
    BlockCountOverflow,
    #[error("CAR readable source-set size overflows u64")]
    SourceSizeOverflow,
    #[error("failed to open local CAR file {path}")]
    LocalOpen {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read metadata for local CAR file {path}")]
    LocalMetadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid CAR scan range: {0}")]
    InvalidRange(String),
    #[error("CAR HTTP source failed")]
    Http(#[from] CarHttpError),
    #[error("CAR query adapter failed")]
    Query(#[from] CarQueryError),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArchivePaths {
    car: String,
    slot_index: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalArchivePaths {
    car: PathBuf,
    slot_index: PathBuf,
    compressed: bool,
}

impl LocalArchivePaths {
    fn new(archive_root: &Path, epoch: u64) -> Self {
        let epoch_root = archive_root.join("car").join(epoch.to_string());
        let raw_car = epoch_root.join(format!("epoch-{epoch}.car"));
        let compressed_car = epoch_root.join(format!("epoch-{epoch}.car.zst"));
        let (car, compressed) = if raw_car.is_file() || !compressed_car.is_file() {
            (raw_car, false)
        } else {
            (compressed_car, true)
        };
        Self {
            car,
            slot_index: epoch_root.join(format!("epoch-{epoch}-slot-ranges.raw")),
            compressed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicRoute {
    BlockzillaWorker,
    OldFaithful,
}

impl ArchivePaths {
    fn parse(
        origin: &str,
        epoch: u64,
        route: PublicRoute,
        allow_insecure_http: bool,
    ) -> Result<Self> {
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

        let origin = parsed.as_str().trim_end_matches('/');
        let base = match route {
            PublicRoute::BlockzillaWorker => format!("{origin}/car/{epoch}"),
            PublicRoute::OldFaithful => format!("{origin}/{epoch}"),
        };
        Ok(Self {
            car: format!("{base}/epoch-{epoch}.car"),
            slot_index: format!("{base}/epoch-{epoch}-slot-ranges.raw"),
        })
    }
}

#[derive(Debug)]
enum CanonicalPlanRequirement {
    TrustedBlockCount(NonZeroU32),
    ExactSlots(Vec<u64>),
}

enum CarNetworkStream {
    StrongEtag(CarHttpStream),
    OperatorTrusted(OperatorTrustedCarHttpStream),
}

enum CarSource {
    Network(CarInstructionSource<CarNetworkStream>),
    Local(CarInstructionSource<Box<dyn Read + Send>>),
}

impl ArchiveInstructionSource for CarSource {
    fn identity(&self) -> &SourceIdentity {
        match self {
            Self::Network(source) => source.identity(),
            Self::Local(source) => source.identity(),
        }
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_query_sdk::Result<ScanReceipt> {
        match self {
            Self::Network(source) => source.scan_ordered(request, sink),
            Self::Local(source) => source.scan_ordered(request, sink),
        }
    }
}

enum CarTransport {
    Network {
        car_stats: CarHttpStatsHandle,
        index_stats: CarHttpStatsHandle,
        profile: CarArchiveHttpProfile,
    },
    Local,
}

impl CarNetworkStream {
    fn open(
        session: &CarHttpSession,
        url: &str,
        verification: CarArchiveHttpVerification,
    ) -> Result<Self> {
        match verification {
            CarArchiveHttpVerification::StrongEtag => Ok(Self::StrongEtag(session.open(url)?)),
            CarArchiveHttpVerification::OperatorTrusted => {
                Ok(Self::OperatorTrusted(session.open_operator_trusted(url)?))
            }
        }
    }

    fn content_length(&self) -> u64 {
        match self {
            Self::StrongEtag(stream) => stream.identity().content_length,
            Self::OperatorTrusted(stream) => stream.identity().content_length,
        }
    }

    fn strong_identity(&self) -> Option<&CarHttpIdentity> {
        match self {
            Self::StrongEtag(stream) => Some(stream.identity()),
            Self::OperatorTrusted(_) => None,
        }
    }

    fn url_length_descriptor(&self) -> (String, u64) {
        match self {
            Self::StrongEtag(stream) => (
                stream.identity().normalized_url.clone(),
                stream.identity().content_length,
            ),
            Self::OperatorTrusted(stream) => (
                stream.identity().normalized_url.clone(),
                stream.identity().content_length,
            ),
        }
    }

    fn stats_handle(&self) -> CarHttpStatsHandle {
        match self {
            Self::StrongEtag(stream) => stream.stats_handle(),
            Self::OperatorTrusted(stream) => stream.stats_handle(),
        }
    }
}

impl Read for CarNetworkStream {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::StrongEtag(stream) => stream.read(output),
            Self::OperatorTrusted(stream) => stream.read(output),
        }
    }
}

/// One admitted CAR epoch with its validated canonical slot plan.
pub struct CarArchive {
    source: CarSource,
    transport: CarTransport,
    car_size_bytes: u64,
    bound_source_size_bytes: u64,
}

impl CarArchive {
    /// Open one CAR epoch from the Blockzilla Worker route. HTTPS is mandatory.
    ///
    /// `expected_blocks` must come from a trusted canonical inventory. Opening
    /// fails if the raw index has a different number of nonempty rows. Use
    /// [`Self::open_with_canonical_slots`] when the epoch contains canonical
    /// blocks with no reconstructed CAR byte range.
    pub fn open(origin: &str, epoch: u64, expected_blocks: NonZeroU32) -> Result<Self> {
        Self::open_with_options(origin, epoch, expected_blocks, CarArchiveOptions::default())
    }

    /// Open one Worker CAR epoch with explicit HTTP range controls.
    pub fn open_with_options(
        origin: &str,
        epoch: u64,
        expected_blocks: NonZeroU32,
        options: CarArchiveOptions,
    ) -> Result<Self> {
        Self::open_route(
            origin,
            epoch,
            PublicRoute::BlockzillaWorker,
            CanonicalPlanRequirement::TrustedBlockCount(expected_blocks),
            CarArchiveHttpVerification::StrongEtag,
            options,
        )
    }

    /// Open one epoch from `<archive-root>/car/<epoch>/`.
    ///
    /// The directory must contain `epoch-<epoch>.car` or
    /// `epoch-<epoch>.car.zst`, plus `epoch-<epoch>-slot-ranges.raw`. The raw
    /// file is preferred when both CAR forms exist. The complete slot index
    /// receives the same geometry and trusted canonical-count checks as a
    /// network source.
    pub fn open_local(
        archive_root: impl AsRef<Path>,
        epoch: u64,
        expected_blocks: NonZeroU32,
    ) -> Result<Self> {
        Self::open_local_with_plan(
            archive_root.as_ref(),
            epoch,
            CanonicalPlanRequirement::TrustedBlockCount(expected_blocks),
        )
    }

    /// Open one local epoch with an exact trusted canonical slot plan.
    pub fn open_local_with_canonical_slots(
        archive_root: impl AsRef<Path>,
        epoch: u64,
        canonical_slots: Vec<u64>,
    ) -> Result<Self> {
        Self::open_local_with_plan(
            archive_root.as_ref(),
            epoch,
            CanonicalPlanRequirement::ExactSlots(canonical_slots),
        )
    }

    /// Open one CAR epoch from an Old Faithful-layout route with strong ETags.
    ///
    /// The public layout uses `https://files.old-faithful.net`, but that service
    /// currently omits ETags, so this strict constructor rejects it. Use the
    /// explicit operator-trusted constructor only when the operator accepts its
    /// stated limits. The canonical-count rule is the same as [`Self::open`].
    pub fn open_old_faithful(
        origin: &str,
        epoch: u64,
        expected_blocks: NonZeroU32,
    ) -> Result<Self> {
        Self::open_old_faithful_with_options(
            origin,
            epoch,
            expected_blocks,
            CarArchiveOptions::default(),
        )
    }

    /// Open one Old Faithful CAR epoch with explicit HTTP range controls.
    pub fn open_old_faithful_with_options(
        origin: &str,
        epoch: u64,
        expected_blocks: NonZeroU32,
        options: CarArchiveOptions,
    ) -> Result<Self> {
        Self::open_route(
            origin,
            epoch,
            PublicRoute::OldFaithful,
            CanonicalPlanRequirement::TrustedBlockCount(expected_blocks),
            CarArchiveHttpVerification::StrongEtag,
            options,
        )
    }

    /// Open one public Old Faithful epoch through explicit operator trust.
    ///
    /// This path requires HTTPS. It accepts the lack of an ETag, but still
    /// requires one exact HEAD length and exact response geometry and body
    /// length. Partial GETs require `206 Partial Content`. A `200 OK` response
    /// is accepted only when the scheduled range covers the complete object
    /// and its declared and returned lengths match HEAD. This path does not
    /// create an object binding or content hash.
    pub fn open_old_faithful_operator_trusted(
        origin: &str,
        epoch: u64,
        expected_blocks: NonZeroU32,
    ) -> Result<Self> {
        Self::open_old_faithful_operator_trusted_with_options(
            origin,
            epoch,
            expected_blocks,
            CarArchiveOptions::default(),
        )
    }

    /// Open one operator-trusted Old Faithful epoch with HTTP range controls.
    pub fn open_old_faithful_operator_trusted_with_options(
        origin: &str,
        epoch: u64,
        expected_blocks: NonZeroU32,
        options: CarArchiveOptions,
    ) -> Result<Self> {
        Self::open_route(
            origin,
            epoch,
            PublicRoute::OldFaithful,
            CanonicalPlanRequirement::TrustedBlockCount(expected_blocks),
            CarArchiveHttpVerification::OperatorTrusted,
            options,
        )
    }

    /// Open one Worker CAR epoch with an exact trusted canonical slot plan.
    ///
    /// This is the correct constructor when one or more canonical blocks have
    /// an empty raw range. The caller supplies the exact trusted plan.
    pub fn open_with_canonical_slots(
        origin: &str,
        epoch: u64,
        canonical_slots: Vec<u64>,
    ) -> Result<Self> {
        Self::open_route(
            origin,
            epoch,
            PublicRoute::BlockzillaWorker,
            CanonicalPlanRequirement::ExactSlots(canonical_slots),
            CarArchiveHttpVerification::StrongEtag,
            CarArchiveOptions::default(),
        )
    }

    /// Open one Old Faithful CAR epoch with an exact trusted canonical plan.
    pub fn open_old_faithful_with_canonical_slots(
        origin: &str,
        epoch: u64,
        canonical_slots: Vec<u64>,
    ) -> Result<Self> {
        Self::open_route(
            origin,
            epoch,
            PublicRoute::OldFaithful,
            CanonicalPlanRequirement::ExactSlots(canonical_slots),
            CarArchiveHttpVerification::StrongEtag,
            CarArchiveOptions::default(),
        )
    }

    /// Open one operator-trusted Old Faithful epoch with an exact slot plan.
    pub fn open_old_faithful_operator_trusted_with_canonical_slots(
        origin: &str,
        epoch: u64,
        canonical_slots: Vec<u64>,
    ) -> Result<Self> {
        Self::open_route(
            origin,
            epoch,
            PublicRoute::OldFaithful,
            CanonicalPlanRequirement::ExactSlots(canonical_slots),
            CarArchiveHttpVerification::OperatorTrusted,
            CarArchiveOptions::default(),
        )
    }

    fn open_route(
        origin: &str,
        epoch: u64,
        route: PublicRoute,
        plan_requirement: CanonicalPlanRequirement,
        http_verification: CarArchiveHttpVerification,
        options: CarArchiveOptions,
    ) -> Result<Self> {
        if http_verification == CarArchiveHttpVerification::OperatorTrusted
            && options.allow_insecure_http
        {
            return Err(Error::OperatorTrustedRequiresHttps);
        }
        let allow_insecure_http = options.allow_insecure_http
            && http_verification == CarArchiveHttpVerification::StrongEtag;
        let paths = ArchivePaths::parse(origin, epoch, route, allow_insecure_http)?;
        let http_options = options.validated_http_options()?;
        let http_session = CarHttpSession::new(http_options)?;
        let http_profile = CarArchiveHttpProfile::new(http_verification, options)?;

        let mut index_stream =
            CarNetworkStream::open(&http_session, &paths.slot_index, http_verification)?;
        let index_content_length = index_stream.content_length();
        if index_content_length != CAR_SLOT_INDEX_BYTES {
            return Err(Error::SlotIndexLength {
                expected: CAR_SLOT_INDEX_BYTES,
                actual: index_content_length,
            });
        }
        let index_strong_identity = index_stream.strong_identity().cloned();
        let index_descriptor = index_stream.url_length_descriptor();
        let index_stats = index_stream.stats_handle();
        let expected_index_bytes = usize::try_from(CAR_SLOT_INDEX_BYTES)
            .expect("the fixed CAR slot-index size fits in usize");
        let mut index_bytes = vec![0_u8; expected_index_bytes];
        index_stream
            .read_exact(&mut index_bytes)
            .map_err(Error::SlotIndexRead)?;
        let mut trailing = [0_u8; 1];
        if index_stream
            .read(&mut trailing)
            .map_err(Error::SlotIndexRead)?
            != 0
        {
            return Err(Error::SlotIndexLength {
                expected: CAR_SLOT_INDEX_BYTES,
                actual: CAR_SLOT_INDEX_BYTES + 1,
            });
        }
        drop(index_stream);

        let first_slot = epoch
            .checked_mul(SLOTS_PER_EPOCH)
            .ok_or(Error::EpochOverflow)?;
        // Resolve the canonical universe before a CAR stream can prefetch body
        // ranges. Known incomplete raw plans therefore fail after the small
        // index read, without starting a large CAR transfer.
        let nonempty_slots = decode_nonempty_slots(&index_bytes, first_slot, u64::MAX)?;
        let slots = resolve_canonical_plan(nonempty_slots, plan_requirement, first_slot)?;

        let car_stream = CarNetworkStream::open(&http_session, &paths.car, http_verification)?;
        let car_content_length = car_stream.content_length();
        let car_strong_identity = car_stream.strong_identity().cloned();
        let car_descriptor = car_stream.url_length_descriptor();
        let car_stats = car_stream.stats_handle();
        validate_slot_range_bounds(&index_bytes, car_content_length)?;
        let block_count = u32::try_from(slots.len()).map_err(|_| Error::BlockCountOverflow)?;
        let binding = match http_verification {
            CarArchiveHttpVerification::StrongEtag => Some(object_set_binding(
                epoch,
                car_strong_identity
                    .as_ref()
                    .expect("strong-ETag route has a strong CAR identity"),
                index_strong_identity
                    .as_ref()
                    .expect("strong-ETag route has a strong index identity"),
            )),
            CarArchiveHttpVerification::OperatorTrusted => Some(
                operator_trusted_descriptor_binding(epoch, &car_descriptor, &index_descriptor),
            ),
        };
        let identity = SourceIdentity {
            format: ArchiveFormat::Car,
            label: format!("epoch-{epoch}.car"),
            // CAR and the raw slot index do not carry a cluster identity.
            cluster_id: None,
            epoch,
            first_slot,
            slots_per_epoch: SLOTS_PER_EPOCH,
            block_count,
            verification: SourceVerification::OperatorTrusted,
            binding,
        };
        let source = match http_verification {
            CarArchiveHttpVerification::StrongEtag => CarInstructionSource::new(
                car_stream,
                identity,
                CanonicalBlockPlan::new(slots),
                CarQueryLimits::default(),
            )?,
            CarArchiveHttpVerification::OperatorTrusted => {
                CarInstructionSource::new_operator_trusted_descriptor(
                    car_stream,
                    identity,
                    CanonicalBlockPlan::new(slots),
                    CarQueryLimits::default(),
                )?
            }
        };
        let bound_source_size_bytes = car_content_length
            .checked_add(CAR_SLOT_INDEX_BYTES)
            .ok_or(Error::SourceSizeOverflow)?;

        Ok(Self {
            source: CarSource::Network(source),
            transport: CarTransport::Network {
                car_stats,
                index_stats,
                profile: http_profile,
            },
            car_size_bytes: car_content_length,
            bound_source_size_bytes,
        })
    }

    fn open_local_with_plan(
        archive_root: &Path,
        epoch: u64,
        plan_requirement: CanonicalPlanRequirement,
    ) -> Result<Self> {
        let paths = LocalArchivePaths::new(archive_root, epoch);
        let mut index_file = open_local_file(&paths.slot_index)?;
        let index_content_length = local_file_len(&index_file, &paths.slot_index)?;
        if index_content_length != CAR_SLOT_INDEX_BYTES {
            return Err(Error::SlotIndexLength {
                expected: CAR_SLOT_INDEX_BYTES,
                actual: index_content_length,
            });
        }
        let expected_index_bytes = usize::try_from(CAR_SLOT_INDEX_BYTES)
            .expect("the fixed CAR slot-index size fits in usize");
        let mut index_bytes = vec![0_u8; expected_index_bytes];
        index_file
            .read_exact(&mut index_bytes)
            .map_err(Error::SlotIndexRead)?;
        let mut trailing = [0_u8; 1];
        if index_file
            .read(&mut trailing)
            .map_err(Error::SlotIndexRead)?
            != 0
        {
            return Err(Error::SlotIndexLength {
                expected: CAR_SLOT_INDEX_BYTES,
                actual: CAR_SLOT_INDEX_BYTES + 1,
            });
        }

        let car_file = open_local_file(&paths.car)?;
        let car_content_length = local_file_len(&car_file, &paths.car)?;
        let car_range_limit = if paths.compressed {
            // Slot-index offsets address the decoded CAR stream. The decoder
            // validates the stream length while reading; stored bytes are used
            // for the source-size receipt.
            u64::MAX
        } else {
            car_content_length
        };
        let first_slot = epoch
            .checked_mul(SLOTS_PER_EPOCH)
            .ok_or(Error::EpochOverflow)?;
        let nonempty_slots = decode_nonempty_slots(&index_bytes, first_slot, car_range_limit)?;
        let slots = resolve_canonical_plan(nonempty_slots, plan_requirement, first_slot)?;
        validate_slot_range_bounds(&index_bytes, car_range_limit)?;
        let block_count = u32::try_from(slots.len()).map_err(|_| Error::BlockCountOverflow)?;
        let binding = local_file_set_binding(
            epoch,
            &paths.car,
            car_content_length,
            &paths.slot_index,
            index_content_length,
        );
        let identity = SourceIdentity {
            format: ArchiveFormat::Car,
            label: paths
                .car
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("local-car")
                .to_owned(),
            cluster_id: None,
            epoch,
            first_slot,
            slots_per_epoch: SLOTS_PER_EPOCH,
            block_count,
            verification: SourceVerification::OperatorTrusted,
            binding: Some(binding),
        };
        let car_reader: Box<dyn Read + Send> = if paths.compressed {
            Box::new(
                zstd::stream::read::Decoder::new(car_file).map_err(|source| Error::LocalOpen {
                    path: paths.car.clone(),
                    source,
                })?,
            )
        } else {
            Box::new(car_file)
        };
        let source = CarInstructionSource::new_operator_trusted_descriptor(
            car_reader,
            identity,
            CanonicalBlockPlan::new(slots),
            CarQueryLimits::default(),
        )?;
        let bound_source_size_bytes = car_content_length
            .checked_add(index_content_length)
            .ok_or(Error::SourceSizeOverflow)?;

        Ok(Self {
            source: CarSource::Local(source),
            transport: CarTransport::Local,
            car_size_bytes: car_content_length,
            bound_source_size_bytes,
        })
    }

    /// Select at most `max_blocks` canonical block rows.
    pub fn bounded_range(&self, first_block: u32, max_blocks: NonZeroU32) -> Result<ScanRange> {
        bounded_range_for(self.source.identity().block_count, first_block, max_blocks)
    }

    /// Return the exact CAR length observed during source admission.
    pub const fn car_size_bytes(&self) -> u64 {
        self.car_size_bytes
    }

    /// Return the admitted CAR plus raw slot-index reader-set size.
    pub const fn bound_source_size_bytes(&self) -> u64 {
        self.bound_source_size_bytes
    }

    /// Return the HTTP profile, or `None` for a local archive.
    pub const fn http_profile(&self) -> Option<CarArchiveHttpProfile> {
        match &self.transport {
            CarTransport::Network { profile, .. } => Some(*profile),
            CarTransport::Local => None,
        }
    }

    /// Return current index and CAR network counters. Local sources return
    /// zero transport counters; logical file bytes remain in the scan receipt.
    pub fn io_snapshot(&self) -> ArchiveIoSnapshot {
        match &self.transport {
            CarTransport::Network {
                car_stats,
                index_stats,
                ..
            } => combined_io(index_stats.snapshot(), car_stats.snapshot()),
            CarTransport::Local => ArchiveIoSnapshot::default(),
        }
    }

    /// Stop background reads and return final network counters. Local sources
    /// return zero transport counters.
    pub fn finish_io(self) -> ArchiveIoSnapshot {
        let Self {
            source, transport, ..
        } = self;
        drop(source);
        match transport {
            CarTransport::Network {
                car_stats,
                index_stats,
                ..
            } => combined_io(index_stats.snapshot(), car_stats.snapshot()),
            CarTransport::Local => ArchiveIoSnapshot::default(),
        }
    }
}

impl ArchiveInstructionSource for CarArchive {
    fn identity(&self) -> &SourceIdentity {
        self.source.identity()
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_query_sdk::Result<ScanReceipt> {
        self.source.scan_ordered(request, sink)
    }
}

fn open_local_file(path: &Path) -> Result<File> {
    File::open(path).map_err(|source| Error::LocalOpen {
        path: path.to_path_buf(),
        source,
    })
}

fn local_file_len(file: &File, path: &Path) -> Result<u64> {
    file.metadata()
        .map(|metadata| metadata.len())
        .map_err(|source| Error::LocalMetadata {
            path: path.to_path_buf(),
            source,
        })
}

fn decode_nonempty_slots(bytes: &[u8], first_slot: u64, car_bytes: u64) -> Result<Vec<u64>> {
    if bytes.len() as u64 != CAR_SLOT_INDEX_BYTES {
        return Err(Error::SlotIndexLength {
            expected: CAR_SLOT_INDEX_BYTES,
            actual: bytes.len() as u64,
        });
    }

    let mut slots = Vec::new();
    slots
        .try_reserve_exact(SLOTS_PER_EPOCH as usize)
        .map_err(|_| Error::SlotPlanAllocation)?;
    let mut previous_end = None;
    for (row, encoded) in bytes.chunks_exact(SLOT_RANGE_ENTRY_SIZE).enumerate() {
        let row = row as u64;
        let range = decode_slot_range_entry(encoded)
            .map_err(|source| Error::SlotIndexRow { row, source })?;
        if range.is_empty() {
            if range.offset != 0 {
                return Err(Error::EmptySlotOffset {
                    row,
                    offset: range.offset,
                });
            }
            continue;
        }

        let end = range
            .offset
            .checked_add(u64::from(range.len))
            .ok_or(Error::SlotRangeOverflow { row })?;
        if end > car_bytes {
            return Err(Error::SlotRangeOutsideCar {
                row,
                offset: range.offset,
                end,
                car_bytes,
            });
        }
        if let Some(previous_end) = previous_end
            && range.offset < previous_end
        {
            return Err(Error::SlotRangeOverlap {
                row,
                offset: range.offset,
                previous_end,
            });
        }
        previous_end = Some(end);
        slots.push(first_slot.checked_add(row).ok_or(Error::EpochOverflow)?);
    }
    if slots.is_empty() {
        return Err(Error::EmptySlotPlan);
    }
    Ok(slots)
}

fn validate_slot_range_bounds(bytes: &[u8], car_bytes: u64) -> Result<()> {
    for (row, encoded) in bytes.chunks_exact(SLOT_RANGE_ENTRY_SIZE).enumerate() {
        let row = row as u64;
        let range = decode_slot_range_entry(encoded)
            .map_err(|source| Error::SlotIndexRow { row, source })?;
        let end = range
            .offset
            .checked_add(u64::from(range.len))
            .ok_or(Error::SlotRangeOverflow { row })?;
        if end > car_bytes {
            return Err(Error::SlotRangeOutsideCar {
                row,
                offset: range.offset,
                end,
                car_bytes,
            });
        }
    }
    Ok(())
}

fn resolve_canonical_plan(
    nonempty_slots: Vec<u64>,
    requirement: CanonicalPlanRequirement,
    first_slot: u64,
) -> Result<Vec<u64>> {
    match requirement {
        CanonicalPlanRequirement::TrustedBlockCount(expected_blocks) => {
            let nonempty_rows =
                u32::try_from(nonempty_slots.len()).map_err(|_| Error::BlockCountOverflow)?;
            if nonempty_rows != expected_blocks.get() {
                return Err(Error::CanonicalBlockCountMismatch {
                    expected_blocks: expected_blocks.get(),
                    nonempty_rows,
                });
            }
            Ok(nonempty_slots)
        }
        CanonicalPlanRequirement::ExactSlots(canonical_slots) => {
            if canonical_slots.is_empty() {
                return Err(Error::EmptySlotPlan);
            }
            let last_slot = first_slot
                .checked_add(SLOTS_PER_EPOCH - 1)
                .ok_or(Error::EpochOverflow)?;
            if let Some(&slot) = canonical_slots
                .iter()
                .find(|&&slot| slot < first_slot || slot > last_slot)
            {
                return Err(Error::CanonicalPlanSlotOutsideEpoch {
                    slot,
                    first_slot,
                    last_slot,
                });
            }
            if let Some(pair) = canonical_slots.windows(2).find(|pair| pair[0] >= pair[1]) {
                return Err(Error::CanonicalPlanOrder {
                    previous: pair[0],
                    slot: pair[1],
                });
            }
            for slot in nonempty_slots {
                if canonical_slots.binary_search(&slot).is_err() {
                    return Err(Error::CanonicalPlanMissingRangeSlot { slot });
                }
            }
            Ok(canonical_slots)
        }
    }
}

fn bounded_range_for(
    block_count: u32,
    first_block: u32,
    max_blocks: NonZeroU32,
) -> Result<ScanRange> {
    if first_block >= block_count {
        return Err(Error::InvalidRange(format!(
            "first block {first_block} is outside block rows 0..{block_count}"
        )));
    }
    let count = NonZeroU32::new((block_count - first_block).min(max_blocks.get()))
        .expect("a validated first block leaves at least one row");
    Ok(ScanRange {
        first_block,
        block_count: count,
    })
}

fn combined_io(index: CarHttpStats, car: CarHttpStats) -> ArchiveIoSnapshot {
    ArchiveIoSnapshot {
        head_requests: index.head_requests.saturating_add(car.head_requests),
        get_requests: index.get_requests.saturating_add(car.get_requests),
        network_body_bytes: index
            .get_body_bytes_received
            .saturating_add(car.get_body_bytes_received),
        ..ArchiveIoSnapshot::default()
    }
}

fn object_set_binding(
    epoch: u64,
    car: &crate::query_sdk_http::CarHttpIdentity,
    index: &crate::query_sdk_http::CarHttpIdentity,
) -> String {
    let mut digest = Sha256::new();
    digest.update(OBJECT_SET_BINDING_DOMAIN);
    digest.update(epoch.to_le_bytes());
    hash_http_identity(&mut digest, b"car", car);
    hash_http_identity(&mut digest, b"slot-index", index);
    hex_lower(&digest.finalize())
}

fn operator_trusted_descriptor_binding(
    epoch: u64,
    car: &(String, u64),
    index: &(String, u64),
) -> String {
    let mut digest = Sha256::new();
    digest.update(OPERATOR_TRUSTED_DESCRIPTOR_DOMAIN);
    digest.update(epoch.to_le_bytes());
    hash_bytes(&mut digest, car.0.as_bytes());
    digest.update(car.1.to_le_bytes());
    hash_bytes(&mut digest, index.0.as_bytes());
    digest.update(index.1.to_le_bytes());
    format!(
        "url-length-descriptor-sha256={}",
        hex_lower(&digest.finalize())
    )
}

fn local_file_set_binding(
    epoch: u64,
    car_path: &Path,
    car_bytes: u64,
    index_path: &Path,
    index_bytes: u64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(LOCAL_FILE_SET_BINDING_DOMAIN);
    digest.update(epoch.to_le_bytes());
    hash_bytes(&mut digest, car_path.as_os_str().as_encoded_bytes());
    digest.update(car_bytes.to_le_bytes());
    hash_bytes(&mut digest, index_path.as_os_str().as_encoded_bytes());
    digest.update(index_bytes.to_le_bytes());
    format!(
        "local-open-file-set-descriptor-sha256={}",
        hex_lower(&digest.finalize())
    )
}

fn hash_http_identity(
    digest: &mut Sha256,
    label: &[u8],
    identity: &crate::query_sdk_http::CarHttpIdentity,
) {
    hash_bytes(digest, label);
    hash_bytes(digest, identity.normalized_url.as_bytes());
    digest.update(identity.content_length.to_le_bytes());
    hash_bytes(digest, identity.strong_etag.as_bytes());
    hash_bytes(digest, identity.object_binding.as_bytes());
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
    use super::*;
    use crate::{
        query_sdk_http::{
            CarHttpIdentity, MAX_HTTP_CHUNK_BYTES, MAX_HTTP_WINDOW_CHUNKS, MAX_HTTP_WORKERS,
        },
        slot_ranges::SlotRange,
    };

    fn index_with(rows: &[(usize, SlotRange)]) -> Vec<u8> {
        let mut bytes = vec![0_u8; CAR_SLOT_INDEX_BYTES as usize];
        for (row, range) in rows {
            let start = row * SLOT_RANGE_ENTRY_SIZE;
            bytes[start..start + SLOT_RANGE_ENTRY_SIZE].copy_from_slice(&range.encode());
        }
        bytes
    }

    #[test]
    fn derives_strict_worker_urls() {
        let paths = ArchivePaths::parse(
            "https://archive.example/",
            7,
            PublicRoute::BlockzillaWorker,
            false,
        )
        .unwrap();
        assert_eq!(paths.car, "https://archive.example/car/7/epoch-7.car");
        assert_eq!(
            paths.slot_index,
            "https://archive.example/car/7/epoch-7-slot-ranges.raw"
        );
    }

    #[test]
    fn derives_strict_old_faithful_urls() {
        let paths = ArchivePaths::parse(
            "https://files.old-faithful.net/",
            100,
            PublicRoute::OldFaithful,
            false,
        )
        .unwrap();
        assert_eq!(
            paths.car,
            "https://files.old-faithful.net/100/epoch-100.car"
        );
        assert_eq!(
            paths.slot_index,
            "https://files.old-faithful.net/100/epoch-100-slot-ranges.raw"
        );
    }

    #[test]
    fn derives_the_local_sample_layout() {
        let paths = LocalArchivePaths::new(Path::new("archive"), 900);
        assert_eq!(paths.car, PathBuf::from("archive/car/900/epoch-900.car"));
        assert_eq!(
            paths.slot_index,
            PathBuf::from("archive/car/900/epoch-900-slot-ranges.raw")
        );
    }

    #[test]
    fn cleartext_requires_explicit_test_option() {
        assert!(
            ArchivePaths::parse(
                "http://127.0.0.1:8000",
                0,
                PublicRoute::BlockzillaWorker,
                false
            )
            .is_err()
        );
        assert!(
            ArchivePaths::parse(
                "http://127.0.0.1:8000",
                0,
                PublicRoute::BlockzillaWorker,
                true
            )
            .is_ok()
        );
    }

    #[test]
    fn archive_options_use_transport_defaults() {
        let options = CarArchiveOptions::default();
        let expected = CarHttpOptions::default();
        assert_eq!(options.http_workers, expected.workers);
        assert_eq!(options.http_window_chunks, expected.window_chunks);
        assert_eq!(options.http_chunk_bytes, expected.chunk_bytes);
        assert_eq!(
            options.http_body_window_bytes().unwrap(),
            expected.body_window_bytes().unwrap()
        );
    }

    #[test]
    fn archive_options_accept_a_bounded_http_profile() {
        let options = CarArchiveOptions {
            http_workers: 2,
            http_window_chunks: 4,
            http_chunk_bytes: 1 << 20,
            ..CarArchiveOptions::default()
        };
        let http = options.validated_http_options().unwrap();
        assert_eq!(http.workers, 2);
        assert_eq!(http.window_chunks, 4);
        assert_eq!(http.chunk_bytes, 1 << 20);
        assert_eq!(options.http_body_window_bytes().unwrap(), 4 << 20);
    }

    #[test]
    fn archive_options_reject_invalid_http_profiles() {
        for options in [
            CarArchiveOptions {
                http_workers: 0,
                ..CarArchiveOptions::default()
            },
            CarArchiveOptions {
                http_workers: 5,
                http_window_chunks: 4,
                ..CarArchiveOptions::default()
            },
            CarArchiveOptions {
                http_chunk_bytes: 0,
                ..CarArchiveOptions::default()
            },
            CarArchiveOptions {
                http_workers: MAX_HTTP_WORKERS + 1,
                http_window_chunks: MAX_HTTP_WORKERS + 1,
                ..CarArchiveOptions::default()
            },
            CarArchiveOptions {
                http_window_chunks: MAX_HTTP_WINDOW_CHUNKS + 1,
                ..CarArchiveOptions::default()
            },
            CarArchiveOptions {
                http_chunk_bytes: MAX_HTTP_CHUNK_BYTES + 1,
                ..CarArchiveOptions::default()
            },
        ] {
            assert!(
                matches!(options.validated_http_options(), Err(Error::Http(_))),
                "accepted {options:?}"
            );
        }
    }

    #[test]
    fn http_profile_reports_defaults_and_verification() {
        let profile = CarArchiveHttpProfile::new(
            CarArchiveHttpVerification::OperatorTrusted,
            CarArchiveOptions::default(),
        )
        .unwrap();
        assert_eq!(profile.verification.to_string(), "operator-trusted");
        assert_eq!(profile.workers, 4);
        assert_eq!(profile.window_chunks, 8);
        assert_eq!(profile.chunk_bytes, 32 << 20);
        assert_eq!(profile.body_window_bytes, 256 << 20);
        assert_eq!(profile.verification.object_binding_kind(), "none");
        assert_eq!(
            CarArchiveHttpVerification::StrongEtag.to_string(),
            "strong-etag"
        );
        assert_eq!(
            CarArchiveHttpVerification::StrongEtag.object_binding_kind(),
            "strong-etag"
        );
    }

    #[test]
    fn operator_trusted_route_rejects_insecure_transport_option() {
        let result = CarArchive::open_old_faithful_operator_trusted_with_options(
            "https://files.old-faithful.net",
            0,
            NonZeroU32::new(1).unwrap(),
            CarArchiveOptions {
                allow_insecure_http: true,
                ..CarArchiveOptions::default()
            },
        );
        assert!(matches!(result, Err(Error::OperatorTrustedRequiresHttps)));
    }

    #[test]
    fn rejects_origin_path_query_and_credentials() {
        for origin in [
            "https://archive.example/car",
            "https://archive.example/?x=1",
            "https://user@archive.example/",
        ] {
            assert!(ArchivePaths::parse(origin, 0, PublicRoute::BlockzillaWorker, false).is_err());
        }
    }

    #[test]
    fn builds_nonempty_plan_in_slot_order() {
        let bytes = index_with(&[
            (1, SlotRange { offset: 10, len: 5 }),
            (3, SlotRange { offset: 15, len: 8 }),
        ]);
        let plan = decode_nonempty_slots(&bytes, 432_000, 23).unwrap();
        assert_eq!(plan, vec![432_001, 432_003]);
    }

    #[test]
    fn uses_fixed_archive_epoch_windows() {
        let first = 1_u64.checked_mul(SLOTS_PER_EPOCH).unwrap();
        assert_eq!(first, 432_000);
        assert_eq!(185_u64.checked_mul(SLOTS_PER_EPOCH).unwrap(), 79_920_000);
    }

    #[test]
    fn binding_covers_both_pinned_objects() {
        let car = CarHttpIdentity {
            normalized_url: "https://example.test/car/0/epoch-0.car".into(),
            content_length: 100,
            strong_etag: "\"car\"".into(),
            object_binding: "car-binding".into(),
        };
        let index = CarHttpIdentity {
            normalized_url: "https://example.test/car/0/epoch-0-slot-ranges.raw".into(),
            content_length: CAR_SLOT_INDEX_BYTES,
            strong_etag: "\"index-a\"".into(),
            object_binding: "index-binding-a".into(),
        };
        let first = object_set_binding(0, &car, &index);
        let mut changed = index.clone();
        changed.strong_etag = "\"index-b\"".into();
        changed.object_binding = "index-binding-b".into();
        assert_eq!(first.len(), 64);
        assert_eq!(first, object_set_binding(0, &car, &index));
        assert_ne!(first, object_set_binding(0, &car, &changed));
    }

    #[test]
    fn operator_trusted_binding_covers_only_url_length_descriptors() {
        let car = ("https://files.old-faithful.net/0/epoch-0.car".into(), 100);
        let index = (
            "https://files.old-faithful.net/0/epoch-0-slot-ranges.raw".into(),
            CAR_SLOT_INDEX_BYTES,
        );
        let first = operator_trusted_descriptor_binding(0, &car, &index);
        let mut changed = car.clone();
        changed.1 += 1;
        assert!(first.starts_with("url-length-descriptor-sha256="));
        assert_ne!(
            first,
            operator_trusted_descriptor_binding(0, &changed, &index)
        );
        assert_eq!(first, operator_trusted_descriptor_binding(0, &car, &index));
    }

    #[test]
    fn rejects_wrong_index_length() {
        let error = decode_nonempty_slots(&[0; 12], 0, 100).unwrap_err();
        assert!(matches!(error, Error::SlotIndexLength { .. }));
    }

    #[test]
    fn rejects_noncanonical_empty_row() {
        let bytes = index_with(&[(2, SlotRange { offset: 9, len: 0 })]);
        let error = decode_nonempty_slots(&bytes, 0, 100).unwrap_err();
        assert!(matches!(error, Error::EmptySlotOffset { row: 2, .. }));
    }

    #[test]
    fn rejects_overlapping_ranges() {
        let bytes = index_with(&[
            (
                1,
                SlotRange {
                    offset: 10,
                    len: 10,
                },
            ),
            (2, SlotRange { offset: 19, len: 2 }),
        ]);
        let error = decode_nonempty_slots(&bytes, 0, 100).unwrap_err();
        assert!(matches!(error, Error::SlotRangeOverlap { row: 2, .. }));
    }

    #[test]
    fn rejects_range_outside_pinned_car() {
        let bytes = index_with(&[(
            1,
            SlotRange {
                offset: 90,
                len: 11,
            },
        )]);
        let error = decode_nonempty_slots(&bytes, 0, 100).unwrap_err();
        assert!(matches!(error, Error::SlotRangeOutsideCar { row: 1, .. }));
    }

    #[test]
    fn trusted_count_rejects_an_incomplete_raw_plan() {
        let error = resolve_canonical_plan(
            vec![10, 12],
            CanonicalPlanRequirement::TrustedBlockCount(NonZeroU32::new(3).unwrap()),
            0,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::CanonicalBlockCountMismatch {
                expected_blocks: 3,
                nonempty_rows: 2
            }
        ));
    }

    #[test]
    fn exact_plan_can_include_zero_range_blocks() {
        let plan = resolve_canonical_plan(
            vec![10, 14],
            CanonicalPlanRequirement::ExactSlots(vec![10, 12, 14]),
            0,
        )
        .unwrap();
        assert_eq!(plan, vec![10, 12, 14]);
    }

    #[test]
    fn exact_plan_must_include_each_nonempty_range() {
        let error = resolve_canonical_plan(
            vec![10, 14],
            CanonicalPlanRequirement::ExactSlots(vec![10, 12]),
            0,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::CanonicalPlanMissingRangeSlot { slot: 14 }
        ));
    }

    #[test]
    fn exact_plan_must_be_strictly_increasing() {
        let error = resolve_canonical_plan(
            vec![10],
            CanonicalPlanRequirement::ExactSlots(vec![12, 10]),
            0,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::CanonicalPlanOrder {
                previous: 12,
                slot: 10
            }
        ));
    }

    #[test]
    fn exact_plan_must_stay_in_the_epoch_window() {
        let error = resolve_canonical_plan(
            vec![432_010],
            CanonicalPlanRequirement::ExactSlots(vec![10, 432_010]),
            432_000,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::CanonicalPlanSlotOutsideEpoch { slot: 10, .. }
        ));
    }

    #[test]
    fn clamps_bounded_range_to_available_rows() {
        let range = bounded_range_for(3, 1, NonZeroU32::new(1024).unwrap()).unwrap();
        assert_eq!(range.first_block, 1);
        assert_eq!(range.block_count.get(), 2);
        assert!(bounded_range_for(3, 3, NonZeroU32::new(1).unwrap()).is_err());
    }

    #[test]
    fn combines_transport_counters() {
        let index = CarHttpStats {
            head_requests: 1,
            get_requests: 1,
            get_body_bytes_received: CAR_SLOT_INDEX_BYTES,
            ..CarHttpStats::default()
        };
        let car = CarHttpStats {
            head_requests: 1,
            get_requests: 4,
            get_body_bytes_received: 128_000_000,
            ..CarHttpStats::default()
        };
        assert_eq!(
            combined_io(index, car),
            ArchiveIoSnapshot {
                head_requests: 2,
                get_requests: 5,
                network_body_bytes: 128_000_000 + CAR_SLOT_INDEX_BYTES,
                ..ArchiveIoSnapshot::default()
            }
        );
    }
}
