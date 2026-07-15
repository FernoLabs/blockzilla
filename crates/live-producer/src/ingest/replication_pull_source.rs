//! Stop-and-wait pull source for an outbound Blockzilla replication client.
//!
//! The client controls neither stream identity nor offset. The source selects the oldest locally
//! durable raw-gRPC prefix not covered by its own fsynced cumulative-ACK WAL, serves one bounded
//! batch, and will serve exactly that batch again until Blockzilla returns a valid signed ACK.
//! Payload bytes are not retained in a second cache: a pending batch keeps only its fenced cursor,
//! exact limits, tail witness, and digest-chain expectation and re-reads the immutable local WAL on
//! retry.

use std::{
    error::Error,
    fmt,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
};

use futures::Stream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};

use crate::grpc_raw::{
    GrpcRawCommittedAdvance, GrpcRawCommittedCursor, GrpcRawCommittedReadLimits,
    GrpcRawLocalGcOutcome, gc_one_acknowledged_grpc_raw_generation,
    open_grpc_raw_replication_generation_cursors,
};

use super::{
    AuthenticatedClient, ClientCertificateAllowlist, ContentDigest, CumulativeAckSignatureVerifier,
    CumulativeAckWal, CumulativePrimaryAck, Ed25519ReceiptKeyring, ExpectedCumulativeAck,
    RawReplicationRecord, ReplicationStreamId, ValidatedCommitAckResponse,
    ValidatedPullBatchRequest, ValidatedPushRecord, VerifiedCumulativeAck, cumulative_chain_next,
    cumulative_chain_seed, verify_cumulative_ack, wire,
};

const PULL_CONTROL_MAX_DECODING_BYTES: usize = 64 * 1024;
const PULL_RECORD_WIRE_RESERVE_BYTES: u64 = 64 * 1024;
const MAX_PULL_MESSAGE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_PULL_BATCH_RECORDS: usize = 65_536;

/// Server-owned limits for a single stop-and-wait batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawReplicationPullLimits {
    pub max_records: usize,
    pub max_compressed_bytes: u64,
    pub max_uncompressed_bytes: u64,
}

impl RawReplicationPullLimits {
    fn validate(self, read_limits: GrpcRawCommittedReadLimits) -> Result<Self, PullSourceError> {
        if self.max_records == 0
            || self.max_records > MAX_PULL_BATCH_RECORDS
            || self.max_compressed_bytes == 0
            || self.max_uncompressed_bytes == 0
            || read_limits.max_compressed_record_bytes == 0
            || read_limits.max_uncompressed_record_bytes == 0
            || read_limits.max_compressed_record_bytes > self.max_compressed_bytes
            || read_limits.max_uncompressed_record_bytes > self.max_uncompressed_bytes
            || self.max_compressed_bytes > MAX_PULL_MESSAGE_BYTES
            || self.max_uncompressed_bytes > MAX_PULL_MESSAGE_BYTES
            || read_limits.max_compressed_record_bytes
                > MAX_PULL_MESSAGE_BYTES - PULL_RECORD_WIRE_RESERVE_BYTES
            || read_limits.max_uncompressed_record_bytes > MAX_PULL_MESSAGE_BYTES
        {
            return Err(pull_error(PullSourceErrorKind::InvalidConfiguration));
        }
        Ok(self)
    }
}

/// Construction inputs for the Hetzner pull source.
///
/// `gc_enabled` is deliberately false in [`Self::new`]. A canary therefore commits and fsyncs
/// Blockzilla ACKs without deleting local generations until an operator explicitly enables GC.
pub struct RawReplicationPullSourceConfig {
    pub cache_root: PathBuf,
    pub cumulative_ack_wal_file: PathBuf,
    pub read_limits: GrpcRawCommittedReadLimits,
    pub batch_limits: RawReplicationPullLimits,
    pub expected_primary_id: String,
    pub trusted_receipt_keys: Ed25519ReceiptKeyring,
    pub gc_enabled: bool,
}

impl RawReplicationPullSourceConfig {
    pub fn new(
        cache_root: PathBuf,
        cumulative_ack_wal_file: PathBuf,
        read_limits: GrpcRawCommittedReadLimits,
        batch_limits: RawReplicationPullLimits,
        expected_primary_id: String,
        trusted_receipt_keys: Ed25519ReceiptKeyring,
    ) -> Self {
        Self {
            cache_root,
            cumulative_ack_wal_file,
            read_limits,
            batch_limits,
            expected_primary_id,
            trusted_receipt_keys,
            gc_enabled: false,
        }
    }

    /// Explicitly opt into (or back out of) one-generation post-ACK GC.
    pub fn with_gc_enabled(mut self, enabled: bool) -> Self {
        self.gc_enabled = enabled;
        self
    }
}

impl fmt::Debug for RawReplicationPullSourceConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawReplicationPullSourceConfig")
            .field("cache_root", &self.cache_root)
            .field("cumulative_ack_wal_file", &self.cumulative_ack_wal_file)
            .field("read_limits", &self.read_limits)
            .field("batch_limits", &self.batch_limits)
            .field("expected_primary_id", &self.expected_primary_id)
            .field("trusted_receipt_keys", &self.trusted_receipt_keys)
            .field("gc_enabled", &self.gc_enabled)
            .finish()
    }
}

/// Stable, credential-free failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PullSourceErrorKind {
    InvalidConfiguration,
    AckWal,
    SourceCursor,
    Chain,
    InvalidAck,
    NoPendingBatch,
    PendingBatchChanged,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PullSourceError {
    kind: PullSourceErrorKind,
}

impl PullSourceError {
    pub fn kind(&self) -> PullSourceErrorKind {
        self.kind
    }
}

impl fmt::Debug for PullSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullSourceError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for PullSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            PullSourceErrorKind::InvalidConfiguration => {
                "invalid raw replication pull-source configuration"
            }
            PullSourceErrorKind::AckWal => "raw replication pull-source ACK WAL failed",
            PullSourceErrorKind::SourceCursor => "raw replication pull-source cursor failed",
            PullSourceErrorKind::Chain => "raw replication pull-source chain validation failed",
            PullSourceErrorKind::InvalidAck => {
                "Blockzilla cumulative ACK did not match the pending batch"
            }
            PullSourceErrorKind::NoPendingBatch => {
                "no raw replication pull batch is awaiting acknowledgement"
            }
            PullSourceErrorKind::PendingBatchChanged => {
                "pending raw replication pull batch changed on local WAL replay"
            }
        };
        formatter.write_str(message)
    }
}

impl Error for PullSourceError {}

/// One source-selected batch. There is intentionally no client-supplied cursor in this API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawReplicationPullBatch {
    stream: ReplicationStreamId,
    records: Vec<RawReplicationRecord>,
}

impl RawReplicationPullBatch {
    pub fn stream(&self) -> &ReplicationStreamId {
        &self.stream
    }

    pub fn records(&self) -> &[RawReplicationRecord] {
        &self.records
    }

    pub fn into_records(self) -> Vec<RawReplicationRecord> {
        self.records
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawReplicationPullOutcome {
    CaughtUp,
    Batch(RawReplicationPullBatch),
}

/// Result of optional retention maintenance after the ACK crossed its fsync boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullSourceGcResult {
    Disabled,
    Completed(GrpcRawLocalGcOutcome),
    /// ACK commit still succeeded. GC is maintenance and must not turn a durable CommitAck into a
    /// retry ambiguity.
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawReplicationPullCommit {
    pub stream: ReplicationStreamId,
    pub through_sequence: u64,
    /// True only when the exact already-fsynced ACK was replayed after a response was lost.
    pub replayed: bool,
    pub gc: PullSourceGcResult,
}

struct BackendReservedBatch<R> {
    stream: ReplicationStreamId,
    records: Vec<RawReplicationRecord>,
    reservation: R,
    previous_ack: Option<CumulativePrimaryAck>,
    minimum_primary_term: u64,
}

trait PullSourceBackend {
    type Reservation;

    fn reserve_next(
        &mut self,
        limits: RawReplicationPullLimits,
    ) -> Result<Option<BackendReservedBatch<Self::Reservation>>, PullSourceError>;

    fn replay_pending(
        &self,
        reservation: &Self::Reservation,
    ) -> Result<Vec<RawReplicationRecord>, PullSourceError>;

    fn commit_verified(
        &mut self,
        verified: VerifiedCumulativeAck,
        reservation: &mut Self::Reservation,
    ) -> Result<(), PullSourceError>;

    fn latest_ack(&self, stream: &ReplicationStreamId) -> Option<CumulativePrimaryAck>;

    fn gc_one(&mut self) -> Result<GrpcRawLocalGcOutcome, PullSourceError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedPending {
    through_sequence: u64,
    through_content_digest: ContentDigest,
    rolling_chain_digest: ContentDigest,
    record_count: usize,
}

struct PendingBatch<R> {
    stream: ReplicationStreamId,
    reservation: R,
    initial_chain: ContentDigest,
    expected: PreparedPending,
    minimum_primary_term: u64,
}

struct PullSourceCore<B: PullSourceBackend, V> {
    backend: B,
    verifier: V,
    expected_primary_id: String,
    limits: RawReplicationPullLimits,
    pending: Option<PendingBatch<B::Reservation>>,
    gc_enabled: bool,
}

impl<B, V> PullSourceCore<B, V>
where
    B: PullSourceBackend,
    V: CumulativeAckSignatureVerifier,
{
    fn pull_batch(&mut self) -> Result<RawReplicationPullOutcome, PullSourceError> {
        if let Some(pending) = self.pending.as_ref() {
            let records = self.backend.replay_pending(&pending.reservation)?;
            validate_replayed_pending(pending, &records)?;
            return Ok(RawReplicationPullOutcome::Batch(RawReplicationPullBatch {
                stream: pending.stream.clone(),
                records,
            }));
        }

        let Some(reserved) = self.backend.reserve_next(self.limits)? else {
            return Ok(RawReplicationPullOutcome::CaughtUp);
        };
        let initial_chain = match reserved.previous_ack.as_ref() {
            Some(previous) => previous.rolling_chain_digest,
            None => cumulative_chain_seed(&reserved.stream)
                .map_err(|_| pull_error(PullSourceErrorKind::Chain))?,
        };
        let expected = prepare_records(&reserved.stream, initial_chain, &reserved.records)?;
        let batch = RawReplicationPullBatch {
            stream: reserved.stream.clone(),
            records: reserved.records,
        };
        self.pending = Some(PendingBatch {
            stream: reserved.stream,
            reservation: reserved.reservation,
            initial_chain,
            expected,
            minimum_primary_term: reserved.minimum_primary_term,
        });
        Ok(RawReplicationPullOutcome::Batch(batch))
    }

    fn commit_ack(
        &mut self,
        ack: CumulativePrimaryAck,
    ) -> Result<RawReplicationPullCommit, PullSourceError> {
        let Some(pending) = self.pending.as_ref() else {
            if self.backend.latest_ack(&ack.stream).as_ref() == Some(&ack) {
                return Ok(RawReplicationPullCommit {
                    stream: ack.stream,
                    through_sequence: ack.through_sequence,
                    replayed: true,
                    gc: PullSourceGcResult::Disabled,
                });
            }
            return Err(pull_error(PullSourceErrorKind::NoPendingBatch));
        };

        let verified = verify_cumulative_ack(
            ack,
            ExpectedCumulativeAck {
                stream: &pending.stream,
                primary_id: &self.expected_primary_id,
                minimum_primary_term: pending.minimum_primary_term,
                through_sequence: pending.expected.through_sequence,
                through_content_digest: pending.expected.through_content_digest,
                rolling_chain_digest: pending.expected.rolling_chain_digest,
            },
            &self.verifier,
        )
        .map_err(|_| pull_error(PullSourceErrorKind::InvalidAck))?;

        // Keep the pending state installed throughout the fsync. The global source mutex prevents
        // another RPC from observing it concurrently, and a failed durable commit leaves it
        // untouched for an exact retry.
        let pending = self
            .pending
            .as_mut()
            .ok_or_else(|| pull_error(PullSourceErrorKind::NoPendingBatch))?;
        let committed_ack = verified.ack().clone();
        self.backend
            .commit_verified(verified, &mut pending.reservation)?;

        // The pending batch is cleared only after commit_verified returns from the ACK-WAL fsync
        // and exact cursor advance. GC is strictly later and disabled by default.
        self.pending = None;
        let gc = self.gc_one_if_enabled();
        Ok(RawReplicationPullCommit {
            stream: committed_ack.stream,
            through_sequence: committed_ack.through_sequence,
            replayed: false,
            gc,
        })
    }

    fn pending_stream(&self) -> Option<&ReplicationStreamId> {
        self.pending.as_ref().map(|pending| &pending.stream)
    }

    fn gc_one_if_enabled(&mut self) -> PullSourceGcResult {
        if !self.gc_enabled {
            return PullSourceGcResult::Disabled;
        }
        match self.backend.gc_one() {
            Ok(outcome) => PullSourceGcResult::Completed(outcome),
            Err(_) => PullSourceGcResult::Failed,
        }
    }
}

fn prepare_records(
    stream: &ReplicationStreamId,
    initial_chain: ContentDigest,
    records: &[RawReplicationRecord],
) -> Result<PreparedPending, PullSourceError> {
    let mut chain = initial_chain;
    for record in records {
        if ReplicationStreamId::from_offer(&record.offer) != *stream {
            return Err(pull_error(PullSourceErrorKind::Chain));
        }
        chain = cumulative_chain_next(stream, chain, &record.offer)
            .map_err(|_| pull_error(PullSourceErrorKind::Chain))?;
    }
    let tail = records
        .last()
        .ok_or_else(|| pull_error(PullSourceErrorKind::SourceCursor))?;
    Ok(PreparedPending {
        through_sequence: tail.offer.record.sequence,
        through_content_digest: tail.offer.content_digest,
        rolling_chain_digest: chain,
        record_count: records.len(),
    })
}

fn validate_replayed_pending<R>(
    pending: &PendingBatch<R>,
    records: &[RawReplicationRecord],
) -> Result<(), PullSourceError> {
    let replayed = prepare_records(&pending.stream, pending.initial_chain, records)
        .map_err(|_| pull_error(PullSourceErrorKind::PendingBatchChanged))?;
    if replayed != pending.expected {
        return Err(pull_error(PullSourceErrorKind::PendingBatchChanged));
    }
    Ok(())
}

struct ProductionReservation {
    cursor: GrpcRawCommittedCursor,
    advance: GrpcRawCommittedAdvance,
    record_count: usize,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    records_digest: [u8; 32],
}

struct ProductionPullBackend {
    cache_root: PathBuf,
    read_limits: GrpcRawCommittedReadLimits,
    ack_wal: CumulativeAckWal,
}

impl PullSourceBackend for ProductionPullBackend {
    type Reservation = ProductionReservation;

    fn reserve_next(
        &mut self,
        limits: RawReplicationPullLimits,
    ) -> Result<Option<BackendReservedBatch<Self::Reservation>>, PullSourceError> {
        let generations =
            open_grpc_raw_replication_generation_cursors(&self.cache_root, self.read_limits)
                .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;

        for generation in generations {
            let mut cursor = generation.cursor;
            let stream = cursor.stream();
            let previous_ack = self.ack_wal.latest_stream_ack(&stream).cloned();
            if generation.sealed {
                if let Some(ack) = previous_ack.as_ref() {
                    if cursor
                        .sealed_generation_is_covered_by(ack.through_sequence)
                        .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?
                    {
                        continue;
                    }
                }
            }

            let next_sequence = match previous_ack.as_ref() {
                Some(ack) => ack
                    .through_sequence
                    .checked_add(1)
                    .ok_or_else(|| pull_error(PullSourceErrorKind::SourceCursor))?,
                None if cursor.next_replication_sequence() == 0 => 0,
                None => return Err(pull_error(PullSourceErrorKind::SourceCursor)),
            };
            if next_sequence < cursor.next_replication_sequence() {
                return Err(pull_error(PullSourceErrorKind::SourceCursor));
            }
            cursor
                .seek_to_replication_sequence(next_sequence)
                .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;
            let batch = cursor
                .read_batch(
                    limits.max_records,
                    limits.max_compressed_bytes,
                    limits.max_uncompressed_bytes,
                )
                .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;
            if batch.records.is_empty() {
                continue;
            }
            let record_count = batch.records.len();
            let compressed_bytes = batch.compressed_bytes;
            let uncompressed_bytes = batch.uncompressed_bytes;
            let (records, advance) = batch
                .into_transport_parts()
                .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;
            let records_digest = digest_records(&records)?;
            let minimum_primary_term = self
                .ack_wal
                .highest_primary_term(&stream.cluster_id)
                .unwrap_or(0);
            return Ok(Some(BackendReservedBatch {
                stream,
                records,
                reservation: ProductionReservation {
                    cursor,
                    advance,
                    record_count,
                    compressed_bytes,
                    uncompressed_bytes,
                    records_digest,
                },
                previous_ack,
                minimum_primary_term,
            }));
        }
        Ok(None)
    }

    fn replay_pending(
        &self,
        reservation: &Self::Reservation,
    ) -> Result<Vec<RawReplicationRecord>, PullSourceError> {
        // Exact record count prevents an active-tail retry from growing after new appends. Exact
        // cumulative byte ceilings make the byte-boundary deterministic too.
        let batch = reservation
            .cursor
            .read_batch(
                reservation.record_count,
                reservation.compressed_bytes,
                reservation.uncompressed_bytes,
            )
            .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;
        if batch.records.len() != reservation.record_count
            || batch.compressed_bytes != reservation.compressed_bytes
            || batch.uncompressed_bytes != reservation.uncompressed_bytes
        {
            return Err(pull_error(PullSourceErrorKind::PendingBatchChanged));
        }
        let (records, advance) = batch
            .into_transport_parts()
            .map_err(|_| pull_error(PullSourceErrorKind::PendingBatchChanged))?;
        if advance != reservation.advance || digest_records(&records)? != reservation.records_digest
        {
            return Err(pull_error(PullSourceErrorKind::PendingBatchChanged));
        }
        Ok(records)
    }

    fn commit_verified(
        &mut self,
        verified: VerifiedCumulativeAck,
        reservation: &mut Self::Reservation,
    ) -> Result<(), PullSourceError> {
        let durable = self
            .ack_wal
            .commit_verified_replication(
                verified,
                reservation.advance.durable_replication_witness(),
            )
            .map_err(|_| pull_error(PullSourceErrorKind::AckWal))?;
        reservation
            .cursor
            .advance_after_durable_ack(&reservation.advance, &durable)
            .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))
    }

    fn latest_ack(&self, stream: &ReplicationStreamId) -> Option<CumulativePrimaryAck> {
        self.ack_wal.latest_stream_ack(stream).cloned()
    }

    fn gc_one(&mut self) -> Result<GrpcRawLocalGcOutcome, PullSourceError> {
        gc_one_acknowledged_grpc_raw_generation(&self.cache_root, &self.ack_wal, self.read_limits)
            .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))
    }
}

fn digest_records(records: &[RawReplicationRecord]) -> Result<[u8; 32], PullSourceError> {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    for record in records {
        digest.update(record.offer.record.sequence.to_le_bytes());
        digest.update(record.offer.content_digest.0);
        digest.update(record.offer.payload_len.to_le_bytes());
        digest.update(record.uncompressed_len.to_le_bytes());
        digest.update(record.raw_protobuf_sha256);
        let payload_len = u64::try_from(record.compressed_payload.len())
            .map_err(|_| pull_error(PullSourceErrorKind::Chain))?;
        digest.update(payload_len.to_le_bytes());
        digest.update(&record.compressed_payload);
    }
    Ok(digest.finalize().into())
}

/// Production stop-and-wait source state. Mutating methods require `&mut self`; the Tonic wrapper
/// adds one global request admission permit and one mutex around this state.
pub struct RawReplicationPullSource {
    inner: PullSourceCore<ProductionPullBackend, Ed25519ReceiptKeyring>,
    max_encoding_message_bytes: usize,
}

impl RawReplicationPullSource {
    pub fn open(config: RawReplicationPullSourceConfig) -> Result<Self, PullSourceError> {
        if !config.cache_root.is_absolute()
            || !config.cumulative_ack_wal_file.is_absolute()
            || config
                .cumulative_ack_wal_file
                .starts_with(&config.cache_root)
            || !valid_stable_id(&config.expected_primary_id)
        {
            return Err(pull_error(PullSourceErrorKind::InvalidConfiguration));
        }
        let limits = config.batch_limits.validate(config.read_limits)?;
        let max_encoding_message_bytes = config
            .read_limits
            .max_compressed_record_bytes
            .checked_add(PULL_RECORD_WIRE_RESERVE_BYTES)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| pull_error(PullSourceErrorKind::InvalidConfiguration))?;
        let ack_wal = CumulativeAckWal::open(&config.cumulative_ack_wal_file)
            .map_err(|_| pull_error(PullSourceErrorKind::AckWal))?;
        let canonical_cache_root = std::fs::canonicalize(&config.cache_root)
            .map_err(|_| pull_error(PullSourceErrorKind::InvalidConfiguration))?;
        let canonical_ack_wal = std::fs::canonicalize(&config.cumulative_ack_wal_file)
            .map_err(|_| pull_error(PullSourceErrorKind::InvalidConfiguration))?;
        if canonical_ack_wal.starts_with(canonical_cache_root) {
            return Err(pull_error(PullSourceErrorKind::InvalidConfiguration));
        }
        Ok(Self {
            inner: PullSourceCore {
                backend: ProductionPullBackend {
                    cache_root: config.cache_root,
                    read_limits: config.read_limits,
                    ack_wal,
                },
                verifier: config.trusted_receipt_keys,
                expected_primary_id: config.expected_primary_id,
                limits,
                pending: None,
                gc_enabled: config.gc_enabled,
            },
            max_encoding_message_bytes,
        })
    }

    pub fn pull_batch(&mut self) -> Result<RawReplicationPullOutcome, PullSourceError> {
        self.inner.pull_batch()
    }

    pub fn commit_ack(
        &mut self,
        ack: CumulativePrimaryAck,
    ) -> Result<RawReplicationPullCommit, PullSourceError> {
        self.inner.commit_ack(ack)
    }

    pub fn pending_stream(&self) -> Option<&ReplicationStreamId> {
        self.inner.pending_stream()
    }

    pub fn gc_enabled(&self) -> bool {
        self.inner.gc_enabled
    }

    pub fn set_gc_enabled(&mut self, enabled: bool) {
        self.inner.gc_enabled = enabled;
    }

    /// Run at most one proof-checked oldest-generation retention pass.
    ///
    /// This remains disabled until explicitly opted in. The underlying GC accepts only an ACK
    /// already recovered from this source's fsynced ACK WAL, so startup maintenance cannot promote
    /// a network value directly into deletion authority.
    pub fn gc_one_acknowledged_generation(&mut self) -> PullSourceGcResult {
        self.inner.gc_one_if_enabled()
    }

    fn discard_new_pending_for_unauthorized_stream(&mut self, stream: &ReplicationStreamId) {
        if self.inner.pending_stream() == Some(stream) {
            self.inner.pending = None;
        }
    }
}

impl fmt::Debug for RawReplicationPullSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawReplicationPullSource")
            .field("pending_stream", &self.pending_stream())
            .field("gc_enabled", &self.gc_enabled())
            .finish_non_exhaustive()
    }
}

/// mTLS-authenticated Tonic wrapper. Exactly one pull stream or CommitAck operation is admitted at
/// a time, preventing concurrent retries from multiplying one batch's memory footprint.
#[derive(Clone)]
pub struct RawReplicationPullService {
    source: Arc<Mutex<RawReplicationPullSource>>,
    allowlist: ClientCertificateAllowlist,
    admission: Arc<Semaphore>,
    max_encoding_message_bytes: usize,
    commit_observer: Option<Arc<dyn RawReplicationPullCommitObserver>>,
}

/// Non-authoritative monitoring hook invoked only after a cumulative ACK has crossed the source's
/// durable ACK-WAL boundary. Observer failure must be handled internally and can never revoke,
/// reinterpret, or turn an already durable CommitAck into a retry ambiguity.
pub trait RawReplicationPullCommitObserver: Send + Sync {
    fn after_durable_commit(&self, ack: &CumulativePrimaryAck);
}

impl RawReplicationPullService {
    pub fn new(source: RawReplicationPullSource, allowlist: ClientCertificateAllowlist) -> Self {
        let max_encoding_message_bytes = source.max_encoding_message_bytes;
        Self {
            source: Arc::new(Mutex::new(source)),
            allowlist,
            admission: Arc::new(Semaphore::new(1)),
            max_encoding_message_bytes,
            commit_observer: None,
        }
    }

    pub fn with_commit_observer(
        mut self,
        observer: Arc<dyn RawReplicationPullCommitObserver>,
    ) -> Self {
        self.commit_observer = Some(observer);
        self
    }

    pub fn into_tonic_service(
        self,
    ) -> wire::raw_replication_pull_server::RawReplicationPullServer<Self> {
        let maximum = self.max_encoding_message_bytes;
        wire::raw_replication_pull_server::RawReplicationPullServer::new(self)
            .max_decoding_message_size(PULL_CONTROL_MAX_DECODING_BYTES)
            .max_encoding_message_size(maximum)
    }

    fn authenticate<T>(&self, request: &Request<T>) -> Result<AuthenticatedClient, Status> {
        self.allowlist.authenticate_request(request)
    }

    fn acquire_admission(&self) -> Result<OwnedSemaphorePermit, Status> {
        self.admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("a pull batch is already in flight"))
    }
}

impl fmt::Debug for RawReplicationPullService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawReplicationPullService")
            .field("allowlist", &self.allowlist)
            .field(
                "max_encoding_message_bytes",
                &self.max_encoding_message_bytes,
            )
            .field("commit_observer", &self.commit_observer.is_some())
            .finish_non_exhaustive()
    }
}

pub struct RawReplicationPullResponseStream {
    records: std::vec::IntoIter<Result<wire::PushBatchRequest, Status>>,
    _permit: OwnedSemaphorePermit,
}

impl Stream for RawReplicationPullResponseStream {
    type Item = Result<wire::PushBatchRequest, Status>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().records.next())
    }
}

#[tonic::async_trait]
impl wire::raw_replication_pull_server::RawReplicationPull for RawReplicationPullService {
    type PullBatchStream = RawReplicationPullResponseStream;

    async fn pull_batch(
        &self,
        request: Request<wire::PullBatchRequest>,
    ) -> Result<Response<Self::PullBatchStream>, Status> {
        let authenticated = self.authenticate(&request)?;
        let _validated = ValidatedPullBatchRequest::try_from(request.into_inner())
            .map_err(|_| Status::invalid_argument("unsupported pull protocol request"))?;
        let permit = self.acquire_admission()?;
        let source = Arc::clone(&self.source);
        let outcome = tokio::task::spawn_blocking(move || {
            let mut source = source
                .lock()
                .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;
            let already_pending = source.pending_stream().is_some();
            let outcome = source.pull_batch()?;
            let maintenance = if matches!(outcome, RawReplicationPullOutcome::CaughtUp) {
                Some(source.gc_one_acknowledged_generation())
            } else {
                None
            };
            Ok::<_, PullSourceError>((outcome, already_pending, maintenance))
        })
        .await
        .map_err(|_| Status::internal("pull source task failed"))?
        .map_err(pull_status)?;

        let (outcome, already_pending, maintenance) = outcome;
        if let Some(maintenance) = maintenance {
            report_pull_source_gc(maintenance, "caught_up_poll");
        }
        let records = match outcome {
            RawReplicationPullOutcome::CaughtUp => Vec::new(),
            RawReplicationPullOutcome::Batch(batch) => {
                if !authenticated.permits(batch.stream()) {
                    if !already_pending {
                        let selected_stream = batch.stream().clone();
                        let source = Arc::clone(&self.source);
                        tokio::task::spawn_blocking(move || {
                            source
                                .lock()
                                .map_err(|_| ())?
                                .discard_new_pending_for_unauthorized_stream(&selected_stream);
                            Ok::<_, ()>(())
                        })
                        .await
                        .map_err(|_| Status::internal("pull source task failed"))?
                        .map_err(|_| Status::internal("pull source state is unavailable"))?;
                    }
                    return Err(Status::permission_denied(
                        "client certificate is not authorized for the selected stream",
                    ));
                }
                let stream = batch.stream().clone();
                batch
                    .into_records()
                    .into_iter()
                    .map(|record| {
                        ValidatedPushRecord::new(stream.clone(), record)
                            .and_then(wire::PushBatchRequest::try_from)
                            .map_err(|_| Status::internal("could not encode pulled WAL record"))
                    })
                    .collect()
            }
        };
        Ok(Response::new(RawReplicationPullResponseStream {
            records: records.into_iter(),
            _permit: permit,
        }))
    }

    async fn commit_ack(
        &self,
        request: Request<wire::CumulativePrimaryAck>,
    ) -> Result<Response<wire::CommitAckResponse>, Status> {
        let authenticated = self.authenticate(&request)?;
        let ack: CumulativePrimaryAck = request
            .into_inner()
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid cumulative acknowledgement"))?;
        if !authenticated.permits(&ack.stream) {
            return Err(Status::permission_denied(
                "client certificate is not authorized for the acknowledged stream",
            ));
        }
        let _permit = self.acquire_admission()?;
        let source = Arc::clone(&self.source);
        let commit_observer = self.commit_observer.clone();
        let status_ack = ack.clone();
        let committed = tokio::task::spawn_blocking(move || {
            let result = source
                .lock()
                .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?
                .commit_ack(ack);
            observe_successful_durable_commit(result, &status_ack, commit_observer.as_deref())
        })
        .await
        .map_err(|_| Status::internal("pull ACK task failed"))?
        .map_err(pull_status)?;
        report_pull_source_gc(committed.gc.clone(), "commit_ack");
        let response =
            ValidatedCommitAckResponse::new(committed.stream, committed.through_sequence)
                .and_then(|response| wire::CommitAckResponse::try_from(&response))
                .map_err(|_| Status::internal("could not encode ACK commit response"))?;
        Ok(Response::new(response))
    }
}

fn observe_successful_durable_commit<E>(
    result: Result<RawReplicationPullCommit, E>,
    ack: &CumulativePrimaryAck,
    observer: Option<&dyn RawReplicationPullCommitObserver>,
) -> Result<RawReplicationPullCommit, E> {
    let committed = result?;
    // A response-loss retry proves no new Blockzilla progress. Refreshing the
    // monitoring timestamp here would let an old ACK suppress the stale-copy
    // alert indefinitely. If the process crashed after the ACK-WAL fsync but
    // before status publication, remain conservatively stale until a new ACK.
    if !committed.replayed
        && let Some(observer) = observer
    {
        observer.after_durable_commit(ack);
    }
    Ok(committed)
}

fn report_pull_source_gc(result: PullSourceGcResult, maintenance_phase: &'static str) {
    match result {
        PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::Retired {
            through_sequence, ..
        }) => tracing::info!(
            maintenance_phase,
            through_sequence,
            "retired one ACK-covered raw generation"
        ),
        PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::Busy) => tracing::debug!(
            maintenance_phase,
            "raw-generation retention is busy; a later pass will retry"
        ),
        PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::NoDurableAck) => tracing::debug!(
            maintenance_phase,
            "raw-generation retention is waiting for the first durable ACK"
        ),
        PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::AckDoesNotCoverOldest {
            oldest_through_sequence,
            durable_through_sequence,
        }) => tracing::debug!(
            maintenance_phase,
            oldest_through_sequence,
            durable_through_sequence,
            "oldest raw generation is not covered by the durable ACK"
        ),
        PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::NothingToRetire)
        | PullSourceGcResult::Disabled => {}
        PullSourceGcResult::Failed => tracing::warn!(
            maintenance_phase,
            "raw-generation retention failed; durable ACK state remains authoritative"
        ),
    }
}

fn pull_status(error: PullSourceError) -> Status {
    match error.kind() {
        PullSourceErrorKind::InvalidAck | PullSourceErrorKind::NoPendingBatch => {
            Status::failed_precondition(error.to_string())
        }
        PullSourceErrorKind::InvalidConfiguration => Status::internal(error.to_string()),
        PullSourceErrorKind::AckWal
        | PullSourceErrorKind::SourceCursor
        | PullSourceErrorKind::Chain
        | PullSourceErrorKind::PendingBatchChanged => Status::unavailable(error.to_string()),
    }
}

fn valid_stable_id(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric())
        && value.len() <= 64
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
}

fn pull_error(kind: PullSourceErrorKind) -> PullSourceError {
    PullSourceError { kind }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{
        CommitmentEvidence, LogicalKey, ObservationId, RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
        REPLICATION_PROTOCOL_VERSION, ReceiptDisposition, ReplicationOffer, compute_content_digest,
    };
    use std::{
        collections::{HashMap, VecDeque},
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct CountingObserver(AtomicUsize);

    impl RawReplicationPullCommitObserver for CountingObserver {
        fn after_durable_commit(&self, _ack: &CumulativePrimaryAck) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct AcceptVerifier;

    impl CumulativeAckSignatureVerifier for AcceptVerifier {
        fn verify_cumulative_ack_signature(
            &self,
            _key_id: &str,
            _signing_bytes: &[u8],
            signature: &[u8],
        ) -> bool {
            signature == [7u8; 64]
        }
    }

    #[derive(Clone)]
    struct FakeReservation {
        records: Vec<RawReplicationRecord>,
    }

    #[derive(Default)]
    struct FakeBackend {
        queued: VecDeque<(ReplicationStreamId, Vec<RawReplicationRecord>)>,
        latest: HashMap<ReplicationStreamId, CumulativePrimaryAck>,
        reserve_calls: usize,
        commit_calls: usize,
        gc_calls: usize,
        fail_commit: bool,
    }

    impl PullSourceBackend for FakeBackend {
        type Reservation = FakeReservation;

        fn reserve_next(
            &mut self,
            _limits: RawReplicationPullLimits,
        ) -> Result<Option<BackendReservedBatch<Self::Reservation>>, PullSourceError> {
            self.reserve_calls += 1;
            let Some((stream, records)) = self.queued.pop_front() else {
                return Ok(None);
            };
            let previous_ack = self.latest.get(&stream).cloned();
            Ok(Some(BackendReservedBatch {
                stream,
                records: records.clone(),
                reservation: FakeReservation { records },
                previous_ack,
                minimum_primary_term: 0,
            }))
        }

        fn replay_pending(
            &self,
            reservation: &Self::Reservation,
        ) -> Result<Vec<RawReplicationRecord>, PullSourceError> {
            Ok(reservation.records.clone())
        }

        fn commit_verified(
            &mut self,
            verified: VerifiedCumulativeAck,
            _reservation: &mut Self::Reservation,
        ) -> Result<(), PullSourceError> {
            self.commit_calls += 1;
            if self.fail_commit {
                return Err(pull_error(PullSourceErrorKind::AckWal));
            }
            let ack = verified.ack().clone();
            self.latest.insert(ack.stream.clone(), ack);
            Ok(())
        }

        fn latest_ack(&self, stream: &ReplicationStreamId) -> Option<CumulativePrimaryAck> {
            self.latest.get(stream).cloned()
        }

        fn gc_one(&mut self) -> Result<GrpcRawLocalGcOutcome, PullSourceError> {
            self.gc_calls += 1;
            Ok(GrpcRawLocalGcOutcome::NothingToRetire)
        }
    }

    fn stream() -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: "solana-mainnet".into(),
            origin_node_id: "hetzner-1".into(),
            source_id: "triton-blocks".into(),
            journal_id: [3u8; 16],
        }
    }

    fn record(sequence: u64) -> RawReplicationRecord {
        let stream = stream();
        let compressed_payload = vec![sequence as u8 + 1, 2, 3];
        let logical_key = LogicalKey::Block {
            slot: 1000 + sequence,
            blockhash: [sequence as u8; 32],
        };
        let content_digest = compute_content_digest(
            &stream.cluster_id,
            &logical_key,
            RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
            &compressed_payload,
        );
        RawReplicationRecord {
            offer: ReplicationOffer {
                protocol_version: REPLICATION_PROTOCOL_VERSION,
                cluster_id: stream.cluster_id,
                record: ObservationId {
                    origin_node_id: stream.origin_node_id,
                    journal_id: stream.journal_id,
                    sequence,
                },
                source_id: stream.source_id,
                logical_key,
                content_digest,
                payload_len: compressed_payload.len() as u64,
                payload_format_version: RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
                commitment: CommitmentEvidence::Processed,
            },
            compressed_payload,
            uncompressed_len: 10,
            raw_protobuf_sha256: [sequence as u8 + 9; 32],
        }
    }

    fn core(
        records: Vec<RawReplicationRecord>,
        gc_enabled: bool,
    ) -> PullSourceCore<FakeBackend, AcceptVerifier> {
        let mut backend = FakeBackend::default();
        backend.queued.push_back((stream(), records));
        PullSourceCore {
            backend,
            verifier: AcceptVerifier,
            expected_primary_id: "blockzilla-primary".into(),
            limits: RawReplicationPullLimits {
                max_records: 10,
                max_compressed_bytes: 1024,
                max_uncompressed_bytes: 4096,
            },
            pending: None,
            gc_enabled,
        }
    }

    fn ack_for(core: &PullSourceCore<FakeBackend, AcceptVerifier>) -> CumulativePrimaryAck {
        let pending = core.pending.as_ref().expect("pending batch");
        CumulativePrimaryAck {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            stream: pending.stream.clone(),
            primary_id: core.expected_primary_id.clone(),
            primary_term: 1,
            through_sequence: pending.expected.through_sequence,
            through_content_digest: pending.expected.through_content_digest,
            rolling_chain_digest: pending.expected.rolling_chain_digest,
            disposition: ReceiptDisposition::DurablyStored,
            durable_lsn: 1,
            signing_key_id: "test-key".into(),
            signature: vec![7u8; 64],
        }
    }

    #[test]
    fn pull_api_has_no_client_offset_and_selects_backend_head() {
        let mut core = core(vec![record(0)], false);
        let RawReplicationPullOutcome::Batch(batch) = core.pull_batch().unwrap() else {
            panic!("expected source-selected batch");
        };
        assert_eq!(batch.stream(), &stream());
        assert_eq!(batch.records()[0].offer.record.sequence, 0);
        assert_eq!(core.backend.reserve_calls, 1);
    }

    #[test]
    fn retry_replays_the_same_pending_batch_without_reserving_again() {
        let mut core = core(vec![record(0), record(1)], false);
        let first = core.pull_batch().unwrap();
        let second = core.pull_batch().unwrap();
        assert_eq!(first, second);
        assert_eq!(core.backend.reserve_calls, 1);
    }

    #[test]
    fn invalid_ack_does_not_advance_or_run_gc() {
        let mut core = core(vec![record(0)], true);
        let expected_batch = core.pull_batch().unwrap();
        let mut ack = ack_for(&core);
        ack.through_sequence += 1;
        assert_eq!(
            core.commit_ack(ack).unwrap_err().kind(),
            PullSourceErrorKind::InvalidAck
        );
        assert_eq!(core.backend.commit_calls, 0);
        assert_eq!(core.backend.gc_calls, 0);
        assert_eq!(core.pull_batch().unwrap(), expected_batch);
    }

    #[test]
    fn valid_ack_crosses_durable_backend_before_pending_advances() {
        let mut core = core(vec![record(0)], false);
        core.pull_batch().unwrap();
        let ack = ack_for(&core);
        let committed = core.commit_ack(ack.clone()).unwrap();
        assert_eq!(committed.stream, stream());
        assert_eq!(committed.through_sequence, 0);
        assert!(!committed.replayed);
        assert_eq!(committed.gc, PullSourceGcResult::Disabled);
        assert_eq!(core.backend.commit_calls, 1);
        assert_eq!(core.backend.latest.get(&stream()), Some(&ack));
        assert!(core.pending.is_none());
        assert_eq!(
            core.pull_batch().unwrap(),
            RawReplicationPullOutcome::CaughtUp
        );
    }

    #[test]
    fn explicitly_enabled_gc_runs_only_after_a_valid_durable_commit() {
        let mut core = core(vec![record(0)], true);
        core.pull_batch().unwrap();
        let ack = ack_for(&core);
        let committed = core.commit_ack(ack).unwrap();
        assert_eq!(core.backend.commit_calls, 1);
        assert_eq!(core.backend.gc_calls, 1);
        assert_eq!(
            committed.gc,
            PullSourceGcResult::Completed(GrpcRawLocalGcOutcome::NothingToRetire)
        );
        assert!(core.pending.is_none());
    }

    #[test]
    fn exact_commit_ack_retry_is_idempotent_after_response_loss() {
        let mut core = core(vec![record(0)], false);
        core.pull_batch().unwrap();
        let ack = ack_for(&core);
        core.commit_ack(ack.clone()).unwrap();
        let replay = core.commit_ack(ack).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.through_sequence, 0);
        assert_eq!(core.backend.commit_calls, 1);
        assert_eq!(core.backend.gc_calls, 0);
    }

    #[test]
    fn failed_durable_commit_keeps_exact_pending_batch_and_skips_gc() {
        let mut core = core(vec![record(0)], true);
        let expected_batch = core.pull_batch().unwrap();
        let ack = ack_for(&core);
        core.backend.fail_commit = true;
        assert_eq!(
            core.commit_ack(ack).unwrap_err().kind(),
            PullSourceErrorKind::AckWal
        );
        assert!(core.pending.is_some());
        assert_eq!(core.backend.gc_calls, 0);
        assert_eq!(core.pull_batch().unwrap(), expected_batch);
    }

    #[test]
    fn monitoring_observer_runs_only_after_a_successful_durable_commit_result() {
        let mut core = core(vec![record(0)], false);
        core.pull_batch().unwrap();
        let ack = ack_for(&core);
        let observer = CountingObserver::default();
        let failed: Result<RawReplicationPullCommit, PullSourceError> =
            Err(pull_error(PullSourceErrorKind::AckWal));
        assert!(observe_successful_durable_commit(failed, &ack, Some(&observer)).is_err());
        assert_eq!(observer.0.load(Ordering::SeqCst), 0);

        let committed = RawReplicationPullCommit {
            stream: ack.stream.clone(),
            through_sequence: ack.through_sequence,
            replayed: false,
            gc: PullSourceGcResult::Disabled,
        };
        observe_successful_durable_commit(
            Ok::<_, PullSourceError>(committed),
            &ack,
            Some(&observer),
        )
        .unwrap();
        assert_eq!(observer.0.load(Ordering::SeqCst), 1);

        let replay = RawReplicationPullCommit {
            stream: ack.stream.clone(),
            through_sequence: ack.through_sequence,
            replayed: true,
            gc: PullSourceGcResult::Disabled,
        };
        observe_successful_durable_commit(Ok::<_, PullSourceError>(replay), &ack, Some(&observer))
            .unwrap();
        assert_eq!(observer.0.load(Ordering::SeqCst), 1);
    }
}
