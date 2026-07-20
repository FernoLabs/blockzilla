//! Stop-and-wait pull source for an outbound Blockzilla replication client.
//!
//! The client controls neither stream identity nor offset. The source selects the oldest locally
//! durable raw-gRPC prefix not covered by its own fsynced cumulative-ACK WAL, serves one bounded
//! batch, and will serve exactly that batch again until Blockzilla returns a valid signed ACK.
//! Payload bytes are not retained in a second cache: a pending batch keeps only its fenced cursor,
//! exact limits, tail witness, and digest-chain expectation and re-reads the immutable local WAL on
//! retry. Protocol v1 exposes this state machine as `PullBatch` plus `CommitAck`; protocol v2 keeps
//! one bidirectional `Sync` RPC open but deliberately preserves the same single-batch window and
//! durable commit boundary.

use std::{
    error::Error,
    fmt,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::Context;
use futures::{Stream, StreamExt};
use serde::Deserialize;
use sha2::Digest;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tonic::{Request, Response, Status};

use crate::grpc_raw::{
    GrpcRawCommittedAdvance, GrpcRawCommittedCursor, GrpcRawCommittedReadLimits,
    GrpcRawLocalGcOutcome, gc_one_acknowledged_grpc_raw_generation,
    open_grpc_raw_replication_generation_cursors,
};

use super::{
    AuthenticatedClient, ClientCertificateAllowlist, CommitmentEvidence, ContentDigest,
    CumulativeAckSignatureVerifier, CumulativeAckWal, CumulativePrimaryAck,
    DurableReplicationWitness, DurableSpoolRecord, Ed25519ReceiptKeyring, ExpectedCumulativeAck,
    LogicalKey, RawReplicationRecord, ReplicationOffer, ReplicationStreamId, SpoolJournalIdentity,
    SpoolLocation, SpoolRecord, ValidatedCommitAckResponse, ValidatedPullBatchRequest,
    ValidatedPushRecord, VerifiedCumulativeAck, cumulative_chain_next, cumulative_chain_seed,
    decode_stored_shred, parse_shred_header, read_spool_committed_snapshot_after,
    verify_cumulative_ack, wire,
};
use crate::ingest::shred_udp::{RAW_SOLANA_SHRED_V1, ZSTD_SOLANA_SHRED_V1};

const PULL_CONTROL_MAX_DECODING_BYTES: usize = 64 * 1024;
const PULL_RECORD_WIRE_RESERVE_BYTES: u64 = 64 * 1024;
const MAX_PULL_MESSAGE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_PULL_BATCH_RECORDS: usize = 65_536;
pub const RAW_REPLICATION_SYNC_PROTOCOL_VERSION: u32 = 2;
const SYNC_CLIENT_HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// The current degraded-path budget permits a 128 MiB batch to take up to ten minutes through the
/// local durable sink. Five additional minutes cover network and ACK-WAL fsync variance without
/// allowing an outstanding batch to monopolize the sole admission permit forever.
const SYNC_ACK_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const SYNC_TAIL_POLL_INTERVAL: Duration = Duration::from_millis(250);
const SYNC_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const SYNC_CAUGHT_UP_GC_INTERVAL: Duration = Duration::from_secs(60);
const SYNC_RESPONSE_CHANNEL_CAPACITY: usize = 8;

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

/// Construction inputs for the Source node pull source.
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

/// Immutable source settings for raw shred replication. The recorder status is used only to
/// expose the writer's fsynced prefix; an ACK is never used to trim this spool.
pub struct ShredSpoolPullSourceConfig {
    pub spool_root: PathBuf,
    pub identity: SpoolJournalIdentity,
    pub recorder_status_file: PathBuf,
    pub cumulative_ack_wal_file: PathBuf,
    pub max_stored_record_bytes: u64,
    pub batch_limits: RawReplicationPullLimits,
    pub expected_primary_id: String,
    pub trusted_receipt_keys: Ed25519ReceiptKeyring,
}

struct ShredSpoolReservation {
    before: Option<SpoolLocation>,
    after: SpoolLocation,
    durable_through_sequence: u64,
    record_count: usize,
    records_digest: [u8; 32],
    witness: DurableReplicationWitness,
}

struct ShredSpoolPullBackend {
    spool_root: PathBuf,
    identity: SpoolJournalIdentity,
    recorder_status_file: PathBuf,
    max_stored_record_bytes: u64,
    ack_wal: CumulativeAckWal,
    after: Option<SpoolLocation>,
}

impl ShredSpoolPullBackend {
    fn durable_through_sequence(&self) -> Result<Option<u64>, PullSourceError> {
        #[derive(Deserialize)]
        struct RecorderStatus {
            schema_version: u32,
            durable_through_sequence: Option<u64>,
        }

        let bytes = std::fs::read(&self.recorder_status_file)
            .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;
        let status: RecorderStatus = serde_json::from_slice(&bytes)
            .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;
        if status.schema_version != 1 {
            return Err(pull_error(PullSourceErrorKind::SourceCursor));
        }
        Ok(status.durable_through_sequence)
    }

    fn records_after(
        &self,
        after: Option<SpoolLocation>,
        durable_through_sequence: u64,
        max_records: usize,
    ) -> Result<Vec<(RawReplicationRecord, DurableSpoolRecord, SpoolLocation)>, PullSourceError>
    {
        let mut records = Vec::new();
        read_spool_committed_snapshot_after(
            &self.spool_root,
            self.identity.clone(),
            self.max_stored_record_bytes,
            after,
            durable_through_sequence,
            max_records,
            |record| {
                let transport = shred_spool_record_to_transport(&record)?;
                let durable = DurableSpoolRecord::from_verified_committed_read(
                    record.location,
                    record.metadata,
                );
                records.push((transport, durable, record.location));
                Ok(())
            },
        )
        .map_err(|error| {
            tracing::warn!(error = ?error, "read raw-shred pull spool snapshot failed");
            pull_error(PullSourceErrorKind::SourceCursor)
        })?;
        Ok(records)
    }

    fn locate_sequence(
        &self,
        sequence: u64,
        durable_through_sequence: u64,
    ) -> Result<SpoolLocation, PullSourceError> {
        let mut found = None;
        read_spool_committed_snapshot_after(
            &self.spool_root,
            self.identity.clone(),
            self.max_stored_record_bytes,
            None,
            durable_through_sequence,
            usize::MAX,
            |record| {
                if record.metadata.observation.sequence == sequence {
                    found = Some(record.location);
                }
                Ok(())
            },
        )
        .map_err(|error| {
            tracing::warn!(error = ?error, "locate raw-shred pull spool sequence failed");
            pull_error(PullSourceErrorKind::SourceCursor)
        })?;
        found.ok_or_else(|| pull_error(PullSourceErrorKind::SourceCursor))
    }
}

impl PullSourceBackend for ShredSpoolPullBackend {
    type Reservation = ShredSpoolReservation;

    fn reserve_next(
        &mut self,
        limits: RawReplicationPullLimits,
    ) -> Result<Option<BackendReservedBatch<Self::Reservation>>, PullSourceError> {
        let stream = ReplicationStreamId {
            cluster_id: self.identity.cluster_id.clone(),
            origin_node_id: self.identity.origin_node_id.clone(),
            source_id: self.identity.source_id.clone(),
            journal_id: self.identity.journal_id,
        };
        let previous_ack = self.ack_wal.latest_stream_ack(&stream).cloned();
        let next_sequence = match previous_ack.as_ref() {
            Some(ack) => ack
                .through_sequence
                .checked_add(1)
                .ok_or_else(|| pull_error(PullSourceErrorKind::SourceCursor))?,
            None => 0,
        };
        let Some(durable_through_sequence) = self.durable_through_sequence()? else {
            return Ok(None);
        };
        if next_sequence > durable_through_sequence {
            return Ok(None);
        }
        if self.after.is_none() && next_sequence > 0 {
            self.after = Some(self.locate_sequence(next_sequence - 1, durable_through_sequence)?);
        }
        let entries =
            self.records_after(self.after, durable_through_sequence, limits.max_records)?;
        let Some((first, _, _)) = entries.first() else {
            return Ok(None);
        };
        if first.offer.record.sequence != next_sequence {
            return Err(pull_error(PullSourceErrorKind::SourceCursor));
        }
        let (records, tail_record, after) = entries.into_iter().fold(
            (Vec::new(), None, None),
            |(mut records, _tail, _after), (record, durable, location)| {
                records.push(record);
                (records, Some(durable), Some(location))
            },
        );
        let tail_record =
            tail_record.ok_or_else(|| pull_error(PullSourceErrorKind::SourceCursor))?;
        let after = after.ok_or_else(|| pull_error(PullSourceErrorKind::SourceCursor))?;
        let record_count = records.len();
        let records_digest = digest_records(&records)?;
        let tail = records
            .last()
            .ok_or_else(|| pull_error(PullSourceErrorKind::SourceCursor))?;
        let witness = match tail_record.metadata().payload_format_version {
            RAW_SOLANA_SHRED_V1 => DurableReplicationWitness::from_verified_transcoded_mapping(
                &tail_record,
                stream.clone(),
                tail.offer.record.sequence,
                tail.offer.content_digest,
            ),
            ZSTD_SOLANA_SHRED_V1 => DurableReplicationWitness::from_verified_mapping(
                &tail_record,
                stream.clone(),
                tail.offer.record.sequence,
                tail.offer.content_digest,
            ),
            _ => Err(anyhow::anyhow!("unexpected raw-shred spool format")),
        }
        .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;
        let minimum_primary_term = self
            .ack_wal
            .highest_primary_term(&stream.cluster_id)
            .unwrap_or(0);
        Ok(Some(BackendReservedBatch {
            stream,
            records,
            reservation: ShredSpoolReservation {
                before: self.after,
                after,
                durable_through_sequence,
                record_count,
                records_digest,
                witness,
            },
            previous_ack,
            minimum_primary_term,
        }))
    }

    fn replay_pending(
        &self,
        reservation: &Self::Reservation,
    ) -> Result<Vec<RawReplicationRecord>, PullSourceError> {
        let entries = self.records_after(
            reservation.before,
            reservation.durable_through_sequence,
            reservation.record_count,
        )?;
        let records = entries
            .into_iter()
            .map(|(record, _, _)| record)
            .collect::<Vec<_>>();
        if records.len() != reservation.record_count
            || digest_records(&records)? != reservation.records_digest
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
        self.ack_wal
            .commit_verified_replication(verified, &reservation.witness)
            .map_err(|_| pull_error(PullSourceErrorKind::AckWal))?;
        self.after = Some(reservation.after);
        Ok(())
    }

    fn latest_ack(&self, stream: &ReplicationStreamId) -> Option<CumulativePrimaryAck> {
        self.ack_wal.latest_stream_ack(stream).cloned()
    }

    fn gc_one(&mut self) -> Result<GrpcRawLocalGcOutcome, PullSourceError> {
        Ok(GrpcRawLocalGcOutcome::NothingToRetire)
    }
}

fn shred_spool_record_to_transport(record: &SpoolRecord) -> anyhow::Result<RawReplicationRecord> {
    // The initial Hetzner recorder used uncompressed raw shred frames (v3). Newer recorders
    // store the same datagram in an independent zstd frame (v4). Both are lossless and retain
    // the exact Solana shred bytes; accept v3 only as a read-compatibility path for an existing
    // durable spool, while all new recordings use v4.
    let (raw, compressed_payload) = match record.metadata.payload_format_version {
        // Replay historical frames as the current canonical wire/spool format.  The NAS receiver
        // intentionally accepts only independently zstd-compressed shreds, so forwarding v3 as
        // raw bytes would make an otherwise valid historical spool permanently unreplicable.
        RAW_SOLANA_SHRED_V1 => {
            let raw = record.payload.clone();
            let compressed = zstd::bulk::compress(&raw, 1)
                .context("compress historical raw shred for durable replication")?;
            (raw, compressed)
        }
        ZSTD_SOLANA_SHRED_V1 => (
            decode_stored_shred(&record.payload)?,
            record.payload.clone(),
        ),
        _ => anyhow::bail!("unexpected raw-shred spool format"),
    };
    let header = parse_shred_header(&raw).context("parse stored raw shred")?;
    let expected = LogicalKey::Shred {
        slot: header.slot,
        kind: header.kind,
        shred_index: header.index,
        fec_set_index: Some(header.fec_set_index),
    };
    anyhow::ensure!(
        record.metadata.logical_key == expected,
        "stored raw shred key mismatch"
    );
    let raw_hash = sha2::Sha256::digest(&raw).into();
    Ok(RawReplicationRecord {
        offer: ReplicationOffer {
            protocol_version: super::REPLICATION_PROTOCOL_VERSION,
            cluster_id: record.metadata.cluster_id.clone(),
            record: record.metadata.observation.clone(),
            source_id: record.metadata.source_id.clone(),
            logical_key: record.metadata.logical_key.clone(),
            content_digest: super::compute_content_digest(
                &record.metadata.cluster_id,
                &record.metadata.logical_key,
                ZSTD_SOLANA_SHRED_V1,
                &compressed_payload,
            ),
            payload_len: u64::try_from(compressed_payload.len())
                .context("compressed raw shred length overflow")?,
            payload_format_version: ZSTD_SOLANA_SHRED_V1,
            commitment: CommitmentEvidence::Unknown,
        },
        compressed_payload,
        raw_protobuf_sha256: raw_hash,
        uncompressed_len: u64::try_from(raw.len()).context("stored raw shred length overflow")?,
    })
}

pub struct ShredSpoolPullSource {
    inner: PullSourceCore<ShredSpoolPullBackend, Ed25519ReceiptKeyring>,
    max_encoding_message_bytes: usize,
}

impl ShredSpoolPullSource {
    pub fn open(config: ShredSpoolPullSourceConfig) -> Result<Self, PullSourceError> {
        if !config.spool_root.is_absolute()
            || !config.recorder_status_file.is_absolute()
            || !config.cumulative_ack_wal_file.is_absolute()
            || config.recorder_status_file.starts_with(&config.spool_root)
            || config
                .cumulative_ack_wal_file
                .starts_with(&config.spool_root)
            || config.max_stored_record_bytes == 0
            || !valid_stable_id(&config.expected_primary_id)
        {
            return Err(pull_error(PullSourceErrorKind::InvalidConfiguration));
        }
        let limits = config.batch_limits.validate(GrpcRawCommittedReadLimits {
            max_compressed_record_bytes: config.max_stored_record_bytes,
            max_uncompressed_record_bytes: config.max_stored_record_bytes,
        })?;
        let max_encoding_message_bytes = config
            .max_stored_record_bytes
            .checked_add(PULL_RECORD_WIRE_RESERVE_BYTES)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| pull_error(PullSourceErrorKind::InvalidConfiguration))?;
        let ack_wal = CumulativeAckWal::open(&config.cumulative_ack_wal_file).map_err(|error| {
            // This is local filesystem state only; retain the detailed cause for operators while
            // continuing to expose a redacted protocol error to callers.
            tracing::error!(error = ?error, "open raw-shred pull ACK WAL failed");
            pull_error(PullSourceErrorKind::AckWal)
        })?;
        Ok(Self {
            inner: PullSourceCore {
                backend: ShredSpoolPullBackend {
                    spool_root: config.spool_root,
                    identity: config.identity,
                    recorder_status_file: config.recorder_status_file,
                    max_stored_record_bytes: config.max_stored_record_bytes,
                    ack_wal,
                    after: None,
                },
                verifier: config.trusted_receipt_keys,
                expected_primary_id: config.expected_primary_id,
                limits,
                pending: None,
                gc_enabled: false,
            },
            max_encoding_message_bytes,
        })
    }
}

impl SyncPullSource for ShredSpoolPullSource {
    fn source_batch_limits(&self) -> RawReplicationPullLimits {
        self.inner.limits
    }

    fn source_max_encoding_message_bytes(&self) -> usize {
        self.max_encoding_message_bytes
    }

    fn pull_for_sync(&mut self) -> Result<RawReplicationPullOutcome, PullSourceError> {
        self.inner.pull_batch()
    }

    fn commit_for_sync(
        &mut self,
        ack: CumulativePrimaryAck,
    ) -> Result<RawReplicationPullCommit, PullSourceError> {
        self.inner.commit_ack(ack)
    }

    fn pending_stream_for_sync(&self) -> Option<&ReplicationStreamId> {
        self.inner.pending_stream()
    }

    fn discard_new_pending_for_sync(&mut self, stream: &ReplicationStreamId) {
        if self.inner.pending_stream() == Some(stream) {
            self.inner.pending = None;
        }
    }

    fn gc_for_sync(&mut self) -> PullSourceGcResult {
        PullSourceGcResult::Disabled
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

    fn batch_limits(&self) -> RawReplicationPullLimits {
        self.inner.limits
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

/// Narrow adapter used by the protocol-v2 session driver. Production calls still enter the exact
/// same source state machine as v1; keeping the driver generic makes its ordering and flow-control
/// behavior testable without constructing a filesystem-backed WAL.
pub trait SyncPullSource: Send + 'static {
    fn source_batch_limits(&self) -> RawReplicationPullLimits {
        RawReplicationPullLimits {
            max_records: 1,
            max_compressed_bytes: 1,
            max_uncompressed_bytes: 1,
        }
    }

    fn source_max_encoding_message_bytes(&self) -> usize {
        PULL_CONTROL_MAX_DECODING_BYTES
    }

    fn pull_for_sync(&mut self) -> Result<RawReplicationPullOutcome, PullSourceError>;
    fn commit_for_sync(
        &mut self,
        ack: CumulativePrimaryAck,
    ) -> Result<RawReplicationPullCommit, PullSourceError>;
    fn pending_stream_for_sync(&self) -> Option<&ReplicationStreamId>;
    fn discard_new_pending_for_sync(&mut self, stream: &ReplicationStreamId);
    fn gc_for_sync(&mut self) -> PullSourceGcResult;
}

impl SyncPullSource for RawReplicationPullSource {
    fn source_batch_limits(&self) -> RawReplicationPullLimits {
        self.batch_limits()
    }

    fn source_max_encoding_message_bytes(&self) -> usize {
        self.max_encoding_message_bytes
    }

    fn pull_for_sync(&mut self) -> Result<RawReplicationPullOutcome, PullSourceError> {
        self.pull_batch()
    }

    fn commit_for_sync(
        &mut self,
        ack: CumulativePrimaryAck,
    ) -> Result<RawReplicationPullCommit, PullSourceError> {
        self.commit_ack(ack)
    }

    fn pending_stream_for_sync(&self) -> Option<&ReplicationStreamId> {
        self.pending_stream()
    }

    fn discard_new_pending_for_sync(&mut self, stream: &ReplicationStreamId) {
        self.discard_new_pending_for_unauthorized_stream(stream);
    }

    fn gc_for_sync(&mut self) -> PullSourceGcResult {
        self.gc_one_acknowledged_generation()
    }
}

trait SyncStreamAuthorizer: Send + Sync + 'static {
    fn permits_sync_stream(&self, stream: &ReplicationStreamId) -> bool;
}

impl SyncStreamAuthorizer for AuthenticatedClient {
    fn permits_sync_stream(&self, stream: &ReplicationStreamId) -> bool {
        self.permits(stream)
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
pub struct RawReplicationPullService<S = RawReplicationPullSource> {
    source: Arc<Mutex<S>>,
    allowlist: ClientCertificateAllowlist,
    admission: Arc<Semaphore>,
    sync_batch_limits: RawReplicationPullLimits,
    max_encoding_message_bytes: usize,
    commit_observer: Option<Arc<dyn RawReplicationPullCommitObserver>>,
}

/// Non-authoritative monitoring hook invoked only after a cumulative ACK has crossed the source's
/// durable ACK-WAL boundary. Observer failure must be handled internally and can never revoke,
/// reinterpret, or turn an already durable CommitAck into a retry ambiguity.
pub trait RawReplicationPullCommitObserver: Send + Sync {
    fn after_durable_commit(&self, ack: &CumulativePrimaryAck);
}

impl<S> RawReplicationPullService<S>
where
    S: SyncPullSource,
{
    pub fn new(source: S, allowlist: ClientCertificateAllowlist) -> Self {
        let max_encoding_message_bytes = source.source_max_encoding_message_bytes();
        let sync_batch_limits = source.source_batch_limits();
        Self {
            source: Arc::new(Mutex::new(source)),
            allowlist,
            admission: Arc::new(Semaphore::new(1)),
            sync_batch_limits,
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
        acquire_replication_admission(&self.admission)
    }
}

fn acquire_replication_admission(
    admission: &Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit, Status> {
    admission.clone().try_acquire_owned().map_err(|_| {
        Status::resource_exhausted("a replication operation or Sync session is already in flight")
    })
}

impl<S> fmt::Debug for RawReplicationPullService<S>
where
    S: SyncPullSource,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawReplicationPullService")
            .field("allowlist", &self.allowlist)
            .field(
                "max_encoding_message_bytes",
                &self.max_encoding_message_bytes,
            )
            .field("sync_batch_limits", &self.sync_batch_limits)
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

pub struct RawReplicationSyncResponseStream {
    receiver: mpsc::Receiver<Result<wire::SyncServerMessage, Status>>,
}

impl Stream for RawReplicationSyncResponseStream {
    type Item = Result<wire::SyncServerMessage, Status>;

    fn poll_next(self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().receiver.poll_recv(context)
    }
}

struct PreparedWireSyncBatch {
    header: wire::SyncBatch,
    records: Vec<wire::PushBatchRequest>,
}

fn validate_sync_client_hello(message: wire::SyncClientMessage) -> Result<(), Status> {
    match message.frame {
        Some(wire::sync_client_message::Frame::Hello(hello))
            if hello.protocol_version == RAW_REPLICATION_SYNC_PROTOCOL_VERSION =>
        {
            Ok(())
        }
        Some(wire::sync_client_message::Frame::Hello(_)) => Err(Status::invalid_argument(
            "unsupported raw replication Sync protocol version",
        )),
        Some(wire::sync_client_message::Frame::Ack(_)) => Err(Status::failed_precondition(
            "the first raw replication Sync frame must be Hello",
        )),
        None => Err(Status::invalid_argument(
            "raw replication Sync client frame is empty",
        )),
    }
}

fn sync_server_hello(
    limits: RawReplicationPullLimits,
    tail_poll_interval: Duration,
    heartbeat_interval: Duration,
) -> Result<wire::SyncServerHello, Status> {
    Ok(wire::SyncServerHello {
        protocol_version: RAW_REPLICATION_SYNC_PROTOCOL_VERSION,
        max_batch_records: u32::try_from(limits.max_records)
            .map_err(|_| Status::internal("Sync batch record limit does not fit the wire"))?,
        max_batch_compressed_bytes: limits.max_compressed_bytes,
        max_batch_uncompressed_bytes: limits.max_uncompressed_bytes,
        tail_poll_interval_ms: u64::try_from(tail_poll_interval.as_millis())
            .map_err(|_| Status::internal("Sync tail-poll interval does not fit the wire"))?,
        heartbeat_interval_ms: u64::try_from(heartbeat_interval.as_millis())
            .map_err(|_| Status::internal("Sync heartbeat interval does not fit the wire"))?,
    })
}

fn prepare_wire_sync_batch(
    batch_id: u64,
    batch: RawReplicationPullBatch,
) -> Result<PreparedWireSyncBatch, Status> {
    let stream = batch.stream().clone();
    let records = batch.into_records();
    let first = records
        .first()
        .ok_or_else(|| Status::internal("pull source produced an empty Sync batch"))?;
    let tail = records
        .last()
        .ok_or_else(|| Status::internal("pull source produced an empty Sync batch"))?;
    let first_sequence = first.offer.record.sequence;
    let through_sequence = tail.offer.record.sequence;
    let through_content_digest = tail.offer.content_digest.0.to_vec();
    let record_count = u32::try_from(records.len())
        .map_err(|_| Status::internal("Sync batch record count does not fit the wire"))?;
    let (compressed_bytes, uncompressed_bytes) =
        records
            .iter()
            .try_fold((0u64, 0u64), |(compressed, uncompressed), record| {
                let payload_len = u64::try_from(record.compressed_payload.len())
                    .map_err(|_| Status::internal("Sync record length does not fit the wire"))?;
                let compressed = compressed
                    .checked_add(payload_len)
                    .ok_or_else(|| Status::internal("Sync compressed byte count overflow"))?;
                let uncompressed = uncompressed
                    .checked_add(record.uncompressed_len)
                    .ok_or_else(|| Status::internal("Sync uncompressed byte count overflow"))?;
                Ok::<_, Status>((compressed, uncompressed))
            })?;
    let wire_stream = wire::StreamId::try_from(&stream)
        .map_err(|_| Status::internal("could not encode Sync stream identity"))?;
    let wire_records = records
        .into_iter()
        .map(|record| {
            ValidatedPushRecord::new(stream.clone(), record)
                .and_then(wire::PushBatchRequest::try_from)
                .map_err(|_| Status::internal("could not encode Sync WAL record"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PreparedWireSyncBatch {
        header: wire::SyncBatch {
            batch_id,
            stream: Some(wire_stream),
            first_sequence,
            through_sequence,
            record_count,
            compressed_bytes,
            uncompressed_bytes,
            through_content_digest,
        },
        records: wire_records,
    })
}

async fn send_sync_frame(
    responses: &mpsc::Sender<Result<wire::SyncServerMessage, Status>>,
    frame: wire::sync_server_message::Frame,
) -> Result<(), Status> {
    responses
        .send(Ok(wire::SyncServerMessage { frame: Some(frame) }))
        .await
        .map_err(|_| Status::cancelled("raw replication Sync response stream closed"))
}

async fn send_outstanding_sync_frame(
    responses: &mpsc::Sender<Result<wire::SyncServerMessage, Status>>,
    frame: wire::sync_server_message::Frame,
    ack_deadline: tokio::time::Instant,
) -> Result<(), Status> {
    tokio::time::timeout_at(ack_deadline, send_sync_frame(responses, frame))
        .await
        .map_err(|_| sync_ack_deadline_status())?
}

fn sync_ack_deadline_status() -> Status {
    Status::deadline_exceeded("raw replication Sync ACK deadline expired for the outstanding batch")
}

async fn pull_for_sync<S: SyncPullSource>(
    source: Arc<Mutex<S>>,
) -> Result<(RawReplicationPullOutcome, bool), Status> {
    tokio::task::spawn_blocking(move || {
        let mut source = source
            .lock()
            .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;
        let already_pending = source.pending_stream_for_sync().is_some();
        let outcome = source.pull_for_sync()?;
        Ok::<_, PullSourceError>((outcome, already_pending))
    })
    .await
    .map_err(|_| Status::internal("Sync pull-source task failed"))?
    .map_err(pull_status)
}

async fn caught_up_gc_for_sync<S: SyncPullSource>(
    source: Arc<Mutex<S>>,
) -> Result<PullSourceGcResult, Status> {
    tokio::task::spawn_blocking(move || {
        let mut source = source
            .lock()
            .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?;
        Ok::<_, PullSourceError>(source.gc_for_sync())
    })
    .await
    .map_err(|_| Status::internal("Sync caught-up GC task failed"))?
    .map_err(pull_status)
}

async fn discard_unauthorized_sync_pending<S: SyncPullSource>(
    source: Arc<Mutex<S>>,
    stream: ReplicationStreamId,
) -> Result<(), Status> {
    tokio::task::spawn_blocking(move || {
        source
            .lock()
            .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?
            .discard_new_pending_for_sync(&stream);
        Ok::<_, PullSourceError>(())
    })
    .await
    .map_err(|_| Status::internal("Sync pull-source task failed"))?
    .map_err(pull_status)
}

async fn commit_sync_ack<S: SyncPullSource>(
    source: Arc<Mutex<S>>,
    ack: CumulativePrimaryAck,
    observer: Option<Arc<dyn RawReplicationPullCommitObserver>>,
) -> Result<RawReplicationPullCommit, Status> {
    let status_ack = ack.clone();
    tokio::task::spawn_blocking(move || {
        let result = source
            .lock()
            .map_err(|_| pull_error(PullSourceErrorKind::SourceCursor))?
            .commit_for_sync(ack);
        observe_successful_durable_commit(result, &status_ack, observer.as_deref())
    })
    .await
    .map_err(|_| Status::internal("Sync ACK task failed"))?
    .map_err(pull_status)
}

fn decode_sync_client_frame(
    message: wire::SyncClientMessage,
) -> Result<wire::sync_client_message::Frame, Status> {
    message
        .frame
        .ok_or_else(|| Status::invalid_argument("raw replication Sync client frame is empty"))
}

/// Drive one protocol-v2 session. The session has an implicit one-batch credit: after emitting a
/// Batch header and its exact record_count records, this function reads but never emits another
/// batch until the matching signed ACK has crossed the existing durable commit path.
async fn run_sync_session<S, I, A>(
    source: Arc<Mutex<S>>,
    authorizer: A,
    mut inbound: I,
    responses: mpsc::Sender<Result<wire::SyncServerMessage, Status>>,
    limits: RawReplicationPullLimits,
    tail_poll_interval: Duration,
    heartbeat_interval: Duration,
    ack_timeout: Duration,
    caught_up_gc_interval: Duration,
    commit_observer: Option<Arc<dyn RawReplicationPullCommitObserver>>,
) -> Result<(), Status>
where
    S: SyncPullSource,
    I: Stream<Item = Result<wire::SyncClientMessage, Status>> + Send + Unpin + 'static,
    A: SyncStreamAuthorizer,
{
    let hello = sync_server_hello(limits, tail_poll_interval, heartbeat_interval)?;
    send_sync_frame(&responses, wire::sync_server_message::Frame::Hello(hello)).await?;

    let mut next_batch_id = 1u64;
    let mut next_heartbeat_id = 1u64;
    let mut next_heartbeat_at = tokio::time::Instant::now() + heartbeat_interval;
    let mut next_caught_up_gc_at = tokio::time::Instant::now() + caught_up_gc_interval;

    loop {
        let (outcome, already_pending) = pull_for_sync(Arc::clone(&source)).await?;

        let batch = match outcome {
            RawReplicationPullOutcome::Batch(batch) => batch,
            RawReplicationPullOutcome::CaughtUp => {
                let now = tokio::time::Instant::now();
                if now >= next_caught_up_gc_at {
                    let maintenance = caught_up_gc_for_sync(Arc::clone(&source)).await?;
                    report_pull_source_gc(maintenance, "sync_caught_up_cadence");
                    next_caught_up_gc_at = tokio::time::Instant::now() + caught_up_gc_interval;
                }
                if now >= next_heartbeat_at {
                    send_sync_frame(
                        &responses,
                        wire::sync_server_message::Frame::Heartbeat(wire::SyncHeartbeat {
                            heartbeat_id: next_heartbeat_id,
                        }),
                    )
                    .await?;
                    next_heartbeat_id = next_heartbeat_id.checked_add(1).ok_or_else(|| {
                        Status::resource_exhausted("Sync heartbeat sequence exhausted")
                    })?;
                    next_heartbeat_at = now + heartbeat_interval;
                }

                tokio::select! {
                    incoming = inbound.next() => {
                        match incoming {
                            None => return Ok(()),
                            Some(Err(status)) => return Err(status),
                            Some(Ok(message)) => match decode_sync_client_frame(message)? {
                                wire::sync_client_message::Frame::Hello(_) => {
                                    return Err(Status::failed_precondition(
                                        "raw replication Sync Hello may appear only once",
                                    ));
                                }
                                wire::sync_client_message::Frame::Ack(_) => {
                                    return Err(Status::failed_precondition(
                                        "raw replication Sync has no batch awaiting acknowledgement",
                                    ));
                                }
                            },
                        }
                    }
                    _ = tokio::time::sleep(tail_poll_interval) => {}
                }
                continue;
            }
        };

        if !authorizer.permits_sync_stream(batch.stream()) {
            if !already_pending {
                discard_unauthorized_sync_pending(Arc::clone(&source), batch.stream().clone())
                    .await?;
            }
            return Err(Status::permission_denied(
                "client certificate is not authorized for the selected Sync stream",
            ));
        }

        // One deadline covers bounded delivery plus Blockzilla's durable local sink and signed ACK.
        // A client that stops consuming response frames cannot bypass the same admission fence.
        let ack_deadline = tokio::time::Instant::now() + ack_timeout;
        let prepared = prepare_wire_sync_batch(next_batch_id, batch)?;
        send_outstanding_sync_frame(
            &responses,
            wire::sync_server_message::Frame::Batch(prepared.header),
            ack_deadline,
        )
        .await?;
        for record in prepared.records {
            send_outstanding_sync_frame(
                &responses,
                wire::sync_server_message::Frame::Record(record),
                ack_deadline,
            )
            .await?;
        }

        let submitted = match tokio::time::timeout_at(ack_deadline, inbound.next())
            .await
            .map_err(|_| sync_ack_deadline_status())?
        {
            None => {
                return Err(Status::failed_precondition(
                    "raw replication Sync closed before acknowledging its outstanding batch",
                ));
            }
            Some(Err(status)) => return Err(status),
            Some(Ok(message)) => match decode_sync_client_frame(message)? {
                wire::sync_client_message::Frame::Hello(_) => {
                    return Err(Status::failed_precondition(
                        "raw replication Sync Hello may appear only once",
                    ));
                }
                wire::sync_client_message::Frame::Ack(ack) => ack,
            },
        };
        if submitted.batch_id != next_batch_id {
            return Err(Status::failed_precondition(
                "raw replication Sync ACK does not match the outstanding batch id",
            ));
        }
        let ack: CumulativePrimaryAck = submitted
            .ack
            .ok_or_else(|| {
                Status::invalid_argument("raw replication Sync ACK is missing its signed value")
            })?
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid cumulative acknowledgement"))?;
        if !authorizer.permits_sync_stream(&ack.stream) {
            return Err(Status::permission_denied(
                "client certificate is not authorized for the acknowledged Sync stream",
            ));
        }

        let committed = commit_sync_ack(Arc::clone(&source), ack, commit_observer.clone()).await?;
        report_pull_source_gc(committed.gc.clone(), "sync_ack");
        next_caught_up_gc_at = tokio::time::Instant::now() + caught_up_gc_interval;
        let wire_stream = wire::StreamId::try_from(&committed.stream)
            .map_err(|_| Status::internal("could not encode Sync ACK commit response"))?;
        send_sync_frame(
            &responses,
            wire::sync_server_message::Frame::AckCommitted(wire::SyncAckCommitted {
                batch_id: next_batch_id,
                stream: Some(wire_stream),
                through_sequence: committed.through_sequence,
                replayed: committed.replayed,
            }),
        )
        .await?;
        next_batch_id = next_batch_id
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("Sync batch id exhausted"))?;
    }
}

#[tonic::async_trait]
impl<S> wire::raw_replication_pull_server::RawReplicationPull for RawReplicationPullService<S>
where
    S: SyncPullSource,
{
    type PullBatchStream = RawReplicationPullResponseStream;
    type SyncStream = RawReplicationSyncResponseStream;

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
            let already_pending = source.pending_stream_for_sync().is_some();
            let outcome = source.pull_for_sync()?;
            let maintenance = if matches!(outcome, RawReplicationPullOutcome::CaughtUp) {
                Some(source.gc_for_sync())
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
                                .discard_new_pending_for_sync(&selected_stream);
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
                .commit_for_sync(ack);
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

    async fn sync(
        &self,
        request: Request<tonic::Streaming<wire::SyncClientMessage>>,
    ) -> Result<Response<Self::SyncStream>, Status> {
        // Authentication is derived exclusively from Tonic's verified mTLS peer certificate before
        // any application frame is trusted. The permit is held for the entire bidirectional RPC,
        // fencing this session against both another Sync and the legacy v1 operations.
        let authenticated = self.authenticate(&request)?;
        let permit = self.acquire_admission()?;
        let mut inbound = request.into_inner();
        let first = tokio::time::timeout(SYNC_CLIENT_HELLO_TIMEOUT, inbound.next())
            .await
            .map_err(|_| Status::deadline_exceeded("raw replication Sync Hello timed out"))?;
        let first = match first {
            Some(Ok(message)) => message,
            Some(Err(status)) => return Err(status),
            None => {
                return Err(Status::failed_precondition(
                    "raw replication Sync closed before Hello",
                ));
            }
        };
        validate_sync_client_hello(first)?;

        let (responses, receiver) = mpsc::channel(SYNC_RESPONSE_CHANNEL_CAPACITY);
        let session_responses = responses.clone();
        let source = Arc::clone(&self.source);
        let limits = self.sync_batch_limits;
        let observer = self.commit_observer.clone();
        tokio::spawn(async move {
            let result = run_sync_session(
                source,
                authenticated,
                inbound,
                session_responses,
                limits,
                SYNC_TAIL_POLL_INTERVAL,
                SYNC_HEARTBEAT_INTERVAL,
                SYNC_ACK_TIMEOUT,
                SYNC_CAUGHT_UP_GC_INTERVAL,
                observer,
            )
            .await;
            // Release global admission before reporting a terminal stream status. A client that
            // stopped consuming responses must not keep the sole permit through channel backpressure.
            drop(permit);
            if let Err(status) = result {
                let _ = responses.send(Err(status)).await;
            }
        });

        Ok(Response::new(RawReplicationSyncResponseStream { receiver }))
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

    impl SyncPullSource for PullSourceCore<FakeBackend, AcceptVerifier> {
        fn pull_for_sync(&mut self) -> Result<RawReplicationPullOutcome, PullSourceError> {
            self.pull_batch()
        }

        fn commit_for_sync(
            &mut self,
            ack: CumulativePrimaryAck,
        ) -> Result<RawReplicationPullCommit, PullSourceError> {
            self.commit_ack(ack)
        }

        fn pending_stream_for_sync(&self) -> Option<&ReplicationStreamId> {
            self.pending_stream()
        }

        fn discard_new_pending_for_sync(&mut self, stream: &ReplicationStreamId) {
            if self.pending_stream() == Some(stream) {
                self.pending = None;
            }
        }

        fn gc_for_sync(&mut self) -> PullSourceGcResult {
            self.gc_one_if_enabled()
        }
    }

    struct AllowAllSyncStreams;

    impl SyncStreamAuthorizer for AllowAllSyncStreams {
        fn permits_sync_stream(&self, _stream: &ReplicationStreamId) -> bool {
            true
        }
    }

    struct TestSyncInbound {
        receiver: mpsc::Receiver<Result<wire::SyncClientMessage, Status>>,
    }

    impl Stream for TestSyncInbound {
        type Item = Result<wire::SyncClientMessage, Status>;

        fn poll_next(
            self: Pin<&mut Self>,
            context: &mut TaskContext<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.get_mut().receiver.poll_recv(context)
        }
    }

    fn stream() -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: "solana-mainnet".into(),
            origin_node_id: "source-node-1".into(),
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

    fn test_limits() -> RawReplicationPullLimits {
        RawReplicationPullLimits {
            max_records: 10,
            max_compressed_bytes: 1024,
            max_uncompressed_bytes: 4096,
        }
    }

    fn core_with_batches(
        batches: Vec<Vec<RawReplicationRecord>>,
        gc_enabled: bool,
    ) -> PullSourceCore<FakeBackend, AcceptVerifier> {
        let mut backend = FakeBackend::default();
        for records in batches {
            backend.queued.push_back((stream(), records));
        }
        PullSourceCore {
            backend,
            verifier: AcceptVerifier,
            expected_primary_id: "blockzilla-primary".into(),
            limits: test_limits(),
            pending: None,
            gc_enabled,
        }
    }

    fn core(
        records: Vec<RawReplicationRecord>,
        gc_enabled: bool,
    ) -> PullSourceCore<FakeBackend, AcceptVerifier> {
        core_with_batches(vec![records], gc_enabled)
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

    fn sync_ack_message(batch_id: u64, ack: &CumulativePrimaryAck) -> wire::SyncClientMessage {
        wire::SyncClientMessage {
            frame: Some(wire::sync_client_message::Frame::Ack(wire::SyncClientAck {
                batch_id,
                ack: Some(
                    wire::CumulativePrimaryAck::try_from(ack).expect("encode cumulative ACK"),
                ),
            })),
        }
    }

    async fn next_sync_server_frame(
        receiver: &mut mpsc::Receiver<Result<wire::SyncServerMessage, Status>>,
    ) -> wire::sync_server_message::Frame {
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("Sync server frame timeout")
            .expect("Sync response stream remains open")
            .expect("successful Sync server frame")
            .frame
            .expect("non-empty Sync server frame")
    }

    #[test]
    fn sync_requires_exact_v2_hello_as_the_first_client_frame() {
        validate_sync_client_hello(wire::SyncClientMessage {
            frame: Some(wire::sync_client_message::Frame::Hello(
                wire::SyncClientHello {
                    protocol_version: RAW_REPLICATION_SYNC_PROTOCOL_VERSION,
                },
            )),
        })
        .expect("v2 Hello");

        let unsupported = validate_sync_client_hello(wire::SyncClientMessage {
            frame: Some(wire::sync_client_message::Frame::Hello(
                wire::SyncClientHello {
                    protocol_version: 1,
                },
            )),
        })
        .expect_err("v1 must not enter the v2 RPC");
        assert_eq!(unsupported.code(), tonic::Code::InvalidArgument);

        let ack_first = validate_sync_client_hello(wire::SyncClientMessage {
            frame: Some(wire::sync_client_message::Frame::Ack(wire::SyncClientAck {
                batch_id: 1,
                ack: None,
            })),
        })
        .expect_err("ACK before Hello");
        assert_eq!(ack_first.code(), tonic::Code::FailedPrecondition);

        let server_hello = sync_server_hello(
            test_limits(),
            SYNC_TAIL_POLL_INTERVAL,
            SYNC_HEARTBEAT_INTERVAL,
        )
        .expect("production Sync server Hello");
        assert_eq!(server_hello.tail_poll_interval_ms, 250);
        assert_eq!(server_hello.heartbeat_interval_ms, 15_000);
    }

    #[test]
    fn one_admission_permit_fences_v1_and_v2_operations_for_the_session_lifetime() {
        let admission = Arc::new(Semaphore::new(1));
        let first = acquire_replication_admission(&admission).expect("first session");
        let second = acquire_replication_admission(&admission).expect_err("second session fenced");
        assert_eq!(second.code(), tonic::Code::ResourceExhausted);
        drop(first);
        let _released =
            acquire_replication_admission(&admission).expect("permit released after session");
    }

    #[tokio::test]
    async fn sync_is_stop_and_wait_and_commits_only_after_the_matching_client_ack() {
        let source = Arc::new(Mutex::new(core_with_batches(
            vec![vec![record(0)], vec![record(1)]],
            false,
        )));
        let (client, client_receiver) = mpsc::channel::<Result<wire::SyncClientMessage, Status>>(8);
        let (responses, mut response_receiver) =
            mpsc::channel::<Result<wire::SyncServerMessage, Status>>(8);
        let task = tokio::spawn(run_sync_session(
            Arc::clone(&source),
            AllowAllSyncStreams,
            TestSyncInbound {
                receiver: client_receiver,
            },
            responses,
            test_limits(),
            Duration::from_millis(5),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            None,
        ));

        let wire::sync_server_message::Frame::Hello(hello) =
            next_sync_server_frame(&mut response_receiver).await
        else {
            panic!("expected Sync server Hello");
        };
        assert_eq!(
            hello.protocol_version,
            RAW_REPLICATION_SYNC_PROTOCOL_VERSION
        );
        assert_eq!(hello.max_batch_records, 10);

        let wire::sync_server_message::Frame::Batch(first_batch) =
            next_sync_server_frame(&mut response_receiver).await
        else {
            panic!("expected first Sync batch header");
        };
        assert_eq!(first_batch.batch_id, 1);
        assert_eq!(first_batch.first_sequence, 0);
        assert_eq!(first_batch.through_sequence, 0);
        assert_eq!(first_batch.record_count, 1);
        assert_eq!(first_batch.compressed_bytes, 3);
        assert_eq!(first_batch.uncompressed_bytes, 10);
        assert_eq!(first_batch.through_content_digest.len(), 32);
        assert!(matches!(
            next_sync_server_frame(&mut response_receiver).await,
            wire::sync_server_message::Frame::Record(_)
        ));

        // The next source batch already exists, but the implicit one-batch credit is exhausted
        // until Blockzilla submits the exact signed durable ACK.
        assert!(
            tokio::time::timeout(Duration::from_millis(25), response_receiver.recv())
                .await
                .is_err()
        );
        {
            let source = source.lock().expect("test source");
            assert_eq!(source.backend.commit_calls, 0);
            assert!(source.pending.is_some());
        }

        let first_ack = {
            let source = source.lock().expect("test source");
            ack_for(&source)
        };
        client
            .send(Ok(sync_ack_message(1, &first_ack)))
            .await
            .expect("submit first ACK");
        let wire::sync_server_message::Frame::AckCommitted(committed) =
            next_sync_server_frame(&mut response_receiver).await
        else {
            panic!("expected durable ACK confirmation");
        };
        assert_eq!(committed.batch_id, 1);
        assert_eq!(committed.through_sequence, 0);
        assert!(!committed.replayed);

        let wire::sync_server_message::Frame::Batch(second_batch) =
            next_sync_server_frame(&mut response_receiver).await
        else {
            panic!("expected second Sync batch header");
        };
        assert_eq!(second_batch.batch_id, 2);
        assert_eq!(second_batch.first_sequence, 1);
        assert!(matches!(
            next_sync_server_frame(&mut response_receiver).await,
            wire::sync_server_message::Frame::Record(_)
        ));
        assert_eq!(source.lock().expect("test source").backend.commit_calls, 1);

        drop(client);
        let closed = task
            .await
            .expect("Sync task joins")
            .expect_err("outstanding second batch requires an ACK");
        assert_eq!(closed.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn sync_ack_timeout_keeps_the_batch_pending_and_releases_session_admission() {
        let source = Arc::new(Mutex::new(core(vec![record(0)], false)));
        let admission = Arc::new(Semaphore::new(1));
        let permit = acquire_replication_admission(&admission).expect("admit timed session");
        let (_client, client_receiver) =
            mpsc::channel::<Result<wire::SyncClientMessage, Status>>(4);
        let (responses, mut response_receiver) =
            mpsc::channel::<Result<wire::SyncServerMessage, Status>>(4);
        let task = tokio::spawn({
            let source = Arc::clone(&source);
            async move {
                let _permit = permit;
                run_sync_session(
                    source,
                    AllowAllSyncStreams,
                    TestSyncInbound {
                        receiver: client_receiver,
                    },
                    responses,
                    test_limits(),
                    Duration::from_millis(5),
                    Duration::from_secs(1),
                    Duration::from_millis(30),
                    Duration::from_secs(1),
                    None,
                )
                .await
            }
        });

        assert!(matches!(
            next_sync_server_frame(&mut response_receiver).await,
            wire::sync_server_message::Frame::Hello(_)
        ));
        assert!(matches!(
            next_sync_server_frame(&mut response_receiver).await,
            wire::sync_server_message::Frame::Batch(_)
        ));
        assert!(matches!(
            next_sync_server_frame(&mut response_receiver).await,
            wire::sync_server_message::Frame::Record(_)
        ));

        let timeout = task
            .await
            .expect("timed Sync task joins")
            .expect_err("missing ACK must time out");
        assert_eq!(timeout.code(), tonic::Code::DeadlineExceeded);
        {
            let source = source.lock().expect("test source");
            assert_eq!(source.backend.commit_calls, 0);
            assert!(source.pending.is_some());
        }
        let _next = acquire_replication_admission(&admission)
            .expect("timed-out session releases its sole admission permit");
    }

    #[tokio::test]
    async fn sync_tail_polls_and_emits_heartbeat_without_creating_ack_progress() {
        let source = Arc::new(Mutex::new(core_with_batches(Vec::new(), true)));
        let (client, client_receiver) = mpsc::channel::<Result<wire::SyncClientMessage, Status>>(4);
        let (responses, mut response_receiver) =
            mpsc::channel::<Result<wire::SyncServerMessage, Status>>(4);
        let task = tokio::spawn(run_sync_session(
            Arc::clone(&source),
            AllowAllSyncStreams,
            TestSyncInbound {
                receiver: client_receiver,
            },
            responses,
            test_limits(),
            Duration::from_millis(5),
            Duration::from_millis(15),
            Duration::from_secs(1),
            Duration::from_secs(1),
            None,
        ));

        assert!(matches!(
            next_sync_server_frame(&mut response_receiver).await,
            wire::sync_server_message::Frame::Hello(_)
        ));
        let wire::sync_server_message::Frame::Heartbeat(heartbeat) =
            next_sync_server_frame(&mut response_receiver).await
        else {
            panic!("expected caught-up heartbeat");
        };
        assert_eq!(heartbeat.heartbeat_id, 1);
        {
            let source = source.lock().expect("test source");
            assert!(source.backend.reserve_calls >= 2);
            assert_eq!(source.backend.commit_calls, 0);
            // The 5 ms tail poll must not run retention. Its independent one-second test cadence
            // has not elapsed yet.
            assert_eq!(source.backend.gc_calls, 0);
            assert!(source.backend.latest.is_empty());
        }

        drop(client);
        task.await
            .expect("Sync task joins")
            .expect("caught-up client may close cleanly");
    }

    #[tokio::test]
    async fn sync_rejects_a_mismatched_batch_id_without_committing_or_advancing() {
        let source = Arc::new(Mutex::new(core(vec![record(0)], false)));
        let (client, client_receiver) = mpsc::channel::<Result<wire::SyncClientMessage, Status>>(4);
        let (responses, mut response_receiver) =
            mpsc::channel::<Result<wire::SyncServerMessage, Status>>(4);
        let task = tokio::spawn(run_sync_session(
            Arc::clone(&source),
            AllowAllSyncStreams,
            TestSyncInbound {
                receiver: client_receiver,
            },
            responses,
            test_limits(),
            Duration::from_millis(5),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            None,
        ));

        assert!(matches!(
            next_sync_server_frame(&mut response_receiver).await,
            wire::sync_server_message::Frame::Hello(_)
        ));
        assert!(matches!(
            next_sync_server_frame(&mut response_receiver).await,
            wire::sync_server_message::Frame::Batch(_)
        ));
        assert!(matches!(
            next_sync_server_frame(&mut response_receiver).await,
            wire::sync_server_message::Frame::Record(_)
        ));
        let ack = {
            let source = source.lock().expect("test source");
            ack_for(&source)
        };
        client
            .send(Ok(sync_ack_message(2, &ack)))
            .await
            .expect("submit mismatched ACK");
        let error = task
            .await
            .expect("Sync task joins")
            .expect_err("mismatched batch id must fail");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let source = source.lock().expect("test source");
        assert_eq!(source.backend.commit_calls, 0);
        assert!(source.pending.is_some());
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

#[cfg(test)]
#[path = "replication_pull_source_sync_network_tests.rs"]
mod sync_network_tests;
