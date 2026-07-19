//! Transactional, single-generation raw replication sender.
//!
//! The sender advances its local committed cursor only after a signed cumulative ACK has crossed
//! the local ACK-WAL fsync boundary. It deliberately exposes no deletion or garbage-collection
//! operation.

use std::{
    error::Error,
    fmt,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
};

use crate::grpc_raw::{
    GrpcRawCommittedAdvance, GrpcRawCommittedCursor, GrpcRawCommittedReadLimits,
};

use super::{
    ContentDigest, CumulativeAckWal, CumulativePrimaryAck, Ed25519ReceiptKeyring,
    ExpectedCumulativeAck, GetAckState, RawReplicationClient, RawReplicationRecord,
    ReplicaUpstreamConfig, ReplicationClientError, ReplicationClientErrorKind, ReplicationStreamId,
    VerifiedCumulativeAck, cumulative_chain_next, cumulative_chain_seed, verify_cumulative_ack,
};

type TransportFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, TransportFailure>> + Send + 'a>>;
type ReservedBatch<R> = Option<(Vec<RawReplicationRecord>, R)>;

/// Stable, non-secret sender failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationSenderErrorKind {
    InvalidConfiguration,
    TrustedReceiptKeys,
    AckWal,
    Cursor,
    /// Retryable connection/request failure.
    Transport,
    /// Authentication, TLS, configuration, protocol, or response failure that retries cannot fix.
    TransportPermanent,
    Reconciliation,
    InvalidAck,
    Chain,
}

/// A redacted sender error which never embeds credential paths, endpoints, signatures, or remote
/// status messages.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ReplicationSenderError {
    kind: ReplicationSenderErrorKind,
}

impl ReplicationSenderError {
    fn new(kind: ReplicationSenderErrorKind) -> Self {
        Self { kind }
    }

    pub fn kind(&self) -> ReplicationSenderErrorKind {
        self.kind
    }
}

impl fmt::Display for ReplicationSenderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            ReplicationSenderErrorKind::InvalidConfiguration => {
                "invalid replication sender configuration"
            }
            ReplicationSenderErrorKind::TrustedReceiptKeys => {
                "trusted receipt keys could not be loaded"
            }
            ReplicationSenderErrorKind::AckWal => "cumulative ACK WAL operation failed",
            ReplicationSenderErrorKind::Cursor => "raw replication cursor operation failed",
            ReplicationSenderErrorKind::Transport => "raw replication transport failed",
            ReplicationSenderErrorKind::TransportPermanent => {
                "raw replication transport requires operator action"
            }
            ReplicationSenderErrorKind::Reconciliation => {
                "local and remote durable prefixes cannot be reconciled"
            }
            ReplicationSenderErrorKind::InvalidAck => "remote cumulative ACK failed verification",
            ReplicationSenderErrorKind::Chain => "local replication chain calculation failed",
        };
        formatter.write_str(message)
    }
}

impl fmt::Debug for ReplicationSenderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplicationSenderError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl Error for ReplicationSenderError {}

/// Result of one bounded send attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationSendOutcome {
    CaughtUp,
    Advanced {
        records: usize,
        compressed_bytes: u64,
        through_sequence: u64,
        primary_term: u64,
    },
}

/// Production sender for one immutable raw-gRPC generation.
pub struct ReplicationSender {
    inner: SenderCore<RawReplicationClient, ProductionBackend>,
}

impl ReplicationSender {
    /// Load trust and durable ACK state, open the generation cursor, connect with mTLS, and
    /// reconcile the remote durable prefix before returning a usable sender.
    pub async fn connect(
        generation_dir: impl AsRef<Path>,
        read_limits: GrpcRawCommittedReadLimits,
        config: &ReplicaUpstreamConfig,
    ) -> Result<Self, ReplicationSenderError> {
        Self::connect_inner(generation_dir.as_ref(), read_limits, config, None).await
    }

    /// Connect only if the directory still owns `expected_stream`. This is the production entry
    /// point for a discovered active generation: the exact identity is fenced before any network
    /// connection or remote ACK reconciliation can occur.
    pub async fn connect_fenced(
        generation_dir: impl AsRef<Path>,
        expected_stream: &ReplicationStreamId,
        read_limits: GrpcRawCommittedReadLimits,
        config: &ReplicaUpstreamConfig,
    ) -> Result<Self, ReplicationSenderError> {
        Self::connect_inner(
            generation_dir.as_ref(),
            read_limits,
            config,
            Some(expected_stream),
        )
        .await
    }

    async fn connect_inner(
        generation_dir: &Path,
        read_limits: GrpcRawCommittedReadLimits,
        config: &ReplicaUpstreamConfig,
        expected_stream: Option<&ReplicationStreamId>,
    ) -> Result<Self, ReplicationSenderError> {
        let generation_dir = generation_dir.to_path_buf();
        let initial_cursor = match expected_stream {
            Some(expected_stream) => GrpcRawCommittedCursor::open_fenced(
                &generation_dir,
                read_limits,
                0,
                expected_stream,
            ),
            None => GrpcRawCommittedCursor::open(&generation_dir, read_limits, 0),
        }
        .map_err(|_| sender_error(ReplicationSenderErrorKind::Cursor))?;
        Self::connect_preopened(initial_cursor, config).await
    }

    /// Connect from an already-opened identity-fenced cursor. Cloning and passing the discovery
    /// cursor is the strongest hot-rotation handoff: startup ACK seeking follows that exact stream
    /// under one shared rotation lock and therefore cannot transiently bind its successor.
    pub async fn connect_preopened(
        mut initial_cursor: GrpcRawCommittedCursor,
        config: &ReplicaUpstreamConfig,
    ) -> Result<Self, ReplicationSenderError> {
        let read_limits = initial_cursor.read_limits();
        validate_sender_config(config, read_limits)?;
        let keyring = load_trusted_keys(config)?;
        let ack_wal = CumulativeAckWal::open(&config.cumulative_ack_wal_file)
            .map_err(|_| sender_error(ReplicationSenderErrorKind::AckWal))?;

        let stream = initial_cursor.stream();
        let local_ack = ack_wal.latest_stream_ack(&stream).cloned();
        let next_sequence = next_sequence_after(local_ack.as_ref())?;
        let chain = match &local_ack {
            Some(ack) => ack.rolling_chain_digest,
            None => cumulative_chain_seed(&stream)
                .map_err(|_| sender_error(ReplicationSenderErrorKind::Chain))?,
        };
        let observed_primary_term = ack_wal
            .highest_primary_term(&stream.cluster_id)
            .unwrap_or(0);
        if next_sequence != initial_cursor.next_replication_sequence() {
            initial_cursor
                .seek_to_replication_sequence(next_sequence)
                .map_err(|_| sender_error(ReplicationSenderErrorKind::Cursor))?;
        }
        let backend = ProductionBackend {
            cursor: initial_cursor,
            ack_wal,
        };

        let transport = RawReplicationClient::connect(config)
            .await
            .map_err(client_transport_error)?;
        let mut inner = SenderCore {
            transport,
            backend,
            keyring,
            stream,
            expected_primary_id: config.expected_primary_id.clone(),
            max_batch_events: config.batch.max_events,
            max_batch_compressed_bytes: config.batch.max_bytes,
            max_batch_uncompressed_bytes: config.batch.max_uncompressed_bytes,
            chain,
            observed_primary_term,
        };
        inner.reconcile_startup().await?;
        Ok(Self { inner })
    }

    /// Reserve and submit at most one configured batch.
    pub async fn send_once(&mut self) -> Result<ReplicationSendOutcome, ReplicationSenderError> {
        self.inner.send_once().await
    }

    pub fn stream(&self) -> &ReplicationStreamId {
        &self.inner.stream
    }

    pub fn next_sequence(&self) -> u64 {
        self.inner.backend.next_sequence()
    }

    pub fn rolling_chain_digest(&self) -> ContentDigest {
        self.inner.chain
    }

    /// Resolve the generation fenced by this sender. An active generation may move into the
    /// sealed queue while this sender retains the same stream identity.
    pub fn bound_generation_dir(&self) -> Result<PathBuf, ReplicationSenderError> {
        self.inner
            .backend
            .cursor
            .bound_generation_dir()
            .map_err(|_| sender_error(ReplicationSenderErrorKind::Cursor))
    }

    pub fn bound_generation_is_sealed(&self) -> Result<bool, ReplicationSenderError> {
        self.inner
            .backend
            .cursor
            .bound_generation_is_sealed()
            .map_err(|_| sender_error(ReplicationSenderErrorKind::Cursor))
    }
}

impl fmt::Debug for ReplicationSender {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplicationSender")
            .field("stream", &self.inner.stream)
            .field("next_sequence", &self.inner.backend.next_sequence())
            .field("max_batch_events", &self.inner.max_batch_events)
            .field(
                "max_batch_compressed_bytes",
                &self.inner.max_batch_compressed_bytes,
            )
            .field(
                "max_batch_uncompressed_bytes",
                &self.inner.max_batch_uncompressed_bytes,
            )
            .field("transport", &"mTLS")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportFailure {
    Retryable,
    Permanent,
}

trait ReplicationTransport: Send + Sync {
    fn get_ack<'a>(&'a self, stream: &'a ReplicationStreamId) -> TransportFuture<'a, GetAckState>;

    fn push_batch<'a>(
        &'a self,
        stream: &'a ReplicationStreamId,
        records: Vec<RawReplicationRecord>,
    ) -> TransportFuture<'a, CumulativePrimaryAck>;
}

impl ReplicationTransport for RawReplicationClient {
    fn get_ack<'a>(&'a self, stream: &'a ReplicationStreamId) -> TransportFuture<'a, GetAckState> {
        Box::pin(async move {
            RawReplicationClient::get_ack(self, stream)
                .await
                .map_err(classify_client_transport_failure)
        })
    }

    fn push_batch<'a>(
        &'a self,
        stream: &'a ReplicationStreamId,
        records: Vec<RawReplicationRecord>,
    ) -> TransportFuture<'a, CumulativePrimaryAck> {
        Box::pin(async move {
            RawReplicationClient::push_batch(self, stream, records)
                .await
                .map_err(classify_client_transport_failure)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackendFailure {
    AckWal,
    Cursor,
}

trait SenderBackend {
    type Reservation;

    fn next_sequence(&self) -> u64;
    fn latest_ack(&self, stream: &ReplicationStreamId) -> Option<CumulativePrimaryAck>;
    fn minimum_primary_term(&self, cluster_id: &str) -> u64;
    fn reserve(
        &self,
        max_records: usize,
        max_compressed_bytes: u64,
        max_uncompressed_bytes: u64,
    ) -> Result<ReservedBatch<Self::Reservation>, BackendFailure>;
    fn commit_and_advance(
        &mut self,
        verified: VerifiedCumulativeAck,
        reservation: Self::Reservation,
    ) -> Result<(), BackendFailure>;
    fn commit_existing_prefix(
        &mut self,
        verified: VerifiedCumulativeAck,
    ) -> Result<(), BackendFailure>;
}

struct ProductionBackend {
    cursor: GrpcRawCommittedCursor,
    ack_wal: CumulativeAckWal,
}

impl SenderBackend for ProductionBackend {
    type Reservation = GrpcRawCommittedAdvance;

    fn next_sequence(&self) -> u64 {
        self.cursor.next_replication_sequence()
    }

    fn latest_ack(&self, stream: &ReplicationStreamId) -> Option<CumulativePrimaryAck> {
        self.ack_wal.latest_stream_ack(stream).cloned()
    }

    fn minimum_primary_term(&self, cluster_id: &str) -> u64 {
        self.ack_wal.highest_primary_term(cluster_id).unwrap_or(0)
    }

    fn reserve(
        &self,
        max_records: usize,
        max_compressed_bytes: u64,
        max_uncompressed_bytes: u64,
    ) -> Result<ReservedBatch<Self::Reservation>, BackendFailure> {
        let batch = self
            .cursor
            .read_batch(max_records, max_compressed_bytes, max_uncompressed_bytes)
            .map_err(|_| BackendFailure::Cursor)?;
        if batch.records.is_empty() {
            return Ok(None);
        }
        batch
            .into_transport_parts()
            .map(Some)
            .map_err(|_| BackendFailure::Cursor)
    }

    fn commit_and_advance(
        &mut self,
        verified: VerifiedCumulativeAck,
        reservation: Self::Reservation,
    ) -> Result<(), BackendFailure> {
        let durable = self
            .ack_wal
            .commit_verified_replication(verified, reservation.durable_replication_witness())
            .map_err(|_| BackendFailure::AckWal)?;
        self.cursor
            .advance_after_durable_ack(&reservation, &durable)
            .map_err(|_| BackendFailure::Cursor)
    }

    fn commit_existing_prefix(
        &mut self,
        verified: VerifiedCumulativeAck,
    ) -> Result<(), BackendFailure> {
        self.ack_wal
            .commit_verified_existing_prefix(verified)
            .map(|_| ())
            .map_err(|_| BackendFailure::AckWal)
    }
}

struct SenderCore<T, B> {
    transport: T,
    backend: B,
    keyring: Ed25519ReceiptKeyring,
    stream: ReplicationStreamId,
    expected_primary_id: String,
    max_batch_events: usize,
    max_batch_compressed_bytes: u64,
    max_batch_uncompressed_bytes: u64,
    chain: ContentDigest,
    observed_primary_term: u64,
}

impl<T, B> SenderCore<T, B>
where
    T: ReplicationTransport,
    B: SenderBackend,
{
    async fn reconcile_startup(&mut self) -> Result<(), ReplicationSenderError> {
        let local = self.backend.latest_ack(&self.stream);
        let expected_next = next_sequence_after(local.as_ref())?;
        if self.backend.next_sequence() != expected_next {
            return Err(sender_error(ReplicationSenderErrorKind::Reconciliation));
        }
        self.observed_primary_term = self
            .observed_primary_term
            .max(self.backend.minimum_primary_term(&self.stream.cluster_id));

        let remote = self
            .transport
            .get_ack(&self.stream)
            .await
            .map_err(transport_failure_error)?;
        match (local, remote) {
            (None, GetAckState::NoDurablePrefix) => Ok(()),
            (Some(_), GetAckState::NoDurablePrefix) => {
                Err(sender_error(ReplicationSenderErrorKind::Reconciliation))
            }
            (local, GetAckState::DurablePrefix(remote)) => {
                self.reconcile_remote_ack(local, remote).await
            }
        }
    }

    async fn reconcile_remote_ack(
        &mut self,
        local: Option<CumulativePrimaryAck>,
        remote: CumulativePrimaryAck,
    ) -> Result<(), ReplicationSenderError> {
        if let Some(local) = &local {
            if remote.through_sequence < local.through_sequence {
                return Err(sender_error(ReplicationSenderErrorKind::Reconciliation));
            }
            if remote.through_sequence == local.through_sequence {
                let verified = verify_cumulative_ack(
                    remote,
                    ExpectedCumulativeAck {
                        stream: &self.stream,
                        primary_id: &self.expected_primary_id,
                        minimum_primary_term: self.minimum_primary_term(),
                        through_sequence: local.through_sequence,
                        through_content_digest: local.through_content_digest,
                        rolling_chain_digest: local.rolling_chain_digest,
                    },
                    &self.keyring,
                )
                .map_err(|_| sender_error(ReplicationSenderErrorKind::InvalidAck))?;
                let primary_term = verified.ack().primary_term;
                if verified.ack() != local {
                    self.backend
                        .commit_existing_prefix(verified)
                        .map_err(backend_error)?;
                }
                self.observed_primary_term = self.observed_primary_term.max(primary_term);
                return Ok(());
            }
        }

        let needed = remote
            .through_sequence
            .checked_sub(self.backend.next_sequence())
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| sender_error(ReplicationSenderErrorKind::Reconciliation))?;
        let needed = usize::try_from(needed)
            .map_err(|_| sender_error(ReplicationSenderErrorKind::Reconciliation))?;
        if needed == 0 || needed > self.max_batch_events {
            return Err(sender_error(ReplicationSenderErrorKind::Reconciliation));
        }

        let Some((records, reservation)) = self
            .backend
            .reserve(
                needed,
                self.max_batch_compressed_bytes,
                self.max_batch_uncompressed_bytes,
            )
            .map_err(backend_error)?
        else {
            return Err(sender_error(ReplicationSenderErrorKind::Reconciliation));
        };
        let prepared = prepare_records(&self.stream, self.chain, &records)?;
        if records.len() != needed || prepared.through_sequence != remote.through_sequence {
            return Err(sender_error(ReplicationSenderErrorKind::Reconciliation));
        }
        let verified = self.verify_ack(remote, prepared)?;
        let primary_term = verified.ack().primary_term;
        self.backend
            .commit_and_advance(verified, reservation)
            .map_err(backend_error)?;
        self.chain = prepared.chain;
        self.observed_primary_term = self.observed_primary_term.max(primary_term);
        Ok(())
    }

    async fn send_once(&mut self) -> Result<ReplicationSendOutcome, ReplicationSenderError> {
        let Some((records, reservation)) = self
            .backend
            .reserve(
                self.max_batch_events,
                self.max_batch_compressed_bytes,
                self.max_batch_uncompressed_bytes,
            )
            .map_err(backend_error)?
        else {
            return Ok(ReplicationSendOutcome::CaughtUp);
        };
        let record_count = records.len();
        let compressed_bytes = records.iter().try_fold(0u64, |total, record| {
            let bytes = u64::try_from(record.compressed_payload.len())
                .map_err(|_| sender_error(ReplicationSenderErrorKind::Chain))?;
            total
                .checked_add(bytes)
                .ok_or_else(|| sender_error(ReplicationSenderErrorKind::Chain))
        })?;
        let prepared = prepare_records(&self.stream, self.chain, &records)?;
        let remote = self
            .transport
            .push_batch(&self.stream, records)
            .await
            .map_err(transport_failure_error)?;
        let verified = self.verify_ack(remote, prepared)?;
        let primary_term = verified.ack().primary_term;
        self.backend
            .commit_and_advance(verified, reservation)
            .map_err(backend_error)?;
        self.chain = prepared.chain;
        self.observed_primary_term = self.observed_primary_term.max(primary_term);
        Ok(ReplicationSendOutcome::Advanced {
            records: record_count,
            compressed_bytes,
            through_sequence: prepared.through_sequence,
            primary_term,
        })
    }

    fn verify_ack(
        &self,
        remote: CumulativePrimaryAck,
        prepared: PreparedPrefix,
    ) -> Result<VerifiedCumulativeAck, ReplicationSenderError> {
        verify_cumulative_ack(
            remote,
            ExpectedCumulativeAck {
                stream: &self.stream,
                primary_id: &self.expected_primary_id,
                minimum_primary_term: self.minimum_primary_term(),
                through_sequence: prepared.through_sequence,
                through_content_digest: prepared.through_content_digest,
                rolling_chain_digest: prepared.chain,
            },
            &self.keyring,
        )
        .map_err(|_| sender_error(ReplicationSenderErrorKind::InvalidAck))
    }

    fn minimum_primary_term(&self) -> u64 {
        self.observed_primary_term
            .max(self.backend.minimum_primary_term(&self.stream.cluster_id))
    }
}

#[derive(Debug, Clone, Copy)]
struct PreparedPrefix {
    through_sequence: u64,
    through_content_digest: ContentDigest,
    chain: ContentDigest,
}

fn prepare_records(
    stream: &ReplicationStreamId,
    initial_chain: ContentDigest,
    records: &[RawReplicationRecord],
) -> Result<PreparedPrefix, ReplicationSenderError> {
    let mut chain = initial_chain;
    for record in records {
        chain = cumulative_chain_next(stream, chain, &record.offer)
            .map_err(|_| sender_error(ReplicationSenderErrorKind::Chain))?;
    }
    let tail = records
        .last()
        .ok_or_else(|| sender_error(ReplicationSenderErrorKind::Cursor))?;
    Ok(PreparedPrefix {
        through_sequence: tail.offer.record.sequence,
        through_content_digest: tail.offer.content_digest,
        chain,
    })
}

fn validate_sender_config(
    config: &ReplicaUpstreamConfig,
    read_limits: GrpcRawCommittedReadLimits,
) -> Result<(), ReplicationSenderError> {
    if !valid_stable_id(&config.expected_primary_id)
        || config.batch.max_events == 0
        || config.batch.max_bytes == 0
        || config.batch.max_uncompressed_bytes == 0
        || config.batch.max_uncompressed_event_bytes > config.batch.max_uncompressed_bytes
        || read_limits.max_compressed_record_bytes == 0
        || read_limits.max_uncompressed_record_bytes == 0
        || read_limits.max_compressed_record_bytes > config.batch.max_compressed_event_bytes
        || read_limits.max_uncompressed_record_bytes > config.batch.max_uncompressed_event_bytes
        || !config.cumulative_ack_wal_file.is_absolute()
        || config.trusted_receipt_keys.is_empty()
    {
        return Err(sender_error(
            ReplicationSenderErrorKind::InvalidConfiguration,
        ));
    }
    Ok(())
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

fn load_trusted_keys(
    config: &ReplicaUpstreamConfig,
) -> Result<Ed25519ReceiptKeyring, ReplicationSenderError> {
    let mut keyring = Ed25519ReceiptKeyring::new();
    for trusted in &config.trusted_receipt_keys {
        keyring
            .insert_spki_pem(&trusted.key_id, &trusted.public_key_file)
            .map_err(|_| sender_error(ReplicationSenderErrorKind::TrustedReceiptKeys))?;
    }
    Ok(keyring)
}

fn next_sequence_after(ack: Option<&CumulativePrimaryAck>) -> Result<u64, ReplicationSenderError> {
    match ack {
        Some(ack) => ack
            .through_sequence
            .checked_add(1)
            .ok_or_else(|| sender_error(ReplicationSenderErrorKind::Reconciliation)),
        None => Ok(0),
    }
}

fn backend_error(error: BackendFailure) -> ReplicationSenderError {
    match error {
        BackendFailure::AckWal => sender_error(ReplicationSenderErrorKind::AckWal),
        BackendFailure::Cursor => sender_error(ReplicationSenderErrorKind::Cursor),
    }
}

fn sender_error(kind: ReplicationSenderErrorKind) -> ReplicationSenderError {
    ReplicationSenderError::new(kind)
}

fn client_transport_error(error: ReplicationClientError) -> ReplicationSenderError {
    transport_failure_error(classify_client_transport_failure(error))
}

fn transport_failure_error(failure: TransportFailure) -> ReplicationSenderError {
    match failure {
        TransportFailure::Retryable => sender_error(ReplicationSenderErrorKind::Transport),
        TransportFailure::Permanent => sender_error(ReplicationSenderErrorKind::TransportPermanent),
    }
}

fn classify_client_transport_failure(error: ReplicationClientError) -> TransportFailure {
    classify_client_transport_kind(error.kind())
}

fn classify_client_transport_kind(kind: ReplicationClientErrorKind) -> TransportFailure {
    use tonic::Code;

    match kind {
        ReplicationClientErrorKind::ConnectTimeout
        | ReplicationClientErrorKind::Connect
        | ReplicationClientErrorKind::RequestTimeout
        | ReplicationClientErrorKind::Rpc(
            Code::Cancelled
            | Code::Unknown
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::Internal
            | Code::Unavailable,
        ) => TransportFailure::Retryable,
        ReplicationClientErrorKind::InvalidConfiguration
        | ReplicationClientErrorKind::CredentialFile
        | ReplicationClientErrorKind::TlsConfiguration
        | ReplicationClientErrorKind::BatchLimit
        | ReplicationClientErrorKind::WireEncoding
        | ReplicationClientErrorKind::ResponseMismatch
        | ReplicationClientErrorKind::Rpc(_) => TransportFailure::Permanent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{
        CommitmentEvidence, Ed25519ReceiptSigner, LogicalKey, ObservationId,
        RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1, REPLICATION_PROTOCOL_VERSION, ReceiptDisposition,
        ReplicationOffer, compute_content_digest,
    };
    use ed25519_dalek::{
        SigningKey,
        pkcs8::{EncodePrivateKey, EncodePublicKey, spki::der::pem::LineEnding},
    };
    use std::{
        collections::VecDeque,
        fs,
        path::PathBuf,
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct TestKeys {
        directory: PathBuf,
        signer: Ed25519ReceiptSigner,
        keyring: Option<Ed25519ReceiptKeyring>,
    }

    impl TestKeys {
        fn new(label: &str) -> Self {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "blockzilla-replication-sender-{label}-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&directory).unwrap();
            let signing_key = SigningKey::from_bytes(&[42; 32]);
            let private = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
            let public = signing_key
                .verifying_key()
                .to_public_key_pem(LineEnding::LF)
                .unwrap();
            let private_path = directory.join("receipt.private.pem");
            let public_path = directory.join("receipt.public.pem");
            fs::write(&private_path, private.as_bytes()).unwrap();
            fs::write(&public_path, public.as_bytes()).unwrap();
            #[cfg(unix)]
            {
                fs::set_permissions(&private_path, fs::Permissions::from_mode(0o600)).unwrap();
                fs::set_permissions(&public_path, fs::Permissions::from_mode(0o644)).unwrap();
            }
            let signer = Ed25519ReceiptSigner::load_pkcs8_pem("receipt-key", private_path).unwrap();
            let keyring = Ed25519ReceiptKeyring::load_spki_pem("receipt-key", public_path).unwrap();
            Self {
                directory,
                signer,
                keyring: Some(keyring),
            }
        }
    }

    impl Drop for TestKeys {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct MockAdvance {
        through_sequence: u64,
    }

    struct MockBackend {
        stream: ReplicationStreamId,
        next_sequence: u64,
        records: Vec<RawReplicationRecord>,
        latest_ack: Option<CumulativePrimaryAck>,
        minimum_term: u64,
        fail_commit: bool,
        advances: usize,
        fence_commits: usize,
    }

    impl SenderBackend for MockBackend {
        type Reservation = MockAdvance;

        fn next_sequence(&self) -> u64 {
            self.next_sequence
        }

        fn latest_ack(&self, stream: &ReplicationStreamId) -> Option<CumulativePrimaryAck> {
            assert_eq!(stream, &self.stream);
            self.latest_ack.clone()
        }

        fn minimum_primary_term(&self, cluster_id: &str) -> u64 {
            assert_eq!(cluster_id, self.stream.cluster_id);
            self.minimum_term
        }

        fn reserve(
            &self,
            max_records: usize,
            max_compressed_bytes: u64,
            max_uncompressed_bytes: u64,
        ) -> Result<ReservedBatch<Self::Reservation>, BackendFailure> {
            let mut bytes = 0u64;
            let mut uncompressed_bytes = 0u64;
            let selected = self
                .records
                .iter()
                .filter(|record| record.offer.record.sequence >= self.next_sequence)
                .take(max_records)
                .take_while(|record| {
                    let next = bytes + record.compressed_payload.len() as u64;
                    let next_uncompressed = uncompressed_bytes + record.uncompressed_len;
                    if next > max_compressed_bytes || next_uncompressed > max_uncompressed_bytes {
                        false
                    } else {
                        bytes = next;
                        uncompressed_bytes = next_uncompressed;
                        true
                    }
                })
                .cloned()
                .collect::<Vec<_>>();
            let Some(through_sequence) = selected.last().map(|tail| tail.offer.record.sequence)
            else {
                return Ok(None);
            };
            Ok(Some((selected, MockAdvance { through_sequence })))
        }

        fn commit_and_advance(
            &mut self,
            verified: VerifiedCumulativeAck,
            reservation: Self::Reservation,
        ) -> Result<(), BackendFailure> {
            if self.fail_commit {
                return Err(BackendFailure::AckWal);
            }
            assert_eq!(
                verified.ack().through_sequence,
                reservation.through_sequence
            );
            self.minimum_term = self.minimum_term.max(verified.ack().primary_term);
            self.latest_ack = Some(verified.ack().clone());
            self.next_sequence = reservation.through_sequence + 1;
            self.advances += 1;
            Ok(())
        }

        fn commit_existing_prefix(
            &mut self,
            verified: VerifiedCumulativeAck,
        ) -> Result<(), BackendFailure> {
            if self.fail_commit {
                return Err(BackendFailure::AckWal);
            }
            let previous = self.latest_ack.as_ref().unwrap();
            assert_eq!(verified.ack().through_sequence, previous.through_sequence);
            assert_eq!(
                verified.ack().rolling_chain_digest,
                previous.rolling_chain_digest
            );
            self.minimum_term = self.minimum_term.max(verified.ack().primary_term);
            self.latest_ack = Some(verified.ack().clone());
            self.fence_commits += 1;
            Ok(())
        }
    }

    struct MockTransport {
        get_ack: Mutex<Option<Result<GetAckState, TransportFailure>>>,
        push_replies: Mutex<VecDeque<Result<CumulativePrimaryAck, TransportFailure>>>,
        pushed_sequences: Mutex<Vec<Vec<u64>>>,
    }

    impl ReplicationTransport for MockTransport {
        fn get_ack<'a>(
            &'a self,
            _stream: &'a ReplicationStreamId,
        ) -> TransportFuture<'a, GetAckState> {
            let result = self.get_ack.lock().unwrap().take().unwrap();
            Box::pin(async move { result })
        }

        fn push_batch<'a>(
            &'a self,
            _stream: &'a ReplicationStreamId,
            records: Vec<RawReplicationRecord>,
        ) -> TransportFuture<'a, CumulativePrimaryAck> {
            self.pushed_sequences.lock().unwrap().push(
                records
                    .iter()
                    .map(|record| record.offer.record.sequence)
                    .collect(),
            );
            let result = self.push_replies.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { result })
        }
    }

    fn stream() -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: "mainnet".to_owned(),
            origin_node_id: "source-node-replica".to_owned(),
            source_id: "yellowstone-blocks".to_owned(),
            journal_id: [7; 16],
        }
    }

    fn records(stream: &ReplicationStreamId, count: u64) -> Vec<RawReplicationRecord> {
        (0..count)
            .map(|sequence| {
                let compressed_payload = vec![sequence as u8 + 1; 32];
                let logical_key = LogicalKey::Block {
                    slot: 100 + sequence,
                    blockhash: [sequence as u8; 32],
                };
                RawReplicationRecord {
                    offer: ReplicationOffer {
                        protocol_version: REPLICATION_PROTOCOL_VERSION,
                        cluster_id: stream.cluster_id.clone(),
                        record: ObservationId {
                            origin_node_id: stream.origin_node_id.clone(),
                            journal_id: stream.journal_id,
                            sequence,
                        },
                        source_id: stream.source_id.clone(),
                        logical_key: logical_key.clone(),
                        content_digest: compute_content_digest(
                            &stream.cluster_id,
                            &logical_key,
                            RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
                            &compressed_payload,
                        ),
                        payload_len: compressed_payload.len() as u64,
                        payload_format_version: RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
                        commitment: CommitmentEvidence::Confirmed,
                    },
                    compressed_payload,
                    raw_protobuf_sha256: [9; 32],
                    uncompressed_len: 64,
                }
            })
            .collect()
    }

    fn signed_ack(
        signer: &Ed25519ReceiptSigner,
        stream: &ReplicationStreamId,
        records: &[RawReplicationRecord],
        initial_chain: ContentDigest,
        primary_term: u64,
    ) -> CumulativePrimaryAck {
        let prepared = prepare_records(stream, initial_chain, records).unwrap();
        let mut ack = CumulativePrimaryAck {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            stream: stream.clone(),
            primary_id: "blockzilla-primary".to_owned(),
            primary_term,
            through_sequence: prepared.through_sequence,
            through_content_digest: prepared.through_content_digest,
            rolling_chain_digest: prepared.chain,
            disposition: ReceiptDisposition::DurablyStored,
            durable_lsn: prepared.through_sequence + 1,
            signing_key_id: String::new(),
            signature: Vec::new(),
        };
        signer.sign_cumulative_ack(&mut ack).unwrap();
        ack
    }

    fn core(
        mut keys: TestKeys,
        records: Vec<RawReplicationRecord>,
        get_ack: Result<GetAckState, TransportFailure>,
        push_replies: Vec<Result<CumulativePrimaryAck, TransportFailure>>,
    ) -> SenderCore<MockTransport, MockBackend> {
        let stream = stream();
        let chain = cumulative_chain_seed(&stream).unwrap();
        SenderCore {
            transport: MockTransport {
                get_ack: Mutex::new(Some(get_ack)),
                push_replies: Mutex::new(push_replies.into()),
                pushed_sequences: Mutex::new(Vec::new()),
            },
            backend: MockBackend {
                stream: stream.clone(),
                next_sequence: 0,
                records,
                latest_ack: None,
                minimum_term: 0,
                fail_commit: false,
                advances: 0,
                fence_commits: 0,
            },
            keyring: keys.keyring.take().unwrap(),
            stream,
            expected_primary_id: "blockzilla-primary".to_owned(),
            max_batch_events: 8,
            max_batch_compressed_bytes: 4096,
            max_batch_uncompressed_bytes: 4096,
            chain,
            observed_primary_term: 0,
        }
    }

    #[tokio::test]
    async fn successful_push_fsyncs_before_advancing_cursor_and_chain() {
        let keys = TestKeys::new("success");
        let stream = stream();
        let records = records(&stream, 2);
        let seed = cumulative_chain_seed(&stream).unwrap();
        let ack = signed_ack(&keys.signer, &stream, &records, seed, 1);
        let expected_chain = ack.rolling_chain_digest;
        let mut sender = core(
            keys,
            records,
            Ok(GetAckState::NoDurablePrefix),
            vec![Ok(ack)],
        );
        sender.reconcile_startup().await.unwrap();

        let outcome = sender.send_once().await.unwrap();

        assert_eq!(
            outcome,
            ReplicationSendOutcome::Advanced {
                records: 2,
                compressed_bytes: 64,
                through_sequence: 1,
                primary_term: 1,
            }
        );
        assert_eq!(sender.backend.next_sequence, 2);
        assert_eq!(sender.backend.advances, 1);
        assert_eq!(sender.chain, expected_chain);
    }

    #[tokio::test]
    async fn push_failure_keeps_exact_batch_and_chain_retryable() {
        let keys = TestKeys::new("push-failure");
        let stream = stream();
        let records = records(&stream, 2);
        let seed = cumulative_chain_seed(&stream).unwrap();
        let ack = signed_ack(&keys.signer, &stream, &records, seed, 1);
        let mut sender = core(
            keys,
            records,
            Ok(GetAckState::NoDurablePrefix),
            vec![Err(TransportFailure::Retryable), Ok(ack)],
        );
        sender.reconcile_startup().await.unwrap();

        assert_eq!(
            sender.send_once().await.unwrap_err().kind(),
            ReplicationSenderErrorKind::Transport
        );
        assert_eq!(sender.backend.next_sequence, 0);
        assert_eq!(sender.chain, seed);
        sender.send_once().await.unwrap();
        assert_eq!(sender.backend.next_sequence, 2);
        assert_eq!(
            *sender.transport.pushed_sequences.lock().unwrap(),
            vec![vec![0, 1], vec![0, 1]]
        );
    }

    #[tokio::test]
    async fn forged_or_wrong_ack_never_advances_cursor_or_chain() {
        let keys = TestKeys::new("forged-ack");
        let stream = stream();
        let records = records(&stream, 2);
        let seed = cumulative_chain_seed(&stream).unwrap();
        let mut forged = signed_ack(&keys.signer, &stream, &records, seed, 1);
        forged.signature[0] ^= 0xff;
        let wrong_prefix = signed_ack(&keys.signer, &stream, &records[..1], seed, 1);
        let mut sender = core(
            keys,
            records,
            Ok(GetAckState::NoDurablePrefix),
            vec![Ok(forged), Ok(wrong_prefix)],
        );
        sender.reconcile_startup().await.unwrap();

        assert_eq!(
            sender.send_once().await.unwrap_err().kind(),
            ReplicationSenderErrorKind::InvalidAck
        );
        assert_eq!(sender.backend.next_sequence, 0);
        assert_eq!(sender.backend.advances, 0);
        assert_eq!(sender.chain, seed);
        assert_eq!(
            sender.send_once().await.unwrap_err().kind(),
            ReplicationSenderErrorKind::InvalidAck
        );
        assert_eq!(sender.backend.next_sequence, 0);
        assert_eq!(sender.backend.advances, 0);
        assert_eq!(sender.chain, seed);
        assert_eq!(
            *sender.transport.pushed_sequences.lock().unwrap(),
            vec![vec![0, 1], vec![0, 1]]
        );
    }

    #[tokio::test]
    async fn ack_wal_failure_leaves_cursor_and_chain_unchanged() {
        let keys = TestKeys::new("ack-wal-failure");
        let stream = stream();
        let records = records(&stream, 1);
        let seed = cumulative_chain_seed(&stream).unwrap();
        let ack = signed_ack(&keys.signer, &stream, &records, seed, 1);
        let mut sender = core(
            keys,
            records,
            Ok(GetAckState::NoDurablePrefix),
            vec![Ok(ack)],
        );
        sender.backend.fail_commit = true;
        sender.reconcile_startup().await.unwrap();

        assert_eq!(
            sender.send_once().await.unwrap_err().kind(),
            ReplicationSenderErrorKind::AckWal
        );
        assert_eq!(sender.backend.next_sequence, 0);
        assert_eq!(sender.backend.advances, 0);
        assert_eq!(sender.chain, seed);
    }

    #[tokio::test]
    async fn remote_ahead_is_recovered_by_one_exact_bounded_reservation() {
        let keys = TestKeys::new("remote-ahead");
        let stream = stream();
        let records = records(&stream, 3);
        let seed = cumulative_chain_seed(&stream).unwrap();
        let remote = signed_ack(&keys.signer, &stream, &records[..2], seed, 4);
        let expected_chain = remote.rolling_chain_digest;
        let mut sender = core(
            keys,
            records,
            Ok(GetAckState::DurablePrefix(remote)),
            vec![],
        );

        sender.reconcile_startup().await.unwrap();

        assert_eq!(sender.backend.next_sequence, 2);
        assert_eq!(sender.backend.advances, 1);
        assert_eq!(sender.chain, expected_chain);
        assert!(sender.transport.pushed_sequences.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn equal_remote_prefix_is_reverified_without_advancing_local_cursor() {
        let keys = TestKeys::new("remote-equal");
        let stream = stream();
        let records = records(&stream, 2);
        let seed = cumulative_chain_seed(&stream).unwrap();
        let local = signed_ack(&keys.signer, &stream, &records, seed, 3);
        // A newly fenced primary may sign the same durable fields with a higher term. Startup
        // verifies it against the local durable prefix and fsyncs the stronger term without moving
        // a cursor which is already at sequence two.
        let remote = signed_ack(&keys.signer, &stream, &records, seed, 4);
        let mut sender = core(
            keys,
            records,
            Ok(GetAckState::DurablePrefix(remote)),
            vec![],
        );
        sender.backend.latest_ack = Some(local.clone());
        sender.backend.minimum_term = local.primary_term;
        sender.backend.next_sequence = 2;
        sender.chain = local.rolling_chain_digest;

        sender.reconcile_startup().await.unwrap();

        assert_eq!(sender.backend.next_sequence, 2);
        assert_eq!(sender.backend.advances, 0);
        assert_eq!(sender.backend.fence_commits, 1);
        assert_eq!(sender.backend.latest_ack.as_ref().unwrap().primary_term, 4);
        assert_eq!(sender.chain, local.rolling_chain_digest);
        assert_eq!(sender.observed_primary_term, 4);
    }

    #[tokio::test]
    async fn equal_prefix_term_is_not_observed_until_its_fence_is_durable() {
        let keys = TestKeys::new("remote-equal-fsync-failure");
        let stream = stream();
        let records = records(&stream, 2);
        let seed = cumulative_chain_seed(&stream).unwrap();
        let local = signed_ack(&keys.signer, &stream, &records, seed, 3);
        let remote = signed_ack(&keys.signer, &stream, &records, seed, 4);
        let mut sender = core(
            keys,
            records,
            Ok(GetAckState::DurablePrefix(remote)),
            vec![],
        );
        sender.backend.latest_ack = Some(local.clone());
        sender.backend.minimum_term = local.primary_term;
        sender.backend.next_sequence = 2;
        sender.backend.fail_commit = true;
        sender.chain = local.rolling_chain_digest;

        assert_eq!(
            sender.reconcile_startup().await.unwrap_err().kind(),
            ReplicationSenderErrorKind::AckWal
        );
        assert_eq!(sender.backend.next_sequence, 2);
        assert_eq!(sender.backend.advances, 0);
        assert_eq!(sender.backend.fence_commits, 0);
        assert_eq!(sender.backend.latest_ack.as_ref().unwrap().primary_term, 3);
        assert_eq!(sender.observed_primary_term, 3);
    }

    #[tokio::test]
    async fn missing_remote_prefix_with_local_durable_ack_fails_closed() {
        let keys = TestKeys::new("remote-missing");
        let stream = stream();
        let records = records(&stream, 1);
        let seed = cumulative_chain_seed(&stream).unwrap();
        let local = signed_ack(&keys.signer, &stream, &records, seed, 2);
        let mut sender = core(keys, records, Ok(GetAckState::NoDurablePrefix), vec![]);
        sender.backend.latest_ack = Some(local.clone());
        sender.backend.minimum_term = local.primary_term;
        sender.backend.next_sequence = 1;
        sender.chain = local.rolling_chain_digest;

        assert_eq!(
            sender.reconcile_startup().await.unwrap_err().kind(),
            ReplicationSenderErrorKind::Reconciliation
        );
        assert_eq!(sender.backend.next_sequence, 1);
        assert_eq!(sender.backend.advances, 0);
    }

    #[tokio::test]
    async fn remote_rollback_from_local_durable_ack_fails_closed() {
        let keys = TestKeys::new("remote-rollback");
        let stream = stream();
        let records = records(&stream, 2);
        let seed = cumulative_chain_seed(&stream).unwrap();
        let local = signed_ack(&keys.signer, &stream, &records, seed, 3);
        let remote = signed_ack(&keys.signer, &stream, &records[..1], seed, 3);
        let mut sender = core(
            keys,
            records,
            Ok(GetAckState::DurablePrefix(remote)),
            vec![],
        );
        sender.backend.latest_ack = Some(local.clone());
        sender.backend.minimum_term = local.primary_term;
        sender.backend.next_sequence = 2;
        sender.chain = local.rolling_chain_digest;

        assert_eq!(
            sender.reconcile_startup().await.unwrap_err().kind(),
            ReplicationSenderErrorKind::Reconciliation
        );
        assert_eq!(sender.backend.next_sequence, 2);
        assert_eq!(sender.backend.advances, 0);
    }

    #[test]
    fn only_transient_transport_failures_are_retryable() {
        use tonic::Code;

        for kind in [
            ReplicationClientErrorKind::ConnectTimeout,
            ReplicationClientErrorKind::Connect,
            ReplicationClientErrorKind::RequestTimeout,
            ReplicationClientErrorKind::Rpc(Code::Unavailable),
            ReplicationClientErrorKind::Rpc(Code::DeadlineExceeded),
        ] {
            assert_eq!(
                classify_client_transport_kind(kind),
                TransportFailure::Retryable
            );
        }
        for kind in [
            ReplicationClientErrorKind::InvalidConfiguration,
            ReplicationClientErrorKind::CredentialFile,
            ReplicationClientErrorKind::TlsConfiguration,
            ReplicationClientErrorKind::BatchLimit,
            ReplicationClientErrorKind::WireEncoding,
            ReplicationClientErrorKind::ResponseMismatch,
            ReplicationClientErrorKind::Rpc(Code::Unauthenticated),
            ReplicationClientErrorKind::Rpc(Code::PermissionDenied),
            ReplicationClientErrorKind::Rpc(Code::InvalidArgument),
        ] {
            assert_eq!(
                classify_client_transport_kind(kind),
                TransportFailure::Permanent
            );
        }
    }
}
