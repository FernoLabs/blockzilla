//! Outbound pull client for a Source node-hosted durable raw source.
//!
//! The ordering in this module is the safety contract:
//!
//! 1. pull one server-bounded batch without supplying a cursor;
//! 2. push that exact batch to Blockzilla's existing mTLS raw receiver;
//! 3. validate the receiver's signed cumulative ACK against the exact pulled tail; and
//! 4. return that ACK to Source node and wait for its correlated commit response.
//!
//! A failed remote commit never permits another batch to be pulled by this call. Retrying is safe:
//! Source node serves the same uncommitted batch and the local receiver accepts the exact duplicate
//! inside its bounded retry window without appending it again.

use std::{error::Error, fmt, future::Future, pin::Pin, time::Duration};

use futures::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    time::{Instant, timeout, timeout_at},
};
use tonic::{Code, transport::Channel};

use super::{
    BoundedPushBatchBuilder, ClientTlsConfig, ContentDigest, CumulativePrimaryAck,
    Ed25519ReceiptKeyring, REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES, RawReceiverLimits,
    RawReplicationClient, RawReplicationRecord, ReplicaUpstreamConfig, ReplicationClientErrorKind,
    ReplicationStreamId, TrustedReceiptKeyConfig, ValidatedCommitAckResponse,
    ValidatedPullBatchRequest, connect_strict_mtls_channel, wire,
};

const MAX_PULL_WIRE_RECORD_BYTES: usize = 256 * 1024 * 1024;
const MAX_PULL_CONTROL_MESSAGE_BYTES: usize = 64 * 1024;
const PULL_SYNC_PROTOCOL_VERSION: u32 = 2;
const PULL_SYNC_OUTBOUND_CAPACITY: usize = 2;

type PullFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, PullClientError>> + Send + 'a>>;

/// Wire protocol used between the outbound Blockzilla client and the durable Source node source.
///
/// Version 1 is the proven `PullBatch` + `CommitAck` stop-and-wait exchange. Version 2 keeps the
/// same stop-and-wait durability contract on one bidirectional stream. The default deliberately
/// remains version 1 until the streaming canary has passed its restart and replay gates.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PullClientProtocol {
    #[default]
    V1,
    V2,
}

/// mTLS transport and bounded-batch policy for the server-pinned Source node source.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullRemoteSourceConfig {
    #[serde(default)]
    pub protocol: PullClientProtocol,
    pub endpoint: String,
    pub tls: ClientTlsConfig,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub stream_idle_timeout_ms: u64,
    pub batch: PullClientBatchLimits,
}

/// Limits for one source-selected response stream. There is intentionally no client cursor or
/// batch-size request field in this contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullClientBatchLimits {
    pub max_events: usize,
    pub max_bytes: u64,
    pub max_uncompressed_bytes: u64,
    pub max_compressed_event_bytes: u64,
    pub max_uncompressed_event_bytes: u64,
}

impl fmt::Debug for PullRemoteSourceConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullRemoteSourceConfig")
            .field("protocol", &self.protocol)
            .field("transport", &"mTLS")
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("stream_idle_timeout_ms", &self.stream_idle_timeout_ms)
            .field("batch", &self.batch)
            .finish_non_exhaustive()
    }
}

/// Complete pull-client configuration.
///
/// `local_sink` points to Blockzilla's existing hardened `RawReplication` receiver. Its trusted
/// receipt keys are also used locally to reject a forged or misrouted ACK before forwarding it to
/// Source node. Its cumulative-ACK WAL field is intentionally not touched by this client.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullClientConfig {
    pub source: PullRemoteSourceConfig,
    pub local_sink: ReplicaUpstreamConfig,
}

impl fmt::Debug for PullClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullClientConfig")
            .field("source", &self.source)
            .field("local_sink_transport", &"mTLS")
            .field(
                "local_sink_expected_primary_id",
                &self.local_sink.expected_primary_id,
            )
            .finish_non_exhaustive()
    }
}

/// Stable, non-secret failure categories suitable for retry policy, logs, and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PullClientErrorKind {
    InvalidConfiguration,
    TrustedSinkKeys,
    SourceTransport(ReplicationClientErrorKind),
    SinkTransport(ReplicationClientErrorKind),
    RequestTimeout,
    StreamIdleTimeout,
    SourceRpc(Code),
    WireEncoding,
    InvalidSinkAck,
    CommitMismatch,
}

/// Deliberately redacted pull-client failure.
///
/// Endpoint names, certificate paths, remote status messages, records, and signatures are never
/// included in either `Display` or `Debug`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PullClientError {
    kind: PullClientErrorKind,
}

impl PullClientError {
    fn new(kind: PullClientErrorKind) -> Self {
        Self { kind }
    }

    pub fn kind(&self) -> PullClientErrorKind {
        self.kind
    }

    /// Whether retrying the same uncommitted source batch is an appropriate automatic response.
    /// Authentication, configuration, trust, wire-integrity, ACK, and correlation failures remain
    /// fail-closed and require operator action.
    pub fn is_retryable(&self) -> bool {
        match self.kind {
            PullClientErrorKind::SourceTransport(kind)
            | PullClientErrorKind::SinkTransport(kind) => retryable_transport_error(kind),
            PullClientErrorKind::RequestTimeout | PullClientErrorKind::StreamIdleTimeout => true,
            PullClientErrorKind::SourceRpc(code) => retryable_rpc_code(code),
            PullClientErrorKind::InvalidConfiguration
            | PullClientErrorKind::TrustedSinkKeys
            | PullClientErrorKind::WireEncoding
            | PullClientErrorKind::InvalidSinkAck
            | PullClientErrorKind::CommitMismatch => false,
        }
    }
}

impl fmt::Display for PullClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            PullClientErrorKind::InvalidConfiguration => "invalid pull client configuration",
            PullClientErrorKind::TrustedSinkKeys => {
                "local receiver trust keys are unavailable or invalid"
            }
            PullClientErrorKind::SourceTransport(_) => "durable source transport is unavailable",
            PullClientErrorKind::SinkTransport(_) => "local durable receiver is unavailable",
            PullClientErrorKind::RequestTimeout => "pull replication request timed out",
            PullClientErrorKind::StreamIdleTimeout => "pulled batch stream became idle",
            PullClientErrorKind::SourceRpc(_) => "durable source RPC failed",
            PullClientErrorKind::WireEncoding => "pulled replication message is invalid",
            PullClientErrorKind::InvalidSinkAck => {
                "local receiver ACK is invalid or does not cover the pulled batch"
            }
            PullClientErrorKind::CommitMismatch => {
                "durable source commit response does not match the submitted ACK"
            }
        };
        formatter.write_str(message)
    }
}

impl fmt::Debug for PullClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullClientError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl Error for PullClientError {}

/// Result of one strictly ordered pull attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullReplicationOutcome {
    CaughtUp,
    /// Protocol-v2 liveness while the durable source is caught up. This does not acknowledge data
    /// and grants no replay or retention authority.
    HeadHeartbeat {
        heartbeat_id: u64,
    },
    Committed {
        protocol: PullClientProtocol,
        stream: ReplicationStreamId,
        records: usize,
        compressed_bytes: u64,
        uncompressed_bytes: u64,
        through_sequence: u64,
        sink_primary_term: u64,
        /// True only when the source recognized an exact ACK-WAL response-loss replay.
        source_ack_replayed: bool,
    },
}

#[derive(Debug, Clone, Copy)]
struct PullLimits {
    receiver: RawReceiverLimits,
    max_decoding_message_bytes: usize,
    connect_timeout: Duration,
    request_timeout: Duration,
    stream_idle_timeout: Duration,
}

#[derive(Debug, Clone)]
struct PulledBatch {
    stream: ReplicationStreamId,
    records: Vec<RawReplicationRecord>,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
}

impl PulledBatch {
    fn witness(&self) -> PulledBatchWitness {
        let tail = self
            .records
            .last()
            .expect("a completed pulled batch is non-empty");
        PulledBatchWitness {
            stream: self.stream.clone(),
            through_sequence: tail.offer.record.sequence,
            through_content_digest: tail.offer.content_digest,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PulledBatchWitness {
    stream: ReplicationStreamId,
    through_sequence: u64,
    through_content_digest: ContentDigest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyncBatchHeader {
    batch_id: u64,
    stream: ReplicationStreamId,
    first_sequence: u64,
    through_sequence: u64,
    record_count: usize,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
    through_content_digest: ContentDigest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyncServerLimits {
    max_batch_records: usize,
    max_batch_compressed_bytes: u64,
    max_batch_uncompressed_bytes: u64,
    tail_poll_interval: Duration,
    heartbeat_interval: Duration,
}

#[derive(Debug)]
enum SyncReceiveOutcome {
    Heartbeat {
        heartbeat_id: u64,
    },
    Batch {
        header: Box<SyncBatchHeader>,
        batch: PulledBatch,
    },
}

enum PullSourceSession {
    V1(PullSourceTransport),
    V2(Box<PullSyncTransport>),
}

impl PullSourceSession {
    fn protocol(&self) -> PullClientProtocol {
        match self {
            Self::V1(_) => PullClientProtocol::V1,
            Self::V2(_) => PullClientProtocol::V2,
        }
    }
}

trait DurableSink {
    fn push_batch<'a>(
        &'a self,
        stream: &'a ReplicationStreamId,
        records: Vec<RawReplicationRecord>,
    ) -> PullFuture<'a, CumulativePrimaryAck>;
}

trait AckCommitter {
    fn commit_ack<'a>(
        &'a self,
        ack: &'a CumulativePrimaryAck,
    ) -> PullFuture<'a, ValidatedCommitAckResponse>;
}

impl DurableSink for RawReplicationClient {
    fn push_batch<'a>(
        &'a self,
        stream: &'a ReplicationStreamId,
        records: Vec<RawReplicationRecord>,
    ) -> PullFuture<'a, CumulativePrimaryAck> {
        Box::pin(async move {
            RawReplicationClient::push_batch(self, stream, records)
                .await
                .map_err(|error| {
                    PullClientError::new(PullClientErrorKind::SinkTransport(error.kind()))
                })
        })
    }
}

#[derive(Clone)]
struct PullSourceTransport {
    channel: Channel,
    limits: PullLimits,
}

impl PullSourceTransport {
    fn rpc_client(&self) -> wire::raw_replication_pull_client::RawReplicationPullClient<Channel> {
        wire::raw_replication_pull_client::RawReplicationPullClient::new(self.channel.clone())
            .max_encoding_message_size(MAX_PULL_CONTROL_MESSAGE_BYTES)
            .max_decoding_message_size(self.limits.max_decoding_message_bytes)
    }

    async fn pull_batch(&self) -> Result<Option<PulledBatch>, PullClientError> {
        let deadline = request_deadline(self.limits.request_timeout)?;
        let request = wire::PullBatchRequest::from(ValidatedPullBatchRequest);
        let mut client = self.rpc_client();
        let response = timeout_at(deadline, client.pull_batch(request))
            .await
            .map_err(|_| pull_error(PullClientErrorKind::RequestTimeout))?
            .map_err(source_rpc_error)?;
        let mut stream = response.into_inner();
        let mut builder = BoundedPushBatchBuilder::new(self.limits.receiver)
            .map_err(|_| pull_error(PullClientErrorKind::InvalidConfiguration))?;

        loop {
            let item = timeout_at(
                deadline,
                timeout(self.limits.stream_idle_timeout, stream.message()),
            )
            .await
            .map_err(|_| pull_error(PullClientErrorKind::RequestTimeout))?
            .map_err(|_| pull_error(PullClientErrorKind::StreamIdleTimeout))?
            .map_err(source_rpc_error)?;
            let Some(item) = item else {
                break;
            };
            builder
                .push_wire(item)
                .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))?;
        }
        finish_pulled_batch(builder)
    }
}

impl AckCommitter for PullSourceTransport {
    fn commit_ack<'a>(
        &'a self,
        ack: &'a CumulativePrimaryAck,
    ) -> PullFuture<'a, ValidatedCommitAckResponse> {
        Box::pin(async move {
            let request = wire::CumulativePrimaryAck::try_from(ack)
                .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))?;
            let mut client = self.rpc_client();
            let response = timeout(self.limits.request_timeout, client.commit_ack(request))
                .await
                .map_err(|_| pull_error(PullClientErrorKind::RequestTimeout))?
                .map_err(source_rpc_error)?
                .into_inner();
            ValidatedCommitAckResponse::try_from(response)
                .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))
        })
    }
}

struct PullSyncTransport {
    outbound: mpsc::Sender<wire::SyncClientMessage>,
    inbound: tonic::Streaming<wire::SyncServerMessage>,
    limits: PullLimits,
    server_limits: SyncServerLimits,
}

impl PullSyncTransport {
    async fn connect(channel: Channel, limits: PullLimits) -> Result<Self, PullClientError> {
        let (outbound, receiver) = mpsc::channel(PULL_SYNC_OUTBOUND_CAPACITY);
        outbound
            .send(wire::SyncClientMessage {
                frame: Some(wire::sync_client_message::Frame::Hello(
                    wire::SyncClientHello {
                        protocol_version: PULL_SYNC_PROTOCOL_VERSION,
                    },
                )),
            })
            .await
            .map_err(|_| pull_error(PullClientErrorKind::SourceRpc(Code::Unavailable)))?;
        let outbound_stream = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|message| (message, receiver))
        });
        let mut client = wire::raw_replication_pull_client::RawReplicationPullClient::new(channel)
            .max_encoding_message_size(MAX_PULL_CONTROL_MESSAGE_BYTES)
            .max_decoding_message_size(limits.max_decoding_message_bytes);
        let response = timeout(limits.request_timeout, client.sync(outbound_stream))
            .await
            .map_err(|_| pull_error(PullClientErrorKind::RequestTimeout))?
            .map_err(source_rpc_error)?;
        let mut transport = Self {
            outbound,
            inbound: response.into_inner(),
            limits,
            // Replaced only after the authenticated peer's first frame is validated.
            server_limits: SyncServerLimits {
                max_batch_records: 0,
                max_batch_compressed_bytes: 0,
                max_batch_uncompressed_bytes: 0,
                tail_poll_interval: Duration::ZERO,
                heartbeat_interval: Duration::ZERO,
            },
        };
        let hello = match transport.receive_frame().await? {
            wire::sync_server_message::Frame::Hello(hello) => hello,
            _ => return Err(pull_error(PullClientErrorKind::WireEncoding)),
        };
        transport.server_limits = validate_sync_server_hello(hello, limits)?;
        Ok(transport)
    }

    async fn receive(&mut self) -> Result<SyncReceiveOutcome, PullClientError> {
        match self.receive_frame().await? {
            wire::sync_server_message::Frame::Heartbeat(heartbeat) => {
                Ok(SyncReceiveOutcome::Heartbeat {
                    heartbeat_id: heartbeat.heartbeat_id,
                })
            }
            wire::sync_server_message::Frame::Batch(batch) => {
                receive_sync_batch(&mut self.inbound, self.limits, self.server_limits, batch).await
            }
            wire::sync_server_message::Frame::Hello(_)
            | wire::sync_server_message::Frame::Record(_)
            | wire::sync_server_message::Frame::AckCommitted(_) => {
                Err(pull_error(PullClientErrorKind::WireEncoding))
            }
        }
    }

    async fn commit_ack(
        &mut self,
        header: &SyncBatchHeader,
        ack: &CumulativePrimaryAck,
    ) -> Result<bool, PullClientError> {
        let ack = wire::CumulativePrimaryAck::try_from(ack)
            .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))?;
        let message = wire::SyncClientMessage {
            frame: Some(wire::sync_client_message::Frame::Ack(wire::SyncClientAck {
                batch_id: header.batch_id,
                ack: Some(ack),
            })),
        };
        // One fixed deadline covers enqueue, every intervening heartbeat, and the correlated
        // AckCommitted. A responsive peer cannot keep an uncommitted batch alive indefinitely by
        // sending liveness-only frames.
        let deadline = request_deadline(self.limits.request_timeout)?;
        timeout_at(deadline, self.outbound.send(message))
            .await
            .map_err(|_| pull_error(PullClientErrorKind::RequestTimeout))?
            .map_err(|_| pull_error(PullClientErrorKind::SourceRpc(Code::Unavailable)))?;
        wait_for_sync_ack_committed(
            &mut self.inbound,
            self.limits.stream_idle_timeout,
            deadline,
            header,
        )
        .await
    }

    async fn receive_frame(&mut self) -> Result<wire::sync_server_message::Frame, PullClientError> {
        receive_sync_frame(&mut self.inbound, self.limits.stream_idle_timeout, None).await
    }
}

async fn receive_sync_batch<S>(
    inbound: &mut S,
    limits: PullLimits,
    server_limits: SyncServerLimits,
    batch: wire::SyncBatch,
) -> Result<SyncReceiveOutcome, PullClientError>
where
    S: Stream<Item = Result<wire::SyncServerMessage, tonic::Status>> + Unpin,
{
    // Start the absolute deadline when the Batch header reaches the client. Per-frame idle checks
    // remain useful diagnostics, but receiving one record never resets this total budget.
    let deadline = request_deadline(limits.request_timeout)?;
    let header = validate_sync_batch_header(batch, server_limits)?;
    let mut builder = BoundedPushBatchBuilder::new(limits.receiver)
        .map_err(|_| pull_error(PullClientErrorKind::InvalidConfiguration))?;
    for _ in 0..header.record_count {
        let record =
            match receive_sync_frame(inbound, limits.stream_idle_timeout, Some(deadline)).await? {
                wire::sync_server_message::Frame::Record(record) => record,
                _ => return Err(pull_error(PullClientErrorKind::WireEncoding)),
            };
        builder
            .push_wire(record)
            .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))?;
    }
    let batch = finish_sync_batch(&header, builder)?;
    Ok(SyncReceiveOutcome::Batch {
        header: Box::new(header),
        batch,
    })
}

async fn wait_for_sync_ack_committed<S>(
    inbound: &mut S,
    stream_idle_timeout: Duration,
    deadline: Instant,
    header: &SyncBatchHeader,
) -> Result<bool, PullClientError>
where
    S: Stream<Item = Result<wire::SyncServerMessage, tonic::Status>> + Unpin,
{
    loop {
        match receive_sync_frame(inbound, stream_idle_timeout, Some(deadline)).await? {
            wire::sync_server_message::Frame::AckCommitted(committed) => {
                return validate_sync_ack_committed(committed, header);
            }
            // A heartbeat is liveness only. Reusing the original deadline ensures it cannot
            // extend the durable ACK boundary.
            wire::sync_server_message::Frame::Heartbeat(_) => {}
            wire::sync_server_message::Frame::Hello(_)
            | wire::sync_server_message::Frame::Batch(_)
            | wire::sync_server_message::Frame::Record(_) => {
                return Err(pull_error(PullClientErrorKind::WireEncoding));
            }
        }
    }
}

async fn receive_sync_frame<S>(
    inbound: &mut S,
    stream_idle_timeout: Duration,
    absolute_deadline: Option<Instant>,
) -> Result<wire::sync_server_message::Frame, PullClientError>
where
    S: Stream<Item = Result<wire::SyncServerMessage, tonic::Status>> + Unpin,
{
    let item = match absolute_deadline {
        Some(deadline) => timeout_at(deadline, timeout(stream_idle_timeout, inbound.next()))
            .await
            .map_err(|_| pull_error(PullClientErrorKind::RequestTimeout))?,
        None => timeout(stream_idle_timeout, inbound.next()).await,
    }
    .map_err(|_| pull_error(PullClientErrorKind::StreamIdleTimeout))?;
    let message = item
        .ok_or_else(|| pull_error(PullClientErrorKind::SourceRpc(Code::Unavailable)))?
        .map_err(source_rpc_error)?;
    message
        .frame
        .ok_or_else(|| pull_error(PullClientErrorKind::WireEncoding))
}

impl fmt::Debug for PullSyncTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PullSyncTransport")
            .field("transport", &"persistent mTLS bidi stream")
            .field("server_limits", &self.server_limits)
            .finish_non_exhaustive()
    }
}

/// Production pull client. A single instance serializes `pull_once` calls through `&mut self`, so
/// a caller cannot overlap batches or advance past a failed `CommitAck` response.
pub struct RawReplicationPullClient {
    source: PullSourceSession,
    local_sink: RawReplicationClient,
    sink_keyring: Ed25519ReceiptKeyring,
    expected_sink_primary_id: String,
    bound_source_identity: Option<(String, String, String)>,
    highest_sink_term: u64,
}

impl RawReplicationPullClient {
    /// Validate limits and trust roots, connect to the local durable sink first, then connect to
    /// the public source. Connecting sink-first prevents fetching any source data when Blockzilla
    /// has nowhere durable to put it.
    pub async fn connect(config: &PullClientConfig) -> Result<Self, PullClientError> {
        Self::connect_with_protocol(config, config.source.protocol).await
    }

    /// Connect using an explicit protocol selection. This is intended for a controlled v2 canary;
    /// callers that need the safe config default should use [`Self::connect`].
    pub async fn connect_with_protocol(
        config: &PullClientConfig,
        protocol: PullClientProtocol,
    ) -> Result<Self, PullClientError> {
        let limits = validate_config(config)?;
        let sink_keyring = load_trusted_sink_keys(&config.local_sink.trusted_receipt_keys)?;
        let local_sink = RawReplicationClient::connect(&config.local_sink)
            .await
            .map_err(|error| pull_error(PullClientErrorKind::SinkTransport(error.kind())))?;
        let channel = connect_strict_mtls_channel(
            &config.source.endpoint,
            &config.source.tls,
            limits.connect_timeout,
            limits.request_timeout,
        )
        .await
        .map_err(|error| pull_error(PullClientErrorKind::SourceTransport(error.kind())))?;
        let source = match protocol {
            PullClientProtocol::V1 => {
                PullSourceSession::V1(PullSourceTransport { channel, limits })
            }
            PullClientProtocol::V2 => {
                PullSourceSession::V2(Box::new(PullSyncTransport::connect(channel, limits).await?))
            }
        };
        Ok(Self {
            source,
            local_sink,
            sink_keyring,
            expected_sink_primary_id: config.local_sink.expected_primary_id.clone(),
            bound_source_identity: None,
            highest_sink_term: 0,
        })
    }

    /// Pull, durably forward, and commit at most one source-selected batch.
    pub async fn pull_once(&mut self) -> Result<PullReplicationOutcome, PullClientError> {
        let Self {
            source,
            local_sink,
            sink_keyring,
            expected_sink_primary_id,
            bound_source_identity,
            highest_sink_term,
        } = self;

        match source {
            PullSourceSession::V1(source) => {
                let Some(batch) = source.pull_batch().await? else {
                    return Ok(PullReplicationOutcome::CaughtUp);
                };
                bind_source_identity(bound_source_identity, &batch.stream)?;
                let record_count = batch.records.len();
                let compressed_bytes = batch.compressed_bytes;
                let uncompressed_bytes = batch.uncompressed_bytes;
                let ack = forward_and_commit(
                    local_sink,
                    source,
                    sink_keyring,
                    expected_sink_primary_id,
                    *highest_sink_term,
                    batch,
                )
                .await?;
                *highest_sink_term = ack.primary_term;
                Ok(PullReplicationOutcome::Committed {
                    protocol: PullClientProtocol::V1,
                    stream: ack.stream,
                    records: record_count,
                    compressed_bytes,
                    uncompressed_bytes,
                    through_sequence: ack.through_sequence,
                    sink_primary_term: ack.primary_term,
                    source_ack_replayed: false,
                })
            }
            PullSourceSession::V2(source) => match source.receive().await? {
                SyncReceiveOutcome::Heartbeat { heartbeat_id } => {
                    Ok(PullReplicationOutcome::HeadHeartbeat { heartbeat_id })
                }
                SyncReceiveOutcome::Batch { header, batch } => {
                    bind_source_identity(bound_source_identity, &batch.stream)?;
                    let record_count = batch.records.len();
                    let compressed_bytes = batch.compressed_bytes;
                    let uncompressed_bytes = batch.uncompressed_bytes;
                    let witness = batch.witness();
                    let ack = DurableSink::push_batch(&*local_sink, &witness.stream, batch.records)
                        .await?;
                    validate_sink_ack(
                        &ack,
                        &witness,
                        expected_sink_primary_id,
                        *highest_sink_term,
                        sink_keyring,
                    )?;
                    let source_ack_replayed = source.commit_ack(&header, &ack).await?;
                    *highest_sink_term = ack.primary_term;
                    Ok(PullReplicationOutcome::Committed {
                        protocol: PullClientProtocol::V2,
                        stream: ack.stream,
                        records: record_count,
                        compressed_bytes,
                        uncompressed_bytes,
                        through_sequence: ack.through_sequence,
                        sink_primary_term: ack.primary_term,
                        source_ack_replayed,
                    })
                }
            },
        }
    }
}

impl fmt::Debug for RawReplicationPullClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawReplicationPullClient")
            .field("protocol", &self.source.protocol())
            .field("source_transport", &"mTLS")
            .field("local_sink_transport", &"mTLS")
            .field("expected_sink_primary_id", &self.expected_sink_primary_id)
            .finish_non_exhaustive()
    }
}

async fn forward_and_commit<S, C>(
    sink: &S,
    committer: &C,
    keyring: &Ed25519ReceiptKeyring,
    expected_primary_id: &str,
    minimum_primary_term: u64,
    batch: PulledBatch,
) -> Result<CumulativePrimaryAck, PullClientError>
where
    S: DurableSink,
    C: AckCommitter,
{
    // `push_batch` returns only after the hardened receiver has synced both its exact raw spool and
    // receiver-progress WAL. Nothing below can run before those ordered durability boundaries.
    let witness = batch.witness();
    let ack = sink.push_batch(&witness.stream, batch.records).await?;
    validate_sink_ack(
        &ack,
        &witness,
        expected_primary_id,
        minimum_primary_term,
        keyring,
    )?;

    // A failed call leaves Source node's cursor untouched. The caller must retry the same source batch.
    let committed = committer.commit_ack(&ack).await?;
    if committed.stream() != &ack.stream || committed.through_sequence() != ack.through_sequence {
        return Err(pull_error(PullClientErrorKind::CommitMismatch));
    }
    Ok(ack)
}

fn validate_sink_ack(
    ack: &CumulativePrimaryAck,
    batch: &PulledBatchWitness,
    expected_primary_id: &str,
    minimum_primary_term: u64,
    keyring: &Ed25519ReceiptKeyring,
) -> Result<(), PullClientError> {
    // The client authenticates every ACK field and binds its stream/sequence/content digest to the
    // pulled tail. It deliberately does not invent a rolling-chain predecessor after a restart:
    // only the Source node source owns the exact previously committed served-chain witness. CommitAck
    // must recompute and compare that rolling chain against its source WAL before its ACK-WAL fsync
    // and retention decision. This division keeps the client restart-stateless without weakening
    // deletion authority.
    if ack.stream != batch.stream
        || ack.primary_id != expected_primary_id
        || ack.primary_term < minimum_primary_term
        || ack.through_sequence != batch.through_sequence
        || ack.through_content_digest != batch.through_content_digest
        || !ack.disposition.authorizes_replica_gc()
    {
        return Err(pull_error(PullClientErrorKind::InvalidSinkAck));
    }
    let signing_bytes = ack
        .signing_bytes()
        .map_err(|_| pull_error(PullClientErrorKind::InvalidSinkAck))?;
    if !keyring.verify_signature(&ack.signing_key_id, &signing_bytes, &ack.signature) {
        return Err(pull_error(PullClientErrorKind::InvalidSinkAck));
    }
    Ok(())
}

fn finish_pulled_batch(
    builder: BoundedPushBatchBuilder,
) -> Result<Option<PulledBatch>, PullClientError> {
    if builder.is_empty() {
        return Ok(None);
    }
    let compressed_bytes = builder.compressed_bytes();
    let uncompressed_bytes = builder.uncompressed_bytes();
    let batch = builder
        .finish()
        .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))?;
    let (stream, records) = batch.into_parts();
    Ok(Some(PulledBatch {
        stream,
        records,
        compressed_bytes,
        uncompressed_bytes,
    }))
}

fn validate_sync_server_hello(
    hello: wire::SyncServerHello,
    limits: PullLimits,
) -> Result<SyncServerLimits, PullClientError> {
    let max_batch_records = usize::try_from(hello.max_batch_records)
        .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))?;
    let tail_poll_interval = Duration::from_millis(hello.tail_poll_interval_ms);
    let heartbeat_interval = Duration::from_millis(hello.heartbeat_interval_ms);
    if hello.protocol_version != PULL_SYNC_PROTOCOL_VERSION
        || max_batch_records == 0
        || max_batch_records > limits.receiver.max_batch_records
        || hello.max_batch_compressed_bytes == 0
        || hello.max_batch_compressed_bytes > limits.receiver.max_batch_compressed_bytes
        || hello.max_batch_uncompressed_bytes == 0
        || hello.max_batch_uncompressed_bytes > limits.receiver.max_batch_uncompressed_bytes
        || tail_poll_interval.is_zero()
        || heartbeat_interval.is_zero()
        || heartbeat_interval >= limits.stream_idle_timeout
    {
        return Err(pull_error(PullClientErrorKind::WireEncoding));
    }
    Ok(SyncServerLimits {
        max_batch_records,
        max_batch_compressed_bytes: hello.max_batch_compressed_bytes,
        max_batch_uncompressed_bytes: hello.max_batch_uncompressed_bytes,
        tail_poll_interval,
        heartbeat_interval,
    })
}

fn validate_sync_batch_header(
    batch: wire::SyncBatch,
    limits: SyncServerLimits,
) -> Result<SyncBatchHeader, PullClientError> {
    let stream = batch
        .stream
        .ok_or_else(|| pull_error(PullClientErrorKind::WireEncoding))?
        .try_into()
        .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))?;
    let record_count = usize::try_from(batch.record_count)
        .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))?;
    let through_content_digest = ContentDigest(
        batch
            .through_content_digest
            .as_slice()
            .try_into()
            .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))?,
    );
    let last_offset = record_count
        .checked_sub(1)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| pull_error(PullClientErrorKind::WireEncoding))?;
    let expected_through = batch
        .first_sequence
        .checked_add(last_offset)
        .ok_or_else(|| pull_error(PullClientErrorKind::WireEncoding))?;
    if batch.batch_id == 0
        || record_count > limits.max_batch_records
        || batch.compressed_bytes > limits.max_batch_compressed_bytes
        || batch.uncompressed_bytes > limits.max_batch_uncompressed_bytes
        || batch.through_sequence != expected_through
    {
        return Err(pull_error(PullClientErrorKind::WireEncoding));
    }
    Ok(SyncBatchHeader {
        batch_id: batch.batch_id,
        stream,
        first_sequence: batch.first_sequence,
        through_sequence: batch.through_sequence,
        record_count,
        compressed_bytes: batch.compressed_bytes,
        uncompressed_bytes: batch.uncompressed_bytes,
        through_content_digest,
    })
}

fn finish_sync_batch(
    header: &SyncBatchHeader,
    builder: BoundedPushBatchBuilder,
) -> Result<PulledBatch, PullClientError> {
    if builder.len() != header.record_count
        || builder.compressed_bytes() != header.compressed_bytes
        || builder.uncompressed_bytes() != header.uncompressed_bytes
    {
        return Err(pull_error(PullClientErrorKind::WireEncoding));
    }
    let batch = finish_pulled_batch(builder)?
        .ok_or_else(|| pull_error(PullClientErrorKind::WireEncoding))?;
    if batch.stream != header.stream {
        return Err(pull_error(PullClientErrorKind::WireEncoding));
    }
    for (offset, record) in batch.records.iter().enumerate() {
        let offset =
            u64::try_from(offset).map_err(|_| pull_error(PullClientErrorKind::WireEncoding))?;
        let expected_sequence = header
            .first_sequence
            .checked_add(offset)
            .ok_or_else(|| pull_error(PullClientErrorKind::WireEncoding))?;
        if record.offer.record.sequence != expected_sequence {
            return Err(pull_error(PullClientErrorKind::WireEncoding));
        }
    }
    let tail = batch
        .records
        .last()
        .ok_or_else(|| pull_error(PullClientErrorKind::WireEncoding))?;
    if tail.offer.record.sequence != header.through_sequence
        || tail.offer.content_digest != header.through_content_digest
    {
        return Err(pull_error(PullClientErrorKind::WireEncoding));
    }
    Ok(batch)
}

fn validate_sync_ack_committed(
    committed: wire::SyncAckCommitted,
    header: &SyncBatchHeader,
) -> Result<bool, PullClientError> {
    let stream: ReplicationStreamId = committed
        .stream
        .ok_or_else(|| pull_error(PullClientErrorKind::CommitMismatch))?
        .try_into()
        .map_err(|_| pull_error(PullClientErrorKind::CommitMismatch))?;
    if committed.batch_id != header.batch_id
        || stream != header.stream
        || committed.through_sequence != header.through_sequence
    {
        return Err(pull_error(PullClientErrorKind::CommitMismatch));
    }
    Ok(committed.replayed)
}

fn bind_source_identity(
    bound_source_identity: &mut Option<(String, String, String)>,
    stream: &ReplicationStreamId,
) -> Result<(), PullClientError> {
    let source_identity = (
        stream.cluster_id.clone(),
        stream.origin_node_id.clone(),
        stream.source_id.clone(),
    );
    if bound_source_identity
        .as_ref()
        .is_some_and(|bound| bound != &source_identity)
    {
        return Err(pull_error(PullClientErrorKind::WireEncoding));
    }
    bound_source_identity.get_or_insert(source_identity);
    Ok(())
}

fn validate_config(config: &PullClientConfig) -> Result<PullLimits, PullClientError> {
    let source = &config.source;
    if source.connect_timeout_ms == 0
        || source.request_timeout_ms == 0
        || source.stream_idle_timeout_ms == 0
        || source.stream_idle_timeout_ms > source.request_timeout_ms
        || source.batch.max_events == 0
        || source.batch.max_bytes == 0
        || source.batch.max_uncompressed_bytes == 0
        || source.batch.max_compressed_event_bytes == 0
        || source.batch.max_uncompressed_event_bytes == 0
        || source.batch.max_compressed_event_bytes > source.batch.max_bytes
        || source.batch.max_uncompressed_event_bytes > source.batch.max_uncompressed_bytes
        || source.batch.max_events > config.local_sink.batch.max_events
        || source.batch.max_bytes > config.local_sink.batch.max_bytes
        || source.batch.max_uncompressed_bytes > config.local_sink.batch.max_uncompressed_bytes
        || source.batch.max_compressed_event_bytes
            > config.local_sink.batch.max_compressed_event_bytes
        || source.batch.max_uncompressed_event_bytes
            > config.local_sink.batch.max_uncompressed_event_bytes
        || config.local_sink.trusted_receipt_keys.is_empty()
    {
        return Err(pull_error(PullClientErrorKind::InvalidConfiguration));
    }

    let maximum_wire_bytes = source
        .batch
        .max_compressed_event_bytes
        .checked_add(REPLICATION_WIRE_ENVELOPE_RESERVE_BYTES)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .filter(|bytes| *bytes <= MAX_PULL_WIRE_RECORD_BYTES)
        .ok_or_else(|| pull_error(PullClientErrorKind::InvalidConfiguration))?;
    let receiver = RawReceiverLimits {
        max_batch_records: source.batch.max_events,
        max_batch_compressed_bytes: source.batch.max_bytes,
        max_batch_uncompressed_bytes: source.batch.max_uncompressed_bytes,
        max_compressed_record_bytes: source.batch.max_compressed_event_bytes,
        max_uncompressed_record_bytes: source.batch.max_uncompressed_event_bytes,
    };
    BoundedPushBatchBuilder::new(receiver)
        .map_err(|_| pull_error(PullClientErrorKind::InvalidConfiguration))?;

    Ok(PullLimits {
        receiver,
        max_decoding_message_bytes: maximum_wire_bytes,
        connect_timeout: Duration::from_millis(source.connect_timeout_ms),
        request_timeout: Duration::from_millis(source.request_timeout_ms),
        stream_idle_timeout: Duration::from_millis(source.stream_idle_timeout_ms),
    })
}

fn load_trusted_sink_keys(
    trusted_keys: &[TrustedReceiptKeyConfig],
) -> Result<Ed25519ReceiptKeyring, PullClientError> {
    let mut keyring = Ed25519ReceiptKeyring::new();
    for trusted in trusted_keys {
        keyring
            .insert_spki_pem(&trusted.key_id, &trusted.public_key_file)
            .map_err(|_| pull_error(PullClientErrorKind::TrustedSinkKeys))?;
    }
    Ok(keyring)
}

fn request_deadline(duration: Duration) -> Result<Instant, PullClientError> {
    Instant::now()
        .checked_add(duration)
        .ok_or_else(|| pull_error(PullClientErrorKind::InvalidConfiguration))
}

fn source_rpc_error(status: tonic::Status) -> PullClientError {
    pull_error(PullClientErrorKind::SourceRpc(status.code()))
}

fn retryable_transport_error(kind: ReplicationClientErrorKind) -> bool {
    match kind {
        ReplicationClientErrorKind::ConnectTimeout
        | ReplicationClientErrorKind::Connect
        | ReplicationClientErrorKind::RequestTimeout => true,
        ReplicationClientErrorKind::Rpc(code) => retryable_rpc_code(code),
        ReplicationClientErrorKind::InvalidConfiguration
        | ReplicationClientErrorKind::CredentialFile
        | ReplicationClientErrorKind::TlsConfiguration
        | ReplicationClientErrorKind::BatchLimit
        | ReplicationClientErrorKind::WireEncoding
        | ReplicationClientErrorKind::ResponseMismatch => false,
    }
}

fn retryable_rpc_code(code: Code) -> bool {
    matches!(
        code,
        Code::Cancelled
            | Code::Unknown
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::Internal
            | Code::Unavailable
    )
}

fn pull_error(kind: PullClientErrorKind) -> PullClientError {
    PullClientError::new(kind)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use ed25519_dalek::{
        Signer, SigningKey,
        pkcs8::{EncodePublicKey, spki::der::pem::LineEnding},
    };
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::ingest::{
        CommitmentEvidence, ContentDigest, LogicalKey, ObservationId,
        RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1, REPLICATION_PROTOCOL_VERSION, ReceiptDisposition,
        ReplicationOffer, ValidatedPushRecord, compute_content_digest,
    };

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after Unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "blockzilla-pull-client-{label}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir(&path).expect("create test directory");
            Self(path)
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_public_key(directory: &TestDirectory, signing_key: &SigningKey) -> PathBuf {
        let path = directory.path("sink-public.pem");
        let encoded = signing_key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("encode public key");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create public key");
        file.write_all(encoded.as_bytes())
            .expect("write public key");
        file.sync_all().expect("sync public key");
        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("set public key mode");
        path
    }

    fn stream(journal: u8) -> ReplicationStreamId {
        ReplicationStreamId {
            cluster_id: "mainnet-beta".to_owned(),
            origin_node_id: "source-node-relay".to_owned(),
            source_id: "yellowstone-blocks".to_owned(),
            journal_id: [journal; 16],
        }
    }

    fn record(stream: &ReplicationStreamId, sequence: u64, slot: u64) -> RawReplicationRecord {
        let payload = vec![sequence as u8, slot as u8, 7];
        let logical_key = LogicalKey::Block {
            slot,
            blockhash: [slot as u8; 32],
        };
        let content_digest = compute_content_digest(
            &stream.cluster_id,
            &logical_key,
            RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
            &payload,
        );
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
                logical_key,
                content_digest,
                payload_len: payload.len() as u64,
                payload_format_version: RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
                commitment: CommitmentEvidence::Confirmed,
            },
            compressed_payload: payload,
            raw_protobuf_sha256: Sha256::digest([sequence as u8]).into(),
            uncompressed_len: 1,
        }
    }

    fn limits() -> RawReceiverLimits {
        RawReceiverLimits {
            max_batch_records: 4,
            max_batch_compressed_bytes: 1024,
            max_batch_uncompressed_bytes: 1024,
            max_compressed_record_bytes: 256,
            max_uncompressed_record_bytes: 256,
        }
    }

    fn pull_limits() -> PullLimits {
        PullLimits {
            receiver: limits(),
            max_decoding_message_bytes: 320,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(10),
            stream_idle_timeout: Duration::from_secs(2),
        }
    }

    fn sync_server_limits() -> SyncServerLimits {
        SyncServerLimits {
            max_batch_records: limits().max_batch_records,
            max_batch_compressed_bytes: limits().max_batch_compressed_bytes,
            max_batch_uncompressed_bytes: limits().max_batch_uncompressed_bytes,
            tail_poll_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_millis(500),
        }
    }

    fn push_builder(
        stream: &ReplicationStreamId,
        records: &[RawReplicationRecord],
    ) -> BoundedPushBatchBuilder {
        let mut builder = BoundedPushBatchBuilder::new(limits()).expect("valid limits");
        for record in records {
            let wire = wire::PushBatchRequest::try_from(
                ValidatedPushRecord::new(stream.clone(), record.clone())
                    .expect("valid test record"),
            )
            .expect("encode test record");
            builder.push_wire(wire).expect("admit test record");
        }
        builder
    }

    fn batch(records: Vec<RawReplicationRecord>) -> PulledBatch {
        let stream = ReplicationStreamId::from_offer(&records[0].offer);
        PulledBatch {
            compressed_bytes: records.iter().map(|record| record.offer.payload_len).sum(),
            uncompressed_bytes: records.iter().map(|record| record.uncompressed_len).sum(),
            stream,
            records,
        }
    }

    fn signed_ack(batch: &PulledBatch, signing_key: &SigningKey) -> CumulativePrimaryAck {
        let witness = batch.witness();
        let mut ack = CumulativePrimaryAck {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            stream: batch.stream.clone(),
            primary_id: "blockzilla-primary".to_owned(),
            primary_term: 1,
            through_sequence: witness.through_sequence,
            through_content_digest: witness.through_content_digest,
            rolling_chain_digest: ContentDigest([19; 32]),
            disposition: ReceiptDisposition::DurablyStored,
            durable_lsn: witness.through_sequence + 1,
            signing_key_id: "sink-key".to_owned(),
            signature: Vec::new(),
        };
        ack.signature = signing_key
            .sign(&ack.signing_bytes().expect("signable ACK"))
            .to_bytes()
            .to_vec();
        ack
    }

    #[derive(Clone)]
    struct MockSink {
        ack: CumulativePrimaryAck,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl DurableSink for MockSink {
        fn push_batch<'a>(
            &'a self,
            _stream: &'a ReplicationStreamId,
            _records: Vec<RawReplicationRecord>,
        ) -> PullFuture<'a, CumulativePrimaryAck> {
            Box::pin(async move {
                self.events.lock().unwrap().push("sink-fsynced");
                Ok(self.ack.clone())
            })
        }
    }

    #[derive(Clone)]
    struct MockCommitter {
        events: Arc<Mutex<Vec<&'static str>>>,
        fail_once: Arc<AtomicBool>,
    }

    impl AckCommitter for MockCommitter {
        fn commit_ack<'a>(
            &'a self,
            ack: &'a CumulativePrimaryAck,
        ) -> PullFuture<'a, ValidatedCommitAckResponse> {
            Box::pin(async move {
                assert_eq!(
                    self.events.lock().unwrap().last().copied(),
                    Some("sink-fsynced")
                );
                self.events.lock().unwrap().push("remote-commit");
                if self.fail_once.swap(false, Ordering::SeqCst) {
                    return Err(pull_error(PullClientErrorKind::SourceRpc(
                        Code::Unavailable,
                    )));
                }
                ValidatedCommitAckResponse::new(ack.stream.clone(), ack.through_sequence)
                    .map_err(|_| pull_error(PullClientErrorKind::WireEncoding))
            })
        }
    }

    fn keyring(directory: &TestDirectory, signing_key: &SigningKey) -> Ed25519ReceiptKeyring {
        let path = write_public_key(directory, signing_key);
        Ed25519ReceiptKeyring::load_spki_pem("sink-key", path).expect("load sink key")
    }

    #[test]
    fn empty_clean_stream_is_caught_up_without_a_fake_ack() {
        let builder = BoundedPushBatchBuilder::new(limits()).expect("valid limits");
        assert!(finish_pulled_batch(builder).expect("clean EOF").is_none());
    }

    #[test]
    fn retry_policy_separates_transient_transport_from_fail_closed_evidence() {
        for kind in [
            PullClientErrorKind::RequestTimeout,
            PullClientErrorKind::StreamIdleTimeout,
            PullClientErrorKind::SourceRpc(Code::Unavailable),
            PullClientErrorKind::SourceRpc(Code::ResourceExhausted),
            PullClientErrorKind::SinkTransport(ReplicationClientErrorKind::Connect),
        ] {
            assert!(PullClientError::new(kind).is_retryable(), "{kind:?}");
        }
        for kind in [
            PullClientErrorKind::InvalidConfiguration,
            PullClientErrorKind::TrustedSinkKeys,
            PullClientErrorKind::WireEncoding,
            PullClientErrorKind::InvalidSinkAck,
            PullClientErrorKind::CommitMismatch,
            PullClientErrorKind::SourceRpc(Code::Unauthenticated),
            PullClientErrorKind::SourceRpc(Code::PermissionDenied),
            PullClientErrorKind::SinkTransport(ReplicationClientErrorKind::TlsConfiguration),
        ] {
            assert!(!PullClientError::new(kind).is_retryable(), "{kind:?}");
        }
    }

    #[test]
    fn pull_batch_config_rejects_unknown_fields() {
        let json = r#"{
            "max_events": 4,
            "max_bytes": 1024,
            "max_uncompressed_bytes": 2048,
            "max_compressed_event_bytes": 512,
            "max_uncompressed_event_bytes": 1024,
            "client_cursor": 99
        }"#;
        assert!(serde_json::from_str::<PullClientBatchLimits>(json).is_err());
    }

    #[test]
    fn pull_protocol_defaults_to_v1_and_unknown_versions_are_rejected() {
        let json = r#"{
            "endpoint": "https://127.0.0.1:10443",
            "tls": {},
            "connect_timeout_ms": 1000,
            "request_timeout_ms": 10000,
            "stream_idle_timeout_ms": 2000,
            "batch": {
                "max_events": 4,
                "max_bytes": 1024,
                "max_uncompressed_bytes": 1024,
                "max_compressed_event_bytes": 256,
                "max_uncompressed_event_bytes": 256
            }
        }"#;
        let source: PullRemoteSourceConfig = serde_json::from_str(json).expect("v1 default");
        assert_eq!(source.protocol, PullClientProtocol::V1);
        assert!(serde_json::from_str::<PullClientProtocol>(r#""v3""#).is_err());
    }

    #[test]
    fn sync_hello_must_fit_local_limits_and_beat_the_idle_deadline() {
        let hello = wire::SyncServerHello {
            protocol_version: PULL_SYNC_PROTOCOL_VERSION,
            max_batch_records: 4,
            max_batch_compressed_bytes: 1024,
            max_batch_uncompressed_bytes: 1024,
            tail_poll_interval_ms: 100,
            heartbeat_interval_ms: 500,
        };
        let validated =
            validate_sync_server_hello(hello.clone(), pull_limits()).expect("valid hello");
        assert_eq!(validated, sync_server_limits());

        let mut wrong_version = hello.clone();
        wrong_version.protocol_version = 1;
        assert!(validate_sync_server_hello(wrong_version, pull_limits()).is_err());

        let mut oversized = hello.clone();
        oversized.max_batch_records = 5;
        assert!(validate_sync_server_hello(oversized, pull_limits()).is_err());

        let mut stale_heartbeat = hello;
        stale_heartbeat.heartbeat_interval_ms = 2_000;
        assert!(validate_sync_server_hello(stale_heartbeat, pull_limits()).is_err());
    }

    #[test]
    fn sync_batch_header_and_records_are_bound_to_one_contiguous_witness() {
        let stream = stream(11);
        let records = vec![record(&stream, 0, 400), record(&stream, 1, 401)];
        let tail = records.last().expect("tail");
        let wire_header = wire::SyncBatch {
            batch_id: 7,
            stream: Some(wire::StreamId::try_from(&stream).expect("encode stream")),
            first_sequence: 0,
            through_sequence: 1,
            record_count: 2,
            compressed_bytes: records.iter().map(|record| record.offer.payload_len).sum(),
            uncompressed_bytes: records.iter().map(|record| record.uncompressed_len).sum(),
            through_content_digest: tail.offer.content_digest.0.to_vec(),
        };
        let header = validate_sync_batch_header(wire_header, sync_server_limits())
            .expect("valid batch header");
        let completed = finish_sync_batch(&header, push_builder(&stream, &records))
            .expect("header matches exact records");
        assert_eq!(completed.records, records);

        let non_contiguous = vec![record(&stream, 0, 400), record(&stream, 2, 402)];
        assert!(finish_sync_batch(&header, push_builder(&stream, &non_contiguous)).is_err());

        let mut wrong_total = header.clone();
        wrong_total.compressed_bytes += 1;
        assert!(finish_sync_batch(&wrong_total, push_builder(&stream, &records)).is_err());
    }

    #[test]
    fn sync_commit_confirmation_is_strictly_correlated_and_surfaces_replay() {
        let stream = stream(12);
        let record = record(&stream, 9, 409);
        let header = SyncBatchHeader {
            batch_id: 3,
            stream: stream.clone(),
            first_sequence: 9,
            through_sequence: 9,
            record_count: 1,
            compressed_bytes: record.offer.payload_len,
            uncompressed_bytes: record.uncompressed_len,
            through_content_digest: record.offer.content_digest,
        };
        let committed = wire::SyncAckCommitted {
            batch_id: 3,
            stream: Some(wire::StreamId::try_from(&stream).expect("encode stream")),
            through_sequence: 9,
            replayed: true,
        };
        assert!(
            validate_sync_ack_committed(committed.clone(), &header).expect("correlated replay")
        );

        let mut wrong_batch = committed;
        wrong_batch.batch_id = 4;
        assert!(matches!(
            validate_sync_ack_committed(wrong_batch, &header),
            Err(error) if error.kind() == PullClientErrorKind::CommitMismatch
        ));
    }

    #[tokio::test]
    async fn sync_batch_record_trickle_cannot_extend_the_absolute_request_deadline() {
        let stream = stream(13);
        let records = vec![record(&stream, 20, 420), record(&stream, 21, 421)];
        let tail = records.last().expect("tail");
        let header = wire::SyncBatch {
            batch_id: 5,
            stream: Some(wire::StreamId::try_from(&stream).expect("encode stream")),
            first_sequence: 20,
            through_sequence: 21,
            record_count: 2,
            compressed_bytes: records.iter().map(|record| record.offer.payload_len).sum(),
            uncompressed_bytes: records.iter().map(|record| record.uncompressed_len).sum(),
            through_content_digest: tail.offer.content_digest.0.to_vec(),
        };
        let frames = records
            .into_iter()
            .map(|record| {
                let record = wire::PushBatchRequest::try_from(
                    ValidatedPushRecord::new(stream.clone(), record).expect("valid record"),
                )
                .expect("encode record");
                wire::SyncServerMessage {
                    frame: Some(wire::sync_server_message::Frame::Record(record)),
                }
            })
            .collect::<Vec<_>>()
            .into_iter();
        let mut inbound = Box::pin(stream::unfold(frames, |mut frames| async move {
            let frame = frames.next()?;
            tokio::time::sleep(Duration::from_millis(20)).await;
            Some((Ok(frame), frames))
        }));
        let mut client_limits = pull_limits();
        client_limits.request_timeout = Duration::from_millis(30);
        client_limits.stream_idle_timeout = Duration::from_millis(200);

        let result =
            receive_sync_batch(&mut inbound, client_limits, sync_server_limits(), header).await;
        assert!(matches!(
            result,
            Err(error) if error.kind() == PullClientErrorKind::RequestTimeout
        ));
    }

    #[tokio::test]
    async fn sync_ack_heartbeats_cannot_extend_commit_but_head_wait_has_no_total_deadline() {
        let stream = stream(14);
        let record = record(&stream, 30, 430);
        let header = SyncBatchHeader {
            batch_id: 6,
            stream,
            first_sequence: 30,
            through_sequence: 30,
            record_count: 1,
            compressed_bytes: record.offer.payload_len,
            uncompressed_bytes: record.uncompressed_len,
            through_content_digest: record.offer.content_digest,
        };
        let heartbeats = 1u64..;
        let mut inbound = Box::pin(stream::unfold(heartbeats, |mut heartbeats| async move {
            let heartbeat_id = heartbeats.next()?;
            tokio::time::sleep(Duration::from_millis(5)).await;
            Some((
                Ok(wire::SyncServerMessage {
                    frame: Some(wire::sync_server_message::Frame::Heartbeat(
                        wire::SyncHeartbeat { heartbeat_id },
                    )),
                }),
                heartbeats,
            ))
        }));
        let deadline = request_deadline(Duration::from_millis(30)).expect("deadline");
        let result = wait_for_sync_ack_committed(
            &mut inbound,
            Duration::from_millis(200),
            deadline,
            &header,
        )
        .await;
        assert!(matches!(
            result,
            Err(error) if error.kind() == PullClientErrorKind::RequestTimeout
        ));

        let one_heartbeat = stream::once(async {
            tokio::time::sleep(Duration::from_millis(35)).await;
            Ok(wire::SyncServerMessage {
                frame: Some(wire::sync_server_message::Frame::Heartbeat(
                    wire::SyncHeartbeat { heartbeat_id: 99 },
                )),
            })
        });
        let mut head = Box::pin(one_heartbeat);
        assert!(matches!(
            receive_sync_frame(&mut head, Duration::from_millis(200), None).await,
            Ok(wire::sync_server_message::Frame::Heartbeat(heartbeat))
                if heartbeat.heartbeat_id == 99
        ));
    }

    #[test]
    fn mixed_stream_batch_is_rejected_incrementally() {
        let first_stream = stream(1);
        let second_stream = stream(2);
        let mut builder = BoundedPushBatchBuilder::new(limits()).expect("valid limits");
        let first = wire::PushBatchRequest::try_from(
            ValidatedPushRecord::new(first_stream.clone(), record(&first_stream, 0, 100))
                .expect("valid first record"),
        )
        .expect("encode first record");
        let second = wire::PushBatchRequest::try_from(
            ValidatedPushRecord::new(second_stream.clone(), record(&second_stream, 1, 101))
                .expect("valid second record"),
        )
        .expect("encode second record");
        builder.push_wire(first).expect("admit first stream");
        assert!(builder.push_wire(second).is_err());
    }

    #[tokio::test]
    async fn sink_durability_and_signature_precede_remote_commit_and_retry_is_safe() {
        let directory = TestDirectory::new("ordering-retry");
        let signing_key = SigningKey::from_bytes(&[42; 32]);
        let keyring = keyring(&directory, &signing_key);
        let stream = stream(3);
        let batch = batch(vec![record(&stream, 0, 100), record(&stream, 1, 101)]);
        let events = Arc::new(Mutex::new(Vec::new()));
        let sink = MockSink {
            ack: signed_ack(&batch, &signing_key),
            events: Arc::clone(&events),
        };
        let committer = MockCommitter {
            events: Arc::clone(&events),
            fail_once: Arc::new(AtomicBool::new(true)),
        };

        let first = forward_and_commit(
            &sink,
            &committer,
            &keyring,
            "blockzilla-primary",
            0,
            batch.clone(),
        )
        .await;
        assert!(matches!(
            first,
            Err(error)
                if error.kind() == PullClientErrorKind::SourceRpc(Code::Unavailable)
        ));
        assert_eq!(
            *events.lock().unwrap(),
            vec!["sink-fsynced", "remote-commit"]
        );

        let second =
            forward_and_commit(&sink, &committer, &keyring, "blockzilla-primary", 0, batch)
                .await
                .expect("retry exact uncommitted batch");
        assert_eq!(second.through_sequence, 1);
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                "sink-fsynced",
                "remote-commit",
                "sink-fsynced",
                "remote-commit"
            ]
        );
    }

    #[tokio::test]
    async fn mismatched_or_unsigned_sink_ack_is_never_forwarded() {
        let directory = TestDirectory::new("bad-ack");
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let keyring = keyring(&directory, &signing_key);
        let stream = stream(4);
        let batch = batch(vec![record(&stream, 0, 200)]);
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut ack = signed_ack(&batch, &signing_key);
        ack.through_sequence = 9;
        let sink = MockSink {
            ack,
            events: Arc::clone(&events),
        };
        let committer = MockCommitter {
            events: Arc::clone(&events),
            fail_once: Arc::new(AtomicBool::new(false)),
        };
        let result =
            forward_and_commit(&sink, &committer, &keyring, "blockzilla-primary", 0, batch).await;
        assert!(matches!(
            result,
            Err(error) if error.kind() == PullClientErrorKind::InvalidSinkAck
        ));
        assert_eq!(*events.lock().unwrap(), vec!["sink-fsynced"]);
    }
}
