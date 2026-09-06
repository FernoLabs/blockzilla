//! Real localhost HTTP/2 coverage for the protocol-v2 `Sync` transport.
//!
//! The production runtime's mandatory mTLS configuration is intentionally not reconstructed here:
//! this crate has no certificate-generation test helper or certificate fixtures. These tests bind
//! a real TCP listener and exercise Tonic's generated bidirectional client/server framing around
//! the same `run_sync_session` driver and stop-and-wait source core used by production.

use super::*;

use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
};

use futures::Stream;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tonic::{
    Request, Response, Status,
    transport::{Channel, Endpoint, Server},
};

use crate::ingest::{
    CommitmentEvidence, LogicalKey, ObservationId, RAW_GRPC_ZSTD_PROTOBUF_UPDATE_V1,
    REPLICATION_PROTOCOL_VERSION, ReceiptDisposition, ReplicationOffer, compute_content_digest,
};

const TEST_TAIL_POLL: Duration = Duration::from_millis(5);
const TEST_HEARTBEAT: Duration = Duration::from_millis(20);
const TEST_ACK_TIMEOUT: Duration = Duration::from_secs(1);
const TEST_GC_INTERVAL: Duration = Duration::from_secs(1);

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
        let ack = verified.ack().clone();
        self.latest.insert(ack.stream.clone(), ack);
        Ok(())
    }

    fn latest_ack(&self, stream: &ReplicationStreamId) -> Option<CumulativePrimaryAck> {
        self.latest.get(stream).cloned()
    }

    fn gc_one(&mut self) -> Result<GrpcRawLocalGcOutcome, PullSourceError> {
        Ok(GrpcRawLocalGcOutcome::NothingToRetire)
    }
}

type TestSource = PullSourceCore<FakeBackend, AcceptVerifier>;

impl SyncPullSource for TestSource {
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

struct AllowAll;

impl SyncStreamAuthorizer for AllowAll {
    fn permits_sync_stream(&self, _stream: &ReplicationStreamId) -> bool {
        true
    }
}

#[derive(Clone)]
struct NetworkSyncService {
    source: Arc<Mutex<TestSource>>,
    admission: Arc<Semaphore>,
}

#[tonic::async_trait]
impl wire::raw_replication_pull_server::RawReplicationPull for NetworkSyncService {
    type PullBatchStream =
        Pin<Box<dyn Stream<Item = Result<wire::PushBatchRequest, Status>> + Send + Sync + 'static>>;
    type SyncStream = RawReplicationSyncResponseStream;

    async fn pull_batch(
        &self,
        _request: Request<wire::PullBatchRequest>,
    ) -> Result<Response<Self::PullBatchStream>, Status> {
        Err(Status::unimplemented("network test exposes only Sync"))
    }

    async fn commit_ack(
        &self,
        _request: Request<wire::CumulativePrimaryAck>,
    ) -> Result<Response<wire::CommitAckResponse>, Status> {
        Err(Status::unimplemented("network test exposes only Sync"))
    }

    async fn sync(
        &self,
        request: Request<tonic::Streaming<wire::SyncClientMessage>>,
    ) -> Result<Response<Self::SyncStream>, Status> {
        let permit = acquire_replication_admission(&self.admission)?;
        let mut inbound = request.into_inner();
        let first = timeout(Duration::from_secs(1), inbound.next())
            .await
            .map_err(|_| Status::deadline_exceeded("test Sync Hello timed out"))?;
        let first = match first {
            Some(Ok(message)) => message,
            Some(Err(status)) => return Err(status),
            None => return Err(Status::failed_precondition("Sync closed before Hello")),
        };
        validate_sync_client_hello(first)?;

        let (responses, receiver) = mpsc::channel(SYNC_RESPONSE_CHANNEL_CAPACITY);
        let task_responses = responses.clone();
        let source = Arc::clone(&self.source);
        tokio::spawn(async move {
            let _permit = permit;
            let result = run_sync_session(
                source,
                AllowAll,
                inbound,
                task_responses,
                test_limits(),
                TEST_TAIL_POLL,
                TEST_HEARTBEAT,
                TEST_ACK_TIMEOUT,
                TEST_GC_INTERVAL,
                None,
            )
            .await;
            if let Err(status) = result {
                let _ = responses.send(Err(status)).await;
            }
        });

        Ok(Response::new(RawReplicationSyncResponseStream { receiver }))
    }
}

struct ClientOutbound {
    receiver: mpsc::Receiver<wire::SyncClientMessage>,
}

impl Stream for ClientOutbound {
    type Item = wire::SyncClientMessage;

    fn poll_next(self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().receiver.poll_recv(context)
    }
}

struct TestServer {
    channel: Channel,
    admission: Arc<Semaphore>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl TestServer {
    async fn start(source: Arc<Mutex<TestSource>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind localhost Sync test server");
        let address = listener.local_addr().expect("test server address");
        let incoming = futures::stream::try_unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await?;
            Ok::<_, std::io::Error>(Some((stream, listener)))
        });
        let admission = Arc::new(Semaphore::new(1));
        let service = NetworkSyncService {
            source,
            admission: Arc::clone(&admission),
        };
        let (shutdown, shutdown_receiver) = oneshot::channel();
        let task = tokio::spawn(
            Server::builder()
                .add_service(
                    wire::raw_replication_pull_server::RawReplicationPullServer::new(service),
                )
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                }),
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .expect("localhost endpoint")
            .connect()
            .await
            .expect("connect real Tonic client");
        Self {
            channel,
            admission,
            shutdown: Some(shutdown),
            task,
        }
    }

    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        timeout(Duration::from_secs(2), self.task)
            .await
            .expect("test server shutdown timeout")
            .expect("test server task joins")
            .expect("test server exits cleanly");
    }
}

fn stream_id() -> ReplicationStreamId {
    ReplicationStreamId {
        cluster_id: "solana-mainnet".into(),
        origin_node_id: "source-node-1".into(),
        source_id: "triton-blocks".into(),
        journal_id: [3u8; 16],
    }
}

fn record(sequence: u64) -> RawReplicationRecord {
    let stream = stream_id();
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

fn source_with_batches(batches: Vec<Vec<RawReplicationRecord>>) -> Arc<Mutex<TestSource>> {
    let mut backend = FakeBackend::default();
    for records in batches {
        backend.queued.push_back((stream_id(), records));
    }
    Arc::new(Mutex::new(PullSourceCore {
        backend,
        verifier: AcceptVerifier,
        expected_primary_id: "blockzilla-primary".into(),
        limits: test_limits(),
        pending: None,
        gc_enabled: false,
    }))
}

fn ack_for(source: &TestSource) -> CumulativePrimaryAck {
    let pending = source.pending.as_ref().expect("pending network batch");
    CumulativePrimaryAck {
        protocol_version: REPLICATION_PROTOCOL_VERSION,
        stream: pending.stream.clone(),
        primary_id: source.expected_primary_id.clone(),
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

fn hello() -> wire::SyncClientMessage {
    wire::SyncClientMessage {
        frame: Some(wire::sync_client_message::Frame::Hello(
            wire::SyncClientHello {
                protocol_version: RAW_REPLICATION_SYNC_PROTOCOL_VERSION,
            },
        )),
    }
}

fn ack_message(batch_id: u64, ack: &CumulativePrimaryAck) -> wire::SyncClientMessage {
    wire::SyncClientMessage {
        frame: Some(wire::sync_client_message::Frame::Ack(wire::SyncClientAck {
            batch_id,
            ack: Some(wire::CumulativePrimaryAck::try_from(ack).expect("encode network ACK")),
        })),
    }
}

async fn open_session(
    channel: Channel,
) -> (
    mpsc::Sender<wire::SyncClientMessage>,
    tonic::Streaming<wire::SyncServerMessage>,
) {
    let (sender, receiver) = mpsc::channel(8);
    sender.send(hello()).await.expect("queue Sync Hello");
    let mut client = wire::raw_replication_pull_client::RawReplicationPullClient::new(channel);
    let response = client
        .sync(ClientOutbound { receiver })
        .await
        .expect("open real Tonic Sync RPC");
    (sender, response.into_inner())
}

async fn next_frame(
    stream: &mut tonic::Streaming<wire::SyncServerMessage>,
) -> wire::sync_server_message::Frame {
    timeout(Duration::from_secs(2), stream.message())
        .await
        .expect("network Sync frame timeout")
        .expect("valid network Sync frame")
        .expect("network Sync stream remains open")
        .frame
        .expect("non-empty network Sync frame")
}

async fn wait_for_admission_release(admission: &Semaphore) {
    timeout(Duration::from_secs(2), async {
        while admission.available_permits() == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cancelled network Sync releases admission");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tonic_sync_network_nominal_batch_ack_and_commit() {
    let source = source_with_batches(vec![vec![record(0)]]);
    let server = TestServer::start(Arc::clone(&source)).await;
    let (sender, mut responses) = open_session(server.channel.clone()).await;

    let wire::sync_server_message::Frame::Hello(server_hello) = next_frame(&mut responses).await
    else {
        panic!("expected server Hello");
    };
    assert_eq!(
        server_hello.protocol_version,
        RAW_REPLICATION_SYNC_PROTOCOL_VERSION
    );
    let wire::sync_server_message::Frame::Batch(batch) = next_frame(&mut responses).await else {
        panic!("expected batch header");
    };
    assert_eq!(batch.batch_id, 1);
    assert_eq!(batch.first_sequence, 0);
    assert_eq!(batch.through_sequence, 0);
    assert_eq!(batch.record_count, 1);
    assert!(matches!(
        next_frame(&mut responses).await,
        wire::sync_server_message::Frame::Record(_)
    ));

    let ack = ack_for(&source.lock().expect("network source"));
    sender
        .send(ack_message(batch.batch_id, &ack))
        .await
        .expect("send signed ACK over same HTTP/2 stream");
    let wire::sync_server_message::Frame::AckCommitted(committed) =
        next_frame(&mut responses).await
    else {
        panic!("expected durable ACK confirmation");
    };
    assert_eq!(committed.batch_id, batch.batch_id);
    assert_eq!(committed.through_sequence, 0);
    assert!(!committed.replayed);
    {
        let source = source.lock().expect("network source");
        assert_eq!(source.backend.commit_calls, 1);
        assert!(source.pending.is_none());
    }

    drop(sender);
    drop(responses);
    wait_for_admission_release(&server.admission).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tonic_sync_network_disconnect_before_ack_replays_on_reconnect() {
    let source = source_with_batches(vec![vec![record(0)]]);
    let server = TestServer::start(Arc::clone(&source)).await;
    let (first_sender, mut first_responses) = open_session(server.channel.clone()).await;

    assert!(matches!(
        next_frame(&mut first_responses).await,
        wire::sync_server_message::Frame::Hello(_)
    ));
    let wire::sync_server_message::Frame::Batch(first_batch) =
        next_frame(&mut first_responses).await
    else {
        panic!("expected first batch header");
    };
    let wire::sync_server_message::Frame::Record(first_record) =
        next_frame(&mut first_responses).await
    else {
        panic!("expected first record");
    };
    drop(first_sender);
    drop(first_responses);
    wait_for_admission_release(&server.admission).await;
    {
        let source = source.lock().expect("network source");
        assert_eq!(source.backend.commit_calls, 0);
        assert!(source.pending.is_some());
    }

    let (second_sender, mut second_responses) = open_session(server.channel.clone()).await;
    assert!(matches!(
        next_frame(&mut second_responses).await,
        wire::sync_server_message::Frame::Hello(_)
    ));
    let wire::sync_server_message::Frame::Batch(replayed_batch) =
        next_frame(&mut second_responses).await
    else {
        panic!("expected replayed batch header");
    };
    let wire::sync_server_message::Frame::Record(replayed_record) =
        next_frame(&mut second_responses).await
    else {
        panic!("expected replayed record");
    };
    assert_eq!(first_batch.first_sequence, replayed_batch.first_sequence);
    assert_eq!(
        first_batch.through_sequence,
        replayed_batch.through_sequence
    );
    assert_eq!(first_record, replayed_record);
    // Correlation IDs are session-local and restart at one; replay authority is the signed ACK.
    assert_eq!(replayed_batch.batch_id, 1);

    let ack = ack_for(&source.lock().expect("network source"));
    second_sender
        .send(ack_message(replayed_batch.batch_id, &ack))
        .await
        .expect("ACK replayed batch");
    let wire::sync_server_message::Frame::AckCommitted(committed) =
        next_frame(&mut second_responses).await
    else {
        panic!("expected replay commit confirmation");
    };
    assert_eq!(committed.through_sequence, 0);
    assert_eq!(
        source.lock().expect("network source").backend.commit_calls,
        1
    );

    drop(second_sender);
    drop(second_responses);
    wait_for_admission_release(&server.admission).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tonic_sync_network_heartbeat_then_new_tail_data_without_reconnect() {
    let source = source_with_batches(Vec::new());
    let server = TestServer::start(Arc::clone(&source)).await;
    let (sender, mut responses) = open_session(server.channel.clone()).await;

    assert!(matches!(
        next_frame(&mut responses).await,
        wire::sync_server_message::Frame::Hello(_)
    ));
    let wire::sync_server_message::Frame::Heartbeat(heartbeat) = next_frame(&mut responses).await
    else {
        panic!("expected head heartbeat");
    };
    assert_eq!(heartbeat.heartbeat_id, 1);
    {
        let mut source = source.lock().expect("network source");
        source
            .backend
            .queued
            .push_back((stream_id(), vec![record(0)]));
        assert_eq!(source.backend.commit_calls, 0);
    }

    let wire::sync_server_message::Frame::Batch(batch) = next_frame(&mut responses).await else {
        panic!("expected newly appended tail batch");
    };
    assert_eq!(batch.first_sequence, 0);
    assert!(matches!(
        next_frame(&mut responses).await,
        wire::sync_server_message::Frame::Record(_)
    ));
    let ack = ack_for(&source.lock().expect("network source"));
    sender
        .send(ack_message(batch.batch_id, &ack))
        .await
        .expect("ACK new tail batch");
    assert!(matches!(
        next_frame(&mut responses).await,
        wire::sync_server_message::Frame::AckCommitted(_)
    ));
    assert_eq!(
        source.lock().expect("network source").backend.commit_calls,
        1
    );

    drop(sender);
    drop(responses);
    wait_for_admission_release(&server.admission).await;
    server.stop().await;
}
