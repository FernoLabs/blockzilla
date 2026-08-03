//! Minimal durable Solana shred UDP source.
//!
//! The adapter preserves each accepted UDP datagram in an independently compressed, lossless
//! ingress frame. Transport duplicates remain distinct observations while sharing a logical shred
//! key. The compressed representation is what is replicated to a remote durable spool.

use std::{
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use socket2::{
    Domain, InterfaceIndexOrAddress, Protocol as SocketProtocol, SockAddr, SockRef, Socket, Type,
};
use tokio::{
    net::UdpSocket,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
};

use super::{
    IngestConfig, IngestRoleConfig, IngressRecordMeta, LogicalKey, ObservationId,
    SourceInputConfig, SpoolFullPolicy, SpoolJournalIdentity, SpoolOptions, SpoolWriter,
};
pub use blockzilla_shred_codec::{
    ParsedShredHeader, RAW_SOLANA_SHRED_V1, ZSTD_SOLANA_SHRED_V1, decode_stored_shred,
    parse_shred_header,
};

const MAX_UDP_DATAGRAM_BYTES: usize = blockzilla_shred_codec::MAX_UDP_DATAGRAM_BYTES;
const UDP_RECEIVE_BUFFER_BYTES: usize = 64 * 1024 * 1024;
const SOCKET_DRAIN_BURST_MAX_RECORDS: usize = 1_024;
const DURABLE_BATCH_MAX_RECORDS: usize = 512;
const DURABLE_BATCH_COLLECTION_WINDOW: Duration = Duration::from_millis(5);
const QUOTA_ENTRY_MIN_BYTES: u64 = 4_096;
const STATUS_SCHEMA_VERSION: u32 = 1;
const STATUS_INTERVAL: Duration = Duration::from_secs(5);
const RECEIVING_FRESHNESS: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct ShredUdpRecordConfig {
    pub ingest: IngestConfig,
    pub source_id: String,
    pub journal_id: [u8; 16],
    pub status_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ShredRecorderState {
    Waiting,
    Receiving,
    Stalled,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ShredRecorderStatus {
    schema_version: u32,
    updated_unix_secs: u64,
    started_unix_secs: u64,
    state: ShredRecorderState,
    accepted_total: u64,
    invalid_total: u64,
    bytes_total: u64,
    durable_through_sequence: Option<u64>,
    latest_slot: Option<u64>,
    shred_version: Option<u16>,
    last_durable_unix_secs: Option<u64>,
    spool_bytes: u64,
    spool_max_bytes: u64,
    filesystem_free_bytes: u64,
    filesystem_total_bytes: u64,
    reserve_free_bytes: u64,
    udp_received_total: u64,
    udp_received_bytes_total: u64,
    ingest_queue_depth_events: u64,
    ingest_queue_depth_bytes: u64,
    ingest_queue_high_water_events: u64,
    ingest_queue_high_water_bytes: u64,
    ingest_queue_capacity_events: usize,
    ingest_queue_capacity_bytes: u64,
    ingest_queue_backpressure_events_total: u64,
    ingest_queue_backpressure_micros_total: u64,
    ingest_queue_backpressured: bool,
    socket_rxq_overflow_supported: bool,
    socket_rxq_overflow_total: Option<u64>,
}

#[derive(Debug, Default)]
struct UdpIngestMetrics {
    received_total: AtomicU64,
    received_bytes_total: AtomicU64,
    queue_depth_events: AtomicU64,
    queue_depth_bytes: AtomicU64,
    queue_high_water_events: AtomicU64,
    queue_high_water_bytes: AtomicU64,
    queue_backpressure_events_total: AtomicU64,
    queue_backpressure_micros_total: AtomicU64,
    queue_backpressured: AtomicU64,
    socket_rxq_overflow_total: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UdpIngestMetricsSnapshot {
    received_total: u64,
    received_bytes_total: u64,
    queue_depth_events: u64,
    queue_depth_bytes: u64,
    queue_high_water_events: u64,
    queue_high_water_bytes: u64,
    queue_backpressure_events_total: u64,
    queue_backpressure_micros_total: u64,
    queue_backpressured: bool,
    socket_rxq_overflow_total: u64,
}

impl UdpIngestMetrics {
    fn record_received(&self, bytes: usize) {
        self.received_total.fetch_add(1, Ordering::Relaxed);
        self.received_bytes_total
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn record_enqueued(&self, queued_bytes: usize) {
        let events = self.queue_depth_events.fetch_add(1, Ordering::Relaxed) + 1;
        let current_bytes = self
            .queue_depth_bytes
            .fetch_add(queued_bytes as u64, Ordering::Relaxed)
            .saturating_add(queued_bytes as u64);
        self.queue_high_water_events
            .fetch_max(events, Ordering::Relaxed);
        self.queue_high_water_bytes
            .fetch_max(current_bytes, Ordering::Relaxed);
    }

    fn record_dequeued(&self, bytes: usize) {
        let previous_events = self.queue_depth_events.fetch_sub(1, Ordering::Relaxed);
        let previous_bytes = self
            .queue_depth_bytes
            .fetch_sub(bytes as u64, Ordering::Relaxed);
        debug_assert!(previous_events >= 1);
        debug_assert!(previous_bytes >= bytes as u64);
    }

    fn begin_backpressure(&self) -> Instant {
        self.queue_backpressure_events_total
            .fetch_add(1, Ordering::Relaxed);
        self.queue_backpressured.store(1, Ordering::Relaxed);
        Instant::now()
    }

    fn end_backpressure(&self, started: Instant) {
        let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.queue_backpressure_micros_total
            .fetch_add(elapsed_micros, Ordering::Relaxed);
        self.queue_backpressured.store(0, Ordering::Relaxed);
    }

    fn record_socket_overflow(&self, dropped: u64) {
        self.socket_rxq_overflow_total
            .fetch_add(dropped, Ordering::Relaxed);
    }

    fn snapshot(&self) -> UdpIngestMetricsSnapshot {
        UdpIngestMetricsSnapshot {
            received_total: self.received_total.load(Ordering::Relaxed),
            received_bytes_total: self.received_bytes_total.load(Ordering::Relaxed),
            queue_depth_events: self.queue_depth_events.load(Ordering::Relaxed),
            queue_depth_bytes: self.queue_depth_bytes.load(Ordering::Relaxed),
            queue_high_water_events: self.queue_high_water_events.load(Ordering::Relaxed),
            queue_high_water_bytes: self.queue_high_water_bytes.load(Ordering::Relaxed),
            queue_backpressure_events_total: self
                .queue_backpressure_events_total
                .load(Ordering::Relaxed),
            queue_backpressure_micros_total: self
                .queue_backpressure_micros_total
                .load(Ordering::Relaxed),
            queue_backpressured: self.queue_backpressured.load(Ordering::Relaxed) != 0,
            socket_rxq_overflow_total: self.socket_rxq_overflow_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct QueuedDatagram {
    payload: Vec<u8>,
    original_length: usize,
    oversized: bool,
    _event_permit: OwnedSemaphorePermit,
    _byte_permit: Option<OwnedSemaphorePermit>,
}

impl QueuedDatagram {
    fn queue_bytes(&self) -> usize {
        self.payload.len()
    }
}

#[derive(Debug, Clone, Copy)]
struct ReceivedDatagram {
    length: usize,
    socket_rxq_overflow: Option<u32>,
}

pub async fn record_shred_udp(config: ShredUdpRecordConfig) -> Result<()> {
    ensure!(
        matches!(config.ingest.spool.full_policy, SpoolFullPolicy::FailClosed),
        "record-shred-udp currently requires spool.full_policy=fail_closed"
    );
    ensure!(
        config
            .ingest
            .sources
            .iter()
            .filter(|source| source.enabled)
            .count()
            == 1,
        "record-shred-udp currently requires exactly one enabled source per spool root"
    );
    if let Some(status_file) = config.status_file.as_deref() {
        ensure!(
            status_file.is_absolute() && status_file != Path::new("/"),
            "record-shred-udp status file must be an absolute non-root path"
        );
        ensure!(
            !status_file.starts_with(&config.ingest.spool.root),
            "record-shred-udp status file must live outside the quota-accounted spool root"
        );
    }
    let source = config
        .ingest
        .sources
        .iter()
        .find(|source| source.id == config.source_id)
        .with_context(|| format!("shred UDP source {:?} is not configured", config.source_id))?;
    ensure!(
        source.enabled,
        "shred UDP source {:?} is disabled",
        source.id
    );
    let SourceInputConfig::ShredUdp {
        bind,
        multicast_group,
        interface,
        auth,
    } = &source.input
    else {
        anyhow::bail!("source {:?} is not a shred_udp input", source.id);
    };
    ensure!(
        auth.is_none(),
        "raw shred UDP recording currently requires auth=null; authenticated envelopes are not yet implemented"
    );
    ensure!(
        source.queue.max_events > 0 && source.queue.max_events <= Semaphore::MAX_PERMITS,
        "shred UDP queue event capacity is outside the async channel limit"
    );
    ensure!(
        source.queue.max_event_bytes > 0 && source.queue.max_event_bytes <= source.queue.max_bytes,
        "shred UDP queue byte limits are invalid"
    );

    let origin_node_id = match &config.ingest.role {
        IngestRoleConfig::Primary { node_id, .. } | IngestRoleConfig::Replica { node_id, .. } => {
            node_id.clone()
        }
    };
    let identity = SpoolJournalIdentity {
        cluster_id: config.ingest.cluster_id.clone(),
        origin_node_id: origin_node_id.clone(),
        source_id: source.id.clone(),
        journal_id: config.journal_id,
    };
    let options = SpoolOptions {
        segment_target_bytes: config.ingest.spool.segment_bytes,
        max_record_bytes: source.queue.max_event_bytes,
    };
    let mut spool = SpoolWriter::open(&config.ingest.spool.root, identity, options)
        .context("open shred UDP ingress spool")?;
    let mut spool_bytes = spool_root_bytes(&config.ingest.spool.root)?;
    let mut next_sequence = spool.last_record().map_or(Ok(0), |record| {
        record
            .metadata()
            .observation
            .sequence
            .checked_add(1)
            .context("shred UDP observation sequence exhausted")
    })?;
    let recovered_header = spool
        .last_record()
        .map(|record| spool.read_record(record))
        .transpose()
        .context("read last recovered shred UDP observation")?
        .and_then(|record| decode_stored_shred(&record.payload).ok())
        .and_then(|payload| parse_shred_header(&payload));

    let queue_capacity_bytes = usize::try_from(source.queue.max_bytes)
        .context("shred UDP queue byte capacity does not fit this platform")?;
    ensure!(
        queue_capacity_bytes <= Semaphore::MAX_PERMITS,
        "shred UDP queue byte capacity exceeds the async semaphore limit"
    );
    let bind_address: SocketAddr = bind.parse().context("parse shred UDP bind address")?;
    let (socket, receive_buffer_bytes, socket_rxq_overflow_supported) =
        bind_udp_socket(bind_address)?;
    join_multicast(&socket, multicast_group.as_deref(), interface.as_deref())?;

    let metrics = Arc::new(UdpIngestMetrics::default());
    let event_budget = Arc::new(Semaphore::new(source.queue.max_events));
    let byte_budget = Arc::new(Semaphore::new(queue_capacity_bytes));
    let (queue_tx, mut queue_rx) = mpsc::channel(source.queue.max_events);
    let (drain_shutdown_tx, drain_shutdown_rx) = watch::channel(false);

    tracing::info!(
        source_id = %source.id,
        %bind_address,
        receive_buffer_bytes,
        socket_rxq_overflow_supported,
        ingest_queue_capacity_events = source.queue.max_events,
        ingest_queue_capacity_bytes = source.queue.max_bytes,
        journal_id = %hex_journal_id(config.journal_id),
        next_sequence,
        "shred UDP durable recorder started"
    );

    let mut accepted = 0u64;
    let mut invalid = 0u64;
    let mut bytes = 0u64;
    let started_unix_secs = unix_time_secs()?;
    let mut latest_slot = recovered_header.map(|header| header.slot);
    let mut shred_version = recovered_header.map(|header| header.version);
    let mut last_durable_unix_secs: Option<u64> = None;
    let mut last_durable_at: Option<Instant> = None;
    let mut last_report = Instant::now();
    publish_recorder_status(
        config.status_file.as_deref(),
        &config.ingest,
        ShredRecorderStatusInput {
            started_unix_secs,
            state: ShredRecorderState::Waiting,
            accepted_total: accepted,
            invalid_total: invalid,
            bytes_total: bytes,
            next_sequence,
            latest_slot,
            shred_version,
            last_durable_unix_secs,
            spool_bytes,
        },
        &metrics,
        source.queue.max_events,
        source.queue.max_bytes,
        socket_rxq_overflow_supported,
    )?;
    let drain_metrics = Arc::clone(&metrics);
    let drain_event_budget = Arc::clone(&event_budget);
    let drain_byte_budget = Arc::clone(&byte_budget);
    let max_event_bytes = source.queue.max_event_bytes;
    let mut drain_task = tokio::spawn(async move {
        socket_drain_loop(
            socket,
            queue_tx,
            drain_event_budget,
            drain_byte_budget,
            max_event_bytes,
            drain_metrics,
            drain_shutdown_rx,
        )
        .await
    });
    let mut status_interval = tokio::time::interval(STATUS_INTERVAL);
    status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    status_interval.tick().await;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut shutdown_requested = false;
    let mut drain_ended_unexpectedly = false;
    let writer_result: Result<()> = async {
        loop {
            let queued = tokio::select! {
                queued = queue_rx.recv() => queued,
                _ = status_interval.tick() => {
                    match spool_root_bytes(&config.ingest.spool.root) {
                        Ok(current) => spool_bytes = current,
                        Err(error) => tracing::warn!(
                            error = %format!("{error:#}"),
                            "refresh shred UDP spool byte accounting"
                        ),
                    }
                    let state = recorder_state(last_durable_at);
                    if let Err(error) = publish_recorder_status(
                        config.status_file.as_deref(),
                        &config.ingest,
                        ShredRecorderStatusInput {
                            started_unix_secs,
                            state,
                            accepted_total: accepted,
                            invalid_total: invalid,
                            bytes_total: bytes,
                            next_sequence,
                            latest_slot,
                            shred_version,
                            last_durable_unix_secs,
                            spool_bytes,
                        },
                        &metrics,
                        source.queue.max_events,
                        source.queue.max_bytes,
                        socket_rxq_overflow_supported,
                    ) {
                        tracing::warn!(error = %format!("{error:#}"), "publish shred UDP recorder status");
                    }
                    continue;
                }
                () = &mut shutdown, if !shutdown_requested => {
                    shutdown_requested = true;
                    let _ = drain_shutdown_tx.send(true);
                    tracing::info!(
                        source_id = %source.id,
                        ingest_queue_depth_events = metrics.snapshot().queue_depth_events,
                        "shred UDP shutdown requested; draining the accepted ingress queue"
                    );
                    continue;
                }
            };
            let Some(first) = queued else {
                if !shutdown_requested {
                    drain_ended_unexpectedly = true;
                }
                break;
            };
            let datagrams = collect_durable_batch(first, &mut queue_rx).await;
            let mut pending = Vec::with_capacity(datagrams.len());
            let mut pending_raw_bytes = 0u64;
            let mut pending_last_header = None;
            let mut pending_max_slot = None;
            let mut pending_next_sequence = next_sequence;
            for datagram in datagrams {
                let queued_bytes = datagram.queue_bytes();
                if datagram.oversized {
                    invalid = invalid.saturating_add(1);
                    metrics.record_dequeued(queued_bytes);
                    continue;
                }
                let Some(header) = parse_shred_header(&datagram.payload) else {
                    invalid = invalid.saturating_add(1);
                    metrics.record_dequeued(queued_bytes);
                    continue;
                };
                let payload = zstd::bulk::compress(&datagram.payload, 1)
                    .context("independently compress shred datagram for durable spool")?;
                ensure!(
                    payload.len() as u64 <= source.queue.max_event_bytes,
                    "compressed shred datagram exceeds configured source maximum"
                );
                let metadata = IngressRecordMeta::from_payload(
                    config.ingest.cluster_id.clone(),
                    ObservationId {
                        origin_node_id: origin_node_id.clone(),
                        journal_id: config.journal_id,
                        sequence: pending_next_sequence,
                    },
                    source.id.clone(),
                    LogicalKey::Shred {
                        slot: header.slot,
                        kind: header.kind,
                        shred_index: header.index,
                        fec_set_index: Some(header.fec_set_index),
                    },
                    ZSTD_SOLANA_SHRED_V1,
                    &payload,
                );
                pending.push((metadata, payload));
                pending_raw_bytes = pending_raw_bytes
                    .checked_add(datagram.original_length as u64)
                    .context("shred UDP batch byte count overflow")?;
                pending_last_header = Some(header);
                pending_max_slot = advance_slot_high_water(pending_max_slot, header.slot);
                pending_next_sequence = pending_next_sequence
                    .checked_add(1)
                    .context("shred UDP observation sequence exhausted")?;
                metrics.record_dequeued(queued_bytes);
            }
            if pending.is_empty() {
                continue;
            }
            let pending_count = pending.len() as u64;
            let projected_bytes = spool.project_batch_additional_bytes(&pending)?;
            if !spool_bytes
                .checked_add(projected_bytes)
                .is_some_and(|total| total <= config.ingest.spool.max_bytes)
            {
                // The pull source retires ACK-covered sealed segments in another process. Refresh
                // on the admission boundary before failing closed so an already-freed segment
                // cannot leave this recorder stopped on stale in-memory quota accounting.
                match spool_root_bytes(&config.ingest.spool.root) {
                    Ok(current) => spool_bytes = current,
                    Err(error) => {
                        return Err(error)
                            .context("refresh shred UDP spool bytes before quota rejection");
                    }
                }
            }
            ensure!(
                spool_bytes
                    .checked_add(projected_bytes)
                    .is_some_and(|total| total <= config.ingest.spool.max_bytes),
                "shred UDP spool capacity would be exceeded"
            );
            let available_bytes = filesystem_available_bytes(&config.ingest.spool.root)?;
            ensure!(
                config
                    .ingest
                    .spool
                    .reserve_free_bytes
                    .checked_add(projected_bytes)
                    .is_some_and(|required| available_bytes >= required),
                "shred UDP filesystem reserve would be crossed"
            );
            let committed = spool
                .append_batch_and_sync(pending)
                .context("durably append shred UDP observation batch")?;
            ensure!(
                committed.records.len() as u64 == pending_count
                    && committed.additional_bytes == projected_bytes,
                "shred UDP group commit did not match its validated projection"
            );
            spool_bytes = spool_bytes
                .checked_add(committed.additional_bytes)
                .context("shred UDP spool byte accounting overflow")?;
            next_sequence = pending_next_sequence;
            accepted = accepted.saturating_add(pending_count);
            bytes = bytes.saturating_add(pending_raw_bytes);
            let last_header =
                pending_last_header.context("validated shred batch has no final header")?;
            let committed_max_slot =
                pending_max_slot.context("validated shred batch has no maximum slot")?;
            latest_slot = advance_slot_high_water(latest_slot, committed_max_slot);
            let durable_slot_high_water =
                latest_slot.context("committed shred batch did not advance a slot high-water")?;
            shred_version = Some(last_header.version);
            last_durable_unix_secs = Some(unix_time_secs()?);
            last_durable_at = Some(Instant::now());

            if last_report.elapsed() >= Duration::from_secs(10) {
                let snapshot = metrics.snapshot();
                tracing::info!(
                    source_id = %source.id,
                    accepted_total = accepted,
                    invalid_total = invalid,
                    bytes_total = bytes,
                    udp_received_total = snapshot.received_total,
                    ingest_queue_depth_events = snapshot.queue_depth_events,
                    ingest_queue_depth_bytes = snapshot.queue_depth_bytes,
                    ingest_queue_high_water_events = snapshot.queue_high_water_events,
                    ingest_queue_high_water_bytes = snapshot.queue_high_water_bytes,
                    ingest_queue_backpressure_events_total = snapshot.queue_backpressure_events_total,
                    socket_rxq_overflow_total = snapshot.socket_rxq_overflow_total,
                    latest_slot = durable_slot_high_water,
                    shred_version = last_header.version,
                    group_commit_records = pending_count,
                    "shred UDP recorder metrics"
                );
                last_report = Instant::now();
            }
        }
        Ok(())
    }
    .await;

    let _ = drain_shutdown_tx.send(true);
    drop(queue_rx);
    event_budget.close();
    byte_budget.close();
    let drain_result = (&mut drain_task)
        .await
        .context("shred UDP socket drain task panicked")?;

    if let Err(error) = writer_result {
        if let Err(drain_error) = drain_result {
            tracing::warn!(
                error = %format!("{drain_error:#}"),
                "shred UDP socket drain also failed while stopping the durable writer"
            );
        }
        return Err(error);
    }
    drain_result.context("shred UDP socket drain failed")?;
    ensure!(
        shutdown_requested && !drain_ended_unexpectedly,
        "shred UDP socket drain stopped unexpectedly"
    );

    if let Err(error) = publish_recorder_status(
        config.status_file.as_deref(),
        &config.ingest,
        ShredRecorderStatusInput {
            started_unix_secs,
            state: ShredRecorderState::Stopped,
            accepted_total: accepted,
            invalid_total: invalid,
            bytes_total: bytes,
            next_sequence,
            latest_slot,
            shred_version,
            last_durable_unix_secs,
            spool_bytes,
        },
        &metrics,
        source.queue.max_events,
        source.queue.max_bytes,
        socket_rxq_overflow_supported,
    ) {
        tracing::warn!(error = %format!("{error:#}"), "publish stopped shred UDP recorder status");
    }
    let snapshot = metrics.snapshot();
    tracing::info!(
        source_id = %source.id,
        accepted_total = accepted,
        invalid_total = invalid,
        bytes_total = bytes,
        udp_received_total = snapshot.received_total,
        ingest_queue_high_water_events = snapshot.queue_high_water_events,
        ingest_queue_high_water_bytes = snapshot.queue_high_water_bytes,
        ingest_queue_backpressure_events_total = snapshot.queue_backpressure_events_total,
        socket_rxq_overflow_total = snapshot.socket_rxq_overflow_total,
        "shred UDP durable recorder stopped cleanly after draining"
    );
    Ok(())
}

async fn socket_drain_loop(
    socket: UdpSocket,
    queue: mpsc::Sender<QueuedDatagram>,
    event_budget: Arc<Semaphore>,
    byte_budget: Arc<Semaphore>,
    max_event_bytes: u64,
    metrics: Arc<UdpIngestMetrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut buffer = vec![0u8; MAX_UDP_DATAGRAM_BYTES];
    let mut previous_socket_overflow = 0u32;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let received = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            received = receive_socket_datagram(&socket, &mut buffer) => {
                received.context("receive shred UDP")?
            }
        };
        observe_socket_overflow(received, &mut previous_socket_overflow, &metrics);
        enqueue_datagram(
            &queue,
            &event_budget,
            &byte_budget,
            max_event_bytes,
            &metrics,
            &buffer[..received.length],
        )
        .await?;

        // Tokio readiness wakes once, then this loop drains every immediately available packet
        // before yielding. This mirrors Agave's packet-batch socket drain without coupling it to
        // compression or stable-storage latency.
        for _ in 1..SOCKET_DRAIN_BURST_MAX_RECORDS {
            let received = match try_receive_socket_datagram(&socket, &mut buffer) {
                Ok(received) => received,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error).context("drain available shred UDP datagrams"),
            };
            observe_socket_overflow(received, &mut previous_socket_overflow, &metrics);
            enqueue_datagram(
                &queue,
                &event_budget,
                &byte_budget,
                max_event_bytes,
                &metrics,
                &buffer[..received.length],
            )
            .await?;
        }
        tokio::task::yield_now().await;
    }
}

fn observe_socket_overflow(
    received: ReceivedDatagram,
    previous: &mut u32,
    metrics: &UdpIngestMetrics,
) {
    let Some(current) = received.socket_rxq_overflow else {
        return;
    };
    let dropped = current.wrapping_sub(*previous) as u64;
    *previous = current;
    if dropped == 0 {
        return;
    }
    metrics.record_socket_overflow(dropped);
    tracing::error!(
        socket_rxq_overflow_delta = dropped,
        socket_rxq_overflow_total = metrics.snapshot().socket_rxq_overflow_total,
        "Linux reported shred UDP datagrams dropped from this socket receive queue"
    );
}

async fn enqueue_datagram(
    queue: &mpsc::Sender<QueuedDatagram>,
    event_budget: &Arc<Semaphore>,
    byte_budget: &Arc<Semaphore>,
    max_event_bytes: u64,
    metrics: &UdpIngestMetrics,
    datagram: &[u8],
) -> Result<()> {
    use tokio::sync::{TryAcquireError, mpsc::error::TrySendError};

    metrics.record_received(datagram.len());
    let mut backpressure_started = None;
    let queue_permit = match queue.try_reserve() {
        Ok(permit) => permit,
        Err(TrySendError::Full(_)) => {
            backpressure_started = Some(metrics.begin_backpressure());
            queue
                .reserve()
                .await
                .map_err(|_| anyhow::anyhow!("shred UDP durable writer queue closed"))?
        }
        Err(TrySendError::Closed(_)) => {
            anyhow::bail!("shred UDP durable writer queue closed")
        }
    };

    let event_permit = match Arc::clone(event_budget).try_acquire_owned() {
        Ok(permit) => permit,
        Err(TryAcquireError::NoPermits) => {
            if backpressure_started.is_none() {
                backpressure_started = Some(metrics.begin_backpressure());
            }
            Arc::clone(event_budget)
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("shred UDP event-bounded queue closed"))?
        }
        Err(TryAcquireError::Closed) => {
            anyhow::bail!("shred UDP event-bounded queue closed")
        }
    };

    let oversized = datagram.len() as u64 > max_event_bytes;
    let queued_bytes = if oversized { 0 } else { datagram.len() };
    let byte_permit = if queued_bytes == 0 {
        None
    } else {
        let permits =
            u32::try_from(queued_bytes).context("shred UDP datagram byte permit count overflow")?;
        match Arc::clone(byte_budget).try_acquire_many_owned(permits) {
            Ok(permit) => Some(permit),
            Err(TryAcquireError::NoPermits) => {
                if backpressure_started.is_none() {
                    backpressure_started = Some(metrics.begin_backpressure());
                }
                Some(
                    Arc::clone(byte_budget)
                        .acquire_many_owned(permits)
                        .await
                        .map_err(|_| anyhow::anyhow!("shred UDP byte-bounded queue closed"))?,
                )
            }
            Err(TryAcquireError::Closed) => {
                anyhow::bail!("shred UDP byte-bounded queue closed")
            }
        }
    };

    let payload = if oversized {
        Vec::new()
    } else {
        datagram.to_vec()
    };
    metrics.record_enqueued(queued_bytes);
    queue_permit.send(QueuedDatagram {
        payload,
        original_length: datagram.len(),
        oversized,
        _event_permit: event_permit,
        _byte_permit: byte_permit,
    });
    if let Some(started) = backpressure_started {
        metrics.end_backpressure(started);
    }
    Ok(())
}

async fn collect_durable_batch(
    first: QueuedDatagram,
    queue: &mut mpsc::Receiver<QueuedDatagram>,
) -> Vec<QueuedDatagram> {
    use tokio::sync::mpsc::error::TryRecvError;

    let mut datagrams = Vec::with_capacity(DURABLE_BATCH_MAX_RECORDS);
    datagrams.push(first);
    let deadline = tokio::time::Instant::now() + DURABLE_BATCH_COLLECTION_WINDOW;
    while datagrams.len() < DURABLE_BATCH_MAX_RECORDS {
        let received = match queue.try_recv() {
            Ok(datagram) => Some(datagram),
            Err(TryRecvError::Disconnected) => None,
            Err(TryRecvError::Empty) => match tokio::time::timeout_at(deadline, queue.recv()).await
            {
                Ok(datagram) => datagram,
                Err(_) => None,
            },
        };
        let Some(datagram) = received else {
            break;
        };
        datagrams.push(datagram);
    }
    datagrams
}

#[cfg(not(target_os = "linux"))]
async fn receive_socket_datagram(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<ReceivedDatagram> {
    socket
        .recv_from(buffer)
        .await
        .map(|(length, _)| ReceivedDatagram {
            length,
            socket_rxq_overflow: None,
        })
}

#[cfg(target_os = "linux")]
async fn receive_socket_datagram(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<ReceivedDatagram> {
    socket
        .async_io(tokio::io::Interest::READABLE, || {
            recvmsg_socket_datagram(socket, buffer)
        })
        .await
}

#[cfg(not(target_os = "linux"))]
fn try_receive_socket_datagram(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<ReceivedDatagram> {
    socket
        .try_recv_from(buffer)
        .map(|(length, _)| ReceivedDatagram {
            length,
            socket_rxq_overflow: None,
        })
}

#[cfg(target_os = "linux")]
fn try_receive_socket_datagram(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<ReceivedDatagram> {
    socket.try_io(tokio::io::Interest::READABLE, || {
        recvmsg_socket_datagram(socket, buffer)
    })
}

#[cfg(target_os = "linux")]
fn recvmsg_socket_datagram(socket: &UdpSocket, buffer: &mut [u8]) -> io::Result<ReceivedDatagram> {
    use std::{mem, os::fd::AsRawFd, ptr};

    let mut io_vector = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    // A usize array gives cmsghdr its required native alignment. SO_RXQ_OVFL contains one u32;
    // the extra space tolerates any kernel alignment without heap allocation per datagram.
    let mut control = [0usize; 8];
    // SAFETY: every pointer in the message references writable storage that remains alive for the
    // syscall. MSG_DONTWAIT preserves Tokio's readiness contract and MSG_TRUNC exposes any
    // impossible-over-65KiB truncation rather than silently accepting a partial datagram.
    let mut message = unsafe { mem::zeroed::<libc::msghdr>() };
    message.msg_iov = &mut io_vector;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = mem::size_of_val(&control);
    let received = unsafe {
        libc::recvmsg(
            socket.as_raw_fd(),
            &mut message,
            libc::MSG_DONTWAIT | libc::MSG_TRUNC,
        )
    };
    if received < 0 {
        return Err(io::Error::last_os_error());
    }
    let length = usize::try_from(received)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative UDP length"))?;
    if length > buffer.len() || message.msg_flags & libc::MSG_TRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "shred UDP datagram exceeded the full-size receive buffer",
        ));
    }
    if message.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "shred UDP ancillary data was truncated",
        ));
    }

    let mut socket_rxq_overflow = None;
    // SAFETY: recvmsg initialized the control region and its reported length. SO_RXQ_OVFL is the
    // only ancillary option enabled on this socket, and CMSG_FIRSTHDR bounds-checks its header;
    // the native u32 payload is then copied unaligned.
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        if !header.is_null()
            && (*header).cmsg_level == libc::SOL_SOCKET
            && (*header).cmsg_type == libc::SO_RXQ_OVFL
            && (*header).cmsg_len >= libc::CMSG_LEN(mem::size_of::<u32>() as libc::c_uint) as usize
        {
            socket_rxq_overflow = Some(ptr::read_unaligned(libc::CMSG_DATA(header).cast::<u32>()));
        }
    }
    Ok(ReceivedDatagram {
        length,
        socket_rxq_overflow,
    })
}

#[derive(Debug, Clone, Copy)]
struct ShredRecorderStatusInput {
    started_unix_secs: u64,
    state: ShredRecorderState,
    accepted_total: u64,
    invalid_total: u64,
    bytes_total: u64,
    next_sequence: u64,
    latest_slot: Option<u64>,
    shred_version: Option<u16>,
    last_durable_unix_secs: Option<u64>,
    spool_bytes: u64,
}

fn publish_recorder_status(
    path: Option<&Path>,
    ingest: &IngestConfig,
    input: ShredRecorderStatusInput,
    metrics: &UdpIngestMetrics,
    queue_capacity_events: usize,
    queue_capacity_bytes: u64,
    socket_rxq_overflow_supported: bool,
) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let (filesystem_free_bytes, filesystem_total_bytes) =
        filesystem_capacity_bytes(&ingest.spool.root)?;
    let metrics = metrics.snapshot();
    let status = ShredRecorderStatus {
        schema_version: STATUS_SCHEMA_VERSION,
        updated_unix_secs: unix_time_secs()?,
        started_unix_secs: input.started_unix_secs,
        state: input.state,
        accepted_total: input.accepted_total,
        invalid_total: input.invalid_total,
        bytes_total: input.bytes_total,
        durable_through_sequence: input.next_sequence.checked_sub(1),
        latest_slot: input.latest_slot,
        shred_version: input.shred_version,
        last_durable_unix_secs: input.last_durable_unix_secs,
        spool_bytes: input.spool_bytes,
        spool_max_bytes: ingest.spool.max_bytes,
        filesystem_free_bytes,
        filesystem_total_bytes,
        reserve_free_bytes: ingest.spool.reserve_free_bytes,
        udp_received_total: metrics.received_total,
        udp_received_bytes_total: metrics.received_bytes_total,
        ingest_queue_depth_events: metrics.queue_depth_events,
        ingest_queue_depth_bytes: metrics.queue_depth_bytes,
        ingest_queue_high_water_events: metrics.queue_high_water_events,
        ingest_queue_high_water_bytes: metrics.queue_high_water_bytes,
        ingest_queue_capacity_events: queue_capacity_events,
        ingest_queue_capacity_bytes: queue_capacity_bytes,
        ingest_queue_backpressure_events_total: metrics.queue_backpressure_events_total,
        ingest_queue_backpressure_micros_total: metrics.queue_backpressure_micros_total,
        ingest_queue_backpressured: metrics.queue_backpressured,
        socket_rxq_overflow_supported,
        socket_rxq_overflow_total: socket_rxq_overflow_supported
            .then_some(metrics.socket_rxq_overflow_total),
    };
    write_json_atomic(path, &status)
}

fn recorder_state(last_durable_at: Option<Instant>) -> ShredRecorderState {
    match last_durable_at {
        None => ShredRecorderState::Waiting,
        Some(last) if last.elapsed() <= RECEIVING_FRESHNESS => ShredRecorderState::Receiving,
        Some(_) => ShredRecorderState::Stalled,
    }
}

fn advance_slot_high_water(current: Option<u64>, committed_slot: u64) -> Option<u64> {
    Some(current.map_or(committed_slot, |slot| slot.max(committed_slot)))
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("shred status file has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create shred status directory {}", parent.display()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("shred status file has an invalid name")?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_nanos();
    let temporary = parent.join(format!(".{name}.{}.{}.tmp", std::process::id(), nonce));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .with_context(|| format!("create shred status temp {}", temporary.display()))?;
        serde_json::to_writer(&mut file, value).context("encode shred recorder status")?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "publish shred recorder status {} from {}",
                path.display(),
                temporary.display()
            )
        })?;
        sync_parent_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn unix_time_secs() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs())
}

fn sync_parent_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open shred status directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync shred status directory {}", path.display()))
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let terminate = signal(SignalKind::terminate());
    let Ok(mut terminate) = terminate else {
        let _ = tokio::signal::ctrl_c().await;
        return;
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = terminate.recv() => {},
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn spool_root_bytes(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in
        fs::read_dir(path).with_context(|| format!("list spool root {}", path.display()))?
    {
        let entry = entry?;
        let entry_path = entry.path();
        let metadata = match fs::symlink_metadata(&entry_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // ACK-driven retention can unlink a sealed segment between read_dir and stat.
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspect spool entry {}", entry_path.display()));
            }
        };
        ensure!(
            !metadata.file_type().is_symlink(),
            "shred UDP spool contains a symbolic link: {}",
            entry_path.display()
        );
        let entry_bytes = if metadata.is_dir() {
            QUOTA_ENTRY_MIN_BYTES
                .checked_add(spool_root_bytes(&entry_path)?)
                .context("shred UDP spool byte count overflow")?
        } else {
            ensure!(
                metadata.is_file(),
                "shred UDP spool contains a non-regular entry: {}",
                entry_path.display()
            );
            metadata.len().max(QUOTA_ENTRY_MIN_BYTES)
        };
        total = total
            .checked_add(entry_bytes)
            .context("shred UDP spool byte count overflow")?;
    }
    Ok(total)
}

#[cfg(unix)]
fn filesystem_capacity_bytes(path: &Path) -> Result<(u64, u64)> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let path = CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("shred UDP spool path contains NUL: {}", path.display()))?;
    // SAFETY: stat is writable storage and path remains a valid NUL-terminated string.
    let mut stat = unsafe { std::mem::zeroed::<libc::statvfs>() };
    let result = unsafe { libc::statvfs(path.as_ptr(), &mut stat) };
    if result != 0 {
        return Err(io::Error::last_os_error()).context("read shred UDP filesystem free space");
    }
    let available = (stat.f_bavail as u64)
        .checked_mul(stat.f_frsize as u64)
        .context("shred UDP filesystem available byte count overflow")?;
    let total = (stat.f_blocks as u64)
        .checked_mul(stat.f_frsize as u64)
        .context("shred UDP filesystem total byte count overflow")?;
    Ok((available, total))
}

#[cfg(not(unix))]
fn filesystem_capacity_bytes(_path: &Path) -> Result<(u64, u64)> {
    Ok((u64::MAX, u64::MAX))
}

fn filesystem_available_bytes(path: &Path) -> Result<u64> {
    filesystem_capacity_bytes(path).map(|(available, _)| available)
}

fn bind_udp_socket(address: SocketAddr) -> Result<(UdpSocket, usize, bool)> {
    let domain = if address.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(SocketProtocol::UDP))
        .context("create shred UDP socket")?;
    socket
        .set_nonblocking(true)
        .context("make shred UDP socket nonblocking")?;
    if let Err(error) = socket.set_recv_buffer_size(UDP_RECEIVE_BUFFER_BYTES) {
        tracing::warn!(
            requested_bytes = UDP_RECEIVE_BUFFER_BYTES,
            error = %error,
            "kernel rejected requested shred UDP receive buffer; continuing with the system limit"
        );
    }
    let socket_rxq_overflow_supported = enable_socket_rxq_overflow(&socket);
    socket
        .bind(&SockAddr::from(address))
        .with_context(|| format!("bind shred UDP source at {address}"))?;
    let effective_buffer = socket
        .recv_buffer_size()
        .context("inspect shred UDP receive buffer")?;
    let socket: std::net::UdpSocket = socket.into();
    Ok((
        UdpSocket::from_std(socket).context("attach shred UDP socket to Tokio")?,
        effective_buffer,
        socket_rxq_overflow_supported,
    ))
}

#[cfg(target_os = "linux")]
fn enable_socket_rxq_overflow(socket: &Socket) -> bool {
    use std::{mem, os::fd::AsRawFd};

    let enabled: libc::c_int = 1;
    // SAFETY: the socket descriptor is valid and the option value points to an initialized int
    // of the exact length supplied to setsockopt.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RXQ_OVFL,
            (&enabled as *const libc::c_int).cast(),
            mem::size_of_val(&enabled) as libc::socklen_t,
        )
    };
    if result == 0 {
        true
    } else {
        tracing::warn!(
            error = %io::Error::last_os_error(),
            "could not enable Linux SO_RXQ_OVFL telemetry for the shred UDP socket"
        );
        false
    }
}

#[cfg(not(target_os = "linux"))]
fn enable_socket_rxq_overflow(_socket: &Socket) -> bool {
    false
}

fn join_multicast(socket: &UdpSocket, group: Option<&str>, interface: Option<&str>) -> Result<()> {
    let Some(group) = group else {
        return Ok(());
    };
    let group: IpAddr = group.parse().context("parse shred UDP multicast group")?;
    let interface = interface.context("shred UDP multicast interface is required")?;
    match group {
        IpAddr::V4(group) => {
            let interface = resolve_ipv4_multicast_interface(interface)?;
            SockRef::from(socket)
                .join_multicast_v4_n(&group, &interface)
                .context("join IPv4 shred UDP multicast group")
        }
        IpAddr::V6(_) => {
            anyhow::bail!("IPv6 shred UDP multicast requires an interface index and is unsupported")
        }
    }
}

fn resolve_ipv4_multicast_interface(interface: &str) -> Result<InterfaceIndexOrAddress> {
    match interface.parse::<IpAddr>() {
        Ok(IpAddr::V4(address)) => Ok(InterfaceIndexOrAddress::Address(address)),
        Ok(IpAddr::V6(_)) => {
            anyhow::bail!("shred UDP multicast group and interface address families differ")
        }
        Err(_) => {
            let interface_name = CString::new(interface)
                .context("shred UDP multicast interface name contains a NUL byte")?;
            // POSIX exposes no safe standard-library wrapper for resolving an interface name.
            let index = unsafe { libc::if_nametoindex(interface_name.as_ptr()) };
            ensure!(
                index != 0,
                "shred UDP multicast interface {interface:?} does not exist"
            );
            Ok(InterfaceIndexOrAddress::Index(index))
        }
    }
}

fn hex_journal_id(journal_id: [u8; 16]) -> String {
    let mut output = String::with_capacity(32);
    for byte in journal_id {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "blockzilla-shred-status-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("test clock")
                .as_nanos()
        ))
    }

    #[test]
    fn parses_data_and_coding_shred_coordinates() {
        const COMMON_SHRED_HEADER_BYTES: usize = 83;
        const SHRED_VARIANT_OFFSET: usize = 64;
        const SLOT_OFFSET: usize = 65;
        const INDEX_OFFSET: usize = 73;
        const VERSION_OFFSET: usize = 77;
        const FEC_SET_INDEX_OFFSET: usize = 79;

        for (variant, kind) in [(0x90, ShredKind::Data), (0x6f, ShredKind::Coding)] {
            let mut payload = [0u8; COMMON_SHRED_HEADER_BYTES];
            payload[SHRED_VARIANT_OFFSET] = variant;
            payload[SLOT_OFFSET..SLOT_OFFSET + 8].copy_from_slice(&42u64.to_le_bytes());
            payload[INDEX_OFFSET..INDEX_OFFSET + 4].copy_from_slice(&7u32.to_le_bytes());
            payload[VERSION_OFFSET..VERSION_OFFSET + 2].copy_from_slice(&50093u16.to_le_bytes());
            payload[FEC_SET_INDEX_OFFSET..FEC_SET_INDEX_OFFSET + 4]
                .copy_from_slice(&3u32.to_le_bytes());

            assert_eq!(
                parse_shred_header(&payload),
                Some(ParsedShredHeader {
                    slot: 42,
                    index: 7,
                    version: 50093,
                    fec_set_index: 3,
                    kind,
                })
            );
        }
    }

    #[test]
    fn rejects_short_or_unknown_shreds() {
        assert_eq!(parse_shred_header(&[0; 82]), None);
        assert_eq!(parse_shred_header(&[0; COMMON_SHRED_HEADER_BYTES]), None);
    }

    #[test]
    fn durable_slot_high_water_never_regresses_on_late_udp() {
        let recovered_high_water = Some(500);
        let committed_batch_max = [501, 502, 499]
            .into_iter()
            .fold(None, advance_slot_high_water)
            .expect("batch maximum");
        assert_eq!(committed_batch_max, 502);
        assert_eq!(
            advance_slot_high_water(recovered_high_water, committed_batch_max),
            Some(502)
        );
        assert_eq!(advance_slot_high_water(Some(502), 498), Some(502));
        assert_eq!(advance_slot_high_water(None, 42), Some(42));
    }

    #[test]
    fn socket_overflow_telemetry_handles_counter_wraparound() {
        let metrics = UdpIngestMetrics::default();
        let mut previous = u32::MAX;
        observe_socket_overflow(
            ReceivedDatagram {
                length: 1,
                socket_rxq_overflow: Some(2),
            },
            &mut previous,
            &metrics,
        );
        assert_eq!(metrics.snapshot().socket_rxq_overflow_total, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn socket_drain_preserves_duplicates_and_backpressures_without_dropping() {
        let (socket, _, _) = bind_udp_socket("127.0.0.1:0".parse().expect("loopback address"))
            .expect("bind receiver");
        let receiver_address = socket.local_addr().expect("receiver address");
        let sender = UdpSocket::bind("127.0.0.1:0").await.expect("bind sender");
        let metrics = Arc::new(UdpIngestMetrics::default());
        let event_budget = Arc::new(Semaphore::new(1));
        let byte_budget = Arc::new(Semaphore::new(4));
        let (queue_tx, mut queue_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let drain = tokio::spawn(socket_drain_loop(
            socket,
            queue_tx,
            Arc::clone(&event_budget),
            Arc::clone(&byte_budget),
            4,
            Arc::clone(&metrics),
            shutdown_rx,
        ));

        sender
            .send_to(&[1, 2, 3, 4], receiver_address)
            .await
            .expect("send first");
        tokio::time::timeout(Duration::from_secs(1), async {
            while metrics.snapshot().queue_depth_events != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first datagram queued");
        sender
            .send_to(&[1, 2, 3, 4], receiver_address)
            .await
            .expect("send duplicate");
        sender
            .send_to(&[9], receiver_address)
            .await
            .expect("send third");
        tokio::time::timeout(Duration::from_secs(1), async {
            while metrics.snapshot().queue_backpressure_events_total == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("socket drain entered backpressure");

        let mut actual = Vec::new();
        for _ in 0..3 {
            let datagram = tokio::time::timeout(Duration::from_secs(1), queue_rx.recv())
                .await
                .expect("queued datagram timeout")
                .expect("queued datagram");
            metrics.record_dequeued(datagram.queue_bytes());
            actual.push(datagram.payload.clone());
            drop(datagram);
        }
        assert_eq!(actual, vec![vec![1, 2, 3, 4], vec![1, 2, 3, 4], vec![9]]);
        assert_eq!(metrics.snapshot().received_total, 3);
        assert_eq!(metrics.snapshot().queue_high_water_events, 1);
        assert_eq!(metrics.snapshot().queue_high_water_bytes, 4);
        assert_eq!(metrics.snapshot().queue_depth_events, 0);
        assert_eq!(metrics.snapshot().queue_depth_bytes, 0);

        shutdown_tx.send(true).expect("request shutdown");
        drain
            .await
            .expect("socket drain task")
            .expect("clean socket drain");
    }

    #[tokio::test]
    async fn oversized_datagram_is_accounted_without_escaping_the_byte_bound() {
        let metrics = UdpIngestMetrics::default();
        let event_budget = Arc::new(Semaphore::new(1));
        let byte_budget = Arc::new(Semaphore::new(2));
        let (queue_tx, mut queue_rx) = mpsc::channel(1);
        enqueue_datagram(
            &queue_tx,
            &event_budget,
            &byte_budget,
            2,
            &metrics,
            &[1, 2, 3],
        )
        .await
        .expect("enqueue oversized marker");
        let datagram = queue_rx.recv().await.expect("oversized marker");
        assert!(datagram.oversized);
        assert_eq!(datagram.original_length, 3);
        assert!(datagram.payload.is_empty());
        assert_eq!(metrics.snapshot().received_total, 1);
        assert_eq!(metrics.snapshot().queue_depth_bytes, 0);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn resolves_a_named_ipv4_multicast_interface() {
        #[cfg(target_os = "linux")]
        let loopback = "lo";
        #[cfg(target_os = "macos")]
        let loopback = "lo0";
        assert!(matches!(
            resolve_ipv4_multicast_interface(loopback).expect("resolve loopback interface"),
            InterfaceIndexOrAddress::Index(index) if index > 0
        ));
    }

    #[test]
    fn atomically_publishes_only_the_recorder_status_contract() {
        let directory = unique_temp_dir();
        let path = directory.join("recorder.json");
        let status = ShredRecorderStatus {
            schema_version: STATUS_SCHEMA_VERSION,
            updated_unix_secs: 20,
            started_unix_secs: 10,
            state: ShredRecorderState::Receiving,
            accepted_total: 8,
            invalid_total: 1,
            bytes_total: 9_600,
            durable_through_sequence: Some(7),
            latest_slot: Some(42),
            shred_version: Some(50_093),
            last_durable_unix_secs: Some(20),
            spool_bytes: 12_345,
            spool_max_bytes: 20_000,
            filesystem_free_bytes: 30_000,
            filesystem_total_bytes: 40_000,
            reserve_free_bytes: 5_000,
            udp_received_total: 10,
            udp_received_bytes_total: 12_000,
            ingest_queue_depth_events: 2,
            ingest_queue_depth_bytes: 2_400,
            ingest_queue_high_water_events: 4,
            ingest_queue_high_water_bytes: 4_800,
            ingest_queue_capacity_events: 8,
            ingest_queue_capacity_bytes: 9_600,
            ingest_queue_backpressure_events_total: 1,
            ingest_queue_backpressure_micros_total: 50,
            ingest_queue_backpressured: false,
            socket_rxq_overflow_supported: true,
            socket_rxq_overflow_total: Some(3),
        };

        write_json_atomic(&path, &status).expect("publish status");
        let raw = fs::read_to_string(&path).expect("read status");
        let actual: serde_json::Value = serde_json::from_str(&raw).expect("decode status");
        assert_eq!(actual["state"], "receiving");
        assert_eq!(actual["durable_through_sequence"], 7);
        assert_eq!(actual["ingest_queue_high_water_events"], 4);
        assert_eq!(actual["socket_rxq_overflow_total"], 3);
        assert_eq!(actual.as_object().expect("object").len(), 29);
        for forbidden in ["bind", "peer", "source_id", "journal_id", "token", "secret"] {
            assert!(!raw.contains(forbidden));
        }

        fs::remove_dir_all(directory).expect("remove test directory");
    }
}
