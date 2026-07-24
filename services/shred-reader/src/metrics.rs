use std::{
    net::IpAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{Json, Router, http::StatusCode, routing::get};
use lru::LruCache;
use serde::Serialize;
use solana_gossip::cluster_info::ClusterInfo;
use solana_ledger::shred::ShredId;
use tokio::{net::TcpListener, sync::watch};

use crate::{config::nonzero_usize, repair_wal::RepairWalInspection};

pub struct Metrics {
    started_at: Instant,
    packets: AtomicU64,
    bytes: AtomicU64,
    parsed: AtomicU64,
    invalid: AtomicU64,
    version_mismatch: AtomicU64,
    unique: AtomicU64,
    duplicates: AtomicU64,
    data: AtomicU64,
    code: AtomicU64,
    forwarded: AtomicU64,
    forward_send_errors: AtomicU64,
    forward_queue_enqueued: AtomicU64,
    forward_queue_dropped: AtomicU64,
    forward_queue_depth: AtomicU64,
    tvu_socket_rxq_overflow_supported: AtomicU64,
    tvu_socket_rxq_overflow: AtomicU64,
    latest_slot: AtomicU64,
    last_packet_unix_ms: AtomicU64,
    last_shred_unix_ms: AtomicU64,
    last_forward_unix_ms: AtomicU64,
    last_forward_error_unix_ms: AtomicU64,
    repair_active: AtomicU64,
    repair_peers: AtomicU64,
    repair_tracked_slots: AtomicU64,
    repair_outstanding: AtomicU64,
    repair_observation_queue_dropped: AtomicU64,
    repair_socket_datagrams_received: AtomicU64,
    repair_response_datagrams_processed: AtomicU64,
    repair_socket_requested_recv_buffer_bytes: AtomicU64,
    repair_socket_effective_recv_buffer_bytes: AtomicU64,
    repair_socket_rxq_overflow_supported: AtomicU64,
    repair_socket_rxq_overflow: AtomicU64,
    repair_response_queue_capacity: AtomicU64,
    repair_response_queue_depth: AtomicU64,
    repair_response_queue_dropped: AtomicU64,
    repair_requests_sent: AtomicU64,
    repair_retries_sent: AtomicU64,
    repair_requests_exhausted: AtomicU64,
    repair_requests_cooldown_deferred: AtomicU64,
    repair_packets_rejected: AtomicU64,
    repair_pings_answered: AtomicU64,
    repair_shreds_accepted: AtomicU64,
    repair_root_anchored_shreds_accepted: AtomicU64,
    repair_wal_bytes: AtomicU64,
    repair_wal_max_bytes: AtomicU64,
    repair_wal_active_segment_bytes: AtomicU64,
    repair_wal_segment_count: AtomicU64,
    repair_wal_active_segment_id: AtomicU64,
    repair_wal_rollovers: AtomicU64,
    repair_wal_durable_through_sequence: AtomicU64,
    repair_wal_total_warning_bytes: AtomicU64,
    repair_wal_total_critical_bytes: AtomicU64,
    repair_wal_total_hard_bytes: AtomicU64,
    repair_wal_filesystem_reserve_bytes: AtomicU64,
    repair_wal_filesystem_available_bytes: AtomicU64,
    repair_wal_v3_sealed: AtomicU64,
    repair_wal_syncs: AtomicU64,
    repair_errors: AtomicU64,
    repair_restart_count: AtomicU64,
    repair_last_success_unix_ms: AtomicU64,
    state: Mutex<ReceiverState>,
}

struct ReceiverState {
    dedup: LruCache<ShredId, ()>,
    sources: LruCache<IpAddr, ()>,
    repair_state: RepairState,
    repair_last_error: Option<String>,
    repair_wal_last_error: Option<String>,
    repair_counter_samples: RepairCounterSamples,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RepairCounterSamples {
    socket_datagrams_received: u64,
    response_datagrams_processed: u64,
    socket_rxq_overflow: u64,
    response_queue_dropped: u64,
    requests_sent: u64,
    retries_sent: u64,
    requests_exhausted: u64,
    requests_cooldown_deferred: u64,
    packets_rejected: u64,
    pings_answered: u64,
    shreds_accepted: u64,
    root_anchored_shreds_accepted: u64,
    wal_rollovers: u64,
    wal_syncs: u64,
}

impl RepairCounterSamples {
    fn take_delta(&mut self, update: &RepairMetricsUpdate) -> Self {
        let next = Self {
            socket_datagrams_received: update.socket_datagrams_received,
            response_datagrams_processed: update.response_datagrams_processed,
            socket_rxq_overflow: update.socket_rxq_overflow,
            response_queue_dropped: update.response_queue_dropped,
            requests_sent: update.requests_sent,
            retries_sent: update.retries_sent,
            requests_exhausted: update.requests_exhausted,
            requests_cooldown_deferred: update.requests_cooldown_deferred,
            packets_rejected: update.packets_rejected,
            pings_answered: update.pings_answered,
            shreds_accepted: update.shreds_accepted,
            root_anchored_shreds_accepted: update.root_anchored_shreds_accepted,
            wal_rollovers: update.wal_rollovers,
            wal_syncs: update.wal_syncs,
        };
        let delta = Self {
            socket_datagrams_received: next
                .socket_datagrams_received
                .saturating_sub(self.socket_datagrams_received),
            response_datagrams_processed: next
                .response_datagrams_processed
                .saturating_sub(self.response_datagrams_processed),
            socket_rxq_overflow: next
                .socket_rxq_overflow
                .saturating_sub(self.socket_rxq_overflow),
            response_queue_dropped: next
                .response_queue_dropped
                .saturating_sub(self.response_queue_dropped),
            requests_sent: next.requests_sent.saturating_sub(self.requests_sent),
            retries_sent: next.retries_sent.saturating_sub(self.retries_sent),
            requests_exhausted: next
                .requests_exhausted
                .saturating_sub(self.requests_exhausted),
            requests_cooldown_deferred: next
                .requests_cooldown_deferred
                .saturating_sub(self.requests_cooldown_deferred),
            packets_rejected: next.packets_rejected.saturating_sub(self.packets_rejected),
            pings_answered: next.pings_answered.saturating_sub(self.pings_answered),
            shreds_accepted: next.shreds_accepted.saturating_sub(self.shreds_accepted),
            root_anchored_shreds_accepted: next
                .root_anchored_shreds_accepted
                .saturating_sub(self.root_anchored_shreds_accepted),
            wal_rollovers: next.wal_rollovers.saturating_sub(self.wal_rollovers),
            wal_syncs: next.wal_syncs.saturating_sub(self.wal_syncs),
        };
        *self = next;
        delta
    }
}

const SOURCE_CACHE_CAPACITY: usize = 65_536;
const RECENT_GOSSIP_PEER_WINDOW: Duration = Duration::from_secs(60);
const RECENT_SHRED_WINDOW: Duration = Duration::from_secs(60);
const RECENT_FORWARD_WINDOW: Duration = Duration::from_secs(60);
const RECENT_FORWARD_ERROR_WINDOW: Duration = Duration::from_secs(15);
const NO_DURABLE_REPAIR_SEQUENCE: u64 = u64::MAX;
const NO_FILESYSTEM_AVAILABLE_BYTES: u64 = u64::MAX;
const MAX_REPAIR_ERROR_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairState {
    Disabled,
    Inactive,
    Starting,
    Active,
    Backoff,
    Stopping,
}

#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub identity: String,
    pub advertised_ip: String,
    pub gossip_port: u16,
    pub tvu_port: u16,
    pub shred_version: u16,
    pub uptime_seconds: u64,
    pub gossip_peers: usize,
    pub recent_gossip_peers: usize,
    pub tvu_peers: usize,
    pub packets_total: u64,
    pub bytes_total: u64,
    pub parsed_total: u64,
    pub invalid_total: u64,
    pub version_mismatch_total: u64,
    pub unique_total: u64,
    pub duplicates_total: u64,
    pub data_total: u64,
    pub code_total: u64,
    pub forward_targets: usize,
    /// Effective local UDP source address used for downstream attribution. None when forwarding
    /// is disabled; an ephemeral port is still reported when no fixed bind was configured.
    pub forward_sender_addr: Option<String>,
    pub forwarded_datagrams_total: u64,
    pub forward_errors_total: u64,
    pub forward_send_errors_total: u64,
    pub forward_queue_enqueued_total: u64,
    pub forward_queue_dropped_total: u64,
    pub forward_queue_depth: u64,
    pub tvu_socket_rxq_overflow_supported: bool,
    pub tvu_socket_rxq_overflow_total: Option<u64>,
    pub tracked_sources: usize,
    pub latest_slot: u64,
    pub seconds_since_last_packet: Option<u64>,
    pub seconds_since_last_shred: Option<u64>,
    pub seconds_since_last_forward: Option<u64>,
    pub seconds_since_last_forward_error: Option<u64>,
    pub repair_enabled: bool,
    pub repair_active: bool,
    pub repair_state: RepairState,
    pub repair_last_error: Option<String>,
    pub repair_restart_count: u64,
    pub repair_last_success_unix_ms: Option<u64>,
    pub seconds_since_repair_success: Option<u64>,
    pub repair_peers: u64,
    pub repair_tracked_slots: u64,
    pub repair_outstanding: u64,
    pub repair_observation_queue_dropped_total: u64,
    pub repair_socket_datagrams_received_total: u64,
    pub repair_response_datagrams_processed_total: u64,
    pub repair_socket_requested_recv_buffer_bytes: u64,
    pub repair_socket_effective_recv_buffer_bytes: u64,
    pub repair_socket_rxq_overflow_supported: bool,
    pub repair_socket_rxq_overflow_total: Option<u64>,
    pub repair_response_queue_capacity: u64,
    pub repair_response_queue_depth: u64,
    pub repair_response_queue_dropped_total: u64,
    pub repair_requests_sent_total: u64,
    pub repair_retries_sent_total: u64,
    pub repair_requests_exhausted_total: u64,
    pub repair_requests_cooldown_deferred_total: u64,
    pub repair_packets_rejected_total: u64,
    pub repair_pings_answered_total: u64,
    pub repair_shreds_accepted_total: u64,
    pub repair_root_anchored_shreds_accepted_total: u64,
    /// Compatibility alias retained from the single-file implementation.
    pub repair_wal_bytes_total: u64,
    pub repair_wal_retained_bytes: u64,
    /// Per-segment rotation target retained under the legacy field name.
    pub repair_wal_max_bytes: u64,
    /// Remaining target bytes in the active segment, not free space across all retained segments.
    pub repair_wal_remaining_bytes: u64,
    pub repair_wal_active_segment_bytes: u64,
    pub repair_wal_segment_count: u64,
    pub repair_wal_active_segment_id: u64,
    pub repair_wal_rollovers_total: u64,
    pub repair_wal_durable_through_sequence: Option<u64>,
    pub repair_wal_total_warning_bytes: u64,
    pub repair_wal_total_critical_bytes: u64,
    pub repair_wal_total_hard_bytes: u64,
    pub repair_wal_filesystem_reserve_bytes: u64,
    pub repair_wal_filesystem_available_bytes: Option<u64>,
    pub repair_wal_v3_sealed: bool,
    pub repair_wal_total_warning: bool,
    pub repair_wal_total_critical: bool,
    pub repair_wal_total_hard: bool,
    pub repair_wal_filesystem_reserve_breached: bool,
    pub repair_wal_admission_blocked: bool,
    pub repair_wal_last_error: Option<String>,
    pub repair_wal_syncs_total: u64,
    pub repair_errors_total: u64,
}

#[derive(Clone)]
pub struct ServiceState {
    pub metrics: Arc<Metrics>,
    pub cluster_info: Arc<ClusterInfo>,
    pub identity: String,
    pub advertised_ip: String,
    pub gossip_port: u16,
    pub tvu_port: u16,
    pub shred_version: u16,
    pub forward_targets: usize,
    pub forward_sender_addr: Option<String>,
    pub repair_enabled: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RepairMetricsUpdate {
    pub active: bool,
    pub peers: usize,
    pub tracked_slots: usize,
    pub outstanding: usize,
    pub socket_datagrams_received: u64,
    pub response_datagrams_processed: u64,
    pub socket_requested_recv_buffer_bytes: u64,
    pub socket_effective_recv_buffer_bytes: u64,
    pub socket_rxq_overflow_supported: bool,
    pub socket_rxq_overflow: u64,
    pub response_queue_capacity: u64,
    pub response_queue_depth: u64,
    pub response_queue_dropped: u64,
    pub requests_sent: u64,
    pub retries_sent: u64,
    pub requests_exhausted: u64,
    pub requests_cooldown_deferred: u64,
    pub packets_rejected: u64,
    pub pings_answered: u64,
    pub shreds_accepted: u64,
    pub root_anchored_shreds_accepted: u64,
    pub wal_bytes: u64,
    pub wal_max_bytes: u64,
    pub wal_active_segment_bytes: u64,
    pub wal_segment_count: u64,
    pub wal_active_segment_id: u64,
    pub wal_rollovers: u64,
    pub wal_durable_through_sequence: Option<u64>,
    pub wal_total_warning_bytes: u64,
    pub wal_total_critical_bytes: u64,
    pub wal_total_hard_bytes: u64,
    pub wal_filesystem_reserve_bytes: u64,
    pub wal_filesystem_available_bytes: Option<u64>,
    pub wal_v3_sealed: bool,
    pub wal_syncs: u64,
}

impl Metrics {
    pub fn new(dedup_capacity: usize) -> anyhow::Result<Self> {
        Ok(Self {
            started_at: Instant::now(),
            packets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            parsed: AtomicU64::new(0),
            invalid: AtomicU64::new(0),
            version_mismatch: AtomicU64::new(0),
            unique: AtomicU64::new(0),
            duplicates: AtomicU64::new(0),
            data: AtomicU64::new(0),
            code: AtomicU64::new(0),
            forwarded: AtomicU64::new(0),
            forward_send_errors: AtomicU64::new(0),
            forward_queue_enqueued: AtomicU64::new(0),
            forward_queue_dropped: AtomicU64::new(0),
            forward_queue_depth: AtomicU64::new(0),
            tvu_socket_rxq_overflow_supported: AtomicU64::new(0),
            tvu_socket_rxq_overflow: AtomicU64::new(0),
            latest_slot: AtomicU64::new(0),
            last_packet_unix_ms: AtomicU64::new(0),
            last_shred_unix_ms: AtomicU64::new(0),
            last_forward_unix_ms: AtomicU64::new(0),
            last_forward_error_unix_ms: AtomicU64::new(0),
            repair_active: AtomicU64::new(0),
            repair_peers: AtomicU64::new(0),
            repair_tracked_slots: AtomicU64::new(0),
            repair_outstanding: AtomicU64::new(0),
            repair_observation_queue_dropped: AtomicU64::new(0),
            repair_socket_datagrams_received: AtomicU64::new(0),
            repair_response_datagrams_processed: AtomicU64::new(0),
            repair_socket_requested_recv_buffer_bytes: AtomicU64::new(0),
            repair_socket_effective_recv_buffer_bytes: AtomicU64::new(0),
            repair_socket_rxq_overflow_supported: AtomicU64::new(0),
            repair_socket_rxq_overflow: AtomicU64::new(0),
            repair_response_queue_capacity: AtomicU64::new(0),
            repair_response_queue_depth: AtomicU64::new(0),
            repair_response_queue_dropped: AtomicU64::new(0),
            repair_requests_sent: AtomicU64::new(0),
            repair_retries_sent: AtomicU64::new(0),
            repair_requests_exhausted: AtomicU64::new(0),
            repair_requests_cooldown_deferred: AtomicU64::new(0),
            repair_packets_rejected: AtomicU64::new(0),
            repair_pings_answered: AtomicU64::new(0),
            repair_shreds_accepted: AtomicU64::new(0),
            repair_root_anchored_shreds_accepted: AtomicU64::new(0),
            repair_wal_bytes: AtomicU64::new(0),
            repair_wal_max_bytes: AtomicU64::new(0),
            repair_wal_active_segment_bytes: AtomicU64::new(0),
            repair_wal_segment_count: AtomicU64::new(0),
            repair_wal_active_segment_id: AtomicU64::new(0),
            repair_wal_rollovers: AtomicU64::new(0),
            repair_wal_durable_through_sequence: AtomicU64::new(NO_DURABLE_REPAIR_SEQUENCE),
            repair_wal_total_warning_bytes: AtomicU64::new(0),
            repair_wal_total_critical_bytes: AtomicU64::new(0),
            repair_wal_total_hard_bytes: AtomicU64::new(0),
            repair_wal_filesystem_reserve_bytes: AtomicU64::new(0),
            repair_wal_filesystem_available_bytes: AtomicU64::new(NO_FILESYSTEM_AVAILABLE_BYTES),
            repair_wal_v3_sealed: AtomicU64::new(0),
            repair_wal_syncs: AtomicU64::new(0),
            repair_errors: AtomicU64::new(0),
            repair_restart_count: AtomicU64::new(0),
            repair_last_success_unix_ms: AtomicU64::new(0),
            state: Mutex::new(ReceiverState {
                dedup: LruCache::new(nonzero_usize(dedup_capacity, "DEDUP_CAPACITY")?),
                sources: LruCache::new(nonzero_usize(
                    SOURCE_CACHE_CAPACITY,
                    "source cache capacity",
                )?),
                repair_state: RepairState::Disabled,
                repair_last_error: None,
                repair_wal_last_error: None,
                repair_counter_samples: RepairCounterSamples::default(),
            }),
        })
    }

    pub fn record_packet(&self, size: usize, source: IpAddr) {
        self.packets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(size as u64, Ordering::Relaxed);
        self.last_packet_unix_ms
            .store(unix_millis(), Ordering::Relaxed);
        let _ = self.state.lock().unwrap().sources.put(source, ());
    }

    pub fn record_invalid(&self) {
        self.invalid.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_version_mismatch(&self) {
        self.version_mismatch.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a structurally valid shred and returns whether this shred ID is new within the
    /// bounded deduplication window.
    pub fn record_shred(&self, id: ShredId, is_data: bool) -> bool {
        self.parsed.fetch_add(1, Ordering::Relaxed);
        self.latest_slot.fetch_max(id.slot(), Ordering::Relaxed);
        self.last_shred_unix_ms
            .store(unix_millis(), Ordering::Relaxed);
        if is_data {
            self.data.fetch_add(1, Ordering::Relaxed);
        } else {
            self.code.fetch_add(1, Ordering::Relaxed);
        }

        let duplicate = self.state.lock().unwrap().dedup.put(id, ()).is_some();
        if duplicate {
            self.duplicates.fetch_add(1, Ordering::Relaxed);
        } else {
            self.unique.fetch_add(1, Ordering::Relaxed);
        }
        !duplicate
    }

    pub fn record_forwarded(&self) {
        self.forwarded.fetch_add(1, Ordering::Relaxed);
        self.last_forward_unix_ms
            .store(unix_millis(), Ordering::Relaxed);
    }

    pub fn record_forward_send_error(&self) {
        self.forward_send_errors.fetch_add(1, Ordering::Relaxed);
        self.last_forward_error_unix_ms
            .store(unix_millis(), Ordering::Relaxed);
    }

    pub fn record_forward_queued(&self) {
        self.forward_queue_enqueued.fetch_add(1, Ordering::Relaxed);
        self.forward_queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_forward_dequeued(&self) {
        self.forward_queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_forward_queue_drop(&self) {
        self.forward_queue_dropped.fetch_add(1, Ordering::Relaxed);
        self.last_forward_error_unix_ms
            .store(unix_millis(), Ordering::Relaxed);
    }

    pub fn forward_queue_depth(&self) -> u64 {
        self.forward_queue_depth.load(Ordering::Relaxed)
    }

    pub fn set_tvu_socket_rxq_overflow_supported(&self, supported: bool) {
        self.tvu_socket_rxq_overflow_supported
            .store(u64::from(supported), Ordering::Relaxed);
    }

    pub fn record_tvu_socket_rxq_overflow(&self, dropped: u64) {
        self.tvu_socket_rxq_overflow
            .fetch_add(dropped, Ordering::Relaxed);
    }

    pub fn record_repair_observation_queue_drop(&self) {
        self.record_repair_observation_queue_drops(1);
    }

    pub fn record_repair_observation_queue_drops(&self, dropped: u64) {
        self.repair_observation_queue_dropped
            .fetch_add(dropped, Ordering::Relaxed);
    }

    pub fn record_repair_error(&self) {
        self.repair_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_repair_inactive(&self) {
        self.state.lock().unwrap().repair_state = RepairState::Inactive;
        self.reset_repair_active_gauges();
    }

    pub fn mark_repair_starting(&self) {
        let mut state = self.state.lock().unwrap();
        state.repair_state = RepairState::Starting;
        state.repair_counter_samples = RepairCounterSamples::default();
        drop(state);
        self.reset_repair_active_gauges();
    }

    pub fn mark_repair_backoff(&self, error: &str) {
        let mut state = self.state.lock().unwrap();
        state.repair_state = RepairState::Backoff;
        state.repair_last_error = Some(bounded_error(error));
        drop(state);
        self.repair_restart_count.fetch_add(1, Ordering::Relaxed);
        self.repair_errors.fetch_add(1, Ordering::Relaxed);
        self.reset_repair_active_gauges();
    }

    pub fn mark_repair_stopping(&self) {
        self.state.lock().unwrap().repair_state = RepairState::Stopping;
        self.reset_repair_active_gauges();
    }

    pub fn record_repair_runtime_error(&self, error: &str) {
        self.state.lock().unwrap().repair_last_error = Some(bounded_error(error));
        self.repair_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_repair_wal_error(&self, error: Option<&str>) {
        self.state.lock().unwrap().repair_wal_last_error = error.map(bounded_error);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn initialize_repair_wal_storage(
        &self,
        wal_max_bytes: u64,
        warning_bytes: u64,
        critical_bytes: u64,
        hard_bytes: u64,
        reserve_bytes: u64,
        inspection: Option<&RepairWalInspection>,
        discovery_error: Option<&str>,
    ) {
        self.repair_wal_max_bytes
            .store(wal_max_bytes, Ordering::Relaxed);
        self.repair_wal_total_warning_bytes
            .store(warning_bytes, Ordering::Relaxed);
        self.repair_wal_total_critical_bytes
            .store(critical_bytes, Ordering::Relaxed);
        self.repair_wal_total_hard_bytes
            .store(hard_bytes, Ordering::Relaxed);
        self.repair_wal_filesystem_reserve_bytes
            .store(reserve_bytes, Ordering::Relaxed);
        if let Some(inspection) = inspection {
            self.repair_wal_bytes
                .store(inspection.retained_bytes, Ordering::Relaxed);
            self.repair_wal_active_segment_bytes
                .store(inspection.active_segment_bytes, Ordering::Relaxed);
            self.repair_wal_segment_count
                .store(inspection.segment_count, Ordering::Relaxed);
            self.repair_wal_active_segment_id
                .store(inspection.active_segment_id, Ordering::Relaxed);
            self.repair_wal_filesystem_available_bytes.store(
                inspection
                    .filesystem_available_bytes
                    .unwrap_or(NO_FILESYSTEM_AVAILABLE_BYTES),
                Ordering::Relaxed,
            );
            self.repair_wal_v3_sealed
                .store(u64::from(inspection.v3_sealed), Ordering::Relaxed);
        }
        self.set_repair_wal_error(
            discovery_error
                .or_else(|| inspection.and_then(|value| value.validation_error.as_deref())),
        );
    }

    fn reset_repair_active_gauges(&self) {
        self.repair_active.store(0, Ordering::Relaxed);
        self.repair_peers.store(0, Ordering::Relaxed);
        self.repair_tracked_slots.store(0, Ordering::Relaxed);
        self.repair_outstanding.store(0, Ordering::Relaxed);
        self.repair_response_queue_depth.store(0, Ordering::Relaxed);
    }

    pub fn update_repair(&self, update: RepairMetricsUpdate) {
        let delta = {
            let mut state = self.state.lock().unwrap();
            let delta = state.repair_counter_samples.take_delta(&update);
            if update.active {
                state.repair_state = RepairState::Active;
                state.repair_last_error = None;
            }
            delta
        };
        self.repair_active
            .store(u64::from(update.active), Ordering::Relaxed);
        // RepairRuntime increments `shreds_accepted` only after RepairWalWorker acknowledges the
        // append, and that ACK is issued only after the record is durable. Merely running the
        // repair loop is not a successful repair.
        if delta.shreds_accepted != 0 {
            self.repair_last_success_unix_ms
                .store(unix_millis(), Ordering::Relaxed);
        }
        self.repair_peers
            .store(update.peers as u64, Ordering::Relaxed);
        self.repair_tracked_slots
            .store(update.tracked_slots as u64, Ordering::Relaxed);
        self.repair_outstanding
            .store(update.outstanding as u64, Ordering::Relaxed);
        self.repair_socket_datagrams_received
            .fetch_add(delta.socket_datagrams_received, Ordering::Relaxed);
        self.repair_response_datagrams_processed
            .fetch_add(delta.response_datagrams_processed, Ordering::Relaxed);
        self.repair_socket_requested_recv_buffer_bytes
            .store(update.socket_requested_recv_buffer_bytes, Ordering::Relaxed);
        self.repair_socket_effective_recv_buffer_bytes
            .store(update.socket_effective_recv_buffer_bytes, Ordering::Relaxed);
        self.repair_socket_rxq_overflow_supported.store(
            u64::from(update.socket_rxq_overflow_supported),
            Ordering::Relaxed,
        );
        self.repair_socket_rxq_overflow
            .fetch_add(delta.socket_rxq_overflow, Ordering::Relaxed);
        self.repair_response_queue_capacity
            .store(update.response_queue_capacity, Ordering::Relaxed);
        self.repair_response_queue_depth
            .store(update.response_queue_depth, Ordering::Relaxed);
        self.repair_response_queue_dropped
            .fetch_add(delta.response_queue_dropped, Ordering::Relaxed);
        self.repair_requests_sent
            .fetch_add(delta.requests_sent, Ordering::Relaxed);
        self.repair_retries_sent
            .fetch_add(delta.retries_sent, Ordering::Relaxed);
        self.repair_requests_exhausted
            .fetch_add(delta.requests_exhausted, Ordering::Relaxed);
        self.repair_requests_cooldown_deferred
            .fetch_add(delta.requests_cooldown_deferred, Ordering::Relaxed);
        self.repair_packets_rejected
            .fetch_add(delta.packets_rejected, Ordering::Relaxed);
        self.repair_pings_answered
            .fetch_add(delta.pings_answered, Ordering::Relaxed);
        self.repair_shreds_accepted
            .fetch_add(delta.shreds_accepted, Ordering::Relaxed);
        self.repair_root_anchored_shreds_accepted
            .fetch_add(delta.root_anchored_shreds_accepted, Ordering::Relaxed);
        self.repair_wal_bytes
            .store(update.wal_bytes, Ordering::Relaxed);
        self.repair_wal_max_bytes
            .store(update.wal_max_bytes, Ordering::Relaxed);
        self.repair_wal_active_segment_bytes
            .store(update.wal_active_segment_bytes, Ordering::Relaxed);
        self.repair_wal_segment_count
            .store(update.wal_segment_count, Ordering::Relaxed);
        self.repair_wal_active_segment_id
            .store(update.wal_active_segment_id, Ordering::Relaxed);
        self.repair_wal_rollovers
            .fetch_add(delta.wal_rollovers, Ordering::Relaxed);
        self.repair_wal_durable_through_sequence.store(
            update
                .wal_durable_through_sequence
                .unwrap_or(NO_DURABLE_REPAIR_SEQUENCE),
            Ordering::Relaxed,
        );
        self.repair_wal_total_warning_bytes
            .store(update.wal_total_warning_bytes, Ordering::Relaxed);
        self.repair_wal_total_critical_bytes
            .store(update.wal_total_critical_bytes, Ordering::Relaxed);
        self.repair_wal_total_hard_bytes
            .store(update.wal_total_hard_bytes, Ordering::Relaxed);
        self.repair_wal_filesystem_reserve_bytes
            .store(update.wal_filesystem_reserve_bytes, Ordering::Relaxed);
        self.repair_wal_filesystem_available_bytes.store(
            update
                .wal_filesystem_available_bytes
                .unwrap_or(NO_FILESYSTEM_AVAILABLE_BYTES),
            Ordering::Relaxed,
        );
        self.repair_wal_v3_sealed
            .store(u64::from(update.wal_v3_sealed), Ordering::Relaxed);
        self.repair_wal_syncs
            .fetch_add(delta.wal_syncs, Ordering::Relaxed);
    }

    pub fn snapshot(&self, service: &ServiceState) -> Snapshot {
        let now = unix_millis();
        let last_packet = self.last_packet_unix_ms.load(Ordering::Relaxed);
        let last_shred = self.last_shred_unix_ms.load(Ordering::Relaxed);
        let last_forward = self.last_forward_unix_ms.load(Ordering::Relaxed);
        let last_forward_error = self.last_forward_error_unix_ms.load(Ordering::Relaxed);
        let forward_send_errors = self.forward_send_errors.load(Ordering::Relaxed);
        let forward_queue_dropped = self.forward_queue_dropped.load(Ordering::Relaxed);
        let tvu_socket_rxq_overflow_supported = self
            .tvu_socket_rxq_overflow_supported
            .load(Ordering::Relaxed)
            != 0;
        let repair_socket_rxq_overflow_supported = self
            .repair_socket_rxq_overflow_supported
            .load(Ordering::Relaxed)
            != 0;
        let repair_wal_bytes = self.repair_wal_bytes.load(Ordering::Relaxed);
        let repair_wal_max_bytes = self.repair_wal_max_bytes.load(Ordering::Relaxed);
        let repair_wal_active_segment_bytes =
            self.repair_wal_active_segment_bytes.load(Ordering::Relaxed);
        let repair_wal_total_warning_bytes =
            self.repair_wal_total_warning_bytes.load(Ordering::Relaxed);
        let repair_wal_total_critical_bytes =
            self.repair_wal_total_critical_bytes.load(Ordering::Relaxed);
        let repair_wal_total_hard_bytes = self.repair_wal_total_hard_bytes.load(Ordering::Relaxed);
        let repair_wal_filesystem_reserve_bytes = self
            .repair_wal_filesystem_reserve_bytes
            .load(Ordering::Relaxed);
        let repair_wal_filesystem_available_raw = self
            .repair_wal_filesystem_available_bytes
            .load(Ordering::Relaxed);
        let repair_wal_filesystem_available_bytes = (repair_wal_filesystem_available_raw
            != NO_FILESYSTEM_AVAILABLE_BYTES)
            .then_some(repair_wal_filesystem_available_raw);
        let repair_wal_durable_through_sequence = self
            .repair_wal_durable_through_sequence
            .load(Ordering::Relaxed);
        let repair_last_success_unix_ms = self.repair_last_success_unix_ms.load(Ordering::Relaxed);
        let (tracked_sources, repair_state, repair_last_error, repair_wal_last_error) = {
            let state = self.state.lock().unwrap();
            (
                state.sources.len(),
                state.repair_state,
                state.repair_last_error.clone(),
                state.repair_wal_last_error.clone(),
            )
        };
        let repair_wal_total_hard =
            repair_wal_total_hard_bytes != 0 && repair_wal_bytes >= repair_wal_total_hard_bytes;
        let repair_wal_filesystem_reserve_breached = repair_wal_filesystem_available_bytes
            .is_some_and(|available| available <= repair_wal_filesystem_reserve_bytes);
        Snapshot {
            identity: service.identity.clone(),
            advertised_ip: service.advertised_ip.clone(),
            gossip_port: service.gossip_port,
            tvu_port: service.tvu_port,
            shred_version: service.shred_version,
            uptime_seconds: self.started_at.elapsed().as_secs(),
            gossip_peers: service.cluster_info.gossip_peers().len(),
            recent_gossip_peers: recent_gossip_peer_count(&service.cluster_info),
            tvu_peers: service.cluster_info.tvu_peers(|_| ()).len(),
            packets_total: self.packets.load(Ordering::Relaxed),
            bytes_total: self.bytes.load(Ordering::Relaxed),
            parsed_total: self.parsed.load(Ordering::Relaxed),
            invalid_total: self.invalid.load(Ordering::Relaxed),
            version_mismatch_total: self.version_mismatch.load(Ordering::Relaxed),
            unique_total: self.unique.load(Ordering::Relaxed),
            duplicates_total: self.duplicates.load(Ordering::Relaxed),
            data_total: self.data.load(Ordering::Relaxed),
            code_total: self.code.load(Ordering::Relaxed),
            forward_targets: service.forward_targets,
            forward_sender_addr: service.forward_sender_addr.clone(),
            forwarded_datagrams_total: self.forwarded.load(Ordering::Relaxed),
            forward_errors_total: forward_send_errors.saturating_add(forward_queue_dropped),
            forward_send_errors_total: forward_send_errors,
            forward_queue_enqueued_total: self.forward_queue_enqueued.load(Ordering::Relaxed),
            forward_queue_dropped_total: forward_queue_dropped,
            forward_queue_depth: self.forward_queue_depth(),
            tvu_socket_rxq_overflow_supported,
            tvu_socket_rxq_overflow_total: tvu_socket_rxq_overflow_supported
                .then(|| self.tvu_socket_rxq_overflow.load(Ordering::Relaxed)),
            tracked_sources,
            latest_slot: self.latest_slot.load(Ordering::Relaxed),
            seconds_since_last_packet: (last_packet != 0)
                .then(|| now.saturating_sub(last_packet) / 1_000),
            seconds_since_last_shred: (last_shred != 0)
                .then(|| now.saturating_sub(last_shred) / 1_000),
            seconds_since_last_forward: (last_forward != 0)
                .then(|| now.saturating_sub(last_forward) / 1_000),
            seconds_since_last_forward_error: (last_forward_error != 0)
                .then(|| now.saturating_sub(last_forward_error) / 1_000),
            repair_enabled: service.repair_enabled,
            repair_active: self.repair_active.load(Ordering::Relaxed) != 0,
            repair_state: if service.repair_enabled {
                repair_state
            } else {
                RepairState::Disabled
            },
            repair_last_error,
            repair_restart_count: self.repair_restart_count.load(Ordering::Relaxed),
            repair_last_success_unix_ms: (repair_last_success_unix_ms != 0)
                .then_some(repair_last_success_unix_ms),
            seconds_since_repair_success: (repair_last_success_unix_ms != 0)
                .then(|| now.saturating_sub(repair_last_success_unix_ms) / 1_000),
            repair_peers: self.repair_peers.load(Ordering::Relaxed),
            repair_tracked_slots: self.repair_tracked_slots.load(Ordering::Relaxed),
            repair_outstanding: self.repair_outstanding.load(Ordering::Relaxed),
            repair_observation_queue_dropped_total: self
                .repair_observation_queue_dropped
                .load(Ordering::Relaxed),
            repair_socket_datagrams_received_total: self
                .repair_socket_datagrams_received
                .load(Ordering::Relaxed),
            repair_response_datagrams_processed_total: self
                .repair_response_datagrams_processed
                .load(Ordering::Relaxed),
            repair_socket_requested_recv_buffer_bytes: self
                .repair_socket_requested_recv_buffer_bytes
                .load(Ordering::Relaxed),
            repair_socket_effective_recv_buffer_bytes: self
                .repair_socket_effective_recv_buffer_bytes
                .load(Ordering::Relaxed),
            repair_socket_rxq_overflow_supported,
            repair_socket_rxq_overflow_total: repair_socket_rxq_overflow_supported
                .then(|| self.repair_socket_rxq_overflow.load(Ordering::Relaxed)),
            repair_response_queue_capacity: self
                .repair_response_queue_capacity
                .load(Ordering::Relaxed),
            repair_response_queue_depth: self.repair_response_queue_depth.load(Ordering::Relaxed),
            repair_response_queue_dropped_total: self
                .repair_response_queue_dropped
                .load(Ordering::Relaxed),
            repair_requests_sent_total: self.repair_requests_sent.load(Ordering::Relaxed),
            repair_retries_sent_total: self.repair_retries_sent.load(Ordering::Relaxed),
            repair_requests_exhausted_total: self.repair_requests_exhausted.load(Ordering::Relaxed),
            repair_requests_cooldown_deferred_total: self
                .repair_requests_cooldown_deferred
                .load(Ordering::Relaxed),
            repair_packets_rejected_total: self.repair_packets_rejected.load(Ordering::Relaxed),
            repair_pings_answered_total: self.repair_pings_answered.load(Ordering::Relaxed),
            repair_shreds_accepted_total: self.repair_shreds_accepted.load(Ordering::Relaxed),
            repair_root_anchored_shreds_accepted_total: self
                .repair_root_anchored_shreds_accepted
                .load(Ordering::Relaxed),
            repair_wal_bytes_total: repair_wal_bytes,
            repair_wal_retained_bytes: repair_wal_bytes,
            repair_wal_max_bytes,
            repair_wal_remaining_bytes: repair_wal_max_bytes
                .saturating_sub(repair_wal_active_segment_bytes),
            repair_wal_active_segment_bytes,
            repair_wal_segment_count: self.repair_wal_segment_count.load(Ordering::Relaxed),
            repair_wal_active_segment_id: self.repair_wal_active_segment_id.load(Ordering::Relaxed),
            repair_wal_rollovers_total: self.repair_wal_rollovers.load(Ordering::Relaxed),
            repair_wal_durable_through_sequence: (repair_wal_durable_through_sequence
                != NO_DURABLE_REPAIR_SEQUENCE)
                .then_some(repair_wal_durable_through_sequence),
            repair_wal_total_warning_bytes,
            repair_wal_total_critical_bytes,
            repair_wal_total_hard_bytes,
            repair_wal_filesystem_reserve_bytes,
            repair_wal_filesystem_available_bytes,
            repair_wal_v3_sealed: self.repair_wal_v3_sealed.load(Ordering::Relaxed) != 0,
            repair_wal_total_warning: repair_wal_total_warning_bytes != 0
                && repair_wal_bytes >= repair_wal_total_warning_bytes,
            repair_wal_total_critical: repair_wal_total_critical_bytes != 0
                && repair_wal_bytes >= repair_wal_total_critical_bytes,
            repair_wal_total_hard,
            repair_wal_filesystem_reserve_breached,
            repair_wal_admission_blocked: repair_wal_total_hard
                || repair_wal_filesystem_reserve_breached
                || repair_wal_last_error.is_some(),
            repair_wal_last_error,
            repair_wal_syncs_total: self.repair_wal_syncs.load(Ordering::Relaxed),
            repair_errors_total: self.repair_errors.load(Ordering::Relaxed),
        }
    }

    fn has_recent_shred(&self) -> bool {
        let last_shred = self.last_shred_unix_ms.load(Ordering::Relaxed);
        last_shred != 0
            && unix_millis().saturating_sub(last_shred) <= RECENT_SHRED_WINDOW.as_millis() as u64
    }

    fn has_recent_error_free_forward(&self) -> bool {
        let now = unix_millis();
        let last_forward = self.last_forward_unix_ms.load(Ordering::Relaxed);
        let last_error = self.last_forward_error_unix_ms.load(Ordering::Relaxed);
        last_forward != 0
            && now.saturating_sub(last_forward) <= RECENT_FORWARD_WINDOW.as_millis() as u64
            && (last_error == 0
                || now.saturating_sub(last_error) > RECENT_FORWARD_ERROR_WINDOW.as_millis() as u64)
    }
}

pub async fn serve(
    listener: TcpListener,
    state: ServiceState,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(snapshot))
        .with_state(state);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            if !*shutdown.borrow() {
                let _ = shutdown.changed().await;
            }
        })
        .await?;
    Ok(())
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn ready(axum::extract::State(state): axum::extract::State<ServiceState>) -> StatusCode {
    readiness_status(
        recent_gossip_peer_count(&state.cluster_info),
        state.metrics.has_recent_shred(),
        state.forward_targets,
        state.metrics.has_recent_error_free_forward(),
    )
}

fn readiness_status(
    recent_gossip_peers: usize,
    has_recent_shred: bool,
    forward_targets: usize,
    has_recent_forward: bool,
) -> StatusCode {
    if recent_gossip_peers == 0 || !has_recent_shred || forward_targets == 0 || !has_recent_forward
    {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

fn recent_gossip_peer_count(cluster_info: &ClusterInfo) -> usize {
    let now = unix_millis();
    let cutoff = now.saturating_sub(RECENT_GOSSIP_PEER_WINDOW.as_millis() as u64);
    let self_id = cluster_info.id();
    cluster_info
        .all_peers()
        .into_iter()
        .filter(|(peer, local_timestamp)| {
            peer.pubkey() != &self_id && peer.gossip().is_some() && *local_timestamp >= cutoff
        })
        .count()
}

async fn snapshot(
    axum::extract::State(state): axum::extract::State<ServiceState>,
) -> Json<Snapshot> {
    Json(state.metrics.snapshot(&state))
}

fn bounded_error(error: &str) -> String {
    let mut end = error.len().min(MAX_REPAIR_ERROR_BYTES);
    while !error.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    error[..end].to_owned()
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_ledger::shred::ShredType;

    #[test]
    fn metrics_requires_a_nonempty_dedup_cache() {
        assert!(Metrics::new(0).is_err());
        assert!(Metrics::new(1).is_ok());
    }

    #[test]
    fn record_shred_reports_only_the_first_observation_as_unique() {
        let metrics = Metrics::new(16).unwrap();
        let id = ShredId::new(42, 7, ShredType::Data);

        assert!(metrics.record_shred(id, true));
        assert!(!metrics.record_shred(id, true));
        assert_eq!(metrics.unique.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.duplicates.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn records_recent_successful_forward() {
        let metrics = Metrics::new(16).unwrap();
        assert!(!metrics.has_recent_error_free_forward());

        metrics.record_forwarded();
        assert!(metrics.has_recent_error_free_forward());

        metrics.last_forward_unix_ms.store(
            unix_millis().saturating_sub(RECENT_FORWARD_WINDOW.as_millis() as u64 + 1),
            Ordering::Relaxed,
        );
        assert!(!metrics.has_recent_error_free_forward());
    }

    #[test]
    fn recent_forward_error_makes_readiness_unhealthy() {
        let metrics = Metrics::new(16).unwrap();
        metrics.record_forwarded();
        metrics.record_forward_queue_drop();

        assert!(!metrics.has_recent_error_free_forward());

        metrics.last_forward_error_unix_ms.store(
            unix_millis().saturating_sub(RECENT_FORWARD_ERROR_WINDOW.as_millis() as u64 + 1),
            Ordering::Relaxed,
        );
        assert!(metrics.has_recent_error_free_forward());
    }

    #[test]
    fn queue_metrics_separate_accepted_depth_and_drops() {
        let metrics = Metrics::new(16).unwrap();

        metrics.record_forward_queued();
        assert_eq!(metrics.forward_queue_enqueued.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.forward_queue_depth(), 1);

        metrics.record_forward_dequeued();
        assert_eq!(metrics.forward_queue_depth(), 0);

        metrics.record_forward_queue_drop();
        assert_eq!(metrics.forward_queue_dropped.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.forward_queue_enqueued.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn residual_attempt_observation_drops_can_be_counted_in_bulk() {
        let metrics = Metrics::new(16).unwrap();
        metrics.record_repair_observation_queue_drop();
        metrics.record_repair_observation_queue_drops(7);

        assert_eq!(
            metrics
                .repair_observation_queue_dropped
                .load(Ordering::Relaxed),
            8
        );
    }

    #[test]
    fn tvu_socket_overflow_support_and_counter_remain_separate() {
        let metrics = Metrics::new(16).unwrap();
        assert_eq!(
            metrics
                .tvu_socket_rxq_overflow_supported
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(metrics.tvu_socket_rxq_overflow.load(Ordering::Relaxed), 0);

        metrics.set_tvu_socket_rxq_overflow_supported(true);
        metrics.record_tvu_socket_rxq_overflow(7);
        metrics.record_tvu_socket_rxq_overflow(5);

        assert_eq!(
            metrics
                .tvu_socket_rxq_overflow_supported
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(metrics.tvu_socket_rxq_overflow.load(Ordering::Relaxed), 12);
    }

    #[test]
    fn repair_socket_gauges_and_attempt_counters_have_unambiguous_units() {
        let metrics = Metrics::new(16).unwrap();
        metrics.mark_repair_starting();
        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            socket_datagrams_received: 12,
            response_datagrams_processed: 9,
            socket_requested_recv_buffer_bytes: 64 * 1_024 * 1_024,
            socket_effective_recv_buffer_bytes: 15_000_000,
            socket_rxq_overflow_supported: true,
            socket_rxq_overflow: 2,
            response_queue_capacity: 4_096,
            response_queue_depth: 3,
            response_queue_dropped: 1,
            ..RepairMetricsUpdate::default()
        });
        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            socket_datagrams_received: 15,
            response_datagrams_processed: 13,
            socket_requested_recv_buffer_bytes: 64 * 1_024 * 1_024,
            socket_effective_recv_buffer_bytes: 15_000_000,
            socket_rxq_overflow_supported: true,
            socket_rxq_overflow: 5,
            response_queue_capacity: 4_096,
            response_queue_depth: 2,
            response_queue_dropped: 2,
            ..RepairMetricsUpdate::default()
        });

        assert_eq!(
            metrics
                .repair_socket_datagrams_received
                .load(Ordering::Relaxed),
            15
        );
        assert_eq!(
            metrics
                .repair_response_datagrams_processed
                .load(Ordering::Relaxed),
            13
        );
        assert_eq!(
            metrics
                .repair_socket_effective_recv_buffer_bytes
                .load(Ordering::Relaxed),
            15_000_000
        );
        assert_eq!(
            metrics.repair_socket_rxq_overflow.load(Ordering::Relaxed),
            5
        );
        assert_eq!(
            metrics
                .repair_response_queue_dropped
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            metrics.repair_response_queue_depth.load(Ordering::Relaxed),
            2
        );

        metrics.mark_repair_backoff("test restart");
        metrics.mark_repair_starting();
        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            socket_datagrams_received: 4,
            response_datagrams_processed: 3,
            socket_requested_recv_buffer_bytes: 64 * 1_024 * 1_024,
            socket_effective_recv_buffer_bytes: 32_000_000,
            socket_rxq_overflow_supported: true,
            socket_rxq_overflow: 1,
            response_queue_capacity: 4_096,
            response_queue_depth: 1,
            response_queue_dropped: 1,
            ..RepairMetricsUpdate::default()
        });
        assert_eq!(
            metrics
                .repair_socket_datagrams_received
                .load(Ordering::Relaxed),
            19,
            "socket counters must remain monotonic across supervised socket replacement"
        );
        assert_eq!(
            metrics.repair_socket_rxq_overflow.load(Ordering::Relaxed),
            6
        );
        assert_eq!(
            metrics
                .repair_response_queue_dropped
                .load(Ordering::Relaxed),
            3
        );
        assert_eq!(
            metrics
                .repair_socket_effective_recv_buffer_bytes
                .load(Ordering::Relaxed),
            32_000_000,
            "effective buffer is a current-socket gauge, not an accumulated counter"
        );
    }

    #[test]
    fn repair_wal_metrics_keep_retained_and_active_bytes_distinct() {
        let metrics = Metrics::new(16).unwrap();
        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            wal_bytes: 900,
            wal_max_bytes: 512,
            wal_active_segment_bytes: 388,
            wal_segment_count: 2,
            wal_active_segment_id: 1,
            wal_rollovers: 1,
            wal_durable_through_sequence: Some(42),
            wal_total_warning_bytes: 800,
            wal_total_critical_bytes: 1_000,
            wal_total_hard_bytes: 1_200,
            wal_filesystem_reserve_bytes: 2_000,
            wal_filesystem_available_bytes: Some(3_000),
            wal_v3_sealed: true,
            ..RepairMetricsUpdate::default()
        });

        assert_eq!(metrics.repair_wal_bytes.load(Ordering::Relaxed), 900);
        assert_eq!(
            metrics
                .repair_wal_active_segment_bytes
                .load(Ordering::Relaxed),
            388
        );
        assert_eq!(metrics.repair_wal_segment_count.load(Ordering::Relaxed), 2);
        assert_eq!(
            metrics
                .repair_wal_durable_through_sequence
                .load(Ordering::Relaxed),
            42
        );
        assert_eq!(
            metrics.repair_wal_total_hard_bytes.load(Ordering::Relaxed),
            1_200
        );
        assert_eq!(
            metrics
                .repair_wal_filesystem_available_bytes
                .load(Ordering::Relaxed),
            3_000
        );
        assert_eq!(metrics.repair_wal_v3_sealed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn repair_totals_remain_monotonic_across_supervised_attempts() {
        let metrics = Metrics::new(16).unwrap();
        metrics.mark_repair_starting();
        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            requests_sent: 5,
            shreds_accepted: 3,
            wal_rollovers: 1,
            wal_syncs: 4,
            ..RepairMetricsUpdate::default()
        });
        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            requests_sent: 7,
            shreds_accepted: 4,
            wal_rollovers: 2,
            wal_syncs: 6,
            ..RepairMetricsUpdate::default()
        });
        metrics.mark_repair_backoff("attempt failed");
        metrics.mark_repair_starting();
        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            requests_sent: 2,
            shreds_accepted: 1,
            wal_rollovers: 1,
            wal_syncs: 2,
            ..RepairMetricsUpdate::default()
        });

        assert_eq!(metrics.repair_requests_sent.load(Ordering::Relaxed), 9);
        assert_eq!(metrics.repair_shreds_accepted.load(Ordering::Relaxed), 5);
        assert_eq!(metrics.repair_wal_rollovers.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.repair_wal_syncs.load(Ordering::Relaxed), 8);
        assert_eq!(metrics.repair_restart_count.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.repair_errors.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics.state.lock().unwrap().repair_state,
            RepairState::Active
        );
    }

    #[test]
    fn repair_success_timestamp_advances_only_for_new_durable_accepts() {
        let metrics = Metrics::new(16).unwrap();
        metrics.mark_repair_starting();

        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            ..RepairMetricsUpdate::default()
        });
        assert_eq!(
            metrics.repair_last_success_unix_ms.load(Ordering::Relaxed),
            0,
            "an active repair loop is not itself a successful repair"
        );

        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            shreds_accepted: 1,
            ..RepairMetricsUpdate::default()
        });
        assert_ne!(
            metrics.repair_last_success_unix_ms.load(Ordering::Relaxed),
            0,
            "a newly acknowledged durable repair must record success"
        );

        metrics
            .repair_last_success_unix_ms
            .store(1, Ordering::Relaxed);
        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            shreds_accepted: 1,
            ..RepairMetricsUpdate::default()
        });
        assert_eq!(
            metrics.repair_last_success_unix_ms.load(Ordering::Relaxed),
            1,
            "re-reporting a cumulative accepted count must not refresh success"
        );

        metrics.update_repair(RepairMetricsUpdate {
            active: true,
            shreds_accepted: 2,
            ..RepairMetricsUpdate::default()
        });
        assert!(
            metrics.repair_last_success_unix_ms.load(Ordering::Relaxed) > 1,
            "a later durable acceptance must refresh success"
        );
    }

    #[test]
    fn startup_wal_inspection_populates_alert_gauges_before_activation() {
        let metrics = Metrics::new(16).unwrap();
        let inspection = RepairWalInspection {
            retained_bytes: 1_100,
            active_segment_bytes: 300,
            segment_count: 3,
            active_segment_id: 2,
            filesystem_available_bytes: Some(900),
            v3_sealed: true,
            validation_error: Some("corrupt frame".to_owned()),
        };
        metrics.initialize_repair_wal_storage(
            512,
            800,
            1_000,
            1_200,
            1_000,
            Some(&inspection),
            None,
        );

        assert_eq!(metrics.repair_wal_bytes.load(Ordering::Relaxed), 1_100);
        assert_eq!(metrics.repair_wal_segment_count.load(Ordering::Relaxed), 3);
        assert_eq!(
            metrics
                .repair_wal_filesystem_available_bytes
                .load(Ordering::Relaxed),
            900
        );
        assert_eq!(
            metrics
                .state
                .lock()
                .unwrap()
                .repair_wal_last_error
                .as_deref(),
            Some("corrupt frame")
        );
    }

    #[test]
    fn readiness_requires_a_recent_compatible_shred() {
        let metrics = Metrics::new(16).unwrap();
        assert!(!metrics.has_recent_shred());

        metrics.record_shred(ShredId::new(42, 7, ShredType::Data), true);
        assert!(metrics.has_recent_shred());

        metrics.last_shred_unix_ms.store(
            unix_millis().saturating_sub(RECENT_SHRED_WINDOW.as_millis() as u64 + 1),
            Ordering::Relaxed,
        );
        assert!(!metrics.has_recent_shred());
    }

    #[test]
    fn readiness_requires_both_gossip_and_shred_activity() {
        assert_eq!(
            readiness_status(0, false, 0, false),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            readiness_status(1, false, 1, true),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            readiness_status(0, true, 1, true),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            readiness_status(1, true, 0, false),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(readiness_status(1, true, 1, true), StatusCode::OK);
    }
}
