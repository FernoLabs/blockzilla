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

use crate::config::nonzero_usize;

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
    repair_wal_syncs: AtomicU64,
    repair_errors: AtomicU64,
    state: Mutex<ReceiverState>,
}

struct ReceiverState {
    dedup: LruCache<ShredId, ()>,
    sources: LruCache<IpAddr, ()>,
}

const SOURCE_CACHE_CAPACITY: usize = 65_536;
const RECENT_GOSSIP_PEER_WINDOW: Duration = Duration::from_secs(60);
const RECENT_SHRED_WINDOW: Duration = Duration::from_secs(60);
const RECENT_FORWARD_WINDOW: Duration = Duration::from_secs(60);
const RECENT_FORWARD_ERROR_WINDOW: Duration = Duration::from_secs(15);

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
    pub forwarded_datagrams_total: u64,
    pub forward_errors_total: u64,
    pub forward_send_errors_total: u64,
    pub forward_queue_enqueued_total: u64,
    pub forward_queue_dropped_total: u64,
    pub forward_queue_depth: u64,
    pub tracked_sources: usize,
    pub latest_slot: u64,
    pub seconds_since_last_packet: Option<u64>,
    pub seconds_since_last_shred: Option<u64>,
    pub seconds_since_last_forward: Option<u64>,
    pub seconds_since_last_forward_error: Option<u64>,
    pub repair_enabled: bool,
    pub repair_active: bool,
    pub repair_peers: u64,
    pub repair_tracked_slots: u64,
    pub repair_outstanding: u64,
    pub repair_observation_queue_dropped_total: u64,
    pub repair_requests_sent_total: u64,
    pub repair_retries_sent_total: u64,
    pub repair_requests_exhausted_total: u64,
    pub repair_requests_cooldown_deferred_total: u64,
    pub repair_packets_rejected_total: u64,
    pub repair_pings_answered_total: u64,
    pub repair_shreds_accepted_total: u64,
    pub repair_root_anchored_shreds_accepted_total: u64,
    pub repair_wal_bytes_total: u64,
    pub repair_wal_max_bytes: u64,
    pub repair_wal_remaining_bytes: u64,
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
    pub repair_enabled: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RepairMetricsUpdate {
    pub active: bool,
    pub peers: usize,
    pub tracked_slots: usize,
    pub outstanding: usize,
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
            repair_wal_syncs: AtomicU64::new(0),
            repair_errors: AtomicU64::new(0),
            state: Mutex::new(ReceiverState {
                dedup: LruCache::new(nonzero_usize(dedup_capacity, "DEDUP_CAPACITY")?),
                sources: LruCache::new(nonzero_usize(
                    SOURCE_CACHE_CAPACITY,
                    "source cache capacity",
                )?),
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

    pub fn record_repair_observation_queue_drop(&self) {
        self.repair_observation_queue_dropped
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_repair_error(&self) {
        self.repair_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_repair_inactive(&self) {
        self.repair_active.store(0, Ordering::Relaxed);
        self.repair_peers.store(0, Ordering::Relaxed);
        self.repair_tracked_slots.store(0, Ordering::Relaxed);
        self.repair_outstanding.store(0, Ordering::Relaxed);
    }

    pub fn update_repair(&self, update: RepairMetricsUpdate) {
        self.repair_active
            .store(u64::from(update.active), Ordering::Relaxed);
        self.repair_peers
            .store(update.peers as u64, Ordering::Relaxed);
        self.repair_tracked_slots
            .store(update.tracked_slots as u64, Ordering::Relaxed);
        self.repair_outstanding
            .store(update.outstanding as u64, Ordering::Relaxed);
        self.repair_requests_sent
            .store(update.requests_sent, Ordering::Relaxed);
        self.repair_retries_sent
            .store(update.retries_sent, Ordering::Relaxed);
        self.repair_requests_exhausted
            .store(update.requests_exhausted, Ordering::Relaxed);
        self.repair_requests_cooldown_deferred
            .store(update.requests_cooldown_deferred, Ordering::Relaxed);
        self.repair_packets_rejected
            .store(update.packets_rejected, Ordering::Relaxed);
        self.repair_pings_answered
            .store(update.pings_answered, Ordering::Relaxed);
        self.repair_shreds_accepted
            .store(update.shreds_accepted, Ordering::Relaxed);
        self.repair_root_anchored_shreds_accepted
            .store(update.root_anchored_shreds_accepted, Ordering::Relaxed);
        self.repair_wal_bytes
            .store(update.wal_bytes, Ordering::Relaxed);
        self.repair_wal_max_bytes
            .store(update.wal_max_bytes, Ordering::Relaxed);
        self.repair_wal_syncs
            .store(update.wal_syncs, Ordering::Relaxed);
    }

    pub fn snapshot(&self, service: &ServiceState) -> Snapshot {
        let now = unix_millis();
        let last_packet = self.last_packet_unix_ms.load(Ordering::Relaxed);
        let last_shred = self.last_shred_unix_ms.load(Ordering::Relaxed);
        let last_forward = self.last_forward_unix_ms.load(Ordering::Relaxed);
        let last_forward_error = self.last_forward_error_unix_ms.load(Ordering::Relaxed);
        let forward_send_errors = self.forward_send_errors.load(Ordering::Relaxed);
        let forward_queue_dropped = self.forward_queue_dropped.load(Ordering::Relaxed);
        let repair_wal_bytes = self.repair_wal_bytes.load(Ordering::Relaxed);
        let repair_wal_max_bytes = self.repair_wal_max_bytes.load(Ordering::Relaxed);
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
            forwarded_datagrams_total: self.forwarded.load(Ordering::Relaxed),
            forward_errors_total: forward_send_errors.saturating_add(forward_queue_dropped),
            forward_send_errors_total: forward_send_errors,
            forward_queue_enqueued_total: self.forward_queue_enqueued.load(Ordering::Relaxed),
            forward_queue_dropped_total: forward_queue_dropped,
            forward_queue_depth: self.forward_queue_depth(),
            tracked_sources: self.state.lock().unwrap().sources.len(),
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
            repair_peers: self.repair_peers.load(Ordering::Relaxed),
            repair_tracked_slots: self.repair_tracked_slots.load(Ordering::Relaxed),
            repair_outstanding: self.repair_outstanding.load(Ordering::Relaxed),
            repair_observation_queue_dropped_total: self
                .repair_observation_queue_dropped
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
            repair_wal_max_bytes,
            repair_wal_remaining_bytes: repair_wal_max_bytes.saturating_sub(repair_wal_bytes),
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
