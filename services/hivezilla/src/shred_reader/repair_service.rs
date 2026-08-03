//! Non-blocking integration between the Turbine receive path and bounded Agave repair.

use std::{
    collections::HashSet,
    io::{self, ErrorKind},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use solana_gossip::{cluster_info::ClusterInfo, contact_info::Protocol};
use solana_ledger::shred::Shred;
use tokio::{
    sync::{mpsc, watch},
    task::{AbortHandle, JoinHandle},
    time::MissedTickBehavior,
};
use tracing::{debug, info, warn};

use super::{
    leader_schedule::LeaderScheduleCache,
    metrics::{Metrics, RepairMetricsUpdate},
    repair_runtime::{RepairPeer, RepairRuntime, RepairRuntimeConfig},
    repair_tracker::{RepairTracker, RepairTrackerConfig},
    repair_trust_store::{
        RepairTrustConflict, RepairTrustStore, RepairTrustStoreConfig, TurbineTrustError,
        TurbineTrustObservation,
    },
    repair_wal::{RepairWalConfig, RepairWalFsyncPolicy},
    repair_wal_worker::RepairWalWorker,
};

const REPAIR_TICK: Duration = Duration::from_millis(50);
const SETTLE_TIME: Duration = Duration::from_millis(200);
const SLOT_RETENTION: Duration = Duration::from_secs(12);
const LEADER_REFRESH_RETRY: Duration = Duration::from_secs(10);
const LEADER_REFRESH_TIMEOUT: Duration = Duration::from_secs(20);
const RESTART_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(60);
const PERSISTENT_STORAGE_RETRY: Duration = Duration::from_secs(15 * 60);
const HEALTHY_ATTEMPT_RESET: Duration = Duration::from_secs(5 * 60);
const ATTEMPT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Debug)]
pub struct RepairServiceConfig {
    pub rpc_url: String,
    pub wal_path: std::path::PathBuf,
    pub wal_max_bytes: u64,
    pub wal_total_warning_bytes: u64,
    pub wal_total_critical_bytes: u64,
    pub wal_total_hard_bytes: u64,
    pub wal_filesystem_reserve_bytes: u64,
    pub max_peers: usize,
    pub shred_version: u16,
    pub observation_queue_capacity: usize,
}

struct Components {
    peer_count: usize,
    tracker: RepairTracker,
    trust: RepairTrustStore,
    runtime: RepairRuntime<RepairTrustStore>,
}

struct RepairSupervisorGuard(Arc<Metrics>);

impl Drop for RepairSupervisorGuard {
    fn drop(&mut self) {
        self.0.mark_repair_inactive();
    }
}

struct AbortTaskOnDrop(AbortHandle);

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RestartBackoff {
    next: Duration,
    maximum: Duration,
    persistent_storage_retry: Duration,
}

impl RestartBackoff {
    fn new(initial: Duration, maximum: Duration, persistent_storage_retry: Duration) -> Self {
        debug_assert!(!initial.is_zero());
        debug_assert!(initial <= maximum);
        debug_assert!(maximum <= persistent_storage_retry);
        Self {
            next: initial,
            maximum,
            persistent_storage_retry,
        }
    }

    fn after_failure(&mut self, persistent_storage_failure: bool) -> Duration {
        if persistent_storage_failure {
            self.next = self.maximum;
            return self.persistent_storage_retry;
        }
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.maximum);
        delay
    }

    fn reset(&mut self, initial: Duration) {
        self.next = initial;
    }
}

pub async fn run(
    config: RepairServiceConfig,
    cluster_info: Arc<ClusterInfo>,
    metrics: Arc<Metrics>,
    mut observations: mpsc::Receiver<Arc<[u8]>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let _supervisor_guard = RepairSupervisorGuard(metrics.clone());
    metrics.mark_repair_starting();
    initialize_repair_wal_metrics(&config, &metrics).await;
    let mut backoff = RestartBackoff::new(
        RESTART_BACKOFF_INITIAL,
        RESTART_BACKOFF_MAX,
        PERSISTENT_STORAGE_RETRY,
    );

    loop {
        if *shutdown.borrow() {
            metrics.mark_repair_stopping();
            return Ok(());
        }
        metrics.mark_repair_starting();
        let (attempt_tx, attempt_rx) = mpsc::channel(config.observation_queue_capacity);
        let (active_tx, active_rx) = watch::channel(None);
        let mut attempt = tokio::spawn(run_attempt(
            config.clone(),
            cluster_info.clone(),
            metrics.clone(),
            attempt_rx,
            shutdown.clone(),
            active_tx,
        ));
        let _attempt_guard = AbortTaskOnDrop(attempt.abort_handle());

        let (failure, persistent_storage_failure) = loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        metrics.mark_repair_stopping();
                        drop(attempt_tx);
                        stop_attempt(attempt, &metrics).await;
                        return Ok(());
                    }
                }
                result = &mut attempt => {
                    break match result {
                        Ok(Ok(())) => ("repair attempt stopped unexpectedly".to_owned(), false),
                        Ok(Err(error)) => {
                            let persistent = is_persistent_storage_failure(&error);
                            (format!("{error:#}"), persistent)
                        }
                        Err(error) => (format!("repair attempt panicked: {error}"), false),
                    };
                }
                observation = observations.recv() => {
                    let Some(observation) = observation else {
                        metrics.mark_repair_stopping();
                        drop(attempt_tx);
                        stop_attempt(attempt, &metrics).await;
                        if *shutdown.borrow() {
                            return Ok(());
                        }
                        bail!("repair observation channel closed before shutdown");
                    };
                    if attempt_tx.try_send(observation).is_err() {
                        // This queue is deliberately lossy. A blocked/restarting repair path may
                        // never apply backpressure to the raw receive and forwarding boundary.
                        metrics.record_repair_observation_queue_drop();
                    }
                }
            }
        };

        let was_healthy = active_rx
            .borrow()
            .is_some_and(|started| started.elapsed() >= HEALTHY_ATTEMPT_RESET);
        if was_healthy {
            backoff.reset(RESTART_BACKOFF_INITIAL);
        }
        let delay = backoff.after_failure(persistent_storage_failure);
        if persistent_storage_failure {
            metrics.set_repair_wal_error(Some(&failure));
        }
        metrics.mark_repair_backoff(&failure);
        warn!(
            error = %failure,
            restart_delay_seconds = delay.as_secs_f64(),
            "bounded repair attempt failed; raw capture remains live and repair will restart"
        );

        let restart = tokio::time::sleep(delay);
        tokio::pin!(restart);
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        metrics.mark_repair_stopping();
                        return Ok(());
                    }
                }
                _ = &mut restart => break,
                observation = observations.recv() => {
                    if observation.is_none() {
                        if *shutdown.borrow() {
                            metrics.mark_repair_stopping();
                            return Ok(());
                        }
                        bail!("repair observation channel closed during restart backoff");
                    }
                    // Do not carry unauthenticated observations across a component restart. New
                    // trust state must be learned afresh from post-restart Turbine evidence.
                    metrics.record_repair_observation_queue_drop();
                }
            }
        }
    }
}

async fn stop_attempt(mut attempt: JoinHandle<Result<()>>, metrics: &Metrics) {
    match tokio::time::timeout(ATTEMPT_SHUTDOWN_TIMEOUT, &mut attempt).await {
        Ok(_) => {}
        Err(_) => {
            attempt.abort();
            let _ = attempt.await;
            metrics.record_repair_runtime_error("repair attempt timed out during shutdown");
            warn!(
                timeout_seconds = ATTEMPT_SHUTDOWN_TIMEOUT.as_secs(),
                "aborted repair attempt during shutdown; blocking WAL cleanup remains isolated"
            );
        }
    }
}

async fn run_attempt(
    config: RepairServiceConfig,
    cluster_info: Arc<ClusterInfo>,
    metrics: Arc<Metrics>,
    mut observations: mpsc::Receiver<Arc<[u8]>>,
    mut shutdown: watch::Receiver<bool>,
    active: watch::Sender<Option<Instant>>,
) -> Result<()> {
    let leaders = LeaderScheduleCache::new(config.rpc_url.clone());
    let mut components: Option<Components> = None;
    let mut latest_slot = None;
    let mut last_leader_refresh_attempt = None;
    let mut warned_trust_conflict_slots = HashSet::new();
    let mut timer = tokio::time::interval(REPAIR_TICK);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    timer.tick().await;

    info!(
        rpc_url = %config.rpc_url,
        repair_wal = %config.wal_path.display(),
        repair_wal_max_bytes = config.wal_max_bytes,
        repair_wal_total_warning_bytes = config.wal_total_warning_bytes,
        repair_wal_total_critical_bytes = config.wal_total_critical_bytes,
        max_peers = config.max_peers,
        "bounded repair observer started"
    );

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    if let Some(components) = &mut components {
                        components.runtime.flush_repair_wal(Instant::now())
                            .await
                            .context("flush repair provenance WAL during shutdown")?;
                    }
                    return Ok(());
                }
            }
            observation = observations.recv() => {
                let Some(payload) = observation else {
                    return Ok(());
                };
                let Ok(shred) = Shred::new_from_serialized_shred(payload.to_vec()) else {
                    metrics.record_repair_error();
                    continue;
                };
                let slot = shred.slot();

                if leaders.leader(slot).is_none()
                    && retry_due(last_leader_refresh_attempt, LEADER_REFRESH_RETRY)
                {
                    last_leader_refresh_attempt = Some(Instant::now());
                    match tokio::time::timeout(
                        LEADER_REFRESH_TIMEOUT,
                        leaders.refresh_current(),
                    ).await {
                        Ok(Ok(outcome)) => info!(
                            epoch = outcome.epoch,
                            first_slot = outcome.first_slot,
                            slots_in_epoch = outcome.slots_in_epoch,
                            inserted = outcome.inserted,
                            cached_epochs = outcome.cached_epochs,
                            "leader schedule ready for repair verification"
                        ),
                        Ok(Err(error)) => {
                            metrics.record_repair_runtime_error(&error.to_string());
                            warn!(slot, %error, "cannot refresh leader schedule; repair remains fail-closed");
                        }
                        Err(_) => {
                            metrics.record_repair_runtime_error("leader schedule refresh timed out");
                            warn!(slot, "leader schedule refresh timed out; repair remains fail-closed");
                        }
                    }
                }

                if components.is_none()
                    && leaders.leader(slot).is_some()
                {
                    match initialize_components(&config, &cluster_info, leaders.clone(), slot).await {
                        Ok(initialized) => {
                            info!(
                                peers = initialized.peer_count,
                                repair_socket = %initialized.runtime.local_addr()?,
                                "bounded repair transport is active"
                            );
                            update_metrics(&metrics, &initialized, &config);
                            metrics.set_repair_wal_error(None);
                            let _ = active.send(Some(Instant::now()));
                            components = Some(initialized);
                        }
                        Err(error) => {
                            return Err(error).context(format!(
                                "initialize bounded repair components for slot {slot}"
                            ));
                        }
                    }
                }

                if let Some(components) = &mut components {
                    let now = Instant::now();
                    match components.trust.observe_turbine_packet(&payload) {
                        Ok(TurbineTrustObservation::Inserted { .. }
                            | TurbineTrustObservation::Duplicate { .. }) => {
                            // Only leader-verified original Turbine evidence may influence gap
                            // tracking. The raw UDP parse above is intentionally not a trust gate.
                            components.tracker.observe(&shred, now);
                            latest_slot = Some(
                                latest_slot.map_or(slot, |current: u64| current.max(slot)),
                            );
                        }
                        Ok(TurbineTrustObservation::Blocked { slot, conflict }) => {
                            if warned_trust_conflict_slots.insert(slot) {
                                warn_trust_conflict(slot, &conflict);
                            }
                        }
                        Ok(TurbineTrustObservation::IgnoredTooOld { .. }) => {}
                        Err(TurbineTrustError::MissingSlotLeader { .. }) => {}
                        Err(error) => {
                            metrics.record_repair_error();
                            debug!(slot, %error, "original Turbine shred was not admitted as repair trust evidence");
                        }
                    }
                }
            }
            _ = timer.tick() => {
                let Some(components) = &mut components else {
                    continue;
                };
                let now = Instant::now();
                let requests = components
                    .tracker
                    .repair_requests_due(now)
                    .into_iter()
                    .filter(|request| components.trust.can_request(request))
                    .collect::<Vec<_>>();
                let poll = components
                    .runtime
                    .service_tracker_requests(requests, now, unix_millis())
                    .await
                    .context("service bounded repair requests")?;
                for accepted in poll.accepted {
                    components.tracker.observe(&accepted.shred, now);
                }
                update_metrics(&metrics, components, &config);
                if warned_trust_conflict_slots.len() > 512 {
                    let oldest_retained_slot = latest_slot.unwrap_or_default().saturating_sub(256);
                    warned_trust_conflict_slots.retain(|slot| *slot >= oldest_retained_slot);
                }
            }
        }
    }
}

async fn initialize_components(
    config: &RepairServiceConfig,
    cluster_info: &Arc<ClusterInfo>,
    leaders: LeaderScheduleCache,
    slot: u64,
) -> Result<Components> {
    let peers = select_repair_peers(cluster_info, slot, config.max_peers);
    if peers.is_empty() {
        bail!("gossip has no compatible serve-repair peers for slot {slot}");
    }
    let peer_count = peers.len();
    let leader_lookup = leaders.clone();
    let trust = RepairTrustStore::new(
        RepairTrustStoreConfig {
            shred_version: config.shred_version,
            max_slots: 256,
            max_fec_sets_per_slot: 1_024,
            max_authorized_peers: config.max_peers,
        },
        peers.clone(),
        move |slot| leader_lookup.leader(slot),
    )?;
    let repair_wal = RepairWalWorker::open(
        RepairWalConfig {
            path: config.wal_path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: config.wal_max_bytes,
            max_retained_bytes: config.wal_total_hard_bytes,
            filesystem_reserve_bytes: config.wal_filesystem_reserve_bytes,
        },
        Instant::now(),
    )
    .await
    .context("open isolated repair provenance WAL")?;
    let runtime = RepairRuntime::bind_with_wal_worker(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        cluster_info.keypair(),
        trust.clone(),
        peers,
        RepairRuntimeConfig {
            max_peers: config.max_peers,
            max_outstanding: 256,
            max_requests_considered_per_tick: 128,
            max_new_requests_per_tick: 64,
            max_packets_per_tick: 512,
            max_packet_bytes: 2_048,
            max_suppressed_requests: 16_384,
            request_timeout: Duration::from_millis(150),
            exhaustion_cooldown: SLOT_RETENTION,
            max_retries: 4,
            initial_nonce: unix_millis() as u32,
        },
        repair_wal,
    )
    .await
    .context("bind bounded repair UDP transport")?;
    Ok(Components {
        peer_count,
        tracker: RepairTracker::new(RepairTrackerConfig {
            settle_time: SETTLE_TIME,
            slot_retention: SLOT_RETENTION,
            max_slots: 256,
            max_fec_sets_per_slot: 1_024,
            max_requests_per_slot: 32,
            max_requests_per_poll: 128,
        }),
        trust,
        runtime,
    })
}

fn select_repair_peers(cluster_info: &ClusterInfo, slot: u64, maximum: usize) -> Vec<RepairPeer> {
    let mut peers = cluster_info
        .repair_peers(slot)
        .into_iter()
        .filter_map(|contact| {
            contact
                .serve_repair(Protocol::UDP)
                .map(|repair_addr| RepairPeer {
                    pubkey: *contact.pubkey(),
                    repair_addr,
                })
        })
        .collect::<Vec<_>>();
    peers.sort_by_key(|peer| peer.pubkey.to_bytes());
    if !peers.is_empty() {
        let rotation = usize::try_from(slot).unwrap_or_default() % peers.len();
        peers.rotate_left(rotation);
    }
    let mut pubkeys = HashSet::new();
    let mut addresses = HashSet::new();
    peers.retain(|peer| pubkeys.insert(peer.pubkey) && addresses.insert(peer.repair_addr));
    peers.truncate(maximum);
    peers
}

fn update_metrics(metrics: &Metrics, components: &Components, config: &RepairServiceConfig) {
    let stats = components.runtime.stats();
    metrics.update_repair(RepairMetricsUpdate {
        active: true,
        peers: components.peer_count,
        tracked_slots: components.tracker.tracked_slot_count(),
        outstanding: components.runtime.outstanding_count(),
        requests_sent: stats.requests_sent,
        retries_sent: stats.retries_sent,
        requests_exhausted: stats.requests_exhausted,
        requests_cooldown_deferred: stats.requests_cooldown_deferred,
        packets_rejected: stats.packets_rejected,
        pings_answered: stats.pings_answered,
        shreds_accepted: stats.shreds_accepted,
        root_anchored_shreds_accepted: stats.root_anchored_shreds_accepted,
        wal_bytes: stats.repair_wal_bytes,
        wal_max_bytes: stats.repair_wal_max_bytes,
        wal_active_segment_bytes: stats.repair_wal_active_segment_bytes,
        wal_segment_count: stats.repair_wal_segment_count,
        wal_active_segment_id: stats.repair_wal_active_segment_id,
        wal_rollovers: stats.repair_wal_rollovers,
        wal_durable_through_sequence: stats.repair_wal_durable_through_sequence,
        wal_total_warning_bytes: config.wal_total_warning_bytes,
        wal_total_critical_bytes: config.wal_total_critical_bytes,
        wal_total_hard_bytes: stats.repair_wal_total_hard_bytes,
        wal_filesystem_reserve_bytes: stats.repair_wal_filesystem_reserve_bytes,
        wal_filesystem_available_bytes: stats.repair_wal_filesystem_available_bytes,
        wal_v3_sealed: stats.repair_wal_v3_sealed,
        wal_syncs: stats.repair_wal_syncs,
    });
}

async fn initialize_repair_wal_metrics(config: &RepairServiceConfig, metrics: &Metrics) {
    let inspection = RepairWalWorker::inspect(config.wal_path.clone()).await;
    match inspection {
        Ok(inspection) => metrics.initialize_repair_wal_storage(
            config.wal_max_bytes,
            config.wal_total_warning_bytes,
            config.wal_total_critical_bytes,
            config.wal_total_hard_bytes,
            config.wal_filesystem_reserve_bytes,
            Some(&inspection),
            None,
        ),
        Err(error) => {
            let error = error.to_string();
            metrics.initialize_repair_wal_storage(
                config.wal_max_bytes,
                config.wal_total_warning_bytes,
                config.wal_total_critical_bytes,
                config.wal_total_hard_bytes,
                config.wal_filesystem_reserve_bytes,
                None,
                Some(&error),
            );
            warn!(%error, "cannot inspect repair WAL before startup; repair remains fail-closed");
        }
    }
}

fn warn_trust_conflict(slot: u64, conflict: &RepairTrustConflict) {
    warn!(
        slot,
        ?conflict,
        "repair disabled for conflicting Turbine slot evidence"
    );
}

fn retry_due(last: Option<Instant>, interval: Duration) -> bool {
    last.is_none_or(|last| last.elapsed() >= interval)
}

fn is_persistent_storage_failure(error: &anyhow::Error) -> bool {
    let wal_context = error.chain().any(|cause| {
        let message = cause.to_string().to_ascii_lowercase();
        message.contains("repair wal") || message.contains("repair provenance wal")
    });
    error.chain().any(|cause| {
        cause.downcast_ref::<io::Error>().is_some_and(|error| {
            error.kind() == ErrorKind::StorageFull
                || wal_context
                    && matches!(
                        error.kind(),
                        ErrorKind::InvalidData | ErrorKind::PermissionDenied
                    )
        })
    })
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

    #[test]
    fn restart_backoff_is_exponential_and_stays_capped() {
        let mut backoff = RestartBackoff::new(
            Duration::from_millis(10),
            Duration::from_millis(40),
            Duration::from_millis(100),
        );

        assert_eq!(backoff.after_failure(false), Duration::from_millis(10));
        assert_eq!(backoff.after_failure(false), Duration::from_millis(20));
        assert_eq!(backoff.after_failure(false), Duration::from_millis(40));
        assert_eq!(backoff.after_failure(false), Duration::from_millis(40));
        assert_eq!(backoff.after_failure(false), Duration::from_millis(40));
    }

    #[test]
    fn persistent_storage_failures_jump_to_and_remain_at_the_cap() {
        let mut backoff = RestartBackoff::new(
            Duration::from_millis(10),
            Duration::from_millis(40),
            Duration::from_millis(100),
        );
        let error = anyhow::Error::new(io::Error::new(
            ErrorKind::StorageFull,
            "repair WAL hard ceiling reached",
        ))
        .context("append repair provenance");

        assert!(is_persistent_storage_failure(&error));
        assert_eq!(backoff.after_failure(true), Duration::from_millis(100));
        assert_eq!(backoff.after_failure(false), Duration::from_millis(40));
    }

    #[test]
    fn non_wal_permission_and_invalid_data_errors_keep_transient_backoff() {
        let udp = anyhow::Error::new(io::Error::new(
            ErrorKind::PermissionDenied,
            "bind repair UDP socket",
        ));
        let wire = anyhow::Error::new(io::Error::new(
            ErrorKind::InvalidData,
            "encode repair wire request",
        ));
        let wal = anyhow::Error::new(io::Error::new(
            ErrorKind::PermissionDenied,
            "repair WAL append failed",
        ));

        assert!(!is_persistent_storage_failure(&udp));
        assert!(!is_persistent_storage_failure(&wire));
        assert!(is_persistent_storage_failure(&wal));
    }

    #[test]
    fn a_healthy_attempt_resets_transient_backoff() {
        let initial = Duration::from_millis(10);
        let mut backoff = RestartBackoff::new(
            initial,
            Duration::from_millis(40),
            Duration::from_millis(100),
        );
        assert_eq!(backoff.after_failure(false), Duration::from_millis(10));
        assert_eq!(backoff.after_failure(false), Duration::from_millis(20));

        backoff.reset(initial);

        assert_eq!(backoff.after_failure(false), Duration::from_millis(10));
    }
}
