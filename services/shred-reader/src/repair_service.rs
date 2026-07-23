//! Non-blocking integration between the Turbine receive path and bounded Agave repair.

use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use solana_gossip::{cluster_info::ClusterInfo, contact_info::Protocol};
use solana_ledger::shred::Shred;
use tokio::{
    sync::{mpsc, watch},
    time::MissedTickBehavior,
};
use tracing::{debug, info, warn};

use crate::{
    leader_schedule::LeaderScheduleCache,
    metrics::{Metrics, RepairMetricsUpdate},
    repair_runtime::{RepairPeer, RepairRuntime, RepairRuntimeConfig},
    repair_tracker::{RepairTracker, RepairTrackerConfig},
    repair_trust_store::{
        RepairTrustConflict, RepairTrustStore, RepairTrustStoreConfig, TurbineTrustError,
        TurbineTrustObservation,
    },
    repair_wal::{RepairWal, RepairWalConfig, RepairWalFsyncPolicy},
};

const REPAIR_TICK: Duration = Duration::from_millis(50);
const SETTLE_TIME: Duration = Duration::from_millis(200);
const SLOT_RETENTION: Duration = Duration::from_secs(12);
const INITIALIZATION_RETRY: Duration = Duration::from_secs(2);
const LEADER_REFRESH_RETRY: Duration = Duration::from_secs(10);
const LEADER_REFRESH_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Debug)]
pub struct RepairServiceConfig {
    pub rpc_url: String,
    pub wal_path: std::path::PathBuf,
    pub wal_max_bytes: u64,
    pub max_peers: usize,
    pub shred_version: u16,
}

struct Components {
    peer_count: usize,
    tracker: RepairTracker,
    trust: RepairTrustStore,
    runtime: RepairRuntime<RepairTrustStore>,
}

pub async fn run(
    config: RepairServiceConfig,
    cluster_info: Arc<ClusterInfo>,
    metrics: Arc<Metrics>,
    mut observations: mpsc::Receiver<Arc<[u8]>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let leaders = LeaderScheduleCache::new(config.rpc_url.clone());
    let mut components: Option<Components> = None;
    let mut latest_slot = None;
    let mut last_initialization_attempt = None;
    let mut last_leader_refresh_attempt = None;
    let mut warned_trust_conflict_slots = HashSet::new();
    let mut timer = tokio::time::interval(REPAIR_TICK);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    timer.tick().await;

    info!(
        rpc_url = %config.rpc_url,
        repair_wal = %config.wal_path.display(),
        repair_wal_max_bytes = config.wal_max_bytes,
        max_peers = config.max_peers,
        "bounded repair observer started"
    );

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    if let Some(components) = &mut components {
                        components.runtime.flush_repair_wal(Instant::now())
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
                            metrics.record_repair_error();
                            warn!(slot, %error, "cannot refresh leader schedule; repair remains fail-closed");
                        }
                        Err(_) => {
                            metrics.record_repair_error();
                            warn!(slot, "leader schedule refresh timed out; repair remains fail-closed");
                        }
                    }
                }

                if components.is_none()
                    && leaders.leader(slot).is_some()
                    && retry_due(last_initialization_attempt, INITIALIZATION_RETRY)
                {
                    last_initialization_attempt = Some(Instant::now());
                    match initialize_components(&config, &cluster_info, leaders.clone(), slot).await {
                        Ok(initialized) => {
                            info!(
                                peers = initialized.peer_count,
                                repair_socket = %initialized.runtime.local_addr()?,
                                "bounded repair transport is active"
                            );
                            components = Some(initialized);
                        }
                        Err(error) => {
                            metrics.record_repair_error();
                            debug!(slot, %error, "repair transport is not ready yet");
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
                update_metrics(&metrics, components);
                if warned_trust_conflict_slots.len() > 512 {
                    let oldest_retained_slot = latest_slot.unwrap_or_default().saturating_sub(256);
                    warned_trust_conflict_slots.retain(|slot| *slot >= oldest_retained_slot);
                }
            }
        }

        if components.is_none() && latest_slot.is_some() && observations.is_closed() {
            bail!("repair observation channel closed before shutdown");
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
    let repair_wal = RepairWal::open(
        RepairWalConfig {
            path: config.wal_path.clone(),
            fsync: RepairWalFsyncPolicy::EveryRecord,
            max_file_bytes: config.wal_max_bytes,
        },
        Instant::now(),
    )
    .context("open isolated repair provenance WAL")?;
    let runtime = RepairRuntime::bind(
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

fn update_metrics(metrics: &Metrics, components: &Components) {
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
        wal_syncs: stats.repair_wal_syncs,
    });
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

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}
