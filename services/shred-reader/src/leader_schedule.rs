//! Bounded, fail-closed leader-schedule cache for repaired-shred verification.
//!
//! Leader schedules are immutable once published, so a successful epoch fetch can be reused by
//! every repair worker. Refreshes are serialized, built outside the read path, and committed
//! atomically. An RPC error or malformed response therefore never clears or partially replaces a
//! previously verified schedule. Callers should run [`LeaderScheduleCache::refresh_for_slot`] in a
//! background task; [`LeaderScheduleCache::leader`] is synchronous and never performs network I/O.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result, bail, ensure};
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::Mutex;

/// Only schedules close enough to the RPC node's current epoch are accepted and retained.
const MAX_EPOCH_DISTANCE: u64 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RefreshOutcome {
    pub epoch: u64,
    pub first_slot: u64,
    pub slots_in_epoch: u64,
    /// False means the complete epoch schedule was already present.
    pub inserted: bool,
    /// Number of complete schedules retained after pruning. This is always at most five.
    pub cached_epochs: usize,
}

#[derive(Debug)]
struct CachedEpoch {
    first_slot: u64,
    leaders: Box<[Pubkey]>,
}

impl CachedEpoch {
    fn slots_in_epoch(&self) -> u64 {
        self.leaders.len() as u64
    }

    fn leader(&self, slot: u64) -> Option<Pubkey> {
        let relative_slot = slot.checked_sub(self.first_slot)?;
        let index = usize::try_from(relative_slot).ok()?;
        self.leaders.get(index).copied()
    }
}

/// Thread-safe cache shared by the receive and repair tasks.
#[derive(Clone)]
pub struct LeaderScheduleCache {
    rpc: Arc<RpcClient>,
    epochs: Arc<RwLock<BTreeMap<u64, CachedEpoch>>>,
    refresh_lock: Arc<Mutex<()>>,
}

impl LeaderScheduleCache {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self::with_client(Arc::new(RpcClient::new(rpc_url.into())))
    }

    /// Allows the service to share an already configured nonblocking RPC client.
    pub fn with_client(rpc: Arc<RpcClient>) -> Self {
        Self {
            rpc,
            epochs: Arc::new(RwLock::new(BTreeMap::new())),
            refresh_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Returns the expected leader without waiting for RPC or an async lock.
    ///
    /// Lock poisoning, a missing epoch, or an out-of-range slot all return `None`; consumers must
    /// treat that as "cannot verify" and reject/quarantine the repaired shred.
    pub fn leader(&self, slot: u64) -> Option<Pubkey> {
        let epochs = self.epochs.read().ok()?;
        epochs.values().find_map(|epoch| epoch.leader(slot))
    }

    /// Refreshes the epoch containing the RPC node's own current slot. This is the safe bootstrap
    /// path when incoming UDP slots are not yet authenticated and therefore must not choose which
    /// multi-megabyte schedule is fetched.
    pub async fn refresh_current(&self) -> Result<RefreshOutcome> {
        let slot = self
            .rpc
            .get_slot()
            .await
            .context("fetch current slot for leader schedule refresh")?;
        self.refresh_for_slot(slot).await
    }

    /// Ensures the complete leader schedule containing `slot` is cached.
    ///
    /// The RPC node's epoch schedule is used for absolute-slot conversion, including warmup
    /// epochs. The RPC's current `EpochInfo` is cross-checked against it before the requested
    /// leader schedule is trusted. On any failure this method returns an error without removing or
    /// changing previously cached schedules.
    pub async fn refresh_for_slot(&self, slot: u64) -> Result<RefreshOutcome> {
        // Do not fan out identical 432k-slot requests when several repair workers notice a new
        // epoch at once. This lock is never used by the live receive/read path.
        let _refresh_guard = self.refresh_lock.lock().await;

        let (epoch_info, epoch_schedule) =
            tokio::try_join!(self.rpc.get_epoch_info(), self.rpc.get_epoch_schedule(),)
                .context("fetch current epoch information and epoch schedule")?;

        validate_epoch_info(
            epoch_info.epoch,
            epoch_info.slot_index,
            epoch_info.slots_in_epoch,
            epoch_info.absolute_slot,
            epoch_schedule.get_epoch_and_slot_index(epoch_info.absolute_slot),
            epoch_schedule.get_first_slot_in_epoch(epoch_info.epoch),
            epoch_schedule.get_slots_in_epoch(epoch_info.epoch),
        )?;

        let (target_epoch, target_slot_index) = epoch_schedule.get_epoch_and_slot_index(slot);
        ensure_epoch_is_cacheable(epoch_info.epoch, target_epoch)?;

        let first_slot = epoch_schedule.get_first_slot_in_epoch(target_epoch);
        let slots_in_epoch = epoch_schedule.get_slots_in_epoch(target_epoch);
        ensure!(
            target_slot_index < slots_in_epoch,
            "slot {slot} maps outside epoch {target_epoch}"
        );
        ensure!(
            first_slot.checked_add(target_slot_index) == Some(slot),
            "epoch schedule produced an inconsistent absolute slot mapping for slot {slot}"
        );

        if self.contains_complete_epoch(target_epoch, first_slot, slots_in_epoch)? {
            let cached_epochs = self.prune_and_count(epoch_info.epoch)?;
            return Ok(RefreshOutcome {
                epoch: target_epoch,
                first_slot,
                slots_in_epoch,
                inserted: false,
                cached_epochs,
            });
        }

        // Supplying any slot in an epoch asks getLeaderSchedule for that epoch. Use the first slot
        // to make the absolute/relative conversion explicit and auditable.
        let schedule = self
            .rpc
            .get_leader_schedule(Some(first_slot))
            .await
            .with_context(|| {
                format!("fetch leader schedule for epoch {target_epoch} (first slot {first_slot})")
            })?
            .with_context(|| format!("RPC returned no leader schedule for epoch {target_epoch}"))?;

        let epoch = build_epoch(first_slot, slots_in_epoch, schedule)
            .with_context(|| format!("validate leader schedule for epoch {target_epoch}"))?;

        // No fallible RPC or parsing work follows this point. Commit the complete schedule and
        // prune in one short write-lock section so readers never observe a partial epoch.
        let mut epochs = self
            .epochs
            .write()
            .map_err(|_| anyhow::anyhow!("leader schedule cache lock is poisoned"))?;
        epochs.insert(target_epoch, epoch);
        prune_epochs(&mut epochs, epoch_info.epoch);
        let cached_epochs = epochs.len();

        Ok(RefreshOutcome {
            epoch: target_epoch,
            first_slot,
            slots_in_epoch,
            inserted: true,
            cached_epochs,
        })
    }

    fn contains_complete_epoch(
        &self,
        epoch: u64,
        first_slot: u64,
        slots_in_epoch: u64,
    ) -> Result<bool> {
        let epochs = self
            .epochs
            .read()
            .map_err(|_| anyhow::anyhow!("leader schedule cache lock is poisoned"))?;
        let Some(cached) = epochs.get(&epoch) else {
            return Ok(false);
        };
        ensure!(
            cached.first_slot == first_slot && cached.slots_in_epoch() == slots_in_epoch,
            "cached epoch {epoch} metadata disagrees with the RPC epoch schedule"
        );
        Ok(true)
    }

    fn prune_and_count(&self, current_epoch: u64) -> Result<usize> {
        let mut epochs = self
            .epochs
            .write()
            .map_err(|_| anyhow::anyhow!("leader schedule cache lock is poisoned"))?;
        prune_epochs(&mut epochs, current_epoch);
        Ok(epochs.len())
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_epoch_info(
    epoch: u64,
    slot_index: u64,
    slots_in_epoch: u64,
    absolute_slot: u64,
    calculated_epoch_and_index: (u64, u64),
    calculated_first_slot: u64,
    calculated_slots_in_epoch: u64,
) -> Result<()> {
    ensure!(
        calculated_epoch_and_index == (epoch, slot_index),
        "RPC epoch info disagrees with the epoch schedule: RPC=({epoch}, {slot_index}), schedule={calculated_epoch_and_index:?}"
    );
    ensure!(
        slots_in_epoch == calculated_slots_in_epoch,
        "RPC epoch length {slots_in_epoch} disagrees with epoch schedule length {calculated_slots_in_epoch}"
    );
    let rpc_first_slot = absolute_slot
        .checked_sub(slot_index)
        .context("RPC epoch slot index exceeds its absolute slot")?;
    ensure!(
        rpc_first_slot == calculated_first_slot,
        "RPC epoch starts at slot {rpc_first_slot}, epoch schedule starts at {calculated_first_slot}"
    );
    Ok(())
}

fn ensure_epoch_is_cacheable(current_epoch: u64, target_epoch: u64) -> Result<()> {
    ensure!(
        current_epoch.abs_diff(target_epoch) <= MAX_EPOCH_DISTANCE,
        "epoch {target_epoch} is outside the verified cache window around current epoch {current_epoch}"
    );
    Ok(())
}

fn build_epoch<I>(first_slot: u64, slots_in_epoch: u64, schedule: I) -> Result<CachedEpoch>
where
    I: IntoIterator<Item = (String, Vec<usize>)>,
{
    ensure!(slots_in_epoch > 0, "leader schedule epoch is empty");
    first_slot
        .checked_add(slots_in_epoch)
        .context("leader schedule absolute slot range overflows u64")?;
    let slot_count = usize::try_from(slots_in_epoch)
        .context("leader schedule is too large for this platform")?;

    let mut leaders = vec![Pubkey::default(); slot_count];
    let mut assigned = vec![false; slot_count];

    for (leader_text, relative_slots) in schedule {
        let leader: Pubkey = leader_text
            .parse()
            .with_context(|| format!("invalid leader identity {leader_text:?}"))?;
        for relative_slot in relative_slots {
            ensure!(
                relative_slot < slot_count,
                "relative leader slot {relative_slot} is outside epoch length {slot_count}"
            );
            if assigned[relative_slot] {
                bail!("relative leader slot {relative_slot} was assigned more than once");
            }
            leaders[relative_slot] = leader;
            assigned[relative_slot] = true;
        }
    }

    if let Some(relative_slot) = assigned.iter().position(|assigned| !assigned) {
        bail!("relative leader slot {relative_slot} has no assigned leader");
    }

    Ok(CachedEpoch {
        first_slot,
        leaders: leaders.into_boxed_slice(),
    })
}

fn prune_epochs(epochs: &mut BTreeMap<u64, CachedEpoch>, current_epoch: u64) {
    epochs.retain(|epoch, _| current_epoch.abs_diff(*epoch) <= MAX_EPOCH_DISTANCE);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn key(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn schedule(entries: &[(Pubkey, &[usize])]) -> BTreeMap<String, Vec<usize>> {
        entries
            .iter()
            .map(|(leader, slots)| (leader.to_string(), slots.to_vec()))
            .collect()
    }

    #[test]
    fn maps_relative_schedule_indices_to_absolute_slots() {
        let alice = key(1);
        let bob = key(2);
        let epoch = build_epoch(10_000, 4, schedule(&[(alice, &[0, 2]), (bob, &[1, 3])])).unwrap();

        assert_eq!(epoch.leader(9_999), None);
        assert_eq!(epoch.leader(10_000), Some(alice));
        assert_eq!(epoch.leader(10_001), Some(bob));
        assert_eq!(epoch.leader(10_002), Some(alice));
        assert_eq!(epoch.leader(10_003), Some(bob));
        assert_eq!(epoch.leader(10_004), None);
    }

    #[test]
    fn rejects_out_of_bounds_duplicate_and_missing_assignments() {
        let alice = key(1);
        let bob = key(2);

        assert!(
            build_epoch(0, 2, schedule(&[(alice, &[0, 2])]))
                .unwrap_err()
                .to_string()
                .contains("outside epoch length")
        );
        assert!(
            build_epoch(0, 2, schedule(&[(alice, &[0]), (bob, &[0, 1])]))
                .unwrap_err()
                .to_string()
                .contains("assigned more than once")
        );
        assert!(
            build_epoch(0, 2, schedule(&[(alice, &[0])]))
                .unwrap_err()
                .to_string()
                .contains("has no assigned leader")
        );
    }

    #[test]
    fn rejects_invalid_identity_and_overflowing_range() {
        assert!(build_epoch(0, 1, [("not-a-pubkey".to_owned(), vec![0])]).is_err());
        assert!(build_epoch(u64::MAX, 1, schedule(&[(key(1), &[0])])).is_err());
    }

    #[test]
    fn validates_rpc_epoch_mapping() {
        assert!(validate_epoch_info(10, 7, 100, 1_007, (10, 7), 1_000, 100).is_ok());
        assert!(validate_epoch_info(10, 7, 100, 1_007, (11, 7), 1_000, 100).is_err());
        assert!(validate_epoch_info(10, 7, 99, 1_007, (10, 7), 1_000, 100).is_err());
        assert!(validate_epoch_info(10, 8, 100, 7, (10, 8), 0, 100).is_err());
    }

    #[test]
    fn accepts_only_current_plus_two_epochs_in_each_direction() {
        for target in 98..=102 {
            assert!(ensure_epoch_is_cacheable(100, target).is_ok());
        }
        assert!(ensure_epoch_is_cacheable(100, 97).is_err());
        assert!(ensure_epoch_is_cacheable(100, 103).is_err());
        assert!(ensure_epoch_is_cacheable(0, 2).is_ok());
        assert!(ensure_epoch_is_cacheable(0, 3).is_err());
    }

    #[test]
    fn pruning_keeps_at_most_five_centered_epochs() {
        let mut epochs = BTreeMap::new();
        for epoch in 95..=105 {
            epochs.insert(
                epoch,
                build_epoch(epoch * 10, 1, schedule(&[(key(epoch as u8), &[0])])).unwrap(),
            );
        }

        prune_epochs(&mut epochs, 100);

        assert_eq!(
            epochs.keys().copied().collect::<Vec<_>>(),
            vec![98, 99, 100, 101, 102]
        );
    }
}
