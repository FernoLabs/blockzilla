//! Bounded, transport-agnostic tracking of recent shred gaps.
//!
//! The tracker deliberately stops at describing Agave repair requests. It does not open sockets,
//! choose repair peers, mutate the live receive path, correlate outstanding nonces, or schedule
//! retry backoff. A future repair transport can feed every valid shred into
//! [`RepairTracker::observe`] and periodically consume [`RepairTracker::repair_plans_due`].

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use solana_ledger::shred::{MAX_CODE_SHREDS_PER_SLOT, MAX_DATA_SHREDS_PER_SLOT, Shred};

// Agave v4.1.2 uses fixed 32-data + 32-coding FEC batches, including padding the final batch.
const DATA_SHREDS_PER_FEC: u32 = 32;
const CODING_SHREDS_PER_FEC: u16 = 32;
const MERKLE_PROOF_ENTRIES_PER_FEC: u8 = 6;

// Agave v4.1.2 serializes CodingShredHeader immediately after the 83-byte common header:
// num_data_shreds (u16), num_coding_shreds (u16), position (u16), all little-endian. These fields
// are crate-private in solana-ledger, so the pure tracker reads the stable wire representation only
// after Shred::new_from_serialized_shred has validated the payload.
const CODING_NUM_DATA_OFFSET: usize = 83;
const CODING_NUM_CODING_OFFSET: usize = 85;
const CODING_POSITION_OFFSET: usize = 87;

type MerkleRoot = [u8; 32];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepairTrackerConfig {
    /// Quiet time after the last newly observed shred before a slot becomes repairable.
    pub settle_time: Duration,
    /// Time since the last packet for a slot before its state is discarded. Must be at least
    /// `settle_time`, otherwise no slot could become repairable before expiry.
    pub slot_retention: Duration,
    /// Hard cap on simultaneously tracked slots.
    pub max_slots: usize,
    /// Hard cap on FEC sets retained per slot. Crossing it blocks repair for that slot instead of
    /// evicting information and later mistaking an old duplicate for new progress.
    pub max_fec_sets_per_slot: usize,
    /// Hard cap on requests emitted for one slot in a poll.
    pub max_requests_per_slot: usize,
    /// Hard cap on requests emitted across all slots in a poll.
    pub max_requests_per_poll: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepairRequest {
    /// Agave `RepairProtocol::HighestWindowIndex`. The request carries the next expected data index:
    /// `N + 1` after the highest observed data shred, or zero when no data shred has been observed.
    HighestWindowIndex {
        slot: u64,
        next_expected_data_index: u32,
    },
    /// Agave `RepairProtocol::WindowIndex`, which asks for one data shred.
    WindowIndex { slot: u64, index: u32 },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RepairConflict {
    /// Two shreds assigned to one FEC set commit to different Merkle roots.
    FecMerkleRoot { fec_set_index: u32 },
    /// The same data index was observed with two different FEC/root identities.
    DataIndexFork { index: u32 },
    /// Coding shreds for one FEC set disagree about their aligned coding-index base.
    FecCodingIndex { fec_set_index: u32 },
    /// Two non-identical shreds both declare themselves the final data shred in the slot.
    ConflictingCompletion {
        declared_index: u32,
        observed_index: u32,
    },
    /// Data, or the data range represented by a coding shred, extends past the declared tail.
    ObservedBeyondCompletion {
        completion_index: u32,
        observed_index: u32,
    },
    /// The configured FEC-state bound was reached. Repair is blocked because silently evicting an
    /// FEC set would make later duplicate observations ambiguous.
    FecCapacityExceeded { rejected_fec_set_index: u32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotRepairPlan {
    pub slot: u64,
    pub completion_index: Option<u32>,
    /// Non-empty conflicts block every request for the slot, but keep the plan visible to callers.
    pub conflicts: Vec<RepairConflict>,
    pub highest_window_request: Option<RepairRequest>,
    /// Missing data indices whose addition makes an observed, consistent FEC set reach its
    /// Reed-Solomon decode threshold. When a request budget truncates work, newer/higher FECs are
    /// selected first so chained-root repair can advance strictly backwards.
    pub fec_threshold_missing_data_indices: Vec<u32>,
    /// Known missing indices in a range with no observed FEC metadata. No Reed-Solomon recovery
    /// assumption is made for these wholly unmapped indices. Higher FECs are selected first under
    /// a cap because only a trusted successor can authenticate a wholly absent FEC.
    pub conservative_missing_data_indices: Vec<u32>,
    /// More repair work existed than the configured per-slot or per-poll request budget allowed.
    pub requests_truncated: bool,
}

impl SlotRepairPlan {
    pub fn requests(&self) -> Vec<RepairRequest> {
        if !self.conflicts.is_empty() {
            return Vec::new();
        }
        let mut requests = Vec::new();
        requests.extend(self.highest_window_request.iter().cloned());
        let indices: BTreeSet<_> = self
            .fec_threshold_missing_data_indices
            .iter()
            .chain(&self.conservative_missing_data_indices)
            .copied()
            .collect();
        requests.extend(indices.into_iter().map(|index| RepairRequest::WindowIndex {
            slot: self.slot,
            index,
        }));
        requests
    }

    pub fn is_blocked(&self) -> bool {
        !self.conflicts.is_empty()
    }

    pub fn is_empty(&self) -> bool {
        self.conflicts.is_empty()
            && self.highest_window_request.is_none()
            && self.fec_threshold_missing_data_indices.is_empty()
            && self.conservative_missing_data_indices.is_empty()
            && !self.requests_truncated
    }

    fn request_count(&self) -> usize {
        usize::from(self.highest_window_request.is_some())
            + self.fec_threshold_missing_data_indices.len()
            + self.conservative_missing_data_indices.len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DataIdentity {
    fec_set_index: u32,
    merkle_root: MerkleRoot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CompletionMarker {
    index: u32,
    fec_set_index: u32,
    merkle_root: MerkleRoot,
}

#[derive(Debug)]
struct FecState {
    merkle_root: MerkleRoot,
    first_coding_index: Option<u32>,
    coding_positions: BTreeSet<u16>,
}

#[derive(Debug)]
struct SlotState {
    data_indices: BTreeMap<u32, DataIdentity>,
    fec_sets: BTreeMap<u32, FecState>,
    completion: Option<CompletionMarker>,
    highest_data_index: Option<u32>,
    conflicts: BTreeSet<RepairConflict>,
    last_progress_at: Instant,
    last_seen_at: Instant,
}

impl SlotState {
    fn new(now: Instant) -> Self {
        Self {
            data_indices: BTreeMap::new(),
            fec_sets: BTreeMap::new(),
            completion: None,
            highest_data_index: None,
            conflicts: BTreeSet::new(),
            last_progress_at: now,
            last_seen_at: now,
        }
    }

    fn ensure_fec(
        &mut self,
        fec_set_index: u32,
        merkle_root: MerkleRoot,
        capacity: usize,
    ) -> Result<bool, RepairConflict> {
        if let Some(fec) = self.fec_sets.get(&fec_set_index) {
            return if fec.merkle_root == merkle_root {
                Ok(false)
            } else {
                Err(RepairConflict::FecMerkleRoot { fec_set_index })
            };
        }
        if self.fec_sets.len() >= capacity {
            return Err(RepairConflict::FecCapacityExceeded {
                rejected_fec_set_index: fec_set_index,
            });
        }
        self.fec_sets.insert(
            fec_set_index,
            FecState {
                merkle_root,
                first_coding_index: None,
                coding_positions: BTreeSet::new(),
            },
        );
        Ok(true)
    }

    fn block<I>(&mut self, conflicts: I, now: Instant)
    where
        I: IntoIterator<Item = RepairConflict>,
    {
        let mut changed = false;
        for conflict in conflicts {
            changed |= self.conflicts.insert(conflict);
        }
        if changed {
            self.last_progress_at = now;
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ValidatedObservation {
    Data {
        slot: u64,
        index: u32,
        fec_set_index: u32,
        merkle_root: MerkleRoot,
        last_in_slot: bool,
    },
    Coding {
        slot: u64,
        fec_set_index: u32,
        merkle_root: MerkleRoot,
        first_coding_index: u32,
        position: u16,
    },
}

impl ValidatedObservation {
    fn slot(self) -> u64 {
        match self {
            Self::Data { slot, .. } | Self::Coding { slot, .. } => slot,
        }
    }
}

#[derive(Debug)]
pub struct RepairTracker {
    config: RepairTrackerConfig,
    slots: BTreeMap<u64, SlotState>,
}

impl RepairTracker {
    pub fn new(config: RepairTrackerConfig) -> Self {
        assert!(config.max_slots > 0, "max_slots must be nonzero");
        assert!(
            config.max_fec_sets_per_slot > 0,
            "max_fec_sets_per_slot must be nonzero"
        );
        assert!(
            config.max_requests_per_slot > 0,
            "max_requests_per_slot must be nonzero"
        );
        assert!(
            config.max_requests_per_poll > 0,
            "max_requests_per_poll must be nonzero"
        );
        assert!(
            config.slot_retention >= config.settle_time,
            "slot_retention must be at least settle_time"
        );
        Self {
            config,
            slots: BTreeMap::new(),
        }
    }

    /// Records a valid, fixed-geometry Agave v4.1.2 shred. Parsed shreds with non-32:32 or
    /// misaligned geometry are ignored before they create or refresh tracker state. Duplicate
    /// Turbine copies refresh retention, but do not defer the settle deadline.
    pub fn observe(&mut self, shred: &Shred, now: Instant) {
        let Some(observation) = validated_observation(shred) else {
            return;
        };
        self.observe_validated(observation, now);
    }

    fn observe_validated(&mut self, observation: ValidatedObservation, now: Instant) {
        self.evict_expired(now);
        let slot = observation.slot();
        if !self.slots.contains_key(&slot) && self.slots.len() >= self.config.max_slots {
            self.evict_least_recent_slot();
        }

        let state = self
            .slots
            .entry(slot)
            .or_insert_with(|| SlotState::new(now));
        state.last_seen_at = now;
        // A conflict is intentionally sticky for the short lifetime of this hot slot. Continuing
        // to mutate partial state could make a later duplicate appear to resolve a fork.
        if !state.conflicts.is_empty() {
            return;
        }

        match observation {
            ValidatedObservation::Data {
                index,
                fec_set_index,
                merkle_root,
                last_in_slot,
                ..
            } => {
                let identity = DataIdentity {
                    fec_set_index,
                    merkle_root,
                };
                let marker = CompletionMarker {
                    index,
                    fec_set_index,
                    merkle_root,
                };
                let mut conflicts = Vec::new();
                if let Some(completion) = state.completion {
                    if index > completion.index {
                        conflicts.push(RepairConflict::ObservedBeyondCompletion {
                            completion_index: completion.index,
                            observed_index: index,
                        });
                    }
                    if last_in_slot && completion != marker {
                        conflicts.push(RepairConflict::ConflictingCompletion {
                            declared_index: completion.index,
                            observed_index: index,
                        });
                    }
                } else if last_in_slot {
                    if let Some(observed_index) =
                        state.highest_data_index.filter(|&seen| seen > index)
                    {
                        conflicts.push(RepairConflict::ObservedBeyondCompletion {
                            completion_index: index,
                            observed_index,
                        });
                    }
                    if let Some((&fec, _)) = state
                        .fec_sets
                        .iter()
                        .find(|(fec, _)| fec.saturating_add(DATA_SHREDS_PER_FEC - 1) > index)
                    {
                        conflicts.push(RepairConflict::ObservedBeyondCompletion {
                            completion_index: index,
                            observed_index: fec.saturating_add(DATA_SHREDS_PER_FEC - 1),
                        });
                    }
                }
                if state
                    .data_indices
                    .get(&index)
                    .is_some_and(|existing| *existing != identity)
                {
                    conflicts.push(RepairConflict::DataIndexFork { index });
                }
                if !conflicts.is_empty() {
                    state.block(conflicts, now);
                    return;
                }

                let mut progressed = match state.ensure_fec(
                    fec_set_index,
                    merkle_root,
                    self.config.max_fec_sets_per_slot,
                ) {
                    Ok(inserted) => inserted,
                    Err(conflict) => {
                        state.block([conflict], now);
                        return;
                    }
                };
                progressed |= state.data_indices.insert(index, identity).is_none();
                if state
                    .highest_data_index
                    .is_none_or(|highest| index > highest)
                {
                    state.highest_data_index = Some(index);
                    progressed = true;
                }
                if last_in_slot && state.completion.is_none() {
                    state.completion = Some(marker);
                    progressed = true;
                }
                if progressed {
                    state.last_progress_at = now;
                }
            }
            ValidatedObservation::Coding {
                fec_set_index,
                merkle_root,
                first_coding_index,
                position,
                ..
            } => {
                if let Some(completion) = state.completion {
                    let represented_tail = fec_set_index.saturating_add(DATA_SHREDS_PER_FEC - 1);
                    if represented_tail > completion.index {
                        state.block(
                            [RepairConflict::ObservedBeyondCompletion {
                                completion_index: completion.index,
                                observed_index: represented_tail,
                            }],
                            now,
                        );
                        return;
                    }
                }
                let mut progressed = match state.ensure_fec(
                    fec_set_index,
                    merkle_root,
                    self.config.max_fec_sets_per_slot,
                ) {
                    Ok(inserted) => inserted,
                    Err(conflict) => {
                        state.block([conflict], now);
                        return;
                    }
                };
                let coding_base_conflict = state.fec_sets[&fec_set_index]
                    .first_coding_index
                    .is_some_and(|first| first != first_coding_index);
                if coding_base_conflict {
                    state.block([RepairConflict::FecCodingIndex { fec_set_index }], now);
                    return;
                }
                let fec = state
                    .fec_sets
                    .get_mut(&fec_set_index)
                    .expect("the FEC set was inserted or already present");
                if fec.first_coding_index.is_none() {
                    fec.first_coding_index = Some(first_coding_index);
                    progressed = true;
                }
                progressed |= fec.coding_positions.insert(position);
                if progressed {
                    state.last_progress_at = now;
                }
            }
        }
    }

    /// Returns repair work for settled slots, ordered by slot and then data index.
    ///
    /// This describes current need, not transport state: repeated calls can return the same request.
    /// Outstanding nonce correlation, retry backoff, peer selection, and response deadlines remain
    /// the repair transport's responsibility. Both request caps are applied before returning.
    pub fn repair_plans_due(&mut self, now: Instant) -> Vec<SlotRepairPlan> {
        self.evict_expired(now);
        let mut poll_budget = self.config.max_requests_per_poll;
        let mut plans = Vec::new();
        for (&slot, state) in self
            .slots
            .iter()
            .filter(|(_, state)| elapsed(now, state.last_progress_at) >= self.config.settle_time)
        {
            let slot_budget = self.config.max_requests_per_slot.min(poll_budget);
            if let Some(plan) = plan_slot(slot, state, slot_budget) {
                poll_budget = poll_budget.saturating_sub(plan.request_count());
                plans.push(plan);
            }
        }
        plans
    }

    pub fn repair_requests_due(&mut self, now: Instant) -> Vec<RepairRequest> {
        self.repair_plans_due(now)
            .into_iter()
            .flat_map(|plan| plan.requests())
            .collect()
    }

    pub fn tracked_slot_count(&self) -> usize {
        self.slots.len()
    }

    pub fn tracked_fec_set_count(&self, slot: u64) -> usize {
        self.slots
            .get(&slot)
            .map_or(0, |state| state.fec_sets.len())
    }

    fn evict_expired(&mut self, now: Instant) {
        let retention = self.config.slot_retention;
        self.slots
            .retain(|_, state| elapsed(now, state.last_seen_at) <= retention);
    }

    fn evict_least_recent_slot(&mut self) {
        let oldest = self
            .slots
            .iter()
            .min_by_key(|(slot, state)| (state.last_seen_at, **slot))
            .map(|(slot, _)| *slot);
        if let Some(slot) = oldest {
            self.slots.remove(&slot);
        }
    }
}

fn plan_slot(slot: u64, state: &SlotState, request_limit: usize) -> Option<SlotRepairPlan> {
    let completion_index = state.completion.map(|marker| marker.index);
    if !state.conflicts.is_empty() {
        return Some(SlotRepairPlan {
            slot,
            completion_index,
            conflicts: state.conflicts.iter().cloned().collect(),
            highest_window_request: None,
            fec_threshold_missing_data_indices: Vec::new(),
            conservative_missing_data_indices: Vec::new(),
            requests_truncated: false,
        });
    }

    let scan_end = completion_index
        .or(state.highest_data_index)
        .and_then(|index| index.checked_add(1))
        .map_or(0, |end| end.min(MAX_DATA_SHREDS_PER_SLOT as u32));
    let mut mapped = vec![false; scan_end as usize];
    let mut threshold_missing = BTreeSet::new();
    let mut conservative_missing = BTreeSet::new();

    for (&fec_set_index, fec) in &state.fec_sets {
        let fec_end = fec_set_index + DATA_SHREDS_PER_FEC;
        mark_range(&mut mapped, fec_set_index, fec_end);
        let observed_data = state
            .data_indices
            .range(fec_set_index..fec_end)
            .filter(|(_, identity)| {
                identity.fec_set_index == fec_set_index && identity.merkle_root == fec.merkle_root
            })
            .count();
        let observed_coding = fec.coding_positions.len();
        let needed = DATA_SHREDS_PER_FEC as usize
            - observed_data
                .saturating_add(observed_coding)
                .min(DATA_SHREDS_PER_FEC as usize);
        threshold_missing.extend(
            (fec_set_index..fec_end)
                .filter(|index| !state.data_indices.contains_key(index))
                .take(needed),
        );
    }

    // A whole FEC set can be absent, leaving no root or parity metadata at all. Once a tail (or at
    // least a highest observed index) bounds the scan, request every uncovered hole conservatively.
    for index in 0..scan_end {
        if !mapped[index as usize] && !state.data_indices.contains_key(&index) {
            conservative_missing.insert(index);
        }
    }

    let highest_window_candidate =
        completion_index
            .is_none()
            .then_some(RepairRequest::HighestWindowIndex {
                slot,
                next_expected_data_index: state
                    .highest_data_index
                    .and_then(|index| index.checked_add(1))
                    .unwrap_or(0),
            });
    let total_candidates = usize::from(highest_window_candidate.is_some())
        + threshold_missing.len()
        + conservative_missing.len();
    let mut remaining = request_limit;
    let highest_window_request = highest_window_candidate.filter(|_| {
        if remaining == 0 {
            false
        } else {
            remaining -= 1;
            true
        }
    });
    let all_indices: BTreeSet<_> = threshold_missing
        .iter()
        .chain(&conservative_missing)
        .copied()
        .collect();
    // Chained roots authenticate their predecessor, never their successor. Selecting the highest
    // gaps first is therefore required for progress when more than one whole FEC is absent and a
    // per-slot cap is smaller than the complete gap. The plan's public vectors remain sorted for
    // deterministic reporting after this priority choice.
    let selected_indices: BTreeSet<_> = all_indices.into_iter().rev().take(remaining).collect();
    let fec_threshold_missing_data_indices = threshold_missing
        .intersection(&selected_indices)
        .copied()
        .collect();
    let conservative_missing_data_indices = conservative_missing
        .intersection(&selected_indices)
        .copied()
        .collect();
    let included = usize::from(highest_window_request.is_some()) + selected_indices.len();
    let plan = SlotRepairPlan {
        slot,
        completion_index,
        conflicts: Vec::new(),
        highest_window_request,
        fec_threshold_missing_data_indices,
        conservative_missing_data_indices,
        requests_truncated: included < total_candidates,
    };
    (!plan.is_empty()).then_some(plan)
}

fn validated_observation(shred: &Shred) -> Option<ValidatedObservation> {
    let slot = shred.slot();
    let fec_set_index = shred.fec_set_index();
    let fec_end = fec_set_index.checked_add(DATA_SHREDS_PER_FEC)?;
    let proof_entries = shred.payload().get(64).map(|variant| variant & 0x0f)?;
    if !fec_set_index.is_multiple_of(DATA_SHREDS_PER_FEC)
        || fec_end > MAX_DATA_SHREDS_PER_SLOT as u32
        || proof_entries != MERKLE_PROOF_ENTRIES_PER_FEC
    {
        return None;
    }
    let merkle_root = shred.merkle_root().ok()?.to_bytes();

    if shred.is_data() {
        let index = shred.index();
        if !(fec_set_index..fec_end).contains(&index)
            || (shred.data_complete() && index.checked_add(1) != Some(fec_end))
        {
            return None;
        }
        return Some(ValidatedObservation::Data {
            slot,
            index,
            fec_set_index,
            merkle_root,
            last_in_slot: shred.last_in_slot(),
        });
    }

    let (num_data, num_coding, position) = coding_header(shred)?;
    let first_coding_index = shred.index().checked_sub(u32::from(position))?;
    let coding_end = first_coding_index.checked_add(u32::from(CODING_SHREDS_PER_FEC))?;
    if num_data != DATA_SHREDS_PER_FEC as u16
        || num_coding != CODING_SHREDS_PER_FEC
        || position >= CODING_SHREDS_PER_FEC
        || first_coding_index != fec_set_index
        || coding_end > MAX_CODE_SHREDS_PER_SLOT as u32
    {
        return None;
    }
    Some(ValidatedObservation::Coding {
        slot,
        fec_set_index,
        merkle_root,
        first_coding_index,
        position,
    })
}

fn mark_range(mapped: &mut [bool], start: u32, end: u32) {
    let start = usize::try_from(start)
        .unwrap_or(usize::MAX)
        .min(mapped.len());
    let end = usize::try_from(end).unwrap_or(usize::MAX).min(mapped.len());
    if let Some(range) = mapped.get_mut(start..end) {
        range.fill(true);
    }
}

fn coding_header(shred: &Shred) -> Option<(u16, u16, u16)> {
    if !shred.is_code() {
        return None;
    }
    let payload = shred.payload();
    Some((
        read_u16(payload, CODING_NUM_DATA_OFFSET)?,
        read_u16(payload, CODING_NUM_CODING_OFFSET)?,
        read_u16(payload, CODING_POSITION_OFFSET)?,
    ))
}

fn read_u16(payload: &[u8], offset: usize) -> Option<u16> {
    payload
        .get(offset..offset + 2)
        .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
        .map(u16::from_le_bytes)
}

fn elapsed(now: Instant, earlier: Instant) -> Duration {
    now.checked_duration_since(earlier)
        .unwrap_or(Duration::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_hash::Hash;
    use solana_keypair::{Keypair, Signer};

    use crate::{
        repair_runtime::RepairPeer,
        repair_trust_store::{RepairTrustStore, RepairTrustStoreConfig},
    };

    const SETTLE: Duration = Duration::from_millis(200);
    const ROOT_A: MerkleRoot = [1; 32];
    const ROOT_B: MerkleRoot = [2; 32];

    fn config() -> RepairTrackerConfig {
        RepairTrackerConfig {
            settle_time: SETTLE,
            slot_retention: Duration::from_secs(10),
            max_slots: 8,
            max_fec_sets_per_slot: 8,
            max_requests_per_slot: 128,
            max_requests_per_poll: 256,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn observe_data(
        tracker: &mut RepairTracker,
        slot: u64,
        index: u32,
        fec_set_index: u32,
        root: MerkleRoot,
        last_in_slot: bool,
        now: Instant,
    ) {
        tracker.observe_validated(
            ValidatedObservation::Data {
                slot,
                index,
                fec_set_index,
                merkle_root: root,
                last_in_slot,
            },
            now,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn observe_coding(
        tracker: &mut RepairTracker,
        slot: u64,
        fec_set_index: u32,
        root: MerkleRoot,
        first_coding_index: u32,
        position: u16,
        now: Instant,
    ) {
        tracker.observe_validated(
            ValidatedObservation::Coding {
                slot,
                fec_set_index,
                merkle_root: root,
                first_coding_index,
                position,
            },
            now,
        );
    }

    fn raw_data_shred(slot: u64, index: u32, fec_set_index: u32) -> Shred {
        let mut payload = vec![0u8; 1_203];
        payload[64] = 0x96; // MerkleData { proof_size: 6, resigned: false }
        payload[65..73].copy_from_slice(&slot.to_le_bytes());
        payload[73..77].copy_from_slice(&index.to_le_bytes());
        payload[77..79].copy_from_slice(&1u16.to_le_bytes());
        payload[79..83].copy_from_slice(&fec_set_index.to_le_bytes());
        payload[83..85].copy_from_slice(&1u16.to_le_bytes());
        payload[85] = 0;
        payload[86..88].copy_from_slice(&88u16.to_le_bytes());
        Shred::new_from_serialized_shred(payload).unwrap()
    }

    fn signed_chained_data_shred(
        slot: u64,
        index: u32,
        version: u16,
        leader: &Keypair,
        chained_root: Hash,
    ) -> Shred {
        let mut payload = vec![0u8; 1_203];
        payload[64] = 0x90; // MerkleData { proof_size: 0, resigned: false }
        payload[65..73].copy_from_slice(&slot.to_le_bytes());
        payload[73..77].copy_from_slice(&index.to_le_bytes());
        payload[77..79].copy_from_slice(&version.to_le_bytes());
        payload[79..83].copy_from_slice(&(index / 32 * 32).to_le_bytes());
        payload[83..85].copy_from_slice(&1u16.to_le_bytes());
        payload[85] = 0b1100_0000;
        payload[86..88].copy_from_slice(&88u16.to_le_bytes());
        let chained_root_offset = payload.len() - 32;
        payload[chained_root_offset..].copy_from_slice(chained_root.as_ref());
        let unsigned = Shred::new_from_serialized_shred(payload.clone()).unwrap();
        let signature = leader.sign_message(unsigned.merkle_root().unwrap().as_ref());
        payload[..64].copy_from_slice(signature.as_ref());
        Shred::new_from_serialized_shred(payload).unwrap()
    }

    fn raw_coding_shred(
        slot: u64,
        fec_set_index: u32,
        first_coding_index: u32,
        num_data: u16,
        num_coding: u16,
        position: u16,
    ) -> Shred {
        let mut payload = vec![0u8; 1_228];
        payload[64] = 0x66; // MerkleCode { proof_size: 6, resigned: false }
        payload[65..73].copy_from_slice(&slot.to_le_bytes());
        payload[73..77].copy_from_slice(&(first_coding_index + u32::from(position)).to_le_bytes());
        payload[77..79].copy_from_slice(&1u16.to_le_bytes());
        payload[79..83].copy_from_slice(&fec_set_index.to_le_bytes());
        payload[83..85].copy_from_slice(&num_data.to_le_bytes());
        payload[85..87].copy_from_slice(&num_coding.to_le_bytes());
        payload[87..89].copy_from_slice(&position.to_le_bytes());
        Shred::new_from_serialized_shred(payload).unwrap()
    }

    #[test]
    fn waits_for_settle_and_highest_window_carries_next_expected_index() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        tracker.observe(&raw_data_shred(100, 0, 0), now);

        assert!(tracker.repair_requests_due(now + SETTLE / 2).is_empty());
        assert_eq!(
            tracker.repair_requests_due(now + SETTLE).first(),
            Some(&RepairRequest::HighestWindowIndex {
                slot: 100,
                next_expected_data_index: 1,
            })
        );
    }

    #[test]
    fn code_only_slot_uses_zero_as_highest_window_baseline() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        observe_coding(&mut tracker, 100, 0, ROOT_A, 0, 0, now);

        assert_eq!(
            tracker.repair_requests_due(now + SETTLE).first(),
            Some(&RepairRequest::HighestWindowIndex {
                slot: 100,
                next_expected_data_index: 0,
            })
        );
    }

    #[test]
    fn requests_only_enough_32_32_data_to_reach_rs_threshold() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        observe_data(&mut tracker, 100, 0, 0, ROOT_A, false, now);
        observe_data(&mut tracker, 100, 31, 0, ROOT_A, true, now);
        for position in 0..29 {
            observe_coding(&mut tracker, 100, 0, ROOT_A, 0, position, now);
        }

        let plans = tracker.repair_plans_due(now + SETTLE);
        assert_eq!(plans[0].fec_threshold_missing_data_indices, [1]);
        assert!(plans[0].conservative_missing_data_indices.is_empty());

        observe_coding(&mut tracker, 100, 0, ROOT_A, 0, 29, now + SETTLE);
        assert!(tracker.repair_plans_due(now + SETTLE * 2).is_empty());
    }

    #[test]
    fn requests_wholly_unmapped_completed_fec_conservatively() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        observe_data(&mut tracker, 100, 32, 32, ROOT_A, false, now);
        observe_data(&mut tracker, 100, 63, 32, ROOT_A, true, now);
        for position in 0..30 {
            observe_coding(&mut tracker, 100, 32, ROOT_A, 32, position, now);
        }

        let plans = tracker.repair_plans_due(now + SETTLE);
        assert!(plans[0].fec_threshold_missing_data_indices.is_empty());
        assert_eq!(
            plans[0].conservative_missing_data_indices,
            (0..32).collect::<Vec<_>>()
        );
    }

    #[test]
    fn capped_whole_fec_gaps_prioritize_the_trusted_successor_boundary() {
        let now = Instant::now();
        let mut bounded = config();
        bounded.max_requests_per_slot = 32;
        bounded.max_requests_per_poll = 32;
        let mut tracker = RepairTracker::new(bounded);

        // FECs 0 and 32 are wholly absent. FEC 64 is complete and is the only evidence capable of
        // anchoring FEC 32; selecting indices 0..31 first would deadlock after trust filtering.
        for index in 64..96 {
            observe_data(&mut tracker, 100, index, 64, ROOT_A, index == 95, now);
        }

        let plans = tracker.repair_plans_due(now + SETTLE);
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].conservative_missing_data_indices,
            (32..64).collect::<Vec<_>>()
        );
        assert!(plans[0].requests_truncated);

        // Exercise the exact production ordering: tracker cap first, then trust filtering. A
        // leader-verified successor at FEC64 anchors FEC32 but not FEC0. All 32 capped requests
        // must therefore survive the filter instead of deadlocking on unauthorized FEC0 work.
        const VERSION: u16 = 50_093;
        let leader = Keypair::new();
        let leader_pubkey = leader.pubkey();
        let peer_key = Keypair::new();
        let store = RepairTrustStore::new(
            RepairTrustStoreConfig {
                shred_version: VERSION,
                max_slots: 4,
                max_fec_sets_per_slot: 4,
                max_authorized_peers: 1,
            },
            [RepairPeer {
                pubkey: peer_key.pubkey(),
                repair_addr: "127.0.0.1:10010".parse().unwrap(),
            }],
            move |_| Some(leader_pubkey),
        )
        .unwrap();
        let successor =
            signed_chained_data_shred(100, 64, VERSION, &leader, Hash::new_from_array([9; 32]));
        store.observe_turbine_packet(successor.payload()).unwrap();
        let trusted = plans[0]
            .requests()
            .into_iter()
            .filter(|request| store.can_request(request))
            .collect::<Vec<_>>();
        assert_eq!(trusted.len(), 32);
        assert!(trusted.iter().all(|request| matches!(
            request,
            RepairRequest::WindowIndex { index, .. } if (32..64).contains(index)
        )));
    }

    #[test]
    fn duplicate_coding_does_not_inflate_parity_or_defer_settle() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        observe_data(&mut tracker, 100, 0, 0, ROOT_A, false, now);
        observe_data(&mut tracker, 100, 31, 0, ROOT_A, true, now);
        for position in 0..29 {
            observe_coding(&mut tracker, 100, 0, ROOT_A, 0, position, now);
        }
        observe_coding(&mut tracker, 100, 0, ROOT_A, 0, 28, now + SETTLE / 2);

        let plans = tracker.repair_plans_due(now + SETTLE);
        assert_eq!(plans[0].fec_threshold_missing_data_indices, [1]);
    }

    #[test]
    fn different_merkle_roots_block_fec_repair() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        tracker.observe(&raw_data_shred(100, 0, 0), now);
        tracker.observe(&raw_coding_shred(100, 0, 0, 32, 32, 0), now);

        let plans = tracker.repair_plans_due(now + SETTLE);
        assert!(plans[0].is_blocked());
        assert_eq!(
            plans[0].conflicts,
            [RepairConflict::FecMerkleRoot { fec_set_index: 0 }]
        );
        assert!(plans[0].requests().is_empty());
    }

    #[test]
    fn conflicting_identity_for_one_data_index_blocks_fork_repair() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        observe_data(&mut tracker, 100, 0, 0, ROOT_A, false, now);
        observe_data(&mut tracker, 100, 0, 0, ROOT_B, false, now);

        let plans = tracker.repair_plans_due(now + SETTLE);
        assert_eq!(
            plans[0].conflicts,
            [RepairConflict::DataIndexFork { index: 0 }]
        );
        assert!(plans[0].requests().is_empty());
    }

    #[test]
    fn conflicting_completion_and_observation_beyond_tail_block_repair() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        observe_data(&mut tracker, 100, 31, 0, ROOT_A, true, now);
        observe_data(&mut tracker, 100, 63, 32, ROOT_B, true, now);

        let plans = tracker.repair_plans_due(now + SETTLE);
        assert!(plans[0].is_blocked());
        assert!(
            plans[0]
                .conflicts
                .contains(&RepairConflict::ConflictingCompletion {
                    declared_index: 31,
                    observed_index: 63,
                })
        );
        assert!(
            plans[0]
                .conflicts
                .contains(&RepairConflict::ObservedBeyondCompletion {
                    completion_index: 31,
                    observed_index: 63,
                })
        );
    }

    #[test]
    fn ordinary_data_beyond_declared_completion_blocks_repair() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        observe_data(&mut tracker, 100, 31, 0, ROOT_A, true, now);
        observe_data(&mut tracker, 100, 32, 32, ROOT_B, false, now);

        let plans = tracker.repair_plans_due(now + SETTLE);
        assert_eq!(
            plans[0].conflicts,
            [RepairConflict::ObservedBeyondCompletion {
                completion_index: 31,
                observed_index: 32,
            }]
        );
    }

    #[test]
    fn rejects_non_32_32_and_misaligned_wire_geometry_before_tracking() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        tracker.observe(&raw_coding_shred(100, 0, 0, 4, 4, 0), now);
        tracker.observe(&raw_data_shred(100, 1, 1), now);

        assert_eq!(tracker.tracked_slot_count(), 0);
    }

    #[test]
    fn rejects_coding_index_base_that_differs_from_fec_set_index() {
        let now = Instant::now();
        let mut tracker = RepairTracker::new(config());
        tracker.observe(&raw_coding_shred(100, 0, 32, 32, 32, 0), now);

        assert_eq!(tracker.tracked_slot_count(), 0);
    }

    #[test]
    fn request_output_obeys_per_slot_and_per_poll_caps() {
        let now = Instant::now();
        let mut capped = config();
        capped.max_requests_per_slot = 3;
        capped.max_requests_per_poll = 4;
        let mut tracker = RepairTracker::new(capped);
        for slot in [100, 101] {
            observe_data(&mut tracker, slot, 0, 0, ROOT_A, false, now);
            observe_data(&mut tracker, slot, 31, 0, ROOT_A, true, now);
        }

        let plans = tracker.repair_plans_due(now + SETTLE);
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].requests().len(), 3);
        assert_eq!(plans[1].requests().len(), 1);
        assert!(plans.iter().all(|plan| plan.requests_truncated));
        assert_eq!(
            plans
                .iter()
                .map(SlotRepairPlan::request_count)
                .sum::<usize>(),
            4
        );
    }

    #[test]
    fn fec_capacity_overflow_blocks_without_evicting_partial_state() {
        let now = Instant::now();
        let mut bounded = config();
        bounded.max_fec_sets_per_slot = 1;
        let mut tracker = RepairTracker::new(bounded);
        observe_data(&mut tracker, 100, 0, 0, ROOT_A, false, now);
        observe_data(&mut tracker, 100, 32, 32, ROOT_B, false, now);

        assert_eq!(tracker.tracked_fec_set_count(100), 1);
        let plans = tracker.repair_plans_due(now + SETTLE);
        assert_eq!(
            plans[0].conflicts,
            [RepairConflict::FecCapacityExceeded {
                rejected_fec_set_index: 32,
            }]
        );
    }

    #[test]
    fn slots_are_lru_bounded_and_expire() {
        let now = Instant::now();
        let mut bounded = config();
        bounded.max_slots = 2;
        let mut tracker = RepairTracker::new(bounded);
        observe_data(&mut tracker, 100, 0, 0, ROOT_A, false, now);
        observe_data(
            &mut tracker,
            101,
            0,
            0,
            ROOT_A,
            false,
            now + Duration::from_millis(1),
        );
        observe_data(
            &mut tracker,
            102,
            0,
            0,
            ROOT_A,
            false,
            now + Duration::from_millis(2),
        );
        assert_eq!(tracker.tracked_slot_count(), 2);

        assert!(
            tracker
                .repair_plans_due(now + bounded.slot_retention + Duration::from_millis(3))
                .is_empty()
        );
        assert_eq!(tracker.tracked_slot_count(), 0);
    }

    #[test]
    #[should_panic(expected = "slot_retention must be at least settle_time")]
    fn retention_must_cover_settle_time() {
        let mut invalid = config();
        invalid.slot_retention = SETTLE / 2;
        RepairTracker::new(invalid);
    }

    #[test]
    #[should_panic(expected = "max_requests_per_slot must be nonzero")]
    fn per_slot_request_cap_must_be_nonzero() {
        let mut invalid = config();
        invalid.max_requests_per_slot = 0;
        RepairTracker::new(invalid);
    }

    #[test]
    #[should_panic(expected = "max_requests_per_poll must be nonzero")]
    fn per_poll_request_cap_must_be_nonzero() {
        let mut invalid = config();
        invalid.max_requests_per_poll = 0;
        RepairTracker::new(invalid);
    }
}
