use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

pub const OLD_FAITHFUL_SLOTS_PER_EPOCH: u64 = 432_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochSlot {
    pub slot: u64,
    pub epoch: u64,
    pub epoch_start_slot: u64,
    pub epoch_end_slot: u64,
    pub slot_index: u64,
    pub slots_per_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochBoundaryEvent {
    pub previous_epoch: u64,
    pub new_epoch: u64,
    pub boundary_slot: u64,
    pub observed_slot: u64,
    pub completed_epoch_start_slot: u64,
    pub completed_epoch_end_slot: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotRange {
    pub start_slot: u64,
    pub end_slot: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochBackfillPlan {
    pub epoch: u64,
    pub epoch_start_slot: u64,
    pub epoch_end_slot: u64,
    pub observed_slots: usize,
    pub missing_slots: u64,
    pub missing_ranges: Vec<SlotRange>,
}

#[derive(Debug, Clone)]
pub struct EpochBoundaryTracker {
    slots_per_epoch: u64,
    current_epoch: Option<u64>,
    last_observed_slot: Option<u64>,
}

impl EpochSlot {
    pub fn from_slot(slot: u64, slots_per_epoch: u64) -> Self {
        let slots_per_epoch = slots_per_epoch.max(1);
        let epoch = slot / slots_per_epoch;
        let epoch_start_slot = epoch * slots_per_epoch;
        let epoch_end_slot = epoch_start_slot + slots_per_epoch - 1;
        Self {
            slot,
            epoch,
            epoch_start_slot,
            epoch_end_slot,
            slot_index: slot - epoch_start_slot,
            slots_per_epoch,
        }
    }
}

impl SlotRange {
    pub fn len(&self) -> u64 {
        self.end_slot
            .saturating_sub(self.start_slot)
            .saturating_add(1)
    }

    pub fn is_empty(&self) -> bool {
        self.end_slot < self.start_slot
    }
}

impl EpochBoundaryTracker {
    pub fn new(slots_per_epoch: u64) -> Self {
        Self {
            slots_per_epoch: slots_per_epoch.max(1),
            current_epoch: None,
            last_observed_slot: None,
        }
    }

    pub fn current_epoch(&self) -> Option<u64> {
        self.current_epoch
    }

    pub fn last_observed_slot(&self) -> Option<u64> {
        self.last_observed_slot
    }

    pub fn seed(&mut self, epoch: u64, observed_slot: Option<u64>) {
        self.current_epoch = Some(epoch);
        if let Some(slot) = observed_slot {
            self.last_observed_slot =
                Some(self.last_observed_slot.map_or(slot, |last| last.max(slot)));
        }
    }

    pub fn observe_slot(&mut self, slot: u64) -> Vec<EpochBoundaryEvent> {
        let observed = EpochSlot::from_slot(slot, self.slots_per_epoch);
        self.last_observed_slot = Some(self.last_observed_slot.map_or(slot, |last| last.max(slot)));

        let Some(previous_epoch) = self.current_epoch else {
            self.current_epoch = Some(observed.epoch);
            return Vec::new();
        };

        if observed.epoch <= previous_epoch {
            return Vec::new();
        }

        let mut events = Vec::new();
        for completed_epoch in previous_epoch..observed.epoch {
            let new_epoch = completed_epoch + 1;
            let completed_start = completed_epoch * self.slots_per_epoch;
            events.push(EpochBoundaryEvent {
                previous_epoch: completed_epoch,
                new_epoch,
                boundary_slot: new_epoch * self.slots_per_epoch,
                observed_slot: slot,
                completed_epoch_start_slot: completed_start,
                completed_epoch_end_slot: completed_start + self.slots_per_epoch - 1,
            });
        }
        self.current_epoch = Some(observed.epoch);
        events
    }
}

pub fn epoch_start_slot(epoch: u64, slots_per_epoch: u64) -> u64 {
    epoch * slots_per_epoch.max(1)
}

pub fn epoch_end_slot(epoch: u64, slots_per_epoch: u64) -> u64 {
    epoch_start_slot(epoch, slots_per_epoch) + slots_per_epoch.max(1) - 1
}

pub fn old_faithful_epoch_slot(slot: u64) -> EpochSlot {
    EpochSlot::from_slot(slot, OLD_FAITHFUL_SLOTS_PER_EPOCH)
}

pub fn plan_epoch_backfill(
    epoch: u64,
    observed_slots: &BTreeSet<u64>,
    slots_per_epoch: u64,
) -> EpochBackfillPlan {
    let slots_per_epoch = slots_per_epoch.max(1);
    let start = epoch_start_slot(epoch, slots_per_epoch);
    let end = epoch_end_slot(epoch, slots_per_epoch);

    let mut missing_ranges = Vec::new();
    let mut range_start = None;
    let mut observed_count = 0usize;
    for slot in start..=end {
        if observed_slots.contains(&slot) {
            observed_count += 1;
            if let Some(start_slot) = range_start.take() {
                missing_ranges.push(SlotRange {
                    start_slot,
                    end_slot: slot - 1,
                });
            }
        } else if range_start.is_none() {
            range_start = Some(slot);
        }
    }
    if let Some(start_slot) = range_start {
        missing_ranges.push(SlotRange {
            start_slot,
            end_slot: end,
        });
    }

    EpochBackfillPlan {
        epoch,
        epoch_start_slot: start,
        epoch_end_slot: end,
        observed_slots: observed_count,
        missing_slots: missing_ranges.iter().map(SlotRange::len).sum(),
        missing_ranges,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_slot_uses_old_faithful_boundaries() {
        let last_epoch_0 = EpochSlot::from_slot(
            OLD_FAITHFUL_SLOTS_PER_EPOCH - 1,
            OLD_FAITHFUL_SLOTS_PER_EPOCH,
        );
        assert_eq!(last_epoch_0.epoch, 0);
        assert_eq!(last_epoch_0.slot_index, OLD_FAITHFUL_SLOTS_PER_EPOCH - 1);

        let first_epoch_1 =
            EpochSlot::from_slot(OLD_FAITHFUL_SLOTS_PER_EPOCH, OLD_FAITHFUL_SLOTS_PER_EPOCH);
        assert_eq!(first_epoch_1.epoch, 1);
        assert_eq!(first_epoch_1.slot_index, 0);
    }

    #[test]
    fn tracker_emits_crossed_boundaries() {
        let mut tracker = EpochBoundaryTracker::new(10);
        assert!(tracker.observe_slot(9).is_empty());
        let events = tracker.observe_slot(21);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].previous_epoch, 0);
        assert_eq!(events[0].new_epoch, 1);
        assert_eq!(events[0].boundary_slot, 10);
        assert_eq!(events[1].previous_epoch, 1);
        assert_eq!(events[1].new_epoch, 2);
        assert_eq!(events[1].boundary_slot, 20);
    }

    #[test]
    fn backfill_plan_groups_missing_ranges() {
        let observed = [0, 1, 4, 9].into_iter().collect::<BTreeSet<_>>();
        let plan = plan_epoch_backfill(0, &observed, 10);
        assert_eq!(plan.observed_slots, 4);
        assert_eq!(plan.missing_slots, 6);
        assert_eq!(
            plan.missing_ranges,
            vec![
                SlotRange {
                    start_slot: 2,
                    end_slot: 3
                },
                SlotRange {
                    start_slot: 5,
                    end_slot: 8
                }
            ]
        );
    }
}
