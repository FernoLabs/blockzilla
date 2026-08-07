//! Wire types for the real `PipelineSnapshot` schema served by
//! `blockzilla-watcher-gateway` at `GET /api/v1/status` and streamed over
//! `GET /api/v1/events` (see `docs/operations/scheduler-control-protocol.md`
//! section 1, and the canonical TypeScript types in
//! `apps/blockzilla-watcher/src/lib/pipeline-snapshot.ts`, which this module
//! mirrors).
//!
//! Every struct derives `Default` and carries a container-level
//! `#[serde(default)]`: the real payload has many more fields than we read
//! here (compaction internals, tuning knobs, artifact detail, ...), and each
//! one still receives new fields over time. Deserializing tolerantly means
//! this dashboard degrades by omission instead of by refusing the whole
//! snapshot when the producer adds or reorders something we don't model yet.

use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PipelineSnapshot {
    pub schema_version: u64,
    pub sequence: u64,
    pub now_unix_secs: u64,
    pub observer_mode: bool,
    pub scheduler: SchedulerSnapshot,
    pub summary: PipelineSummary,
    pub machine: MachineStatus,
    pub epochs: Vec<EpochStatus>,
    pub lanes: Vec<LaneStatus>,
    pub live: Vec<LiveStatus>,
    pub errors: Vec<PipelineError>,
    pub recent_compactions: Vec<CompactionHistoryEntry>,
    pub process_io: Option<ProcessIoSnapshot>,
    /// Live-authoritative epoch date timings, merged over the bundled
    /// reference calendar (`calendar::reference_calendar`) -- currently
    /// always empty on the real wire (the scheduler doesn't emit this
    /// yet), but the merge is field-for-field with
    /// `apps/blockzilla-watcher/src/lib/epoch-calendar.ts` so this starts
    /// working the moment it does, with no client change needed.
    pub epoch_calendar: Vec<EpochCalendarEntry>,
}

/// One epoch's real-world date range. Mirrors `EpochCalendarEntry` in
/// `apps/blockzilla-watcher/src/lib/epoch-calendar.ts`.
#[derive(Clone, Debug, Deserialize, serde::Serialize)]
pub struct EpochCalendarEntry {
    pub epoch: u32,
    pub start_unix_secs: u64,
    pub end_unix_secs: Option<u64>,
    pub precision: CalendarPrecision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CalendarPrecision {
    Observed,
    Estimated,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct SchedulerSnapshot {
    pub paused: bool,
    pub updated_unix_secs: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PipelineSummary {
    pub epochs_total: u32,
    pub queued: u32,
    pub scanning: u32,
    pub scan_ready: u32,
    pub finalizing: u32,
    pub complete: u32,
    pub failed: u32,
    pub blocked: u32,
    pub progress_pct: f32,
    // These arrive as fractional seconds (e.g. `45585.212651516056`), not
    // whole seconds -- an earlier `Option<u64>` here made every real
    // snapshot fail to deserialize (`serde_json` refuses a float into an
    // integer field), which surfaced as a misleading "error decoding
    // response body" with no indication of which field was at fault.
    pub eta_secs: Option<f64>,
    pub queue_eta_secs: Option<f64>,
    pub queue_eta_reason: Option<String>,
    pub blocks_done: u64,
    pub blocks_total: u64,
    pub blocks_per_sec: f32,
    pub legacy_compact_running: Option<u32>,
    pub legacy_compact_paused: Option<u32>,
    // "Why" fields -- populated every tick by the scheduler but, until now,
    // never read here even though they were already on the wire. See
    // docs/operations/blockzilla-monitor-roadmap.md §4/§1.2.
    pub admission_blocked_reason: Option<String>,
    pub legacy_compact_admission_blocked_reason: Option<String>,
    pub finalizer_admission_blocked_reason: Option<String>,
    pub legacy_compact_last_action: Option<String>,
    pub legacy_compact_last_action_unix_secs: Option<u64>,
    pub legacy_compact_tuning_last_decision: Option<String>,
    pub poh_migration_bytes_total: u64,
    pub poh_migration_bytes_done: u64,
    pub poh_migration_epochs_total: u32,
    pub poh_migration_epochs_done: u32,
    pub poh_migration_bytes_per_sec: Option<f64>,
    pub poh_migration_eta_secs: Option<f64>,
    pub poh_migration_capacity_configured: u32,
    pub poh_migration_running: u32,
}

impl PipelineSummary {
    /// Mirrors the Svelte client's `historicalNeedsAction`: blocked + failed
    /// historical epochs, excluded from the runnable-queue ETA.
    pub fn needs_action(&self) -> u32 {
        self.blocked + self.failed
    }

    pub fn runnable_eta_secs(&self) -> Option<f64> {
        self.queue_eta_secs.or(self.eta_secs)
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct MachineStatus {
    pub memory_total_bytes: u64,
    pub memory_used_bytes: u64,
    pub memory_available_bytes: u64,
    pub swap_total_bytes: u64,
    pub swap_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_available_bytes: u64,
    pub load_1m: f32,
    pub archive_device_read_mib_per_sec: Option<f32>,
    pub archive_device_write_mib_per_sec: Option<f32>,
    pub memory_pressure_full_avg10: Option<f32>,
    pub io_pressure_full_avg10: Option<f32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ProgressSnapshot {
    pub blocks_done: u64,
    pub blocks_total: u64,
    pub progress_pct: Option<f32>,
    /// Fractional seconds, same reasoning as `PipelineSummary::eta_secs`.
    pub eta_secs: Option<f64>,
    pub updated_unix_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct EpochStatus {
    pub epoch: u32,
    pub state: String,
    pub progress: ProgressSnapshot,
    pub updated_unix_secs: u64,
    pub message: Option<String>,
    /// `"first_seen"` (recompactable) vs `"usage_sorted"` (canonical
    /// layout) vs `"unknown"` -- distinguishes "complete" from "complete,
    /// but still on the older first-seen registry ordering" on the
    /// calendar view. See docs/operations/archive-completion-audit-2026-08-04.md.
    pub registry_order: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct LaneStatus {
    pub id: String,
    pub kind: String,
    pub state: String,
    /// Human-readable current phase within this lane's job, e.g. `"Archive
    /// V2 Hot Write"` or `"Prev Blockhash Seed"` -- a short, fixed set of
    /// internal phase names, not free text, so it's shown unredacted like
    /// `state`/`kind`.
    pub phase: String,
    pub epoch: Option<u32>,
    pub auto_paused: bool,
    pub auto_pause_reason: Option<String>,
    pub progress: ProgressSnapshot,
}

impl LaneStatus {
    pub fn is_active(&self) -> bool {
        !matches!(
            normalize(&self.state).as_str(),
            "idle" | "done" | "complete" | "completed" | "failed" | "stopped" | "cancelled"
        )
    }

    pub fn is_paused(&self) -> bool {
        normalize(&self.state) == "paused"
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct LiveStatus {
    pub id: String,
    pub epoch: Option<u32>,
    pub state: String,
    pub is_current: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PipelineError {
    pub at_unix_secs: u64,
    pub scope: String,
    pub message: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct CompactionHistoryEntry {
    pub id: String,
    pub epoch: u32,
    pub workflow: String,
    pub completed_unix_secs: Option<u64>,
    pub duration_secs: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ProcessIoSnapshot {
    pub state: String,
    pub sampled_unix_secs: Option<u64>,
    pub sample_window_secs: Option<u64>,
    pub active_count: u32,
    pub truncated: bool,
    pub processes: Vec<ProcessIoEntry>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ProcessIoEntry {
    pub id: String,
    pub pid: u32,
    pub name: String,
    pub read_mib_per_sec: Option<f64>,
    pub write_mib_per_sec: Option<f64>,
    pub cpu_percent: Option<f64>,
    pub rss_bytes: Option<u64>,
    pub blockzilla_owned: Option<bool>,
}

/// Wire schema for `GET {upstream}/api/v1/sidecars/block-time-gaps/index.json`
/// -- a separate, slow-changing sidecar artifact (built offline by
/// `blockzilla build-block-time-gaps` + `build-block-time-gap-index`, not
/// part of the live scheduler snapshot), day-bucketing observed slot-time
/// interruptions ("big slot skip") for the calendar's outage overlay.
/// Mirrors `BlockTimeGapIndex` in
/// `apps/blockzilla-watcher/src/lib/block-time-gap-index.ts`, trimmed to
/// the fields the calendar view actually renders.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct BlockTimeGapIndex {
    pub schema_version: u64,
    pub generated_unix_secs: u64,
    pub minimum_interruption_secs: u64,
    pub coverage: BlockTimeGapCoverage,
    pub days: Vec<BlockTimeInterruptionDay>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct BlockTimeGapCoverage {
    pub start_epoch: u32,
    pub end_epoch: u32,
    pub missing_epochs: Vec<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct BlockTimeInterruptionDay {
    pub day_start_unix_secs: u64,
    pub interruption_count: u32,
    pub boundary_interruption_count: u32,
    pub longest_interruption_secs: u64,
    pub largest_missing_slots: u64,
}

/// Incremental update carried by an `event: snapshot_patch` SSE frame.
/// Mirrors `SnapshotPatch<PipelineSnapshot, EpochStatus>` in
/// `snapshot-patch.ts`: `epochs` is reconciled by key (`epochs_changed` /
/// `epochs_removed`) since it's the overwhelming majority of a snapshot's
/// bytes, and every other field simply replaces the corresponding field on
/// the base snapshot. `recent_compactions` / `process_io` are themselves
/// `Option` here (on top of already being optional on the base type) so a
/// patch that omits the key can be told apart from one that includes it as
/// empty -- see `apply_to` below.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PipelineSnapshotPatch {
    pub schema_version: u64,
    pub sequence: u64,
    pub now_unix_secs: u64,
    pub observer_mode: bool,
    pub scheduler: SchedulerSnapshot,
    pub summary: PipelineSummary,
    pub machine: MachineStatus,
    pub epochs_changed: Vec<EpochStatus>,
    pub epochs_removed: Vec<u32>,
    pub lanes: Vec<LaneStatus>,
    pub live: Vec<LiveStatus>,
    pub errors: Vec<PipelineError>,
    pub recent_compactions: Option<Vec<CompactionHistoryEntry>>,
    pub process_io: Option<ProcessIoSnapshot>,
}

impl PipelineSnapshot {
    /// Applies a patch in place: reconciles `epochs` by key, replaces every
    /// other field wholesale. Faithful port of `applySnapshotPatch` in
    /// `snapshot-patch.ts` -- see that file for the reference semantics.
    pub fn apply_patch(&mut self, patch: PipelineSnapshotPatch) {
        let mut epochs: std::collections::BTreeMap<u32, EpochStatus> =
            std::mem::take(&mut self.epochs)
                .into_iter()
                .map(|epoch| (epoch.epoch, epoch))
                .collect();
        for epoch in patch.epochs_removed {
            epochs.remove(&epoch);
        }
        for epoch in patch.epochs_changed {
            epochs.insert(epoch.epoch, epoch);
        }

        self.schema_version = patch.schema_version;
        self.sequence = patch.sequence;
        self.now_unix_secs = patch.now_unix_secs;
        self.observer_mode = patch.observer_mode;
        self.scheduler = patch.scheduler;
        self.summary = patch.summary;
        self.machine = patch.machine;
        self.lanes = patch.lanes;
        self.live = patch.live;
        self.errors = patch.errors;
        if let Some(recent_compactions) = patch.recent_compactions {
            self.recent_compactions = recent_compactions;
        }
        if patch.process_io.is_some() {
            self.process_io = patch.process_io;
        }
        self.epochs = epochs.into_values().collect();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SequenceAction {
    Apply,
    Ignore,
    Resync,
}

/// Faithful port of `snapshotPatchSequenceAction` in `snapshot-patch.ts`.
/// `last_sequence` is `-1` when there is no base snapshot yet.
pub fn sequence_action(last_sequence: i64, incoming_sequence: i64) -> SequenceAction {
    if incoming_sequence <= last_sequence {
        SequenceAction::Ignore
    } else if last_sequence < 0 || incoming_sequence > last_sequence + 1 {
        SequenceAction::Resync
    } else {
        SequenceAction::Apply
    }
}

pub fn normalize(value: &str) -> String {
    value.trim().to_lowercase().replace(['-', ' '], "_")
}

pub fn humanize(value: &str) -> String {
    if value.is_empty() {
        return "-".to_string();
    }
    value.replace(['-', '_'], " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(n: u32, state: &str) -> EpochStatus {
        EpochStatus {
            epoch: n,
            state: state.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn apply_patch_reconciles_epochs_by_key() {
        let mut snapshot = PipelineSnapshot {
            epochs: vec![
                epoch(790, "scanning"),
                epoch(791, "queued"),
                epoch(792, "scanning"),
            ],
            ..Default::default()
        };
        let patch = PipelineSnapshotPatch {
            sequence: 2,
            epochs_changed: vec![epoch(790, "finalizing"), epoch(793, "queued")],
            epochs_removed: vec![791],
            ..Default::default()
        };

        snapshot.apply_patch(patch);

        let states: Vec<(u32, String)> = snapshot
            .epochs
            .iter()
            .map(|e| (e.epoch, e.state.clone()))
            .collect();
        // 791 removed, 790 updated in place, 793 inserted, 792 untouched --
        // and sorted by epoch, same as `applySnapshotPatch` in
        // snapshot-patch.ts.
        assert_eq!(
            states,
            vec![
                (790, "finalizing".to_string()),
                (792, "scanning".to_string()),
                (793, "queued".to_string()),
            ]
        );
    }

    #[test]
    fn apply_patch_replaces_everything_else_wholesale() {
        let mut snapshot = PipelineSnapshot {
            sequence: 1,
            scheduler: SchedulerSnapshot {
                paused: false,
                updated_unix_secs: 1,
            },
            ..Default::default()
        };
        let patch = PipelineSnapshotPatch {
            sequence: 2,
            scheduler: SchedulerSnapshot {
                paused: true,
                updated_unix_secs: 2,
            },
            errors: vec![PipelineError {
                at_unix_secs: 2,
                scope: "x".into(),
                message: "y".into(),
            }],
            ..Default::default()
        };

        snapshot.apply_patch(patch);

        assert!(snapshot.scheduler.paused);
        assert_eq!(snapshot.errors.len(), 1);
    }

    #[test]
    fn apply_patch_keeps_optional_fields_when_patch_omits_them() {
        let mut snapshot = PipelineSnapshot {
            recent_compactions: vec![CompactionHistoryEntry {
                id: "a".into(),
                ..Default::default()
            }],
            process_io: Some(ProcessIoSnapshot::default()),
            ..Default::default()
        };
        let patch = PipelineSnapshotPatch::default(); // recent_compactions/process_io absent

        snapshot.apply_patch(patch);

        assert_eq!(
            snapshot.recent_compactions.len(),
            1,
            "omitted field must not be wiped"
        );
        assert!(
            snapshot.process_io.is_some(),
            "omitted field must not be wiped"
        );
    }

    #[test]
    fn sequence_action_matches_reference_semantics() {
        assert_eq!(
            sequence_action(-1, 0),
            SequenceAction::Resync,
            "no base snapshot yet"
        );
        assert_eq!(sequence_action(5, 5), SequenceAction::Ignore, "duplicate");
        assert_eq!(
            sequence_action(5, 3),
            SequenceAction::Ignore,
            "stale/out of order"
        );
        assert_eq!(sequence_action(5, 6), SequenceAction::Apply, "contiguous");
        assert_eq!(sequence_action(5, 8), SequenceAction::Resync, "gap");
    }
}
