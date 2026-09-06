//! Wire types for the real `PipelineSnapshot` schema served by
//! the Blockzilla scheduler at `GET /api/v1/status` and streamed over
//! `GET /api/v1/events` (see `docs/operations/scheduler-control-protocol.md`
//! section 1, and the canonical TypeScript types in
//! `web/blockzilla-watcher/src/lib/pipeline-snapshot.ts`, which this module
//! mirrors).
//!
//! Unknown fields remain accepted, so schema-v3 can grow additive optional
//! detail without breaking this client. Core fields are intentionally not
//! container-defaulted: omitting `summary`, `machine`, or a row's identity
//! must fail closed instead of silently becoming a plausible zero. The
//! validation pass below then enforces v3, finite/ranged metrics, internal
//! summary consistency, unique identities, and bounded collections.

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

pub const STATUS_SCHEMA_VERSION: u64 = 3;
const MAX_EPOCHS: usize = 4_096;
const MAX_LANES: usize = 1_024;
const MAX_LIVE_CAPTURES: usize = 4_096;
const MAX_ERRORS: usize = 256;
const MAX_COMPACTIONS: usize = 4_096;
const MAX_PROCESS_ROWS: usize = 64;
const MAX_CALENDAR_ENTRIES: usize = 4_096;
const MAX_GAP_DAYS: usize = 16_384;
const MAX_IDENTIFIER_BYTES: usize = 1_024;
const MAX_TEXT_BYTES: usize = 16 * 1_024;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
    #[serde(default)]
    pub recent_compactions: Vec<CompactionHistoryEntry>,
    #[serde(default)]
    pub process_io: Option<ProcessIoSnapshot>,
    /// Live-authoritative epoch date timings, merged over the bundled
    /// reference calendar (`calendar::reference_calendar`) -- currently
    /// always empty on the real wire (the scheduler doesn't emit this
    /// yet), but the merge is field-for-field with
    /// `web/blockzilla-watcher/src/lib/epoch-calendar.ts` so this starts
    /// working the moment it does, with no client change needed.
    #[serde(default)]
    pub epoch_calendar: Vec<EpochCalendarEntry>,
}

/// One epoch's real-world date range. Mirrors `EpochCalendarEntry` in
/// `web/blockzilla-watcher/src/lib/epoch-calendar.ts`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EpochCalendarEntry {
    pub epoch: u32,
    pub start_unix_secs: u64,
    pub end_unix_secs: Option<u64>,
    pub precision: CalendarPrecision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CalendarPrecision {
    Observed,
    Estimated,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SchedulerSnapshot {
    pub paused: bool,
    pub updated_unix_secs: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
    /// First-seen registry generations that must be rewritten to historical
    /// usage-sorted order. These fields were added to schema v3 after the
    /// monitor shipped, so defaults keep the monitor compatible with an
    /// older schema-v3 scheduler during a rolling deployment.
    #[serde(default)]
    pub registry_reprocess_capacity_configured: u32,
    #[serde(default)]
    pub registry_reprocess_running: u32,
    #[serde(default)]
    pub registry_reprocess_epochs_total: u32,
    #[serde(default)]
    pub registry_reprocess_epochs_done: u32,
    #[serde(default)]
    pub registry_reprocess_admission_blocked_reason: Option<String>,
    /// Scheduler-owned Firewatch signer-to-program index project. Defaults
    /// preserve compatibility with an older schema-v3 scheduler during a
    /// rolling deployment.
    #[serde(default)]
    pub firewatch_index_capacity_configured: u32,
    #[serde(default)]
    pub firewatch_index_running: u32,
    #[serde(default)]
    pub firewatch_index_epochs_total: u32,
    #[serde(default)]
    pub firewatch_index_epochs_accepted: u32,
    #[serde(default)]
    pub firewatch_index_epochs_queued: u32,
    /// Additive schema-1 controller coverage fields. `None` means the
    /// connected controller predates all-archive coverage reporting.
    #[serde(default)]
    pub firewatch_index_archive_epochs_total: Option<u32>,
    #[serde(default)]
    pub firewatch_index_epochs_eligible: Option<u32>,
    #[serde(default)]
    pub firewatch_index_epochs_blocked_migration: Option<u32>,
    #[serde(default)]
    pub firewatch_index_epochs_blocked_wire_profile: Option<u32>,
    #[serde(default)]
    pub firewatch_index_queue_eta_secs: Option<f64>,
    #[serde(default)]
    pub firewatch_index_admission_blocked_reason: Option<String>,
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ProgressSnapshot {
    pub blocks_done: u64,
    pub blocks_total: u64,
    pub progress_pct: Option<f32>,
    /// Fractional seconds, same reasoning as `PipelineSummary::eta_secs`.
    pub eta_secs: Option<f64>,
    #[serde(default)]
    pub input_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub source_read_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub disk_read_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub disk_write_mib_per_sec: Option<f64>,
    #[serde(default)]
    pub rss_bytes: Option<u64>,
    #[serde(default)]
    pub peak_rss_bytes: Option<u64>,
    pub updated_unix_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
    #[serde(default)]
    pub rss_bytes: Option<u64>,
    /// Counts and parity become available after the index manifest/parity
    /// receipt is read. They remain absent during early build phases.
    #[serde(default)]
    pub wallet_count: Option<u64>,
    #[serde(default)]
    pub relation_count: Option<u64>,
    #[serde(default)]
    pub parity_status: Option<String>,
}

impl LaneStatus {
    pub fn is_active(&self) -> bool {
        !matches!(
            normalize(&self.state).as_str(),
            "idle"
                | "queued"
                | "done"
                | "complete"
                | "completed"
                | "accepted"
                | "failed"
                | "blocked"
                | "profile_audit_required"
                | "stopped"
                | "cancelled"
        )
    }

    pub fn is_paused(&self) -> bool {
        normalize(&self.state) == "paused"
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LiveStatus {
    pub id: String,
    pub epoch: Option<u32>,
    pub state: String,
    pub is_current: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PipelineError {
    pub at_unix_secs: u64,
    pub scope: String,
    pub message: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CompactionHistoryEntry {
    pub id: String,
    pub epoch: u32,
    pub workflow: String,
    pub completed_unix_secs: Option<u64>,
    pub duration_secs: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ProcessIoSnapshot {
    pub state: String,
    pub sampled_unix_secs: Option<u64>,
    pub sample_window_secs: Option<u64>,
    pub active_count: u32,
    pub truncated: bool,
    pub processes: Vec<ProcessIoEntry>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
/// `web/blockzilla-watcher/src/lib/block-time-gap-index.ts`, trimmed to
/// the fields the calendar view actually renders.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct BlockTimeGapIndex {
    pub schema_version: u64,
    pub generated_unix_secs: u64,
    pub minimum_interruption_secs: u64,
    pub coverage: BlockTimeGapCoverage,
    pub days: Vec<BlockTimeInterruptionDay>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct BlockTimeGapCoverage {
    pub start_epoch: u32,
    pub end_epoch: u32,
    pub missing_epochs: Vec<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
    #[serde(default)]
    pub recent_compactions: Option<Vec<CompactionHistoryEntry>>,
    #[serde(default)]
    pub process_io: Option<ProcessIoSnapshot>,
}

impl PipelineSnapshot {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == STATUS_SCHEMA_VERSION,
            "unsupported scheduler status schema {}; expected {STATUS_SCHEMA_VERSION}",
            self.schema_version
        );
        ensure!(
            self.sequence <= i64::MAX as u64,
            "scheduler sequence exceeds the supported signed range"
        );
        ensure!(self.now_unix_secs > 0, "scheduler timestamp is missing");
        ensure!(
            self.epochs.len() <= MAX_EPOCHS,
            "scheduler epoch collection exceeds {MAX_EPOCHS} rows"
        );
        ensure!(
            self.lanes.len() <= MAX_LANES,
            "scheduler lane collection exceeds {MAX_LANES} rows"
        );
        ensure!(
            self.live.len() <= MAX_LIVE_CAPTURES,
            "scheduler live-capture collection exceeds {MAX_LIVE_CAPTURES} rows"
        );
        ensure!(
            self.errors.len() <= MAX_ERRORS,
            "scheduler error collection exceeds {MAX_ERRORS} rows"
        );
        ensure!(
            self.recent_compactions.len() <= MAX_COMPACTIONS,
            "scheduler compaction collection exceeds {MAX_COMPACTIONS} rows"
        );
        ensure!(
            self.epoch_calendar.len() <= MAX_CALENDAR_ENTRIES,
            "scheduler calendar collection exceeds {MAX_CALENDAR_ENTRIES} rows"
        );

        validate_summary(&self.summary)?;
        validate_machine(&self.machine)?;
        validate_epochs(&self.epochs, Some(&self.summary))?;
        validate_lanes(&self.lanes)?;
        validate_live(&self.live)?;
        validate_errors(&self.errors)?;
        validate_compactions(&self.recent_compactions)?;
        validate_process_io(self.process_io.as_ref())?;
        validate_calendar(&self.epoch_calendar)?;
        Ok(())
    }

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

impl PipelineSnapshotPatch {
    pub fn validate_shape(&self) -> Result<()> {
        ensure!(
            self.schema_version == STATUS_SCHEMA_VERSION,
            "unsupported scheduler patch schema {}; expected {STATUS_SCHEMA_VERSION}",
            self.schema_version
        );
        ensure!(
            self.sequence <= i64::MAX as u64,
            "scheduler patch sequence exceeds the supported signed range"
        );
        ensure!(
            self.now_unix_secs > 0,
            "scheduler patch timestamp is missing"
        );
        ensure!(
            self.epochs_changed.len() <= MAX_EPOCHS,
            "scheduler changed-epoch collection exceeds {MAX_EPOCHS} rows"
        );
        ensure!(
            self.epochs_removed.len() <= MAX_EPOCHS,
            "scheduler removed-epoch collection exceeds {MAX_EPOCHS} rows"
        );
        ensure!(
            self.lanes.len() <= MAX_LANES,
            "scheduler patch lane collection exceeds {MAX_LANES} rows"
        );
        ensure!(
            self.live.len() <= MAX_LIVE_CAPTURES,
            "scheduler patch live-capture collection exceeds {MAX_LIVE_CAPTURES} rows"
        );
        ensure!(
            self.errors.len() <= MAX_ERRORS,
            "scheduler patch error collection exceeds {MAX_ERRORS} rows"
        );
        if let Some(compactions) = &self.recent_compactions {
            ensure!(
                compactions.len() <= MAX_COMPACTIONS,
                "scheduler patch compaction collection exceeds {MAX_COMPACTIONS} rows"
            );
            validate_compactions(compactions)?;
        }

        validate_summary(&self.summary)?;
        validate_machine(&self.machine)?;
        validate_epochs(&self.epochs_changed, None)?;
        validate_lanes(&self.lanes)?;
        validate_live(&self.live)?;
        validate_errors(&self.errors)?;
        validate_process_io(self.process_io.as_ref())?;

        let changed: std::collections::BTreeSet<_> = self
            .epochs_changed
            .iter()
            .map(|epoch| epoch.epoch)
            .collect();
        let removed: std::collections::BTreeSet<_> = self.epochs_removed.iter().copied().collect();
        ensure!(
            changed.len() == self.epochs_changed.len(),
            "scheduler patch contains duplicate changed epochs"
        );
        ensure!(
            removed.len() == self.epochs_removed.len(),
            "scheduler patch contains duplicate removed epochs"
        );
        ensure!(
            changed.is_disjoint(&removed),
            "scheduler patch changes and removes the same epoch"
        );
        Ok(())
    }
}

impl BlockTimeGapIndex {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported block-time-gap schema"
        );
        ensure!(
            self.generated_unix_secs > 0,
            "gap index timestamp is missing"
        );
        ensure!(
            self.minimum_interruption_secs > 1,
            "gap index interruption threshold is invalid"
        );
        ensure!(
            self.coverage.start_epoch <= self.coverage.end_epoch,
            "gap index coverage is reversed"
        );
        let expected = usize::try_from(
            u64::from(self.coverage.end_epoch) - u64::from(self.coverage.start_epoch) + 1,
        )
        .unwrap_or(usize::MAX);
        ensure!(
            expected <= MAX_EPOCHS,
            "gap index coverage exceeds {MAX_EPOCHS} epochs"
        );
        ensure!(
            self.coverage.missing_epochs.len() <= expected,
            "gap index has more missing epochs than its coverage"
        );
        ensure!(
            self.days.len() <= MAX_GAP_DAYS,
            "gap index exceeds {MAX_GAP_DAYS} day rows"
        );

        let mut previous_missing = None;
        for epoch in &self.coverage.missing_epochs {
            ensure!(
                *epoch >= self.coverage.start_epoch && *epoch <= self.coverage.end_epoch,
                "gap index missing epoch lies outside coverage"
            );
            ensure!(
                previous_missing.is_none_or(|previous| *epoch > previous),
                "gap index missing epochs are not unique and sorted"
            );
            previous_missing = Some(*epoch);
        }
        let mut previous_day = None;
        for day in &self.days {
            ensure!(
                day.day_start_unix_secs > 0 && day.day_start_unix_secs % 86_400 == 0,
                "gap index contains a non-UTC day boundary"
            );
            ensure!(
                previous_day.is_none_or(|previous| day.day_start_unix_secs > previous),
                "gap index days are not unique and sorted"
            );
            ensure!(
                day.interruption_count > 0
                    && day.boundary_interruption_count <= day.interruption_count,
                "gap index day counters are inconsistent"
            );
            previous_day = Some(day.day_start_unix_secs);
        }
        Ok(())
    }
}

fn validate_summary(summary: &PipelineSummary) -> Result<()> {
    ensure_percent("summary progress", summary.progress_pct as f64)?;
    ensure_nonnegative("summary block rate", summary.blocks_per_sec as f64)?;
    ensure_optional_nonnegative("summary ETA", summary.eta_secs)?;
    ensure_optional_nonnegative("summary queue ETA", summary.queue_eta_secs)?;
    ensure_optional_nonnegative("PoH migration rate", summary.poh_migration_bytes_per_sec)?;
    ensure_optional_nonnegative("PoH migration ETA", summary.poh_migration_eta_secs)?;
    ensure_optional_nonnegative(
        "Firewatch queue ETA",
        summary.firewatch_index_queue_eta_secs,
    )?;
    ensure_optional_text("queue ETA reason", summary.queue_eta_reason.as_deref())?;
    ensure_optional_text(
        "admission blocked reason",
        summary.admission_blocked_reason.as_deref(),
    )?;
    ensure_optional_text(
        "legacy admission blocked reason",
        summary.legacy_compact_admission_blocked_reason.as_deref(),
    )?;
    ensure_optional_text(
        "finalizer admission blocked reason",
        summary.finalizer_admission_blocked_reason.as_deref(),
    )?;
    ensure_optional_text(
        "registry reprocess admission blocked reason",
        summary
            .registry_reprocess_admission_blocked_reason
            .as_deref(),
    )?;
    ensure_optional_text(
        "Firewatch index admission blocked reason",
        summary.firewatch_index_admission_blocked_reason.as_deref(),
    )?;
    ensure_optional_text(
        "legacy compact last action",
        summary.legacy_compact_last_action.as_deref(),
    )?;
    ensure_optional_text(
        "legacy tuning decision",
        summary.legacy_compact_tuning_last_decision.as_deref(),
    )?;

    let classified = [
        summary.queued,
        summary.scanning,
        summary.scan_ready,
        summary.finalizing,
        summary.complete,
        summary.failed,
        summary.blocked,
    ]
    .into_iter()
    .map(u64::from)
    .sum::<u64>();
    ensure!(
        classified == u64::from(summary.epochs_total),
        "scheduler summary state counts do not equal epochs_total"
    );
    ensure!(
        summary.poh_migration_bytes_done <= summary.poh_migration_bytes_total,
        "PoH migrated bytes exceed total bytes"
    );
    ensure!(
        summary.poh_migration_epochs_done <= summary.poh_migration_epochs_total,
        "PoH migrated epochs exceed total epochs"
    );
    ensure!(
        summary.registry_reprocess_epochs_done <= summary.registry_reprocess_epochs_total,
        "registry reprocess completed epochs exceed total epochs"
    );
    ensure!(
        summary.registry_reprocess_running <= summary.registry_reprocess_capacity_configured,
        "registry reprocess running workers exceed configured capacity"
    );
    ensure!(
        summary.firewatch_index_epochs_accepted <= summary.firewatch_index_epochs_total,
        "Firewatch accepted epochs exceed total epochs"
    );
    ensure!(
        summary.firewatch_index_epochs_queued <= summary.firewatch_index_epochs_total,
        "Firewatch queued epochs exceed total epochs"
    );
    ensure!(
        summary.firewatch_index_running <= summary.firewatch_index_capacity_configured,
        "Firewatch running workers exceed configured capacity"
    );
    if let Some(eligible) = summary.firewatch_index_epochs_eligible {
        ensure!(
            summary.firewatch_index_epochs_accepted <= eligible,
            "Firewatch accepted epochs exceed eligible epochs"
        );
        ensure!(
            summary.firewatch_index_running <= eligible,
            "Firewatch running epochs exceed eligible epochs"
        );
        ensure!(
            summary.firewatch_index_epochs_queued <= eligible,
            "Firewatch queued epochs exceed eligible epochs"
        );
    }
    if summary
        .firewatch_index_epochs_blocked_wire_profile
        .is_some()
    {
        ensure!(
            summary.firewatch_index_archive_epochs_total.is_some()
                && summary.firewatch_index_epochs_eligible.is_some()
                && summary.firewatch_index_epochs_blocked_migration.is_some(),
            "Firewatch wire-profile coverage is incomplete"
        );
    }
    if let Some(archive_total) = summary.firewatch_index_archive_epochs_total {
        if let Some(eligible) = summary.firewatch_index_epochs_eligible {
            ensure!(
                eligible <= archive_total,
                "Firewatch eligible epochs exceed archive scope"
            );
        }
        if let Some(blocked) = summary.firewatch_index_epochs_blocked_migration {
            ensure!(
                blocked <= archive_total,
                "Firewatch migration-blocked epochs exceed archive scope"
            );
        }
        if let Some(blocked) = summary.firewatch_index_epochs_blocked_wire_profile {
            ensure!(
                blocked <= archive_total,
                "Firewatch wire-profile-blocked epochs exceed archive scope"
            );
        }
        if let (Some(eligible), Some(blocked_migration), Some(blocked_wire_profile)) = (
            summary.firewatch_index_epochs_eligible,
            summary.firewatch_index_epochs_blocked_migration,
            summary.firewatch_index_epochs_blocked_wire_profile,
        ) {
            ensure!(
                u64::from(eligible)
                    + u64::from(blocked_migration)
                    + u64::from(blocked_wire_profile)
                    == u64::from(archive_total),
                "Firewatch coverage classes do not equal archive scope"
            );
        } else if let (Some(eligible), Some(blocked)) = (
            summary.firewatch_index_epochs_eligible,
            summary.firewatch_index_epochs_blocked_migration,
        ) {
            // Preserve schema-v3 snapshots from controllers that predate
            // wire-profile coverage. Their unclassified remainder is valid.
            ensure!(
                u64::from(eligible) + u64::from(blocked) <= u64::from(archive_total),
                "Firewatch eligible and migration-blocked epochs exceed archive scope"
            );
        }
    }
    Ok(())
}

fn validate_machine(machine: &MachineStatus) -> Result<()> {
    ensure!(
        machine.memory_used_bytes <= machine.memory_total_bytes
            && machine.memory_available_bytes <= machine.memory_total_bytes,
        "machine memory counters exceed total memory"
    );
    ensure!(
        machine.swap_used_bytes <= machine.swap_total_bytes,
        "machine swap counters exceed total swap"
    );
    ensure!(
        machine.disk_used_bytes <= machine.disk_total_bytes
            && machine.disk_available_bytes <= machine.disk_total_bytes,
        "machine disk counters exceed total disk"
    );
    ensure_nonnegative("machine load", machine.load_1m as f64)?;
    ensure_optional_nonnegative_f32(
        "archive device read rate",
        machine.archive_device_read_mib_per_sec,
    )?;
    ensure_optional_nonnegative_f32(
        "archive device write rate",
        machine.archive_device_write_mib_per_sec,
    )?;
    ensure_optional_percent("memory pressure", machine.memory_pressure_full_avg10)?;
    ensure_optional_percent("I/O pressure", machine.io_pressure_full_avg10)?;
    Ok(())
}

fn validate_epochs(epochs: &[EpochStatus], summary: Option<&PipelineSummary>) -> Result<()> {
    let mut ids = std::collections::BTreeSet::new();
    let mut counts = [0_u32; 7];
    for epoch in epochs {
        ensure!(
            ids.insert(epoch.epoch),
            "scheduler contains duplicate epochs"
        );
        let state_index = match epoch.state.as_str() {
            "queued" => 0,
            "scanning" => 1,
            "scan_ready" => 2,
            "finalizing" => 3,
            "complete" => 4,
            "failed" => 5,
            "blocked" => 6,
            _ => anyhow::bail!("scheduler epoch has an unknown state"),
        };
        counts[state_index] += 1;
        validate_progress(&epoch.progress)?;
        ensure_optional_text("epoch message", epoch.message.as_deref())?;
        ensure!(
            matches!(
                epoch.registry_order.as_deref(),
                Some("usage_sorted" | "first_seen" | "unknown")
            ),
            "scheduler epoch registry_order is missing or invalid"
        );
    }
    if let Some(summary) = summary {
        ensure!(
            usize::try_from(summary.epochs_total).ok() == Some(epochs.len()),
            "scheduler summary epochs_total does not match the epoch collection"
        );
        ensure!(
            counts
                == [
                    summary.queued,
                    summary.scanning,
                    summary.scan_ready,
                    summary.finalizing,
                    summary.complete,
                    summary.failed,
                    summary.blocked,
                ],
            "scheduler summary state counts do not match epoch rows"
        );
    }
    Ok(())
}

fn validate_lanes(lanes: &[LaneStatus]) -> Result<()> {
    let mut ids = std::collections::BTreeSet::new();
    for lane in lanes {
        ensure_identifier("lane id", &lane.id)?;
        ensure!(
            ids.insert(lane.id.as_str()),
            "scheduler contains duplicate lane ids"
        );
        ensure_identifier("lane kind", &lane.kind)?;
        ensure_identifier("lane state", &lane.state)?;
        ensure_text_len("lane phase", &lane.phase, MAX_IDENTIFIER_BYTES)?;
        ensure_optional_text("lane pause reason", lane.auto_pause_reason.as_deref())?;
        ensure_optional_text("lane parity status", lane.parity_status.as_deref())?;
        validate_progress(&lane.progress)?;
    }
    Ok(())
}

fn validate_live(live: &[LiveStatus]) -> Result<()> {
    let mut ids = std::collections::BTreeSet::new();
    for capture in live {
        ensure_identifier("live capture id", &capture.id)?;
        ensure!(
            ids.insert(capture.id.as_str()),
            "scheduler contains duplicate live-capture ids"
        );
        ensure!(
            matches!(
                capture.state.as_str(),
                "capturing"
                    | "repair_gate"
                    | "repair_required"
                    | "ready_to_package"
                    | "packaging"
                    | "packaged"
                    | "complete"
                    | "failed"
                    | "blocked"
            ),
            "scheduler live capture has an unknown state"
        );
    }
    Ok(())
}

fn validate_errors(errors: &[PipelineError]) -> Result<()> {
    for error in errors {
        ensure_identifier("error scope", &error.scope)?;
        ensure_text_len("error message", &error.message, MAX_TEXT_BYTES)?;
    }
    Ok(())
}

fn validate_compactions(entries: &[CompactionHistoryEntry]) -> Result<()> {
    let mut ids = std::collections::BTreeSet::new();
    for entry in entries {
        ensure_identifier("compaction id", &entry.id)?;
        ensure!(ids.insert(entry.id.as_str()), "duplicate compaction ids");
        ensure_identifier("compaction workflow", &entry.workflow)?;
        ensure_optional_nonnegative("compaction duration", entry.duration_secs)?;
    }
    Ok(())
}

fn validate_process_io(process_io: Option<&ProcessIoSnapshot>) -> Result<()> {
    let Some(process_io) = process_io else {
        return Ok(());
    };
    ensure_identifier("process I/O state", &process_io.state)?;
    ensure!(
        process_io.processes.len() <= MAX_PROCESS_ROWS,
        "process I/O collection exceeds {MAX_PROCESS_ROWS} rows"
    );
    ensure!(
        usize::try_from(process_io.active_count).unwrap_or(usize::MAX)
            >= process_io.processes.len(),
        "process I/O active_count is smaller than the row collection"
    );
    let mut ids = std::collections::BTreeSet::new();
    for process in &process_io.processes {
        ensure_identifier("process id", &process.id)?;
        ensure!(ids.insert(process.id.as_str()), "duplicate process ids");
        ensure_text_len("process name", &process.name, MAX_IDENTIFIER_BYTES)?;
        ensure_optional_nonnegative("process read rate", process.read_mib_per_sec)?;
        ensure_optional_nonnegative("process write rate", process.write_mib_per_sec)?;
        ensure_optional_nonnegative("process CPU", process.cpu_percent)?;
    }
    Ok(())
}

fn validate_calendar(entries: &[EpochCalendarEntry]) -> Result<()> {
    let mut ids = std::collections::BTreeSet::new();
    for entry in entries {
        ensure!(ids.insert(entry.epoch), "duplicate calendar epochs");
        ensure!(entry.start_unix_secs > 0, "calendar start time is missing");
        ensure!(
            entry
                .end_unix_secs
                .is_none_or(|end| end >= entry.start_unix_secs),
            "calendar epoch ends before it starts"
        );
    }
    Ok(())
}

fn validate_progress(progress: &ProgressSnapshot) -> Result<()> {
    if let Some(percent) = progress.progress_pct {
        ensure_percent("row progress", percent as f64)?;
    }
    ensure_optional_nonnegative("row ETA", progress.eta_secs)?;
    ensure_optional_nonnegative("row input rate", progress.input_mib_per_sec)?;
    ensure_optional_nonnegative("row source read rate", progress.source_read_mib_per_sec)?;
    ensure_optional_nonnegative("row disk read rate", progress.disk_read_mib_per_sec)?;
    ensure_optional_nonnegative("row disk write rate", progress.disk_write_mib_per_sec)?;
    Ok(())
}

fn ensure_identifier(label: &str, value: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{label} is empty");
    ensure_text_len(label, value, MAX_IDENTIFIER_BYTES)
}

fn ensure_optional_text(label: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        ensure_text_len(label, value, MAX_TEXT_BYTES)?;
    }
    Ok(())
}

fn ensure_text_len(label: &str, value: &str, max: usize) -> Result<()> {
    ensure!(value.len() <= max, "{label} exceeds {max} bytes");
    Ok(())
}

fn ensure_nonnegative(label: &str, value: f64) -> Result<()> {
    ensure!(
        value.is_finite() && value >= 0.0,
        "{label} is not finite and nonnegative"
    );
    Ok(())
}

fn ensure_optional_nonnegative(label: &str, value: Option<f64>) -> Result<()> {
    if let Some(value) = value {
        ensure_nonnegative(label, value)?;
    }
    Ok(())
}

fn ensure_optional_nonnegative_f32(label: &str, value: Option<f32>) -> Result<()> {
    ensure_optional_nonnegative(label, value.map(f64::from))
}

fn ensure_percent(label: &str, value: f64) -> Result<()> {
    ensure!(
        value.is_finite() && (0.0..=100.0).contains(&value),
        "{label} is outside 0..=100"
    );
    Ok(())
}

fn ensure_optional_percent(label: &str, value: Option<f32>) -> Result<()> {
    if let Some(value) = value {
        ensure_percent(label, f64::from(value))?;
    }
    Ok(())
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

    fn valid_snapshot() -> PipelineSnapshot {
        PipelineSnapshot {
            schema_version: STATUS_SCHEMA_VERSION,
            sequence: 1,
            now_unix_secs: 1,
            ..Default::default()
        }
    }

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

    #[test]
    fn schema_v3_accepts_unknown_additive_fields_but_requires_core_fields() {
        let mut value = serde_json::to_value(valid_snapshot()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("future_optional_detail".into(), serde_json::json!({"x": 1}));
        let decoded: PipelineSnapshot = serde_json::from_value(value.clone()).unwrap();
        decoded.validate().unwrap();

        value.as_object_mut().unwrap().remove("summary");
        assert!(
            serde_json::from_value::<PipelineSnapshot>(value).is_err(),
            "missing core objects must not silently default to zero"
        );
    }

    #[test]
    fn validation_rejects_wrong_schema_inconsistent_summary_and_oversized_rows() {
        let mut wrong_schema = valid_snapshot();
        wrong_schema.schema_version = 2;
        assert!(wrong_schema.validate().is_err());

        let mut inconsistent = valid_snapshot();
        inconsistent.epochs = vec![EpochStatus {
            epoch: 1,
            state: "queued".into(),
            registry_order: Some("unknown".into()),
            ..Default::default()
        }];
        assert!(inconsistent.validate().is_err());

        let mut oversized = valid_snapshot();
        oversized.epochs = vec![EpochStatus::default(); MAX_EPOCHS + 1];
        assert!(oversized.validate().is_err());

        let mut invalid_registry = valid_snapshot();
        invalid_registry.summary.registry_reprocess_epochs_total = 2;
        invalid_registry.summary.registry_reprocess_epochs_done = 3;
        assert!(invalid_registry.validate().is_err());

        let mut excess_registry_workers = valid_snapshot();
        excess_registry_workers
            .summary
            .registry_reprocess_capacity_configured = 1;
        excess_registry_workers.summary.registry_reprocess_running = 2;
        assert!(excess_registry_workers.validate().is_err());

        let mut invalid_firewatch = valid_snapshot();
        invalid_firewatch.summary.firewatch_index_epochs_total = 1;
        invalid_firewatch.summary.firewatch_index_epochs_accepted = 2;
        assert!(invalid_firewatch.validate().is_err());

        let mut excess_firewatch_workers = valid_snapshot();
        excess_firewatch_workers
            .summary
            .firewatch_index_capacity_configured = 1;
        excess_firewatch_workers.summary.firewatch_index_running = 2;
        assert!(excess_firewatch_workers.validate().is_err());

        let mut invalid_firewatch_coverage = valid_snapshot();
        invalid_firewatch_coverage
            .summary
            .firewatch_index_archive_epochs_total = Some(736);
        invalid_firewatch_coverage
            .summary
            .firewatch_index_epochs_eligible = Some(730);
        invalid_firewatch_coverage
            .summary
            .firewatch_index_epochs_blocked_migration = Some(7);
        assert!(invalid_firewatch_coverage.validate().is_err());

        let mut complete_firewatch_coverage = valid_snapshot();
        complete_firewatch_coverage
            .summary
            .firewatch_index_archive_epochs_total = Some(10);
        complete_firewatch_coverage
            .summary
            .firewatch_index_epochs_total = 3;
        complete_firewatch_coverage
            .summary
            .firewatch_index_epochs_eligible = Some(2);
        complete_firewatch_coverage
            .summary
            .firewatch_index_epochs_blocked_migration = Some(7);
        complete_firewatch_coverage
            .summary
            .firewatch_index_epochs_blocked_wire_profile = Some(1);
        complete_firewatch_coverage.validate().unwrap();
        complete_firewatch_coverage
            .summary
            .firewatch_index_epochs_blocked_wire_profile = Some(2);
        assert!(complete_firewatch_coverage.validate().is_err());

        let mut invalid_firewatch_eta = valid_snapshot();
        invalid_firewatch_eta.summary.firewatch_index_queue_eta_secs = Some(-1.0);
        assert!(invalid_firewatch_eta.validate().is_err());
    }

    #[test]
    fn older_schema_v3_payload_defaults_firewatch_fields() {
        let mut value = serde_json::to_value(valid_snapshot()).unwrap();
        let summary = value["summary"].as_object_mut().unwrap();
        for key in [
            "firewatch_index_capacity_configured",
            "firewatch_index_running",
            "firewatch_index_epochs_total",
            "firewatch_index_epochs_accepted",
            "firewatch_index_epochs_queued",
            "firewatch_index_archive_epochs_total",
            "firewatch_index_epochs_eligible",
            "firewatch_index_epochs_blocked_migration",
            "firewatch_index_epochs_blocked_wire_profile",
            "firewatch_index_queue_eta_secs",
            "firewatch_index_admission_blocked_reason",
        ] {
            summary.remove(key);
        }

        let decoded: PipelineSnapshot = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.summary.firewatch_index_epochs_total, 0);
        assert_eq!(decoded.summary.firewatch_index_epochs_accepted, 0);
        assert!(
            decoded
                .summary
                .firewatch_index_archive_epochs_total
                .is_none()
        );
        assert!(decoded.summary.firewatch_index_epochs_eligible.is_none());
        assert!(
            decoded
                .summary
                .firewatch_index_epochs_blocked_migration
                .is_none()
        );
        assert!(
            decoded
                .summary
                .firewatch_index_epochs_blocked_wire_profile
                .is_none()
        );
        assert!(decoded.summary.firewatch_index_queue_eta_secs.is_none());
        assert!(
            decoded
                .summary
                .firewatch_index_admission_blocked_reason
                .is_none()
        );
        decoded.validate().unwrap();
    }

    #[test]
    fn patch_validation_rejects_duplicate_or_conflicting_epoch_operations() {
        let changed = EpochStatus {
            epoch: 7,
            state: "queued".into(),
            registry_order: Some("unknown".into()),
            ..Default::default()
        };
        let patch = PipelineSnapshotPatch {
            schema_version: STATUS_SCHEMA_VERSION,
            sequence: 2,
            now_unix_secs: 2,
            epochs_changed: vec![changed],
            epochs_removed: vec![7],
            ..Default::default()
        };
        assert!(patch.validate_shape().is_err());
    }
}
