//! In-memory dashboard state.
//!
//! `set_snapshot` (called from `client.rs`, or from the demo simulator
//! below) is the single write path: it maps a real `snapshot::PipelineSnapshot`
//! into the flatter `DashboardState` the views bind to, stores it in
//! memory, diffs it against the last published signal map, and broadcasts
//! only the keys that changed (`publish` / `diff_signals` below) to every
//! connected SSE client as a `PatchSignals` payload. Datastar merges that
//! into each browser's signal store, and every `data-text` /
//! `data-attr:style` binding in the page updates itself without us
//! touching the DOM from the server.

use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::snapshot::{self, PipelineSnapshot};

/// Controls how much of a real snapshot reaches the rendered dashboard.
/// This binary has no authentication of its own -- see
/// `docs/operations/blockzilla-monitor-roadmap.md` §3 -- so the tier is
/// fixed for the lifetime of one running instance via `--tier`, and the
/// intended deployment is *two* instances: one `Public`, reachable from
/// the open internet, and one `Full`, reachable only from a network-gated
/// path (Cloudflare Access, Tailscale, an IP allowlist -- never this
/// binary's own code).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum RedactionTier {
    /// Anonymous, public-internet-safe. Drops the process table entirely
    /// (real process names/PIDs are host fingerprinting, not something a
    /// public viewer needs) and runs free-text fields through the same
    /// path/credential scrubber `blockzilla-watcher-gateway`'s
    /// `public_json` already applies upstream -- defense in depth, not a
    /// second independent redaction policy, in case that filter is ever
    /// bypassed or the wire schema grows a field it doesn't know about.
    #[default]
    Public,
    /// Full detail, including the process table and unscrubbed text.
    /// Must only be reachable from a gated deployment.
    Full,
}

static TIER: OnceLock<RedactionTier> = OnceLock::new();

/// Set once at startup from `--tier`. Safe to call at most once; later
/// calls are ignored (the first one -- the real CLI-driven call in
/// `main.rs` -- wins, matching `OnceLock` semantics elsewhere in this
/// file).
pub fn set_tier(tier: RedactionTier) {
    let _ = TIER.set(tier);
}

fn tier() -> RedactionTier {
    TIER.get().copied().unwrap_or_default()
}

/// Defense-in-depth text scrub for the public tier -- a no-op pass-through
/// on the full tier. Reuses `blockzilla-watcher-gateway`'s already-audited
/// regexes rather than a second, possibly-divergent implementation. Takes
/// `tier` explicitly (rather than reading the global) so `from_snapshot`
/// stays a pure function of its inputs and is testable for both tiers
/// without mutating shared process-global state.
fn redact_text(tier: RedactionTier, text: &str) -> String {
    match tier {
        RedactionTier::Public => blockzilla_watcher_gateway::public_json::sanitize_public_string(text),
        RedactionTier::Full => text.to_string(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EpochTask {
    pub epoch: u32,
    pub label: String,
    pub pct: u8,
    pub blocks: u64,
    pub eta_secs: u64,
    /// True for a `failed`/`blocked` epoch with no matching entry in
    /// `lanes[]` -- nothing is actively retrying it, so there's no live
    /// process to kill and it's just stale clutter on the Overview page.
    /// `epoch_list` (Overview) hides these; `epoch_table` (the /epochs
    /// page) still shows everything, since that page's whole job is full
    /// visibility, not a decluttered summary.
    pub hidden_from_overview: bool,
}

impl EpochTask {
    pub fn dom_id(&self) -> String {
        format!("epoch-{}", self.epoch)
    }

    pub fn eta_label(&self) -> String {
        format_duration(self.eta_secs)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactionHistoryEntry {
    pub id: String,
    pub epoch: u32,
    pub workflow: String,
    pub completed_unix_secs: Option<u64>,
    pub duration_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorEntry {
    pub at_unix_secs: u64,
    pub scope: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct MachineSnapshot {
    pub load_1m: f32,
    pub memory_available_bytes: u64,
    pub memory_total_bytes: u64,
    pub swap_used_bytes: u64,
    pub swap_total_bytes: u64,
    pub disk_available_bytes: u64,
    pub disk_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub archive_device_read_mib_per_sec: f32,
    pub archive_device_write_mib_per_sec: f32,
    pub pressure_memory_full_avg10: f32,
    pub pressure_io_full_avg10: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessEntry {
    pub id: String,
    pub name: String,
    pub pid: u32,
    pub cpu_percent: Option<f64>,
    pub rss_bytes: Option<u64>,
    pub read_mib_per_sec: Option<f64>,
    pub write_mib_per_sec: Option<f64>,
}

/// A lane the scheduler has auto-paused, with why -- the "reasoning" panel
/// only lists paused lanes, not every lane, since a running lane needs no
/// explanation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PausedLane {
    pub id: String,
    pub kind: String,
    pub reason: Option<String>,
}

/// Scheduler decision data that's already on the wire (populated every
/// tick by the scheduler) but wasn't previously read by this dashboard --
/// see docs/operations/blockzilla-monitor-roadmap.md §4. Rendered
/// server-side only, like `errors`/`compactions`/`processes`; refreshed on
/// the next full page load rather than patched live over SSE, consistent
/// with how those other list-shaped panels already work.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct SchedulerReasoning {
    pub admission_blocked_reason: Option<String>,
    pub legacy_compact_admission_blocked_reason: Option<String>,
    pub finalizer_admission_blocked_reason: Option<String>,
    pub legacy_compact_last_action: Option<String>,
    pub legacy_compact_last_action_unix_secs: Option<u64>,
    pub legacy_compact_tuning_last_decision: Option<String>,
    pub paused_lanes: Vec<PausedLane>,
}

impl SchedulerReasoning {
    pub fn is_empty(&self) -> bool {
        self.admission_blocked_reason.is_none()
            && self.legacy_compact_admission_blocked_reason.is_none()
            && self.finalizer_admission_blocked_reason.is_none()
            && self.legacy_compact_last_action.is_none()
            && self.legacy_compact_tuning_last_decision.is_none()
            && self.paused_lanes.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DashboardState {
    /// True once we have ever received a real (or demo) snapshot. Drives
    /// whether pages render live content or `service_unavailable`.
    pub live: bool,
    pub connection_state: String,
    pub connection_message: String,
    pub demo: bool,
    pub scheduler_paused: bool,
    pub updated_unix_secs: u64,
    pub runnable_eta_secs: Option<u64>,
    pub queued: u32,
    pub needs_action: u32,
    pub archive_complete: u32,
    pub archive_total: u32,
    pub poh_migration_epochs_done: u32,
    pub poh_migration_epochs_total: u32,
    /// Bytes back the progress percentage (epochs aren't equal-sized work)
    /// and the small "processed" subtext under the epoch count -- but
    /// epoch counts are the primary, at-a-glance number. See
    /// `poh_migration_epoch_label`/`poh_migration_bytes_label`.
    pub poh_migration_bytes_done: u64,
    pub poh_migration_bytes_total: u64,
    pub poh_migration_eta_secs: Option<u64>,
    pub live_capture_active: bool,
    pub epochs: Vec<EpochTask>,
    /// The currently-running `poh_signature_count_migration` lanes, one row
    /// per active worker. These operate on already-archive-complete epochs
    /// (a post-hoc sidecar backfill, not part of building the archive), so
    /// they never appear in `epochs` above -- that list is filtered to
    /// non-complete epochs. Without this, the only PoH migration visibility
    /// was the aggregate byte total and an undifferentiated `tasks_active`
    /// count, with no way to see which epochs were actually in flight.
    pub poh_migration_lanes: Vec<EpochTask>,
    pub tasks_active: u32,
    pub tasks_paused: u32,
    pub error_count: u32,
    pub errors: Vec<ErrorEntry>,
    pub compactions: Vec<CompactionHistoryEntry>,
    pub machine: MachineSnapshot,
    pub processes: Vec<ProcessEntry>,
    pub process_io_active_count: u32,
    /// True when the upstream snapshot had process entries but this
    /// instance's tier dropped them -- lets the UI say "hidden" instead
    /// of the misleading "no telemetry reported" it'd otherwise show.
    pub process_io_hidden: bool,
    pub reasoning: SchedulerReasoning,
}

impl Default for DashboardState {
    fn default() -> Self {
        DashboardState {
            live: false,
            connection_state: "connecting".into(),
            connection_message: "Connecting to blockzilla-watcher-gateway".into(),
            demo: false,
            scheduler_paused: false,
            updated_unix_secs: 0,
            runnable_eta_secs: None,
            queued: 0,
            needs_action: 0,
            archive_complete: 0,
            archive_total: 0,
            poh_migration_epochs_done: 0,
            poh_migration_epochs_total: 0,
            poh_migration_bytes_done: 0,
            poh_migration_bytes_total: 0,
            poh_migration_eta_secs: None,
            live_capture_active: false,
            epochs: Vec::new(),
            poh_migration_lanes: Vec::new(),
            tasks_active: 0,
            tasks_paused: 0,
            error_count: 0,
            errors: Vec::new(),
            compactions: Vec::new(),
            machine: MachineSnapshot::default(),
            processes: Vec::new(),
            process_io_active_count: 0,
            process_io_hidden: false,
            reasoning: SchedulerReasoning::default(),
        }
    }
}

impl DashboardState {
    pub fn archive_pct(&self) -> f32 {
        100.0 * self.archive_complete as f32 / self.archive_total.max(1) as f32
    }

    /// Divides in `f64` before narrowing to `f32`: at TB-scale byte counts (~10^12), casting
    /// the raw `u64` operands straight to `f32` first loses real precision (`f32` only carries
    /// ~7 significant digits) -- narrowing only the final 0-100 result is lossless for display.
    pub fn poh_migration_pct(&self) -> f32 {
        (100.0 * self.poh_migration_bytes_done as f64 / self.poh_migration_bytes_total.max(1) as f64) as f32
    }

    /// "812 done \u{b7} 197 remaining" -- the primary, at-a-glance count. Bytes still drive the
    /// percentage (epochs aren't equal-sized work) but are relegated to
    /// `poh_migration_bytes_label`'s small subtext, since "how many epochs are left" is what
    /// operators actually want to read first.
    pub fn poh_migration_epoch_label(&self) -> String {
        let remaining = self.poh_migration_epochs_total.saturating_sub(self.poh_migration_epochs_done);
        format!(
            "{} done \u{b7} {} remaining",
            format_thousands(self.poh_migration_epochs_done as u64),
            format_thousands(remaining as u64)
        )
    }

    /// "2.3 TiB / 6.0 TiB processed" -- small subtext under `poh_migration_epoch_label`, not
    /// the primary number. Still the basis for the progress percentage.
    pub fn poh_migration_bytes_label(&self) -> String {
        format!(
            "{} / {} processed",
            format_bytes(self.poh_migration_bytes_done),
            format_bytes(self.poh_migration_bytes_total)
        )
    }

    pub fn poh_migration_eta_label(&self) -> String {
        match self.poh_migration_eta_secs {
            Some(secs) => format_duration(secs),
            None => "unknown".to_string(),
        }
    }

    pub fn eta_label(&self) -> String {
        match self.runnable_eta_secs {
            Some(secs) => format_duration(secs),
            None => "unknown".to_string(),
        }
    }

    /// Flattens state into the exact signal names the views bind to
    /// (`$queued`, `$archive_pct`, `$epoch_790_pct`, ...). This -- not the
    /// nested struct itself -- is what `PatchSignals::json` should send,
    /// since Datastar's signal store is a flat-ish object keyed by these
    /// names, not a Rust type it deserializes into.
    pub fn to_signals(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("live".into(), self.live.into());
        map.insert("connection_state".into(), self.connection_state.clone().into());
        map.insert("runnable_eta_label".into(), self.eta_label().into());
        map.insert("queued".into(), self.queued.into());
        map.insert("needs_action".into(), self.needs_action.into());
        map.insert("archive_complete".into(), self.archive_complete.into());
        map.insert("archive_total".into(), self.archive_total.into());
        // Sent as pre-formatted, capped-decimal strings (`format_pct`) rather than raw floats:
        // rounding the *value* to one decimal isn't enough, since 38.4 has no exact binary
        // representation -- a raw f32/f64 in a JSON number re-expands to something like
        // `38.400001525878906` once Datastar reads it as a JS number for a live
        // `data-text`/`data-attr:style` patch (the initial server-rendered HTML looks fine
        // either way, since that's already a pre-formatted string -- it's only the
        // live-patched value that was ever wrong).
        map.insert("archive_pct".into(), format_pct(self.archive_pct()).into());
        map.insert("poh_migration_epoch_label".into(), self.poh_migration_epoch_label().into());
        map.insert("poh_migration_bytes_label".into(), self.poh_migration_bytes_label().into());
        map.insert("poh_migration_eta_label".into(), self.poh_migration_eta_label().into());
        map.insert("poh_migration_pct".into(), format_pct(self.poh_migration_pct()).into());
        map.insert("live_capture_active".into(), self.live_capture_active.into());
        map.insert("load_1m".into(), format_pct(self.machine.load_1m).into());
        map.insert("error_count".into(), self.error_count.into());
        map.insert("tasks_active".into(), self.tasks_active.into());
        map.insert("tasks_paused".into(), self.tasks_paused.into());
        map.insert("scheduler_paused".into(), self.scheduler_paused.into());
        map.insert("history_count".into(), self.compactions.len().into());
        map.insert("process_io_active_count".into(), self.process_io_active_count.into());

        // `poh_migration_lanes` never overlaps `epochs` by epoch number (an
        // epoch is either still building, in `epochs`, or already
        // archive-complete and migration-eligible, in `poh_migration_lanes`
        // -- never both), so both can share the same `epoch_{N}_*` signal
        // names without collision.
        for task in self.epochs.iter().chain(self.poh_migration_lanes.iter()) {
            let sig = format!("epoch_{}", task.epoch);
            map.insert(format!("{sig}_pct"), task.pct.into());
            map.insert(format!("{sig}_blocks"), format_thousands(task.blocks).into());
            map.insert(format!("{sig}_eta"), task.eta_label().into());
        }

        serde_json::Value::Object(map)
    }

    fn from_snapshot(snapshot: &PipelineSnapshot, tier: RedactionTier) -> Self {
        let summary = &snapshot.summary;
        let machine = &snapshot.machine;

        let epochs = snapshot
            .epochs
            .iter()
            .filter(|epoch| epoch.state != "complete")
            .map(|epoch| {
                let is_failed_or_blocked = matches!(epoch.state.as_str(), "failed" | "blocked");
                let has_active_process = snapshot.lanes.iter().any(|lane| lane.epoch == Some(epoch.epoch));
                EpochTask {
                    epoch: epoch.epoch,
                    label: snapshot::humanize(&epoch.state),
                    pct: epoch
                        .progress
                        .progress_pct
                        .unwrap_or(0.0)
                        .clamp(0.0, 100.0) as u8,
                    blocks: epoch.progress.blocks_done,
                    eta_secs: epoch.progress.eta_secs.unwrap_or(0.0).max(0.0).round() as u64,
                    hidden_from_overview: is_failed_or_blocked && !has_active_process,
                }
            })
            .take(12)
            .collect();

        let poh_migration_lanes = snapshot
            .lanes
            .iter()
            .filter(|lane| lane.kind == "poh_signature_count_migration")
            .filter_map(|lane| {
                let epoch = lane.epoch?;
                Some(EpochTask {
                    epoch,
                    label: snapshot::humanize(&lane.state),
                    pct: lane
                        .progress
                        .progress_pct
                        .unwrap_or(0.0)
                        .clamp(0.0, 100.0) as u8,
                    blocks: lane.progress.blocks_done,
                    eta_secs: lane.progress.eta_secs.unwrap_or(0.0).max(0.0).round() as u64,
                    hidden_from_overview: false,
                })
            })
            .collect();

        let paused_lanes = snapshot
            .lanes
            .iter()
            .filter(|lane| lane.auto_paused)
            .map(|lane| PausedLane {
                id: redact_text(tier, &lane.id),
                kind: redact_text(tier, &lane.kind),
                reason: lane.auto_pause_reason.as_deref().map(|reason| redact_text(tier, reason)),
            })
            .collect();

        let reasoning = SchedulerReasoning {
            admission_blocked_reason: summary.admission_blocked_reason.as_deref().map(|r| redact_text(tier, r)),
            legacy_compact_admission_blocked_reason: summary
                .legacy_compact_admission_blocked_reason
                .as_deref()
                .map(|r| redact_text(tier, r)),
            finalizer_admission_blocked_reason: summary
                .finalizer_admission_blocked_reason
                .as_deref()
                .map(|r| redact_text(tier, r)),
            legacy_compact_last_action: summary.legacy_compact_last_action.as_deref().map(|r| redact_text(tier, r)),
            legacy_compact_last_action_unix_secs: summary.legacy_compact_last_action_unix_secs,
            legacy_compact_tuning_last_decision: summary
                .legacy_compact_tuning_last_decision
                .as_deref()
                .map(|r| redact_text(tier, r)),
            paused_lanes,
        };

        let mut errors: Vec<ErrorEntry> = snapshot
            .errors
            .iter()
            .map(|error| ErrorEntry {
                at_unix_secs: error.at_unix_secs,
                scope: redact_text(tier, &error.scope),
                message: redact_text(tier, &error.message),
            })
            .collect();
        errors.sort_by_key(|error| std::cmp::Reverse(error.at_unix_secs));
        errors.truncate(20);

        let compactions = snapshot
            .recent_compactions
            .iter()
            .map(|entry| CompactionHistoryEntry {
                id: redact_text(tier, &entry.id),
                epoch: entry.epoch,
                workflow: entry.workflow.clone(),
                completed_unix_secs: entry.completed_unix_secs,
                duration_secs: entry.duration_secs.map(|secs| secs.round() as u64),
            })
            .collect();

        // The process table is real host fingerprinting (names/PIDs of
        // whatever else runs on the box) with no value to a public
        // read-only viewer -- dropped wholesale on the public tier rather
        // than field-by-field, per the exposure audit in
        // docs/operations/blockzilla-monitor-roadmap.md §2.
        let raw_processes = snapshot.process_io.as_ref().map(|io| io.processes.as_slice()).unwrap_or_default();
        let has_visible_processes = raw_processes.iter().any(|process| process.blockzilla_owned != Some(true));
        let process_io_hidden = tier == RedactionTier::Public && has_visible_processes;
        let processes = if tier == RedactionTier::Public {
            Vec::new()
        } else {
            raw_processes
                .iter()
                .filter(|process| process.blockzilla_owned != Some(true))
                .map(|process| ProcessEntry {
                    id: process.id.clone(),
                    name: process.name.clone(),
                    pid: process.pid,
                    cpu_percent: process.cpu_percent,
                    rss_bytes: process.rss_bytes,
                    read_mib_per_sec: process.read_mib_per_sec,
                    write_mib_per_sec: process.write_mib_per_sec,
                })
                .take(15)
                .collect()
        };

        DashboardState {
            live: true,
            connection_state: "live".into(),
            connection_message: "Live event stream".into(),
            demo: false,
            scheduler_paused: snapshot.scheduler.paused,
            updated_unix_secs: snapshot.now_unix_secs,
            runnable_eta_secs: summary.runnable_eta_secs().map(|secs| secs.max(0.0).round() as u64),
            queued: summary.queued,
            needs_action: summary.needs_action(),
            archive_complete: summary.complete,
            archive_total: summary.epochs_total,
            poh_migration_epochs_done: summary.poh_migration_epochs_done,
            poh_migration_epochs_total: summary.poh_migration_epochs_total,
            poh_migration_bytes_done: summary.poh_migration_bytes_done,
            poh_migration_bytes_total: summary.poh_migration_bytes_total,
            poh_migration_eta_secs: summary
                .poh_migration_eta_secs
                .filter(|secs| secs.is_finite() && *secs >= 0.0)
                .map(|secs| secs.round() as u64),
            live_capture_active: snapshot
                .live
                .iter()
                .any(|capture| capture.state == "capturing"),
            epochs,
            poh_migration_lanes,
            tasks_active: snapshot.lanes.iter().filter(|l| l.is_active() && !l.is_paused()).count() as u32,
            tasks_paused: snapshot.lanes.iter().filter(|l| l.is_paused()).count() as u32,
            error_count: snapshot.errors.len() as u32,
            errors,
            compactions,
            machine: MachineSnapshot {
                load_1m: machine.load_1m,
                memory_available_bytes: machine.memory_available_bytes,
                memory_total_bytes: machine.memory_total_bytes,
                swap_used_bytes: machine.swap_used_bytes,
                swap_total_bytes: machine.swap_total_bytes,
                disk_available_bytes: machine.disk_available_bytes,
                disk_total_bytes: machine.disk_total_bytes,
                disk_used_bytes: machine.disk_used_bytes,
                archive_device_read_mib_per_sec: machine.archive_device_read_mib_per_sec.unwrap_or(0.0),
                archive_device_write_mib_per_sec: machine.archive_device_write_mib_per_sec.unwrap_or(0.0),
                pressure_memory_full_avg10: machine.memory_pressure_full_avg10.unwrap_or(0.0),
                pressure_io_full_avg10: machine.io_pressure_full_avg10.unwrap_or(0.0),
            },
            processes,
            process_io_active_count: snapshot
                .process_io
                .as_ref()
                .map(|io| io.active_count)
                .unwrap_or(0),
            process_io_hidden,
            reasoning,
        }
    }
}

/// One decimal place, pre-formatted as a string -- see the comment at its
/// call sites in `to_signals` for why a raw rounded float isn't enough.
fn format_pct(v: f32) -> String {
    format!("{v:.1}")
}

/// Shared with `components.rs` so the initial server-render and the live
/// SSE patches always format the same way.
pub fn format_thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i != 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out.chars().rev().collect()
}

pub fn format_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {s}s")
    } else {
        format!("{s}s")
    }
}

/// Shared with `components.rs` so the initial server-render and the live
/// SSE patches always format the same way.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

struct Shared {
    current: Mutex<DashboardState>,
    /// The signal map from the most recent broadcast (or the initial
    /// `DashboardState::default()` before anything has published), kept
    /// only to diff the next update against. Every subscriber gets the
    /// same delta stream -- see `publish` for why that's safe even though
    /// subscribers can join at different times.
    last_signals: Mutex<serde_json::Value>,
    tx: broadcast::Sender<serde_json::Value>,
    /// The raw ingredients the `/calendar` page needs that `DashboardState`
    /// doesn't otherwise keep: the *full* epoch list (the main dashboard
    /// only retains the top 12 non-complete epochs) and any
    /// live-authoritative `epoch_calendar` entries. Kept separately and
    /// computed into a year-grid lazily, only when `/calendar` is actually
    /// requested, rather than on every snapshot tick -- unlike the rest of
    /// `DashboardState`, nothing here needs to be live-patched over SSE.
    calendar_source: Mutex<CalendarSource>,
    /// The block-time-gap sidecar (`GET /api/v1/sidecars/block-time-gaps/index.json`),
    /// polled independently and much less frequently by `client.rs` since
    /// it's a slow-changing offline batch artifact, not live telemetry.
    /// `None` covers both "never fetched yet" and "upstream doesn't have
    /// it" -- `gap_index_error` distinguishes them for the UI.
    gap_index: Mutex<Option<snapshot::BlockTimeGapIndex>>,
    gap_index_error: Mutex<Option<String>>,
    /// The most recent snapshot from `client.rs`, kept so `runtime_operations.rs`'s
    /// independently-ticking process-I/O sampler can be merged in and
    /// re-published without waiting for the next upstream snapshot/patch.
    /// `None` both before the first snapshot arrives and after `set_offline`
    /// -- see `recompute_and_publish`.
    last_snapshot: Mutex<Option<PipelineSnapshot>>,
    /// Locally-collected process I/O (see `runtime_operations.rs`), merged
    /// into `last_snapshot` on every publish. The scheduler this dashboard
    /// talks to directly does not emit `process_io` itself, so this is the
    /// sole source for it today.
    local_process_io: Mutex<Option<snapshot::ProcessIoSnapshot>>,
}

/// See `Shared::calendar_source`.
#[derive(Clone, Default)]
struct CalendarSource {
    epochs: Vec<snapshot::EpochStatus>,
    epoch_calendar: Vec<snapshot::EpochCalendarEntry>,
    now_unix_secs: u64,
}

static SHARED: OnceLock<Shared> = OnceLock::new();

fn shared() -> &'static Shared {
    SHARED.get_or_init(|| {
        let (tx, _rx) = broadcast::channel(64);
        let initial = DashboardState::default();
        Shared {
            last_signals: Mutex::new(initial.to_signals()),
            current: Mutex::new(initial),
            tx,
            calendar_source: Mutex::new(CalendarSource::default()),
            gap_index: Mutex::new(None),
            gap_index_error: Mutex::new(None),
            last_snapshot: Mutex::new(None),
            local_process_io: Mutex::new(None),
        }
    })
}

/// Current snapshot, for the initial server-rendered page. `async` for a
/// uniform call site (`snapshot().await` from view code) even though the
/// lock underneath is a plain `std::sync::Mutex` -- the critical section is
/// a `Clone`, never held across an `.await`, so there's nothing to gain from
/// an async mutex here.
pub async fn snapshot() -> DashboardState {
    shared().current.lock().expect("state mutex poisoned").clone()
}

/// The full current signal map, matching what the SSR page seeds into
/// `data-signals`. Used to open a new `/api/stream` connection and to
/// self-heal a client that fell behind the broadcast buffer -- both cases
/// need a complete, not incremental, payload.
pub fn full_signals() -> serde_json::Value {
    shared().last_signals.lock().expect("state mutex poisoned").clone()
}

/// Subscribe to future updates. Every `/api/stream` connection gets its own
/// receiver; `broadcast` means one upstream feeds every open browser tab.
/// Each item is a signal-map *delta*: only keys that changed since the
/// previous publish, with removed keys carrying `null` so Datastar deletes
/// them from the client's signal store (see the `datastar` docs: a signal
/// set to `null` in a patch is removed, not zeroed).
pub fn subscribe() -> broadcast::Receiver<serde_json::Value> {
    shared().tx.subscribe()
}

fn publish(state: DashboardState) {
    let shared = shared();
    let new_signals = state.to_signals();

    *shared.current.lock().expect("state mutex poisoned") = state;

    let mut last_signals = shared.last_signals.lock().expect("state mutex poisoned");
    let delta = diff_signals(&last_signals, &new_signals);
    *last_signals = new_signals;
    drop(last_signals);

    // Nothing actually changed (e.g. a snapshot_patch that only touched
    // fields this dashboard doesn't surface) -- skip the broadcast instead
    // of sending an empty patch to every open tab.
    if let serde_json::Value::Object(map) = &delta
        && map.is_empty()
    {
        return;
    }
    let _ = shared.tx.send(delta);
}

/// Keys present (with a different value) in `new` but not `old`, or
/// present in `old` but absent from `new` (emitted as `null` so Datastar
/// removes them). Both `old` and `new` are always the flat objects
/// `DashboardState::to_signals` produces.
fn diff_signals(old: &serde_json::Value, new: &serde_json::Value) -> serde_json::Value {
    let (Some(old), Some(new)) = (old.as_object(), new.as_object()) else {
        return new.clone();
    };
    let mut delta = serde_json::Map::new();
    for (key, value) in new {
        if old.get(key) != Some(value) {
            delta.insert(key.clone(), value.clone());
        }
    }
    for key in old.keys() {
        if !new.contains_key(key) {
            delta.insert(key.clone(), serde_json::Value::Null);
        }
    }
    serde_json::Value::Object(delta)
}

/// Called by `client.rs` whenever a fresh real snapshot arrives.
pub fn set_snapshot(snapshot: PipelineSnapshot) {
    *shared().calendar_source.lock().expect("state mutex poisoned") = CalendarSource {
        epochs: snapshot.epochs.clone(),
        epoch_calendar: snapshot.epoch_calendar.clone(),
        now_unix_secs: snapshot.now_unix_secs,
    };
    *shared().last_snapshot.lock().expect("state mutex poisoned") = Some(snapshot);
    recompute_and_publish();
}

/// Called by `runtime_operations.rs` on its own ~5s sampling tick,
/// independent of when the next upstream snapshot/patch happens to arrive.
pub fn set_local_process_io(io: snapshot::ProcessIoSnapshot) {
    *shared().local_process_io.lock().expect("state mutex poisoned") = Some(io);
    recompute_and_publish();
}

/// Merges the last upstream snapshot with the last locally-collected
/// process I/O sample and publishes the result. A no-op before the first
/// snapshot arrives, and after `set_offline` -- there is nothing live to
/// merge into, and publishing here would otherwise let a `runtime_operations.rs`
/// tick that lands while offline resurrect a stale "live" dashboard.
fn recompute_and_publish() {
    let Some(mut snapshot) = shared().last_snapshot.lock().expect("state mutex poisoned").clone() else {
        return;
    };
    if let Some(io) = shared().local_process_io.lock().expect("state mutex poisoned").clone() {
        snapshot.process_io = Some(io);
    }
    publish(DashboardState::from_snapshot(&snapshot, tier()));
}

/// The full epoch list + live-authoritative calendar entries + current
/// time, as of the most recent snapshot -- everything `calendar::build_years`
/// needs that isn't already in `DashboardState`. See `Shared::calendar_source`.
pub fn epochs_for_calendar() -> (Vec<snapshot::EpochStatus>, Vec<snapshot::EpochCalendarEntry>, u64) {
    let source = shared().calendar_source.lock().expect("state mutex poisoned");
    (source.epochs.clone(), source.epoch_calendar.clone(), source.now_unix_secs)
}

/// Called by `client.rs`'s gap-index poller on a successful fetch.
pub fn set_gap_index(index: snapshot::BlockTimeGapIndex) {
    *shared().gap_index.lock().expect("state mutex poisoned") = Some(index);
    *shared().gap_index_error.lock().expect("state mutex poisoned") = None;
}

/// Called by the same poller when a fetch fails -- keeps whatever the last
/// successfully-fetched index was (a transient error shouldn't blank out
/// working data), but records why, so the calendar page can say why the
/// outage overlay might be stale or missing instead of just going quiet.
pub fn set_gap_index_error(reason: String) {
    *shared().gap_index_error.lock().expect("state mutex poisoned") = Some(reason);
}

pub fn gap_index() -> (Option<snapshot::BlockTimeGapIndex>, Option<String>) {
    (
        shared().gap_index.lock().expect("state mutex poisoned").clone(),
        shared().gap_index_error.lock().expect("state mutex poisoned").clone(),
    )
}

/// Called by `client.rs` when the upstream gateway is unreachable or sends
/// something we can't parse. Keeps the dashboard honestly in the
/// `service_unavailable` state instead of freezing on stale numbers.
pub fn set_offline(reason: String) {
    *shared().last_snapshot.lock().expect("state mutex poisoned") = None;
    publish(DashboardState {
        live: false,
        connection_state: "offline".into(),
        connection_message: reason,
        ..DashboardState::default()
    });
}

/// Deterministic in-process demo data, for UI iteration with no gateway
/// running. Only started when the binary is launched with `--demo`; never
/// the default, so this dashboard cannot accidentally present fabricated
/// numbers as real telemetry.
pub fn start_demo_simulation() {
    tokio::spawn(async move {
        let mut state = demo_seed();
        state.live = true;
        state.demo = true;
        state.connection_state = "live".into();
        state.connection_message = "Demo data (--demo, no gateway connected)".into();
        publish(state.clone());
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            demo_tick(&mut state);
            publish(state.clone());
        }
    });
}

fn demo_seed() -> DashboardState {
    DashboardState {
        runnable_eta_secs: Some(7 * 3600 + 42 * 60),
        queued: 6,
        needs_action: 5,
        archive_complete: 990,
        archive_total: 1005,
        live_capture_active: false,
        epochs: vec![
            EpochTask { epoch: 790, label: "scanning".into(), pct: 88, blocks: 376_536, eta_secs: 28 * 60 + 58, hidden_from_overview: false },
            EpochTask { epoch: 791, label: "scanning".into(), pct: 85, blocks: 356_870, eta_secs: 40 * 60 + 44, hidden_from_overview: false },
            EpochTask { epoch: 792, label: "finalizing".into(), pct: 86, blocks: 357_498, eta_secs: 39 * 60 + 41, hidden_from_overview: false },
            EpochTask { epoch: 793, label: "queued".into(), pct: 0, blocks: 0, eta_secs: 0, hidden_from_overview: false },
            // Demonstrates the Overview filter: stale, no active process --
            // hidden here, but still visible on the /epochs page.
            EpochTask { epoch: 705, label: "failed".into(), pct: 62, blocks: 210_004, eta_secs: 0, hidden_from_overview: true },
        ],
        tasks_active: 4,
        tasks_paused: 1,
        error_count: 2,
        errors: vec![
            ErrorEntry { at_unix_secs: 1_739_090_100, scope: "historical_scan:790".into(), message: "retrying after transient CAR read timeout".into() },
            ErrorEntry { at_unix_secs: 1_739_089_800, scope: "live_finalizer:cap-2026-08-04".into(), message: "repair gate waiting on predecessor blockhash tail".into() },
        ],
        compactions: vec![
            CompactionHistoryEntry { id: "complete-1010-historical".into(), epoch: 1010, workflow: "historical".into(), completed_unix_secs: Some(1_739_040_000), duration_secs: Some(4 * 3600 + 26 * 60 + 11) },
            CompactionHistoryEntry { id: "complete-1009-live".into(), epoch: 1009, workflow: "live".into(), completed_unix_secs: Some(1_738_980_000), duration_secs: Some(2 * 3600 + 12 * 60 + 39) },
        ],
        machine: MachineSnapshot {
            load_1m: 1.8,
            memory_available_bytes: 84_000_000_000,
            memory_total_bytes: 128_000_000_000,
            swap_used_bytes: 2_120_000_000,
            swap_total_bytes: 4_000_000_000,
            disk_available_bytes: 2_140_000_000_000,
            disk_total_bytes: 6_000_000_000_000,
            disk_used_bytes: 3_860_000_000_000,
            archive_device_read_mib_per_sec: 52.1,
            archive_device_write_mib_per_sec: 34.8,
            pressure_memory_full_avg10: 0.2,
            pressure_io_full_avg10: 0.9,
        },
        processes: vec![
            ProcessEntry { id: "lane:replayer".into(), name: "replayer".into(), pid: 1182, cpu_percent: Some(41.8), rss_bytes: Some(6_400_000_000), read_mib_per_sec: Some(74.0), write_mib_per_sec: Some(11.0) },
            ProcessEntry { id: "lane:packager".into(), name: "packager".into(), pid: 1184, cpu_percent: Some(17.1), rss_bytes: Some(2_800_000_000), read_mib_per_sec: Some(15.4), write_mib_per_sec: Some(7.8) },
        ],
        process_io_active_count: 2,
        reasoning: SchedulerReasoning {
            legacy_compact_tuning_last_decision: Some("scaled up: 3 lanes accepted, 78 MiB/s baseline".into()),
            legacy_compact_last_action: Some("increased worker budget to 4 cores".into()),
            legacy_compact_last_action_unix_secs: Some(1_739_089_500),
            paused_lanes: vec![PausedLane {
                id: "compact_reuse:701".into(),
                kind: "historical_compact_reuse".into(),
                reason: Some("IO PSI full avg10 92.1 reached pause threshold 85.0".into()),
            }],
            ..Default::default()
        },
        ..Default::default()
    }
}

fn demo_tick(state: &mut DashboardState) {
    use rand::Rng;
    let mut rng = rand::thread_rng();

    if let Some(eta) = state.runnable_eta_secs.as_mut() {
        *eta = eta.saturating_sub(rng.gen_range(1..=6));
    }
    state.queued = (state.queued as i32 + rng.gen_range(-1..=1)).max(0) as u32;
    state.machine.load_1m = (state.machine.load_1m + rng.gen_range(-0.25..=0.25)).max(0.0);
    state.live_capture_active = rng.gen_bool(0.1) ^ state.live_capture_active && rng.gen_bool(0.15);

    for task in &mut state.epochs {
        if task.pct < 100 {
            task.pct = (task.pct + rng.gen_range(0..=2)).min(100);
            task.blocks += rng.gen_range(0..=1200);
            task.eta_secs = task.eta_secs.saturating_sub(rng.gen_range(0..=4));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// `shared()` is one process-global `OnceLock`, and cargo runs test
    /// functions concurrently by default -- any two tests that both publish
    /// to it (broadcast included) can interleave. Every test that does so
    /// must hold this for its duration. `unwrap_or_else` recovers from a
    /// poisoned lock instead of cascading one panicking test's failure into
    /// every other global-state test.
    static GLOBAL_STATE_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn snapshot_with_process_and_leaky_error() -> PipelineSnapshot {
        PipelineSnapshot {
            errors: vec![snapshot::PipelineError {
                at_unix_secs: 1,
                scope: "historical_scan:790".into(),
                message: "open /home/operator/private/state failed".into(),
            }],
            process_io: Some(snapshot::ProcessIoSnapshot {
                active_count: 1,
                processes: vec![snapshot::ProcessIoEntry {
                    id: "proc:1182".into(),
                    pid: 1182,
                    name: "some-backup-agent".into(),
                    blockzilla_owned: Some(false),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn public_tier_drops_process_table_and_scrubs_paths() {
        let snapshot = snapshot_with_process_and_leaky_error();
        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Public);

        assert!(state.processes.is_empty(), "public tier must not expose process name/pid");
        assert!(state.process_io_hidden, "UI must say processes are hidden, not absent");
        assert!(
            !state.errors[0].message.contains("/home/operator"),
            "public tier must scrub absolute paths from error text: {}",
            state.errors[0].message
        );
    }

    #[test]
    fn full_tier_keeps_process_table_and_raw_text() {
        let snapshot = snapshot_with_process_and_leaky_error();
        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);

        assert_eq!(state.processes.len(), 1);
        assert_eq!(state.processes[0].pid, 1182);
        assert_eq!(state.processes[0].name, "some-backup-agent");
        assert!(!state.process_io_hidden);
        assert!(state.errors[0].message.contains("/home/operator/private/state"));
    }

    #[test]
    fn reasoning_surfaces_already_available_wire_fields() {
        let snapshot = PipelineSnapshot {
            summary: snapshot::PipelineSummary {
                admission_blocked_reason: Some("queue at capacity".into()),
                legacy_compact_tuning_last_decision: Some("scaled up: headroom available".into()),
                legacy_compact_last_action_unix_secs: Some(42),
                ..Default::default()
            },
            lanes: vec![
                snapshot::LaneStatus {
                    id: "compact_reuse:794".into(),
                    kind: "historical_compact_reuse".into(),
                    state: "paused".into(),
                    auto_paused: true,
                    auto_pause_reason: Some("IO PSI full avg10 92.1 reached pause threshold 85.0".into()),
                    ..Default::default()
                },
                snapshot::LaneStatus {
                    id: "compact_reuse:795".into(),
                    state: "running".into(),
                    auto_paused: false,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);

        assert_eq!(state.reasoning.admission_blocked_reason.as_deref(), Some("queue at capacity"));
        assert_eq!(
            state.reasoning.legacy_compact_tuning_last_decision.as_deref(),
            Some("scaled up: headroom available")
        );
        assert_eq!(state.reasoning.legacy_compact_last_action_unix_secs, Some(42));
        // Only the auto-paused lane appears -- a running lane needs no
        // explanation and would just be noise.
        assert_eq!(state.reasoning.paused_lanes.len(), 1);
        assert_eq!(state.reasoning.paused_lanes[0].id, "compact_reuse:794");
        assert!(state.reasoning.paused_lanes[0].reason.as_deref().unwrap().contains("PSI"));
        assert!(!state.reasoning.is_empty());
    }

    #[test]
    fn reasoning_is_empty_when_snapshot_has_no_reasons() {
        let state = DashboardState::from_snapshot(&PipelineSnapshot::default(), RedactionTier::Full);
        assert!(state.reasoning.is_empty());
    }

    #[test]
    fn poh_migration_progress_maps_from_summary_and_computes_pct() {
        let snapshot = PipelineSnapshot {
            summary: snapshot::PipelineSummary {
                poh_migration_bytes_done: 429_000,
                poh_migration_bytes_total: 1_000_000,
                poh_migration_epochs_done: 812,
                poh_migration_epochs_total: 1_009,
                poh_migration_eta_secs: Some(22_320.0),
                ..Default::default()
            },
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);

        assert_eq!(state.poh_migration_bytes_done, 429_000);
        assert_eq!(state.poh_migration_bytes_total, 1_000_000);
        assert!((state.poh_migration_pct() - 42.9).abs() < 0.01);
        assert_eq!(state.poh_migration_bytes_label(), "418.9 KiB / 976.6 KiB processed");
        // Epoch counts, not bytes, are the primary label -- bytes stay as
        // the small subtext above.
        assert_eq!(state.poh_migration_epoch_label(), "812 done \u{b7} 197 remaining");
        assert_eq!(state.poh_migration_eta_label(), "6h 12m");
    }

    #[test]
    fn poh_migration_eta_label_is_unknown_without_a_current_rate() {
        let state = DashboardState { poh_migration_eta_secs: None, ..Default::default() };
        assert_eq!(state.poh_migration_eta_label(), "unknown");
    }

    #[test]
    fn poh_migration_pct_stays_precise_at_terabyte_scale() {
        // Regression: computing this in f32 straight from the raw u64 byte
        // counts loses real precision above ~16M (f32 has ~7 significant
        // digits) -- at the multi-TB scale this job actually runs at, that
        // previously produced a visibly wrong percentage, not just a
        // cosmetic rounding artifact.
        let state = DashboardState {
            poh_migration_bytes_done: 2_539_466_420_707,
            poh_migration_bytes_total: 6_613_629_103_779,
            ..Default::default()
        };
        assert!(
            (state.poh_migration_pct() - 38.39).abs() < 0.01,
            "got {}",
            state.poh_migration_pct()
        );
    }

    #[test]
    fn to_signals_sends_capped_decimal_strings_not_raw_floats() {
        // Regression: a raw f32 percentage in a JSON number re-expands to
        // something like `38.400001525878906` once read back as a JS
        // number -- see the comment in `to_signals`. Sending a
        // pre-formatted string is what actually caps the decimal places
        // shown, not just rounding the underlying value.
        let state = DashboardState {
            live: true,
            archive_complete: 1,
            archive_total: 3,
            poh_migration_bytes_done: 429_000,
            poh_migration_bytes_total: 1_000_000,
            ..Default::default()
        };
        let signals = state.to_signals();
        assert_eq!(signals["archive_pct"], json!("33.3"));
        assert_eq!(signals["poh_migration_pct"], json!("42.9"));
        assert_eq!(signals["load_1m"], json!("0.0"));
    }

    #[test]
    fn poh_migration_lanes_lists_running_workers_by_epoch_and_ignores_other_lane_kinds() {
        let snapshot = PipelineSnapshot {
            lanes: vec![
                snapshot::LaneStatus {
                    id: "poh_migration:794".into(),
                    kind: "poh_signature_count_migration".into(),
                    state: "running".into(),
                    epoch: Some(794),
                    progress: snapshot::ProgressSnapshot {
                        blocks_done: 3_159,
                        blocks_total: 431_190,
                        progress_pct: Some(0.733),
                        eta_secs: Some(2_400.0),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                // A different lane kind on an unrelated epoch must not leak in.
                snapshot::LaneStatus {
                    id: "compact_reuse:701".into(),
                    kind: "historical_compact_reuse".into(),
                    state: "running".into(),
                    epoch: Some(701),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);

        assert_eq!(state.poh_migration_lanes.len(), 1);
        let lane = &state.poh_migration_lanes[0];
        assert_eq!(lane.epoch, 794);
        assert_eq!(lane.label, "running");
        assert_eq!(lane.blocks, 3_159);
        assert_eq!(lane.pct, 0);
        assert_eq!(lane.eta_secs, 2_400);

        // Live-updates via the same `epoch_{N}_*` signal names `epoch_row` binds to.
        let signals = state.to_signals();
        assert_eq!(signals["epoch_794_pct"], 0);
        assert_eq!(signals["epoch_794_blocks"], "3,159");
    }

    #[test]
    fn poh_migration_pct_is_zero_not_nan_when_total_is_zero() {
        let state = DashboardState::default();
        assert_eq!(state.poh_migration_pct(), 0.0);
    }

    #[test]
    fn stale_failed_or_blocked_epochs_are_flagged_hidden_only_without_an_active_lane() {
        let snapshot = PipelineSnapshot {
            epochs: vec![
                snapshot::EpochStatus { epoch: 1, state: "failed".into(), ..Default::default() },
                snapshot::EpochStatus { epoch: 2, state: "blocked".into(), ..Default::default() },
                // Same failed state, but a lane is still actively retrying it.
                snapshot::EpochStatus { epoch: 3, state: "failed".into(), ..Default::default() },
                snapshot::EpochStatus { epoch: 4, state: "scanning".into(), ..Default::default() },
            ],
            lanes: vec![snapshot::LaneStatus { epoch: Some(3), state: "running".into(), ..Default::default() }],
            ..Default::default()
        };
        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);
        let hidden: std::collections::BTreeMap<u32, bool> =
            state.epochs.iter().map(|task| (task.epoch, task.hidden_from_overview)).collect();
        assert!(hidden[&1], "failed with no lane must be hidden");
        assert!(hidden[&2], "blocked with no lane must be hidden");
        assert!(!hidden[&3], "failed but still has an active lane must stay visible");
        assert!(!hidden[&4], "scanning is never hidden regardless of lane state");
    }

    #[test]
    fn diff_signals_only_includes_changed_keys() {
        let old = json!({"a": 1, "b": 2, "c": 3});
        let new = json!({"a": 1, "b": 5, "c": 3});
        assert_eq!(diff_signals(&old, &new), json!({"b": 5}));
    }

    #[test]
    fn diff_signals_nulls_removed_keys_so_datastar_deletes_them() {
        let old = json!({"epoch_790_pct": 88, "queued": 3});
        let new = json!({"queued": 3});
        assert_eq!(diff_signals(&old, &new), json!({"epoch_790_pct": null}));
    }

    #[test]
    fn diff_signals_of_identical_maps_is_empty() {
        let state = json!({"a": 1, "b": "x"});
        assert_eq!(diff_signals(&state, &state), json!({}));
    }

    #[test]
    fn publish_skips_broadcast_when_nothing_changed() {
        let _guard = GLOBAL_STATE_TEST_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // Two states that map to the same signals (e.g. only a field this
        // dashboard doesn't surface changed upstream) must not wake up
        // every open tab with an empty patch.
        let mut rx = subscribe();
        let state = snapshot_blocking();
        publish(state.clone());
        publish(state);
        assert!(rx.try_recv().is_err(), "no delta should have been broadcast");
    }

    fn snapshot_blocking() -> DashboardState {
        shared().current.lock().expect("state mutex poisoned").clone()
    }

    #[test]
    fn local_process_io_merges_into_snapshots_and_is_cleared_on_offline() {
        let _guard = GLOBAL_STATE_TEST_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // Sequential by design (see `publish_skips_broadcast_when_nothing_changed`
        // above): every assertion here reads `shared()`, a process-global,
        // so interleaving with a concurrently-running test that also
        // mutates it would be flaky either way -- keeping the whole
        // set_snapshot/set_local_process_io/set_offline story in one test
        // avoids that instead of chasing it across several. Uses distinct
        // active_count values throughout (rather than asserting a "0" or
        // "unset" baseline) since `local_process_io` is process-global and
        // intentionally sticky -- a prior test in this binary may have left
        // it set, so only tracking a value change is reliable here.
        let io = |active_count: u32| snapshot::ProcessIoSnapshot {
            state: "ready".into(),
            active_count,
            processes: vec![snapshot::ProcessIoEntry {
                id: format!("9-{active_count}"),
                pid: 9,
                name: "backup-agent".into(),
                blockzilla_owned: Some(false),
                ..Default::default()
            }],
            ..Default::default()
        };

        // A process-I/O tick before any snapshot has (re-)arrived must not
        // conjure a "live" dashboard out of nothing -- even though the
        // value is retained for whenever a snapshot does arrive.
        set_offline("reset for test".into());
        set_local_process_io(io(11));
        assert!(!snapshot_blocking().live);

        // Once a snapshot arrives, the already-pending value merges in
        // immediately -- no extra process-I/O tick required.
        set_snapshot(PipelineSnapshot { sequence: 1, ..Default::default() });
        assert_eq!(snapshot_blocking().process_io_active_count, 11);

        // A later process-I/O tick updates it without waiting for the next
        // snapshot/patch.
        set_local_process_io(io(12));
        assert_eq!(snapshot_blocking().process_io_active_count, 12);

        // A subsequent snapshot keeps carrying the locally-collected value
        // forward -- it should not require a fresh process-I/O tick.
        set_snapshot(PipelineSnapshot { sequence: 2, ..Default::default() });
        assert_eq!(snapshot_blocking().process_io_active_count, 12);

        // Going offline must not leave a stale snapshot around for a late
        // process-I/O tick to republish as "live" again.
        set_offline("test offline".into());
        assert!(!snapshot_blocking().live);
        set_local_process_io(io(13));
        assert!(!snapshot_blocking().live, "a process-I/O tick alone must not revive the dashboard while offline");
    }
}
