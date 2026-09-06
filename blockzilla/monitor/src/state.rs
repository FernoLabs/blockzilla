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
//! touching the DOM from the server. Changes that affect page structure
//! also emit `StreamEvent::Structure`; each subscriber renders that marker
//! for its own route and morphs the stable dashboard frame.

use std::{
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, broadcast};

use crate::snapshot::{self, PipelineSnapshot};

/// One item on the `/api/stream` broadcast: either a signal-value delta
/// (the common case -- pct/blocks/eta/label/phase ticking on rows that
/// already exist) or a marker that page structure changed. The stream
/// handler turns `Structure` into a fresh, route-specific
/// `#dashboard-frame` morph. Keeping rendering out of this global
/// broadcast matters because subscribers can be on five different pages.
#[derive(Clone, Debug)]
pub enum StreamEvent {
    Signals(serde_json::Value),
    Structure,
}

/// Overview is deliberately concise; `/epochs` retains and renders the
/// complete non-finished queue.
pub const OVERVIEW_EPOCH_LIMIT: usize = 12;
/// Firewatch can cover hundreds of completed archive epochs. Overview keeps
/// the full counters but renders only an operator-focused sample.
pub const OVERVIEW_FIREWATCH_LIMIT: usize = 16;
const OVERVIEW_FIREWATCH_QUEUED_SAMPLE: usize = 8;
const OVERVIEW_FIREWATCH_ACCEPTED_SAMPLE: usize = 4;

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
    /// monitor's audited path/credential scrubber.
    /// This protects the public surface even though the monitor reads the
    /// scheduler's private loopback status endpoint directly.
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

/// Text scrub for the public tier -- a no-op pass-through on the full tier.
/// Reuses the monitor's audited regexes. Takes
/// `tier` explicitly (rather than reading the global) so `from_snapshot`
/// stays a pure function of its inputs and is testable for both tiers
/// without mutating shared process-global state.
fn redact_text(tier: RedactionTier, text: &str) -> String {
    match tier {
        RedactionTier::Public => crate::public_json::sanitize_public_string(text),
        RedactionTier::Full => text.to_string(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EpochTask {
    pub epoch: u32,
    pub label: String,
    /// Current phase of the active lane working this epoch, e.g. `"Archive
    /// V2 Hot Write"` -- empty when nothing is actively running (queued,
    /// failed, blocked). Shown alongside `label` so "Scanning" also says
    /// which of scanning's several phases is running right now.
    pub phase: String,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FirewatchIndexEntry {
    pub epoch: u32,
    pub state: String,
    pub phase: String,
    pub pct: u8,
    pub blocks: u64,
    pub eta_secs: Option<u64>,
    pub wallet_count: Option<u64>,
    pub relation_count: Option<u64>,
    pub parity_status: Option<String>,
    pub rss_bytes: Option<u64>,
    pub read_mib_per_sec: Option<f64>,
    pub write_mib_per_sec: Option<f64>,
}

impl FirewatchIndexEntry {
    pub fn counts_label(&self) -> String {
        match (self.wallet_count, self.relation_count) {
            (Some(wallets), Some(relations)) => format!(
                "{} wallets \u{b7} {} relations",
                format_thousands(wallets),
                format_thousands(relations)
            ),
            (Some(wallets), None) => format!("{} wallets", format_thousands(wallets)),
            (None, Some(relations)) => {
                format!("{} relations", format_thousands(relations))
            }
            (None, None) => "not reported".to_string(),
        }
    }

    pub fn parity_label(&self) -> String {
        self.parity_status
            .as_deref()
            .map(snapshot::humanize)
            .unwrap_or_else(|| "not checked".to_string())
    }

    pub fn progress_label(&self) -> String {
        let eta = self
            .eta_secs
            .map(|secs| format!(" \u{b7} ETA {}", format_duration(secs)))
            .unwrap_or_default();
        let progress_is_unavailable = self.pct == 0
            && self.blocks == 0
            && (self.rss_bytes.is_some()
                || self.read_mib_per_sec.is_some()
                || self.write_mib_per_sec.is_some());

        if progress_is_unavailable {
            match snapshot::normalize(&self.state).as_str() {
                "paused" => return format!("Paused{eta}"),
                "running" => {
                    let activity = match (self.read_mib_per_sec, self.write_mib_per_sec) {
                        (Some(read), Some(write)) => {
                            format!("{read:.1}/{write:.1} MiB/s R/W")
                        }
                        (Some(read), None) => format!("{read:.1} MiB/s read"),
                        (None, Some(write)) => format!("{write:.1} MiB/s write"),
                        (None, None) => self
                            .rss_bytes
                            .map(|rss| format!("{} RSS", format_bytes(rss)))
                            .unwrap_or_default(),
                    };
                    return format!("Working \u{b7} {activity}{eta}");
                }
                _ => {}
            }
        }

        format!(
            "{}% \u{b7} {} blocks{eta}",
            self.pct,
            format_thousands(self.blocks)
        )
    }

    pub fn resources_label(&self) -> String {
        let mut parts = Vec::new();
        if let Some(rss) = self.rss_bytes {
            parts.push(format!("{} RSS", format_bytes(rss)));
        }
        match (self.read_mib_per_sec, self.write_mib_per_sec) {
            (Some(read), Some(write)) => {
                parts.push(format!("{read:.1}/{write:.1} MiB/s R/W"));
            }
            (Some(read), None) => parts.push(format!("{read:.1} MiB/s read")),
            (None, Some(write)) => parts.push(format!("{write:.1} MiB/s write")),
            (None, None) => {}
        }
        if parts.is_empty() {
            "not reported".to_string()
        } else {
            parts.join(" \u{b7} ")
        }
    }
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
    pub registry_reprocess_admission_blocked_reason: Option<String>,
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
            && self.registry_reprocess_admission_blocked_reason.is_none()
            && self.legacy_compact_last_action.is_none()
            && self.legacy_compact_tuning_last_decision.is_none()
            && self.paused_lanes.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DashboardState {
    /// True while the current state is backed by a real (or demo)
    /// snapshot. Set back to false when the upstream goes offline.
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
    pub registry_reprocess_epochs_done: u32,
    pub registry_reprocess_epochs_total: u32,
    pub registry_reprocess_capacity_configured: u32,
    pub registry_reprocess_running: u32,
    pub firewatch_enabled: bool,
    pub firewatch_capacity_configured: u32,
    pub firewatch_running: u32,
    pub firewatch_epochs_total: u32,
    pub firewatch_epochs_accepted: u32,
    pub firewatch_epochs_queued: u32,
    /// Optional coverage fields distinguish an older migration-only
    /// controller from the all-archive controller without changing schema 1.
    pub firewatch_archive_epochs_total: Option<u32>,
    pub firewatch_epochs_eligible: Option<u32>,
    pub firewatch_epochs_blocked_migration: Option<u32>,
    pub firewatch_epochs_blocked_wire_profile: Option<u32>,
    pub firewatch_queue_eta_secs: Option<u64>,
    pub firewatch_next_epoch: Option<u32>,
    pub firewatch_admission_blocked_reason: Option<String>,
    pub firewatch_indexes: Vec<FirewatchIndexEntry>,
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
    /// Active first-seen to usage-sorted registry workers. Their epochs are
    /// archive-complete, so they need a separate list for the same reason as
    /// the PoH migration workers above.
    pub registry_reprocess_lanes: Vec<EpochTask>,
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
            connection_message: "Connecting to Blockzilla scheduler".into(),
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
            registry_reprocess_epochs_done: 0,
            registry_reprocess_epochs_total: 0,
            registry_reprocess_capacity_configured: 0,
            registry_reprocess_running: 0,
            firewatch_enabled: false,
            firewatch_capacity_configured: 0,
            firewatch_running: 0,
            firewatch_epochs_total: 0,
            firewatch_epochs_accepted: 0,
            firewatch_epochs_queued: 0,
            firewatch_archive_epochs_total: None,
            firewatch_epochs_eligible: None,
            firewatch_epochs_blocked_migration: None,
            firewatch_epochs_blocked_wire_profile: None,
            firewatch_queue_eta_secs: None,
            firewatch_next_epoch: None,
            firewatch_admission_blocked_reason: None,
            firewatch_indexes: Vec::new(),
            live_capture_active: false,
            epochs: Vec::new(),
            poh_migration_lanes: Vec::new(),
            registry_reprocess_lanes: Vec::new(),
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
        (100.0 * self.poh_migration_bytes_done as f64
            / self.poh_migration_bytes_total.max(1) as f64) as f32
    }

    /// "812 done \u{b7} 197 remaining" -- the primary, at-a-glance count. Bytes still drive the
    /// percentage (epochs aren't equal-sized work) but are relegated to
    /// `poh_migration_bytes_label`'s small subtext, since "how many epochs are left" is what
    /// operators actually want to read first.
    pub fn poh_migration_epoch_label(&self) -> String {
        let remaining = self
            .poh_migration_epochs_total
            .saturating_sub(self.poh_migration_epochs_done);
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

    pub fn registry_reprocess_pct(&self) -> f32 {
        100.0 * self.registry_reprocess_epochs_done as f32
            / self.registry_reprocess_epochs_total.max(1) as f32
    }

    pub fn registry_reprocess_epoch_label(&self) -> String {
        let remaining = self
            .registry_reprocess_epochs_total
            .saturating_sub(self.registry_reprocess_epochs_done);
        format!(
            "{} done \u{b7} {} remaining",
            format_thousands(self.registry_reprocess_epochs_done as u64),
            format_thousands(remaining as u64)
        )
    }

    pub fn registry_reprocess_worker_label(&self) -> String {
        format!(
            "{} active \u{b7} capacity {}",
            format_thousands(self.registry_reprocess_running as u64),
            format_thousands(self.registry_reprocess_capacity_configured as u64)
        )
    }

    pub fn firewatch_summary_label(&self) -> String {
        let failed = self
            .firewatch_indexes
            .iter()
            .filter(|entry| snapshot::normalize(&entry.state) == "failed")
            .count();
        let profile_audit = self
            .firewatch_epochs_blocked_wire_profile
            .unwrap_or_else(|| {
                self.firewatch_indexes
                    .iter()
                    .filter(|entry| snapshot::normalize(&entry.state) == "profile_audit_required")
                    .count() as u32
            });
        format!(
            "{} accepted \u{b7} {} active \u{b7} {} queued \u{b7} {} failed \u{b7} {} awaiting profile audit",
            format_thousands(self.firewatch_epochs_accepted as u64),
            format_thousands(self.firewatch_running as u64),
            format_thousands(self.firewatch_epochs_queued as u64),
            format_thousands(failed as u64),
            format_thousands(profile_audit as u64)
        )
    }

    pub fn firewatch_coverage_label(&self) -> String {
        let archive_total = self
            .firewatch_archive_epochs_total
            .unwrap_or(self.archive_complete);
        let blocked_migration = self.firewatch_epochs_blocked_migration.unwrap_or_else(|| {
            self.registry_reprocess_epochs_total
                .saturating_sub(self.registry_reprocess_epochs_done)
        });
        match (
            self.firewatch_epochs_eligible,
            self.firewatch_epochs_blocked_wire_profile,
        ) {
            (Some(eligible), Some(blocked_wire_profile)) => format!(
                "{} archive-complete \u{b7} {} indexable \u{b7} {} blocked by registry migration \u{b7} {} awaiting profile audit",
                format_thousands(archive_total as u64),
                format_thousands(eligible as u64),
                format_thousands(blocked_migration as u64),
                format_thousands(blocked_wire_profile as u64)
            ),
            (Some(eligible), None) => format!(
                "{} archive-complete \u{b7} {} indexable \u{b7} {} blocked by registry migration",
                format_thousands(archive_total as u64),
                format_thousands(eligible as u64),
                format_thousands(blocked_migration as u64)
            ),
            (None, _) => format!(
                "{} archive-complete \u{b7} {} tracked by this controller \u{b7} {} blocked by registry migration",
                format_thousands(archive_total as u64),
                format_thousands(self.firewatch_epochs_total as u64),
                format_thousands(blocked_migration as u64)
            ),
        }
    }

    pub fn firewatch_next_label(&self) -> String {
        self.firewatch_next_epoch
            .map(|epoch| format!("Epoch {epoch}"))
            .unwrap_or_else(|| "None queued".to_string())
    }

    pub fn firewatch_capacity_label(&self) -> String {
        format!(
            "{} of {} workers active",
            format_thousands(self.firewatch_running as u64),
            format_thousands(self.firewatch_capacity_configured as u64)
        )
    }

    pub fn firewatch_queue_eta_label(&self) -> String {
        self.firewatch_queue_eta_secs
            .map(format_duration)
            .unwrap_or_else(|| "unknown".to_string())
    }

    pub fn overview_firewatch_indexes(&self) -> Vec<&FirewatchIndexEntry> {
        let mut candidates = self.firewatch_indexes.iter().collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            firewatch_overview_priority(left)
                .cmp(&firewatch_overview_priority(right))
                .then_with(|| right.epoch.cmp(&left.epoch))
        });

        let mut visible = candidates
            .iter()
            .copied()
            .filter(|entry| firewatch_overview_priority(entry) <= 1)
            .take(OVERVIEW_FIREWATCH_LIMIT)
            .collect::<Vec<_>>();

        append_firewatch_sample(
            &mut visible,
            &candidates,
            OVERVIEW_FIREWATCH_QUEUED_SAMPLE,
            "queued",
        );
        append_firewatch_sample(
            &mut visible,
            &candidates,
            OVERVIEW_FIREWATCH_ACCEPTED_SAMPLE,
            "accepted",
        );
        for candidate in &candidates {
            if visible.len() == OVERVIEW_FIREWATCH_LIMIT {
                break;
            }
            if !visible.iter().any(|entry| entry.epoch == candidate.epoch) {
                visible.push(candidate);
            }
        }
        visible.sort_by(|left, right| {
            firewatch_overview_priority(left)
                .cmp(&firewatch_overview_priority(right))
                .then_with(|| right.epoch.cmp(&left.epoch))
        });
        visible
    }

    pub fn firewatch_rows_label(&self) -> String {
        let visible = self.overview_firewatch_indexes().len();
        let reported = self.firewatch_indexes.len();
        if reported == 0 {
            "No detailed epoch rows reported".to_string()
        } else if visible == reported {
            format!("All {} reported epochs", format_thousands(reported as u64))
        } else {
            format!(
                "Priority sample: {} of {} reported epochs. Project counts above include all reported epochs.",
                format_thousands(visible as u64),
                format_thousands(reported as u64)
            )
        }
    }

    pub fn eta_label(&self) -> String {
        match self.runnable_eta_secs {
            Some(secs) => format_duration(secs),
            None => "unknown".to_string(),
        }
    }

    pub fn last_updated_label(&self) -> String {
        self.last_updated_label_at(unix_now())
    }

    fn last_updated_label_at(&self, now_unix_secs: u64) -> String {
        if self.updated_unix_secs == 0 {
            return "No scheduler update received".to_string();
        }
        let age = now_unix_secs.saturating_sub(self.updated_unix_secs);
        if age < 5 {
            "Updated just now".to_string()
        } else {
            format!("Updated {} ago", format_duration(age))
        }
    }

    /// Epochs shown on Overview, after dropping stale failed/blocked rows
    /// and prioritizing work an operator can act on. The cap belongs here,
    /// at the presentation boundary: `self.epochs` remains complete for
    /// `/epochs` and for structural-membership detection.
    pub fn overview_epochs(&self) -> Vec<&EpochTask> {
        let mut visible: Vec<_> = self
            .epochs
            .iter()
            .filter(|task| !task.hidden_from_overview)
            .collect();
        visible.sort_by_key(|task| (overview_priority(task), task.epoch));
        visible.truncate(OVERVIEW_EPOCH_LIMIT);
        visible
    }

    /// Flattens state into the exact signal names the views bind to
    /// (`$queued`, `$archive_pct`, `$epoch_790_pct`, ...). This -- not the
    /// nested struct itself -- is what `PatchSignals::json` should send,
    /// since Datastar's signal store is a flat-ish object keyed by these
    /// names, not a Rust type it deserializes into.
    pub fn to_signals(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("live".into(), self.live.into());
        map.insert(
            "connection_state".into(),
            self.connection_state.clone().into(),
        );
        map.insert(
            "last_updated_label".into(),
            self.last_updated_label().into(),
        );
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
        map.insert(
            "poh_migration_epoch_label".into(),
            self.poh_migration_epoch_label().into(),
        );
        map.insert(
            "poh_migration_bytes_label".into(),
            self.poh_migration_bytes_label().into(),
        );
        map.insert(
            "poh_migration_eta_label".into(),
            self.poh_migration_eta_label().into(),
        );
        map.insert(
            "poh_migration_pct".into(),
            format_pct(self.poh_migration_pct()).into(),
        );
        map.insert(
            "registry_reprocess_epoch_label".into(),
            self.registry_reprocess_epoch_label().into(),
        );
        map.insert(
            "registry_reprocess_worker_label".into(),
            self.registry_reprocess_worker_label().into(),
        );
        map.insert(
            "registry_reprocess_pct".into(),
            format_pct(self.registry_reprocess_pct()).into(),
        );
        map.insert(
            "firewatch_summary_label".into(),
            self.firewatch_summary_label().into(),
        );
        map.insert(
            "firewatch_coverage_label".into(),
            self.firewatch_coverage_label().into(),
        );
        map.insert(
            "firewatch_next_label".into(),
            self.firewatch_next_label().into(),
        );
        map.insert(
            "firewatch_queue_eta_label".into(),
            self.firewatch_queue_eta_label().into(),
        );
        map.insert(
            "firewatch_capacity_label".into(),
            self.firewatch_capacity_label().into(),
        );
        map.insert(
            "firewatch_rows_label".into(),
            self.firewatch_rows_label().into(),
        );
        map.insert(
            "firewatch_admission_blocked_reason".into(),
            self.firewatch_admission_blocked_reason.clone().into(),
        );
        map.insert(
            "live_capture_active".into(),
            self.live_capture_active.into(),
        );
        map.insert("load_1m".into(), format_pct(self.machine.load_1m).into());
        map.insert("error_count".into(), self.error_count.into());
        map.insert("tasks_active".into(), self.tasks_active.into());
        map.insert("tasks_paused".into(), self.tasks_paused.into());
        map.insert("scheduler_paused".into(), self.scheduler_paused.into());
        map.insert("history_count".into(), self.compactions.len().into());
        map.insert(
            "process_io_active_count".into(),
            self.process_io_active_count.into(),
        );

        // `poh_migration_lanes` never overlaps `epochs` by epoch number (an
        // epoch is either still building, in `epochs`, or already
        // archive-complete and migration-eligible, in `poh_migration_lanes`
        // -- never both), so both can share the same `epoch_{N}_*` signal
        // names without collision.
        for task in self
            .overview_epochs()
            .into_iter()
            .chain(self.poh_migration_lanes.iter())
        {
            let sig = format!("epoch_{}", task.epoch);
            map.insert(format!("{sig}_pct"), task.pct.into());
            map.insert(
                format!("{sig}_blocks"),
                format_thousands(task.blocks).into(),
            );
            map.insert(format!("{sig}_eta"), task.eta_label().into());
            map.insert(format!("{sig}_label"), task.label.clone().into());
            map.insert(format!("{sig}_phase"), task.phase.clone().into());
        }

        // Registry and PoH jobs can both target an already-complete epoch.
        // Use a registry-specific signal prefix so two simultaneous workers
        // cannot overwrite each other's row values.
        for task in &self.registry_reprocess_lanes {
            let sig = format!("registry_epoch_{}", task.epoch);
            map.insert(format!("{sig}_pct"), task.pct.into());
            map.insert(
                format!("{sig}_blocks"),
                format_thousands(task.blocks).into(),
            );
            map.insert(format!("{sig}_eta"), registry_task_eta_label(task).into());
            map.insert(format!("{sig}_label"), task.label.clone().into());
            map.insert(format!("{sig}_phase"), task.phase.clone().into());
        }

        for index in self.overview_firewatch_indexes() {
            let sig = format!("firewatch_epoch_{}", index.epoch);
            map.insert(format!("{sig}_state"), index.state.clone().into());
            map.insert(format!("{sig}_phase"), index.phase.clone().into());
            map.insert(format!("{sig}_pct"), index.pct.into());
            map.insert(format!("{sig}_progress"), index.progress_label().into());
            map.insert(format!("{sig}_counts"), index.counts_label().into());
            map.insert(format!("{sig}_parity"), index.parity_label().into());
            map.insert(format!("{sig}_resources"), index.resources_label().into());
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
                let active_lane = snapshot
                    .lanes
                    .iter()
                    .find(|lane| lane.epoch == Some(epoch.epoch));
                EpochTask {
                    epoch: epoch.epoch,
                    label: snapshot::humanize(&epoch.state),
                    phase: active_lane
                        .map(|lane| lane.phase.clone())
                        .unwrap_or_default(),
                    pct: epoch.progress.progress_pct.unwrap_or(0.0).clamp(0.0, 100.0) as u8,
                    blocks: epoch.progress.blocks_done,
                    eta_secs: epoch.progress.eta_secs.unwrap_or(0.0).max(0.0).round() as u64,
                    hidden_from_overview: is_failed_or_blocked && active_lane.is_none(),
                }
            })
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
                    phase: lane.phase.clone(),
                    pct: lane.progress.progress_pct.unwrap_or(0.0).clamp(0.0, 100.0) as u8,
                    blocks: lane.progress.blocks_done,
                    eta_secs: lane.progress.eta_secs.unwrap_or(0.0).max(0.0).round() as u64,
                    hidden_from_overview: false,
                })
            })
            .collect();

        let registry_reprocess_lanes = snapshot
            .lanes
            .iter()
            .filter(|lane| lane.kind == "archive_v2_registry_reprocess")
            .filter_map(|lane| {
                let epoch = lane.epoch?;
                Some(EpochTask {
                    epoch,
                    label: snapshot::humanize(&lane.state),
                    phase: lane.phase.clone(),
                    pct: lane_progress_pct(&lane.progress),
                    blocks: lane.progress.blocks_done,
                    eta_secs: lane.progress.eta_secs.unwrap_or(0.0).max(0.0).round() as u64,
                    hidden_from_overview: false,
                })
            })
            .collect();

        let firewatch_lanes: Vec<_> = snapshot
            .lanes
            .iter()
            .filter(|lane| lane.kind == "firewatch_index")
            .filter(|lane| lane.epoch.is_some())
            .collect();
        let firewatch_next_epoch = firewatch_lanes
            .iter()
            .find(|lane| snapshot::normalize(&lane.state) == "queued")
            .and_then(|lane| lane.epoch);
        let firewatch_indexes = firewatch_lanes
            .iter()
            .map(|lane| FirewatchIndexEntry {
                epoch: lane.epoch.expect("filtered to Firewatch lanes with epochs"),
                state: snapshot::humanize(&lane.state),
                phase: snapshot::humanize(&lane.phase),
                pct: lane_progress_pct(&lane.progress),
                blocks: lane.progress.blocks_done,
                eta_secs: lane
                    .progress
                    .eta_secs
                    .filter(|secs| secs.is_finite() && *secs >= 0.0)
                    .map(|secs| secs.round() as u64),
                wallet_count: lane.wallet_count,
                relation_count: lane.relation_count,
                parity_status: lane.parity_status.clone(),
                rss_bytes: lane
                    .progress
                    .rss_bytes
                    .or(lane.rss_bytes)
                    .or(lane.progress.peak_rss_bytes),
                read_mib_per_sec: lane
                    .progress
                    .source_read_mib_per_sec
                    .or(lane.progress.disk_read_mib_per_sec)
                    .or(lane.progress.input_mib_per_sec),
                write_mib_per_sec: lane.progress.disk_write_mib_per_sec,
            })
            .collect();
        let firewatch_enabled = summary.firewatch_index_epochs_total > 0
            || summary.firewatch_index_capacity_configured > 0
            || summary.firewatch_index_archive_epochs_total.is_some()
            || summary.firewatch_index_epochs_eligible.is_some()
            || summary
                .firewatch_index_epochs_blocked_migration
                .is_some_and(|blocked| blocked > 0)
            || summary
                .firewatch_index_epochs_blocked_wire_profile
                .is_some_and(|blocked| blocked > 0)
            || !firewatch_lanes.is_empty()
            || summary.firewatch_index_admission_blocked_reason.is_some();

        let paused_lanes = snapshot
            .lanes
            .iter()
            .filter(|lane| lane.auto_paused)
            .map(|lane| PausedLane {
                id: redact_text(tier, &lane.id),
                kind: redact_text(tier, &lane.kind),
                reason: lane
                    .auto_pause_reason
                    .as_deref()
                    .map(|reason| redact_text(tier, reason)),
            })
            .collect();

        let reasoning = SchedulerReasoning {
            admission_blocked_reason: summary
                .admission_blocked_reason
                .as_deref()
                .map(|r| redact_text(tier, r)),
            legacy_compact_admission_blocked_reason: summary
                .legacy_compact_admission_blocked_reason
                .as_deref()
                .map(|r| redact_text(tier, r)),
            finalizer_admission_blocked_reason: summary
                .finalizer_admission_blocked_reason
                .as_deref()
                .map(|r| redact_text(tier, r)),
            registry_reprocess_admission_blocked_reason: summary
                .registry_reprocess_admission_blocked_reason
                .as_deref()
                .map(|r| redact_text(tier, r)),
            legacy_compact_last_action: summary
                .legacy_compact_last_action
                .as_deref()
                .map(|r| redact_text(tier, r)),
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
        let raw_processes = snapshot
            .process_io
            .as_ref()
            .map(|io| io.processes.as_slice())
            .unwrap_or_default();
        let has_visible_processes = raw_processes
            .iter()
            .any(|process| process.blockzilla_owned != Some(true));
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
            runnable_eta_secs: summary
                .runnable_eta_secs()
                .map(|secs| secs.max(0.0).round() as u64),
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
            registry_reprocess_epochs_done: summary.registry_reprocess_epochs_done,
            registry_reprocess_epochs_total: summary.registry_reprocess_epochs_total,
            registry_reprocess_capacity_configured: summary.registry_reprocess_capacity_configured,
            registry_reprocess_running: summary.registry_reprocess_running,
            firewatch_enabled,
            firewatch_capacity_configured: summary.firewatch_index_capacity_configured,
            firewatch_running: summary.firewatch_index_running,
            firewatch_epochs_total: summary.firewatch_index_epochs_total,
            firewatch_epochs_accepted: summary.firewatch_index_epochs_accepted,
            firewatch_epochs_queued: summary.firewatch_index_epochs_queued,
            firewatch_archive_epochs_total: summary.firewatch_index_archive_epochs_total,
            firewatch_epochs_eligible: summary.firewatch_index_epochs_eligible,
            firewatch_epochs_blocked_migration: summary.firewatch_index_epochs_blocked_migration,
            firewatch_epochs_blocked_wire_profile: summary
                .firewatch_index_epochs_blocked_wire_profile,
            firewatch_queue_eta_secs: summary
                .firewatch_index_queue_eta_secs
                .filter(|secs| secs.is_finite() && *secs >= 0.0)
                .map(|secs| secs.round() as u64),
            firewatch_next_epoch,
            firewatch_admission_blocked_reason: summary
                .firewatch_index_admission_blocked_reason
                .as_deref()
                .map(|reason| redact_text(tier, reason)),
            firewatch_indexes,
            live_capture_active: snapshot
                .live
                .iter()
                .any(|capture| capture.state == "capturing"),
            epochs,
            poh_migration_lanes,
            registry_reprocess_lanes,
            tasks_active: snapshot
                .lanes
                .iter()
                .filter(|l| l.is_active() && !l.is_paused())
                .count() as u32,
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
                archive_device_read_mib_per_sec: machine
                    .archive_device_read_mib_per_sec
                    .unwrap_or(0.0),
                archive_device_write_mib_per_sec: machine
                    .archive_device_write_mib_per_sec
                    .unwrap_or(0.0),
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

fn overview_priority(task: &EpochTask) -> u8 {
    if !task.phase.is_empty() {
        return 0;
    }
    match snapshot::normalize(&task.label).as_str() {
        "scanning" | "finalizing" | "running" | "capturing" | "compacting" => 0,
        "ready" => 1,
        "queued" => 2,
        _ => 3,
    }
}

fn firewatch_overview_priority(entry: &FirewatchIndexEntry) -> u8 {
    match snapshot::normalize(&entry.state).as_str() {
        "running" | "paused" => 0,
        "failed" | "blocked" | "profile_audit_required" => 1,
        "queued" => 2,
        "accepted" => 3,
        _ => 4,
    }
}

fn append_firewatch_sample<'a>(
    visible: &mut Vec<&'a FirewatchIndexEntry>,
    candidates: &[&'a FirewatchIndexEntry],
    group_limit: usize,
    state: &str,
) {
    let mut added = 0;
    for candidate in candidates.iter().copied() {
        if visible.len() == OVERVIEW_FIREWATCH_LIMIT || added == group_limit {
            break;
        }
        if snapshot::normalize(&candidate.state) == state
            && !visible.iter().any(|entry| entry.epoch == candidate.epoch)
        {
            visible.push(candidate);
            added += 1;
        }
    }
}

fn lane_progress_pct(progress: &snapshot::ProgressSnapshot) -> u8 {
    let pct = progress.progress_pct.unwrap_or_else(|| {
        if progress.blocks_total == 0 {
            0.0
        } else {
            100.0 * progress.blocks_done as f32 / progress.blocks_total as f32
        }
    });
    pct.clamp(0.0, 100.0) as u8
}

pub(crate) fn registry_task_eta_label(task: &EpochTask) -> String {
    if task.eta_secs == 0 {
        "unknown".to_string()
    } else {
        task.eta_label()
    }
}

/// One decimal place, pre-formatted as a string -- see the comment at its
/// call sites in `to_signals` for why a raw rounded float isn't enough.
fn format_pct(v: f32) -> String {
    format!("{v:.1}")
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
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
    let d = secs / 86_400;
    let h = (secs % 86_400) / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
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
    /// Serializes every mutation that can replace the published dashboard
    /// state. Without this gate, a local-process recomputation could clone
    /// an old snapshot, race `set_offline`, and publish that stale clone as
    /// live after the disconnect was already visible.
    publication: AsyncMutex<()>,
    current: Mutex<DashboardState>,
    /// The signal map from the most recent broadcast (or the initial
    /// `DashboardState::default()` before anything has published), kept
    /// only to diff the next update against. Every subscriber gets the
    /// same delta stream -- see `publish` for why that's safe even though
    /// subscribers can join at different times.
    last_signals: Mutex<serde_json::Value>,
    /// All non-complete epoch numbers as of the last publish. A change
    /// affects `/epochs`, even when Overview's selected twelve stay the
    /// same.
    last_epoch_ids: Mutex<Vec<u32>>,
    /// The prioritized/capped epoch membership actually rendered on
    /// Overview. A priority change can swap a row without changing the
    /// complete epoch set.
    last_overview_epoch_ids: Mutex<Vec<u32>>,
    /// Same idea for the currently rendered PoH migration lanes.
    last_poh_lane_ids: Mutex<Vec<u32>>,
    /// Same idea for the currently rendered registry reprocess lanes.
    last_registry_reprocess_lane_ids: Mutex<Vec<u32>>,
    /// Firewatch rows include terminal parity results as well as active and
    /// queued work. A state change must remorph the row so its status color
    /// remains correct, while scalar counts continue to use signal patches.
    last_firewatch_rows: Mutex<Vec<(u32, String)>>,
    last_firewatch_enabled: Mutex<bool>,
    /// The stable frame must morph when a previously live dashboard goes
    /// offline or an offline-first page receives its first snapshot.
    last_live: Mutex<bool>,
    tx: broadcast::Sender<StreamEvent>,
    /// The raw ingredients the `/calendar` page needs that `DashboardState`
    /// doesn't otherwise keep: epoch status fields needed by the calendar
    /// and any live-authoritative `epoch_calendar` entries. Kept
    /// separately and
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
            publication: AsyncMutex::new(()),
            last_signals: Mutex::new(initial.to_signals()),
            last_epoch_ids: Mutex::new(Vec::new()),
            last_overview_epoch_ids: Mutex::new(Vec::new()),
            last_poh_lane_ids: Mutex::new(Vec::new()),
            last_registry_reprocess_lane_ids: Mutex::new(Vec::new()),
            last_firewatch_rows: Mutex::new(Vec::new()),
            last_firewatch_enabled: Mutex::new(initial.firewatch_enabled),
            last_live: Mutex::new(initial.live),
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
    shared()
        .current
        .lock()
        .expect("state mutex poisoned")
        .clone()
}

/// Subscribe to future updates. Every `/api/stream` connection gets its own
/// receiver; `broadcast` means one upstream feeds every open browser tab.
/// Each item is a signal-map *delta*: only keys that changed since the
/// previous publish, with removed keys carrying `null` so Datastar deletes
/// them from the client's signal store (see the `datastar` docs: a signal
/// set to `null` in a patch is removed, not zeroed).
pub fn subscribe() -> broadcast::Receiver<StreamEvent> {
    shared().tx.subscribe()
}

fn remember_if_changed<T: PartialEq>(last: &Mutex<T>, next: T) -> bool {
    let mut last = last.lock().expect("state mutex poisoned");
    let changed = *last != next;
    *last = next;
    changed
}

async fn publish(state: DashboardState) {
    let shared = shared();
    let new_signals = state.to_signals();
    let epoch_ids: Vec<u32> = state.epochs.iter().map(|task| task.epoch).collect();
    let overview_epoch_ids: Vec<u32> = state
        .overview_epochs()
        .into_iter()
        .map(|task| task.epoch)
        .collect();
    let poh_lane_ids: Vec<u32> = state
        .poh_migration_lanes
        .iter()
        .map(|task| task.epoch)
        .collect();
    let registry_reprocess_lane_ids: Vec<u32> = state
        .registry_reprocess_lanes
        .iter()
        .map(|task| task.epoch)
        .collect();
    let firewatch_rows: Vec<(u32, String)> = state
        .overview_firewatch_indexes()
        .into_iter()
        .map(|index| (index.epoch, index.state.clone()))
        .collect();
    let firewatch_enabled = state.firewatch_enabled;
    let live = state.live;

    *shared.current.lock().expect("state mutex poisoned") = state.clone();

    // Update the canonical full map at the same time we calculate the
    // delta, so the next publisher always diffs from exactly this state.
    let delta = {
        let mut last_signals = shared.last_signals.lock().expect("state mutex poisoned");
        let delta = diff_signals(&last_signals, &new_signals);
        *last_signals = new_signals;
        delta
    };

    // Nothing actually changed (e.g. a snapshot_patch that only touched
    // fields this dashboard doesn't surface) -- skip the broadcast instead
    // of sending an empty patch to every open tab.
    let signals_changed = !matches!(&delta, serde_json::Value::Object(map) if map.is_empty());
    if signals_changed {
        let _ = shared.tx.send(StreamEvent::Signals(delta));
    }

    // Evaluate every comparison independently: short-circuiting would
    // leave later remembered values stale and emit redundant structure
    // events on the following tick.
    let epoch_membership_changed = remember_if_changed(&shared.last_epoch_ids, epoch_ids);
    let overview_membership_changed =
        remember_if_changed(&shared.last_overview_epoch_ids, overview_epoch_ids);
    let poh_membership_changed = remember_if_changed(&shared.last_poh_lane_ids, poh_lane_ids);
    let registry_reprocess_membership_changed = remember_if_changed(
        &shared.last_registry_reprocess_lane_ids,
        registry_reprocess_lane_ids,
    );
    let firewatch_rows_changed = remember_if_changed(&shared.last_firewatch_rows, firewatch_rows);
    let firewatch_enabled_changed =
        remember_if_changed(&shared.last_firewatch_enabled, firewatch_enabled);
    let live_changed = remember_if_changed(&shared.last_live, live);
    if epoch_membership_changed
        || overview_membership_changed
        || poh_membership_changed
        || registry_reprocess_membership_changed
        || firewatch_rows_changed
        || firewatch_enabled_changed
        || live_changed
    {
        let _ = shared.tx.send(StreamEvent::Structure);
    }
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
pub async fn set_snapshot(snapshot: PipelineSnapshot) {
    let shared = shared();
    let _publication = shared.publication.lock().await;
    *shared.calendar_source.lock().expect("state mutex poisoned") = CalendarSource {
        epochs: snapshot.epochs.clone(),
        epoch_calendar: snapshot.epoch_calendar.clone(),
        now_unix_secs: snapshot.now_unix_secs,
    };
    *shared.last_snapshot.lock().expect("state mutex poisoned") = Some(snapshot);
    recompute_and_publish_locked().await;
}

/// Called by `runtime_operations.rs` on its own ~5s sampling tick,
/// independent of when the next upstream snapshot/patch happens to arrive.
pub async fn set_local_process_io(io: snapshot::ProcessIoSnapshot) {
    let shared = shared();
    let _publication = shared.publication.lock().await;
    *shared
        .local_process_io
        .lock()
        .expect("state mutex poisoned") = Some(io);
    recompute_and_publish_locked().await;
}

/// Merges the last upstream snapshot with the last locally-collected
/// process I/O sample and publishes the result. A no-op before the first
/// snapshot arrives, and after `set_offline` -- there is nothing live to
/// merge into, and publishing here would otherwise let a `runtime_operations.rs`
/// tick that lands while offline resurrect a stale "live" dashboard.
async fn recompute_and_publish_locked() {
    let Some(mut snapshot) = shared()
        .last_snapshot
        .lock()
        .expect("state mutex poisoned")
        .clone()
    else {
        return;
    };
    if let Some(io) = shared()
        .local_process_io
        .lock()
        .expect("state mutex poisoned")
        .clone()
    {
        snapshot.process_io = Some(io);
    }
    publish(DashboardState::from_snapshot(&snapshot, tier())).await;
}

/// The full epoch list + live-authoritative calendar entries + current
/// time, as of the most recent snapshot -- everything `calendar::build_years`
/// needs that isn't already in `DashboardState`. See `Shared::calendar_source`.
pub fn epochs_for_calendar() -> (
    Vec<snapshot::EpochStatus>,
    Vec<snapshot::EpochCalendarEntry>,
    u64,
) {
    let source = shared()
        .calendar_source
        .lock()
        .expect("state mutex poisoned");
    (
        source.epochs.clone(),
        source.epoch_calendar.clone(),
        source.now_unix_secs,
    )
}

/// Called by `client.rs`'s gap-index poller on a successful fetch.
pub fn set_gap_index(index: snapshot::BlockTimeGapIndex) {
    *shared().gap_index.lock().expect("state mutex poisoned") = Some(index);
    *shared()
        .gap_index_error
        .lock()
        .expect("state mutex poisoned") = None;
}

/// Called by the same poller when a fetch fails -- keeps whatever the last
/// successfully-fetched index was (a transient error shouldn't blank out
/// working data), but records why, so the calendar page can say why the
/// outage overlay might be stale or missing instead of just going quiet.
pub fn set_gap_index_error(reason: String) {
    *shared()
        .gap_index_error
        .lock()
        .expect("state mutex poisoned") = Some(reason);
}

pub fn gap_index() -> (Option<snapshot::BlockTimeGapIndex>, Option<String>) {
    (
        shared()
            .gap_index
            .lock()
            .expect("state mutex poisoned")
            .clone(),
        shared()
            .gap_index_error
            .lock()
            .expect("state mutex poisoned")
            .clone(),
    )
}

/// Called by `client.rs` when the upstream scheduler is unreachable or sends
/// something we can't parse. Keeps the dashboard honestly in the
/// `service_unavailable` state instead of freezing on stale numbers.
pub async fn set_offline(reason: String) {
    let shared = shared();
    let _publication = shared.publication.lock().await;
    *shared.last_snapshot.lock().expect("state mutex poisoned") = None;
    let last_updated = shared
        .current
        .lock()
        .expect("state mutex poisoned")
        .updated_unix_secs;
    let connection_state = if last_updated == 0 {
        "offline"
    } else {
        "stale"
    };
    publish(DashboardState {
        live: false,
        connection_state: connection_state.into(),
        connection_message: reason,
        updated_unix_secs: last_updated,
        ..DashboardState::default()
    })
    .await;
}

/// Deterministic in-process demo data, for UI iteration with no scheduler
/// running. Only started when the binary is launched with `--demo`; never
/// the default, so this dashboard cannot accidentally present fabricated
/// numbers as real telemetry.
pub fn start_demo_simulation() {
    tokio::spawn(async move {
        let mut state = demo_seed();
        state.live = true;
        state.demo = true;
        state.connection_state = "live".into();
        state.connection_message = "Demo data (--demo, no scheduler connected)".into();
        publish(state.clone()).await;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            demo_tick(&mut state);
            publish(state.clone()).await;
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
            EpochTask {
                epoch: 790,
                label: "scanning".into(),
                phase: "Archive V2 Hot Write".into(),
                pct: 88,
                blocks: 376_536,
                eta_secs: 28 * 60 + 58,
                hidden_from_overview: false,
            },
            EpochTask {
                epoch: 791,
                label: "scanning".into(),
                phase: "Prev Blockhash Seed".into(),
                pct: 85,
                blocks: 356_870,
                eta_secs: 40 * 60 + 44,
                hidden_from_overview: false,
            },
            EpochTask {
                epoch: 792,
                label: "finalizing".into(),
                phase: "Registry Index".into(),
                pct: 86,
                blocks: 357_498,
                eta_secs: 39 * 60 + 41,
                hidden_from_overview: false,
            },
            EpochTask {
                epoch: 793,
                label: "queued".into(),
                phase: String::new(),
                pct: 0,
                blocks: 0,
                eta_secs: 0,
                hidden_from_overview: false,
            },
            // Demonstrates the Overview filter: stale, no active process --
            // hidden here, but still visible on the /epochs page.
            EpochTask {
                epoch: 705,
                label: "failed".into(),
                phase: String::new(),
                pct: 62,
                blocks: 210_004,
                eta_secs: 0,
                hidden_from_overview: true,
            },
        ],
        tasks_active: 4,
        tasks_paused: 1,
        error_count: 2,
        errors: vec![
            ErrorEntry {
                at_unix_secs: 1_739_090_100,
                scope: "historical_scan:790".into(),
                message: "retrying after transient CAR read timeout".into(),
            },
            ErrorEntry {
                at_unix_secs: 1_739_089_800,
                scope: "live_finalizer:cap-2026-08-04".into(),
                message: "repair gate waiting on predecessor blockhash tail".into(),
            },
        ],
        compactions: vec![
            CompactionHistoryEntry {
                id: "complete-1010-historical".into(),
                epoch: 1010,
                workflow: "historical".into(),
                completed_unix_secs: Some(1_739_040_000),
                duration_secs: Some(4 * 3600 + 26 * 60 + 11),
            },
            CompactionHistoryEntry {
                id: "complete-1009-live".into(),
                epoch: 1009,
                workflow: "live".into(),
                completed_unix_secs: Some(1_738_980_000),
                duration_secs: Some(2 * 3600 + 12 * 60 + 39),
            },
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
            ProcessEntry {
                id: "lane:replayer".into(),
                name: "replayer".into(),
                pid: 1182,
                cpu_percent: Some(41.8),
                rss_bytes: Some(6_400_000_000),
                read_mib_per_sec: Some(74.0),
                write_mib_per_sec: Some(11.0),
            },
            ProcessEntry {
                id: "lane:packager".into(),
                name: "packager".into(),
                pid: 1184,
                cpu_percent: Some(17.1),
                rss_bytes: Some(2_800_000_000),
                read_mib_per_sec: Some(15.4),
                write_mib_per_sec: Some(7.8),
            },
        ],
        process_io_active_count: 2,
        reasoning: SchedulerReasoning {
            legacy_compact_tuning_last_decision: Some(
                "scaled up: 3 lanes accepted, 78 MiB/s baseline".into(),
            ),
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
    use rand::RngExt;
    let mut rng = rand::rng();

    if let Some(eta) = state.runnable_eta_secs.as_mut() {
        *eta = eta.saturating_sub(rng.random_range(1..=6));
    }
    state.queued = (state.queued as i32 + rng.random_range(-1..=1)).max(0) as u32;
    state.machine.load_1m = (state.machine.load_1m + rng.random_range(-0.25..=0.25)).max(0.0);
    state.live_capture_active =
        rng.random_bool(0.1) ^ state.live_capture_active && rng.random_bool(0.15);

    for task in &mut state.epochs {
        if task.pct < 100 {
            task.pct = (task.pct + rng.random_range(0..=2)).min(100);
            task.blocks += rng.random_range(0..=1200);
            task.eta_secs = task.eta_secs.saturating_sub(rng.random_range(0..=4));
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

    fn firewatch_entry(epoch: u32, state: &str) -> FirewatchIndexEntry {
        FirewatchIndexEntry {
            epoch,
            state: state.into(),
            phase: "target build".into(),
            pct: if state == "accepted" { 100 } else { 0 },
            blocks: 0,
            eta_secs: None,
            wallet_count: None,
            relation_count: None,
            parity_status: None,
            rss_bytes: None,
            read_mib_per_sec: None,
            write_mib_per_sec: None,
        }
    }

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

        assert!(
            state.processes.is_empty(),
            "public tier must not expose process name/pid"
        );
        assert!(
            state.process_io_hidden,
            "UI must say processes are hidden, not absent"
        );
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
        assert!(
            state.errors[0]
                .message
                .contains("/home/operator/private/state")
        );
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
                    auto_pause_reason: Some(
                        "IO PSI full avg10 92.1 reached pause threshold 85.0".into(),
                    ),
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

        assert_eq!(
            state.reasoning.admission_blocked_reason.as_deref(),
            Some("queue at capacity")
        );
        assert_eq!(
            state
                .reasoning
                .legacy_compact_tuning_last_decision
                .as_deref(),
            Some("scaled up: headroom available")
        );
        assert_eq!(
            state.reasoning.legacy_compact_last_action_unix_secs,
            Some(42)
        );
        // Only the auto-paused lane appears -- a running lane needs no
        // explanation and would just be noise.
        assert_eq!(state.reasoning.paused_lanes.len(), 1);
        assert_eq!(state.reasoning.paused_lanes[0].id, "compact_reuse:794");
        assert!(
            state.reasoning.paused_lanes[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("PSI")
        );
        assert!(!state.reasoning.is_empty());
    }

    #[test]
    fn reasoning_is_empty_when_snapshot_has_no_reasons() {
        let state =
            DashboardState::from_snapshot(&PipelineSnapshot::default(), RedactionTier::Full);
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
        assert_eq!(
            state.poh_migration_bytes_label(),
            "418.9 KiB / 976.6 KiB processed"
        );
        // Epoch counts, not bytes, are the primary label -- bytes stay as
        // the small subtext above.
        assert_eq!(
            state.poh_migration_epoch_label(),
            "812 done \u{b7} 197 remaining"
        );
        assert_eq!(state.poh_migration_eta_label(), "6h 12m");
    }

    #[test]
    fn poh_migration_eta_label_is_unknown_without_a_current_rate() {
        let state = DashboardState {
            poh_migration_eta_secs: None,
            ..Default::default()
        };
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
    fn last_updated_label_reports_missing_recent_and_stale_ages() {
        let missing = DashboardState::default();
        assert_eq!(
            missing.last_updated_label_at(100),
            "No scheduler update received"
        );

        let state = DashboardState {
            updated_unix_secs: 100,
            ..Default::default()
        };
        assert_eq!(state.last_updated_label_at(103), "Updated just now");
        assert_eq!(state.last_updated_label_at(165), "Updated 1m 5s ago");
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
    fn registry_reprocess_summary_and_worker_are_mapped_to_distinct_live_signals() {
        let snapshot = PipelineSnapshot {
            summary: snapshot::PipelineSummary {
                registry_reprocess_capacity_configured: 3,
                registry_reprocess_running: 1,
                registry_reprocess_epochs_total: 23,
                registry_reprocess_epochs_done: 2,
                registry_reprocess_admission_blocked_reason: Some(
                    "memory reserve is active".into(),
                ),
                ..Default::default()
            },
            lanes: vec![
                snapshot::LaneStatus {
                    id: "registry_reprocess:1000".into(),
                    kind: "archive_v2_registry_reprocess".into(),
                    state: "running".into(),
                    phase: "registry reprocess".into(),
                    epoch: Some(1000),
                    progress: snapshot::ProgressSnapshot {
                        blocks_done: 456_240,
                        blocks_total: 863_563,
                        progress_pct: None,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                // A PoH lane for the same epoch proves that its existing
                // `epoch_1000_*` signals do not collide with registry data.
                snapshot::LaneStatus {
                    id: "poh_migration:1000".into(),
                    kind: "poh_signature_count_migration".into(),
                    state: "running".into(),
                    epoch: Some(1000),
                    progress: snapshot::ProgressSnapshot {
                        blocks_done: 7,
                        blocks_total: 10,
                        progress_pct: Some(70.0),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);

        assert_eq!(
            state.registry_reprocess_epoch_label(),
            "2 done \u{b7} 21 remaining"
        );
        assert_eq!(
            state.registry_reprocess_worker_label(),
            "1 active \u{b7} capacity 3"
        );
        assert_eq!(state.registry_reprocess_lanes.len(), 1);
        assert_eq!(state.registry_reprocess_lanes[0].epoch, 1000);
        assert_eq!(state.registry_reprocess_lanes[0].pct, 52);
        assert_eq!(
            state
                .reasoning
                .registry_reprocess_admission_blocked_reason
                .as_deref(),
            Some("memory reserve is active")
        );

        let signals = state.to_signals();
        assert_eq!(signals["registry_reprocess_pct"], "8.7");
        assert_eq!(signals["registry_epoch_1000_pct"], 52);
        assert_eq!(signals["registry_epoch_1000_blocks"], "456,240");
        assert_eq!(signals["registry_epoch_1000_eta"], "unknown");
        assert_eq!(signals["epoch_1000_pct"], 70);
        assert_eq!(signals["epoch_1000_blocks"], "7");
    }

    #[test]
    fn firewatch_summary_and_per_epoch_build_evidence_map_to_live_signals() {
        let snapshot = PipelineSnapshot {
            summary: snapshot::PipelineSummary {
                firewatch_index_capacity_configured: 1,
                firewatch_index_running: 1,
                firewatch_index_epochs_total: 3,
                firewatch_index_epochs_accepted: 1,
                firewatch_index_epochs_queued: 1,
                firewatch_index_archive_epochs_total: Some(736),
                firewatch_index_epochs_eligible: Some(729),
                firewatch_index_epochs_blocked_migration: Some(7),
                firewatch_index_epochs_blocked_wire_profile: Some(0),
                firewatch_index_queue_eta_secs: Some(86_400.0),
                firewatch_index_admission_blocked_reason: Some(
                    "archive compaction has storage priority".into(),
                ),
                ..Default::default()
            },
            lanes: vec![
                snapshot::LaneStatus {
                    id: "firewatch_index:301".into(),
                    kind: "firewatch_index".into(),
                    state: "running".into(),
                    phase: "canonical_build".into(),
                    epoch: Some(301),
                    wallet_count: Some(2_045_290),
                    relation_count: Some(6_018_402),
                    parity_status: Some("pending".into()),
                    rss_bytes: Some(900 * 1024 * 1024),
                    progress: snapshot::ProgressSnapshot {
                        blocks_done: 250_000,
                        blocks_total: 400_000,
                        progress_pct: Some(62.5),
                        eta_secs: Some(180.0),
                        source_read_mib_per_sec: Some(82.5),
                        disk_write_mib_per_sec: Some(3.25),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                snapshot::LaneStatus {
                    id: "firewatch_index:900".into(),
                    kind: "firewatch_index".into(),
                    state: "queued".into(),
                    phase: "target_build".into(),
                    epoch: Some(900),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);
        assert!(state.firewatch_enabled);
        assert_eq!(
            state.tasks_active, 1,
            "queued Firewatch rows are not active"
        );
        assert_eq!(state.firewatch_next_epoch, Some(900));
        assert_eq!(state.firewatch_indexes.len(), 2);
        assert_eq!(state.firewatch_indexes[0].phase, "canonical build");
        assert_eq!(
            state.firewatch_summary_label(),
            "1 accepted \u{b7} 1 active \u{b7} 1 queued \u{b7} 0 failed \u{b7} 0 awaiting profile audit"
        );
        assert_eq!(
            state.firewatch_coverage_label(),
            "736 archive-complete \u{b7} 729 indexable \u{b7} 7 blocked by registry migration \u{b7} 0 awaiting profile audit"
        );
        assert_eq!(state.firewatch_queue_eta_label(), "1d 0h");
        assert_eq!(
            state.firewatch_indexes[0].counts_label(),
            "2,045,290 wallets \u{b7} 6,018,402 relations"
        );
        assert_eq!(state.firewatch_indexes[0].parity_label(), "pending");
        assert_eq!(
            state.firewatch_indexes[0].resources_label(),
            "900.0 MiB RSS \u{b7} 82.5/3.2 MiB/s R/W"
        );

        let signals = state.to_signals();
        assert_eq!(signals["firewatch_next_label"], "Epoch 900");
        assert_eq!(signals["firewatch_queue_eta_label"], "1d 0h");
        assert_eq!(
            signals["firewatch_coverage_label"],
            "736 archive-complete \u{b7} 729 indexable \u{b7} 7 blocked by registry migration \u{b7} 0 awaiting profile audit"
        );
        assert_eq!(signals["firewatch_epoch_301_pct"], 62);
        assert_eq!(signals["firewatch_epoch_301_phase"], "canonical build");
        assert_eq!(
            signals["firewatch_epoch_301_counts"],
            "2,045,290 wallets \u{b7} 6,018,402 relations"
        );
        assert_eq!(signals["firewatch_epoch_301_parity"], "pending");
    }

    #[test]
    fn firewatch_active_rows_show_measured_activity_when_progress_is_unavailable() {
        let snapshot = PipelineSnapshot {
            lanes: vec![
                snapshot::LaneStatus {
                    id: "firewatch_index:864".into(),
                    kind: "firewatch_index".into(),
                    state: "running".into(),
                    phase: "source_build".into(),
                    epoch: Some(864),
                    progress: snapshot::ProgressSnapshot {
                        rss_bytes: Some(640 * 1024 * 1024),
                        source_read_mib_per_sec: Some(378.9),
                        eta_secs: Some(180.0),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                snapshot::LaneStatus {
                    id: "firewatch_index:865".into(),
                    kind: "firewatch_index".into(),
                    state: "paused".into(),
                    phase: "target_build".into(),
                    epoch: Some(865),
                    progress: snapshot::ProgressSnapshot {
                        rss_bytes: Some(512 * 1024 * 1024),
                        disk_write_mib_per_sec: Some(12.4),
                        eta_secs: Some(90.0),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);
        assert_eq!(
            state.firewatch_indexes[0].progress_label(),
            "Working \u{b7} 378.9 MiB/s read \u{b7} ETA 3m 0s"
        );
        assert_eq!(
            state.firewatch_indexes[1].progress_label(),
            "Paused \u{b7} ETA 1m 30s"
        );

        let signals = state.to_signals();
        assert_eq!(
            signals["firewatch_epoch_864_progress"],
            "Working \u{b7} 378.9 MiB/s read \u{b7} ETA 3m 0s"
        );
        assert_eq!(
            signals["firewatch_epoch_865_progress"],
            "Paused \u{b7} ETA 1m 30s"
        );
    }

    #[test]
    fn firewatch_profile_audit_rows_are_visible_but_not_active_work() {
        let snapshot = PipelineSnapshot {
            summary: snapshot::PipelineSummary {
                firewatch_index_capacity_configured: 1,
                firewatch_index_epochs_total: 3,
                firewatch_index_epochs_accepted: 1,
                firewatch_index_archive_epochs_total: Some(3),
                firewatch_index_epochs_eligible: Some(2),
                firewatch_index_epochs_blocked_migration: Some(0),
                firewatch_index_epochs_blocked_wire_profile: Some(1),
                ..Default::default()
            },
            lanes: vec![
                snapshot::LaneStatus {
                    id: "firewatch_index:10".into(),
                    kind: "firewatch_index".into(),
                    state: "accepted".into(),
                    phase: "parity".into(),
                    epoch: Some(10),
                    ..Default::default()
                },
                snapshot::LaneStatus {
                    id: "firewatch_index:11".into(),
                    kind: "firewatch_index".into(),
                    state: "failed".into(),
                    phase: "canonical_build".into(),
                    epoch: Some(11),
                    ..Default::default()
                },
                snapshot::LaneStatus {
                    id: "firewatch_index:12".into(),
                    kind: "firewatch_index".into(),
                    state: "profile_audit_required".into(),
                    phase: "wire_profile_audit".into(),
                    epoch: Some(12),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);
        assert_eq!(state.tasks_active, 0);
        assert_eq!(
            state.firewatch_summary_label(),
            "1 accepted \u{b7} 0 active \u{b7} 0 queued \u{b7} 1 failed \u{b7} 1 awaiting profile audit"
        );
        assert_eq!(
            state.firewatch_coverage_label(),
            "3 archive-complete \u{b7} 2 indexable \u{b7} 0 blocked by registry migration \u{b7} 1 awaiting profile audit"
        );
        assert!(
            state
                .overview_firewatch_indexes()
                .iter()
                .any(|entry| entry.epoch == 12)
        );
        let signals = state.to_signals();
        assert_eq!(
            signals["firewatch_epoch_12_state"],
            "profile audit required"
        );
        assert_eq!(signals["firewatch_epoch_12_phase"], "wire profile audit");
    }

    #[test]
    fn firewatch_queued_row_keeps_counter_label_when_progress_is_zero() {
        let entry = FirewatchIndexEntry {
            epoch: 900,
            state: "queued".into(),
            phase: "source build".into(),
            pct: 0,
            blocks: 0,
            eta_secs: None,
            wallet_count: None,
            relation_count: None,
            parity_status: None,
            rss_bytes: Some(256 * 1024 * 1024),
            read_mib_per_sec: Some(0.0),
            write_mib_per_sec: None,
        };

        assert_eq!(entry.progress_label(), "0% \u{b7} 0 blocks");
    }

    #[test]
    fn firewatch_overview_uses_a_bounded_priority_sample() {
        let mut indexes = vec![
            firewatch_entry(1_000, "running"),
            firewatch_entry(999, "paused"),
            firewatch_entry(998, "failed"),
            firewatch_entry(997, "blocked"),
        ];
        indexes.extend((900..930).map(|epoch| firewatch_entry(epoch, "queued")));
        indexes.extend((700..720).map(|epoch| firewatch_entry(epoch, "accepted")));
        let state = DashboardState {
            firewatch_indexes: indexes,
            ..Default::default()
        };

        let visible = state.overview_firewatch_indexes();
        assert_eq!(visible.len(), OVERVIEW_FIREWATCH_LIMIT);
        for epoch in [1_000, 999, 998, 997] {
            assert!(visible.iter().any(|entry| entry.epoch == epoch));
        }
        assert_eq!(
            visible
                .iter()
                .filter(|entry| snapshot::normalize(&entry.state) == "accepted")
                .count(),
            OVERVIEW_FIREWATCH_ACCEPTED_SAMPLE
        );
        assert_eq!(
            state.firewatch_rows_label(),
            "Priority sample: 16 of 54 reported epochs. Project counts above include all reported epochs."
        );

        let signals = state.to_signals();
        assert!(signals.get("firewatch_epoch_1000_state").is_some());
        assert!(signals.get("firewatch_epoch_700_state").is_none());
    }

    #[test]
    fn legacy_firewatch_coverage_label_does_not_claim_full_eligibility() {
        let state = DashboardState {
            archive_complete: 736,
            registry_reprocess_epochs_total: 35,
            registry_reprocess_epochs_done: 28,
            firewatch_epochs_total: 28,
            ..Default::default()
        };
        assert_eq!(
            state.firewatch_coverage_label(),
            "736 archive-complete \u{b7} 28 tracked by this controller \u{b7} 7 blocked by registry migration"
        );
    }

    #[test]
    fn firewatch_overlay_failure_reason_keeps_the_project_visible() {
        let snapshot = PipelineSnapshot {
            summary: snapshot::PipelineSummary {
                firewatch_index_admission_blocked_reason: Some(
                    "Firewatch controller status unavailable".into(),
                ),
                ..Default::default()
            },
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);
        assert!(state.firewatch_enabled);
        assert!(state.firewatch_indexes.is_empty());
        assert_eq!(state.firewatch_epochs_accepted, 0);
    }

    #[test]
    fn format_duration_switches_to_days_past_24_hours() {
        assert_eq!(format_duration(428_400), "4d 23h"); // 119h, the case that motivated this
        assert_eq!(format_duration(86_400), "1d 0h");
        assert_eq!(format_duration(3_600), "1h 0m");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(5), "5s");
    }

    #[test]
    fn epoch_task_phase_flows_from_the_active_lane_into_state_and_signals() {
        let snapshot = PipelineSnapshot {
            epochs: vec![snapshot::EpochStatus {
                epoch: 761,
                state: "scanning".into(),
                ..Default::default()
            }],
            lanes: vec![snapshot::LaneStatus {
                id: "compact_reuse:761".into(),
                kind: "historical_compact_reuse".into(),
                state: "running".into(),
                phase: "Archive V2 Hot Write".into(),
                epoch: Some(761),
                ..Default::default()
            }],
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);

        let task = state.epochs.iter().find(|t| t.epoch == 761).unwrap();
        assert_eq!(task.phase, "Archive V2 Hot Write");
        // Not hidden: it's failed/blocked-only filtering, and this epoch has
        // an active lane regardless.
        assert!(!task.hidden_from_overview);

        let signals = state.to_signals();
        assert_eq!(signals["epoch_761_phase"], "Archive V2 Hot Write");
        assert_eq!(signals["epoch_761_label"], "scanning");
    }

    #[test]
    fn epoch_task_phase_is_empty_without_an_active_lane() {
        let snapshot = PipelineSnapshot {
            epochs: vec![snapshot::EpochStatus {
                epoch: 1010,
                state: "queued".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);

        let task = state.epochs.iter().find(|t| t.epoch == 1010).unwrap();
        assert_eq!(task.phase, "");
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
                snapshot::EpochStatus {
                    epoch: 1,
                    state: "failed".into(),
                    ..Default::default()
                },
                snapshot::EpochStatus {
                    epoch: 2,
                    state: "blocked".into(),
                    ..Default::default()
                },
                // Same failed state, but a lane is still actively retrying it.
                snapshot::EpochStatus {
                    epoch: 3,
                    state: "failed".into(),
                    ..Default::default()
                },
                snapshot::EpochStatus {
                    epoch: 4,
                    state: "scanning".into(),
                    ..Default::default()
                },
            ],
            lanes: vec![snapshot::LaneStatus {
                epoch: Some(3),
                state: "running".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);
        let hidden: std::collections::BTreeMap<u32, bool> = state
            .epochs
            .iter()
            .map(|task| (task.epoch, task.hidden_from_overview))
            .collect();
        assert!(hidden[&1], "failed with no lane must be hidden");
        assert!(hidden[&2], "blocked with no lane must be hidden");
        assert!(
            !hidden[&3],
            "failed but still has an active lane must stay visible"
        );
        assert!(
            !hidden[&4],
            "scanning is never hidden regardless of lane state"
        );
    }

    #[test]
    fn state_retains_all_epochs_while_overview_filters_then_prioritizes_and_caps() {
        let mut epochs: Vec<_> = (1..=14)
            .map(|epoch| snapshot::EpochStatus {
                epoch,
                state: "failed".into(),
                ..Default::default()
            })
            .collect();
        epochs.extend((20..=32).map(|epoch| snapshot::EpochStatus {
            epoch,
            state: "queued".into(),
            ..Default::default()
        }));
        // Deliberately last: the old `.take(12)` in `from_snapshot` lost
        // this active row behind stale hidden failures.
        epochs.push(snapshot::EpochStatus {
            epoch: 99,
            state: "scanning".into(),
            ..Default::default()
        });
        let snapshot = PipelineSnapshot {
            epochs,
            lanes: vec![snapshot::LaneStatus {
                epoch: Some(99),
                state: "running".into(),
                phase: "Archive V2 Hot Write".into(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let state = DashboardState::from_snapshot(&snapshot, RedactionTier::Full);
        let overview: Vec<u32> = state
            .overview_epochs()
            .into_iter()
            .map(|task| task.epoch)
            .collect();

        assert_eq!(state.epochs.len(), 28, "/epochs must retain every row");
        assert_eq!(overview.len(), OVERVIEW_EPOCH_LIMIT);
        assert_eq!(overview[0], 99, "active work must be prioritized");
        assert!(overview.iter().all(|epoch| *epoch > 14));

        let signals = state.to_signals();
        assert!(signals.get("epoch_99_pct").is_some());
        assert!(signals.get("epoch_1_pct").is_none());
        assert!(signals.get("epoch_32_pct").is_none());
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

    #[tokio::test]
    async fn publish_skips_broadcast_when_nothing_changed() {
        let _guard = GLOBAL_STATE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Two states that map to the same signals (e.g. only a field this
        // dashboard doesn't surface changed upstream) must not wake up
        // every open tab with an empty patch.
        let mut rx = subscribe();
        let state = snapshot_blocking();
        publish(state.clone()).await;
        publish(state).await;
        assert!(
            rx.try_recv().is_err(),
            "no delta should have been broadcast"
        );
    }

    #[tokio::test]
    async fn publish_marks_live_offline_transitions_as_structural() {
        let _guard = GLOBAL_STATE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let offline = DashboardState {
            live: false,
            connection_state: "offline".into(),
            ..Default::default()
        };
        publish(offline).await;
        let mut rx = subscribe();

        publish(DashboardState {
            live: true,
            connection_state: "live".into(),
            ..Default::default()
        })
        .await;

        let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, StreamEvent::Structure)),
            "offline-to-live must morph the stable frame"
        );

        publish(DashboardState {
            live: false,
            connection_state: "offline".into(),
            ..Default::default()
        })
        .await;
        let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, StreamEvent::Structure)),
            "live-to-offline must morph the stable frame"
        );
    }

    #[tokio::test]
    async fn disconnect_after_a_snapshot_is_stale_and_preserves_last_update() {
        let _guard = GLOBAL_STATE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        set_snapshot(PipelineSnapshot {
            now_unix_secs: 123,
            ..Default::default()
        })
        .await;
        set_offline("application freshness deadline exceeded".into()).await;

        let state = snapshot_blocking();
        assert!(!state.live);
        assert_eq!(state.connection_state, "stale");
        assert_eq!(state.updated_unix_secs, 123);
        assert_ne!(
            state.last_updated_label_at(183),
            "No scheduler update received"
        );
    }

    #[tokio::test]
    async fn publication_gate_prevents_queued_local_recompute_from_resurrecting_offline_state() {
        let _guard = GLOBAL_STATE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        set_snapshot(PipelineSnapshot {
            now_unix_secs: 456,
            ..Default::default()
        })
        .await;

        let held = shared().publication.lock().await;
        let local = tokio::spawn(set_local_process_io(snapshot::ProcessIoSnapshot {
            state: "ready".into(),
            active_count: 7,
            ..Default::default()
        }));
        tokio::task::yield_now().await;
        let offline = tokio::spawn(set_offline("upstream disconnected".into()));
        tokio::task::yield_now().await;
        assert!(!local.is_finished() && !offline.is_finished());
        drop(held);
        local.await.unwrap();
        offline.await.unwrap();

        let state = snapshot_blocking();
        assert!(!state.live);
        assert_eq!(state.connection_state, "stale");
        assert_eq!(state.updated_unix_secs, 456);
        assert!(
            shared()
                .last_snapshot
                .lock()
                .expect("state mutex poisoned")
                .is_none()
        );
    }

    fn poh_lane_task(epoch: u32) -> EpochTask {
        EpochTask {
            epoch,
            label: "running".into(),
            phase: "PoH Signature Count Migration".into(),
            pct: 40,
            blocks: 1_000,
            eta_secs: 60,
            hidden_from_overview: false,
        }
    }

    fn registry_lane_task(epoch: u32) -> EpochTask {
        EpochTask {
            epoch,
            label: "running".into(),
            phase: "registry reprocess".into(),
            pct: 40,
            blocks: 1_000,
            eta_secs: 60,
            hidden_from_overview: false,
        }
    }

    /// The bug this covers: a worker finishing (or a new one starting)
    /// changes which epochs are in `poh_migration_lanes`/`epochs`, not just
    /// an existing row's field values -- `diff_signals` alone has nothing
    /// to add/remove a DOM row for, since Datastar's signal patches only
    /// update bindings on elements that already exist. `publish` must also
    /// broadcast a `StreamEvent::Structure` whenever that set changes, so
    /// each connected route can morph its complete stable frame and the
    /// Overview list actually gains/loses rows without a manual refresh.
    #[tokio::test]
    async fn publish_broadcasts_structure_when_poh_lane_membership_changes() {
        let _guard = GLOBAL_STATE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Seed at a known membership first so this test doesn't depend on
        // whatever the previous test in this binary happened to leave
        // `last_poh_lane_ids` at (`shared()` is process-global).
        publish(DashboardState {
            poh_migration_lanes: vec![poh_lane_task(493)],
            ..snapshot_blocking()
        })
        .await;

        let mut rx = subscribe();

        // Same membership, only a field value changing: signals only, no
        // structural patch -- the row already exists and `data-text` covers it.
        publish(DashboardState {
            poh_migration_lanes: vec![EpochTask {
                pct: 41,
                ..poh_lane_task(493)
            }],
            ..snapshot_blocking()
        })
        .await;
        assert!(
            (0..8)
                .map(|_| rx.try_recv())
                .take_while(Result::is_ok)
                .all(|event| !matches!(event, Ok(StreamEvent::Structure))),
            "a same-membership publish must not emit a structural patch"
        );

        // Epoch 493 finishes (drops out), epoch 494 starts (appears): a
        // real membership change must emit one structural frame marker.
        publish(DashboardState {
            poh_migration_lanes: vec![poh_lane_task(494)],
            ..snapshot_blocking()
        })
        .await;
        let mut saw_structure = false;
        while let Ok(event) = rx.try_recv() {
            if matches!(event, StreamEvent::Structure) {
                saw_structure = true;
            }
        }
        assert!(
            saw_structure,
            "a membership change must broadcast a StreamEvent::Structure marker"
        );
    }

    #[tokio::test]
    async fn publish_broadcasts_structure_when_registry_lane_membership_changes() {
        let _guard = GLOBAL_STATE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        publish(DashboardState {
            registry_reprocess_lanes: vec![registry_lane_task(1000)],
            ..snapshot_blocking()
        })
        .await;
        let mut rx = subscribe();

        publish(DashboardState {
            registry_reprocess_lanes: vec![registry_lane_task(864)],
            ..snapshot_blocking()
        })
        .await;

        let mut saw_structure = false;
        while let Ok(event) = rx.try_recv() {
            if matches!(event, StreamEvent::Structure) {
                saw_structure = true;
            }
        }
        assert!(
            saw_structure,
            "a registry worker membership change must broadcast a structural frame"
        );
    }

    fn snapshot_blocking() -> DashboardState {
        shared()
            .current
            .lock()
            .expect("state mutex poisoned")
            .clone()
    }

    #[tokio::test]
    async fn local_process_io_merges_into_snapshots_and_is_cleared_on_offline() {
        let _guard = GLOBAL_STATE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        set_offline("reset for test".into()).await;
        set_local_process_io(io(11)).await;
        assert!(!snapshot_blocking().live);

        // Once a snapshot arrives, the already-pending value merges in
        // immediately -- no extra process-I/O tick required.
        set_snapshot(PipelineSnapshot {
            sequence: 1,
            ..Default::default()
        })
        .await;
        assert_eq!(snapshot_blocking().process_io_active_count, 11);

        // A later process-I/O tick updates it without waiting for the next
        // snapshot/patch.
        set_local_process_io(io(12)).await;
        assert_eq!(snapshot_blocking().process_io_active_count, 12);

        // A subsequent snapshot keeps carrying the locally-collected value
        // forward -- it should not require a fresh process-I/O tick.
        set_snapshot(PipelineSnapshot {
            sequence: 2,
            ..Default::default()
        })
        .await;
        assert_eq!(snapshot_blocking().process_io_active_count, 12);

        // Going offline must not leave a stale snapshot around for a late
        // process-I/O tick to republish as "live" again.
        set_offline("test offline".into()).await;
        assert!(!snapshot_blocking().live);
        set_local_process_io(io(13)).await;
        assert!(
            !snapshot_blocking().live,
            "a process-I/O tick alone must not revive the dashboard while offline"
        );
    }
}
