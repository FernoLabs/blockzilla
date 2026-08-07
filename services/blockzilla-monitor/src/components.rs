//! View components for the Blockzilla monitor dashboard.
//!
//! Everything here binds to `DashboardState` (see `state.rs`), which is
//! populated either from the real gateway snapshot (`client.rs`) or, only
//! when the binary is launched with `--demo`, a synthetic ticker. Nothing in
//! this file should fabricate data on its own -- if a real field isn't
//! available yet, the component should say so rather than interpolate a
//! placeholder number.
//!
//! ## Design tokens
//!
//! A handful of class fragments repeat everywhere on purpose, so the app
//! reads as one system instead of four hand-tuned pages:
//! - Card surface: `rounded-lg border border-zinc-800/70 bg-white/[0.02]`
//!   (a faint wash, not fully transparent -- flat `border`-only cards on a
//!   plain background read as an unstyled wireframe).
//! - Eyebrow label: `text-[11px] font-medium uppercase tracking-wider
//!   text-zinc-500` for anything naming a stat or a table column.
//! - Numeric value: always paired with `tabular-nums` so a changing digit
//!   count (an SSE patch bumping "9,306" to "10,694") doesn't jiggle
//!   neighboring text.
//! - Interactive elements get `transition-colors` and a focus-visible ring;
//!   nothing should only be discoverable by hover.

use topcoat::{
    view::{component, view, View},
    Result,
};

use crate::state::{
    format_bytes, format_thousands, CompactionHistoryEntry, DashboardState, EpochTask, ErrorEntry,
    MachineSnapshot, ProcessEntry, SchedulerReasoning,
};

const CARD: &str = "rounded-lg border border-zinc-800/70 bg-white/[0.02]";
const EYEBROW: &str = "text-[11px] font-medium uppercase tracking-wider text-zinc-500";
const FOCUS_RING: &str =
    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-zinc-950";

/// Page chrome shared by every route: a fixed-width column (so nothing
/// shifts horizontally between pages), the header/nav, and Datastar's
/// signal seed + live-stream subscription.
///
/// This used to be two things -- an outer shell in `app.rs` duplicated on
/// every page with slightly different widths and padding (`max-w-[1500px]
/// px-8` on Overview, `max-w-[1400px] px-6` everywhere else, some of them
/// double-stacking vertical padding from a wrapper *and* an inner `<main>`)
/// -- which is exactly why navigating between pages visibly shifted
/// content. One shell, one set of numbers, used everywhere.
#[component]
pub async fn dashboard_shell(state: &DashboardState, active: &str, child: View) -> Result<View> {
    let initial_signals = state.to_signals().to_string();
    view! {
        <div
            id="dashboard"
            data-signals=(initial_signals)
            data-on:load__window="@get('/api/stream')"
        >
            <div class="mx-auto max-w-[1400px] px-6">
                header_nav(active: active, state: state)
                <main class="flex flex-col gap-6 py-8">(child)</main>
            </div>
        </div>
    }
}

#[component]
async fn header_nav(active: &str, state: &DashboardState) -> Result<View> {
    view! {
        <header
            class="flex items-center justify-between py-5 border-b border-zinc-800/70"
        >
            <div class="flex items-center gap-10">
                <h1
                    class="flex items-center gap-2 text-base font-semibold text-zinc-100"
                >
                    <span class="h-2 w-2 rounded-sm bg-emerald-400"></span>
                    "Blockzilla Monitor"
                </h1>
                <nav class="flex items-center gap-6 text-sm">
                    nav_link(label: "Overview", href: "/", active: active == "overview")
                    nav_link(
                        label: "History",
                        href: "/history",
                        active: active == "history"
                    )
                    nav_link(
                        label: "System",
                        href: "/system",
                        active: active == "system"
                    )
                    nav_link(
                        label: "Epochs",
                        href: "/epochs",
                        active: active == "epochs"
                    )
                    nav_link(
                        label: "Calendar",
                        href: "/calendar",
                        active: active == "calendar"
                    )
                </nav>
            </div>
            <div class="flex items-center gap-3 text-sm">
                if state.demo {
                    badge_pill(label: "demo data", tone: "amber")
                }
                if state.scheduler_paused {
                    badge_pill(label: "scheduler paused", tone: "amber")
                }
                <div id="live-indicator" class="flex items-center gap-2 text-zinc-400">
                    <span class="relative flex h-2 w-2">
                        <span
                            data-class="{ 'animate-ping': $live }"
                            class=(if state.live {
                                "absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-75"
                            } else {
                                "hidden"
                            })
                        ></span>
                        <span
                            data-class="{ 'bg-emerald-400': $live, 'bg-zinc-600': !$live }"
                            class=(if state.live {
                                "relative inline-flex h-2 w-2 rounded-full bg-emerald-400"
                            } else {
                                "relative inline-flex h-2 w-2 rounded-full bg-zinc-600"
                            })
                        ></span>
                    </span>
                    <span data-text="$connection_state" class="tabular-nums">
                        (state.connection_state.clone())
                    </span>
                </div>
            </div>
        </header>
    }
}

#[component]
async fn badge_pill(label: &str, tone: &str) -> Result<View> {
    let class = match tone {
        "amber" => "rounded-full border border-amber-700/60 bg-amber-500/10 px-2 py-0.5 text-xs font-medium text-amber-400",
        "rose" => "rounded-full border border-rose-700/60 bg-rose-500/10 px-2 py-0.5 text-xs font-medium text-rose-400",
        _ => "rounded-full border border-zinc-700/60 bg-zinc-500/10 px-2 py-0.5 text-xs font-medium text-zinc-400",
    };
    view! { <span class=(class)>(label)</span> }
}

#[component]
async fn nav_link(label: &str, href: &str, active: bool) -> Result<View> {
    let class = if active {
        "border-b-2 border-emerald-400 pb-1 font-medium text-zinc-100".to_string()
    } else {
        format!("border-b-2 border-transparent pb-1 text-zinc-500 transition-colors hover:text-zinc-300 {FOCUS_RING}")
    };
    view! { <a href=(href) class=(class)>(label)</a> }
}

/// Runnable ETA / Queued / Needs action strip.
#[component]
pub async fn top_stats(state: &DashboardState) -> Result<View> {
    let card = CARD;
    view! {
        <div class=(format!("grid grid-cols-[2fr_1fr_1fr] overflow-hidden {card}"))>
            <div class="border-l-2 border-emerald-400 px-6 py-4">
                <div class=(EYEBROW)>"Runnable ETA"</div>
                <div
                    id="runnable-eta"
                    data-text="$runnable_eta_label"
                    class="mt-1 text-3xl font-semibold tabular-nums text-zinc-50"
                >
                    (state.eta_label())
                </div>
            </div>
            <div class="border-l border-zinc-800/70 px-6 py-4">
                <div class=(EYEBROW)>"Queued"</div>
                <div
                    id="queued"
                    data-text="$queued"
                    class="mt-1 text-2xl font-semibold tabular-nums text-zinc-50"
                >
                    (state.queued)
                </div>
            </div>
            <div class="border-l border-zinc-800/70 px-6 py-4">
                <div class=(EYEBROW)>"Needs action"</div>
                <div
                    id="needs-action"
                    data-text="$needs_action"
                    class="mt-1 text-2xl font-semibold tabular-nums text-zinc-50"
                >
                    (state.needs_action)
                </div>
            </div>
        </div>
    }
}

#[component]
pub async fn archive_progress(state: &DashboardState) -> Result<View> {
    let pct = state.archive_pct();
    let card = CARD;
    view! {
        <div class=(format!("flex items-center gap-6 px-6 py-4 {card}"))>
            <div class="flex items-baseline gap-2 whitespace-nowrap">
                <span class=(EYEBROW)>"Archive progress"</span>
                <span
                    id="archive-count"
                    data-text="`${$archive_complete} / ${$archive_total} epochs complete`"
                    class="font-medium tabular-nums text-zinc-100"
                >
                    (format!(
                        "{} / {} epochs complete", state.archive_complete, state
                        .archive_total
                    ))
                </span>
            </div>
            <div class="h-1.5 flex-1 overflow-hidden rounded-full bg-zinc-800">
                <div
                    id="archive-bar"
                    data-attr:style="`width: ${$archive_pct}%`"
                    class="h-full rounded-full bg-emerald-400 transition-[width] duration-500 ease-out"
                    style=(format!("width: {pct:.1}%"))
                ></div>
            </div>
            <div
                id="archive-pct"
                data-text="`${$archive_pct}%`"
                class="whitespace-nowrap font-semibold tabular-nums text-emerald-400"
            >
                (format!("{pct:.1}%"))
            </div>
        </div>
    }
}

#[component]
pub async fn poh_migration_progress(state: &DashboardState) -> Result<View> {
    let pct = state.poh_migration_pct();
    let card = CARD;
    view! {
        <div class=(format!("flex flex-col gap-1 px-6 py-4 {card}"))>
            <div class="flex items-center gap-6">
                <div class="flex items-baseline gap-2 whitespace-nowrap">
                    <span class=(EYEBROW)>"PoH signature-count migration"</span>
                    <span
                        id="poh-migration-count"
                        data-text="$poh_migration_epoch_label"
                        class="font-medium tabular-nums text-zinc-100"
                        title=(state.poh_migration_bytes_label())
                    >
                        (state.poh_migration_epoch_label())
                    </span>
                </div>
                <div class="h-1.5 flex-1 overflow-hidden rounded-full bg-zinc-800">
                    <div
                        id="poh-migration-bar"
                        data-attr:style="`width: ${$poh_migration_pct}%`"
                        class="h-full rounded-full bg-emerald-400 transition-[width] duration-500 ease-out"
                        style=(format!("width: {pct:.1}%"))
                    ></div>
                </div>
                <div
                    id="poh-migration-pct"
                    data-text="`${$poh_migration_pct}%`"
                    class="whitespace-nowrap font-semibold tabular-nums text-emerald-400"
                >
                    (format!("{pct:.1}%"))
                </div>
                <div
                    id="poh-migration-eta"
                    data-text="`ETA ${$poh_migration_eta_label}`"
                    class="whitespace-nowrap text-sm tabular-nums text-zinc-500"
                >
                    (format!("ETA {}", state.poh_migration_eta_label()))
                </div>
            </div>
            <div
                id="poh-migration-bytes"
                data-text="$poh_migration_bytes_label"
                class="text-xs text-zinc-600"
            >
                (state.poh_migration_bytes_label())
            </div>
        </div>
    }
}

/// Which specific epochs the PoH migration workers are on right now.
/// `poh_migration_progress` above is only the aggregate byte total -- with
/// no per-epoch breakdown, there was no way to tell active workers from a
/// stalled/idle migration lane short of reading scheduler logs directly.
/// Reuses `epoch_row` (the same row `epoch_list` renders for the build
/// queue) since `EpochTask`'s shape already fits.
#[component]
pub async fn poh_migration_lane_list(state: &DashboardState) -> Result<View> {
    let card = CARD;
    view! {
        <div class="flex flex-col gap-2">
            <span class=(EYEBROW)>"PoH migration workers"</span>
            <div class=(format!("divide-y divide-zinc-800/70 {card}"))>
                if state.poh_migration_lanes.is_empty() {
                    <div class="px-6 py-4 text-sm text-zinc-500">
                        "No PoH signature-count migration workers running right now."
                    </div>
                } else {
                    for task in &state.poh_migration_lanes {
                        epoch_row(task: task)
                    }
                }
            </div>
        </div>
    }
}

#[component]
pub async fn live_capture_banner(state: &DashboardState) -> Result<View> {
    view! {
        <div
            id="live-capture"
            data-text="$live_capture_active ? 'Live capture active' : 'Live capture idle'"
            class="-mt-2 px-1 text-sm font-medium text-zinc-400"
        >
            (if state.live_capture_active {
                "Live capture active"
            } else {
                "Live capture idle"
            })
        </div>
    }
}

#[component]
pub async fn epoch_list(state: &DashboardState) -> Result<View> {
    let card = CARD;
    let visible: Vec<&EpochTask> = state.epochs.iter().filter(|task| !task.hidden_from_overview).collect();
    let hidden_count = state.epochs.len() - visible.len();
    view! {
        <div class="flex flex-col gap-2">
            <div class=(format!("divide-y divide-zinc-800/70 {card}"))>
                if visible.is_empty() {
                    <div class="px-6 py-4 text-sm text-zinc-500">
                        "No non-complete epochs tracked right now."
                    </div>
                } else {
                    for task in &visible {
                        epoch_row(task: task)
                    }
                }
            </div>
            if hidden_count > 0 {
                <p class="px-1 text-xs text-zinc-600">
                    (format!(
                        "{hidden_count} failed/blocked {} with no active process not shown here -- see ",
                        if hidden_count == 1 { "epoch" } else { "epochs" }
                    ))
                    <a
                        href="/epochs"
                        class="text-zinc-400 underline underline-offset-2 hover:text-zinc-300"
                    >
                        "Epochs"
                    </a>
                    "."
                </p>
            }
        </div>
    }
}

#[component]
pub async fn epoch_table(tasks: &[EpochTask]) -> Result<View> {
    let card = CARD;
    view! {
        <section class=(format!("overflow-hidden {card}"))>
            <div class="border-b border-zinc-800/70 px-6 py-4">
                <h2 class="text-sm font-semibold text-zinc-100">"Epochs"</h2>
                <p class="mt-0.5 text-xs text-zinc-500">
                    "Non-complete epochs from the active queue."
                </p>
            </div>
            <div class="px-6 py-3">
                <table class="w-full text-left text-sm">
                    <thead>
                        <tr class="border-b border-zinc-800/70">
                            <th
                                class=(format!(
                                    "py-2 pr-3 text-left font-normal {EYEBROW}"
                                ))
                            >
                                "Epoch"
                            </th>
                            <th
                                class=(format!(
                                    "px-3 py-2 text-left font-normal {EYEBROW}"
                                ))
                            >
                                "State"
                            </th>
                            <th
                                class=(format!(
                                    "px-3 py-2 text-left font-normal {EYEBROW}"
                                ))
                            >
                                "Blocks"
                            </th>
                            <th
                                class=(format!(
                                    "py-2 pl-3 text-left font-normal {EYEBROW}"
                                ))
                            >
                                "ETA"
                            </th>
                        </tr>
                    </thead>
                    <tbody>
                        if tasks.is_empty() {
                            <tr>
                                <td class="py-3 text-zinc-500" colspan="4">
                                    "No tracked epochs yet"
                                </td>
                            </tr>
                        } else {
                            for task in tasks {
                                epoch_table_row(task: task)
                            }
                        }
                    </tbody>
                </table>
            </div>
        </section>
    }
}

#[component]
async fn epoch_row(task: &EpochTask) -> Result<View> {
    let id = task.dom_id();
    let bar_id = format!("{id}-bar");
    let blocks_id = format!("{id}-blocks");
    let eta_id = format!("{id}-eta");
    // Per-row signal names, e.g. `$epoch_790_pct`, so the SSE patch can
    // update just this row without touching the others.
    let sig = format!("epoch_{}", task.epoch);
    let dot = state_dot_class(&task.label);

    view! {
        <div
            id=(id)
            class="flex items-center gap-6 px-6 py-4 transition-colors hover:bg-white/[0.02]"
        >
            <div class="w-24 font-medium text-zinc-300 tabular-nums">
                (format!("Epoch {}", task.epoch))
            </div>
            <div class="flex w-40 items-center gap-2 capitalize text-zinc-100">
                <span class=(format!("h-1.5 w-1.5 shrink-0 rounded-full {dot}"))></span>
                (task.label.clone())
            </div>
            <div class="h-1.5 flex-1 overflow-hidden rounded-full bg-zinc-800">
                <div
                    id=(bar_id)
                    data-attr:style=(format!("`width: ${{${sig}_pct}}%`"))
                    class="h-full rounded-full bg-emerald-400 transition-[width] duration-500 ease-out"
                    style=(format!("width: {}%", task.pct))
                ></div>
            </div>
            <div class="w-32 text-right">
                <div class=(EYEBROW)>"Blocks"</div>
                <div
                    id=(blocks_id)
                    data-text=(format!("${sig}_blocks"))
                    class="font-medium tabular-nums text-zinc-100"
                >
                    (format_thousands(task.blocks))
                </div>
            </div>
            <div class="w-28 text-right">
                <div class=(EYEBROW)>"Task ETA"</div>
                <div
                    id=(eta_id)
                    data-text=(format!("${sig}_eta"))
                    class="font-semibold tabular-nums text-cyan-400"
                >
                    (task.eta_label())
                </div>
            </div>
        </div>
    }
}

#[component]
async fn epoch_table_row(task: &EpochTask) -> Result<View> {
    let dot = state_dot_class(&task.label);
    view! {
        <tr class="border-b border-zinc-800/70 transition-colors hover:bg-white/[0.02]">
            <td class="py-2 pr-3 font-medium tabular-nums text-zinc-100">
                "Epoch "
                (task.epoch)
            </td>
            <td class="px-3 py-2 text-zinc-300">
                <span class="inline-flex items-center gap-2 capitalize">
                    <span class=(format!("h-1.5 w-1.5 shrink-0 rounded-full {dot}"))></span>
                    (task.label.clone())
                </span>
            </td>
            <td class="px-3 py-2 tabular-nums text-zinc-300">
                (format_thousands(task.blocks))
            </td>
            <td class="py-2 pl-3 tabular-nums text-zinc-300">(task.eta_label())</td>
        </tr>
    }
}

/// Maps an epoch/lane state label to a small status-dot color so rows can
/// be scanned at a glance without reading every word -- kept to a handful
/// of already-established accents (emerald/cyan/amber/rose/zinc) rather
/// than inventing a new palette just for this.
fn state_dot_class(label: &str) -> &'static str {
    let normalized = crate::snapshot::normalize(label);
    match normalized.as_str() {
        "complete" | "completed" => "bg-emerald-400",
        "scanning" | "finalizing" | "active" | "running" | "capturing" | "packaging" => "bg-cyan-400",
        "queued" | "scan_ready" | "ready" | "ready_to_package" => "bg-zinc-500",
        "failed" | "blocked" | "repair_required" => "bg-rose-400",
        _ => "bg-amber-400",
    }
}

/// Bottom summary panels: NAS/system resources, recent error log, and
/// external process I/O -- all sourced from real snapshot fields. Expand
/// state is purely client-side via a per-panel Datastar signal; no round
/// trip needed since the detail is already in the initial payload.
#[component]
pub async fn bottom_panels(state: &DashboardState) -> Result<View> {
    let tasks_label = format!(
        "{} active{}",
        state.tasks_active,
        if state.tasks_paused > 0 { format!(" · {} paused", state.tasks_paused) } else { String::new() }
    );
    let nas_label = format!("load {:.2}", state.machine.load_1m);
    let errors_label = format!("{} errors", state.error_count);
    let io_label = if state.process_io_hidden {
        "hidden in public view".to_string()
    } else {
        format!("{} active", state.process_io_active_count)
    };
    let reasoning_count = state.reasoning.paused_lanes.len()
        + [
            &state.reasoning.admission_blocked_reason,
            &state.reasoning.legacy_compact_admission_blocked_reason,
            &state.reasoning.finalizer_admission_blocked_reason,
        ]
        .iter()
        .filter(|reason| reason.is_some())
        .count();
    let reasoning_label =
        if reasoning_count == 0 { "clear".to_string() } else { format!("{reasoning_count} to review") };
    let card = CARD;

    view! {
        <div class="flex flex-col gap-3">
            <div
                class=(format!(
                    "flex items-center justify-between px-6 py-4 {card}"
                ))
            >
                <span class="text-sm font-semibold text-zinc-100">"Tasks"</span>
                <span class="text-sm tabular-nums text-zinc-400">(tasks_label)</span>
            </div>
            summary_panel(
                key: "reasoning",
                title: "Scheduler reasoning",
                trailing: &reasoning_label,
                open_by_default: reasoning_count > 0,
                scheduler_reasoning(reasoning: &state.reasoning)
            )
            summary_panel(
                key: "nas",
                title: "System resources",
                trailing: &nas_label,
                open_by_default: false,
                machine_summary(machine: &state.machine)
            )
            summary_panel(
                key: "errors",
                title: "Recent error log",
                trailing: &errors_label,
                open_by_default: false,
                error_log(errors: &state.errors)
            )
            summary_panel(
                key: "io",
                title: "External process I/O",
                trailing: &io_label,
                open_by_default: false,
                process_table(
                    processes: &state.processes,
                    hidden: state.process_io_hidden
                )
            )
        </div>
    }
}

/// Surfaces the scheduler's own current admission/pause/tuning reasons --
/// data that was already on the wire (see `snapshot::PipelineSummary`) but
/// unused until now. This is the *current* state only, not a history: the
/// scheduler overwrites these fields in place every tick rather than
/// logging them, so there is nothing to show older than "right now" without
/// a new backend endpoint (tracked as
/// docs/operations/blockzilla-monitor-roadmap.md §5).
#[component]
async fn scheduler_reasoning(reasoning: &SchedulerReasoning) -> Result<View> {
    view! {
        if reasoning.is_empty() {
            <p class="text-sm text-zinc-500">
                "Nothing is currently blocked, auto-paused, or worth flagging."
            </p>
        } else {
            <div class="flex flex-col gap-3 text-sm">
                if let Some(reason) = &reasoning.admission_blocked_reason {
                    reasoning_row(
                        label: "Admission blocked",
                        text: reason,
                        tone: "amber"
                    )
                }
                if let Some(reason) = &reasoning
                    .legacy_compact_admission_blocked_reason {
                    reasoning_row(
                        label: "Legacy compact admission blocked",
                        text: reason,
                        tone: "amber"
                    )
                }
                if let Some(reason) = &reasoning
                    .finalizer_admission_blocked_reason {
                    reasoning_row(
                        label: "Finalizer admission blocked",
                        text: reason,
                        tone: "amber"
                    )
                }
                if let Some(action) = &reasoning.legacy_compact_last_action {
                    let action_label = reasoning
                        .legacy_compact_last_action_unix_secs
                        .map(|secs| {
                            format!("Last tuner action · {}", seconds_ago(secs))
                        })
                        .unwrap_or_else(|| "Last tuner action".to_string());
                    reasoning_row(label: &action_label, text: action, tone: "zinc")
                }
                if let Some(decision) = &reasoning
                    .legacy_compact_tuning_last_decision {
                    reasoning_row(
                        label: "Last tuning decision",
                        text: decision,
                        tone: "zinc"
                    )
                }
                for lane in &reasoning.paused_lanes {
                    let lane_label = format!("Auto-paused · {} ({})", lane.id, lane.kind);
                    let lane_reason = lane.reason.as_deref().unwrap_or("no reason reported");
                    reasoning_row(label: &lane_label, text: lane_reason, tone: "rose")
                }
            </div>
        }
    }
}

#[component]
async fn reasoning_row(label: &str, text: &str, tone: &str) -> Result<View> {
    let dot = match tone {
        "amber" => "bg-amber-400",
        "rose" => "bg-rose-400",
        _ => "bg-zinc-500",
    };
    view! {
        <div class="flex items-start gap-2.5">
            <span class=(format!("mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full {dot}"))></span>
            <div class="flex flex-col gap-0.5">
                <span class=(EYEBROW)>(label)</span>
                <span class="text-zinc-200">(text)</span>
            </div>
        </div>
    }
}

#[component]
async fn summary_panel(key: &str, title: &str, trailing: &str, open_by_default: bool, child: View) -> Result<View> {
    let sig = format!("panel_{key}_open");
    let card = CARD;
    view! {
        <div data-signals=(format!("{{{sig}: {open_by_default}}}")) class=(card)>
            <button
                data-on:click=(format!("${sig} = !${sig}"))
                class=(format!(
                    "flex w-full items-center justify-between px-6 py-4 text-left transition-colors hover:bg-white/[0.02] {FOCUS_RING}"
                ))
            >
                <span class="text-sm font-semibold text-zinc-100">(title)</span>
                <div class="flex items-center gap-4">
                    <span class="text-sm tabular-nums text-zinc-400">(trailing)</span>
                    <span
                        data-text=(format!("${sig} ? '\u{2212}' : '+'"))
                        class="w-4 text-center text-zinc-500 transition-transform"
                    >
                        "+"
                    </span>
                </div>
            </button>
            // No static `hidden` class here: Datastar's `data-show` toggles
            // the element's inline `display` style directly, and a
            // stylesheet-applied `.hidden { display: none }` class doesn't
            // get cleared by that -- the two fought each other and the
            // panel could never actually open. `data-show` alone already
            // matches the signal's initial value on first paint, so there's
            // no flash of wrongly-shown-or-hidden content before Datastar
            // attaches, whichever `open_by_default` is.
            <div
                data-show=(format!("${sig}"))
                class="border-t border-zinc-800/70 px-6 py-4"
            >
                (child)
            </div>
        </div>
    }
}

#[component]
async fn machine_summary(machine: &MachineSnapshot) -> Result<View> {
    let load = format!("{:.2}", machine.load_1m);
    let memory = format!(
        "{} / {}",
        format_bytes(machine.memory_total_bytes - machine.memory_available_bytes),
        format_bytes(machine.memory_total_bytes)
    );
    let swap = format!("{} / {}", format_bytes(machine.swap_used_bytes), format_bytes(machine.swap_total_bytes));
    let disk = format!("{} / {}", format_bytes(machine.disk_used_bytes), format_bytes(machine.disk_total_bytes));
    let device_io = format!(
        "{:.1} / {:.1} MiB/s r/w",
        machine.archive_device_read_mib_per_sec, machine.archive_device_write_mib_per_sec
    );
    let pressure = format!(
        "{:.1}% / {:.1}%",
        machine.pressure_memory_full_avg10, machine.pressure_io_full_avg10
    );
    view! {
        <div class="grid gap-x-8 gap-y-3 text-sm text-zinc-400 sm:grid-cols-2">
            machine_stat(label: "Load (1m)", value: &load)
            machine_stat(label: "Memory", value: &memory)
            machine_stat(label: "Swap", value: &swap)
            machine_stat(label: "Archive disk", value: &disk)
            machine_stat(label: "Archive device I/O", value: &device_io)
            machine_stat(label: "Pressure (mem/io, full avg10)", value: &pressure)
        </div>
    }
}

#[component]
async fn machine_stat(label: &str, value: &str) -> Result<View> {
    view! {
        <p class="flex items-baseline justify-between gap-4 sm:justify-start">
            <span>
                (label)
                ": "
            </span>
            <span class="font-medium tabular-nums text-zinc-200">(value)</span>
        </p>
    }
}

#[component]
async fn error_log(errors: &[ErrorEntry]) -> Result<View> {
    view! {
        if errors.is_empty() {
            <p class="text-sm text-zinc-500">"No recent errors reported."</p>
        } else {
            <ul class="divide-y divide-zinc-800/70 text-sm">
                for error in errors {
                    <li class="flex items-start gap-2.5 py-2.5 first:pt-0 last:pb-0">
                        <span
                            class="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-rose-400"
                        ></span>
                        <div class="flex flex-col gap-0.5">
                            <span class="text-zinc-200">(error.message.clone())</span>
                            <span class="text-xs tabular-nums text-zinc-500">
                                (error.scope.clone())
                                " · "
                                (seconds_ago(error.at_unix_secs))
                                " ago"
                            </span>
                        </div>
                    </li>
                }
            </ul>
        }
    }
}

/// A compact placeholder while the gateway is unavailable or starting.
#[component]
pub async fn service_unavailable(connection_state: &str, connection_message: &str) -> Result<View> {
    let is_retry = connection_state == "retrying" || connection_state == "offline";
    let status_title = if is_retry { "Gateway unavailable" } else { "Connecting to watcher gateway" };
    let status_body = if is_retry {
        "blockzilla-watcher-gateway did not respond. This monitor never shows fabricated numbers in place of a real snapshot."
    } else {
        "Waiting for the first snapshot from blockzilla-watcher-gateway."
    };
    view! {
        <main class="grid min-h-[70vh] place-items-center px-4 py-10">
            <section
                class="max-w-md rounded-xl border border-zinc-800/70 bg-white/[0.02] p-6 text-center"
            >
                <span
                    class="mx-auto mb-3 flex h-2 w-2 items-center justify-center rounded-full bg-amber-400"
                ></span>
                <h2 class="mb-2 text-lg font-semibold text-zinc-100">(status_title)</h2>
                <p class="text-sm text-zinc-300">(status_body)</p>
                <p class="mt-2 text-sm text-zinc-500">
                    "Retrying automatically every few seconds."
                </p>
                <button
                    onclick="window.location.reload()"
                    class=(format!(
                        "mt-4 rounded-md border border-zinc-700 px-3 py-2 text-sm text-zinc-200 transition-colors hover:border-zinc-500 hover:bg-white/[0.03] {FOCUS_RING}"
                    ))
                >
                    "Retry now"
                </button>
                <details class="mt-4 text-left text-sm text-zinc-500">
                    <summary
                        class="cursor-pointer text-zinc-400 transition-colors hover:text-zinc-300"
                    >
                        "Connection details"
                    </summary>
                    <p class="mt-2 whitespace-pre-wrap text-xs text-zinc-500">
                        (connection_message)
                    </p>
                </details>
            </section>
        </main>
    }
}

/// Compaction archive history panel.
#[component]
pub async fn compaction_history(entries: &[CompactionHistoryEntry]) -> Result<View> {
    let show_workflow = entries.iter().any(|entry| entry.workflow.as_str() != "historical");
    let colspan = if show_workflow { 4 } else { 3 };
    let card = CARD;
    view! {
        <section class=(format!("overflow-hidden {card}"))>
            <div
                class="flex items-center justify-between border-b border-zinc-800/70 px-6 py-4"
            >
                <h2 class="text-sm font-semibold text-zinc-100">"Archive history"</h2>
                <p class="text-xs text-zinc-500">
                    "Newest completed runs first, as reported by the scheduler"
                </p>
            </div>
            <div
                class="border-b border-zinc-800/70 px-6 py-3 text-sm tabular-nums text-zinc-400"
            >
                (format!("{} entries", entries.len()))
            </div>
            <div class="max-h-[70vh] overflow-auto">
                <table class="w-full text-sm">
                    <thead>
                        <tr class="border-b border-zinc-800/70 text-left">
                            <th class=(format!("px-4 py-3 {EYEBROW}"))>"Epoch"</th>
                            if show_workflow {
                                <th class=(format!("px-4 py-3 {EYEBROW}"))>"Type"</th>
                            }
                            <th class=(format!("px-4 py-3 {EYEBROW}"))>"Completed"</th>
                            <th class=(format!("px-4 py-3 {EYEBROW}"))>"Duration"</th>
                        </tr>
                    </thead>
                    <tbody>
                        if entries.is_empty() {
                            <tr>
                                <td colspan=(colspan) class="px-4 py-6 text-zinc-500">
                                    "No compactions reported yet"
                                </td>
                            </tr>
                        } else {
                            for entry in entries {
                                history_row(entry: entry, show_workflow: show_workflow)
                            }
                        }
                    </tbody>
                </table>
            </div>
        </section>
    }
}

#[component]
async fn history_row(entry: &CompactionHistoryEntry, show_workflow: bool) -> Result<View> {
    let workflow = match entry.workflow.as_str() {
        "live" => "Live",
        "recompact" => "Recompact",
        "historical" => "Historical",
        _ => "Unknown",
    };
    let completion = entry.completed_unix_secs.map(format_unix_timestamp).unwrap_or_else(|| "—".to_string());
    let duration = entry.duration_secs.map(crate::state::format_duration).unwrap_or_else(|| "—".to_string());
    view! {
        <tr class="border-b border-zinc-800/70 transition-colors hover:bg-white/[0.02]">
            <td class="px-4 py-3 font-medium tabular-nums text-zinc-100">
                (entry.epoch)
            </td>
            if show_workflow {
                <td class="px-4 py-3 text-zinc-400">(workflow)</td>
            }
            <td class="px-4 py-3 tabular-nums text-zinc-400">(completion)</td>
            <td class="px-4 py-3 tabular-nums text-zinc-400">(duration)</td>
        </tr>
    }
}

/// Machine health + external process I/O -- the `/system` page. This
/// replaces the old fabricated "ingest pipeline" mock (three hardcoded
/// stage cards with no backing field); everything below reads from
/// `machine` / `process_io` on the real snapshot.
#[component]
pub async fn system_dashboard(state: &DashboardState) -> Result<View> {
    let card = CARD;
    view! {
        <div class="flex flex-col gap-4">
            <section class=(format!("overflow-hidden {card}"))>
                <div class="border-b border-zinc-800/70 px-6 py-4">
                    <h2 class="text-sm font-semibold text-zinc-100">
                        "Machine health"
                    </h2>
                    <p class="mt-0.5 text-xs text-zinc-500">
                        "From `machine` on the pipeline snapshot."
                    </p>
                </div>
                <div class="px-6 py-4">machine_summary(machine: &state.machine)</div>
            </section>
            <section class=(format!("overflow-hidden {card}"))>
                <div class="border-b border-zinc-800/70 px-6 py-4">
                    <h2 class="text-sm font-semibold text-zinc-100">
                        "External process I/O"
                    </h2>
                    <p class="mt-0.5 text-xs text-zinc-500">
                        "From `process_io` on the pipeline snapshot."
                    </p>
                </div>
                <div class="max-h-96 overflow-auto">
                    process_table(
                        processes: &state.processes,
                        hidden: state.process_io_hidden
                    )
                </div>
            </section>
        </div>
    }
}

#[component]
async fn process_table(processes: &[ProcessEntry], hidden: bool) -> Result<View> {
    view! {
        if hidden {
            <p class="px-6 py-4 text-sm text-zinc-500">
                "Process names and PIDs are hidden in the public view (host fingerprinting risk). Available on the full/operator tier."
            </p>
        } else if processes.is_empty() {
            <p class="px-6 py-4 text-sm text-zinc-500">
                "No process I/O telemetry reported by the snapshot."
            </p>
        } else {
            <table class="w-full text-sm">
                <thead>
                    <tr class="border-b border-zinc-800/70 text-left">
                        <th class=(format!("px-4 py-2 {EYEBROW}"))>"Process"</th>
                        <th class=(format!("px-4 py-2 {EYEBROW}"))>"PID"</th>
                        <th class=(format!("px-4 py-2 {EYEBROW}"))>"CPU"</th>
                        <th class=(format!("px-4 py-2 {EYEBROW}"))>"RSS"</th>
                        <th class=(format!("px-4 py-2 {EYEBROW}"))>"R/W MiB/s"</th>
                    </tr>
                </thead>
                <tbody>
                    for process in processes {
                        process_row(process: process)
                    }
                </tbody>
            </table>
        }
    }
}

#[component]
async fn process_row(process: &ProcessEntry) -> Result<View> {
    let cpu_label = process.cpu_percent.map(|v| format!("{v:.1}%")).unwrap_or_else(|| "-".to_string());
    let rss_label = process.rss_bytes.map(format_bytes).unwrap_or_else(|| "-".to_string());
    view! {
        <tr class="border-b border-zinc-800/70 transition-colors hover:bg-white/[0.02]">
            <td class="px-4 py-2 text-zinc-100">(process.name.clone())</td>
            <td class="px-4 py-2 tabular-nums text-zinc-300">(process.pid)</td>
            <td class="px-4 py-2 tabular-nums text-zinc-300">(cpu_label)</td>
            <td class="px-4 py-2 tabular-nums text-zinc-300">(rss_label)</td>
            <td class="px-4 py-2 tabular-nums text-zinc-300">
                (format_io_cell(
                    process.read_mib_per_sec,
                    process.write_mib_per_sec,
                ))
            </td>
        </tr>
    }
}

fn format_io_cell(read: Option<f64>, write: Option<f64>) -> String {
    match (read, write) {
        (Some(r), Some(w)) => format!("{r:.1}/{w:.1}"),
        (Some(r), None) => format!("{r:.1}/-"),
        (None, Some(w)) => format!("-/{w:.1}"),
        (None, None) => "—".to_string(),
    }
}

/// Formats a Unix timestamp as a UTC calendar date and time
/// (`YYYY-MM-DD HH:MM`). The previous version printed the raw day count
/// since the epoch (e.g. "20127d 18:40"), which is not a date anyone can
/// read -- civil-date math with no extra dependency, per Howard Hinnant's
/// `civil_from_days`: <https://howardhinnant.github.io/date_algorithms.html>.
fn format_unix_timestamp(unix_secs: u64) -> String {
    let seconds = unix_secs as i64;
    let days_since_epoch = seconds.div_euclid(86_400);
    let secs_of_day = seconds.rem_euclid(86_400);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;

    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };

    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}")
}

fn seconds_ago(updated_unix_secs: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(updated_unix_secs);
    let delta = now.saturating_sub(updated_unix_secs);
    if delta < 60 {
        format!("{delta}s")
    } else if delta < 3600 {
        format!("{}m", delta / 60)
    } else if delta < 86_400 {
        format!("{}h", delta / 3600)
    } else {
        format!("{}d", delta / 86_400)
    }
}
