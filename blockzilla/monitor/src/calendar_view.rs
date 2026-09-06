//! Rendering for the `/calendar` page. `calendar.rs` computes what to draw
//! (year grids, day tones, the interruption overlay); this file is only
//! concerned with turning that into HTML -- the port of
//! `web/blockzilla-watcher/src/lib/EpochYearCalendar.svelte`.
//!
//! The grid uses inline CSS (`grid-column`/`grid-row`/`grid-template-columns`)
//! rather than Tailwind utility classes for cell positioning: a year can be
//! up to ~53 weeks wide and every cell's column/row is a distinct runtime
//! number, which isn't something a build-time class scanner can enumerate.
//! Colors, borders, and everything else that only takes a handful of fixed
//! values stay as ordinary Tailwind classes, consistent with the rest of
//! this crate.

use topcoat::{
    Result,
    view::{View, component, view},
};

use crate::calendar::{CalendarDay, CalendarYear, InterruptionCoverage};

const CARD: &str = "rounded-lg border border-zinc-800/70 bg-white/[0.02]";

const CELL_PX: u32 = 11;
const GAP_PX: u32 = 2;
const MONTH_ROW_PX: u32 = 14;

#[component]
pub async fn calendar_page(
    years: &[CalendarYear],
    gap_index_error: Option<&str>,
    has_interruption_data: bool,
) -> Result<View> {
    view! {
        <section class=(format!("overflow-hidden {CARD}"))>
            <div class="border-b border-zinc-800/70 px-6 py-4">
                <h2 class="text-sm font-semibold text-zinc-100">"Archive calendar"</h2>
                <p class="mt-0.5 text-xs text-zinc-500">
                    "One cell per UTC day a tracked epoch was active or complete. Ported from the retired Svelte watcher UI."
                </p>
            </div>
            legend()
            if let Some(error) = gap_index_error {
                interruption_status_banner(
                    error: error,
                    has_interruption_data: has_interruption_data
                )
            }
            if years.is_empty() {
                <p class="px-6 py-6 text-sm text-zinc-500">
                    "No epoch date data yet -- waiting for the first snapshot."
                </p>
            } else {
                <div class="flex flex-col divide-y divide-zinc-800/70">
                    for year in years {
                        year_block(year: year)
                    }
                </div>
            }
        </section>
    }
}

#[component]
async fn interruption_status_banner(error: &str, has_interruption_data: bool) -> Result<View> {
    let message = if has_interruption_data {
        format!(
            "Interruption overlay is showing the last successful fetch; most recent refresh failed: {error}"
        )
    } else {
        format!(
            "Interruption overlay (slot-time-drift outage markers) isn't available yet -- the block-time-gap sidecar hasn't been built/served: {error}"
        )
    };
    view! {
        <div
            class="border-b border-zinc-800/70 bg-amber-500/5 px-6 py-3 text-xs text-amber-400"
        >
            (message)
        </div>
    }
}

#[component]
async fn legend() -> Result<View> {
    const ENTRIES: [(&str, &str); 5] = [
        ("emerald", "archived"),
        ("cyan", "in progress"),
        ("zinc", "queued / untracked"),
        ("amber", "needs attention"),
        ("rose", "failed"),
    ];
    view! {
        <div
            class="flex flex-wrap items-center gap-x-4 gap-y-2 border-b border-zinc-800/70 px-6 py-3 text-xs text-zinc-400"
        >
            for (color, label) in ENTRIES {
                <span class="inline-flex items-center gap-1.5">
                    <span
                        class=(format!(
                            "h-2.5 w-2.5 rounded-[2px] {}", tone_bg_class(color)
                        ))
                    ></span>
                    (label)
                </span>
            }
            <span class="inline-flex items-center gap-1.5">
                <span
                    class="h-2.5 w-2.5 rounded-[2px] bg-zinc-700"
                    style="box-shadow: inset 0 2px 0 0 #fbbf24;"
                ></span>
                "block-time interruption"
            </span>
            <span
                class="h-2.5 w-2.5 rounded-[2px] bg-zinc-700"
                style="outline: 1px dashed #71717a; outline-offset: -1px;"
            ></span>
            <span>"interruption index unavailable for that day"</span>
        </div>
    }
}

#[component]
async fn year_block(year: &CalendarYear) -> Result<View> {
    let grid_width = year.week_count * (CELL_PX + GAP_PX);
    let grid_style = format!(
        "display:grid; grid-template-columns: repeat({}, {CELL_PX}px); grid-template-rows: {MONTH_ROW_PX}px repeat(7, {CELL_PX}px); column-gap: {GAP_PX}px; row-gap: {GAP_PX}px; width: {grid_width}px;",
        year.week_count.max(1)
    );
    let interruption_note = if year.interruption_day_count > 0 {
        Some(format!(
            "{} interruption {}",
            year.interruption_day_count,
            if year.interruption_day_count == 1 {
                "day"
            } else {
                "days"
            }
        ))
    } else {
        None
    };
    let missing_note = if year.missing_interruption_coverage_day_count > 0 {
        Some(format!(
            "{} unindexed {}",
            year.missing_interruption_coverage_day_count,
            if year.missing_interruption_coverage_day_count == 1 {
                "day"
            } else {
                "days"
            }
        ))
    } else {
        None
    };

    view! {
        <div class="flex flex-col gap-2 px-6 py-4 sm:flex-row sm:items-start">
            <div class="flex shrink-0 flex-col gap-0.5 sm:w-28">
                <h3 class="text-sm font-semibold tabular-nums text-zinc-200">
                    (year.year)
                </h3>
                <span class="text-[11px] tabular-nums text-zinc-500">
                    (format!(
                        "{}/{} archived", year.archived_epoch_count, year.epoch_count
                    ))
                </span>
                if let Some(note) = &interruption_note {
                    <span class="text-[11px] text-amber-400">(note.clone())</span>
                }
                if let Some(note) = &missing_note {
                    <span class="text-[11px] text-zinc-600">(note.clone())</span>
                }
            </div>
            <div class="min-w-0 overflow-x-auto">
                <div style=(grid_style) role="img" aria-label=(year_aria_label(year))>
                    for (label, week) in &year.months {
                        <span
                            style=(format!(
                                "grid-column: {} / span 3; grid-row: 1;", week + 1
                            ))
                            class="whitespace-nowrap text-[9px] leading-[14px] text-zinc-500"
                        >
                            (label.clone())
                        </span>
                    }
                    for day in &year.days {
                        day_cell(day: day)
                    }
                </div>
            </div>
        </div>
    }
}

#[component]
async fn day_cell(day: &CalendarDay) -> Result<View> {
    let position = format!(
        "grid-column: {}; grid-row: {};",
        day.week + 1,
        day.weekday + 2
    );
    let Some(content) = &day.content else {
        // Before the first tracked epoch, or in the future: a plain dim
        // placeholder so the grid stays a filled rectangle instead of
        // having holes -- mirrors the Svelte template's `empty-day` span.
        let style = format!(
            "{position} opacity: {};",
            if day.future { 0.25 } else { 0.6 }
        );
        return view! { <span style=(style) class="rounded-[1px] bg-zinc-800"></span> };
    };

    let mut box_shadow_layers = Vec::new();
    if content.interruption.is_some() {
        box_shadow_layers.push("inset 0 2px 0 0 #fbbf24".to_string());
    }
    if day.today {
        box_shadow_layers.push("0 0 0 1px #f4f4f5".to_string());
    }
    let box_shadow = if box_shadow_layers.is_empty() {
        String::new()
    } else {
        format!("box-shadow: {};", box_shadow_layers.join(", "))
    };
    let outline = if content.interruption_coverage == InterruptionCoverage::Missing {
        "outline: 1px dashed #71717a; outline-offset: -1px;"
    } else {
        ""
    };
    let opacity = if content.estimated {
        "opacity: 0.82;"
    } else {
        ""
    };
    let style = format!("{position} {box_shadow} {outline} {opacity}");

    view! {
        <span
            style=(style)
            class=(format!("rounded-[1px] {}", tone_bg_class(content.color)))
            title=(day_title(day, content))
        ></span>
    }
}

fn day_title(day: &CalendarDay, content: &crate::calendar::CalendarDayContent) -> String {
    let epochs = content
        .epochs
        .iter()
        .map(|epoch| epoch.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let mut parts = vec![
        day.date.clone(),
        format!(
            "Epoch{} {epochs}",
            if content.epochs.len() > 1 { "s" } else { "" }
        ),
        content.label.clone(),
    ];
    if content.estimated {
        parts.push("estimated chain date".to_string());
    }
    if let Some(interruption) = &content.interruption {
        parts.push(format!(
            "{} block-time {} · longest {}{}",
            interruption.count,
            if interruption.count == 1 {
                "interruption"
            } else {
                "interruptions"
            },
            format_elapsed(interruption.longest_secs),
            if interruption.boundary_count > 0 {
                format!(
                    " · {} crosses an epoch boundary",
                    interruption.boundary_count
                )
            } else {
                String::new()
            }
        ));
    }
    if content.interruption_coverage == InterruptionCoverage::Missing {
        parts.push("interruption index unavailable for one or more epochs this day".to_string());
    }
    parts.join(" · ")
}

fn format_elapsed(seconds: u64) -> String {
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{}m", minutes.max(1))
    }
}

fn year_aria_label(year: &CalendarYear) -> String {
    format!(
        "{} daily UTC archive coverage: {} of {} epochs that started this year are archived. {} days contain observed block-time interruptions. {} days have unavailable interruption coverage. Use the Epochs tab for exact archive details.",
        year.year,
        year.archived_epoch_count,
        year.epoch_count,
        year.interruption_day_count,
        year.missing_interruption_coverage_day_count
    )
}

fn tone_bg_class(color: &str) -> &'static str {
    match color {
        "emerald" => "bg-emerald-500/80",
        "cyan" => "bg-cyan-500/80",
        "amber" => "bg-amber-500/80",
        "rose" => "bg-rose-500/80",
        _ => "bg-zinc-600",
    }
}
