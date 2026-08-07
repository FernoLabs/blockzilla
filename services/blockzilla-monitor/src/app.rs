use topcoat::{
    Result,
    router::{layout, page},
    view::view,
};

use crate::calendar;
use crate::calendar_view::calendar_page;
use crate::components::{
    archive_progress, bottom_panels, compaction_history, dashboard_shell, epoch_list, epoch_table,
    live_capture_banner, poh_migration_lane_list, poh_migration_progress, service_unavailable,
    system_dashboard, top_stats,
};
use crate::state::snapshot;

/// Small inline data-URI favicon: a rounded dark square with the app's
/// emerald accent dot. No asset pipeline needed for one 32x32 SVG.
const FAVICON: &str = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='8' fill='%2309090b'/%3E%3Ccircle cx='16' cy='16' r='7' fill='%2334d399'/%3E%3C/svg%3E";

#[layout("/")]
async fn root(slot: Result) -> Result {
    view! {
        <!DOCTYPE html>
        <html lang="en" class="dark">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <meta name="theme-color" content="#09090b" />
                <link rel="icon" href=(FAVICON) />
                <title>"Blockzilla Monitor"</title>
                <link rel="stylesheet" href="/app.css" />
                <script type="module" src="/datastar.js"></script>
            </head>
            <body class="bg-zinc-950 text-zinc-200 font-sans antialiased">(slot?)</body>
        </html>
    }
}

#[page("/")]
async fn overview() -> Result {
    // First paint is a normal server render from the current snapshot -- no
    // flash of empty state while the SSE connection to *this* app's own
    // `/api/stream` spins up. `dashboard_shell` seeds `data-signals` from
    // this same `state` so `data-text` / `data-attr:style` bindings have
    // something correct to show before the first patch lands a beat later.
    let state = snapshot().await;

    view! {
        if state.live {
            dashboard_shell(
                state: &state,
                active: "overview",
                top_stats(state: &state)
                archive_progress(state: &state)
                poh_migration_progress(state: &state)
                poh_migration_lane_list(state: &state)
                live_capture_banner(state: &state)
                epoch_list(state: &state)
                bottom_panels(state: &state)
            )
        } else {
            service_unavailable(
                connection_state: &state.connection_state,
                connection_message: &state.connection_message
            )
        }
    }
}

#[page("/history")]
async fn history() -> Result {
    let state = snapshot().await;
    view! {
        if state.live {
            dashboard_shell(
                state: &state,
                active: "history",
                compaction_history(entries: &state.compactions)
            )
        } else {
            service_unavailable(
                connection_state: &state.connection_state,
                connection_message: &state.connection_message
            )
        }
    }
}

#[page("/system")]
async fn system() -> Result {
    let state = snapshot().await;
    view! {
        if state.live {
            dashboard_shell(
                state: &state,
                active: "system",
                system_dashboard(state: &state)
            )
        } else {
            service_unavailable(
                connection_state: &state.connection_state,
                connection_message: &state.connection_message
            )
        }
    }
}

#[page("/epochs")]
async fn epochs() -> Result {
    let state = snapshot().await;
    view! {
        if state.live {
            dashboard_shell(
                state: &state,
                active: "epochs",
                epoch_table(tasks: &state.epochs)
            )
        } else {
            service_unavailable(
                connection_state: &state.connection_state,
                connection_message: &state.connection_message
            )
        }
    }
}

#[page("/calendar")]
async fn calendar_route() -> Result {
    let state = snapshot().await;

    let (tracked_epochs, live_calendar, now_unix_secs) = crate::state::epochs_for_calendar();
    let merged = calendar::merge_calendars(calendar::reference_calendar(), &live_calendar);
    let latest_tracked_epoch = tracked_epochs.iter().map(|epoch| epoch.epoch).max();
    let extended = calendar::extend_tail(&merged, latest_tracked_epoch);
    let (gap_index, gap_index_error) = crate::state::gap_index();
    let years = calendar::build_years(
        &tracked_epochs,
        &extended,
        now_unix_secs,
        gap_index.as_ref(),
    );
    let has_interruption_data = gap_index.is_some();

    view! {
        if state.live {
            dashboard_shell(
                state: &state,
                active: "calendar",
                calendar_page(
                    years: &years,
                    gap_index_error: gap_index_error.as_deref(),
                    has_interruption_data: has_interruption_data
                )
            )
        } else {
            service_unavailable(
                connection_state: &state.connection_state,
                connection_message: &state.connection_message
            )
        }
    }
}
