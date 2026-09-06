use serde::Deserialize;
use topcoat::{
    Result,
    context::Cx,
    router::{Slot, layout, page},
    view::{View, ViewExt, ViewHandle, view},
};

use crate::calendar;
use crate::calendar_view::calendar_page;
use crate::components::{
    archive_progress, bottom_panels, compaction_history, dashboard_frame, dashboard_shell,
    epoch_list, epoch_table, firewatch_project, live_capture_banner, poh_migration_lane_list,
    poh_migration_progress, registry_reprocess_lane_list, registry_reprocess_progress,
    service_unavailable, system_dashboard, top_stats,
};
use crate::state::{DashboardState, snapshot};

/// The route-specific frame a stream connection must rebuild after a
/// structural state change or resync. The value is also carried in the
/// `/api/stream?view=...` query so one global state broadcast can serve all
/// five pages without embedding page-specific HTML in it.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum DashboardPage {
    Overview,
    History,
    System,
    Epochs,
    Calendar,
}

impl DashboardPage {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Overview => "overview",
            Self::History => "history",
            Self::System => "system",
            Self::Epochs => "epochs",
            Self::Calendar => "calendar",
        }
    }
}

/// Small inline data-URI favicon: a rounded dark square with the app's
/// emerald accent dot. No asset pipeline needed for one 32x32 SVG.
const FAVICON: &str = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='8' fill='%2309090b'/%3E%3Ccircle cx='16' cy='16' r='7' fill='%2334d399'/%3E%3C/svg%3E";

#[layout("/")]
async fn root(slot: Slot<'_>) -> Result<impl View> {
    Ok(view! {
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
            <body class="bg-zinc-950 text-zinc-200 font-sans antialiased">(slot)</body>
        </html>
    })
}

/// Render only the stable morph target. The shell and its stream action
/// stay mounted around this frame through offline/live transitions.
pub(crate) async fn render_dashboard_frame(
    page: DashboardPage,
    state: &DashboardState,
) -> Result<ViewHandle> {
    let cx = &Cx::default();
    if !state.live {
        let last_updated_label = state.last_updated_label();
        return view! { cx =>
            <div id="dashboard-frame">
                service_unavailable(
                    connection_state: &state.connection_state,
                    connection_message: &state.connection_message,
                    last_updated_label: &last_updated_label
                )
            </div>
        }
        .single()
        .await;
    }

    match page {
        DashboardPage::Overview => {
            view! { cx =>
                dashboard_frame(
                    state: state,
                    active: page.as_str(),
                    top_stats(state: state)
                    archive_progress(state: state)
                    poh_migration_progress(state: state)
                    poh_migration_lane_list(state: state)
                    registry_reprocess_progress(state: state)
                    registry_reprocess_lane_list(state: state)
                    firewatch_project(state: state)
                    live_capture_banner(state: state)
                    epoch_list(state: state)
                    bottom_panels(state: state)
                )
            }
            .single()
            .await
        }
        DashboardPage::History => {
            view! { cx =>
                dashboard_frame(
                    state: state,
                    active: page.as_str(),
                    compaction_history(entries: &state.compactions)
                )
            }
            .single()
            .await
        }
        DashboardPage::System => {
            view! { cx =>
                dashboard_frame(
                    state: state,
                    active: page.as_str(),
                    system_dashboard(state: state)
                )
            }
            .single()
            .await
        }
        DashboardPage::Epochs => {
            view! { cx =>
                dashboard_frame(
                    state: state,
                    active: page.as_str(),
                    epoch_table(tasks: &state.epochs)
                )
            }
            .single()
            .await
        }
        DashboardPage::Calendar => {
            let (tracked_epochs, live_calendar, now_unix_secs) =
                crate::state::epochs_for_calendar();
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

            view! { cx =>
                dashboard_frame(
                    state: state,
                    active: page.as_str(),
                    calendar_page(
                        years: &years,
                        gap_index_error: gap_index_error.as_deref(),
                        has_interruption_data: has_interruption_data
                    )
                )
            }
            .single()
            .await
        }
    }
}

async fn render_dashboard_page(page: DashboardPage) -> Result<ViewHandle> {
    // First paint is a normal server render from the current snapshot -- no
    // flash of empty state while the SSE connection spins up. The stable
    // shell exists even when that snapshot is offline, so the same page can
    // recover automatically when the first live snapshot arrives.
    let state = snapshot().await;
    let frame = render_dashboard_frame(page, &state).await?;
    let cx = &Cx::default();
    view! { cx =>
        dashboard_shell(
            state: &state,
            stream_view: page.as_str(),
            (frame)
        )
    }
    .single()
    .await
}

#[page("/")]
async fn overview() -> Result<impl View> {
    Ok(view! { (render_dashboard_page(DashboardPage::Overview).await?) })
}

#[page("/history")]
async fn history() -> Result<impl View> {
    Ok(view! { (render_dashboard_page(DashboardPage::History).await?) })
}

#[page("/system")]
async fn system() -> Result<impl View> {
    Ok(view! { (render_dashboard_page(DashboardPage::System).await?) })
}

#[page("/epochs")]
async fn epochs() -> Result<impl View> {
    Ok(view! { (render_dashboard_page(DashboardPage::Epochs).await?) })
}

#[page("/calendar")]
async fn calendar_route() -> Result<impl View> {
    Ok(view! { (render_dashboard_page(DashboardPage::Calendar).await?) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{EpochTask, FirewatchIndexEntry};

    #[tokio::test]
    async fn offline_first_page_keeps_the_stable_streaming_shell() {
        let state = DashboardState {
            live: false,
            connection_state: "offline".into(),
            connection_message: "scheduler refused connection".into(),
            ..Default::default()
        };
        let frame = render_dashboard_frame(DashboardPage::Overview, &state)
            .await
            .unwrap();
        let cx = &Cx::default();
        let page = view! { cx =>
            dashboard_shell(
                state: &state,
                stream_view: DashboardPage::Overview.as_str(),
                (frame)
            )
        }
        .single()
        .await
        .unwrap()
        .render(cx);

        assert!(page.contains("id=\"dashboard\""));
        assert!(page.contains("id=\"dashboard-frame\""));
        assert!(page.contains("/api/stream?view=overview"));
        assert!(page.contains("retry"));
        assert!(page.contains("1000000"));
        assert!(page.contains("Scheduler unavailable"));
    }

    #[tokio::test]
    async fn live_and_offline_frames_share_the_same_morph_target() {
        let live = DashboardState {
            live: true,
            ..Default::default()
        };
        let offline = DashboardState {
            live: false,
            connection_state: "offline".into(),
            ..Default::default()
        };
        let cx = &Cx::default();
        let live_html = render_dashboard_frame(DashboardPage::Overview, &live)
            .await
            .unwrap()
            .render(cx);
        let offline_html = render_dashboard_frame(DashboardPage::Overview, &offline)
            .await
            .unwrap()
            .render(cx);

        assert_eq!(live_html.matches("id=\"dashboard-frame\"").count(), 1);
        assert_eq!(offline_html.matches("id=\"dashboard-frame\"").count(), 1);
        assert!(live_html.contains("Blockzilla Monitor"));
        assert!(offline_html.contains("Scheduler unavailable"));
    }

    #[tokio::test]
    async fn stale_frame_explains_freshness_failure_and_shows_last_update() {
        let state = DashboardState {
            live: false,
            connection_state: "stale".into(),
            connection_message: "no valid scheduler state for 45 seconds".into(),
            updated_unix_secs: 1,
            ..Default::default()
        };
        let cx = &Cx::default();
        let html = render_dashboard_frame(DashboardPage::Overview, &state)
            .await
            .unwrap()
            .render(cx);

        assert!(html.contains("Scheduler telemetry stale"));
        assert!(html.contains("Stale task numbers have been removed"));
        assert!(html.contains("Updated "));
    }

    #[tokio::test]
    async fn overview_renders_registry_progress_and_active_worker() {
        let state = DashboardState {
            live: true,
            registry_reprocess_epochs_done: 2,
            registry_reprocess_epochs_total: 23,
            registry_reprocess_capacity_configured: 3,
            registry_reprocess_running: 1,
            registry_reprocess_lanes: vec![EpochTask {
                epoch: 1000,
                label: "running".into(),
                phase: "registry reprocess".into(),
                pct: 52,
                blocks: 456_240,
                eta_secs: 0,
                hidden_from_overview: false,
            }],
            ..Default::default()
        };

        let html = render_dashboard_frame(DashboardPage::Overview, &state)
            .await
            .unwrap()
            .render(&Cx::default());

        assert!(html.contains("Registry order conversion"));
        assert!(html.contains("2 done \u{b7} 21 remaining"));
        assert!(html.contains("1 active \u{b7} capacity 3"));
        assert!(html.contains("id=\"registry-epoch-1000\""));
        assert!(html.contains("456,240"));
        assert!(html.contains("width: 52%"));
    }

    #[tokio::test]
    async fn overview_renders_firewatch_project_evidence_and_next_item() {
        let state = DashboardState {
            live: true,
            firewatch_enabled: true,
            firewatch_capacity_configured: 1,
            firewatch_running: 1,
            firewatch_epochs_total: 3,
            firewatch_epochs_accepted: 1,
            firewatch_epochs_queued: 1,
            firewatch_archive_epochs_total: Some(736),
            firewatch_epochs_eligible: Some(729),
            firewatch_epochs_blocked_migration: Some(7),
            firewatch_epochs_blocked_wire_profile: Some(0),
            firewatch_queue_eta_secs: Some(86_400),
            firewatch_next_epoch: Some(900),
            firewatch_admission_blocked_reason: Some("Waiting for archive storage headroom".into()),
            firewatch_indexes: vec![FirewatchIndexEntry {
                epoch: 301,
                state: "accepted".into(),
                phase: "parity".into(),
                pct: 100,
                blocks: 398_090,
                eta_secs: None,
                wallet_count: Some(2_045_290),
                relation_count: Some(6_018_402),
                parity_status: Some("equal".into()),
                rss_bytes: Some(900 * 1024 * 1024),
                read_mib_per_sec: Some(82.5),
                write_mib_per_sec: Some(3.25),
            }],
            ..Default::default()
        };

        let html = render_dashboard_frame(DashboardPage::Overview, &state)
            .await
            .unwrap()
            .render(&Cx::default());

        assert!(html.contains("Firewatch indexes"));
        assert!(html.contains(
            "1 accepted \u{b7} 1 active \u{b7} 1 queued \u{b7} 0 failed \u{b7} 0 awaiting profile audit"
        ));
        assert!(html.contains(
            "736 archive-complete \u{b7} 729 indexable \u{b7} 7 blocked by registry migration \u{b7} 0 awaiting profile audit"
        ));
        assert!(html.contains("Next queued"));
        assert!(html.contains("Epoch 900"));
        assert!(html.contains("Runnable-work ETA"));
        assert!(html.contains("Failed epochs and profile-audit work are excluded."));
        assert!(html.contains("1d 0h"));
        assert!(html.contains("All 1 reported epochs"));
        assert!(html.contains("id=\"firewatch-epoch-301\""));
        assert!(html.contains("2,045,290 wallets \u{b7} 6,018,402 relations"));
        assert!(html.contains("Waiting for archive storage headroom"));
        assert!(html.contains("82.5/3.2 MiB/s R/W"));
    }

    #[tokio::test]
    async fn overview_hides_firewatch_until_scheduler_reports_the_project() {
        let state = DashboardState {
            live: true,
            ..Default::default()
        };
        let html = render_dashboard_frame(DashboardPage::Overview, &state)
            .await
            .unwrap()
            .render(&Cx::default());

        assert!(!html.contains("id=\"firewatch-project\""));
    }

    #[tokio::test]
    async fn epochs_frame_renders_rows_beyond_the_overview_limit() {
        let state = DashboardState {
            live: true,
            epochs: (1..=15)
                .map(|epoch| EpochTask {
                    epoch,
                    label: "queued".into(),
                    phase: String::new(),
                    pct: 0,
                    blocks: 0,
                    eta_secs: 0,
                    hidden_from_overview: false,
                })
                .collect(),
            ..Default::default()
        };
        let cx = &Cx::default();
        let html = render_dashboard_frame(DashboardPage::Epochs, &state)
            .await
            .unwrap()
            .render(cx);

        assert!(html.contains("Epoch 15"));
    }
}
