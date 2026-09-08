use std::num::NonZeroUsize;

use clap::{Parser, ValueEnum};
use topcoat::router::{
    HeaderName, HeaderValue, Router, RouterBuilderDiscoverExt, tower::TowerLayer,
};
use tower_http::set_header::SetResponseHeaderLayer;

mod api;
mod app;
mod calendar;
mod calendar_view;
mod client;
mod components;
mod process_telemetry;
mod public_json;
mod runtime_operations;
mod snapshot;
mod state;

use state::RedactionTier;

/// Which tier of detail this instance serves -- see
/// `docs/operations/blockzilla-monitor-roadmap.md` §3. `full` must never be
/// reachable from the open internet: this binary adds no authentication of
/// its own, so gate it at the network layer (Cloudflare Access, Tailscale,
/// an IP allowlist) if you run it.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Tier {
    Public,
    Full,
}

/// Blockzilla task/health monitor -- a Topcoat + Datastar dashboard fed by
/// the scheduler's loopback-only status API and event stream.
#[derive(Parser, Debug)]
struct Cli {
    /// Base URL of the Blockzilla scheduler's read-only status listener.
    /// Keep this listener private; the monitor is the curated public boundary.
    #[arg(
        long,
        env = "BLOCKZILLA_MONITOR_UPSTREAM",
        default_value = "http://127.0.0.1:8787"
    )]
    upstream: String,

    /// Run against a synthetic, in-process ticker instead of a real
    /// scheduler -- for local UI iteration only. Never the default: this
    /// dashboard should not silently show fabricated numbers as live
    /// telemetry.
    #[arg(long)]
    demo: bool,

    /// `public` drops the process table and scrubs free-text fields for an
    /// anonymous internet audience; `full` shows everything and must only
    /// be exposed behind a network-level gate. Defaults to the safe option.
    #[arg(long, value_enum, default_value = "public")]
    tier: Tier,

    /// Caps concurrent open `/api/stream` connections. Each open dashboard
    /// tab holds one permit until its SSE response body is dropped. Past
    /// the cap, new streams receive 503 and the dashboard retries with
    /// backoff. Zero is rejected because it would disable all updates.
    #[arg(long, default_value = "512")]
    max_stream_connections: NonZeroUsize,

    /// Local filesystem path to a `blockzilla build-block-time-gap-index`
    /// output (see docs/reference/block-time-gap-sidecar.md). When set,
    /// this is read directly from disk on a poll loop instead of fetched
    /// over HTTP. The scheduler has no handler for that sidecar route, so
    /// this only works when the monitor runs on the same host as the
    /// archive. The file itself contains nothing more sensitive than the
    /// bundled reference calendar (epoch numbers, timestamps, slot
    /// numbers, a source SHA-256) -- reading it locally doesn't cross the
    /// monitor's curated public status surface.
    #[arg(long)]
    gap_index_file: Option<std::path::PathBuf>,

    /// Optional schema-1 JSON status from the local user-program-index controller.
    /// When configured, this file is the authoritative source for only the
    /// user-program-index summary and rows; scheduler archive telemetry is unchanged.
    #[arg(
        long,
        alias = "firewatch-status-file",
        env = "BLOCKZILLA_MONITOR_USER_PROGRAM_INDEX_STATUS_FILE"
    )]
    user_program_index_status_file: Option<std::path::PathBuf>,
}

impl Cli {
    /// Old deployments can retain their environment setting. An explicit CLI
    /// value or canonical environment setting always takes precedence.
    fn with_legacy_env(mut self, lookup: impl FnOnce(&str) -> Option<std::ffi::OsString>) -> Self {
        if self.user_program_index_status_file.is_none() {
            self.user_program_index_status_file =
                lookup("BLOCKZILLA_MONITOR_FIREWATCH_STATUS_FILE").map(std::path::PathBuf::from);
        }
        self
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse().with_legacy_env(|name| std::env::var_os(name));

    api::configure_stream_limit(cli.max_stream_connections)
        .expect("stream connection limit must only be configured once");

    state::set_tier(match cli.tier {
        Tier::Public => RedactionTier::Public,
        Tier::Full => RedactionTier::Full,
    });

    if cli.demo {
        state::start_demo_simulation();
    } else {
        client::start(cli.upstream.clone(), cli.user_program_index_status_file);
        runtime_operations::start();
        match cli.gap_index_file {
            Some(path) => client::start_gap_index_file_poller(path),
            None => client::start_gap_index_poller(cli.upstream),
        }
    }

    topcoat::start(monitor_router()).await.unwrap();
}

fn monitor_router() -> Router {
    Router::builder()
        .discover()
        // Operational telemetry has no business in a search index, on
        // either tier.
        .layer(
            TowerLayer::new(SetResponseHeaderLayer::overriding(
                HeaderName::from_static("x-robots-tag"),
                HeaderValue::from_static("noindex, nofollow"),
            ))
            .at("/"),
        )
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn router_serves_complete_pages_and_assets_with_noindex_headers() {
        use topcoat::router::{Body, StatusCode, request::Request, to_bytes};

        let router = monitor_router();
        for (path, view) in [
            ("/", "overview"),
            ("/history", "history"),
            ("/system", "system"),
            ("/epochs", "epochs"),
            ("/calendar", "calendar"),
        ] {
            let response = router
                .handle(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(response.headers()["x-robots-tag"], "noindex, nofollow");
            let body = to_bytes(response.into_body(), 4 * 1024 * 1024)
                .await
                .unwrap();
            let html = std::str::from_utf8(&body).unwrap();
            assert!(html.contains("<title>Blockzilla Monitor</title>"), "{path}");
            assert_eq!(html.matches("id=\"dashboard\"").count(), 1, "{path}");
            assert_eq!(html.matches("id=\"dashboard-frame\"").count(), 1, "{path}");
            assert!(html.contains(&format!("/api/stream?view={view}")), "{path}");
            assert!(
                html.ends_with("</body></html>"),
                "{path}: incomplete SSR body"
            );
        }

        for (path, content_type, expected) in [
            (
                "/app.css",
                "text/css; charset=utf-8",
                include_str!("assets/app.css"),
            ),
            (
                "/datastar.js",
                "text/javascript; charset=utf-8",
                include_str!("assets/datastar.js"),
            ),
        ] {
            let response = router
                .handle(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(response.headers()["x-robots-tag"], "noindex, nofollow");
            assert_eq!(response.headers()["content-type"], content_type);
            assert_eq!(response.headers()["cache-control"], "public, max-age=3600");
            let body = to_bytes(response.into_body(), 4 * 1024 * 1024)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), expected.as_bytes(), "{path}");
        }

        let missing = router
            .handle(
                Request::builder()
                    .uri("/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn zero_stream_limit_is_rejected_by_cli() {
        let parsed = Cli::try_parse_from(["blockzilla-monitor", "--max-stream-connections", "0"]);
        assert!(parsed.is_err());
    }

    #[test]
    fn user_program_index_status_file_accepts_canonical_and_legacy_cli_and_env() {
        let absent = Cli::try_parse_from(["blockzilla-monitor"]).unwrap();
        assert!(absent.user_program_index_status_file.is_none());

        for flag in [
            "--user-program-index-status-file",
            "--firewatch-status-file",
        ] {
            let present = Cli::try_parse_from([
                "blockzilla-monitor",
                flag,
                "/run/blockzilla/user-program-index-status.json",
            ])
            .unwrap()
            .with_legacy_env(|_| panic!("explicit configuration must take precedence"));
            assert_eq!(
                present.user_program_index_status_file.as_deref(),
                Some(std::path::Path::new(
                    "/run/blockzilla/user-program-index-status.json"
                ))
            );
        }

        let legacy = absent.with_legacy_env(|name| {
            assert_eq!(name, "BLOCKZILLA_MONITOR_FIREWATCH_STATUS_FILE");
            Some("/run/blockzilla/legacy-status.json".into())
        });
        assert_eq!(
            legacy.user_program_index_status_file.as_deref(),
            Some(std::path::Path::new("/run/blockzilla/legacy-status.json"))
        );

        use clap::CommandFactory;
        let mut command = Cli::command();
        let argument = command
            .get_arguments()
            .find(|argument| argument.get_id() == "user_program_index_status_file")
            .unwrap();
        assert_eq!(
            argument.get_env(),
            Some(std::ffi::OsStr::new(
                "BLOCKZILLA_MONITOR_USER_PROGRAM_INDEX_STATUS_FILE"
            ))
        );
        let help = command.render_long_help().to_string();
        assert!(help.contains("--user-program-index-status-file"));
        assert!(!help.contains("firewatch"));
    }
}
