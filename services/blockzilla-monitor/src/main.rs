use std::num::NonZeroUsize;

use clap::{Parser, ValueEnum};
use topcoat::router::{
    HeaderName, HeaderValue, Path, Router, RouterBuilderDiscoverExt, tower::TowerLayer,
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
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    api::configure_stream_limit(cli.max_stream_connections)
        .expect("stream connection limit must only be configured once");

    state::set_tier(match cli.tier {
        Tier::Public => RedactionTier::Public,
        Tier::Full => RedactionTier::Full,
    });

    if cli.demo {
        state::start_demo_simulation();
    } else {
        client::start(cli.upstream.clone());
        runtime_operations::start();
        match cli.gap_index_file {
            Some(path) => client::start_gap_index_file_poller(path),
            None => client::start_gap_index_poller(cli.upstream),
        }
    }

    let router = Router::builder()
        .discover()
        // Operational telemetry has no business in a search index, on
        // either tier.
        .layer(TowerLayer::new(
            Path::new("/"),
            SetResponseHeaderLayer::overriding(
                HeaderName::from_static("x-robots-tag"),
                HeaderValue::from_static("noindex, nofollow"),
            ),
        ))
        .build();

    topcoat::start(router).await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_stream_limit_is_rejected_by_cli() {
        let parsed = Cli::try_parse_from(["blockzilla-monitor", "--max-stream-connections", "0"]);
        assert!(parsed.is_err());
    }
}
