use clap::{Parser, ValueEnum};
use topcoat::router::{tower::TowerLayer, HeaderName, HeaderValue, Path, Router, RouterBuilderDiscoverExt};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::set_header::SetResponseHeaderLayer;

mod api;
mod app;
mod calendar;
mod calendar_view;
mod client;
mod components;
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
/// `blockzilla-watcher-gateway`'s public status API and event stream.
#[derive(Parser, Debug)]
struct Cli {
    /// Base URL of the redacted, public-safe blockzilla-watcher-gateway
    /// (see services/blockzilla-watcher-gateway). This dashboard never talks
    /// to the scheduler's raw status port directly.
    #[arg(long, env = "BLOCKZILLA_MONITOR_UPSTREAM", default_value = "http://127.0.0.1:8787")]
    upstream: String,

    /// Run against a synthetic, in-process ticker instead of a real
    /// gateway -- for local UI iteration only. Never the default: this
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
    /// tab holds one for as long as it's open; nothing else in this binary
    /// or the framework bounds that, so an anonymous public deployment
    /// needs this. Past the cap, `tower::limit::ConcurrencyLimitLayer`
    /// queues new connections rather than rejecting them outright.
    #[arg(long, default_value_t = 512)]
    max_stream_connections: usize,

    /// Local filesystem path to a `blockzilla build-block-time-gap-index`
    /// output (see docs/reference/block-time-gap-sidecar.md). When set,
    /// this is read directly from disk on a poll loop instead of fetched
    /// over HTTP through the gateway's `/api/v1/sidecars/block-time-gaps/index.json`
    /// proxy -- that route has no backend handler in the scheduler as of
    /// this writing (see docs/operations/blockzilla-monitor-roadmap.md),
    /// so this only works when this binary runs on the same host as the
    /// archive. The file itself contains nothing more sensitive than the
    /// bundled reference calendar (epoch numbers, timestamps, slot
    /// numbers, a source SHA-256) -- reading it locally doesn't cross the
    /// gateway's redaction boundary the way talking to the scheduler's raw
    /// status port would.
    #[arg(long)]
    gap_index_file: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

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
        .layer(TowerLayer::new(
            Path::new("/api/stream"),
            ConcurrencyLimitLayer::new(cli.max_stream_connections),
        ))
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
