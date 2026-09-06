use topcoat::{
    Result,
    router::{HeaderName, header, route},
};

/// Vendored Datastar client bundle -- see the README for how to update the
/// pinned version.
///
/// This replaces a `<script src="https://cdn.jsdelivr.net/...">` tag.
/// Datastar's own docs recommend self-hosting for production: a CDN
/// dependency is a third-party outage/blocking risk for a dashboard that's
/// supposed to be always-up, leaks every visitor's IP to jsdelivr, and (for
/// an unpinned or SRI-less reference) is a supply-chain risk. Every
/// `data-*` binding in this app depends on this one script loading.
const DATASTAR_JS: &str = include_str!("../assets/datastar.js");

#[route(GET "/datastar.js")]
async fn datastar_js() -> Result<([(HeaderName, &'static str); 2], &'static str)> {
    Ok((
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        DATASTAR_JS,
    ))
}
