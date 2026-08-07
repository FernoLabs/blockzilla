use topcoat::{
    router::{header, route, HeaderName},
    Result,
};

/// Pre-compiled, minified Tailwind output for exactly the classes this
/// crate's `view!` markup uses -- see `src/assets/app.css` and the README
/// for how to regenerate it after adding new classes.
///
/// This replaces a `<script src="https://cdn.tailwindcss.com">` tag. That
/// CDN build re-scans the DOM and re-compiles CSS with a JIT compiler in
/// the browser on every single page load: an extra render-blocking
/// request, a flash of unstyled content while it runs, and Tailwind's own
/// runtime warns `cdn.tailwindcss.com should not be used in production`.
/// A static stylesheet has none of that.
const APP_CSS: &str = include_str!("../assets/app.css");

#[route(GET "/app.css")]
async fn app_css() -> Result<([(HeaderName, &'static str); 2], &'static str)> {
    Ok((
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        APP_CSS,
    ))
}
