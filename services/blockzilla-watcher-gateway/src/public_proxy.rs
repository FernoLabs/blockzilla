use crate::{
    ingest_contract::{MAX_INGEST_JSON_BYTES, public_ingest_json_bytes},
    public_json::{MAX_JSON_BYTES, public_json_bytes},
    sse::{MAX_SSE_LINE_BYTES, public_sse_line},
};
use anyhow::{Context, Result, anyhow, ensure};
use futures_util::{StreamExt, TryStreamExt};
use http_body::Frame;
use http_body_util::StreamBody;
use serde_json::Value;
use std::{
    borrow::Cow,
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
    time::SystemTime,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use topcoat::Result as TopcoatResult;
use topcoat::context::{Cx, app_context};
use topcoat::router::{
    Body, HeaderMap, HeaderName, HeaderValue, Method, Path, Response, RouteFn, RouteFuture, Router,
    StatusCode, header, headers, method, uri,
};

const PUBLIC_ERROR: &[u8] = br#"{"error":"watcher upstream unavailable"}"#;
const RESPONSE_CHUNK_BYTES: usize = 64 * 1024;
// Process upstream SSE chunks incrementally so one transport frame cannot make
// every connected client allocate a complete multi-megabyte snapshot before
// the shared JSON-transform gate is acquired.
const SSE_UNGATED_CHUNK_BYTES: usize = 16 * 1024;
const PUBLIC_API_GETS: &[&str] = &[
    "/api/v1/events",
    "/api/v1/sidecars/block-time-gaps/index.json",
    "/api/v1/sidecars/ingest-pipeline/status.json",
    "/api/v1/sidecars/runtime-operations/status.json",
    "/api/v1/sidecars/shred-ingest/status.json",
    "/api/v1/status",
];
const MONITOR_UI_SHELL: &str = r#"<!doctype html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Blockzilla Monitor</title>
    <script
      type="module"
      src="https://cdn.jsdelivr.net/gh/starfederation/datastar@1.0.2/bundles/datastar.js"
    ></script>
    <style>
      :root { color-scheme: dark; font-family: system-ui, sans-serif; }
      body { margin: 0; padding: 24px; }
      pre { background: #101010; border-radius: 8px; padding: 12px; overflow: auto; white-space: pre-wrap; }
      button { margin-right: 8px; padding: 6px 10px; }
      .grid { display: grid; gap: 12px; }
      .row { display: flex; gap: 8px; align-items: center; }
      .subtitle { color: #c0c0c0; }
    </style>
  </head>
  <body
    data-signals='{"monitor_status":"loading status...","status_last_fetched":"never","backend":null}'
  >
    <h1>Blockzilla Monitor</h1>
    <p class="subtitle">Single-process Topcoat + Datastar monitor shell.</p>
    <p data-text="$backend"></p>
    <div class="grid">
      <div class="row">
        <button onclick="window.location.href='/api/v1/status'">Status JSON</button>
        <button onclick="window.location.href='/api/v1/events'">SSE</button>
        <button
          id="refresh-status"
          data-on:click="@get('/ui/status')"
        >Refresh status</button>
      </div>
      <section class="grid">
        <p>Last fetched: <span id="status-fetched" data-text="$status_last_fetched"></span></p>
        <div>
          <button id="auto-refresh" onclick="return false;">Auto refresh</button>
          <span id="auto-refresh-state">off</span>
        </div>
        <pre id="status" data-text="$monitor_status"></pre>
      </section>
      <section id="event-log" class="grid">
        <strong>Live event stream (raw):</strong>
        <pre id="events"></pre>
      </section>
    </div>
    <script>
      window.__blockzillaMonitorAutoRefresh = null;
      const autoRefreshButton = document.getElementById('auto-refresh');
      const autoRefreshLabel = document.getElementById('auto-refresh-state');
      const fetchedNode = document.getElementById('status-fetched');
      const refreshStatus = () => {
        if (window.datastar && window.datastar.sendAction) {
          window.datastar.sendAction("@get('/ui/status')");
          return true;
        }
        return false;
      };
      const render = async () => {
        try {
          const response = await fetch('/api/v1/status');
          const text = await response.text();
          const statusNode = document.getElementById('status');
          if (statusNode) {
            statusNode.textContent = response.ok ? text : `status ${response.status}`;
          }
          if (fetchedNode) {
            fetchedNode.textContent = new Date().toISOString();
          }
        } catch (error) {
          document.getElementById('status').textContent = String(error);
          if (fetchedNode) {
            fetchedNode.textContent = new Date().toISOString();
          }
        }
      };
      const events = new EventSource('/api/v1/events');
      events.onmessage = (event) => {
        const output = document.getElementById('events');
        output.textContent = `${output.textContent}${event.data}\n`;
      };
      const bootstrap = async () => {
        const datastarPatched = refreshStatus();
        if (!datastarPatched) {
          await render();
          setInterval(render, 5000);
        }
      };
      bootstrap();
      autoRefreshButton.addEventListener('click', () => {
        if (window.__blockzillaMonitorAutoRefresh === null) {
          if (window.datastar && window.datastar.sendAction) {
            window.__blockzillaMonitorAutoRefresh = setInterval(refreshStatus, 5000);
          } else {
            window.__blockzillaMonitorAutoRefresh = setInterval(render, 5000);
          }
          autoRefreshLabel.textContent = 'on';
        } else {
          clearInterval(window.__blockzillaMonitorAutoRefresh);
          window.__blockzillaMonitorAutoRefresh = null;
          autoRefreshLabel.textContent = 'off';
        }
      });
    </script>
  </body>
</html>
"#;
const COPY_REQUEST_HEADERS: &[&str] = &[
    "accept",
    "cache-control",
    "if-modified-since",
    "if-none-match",
    "range",
    "user-agent",
];
const SAFE_RESPONSE_HEADERS: &[&str] = &[
    "accept-ranges",
    "cache-control",
    "content-encoding",
    "content-language",
    "content-length",
    "content-range",
    "etag",
    "expires",
    "last-modified",
    "vary",
];
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];
#[derive(Debug, Clone, clap::Args)]
pub struct ServeArgs {
    /// Explicit loopback or RFC1918 listener. Wildcard and public binds are rejected.
    #[arg(long, default_value = "127.0.0.1:8787")]
    listen: String,

    /// Loopback-only scheduler/UI upstream.
    #[arg(long, default_value = "127.0.0.1:8786")]
    upstream: String,

    /// Separate loopback-only ingest-status upstream.
    #[arg(long, default_value = "127.0.0.1:8790")]
    ingest_upstream: String,

    /// Whole upstream request timeout.
    #[arg(long, default_value_t = 60.0)]
    upstream_timeout_secs: f64,

    /// Maximum requests or SSE clients held concurrently.
    #[arg(long, default_value_t = 64)]
    max_requests: usize,

    /// Maximum CPU-heavy JSON/SSE redactions running concurrently.
    #[arg(long, default_value_t = 2)]
    max_json_transforms: usize,
}

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    upstream: SocketAddr,
    ingest_upstream: SocketAddr,
    slots: Arc<Semaphore>,
    json_transforms: Arc<Semaphore>,
}

pub async fn serve(args: ServeArgs) -> Result<()> {
    ensure!(
        args.upstream_timeout_secs.is_finite() && args.upstream_timeout_secs > 0.0,
        "--upstream-timeout-secs must be positive"
    );
    ensure!(args.max_requests > 0, "--max-requests must be positive");
    ensure!(
        args.max_json_transforms > 0 && args.max_json_transforms <= args.max_requests,
        "--max-json-transforms must be positive and no greater than --max-requests"
    );
    let listen = network_address(&args.listen, "--listen", true)?;
    let upstream = network_address(&args.upstream, "--upstream", false)?;
    let ingest_upstream = network_address(&args.ingest_upstream, "--ingest-upstream", false)?;
    ensure!(
        listen != upstream && listen != ingest_upstream,
        "--listen must differ from both upstream addresses"
    );
    ensure!(
        upstream != ingest_upstream,
        "--upstream and --ingest-upstream must differ"
    );

    let client = build_upstream_client(Duration::from_secs_f64(args.upstream_timeout_secs))?;
    let state = Arc::new(AppState {
        client,
        upstream,
        ingest_upstream,
        slots: Arc::new(Semaphore::new(args.max_requests)),
        json_transforms: Arc::new(Semaphore::new(args.max_json_transforms)),
    });

    let router = Router::builder()
        .route(RouteFn::new(
            &[Method::GET],
            Cow::Borrowed(Path::new("/ui/status")),
            monitor_status_route_handler,
        ))
        .route(RouteFn::new(
            &[Method::GET, Method::HEAD],
            Cow::Borrowed(Path::new("/api/{*path}")),
            proxy_route_handler,
        ))
        .route(RouteFn::new(
            &[Method::GET],
            Cow::Borrowed(Path::new("/{*path}")),
            monitor_shell_route_handler,
        ))
        .app_context(state)
        .build();

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind public watcher on {listen}"))?;
    topcoat::serve(listener, router)
        .await
        .context("serve public watcher gateway")
}

fn monitor_shell_route_handler(cx: &Cx, body: Body) -> RouteFuture<'_> {
    Box::pin(monitor_shell_route(cx, body))
}

fn monitor_status_route_handler(cx: &Cx, body: Body) -> RouteFuture<'_> {
    Box::pin(monitor_status_route(cx, body))
}

fn proxy_route_handler(cx: &Cx, body: Body) -> RouteFuture<'_> {
    Box::pin(proxy_route(cx, body))
}

fn build_upstream_client(timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(timeout)
        .read_timeout(timeout)
        .pool_max_idle_per_host(8)
        .build()
        .context("build watcher upstream client")
}

async fn monitor_shell_route(_cx: &Cx, _body: Body) -> TopcoatResult<Response> {
    let _ = _body;
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(
            HeaderName::from_static("content-type"),
            "text/html; charset=utf-8",
        )
        .header(HeaderName::from_static("cache-control"), "no-store")
        .body(Body::from(MONITOR_UI_SHELL))
        .map_err(|error| anyhow!("failed to build monitor shell response: {error}"))?;
    Ok(response)
}

async fn monitor_status_route(_cx: &Cx, body: Body) -> TopcoatResult<Response> {
    let _ = body;
    let state = app_context::<Arc<AppState>>(_cx);
    let permit = acquire_json_transform(&state.json_transforms)
        .await
        .context("acquire json transform permit for ui status")?;
    let url = format!("http://{}/api/v1/status", state.upstream);
    let upstream = state
        .client
        .get(url)
        .send()
        .await
        .context("request upstream status for monitor ui")?;
    if upstream.status() != StatusCode::OK {
        return Ok(Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .header(HeaderName::from_static("content-type"), "application/json")
            .body(Body::from(
                serde_json::json!({
                    "monitor_status": "upstream status endpoint is unavailable",
                    "status_last_fetched": SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|time| time.as_secs())
                        .unwrap_or_default()
                })
                .to_string(),
            ))
            .map_err(|error| anyhow!("failed to build monitor status response: {error}"))?);
    }
    let raw = read_limited(upstream, MAX_JSON_BYTES).await?;
    let transformed = transform_public_json(raw, permit).await?;
    let status_pretty = serde_json::from_slice::<Value>(&transformed)
        .ok()
        .map_or_else(
            || status_pretty_fallback(&transformed),
            |value| {
                serde_json::to_string_pretty(&value)
                    .unwrap_or_else(|_| status_pretty_fallback(&transformed))
            },
        );
    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_secs())
        .unwrap_or_default();
    let payload = serde_json::json!({
        "monitor_status": status_pretty,
        "status_last_fetched": now
    });
    let body = serde_json::to_vec(&payload).context("serialize monitor status payload")?;
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(HeaderName::from_static("content-type"), "application/json")
        .body(Body::from(body))
        .map_err(|error| anyhow!("failed to build monitor status response: {error}"))?;
    Ok(response)
}

fn status_pretty_fallback(transformed: &[u8]) -> String {
    let value = String::from_utf8_lossy(transformed);
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

async fn proxy_route(cx: &Cx, body: Body) -> TopcoatResult<Response> {
    let _request_body = body;
    let state = app_context::<Arc<AppState>>(cx);
    let send_body = matches!(method(cx), &Method::GET);
    if !send_body && !matches!(method(cx), &Method::HEAD) {
        return Ok(status_response(StatusCode::METHOD_NOT_ALLOWED));
    }

    let permit = match state.slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return Ok(status_response(StatusCode::SERVICE_UNAVAILABLE));
        }
    };

    match proxy_request(app_context::<Arc<AppState>>(cx), cx, permit).await {
        Ok(response) => Ok(response),
        Err(error) => {
            eprintln!("watcher gateway: upstream request failed: {error}");
            Ok(public_error(send_body))
        }
    }
}

async fn proxy_request(
    state: &AppState,
    cx: &Cx,
    permit: OwnedSemaphorePermit,
) -> Result<Response> {
    let request_uri = uri(cx);
    let send_body = matches!(method(cx), &Method::GET);
    if request_uri.scheme().is_some()
        || request_uri.authority().is_some()
        || !request_uri.path().starts_with('/')
    {
        return Ok(status_response(StatusCode::BAD_REQUEST));
    }
    let path = request_uri.path();
    if path.starts_with("/api/") && !PUBLIC_API_GETS.contains(&path) {
        return Ok(status_response(StatusCode::NOT_FOUND));
    }

    let is_ingest = path == "/api/v1/sidecars/ingest-pipeline/status.json";
    let target = request_uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or(path);
    let upstream = if is_ingest {
        state.ingest_upstream
    } else {
        state.upstream
    };
    let url = format!("http://{upstream}{target}");
    let method = if send_body {
        reqwest::Method::GET
    } else {
        reqwest::Method::HEAD
    };
    let mut upstream_request = state.client.request(method, url);
    for name in COPY_REQUEST_HEADERS {
        if let Some(value) = headers(cx).get(*name) {
            upstream_request = upstream_request.header(*name, value.clone());
        }
    }
    upstream_request = upstream_request.header(header::ACCEPT_ENCODING, "identity");

    let upstream_response = upstream_request
        .send()
        .await
        .context("request private watcher upstream")?;
    let status = StatusCode::from_u16(upstream_response.status().as_u16())
        .context("convert upstream status")?;
    if status.is_client_error() || status.is_server_error() {
        return Ok(public_error(send_body));
    }

    let upstream_headers = upstream_response.headers().clone();
    let media_type = upstream_headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let is_json = media_type == "application/json"
        && status != StatusCode::NO_CONTENT
        && status != StatusCode::NOT_MODIFIED;

    if is_ingest {
        if status != StatusCode::OK || !is_json {
            return Ok(public_error(send_body));
        }
        let body = if send_body {
            let transform_permit = acquire_json_transform(&state.json_transforms).await?;
            let raw = read_limited(upstream_response, MAX_INGEST_JSON_BYTES).await?;
            transform_ingest_json(raw, transform_permit).await?
        } else {
            Vec::new()
        };
        return Ok(ingest_response(body, send_body, permit));
    }

    let is_api = path.starts_with("/api/");
    let is_sse = path == "/api/v1/events";
    if (is_sse && media_type != "text/event-stream") || (is_api && !is_sse && !is_json) {
        return Ok(public_error(send_body));
    }

    if is_json && send_body {
        let transform_permit = acquire_json_transform(&state.json_transforms).await?;
        let raw = read_limited(upstream_response, MAX_JSON_BYTES).await?;
        let body = transform_public_json(raw, transform_permit).await?;
        return Ok(transformed_json_response(
            status,
            &upstream_headers,
            body,
            permit,
        ));
    }

    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    copy_response_headers(&upstream_headers, response.headers_mut(), is_sse || is_json);
    if is_sse {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        response
            .headers_mut()
            .insert("x-accel-buffering", HeaderValue::from_static("no"));
        response
            .headers_mut()
            .insert(header::CONNECTION, HeaderValue::from_static("close"));
        if send_body {
            *response.body_mut() = Body::new(sanitized_sse_body(
                upstream_response,
                permit,
                Arc::clone(&state.json_transforms),
            ));
        }
        return Ok(response);
    }

    if is_json {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
    }
    if send_body {
        *response.body_mut() = Body::new(StreamBody::new(
            upstream_response.bytes_stream().map_ok(Frame::data),
        ));
    }
    Ok(response)
}

async fn read_limited(response: reqwest::Response, limit: usize) -> Result<bytes::Bytes> {
    if response
        .content_length()
        .is_some_and(|content_length| content_length > limit as u64)
    {
        return Err(anyhow!("upstream body exceeds public limit"));
    }
    let mut stream = response.bytes_stream();
    let mut output = bytes::BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("read watcher upstream body")?;
        ensure!(
            output.len().saturating_add(chunk.len()) <= limit,
            "upstream body exceeds public limit"
        );
        output.extend_from_slice(&chunk);
    }
    Ok(output.freeze())
}

async fn acquire_json_transform(json_transforms: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
    Arc::clone(json_transforms)
        .acquire_owned()
        .await
        .map_err(|_| anyhow!("JSON transform gate closed"))
}

async fn transform_public_json(
    raw: bytes::Bytes,
    transform_permit: OwnedSemaphorePermit,
) -> Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        let _transform_permit = transform_permit;
        public_json_bytes(&raw)
    })
    .await
    .context("join public JSON transform")?
}

async fn transform_ingest_json(
    raw: bytes::Bytes,
    transform_permit: OwnedSemaphorePermit,
) -> Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        let _transform_permit = transform_permit;
        public_ingest_json_bytes(&raw)
    })
    .await
    .context("join ingest-status transform")?
}

fn sanitized_sse_body(
    response: reqwest::Response,
    permit: OwnedSemaphorePermit,
    json_transforms: Arc<Semaphore>,
) -> Body {
    let (sender, receiver) = mpsc::channel::<std::result::Result<bytes::Bytes, std::io::Error>>(8);
    tokio::spawn(async move {
        let _permit = permit;
        let mut upstream = response.bytes_stream();
        let mut pending = bytes::BytesMut::new();
        let mut pending_data_permit = None;
        loop {
            let chunk = tokio::select! {
                () = sender.closed() => return,
                chunk = upstream.next() => chunk,
            };
            let Some(chunk) = chunk else {
                break;
            };
            let Ok(chunk) = chunk else {
                return;
            };
            for slice in chunk.chunks(SSE_UNGATED_CHUNK_BYTES) {
                if pending_data_permit.is_none() && is_data_line_prefix(&pending) {
                    pending_data_permit = Some(tokio::select! {
                        () = sender.closed() => return,
                        permit = acquire_json_transform(&json_transforms) => {
                            let Ok(permit) = permit else { return; };
                            permit
                        }
                    });
                }
                if pending.len().saturating_add(slice.len()) > MAX_SSE_LINE_BYTES {
                    return;
                }
                pending.extend_from_slice(slice);
                while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                    let line = pending.split_to(newline + 1).freeze();
                    let line_permit = if is_data_line_prefix(&line) {
                        pending_data_permit.take()
                    } else {
                        pending_data_permit.take();
                        None
                    };
                    let Some(public) =
                        transform_sse_line(line, line_permit, Arc::clone(&json_transforms)).await
                    else {
                        return;
                    };
                    if !public.is_empty()
                        && sender.send(Ok(bytes::Bytes::from(public))).await.is_err()
                    {
                        return;
                    }
                }
                if pending_data_permit.is_none() && is_data_line_prefix(&pending) {
                    pending_data_permit = Some(tokio::select! {
                        () = sender.closed() => return,
                        permit = acquire_json_transform(&json_transforms) => {
                            let Ok(permit) = permit else { return; };
                            permit
                        }
                    });
                }
            }
        }
        if !pending.is_empty() {
            let Some(public) = transform_sse_line(
                pending.freeze(),
                pending_data_permit.take(),
                Arc::clone(&json_transforms),
            )
            .await
            else {
                return;
            };
            if !public.is_empty() {
                let _ = sender.send(Ok(bytes::Bytes::from(public))).await;
            }
        }
    });
    Body::new(StreamBody::new(
        ReceiverStream::new(receiver).map_ok(Frame::data),
    ))
}

fn is_data_line_prefix(line: &[u8]) -> bool {
    line.starts_with(b"data:") || line == b"data"
}

async fn transform_sse_line(
    line: bytes::Bytes,
    reserved_permit: Option<OwnedSemaphorePermit>,
    json_transforms: Arc<Semaphore>,
) -> Option<Vec<u8>> {
    if !is_data_line_prefix(&line) {
        drop(reserved_permit);
        return public_sse_line(&line);
    }
    let transform_permit = match reserved_permit {
        Some(permit) => permit,
        None => acquire_json_transform(&json_transforms).await.ok()?,
    };
    tokio::task::spawn_blocking(move || {
        let _transform_permit = transform_permit;
        public_sse_line(&line)
    })
    .await
    .ok()?
}

struct PermitBodyState {
    bytes: bytes::Bytes,
    offset: usize,
    _permit: OwnedSemaphorePermit,
}

fn bounded_response_body(bytes: bytes::Bytes, permit: OwnedSemaphorePermit) -> Body {
    let state = PermitBodyState {
        bytes,
        offset: 0,
        _permit: permit,
    };
    let stream = futures_util::stream::unfold(state, |mut state| async move {
        if state.offset >= state.bytes.len() {
            return None;
        }
        let end = state
            .offset
            .saturating_add(RESPONSE_CHUNK_BYTES)
            .min(state.bytes.len());
        let chunk = state.bytes.slice(state.offset..end);
        state.offset = end;
        Some((Ok::<bytes::Bytes, Infallible>(chunk), state))
    });
    Body::new(StreamBody::new(stream.map_ok(Frame::data)))
}

fn transformed_json_response(
    status: StatusCode,
    upstream_headers: &reqwest::header::HeaderMap,
    body: Vec<u8>,
    permit: OwnedSemaphorePermit,
) -> Response {
    let length = body.len();
    let mut response = Response::new(bounded_response_body(bytes::Bytes::from(body), permit));
    *response.status_mut() = status;
    copy_response_headers(upstream_headers, response.headers_mut(), true);
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).expect("valid content length"),
    );
    response
}

fn ingest_response(body: Vec<u8>, send_body: bool, permit: OwnedSemaphorePermit) -> Response {
    let length = if send_body { body.len() } else { 0 };
    let mut response = Response::new(if send_body {
        bounded_response_body(bytes::Bytes::from(body), permit)
    } else {
        drop(permit);
        Body::empty()
    });
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).expect("valid content length"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn public_error(send_body: bool) -> Response {
    let length = if send_body { PUBLIC_ERROR.len() } else { 0 };
    let mut response = Response::new(if send_body {
        Body::from(PUBLIC_ERROR)
    } else {
        Body::empty()
    });
    *response.status_mut() = StatusCode::BAD_GATEWAY;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).expect("valid content length"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn status_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

fn copy_response_headers(
    source: &reqwest::header::HeaderMap,
    target: &mut HeaderMap,
    transformed: bool,
) {
    for (name, value) in source {
        let lower = name.as_str();
        if HOP_BY_HOP_HEADERS.contains(&lower) {
            continue;
        }
        if lower != "content-type" && !SAFE_RESPONSE_HEADERS.contains(&lower) {
            continue;
        }
        if transformed
            && matches!(
                lower,
                "content-encoding" | "content-length" | "etag" | "content-type"
            )
        {
            continue;
        }
        target.append(name.clone(), value.clone());
    }
}

fn network_address(value: &str, flag: &str, allow_private: bool) -> Result<SocketAddr> {
    let address = if let Some(port) = value.strip_prefix("localhost:") {
        let port = port
            .parse::<u16>()
            .with_context(|| format!("{flag} must include an explicit host and port"))?;
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    } else {
        value.parse::<SocketAddr>().with_context(|| {
            format!("{flag} must use localhost or an explicit IPv4 address and port")
        })?
    };
    let IpAddr::V4(ip) = address.ip() else {
        return Err(anyhow!("{flag} must use an explicit IPv4 address"));
    };
    let private = allow_private
        && (ip.octets()[0] == 10
            || (ip.octets()[0] == 172 && (16..=31).contains(&ip.octets()[1]))
            || (ip.octets()[0] == 192 && ip.octets()[1] == 168));
    ensure!(
        !ip.is_unspecified() && (ip.is_loopback() || private),
        "{flag} must use an explicit {} address",
        if allow_private {
            "loopback or private"
        } else {
            "loopback"
        }
    );
    Ok(address)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use tokio::task::JoinHandle;
    use topcoat::context::Cx;
    use topcoat::router::to_bytes;

    #[derive(Clone)]
    struct MockResponse {
        status: StatusCode,
        content_type: &'static str,
        body: bytes::Bytes,
    }

    fn mock_upstream_handler(cx: &Cx, body: Body) -> RouteFuture<'_> {
        Box::pin(mock_upstream(cx, body))
    }

    async fn mock_upstream(cx: &Cx, body: Body) -> TopcoatResult<Response> {
        let mock = app_context::<MockResponse>(cx);
        let mut response = Response::new(if matches!(method(cx), &Method::GET) {
            // HEAD calls for the same path must stay bodyless.
            drop(body);
            Body::from(mock.body.clone())
        } else {
            drop(body);
            Body::empty()
        });
        *response.status_mut() = mock.status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(mock.content_type),
        );
        response.headers_mut().insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&mock.body.len().to_string()).unwrap(),
        );
        response.headers_mut().insert(
            HeaderName::from_static("x-internal-diagnostic"),
            HeaderValue::from_static("must-not-leak"),
        );
        Ok(response)
    }

    async fn mock_router(mock: MockResponse) -> (SocketAddr, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::builder()
            .route(RouteFn::new(
                &[Method::GET, Method::HEAD],
                Cow::Borrowed(Path::new("/{*path}")),
                mock_upstream_handler,
            ))
            .app_context(mock)
            .build();
        let task = tokio::spawn(async move {
            topcoat::serve(listener, app).await.unwrap();
        });
        (address, task)
    }

    async fn spawn_gateway(
        upstream: SocketAddr,
        ingest: SocketAddr,
    ) -> (SocketAddr, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(AppState {
            client: build_upstream_client(Duration::from_secs(2)).unwrap(),
            upstream,
            ingest_upstream: ingest,
            slots: Arc::new(Semaphore::new(64)),
            json_transforms: Arc::new(Semaphore::new(2)),
        });
        let router = Router::builder()
            .route(RouteFn::new(
                &[Method::GET],
                Cow::Borrowed(Path::new("/ui/status")),
                monitor_status_route_handler,
            ))
            .route(RouteFn::new(
                &[Method::GET, Method::HEAD],
                Cow::Borrowed(Path::new("/api/{*path}")),
                proxy_route_handler,
            ))
            .route(RouteFn::new(
                &[Method::GET],
                Cow::Borrowed(Path::new("/{*path}")),
                monitor_shell_route_handler,
            ))
            .app_context(state)
            .build();
        let task = tokio::spawn(async move {
            topcoat::serve(listener, router).await.unwrap();
        });
        (address, task)
    }

    #[test]
    fn address_scope_is_fail_closed() {
        assert_eq!(
            network_address("127.0.0.1:8787", "--listen", true).unwrap(),
            "127.0.0.1:8787".parse().unwrap()
        );
        assert_eq!(
            network_address("192.168.1.10:8787", "--listen", true).unwrap(),
            "192.168.1.10:8787".parse().unwrap()
        );
        assert!(network_address("0.0.0.0:8787", "--listen", true).is_err());
        assert!(network_address("192.168.1.10:8786", "--upstream", false).is_err());
        assert!(network_address("203.0.113.10:8787", "--listen", true).is_err());
        assert!(network_address("[::1]:8787", "--listen", true).is_err());
    }

    #[test]
    fn public_api_allowlist_is_explicit() {
        assert!(PUBLIC_API_GETS.contains(&"/api/v1/status"));
        assert!(PUBLIC_API_GETS.contains(&"/api/v1/events"));
        assert!(PUBLIC_API_GETS.contains(&"/api/v1/sidecars/block-time-gaps/index.json"));
        assert!(PUBLIC_API_GETS.contains(&"/api/v1/sidecars/ingest-pipeline/status.json"));
        assert!(PUBLIC_API_GETS.contains(&"/api/v1/sidecars/runtime-operations/status.json"));
    }

    #[test]
    fn upstream_client_is_direct_and_has_idle_not_total_timeout() {
        let client = build_upstream_client(Duration::from_secs(7)).unwrap();
        let debug = format!("{client:?}");
        assert!(
            !debug.contains("proxies"),
            "system proxy is still active: {debug}"
        );
        assert!(debug.contains("read_timeout: 7s"), "{debug}");
        assert!(!debug.contains("total_timeout"), "{debug}");
    }

    #[tokio::test]
    async fn response_permit_lives_until_buffered_body_finishes_or_is_dropped() {
        let slots = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&slots).acquire_owned().await.unwrap();
        let body = bounded_response_body(bytes::Bytes::from_static(b"public response"), permit);
        assert_eq!(slots.available_permits(), 0);

        let collected = to_bytes(body, 1024).await.unwrap();
        assert_eq!(collected.as_ref(), b"public response");
        assert_eq!(slots.available_permits(), 1);

        let permit = Arc::clone(&slots).acquire_owned().await.unwrap();
        let body = bounded_response_body(bytes::Bytes::from_static(b"abandoned"), permit);
        assert_eq!(slots.available_permits(), 0);
        drop(body);
        assert_eq!(slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn json_transform_gate_bounds_work_before_body_buffering() {
        let transforms = Arc::new(Semaphore::new(1));
        let held = acquire_json_transform(&transforms).await.unwrap();
        let waiting_transforms = Arc::clone(&transforms);
        let waiter = tokio::spawn(async move { acquire_json_transform(&waiting_transforms).await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());
        drop(held);
        let acquired = waiter.await.unwrap().unwrap();
        assert_eq!(transforms.available_permits(), 0);
        drop(acquired);
        assert_eq!(transforms.available_permits(), 1);
    }

    fn slow_sse_handler(cx: &Cx, body: Body) -> RouteFuture<'_> {
        Box::pin(slow_sse(cx, body))
    }

    async fn slow_sse(_cx: &Cx, body: Body) -> TopcoatResult<Response> {
        drop(body);
        let stream = futures_util::stream::unfold(0_u8, |index| async move {
            if index == 10 {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            Some((
                Ok::<bytes::Bytes, Infallible>(bytes::Bytes::from_static(b": heartbeat\n\n")),
                index + 1,
            ))
        });
        let mut response = Response::new(Body::new(StreamBody::new(stream.map_ok(Frame::data))));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        Ok(response)
    }

    #[tokio::test]
    async fn read_timeout_resets_for_a_healthy_long_lived_sse_stream() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let app = Router::builder()
                .route(RouteFn::new(
                    &[Method::GET, Method::HEAD],
                    Cow::Borrowed(Path::new("/{*path}")),
                    slow_sse_handler,
                ))
                .build();
            topcoat::serve(listener, app).await.unwrap();
        });

        let client = build_upstream_client(Duration::from_millis(100)).unwrap();
        let started = tokio::time::Instant::now();
        let response = client
            .get(format!("http://{address}/api/v1/events"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(response.as_ref(), b": heartbeat\n\n".repeat(10));
        assert!(started.elapsed() >= Duration::from_millis(200));
        upstream_task.abort();
    }

    #[tokio::test]
    async fn generic_json_is_sanitized_and_private_headers_are_dropped() {
        let (upstream, upstream_task) = mock_router(MockResponse {
            status: StatusCode::OK,
            content_type: "application/json",
            body: bytes::Bytes::from_static(
                br#"{"token":"TOPSECRET","path":"/tmp/private/file","ok":true}"#,
            ),
        })
        .await;
        let (gateway, gateway_task) = spawn_gateway(upstream, upstream).await;
        let response = reqwest::get(format!("http://{gateway}/api/v1/status"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("x-internal-diagnostic").is_none());
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/json; charset=utf-8"
        );
        let public: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(public["ok"], true);
        assert_eq!(public["path"], "file");
        assert!(public.get("token").is_none());
        gateway_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn public_api_content_mismatch_and_upstream_errors_fail_closed() {
        for mock in [
            MockResponse {
                status: StatusCode::OK,
                content_type: "text/plain",
                body: bytes::Bytes::from_static(b"private diagnostic"),
            },
            MockResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                content_type: "application/json",
                body: bytes::Bytes::from_static(br#"{"token":"TOPSECRET"}"#),
            },
        ] {
            let (upstream, upstream_task) = mock_router(mock).await;
            let (gateway, gateway_task) = spawn_gateway(upstream, upstream).await;
            let response = reqwest::get(format!("http://{gateway}/api/v1/status"))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            assert!(response.headers().get("x-internal-diagnostic").is_none());
            assert_eq!(response.bytes().await.unwrap().as_ref(), PUBLIC_ERROR);
            gateway_task.abort();
            upstream_task.abort();
        }
    }

    #[tokio::test]
    async fn head_upstream_failure_is_bodyless() {
        let (upstream, upstream_task) = mock_router(MockResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: "application/json",
            body: bytes::Bytes::from_static(br#"{"private":"diagnostic"}"#),
        })
        .await;
        let (gateway, gateway_task) = spawn_gateway(upstream, upstream).await;
        let response = reqwest::Client::new()
            .head(format!("http://{gateway}/api/v1/status"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(response.content_length(), Some(0));
        assert!(response.bytes().await.unwrap().is_empty());
        gateway_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn ingest_uses_dedicated_upstream_and_projects_contract() {
        let (upstream, upstream_task) = mock_router(MockResponse {
            status: StatusCode::OK,
            content_type: "application/json",
            body: bytes::Bytes::from_static(br#"{"wrong":true}"#),
        })
        .await;
        let ingest_upstream = json!({
            "schema_version": 1,
            "updated_unix_secs": 100,
            "overall_state": "healthy",
            "upstream": {"state": "connected", "updated_unix_secs": 100, "reconnects_1h": null, "token": "private"},
            "recorder": {"state": "recording", "durable_slot": 1000, "updated_unix_secs": 100, "active_bytes": 1, "sealed_generations": 0, "unacknowledged_bytes": 0, "disk_free_bytes": 2, "disk_total_bytes": 4},
            "replication": {"state": "caught_up", "ack_through_sequence": 1000, "ack_slot": 1000, "updated_unix_secs": 100, "lag_records": 0},
            "indexer": {"state": "unavailable", "last_slot": null, "updated_unix_secs": null, "lag_slots": null},
            "object_store": {"state": "unavailable", "provider": "r2", "committed_bytes": null, "pending_bytes": 0, "updated_unix_secs": null},
            "fallback": {"state": "unavailable", "last_slot": null, "updated_unix_secs": null, "lag_slots": null},
            "gaps": [], "gaps_truncated": false, "incidents": [], "secret": "private"
        });
        let (ingest_upstream_address, ingest_task) = mock_router(MockResponse {
            status: StatusCode::OK,
            content_type: "application/json; charset=utf-8",
            body: bytes::Bytes::from(serde_json::to_vec(&ingest_upstream).unwrap()),
        })
        .await;
        let (gateway, gateway_task) = spawn_gateway(upstream, ingest_upstream_address).await;
        let response = reqwest::get(format!(
            "http://{gateway}/api/v1/sidecars/ingest-pipeline/status.json"
        ))
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let body = response.bytes().await.unwrap();
        let public: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(public["schema_version"], 1);
        assert!(public.get("secret").is_none());
        assert!(public["upstream"].get("token").is_none());
        gateway_task.abort();
        upstream_task.abort();
        ingest_task.abort();
    }

    #[tokio::test]
    async fn monitor_status_endpoint_redacts_and_prettifies_status_payload() {
        let (upstream, upstream_task) = mock_router(MockResponse {
            status: StatusCode::OK,
            content_type: "application/json",
            body: bytes::Bytes::from_static(
                br#"{"token":"TOPSECRET","path":"/tmp/private/path","message":"ok"}"#,
            ),
        })
        .await;
        let (gateway, gateway_task) = spawn_gateway(upstream, upstream).await;
        let status = reqwest::get(format!("http://{gateway}/ui/status"))
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let payload: Value = serde_json::from_slice(&status.bytes().await.unwrap()).unwrap();
        let monitor_status = payload["monitor_status"].as_str().unwrap();
        assert!(monitor_status.contains("ok"));
        assert!(!monitor_status.contains("TOPSECRET"));
        assert!(!monitor_status.contains("/tmp/private/path"));
        assert!(payload["status_last_fetched"].as_u64().is_some());
        gateway_task.abort();
        upstream_task.abort();
    }

    #[tokio::test]
    async fn sse_is_sanitized_and_unknown_api_or_mutation_never_reaches_upstream() {
        let (upstream, upstream_task) = mock_router(MockResponse {
            status: StatusCode::OK,
            content_type: "Text/Event-Stream; Charset=UTF-8",
            body: bytes::Bytes::from_static(
                b"event: snapshot_patch\ndata: {\"message\":\"Bearer TOPSECRET\",\"path\":\"/tmp/private\"}\n\n",
            ),
        })
        .await;
        let (gateway, gateway_task) = spawn_gateway(upstream, upstream).await;
        let response = reqwest::get(format!("http://{gateway}/api/v1/events"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.bytes().await.unwrap();
        assert!(
            body.windows(b"event:snapshot_patch".len())
                .any(|item| item == b"event:snapshot_patch")
        );
        assert!(
            body.windows(b"Bearer <redacted>".len())
                .any(|item| item == b"Bearer <redacted>")
        );
        assert!(
            !body
                .windows(b"TOPSECRET".len())
                .any(|item| item == b"TOPSECRET")
        );
        assert!(
            !body
                .windows(b"/tmp/private".len())
                .any(|item| item == b"/tmp/private")
        );

        let client = reqwest::Client::new();
        let unknown = client
            .get(format!("http://{gateway}/api/v1/private"))
            .send()
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
        let mutation = client
            .post(format!("http://{gateway}/api/v1/status"))
            .send()
            .await
            .unwrap();
        assert_eq!(mutation.status(), StatusCode::METHOD_NOT_ALLOWED);
        gateway_task.abort();
        upstream_task.abort();
    }
}
