use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use clap::Parser;
use futures_util::TryStreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use serde::Serialize;
use serde_json::Value;
use std::{
    borrow::Cow,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::SystemTime,
};
use tokio::time::timeout;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Duration,
};
use topcoat::Result as TopcoatResult;
use topcoat::context::{Cx, app_context};
use topcoat::router::{
    Body, HeaderMap, HeaderName, HeaderValue, Method, Path, Response, RouteFn, RouteFuture, Router,
    StatusCode, header, headers, method, uri,
};

const PUBLIC_API_GETS: &[&str] = &[
    "/api/v1/events",
    "/api/v1/sidecars/block-time-gaps/index.json",
    "/api/v1/sidecars/ingest-pipeline/status.json",
    "/api/v1/sidecars/runtime-operations/status.json",
    "/api/v1/sidecars/shred-ingest/status.json",
    "/api/v1/status",
];

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

const MONITOR_UI_SHELL: &str = r#"<!doctype html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Blockzilla Monitor</title>
    <script type="module" src="https://cdn.jsdelivr.net/gh/starfederation/datastar@1.0.2/bundles/datastar.js"></script>
    <style>
      :root { color-scheme: dark; font-family: system-ui, sans-serif; }
      body { margin: 0; padding: 24px; }
      pre { background: #101010; border-radius: 8px; padding: 12px; overflow: auto; white-space: pre-wrap; }
      button { margin-right: 8px; padding: 6px 10px; }
      .grid { display: grid; gap: 12px; }
      .row { display: flex; gap: 8px; align-items: center; }
      .subtitle { color: #c0c0c0; }
      .progress-card { border: 1px solid #2d2d2d; border-radius: 8px; padding: 12px; }
    </style>
  </head>
  <body
    data-signals='{"monitor_status":"loading status...","status_last_fetched":"never","stream_connected":"off","event_count":0,"monitor_last_event":"none","monitor_progress_label":"warming","monitor_progress_pct":0}'
  >
    <h1>Blockzilla Monitor (Rust)</h1>
    <p class="subtitle">Single binary for monitor proxy + Topcoat page.</p>
    <div class="grid">
      <div class="row">
        <button onclick="window.location.href='/api/v1/status'">Status JSON</button>
        <button onclick="window.location.href='/api/v1/events'">SSE</button>
        <button id="refresh" type="button">Refresh status</button>
      </div>
      <section>
        <p>Last fetched: <span id="status-fetched" data-text="$status_last_fetched"></span></p>
        <pre id="status" data-text="$monitor_status"></pre>
      </section>
      <section class="progress-card">
        <strong>Archive estimate</strong>
        <div><span data-text="$monitor_progress_label"></span> · <span data-text="$monitor_progress_pct"></span>%</div>
      </section>
      <section>
        <strong>Live events</strong>
        <div>stream: <span data-text="$stream_connected"></span> · last: <span data-text="$monitor_last_event"></span> · total: <span data-text="$event_count"></span></div>
        <div id="events"></div>
      </section>
    </div>
    <script>
      const statusField = document.getElementById('status');
      const fetched = document.getElementById('status-fetched');
      const refresh = document.getElementById('refresh');
      const events = document.getElementById('events');

      async function refreshStatus() {
        try {
          const response = await fetch('/ui/status', {cache: 'no-store'});
          if (!response.ok) return;
          const payload = await response.json();
          const now = Number(payload.status_last_fetched) || 0;
          statusField.textContent = payload.monitor_status || JSON.stringify(payload, null, 2);
          if (fetched) {
            if (now > 0) {
              fetched.textContent = new Date(now * 1000).toLocaleString();
            } else {
              fetched.textContent = 'never';
            }
          }
        } catch {
          statusField.textContent = 'failed to refresh /ui/status';
        }
      }

      function renderEventsText(raw) {
        if (!events) return;
        const lines = raw
          .split('\n')
          .map((line) => line.trim())
          .filter(Boolean)
          .slice(0, 40);
        events.textContent = lines.length ? lines.join('\n') : '(waiting for stream)';
      }

      refreshStatus();
      window.setInterval(refreshStatus, 7000);
      refresh?.addEventListener('click', () => void refreshStatus());

      const source = new EventSource('/api/v1/events');
      source.onopen = () => {
        window.datastar && window.datastar.sendAction('event:datastar-patch-signals:data: "stream_connected": "connected"');
      };
      source.onerror = () => {
        renderEventsText('stream error');
      };
      let lastCount = 0;
      source.addEventListener('message', (event) => {
        const raw = event.data || '';
        lastCount += 1;
        if (raw.length > 0) {
          renderEventsText(`${raw}\\n--\\n${events.textContent || ''}`);
        } else {
          renderEventsText('event received');
        }
      });
    </script>
  </body>
</html>
"#;

#[derive(Debug, Parser)]
#[command(name = "blockzilla-monitor")]
#[command(about = "Standalone blockzilla monitor endpoint (Topcoat shell + proxy)")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:8890")]
    listen: String,

    #[arg(long, default_value = "127.0.0.1:8786")]
    upstream: String,

    #[arg(long, default_value = "127.0.0.1:8790")]
    ingest_upstream: String,

    #[arg(long, default_value_t = 60.0)]
    upstream_timeout_secs: f64,

    #[arg(long, default_value_t = 64)]
    max_requests: usize,

    #[arg(long, default_value_t = 2)]
    max_json_transforms: usize,
}

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    upstream: SocketAddr,
    ingest_upstream: SocketAddr,
    slots: Arc<Semaphore>,
}

#[derive(Debug, Serialize)]
struct MonitorCalendarCell {
    epoch: u64,
    start_unix_secs: u64,
    precision: String,
    state: String,
}

#[derive(Debug, Serialize)]
struct MonitorStatusPayload {
    monitor_status: String,
    status_last_fetched: u64,
    monitor_progress_pct: f64,
    monitor_progress_label: String,
    monitor_progress_detail: String,
    monitor_progress_done: u64,
    monitor_progress_total: u64,
    monitor_calendar_cells: Vec<MonitorCalendarCell>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(run(cli))
}

async fn run(cli: Cli) -> Result<()> {
    if !cli.upstream_timeout_secs.is_finite() || cli.upstream_timeout_secs <= 0.0 {
        return Err(anyhow!("--upstream-timeout-secs must be positive"));
    }
    if cli.max_requests == 0 {
        return Err(anyhow!("--max-requests must be positive"));
    }
    if cli.max_json_transforms == 0 {
        return Err(anyhow!("--max-json-transforms must be positive"));
    }

    let listen = network_address(&cli.listen, true)?;
    let upstream = network_address(&cli.upstream, false)?;
    let ingest_upstream = network_address(&cli.ingest_upstream, false)?;
    ensure_network_distinct(listen, upstream, ingest_upstream)?;

    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs_f64(cli.upstream_timeout_secs))
        .read_timeout(Duration::from_secs_f64(cli.upstream_timeout_secs))
        .pool_max_idle_per_host(8)
        .build()?;

    let state = Arc::new(AppState {
        client,
        upstream,
        ingest_upstream,
        slots: Arc::new(Semaphore::new(
            cli.max_requests.max(cli.max_json_transforms),
        )),
    });

    let router = build_router(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind monitor service on {listen}"))?;
    topcoat::serve(listener, router)
        .await
        .context("serve blockzilla monitor")
}

fn build_router(state: Arc<AppState>) -> Router {
    Router::builder()
        .route(RouteFn::new(
            &[Method::GET],
            Cow::Borrowed(Path::new("/")),
            monitor_shell_route_handler,
        ))
        .route(RouteFn::new(
            &[Method::GET],
            Cow::Borrowed(Path::new("/ui/status")),
            monitor_status_route_handler,
        ))
        .route(RouteFn::new(
            &[Method::GET],
            Cow::Borrowed(Path::new("/ui/stream")),
            monitor_stream_route_handler,
        ))
        .route(RouteFn::new(
            &[Method::GET, Method::HEAD],
            Cow::Borrowed(Path::new("/{*path}")),
            proxy_route_handler,
        ))
        .app_context(state)
        .build()
}

fn monitor_shell_route_handler(cx: &Cx, body: Body) -> RouteFuture<'_> {
    Box::pin(monitor_shell_route(cx, body))
}

fn monitor_status_route_handler(cx: &Cx, body: Body) -> RouteFuture<'_> {
    Box::pin(monitor_status_route(cx, body))
}

fn monitor_stream_route_handler(cx: &Cx, body: Body) -> RouteFuture<'_> {
    Box::pin(monitor_stream_route(cx, body))
}

fn proxy_route_handler(cx: &Cx, body: Body) -> RouteFuture<'_> {
    Box::pin(proxy_route(cx, body))
}

async fn monitor_shell_route(_cx: &Cx, _body: Body) -> TopcoatResult<Response> {
    let _ = _body;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(
            HeaderName::from_static("content-type"),
            "text/html; charset=utf-8",
        )
        .header(HeaderName::from_static("cache-control"), "no-store")
        .body(Body::from(MONITOR_UI_SHELL))
        .expect("build monitor shell response"))
}

async fn monitor_status_route(cx: &Cx, body: Body) -> TopcoatResult<Response> {
    let _ = body;
    let state = app_context::<Arc<AppState>>(cx);
    let permit = acquire_slot(&state).await?;

    let url = format!("http://{}/api/v1/status", state.upstream);
    let upstream = match state.client.get(&url).send().await {
        Ok(response) => response,
        Err(error) => {
            drop(permit);
            return Ok(status_json(
                StatusCode::BAD_GATEWAY,
                &build_status_payload(format!("upstream status request failed: {error}"), 0),
            ));
        }
    };

    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_secs())
        .unwrap_or_default();

    if upstream.status() != StatusCode::OK {
        drop(permit);
        return Ok(status_json(
            StatusCode::BAD_GATEWAY,
            &build_status_payload(
                format!("upstream status returned {}", upstream.status()),
                now,
            ),
        ));
    }

    let raw = upstream.bytes().await.unwrap_or_else(|_| Bytes::new());
    let status = serde_json::from_slice::<Value>(&raw).ok();
    drop(permit);

    let pretty = status
        .as_ref()
        .and_then(|value| serde_json::to_string_pretty(value).ok())
        .unwrap_or_else(|| String::from_utf8_lossy(&raw).to_string());

    let payload = build_status_payload(pretty, now).with_projection(status.as_ref());
    Ok(status_json(StatusCode::OK, &payload))
}

async fn monitor_stream_route(cx: &Cx, body: Body) -> TopcoatResult<Response> {
    let _ = body;
    let state = app_context::<Arc<AppState>>(cx);
    let _permit = match acquire_slot(&state).await {
        Ok(permit) => permit,
        Err(_) => return Ok(status_response(StatusCode::SERVICE_UNAVAILABLE)),
    };

    let url = format!("http://{}/api/v1/events", state.upstream);
    let upstream = state.client.get(&url).send().await;
    match upstream {
        Ok(upstream) => {
            if upstream.status() != StatusCode::OK {
                return Ok(status_response(StatusCode::BAD_GATEWAY));
            }
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::OK;
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
            if matches!(*method(cx), Method::GET) {
                *response.body_mut() =
                    Body::new(StreamBody::new(upstream.bytes_stream().map_ok(Frame::data)));
            }
            drop(_permit);
            Ok(response)
        }
        Err(_) => Ok(status_response(StatusCode::BAD_GATEWAY)),
    }
}

async fn proxy_route(cx: &Cx, body: Body) -> TopcoatResult<Response> {
    let _ = body;
    let state = app_context::<Arc<AppState>>(cx);
    let permit = acquire_slot(&state).await?;

    let request_uri = uri(cx);
    let request_method = method(cx);
    let send_body = matches!(request_method, &Method::GET);
    if request_uri.scheme().is_some()
        || request_uri.authority().is_some()
        || !request_uri.path().starts_with('/')
    {
        drop(permit);
        return Ok(status_response(StatusCode::BAD_REQUEST));
    }

    let path = request_uri.path();
    if path == "/" || path == "" || !path.starts_with('/') {
        drop(permit);
        return Ok(monitor_shell_response());
    }
    if path.starts_with("/api/") && !PUBLIC_API_GETS.contains(&path) {
        drop(permit);
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

    let mut upstream_request = match *request_method {
        Method::HEAD => state.client.head(&url),
        _ => state.client.get(&url),
    };
    for name in COPY_REQUEST_HEADERS {
        if let Some(value) = headers(cx).get(*name) {
            upstream_request = upstream_request.header(*name, value.clone());
        }
    }
    upstream_request = upstream_request.header(header::ACCEPT_ENCODING, "identity");

    let upstream_response =
        match timeout(Duration::from_secs_f64(30.0), upstream_request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                drop(permit);
                return Ok(public_error(send_body));
            }
            Err(_) => {
                drop(permit);
                return Ok(status_response(StatusCode::GATEWAY_TIMEOUT));
            }
        };

    let status =
        StatusCode::from_u16(upstream_response.status().as_u16()).expect("valid upstream status");

    let upstream_headers = upstream_response.headers().clone();
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    copy_headers(
        &upstream_headers,
        response.headers_mut(),
        path == "/api/v1/events",
    );

    if path == "/api/v1/events" {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
    }
    if matches!(*method(cx), Method::GET) {
        *response.body_mut() = Body::new(StreamBody::new(
            upstream_response.bytes_stream().map_ok(Frame::data),
        ));
    }
    drop(permit);
    Ok(response)
}

fn build_status_payload(message: String, now: u64) -> MonitorStatusPayload {
    MonitorStatusPayload {
        monitor_status: message,
        status_last_fetched: now,
        monitor_progress_pct: 0.0,
        monitor_progress_label: String::from("No progress estimate"),
        monitor_progress_detail: String::from("unavailable"),
        monitor_progress_done: 0,
        monitor_progress_total: 0,
        monitor_calendar_cells: Vec::new(),
    }
}

impl MonitorStatusPayload {
    fn with_projection(mut self, raw: Option<&Value>) -> Self {
        if let Some(raw) = raw {
            if let Some(progress_pct) = raw
                .get("summary")
                .and_then(|s| s.get("progress_pct"))
                .and_then(Value::as_f64)
            {
                self.monitor_progress_pct = progress_pct;
                self.monitor_progress_label = String::from("Status progress");
                self.monitor_progress_detail = "from /summary".into();
            }
            if let (Some(done), Some(total)) = (raw.get("epochs_done"), raw.get("epochs_total")) {
                self.monitor_progress_done = done.as_u64().unwrap_or(self.monitor_progress_done);
                self.monitor_progress_total = total.as_u64().unwrap_or(self.monitor_progress_total);
                if self.monitor_progress_total > 0 {
                    self.monitor_progress_pct = (self.monitor_progress_done as f64) * 100.0
                        / self.monitor_progress_total as f64;
                    self.monitor_progress_label = String::from("Backfill epochs");
                    self.monitor_progress_detail = format!(
                        "{}/{} epochs",
                        self.monitor_progress_done, self.monitor_progress_total
                    );
                }
            }
            if let Some(calendar) = raw.get("epoch_calendar").and_then(|v| v.as_array()) {
                self.monitor_calendar_cells = calendar
                    .iter()
                    .filter_map(|entry| {
                        let entry = entry.as_object()?;
                        let epoch = entry.get("epoch")?.as_u64()?;
                        let start_unix_secs = entry.get("start_unix_secs")?.as_u64()?;
                        let precision = entry
                            .get("precision")
                            .and_then(Value::as_str)
                            .unwrap_or("observed")
                            .to_string();
                        Some(MonitorCalendarCell {
                            epoch,
                            start_unix_secs,
                            precision,
                            state: String::from("tracked"),
                        })
                    })
                    .collect();
            }
            self.monitor_status = serde_json::to_string_pretty(raw)
                .unwrap_or_else(|_| String::from("{\"status\":\"ok\"}"));
        }
        self
    }
}

fn status_json(status: StatusCode, payload: &MonitorStatusPayload) -> Response {
    let body = serde_json::to_vec(payload).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(status)
        .header(
            HeaderName::from_static("content-type"),
            "application/json; charset=utf-8",
        )
        .header(HeaderName::from_static("cache-control"), "no-store")
        .body(Body::from(body))
        .expect("build status payload")
}

fn public_error(include_body: bool) -> Response {
    if include_body {
        Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .header(HeaderName::from_static("content-type"), "application/json")
            .body(Body::from(
                r#"{\"error\":\"watcher upstream unavailable\"}"#,
            ))
            .expect("build upstream unavailable")
    } else {
        status_response(StatusCode::BAD_GATEWAY)
    }
}

fn monitor_shell_response() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(
            HeaderName::from_static("content-type"),
            "text/html; charset=utf-8",
        )
        .body(Body::from(MONITOR_UI_SHELL))
        .expect("build monitor shell response")
}

fn status_response(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .body(Body::empty())
        .expect("build status-only response")
}

fn copy_headers(input: &HeaderMap, output: &mut HeaderMap, include_length: bool) {
    let mut includes_length = false;
    for (name, value) in input {
        let name = name.as_str();
        if SAFE_RESPONSE_HEADERS.contains(&name) {
            if name == "content-length" {
                includes_length = true;
            }
            if let Ok(name) = HeaderName::from_bytes(name.as_bytes()) {
                output.insert(name, value.clone());
            }
        }
    }
    if include_length && !includes_length {
        output.insert(
            HeaderName::from_static("content-length"),
            HeaderValue::from_static("0"),
        );
    }
}

fn network_address(raw: &str, allow_private: bool) -> Result<SocketAddr> {
    let address = raw
        .parse::<SocketAddr>()
        .with_context(|| format!("unable to parse {raw}"))?;
    match address.ip() {
        IpAddr::V4(ip) => {
            if allow_private {
                if !(ip.is_private() || ip.is_loopback()) {
                    return Err(anyhow!("{raw} must be loopback or private IPv4"));
                }
            } else if !ip.is_loopback() {
                return Err(anyhow!("{raw} must be loopback IPv4"));
            }
        }
        IpAddr::V6(ip) => {
            if allow_private {
                if !ip.is_loopback() {
                    return Err(anyhow!("{raw} must be loopback IPv6"));
                }
            } else if !ip.is_loopback() {
                return Err(anyhow!("{raw} must be loopback IPv6"));
            }
        }
    }
    Ok(address)
}

fn ensure_network_distinct(
    listen: SocketAddr,
    upstream: SocketAddr,
    ingest: SocketAddr,
) -> Result<()> {
    if listen == upstream {
        return Err(anyhow!("--listen must differ from --upstream"));
    }
    if listen == ingest {
        return Err(anyhow!("--listen must differ from --ingest-upstream"));
    }
    if upstream == ingest {
        return Err(anyhow!("--upstream and --ingest-upstream must differ"));
    }
    Ok(())
}

async fn acquire_slot(state: &AppState) -> Result<OwnedSemaphorePermit> {
    state
        .slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| anyhow!("request gate closed"))
}
