use crate::{
    ingest_contract::{MAX_INGEST_JSON_BYTES, public_ingest_json_bytes},
    public_json::{MAX_JSON_BYTES, public_json_bytes},
    sse::{MAX_SSE_LINE_BYTES, public_sse_line},
};
use anyhow::{Context, Result, anyhow, ensure};
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header},
    response::Response,
};
use bytes::{Bytes, BytesMut};
use clap::Args;
use futures_util::StreamExt;
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;

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
    "/api/v1/sidecars/shred-ingest/status.json",
    "/api/v1/sidecars/runtime-operations/status.json",
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

#[derive(Debug, Clone, Args)]
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
    let app = Router::new().fallback(proxy_handler).with_state(state);
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind public watcher on {listen}"))?;
    axum::serve(listener, app)
        .await
        .context("serve public watcher gateway")
}

fn build_upstream_client(timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        // A loopback trust boundary must never honor HTTP_PROXY inherited from
        // a login shell or the systemd user manager.
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        // Match Python's socket timeout: bound connection setup and each idle
        // read, but do not impose a total lifetime on healthy SSE streams.
        .connect_timeout(timeout)
        .read_timeout(timeout)
        .pool_max_idle_per_host(8)
        .build()
        .context("build watcher upstream client")
}

async fn proxy_handler(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
) -> Response<Body> {
    let send_body = request.method() == Method::GET;
    let Ok(permit) = Arc::clone(&state.slots).try_acquire_owned() else {
        return status_response(StatusCode::SERVICE_UNAVAILABLE);
    };
    match proxy_request(&state, request, permit).await {
        Ok(response) => response,
        Err(_) => {
            eprintln!("watcher gateway: upstream request failed");
            public_error(send_body)
        }
    }
}

async fn proxy_request(
    state: &AppState,
    request: Request<Body>,
    permit: OwnedSemaphorePermit,
) -> Result<Response<Body>> {
    let send_body = request.method() == Method::GET;
    if !send_body && request.method() != Method::HEAD {
        return Ok(status_response(StatusCode::METHOD_NOT_ALLOWED));
    }
    if request.uri().scheme().is_some()
        || request.uri().authority().is_some()
        || !request.uri().path().starts_with('/')
    {
        return Ok(status_response(StatusCode::BAD_REQUEST));
    }
    let path = request.uri().path();
    if path.starts_with("/api/") && !PUBLIC_API_GETS.contains(&path) {
        return Ok(status_response(StatusCode::NOT_FOUND));
    }

    let is_ingest = path == "/api/v1/sidecars/ingest-pipeline/status.json";
    let target = request
        .uri()
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
        if let Some(value) = request.headers().get(*name) {
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
            *response.body_mut() = sanitized_sse_body(
                upstream_response,
                permit,
                Arc::clone(&state.json_transforms),
            );
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
        let stream = upstream_response.bytes_stream().map(move |item| {
            let _keep_permit = &permit;
            item
        });
        *response.body_mut() = Body::from_stream(stream);
    }
    Ok(response)
}

async fn read_limited(response: reqwest::Response, limit: usize) -> Result<Bytes> {
    if response
        .content_length()
        .is_some_and(|content_length| content_length > limit as u64)
    {
        return Err(anyhow!("upstream body exceeds public limit"));
    }
    let mut stream = response.bytes_stream();
    let mut output = BytesMut::new();
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
    raw: Bytes,
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
    raw: Bytes,
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
    let (sender, receiver) = mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(8);
    tokio::spawn(async move {
        let _permit = permit;
        let mut upstream = response.bytes_stream();
        let mut pending = BytesMut::new();
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
                        // A permit is only reserved for a line that began with
                        // `data`; drop it if a malformed prefix changes shape.
                        pending_data_permit.take();
                        None
                    };
                    let Some(public) =
                        transform_sse_line(line, line_permit, Arc::clone(&json_transforms)).await
                    else {
                        return;
                    };
                    if !public.is_empty() && sender.send(Ok(Bytes::from(public))).await.is_err() {
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
                let _ = sender.send(Ok(Bytes::from(public))).await;
            }
        }
    });
    Body::from_stream(ReceiverStream::new(receiver))
}

fn is_data_line_prefix(line: &[u8]) -> bool {
    line.starts_with(b"data:") || line == b"data"
}

async fn transform_sse_line(
    line: Bytes,
    reserved_permit: Option<OwnedSemaphorePermit>,
    json_transforms: Arc<Semaphore>,
) -> Option<Vec<u8>> {
    if !is_data_line_prefix(&line) {
        drop(reserved_permit);
        return public_sse_line(&line);
    }
    let transform_permit = match reserved_permit {
        Some(permit) => permit,
        None => json_transforms.acquire_owned().await.ok()?,
    };
    tokio::task::spawn_blocking(move || {
        let _transform_permit = transform_permit;
        public_sse_line(&line)
    })
    .await
    .ok()?
}

struct PermitBodyState {
    bytes: Bytes,
    offset: usize,
    _permit: OwnedSemaphorePermit,
}

fn bounded_response_body(bytes: Bytes, permit: OwnedSemaphorePermit) -> Body {
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
        Some((Ok::<Bytes, Infallible>(chunk), state))
    });
    Body::from_stream(stream)
}

fn transformed_json_response(
    status: StatusCode,
    upstream_headers: &reqwest::header::HeaderMap,
    body: Vec<u8>,
    permit: OwnedSemaphorePermit,
) -> Response<Body> {
    let length = body.len();
    let mut response = Response::new(bounded_response_body(Bytes::from(body), permit));
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

fn ingest_response(body: Vec<u8>, send_body: bool, permit: OwnedSemaphorePermit) -> Response<Body> {
    let length = if send_body { body.len() } else { 0 };
    let mut response = Response::new(if send_body {
        bounded_response_body(Bytes::from(body), permit)
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
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn public_error(send_body: bool) -> Response<Body> {
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
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn status_response(status: StatusCode) -> Response<Body> {
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
    use axum::{extract::State, routing::any};
    use serde_json::{Value, json};
    use tokio::task::JoinHandle;

    #[derive(Clone)]
    struct MockResponse {
        status: StatusCode,
        content_type: &'static str,
        body: Bytes,
    }

    async fn mock_upstream(
        State(mock): State<MockResponse>,
        request: Request<Body>,
    ) -> Response<Body> {
        let mut response = Response::new(if request.method() == Method::HEAD {
            Body::empty()
        } else {
            Body::from(mock.body.clone())
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
            "x-internal-diagnostic",
            HeaderValue::from_static("must-not-leak"),
        );
        response
    }

    async fn spawn_mock(mock: MockResponse) -> (SocketAddr, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().fallback(any(mock_upstream)).with_state(mock);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
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
        let app = Router::new().fallback(proxy_handler).with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
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
        let body = bounded_response_body(Bytes::from_static(b"public response"), permit);
        assert_eq!(slots.available_permits(), 0);

        let collected = axum::body::to_bytes(body, 1024).await.unwrap();
        assert_eq!(collected.as_ref(), b"public response");
        assert_eq!(slots.available_permits(), 1);

        let permit = Arc::clone(&slots).acquire_owned().await.unwrap();
        let body = bounded_response_body(Bytes::from_static(b"abandoned"), permit);
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

    async fn slow_sse() -> Response<Body> {
        let stream = futures_util::stream::unfold(0_u8, |index| async move {
            if index == 10 {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            Some((
                Ok::<Bytes, Infallible>(Bytes::from_static(b": heartbeat\n\n")),
                index + 1,
            ))
        });
        let mut response = Response::new(Body::from_stream(stream));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        response
    }

    #[tokio::test]
    async fn read_timeout_resets_for_a_healthy_long_lived_sse_stream() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            axum::serve(listener, Router::new().fallback(any(slow_sse)))
                .await
                .unwrap();
        });
        let client = build_upstream_client(Duration::from_millis(100)).unwrap();
        let started = tokio::time::Instant::now();
        let body = client
            .get(format!("http://{address}/api/v1/events"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b": heartbeat\n\n".repeat(10));
        assert!(started.elapsed() >= Duration::from_millis(200));
        upstream_task.abort();
    }

    #[tokio::test]
    async fn generic_json_is_sanitized_and_private_headers_are_dropped() {
        let (upstream, upstream_task) = spawn_mock(MockResponse {
            status: StatusCode::OK,
            content_type: "application/json",
            body: Bytes::from_static(
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
                body: Bytes::from_static(b"private diagnostic"),
            },
            MockResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                content_type: "application/json",
                body: Bytes::from_static(br#"{"token":"TOPSECRET"}"#),
            },
        ] {
            let (upstream, upstream_task) = spawn_mock(mock).await;
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
        let (upstream, upstream_task) = spawn_mock(MockResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: "application/json",
            body: Bytes::from_static(br#"{"private":"diagnostic"}"#),
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
        let (upstream, upstream_task) = spawn_mock(MockResponse {
            status: StatusCode::OK,
            content_type: "application/json",
            body: Bytes::from_static(br#"{"wrong":true}"#),
        })
        .await;
        let ingest = json!({
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
        let (ingest_upstream, ingest_task) = spawn_mock(MockResponse {
            status: StatusCode::OK,
            content_type: "application/json; charset=utf-8",
            body: Bytes::from(serde_json::to_vec(&ingest).unwrap()),
        })
        .await;
        let (gateway, gateway_task) = spawn_gateway(upstream, ingest_upstream).await;
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
    async fn sse_is_sanitized_and_unknown_api_or_mutation_never_reaches_upstream() {
        let (upstream, upstream_task) = spawn_mock(MockResponse {
            status: StatusCode::OK,
            content_type: "Text/Event-Stream; Charset=UTF-8",
            body: Bytes::from_static(
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
