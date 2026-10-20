use crate::{frame::Chunk, source::bounded_file, store::Store};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto::Builder,
    service::TowerToHyperService,
};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use serde::Deserialize;
use serde_json::json;
use std::{
    future::Future,
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
    sync::Semaphore,
};

#[derive(Clone)]
pub struct Api {
    pub store: Arc<Store>,
    pub token: Arc<String>,
    pub cors: Option<String>,
    pub requests: Arc<Semaphore>,
    pub allow_query_api_key: bool,
    pub public_blockhashes: bool,
    pub public_requests: Arc<Semaphore>,
    pub banks: Option<crate::bank_server::BankApi>,
    pub bank_only: bool,
}
#[derive(Clone, Default)]
pub struct Tls {
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub client_ca: Option<PathBuf>,
}

fn key_matches(value: &str, token: &str) -> bool {
    if value.len() != token.len() {
        return false;
    }
    value
        .bytes()
        .zip(token.bytes())
        .fold(0u8, |d, (a, b)| d | (a ^ b))
        == 0
}
#[derive(Default, Deserialize)]
struct Credential {
    api_key: Option<String>,
}
fn authorized(req: &Request, api: &Api) -> bool {
    // Parse once before handlers. Duplicate api_key fields fail closed, and an
    // invalid header cannot fall back to a valid URL credential.
    if req.uri().query().is_some_and(|q| q.len() > 4096) {
        return false;
    }
    let Ok(Query(p)) = Query::<Credential>::try_from_uri(req.uri()) else {
        return false;
    };
    if req.uri().path().starts_with("/v2/banks") && p.api_key.is_some() {
        return false;
    }
    if p.api_key.is_some() && !api.allow_query_api_key {
        return false;
    }
    if let Some(h) = req.headers().get(header::AUTHORIZATION) {
        return req.headers().get_all(header::AUTHORIZATION).iter().count() == 1
            && h.to_str()
                .ok()
                .and_then(|s| s.strip_prefix("Bearer "))
                .is_some_and(|value| {
                    key_matches(value, &api.token)
                        && p.api_key
                            .as_deref()
                            .is_none_or(|key| key_matches(key, &api.token))
                });
    }
    p.api_key
        .as_deref()
        .is_some_and(|key| key_matches(key, &api.token))
}
async fn access(State(api): State<Api>, req: Request, next: Next) -> Response {
    let public = api.public_blockhashes
        && matches!(
            req.uri().path(),
            "/v1/blockhashes" | "/demo" | "/demo.js" | "/demo.css"
        )
        && matches!(
            *req.method(),
            axum::http::Method::GET | axum::http::Method::OPTIONS
        );
    let mut r = access_inner(api, req, next, public).await;
    r.headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    r.headers_mut()
        .insert("x-accel-buffering", "no".parse().unwrap());
    r.headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    r.headers_mut()
        .insert("referrer-policy", "no-referrer".parse().unwrap());
    if public {
        r.headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
        r.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            "GET, OPTIONS".parse().unwrap(),
        );
        r.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            "Last-Event-ID".parse().unwrap(),
        );
    }
    r
}
async fn access_inner(api: Api, req: Request, next: Next, public: bool) -> Response {
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    if !public
        && origin
            .as_ref()
            .is_some_and(|o| api.cors.as_ref() != Some(o))
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    if req.method() == axum::http::Method::OPTIONS {
        let mut r = StatusCode::NO_CONTENT.into_response();
        cors(&mut r, origin.as_deref());
        return r;
    }
    if !public && !authorized(&req, &api) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"unauthorized"})),
        )
            .into_response();
    }
    let budget = if public {
        &api.public_requests
    } else {
        &api.requests
    };
    let Ok(permit) = budget.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let r = next.run(req).await;
    let (parts, body) = r.into_parts();
    let output = async_stream::stream! {
        let _permit=permit;
        use futures_util::StreamExt;
        let mut input=body.into_data_stream();
        while let Some(b)=input.next().await {yield b.map_err(std::io::Error::other);}
    };
    let mut r = Response::from_parts(parts, Body::from_stream(output));
    if !public {
        cors(&mut r, origin.as_deref());
    }
    r
}
fn cors(r: &mut Response, origin: Option<&str>) {
    if let Some(o) = origin {
        r.headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, o.parse().unwrap());
        r.headers_mut()
            .insert(header::VARY, "Origin".parse().unwrap());
        r.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            "GET, OPTIONS".parse().unwrap(),
        );
        r.headers_mut().insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            "Authorization, Last-Event-ID".parse().unwrap(),
        );
        r.headers_mut().insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            "X-Blockzilla-Start-Cursor, ETag".parse().unwrap(),
        );
    }
}
pub fn router(api: Api) -> Router {
    let mut routes = if api.bank_only {
        Router::new()
    } else {
        Router::new()
            .route("/v1/head", get(head))
            .route("/v1/blocks", get(binary))
            .route("/v1/events", get(events))
            .route("/v1/chunks/{id}", get(chunk))
    };
    if let Some(banks) = &api.banks {
        routes = routes.merge(crate::bank_server::routes(banks.origin));
    }
    if api.public_blockhashes {
        routes = routes
            .route("/v1/blockhashes", get(blockhashes))
            .route("/demo", get(demo))
            .route("/demo.js", get(demo_script))
            .route("/demo.css", get(demo_style));
    }
    routes
        .layer(middleware::from_fn_with_state(api.clone(), access))
        .with_state(api)
}
async fn head(State(api): State<Api>) -> Json<serde_json::Value> {
    Json(api.store.head())
}
async fn demo() -> Response {
    let mut r = Html(include_str!("../demo.html")).into_response();
    r.headers_mut().insert("content-security-policy",
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'".parse().unwrap());
    r
}
async fn demo_script() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../demo.js"),
    )
}
async fn demo_style() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../demo.css"),
    )
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    after: Option<String>,
    #[serde(rename = "api_key")]
    _api_key: Option<String>,
}
fn position(api: &Api, p: Position, headers: &HeaderMap) -> Result<String> {
    let header = headers
        .get("last-event-id")
        .map(|v| v.to_str())
        .transpose()?;
    ensure!(
        p.after.as_ref().zip(header).is_none_or(|(a, b)| a == b),
        "conflicting_cursor"
    );
    let after = p
        .after
        .or_else(|| header.map(str::to_owned))
        .unwrap_or_else(|| api.store.latest());
    ensure!(after.len() <= 240, "invalid_cursor");
    api.store.after(&after)?;
    Ok(after)
}
fn replay_error(api: &Api, e: anyhow::Error) -> Response {
    let msg = e.to_string();
    let status = if msg == "replay_unavailable" {
        StatusCode::GONE
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, Json(json!({"error":msg,"head":api.store.head()}))).into_response()
}
// Subscribe before checking state: a publish between the check and await cannot be lost.
async fn next_chunk(api: &Api, cursor: &str) -> Result<Option<Arc<Chunk>>> {
    let notified = api.store.notify.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    if let Some(c) = api.store.after(cursor)? {
        return Ok(Some(c));
    }
    let head = api.store.head();
    ensure!(head["state"] != "FAILED", "source_failed");
    let _ = tokio::time::timeout(Duration::from_secs(10), notified).await;
    Ok(None)
}
async fn binary(State(api): State<Api>, Query(p): Query<Position>, headers: HeaderMap) -> Response {
    let mut after = match position(&api, p, &headers) {
        Ok(p) => p,
        Err(e) => return replay_error(&api, e),
    };
    let start = after.clone();
    let stream = async_stream::stream! {
        loop {
            match next_chunk(&api,&after).await {
                Ok(Some(c))=>{
                    after=c.cursor.clone(); yield Ok::<_,std::io::Error>(c.wire.clone());
                    // Hold the shared chunk until the HTTP consumer polls the next item.
                    drop(c);
                }
                Ok(None)=>{},
                Err(e)=>{yield Err(std::io::Error::other(e));break;},
            }
        }
    };
    let mut r = Body::from_stream(stream).into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/vnd.blockzilla.stream.v1".parse().unwrap(),
    );
    r.headers_mut()
        .insert("x-blockzilla-start-cursor", start.parse().unwrap());
    r
}
async fn events(State(api): State<Api>, Query(p): Query<Position>, headers: HeaderMap) -> Response {
    let mut after = match position(&api, p, &headers) {
        Ok(p) => p,
        Err(e) => return replay_error(&api, e),
    };
    let stream = async_stream::stream! {
        loop {
            match next_chunk(&api,&after).await {
                Ok(Some(c))=>{
                    let notice=json!({"cursor":c.cursor,"sha256":c.digest,"bytes":c.wire.len(),"start_slot":c.first_slot,"end_slot":c.last_slot,"url":format!("/v1/chunks/{}",c.digest)});
                    after=c.cursor.clone();
                    yield Ok::<Bytes,std::io::Error>(format!("id: {}\nevent: chunk\ndata: {}\n\n",c.cursor,notice).into());
                }
                Ok(None)=>{yield Ok(format!("event: status\ndata: {}\n\n",api.store.head()).into());}
                Err(e)=>{yield Ok(format!("event: gap\ndata: {}\n\n",json!({"error":e.to_string(),"head":api.store.head()})).into());break;}
            }
        }
    };
    let mut r = Body::from_stream(stream).into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/event-stream; charset=utf-8".parse().unwrap(),
    );
    r
}
fn public_head(api: &Api) -> serde_json::Value {
    let h = api.store.head();
    json!({"state":h["state"],"stale":h["stale"],"last_slot":h["last_slot"],"event_sequence":h["event_sequence"]})
}
fn blockhash_notice(c: &Chunk) -> Result<Bytes> {
    let e = c
        .envelope
        .event
        .as_ref()
        .context("processed_feed_required")?;
    // All clients share these encoded bytes. No transaction decode, signature
    // export, or additional validator read is needed for this metadata view.
    Ok(c.blockhash_notice
        .get_or_init(|| {
            // The SSE envelope already carries the event kind and resume ID.
            // Controls refer to a published block; repeat no parent metadata.
            let mut notice = json!({"slot":e.bank.slot,
                "blockhash":bs58::encode(e.bank.blockhash).into_string()});
            if e.kind == "block" {
                notice["parent_blockhash"] =
                    bs58::encode(e.bank.parent_blockhash).into_string().into();
            }
            format!("id: {}\nevent: {}\ndata: {}\n\n", c.cursor, e.kind, notice).into()
        })
        .clone())
}
async fn blockhashes(
    State(api): State<Api>,
    Query(p): Query<Position>,
    headers: HeaderMap,
) -> Response {
    if api.store.head()["profile"] != crate::frame::PROCESSED {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"processed_feed_required"})),
        )
            .into_response();
    }
    let mut after = match position(&api, p, &headers) {
        Ok(p) => p,
        Err(e) => {
            let unavailable = e.to_string() == "replay_unavailable";
            return (if unavailable {StatusCode::GONE} else {StatusCode::BAD_REQUEST},
                Json(json!({"error":if unavailable {"replay_unavailable"} else {"invalid_cursor"},"head":public_head(&api)}))).into_response();
        }
    };
    let stream = async_stream::stream! {
        yield Ok::<Bytes,std::io::Error>(Bytes::from_static(b": Blockzilla blockhash stream\n\n"));
        loop {
            match next_chunk(&api,&after).await {
                Ok(Some(c))=>{
                    let notice=blockhash_notice(&c);
                    after=c.cursor.clone();
                    // A slow metadata reader must not keep a full block alive.
                    drop(c);
                    match notice {
                        Ok(b)=>yield Ok(b),
                        Err(_)=>{yield Ok(Bytes::from_static(b"event: gap\ndata: {\"error\":\"processed_feed_required\"}\n\n"));break;}
                    }
                }
                Ok(None)=>yield Ok(format!("event: status\ndata: {}\n\n",public_head(&api)).into()),
                Err(e)=>{
                    let error=if e.to_string()=="replay_unavailable" {"replay_unavailable"} else {"source_failed"};
                    yield Ok(format!("event: gap\ndata: {}\n\n",json!({"error":error,"head":public_head(&api)})).into());break;
                }
            }
        }
    };
    let mut r = Body::from_stream(stream).into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/event-stream; charset=utf-8".parse().unwrap(),
    );
    r
}
async fn chunk(State(api): State<Api>, Path(id): Path<String>) -> Response {
    if id.len() != 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(c) = api.store.chunk(&id) else {
        return (
            StatusCode::GONE,
            Json(json!({"error":"replay_unavailable","head":api.store.head()})),
        )
            .into_response();
    };
    let len = c.wire.len();
    let sha = c.digest.clone();
    let stream = async_stream::stream! {yield Ok::<_,std::io::Error>(c.wire.clone());drop(c);};
    let mut r = Body::from_stream(stream).into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/vnd.blockzilla.stream.v1".parse().unwrap(),
    );
    r.headers_mut()
        .insert(header::CONTENT_LENGTH, len.to_string().parse().unwrap());
    r.headers_mut()
        .insert(header::ETAG, format!("\"{sha}\"").parse().unwrap());
    r
}
fn tls_config(tls: &Tls, origin: bool) -> Result<Option<Arc<ServerConfig>>> {
    let (Some(cert), Some(key)) = (&tls.cert, &tls.key) else {
        ensure!(
            tls.cert.is_none() && tls.key.is_none() && tls.client_ca.is_none(),
            "incomplete_tls_config"
        );
        return Ok(None);
    };
    let certs = CertificateDer::pem_slice_iter(&bounded_file(cert, 65536)?)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_slice(&bounded_file(key, 65536)?)?;
    let builder = ServerConfig::builder();
    let mut config = if let Some(ca) = &tls.client_ca {
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(&bounded_file(ca, 65536)?) {
            roots.add(cert?)?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)?
    } else {
        ensure!(!origin, "origin_tls_requires_client_ca");
        builder.with_no_client_auth().with_single_cert(certs, key)?
    };
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    config.max_early_data_size = 0;
    Ok(Some(Arc::new(config)))
}

/// Bound silent or blocked connections without limiting an active stream's age.
struct IdleIo<I> {
    io: I,
    timeout: Duration,
    deadline: Pin<Box<tokio::time::Sleep>>,
}
impl<I> IdleIo<I> {
    fn new(io: I, timeout: Duration) -> Self {
        Self {
            io,
            timeout,
            deadline: Box::pin(tokio::time::sleep(timeout)),
        }
    }
    fn check_idle(&mut self, cx: &mut TaskContext<'_>) -> io::Result<()> {
        if self.deadline.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection_idle_timeout",
            ))
        } else {
            Ok(())
        }
    }
    fn progress(&mut self) {
        self.deadline
            .as_mut()
            .reset(tokio::time::Instant::now() + self.timeout);
    }
}
impl<I: AsyncRead + Unpin> AsyncRead for IdleIo<I> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_idle(cx)?;
        let before = buf.filled().len();
        let result = Pin::new(&mut this.io).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            this.progress();
        }
        result
    }
}
impl<I: AsyncWrite + Unpin> AsyncWrite for IdleIo<I> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check_idle(cx)?;
        let result = Pin::new(&mut this.io).poll_write(cx, buf);
        if matches!(result, Poll::Ready(Ok(n)) if n > 0) {
            this.progress();
        }
        result
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check_idle(cx)?;
        let result = Pin::new(&mut this.io).poll_write_vectored(cx, bufs);
        if matches!(result, Poll::Ready(Ok(n)) if n > 0) {
            this.progress();
        }
        result
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_idle(cx)?;
        Pin::new(&mut this.io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_idle(cx)?;
        Pin::new(&mut this.io).poll_shutdown(cx)
    }
}
async fn http_connection<I>(io: I, app: Router, idle: Duration)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut b = Builder::new(TokioExecutor::new());
    b.http1()
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(5))
        .max_buf_size(64 * 1024);
    b.http2()
        .timer(TokioTimer::new())
        .max_concurrent_streams(32)
        .max_send_buf_size(64 * 1024);
    let service = TowerToHyperService::new(app);
    let _ = b
        .serve_connection(TokioIo::new(IdleIo::new(io, idle)), service)
        .await;
}
pub async fn serve(bind: SocketAddr, api: Api, tls: Tls, origin: bool) -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls = tls_config(&tls, origin)?;
    ensure!(
        bind.ip().is_loopback() || tls.is_some(),
        "non_loopback_requires_tls"
    );
    let listener = TcpListener::bind(bind)
        .await
        .context("bind stream service")?;
    let connections = Arc::new(Semaphore::new(64));
    let app = router(api);
    eprintln!("stream listening on {bind}; tls={}", tls.is_some());
    loop {
        let (socket, _) = listener.accept().await?;
        let Ok(permit) = connections.clone().try_acquire_owned() else {
            drop(socket);
            continue;
        };
        socket.set_nodelay(true)?;
        let app = app.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Some(config) = tls {
                if let Ok(Ok(s)) = tokio::time::timeout(
                    Duration::from_secs(5),
                    tokio_rustls::TlsAcceptor::from(config).accept(socket),
                )
                .await
                {
                    http_connection(s, app, Duration::from_secs(300)).await;
                }
            } else {
                http_connection(socket, app, Duration::from_secs(300)).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_api() -> Api {
        Api {
            store: Store::open(
                crate::frame::processed_anchor("session1", 0),
                crate::frame::MAX_FRAME,
                None,
            )
            .unwrap(),
            token: Arc::new("abc+/?#%=".into()),
            cors: None,
            requests: Arc::new(Semaphore::new(2)),
            allow_query_api_key: false,
            public_blockhashes: false,
            public_requests: Arc::new(Semaphore::new(1)),
            banks: None,
            bank_only: false,
        }
    }
    fn request(query: Option<&str>, header: Option<&str>) -> Request {
        let mut b = Request::builder().uri(format!(
            "/v1/events{}",
            query.map_or(String::new(), |q| format!("?{q}"))
        ));
        if let Some(h) = header {
            b = b.header(header::AUTHORIZATION, h);
        }
        b.body(Body::empty()).unwrap()
    }
    #[test]
    fn query_keys_are_opt_in_and_conflicts_fail_closed() {
        let mut a = test_api();
        let encoded = "api_key=abc%2B%2F%3F%23%25%3D";
        assert!(!authorized(&request(None, None), &a));
        assert!(authorized(&request(None, Some("Bearer abc+/?#%=")), &a));
        assert!(!authorized(&request(Some(encoded), None), &a));
        a.allow_query_api_key = true;
        assert!(authorized(&request(Some(encoded), None), &a));
        assert!(authorized(
            &request(Some(encoded), Some("Bearer abc+/?#%=")),
            &a
        ));
        assert!(!authorized(
            &request(Some(encoded), Some("Bearer wrong")),
            &a
        ));
        assert!(!authorized(
            &request(Some("api_key=wrong"), Some("Bearer abc+/?#%=")),
            &a
        ));
        assert!(!authorized(
            &request(Some(&format!("{encoded}&{encoded}")), None),
            &a
        ));
        assert!(!authorized(&request(Some("api_key="), None), &a));
        assert!(!authorized(
            &request(Some("api_key=abc+/?%23%25="), None),
            &a
        ));
    }
    async fn listen(a: Api) -> (String, tokio::task::JoinHandle<()>) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(l, router(a)).await.unwrap();
        });
        (url, task)
    }
    #[tokio::test]
    async fn active_sse_survives_multiple_connection_idle_intervals() {
        use futures_util::StreamExt;
        let api = test_api();
        let store = api.store.clone();
        store
            .admit(crate::frame::processed_fixture(1, 100, 1, "block"))
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let idle = Duration::from_secs(1);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            http_connection(socket, router(api), idle).await;
        });
        let response = reqwest::Client::new()
            .get(format!(
                "http://{address}/v1/events?after={}",
                crate::frame::processed_anchor("session1", 0)
            ))
            .bearer_auth("abc+/?#%=")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/event-stream")
        );
        let mut body = response.bytes_stream();
        let mut text = String::new();
        let started = tokio::time::Instant::now();
        for sequence in 1..=19 {
            if sequence > 1 {
                tokio::time::sleep(Duration::from_millis(120)).await;
                store
                    .admit(crate::frame::processed_fixture(
                        sequence,
                        100 + sequence,
                        sequence,
                        "block",
                    ))
                    .unwrap();
            }
            let target = format!("id: {}", store.latest());
            tokio::time::timeout(idle, async {
                while !text.contains(&target) {
                    let bytes = body
                        .next()
                        .await
                        .expect("SSE closed")
                        .expect("SSE read failed");
                    text.push_str(std::str::from_utf8(&bytes).unwrap());
                }
            })
            .await
            .unwrap();
        }
        assert!(started.elapsed() > 2 * idle);
        assert!(!server.is_finished());
        drop(body);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn silent_http_connection_expires_and_releases_its_task() {
        use tokio::io::AsyncReadExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            http_connection(socket, router(test_api()), Duration::from_millis(100)).await;
        });
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        server.await.unwrap();
    }
    #[tokio::test]
    async fn zero_progress_reads_and_blocked_writes_do_not_renew_idle_time() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (server, _peer) = tokio::io::duplex(1);
        let mut read = IdleIo::new(server, Duration::from_millis(100));
        let mut byte = [0];
        let error = tokio::time::timeout(Duration::from_secs(2), read.read_exact(&mut byte))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let (server, _peer) = tokio::io::duplex(1);
        let mut write = IdleIo::new(server, Duration::from_millis(100));
        let error = tokio::time::timeout(Duration::from_secs(2), write.write_all(&[1, 2]))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
    fn keyed(base: &str, path: &str) -> reqwest::Url {
        let mut u = reqwest::Url::parse(&format!("{base}{path}")).unwrap();
        u.query_pairs_mut().append_pair("api_key", "abc+/?#%=");
        u
    }
    #[tokio::test]
    async fn url_key_works_for_notices_and_chunk_get_without_exposure() {
        let mut a = test_api();
        a.allow_query_api_key = true;
        let c = crate::frame::processed_fixture(1, 100, 7, "block");
        a.store.admit(c.clone()).unwrap();
        let (base, task) = listen(a).await;
        let client = reqwest::Client::new();
        assert_eq!(
            client
                .get(format!("{base}/v1/head"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let r = client.get(keyed(&base, "/v1/head")).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers()["referrer-policy"], "no-referrer");
        assert_eq!(r.headers()["cache-control"], "no-store");
        assert!(!r.text().await.unwrap().contains("abc+/?#%="));
        let r = client
            .get(keyed(&base, &format!("/v1/chunks/{}", c.digest)))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.bytes().await.unwrap(), c.wire);
        let mut u = keyed(&base, "/v1/events");
        u.query_pairs_mut()
            .append_pair("after", &crate::frame::processed_anchor("session1", 0));
        let r = client.get(u).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        use futures_util::StreamExt;
        let b = r.bytes_stream().next().await.unwrap().unwrap();
        let text = std::str::from_utf8(&b).unwrap();
        assert!(text.contains("event: chunk"));
        assert!(!text.contains("api_key"));
        task.abort();
    }
    #[tokio::test]
    async fn public_metadata_has_fork_events_resume_and_no_block_access() {
        let mut a = test_api();
        a.public_blockhashes = true;
        let store = a.store.clone();
        for (seq, bank, kind) in [
            (1, 7, "block"),
            (2, 8, "block"),
            (3, 7, "invalidate"),
            (4, 8, "root"),
        ] {
            store
                .admit(crate::frame::processed_fixture(seq, 100, bank, kind))
                .unwrap();
        }
        let (base, task) = listen(a).await;
        let client = reqwest::Client::new();
        for path in [
            "/v1/head",
            "/v1/events",
            "/v1/blocks",
            "/v1/chunks/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ] {
            assert_eq!(
                client
                    .get(format!("{base}{path}"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::UNAUTHORIZED
            );
        }
        let mut u = reqwest::Url::parse(&format!("{base}/v1/blockhashes")).unwrap();
        u.query_pairs_mut()
            .append_pair("after", &crate::frame::processed_anchor("session1", 0));
        let r = client
            .get(u.clone())
            .header(header::ORIGIN, "https://demo.example")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        use futures_util::StreamExt;
        let mut body = r.bytes_stream();
        let mut text = String::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !text.contains("event: root") {
                let b = body.next().await.unwrap().unwrap();
                text.push_str(std::str::from_utf8(&b).unwrap());
            }
        })
        .await
        .unwrap();
        drop(body);
        let events: Vec<(String, String, serde_json::Value)> = text
            .split("\n\n")
            .filter(|record| record.starts_with("id: "))
            .map(|record| {
                let mut lines = record.lines();
                let id = lines.next().unwrap().strip_prefix("id: ").unwrap();
                let kind = lines.next().unwrap().strip_prefix("event: ").unwrap();
                let data = lines.next().unwrap().strip_prefix("data: ").unwrap();
                (id.into(), kind.into(), serde_json::from_str(data).unwrap())
            })
            .collect();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].1, "block");
        assert_eq!(events[1].1, "block");
        assert_eq!(events[2].1, "invalidate");
        assert_eq!(events[3].1, "root");
        assert_eq!(
            bs58::decode(events[0].2["blockhash"].as_str().unwrap())
                .into_vec()
                .unwrap(),
            vec![1; 32]
        );
        for (id, kind, data) in &events {
            assert!(crate::frame::parse_cursor(id).is_ok());
            let expected = if kind == "block" {
                vec!["blockhash", "parent_blockhash", "slot"]
            } else {
                vec!["blockhash", "slot"]
            };
            let mut keys: Vec<_> = data
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            keys.sort_unstable();
            assert_eq!(keys, expected);
            assert_eq!(data["slot"], 100);
            if kind == "block" {
                assert_eq!(
                    bs58::decode(data["parent_blockhash"].as_str().unwrap())
                        .into_vec()
                        .unwrap(),
                    vec![1; 32]
                );
            }
        }
        // The stream body owns the permit until the disconnect reaches the server.
        tokio::time::sleep(Duration::from_millis(30)).await;
        let r = client
            .get(format!("{base}/v1/blockhashes"))
            .header("Last-Event-ID", &events[2].0)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let mut body = r.bytes_stream();
        let mut resumed = String::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !resumed.contains("event: root") {
                resumed
                    .push_str(std::str::from_utf8(&body.next().await.unwrap().unwrap()).unwrap());
            }
        })
        .await
        .unwrap();
        assert!(!resumed.contains("event: block"));
        assert!(!resumed.contains("event: invalidate"));
        drop(body);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let r = client
            .get(u)
            .header("Last-Event-ID", &events[2].0)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        assert!(!r.text().await.unwrap().contains("abc+/?#%="));
        task.abort();
    }
    #[tokio::test]
    async fn public_request_budget_does_not_take_private_capacity() {
        let a = test_api();
        let (disabled, off) = listen(a.clone()).await;
        let client = reqwest::Client::new();
        assert_eq!(
            client
                .get(format!("{disabled}/v1/blockhashes"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        off.abort();
        let mut a = a;
        a.public_blockhashes = true;
        let (base, task) = listen(a).await;
        let page = client.get(format!("{base}/demo")).send().await.unwrap();
        assert_eq!(page.status(), StatusCode::OK);
        assert!(
            page.headers()["content-security-policy"]
                .to_str()
                .unwrap()
                .contains("connect-src 'self'")
        );
        assert!(page.text().await.unwrap().contains("/demo.js"));
        let held = client
            .get(format!("{base}/v1/blockhashes"))
            .send()
            .await
            .unwrap();
        assert_eq!(held.status(), StatusCode::OK);
        let r = client
            .get(format!("{base}/v1/blockhashes"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(r.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        let r = client
            .get(format!("{base}/v1/head"))
            .bearer_auth("abc+/?#%=")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        drop(held);
        task.abort();
    }
    #[test]
    fn origin_tls_cannot_disable_client_verification() {
        assert!(
            tls_config(
                &Tls {
                    cert: Some("not-found".into()),
                    ..Default::default()
                },
                true
            )
            .is_err()
        );
    }
}
