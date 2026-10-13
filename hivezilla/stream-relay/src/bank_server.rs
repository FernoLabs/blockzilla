use crate::{bank_store::BankStore, server::Api};
use anyhow::{Result, ensure};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use blockzilla_bank_stream_v2::{self as v2, Notice};
use serde::Deserialize;
use serde_json::json;
use std::{path::PathBuf, sync::Arc, time::Duration};
#[derive(Clone)]
pub struct BankApi {
    pub store: Arc<BankStore>,
    pub origin: bool,
    pub output: Option<PathBuf>,
}
pub fn routes(origin: bool) -> Router<Api> {
    let mut r = Router::new()
        .route("/v2/banks/head", get(head))
        .route("/v2/banks/events", get(events))
        .route("/v2/banks/chunks/{sha}", get(chunk));
    // Provider-to-relay bootstrap is protected by the origin's mTLS and key.
    // It is never installed on the public relay or in the consumer contract.
    if origin {
        r = r.route("/v2/banks/state", get(bootstrap));
    }
    r
}
async fn head(State(api): State<Api>) -> impl IntoResponse {
    Json(api.banks.unwrap().store.head())
}
async fn bootstrap(State(api): State<Api>) -> impl IntoResponse {
    Json(api.banks.unwrap().store.bootstrap())
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    after: Option<String>,
}
fn accepted(store: &BankStore, p: Position, h: &HeaderMap) -> Result<String> {
    ensure!(
        h.get_all("last-event-id").iter().count() <= 1,
        "invalid_cursor"
    );
    let header = h.get("last-event-id").map(|v| v.to_str()).transpose()?;
    if let (Some(q), Some(h)) = (p.after.as_deref(), header) {
        ensure!(q == h, "cursor_conflict");
    }
    let after = p
        .after
        .or_else(|| header.map(str::to_owned))
        .unwrap_or_else(|| store.latest());
    store.after(&after)?;
    Ok(after)
}
fn code(e: &anyhow::Error) -> &'static str {
    match e.to_string().as_str() {
        "source_changed" => "source_changed",
        "replay_unavailable" => "replay_unavailable",
        "cursor_conflict" => "cursor_conflict",
        _ => "invalid_cursor",
    }
}
fn failure(status: StatusCode, error: &str, store: &BankStore) -> Response {
    (status, Json(json!({"error":error,"head":store.head()}))).into_response()
}
async fn events(
    State(api): State<Api>,
    p: std::result::Result<Query<Position>, QueryRejection>,
    headers: HeaderMap,
) -> Response {
    let store = api.banks.unwrap().store;
    let p = match p {
        Ok(Query(p)) => p,
        Err(_) => return failure(StatusCode::BAD_REQUEST, "invalid_cursor", &store),
    };
    let mut after = match accepted(&store, p, &headers) {
        Ok(v) => v,
        Err(e) => {
            let c = code(&e);
            return failure(
                if matches!(c, "source_changed" | "replay_unavailable") {
                    StatusCode::GONE
                } else {
                    StatusCode::BAD_REQUEST
                },
                c,
                &store,
            );
        }
    };
    if store.head().status == "failed" {
        return failure(StatusCode::GONE, "source_failed", &store);
    }
    let start = after.clone();
    let stream = async_stream::stream! {
        yield Ok::<bytes::Bytes,std::io::Error>(format!("event: status\ndata: {}\n\n",serde_json::to_string(&store.head()).unwrap()).into());
        let mut next_status=tokio::time::Instant::now()+Duration::from_secs(5);
        loop {
            let head=store.head();
            if head.status=="failed" {
                let error=match store.error().as_deref() {Some("source_changed")=>"source_changed",Some("dependency_unavailable")=>"dependency_unavailable",_=>"source_failed"};
                yield Ok(format!("event: gap\ndata: {}\n\n",json!({"error":error,"head":head})).into());break;
            }
            if tokio::time::Instant::now()>=next_status {
                yield Ok(format!("event: status\ndata: {}\n\n",serde_json::to_string(&head).unwrap()).into());
                next_status=tokio::time::Instant::now()+Duration::from_secs(5);
            }
            let ready=store.notify.notified();
            match store.after(&after) {
                Ok(Some(f))=>{
                    after=f.cursor.clone();
                    let notice=serde_json::to_string(&Notice::from_frame(&f)).unwrap();
                    // Hold only the notice while a slow SSE reader waits.
                    drop(f);yield Ok(format!("id: {after}\nevent: chunk\ndata: {notice}\n\n").into());
                }
                Ok(None)=>{tokio::select! { _=ready=>{},_=tokio::time::sleep_until(next_status)=>{} }}
                Err(e)=>{let error=if code(&e)=="invalid_cursor" {"replay_unavailable"}else{code(&e)};
                    yield Ok(format!("event: gap\ndata: {}\n\n",json!({"error":error,"head":store.head()})).into());break;}
            }
        }
    };
    let mut r = Body::from_stream(stream).into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/event-stream; charset=utf-8".parse().unwrap(),
    );
    r.headers_mut()
        .insert("x-blockzilla-start-cursor", start.parse().unwrap());
    r
}
async fn chunk(State(api): State<Api>, Path(sha): Path<String>) -> Response {
    let banks = api.banks.unwrap();
    if !v2::is_digest(&sha) {
        return failure(StatusCode::BAD_REQUEST, "invalid_cursor", &banks.store);
    }
    let f = if let Some(f) = banks.store.chunk(&sha) {
        Some(f)
    } else if let Some(output) = banks.output {
        let sha = sha.clone();
        match tokio::task::spawn_blocking(move || {
            crate::bank_source::BankSource::open(&output)?.chunk(&sha)
        })
        .await
        {
            Ok(Ok(Some(f))) => Some(Arc::new(f)),
            Ok(Ok(None)) => None,
            _ => return failure(StatusCode::GONE, "dependency_unavailable", &banks.store),
        }
    } else {
        None
    };
    let Some(f) = f else {
        return failure(StatusCode::GONE, "dependency_unavailable", &banks.store);
    };
    let mut r = f.wire.clone().into_response();
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/vnd.blockzilla.stream.v2".parse().unwrap(),
    );
    r.headers_mut().insert(
        header::CONTENT_LENGTH,
        f.wire.len().to_string().parse().unwrap(),
    );
    r.headers_mut()
        .insert(header::ETAG, format!("\"{}\"", f.sha256).parse().unwrap());
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{server, store::Store};
    use axum::http::Request;
    use tokio::sync::Semaphore;
    use v2::fixtures::*;
    fn api(store: Arc<BankStore>) -> Api {
        Api {
            store: Store::open("fixture-session:0:-".into(), v2::MAX_FRAME, None).unwrap(),
            token: Arc::new("k".repeat(32)),
            cors: None,
            requests: Arc::new(Semaphore::new(16)),
            allow_query_api_key: true,
            public_blockhashes: false,
            public_requests: Arc::new(Semaphore::new(0)),
            banks: Some(BankApi {
                store,
                origin: false,
                output: None,
            }),
            bank_only: true,
        }
    }
    #[tokio::test]
    async fn public_v2_bearer_headers_cursor_conflict_and_gap_contract() {
        let t = tempfile::tempdir().unwrap();
        let base = v2::cursor(&source().session_id, 0, "-");
        let store =
            BankStore::open(source(), base.clone(), v2::MAX_FRAME, t.path().into()).unwrap();
        let b = block(1, 9, 1, 8, 9);
        store.admit(b.clone(), None).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = server::router(api(store.clone()));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = reqwest::Client::new();
        let url = format!("http://{addr}");
        let key = "k".repeat(32);
        assert_eq!(
            client
                .get(format!("{url}/v2/banks/head?api_key={key}"))
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
        assert_eq!(
            client
                .get(format!("{url}/v2/banks/head"))
                .bearer_auth(&key)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let mut r = client
            .get(format!("{url}/v2/banks/events?after={base}"))
            .bearer_auth(&key)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(r.headers()["x-blockzilla-start-cursor"], base);
        let initial = r.chunk().await.unwrap().unwrap();
        let text = String::from_utf8(initial.to_vec()).unwrap();
        assert!(text.starts_with("event: status\ndata: ") && !text.starts_with("id:"));
        drop(r);
        let r = client
            .get(format!("{url}/v2/banks/events?after={base}"))
            .header("last-event-id", &b.cursor)
            .bearer_auth(&key)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400);
        assert_eq!(
            r.json::<serde_json::Value>().await.unwrap()["error"],
            "cursor_conflict"
        );
        let r = client
            .get(format!("{url}/v2/banks/events?after=other-session:p0:-"))
            .bearer_auth(&key)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 410);
        let gap = r.json::<serde_json::Value>().await.unwrap();
        assert_eq!(gap["error"], "source_changed");
        assert_eq!(gap["head"]["profile"], v2::PROFILE);
        let r = client
            .get(format!("{url}/v2/banks/chunks/{}", b.sha256))
            .bearer_auth(&key)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(r.bytes().await.unwrap(), b.wire);
        store.set_health("failed", None);
        let r = client
            .get(format!("{url}/v2/banks/events?after={}", b.cursor))
            .bearer_auth(&key)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 410);
        assert_eq!(
            r.json::<serde_json::Value>().await.unwrap()["error"],
            "source_failed"
        );
        task.abort();
    }
    #[test]
    fn notice_url_and_id_are_checked_before_any_credentialed_get() {
        let b = block(1, 17, 7, 3, 10);
        let n = Notice::from_frame(&b);
        n.preflight(&b.cursor, 1, &source()).unwrap();
        let mut n = n;
        n.url = "https://attacker.invalid/steal".into();
        assert!(n.preflight(&b.cursor, 1, &source()).is_err());
        n.url = format!("/v2/banks/chunks/{}", b.sha256);
        assert!(n.preflight("wrong", 1, &source()).is_err());
        let _ = Request::builder();
    }
}
