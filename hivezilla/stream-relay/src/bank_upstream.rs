//! One authenticated upstream. Durable admission precedes the resume cursor.
use crate::{
    bank_store::{BankStore, Bootstrap},
    upstream::Upstream,
};
use anyhow::{Context, Result, ensure};
use blockzilla_bank_stream_v2::{self as v2, Frame, Head, Notice, SourceIdentity};
use bytes::BytesMut;
use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};

#[derive(Default)]
pub struct Sse {
    pub pending: BytesMut,
    event: String,
    id: String,
    data: Vec<String>,
    size: usize,
}
impl Sse {
    pub fn line(&mut self, line: &[u8]) -> Result<Option<(String, String, String)>> {
        self.size += line.len() + 1;
        ensure!(self.size <= v2::MAX_META, "sse_event_too_large");
        let line = std::str::from_utf8(line)?.trim_end_matches('\r');
        if line.is_empty() {
            let result = (!self.data.is_empty())
                .then(|| (self.event.clone(), self.id.clone(), self.data.join("\n")));
            self.event.clear();
            self.id.clear();
            self.data.clear();
            self.size = 0;
            return Ok(result);
        }
        if line.starts_with(':') {
            return Ok(None);
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => {
                ensure!(self.event.is_empty(), "duplicate_sse_event");
                self.event = value.into();
            }
            "id" => {
                ensure!(
                    self.id.is_empty() && !value.contains('\0'),
                    "invalid_sse_id"
                );
                self.id = value.into();
            }
            "data" => self.data.push(value.into()),
            _ => {}
        }
        Ok(None)
    }
}
pub async fn bounded(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    ensure!(
        !matches!(response.status().as_u16(), 400 | 401 | 403 | 410),
        "upstream_terminal_http_{}",
        response.status().as_u16()
    );
    response.error_for_status_ref()?;
    let mut bytes = Vec::new();
    while let Some(part) = response.chunk().await? {
        ensure!(bytes.len() + part.len() <= limit, "response_too_large");
        bytes.extend_from_slice(&part);
    }
    Ok(bytes)
}
pub async fn fetch(
    u: &Upstream,
    client: &reqwest::Client,
    sha: &str,
    source: &SourceIdentity,
) -> Result<Frame> {
    ensure!(v2::is_digest(sha), "invalid_digest");
    let r = client
        .get(format!("{}v2/banks/chunks/{sha}", u.url))
        .bearer_auth(&u.token)
        .send()
        .await?;
    ensure!(
        !matches!(r.status().as_u16(), 400 | 401 | 403 | 410),
        "upstream_terminal_http_{}",
        r.status().as_u16()
    );
    r.error_for_status_ref()?;
    ensure!(
        r.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            == Some("application/vnd.blockzilla.stream.v2"),
        "chunk_content_type_mismatch"
    );
    let f = v2::decode(bounded(r, v2::MAX_FRAME).await?.into())?;
    ensure!(
        f.sha256 == sha && &f.source == source,
        "source_or_digest_changed"
    );
    Ok(f)
}
pub async fn head(u: &Upstream, client: &reqwest::Client, source: &SourceIdentity) -> Result<Head> {
    let r = client
        .get(format!("{}v2/banks/head", u.url))
        .bearer_auth(&u.token)
        .send()
        .await?;
    let h: Head = serde_json::from_slice(&bounded(r, v2::MAX_META).await?)?;
    h.validate(source, crate::store::now_ms(), v2::DEFAULT_MAX_LAG_MS)?;
    Ok(h)
}
pub async fn bootstrap(
    u: &Upstream,
    client: &reqwest::Client,
    source: &SourceIdentity,
) -> Result<Bootstrap> {
    let r = client
        .get(format!("{}v2/banks/state", u.url))
        .bearer_auth(&u.token)
        .send()
        .await?;
    let b: Bootstrap = serde_json::from_slice(&bounded(r, 16 * 1024 * 1024).await?)?;
    ensure!(&b.source == source, "source_changed");
    b.chain.validate(source, &b.cursor)?;
    Ok(b)
}
pub async fn install(
    u: &Upstream,
    client: &reqwest::Client,
    store: &Arc<BankStore>,
    b: Bootstrap,
) -> Result<()> {
    let mut dependencies = Vec::new();
    if let Some(h) = &b.chain.latest {
        let control = fetch(
            u,
            client,
            v2::parse_cursor(&h.event_cursor)?.2,
            &store.source,
        )
        .await?;
        ensure!(
            v2::ConfirmedObservation::from_frame(&control)? == *h,
            "bootstrap_head_changed"
        );
        let payload = fetch(u, client, &h.payload_frame_sha256, &store.source).await?;
        control.envelope.event.matches_payload(&payload)?;
        dependencies.push(control);
        dependencies.push(payload);
    }
    let s = store.clone();
    tokio::task::spawn_blocking(move || s.install_bootstrap(b, dependencies)).await??;
    Ok(())
}
async fn receive(u: &Upstream, client: &reqwest::Client, store: &Arc<BankStore>) -> Result<()> {
    let mut after = store.latest();
    let mut url = reqwest::Url::parse(&format!("{}v2/banks/events", u.url))?;
    url.query_pairs_mut().append_pair("after", &after);
    let r = client.get(url).bearer_auth(&u.token).send().await?;
    ensure!(
        !matches!(r.status().as_u16(), 400 | 401 | 403 | 410),
        "upstream_terminal_http_{}",
        r.status().as_u16()
    );
    let r = r.error_for_status()?;
    ensure!(
        r.headers()
            .get("x-blockzilla-start-cursor")
            .and_then(|v| v.to_str().ok())
            == Some(&after),
        "upstream_cursor_mismatch"
    );
    ensure!(
        r.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| s.starts_with("text/event-stream")),
        "upstream_content_type_mismatch"
    );
    let mut input = r.bytes_stream();
    let mut sse = Sse::default();
    let mut initial = true;
    let mut batch: Vec<(Frame, Option<Frame>)> = Vec::new();
    let mut flush_due = tokio::time::Instant::now();
    loop {
        let next = if batch.is_empty() {
            input.next().await
        } else {
            tokio::select! {
                value=input.next()=>value,
                _=tokio::time::sleep_until(flush_due)=>{flush(store,&mut batch).await?;continue;}
            }
        };
        let Some(part) = next else { break };
        let part = part?;
        ensure!(
            sse.pending.len() + part.len() <= 2 * v2::MAX_META,
            "sse_read_too_large"
        );
        sse.pending.extend_from_slice(&part);
        while let Some(end) = sse.pending.iter().position(|b| *b == b'\n') {
            let line = sse.pending.split_to(end + 1);
            let Some((event, id, data)) = sse.line(&line[..end])? else {
                continue;
            };
            if initial {
                ensure!(event == "status" && id.is_empty(), "missing_initial_status");
                initial = false;
            }
            match event.as_str() {
                "status" => {
                    flush(store, &mut batch).await?;
                    ensure!(id.is_empty(), "status_has_id");
                    let h: Head = serde_json::from_str(&data)?;
                    h.validate(
                        &store.source,
                        crate::store::now_ms(),
                        v2::DEFAULT_MAX_LAG_MS,
                    )?;
                    ensure!(h.status != "failed", "source_failed");
                    store.set_health(&h.status, None);
                }
                "gap" => {
                    ensure!(id.is_empty(), "gap_has_id");
                    let gap: serde_json::Value = serde_json::from_str(&data)?;
                    let h: Head = serde_json::from_value(gap["head"].clone())?;
                    h.validate(
                        &store.source,
                        crate::store::now_ms(),
                        v2::DEFAULT_MAX_LAG_MS,
                    )?;
                    let error = gap["error"].as_str().context("invalid_gap")?;
                    ensure!(
                        matches!(
                            error,
                            "source_changed"
                                | "replay_unavailable"
                                | "source_failed"
                                | "dependency_unavailable"
                        ),
                        "invalid_gap"
                    );
                    anyhow::bail!("{error}");
                }
                "chunk" => {
                    let notice: Notice = serde_json::from_str(&data)?;
                    let sequence = v2::parse_cursor(&after)?
                        .1
                        .checked_add(1)
                        .context("sequence_overflow")?;
                    notice.preflight(&id, sequence, &store.source)?;
                    let frame = fetch(u, client, &notice.sha256, &store.source).await?;
                    notice.validate(&id, &frame, sequence, &store.source)?;
                    let dependency = if let Some(sha) = &frame.envelope.event.payload_frame_sha256 {
                        if store.chunk(sha).is_none()
                            && !batch.iter().any(|(f, dep)| {
                                &f.sha256 == sha || dep.as_ref().is_some_and(|d| &d.sha256 == sha)
                            })
                        {
                            Some(fetch(u, client, sha, &store.source).await?)
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    after = frame.cursor.clone();
                    if batch.is_empty() {
                        flush_due = tokio::time::Instant::now() + Duration::from_millis(10);
                    }
                    batch.push((frame, dependency));
                    if batch.len() >= crate::bank_store::MAX_BATCH_EVENTS
                        || tokio::time::Instant::now() >= flush_due
                    {
                        flush(store, &mut batch).await?;
                    }
                }
                _ => anyhow::bail!("unsupported_sse_event"),
            }
        }
    }
    flush(store, &mut batch).await?;
    anyhow::bail!("upstream_closed")
}
async fn flush(store: &Arc<BankStore>, batch: &mut Vec<(Frame, Option<Frame>)>) -> Result<()> {
    if !batch.is_empty() {
        let s = store.clone();
        let frames = std::mem::take(batch);
        tokio::task::spawn_blocking(move || s.admit_batch(frames)).await??;
    }
    Ok(())
}
pub async fn run(u: Upstream, client: reqwest::Client, store: Arc<BankStore>) {
    let mut delay = 1;
    loop {
        let before = store.latest();
        let result = async {
            // A saved checkpoint is already an admitted boundary. Bootstrap only a
            // fresh store, and retry transient startup I/O without changing its base.
            if store.needs_bootstrap() {
                let b = bootstrap(&u, &client, &store.source).await?;
                install(&u, &client, &store, b).await?;
            }
            receive(&u, &client, &store).await
        }
        .await;
        if let Err(e) = result {
            let msg = e.to_string();
            let retry = e.downcast_ref::<reqwest::Error>().is_some() || msg == "upstream_closed";
            store.set_health(if retry { "stale" } else { "failed" }, Some(msg));
            if !retry {
                return;
            }
        }
        if store.latest() != before {
            delay = 1;
        }
        tokio::time::sleep(Duration::from_secs(delay)).await;
        delay = (delay * 2).min(10);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bank_server::BankApi,
        server::{self, Api},
        store::Store,
    };
    use tokio::sync::Semaphore;
    use v2::fixtures::*;
    fn fresh(f: Frame) -> Frame {
        let n = u32::from_le_bytes(f.wire[8..12].try_into().unwrap()) as usize;
        let mut e = f.envelope;
        e.event.observed_at_unix_ms = crate::store::now_ms();
        v2::encode(e, &f.wire[16 + n..]).unwrap()
    }
    fn api(banks: Arc<BankStore>, origin: bool) -> Api {
        Api {
            store: Store::open("fixture-session:0:-".into(), v2::MAX_FRAME, None).unwrap(),
            token: Arc::new("k".repeat(32)),
            cors: None,
            requests: Arc::new(Semaphore::new(16)),
            allow_query_api_key: false,
            public_blockhashes: false,
            public_requests: Arc::new(Semaphore::new(0)),
            banks: Some(BankApi {
                store: banks,
                origin,
                output: None,
            }),
            bank_only: true,
        }
    }
    fn upstream(addr: std::net::SocketAddr) -> Upstream {
        Upstream {
            url: format!("http://{addr}/"),
            token: "k".repeat(32),
            ca: None,
            identity: None,
        }
    }
    fn store(path: &std::path::Path) -> Arc<BankStore> {
        BankStore::open(
            source(),
            v2::cursor(&source().session_id, 0, "-"),
            v2::MAX_FRAME,
            path.into(),
        )
        .unwrap()
    }
    async fn wait_sequence(s: &Arc<BankStore>, sequence: u64) {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if v2::parse_cursor(&s.latest()).unwrap().1 >= sequence {
                    break;
                }
                assert_ne!(s.head().status, "failed");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn fresh_bootstrap_retries_connection_refusal_without_reset() {
        let t = tempfile::tempdir().unwrap();
        let origin = store(&t.path().join("origin"));
        let relay = store(&t.path().join("relay"));
        let b = fresh(block(1, 9, 1, 8, 9));
        origin.admit(b.clone(), None).unwrap();
        origin
            .admit(control(&b, 2, "root", crate::store::now_ms()), None)
            .unwrap();
        origin.set_health("live", None);
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reservation.local_addr().unwrap();
        drop(reservation);
        let u = upstream(addr);
        let task = tokio::spawn(run(u.clone(), u.client().unwrap(), relay.clone()));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(v2::parse_cursor(&relay.latest()).unwrap().1, 0);
        assert_ne!(relay.head().status, "failed");
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let app = server::router(api(origin.clone(), true));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        wait_sequence(&relay, 2).await;
        assert!(relay.chunk(&b.sha256).is_some());
        assert_eq!(
            relay.head().latest_confirmed.unwrap().bank,
            b.envelope.event.bank
        );
        let next = fresh(block(3, 17, 2, 9, 10));
        origin.admit(next.clone(), None).unwrap();
        origin
            .admit(control(&next, 4, "confirmed", crate::store::now_ms()), None)
            .unwrap();
        wait_sequence(&relay, 4).await;
        assert_eq!(relay.head().status, "live");
        task.abort();
        server.abort();
    }
    #[tokio::test]
    async fn restart_uses_saved_boundary_without_bootstrap_route() {
        let t = tempfile::tempdir().unwrap();
        let origin = store(&t.path().join("origin"));
        let relay = store(&t.path().join("relay"));
        let b = fresh(block(1, 9, 1, 8, 9));
        let root = control(&b, 2, "root", crate::store::now_ms());
        for s in [&origin, &relay] {
            s.admit(b.clone(), None).unwrap();
            s.admit(root.clone(), None).unwrap();
        }
        drop(relay);
        let relay = store(&t.path().join("relay"));
        origin.set_health("live", None);
        let next = fresh(block(3, 17, 2, 9, 10));
        origin.admit(next.clone(), None).unwrap();
        origin
            .admit(control(&next, 4, "confirmed", crate::store::now_ms()), None)
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // No private state route: restoration must not request another bootstrap.
        let app = server::router(api(origin, false));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let u = upstream(addr);
        let task = tokio::spawn(run(u.clone(), u.client().unwrap(), relay.clone()));
        wait_sequence(&relay, 4).await;
        assert!(relay.chunk(&b.sha256).is_some());
        assert_eq!(relay.head().status, "live");
        task.abort();
        server.abort();
    }
}
