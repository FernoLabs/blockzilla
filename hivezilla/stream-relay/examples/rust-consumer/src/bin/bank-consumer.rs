//! Opt-in backend example. SQLite commits decoded data and the cursor together.
use anyhow::{Context, Result, ensure};
use blockzilla_bank_stream_v2::{self as v2, Head, Notice, SourceIdentity, client::ClientState};
use futures_util::StreamExt;
use hivezilla_stream::{
    bank_upstream::{self, Sse},
    upstream::Upstream,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    time::{Duration, Instant},
};
fn now() -> u64 {
    hivezilla_stream::store::now_ms()
}
fn transient(e: &anyhow::Error) -> bool {
    // Reqwest can wrap a connection reset as a decode error while reading a
    // response body. Protocol/JSON admission errors have their own types.
    e.downcast_ref::<reqwest::Error>().is_some_and(|e| {
        !e.is_builder()
            && !e.is_redirect()
            && e.status()
                .is_none_or(|s| s.is_server_error() || s.as_u16() == 429)
    })
}
async fn fetch_retry(
    u: &Upstream,
    client: &reqwest::Client,
    sha: &str,
    source: &SourceIdentity,
) -> Result<v2::Frame> {
    let mut delay = 1;
    loop {
        match bank_upstream::fetch(u, client, sha, source).await {
            Ok(frame) => return Ok(frame),
            Err(e) if transient(&e) => {
                tokio::time::sleep(Duration::from_secs(delay)).await;
                delay = (delay * 2).min(10);
            }
            Err(e) => return Err(e),
        }
    }
}
fn require(ok: bool, code: &str) -> Result<()> {
    ensure!(ok, "{code}");
    Ok(())
}
// Read and preflight a bounded notice stream before starting credentialed GETs.
fn notices(
    response: reqwest::Response,
    after: String,
    source: SourceIdentity,
) -> impl futures_util::Stream<Item = Result<(String, String, String)>> {
    async_stream::try_stream! {
        let mut input=response.bytes_stream();let mut sse=Sse::default();let mut initial=true;
        let mut sequence=v2::parse_cursor(&after)?.1;
        while let Some(part)=input.next().await {
            let part=part?;require(sse.pending.len()+part.len()<=2*v2::MAX_META,"sse_read_too_large")?;sse.pending.extend_from_slice(&part);
            while let Some(end)=sse.pending.iter().position(|b|*b==b'\n') {
                let line=sse.pending.split_to(end+1);let Some((event,id,data))=sse.line(&line[..end])? else {continue};
                if initial {require(event=="status" && id.is_empty(),"missing_initial_status")?;initial=false;}
                if event=="chunk" {
                    let n:Notice=serde_json::from_str(&data)?;n.preflight(&id,sequence+1,&source)?;sequence+=1;
                }else {require(matches!(event.as_str(),"status"|"gap") && id.is_empty(),"invalid_sse_event")?;}
                yield (event,id,data);
            }
        }
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        (5..=6).contains(&args.len()),
        "Usage: bank-consumer HTTPS_URL KEY_FILE EXPECTED_SOURCE_JSON SQLITE_FILE [EVENTS]"
    );
    let base = reqwest::Url::parse(&args[1])?;
    ensure!(
        base.scheme() == "https"
            && base.path() == "/"
            && base.query().is_none()
            && base.fragment().is_none()
            && base.username().is_empty()
            && base.password().is_none(),
        "invalid_https_base"
    );
    let key = std::fs::read_to_string(&args[2])?;
    ensure!((32..=256).contains(&key.trim().len()), "invalid_api_key");
    let source: SourceIdentity = serde_json::from_slice(&std::fs::read(&args[3])?)?;
    source.validate()?;
    let limit: usize = args.get(5).map_or(Ok(100), |v| v.parse())?;
    ensure!((1..=1_000_000).contains(&limit), "invalid_event_limit");
    let client = reqwest::Client::builder()
        .user_agent("blockzilla-bank-consumer/2")
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(15))
        .build()?;
    let u = Upstream {
        url: base.to_string(),
        token: key.trim().into(),
        ca: None,
        identity: None,
    };
    let mut db = Connection::open(Path::new(&args[4]))?;
    db.execute_batch("PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;CREATE TABLE IF NOT EXISTS source(id INTEGER PRIMARY KEY CHECK(id=1),identity TEXT NOT NULL,cursor TEXT NOT NULL);CREATE TABLE IF NOT EXISTS payloads(frame_sha256 TEXT PRIMARY KEY,decoded_json TEXT NOT NULL);CREATE TABLE IF NOT EXISTS events(sequence INTEGER PRIMARY KEY,cursor TEXT NOT NULL,frame_sha256 TEXT NOT NULL,kind TEXT NOT NULL,decoded_json TEXT NOT NULL,observed_at_unix_ms INTEGER NOT NULL,received_at_unix_ms INTEGER NOT NULL);")?;
    let pinned = serde_json::to_string(&source)?;
    let h = bank_upstream::head(&u, &client, &source).await?;
    let saved: Option<(String, String)> = db
        .query_row("SELECT identity,cursor FROM source WHERE id=1", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .optional()?;
    let mut after = if let Some((old, cursor)) = saved {
        ensure!(
            old == pinned,
            "source_changed; keep the old database and start a separate source"
        );
        cursor
    } else {
        h.cursor.clone().context("source_not_started")?
    };
    let mut state = ClientState::new(source.clone(), after.clone())?;
    if let Some(c) = &h.latest_confirmed {
        let control =
            fetch_retry(&u, &client, v2::parse_cursor(&c.event_cursor)?.2, &source).await?;
        let payload = fetch_retry(&u, &client, &c.payload_frame_sha256, &source).await?;
        let decoded = blockzilla_stream_decoder::decode_banks(&payload.wire, &Default::default())?;
        let tx = db.transaction()?;
        tx.execute(
            "INSERT INTO source VALUES(1,?1,?2) ON CONFLICT(id) DO NOTHING",
            params![pinned, after],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO payloads VALUES(?1,?2)",
            params![payload.sha256, std::str::from_utf8(&decoded)?],
        )?;
        tx.commit()?;
        state.resolve_head(&h, &control, &payload, now())?;
    }
    state.health(h, now())?;
    let mut count = 0usize;
    let mut delay = 1u64;
    loop {
        let mut url = base.join("v2/banks/events")?;
        url.query_pairs_mut().append_pair("after", &after);
        let r = match client.get(url).bearer_auth(&u.token).send().await {
            Ok(r) => r,
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(delay)).await;
                delay = (delay * 2).min(10);
                continue;
            }
        };
        ensure!(
            !matches!(r.status().as_u16(), 400 | 401 | 403 | 410),
            "explicit_http_gap_{}; application reconciliation required",
            r.status().as_u16()
        );
        if r.status().is_server_error() {
            tokio::time::sleep(Duration::from_secs(delay)).await;
            delay = (delay * 2).min(10);
            continue;
        }
        r.error_for_status_ref()?;
        ensure!(
            r.headers()
                .get("x-blockzilla-start-cursor")
                .and_then(|v| v.to_str().ok())
                == Some(&after),
            "start_cursor_mismatch"
        );
        ensure!(
            r.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("text/event-stream")),
            "wrong_sse_content_type"
        );
        let incoming = notices(r, after.clone(), source.clone());
        let get_limit = tokio::sync::Semaphore::new(4);
        let mut pending = Box::pin(
            incoming
                .map(|message| async {
                    let (event, id, data) = message?;
                    let frame = if event == "chunk" {
                        let n: Notice = serde_json::from_str(&data)?;
                        let permit = get_limit.acquire().await?;
                        let f = fetch_retry(&u, &client, &n.sha256, &source).await?;
                        drop(permit);
                        Some(f)
                    } else {
                        None
                    };
                    let decoded = if let Some(f) = &frame {
                        let wire = f.wire.clone();
                        Some(
                            tokio::task::spawn_blocking(move || {
                                blockzilla_stream_decoder::decode_banks(&wire, &Default::default())
                            })
                            .await??,
                        )
                    } else {
                        None
                    };
                    Ok::<_, anyhow::Error>((event, id, data, frame, decoded))
                })
                .buffered(12),
        );
        while let Some(message) = pending.next().await {
            let (event, id, data, fetched, decoded) = match message {
                Ok(m) => m,
                Err(e) if transient(&e) => break,
                Err(e) => return Err(e),
            };
            match event.as_str() {
                "status" => {
                    ensure!(id.is_empty(), "status_has_id");
                    let h: Head = serde_json::from_str(&data)?;
                    state.health(h, now())?;
                }
                "gap" => {
                    ensure!(id.is_empty(), "gap_has_id");
                    let gap: serde_json::Value = serde_json::from_str(&data)?;
                    let h: Head = serde_json::from_value(gap["head"].clone())?;
                    state.health(h, now())?;
                    anyhow::bail!(
                        "explicit_sse_gap: {}; application reconciliation required",
                        gap["error"]
                    );
                }
                "chunk" => {
                    let start = Instant::now();
                    let notice: Notice = serde_json::from_str(&data)?;
                    let seq = v2::parse_cursor(&after)?.1 + 1;
                    notice.preflight(&id, seq, &source)?;
                    let f = fetched.context("prefetched_frame_unavailable")?;
                    notice.validate(&id, &f, seq, &source)?;
                    let dependency = if let Some(sha) = &f.envelope.event.payload_frame_sha256 {
                        if state.has_payload(sha) {
                            None
                        } else {
                            Some(fetch_retry(&u, &client, sha, &source).await?)
                        }
                    } else {
                        None
                    };
                    // Preserve failed outcomes. Never use successful_only for signature watches.
                    let decoded_dependency = if let Some(p) = &dependency {
                        Some(blockzilla_stream_decoder::decode_banks(
                            &p.wire,
                            &Default::default(),
                        )?)
                    } else {
                        None
                    };
                    let decoded = decoded.context("prefetched_decode_unavailable")?;
                    if state.apply(&f, dependency.as_ref(), now())? {
                        let tx = db.transaction()?;
                        if let Some(p) = &dependency {
                            tx.execute(
                                "INSERT OR IGNORE INTO payloads VALUES(?1,?2)",
                                params![
                                    p.sha256,
                                    std::str::from_utf8(decoded_dependency.as_ref().unwrap())?
                                ],
                            )?;
                        }
                        let event_json = if f.envelope.event.kind == "block" {
                            tx.execute(
                                "INSERT OR IGNORE INTO payloads VALUES(?1,?2)",
                                params![f.sha256, std::str::from_utf8(&decoded)?],
                            )?;
                            serde_json::to_string(
                                &serde_json::json!({"profile":v2::PROFILE,"event":f.envelope.event}),
                            )?
                        } else {
                            std::str::from_utf8(&decoded)?.to_owned()
                        };
                        tx.execute(
                            "INSERT INTO events VALUES(?1,?2,?3,?4,?5,?6,?7)",
                            params![
                                seq,
                                f.cursor,
                                f.sha256,
                                f.envelope.event.kind,
                                event_json,
                                f.envelope.event.observed_at_unix_ms,
                                now()
                            ],
                        )?;
                        tx.execute("INSERT INTO source VALUES(1,?1,?2) ON CONFLICT(id) DO UPDATE SET cursor=excluded.cursor",params![pinned,f.cursor])?;
                        tx.commit()?;
                    }
                    after = f.cursor.clone();
                    count += 1;
                    delay = 1;
                    println!(
                        "{}",
                        serde_json::json!({"sequence":seq,"kind":f.envelope.event.kind,"slot":f.envelope.event.bank.slot,"apply_ms":start.elapsed().as_millis(),"source_to_consumer_ms":now().saturating_sub(f.envelope.event.observed_at_unix_ms),"confirmed_hash_ready":state.ready_hash(now()).is_ok()})
                    );
                    if count >= limit {
                        return Ok(());
                    }
                }
                _ => anyhow::bail!("unsupported_sse_event"),
            }
        }
        tokio::time::sleep(Duration::from_secs(delay)).await;
        delay = (delay * 2).min(10);
    }
}
