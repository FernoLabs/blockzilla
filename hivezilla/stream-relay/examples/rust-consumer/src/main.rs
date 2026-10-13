//! Read-only live probe. Console output is not a durable application checkpoint.
use anyhow::{Context, Result, ensure};
use bytes::BytesMut;
use futures_util::StreamExt;
use hivezilla_stream::frame::{self, Chunk};
use reqwest::{Client, Response, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{path::Path, time::Duration};

#[derive(Default)]
struct Sse {
    pending: BytesMut,
    event: String,
    id: String,
    data: Vec<String>,
    bytes: usize,
}
impl Sse {
    fn line(&mut self, line: &[u8]) -> Result<Option<(String, String, String)>> {
        self.bytes += line.len() + 1;
        ensure!(self.bytes <= frame::MAX_META, "sse_event_too_large");
        let line = std::str::from_utf8(line)?.trim_end_matches('\r');
        if line.is_empty() {
            let output = (!self.data.is_empty())
                .then(|| (self.event.clone(), self.id.clone(), self.data.join("\n")));
            self.event.clear();
            self.id.clear();
            self.data.clear();
            self.bytes = 0;
            return Ok(output);
        }
        if line.starts_with(':') {
            return Ok(None);
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => self.event = value.into(),
            "id" => {
                ensure!(!value.contains('\0'), "invalid_sse_id");
                self.id = value.into();
            }
            "data" => self.data.push(value.into()),
            _ => {}
        }
        Ok(None)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Notice {
    cursor: String,
    sha256: String,
    bytes: usize,
    start_slot: u64,
    end_slot: u64,
    url: String,
}

async fn bounded(mut response: Response, limit: usize) -> Result<Vec<u8>> {
    response.error_for_status_ref()?;
    let mut bytes = Vec::new();
    while let Some(part) = response.chunk().await? {
        ensure!(bytes.len() + part.len() <= limit, "response_too_large");
        bytes.extend_from_slice(&part);
    }
    Ok(bytes)
}

fn base_url(text: &str) -> Result<Url> {
    let mut url = Url::parse(text)?;
    ensure!(
        url.scheme() == "https"
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "Use an HTTPS base URL with no credentials or extra path"
    );
    url.set_path("/");
    Ok(url)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        (3..=5).contains(&args.len()),
        "Usage: blockzilla-rust-stream-example HTTPS_URL KEY_FILE [CHUNKS] [AFTER_CURSOR]"
    );
    let base = base_url(&args[1])?;
    let key = std::fs::read_to_string(Path::new(&args[2]))?;
    let key = key.trim();
    ensure!(!key.is_empty() && key.len() <= 4096, "Invalid API key file");
    let limit: usize = args.get(3).map_or(Ok(3), |s| s.parse())?;
    ensure!(
        (1..=1000).contains(&limit),
        "CHUNKS must be between 1 and 1000"
    );
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .user_agent("blockzilla-rust-consumer/0.1")
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .build()?;
    let head: Value = serde_json::from_slice(
        &bounded(
            client
                .get(base.join("v1/head")?)
                .bearer_auth(key)
                .send()
                .await?,
            frame::MAX_META,
        )
        .await?,
    )?;
    ensure!(
        head["schema"] == "blockzilla-stream-head-v1" && head["state"] == "LIVE",
        "Source is not live"
    );
    let mut after = args.get(4).cloned().unwrap_or(
        head["latest_cursor"]
            .as_str()
            .context("Missing head cursor")?
            .into(),
    );
    frame::parse_cursor(&after)?;
    let mut events = base.join("v1/events")?;
    events.query_pairs_mut().append_pair("after", &after);
    let response = client
        .get(events)
        .bearer_auth(key)
        .send()
        .await?
        .error_for_status()?;
    ensure!(
        response
            .headers()
            .get("content-type")
            .and_then(|s| s.to_str().ok())
            .is_some_and(|s| s.starts_with("text/event-stream")),
        "Wrong event content type"
    );
    let mut input = response.bytes_stream();
    let mut sse = Sse::default();
    let mut previous: Option<Chunk> = None;
    let mut chunks = 0;
    while let Some(part) = input.next().await {
        let part = part?;
        ensure!(
            sse.pending.len() + part.len() <= 2 * frame::MAX_META,
            "sse_read_too_large"
        );
        sse.pending.extend_from_slice(&part);
        while let Some(end) = sse.pending.iter().position(|b| *b == b'\n') {
            let line = sse.pending.split_to(end + 1);
            let Some((event, id, data)) = sse.line(&line[..end])? else {
                continue;
            };
            let value: Value = serde_json::from_str(&data)?;
            match event.as_str() {
                "gap" => anyhow::bail!("Replay unavailable: {value}"),
                "status" => {
                    ensure!(value["state"] != "FAILED", "Source failed: {value}");
                    continue;
                }
                "chunk" => {}
                _ => continue,
            }
            let notice: Notice = serde_json::from_value(value)?;
            ensure!(
                notice.cursor == id && notice.bytes <= frame::MAX_FRAME,
                "Invalid notice"
            );
            let (_, _, digest) = frame::parse_cursor(&notice.cursor)?;
            ensure!(
                digest == notice.sha256
                    && digest != "-"
                    && notice.url == format!("/v1/chunks/{digest}"),
                "Invalid chunk URL"
            );
            // The exact relative path keeps the credential on the selected origin.
            let wire = bounded(
                client
                    .get(base.join(&notice.url)?)
                    .bearer_auth(key)
                    .send()
                    .await?,
                frame::MAX_FRAME,
            )
            .await?;
            ensure!(wire.len() == notice.bytes, "Chunk length mismatch");
            let chunk = frame::decode(wire.into())?;
            ensure!(
                chunk.cursor == notice.cursor
                    && chunk.digest == notice.sha256
                    && chunk.first_slot == notice.start_slot
                    && chunk.last_slot == notice.end_slot,
                "Chunk notice mismatch"
            );
            let (session, last, _) = frame::parse_cursor(&after)?;
            ensure!(
                chunk.session == session
                    && frame::processed_cursor(&after)
                        == (chunk.envelope.profile == frame::PROCESSED)
                    && (if chunk.envelope.profile == frame::PROCESSED {
                        chunk.position() == last + 1
                    } else {
                        chunk.position() > last
                    }),
                "Session or cursor order changed"
            );
            if let Some(prev) = &previous {
                frame::follows(prev, &chunk)?;
            }
            let options = blockzilla_stream_decoder::Options {
                include_details: false,
                ..Default::default()
            };
            let control = chunk
                .envelope
                .event
                .as_ref()
                .is_some_and(|e| e.kind != "block");
            let decoded: Value = if control {
                json!({"blocks":[],"transactionCount":0,"canonical":false,"publicationReady":false})
            } else {
                serde_json::from_slice(&blockzilla_stream_decoder::decode(&chunk.wire, &options)?)?
            };
            println!(
                "{}",
                json!({"state":"LIVE","cursor":chunk.cursor,"slot":chunk.last_slot,
                "event":chunk.envelope.event,"profile":chunk.envelope.profile,
                "blocks":decoded["blocks"].as_array().context("Missing blocks")?.len(),
                "transactions":decoded["transactionCount"],"wireBytes":chunk.wire.len(),
                "canonical":decoded["canonical"],"publicationReady":decoded["publicationReady"]})
            );
            // Probe only: a real application must commit its data and cursor together.
            after = chunk.cursor.clone();
            previous = Some(chunk);
            chunks += 1;
            if chunks == limit {
                return Ok(());
            }
        }
        ensure!(sse.pending.len() <= frame::MAX_META, "sse_line_too_large");
    }
    anyhow::bail!("Stream ended before the requested chunk count; last probe cursor: {after}")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sse_multiline_comments_and_crlf() {
        let mut s = Sse::default();
        for line in [
            b": ping\r".as_slice(),
            b"event: chunk\r",
            b"id: position\r",
            b"data: first\r",
            b"data: second\r",
        ] {
            assert!(s.line(line).unwrap().is_none());
        }
        assert_eq!(
            s.line(b"\r").unwrap(),
            Some(("chunk".into(), "position".into(), "first\nsecond".into()))
        );
        assert!(s.line(b"").unwrap().is_none());
    }
    #[test]
    fn reject_large_event_and_invalid_id() {
        assert!(Sse::default().line(&vec![b'x'; frame::MAX_META]).is_err());
        assert!(Sse::default().line(b"id: bad\0id").is_err());
    }
    #[test]
    fn restrict_base_url() {
        assert!(base_url("https://stream.blockzilla.dev").is_ok());
        for url in [
            "http://stream.blockzilla.dev",
            "https://key@stream.blockzilla.dev",
            "https://stream.blockzilla.dev/v1/",
            "https://stream.blockzilla.dev/?key=secret",
        ] {
            assert!(base_url(url).is_err());
        }
    }
}
