use crate::{frame, source::bounded_file, store::Store};
use anyhow::{Result, ensure};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use serde_json::Value;
use std::{path::PathBuf, sync::Arc, time::Duration};

#[derive(Clone)]
pub struct Upstream {
    pub url: String,
    pub token: String,
    pub ca: Option<PathBuf>,
    pub identity: Option<PathBuf>,
}
impl Upstream {
    pub fn client(&self) -> Result<reqwest::Client> {
        let u = reqwest::Url::parse(&self.url)?;
        ensure!(
            u.path() == "/"
                && u.query().is_none()
                && u.fragment().is_none()
                && u.username().is_empty()
                && u.password().is_none(),
            "upstream_requires_base_url"
        );
        let loopback = u.host_str().is_some_and(|s| {
            s.parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
        });
        ensure!(
            u.scheme() == "https" || (u.scheme() == "http" && loopback),
            "upstream_requires_https"
        );
        ensure!(
            loopback || (self.ca.is_some() && self.identity.is_some()),
            "remote_upstream_requires_mtls"
        );
        let mut b = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .read_timeout(Duration::from_secs(30))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .http2_keep_alive_timeout(Duration::from_secs(5))
            .http2_keep_alive_while_idle(true);
        if let Some(p) = &self.ca {
            b = b.add_root_certificate(reqwest::Certificate::from_pem(&bounded_file(p, 65536)?)?);
        }
        if let Some(p) = &self.identity {
            b = b.identity(reqwest::Identity::from_pem(&bounded_file(p, 65536)?)?);
        }
        Ok(b.build()?)
    }
    pub async fn head(&self, client: &reqwest::Client) -> Result<Value> {
        let r = client
            .get(format!("{}v1/head", self.url))
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?;
        let mut bytes = Vec::new();
        let mut input = r.bytes_stream();
        while let Some(b) = input.next().await {
            let b = b?;
            ensure!(bytes.len() + b.len() <= 65536, "head_too_large");
            bytes.extend_from_slice(&b);
        }
        let v: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            v["schema"] == "blockzilla-stream-head-v1",
            "unsupported_head"
        );
        Ok(v)
    }
    async fn receive(&self, client: &reqwest::Client, store: &Arc<Store>) -> Result<()> {
        let after = store.latest();
        let mut url = reqwest::Url::parse(&format!("{}v1/blocks", self.url))?;
        url.query_pairs_mut().append_pair("after", &after);
        let r = client.get(url).bearer_auth(&self.token).send().await?;
        ensure!(
            !matches!(r.status().as_u16(), 400 | 401 | 403 | 410),
            "upstream_terminal_http_{}",
            r.status().as_u16()
        );
        let r = r.error_for_status()?;
        store.state.lock().unwrap().upstream_http_version = Some(format!("{:?}", r.version()));
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
                == Some("application/vnd.blockzilla.stream.v1"),
            "upstream_content_type_mismatch"
        );
        let mut input = r.bytes_stream();
        let mut pending = BytesMut::new();
        while let Some(data) = input.next().await {
            let data = data?;
            ensure!(
                pending.len() + data.len() <= 2 * frame::MAX_FRAME,
                "upstream_read_too_large"
            );
            pending.extend_from_slice(&data);
            while pending.len() >= 16 {
                let n = frame::frame_length(&pending)?;
                if pending.len() < n {
                    break;
                }
                let c = frame::decode(Bytes::copy_from_slice(&pending.split_to(n)))?;
                let s = store.clone();
                tokio::task::spawn_blocking(move || s.admit(c)).await??;
            }
        }
        ensure!(pending.is_empty(), "upstream_truncated_frame");
        anyhow::bail!("upstream_closed")
    }
    pub async fn run(self, client: reqwest::Client, store: Arc<Store>) {
        let mut delay = 1;
        loop {
            let before = store.latest();
            if let Err(e) = self.receive(&client, &store).await {
                let msg = e.to_string();
                // Protocol, identity, digest and replay errors require operator action.
                let mut terminal = !(e.downcast_ref::<reqwest::Error>().is_some()
                    || matches!(msg.as_str(), "upstream_closed" | "upstream_truncated_frame"));
                if !terminal {
                    if let Ok(head) = self.head(&client).await {
                        terminal = head["state"] == "FAILED";
                    }
                }
                store.set_status(if terminal { "FAILED" } else { "RECONNECTING" }, Some(msg));
                if terminal {
                    return;
                }
            }
            if store.latest() != before {
                delay = 1;
            }
            tokio::time::sleep(Duration::from_secs(delay)).await;
            delay = (delay * 2).min(30);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn no_cleartext_remote_or_redirect_credentials() {
        let u = |url: &str| Upstream {
            url: url.into(),
            token: "secret".into(),
            ca: None,
            identity: None,
        };
        assert!(u("http://example.com/").client().is_err());
        assert!(u("https://example.com/").client().is_err());
        assert!(u("http://127.0.0.1/?key=secret").client().is_err());
        assert!(u("http://127.0.0.1/").client().is_ok());
    }
}
