use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use clap::{Args, ValueEnum};
use reqwest::{
    Client, StatusCode, Url,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use sha2::{Digest, Sha256};
use std::{env, sync::Arc};
use time::{OffsetDateTime, macros::format_description};

#[derive(Debug, Clone, Args)]
pub struct SourceArgs {
    /// Object source type. HTTP works with files.old-faithful.net, public buckets, and authenticated HTTP.
    #[arg(long, value_enum, default_value_t = SourceKind::Http)]
    pub source_kind: SourceKind,

    /// Base URL for HTTP mode, for example https://files.old-faithful.net.
    #[arg(long, default_value = "https://files.old-faithful.net")]
    pub base_url: String,

    /// Extra HTTP header as NAME=VALUE. May be repeated.
    #[arg(long = "header")]
    pub header: Vec<String>,

    /// Read a bearer token from this environment variable and send Authorization: Bearer <token>.
    #[arg(long)]
    pub bearer_token_env: Option<String>,

    /// S3-compatible endpoint, for example https://s3.us-west-004.backblazeb2.com or https://<account>.r2.cloudflarestorage.com.
    #[arg(long)]
    pub s3_endpoint: Option<String>,

    /// S3 bucket name for S3-compatible mode.
    #[arg(long)]
    pub s3_bucket: Option<String>,

    /// S3 signing region. Use auto for Cloudflare R2.
    #[arg(long, default_value = "auto")]
    pub s3_region: String,

    /// Optional object key prefix before Old Faithful paths.
    #[arg(long, default_value = "")]
    pub s3_key_prefix: String,

    #[arg(long, default_value = "AWS_ACCESS_KEY_ID")]
    pub s3_access_key_id_env: String,

    #[arg(long, default_value = "AWS_SECRET_ACCESS_KEY")]
    pub s3_secret_access_key_env: String,

    #[arg(long)]
    pub s3_session_token_env: Option<String>,
}

impl SourceArgs {
    pub fn into_source(self) -> Result<Source> {
        let headers = self
            .header
            .iter()
            .cloned()
            .map(HttpHeader::parse)
            .collect::<Result<Vec<_>>>()?;
        Source::from_args(self, headers)
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SourceKind {
    Http,
    S3,
}

#[derive(Debug, Clone)]
pub struct HttpHeader {
    name: HeaderName,
    value: HeaderValue,
}

impl HttpHeader {
    pub fn parse(value: String) -> Result<Self> {
        let Some((name, raw_value)) = value.split_once('=') else {
            bail!("header must be NAME=VALUE");
        };
        Ok(Self {
            name: HeaderName::from_bytes(name.as_bytes())
                .with_context(|| format!("invalid header name {name}"))?,
            value: HeaderValue::from_str(raw_value)
                .with_context(|| format!("invalid value for header {name}"))?,
        })
    }
}

#[derive(Clone)]
pub struct Source {
    inner: Arc<dyn ObjectSource>,
}

impl Source {
    pub fn from_args(args: SourceArgs, headers: Vec<HttpHeader>) -> Result<Self> {
        let client = Client::builder()
            .http1_only()
            .tcp_nodelay(true)
            .build()
            .context("build HTTP client")?;

        let mut header_map = HeaderMap::new();
        for header in headers {
            header_map.insert(header.name, header.value);
        }
        if let Some(env_name) = &args.bearer_token_env {
            let token = env::var(env_name).with_context(|| format!("read ${env_name}"))?;
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .with_context(|| format!("build Authorization header from ${env_name}"))?;
            header_map.insert(reqwest::header::AUTHORIZATION, value);
        }

        let inner: Arc<dyn ObjectSource> = match args.source_kind {
            SourceKind::Http => Arc::new(HttpSource::new(args.base_url, header_map, client)?),
            SourceKind::S3 => Arc::new(S3Source::new(args, client)?),
        };
        Ok(Self { inner })
    }

    pub async fn get_text(&self, path: &str) -> Result<String> {
        self.inner.get_text(path).await
    }

    pub async fn get_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.inner.get_range(path, offset, len).await
    }
}

#[async_trait]
trait ObjectSource: Send + Sync {
    async fn get_text(&self, path: &str) -> Result<String> {
        let bytes = self.get_bytes(path).await?;
        String::from_utf8(bytes).with_context(|| format!("decode {path} as UTF-8"))
    }

    async fn get_bytes(&self, path: &str) -> Result<Vec<u8>>;

    async fn get_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>>;
}

struct HttpSource {
    base_url: String,
    headers: HeaderMap,
    client: Client,
}

impl HttpSource {
    fn new(base_url: String, headers: HeaderMap, client: Client) -> Result<Self> {
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            bail!("HTTP base URL must start with http:// or https://");
        }
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            headers,
            client,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }

    fn apply_headers(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        self.headers.iter().fold(request, |request, (name, value)| {
            request.header(name, value)
        })
    }
}

#[async_trait]
impl ObjectSource for HttpSource {
    async fn get_bytes(&self, path: &str) -> Result<Vec<u8>> {
        let url = self.url(path);
        let response = self
            .apply_headers(self.client.get(&url))
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        if response.status() != StatusCode::OK {
            bail!("{url} returned HTTP {}", response.status());
        }
        Ok(response.bytes().await?.to_vec())
    }

    async fn get_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(len as u64)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| anyhow!("range end overflow for {path}"))?;
        let range = format!("bytes={offset}-{end}");
        let url = self.url(path);
        let response = self
            .apply_headers(self.client.get(&url).header(reqwest::header::RANGE, &range))
            .send()
            .await
            .with_context(|| format!("GET {url} {range}"))?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            bail!(
                "{url} returned HTTP {} for {}, expected 206",
                response.status(),
                range
            );
        }
        let bytes = response.bytes().await?.to_vec();
        if bytes.len() != len {
            bail!(
                "{url} returned {} bytes for {}, expected {len}",
                bytes.len(),
                range
            );
        }
        Ok(bytes)
    }
}

struct S3Source {
    endpoint: String,
    host: String,
    bucket: String,
    region: String,
    key_prefix: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    client: Client,
}

impl S3Source {
    fn new(args: SourceArgs, client: Client) -> Result<Self> {
        let endpoint = args
            .s3_endpoint
            .ok_or_else(|| anyhow!("--s3-endpoint is required for --source-kind s3"))?;
        let bucket = args
            .s3_bucket
            .ok_or_else(|| anyhow!("--s3-bucket is required for --source-kind s3"))?;
        let parsed = Url::parse(endpoint.trim_end_matches('/'))
            .with_context(|| format!("parse S3 endpoint {endpoint}"))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow!("S3 endpoint must include a host"))?
            .to_string();
        let host = match parsed.port() {
            Some(port) => format!("{host}:{port}"),
            None => host,
        };

        let access_key = env::var(&args.s3_access_key_id_env)
            .with_context(|| format!("read ${}", args.s3_access_key_id_env))?;
        let secret_key = env::var(&args.s3_secret_access_key_env)
            .with_context(|| format!("read ${}", args.s3_secret_access_key_env))?;
        let session_token = args
            .s3_session_token_env
            .as_ref()
            .map(env::var)
            .transpose()
            .context("read S3 session token env")?;

        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            host,
            bucket,
            region: args.s3_region,
            key_prefix: args.s3_key_prefix.trim_matches('/').to_string(),
            access_key,
            secret_key,
            session_token,
            client,
        })
    }

    fn key(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        if self.key_prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{path}", self.key_prefix)
        }
    }

    fn signed_get(&self, path: &str, range: Option<&str>) -> Result<reqwest::RequestBuilder> {
        let key = self.key(path);
        let canonical_uri = format!(
            "/{}/{}",
            uri_encode_path_segment(&self.bucket),
            uri_encode_path(&key)
        );
        let url = format!("{}{}", self.endpoint, canonical_uri);
        let (date, amz_date) = amz_dates()?;

        let mut headers = vec![
            ("host".to_string(), self.host.clone()),
            (
                "x-amz-content-sha256".to_string(),
                "UNSIGNED-PAYLOAD".to_string(),
            ),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(range) = range {
            headers.push(("range".to_string(), range.to_string()));
        }
        if let Some(token) = &self.session_token {
            headers.push(("x-amz-security-token".to_string(), token.clone()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_headers = headers
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect::<String>();
        let signed_headers = headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_request = format!(
            "GET\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD"
        );
        let credential_scope = format!("{date}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            hex_lower(&Sha256::digest(canonical_request.as_bytes()))
        );
        let signing_key = signing_key(&self.secret_key, &date, &self.region);
        let signature = hex_lower(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key
        );

        let mut request = self
            .client
            .get(url)
            .header("host", &self.host)
            .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
            .header("x-amz-date", amz_date)
            .header("authorization", authorization);
        if let Some(range) = range {
            request = request.header(reqwest::header::RANGE, range);
        }
        if let Some(token) = &self.session_token {
            request = request.header("x-amz-security-token", token);
        }
        Ok(request)
    }
}

#[async_trait]
impl ObjectSource for S3Source {
    async fn get_bytes(&self, path: &str) -> Result<Vec<u8>> {
        let response = self
            .signed_get(path, None)?
            .send()
            .await
            .with_context(|| format!("S3 GET {path}"))?;
        if response.status() != StatusCode::OK {
            bail!("S3 GET {path} returned HTTP {}", response.status());
        }
        Ok(response.bytes().await?.to_vec())
    }

    async fn get_range(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(len as u64)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| anyhow!("range end overflow for {path}"))?;
        let range = format!("bytes={offset}-{end}");
        let response = self
            .signed_get(path, Some(&range))?
            .send()
            .await
            .with_context(|| format!("S3 GET {path} {range}"))?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            bail!(
                "S3 GET {path} returned HTTP {} for {}, expected 206",
                response.status(),
                range
            );
        }
        let bytes = response.bytes().await?.to_vec();
        if bytes.len() != len {
            bail!(
                "S3 GET {path} returned {} bytes for {}, expected {len}",
                bytes.len(),
                range
            );
        }
        Ok(bytes)
    }
}

fn amz_dates() -> Result<(String, String)> {
    let now = OffsetDateTime::now_utc();
    let date = now.format(format_description!("[year][month][day]"))?;
    let amz_date = now.format(format_description!(
        "[year][month][day]T[hour][minute][second]Z"
    ))?;
    Ok((date, amz_date))
}

fn signing_key(secret_key: &str, date: &str, region: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut key_block = [0u8; 64];
    if key.len() > 64 {
        key_block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut inner_pad = [0x36u8; 64];
    let mut outer_pad = [0x5cu8; 64];
    for i in 0..64 {
        inner_pad[i] ^= key_block[i];
        outer_pad[i] ^= key_block[i];
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_hash);
    let out = outer.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&out);
    bytes
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn uri_encode_path(path: &str) -> String {
    path.split('/')
        .map(uri_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn uri_encode_path_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => {
                out.push('%');
                out.push(char::from(b"0123456789ABCDEF"[(byte >> 4) as usize]));
                out.push(char::from(b"0123456789ABCDEF"[(byte & 0x0f) as usize]));
            }
        }
    }
    out
}
