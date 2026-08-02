use super::config::{Provider, StorageSettings};
use super::{
    ApiError, MAX_API_ERROR_BODY_BYTES, MAX_RETRIES, Result, UploaderError, api_error_code,
};
use chrono::Utc;
use reqwest::Method;
use reqwest::blocking::{Body, Client, Response};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use url::Url;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const RETRYABLE: &[u16] = &[429, 500, 502, 503, 504];
const RESERVED_CALLER_HEADERS: &[&str] = &[
    "authorization",
    "connection",
    "content-length",
    "expect",
    "host",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "x-amz-content-sha256",
    "x-amz-date",
    "x-amz-security-token",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    len: u64,
    modified_secs: i64,
    modified_nanos: i64,
    changed_secs: i64,
    changed_nanos: i64,
}

#[derive(Clone)]
pub struct FilePayload {
    file: Arc<File>,
    path: PathBuf,
    identity: FileIdentity,
}

impl fmt::Debug for FilePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FilePayload")
            .field("len", &self.identity.len)
            .finish()
    }
}

impl FilePayload {
    /// Open once without following the final symlink. Every upload attempt
    /// reads this held descriptor from byte zero and verifies that the original
    /// pathname still names the same unchanged inode before and after sending.
    pub fn open(path: &Path) -> Result<Self> {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let before = fs::symlink_metadata(&path)?;
        if before.file_type().is_symlink() || !before.is_file() {
            return Err(config(
                "upload source must be a regular file, not a symlink",
            ));
        }
        let identity = file_identity(&before);
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&path)?;
        if file_identity(&file.metadata()?) != identity {
            return Err(protocol("upload source changed while opening"));
        }
        Ok(Self {
            file: Arc::new(file),
            path,
            identity,
        })
    }

    pub fn len(&self) -> u64 {
        self.identity.len
    }

    pub fn is_empty(&self) -> bool {
        self.identity.len == 0
    }

    pub(crate) fn reader(&self) -> HeldFileReader {
        HeldFileReader {
            file: Arc::clone(&self.file),
            offset: 0,
            len: self.identity.len,
        }
    }

    pub(crate) fn verify(&self, phase: &str) -> Result<()> {
        let descriptor = self.file.metadata()?;
        let path = fs::symlink_metadata(&self.path).map_err(|error| {
            protocol(format!(
                "upload source path is unavailable {phase}: {error}"
            ))
        })?;
        if path.file_type().is_symlink()
            || !path.is_file()
            || file_identity(&descriptor) != self.identity
            || file_identity(&path) != self.identity
        {
            return Err(protocol(format!("upload source changed {phase}")));
        }
        Ok(())
    }
}

pub(crate) struct HeldFileReader {
    file: Arc<File>,
    offset: u64,
    len: u64,
}

impl Read for HeldFileReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.offset >= self.len || buffer.is_empty() {
            return Ok(0);
        }
        let maximum = usize::try_from((self.len - self.offset).min(buffer.len() as u64))
            .expect("bounded by buffer length");
        let count = self.file.read_at(&mut buffer[..maximum], self.offset)?;
        self.offset = self
            .offset
            .checked_add(count as u64)
            .ok_or_else(|| std::io::Error::other("upload source offset overflow"))?;
        Ok(count)
    }
}

fn file_identity(metadata: &fs::Metadata) -> FileIdentity {
    FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        len: metadata.len(),
        modified_secs: metadata.mtime(),
        modified_nanos: metadata.mtime_nsec(),
        changed_secs: metadata.ctime(),
        changed_nanos: metadata.ctime_nsec(),
    }
}

#[derive(Clone)]
pub enum Payload {
    Empty,
    Bytes(Vec<u8>),
    File(FilePayload),
}

impl fmt::Debug for Payload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("Payload::Empty"),
            Self::Bytes(bytes) => formatter
                .debug_struct("Payload::Bytes")
                .field("len", &bytes.len())
                .finish(),
            Self::File(source) => formatter
                .debug_struct("Payload::File")
                .field("len", &source.len())
                .finish(),
        }
    }
}

impl Payload {
    fn body(&self) -> Result<Body> {
        match self {
            Self::Empty => Ok(Body::from(Vec::new())),
            Self::Bytes(bytes) => Ok(Body::from(bytes.clone())),
            Self::File(source) => Ok(Body::sized(source.reader(), source.len())),
        }
    }

    fn verify(&self, phase: &str) -> Result<()> {
        match self {
            Self::File(source) => source.verify(phase),
            Self::Empty | Self::Bytes(_) => Ok(()),
        }
    }
}

pub struct S3Response {
    pub status: u16,
    pub headers: HeaderMap,
    response: Response,
}

impl S3Response {
    pub fn exact_header(&self, name: &str, operation: &str) -> Result<&str> {
        exact_header_value(&self.headers, name, operation)
    }

    pub fn exact_content_length(&self, operation: &str) -> Result<u64> {
        let values = self.headers.get_all("content-length");
        let mut iter = values.iter();
        let value = iter
            .next()
            .ok_or_else(|| protocol(format!("{operation} is missing an exact Content-Length")))?;
        if iter.next().is_some() {
            return Err(protocol(format!(
                "{operation} returned multiple Content-Length values"
            )));
        }
        let text = value
            .to_str()
            .map_err(|_| protocol(format!("{operation} returned invalid Content-Length")))?;
        if text.is_empty() || text.len() > 20 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(protocol(format!(
                "{operation} returned invalid Content-Length"
            )));
        }
        text.parse()
            .map_err(|_| protocol(format!("{operation} returned invalid Content-Length")))
    }

    pub fn read_bounded(mut self, maximum: usize, operation: &str) -> Result<Vec<u8>> {
        let length = self.exact_content_length(operation)?;
        if length > maximum as u64 {
            return Err(protocol(format!(
                "{operation} exceeds the bounded response limit"
            )));
        }
        let mut bytes = Vec::with_capacity(length as usize);
        self.response
            .by_ref()
            .take(maximum as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() != length as usize {
            return Err(protocol(format!(
                "{operation} body length does not match Content-Length"
            )));
        }
        Ok(bytes)
    }

    pub fn into_reader(self) -> Response {
        self.response
    }
}

#[derive(Clone)]
pub struct S3Client {
    endpoint: String,
    host: String,
    pub bucket: String,
    pub provider: Provider,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    retries: u32,
    client: Client,
}

impl fmt::Debug for S3Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3Client")
            .field("endpoint", &"<redacted>")
            .field("bucket", &self.bucket)
            .field("provider", &self.provider)
            .field("region", &self.region)
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("retries", &self.retries)
            .finish()
    }
}

impl S3Client {
    pub fn new(settings: StorageSettings, retries: u32) -> Result<Self> {
        if retries > MAX_RETRIES {
            return Err(config(format!("retry count must be at most {MAX_RETRIES}")));
        }
        let parsed = Url::parse(&settings.endpoint)
            .map_err(|_| config("S3 endpoint must be an absolute HTTP(S) URL"))?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(config("S3 endpoint must be an absolute HTTP(S) URL"));
        }
        if parsed.scheme() != "https"
            && !matches!(parsed.host_str(), Some("127.0.0.1" | "::1" | "localhost"))
        {
            return Err(config("S3 endpoint must use HTTPS"));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(config("S3 endpoint must not contain user information"));
        }
        if !matches!(parsed.path(), "" | "/")
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(config(
                "S3 endpoint must not contain a path, query, or fragment",
            ));
        }
        if settings.bucket.is_empty()
            || settings.bucket.contains('/')
            || settings.bucket.chars().any(|value| value < '!')
        {
            return Err(config("S3 bucket name is invalid"));
        }
        if settings.provider == Provider::R2 && settings.region != "auto" {
            return Err(config("Cloudflare R2 client region must be auto"));
        }
        if settings.session_token.as_ref().is_some_and(|value| {
            value.is_empty() || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        }) {
            return Err(config("S3 session token is invalid"));
        }
        let mut host = parsed.host_str().expect("checked").to_string();
        if host.contains(':') && !host.starts_with('[') {
            host = format!("[{host}]");
        }
        if let Some(port) = parsed.port() {
            host.push(':');
            host.push_str(&port.to_string());
        }
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(300))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()?;
        Ok(Self {
            endpoint: settings.endpoint.trim_end_matches('/').into(),
            host,
            bucket: settings.bucket,
            provider: settings.provider,
            region: settings.region,
            access_key: settings.access_key,
            secret_key: settings.secret_key,
            session_token: settings.session_token,
            retries,
            client,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[allow(clippy::too_many_arguments)]
    pub fn request(
        &self,
        method: Method,
        key: &str,
        params: &BTreeMap<String, String>,
        headers: &BTreeMap<String, String>,
        payload_hash: &str,
        payload: &Payload,
        allowed_statuses: &[u16],
    ) -> Result<S3Response> {
        validate_key(key)?;
        validate_sha256(payload_hash, "request payload SHA-256")?;
        validate_caller_headers(headers)?;
        let mut last = None;
        for attempt in 1..=self.retries.saturating_add(1) {
            match self.request_once(method.clone(), key, params, headers, payload_hash, payload) {
                Ok(response)
                    if response.status().is_success()
                        || allowed_statuses.contains(&response.status().as_u16()) =>
                {
                    let status = response.status().as_u16();
                    let headers = response.headers().clone();
                    return Ok(S3Response {
                        status,
                        headers,
                        response,
                    });
                }
                Ok(response) => {
                    let status = response.status().as_u16();
                    let code = bounded_error_code(response);
                    let error = ApiError {
                        operation: format!("{} {key}", method.as_str()),
                        status,
                        code,
                    };
                    if capacity(&error.code) || !RETRYABLE.contains(&status) {
                        return Err(error.into());
                    }
                    last = Some(UploaderError::Api(error));
                    if attempt <= self.retries {
                        let delay = retry_delay(attempt);
                        eprintln!(
                            "retry {attempt}/{} after HTTP {status} for {} {key}; sleep={}s",
                            self.retries,
                            method.as_str(),
                            delay.as_secs()
                        );
                        thread::sleep(delay);
                    }
                }
                Err(error) => {
                    if !retryable_transport(&error) {
                        return Err(error);
                    }
                    last = Some(error);
                    if attempt <= self.retries {
                        let delay = retry_delay(attempt);
                        eprintln!(
                            "retry {attempt}/{} after transport error for {} {key}; sleep={}s",
                            self.retries,
                            method.as_str(),
                            delay.as_secs()
                        );
                        thread::sleep(delay);
                    }
                }
            }
        }
        Err(last.unwrap_or_else(|| protocol("S3 request failed without a response")))
    }

    fn request_once(
        &self,
        method: Method,
        key: &str,
        params: &BTreeMap<String, String>,
        headers: &BTreeMap<String, String>,
        payload_hash: &str,
        payload: &Payload,
    ) -> Result<Response> {
        payload.verify("before upload attempt")?;
        let now = Utc::now();
        let date = now.format("%Y%m%d").to_string();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let canonical_uri = format!(
            "/{}/{}",
            uri_encode(&self.bucket, false),
            uri_encode(key.trim_start_matches('/'), true)
        );
        let canonical_query = params
            .iter()
            .map(|(key, value)| format!("{}={}", uri_encode(key, false), uri_encode(value, false)))
            .collect::<Vec<_>>()
            .join("&");
        let mut signed = BTreeMap::from([
            ("host".to_string(), self.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.into()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ]);
        for (name, value) in headers {
            signed.insert(name.clone(), value.trim().to_string());
        }
        if let Some(token) = &self.session_token {
            signed.insert("x-amz-security-token".into(), token.clone());
        }
        let canonical_headers = signed
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect::<String>();
        let signed_names = signed.keys().cloned().collect::<Vec<_>>().join(";");
        let canonical_request = format!(
            "{}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_names}\n{payload_hash}",
            method.as_str()
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signature = hex::encode(hmac_sha256(
            &signing_key(&self.secret_key, &date, &self.region),
            string_to_sign.as_bytes(),
        ));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope},SignedHeaders={signed_names},Signature={signature}",
            self.access_key
        );
        let mut url = format!("{}{}", self.endpoint, canonical_uri);
        if !canonical_query.is_empty() {
            url.push('?');
            url.push_str(&canonical_query);
        }
        let mut request = self.client.request(method, url).body(payload.body()?);
        for (name, value) in signed {
            request = request.header(
                HeaderName::from_bytes(name.as_bytes())
                    .map_err(|_| config("invalid S3 header name"))?,
                HeaderValue::from_str(&value).map_err(|_| config("invalid S3 header value"))?,
            );
        }
        request = request.header("authorization", authorization);
        let response = request.send();
        payload.verify("after upload attempt")?;
        Ok(response?)
    }

    pub fn head(&self, key: &str) -> Result<S3Response> {
        self.head_with_params(key, &BTreeMap::new())
    }

    pub fn head_with_params(
        &self,
        key: &str,
        params: &BTreeMap<String, String>,
    ) -> Result<S3Response> {
        self.request(
            Method::HEAD,
            key,
            params,
            &BTreeMap::new(),
            EMPTY_SHA256,
            &Payload::Empty,
            &[404],
        )
    }

    pub fn get(&self, key: &str) -> Result<S3Response> {
        self.get_with_params(key, &BTreeMap::new())
    }

    pub fn get_with_params(
        &self,
        key: &str,
        params: &BTreeMap<String, String>,
    ) -> Result<S3Response> {
        let headers = BTreeMap::from([("accept-encoding".into(), "identity".into())]);
        self.request(
            Method::GET,
            key,
            params,
            &headers,
            EMPTY_SHA256,
            &Payload::Empty,
            &[],
        )
    }

    pub fn put(
        &self,
        key: &str,
        content_type: &str,
        sha256: &str,
        md5_base64: Option<&str>,
        if_none_match: bool,
        payload: &Payload,
    ) -> Result<S3Response> {
        let mut headers = BTreeMap::from([
            ("content-type".into(), content_type.into()),
            ("x-amz-meta-sha256".into(), sha256.into()),
        ]);
        if let Some(md5) = md5_base64 {
            headers.insert("content-md5".into(), md5.into());
        }
        if if_none_match {
            headers.insert("if-none-match".into(), "*".into());
        }
        self.request(
            Method::PUT,
            key,
            &BTreeMap::new(),
            &headers,
            sha256,
            payload,
            if if_none_match { &[412] } else { &[] },
        )
    }

    pub fn delete(&self, key: &str) -> Result<S3Response> {
        self.request(
            Method::DELETE,
            key,
            &BTreeMap::new(),
            &BTreeMap::new(),
            EMPTY_SHA256,
            &Payload::Empty,
            &[404],
        )
    }
}

fn validate_caller_headers(headers: &BTreeMap<String, String>) -> Result<()> {
    for (name, value) in headers {
        let normalized = name.to_ascii_lowercase();
        if RESERVED_CALLER_HEADERS.contains(&normalized.as_str()) {
            return Err(config(format!(
                "caller cannot set reserved S3 header {normalized}"
            )));
        }
        if normalized != *name {
            return Err(config("S3 signed header names must be lowercase"));
        }
        HeaderName::from_bytes(name.as_bytes()).map_err(|_| config("invalid S3 header name"))?;
        if value.contains(['\r', '\n']) {
            return Err(config("S3 signed header value contains a line break"));
        }
        HeaderValue::from_str(value).map_err(|_| config("invalid S3 header value"))?;
    }
    Ok(())
}

fn retryable_transport(error: &UploaderError) -> bool {
    matches!(error, UploaderError::Http(source) if source.is_timeout()
        || source.is_connect()
        || (source.is_request() && !source.is_builder() && !source.is_body()))
}

pub(crate) fn validate_key(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with('/')
        || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
    {
        return Err(config(
            "object key must be non-empty, relative, and control-free",
        ));
    }
    Ok(())
}

pub(crate) fn validate_sha256(value: &str, label: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(config(format!(
            "{label} must be exactly 64 hexadecimal characters"
        )));
    }
    Ok(())
}

fn exact_header_value<'a>(headers: &'a HeaderMap, name: &str, operation: &str) -> Result<&'a str> {
    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| protocol(format!("{operation} requested an invalid header name")))?;
    let values = headers.get_all(&name);
    let mut iter = values.iter();
    let value = iter
        .next()
        .ok_or_else(|| protocol(format!("{operation} is missing required {name}")))?;
    if iter.next().is_some() {
        return Err(protocol(format!(
            "{operation} returned multiple {name} values"
        )));
    }
    value
        .to_str()
        .map_err(|_| protocol(format!("{operation} returned invalid {name}")))
}

fn uri_encode(value: &str, preserve_slash: bool) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_' | b'.' | b'~')
            || (preserve_slash && byte == b'/')
        {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(char::from(b"0123456789ABCDEF"[(byte >> 4) as usize]));
            encoded.push(char::from(b"0123456789ABCDEF"[(byte & 0xf) as usize]));
        }
    }
    encoded
}

fn signing_key(secret: &str, date: &str, region: &str) -> [u8; 32] {
    let date_key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, b"s3");
    hmac_sha256(&service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut key_block = [0u8; 64];
    if key.len() > key_block.len() {
        key_block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }
    let mut inner_pad = [0x36u8; 64];
    let mut outer_pad = [0x5cu8; 64];
    for index in 0..64 {
        inner_pad[index] ^= key_block[index];
        outer_pad[index] ^= key_block[index];
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner.finalize());
    outer.finalize().into()
}

fn bounded_error_code(mut response: Response) -> String {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_API_ERROR_BODY_BYTES as u64)
    {
        return String::new();
    }
    let mut bytes = Vec::new();
    if response
        .by_ref()
        .take(MAX_API_ERROR_BODY_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .is_err()
        || bytes.len() > MAX_API_ERROR_BODY_BYTES
    {
        return String::new();
    }
    api_error_code(&bytes)
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_secs(
        1u64.checked_shl(attempt.saturating_sub(1))
            .unwrap_or(60)
            .min(60),
    )
}

fn capacity(code: &str) -> bool {
    matches!(
        code,
        "download_cap_exceeded"
            | "transaction_cap_exceeded"
            | "storage_cap_exceeded"
            | "cap_exceeded"
    )
}

fn config(message: impl Into<String>) -> UploaderError {
    UploaderError::Config(message.into())
}

fn protocol(message: impl Into<String>) -> UploaderError {
    UploaderError::Protocol(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustix::fs::{Timespec, Timestamps};
    use sha2::Digest;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::os::unix::fs::OpenOptionsExt;
    use std::thread;
    use std::time::Duration;

    fn test_client(endpoint: String, retries: u32) -> S3Client {
        S3Client::new(
            StorageSettings {
                endpoint,
                region: "test".into(),
                bucket: "bucket".into(),
                access_key: "access".into(),
                secret_key: "secret".into(),
                provider: Provider::S3,
                session_token: None,
            },
            retries,
        )
        .unwrap()
    }

    fn read_request_body(stream: &mut std::net::TcpStream) -> Vec<u8> {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut buffer).unwrap();
            assert!(count > 0, "request ended before headers");
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                break index + 4;
            }
            assert!(bytes.len() <= 64 * 1024, "request headers are unbounded");
        };
        let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(str::trim)
                    .map(str::parse::<usize>)
            })
            .expect("content length")
            .unwrap();
        while bytes.len() - header_end < content_length {
            let count = stream.read(&mut buffer).unwrap();
            assert!(count > 0, "request body ended early");
            bytes.extend_from_slice(&buffer[..count]);
        }
        bytes[header_end..header_end + content_length].to_vec()
    }

    #[test]
    fn aws_uri_encoding_matches_path_rules() {
        assert_eq!(uri_encode("a b/c+d", true), "a%20b/c%2Bd");
        assert_eq!(uri_encode("a/b", false), "a%2Fb");
    }

    #[test]
    fn signing_key_matches_aws_documented_vector() {
        assert_eq!(
            hex::encode(signing_key(
                "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                "20150830",
                "us-east-1"
            )),
            "32f78051dcde24c552811d654f4a769112bb834b03975cdd6b1fd7d16248c269"
        );
    }

    #[test]
    fn signed_requests_never_follow_provider_redirects() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 8192];
            let count = stream.read(&mut request).unwrap();
            let request = std::str::from_utf8(&request[..count]).unwrap();
            assert!(request.starts_with("HEAD /bucket/key "));
            let lower = request.to_ascii_lowercase();
            assert!(lower.contains("x-amz-security-token: session\r\n"));
            let authorization = lower
                .lines()
                .find(|line| line.starts_with("authorization:"))
                .unwrap();
            assert!(authorization.contains("signedheaders="));
            assert!(authorization.contains("x-amz-security-token"));
            write!(
                stream,
                "HTTP/1.1 307 Redirect\r\nLocation: http://127.0.0.1:1/credential-sink\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
        });
        let client = S3Client::new(
            StorageSettings {
                endpoint,
                region: "test".into(),
                bucket: "bucket".into(),
                access_key: "access".into(),
                secret_key: "secret".into(),
                provider: Provider::S3,
                session_token: Some("session".into()),
            },
            0,
        )
        .unwrap();
        let error = match client.head("key") {
            Ok(_) => panic!("redirect unexpectedly followed or accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("HTTP 307"));
        thread.join().unwrap();
    }

    #[test]
    fn absurd_retry_count_is_rejected_before_range_overflow_or_hang() {
        let result = S3Client::new(
            StorageSettings {
                endpoint: "https://example.invalid".into(),
                region: "test".into(),
                bucket: "bucket".into(),
                access_key: "access".into(),
                secret_key: "secret".into(),
                provider: Provider::S3,
                session_token: None,
            },
            u32::MAX,
        );
        let error = match result {
            Ok(_) => panic!("absurd retry count accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("retry count"));
    }

    #[test]
    fn debug_output_never_exposes_credentials_or_payload_contents() {
        let client = S3Client::new(
            StorageSettings {
                endpoint: "https://example.invalid".into(),
                region: "test".into(),
                bucket: "bucket".into(),
                access_key: "s3-access-credential".into(),
                secret_key: "s3-secret-credential".into(),
                provider: Provider::S3,
                session_token: Some("s3-session-credential".into()),
            },
            0,
        )
        .unwrap();
        let bytes = Payload::Bytes(b"private-upload-body".to_vec());
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("private-upload-path");
        fs::write(&path, vec![0u8; 99]).unwrap();
        let file = Payload::File(FilePayload::open(&path).unwrap());

        let debug = format!("{client:?} {bytes:?} {file:?}");
        for private in [
            "s3-access-credential",
            "s3-secret-credential",
            "s3-session-credential",
            "example.invalid",
            "private-upload-body",
            "private-upload-path",
        ] {
            assert!(!debug.contains(private));
        }
        assert!(debug.contains("<redacted>"));
        assert!(debug.contains("len: 19"));
        assert!(debug.contains("len: 99"));
    }

    #[test]
    fn reserved_signing_and_framing_headers_are_rejected_before_network_io() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let client = test_client(format!("http://{}", listener.local_addr().unwrap()), 32);
        for name in [
            "Host",
            "authorization",
            "Content-Length",
            "Transfer-Encoding",
            "X-Amz-Date",
            "x-amz-content-sha256",
            "X-Amz-Security-Token",
        ] {
            let error = match client.request(
                Method::GET,
                "key",
                &BTreeMap::new(),
                &BTreeMap::from([(name.into(), "attacker-controlled".into())]),
                EMPTY_SHA256,
                &Payload::Empty,
                &[],
            ) {
                Ok(_) => panic!("reserved header unexpectedly reached the network"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("reserved S3 header"));
        }
        assert!(matches!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn held_file_detects_path_replacement_and_same_size_restored_mtime_mutation() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("payload");
        fs::write(&path, b"original").unwrap();
        let source = FilePayload::open(&path).unwrap();
        fs::rename(&path, temporary.path().join("held-inode")).unwrap();
        fs::write(&path, b"replaced").unwrap();
        assert!(source.verify("after path replacement").is_err());
        let mut reader = source.reader();
        let mut held = Vec::new();
        reader.read_to_end(&mut held).unwrap();
        assert_eq!(held, b"original");

        let path = temporary.path().join("mutated");
        fs::write(&path, b"original").unwrap();
        let source = FilePayload::open(&path).unwrap();
        let before = fs::metadata(&path).unwrap();
        thread::sleep(Duration::from_millis(5));
        let mut writer = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .unwrap();
        writer.write_all(b"mutated!").unwrap();
        writer.flush().unwrap();
        rustix::fs::futimens(
            &writer,
            &Timestamps {
                last_access: Timespec {
                    tv_sec: before.atime(),
                    tv_nsec: before.atime_nsec() as _,
                },
                last_modification: Timespec {
                    tv_sec: before.mtime(),
                    tv_nsec: before.mtime_nsec() as _,
                },
            },
        )
        .unwrap();
        assert!(source.verify("after same-size mutation").is_err());
    }

    #[test]
    fn held_file_retry_streams_complete_body_from_offset_zero() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("payload");
        let expected = (0..32_768u32)
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        fs::write(&path, &expected).unwrap();
        let payload = Payload::File(FilePayload::open(&path).unwrap());
        let digest = hex::encode(Sha256::digest(&expected));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let expected_for_server = expected.clone();
        let server = thread::spawn(move || {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                assert_eq!(read_request_body(&mut stream), expected_for_server);
                let status = if attempt == 0 {
                    "500 Internal Server Error"
                } else {
                    "200 OK"
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
            }
        });
        let client = test_client(endpoint, 1);
        let response = client
            .request(
                Method::PUT,
                "retry-body",
                &BTreeMap::new(),
                &BTreeMap::new(),
                &digest,
                &payload,
                &[],
            )
            .unwrap();
        assert_eq!(response.status, 200);
        server.join().unwrap();
    }

    #[test]
    fn deterministic_transport_builder_errors_are_not_retryable() {
        let source = reqwest::blocking::Client::new()
            .get("://invalid-url")
            .send()
            .unwrap_err();
        assert!(source.is_builder());
        assert!(!retryable_transport(&UploaderError::Http(source)));
        assert!(!retryable_transport(&UploaderError::Protocol(
            "deterministic".into()
        )));
    }

    #[test]
    fn exact_identity_headers_reject_duplicates_and_invalid_values() {
        for name in ["etag", "x-amz-meta-sha256", "x-amz-version-id"] {
            let mut headers = HeaderMap::new();
            let name = HeaderName::from_bytes(name.as_bytes()).unwrap();
            headers.append(name.clone(), HeaderValue::from_static("first"));
            headers.append(name.clone(), HeaderValue::from_static("second"));
            let error = exact_header_value(&headers, name.as_str(), "identity check").unwrap_err();
            assert!(error.to_string().contains("multiple"));

            let mut headers = HeaderMap::new();
            headers.insert(name.clone(), HeaderValue::from_bytes(b"\xff").unwrap());
            let error = exact_header_value(&headers, name.as_str(), "identity check").unwrap_err();
            assert!(error.to_string().contains("invalid"));

            let headers = HeaderMap::new();
            let error = exact_header_value(&headers, name.as_str(), "identity check").unwrap_err();
            assert!(error.to_string().contains("missing"));
        }
    }
}
