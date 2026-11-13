//! Send one archived epoch to Cloudflare R2.
//!
//! The process environment supplies `R2_ACCESS_KEY_ID`,
//! `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT` or `R2_S3_ENDPOINT`, `R2_BUCKET`,
//! and `R2_PREFIX`. The command uploads the eleven
//! published files and leaves a remote object in place when its size already
//! matches. A different remote size stops the command. Telegram runs only
//! after every object is confirmed.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use reqwest::Method;
use reqwest::blocking::{Body, Client};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};

use crate::ledger::EPOCH_ARCHIVE_FILES;

pub(crate) const R2_ACCESS_KEY_ID_ENV: &str = "R2_ACCESS_KEY_ID";
pub(crate) const R2_SECRET_ACCESS_KEY_ENV: &str = "R2_SECRET_ACCESS_KEY";
pub(crate) const R2_ENDPOINT_ENV: &str = "R2_ENDPOINT";
pub(crate) const R2_S3_ENDPOINT_ENV: &str = "R2_S3_ENDPOINT";
pub(crate) const R2_BUCKET_ENV: &str = "R2_BUCKET";
pub(crate) const R2_PREFIX_ENV: &str = "R2_PREFIX";
pub(crate) const R2_REGION_ENV: &str = "R2_REGION";
const DEFAULT_REGION: &str = "auto";
const PART_SIZE: u64 = 64 * 1024 * 1024;
const PARALLEL_UPLOADS: usize = 8;
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;
const ATTEMPTS: u32 = 4;

pub(crate) struct PublishSettings {
    pub(crate) endpoint: String,
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    region: String,
    access_key: String,
    secret_key: String,
}

pub(crate) fn resolve_settings(lookup: &dyn Fn(&str) -> Option<String>) -> Result<PublishSettings> {
    let endpoint = first_present(lookup, &[R2_ENDPOINT_ENV, R2_S3_ENDPOINT_ENV]);
    if endpoint.is_empty() {
        bail!("{R2_ENDPOINT_ENV} is empty");
    }
    validate_endpoint(&endpoint)?;
    let bucket = required(lookup, R2_BUCKET_ENV)?;
    let prefix = required(lookup, R2_PREFIX_ENV)?;
    let region = present_or(lookup, R2_REGION_ENV, DEFAULT_REGION);
    if region != DEFAULT_REGION {
        bail!("R2 region must be auto");
    }
    if bucket.is_empty() || bucket.contains('/') || bucket.chars().any(|value| value < '!') {
        bail!("R2 bucket name is invalid");
    }
    Ok(PublishSettings {
        endpoint: endpoint.trim_end_matches('/').to_string(),
        bucket,
        prefix: normalize_prefix(&prefix)?,
        region,
        access_key: required(lookup, R2_ACCESS_KEY_ID_ENV)?,
        secret_key: required(lookup, R2_SECRET_ACCESS_KEY_ENV)?,
    })
}

pub(crate) fn epoch_folder(prefix: &str, epoch: u64) -> String {
    format!("{prefix}/epoch-{epoch}")
}

pub(crate) fn object_key(prefix: &str, epoch: u64, file_name: &str) -> String {
    format!("{}/{file_name}", epoch_folder(prefix, epoch))
}

pub(crate) fn part_ranges(len: u64, part_size: u64) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    let mut offset = 0;
    while offset < len {
        let size = part_size.min(len - offset);
        ranges.push((offset, size));
        offset += size;
    }
    ranges
}

pub(crate) fn classify_remote(local: u64, remote: Option<u64>) -> RemoteFile {
    match remote {
        Some(size) if size == local => RemoteFile::SameSize,
        Some(remote) => RemoteFile::DifferentSize { remote },
        None => RemoteFile::Missing,
    }
}

pub(crate) enum RemoteFile {
    Missing,
    SameSize,
    DifferentSize { remote: u64 },
}

pub(crate) fn require_epoch_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::with_capacity(EPOCH_ARCHIVE_FILES.len());
    for name in EPOCH_ARCHIVE_FILES {
        let path = dir.join(name);
        let metadata = std::fs::symlink_metadata(&path)
            .with_context(|| format!("archive file {} is missing", path.display()))?;
        if !metadata.file_type().is_file() {
            bail!("archive file {} is not a regular file", path.display());
        }
        files.push(path);
    }
    Ok(files)
}

pub(crate) fn upload_epoch(settings: &PublishSettings, epoch: u64, files: &[PathBuf]) -> Result<()> {
    let client = R2Client::new(settings)?;
    for path in files {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .context("archive file name is not UTF-8")?;
        let key = object_key(&settings.prefix, epoch, name);
        client.upload_file(path, &key)?;
    }
    Ok(())
}

pub(crate) fn validate_endpoint(endpoint: &str) -> Result<()> {
    let parsed = url::Url::parse(endpoint).context("R2 endpoint is not a URL")?;
    if parsed.scheme() != "https" || parsed.host_str().is_none() {
        bail!("R2 endpoint must be an absolute HTTPS URL");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        bail!("R2 endpoint must not contain user information");
    }
    if !matches!(parsed.path(), "" | "/") || parsed.query().is_some() || parsed.fragment().is_some()
    {
        bail!("R2 endpoint must not contain a path, query, or fragment");
    }
    Ok(())
}

pub(crate) fn xml_element<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}");
    let start_tag = body.find(&open)?;
    let after = body[start_tag..].find('>')? + start_tag + 1;
    let close = format!("</{tag}>");
    let end = body[after..].find(&close)? + after;
    Some(body[after..end].trim())
}

fn ordered_part_etags(mut parts: Vec<(u32, String)>) -> Vec<(u32, String)> {
    parts.sort_by_key(|(number, _)| *number);
    parts
}

fn run_parallel<T, F>(count: usize, workers: usize, task: F) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(usize) -> Result<T> + Sync,
{
    if count == 0 {
        return Ok(Vec::new());
    }
    let workers = workers.max(1).min(count);
    let next = Mutex::new(0usize);
    let failed = AtomicBool::new(false);
    let results = Mutex::new((0..count).map(|_| None).collect::<Vec<Option<T>>>());
    let error = Mutex::new(None);
    thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let index = {
                        let mut next = next.lock().expect("part index");
                        if failed.load(Ordering::SeqCst) || *next >= count {
                            break;
                        }
                        let index = *next;
                        *next += 1;
                        index
                    };
                    match task(index) {
                        Ok(value) => results.lock().expect("part results")[index] = Some(value),
                        Err(err) => {
                            failed.store(true, Ordering::SeqCst);
                            let mut saved = error.lock().expect("part error");
                            if saved.is_none() {
                                *saved = Some(err);
                            }
                            break;
                        }
                    }
                }
            });
        }
    });
    if let Some(err) = error.into_inner().expect("part error") {
        return Err(err);
    }
    Ok(results
        .into_inner()
        .expect("part results")
        .into_iter()
        .map(|item| item.expect("part result"))
        .collect())
}

pub(crate) fn complete_multipart_body(parts: &[(u32, &str)]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (number, etag) in parts {
        xml.push_str("<Part><PartNumber>");
        xml.push_str(&number.to_string());
        xml.push_str("</PartNumber><ETag>");
        xml.push_str(&xml_escape(etag));
        xml.push_str("</ETag></Part>");
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

struct R2Client {
    endpoint: String,
    host: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    client: Client,
}

impl R2Client {
    fn new(settings: &PublishSettings) -> Result<Self> {
        validate_endpoint(&settings.endpoint)?;
        let parsed = url::Url::parse(&settings.endpoint)?;
        let mut host = parsed.host_str().context("R2 endpoint has no host")?.to_string();
        if host.contains(':') && !host.starts_with('[') {
            host = format!("[{host}]");
        }
        if let Some(port) = parsed.port() {
            host.push(':');
            host.push_str(&port.to_string());
        }
        let client = Client::builder()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(600))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .context("build R2 client")?;
        Ok(Self {
            endpoint: settings.endpoint.clone(),
            host,
            bucket: settings.bucket.clone(),
            region: settings.region.clone(),
            access_key: settings.access_key.clone(),
            secret_key: settings.secret_key.clone(),
            client,
        })
    }

    fn upload_file(&self, path: &Path, key: &str) -> Result<()> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("stat {}", path.display()))?;
        let local = metadata.len();
        let name = path.file_name().and_then(|value| value.to_str()).unwrap_or(key);
        match classify_remote(local, self.head_len(key)?) {
            RemoteFile::SameSize => {
                eprintln!("{name}: already on CF, {local} bytes");
                return Ok(());
            }
            RemoteFile::DifferentSize { remote } => {
                bail!("{name} on CF has {remote} bytes and the local file has {local} bytes");
            }
            RemoteFile::Missing => {}
        }
        if local == 0 {
            self.put_bytes(key, &[])?;
        } else {
            let upload_id = self.create_multipart(key)?;
            if let Err(err) = self.upload_parts(&file, key, &upload_id, local, name) {
                if let Err(abort_err) = self.abort_multipart(key, &upload_id) {
                    eprintln!("abort {name}: {abort_err:#}");
                }
                return Err(err);
            }
        }
        let confirmed = self
            .head_len(key)?
            .with_context(|| format!("{name} is missing on CF after upload"))?;
        if confirmed != local {
            bail!("{name} on CF has {confirmed} bytes after upload and the local file has {local} bytes");
        }
        let after = std::fs::symlink_metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .len();
        if after != local {
            bail!("{name} changed during upload");
        }
        eprintln!("{name}: sent, {local} bytes");
        Ok(())
    }

    fn upload_parts(
        &self,
        file: &File,
        key: &str,
        upload_id: &str,
        local: u64,
        name: &str,
    ) -> Result<()> {
        let ranges = part_ranges(local, PART_SIZE);
        let total = ranges.len();
        let etags = run_parallel(total, PARALLEL_UPLOADS, |index| {
            let (offset, size) = ranges[index];
            let part_number = u32::try_from(index + 1).context("part number overflows")?;
            eprintln!("{name}: part {part_number}/{total}");
            let bytes = read_range(file, offset, size)?;
            let etag = self.upload_part(key, upload_id, part_number, &bytes)?;
            Ok((part_number, etag))
        })?;
        let etags = ordered_part_etags(etags);
        let borrowed = etags
            .iter()
            .map(|(number, etag)| (*number, etag.as_str()))
            .collect::<Vec<_>>();
        self.complete_multipart(key, upload_id, &borrowed)
    }

    fn head_len(&self, key: &str) -> Result<Option<u64>> {
        let response = self.request(Method::HEAD, key, &BTreeMap::new(), &BTreeMap::new(), &[])?;
        if response.status == 404 {
            return Ok(None);
        }
        if response.status != 200 {
            bail!("HEAD {key} returned HTTP {}", response.status);
        }
        Ok(Some(content_length(&response.headers, "HEAD")?))
    }

    fn put_bytes(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let headers = BTreeMap::from([(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        )]);
        let response = self.request(Method::PUT, key, &BTreeMap::new(), &headers, bytes)?;
        if response.status != 200 {
            bail!("PUT {key} returned HTTP {}", response.status);
        }
        Ok(())
    }

    fn create_multipart(&self, key: &str) -> Result<String> {
        let params = BTreeMap::from([("uploads".to_string(), String::new())]);
        let headers = BTreeMap::from([(
            "content-type".to_string(),
            "application/xml".to_string(),
        )]);
        let response = self.request(Method::POST, key, &params, &headers, &[])?;
        if response.status != 200 {
            bail!("create multipart {key} returned HTTP {}", response.status);
        }
        let body = String::from_utf8(response.body).context("multipart response is not UTF-8")?;
        xml_element(&body, "UploadId")
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .context("multipart response has no upload id")
    }

    fn upload_part(&self, key: &str, upload_id: &str, part_number: u32, bytes: &[u8]) -> Result<String> {
        let params = BTreeMap::from([
            ("partNumber".to_string(), part_number.to_string()),
            ("uploadId".to_string(), upload_id.to_string()),
        ]);
        let headers = BTreeMap::from([(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        )]);
        let response = self.request(Method::PUT, key, &params, &headers, bytes)?;
        if response.status != 200 {
            bail!("upload part {part_number} of {key} returned HTTP {}", response.status);
        }
        let etag = header_value(&response.headers, "etag", "upload part")?;
        Ok(quote_etag(etag))
    }

    fn complete_multipart(&self, key: &str, upload_id: &str, parts: &[(u32, &str)]) -> Result<()> {
        let params = BTreeMap::from([("uploadId".to_string(), upload_id.to_string())]);
        let headers = BTreeMap::from([(
            "content-type".to_string(),
            "application/xml".to_string(),
        )]);
        let body = complete_multipart_body(parts);
        let response = self.request(
            Method::POST,
            key,
            &params,
            &headers,
            body.as_bytes(),
        )?;
        if response.status != 200 {
            bail!("complete multipart {key} returned HTTP {}", response.status);
        }
        Ok(())
    }

    fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
        let params = BTreeMap::from([("uploadId".to_string(), upload_id.to_string())]);
        let response = self.request(Method::DELETE, key, &params, &BTreeMap::new(), &[])?;
        if response.status != 204 && response.status != 200 {
            bail!("abort multipart {key} returned HTTP {}", response.status);
        }
        Ok(())
    }

    fn request(
        &self,
        method: Method,
        key: &str,
        params: &BTreeMap<String, String>,
        headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<HttpResult> {
        let mut last = None;
        for attempt in 1..=ATTEMPTS {
            match self.request_once(method.clone(), key, params, headers, body) {
                Ok(response) => return Ok(response),
                Err(err) if attempt < ATTEMPTS && retryable(&err) => {
                    eprintln!("retry {attempt}/{ATTEMPTS} for {key}");
                    last = Some(err);
                    thread::sleep(Duration::from_secs(u64::from(attempt)));
                }
                Err(err) => return Err(err),
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("R2 request failed")))
    }

    fn request_once(
        &self,
        method: Method,
        key: &str,
        params: &BTreeMap<String, String>,
        headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<HttpResult> {
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
            .map(|(name, value)| format!("{}={}", uri_encode(name, false), uri_encode(value, false)))
            .collect::<Vec<_>>()
            .join("&");
        let mut signed = BTreeMap::from([
            ("host".to_string(), self.host.clone()),
            ("x-amz-content-sha256".to_string(), UNSIGNED_PAYLOAD.to_string()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ]);
        for (name, value) in headers {
            signed.insert(name.clone(), value.trim().to_string());
        }
        let canonical_headers = signed
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect::<String>();
        let signed_names = signed.keys().cloned().collect::<Vec<_>>().join(";");
        let canonical_request = format!(
            "{}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_names}\n{UNSIGNED_PAYLOAD}",
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
        let mut request = self
            .client
            .request(method.clone(), &url)
            .body(Body::from(body.to_vec()));
        for (name, value) in &signed {
            request = request.header(
                HeaderName::from_bytes(name.as_bytes()).context("invalid R2 header name")?,
                HeaderValue::from_str(value).context("invalid R2 header value")?,
            );
        }
        request = request.header("authorization", authorization);
        let response = request.send().with_context(|| format!("{method} {key}"))?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        if status >= 500 || status == 429 {
            bail!("{method} {key} returned HTTP {status}");
        }
        let mut bytes = Vec::new();
        response
            .take(MAX_RESPONSE_BYTES + 1)
            .read_to_end(&mut bytes)
            .with_context(|| format!("read {method} {key} response"))?;
        if bytes.len() as u64 > MAX_RESPONSE_BYTES {
            bail!("{method} {key} response is too large");
        }
        Ok(HttpResult {
            status,
            headers,
            body: bytes,
        })
    }
}

struct HttpResult {
    status: u16,
    headers: HeaderMap,
    body: Vec<u8>,
}

fn present_or(lookup: &dyn Fn(&str) -> Option<String>, key: &str, fallback: &str) -> String {
    let value = first_present(lookup, &[key]);
    if value.is_empty() { fallback.to_string() } else { value }
}

fn first_present(lookup: &dyn Fn(&str) -> Option<String>, keys: &[&str]) -> String {
    for key in keys {
        if let Some(value) = lookup(key).map(|value| value.trim().to_string()).filter(|value| !value.is_empty())
        {
            return value;
        }
    }
    String::new()
}

fn required(lookup: &dyn Fn(&str) -> Option<String>, key: &str) -> Result<String> {
    match lookup(key).map(|value| value.trim().to_string()).filter(|value| !value.is_empty()) {
        Some(value) => Ok(value),
        None => bail!("{key} is empty"),
    }
}

fn normalize_prefix(prefix: &str) -> Result<String> {
    let prefix = prefix.trim().trim_matches('/');
    if prefix.is_empty()
        || prefix.split('/').any(|part| part.is_empty() || part == "." || part == "..")
    {
        bail!("R2 prefix is invalid");
    }
    Ok(prefix.to_string())
}

fn content_length(headers: &HeaderMap, operation: &str) -> Result<u64> {
    let text = header_value(headers, "content-length", operation)?;
    if text.is_empty() || text.len() > 20 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("{operation} returned an invalid content length");
    }
    text.parse()
        .context(format!("{operation} returned an invalid content length"))
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str, operation: &str) -> Result<&'a str> {
    let header_name = HeaderName::from_bytes(name.as_bytes())
        .with_context(|| format!("{operation} requested an invalid header"))?;
    let mut values = headers.get_all(&header_name).iter();
    let value = values
        .next()
        .with_context(|| format!("{operation} is missing {name}"))?;
    if values.next().is_some() {
        bail!("{operation} returned multiple {name} values");
    }
    value
        .to_str()
        .with_context(|| format!("{operation} returned an invalid {name}"))
}

fn quote_etag(etag: &str) -> String {
    if etag.starts_with('"') && etag.ends_with('"') {
        etag.to_string()
    } else {
        format!("\"{etag}\"")
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn read_range(file: &File, mut offset: u64, len: u64) -> Result<Vec<u8>> {
    let len = usize::try_from(len).context("part is too large")?;
    let mut bytes = vec![0u8; len];
    let mut filled = 0;
    while filled < len {
        let count = file
            .read_at(&mut bytes[filled..], offset)
            .context("read archive part")?;
        if count == 0 {
            bail!("archive part ended early");
        }
        filled += count;
        offset += count as u64;
    }
    Ok(bytes)
}

fn retryable(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}");
    text.contains("HTTP 429")
        || text.contains("HTTP 500")
        || text.contains("HTTP 502")
        || text.contains("HTTP 503")
        || text.contains("HTTP 504")
        || text.contains("error sending request")
        || text.contains("operation timed out")
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
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_hash);
    outer.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::AtomicUsize;

    fn lookup(values: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<std::collections::BTreeMap<_, _>>();
        move |key| values.get(key).cloned()
    }

    #[test]
    fn the_epoch_object_key_uses_the_given_prefix() {
        assert_eq!(
            object_key("compact-v2/blockzilla-20261004", 1084, "registry.bin"),
            "compact-v2/blockzilla-20261004/epoch-1084/registry.bin"
        );
    }

    #[test]
    fn part_etags_return_to_part_number_order() {
        let ordered = ordered_part_etags(vec![
            (3, "c".to_string()),
            (1, "a".to_string()),
            (2, "b".to_string()),
        ]);
        assert_eq!(
            ordered,
            vec![(1, "a".to_string()), (2, "b".to_string()), (3, "c".to_string())]
        );
        let xml = complete_multipart_body(
            &ordered
                .iter()
                .map(|(number, etag)| (*number, etag.as_str()))
                .collect::<Vec<_>>(),
        );
        assert!(xml.find("<PartNumber>1</PartNumber>").unwrap() < xml.find("<PartNumber>3</PartNumber>").unwrap());
    }

    #[test]
    fn parallel_uploads_stay_within_the_limit_and_stop_after_a_failure() {
        let current = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let started = AtomicUsize::new(0);
        let release = std::sync::Barrier::new(PARALLEL_UPLOADS);
        let error = run_parallel(PARALLEL_UPLOADS * 2, PARALLEL_UPLOADS, |index| {
            started.fetch_add(1, Ordering::SeqCst);
            let now = current.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            let outcome = (|| {
                if index < PARALLEL_UPLOADS {
                    release.wait();
                }
                if index == 0 {
                    bail!("part failed");
                }
                thread::sleep(Duration::from_millis(40));
                Ok(index)
            })();
            current.fetch_sub(1, Ordering::SeqCst);
            outcome
        });
        assert!(error.is_err());
        assert!(peak.load(Ordering::SeqCst) <= PARALLEL_UPLOADS);
        assert_eq!(started.load(Ordering::SeqCst), PARALLEL_UPLOADS);
    }

    #[test]
    fn part_ranges_cover_the_file_once() {
        assert!(part_ranges(0, PART_SIZE).is_empty());
        assert_eq!(part_ranges(10, PART_SIZE), vec![(0, 10)]);
        assert_eq!(
            part_ranges(PART_SIZE + 1, PART_SIZE),
            vec![(0, PART_SIZE), (PART_SIZE, 1)]
        );
    }

    #[test]
    fn a_same_size_object_stays_and_a_different_size_does_not() {
        assert!(matches!(classify_remote(10, Some(10)), RemoteFile::SameSize));
        assert!(matches!(classify_remote(10, None), RemoteFile::Missing));
        assert!(matches!(
            classify_remote(10, Some(11)),
            RemoteFile::DifferentSize { remote: 11 }
        ));
    }

    #[test]
    fn settings_require_the_bucket_and_the_prefix() {
        let missing = lookup(&[
            ("R2_ENDPOINT", "https://account.r2.cloudflarestorage.com"),
            ("R2_ACCESS_KEY_ID", "key"),
            ("R2_SECRET_ACCESS_KEY", "secret"),
            ("R2_PREFIX", "compact-v2/blockzilla-20261004"),
        ]);
        let Err(error) = resolve_settings(&missing) else {
            panic!("missing R2_BUCKET was accepted");
        };
        assert!(error.to_string().contains(R2_BUCKET_ENV), "{error:#}");

        let ready = lookup(&[
            ("R2_S3_ENDPOINT", "https://account.r2.cloudflarestorage.com"),
            ("R2_ACCESS_KEY_ID", "key"),
            ("R2_SECRET_ACCESS_KEY", "secret"),
            ("R2_BUCKET", "solana-archive-data"),
            ("R2_PREFIX", "compact-v2/blockzilla-20261004/"),
        ]);
        let settings = resolve_settings(&ready).unwrap();
        assert_eq!(settings.bucket, "solana-archive-data");
        assert_eq!(settings.prefix, "compact-v2/blockzilla-20261004");
        assert_eq!(settings.region, "auto");
        assert_eq!(
            object_key(&settings.prefix, 1084, "poh.wincode"),
            "compact-v2/blockzilla-20261004/epoch-1084/poh.wincode"
        );
    }

    #[test]
    fn an_http_endpoint_is_rejected() {
        let error = validate_endpoint("http://account.r2.cloudflarestorage.com").unwrap_err();
        assert!(error.to_string().contains("HTTPS"), "{error:#}");
    }

    #[test]
    fn a_missing_archive_file_stops_before_upload() {
        let dir = tempfile::tempdir().unwrap();
        let error = require_epoch_files(dir.path()).unwrap_err();
        assert!(error.to_string().contains("is missing"), "{error:#}");
    }

    #[test]
    fn the_eleven_archive_files_are_accepted() {
        let dir = tempfile::tempdir().unwrap();
        for name in EPOCH_ARCHIVE_FILES {
            File::create(dir.path().join(name)).unwrap();
        }
        let files = require_epoch_files(dir.path()).unwrap();
        assert_eq!(files.len(), 11);
    }

    #[test]
    fn a_symlink_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        for name in EPOCH_ARCHIVE_FILES {
            let path = dir.path().join(name);
            if name == EPOCH_ARCHIVE_FILES[0] {
                let target = dir.path().join("target");
                let mut file = File::create(&target).unwrap();
                file.write_all(b"x").unwrap();
                std::os::unix::fs::symlink(&target, &path).unwrap();
            } else {
                File::create(path).unwrap();
            }
        }
        let error = require_epoch_files(dir.path()).unwrap_err();
        assert!(error.to_string().contains("not a regular file"), "{error:#}");
    }

    #[test]
    fn the_upload_id_and_the_complete_body_keep_the_part_order() {
        let body = "<InitiateMultipartUploadResult><UploadId>abc/123=</UploadId></InitiateMultipartUploadResult>";
        assert_eq!(xml_element(body, "UploadId"), Some("abc/123="));
        let xml = complete_multipart_body(&[(1, "\"aaa\""), (2, "\"bbb\"")]);
        assert!(xml.find("\"aaa\"").unwrap() < xml.find("\"bbb\"").unwrap());
        assert!(xml.contains("<PartNumber>1</PartNumber>"));
        assert!(xml.contains("<PartNumber>2</PartNumber>"));
    }
}
