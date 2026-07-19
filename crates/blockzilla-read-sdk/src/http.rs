use std::{fmt, io::Read, time::Duration};

use reqwest::{
    StatusCode,
    blocking::{Client, RequestBuilder, Response},
    header::{ACCEPT_ENCODING, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, HeaderValue, RANGE},
    redirect,
};
use url::Url;

use crate::{
    SourceError,
    manifest::{GENERATION_MANIFEST_FILE, validate_object_name},
    source::{RangeSource, SourceResult},
};

const DEFAULT_MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct HttpRangeSourceOptions {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_manifest_bytes: usize,
    /// Bearer tokens must not travel over cleartext. This escape hatch is only
    /// for a trusted local development server.
    pub allow_insecure_http: bool,
}

impl Default for HttpRangeSourceOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            max_manifest_bytes: DEFAULT_MAX_MANIFEST_BYTES,
            allow_insecure_http: false,
        }
    }
}

/// Blocking HTTP Range source for the Blockzilla read gateway.
///
/// The secret is held only as an already-redacted header value. `Debug` never
/// prints it. Redirects and ambient HTTP proxies are disabled so a bearer token
/// cannot be forwarded to a different origin.
#[derive(Clone)]
pub struct HttpRangeSource {
    client: Client,
    base_url: Url,
    epoch: u64,
    authorization: Option<HeaderValue>,
    max_manifest_bytes: usize,
}

impl fmt::Debug for HttpRangeSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRangeSource")
            .field("base_url", &self.base_url)
            .field("epoch", &self.epoch)
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .field("max_manifest_bytes", &self.max_manifest_bytes)
            .finish()
    }
}

impl HttpRangeSource {
    pub fn new(
        base_url: impl AsRef<str>,
        epoch: u64,
        bearer_token: Option<&str>,
    ) -> SourceResult<Self> {
        Self::with_options(
            base_url,
            epoch,
            bearer_token,
            HttpRangeSourceOptions::default(),
        )
    }

    pub fn with_options(
        base_url: impl AsRef<str>,
        epoch: u64,
        bearer_token: Option<&str>,
        options: HttpRangeSourceOptions,
    ) -> SourceResult<Self> {
        let mut base_url = Url::parse(base_url.as_ref())
            .map_err(|error| SourceError::Protocol(format!("invalid gateway URL: {error}")))?;
        if !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(SourceError::Protocol(
                "gateway URL must not contain credentials, a query, or a fragment".into(),
            ));
        }
        if base_url.host_str().is_none() {
            return Err(SourceError::Protocol(
                "gateway URL must have an origin".into(),
            ));
        }
        match base_url.scheme() {
            "https" => {}
            "http" if options.allow_insecure_http => {}
            "http" => {
                return Err(SourceError::Protocol(
                    "cleartext HTTP is disabled; explicitly enable it only for local development"
                        .into(),
                ));
            }
            scheme => {
                return Err(SourceError::Protocol(format!(
                    "unsupported gateway URL scheme {scheme}"
                )));
            }
        }
        if options.max_manifest_bytes == 0 {
            return Err(SourceError::Protocol(
                "max_manifest_bytes must be non-zero".into(),
            ));
        }
        // A base ending with a file-looking segment is still treated as a path
        // prefix. Gateway route components are appended, never URL-joined.
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }

        let authorization = match bearer_token {
            Some("") => {
                return Err(SourceError::Protocol("bearer token is empty".into()));
            }
            Some(token) => Some(HeaderValue::from_str(&format!("Bearer {token}")).map_err(
                |_| SourceError::Protocol("bearer token contains invalid header bytes".into()),
            )?),
            None => None,
        };
        let client = Client::builder()
            .connect_timeout(options.connect_timeout)
            .timeout(options.request_timeout)
            .redirect(redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|error| SourceError::Protocol(format!("build HTTP range client: {error}")))?;
        Ok(Self {
            client,
            base_url,
            epoch,
            authorization,
            max_manifest_bytes: options.max_manifest_bytes,
        })
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    fn object_url(&self, object: &str) -> SourceResult<Url> {
        validate_object_name(object).map_err(|_| SourceError::InvalidName(object.to_owned()))?;
        let mut url = self.base_url.clone();
        {
            let mut path = url.path_segments_mut().map_err(|_| {
                SourceError::Protocol("gateway URL cannot accept path segments".into())
            })?;
            path.pop_if_empty();
            path.push("v1");
            path.push("epochs");
            path.push(&self.epoch.to_string());
            if object == GENERATION_MANIFEST_FILE {
                path.push("manifest");
            } else {
                path.push("files");
                path.push(object);
            }
        }
        Ok(url)
    }

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.authorization {
            Some(value) => request.header(AUTHORIZATION, value.clone()),
            None => request,
        }
    }

    fn check_origin(&self, response: &Response) -> SourceResult<()> {
        if response.url().origin() != self.base_url.origin() {
            return Err(SourceError::Protocol(
                "gateway response crossed origin".into(),
            ));
        }
        Ok(())
    }

    fn full_manifest(&self, expected_length: usize) -> SourceResult<Vec<u8>> {
        if expected_length > self.max_manifest_bytes {
            return Err(SourceError::Protocol(format!(
                "manifest is {expected_length} bytes, above the {} byte limit",
                self.max_manifest_bytes
            )));
        }
        let url = self.object_url(GENERATION_MANIFEST_FILE)?;
        let mut response = self
            .authorize(
                self.client
                    .get(url)
                    .header(ACCEPT_ENCODING, HeaderValue::from_static("identity")),
            )
            .send()
            .map_err(sanitize_http_error)?;
        self.check_origin(&response)?;
        if response.status() != StatusCode::OK {
            return Err(SourceError::Protocol(format!(
                "manifest GET returned HTTP {}",
                response.status()
            )));
        }
        enforce_content_length(&response, expected_length)?;
        read_bounded(&mut response, expected_length, GENERATION_MANIFEST_FILE)
    }
}

impl RangeSource for HttpRangeSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        let url = self.object_url(object)?;
        let response = self
            .authorize(
                self.client
                    .head(url)
                    .header(ACCEPT_ENCODING, HeaderValue::from_static("identity")),
            )
            .send()
            .map_err(sanitize_http_error)?;
        self.check_origin(&response)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if response.status() != StatusCode::OK {
            return Err(SourceError::Protocol(format!(
                "HEAD for {object} returned HTTP {}",
                response.status()
            )));
        }
        let value = response.headers().get(CONTENT_LENGTH).ok_or_else(|| {
            SourceError::Protocol(format!("HEAD for {object} omitted Content-Length"))
        })?;
        let value = value.to_str().map_err(|_| {
            SourceError::Protocol(format!("HEAD for {object} has invalid Content-Length"))
        })?;
        let size = value.parse::<u64>().map_err(|_| {
            SourceError::Protocol(format!("HEAD for {object} has invalid Content-Length"))
        })?;
        Ok(Some(size))
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        if length == 0 {
            let size = self
                .size(object)?
                .ok_or_else(|| SourceError::NotFound(object.to_owned()))?;
            if offset > size {
                return Err(SourceError::OutOfBounds {
                    object: object.to_owned(),
                    offset,
                    length,
                    size,
                });
            }
            return Ok(Vec::new());
        }
        if object == GENERATION_MANIFEST_FILE {
            if offset != 0 {
                return Err(SourceError::Protocol(
                    "manifest only supports a complete bounded GET".into(),
                ));
            }
            return self.full_manifest(length);
        }

        let length_u64 = u64::try_from(length).map_err(|_| {
            SourceError::Protocol("requested HTTP range length does not fit u64".into())
        })?;
        let end_exclusive = offset
            .checked_add(length_u64)
            .ok_or_else(|| SourceError::Protocol(format!("range overflow for {object}")))?;
        let end_inclusive = end_exclusive - 1;
        let url = self.object_url(object)?;
        let mut response = self
            .authorize(
                self.client
                    .get(url)
                    .header(ACCEPT_ENCODING, HeaderValue::from_static("identity"))
                    .header(RANGE, format!("bytes={offset}-{end_inclusive}")),
            )
            .send()
            .map_err(sanitize_http_error)?;
        self.check_origin(&response)?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(SourceError::Protocol(format!(
                "range GET for {object} returned HTTP {}, expected 206",
                response.status()
            )));
        }
        enforce_content_length(&response, length)?;
        let content_range = response.headers().get(CONTENT_RANGE).ok_or_else(|| {
            SourceError::Protocol(format!("range GET for {object} omitted Content-Range"))
        })?;
        let content_range = content_range.to_str().map_err(|_| {
            SourceError::Protocol(format!("range GET for {object} has invalid Content-Range"))
        })?;
        let (actual_start, actual_end, total) =
            parse_content_range(content_range).ok_or_else(|| {
                SourceError::Protocol(format!(
                    "range GET for {object} has malformed Content-Range"
                ))
            })?;
        if actual_start != offset || actual_end != end_inclusive || end_exclusive > total {
            return Err(SourceError::Protocol(format!(
                "range GET for {object} returned an unexpected Content-Range"
            )));
        }
        read_bounded(&mut response, length, object)
    }
}

fn enforce_content_length(response: &Response, expected: usize) -> SourceResult<()> {
    if let Some(value) = response.headers().get(CONTENT_LENGTH) {
        let value = value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| SourceError::Protocol("invalid Content-Length".into()))?;
        if value != expected as u64 {
            return Err(SourceError::Protocol(format!(
                "response Content-Length is {value}, expected {expected}"
            )));
        }
    }
    Ok(())
}

fn read_bounded(response: &mut Response, expected: usize, object: &str) -> SourceResult<Vec<u8>> {
    let bound = u64::try_from(expected)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::with_capacity(expected.min(8 * 1024 * 1024));
    response
        .take(bound)
        .read_to_end(&mut bytes)
        .map_err(|source| SourceError::Io {
            object: object.to_owned(),
            source,
        })?;
    if bytes.len() != expected {
        return Err(SourceError::ShortRead {
            object: object.to_owned(),
            expected,
            actual: bytes.len(),
        });
    }
    Ok(bytes)
}

fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let value = value.strip_prefix("bytes ")?;
    let (range, total) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse().ok()?;
    let end = end.parse().ok()?;
    let total = total.parse().ok()?;
    (start <= end && end < total).then_some((start, end, total))
}

fn sanitize_http_error(error: reqwest::Error) -> SourceError {
    // reqwest errors can contain a URL but do not include request headers. Keep
    // the message generic anyway so credentials can never become diagnostic data.
    SourceError::Protocol(format!("HTTP request failed ({})", error.without_url()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_strict_content_range() {
        assert_eq!(parse_content_range("bytes 5-9/100"), Some((5, 9, 100)));
        assert_eq!(parse_content_range("bytes 9-5/100"), None);
        assert_eq!(parse_content_range("bytes 5-100/100"), None);
        assert_eq!(parse_content_range("bytes 5-9/*"), None);
    }

    #[test]
    fn debug_redacts_bearer_token() {
        let options = HttpRangeSourceOptions {
            allow_insecure_http: true,
            ..HttpRangeSourceOptions::default()
        };
        let source = HttpRangeSource::with_options(
            "http://127.0.0.1:1",
            999,
            Some("highly-secret-token"),
            options,
        )
        .unwrap();
        let rendered = format!("{source:?}");
        assert!(!rendered.contains("highly-secret-token"));
        assert!(rendered.contains("<redacted>"));
    }
}
