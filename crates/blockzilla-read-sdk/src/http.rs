use std::{
    collections::HashMap,
    fmt,
    io::Read,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use reqwest::{
    StatusCode,
    blocking::{Client, RequestBuilder, Response},
    header::{
        ACCEPT_ENCODING, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderMap,
        HeaderValue, RANGE,
    },
    redirect,
};
use url::Url;

use crate::{
    SourceError,
    manifest::{GENERATION_MANIFEST_FILE, validate_object_name},
    source::{RangeSource, SourceResult},
};

const DEFAULT_MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_INCOMPLETE_RANGE_BODY_RETRIES: usize = 2;

/// Exact HTTP work completed by one source and all its clones.
///
/// `returned_body_bytes` counts all response-body bytes consumed by the
/// source, including partial bytes from an incomplete range attempt. Response
/// headers are excluded. HEAD responses have no body.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HttpRangeSourceStats {
    pub head_requests: u64,
    pub get_requests: u64,
    /// New full-range GET attempts made after an incomplete response body.
    pub incomplete_body_retries: u64,
    pub returned_body_bytes: u64,
}

impl HttpRangeSourceStats {
    pub fn requests(self) -> u64 {
        self.head_requests.saturating_add(self.get_requests)
    }

    pub fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            head_requests: self.head_requests.saturating_sub(earlier.head_requests),
            get_requests: self.get_requests.saturating_sub(earlier.get_requests),
            incomplete_body_retries: self
                .incomplete_body_retries
                .saturating_sub(earlier.incomplete_body_retries),
            returned_body_bytes: self
                .returned_body_bytes
                .saturating_sub(earlier.returned_body_bytes),
        }
    }
}

#[derive(Debug, Default)]
struct HttpRangeSourceCounters {
    head_requests: AtomicU64,
    get_requests: AtomicU64,
    incomplete_body_retries: AtomicU64,
    returned_body_bytes: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ObservedObjectIdentity {
    Absent,
    Present {
        length: u64,
        strong_etag: Option<String>,
    },
}

/// Immutable HTTP object identity required by the persistent range cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpObjectIdentity {
    pub length: u64,
    pub strong_etag: String,
}

#[derive(Debug, Clone)]
pub struct HttpRangeSourceOptions {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_manifest_bytes: usize,
    /// Bearer tokens must not travel over cleartext. This escape hatch is only
    /// for a trusted local development server.
    pub allow_insecure_http: bool,
    /// Path contract used to find one archive object below `base_url`.
    pub object_path_layout: HttpObjectPathLayout,
}

/// URL path contract for archive objects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HttpObjectPathLayout {
    /// Existing gateway route: `v1/epochs/<epoch>/files/<object>`.
    #[default]
    GatewayV1,
    /// Simple sample route: `<epoch>/<object>`.
    FlatEpoch,
}

impl Default for HttpRangeSourceOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            max_manifest_bytes: DEFAULT_MAX_MANIFEST_BYTES,
            allow_insecure_http: false,
            object_path_layout: HttpObjectPathLayout::default(),
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
    object_path_layout: HttpObjectPathLayout,
    counters: Arc<HttpRangeSourceCounters>,
    observed_objects: Arc<Mutex<HashMap<String, ObservedObjectIdentity>>>,
}

impl fmt::Debug for HttpRangeSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRangeSource")
            .field("base_url", &self.base_url)
            .field("epoch", &self.epoch)
            .field("object_path_layout", &self.object_path_layout)
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .field("max_manifest_bytes", &self.max_manifest_bytes)
            .field("stats", &self.stats())
            .field(
                "observed_object_count",
                &self
                    .observed_objects
                    .lock()
                    .map(|objects| objects.len())
                    .ok(),
            )
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
            object_path_layout: options.object_path_layout,
            counters: Arc::new(HttpRangeSourceCounters::default()),
            observed_objects: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Return a shared-source snapshot. Clones contribute to the same totals.
    pub fn stats(&self) -> HttpRangeSourceStats {
        HttpRangeSourceStats {
            head_requests: self.counters.head_requests.load(Ordering::Relaxed),
            get_requests: self.counters.get_requests.load(Ordering::Relaxed),
            incomplete_body_retries: self
                .counters
                .incomplete_body_retries
                .load(Ordering::Relaxed),
            returned_body_bytes: self.counters.returned_body_bytes.load(Ordering::Relaxed),
        }
    }

    /// Perform a fresh HEAD and require an exact strong ETag and length.
    pub fn strong_identity(&self, object: &str) -> SourceResult<HttpObjectIdentity> {
        self.head_identity(object, true)?
            .ok_or_else(|| SourceError::NotFound(object.to_owned()))
    }

    fn record_head_request(&self) {
        self.counters.head_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_get_request(&self) {
        self.counters.get_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_incomplete_body_retry(&self) {
        self.counters
            .incomplete_body_retries
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_returned_body(&self, length: usize) {
        self.counters
            .returned_body_bytes
            .fetch_add(length as u64, Ordering::Relaxed);
    }

    fn bind_object_identity(
        &self,
        object: &str,
        length: u64,
        strong_etag: Option<&str>,
    ) -> SourceResult<()> {
        let mut objects = self.observed_objects.lock().map_err(|_| {
            SourceError::Protocol("HTTP object-identity binding cache is poisoned".into())
        })?;
        if let Some(observed) = objects.get_mut(object) {
            match observed {
                ObservedObjectIdentity::Absent => {
                    return Err(SourceError::Protocol(format!(
                        "object {object} became present after its absence was pinned"
                    )));
                }
                ObservedObjectIdentity::Present {
                    length: observed_length,
                    strong_etag: observed_etag,
                } => {
                    if *observed_length != length {
                        return Err(SourceError::Protocol(format!(
                            "object {object} changed size from {observed_length} to {length}"
                        )));
                    }
                    match (&*observed_etag, strong_etag) {
                        (Some(expected), Some(actual)) if expected == actual => {}
                        (Some(expected), Some(actual)) => {
                            return Err(SourceError::Protocol(format!(
                                "object {object} changed strong ETag from {expected} to {actual}"
                            )));
                        }
                        (Some(_), None) => {
                            return Err(SourceError::Protocol(format!(
                                "object {object} omitted the pinned strong ETag"
                            )));
                        }
                        (None, Some(actual)) => *observed_etag = Some(actual.to_owned()),
                        (None, None) => {}
                    }
                }
            }
        } else {
            objects.insert(
                object.to_owned(),
                ObservedObjectIdentity::Present {
                    length,
                    strong_etag: strong_etag.map(str::to_owned),
                },
            );
        }
        Ok(())
    }

    fn bind_object_absence(&self, object: &str) -> SourceResult<()> {
        let mut objects = self.observed_objects.lock().map_err(|_| {
            SourceError::Protocol("HTTP object-identity binding cache is poisoned".into())
        })?;
        match objects.get(object) {
            Some(ObservedObjectIdentity::Absent) => Ok(()),
            Some(ObservedObjectIdentity::Present { .. }) => Err(SourceError::Protocol(format!(
                "object {object} became absent after its presence was pinned"
            ))),
            None => {
                objects.insert(object.to_owned(), ObservedObjectIdentity::Absent);
                Ok(())
            }
        }
    }

    fn head_identity(
        &self,
        object: &str,
        require_strong_etag: bool,
    ) -> SourceResult<Option<HttpObjectIdentity>> {
        let url = self.object_url(object)?;
        self.record_head_request();
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
            self.bind_object_absence(object)?;
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
        let length = value.parse::<u64>().map_err(|_| {
            SourceError::Protocol(format!("HEAD for {object} has invalid Content-Length"))
        })?;
        let strong_etag = strong_etag(response.headers(), object)?;
        if require_strong_etag && strong_etag.is_none() {
            return Err(SourceError::Protocol(format!(
                "HEAD for {object} requires one exact strong ETag"
            )));
        }
        self.bind_object_identity(object, length, strong_etag.as_deref())?;
        Ok(Some(HttpObjectIdentity {
            length,
            strong_etag: strong_etag.unwrap_or_default(),
        }))
    }

    fn object_url(&self, object: &str) -> SourceResult<Url> {
        validate_object_name(object).map_err(|_| SourceError::InvalidName(object.to_owned()))?;
        let mut url = self.base_url.clone();
        {
            let mut path = url.path_segments_mut().map_err(|_| {
                SourceError::Protocol("gateway URL cannot accept path segments".into())
            })?;
            path.pop_if_empty();
            match self.object_path_layout {
                HttpObjectPathLayout::GatewayV1 => {
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
                HttpObjectPathLayout::FlatEpoch => {
                    path.push(&self.epoch.to_string());
                    path.push(object);
                }
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
        self.record_get_request();
        let mut response = self
            .authorize(
                self.client
                    .get(url)
                    .header(ACCEPT_ENCODING, HeaderValue::from_static("identity")),
            )
            .send()
            .map_err(sanitize_http_error)?;
        self.check_origin(&response)?;
        if response.status() == StatusCode::NOT_FOUND {
            self.bind_object_absence(GENERATION_MANIFEST_FILE)?;
            return Err(SourceError::NotFound(GENERATION_MANIFEST_FILE.to_owned()));
        }
        if response.status() != StatusCode::OK {
            return Err(SourceError::Protocol(format!(
                "manifest GET returned HTTP {}",
                response.status()
            )));
        }
        let response_etag = strong_etag(response.headers(), GENERATION_MANIFEST_FILE)?;
        self.bind_object_identity(
            GENERATION_MANIFEST_FILE,
            expected_length as u64,
            response_etag.as_deref(),
        )?;
        enforce_content_length(&response, expected_length)?;
        let mut bytes = Vec::new();
        let result = read_bounded_into(
            &mut response,
            expected_length,
            GENERATION_MANIFEST_FILE,
            &mut bytes,
        );
        self.record_returned_body(bytes.len());
        result?;
        Ok(bytes)
    }

    fn range_response(&self, object: &str, offset: u64, length: usize) -> SourceResult<Response> {
        debug_assert!(length != 0);
        debug_assert!(object != GENERATION_MANIFEST_FILE);
        let length_u64 = u64::try_from(length).map_err(|_| {
            SourceError::Protocol("requested HTTP range length does not fit u64".into())
        })?;
        let end_exclusive = offset
            .checked_add(length_u64)
            .ok_or_else(|| SourceError::Protocol(format!("range overflow for {object}")))?;
        let end_inclusive = end_exclusive - 1;
        let url = self.object_url(object)?;
        self.record_get_request();
        let response = self
            .authorize(
                self.client
                    .get(url)
                    .header(ACCEPT_ENCODING, HeaderValue::from_static("identity"))
                    .header(RANGE, format!("bytes={offset}-{end_inclusive}")),
            )
            .send()
            .map_err(sanitize_http_error)?;
        self.check_origin(&response)?;
        if response.status() == StatusCode::NOT_FOUND {
            self.bind_object_absence(object)?;
            return Err(SourceError::NotFound(object.to_owned()));
        }
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
        let response_etag = strong_etag(response.headers(), object)?;
        self.bind_object_identity(object, total, response_etag.as_deref())?;
        if actual_start != offset || actual_end != end_inclusive || end_exclusive > total {
            return Err(SourceError::Protocol(format!(
                "range GET for {object} returned an unexpected Content-Range"
            )));
        }
        Ok(response)
    }
}

impl RangeSource for HttpRangeSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        Ok(self
            .head_identity(object, false)?
            .map(|identity| identity.length))
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        let mut bytes = Vec::new();
        self.read_range_into(object, offset, length, &mut bytes)?;
        Ok(bytes)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
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
            destination.clear();
            return Ok(());
        }
        if object == GENERATION_MANIFEST_FILE {
            if offset != 0 {
                return Err(SourceError::Protocol(
                    "manifest only supports a complete bounded GET".into(),
                ));
            }
            *destination = self.full_manifest(length)?;
            return Ok(());
        }
        for attempt in 0..=MAX_INCOMPLETE_RANGE_BODY_RETRIES {
            let mut response = self.range_response(object, offset, length)?;
            let result = read_bounded_into(&mut response, length, object, destination);
            self.record_returned_body(destination.len());
            match result {
                Ok(()) => return Ok(()),
                Err(error)
                    if attempt < MAX_INCOMPLETE_RANGE_BODY_RETRIES
                        && is_incomplete_range_body(&error, length) =>
                {
                    // Start over at the original offset. A partial body never
                    // enters a cache, and the next response repeats all range
                    // and object-identity validation in `range_response`.
                    self.record_incomplete_body_retry();
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("the bounded range retry loop always returns")
    }

    fn read_range_into_slice(
        &self,
        object: &str,
        offset: u64,
        destination: &mut [u8],
    ) -> SourceResult<()> {
        let length = destination.len();
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
            return Ok(());
        }
        if object == GENERATION_MANIFEST_FILE {
            if offset != 0 {
                return Err(SourceError::Protocol(
                    "manifest only supports a complete bounded GET".into(),
                ));
            }
            let bytes = self.full_manifest(length)?;
            destination.copy_from_slice(&bytes);
            return Ok(());
        }
        for attempt in 0..=MAX_INCOMPLETE_RANGE_BODY_RETRIES {
            let mut response = self.range_response(object, offset, length)?;
            let mut body_bytes = 0_usize;
            let result =
                read_bounded_into_slice(&mut response, destination, object, &mut body_bytes);
            self.record_returned_body(body_bytes);
            match result {
                Ok(()) => return Ok(()),
                Err(error)
                    if attempt < MAX_INCOMPLETE_RANGE_BODY_RETRIES
                        && is_incomplete_range_body(&error, length) =>
                {
                    self.record_incomplete_body_retry();
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("the bounded range retry loop always returns")
    }
}

fn strong_etag(headers: &HeaderMap, object: &str) -> SourceResult<Option<String>> {
    let mut values = headers.get_all(ETAG).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(SourceError::Protocol(format!(
            "response for {object} has multiple ETag fields"
        )));
    }
    let value = value.to_str().map_err(|_| {
        SourceError::Protocol(format!("response for {object} has a non-ASCII ETag"))
    })?;
    let (weak, opaque) = match value.strip_prefix("W/") {
        Some(opaque) => (true, opaque),
        None => (false, value),
    };
    let bytes = opaque.as_bytes();
    if bytes.len() < 2
        || bytes.first() != Some(&b'"')
        || bytes.last() != Some(&b'"')
        || bytes[1..bytes.len() - 1]
            .iter()
            .any(|byte| !matches!(*byte, 0x21 | 0x23..=0x7e))
    {
        return Err(SourceError::Protocol(format!(
            "response for {object} has a malformed ETag"
        )));
    }
    Ok((!weak).then(|| value.to_owned()))
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

fn is_incomplete_range_body(error: &SourceError, expected: usize) -> bool {
    match error {
        // This branch only receives errors produced while reading an already
        // validated response body. It includes transport timeouts, resets, and
        // reqwest's IncompleteBody/UnexpectedEof error.
        SourceError::Io { .. } => true,
        SourceError::ShortRead {
            expected: actual_expected,
            actual,
            ..
        } => *actual_expected == expected && *actual < expected,
        _ => false,
    }
}

fn read_bounded_into(
    response: &mut Response,
    expected: usize,
    object: &str,
    destination: &mut Vec<u8>,
) -> SourceResult<()> {
    let allocation = expected.checked_add(1).ok_or_else(|| {
        SourceError::Protocol(format!("response allocation bound overflows for {object}"))
    })?;
    destination.clear();
    if destination.capacity() < allocation {
        destination.try_reserve_exact(allocation).map_err(|error| {
            SourceError::Protocol(format!(
                "cannot reserve {allocation} bounded response bytes for {object}: {error}"
            ))
        })?;
    }
    let bound = u64::try_from(expected)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    response
        .take(bound)
        .read_to_end(destination)
        .map_err(|source| SourceError::Io {
            object: object.to_owned(),
            source,
        })?;
    if destination.len() != expected {
        return Err(SourceError::ShortRead {
            object: object.to_owned(),
            expected,
            actual: destination.len(),
        });
    }
    Ok(())
}

fn read_bounded_into_slice(
    response: &mut Response,
    destination: &mut [u8],
    object: &str,
    body_bytes: &mut usize,
) -> SourceResult<()> {
    let expected = destination.len();
    let mut read = 0_usize;
    *body_bytes = 0;
    while read < expected {
        let count = response
            .read(&mut destination[read..])
            .map_err(|source| SourceError::Io {
                object: object.to_owned(),
                source,
            })?;
        if count == 0 {
            return Err(SourceError::ShortRead {
                object: object.to_owned(),
                expected,
                actual: read,
            });
        }
        read += count;
        *body_bytes = read;
    }
    let mut extra = [0_u8; 1];
    let extra = response
        .read(&mut extra)
        .map_err(|source| SourceError::Io {
            object: object.to_owned(),
            source,
        })?;
    *body_bytes = read.saturating_add(extra);
    if extra != 0 {
        return Err(SourceError::ShortRead {
            object: object.to_owned(),
            expected,
            actual: expected.saturating_add(extra),
        });
    }
    Ok(())
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
    use std::{
        io::{Read as _, Write as _},
        net::TcpListener,
        thread,
    };

    use super::*;

    fn serve_once(
        expected_request_lines: Vec<&'static str>,
        responses: Vec<&'static [u8]>,
    ) -> (String, thread::JoinHandle<()>) {
        assert_eq!(expected_request_lines.len(), responses.len());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let task = thread::spawn(move || {
            for (expected, response) in expected_request_lines.into_iter().zip(responses) {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = stream.read(&mut buffer).unwrap();
                    assert_ne!(read, 0, "client closed before its request headers ended");
                    request.extend_from_slice(&buffer[..read]);
                    assert!(
                        request.len() <= 16 * 1024,
                        "request headers exceed test cap"
                    );
                }
                let request = String::from_utf8(request).unwrap();
                assert_eq!(request.lines().next(), Some(expected));
                stream.write_all(response).unwrap();
            }
        });
        (format!("http://{address}/gateway"), task)
    }

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

    #[test]
    fn shared_stats_count_exact_head_get_and_consumed_body_bytes() {
        let (base_url, server) = serve_once(
            vec![
                "HEAD /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 1-2/4\r\nConnection: close\r\n\r\nbc",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();
        let clone = source.clone();
        assert_eq!(source.size("thing.bin").unwrap(), Some(4));
        assert_eq!(clone.read_range("thing.bin", 1, 2).unwrap(), b"bc");
        assert_eq!(
            source.stats(),
            HttpRangeSourceStats {
                head_requests: 1,
                get_requests: 1,
                incomplete_body_retries: 0,
                returned_body_bytes: 2,
            }
        );
        assert_eq!(clone.stats(), source.stats());
        server.join().unwrap();
    }

    #[test]
    fn flat_epoch_layout_uses_the_simple_sample_path() {
        let (base_url, server) = serve_once(
            vec!["HEAD /gateway/7/thing.bin HTTP/1.1"],
            vec![b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n"],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                object_path_layout: HttpObjectPathLayout::FlatEpoch,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();

        assert_eq!(source.size("thing.bin").unwrap(), Some(4));
        server.join().unwrap();
    }

    #[test]
    fn exact_range_reads_reuse_vectors_and_fill_final_slices() {
        let (base_url, server) = serve_once(
            vec![
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/6\r\nConnection: close\r\n\r\nab",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 2-3/6\r\nConnection: close\r\n\r\ncd",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 4-5/6\r\nConnection: close\r\n\r\nef",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();

        let mut reusable = Vec::with_capacity(8);
        source
            .read_range_into("thing.bin", 0, 2, &mut reusable)
            .unwrap();
        let allocation = reusable.as_ptr();
        assert_eq!(reusable, b"ab");
        source
            .read_range_into("thing.bin", 2, 2, &mut reusable)
            .unwrap();
        assert_eq!(reusable.as_ptr(), allocation);
        assert_eq!(reusable, b"cd");

        let mut direct = [0_u8; 2];
        source
            .read_range_into_slice("thing.bin", 4, &mut direct)
            .unwrap();
        assert_eq!(&direct, b"ef");
        assert_eq!(source.stats().get_requests, 3);
        assert_eq!(source.stats().returned_body_bytes, 6);
        server.join().unwrap();
    }

    #[test]
    fn direct_slice_read_rejects_overlong_http_body_without_retry() {
        let (base_url, server) = serve_once(
            vec!["GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1"],
            vec![
                b"HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\nContent-Range: bytes 0-1/4\r\nConnection: close\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();
        let mut direct = [0_u8; 2];
        let overlong = source
            .read_range_into_slice("thing.bin", 0, &mut direct)
            .unwrap_err();
        assert!(matches!(
            overlong,
            SourceError::ShortRead {
                expected: 2,
                actual: 3,
                ..
            }
        ));
        assert_eq!(source.stats().get_requests, 1);
        assert_eq!(source.stats().incomplete_body_retries, 0);
        assert_eq!(source.stats().returned_body_bytes, 3);
        server.join().unwrap();
    }

    #[test]
    fn incomplete_range_body_retries_the_same_bound_range_and_counts_all_bytes() {
        let (base_url, server) = serve_once(
            vec![
                "HEAD /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 1-2/4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nb",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 1-2/4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nbc",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();

        assert_eq!(source.strong_identity("thing.bin").unwrap().length, 4);
        assert_eq!(source.read_range("thing.bin", 1, 2).unwrap(), b"bc");
        assert_eq!(
            source.stats(),
            HttpRangeSourceStats {
                head_requests: 1,
                get_requests: 2,
                incomplete_body_retries: 1,
                returned_body_bytes: 3,
            }
        );
        server.join().unwrap();
    }

    #[test]
    fn incomplete_range_body_retry_keeps_the_pinned_etag() {
        let (base_url, server) = serve_once(
            vec![
                "HEAD /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 1-2/4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\nb",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 1-2/4\r\nETag: \"v2\"\r\nConnection: close\r\n\r\nbc",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();

        assert_eq!(source.strong_identity("thing.bin").unwrap().length, 4);
        let error = source.read_range("thing.bin", 1, 2).unwrap_err();
        assert!(error.to_string().contains("changed strong ETag"));
        assert_eq!(source.stats().get_requests, 2);
        assert_eq!(source.stats().incomplete_body_retries, 1);
        assert_eq!(source.stats().returned_body_bytes, 1);
        server.join().unwrap();
    }

    #[test]
    fn incomplete_range_body_retry_is_bounded_for_direct_slices() {
        let (base_url, server) = serve_once(
            vec![
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\na",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\na",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\na",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();
        let mut direct = [0_u8; 2];

        assert!(
            source
                .read_range_into_slice("thing.bin", 0, &mut direct)
                .is_err()
        );
        assert_eq!(source.stats().get_requests, 3);
        assert_eq!(source.stats().incomplete_body_retries, 2);
        assert_eq!(source.stats().returned_body_bytes, 3);
        server.join().unwrap();
    }

    #[test]
    fn invalid_content_range_is_not_counted_as_body_bytes() {
        let (base_url, server) = serve_once(
            vec!["GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1"],
            vec![
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/4\r\nConnection: close\r\n\r\nab",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();
        assert!(source.read_range("thing.bin", 1, 2).is_err());
        assert_eq!(
            source.stats(),
            HttpRangeSourceStats {
                head_requests: 0,
                get_requests: 1,
                incomplete_body_retries: 0,
                returned_body_bytes: 0,
            }
        );
        server.join().unwrap();
    }

    #[test]
    fn head_size_is_pinned_against_later_range_totals() {
        let (base_url, server) = serve_once(
            vec![
                "HEAD /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 1-2/5\r\nConnection: close\r\n\r\nbc",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();
        assert_eq!(source.size("thing.bin").unwrap(), Some(4));
        let error = source.read_range("thing.bin", 1, 2).unwrap_err();
        assert!(error.to_string().contains("changed size from 4 to 5"));
        assert_eq!(source.stats().returned_body_bytes, 0);
        server.join().unwrap();
    }

    #[test]
    fn head_absence_is_pinned_against_later_head_and_range_presence() {
        let (base_url, server) = serve_once(
            vec![
                "HEAD /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "HEAD /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\na",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();

        assert_eq!(source.size("thing.bin").unwrap(), None);
        let head_error = source.strong_identity("thing.bin").unwrap_err();
        assert!(
            head_error
                .to_string()
                .contains("became present after its absence was pinned")
        );
        let read_error = source.read_range("thing.bin", 0, 1).unwrap_err();
        assert!(
            read_error
                .to_string()
                .contains("became present after its absence was pinned")
        );
        assert_eq!(source.stats().returned_body_bytes, 0);
        server.join().unwrap();
    }

    #[test]
    fn head_presence_is_pinned_against_later_head_and_range_absence() {
        let (base_url, server) = serve_once(
            vec![
                "HEAD /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "HEAD /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();

        assert_eq!(source.strong_identity("thing.bin").unwrap().length, 4);
        let head_error = source.size("thing.bin").unwrap_err();
        assert!(
            head_error
                .to_string()
                .contains("became absent after its presence was pinned")
        );
        let read_error = source.read_range("thing.bin", 0, 1).unwrap_err();
        assert!(
            read_error
                .to_string()
                .contains("became absent after its presence was pinned")
        );
        assert_eq!(source.stats().returned_body_bytes, 0);
        server.join().unwrap();
    }

    #[test]
    fn cache_identity_rejects_missing_and_weak_etags() {
        for response in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: W/\"v1\"\r\nConnection: close\r\n\r\n"
                .as_slice(),
        ] {
            let (base_url, server) = serve_once(
                vec!["HEAD /gateway/v1/epochs/7/files/thing.bin HTTP/1.1"],
                vec![response],
            );
            let source = HttpRangeSource::with_options(
                base_url,
                7,
                None,
                HttpRangeSourceOptions {
                    allow_insecure_http: true,
                    ..HttpRangeSourceOptions::default()
                },
            )
            .unwrap();
            let error = source.strong_identity("thing.bin").unwrap_err();
            assert!(error.to_string().contains("exact strong ETag"));
            server.join().unwrap();
        }
    }

    #[test]
    fn range_get_must_keep_the_pinned_strong_etag() {
        let (base_url, server) = serve_once(
            vec![
                "HEAD /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
                "GET /gateway/v1/epochs/7/files/thing.bin HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 1-2/4\r\nETag: \"v2\"\r\nConnection: close\r\n\r\nbc",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();
        assert_eq!(source.strong_identity("thing.bin").unwrap().length, 4);
        let error = source.read_range("thing.bin", 1, 2).unwrap_err();
        assert!(error.to_string().contains("changed strong ETag"));
        assert_eq!(source.stats().returned_body_bytes, 0);
        server.join().unwrap();
    }

    #[test]
    fn manifest_get_keeps_the_head_pinned_strong_etag() {
        let (base_url, server) = serve_once(
            vec![
                "HEAD /gateway/v1/epochs/7/manifest HTTP/1.1",
                "GET /gateway/v1/epochs/7/manifest HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\ntest",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();
        assert_eq!(source.size(GENERATION_MANIFEST_FILE).unwrap(), Some(4));
        assert_eq!(
            source.read_range(GENERATION_MANIFEST_FILE, 0, 4).unwrap(),
            b"test"
        );
        assert_eq!(
            source.stats(),
            HttpRangeSourceStats {
                head_requests: 1,
                get_requests: 1,
                incomplete_body_retries: 0,
                returned_body_bytes: 4,
            }
        );
        server.join().unwrap();
    }

    #[test]
    fn manifest_get_rejects_a_changed_head_pinned_strong_etag() {
        let (base_url, server) = serve_once(
            vec![
                "HEAD /gateway/v1/epochs/7/manifest HTTP/1.1",
                "GET /gateway/v1/epochs/7/manifest HTTP/1.1",
            ],
            vec![
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nETag: \"v2\"\r\nConnection: close\r\n\r\ntest",
            ],
        );
        let source = HttpRangeSource::with_options(
            base_url,
            7,
            None,
            HttpRangeSourceOptions {
                allow_insecure_http: true,
                ..HttpRangeSourceOptions::default()
            },
        )
        .unwrap();
        assert_eq!(source.size(GENERATION_MANIFEST_FILE).unwrap(), Some(4));
        let error = source
            .read_range(GENERATION_MANIFEST_FILE, 0, 4)
            .unwrap_err();
        assert!(error.to_string().contains("changed strong ETag"));
        assert_eq!(source.stats().returned_body_bytes, 0);
        server.join().unwrap();
    }
}
