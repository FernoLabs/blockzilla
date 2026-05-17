use sha2::{Digest, Sha256};
use url::Url;
use worker::{Bucket, Env, Error, Fetch, Headers, Method, Range, Request, RequestInit, Result};

const OLD_FAITHFUL_URL: &str = "https://files.old-faithful.net";
const DEFAULT_R2_BUCKET_BINDING: &str = "SLOT_RANGES";

#[derive(Clone)]
pub struct Storage {
    pub slot_index: ObjectSource,
    pub archive: ObjectSource,
}

impl Storage {
    pub fn from_env(env: &Env) -> Result<Self> {
        Ok(Self {
            slot_index: object_source_from_env(env, SourceRole::SlotIndex)?,
            archive: object_source_from_env(env, SourceRole::Archive)?,
        })
    }
}

#[derive(Clone)]
pub enum ObjectSource {
    R2 {
        bucket: Bucket,
        key_prefix: String,
    },
    Http {
        base_url: String,
        authorization: Option<String>,
    },
    S3 {
        endpoint: String,
        host: String,
        bucket: String,
        region: String,
        key_prefix: String,
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
    },
}

#[derive(Clone, Copy)]
enum SourceRole {
    SlotIndex,
    Archive,
}

impl SourceRole {
    fn kind_var(self) -> &'static str {
        match self {
            Self::SlotIndex => "OF_SLOT_INDEX_SOURCE",
            Self::Archive => "OF_ARCHIVE_SOURCE",
        }
    }

    fn default_kind(self) -> &'static str {
        match self {
            Self::SlotIndex => "r2",
            Self::Archive => "http",
        }
    }

    fn base_url_var(self) -> &'static str {
        match self {
            Self::SlotIndex => "OF_SLOT_INDEX_BASE_URL",
            Self::Archive => "OF_ARCHIVE_BASE_URL",
        }
    }

    fn prefix_var(self) -> &'static str {
        match self {
            Self::SlotIndex => "OF_SLOT_INDEX_KEY_PREFIX",
            Self::Archive => "OF_ARCHIVE_KEY_PREFIX",
        }
    }
}

impl ObjectSource {
    pub async fn get_text(&self, path: &str) -> SourceResult<String> {
        let bytes = self.get_bytes(path).await?;
        String::from_utf8(bytes).map_err(|err| SourceError::SourceBody {
            path: path.to_string(),
            reason: err.to_string(),
        })
    }

    pub async fn get_bytes(&self, path: &str) -> SourceResult<Vec<u8>> {
        match self {
            Self::R2 { bucket, .. } => {
                let key = self.object_key(path);
                let object = bucket
                    .get(&key)
                    .execute()
                    .await?
                    .ok_or_else(|| SourceError::SourceMissing { path: key.clone() })?;
                object_body_bytes(object.body(), &key).await
            }
            Self::Http { base_url, .. } => {
                let url = object_url(base_url, path);
                let mut response = self.fetch_http(&url, None).await?;
                let status = response.status_code();
                if status != 200 {
                    return Err(SourceError::SourceStatus {
                        path: url,
                        range: None,
                        status,
                    });
                }
                response
                    .bytes()
                    .await
                    .map_err(|err| SourceError::SourceBody {
                        path: path.to_string(),
                        reason: err.to_string(),
                    })
            }
            Self::S3 { .. } => {
                let (url, headers) = self.signed_s3_get(path, None)?;
                let mut response = fetch_with_headers(&url, headers).await?;
                let status = response.status_code();
                if status != 200 {
                    return Err(SourceError::SourceStatus {
                        path: url,
                        range: None,
                        status,
                    });
                }
                response
                    .bytes()
                    .await
                    .map_err(|err| SourceError::SourceBody {
                        path: path.to_string(),
                        reason: err.to_string(),
                    })
            }
        }
    }

    pub async fn get_range(&self, path: &str, offset: u64, len: usize) -> SourceResult<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(len as u64)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| SourceError::RangeOverflow {
                path: path.to_string(),
                offset,
                len,
            })?;
        let range_header = format!("bytes={offset}-{end}");

        match self {
            Self::R2 { bucket, .. } => {
                let key = self.object_key(path);
                let object = bucket
                    .get(&key)
                    .range(Range::OffsetWithLength {
                        offset,
                        length: len as u64,
                    })
                    .execute()
                    .await?
                    .ok_or_else(|| SourceError::SourceMissing { path: key.clone() })?;
                let bytes = object_body_bytes(object.body(), &key).await?;
                expect_len(bytes, &key, Some(range_header), len)
            }
            Self::Http { base_url, .. } => {
                let url = object_url(base_url, path);
                let mut response = self.fetch_http(&url, Some(&range_header)).await?;
                let status = response.status_code();
                if status != 206 {
                    return Err(SourceError::SourceStatus {
                        path: url,
                        range: Some(range_header),
                        status,
                    });
                }
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|err| SourceError::SourceBody {
                        path: path.to_string(),
                        reason: err.to_string(),
                    })?;
                expect_len(bytes, path, Some(range_header), len)
            }
            Self::S3 { .. } => {
                let (url, headers) = self.signed_s3_get(path, Some(&range_header))?;
                let mut response = fetch_with_headers(&url, headers).await?;
                let status = response.status_code();
                if status != 206 {
                    return Err(SourceError::SourceStatus {
                        path: url,
                        range: Some(range_header),
                        status,
                    });
                }
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|err| SourceError::SourceBody {
                        path: path.to_string(),
                        reason: err.to_string(),
                    })?;
                expect_len(bytes, path, Some(range_header), len)
            }
        }
    }

    fn object_key(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        let prefix = match self {
            Self::R2 { key_prefix, .. } | Self::S3 { key_prefix, .. } => key_prefix,
            Self::Http { .. } => "",
        };
        if prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{path}", prefix.trim_matches('/'))
        }
    }

    async fn fetch_http(
        &self,
        url: &str,
        range_header: Option<&str>,
    ) -> SourceResult<worker::Response> {
        let Self::Http { authorization, .. } = self else {
            unreachable!("fetch_http called for a non-HTTP source");
        };
        let headers = Headers::new();
        if let Some(range_header) = range_header {
            headers.set("Range", range_header)?;
        }
        if let Some(authorization) = authorization {
            headers.set("Authorization", authorization)?;
        }
        fetch_with_headers(url, headers).await
    }

    fn signed_s3_get(&self, path: &str, range: Option<&str>) -> SourceResult<(String, Headers)> {
        let Self::S3 {
            endpoint,
            host,
            bucket,
            region,
            access_key,
            secret_key,
            session_token,
            ..
        } = self
        else {
            unreachable!("signed_s3_get called for a non-S3 source");
        };

        let key = self.object_key(path);
        let canonical_uri = format!(
            "/{}/{}",
            uri_encode_path_segment(bucket),
            uri_encode_path(&key)
        );
        let url = format!("{endpoint}{canonical_uri}");
        let (date, amz_date) = amz_dates();

        let mut signed = vec![
            ("host".to_string(), host.clone()),
            (
                "x-amz-content-sha256".to_string(),
                "UNSIGNED-PAYLOAD".to_string(),
            ),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(range) = range {
            signed.push(("range".to_string(), range.to_string()));
        }
        if let Some(token) = session_token {
            signed.push(("x-amz-security-token".to_string(), token.clone()));
        }
        signed.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_headers = signed
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect::<String>();
        let signed_headers = signed
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_request = format!(
            "GET\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD"
        );
        let credential_scope = format!("{date}/{region}/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            hex_lower(&Sha256::digest(canonical_request.as_bytes()))
        );
        let signing_key = signing_key(secret_key, &date, region);
        let signature = hex_lower(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
        );

        let headers = Headers::new();
        headers.set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")?;
        headers.set("x-amz-date", &amz_date)?;
        headers.set("authorization", &authorization)?;
        if let Some(range) = range {
            headers.set("Range", range)?;
        }
        if let Some(token) = session_token {
            headers.set("x-amz-security-token", token)?;
        }

        Ok((url, headers))
    }
}

async fn fetch_with_headers(url: &str, headers: Headers) -> SourceResult<worker::Response> {
    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    init.with_headers(headers);
    let request = Request::new_with_init(url, &init)?;
    Fetch::Request(request).send().await.map_err(Into::into)
}

async fn object_body_bytes(
    body: Option<worker::ObjectBody<'_>>,
    path: &str,
) -> SourceResult<Vec<u8>> {
    let body = body.ok_or_else(|| SourceError::SourceBody {
        path: path.to_string(),
        reason: "object metadata did not include a body".to_string(),
    })?;
    body.bytes().await.map_err(Into::into)
}

fn expect_len(
    bytes: Vec<u8>,
    path: &str,
    range: Option<String>,
    expected_len: usize,
) -> SourceResult<Vec<u8>> {
    if bytes.len() == expected_len {
        Ok(bytes)
    } else {
        Err(SourceError::SourceBody {
            path: path.to_string(),
            reason: format!(
                "{} returned {} bytes, expected {expected_len}",
                range.unwrap_or_else(|| "object".to_string()),
                bytes.len()
            ),
        })
    }
}

fn object_source_from_env(env: &Env, role: SourceRole) -> Result<ObjectSource> {
    let kind = optional_var(env, role.kind_var())
        .or_else(|| optional_var(env, "OF_SOURCE_KIND"))
        .unwrap_or_else(|| role.default_kind().to_string())
        .to_ascii_lowercase();

    match kind.as_str() {
        "r2" => {
            let binding = optional_var(env, "OF_R2_BUCKET_BINDING")
                .unwrap_or_else(|| DEFAULT_R2_BUCKET_BINDING.to_string());
            Ok(ObjectSource::R2 {
                bucket: env.bucket(&binding)?,
                key_prefix: source_prefix(env, role),
            })
        }
        "http" | "https" => Ok(ObjectSource::Http {
            base_url: source_base_url(env, role),
            authorization: http_authorization(env),
        }),
        "s3" | "backblaze" | "b2" => s3_source_from_env(env, role),
        _ => Err(Error::RustError(format!(
            "{} must be one of r2, http, or s3; got {kind}",
            role.kind_var()
        ))),
    }
}

fn s3_source_from_env(env: &Env, role: SourceRole) -> Result<ObjectSource> {
    let endpoint = required_var(env, "OF_S3_ENDPOINT")?
        .trim_end_matches('/')
        .to_string();
    let parsed = Url::parse(&endpoint)
        .map_err(|err| Error::RustError(format!("OF_S3_ENDPOINT is not a valid URL: {err}")))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| Error::RustError("OF_S3_ENDPOINT must include a host".to_string()))?;
    let host = match parsed.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };

    Ok(ObjectSource::S3 {
        endpoint,
        host,
        bucket: required_var(env, "OF_S3_BUCKET")?,
        region: required_var(env, "OF_S3_REGION")?,
        key_prefix: source_prefix(env, role),
        access_key: required_var(env, "OF_S3_ACCESS_KEY_ID")?,
        secret_key: required_var(env, "OF_S3_SECRET_ACCESS_KEY")?,
        session_token: optional_var(env, "OF_S3_SESSION_TOKEN"),
    })
}

fn source_base_url(env: &Env, role: SourceRole) -> String {
    optional_var(env, role.base_url_var())
        .or_else(|| optional_var(env, "OF_HTTP_BASE_URL"))
        .unwrap_or_else(|| OLD_FAITHFUL_URL.to_string())
        .trim_end_matches('/')
        .to_string()
}

fn source_prefix(env: &Env, role: SourceRole) -> String {
    optional_var(env, role.prefix_var())
        .or_else(|| optional_var(env, "OF_KEY_PREFIX"))
        .unwrap_or_default()
        .trim_matches('/')
        .to_string()
}

fn http_authorization(env: &Env) -> Option<String> {
    optional_var(env, "OF_HTTP_AUTHORIZATION").or_else(|| {
        optional_var(env, "OF_HTTP_BEARER_TOKEN").map(|token| format!("Bearer {token}"))
    })
}

fn required_var(env: &Env, name: &str) -> Result<String> {
    env.var(name)
        .map(|value| value.to_string())
        .map_err(|_| Error::RustError(format!("missing required binding {name}")))
}

fn optional_var(env: &Env, name: &str) -> Option<String> {
    env.var(name)
        .ok()
        .map(|value| value.to_string())
        .filter(|value| !value.is_empty())
}

fn object_url(base_url: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn amz_dates() -> (String, String) {
    let now = js_sys::Date::new_0();
    let year = now.get_utc_full_year();
    let month = now.get_utc_month() + 1;
    let day = now.get_utc_date();
    let hour = now.get_utc_hours();
    let minute = now.get_utc_minutes();
    let second = now.get_utc_seconds();
    (
        format!("{year:04}{month:02}{day:02}"),
        format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"),
    )
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
    let mut out = String::new();
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

pub type SourceResult<T> = std::result::Result<T, SourceError>;

#[derive(Debug)]
pub enum SourceError {
    SourceMissing {
        path: String,
    },
    SourceStatus {
        path: String,
        range: Option<String>,
        status: u16,
    },
    SourceBody {
        path: String,
        reason: String,
    },
    RangeOverflow {
        path: String,
        offset: u64,
        len: usize,
    },
    Worker(Error),
}

impl SourceError {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::SourceMissing { .. } => 404,
            Self::SourceStatus { .. } | Self::SourceBody { .. } => 502,
            Self::RangeOverflow { .. } | Self::Worker(_) => 500,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::SourceMissing { .. } => "source_missing",
            Self::SourceStatus { .. } => "source_status",
            Self::SourceBody { .. } => "source_body",
            Self::RangeOverflow { .. } => "range_overflow",
            Self::Worker(_) => "worker_error",
        }
    }

    pub fn client_message(&self) -> String {
        match self {
            Self::SourceMissing { path } => format!("{path} is not available in the object source"),
            Self::SourceStatus {
                path,
                range,
                status,
            } => match range {
                Some(range) => format!("{path} returned HTTP {status} for {range}"),
                None => format!("{path} returned HTTP {status}"),
            },
            Self::SourceBody { path, reason } => {
                format!("{path} returned an invalid body: {reason}")
            }
            Self::RangeOverflow { path, offset, len } => {
                format!("range offset {offset} plus length {len} overflows for {path}")
            }
            Self::Worker(err) => err.to_string(),
        }
    }
}

impl From<Error> for SourceError {
    fn from(err: Error) -> Self {
        Self::Worker(err)
    }
}
