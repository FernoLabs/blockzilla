use super::{
    ApiError, MAX_API_ERROR_BODY_BYTES, MAX_CONTROL_RESPONSE_BYTES, MAX_RETRIES, Result,
    UploaderError, api_error_code, strict_json_value,
};
use reqwest::Method;
use reqwest::blocking::{Client, RequestBuilder, Response};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::io::Read;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

const AUTHORIZE_URL: &str = "https://api.backblazeb2.com/b2api/v4/b2_authorize_account";
const API_VERSION: &str = "v4";
const RETRYABLE: &[u16] = &[429, 500, 502, 503, 504];
const MAX_VERSION_ID_BYTES: usize = 1024;
const MAX_BUCKET_ID_BYTES: usize = 1024;
const MAX_EXACT_KEY_PAGES: usize = 8;
const EXACT_KEY_PAGE_SIZE: usize = 100;
const MAX_GENERATION_PAGES: usize = 64;
const GENERATION_PAGE_SIZE: usize = 1000;
const ACCOUNT_UNFINISHED_PAGE_SIZE: usize = 100;
const ACCOUNT_PART_PAGE_SIZE: usize = 1000;
const ACCOUNT_VERSION_PAGE_SIZE: usize = 1000;
const MAX_ACCOUNT_FILE_NAME_BYTES: usize = 4096;
const MAX_ACCOUNT_CURSOR_BYTES: usize = 1024;

// Account usage is intentionally exhaustive, but it must still terminate if a
// provider returns an endless stream of distinct pagination markers. These
// limits are far above a normal recorder account while keeping request count
// and marker-tracking memory finite.
const ACCOUNT_USAGE_LIMITS: AccountUsageLimits = AccountUsageLimits {
    maximum_buckets: 10_000,
    maximum_total_requests: 50_000,
    maximum_total_entries: 10_000_000,
    maximum_unfinished_pages_per_bucket: 2_000,
    maximum_unfinished_files_per_bucket: 200_000,
    maximum_part_pages_per_file: 32,
    maximum_parts_per_file: 10_000,
    maximum_version_pages_per_bucket: 5_000,
    maximum_versions_per_bucket: 1_000_000,
};

#[derive(Clone, Copy)]
struct AccountUsageLimits {
    maximum_buckets: u64,
    maximum_total_requests: u64,
    maximum_total_entries: u64,
    maximum_unfinished_pages_per_bucket: u64,
    maximum_unfinished_files_per_bucket: u64,
    maximum_part_pages_per_file: u64,
    maximum_parts_per_file: u64,
    maximum_version_pages_per_bucket: u64,
    maximum_versions_per_bucket: u64,
}

#[derive(Default)]
struct AccountUsageBudget {
    requests: u64,
    entries: u64,
}

impl AccountUsageBudget {
    fn reserve_request(&mut self, limits: AccountUsageLimits, operation: &str) -> Result<()> {
        reserve_bounded(
            &mut self.requests,
            1,
            limits.maximum_total_requests,
            &format!("{operation} account-wide request"),
        )
    }

    fn reserve_entry(&mut self, limits: AccountUsageLimits, operation: &str) -> Result<()> {
        reserve_bounded(
            &mut self.entries,
            1,
            limits.maximum_total_entries,
            &format!("{operation} account-wide returned entry"),
        )
    }
}

pub type NativeSnapshot = BTreeMap<String, Vec<Value>>;

pub struct B2NativeClient {
    application_key_id: String,
    application_key: String,
    retries: u32,
    authorize_url: String,
    client: Client,
    account_id: Option<String>,
    authorization_token: Option<String>,
    api_url: Option<String>,
    allowed: Option<Map<String, Value>>,
}

impl fmt::Debug for B2NativeClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("B2NativeClient")
            .field("application_key_id", &"<redacted>")
            .field("application_key", &"<redacted>")
            .field("retries", &self.retries)
            .field("authorize_url", &"<redacted>")
            .field(
                "account_id",
                &self.account_id.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "authorization_token",
                &self.authorization_token.as_ref().map(|_| "<redacted>"),
            )
            .field("api_url", &self.api_url.as_ref().map(|_| "<redacted>"))
            .finish_non_exhaustive()
    }
}

impl B2NativeClient {
    pub fn new(application_key_id: String, application_key: String, retries: u32) -> Result<Self> {
        Self::with_authorize_url(
            application_key_id,
            application_key,
            retries,
            AUTHORIZE_URL.into(),
        )
    }

    pub fn with_authorize_url(
        application_key_id: String,
        application_key: String,
        retries: u32,
        authorize_url: String,
    ) -> Result<Self> {
        if application_key_id.is_empty() || application_key.is_empty() {
            return Err(UploaderError::Config(
                "Backblaze application credentials must be non-empty".into(),
            ));
        }
        if retries > MAX_RETRIES {
            return Err(UploaderError::Config(format!(
                "retry count must be at most {MAX_RETRIES}"
            )));
        }
        let authorize_url = validate_api_url(&authorize_url, "Backblaze authorization URL")?;
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()?;
        Ok(Self {
            application_key_id,
            application_key,
            retries,
            authorize_url,
            client,
            account_id: None,
            authorization_token: None,
            api_url: None,
            allowed: None,
        })
    }

    fn request_json<F>(&self, operation: &str, mut build: F) -> Result<Value>
    where
        F: FnMut() -> RequestBuilder,
    {
        let mut last = None;
        for attempt in 1..=self.retries.saturating_add(1) {
            match build().send() {
                Ok(response) if response.status().is_success() => {
                    return bounded_json(response, operation);
                }
                Ok(response) => {
                    let status = response.status().as_u16();
                    let retry_after = retry_after(&response);
                    let code = bounded_error_code(response);
                    let error = ApiError {
                        operation: operation.into(),
                        status,
                        code,
                    };
                    if error.exit_status().is_some() || !RETRYABLE.contains(&status) {
                        return Err(error.into());
                    }
                    last = Some(UploaderError::Api(error));
                    if attempt <= self.retries {
                        let delay = retry_delay(attempt, retry_after);
                        eprintln!(
                            "retry {attempt}/{} after HTTP {status} for {operation}; sleep={}s",
                            self.retries,
                            delay.as_secs()
                        );
                        thread::sleep(delay);
                    }
                }
                Err(error) => {
                    if !retryable_transport(&error) {
                        return Err(UploaderError::Http(error));
                    }
                    let detail = error
                        .status()
                        .map(|status| format!("HTTP {}", status.as_u16()))
                        .unwrap_or_else(|| "transport error".into());
                    last = Some(UploaderError::Http(error));
                    if attempt <= self.retries {
                        let delay = retry_delay(attempt, None);
                        eprintln!(
                            "retry {attempt}/{} after {detail} for {operation}; sleep={}s",
                            self.retries,
                            delay.as_secs()
                        );
                        thread::sleep(delay);
                    }
                }
            }
        }
        Err(last.unwrap_or_else(|| {
            UploaderError::Protocol(format!("{operation} failed without a response"))
        }))
    }

    pub fn authorize(&mut self) -> Result<Value> {
        let url = self.authorize_url.clone();
        let key_id = self.application_key_id.clone();
        let key = self.application_key.clone();
        let payload = self.request_json("b2_authorize_account", || {
            self.client
                .get(&url)
                .basic_auth(key_id.clone(), Some(key.clone()))
        })?;
        let object = object(&payload, "b2_authorize_account")?;
        let account_id = required_string(object, "accountId", "b2_authorize_account")?;
        let authorization_token =
            required_string(object, "authorizationToken", "b2_authorize_account")?;
        let api_info = object
            .get("apiInfo")
            .and_then(Value::as_object)
            .ok_or_else(|| protocol("b2_authorize_account omitted apiInfo"))?;
        let storage = api_info
            .get("storageApi")
            .and_then(Value::as_object)
            .ok_or_else(|| protocol("b2_authorize_account omitted storageApi"))?;
        let api_url = required_string(storage, "apiUrl", "b2_authorize_account")?;
        let allowed = storage
            .get("allowed")
            .and_then(Value::as_object)
            .cloned()
            .ok_or_else(|| protocol("b2_authorize_account omitted allowed capabilities"))?;
        self.account_id = Some(account_id);
        self.authorization_token = Some(authorization_token);
        self.api_url = Some(validate_api_url(&api_url, "Backblaze storage API URL")?);
        self.allowed = Some(allowed);
        Ok(payload)
    }

    fn require_account_wide_list_access(&mut self) -> Result<()> {
        if self.allowed.is_none() {
            self.authorize()?;
        }
        let allowed = self.allowed.as_ref().expect("authorized");
        let capabilities = allowed
            .get("capabilities")
            .and_then(Value::as_array)
            .ok_or_else(|| protocol("Backblaze capability list is malformed"))?;
        let capabilities = capabilities
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| protocol("Backblaze capability list is malformed"))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        // Backblaze gates b2_list_parts behind writeFiles even though the
        // operation itself is read-only.
        let missing = ["listBuckets", "listFiles", "writeFiles"]
            .into_iter()
            .filter(|value| !capabilities.contains(value))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(protocol(format!(
                "Backblaze key lacks account usage capabilities: {}",
                missing.join(", ")
            )));
        }
        if ["bucketId", "bucketIds", "namePrefix"]
            .iter()
            .any(|key| allowed.get(*key).is_some_and(|value| !value.is_null()))
        {
            return Err(protocol(
                "Backblaze key is bucket- or prefix-restricted; account usage is incomplete",
            ));
        }
        Ok(())
    }

    fn require_file_version_list_access(&mut self, bucket_id: &str, key: &str) -> Result<()> {
        if self.allowed.is_none() {
            self.authorize()?;
        }
        let allowed = self.allowed.as_ref().expect("authorized");
        let capabilities = allowed
            .get("capabilities")
            .and_then(Value::as_array)
            .ok_or_else(|| protocol("Backblaze capability list is malformed"))?;
        if !capabilities
            .iter()
            .any(|value| value.as_str() == Some("listFiles"))
            || capabilities.iter().any(|value| !value.is_string())
        {
            return Err(protocol("Backblaze key lacks the listFiles capability"));
        }
        if let Some(value) = optional_string(allowed, "bucketId")?
            && value != bucket_id
        {
            return Err(protocol(
                "Backblaze key does not allow the configured bucket",
            ));
        }
        if let Some(value) = allowed.get("bucketIds").filter(|value| !value.is_null()) {
            let ids = value
                .as_array()
                .ok_or_else(|| protocol("Backblaze bucket restriction is malformed"))?;
            if ids.iter().any(|value| !value.is_string()) {
                return Err(protocol("Backblaze bucket restriction is malformed"));
            }
            if !ids.iter().any(|value| value.as_str() == Some(bucket_id)) {
                return Err(protocol(
                    "Backblaze key does not allow the configured bucket",
                ));
            }
        }
        if let Some(prefix) = optional_string(allowed, "namePrefix")?
            && !key.starts_with(&prefix)
        {
            return Err(protocol(
                "Backblaze key does not allow the requested object key",
            ));
        }
        Ok(())
    }

    pub fn api_request(
        &mut self,
        operation: &str,
        method: Method,
        params: &[(String, String)],
        body: Option<&Value>,
    ) -> Result<Value> {
        if self.authorization_token.is_none() || self.api_url.is_none() {
            self.authorize()?;
        }
        for authorization_attempt in 0..2 {
            let url = format!(
                "{}/b2api/{API_VERSION}/{operation}",
                self.api_url.as_deref().expect("authorized")
            );
            let token = self
                .authorization_token
                .as_deref()
                .expect("authorized")
                .to_string();
            let result = self.request_json(operation, || {
                let mut request = self
                    .client
                    .request(method.clone(), &url)
                    .header("Authorization", &token);
                if !params.is_empty() {
                    request = request.query(params);
                }
                if let Some(body) = body {
                    request = request.json(body);
                }
                request
            });
            match result {
                Err(UploaderError::Api(error))
                    if error.code == "expired_auth_token" && authorization_attempt == 0 =>
                {
                    self.authorize()?;
                }
                other => return other,
            }
        }
        Err(protocol(format!("{operation} authorization retry failed")))
    }
}

pub struct B2NativeObjectVerifier {
    client: B2NativeClient,
    bucket_id: String,
    bucket_name: String,
    bucket_identity_verified: bool,
}

impl B2NativeObjectVerifier {
    pub fn new(client: B2NativeClient, bucket_id: String, bucket_name: String) -> Result<Self> {
        if bucket_id.is_empty() {
            return Err(UploaderError::Config(
                "B2 bucket ID must be non-empty".into(),
            ));
        }
        if bucket_id.len() > MAX_BUCKET_ID_BYTES
            || bucket_id.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        {
            return Err(UploaderError::Config("B2 bucket ID is invalid".into()));
        }
        if bucket_name.is_empty() {
            return Err(UploaderError::Config(
                "B2 bucket name must be non-empty".into(),
            ));
        }
        Ok(Self {
            client,
            bucket_id,
            bucket_name,
            bucket_identity_verified: false,
        })
    }

    fn ensure_bucket_identity(&mut self) -> Result<()> {
        if self.bucket_identity_verified {
            return Ok(());
        }
        if self.client.allowed.is_none() {
            self.client.authorize()?;
        }
        let account_id = self
            .client
            .account_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| protocol("b2_authorize_account omitted accountId"))?
            .to_string();
        let allowed = self.client.allowed.as_ref().expect("authorized");
        if let Some(allowed_bucket_id) = optional_string(allowed, "bucketId")? {
            if allowed_bucket_id != self.bucket_id {
                return Err(protocol(
                    "Backblaze key does not allow the configured bucket",
                ));
            }
            if let Some(allowed_bucket_name) = optional_string(allowed, "bucketName")? {
                if allowed_bucket_name != self.bucket_name {
                    return Err(protocol(
                        "Backblaze bucket ID does not match the configured bucket name",
                    ));
                }
                self.bucket_identity_verified = true;
                return Ok(());
            }
        }
        let can_list_buckets = allowed
            .get("capabilities")
            .and_then(Value::as_array)
            .is_some_and(|values| {
                values
                    .iter()
                    .any(|value| value.as_str() == Some("listBuckets"))
            });
        if !can_list_buckets {
            return Err(protocol(
                "Backblaze key cannot prove the configured bucket ID/name mapping",
            ));
        }
        let payload = self.client.api_request(
            "b2_list_buckets",
            Method::POST,
            &[],
            Some(&json!({
                "accountId": account_id,
                "bucketId": self.bucket_id,
                "bucketTypes": ["all"],
            })),
        )?;
        let buckets = response_objects(&payload, "buckets", "b2_list_buckets")?;
        if buckets.len() != 1 {
            return Err(protocol(
                "b2_list_buckets did not return exactly the configured bucket",
            ));
        }
        let bucket = buckets[0];
        if bucket.get("accountId").and_then(Value::as_str) != Some(&account_id)
            || bucket.get("bucketId").and_then(Value::as_str) != Some(&self.bucket_id)
            || bucket.get("bucketName").and_then(Value::as_str) != Some(&self.bucket_name)
        {
            return Err(protocol(
                "Backblaze bucket ID does not match the configured bucket name/account",
            ));
        }
        self.bucket_identity_verified = true;
        Ok(())
    }

    fn ensure_access(&mut self, key: &str) -> Result<()> {
        self.client
            .require_file_version_list_access(&self.bucket_id, key)?;
        self.ensure_bucket_identity()
    }

    fn version_identity(&self, version: &Map<String, Value>, key: &str) -> Result<String> {
        if version.get("fileName").and_then(Value::as_str) != Some(key) {
            return Err(protocol(
                "b2_list_file_versions returned a different object key",
            ));
        }
        let file_id = version
            .get("fileId")
            .and_then(Value::as_str)
            .ok_or_else(|| protocol("b2_list_file_versions returned an invalid fileId"))?;
        validate_version_id(file_id, "B2 file ID")?;
        if version.get("accountId").and_then(Value::as_str) != self.client.account_id.as_deref() {
            return Err(protocol(
                "b2_list_file_versions returned a different accountId",
            ));
        }
        if version.get("bucketId").and_then(Value::as_str) != Some(&self.bucket_id) {
            return Err(protocol(
                "b2_list_file_versions returned a different bucketId",
            ));
        }
        Ok(file_id.to_string())
    }

    fn validate_versions(
        &self,
        versions: &[Value],
        key: &str,
        expected_size: u64,
        expected_sha256: &str,
        expected_sha1: &str,
        expected_etag: &str,
    ) -> Result<Vec<String>> {
        validate_digest(expected_sha256, 64, "expected SHA-256")?;
        validate_digest(expected_sha1, 40, "expected SHA-1")?;
        let expected_sha256 = expected_sha256.to_ascii_lowercase();
        let expected_sha1 = expected_sha1.to_ascii_lowercase();
        let expected_etag = normalize_etag(expected_etag, "expected ETag")?;
        let mut ids = Vec::with_capacity(versions.len());
        for version in versions {
            let version = version
                .as_object()
                .ok_or_else(|| protocol("b2_list_file_versions returned malformed entry"))?;
            let file_id = self.version_identity(version, key)?;
            let action = version.get("action").and_then(Value::as_str);
            if action != Some("upload") {
                return Err(protocol(format!(
                    "immutable B2 key {key} contains unsupported action {action:?}"
                )));
            }
            let size = nonnegative(
                version.get("contentLength"),
                &format!("b2_list_file_versions {key} contentLength"),
            )?;
            let md5 = normalize_etag(
                version
                    .get("contentMd5")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                &format!("b2_list_file_versions {key} contentMd5"),
            )?;
            let remote_sha1 = version
                .get("contentSha1")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    protocol(format!("b2_list_file_versions {key} omitted contentSha1"))
                })?
                .to_ascii_lowercase();
            if remote_sha1.len() == 40 && remote_sha1.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                if remote_sha1 != expected_sha1 {
                    return Err(protocol(format!(
                        "immutable B2 key {key} has conflicting SHA-1 metadata"
                    )));
                }
            } else if let Some(unverified) = remote_sha1.strip_prefix("unverified:") {
                if unverified.len() != 40
                    || !unverified.bytes().all(|byte| byte.is_ascii_hexdigit())
                    || unverified != expected_sha1
                {
                    return Err(protocol(format!(
                        "immutable B2 key {key} has conflicting SHA-1 metadata"
                    )));
                }
            } else if remote_sha1 != "none" {
                return Err(protocol(format!(
                    "b2_list_file_versions {key} returned malformed contentSha1"
                )));
            }
            let file_info = version
                .get("fileInfo")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    protocol(format!(
                        "b2_list_file_versions {key} returned malformed fileInfo"
                    ))
                })?;
            let remote_sha256 =
                file_info
                    .get("sha256")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        protocol(format!(
                            "b2_list_file_versions {key} omitted SHA-256 metadata"
                        ))
                    })?;
            validate_digest(
                remote_sha256,
                64,
                &format!("b2_list_file_versions {key} SHA-256"),
            )?;
            if size != expected_size
                || remote_sha256.to_ascii_lowercase() != expected_sha256
                || md5 != expected_etag
            {
                return Err(protocol(format!(
                    "immutable B2 key {key} has conflicting object metadata"
                )));
            }
            ids.push(file_id);
        }
        Ok(ids)
    }

    pub fn list_generation_versions(
        &mut self,
        remote_prefix: &str,
        allowed_keys: &BTreeSet<String>,
    ) -> Result<NativeSnapshot> {
        let prefix = normalize_remote_prefix(remote_prefix)? + "/";
        if allowed_keys.is_empty() {
            return Err(UploaderError::Config(
                "generation snapshot requires a non-empty key set".into(),
            ));
        }
        for key in allowed_keys {
            validate_object_key(key)?;
            if !key.starts_with(&prefix) {
                return Err(UploaderError::Config(
                    "generation snapshot key is outside the remote prefix".into(),
                ));
            }
        }
        self.ensure_access(&prefix)?;
        let mut snapshot = allowed_keys
            .iter()
            .map(|key| (key.clone(), Vec::new()))
            .collect::<NativeSnapshot>();
        let mut next_name = prefix.clone();
        let mut next_id: Option<String> = None;
        let mut previous_name: Option<String> = None;
        let mut seen_ids = HashSet::new();
        let mut seen_markers = HashSet::new();
        for _ in 0..MAX_GENERATION_PAGES {
            let mut params = vec![
                ("bucketId".into(), self.bucket_id.clone()),
                ("prefix".into(), prefix.clone()),
                ("startFileName".into(), next_name.clone()),
                ("maxFileCount".into(), GENERATION_PAGE_SIZE.to_string()),
            ];
            if let Some(id) = &next_id {
                params.push(("startFileId".into(), id.clone()));
            }
            let payload =
                self.client
                    .api_request("b2_list_file_versions", Method::GET, &params, None)?;
            let versions = payload
                .get("files")
                .and_then(Value::as_array)
                .ok_or_else(|| protocol("b2_list_file_versions returned a malformed files list"))?;
            if versions.len() > GENERATION_PAGE_SIZE {
                return Err(protocol(
                    "b2_list_file_versions exceeded the requested generation page size",
                ));
            }
            for version in versions {
                let object = version.as_object().ok_or_else(|| {
                    protocol("b2_list_file_versions returned a malformed files entry")
                })?;
                let name = object
                    .get("fileName")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| {
                        protocol("b2_list_file_versions returned an invalid fileName")
                    })?;
                if !name.starts_with(&prefix) {
                    return Err(protocol(
                        "b2_list_file_versions returned a key outside the generation prefix",
                    ));
                }
                if name < next_name.as_str() {
                    return Err(protocol(
                        "b2_list_file_versions returned an entry before its generation cursor",
                    ));
                }
                if previous_name
                    .as_deref()
                    .is_some_and(|previous| name < previous)
                {
                    return Err(protocol(
                        "b2_list_file_versions returned out-of-order file names",
                    ));
                }
                previous_name = Some(name.into());
                if !allowed_keys.contains(name) {
                    return Err(protocol(format!(
                        "immutable B2 generation contains unexpected key {name}"
                    )));
                }
                let id = self.version_identity(object, name)?;
                if !seen_ids.insert(id) {
                    return Err(protocol(
                        "b2_list_file_versions returned a duplicate fileId",
                    ));
                }
                if object.get("action").and_then(Value::as_str) != Some("upload") {
                    return Err(protocol(format!(
                        "immutable B2 key {name} contains unsupported action {:?}",
                        object.get("action")
                    )));
                }
                snapshot
                    .get_mut(name)
                    .expect("allowed key")
                    .push(version.clone());
            }
            let following_name = payload.get("nextFileName");
            let following_id = payload.get("nextFileId");
            if matches!(following_name, None | Some(Value::Null))
                && matches!(following_id, None | Some(Value::Null))
            {
                return Ok(snapshot);
            }
            let name = bounded_response_string(
                following_name,
                "b2_list_file_versions generation nextFileName",
                MAX_ACCOUNT_FILE_NAME_BYTES,
            )?;
            if !name.starts_with(&prefix)
                || previous_name
                    .as_deref()
                    .is_some_and(|previous| name < previous)
            {
                return Err(protocol(
                    "b2_list_file_versions returned a backtracking generation cursor",
                ));
            }
            let id = bounded_response_string(
                following_id,
                "b2_list_file_versions generation nextFileId",
                MAX_ACCOUNT_CURSOR_BYTES,
            )?;
            if !seen_markers.insert(cursor_fingerprint(&[name, id])) {
                return Err(protocol("b2_list_file_versions pagination did not advance"));
            }
            next_name = name.to_string();
            next_id = Some(id.to_string());
        }
        Err(protocol(
            "b2_list_file_versions exceeded the generation-prefix page limit",
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn snapshot_exact_version(
        &self,
        snapshot: &NativeSnapshot,
        key: &str,
        expected_size: u64,
        expected_sha256: &str,
        expected_sha1: &str,
        expected_etag: &str,
        pinned_version_id: Option<&str>,
    ) -> Result<Option<String>> {
        let versions = snapshot.get(key).ok_or_else(|| {
            UploaderError::Config("object key is outside the generation snapshot".into())
        })?;
        let ids = self.validate_versions(
            versions,
            key,
            expected_size,
            expected_sha256,
            expected_sha1,
            expected_etag,
        )?;
        if let Some(pinned) = pinned_version_id {
            validate_version_id(pinned, "pinned B2 file ID")?;
            if !ids.iter().any(|id| id == pinned) {
                return Err(protocol(format!(
                    "b2_list_file_versions {key} returned a different pinned version"
                )));
            }
        }
        Ok(ids.into_iter().next())
    }

    pub fn latest_exact_version(
        &mut self,
        key: &str,
        expected_size: u64,
        expected_sha256: &str,
        expected_sha1: &str,
        expected_etag: &str,
    ) -> Result<Option<String>> {
        validate_object_key(key)?;
        self.ensure_access(key)?;
        let mut versions = Vec::new();
        let mut next_name = key.to_string();
        let mut next_id: Option<String> = None;
        let mut seen_markers = HashSet::new();
        let mut seen_ids = HashSet::new();
        let mut completed = false;
        for _ in 0..MAX_EXACT_KEY_PAGES {
            let mut params = vec![
                ("bucketId".into(), self.bucket_id.clone()),
                ("prefix".into(), key.into()),
                ("startFileName".into(), next_name.clone()),
                ("maxFileCount".into(), EXACT_KEY_PAGE_SIZE.to_string()),
            ];
            if let Some(id) = &next_id {
                params.push(("startFileId".into(), id.clone()));
            }
            let payload =
                self.client
                    .api_request("b2_list_file_versions", Method::GET, &params, None)?;
            let page = payload
                .get("files")
                .and_then(Value::as_array)
                .ok_or_else(|| protocol("b2_list_file_versions returned a malformed files list"))?;
            if page.len() > EXACT_KEY_PAGE_SIZE {
                return Err(protocol(
                    "b2_list_file_versions exceeded the requested exact-key page size",
                ));
            }
            let mut passed = false;
            let mut previous = None;
            for value in page {
                let object = value.as_object().ok_or_else(|| {
                    protocol("b2_list_file_versions returned a malformed files entry")
                })?;
                let name = object
                    .get("fileName")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| {
                        protocol("b2_list_file_versions returned an invalid fileName")
                    })?;
                if previous.is_some_and(|previous: &str| name < previous) {
                    return Err(protocol(
                        "b2_list_file_versions returned out-of-order file names",
                    ));
                }
                previous = Some(name);
                if name < key {
                    return Err(protocol(
                        "b2_list_file_versions returned a file before the requested key",
                    ));
                }
                if name != key {
                    passed = true;
                    continue;
                }
                if passed {
                    return Err(protocol(
                        "b2_list_file_versions returned a discontiguous exact key",
                    ));
                }
                let id = self.version_identity(object, key)?;
                if !seen_ids.insert(id) {
                    return Err(protocol(
                        "b2_list_file_versions returned a duplicate fileId",
                    ));
                }
                versions.push(value.clone());
            }
            let name = payload.get("nextFileName");
            let id = payload.get("nextFileId");
            if matches!(name, None | Some(Value::Null)) && matches!(id, None | Some(Value::Null)) {
                completed = true;
                break;
            }
            let name = bounded_response_string(
                name,
                "b2_list_file_versions exact-key nextFileName",
                MAX_ACCOUNT_FILE_NAME_BYTES,
            )?;
            let id = bounded_response_string(
                id,
                "b2_list_file_versions exact-key nextFileId",
                MAX_ACCOUNT_CURSOR_BYTES,
            )?;
            if name < key {
                return Err(protocol(
                    "b2_list_file_versions pagination moved before the requested key",
                ));
            }
            if passed || name > key {
                completed = true;
                break;
            }
            if !seen_markers.insert(cursor_fingerprint(&[name, id])) {
                return Err(protocol("b2_list_file_versions pagination did not advance"));
            }
            next_name = name.to_string();
            next_id = Some(id.to_string());
        }
        if !completed {
            return Err(protocol(
                "b2_list_file_versions exceeded the exact-key page limit",
            ));
        }
        let ids = self.validate_versions(
            &versions,
            key,
            expected_size,
            expected_sha256,
            expected_sha1,
            expected_etag,
        )?;
        let Some(selected) = ids.into_iter().next() else {
            return Ok(None);
        };
        let pinned = self.seek_exact_version(key, &selected)?;
        self.validate_versions(
            &[pinned],
            key,
            expected_size,
            expected_sha256,
            expected_sha1,
            expected_etag,
        )?;
        Ok(Some(selected))
    }

    pub fn verify_exact_version(
        &mut self,
        key: &str,
        expected_size: u64,
        expected_sha256: &str,
        expected_sha1: &str,
        version_id: &str,
        expected_etag: &str,
    ) -> Result<()> {
        let version = self.seek_exact_version(key, version_id)?;
        self.validate_versions(
            &[version],
            key,
            expected_size,
            expected_sha256,
            expected_sha1,
            expected_etag,
        )?;
        Ok(())
    }

    fn seek_exact_version(&mut self, key: &str, version_id: &str) -> Result<Value> {
        validate_object_key(key)?;
        validate_version_id(version_id, "version ID")?;
        self.ensure_access(key)?;
        let params = vec![
            ("bucketId".into(), self.bucket_id.clone()),
            ("prefix".into(), key.into()),
            ("startFileName".into(), key.into()),
            ("startFileId".into(), version_id.into()),
            ("maxFileCount".into(), "1".into()),
        ];
        let payload =
            self.client
                .api_request("b2_list_file_versions", Method::GET, &params, None)?;
        let versions = payload
            .get("files")
            .and_then(Value::as_array)
            .ok_or_else(|| protocol("b2_list_file_versions returned a malformed files list"))?;
        if versions.len() != 1 {
            return Err(protocol(format!(
                "b2_list_file_versions {key} did not return the pinned object version"
            )));
        }
        let object = versions[0]
            .as_object()
            .ok_or_else(|| protocol("b2_list_file_versions returned a malformed files entry"))?;
        let returned = self.version_identity(object, key)?;
        if returned != version_id {
            return Err(protocol(format!(
                "b2_list_file_versions {key} returned a different pinned version"
            )));
        }
        Ok(versions[0].clone())
    }

    pub fn into_client(self) -> B2NativeClient {
        self.client
    }
}

impl ApiError {
    fn exit_status(&self) -> Option<u8> {
        match self.code.as_str() {
            "download_cap_exceeded" => Some(20),
            "transaction_cap_exceeded" => Some(21),
            "storage_cap_exceeded" | "cap_exceeded" => Some(22),
            _ => None,
        }
    }
}

#[derive(Default)]
struct UnfinishedUsage {
    large_file_count: u64,
    large_file_pages: u64,
    part_bytes: u64,
    part_count: u64,
    part_pages: u64,
}

#[derive(Default)]
struct VersionUsage {
    folder_count: u64,
    hide_count: u64,
    start_count: u64,
    stored_bytes: u64,
    upload_count: u64,
    pages: u64,
}

#[derive(Debug, Serialize)]
struct AccountUsage {
    bucket_count: u64,
    folder_entry_count: u64,
    hide_marker_count: u64,
    schema_version: u64,
    scope: &'static str,
    /// True means every bucket/page visible during the scan was enumerated.
    /// Backblaze does not provide a cross-bucket read snapshot, so this is not
    /// an atomic point-in-time total when objects mutate concurrently.
    scope_complete: bool,
    scanned_unix_secs: u64,
    start_marker_count: u64,
    stored_upload_bytes: u64,
    total_stored_bytes: u64,
    unfinished_large_file_count: u64,
    unfinished_large_file_page_count: u64,
    unfinished_part_bytes: u64,
    unfinished_part_count: u64,
    unfinished_part_page_count: u64,
    upload_version_count: u64,
    version_page_count: u64,
}

/// Enumerate every account object/version and unfinished part visible while
/// the scan progresses. `scope_complete=true` means the bounded enumeration
/// reached every terminal cursor; it is not an atomic point-in-time snapshot
/// when the account is mutated concurrently.
pub fn account_usage(client: &mut B2NativeClient) -> Result<Value> {
    account_usage_with_limits(client, ACCOUNT_USAGE_LIMITS)
}

fn account_usage_with_limits(
    client: &mut B2NativeClient,
    limits: AccountUsageLimits,
) -> Result<Value> {
    client.authorize()?;
    client.require_account_wide_list_access()?;
    let account_id = client
        .account_id
        .as_deref()
        .expect("authorized")
        .to_string();
    let mut budget = AccountUsageBudget::default();
    budget.reserve_request(limits, "b2_list_buckets")?;
    let payload = client.api_request(
        "b2_list_buckets",
        Method::POST,
        &[],
        Some(&json!({"accountId": account_id, "bucketTypes": ["all"]})),
    )?;
    let mut bucket_ids = Vec::new();
    let mut bucket_count = 0;
    let mut seen = HashSet::new();
    let maximum_buckets = usize::try_from(limits.maximum_buckets)
        .map_err(|_| protocol("b2_list_buckets bucket safety limit is not representable"))?;
    for bucket in response_objects_bounded(&payload, "buckets", "b2_list_buckets", maximum_buckets)?
    {
        let id = bounded_response_string(
            bucket.get("bucketId"),
            "b2_list_buckets bucketId",
            MAX_BUCKET_ID_BYTES,
        )?;
        let fingerprint = cursor_fingerprint(&[id]);
        if seen.contains(&fingerprint) {
            return Err(protocol(
                "b2_list_buckets returned an invalid or duplicate bucketId",
            ));
        }
        budget.reserve_entry(limits, "b2_list_buckets")?;
        reserve_bounded(
            &mut bucket_count,
            1,
            limits.maximum_buckets,
            "b2_list_buckets bucket",
        )?;
        seen.insert(fingerprint);
        bucket_ids.push(id.to_string());
    }

    let mut unfinished = UnfinishedUsage::default();
    let mut versions = VersionUsage::default();
    for bucket_id in &bucket_ids {
        add_unfinished(
            &mut unfinished,
            list_unfinished_part_usage(client, bucket_id, limits, &mut budget)?,
        )?;
        add_versions(
            &mut versions,
            list_file_version_usage(client, bucket_id, limits, &mut budget)?,
        )?;
    }
    let total = checked_add(
        versions.stored_bytes,
        unfinished.part_bytes,
        "Backblaze total stored bytes",
    )?;
    let scanned_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| protocol("system clock is before the Unix epoch"))?
        .as_secs();
    Ok(serde_json::to_value(AccountUsage {
        bucket_count: bucket_ids.len() as u64,
        folder_entry_count: versions.folder_count,
        hide_marker_count: versions.hide_count,
        schema_version: 1,
        scope: "account",
        scope_complete: true,
        scanned_unix_secs,
        start_marker_count: versions.start_count,
        stored_upload_bytes: versions.stored_bytes,
        total_stored_bytes: total,
        unfinished_large_file_count: unfinished.large_file_count,
        unfinished_large_file_page_count: unfinished.large_file_pages,
        unfinished_part_bytes: unfinished.part_bytes,
        unfinished_part_count: unfinished.part_count,
        unfinished_part_page_count: unfinished.part_pages,
        upload_version_count: versions.upload_count,
        version_page_count: versions.pages,
    })?)
}

fn list_unfinished_part_usage(
    client: &mut B2NativeClient,
    bucket_id: &str,
    limits: AccountUsageLimits,
    budget: &mut AccountUsageBudget,
) -> Result<UnfinishedUsage> {
    let mut totals = UnfinishedUsage::default();
    let mut next_file_id: Option<String> = None;
    let mut seen_markers = HashSet::new();
    let mut seen_files = HashSet::new();
    loop {
        reserve_bounded(
            &mut totals.large_file_pages,
            1,
            limits.maximum_unfinished_pages_per_bucket,
            "b2_list_unfinished_large_files per-bucket page",
        )?;
        budget.reserve_request(limits, "b2_list_unfinished_large_files")?;
        let mut params = vec![
            ("bucketId".into(), bucket_id.into()),
            (
                "maxFileCount".into(),
                ACCOUNT_UNFINISHED_PAGE_SIZE.to_string(),
            ),
        ];
        if let Some(marker) = &next_file_id {
            params.push(("startFileId".into(), marker.clone()));
        }
        let payload =
            client.api_request("b2_list_unfinished_large_files", Method::GET, &params, None)?;
        for unfinished in response_objects_bounded(
            &payload,
            "files",
            "b2_list_unfinished_large_files",
            ACCOUNT_UNFINISHED_PAGE_SIZE,
        )? {
            let file_id = bounded_response_string(
                unfinished.get("fileId"),
                "b2_list_unfinished_large_files fileId",
                MAX_ACCOUNT_CURSOR_BYTES,
            )?;
            let file_fingerprint = cursor_fingerprint(&[file_id]);
            if seen_files.contains(&file_fingerprint) {
                return Err(protocol(
                    "b2_list_unfinished_large_files returned a duplicate fileId",
                ));
            }
            reserve_bounded(
                &mut totals.large_file_count,
                1,
                limits.maximum_unfinished_files_per_bucket,
                "b2_list_unfinished_large_files per-bucket object",
            )?;
            budget.reserve_entry(limits, "b2_list_unfinished_large_files")?;
            seen_files.insert(file_fingerprint);
            let mut next_part: Option<u64> = None;
            let mut previous_part_cursor = None;
            let mut last_part_number = 0;
            let mut file_part_pages = 0;
            let mut file_part_count = 0;
            loop {
                reserve_bounded(
                    &mut file_part_pages,
                    1,
                    limits.maximum_part_pages_per_file,
                    "b2_list_parts per-file page",
                )?;
                budget.reserve_request(limits, "b2_list_parts")?;
                let mut part_params = vec![
                    ("fileId".into(), file_id.to_string()),
                    ("maxPartCount".into(), ACCOUNT_PART_PAGE_SIZE.to_string()),
                ];
                if let Some(marker) = next_part {
                    part_params.push(("startPartNumber".into(), marker.to_string()));
                }
                let parts = client.api_request("b2_list_parts", Method::GET, &part_params, None)?;
                totals.part_pages = checked_add(totals.part_pages, 1, "part page count")?;
                for part in response_objects_bounded(
                    &parts,
                    "parts",
                    "b2_list_parts",
                    ACCOUNT_PART_PAGE_SIZE,
                )? {
                    let number = nonnegative(part.get("partNumber"), "Backblaze part number")?;
                    if number <= last_part_number || next_part.is_some_and(|cursor| number < cursor)
                    {
                        return Err(protocol(
                            "b2_list_parts returned a non-increasing part number",
                        ));
                    }
                    reserve_bounded(
                        &mut file_part_count,
                        1,
                        limits.maximum_parts_per_file,
                        "b2_list_parts per-file part",
                    )?;
                    budget.reserve_entry(limits, "b2_list_parts")?;
                    last_part_number = number;
                    totals.part_count = checked_add(totals.part_count, 1, "part count")?;
                    totals.part_bytes = checked_add(
                        totals.part_bytes,
                        nonnegative(part.get("contentLength"), "Backblaze part contentLength")?,
                        "unfinished part bytes",
                    )?;
                }
                match parts.get("nextPartNumber") {
                    None | Some(Value::Null) => break,
                    value => {
                        let marker = nonnegative(value, "Backblaze nextPartNumber")?;
                        if marker <= last_part_number
                            || previous_part_cursor.is_some_and(|previous| marker <= previous)
                        {
                            return Err(protocol("b2_list_parts pagination did not advance"));
                        }
                        previous_part_cursor = Some(marker);
                        next_part = Some(marker);
                    }
                }
            }
        }
        match payload.get("nextFileId") {
            None | Some(Value::Null) => break,
            value => {
                let marker = bounded_response_string(
                    value,
                    "b2_list_unfinished_large_files nextFileId",
                    MAX_ACCOUNT_CURSOR_BYTES,
                )?;
                insert_bounded_marker(
                    &mut seen_markers,
                    cursor_fingerprint(&[marker]),
                    limits.maximum_unfinished_pages_per_bucket,
                    "b2_list_unfinished_large_files",
                )?;
                next_file_id = Some(marker.to_string());
            }
        }
    }
    Ok(totals)
}

fn list_file_version_usage(
    client: &mut B2NativeClient,
    bucket_id: &str,
    limits: AccountUsageLimits,
    budget: &mut AccountUsageBudget,
) -> Result<VersionUsage> {
    let mut totals = VersionUsage::default();
    let mut version_count = 0;
    let mut next: Option<(String, String)> = None;
    let mut seen_markers = HashSet::new();
    let mut seen_versions = HashSet::new();
    loop {
        reserve_bounded(
            &mut totals.pages,
            1,
            limits.maximum_version_pages_per_bucket,
            "b2_list_file_versions per-bucket page",
        )?;
        budget.reserve_request(limits, "b2_list_file_versions")?;
        let mut params = vec![
            ("bucketId".into(), bucket_id.into()),
            ("maxFileCount".into(), ACCOUNT_VERSION_PAGE_SIZE.to_string()),
        ];
        if let Some((name, id)) = &next {
            params.push(("startFileName".into(), name.clone()));
            params.push(("startFileId".into(), id.clone()));
        }
        let payload = client.api_request("b2_list_file_versions", Method::GET, &params, None)?;
        for version in response_objects_bounded(
            &payload,
            "files",
            "b2_list_file_versions",
            ACCOUNT_VERSION_PAGE_SIZE,
        )? {
            let action =
                bounded_response_string(version.get("action"), "b2_list_file_versions action", 16)?;
            let file_name = bounded_response_string(
                version.get("fileName"),
                "b2_list_file_versions fileName",
                MAX_ACCOUNT_FILE_NAME_BYTES,
            )?;
            let identity = match version.get("fileId") {
                Some(Value::String(file_id)) if !file_id.is_empty() => {
                    let file_id = bounded_response_string(
                        version.get("fileId"),
                        "b2_list_file_versions fileId",
                        MAX_ACCOUNT_CURSOR_BYTES,
                    )?;
                    cursor_fingerprint(&[file_name, "fileId", file_id])
                }
                None | Some(Value::Null) if action == "folder" => {
                    cursor_fingerprint(&[file_name, "action", action])
                }
                _ => {
                    return Err(protocol("b2_list_file_versions returned an invalid fileId"));
                }
            };
            if seen_versions.contains(&identity) {
                return Err(protocol(
                    "b2_list_file_versions returned a duplicate fileName/fileId identity",
                ));
            }
            reserve_bounded(
                &mut version_count,
                1,
                limits.maximum_versions_per_bucket,
                "b2_list_file_versions per-bucket object",
            )?;
            budget.reserve_entry(limits, "b2_list_file_versions")?;
            seen_versions.insert(identity);
            let length = nonnegative(
                version.get("contentLength"),
                "Backblaze version contentLength",
            )?;
            match action {
                "upload" => {
                    totals.upload_count = checked_add(totals.upload_count, 1, "upload count")?;
                    totals.stored_bytes =
                        checked_add(totals.stored_bytes, length, "stored upload bytes")?;
                }
                "hide" | "start" | "folder" if length != 0 => {
                    return Err(protocol(format!(
                        "Backblaze {action} marker has non-zero contentLength"
                    )));
                }
                "hide" => totals.hide_count = checked_add(totals.hide_count, 1, "hide count")?,
                "start" => totals.start_count = checked_add(totals.start_count, 1, "start count")?,
                "folder" => {
                    totals.folder_count = checked_add(totals.folder_count, 1, "folder count")?
                }
                _ => {
                    return Err(protocol(format!(
                        "b2_list_file_versions returned unsupported action {action:?}"
                    )));
                }
            }
        }
        let name = payload.get("nextFileName");
        let id = payload.get("nextFileId");
        match (name, id) {
            (None | Some(Value::Null), None | Some(Value::Null)) => break,
            (Some(name), Some(id)) => {
                let name = bounded_response_string(
                    Some(name),
                    "b2_list_file_versions nextFileName",
                    MAX_ACCOUNT_FILE_NAME_BYTES,
                )?;
                let id = bounded_response_string(
                    Some(id),
                    "b2_list_file_versions nextFileId",
                    MAX_ACCOUNT_CURSOR_BYTES,
                )?;
                insert_bounded_marker(
                    &mut seen_markers,
                    cursor_fingerprint(&[name, id]),
                    limits.maximum_version_pages_per_bucket,
                    "b2_list_file_versions",
                )?;
                next = Some((name.to_string(), id.to_string()));
            }
            _ => return Err(protocol("b2_list_file_versions pagination did not advance")),
        }
    }
    Ok(totals)
}

fn add_unfinished(total: &mut UnfinishedUsage, value: UnfinishedUsage) -> Result<()> {
    total.large_file_count = checked_add(total.large_file_count, value.large_file_count, "count")?;
    total.large_file_pages = checked_add(total.large_file_pages, value.large_file_pages, "count")?;
    total.part_bytes = checked_add(total.part_bytes, value.part_bytes, "bytes")?;
    total.part_count = checked_add(total.part_count, value.part_count, "count")?;
    total.part_pages = checked_add(total.part_pages, value.part_pages, "count")?;
    Ok(())
}

fn add_versions(total: &mut VersionUsage, value: VersionUsage) -> Result<()> {
    total.folder_count = checked_add(total.folder_count, value.folder_count, "count")?;
    total.hide_count = checked_add(total.hide_count, value.hide_count, "count")?;
    total.start_count = checked_add(total.start_count, value.start_count, "count")?;
    total.stored_bytes = checked_add(total.stored_bytes, value.stored_bytes, "bytes")?;
    total.upload_count = checked_add(total.upload_count, value.upload_count, "count")?;
    total.pages = checked_add(total.pages, value.pages, "count")?;
    Ok(())
}

fn checked_add(left: u64, right: u64, label: &str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| protocol(format!("{label} overflow")))
}

fn reserve_bounded(current: &mut u64, amount: u64, maximum: u64, label: &str) -> Result<()> {
    let next = current
        .checked_add(amount)
        .ok_or_else(|| protocol(format!("{label} count overflow")))?;
    if next > maximum {
        return Err(protocol(format!(
            "{label} safety limit exceeded (maximum {maximum})"
        )));
    }
    *current = next;
    Ok(())
}

fn insert_bounded_marker(
    markers: &mut HashSet<[u8; 32]>,
    marker: [u8; 32],
    maximum: u64,
    operation: &str,
) -> Result<()> {
    if markers.contains(&marker) {
        return Err(protocol(format!("{operation} pagination did not advance")));
    }
    let marker_count = u64::try_from(markers.len())
        .map_err(|_| protocol(format!("{operation} pagination marker count overflow")))?;
    if marker_count >= maximum {
        return Err(protocol(format!(
            "{operation} pagination marker safety limit exceeded (maximum {maximum})"
        )));
    }
    markers.insert(marker);
    Ok(())
}

fn cursor_fingerprint(parts: &[&str]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    digest.finalize().into()
}

fn bounded_response_string<'a>(
    value: Option<&'a Value>,
    label: &str,
    maximum: usize,
) -> Result<&'a str> {
    value
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= maximum
                && !value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        })
        .ok_or_else(|| {
            protocol(format!(
                "{label} must be non-empty, control-free, and at most {maximum} bytes"
            ))
        })
}

fn nonnegative(value: Option<&Value>, label: &str) -> Result<u64> {
    value
        .and_then(Value::as_u64)
        .ok_or_else(|| protocol(format!("{label} must be a non-negative integer")))
}

fn response_objects<'a>(
    payload: &'a Value,
    key: &str,
    operation: &str,
) -> Result<Vec<&'a Map<String, Value>>> {
    let values = payload
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| protocol(format!("{operation} returned a malformed {key} list")))?;
    values
        .iter()
        .map(|value| {
            value
                .as_object()
                .ok_or_else(|| protocol(format!("{operation} returned a malformed {key} entry")))
        })
        .collect()
}

fn response_objects_bounded<'a>(
    payload: &'a Value,
    key: &str,
    operation: &str,
    maximum: usize,
) -> Result<Vec<&'a Map<String, Value>>> {
    let values = payload
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| protocol(format!("{operation} returned a malformed {key} list")))?;
    if values.len() > maximum {
        return Err(protocol(format!(
            "{operation} returned more than the requested {maximum} {key} entries"
        )));
    }
    values
        .iter()
        .map(|value| {
            value
                .as_object()
                .ok_or_else(|| protocol(format!("{operation} returned a malformed {key} entry")))
        })
        .collect()
}

fn object<'a>(value: &'a Value, operation: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| protocol(format!("{operation} returned a non-object response")))
}

fn required_string(object: &Map<String, Value>, key: &str, operation: &str) -> Result<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| protocol(format!("{operation} omitted {key}")))
}

fn optional_string(object: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(protocol(format!(
            "Backblaze {key} restriction is malformed"
        ))),
    }
}

fn validate_version_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_VERSION_ID_BYTES
        || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
    {
        return Err(UploaderError::Config(format!(
            "{label} must be non-empty, control-free, and at most {MAX_VERSION_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_object_key(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with('/')
        || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
    {
        return Err(UploaderError::Config(
            "object key must be non-empty and relative".into(),
        ));
    }
    Ok(())
}

fn normalize_remote_prefix(value: &str) -> Result<String> {
    let value = value.trim_end_matches('/');
    validate_object_key(value)?;
    if value
        .split('/')
        .any(|component| matches!(component, "" | "." | ".."))
    {
        return Err(UploaderError::Config(
            "remote prefix contains an unsafe path component".into(),
        ));
    }
    Ok(value.to_string())
}

fn validate_digest(value: &str, length: usize, label: &str) -> Result<()> {
    if value.len() != length || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(UploaderError::Config(format!(
            "{label} must be exactly {length} hexadecimal characters"
        )));
    }
    Ok(())
}

fn normalize_etag(value: &str, label: &str) -> Result<String> {
    let value = value.trim();
    let value = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
        .to_ascii_lowercase();
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(UploaderError::Config(format!(
            "{label} must be a single-part 32-hexadecimal ETag"
        )));
    }
    Ok(value)
}

fn validate_api_url(value: &str, label: &str) -> Result<String> {
    let parsed = Url::parse(value)
        .map_err(|_| UploaderError::Config(format!("{label} must be an absolute HTTP(S) URL")))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(UploaderError::Config(format!(
            "{label} must be an absolute HTTP(S) URL"
        )));
    }
    if parsed.scheme() != "https"
        && !matches!(parsed.host_str(), Some("127.0.0.1" | "::1" | "localhost"))
    {
        return Err(UploaderError::Config(format!("{label} must use HTTPS")));
    }
    Ok(value.trim_end_matches('/').to_string())
}

fn bounded_json(mut response: Response, operation: &str) -> Result<Value> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_CONTROL_RESPONSE_BYTES as u64)
    {
        return Err(protocol(format!("{operation} returned oversized JSON")));
    }
    let mut bytes = Vec::new();
    response
        .by_ref()
        .take(MAX_CONTROL_RESPONSE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_CONTROL_RESPONSE_BYTES {
        return Err(protocol(format!("{operation} returned oversized JSON")));
    }
    let value = strict_json_value(&bytes)
        .map_err(|_| protocol(format!("{operation} returned invalid JSON")))?;
    if !value.is_object() {
        return Err(protocol(format!(
            "{operation} returned a non-object response"
        )));
    }
    Ok(value)
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

fn retry_after(response: &Response) -> Option<u64> {
    response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn retryable_transport(error: &reqwest::Error) -> bool {
    error.is_timeout()
        || error.is_connect()
        || (error.is_request() && !error.is_builder() && !error.is_body())
}

fn retry_delay(attempt: u32, retry_after: Option<u64>) -> Duration {
    let exponential = 1u64
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(60)
        .min(60);
    Duration::from_secs(exponential.max(retry_after.unwrap_or(0)).min(60))
}

fn protocol(message: impl Into<String>) -> UploaderError {
    UploaderError::Protocol(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use md5::Md5;
    use sha1::Sha1;
    use sha2::{Digest, Sha256};
    use std::collections::{BTreeMap, VecDeque};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;

    struct MockResponse {
        status: u16,
        body: Vec<u8>,
    }

    #[derive(Default)]
    struct MockState {
        queues: Mutex<BTreeMap<String, VecDeque<MockResponse>>>,
        targets: Mutex<Vec<String>>,
    }

    struct MockServer {
        state: Arc<MockState>,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
        endpoint: String,
    }

    impl MockServer {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let state = Arc::new(MockState::default());
            let stop = Arc::new(AtomicBool::new(false));
            let thread_state = Arc::clone(&state);
            let thread_stop = Arc::clone(&stop);
            let thread = std::thread::spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => handle(stream, &thread_state),
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(_) => break,
                    }
                }
            });
            Self {
                state,
                stop,
                thread: Some(thread),
                endpoint,
            }
        }

        fn enqueue(&self, operation: &str, status: u16, value: Value) {
            self.enqueue_bytes(operation, status, serde_json::to_vec(&value).unwrap());
        }

        fn enqueue_bytes(&self, operation: &str, status: u16, body: Vec<u8>) {
            self.state
                .queues
                .lock()
                .unwrap()
                .entry(operation.into())
                .or_default()
                .push_back(MockResponse { status, body });
        }

        fn authorize(&self, allowed: Value) {
            self.enqueue(
                "b2_authorize_account",
                200,
                json!({
                    "accountId": "account",
                    "authorizationToken": "token",
                    "apiInfo": {"storageApi": {"apiUrl": self.endpoint, "allowed": allowed}},
                }),
            );
        }

        fn client(&self, retries: u32) -> B2NativeClient {
            B2NativeClient::with_authorize_url(
                "id".into(),
                "secret".into(),
                retries,
                format!("{}/b2api/v4/b2_authorize_account", self.endpoint),
            )
            .unwrap()
        }

        fn targets(&self) -> Vec<String> {
            self.state.targets.lock().unwrap().clone()
        }
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = TcpStream::connect(self.endpoint.trim_start_matches("http://"));
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
    }

    fn handle(mut stream: TcpStream, state: &MockState) {
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut chunk).unwrap_or(0);
            if count == 0 {
                return;
            }
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(index) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
                break index + 4;
            }
            assert!(bytes.len() < 128 * 1024);
        };
        let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
        let target = headers
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .to_string();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
            })
            .unwrap_or(0);
        while bytes.len() - header_end < content_length {
            let count = stream.read(&mut chunk).unwrap();
            bytes.extend_from_slice(&chunk[..count]);
        }
        state.targets.lock().unwrap().push(target.clone());
        let path = target.split('?').next().unwrap();
        let operation = path.rsplit('/').next().unwrap();
        let response = state
            .queues
            .lock()
            .unwrap()
            .get_mut(operation)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| MockResponse {
                status: 500,
                body: br#"{"code":"unexpected_test_request"}"#.to_vec(),
            });
        let reason = if response.status < 300 { "OK" } else { "Error" };
        write!(
            stream,
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response.status,
            reason,
            response.body.len()
        )
        .unwrap();
        stream.write_all(&response.body).unwrap();
    }

    fn verifier() -> B2NativeObjectVerifier {
        let mut client = B2NativeClient::with_authorize_url(
            "id".into(),
            "secret".into(),
            0,
            "http://127.0.0.1:1/authorize".into(),
        )
        .unwrap();
        client.account_id = Some("account".into());
        client.authorization_token = Some("token".into());
        client.api_url = Some("http://127.0.0.1:1".into());
        client.allowed = Some(
            json!({
                "bucketId": "bucket-id",
                "bucketName": "bucket-name",
                "capabilities": ["listFiles"],
                "namePrefix": null,
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        B2NativeObjectVerifier::new(client, "bucket-id".into(), "bucket-name".into()).unwrap()
    }

    fn object(data: &[u8]) -> (Value, String, String, String) {
        let sha256 = hex::encode(Sha256::digest(data));
        let sha1 = hex::encode(Sha1::digest(data));
        let etag = hex::encode(Md5::digest(data));
        (
            json!({
                "accountId": "account",
                "action": "upload",
                "bucketId": "bucket-id",
                "contentLength": data.len(),
                "contentMd5": etag,
                "contentSha1": sha1,
                "fileId": "version-1",
                "fileInfo": {"sha256": sha256},
                "fileName": "prefix/object",
            }),
            sha256,
            sha1,
            etag,
        )
    }

    fn test_account_limits() -> AccountUsageLimits {
        AccountUsageLimits {
            maximum_buckets: 8,
            maximum_total_requests: 64,
            maximum_total_entries: 64,
            maximum_unfinished_pages_per_bucket: 8,
            maximum_unfinished_files_per_bucket: 8,
            maximum_part_pages_per_file: 8,
            maximum_parts_per_file: 8,
            maximum_version_pages_per_bucket: 8,
            maximum_versions_per_bucket: 8,
        }
    }

    fn authorize_account(server: &MockServer) {
        server.authorize(json!({
            "capabilities": ["listBuckets", "listFiles", "writeFiles"],
            "bucketId": null,
            "bucketIds": null,
            "namePrefix": null,
        }));
        server.enqueue(
            "b2_list_buckets",
            200,
            json!({"buckets": [{"bucketId": "bucket"}]}),
        );
    }

    fn version_entry(name: &str, id: &str, bytes: u64) -> Value {
        json!({
            "action": "upload",
            "contentLength": bytes,
            "fileId": id,
            "fileName": name,
        })
    }

    fn request_count(server: &MockServer, operation: &str) -> usize {
        let suffix = format!("/{operation}");
        server
            .targets()
            .iter()
            .filter(|target| {
                target
                    .split('?')
                    .next()
                    .is_some_and(|path| path.ends_with(&suffix))
            })
            .count()
    }

    #[test]
    fn snapshot_pins_exact_native_identity_and_digests() {
        let verifier = verifier();
        let data = b"native metadata payload";
        let (value, sha256, sha1, etag) = object(data);
        let snapshot = BTreeMap::from([("prefix/object".into(), vec![value])]);
        assert_eq!(
            verifier
                .snapshot_exact_version(
                    &snapshot,
                    "prefix/object",
                    data.len() as u64,
                    &sha256,
                    &sha1,
                    &etag,
                    Some("version-1"),
                )
                .unwrap(),
            Some("version-1".into())
        );
        let error = verifier
            .snapshot_exact_version(
                &snapshot,
                "prefix/object",
                data.len() as u64,
                &sha256,
                &sha1,
                &etag,
                Some("different-version"),
            )
            .unwrap_err();
        assert!(error.to_string().contains("different pinned version"));
    }

    #[test]
    fn snapshot_rejects_identity_action_and_hash_tampering() {
        let verifier = verifier();
        let data = b"native metadata payload";
        let (original, sha256, sha1, etag) = object(data);
        for (pointer, replacement, expected) in [
            ("/accountId", json!("other"), "different accountId"),
            ("/bucketId", json!("other"), "different bucketId"),
            ("/action", json!("hide"), "unsupported action"),
            (
                "/contentSha1",
                json!(format!("unverified:{}", "0".repeat(40))),
                "conflicting SHA-1",
            ),
            (
                "/fileInfo/sha256",
                json!("0".repeat(64)),
                "conflicting object metadata",
            ),
        ] {
            let mut value = original.clone();
            *value.pointer_mut(pointer).unwrap() = replacement;
            let snapshot = BTreeMap::from([("prefix/object".into(), vec![value])]);
            let error = verifier
                .snapshot_exact_version(
                    &snapshot,
                    "prefix/object",
                    data.len() as u64,
                    &sha256,
                    &sha1,
                    &etag,
                    None,
                )
                .unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
        }
    }

    #[test]
    fn unfinished_page_limit_allows_exact_terminal_page_and_blocks_n_plus_one() {
        let mut limits = test_account_limits();
        limits.maximum_unfinished_pages_per_bucket = 2;

        let exact = MockServer::start();
        authorize_account(&exact);
        exact.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": "cursor-1"}),
        );
        exact.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": null}),
        );
        exact.enqueue(
            "b2_list_file_versions",
            200,
            json!({"files": [], "nextFileName": null, "nextFileId": null}),
        );
        let result = account_usage_with_limits(&mut exact.client(0), limits).unwrap();
        assert_eq!(result["unfinished_large_file_page_count"], 2);
        assert_eq!(request_count(&exact, "b2_list_unfinished_large_files"), 2);

        let excessive = MockServer::start();
        authorize_account(&excessive);
        excessive.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": "cursor-1"}),
        );
        excessive.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": "cursor-2"}),
        );
        let error = account_usage_with_limits(&mut excessive.client(0), limits).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("b2_list_unfinished_large_files per-bucket page safety limit")
        );
        assert_eq!(
            request_count(&excessive, "b2_list_unfinished_large_files"),
            2,
            "the request beyond the cap must not be sent"
        );
    }

    #[test]
    fn part_page_limit_allows_exact_terminal_page_and_blocks_n_plus_one() {
        let mut limits = test_account_limits();
        limits.maximum_part_pages_per_file = 2;

        let exact = MockServer::start();
        authorize_account(&exact);
        exact.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [{"fileId": "large"}], "nextFileId": null}),
        );
        exact.enqueue(
            "b2_list_parts",
            200,
            json!({
                "parts": [{"partNumber": 1, "contentLength": 10}],
                "nextPartNumber": 2,
            }),
        );
        exact.enqueue(
            "b2_list_parts",
            200,
            json!({
                "parts": [{"partNumber": 2, "contentLength": 20}],
                "nextPartNumber": null,
            }),
        );
        exact.enqueue(
            "b2_list_file_versions",
            200,
            json!({"files": [], "nextFileName": null, "nextFileId": null}),
        );
        let result = account_usage_with_limits(&mut exact.client(0), limits).unwrap();
        assert_eq!(result["unfinished_part_page_count"], 2);
        assert_eq!(result["unfinished_part_count"], 2);
        assert_eq!(request_count(&exact, "b2_list_parts"), 2);

        let excessive = MockServer::start();
        authorize_account(&excessive);
        excessive.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [{"fileId": "large"}], "nextFileId": null}),
        );
        excessive.enqueue(
            "b2_list_parts",
            200,
            json!({"parts": [], "nextPartNumber": 1}),
        );
        excessive.enqueue(
            "b2_list_parts",
            200,
            json!({"parts": [], "nextPartNumber": 2}),
        );
        let error = account_usage_with_limits(&mut excessive.client(0), limits).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("b2_list_parts per-file page safety limit")
        );
        assert_eq!(
            request_count(&excessive, "b2_list_parts"),
            2,
            "the request beyond the cap must not be sent"
        );
    }

    #[test]
    fn version_page_limit_allows_exact_terminal_page_and_blocks_n_plus_one() {
        let mut limits = test_account_limits();
        limits.maximum_version_pages_per_bucket = 2;

        let exact = MockServer::start();
        authorize_account(&exact);
        exact.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": null}),
        );
        exact.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [version_entry("a", "version-a", 10)],
                "nextFileName": "cursor-a",
                "nextFileId": "cursor-id-a",
            }),
        );
        exact.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [version_entry("b", "version-b", 20)],
                "nextFileName": null,
                "nextFileId": null,
            }),
        );
        let result = account_usage_with_limits(&mut exact.client(0), limits).unwrap();
        assert_eq!(result["version_page_count"], 2);
        assert_eq!(result["upload_version_count"], 2);
        assert_eq!(request_count(&exact, "b2_list_file_versions"), 2);

        let excessive = MockServer::start();
        authorize_account(&excessive);
        excessive.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": null}),
        );
        excessive.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [version_entry("a", "version-a", 10)],
                "nextFileName": "cursor-a",
                "nextFileId": "cursor-id-a",
            }),
        );
        excessive.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [version_entry("b", "version-b", 20)],
                "nextFileName": "cursor-b",
                "nextFileId": "cursor-id-b",
            }),
        );
        let error = account_usage_with_limits(&mut excessive.client(0), limits).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("b2_list_file_versions per-bucket page safety limit")
        );
        assert_eq!(
            request_count(&excessive, "b2_list_file_versions"),
            2,
            "the request beyond the cap must not be sent"
        );
    }

    #[test]
    fn bucket_entry_and_global_request_limits_fail_before_excess_work() {
        let bucket_limited = MockServer::start();
        bucket_limited.authorize(json!({
            "capabilities": ["listBuckets", "listFiles", "writeFiles"],
            "bucketId": null,
            "bucketIds": null,
            "namePrefix": null,
        }));
        bucket_limited.enqueue(
            "b2_list_buckets",
            200,
            json!({"buckets": [{"bucketId": "a"}, {"bucketId": "b"}]}),
        );
        let mut limits = test_account_limits();
        limits.maximum_buckets = 1;
        let error = account_usage_with_limits(&mut bucket_limited.client(0), limits).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("more than the requested 1 buckets entries")
        );
        assert_eq!(request_count(&bucket_limited, "b2_list_buckets"), 1);

        let entry_limited = MockServer::start();
        authorize_account(&entry_limited);
        entry_limited.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [{"fileId": "large"}], "nextFileId": null}),
        );
        let mut limits = test_account_limits();
        limits.maximum_total_entries = 1;
        let error = account_usage_with_limits(&mut entry_limited.client(0), limits).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("account-wide returned entry safety limit")
        );
        assert_eq!(
            request_count(&entry_limited, "b2_list_parts"),
            0,
            "an over-budget returned entry must fail before child requests"
        );

        let request_limited = MockServer::start();
        authorize_account(&request_limited);
        request_limited.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": null}),
        );
        let mut limits = test_account_limits();
        limits.maximum_total_requests = 2;
        let error = account_usage_with_limits(&mut request_limited.client(0), limits).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("account-wide request safety limit")
        );
        assert_eq!(request_count(&request_limited, "b2_list_file_versions"), 0);
    }

    #[test]
    fn per_source_returned_entry_limits_bound_dedupe_memory() {
        let unfinished = MockServer::start();
        authorize_account(&unfinished);
        unfinished.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({
                "files": [{"fileId": "large-a"}, {"fileId": "large-b"}],
                "nextFileId": null,
            }),
        );
        unfinished.enqueue(
            "b2_list_parts",
            200,
            json!({"parts": [], "nextPartNumber": null}),
        );
        let mut limits = test_account_limits();
        limits.maximum_unfinished_files_per_bucket = 1;
        let error = account_usage_with_limits(&mut unfinished.client(0), limits).unwrap_err();
        assert!(error.to_string().contains("per-bucket object safety limit"));

        let parts = MockServer::start();
        authorize_account(&parts);
        parts.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [{"fileId": "large"}], "nextFileId": null}),
        );
        parts.enqueue(
            "b2_list_parts",
            200,
            json!({
                "parts": [
                    {"partNumber": 1, "contentLength": 10},
                    {"partNumber": 2, "contentLength": 20},
                ],
                "nextPartNumber": null,
            }),
        );
        let mut limits = test_account_limits();
        limits.maximum_parts_per_file = 1;
        let error = account_usage_with_limits(&mut parts.client(0), limits).unwrap_err();
        assert!(error.to_string().contains("per-file part safety limit"));

        let versions = MockServer::start();
        authorize_account(&versions);
        versions.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": null}),
        );
        versions.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [
                    version_entry("a", "version-a", 10),
                    version_entry("b", "version-b", 20),
                ],
                "nextFileName": null,
                "nextFileId": null,
            }),
        );
        let mut limits = test_account_limits();
        limits.maximum_versions_per_bucket = 1;
        let error = account_usage_with_limits(&mut versions.client(0), limits).unwrap_err();
        assert!(error.to_string().contains("per-bucket object safety limit"));
    }

    #[test]
    fn version_page_overlap_is_deduplicated_by_fixed_size_identity() {
        let server = MockServer::start();
        authorize_account(&server);
        server.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": null}),
        );
        server.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [version_entry("a", "version-a", 10)],
                "nextFileName": "cursor",
                "nextFileId": "cursor-id",
            }),
        );
        server.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [version_entry("a", "version-a", 10)],
                "nextFileName": null,
                "nextFileId": null,
            }),
        );
        let error =
            account_usage_with_limits(&mut server.client(0), test_account_limits()).unwrap_err();
        assert!(error.to_string().contains("duplicate fileName/fileId"));
        assert_eq!(request_count(&server, "b2_list_file_versions"), 2);
    }

    #[test]
    fn provider_arrays_larger_than_requested_pages_are_rejected() {
        for (operation, key, maximum) in [
            (
                "b2_list_unfinished_large_files",
                "files",
                ACCOUNT_UNFINISHED_PAGE_SIZE,
            ),
            ("b2_list_parts", "parts", ACCOUNT_PART_PAGE_SIZE),
            ("b2_list_file_versions", "files", ACCOUNT_VERSION_PAGE_SIZE),
        ] {
            let payload = Value::Object(Map::from_iter([(
                key.to_string(),
                Value::Array(vec![json!({}); maximum + 1]),
            )]));
            let error = response_objects_bounded(&payload, key, operation, maximum).unwrap_err();
            assert!(error.to_string().contains("more than the requested"));
        }
    }

    #[test]
    fn account_cursor_strings_are_bounded_and_control_free() {
        for value in [
            Value::String("x".repeat(MAX_ACCOUNT_CURSOR_BYTES + 1)),
            Value::String("cursor\nvalue".into()),
            Value::String(String::new()),
        ] {
            let error =
                bounded_response_string(Some(&value), "provider cursor", MAX_ACCOUNT_CURSOR_BYTES)
                    .unwrap_err();
            assert!(error.to_string().contains("control-free"));
        }
        let valid = Value::String("cursor".into());
        assert_eq!(
            bounded_response_string(Some(&valid), "provider cursor", MAX_ACCOUNT_CURSOR_BYTES)
                .unwrap(),
            "cursor"
        );
    }

    #[test]
    fn account_usage_sums_versions_and_all_pagination() {
        let server = MockServer::start();
        server.authorize(json!({
            "capabilities": ["listBuckets", "listFiles", "writeFiles"],
            "bucketId": null,
            "bucketIds": null,
            "namePrefix": null,
        }));
        server.enqueue(
            "b2_list_buckets",
            200,
            json!({"buckets": [{"bucketId": "a"}, {"bucketId": "b"}]}),
        );
        for _ in 0..2 {
            server.enqueue(
                "b2_list_unfinished_large_files",
                200,
                json!({"files": [], "nextFileId": null}),
            );
        }
        server.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [
                    {"action": "upload", "contentLength": 10, "fileId": "version-a", "fileName": "a"},
                    {"action": "hide", "contentLength": 0, "fileId": "version-b", "fileName": "b"},
                ],
                "nextFileName": "same-key",
                "nextFileId": "next-id",
            }),
        );
        server.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [
                    {"action": "upload", "contentLength": 20, "fileId": "version-c", "fileName": "c"},
                    {"action": "folder", "contentLength": 0, "fileName": "folder/"},
                ],
                "nextFileName": null,
                "nextFileId": null,
            }),
        );
        server.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [{"action": "upload", "contentLength": 30, "fileId": "version-d", "fileName": "d"}],
                "nextFileName": null,
                "nextFileId": null,
            }),
        );
        let result = account_usage(&mut server.client(0)).unwrap();
        assert_eq!(result["schema_version"], 1);
        assert_eq!(result["scope"], "account");
        assert_eq!(result["scope_complete"], true);
        assert_eq!(result["bucket_count"], 2);
        assert_eq!(result["upload_version_count"], 3);
        assert_eq!(result["hide_marker_count"], 1);
        assert_eq!(result["folder_entry_count"], 1);
        assert_eq!(result["stored_upload_bytes"], 60);
        assert_eq!(result["total_stored_bytes"], 60);
        assert_eq!(result["version_page_count"], 3);
        assert!(server.targets().iter().any(|target| {
            target.contains("startFileName=same-key") && target.contains("startFileId=next-id")
        }));
    }

    #[test]
    fn account_usage_includes_every_unfinished_part_page() {
        let server = MockServer::start();
        server.authorize(json!({
            "capabilities": ["listBuckets", "listFiles", "writeFiles"],
            "bucketId": null,
            "bucketIds": null,
            "namePrefix": null,
        }));
        server.enqueue(
            "b2_list_buckets",
            200,
            json!({"buckets": [{"bucketId": "a"}]}),
        );
        server.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [{"fileId": "large-1"}], "nextFileId": "large-2"}),
        );
        server.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [{"fileId": "large-2"}], "nextFileId": null}),
        );
        server.enqueue(
            "b2_list_parts",
            200,
            json!({"parts": [{"partNumber": 1, "contentLength": 100}], "nextPartNumber": 2}),
        );
        server.enqueue(
            "b2_list_parts",
            200,
            json!({"parts": [{"partNumber": 2, "contentLength": 200}], "nextPartNumber": null}),
        );
        server.enqueue(
            "b2_list_parts",
            200,
            json!({"parts": [{"partNumber": 1, "contentLength": 300}], "nextPartNumber": null}),
        );
        server.enqueue(
            "b2_list_file_versions",
            200,
            json!({"files": [], "nextFileName": null, "nextFileId": null}),
        );
        let result = account_usage(&mut server.client(0)).unwrap();
        assert_eq!(result["unfinished_large_file_count"], 2);
        assert_eq!(result["unfinished_large_file_page_count"], 2);
        assert_eq!(result["unfinished_part_count"], 3);
        assert_eq!(result["unfinished_part_page_count"], 3);
        assert_eq!(result["unfinished_part_bytes"], 600);
        assert_eq!(result["total_stored_bytes"], 600);
    }

    #[test]
    fn account_scope_restriction_and_exact_cap_code_fail_closed() {
        let missing_write = MockServer::start();
        missing_write.authorize(json!({
            "capabilities": ["listBuckets", "listFiles"],
            "bucketId": null,
            "bucketIds": null,
            "namePrefix": null,
        }));
        let error = account_usage(&mut missing_write.client(0)).unwrap_err();
        assert!(error.to_string().contains("writeFiles"));
        assert_eq!(
            missing_write.targets().len(),
            1,
            "capability failure must precede account listing"
        );

        let restricted = MockServer::start();
        restricted.authorize(json!({
            "capabilities": ["listBuckets", "listFiles", "writeFiles"],
            "bucketId": "restricted",
            "namePrefix": null,
        }));
        let error = account_usage(&mut restricted.client(0)).unwrap_err();
        assert!(error.to_string().contains("account usage is incomplete"));

        let capped = MockServer::start();
        capped.enqueue_bytes(
            "b2_authorize_account",
            403,
            br#"{"code":"transaction_cap_exceeded","message":"must-not-leak"}"#.to_vec(),
        );
        let error = account_usage(&mut capped.client(3)).unwrap_err();
        assert_eq!(error.exit_status(), 21);
        assert!(!error.to_string().contains("must-not-leak"));
        assert_eq!(capped.targets().len(), 1, "capacity failure must not retry");
    }

    #[test]
    fn account_pagination_and_numeric_malformation_fail_closed() {
        let repeated = MockServer::start();
        repeated.authorize(json!({
            "capabilities": ["listBuckets", "listFiles", "writeFiles"],
            "bucketId": null,
            "bucketIds": null,
            "namePrefix": null,
        }));
        repeated.enqueue(
            "b2_list_buckets",
            200,
            json!({"buckets": [{"bucketId": "a"}]}),
        );
        repeated.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": "same"}),
        );
        repeated.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": "same"}),
        );
        assert!(
            account_usage(&mut repeated.client(0))
                .unwrap_err()
                .to_string()
                .contains("pagination did not advance")
        );

        let invalid = MockServer::start();
        invalid.authorize(json!({
            "capabilities": ["listBuckets", "listFiles", "writeFiles"],
            "bucketId": null,
            "bucketIds": null,
            "namePrefix": null,
        }));
        invalid.enqueue(
            "b2_list_buckets",
            200,
            json!({"buckets": [{"bucketId": "a"}]}),
        );
        invalid.enqueue(
            "b2_list_unfinished_large_files",
            200,
            json!({"files": [], "nextFileId": null}),
        );
        invalid.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [{"action": "upload", "contentLength": -1, "fileId": "version-a", "fileName": "a"}],
                "nextFileName": null,
                "nextFileId": null,
            }),
        );
        assert!(
            account_usage(&mut invalid.client(0))
                .unwrap_err()
                .to_string()
                .contains("non-negative integer")
        );
    }

    #[test]
    fn generation_snapshot_is_prefix_wide_and_rejects_unknown_keys() {
        let server = MockServer::start();
        server.authorize(json!({
            "capabilities": ["listFiles"],
            "bucketId": "bucket-id",
            "bucketName": "bucket-name",
            "namePrefix": "prefix/",
        }));
        let entry = |name: &str, id: &str| {
            json!({
                "accountId": "account",
                "action": "upload",
                "bucketId": "bucket-id",
                "fileId": id,
                "fileName": name,
            })
        };
        server.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [entry("prefix/a", "id-a")],
                "nextFileName": "prefix/aa-cursor-only",
                "nextFileId": "id-cursor-only",
            }),
        );
        server.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [entry("prefix/b", "id-b")],
                "nextFileName": null,
                "nextFileId": null,
            }),
        );
        let client = server.client(0);
        let mut verifier =
            B2NativeObjectVerifier::new(client, "bucket-id".into(), "bucket-name".into()).unwrap();
        let allowed = BTreeSet::from(["prefix/a".into(), "prefix/b".into()]);
        let snapshot = verifier
            .list_generation_versions("prefix", &allowed)
            .unwrap();
        assert_eq!(snapshot["prefix/a"].len(), 1);
        assert_eq!(snapshot["prefix/b"].len(), 1);
        assert!(
            server
                .targets()
                .iter()
                .any(|target| target.contains("startFileId=id-cursor-only"))
        );

        let unexpected = MockServer::start();
        unexpected.authorize(json!({
            "capabilities": ["listFiles"],
            "bucketId": "bucket-id",
            "bucketName": "bucket-name",
            "namePrefix": "prefix/",
        }));
        unexpected.enqueue(
            "b2_list_file_versions",
            200,
            json!({
                "files": [entry("prefix/not-local", "id-x")],
                "nextFileName": null,
                "nextFileId": null,
            }),
        );
        let mut verifier = B2NativeObjectVerifier::new(
            unexpected.client(0),
            "bucket-id".into(),
            "bucket-name".into(),
        )
        .unwrap();
        let error = verifier
            .list_generation_versions("prefix", &BTreeSet::from(["prefix/a".into()]))
            .unwrap_err();
        assert!(error.to_string().contains("unexpected key"));
    }

    #[test]
    fn latest_version_is_reseeked_and_pinned_before_acceptance() {
        let server = MockServer::start();
        server.authorize(json!({
            "capabilities": ["listFiles"],
            "bucketId": "bucket-id",
            "bucketName": "bucket-name",
            "namePrefix": "prefix/",
        }));
        let data = b"pinned object";
        let (entry, sha256, sha1, etag) = object(data);
        server.enqueue(
            "b2_list_file_versions",
            200,
            json!({"files": [entry.clone()], "nextFileName": null, "nextFileId": null}),
        );
        server.enqueue(
            "b2_list_file_versions",
            200,
            json!({"files": [entry], "nextFileName": null, "nextFileId": null}),
        );
        let mut verifier =
            B2NativeObjectVerifier::new(server.client(0), "bucket-id".into(), "bucket-name".into())
                .unwrap();
        assert_eq!(
            verifier
                .latest_exact_version("prefix/object", data.len() as u64, &sha256, &sha1, &etag,)
                .unwrap(),
            Some("version-1".into())
        );
        assert_eq!(
            server
                .targets()
                .iter()
                .filter(|target| target.contains("b2_list_file_versions"))
                .count(),
            2
        );
    }

    #[test]
    fn absurd_native_retry_count_is_rejected() {
        let error = B2NativeClient::with_authorize_url(
            "id".into(),
            "secret".into(),
            u32::MAX,
            "http://127.0.0.1:1/authorize".into(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("retry count"));
    }

    #[test]
    fn deterministic_native_request_builder_errors_are_not_retryable() {
        let error = Client::new().get("://invalid-url").send().unwrap_err();
        assert!(error.is_builder());
        assert!(!retryable_transport(&error));
    }

    #[test]
    fn native_client_debug_output_redacts_credentials_and_tokens() {
        let mut client = B2NativeClient::with_authorize_url(
            "b2-access-credential".into(),
            "b2-secret-credential".into(),
            0,
            "http://127.0.0.1:1/authorize".into(),
        )
        .unwrap();
        client.authorization_token = Some("b2-authorization-token".into());
        client.account_id = Some("b2-account-identifier".into());
        client.api_url = Some("https://private-api.example.invalid".into());

        let debug = format!("{client:?}");
        for credential in [
            "b2-access-credential",
            "b2-secret-credential",
            "b2-authorization-token",
            "b2-account-identifier",
            "private-api.example.invalid",
            "127.0.0.1:1/authorize",
        ] {
            assert!(!debug.contains(credential));
        }
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn provider_control_json_rejects_duplicate_authority_fields_recursively() {
        let authorization = MockServer::start();
        authorization.enqueue_bytes(
            "b2_authorize_account",
            200,
            br#"{"accountId":"one","accountId":"two"}"#.to_vec(),
        );
        let error = authorization.client(0).authorize().unwrap_err();
        assert!(error.to_string().contains("invalid JSON"));

        let versions = MockServer::start();
        versions.authorize(json!({
            "capabilities": ["listFiles"],
            "bucketId": "bucket-id",
            "bucketName": "bucket-name",
            "namePrefix": "prefix/",
        }));
        versions.enqueue_bytes(
            "b2_list_file_versions",
            200,
            br#"{"files":[{"fileId":"one","fileId":"two"}],"nextFileName":null,"nextFileId":null}"#
                .to_vec(),
        );
        let mut verifier = B2NativeObjectVerifier::new(
            versions.client(0),
            "bucket-id".into(),
            "bucket-name".into(),
        )
        .unwrap();
        let error = verifier
            .latest_exact_version(
                "prefix/object",
                1,
                &"0".repeat(64),
                &"0".repeat(40),
                &"0".repeat(32),
            )
            .unwrap_err();
        assert!(error.to_string().contains("invalid JSON"));
    }
}
