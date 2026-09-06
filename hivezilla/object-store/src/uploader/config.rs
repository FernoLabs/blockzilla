use super::{Result, UploaderError};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use url::Url;

const MAX_CREDENTIALS_FILE_BYTES: usize = 64 * 1024;
const ALLOWED_CREDENTIAL_KEYS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT_URL_S3",
    "AWS_ENDPOINT_URL",
    "AWS_REGION",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "B2_APPLICATION_KEY",
    "B2_APPLICATION_KEY_ID",
    "B2_BUCKET",
    "B2_BUCKET_ID",
    "B2_S3_ENDPOINT",
    "B2_S3_REGION",
    "R2_ACCESS_KEY_ID",
    "R2_BUCKET",
    "R2_ENDPOINT",
    "R2_REGION",
    "R2_S3_ENDPOINT",
    "R2_S3_REGION",
    "R2_SECRET_ACCESS_KEY",
    "R2_SESSION_TOKEN",
    "S3_ACCESS_KEY_ID",
    "S3_BUCKET",
    "S3_ENDPOINT",
    "S3_PROVIDER",
    "S3_REGION",
    "S3_SECRET_ACCESS_KEY",
    "S3_SESSION_TOKEN",
    "STORAGE_PROVIDER",
];

const R2_FAMILY_KEYS: &[&str] = &[
    "R2_ACCESS_KEY_ID",
    "R2_BUCKET",
    "R2_ENDPOINT",
    "R2_REGION",
    "R2_S3_ENDPOINT",
    "R2_S3_REGION",
    "R2_SECRET_ACCESS_KEY",
    "R2_SESSION_TOKEN",
];
const B2_FAMILY_KEYS: &[&str] = &[
    "B2_APPLICATION_KEY",
    "B2_APPLICATION_KEY_ID",
    "B2_BUCKET",
    "B2_BUCKET_ID",
    "B2_S3_ENDPOINT",
    "B2_S3_REGION",
];
const S3_FAMILY_KEYS: &[&str] = &[
    "S3_ACCESS_KEY_ID",
    "S3_BUCKET",
    "S3_ENDPOINT",
    "S3_REGION",
    "S3_SECRET_ACCESS_KEY",
    "S3_SESSION_TOKEN",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Provider {
    B2,
    R2,
    S3,
}

#[derive(Clone, Eq, PartialEq)]
pub struct StorageSettings {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub provider: Provider,
    pub session_token: Option<String>,
}

impl fmt::Debug for StorageSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageSettings")
            // This type can be formatted before URL validation. Keep a malformed
            // endpoint containing user information out of diagnostic output too.
            .field("endpoint", &"<redacted>")
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field("provider", &self.provider)
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct B2Settings {
    pub application_key_id: String,
    pub application_key: String,
}

impl fmt::Debug for B2Settings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("B2Settings")
            .field("application_key_id", &"<redacted>")
            .field("application_key", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct B2ObjectSettings {
    pub application_key_id: String,
    pub application_key: String,
    pub bucket_id: String,
}

impl fmt::Debug for B2ObjectSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("B2ObjectSettings")
            .field("application_key_id", &"<redacted>")
            .field("application_key", &"<redacted>")
            .field("bucket_id", &self.bucket_id)
            .finish()
    }
}

fn invalid(message: impl Into<String>) -> UploaderError {
    UploaderError::Config(message.into())
}

pub fn parse_credentials_file(path: &Path) -> Result<BTreeMap<String, String>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| invalid(format!("cannot open credentials file safely: {error}")))?;
    let metadata = file
        .metadata()
        .map_err(|error| invalid(format!("cannot inspect credentials file: {error}")))?;
    if !metadata.is_file() {
        return Err(invalid(
            "credentials path must be a regular file, not a symlink",
        ));
    }
    if metadata.len() > MAX_CREDENTIALS_FILE_BYTES as u64 {
        return Err(invalid("credentials file is unexpectedly large"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(MAX_CREDENTIALS_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| invalid(format!("cannot read credentials file: {error}")))?;
    if bytes.len() > MAX_CREDENTIALS_FILE_BYTES {
        return Err(invalid("credentials file is unexpectedly large"));
    }
    let contents = std::str::from_utf8(&bytes)
        .map_err(|error| invalid(format!("cannot read credentials file: {error}")))?;
    if contents.contains(['\0', '\r']) {
        return Err(invalid(
            "credentials file contains an invalid NUL or carriage return",
        ));
    }

    let mut values = BTreeMap::new();
    for (index, raw_line) in contents.lines().enumerate() {
        let line_number = index + 1;
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("export") {
            if rest.is_empty() || !rest.starts_with(char::is_whitespace) {
                return Err(invalid(format!(
                    "credentials file line {line_number} is malformed"
                )));
            }
            line = rest.trim_start();
        }
        let Some((key, raw_value)) = line.split_once('=') else {
            return Err(invalid(format!(
                "credentials file line {line_number} is missing '='"
            )));
        };
        let key = key.trim();
        if !ALLOWED_CREDENTIAL_KEYS.contains(&key) {
            return Err(invalid(format!(
                "credentials file line {line_number} uses unsupported key {key:?}"
            )));
        }
        if values.contains_key(key) {
            return Err(invalid(format!(
                "credentials file contains duplicate key {key}"
            )));
        }
        values.insert(key.to_string(), literal_value(raw_value, line_number)?);
    }
    Ok(values)
}

fn literal_value(raw: &str, line_number: usize) -> Result<String> {
    let mut value = raw.trim();
    if value.is_empty() {
        return Err(invalid(format!(
            "credentials file line {line_number} has an empty value"
        )));
    }
    if matches!(value.as_bytes().first(), Some(b'\'' | b'"')) {
        let quote = value.as_bytes()[0] as char;
        if value.len() < 2 || !value.ends_with(quote) {
            return Err(invalid(format!(
                "credentials file line {line_number} has an unterminated quote"
            )));
        }
        value = &value[1..value.len() - 1];
        if value.contains(quote) {
            return Err(invalid(format!(
                "credentials file line {line_number} contains an unsupported embedded quote"
            )));
        }
    } else if value.chars().any(char::is_whitespace) {
        return Err(invalid(format!(
            "credentials file line {line_number} must quote values containing whitespace"
        )));
    }
    if value.is_empty() || value.contains(['\0', '\r', '\n']) {
        return Err(invalid(format!(
            "credentials file line {line_number} has an invalid value"
        )));
    }
    Ok(value.to_string())
}

fn environment() -> BTreeMap<String, String> {
    ALLOWED_CREDENTIAL_KEYS
        .iter()
        .filter_map(|key| std::env::var(key).ok().map(|value| ((*key).into(), value)))
        .collect()
}

fn values(path: Option<&Path>) -> Result<BTreeMap<String, String>> {
    path.map(parse_credentials_file)
        .unwrap_or_else(|| Ok(environment()))
}

fn first(values: &BTreeMap<String, String>, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| values.get(*name).filter(|value| !value.is_empty()).cloned())
}

fn normalize_provider(value: Option<&str>) -> Result<Option<Provider>> {
    let normalized = value.unwrap_or("auto").trim().to_ascii_lowercase();
    match if normalized.is_empty() {
        "auto"
    } else {
        normalized.as_str()
    } {
        "auto" => Ok(None),
        "b2" | "backblaze" | "backblaze-b2" => Ok(Some(Provider::B2)),
        "r2" | "cloudflare" | "cloudflare-r2" => Ok(Some(Provider::R2)),
        "s3" => Ok(Some(Provider::S3)),
        _ => Err(invalid("storage provider must be auto, b2, r2, or s3")),
    }
}

fn configured_provider(values: &BTreeMap<String, String>) -> Result<Option<Provider>> {
    let storage = normalize_provider(values.get("STORAGE_PROVIDER").map(String::as_str))?;
    let s3 = normalize_provider(values.get("S3_PROVIDER").map(String::as_str))?;
    if storage.is_some() && s3.is_some() && storage != s3 {
        return Err(invalid(
            "STORAGE_PROVIDER and S3_PROVIDER select different storage providers",
        ));
    }
    Ok(storage.or(s3))
}

fn family_is_present(values: &BTreeMap<String, String>, keys: &[&str]) -> bool {
    keys.iter()
        .any(|key| values.get(*key).is_some_and(|value| !value.is_empty()))
}

fn provider_for_canonical_host(endpoint: Option<&str>) -> Option<Provider> {
    let host = endpoint
        .and_then(|value| Url::parse(value).ok())
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))?;
    if host == "r2.cloudflarestorage.com" || host.ends_with(".r2.cloudflarestorage.com") {
        Some(Provider::R2)
    } else if host == "backblazeb2.com" || host.ends_with(".backblazeb2.com") {
        Some(Provider::B2)
    } else {
        None
    }
}

fn infer_provider(
    values: &BTreeMap<String, String>,
    override_value: Option<&str>,
) -> Result<Provider> {
    if let Some(provider) = normalize_provider(override_value)? {
        return Ok(provider);
    }
    if let Some(provider) = configured_provider(values)? {
        return Ok(provider);
    }

    let families = [
        (Provider::R2, family_is_present(values, R2_FAMILY_KEYS)),
        (Provider::B2, family_is_present(values, B2_FAMILY_KEYS)),
        (Provider::S3, family_is_present(values, S3_FAMILY_KEYS)),
    ]
    .into_iter()
    .filter_map(|(provider, present)| present.then_some(provider))
    .collect::<Vec<_>>();
    if families.len() > 1 {
        return Err(invalid(
            "storage provider auto-detection is ambiguous across provider-specific settings",
        ));
    }
    if let Some(provider) = families.into_iter().next() {
        return Ok(provider);
    }
    Ok(provider_for_canonical_host(
        first(values, &["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"]).as_deref(),
    )
    .unwrap_or(Provider::S3))
}

struct ProviderKeys {
    endpoint: &'static [&'static str],
    region: &'static [&'static str],
    bucket: &'static [&'static str],
    access: &'static [&'static str],
    secret: &'static [&'static str],
    token: &'static [&'static str],
}

fn provider_keys(provider: Provider) -> ProviderKeys {
    match provider {
        Provider::R2 => ProviderKeys {
            endpoint: &[
                "R2_S3_ENDPOINT",
                "R2_ENDPOINT",
                "AWS_ENDPOINT_URL_S3",
                "AWS_ENDPOINT_URL",
            ],
            region: &[
                "R2_S3_REGION",
                "R2_REGION",
                "AWS_REGION",
                "AWS_DEFAULT_REGION",
            ],
            bucket: &["R2_BUCKET"],
            access: &["R2_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"],
            secret: &["R2_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"],
            token: &["R2_SESSION_TOKEN", "AWS_SESSION_TOKEN"],
        },
        Provider::B2 => ProviderKeys {
            endpoint: &["B2_S3_ENDPOINT", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"],
            region: &["B2_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"],
            bucket: &["B2_BUCKET"],
            access: &["B2_APPLICATION_KEY_ID", "AWS_ACCESS_KEY_ID"],
            secret: &["B2_APPLICATION_KEY", "AWS_SECRET_ACCESS_KEY"],
            token: &[],
        },
        Provider::S3 => ProviderKeys {
            endpoint: &["S3_ENDPOINT", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"],
            region: &["S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"],
            bucket: &["S3_BUCKET"],
            access: &["S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"],
            secret: &["S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"],
            token: &["S3_SESSION_TOKEN", "AWS_SESSION_TOKEN"],
        },
    }
}

impl StorageSettings {
    pub fn load(path: Option<&Path>, provider_override: Option<&str>) -> Result<Self> {
        let values = values(path)?;
        let provider = infer_provider(&values, provider_override)?;
        // AWS_* names are the only intentionally provider-neutral compatibility
        // aliases. They are consulted only after a provider has been selected;
        // another provider's R2_*, B2_*, or S3_* namespace is never a fallback.
        let keys = provider_keys(provider);
        let endpoint = first(&values, keys.endpoint);
        if provider_for_canonical_host(endpoint.as_deref()).is_some_and(|host| host != provider) {
            return Err(invalid(
                "storage provider conflicts with the canonical endpoint host",
            ));
        }
        let mut region = first(&values, keys.region);
        if provider == Provider::R2 {
            if region.as_deref().is_some_and(|value| {
                !matches!(value.to_ascii_lowercase().as_str(), "auto" | "us-east-1")
            }) {
                return Err(invalid("Cloudflare R2 region must be auto"));
            }
            region = Some("auto".into());
        }
        let bucket = first(&values, keys.bucket);
        let access_key = first(&values, keys.access);
        let secret_key = first(&values, keys.secret);
        let missing = [
            ("S3 endpoint", endpoint.as_ref()),
            ("S3 region", region.as_ref()),
            ("S3 bucket", bucket.as_ref()),
            ("AWS_ACCESS_KEY_ID", access_key.as_ref()),
            ("AWS_SECRET_ACCESS_KEY", secret_key.as_ref()),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.is_none().then_some(name))
        .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(invalid(format!(
                "missing required storage settings: {}",
                missing.join(", ")
            )));
        }
        Ok(Self {
            endpoint: endpoint.unwrap(),
            region: region.unwrap(),
            bucket: bucket.unwrap(),
            access_key: access_key.unwrap(),
            secret_key: secret_key.unwrap(),
            provider,
            session_token: first(&values, keys.token),
        })
    }
}

impl B2Settings {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let values = values(path)?;
        if family_is_present(&values, R2_FAMILY_KEYS) || family_is_present(&values, S3_FAMILY_KEYS)
        {
            return Err(invalid(
                "b2-account-usage credentials contain non-B2 provider settings",
            ));
        }
        if configured_provider(&values)?.is_some_and(|provider| provider != Provider::B2) {
            return Err(invalid(
                "b2-account-usage credentials select a non-B2 storage provider",
            ));
        }
        let application_key_id = first(&values, &["B2_APPLICATION_KEY_ID", "AWS_ACCESS_KEY_ID"]);
        let application_key = first(&values, &["B2_APPLICATION_KEY", "AWS_SECRET_ACCESS_KEY"]);
        let missing = [
            ("B2_APPLICATION_KEY_ID", application_key_id.as_ref()),
            ("B2_APPLICATION_KEY", application_key.as_ref()),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.is_none().then_some(name))
        .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(invalid(format!(
                "missing required Backblaze settings: {}",
                missing.join(", ")
            )));
        }
        Ok(Self {
            application_key_id: application_key_id.unwrap(),
            application_key: application_key.unwrap(),
        })
    }
}

pub(crate) fn validate_b2_provider_override(value: Option<&str>) -> Result<()> {
    if normalize_provider(value)?.is_some_and(|provider| provider != Provider::B2) {
        return Err(invalid("b2-account-usage requires provider b2 or auto"));
    }
    Ok(())
}

pub fn optional_b2_object_settings(path: Option<&Path>) -> Result<Option<B2ObjectSettings>> {
    let values = values(path)?;
    let Some(bucket_id) = values.get("B2_BUCKET_ID").cloned() else {
        return Ok(None);
    };
    if family_is_present(&values, R2_FAMILY_KEYS) || family_is_present(&values, S3_FAMILY_KEYS) {
        return Err(invalid(
            "native B2 verification credentials contain non-B2 provider settings",
        ));
    }
    if configured_provider(&values)?.is_some_and(|provider| provider != Provider::B2) {
        return Err(invalid(
            "native B2 verification credentials select a non-B2 storage provider",
        ));
    }
    if bucket_id.is_empty()
        || bucket_id.len() > 1024
        || bucket_id.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
    {
        return Err(invalid("B2_BUCKET_ID is invalid"));
    }
    let application_key_id = first(&values, &["B2_APPLICATION_KEY_ID", "AWS_ACCESS_KEY_ID"]);
    let application_key = first(&values, &["B2_APPLICATION_KEY", "AWS_SECRET_ACCESS_KEY"]);
    let missing = [
        ("B2_APPLICATION_KEY_ID", application_key_id.as_ref()),
        ("B2_APPLICATION_KEY", application_key.as_ref()),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.is_none().then_some(name))
    .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(invalid(format!(
            "missing required Backblaze settings: {}",
            missing.join(", ")
        )));
    }
    Ok(Some(B2ObjectSettings {
        application_key_id: application_key_id.unwrap(),
        application_key: application_key.unwrap(),
        bucket_id,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn credentials_are_literal_and_duplicates_fail() {
        let temporary = tempfile::tempdir().unwrap();
        let marker = temporary.path().join("must-not-exist");
        let credentials = temporary.path().join("credentials.env");
        fs::write(
            &credentials,
            format!(
                "B2_S3_ENDPOINT=https://example.invalid\nB2_S3_REGION=test\nB2_BUCKET=b\nAWS_ACCESS_KEY_ID=id\nAWS_SECRET_ACCESS_KEY=$(touch${{IFS}}{})\n",
                marker.display()
            ),
        )
        .unwrap();
        let parsed = parse_credentials_file(&credentials).unwrap();
        assert!(parsed["AWS_SECRET_ACCESS_KEY"].starts_with("$(touch"));
        assert!(!marker.exists());
        fs::write(
            &credentials,
            "AWS_ACCESS_KEY_ID=one\nAWS_ACCESS_KEY_ID=two\n",
        )
        .unwrap();
        assert!(
            parse_credentials_file(&credentials)
                .unwrap_err()
                .to_string()
                .contains("duplicate key")
        );
    }

    #[test]
    fn credentials_symlink_is_rejected() {
        use std::os::unix::fs::symlink;
        let temporary = tempfile::tempdir().unwrap();
        let target = temporary.path().join("target");
        let link = temporary.path().join("link");
        fs::write(&target, "AWS_ACCESS_KEY_ID=id\n").unwrap();
        symlink(target, &link).unwrap();
        assert!(parse_credentials_file(&link).is_err());
    }

    #[test]
    fn r2_provider_aliases_infer_region_auto_and_preserve_session_token() {
        let temporary = tempfile::tempdir().unwrap();
        let credentials = temporary.path().join("credentials.env");
        fs::write(
            &credentials,
            "STORAGE_PROVIDER=cloudflare-r2\nR2_ENDPOINT=https://account.r2.cloudflarestorage.com\nR2_BUCKET=bucket\nR2_ACCESS_KEY_ID=access\nR2_SECRET_ACCESS_KEY=secret\nAWS_REGION=us-east-1\nAWS_SESSION_TOKEN=session\n",
        )
        .unwrap();

        let inferred = StorageSettings::load(Some(&credentials), None).unwrap();
        assert_eq!(inferred.provider, Provider::R2);
        assert_eq!(inferred.region, "auto");
        assert_eq!(inferred.session_token.as_deref(), Some("session"));

        let overridden = StorageSettings::load(Some(&credentials), Some("cloudflare")).unwrap();
        assert_eq!(overridden.provider, Provider::R2);
        assert_eq!(overridden.region, "auto");
    }

    #[test]
    fn explicit_provider_reads_only_its_namespace() {
        let temporary = tempfile::tempdir().unwrap();
        let credentials = temporary.path().join("credentials.env");
        fs::write(
            &credentials,
            concat!(
                "R2_ENDPOINT=https://account.r2.cloudflarestorage.com\n",
                "R2_BUCKET=r2-bucket\n",
                "R2_ACCESS_KEY_ID=r2-access\n",
                "R2_SECRET_ACCESS_KEY=r2-secret\n",
                "B2_S3_ENDPOINT=https://s3.us-west-000.backblazeb2.com\n",
                "B2_S3_REGION=us-west-000\n",
                "B2_BUCKET=b2-bucket\n",
                "B2_APPLICATION_KEY_ID=b2-access\n",
                "B2_APPLICATION_KEY=b2-secret\n",
                "B2_BUCKET_ID=b2-id\n",
                "S3_ENDPOINT=https://s3.example.invalid\n",
                "S3_REGION=s3-region\n",
                "S3_BUCKET=s3-bucket\n",
                "S3_ACCESS_KEY_ID=s3-access\n",
                "S3_SECRET_ACCESS_KEY=s3-secret\n",
                "AWS_ACCESS_KEY_ID=generic-access\n",
                "AWS_SECRET_ACCESS_KEY=generic-secret\n",
            ),
        )
        .unwrap();

        let s3 = StorageSettings::load(Some(&credentials), Some("s3")).unwrap();
        assert_eq!(s3.provider, Provider::S3);
        assert_eq!(s3.endpoint, "https://s3.example.invalid");
        assert_eq!(s3.bucket, "s3-bucket");
        assert_eq!(s3.access_key, "s3-access");
        assert_eq!(s3.secret_key, "s3-secret");

        let r2 = StorageSettings::load(Some(&credentials), Some("r2")).unwrap();
        assert_eq!(r2.provider, Provider::R2);
        assert_eq!(r2.endpoint, "https://account.r2.cloudflarestorage.com");
        assert_eq!(r2.bucket, "r2-bucket");
        assert_eq!(r2.access_key, "r2-access");
        assert_eq!(r2.secret_key, "r2-secret");

        let b2 = StorageSettings::load(Some(&credentials), Some("b2")).unwrap();
        assert_eq!(b2.provider, Provider::B2);
        assert_eq!(b2.endpoint, "https://s3.us-west-000.backblazeb2.com");
        assert_eq!(b2.bucket, "b2-bucket");
        assert_eq!(b2.access_key, "b2-access");
        assert_eq!(b2.secret_key, "b2-secret");
    }

    #[test]
    fn auto_provider_rejects_mixed_families_and_infers_generic_canonical_hosts() {
        let temporary = tempfile::tempdir().unwrap();
        let credentials = temporary.path().join("credentials.env");
        fs::write(
            &credentials,
            concat!(
                "R2_ENDPOINT=https://account.r2.cloudflarestorage.com\n",
                "R2_BUCKET=r2-bucket\n",
                "B2_BUCKET=b2-bucket\n",
            ),
        )
        .unwrap();
        let error = StorageSettings::load(Some(&credentials), None).unwrap_err();
        assert!(error.to_string().contains("ambiguous"));

        fs::write(
            &credentials,
            concat!(
                "AWS_ENDPOINT_URL=https://account.r2.cloudflarestorage.com\n",
                "AWS_ACCESS_KEY_ID=generic-access\n",
                "AWS_SECRET_ACCESS_KEY=generic-secret\n",
                "R2_BUCKET=r2-bucket\n",
            ),
        )
        .unwrap();
        let inferred = StorageSettings::load(Some(&credentials), None).unwrap();
        assert_eq!(inferred.provider, Provider::R2);
        assert_eq!(inferred.access_key, "generic-access");
        assert_eq!(inferred.secret_key, "generic-secret");
    }

    #[test]
    fn selected_provider_never_falls_back_to_foreign_credentials() {
        let temporary = tempfile::tempdir().unwrap();
        let credentials = temporary.path().join("credentials.env");
        fs::write(
            &credentials,
            concat!(
                "S3_ENDPOINT=https://s3.example.invalid\n",
                "S3_REGION=region\n",
                "S3_BUCKET=bucket\n",
                "R2_ACCESS_KEY_ID=foreign-access\n",
                "R2_SECRET_ACCESS_KEY=foreign-secret\n",
            ),
        )
        .unwrap();
        let error = StorageSettings::load(Some(&credentials), Some("s3")).unwrap_err();
        assert!(error.to_string().contains("AWS_ACCESS_KEY_ID"));
        assert!(!error.to_string().contains("foreign-access"));
    }

    #[test]
    fn native_b2_loaders_reject_foreign_provider_families() {
        let temporary = tempfile::tempdir().unwrap();
        let credentials = temporary.path().join("credentials.env");
        fs::write(
            &credentials,
            concat!(
                "B2_BUCKET_ID=b2-id\n",
                "AWS_ACCESS_KEY_ID=generic-access\n",
                "AWS_SECRET_ACCESS_KEY=generic-secret\n",
                "R2_BUCKET=foreign-bucket\n",
            ),
        )
        .unwrap();
        assert!(B2Settings::load(Some(&credentials)).is_err());
        assert!(optional_b2_object_settings(Some(&credentials)).is_err());
    }

    #[test]
    fn credential_settings_debug_output_is_redacted() {
        let storage = StorageSettings {
            endpoint: "https://endpoint-user:credential@example.invalid".into(),
            region: "test-region".into(),
            bucket: "test-bucket".into(),
            access_key: "storage-access-credential".into(),
            secret_key: "storage-secret-credential".into(),
            provider: Provider::S3,
            session_token: Some("storage-session-credential".into()),
        };
        let b2 = B2Settings {
            application_key_id: "b2-access-credential".into(),
            application_key: "b2-secret-credential".into(),
        };
        let b2_object = B2ObjectSettings {
            application_key_id: "b2-object-access-credential".into(),
            application_key: "b2-object-secret-credential".into(),
            bucket_id: "bucket-id".into(),
        };

        let debug = format!("{storage:?} {b2:?} {b2_object:?}");
        for credential in [
            "storage-access-credential",
            "storage-secret-credential",
            "storage-session-credential",
            "endpoint-user",
            "b2-access-credential",
            "b2-secret-credential",
            "b2-object-access-credential",
            "b2-object-secret-credential",
        ] {
            assert!(!debug.contains(credential));
        }
        assert!(debug.contains("<redacted>"));
    }
}
