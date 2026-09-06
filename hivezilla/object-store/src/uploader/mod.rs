//! Native custody uploader used by the raw gRPC recorder.
//!
//! The public CLI intentionally mirrors the recorder-facing subset of the
//! historical Python uploader.  Every response-controlled error is bounded and
//! sanitized before it reaches logs.

mod b2;
mod config;
mod dirfd;
mod generation;
mod retention;
mod s3;

use clap::{Args, Parser, Subcommand};
use quick_xml::Reader;
use quick_xml::events::Event;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;

pub use b2::{B2NativeClient, B2NativeObjectVerifier, NativeSnapshot, account_usage};
pub use config::{
    B2ObjectSettings, Provider, StorageSettings, optional_b2_object_settings,
    parse_credentials_file,
};
pub use generation::upload_generation;
pub use retention::{R2RetentionOptions, r2_retention};
pub use s3::{FilePayload, Payload, S3Client, S3Response};

pub const MAX_API_ERROR_BODY_BYTES: usize = 16 * 1024;
pub const MAX_CONTROL_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_RETRIES: u32 = 32;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApiError {
    pub operation: String,
    pub status: u16,
    pub code: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let detail = capacity_exit_status(&self.code)
            .map(|_| format!(" ({})", self.code))
            .unwrap_or_default();
        write!(
            formatter,
            "{} failed HTTP {}{}",
            self.operation, self.status, detail
        )
    }
}

impl std::error::Error for ApiError {}

#[derive(Debug, Error)]
pub enum UploaderError {
    #[error("{0}")]
    Config(String),
    #[error("{0}")]
    Protocol(String),
    #[error("{0}")]
    Api(#[from] ApiError),
    #[error("{0}")]
    Http(#[from] reqwest::Error),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
}

impl UploaderError {
    pub fn exit_status(&self) -> u8 {
        match self {
            Self::Api(error) => capacity_exit_status(&error.code).unwrap_or(1),
            _ => 1,
        }
    }
}

pub type Result<T> = std::result::Result<T, UploaderError>;

fn capacity_exit_status(code: &str) -> Option<u8> {
    match code {
        "download_cap_exceeded" => Some(20),
        "transaction_cap_exceeded" => Some(21),
        "storage_cap_exceeded" | "cap_exceeded" => Some(22),
        _ => None,
    }
}

pub(crate) fn valid_api_code(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z'))
        && value.len() <= 128
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

pub(crate) fn api_error_code(body: &[u8]) -> String {
    if body.is_empty() || body.len() > MAX_API_ERROR_BODY_BYTES {
        return String::new();
    }
    if let Ok(Value::Object(object)) = strict_json_value(body) {
        let lower = object.get("code");
        let upper = object.get("Code");
        if lower.is_some() && upper.is_some() {
            return String::new();
        }
        if let Some(Value::String(code)) = lower.or(upper)
            && valid_api_code(code)
        {
            return code.clone();
        }
    }

    xml_api_error_code(body).unwrap_or_default()
}

/// Accept exactly one direct `<Code>` child of an `<Error>` document. Nested
/// response-controlled text must never change recorder capacity-stop exits.
fn xml_api_error_code(body: &[u8]) -> Option<String> {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(false);
    let mut buffer = Vec::new();
    let mut depth = 0usize;
    let mut root_seen = false;
    let mut root_closed = false;
    let mut direct_code_seen = false;
    let mut code_depth = None;
    let mut code = String::new();
    let mut elements = Vec::<String>::new();
    loop {
        match reader.read_event_into(&mut buffer).ok()? {
            Event::Start(event) => {
                if root_closed {
                    return None;
                }
                depth = depth.checked_add(1)?;
                let name = event.local_name();
                if depth > 64 {
                    return None;
                }
                if depth == 1 {
                    if root_seen || name.as_ref() != "Error" {
                        return None;
                    }
                    root_seen = true;
                } else if depth == 2 && matches!(name.as_ref(), "Code" | "code") {
                    if direct_code_seen {
                        return None;
                    }
                    direct_code_seen = true;
                    code_depth = Some(depth);
                } else if code_depth.is_some() {
                    return None;
                }
                elements.push(name.as_ref().to_owned());
            }
            Event::Empty(event) => {
                if depth == 0 || code_depth.is_some() {
                    return None;
                }
                if depth == 1 && matches!(event.local_name().as_ref(), "Code" | "code") {
                    return None;
                }
            }
            Event::End(event) => {
                if depth == 0 {
                    return None;
                }
                let expected = elements.pop()?;
                if event.local_name().as_ref() != expected {
                    return None;
                }
                if code_depth == Some(depth) {
                    code_depth = None;
                }
                depth -= 1;
                if depth == 0 {
                    root_closed = true;
                }
            }
            Event::Text(text) => {
                let decoded = text.as_ref();
                if code_depth.is_some() {
                    code.push_str(decoded);
                } else if (depth == 0 || root_closed) && !decoded.trim().is_empty() {
                    return None;
                }
            }
            Event::GeneralRef(reference) => {
                // quick-xml emits references separately from text. Keep the
                // same XML character/entity decoding as the older reader.
                let decoded = match reference.resolve_char_ref().ok()? {
                    Some(character) => character,
                    None => match reference.as_ref() {
                        "lt" => '<',
                        "gt" => '>',
                        "amp" => '&',
                        "apos" => '\'',
                        "quot" => '"',
                        _ => return None,
                    },
                };
                if code_depth.is_some() {
                    code.push(decoded);
                } else if (depth == 0 || root_closed) && !decoded.is_whitespace() {
                    return None;
                }
            }
            Event::CData(_) | Event::DocType(_) => return None,
            Event::Decl(_) | Event::PI(_) | Event::Comment(_) if code_depth.is_some() => {
                return None;
            }
            Event::Decl(_) | Event::PI(_) | Event::Comment(_) => {}
            Event::Eof => break,
        }
        buffer.clear();
    }
    let code = code.trim();
    (root_seen
        && root_closed
        && depth == 0
        && elements.is_empty()
        && direct_code_seen
        && valid_api_code(code))
    .then(|| code.to_string())
}

/// Decode one complete JSON value while rejecting duplicate object keys and
/// floating-point numbers at every nesting level. Provider control responses
/// must have one unambiguous interpretation before any authority-bearing field
/// is inspected.
pub(crate) fn strict_json_value(bytes: &[u8]) -> serde_json::Result<Value> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let StrictJsonValue(value) = StrictJsonValue::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value)
}

struct StrictJsonValue(Value);

impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictJsonVisitor)
    }
}

struct StrictJsonVisitor;

impl<'de> Visitor<'de> for StrictJsonVisitor {
    type Value = StrictJsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("strict JSON without duplicate keys or floating-point numbers")
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, _value: f64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Err(E::custom("floating-point JSON numbers are not permitted"))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string(value.to_string())
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(Value::Null))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(StrictJsonValue(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(StrictJsonValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom("duplicate JSON object key"));
            }
            let StrictJsonValue(value) = map.next_value()?;
            values.insert(key, value);
        }
        Ok(StrictJsonValue(Value::Object(values)))
    }
}

#[derive(Debug, Clone, Args)]
pub struct StorageArguments {
    /// Literal dotenv file containing storage settings; it is never sourced.
    #[arg(long)]
    pub credentials_file: Option<PathBuf>,
    /// Storage provider: auto, b2, r2, or s3.
    #[arg(long)]
    pub provider: Option<String>,
    #[arg(long, default_value_t = 8)]
    pub retries: u32,
}

#[derive(Debug, Parser)]
#[command(
    name = "blockzilla-s3-upload",
    about = "Upload and independently verify immutable S3-compatible objects"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Manifest and commit a stopped self-contained WAL generation.
    UploadGeneration {
        generation_dir: PathBuf,
        remote_prefix: String,
        receipt: PathBuf,
        #[arg(long)]
        generation_id: String,
        #[arg(long)]
        predecessor_manifest_sha256: Option<String>,
        #[command(flatten)]
        storage: StorageArguments,
    },
    /// Plan or apply R2 retention from a validated local receipt chain.
    R2Retention {
        receipt_directory: PathBuf,
        remote_prefix: String,
        #[arg(long)]
        target_bytes: u64,
        #[arg(long)]
        minimum_age_secs: u64,
        #[arg(long)]
        maximum_generation_slot: u64,
        #[arg(long, default_value_t = 2)]
        minimum_retained_generations: usize,
        #[arg(long)]
        apply: bool,
        #[command(flatten)]
        storage: StorageArguments,
    },
    /// Report account-wide Backblaze object-version and unfinished-part usage.
    B2AccountUsage {
        #[command(flatten)]
        storage: StorageArguments,
    },
}

pub fn run_from_env() -> Result<()> {
    run(Cli::parse())
}

fn run(cli: Cli) -> Result<()> {
    let result = match cli.command {
        Command::B2AccountUsage { storage } => {
            config::validate_b2_provider_override(storage.provider.as_deref())?;
            let settings = config::B2Settings::load(storage.credentials_file.as_deref())?;
            let mut client = B2NativeClient::new(
                settings.application_key_id,
                settings.application_key,
                storage.retries,
            )?;
            account_usage(&mut client)?
        }
        Command::UploadGeneration {
            generation_dir,
            remote_prefix,
            receipt,
            generation_id,
            predecessor_manifest_sha256,
            storage,
        } => {
            let settings = StorageSettings::load(
                storage.credentials_file.as_deref(),
                storage.provider.as_deref(),
            )?;
            let client = S3Client::new(settings.clone(), storage.retries)?;
            let native = native_b2_settings_for_provider(
                settings.provider,
                storage.credentials_file.as_deref(),
            )?;
            if native.is_none() && settings.provider == Provider::B2 {
                let host = url::Url::parse(&settings.endpoint)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
                    .unwrap_or_default();
                if host == "backblazeb2.com" || host.ends_with(".backblazeb2.com") {
                    return Err(UploaderError::Config(
                        "B2_BUCKET_ID is required for cap-safe Backblaze generation verification"
                            .into(),
                    ));
                }
            }
            let mut verifier = native
                .map(|native| {
                    B2NativeObjectVerifier::new(
                        B2NativeClient::new(
                            native.application_key_id,
                            native.application_key,
                            storage.retries,
                        )?,
                        native.bucket_id,
                        settings.bucket.clone(),
                    )
                })
                .transpose()?;
            upload_generation(
                &client,
                &generation_dir,
                &generation_id,
                &remote_prefix,
                &receipt,
                predecessor_manifest_sha256.as_deref(),
                verifier.as_mut(),
            )?
        }
        Command::R2Retention {
            receipt_directory,
            remote_prefix,
            target_bytes,
            minimum_age_secs,
            maximum_generation_slot,
            minimum_retained_generations,
            apply,
            storage,
        } => {
            let settings = StorageSettings::load(
                storage.credentials_file.as_deref(),
                storage.provider.as_deref(),
            )?;
            let client = S3Client::new(settings, storage.retries)?;
            let mut options = R2RetentionOptions::new(
                receipt_directory,
                remote_prefix,
                target_bytes,
                minimum_age_secs,
                maximum_generation_slot,
            );
            options.minimum_retained_generations = minimum_retained_generations;
            options.apply = apply;
            r2_retention(&client, &options)?
        }
    };
    let encoded = canonical_json_bytes(&result)?;
    use std::io::Write;
    std::io::stdout().lock().write_all(&encoded)?;
    Ok(())
}

fn native_b2_settings_for_provider(
    provider: Provider,
    credentials_file: Option<&std::path::Path>,
) -> Result<Option<B2ObjectSettings>> {
    if provider == Provider::B2 {
        optional_b2_object_settings(credentials_file)
    } else {
        Ok(None)
    }
}

pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    write_canonical_json(value, &mut output)?;
    output.push(b'\n');
    Ok(output)
}

fn write_canonical_json(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            output.push(b'{');
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)?;
                output.push(b':');
                write_canonical_json(value, output)?;
            }
            output.push(b'}');
        }
        _ => serde_json::to_writer(output, value)?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn exact_capacity_codes_have_stable_exit_statuses() {
        for (code, expected) in [
            ("download_cap_exceeded", 20),
            ("transaction_cap_exceeded", 21),
            ("storage_cap_exceeded", 22),
            ("cap_exceeded", 22),
        ] {
            let error = UploaderError::Api(ApiError {
                operation: "test".into(),
                status: 403,
                code: code.into(),
            });
            assert_eq!(error.exit_status(), expected);
            assert!(error.to_string().contains(code));
        }
        for code in [
            "",
            "transaction_cap_exceeded_extra",
            "TRANSACTION_CAP_EXCEEDED",
        ] {
            let error = UploaderError::Api(ApiError {
                operation: "test".into(),
                status: 403,
                code: code.into(),
            });
            assert_eq!(error.exit_status(), 1);
        }
    }

    #[test]
    fn error_code_parser_is_bounded_and_unambiguous() {
        assert_eq!(
            api_error_code(br#"{"code":"transaction_cap_exceeded","message":"secret"}"#),
            "transaction_cap_exceeded"
        );
        assert_eq!(
            api_error_code(b"<Error><Code>cap_exceeded</Code><Message>secret</Message></Error>"),
            "cap_exceeded"
        );
        assert_eq!(
            api_error_code(
                b"<Error><Code>&#99;ap&#x5f;exceeded</Code><Message>a &amp; b</Message></Error>"
            ),
            "cap_exceeded"
        );
        assert!(api_error_code(b"<Error><Code>cap&unknown;exceeded</Code></Error>").is_empty());
        assert!(api_error_code(b"<Error><Code>cap&amp;exceeded</Code></Error>").is_empty());
        assert!(api_error_code(b"&#65;<Error><Code>cap_exceeded</Code></Error>").is_empty());
        assert!(api_error_code(b"<Error><Code>a</Code><Code>b</Code></Error>").is_empty());
        assert!(
            api_error_code(
                b"<Error><Message><Code>transaction_cap_exceeded</Code></Message></Error>"
            )
            .is_empty()
        );
        assert!(
            api_error_code(
                b"<Envelope><Error><Code>transaction_cap_exceeded</Code></Error></Envelope>"
            )
            .is_empty()
        );
        assert_eq!(
            api_error_code(
                b"<?xml version=\"1.0\"?><Error><Message>x</Message><Code>cap_exceeded</Code></Error>"
            ),
            "cap_exceeded"
        );
        assert!(api_error_code(br#"{"code":"cap_exceeded","code":"retry_me"}"#).is_empty());
        assert!(api_error_code(br#"{"code":"cap_exceeded","Code":"cap_exceeded"}"#).is_empty());
        assert!(api_error_code(br#"{"code":"cap_exceeded","detail":{"x":1,"x":2}}"#).is_empty());
        assert!(api_error_code(&vec![b'x'; MAX_API_ERROR_BODY_BYTES + 1]).is_empty());
    }

    #[test]
    fn non_b2_providers_never_load_native_b2_credentials() {
        let temporary = tempfile::tempdir().unwrap();
        let credentials = temporary.path().join("credentials.env");
        std::fs::write(
            &credentials,
            "B2_BUCKET_ID=bucket-id\nAWS_ACCESS_KEY_ID=aws-access\nAWS_SECRET_ACCESS_KEY=aws-secret\n",
        )
        .unwrap();
        for provider in [Provider::S3, Provider::R2] {
            assert_eq!(
                native_b2_settings_for_provider(provider, Some(&credentials)).unwrap(),
                None
            );
        }
        assert!(
            native_b2_settings_for_provider(Provider::B2, Some(&credentials))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn b2_account_usage_rejects_non_b2_provider_before_loading_credentials() {
        let cli =
            Cli::try_parse_from(["uploader", "b2-account-usage", "--provider", "r2"]).unwrap();
        let error = run(cli).unwrap_err();
        assert!(error.to_string().contains("requires provider b2 or auto"));
    }

    #[test]
    fn strict_json_rejects_nested_duplicates_floats_and_trailing_values() {
        for input in [
            br#"{"a":1,"a":2}"#.as_slice(),
            br#"{"a":{"b":1,"b":2}}"#.as_slice(),
            br#"{"a":1.0}"#.as_slice(),
            br#"{"a":1} {"b":2}"#.as_slice(),
        ] {
            assert!(strict_json_value(input).is_err());
        }
        assert_eq!(
            strict_json_value(br#"{"a":[1,true,null,"x"]}"#).unwrap(),
            serde_json::json!({"a": [1, true, null, "x"]})
        );
    }

    #[test]
    fn canonical_json_sorts_every_object_and_has_one_newline() {
        let value = serde_json::json!({"z": {"b": 2, "a": 1}, "a": [{"d": 4, "c": 3}]});
        assert_eq!(
            canonical_json_bytes(&value).unwrap(),
            b"{\"a\":[{\"c\":3,\"d\":4}],\"z\":{\"a\":1,\"b\":2}}\n"
        );
    }

    #[test]
    fn upload_generation_cli_accepts_credentials_provider_and_retry_selection() {
        let cli = Cli::try_parse_from([
            "blockzilla-s3-upload",
            "upload-generation",
            "/generation",
            "prefix/generation-1",
            "/receipt.json",
            "--generation-id",
            "generation-1",
            "--credentials-file",
            "/credentials.env",
            "--provider",
            "cloudflare-r2",
            "--retries",
            "3",
        ])
        .unwrap();
        let Command::UploadGeneration {
            generation_dir,
            remote_prefix,
            receipt,
            generation_id,
            predecessor_manifest_sha256,
            storage,
        } = cli.command
        else {
            panic!("wrong command parsed");
        };
        assert_eq!(generation_dir, PathBuf::from("/generation"));
        assert_eq!(remote_prefix, "prefix/generation-1");
        assert_eq!(receipt, PathBuf::from("/receipt.json"));
        assert_eq!(generation_id, "generation-1");
        assert!(predecessor_manifest_sha256.is_none());
        assert_eq!(
            storage.credentials_file,
            Some(PathBuf::from("/credentials.env"))
        );
        assert_eq!(storage.provider.as_deref(), Some("cloudflare-r2"));
        assert_eq!(storage.retries, 3);
    }

    #[test]
    fn r2_retention_cli_preserves_dry_run_cutoff_and_storage_arguments() {
        let cli = Cli::try_parse_from([
            "blockzilla-s3-upload",
            "r2-retention",
            "/receipts",
            "live-grpc-backup/v1",
            "--target-bytes",
            "1000",
            "--minimum-age-secs",
            "3600",
            "--maximum-generation-slot",
            "761",
            "--minimum-retained-generations",
            "3",
            "--provider",
            "r2",
            "--credentials-file",
            "/credentials.env",
        ])
        .unwrap();
        let Command::R2Retention {
            receipt_directory,
            remote_prefix,
            target_bytes,
            minimum_age_secs,
            maximum_generation_slot,
            minimum_retained_generations,
            apply,
            storage,
        } = cli.command
        else {
            panic!("wrong command parsed");
        };
        assert_eq!(receipt_directory, PathBuf::from("/receipts"));
        assert_eq!(remote_prefix, "live-grpc-backup/v1");
        assert_eq!(target_bytes, 1000);
        assert_eq!(minimum_age_secs, 3600);
        assert_eq!(maximum_generation_slot, 761);
        assert_eq!(minimum_retained_generations, 3);
        assert!(!apply, "retention must default to a dry run");
        assert_eq!(storage.provider.as_deref(), Some("r2"));
        assert_eq!(
            storage.credentials_file,
            Some(PathBuf::from("/credentials.env"))
        );
    }
}
