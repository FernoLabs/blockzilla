//! Crash-safe rolling retention for immutable R2 generation prefixes.
//!
//! R2 does not expose a conditional `DeleteObject`.  Consequently this
//! protocol is safe only for a dedicated, create-only prefix with no writer or
//! second retention principal racing the process.  Before every delete we
//! verify the exact immutable object identity recorded by the committed
//! generation, then verify absence afterwards.  A durable local pending
//! journal makes an interrupted oldest-first prune idempotently recoverable.
//!
//! The receipt directory is private-owner controlled. Other processes running
//! as that same effective user must honor the retention lock; exact-byte
//! compare-before-replace checks detect unsynchronized state changes at each
//! publication boundary, but Unix cannot revoke a same-UID process's ability
//! to mutate an owner-writable inode.

use super::config::Provider;
use super::dirfd::DirectoryHandle;
use super::{Payload, Result, S3Client, UploaderError, canonical_json_bytes, strict_json_value};
use fs2::FileExt;
use md5::Md5;
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const R2_RETENTION_SCHEMA_VERSION: u64 = 1;
pub const R2_RETENTION_MIN_GENERATIONS: usize = 2;
pub const R2_RETENTION_ANCHOR_NAME: &str = ".r2-retention-anchor.json";
pub const R2_RETENTION_PENDING_NAME: &str = ".r2-retention-pending.json";
pub const R2_RETENTION_LOCK_NAME: &str = ".r2-retention.lock";
pub const R2_DELETE_CONCURRENCY_PRECONDITION: &str =
    "exclusive-immutable-prefix-no-concurrent-overwrite-or-delete";

const GENERATION_MANIFEST_SCHEMA_VERSION: u64 = 1;
const GENERATION_COMMIT_SCHEMA_VERSION: u64 = 1;
const GENERATION_RECEIPT_SCHEMA_VERSION: u64 = 1;
const MAX_RETENTION_CONTROL_OBJECT_BYTES: usize = 16 * 1024 * 1024;
const MAX_RETENTION_LOCAL_STATE_BYTES: usize = 32 * 1024 * 1024;
const MAX_RETENTION_CHAIN_FILE_BYTES: usize = 4096;
const MAX_GENERATION_RECEIPT_BYTES: usize = 1024 * 1024;
const MAX_RETENTION_GENERATIONS: usize = 1_000_000;
const MAX_RETENTION_DIRECTORY_ENTRIES: usize = 2_000_000;
const MAX_SUPPORTED_INTEGER: u64 = i64::MAX as u64;
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct R2RetentionOptions {
    pub receipt_directory: PathBuf,
    pub remote_prefix: String,
    pub target_bytes: u64,
    pub minimum_age_secs: u64,
    pub minimum_retained_generations: usize,
    /// Highest generation slot independently confirmed durable by Blockzilla.
    /// Zero intentionally authorizes no deletion.
    pub maximum_generation_slot: u64,
    /// Retention is a dry run unless this is explicitly true.
    pub apply: bool,
    /// Test/recovery hook. Production callers should leave this unset.
    pub now_unix_secs: Option<u64>,
}

impl R2RetentionOptions {
    pub fn new(
        receipt_directory: PathBuf,
        remote_prefix: String,
        target_bytes: u64,
        minimum_age_secs: u64,
        maximum_generation_slot: u64,
    ) -> Self {
        Self {
            receipt_directory,
            remote_prefix,
            target_bytes,
            minimum_age_secs,
            minimum_retained_generations: R2_RETENTION_MIN_GENERATIONS,
            maximum_generation_slot,
            apply: false,
            now_unix_secs: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Receipt {
    commit_etag: String,
    commit_key: String,
    commit_sha256: String,
    file_count: usize,
    generation_id: String,
    manifest_etag: String,
    manifest_key: String,
    manifest_sha256: String,
    predecessor_manifest_sha256: Option<String>,
    remote_prefix: String,
    total_bytes: u64,
    verified_unix_secs: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReceiptChain {
    head_generation_id: String,
    head_manifest_sha256: String,
    oldest_to_newest: Vec<Receipt>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ObjectSpec {
    etag: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    sha256: String,
    size: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PreparedGeneration {
    commit: ObjectSpec,
    file_count: usize,
    generation_id: String,
    generation_prefix: String,
    manifest: ObjectSpec,
    manifest_sha256: String,
    payloads: Vec<ObjectSpec>,
    predecessor_manifest_sha256: Option<String>,
    total_bytes: u64,
    verified_unix_secs: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Anchor {
    bucket: String,
    chain_head_generation_id: String,
    chain_head_manifest_sha256: String,
    endpoint: String,
    first_retained_generation_id: String,
    first_retained_manifest_sha256: String,
    first_retained_predecessor_manifest_sha256: Option<String>,
    kind: String,
    last_operation_id: String,
    pruned_generation_count: usize,
    pruned_oldest_manifest_sha256: String,
    pruned_newest_manifest_sha256: String,
    pruned_payload_bytes: u64,
    remote_prefix: String,
    schema_version: u64,
    sequence: u64,
    storage_provider: String,
    updated_unix_secs: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Material {
    anchor_before_sha256: Option<String>,
    bucket: String,
    chain_head_generation_id: String,
    chain_head_manifest_sha256: String,
    delete_concurrency_precondition: String,
    endpoint: String,
    limited_by: Option<String>,
    maximum_generation_slot: u64,
    minimum_age_secs: u64,
    minimum_retained_generations: usize,
    planned_unix_secs: u64,
    remote_prefix: String,
    retained_generation_count_after: usize,
    retained_generation_count_before: usize,
    retained_payload_bytes_after: u64,
    retained_payload_bytes_before: u64,
    selected_generations: Vec<PreparedGeneration>,
    selected_payload_bytes: u64,
    target_bytes: u64,
    target_satisfied: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PhaseRecord {
    phase: String,
    unix_secs: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    anchor_after: Anchor,
    kind: String,
    operation_id: String,
    phase: String,
    phase_log: Vec<PhaseRecord>,
    plan: Material,
    prepared_unix_secs: u64,
    schema_version: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CompletedAudit {
    anchor_after: Anchor,
    completed_unix_secs: u64,
    kind: String,
    operation_id: String,
    phase: String,
    phase_log: Vec<PhaseRecord>,
    plan: Material,
    prepared_unix_secs: u64,
    schema_version: u64,
}

#[derive(Clone, Debug)]
struct Selection {
    limited_by: Option<&'static str>,
    retained_payload_bytes_after: u64,
    retained_payload_bytes_before: u64,
    selected: Vec<Receipt>,
    target_satisfied: bool,
}

trait R2Store {
    fn provider(&self) -> Provider;
    fn bucket(&self) -> &str;
    fn endpoint(&self) -> &str;
    fn get_control(&self, key: &str, expected_sha256: &str, expected_etag: &str)
    -> Result<Vec<u8>>;
    fn exact_identity(&self, spec: &ObjectSpec) -> Result<bool>;
    fn delete_object(&self, key: &str) -> Result<()>;
}

impl R2Store for S3Client {
    fn provider(&self) -> Provider {
        self.provider
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }

    fn endpoint(&self) -> &str {
        self.endpoint()
    }

    fn get_control(
        &self,
        key: &str,
        expected_sha256: &str,
        expected_etag: &str,
    ) -> Result<Vec<u8>> {
        let expected_sha256 = normalize_sha256(expected_sha256, "control-object SHA-256")?;
        let expected_etag = normalize_etag(expected_etag, "control-object ETag")?;
        let headers = BTreeMap::from([("if-match".to_string(), format!("\"{expected_etag}\""))]);
        let response = self.request(
            Method::GET,
            key,
            &BTreeMap::new(),
            &headers,
            EMPTY_SHA256,
            &Payload::Empty,
            &[],
        )?;
        reject_encoded_control_response(&response, key)?;
        let operation = format!("GET {key}");
        let remote_sha256 = response
            .exact_header("x-amz-meta-sha256", &operation)?
            .to_ascii_lowercase();
        if remote_sha256 != expected_sha256 {
            return Err(protocol(format!("GET {key} SHA-256 metadata mismatch")));
        }
        let remote_etag = normalize_etag(
            response.exact_header("etag", &operation)?,
            &format!("GET {key} ETag"),
        )?;
        if remote_etag != expected_etag {
            return Err(protocol(format!("GET {key} ETag mismatch")));
        }
        let declared = response.exact_content_length(&format!("GET {key}"))?;
        if declared == 0 || declared > MAX_RETENTION_CONTROL_OBJECT_BYTES as u64 {
            return Err(protocol(format!(
                "GET {key} returned an unsafe control-object size"
            )));
        }
        let body =
            response.read_bounded(MAX_RETENTION_CONTROL_OBJECT_BYTES, &format!("GET {key}"))?;
        if hex::encode(Sha256::digest(&body)) != expected_sha256 {
            return Err(protocol(format!("GET {key} body SHA-256 mismatch")));
        }
        if hex::encode(Md5::digest(&body)) != expected_etag {
            return Err(protocol(format!("GET {key} body ETag mismatch")));
        }
        Ok(body)
    }

    fn exact_identity(&self, spec: &ObjectSpec) -> Result<bool> {
        let response = self.head(&spec.key)?;
        if response.status == 404 {
            return Ok(false);
        }
        let length = response.exact_content_length(&format!("HEAD {}", spec.key))?;
        if length != spec.size {
            return Err(protocol(format!(
                "immutable R2 object collision at {}: size differs",
                spec.key
            )));
        }
        let operation = format!("HEAD {}", spec.key);
        let remote_sha256 = response
            .exact_header("x-amz-meta-sha256", &operation)?
            .to_ascii_lowercase();
        if remote_sha256 != spec.sha256 {
            return Err(protocol(format!(
                "immutable R2 object collision at {}: SHA-256 metadata differs",
                spec.key
            )));
        }
        let remote_etag = normalize_etag(
            response.exact_header("etag", &operation)?,
            &format!("HEAD {} ETag", spec.key),
        )?;
        if remote_etag != spec.etag {
            return Err(protocol(format!(
                "immutable R2 object collision at {}: ETag differs",
                spec.key
            )));
        }
        Ok(true)
    }

    fn delete_object(&self, key: &str) -> Result<()> {
        let _response = self.delete(key)?;
        Ok(())
    }
}

fn reject_encoded_control_response(response: &super::S3Response, key: &str) -> Result<()> {
    for (name, label) in [
        ("transfer-encoding", "Transfer-Encoding"),
        ("content-encoding", "Content-Encoding"),
    ] {
        let values = response.headers.get_all(name);
        let mut count = 0usize;
        for value in values.iter() {
            count += 1;
            let text = value
                .to_str()
                .map_err(|_| protocol(format!("GET {key} returned invalid {label}")))?;
            if !text.eq_ignore_ascii_case("identity") {
                return Err(protocol(format!("GET {key} must not use {label}")));
            }
        }
        if count > 1 {
            return Err(protocol(format!(
                "GET {key} returned multiple {label} values"
            )));
        }
    }
    Ok(())
}

/// Plan or apply R2 rolling retention from a locally durable receipt chain.
pub fn r2_retention(client: &S3Client, options: &R2RetentionOptions) -> Result<Value> {
    r2_retention_with_store(client, options)
}

fn r2_retention_with_store(store: &dyn R2Store, options: &R2RetentionOptions) -> Result<Value> {
    if store.provider() != Provider::R2 {
        return Err(config("rolling retention requires provider=r2"));
    }
    let retention_prefix = normalize_retention_prefix(&options.remote_prefix)?;
    validate_limits(options)?;
    let now_unix_secs = options.now_unix_secs.unwrap_or_else(current_unix_secs);
    require_positive(now_unix_secs, "current Unix time")?;
    let directory = RetentionDirectory::open(&options.receipt_directory)?;
    let lock = directory.lock()?;

    let chain = load_receipt_chain(&directory, &retention_prefix)?;
    let anchor = load_anchor(&directory, &chain, &retention_prefix, store)?;
    let pending = read_optional_pending(
        &directory,
        R2_RETENTION_PENDING_NAME,
        MAX_RETENTION_LOCAL_STATE_BYTES,
        "pending retention operation",
    )?;
    if let Some(pending) = pending {
        if !options.apply {
            return Err(protocol(
                "a prepared retention operation requires explicit --apply recovery",
            ));
        }
        let pending = validate_pending(
            pending,
            &chain,
            anchor.as_ref(),
            &retention_prefix,
            store,
            options,
        )?;
        return complete_apply(store, &directory, &lock, pending);
    }
    if anchor.is_some() && validate_completed_anchor_audit(&directory, anchor.as_ref())?.is_none() {
        return Err(config("completed prune receipt is missing"));
    }

    let active = active_chain(&chain, anchor.as_ref())?;
    let selection = select_tail(
        &active,
        options.target_bytes,
        options.minimum_age_secs,
        options.minimum_retained_generations,
        options.maximum_generation_slot,
        now_unix_secs,
    )?;
    let mut validated = Vec::with_capacity(selection.selected.len());
    for receipt in &selection.selected {
        validated.push(validate_remote_generation(store, receipt)?);
    }
    let material = build_material(
        &chain,
        anchor.as_ref(),
        &selection,
        validated,
        &retention_prefix,
        store,
        options,
        now_unix_secs,
    )?;
    if material.selected_generations.is_empty() {
        return public_result(
            &material,
            None,
            if options.apply { "apply" } else { "dry-run" },
            0,
            0,
        );
    }
    let operation_id = digest_serializable(&material)?;
    let anchor_after = build_anchor(
        &chain,
        anchor.as_ref(),
        &active,
        material.selected_generations.len(),
        &retention_prefix,
        store,
        &operation_id,
        now_unix_secs,
    )?;
    if !options.apply {
        return public_result(&material, Some(&operation_id), "dry-run", 0, 0);
    }

    // Remote validation may take minutes. The local chain and anchor are the
    // deletion authority, so pin them again immediately before journaling.
    lock.verify()?;
    let refreshed = load_receipt_chain(&directory, &retention_prefix)?;
    if refreshed != chain {
        return Err(protocol("receipt chain changed while planning retention"));
    }
    let refreshed_anchor = load_anchor(&directory, &refreshed, &retention_prefix, store)?;
    if anchor_digest(refreshed_anchor.as_ref())? != anchor_digest(anchor.as_ref())? {
        return Err(protocol("retention anchor changed while planning"));
    }
    if directory.entry_exists(R2_RETENTION_PENDING_NAME, "pending retention operation")? {
        return Err(protocol(
            "another retention operation was prepared concurrently",
        ));
    }
    let pending = Pending {
        anchor_after,
        kind: "r2-retention-pending".into(),
        operation_id,
        phase: "prepared".into(),
        phase_log: vec![PhaseRecord {
            phase: "prepared".into(),
            unix_secs: now_unix_secs,
        }],
        plan: material,
        prepared_unix_secs: now_unix_secs,
        schema_version: R2_RETENTION_SCHEMA_VERSION,
    };
    directory.write_json_exclusive(R2_RETENTION_PENDING_NAME, &pending)?;
    complete_apply(store, &directory, &lock, pending)
}

fn validate_limits(options: &R2RetentionOptions) -> Result<()> {
    for (value, label) in [
        (options.target_bytes, "retention target bytes"),
        (options.minimum_age_secs, "minimum age seconds"),
        (options.maximum_generation_slot, "maximum generation slot"),
    ] {
        if value > MAX_SUPPORTED_INTEGER {
            return Err(config(format!(
                "{label} exceeds the supported integer range"
            )));
        }
    }
    if options.minimum_retained_generations < R2_RETENTION_MIN_GENERATIONS {
        return Err(config(format!(
            "minimum retained generations must be an integer greater than or equal to {R2_RETENTION_MIN_GENERATIONS}"
        )));
    }
    Ok(())
}

fn normalize_retention_prefix(value: &str) -> Result<String> {
    let normalized = normalize_remote_prefix(value)?;
    if normalized.split('/').count() < 2 {
        return Err(config(
            "R2 retention prefix must contain at least two safe path components",
        ));
    }
    Ok(normalized)
}

fn normalize_remote_prefix(value: &str) -> Result<String> {
    let normalized = value.trim_end_matches('/');
    if normalized.is_empty()
        || normalized.starts_with('/')
        || normalized.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
    {
        return Err(config(
            "remote prefix must be non-empty, relative, and control-free",
        ));
    }
    if normalized
        .split('/')
        .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(config("remote prefix contains an unsafe path component"));
    }
    Ok(normalized.to_string())
}

fn normalize_sha256(value: &str, label: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    if normalized.len() != 64
        || !normalized
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(config(format!(
            "{label} must be exactly 64 hexadecimal characters"
        )));
    }
    Ok(normalized)
}

fn normalize_etag(value: &str, label: &str) -> Result<String> {
    let trimmed = value.trim();
    let unquoted = trimmed
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(trimmed);
    let normalized = unquoted.to_ascii_lowercase();
    if normalized.len() != 32
        || !normalized
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(config(format!(
            "{label} must be a single-part 32-hexadecimal ETag"
        )));
    }
    Ok(normalized)
}

fn generation_slot(generation_id: &str) -> Result<u64> {
    let Some(digits) = generation_id.strip_prefix("slot-") else {
        return Err(config("retention generation ID must use slot-%020d format"));
    };
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(config("retention generation ID must use slot-%020d format"));
    }
    let slot = digits
        .parse::<u64>()
        .map_err(|_| config("retention generation slot exceeds the supported integer range"))?;
    if slot > MAX_SUPPORTED_INTEGER {
        return Err(config(
            "retention generation slot exceeds the supported integer range",
        ));
    }
    Ok(slot)
}

fn valid_general_generation_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn validate_relative_path(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with('/')
        || value.contains('\\')
        || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        || value
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(config(format!("unsafe generation path {value:?}")));
    }
    Ok(())
}

fn validate_receipt(value: &Value, filename_id: &str, prefix: &str) -> Result<Receipt> {
    let object = value
        .as_object()
        .ok_or_else(|| config("generation receipt must be a JSON object"))?;
    if required_u64(object, "schema_version", "receipt schema")?
        != GENERATION_RECEIPT_SCHEMA_VERSION
    {
        return Err(config("unsupported generation receipt schema"));
    }
    if required_str(object, "storage_provider", "receipt storage provider")? != "r2" {
        return Err(config("retention authority contains a non-R2 receipt"));
    }
    if required_str(object, "object_identity", "receipt object identity")? != "single-put-etag" {
        return Err(config(
            "R2 receipt does not pin single-PUT object identities",
        ));
    }
    let generation_id = required_str(object, "generation_id", "generation receipt ID")?;
    if !valid_general_generation_id(generation_id) {
        return Err(config("generation receipt ID is invalid"));
    }
    generation_slot(generation_id)?;
    if generation_id != filename_id {
        return Err(config("generation receipt filename does not match its ID"));
    }
    let generation_prefix = normalize_remote_prefix(required_str(
        object,
        "remote_prefix",
        "generation receipt prefix",
    )?)?;
    if !generation_prefix.starts_with(&format!("{prefix}/")) {
        return Err(config("generation receipt is outside the retention prefix"));
    }
    if generation_prefix.rsplit('/').next() != Some(generation_id) {
        return Err(config("generation receipt prefix does not end with its ID"));
    }
    let manifest_key = required_str(object, "manifest_key", "receipt manifest key")?;
    let commit_key = required_str(object, "commit_key", "receipt commit key")?;
    if manifest_key != format!("{generation_prefix}/manifest.json") {
        return Err(config("generation receipt manifest key is invalid"));
    }
    if commit_key != format!("{generation_prefix}/_COMMITTED") {
        return Err(config("generation receipt commit key is invalid"));
    }
    let predecessor = optional_string(object, "predecessor_manifest_sha256")?
        .map(|value| normalize_sha256(value, "receipt predecessor SHA-256"))
        .transpose()?;
    let file_count_u64 = require_minimum(
        required_u64(object, "file_count", "receipt file count")?,
        1,
        "receipt file count",
    )?;
    let file_count = usize::try_from(file_count_u64)
        .map_err(|_| config("receipt file count exceeds the supported integer range"))?;
    Ok(Receipt {
        commit_etag: normalize_etag(
            required_str(object, "commit_version_id", "receipt commit identity")?,
            "commit ETag",
        )?,
        commit_key: commit_key.into(),
        commit_sha256: normalize_sha256(
            required_str(object, "commit_sha256", "receipt commit digest")?,
            "receipt commit SHA-256",
        )?,
        file_count,
        generation_id: generation_id.into(),
        manifest_etag: normalize_etag(
            required_str(object, "manifest_version_id", "receipt manifest identity")?,
            "manifest ETag",
        )?,
        manifest_key: manifest_key.into(),
        manifest_sha256: normalize_sha256(
            required_str(object, "manifest_sha256", "receipt manifest digest")?,
            "receipt manifest SHA-256",
        )?,
        predecessor_manifest_sha256: predecessor,
        remote_prefix: generation_prefix,
        total_bytes: require_minimum(
            required_u64(object, "total_bytes", "receipt total bytes")?,
            1,
            "receipt total bytes",
        )?,
        verified_unix_secs: require_minimum(
            required_u64(object, "verified_unix_secs", "receipt verification time")?,
            1,
            "receipt verification time",
        )?,
    })
}

fn load_receipt_chain(directory: &RetentionDirectory, prefix: &str) -> Result<ReceiptChain> {
    directory.verify_identity()?;
    let head_bytes = read_regular_file(
        directory,
        ".chain",
        MAX_RETENTION_CHAIN_FILE_BYTES,
        "receipt chain head",
    )?;
    if !head_bytes.is_ascii() {
        return Err(config("receipt chain head is not ASCII"));
    }
    let head_text =
        std::str::from_utf8(&head_bytes).map_err(|_| config("receipt chain head is not ASCII"))?;
    let parts = head_text.split_ascii_whitespace().collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(config(
            "receipt chain head must contain an ID and manifest digest",
        ));
    }
    if !valid_general_generation_id(parts[0]) {
        return Err(config("receipt chain head generation ID is invalid"));
    }
    generation_slot(parts[0])?;
    let head_hash = normalize_sha256(parts[1], "receipt chain head SHA-256")?;

    let mut receipts_by_hash = HashMap::new();
    for name in directory
        .handle
        .entry_names(MAX_RETENTION_DIRECTORY_ENTRIES, "receipt-chain directory")?
    {
        let Some(name) = name.to_str().map(str::to_owned) else {
            continue;
        };
        if name.starts_with('.') || !name.ends_with(".json") {
            continue;
        }
        let generation_id = &name[..name.len() - 5];
        if !valid_general_generation_id(generation_id) {
            continue;
        }
        let value = read_strict_json_value(
            directory,
            &name,
            MAX_GENERATION_RECEIPT_BYTES,
            &format!("generation receipt {name}"),
        )?;
        let receipt = validate_receipt(&value, generation_id, prefix)?;
        let digest = receipt.manifest_sha256.clone();
        if receipts_by_hash.insert(digest, receipt).is_some() {
            return Err(config("receipt chain contains duplicate manifest digests"));
        }
        if receipts_by_hash.len() > MAX_RETENTION_GENERATIONS {
            return Err(config("receipt chain contains too many generations"));
        }
    }
    directory.verify_identity()?;

    let mut newest_to_oldest = Vec::new();
    let mut seen = HashSet::new();
    let mut current = Some(head_hash.clone());
    while let Some(digest) = current {
        if !seen.insert(digest.clone()) {
            return Err(config("receipt chain is cyclic"));
        }
        let receipt = receipts_by_hash
            .get(&digest)
            .ok_or_else(|| config("receipt chain is incomplete"))?
            .clone();
        current = receipt.predecessor_manifest_sha256.clone();
        newest_to_oldest.push(receipt);
    }
    if newest_to_oldest.is_empty() {
        return Err(config("receipt chain is empty"));
    }
    if newest_to_oldest[0].generation_id != parts[0] {
        return Err(config(
            "receipt chain head ID does not match its manifest digest",
        ));
    }
    newest_to_oldest.reverse();
    Ok(ReceiptChain {
        head_generation_id: parts[0].into(),
        head_manifest_sha256: head_hash,
        oldest_to_newest: newest_to_oldest,
    })
}

fn load_anchor(
    directory: &RetentionDirectory,
    chain: &ReceiptChain,
    prefix: &str,
    store: &dyn R2Store,
) -> Result<Option<Anchor>> {
    let Some(anchor) = read_optional_json::<Anchor>(
        directory,
        R2_RETENTION_ANCHOR_NAME,
        MAX_RETENTION_LOCAL_STATE_BYTES,
        "retention anchor",
        ANCHOR_FIELDS,
    )?
    else {
        return Ok(None);
    };
    validate_anchor(anchor, chain, prefix, store).map(Some)
}

fn validate_anchor(
    mut anchor: Anchor,
    chain: &ReceiptChain,
    prefix: &str,
    store: &dyn R2Store,
) -> Result<Anchor> {
    if anchor.schema_version != R2_RETENTION_SCHEMA_VERSION
        || anchor.kind != "r2-retention-anchor"
        || anchor.storage_provider != "r2"
        || anchor.remote_prefix != prefix
        || anchor.bucket != store.bucket()
        || anchor.endpoint != store.endpoint()
    {
        return Err(config("retention anchor storage identity is invalid"));
    }
    require_positive(anchor.sequence, "anchor sequence")?;
    require_positive(anchor.updated_unix_secs, "anchor update time")?;
    anchor.last_operation_id =
        normalize_sha256(&anchor.last_operation_id, "retention operation ID")?;
    anchor.first_retained_manifest_sha256 = normalize_sha256(
        &anchor.first_retained_manifest_sha256,
        "first-retained manifest SHA-256",
    )?;
    let first_index = chain
        .oldest_to_newest
        .iter()
        .position(|receipt| receipt.manifest_sha256 == anchor.first_retained_manifest_sha256)
        .ok_or_else(|| config("retention anchor is not part of the local receipt chain"))?;
    if first_index < 1 {
        return Err(config("retention anchor does not bind a pruned tail"));
    }
    let first = &chain.oldest_to_newest[first_index];
    let pruned = &chain.oldest_to_newest[..first_index];
    if anchor.first_retained_generation_id != first.generation_id {
        return Err(config("retention anchor first-retained ID is invalid"));
    }
    if anchor.first_retained_predecessor_manifest_sha256 != first.predecessor_manifest_sha256 {
        return Err(config("retention anchor predecessor binding is invalid"));
    }
    if anchor.first_retained_predecessor_manifest_sha256.as_deref()
        != Some(pruned.last().expect("non-empty").manifest_sha256.as_str())
    {
        return Err(config(
            "retention anchor does not bind the newest pruned manifest",
        ));
    }
    if anchor.pruned_oldest_manifest_sha256 != pruned[0].manifest_sha256 {
        return Err(config("retention anchor oldest-tail binding is invalid"));
    }
    if anchor.pruned_newest_manifest_sha256 != pruned.last().expect("non-empty").manifest_sha256 {
        return Err(config("retention anchor newest-tail binding is invalid"));
    }
    if anchor.pruned_generation_count != pruned.len() {
        return Err(config("retention anchor pruned count is inconsistent"));
    }
    if anchor.pruned_payload_bytes != checked_receipt_bytes(pruned)? {
        return Err(config("retention anchor pruned bytes are inconsistent"));
    }
    anchor.chain_head_manifest_sha256 = normalize_sha256(
        &anchor.chain_head_manifest_sha256,
        "anchor chain-head SHA-256",
    )?;
    let historical_index = chain
        .oldest_to_newest
        .iter()
        .position(|receipt| receipt.manifest_sha256 == anchor.chain_head_manifest_sha256)
        .ok_or_else(|| config("retention anchor historical chain head is invalid"))?;
    if historical_index < first_index {
        return Err(config("retention anchor historical chain head is invalid"));
    }
    if anchor.chain_head_generation_id != chain.oldest_to_newest[historical_index].generation_id {
        return Err(config(
            "retention anchor historical chain-head ID is invalid",
        ));
    }
    Ok(anchor)
}

fn active_chain(chain: &ReceiptChain, anchor: Option<&Anchor>) -> Result<Vec<Receipt>> {
    let Some(anchor) = anchor else {
        return Ok(chain.oldest_to_newest.clone());
    };
    let index = chain
        .oldest_to_newest
        .iter()
        .position(|receipt| receipt.manifest_sha256 == anchor.first_retained_manifest_sha256)
        .ok_or_else(|| config("retention anchor is outside the receipt chain"))?;
    Ok(chain.oldest_to_newest[index..].to_vec())
}

fn select_tail(
    active: &[Receipt],
    target_bytes: u64,
    minimum_age_secs: u64,
    minimum_retained_generations: usize,
    maximum_generation_slot: u64,
    now_unix_secs: u64,
) -> Result<Selection> {
    let before = checked_receipt_bytes(active)?;
    let mut remaining = before;
    let mut selected = Vec::new();
    let mut limited_by = None;
    let maximum_prunable = active.len().saturating_sub(minimum_retained_generations);
    for receipt in active.iter().take(maximum_prunable) {
        if remaining <= target_bytes {
            break;
        }
        if maximum_generation_slot == 0
            || generation_slot(&receipt.generation_id)? > maximum_generation_slot
        {
            limited_by = Some("blockzilla_sync");
            break;
        }
        if receipt.verified_unix_secs > now_unix_secs
            || now_unix_secs - receipt.verified_unix_secs < minimum_age_secs
        {
            limited_by = Some("minimum_age");
            break;
        }
        selected.push(receipt.clone());
        remaining = remaining
            .checked_sub(receipt.total_bytes)
            .ok_or_else(|| config("retained payload total underflow"))?;
    }
    if remaining > target_bytes && limited_by.is_none() {
        limited_by = Some("minimum_retained_generations");
    }
    Ok(Selection {
        limited_by,
        retained_payload_bytes_after: remaining,
        retained_payload_bytes_before: before,
        selected,
        target_satisfied: remaining <= target_bytes,
    })
}

fn validate_remote_generation(
    store: &dyn R2Store,
    receipt: &Receipt,
) -> Result<PreparedGeneration> {
    let manifest_bytes = store.get_control(
        &receipt.manifest_key,
        &receipt.manifest_sha256,
        &receipt.manifest_etag,
    )?;
    let manifest = parse_strict_json(&manifest_bytes, "R2 generation manifest")?;
    let payloads = validate_remote_manifest(&manifest, receipt)?;
    let manifest_spec = ObjectSpec {
        etag: receipt.manifest_etag.clone(),
        key: receipt.manifest_key.clone(),
        path: None,
        sha256: receipt.manifest_sha256.clone(),
        size: manifest_bytes.len() as u64,
    };
    let commit_bytes = store.get_control(
        &receipt.commit_key,
        &receipt.commit_sha256,
        &receipt.commit_etag,
    )?;
    let commit = parse_strict_json(&commit_bytes, "R2 generation commit")?;
    validate_remote_commit(&commit, receipt)?;
    let commit_spec = ObjectSpec {
        etag: receipt.commit_etag.clone(),
        key: receipt.commit_key.clone(),
        path: None,
        sha256: receipt.commit_sha256.clone(),
        size: commit_bytes.len() as u64,
    };
    for spec in &payloads {
        if !store.exact_identity(spec)? {
            return Err(protocol(format!(
                "R2 committed generation payload is missing: {}",
                spec.key
            )));
        }
    }
    Ok(PreparedGeneration {
        commit: commit_spec,
        file_count: receipt.file_count,
        generation_id: receipt.generation_id.clone(),
        generation_prefix: receipt.remote_prefix.clone(),
        manifest: manifest_spec,
        manifest_sha256: receipt.manifest_sha256.clone(),
        payloads,
        predecessor_manifest_sha256: receipt.predecessor_manifest_sha256.clone(),
        total_bytes: receipt.total_bytes,
        verified_unix_secs: receipt.verified_unix_secs,
    })
}

fn validate_remote_manifest(value: &Value, receipt: &Receipt) -> Result<Vec<ObjectSpec>> {
    let object = exact_object(
        value,
        if receipt.predecessor_manifest_sha256.is_some() {
            REMOTE_MANIFEST_WITH_PREDECESSOR_FIELDS
        } else {
            REMOTE_MANIFEST_FIELDS
        },
        "R2 generation manifest fields are invalid",
    )?;
    if required_u64(object, "schema_version", "manifest schema")?
        != GENERATION_MANIFEST_SCHEMA_VERSION
        || required_u64(object, "total_bytes", "manifest total bytes")? != receipt.total_bytes
        || required_str(object, "storage_provider", "manifest storage provider")? != "r2"
        || required_str(object, "object_identity", "manifest object identity")? != "single-put-etag"
        || required_str(object, "generation_id", "manifest generation ID")? != receipt.generation_id
        || optional_string(object, "predecessor_manifest_sha256")?
            != receipt.predecessor_manifest_sha256.as_deref()
    {
        return Err(protocol(
            "R2 generation manifest does not match its local receipt",
        ));
    }
    let files = object
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| protocol("R2 generation manifest file count is invalid"))?;
    if files.len() != receipt.file_count {
        return Err(protocol("R2 generation manifest file count is invalid"));
    }
    let mut validated = Vec::with_capacity(files.len());
    let mut paths = HashSet::new();
    let mut keys = HashSet::new();
    let mut total = 0u64;
    for record in files {
        let record = exact_object(
            record,
            REMOTE_MANIFEST_FILE_FIELDS,
            "R2 generation manifest file record is invalid",
        )?;
        let path = required_str(record, "path", "R2 generation manifest path")?;
        validate_relative_path(path).map_err(|error| protocol(error.to_string()))?;
        let expected_key = format!("{}/files/{path}", receipt.remote_prefix);
        let key = required_str(record, "object_key", "manifest object key")?;
        if key != expected_key {
            return Err(protocol(
                "R2 generation manifest contains a key outside its generation prefix",
            ));
        }
        let size = required_u64(record, "size", "manifest file size")?;
        let sha256 = normalize_sha256(
            required_str(record, "sha256", "manifest file SHA-256")?,
            "manifest file SHA-256",
        )?;
        let etag = normalize_etag(
            required_str(record, "version_id", "manifest file ETag")?,
            "manifest file ETag",
        )?;
        if !paths.insert(path.to_string()) || !keys.insert(key.to_string()) {
            return Err(protocol("R2 generation manifest contains a duplicate file"));
        }
        total = checked_add(total, size, "R2 generation manifest byte total")?;
        validated.push(ObjectSpec {
            etag,
            key: key.into(),
            path: Some(path.into()),
            sha256,
            size,
        });
    }
    if !validated
        .windows(2)
        .all(|pair| pair[0].path.as_deref() < pair[1].path.as_deref())
    {
        return Err(protocol(
            "R2 generation manifest files are not canonically ordered",
        ));
    }
    if total != receipt.total_bytes {
        return Err(protocol("R2 generation manifest byte total is invalid"));
    }
    Ok(validated)
}

fn validate_remote_commit(value: &Value, receipt: &Receipt) -> Result<()> {
    let object = exact_object(
        value,
        if receipt.predecessor_manifest_sha256.is_some() {
            REMOTE_COMMIT_WITH_PREDECESSOR_FIELDS
        } else {
            REMOTE_COMMIT_FIELDS
        },
        "R2 generation commit fields are invalid",
    )?;
    if required_u64(object, "schema_version", "commit schema")? != GENERATION_COMMIT_SCHEMA_VERSION
        || required_u64(object, "file_count", "commit file count")? != receipt.file_count as u64
        || required_u64(object, "total_bytes", "commit total bytes")? != receipt.total_bytes
        || required_str(object, "storage_provider", "commit storage provider")? != "r2"
        || required_str(object, "object_identity", "commit object identity")? != "single-put-etag"
        || required_str(object, "generation_id", "commit generation ID")? != receipt.generation_id
        || required_str(object, "manifest_key", "commit manifest key")? != receipt.manifest_key
        || normalize_sha256(
            required_str(object, "manifest_sha256", "commit manifest digest")?,
            "commit manifest SHA-256",
        )? != receipt.manifest_sha256
        || normalize_etag(
            required_str(object, "manifest_version_id", "commit manifest identity")?,
            "commit manifest ETag",
        )? != receipt.manifest_etag
        || optional_string(object, "predecessor_manifest_sha256")?
            != receipt.predecessor_manifest_sha256.as_deref()
    {
        return Err(protocol(
            "R2 generation commit does not match its local receipt",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_material(
    chain: &ReceiptChain,
    anchor: Option<&Anchor>,
    selection: &Selection,
    selected_generations: Vec<PreparedGeneration>,
    prefix: &str,
    store: &dyn R2Store,
    options: &R2RetentionOptions,
    now: u64,
) -> Result<Material> {
    let active_count = active_chain(chain, anchor)?.len();
    Ok(Material {
        anchor_before_sha256: anchor_digest(anchor)?,
        bucket: store.bucket().into(),
        chain_head_generation_id: chain.head_generation_id.clone(),
        chain_head_manifest_sha256: chain.head_manifest_sha256.clone(),
        delete_concurrency_precondition: R2_DELETE_CONCURRENCY_PRECONDITION.into(),
        endpoint: store.endpoint().into(),
        limited_by: selection.limited_by.map(str::to_string),
        maximum_generation_slot: options.maximum_generation_slot,
        minimum_age_secs: options.minimum_age_secs,
        minimum_retained_generations: options.minimum_retained_generations,
        planned_unix_secs: now,
        remote_prefix: prefix.into(),
        retained_generation_count_after: active_count - selected_generations.len(),
        retained_generation_count_before: active_count,
        retained_payload_bytes_after: selection.retained_payload_bytes_after,
        retained_payload_bytes_before: selection.retained_payload_bytes_before,
        selected_payload_bytes: checked_prepared_bytes(&selected_generations)?,
        selected_generations,
        target_bytes: options.target_bytes,
        target_satisfied: selection.target_satisfied,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_anchor(
    chain: &ReceiptChain,
    previous: Option<&Anchor>,
    active: &[Receipt],
    selected_count: usize,
    prefix: &str,
    store: &dyn R2Store,
    operation_id: &str,
    now: u64,
) -> Result<Anchor> {
    if selected_count == 0 || selected_count >= active.len() {
        return Err(config("retention anchor requires a non-empty pruned tail"));
    }
    let first = &active[selected_count];
    let first_index = chain
        .oldest_to_newest
        .iter()
        .position(|receipt| receipt.manifest_sha256 == first.manifest_sha256)
        .ok_or_else(|| config("first retained generation is outside the receipt chain"))?;
    let pruned = &chain.oldest_to_newest[..first_index];
    let sequence = previous
        .map_or(0, |anchor| anchor.sequence)
        .checked_add(1)
        .ok_or_else(|| config("retention anchor sequence overflow"))?;
    Ok(Anchor {
        bucket: store.bucket().into(),
        chain_head_generation_id: chain.head_generation_id.clone(),
        chain_head_manifest_sha256: chain.head_manifest_sha256.clone(),
        endpoint: store.endpoint().into(),
        first_retained_generation_id: first.generation_id.clone(),
        first_retained_manifest_sha256: first.manifest_sha256.clone(),
        first_retained_predecessor_manifest_sha256: first.predecessor_manifest_sha256.clone(),
        kind: "r2-retention-anchor".into(),
        last_operation_id: operation_id.into(),
        pruned_generation_count: pruned.len(),
        pruned_oldest_manifest_sha256: pruned[0].manifest_sha256.clone(),
        pruned_newest_manifest_sha256: pruned.last().expect("non-empty").manifest_sha256.clone(),
        pruned_payload_bytes: checked_receipt_bytes(pruned)?,
        remote_prefix: prefix.into(),
        schema_version: R2_RETENTION_SCHEMA_VERSION,
        sequence,
        storage_provider: "r2".into(),
        updated_unix_secs: now,
    })
}

fn validate_pending(
    pending: Pending,
    chain: &ReceiptChain,
    current_anchor: Option<&Anchor>,
    prefix: &str,
    store: &dyn R2Store,
    options: &R2RetentionOptions,
) -> Result<Pending> {
    if pending.schema_version != R2_RETENTION_SCHEMA_VERSION
        || pending.kind != "r2-retention-pending"
    {
        return Err(config("pending retention operation schema is invalid"));
    }
    let operation_id = normalize_sha256(&pending.operation_id, "retention operation ID")?;
    if digest_serializable(&pending.plan)? != operation_id {
        return Err(config("pending retention operation digest is invalid"));
    }
    if !valid_pending_phase(&pending.phase) || pending.phase_log.is_empty() {
        return Err(config("pending retention phase log is invalid"));
    }
    for record in &pending.phase_log {
        if !valid_pending_phase(&record.phase) {
            return Err(config("pending retention phase log is invalid"));
        }
        require_positive(record.unix_secs, "pending phase time")?;
    }
    require_positive(pending.prepared_unix_secs, "retention prepare time")?;
    validate_material_shape(&pending.plan, prefix, store, options)?;
    let anchor_after = validate_anchor(pending.anchor_after.clone(), chain, prefix, store)?;
    if anchor_after.last_operation_id != operation_id {
        return Err(config(
            "pending retention anchor operation binding is invalid",
        ));
    }
    let current_digest = anchor_digest(current_anchor)?;
    let after_digest = anchor_digest(Some(&anchor_after))?;
    if current_digest != pending.plan.anchor_before_sha256 && current_digest != after_digest {
        return Err(config(
            "retention anchor changed outside the pending operation",
        ));
    }
    let plan_head_index = chain
        .oldest_to_newest
        .iter()
        .position(|receipt| {
            receipt.manifest_sha256 == pending.plan.chain_head_manifest_sha256
                && receipt.generation_id == pending.plan.chain_head_generation_id
        })
        .ok_or_else(|| config("pending retention plan chain head is invalid"))?;
    let first_retained_index = chain
        .oldest_to_newest
        .iter()
        .position(|receipt| receipt.manifest_sha256 == anchor_after.first_retained_manifest_sha256)
        .ok_or_else(|| config("pending retention anchor is outside the receipt chain"))?;
    if plan_head_index < first_retained_index {
        return Err(config("pending retention plan retains no chain head"));
    }
    let selected_len = pending.plan.selected_generations.len();
    let selected_start = first_retained_index
        .checked_sub(selected_len)
        .ok_or_else(|| config("pending retention selection is outside the receipt chain"))?;
    if selected_len == 0 {
        return Err(config("pending retention plan has no selected generations"));
    }
    let expected = &chain.oldest_to_newest[selected_start..first_retained_index];
    if pending
        .plan
        .selected_generations
        .iter()
        .map(|entry| entry.manifest_sha256.as_str())
        .ne(expected
            .iter()
            .map(|receipt| receipt.manifest_sha256.as_str()))
    {
        return Err(config(
            "pending retention selection is not the oldest contiguous tail",
        ));
    }
    for (entry, receipt) in pending
        .plan
        .selected_generations
        .iter()
        .zip(expected.iter())
    {
        validate_prepared_generation(entry, receipt)?;
    }
    let active_at_plan = &chain.oldest_to_newest[selected_start..=plan_head_index];
    let recomputed = select_tail(
        active_at_plan,
        options.target_bytes,
        options.minimum_age_secs,
        options.minimum_retained_generations,
        options.maximum_generation_slot,
        pending.plan.planned_unix_secs,
    )?;
    if recomputed
        .selected
        .iter()
        .map(|receipt| receipt.manifest_sha256.as_str())
        .ne(pending
            .plan
            .selected_generations
            .iter()
            .map(|entry| entry.manifest_sha256.as_str()))
    {
        return Err(config("pending retention selection no longer validates"));
    }
    let expected_values_match = pending.plan.limited_by.as_deref() == recomputed.limited_by
        && pending.plan.retained_generation_count_after == active_at_plan.len() - selected_len
        && pending.plan.retained_generation_count_before == active_at_plan.len()
        && pending.plan.retained_payload_bytes_after == recomputed.retained_payload_bytes_after
        && pending.plan.retained_payload_bytes_before == recomputed.retained_payload_bytes_before
        && pending.plan.selected_payload_bytes
            == checked_prepared_bytes(&pending.plan.selected_generations)?
        && pending.plan.target_satisfied == recomputed.target_satisfied;
    if !expected_values_match {
        return Err(config("pending retention plan is inconsistent"));
    }
    Ok(pending)
}

fn validate_material_shape(
    material: &Material,
    prefix: &str,
    store: &dyn R2Store,
    options: &R2RetentionOptions,
) -> Result<()> {
    if !matches!(
        material.limited_by.as_deref(),
        None | Some("blockzilla_sync" | "minimum_age" | "minimum_retained_generations")
    ) || material.delete_concurrency_precondition != R2_DELETE_CONCURRENCY_PRECONDITION
    {
        return Err(config("pending retention plan result fields are invalid"));
    }
    if material.retained_generation_count_after < R2_RETENTION_MIN_GENERATIONS
        || material.retained_generation_count_before < R2_RETENTION_MIN_GENERATIONS + 1
        || material.retained_payload_bytes_after == 0
        || material.retained_payload_bytes_before == 0
        || material.selected_payload_bytes == 0
    {
        return Err(config("pending retention plan integer fields are invalid"));
    }
    require_positive(material.planned_unix_secs, "retention plan time")?;
    if material.remote_prefix != prefix
        || material.bucket != store.bucket()
        || material.endpoint != store.endpoint()
        || material.target_bytes != options.target_bytes
        || material.maximum_generation_slot != options.maximum_generation_slot
        || material.minimum_age_secs != options.minimum_age_secs
        || material.minimum_retained_generations != options.minimum_retained_generations
    {
        return Err(config(
            "pending retention plan does not match this invocation",
        ));
    }
    if let Some(digest) = &material.anchor_before_sha256 {
        normalize_sha256(digest, "prior anchor SHA-256")?;
    }
    validate_delete_cutoff(material)
}

fn validate_delete_cutoff(material: &Material) -> Result<()> {
    if material.maximum_generation_slot > MAX_SUPPORTED_INTEGER {
        return Err(config(
            "maximum generation slot exceeds the supported integer range",
        ));
    }
    if material.maximum_generation_slot == 0 && !material.selected_generations.is_empty() {
        return Err(config(
            "maximum generation slot zero authorizes no deletion",
        ));
    }
    for entry in &material.selected_generations {
        if generation_slot(&entry.generation_id)? > material.maximum_generation_slot {
            return Err(config(
                "prepared retention generation exceeds the Blockzilla durable cutoff",
            ));
        }
    }
    Ok(())
}

fn validate_prepared_generation(entry: &PreparedGeneration, receipt: &Receipt) -> Result<()> {
    if entry.generation_id != receipt.generation_id
        || entry.generation_prefix != receipt.remote_prefix
        || entry.manifest_sha256 != receipt.manifest_sha256
        || entry.predecessor_manifest_sha256 != receipt.predecessor_manifest_sha256
        || entry.file_count != receipt.file_count
        || entry.total_bytes != receipt.total_bytes
        || entry.verified_unix_secs != receipt.verified_unix_secs
    {
        return Err(config(
            "prepared retention generation does not match its receipt",
        ));
    }
    validate_spec(&entry.commit, &receipt.commit_key, false)?;
    validate_spec(&entry.manifest, &receipt.manifest_key, false)?;
    if entry.commit.sha256 != receipt.commit_sha256 || entry.commit.etag != receipt.commit_etag {
        return Err(config("prepared commit does not match its receipt"));
    }
    if entry.manifest.sha256 != receipt.manifest_sha256
        || entry.manifest.etag != receipt.manifest_etag
    {
        return Err(config("prepared manifest does not match its receipt"));
    }
    if entry.payloads.len() != receipt.file_count {
        return Err(config("prepared payload list has an invalid file count"));
    }
    let mut seen = HashSet::new();
    let mut previous_path: Option<&str> = None;
    let mut total = 0;
    for spec in &entry.payloads {
        let path = spec
            .path
            .as_deref()
            .ok_or_else(|| config("prepared payload specification is invalid"))?;
        validate_relative_path(path)?;
        validate_spec(
            spec,
            &format!("{}/files/{path}", receipt.remote_prefix),
            true,
        )?;
        if !seen.insert(&spec.key) {
            return Err(config("prepared payload list contains duplicate keys"));
        }
        if previous_path.is_some_and(|previous| previous >= path) {
            return Err(config("prepared payload list is not canonically ordered"));
        }
        previous_path = Some(path);
        total = checked_add(total, spec.size, "prepared payload byte total")?;
    }
    if total != receipt.total_bytes {
        return Err(config("prepared payload byte total is invalid"));
    }
    Ok(())
}

fn validate_spec(spec: &ObjectSpec, expected_key: &str, payload: bool) -> Result<()> {
    if spec.key != expected_key || spec.path.is_some() != payload {
        return Err(config("prepared retention object key is invalid"));
    }
    normalize_sha256(&spec.sha256, "prepared object SHA-256")?;
    normalize_etag(&spec.etag, "prepared object ETag")?;
    if !payload && spec.size == 0 {
        return Err(config("prepared control-object size must be positive"));
    }
    Ok(())
}

fn complete_apply(
    store: &dyn R2Store,
    directory: &RetentionDirectory,
    lock: &RetentionLock,
    mut pending: Pending,
) -> Result<Value> {
    lock.verify()?;
    validate_delete_cutoff(&pending.plan)?;
    let current_anchor = read_optional_json::<Anchor>(
        directory,
        R2_RETENTION_ANCHOR_NAME,
        MAX_RETENTION_LOCAL_STATE_BYTES,
        "retention anchor",
        ANCHOR_FIELDS,
    )?;
    let current_anchor_digest = anchor_digest(current_anchor.as_ref())?;
    let anchor_after_digest = anchor_digest(Some(&pending.anchor_after))?;
    if current_anchor_digest != pending.plan.anchor_before_sha256
        && current_anchor_digest != anchor_after_digest
    {
        return Err(config(
            "retention anchor changed outside the pending operation",
        ));
    }
    if current_anchor.as_ref() == Some(&pending.anchor_after)
        && let Some(audit) = validate_completed_anchor_audit(directory, current_anchor.as_ref())?
    {
        if audit.plan != pending.plan {
            return Err(config("completed prune receipt plan collision"));
        }
        directory.unlink_regular(R2_RETENTION_PENDING_NAME)?;
        let absent =
            pending
                .plan
                .selected_generations
                .iter()
                .try_fold(0usize, |count, entry| {
                    count
                        .checked_add(2 + entry.payloads.len())
                        .ok_or_else(|| config("retention object count overflow"))
                })?;
        return public_result(
            &pending.plan,
            Some(&pending.operation_id),
            "apply",
            0,
            absent,
        );
    }
    // If the current anchor is `anchor_after` but its audit is missing, this is
    // the crash boundary immediately after the durable anchor rename. Fall
    // through, re-verify absence, recreate the audit, and then clear pending.

    let mut delete_requests = 0usize;
    let mut already_absent = 0usize;
    pending = advance_phase(directory, pending, "deleting_generations")?;
    for entry in &pending.plan.selected_generations {
        for spec in std::iter::once(&entry.commit)
            .chain(std::iter::once(&entry.manifest))
            .chain(entry.payloads.iter())
        {
            lock.verify()?;
            if delete_verified(store, spec)? {
                delete_requests += 1;
            } else {
                already_absent += 1;
            }
        }
    }
    lock.verify()?;
    pending = advance_phase(directory, pending, "generations_deleted")?;
    for entry in &pending.plan.selected_generations {
        for spec in std::iter::once(&entry.commit)
            .chain(std::iter::once(&entry.manifest))
            .chain(entry.payloads.iter())
        {
            if store.exact_identity(spec)? {
                return Err(protocol(format!(
                    "planned R2 object remains after pruning: {}",
                    spec.key
                )));
            }
        }
    }

    lock.verify()?;
    let expected_anchor = current_anchor
        .as_ref()
        .map(|anchor| serialized_bytes(anchor, MAX_RETENTION_LOCAL_STATE_BYTES, "retention anchor"))
        .transpose()?;
    directory.write_json_atomic(
        R2_RETENTION_ANCHOR_NAME,
        expected_anchor.as_deref(),
        &pending.anchor_after,
    )?;
    pending = advance_phase(directory, pending, "anchor_written")?;
    let completed_at = current_unix_secs();
    let mut phase_log = pending.phase_log.clone();
    phase_log.push(PhaseRecord {
        phase: "completed".into(),
        unix_secs: completed_at,
    });
    let completed = CompletedAudit {
        anchor_after: pending.anchor_after.clone(),
        completed_unix_secs: completed_at,
        kind: "r2-retention-prune-receipt".into(),
        operation_id: pending.operation_id.clone(),
        phase: "completed".into(),
        phase_log,
        plan: pending.plan.clone(),
        prepared_unix_secs: pending.prepared_unix_secs,
        schema_version: R2_RETENTION_SCHEMA_VERSION,
    };
    let audit_name = audit_filename(pending.anchor_after.sequence, &pending.operation_id);
    match directory.write_json_exclusive(&audit_name, &completed) {
        Ok(()) => {}
        Err(UploaderError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = read_required_completed_audit(
                directory,
                &audit_name,
                MAX_RETENTION_LOCAL_STATE_BYTES,
                "completed prune receipt",
            )?;
            if existing != completed {
                return Err(config("completed prune receipt collision"));
            }
        }
        Err(error) => return Err(error),
    }
    directory.unlink_regular(R2_RETENTION_PENDING_NAME)?;
    public_result(
        &pending.plan,
        Some(&pending.operation_id),
        "apply",
        delete_requests,
        already_absent,
    )
}

fn delete_verified(store: &dyn R2Store, spec: &ObjectSpec) -> Result<bool> {
    if !store.exact_identity(spec)? {
        return Ok(false);
    }
    store.delete_object(&spec.key)?;
    if store.exact_identity(spec)? {
        return Err(protocol(format!(
            "R2 object still exists after DELETE: {}",
            spec.key
        )));
    }
    Ok(true)
}

fn advance_phase(
    directory: &RetentionDirectory,
    mut pending: Pending,
    phase: &str,
) -> Result<Pending> {
    let expected = serialized_bytes(
        &pending,
        MAX_RETENTION_LOCAL_STATE_BYTES,
        "pending retention operation",
    )?;
    pending.phase = phase.into();
    pending.phase_log.push(PhaseRecord {
        phase: phase.into(),
        unix_secs: current_unix_secs(),
    });
    directory.write_json_atomic(R2_RETENTION_PENDING_NAME, Some(&expected), &pending)?;
    Ok(pending)
}

fn validate_completed_anchor_audit(
    directory: &RetentionDirectory,
    anchor: Option<&Anchor>,
) -> Result<Option<CompletedAudit>> {
    let Some(anchor) = anchor else {
        return Ok(None);
    };
    let Some(audit) = read_optional_completed_audit(
        directory,
        &audit_filename(anchor.sequence, &anchor.last_operation_id),
        MAX_RETENTION_LOCAL_STATE_BYTES,
        "completed prune receipt",
    )?
    else {
        return Ok(None);
    };
    if audit.schema_version != R2_RETENTION_SCHEMA_VERSION
        || audit.kind != "r2-retention-prune-receipt"
        || audit.phase != "completed"
        || audit.operation_id != anchor.last_operation_id
        || audit.anchor_after != *anchor
    {
        return Err(config(
            "completed prune receipt does not match the retention anchor",
        ));
    }
    if audit.phase_log.is_empty()
        || audit.phase_log.last().map(|record| record.phase.as_str()) != Some("completed")
    {
        return Err(config("completed prune receipt audit trail is invalid"));
    }
    for record in &audit.phase_log {
        require_positive(record.unix_secs, "prune phase time")?;
    }
    require_positive(audit.prepared_unix_secs, "prune prepare time")?;
    require_positive(audit.completed_unix_secs, "prune completion time")?;
    if digest_serializable(&audit.plan)? != audit.operation_id {
        return Err(config(
            "completed prune receipt operation digest is invalid",
        ));
    }
    Ok(Some(audit))
}

fn public_result(
    material: &Material,
    operation_id: Option<&str>,
    mode: &str,
    delete_requests: usize,
    already_absent: usize,
) -> Result<Value> {
    let selected_ids = material
        .selected_generations
        .iter()
        .map(|entry| Value::String(entry.generation_id.clone()))
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "already_absent_object_count": already_absent,
        "bucket": material.bucket,
        "delete_concurrency_precondition": material.delete_concurrency_precondition,
        "delete_request_count": delete_requests,
        "limited_by": material.limited_by,
        "maximum_generation_slot": material.maximum_generation_slot,
        "minimum_age_secs": material.minimum_age_secs,
        "minimum_retained_generations": material.minimum_retained_generations,
        "mode": mode,
        "operation_id": operation_id,
        "remote_prefix": material.remote_prefix,
        "retained_generation_count_after": material.retained_generation_count_after,
        "retained_generation_count_before": material.retained_generation_count_before,
        "retained_payload_bytes_after": material.retained_payload_bytes_after,
        "retained_payload_bytes_before": material.retained_payload_bytes_before,
        "schema_version": R2_RETENTION_SCHEMA_VERSION,
        "selected_generation_ids": selected_ids,
        "selected_payload_bytes": material.selected_payload_bytes,
        "storage_provider": "r2",
        "target_bytes": material.target_bytes,
        "target_satisfied": material.target_satisfied,
    }))
}

fn audit_filename(sequence: u64, operation_id: &str) -> String {
    format!(".r2-prune-{sequence:020}-{operation_id}.json")
}

fn anchor_digest(anchor: Option<&Anchor>) -> Result<Option<String>> {
    anchor.map(digest_serializable).transpose()
}

fn digest_serializable(value: &impl Serialize) -> Result<String> {
    let value = serde_json::to_value(value)?;
    Ok(hex::encode(Sha256::digest(canonical_json_bytes(&value)?)))
}

fn checked_receipt_bytes(receipts: &[Receipt]) -> Result<u64> {
    receipts.iter().try_fold(0u64, |sum, receipt| {
        checked_add(sum, receipt.total_bytes, "retained payload total")
    })
}

fn checked_prepared_bytes(generations: &[PreparedGeneration]) -> Result<u64> {
    generations.iter().try_fold(0u64, |sum, generation| {
        checked_add(sum, generation.total_bytes, "selected payload total")
    })
}

fn checked_add(left: u64, right: u64, label: &str) -> Result<u64> {
    let total = left
        .checked_add(right)
        .ok_or_else(|| config(format!("{label} exceeds the supported integer range")))?;
    if total > MAX_SUPPORTED_INTEGER {
        return Err(config(format!(
            "{label} exceeds the supported integer range"
        )));
    }
    Ok(total)
}

fn require_minimum(value: u64, minimum: u64, label: &str) -> Result<u64> {
    if value < minimum || value > MAX_SUPPORTED_INTEGER {
        return Err(config(format!(
            "{label} must be an integer greater than or equal to {minimum}"
        )));
    }
    Ok(value)
}

fn require_positive(value: u64, label: &str) -> Result<u64> {
    require_minimum(value, 1, label)
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |duration| duration.as_secs().max(1))
}

fn required_str<'a>(object: &'a Map<String, Value>, name: &str, label: &str) -> Result<&'a str> {
    object
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| config(format!("{label} is invalid")))
}

fn optional_string<'a>(object: &'a Map<String, Value>, name: &str) -> Result<Option<&'a str>> {
    match object.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(config(format!("{name} is invalid"))),
    }
}

fn required_u64(object: &Map<String, Value>, name: &str, label: &str) -> Result<u64> {
    object
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| config(format!("{label} must be a non-negative integer")))
}

fn exact_object<'a>(
    value: &'a Value,
    expected: &[&str],
    message: &str,
) -> Result<&'a Map<String, Value>> {
    let object = value
        .as_object()
        .ok_or_else(|| protocol(message.to_string()))?;
    let actual = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(protocol(message.to_string()));
    }
    Ok(object)
}

fn valid_pending_phase(value: &str) -> bool {
    matches!(
        value,
        "prepared"
            | "deleting_generations"
            | "generations_deleted"
            | "deleting_commits"
            | "commits_deleted"
            | "deleting_manifests"
            | "manifests_deleted"
            | "deleting_payloads"
            | "payloads_deleted"
            | "anchor_written"
    )
}

struct RetentionDirectory {
    handle: DirectoryHandle,
}

impl RetentionDirectory {
    fn open(requested: &Path) -> Result<Self> {
        let handle = DirectoryHandle::open_existing(requested, "receipt-chain path")?;
        handle.require_private_owner("receipt-chain directory")?;
        Ok(Self { handle })
    }

    fn verify_identity(&self) -> Result<()> {
        self.handle.verify()
    }

    fn entry_exists(&self, name: &str, label: &str) -> Result<bool> {
        Ok(self
            .handle
            .open_regular_optional(OsStr::new(name), label)?
            .is_some())
    }

    fn lock(&self) -> Result<RetentionLock> {
        self.verify_identity()?;
        let file =
            self.handle
                .open_lock(OsStr::new(R2_RETENTION_LOCK_NAME), 0o600, "retention lock")?;
        let opened = file.metadata()?;
        let directory_metadata = self.handle.metadata()?;
        if opened.uid() != directory_metadata.uid() || opened.mode() & 0o077 != 0 {
            return Err(config(
                "retention lock must be owner-controlled with private permissions",
            ));
        }
        FileExt::lock_exclusive(&file)?;
        let current = self
            .handle
            .open_regular_optional(OsStr::new(R2_RETENTION_LOCK_NAME), "retention lock")?
            .ok_or_else(|| config("retention lock disappeared while acquiring it"))?
            .metadata()?;
        if current.dev() != opened.dev() || current.ino() != opened.ino() {
            return Err(config("retention lock changed while acquiring it"));
        }
        self.verify_identity()?;
        Ok(RetentionLock {
            file,
            directory: self.handle.try_clone()?,
            device: opened.dev(),
            inode: opened.ino(),
        })
    }

    fn require_expected_state(&self, name: &str, expected: Option<&[u8]>) -> Result<()> {
        let current = self.handle.read_regular_optional(
            OsStr::new(name),
            MAX_RETENTION_LOCAL_STATE_BYTES,
            "retention state",
        )?;
        match (current.as_deref(), expected) {
            (None, None) => Ok(()),
            (Some(current), Some(expected)) if current == expected => Ok(()),
            _ => Err(protocol("retention state changed before publication")),
        }
    }

    fn write_json_atomic(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        value: &impl Serialize,
    ) -> Result<()> {
        let temporary_name = format!(
            ".{name}.tmp.{}.{}",
            std::process::id(),
            TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        self.write_json_atomic_with_temporary(name, expected, value, &temporary_name)
    }

    fn write_json_atomic_with_temporary(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        value: &impl Serialize,
        temporary_name: &str,
    ) -> Result<()> {
        self.verify_identity()?;
        self.require_expected_state(name, expected)?;
        let bytes = serialized_bytes(value, MAX_RETENTION_LOCAL_STATE_BYTES, "retention state")?;
        let mut created = None;
        let result = (|| {
            let mut file = self
                .handle
                .create_exclusive(OsStr::new(temporary_name), 0o600)?;
            let metadata = file.metadata()?;
            created = Some((metadata.dev(), metadata.ino()));
            file.write_all(&bytes)?;
            file.sync_all()?;
            self.verify_identity()?;
            self.require_expected_state(name, expected)?;
            self.handle.rename_same_inode(
                OsStr::new(temporary_name),
                OsStr::new(name),
                created.expect("temporary identity recorded"),
                "retention temporary state",
            )?;
            self.handle.sync()?;
            if self.handle.read_regular(
                OsStr::new(name),
                MAX_RETENTION_LOCAL_STATE_BYTES,
                "retention state",
            )? != bytes
            {
                return Err(protocol("retention state changed after publication"));
            }
            Ok(())
        })();
        if let Some(identity) = created {
            let cleanup = self.handle.unlink_if_same_inode(
                OsStr::new(temporary_name),
                identity,
                "retention temporary state",
            );
            if result.is_ok() {
                cleanup?;
            }
        }
        result
    }

    fn write_json_exclusive(&self, name: &str, value: &impl Serialize) -> Result<()> {
        let temporary = temporary_name(name);
        self.write_json_exclusive_with_temporary(name, value, &temporary)
    }

    fn write_json_exclusive_with_temporary(
        &self,
        name: &str,
        value: &impl Serialize,
        temporary: &str,
    ) -> Result<()> {
        self.verify_identity()?;
        let bytes = serialized_bytes(
            value,
            MAX_RETENTION_LOCAL_STATE_BYTES,
            "retention audit receipt",
        )?;
        let mut created = None;
        let result = (|| {
            let mut file = self.handle.create_exclusive(OsStr::new(temporary), 0o600)?;
            let metadata = file.metadata()?;
            created = Some((metadata.dev(), metadata.ino()));
            file.write_all(&bytes)?;
            file.sync_all()?;
            self.verify_identity()?;
            // A same-directory hard link publishes the fully fsynced inode
            // atomically and refuses to replace any existing pending/audit
            // authority. This avoids a truncated final file after a crash.
            if !self.handle.link_same_inode_no_replace(
                OsStr::new(temporary),
                OsStr::new(name),
                created.expect("temporary identity recorded"),
                "retention temporary audit state",
            )? {
                return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists).into());
            }
            self.handle.sync()?;
            if self.handle.read_regular(
                OsStr::new(name),
                MAX_RETENTION_LOCAL_STATE_BYTES,
                "retention audit state",
            )? != bytes
            {
                return Err(protocol("retention audit state changed after publication"));
            }
            Ok(())
        })();
        let cleanup = if let Some(identity) = created {
            self.handle
                .unlink_if_same_inode(
                    OsStr::new(temporary),
                    identity,
                    "retention temporary audit state",
                )
                .and_then(|removed| removed.then(|| self.handle.sync()).transpose())
                .map(|_| ())
        } else {
            Ok(())
        };
        result.and(cleanup)?;
        self.verify_identity()
    }

    fn unlink_regular(&self, name: &str) -> Result<()> {
        self.verify_identity()?;
        if self
            .handle
            .open_regular_optional(OsStr::new(name), "retention state")?
            .is_none()
        {
            return Ok(());
        }
        self.handle.unlink(OsStr::new(name))?;
        self.handle.sync()?;
        self.verify_identity()
    }
}

struct RetentionLock {
    file: File,
    directory: DirectoryHandle,
    device: u64,
    inode: u64,
}

impl RetentionLock {
    fn verify(&self) -> Result<()> {
        let opened = self.file.metadata()?;
        let current = self
            .directory
            .open_regular_optional(OsStr::new(R2_RETENTION_LOCK_NAME), "retention lock")?
            .ok_or_else(|| config("retention lock disappeared while held"))?
            .metadata()?;
        if !opened.is_file()
            || !current.is_file()
            || opened.dev() != self.device
            || opened.ino() != self.inode
            || current.dev() != self.device
            || current.ino() != self.inode
        {
            return Err(config("retention lock changed while held"));
        }
        Ok(())
    }
}

fn temporary_name(name: &str) -> String {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!(
        ".{name}.tmp.{}.{}.{}",
        std::process::id(),
        time,
        TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

fn read_regular_file(
    directory: &RetentionDirectory,
    name: &str,
    maximum: usize,
    label: &str,
) -> Result<Vec<u8>> {
    directory
        .handle
        .read_regular(OsStr::new(name), maximum, label)
}

fn read_strict_json_value(
    directory: &RetentionDirectory,
    name: &str,
    maximum: usize,
    label: &str,
) -> Result<Value> {
    let bytes = read_regular_file(directory, name, maximum, label)?;
    parse_strict_json(&bytes, label)
}

#[cfg(test)]
fn read_required_json<T: DeserializeOwned>(
    directory: &RetentionDirectory,
    name: &str,
    maximum: usize,
    label: &str,
    fields: &[&str],
) -> Result<T> {
    let value = read_strict_json_value(directory, name, maximum, label)?;
    require_exact_fields(&value, fields, label)?;
    serde_json::from_value(value).map_err(|_| config(format!("{label} fields are invalid")))
}

fn read_optional_json<T: DeserializeOwned>(
    directory: &RetentionDirectory,
    name: &str,
    maximum: usize,
    label: &str,
    fields: &[&str],
) -> Result<Option<T>> {
    let Some(value) = read_optional_strict_json_value(directory, name, maximum, label)? else {
        return Ok(None);
    };
    require_exact_fields(&value, fields, label)?;
    serde_json::from_value(value)
        .map(Some)
        .map_err(|_| config(format!("{label} fields are invalid")))
}

fn read_optional_pending(
    directory: &RetentionDirectory,
    name: &str,
    maximum: usize,
    label: &str,
) -> Result<Option<Pending>> {
    let Some(value) = read_optional_strict_json_value(directory, name, maximum, label)? else {
        return Ok(None);
    };
    validate_pending_json_shape(&value, label)?;
    serde_json::from_value(value)
        .map(Some)
        .map_err(|_| config(format!("{label} fields are invalid")))
}

fn read_required_completed_audit(
    directory: &RetentionDirectory,
    name: &str,
    maximum: usize,
    label: &str,
) -> Result<CompletedAudit> {
    let value = read_strict_json_value(directory, name, maximum, label)?;
    validate_completed_audit_json_shape(&value, label)?;
    serde_json::from_value(value).map_err(|_| config(format!("{label} fields are invalid")))
}

fn read_optional_completed_audit(
    directory: &RetentionDirectory,
    name: &str,
    maximum: usize,
    label: &str,
) -> Result<Option<CompletedAudit>> {
    let Some(value) = read_optional_strict_json_value(directory, name, maximum, label)? else {
        return Ok(None);
    };
    validate_completed_audit_json_shape(&value, label)?;
    serde_json::from_value(value)
        .map(Some)
        .map_err(|_| config(format!("{label} fields are invalid")))
}

fn read_optional_strict_json_value(
    directory: &RetentionDirectory,
    name: &str,
    maximum: usize,
    label: &str,
) -> Result<Option<Value>> {
    directory
        .handle
        .read_regular_optional(OsStr::new(name), maximum, label)?
        .map(|bytes| parse_strict_json(&bytes, label))
        .transpose()
}

fn validate_pending_json_shape(value: &Value, label: &str) -> Result<()> {
    require_exact_fields(value, PENDING_FIELDS, label)?;
    let object = value.as_object().expect("validated object");
    require_exact_fields(
        object
            .get("anchor_after")
            .ok_or_else(|| config(format!("{label} anchor is missing")))?,
        ANCHOR_FIELDS,
        "pending retention anchor",
    )?;
    validate_phase_log_shape(
        object
            .get("phase_log")
            .ok_or_else(|| config(format!("{label} phase log is missing")))?,
        "pending retention phase log",
    )?;
    validate_material_json_shape(
        object
            .get("plan")
            .ok_or_else(|| config(format!("{label} plan is missing")))?,
    )
}

fn validate_completed_audit_json_shape(value: &Value, label: &str) -> Result<()> {
    require_exact_fields(value, COMPLETED_AUDIT_FIELDS, label)?;
    let object = value.as_object().expect("validated object");
    require_exact_fields(
        object
            .get("anchor_after")
            .ok_or_else(|| config(format!("{label} anchor is missing")))?,
        ANCHOR_FIELDS,
        "completed prune anchor",
    )?;
    validate_phase_log_shape(
        object
            .get("phase_log")
            .ok_or_else(|| config(format!("{label} phase log is missing")))?,
        "completed prune phase log",
    )?;
    validate_material_json_shape(
        object
            .get("plan")
            .ok_or_else(|| config(format!("{label} plan is missing")))?,
    )
}

fn validate_phase_log_shape(value: &Value, label: &str) -> Result<()> {
    let records = value
        .as_array()
        .ok_or_else(|| config(format!("{label} is invalid")))?;
    for record in records {
        require_exact_fields(record, PHASE_RECORD_FIELDS, label)?;
    }
    Ok(())
}

fn validate_material_json_shape(value: &Value) -> Result<()> {
    require_exact_fields(value, MATERIAL_FIELDS, "pending retention plan")?;
    let object = value.as_object().expect("validated object");
    let selected = object
        .get("selected_generations")
        .and_then(Value::as_array)
        .ok_or_else(|| config("pending retention selection is invalid"))?;
    for entry in selected {
        require_exact_fields(
            entry,
            PREPARED_GENERATION_FIELDS,
            "prepared retention generation",
        )?;
        let entry = entry.as_object().expect("validated object");
        require_exact_fields(
            entry
                .get("commit")
                .ok_or_else(|| config("prepared commit is missing"))?,
            CONTROL_SPEC_FIELDS,
            "prepared commit",
        )?;
        require_exact_fields(
            entry
                .get("manifest")
                .ok_or_else(|| config("prepared manifest is missing"))?,
            CONTROL_SPEC_FIELDS,
            "prepared manifest",
        )?;
        let payloads = entry
            .get("payloads")
            .and_then(Value::as_array)
            .ok_or_else(|| config("prepared payload list is invalid"))?;
        for payload in payloads {
            require_exact_fields(
                payload,
                PAYLOAD_SPEC_FIELDS,
                "prepared payload specification",
            )?;
        }
    }
    Ok(())
}

fn require_exact_fields(value: &Value, fields: &[&str], label: &str) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| config(format!("{label} must contain a JSON object")))?;
    let actual = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = fields.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(config(format!("{label} fields are invalid")));
    }
    Ok(())
}

fn serialized_bytes(value: &impl Serialize, maximum: usize, label: &str) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    let bytes = canonical_json_bytes(&value)?;
    if bytes.len() > maximum {
        return Err(config(format!("{label} is unexpectedly large")));
    }
    Ok(bytes)
}

fn parse_strict_json(bytes: &[u8], label: &str) -> Result<Value> {
    strict_json_value(bytes)
        .map_err(|error| config(format!("{label} is not valid strict UTF-8 JSON: {error}")))
}

fn config(message: impl Into<String>) -> UploaderError {
    UploaderError::Config(message.into())
}

fn protocol(message: impl Into<String>) -> UploaderError {
    UploaderError::Protocol(message.into())
}

const ANCHOR_FIELDS: &[&str] = &[
    "bucket",
    "chain_head_generation_id",
    "chain_head_manifest_sha256",
    "endpoint",
    "first_retained_generation_id",
    "first_retained_manifest_sha256",
    "first_retained_predecessor_manifest_sha256",
    "kind",
    "last_operation_id",
    "pruned_generation_count",
    "pruned_oldest_manifest_sha256",
    "pruned_newest_manifest_sha256",
    "pruned_payload_bytes",
    "remote_prefix",
    "schema_version",
    "sequence",
    "storage_provider",
    "updated_unix_secs",
];
const MATERIAL_FIELDS: &[&str] = &[
    "anchor_before_sha256",
    "bucket",
    "chain_head_generation_id",
    "chain_head_manifest_sha256",
    "delete_concurrency_precondition",
    "endpoint",
    "limited_by",
    "maximum_generation_slot",
    "minimum_age_secs",
    "minimum_retained_generations",
    "planned_unix_secs",
    "remote_prefix",
    "retained_generation_count_after",
    "retained_generation_count_before",
    "retained_payload_bytes_after",
    "retained_payload_bytes_before",
    "selected_generations",
    "selected_payload_bytes",
    "target_bytes",
    "target_satisfied",
];
const PENDING_FIELDS: &[&str] = &[
    "anchor_after",
    "kind",
    "operation_id",
    "phase",
    "phase_log",
    "plan",
    "prepared_unix_secs",
    "schema_version",
];
const COMPLETED_AUDIT_FIELDS: &[&str] = &[
    "anchor_after",
    "completed_unix_secs",
    "kind",
    "operation_id",
    "phase",
    "phase_log",
    "plan",
    "prepared_unix_secs",
    "schema_version",
];
const PHASE_RECORD_FIELDS: &[&str] = &["phase", "unix_secs"];
const PREPARED_GENERATION_FIELDS: &[&str] = &[
    "commit",
    "file_count",
    "generation_id",
    "generation_prefix",
    "manifest",
    "manifest_sha256",
    "payloads",
    "predecessor_manifest_sha256",
    "total_bytes",
    "verified_unix_secs",
];
const CONTROL_SPEC_FIELDS: &[&str] = &["etag", "key", "sha256", "size"];
const PAYLOAD_SPEC_FIELDS: &[&str] = &["etag", "key", "path", "sha256", "size"];
const REMOTE_MANIFEST_FIELDS: &[&str] = &[
    "files",
    "generation_id",
    "object_identity",
    "schema_version",
    "storage_provider",
    "total_bytes",
];
const REMOTE_MANIFEST_WITH_PREDECESSOR_FIELDS: &[&str] = &[
    "files",
    "generation_id",
    "object_identity",
    "predecessor_manifest_sha256",
    "schema_version",
    "storage_provider",
    "total_bytes",
];
const REMOTE_MANIFEST_FILE_FIELDS: &[&str] =
    &["object_key", "path", "sha256", "size", "version_id"];
const REMOTE_COMMIT_FIELDS: &[&str] = &[
    "file_count",
    "generation_id",
    "manifest_key",
    "manifest_sha256",
    "manifest_version_id",
    "object_identity",
    "schema_version",
    "storage_provider",
    "total_bytes",
];
const REMOTE_COMMIT_WITH_PREDECESSOR_FIELDS: &[&str] = &[
    "file_count",
    "generation_id",
    "manifest_key",
    "manifest_sha256",
    "manifest_version_id",
    "object_identity",
    "predecessor_manifest_sha256",
    "schema_version",
    "storage_provider",
    "total_bytes",
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Mutex;
    use tempfile::TempDir;

    #[derive(Clone, Debug)]
    struct StoredObject {
        bytes: Vec<u8>,
        etag: String,
        sha256: String,
    }

    #[derive(Default)]
    struct FakeState {
        deletes: Vec<String>,
        fail_delete_once: Option<String>,
        gets: Vec<String>,
        objects: BTreeMap<String, StoredObject>,
    }

    struct FakeStore {
        provider: Provider,
        state: Mutex<FakeState>,
    }

    impl FakeStore {
        fn new() -> Self {
            Self {
                provider: Provider::R2,
                state: Mutex::new(FakeState::default()),
            }
        }

        fn insert_bytes(&self, key: &str, bytes: Vec<u8>) -> ObjectSpec {
            let object = StoredObject {
                etag: hex::encode(Md5::digest(&bytes)),
                sha256: hex::encode(Sha256::digest(&bytes)),
                bytes,
            };
            let spec = ObjectSpec {
                etag: object.etag.clone(),
                key: key.into(),
                path: None,
                sha256: object.sha256.clone(),
                size: object.bytes.len() as u64,
            };
            self.state
                .lock()
                .expect("fake state")
                .objects
                .insert(key.into(), object);
            spec
        }

        fn replace_with_collision(&self, key: &str) {
            let bytes = b"concurrent replacement".to_vec();
            self.state.lock().expect("fake state").objects.insert(
                key.into(),
                StoredObject {
                    etag: hex::encode(Md5::digest(&bytes)),
                    sha256: hex::encode(Sha256::digest(&bytes)),
                    bytes,
                },
            );
        }

        fn object_exists(&self, key: &str) -> bool {
            self.state
                .lock()
                .expect("fake state")
                .objects
                .contains_key(key)
        }

        fn deletes(&self) -> Vec<String> {
            self.state.lock().expect("fake state").deletes.clone()
        }
    }

    impl R2Store for FakeStore {
        fn provider(&self) -> Provider {
            self.provider
        }

        fn bucket(&self) -> &str {
            "test-bucket"
        }

        fn endpoint(&self) -> &str {
            "http://127.0.0.1:9000"
        }

        fn get_control(
            &self,
            key: &str,
            expected_sha256: &str,
            expected_etag: &str,
        ) -> Result<Vec<u8>> {
            let mut state = self.state.lock().expect("fake state");
            state.gets.push(key.into());
            let object = state
                .objects
                .get(key)
                .ok_or_else(|| protocol(format!("missing control object: {key}")))?;
            if object.sha256 != expected_sha256 || object.etag != expected_etag {
                return Err(protocol(format!("immutable R2 object collision at {key}")));
            }
            Ok(object.bytes.clone())
        }

        fn exact_identity(&self, spec: &ObjectSpec) -> Result<bool> {
            let state = self.state.lock().expect("fake state");
            let Some(object) = state.objects.get(&spec.key) else {
                return Ok(false);
            };
            if object.bytes.len() as u64 != spec.size
                || object.sha256 != spec.sha256
                || object.etag != spec.etag
            {
                return Err(protocol(format!(
                    "immutable R2 object collision at {}",
                    spec.key
                )));
            }
            Ok(true)
        }

        fn delete_object(&self, key: &str) -> Result<()> {
            let mut state = self.state.lock().expect("fake state");
            state.deletes.push(key.into());
            if state.fail_delete_once.as_deref() == Some(key) {
                state.fail_delete_once = None;
                return Err(protocol("injected DELETE failure"));
            }
            state.objects.remove(key);
            Ok(())
        }
    }

    struct Fixture {
        _temporary: TempDir,
        directory: PathBuf,
        receipts: Vec<Receipt>,
        store: FakeStore,
    }

    impl Fixture {
        fn options(&self, target_bytes: u64, maximum_slot: u64, apply: bool) -> R2RetentionOptions {
            R2RetentionOptions {
                receipt_directory: self.directory.clone(),
                remote_prefix: "live-grpc-backup/v1".into(),
                target_bytes,
                minimum_age_secs: 0,
                minimum_retained_generations: 2,
                maximum_generation_slot: maximum_slot,
                apply,
                now_unix_secs: Some(10_000),
            }
        }

        fn target_for_last(&self, count: usize) -> u64 {
            checked_receipt_bytes(&self.receipts[self.receipts.len() - count..]).unwrap()
        }

        fn all_keys(&self, receipt: &Receipt) -> Vec<String> {
            let state = self.store.state.lock().expect("fake state");
            let manifest = parse_strict_json(
                &state
                    .objects
                    .get(&receipt.manifest_key)
                    .expect("manifest")
                    .bytes,
                "manifest",
            )
            .unwrap();
            let mut keys = vec![receipt.commit_key.clone(), receipt.manifest_key.clone()];
            keys.extend(
                manifest["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|file| file["object_key"].as_str().unwrap().to_string()),
            );
            keys
        }
    }

    fn fixture(count: usize, verified_times: Option<&[u64]>) -> Fixture {
        fixture_with_manifest_mutator(count, verified_times, |_, _| {})
    }

    fn fixture_with_manifest_mutator(
        count: usize,
        verified_times: Option<&[u64]>,
        mut mutate: impl FnMut(usize, &mut Value),
    ) -> Fixture {
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path().join("receipts");
        fs::create_dir(&directory).unwrap();
        let store = FakeStore::new();
        let times = verified_times
            .map(<[u64]>::to_vec)
            .unwrap_or_else(|| (0..count).map(|index| 1_000 + index as u64).collect());
        assert_eq!(times.len(), count);
        let mut predecessor: Option<String> = None;
        let mut receipts = Vec::new();
        for (index, verified_unix_secs) in times.iter().copied().enumerate().take(count) {
            let generation_id = format!("slot-{:020}", index + 1);
            let generation_prefix =
                format!("live-grpc-backup/v1/test-cluster/test-node/{generation_id}");
            let mut files = Vec::new();
            let mut total_bytes = 0u64;
            for path in ["identity.json", "raw-blocks.jsonl"] {
                let bytes = format!("generation={} path={path}\n", index + 1).into_bytes();
                let key = format!("{generation_prefix}/files/{path}");
                let mut spec = store.insert_bytes(&key, bytes);
                spec.path = Some(path.into());
                total_bytes += spec.size;
                files.push(serde_json::json!({
                    "object_key": spec.key,
                    "path": path,
                    "sha256": spec.sha256,
                    "size": spec.size,
                    "version_id": spec.etag,
                }));
            }
            let mut manifest = serde_json::json!({
                "files": files,
                "generation_id": generation_id,
                "object_identity": "single-put-etag",
                "schema_version": GENERATION_MANIFEST_SCHEMA_VERSION,
                "storage_provider": "r2",
                "total_bytes": total_bytes,
            });
            if let Some(predecessor) = &predecessor {
                manifest["predecessor_manifest_sha256"] = Value::String(predecessor.clone());
            }
            mutate(index, &mut manifest);
            let manifest_bytes = canonical_json_bytes(&manifest).unwrap();
            let manifest_spec = store.insert_bytes(
                &format!("{generation_prefix}/manifest.json"),
                manifest_bytes,
            );
            let mut commit = serde_json::json!({
                "file_count": 2,
                "generation_id": generation_id,
                "manifest_key": manifest_spec.key,
                "manifest_sha256": manifest_spec.sha256,
                "manifest_version_id": manifest_spec.etag,
                "object_identity": "single-put-etag",
                "schema_version": GENERATION_COMMIT_SCHEMA_VERSION,
                "storage_provider": "r2",
                "total_bytes": total_bytes,
            });
            if let Some(predecessor) = &predecessor {
                commit["predecessor_manifest_sha256"] = Value::String(predecessor.clone());
            }
            let commit_bytes = canonical_json_bytes(&commit).unwrap();
            let commit_spec =
                store.insert_bytes(&format!("{generation_prefix}/_COMMITTED"), commit_bytes);
            let receipt = Receipt {
                commit_etag: commit_spec.etag,
                commit_key: commit_spec.key,
                commit_sha256: commit_spec.sha256,
                file_count: 2,
                generation_id: generation_id.clone(),
                manifest_etag: manifest_spec.etag,
                manifest_key: manifest_spec.key,
                manifest_sha256: manifest_spec.sha256,
                predecessor_manifest_sha256: predecessor.clone(),
                remote_prefix: generation_prefix,
                total_bytes,
                verified_unix_secs,
            };
            write_receipt(&directory, &receipt);
            predecessor = Some(receipt.manifest_sha256.clone());
            receipts.push(receipt);
        }
        fs::write(
            directory.join(".chain"),
            format!(
                "{} {}\n",
                receipts.last().unwrap().generation_id,
                receipts.last().unwrap().manifest_sha256
            ),
        )
        .unwrap();
        Fixture {
            _temporary: temporary,
            directory,
            receipts,
            store,
        }
    }

    fn write_receipt(directory: &Path, receipt: &Receipt) {
        let mut value = serde_json::json!({
            "commit_key": receipt.commit_key,
            "commit_sha256": receipt.commit_sha256,
            "commit_version_id": receipt.commit_etag,
            "file_count": receipt.file_count,
            "generation_id": receipt.generation_id,
            "manifest_key": receipt.manifest_key,
            "manifest_sha256": receipt.manifest_sha256,
            "manifest_version_id": receipt.manifest_etag,
            "object_identity": "single-put-etag",
            "remote_prefix": receipt.remote_prefix,
            "schema_version": GENERATION_RECEIPT_SCHEMA_VERSION,
            "storage_provider": "r2",
            "total_bytes": receipt.total_bytes,
            "verified_unix_secs": receipt.verified_unix_secs,
        });
        if let Some(predecessor) = &receipt.predecessor_manifest_sha256 {
            value["predecessor_manifest_sha256"] = Value::String(predecessor.clone());
        }
        fs::write(
            directory.join(format!("{}.json", receipt.generation_id)),
            canonical_json_bytes(&value).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn dry_run_verifies_selected_generations_without_deleting_or_journaling() {
        let fixture = fixture(3, None);
        let target = fixture.target_for_last(2);
        let result =
            r2_retention_with_store(&fixture.store, &fixture.options(target, 999, false)).unwrap();
        assert_eq!(result["mode"], "dry-run");
        assert_eq!(
            result["selected_generation_ids"],
            serde_json::json!([fixture.receipts[0].generation_id])
        );
        assert_eq!(result["retained_payload_bytes_after"], target);
        assert!(fixture.store.deletes().is_empty());
        let state = fixture.store.state.lock().unwrap();
        assert_eq!(
            state.gets.iter().collect::<BTreeSet<_>>(),
            [
                &fixture.receipts[0].manifest_key,
                &fixture.receipts[0].commit_key
            ]
            .into_iter()
            .collect()
        );
        assert!(!fixture.directory.join(R2_RETENTION_PENDING_NAME).exists());
        assert!(!fixture.directory.join(R2_RETENTION_ANCHOR_NAME).exists());
    }

    #[test]
    fn apply_deletes_whole_generations_fifo_and_persists_anchor_and_audit() {
        let fixture = fixture(4, None);
        let target = fixture.target_for_last(2);
        let expected = fixture.receipts[..2]
            .iter()
            .flat_map(|receipt| fixture.all_keys(receipt))
            .collect::<Vec<_>>();
        let retained = fixture.receipts[2..]
            .iter()
            .flat_map(|receipt| fixture.all_keys(receipt))
            .collect::<Vec<_>>();
        let result =
            r2_retention_with_store(&fixture.store, &fixture.options(target, 999, true)).unwrap();
        assert_eq!(fixture.store.deletes(), expected);
        assert_eq!(result["delete_request_count"], expected.len());
        assert_eq!(
            result["delete_concurrency_precondition"],
            R2_DELETE_CONCURRENCY_PRECONDITION
        );
        assert!(expected.iter().all(|key| !fixture.store.object_exists(key)));
        assert!(retained.iter().all(|key| fixture.store.object_exists(key)));
        let directory = RetentionDirectory::open(&fixture.directory).unwrap();
        let anchor = read_required_json::<Anchor>(
            &directory,
            R2_RETENTION_ANCHOR_NAME,
            MAX_RETENTION_LOCAL_STATE_BYTES,
            "anchor",
            ANCHOR_FIELDS,
        )
        .unwrap();
        assert_eq!(
            anchor.first_retained_manifest_sha256,
            fixture.receipts[2].manifest_sha256
        );
        assert_eq!(anchor.pruned_generation_count, 2);
        assert_eq!(
            anchor.first_retained_predecessor_manifest_sha256.as_deref(),
            Some(fixture.receipts[1].manifest_sha256.as_str())
        );
        assert!(!fixture.directory.join(R2_RETENTION_PENDING_NAME).exists());
        let audit_count = fs::read_dir(&fixture.directory)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".r2-prune-")
            })
            .count();
        assert_eq!(audit_count, 1);

        let before = fixture.store.deletes().len();
        let repeated =
            r2_retention_with_store(&fixture.store, &fixture.options(target, 999, false)).unwrap();
        assert_eq!(repeated["selected_generation_ids"], serde_json::json!([]));
        assert_eq!(fixture.store.deletes().len(), before);
    }

    #[test]
    fn cutoff_age_and_minimum_count_bound_the_oldest_contiguous_tail() {
        let fixture = fixture(5, Some(&[100, 200, 950, 960, 970]));
        let mut options = fixture.options(0, 999, false);
        options.minimum_age_secs = 500;
        options.now_unix_secs = Some(1_000);
        let age = r2_retention_with_store(&fixture.store, &options).unwrap();
        assert_eq!(
            age["selected_generation_ids"],
            serde_json::json!([
                fixture.receipts[0].generation_id,
                fixture.receipts[1].generation_id
            ])
        );
        assert_eq!(age["limited_by"], "minimum_age");
        assert_eq!(age["retained_generation_count_after"], 3);

        let mut options = fixture.options(0, 999, false);
        options.minimum_retained_generations = 4;
        let count = r2_retention_with_store(&fixture.store, &options).unwrap();
        assert_eq!(
            count["selected_generation_ids"],
            serde_json::json!([fixture.receipts[0].generation_id])
        );
        assert_eq!(count["limited_by"], "minimum_retained_generations");

        let blocked =
            r2_retention_with_store(&fixture.store, &fixture.options(0, 0, true)).unwrap();
        assert_eq!(blocked["selected_generation_ids"], serde_json::json!([]));
        assert_eq!(blocked["limited_by"], "blockzilla_sync");
        assert!(fixture.store.deletes().is_empty());
    }

    #[test]
    fn blockzilla_cutoff_allows_only_confirmed_fifo_generations() {
        let fixture = fixture(5, None);
        let result = r2_retention_with_store(&fixture.store, &fixture.options(0, 2, true)).unwrap();
        assert_eq!(
            result["selected_generation_ids"],
            serde_json::json!([
                fixture.receipts[0].generation_id,
                fixture.receipts[1].generation_id
            ])
        );
        assert_eq!(result["limited_by"], "blockzilla_sync");
        for receipt in &fixture.receipts[..2] {
            assert!(!fixture.store.object_exists(&receipt.commit_key));
        }
        for receipt in &fixture.receipts[2..] {
            assert!(fixture.store.object_exists(&receipt.commit_key));
        }
    }

    #[test]
    fn remote_manifest_key_escape_and_identity_collision_fail_before_delete() {
        let escaped = fixture_with_manifest_mutator(3, None, |index, manifest| {
            if index == 0 {
                manifest["files"][0]["object_key"] =
                    Value::String("live-grpc-backup/v1/another-generation/files/victim".into());
            }
        });
        let target = escaped.target_for_last(2);
        let error = r2_retention_with_store(&escaped.store, &escaped.options(target, 999, true))
            .unwrap_err();
        assert!(error.to_string().contains("outside its generation prefix"));
        assert!(escaped.store.deletes().is_empty());
        assert!(!escaped.directory.join(R2_RETENTION_PENDING_NAME).exists());

        let collision = fixture(3, None);
        let payload_key = collision.all_keys(&collision.receipts[0])[2].clone();
        collision.store.replace_with_collision(&payload_key);
        let target = collision.target_for_last(2);
        let error =
            r2_retention_with_store(&collision.store, &collision.options(target, 999, true))
                .unwrap_err();
        assert!(error.to_string().contains("immutable R2 object collision"));
        assert!(collision.store.deletes().is_empty());
        assert!(collision.store.object_exists(&payload_key));
    }

    #[test]
    fn interrupted_delete_recovers_idempotently_and_rejects_parameter_drift() {
        let fixture = fixture(4, None);
        let target = fixture.target_for_last(2);
        let failing_key = fixture.receipts[0].manifest_key.clone();
        fixture.store.state.lock().unwrap().fail_delete_once = Some(failing_key.clone());
        let error =
            r2_retention_with_store(&fixture.store, &fixture.options(target, 2, true)).unwrap_err();
        assert!(error.to_string().contains("injected DELETE failure"));
        assert!(fixture.directory.join(R2_RETENTION_PENDING_NAME).is_file());
        assert!(!fixture.directory.join(R2_RETENTION_ANCHOR_NAME).exists());

        let deletes_before = fixture.store.deletes().len();
        let mismatch =
            r2_retention_with_store(&fixture.store, &fixture.options(target, 3, true)).unwrap_err();
        assert!(
            mismatch
                .to_string()
                .contains("does not match this invocation")
        );
        assert_eq!(fixture.store.deletes().len(), deletes_before);

        let result =
            r2_retention_with_store(&fixture.store, &fixture.options(target, 2, true)).unwrap();
        let deletes = fixture.store.deletes();
        assert_eq!(
            deletes
                .iter()
                .filter(|key| **key == fixture.receipts[0].commit_key)
                .count(),
            1
        );
        assert_eq!(deletes.iter().filter(|key| **key == failing_key).count(), 2);
        assert!(result["already_absent_object_count"].as_u64().unwrap() >= 1);
        assert!(!fixture.directory.join(R2_RETENTION_PENDING_NAME).exists());
        assert!(fixture.directory.join(R2_RETENTION_ANCHOR_NAME).is_file());
    }

    #[test]
    fn complete_apply_rechecks_fresh_anchor_digest_before_any_delete() {
        let fixture = fixture(4, None);
        let target = fixture.target_for_last(2);
        fixture.store.state.lock().unwrap().fail_delete_once =
            Some(fixture.receipts[0].commit_key.clone());
        r2_retention_with_store(&fixture.store, &fixture.options(target, 2, true)).unwrap_err();
        let directory = RetentionDirectory::open(&fixture.directory).unwrap();
        let pending = read_optional_pending(
            &directory,
            R2_RETENTION_PENDING_NAME,
            MAX_RETENTION_LOCAL_STATE_BYTES,
            "pending",
        )
        .unwrap()
        .unwrap();
        let mut unrelated = pending.anchor_after.clone();
        unrelated.sequence += 1;
        unrelated.last_operation_id = "f".repeat(64);
        fs::write(
            fixture.directory.join(R2_RETENTION_ANCHOR_NAME),
            serialized_bytes(&unrelated, MAX_RETENTION_LOCAL_STATE_BYTES, "anchor").unwrap(),
        )
        .unwrap();
        let lock = directory.lock().unwrap();
        let before = fixture.store.deletes().len();
        let error = complete_apply(&fixture.store, &directory, &lock, pending).unwrap_err();
        assert!(error.to_string().contains("anchor changed"));
        assert_eq!(fixture.store.deletes().len(), before);
    }

    #[test]
    fn exclusive_state_publication_is_atomic_no_replace_and_private() {
        let fixture = fixture(3, None);
        let directory = RetentionDirectory::open(&fixture.directory).unwrap();
        let _lock = directory.lock().unwrap();
        let value = serde_json::json!({"complete": true, "sequence": 1});
        directory
            .write_json_exclusive(".exclusive.json", &value)
            .unwrap();
        let path = fixture.directory.join(".exclusive.json");
        assert_eq!(
            fs::read(&path).unwrap(),
            canonical_json_bytes(&value).unwrap()
        );
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
        let replacement = serde_json::json!({"complete": false});
        let error = directory
            .write_json_exclusive(".exclusive.json", &replacement)
            .unwrap_err();
        assert!(matches!(
            error,
            UploaderError::Io(ref source)
                if source.kind() == std::io::ErrorKind::AlreadyExists
        ));
        assert_eq!(
            fs::read(&path).unwrap(),
            canonical_json_bytes(&value).unwrap()
        );
        assert!(
            fs::read_dir(&fixture.directory)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp."))
        );
    }

    #[test]
    fn preexisting_temporary_collision_is_never_deleted_or_published() {
        let fixture = fixture(3, None);
        let directory = RetentionDirectory::open(&fixture.directory).unwrap();
        let _lock = directory.lock().unwrap();
        let temporary = ".preexisting.tmp";
        let evidence = b"custody evidence";
        fs::write(fixture.directory.join(temporary), evidence).unwrap();

        assert!(
            directory
                .write_json_exclusive_with_temporary(
                    ".exclusive-collision.json",
                    &serde_json::json!({"new": true}),
                    temporary,
                )
                .is_err()
        );
        assert_eq!(
            fs::read(fixture.directory.join(temporary)).unwrap(),
            evidence
        );
        assert!(!fixture.directory.join(".exclusive-collision.json").exists());

        let state_name = ".atomic-state.json";
        let old = canonical_json_bytes(&serde_json::json!({"old": true})).unwrap();
        fs::write(fixture.directory.join(state_name), &old).unwrap();
        assert!(
            directory
                .write_json_atomic_with_temporary(
                    state_name,
                    Some(&old),
                    &serde_json::json!({"new": true}),
                    temporary,
                )
                .is_err()
        );
        assert_eq!(
            fs::read(fixture.directory.join(temporary)).unwrap(),
            evidence
        );
        assert_eq!(fs::read(fixture.directory.join(state_name)).unwrap(), old);
    }

    #[test]
    fn crash_after_anchor_before_audit_recreates_audit_before_clearing_pending() {
        let fixture = fixture(4, None);
        let target = fixture.target_for_last(2);
        r2_retention_with_store(&fixture.store, &fixture.options(target, 2, true)).unwrap();
        let directory = RetentionDirectory::open(&fixture.directory).unwrap();
        let anchor = read_required_json::<Anchor>(
            &directory,
            R2_RETENTION_ANCHOR_NAME,
            MAX_RETENTION_LOCAL_STATE_BYTES,
            "anchor",
            ANCHOR_FIELDS,
        )
        .unwrap();
        let audit_name = audit_filename(anchor.sequence, &anchor.last_operation_id);
        let audit = read_required_completed_audit(
            &directory,
            &audit_name,
            MAX_RETENTION_LOCAL_STATE_BYTES,
            "audit",
        )
        .unwrap();
        fs::remove_file(fixture.directory.join(&audit_name)).unwrap();
        let mut phase_log = audit.phase_log;
        assert_eq!(phase_log.pop().unwrap().phase, "completed");
        let pending = Pending {
            anchor_after: anchor,
            kind: "r2-retention-pending".into(),
            operation_id: audit.operation_id,
            phase: "anchor_written".into(),
            phase_log,
            plan: audit.plan,
            prepared_unix_secs: audit.prepared_unix_secs,
            schema_version: R2_RETENTION_SCHEMA_VERSION,
        };
        fs::write(
            fixture.directory.join(R2_RETENTION_PENDING_NAME),
            serialized_bytes(&pending, MAX_RETENTION_LOCAL_STATE_BYTES, "pending").unwrap(),
        )
        .unwrap();
        let before = fixture.store.deletes().len();
        let recovered =
            r2_retention_with_store(&fixture.store, &fixture.options(target, 2, true)).unwrap();
        assert_eq!(recovered["delete_request_count"], 0);
        assert!(recovered["already_absent_object_count"].as_u64().unwrap() > 0);
        assert_eq!(fixture.store.deletes().len(), before);
        assert!(fixture.directory.join(audit_name).is_file());
        assert!(!fixture.directory.join(R2_RETENTION_PENDING_NAME).exists());
    }

    #[test]
    fn strict_json_rejects_duplicate_nested_keys_floats_and_trailing_data() {
        for input in [
            br#"{"a":{"b":1,"b":2}}"#.as_slice(),
            br#"{"a":1.0}"#.as_slice(),
            br#"{"a":1} {"b":2}"#.as_slice(),
            br#"{"a":18446744073709551616}"#.as_slice(),
        ] {
            assert!(parse_strict_json(input, "control document").is_err());
        }
        assert_eq!(
            parse_strict_json(br#"{"a":[1,true,null,"x"]}"#, "control").unwrap(),
            serde_json::json!({"a": [1, true, null, "x"]})
        );
    }

    #[test]
    fn nested_pending_field_omission_and_digest_tampering_fail_closed() {
        let fixture = fixture(4, None);
        let target = fixture.target_for_last(2);
        fixture.store.state.lock().unwrap().fail_delete_once =
            Some(fixture.receipts[0].manifest_key.clone());
        r2_retention_with_store(&fixture.store, &fixture.options(target, 2, true)).unwrap_err();
        let pending_path = fixture.directory.join(R2_RETENTION_PENDING_NAME);
        let directory = RetentionDirectory::open(&fixture.directory).unwrap();
        let mut pending = read_strict_json_value(
            &directory,
            R2_RETENTION_PENDING_NAME,
            MAX_RETENTION_LOCAL_STATE_BYTES,
            "pending",
        )
        .unwrap();
        pending["plan"]["selected_generations"][0]
            .as_object_mut()
            .unwrap()
            .remove("predecessor_manifest_sha256");
        fs::write(&pending_path, canonical_json_bytes(&pending).unwrap()).unwrap();
        let before = fixture.store.deletes().len();
        let error =
            r2_retention_with_store(&fixture.store, &fixture.options(target, 2, true)).unwrap_err();
        assert!(error.to_string().contains("prepared retention generation"));
        assert_eq!(fixture.store.deletes().len(), before);
    }

    #[test]
    fn incomplete_cyclic_and_duplicate_receipt_chains_fail_before_remote_access() {
        for mode in ["incomplete", "cyclic", "duplicate"] {
            let mut fixture = fixture(3, None);
            match mode {
                "incomplete" => {
                    fixture.receipts[0].predecessor_manifest_sha256 = Some("a".repeat(64));
                    write_receipt(&fixture.directory, &fixture.receipts[0]);
                }
                "cyclic" => {
                    fixture.receipts[0].predecessor_manifest_sha256 =
                        Some(fixture.receipts[2].manifest_sha256.clone());
                    write_receipt(&fixture.directory, &fixture.receipts[0]);
                }
                "duplicate" => {
                    fixture.receipts[0].manifest_sha256 =
                        fixture.receipts[1].manifest_sha256.clone();
                    write_receipt(&fixture.directory, &fixture.receipts[0]);
                }
                _ => unreachable!(),
            }
            let error = r2_retention_with_store(&fixture.store, &fixture.options(0, 999, false))
                .unwrap_err();
            assert!(
                error.to_string().contains("receipt chain"),
                "mode={mode} error={error}"
            );
            assert!(fixture.store.state.lock().unwrap().gets.is_empty());
            assert!(fixture.store.deletes().is_empty());
        }
    }

    #[test]
    fn missing_or_tampered_completed_audit_blocks_new_plans() {
        let fixture = fixture(4, None);
        let target = fixture.target_for_last(2);
        r2_retention_with_store(&fixture.store, &fixture.options(target, 2, true)).unwrap();
        let directory = RetentionDirectory::open(&fixture.directory).unwrap();
        let anchor = read_required_json::<Anchor>(
            &directory,
            R2_RETENTION_ANCHOR_NAME,
            MAX_RETENTION_LOCAL_STATE_BYTES,
            "anchor",
            ANCHOR_FIELDS,
        )
        .unwrap();
        let audit_path = fixture
            .directory
            .join(audit_filename(anchor.sequence, &anchor.last_operation_id));
        let original = fs::read(&audit_path).unwrap();
        fs::remove_file(&audit_path).unwrap();
        let before = fixture.store.deletes().len();
        let missing = r2_retention_with_store(&fixture.store, &fixture.options(target, 2, false))
            .unwrap_err();
        assert!(missing.to_string().contains("audit") || missing.to_string().contains("receipt"));
        assert_eq!(fixture.store.deletes().len(), before);

        fs::write(&audit_path, original).unwrap();
        let audit_name = audit_path.file_name().unwrap().to_str().unwrap();
        let mut audit = read_strict_json_value(
            &directory,
            audit_name,
            MAX_RETENTION_LOCAL_STATE_BYTES,
            "audit",
        )
        .unwrap();
        audit["plan"]["target_bytes"] = serde_json::json!(target + 1);
        fs::write(&audit_path, canonical_json_bytes(&audit).unwrap()).unwrap();
        let tampered = r2_retention_with_store(&fixture.store, &fixture.options(target, 2, false))
            .unwrap_err();
        assert!(tampered.to_string().contains("operation digest"));
        assert_eq!(fixture.store.deletes().len(), before);
    }

    #[test]
    fn supported_integer_bounds_fail_closed_before_remote_access() {
        let fixture = fixture(3, None);
        for mut options in [
            fixture.options(MAX_SUPPORTED_INTEGER + 1, 999, false),
            {
                let mut options = fixture.options(0, 999, false);
                options.minimum_age_secs = MAX_SUPPORTED_INTEGER + 1;
                options
            },
            fixture.options(0, MAX_SUPPORTED_INTEGER + 1, false),
        ] {
            options.now_unix_secs = Some(10_000);
            assert!(r2_retention_with_store(&fixture.store, &options).is_err());
        }
        assert!(fixture.store.state.lock().unwrap().gets.is_empty());
        assert!(fixture.store.deletes().is_empty());
    }

    #[test]
    fn symlinked_directory_receipt_and_lock_are_rejected_before_remote_access() {
        use std::os::unix::fs::symlink;

        let directory_link = fixture(3, None);
        let link = directory_link._temporary.path().join("receipt-link");
        symlink(&directory_link.directory, &link).unwrap();
        let mut options = directory_link.options(0, 999, false);
        options.receipt_directory = link;
        assert!(
            r2_retention_with_store(&directory_link.store, &options)
                .unwrap_err()
                .to_string()
                .contains("not a symlink")
        );
        assert!(directory_link.store.state.lock().unwrap().gets.is_empty());

        let receipt_link = fixture(3, None);
        let receipt_path = receipt_link
            .directory
            .join(format!("{}.json", receipt_link.receipts[0].generation_id));
        fs::remove_file(&receipt_path).unwrap();
        symlink(".chain", &receipt_path).unwrap();
        let error =
            r2_retention_with_store(&receipt_link.store, &receipt_link.options(0, 999, false))
                .unwrap_err();
        assert!(error.to_string().contains("generation receipt"), "{error}");
        assert!(receipt_link.store.state.lock().unwrap().gets.is_empty());

        let lock_link = fixture(3, None);
        symlink(".chain", lock_link.directory.join(R2_RETENTION_LOCK_NAME)).unwrap();
        assert!(
            r2_retention_with_store(&lock_link.store, &lock_link.options(0, 999, false))
                .unwrap_err()
                .to_string()
                .contains("cannot open retention lock safely")
        );
        assert!(lock_link.store.state.lock().unwrap().gets.is_empty());
    }

    #[test]
    fn wrong_provider_and_unsafe_prefix_fail_before_remote_access() {
        let mut fixture = fixture(3, None);
        fixture.store.provider = Provider::S3;
        assert!(
            r2_retention_with_store(&fixture.store, &fixture.options(0, 999, false))
                .unwrap_err()
                .to_string()
                .contains("provider=r2")
        );
        fixture.store.provider = Provider::R2;
        let mut options = fixture.options(0, 999, false);
        options.remote_prefix = "single-component".into();
        assert!(
            r2_retention_with_store(&fixture.store, &options)
                .unwrap_err()
                .to_string()
                .contains("at least two")
        );
        assert!(fixture.store.state.lock().unwrap().gets.is_empty());
    }
}
