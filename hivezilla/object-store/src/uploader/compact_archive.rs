//! Immutable execution-canary publication. This is not canonical catalog authority.
//!
//! A trusted Compact V2 verifier must produce the COMPLETE source marker. This
//! module checks its contract and every file digest; it does not infer finality,
//! verify validator execution, or turn raw format-3 records into an archive.

use super::dirfd::DirectoryHandle;
use super::{
    FilePayload, Payload, Provider, Result, S3Client, S3Response, UploaderError,
    canonical_json_bytes, strict_json_value,
};
use base64::Engine;
use fs2::FileExt;
use md5::Md5;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub const COMPACT_ARCHIVE_MARKER: &str = "LIVE-EXECUTION-CANARY.json";
pub const COMPACT_ARCHIVE_MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_CONTROL_BYTES: usize = 1024 * 1024;
const MAX_FILES: usize = 64;
const SOURCE_PROFILE: &str = "validator-finalized-execution-v1";
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Deserialize)]
struct Marker {
    schema: String,
    source_profile: String,
    state: String,
    canonical: bool,
    publication_ready: bool,
    finalized: bool,
    archive_verified: bool,
    source_receipt_sha256: String,
    files: Vec<SourceFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceFile {
    path: String,
    bytes: u64,
    sha256: String,
}

struct FileSpec {
    source: FilePayload,
    key: String,
    sha256: String,
    md5: String,
}

/// Publish a verified, sealed execution canary using bounded single-file PUTs.
///
/// `state_dir` must already exist, be private-owner controlled, and be outside
/// `archive_dir`. One state directory belongs to one exact publication and R2
/// namespace. No credentials or endpoint text are written into its records.
pub fn upload_compact_archive(
    client: &S3Client,
    archive_dir: &Path,
    remote_prefix: &str,
    state_dir: &Path,
) -> Result<Value> {
    if client.provider != Provider::R2 {
        return Err(config(
            "compact execution-canary publication requires provider=r2",
        ));
    }
    validate_prefix(remote_prefix)?;
    let source_dir = DirectoryHandle::open_existing(archive_dir, "sealed archive directory")?;
    source_dir.require_private_owner("sealed archive directory")?;
    let state = DirectoryHandle::open_existing(state_dir, "archive publication state")?;
    state.require_private_owner("archive publication state")?;
    if state.contains_ancestor(source_dir.identity())? {
        return Err(config(
            "publication state must be outside the sealed archive directory",
        ));
    }
    let lock = state.open_lock(
        OsStr::new("publisher.lock"),
        0o600,
        "archive publication lock",
    )?;
    lock.try_lock_exclusive()
        .map_err(|_| protocol("archive publication state is busy"))?;
    let lock_meta = lock.metadata()?;
    let lock_identity = (lock_meta.dev(), lock_meta.ino());
    state.sync()?;

    let marker_bytes = source_dir.read_regular(
        OsStr::new(COMPACT_ARCHIVE_MARKER),
        MAX_CONTROL_BYTES,
        "sealed archive marker",
    )?;
    let marker_value = strict_json_value(&marker_bytes)?;
    let marker: Marker = serde_json::from_value(marker_value.clone())?;
    validate_marker(&marker)?;
    let marker_sha256 = hex::encode(Sha256::digest(&marker_bytes));
    let object_prefix = format!("{remote_prefix}/{marker_sha256}");
    let mut specs = Vec::with_capacity(marker.files.len());
    let mut objects = Vec::with_capacity(marker.files.len());
    let mut total_bytes = 0u64;
    // Hash all input before any remote request. The held files remain pinned
    // until after commit; stream reads use pread rather than a shared seek offset.
    for record in &marker.files {
        source_dir.verify_path_binding("sealed archive directory")?;
        let source = FilePayload::open(&archive_dir.join(&record.path))?;
        if source.len() != record.bytes {
            return Err(protocol("sealed archive file length differs from marker"));
        }
        let (sha256, md5) = hash_file(&source)?;
        if sha256 != record.sha256 {
            return Err(protocol("sealed archive file SHA-256 differs from marker"));
        }
        let key = format!("{object_prefix}/files/{}", record.path);
        total_bytes = total_bytes
            .checked_add(record.bytes)
            .ok_or_else(|| config("archive byte count overflow"))?;
        objects.push(json!({"path": record.path, "bytes": record.bytes,
            "sha256": record.sha256, "key": key}));
        specs.push(FileSpec {
            source,
            key,
            sha256,
            md5,
        });
    }
    verify_source(&source_dir, &specs, &marker_bytes)?;
    let manifest = json!({
        "schema": "blockzilla-r2-execution-canary-manifest-v1",
        "source_profile": SOURCE_PROFILE,
        "canonical": false,
        "publication_ready": false,
        "source_marker_sha256": marker_sha256,
        "source_marker": marker_value,
        "files": objects,
        "total_bytes": total_bytes,
    });
    let manifest_bytes = canonical_json_bytes(&manifest)?;
    if manifest_bytes.len() > MAX_CONTROL_BYTES * 2 {
        return Err(config("archive publication manifest exceeds control limit"));
    }
    let manifest_sha256 = hex::encode(Sha256::digest(&manifest_bytes));
    let manifest_key = format!("{object_prefix}/EXECUTION-CANARY-MANIFEST.json");
    let namespace_sha256 = namespace_sha256(client, remote_prefix);
    let intent = json!({
        "schema": "blockzilla-r2-execution-canary-intent-v1",
        "namespace_sha256": namespace_sha256,
        "source_marker_sha256": marker_sha256,
        "manifest_key": manifest_key,
        "manifest_sha256": manifest_sha256,
        "file_count": specs.len(),
        "total_bytes": total_bytes,
    });
    persist_exact(&state, "intent.json", &canonical_json_bytes(&intent)?)?;
    // Every retry verifies all remote bytes, including an existing commit.
    // A local report is never a substitute for remote verification.
    for spec in &specs {
        ensure_object(
            client,
            &spec.key,
            spec.source.len(),
            &spec.sha256,
            &spec.md5,
            &Payload::File(spec.source.clone()),
        )?;
    }
    verify_source(&source_dir, &specs, &marker_bytes)?;
    state.require_same_inode(
        OsStr::new("publisher.lock"),
        lock_identity,
        "publication lock",
    )?;
    state.verify_path_binding("archive publication state")?;
    // This is the last remote write. It is an execution-canary commit only;
    // no READY file, canonical manifest, ACK, or retention record is produced.
    let manifest_md5 =
        base64::engine::general_purpose::STANDARD.encode(Md5::digest(&manifest_bytes));
    ensure_object(
        client,
        &manifest_key,
        manifest_bytes.len() as u64,
        &manifest_sha256,
        &manifest_md5,
        &Payload::Bytes(manifest_bytes),
    )?;
    verify_source(&source_dir, &specs, &marker_bytes)?;
    let report = json!({
        "schema": "blockzilla-r2-execution-canary-report-v1",
        "state": "COMPLETE",
        "source_profile": SOURCE_PROFILE,
        "canonical": false,
        "publication_ready": false,
        "namespace_sha256": namespace_sha256,
        "source_marker_sha256": marker_sha256,
        "manifest_key": manifest_key,
        "manifest_sha256": manifest_sha256,
        "file_count": specs.len(),
        "total_bytes": total_bytes,
        "verification": "full-sha256-readback",
        "source_retirement_authorized": false,
    });
    persist_exact(&state, "complete.json", &canonical_json_bytes(&report)?)?;
    Ok(report)
}

fn validate_marker(marker: &Marker) -> Result<()> {
    if marker.schema != "blockzilla-live-execution-canary-v1"
        || marker.source_profile != SOURCE_PROFILE
        || marker.state != "COMPLETE"
        || marker.canonical
        || marker.publication_ready
        || !marker.finalized
        || !marker.archive_verified
    {
        return Err(config(
            "input requires a verified COMPLETE finalized execution-canary marker; canonical publication is not supported",
        ));
    }
    validate_digest(&marker.source_receipt_sha256)?;
    if marker.files.is_empty() || marker.files.len() > MAX_FILES {
        return Err(config("sealed archive must list 1..=64 files"));
    }
    let mut names = BTreeSet::new();
    for file in &marker.files {
        validate_component(&file.path)?;
        if file.path == COMPACT_ARCHIVE_MARKER || !names.insert(&file.path) {
            return Err(config("duplicate or reserved sealed archive filename"));
        }
        if file.bytes > COMPACT_ARCHIVE_MAX_FILE_BYTES {
            return Err(config(
                "sealed archive file exceeds the 512 MiB single-PUT limit; multipart is not implemented",
            ));
        }
        validate_digest(&file.sha256)?;
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<()> {
    let Some(tail) = prefix.strip_prefix("execution-canary/") else {
        return Err(config("remote prefix must start with execution-canary/"));
    };
    if prefix.len() > 400 {
        return Err(config("execution-canary prefix is too long"));
    }
    for component in tail.split('/') {
        validate_component(component)?;
    }
    Ok(())
}

fn validate_component(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 160
        || value.starts_with('.')
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return Err(config("archive paths must be safe flat ASCII names"));
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(config("archive digest must be lowercase SHA-256 hex"));
    }
    Ok(())
}

fn namespace_sha256(client: &S3Client, prefix: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"blockzilla-r2-execution-canary-namespace-v1\0");
    for field in [client.endpoint(), client.bucket.as_str(), prefix] {
        hash.update((field.len() as u64).to_le_bytes());
        hash.update(field.as_bytes());
    }
    hex::encode(hash.finalize())
}

fn hash_file(source: &FilePayload) -> Result<(String, String)> {
    source.verify("before archive hashing")?;
    let mut reader = source.reader();
    let mut sha256 = Sha256::new();
    let mut md5 = Md5::new();
    let mut buffer = [0u8; 128 * 1024];
    let mut count = 0u64;
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        count += n as u64;
        sha256.update(&buffer[..n]);
        md5.update(&buffer[..n]);
    }
    source.verify("after archive hashing")?;
    if count != source.len() {
        return Err(protocol("sealed archive file is truncated"));
    }
    Ok((
        hex::encode(sha256.finalize()),
        base64::engine::general_purpose::STANDARD.encode(md5.finalize()),
    ))
}

fn verify_source(directory: &DirectoryHandle, specs: &[FileSpec], marker: &[u8]) -> Result<()> {
    directory.verify_path_binding("sealed archive directory")?;
    let current = directory.read_regular(
        OsStr::new(COMPACT_ARCHIVE_MARKER),
        MAX_CONTROL_BYTES,
        "sealed archive marker",
    )?;
    if current != marker {
        return Err(protocol("sealed archive marker changed"));
    }
    for spec in specs {
        spec.source.verify("at archive publication boundary")?;
    }
    Ok(())
}

fn ensure_object(
    client: &S3Client,
    key: &str,
    len: u64,
    sha256: &str,
    md5: &str,
    payload: &Payload,
) -> Result<()> {
    let head = client.head(key).map_err(remote_error)?;
    if head.status == 404 {
        let response = client
            .put(
                key,
                "application/octet-stream",
                sha256,
                Some(md5),
                true,
                payload,
            )
            .map_err(remote_error)?;
        if !matches!(response.status, 200 | 412) {
            return Err(protocol("unexpected conditional PUT status"));
        }
    } else {
        validate_remote_metadata(&head, len)?;
    }
    let response = client.get(key).map_err(remote_error)?;
    validate_remote_metadata(&response, len)?;
    let mut reader = response.into_reader().take(len + 1);
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    let mut read = 0u64;
    loop {
        let n = reader
            .read(&mut buffer)
            .map_err(|_| protocol("archive remote read-back failed"))?;
        if n == 0 {
            break;
        }
        read += n as u64;
        hash.update(&buffer[..n]);
    }
    if read != len || hex::encode(hash.finalize()) != sha256 {
        return Err(protocol(
            "immutable archive object collision or corrupt read-back",
        ));
    }
    Ok(())
}

fn validate_remote_metadata(response: &S3Response, len: u64) -> Result<()> {
    if response.status != 200
        || response.headers.contains_key("content-range")
        || response.exact_content_length("archive object")? != len
    {
        return Err(protocol("archive remote object length/status differs"));
    }
    if response.headers.contains_key("content-encoding")
        && response.exact_header("content-encoding", "archive object")? != "identity"
    {
        return Err(protocol("archive remote content encoding is not identity"));
    }
    Ok(())
}

fn remote_error(error: UploaderError) -> UploaderError {
    match error {
        UploaderError::Api(error) => protocol(&format!(
            "archive object-store request failed HTTP {}",
            error.status
        )),
        _ => protocol("archive object-store request failed"),
    }
}

fn persist_exact(directory: &DirectoryHandle, name: &str, bytes: &[u8]) -> Result<()> {
    directory.verify_path_binding("archive publication state")?;
    if let Some(existing) = directory.read_regular_optional(
        OsStr::new(name),
        MAX_CONTROL_BYTES,
        "archive publication record",
    )? {
        if existing != bytes {
            return Err(protocol(
                "archive publication state belongs to different inputs or namespace",
            ));
        }
        // Also sync a record recovered after link but before directory fsync.
        directory.sync()?;
        return Ok(());
    }
    // A restarted process can reuse a PID. Never remove an old temporary by
    // name or let one old crash file prevent recovery of an intact intent.
    let mut created = None;
    for _ in 0..32 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = format!(".archive-{}-{sequence}.tmp", std::process::id());
        match directory.create_exclusive(OsStr::new(&temporary), 0o600) {
            Ok(file) => {
                created = Some((temporary, file));
                break;
            }
            Err(UploaderError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    let (temporary, mut file) =
        created.ok_or_else(|| protocol("too many existing archive publication temporary names"))?;
    let metadata = file.metadata()?;
    let identity = (metadata.dev(), metadata.ino());
    file.write_all(bytes)?;
    file.sync_all()?;
    if !directory.link_same_inode_no_replace(
        OsStr::new(&temporary),
        OsStr::new(name),
        identity,
        "archive publication record",
    )? {
        let existing = directory.read_regular(
            OsStr::new(name),
            MAX_CONTROL_BYTES,
            "archive publication record",
        )?;
        if existing != bytes {
            return Err(protocol("archive publication record collision"));
        }
    }
    directory.sync()?;
    directory.unlink_if_same_inode(
        OsStr::new(&temporary),
        identity,
        "archive publication temporary",
    )?;
    directory.sync()?;
    Ok(())
}

fn config(message: &str) -> UploaderError {
    UploaderError::Config(message.into())
}
fn protocol(message: &str) -> UploaderError {
    UploaderError::Protocol(message.into())
}
