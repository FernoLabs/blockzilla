use super::dirfd::DirectoryHandle;
use super::s3::FilePayload;
use super::{
    B2NativeObjectVerifier, NativeSnapshot, Payload, Provider, Result, S3Client, UploaderError,
    canonical_json_bytes, strict_json_value,
};
use base64::Engine;
use fs2::FileExt;
use md5::Md5;
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const SINGLE_PUT_LIMIT: u64 = 512 * 1024 * 1024;
const MAX_GENERATION_RECEIPT_BYTES: usize = 16 * 1024 * 1024;
static RECEIPT_TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct LocalRecord {
    object_key: String,
    path: String,
    sha256: String,
    size: u64,
}

#[derive(Clone, Debug)]
struct FileSpec {
    record: LocalRecord,
    source: FilePayload,
    sha1: String,
    md5: String,
}

#[derive(Debug)]
struct GenerationLock {
    _file: File,
}

pub fn upload_generation(
    client: &S3Client,
    generation_dir: &Path,
    generation_id: &str,
    remote_prefix: &str,
    receipt_path: &Path,
    predecessor_manifest_sha256: Option<&str>,
    native_verifier: Option<&mut B2NativeObjectVerifier>,
) -> Result<Value> {
    validate_generation_id(generation_id)?;
    let remote_prefix = normalize_remote_prefix(remote_prefix)?;
    let predecessor = predecessor_manifest_sha256
        .map(|value| validate_digest(value, 64, "predecessor manifest SHA-256"))
        .transpose()?;
    let generation_dir = absolute(generation_dir)?;
    let root_metadata = fs::symlink_metadata(&generation_dir)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(config("generation root must be a directory, not a symlink"));
    }
    let generation_dir = fs::canonicalize(generation_dir)?;
    let generation_handle = DirectoryHandle::open_existing(&generation_dir, "generation root")?;
    generation_handle.require_private_owner("generation root")?;
    generation_handle.verify_path_binding("generation root")?;
    let receipt_target = GenerationReceiptTarget::open(receipt_path, &generation_handle)?;
    generation_handle.verify_path_binding("generation root")?;
    let _lock = lock_stopped_generation(&generation_dir)?;
    generation_handle.verify_path_binding("generation root")?;
    let specs = build_specs(&generation_dir, &remote_prefix)?;
    generation_handle.verify_path_binding("generation root")?;
    if specs.iter().any(|spec| spec.record.size > SINGLE_PUT_LIMIT) {
        let paths = specs
            .iter()
            .filter(|spec| spec.record.size > SINGLE_PUT_LIMIT)
            .map(|spec| spec.record.path.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(config(format!(
            "immutable generation object exceeds the single-PUT limit: {paths}"
        )));
    }
    let total_bytes = specs.iter().try_fold(0u64, |total, spec| {
        total
            .checked_add(spec.record.size)
            .ok_or_else(|| protocol("generation total byte count overflow"))
    })?;
    if total_bytes == 0 {
        return Err(config(
            "committed generation must contain at least one byte in one file",
        ));
    }

    let publication = match client.provider {
        Provider::R2 => {
            if native_verifier.is_some() {
                return Err(config(
                    "R2 generation upload cannot use a B2 Native verifier",
                ));
            }
            publish_r2(
                client,
                generation_id,
                &remote_prefix,
                &specs,
                total_bytes,
                predecessor.as_deref(),
            )?
        }
        _ if native_verifier.is_some() => publish_native_b2(
            client,
            native_verifier.expect("checked"),
            generation_id,
            &remote_prefix,
            &specs,
            total_bytes,
            predecessor.as_deref(),
        )?,
        _ => publish_versioned_s3(
            client,
            generation_id,
            &remote_prefix,
            &specs,
            total_bytes,
            predecessor.as_deref(),
        )?,
    };

    let final_specs = build_specs(&generation_dir, &remote_prefix)?;
    generation_handle.verify_path_binding("generation root")?;
    if specs
        .iter()
        .map(|spec| &spec.record)
        .ne(final_specs.iter().map(|spec| &spec.record))
    {
        return Err(protocol("generation changed while it was being uploaded"));
    }

    let verified_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| protocol("system clock is before the Unix epoch"))?
        .as_secs();
    let mut receipt = Map::from_iter([
        ("commit_key".into(), json!(publication.commit_key)),
        ("commit_sha256".into(), json!(publication.commit_sha256)),
        (
            "commit_version_id".into(),
            json!(publication.commit_version_id),
        ),
        ("file_count".into(), json!(specs.len())),
        ("generation_id".into(), json!(generation_id)),
        ("manifest_key".into(), json!(publication.manifest_key)),
        ("manifest_sha256".into(), json!(publication.manifest_sha256)),
        (
            "manifest_version_id".into(),
            json!(publication.manifest_version_id),
        ),
        ("remote_prefix".into(), json!(remote_prefix)),
        ("schema_version".into(), json!(1)),
        ("total_bytes".into(), json!(total_bytes)),
        ("verified_unix_secs".into(), json!(verified_unix_secs)),
    ]);
    if client.provider == Provider::R2 {
        receipt.insert("object_identity".into(), json!("single-put-etag"));
        receipt.insert("storage_provider".into(), json!("r2"));
    }
    if let Some(predecessor) = predecessor {
        receipt.insert("predecessor_manifest_sha256".into(), json!(predecessor));
    }
    generation_handle.verify_path_binding("generation root")?;
    receipt_target.publish(Value::Object(receipt))
}

struct Publication {
    commit_key: String,
    commit_sha256: String,
    commit_version_id: String,
    manifest_key: String,
    manifest_sha256: String,
    manifest_version_id: String,
}

fn publish_r2(
    client: &S3Client,
    generation_id: &str,
    remote_prefix: &str,
    specs: &[FileSpec],
    total_bytes: u64,
    predecessor: Option<&str>,
) -> Result<Publication> {
    let file_versions = specs
        .iter()
        .map(|spec| (spec.record.path.clone(), spec.md5.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut manifest = generation_manifest(
        generation_id,
        specs,
        &file_versions,
        total_bytes,
        predecessor,
    )?;
    manifest.as_object_mut().expect("manifest object").extend([
        ("object_identity".into(), json!("single-put-etag")),
        ("storage_provider".into(), json!("r2")),
    ]);
    let manifest_bytes = canonical_json_bytes(&manifest)?;
    let manifest_spec = BytesSpec::new(manifest_bytes);
    let manifest_key = format!("{remote_prefix}/manifest.json");
    let mut commit = generation_commit(
        generation_id,
        &manifest_key,
        &manifest_spec.sha256,
        &manifest_spec.md5,
        specs.len(),
        total_bytes,
        predecessor,
    )?;
    commit.as_object_mut().expect("commit object").extend([
        ("object_identity".into(), json!("single-put-etag")),
        ("storage_provider".into(), json!("r2")),
    ]);
    let commit_spec = BytesSpec::new(canonical_json_bytes(&commit)?);
    let commit_key = format!("{remote_prefix}/_COMMITTED");

    let mut existing = BTreeMap::new();
    for spec in specs {
        existing.insert(
            spec.record.object_key.clone(),
            r2_identity(
                client,
                &spec.record.object_key,
                spec.record.size,
                &spec.record.sha256,
                &spec.md5,
            )?,
        );
    }
    let existing_manifest = r2_identity(
        client,
        &manifest_key,
        manifest_spec.size,
        &manifest_spec.sha256,
        &manifest_spec.md5,
    )?;
    let existing_commit = r2_identity(
        client,
        &commit_key,
        commit_spec.size,
        &commit_spec.sha256,
        &commit_spec.md5,
    )?;
    if existing.values().any(Option::is_none)
        && (existing_manifest.is_some() || existing_commit.is_some())
    {
        return Err(protocol(
            "immutable R2 generation has a manifest or commit before all files",
        ));
    }
    if existing_commit.is_some() && existing_manifest.is_none() {
        return Err(protocol(
            "immutable R2 generation has a commit without its manifest",
        ));
    }

    for spec in specs {
        if existing[&spec.record.object_key].is_none() {
            put_r2_file(client, spec)?;
        }
    }
    verify_r2_files(client, specs)?;
    if existing_manifest.is_none() {
        put_r2_bytes(client, &manifest_key, "application/json", &manifest_spec)?;
    }
    verify_r2_files(client, specs)?;
    require_r2_identity(client, &manifest_key, &manifest_spec)?;

    // Commit is intentionally the final remote write.
    if existing_commit.is_none() {
        put_r2_bytes(client, &commit_key, "application/json", &commit_spec)?;
    }
    verify_r2_files(client, specs)?;
    require_r2_identity(client, &manifest_key, &manifest_spec)?;
    require_r2_identity(client, &commit_key, &commit_spec)?;
    Ok(Publication {
        commit_key,
        commit_sha256: commit_spec.sha256,
        commit_version_id: commit_spec.md5,
        manifest_key,
        manifest_sha256: manifest_spec.sha256,
        manifest_version_id: manifest_spec.md5,
    })
}

fn publish_native_b2(
    client: &S3Client,
    verifier: &mut B2NativeObjectVerifier,
    generation_id: &str,
    remote_prefix: &str,
    specs: &[FileSpec],
    total_bytes: u64,
    predecessor: Option<&str>,
) -> Result<Publication> {
    let manifest_key = format!("{remote_prefix}/manifest.json");
    let commit_key = format!("{remote_prefix}/_COMMITTED");
    let allowed = specs
        .iter()
        .map(|spec| spec.record.object_key.clone())
        .chain([manifest_key.clone(), commit_key.clone()])
        .collect::<BTreeSet<_>>();
    let mut snapshot = verifier.list_generation_versions(remote_prefix, &allowed)?;
    let mut versions = BTreeMap::new();
    let mut missing = Vec::new();
    for spec in specs {
        match verifier.snapshot_exact_version(
            &snapshot,
            &spec.record.object_key,
            spec.record.size,
            &spec.record.sha256,
            &spec.sha1,
            &spec.md5,
            None,
        )? {
            Some(version) => {
                versions.insert(spec.record.path.clone(), version);
            }
            None => missing.push(spec),
        }
    }
    if !missing.is_empty()
        && (!snapshot[&manifest_key].is_empty() || !snapshot[&commit_key].is_empty())
    {
        return Err(protocol(
            "immutable B2 generation has a manifest or commit before all files",
        ));
    }
    for spec in missing {
        let version = put_versioned_file(client, spec)?;
        versions.insert(spec.record.path.clone(), version);
    }
    if versions.len() != specs.len() {
        return Err(protocol("generation file-version map is incomplete"));
    }
    snapshot = verifier.list_generation_versions(remote_prefix, &allowed)?;
    verify_snapshot_files(verifier, &snapshot, specs, &versions)?;

    let manifest = generation_manifest(generation_id, specs, &versions, total_bytes, predecessor)?;
    let manifest_spec = BytesSpec::new(canonical_json_bytes(&manifest)?);
    let mut manifest_version = verifier.snapshot_exact_version(
        &snapshot,
        &manifest_key,
        manifest_spec.size,
        &manifest_spec.sha256,
        &manifest_spec.sha1,
        &manifest_spec.md5,
        None,
    )?;
    if manifest_version.is_none() {
        if !snapshot[&commit_key].is_empty() {
            return Err(protocol(
                "immutable B2 generation has a commit without its manifest",
            ));
        }
        let uploaded =
            put_versioned_bytes(client, &manifest_key, "application/json", &manifest_spec)?;
        snapshot = verifier.list_generation_versions(remote_prefix, &allowed)?;
        verify_snapshot_files(verifier, &snapshot, specs, &versions)?;
        verifier.snapshot_exact_version(
            &snapshot,
            &manifest_key,
            manifest_spec.size,
            &manifest_spec.sha256,
            &manifest_spec.sha1,
            &manifest_spec.md5,
            Some(&uploaded),
        )?;
        manifest_version = Some(uploaded);
    }
    let manifest_version = manifest_version.expect("present");
    let commit = generation_commit(
        generation_id,
        &manifest_key,
        &manifest_spec.sha256,
        &manifest_version,
        specs.len(),
        total_bytes,
        predecessor,
    )?;
    let commit_spec = BytesSpec::new(canonical_json_bytes(&commit)?);
    let mut commit_version = verifier.snapshot_exact_version(
        &snapshot,
        &commit_key,
        commit_spec.size,
        &commit_spec.sha256,
        &commit_spec.sha1,
        &commit_spec.md5,
        None,
    )?;
    if commit_version.is_none() {
        let uploaded = put_versioned_bytes(client, &commit_key, "application/json", &commit_spec)?;
        snapshot = verifier.list_generation_versions(remote_prefix, &allowed)?;
        verify_snapshot_files(verifier, &snapshot, specs, &versions)?;
        verifier.snapshot_exact_version(
            &snapshot,
            &manifest_key,
            manifest_spec.size,
            &manifest_spec.sha256,
            &manifest_spec.sha1,
            &manifest_spec.md5,
            Some(&manifest_version),
        )?;
        verifier.snapshot_exact_version(
            &snapshot,
            &commit_key,
            commit_spec.size,
            &commit_spec.sha256,
            &commit_spec.sha1,
            &commit_spec.md5,
            Some(&uploaded),
        )?;
        commit_version = Some(uploaded);
    }
    Ok(Publication {
        commit_key,
        commit_sha256: commit_spec.sha256,
        commit_version_id: commit_version.expect("present"),
        manifest_key,
        manifest_sha256: manifest_spec.sha256,
        manifest_version_id: manifest_version,
    })
}

fn publish_versioned_s3(
    client: &S3Client,
    generation_id: &str,
    remote_prefix: &str,
    specs: &[FileSpec],
    total_bytes: u64,
    predecessor: Option<&str>,
) -> Result<Publication> {
    let mut versions = BTreeMap::new();
    for spec in specs {
        let version = match current_version(
            client,
            &spec.record.object_key,
            spec.record.size,
            &spec.record.sha256,
            &spec.md5,
        )? {
            Some(version) => version,
            None => {
                let version = put_versioned_file(client, spec)?;
                verify_version(
                    client,
                    &spec.record.object_key,
                    spec.record.size,
                    &spec.record.sha256,
                    &spec.md5,
                    &version,
                )?;
                current_version(
                    client,
                    &spec.record.object_key,
                    spec.record.size,
                    &spec.record.sha256,
                    &spec.md5,
                )?
                .ok_or_else(|| protocol("uploaded object disappeared before publication"))?;
                version
            }
        };
        versions.insert(spec.record.path.clone(), version);
    }
    let manifest = generation_manifest(generation_id, specs, &versions, total_bytes, predecessor)?;
    let manifest_spec = BytesSpec::new(canonical_json_bytes(&manifest)?);
    let manifest_key = format!("{remote_prefix}/manifest.json");
    let manifest_version = upload_versioned_bytes_idempotent(
        client,
        &manifest_key,
        "application/json",
        &manifest_spec,
    )?;
    let commit = generation_commit(
        generation_id,
        &manifest_key,
        &manifest_spec.sha256,
        &manifest_version,
        specs.len(),
        total_bytes,
        predecessor,
    )?;
    let commit_spec = BytesSpec::new(canonical_json_bytes(&commit)?);
    let commit_key = format!("{remote_prefix}/_COMMITTED");
    // A versioned generation still requires Object Lock/retention or an
    // operational guarantee that no principal can DeleteObjectVersion while
    // publication runs. These exact-version HEAD sweeps cheaply detect a lost
    // pinned version before and after commit, but sequential requests cannot
    // make concurrent remote deletion atomic with the commit PUT.
    verify_versioned_files_live(client, specs, &versions)?;
    verify_version_head_only(
        client,
        &manifest_key,
        manifest_spec.size,
        &manifest_spec.sha256,
        &manifest_spec.md5,
        &manifest_version,
    )?;
    let commit_version =
        upload_versioned_bytes_idempotent(client, &commit_key, "application/json", &commit_spec)?;
    verify_versioned_files_live(client, specs, &versions)?;
    verify_version_head_only(
        client,
        &manifest_key,
        manifest_spec.size,
        &manifest_spec.sha256,
        &manifest_spec.md5,
        &manifest_version,
    )?;
    verify_version_head_only(
        client,
        &commit_key,
        commit_spec.size,
        &commit_spec.sha256,
        &commit_spec.md5,
        &commit_version,
    )?;
    Ok(Publication {
        commit_key,
        commit_sha256: commit_spec.sha256,
        commit_version_id: commit_version,
        manifest_key,
        manifest_sha256: manifest_spec.sha256,
        manifest_version_id: manifest_version,
    })
}

fn generation_manifest(
    generation_id: &str,
    specs: &[FileSpec],
    versions: &BTreeMap<String, String>,
    total_bytes: u64,
    predecessor: Option<&str>,
) -> Result<Value> {
    if versions.len() != specs.len()
        || specs
            .iter()
            .any(|spec| !versions.contains_key(&spec.record.path))
    {
        return Err(config(
            "generation file-version map does not match local files",
        ));
    }
    let files = specs
        .iter()
        .map(|spec| {
            json!({
                "object_key": spec.record.object_key,
                "path": spec.record.path,
                "sha256": spec.record.sha256,
                "size": spec.record.size,
                "version_id": versions[&spec.record.path],
            })
        })
        .collect::<Vec<_>>();
    let mut manifest = Map::from_iter([
        ("files".into(), Value::Array(files)),
        ("generation_id".into(), json!(generation_id)),
        ("schema_version".into(), json!(1)),
        ("total_bytes".into(), json!(total_bytes)),
    ]);
    if let Some(predecessor) = predecessor {
        manifest.insert("predecessor_manifest_sha256".into(), json!(predecessor));
    }
    Ok(Value::Object(manifest))
}

fn generation_commit(
    generation_id: &str,
    manifest_key: &str,
    manifest_sha256: &str,
    manifest_version_id: &str,
    file_count: usize,
    total_bytes: u64,
    predecessor: Option<&str>,
) -> Result<Value> {
    validate_generation_id(generation_id)?;
    validate_object_key(manifest_key)?;
    validate_digest(manifest_sha256, 64, "manifest SHA-256")?;
    validate_version_id(manifest_version_id, "manifest version ID")?;
    if file_count < 1 || total_bytes < 1 {
        return Err(config(
            "committed generation must contain at least one byte in one file",
        ));
    }
    let mut commit = Map::from_iter([
        ("file_count".into(), json!(file_count)),
        ("generation_id".into(), json!(generation_id)),
        ("manifest_key".into(), json!(manifest_key)),
        ("manifest_sha256".into(), json!(manifest_sha256)),
        ("manifest_version_id".into(), json!(manifest_version_id)),
        ("schema_version".into(), json!(1)),
        ("total_bytes".into(), json!(total_bytes)),
    ]);
    if let Some(predecessor) = predecessor {
        commit.insert("predecessor_manifest_sha256".into(), json!(predecessor));
    }
    Ok(Value::Object(commit))
}

fn build_specs(root: &Path, remote_prefix: &str) -> Result<Vec<FileSpec>> {
    let mut specs = Vec::new();
    for (relative, path) in walk_generation(root)? {
        let (source, sha256, sha1, md5) = hash_regular_file(&path)?;
        let size = source.len();
        specs.push(FileSpec {
            record: LocalRecord {
                object_key: format!("{remote_prefix}/files/{relative}"),
                path: relative,
                sha256,
                size,
            },
            source,
            sha1,
            md5,
        });
    }
    Ok(specs)
}

fn walk_generation(root: &Path) -> Result<Vec<(String, PathBuf)>> {
    let metadata = fs::symlink_metadata(root)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(config("generation root must be a directory, not a symlink"));
    }
    let mut files = Vec::new();
    visit(root, root, &mut files)?;
    files.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    if files.is_empty() {
        return Err(config("generation contains no regular files"));
    }
    Ok(files)
}

fn visit(root: &Path, directory: &Path, files: &mut Vec<(String, PathBuf)>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        let relative = path
            .strip_prefix(root)
            .map_err(|_| protocol("generation traversal escaped its root"))?;
        let relative = relative_path(relative)?;
        if metadata.file_type().is_symlink() {
            return Err(config(format!("generation contains a symlink: {relative}")));
        }
        if metadata.is_dir() {
            visit(root, &path, files)?;
        } else if metadata.is_file() {
            files.push((relative, path));
        } else {
            return Err(config(format!(
                "generation contains a non-regular entry: {relative}"
            )));
        }
    }
    Ok(())
}

fn lock_stopped_generation(root: &Path) -> Result<GenerationLock> {
    let files = walk_generation(root)?;
    let paths = files
        .iter()
        .map(|(relative, _)| relative.as_str())
        .collect::<BTreeSet<_>>();
    let missing = ["identity.json", "raw-blocks.jsonl"]
        .into_iter()
        .filter(|required| !paths.contains(required))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(config(format!(
            "generation is missing required file(s): {}",
            missing.join(", ")
        )));
    }
    if !paths.iter().any(|path| path.ends_with(".wal")) {
        return Err(config("generation contains no WAL segment"));
    }
    let locks = files
        .iter()
        .filter(|(relative, _)| {
            relative
                .rsplit('/')
                .next()
                .is_some_and(|name| name == "writer.lock")
        })
        .collect::<Vec<_>>();
    if locks.len() != 1 {
        return Err(config(format!(
            "generation must contain exactly one WAL writer.lock, found {}",
            locks.len()
        )));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&locks[0].1)?;
    if !file.metadata()?.is_file() {
        return Err(config("generation writer.lock is not a regular file"));
    }
    file.try_lock_exclusive().map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            protocol("generation WAL is still locked by an active writer")
        } else {
            UploaderError::Io(error)
        }
    })?;
    Ok(GenerationLock { _file: file })
}

fn hash_regular_file(path: &Path) -> Result<(FilePayload, String, String, String)> {
    let source = FilePayload::open(path)?;
    source.verify("before hashing")?;
    let mut file = source.reader();
    let mut sha256 = Sha256::new();
    let mut sha1 = Sha1::new();
    let mut md5 = Md5::new();
    let mut size = 0u64;
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        size = size
            .checked_add(count as u64)
            .ok_or_else(|| protocol("local file byte count overflow"))?;
        sha256.update(&buffer[..count]);
        sha1.update(&buffer[..count]);
        md5.update(&buffer[..count]);
    }
    source.verify("while hashing")?;
    if size != source.len() {
        return Err(protocol("local file changed while hashing"));
    }
    Ok((
        source,
        hex::encode(sha256.finalize()),
        hex::encode(sha1.finalize()),
        hex::encode(md5.finalize()),
    ))
}

#[derive(Clone)]
struct BytesSpec {
    bytes: Vec<u8>,
    size: u64,
    sha256: String,
    sha1: String,
    md5: String,
}

impl BytesSpec {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            size: bytes.len() as u64,
            sha256: hex::encode(Sha256::digest(&bytes)),
            sha1: hex::encode(Sha1::digest(&bytes)),
            md5: hex::encode(Md5::digest(&bytes)),
            bytes,
        }
    }
}

fn r2_identity(
    client: &S3Client,
    key: &str,
    size: u64,
    sha256: &str,
    etag: &str,
) -> Result<Option<String>> {
    if client.provider != Provider::R2 {
        return Err(config("R2 immutable verification requires an R2 client"));
    }
    let response = client.head(key)?;
    if response.status == 404 {
        return Ok(None);
    }
    let remote_size = response.exact_content_length(&format!("HEAD {key}"))?;
    if remote_size != size {
        return Err(protocol(format!(
            "immutable R2 object collision at {key}: size differs"
        )));
    }
    let operation = format!("HEAD {key}");
    if response
        .exact_header("x-amz-meta-sha256", &operation)?
        .to_ascii_lowercase()
        != sha256
    {
        return Err(protocol(format!(
            "immutable R2 object collision at {key}: SHA-256 metadata differs"
        )));
    }
    let remote_etag = response_etag(&response, "HEAD", key)?;
    if remote_etag != etag {
        return Err(protocol(format!(
            "immutable R2 object collision at {key}: ETag differs"
        )));
    }
    Ok(Some(remote_etag))
}

fn put_r2_file(client: &S3Client, spec: &FileSpec) -> Result<()> {
    let md5 = md5_base64(&spec.md5)?;
    let response = client.put(
        &spec.record.object_key,
        "application/octet-stream",
        &spec.record.sha256,
        Some(&md5),
        true,
        &Payload::File(spec.source.clone()),
    )?;
    complete_r2_put(
        client,
        response,
        &spec.record.object_key,
        spec.record.size,
        &spec.record.sha256,
        &spec.md5,
    )
}

fn put_r2_bytes(client: &S3Client, key: &str, content_type: &str, spec: &BytesSpec) -> Result<()> {
    if spec.size > SINGLE_PUT_LIMIT {
        return Err(config(format!(
            "immutable R2 object exceeds the single-PUT limit: {key}"
        )));
    }
    if r2_identity(client, key, spec.size, &spec.sha256, &spec.md5)?.is_some() {
        return Ok(());
    }
    let md5 = md5_base64(&spec.md5)?;
    let response = client.put(
        key,
        content_type,
        &spec.sha256,
        Some(&md5),
        true,
        &Payload::Bytes(spec.bytes.clone()),
    )?;
    complete_r2_put(client, response, key, spec.size, &spec.sha256, &spec.md5)
}

fn complete_r2_put(
    client: &S3Client,
    response: super::S3Response,
    key: &str,
    size: u64,
    sha256: &str,
    etag: &str,
) -> Result<()> {
    if response.status == 412 {
        return r2_identity(client, key, size, sha256, etag)?
            .map(|_| ())
            .ok_or_else(|| {
                protocol(format!(
                    "immutable R2 PUT precondition failed but {key} is absent"
                ))
            });
    }
    if response_etag(&response, "PUT", key)? != etag {
        return Err(protocol(format!("PUT {key} ETag mismatch")));
    }
    r2_identity(client, key, size, sha256, etag)?
        .map(|_| ())
        .ok_or_else(|| {
            protocol(format!(
                "uploaded R2 object disappeared before publication: {key}"
            ))
        })
}

fn verify_r2_files(client: &S3Client, specs: &[FileSpec]) -> Result<()> {
    for spec in specs {
        if r2_identity(
            client,
            &spec.record.object_key,
            spec.record.size,
            &spec.record.sha256,
            &spec.md5,
        )?
        .is_none()
        {
            return Err(protocol(format!(
                "immutable R2 object is missing after publication: {}",
                spec.record.object_key
            )));
        }
    }
    Ok(())
}

fn require_r2_identity(client: &S3Client, key: &str, spec: &BytesSpec) -> Result<()> {
    r2_identity(client, key, spec.size, &spec.sha256, &spec.md5)?
        .map(|_| ())
        .ok_or_else(|| {
            protocol(format!(
                "immutable R2 object is missing after publication: {key}"
            ))
        })
}

fn put_versioned_file(client: &S3Client, spec: &FileSpec) -> Result<String> {
    let response = client.put(
        &spec.record.object_key,
        "application/octet-stream",
        &spec.record.sha256,
        None,
        false,
        &Payload::File(spec.source.clone()),
    )?;
    let version = response_version(&response, "PUT", &spec.record.object_key)?;
    if response_etag(&response, "PUT", &spec.record.object_key)? != spec.md5 {
        return Err(protocol(format!(
            "PUT {} ETag mismatch",
            spec.record.object_key
        )));
    }
    Ok(version)
}

fn put_versioned_bytes(
    client: &S3Client,
    key: &str,
    content_type: &str,
    spec: &BytesSpec,
) -> Result<String> {
    let response = client.put(
        key,
        content_type,
        &spec.sha256,
        None,
        false,
        &Payload::Bytes(spec.bytes.clone()),
    )?;
    let version = response_version(&response, "PUT", key)?;
    if response_etag(&response, "PUT", key)? != spec.md5 {
        return Err(protocol(format!("PUT {key} ETag mismatch")));
    }
    Ok(version)
}

fn upload_versioned_bytes_idempotent(
    client: &S3Client,
    key: &str,
    content_type: &str,
    spec: &BytesSpec,
) -> Result<String> {
    if let Some(version) = current_version(client, key, spec.size, &spec.sha256, &spec.md5)? {
        return Ok(version);
    }
    let version = put_versioned_bytes(client, key, content_type, spec)?;
    verify_version(client, key, spec.size, &spec.sha256, &spec.md5, &version)?;
    current_version(client, key, spec.size, &spec.sha256, &spec.md5)?.ok_or_else(|| {
        protocol(format!(
            "uploaded object disappeared before publication: {key}"
        ))
    })?;
    Ok(version)
}

fn current_version(
    client: &S3Client,
    key: &str,
    size: u64,
    sha256: &str,
    etag: &str,
) -> Result<Option<String>> {
    let response = client.head(key)?;
    if response.status == 404 {
        return Ok(None);
    }
    let version = verify_head(&response, key, size, sha256, etag, None)?;
    verify_full_get(client, key, size, sha256, etag, &version)?;
    Ok(Some(version))
}

fn verify_version(
    client: &S3Client,
    key: &str,
    size: u64,
    sha256: &str,
    etag: &str,
    version: &str,
) -> Result<()> {
    verify_version_head_only(client, key, size, sha256, etag, version)?;
    verify_full_get(client, key, size, sha256, etag, version)?;
    Ok(())
}

fn verify_version_head_only(
    client: &S3Client,
    key: &str,
    size: u64,
    sha256: &str,
    etag: &str,
    version: &str,
) -> Result<()> {
    validate_version_id(version, "expected version ID")?;
    let params = BTreeMap::from([("versionId".into(), version.into())]);
    let response = client.head_with_params(key, &params)?;
    if response.status == 404 {
        return Err(protocol(format!(
            "pinned object version is missing before generation publication: {key}"
        )));
    }
    verify_head(&response, key, size, sha256, etag, Some(version))?;
    Ok(())
}

fn verify_versioned_files_live(
    client: &S3Client,
    specs: &[FileSpec],
    versions: &BTreeMap<String, String>,
) -> Result<()> {
    for spec in specs {
        let version = versions
            .get(&spec.record.path)
            .ok_or_else(|| config("generation file-version map is incomplete"))?;
        verify_version_head_only(
            client,
            &spec.record.object_key,
            spec.record.size,
            &spec.record.sha256,
            &spec.md5,
            version,
        )?;
    }
    Ok(())
}

fn verify_full_get(
    client: &S3Client,
    key: &str,
    size: u64,
    sha256: &str,
    etag: &str,
    version: &str,
) -> Result<()> {
    let params = BTreeMap::from([("versionId".into(), version.into())]);
    let response = client.get_with_params(key, &params)?;
    if response.exact_content_length(&format!("GET {key}"))? != size {
        return Err(protocol(format!(
            "GET {key} size mismatch: expected={size}"
        )));
    }
    if response_version(&response, "GET", key)? != version {
        return Err(protocol(format!(
            "GET {key} returned a different object version"
        )));
    }
    if response_etag(&response, "GET", key)? != etag {
        return Err(protocol(format!("GET {key} ETag mismatch")));
    }
    let mut reader = response.into_reader();
    let mut digest = Sha256::new();
    let mut downloaded = 0u64;
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let remaining = size.saturating_sub(downloaded).saturating_add(1);
        let maximum = remaining.min(buffer.len() as u64) as usize;
        let count = reader.read(&mut buffer[..maximum])?;
        if count == 0 {
            break;
        }
        downloaded = downloaded
            .checked_add(count as u64)
            .ok_or_else(|| protocol("downloaded object length overflow"))?;
        if downloaded > size {
            return Err(protocol(format!(
                "GET {key} size mismatch: downloaded={downloaded} expected={size}"
            )));
        }
        digest.update(&buffer[..count]);
    }
    if downloaded != size {
        return Err(protocol(format!(
            "GET {key} size mismatch: downloaded={downloaded} expected={size}"
        )));
    }
    if hex::encode(digest.finalize()) != sha256 {
        return Err(protocol(format!("GET {key} SHA-256 mismatch")));
    }
    Ok(())
}

fn verify_head(
    response: &super::S3Response,
    key: &str,
    size: u64,
    sha256: &str,
    etag: &str,
    version: Option<&str>,
) -> Result<String> {
    let remote_size = response.exact_content_length(&format!("HEAD {key}"))?;
    if remote_size != size {
        return Err(protocol(format!(
            "HEAD {key} size mismatch: remote={remote_size} expected={size}"
        )));
    }
    let operation = format!("HEAD {key}");
    if response
        .exact_header("x-amz-meta-sha256", &operation)?
        .to_ascii_lowercase()
        != sha256
    {
        return Err(protocol(format!("HEAD {key} SHA-256 metadata mismatch")));
    }
    if response_etag(response, "HEAD", key)? != etag {
        return Err(protocol(format!("HEAD {key} ETag mismatch")));
    }
    let returned = response_version(response, "HEAD", key)?;
    if version.is_some_and(|version| returned != version) {
        return Err(protocol(format!(
            "HEAD {key} returned a different object version"
        )));
    }
    Ok(returned)
}

fn verify_snapshot_files(
    verifier: &B2NativeObjectVerifier,
    snapshot: &NativeSnapshot,
    specs: &[FileSpec],
    versions: &BTreeMap<String, String>,
) -> Result<()> {
    for spec in specs {
        verifier.snapshot_exact_version(
            snapshot,
            &spec.record.object_key,
            spec.record.size,
            &spec.record.sha256,
            &spec.sha1,
            &spec.md5,
            Some(&versions[&spec.record.path]),
        )?;
    }
    Ok(())
}

fn response_version(response: &super::S3Response, operation: &str, key: &str) -> Result<String> {
    let request = format!("{operation} {key}");
    let value = response.exact_header("x-amz-version-id", &request)?;
    validate_version_id(value, &format!("{request} version ID"))?;
    Ok(value.into())
}

fn response_etag(response: &super::S3Response, operation: &str, key: &str) -> Result<String> {
    let request = format!("{operation} {key}");
    normalize_etag(
        response.exact_header("etag", &request)?,
        &format!("{request} ETag"),
    )
}

fn normalize_etag(value: &str, label: &str) -> Result<String> {
    let value = value.trim();
    let value = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
        .to_ascii_lowercase();
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(config(format!(
            "{label} must be a single-part 32-hexadecimal ETag"
        )));
    }
    Ok(value)
}

fn md5_base64(hex_digest: &str) -> Result<String> {
    let bytes = hex::decode(hex_digest)
        .map_err(|_| config("MD5 digest must contain exactly 32 hexadecimal characters"))?;
    if bytes.len() != 16 {
        return Err(config(
            "MD5 digest must contain exactly 32 hexadecimal characters",
        ));
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
}

struct GenerationReceiptTarget {
    directory: DirectoryHandle,
    file_name: OsString,
    initial_existing: Option<Vec<u8>>,
}

impl GenerationReceiptTarget {
    fn open(path: &Path, generation: &DirectoryHandle) -> Result<Self> {
        let requested = absolute(path)?;
        let parent = requested
            .parent()
            .ok_or_else(|| config("generation receipt has no parent directory"))?;
        let file_name = requested
            .file_name()
            .ok_or_else(|| config("generation receipt has no file name"))?
            .to_os_string();
        let directory = DirectoryHandle::open_or_create(parent, "generation receipt parent")?;
        directory.require_private_owner("generation receipt parent")?;
        if directory.contains_ancestor(generation.identity())? {
            return Err(config(
                "generation receipt must be outside the generation directory",
            ));
        }
        let initial_existing = directory.read_regular_optional(
            &file_name,
            MAX_GENERATION_RECEIPT_BYTES,
            "generation receipt",
        )?;
        if let Some(bytes) = &initial_existing {
            existing_receipt_time(bytes)?;
        }
        Ok(Self {
            directory,
            file_name,
            initial_existing,
        })
    }

    fn publish(&self, value: Value) -> Result<Value> {
        let temporary = format!(
            ".{}.tmp.{}.{}",
            self.file_name.to_string_lossy(),
            std::process::id(),
            RECEIPT_TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        self.publish_with_temporary(value, OsStr::new(&temporary))
    }

    fn publish_with_temporary(&self, mut value: Value, temporary: &OsStr) -> Result<Value> {
        if let Some(initial) = &self.initial_existing {
            let current = self.directory.read_regular(
                &self.file_name,
                MAX_GENERATION_RECEIPT_BYTES,
                "generation receipt",
            )?;
            if &current != initial {
                return Err(protocol("generation receipt changed during publication"));
            }
            value["verified_unix_secs"] = Value::from(existing_receipt_time(initial)?);
            if canonical_json_bytes(&value)? != *initial {
                return Err(protocol("generation receipt collision"));
            }
            return Ok(value);
        }

        let payload = canonical_json_bytes(&value)?;
        if payload.len() > MAX_GENERATION_RECEIPT_BYTES {
            return Err(config("generation receipt is unexpectedly large"));
        }
        let mut created = None;
        let result = (|| {
            let mut file = self.directory.create_exclusive(temporary, 0o600)?;
            let metadata = file.metadata()?;
            created = Some((metadata.dev(), metadata.ino()));
            file.write_all(&payload)?;
            file.sync_all()?;
            if self.directory.link_same_inode_no_replace(
                temporary,
                &self.file_name,
                created.expect("temporary identity recorded"),
                "generation receipt temporary",
            )? {
                self.directory.sync()?;
                if self.directory.read_regular(
                    &self.file_name,
                    MAX_GENERATION_RECEIPT_BYTES,
                    "generation receipt",
                )? != payload
                {
                    return Err(protocol("generation receipt changed after publication"));
                }
                return Ok(());
            }
            let current = self.directory.read_regular(
                &self.file_name,
                MAX_GENERATION_RECEIPT_BYTES,
                "generation receipt",
            )?;
            if current != payload {
                return Err(protocol("generation receipt collision"));
            }
            Ok(())
        })();
        let cleanup = if let Some(identity) = created {
            self.directory
                .unlink_if_same_inode(temporary, identity, "generation receipt temporary")
                .and_then(|removed| removed.then(|| self.directory.sync()).transpose())
                .map(|_| ())
        } else {
            Ok(())
        };
        result.and(cleanup)?;
        Ok(value)
    }
}

fn existing_receipt_time(bytes: &[u8]) -> Result<u64> {
    let value = strict_json_value(bytes).map_err(|_| config("generation receipt is invalid"))?;
    value
        .as_object()
        .and_then(|object| object.get("verified_unix_secs"))
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| config("generation receipt verification time is invalid"))
}

fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn relative_path(path: &Path) -> Result<String> {
    if path.as_os_str().is_empty() {
        return Err(config("unsafe empty generation path"));
    }
    let mut values = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                let value = value
                    .to_str()
                    .ok_or_else(|| config("generation path is not valid UTF-8"))?;
                if value.is_empty()
                    || value == "."
                    || value == ".."
                    || value.contains('\\')
                    || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
                {
                    return Err(config(format!("unsafe generation path {path:?}")));
                }
                values.push(value);
            }
            _ => return Err(config(format!("unsafe generation path {path:?}"))),
        }
    }
    Ok(values.join("/"))
}

fn normalize_remote_prefix(value: &str) -> Result<String> {
    let value = value.trim_end_matches('/');
    validate_object_key(value)?;
    if value
        .split('/')
        .any(|component| matches!(component, "" | "." | ".."))
    {
        return Err(config("remote prefix contains an unsafe path component"));
    }
    Ok(value.into())
}

fn validate_object_key(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with('/')
        || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
    {
        return Err(config("object key must be non-empty and relative"));
    }
    Ok(())
}

fn validate_generation_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value.as_bytes()[0].is_ascii_alphanumeric()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(config(
            "generation ID must be 1-128 safe ASCII characters and start with an alphanumeric",
        ));
    }
    Ok(())
}

fn validate_version_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 1024
        || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
    {
        return Err(config(format!(
            "{label} must be non-empty and at most 1024 bytes"
        )));
    }
    Ok(())
}

fn validate_digest(value: &str, length: usize, label: &str) -> Result<String> {
    let value = value.to_ascii_lowercase();
    if value.len() != length || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(config(format!(
            "{label} must be exactly {length} hexadecimal characters"
        )));
    }
    Ok(value)
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
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::fs::symlink;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;

    #[derive(Clone)]
    struct Stored {
        data: Vec<u8>,
        etag: String,
        sha256: String,
        version: String,
    }

    #[derive(Default)]
    struct StoreState {
        objects: BTreeMap<String, Vec<Stored>>,
        requests: Vec<(String, String)>,
        next_version: u64,
        corrupt_get_key: Option<String>,
        drop_first_put_response: bool,
        conflict_after_put_key: Option<String>,
        delete_during_put: Option<(String, String)>,
    }

    struct StoreServer {
        state: Arc<Mutex<StoreState>>,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
        endpoint: String,
        versioned: bool,
    }

    impl StoreServer {
        fn start(versioned: bool) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let state = Arc::new(Mutex::new(StoreState::default()));
            let stop = Arc::new(AtomicBool::new(false));
            let thread_state = Arc::clone(&state);
            let thread_stop = Arc::clone(&stop);
            let thread = std::thread::spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => handle_store(stream, &thread_state, versioned),
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(2));
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
                versioned,
            }
        }

        fn client(&self, provider: Provider) -> S3Client {
            self.client_with_retries(provider, 0)
        }

        fn client_with_retries(&self, provider: Provider, retries: u32) -> S3Client {
            S3Client::new(
                super::super::StorageSettings {
                    endpoint: self.endpoint.clone(),
                    region: if provider == Provider::R2 {
                        "auto".into()
                    } else {
                        "test-region".into()
                    },
                    bucket: "bucket".into(),
                    access_key: "access".into(),
                    secret_key: "secret".into(),
                    provider,
                    session_token: None,
                },
                retries,
            )
            .unwrap()
        }

        fn put_count(&self) -> usize {
            self.state
                .lock()
                .unwrap()
                .requests
                .iter()
                .filter(|(method, _)| method == "PUT")
                .count()
        }

        fn put_keys(&self) -> Vec<String> {
            self.state
                .lock()
                .unwrap()
                .requests
                .iter()
                .filter(|(method, _)| method == "PUT")
                .map(|(_, key)| key.clone())
                .collect()
        }

        fn object(&self, key: &str) -> Vec<u8> {
            self.state.lock().unwrap().objects[key]
                .last()
                .unwrap()
                .data
                .clone()
        }

        fn inject(&self, key: &str, data: &[u8]) {
            let mut state = self.state.lock().unwrap();
            state.next_version += 1;
            let version = format!("version-{}", state.next_version);
            state.objects.entry(key.into()).or_default().push(Stored {
                data: data.to_vec(),
                etag: hex::encode(Md5::digest(data)),
                sha256: hex::encode(Sha256::digest(data)),
                version,
            });
        }

        fn corrupt_get(&self, key: &str) {
            self.state.lock().unwrap().corrupt_get_key = Some(key.into());
        }

        fn drop_first_put_response(&self) {
            self.state.lock().unwrap().drop_first_put_response = true;
        }

        fn conflict_after_put(&self, key: &str) {
            self.state.lock().unwrap().conflict_after_put_key = Some(key.into());
        }

        fn delete_during_put(&self, trigger_key: &str, victim_key: &str) {
            self.state.lock().unwrap().delete_during_put =
                Some((trigger_key.into(), victim_key.into()));
        }

        fn native_verifier(&self, allowed_prefix: &str) -> B2NativeObjectVerifier {
            let client = super::super::B2NativeClient::with_authorize_url(
                "application-key-id".into(),
                "application-key".into(),
                0,
                format!("{}/b2api/v4/b2_authorize_account", self.endpoint),
            )
            .unwrap();
            let verifier =
                B2NativeObjectVerifier::new(client, "bucket-id".into(), "bucket".into()).unwrap();
            // The mock authorization endpoint obtains this from each request;
            // keeping the argument here makes the intended authority explicit.
            assert!(allowed_prefix.starts_with("prefix/"));
            verifier
        }
    }

    impl Drop for StoreServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = TcpStream::connect(self.endpoint.trim_start_matches("http://"));
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
    }

    fn handle_store(mut stream: TcpStream, state: &Mutex<StoreState>, versioned: bool) {
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 8192];
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
        let header_text = std::str::from_utf8(&bytes[..header_end])
            .unwrap()
            .to_string();
        let request_line = header_text.lines().next().unwrap();
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts.next().unwrap().to_string();
        let target = request_parts.next().unwrap();
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let headers = header_text
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
            .collect::<BTreeMap<_, _>>();
        assert!(headers.contains_key("authorization"));
        let content_length = headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        while bytes.len() - header_end < content_length {
            let count = stream.read(&mut chunk).unwrap();
            bytes.extend_from_slice(&chunk[..count]);
        }
        let body = &bytes[header_end..header_end + content_length];
        if path.starts_with("/b2api/v4/") {
            handle_native_b2(stream, state, path, query);
            return;
        }
        let key = path.strip_prefix("/bucket/").unwrap().to_string();
        let requested_version = url::form_urlencoded::parse(query.as_bytes())
            .find(|(name, _)| name == "versionId")
            .map(|(_, value)| value.into_owned());
        let mut state = state.lock().unwrap();
        state.requests.push((method.clone(), key.clone()));
        match method.as_str() {
            "HEAD" => {
                let stored = state.objects.get(&key).and_then(|versions| {
                    requested_version.as_ref().map_or_else(
                        || versions.last(),
                        |requested| versions.iter().find(|value| &value.version == requested),
                    )
                });
                if let Some(stored) = stored {
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"{}\"\r\nx-amz-meta-sha256: {}\r\n",
                        stored.data.len(), stored.etag, stored.sha256
                    )
                    .unwrap();
                    if versioned {
                        write!(stream, "x-amz-version-id: {}\r\n", stored.version).unwrap();
                    }
                    write!(stream, "Connection: close\r\n\r\n").unwrap();
                } else {
                    write!(
                        stream,
                        "HTTP/1.1 404 Missing\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                }
            }
            "GET" => {
                let stored = state.objects.get(&key).and_then(|versions| {
                    requested_version.as_ref().map_or_else(
                        || versions.last(),
                        |requested| versions.iter().find(|value| &value.version == requested),
                    )
                });
                if let Some(stored) = stored {
                    let mut body = stored.data.clone();
                    if state.corrupt_get_key.as_deref() == Some(&key) && !body.is_empty() {
                        body[0] ^= 0xff;
                    }
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"{}\"\r\nx-amz-meta-sha256: {}\r\n",
                        body.len(), stored.etag, stored.sha256
                    )
                    .unwrap();
                    if versioned {
                        write!(stream, "x-amz-version-id: {}\r\n", stored.version).unwrap();
                    }
                    write!(stream, "Connection: close\r\n\r\n").unwrap();
                    stream.write_all(&body).unwrap();
                } else {
                    write!(
                        stream,
                        "HTTP/1.1 404 Missing\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                }
            }
            "PUT" => {
                if headers.get("if-none-match").map(String::as_str) == Some("*")
                    && state.objects.contains_key(&key)
                {
                    write!(
                        stream,
                        "HTTP/1.1 412 Exists\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                    return;
                }
                let sha256 = hex::encode(Sha256::digest(body));
                assert_eq!(headers["x-amz-meta-sha256"], sha256);
                let etag = hex::encode(Md5::digest(body));
                state.next_version += 1;
                let version = format!("version-{}", state.next_version);
                state.objects.entry(key.clone()).or_default().push(Stored {
                    data: body.to_vec(),
                    etag: etag.clone(),
                    sha256,
                    version: version.clone(),
                });
                if state.conflict_after_put_key.as_deref() == Some(&key) {
                    state.conflict_after_put_key = None;
                    let conflicting = b"competing immutable object".to_vec();
                    state.next_version += 1;
                    let conflicting_version = format!("version-{}", state.next_version);
                    state.objects.get_mut(&key).unwrap().push(Stored {
                        etag: hex::encode(Md5::digest(&conflicting)),
                        sha256: hex::encode(Sha256::digest(&conflicting)),
                        data: conflicting,
                        version: conflicting_version,
                    });
                }
                if state
                    .delete_during_put
                    .as_ref()
                    .is_some_and(|(trigger, _)| trigger == &key)
                {
                    let (_, victim) = state.delete_during_put.take().expect("checked");
                    state.objects.remove(&victim);
                }
                if std::mem::take(&mut state.drop_first_put_response) {
                    return;
                }
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"{etag}\"\r\n"
                )
                .unwrap();
                if versioned {
                    write!(stream, "x-amz-version-id: {version}\r\n").unwrap();
                }
                write!(stream, "Connection: close\r\n\r\n").unwrap();
            }
            other => panic!("unexpected method {other}"),
        }
    }

    fn handle_native_b2(mut stream: TcpStream, state: &Mutex<StoreState>, path: &str, query: &str) {
        let response = if path.ends_with("/b2_authorize_account") {
            json!({
                "accountId": "account",
                "authorizationToken": "native-token",
                "apiInfo": {
                    "storageApi": {
                        "apiUrl": format!(
                            "http://{}",
                            stream.local_addr().unwrap()
                        ),
                        "allowed": {
                            "bucketId": "bucket-id",
                            "bucketName": "bucket",
                            "capabilities": ["listFiles"],
                            "namePrefix": "prefix/"
                        }
                    }
                }
            })
        } else if path.ends_with("/b2_list_file_versions") {
            let parameters = url::form_urlencoded::parse(query.as_bytes())
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(
                parameters.get("bucketId").map(String::as_str),
                Some("bucket-id")
            );
            let prefix = parameters.get("prefix").map(String::as_str).unwrap_or("");
            let start_name = parameters
                .get("startFileName")
                .map(String::as_str)
                .unwrap_or("");
            let start_id = parameters.get("startFileId").map(String::as_str);
            let maximum = parameters
                .get("maxFileCount")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(1000);
            let state = state.lock().unwrap();
            let mut entries = Vec::new();
            for (key, versions) in &state.objects {
                if !key.starts_with(prefix) || key.as_str() < start_name {
                    continue;
                }
                for stored in versions.iter().rev() {
                    entries.push((key, stored));
                }
            }
            if let Some(start_id) = start_id {
                let start = entries
                    .iter()
                    .position(|(key, stored)| *key == start_name && stored.version == start_id)
                    .unwrap_or(entries.len());
                entries.drain(..start);
            }
            let next = entries
                .get(maximum)
                .map(|(key, stored)| ((*key).clone(), stored.version.clone()));
            entries.truncate(maximum);
            let files = entries
                .iter()
                .map(|(key, stored)| {
                    json!({
                        "accountId": "account",
                        "action": "upload",
                        "bucketId": "bucket-id",
                        "contentLength": stored.data.len(),
                        "contentMd5": stored.etag,
                        "contentSha1": hex::encode(Sha1::digest(&stored.data)),
                        "fileId": stored.version,
                        "fileInfo": {"sha256": stored.sha256},
                        "fileName": key,
                    })
                })
                .collect::<Vec<_>>();
            let mut response = json!({"files": files});
            if let Some((key, version)) = next {
                response["nextFileName"] = json!(key);
                response["nextFileId"] = json!(version);
            }
            response
        } else {
            panic!("unexpected native B2 path {path}");
        };
        let body = serde_json::to_vec(&response).unwrap();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    }

    fn generation(root: &Path) {
        fs::create_dir_all(root.join("wal/journal")).unwrap();
        fs::write(root.join("wal/journal/writer.lock"), b"").unwrap();
        fs::write(root.join("wal/journal/segment-0.wal"), b"durable wal").unwrap();
        fs::write(root.join("identity.json"), b"{}\n").unwrap();
        fs::write(root.join("raw-blocks.jsonl"), b"{}\n").unwrap();
    }

    #[test]
    fn walking_is_sorted_and_rejects_links_and_nonregular_entries() {
        let temporary = tempfile::tempdir().unwrap();
        generation(temporary.path());
        let files = walk_generation(temporary.path()).unwrap();
        assert_eq!(
            files
                .iter()
                .map(|value| value.0.as_str())
                .collect::<Vec<_>>(),
            vec![
                "identity.json",
                "raw-blocks.jsonl",
                "wal/journal/segment-0.wal",
                "wal/journal/writer.lock"
            ]
        );
        symlink(
            temporary.path().join("identity.json"),
            temporary.path().join("unsafe"),
        )
        .unwrap();
        assert!(
            walk_generation(temporary.path())
                .unwrap_err()
                .to_string()
                .contains("symlink")
        );
    }

    #[test]
    fn active_writer_lock_and_inside_receipt_fail_before_upload() {
        let temporary = tempfile::tempdir().unwrap();
        generation(temporary.path());
        let lock = File::open(temporary.path().join("wal/journal/writer.lock")).unwrap();
        lock.try_lock_exclusive().unwrap();
        assert!(
            lock_stopped_generation(temporary.path())
                .unwrap_err()
                .to_string()
                .contains("active writer")
        );
        assert!(
            GenerationReceiptTarget::open(
                &temporary.path().join("receipt.json"),
                &DirectoryHandle::open_existing(temporary.path(), "generation").unwrap(),
            )
            .is_err()
        );
        let alias = temporary
            .path()
            .parent()
            .unwrap()
            .join(format!("generation-alias-{}", std::process::id()));
        let _ = fs::remove_file(&alias);
        symlink(temporary.path(), &alias).unwrap();
        assert!(
            GenerationReceiptTarget::open(
                &alias.join("nested/receipt.json"),
                &DirectoryHandle::open_existing(temporary.path(), "generation").unwrap(),
            )
            .is_err()
        );
        fs::remove_file(alias).unwrap();
    }

    #[test]
    fn generation_root_must_remain_private_and_bound_to_the_opened_directory() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("generation");
        generation(&root);
        fs::set_permissions(&root, fs::Permissions::from_mode(0o770)).unwrap();
        let server = StoreServer::start(true);
        let error = upload_generation(
            &server.client(Provider::S3),
            &root,
            "generation-1",
            "prefix/generation-1",
            &temporary.path().join("receipt.json"),
            None,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("effective user"), "{error}");
        assert_eq!(server.put_count(), 0);

        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let handle = DirectoryHandle::open_existing(&root, "generation root").unwrap();
        let moved = temporary.path().join("moved-generation");
        fs::rename(&root, &moved).unwrap();
        generation(&root);
        assert!(handle.verify_path_binding("generation root").is_err());
    }

    #[test]
    fn receipt_publication_is_idempotent_collision_safe_and_symlink_safe() {
        let temporary = tempfile::tempdir().unwrap();
        let generation_path = temporary.path().join("generation");
        let receipts = temporary.path().join("receipts");
        fs::create_dir(&generation_path).unwrap();
        fs::create_dir(&receipts).unwrap();
        let generation = DirectoryHandle::open_existing(&generation_path, "generation").unwrap();
        let receipt_path = receipts.join("receipt.json");
        let value = json!({"generation_id": "generation-1", "verified_unix_secs": 7});

        let target = GenerationReceiptTarget::open(&receipt_path, &generation).unwrap();
        target.publish(value.clone()).unwrap();
        let original = fs::read(&receipt_path).unwrap();

        let retry = GenerationReceiptTarget::open(&receipt_path, &generation).unwrap();
        assert_eq!(retry.publish(value.clone()).unwrap(), value);
        assert_eq!(fs::read(&receipt_path).unwrap(), original);
        assert!(
            retry
                .publish(json!({
                    "generation_id": "different",
                    "verified_unix_secs": 8,
                }))
                .unwrap_err()
                .to_string()
                .contains("collision")
        );
        assert_eq!(fs::read(&receipt_path).unwrap(), original);

        let linked_path = receipts.join("linked.json");
        symlink(&receipt_path, &linked_path).unwrap();
        assert!(GenerationReceiptTarget::open(&linked_path, &generation).is_err());
    }

    #[test]
    fn receipt_parent_replacement_cannot_redirect_or_remove_colliding_temporary() {
        let temporary = tempfile::tempdir().unwrap();
        let generation_path = temporary.path().join("generation");
        let requested = temporary.path().join("receipts");
        let moved = temporary.path().join("receipts-held");
        let replacement = temporary.path().join("replacement");
        fs::create_dir(&generation_path).unwrap();
        fs::create_dir(&requested).unwrap();
        fs::create_dir(&replacement).unwrap();
        let generation = DirectoryHandle::open_existing(&generation_path, "generation").unwrap();
        let target =
            GenerationReceiptTarget::open(&requested.join("receipt.json"), &generation).unwrap();

        fs::rename(&requested, &moved).unwrap();
        symlink(&replacement, &requested).unwrap();
        target
            .publish(json!({"generation_id": "generation-1", "verified_unix_secs": 7}))
            .unwrap();
        assert!(moved.join("receipt.json").is_file());
        assert!(!replacement.join("receipt.json").exists());

        let collision = OsStr::new(".preexisting.tmp");
        fs::write(moved.join(collision), b"custody evidence").unwrap();
        let second =
            GenerationReceiptTarget::open(&moved.join("second.json"), &generation).unwrap();
        assert!(
            second
                .publish_with_temporary(
                    json!({"generation_id": "generation-2", "verified_unix_secs": 8}),
                    collision,
                )
                .is_err()
        );
        assert_eq!(
            fs::read(moved.join(collision)).unwrap(),
            b"custody evidence"
        );
        assert!(!moved.join("second.json").exists());
    }

    #[test]
    fn manifest_and_commit_are_canonical_and_bind_versions() {
        let temporary = tempfile::tempdir().unwrap();
        generation(temporary.path());
        let specs = build_specs(temporary.path(), "prefix").unwrap();
        let versions = specs
            .iter()
            .map(|spec| (spec.record.path.clone(), "version".into()))
            .collect();
        let manifest = generation_manifest("generation-1", &specs, &versions, 15, None).unwrap();
        let bytes = canonical_json_bytes(&manifest).unwrap();
        assert!(bytes.ends_with(b"\n"));
        assert!(
            std::str::from_utf8(&bytes)
                .unwrap()
                .contains("\"version_id\":\"version\"")
        );
        let digest = hex::encode(Sha256::digest(&bytes));
        let commit = generation_commit(
            "generation-1",
            "prefix/manifest.json",
            &digest,
            "manifest-version",
            specs.len(),
            15,
            None,
        )
        .unwrap();
        assert_eq!(commit["manifest_sha256"], digest);
    }

    #[test]
    fn r2_upload_is_conditional_commit_last_and_retry_is_read_only() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("generation");
        generation(&root);
        let server = StoreServer::start(false);
        assert!(!server.versioned);
        let receipt_path = temporary.path().join("receipt.json");
        let first = upload_generation(
            &server.client(Provider::R2),
            &root,
            "generation-1",
            "prefix/generation-1",
            &receipt_path,
            None,
            None,
        )
        .unwrap();
        assert_eq!(first["storage_provider"], "r2");
        assert_eq!(first["object_identity"], "single-put-etag");
        let put_keys = server.put_keys();
        assert_eq!(put_keys.last().unwrap(), "prefix/generation-1/_COMMITTED");
        assert_eq!(
            put_keys[put_keys.len() - 2],
            "prefix/generation-1/manifest.json"
        );
        let commit: Value =
            serde_json::from_slice(&server.object("prefix/generation-1/_COMMITTED")).unwrap();
        assert_eq!(commit["manifest_sha256"], first["manifest_sha256"]);
        let receipt_disk: Value =
            serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        assert_eq!(receipt_disk, first);
        let puts = server.put_count();
        let retried = upload_generation(
            &server.client(Provider::R2),
            &root,
            "generation-1",
            "prefix/generation-1",
            &receipt_path,
            None,
            None,
        )
        .unwrap();
        assert_eq!(server.put_count(), puts);
        assert_eq!(retried["commit_version_id"], first["commit_version_id"]);
    }

    #[test]
    fn r2_collision_fails_before_any_remote_write() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("generation");
        generation(&root);
        let server = StoreServer::start(false);
        server.inject(
            "prefix/generation-1/files/identity.json",
            b"conflicting identity",
        );
        let error = upload_generation(
            &server.client(Provider::R2),
            &root,
            "generation-1",
            "prefix/generation-1",
            &temporary.path().join("receipt.json"),
            None,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("collision"), "{error}");
        assert_eq!(server.put_count(), 0);
    }

    #[test]
    fn versioned_s3_upload_pins_versions_and_is_idempotent() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("generation");
        generation(&root);
        let server = StoreServer::start(true);
        assert!(server.versioned);
        let receipt_path = temporary.path().join("receipt.json");
        let first = upload_generation(
            &server.client(Provider::S3),
            &root,
            "generation-1",
            "prefix/generation-1",
            &receipt_path,
            None,
            None,
        )
        .unwrap();
        assert!(
            first["manifest_version_id"]
                .as_str()
                .unwrap()
                .starts_with("version-")
        );
        assert_eq!(
            server.put_keys().last().unwrap(),
            "prefix/generation-1/_COMMITTED"
        );
        let puts = server.put_count();
        let second = upload_generation(
            &server.client(Provider::S3),
            &root,
            "generation-1",
            "prefix/generation-1",
            &receipt_path,
            None,
            None,
        )
        .unwrap();
        assert_eq!(server.put_count(), puts);
        assert_eq!(second["manifest_version_id"], first["manifest_version_id"]);
        assert_eq!(second["commit_version_id"], first["commit_version_id"]);
    }

    #[test]
    fn native_b2_upload_commits_last_pins_versions_and_retries_read_only() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("generation");
        generation(&root);
        let server = StoreServer::start(true);
        let client = server.client(Provider::B2);
        let receipt_path = temporary.path().join("receipt.json");
        let mut verifier = server.native_verifier("prefix/");

        let first = upload_generation(
            &client,
            &root,
            "generation-1",
            "prefix/generation-1",
            &receipt_path,
            None,
            Some(&mut verifier),
        )
        .unwrap();
        let put_keys = server.put_keys();
        assert_eq!(put_keys.last().unwrap(), "prefix/generation-1/_COMMITTED");
        assert_eq!(
            put_keys[put_keys.len() - 2],
            "prefix/generation-1/manifest.json"
        );
        assert!(
            first["manifest_version_id"]
                .as_str()
                .unwrap()
                .starts_with("version-")
        );
        assert!(
            first["commit_version_id"]
                .as_str()
                .unwrap()
                .starts_with("version-")
        );
        let manifest: Value =
            serde_json::from_slice(&server.object("prefix/generation-1/manifest.json")).unwrap();
        assert!(manifest["files"].as_array().unwrap().iter().all(|file| {
            file["version_id"]
                .as_str()
                .is_some_and(|version| version.starts_with("version-"))
        }));
        let writes = server.put_count();

        let second = upload_generation(
            &client,
            &root,
            "generation-1",
            "prefix/generation-1",
            &receipt_path,
            None,
            Some(&mut verifier),
        )
        .unwrap();
        assert_eq!(server.put_count(), writes);
        assert_eq!(second["manifest_version_id"], first["manifest_version_id"]);
        assert_eq!(second["commit_version_id"], first["commit_version_id"]);
    }

    #[test]
    fn native_b2_final_snapshot_rejects_a_competing_commit_version() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("generation");
        generation(&root);
        let server = StoreServer::start(true);
        server.conflict_after_put("prefix/generation-1/_COMMITTED");
        let client = server.client(Provider::B2);
        let receipt_path = temporary.path().join("receipt.json");
        let mut verifier = server.native_verifier("prefix/");

        let error = upload_generation(
            &client,
            &root,
            "generation-1",
            "prefix/generation-1",
            &receipt_path,
            None,
            Some(&mut verifier),
        )
        .unwrap_err();
        assert!(error.to_string().contains("conflicting"), "{error}");
        assert_eq!(
            server.put_keys().last().unwrap(),
            "prefix/generation-1/_COMMITTED"
        );
        assert!(!receipt_path.exists());
    }

    #[test]
    fn versioned_s3_full_get_corruption_blocks_receipt() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("generation");
        generation(&root);
        let server = StoreServer::start(true);
        server.corrupt_get("prefix/generation-1/files/identity.json");
        let receipt = temporary.path().join("receipt.json");
        let error = upload_generation(
            &server.client(Provider::S3),
            &root,
            "generation-1",
            "prefix/generation-1",
            &receipt,
            None,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("SHA-256 mismatch"), "{error}");
        assert!(!receipt.exists());
    }

    #[test]
    fn versioned_s3_post_commit_sweep_detects_deleted_payload_or_manifest() {
        for victim in [
            "prefix/generation-1/files/identity.json",
            "prefix/generation-1/manifest.json",
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let root = temporary.path().join("generation");
            generation(&root);
            let server = StoreServer::start(true);
            server.delete_during_put("prefix/generation-1/_COMMITTED", victim);
            let receipt = temporary.path().join("receipt.json");
            let error = upload_generation(
                &server.client(Provider::S3),
                &root,
                "generation-1",
                "prefix/generation-1",
                &receipt,
                None,
                None,
            )
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("pinned object version is missing")
            );
            assert_eq!(
                server.put_keys().last().map(String::as_str),
                Some("prefix/generation-1/_COMMITTED")
            );
            assert!(!receipt.exists());
        }
    }

    #[test]
    fn versioned_transport_retry_accepts_only_identical_duplicate_versions() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("generation");
        generation(&root);
        let server = StoreServer::start(true);
        server.drop_first_put_response();
        let receipt = upload_generation(
            &server.client_with_retries(Provider::S3, 1),
            &root,
            "generation-1",
            "prefix/generation-1",
            &temporary.path().join("receipt.json"),
            None,
            None,
        )
        .unwrap();
        assert!(receipt["commit_version_id"].is_string());
        let state = server.state.lock().unwrap();
        let versions = &state.objects["prefix/generation-1/files/identity.json"];
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].data, versions[1].data);
        assert_eq!(versions[0].sha256, versions[1].sha256);
    }
}
