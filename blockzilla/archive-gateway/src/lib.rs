use anyhow::{Context, Result, anyhow, bail, ensure};
use async_stream::stream;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path as AxumPath, State},
    http::{
        HeaderMap, HeaderValue, Method, Request, Response, StatusCode,
        header::{
            ACCEPT_RANGES, AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE,
            CONTENT_TYPE, ETAG, IF_NONE_MATCH, RANGE, WWW_AUTHENTICATE,
        },
    },
    middleware::{self, Next},
    response::IntoResponse,
    routing::get,
};
use blockzilla_read_sdk::manifest::{
    GENERATION_MANIFEST_FILE, GENERATION_MANIFEST_SCHEMA_VERSION, GENESIS_BIN_FILE, GenerationFile,
    GenerationManifest, REGISTRY_FILE, REGISTRY_INDEX_FILE, REQUIRED_GENERATION_FILES,
    SIGNATURES_FILE, compute_generation_digest,
};
use blockzilla_read_sdk::{
    ARCHIVE_V2_PUBLICATION_LOCK_FILE, ArchiveReader, ArchiveV2MetadataWireProfile,
    ArchiveV2WireProfile, AuditedCurrentMetadataMarkerPublication,
    CURRENT_TYPED_ERRORS_MARKER_BYTES, CURRENT_TYPED_ERRORS_MARKER_FILE,
    CURRENT_TYPED_ERRORS_MARKER_SHA256, CURRENT_TYPED_ERRORS_MARKER_SIZE, HashVerification,
    OpenOptions as ReaderOpenOptions, PinnedLocalRangeSource, RangeSource, SourceError,
    SourceResult, UnprovenWireProfileDecision, acquire_archive_v2_publication_lock,
    audit_current_metadata_for_marker_publication, audit_full_generation_wire_profile,
    validate_manifest_bound_pinned_local_registry_index, wire_profile_marker,
    wire_profile_marker_bytes,
};
use bytes::Bytes;
use serde::Serialize;
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions as FsOpenOptions},
    io::{self, Read, Seek, SeekFrom as StdSeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    sync::Semaphore,
};
use tokio_util::io::ReaderStream;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{limit::RequestBodyLimitLayer, trace::TraceLayer};

const MAX_MANIFEST_BYTES: u64 = 1 << 20;
const MAX_SLOTS_PER_EPOCH: u64 = 1_000_000;
const MAX_PROFILE_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct Catalog {
    archives: BTreeMap<u64, Arc<ArchiveGeneration>>,
}

impl Catalog {
    pub fn len(&self) -> usize {
        self.archives.len()
    }

    pub fn is_empty(&self) -> bool {
        self.archives.is_empty()
    }
}

#[derive(Clone)]
struct ArchiveGeneration {
    root: PathBuf,
    manifest: GenerationManifest,
    manifest_bytes: Bytes,
    files: BTreeMap<String, PublishedFile>,
}

#[derive(Clone)]
struct PublishedFile {
    manifest: GenerationFile,
    identity: FileIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileIdentity {
    len: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
        }
    }
}

pub struct GatewayConfig {
    pub catalog: Catalog,
    pub bearer_token: Option<String>,
    pub max_range_bytes: u64,
    pub max_concurrent_downloads: usize,
    pub max_request_body_bytes: usize,
}

#[derive(Clone)]
struct AppState {
    catalog: Catalog,
    max_range_bytes: u64,
    downloads: Arc<Semaphore>,
}

#[derive(Clone)]
enum AuthPolicy {
    Disabled,
    Required([u8; 32]),
}

#[derive(Debug, Clone)]
pub struct GenerateManifestOptions {
    pub archive_dir: PathBuf,
    pub cluster_id: String,
    pub epoch: u64,
    pub generation_id: String,
    pub slots_per_epoch: u64,
    pub wire_profile: ArchiveV2WireProfile,
    pub additional_files: Vec<String>,
    pub output: Option<PathBuf>,
}

#[derive(Serialize)]
struct CatalogResponse {
    schema_version: u32,
    archives: Vec<CatalogEntry>,
}

#[derive(Serialize)]
struct CatalogEntry {
    cluster_id: String,
    epoch: u64,
    generation_id: String,
    generation_digest: String,
    slots_per_epoch: u64,
    manifest_url: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: &'static str,
}

/// Load and structurally validate every explicitly configured generation.
///
/// Startup hashes every manifest object, checks the canonical marker bytes,
/// and runs the bounded full-generation message-profile audit. The configured
/// path must still be an immutable snapshot for the server lifetime.
pub fn load_catalog(archive_dirs: &[PathBuf]) -> Result<Catalog> {
    ensure!(
        !archive_dirs.is_empty(),
        "no archive directories configured"
    );
    let mut archives = BTreeMap::new();
    for configured_root in archive_dirs {
        let generation = Arc::new(load_generation(configured_root)?);
        let epoch = generation.manifest.epoch;
        if archives.insert(epoch, generation).is_some() {
            bail!("more than one archive directory is configured for epoch {epoch}");
        }
    }
    Ok(Catalog { archives })
}

fn load_generation(configured_root: &Path) -> Result<ArchiveGeneration> {
    let configured_metadata = std::fs::symlink_metadata(configured_root)
        .with_context(|| format!("stat archive directory {}", configured_root.display()))?;
    ensure!(
        configured_metadata.is_dir() && !configured_metadata.file_type().is_symlink(),
        "archive directory must be a real directory, not a symlink: {}",
        configured_root.display()
    );
    let root = configured_root.canonicalize().with_context(|| {
        format!(
            "canonicalize archive directory {}",
            configured_root.display()
        )
    })?;
    let manifest_path = root.join(GENERATION_MANIFEST_FILE);
    let (manifest_bytes, _) = read_regular_file_bounded(&manifest_path, MAX_MANIFEST_BYTES)
        .with_context(|| format!("read generation manifest in {}", root.display()))?;
    let manifest = GenerationManifest::parse(&manifest_bytes)
        .map_err(|error| anyhow!(error))
        .with_context(|| format!("validate {}", manifest_path.display()))?;
    ensure!(
        manifest.complete,
        "refusing incomplete generation manifest for epoch {}",
        manifest.epoch
    );
    ensure!(
        manifest.slots_per_epoch <= MAX_SLOTS_PER_EPOCH,
        "slots_per_epoch {} exceeds gateway safety limit {MAX_SLOTS_PER_EPOCH}",
        manifest.slots_per_epoch
    );
    for required in REQUIRED_GENERATION_FILES {
        manifest
            .required_file(required)
            .map_err(|error| anyhow!(error))?;
    }
    manifest
        .required_file(REGISTRY_INDEX_FILE)
        .map_err(|error| anyhow!(error))?;
    ensure!(
        manifest.file(GENERATION_MANIFEST_FILE).is_none(),
        "manifest must not publish itself"
    );
    let wire_profile =
        ArchiveV2WireProfile::for_published_manifest(&manifest).map_err(|error| anyhow!(error))?;
    let profile_marker = wire_profile_marker(wire_profile);
    let expected_marker_bytes = wire_profile_marker_bytes(wire_profile);

    let mut files = BTreeMap::new();
    for entry in &manifest.files {
        let path = root.join(&entry.name);
        let file = open_regular_nofollow(&path)
            .with_context(|| format!("open published file {}", entry.name))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("stat published file {}", entry.name))?;
        ensure!(
            metadata.len() == entry.size,
            "published file {} has size {}, manifest says {}",
            entry.name,
            metadata.len(),
            entry.size
        );
        if entry.name == profile_marker.name {
            let (actual, _) = read_regular_file_bounded(&path, entry.size)
                .with_context(|| format!("read wire-profile marker {}", entry.name))?;
            ensure!(
                actual == expected_marker_bytes,
                "wire-profile marker {} does not contain its canonical bytes",
                entry.name
            );
        }
        if entry.name == CURRENT_TYPED_ERRORS_MARKER_FILE {
            let (actual, _) = read_regular_file_bounded(&path, entry.size)
                .with_context(|| format!("read metadata-schema marker {}", entry.name))?;
            ensure!(
                actual == CURRENT_TYPED_ERRORS_MARKER_BYTES,
                "metadata-schema marker {} does not contain its canonical bytes",
                entry.name
            );
        }
        files.insert(
            entry.name.clone(),
            PublishedFile {
                manifest: entry.clone(),
                identity: FileIdentity::from_metadata(&metadata),
            },
        );
    }

    validate_required_archive(&root, &manifest, &files)
        .with_context(|| format!("validate Archive V2 generation epoch {}", manifest.epoch))?;
    for (name, published) in &files {
        let file = open_regular_nofollow(&root.join(name))
            .with_context(|| format!("reopen validated file {name}"))?;
        ensure!(
            FileIdentity::from_metadata(&file.metadata()?) == published.identity,
            "published file {name} changed during startup validation"
        );
    }

    Ok(ArchiveGeneration {
        root,
        manifest,
        manifest_bytes: Bytes::from(manifest_bytes),
        files,
    })
}

fn validate_required_archive(
    root: &Path,
    manifest: &GenerationManifest,
    files: &BTreeMap<String, PublishedFile>,
) -> Result<()> {
    for required in REQUIRED_GENERATION_FILES {
        ensure!(
            files.contains_key(required),
            "required file {required} was not opened"
        );
    }
    ensure!(
        files.contains_key(REGISTRY_INDEX_FILE),
        "required canonical registry index {REGISTRY_INDEX_FILE} was not opened"
    );
    let registry_size = manifest
        .required_file(REGISTRY_FILE)
        .map_err(|e| anyhow!(e))?
        .size;
    ensure!(
        registry_size > 0 && registry_size % 32 == 0,
        "{REGISTRY_FILE} must be a non-empty sequence of 32-byte pubkeys"
    );
    validate_archive_structure(root, manifest)?;
    Ok(())
}

fn validate_archive_structure(root: &Path, manifest: &GenerationManifest) -> Result<()> {
    let source = PinnedLocalRangeSource::new(root);
    let options = ReaderOpenOptions {
        hash_verification: HashVerification::AllFiles,
        ..ReaderOpenOptions::default()
    };
    let reader = ArchiveReader::open_candidate(source.clone(), manifest.clone(), options)
        .map_err(|error| anyhow!(error))?;
    validate_manifest_bound_pinned_local_registry_index(&source, manifest)
        .map_err(|error| anyhow!(error))?;
    ensure!(
        !reader.index().rows.is_empty(),
        "hot-block index has no rows"
    );
    require_canonical_unproven_profile(&reader)?;
    source.verify_unchanged().map_err(|error| anyhow!(error))?;
    Ok(())
}

fn audit_archive_structure_before_metadata_marker(
    files: PinnedLocalRangeSource,
    manifest: &GenerationManifest,
    message_marker_name: &str,
    message_marker_bytes: &'static [u8],
) -> Result<AuditedCurrentMetadataMarkerPublication> {
    validate_manifest_bound_pinned_local_registry_index(&files, manifest)
        .map_err(|error| anyhow!(error))?;
    let source = StagedMarkerRangeSource {
        files,
        first_marker: (message_marker_name, message_marker_bytes),
        second_marker: None,
    };
    let options = ReaderOpenOptions {
        hash_verification: HashVerification::SizesOnly,
        ..ReaderOpenOptions::default()
    };
    let reader = ArchiveReader::open_candidate_with_metadata_admission(
        source,
        manifest.clone(),
        options,
        blockzilla_read_sdk::ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
    )
    .map_err(|error| anyhow!(error))?;
    ensure!(
        !reader.index().rows.is_empty(),
        "hot-block index has no rows"
    );
    audit_candidate_for_current_metadata_publication(&reader)
}

fn validate_archive_structure_with_staged_markers(
    files: PinnedLocalRangeSource,
    manifest: &GenerationManifest,
    message_marker_name: &str,
    message_marker_bytes: &'static [u8],
) -> Result<()> {
    let source = StagedMarkerRangeSource {
        files,
        first_marker: (message_marker_name, message_marker_bytes),
        second_marker: Some((
            CURRENT_TYPED_ERRORS_MARKER_FILE,
            CURRENT_TYPED_ERRORS_MARKER_BYTES,
        )),
    };
    let options = ReaderOpenOptions {
        hash_verification: HashVerification::SizesOnly,
        ..ReaderOpenOptions::default()
    };
    let reader = ArchiveReader::open_candidate(source, manifest.clone(), options)
        .map_err(|error| anyhow!(error))?;
    ensure!(
        reader.metadata_wire_profile() == ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1,
        "final candidate did not bind the current typed-error metadata profile"
    );
    Ok(())
}

fn require_canonical_unproven_profile<S: RangeSource>(
    reader: &ArchiveReader<S>,
) -> Result<AuditedCurrentMetadataMarkerPublication> {
    ensure!(
        reader.metadata_wire_profile() == ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1,
        "gateway generations must use the current typed-error metadata profile"
    );
    audit_candidate_for_current_metadata_publication(reader)
}

fn audit_candidate_for_current_metadata_publication<S: RangeSource>(
    reader: &ArchiveReader<S>,
) -> Result<AuditedCurrentMetadataMarkerPublication> {
    let decision = audit_full_generation_wire_profile(reader, MAX_PROFILE_MESSAGE_BYTES)
        .map_err(|error| anyhow!(error))?
        .require_unproven_authority()
        .map_err(|error| anyhow!(error))?;
    if decision == UnprovenWireProfileDecision::AllSemanticallyEquivalent {
        ensure!(
            reader.wire_profile() == ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            "an all-equivalent generation must use the canonical post-fallback wire profile"
        );
    }
    audit_current_metadata_for_marker_publication(reader).map_err(|error| anyhow!(error))
}

/// During offline validation, expose the SDK-owned marker bytes from memory.
/// The durable marker is published only after every archive check succeeds.
struct StagedMarkerRangeSource<'a, S> {
    files: S,
    first_marker: (&'a str, &'static [u8]),
    second_marker: Option<(&'a str, &'static [u8])>,
}

impl<S: RangeSource> RangeSource for StagedMarkerRangeSource<'_, S> {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        if let Some(bytes) = self.marker_bytes(object) {
            return Ok(Some(bytes.len() as u64));
        }
        self.files.size(object)
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        let Some(marker_bytes) = self.marker_bytes(object) else {
            return self.files.read_range(object, offset, length);
        };
        let size = marker_bytes.len() as u64;
        let length_u64 = u64::try_from(length).map_err(|_| SourceError::OutOfBounds {
            object: object.to_owned(),
            offset,
            length,
            size,
        })?;
        let end = offset
            .checked_add(length_u64)
            .ok_or_else(|| SourceError::OutOfBounds {
                object: object.to_owned(),
                offset,
                length,
                size,
            })?;
        if end > size {
            return Err(SourceError::OutOfBounds {
                object: object.to_owned(),
                offset,
                length,
                size,
            });
        }
        Ok(marker_bytes[offset as usize..end as usize].to_vec())
    }
}

impl<S> StagedMarkerRangeSource<'_, S> {
    fn marker_bytes(&self, object: &str) -> Option<&'static [u8]> {
        if object == self.first_marker.0 {
            return Some(self.first_marker.1);
        }
        self.second_marker
            .filter(|(name, _)| object == *name)
            .map(|(_, bytes)| bytes)
    }
}

fn is_safe_object_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name
            .bytes()
            .any(|byte| byte == 0 || byte.is_ascii_control())
}

pub fn build_router(config: Arc<GatewayConfig>) -> Result<Router> {
    ensure!(!config.catalog.is_empty(), "catalog is empty");
    ensure!(config.max_range_bytes > 0, "max_range_bytes is zero");
    ensure!(
        config.max_concurrent_downloads > 0,
        "max_concurrent_downloads is zero"
    );
    if let Some(token) = &config.bearer_token {
        ensure!(
            !token.is_empty() && !token.bytes().any(|byte| byte.is_ascii_control()),
            "bearer token is empty or contains a control character"
        );
    }
    let auth = config
        .bearer_token
        .as_ref()
        .map(|token| {
            let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
            AuthPolicy::Required(digest)
        })
        .unwrap_or(AuthPolicy::Disabled);
    let state = AppState {
        catalog: config.catalog.clone(),
        max_range_bytes: config.max_range_bytes,
        downloads: Arc::new(Semaphore::new(config.max_concurrent_downloads)),
    };

    let protected = Router::new()
        .route("/v1/catalog", get(catalog_handler))
        .route("/v1/epochs/{epoch}/manifest", get(manifest_handler))
        .route("/v1/epochs/{epoch}/files/{name}", get(file_handler))
        .layer(middleware::from_fn_with_state(auth, authorize));

    Ok(Router::new()
        .route("/healthz", get(health_handler))
        .merge(protected)
        .with_state(state)
        .layer(RequestBodyLimitLayer::new(config.max_request_body_bytes))
        .layer(ConcurrencyLimitLayer::new(
            config.max_concurrent_downloads.saturating_mul(4).max(16),
        ))
        .layer(TraceLayer::new_for_http()))
}

async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn catalog_handler(State(state): State<AppState>) -> impl IntoResponse {
    let archives = state
        .catalog
        .archives
        .values()
        .map(|generation| CatalogEntry {
            cluster_id: generation.manifest.cluster_id.clone(),
            epoch: generation.manifest.epoch,
            generation_id: generation.manifest.generation_id.clone(),
            generation_digest: generation.manifest.generation_digest.clone(),
            slots_per_epoch: generation.manifest.slots_per_epoch,
            manifest_url: format!("/v1/epochs/{}/manifest", generation.manifest.epoch),
        })
        .collect();
    let mut response = Json(CatalogResponse {
        schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
        archives,
    })
    .into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn manifest_handler(
    State(state): State<AppState>,
    AxumPath(epoch): AxumPath<u64>,
    headers: HeaderMap,
) -> Response<Body> {
    let Some(generation) = state.catalog.archives.get(&epoch) else {
        return api_error(StatusCode::NOT_FOUND, "epoch is not published");
    };
    let etag = format!("\"generation-{}\"", generation.manifest.generation_digest);
    if if_none_match(&headers, &etag) {
        return not_modified(&etag, "private, max-age=0, must-revalidate");
    }
    response_with_bytes(
        StatusCode::OK,
        generation.manifest_bytes.clone(),
        "application/json",
        &etag,
        "private, max-age=0, must-revalidate",
    )
}

async fn file_handler(
    State(state): State<AppState>,
    AxumPath((epoch, name)): AxumPath<(u64, String)>,
    method: Method,
    headers: HeaderMap,
) -> Response<Body> {
    let Some(generation) = state.catalog.archives.get(&epoch) else {
        return api_error(StatusCode::NOT_FOUND, "epoch is not published");
    };
    let Some(published) = generation.files.get(&name) else {
        return api_error(StatusCode::NOT_FOUND, "file is not published");
    };
    let etag = format!("\"sha256-{}\"", published.manifest.sha256);
    if if_none_match(&headers, &etag) {
        return not_modified(&etag, "private, max-age=31536000, immutable");
    }
    let size = published.manifest.size;
    let requested_range = match headers.get(RANGE) {
        Some(value) => match parse_single_range(value, size, state.max_range_bytes) {
            Ok(range) => Some(range),
            Err(()) => return range_not_satisfiable(size),
        },
        None => None,
    };
    let (status, start, length) = match requested_range {
        Some(range) => (StatusCode::PARTIAL_CONTENT, range.start, range.length),
        None => (StatusCode::OK, 0, size),
    };

    if method == Method::HEAD {
        return file_response_headers(status, size, start, length, &etag, Body::empty());
    }

    let permit = match state.downloads.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "download concurrency limit reached",
            );
        }
    };
    let path = generation.root.join(&name);
    let expected_identity = published.identity.clone();
    let opened = tokio::task::spawn_blocking(move || -> Result<File> {
        let file = open_regular_nofollow(&path)?;
        let actual = FileIdentity::from_metadata(&file.metadata()?);
        ensure!(
            actual == expected_identity,
            "published file identity changed after startup"
        );
        Ok(file)
    })
    .await;
    let file = match opened {
        Ok(Ok(file)) => file,
        _ => {
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "published file changed or is unavailable",
            );
        }
    };
    let mut file = tokio::fs::File::from_std(file);
    if start != 0 && file.seek(SeekFrom::Start(start)).await.is_err() {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "cannot seek published file",
        );
    }
    let stream = stream! {
        let _permit = permit;
        let mut reader = ReaderStream::new(file.take(length));
        while let Some(chunk) = stream_next(&mut reader).await {
            yield chunk;
        }
    };
    file_response_headers(
        status,
        size,
        start,
        length,
        &etag,
        Body::from_stream(stream),
    )
}

async fn stream_next<S: futures_core::Stream + Unpin>(stream: &mut S) -> Option<S::Item> {
    std::future::poll_fn(|cx| std::pin::Pin::new(&mut *stream).poll_next(cx)).await
}

async fn authorize(
    State(policy): State<AuthPolicy>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let authorized = match policy {
        AuthPolicy::Disabled => true,
        AuthPolicy::Required(expected) => request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split_once(' '))
            .filter(|(scheme, token)| scheme.eq_ignore_ascii_case("Bearer") && !token.is_empty())
            .map(|(_, token)| {
                let actual: [u8; 32] = Sha256::digest(token.as_bytes()).into();
                bool::from(actual.ct_eq(&expected))
            })
            .unwrap_or(false),
    };
    if authorized {
        next.run(request).await
    } else {
        let mut response = api_error(StatusCode::UNAUTHORIZED, "bearer token required");
        response.headers_mut().insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"blockzilla-archive\""),
        );
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteRange {
    start: u64,
    length: u64,
}

fn parse_single_range(
    value: &HeaderValue,
    size: u64,
    max_len: u64,
) -> std::result::Result<ByteRange, ()> {
    let value = value.to_str().map_err(|_| ())?;
    let spec = value.strip_prefix("bytes=").ok_or(())?;
    if spec.is_empty() || spec.contains(',') || spec.chars().any(char::is_whitespace) {
        return Err(());
    }
    let (start, end) = spec.split_once('-').ok_or(())?;
    if size == 0 {
        return Err(());
    }
    let (start, end) = if start.is_empty() {
        let suffix = end.parse::<u64>().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        let length = suffix.min(size);
        (size - length, size - 1)
    } else {
        let start = start.parse::<u64>().map_err(|_| ())?;
        if start >= size {
            return Err(());
        }
        let end = if end.is_empty() {
            size - 1
        } else {
            end.parse::<u64>().map_err(|_| ())?.min(size - 1)
        };
        if end < start {
            return Err(());
        }
        (start, end)
    };
    let length = end
        .checked_sub(start)
        .and_then(|n| n.checked_add(1))
        .ok_or(())?;
    if length > max_len {
        return Err(());
    }
    Ok(ByteRange { start, length })
}

fn if_none_match(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value.split(',').any(|candidate| {
                let candidate = candidate.trim();
                candidate == "*" || candidate == etag || candidate.strip_prefix("W/") == Some(etag)
            })
        })
        .unwrap_or(false)
}

fn file_response_headers(
    status: StatusCode,
    full_size: u64,
    start: u64,
    length: u64,
    etag: &str,
    body: Body,
) -> Response<Body> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=31536000, immutable"),
    );
    insert_header(headers, ETAG, etag);
    insert_header(headers, CONTENT_LENGTH, &length.to_string());
    if status == StatusCode::PARTIAL_CONTENT {
        let end = start + length - 1;
        insert_header(
            headers,
            CONTENT_RANGE,
            &format!("bytes {start}-{end}/{full_size}"),
        );
    }
    response
}

fn response_with_bytes(
    status: StatusCode,
    bytes: Bytes,
    content_type: &'static str,
    etag: &str,
    cache_control: &'static str,
) -> Response<Body> {
    let len = bytes.len();
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static(cache_control));
    insert_header(response.headers_mut(), ETAG, etag);
    insert_header(response.headers_mut(), CONTENT_LENGTH, &len.to_string());
    response
}

fn not_modified(etag: &str, cache_control: &'static str) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NOT_MODIFIED;
    insert_header(response.headers_mut(), ETAG, etag);
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static(cache_control));
    response
}

fn range_not_satisfiable(size: u64) -> Response<Body> {
    let mut response = api_error(
        StatusCode::RANGE_NOT_SATISFIABLE,
        "range is not satisfiable",
    );
    insert_header(
        response.headers_mut(),
        CONTENT_RANGE,
        &format!("bytes */{size}"),
    );
    response
}

fn api_error(status: StatusCode, message: &'static str) -> Response<Body> {
    (status, Json(ErrorResponse { error: message })).into_response()
}

fn insert_header(headers: &mut HeaderMap, name: http::header::HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

pub fn generate_manifest(options: GenerateManifestOptions) -> Result<PathBuf> {
    ensure!(
        (1..=MAX_SLOTS_PER_EPOCH).contains(&options.slots_per_epoch),
        "slots_per_epoch must be between 1 and {MAX_SLOTS_PER_EPOCH}"
    );
    let configured_metadata = std::fs::symlink_metadata(&options.archive_dir)
        .with_context(|| format!("stat archive directory {}", options.archive_dir.display()))?;
    ensure!(
        configured_metadata.is_dir() && !configured_metadata.file_type().is_symlink(),
        "archive directory must be a real directory, not a symlink"
    );
    let root = options.archive_dir.canonicalize()?;
    let output = options
        .output
        .unwrap_or_else(|| root.join(GENERATION_MANIFEST_FILE));
    let output_parent = output
        .parent()
        .context("manifest output has no parent")?
        .canonicalize()
        .context("canonicalize manifest output parent")?;
    ensure!(
        output_parent == root,
        "manifest output must be inside archive_dir"
    );
    ensure!(
        output.file_name().and_then(|name| name.to_str()) == Some(GENERATION_MANIFEST_FILE),
        "manifest output basename must be {GENERATION_MANIFEST_FILE}"
    );
    ensure!(
        !output.exists(),
        "refusing to overwrite {}",
        output.display()
    );

    let profile_marker = wire_profile_marker(options.wire_profile);
    let marker_bytes = wire_profile_marker_bytes(options.wire_profile);
    let pinned_source = PinnedLocalRangeSource::open_directory(&root)
        .map_err(|error| anyhow!(error))
        .context("open descriptor-pinned archive directory")?;

    let mut names = BTreeSet::from(REQUIRED_GENERATION_FILES.map(str::to_owned));
    names.insert(REGISTRY_INDEX_FILE.to_owned());
    let signatures_path = root.join(SIGNATURES_FILE);
    if signatures_path.try_exists()? {
        names.insert(SIGNATURES_FILE.to_owned());
    }
    let genesis_path = root.join(GENESIS_BIN_FILE);
    if options.epoch == 0 && genesis_path.try_exists()? {
        names.insert(GENESIS_BIN_FILE.to_owned());
    }
    for name in options.additional_files {
        ensure!(
            is_safe_object_name(&name),
            "additional file must be one safe non-control path component: {name:?}"
        );
        ensure!(
            !is_wire_profile_marker_name(&name) && !is_metadata_profile_marker_name(&name),
            "Archive V2 profile marker names are reserved; select profiles through the producer audit: {name:?}"
        );
        ensure!(
            name != ARCHIVE_V2_PUBLICATION_LOCK_FILE,
            "manifest publication lock name is reserved: {name:?}"
        );
        names.insert(name);
    }
    reject_conflicting_wire_profile_marker(&root, options.wire_profile)?;
    validate_selected_wire_profile_marker_if_present(&root, options.wire_profile)?;
    validate_current_metadata_marker_if_present(&root)?;
    names.insert(profile_marker.name.clone());
    ensure!(
        !names.contains(GENERATION_MANIFEST_FILE),
        "manifest cannot publish itself"
    );

    let mut files = Vec::with_capacity(names.len());
    let mut hashed_identities = BTreeMap::new();
    for name in names {
        if name == profile_marker.name {
            // The message marker is staged in memory until all audits pass.
            files.push(profile_marker.clone());
            continue;
        }
        let file = pinned_source
            .open_file(&name)
            .map_err(|error| anyhow!(error))
            .with_context(|| format!("open manifest input {name}"))?;
        let metadata = file.metadata()?;
        let identity = FileIdentity::from_metadata(&metadata);
        let sha256 = hash_file(file).with_context(|| format!("hash {name}"))?;
        hashed_identities.insert(name.clone(), identity);
        files.push(GenerationFile {
            name,
            size: metadata.len(),
            sha256,
        });
    }

    let mut manifest = GenerationManifest {
        schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
        cluster_id: options.cluster_id,
        epoch: options.epoch,
        generation_id: options.generation_id,
        generation_digest: "0".repeat(64),
        slots_per_epoch: options.slots_per_epoch,
        complete: true,
        files,
    };
    manifest.generation_digest =
        compute_generation_digest(&manifest).map_err(|error| anyhow!(error))?;
    manifest.validate().map_err(|error| anyhow!(error))?;
    let metadata_publication = audit_archive_structure_before_metadata_marker(
        pinned_source.clone(),
        &manifest,
        &profile_marker.name,
        marker_bytes,
    )?;
    let metadata_marker = metadata_publication.marker_manifest_entry();
    manifest.files.push(metadata_marker.clone());
    manifest.generation_digest =
        compute_generation_digest(&manifest).map_err(|error| anyhow!(error))?;
    manifest.validate().map_err(|error| anyhow!(error))?;
    validate_archive_structure_with_staged_markers(
        pinned_source.clone(),
        &manifest,
        &profile_marker.name,
        marker_bytes,
    )?;

    // Serialize before the publication lock. No durable profile decision has
    // been created yet.
    let mut bytes = serde_json::to_vec_pretty(&manifest)?;
    bytes.push(b'\n');

    // Serialize marker selection and manifest publication across concurrent
    // producers. The lock file is not a published generation object.
    let publish_lock =
        acquire_archive_v2_publication_lock(&root).map_err(|error| anyhow!(error))?;

    pinned_source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("archive inputs changed during manifest validation")?;

    // The manifest is the publication boundary. Under the publication lock,
    // hash every pinned input again and then reopen its path. This proves that
    // the bytes and identity still match the candidate manifest immediately
    // before its control files become visible.
    for entry in &manifest.files {
        if entry.name == profile_marker.name || entry.name == metadata_marker.name {
            continue;
        }
        let pinned = pinned_source
            .open_file(&entry.name)
            .map_err(|error| anyhow!(error))
            .with_context(|| format!("reopen pinned manifest input {}", entry.name))?;
        let actual_sha256 = hash_file(pinned).with_context(|| format!("rehash {}", entry.name))?;
        ensure!(
            actual_sha256 == entry.sha256,
            "manifest input {} changed after validation",
            entry.name
        );
        let expected = hashed_identities
            .get(&entry.name)
            .context("hashed input identity is missing")?;
        let file = open_regular_nofollow(&root.join(&entry.name))
            .with_context(|| format!("reopen hashed input {}", entry.name))?;
        let actual = FileIdentity::from_metadata(&file.metadata()?);
        ensure!(
            &actual == expected,
            "manifest input {} changed after it was hashed",
            entry.name
        );
    }
    publish_lock.recheck().map_err(|error| anyhow!(error))?;
    publish_wire_profile_marker(&root, options.wire_profile)?;
    publish_lock.recheck().map_err(|error| anyhow!(error))?;
    publish_current_metadata_marker(&root, &metadata_publication)?;
    publish_lock.recheck().map_err(|error| anyhow!(error))?;
    ensure!(
        publish_immutable_object_noclobber(&output, &bytes)?,
        "refusing to overwrite {}",
        output.display()
    );
    Ok(output)
}

fn is_wire_profile_marker_name(name: &str) -> bool {
    [
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
    ]
    .into_iter()
    .any(|profile| wire_profile_marker(profile).name == name)
}

fn is_metadata_profile_marker_name(name: &str) -> bool {
    name.starts_with("archive-v2-metadata-schema-")
}

fn expected_current_metadata_marker_binding() -> GenerationFile {
    GenerationFile {
        name: CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
        size: CURRENT_TYPED_ERRORS_MARKER_SIZE,
        sha256: CURRENT_TYPED_ERRORS_MARKER_SHA256.to_owned(),
    }
}

fn reject_conflicting_wire_profile_marker(
    root: &Path,
    selected_profile: ArchiveV2WireProfile,
) -> Result<()> {
    for profile in [
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
    ] {
        if profile == selected_profile {
            continue;
        }
        let marker = wire_profile_marker(profile);
        let path = root.join(&marker.name);
        match std::fs::symlink_metadata(&path) {
            Ok(_) => bail!(
                "conflicting Archive V2 wire-profile marker {} exists; selected {}",
                marker.name,
                selected_profile
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("stat wire-profile marker {}", path.display()));
            }
        }
    }
    Ok(())
}

fn validate_selected_wire_profile_marker_if_present(
    root: &Path,
    profile: ArchiveV2WireProfile,
) -> Result<Option<FileIdentity>> {
    let marker = wire_profile_marker(profile);
    let bytes = wire_profile_marker_bytes(profile);
    let path = root.join(&marker.name);
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat wire-profile marker {}", path.display()));
        }
    }
    let (actual, identity) = read_regular_file_bounded(&path, marker.size)
        .with_context(|| format!("read wire-profile marker {}", marker.name))?;
    ensure!(
        actual == bytes && identity.len == marker.size,
        "refusing to replace conflicting Archive V2 wire-profile marker {}",
        marker.name
    );
    let reopened = FileIdentity::from_metadata(&open_regular_nofollow(&path)?.metadata()?);
    ensure!(
        reopened == identity,
        "Archive V2 wire-profile marker {} changed while it was checked",
        marker.name
    );
    Ok(Some(identity))
}

fn publish_wire_profile_marker(root: &Path, profile: ArchiveV2WireProfile) -> Result<()> {
    let marker = wire_profile_marker(profile);
    let marker_bytes = wire_profile_marker_bytes(profile);
    let marker_path = root.join(&marker.name);

    reject_conflicting_wire_profile_marker(root, profile)?;
    if !publish_immutable_object_noclobber(&marker_path, marker_bytes)? {
        // Accept a same-profile race only when the winner published the exact
        // SDK object. An opposite marker is checked again below.
        validate_selected_wire_profile_marker_if_present(root, profile)?
            .context("selected wire-profile marker disappeared after publication race")?;
    }
    reject_conflicting_wire_profile_marker(root, profile)?;
    validate_selected_wire_profile_marker_if_present(root, profile)?
        .context("selected wire-profile marker is absent after publication")?;
    Ok(())
}

fn validate_current_metadata_marker_if_present(root: &Path) -> Result<Option<FileIdentity>> {
    let marker = expected_current_metadata_marker_binding();
    let path = root.join(&marker.name);
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat metadata-schema marker {}", path.display()));
        }
    }
    let (actual, identity) = read_regular_file_bounded(&path, marker.size)
        .with_context(|| format!("read metadata-schema marker {}", marker.name))?;
    ensure!(
        actual == CURRENT_TYPED_ERRORS_MARKER_BYTES && identity.len == marker.size,
        "refusing to replace conflicting Archive V2 metadata-schema marker {}",
        marker.name
    );
    let reopened = FileIdentity::from_metadata(&open_regular_nofollow(&path)?.metadata()?);
    ensure!(
        reopened == identity,
        "Archive V2 metadata-schema marker {} changed while it was checked",
        marker.name
    );
    Ok(Some(identity))
}

/// Publish the fixed metadata marker only with authority from a complete exact
/// generation scan. This function never derives permission from manifest
/// fields or from the fixed marker constants alone.
fn publish_current_metadata_marker(
    root: &Path,
    publication: &AuditedCurrentMetadataMarkerPublication,
) -> Result<()> {
    let marker = publication.marker_manifest_entry();
    let marker_path = root.join(&marker.name);
    if !publish_immutable_object_noclobber(&marker_path, publication.marker_bytes())? {
        validate_current_metadata_marker_if_present(root)?
            .context("current metadata-schema marker disappeared after publication race")?;
    }
    validate_current_metadata_marker_if_present(root)?
        .context("current metadata-schema marker is absent after publication")?;
    Ok(())
}

/// Atomically publish `bytes` without replacing `output`.
///
/// Returns `true` when this call created `output` and `false` when another
/// publisher won the race. The caller must validate an existing object before
/// it accepts the `false` result.
fn publish_immutable_object_noclobber(output: &Path, bytes: &[u8]) -> Result<bool> {
    let parent = output.parent().context("immutable object has no parent")?;
    let basename = output
        .file_name()
        .and_then(|name| name.to_str())
        .context("immutable object basename is not UTF-8")?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp = parent.join(format!(
        ".{basename}.tmp.{}.{}",
        std::process::id(),
        timestamp
    ));
    let result = (|| -> Result<bool> {
        let mut file = FsOpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .with_context(|| format!("create {}", temp.display()))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        match std::fs::hard_link(&temp, output) {
            Ok(()) => {
                File::open(parent)?.sync_all()?;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error)
                .with_context(|| format!("publish {} without replacing it", output.display())),
        }
    })();
    let _ = std::fs::remove_file(&temp);
    result
}

fn hash_file(mut file: File) -> Result<String> {
    file.seek(StdSeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 8 << 20];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn read_regular_file_bounded(path: &Path, max_bytes: u64) -> Result<(Vec<u8>, FileIdentity)> {
    let mut file = open_regular_nofollow(path)?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.len() <= max_bytes,
        "file exceeds {max_bytes} bytes"
    );
    let capacity = usize::try_from(metadata.len()).context("file is too large for this host")?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 == metadata.len(),
        "file changed while being read"
    );
    Ok((bytes, FileIdentity::from_metadata(&metadata)))
}

fn open_regular_nofollow(path: &Path) -> Result<File> {
    open_regular_nofollow_io(path).map_err(Into::into)
}

fn open_regular_nofollow_io(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    let file = FsOpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;

    #[cfg(not(unix))]
    let file = {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "refusing symbolic link",
            ));
        }
        FsOpenOptions::new().read(true).open(path)?
    };

    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path is not a regular file",
        ));
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use blockzilla_archive_v2::{
        ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_METADATA, ArchiveV2HotBlockBlob,
        ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, ArchiveV2HotInstruction,
        ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
        ArchiveV2HotMetaRecord, ArchiveV2HotTxRow, ArchiveV2HotV0Message,
        ArchiveV2SystemInstructionData, WINCODE_ARCHIVE_V2_FLAG_LEB128,
        WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Header,
        write_archive_v2_hot_block_index,
    };
    use blockzilla_compact::{
        CompactMessageHeader, CompactMetaV1, CompactTransactionError, OwnedCompactRecentBlockhash,
    };
    use blockzilla_primitives::{CompactPubkey, WincodeLeb128FramedWriter, wincode_leb128_config};
    use blockzilla_read_sdk::manifest::{BLOCK_INDEX_FILE, BLOCKS_FILE, META_FILE};
    use blockzilla_registry::KeyIndex;
    use http_body_util::BodyExt;
    use std::fs;
    use tempfile::{TempDir, tempdir};
    use tower::ServiceExt;

    const EPOCH: u64 = 1;
    const SLOTS_PER_EPOCH: u64 = 10;
    const SLOT: u64 = 10;
    fn write_archive_files(
        root: &Path,
        index_flags: u32,
        first_signature_ordinal: u64,
        footer_transactions: u64,
        signature_bytes: usize,
    ) {
        write_archive_files_with_instruction(
            root,
            index_flags,
            first_signature_ordinal,
            footer_transactions,
            signature_bytes,
            None,
        );
    }

    fn write_archive_files_with_instruction(
        root: &Path,
        index_flags: u32,
        first_signature_ordinal: u64,
        footer_transactions: u64,
        signature_bytes: usize,
        instruction_data: Option<ArchiveV2HotInstructionData>,
    ) {
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: instruction_data
                .map(|data| {
                    vec![ArchiveV2HotInstruction {
                        program_id_index: 1,
                        accounts: Vec::new(),
                        data,
                    }]
                })
                .unwrap_or_default(),
        });
        write_archive_files_with_message(
            root,
            index_flags,
            first_signature_ordinal,
            footer_transactions,
            signature_bytes,
            message,
            Vec::new(),
            0,
            1,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn write_archive_files_with_message(
        root: &Path,
        index_flags: u32,
        first_signature_ordinal: u64,
        footer_transactions: u64,
        signature_bytes: usize,
        message: ArchiveV2HotMessagePayload,
        metadata: Vec<u8>,
        row_flags: u32,
        row_signature_count: u8,
    ) {
        let message = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
        let block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: SLOT,
                parent_slot: SLOT - 1,
                blockhash_id: 1,
                previous_blockhash_id: 0,
                block_time: Some(1_700_000_000),
                block_height: Some(1),
                rewards: None,
            },
            tx_count: 1,
            tx_rows: vec![ArchiveV2HotTxRow {
                tx_index: 0,
                flags: row_flags,
                message_offset: 0,
                message_len: message.len() as u32,
                metadata_offset: 0,
                metadata_len: metadata.len() as u32,
                signature_count: row_signature_count,
                reserved: [0; 3],
            }],
            message_bytes: message,
            metadata_bytes: metadata,
        };
        let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let compressed = zstd::bulk::compress(&uncompressed, 3).unwrap();
        fs::write(root.join(BLOCKS_FILE), &compressed).unwrap();
        let registry_keys = [[7u8; 32], [0u8; 32]];
        fs::write(root.join(REGISTRY_FILE), registry_keys.concat()).unwrap();
        KeyIndex::build(registry_keys.to_vec())
            .write(&root.join(REGISTRY_INDEX_FILE))
            .unwrap();
        fs::write(root.join(SIGNATURES_FILE), vec![9u8; signature_bytes]).unwrap();
        write_archive_v2_hot_block_index(
            &root.join(BLOCK_INDEX_FILE),
            compressed.len() as u64,
            3,
            index_flags,
            &[ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: SLOT,
                compressed_offset: 0,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: 1,
                first_tx_ordinal: 0,
                first_signature_ordinal,
                signature_count: u32::from(row_signature_count),
            }],
        )
        .unwrap();

        let meta_file = File::create(root.join(META_FILE)).unwrap();
        let mut meta = WincodeLeb128FramedWriter::new(meta_file);
        meta.write(&ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
        }))
        .unwrap();
        meta.write(&ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
            blocks: 1,
            transactions: footer_transactions,
            ..WincodeArchiveV2Footer::default()
        }))
        .unwrap();
        meta.flush().unwrap();
    }

    fn options(root: &Path) -> GenerateManifestOptions {
        GenerateManifestOptions {
            archive_dir: root.to_owned(),
            cluster_id: "mainnet-beta".to_owned(),
            epoch: EPOCH,
            generation_id: "epoch-1-test".to_owned(),
            slots_per_epoch: SLOTS_PER_EPOCH,
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            additional_files: Vec::new(),
            output: None,
        }
    }

    fn legacy_account_in_use_metadata() -> Vec<u8> {
        let current = CompactMetaV1 {
            err: Some(CompactTransactionError::AccountInUse),
            fee: 5_000,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        };
        let current = wincode::config::serialize(&current, wincode_leb128_config()).unwrap();
        assert_eq!(&current[..2], &[1, 0]);
        let mut legacy = Vec::with_capacity(current.len() + 4);
        legacy.extend_from_slice(&[1, 4, 0, 0, 0, 0]);
        legacy.extend_from_slice(&current[2..]);
        legacy
    }

    fn assert_no_wire_profile_marker(root: &Path) {
        for profile in [
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        ] {
            assert!(!root.join(wire_profile_marker(profile).name).exists());
        }
        assert!(!root.join(CURRENT_TYPED_ERRORS_MARKER_FILE).exists());
    }

    fn valid_fixture() -> TempDir {
        let directory = tempdir().unwrap();
        write_archive_files(directory.path(), 0, 0, 1, 64);
        generate_manifest(options(directory.path())).unwrap();
        directory
    }

    fn app(directory: &Path, token: Option<&str>, max_range_bytes: u64) -> Router {
        let catalog = load_catalog(&[directory.to_owned()]).unwrap();
        build_router(Arc::new(GatewayConfig {
            catalog,
            bearer_token: token.map(str::to_owned),
            max_range_bytes,
            max_concurrent_downloads: 2,
            max_request_body_bytes: 64,
        }))
        .unwrap()
    }

    #[test]
    fn parses_only_one_bounded_range() {
        assert_eq!(
            parse_single_range(&HeaderValue::from_static("bytes=2-5"), 10, 10),
            Ok(ByteRange {
                start: 2,
                length: 4
            })
        );
        assert_eq!(
            parse_single_range(&HeaderValue::from_static("bytes=-3"), 10, 10),
            Ok(ByteRange {
                start: 7,
                length: 3
            })
        );
        assert!(parse_single_range(&HeaderValue::from_static("bytes=0-1,4-5"), 10, 10).is_err());
        assert!(parse_single_range(&HeaderValue::from_static("bytes=0-9"), 10, 4).is_err());
        assert!(parse_single_range(&HeaderValue::from_static("bytes=10-"), 10, 10).is_err());
    }

    #[test]
    fn weak_if_none_match_matches_strong_etag() {
        let mut headers = HeaderMap::new();
        headers.insert(
            IF_NONE_MATCH,
            HeaderValue::from_static("W/\"abc\", \"other\""),
        );
        assert!(if_none_match(&headers, "\"abc\""));
    }

    #[test]
    fn generator_publishes_a_valid_complete_manifest() {
        let directory = valid_fixture();
        let bytes = fs::read(directory.path().join(GENERATION_MANIFEST_FILE)).unwrap();
        let manifest = GenerationManifest::parse(&bytes).unwrap();
        assert!(manifest.complete);
        assert_eq!(manifest.epoch, EPOCH);
        for name in REQUIRED_GENERATION_FILES {
            assert!(manifest.file(name).is_some(), "{name}");
        }
        assert!(manifest.file(REGISTRY_INDEX_FILE).is_some());
        let marker = wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
        assert_eq!(manifest.file(&marker.name), Some(&marker));
        assert_eq!(
            fs::read(directory.path().join(&marker.name)).unwrap(),
            wire_profile_marker_bytes(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1)
        );
        assert_eq!(
            ArchiveV2WireProfile::for_published_manifest(&manifest).unwrap(),
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        );
        let metadata_marker = expected_current_metadata_marker_binding();
        assert_eq!(manifest.file(&metadata_marker.name), Some(&metadata_marker));
        assert_eq!(
            fs::read(directory.path().join(&metadata_marker.name)).unwrap(),
            CURRENT_TYPED_ERRORS_MARKER_BYTES
        );
        assert_eq!(
            ArchiveV2MetadataWireProfile::for_manifest(
                &manifest,
                blockzilla_read_sdk::ArchiveV2MetadataProfileAdmission::RequireCurrentTypedErrors,
            )
            .unwrap(),
            ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1
        );
        let published_digest = manifest.generation_digest.clone();
        let mut without_marker = manifest.clone();
        without_marker.files.retain(|file| file.name != marker.name);
        without_marker.generation_digest = "0".repeat(64);
        assert_ne!(
            compute_generation_digest(&without_marker).unwrap(),
            published_digest
        );
        assert_eq!(manifest.file(SIGNATURES_FILE).unwrap().size, 64);
        assert_eq!(
            load_catalog(&[directory.path().to_owned()]).unwrap().len(),
            1
        );
        let before = fs::read(directory.path().join(GENERATION_MANIFEST_FILE)).unwrap();
        assert!(generate_manifest(options(directory.path())).is_err());
        assert_eq!(
            fs::read(directory.path().join(GENERATION_MANIFEST_FILE)).unwrap(),
            before
        );
    }

    #[test]
    fn generator_rejects_noncanonical_pre_profile_for_all_equivalent_generation() {
        let directory = tempdir().unwrap();
        write_archive_files(directory.path(), 0, 0, 1, 64);
        let mut generate = options(directory.path());
        generate.wire_profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        let error = generate_manifest(generate).unwrap_err().to_string();
        assert!(error.contains("canonical post-fallback"), "{error}");
        assert_no_wire_profile_marker(directory.path());
        assert!(!directory.path().join(GENERATION_MANIFEST_FILE).exists());
    }

    #[test]
    fn generator_rejects_duplicate_registry_keys() {
        let directory = tempdir().unwrap();
        write_archive_files(directory.path(), 0, 0, 1, 64);
        let indexed_keys = [[71u8; 32], [72u8; 32]];
        let duplicate_registry = [indexed_keys[0], indexed_keys[0]];
        fs::write(
            directory.path().join(REGISTRY_FILE),
            duplicate_registry.concat(),
        )
        .unwrap();
        KeyIndex::build(indexed_keys.to_vec())
            .write(&directory.path().join(REGISTRY_INDEX_FILE))
            .unwrap();

        let error = generate_manifest(options(directory.path()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("duplicate key"), "{error}");
        assert_no_wire_profile_marker(directory.path());
        assert!(!directory.path().join(GENERATION_MANIFEST_FILE).exists());
    }

    #[test]
    fn generator_requires_the_canonical_registry_index() {
        let directory = tempdir().unwrap();
        write_archive_files(directory.path(), 0, 0, 1, 64);
        fs::remove_file(directory.path().join(REGISTRY_INDEX_FILE)).unwrap();

        let error = generate_manifest(options(directory.path()))
            .unwrap_err()
            .to_string();
        assert!(error.contains(REGISTRY_INDEX_FILE), "{error}");
        assert_no_wire_profile_marker(directory.path());
        assert!(!directory.path().join(GENERATION_MANIFEST_FILE).exists());
    }

    #[test]
    fn loader_rejects_manifest_bound_duplicate_registry_keys() {
        let directory = tempdir().unwrap();
        write_archive_files(directory.path(), 0, 0, 1, 64);
        let indexed_keys = [[81u8; 32], [82u8; 32]];
        fs::write(directory.path().join(REGISTRY_FILE), indexed_keys.concat()).unwrap();
        KeyIndex::build(indexed_keys.to_vec())
            .write(&directory.path().join(REGISTRY_INDEX_FILE))
            .unwrap();
        generate_manifest(options(directory.path())).unwrap();

        let duplicate_registry = [indexed_keys[0], indexed_keys[0]];
        let registry_bytes = duplicate_registry.concat();
        fs::write(directory.path().join(REGISTRY_FILE), &registry_bytes).unwrap();
        let manifest_path = directory.path().join(GENERATION_MANIFEST_FILE);
        let mut manifest = GenerationManifest::parse(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest
            .files
            .iter_mut()
            .find(|file| file.name == REGISTRY_FILE)
            .unwrap()
            .sha256 = hex::encode(Sha256::digest(&registry_bytes));
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        let mut manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
        manifest_bytes.push(b'\n');
        fs::write(&manifest_path, manifest_bytes).unwrap();

        let error = load_catalog(&[directory.path().to_owned()])
            .err()
            .expect("duplicate registry must be rejected");
        let error = format!("{error:#}");
        assert!(error.contains("duplicate key"), "{error}");
    }

    #[test]
    fn loader_rejects_same_size_corrupt_wire_profile_marker() {
        let directory = valid_fixture();
        let marker = wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
        let path = directory.path().join(marker.name);
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 1;
        fs::write(path, bytes).unwrap();

        let error = load_catalog(&[directory.path().to_owned()])
            .err()
            .expect("corrupt marker must be rejected")
            .to_string();
        assert!(error.contains("canonical bytes"), "{error}");
    }

    #[test]
    fn loader_rejects_same_size_corrupt_metadata_schema_marker() {
        let directory = valid_fixture();
        let path = directory.path().join(CURRENT_TYPED_ERRORS_MARKER_FILE);
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 1;
        fs::write(path, bytes).unwrap();

        let error = load_catalog(&[directory.path().to_owned()])
            .err()
            .expect("corrupt metadata marker must be rejected")
            .to_string();
        assert!(error.contains("canonical bytes"), "{error}");
    }

    #[test]
    fn loader_rejects_same_size_payload_change() {
        let directory = valid_fixture();
        let path = directory.path().join(BLOCKS_FILE);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.last_mut().unwrap();
        *last ^= 1;
        fs::write(path, bytes).unwrap();

        assert!(load_catalog(&[directory.path().to_owned()]).is_err());
    }

    #[test]
    fn generator_uses_program_id_semantics_to_resolve_dual_parse() {
        let directory = tempdir().unwrap();
        write_archive_files_with_instruction(
            directory.path(),
            0,
            0,
            1,
            64,
            Some(ArchiveV2HotInstructionData::UnknownSystem(Vec::new())),
        );
        generate_manifest(options(directory.path())).unwrap();
        let manifest = GenerationManifest::parse(
            &fs::read(directory.path().join(GENERATION_MANIFEST_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            ArchiveV2WireProfile::for_published_manifest(&manifest).unwrap(),
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        );
    }

    #[test]
    fn generator_rejects_a_selected_profile_that_cannot_decode() {
        let directory = tempdir().unwrap();
        write_archive_files_with_instruction(
            directory.path(),
            0,
            0,
            1,
            64,
            Some(ArchiveV2HotInstructionData::System(
                ArchiveV2SystemInstructionData::Transfer { lamports: 4 },
            )),
        );
        let mut generate = options(directory.path());
        generate.wire_profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        let error = generate_manifest(generate).unwrap_err().to_string();
        assert!(
            error.contains("selected Archive V2 wire profile"),
            "{error}"
        );
        assert_no_wire_profile_marker(directory.path());
        assert!(!directory.path().join(GENERATION_MANIFEST_FILE).exists());
    }

    #[test]
    fn generator_does_not_publish_current_marker_over_legacy_metadata() {
        let directory = tempdir().unwrap();
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
        });
        write_archive_files_with_message(
            directory.path(),
            0,
            0,
            1,
            64,
            message,
            legacy_account_in_use_metadata(),
            ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_ERROR,
            1,
        );

        let error = generate_manifest(options(directory.path()))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("metadata") || error.contains("selected Archive V2 wire profile"),
            "{error}"
        );
        assert_no_wire_profile_marker(directory.path());
        assert!(!directory.path().join(GENERATION_MANIFEST_FILE).exists());
    }

    #[test]
    fn generator_rejects_message_version_that_disagrees_with_row() {
        let directory = tempdir().unwrap();
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
            address_table_lookups: Vec::new(),
        });
        write_archive_files_with_message(directory.path(), 0, 0, 1, 64, message, Vec::new(), 0, 1);

        let error = generate_manifest(options(directory.path()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("message version disagrees"), "{error}");
        assert_no_wire_profile_marker(directory.path());
        assert!(!directory.path().join(GENERATION_MANIFEST_FILE).exists());
    }

    #[test]
    fn generator_rejects_signature_count_that_disagrees_with_message() {
        let directory = tempdir().unwrap();
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
        });
        write_archive_files_with_message(directory.path(), 0, 0, 1, 128, message, Vec::new(), 0, 2);

        let error = generate_manifest(options(directory.path()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("requires 1 signatures"), "{error}");
        assert_no_wire_profile_marker(directory.path());
        assert!(!directory.path().join(GENERATION_MANIFEST_FILE).exists());
    }

    #[test]
    fn generator_does_not_replace_a_conflicting_selected_marker() {
        let directory = tempdir().unwrap();
        write_archive_files(directory.path(), 0, 0, 1, 64);
        let marker = wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
        let conflicting_bytes = b"operator-owned-conflict";
        fs::write(directory.path().join(&marker.name), conflicting_bytes).unwrap();

        let error = generate_manifest(options(directory.path()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("refusing to replace conflicting"), "{error}");
        assert_eq!(
            fs::read(directory.path().join(&marker.name)).unwrap(),
            conflicting_bytes
        );
        assert!(!directory.path().join(GENERATION_MANIFEST_FILE).exists());
    }

    #[test]
    fn generator_does_not_replace_a_conflicting_metadata_schema_marker() {
        let directory = tempdir().unwrap();
        write_archive_files(directory.path(), 0, 0, 1, 64);
        let conflicting_bytes = vec![b'x'; CURRENT_TYPED_ERRORS_MARKER_SIZE as usize];
        fs::write(
            directory.path().join(CURRENT_TYPED_ERRORS_MARKER_FILE),
            &conflicting_bytes,
        )
        .unwrap();

        let error = generate_manifest(options(directory.path()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("refusing to replace conflicting"), "{error}");
        assert_eq!(
            fs::read(directory.path().join(CURRENT_TYPED_ERRORS_MARKER_FILE)).unwrap(),
            conflicting_bytes
        );
        assert!(!directory.path().join(GENERATION_MANIFEST_FILE).exists());
    }

    #[test]
    fn generator_rejects_an_opposite_or_caller_supplied_profile_marker() {
        let opposite = tempdir().unwrap();
        write_archive_files(opposite.path(), 0, 0, 1, 64);
        let pre_marker =
            wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1);
        fs::write(
            opposite.path().join(&pre_marker.name),
            wire_profile_marker_bytes(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1),
        )
        .unwrap();
        let error = generate_manifest(options(opposite.path()))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("conflicting Archive V2 wire-profile marker"),
            "{error}"
        );
        assert!(!opposite.path().join(GENERATION_MANIFEST_FILE).exists());

        let supplied = tempdir().unwrap();
        write_archive_files(supplied.path(), 0, 0, 1, 64);
        let mut generate = options(supplied.path());
        generate.additional_files = vec![pre_marker.name];
        let error = generate_manifest(generate).unwrap_err().to_string();
        assert!(error.contains("marker names are reserved"), "{error}");
        assert!(!supplied.path().join(GENERATION_MANIFEST_FILE).exists());

        let supplied_metadata = tempdir().unwrap();
        write_archive_files(supplied_metadata.path(), 0, 0, 1, 64);
        let mut generate = options(supplied_metadata.path());
        generate.additional_files = vec![CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned()];
        let error = generate_manifest(generate).unwrap_err().to_string();
        assert!(error.contains("marker names are reserved"), "{error}");
        assert_no_wire_profile_marker(supplied_metadata.path());
        assert!(
            !supplied_metadata
                .path()
                .join(GENERATION_MANIFEST_FILE)
                .exists()
        );
    }

    #[test]
    fn generator_rejects_corrupt_index_and_footer_totals() {
        let bad_flags = tempdir().unwrap();
        write_archive_files(bad_flags.path(), 1, 0, 1, 64);
        assert!(generate_manifest(options(bad_flags.path())).is_err());
        assert_no_wire_profile_marker(bad_flags.path());

        let bad_ordinal = tempdir().unwrap();
        write_archive_files(bad_ordinal.path(), 0, 1, 1, 64);
        assert!(generate_manifest(options(bad_ordinal.path())).is_err());
        assert_no_wire_profile_marker(bad_ordinal.path());

        let bad_footer = tempdir().unwrap();
        write_archive_files(bad_footer.path(), 0, 0, 2, 64);
        assert!(generate_manifest(options(bad_footer.path())).is_err());
        assert_no_wire_profile_marker(bad_footer.path());

        let bad_signatures = tempdir().unwrap();
        write_archive_files(bad_signatures.path(), 0, 0, 1, 128);
        assert!(generate_manifest(options(bad_signatures.path())).is_err());
        assert_no_wire_profile_marker(bad_signatures.path());
    }

    #[test]
    fn concurrent_opposite_profile_producers_publish_only_one_profile() {
        let directory = tempdir().unwrap();
        write_archive_files(directory.path(), 0, 0, 1, 64);
        let root = directory.path().to_owned();
        let mut pre = options(&root);
        pre.wire_profile = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        let post = options(&root);
        let pre_thread = std::thread::spawn(move || generate_manifest(pre));
        let post_thread = std::thread::spawn(move || generate_manifest(post));
        let pre_result = pre_thread.join().unwrap();
        let post_result = post_thread.join().unwrap();
        assert_ne!(pre_result.is_ok(), post_result.is_ok());

        let pre_marker = root
            .join(wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1).name);
        let post_marker = root.join(
            wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1).name,
        );
        assert_ne!(pre_marker.exists(), post_marker.exists());
        let manifest =
            GenerationManifest::parse(&fs::read(root.join(GENERATION_MANIFEST_FILE)).unwrap())
                .unwrap();
        let selected = ArchiveV2WireProfile::for_published_manifest(&manifest).unwrap();
        assert_eq!(
            selected == ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
            pre_marker.exists()
        );
    }

    #[test]
    fn generator_rejects_traversal_before_opening_it() {
        let parent = tempdir().unwrap();
        let archive = parent.path().join("archive");
        fs::create_dir(&archive).unwrap();
        write_archive_files(&archive, 0, 0, 1, 64);
        fs::write(parent.path().join("outside-secret"), b"secret").unwrap();
        let mut options = options(&archive);
        options.additional_files = vec!["../outside-secret".to_owned()];
        let error = generate_manifest(options).unwrap_err().to_string();
        assert!(error.contains("safe non-control path component"), "{error}");
        assert!(!archive.join(GENERATION_MANIFEST_FILE).exists());
    }

    #[cfg(unix)]
    #[test]
    fn generator_and_loader_refuse_symlinked_files() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        write_archive_files(directory.path(), 0, 0, 1, 64);
        fs::write(directory.path().join("real-extra"), b"extra").unwrap();
        symlink("real-extra", directory.path().join("extra-link")).unwrap();
        let mut generate = options(directory.path());
        generate.additional_files = vec!["extra-link".to_owned()];
        assert!(generate_manifest(generate).is_err());

        fs::remove_file(directory.path().join("extra-link")).unwrap();
        generate_manifest(options(directory.path())).unwrap();
        fs::write(directory.path().join("registry-replacement"), [7u8; 32]).unwrap();
        fs::remove_file(directory.path().join(REGISTRY_FILE)).unwrap();
        symlink("registry-replacement", directory.path().join(REGISTRY_FILE)).unwrap();
        assert!(load_catalog(&[directory.path().to_owned()]).is_err());
    }

    #[test]
    fn loader_refuses_incomplete_manifest() {
        let directory = valid_fixture();
        let path = directory.path().join(GENERATION_MANIFEST_FILE);
        let mut manifest = GenerationManifest::parse(&fs::read(&path).unwrap()).unwrap();
        manifest.complete = false;
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        fs::write(path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
        assert!(load_catalog(&[directory.path().to_owned()]).is_err());
    }

    #[tokio::test]
    async fn bearer_auth_range_head_and_private_cache_contract() {
        let directory = valid_fixture();
        let block_bytes = fs::read(directory.path().join(BLOCKS_FILE)).unwrap();
        fs::write(directory.path().join("not-published"), b"no").unwrap();
        let app = app(directory.path(), Some("secret"), 64);

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/epochs/{EPOCH}/manifest"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let manifest_head = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri(format!("/v1/epochs/{EPOCH}/manifest"))
                    .header(AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let manifest_size = fs::metadata(directory.path().join(GENERATION_MANIFEST_FILE))
            .unwrap()
            .len();
        assert_eq!(manifest_head.status(), StatusCode::OK);
        assert_eq!(
            manifest_head.headers()[CONTENT_LENGTH],
            manifest_size.to_string()
        );
        assert!(
            manifest_head
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );

        let range = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/epochs/{EPOCH}/files/{BLOCKS_FILE}"))
                    .header(AUTHORIZATION, "Bearer secret")
                    .header(RANGE, "bytes=2-5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            range.headers()[CONTENT_RANGE],
            format!("bytes 2-5/{}", block_bytes.len())
        );
        assert_eq!(
            range.headers()[CACHE_CONTROL],
            "private, max-age=31536000, immutable"
        );
        let etag = range.headers()[ETAG].clone();
        assert_eq!(
            range.into_body().collect().await.unwrap().to_bytes(),
            &block_bytes[2..=5]
        );

        let head = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri(format!("/v1/epochs/{EPOCH}/files/{BLOCKS_FILE}"))
                    .header(AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(
            head.headers()[CONTENT_LENGTH],
            block_bytes.len().to_string()
        );
        assert!(
            head.into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );

        let not_modified = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/epochs/{EPOCH}/files/{BLOCKS_FILE}"))
                    .header(AUTHORIZATION, "Bearer secret")
                    .header(IF_NONE_MATCH, etag)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);

        let unlisted = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/epochs/{EPOCH}/files/not-published"))
                    .header(AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unlisted.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn manifest_is_private_and_oversized_or_multi_ranges_are_rejected() {
        let directory = valid_fixture();
        let block_size = fs::metadata(directory.path().join(BLOCKS_FILE))
            .unwrap()
            .len();
        let app = app(directory.path(), None, 4);

        let manifest = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/epochs/{EPOCH}/manifest"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            manifest.headers()[CACHE_CONTROL],
            "private, max-age=0, must-revalidate"
        );

        for range in ["bytes=0-4", "bytes=0-1,4-5"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/epochs/{EPOCH}/files/{BLOCKS_FILE}"))
                        .header(RANGE, range)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
            assert_eq!(
                response.headers()[CONTENT_RANGE],
                format!("bytes */{block_size}")
            );
        }
    }
}
