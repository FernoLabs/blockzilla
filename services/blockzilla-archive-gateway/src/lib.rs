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
    GENERATION_MANIFEST_FILE, GENERATION_MANIFEST_SCHEMA_VERSION, GenerationFile,
    GenerationManifest, REGISTRY_FILE, REQUIRED_GENERATION_FILES, SIGNATURES_FILE,
    compute_generation_digest,
};
use blockzilla_read_sdk::{
    HashVerification, OpenOptions as ReaderOpenOptions, RangeSource, SourceError, SourceResult,
    validate_generation_structure,
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
/// File hashes are deliberately not recomputed here. Hashing belongs to the
/// offline publication command; startup only validates the manifest digest,
/// file identities/sizes, and cheap structural completion markers.
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
    ensure!(
        manifest.file(GENERATION_MANIFEST_FILE).is_none(),
        "manifest must not publish itself"
    );

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
    let source = NoFollowRangeSource {
        root: root.to_owned(),
    };
    let options = ReaderOpenOptions {
        hash_verification: HashVerification::SizesOnly,
        ..ReaderOpenOptions::default()
    };
    let validated = validate_generation_structure(&source, manifest, &options)
        .map_err(|error| anyhow!(error))?;
    ensure!(
        !validated.index.rows.is_empty(),
        "hot-block index has no rows"
    );
    Ok(())
}

#[derive(Clone)]
struct NoFollowRangeSource {
    root: PathBuf,
}

impl NoFollowRangeSource {
    fn path(&self, object: &str) -> SourceResult<PathBuf> {
        if !is_safe_object_name(object) {
            return Err(SourceError::InvalidName(object.to_owned()));
        }
        Ok(self.root.join(object))
    }

    fn open(&self, object: &str) -> SourceResult<File> {
        let path = self.path(object)?;
        open_regular_nofollow_io(&path).map_err(|source| SourceError::Io {
            object: object.to_owned(),
            source,
        })
    }
}

impl RangeSource for NoFollowRangeSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        let file = match self.open(object) {
            Ok(file) => file,
            Err(SourceError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        file.metadata()
            .map(|metadata| Some(metadata.len()))
            .map_err(|source| SourceError::Io {
                object: object.to_owned(),
                source,
            })
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        let mut file = self.open(object)?;
        let size = file
            .metadata()
            .map_err(|source| SourceError::Io {
                object: object.to_owned(),
                source,
            })?
            .len();
        let length_u64 = u64::try_from(length).map_err(|_| SourceError::OutOfBounds {
            object: object.to_owned(),
            offset,
            length,
            size,
        })?;
        let end = offset
            .checked_add(length_u64)
            .filter(|end| *end <= size)
            .ok_or_else(|| SourceError::OutOfBounds {
                object: object.to_owned(),
                offset,
                length,
                size,
            })?;
        debug_assert!(end <= size);
        file.seek(StdSeekFrom::Start(offset))
            .map_err(|source| SourceError::Io {
                object: object.to_owned(),
                source,
            })?;
        let mut bytes = vec![0u8; length];
        file.read_exact(&mut bytes)
            .map_err(|source| SourceError::Io {
                object: object.to_owned(),
                source,
            })?;
        Ok(bytes)
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

    let mut names = BTreeSet::from(REQUIRED_GENERATION_FILES.map(str::to_owned));
    let signatures_path = root.join(SIGNATURES_FILE);
    if signatures_path.try_exists()? {
        names.insert(SIGNATURES_FILE.to_owned());
    }
    for name in options.additional_files {
        ensure!(
            is_safe_object_name(&name),
            "additional file must be one safe non-control path component: {name:?}"
        );
        names.insert(name);
    }
    ensure!(
        !names.contains(GENERATION_MANIFEST_FILE),
        "manifest cannot publish itself"
    );

    let mut files = Vec::with_capacity(names.len());
    let mut hashed_identities = BTreeMap::new();
    for name in names {
        let path = root.join(&name);
        let file =
            open_regular_nofollow(&path).with_context(|| format!("open manifest input {name}"))?;
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
    validate_archive_structure(&root, &manifest)?;

    // The manifest is the publication boundary. Reopen every input after all
    // validation and reject any replacement, resize, or mtime change that
    // happened after its bytes were hashed.
    for (name, expected) in &hashed_identities {
        let file = open_regular_nofollow(&root.join(name))
            .with_context(|| format!("reopen hashed input {name}"))?;
        let actual = FileIdentity::from_metadata(&file.metadata()?);
        ensure!(
            &actual == expected,
            "manifest input {name} changed after it was hashed"
        );
    }

    let mut bytes = serde_json::to_vec_pretty(&manifest)?;
    bytes.push(b'\n');
    publish_manifest_noclobber(&output, &bytes)?;
    Ok(output)
}

fn hash_file(mut file: File) -> Result<String> {
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

fn publish_manifest_noclobber(output: &Path, bytes: &[u8]) -> Result<()> {
    let parent = output.parent().context("manifest output has no parent")?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp = parent.join(format!(
        ".{GENERATION_MANIFEST_FILE}.tmp.{}.{}",
        std::process::id(),
        timestamp
    ));
    let result = (|| -> Result<()> {
        let mut file = FsOpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .with_context(|| format!("create {}", temp.display()))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::hard_link(&temp, output)
            .with_context(|| format!("publish {} without replacing it", output.display()))?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    let _ = std::fs::remove_file(&temp);
    result
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
    use blockzilla_format::{
        ArchiveV2HotMetaRecord, WINCODE_ARCHIVE_V2_FLAG_LEB128,
        WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Header,
        WincodeLeb128FramedWriter,
    };
    use blockzilla_read_sdk::manifest::{BLOCK_INDEX_FILE, BLOCKS_FILE, META_FILE};
    use http_body_util::BodyExt;
    use std::fs;
    use tempfile::{TempDir, tempdir};
    use tower::ServiceExt;

    const EPOCH: u64 = 1;
    const SLOTS_PER_EPOCH: u64 = 10;
    const SLOT: u64 = 10;
    const BLOCK_BYTES: &[u8] = b"frame-data";

    fn write_archive_files(
        root: &Path,
        index_flags: u32,
        first_signature_ordinal: u64,
        footer_transactions: u64,
        signature_bytes: usize,
    ) {
        fs::write(root.join(BLOCKS_FILE), BLOCK_BYTES).unwrap();
        fs::write(root.join(REGISTRY_FILE), [7u8; 32]).unwrap();
        fs::write(root.join(SIGNATURES_FILE), vec![9u8; signature_bytes]).unwrap();

        let mut index = Vec::new();
        index.extend_from_slice(b"BZV2HIX1");
        index.extend_from_slice(&1u16.to_le_bytes());
        index.extend_from_slice(&0u16.to_le_bytes());
        index.extend_from_slice(&1u64.to_le_bytes());
        index.extend_from_slice(&(BLOCK_BYTES.len() as u64).to_le_bytes());
        index.extend_from_slice(&3i32.to_le_bytes());
        index.extend_from_slice(&index_flags.to_le_bytes());
        index.extend_from_slice(&0u32.to_le_bytes());
        index.extend_from_slice(&SLOT.to_le_bytes());
        index.extend_from_slice(&0u64.to_le_bytes());
        index.extend_from_slice(&(BLOCK_BYTES.len() as u32).to_le_bytes());
        index.extend_from_slice(&100u32.to_le_bytes());
        index.extend_from_slice(&1u32.to_le_bytes());
        index.extend_from_slice(&0u64.to_le_bytes());
        index.extend_from_slice(&first_signature_ordinal.to_le_bytes());
        index.extend_from_slice(&1u32.to_le_bytes());
        assert_eq!(index.len(), 36 + 52);
        fs::write(root.join(BLOCK_INDEX_FILE), index).unwrap();

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
            additional_files: Vec::new(),
            output: None,
        }
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
    fn generator_rejects_corrupt_index_and_footer_totals() {
        let bad_flags = tempdir().unwrap();
        write_archive_files(bad_flags.path(), 1, 0, 1, 64);
        assert!(generate_manifest(options(bad_flags.path())).is_err());

        let bad_ordinal = tempdir().unwrap();
        write_archive_files(bad_ordinal.path(), 0, 1, 1, 64);
        assert!(generate_manifest(options(bad_ordinal.path())).is_err());

        let bad_footer = tempdir().unwrap();
        write_archive_files(bad_footer.path(), 0, 0, 2, 64);
        assert!(generate_manifest(options(bad_footer.path())).is_err());

        let bad_signatures = tempdir().unwrap();
        write_archive_files(bad_signatures.path(), 0, 0, 1, 128);
        assert!(generate_manifest(options(bad_signatures.path())).is_err());
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

    #[test]
    fn startup_does_not_rehash_archive_payloads() {
        let directory = valid_fixture();
        fs::write(directory.path().join(BLOCKS_FILE), b"other-data").unwrap();
        assert_eq!(BLOCK_BYTES.len(), b"other-data".len());
        assert!(load_catalog(&[directory.path().to_owned()]).is_ok());
    }

    #[tokio::test]
    async fn bearer_auth_range_head_and_private_cache_contract() {
        let directory = valid_fixture();
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
        assert_eq!(range.headers()[CONTENT_RANGE], "bytes 2-5/10");
        assert_eq!(
            range.headers()[CACHE_CONTROL],
            "private, max-age=31536000, immutable"
        );
        let etag = range.headers()[ETAG].clone();
        assert_eq!(
            range.into_body().collect().await.unwrap().to_bytes(),
            &BLOCK_BYTES[2..=5]
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
        assert_eq!(head.headers()[CONTENT_LENGTH], "10");
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
            assert_eq!(response.headers()[CONTENT_RANGE], "bytes */10");
        }
    }
}
