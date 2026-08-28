use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fs::{File, Metadata},
    io::{self, Write},
    os::unix::ffi::OsStrExt,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use rustix::fs::{AtFlags, Mode, OFlags};

use crate::{
    HttpObjectIdentity, HttpRangeSource, HttpRangeSourceStats, SourceError,
    manifest::validate_object_name,
    source::{RangeSource, SourceResult},
};

pub const MAX_HTTP_CACHE_DOWNLOAD_RANGE_BYTES: usize = 32 << 20;
pub const DEFAULT_HTTP_CACHE_MAX_OBJECT_BYTES: u64 = 8 << 30;
pub const DEFAULT_HTTP_CACHE_MAX_TOTAL_BYTES: u64 = 16 << 30;
const MAX_CACHED_OBJECTS: usize = 8;
const MAX_CACHE_OBJECT_NAME_BYTES: usize = 192;
const MAX_CACHE_BASE_URL_BYTES: usize = 4 * 1024;
const MAX_CACHE_ETAG_BYTES: usize = 4 * 1024;
const CACHE_FIXED_HEADER_BYTES: usize = 64;
const MAX_CACHE_HEADER_BYTES: usize = CACHE_FIXED_HEADER_BYTES
    + MAX_CACHE_BASE_URL_BYTES
    + MAX_CACHE_OBJECT_NAME_BYTES
    + MAX_CACHE_ETAG_BYTES;
const CACHE_MAGIC: [u8; 8] = *b"BZHTTPC1";
const CACHE_VERSION: u16 = 1;
const MAX_TEMP_NAME_ATTEMPTS: u64 = 128;

/// Create an absolute cache directory without following symlinks or using a
/// path-based permission change. Every new component is private to the user.
pub fn create_http_cache_directory(path: impl AsRef<Path>) -> SourceResult<()> {
    let path = path.as_ref();
    if !path.is_absolute() {
        return Err(protocol("cache root path must be absolute"));
    }
    for component in path.components() {
        match component {
            std::path::Component::RootDir
            | std::path::Component::CurDir
            | std::path::Component::Normal(_) => {}
            std::path::Component::ParentDir => {
                return Err(protocol("cache root path must not contain '..'"));
            }
            std::path::Component::Prefix(_) => {
                return Err(protocol("cache root path has an unsupported prefix"));
            }
        }
    }

    let flags = OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY;
    let root = rustix::fs::open("/", flags, Mode::empty())
        .map_err(io::Error::from)
        .map_err(|source| cache_io("cache-root", source))?;
    let mut directory = File::from(root);
    for component in path.components() {
        let std::path::Component::Normal(name) = component else {
            continue;
        };
        let opened = match rustix::fs::openat(&directory, name, flags, Mode::empty()) {
            Ok(descriptor) => descriptor,
            Err(error) if error == rustix::io::Errno::NOENT => {
                match rustix::fs::mkdirat(&directory, name, Mode::from_bits_truncate(0o700)) {
                    Ok(()) => {}
                    Err(error) if error == rustix::io::Errno::EXIST => {}
                    Err(error) => {
                        return Err(cache_io("cache-root", io::Error::from(error)));
                    }
                }
                rustix::fs::openat(&directory, name, flags, Mode::empty())
                    .map_err(io::Error::from)
                    .map_err(|source| cache_io("cache-root", source))?
            }
            Err(error) => return Err(cache_io("cache-root", io::Error::from(error))),
        };
        directory = File::from(opened);
    }
    drop(directory);
    CacheDirectory::open_existing(path).map(|_| ())
}

#[derive(Debug, Clone, Copy)]
pub struct HttpRangeCacheOptions {
    pub download_range_bytes: usize,
    /// Maximum final bytes for one cache file, including its identity envelope.
    pub max_cached_object_bytes: u64,
    /// Maximum final bytes for all configured cache files, including envelopes.
    pub max_configured_cache_bytes: u64,
}

impl Default for HttpRangeCacheOptions {
    fn default() -> Self {
        Self {
            download_range_bytes: MAX_HTTP_CACHE_DOWNLOAD_RANGE_BYTES,
            max_cached_object_bytes: DEFAULT_HTTP_CACHE_MAX_OBJECT_BYTES,
            max_configured_cache_bytes: DEFAULT_HTTP_CACHE_MAX_TOTAL_BYTES,
        }
    }
}

/// The complete cache work known after identity HEAD requests and before any
/// cold body GET starts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HttpRangeCachePlan {
    pub configured_objects: u64,
    /// Exact remote object body bytes.
    pub configured_cache_bytes: u64,
    /// Exact final cache-file bytes, including identity envelopes.
    pub configured_disk_bytes: u64,
    pub cache_hits: u64,
    pub planned_downloads: u64,
    /// Exact remote body bytes needed for cold objects.
    pub planned_download_bytes: u64,
    /// Exact cache-file bytes written for cold objects.
    pub planned_disk_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HttpRangeCacheStats {
    pub configured_objects: u64,
    pub configured_cache_bytes: u64,
    pub configured_disk_bytes: u64,
    pub identity_head_requests: u64,
    pub cache_hits: u64,
    pub cache_downloads: u64,
    pub planned_download_bytes: u64,
    pub planned_disk_bytes: u64,
    pub cold_network_body_bytes: u64,
    pub local_read_calls: u64,
    pub local_read_bytes: u64,
    pub uncached_payload_network_bytes: u64,
}

#[derive(Debug, Default)]
struct CacheCounters {
    hits: AtomicU64,
    downloads: AtomicU64,
    cold_network_body_bytes: AtomicU64,
    local_read_calls: AtomicU64,
    local_read_bytes: AtomicU64,
}

#[derive(Debug)]
struct CacheEntry {
    file: Arc<File>,
    payload_offset: u64,
    payload_length: u64,
}

#[derive(Clone)]
pub struct CachedHttpRangeSource {
    http: HttpRangeSource,
    entries: Arc<HashMap<String, CacheEntry>>,
    counters: Arc<CacheCounters>,
    configured_cache_bytes: u64,
    configured_disk_bytes: u64,
    planned_download_bytes: u64,
    planned_disk_bytes: u64,
    identity_head_requests: u64,
    http_before: HttpRangeSourceStats,
}

impl std::fmt::Debug for CachedHttpRangeSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CachedHttpRangeSource")
            .field("http", &self.http)
            .field("cached_objects", &self.entries.keys().collect::<Vec<_>>())
            .field("stats", &self.stats())
            .finish()
    }
}

impl CachedHttpRangeSource {
    pub fn open(
        http: HttpRangeSource,
        cache_root: impl AsRef<Path>,
        objects: &[&str],
    ) -> SourceResult<Self> {
        Self::with_options(http, cache_root, objects, HttpRangeCacheOptions::default())
    }

    pub fn with_options(
        http: HttpRangeSource,
        cache_root: impl AsRef<Path>,
        objects: &[&str],
        options: HttpRangeCacheOptions,
    ) -> SourceResult<Self> {
        Self::with_options_and_plan_reporter(http, cache_root, objects, options, |_| {})
    }

    /// Open the cache and report all planned cold bytes before the first body
    /// GET. Every configured object has completed its fresh identity HEAD when
    /// `report_plan` runs.
    pub fn with_options_and_plan_reporter(
        http: HttpRangeSource,
        cache_root: impl AsRef<Path>,
        objects: &[&str],
        options: HttpRangeCacheOptions,
        report_plan: impl FnOnce(HttpRangeCachePlan),
    ) -> SourceResult<Self> {
        validate_options(objects, options)?;
        let directory = CacheDirectory::open_existing(cache_root.as_ref())?;
        let http_before = http.stats();
        let counters = Arc::new(CacheCounters::default());
        let mut identities = Vec::with_capacity(objects.len());
        let mut configured_cache_bytes = 0_u64;
        let mut configured_disk_bytes = 0_u64;
        for object in objects {
            let identity = http.strong_identity(object)?;
            let header = CacheHeader::new(&http, object, &identity)?.encode()?;
            let disk_bytes = (header.len() as u64)
                .checked_add(identity.length)
                .ok_or_else(|| protocol(format!("cache file length overflow for {object}")))?;
            if disk_bytes > options.max_cached_object_bytes {
                return Err(protocol(format!(
                    "cache file for {object} is {disk_bytes} bytes, above the {} byte per-object disk limit",
                    options.max_cached_object_bytes
                )));
            }
            configured_cache_bytes = configured_cache_bytes
                .checked_add(identity.length)
                .ok_or_else(|| protocol("configured cache byte count overflow"))?;
            configured_disk_bytes = configured_disk_bytes
                .checked_add(disk_bytes)
                .ok_or_else(|| protocol("configured cache disk byte count overflow"))?;
            identities.push(((*object).to_owned(), identity, header, disk_bytes));
        }
        if configured_disk_bytes > options.max_configured_cache_bytes {
            return Err(protocol(format!(
                "configured cache files use {configured_disk_bytes} bytes, above the {} byte aggregate disk limit",
                options.max_configured_cache_bytes
            )));
        }

        let mut planned = Vec::with_capacity(identities.len());
        let mut cache_hits = 0_u64;
        let mut planned_download_bytes = 0_u64;
        let mut planned_disk_bytes = 0_u64;
        for (object, identity, header, disk_bytes) in identities {
            let final_name = final_name(http.epoch(), &object);
            let existing = match directory.open_regular_optional(&final_name)? {
                Some(file) => {
                    cache_hits = cache_hits
                        .checked_add(1)
                        .ok_or_else(|| protocol("cache hit count overflow"))?;
                    Some(validate_cache_file(&http, &object, &identity, file)?)
                }
                None => {
                    planned_download_bytes = planned_download_bytes
                        .checked_add(identity.length)
                        .ok_or_else(|| protocol("planned cache download byte count overflow"))?;
                    planned_disk_bytes = planned_disk_bytes
                        .checked_add(disk_bytes)
                        .ok_or_else(|| protocol("planned cache disk byte count overflow"))?;
                    None
                }
            };
            planned.push(PlannedEntry {
                object,
                identity,
                header,
                existing,
            });
        }
        let planned_downloads = (planned.len() as u64)
            .checked_sub(cache_hits)
            .ok_or_else(|| protocol("planned cache download count underflow"))?;
        report_plan(HttpRangeCachePlan {
            configured_objects: planned.len() as u64,
            configured_cache_bytes,
            configured_disk_bytes,
            cache_hits,
            planned_downloads,
            planned_download_bytes,
            planned_disk_bytes,
        });
        counters.hits.store(cache_hits, Ordering::Relaxed);

        let mut entries = HashMap::with_capacity(planned.len());
        for planned in planned {
            let entry = match planned.existing {
                Some(entry) => entry,
                None => download_entry(
                    &http,
                    &directory,
                    &planned.object,
                    &planned.identity,
                    &planned.header,
                    options,
                    counters.as_ref(),
                )?,
            };
            let object = planned.object;
            entries.insert(object, entry);
        }
        directory.sync()?;
        Ok(Self {
            http,
            entries: Arc::new(entries),
            counters,
            configured_cache_bytes,
            configured_disk_bytes,
            planned_download_bytes,
            planned_disk_bytes,
            identity_head_requests: objects.len() as u64,
            http_before,
        })
    }

    pub fn http(&self) -> &HttpRangeSource {
        &self.http
    }

    pub fn stats(&self) -> HttpRangeCacheStats {
        let network = self.http.stats().saturating_sub(self.http_before);
        let cold_network_body_bytes = self
            .counters
            .cold_network_body_bytes
            .load(Ordering::Relaxed);
        HttpRangeCacheStats {
            configured_objects: self.entries.len() as u64,
            configured_cache_bytes: self.configured_cache_bytes,
            configured_disk_bytes: self.configured_disk_bytes,
            identity_head_requests: self.identity_head_requests,
            cache_hits: self.counters.hits.load(Ordering::Relaxed),
            cache_downloads: self.counters.downloads.load(Ordering::Relaxed),
            planned_download_bytes: self.planned_download_bytes,
            planned_disk_bytes: self.planned_disk_bytes,
            cold_network_body_bytes,
            local_read_calls: self.counters.local_read_calls.load(Ordering::Relaxed),
            local_read_bytes: self.counters.local_read_bytes.load(Ordering::Relaxed),
            uncached_payload_network_bytes: network
                .returned_body_bytes
                .saturating_sub(cold_network_body_bytes),
        }
    }

    pub fn cached_objects(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    fn read_cached_into(
        &self,
        object: &str,
        entry: &CacheEntry,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        let length_u64 = u64::try_from(length).map_err(|_| SourceError::OutOfBounds {
            object: object.to_owned(),
            offset,
            length,
            size: entry.payload_length,
        })?;
        let end = offset
            .checked_add(length_u64)
            .ok_or_else(|| SourceError::OutOfBounds {
                object: object.to_owned(),
                offset,
                length,
                size: entry.payload_length,
            })?;
        if end > entry.payload_length {
            return Err(SourceError::OutOfBounds {
                object: object.to_owned(),
                offset,
                length,
                size: entry.payload_length,
            });
        }
        if length == 0 {
            destination.clear();
            return Ok(());
        }
        if destination.len() < length {
            destination.resize(length, 0);
        } else {
            destination.truncate(length);
        }
        let mut read = 0_usize;
        while read < length {
            let position = entry
                .payload_offset
                .checked_add(offset)
                .and_then(|value| value.checked_add(read as u64))
                .ok_or_else(|| protocol(format!("cache read offset overflow for {object}")))?;
            let count = entry
                .file
                .read_at(&mut destination[read..], position)
                .map_err(|source| cache_io(object, source))?;
            self.counters
                .local_read_calls
                .fetch_add(1, Ordering::Relaxed);
            self.counters
                .local_read_bytes
                .fetch_add(count as u64, Ordering::Relaxed);
            if count == 0 {
                return Err(SourceError::ShortRead {
                    object: object.to_owned(),
                    expected: length,
                    actual: read,
                });
            }
            read += count;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PlannedEntry {
    object: String,
    identity: HttpObjectIdentity,
    header: Vec<u8>,
    existing: Option<CacheEntry>,
}

impl RangeSource for CachedHttpRangeSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        match self.entries.get(object) {
            Some(entry) => Ok(Some(entry.payload_length)),
            None => self.http.size(object),
        }
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        match self.entries.get(object) {
            Some(entry) => {
                let mut bytes = Vec::new();
                self.read_cached_into(object, entry, offset, length, &mut bytes)?;
                Ok(bytes)
            }
            None => self.http.read_range(object, offset, length),
        }
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        match self.entries.get(object) {
            Some(entry) => self.read_cached_into(object, entry, offset, length, destination),
            None => self
                .http
                .read_range_into(object, offset, length, destination),
        }
    }
}

fn validate_options(objects: &[&str], options: HttpRangeCacheOptions) -> SourceResult<()> {
    if objects.is_empty() || objects.len() > MAX_CACHED_OBJECTS {
        return Err(protocol(format!(
            "HTTP range cache requires 1..={MAX_CACHED_OBJECTS} objects"
        )));
    }
    if options.download_range_bytes == 0
        || options.download_range_bytes > MAX_HTTP_CACHE_DOWNLOAD_RANGE_BYTES
    {
        return Err(protocol(format!(
            "cache download range must be 1..={MAX_HTTP_CACHE_DOWNLOAD_RANGE_BYTES} bytes"
        )));
    }
    if options.max_cached_object_bytes == 0 {
        return Err(protocol("maximum cached object bytes must be non-zero"));
    }
    if options.max_configured_cache_bytes == 0 {
        return Err(protocol("maximum configured cache bytes must be non-zero"));
    }
    let mut unique = HashSet::with_capacity(objects.len());
    for object in objects {
        validate_object_name(object).map_err(|_| SourceError::InvalidName((*object).to_owned()))?;
        if object.len() > MAX_CACHE_OBJECT_NAME_BYTES
            || !object
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        {
            return Err(protocol(format!(
                "cache object {object} is not a bounded portable file name"
            )));
        }
        if !unique.insert(*object) {
            return Err(protocol(format!(
                "HTTP range cache contains duplicate object {object}"
            )));
        }
        let final_name = final_name(u64::MAX, object);
        validate_child_name(&final_name)?;
        let longest_temporary = format!(
            ".{final_name}.partial.{}.{}.{}",
            u32::MAX,
            u128::MAX,
            MAX_TEMP_NAME_ATTEMPTS - 1,
        );
        validate_child_name(&longest_temporary)?;
    }
    Ok(())
}

fn download_entry(
    http: &HttpRangeSource,
    directory: &CacheDirectory,
    object: &str,
    identity: &HttpObjectIdentity,
    header: &[u8],
    options: HttpRangeCacheOptions,
    counters: &CacheCounters,
) -> SourceResult<CacheEntry> {
    let final_name = final_name(http.epoch(), object);
    let (temporary_name, mut temporary, temporary_identity) =
        directory.create_private_temporary(&final_name)?;
    let mut guard = OwnedTemporary {
        directory,
        name: temporary_name.clone(),
        identity: temporary_identity,
        armed: true,
    };
    temporary
        .write_all(header)
        .map_err(|source| cache_io(object, source))?;
    let mut offset = 0_u64;
    while offset < identity.length {
        let remaining = identity.length - offset;
        let length =
            usize::try_from(remaining.min(options.download_range_bytes as u64)).map_err(|_| {
                protocol(format!(
                    "cache range length does not fit usize for {object}"
                ))
            })?;
        let bytes = http.read_range(object, offset, length)?;
        if bytes.len() != length {
            return Err(SourceError::ShortRead {
                object: object.to_owned(),
                expected: length,
                actual: bytes.len(),
            });
        }
        temporary
            .write_all(&bytes)
            .map_err(|source| cache_io(object, source))?;
        counters
            .cold_network_body_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        offset = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| protocol(format!("cache download offset overflow for {object}")))?;
    }
    temporary
        .sync_all()
        .map_err(|source| cache_io(object, source))?;
    let expected_file_length = (header.len() as u64)
        .checked_add(identity.length)
        .ok_or_else(|| protocol(format!("cache file length overflow for {object}")))?;
    if temporary
        .metadata()
        .map_err(|source| cache_io(object, source))?
        .len()
        != expected_file_length
    {
        return Err(protocol(format!(
            "completed cache temporary has the wrong length for {object}"
        )));
    }

    let published =
        directory.link_no_replace(&temporary_name, &final_name, temporary_identity, object)?;
    let file = if published {
        directory
            .open_regular_optional(&final_name)?
            .ok_or_else(|| protocol(format!("published cache file disappeared for {object}")))?
    } else {
        directory
            .open_regular_optional(&final_name)?
            .ok_or_else(|| protocol(format!("racing cache file disappeared for {object}")))?
    };
    guard.cleanup()?;
    directory.sync()?;
    let entry = validate_cache_file(http, object, identity, file)?;
    counters.downloads.fetch_add(1, Ordering::Relaxed);
    Ok(entry)
}

#[derive(Debug)]
struct CacheHeader {
    base_url: String,
    epoch: u64,
    object: String,
    strong_etag: String,
    payload_length: u64,
}

impl CacheHeader {
    fn new(
        http: &HttpRangeSource,
        object: &str,
        identity: &HttpObjectIdentity,
    ) -> SourceResult<Self> {
        let header = Self {
            base_url: http.base_url().as_str().to_owned(),
            epoch: http.epoch(),
            object: object.to_owned(),
            strong_etag: identity.strong_etag.clone(),
            payload_length: identity.length,
        };
        header.validate_lengths()?;
        Ok(header)
    }

    fn validate_lengths(&self) -> SourceResult<()> {
        if self.base_url.is_empty() || self.base_url.len() > MAX_CACHE_BASE_URL_BYTES {
            return Err(protocol("cache base URL length is invalid"));
        }
        if self.object.is_empty() || self.object.len() > MAX_CACHE_OBJECT_NAME_BYTES {
            return Err(protocol("cache object name length is invalid"));
        }
        if self.strong_etag.is_empty() || self.strong_etag.len() > MAX_CACHE_ETAG_BYTES {
            return Err(protocol("cache strong ETag length is invalid"));
        }
        Ok(())
    }

    fn encode(&self) -> SourceResult<Vec<u8>> {
        self.validate_lengths()?;
        let header_length = CACHE_FIXED_HEADER_BYTES
            .checked_add(self.base_url.len())
            .and_then(|value| value.checked_add(self.object.len()))
            .and_then(|value| value.checked_add(self.strong_etag.len()))
            .ok_or_else(|| protocol("cache header length overflow"))?;
        if header_length > MAX_CACHE_HEADER_BYTES {
            return Err(protocol("cache header exceeds its bound"));
        }
        let mut output = vec![0_u8; header_length];
        output[0..8].copy_from_slice(&CACHE_MAGIC);
        output[8..10].copy_from_slice(&CACHE_VERSION.to_le_bytes());
        output[10..12].copy_from_slice(&(CACHE_FIXED_HEADER_BYTES as u16).to_le_bytes());
        output[12..16].copy_from_slice(&(header_length as u32).to_le_bytes());
        output[16..24].copy_from_slice(&self.payload_length.to_le_bytes());
        output[24..32].copy_from_slice(&self.epoch.to_le_bytes());
        output[32..34].copy_from_slice(&(self.base_url.len() as u16).to_le_bytes());
        output[34..36].copy_from_slice(&(self.object.len() as u16).to_le_bytes());
        output[36..38].copy_from_slice(&(self.strong_etag.len() as u16).to_le_bytes());
        let mut cursor = CACHE_FIXED_HEADER_BYTES;
        for bytes in [
            self.base_url.as_bytes(),
            self.object.as_bytes(),
            self.strong_etag.as_bytes(),
        ] {
            output[cursor..cursor + bytes.len()].copy_from_slice(bytes);
            cursor += bytes.len();
        }
        Ok(output)
    }

    fn decode(bytes: &[u8]) -> SourceResult<Self> {
        if bytes.len() < CACHE_FIXED_HEADER_BYTES || bytes[0..8] != CACHE_MAGIC {
            return Err(protocol("cache file has the wrong magic or a short header"));
        }
        if u16::from_le_bytes(bytes[8..10].try_into().expect("two bytes")) != CACHE_VERSION
            || usize::from(u16::from_le_bytes(
                bytes[10..12].try_into().expect("two bytes"),
            )) != CACHE_FIXED_HEADER_BYTES
        {
            return Err(protocol(
                "cache file version or fixed header length differs",
            ));
        }
        if bytes[38..CACHE_FIXED_HEADER_BYTES] != [0; CACHE_FIXED_HEADER_BYTES - 38] {
            return Err(protocol("cache file reserved header bytes are nonzero"));
        }
        let header_length = u32::from_le_bytes(bytes[12..16].try_into().expect("four bytes"));
        let header_length = usize::try_from(header_length)
            .map_err(|_| protocol("cache header length does not fit usize"))?;
        if header_length != bytes.len() || header_length > MAX_CACHE_HEADER_BYTES {
            return Err(protocol("cache variable header length differs"));
        }
        let base_length = usize::from(u16::from_le_bytes(
            bytes[32..34].try_into().expect("two bytes"),
        ));
        let object_length = usize::from(u16::from_le_bytes(
            bytes[34..36].try_into().expect("two bytes"),
        ));
        let etag_length = usize::from(u16::from_le_bytes(
            bytes[36..38].try_into().expect("two bytes"),
        ));
        let expected = CACHE_FIXED_HEADER_BYTES
            .checked_add(base_length)
            .and_then(|value| value.checked_add(object_length))
            .and_then(|value| value.checked_add(etag_length))
            .ok_or_else(|| protocol("cache decoded header length overflow"))?;
        if expected != header_length {
            return Err(protocol("cache header field lengths differ"));
        }
        let mut cursor = CACHE_FIXED_HEADER_BYTES;
        let mut field = |length: usize, label: &str| -> SourceResult<String> {
            let end = cursor
                .checked_add(length)
                .ok_or_else(|| protocol(format!("cache {label} range overflow")))?;
            let value = std::str::from_utf8(
                bytes
                    .get(cursor..end)
                    .ok_or_else(|| protocol(format!("cache {label} is truncated")))?,
            )
            .map_err(|_| protocol(format!("cache {label} is not UTF-8")))?
            .to_owned();
            cursor = end;
            Ok(value)
        };
        let header = Self {
            base_url: field(base_length, "base URL")?,
            object: field(object_length, "object name")?,
            strong_etag: field(etag_length, "strong ETag")?,
            payload_length: u64::from_le_bytes(bytes[16..24].try_into().expect("eight bytes")),
            epoch: u64::from_le_bytes(bytes[24..32].try_into().expect("eight bytes")),
        };
        header.validate_lengths()?;
        Ok(header)
    }
}

fn validate_cache_file(
    http: &HttpRangeSource,
    object: &str,
    identity: &HttpObjectIdentity,
    file: File,
) -> SourceResult<CacheEntry> {
    let before = validate_private_regular_file(&file, object)?;
    if before.len() < CACHE_FIXED_HEADER_BYTES as u64 {
        return Err(protocol(format!("cache file is too short for {object}")));
    }
    let mut fixed = [0_u8; CACHE_FIXED_HEADER_BYTES];
    read_exact_at(&file, &mut fixed, 0, object)?;
    let header_length = u32::from_le_bytes(fixed[12..16].try_into().expect("four bytes"));
    let header_length = usize::try_from(header_length).map_err(|_| {
        protocol(format!(
            "cache header length does not fit usize for {object}"
        ))
    })?;
    if !(CACHE_FIXED_HEADER_BYTES..=MAX_CACHE_HEADER_BYTES).contains(&header_length) {
        return Err(protocol(format!(
            "cache header length is invalid for {object}"
        )));
    }
    let mut header_bytes = vec![0_u8; header_length];
    header_bytes[..CACHE_FIXED_HEADER_BYTES].copy_from_slice(&fixed);
    read_exact_at(
        &file,
        &mut header_bytes[CACHE_FIXED_HEADER_BYTES..],
        CACHE_FIXED_HEADER_BYTES as u64,
        object,
    )?;
    let header = CacheHeader::decode(&header_bytes)?;
    if header.base_url != http.base_url().as_str()
        || header.epoch != http.epoch()
        || header.object != object
    {
        return Err(protocol(format!(
            "cache file source binding differs for {object}; use a new cache root"
        )));
    }
    if header.payload_length != identity.length {
        return Err(protocol(format!(
            "cache file length identity differs for {object}; use a new cache root"
        )));
    }
    if header.strong_etag != identity.strong_etag {
        return Err(protocol(format!(
            "cache file strong ETag differs for {object}; use a new cache root"
        )));
    }
    let expected_length = (header_length as u64)
        .checked_add(identity.length)
        .ok_or_else(|| protocol(format!("cache file geometry overflow for {object}")))?;
    if before.len() != expected_length {
        return Err(protocol(format!(
            "cache file geometry differs for {object}"
        )));
    }
    let after = file.metadata().map_err(|source| cache_io(object, source))?;
    if !same_file_identity(&before, &after) {
        return Err(protocol(format!(
            "cache file changed while it was validated for {object}"
        )));
    }
    Ok(CacheEntry {
        file: Arc::new(file),
        payload_offset: header_length as u64,
        payload_length: identity.length,
    })
}

fn read_exact_at(file: &File, output: &mut [u8], offset: u64, object: &str) -> SourceResult<()> {
    let mut read = 0_usize;
    while read < output.len() {
        let position = offset
            .checked_add(read as u64)
            .ok_or_else(|| protocol(format!("cache header read offset overflow for {object}")))?;
        let count = file
            .read_at(&mut output[read..], position)
            .map_err(|source| cache_io(object, source))?;
        if count == 0 {
            return Err(SourceError::ShortRead {
                object: object.to_owned(),
                expected: output.len(),
                actual: read,
            });
        }
        read += count;
    }
    Ok(())
}

struct CacheDirectory {
    file: File,
    display_path: PathBuf,
    device: u64,
    inode: u64,
}

impl CacheDirectory {
    fn open_existing(path: &Path) -> SourceResult<Self> {
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|source| cache_io("cache-root", source))?
                .join(path)
        };
        let file = open_directory_without_symlinks(&absolute)?;
        let opened = file
            .metadata()
            .map_err(|source| cache_io("cache-root", source))?;
        if opened.uid() != rustix::process::geteuid().as_raw() || opened.mode() & 0o022 != 0 {
            return Err(protocol(
                "cache root must be owned by the effective user and not group/world writable",
            ));
        }
        Ok(Self {
            file,
            display_path: absolute,
            device: opened.dev(),
            inode: opened.ino(),
        })
    }

    fn verify(&self) -> SourceResult<()> {
        let metadata = self
            .file
            .metadata()
            .map_err(|source| cache_io("cache-root", source))?;
        if !metadata.is_dir()
            || metadata.dev() != self.device
            || metadata.ino() != self.inode
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.mode() & 0o022 != 0
        {
            return Err(protocol("cache root directory capability changed"));
        }
        let path_file = open_directory_without_symlinks(&self.display_path)?;
        let path_metadata = path_file
            .metadata()
            .map_err(|source| cache_io("cache-root", source))?;
        if !path_metadata.is_dir()
            || path_metadata.dev() != self.device
            || path_metadata.ino() != self.inode
            || path_metadata.uid() != rustix::process::geteuid().as_raw()
            || path_metadata.mode() & 0o022 != 0
        {
            return Err(protocol("cache root path binding changed"));
        }
        Ok(())
    }

    fn open_regular_optional(&self, name: &str) -> SourceResult<Option<File>> {
        validate_child_name(name)?;
        self.verify()?;
        let descriptor = match rustix::fs::openat(
            &self.file,
            name,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        ) {
            Ok(descriptor) => descriptor,
            Err(error) if error == rustix::io::Errno::NOENT => return Ok(None),
            Err(error) => return Err(cache_io(name, io::Error::from(error))),
        };
        let file = File::from(descriptor);
        validate_private_regular_file(&file, name)?;
        Ok(Some(file))
    }

    fn create_private_temporary(
        &self,
        final_name: &str,
    ) -> SourceResult<(String, File, (u64, u64))> {
        self.verify()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for attempt in 0..MAX_TEMP_NAME_ATTEMPTS {
            let name = format!(
                ".{final_name}.partial.{}.{}.{}",
                std::process::id(),
                now,
                attempt
            );
            validate_child_name(&name)?;
            let descriptor = match rustix::fs::openat(
                &self.file,
                &name,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::from_raw_mode(0o600),
            ) {
                Ok(descriptor) => descriptor,
                Err(error) if error == rustix::io::Errno::EXIST => continue,
                Err(error) => return Err(cache_io(&name, io::Error::from(error))),
            };
            let file = File::from(descriptor);
            let metadata = validate_private_regular_file(&file, &name)?;
            return Ok((name, file, (metadata.dev(), metadata.ino())));
        }
        Err(protocol(
            "could not allocate one private cache temporary name",
        ))
    }

    fn require_same_inode(
        &self,
        name: &str,
        expected: (u64, u64),
        label: &str,
    ) -> SourceResult<()> {
        let metadata = self
            .open_regular_optional(name)?
            .ok_or_else(|| protocol(format!("{label} disappeared")))?
            .metadata()
            .map_err(|source| cache_io(label, source))?;
        if (metadata.dev(), metadata.ino()) != expected {
            return Err(protocol(format!("{label} changed inode")));
        }
        Ok(())
    }

    fn link_no_replace(
        &self,
        source: &str,
        destination: &str,
        expected: (u64, u64),
        label: &str,
    ) -> SourceResult<bool> {
        self.require_same_inode(source, expected, label)?;
        validate_child_name(source)?;
        validate_child_name(destination)?;
        match rustix::fs::linkat(
            &self.file,
            source,
            &self.file,
            destination,
            AtFlags::empty(),
        ) {
            Ok(()) => {
                self.require_same_inode(destination, expected, label)?;
                Ok(true)
            }
            Err(error) if error == rustix::io::Errno::EXIST => Ok(false),
            Err(error) => Err(cache_io(label, io::Error::from(error))),
        }
    }

    fn unlink_if_same_inode(
        &self,
        name: &str,
        expected: (u64, u64),
        label: &str,
    ) -> SourceResult<bool> {
        let Some(file) = self.open_regular_optional(name)? else {
            return Ok(false);
        };
        let metadata = file.metadata().map_err(|source| cache_io(label, source))?;
        if (metadata.dev(), metadata.ino()) != expected {
            return Err(protocol(format!(
                "refusing to clean a replaced cache temporary for {label}"
            )));
        }
        match rustix::fs::unlinkat(&self.file, name, AtFlags::empty()) {
            Ok(()) => Ok(true),
            Err(error) if error == rustix::io::Errno::NOENT => Ok(false),
            Err(error) => Err(cache_io(label, io::Error::from(error))),
        }
    }

    fn sync(&self) -> SourceResult<()> {
        self.verify()?;
        self.file
            .sync_all()
            .map_err(|source| cache_io("cache-root", source))
    }
}

fn open_directory_without_symlinks(path: &Path) -> SourceResult<File> {
    if !path.is_absolute() {
        return Err(protocol("cache root path must resolve to an absolute path"));
    }
    let flags = OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY;
    let root = rustix::fs::open("/", flags, Mode::empty())
        .map_err(io::Error::from)
        .map_err(|source| cache_io("cache-root", source))?;
    let mut directory = File::from(root);
    for component in path.components() {
        match component {
            std::path::Component::RootDir | std::path::Component::CurDir => {}
            std::path::Component::Normal(name) => {
                let descriptor = rustix::fs::openat(&directory, name, flags, Mode::empty())
                    .map_err(io::Error::from)
                    .map_err(|source| cache_io("cache-root", source))?;
                directory = File::from(descriptor);
            }
            std::path::Component::ParentDir => {
                return Err(protocol("cache root path must not contain '..'"));
            }
            std::path::Component::Prefix(_) => {
                return Err(protocol("cache root path has an unsupported prefix"));
            }
        }
    }
    Ok(directory)
}

struct OwnedTemporary<'a> {
    directory: &'a CacheDirectory,
    name: String,
    identity: (u64, u64),
    armed: bool,
}

impl OwnedTemporary<'_> {
    fn cleanup(&mut self) -> SourceResult<()> {
        if self.armed {
            self.directory.unlink_if_same_inode(
                &self.name,
                self.identity,
                "owned cache temporary",
            )?;
            self.armed = false;
        }
        Ok(())
    }
}

impl Drop for OwnedTemporary<'_> {
    fn drop(&mut self) {
        if self.armed {
            let removed = self.directory.unlink_if_same_inode(
                &self.name,
                self.identity,
                "owned cache temporary",
            );
            if matches!(removed, Ok(true)) {
                let _ = self.directory.sync();
            }
        }
    }
}

fn validate_private_regular_file(file: &File, label: &str) -> SourceResult<Metadata> {
    let metadata = file.metadata().map_err(|source| cache_io(label, source))?;
    if !metadata.is_file() {
        return Err(protocol(format!(
            "cache entry {label} is not a regular file"
        )));
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o077 != 0 {
        return Err(protocol(format!(
            "cache entry {label} must be private and owned by the effective user"
        )));
    }
    Ok(metadata)
}

fn same_file_identity(left: &Metadata, right: &Metadata) -> bool {
    left.is_file()
        && right.is_file()
        && left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

fn final_name(epoch: u64, object: &str) -> String {
    format!("epoch-{epoch}.{object}.http-range-cache-v1")
}

fn validate_child_name(name: &str) -> SourceResult<()> {
    let bytes = OsStr::new(name).as_bytes();
    if bytes.is_empty()
        || matches!(bytes, b"." | b"..")
        || bytes.contains(&b'/')
        || bytes.contains(&0)
        || bytes.len() > 240
    {
        return Err(protocol("unsafe or overlong cache child name"));
    }
    Ok(())
}

fn cache_io(object: impl Into<String>, source: io::Error) -> SourceError {
    SourceError::Io {
        object: object.into(),
        source,
    }
}

fn protocol(message: impl Into<String>) -> SourceError {
    SourceError::Protocol(message.into())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        io::{Read as _, Write as _},
        net::{TcpListener, TcpStream},
        os::unix::fs::symlink,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    };

    use tempfile::{TempDir, tempdir, tempdir_in};

    use super::*;
    use crate::HttpRangeSourceOptions;

    fn cache_tempdir() -> TempDir {
        let private_tmp = Path::new("/private/tmp");
        if private_tmp.is_dir() {
            tempdir_in(private_tmp).unwrap()
        } else {
            tempdir().unwrap()
        }
    }

    #[derive(Clone)]
    struct ServedObject {
        bytes: Vec<u8>,
        etag: String,
        interrupt_next_get: bool,
    }

    #[derive(Debug, Clone)]
    struct RequestRecord {
        method: String,
        object: String,
        range_length: Option<usize>,
    }

    #[derive(Default)]
    struct ServerState {
        objects: BTreeMap<String, ServedObject>,
        requests: Vec<RequestRecord>,
    }

    struct TestServer {
        address: std::net::SocketAddr,
        state: Arc<Mutex<ServerState>>,
        stop: Arc<AtomicBool>,
        task: Option<thread::JoinHandle<()>>,
    }

    impl TestServer {
        fn start(objects: impl IntoIterator<Item = (&'static str, Vec<u8>, &'static str)>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let state = Arc::new(Mutex::new(ServerState::default()));
            {
                let mut state = state.lock().unwrap();
                for (name, bytes, etag) in objects {
                    state.objects.insert(
                        name.to_owned(),
                        ServedObject {
                            bytes,
                            etag: etag.to_owned(),
                            interrupt_next_get: false,
                        },
                    );
                }
            }
            let stop = Arc::new(AtomicBool::new(false));
            let task_state = state.clone();
            let task_stop = stop.clone();
            let task = thread::spawn(move || {
                while !task_stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            stream.set_nonblocking(false).unwrap();
                            serve_connection(stream, &task_state);
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2));
                        }
                        Err(error) => panic!("test server accept failed: {error}"),
                    }
                }
            });
            Self {
                address,
                state,
                stop,
                task: Some(task),
            }
        }

        fn base_url(&self) -> String {
            format!("http://{}/gateway", self.address)
        }

        fn source(&self) -> HttpRangeSource {
            HttpRangeSource::with_options(
                self.base_url(),
                7,
                None,
                HttpRangeSourceOptions {
                    allow_insecure_http: true,
                    ..HttpRangeSourceOptions::default()
                },
            )
            .unwrap()
        }

        fn requests(&self) -> Vec<RequestRecord> {
            self.state.lock().unwrap().requests.clone()
        }

        fn clear_requests(&self) {
            self.state.lock().unwrap().requests.clear();
        }

        fn set_etag(&self, object: &str, etag: &str) {
            self.state
                .lock()
                .unwrap()
                .objects
                .get_mut(object)
                .unwrap()
                .etag = etag.to_owned();
        }

        fn interrupt_next_get(&self, object: &str) {
            self.state
                .lock()
                .unwrap()
                .objects
                .get_mut(object)
                .unwrap()
                .interrupt_next_get = true;
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            let _ = TcpStream::connect(self.address);
            if let Some(task) = self.task.take() {
                task.join().unwrap();
            }
        }
    }

    fn serve_connection(mut stream: TcpStream, state: &Arc<Mutex<ServerState>>) {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let count = match stream.read(&mut buffer) {
                Ok(count) => count,
                Err(_) => return,
            };
            if count == 0 {
                return;
            }
            request.extend_from_slice(&buffer[..count]);
            assert!(request.len() <= 32 * 1024);
        }
        let request = String::from_utf8(request).unwrap();
        let mut lines = request.lines();
        let first = lines.next().unwrap();
        let mut first = first.split_whitespace();
        let method = first.next().unwrap().to_owned();
        let path = first.next().unwrap();
        let object = path
            .strip_prefix("/gateway/v1/epochs/7/files/")
            .unwrap()
            .to_owned();
        let range = lines.find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("range: bytes=")
                .map(str::to_owned)
        });
        let parsed_range = range.as_deref().map(|value| {
            let (start, end) = value.split_once('-').unwrap();
            let start = start.parse::<usize>().unwrap();
            let end = end.parse::<usize>().unwrap();
            (start, end)
        });

        let (served, interrupt) = {
            let mut state = state.lock().unwrap();
            let range_length = parsed_range.map(|(start, end)| end - start + 1);
            state.requests.push(RequestRecord {
                method: method.clone(),
                object: object.clone(),
                range_length,
            });
            let served = state.objects.get_mut(&object).unwrap();
            let interrupt = served.interrupt_next_get && method == "GET";
            if interrupt {
                served.interrupt_next_get = false;
            }
            (served.clone(), interrupt)
        };

        if method == "HEAD" {
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: {}\r\nConnection: close\r\n\r\n",
                served.bytes.len(),
                served.etag
            )
            .unwrap();
            return;
        }
        let (start, end) = parsed_range.unwrap();
        let body = &served.bytes[start..=end];
        write!(
            stream,
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nETag: {}\r\nConnection: close\r\n\r\n",
            body.len(),
            start,
            end,
            served.bytes.len(),
            served.etag
        )
        .unwrap();
        let delivered = if interrupt {
            body.len().saturating_sub(1)
        } else {
            body.len()
        };
        stream.write_all(&body[..delivered]).unwrap();
    }

    #[test]
    fn cold_then_warm_cache_returns_equal_bytes_and_separates_io() {
        let server = TestServer::start([
            ("sidecar.bin", (0_u8..20).collect(), "\"sidecar-v1\""),
            ("payload.bin", vec![91, 92, 93], "\"payload-v1\""),
        ]);
        let directory = cache_tempdir();
        let options = HttpRangeCacheOptions {
            download_range_bytes: 7,
            max_cached_object_bytes: 1024,
            max_configured_cache_bytes: 2048,
        };
        let mut cold_plan = None;
        let cold = CachedHttpRangeSource::with_options_and_plan_reporter(
            server.source(),
            directory.path(),
            &["sidecar.bin"],
            options,
            |plan| {
                assert_eq!(
                    server
                        .requests()
                        .iter()
                        .filter(|request| request.method == "GET")
                        .count(),
                    0
                );
                cold_plan = Some(plan);
            },
        )
        .unwrap();
        let cold_plan = cold_plan.unwrap();
        assert_eq!(cold_plan.configured_objects, 1);
        assert_eq!(cold_plan.configured_cache_bytes, 20);
        assert!(cold_plan.configured_disk_bytes > 20);
        assert_eq!(cold_plan.cache_hits, 0);
        assert_eq!(cold_plan.planned_downloads, 1);
        assert_eq!(cold_plan.planned_download_bytes, 20);
        assert_eq!(
            cold_plan.planned_disk_bytes,
            cold_plan.configured_disk_bytes
        );
        let cold_bytes = cold.read_range("sidecar.bin", 0, 20).unwrap();
        assert_eq!(cold.read_range("payload.bin", 0, 3).unwrap(), [91, 92, 93]);
        let cold_stats = cold.stats();
        assert_eq!(cold_stats.configured_cache_bytes, 20);
        assert_eq!(
            cold_stats.configured_disk_bytes,
            cold_plan.configured_disk_bytes
        );
        assert_eq!(cold_stats.identity_head_requests, 1);
        assert_eq!(cold_stats.cache_hits, 0);
        assert_eq!(cold_stats.cache_downloads, 1);
        assert_eq!(cold_stats.planned_download_bytes, 20);
        assert_eq!(cold_stats.planned_disk_bytes, cold_plan.planned_disk_bytes);
        assert_eq!(cold_stats.cold_network_body_bytes, 20);
        assert_eq!(cold_stats.local_read_bytes, 20);
        assert_eq!(cold_stats.uncached_payload_network_bytes, 3);
        assert!(
            server
                .requests()
                .iter()
                .filter_map(|request| request.range_length)
                .all(|length| length <= 7)
        );

        server.clear_requests();
        let warm = CachedHttpRangeSource::with_options(
            server.source(),
            directory.path(),
            &["sidecar.bin"],
            options,
        )
        .unwrap();
        let warm_bytes = warm.read_range("sidecar.bin", 0, 20).unwrap();
        assert_eq!(warm_bytes, cold_bytes);
        let warm_stats = warm.stats();
        assert_eq!(warm_stats.cache_hits, 1);
        assert_eq!(warm_stats.cache_downloads, 0);
        assert_eq!(warm_stats.planned_disk_bytes, 0);
        assert_eq!(warm_stats.cold_network_body_bytes, 0);
        assert_eq!(warm_stats.local_read_bytes, 20);
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "HEAD");
        assert_eq!(requests[0].object, "sidecar.bin");
    }

    #[test]
    fn stale_same_size_strong_etag_is_a_hard_error() {
        let server = TestServer::start([("sidecar.bin", vec![7; 20], "\"sidecar-v1\"")]);
        let directory = cache_tempdir();
        CachedHttpRangeSource::open(server.source(), directory.path(), &["sidecar.bin"]).unwrap();
        server.set_etag("sidecar.bin", "\"sidecar-v2\"");
        server.clear_requests();
        let error =
            CachedHttpRangeSource::open(server.source(), directory.path(), &["sidecar.bin"])
                .unwrap_err();
        assert!(error.to_string().contains("strong ETag differs"));
        assert!(error.to_string().contains("new cache root"));
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|request| request.method == "GET")
                .count(),
            0
        );
    }

    #[test]
    fn interrupted_download_removes_only_its_private_partial() {
        let server = TestServer::start([("sidecar.bin", vec![8; 20], "\"sidecar-v1\"")]);
        server.interrupt_next_get("sidecar.bin");
        let directory = cache_tempdir();
        fs::write(directory.path().join("unrelated.partial"), b"keep").unwrap();
        let error = CachedHttpRangeSource::with_options(
            server.source(),
            directory.path(),
            &["sidecar.bin"],
            HttpRangeCacheOptions {
                download_range_bytes: 7,
                max_cached_object_bytes: 1024,
                max_configured_cache_bytes: 2048,
            },
        )
        .unwrap_err();
        assert!(!error.to_string().is_empty());
        let mut names = fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, vec!["unrelated.partial"]);
        assert_eq!(
            fs::read(directory.path().join("unrelated.partial")).unwrap(),
            b"keep"
        );
    }

    #[test]
    fn cache_range_configuration_cannot_exceed_thirty_two_mib() {
        let server = TestServer::start([("sidecar.bin", vec![1], "\"sidecar-v1\"")]);
        let directory = cache_tempdir();
        let error = CachedHttpRangeSource::with_options(
            server.source(),
            directory.path(),
            &["sidecar.bin"],
            HttpRangeCacheOptions {
                download_range_bytes: MAX_HTTP_CACHE_DOWNLOAD_RANGE_BYTES + 1,
                max_cached_object_bytes: 1024,
                max_configured_cache_bytes: 2048,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("cache download range"));
        assert!(server.requests().is_empty());
    }

    #[test]
    fn cache_root_path_rejects_symlink_components_before_head() {
        let server = TestServer::start([("sidecar.bin", vec![1], "\"sidecar-v1\"")]);
        let parent = cache_tempdir();
        let real_parent = parent.path().join("real-parent");
        let real_cache = real_parent.join("cache");
        fs::create_dir(&real_parent).unwrap();
        fs::create_dir(&real_cache).unwrap();
        let linked_parent = parent.path().join("linked-parent");
        symlink(&real_parent, &linked_parent).unwrap();

        let error = CachedHttpRangeSource::open(
            server.source(),
            linked_parent.join("cache"),
            &["sidecar.bin"],
        )
        .unwrap_err();
        assert!(!error.to_string().is_empty());
        assert!(server.requests().is_empty());
    }

    #[test]
    fn cache_directory_creation_is_anchored_and_private() {
        let parent = cache_tempdir();
        let target = parent.path().join("one").join("two");
        create_http_cache_directory(&target).unwrap();
        let metadata = fs::metadata(&target).unwrap();
        assert!(metadata.is_dir());
        assert_eq!(metadata.mode() & 0o077, 0);
    }

    #[test]
    fn cache_directory_creation_rejects_parent_traversal_without_writes() {
        let parent = cache_tempdir();
        let target = parent.path().join("new").join("..").join("escape");
        let error = create_http_cache_directory(&target).unwrap_err();
        assert!(error.to_string().contains("must not contain '..'"));
        assert!(!parent.path().join("new").exists());
        assert!(!parent.path().join("escape").exists());
    }

    #[test]
    fn cache_directory_creation_rejects_symlink_without_target_writes() {
        let parent = cache_tempdir();
        let real = parent.path().join("real");
        fs::create_dir(&real).unwrap();
        let linked = parent.path().join("linked");
        symlink(&real, &linked).unwrap();
        let error = create_http_cache_directory(linked.join("child")).unwrap_err();
        assert!(!error.to_string().is_empty());
        assert!(!real.join("child").exists());
    }

    #[test]
    fn aggregate_cache_cap_is_checked_after_heads_and_before_gets() {
        let server = TestServer::start([
            ("one.bin", vec![1; 7], "\"one-v1\""),
            ("two.bin", vec![2; 8], "\"two-v1\""),
        ]);
        let directory = cache_tempdir();
        let error = CachedHttpRangeSource::with_options(
            server.source(),
            directory.path(),
            &["one.bin", "two.bin"],
            HttpRangeCacheOptions {
                download_range_bytes: 7,
                max_cached_object_bytes: 1024,
                max_configured_cache_bytes: 200,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("aggregate disk limit"));
        let requests = server.requests();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == "HEAD")
                .count(),
            2
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == "GET")
                .count(),
            0
        );
    }

    #[test]
    fn per_object_disk_cap_is_checked_after_head_and_before_get() {
        let server = TestServer::start([("one.bin", vec![1], "\"one-v1\"")]);
        let directory = cache_tempdir();
        let error = CachedHttpRangeSource::with_options(
            server.source(),
            directory.path(),
            &["one.bin"],
            HttpRangeCacheOptions {
                download_range_bytes: 7,
                max_cached_object_bytes: 100,
                max_configured_cache_bytes: 1024,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("per-object disk limit"));
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "HEAD");
    }
}
