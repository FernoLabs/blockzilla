//! Provider-neutral immutable object storage primitives for Hivezilla.
//!
//! The trait deliberately exposes only the operations required by custody and
//! recovery code. It does not assign durability, ACK, retirement, or catalog
//! authority to an object-store operation.

#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

pub const MAX_OBJECT_KEY_BYTES: usize = 4_096;
pub const MAX_OBJECT_VERSION_BYTES: usize = 4_096;

const OBJECTS_DIRECTORY: &str = "objects";
const TEMPORARY_DIRECTORY: &str = "temporary";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_key(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn to_relative_path(&self) -> PathBuf {
        self.0.split('/').collect()
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectVersion(Vec<u8>);

impl ObjectVersion {
    pub fn new(value: Vec<u8>) -> Result<Self> {
        if value.is_empty() || value.len() > MAX_OBJECT_VERSION_BYTES {
            return Err(Error::InvalidVersion);
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    pub key: ObjectKey,
    pub encoded_len: u64,
    /// Provider-attested end-to-end digest, when the backend can supply one.
    /// Callers must perform a complete readback when this is `None`.
    pub provider_sha256: Option<[u8; 32]>,
    pub version: Option<ObjectVersion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateRequest {
    pub key: ObjectKey,
    pub encoded_len: u64,
    pub encoded_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDisposition {
    Created,
    AlreadyExisted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateResult {
    pub disposition: CreateDisposition,
    pub metadata: ObjectMetadata,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteDisposition {
    Deleted,
    NotFound,
}

pub struct ObjectRead {
    pub metadata: ObjectMetadata,
    pub body: Box<dyn Read + Send>,
}

pub trait ImmutableObjectStore: Send + Sync {
    /// Atomically creates `request.key` only when it does not already exist.
    /// An exact pre-existing object is idempotent; a different object at the
    /// same key is an immutable-key collision and must fail closed.
    fn create_if_absent(
        &self,
        request: &CreateRequest,
        source: &mut dyn Read,
    ) -> Result<CreateResult>;

    fn open(&self, key: &ObjectKey) -> Result<ObjectRead>;

    fn stat(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>>;

    /// Verifies that the exact expected object is readable. A trustworthy
    /// provider digest avoids a readback; backends without one are streamed in
    /// full and hashed here.
    fn verify_exact(&self, expected: &CreateRequest) -> Result<ObjectMetadata> {
        let metadata = self
            .stat(&expected.key)?
            .ok_or_else(|| Error::NotFound(expected.key.clone()))?;
        verify_metadata(expected, &metadata)?;
        match metadata.provider_sha256 {
            Some(_) => Ok(metadata),
            None => {
                let mut object = self.open(&expected.key)?;
                verify_metadata(expected, &object.metadata)?;
                if object.metadata.version != metadata.version {
                    return Err(Error::VersionMismatch(expected.key.clone()));
                }
                verify_reader(
                    object.body.as_mut(),
                    expected.encoded_len,
                    expected.encoded_sha256,
                )?;
                Ok(object.metadata)
            }
        }
    }

    /// Returns a stable lexicographic snapshot. Listing is for audit and
    /// discovery only; protocol recovery must use its exact checkpoint/index.
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>>;

    /// Deletes only the requested immutable key. When `expected_version` is
    /// present, a mismatch fails closed.
    fn delete(
        &self,
        key: &ObjectKey,
        expected_version: Option<&ObjectVersion>,
    ) -> Result<DeleteDisposition>;
}

#[derive(Debug)]
pub enum Error {
    InvalidKey(&'static str),
    InvalidVersion,
    CapacityExceeded(u64),
    LengthMismatch {
        expected: u64,
        actual: u64,
    },
    DigestMismatch {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    MetadataKeyMismatch {
        expected: ObjectKey,
        actual: ObjectKey,
    },
    ImmutableCollision(ObjectKey),
    VersionMismatch(ObjectKey),
    NotFound(ObjectKey),
    LockPoisoned,
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKey(reason) => write!(formatter, "invalid object key: {reason}"),
            Self::InvalidVersion => formatter.write_str("invalid object version"),
            Self::CapacityExceeded(bytes) => {
                write!(
                    formatter,
                    "object is too large for the in-memory store: {bytes}"
                )
            }
            Self::LengthMismatch { expected, actual } => {
                write!(
                    formatter,
                    "object length mismatch: expected {expected}, got {actual}"
                )
            }
            Self::DigestMismatch { .. } => formatter.write_str("object SHA-256 mismatch"),
            Self::MetadataKeyMismatch { expected, actual } => {
                write!(
                    formatter,
                    "object metadata key mismatch: expected {expected}, got {actual}"
                )
            }
            Self::ImmutableCollision(key) => {
                write!(formatter, "immutable object key collision at {key}")
            }
            Self::VersionMismatch(key) => write!(formatter, "object version mismatch at {key}"),
            Self::NotFound(key) => write!(formatter, "object not found: {key}"),
            Self::LockPoisoned => formatter.write_str("object store lock is poisoned"),
            Self::Io(error) => write!(formatter, "object store I/O failed: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct MemoryObjectStore {
    state: Arc<MemoryState>,
    attest_sha256: bool,
}

struct MemoryState {
    objects: RwLock<BTreeMap<ObjectKey, MemoryObject>>,
    next_version: AtomicU64,
}

#[derive(Clone)]
struct MemoryObject {
    bytes: Arc<[u8]>,
    sha256: [u8; 32],
    version: ObjectVersion,
}

impl Default for MemoryObjectStore {
    fn default() -> Self {
        Self::new(true)
    }
}

impl MemoryObjectStore {
    pub fn new(attest_sha256: bool) -> Self {
        Self {
            state: Arc::new(MemoryState {
                objects: RwLock::new(BTreeMap::new()),
                next_version: AtomicU64::new(1),
            }),
            attest_sha256,
        }
    }

    fn metadata(&self, key: &ObjectKey, object: &MemoryObject) -> ObjectMetadata {
        ObjectMetadata {
            key: key.clone(),
            encoded_len: object.bytes.len() as u64,
            provider_sha256: self.attest_sha256.then_some(object.sha256),
            version: Some(object.version.clone()),
        }
    }
}

impl ImmutableObjectStore for MemoryObjectStore {
    fn create_if_absent(
        &self,
        request: &CreateRequest,
        source: &mut dyn Read,
    ) -> Result<CreateResult> {
        let mut objects = self
            .state
            .objects
            .write()
            .map_err(|_| Error::LockPoisoned)?;
        if let Some(existing) = objects.get(&request.key) {
            if existing.bytes.len() as u64 != request.encoded_len
                || existing.sha256 != request.encoded_sha256
            {
                return Err(Error::ImmutableCollision(request.key.clone()));
            }
            return Ok(CreateResult {
                disposition: CreateDisposition::AlreadyExisted,
                metadata: self.metadata(&request.key, existing),
            });
        }

        let bytes = read_and_verify(source, request.encoded_len, request.encoded_sha256)?;
        let ordinal = self.state.next_version.fetch_add(1, Ordering::Relaxed);
        let version = ObjectVersion::new(ordinal.to_be_bytes().to_vec())?;
        let object = MemoryObject {
            bytes: Arc::from(bytes),
            sha256: request.encoded_sha256,
            version,
        };
        let metadata = self.metadata(&request.key, &object);
        objects.insert(request.key.clone(), object);
        Ok(CreateResult {
            disposition: CreateDisposition::Created,
            metadata,
        })
    }

    fn open(&self, key: &ObjectKey) -> Result<ObjectRead> {
        let objects = self.state.objects.read().map_err(|_| Error::LockPoisoned)?;
        let object = objects
            .get(key)
            .ok_or_else(|| Error::NotFound(key.clone()))?;
        Ok(ObjectRead {
            metadata: self.metadata(key, object),
            body: Box::new(Cursor::new(object.bytes.clone())),
        })
    }

    fn stat(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>> {
        let objects = self.state.objects.read().map_err(|_| Error::LockPoisoned)?;
        Ok(objects.get(key).map(|object| self.metadata(key, object)))
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>> {
        validate_prefix(prefix)?;
        let objects = self.state.objects.read().map_err(|_| Error::LockPoisoned)?;
        Ok(objects
            .range(ObjectKey(prefix.to_owned())..)
            .take_while(|(key, _)| key.as_str().starts_with(prefix))
            .map(|(key, object)| self.metadata(key, object))
            .collect())
    }

    fn delete(
        &self,
        key: &ObjectKey,
        expected_version: Option<&ObjectVersion>,
    ) -> Result<DeleteDisposition> {
        let mut objects = self
            .state
            .objects
            .write()
            .map_err(|_| Error::LockPoisoned)?;
        let Some(object) = objects.get(key) else {
            return Ok(DeleteDisposition::NotFound);
        };
        if expected_version.is_some_and(|version| version != &object.version) {
            return Err(Error::VersionMismatch(key.clone()));
        }
        objects.remove(key);
        Ok(DeleteDisposition::Deleted)
    }
}

pub struct FilesystemObjectStore {
    root: PathBuf,
    objects_root: PathBuf,
    temporary_root: PathBuf,
    temporary_counter: AtomicU64,
}

impl FilesystemObjectStore {
    pub fn open_root(root: impl AsRef<Path>) -> Result<Self> {
        let requested_root = root.as_ref();
        fs::create_dir_all(requested_root)?;
        let metadata = fs::symlink_metadata(requested_root)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(Error::InvalidKey(
                "filesystem store root must be a directory",
            ));
        }
        let root = fs::canonicalize(requested_root)?;
        let objects_root = ensure_internal_directory(&root, OBJECTS_DIRECTORY)?;
        let temporary_root = ensure_internal_directory(&root, TEMPORARY_DIRECTORY)?;
        fsync_directory(&root)?;
        Ok(Self {
            root,
            objects_root,
            temporary_root,
            temporary_counter: AtomicU64::new(1),
        })
    }

    fn path_for(&self, key: &ObjectKey) -> PathBuf {
        self.objects_root.join(key.to_relative_path())
    }

    fn metadata_for_path(&self, key: &ObjectKey, path: &Path) -> Result<ObjectMetadata> {
        let mut file = open_regular_file(path)?;
        let (encoded_len, sha256) = hash_reader(&mut file)?;
        Ok(ObjectMetadata {
            key: key.clone(),
            encoded_len,
            provider_sha256: Some(sha256),
            version: None,
        })
    }

    fn temporary_path(&self) -> PathBuf {
        let sequence = self.temporary_counter.fetch_add(1, Ordering::Relaxed);
        self.temporary_root
            .join(format!(".hive-put-{}-{sequence:016x}", std::process::id()))
    }

    fn create_temporary_file(&self) -> Result<(PathBuf, File)> {
        loop {
            let path = self.temporary_path();
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(file) => return Ok((path, file)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(Error::Io(error)),
            }
        }
    }

    fn validate_layout(&self) -> Result<()> {
        validate_directory(&self.root, "filesystem store root is not a directory")?;
        validate_directory(
            &self.objects_root,
            "filesystem object namespace is not a directory",
        )?;
        validate_directory(
            &self.temporary_root,
            "filesystem temporary namespace is not a directory",
        )
    }
}

impl ImmutableObjectStore for FilesystemObjectStore {
    fn create_if_absent(
        &self,
        request: &CreateRequest,
        source: &mut dyn Read,
    ) -> Result<CreateResult> {
        self.validate_layout()?;
        let destination = self.path_for(&request.key);
        ensure_directory_chain(&self.objects_root, &request.key)?;
        match fs::symlink_metadata(&destination) {
            Ok(_) => return exact_existing_result(self, request, &destination),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::Io(error)),
        }
        let parent = destination
            .parent()
            .ok_or(Error::InvalidKey("object key has no parent"))?;
        let (temporary, mut file) = self.create_temporary_file()?;
        let operation = (|| -> Result<CreateResult> {
            copy_and_verify(
                source,
                &mut file,
                request.encoded_len,
                request.encoded_sha256,
            )?;
            file.sync_all()?;
            drop(file);

            match fs::hard_link(&temporary, &destination) {
                Ok(()) => {
                    fsync_directory(parent)?;
                    Ok(CreateResult {
                        disposition: CreateDisposition::Created,
                        metadata: ObjectMetadata {
                            key: request.key.clone(),
                            encoded_len: request.encoded_len,
                            provider_sha256: Some(request.encoded_sha256),
                            version: None,
                        },
                    })
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    exact_existing_result(self, request, &destination)
                }
                Err(error) => Err(Error::Io(error)),
            }
        })();
        let cleanup = remove_temporary_file(&temporary, &self.temporary_root);
        match (operation, cleanup) {
            (Ok(result), Ok(())) => Ok(result),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), _) => Err(error),
        }
    }

    fn open(&self, key: &ObjectKey) -> Result<ObjectRead> {
        self.validate_layout()?;
        if !existing_directory_chain(&self.objects_root, key)? {
            return Err(Error::NotFound(key.clone()));
        }
        let path = self.path_for(key);
        let mut file = match open_regular_file(&path) {
            Ok(file) => file,
            Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Err(Error::NotFound(key.clone()));
            }
            Err(error) => return Err(error),
        };
        let (encoded_len, sha256) = hash_reader(&mut file)?;
        file.rewind()?;
        let metadata = ObjectMetadata {
            key: key.clone(),
            encoded_len,
            provider_sha256: Some(sha256),
            version: None,
        };
        Ok(ObjectRead {
            metadata,
            body: Box::new(file),
        })
    }

    fn stat(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>> {
        self.validate_layout()?;
        if !existing_directory_chain(&self.objects_root, key)? {
            return Ok(None);
        }
        let path = self.path_for(key);
        match fs::symlink_metadata(&path) {
            Ok(_) => self.metadata_for_path(key, &path).map(Some),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::Io(error)),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>> {
        validate_prefix(prefix)?;
        self.validate_layout()?;
        let mut keys = Vec::new();
        collect_files(&self.objects_root, &self.objects_root, &mut keys)?;
        keys.sort();
        keys.into_iter()
            .filter(|key| key.as_str().starts_with(prefix))
            .map(|key| {
                let path = self.path_for(&key);
                self.metadata_for_path(&key, &path)
            })
            .collect()
    }

    fn delete(
        &self,
        key: &ObjectKey,
        expected_version: Option<&ObjectVersion>,
    ) -> Result<DeleteDisposition> {
        self.validate_layout()?;
        if !existing_directory_chain(&self.objects_root, key)? {
            return Ok(DeleteDisposition::NotFound);
        }
        let path = self.path_for(key);
        match fs::symlink_metadata(&path) {
            Ok(metadata)
                if metadata.file_type().is_symlink() || !metadata.file_type().is_file() =>
            {
                return Err(Error::InvalidKey("object path must be a regular file"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(DeleteDisposition::NotFound);
            }
            Err(error) => return Err(Error::Io(error)),
        }
        if expected_version.is_some() {
            return Err(Error::VersionMismatch(key.clone()));
        }
        match fs::remove_file(&path) {
            Ok(()) => {
                if let Some(parent) = path.parent() {
                    fsync_directory(parent)?;
                }
                Ok(DeleteDisposition::Deleted)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok(DeleteDisposition::NotFound)
            }
            Err(error) => Err(Error::Io(error)),
        }
    }
}

fn exact_existing_result(
    store: &FilesystemObjectStore,
    request: &CreateRequest,
    path: &Path,
) -> Result<CreateResult> {
    let metadata = store.metadata_for_path(&request.key, path)?;
    if metadata.encoded_len != request.encoded_len
        || metadata.provider_sha256 != Some(request.encoded_sha256)
    {
        return Err(Error::ImmutableCollision(request.key.clone()));
    }
    Ok(CreateResult {
        disposition: CreateDisposition::AlreadyExisted,
        metadata,
    })
}

fn verify_metadata(expected: &CreateRequest, metadata: &ObjectMetadata) -> Result<()> {
    if metadata.key != expected.key {
        return Err(Error::MetadataKeyMismatch {
            expected: expected.key.clone(),
            actual: metadata.key.clone(),
        });
    }
    if metadata.encoded_len != expected.encoded_len {
        return Err(Error::LengthMismatch {
            expected: expected.encoded_len,
            actual: metadata.encoded_len,
        });
    }
    if let Some(actual) = metadata.provider_sha256
        && actual != expected.encoded_sha256
    {
        return Err(Error::DigestMismatch {
            expected: expected.encoded_sha256,
            actual,
        });
    }
    Ok(())
}

fn validate_key(value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidKey("key is empty"));
    }
    if value.len() > MAX_OBJECT_KEY_BYTES {
        return Err(Error::InvalidKey("key is too long"));
    }
    if value.starts_with('/') || value.ends_with('/') {
        return Err(Error::InvalidKey("key must be relative and name an object"));
    }
    if value.contains('\0') || value.contains('\\') {
        return Err(Error::InvalidKey("key contains a forbidden character"));
    }
    if value
        .split('/')
        .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(Error::InvalidKey("key contains an unsafe path component"));
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<()> {
    if prefix.len() > MAX_OBJECT_KEY_BYTES || prefix.contains('\0') || prefix.contains('\\') {
        return Err(Error::InvalidKey("invalid list prefix"));
    }
    if prefix.starts_with('/') || prefix.split('/').any(|part| part == "." || part == "..") {
        return Err(Error::InvalidKey("unsafe list prefix"));
    }
    Ok(())
}

fn read_and_verify(
    source: &mut dyn Read,
    expected_len: u64,
    expected_sha256: [u8; 32],
) -> Result<Vec<u8>> {
    let capacity =
        usize::try_from(expected_len).map_err(|_| Error::CapacityExceeded(expected_len))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| Error::CapacityExceeded(expected_len))?;
    copy_and_verify(source, &mut bytes, expected_len, expected_sha256)?;
    Ok(bytes)
}

fn verify_reader(
    source: &mut dyn Read,
    expected_len: u64,
    expected_sha256: [u8; 32],
) -> Result<()> {
    copy_and_verify(source, &mut io::sink(), expected_len, expected_sha256)
}

fn copy_and_verify(
    source: &mut dyn Read,
    destination: &mut dyn Write,
    expected_len: u64,
    expected_sha256: [u8; 32],
) -> Result<()> {
    let mut hasher = Sha256::new();
    let mut actual_len = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = match source.read(&mut buffer) {
            Ok(read) => read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(Error::Io(error)),
        };
        if read == 0 {
            break;
        }
        actual_len = actual_len
            .checked_add(read as u64)
            .ok_or(Error::LengthMismatch {
                expected: expected_len,
                actual: u64::MAX,
            })?;
        if actual_len > expected_len {
            return Err(Error::LengthMismatch {
                expected: expected_len,
                actual: actual_len,
            });
        }
        hasher.update(&buffer[..read]);
        destination.write_all(&buffer[..read])?;
    }
    if actual_len != expected_len {
        return Err(Error::LengthMismatch {
            expected: expected_len,
            actual: actual_len,
        });
    }
    let actual: [u8; 32] = hasher.finalize().into();
    if actual != expected_sha256 {
        return Err(Error::DigestMismatch {
            expected: expected_sha256,
            actual,
        });
    }
    Ok(())
}

fn hash_reader(file: &mut File) -> Result<(u64, [u8; 32])> {
    let mut hasher = Sha256::new();
    let mut encoded_len = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = match file.read(&mut buffer) {
            Ok(read) => read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(Error::Io(error)),
        };
        if read == 0 {
            break;
        }
        encoded_len = encoded_len
            .checked_add(read as u64)
            .ok_or(Error::LengthMismatch {
                expected: u64::MAX,
                actual: u64::MAX,
            })?;
        hasher.update(&buffer[..read]);
    }
    Ok((encoded_len, hasher.finalize().into()))
}

fn open_regular_file(path: &Path) -> Result<File> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(Error::InvalidKey("object path must be a regular file"));
    }
    let file = OpenOptions::new().read(true).open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(Error::InvalidKey("object path must be a regular file"));
    }
    Ok(file)
}

fn ensure_internal_directory(root: &Path, name: &str) -> Result<PathBuf> {
    let directory = root.join(name);
    match fs::symlink_metadata(&directory) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
                return Err(Error::InvalidKey(
                    "filesystem store namespace must be a directory",
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => match fs::create_dir(&directory) {
            Ok(()) => fsync_directory(root)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                validate_directory(&directory, "filesystem store namespace must be a directory")?;
            }
            Err(error) => return Err(Error::Io(error)),
        },
        Err(error) => return Err(Error::Io(error)),
    }
    fsync_directory(&directory)?;
    Ok(directory)
}

fn validate_directory(path: &Path, reason: &'static str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        return Err(Error::InvalidKey(reason));
    }
    Ok(())
}

fn existing_directory_chain(root: &Path, key: &ObjectKey) -> Result<bool> {
    validate_directory(root, "filesystem object namespace is not a directory")?;
    let components = key.as_str().split('/').collect::<Vec<_>>();
    let mut directory = root.to_path_buf();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        directory.push(component);
        match fs::symlink_metadata(&directory) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
                    return Err(Error::InvalidKey(
                        "object parent contains a non-directory or symlink",
                    ));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(Error::Io(error)),
        }
    }
    Ok(true)
}

fn remove_temporary_file(path: &Path, temporary_root: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => fsync_directory(temporary_root),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::Io(error)),
    }
}

fn collect_files(root: &Path, directory: &Path, keys: &mut Vec<ObjectKey>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_symlink() {
            return Err(Error::InvalidKey("filesystem store contains a symlink"));
        }
        if file_type.is_dir() {
            collect_files(root, &path, keys)?;
        } else if file_type.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| Error::InvalidKey("object escaped store root"))?;
            let key = relative
                .iter()
                .map(|part| {
                    part.to_str()
                        .ok_or(Error::InvalidKey("object key is not valid UTF-8"))
                })
                .collect::<Result<Vec<_>>>()?
                .join("/");
            keys.push(ObjectKey::new(key)?);
        } else {
            return Err(Error::InvalidKey(
                "filesystem store contains a special file",
            ));
        }
    }
    Ok(())
}

fn ensure_directory_chain(root: &Path, key: &ObjectKey) -> Result<()> {
    validate_directory(root, "filesystem object namespace is not a directory")?;
    let components = key.as_str().split('/').collect::<Vec<_>>();
    let mut directory = root.to_path_buf();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        directory.push(component);
        match fs::symlink_metadata(&directory) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
                    return Err(Error::InvalidKey(
                        "object parent contains a non-directory or symlink",
                    ));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match fs::create_dir(&directory) {
                    Ok(()) => {
                        if let Some(parent) = directory.parent() {
                            fsync_directory(parent)?;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        validate_directory(
                            &directory,
                            "object parent contains a non-directory or symlink",
                        )?;
                    }
                    Err(error) => return Err(Error::Io(error)),
                }
            }
            Err(error) => return Err(Error::Io(error)),
        }
    }
    Ok(())
}

fn fsync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    struct BarrierReader {
        barrier: Arc<Barrier>,
        bytes: Cursor<Vec<u8>>,
        synchronized: bool,
    }

    impl BarrierReader {
        fn new(barrier: Arc<Barrier>, bytes: &[u8]) -> Self {
            Self {
                barrier,
                bytes: Cursor::new(bytes.to_vec()),
                synchronized: false,
            }
        }
    }

    impl Read for BarrierReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if !self.synchronized {
                self.synchronized = true;
                self.barrier.wait();
            }
            self.bytes.read(buffer)
        }
    }

    fn request(key: &str, bytes: &[u8]) -> CreateRequest {
        CreateRequest {
            key: ObjectKey::new(key).unwrap(),
            encoded_len: bytes.len() as u64,
            encoded_sha256: Sha256::digest(bytes).into(),
        }
    }

    fn assert_store_contract(store: &dyn ImmutableObjectStore) {
        let alpha = b"alpha";
        let beta = b"beta";
        let alpha_request = request("streams/01/alpha", alpha);
        let beta_request = request("streams/01/beta", beta);

        let created = store
            .create_if_absent(&alpha_request, &mut &alpha[..])
            .unwrap();
        assert_eq!(created.disposition, CreateDisposition::Created);

        let repeated = store
            .create_if_absent(&alpha_request, &mut io::empty())
            .unwrap();
        assert_eq!(repeated.disposition, CreateDisposition::AlreadyExisted);

        let collision = request("streams/01/alpha", beta);
        assert!(matches!(
            store.create_if_absent(&collision, &mut &beta[..]),
            Err(Error::ImmutableCollision(_))
        ));

        store
            .create_if_absent(&beta_request, &mut &beta[..])
            .unwrap();
        let listed = store.list("streams/01/").unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|item| item.key.as_str())
                .collect::<Vec<_>>(),
            vec!["streams/01/alpha", "streams/01/beta"]
        );

        let mut read = store.open(&alpha_request.key).unwrap();
        let mut bytes = Vec::new();
        read.body.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, alpha);

        assert_eq!(
            store
                .delete(&alpha_request.key, read.metadata.version.as_ref())
                .unwrap(),
            DeleteDisposition::Deleted
        );
        assert!(store.stat(&alpha_request.key).unwrap().is_none());
        assert_eq!(
            store.delete(&alpha_request.key, None).unwrap(),
            DeleteDisposition::NotFound
        );
        let unknown_version = ObjectVersion::new(vec![0x42]).unwrap();
        assert_eq!(
            store
                .delete(&alpha_request.key, Some(&unknown_version))
                .unwrap(),
            DeleteDisposition::NotFound
        );
    }

    fn assert_invalid_sources_are_not_published(store: &dyn ImmutableObjectStore) {
        let key = ObjectKey::new("object").unwrap();
        let expected = request("object", b"right");

        assert!(matches!(
            store.create_if_absent(&expected, &mut &b"no"[..]),
            Err(Error::LengthMismatch { .. })
        ));
        assert!(store.stat(&key).unwrap().is_none());

        assert!(matches!(
            store.create_if_absent(&expected, &mut &b"too-long"[..]),
            Err(Error::LengthMismatch { .. })
        ));
        assert!(store.stat(&key).unwrap().is_none());

        assert!(matches!(
            store.create_if_absent(&expected, &mut &b"wrong"[..]),
            Err(Error::DigestMismatch { .. })
        ));
        assert!(store.stat(&key).unwrap().is_none());
        assert!(store.list("").unwrap().is_empty());
    }

    #[test]
    fn memory_store_obeys_the_immutable_contract() {
        assert_store_contract(&MemoryObjectStore::default());
    }

    #[test]
    fn memory_store_can_require_readback_verification() {
        let store = MemoryObjectStore::new(false);
        let bytes = b"payload";
        let result = store
            .create_if_absent(&request("object", bytes), &mut &bytes[..])
            .unwrap();
        assert_eq!(result.metadata.provider_sha256, None);
        assert_eq!(
            store.verify_exact(&request("object", bytes)).unwrap().key,
            ObjectKey::new("object").unwrap()
        );
        let mut read = store.open(&ObjectKey::new("object").unwrap()).unwrap();
        let mut restored = Vec::new();
        read.body.read_to_end(&mut restored).unwrap();
        assert_eq!(
            Sha256::digest(&restored).as_slice(),
            Sha256::digest(bytes).as_slice()
        );
    }

    #[test]
    fn filesystem_store_obeys_the_immutable_contract() {
        let temporary = tempfile::tempdir().unwrap();
        let store = FilesystemObjectStore::open_root(temporary.path()).unwrap();
        assert_store_contract(&store);
    }

    #[test]
    fn filesystem_conditional_create_is_atomic_across_store_handles() {
        let temporary = tempfile::tempdir().unwrap();
        let first = FilesystemObjectStore::open_root(temporary.path()).unwrap();
        let second = FilesystemObjectStore::open_root(temporary.path()).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let bytes = b"same immutable bytes";
        let expected = request("concurrent/object", bytes);

        let first_request = expected.clone();
        let first_barrier = Arc::clone(&barrier);
        let first_thread = std::thread::spawn(move || {
            first.create_if_absent(
                &first_request,
                &mut BarrierReader::new(first_barrier, bytes),
            )
        });
        let second_request = expected.clone();
        let second_thread = std::thread::spawn(move || {
            second.create_if_absent(&second_request, &mut BarrierReader::new(barrier, bytes))
        });

        let first_result = first_thread.join().unwrap().unwrap();
        let second_result = second_thread.join().unwrap().unwrap();
        let mut dispositions = [first_result.disposition, second_result.disposition];
        dispositions.sort_by_key(|disposition| match disposition {
            CreateDisposition::Created => 0,
            CreateDisposition::AlreadyExisted => 1,
        });
        assert_eq!(
            dispositions,
            [
                CreateDisposition::Created,
                CreateDisposition::AlreadyExisted
            ]
        );

        let reopened = FilesystemObjectStore::open_root(temporary.path()).unwrap();
        assert_eq!(reopened.verify_exact(&expected).unwrap().encoded_len, 20);
        assert!(
            fs::read_dir(&reopened.temporary_root)
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    fn filesystem_stale_temporary_files_do_not_collide_or_appear_in_lists() {
        let temporary = tempfile::tempdir().unwrap();
        let store = FilesystemObjectStore::open_root(temporary.path()).unwrap();
        let stale =
            store
                .temporary_root
                .join(format!(".hive-put-{}-{:016x}", std::process::id(), 1));
        fs::write(&stale, b"stale").unwrap();

        let bytes = b"object";
        assert_eq!(
            store
                .create_if_absent(&request("key", bytes), &mut &bytes[..])
                .unwrap()
                .disposition,
            CreateDisposition::Created
        );
        assert_eq!(
            store
                .list("")
                .unwrap()
                .into_iter()
                .map(|metadata| metadata.key)
                .collect::<Vec<_>>(),
            vec![ObjectKey::new("key").unwrap()]
        );
        assert!(stale.exists());
    }

    #[test]
    fn rejects_short_long_and_wrong_digest_sources_without_publication() {
        assert_invalid_sources_are_not_published(&MemoryObjectStore::default());

        let temporary = tempfile::tempdir().unwrap();
        let filesystem = FilesystemObjectStore::open_root(temporary.path()).unwrap();
        assert_invalid_sources_are_not_published(&filesystem);
        assert!(
            fs::read_dir(&filesystem.temporary_root)
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    fn key_validation_prevents_root_escape() {
        for key in [
            "",
            "/absolute",
            "trailing/",
            "a//b",
            "a/../b",
            "a/./b",
            "a\\b",
        ] {
            assert!(ObjectKey::new(key).is_err(), "accepted {key:?}");
        }
        assert!(ObjectKey::new("x".repeat(MAX_OBJECT_KEY_BYTES)).is_ok());
        assert!(ObjectKey::new("x".repeat(MAX_OBJECT_KEY_BYTES + 1)).is_err());
    }

    #[test]
    fn version_condition_fails_closed() {
        let bytes = b"value";
        let wrong = ObjectVersion::new(99u64.to_be_bytes().to_vec()).unwrap();

        let memory = MemoryObjectStore::default();
        let memory_request = request("object", bytes);
        memory
            .create_if_absent(&memory_request, &mut &bytes[..])
            .unwrap();
        assert!(matches!(
            memory.delete(&memory_request.key, Some(&wrong)),
            Err(Error::VersionMismatch(_))
        ));
        assert!(memory.stat(&memory_request.key).unwrap().is_some());

        let temporary = tempfile::tempdir().unwrap();
        let filesystem = FilesystemObjectStore::open_root(temporary.path()).unwrap();
        let filesystem_request = request("object", bytes);
        filesystem
            .create_if_absent(&filesystem_request, &mut &bytes[..])
            .unwrap();
        assert!(matches!(
            filesystem.delete(&filesystem_request.key, Some(&wrong)),
            Err(Error::VersionMismatch(_))
        ));
        assert!(filesystem.stat(&filesystem_request.key).unwrap().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_store_rejects_a_symlinked_key_parent() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let store = FilesystemObjectStore::open_root(temporary.path()).unwrap();
        symlink(outside.path(), store.objects_root.join("escape")).unwrap();
        let bytes = b"value";
        let key = ObjectKey::new("escape/object").unwrap();
        assert!(matches!(
            store.create_if_absent(&request("escape/object", bytes), &mut &bytes[..]),
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(store.open(&key), Err(Error::InvalidKey(_))));
        assert!(matches!(store.stat(&key), Err(Error::InvalidKey(_))));
        assert!(matches!(
            store.delete(&key, None),
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(store.list(""), Err(Error::InvalidKey(_))));
        assert!(!outside.path().join("object").exists());
    }
}
