//! Local and pinned immutable byte sources.

use std::{
    collections::HashMap,
    fs::File,
    io,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use rustix::fs::{Mode, OFlags};
use serde::Serialize;

use blockzilla_source::{RangeSource, SourceError, SourceResult, validate_object_name};

#[derive(Debug, Clone)]
pub struct LocalRangeSource {
    root: PathBuf,
}

impl LocalRangeSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, object: &str) -> SourceResult<PathBuf> {
        validate_object_name(object).map_err(|_| SourceError::InvalidName(object.to_owned()))?;
        Ok(self.root.join(object))
    }
}

impl RangeSource for LocalRangeSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        let path = self.path(object)?;
        match std::fs::metadata(&path) {
            Ok(metadata) => {
                if !metadata.is_file() {
                    return Err(SourceError::Protocol(format!(
                        "{} is not a regular file",
                        path.display()
                    )));
                }
                Ok(Some(metadata.len()))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(SourceError::Io {
                object: object.to_owned(),
                source,
            }),
        }
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        let mut bytes = Vec::new();
        self.read_range_into(object, offset, length, &mut bytes)?;
        Ok(bytes)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        bytes: &mut Vec<u8>,
    ) -> SourceResult<()> {
        // Preserve initialized storage across sequential range reads. Clearing
        // before resizing would zero the entire reused batch even though the
        // following `read_at` loop overwrites every requested byte.
        if bytes.len() < length {
            bytes.resize(length, 0);
        } else {
            bytes.truncate(length);
        }
        self.read_range_into_slice(object, offset, bytes)
    }

    fn read_range_into_slice(
        &self,
        object: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> SourceResult<()> {
        let path = self.path(object)?;
        let file = File::open(&path).map_err(|source| {
            if source.kind() == io::ErrorKind::NotFound {
                SourceError::NotFound(object.to_owned())
            } else {
                SourceError::Io {
                    object: object.to_owned(),
                    source,
                }
            }
        })?;
        let size = file
            .metadata()
            .map_err(|source| SourceError::Io {
                object: object.to_owned(),
                source,
            })?
            .len();
        let length = bytes.len();
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

        let mut read = 0usize;
        while read < length {
            let read_offset = offset + read as u64;
            let count = file
                .read_at(&mut bytes[read..], read_offset)
                .map_err(|source| SourceError::Io {
                    object: object.to_owned(),
                    source,
                })?;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UnixFileIdentity {
    device: u64,
    inode: u64,
    size: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

/// Stable local-file identity captured when an anchored source opens an object.
///
/// This is metadata, not a content hash. A durable consumer can store these
/// values and refuse resume after a pathname is replaced, even when the new
/// file has the same length.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PinnedLocalObjectIdentity {
    pub object: String,
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub modified_seconds: i64,
    pub modified_nanoseconds: i64,
    pub changed_seconds: i64,
    pub changed_nanoseconds: i64,
}

impl UnixFileIdentity {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.len(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    }
}

#[derive(Debug, Clone)]
struct PinnedFile {
    file: Arc<File>,
    identity: UnixFileIdentity,
}

/// Successful local range reads performed through a pinned source.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PinnedLocalRangeSourceStats {
    pub read_calls: u64,
    pub read_bytes: u64,
}

#[derive(Debug, Default)]
struct PinnedLocalRangeSourceCounters {
    read_calls: AtomicU64,
    read_bytes: AtomicU64,
}

/// Random-access source that pins every opened object to one file descriptor.
///
/// This is intended for long-running local archive scans. The first lookup of
/// an object opens it and captures its Unix identity; later size and range
/// reads use that same handle even if the pathname is replaced. Missing
/// objects are cached as missing because a published generation is immutable.
#[derive(Debug, Clone)]
pub struct PinnedLocalRangeSource {
    root: PathBuf,
    root_directory: Option<Arc<File>>,
    allowed_objects: Option<Arc<HashMap<String, ()>>>,
    files: Arc<Mutex<HashMap<String, Option<PinnedFile>>>>,
    counters: Arc<PinnedLocalRangeSourceCounters>,
}

impl PinnedLocalRangeSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            root_directory: None,
            allowed_objects: None,
            files: Arc::new(Mutex::new(HashMap::new())),
            counters: Arc::new(PinnedLocalRangeSourceCounters::default()),
        }
    }

    /// Open and pin the source directory before any object lookup.
    ///
    /// Every later object open is relative to this descriptor. Replacing the
    /// pathname cannot make one source instance mix objects from two directory
    /// generations.
    pub fn new_anchored(root: impl Into<PathBuf>, allowed_objects: &[&str]) -> SourceResult<Self> {
        const MAX_ALLOWED_OBJECTS: usize = 64;
        if allowed_objects.is_empty() || allowed_objects.len() > MAX_ALLOWED_OBJECTS {
            return Err(SourceError::Protocol(format!(
                "anchored local source allowlist must contain 1..={MAX_ALLOWED_OBJECTS} objects"
            )));
        }
        let mut allowed = HashMap::with_capacity(allowed_objects.len());
        for object in allowed_objects {
            validate_object_name(object)
                .map_err(|_| SourceError::InvalidName((*object).to_owned()))?;
            if allowed.insert((*object).to_owned(), ()).is_some() {
                return Err(SourceError::Protocol(format!(
                    "anchored local source allowlist contains duplicate object {object}"
                )));
            }
        }
        let root = root.into();
        let directory = rustix::fs::open(
            &root,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .map(File::from)
        .map_err(|error| SourceError::Io {
            object: root.display().to_string(),
            source: io::Error::from(error),
        })?;
        let metadata = directory.metadata().map_err(|source| SourceError::Io {
            object: root.display().to_string(),
            source,
        })?;
        if !metadata.is_dir() {
            return Err(SourceError::Protocol(format!(
                "{} is not a directory",
                root.display()
            )));
        }
        Ok(Self {
            root,
            root_directory: Some(Arc::new(directory)),
            allowed_objects: Some(Arc::new(allowed)),
            files: Arc::new(Mutex::new(HashMap::new())),
            counters: Arc::new(PinnedLocalRangeSourceCounters::default()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return successful range-read counters shared by all source clones.
    pub fn stats(&self) -> PinnedLocalRangeSourceStats {
        PinnedLocalRangeSourceStats {
            read_calls: self.counters.read_calls.load(Ordering::Relaxed),
            read_bytes: self.counters.read_bytes.load(Ordering::Relaxed),
        }
    }

    /// Clone the pinned source-directory descriptor, when this source was
    /// created with [`Self::new_anchored`].
    pub fn pinned_root_file_clone(&self) -> SourceResult<Option<File>> {
        self.root_directory
            .as_ref()
            .map(|directory| {
                directory.try_clone().map_err(|source| SourceError::Io {
                    object: self.root.display().to_string(),
                    source,
                })
            })
            .transpose()
    }

    /// Clone the descriptor pinned for one immutable object.
    ///
    /// The returned handle refers to the same inode that range reads and
    /// manifest verification use. A caller can therefore use APIs that accept
    /// `File` without reopening a pathname and creating a replacement race.
    pub fn pinned_file_clone(&self, object: &str) -> SourceResult<Option<File>> {
        self.pinned_file(object)?
            .map(|pinned| {
                pinned.file.try_clone().map_err(|source| SourceError::Io {
                    object: object.to_owned(),
                    source,
                })
            })
            .transpose()
    }

    /// Return the sorted identities of every present object admitted so far.
    pub fn pinned_object_identities(&self) -> SourceResult<Vec<PinnedLocalObjectIdentity>> {
        let files = self.files.lock().map_err(|_| {
            SourceError::Protocol("pinned local source file cache is poisoned".to_owned())
        })?;
        let mut identities = files
            .iter()
            .filter_map(|(object, file)| {
                let identity = file.as_ref()?.identity;
                Some(PinnedLocalObjectIdentity {
                    object: object.clone(),
                    device: identity.device,
                    inode: identity.inode,
                    size: identity.size,
                    modified_seconds: identity.modified_seconds,
                    modified_nanoseconds: identity.modified_nanoseconds,
                    changed_seconds: identity.changed_seconds,
                    changed_nanoseconds: identity.changed_nanoseconds,
                })
            })
            .collect::<Vec<_>>();
        identities.sort_unstable_by(|left, right| left.object.cmp(&right.object));
        Ok(identities)
    }

    fn path(&self, object: &str) -> SourceResult<PathBuf> {
        validate_object_name(object).map_err(|_| SourceError::InvalidName(object.to_owned()))?;
        Ok(self.root.join(object))
    }

    fn pinned_file(&self, object: &str) -> SourceResult<Option<PinnedFile>> {
        if let Some(allowed) = &self.allowed_objects
            && !allowed.contains_key(object)
        {
            return Err(SourceError::Protocol(format!(
                "object {object} is outside the anchored local source allowlist"
            )));
        }
        let root_directory = self.root_directory.clone();
        self.pinned_file_with(object, move |path| {
            let descriptor = if let Some(directory) = root_directory {
                rustix::fs::openat(
                    directory.as_ref(),
                    object,
                    OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                    Mode::empty(),
                )
            } else {
                rustix::fs::open(
                    path,
                    OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                    Mode::empty(),
                )
            };
            descriptor.map(File::from).map_err(io::Error::from)
        })
    }

    fn pinned_file_with(
        &self,
        object: &str,
        open: impl FnOnce(&Path) -> io::Result<File>,
    ) -> SourceResult<Option<PinnedFile>> {
        let path = self.path(object)?;
        {
            let files = self.files.lock().map_err(|_| {
                SourceError::Protocol("pinned local source file cache is poisoned".to_owned())
            })?;
            if let Some(file) = files.get(object) {
                return Ok(file.clone());
            }
        }

        // Opening a local/NAS object can involve filesystem I/O, so never hold the source-wide
        // cache mutex across it. Multiple first-open racers are reconciled below: whichever
        // result enters the cache first becomes the one pinned identity returned to every caller.
        let file = match open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut files = self.files.lock().map_err(|_| {
                    SourceError::Protocol("pinned local source file cache is poisoned".to_owned())
                })?;
                return Ok(files.entry(object.to_owned()).or_insert(None).clone());
            }
            Err(source) => {
                return Err(SourceError::Io {
                    object: object.to_owned(),
                    source,
                });
            }
        };
        let metadata = file.metadata().map_err(|source| SourceError::Io {
            object: object.to_owned(),
            source,
        })?;
        if !metadata.is_file() {
            return Err(SourceError::Protocol(format!(
                "{} is not a regular file",
                path.display()
            )));
        }
        let pinned = PinnedFile {
            file: Arc::new(file),
            identity: UnixFileIdentity::from_metadata(&metadata),
        };
        let mut files = self.files.lock().map_err(|_| {
            SourceError::Protocol("pinned local source file cache is poisoned".to_owned())
        })?;
        Ok(files
            .entry(object.to_owned())
            .or_insert_with(|| Some(pinned))
            .clone())
    }

    /// Verify that every object opened so far still has its captured file
    /// identity, size, and modification/change timestamps.
    pub fn verify_unchanged(&self) -> SourceResult<()> {
        let files: Vec<(String, PinnedFile)> = self
            .files
            .lock()
            .map_err(|_| {
                SourceError::Protocol("pinned local source file cache is poisoned".to_owned())
            })?
            .iter()
            .filter_map(|(object, file)| file.as_ref().map(|file| (object.clone(), file.clone())))
            .collect();

        for (object, file) in files {
            let metadata = file.file.metadata().map_err(|source| SourceError::Io {
                object: object.clone(),
                source,
            })?;
            if !metadata.is_file() || UnixFileIdentity::from_metadata(&metadata) != file.identity {
                return Err(SourceError::Protocol(format!(
                    "object {object} changed after it was opened"
                )));
            }
        }
        Ok(())
    }
}

impl RangeSource for PinnedLocalRangeSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        Ok(self.pinned_file(object)?.map(|file| file.identity.size))
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        let mut bytes = Vec::new();
        self.read_range_into(object, offset, length, &mut bytes)?;
        Ok(bytes)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        bytes: &mut Vec<u8>,
    ) -> SourceResult<()> {
        if bytes.len() < length {
            bytes.resize(length, 0);
        } else {
            bytes.truncate(length);
        }
        self.read_range_into_slice(object, offset, bytes)
    }

    fn read_range_into_slice(
        &self,
        object: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> SourceResult<()> {
        let pinned = self
            .pinned_file(object)?
            .ok_or_else(|| SourceError::NotFound(object.to_owned()))?;
        let size = pinned.identity.size;
        let length = bytes.len();
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

        let mut read = 0usize;
        while read < length {
            let read_offset = offset + read as u64;
            let count = pinned
                .file
                .read_at(&mut bytes[read..], read_offset)
                .map_err(|source| SourceError::Io {
                    object: object.to_owned(),
                    source,
                })?;
            if count == 0 {
                return Err(SourceError::ShortRead {
                    object: object.to_owned(),
                    expected: length,
                    actual: read,
                });
            }
            read += count;
        }
        self.counters.read_calls.fetch_add(1, Ordering::Relaxed);
        self.counters
            .read_bytes
            .fetch_add(length as u64, Ordering::Relaxed);
        Ok(())
    }
}

/// Route objects found in `primary` there, and all other objects to `fallback`.
///
/// The intended Mac setup uses a local cache as `primary` for manifest,
/// registry, index and metadata, with the gateway HTTP source as `fallback`
/// for blocks and signatures.
#[derive(Debug, Clone)]
pub struct OverlayRangeSource<P, F> {
    primary: P,
    fallback: F,
}

impl<P, F> OverlayRangeSource<P, F> {
    pub fn new(primary: P, fallback: F) -> Self {
        Self { primary, fallback }
    }

    pub fn primary(&self) -> &P {
        &self.primary
    }

    pub fn fallback(&self) -> &F {
        &self.fallback
    }
}

impl<P: RangeSource, F: RangeSource> RangeSource for OverlayRangeSource<P, F> {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        match self.primary.size(object)? {
            Some(size) => Ok(Some(size)),
            None => self.fallback.size(object),
        }
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        if self.primary.size(object)?.is_some() {
            self.primary.read_range(object, offset, length)
        } else {
            self.fallback.read_range(object, offset, length)
        }
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        if self.primary.size(object)?.is_some() {
            self.primary
                .read_range_into(object, offset, length, destination)
        } else {
            self.fallback
                .read_range_into(object, offset, length, destination)
        }
    }

    fn read_range_into_slice(
        &self,
        object: &str,
        offset: u64,
        destination: &mut [u8],
    ) -> SourceResult<()> {
        if self.primary.size(object)?.is_some() {
            self.primary
                .read_range_into_slice(object, offset, destination)
        } else {
            self.fallback
                .read_range_into_slice(object, offset, destination)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, thread, time::Duration};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn local_source_reads_exact_ranges_and_rejects_traversal() {
        let directory = tempdir().unwrap();
        fs::write(directory.path().join("object.bin"), b"0123456789").unwrap();
        let source = LocalRangeSource::new(directory.path());
        assert_eq!(source.size("object.bin").unwrap(), Some(10));
        assert_eq!(source.read_range("object.bin", 3, 4).unwrap(), b"3456");
        let mut reusable = Vec::with_capacity(16);
        source
            .read_range_into("object.bin", 1, 6, &mut reusable)
            .unwrap();
        let allocation = reusable.as_ptr();
        assert_eq!(reusable, b"123456");
        source
            .read_range_into("object.bin", 7, 3, &mut reusable)
            .unwrap();
        assert_eq!(reusable.as_ptr(), allocation);
        assert_eq!(reusable, b"789");
        let mut direct = [0_u8; 4];
        source
            .read_range_into_slice("object.bin", 2, &mut direct)
            .unwrap();
        assert_eq!(&direct, b"2345");
        assert!(source.read_range("../object.bin", 0, 1).is_err());
        assert!(source.read_range("object.bin", 9, 2).is_err());
    }

    #[test]
    fn pinned_source_keeps_reading_open_file_after_path_replacement() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("object.bin");
        fs::write(&path, b"original").unwrap();
        let source = PinnedLocalRangeSource::new(directory.path());

        assert_eq!(source.read_range("object.bin", 0, 8).unwrap(), b"original");
        fs::rename(&path, directory.path().join("original.bin")).unwrap();
        fs::write(&path, b"replacement").unwrap();

        assert_eq!(source.size("object.bin").unwrap(), Some(8));
        assert_eq!(source.read_range("object.bin", 0, 8).unwrap(), b"original");
        assert_eq!(
            source.stats(),
            PinnedLocalRangeSourceStats {
                read_calls: 2,
                read_bytes: 16,
            }
        );
    }

    #[test]
    fn anchored_source_keeps_one_directory_generation_and_enforces_allowlist() {
        let parent = tempdir().unwrap();
        let active = parent.path().join("active");
        let replacement = parent.path().join("replacement");
        let held = parent.path().join("held");
        fs::create_dir(&active).unwrap();
        fs::create_dir(&replacement).unwrap();
        fs::write(active.join("object.bin"), b"original").unwrap();
        fs::write(replacement.join("object.bin"), b"different").unwrap();
        fs::write(replacement.join("outside.bin"), b"outside").unwrap();
        let source = PinnedLocalRangeSource::new_anchored(&active, &["object.bin"]).unwrap();

        fs::rename(&active, &held).unwrap();
        fs::rename(&replacement, &active).unwrap();
        assert_eq!(source.read_range("object.bin", 0, 8).unwrap(), b"original");
        assert!(
            source
                .read_range("outside.bin", 0, 7)
                .unwrap_err()
                .to_string()
                .contains("outside the anchored local source allowlist")
        );

        let names = (0..65)
            .map(|index| format!("{index}.bin"))
            .collect::<Vec<_>>();
        let names = names.iter().map(String::as_str).collect::<Vec<_>>();
        assert!(PinnedLocalRangeSource::new_anchored(&held, &names).is_err());
    }

    #[test]
    fn pinned_source_detects_same_length_in_place_mutation() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("object.bin");
        fs::write(&path, b"before").unwrap();
        let source = PinnedLocalRangeSource::new(directory.path());

        assert_eq!(source.read_range("object.bin", 0, 6).unwrap(), b"before");
        thread::sleep(Duration::from_millis(2));
        fs::write(&path, b"after!").unwrap();

        assert!(source.verify_unchanged().is_err());
    }

    #[test]
    fn pinned_source_rejects_fifo_without_blocking() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("object.bin");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );
        let source = PinnedLocalRangeSource::new(directory.path());
        let (sender, receiver) = std::sync::mpsc::channel();
        let reader = thread::spawn(move || {
            sender.send(source.size("object.bin")).unwrap();
        });
        let error = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("opening a FIFO blocked before the regular-file check")
            .unwrap_err();
        assert!(
            error.to_string().contains("not a regular file"),
            "unexpected FIFO error: {error}"
        );
        reader.join().unwrap();
    }

    #[test]
    fn pinned_source_does_not_hold_cache_lock_while_opening_and_converges_first_open_racers() {
        let directory = tempdir().unwrap();
        fs::write(directory.path().join("ready.bin"), b"ready").unwrap();
        fs::write(directory.path().join("slow.bin"), b"slow").unwrap();
        fs::write(directory.path().join("raced.bin"), b"raced").unwrap();
        let source = PinnedLocalRangeSource::new(directory.path());
        assert_eq!(source.size("ready.bin").unwrap(), Some(5));

        let (open_started_sender, open_started_receiver) = std::sync::mpsc::channel();
        let (release_open_sender, release_open_receiver) = std::sync::mpsc::channel();
        let slow_source = source.clone();
        let slow_open = thread::spawn(move || {
            slow_source
                .pinned_file_with("slow.bin", |path| {
                    open_started_sender.send(()).unwrap();
                    release_open_receiver.recv().unwrap();
                    File::open(path)
                })
                .unwrap()
        });
        open_started_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let cached_source = source.clone();
        let (cached_sender, cached_receiver) = std::sync::mpsc::channel();
        let cached_read = thread::spawn(move || {
            cached_sender.send(cached_source.size("ready.bin")).unwrap();
        });
        assert_eq!(
            cached_receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("a slow filesystem open held the source-wide cache mutex")
                .unwrap(),
            Some(5)
        );
        cached_read.join().unwrap();
        release_open_sender.send(()).unwrap();
        assert!(slow_open.join().unwrap().is_some());

        let source = std::sync::Arc::new(source);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(16));
        let handles = (0..16)
            .map(|_| {
                let source = source.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    source.pinned_file("raced.bin").unwrap().unwrap().file
                })
            })
            .collect::<Vec<_>>();
        let files = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert!(
            files
                .iter()
                .all(|file| std::sync::Arc::ptr_eq(file, &files[0])),
            "concurrent first-open callers did not converge on one cached handle"
        );
    }
}
