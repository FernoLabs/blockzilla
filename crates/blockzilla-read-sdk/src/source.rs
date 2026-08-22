use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read},
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use rustix::fs::{Mode, OFlags};

use crate::{SourceError, manifest::validate_object_name};

pub type SourceResult<T> = std::result::Result<T, SourceError>;

/// Random-access byte source for immutable files in one published generation.
///
/// Object names are single path components from the generation manifest. An
/// HTTP implementation maps the manifest to
/// `/v1/epochs/{epoch}/manifest` and files to
/// `/v1/epochs/{epoch}/files/{name}`.
pub trait RangeSource: Send + Sync {
    /// Return `None` only when the object does not exist.
    fn size(&self, object: &str) -> SourceResult<Option<u64>>;

    /// Read exactly `length` bytes starting at `offset`.
    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>>;

    /// Read an exact range into reusable caller-owned storage.
    ///
    /// Remote and custom sources keep compatibility through this default.
    /// Local sequential readers override it to retain allocation capacity
    /// across adjacent prefetch batches.
    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        *destination = self.read_range(object, offset, length)?;
        Ok(())
    }

    fn read_all_bounded(&self, object: &str, max_length: usize) -> SourceResult<Vec<u8>> {
        let size = self
            .size(object)?
            .ok_or_else(|| SourceError::NotFound(object.to_owned()))?;
        let length = usize::try_from(size).map_err(|_| {
            SourceError::Protocol(format!("object {object} size does not fit this platform"))
        })?;
        if length > max_length {
            return Err(SourceError::Protocol(format!(
                "object {object} is {length} bytes, above the {max_length} byte limit"
            )));
        }
        self.read_range(object, 0, length)
    }
}

impl<T: RangeSource + ?Sized> RangeSource for Arc<T> {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        (**self).size(object)
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        (**self).read_range(object, offset, length)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        (**self).read_range_into(object, offset, length, destination)
    }
}

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

        // Preserve initialized storage across sequential range reads. Clearing
        // before resizing would zero the entire reused batch even though the
        // following `read_at` loop overwrites every requested byte.
        if bytes.len() < length {
            bytes.resize(length, 0);
        } else {
            bytes.truncate(length);
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

/// Random-access source that pins every opened object to one file descriptor.
///
/// This is intended for long-running local archive scans. The first lookup of
/// an object opens it and captures its Unix identity; later size and range
/// reads use that same handle even if the pathname is replaced. Missing
/// objects are cached as missing because a published generation is immutable.
#[derive(Debug, Clone)]
pub struct PinnedLocalRangeSource {
    root: PathBuf,
    files: Arc<Mutex<HashMap<String, Option<PinnedFile>>>>,
}

impl PinnedLocalRangeSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return a cloned descriptor for one object from this source's pinned
    /// generation view.
    ///
    /// The returned descriptor refers to the same file identity used by range
    /// reads, even if the pathname is replaced after the generation opens.
    pub fn open_file(&self, object: &str) -> SourceResult<File> {
        let pinned = self
            .pinned_file(object)?
            .ok_or_else(|| SourceError::NotFound(object.to_owned()))?;
        pinned.file.try_clone().map_err(|source| SourceError::Io {
            object: object.to_owned(),
            source,
        })
    }

    fn path(&self, object: &str) -> SourceResult<PathBuf> {
        validate_object_name(object).map_err(|_| SourceError::InvalidName(object.to_owned()))?;
        Ok(self.root.join(object))
    }

    fn pinned_file(&self, object: &str) -> SourceResult<Option<PinnedFile>> {
        self.pinned_file_with(object, |path| {
            rustix::fs::open(
                path,
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map(File::from)
            .map_err(io::Error::from)
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
    /// identity, size, and modification/change timestamps, and that its path
    /// still names that same file.
    pub fn verify_unchanged(&self) -> SourceResult<()> {
        let files: Vec<(String, Option<PinnedFile>)> = self
            .files
            .lock()
            .map_err(|_| {
                SourceError::Protocol("pinned local source file cache is poisoned".to_owned())
            })?
            .iter()
            .map(|(object, file)| (object.clone(), file.clone()))
            .collect();

        for (object, pinned) in files {
            let path = self.path(&object)?;
            let current = match rustix::fs::open(
                &path,
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                Mode::empty(),
            ) {
                Ok(file) => Some(File::from(file)),
                Err(error) if error == rustix::io::Errno::NOENT => None,
                Err(error) => {
                    return Err(SourceError::Io {
                        object: object.clone(),
                        source: io::Error::from(error),
                    });
                }
            };

            match (pinned, current) {
                (None, None) => {}
                (None, Some(_)) | (Some(_), None) => {
                    return Err(SourceError::Protocol(format!(
                        "object {object} changed after it was opened"
                    )));
                }
                (Some(pinned), Some(current)) => {
                    let pinned_metadata =
                        pinned.file.metadata().map_err(|source| SourceError::Io {
                            object: object.clone(),
                            source,
                        })?;
                    let current_metadata =
                        current.metadata().map_err(|source| SourceError::Io {
                            object: object.clone(),
                            source,
                        })?;
                    if !pinned_metadata.is_file()
                        || !current_metadata.is_file()
                        || UnixFileIdentity::from_metadata(&pinned_metadata) != pinned.identity
                        || UnixFileIdentity::from_metadata(&current_metadata) != pinned.identity
                    {
                        return Err(SourceError::Protocol(format!(
                            "object {object} changed after it was opened"
                        )));
                    }
                }
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
        let pinned = self
            .pinned_file(object)?
            .ok_or_else(|| SourceError::NotFound(object.to_owned()))?;
        let size = pinned.identity.size;
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

        if bytes.len() < length {
            bytes.resize(length, 0);
        } else {
            bytes.truncate(length);
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
}

pub(crate) struct RangeSourceReader<'a, S: RangeSource> {
    source: &'a S,
    object: &'a str,
    position: u64,
    end: u64,
    chunk_size: usize,
    chunk: Vec<u8>,
    chunk_position: usize,
}

impl<'a, S: RangeSource> RangeSourceReader<'a, S> {
    pub(crate) fn new(source: &'a S, object: &'a str, size: u64, chunk_size: usize) -> Self {
        Self {
            source,
            object,
            position: 0,
            end: size,
            chunk_size: chunk_size.max(1),
            chunk: Vec::new(),
            chunk_position: 0,
        }
    }

    fn refill(&mut self) -> io::Result<bool> {
        if self.position == self.end {
            return Ok(false);
        }
        let remaining = self.end - self.position;
        let length = usize::try_from(remaining.min(self.chunk_size as u64))
            .expect("chunk length is bounded by usize");
        self.source
            .read_range_into(self.object, self.position, length, &mut self.chunk)
            .map_err(io::Error::other)?;
        if self.chunk.len() != length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "source returned {} bytes for requested {} byte range",
                    self.chunk.len(),
                    length
                ),
            ));
        }
        self.position += length as u64;
        self.chunk_position = 0;
        Ok(true)
    }
}

impl<S: RangeSource> Read for RangeSourceReader<'_, S> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.chunk_position == self.chunk.len() && !self.refill()? {
            return Ok(0);
        }
        let available = &self.chunk[self.chunk_position..];
        let count = available.len().min(output.len());
        output[..count].copy_from_slice(&available[..count]);
        self.chunk_position += count;
        Ok(count)
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
        let mut retained = source.open_file("object.bin").unwrap();
        let mut retained_bytes = Vec::new();
        retained.read_to_end(&mut retained_bytes).unwrap();
        assert_eq!(retained_bytes, b"original");
        assert!(source.verify_unchanged().is_err());
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
